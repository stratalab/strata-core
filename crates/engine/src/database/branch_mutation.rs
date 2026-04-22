//! Branch mutation atomicity boundary.
//!
//! `BranchMutation` ensures that branch operations and DAG writes succeed or
//! fail together. If a DAG write fails after a branch mutation has completed,
//! the branch mutation is rolled back via compensating operations.
//!
//! ## Design
//!
//! Branch mutations in Strata are multi-step:
//!
//! 1. **Branch mutation** — storage fork, KV metadata creation, space registration
//! 2. **DAG recording** — write fork/merge/revert event to `_branch_dag`
//! 3. **Observer notification** — fire `BranchOpObserver` callbacks
//!
//! These steps are not transactional (each commits independently), so failures
//! at step 2 or 3 could leave inconsistent state. `BranchMutation` provides
//! atomicity by:
//!
//! - Registering rollback actions before mutations
//! - Executing rollback if DAG write fails
//! - Only firing observers after all load-bearing work succeeds
//!
//! ## Failure Model
//!
//! | Failure Point | Behavior |
//! |---------------|----------|
//! | Branch mutation fails | No DAG event, no observer, error returned |
//! | DAG write fails | Rollback branch mutation, no observer, error returned |
//! | Observer fails | Logged and swallowed (best-effort) |
//! | Rollback fails | Error surfaced (corruption-level severity) |
//!
//! ## Usage
//!
//! ```text
//! let mut mutation = BranchMutation::new(&db);
//!
//! // Do the mutation
//! let info = branch_ops::fork_branch_with_metadata(&db, source, dest, ...)?;
//!
//! // Register rollback in case DAG fails
//! mutation.on_rollback_delete_branch(dest, true);
//!
//! // Record to DAG — if this fails, rollback is executed
//! if let Some(version) = info.fork_version {
//!     let event = DagEvent::fork(...);
//!     mutation.record_dag_event(&event)?;
//! }
//!
//! // Commit — fires observers, clears rollback actions
//! mutation.commit(observer_event);
//! ```
//!
//! ## Failure Injection (Testing)
//!
//! For testing atomicity guarantees, `BranchMutation` supports failure injection
//! via `inject_failure()`. This allows tests to simulate failures at specific
//! points and verify correct rollback behavior.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::{BranchControlRecord, StrataError, StrataResult, VersionedValue};
use tracing::{error, info, warn};

use super::dag_hook::{BranchDagError, BranchDagHook, DagEvent};
use super::observers::BranchOpEvent;
use super::Database;
use crate::branch_ops::branch_control_store::BranchControlStore;
use crate::branch_ops::{is_user_visible_space, with_branch_dag_hooks_suppressed, DATA_TYPE_TAGS};
use crate::primitives::branch::{resolve_branch_name, BranchIndex, BranchMetadata};
use crate::SpaceIndex;

// =============================================================================
// Failure Injection (Test Support)
// =============================================================================

/// Points where failures can be injected for testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FailurePoint {
    /// Fail when recording DAG event.
    DagWrite,
    /// Fail when committing (after DAG write, before observers).
    Commit,
    /// Fail when executing rollback.
    Rollback,
}

// =============================================================================
// Rollback Actions
// =============================================================================

/// Actions to execute if the mutation needs to roll back.
#[derive(Debug)]
enum RollbackAction {
    /// Delete a branch and optionally clear its storage.
    DeleteBranch {
        /// Branch name to delete.
        name: String,
        /// Whether to clear storage-layer data (segments, manifests).
        clear_storage: bool,
    },
    /// Revert the exact branch version range written by a failed mutation.
    RevertBranchRange {
        /// Branch name to revert.
        name: String,
        /// First version written by the failed mutation.
        from_version: CommitVersion,
        /// Last version written by the failed mutation.
        to_version: CommitVersion,
    },
    /// Restore a branch snapshot captured before delete.
    RestoreBranchSnapshot(Box<BranchSnapshot>),
}

impl RollbackAction {
    /// Execute this rollback action.
    fn execute(self, db: &Arc<Database>) -> StrataResult<()> {
        match self {
            Self::DeleteBranch {
                name,
                clear_storage,
            } => {
                info!(
                    target: "strata::branch_mutation",
                    branch = %name,
                    clear_storage,
                    "Executing rollback: delete branch"
                );

                let branch_index = BranchIndex::new(db.clone());
                let control_store = BranchControlStore::new(db.clone());
                let branch_id = crate::primitives::branch::resolve_branch_name(&name);
                let metadata_exists = branch_index.exists(&name)?;

                if metadata_exists {
                    if let Err(e) = with_branch_dag_hooks_suppressed(|| {
                        branch_index.delete_branch_with_hook(&name, |txn| {
                            match control_store.mark_deleted_by_name(&name, txn)? {
                                Some(_) => Ok(()),
                                None => Err(StrataError::corruption(format!(
                                    "rollback delete for branch '{name}' found legacy metadata without an active control record"
                                ))),
                            }
                        })
                    }) {
                        warn!(
                            target: "strata::branch_mutation",
                            branch = %name,
                            error = %e,
                            "Rollback delete_branch failed"
                        );
                    }
                } else if let Some(rec) = control_store.find_active_by_name(&name)? {
                    db.transaction(global_branch_id(), |txn| {
                        control_store.mark_deleted(rec.branch, txn)
                    })?;
                    if clear_storage {
                        db.clear_branch_storage(&branch_id);
                    }
                }

                Ok(())
            }
            Self::RevertBranchRange {
                name,
                from_version,
                to_version,
            } => {
                info!(
                    target: "strata::branch_mutation",
                    branch = %name,
                    from_version = from_version.as_u64(),
                    to_version = to_version.as_u64(),
                    "Executing rollback: revert branch version range"
                );
                rollback_branch_range(db, &name, from_version, to_version)
            }
            Self::RestoreBranchSnapshot(snapshot) => {
                info!(
                    target: "strata::branch_mutation",
                    branch = %snapshot.name,
                    entry_count = snapshot.entries.len(),
                    "Executing rollback: restore deleted branch snapshot"
                );
                snapshot.restore(db)
            }
        }
    }
}

#[derive(Debug)]
struct BranchSnapshot {
    name: String,
    metadata: BranchMetadata,
    control_record: BranchControlRecord,
    executor_branch_id: BranchId,
    metadata_branch_id: Option<BranchId>,
    entries: Vec<(Key, Value)>,
    spaces: Vec<(BranchId, String)>,
}

impl BranchSnapshot {
    fn capture(db: &Arc<Database>, name: &str) -> StrataResult<Self> {
        let branch_index = BranchIndex::new(db.clone());
        let control_store = BranchControlStore::new(db.clone());
        let metadata = branch_index
            .get_branch(name)?
            .ok_or_else(|| StrataError::invalid_input(format!("Branch '{}' not found", name)))?
            .value;
        let control_record = control_store.find_active_by_name(name)?.ok_or_else(|| {
            StrataError::corruption(format!(
                "branch '{}' has legacy metadata but no active control record",
                name
            ))
        })?;

        let executor_branch_id = resolve_branch_name(name);
        let metadata_branch_id = BranchId::from_string(&metadata.branch_id)
            .filter(|meta_id| *meta_id != executor_branch_id);

        let mut entries = collect_branch_entries(db, executor_branch_id);
        if let Some(meta_id) = metadata_branch_id {
            entries.extend(collect_branch_entries(db, meta_id));
        }

        let mut spaces = collect_branch_spaces(db, executor_branch_id)?;
        if let Some(meta_id) = metadata_branch_id {
            spaces.extend(collect_branch_spaces(db, meta_id)?);
        }

        Ok(Self {
            name: name.to_string(),
            metadata,
            control_record,
            executor_branch_id,
            metadata_branch_id,
            entries,
            spaces,
        })
    }

    fn restore(self, db: &Arc<Database>) -> StrataResult<()> {
        let meta_key = Key::new_branch_with_id(global_namespace(), &self.name);
        let metadata_value = stored_branch_metadata_value(&self.metadata)?;
        let control_store = BranchControlStore::new(db.clone());

        db.transaction(global_branch_id(), |txn| {
            txn.set_allow_cross_branch(true);
            txn.put(meta_key.clone(), metadata_value.clone())?;
            control_store.put_record(&self.control_record, txn)?;
            for (key, value) in &self.entries {
                txn.put(key.clone(), value.clone())?;
            }
            Ok(())
        })?;

        let space_index = SpaceIndex::new(db.clone());
        for (branch_id, space) in &self.spaces {
            space_index.register(*branch_id, space)?;
        }

        db.unmark_branch_deleting(&self.executor_branch_id);
        if let Some(meta_id) = self.metadata_branch_id {
            db.unmark_branch_deleting(&meta_id);
        }

        crate::primitives::kv::invalidate_kv_namespace_cache(&self.executor_branch_id);
        crate::system_space::invalidate_cache(&self.executor_branch_id);
        if let Some(meta_id) = self.metadata_branch_id {
            crate::primitives::kv::invalidate_kv_namespace_cache(&meta_id);
            crate::system_space::invalidate_cache(&meta_id);
        }

        Ok(())
    }
}

fn collect_branch_entries(db: &Arc<Database>, branch_id: BranchId) -> Vec<(Key, Value)> {
    let storage = db.storage();
    let mut entries = Vec::new();
    for &type_tag in &DATA_TYPE_TAGS {
        entries.extend(
            storage
                .list_by_type(&branch_id, type_tag)
                .into_iter()
                .map(|(key, vv)| (key, vv.value)),
        );
    }
    entries
}

fn collect_branch_spaces(
    db: &Arc<Database>,
    branch_id: BranchId,
) -> StrataResult<Vec<(BranchId, String)>> {
    let space_index = SpaceIndex::new(db.clone());
    let spaces = space_index
        .list(branch_id)?
        .into_iter()
        .filter(|space| space != "default" && is_user_visible_space(space))
        .collect::<BTreeSet<_>>();
    Ok(spaces.into_iter().map(|space| (branch_id, space)).collect())
}

fn global_branch_id() -> BranchId {
    BranchId::from_bytes([0; 16])
}

fn global_namespace() -> Arc<Namespace> {
    Arc::new(Namespace::for_branch(global_branch_id()))
}

fn stored_branch_metadata_value(metadata: &BranchMetadata) -> StrataResult<Value> {
    serde_json::to_string(metadata)
        .map(Value::String)
        .map_err(|e| StrataError::serialization(e.to_string()))
}

fn build_versioned_space_map(
    entries: &[strata_storage::VersionedEntry],
) -> HashMap<(String, Vec<u8>), Option<Value>> {
    let mut map = HashMap::new();
    for entry in entries {
        let key = (
            entry.key.namespace.space.clone(),
            entry.key.user_key.to_vec(),
        );
        if entry.is_tombstone {
            map.insert(key, None);
        } else {
            map.insert(key, Some(entry.value.clone()));
        }
    }
    map
}

fn build_live_space_map(entries: &[(Key, VersionedValue)]) -> HashMap<(String, Vec<u8>), Value> {
    let mut map = HashMap::new();
    for (key, vv) in entries {
        map.insert(
            (key.namespace.space.clone(), key.user_key.to_vec()),
            vv.value.clone(),
        );
    }
    map
}

fn rollback_branch_range(
    db: &Arc<Database>,
    branch: &str,
    from_version: CommitVersion,
    to_version: CommitVersion,
) -> StrataResult<()> {
    let branch_id = resolve_branch_name(branch);
    let branch_index = BranchIndex::new(db.clone());
    branch_index
        .get_branch(branch)?
        .ok_or_else(|| StrataError::invalid_input(format!("Branch '{}' not found", branch)))?;

    let storage = db.storage();
    let mut puts: Vec<(Key, Value)> = Vec::new();
    let mut deletes: Vec<Key> = Vec::new();

    for &type_tag in &DATA_TYPE_TAGS {
        let before_entries = storage.list_by_type_at_version(
            &branch_id,
            type_tag,
            CommitVersion(from_version.as_u64().saturating_sub(1)),
        );
        let after_entries = storage.list_by_type_at_version(&branch_id, type_tag, to_version);
        let current_entries = storage.list_by_type(&branch_id, type_tag);

        let before_map = build_versioned_space_map(&before_entries);
        let after_map = build_versioned_space_map(&after_entries);
        let current_map = build_live_space_map(&current_entries);

        let all_keys: BTreeSet<(String, Vec<u8>)> =
            before_map.keys().chain(after_map.keys()).cloned().collect();

        for compound_key in &all_keys {
            let before_state = before_map.get(compound_key).cloned().flatten();
            let after_state = after_map.get(compound_key).cloned().flatten();
            let current_state = current_map.get(compound_key).cloned();

            if before_state == after_state {
                continue;
            }

            if current_state != after_state {
                return Err(StrataError::conflict(format!(
                    "rollback of branch '{}' failed: key changed after failed mutation",
                    branch
                )));
            }

            let (space, user_key) = compound_key;
            let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
            let key = Key::new(ns, type_tag, user_key.clone());

            match before_state {
                Some(value) => puts.push((key, value)),
                None => deletes.push(key),
            }
        }
    }

    if puts.is_empty() && deletes.is_empty() {
        return Ok(());
    }

    db.transaction(global_branch_id(), |txn| {
        txn.set_allow_cross_branch(true);
        for (key, value) in &puts {
            txn.put(key.clone(), value.clone())?;
        }
        for key in &deletes {
            txn.delete(key.clone())?;
        }
        Ok(())
    })?;

    Ok(())
}

// =============================================================================
// Mutation State
// =============================================================================

/// State of a branch mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationState {
    /// Mutation in progress, not yet committed.
    Pending,
    /// Mutation committed successfully.
    Committed,
    /// Mutation rolled back.
    RolledBack,
}

// =============================================================================
// BranchMutation
// =============================================================================

/// Branch mutation with atomicity guarantees.
///
/// Ensures that branch operations and DAG writes succeed or fail together.
/// See module documentation for usage.
pub struct BranchMutation<'a> {
    /// Database reference.
    db: &'a Arc<Database>,

    /// Cached DAG hook (avoids repeated lookups).
    dag_hook: Option<Arc<dyn BranchDagHook>>,

    /// Rollback actions to execute if the mutation fails.
    rollback_actions: Vec<RollbackAction>,

    /// Current state of the mutation.
    state: MutationState,

    /// Failure injection point for testing.
    #[cfg(any(test, feature = "test-support"))]
    failure_injection: Option<FailurePoint>,
}

impl<'a> BranchMutation<'a> {
    /// Create a new branch mutation context.
    pub fn new(db: &'a Arc<Database>) -> Self {
        let dag_hook = db.dag_hook().get();

        Self {
            db,
            dag_hook,
            rollback_actions: Vec::new(),
            state: MutationState::Pending,
            #[cfg(any(test, feature = "test-support"))]
            failure_injection: None,
        }
    }

    /// Check if a DAG hook is installed.
    ///
    /// Operations that require DAG recording should check this and fail early
    /// if no hook is installed.
    pub fn has_dag_hook(&self) -> bool {
        self.dag_hook.is_some()
    }

    /// Require a DAG hook for this operation.
    ///
    /// Returns an error if no DAG hook is installed.
    pub fn require_dag_hook(&self, operation: &str) -> StrataResult<()> {
        if self.dag_hook.is_none() {
            return Err(StrataError::invalid_input(format!(
                "operation '{}' requires a DAG hook but none is installed",
                operation
            )));
        }
        Ok(())
    }

    // =========================================================================
    // Rollback Registration
    // =========================================================================

    /// Register a rollback action to delete a branch.
    ///
    /// If the mutation fails (e.g., DAG write fails), the branch is deleted.
    ///
    /// # Arguments
    ///
    /// * `name` — Branch name to delete on rollback.
    /// * `clear_storage` — Whether to clear storage-layer data (segments, manifests).
    pub fn on_rollback_delete_branch(&mut self, name: impl Into<String>, clear_storage: bool) {
        self.rollback_actions.push(RollbackAction::DeleteBranch {
            name: name.into(),
            clear_storage,
        });
    }

    /// Register a rollback action to revert an exact branch version range.
    pub fn on_rollback_revert_range(
        &mut self,
        name: impl Into<String>,
        from_version: CommitVersion,
        to_version: CommitVersion,
    ) {
        self.rollback_actions
            .push(RollbackAction::RevertBranchRange {
                name: name.into(),
                from_version,
                to_version,
            });
    }

    /// Capture a branch snapshot and register it for restore-on-rollback.
    pub fn on_rollback_restore_branch(&mut self, name: &str) -> StrataResult<()> {
        let snapshot = BranchSnapshot::capture(self.db, name)?;
        self.rollback_actions
            .push(RollbackAction::RestoreBranchSnapshot(Box::new(snapshot)));
        Ok(())
    }

    // =========================================================================
    // DAG Recording
    // =========================================================================

    /// Record a DAG event.
    ///
    /// If the DAG hook is installed and the write fails, registered rollback
    /// actions are executed and an error is returned.
    ///
    /// If no DAG hook is installed, this is a no-op (returns `Ok`).
    ///
    /// # Errors
    ///
    /// - DAG write failed
    /// - Rollback failed (corruption-level severity)
    pub fn record_dag_event(&mut self, event: &DagEvent) -> StrataResult<()> {
        // Check for injected failure
        #[cfg(any(test, feature = "test-support"))]
        if self.failure_injection == Some(FailurePoint::DagWrite) {
            return self
                .handle_dag_failure(BranchDagError::write_failed("injected failure for testing"));
        }

        let Some(hook) = &self.dag_hook else {
            // No DAG hook installed — DAG recording is optional
            return Ok(());
        };

        match hook.record_event(event) {
            Ok(()) => Ok(()),
            Err(e) => self.handle_dag_failure(e),
        }
    }

    /// Handle a DAG write failure by executing rollback.
    fn handle_dag_failure(&mut self, dag_error: BranchDagError) -> StrataResult<()> {
        error!(
            target: "strata::branch_mutation",
            error = %dag_error,
            "DAG write failed, executing rollback"
        );

        // Execute rollback
        if let Err(rollback_error) = self.execute_rollback() {
            // Rollback failure is corruption-level severity
            error!(
                target: "strata::branch_mutation",
                dag_error = %dag_error,
                rollback_error = %rollback_error,
                "CRITICAL: Rollback failed after DAG write failure"
            );
            return Err(StrataError::corruption(format!(
                "rollback failed after DAG write failure: dag_error={}, rollback_error={}",
                dag_error, rollback_error
            )));
        }

        self.state = MutationState::RolledBack;
        Err(StrataError::internal(format!(
            "branch operation failed: DAG write error ({}); mutation rolled back",
            dag_error
        )))
    }

    // =========================================================================
    // Commit
    // =========================================================================

    /// Commit the mutation.
    ///
    /// Fires observers and clears rollback actions. After this, the mutation
    /// is considered successful and cannot be rolled back.
    ///
    /// Observer failures are logged but not propagated (best-effort).
    pub fn commit(mut self, observer_event: BranchOpEvent) {
        // Check for injected failure
        #[cfg(any(test, feature = "test-support"))]
        if self.failure_injection == Some(FailurePoint::Commit) {
            // For commit failure injection, we execute rollback
            let _ = self.execute_rollback();
            self.state = MutationState::RolledBack;
            return;
        }

        // Clear rollback actions — mutation is now permanent
        self.rollback_actions.clear();
        self.state = MutationState::Committed;

        // Fire observers (best-effort)
        self.db.branch_op_observers().notify(&observer_event);
    }

    /// Commit without firing observers.
    ///
    /// Used when the operation doesn't have an observer event (e.g., simple
    /// operations or when observers are fired separately).
    pub fn commit_silent(mut self) {
        self.rollback_actions.clear();
        self.state = MutationState::Committed;
    }

    // =========================================================================
    // Rollback
    // =========================================================================

    /// Execute all registered rollback actions.
    ///
    /// Returns the first error encountered, but attempts all rollbacks.
    fn execute_rollback(&mut self) -> StrataResult<()> {
        // Check for injected failure
        #[cfg(any(test, feature = "test-support"))]
        if self.failure_injection == Some(FailurePoint::Rollback) {
            return Err(StrataError::internal(
                "injected rollback failure for testing",
            ));
        }

        let actions = std::mem::take(&mut self.rollback_actions);
        let mut first_error: Option<StrataError> = None;

        for action in actions {
            if let Err(e) = action.execute(self.db) {
                warn!(
                    target: "strata::branch_mutation",
                    error = %e,
                    "Rollback action failed"
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Explicitly abort the mutation and execute rollback.
    ///
    /// Use this when the mutation fails before DAG recording (e.g., if the
    /// branch_ops function returns an error).
    pub fn abort(mut self) -> StrataResult<()> {
        let result = self.execute_rollback();
        self.state = MutationState::RolledBack;
        result
    }

    /// Cancel the mutation without running rollback actions.
    ///
    /// Use this when rollback actions were registered proactively but the
    /// low-level branch mutation itself returned an error and preserved state.
    pub fn cancel(mut self) {
        self.rollback_actions.clear();
        self.state = MutationState::RolledBack;
    }

    // =========================================================================
    // Failure Injection (Testing)
    // =========================================================================

    /// Inject a failure at the specified point.
    ///
    /// Only available in test builds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn inject_failure(&mut self, point: FailurePoint) {
        self.failure_injection = Some(point);
    }
}

impl Drop for BranchMutation<'_> {
    fn drop(&mut self) {
        if self.state == MutationState::Pending && !self.rollback_actions.is_empty() {
            // Uncommitted mutation with rollback actions — this is a bug
            warn!(
                target: "strata::branch_mutation",
                rollback_count = self.rollback_actions.len(),
                "BranchMutation dropped without commit or explicit abort; executing rollback"
            );
            let _ = self.execute_rollback();
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::dag_hook::{AncestryEntry, MergeBaseResult};
    use crate::Database;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use strata_core::id::CommitVersion;
    use strata_core::types::BranchId;

    /// Test DAG hook that can be configured to fail.
    struct TestDagHook {
        should_fail: AtomicBool,
        events_recorded: AtomicUsize,
    }

    impl TestDagHook {
        fn new() -> Self {
            Self {
                should_fail: AtomicBool::new(false),
                events_recorded: AtomicUsize::new(0),
            }
        }

        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::SeqCst);
        }

        fn event_count(&self) -> usize {
            self.events_recorded.load(Ordering::SeqCst)
        }
    }

    impl BranchDagHook for TestDagHook {
        fn name(&self) -> &'static str {
            "test"
        }

        fn record_event(&self, _event: &DagEvent) -> Result<(), BranchDagError> {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(BranchDagError::write_failed("test failure"));
            }
            self.events_recorded.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn find_merge_base(
            &self,
            _branch_a: &str,
            _branch_b: &str,
        ) -> Result<Option<MergeBaseResult>, BranchDagError> {
            Ok(None)
        }

        fn log(&self, _branch: &str, _limit: usize) -> Result<Vec<DagEvent>, BranchDagError> {
            Ok(Vec::new())
        }

        fn ancestors(&self, _branch: &str) -> Result<Vec<AncestryEntry>, BranchDagError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn test_mutation_without_dag_hook() {
        let db = Database::cache().unwrap();
        let mut mutation = BranchMutation::new(&db);

        assert!(!mutation.has_dag_hook());

        // DAG recording should be no-op without hook
        let event = DagEvent::branch_create(BranchId::new(), "test", CommitVersion(1));
        assert!(mutation.record_dag_event(&event).is_ok());

        // Commit should work
        let observer_event =
            super::super::observers::BranchOpEvent::create(BranchId::new(), "test");
        mutation.commit(observer_event);
    }

    #[test]
    fn test_mutation_with_dag_hook_success() {
        let db = Database::cache().unwrap();
        let hook = Arc::new(TestDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        let mut mutation = BranchMutation::new(&db);
        assert!(mutation.has_dag_hook());

        // Record event
        let event = DagEvent::branch_create(BranchId::new(), "test", CommitVersion(1));
        assert!(mutation.record_dag_event(&event).is_ok());
        assert_eq!(hook.event_count(), 1);

        // Commit
        let observer_event =
            super::super::observers::BranchOpEvent::create(BranchId::new(), "test");
        mutation.commit(observer_event);
    }

    #[test]
    fn test_mutation_dag_failure_triggers_rollback() {
        let db = Database::cache().unwrap();
        let hook = Arc::new(TestDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        // Create a branch that will be rolled back
        db.branches().create("rollback-test").unwrap();
        hook.set_should_fail(true);
        let branch_index = BranchIndex::new(db.clone());
        assert!(branch_index.exists("rollback-test").unwrap());

        let mut mutation = BranchMutation::new(&db);
        mutation.on_rollback_delete_branch("rollback-test", false);

        // Record event should fail
        let event = DagEvent::branch_create(BranchId::new(), "rollback-test", CommitVersion(1));
        let result = mutation.record_dag_event(&event);
        assert!(result.is_err());

        // Branch should be deleted by rollback
        assert!(!branch_index.exists("rollback-test").unwrap());
        let store = BranchControlStore::new(db.clone());
        let tombstone = store
            .get_record(strata_core::BranchRef::new(
                resolve_branch_name("rollback-test"),
                0,
            ))
            .unwrap()
            .expect("rollback should leave a tombstone control record");
        assert!(matches!(
            tombstone.lifecycle,
            strata_core::branch::BranchLifecycleStatus::Deleted
        ));
    }

    #[test]
    fn test_rollback_delete_does_not_emit_best_effort_delete_hook() {
        let db = Database::cache().unwrap();
        let hook = Arc::new(TestDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        db.branches().create("rollback-hook-test").unwrap();
        assert_eq!(
            hook.event_count(),
            1,
            "branch creation should record exactly one canonical DAG event"
        );

        let mut mutation = BranchMutation::new(&db);
        mutation.on_rollback_delete_branch("rollback-hook-test", false);
        mutation.abort().unwrap();

        assert_eq!(
            hook.event_count(),
            1,
            "rollback delete must not emit a best-effort DAG delete hook"
        );
    }

    #[test]
    fn test_mutation_injected_dag_failure() {
        let db = Database::cache().unwrap();

        // Create a branch
        db.branches().create("inject-test").unwrap();
        let branch_index = BranchIndex::new(db.clone());

        let mut mutation = BranchMutation::new(&db);
        mutation.on_rollback_delete_branch("inject-test", false);
        mutation.inject_failure(FailurePoint::DagWrite);

        // Record should fail even without a real hook
        let event = DagEvent::branch_create(BranchId::new(), "inject-test", CommitVersion(1));
        let result = mutation.record_dag_event(&event);
        assert!(result.is_err());

        // Branch should be deleted
        assert!(!branch_index.exists("inject-test").unwrap());
        let store = BranchControlStore::new(db.clone());
        let tombstone = store
            .get_record(strata_core::BranchRef::new(
                resolve_branch_name("inject-test"),
                0,
            ))
            .unwrap()
            .expect("rollback should leave a tombstone control record");
        assert!(matches!(
            tombstone.lifecycle,
            strata_core::branch::BranchLifecycleStatus::Deleted
        ));
    }

    #[test]
    fn test_mutation_explicit_abort() {
        let db = Database::cache().unwrap();

        // Create a branch
        db.branches().create("abort-test").unwrap();
        let branch_index = BranchIndex::new(db.clone());

        let mut mutation = BranchMutation::new(&db);
        mutation.on_rollback_delete_branch("abort-test", false);

        // Explicit abort
        mutation.abort().unwrap();

        // Branch should be deleted
        assert!(!branch_index.exists("abort-test").unwrap());
        let store = BranchControlStore::new(db.clone());
        let tombstone = store
            .get_record(strata_core::BranchRef::new(
                resolve_branch_name("abort-test"),
                0,
            ))
            .unwrap()
            .expect("rollback should leave a tombstone control record");
        assert!(matches!(
            tombstone.lifecycle,
            strata_core::branch::BranchLifecycleStatus::Deleted
        ));
    }

    #[test]
    fn test_mutation_drop_without_commit_triggers_rollback() {
        let db = Database::cache().unwrap();

        // Create a branch
        db.branches().create("drop-test").unwrap();
        let branch_index = BranchIndex::new(db.clone());

        {
            let mut mutation = BranchMutation::new(&db);
            mutation.on_rollback_delete_branch("drop-test", false);
            // Drop without commit or abort
        }

        // Branch should be deleted by drop
        assert!(!branch_index.exists("drop-test").unwrap());
        let store = BranchControlStore::new(db.clone());
        let tombstone = store
            .get_record(strata_core::BranchRef::new(
                resolve_branch_name("drop-test"),
                0,
            ))
            .unwrap()
            .expect("rollback should leave a tombstone control record");
        assert!(matches!(
            tombstone.lifecycle,
            strata_core::branch::BranchLifecycleStatus::Deleted
        ));
    }

    #[test]
    fn test_rollback_restore_branch_restores_active_control_record() {
        let db = Database::cache().unwrap();
        db.branches().create("restore-test").unwrap();

        let mut mutation = BranchMutation::new(&db);
        mutation.on_rollback_restore_branch("restore-test").unwrap();

        let branch_index = BranchIndex::new(db.clone());
        let store = BranchControlStore::new(db.clone());
        branch_index
            .delete_branch_with_hook("restore-test", |txn| {
                match store.mark_deleted_by_name("restore-test", txn)? {
                    Some(_) => Ok(()),
                    None => Err(StrataError::corruption(
                        "test setup deleted branch without active control record".to_string(),
                    )),
                }
            })
            .unwrap();

        mutation.abort().unwrap();

        assert!(branch_index.exists("restore-test").unwrap());
        let restored = db
            .branches()
            .control_record("restore-test")
            .unwrap()
            .expect("rollback restore must reactivate the control record");
        assert!(matches!(
            restored.lifecycle,
            strata_core::branch::BranchLifecycleStatus::Active
        ));
        assert_eq!(restored.branch.generation, 0);
    }

    #[test]
    fn test_require_dag_hook() {
        let db = Database::cache().unwrap();
        let mutation = BranchMutation::new(&db);

        // Should fail without hook
        assert!(mutation.require_dag_hook("merge_base").is_err());

        // Install hook
        let hook = Arc::new(TestDagHook::new());
        db.install_dag_hook(hook).unwrap();

        let mutation = BranchMutation::new(&db);
        assert!(mutation.require_dag_hook("merge_base").is_ok());
    }
}
