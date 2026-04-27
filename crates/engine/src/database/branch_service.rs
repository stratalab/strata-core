//! BranchService: Canonical branch operation facade.
//!
//! `BranchService` is the single canonical path for all branch operations.
//! It wraps existing `branch_ops` functions and adds:
//!
//! - Branch name validation
//! - DAG integration via `BranchDagHook`
//! - Observer notification via `BranchOpObserver`
//! - Capability gating for DAG-required operations
//!
//! ## Usage
//!
//! ```text
//! let branches = db.branches();
//! branches.create("feature-x")?;
//! branches.fork("main", "feature-x")?;
//! branches.merge("feature-x", "main", MergeOptions::default())?;
//! ```
//!
//! ## Same-name serialization (B4.4)
//!
//! Concurrent mutations that target the same branch name — e.g. a
//! `delete foo` racing a fresh `create foo`, a `fork x → foo` racing a
//! `delete foo`, or two threads calling `fork ? → foo` at once — must
//! never leave a half-state: every live record, active-pointer row,
//! and DAG projection converges to a single consistent winner and the
//! losers fail with a typed, retryable error. Same-name safety is
//! enforced by **three interlocking mechanisms**, not by a per-name
//! lifecycle lock. A change to any one of them weakens the whole model.
//!
//! 1. **Quiesce.** `BranchIndex::delete_branch_with_hook` calls
//!    `db.mark_branch_deleting(branch_id)` (see
//!    `primitives/branch/index.rs:536`) before the delete transaction
//!    opens. New transactions attempting to start on the marked branch
//!    are rejected with a typed error: a write that arrived after the
//!    quiesce mark cannot land on the lifecycle instance that is about
//!    to be tombstoned.
//!
//! 2. **Drain.** Immediately after the mark, the delete path acquires
//!    `db.branch_commit_lock(branch_id)` (see
//!    `primitives/branch/index.rs:542`). This lock is the same one
//!    every commit path holds across `commit()`; holding it here
//!    forces the delete to wait for any in-flight commit that started
//!    before the quiesce mark to finish and release. Once the lock is
//!    held, no new commit can acquire it — the mark rejects the next
//!    caller after it takes the lock. Combined with (1), the target
//!    branch's commit queue is strictly empty at the moment the delete
//!    transaction opens.
//!
//! 3. **OCC on the active pointer.**
//!    `BranchControlStore::mark_deleted_by_name` (see
//!    `branch_ops/branch_control_store.rs:642`) reads
//!    `__ctl__active__/<id>` **inside** the delete transaction, adding
//!    that row to the transaction's read set. Any concurrent
//!    `create` / fork-destination / recreate that advances or clears
//!    the active pointer between the read and the delete's commit
//!    aborts with `StrataError::Conflict`. This catches races the
//!    per-branch mark + commit lock cannot see — e.g. two threads
//!    allocating a fresh generation for the same *name* on two
//!    different lifecycle instances — because the active-pointer row
//!    is a name-scoped rendezvous point, not a branch-id-scoped one.
//!
//! ### Why not a per-name lifecycle lock
//!
//! The quiesce + drain + OCC stack already serialises every race the
//! B4 scope targets (see `docs/design/branching/b4-phasing-plan.md`
//! KD5). A `BranchLifecycleLock` keyed by branch name would add
//! contention on the cross-lifecycle-instance name rendezvous — the
//! single point in the system where a `foo@genN` tombstone and a
//! fresh `foo@genN+1` allocation meet — without fixing a race the
//! existing stack cannot already reject. B4.4 introduces such a lock
//! **only if** the race harness in
//! `tests/integration/branching_same_name_race.rs` reveals a failure
//! case the stack above cannot serialise. As of B4.4 ship, the
//! harness passes deterministically without it.
//!
//! The harness covers: concurrent delete + create, delete-then-create
//! sequential (generation monotonicity), fork-destination race,
//! double-fork-to-same-destination, and merge-target disappearance.
//! Each invariant below is asserted end-to-end by at least one
//! scenario:
//!
//! - At most one winner per race; all losers surface a typed error
//!   (`BranchNotFoundByName` / `InvalidInput` duplicate /
//!   `Conflict` / `BranchArchived`).
//! - No orphaned lineage edges, DAG events, or observer notifications
//!   from the losing side.
//! - Generation monotonicity holds across interleavings: every
//!   successful create on a tombstoned name observes a generation
//!   strictly greater than every prior tombstone under that name.
//! - Merge / fork never silently target a branch that has already
//!   transitioned to `Deleted` — they either complete against the
//!   pre-transition state or fail with a typed error.

use std::sync::Arc;

use crate::{StrataError, StrataResult};
use strata_core::branch::BranchLifecycleStatus;
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use strata_core::EntityRef;
use strata_core::{BranchControlRecord, BranchRef};

use crate::branch_ops::branch_control_store::{
    BranchControlStore, LineageEdgeKind, LineageEdgeRecord, MergeBasePoint,
};
use crate::branch_ops::with_branch_dag_hooks_suppressed;
use crate::branch_ops::{
    self, BranchDiffResult, CherryPickFilter, CherryPickInfo, DiffOptions, ForkInfo, MergeInfo,
    MergeStrategy, NoteInfo, RevertInfo, TagInfo, ThreeWayDiffResult,
};
use crate::database::branch_mutation::BranchMutation;
use crate::database::dag_hook::{BranchDagError, DagEvent, DagHookSlot, MergeBaseResult};
use crate::database::observers::{BranchOpEvent, BranchOpKind};
use crate::database::Database;
use crate::primitives::branch::{
    aliases_default_branch_sentinel, resolve_branch_name, BranchIndex, BranchMetadata,
    DeleteBranchCommitted, DeleteBranchCompletion, DeletePostCommitOptions,
};
use crate::primitives::event::EventLog;
use crate::SYSTEM_BRANCH;

// =============================================================================
// Merge Options
// =============================================================================

/// Options for branch merge operations.
///
/// B3.3 removed the `merge_base` field. Merge base is derived from the
/// authoritative [`BranchControlStore`] lineage (fork anchors + merge
/// edges) — callers can no longer inject a synthetic base. Unrelated
/// branches now refuse to merge; before B3.3 a caller-supplied override
/// could bypass that check, which silently produced incorrect ancestor
/// reads.
#[derive(Debug, Clone)]
pub struct MergeOptions {
    /// Conflict resolution strategy.
    pub strategy: MergeStrategy,
    /// Optional commit message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

impl Default for MergeOptions {
    fn default() -> Self {
        Self {
            strategy: MergeStrategy::LastWriterWins,
            message: None,
            creator: None,
        }
    }
}

impl MergeOptions {
    /// Create options with a specific strategy.
    pub fn with_strategy(strategy: MergeStrategy) -> Self {
        Self {
            strategy,
            ..Default::default()
        }
    }

    /// Set the commit message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Set the creator identifier.
    pub fn with_creator(mut self, creator: impl Into<String>) -> Self {
        self.creator = Some(creator.into());
        self
    }
}

/// Options for branch fork operations.
#[derive(Debug, Clone, Default)]
pub struct ForkOptions {
    /// Optional commit message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

impl ForkOptions {
    /// Set the commit message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Set the creator identifier.
    pub fn with_creator(mut self, creator: impl Into<String>) -> Self {
        self.creator = Some(creator.into());
        self
    }
}

// =============================================================================
// Branch Validation
// =============================================================================

/// Reserved branch name prefix for system branches.
const SYSTEM_PREFIX: &str = "_";

/// Maximum branch name length.
const MAX_BRANCH_NAME_LEN: usize = 255;

/// Validate a branch name.
fn validate_branch_name(name: &str) -> StrataResult<()> {
    if name.is_empty() {
        return Err(StrataError::invalid_input("branch name cannot be empty"));
    }

    if name.trim().is_empty() {
        return Err(StrataError::invalid_input(
            "branch name cannot be whitespace-only",
        ));
    }

    if name.len() > MAX_BRANCH_NAME_LEN {
        return Err(StrataError::invalid_input(format!(
            "branch name exceeds maximum length of {} characters",
            MAX_BRANCH_NAME_LEN
        )));
    }

    if name.starts_with(SYSTEM_PREFIX) {
        return Err(StrataError::invalid_input(format!(
            "branch name cannot start with '{}' (reserved for system)",
            SYSTEM_PREFIX
        )));
    }

    if name.chars().any(|c| c.is_control()) {
        return Err(StrataError::invalid_input(
            "branch name cannot contain control characters",
        ));
    }

    if aliases_default_branch_sentinel(name) {
        return Err(StrataError::invalid_input(
            "branch name aliases reserved default-branch sentinel",
        ));
    }

    Ok(())
}

fn reject_system_branch(name: &str, operation: &str) -> StrataResult<()> {
    if name.starts_with(SYSTEM_PREFIX) {
        return Err(StrataError::invalid_input(format!(
            "cannot {} system branch '{}'",
            operation, name
        )));
    }

    Ok(())
}

fn reject_default_branch(db: &Database, name: &str, operation: &str) -> StrataResult<()> {
    if db.default_branch_name().as_deref() == Some(name) {
        return Err(StrataError::invalid_operation(
            EntityRef::branch(resolve_branch_name(name)),
            format!("cannot {} the default branch", operation),
        ));
    }

    Ok(())
}

// =============================================================================
// BranchService
// =============================================================================

/// Canonical branch operation facade.
///
/// Obtained via `db.branches()`. Provides all branch operations with
/// integrated validation, DAG recording, and observer notification.
pub struct BranchService {
    db: Arc<Database>,
}

impl BranchService {
    /// Create a new BranchService for the given database.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    // =========================================================================
    // Core Operations (DAG optional)
    // =========================================================================

    /// Create a branch as part of bundle import, preserving the bundle's
    /// generation only when the target database has no prior lineage for the
    /// name.
    ///
    /// This stays `pub(crate)` because bundle import is the only caller. The
    /// helper exists to keep import on the canonical branch mutation boundary:
    /// control-record write, rollback, DAG create, and observer emission all
    /// follow the same path as ordinary branch creation.
    pub(crate) fn create_imported_branch(
        &self,
        name: &str,
        bundle_generation: u64,
    ) -> StrataResult<(BranchMetadata, BranchRef, bool)> {
        reject_system_branch(name, "import")?;
        validate_branch_name(name)?;

        let mut mutation = BranchMutation::new(&self.db);
        let branch_id = resolve_branch_name(name);
        let store = BranchControlStore::new(self.db.clone());
        let index = BranchIndex::new(self.db.clone());

        // Fail closed on metadata/control-store splits before import.
        let metadata_exists = index.exists(name)?;
        let active_control = store.find_active_by_name(name)?;
        match (metadata_exists, active_control) {
            (true, Some(_)) => {
                return Err(StrataError::invalid_input(format!(
                    "Branch '{}' already exists. Delete it first or use a different name.",
                    name
                )));
            }
            (true, None) => {
                return Err(StrataError::corruption(format!(
                    "branch '{}' has legacy metadata but no active control record during import",
                    name
                )));
            }
            (false, Some(_)) => {
                return Err(StrataError::corruption(format!(
                    "branch '{}' has an active control record but no legacy metadata during import",
                    name
                )));
            }
            (false, None) => {}
        }

        let bundle_seed_value = bundle_generation.checked_add(1).ok_or_else(|| {
            StrataError::invalid_input(
                "Bundle generation u64::MAX cannot be imported — would overflow next-gen counter",
            )
        })?;

        let chosen_generation: std::sync::Mutex<Option<(u64, bool)>> = std::sync::Mutex::new(None);
        let versioned = with_branch_dag_hooks_suppressed(|| {
            index.create_branch_with_hook(name, |txn| {
                let allocated = store.next_generation(branch_id, txn)?;
                let (generation, collision) = if allocated == 0 {
                    if bundle_generation > 0 {
                        store.seed_next_generation(branch_id, bundle_seed_value, txn)?;
                    }
                    (bundle_generation, false)
                } else {
                    (allocated, true)
                };

                let record = BranchControlRecord {
                    branch: BranchRef::new(branch_id, generation),
                    name: name.to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                };
                store.put_record(&record, txn)?;
                *chosen_generation.lock().unwrap() = Some((generation, collision));
                Ok(())
            })
        })?;

        let (generation, collision) = chosen_generation.lock().unwrap().expect(
            "create_branch_with_hook closure must run on the success path; \
             chosen_generation would otherwise stay None",
        );
        let branch_ref = BranchRef::new(branch_id, generation);

        self.db.unmark_branch_deleting(&branch_id);

        mutation.on_rollback_delete_branch(name, false);
        let event = DagEvent::create(branch_id, name).with_branch_ref(branch_ref);
        mutation.record_dag_event(&event)?;
        mutation.commit(BranchOpEvent::create(branch_ref, name));

        Ok((versioned.value, branch_ref, collision))
    }

    /// Create a new branch.
    ///
    /// Emits a DAG create event if a DAG hook is installed.
    ///
    /// B3.2: writes a canonical `BranchControlRecord` atomically with the
    /// legacy `BranchMetadata`. Same-name recreate allocates a fresh
    /// generation via `BranchControlStore::next_generation`.
    pub fn create(&self, name: &str) -> StrataResult<BranchMetadata> {
        validate_branch_name(name)?;

        let mut mutation = BranchMutation::new(&self.db);
        let branch_id = resolve_branch_name(name);
        let store = BranchControlStore::new(self.db.clone());

        // Defense-in-depth: reject if an active record already exists.
        // The per-txn duplicate check inside `create_branch_with_hook`
        // is the atomic guard; this early check surfaces a cleaner error
        // and short-circuits the name-lookup + counter-bump work.
        if store.find_active_by_name(name)?.is_some() {
            return Err(StrataError::invalid_input(format!(
                "Branch '{}' already exists",
                name
            )));
        }

        let index = BranchIndex::new(self.db.clone());
        // Capture the generation allocated inside the txn so the observer
        // event carries the new lifecycle instance's full `BranchRef`
        // (B3.4) — the closure runs synchronously, so the mutex is purely
        // a one-shot inside-out channel.
        let allocated_gen: std::sync::Mutex<Option<u64>> = std::sync::Mutex::new(None);
        let versioned = with_branch_dag_hooks_suppressed(|| {
            index.create_branch_with_hook(name, |txn| {
                let generation = store.next_generation(branch_id, txn)?;
                let record = BranchControlRecord {
                    branch: BranchRef::new(branch_id, generation),
                    name: name.to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                };
                store.put_record(&record, txn)?;
                *allocated_gen.lock().unwrap() = Some(generation);
                Ok(())
            })
        })?;
        let generation = allocated_gen.lock().unwrap().expect(
            "create_branch_with_hook closure must run on the success path; \
             allocated_gen would otherwise stay None",
        );
        let branch_ref = BranchRef::new(branch_id, generation);

        // B3.2: recreating a previously-deleted name must yield a
        // writable branch. `BranchIndex::delete_branch` intentionally
        // leaves the `is_branch_deleting` mark set to reject pre-delete
        // in-flight commits (#1916); the mark is cleared once the fresh
        // lifecycle instance is durable. Transactions now snapshot the
        // branch's active generation at start and recheck it on commit,
        // so stale pre-delete txns abort instead of writing into the new
        // lifecycle instance.
        self.db.unmark_branch_deleting(&branch_id);

        // Rollback: delete the branch on DAG failure
        mutation.on_rollback_delete_branch(name, false);

        // Record to DAG if hook installed (optional, not required)
        let event = DagEvent::create(branch_id, name).with_branch_ref(branch_ref);
        mutation.record_dag_event(&event)?;

        // Commit: fires observers
        mutation.commit(BranchOpEvent::create(branch_ref, name));

        Ok(versioned.value)
    }

    /// Delete a branch.
    ///
    /// Emits a DAG delete event if a DAG hook is installed.
    ///
    /// B3.2: flips the canonical `BranchControlRecord` to `Deleted` and
    /// clears the active-pointer atomically with the legacy purge. A
    /// subsequent `create` with the same name allocates a fresh
    /// generation.
    pub fn delete(&self, name: &str) -> StrataResult<()> {
        reject_system_branch(name, "delete")?;
        reject_default_branch(&self.db, name, "delete")?;

        let store = BranchControlStore::new(self.db.clone());

        // B4 lifecycle gate: delete is permitted for `Active` and `Archived`
        // targets; `Deleted` and missing branches surface as
        // `BranchNotFound`. Archived records stay visible via the widened
        // active-pointer semantics (KD1). Runs before `BranchMutation::new`
        // so a lifecycle-refused delete never registers rollback actions.
        store.require_visible_by_name(name)?;

        let mut mutation = BranchMutation::new(&self.db);
        let branch_id = resolve_branch_name(name);

        let index = BranchIndex::new(self.db.clone());

        if mutation.has_dag_hook() {
            mutation.on_rollback_restore_branch(name)?;
        }

        // B3.2: look up and mark the active control record INSIDE the
        // delete transaction so a racing `delete` + `create` between a
        // pre-txn lookup and the txn commit can't leave the control
        // store pointing at a live record whose legacy metadata was
        // just purged. The read of the active-pointer row is part of
        // the delete txn's read set → OCC fails it on conflict rather
        // than silently operating on a stale `BranchRef`.
        //
        // A missing active record here is corruption: the legacy metadata
        // row exists (BranchIndex resolved it) but the authoritative
        // control store has no active lifecycle for the same branch name.
        // Capture the deleted lifecycle instance's `BranchRef` so the
        // observer event records the exact generation that was tombstoned
        // (B3.4). The closure runs synchronously inside
        // `delete_branch_with_hook`, so the mutex is just a one-shot
        // inside-out channel.
        let deleted_ref: std::sync::Mutex<Option<BranchRef>> = std::sync::Mutex::new(None);
        let delete_result: StrataResult<DeleteBranchCommitted> =
            with_branch_dag_hooks_suppressed(|| {
                index.delete_branch_with_hook(name, |txn| {
                    match store.mark_deleted_by_name(name, txn)? {
                        Some(branch_ref) => {
                            *deleted_ref.lock().unwrap() = Some(branch_ref);
                            Ok(())
                        }
                        None => Err(StrataError::corruption(format!(
                            "live branch '{name}' has legacy metadata but no active control record"
                        ))),
                    }
                })
            });

        let delete_committed = match delete_result {
            Ok(committed) => committed,
            Err(e) => {
                mutation.cancel();
                return Err(e);
            }
        };

        let branch_ref = deleted_ref.lock().unwrap().expect(
            "delete_branch_with_hook closure must run on the success path; \
             deleted_ref would otherwise stay None",
        );

        // Record to DAG if hook installed (optional, not required)
        let event = DagEvent::delete(branch_id, name).with_branch_ref(branch_ref);
        mutation.record_dag_event(&event)?;

        // Commit: fires observers
        mutation.commit(BranchOpEvent::delete(branch_ref, name));

        match index.complete_delete_post_commit(
            name,
            &delete_committed,
            DeletePostCommitOptions {
                clear_storage: true,
                dispatch_best_effort_delete_hook: false,
            },
        ) {
            DeleteBranchCompletion::Clean => {}
            DeleteBranchCompletion::Warning(warning) => {
                tracing::warn!(
                    target: "strata::branch",
                    branch = name,
                    error = %warning,
                    "Branch delete completed, but storage cleanup reported retention debt"
                );
            }
            DeleteBranchCompletion::PostCommitError(err) => {
                tracing::warn!(
                    target: "strata::branch",
                    branch = name,
                    error = %err,
                    "Branch delete committed, but post-commit cleanup left retention debt"
                );
            }
        }

        Ok(())
    }

    /// Check if a branch exists.
    pub fn exists(&self, name: &str) -> StrataResult<bool> {
        let index = BranchIndex::new(self.db.clone());
        index.exists(name)
    }

    /// Get branch metadata.
    pub fn info(&self, name: &str) -> StrataResult<Option<BranchMetadata>> {
        let index = BranchIndex::new(self.db.clone());
        index.get_branch(name).map(|opt| opt.map(|v| v.value))
    }

    /// Get branch metadata with version/timestamp details.
    pub fn info_versioned(
        &self,
        name: &str,
    ) -> StrataResult<Option<strata_core::Versioned<BranchMetadata>>> {
        let index = BranchIndex::new(self.db.clone());
        index.get_branch(name)
    }

    /// List all branch names.
    pub fn list(&self) -> StrataResult<Vec<String>> {
        let index = BranchIndex::new(self.db.clone());
        index.list_branches()
    }

    /// Diff two branches.
    pub fn diff(&self, source: &str, target: &str) -> StrataResult<BranchDiffResult> {
        branch_ops::diff_branches(&self.db, source, target)
    }

    /// Diff two branches with options.
    pub fn diff_with_options(
        &self,
        source: &str,
        target: &str,
        options: DiffOptions,
    ) -> StrataResult<BranchDiffResult> {
        branch_ops::diff_branches_with_options(&self.db, source, target, options)
    }

    /// Three-way diff between branches.
    ///
    /// B3.3: merge base is derived from the authoritative
    /// [`BranchControlStore`] — callers no longer inject a base.
    pub fn diff3(&self, source: &str, target: &str) -> StrataResult<ThreeWayDiffResult> {
        branch_ops::diff_three_way(&self.db, source, target)
    }

    // =========================================================================
    // DAG-Required Operations
    // =========================================================================

    /// Fork a branch.
    pub fn fork(&self, source: &str, destination: &str) -> StrataResult<ForkInfo> {
        self.fork_with_options(source, destination, ForkOptions::default())
    }

    /// Fork a branch with options.
    ///
    /// Uses `BranchMutation` for atomicity: if DAG recording fails, the branch
    /// fork is rolled back (the new branch is deleted).
    ///
    /// Requires a DAG hook to be installed — forks without DAG recording would
    /// create orphan branches with no lineage tracking.
    pub fn fork_with_options(
        &self,
        source: &str,
        destination: &str,
        options: ForkOptions,
    ) -> StrataResult<ForkInfo> {
        reject_system_branch(source, "fork from")?;
        validate_branch_name(destination)?;

        let store = BranchControlStore::new(self.db.clone());
        let branch_index = BranchIndex::new(self.db.clone());

        // B4 lifecycle gate: fork is a read-then-create op. The source
        // needs to be visible (Active or Archived — copying a frozen
        // snapshot is valid); the destination must have no live record,
        // which preserves the pre-B4 duplicate semantics after the
        // active-pointer widening (KD1: an Archived record also counts
        // as a live duplicate for name allocation, not as a fork-from
        // lifecycle state the gate can override).
        store.require_visible_by_name(source)?;
        if branch_index.exists(destination)? || store.find_active_by_name(destination)?.is_some() {
            return Err(StrataError::invalid_input(format!(
                "Destination branch '{}' already exists",
                destination
            )));
        }

        // Create mutation context for atomicity
        let mut mutation = BranchMutation::new(&self.db);

        // Fork requires DAG — without it, lineage is lost
        mutation.require_dag_hook("fork")?;

        let dest_id = resolve_branch_name(destination);
        let dest_gen = self
            .db
            .transaction(BranchId::from_bytes([0u8; 16]), |txn| {
                store.next_generation(dest_id, txn)
            })?;

        // Execute the fork
        let info = with_branch_dag_hooks_suppressed(|| {
            branch_ops::fork_branch_with_metadata(
                &self.db,
                source,
                destination,
                options.message.as_deref(),
                options.creator.as_deref(),
                dest_gen,
            )
        })?;

        // Register rollback: if DAG write fails, delete the forked branch
        // We set clear_storage=true because fork_branch creates storage state
        mutation.on_rollback_delete_branch(destination, true);

        // Resolve the authoritative lifecycle refs written by the fork txn
        // once, then reuse them for both DAG and observer emission. This keeps
        // the projection and observer surfaces aligned to the exact
        // generation-aware parent/child relationship the storage fork used.
        let fork_refs = if let Some(fork_version) = info.fork_version {
            // Read the freshly-written child control record so the
            // observer event carries the exact source `BranchRef` recorded
            // on its fork anchor (B3.4). This avoids a `find_active_by_name`
            // race against a concurrent delete + recreate of `source`
            // between the fork txn and the observer notification.
            //
            // Fallback path: if the fork anchor is somehow absent (the
            // child record itself missing or carrying `fork: None`),
            // resolve the source via `find_active_by_name` instead of
            // assuming gen 0 — a recreated source could be at any
            // generation, and reporting gen 0 in the observer event
            // would silently mislabel the parent's lifecycle instance.
            let dest_ref = BranchRef::new(dest_id, dest_gen);
            let anchor_parent = store
                .get_record(dest_ref)?
                .and_then(|rec| rec.fork.map(|anchor| anchor.parent));
            let source_ref = anchor_parent.ok_or_else(|| {
                StrataError::corruption(format!(
                    "forked branch '{}' is missing authoritative ForkAnchor parent in control store",
                    destination
                ))
            })?;

            Some((fork_version, dest_ref, source_ref))
        } else {
            None
        };

        // Record to DAG — if this fails, rollback is executed
        if let Some((fork_version, dest_ref, source_ref)) = fork_refs {
            let source_id = resolve_branch_name(source);
            let dest_id = resolve_branch_name(destination);
            let mut event = DagEvent::fork(
                dest_id,
                destination,
                source_id,
                source,
                CommitVersion(fork_version),
            )
            .with_branch_ref(dest_ref)
            .with_source_branch_ref(source_ref);
            if let Some(msg) = &options.message {
                event = event.with_message(msg.clone());
            }
            if let Some(creator) = &options.creator {
                event = event.with_creator(creator.clone());
            }
            mutation.record_dag_event(&event)?;

            let mut observer_event = BranchOpEvent::fork(
                dest_ref,
                destination,
                source_ref,
                source,
                CommitVersion(fork_version),
            );
            if let Some(msg) = &options.message {
                observer_event = observer_event.with_message(msg.clone());
            }
            if let Some(creator) = &options.creator {
                observer_event = observer_event.with_creator(creator.clone());
            }
            mutation.commit(observer_event);
        } else {
            mutation.commit_silent();
        }

        Ok(info)
    }

    /// Merge one branch into another.
    pub fn merge(&self, source: &str, target: &str) -> StrataResult<MergeInfo> {
        self.merge_with_options(source, target, MergeOptions::default())
    }

    /// Merge one branch into another with options.
    ///
    /// Uses `BranchMutation` for atomicity: if DAG recording fails after the
    /// merge commit, the exact merge commit is reverted before returning.
    ///
    /// Requires a DAG hook to be installed — merges without DAG recording would
    /// lose merge provenance and break merge-base computation.
    ///
    /// B3.3: merge base is always derived from the authoritative
    /// [`BranchControlStore`]; after a successful merge, a
    /// `LineageEdgeRecord::Merge` edge is appended to the store so
    /// subsequent `find_merge_base` calls advance past this merge point.
    pub fn merge_with_options(
        &self,
        source: &str,
        target: &str,
        options: MergeOptions,
    ) -> StrataResult<MergeInfo> {
        reject_system_branch(source, "merge from")?;
        reject_system_branch(target, "merge into")?;

        // B4 lifecycle gate: source must be visible (Active or Archived —
        // merging from a frozen snapshot is valid); target must be
        // writable. An archived target refuses with `BranchArchived`.
        //
        // Gate reads the same records the low-level merge needs for
        // generation-scoped lineage, so we snapshot them here rather than
        // re-reading inside the merge body. Placed before
        // `BranchMutation::new` so a lifecycle-refused merge never
        // registers rollback actions.
        let store = BranchControlStore::new(self.db.clone());
        let source_rec = store.require_visible_by_name(source)?;
        let target_rec = store.require_writable_by_name(target)?;

        let mut mutation = BranchMutation::new(&self.db);

        // Merge requires DAG — without it, merge provenance is lost
        mutation.require_dag_hook("merge")?;

        let merge_exec = with_branch_dag_hooks_suppressed(|| {
            branch_ops::merge_branches_with_metadata_expected_detailed(
                &self.db,
                source,
                target,
                options.strategy,
                options.message.as_deref(),
                options.creator.as_deref(),
                Some(source_rec.branch),
                Some(target_rec.branch),
            )
        })?;
        let crate::branch_ops::MergeExecutionResult {
            info,
            merge_base_used,
        } = merge_exec;

        // Record to DAG — only if merge actually committed changes
        let source_id = source_rec.branch.id;
        let target_id = target_rec.branch.id;
        if let Some(merge_version) = info.merge_version {
            // Register rollback FIRST so every downstream failure
            // (DAG write, edge write) triggers a compensating revert
            // of the merge commit range. Without this, a failure later
            // in the block would leave the merge commit applied with
            // no lineage authority and no compensating revert.
            mutation.on_rollback_revert_range(
                target,
                CommitVersion(merge_version),
                CommitVersion(merge_version),
            );
            // Persist lineage edge BEFORE the DAG event. If the later DAG write
            // fails, rollback deletes this edge so neither authority leaks a
            // partially-recorded merge.
            mutation
                .on_rollback_delete_lineage_edge(target_rec.branch, CommitVersion(merge_version));
            append_merge_edge(
                &self.db,
                &store,
                target_rec.branch,
                source_rec.branch,
                CommitVersion(merge_version),
                Some(merge_base_used),
            )?;

            let mut event = DagEvent::merge(
                target_id,
                target,
                source_id,
                source,
                CommitVersion(merge_version),
                info.clone(),
                options.strategy,
            )
            .with_branch_ref(target_rec.branch)
            .with_source_branch_ref(source_rec.branch);
            if let Some(msg) = &options.message {
                event = event.with_message(msg.clone());
            }
            if let Some(creator) = &options.creator {
                event = event.with_creator(creator.clone());
            }
            mutation.record_dag_event(&event)?;

            // Commit: fires observers
            let strategy_str = match options.strategy {
                crate::MergeStrategy::LastWriterWins => "last_writer_wins",
                crate::MergeStrategy::Strict => "strict",
            };
            let mut observer_event = BranchOpEvent::merge(
                target_rec.branch,
                target,
                source_rec.branch,
                source,
                strategy_str,
                info.keys_applied,
                info.keys_deleted,
                CommitVersion(merge_version),
            );
            if let Some(msg) = &options.message {
                observer_event = observer_event.with_message(msg.clone());
            }
            if let Some(creator) = &options.creator {
                observer_event = observer_event.with_creator(creator.clone());
            }
            mutation.commit(observer_event);
        } else {
            // No-op merge: skip DAG recording but still commit silently
            mutation.commit_silent();
        }

        Ok(info)
    }

    /// Revert a version range on a branch.
    ///
    /// Uses `BranchMutation` for atomicity. Emits a DAG revert event and
    /// reverts the revert commit if DAG recording fails.
    ///
    /// Requires a DAG hook to be installed — reverts without DAG recording
    /// would lose revert provenance and break history queries.
    pub fn revert(
        &self,
        branch: &str,
        from_version: CommitVersion,
        to_version: CommitVersion,
    ) -> StrataResult<RevertInfo> {
        reject_system_branch(branch, "revert")?;

        // B4 lifecycle gate: revert targets must be writable. The captured
        // `BranchRef` also pins the lifecycle instance against a concurrent
        // delete+recreate between here and the edge write.
        let store = BranchControlStore::new(self.db.clone());
        let branch_rec = store.require_writable_by_name(branch)?;
        let branch_id = branch_rec.branch.id;

        let mut mutation = BranchMutation::new(&self.db);

        // Revert requires DAG — without it, revert history is lost
        mutation.require_dag_hook("revert")?;

        // Execute the revert
        let info = with_branch_dag_hooks_suppressed(|| {
            branch_ops::revert_version_range_with_expected(
                &self.db,
                branch,
                from_version,
                to_version,
                None,
                None,
                Some(branch_rec.branch),
            )
        })?;

        // Record to DAG — only if revert actually committed changes
        if let Some(revert_version) = info.revert_version {
            // Register rollback FIRST; edge + DAG writes are the
            // downstream steps a failure mid-path needs to compensate.
            mutation.on_rollback_revert_range(branch, revert_version, revert_version);
            mutation.on_rollback_delete_lineage_edge(branch_rec.branch, revert_version);
            append_revert_edge(&self.db, &store, branch_rec.branch, revert_version)?;
            let event = DagEvent::revert(branch_id, branch, revert_version, info.clone())
                .with_branch_ref(branch_rec.branch);
            mutation.record_dag_event(&event)?;

            // Commit: fires observers
            let observer_event = BranchOpEvent::revert(
                branch_rec.branch,
                branch,
                from_version,
                to_version,
                info.keys_reverted,
            );
            mutation.commit(observer_event);
        } else {
            // No-op revert: skip DAG recording
            mutation.commit_silent();
        }

        Ok(info)
    }

    /// Cherry-pick specific keys from one branch to another.
    ///
    /// Uses `BranchMutation` for atomicity. Emits a DAG cherry-pick event and
    /// reverts the cherry-pick commit if DAG recording fails.
    ///
    /// Requires a DAG hook to be installed — cherry-picks without DAG recording
    /// would lose cherry-pick provenance and break history queries.
    pub fn cherry_pick(
        &self,
        source: &str,
        target: &str,
        keys: &[(String, String)],
    ) -> StrataResult<CherryPickInfo> {
        reject_system_branch(source, "cherry-pick from")?;
        reject_system_branch(target, "cherry-pick into")?;

        // B4 lifecycle gate: source must be visible, target must be writable.
        let store = BranchControlStore::new(self.db.clone());
        let source_rec = store.require_visible_by_name(source)?;
        let target_rec = store.require_writable_by_name(target)?;
        let source_id = source_rec.branch.id;
        let target_id = target_rec.branch.id;

        let mut mutation = BranchMutation::new(&self.db);

        // Cherry-pick requires DAG — without it, cherry-pick history is lost
        mutation.require_dag_hook("cherry_pick")?;

        // Execute the cherry-pick
        let info = with_branch_dag_hooks_suppressed(|| {
            branch_ops::cherry_pick_keys_expected(
                &self.db,
                source,
                target,
                keys,
                Some(source_rec.branch),
                Some(target_rec.branch),
            )
        })?;

        // Record to DAG — only if cherry-pick actually committed changes
        if let Some(cp_version) = info.cherry_pick_version {
            // Register rollback FIRST.
            mutation.on_rollback_revert_range(
                target,
                CommitVersion(cp_version),
                CommitVersion(cp_version),
            );
            mutation.on_rollback_delete_lineage_edge(target_rec.branch, CommitVersion(cp_version));
            append_cherry_pick_edge(
                &self.db,
                &store,
                target_rec.branch,
                source_rec.branch,
                CommitVersion(cp_version),
            )?;
            let event = DagEvent::cherry_pick(
                target_id,
                target,
                source_id,
                source,
                CommitVersion(cp_version),
                info.clone(),
            )
            .with_branch_ref(target_rec.branch)
            .with_source_branch_ref(source_rec.branch);
            mutation.record_dag_event(&event)?;

            // Commit: fires observers
            let observer_event = BranchOpEvent::cherry_pick(
                target_rec.branch,
                target,
                source_rec.branch,
                source,
                info.keys_applied,
                info.keys_deleted,
            );
            mutation.commit(observer_event);
        } else {
            // No-op cherry-pick: skip DAG recording
            mutation.commit_silent();
        }

        Ok(info)
    }

    /// Cherry-pick changes from one branch to another using diff-based filtering.
    ///
    /// Uses `BranchMutation` for atomicity. Emits a DAG cherry-pick event and
    /// reverts the cherry-pick commit if DAG recording fails.
    ///
    /// Requires a DAG hook to be installed — cherry-picks without DAG recording
    /// would lose cherry-pick provenance and break history queries.
    pub fn cherry_pick_from_diff(
        &self,
        source: &str,
        target: &str,
        filter: CherryPickFilter,
    ) -> StrataResult<CherryPickInfo> {
        reject_system_branch(source, "cherry-pick from")?;
        reject_system_branch(target, "cherry-pick into")?;

        // B4 lifecycle gate: source must be visible, target must be writable.
        let store = BranchControlStore::new(self.db.clone());
        let source_rec = store.require_visible_by_name(source)?;
        let target_rec = store.require_writable_by_name(target)?;

        let mut mutation = BranchMutation::new(&self.db);

        // Cherry-pick requires DAG — without it, cherry-pick history is lost
        mutation.require_dag_hook("cherry_pick")?;
        let source_id = source_rec.branch.id;
        let target_id = target_rec.branch.id;

        // Execute the cherry-pick
        let info = with_branch_dag_hooks_suppressed(|| {
            branch_ops::cherry_pick_from_diff_expected(
                &self.db,
                source,
                target,
                filter,
                Some(source_rec.branch),
                Some(target_rec.branch),
            )
        })?;

        // Record to DAG — only if cherry-pick actually committed changes
        if let Some(cp_version) = info.cherry_pick_version {
            // Register rollback FIRST.
            mutation.on_rollback_revert_range(
                target,
                CommitVersion(cp_version),
                CommitVersion(cp_version),
            );
            mutation.on_rollback_delete_lineage_edge(target_rec.branch, CommitVersion(cp_version));
            append_cherry_pick_edge(
                &self.db,
                &store,
                target_rec.branch,
                source_rec.branch,
                CommitVersion(cp_version),
            )?;
            let event = DagEvent::cherry_pick(
                target_id,
                target,
                source_id,
                source,
                CommitVersion(cp_version),
                info.clone(),
            )
            .with_branch_ref(target_rec.branch)
            .with_source_branch_ref(source_rec.branch);
            mutation.record_dag_event(&event)?;

            // Commit: fires observers
            let observer_event = BranchOpEvent::cherry_pick(
                target_rec.branch,
                target,
                source_rec.branch,
                source,
                info.keys_applied,
                info.keys_deleted,
            );
            mutation.commit(observer_event);
        } else {
            // No-op cherry-pick: skip DAG recording
            mutation.commit_silent();
        }

        Ok(info)
    }

    // =========================================================================
    // Control-record queries (B3.2)
    // =========================================================================

    /// Look up the currently-active `BranchControlRecord` for `name`.
    ///
    /// Returns `None` if no such branch exists. Propagates
    /// `branch_lineage_unavailable` on an unmigrated follower (AD5).
    ///
    /// This is the canonical surface for observing a branch's
    /// generation-aware identity (`BranchRef`), lifecycle, and fork
    /// anchor. Both runtime consumers and tests use it to verify
    /// generation-safe write-path invariants (B3.2).
    pub fn control_record(&self, name: &str) -> StrataResult<Option<BranchControlRecord>> {
        BranchControlStore::new(self.db.clone()).find_active_by_name(name)
    }

    // =========================================================================
    // DAG Queries
    // =========================================================================

    /// Find the merge base (common ancestor) of two branches.
    ///
    /// Returns `None` if no common ancestor exists (unrelated branches).
    ///
    /// B3.3 cutover: authority is `BranchControlStore::find_merge_base`
    /// — no DAG fallback, no storage fallback. The store's lineage edges
    /// cover every fork/merge that B3.1 migration backfilled plus every
    /// new write from B3.2/B3.3, so a `None` result means the two
    /// generations are genuinely unrelated.
    pub fn merge_base(
        &self,
        branch_a: &str,
        branch_b: &str,
    ) -> StrataResult<Option<MergeBaseResult>> {
        let store = BranchControlStore::new(self.db.clone());

        let a_rec = store
            .find_active_by_name(branch_a)?
            .ok_or_else(|| StrataError::branch_not_found_by_name(branch_a))?;
        let b_rec = store
            .find_active_by_name(branch_b)?
            .ok_or_else(|| StrataError::branch_not_found_by_name(branch_b))?;

        let Some(point) = store.find_merge_base(a_rec.branch, b_rec.branch)? else {
            return Ok(None);
        };

        let branch_name = if point.branch == a_rec.branch {
            a_rec.name
        } else if point.branch == b_rec.branch {
            b_rec.name
        } else {
            store
                .get_record(point.branch)?
                .map(|r| r.name)
                .unwrap_or_else(|| point.branch.id.to_string())
        };

        Ok(Some(MergeBaseResult {
            branch_id: point.branch.id,
            branch_name,
            commit_version: point.commit_version,
        }))
    }

    /// Get the history log for a branch.
    pub fn log(
        &self,
        branch: &str,
        limit: usize,
    ) -> StrataResult<Vec<crate::database::dag_hook::DagEvent>> {
        let store = BranchControlStore::new(self.db.clone());
        store.ensure_lineage_read_available()?;
        match store.find_active_by_name(branch)? {
            Some(_) => {}
            None => {
                if BranchIndex::new(self.db.clone()).exists(branch)? {
                    return Err(StrataError::corruption(format!(
                        "branch '{}' has legacy metadata but no active control record for DAG history read",
                        branch
                    )));
                }
                return Err(StrataError::branch_not_found_by_name(branch));
            }
        }
        let hook = self.dag_hook().require("log").map_err(dag_to_strata)?;
        hook.log(branch, limit).map_err(dag_to_strata)
    }

    /// Get the ancestry chain for a branch.
    pub fn ancestors(
        &self,
        branch: &str,
    ) -> StrataResult<Vec<crate::database::dag_hook::AncestryEntry>> {
        let store = BranchControlStore::new(self.db.clone());
        store.ensure_lineage_read_available()?;
        match store.find_active_by_name(branch)? {
            Some(_) => {}
            None => {
                if BranchIndex::new(self.db.clone()).exists(branch)? {
                    return Err(StrataError::corruption(format!(
                        "branch '{}' has legacy metadata but no active control record for DAG ancestry read",
                        branch
                    )));
                }
                return Err(StrataError::branch_not_found_by_name(branch));
            }
        }
        let hook = self
            .dag_hook()
            .require("ancestors")
            .map_err(dag_to_strata)?;
        hook.ancestors(branch).map_err(dag_to_strata)
    }

    // =========================================================================
    // Tagging
    // =========================================================================

    /// Create a tag on a branch.
    ///
    /// Emits a `BranchOpEvent::Tag` to observers after successful creation.
    pub fn tag(
        &self,
        branch: &str,
        name: &str,
        version: Option<u64>,
        message: Option<&str>,
        creator: Option<&str>,
    ) -> StrataResult<TagInfo> {
        reject_system_branch(branch, "tag")?;

        // B4/KD4: annotation writes follow the same lifecycle gate as
        // structural mutations. Tags on an Archived branch are refused —
        // an archived lifecycle instance is read-only.
        let store = BranchControlStore::new(self.db.clone());
        let branch_rec = store.require_writable_by_name(branch)?;
        let branch_ref = branch_rec.branch;

        let info = branch_ops::create_tag_with_expected(
            &self.db,
            branch,
            name,
            version,
            message,
            creator,
            Some(branch_ref),
        )?;

        let branch_id = branch_rec.branch.id;
        let event = BranchOpEvent {
            kind: BranchOpKind::Tag,
            branch_id,
            branch_ref,
            branch_name: Some(branch.to_string()),
            source_branch_id: None,
            source_branch_ref: None,
            source_branch_name: None,
            commit_version: Some(CommitVersion(info.version)),
            tag_name: Some(name.to_string()),
            message: message.map(|s| s.to_string()),
            creator: creator.map(|s| s.to_string()),
            merge_strategy: None,
            keys_applied: None,
            keys_deleted: None,
            keys_reverted: None,
            from_version: None,
            to_version: None,
        };
        self.db.branch_op_observers().notify(&event);

        Ok(info)
    }

    /// Delete a tag from a branch.
    ///
    /// Emits a `BranchOpEvent::Untag` to observers if the tag existed.
    pub fn untag(&self, branch: &str, name: &str) -> StrataResult<bool> {
        reject_system_branch(branch, "untag")?;

        // B4/KD4: tag deletion is a branch-scoped write; same gate as tag
        // creation. Archived → `BranchArchived`; Deleted/missing →
        // `BranchNotFound`.
        let store = BranchControlStore::new(self.db.clone());
        let branch_rec = store.require_writable_by_name(branch)?;

        let deleted =
            branch_ops::delete_tag_with_expected(&self.db, branch, name, Some(branch_rec.branch))?;

        if deleted {
            let branch_id = branch_rec.branch.id;
            let branch_ref = branch_rec.branch;
            let event = BranchOpEvent {
                kind: BranchOpKind::Untag,
                branch_id,
                branch_ref,
                branch_name: Some(branch.to_string()),
                source_branch_id: None,
                source_branch_ref: None,
                source_branch_name: None,
                commit_version: None,
                tag_name: Some(name.to_string()),
                message: None,
                creator: None,
                merge_strategy: None,
                keys_applied: None,
                keys_deleted: None,
                keys_reverted: None,
                from_version: None,
                to_version: None,
            };
            self.db.branch_op_observers().notify(&event);
        }

        Ok(deleted)
    }

    /// List all tags on a branch.
    pub fn list_tags(&self, branch: &str) -> StrataResult<Vec<TagInfo>> {
        reject_system_branch(branch, "list tags on")?;
        branch_ops::list_tags(&self.db, branch)
    }

    /// Resolve a tag to its version.
    pub fn resolve_tag(&self, branch: &str, name: &str) -> StrataResult<Option<TagInfo>> {
        reject_system_branch(branch, "resolve tag on")?;
        branch_ops::resolve_tag(&self.db, branch, name)
    }

    // =========================================================================
    // Notes
    // =========================================================================

    /// Add a note to a specific version on a branch.
    ///
    /// Notes are not tracked by the branch-op observer pipeline since they are
    /// metadata annotations, not structural branch operations. Audit emission
    /// therefore happens directly here so the executor/API contract still
    /// exposes `branch.note` entries.
    pub fn add_note(
        &self,
        branch: &str,
        version: CommitVersion,
        message: &str,
        author: Option<&str>,
        metadata: Option<strata_core::Value>,
    ) -> StrataResult<NoteInfo> {
        reject_system_branch(branch, "add note to")?;

        // B4/KD4: notes are branch-scoped writes; same gate as tags.
        let branch_rec =
            BranchControlStore::new(self.db.clone()).require_writable_by_name(branch)?;

        let note = branch_ops::add_note_with_expected(
            &self.db,
            branch,
            version,
            message,
            author,
            metadata,
            Some(branch_rec.branch),
        )?;

        let system_branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let payload = strata_core::value::Value::object(
            [
                ("branch".to_string(), branch.into()),
                (
                    "branch_id".to_string(),
                    branch_rec.branch.id.to_string().into(),
                ),
                (
                    "branch_generation".to_string(),
                    strata_core::Value::Int(branch_rec.branch.generation as i64),
                ),
                (
                    "version".to_string(),
                    strata_core::Value::Int(version.0 as i64),
                ),
                ("message".to_string(), message.into()),
            ]
            .into_iter()
            .collect(),
        );

        if let Err(e) = EventLog::new(self.db.clone()).append(
            &system_branch_id,
            "default",
            "branch.note",
            payload,
        ) {
            tracing::warn!(
                target: "strata::audit",
                error = %e,
                branch,
                version = version.0,
                "Failed to emit branch.note audit event"
            );
        }

        Ok(note)
    }

    /// Get notes for a branch, optionally filtered by version.
    pub fn get_notes(&self, branch: &str, version: Option<u64>) -> StrataResult<Vec<NoteInfo>> {
        reject_system_branch(branch, "read notes from")?;
        branch_ops::get_notes(&self.db, branch, version)
    }

    /// Delete a note from a specific version on a branch.
    pub fn delete_note(&self, branch: &str, version: CommitVersion) -> StrataResult<bool> {
        reject_system_branch(branch, "delete note from")?;

        // B4/KD4: note deletion follows the same gate as creation.
        let branch_rec =
            BranchControlStore::new(self.db.clone()).require_writable_by_name(branch)?;

        branch_ops::delete_note_with_expected(&self.db, branch, version, Some(branch_rec.branch))
    }

    // =========================================================================
    // Test-only helpers (B4.2)
    // =========================================================================
    //
    // These are `#[doc(hidden)] pub` so the coverage-matrix tests in
    // `tests/integration/branching_lifecycle_*` can synthesize lifecycle
    // states no production path produces (most notably `Archived`) and
    // observe the control-store's lineage surface without leaking
    // `BranchControlStore` itself through the public API.
    //
    // The test hooks deliberately bypass the write gate — their whole
    // purpose is to set up state the gate is then asserted against. Do
    // not build product code on top of them.

    /// TEST-ONLY: overwrite the live `BranchControlRecord` for `name` with
    /// a new lifecycle status, bypassing the B4 write gate.
    ///
    /// Returns the `BranchRef` of the affected lifecycle instance (the
    /// generation is unchanged). Refuses if the branch has no live record
    /// — callers must create the branch first, then mutate its lifecycle.
    ///
    /// The update is atomic: the record row and the active-pointer row
    /// are written inside one transaction, matching the same
    /// `put_record` path the production create/delete paths take. When
    /// `status = Deleted`, the active pointer is cleared so subsequent
    /// `require_writable` / `require_visible` calls see a missing
    /// lifecycle (B4/KD1).
    ///
    /// Caveat: flipping to `Deleted` via this helper writes the control
    /// record but does NOT purge the legacy `BranchMetadata` the way
    /// `BranchService::delete` does. A subsequent `create` on the same
    /// name will observe leftover metadata and fail the
    /// metadata/control-store consistency check. Use
    /// `BranchService::delete` for Deleted state in realistic flows;
    /// reserve this helper for `Archived` (or round-tripping back to
    /// `Active` in tests that need idempotency).
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn set_lifecycle_for_test(
        &self,
        name: &str,
        status: BranchLifecycleStatus,
    ) -> StrataResult<BranchRef> {
        let store = BranchControlStore::new(self.db.clone());
        let record = store
            .find_active_by_name(name)?
            .ok_or_else(|| StrataError::branch_not_found_by_name(name))?;
        let branch_ref = record.branch;
        self.db
            .transaction(BranchId::from_bytes([0u8; 16]), |txn| {
                let mut updated = record.clone();
                updated.lifecycle = status;
                store.put_record(&updated, txn)
            })?;
        Ok(branch_ref)
    }

    /// TEST-ONLY: number of lineage edges whose `target` is the live
    /// lifecycle instance of `name`.
    ///
    /// Returns `Ok(0)` when `name` has no live record. Used by the B4.2
    /// coverage matrix to assert that a lifecycle-refused mutation
    /// appends no lineage edge to the target.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn lineage_edge_count_for_branch_ref_for_test(
        &self,
        branch: BranchRef,
    ) -> StrataResult<usize> {
        let store = BranchControlStore::new(self.db.clone());
        Ok(store.edges_for(branch)?.len())
    }

    /// TEST-ONLY: read the control record at a specific `BranchRef`
    /// regardless of lifecycle state.
    ///
    /// The production `control_record(name)` helper returns only the
    /// live record (via the active-pointer row). Tests that assert
    /// tombstone preservation — e.g. B4.4's delete-recreate cycle
    /// suite checking that each historical generation still has a
    /// `Deleted` record persisted — need to reach the underlying
    /// `BranchControlStore::get_record` by-ref path.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn control_record_for_ref_for_test(
        &self,
        branch: BranchRef,
    ) -> StrataResult<Option<BranchControlRecord>> {
        BranchControlStore::new(self.db.clone()).get_record(branch)
    }

    // =========================================================================
    // Internal Helpers
    // =========================================================================

    fn dag_hook(&self) -> &DagHookSlot {
        self.db.dag_hook()
    }
}

fn dag_to_strata(e: BranchDagError) -> StrataError {
    StrataError::invalid_input(e.message)
}

// =============================================================================
// Lineage edge writers (B3.3)
// =============================================================================
//
// Lineage edges live on the engine-global control-store namespace (nil
// UUID branch, "default" space) rather than the user's target branch,
// so edge writes use a transaction keyed on the global branch id — not
// the target branch's id. This matches how `BranchControlStore::put_record`
// participates in create / fork transactions, and keeps lineage writes
// out of the user branch's OCC read/write sets.

fn append_merge_edge(
    db: &Arc<Database>,
    store: &BranchControlStore,
    target: BranchRef,
    source: BranchRef,
    commit_version: CommitVersion,
    merge_base: Option<MergeBasePoint>,
) -> StrataResult<()> {
    let edge = LineageEdgeRecord {
        kind: LineageEdgeKind::Merge,
        target,
        source: Some(source),
        commit_version,
        merge_base,
    };
    db.transaction(BranchId::from_bytes([0u8; 16]), |txn| {
        store.append_edge(&edge, txn)
    })
}

fn append_revert_edge(
    db: &Arc<Database>,
    store: &BranchControlStore,
    target: BranchRef,
    commit_version: CommitVersion,
) -> StrataResult<()> {
    let edge = LineageEdgeRecord {
        kind: LineageEdgeKind::Revert,
        target,
        source: None,
        commit_version,
        merge_base: None,
    };
    db.transaction(BranchId::from_bytes([0u8; 16]), |txn| {
        store.append_edge(&edge, txn)
    })
}

fn append_cherry_pick_edge(
    db: &Arc<Database>,
    store: &BranchControlStore,
    target: BranchRef,
    source: BranchRef,
    commit_version: CommitVersion,
) -> StrataResult<()> {
    let edge = LineageEdgeRecord {
        kind: LineageEdgeKind::CherryPick,
        target,
        source: Some(source),
        commit_version,
        merge_base: None,
    };
    db.transaction(BranchId::from_bytes([0u8; 16]), |txn| {
        store.append_edge(&edge, txn)
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::database::dag_hook::{AncestryEntry, BranchDagHook, DagEventKind};
    use crate::database::dag_hook::{BranchDagError, MergeBaseResult as DagMergeBaseResult};
    use crate::database::OpenSpec;
    use crate::primitives::kv::KVStore;
    use strata_core::types::{Key, TypeTag};
    use strata_core::value::Value;
    use tempfile::TempDir;

    struct ToggleFailDagHook {
        fail_writes: AtomicBool,
        fail_after_record_once: AtomicBool,
        events: Mutex<Vec<DagEvent>>,
    }

    impl ToggleFailDagHook {
        fn new() -> Self {
            Self {
                fail_writes: AtomicBool::new(false),
                fail_after_record_once: AtomicBool::new(false),
                events: Mutex::new(Vec::new()),
            }
        }

        fn set_fail_writes(&self, fail: bool) {
            self.fail_writes.store(fail, Ordering::SeqCst);
        }

        fn set_fail_after_record_once(&self, fail: bool) {
            self.fail_after_record_once.store(fail, Ordering::SeqCst);
        }
    }

    impl BranchDagHook for ToggleFailDagHook {
        fn name(&self) -> &'static str {
            "toggle-fail"
        }

        fn record_event(&self, event: &DagEvent) -> Result<(), BranchDagError> {
            if self.fail_after_record_once.swap(false, Ordering::SeqCst) {
                self.events.lock().unwrap().push(event.clone());
                return Err(BranchDagError::write_failed(
                    "injected DAG partial write failure",
                ));
            }
            if self.fail_writes.load(Ordering::SeqCst) {
                Err(BranchDagError::write_failed("injected DAG write failure"))
            } else {
                self.events.lock().unwrap().push(event.clone());
                Ok(())
            }
        }

        fn reset_projection(&self) -> Result<(), BranchDagError> {
            self.events.lock().unwrap().clear();
            Ok(())
        }

        fn find_merge_base(
            &self,
            _branch_a: &str,
            _branch_b: &str,
        ) -> Result<Option<DagMergeBaseResult>, BranchDagError> {
            Ok(None)
        }

        fn log(&self, branch: &str, limit: usize) -> Result<Vec<DagEvent>, BranchDagError> {
            let mut events: Vec<DagEvent> = self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| {
                    event.branch_name == branch
                        || event.source_branch_name.as_deref() == Some(branch)
                })
                .cloned()
                .collect();
            events.truncate(limit);
            Ok(events)
        }

        fn ancestors(&self, _branch: &str) -> Result<Vec<AncestryEntry>, BranchDagError> {
            Ok(Vec::new())
        }
    }
    fn seed_legacy_branch_metadata(db: &Arc<Database>, name: &str) {
        let meta = BranchMetadata::new(name);
        let json = serde_json::to_string(&meta).unwrap();
        db.transaction(BranchId::from_bytes([0u8; 16]), |txn| {
            txn.put(
                Key::new(
                    Arc::new(strata_core::types::Namespace::for_branch(
                        BranchId::from_bytes([0u8; 16]),
                    )),
                    TypeTag::Branch,
                    name.as_bytes().to_vec(),
                ),
                Value::String(json),
            )
        })
        .unwrap();
    }

    fn force_follower_mode(db: &Arc<Database>) {
        let mut sig = db
            .runtime_signature()
            .expect("cache db has runtime signature");
        sig.mode = crate::database::DatabaseMode::Follower;
        db.set_runtime_signature(sig);
    }

    #[test]
    fn test_validate_branch_name() {
        // Valid names
        assert!(validate_branch_name("main").is_ok());
        assert!(validate_branch_name("feature-x").is_ok());
        assert!(validate_branch_name("feature/foo").is_ok());
        assert!(validate_branch_name("a").is_ok());
        assert!(validate_branch_name("f47ac10b-58cc-4372-a567-0e02b2c3d479").is_ok());

        // Invalid: empty
        assert!(validate_branch_name("").is_err());

        // Invalid: whitespace-only
        assert!(validate_branch_name("   ").is_err());
        assert!(validate_branch_name("\t").is_err());

        // Invalid: system prefix
        assert!(validate_branch_name("_system").is_err());
        assert!(validate_branch_name("_").is_err());

        // Invalid: too long
        assert!(validate_branch_name(&"x".repeat(256)).is_err());
        assert!(validate_branch_name(&"x".repeat(255)).is_ok());

        // Invalid: control characters
        assert!(validate_branch_name("foo\nbar").is_err());
        assert!(validate_branch_name("foo\x00bar").is_err());

        // Invalid: aliases the load-bearing default-branch nil UUID sentinel.
        assert!(validate_branch_name("00000000-0000-0000-0000-000000000000").is_err());
        assert!(validate_branch_name("00000000000000000000000000000000").is_err());
        let upper_nil = "00000000-0000-0000-0000-000000000000".to_uppercase();
        assert!(validate_branch_name(&upper_nil).is_err());
    }

    #[test]
    fn test_create_rejects_default_branch_nil_uuid_alias() {
        let db = Database::cache().unwrap();
        let err = db
            .branches()
            .create("00000000-0000-0000-0000-000000000000")
            .unwrap_err();
        assert!(matches!(err, StrataError::InvalidInput { .. }));
        assert!(err
            .to_string()
            .contains("aliases reserved default-branch sentinel"));
    }

    #[test]
    fn test_merge_options() {
        let opts = MergeOptions::with_strategy(MergeStrategy::Strict)
            .with_message("Merge feature")
            .with_creator("alice");

        assert_eq!(opts.strategy, MergeStrategy::Strict);
        assert_eq!(opts.message, Some("Merge feature".to_string()));
    }

    #[test]
    fn test_delete_default_branch_is_rejected_by_service() {
        let db = Database::open_runtime(
            OpenSpec::cache()
                .with_subsystem(crate::search::SearchSubsystem)
                .with_default_branch("main"),
        )
        .unwrap();
        let err = db.branches().delete("main").unwrap_err();
        assert!(matches!(err, StrataError::InvalidOperation { .. }));
    }

    #[test]
    fn test_literal_default_branch_is_not_special_without_runtime_config() {
        let db = Database::cache().unwrap();
        assert!(reject_default_branch(&db, "default", "delete").is_ok());
    }

    #[test]
    fn test_fork_rejects_system_source_before_dag_check() {
        let db = Database::cache().unwrap();
        let err = db.branches().fork("_system_", "feature").unwrap_err();
        assert!(matches!(err, StrataError::InvalidInput { .. }));
    }

    #[test]
    fn test_branch_service_propagates_lineage_unavailable_on_unmigrated_follower() {
        let db = Database::cache().unwrap();
        force_follower_mode(&db);
        seed_legacy_branch_metadata(&db, "legacy");

        let merge_err = db.branches().merge_base("legacy", "legacy").unwrap_err();
        assert!(merge_err.is_branch_lineage_unavailable());

        let log_err = db.branches().log("legacy", 10).unwrap_err();
        assert!(log_err.is_branch_lineage_unavailable());

        let ancestors_err = db.branches().ancestors("legacy").unwrap_err();
        assert!(ancestors_err.is_branch_lineage_unavailable());
    }

    #[test]
    fn test_retention_report_refuses_on_unmigrated_follower() {
        let db = Database::cache().unwrap();
        force_follower_mode(&db);
        seed_legacy_branch_metadata(&db, "legacy");

        let err = db.retention_report().unwrap_err();
        match err {
            StrataError::RetentionReportUnavailable { class } => {
                assert_eq!(class, "PolicyDowngrade");
            }
            other => panic!("expected RetentionReportUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn test_branch_service_history_reads_fail_closed_on_metadata_without_control_record() {
        let db = Database::cache().unwrap();
        seed_legacy_branch_metadata(&db, "legacy");

        let log_err = db.branches().log("legacy", 10).unwrap_err();
        assert!(matches!(log_err, StrataError::Corruption { .. }));
        assert!(log_err
            .to_string()
            .contains("legacy metadata but no active control record"));

        let ancestors_err = db.branches().ancestors("legacy").unwrap_err();
        assert!(matches!(ancestors_err, StrataError::Corruption { .. }));
        assert!(ancestors_err
            .to_string()
            .contains("legacy metadata but no active control record"));
    }

    #[test]
    fn test_history_reads_preserve_missing_branch_name_in_not_found() {
        let db = Database::cache().unwrap();

        let merge_err = db
            .branches()
            .merge_base("missing-a", "missing-b")
            .unwrap_err();
        assert!(
            matches!(merge_err, StrataError::BranchNotFoundByName { ref name, .. } if name == "missing-a"),
            "merge_base should preserve the user branch name, got {merge_err:?}"
        );

        let log_err = db.branches().log("missing-log", 10).unwrap_err();
        assert!(
            matches!(log_err, StrataError::BranchNotFoundByName { ref name, .. } if name == "missing-log"),
            "log should preserve the user branch name, got {log_err:?}"
        );

        let ancestors_err = db.branches().ancestors("missing-ancestors").unwrap_err();
        assert!(
            matches!(ancestors_err, StrataError::BranchNotFoundByName { ref name, .. } if name == "missing-ancestors"),
            "ancestors should preserve the user branch name, got {ancestors_err:?}"
        );
    }

    #[test]
    fn test_merge_dag_failure_rolls_back_prewritten_lineage_edge() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(
            OpenSpec::primary(temp_dir.path()).with_subsystem(crate::search::SearchSubsystem),
        )
        .unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        db.branches().create("main").unwrap();
        let kv = KVStore::new(db.clone());
        let main_id = BranchId::from_user_name("main");
        kv.put(&main_id, "default", "seed", Value::Int(1)).unwrap();
        let fork_info = db.branches().fork("main", "feature").unwrap();
        let feature_id = BranchId::from_user_name("feature");
        kv.put(&feature_id, "default", "delta", Value::Int(2))
            .unwrap();

        let main_ref = db
            .branches()
            .control_record("main")
            .unwrap()
            .unwrap()
            .branch;

        hook.set_fail_writes(true);
        let err = db.branches().merge("feature", "main").unwrap_err();
        assert!(
            err.to_string().contains("DAG")
                || err.to_string().contains("projection rebuild failed"),
            "expected DAG-related failure, got {err}"
        );

        let store = BranchControlStore::new(db.clone());
        assert!(
            store.edges_for(main_ref).unwrap().is_empty(),
            "rollback must delete the prewritten lineage edge when DAG recording fails"
        );

        let mb = db
            .branches()
            .merge_base("main", "feature")
            .unwrap()
            .unwrap();
        assert_eq!(
            mb.commit_version,
            fork_info.fork_version.map(CommitVersion).unwrap(),
            "failed merge must not advance the authoritative merge base"
        );
    }

    #[test]
    fn test_partial_dag_failure_rebuilds_projection_before_return() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(
            OpenSpec::primary(temp_dir.path()).with_subsystem(crate::search::SearchSubsystem),
        )
        .unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        db.branches().create("main").unwrap();
        let kv = KVStore::new(db.clone());
        let main_id = BranchId::from_user_name("main");
        kv.put(&main_id, "default", "seed", Value::Int(1)).unwrap();
        db.branches().fork("main", "feature").unwrap();
        let feature_id = BranchId::from_user_name("feature");
        kv.put(&feature_id, "default", "delta", Value::Int(2))
            .unwrap();

        hook.set_fail_after_record_once(true);
        let err = db.branches().merge("feature", "main").unwrap_err();
        assert!(err.to_string().contains("DAG write error"));

        let log = db.branches().log("main", 100).unwrap();
        assert!(
            !log.iter().any(|event| event.kind == DagEventKind::Merge),
            "failed merge must not leave a partial merge event visible in branch history"
        );
        assert!(
            log.iter()
                .any(|event| event.kind == DagEventKind::BranchCreate),
            "projection rebuild should preserve authoritative pre-existing history"
        );
    }

    // =========================================================================
    // B4.1: lifecycle write gate end-to-end smoke
    // =========================================================================
    //
    // Unit coverage for `require_writable_by_name` / `require_visible_by_name`
    // lives in `branch_control_store::tests`. These tests prove the gate is
    // actually wired into `BranchService` entry points; exhaustive
    // operation × lifecycle matrix coverage is B4.2.

    /// Helper: flip a branch's control record to `Archived` directly through
    /// the store. No public engine path produces `Archived` today (B4
    /// deferred the archive op).
    fn archive_branch_for_test(db: &Arc<Database>, name: &str) -> BranchRef {
        let store = BranchControlStore::new(db.clone());
        let rec = store
            .find_active_by_name(name)
            .unwrap()
            .expect("branch must exist before archiving");
        db.transaction(BranchId::from_bytes([0u8; 16]), |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: rec.branch,
                    name: rec.name.clone(),
                    lifecycle: BranchLifecycleStatus::Archived,
                    fork: rec.fork,
                },
                txn,
            )
        })
        .unwrap();
        rec.branch
    }

    #[test]
    fn test_merge_into_archived_target_returns_branch_archived() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(OpenSpec::primary(temp_dir.path())).unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook).unwrap();
        db.branches().create("main").unwrap();
        let kv = KVStore::new(db.clone());
        let main_id = BranchId::from_user_name("main");
        kv.put(&main_id, "default", "seed", Value::Int(1)).unwrap();
        db.branches().fork("main", "feature").unwrap();

        // Archive the merge target.
        archive_branch_for_test(&db, "main");

        let err = db
            .branches()
            .merge_with_options("feature", "main", MergeOptions::default())
            .unwrap_err();
        match err {
            StrataError::BranchArchived { ref name } => assert_eq!(name, "main"),
            other => panic!("expected BranchArchived on archived merge target, got {other:?}"),
        }
    }

    #[test]
    fn test_merge_from_archived_source_succeeds_archived_is_visible() {
        // KD-matrix: source needs `require_visible`; Archived is visible, so
        // merging *from* a frozen snapshot must still be allowed.
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(OpenSpec::primary(temp_dir.path())).unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook).unwrap();
        db.branches().create("main").unwrap();
        let kv = KVStore::new(db.clone());
        let main_id = BranchId::from_user_name("main");
        kv.put(&main_id, "default", "seed", Value::Int(1)).unwrap();
        db.branches().fork("main", "feature").unwrap();
        let feature_id = BranchId::from_user_name("feature");
        kv.put(&feature_id, "default", "feature-key", Value::Int(42))
            .unwrap();

        archive_branch_for_test(&db, "feature");

        // Merge archived source → active target. Gate allows this;
        // real work completes with an archived source snapshot.
        db.branches()
            .merge_with_options("feature", "main", MergeOptions::default())
            .expect("merge from archived source must succeed");
    }

    #[test]
    fn test_tag_on_archived_branch_returns_branch_archived() {
        // KD4: annotation writes follow the same gate as structural mutations.
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(OpenSpec::primary(temp_dir.path())).unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook).unwrap();
        db.branches().create("release").unwrap();
        archive_branch_for_test(&db, "release");

        let err = db
            .branches()
            .tag("release", "v1.0", None, None, None)
            .unwrap_err();
        assert!(
            matches!(err, StrataError::BranchArchived { ref name } if name == "release"),
            "expected BranchArchived on archived branch tag, got {err:?}"
        );
    }

    #[test]
    fn test_branch_note_audit_payload_carries_generation() {
        let db = Database::cache().unwrap();
        db.branches().create("release").unwrap();
        db.branches()
            .add_note("release", CommitVersion(1), "gen0", None, None)
            .unwrap();

        db.branches().delete("release").unwrap();
        db.branches().create("release").unwrap();
        db.branches()
            .add_note("release", CommitVersion(2), "gen1", None, None)
            .unwrap();

        let system_branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let events = EventLog::new(db.clone())
            .get_by_type(&system_branch_id, "default", "branch.note", None, None)
            .unwrap();
        assert_eq!(events.len(), 2, "expected one audit event per note write");

        let payload0 = events[0]
            .value
            .payload
            .as_object()
            .expect("branch.note payload must be an object");
        assert_eq!(
            payload0.get("branch").and_then(|v| v.as_str()),
            Some("release")
        );
        assert_eq!(
            payload0.get("branch_generation").and_then(|v| v.as_int()),
            Some(0)
        );
        assert!(payload0
            .get("branch_id")
            .and_then(|v| v.as_str())
            .is_some_and(|id| !id.is_empty()));

        let payload1 = events[1]
            .value
            .payload
            .as_object()
            .expect("branch.note payload must be an object");
        assert_eq!(
            payload1.get("branch_generation").and_then(|v| v.as_int()),
            Some(1)
        );
    }

    #[test]
    fn test_delete_on_archived_branch_is_allowed() {
        // Delete uses `require_visible`, not `require_writable`: archived
        // lifecycle instances remain deletable so operators can retire them.
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(OpenSpec::primary(temp_dir.path())).unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook).unwrap();
        db.branches().create("retired").unwrap();
        archive_branch_for_test(&db, "retired");

        db.branches()
            .delete("retired")
            .expect("delete must accept an archived target");
        let store = BranchControlStore::new(db.clone());
        assert!(
            store.find_active_by_name("retired").unwrap().is_none(),
            "delete of archived branch must clear the active pointer"
        );
    }

    #[test]
    fn test_delete_dag_failure_restores_branch_before_post_commit_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(OpenSpec::primary(temp_dir.path())).unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        db.branches().create("victim").unwrap();
        let kv = KVStore::new(db.clone());
        let victim_id = BranchId::from_user_name("victim");
        kv.put(&victim_id, "default", "seed", Value::Int(7))
            .unwrap();

        hook.set_fail_writes(true);
        let err = db.branches().delete("victim").unwrap_err();
        assert!(
            err.to_string().contains("DAG") || err.to_string().contains("write"),
            "expected DAG failure, got {err}"
        );

        let branch_index = BranchIndex::new(db.clone());
        assert!(
            branch_index.exists("victim").unwrap(),
            "delete must roll back the logical branch mutation when DAG recording fails"
        );

        let restored = kv
            .get(&victim_id, "default", "seed")
            .unwrap()
            .expect("post-commit storage cleanup must not run before DAG success");
        assert_eq!(restored, Value::Int(7));

        let store = BranchControlStore::new(db.clone());
        let active = store
            .find_active_by_name("victim")
            .unwrap()
            .expect("control record must be restored on DAG failure");
        assert_eq!(active.branch.generation, 0);
    }

    #[test]
    fn test_delete_post_commit_cleanup_failure_keeps_branch_deleted() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(OpenSpec::primary(temp_dir.path())).unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        db.branches().create("cleanup-debt").unwrap();
        let kv = KVStore::new(db.clone());
        let branch_id = BranchId::from_user_name("cleanup-debt");
        kv.put(&branch_id, "default", "seed", Value::Int(11))
            .unwrap();
        db.storage().rotate_memtable(&branch_id);
        db.storage()
            .flush_oldest_frozen(&branch_id)
            .expect("setup flush succeeds");

        crate::database::test_hooks::clear_clear_branch_storage_failure(temp_dir.path());
        crate::database::test_hooks::inject_clear_branch_storage_failure(
            temp_dir.path(),
            std::io::ErrorKind::Other,
        );

        db.branches()
            .delete("cleanup-debt")
            .expect("logical delete must stay committed even if post-commit cleanup fails");
        crate::database::test_hooks::clear_clear_branch_storage_failure(temp_dir.path());

        let branch_index = BranchIndex::new(db.clone());
        assert!(
            !branch_index.exists("cleanup-debt").unwrap(),
            "branch must remain logically deleted after cleanup debt"
        );

        let store = BranchControlStore::new(db.clone());
        assert!(
            store.find_active_by_name("cleanup-debt").unwrap().is_none(),
            "active control record must stay cleared after committed delete"
        );

        let report = db
            .retention_report()
            .expect("cleanup debt after committed delete must surface as orphan storage");
        let orphan = report
            .orphan_storage
            .iter()
            .find(|entry| entry.branch_id == branch_id)
            .expect("cleanup failure should leave orphaned storage debt behind");
        assert!(
            orphan.bytes > 0 || orphan.quarantined_bytes > 0,
            "cleanup debt must keep retained bytes visible to retention reporting",
        );

        let dag_events = hook.events.lock().unwrap().clone();
        let delete_events = dag_events
            .iter()
            .filter(|event| {
                event.kind == DagEventKind::BranchDelete && event.branch_name == "cleanup-debt"
            })
            .count();
        assert_eq!(
            delete_events, 1,
            "committed delete with cleanup debt must still record exactly one delete event"
        );
    }

    #[test]
    fn test_merge_expected_refs_conflict_after_source_recreate() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(
            OpenSpec::primary(temp_dir.path()).with_subsystem(crate::search::SearchSubsystem),
        )
        .unwrap();
        let hook = Arc::new(ToggleFailDagHook::new());
        db.install_dag_hook(hook).unwrap();
        db.branches().create("main").unwrap();
        let kv = KVStore::new(db.clone());
        let main_id = BranchId::from_user_name("main");
        kv.put(&main_id, "default", "seed", Value::Int(1)).unwrap();
        db.branches().fork("main", "feature").unwrap();

        let store = BranchControlStore::new(db.clone());
        let target_ref = store.find_active_by_name("main").unwrap().unwrap().branch;
        let stale_source_ref = store
            .find_active_by_name("feature")
            .unwrap()
            .unwrap()
            .branch;

        db.branches().delete("feature").unwrap();
        db.branches().create("feature").unwrap();

        let err = crate::branch_ops::merge_branches_with_metadata_expected(
            &db,
            "feature",
            "main",
            MergeOptions::default().strategy,
            None,
            None,
            Some(stale_source_ref),
            Some(target_ref),
        )
        .unwrap_err();

        assert!(
            matches!(err, StrataError::Conflict { .. }),
            "expected stale BranchRef mismatch to fail as a conflict, got {err:?}"
        );
        assert!(
            err.to_string().contains("lifecycle advanced"),
            "conflict should explain that the branch generation changed"
        );
    }
}
