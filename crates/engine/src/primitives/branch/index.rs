//! BranchIndex: Branch lifecycle management
//!
//! BranchIndex tracks which branches exist. Branches are created or deleted.
//! Export/import is handled by the bundle module.
//!
//! ## MVP Methods
//!
//! - `create_branch(name)` - Create a new branch
//! - `get_branch(name)` - Get branch metadata
//! - `exists(name)` - Check if branch exists
//! - `list_branches()` - List all branch names
//! - `delete_branch(name)` - Delete branch and ALL its data (cascading)
//!
//! ## Key Design
//!
//! - TypeTag: Run (0x05)
//! - Primary key format: `<global_namespace>:<TypeTag::Branch>:<branch_id>`
//! - BranchIndex uses a global namespace (not branch-scoped) since it manages branches themselves.

use crate::branch_ops::dag_hooks::{dispatch_create_hook, dispatch_delete_hook};
use crate::branch_ops::{self, branch_control_store::is_control_store_key};
use crate::database::Database;
use crate::{StrataError, StrataResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_storage::StorageError;
use strata_storage::{Key, Namespace, TypeTag};
use tracing::{info, warn};

/// Internal metadata key storing the effective default branch name for
/// disk-backed databases. This lets follower and reopened primary handles
/// recover branch semantics across process boundaries.
const DEFAULT_BRANCH_MARKER_KEY: &str = "__default_branch__";

/// Resolve a branch name to a core `BranchId`.
///
/// Passthrough to the canonical [`BranchId::from_user_name`] in
/// `strata_core::branch` (B2 collapsed the duplicated engine/executor
/// derivations). Kept as a free function so existing engine call sites
/// read unchanged.
pub fn resolve_branch_name(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

/// `true` if `name` resolves to the nil-UUID default-branch sentinel without
/// being the literal `"default"` branch name.
///
/// These aliases are forbidden at user-facing boundaries because they collide
/// with the reserved nil-UUID branch sentinel used by the literal `"default"`
/// branch and branch-control paths.
pub(crate) fn aliases_default_branch_sentinel(name: &str) -> bool {
    strata_core::branch::aliases_default_branch_sentinel(name)
}

/// Reject persisted branch-control artifacts that still address the reserved
/// nil-UUID branch sentinel by a non-literal alias.
///
/// This quarantines historical bad state from older builds before branch
/// operations can resolve the alias back to the nil UUID and touch the wrong
/// namespace during open/reopen. Runs on primary, follower, and cache
/// open paths, so scans go through the read-only storage surface rather
/// than `db.transaction()` (followers cannot commit).
pub(crate) fn validate_reserved_branch_aliases(db: &Arc<Database>) -> StrataResult<()> {
    if let Some(name) = read_default_branch_marker(db.as_ref())? {
        if aliases_default_branch_sentinel(&name) {
            return Err(StrataError::corruption(format!(
                "persisted default branch marker uses reserved default-branch sentinel alias '{name}'"
            )));
        }
    }

    let prefix = Key::new_branch_with_id(global_namespace(), "");
    let rows = db.storage().scan_prefix(&prefix, CommitVersion::MAX)?;
    for (key, _) in rows {
        // Control-store rows (records, counters, active-pointer index,
        // lineage edges) live in this scan range. Skip them before
        // attempting to interpret the user_key as a branch name.
        if is_control_store_key(&key.user_key) {
            continue;
        }
        let Ok(name) = String::from_utf8(key.user_key.to_vec()) else {
            continue;
        };
        if name.contains("__idx_")
            || name == DEFAULT_BRANCH_MARKER_KEY
            || strata_core::branch_dag::is_system_branch(&name)
        {
            continue;
        }
        if aliases_default_branch_sentinel(&name) {
            return Err(StrataError::corruption(format!(
                "branch metadata contains reserved default-branch sentinel alias '{name}'"
            )));
        }
    }

    Ok(())
}

// ========== Global Branch ID for BranchIndex Operations ==========

/// Get the global BranchId used for BranchIndex operations
///
/// BranchIndex is a global index (not scoped to any particular branch),
/// so we use a nil UUID as a sentinel value.
fn global_branch_id() -> BranchId {
    BranchId::from_bytes([0; 16])
}

/// Get the global namespace for BranchIndex operations
fn global_namespace() -> Arc<Namespace> {
    Arc::new(Namespace::for_branch(global_branch_id()))
}

/// Internal transaction branch id used for global branch-index metadata work.
///
/// Most delete operations can run on the global/nil branch because the target
/// branch lock is distinct and the global branch id is convenient. Literal
/// `"default"` is special: it resolves to the same nil BranchId used by
/// global branch-index operations. In that case, using the nil branch for an
/// internal metadata transaction can self-conflict with delete marks / commit
/// locks that intentionally remain on the real branch while a lifecycle
/// cutover is in progress. Route the internal metadata transaction through
/// `_system_` instead while leaving the target delete mark/lock on the actual
/// branch being created/deleted.
fn admin_transaction_branch_id(target_branch_id: BranchId) -> BranchId {
    if target_branch_id == global_branch_id() {
        resolve_branch_name(crate::SYSTEM_BRANCH)
    } else {
        global_branch_id()
    }
}

pub(crate) fn default_branch_marker_key() -> Key {
    Key::new_branch_with_id(global_namespace(), DEFAULT_BRANCH_MARKER_KEY)
}

pub(crate) fn read_default_branch_marker(db: &Database) -> StrataResult<Option<String>> {
    match db
        .storage()
        .get_versioned(&default_branch_marker_key(), CommitVersion::MAX)?
    {
        Some(entry) => match entry.value {
            Value::String(name) => Ok(Some(name)),
            _ => Err(StrataError::serialization(
                "default branch marker must be stored as a string",
            )),
        },
        None => Ok(None),
    }
}

pub(crate) fn write_default_branch_marker(
    db: &Arc<Database>,
    branch_name: &str,
) -> StrataResult<()> {
    db.transaction(global_branch_id(), |txn| {
        txn.put(
            default_branch_marker_key(),
            Value::String(branch_name.to_string()),
        )?;
        Ok(())
    })
}

pub(crate) fn clear_default_branch_marker_if(
    db: &Arc<Database>,
    branch_name: &str,
) -> StrataResult<()> {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(inner) = Database::take_clear_default_branch_marker_failure_for_test(db.data_dir())
    {
        return Err(StrataError::from(strata_storage::StorageError::Io(inner)));
    }

    db.transaction(global_branch_id(), |txn| {
        let key = default_branch_marker_key();
        let should_clear =
            matches!(txn.get(&key)?, Some(Value::String(name)) if name == branch_name);
        if should_clear {
            txn.delete(key)?;
        }
        Ok(())
    })
}

// ========== BranchStatus Enum ==========

/// Branch lifecycle status.
///
/// All branches are Active. Additional statuses will be added when
/// lifecycle transitions are implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum BranchStatus {
    /// Branch is currently active
    #[default]
    Active,
}

impl BranchStatus {
    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            BranchStatus::Active => "Active",
        }
    }
}

// ========== BranchMetadata Struct ==========

/// Metadata about a branch
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BranchMetadata {
    /// User-provided name/key for the branch (used for lookups)
    pub name: String,
    /// Unique branch identifier (UUID) for internal use and namespacing
    pub branch_id: String,
    /// Parent branch name if forked (post-MVP)
    pub parent_branch: Option<String>,

    /// Current status
    pub status: BranchStatus,
    /// Creation timestamp (microseconds since epoch)
    pub created_at: Timestamp,
    /// Last update timestamp (microseconds since epoch)
    pub updated_at: Timestamp,
    /// Completion timestamp (post-MVP)
    pub completed_at: Option<Timestamp>,
    /// Error message if failed (post-MVP)
    pub error: Option<String>,
    /// Internal version counter
    #[serde(default = "default_version")]
    pub version: u64,
}

fn default_version() -> u64 {
    1
}

impl BranchMetadata {
    /// Create new branch metadata with Active status
    pub fn new(name: &str) -> Self {
        let now = Self::now();
        let branch_id = BranchId::new();
        Self {
            name: name.to_string(),
            branch_id: branch_id.to_string(),
            parent_branch: None,
            status: BranchStatus::Active,
            created_at: now,
            updated_at: now,
            completed_at: None,
            error: None,
            version: 1,
        }
    }

    /// Wrap this metadata in a Versioned container
    pub fn into_versioned(self) -> Versioned<BranchMetadata> {
        let version = self.version;
        let timestamp = self.updated_at;
        Versioned::with_timestamp(self, Version::counter(version), timestamp)
    }

    /// Get current timestamp in microseconds
    fn now() -> Timestamp {
        Timestamp::now()
    }
}

// ========== Serialization Helpers ==========

/// Serialize a struct to Value::String for storage
fn to_stored_value<T: Serialize>(v: &T) -> StrataResult<Value> {
    serde_json::to_string(v)
        .map(Value::String)
        .map_err(|e| StrataError::serialization(e.to_string()))
}

/// Deserialize from Value::String storage
fn from_stored_value<T: for<'de> Deserialize<'de>>(
    v: &Value,
) -> std::result::Result<T, serde_json::Error> {
    match v {
        Value::String(s) => serde_json::from_str(s),
        _ => serde_json::from_str("null"),
    }
}

// ========== BranchIndex Core ==========

/// Branch lifecycle management primitive (MVP)
///
/// ## Design
///
/// BranchIndex provides branch lifecycle management. It is a stateless facade
/// over the Database engine, holding only an `Arc<Database>` reference.
///
/// ## MVP Methods
///
/// - `create_branch()` - Create a new branch
/// - `get_branch()` - Get branch metadata
/// - `exists()` - Check if branch exists
/// - `list_branches()` - List all branchs
/// - `delete_branch()` - Delete branch and all its data
///
/// ## Example
///
/// ```text
/// let ri = BranchIndex::new(db.clone());
///
/// // Create a branch
/// let meta = ri.create_branch("my-run")?;
/// assert_eq!(meta.value.status, BranchStatus::Active);
///
/// // Check existence
/// assert!(ri.exists("my-run")?);
///
/// // Delete it
/// ri.delete_branch("my-run")?;
/// ```
#[derive(Clone)]
pub(crate) struct BranchIndex {
    db: Arc<Database>,
}

impl BranchIndex {
    /// Create new BranchIndex instance
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Build key for branch metadata
    fn key_for(&self, branch_id: &str) -> Key {
        Key::new_branch_with_id(global_namespace(), branch_id)
    }

    // ========== MVP Methods ==========

    /// Create a new branch
    ///
    /// Creates a branch with Active status.
    ///
    /// ## Errors
    /// - `InvalidInput` if branch already exists
    pub(crate) fn create_branch(&self, branch_id: &str) -> StrataResult<Versioned<BranchMetadata>> {
        self.create_branch_with_hook(branch_id, |_| Ok(()))
    }

    /// Create a new branch, running `pre_commit` inside the same KV
    /// transaction that writes the legacy `BranchMetadata` row.
    ///
    /// Used by `BranchService::create` (B3.2) to write the new
    /// `BranchControlRecord` atomically with the metadata row. The hook
    /// runs after the metadata is staged; if it returns an error the
    /// entire transaction rolls back.
    pub(crate) fn create_branch_with_hook<F>(
        &self,
        branch_id: &str,
        pre_commit: F,
    ) -> StrataResult<Versioned<BranchMetadata>>
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<()>,
    {
        if aliases_default_branch_sentinel(branch_id) {
            return Err(StrataError::invalid_input(
                "branch name aliases reserved default-branch sentinel",
            ));
        }

        let txn_branch_id = admin_transaction_branch_id(resolve_branch_name(branch_id));
        let result = self.db.transaction(txn_branch_id, |txn| {
            txn.set_allow_cross_branch(true);
            self.create_branch_in_txn(branch_id, txn)
                .and_then(|versioned| pre_commit(txn).map(|_| versioned))
        })?;

        // Note: DAG recording is handled by BranchService.create(), which is
        // the canonical branch creation path and suppresses this best-effort
        // low-level dispatch so it can record through the load-bearing
        // BranchMutation boundary instead.
        dispatch_create_hook(&self.db, branch_id);

        Ok(result)
    }

    /// Create a new branch's legacy `BranchMetadata` row inside an
    /// externally-owned transaction.
    ///
    /// Low-level entry point used by fork and hooked-create paths that
    /// need to batch the metadata write with additional control-store
    /// writes in one atomic commit (B3.2). Does not dispatch any DAG
    /// hooks — that is the caller's responsibility.
    ///
    /// ## Errors
    /// - `InvalidInput` if branch already exists or name aliases the
    ///   reserved default-branch sentinel.
    pub(crate) fn create_branch_in_txn(
        &self,
        branch_id: &str,
        txn: &mut TransactionContext,
    ) -> StrataResult<Versioned<BranchMetadata>> {
        if aliases_default_branch_sentinel(branch_id) {
            return Err(StrataError::invalid_input(
                "branch name aliases reserved default-branch sentinel",
            ));
        }

        let key = self.key_for(branch_id);
        if txn.get(&key)?.is_some() {
            return Err(StrataError::invalid_input(format!(
                "Branch '{}' already exists",
                branch_id
            )));
        }

        let branch_meta = BranchMetadata::new(branch_id);
        txn.put(key, to_stored_value(&branch_meta)?)?;

        info!(target: "strata::branch", %branch_id, "Branch created");
        Ok(branch_meta.into_versioned())
    }

    /// Get branch metadata
    ///
    /// ## Returns
    /// - `Some(Versioned<metadata>)` if branch exists
    /// - `None` if branch doesn't exist
    pub(crate) fn get_branch(
        &self,
        branch_id: &str,
    ) -> StrataResult<Option<Versioned<BranchMetadata>>> {
        self.db.transaction(global_branch_id(), |txn| {
            let key = self.key_for(branch_id);
            match txn.get(&key)? {
                Some(v) => {
                    let meta: BranchMetadata = from_stored_value(&v)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    Ok(Some(meta.into_versioned()))
                }
                None => Ok(None),
            }
        })
    }

    /// Check if a branch exists
    pub(crate) fn exists(&self, branch_id: &str) -> StrataResult<bool> {
        self.db.transaction(global_branch_id(), |txn| {
            let key = self.key_for(branch_id);
            Ok(txn.get(&key)?.is_some())
        })
    }

    /// List all branch IDs
    pub(crate) fn list_branches(&self) -> StrataResult<Vec<String>> {
        self.db.transaction(global_branch_id(), |txn| {
            let prefix = Key::new_branch_with_id(global_namespace(), "");
            let results = txn.scan_prefix(&prefix)?;

            Ok(results
                .into_iter()
                .filter_map(|(k, _)| {
                    // Control-store rows share this scan range. Skip them
                    // before even decoding as UTF-8 — edge keys include
                    // raw generation / commit-version suffixes that are
                    // never user-facing branch names.
                    if is_control_store_key(&k.user_key) {
                        return None;
                    }
                    let key_str = String::from_utf8(k.user_key.to_vec()).ok()?;
                    // Filter out index keys (legacy) and system branches
                    if key_str.contains("__idx_")
                        || key_str == DEFAULT_BRANCH_MARKER_KEY
                        || strata_core::branch_dag::is_system_branch(&key_str)
                    {
                        None
                    } else {
                        Some(key_str)
                    }
                })
                .collect())
        })
    }

    /// Delete a branch and ALL its data (cascading delete)
    ///
    /// This deletes:
    /// - The branch metadata
    /// - All branch-scoped data (KV, Events, JSON, Vectors)
    ///
    /// USE WITH CAUTION - this is irreversible!
    pub(crate) fn delete_branch(&self, branch_id: &str) -> StrataResult<()> {
        let committed = self.delete_branch_with_hook(branch_id, |_| Ok(()))?;
        match self.complete_delete_post_commit(
            branch_id,
            &committed,
            DeletePostCommitOptions {
                clear_storage: true,
                dispatch_best_effort_delete_hook: true,
            },
        ) {
            DeleteBranchCompletion::Clean | DeleteBranchCompletion::Warning(_) => Ok(()),
            DeleteBranchCompletion::PostCommitError(err) => Err(err),
        }
    }

    /// Delete a branch logically, running `pre_commit` inside the same KV
    /// transaction that purges the legacy metadata + namespace data.
    ///
    /// Used by `BranchService::delete` (B3.2) to atomically mark the
    /// corresponding `BranchControlRecord` as `Deleted` alongside the
    /// legacy purge. The hook runs after namespace data is staged for
    /// removal and before the metadata row is dropped; if it errors the
    /// entire delete transaction rolls back and the branch remains live.
    ///
    /// This method stops at the committed logical-delete boundary. Any
    /// storage cleanup, subsystem cleanup, or best-effort delete-hook
    /// dispatch must run later through [`BranchIndex::complete_delete_post_commit`].
    pub(crate) fn delete_branch_with_hook<F>(
        &self,
        branch_id: &str,
        pre_commit: F,
    ) -> StrataResult<DeleteBranchCommitted>
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<()>,
    {
        // First get the branch metadata (read-only, no WAL after #970)
        let branch_meta = self
            .get_branch(branch_id)?
            .ok_or_else(|| StrataError::invalid_input(format!("Branch '{}' not found", branch_id)))?
            .value;

        // Resolve the executor's deterministic BranchId for this name.
        let executor_branch_id = resolve_branch_name(branch_id);

        // Also get the metadata BranchId (random UUID from BranchMetadata::new).
        let metadata_branch_id = BranchId::from_string(&branch_meta.branch_id);

        let meta_key = self.key_for(branch_id);

        // --- #1916: Quiesce the target branch before deletion ---
        //
        // 1. Mark as deleting → future commits on this branch are rejected.
        // 2. Acquire the target branch's commit lock → waits for any
        //    in-flight commit to finish.  While we hold the lock no new
        //    commit can start (the mark rejects them after they acquire
        //    the lock, so the lock is released immediately).
        // 3. Run the delete transaction (on global_branch_id, cross-branch).
        // 4. Hand the committed delete to the post-commit cleanup phase.
        // 5. Post-commit cleanup clears storage and releases the lock entry.
        self.db.mark_branch_deleting(&executor_branch_id);
        if let Some(meta_id) = metadata_branch_id {
            if meta_id != executor_branch_id {
                self.db.mark_branch_deleting(&meta_id);
            }
        }
        let target_lock = self.db.branch_commit_lock(&executor_branch_id);
        let _target_guard = target_lock.lock();

        let txn_branch_id = admin_transaction_branch_id(executor_branch_id);
        let result = self.db.transaction(txn_branch_id, |txn| {
            txn.set_allow_cross_branch(true);
            // Delete data from the executor's namespace
            Self::delete_namespace_data(txn, executor_branch_id)?;

            // If the metadata BranchId differs, also delete from that namespace
            if let Some(meta_id) = metadata_branch_id {
                if meta_id != executor_branch_id {
                    Self::delete_namespace_data(txn, meta_id)?;
                }
            }

            // Branch-scoped annotations live under the `_system_` branch, not
            // under the user branch namespace. Clear them in the same
            // transaction so same-name recreate cannot inherit stale tags or
            // notes from the prior lifecycle instance.
            branch_ops::delete_annotations_for_branch_in_txn(txn, branch_id)?;

            // Delete the branch metadata entry
            txn.delete(meta_key.clone())?;

            // B3.2: run the caller-supplied hook (e.g. mark_deleted on the
            // BranchControlRecord) inside the same transaction so legacy
            // metadata purge and control-record lifecycle flip commit
            // together.
            pre_commit(txn)?;

            info!(target: "strata::branch", %branch_id, "Branch deleted");
            Ok(())
        });

        if let Err(e) = result {
            // Roll back the deleting mark on failure so the branch
            // remains usable.
            self.db.unmark_branch_deleting(&executor_branch_id);
            if let Some(meta_id) = metadata_branch_id {
                if meta_id != executor_branch_id {
                    self.db.unmark_branch_deleting(&meta_id);
                }
            }
            return Err(e);
        }

        // Evict cached namespaces for the deleted branch to prevent unbounded
        // growth of the global NS_CACHE (SCALE-001).
        crate::primitives::kv::invalidate_kv_namespace_cache(&executor_branch_id);
        crate::system_space::invalidate_cache(&executor_branch_id);

        Ok(DeleteBranchCommitted { executor_branch_id })
    }

    /// Complete the post-commit cleanup phase for a logically deleted branch.
    ///
    /// This runs after the delete transaction has committed and therefore must
    /// not be treated as rollbackable logical state. It may report cleanup
    /// debt via [`DeleteBranchCompletion::Warning`] or
    /// [`DeleteBranchCompletion::PostCommitError`], but the branch is already
    /// logically deleted at this point.
    pub(crate) fn complete_delete_post_commit(
        &self,
        branch_id: &str,
        committed: &DeleteBranchCommitted,
        options: DeletePostCommitOptions,
    ) -> DeleteBranchCompletion {
        let executor_branch_id = committed.executor_branch_id;
        let mut completion = DeleteBranchCompletion::Clean;

        if let Err(err) = clear_default_branch_marker_if(&self.db, branch_id) {
            warn!(
                target: "strata::branch",
                branch = branch_id,
                error = %err,
                "Failed to clear persisted default-branch marker after committed delete"
            );
            completion.record_error(err);
        }

        if options.clear_storage {
            // Drain background tasks before clearing storage. The delete
            // transaction above commits writes that schedule flush/compaction
            // via `schedule_flush_if_needed`. If compaction runs concurrently
            // with clear_branch_storage, it can leave `.tmp` files mid-write
            // or create new `.sst` files that our cleanup enumeration missed.
            //
            // Limitation: drain() waits for all scheduled tasks, not just
            // tasks for this branch. In a busy multi-branch deployment with
            // continuous writes, delete_branch may block while other branches
            // finish their own compaction rounds. Acceptable for an admin
            // operation; if it becomes a concern, add a drain_with_timeout API.
            self.db.scheduler().drain();

            // Clean up storage-layer segments, manifest, and refcounts (#1702).
            // Must happen after logical deletion so in-progress reads see the
            // deletion before files disappear.
            //
            // Literal `"default"` is special: its canonical BranchId is the same
            // nil branch id used by global branch-index/control-store metadata.
            // The delete transaction above already tombstones user primitive rows
            // in that namespace. A physical `clear_branch_storage(nil)` would also
            // wipe the branch-control ledger itself (next-generation counters,
            // active pointers, metadata rows for other branches), breaking
            // generation-safe recreate. Keep nil/default deletion logical-only at
            // the storage layer; later compaction/GC will reclaim the user data.
            if executor_branch_id != global_branch_id() {
                match self.db.clear_branch_storage_result(&executor_branch_id) {
                    Ok(_) => {}
                    Err(StorageError::DirFsync { dir, inner }) => {
                        let warning = StrataError::from(StorageError::DirFsync { dir, inner });
                        warn!(
                            target: "strata::branch",
                            branch = branch_id,
                            error = %warning,
                            "Logical delete committed; storage cleanup completed with unconfirmed manifest durability",
                        );
                        completion.record_warning(warning);
                    }
                    Err(storage_err) => {
                        let err = StrataError::from(storage_err);
                        warn!(
                            target: "strata::branch",
                            branch = branch_id,
                            error = %err,
                            "Logical delete committed, but storage cleanup failed after commit",
                        );
                        completion.record_error(err);
                    }
                }
            }
        }

        let subsystems = self.db.installed_subsystems();
        for subsystem in subsystems.iter() {
            if let Err(err) =
                subsystem.cleanup_deleted_branch(&self.db, &executor_branch_id, branch_id)
            {
                warn!(
                    target: "strata::branch",
                    subsystem = subsystem.name(),
                    branch = branch_id,
                    error = %err,
                    "Subsystem branch cleanup failed after delete"
                );
                completion.record_error(err);
            }
        }

        // B5.4 — clear any primitive-degradation entries attributed to
        // the deleted BranchId so a same-name recreate starts with a
        // clean registry (retention contract §"Same-name recreate").
        // Post-commit best-effort: failure here is logged but not
        // raised — consistent with the surrounding cleanup pattern.
        if let Ok(registry) =
            self.db
                .extension::<crate::database::primitive_degradation::PrimitiveDegradationRegistry>()
        {
            registry.clear_branch(executor_branch_id);
        }

        // Remove commit lock entry. The deleting mark is intentionally
        // kept: any pre-existing transaction that tries to commit on
        // this branch after deletion will be rejected (#1916).
        self.db.remove_branch_lock(&executor_branch_id);

        if options.dispatch_best_effort_delete_hook {
            dispatch_delete_hook(&self.db, branch_id);
        }

        completion
    }

    /// Delete all branch-scoped data within an existing transaction context.
    fn delete_namespace_data(
        txn: &mut strata_concurrency::TransactionContext,
        branch_id: BranchId,
    ) -> StrataResult<()> {
        let ns = Arc::new(Namespace::for_branch(branch_id));

        for type_tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::Json,
            TypeTag::Vector,
            TypeTag::Graph,
        ] {
            let prefix = Key::new(ns.clone(), type_tag, vec![]);
            let entries = txn.scan_prefix(&prefix)?;

            for (key, _) in entries {
                txn.delete(key)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct DeleteBranchCommitted {
    pub(crate) executor_branch_id: BranchId,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeletePostCommitOptions {
    pub(crate) clear_storage: bool,
    pub(crate) dispatch_best_effort_delete_hook: bool,
}

#[derive(Debug)]
pub(crate) enum DeleteBranchCompletion {
    Clean,
    Warning(StrataError),
    PostCommitError(StrataError),
}

impl DeleteBranchCompletion {
    fn record_warning(&mut self, warning: StrataError) {
        if matches!(self, Self::Clean) {
            *self = Self::Warning(warning);
        }
    }

    fn record_error(&mut self, err: StrataError) {
        if !matches!(self, Self::PostCommitError(_)) {
            *self = Self::PostCommitError(err);
        }
    }
}

// ========== Searchable Trait Implementation ==========
//
// Search is handled by the intelligence layer.
// This implementation returns empty results.

impl crate::search::Searchable for BranchIndex {
    fn search(&self, _req: &crate::SearchRequest) -> StrataResult<crate::SearchResponse> {
        // Search moved to intelligence layer - return empty results
        Ok(crate::SearchResponse::empty())
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Branch
    }
}

// ========== Tests ==========

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::OpenSpec;
    use crate::Subsystem;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, BranchIndex) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let ri = BranchIndex::new(db.clone());
        (temp_dir, db, ri)
    }

    struct FailingCleanupSubsystem;

    impl Subsystem for FailingCleanupSubsystem {
        fn name(&self) -> &'static str {
            "failing-cleanup"
        }

        fn recover(&self, _db: &Arc<Database>) -> StrataResult<()> {
            Ok(())
        }

        fn cleanup_deleted_branch(
            &self,
            _db: &Arc<Database>,
            _branch_id: &BranchId,
            _branch_name: &str,
        ) -> StrataResult<()> {
            Err(StrataError::internal(
                "injected cleanup_deleted_branch failure".to_string(),
            ))
        }
    }

    #[test]
    fn test_create_branch() {
        let (_temp, _db, ri) = setup();

        let result = ri.create_branch("test-run").unwrap();
        assert_eq!(result.value.name, "test-run");
        assert_eq!(result.value.status, BranchStatus::Active);
    }

    #[test]
    fn test_create_branch_duplicate_fails() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("test-run").unwrap();
        let result = ri.create_branch("test-run");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_branch_rejects_default_branch_nil_uuid_alias() {
        let (_temp, _db, ri) = setup();

        let err = ri
            .create_branch("00000000-0000-0000-0000-000000000000")
            .unwrap_err();
        assert!(matches!(err, StrataError::InvalidInput { .. }));
        assert!(err
            .to_string()
            .contains("aliases reserved default-branch sentinel"));
    }

    #[test]
    fn test_validate_reserved_branch_aliases_rejects_historical_branch_metadata() {
        let (_temp, _db, ri) = setup();
        let bad_name = "00000000-0000-0000-0000-000000000000";

        ri.db
            .transaction(global_branch_id(), |txn| {
                txn.put(
                    ri.key_for(bad_name),
                    to_stored_value(&BranchMetadata::new(bad_name))?,
                )?;
                Ok(())
            })
            .unwrap();

        let err = validate_reserved_branch_aliases(&ri.db).unwrap_err();
        assert!(matches!(err, StrataError::Corruption { .. }));
        assert!(err
            .to_string()
            .contains("branch metadata contains reserved default-branch sentinel alias"));
    }

    #[test]
    fn test_validate_reserved_branch_aliases_rejects_historical_default_marker() {
        let (_temp, _db, ri) = setup();
        let bad_name = "00000000-0000-0000-0000-000000000000";

        write_default_branch_marker(&ri.db, bad_name).unwrap();

        let err = validate_reserved_branch_aliases(&ri.db).unwrap_err();
        assert!(matches!(err, StrataError::Corruption { .. }));
        assert!(err.to_string().contains(
            "persisted default branch marker uses reserved default-branch sentinel alias"
        ));
    }

    #[test]
    fn test_get_branch() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("test-run").unwrap();

        let result = ri.get_branch("test-run").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value.name, "test-run");
    }

    #[test]
    fn test_get_branch_not_found() {
        let (_temp, _db, ri) = setup();

        let result = ri.get_branch("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_exists() {
        let (_temp, _db, ri) = setup();

        assert!(!ri.exists("test-run").unwrap());

        ri.create_branch("test-run").unwrap();
        assert!(ri.exists("test-run").unwrap());
    }

    #[test]
    fn test_list_branches() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("run-a").unwrap();
        ri.create_branch("run-b").unwrap();
        ri.create_branch("run-c").unwrap();

        let branches = ri.list_branches().unwrap();
        // Filter out _system_ branch created by init_system_branch
        let user_branches: Vec<_> = branches
            .iter()
            .filter(|b| !b.starts_with("_system"))
            .collect();
        assert_eq!(user_branches.len(), 3);
        assert!(branches.contains(&"run-a".to_string()));
        assert!(branches.contains(&"run-b".to_string()));
        assert!(branches.contains(&"run-c".to_string()));
    }

    #[test]
    fn test_delete_branch() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("test-run").unwrap();
        assert!(ri.exists("test-run").unwrap());

        ri.delete_branch("test-run").unwrap();
        assert!(!ri.exists("test-run").unwrap());
    }

    #[test]
    fn test_delete_branch_not_found() {
        let (_temp, _db, ri) = setup();

        let result = ri.delete_branch("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_delete_post_commit_classifies_default_marker_clear_failure() {
        let (temp_dir, db, ri) = setup();
        ri.create_branch("marker-victim").unwrap();
        write_default_branch_marker(&db, "marker-victim").unwrap();

        Database::clear_clear_default_branch_marker_failure_for_test(temp_dir.path());
        Database::inject_clear_default_branch_marker_failure_for_test(
            temp_dir.path(),
            std::io::ErrorKind::Other,
        );

        let committed = ri
            .delete_branch_with_hook("marker-victim", |_| Ok(()))
            .expect("logical delete must commit");
        let completion = ri.complete_delete_post_commit(
            "marker-victim",
            &committed,
            DeletePostCommitOptions {
                clear_storage: false,
                dispatch_best_effort_delete_hook: false,
            },
        );
        Database::clear_clear_default_branch_marker_failure_for_test(temp_dir.path());

        match completion {
            DeleteBranchCompletion::PostCommitError(err) => {
                assert!(
                    error_chain_contains(&err, "clear_default_branch_marker")
                        || error_chain_contains(&err, "injected"),
                    "expected marker clear failure in error chain, got {err}",
                );
            }
            other => panic!("expected PostCommitError, got {other:?}"),
        }
    }

    fn error_chain_contains(err: &StrataError, needle: &str) -> bool {
        let mut current: Option<&dyn std::error::Error> = Some(err);
        while let Some(e) = current {
            if e.to_string().contains(needle) {
                return true;
            }
            current = e.source();
        }
        false
    }

    #[test]
    fn test_complete_delete_post_commit_classifies_subsystem_cleanup_failure() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(
            OpenSpec::primary(temp_dir.path()).with_subsystem(FailingCleanupSubsystem),
        )
        .unwrap();
        let ri = BranchIndex::new(db.clone());
        ri.create_branch("subsystem-victim").unwrap();

        let committed = ri
            .delete_branch_with_hook("subsystem-victim", |_| Ok(()))
            .expect("logical delete must commit");
        let completion = ri.complete_delete_post_commit(
            "subsystem-victim",
            &committed,
            DeletePostCommitOptions {
                clear_storage: false,
                dispatch_best_effort_delete_hook: false,
            },
        );

        match completion {
            DeleteBranchCompletion::PostCommitError(err) => {
                assert!(
                    err.to_string().contains("cleanup_deleted_branch")
                        || err
                            .to_string()
                            .contains("injected cleanup_deleted_branch failure"),
                    "expected subsystem cleanup failure, got {err}"
                );
            }
            other => panic!("expected PostCommitError, got {other:?}"),
        }
    }

    #[test]
    fn test_delete_branch_surfaces_post_commit_cleanup_error() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(
            OpenSpec::primary(temp_dir.path()).with_subsystem(FailingCleanupSubsystem),
        )
        .unwrap();
        let ri = BranchIndex::new(db.clone());
        ri.create_branch("helper-contract").unwrap();

        let err = ri
            .delete_branch("helper-contract")
            .expect_err("delete_branch helper must not flatten post-commit cleanup failure");
        assert!(
            err.to_string().contains("cleanup_deleted_branch")
                || err
                    .to_string()
                    .contains("injected cleanup_deleted_branch failure"),
            "expected subsystem cleanup failure, got {err}"
        );
    }

    #[test]
    fn test_branch_status_default() {
        assert_eq!(BranchStatus::default(), BranchStatus::Active);
    }

    #[test]
    fn test_branch_status_as_str() {
        assert_eq!(BranchStatus::Active.as_str(), "Active");
    }

    /// #1916: Concurrent commit on target branch must not resurrect data
    /// after delete_branch() completes.
    ///
    /// Scenario: T1 starts a transaction on branch "victim", then T2
    /// deletes "victim". If T1 commits after T2 finishes, T1's writes
    /// must be rejected — otherwise data is resurrected on a deleted branch.
    #[test]
    fn test_issue_1916_delete_branch_rejects_concurrent_commit() {
        let (_temp, db, ri) = setup();

        // 1. Create the target branch and write initial data
        ri.create_branch("victim").unwrap();
        let branch_id = resolve_branch_name("victim");
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key_a = Key::new(ns.clone(), TypeTag::KV, b"key_a".to_vec());

        db.transaction(branch_id, |txn| {
            txn.put(key_a.clone(), Value::from("initial"))?;
            Ok(())
        })
        .unwrap();

        // 2. Start a transaction on the target branch (don't commit yet)
        let mut inflight_txn = db.begin_transaction(branch_id).unwrap();
        let key_b = Key::new(ns.clone(), TypeTag::KV, b"key_b".to_vec());
        inflight_txn
            .put(key_b.clone(), Value::from("resurrected"))
            .unwrap();

        // 3. Delete the branch (should mark as deleting, drain, then delete)
        ri.delete_branch("victim").unwrap();

        // 4. The in-flight transaction's commit must fail
        let commit_result = db.commit_transaction(&mut inflight_txn);
        db.end_transaction(inflight_txn);

        assert!(
            commit_result.is_err(),
            "Commit on a deleted branch must be rejected, but it succeeded"
        );

        // 5. Verify no data leaked — branch should not exist
        assert!(!ri.exists("victim").unwrap());
    }

    /// #1916: Stress test — concurrent writers vs delete_branch.
    #[test]
    fn test_issue_1916_delete_branch_concurrent_stress() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let (_temp, db, ri) = setup();

        ri.create_branch("stress-victim").unwrap();
        let branch_id = resolve_branch_name("stress-victim");
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Seed initial data
        db.transaction(branch_id, |txn| {
            for i in 0..10 {
                let key = Key::new(ns.clone(), TypeTag::KV, format!("k{i}").into_bytes());
                txn.put(key, Value::Int(i))?;
            }
            Ok(())
        })
        .unwrap();

        let barrier = Arc::new(Barrier::new(6)); // 5 writers + 1 deleter
        let db = Arc::new(db);
        let ri = Arc::new(ri);

        // Spawn 5 writer threads
        let mut handles = Vec::new();
        for t in 0..5u32 {
            let db = Arc::clone(&db);
            let ns = ns.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let key = Key::new(ns, TypeTag::KV, format!("writer-{t}").into_bytes());
                // This may succeed or fail — either is acceptable.
                // What matters is that after delete completes, no data remains.
                let _ = db.transaction(branch_id, |txn| {
                    txn.put(key.clone(), Value::Int(t as i64))?;
                    Ok(())
                });
            }));
        }

        // Spawn deleter thread
        let ri_del = Arc::clone(&ri);
        let barrier_del = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_del.wait();
            ri_del.delete_branch("stress-victim").unwrap();
        }));

        for h in handles {
            h.join().unwrap();
        }

        // After deletion completes, branch must not exist
        assert!(
            !ri.exists("stress-victim").unwrap(),
            "Branch must not exist after delete_branch completes"
        );
    }
}
