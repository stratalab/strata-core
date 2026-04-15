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
use crate::database::Database;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::id::CommitVersion;
use strata_core::traits::Storage;
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::value::Value;
use strata_core::StrataError;
use strata_core::StrataResult;
use tracing::{info, warn};
use uuid::Uuid;

/// Namespace UUID for generating deterministic branch IDs from names.
/// Must match the executor's BRANCH_NAMESPACE for consistency.
const BRANCH_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// Internal metadata key storing the effective default branch name for
/// disk-backed databases. This lets follower and reopened primary handles
/// recover branch semantics across process boundaries.
const DEFAULT_BRANCH_MARKER_KEY: &str = "__default_branch__";

/// Resolve a branch name to a core BranchId using the same logic as the executor.
///
/// - "default" → nil UUID (all zeros)
/// - Valid UUID string → parsed directly
/// - Any other string → deterministic UUID v5 from name
pub fn resolve_branch_name(name: &str) -> BranchId {
    if name == "default" {
        BranchId::from_bytes([0u8; 16])
    } else if let Ok(u) = Uuid::parse_str(name) {
        BranchId::from_bytes(*u.as_bytes())
    } else {
        let uuid = Uuid::new_v5(&BRANCH_NAMESPACE, name.as_bytes());
        BranchId::from_bytes(*uuid.as_bytes())
    }
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

fn default_branch_marker_key() -> Key {
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
    db.transaction(global_branch_id(), |txn| {
        let key = default_branch_marker_key();
        let should_clear = matches!(txn.get(&key)?, Some(Value::String(name)) if name == branch_name);
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
        let result = self.db.transaction(global_branch_id(), |txn| {
            let key = self.key_for(branch_id);

            // Check if branch already exists
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
        })?;

        // Note: DAG recording is handled by BranchService.create(), which is
        // the canonical branch creation path and suppresses this best-effort
        // low-level dispatch so it can record through the load-bearing
        // BranchMutation boundary instead.
        dispatch_create_hook(&self.db, branch_id);

        Ok(result)
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
        // 4. Clear storage.
        // 5. Clean up the mark and commit lock entry.
        self.db.mark_branch_deleting(&executor_branch_id);
        if let Some(meta_id) = metadata_branch_id {
            if meta_id != executor_branch_id {
                self.db.mark_branch_deleting(&meta_id);
            }
        }
        let target_lock = self.db.branch_commit_lock(&executor_branch_id);
        let _target_guard = target_lock.lock();

        let result = self.db.transaction(global_branch_id(), |txn| {
            txn.set_allow_cross_branch(true);
            // Delete data from the executor's namespace
            Self::delete_namespace_data(txn, executor_branch_id)?;

            // If the metadata BranchId differs, also delete from that namespace
            if let Some(meta_id) = metadata_branch_id {
                if meta_id != executor_branch_id {
                    Self::delete_namespace_data(txn, meta_id)?;
                }
            }

            // Delete the branch metadata entry
            txn.delete(meta_key.clone())?;

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

        if let Err(e) = clear_default_branch_marker_if(&self.db, branch_id) {
            warn!(
                target: "strata::branch",
                branch = branch_id,
                error = %e,
                "Failed to clear persisted default-branch marker after delete"
            );
        }

        // Evict cached namespaces for the deleted branch to prevent unbounded
        // growth of the global NS_CACHE (SCALE-001).
        crate::primitives::kv::invalidate_kv_namespace_cache(&executor_branch_id);
        crate::system_space::invalidate_cache(&executor_branch_id);

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
        // deletion before files disappear. clear_branch_storage also
        // retries directory removal after gc_orphan_segments as a safety
        // net for anything drain() may have missed.
        self.db.clear_branch_storage(&executor_branch_id);

        let subsystems = self.db.installed_subsystems();
        for subsystem in subsystems.iter() {
            if let Err(e) =
                subsystem.cleanup_deleted_branch(&self.db, &executor_branch_id, branch_id)
            {
                warn!(
                    target: "strata::branch",
                    subsystem = subsystem.name(),
                    branch = branch_id,
                    error = %e,
                    "Subsystem branch cleanup failed after delete"
                );
            }
        }

        // Remove commit lock entry. The deleting mark is intentionally
        // kept: any pre-existing transaction that tries to commit on
        // this branch after deletion will be rejected (#1916).
        self.db.remove_branch_lock(&executor_branch_id);

        // BranchService.delete() suppresses this best-effort dispatch so it can
        // record through the load-bearing BranchMutation boundary instead.
        dispatch_delete_hook(&self.db, branch_id);

        Ok(())
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

// ========== Searchable Trait Implementation ==========
//
// Search is handled by the intelligence layer.
// This implementation returns empty results.

impl crate::search::Searchable for BranchIndex {
    fn search(
        &self,
        _req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
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
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, BranchIndex) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let ri = BranchIndex::new(db.clone());
        (temp_dir, db, ri)
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
