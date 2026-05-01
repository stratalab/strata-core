//! Generic transaction manager for the storage substrate.
//!
//! This manager owns primitive-agnostic commit coordination:
//! - global version / transaction ID allocation
//! - per-branch commit locks
//! - commit quiescing for checkpoint safety
//! - branch-deletion rejection
//! - WAL-free commit execution
//!
//! Higher layers that need durability can interpose their own WAL write through
//! [`commit_with_hook`](TransactionManager::commit_with_hook) without pulling
//! this state back out of `strata-storage`.

use super::context::{CommitError, TransactionContext, TransactionStatus};
use crate::Storage;
use dashmap::{DashMap, DashSet};
use parking_lot::{ArcMutexGuard, Mutex, RawMutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::BranchId;

/// Result of the generic pre-commit phase.
///
/// Read-only transactions can commit immediately without locks, version
/// allocation, or write application. Mutating transactions return a private
/// preparation that must be driven to completion by the storage manager.
enum CommitPreparation<'a> {
    ReadOnly(CommitVersion),
    Prepared(PreparedCommit<'a>),
}

/// A mutating transaction that has passed generic validation and has a reserved
/// commit version.
///
/// The held guards keep the quiesce read-side and the per-branch commit lock
/// alive until the storage manager finishes the commit.
struct PreparedCommit<'a> {
    commit_version: CommitVersion,
    _quiesce_guard: RwLockReadGuard<'a, ()>,
    _branch_guard: ArcMutexGuard<RawMutex, ()>,
}

impl PreparedCommit<'_> {
    /// Version reserved for this prepared commit.
    fn commit_version(&self) -> CommitVersion {
        self.commit_version
    }
}

/// Generic transaction manager for the storage substrate.
pub struct TransactionManager {
    version: AtomicU64,
    next_txn_id: AtomicU64,
    commit_locks: DashMap<BranchId, Arc<Mutex<()>>>,
    commit_quiesce: RwLock<()>,
    visible_version: AtomicU64,
    pending_versions: Mutex<BTreeSet<u64>>,
    deleting_branches: DashSet<BranchId>,
}

impl TransactionManager {
    /// Create a new transaction manager.
    pub fn new(initial_version: CommitVersion) -> Self {
        Self::with_txn_id(initial_version, TxnId::ZERO)
    }

    /// Create a new transaction manager with a specific starting transaction
    /// ID, typically after recovery.
    pub fn with_txn_id(initial_version: CommitVersion, max_txn_id: TxnId) -> Self {
        TransactionManager {
            version: AtomicU64::new(initial_version.as_u64()),
            next_txn_id: AtomicU64::new(max_txn_id.as_u64() + 1),
            commit_locks: DashMap::new(),
            commit_quiesce: RwLock::new(()),
            visible_version: AtomicU64::new(initial_version.as_u64()),
            pending_versions: Mutex::new(BTreeSet::new()),
            deleting_branches: DashSet::new(),
        }
    }

    /// Get current global version.
    pub fn current_version(&self) -> CommitVersion {
        CommitVersion(self.version.load(Ordering::Acquire))
    }

    /// Get the highest fully-applied version that is safe for snapshot reads.
    pub fn visible_version(&self) -> CommitVersion {
        CommitVersion(self.visible_version.load(Ordering::Acquire))
    }

    /// Mark a version as fully applied to storage.
    pub fn mark_version_applied(&self, version: CommitVersion) {
        let mut pending = self.pending_versions.lock();
        pending.remove(&version.as_u64());
        let new_visible = match pending.iter().next() {
            Some(&min_pending) => min_pending.saturating_sub(1),
            None => self.version.load(Ordering::Acquire),
        };
        self.visible_version
            .fetch_max(new_visible, Ordering::AcqRel);
    }

    /// Ensure the version counter is at least `floor`.
    pub fn bump_version_floor(&self, floor: CommitVersion) {
        self.version.fetch_max(floor.as_u64(), Ordering::AcqRel);
        self.visible_version
            .fetch_max(floor.as_u64(), Ordering::AcqRel);
    }

    /// Get the current version after draining all in-flight commits.
    pub fn quiesced_version(&self) -> u64 {
        let _guard = self.commit_quiesce.write();
        self.version.load(Ordering::Acquire)
    }

    /// Acquire the exclusive commit-quiesce lock.
    pub fn quiesce_commits(&self) -> RwLockWriteGuard<'_, ()> {
        self.commit_quiesce.write()
    }

    /// Allocate next transaction ID.
    pub fn next_txn_id(&self) -> std::result::Result<TxnId, CommitError> {
        let prev = self.next_txn_id.fetch_add(1, Ordering::AcqRel);
        if prev == u64::MAX {
            self.next_txn_id.store(u64::MAX, Ordering::Release);
            return Err(CommitError::CounterOverflow(
                "transaction ID counter at u64::MAX".into(),
            ));
        }
        Ok(TxnId(prev))
    }

    /// Allocate the next commit version.
    pub fn allocate_version(&self) -> std::result::Result<CommitVersion, CommitError> {
        let mut pending = self.pending_versions.lock();
        let prev = self.version.fetch_add(1, Ordering::AcqRel);
        if prev == u64::MAX {
            self.version.store(u64::MAX, Ordering::Release);
            return Err(CommitError::CounterOverflow(
                "version counter at u64::MAX".into(),
            ));
        }
        let version = prev + 1;
        pending.insert(version);
        Ok(CommitVersion(version))
    }

    /// Generic pre-commit phase shared by the no-WAL storage path and any
    /// higher-level durability wrappers.
    fn begin_commit<'a, S: Storage>(
        &'a self,
        txn: &mut TransactionContext,
        store: &S,
        external_version: Option<CommitVersion>,
    ) -> std::result::Result<CommitPreparation<'a>, CommitError> {
        if txn.is_read_only() {
            if !txn.is_active() {
                return Err(CommitError::InvalidState(format!(
                    "Cannot commit transaction {} from {:?} state - must be Active",
                    txn.txn_id, txn.status
                )));
            }
            txn.status = TransactionStatus::Committed;
            return Ok(CommitPreparation::ReadOnly(
                external_version
                    .unwrap_or_else(|| CommitVersion(self.version.load(Ordering::Acquire))),
            ));
        }

        let quiesce_guard = self.commit_quiesce.read();
        let commit_mutex = self
            .commit_locks
            .entry(txn.branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let branch_guard = commit_mutex.lock_arc();

        if self.deleting_branches.contains(&txn.branch_id) {
            return Err(CommitError::BranchDeleting(txn.branch_id));
        }

        let can_skip_validation = txn.read_set.is_empty() && txn.cas_set.is_empty();
        if can_skip_validation {
            if !txn.is_active() {
                return Err(CommitError::InvalidState(format!(
                    "Cannot commit transaction {} from {:?} state - must be Active",
                    txn.txn_id, txn.status
                )));
            }
            txn.status = TransactionStatus::Committed;
        } else {
            txn.commit(store)?;
        }

        let commit_version = match external_version {
            None => self.allocate_version()?,
            Some(v) => {
                let mut pending = self.pending_versions.lock();
                self.version.fetch_max(v.as_u64(), Ordering::AcqRel);
                pending.insert(v.as_u64());
                v
            }
        };

        Ok(CommitPreparation::Prepared(PreparedCommit {
            commit_version,
            _quiesce_guard: quiesce_guard,
            _branch_guard: branch_guard,
        }))
    }

    /// Apply a previously prepared commit to storage.
    ///
    /// `durable` indicates whether a higher layer has already made the commit
    /// durable (for example, by appending a WAL record). When `true`, storage
    /// apply failures must surface as `DurableButNotVisible`.
    fn finish_prepared_commit<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        prepared: PreparedCommit<'_>,
        durable: bool,
    ) -> std::result::Result<CommitVersion, CommitError> {
        let commit_version = prepared.commit_version();
        let _prepared = prepared;

        if let Err(e) = txn.apply_writes(store, commit_version) {
            self.mark_version_applied(commit_version);
            if durable {
                return Err(CommitError::DurableButNotVisible {
                    txn_id: txn.txn_id.as_u64(),
                    commit_version: commit_version.as_u64(),
                    reason: format!("Storage application failed: {}", e),
                });
            }

            txn.status = TransactionStatus::Aborted {
                reason: format!("Storage application failed: {}", e),
            };
            return Err(CommitError::WALError(format!(
                "Storage application failed (no WAL): {}",
                e
            )));
        }

        self.mark_version_applied(commit_version);
        Ok(commit_version)
    }

    /// Commit a transaction without durability.
    pub fn commit<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
    ) -> std::result::Result<CommitVersion, CommitError> {
        self.commit_with_hook(txn, store, None, false, |_txn, _version| Ok(()))
    }

    /// Commit a transaction using an externally allocated version.
    pub fn commit_with_version<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        version: CommitVersion,
    ) -> std::result::Result<CommitVersion, CommitError> {
        self.commit_with_hook(txn, store, Some(version), false, |_txn, _version| Ok(()))
    }

    /// Commit a transaction while letting a higher layer interpose work
    /// between generic validation/version allocation and storage apply.
    ///
    /// This is the storage-owned split point for durability wrappers:
    /// the hook can append a WAL record or perform other pre-apply work while
    /// the generic commit guards remain held, without exposing a public
    /// prepared-commit object that can be dropped accidentally.
    #[doc(hidden)]
    pub fn commit_with_hook<S: Storage, F>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        external_version: Option<CommitVersion>,
        durable: bool,
        before_apply: F,
    ) -> std::result::Result<CommitVersion, CommitError>
    where
        F: FnOnce(&mut TransactionContext, CommitVersion) -> std::result::Result<(), CommitError>,
    {
        match self.begin_commit(txn, store, external_version)? {
            CommitPreparation::ReadOnly(version) => Ok(version),
            CommitPreparation::Prepared(prepared) => {
                let commit_version = prepared.commit_version();
                if let Err(err) = before_apply(txn, commit_version) {
                    if !matches!(err, CommitError::DurableButNotVisible { .. }) {
                        let reason = match &err {
                            CommitError::WriterHalted { reason, .. } => {
                                format!("WAL writer halted: {}", reason)
                            }
                            CommitError::WALError(msg) => format!("WAL write failed: {}", msg),
                            other => format!("Pre-apply hook failed: {other}"),
                        };
                        txn.status = TransactionStatus::Aborted { reason };
                    }
                    self.mark_version_applied(commit_version);
                    return Err(err);
                }
                self.finish_prepared_commit(txn, store, prepared, durable)
            }
        }
    }

    /// Remove the per-branch commit lock for a deleted branch.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) -> bool {
        self.commit_locks.remove(branch_id);
        true
    }

    /// Mark a branch as being deleted.
    pub fn mark_branch_deleting(&self, branch_id: &BranchId) {
        self.deleting_branches.insert(*branch_id);
    }

    /// Check if a branch is currently marked as deleting.
    pub fn is_branch_deleting(&self, branch_id: &BranchId) -> bool {
        self.deleting_branches.contains(branch_id)
    }

    /// Remove the deleting mark for a branch.
    pub fn unmark_branch_deleting(&self, branch_id: &BranchId) {
        self.deleting_branches.remove(branch_id);
    }

    /// Clone the branch's commit lock.
    pub fn branch_commit_lock(&self, branch_id: &BranchId) -> Arc<Mutex<()>> {
        self.commit_locks
            .entry(*branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Advance the version counter to at least `v`.
    pub fn catch_up_version(&self, v: u64) {
        self.version.fetch_max(v, Ordering::AcqRel);
        self.visible_version.fetch_max(v, Ordering::AcqRel);
    }

    /// Set the visible version exactly.
    pub fn set_visible_version(&self, version: CommitVersion) {
        self.visible_version
            .store(version.as_u64(), Ordering::Release);
    }

    /// Advance the next transaction ID counter to at least `id + 1`.
    pub fn catch_up_txn_id(&self, id: u64) {
        self.next_txn_id.fetch_max(id + 1, Ordering::AcqRel);
    }

    /// Test/support helper for asserting branch-lock creation without exposing
    /// the underlying map.
    #[doc(hidden)]
    pub fn has_branch_lock(&self, branch_id: &BranchId) -> bool {
        self.commit_locks.contains_key(branch_id)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new(CommitVersion::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionManager;
    use crate::{
        CommitError, Key, Namespace, SegmentedStore, Storage, StorageError, TransactionContext,
    };
    use std::sync::Arc;
    use strata_core::id::{CommitVersion, TxnId};
    use strata_core::value::Value;
    use strata_core::BranchId;

    fn test_namespace(branch_id: BranchId) -> Arc<Namespace> {
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    fn test_key(ns: &Arc<Namespace>, name: &str) -> Key {
        Key::new_kv(ns.clone(), name)
    }

    struct FailingApplyStore {
        inner: Arc<SegmentedStore>,
    }

    impl Storage for FailingApplyStore {
        fn get_versioned(
            &self,
            key: &Key,
            max_version: CommitVersion,
        ) -> crate::StorageResult<Option<strata_core::VersionedValue>> {
            self.inner.get_versioned(key, max_version)
        }

        fn get_history(
            &self,
            key: &Key,
            limit: Option<usize>,
            before_version: Option<CommitVersion>,
        ) -> crate::StorageResult<Vec<strata_core::VersionedValue>> {
            self.inner.get_history(key, limit, before_version)
        }

        fn scan_prefix(
            &self,
            prefix: &Key,
            max_version: CommitVersion,
        ) -> crate::StorageResult<Vec<(Key, strata_core::VersionedValue)>> {
            self.inner.scan_prefix(prefix, max_version)
        }

        fn current_version(&self) -> CommitVersion {
            self.inner.current_version()
        }

        fn put_with_version_mode(
            &self,
            _key: Key,
            _value: Value,
            _version: CommitVersion,
            _ttl: Option<std::time::Duration>,
            _mode: crate::WriteMode,
        ) -> crate::StorageResult<()> {
            Err(StorageError::InvalidInput {
                message: "injected apply failure".to_string(),
            })
        }

        fn delete_with_version(
            &self,
            _key: &Key,
            _version: CommitVersion,
        ) -> crate::StorageResult<()> {
            Err(StorageError::InvalidInput {
                message: "injected apply failure".to_string(),
            })
        }
    }

    #[test]
    fn commit_applies_writes_without_wal() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key = test_key(&ns, "bulk_key");

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(42)).unwrap();

        let version = manager.commit(&mut txn, store.as_ref()).unwrap();
        assert!(version > CommitVersion::ZERO);

        let stored = store
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("committed value must exist");
        assert_eq!(stored.value, Value::Int(42));
        assert_eq!(stored.version.as_u64(), version.as_u64());
    }

    #[test]
    fn commit_creates_branch_lock_for_blind_writes() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key = test_key(&ns, "blind_key");

        assert!(!manager.has_branch_lock(&branch_id));

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key, Value::Int(42)).unwrap();
        assert!(txn.read_set.is_empty(), "must be a blind write");

        manager.commit(&mut txn, store.as_ref()).unwrap();
        assert!(manager.has_branch_lock(&branch_id));
    }

    #[test]
    fn commit_with_external_version_respects_reserved_version() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key = test_key(&ns, "external_version");

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(7)).unwrap();

        let version = manager
            .commit_with_version(&mut txn, store.as_ref(), CommitVersion(42))
            .unwrap();
        assert_eq!(version, CommitVersion(42));

        let stored = store
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("committed value must exist");
        assert_eq!(stored.version.as_u64(), 42);
    }

    #[test]
    fn commit_with_hook_failure_releases_reserved_version() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let failed_key = test_key(&ns, "hook_fail");
        let success_key = test_key(&ns, "recovered");

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(failed_key.clone(), Value::Int(1)).unwrap();

        let err = manager
            .commit_with_hook(&mut txn, store.as_ref(), None, false, |_txn, _version| {
                Err(CommitError::WALError("injected hook failure".to_string()))
            })
            .unwrap_err();
        assert!(matches!(err, CommitError::WALError(_)));
        assert_eq!(manager.current_version(), CommitVersion(1));
        assert_eq!(manager.visible_version(), CommitVersion(1));
        assert!(store
            .get_versioned(&failed_key, CommitVersion::MAX)
            .unwrap()
            .is_none());

        let mut next_txn = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
        next_txn.put(success_key.clone(), Value::Int(2)).unwrap();
        let committed = manager.commit(&mut next_txn, store.as_ref()).unwrap();
        assert_eq!(committed, CommitVersion(2));
        assert_eq!(manager.visible_version(), CommitVersion(2));
    }

    #[test]
    fn commit_rejects_branch_marked_deleting() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key = test_key(&ns, "deleted_branch");

        manager.mark_branch_deleting(&branch_id);

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key, Value::Int(9)).unwrap();

        let err = manager.commit(&mut txn, store.as_ref()).unwrap_err();
        assert!(matches!(err, CommitError::BranchDeleting(id) if id == branch_id));
        assert_eq!(manager.current_version(), CommitVersion::ZERO);
        assert_eq!(manager.visible_version(), CommitVersion::ZERO);
    }

    #[test]
    fn no_wal_apply_failure_does_not_stall_visible_version() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let failing_store = FailingApplyStore {
            inner: Arc::clone(&store),
        };
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key = test_key(&ns, "apply_fail");

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key, Value::Int(7)).unwrap();

        let err = manager.commit(&mut txn, &failing_store).unwrap_err();
        assert!(matches!(err, CommitError::WALError(_)));
        assert_eq!(manager.current_version(), CommitVersion(1));
        assert_eq!(manager.visible_version(), CommitVersion(1));

        let success_key = test_key(&ns, "after_fail");
        let mut next_txn = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
        next_txn.put(success_key.clone(), Value::Int(8)).unwrap();
        let committed = manager.commit(&mut next_txn, store.as_ref()).unwrap();
        assert_eq!(committed, CommitVersion(2));
        assert_eq!(manager.visible_version(), CommitVersion(2));
    }
}
