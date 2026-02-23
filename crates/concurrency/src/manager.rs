//! Transaction manager for coordinating commit operations
//!
//! Provides atomic commit by orchestrating:
//! 1. Validation (first-committer-wins)
//! 2. WAL writing (durability)
//! 3. Storage application (visibility)
//!
//! Per spec Core Invariants:
//! - All-or-nothing commit: transaction writes either ALL succeed or ALL fail
//! - WAL before storage: durability requires WAL to be written first
//! - CommitTxn = durable: transaction is only durable when CommitTxn is in WAL
//!
//! ## Commit Sequence
//!
//! ```text
//! 1. begin_validation() - Change state to Validating
//! 2. validate_transaction() - Check for conflicts
//! 3. IF conflicts: abort() and return error
//! 4. mark_committed() - Change state to Committed
//! 5. Allocate commit_version (increment global version)
//! 6. Build TransactionPayload from write/delete/cas sets
//! 7. Append single WalRecord to segmented WAL (DURABILITY POINT)
//! 8. apply_writes() to storage - Apply to in-memory storage
//! 9. Return Ok(commit_version)
//! ```
//!
//! If crash occurs before step 7: Transaction is not durable, discarded on recovery.
//! If crash occurs after step 7: Transaction is durable, replayed on recovery.

use crate::payload::TransactionPayload;
use crate::{CommitError, TransactionContext, TransactionStatus};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use strata_core::traits::Storage;
use strata_core::types::BranchId;
use strata_durability::format::WalRecord;
use strata_durability::now_micros;
use strata_durability::wal::WalWriter;

/// Manages transaction lifecycle and atomic commits
///
/// TransactionManager coordinates the commit protocol:
/// - Validation against current storage state
/// - WAL writing for durability
/// - Storage application for visibility
///
/// Per spec Section 6.1: Global version counter is incremented once per transaction.
/// All keys in a transaction get the same commit version.
///
/// # Thread Safety
///
/// Commits are serialized per-branch via internal locks to prevent TOCTOU
/// (time-of-check-to-time-of-use) races between validation and storage application.
/// This ensures that no other transaction on the same branch can modify storage
/// between the time we validate and the time we apply our writes.
///
/// Transactions on different branches can commit in parallel, as ShardedStore
/// maintains per-branch shards and there's no cross-branch conflict.
pub struct TransactionManager {
    /// Global version counter
    ///
    /// Monotonically increasing. Each committed transaction increments by 1.
    /// Shared across all branches for consistent MVCC ordering.
    version: AtomicU64,

    /// Next transaction ID
    ///
    /// Unique identifier for transactions. Used in WAL entries.
    next_txn_id: AtomicU64,

    /// Per-branch commit locks
    ///
    /// Prevents TOCTOU race between validation and apply within the same branch.
    /// Without this lock, the following race can occur:
    /// 1. T1 validates (succeeds, storage at v1)
    /// 2. T2 validates (succeeds, storage still at v1)
    /// 3. T1 applies (storage now at v2)
    /// 4. T2 applies (uses stale validation from step 2)
    ///
    /// Using per-branch locks allows parallel commits for different branches while
    /// still preventing TOCTOU within each branch.
    commit_locks: DashMap<BranchId, Mutex<()>>,
}

impl TransactionManager {
    /// Create a new transaction manager
    ///
    /// # Arguments
    /// * `initial_version` - Starting version (typically from recovery's final_version)
    pub fn new(initial_version: u64) -> Self {
        Self::with_txn_id(initial_version, 0)
    }

    /// Create a new transaction manager with specific starting txn_id
    ///
    /// This is used during recovery to ensure new transactions get unique IDs
    /// that don't conflict with transactions already in the WAL.
    ///
    /// # Arguments
    /// * `initial_version` - Starting version (from recovery's final_version)
    /// * `max_txn_id` - Maximum txn_id seen in WAL (new transactions start at max_txn_id + 1)
    pub fn with_txn_id(initial_version: u64, max_txn_id: u64) -> Self {
        TransactionManager {
            version: AtomicU64::new(initial_version),
            // Start next_txn_id at max_txn_id + 1 to avoid conflicts
            next_txn_id: AtomicU64::new(max_txn_id + 1),
            commit_locks: DashMap::new(),
        }
    }

    /// Get current global version
    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Allocate next transaction ID
    ///
    /// # Panics
    ///
    /// Panics if the transaction ID counter reaches `u64::MAX` (overflow).
    pub fn next_txn_id(&self) -> u64 {
        self.next_txn_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .expect("transaction ID overflow: u64::MAX reached")
    }

    /// Allocate next commit version (increment global version)
    ///
    /// Per spec Section 6.1: Version incremented ONCE for the whole transaction.
    ///
    /// # Version Gaps
    ///
    /// Version gaps may occur if a transaction fails after version allocation
    /// but before successful commit (e.g., WAL write failure). Consumers should
    /// not assume version numbers are contiguous. A gap means the version was
    /// allocated but no data was committed with that version.
    ///
    /// This is by design - version allocation is atomic and non-blocking,
    /// while failure handling during commit does not attempt to "return"
    /// the allocated version.
    ///
    /// # Panics
    ///
    /// Panics if the version counter reaches `u64::MAX` (overflow).
    pub fn allocate_version(&self) -> u64 {
        self.version
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .expect("version counter overflow: u64::MAX reached")
            + 1
    }

    /// Commit a transaction atomically
    ///
    /// Per spec Core Invariants:
    /// - Validates transaction (first-committer-wins)
    /// - Writes to WAL for durability (when WAL is provided)
    /// - Applies to storage only after WAL is durable
    /// - All-or-nothing: either all writes succeed or transaction aborts
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit (must be in Active state)
    /// * `store` - Storage to validate against and apply writes to
    /// * `wal` - Optional WAL for durability. Pass `None` for ephemeral databases
    ///   or when durability is not required (DurabilityMode::Cache).
    ///
    /// # Returns
    /// - Ok(commit_version) on success
    /// - Err(CommitError) if validation fails or WAL write fails
    ///
    /// # Commit Sequence
    ///
    /// 1. Acquire per-branch commit lock (prevents TOCTOU race within same branch)
    /// 2. Validate and mark committed (in-memory state transition)
    /// 3. Allocate commit version
    /// 4. Write to WAL if provided (BeginTxn, operations, CommitTxn)
    /// 5. Apply writes to storage
    /// 6. Release commit lock
    /// 7. Return commit version
    ///
    /// When `wal` is `None`, steps 4 is skipped entirely. The transaction is
    /// still validated and applied to storage, but without durability guarantees.
    ///
    /// # Thread Safety
    ///
    /// Per-branch commit locks ensure that validation and apply happen atomically
    /// with respect to other transactions on the same branch. This prevents the
    /// TOCTOU race where validation passes but storage changes before apply.
    ///
    /// Transactions on different branches can commit in parallel.
    pub fn commit<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        mut wal: Option<&mut WalWriter>,
    ) -> std::result::Result<u64, CommitError> {
        // Fast path: read-only transactions skip lock, validation, version alloc, WAL, apply
        if txn.is_read_only() && txn.json_writes().is_empty() {
            if !txn.is_active() {
                return Err(CommitError::InvalidState(format!(
                    "Cannot commit transaction {} from {:?} state - must be Active",
                    txn.txn_id, txn.status
                )));
            }
            txn.status = TransactionStatus::Committed;
            return Ok(self.version.load(Ordering::SeqCst));
        }

        // Acquire per-branch commit lock to prevent TOCTOU race between validation and apply
        // This ensures no other transaction on the same branch can modify storage between
        // our validation check and our apply_writes call.
        // Transactions on different branches can proceed in parallel.
        let branch_lock = self
            .commit_locks
            .entry(txn.branch_id)
            .or_insert_with(|| Mutex::new(()));
        let _commit_guard = branch_lock.lock();

        // Step 1: Validate and mark committed (in-memory)
        // This performs: Active → Validating → Committed
        // Or: Active → Validating → Aborted (if conflicts detected)
        // Skip validation for blind writes (no reads, no CAS, no JSON snapshots/writes)
        let can_skip_validation = txn.read_set.is_empty()
            && txn.cas_set.is_empty()
            && txn.json_snapshot_versions().map_or(true, |v| v.is_empty())
            && txn.json_writes().is_empty();

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
            tracing::debug!(target: "strata::txn", txn_id = txn.txn_id, "Validation passed");
        }

        // At this point, transaction is in Committed state
        // but NOT yet durable (not in WAL)

        // Step 2: Allocate commit version
        let commit_version = self.allocate_version();

        // Step 3: Write to WAL (durability) - only for transactions with mutations
        // Skip WAL for read-only transactions (no writes, deletes, CAS ops, or JSON patches)
        let has_mutations = !txn.is_read_only() || !txn.json_writes().is_empty();
        if has_mutations {
            if let Some(wal) = wal.as_mut() {
                let payload = TransactionPayload::from_transaction(txn, commit_version);
                let record = WalRecord::new(
                    txn.txn_id,
                    *txn.branch_id.as_bytes(),
                    now_micros(),
                    payload.to_bytes(),
                );

                if let Err(e) = wal.append(&record) {
                    txn.status = TransactionStatus::Aborted {
                        reason: format!("WAL write failed: {}", e),
                    };
                    return Err(CommitError::WALError(e.to_string()));
                }

                // DURABILITY POINT: Transaction is now durable
                // Even if we crash after this, recovery will replay from WAL
                tracing::debug!(target: "strata::txn", txn_id = txn.txn_id, commit_version, "WAL durable");
            }
        }

        // Step 4: Apply to storage
        if let Err(e) = txn.apply_writes(store, commit_version) {
            if wal.is_some() {
                // WAL says committed but storage failed - serious error
                // Log error but return success since WAL is authoritative
                // Recovery will replay the transaction anyway
                tracing::error!(
                    target: "strata::txn",
                    txn_id = txn.txn_id,
                    commit_version = commit_version,
                    error = %e,
                    "Storage application failed after WAL commit - will be recovered on restart"
                );
            } else {
                // No WAL - storage failure means data loss, return error
                txn.status = TransactionStatus::Aborted {
                    reason: format!("Storage application failed: {}", e),
                };
                return Err(CommitError::WALError(format!(
                    "Storage application failed (no WAL): {}",
                    e
                )));
            }
        }

        // Step 5: Return commit version
        Ok(commit_version)
    }

    /// Remove the per-branch commit lock for a deleted branch.
    ///
    /// Called during branch deletion to prevent unbounded growth of the
    /// `commit_locks` map. Safe to call even if no lock exists for the branch.
    ///
    /// # Safety
    ///
    /// This should only be called after the branch has been fully deleted
    /// and no further transactions will target it. If a concurrent transaction
    /// is in-flight for this branch, the lock will be lazily re-created on
    /// next commit (via `or_insert_with` in `commit()`).
    pub fn remove_branch_lock(&self, branch_id: &BranchId) {
        self.commit_locks.remove(branch_id);
    }

    /// Advance the version counter to at least `v`.
    ///
    /// Used during multi-process refresh to ensure the local version counter
    /// reflects writes applied from other processes' WAL entries.
    /// Uses `fetch_max` so the counter never goes backward.
    pub fn catch_up_version(&self, v: u64) {
        self.version.fetch_max(v, Ordering::SeqCst);
    }

    /// Advance the next_txn_id counter to at least `id + 1`.
    ///
    /// Used during multi-process refresh to ensure locally-allocated transaction
    /// IDs never collide with IDs already used by other processes.
    pub fn catch_up_txn_id(&self, id: u64) {
        self.next_txn_id.fetch_max(id + 1, Ordering::SeqCst);
    }

    /// Commit a transaction with an externally-allocated version.
    ///
    /// Similar to `commit()` but does NOT allocate a version from the local
    /// counter — instead, uses the provided `version`. This is used by the
    /// coordinated commit path where version allocation happens under the
    /// WAL file lock via the shared counter file.
    ///
    /// The per-branch commit lock is still acquired for thread safety.
    pub fn commit_with_version<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        mut wal: Option<&mut WalWriter>,
        version: u64,
    ) -> std::result::Result<u64, CommitError> {
        // Fast path: read-only transactions
        if txn.is_read_only() && txn.json_writes().is_empty() {
            if !txn.is_active() {
                return Err(CommitError::InvalidState(format!(
                    "Cannot commit transaction {} from {:?} state - must be Active",
                    txn.txn_id, txn.status
                )));
            }
            txn.status = TransactionStatus::Committed;
            return Ok(version);
        }

        // Acquire per-branch commit lock
        let branch_lock = self
            .commit_locks
            .entry(txn.branch_id)
            .or_insert_with(|| Mutex::new(()));
        let _commit_guard = branch_lock.lock();

        // Validate
        let can_skip_validation = txn.read_set.is_empty()
            && txn.cas_set.is_empty()
            && txn.json_snapshot_versions().map_or(true, |v| v.is_empty())
            && txn.json_writes().is_empty();

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

        // Use the externally-provided version (do NOT allocate from local counter)
        let commit_version = version;

        // Advance local counter so it stays in sync
        self.version.fetch_max(commit_version, Ordering::SeqCst);

        // Write to WAL
        let has_mutations = !txn.is_read_only() || !txn.json_writes().is_empty();
        if has_mutations {
            if let Some(wal) = wal.as_mut() {
                let payload = TransactionPayload::from_transaction(txn, commit_version);
                let record = WalRecord::new(
                    txn.txn_id,
                    *txn.branch_id.as_bytes(),
                    now_micros(),
                    payload.to_bytes(),
                );

                if let Err(e) = wal.append(&record) {
                    txn.status = TransactionStatus::Aborted {
                        reason: format!("WAL write failed: {}", e),
                    };
                    return Err(CommitError::WALError(e.to_string()));
                }
            }
        }

        // Apply to storage
        if let Err(e) = txn.apply_writes(store, commit_version) {
            if wal.is_some() {
                tracing::error!(
                    target: "strata::txn",
                    txn_id = txn.txn_id,
                    commit_version = commit_version,
                    error = %e,
                    "Storage application failed after WAL commit - will be recovered on restart"
                );
            } else {
                txn.status = TransactionStatus::Aborted {
                    reason: format!("Storage application failed: {}", e),
                };
                return Err(CommitError::WALError(format!(
                    "Storage application failed (no WAL): {}",
                    e
                )));
            }
        }

        Ok(commit_version)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransactionContext;
    use parking_lot::Mutex as ParkingMutex;
    use std::sync::Arc;
    use strata_core::types::{Key, Namespace};
    use strata_core::value::Value;
    use strata_durability::codec::IdentityCodec;
    use strata_durability::wal::{DurabilityMode, WalConfig};
    use strata_storage::ShardedStore;
    use tempfile::TempDir;

    fn create_test_namespace(branch_id: BranchId) -> Namespace {
        Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        )
    }

    fn create_test_key(ns: &Namespace, name: &str) -> Key {
        Key::new_kv(ns.clone(), name)
    }

    fn create_test_wal(dir: &std::path::Path) -> WalWriter {
        WalWriter::new(
            dir.to_path_buf(),
            [0u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            Box::new(IdentityCodec),
        )
        .unwrap()
    }

    #[test]
    fn test_new_manager_has_correct_initial_version() {
        let manager = TransactionManager::new(100);
        assert_eq!(manager.current_version(), 100);
    }

    #[test]
    fn test_allocate_version_increments() {
        let manager = TransactionManager::new(0);
        assert_eq!(manager.allocate_version(), 1);
        assert_eq!(manager.allocate_version(), 2);
        assert_eq!(manager.allocate_version(), 3);
        assert_eq!(manager.current_version(), 3);
    }

    #[test]
    fn test_next_txn_id_increments() {
        // TransactionManager::new(0) calls with_txn_id(0, 0), which sets next_txn_id = 0 + 1 = 1
        let manager = TransactionManager::new(0);
        assert_eq!(manager.next_txn_id(), 1);
        assert_eq!(manager.next_txn_id(), 2);
        assert_eq!(manager.next_txn_id(), 3);
    }

    #[test]
    fn test_with_txn_id_starts_from_max_plus_one() {
        let manager = TransactionManager::with_txn_id(50, 100);
        assert_eq!(manager.current_version(), 50);
        assert_eq!(manager.next_txn_id(), 101); // max_txn_id + 1
    }

    #[test]
    fn test_per_branch_commit_locks_allow_parallel_different_branches() {
        // This test verifies that commits on different branches can proceed in parallel
        // by checking that both commits complete and produce unique versions
        // Note: WalWriter requires &mut so we use a Mutex for shared access from threads
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(ShardedStore::new());
        let manager = Arc::new(TransactionManager::new(0));

        let branch_id1 = BranchId::new();
        let branch_id2 = BranchId::new();
        let ns1 = create_test_namespace(branch_id1);
        let ns2 = create_test_namespace(branch_id2);
        let key1 = create_test_key(&ns1, "key1");
        let key2 = create_test_key(&ns2, "key2");

        // Prepare transactions
        let snapshot1 = store.snapshot();
        let mut txn1 = TransactionContext::with_snapshot(1, branch_id1, Box::new(snapshot1));
        txn1.put(key1.clone(), Value::Int(1)).unwrap();

        let snapshot2 = store.snapshot();
        let mut txn2 = TransactionContext::with_snapshot(2, branch_id2, Box::new(snapshot2));
        txn2.put(key2.clone(), Value::Int(2)).unwrap();

        // Commit both in parallel threads
        let manager_clone = Arc::clone(&manager);
        let store_clone = Arc::clone(&store);
        let wal_clone = Arc::clone(&wal);

        let handle1 = std::thread::spawn(move || {
            let mut guard = wal_clone.lock();
            manager_clone.commit(&mut txn1, store_clone.as_ref(), Some(&mut *guard))
        });

        let manager_clone2 = Arc::clone(&manager);
        let store_clone2 = Arc::clone(&store);
        let wal_clone2 = Arc::clone(&wal);

        let handle2 = std::thread::spawn(move || {
            let mut guard = wal_clone2.lock();
            manager_clone2.commit(&mut txn2, store_clone2.as_ref(), Some(&mut *guard))
        });

        let v1 = handle1.join().unwrap().unwrap();
        let v2 = handle2.join().unwrap().unwrap();

        // Both commits should succeed with unique versions
        assert!(v1 >= 1 && v1 <= 2);
        assert!(v2 >= 1 && v2 <= 2);
        assert_ne!(v1, v2); // Versions must be unique

        // Both keys should be in storage
        assert!(store.get(&key1).unwrap().is_some());
        assert!(store.get(&key2).unwrap().is_some());
    }

    #[test]
    fn test_same_branch_commits_serialize() {
        // This test verifies that commits on the same branch are serialized
        // (one completes before the other starts its critical section)
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key1 = create_test_key(&ns, "key1");
        let key2 = create_test_key(&ns, "key2");

        // Commit first transaction
        {
            let snapshot = store.snapshot();
            let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
            txn.put(key1.clone(), Value::Int(100)).unwrap();
            let v = manager
                .commit(&mut txn, store.as_ref(), Some(&mut wal))
                .unwrap();
            assert_eq!(v, 1);
        }

        // Commit second transaction on same branch
        {
            let snapshot = store.snapshot();
            let mut txn = TransactionContext::with_snapshot(2, branch_id, Box::new(snapshot));
            txn.put(key2.clone(), Value::Int(200)).unwrap();
            let v = manager
                .commit(&mut txn, store.as_ref(), Some(&mut wal))
                .unwrap();
            assert_eq!(v, 2);
        }

        // Both values should be present with correct versions
        let v1 = store.get(&key1).unwrap().unwrap();
        assert_eq!(v1.value, Value::Int(100));
        assert_eq!(v1.version.as_u64(), 1);

        let v2 = store.get(&key2).unwrap().unwrap();
        assert_eq!(v2.value, Value::Int(200));
        assert_eq!(v2.version.as_u64(), 2);
    }

    #[test]
    fn test_many_parallel_commits_different_branches() {
        // Stress test: many parallel commits on different branches
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(ShardedStore::new());
        let manager = Arc::new(TransactionManager::new(0));

        let num_threads = 10;
        let mut handles = Vec::new();

        for i in 0..num_threads {
            let manager_clone = Arc::clone(&manager);
            let store_clone = Arc::clone(&store);
            let wal_clone = Arc::clone(&wal);

            handles.push(std::thread::spawn(move || {
                let branch_id = BranchId::new();
                let ns = create_test_namespace(branch_id);
                let key = create_test_key(&ns, &format!("key_{}", i));

                let snapshot = store_clone.snapshot();
                let mut txn =
                    TransactionContext::with_snapshot(i as u64 + 1, branch_id, Box::new(snapshot));
                txn.put(key, Value::Int(i as i64)).unwrap();

                let mut guard = wal_clone.lock();
                manager_clone.commit(&mut txn, store_clone.as_ref(), Some(&mut *guard))
            }));
        }

        // All commits should succeed
        let versions: Vec<u64> = handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect();

        // All versions should be unique and in range 1..=num_threads
        let mut sorted = versions.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), num_threads);
        assert_eq!(sorted[0], 1);
        assert_eq!(sorted[num_threads - 1], num_threads as u64);
    }

    /// Test that scan_prefix tracks deleted keys in read_set for conflict detection.
    #[test]
    fn test_scan_prefix_deleted_key_conflict_detection() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_alice = create_test_key(&ns, "user:alice");
        let key_bob = create_test_key(&ns, "user:bob");
        let prefix = create_test_key(&ns, "user:");

        // Setup: Create initial data with alice and bob
        {
            let snapshot = store.snapshot();
            let mut setup_txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
            setup_txn.put(key_alice.clone(), Value::Int(100)).unwrap();
            setup_txn.put(key_bob.clone(), Value::Int(200)).unwrap();
            manager
                .commit(&mut setup_txn, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: Delete alice (blind), then scan
        let snapshot1 = store.snapshot();
        let mut txn1 = TransactionContext::with_snapshot(2, branch_id, Box::new(snapshot1));
        txn1.delete(key_alice.clone()).unwrap(); // Blind delete
        let scan_result = txn1.scan_prefix(&prefix).unwrap();

        // T1 sees only bob (alice excluded due to delete_set)
        assert_eq!(scan_result.len(), 1);
        assert!(scan_result.iter().any(|(k, _)| k == &key_bob));

        // T2: Update alice and commit first
        {
            let snapshot2 = store.snapshot();
            let mut txn2 = TransactionContext::with_snapshot(3, branch_id, Box::new(snapshot2));
            // T2 reads alice first (creates read_set entry)
            let _ = txn2.get(&key_alice).unwrap();
            txn2.put(key_alice.clone(), Value::Int(999)).unwrap();
            manager
                .commit(&mut txn2, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1 commits - should FAIL because alice was modified after T1 observed it in scan
        let result = manager.commit(&mut txn1, store.as_ref(), Some(&mut wal));

        // Conflict should be detected: T1 scanned and saw alice at v1, but T2 updated it to v2
        assert!(
            result.is_err(),
            "Should detect conflict when scanned deleted key is modified"
        );

        // T2's update should be preserved
        let final_value = store.get(&key_alice).unwrap().unwrap();
        assert_eq!(
            final_value.value,
            Value::Int(999),
            "T2's update should be preserved"
        );
    }

    /// Test that blind delete without scan does NOT cause conflict (by design).
    #[test]
    fn test_blind_delete_no_conflict() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_alice = create_test_key(&ns, "user:alice");

        // Setup: Create alice
        {
            let snapshot = store.snapshot();
            let mut setup_txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
            setup_txn.put(key_alice.clone(), Value::Int(100)).unwrap();
            manager
                .commit(&mut setup_txn, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: Blind delete alice (no read, no scan)
        let snapshot1 = store.snapshot();
        let mut txn1 = TransactionContext::with_snapshot(2, branch_id, Box::new(snapshot1));
        txn1.delete(key_alice.clone()).unwrap(); // Blind delete - no read_set entry

        // T2: Update alice and commit first
        {
            let snapshot2 = store.snapshot();
            let mut txn2 = TransactionContext::with_snapshot(3, branch_id, Box::new(snapshot2));
            let _ = txn2.get(&key_alice).unwrap();
            txn2.put(key_alice.clone(), Value::Int(999)).unwrap();
            manager
                .commit(&mut txn2, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1 commits - should SUCCEED because blind writes don't conflict
        let result = manager.commit(&mut txn1, store.as_ref(), Some(&mut wal));
        assert!(
            result.is_ok(),
            "Blind delete should succeed (no read_set entry)"
        );

        // T1's delete overwrites T2's update - this is expected for blind writes
        let final_value = store.get(&key_alice).unwrap();
        assert!(
            final_value.is_none(),
            "Blind delete should succeed, removing alice"
        );
    }

    // ========================================================================
    // Read-Only Fast Path Tests
    // ========================================================================

    #[test]
    fn test_read_only_no_version_increment() {
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        // Read-only transaction (just reads, no writes)
        let snapshot = store.snapshot();
        let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
        // No operations — pure read-only
        let result = manager.commit(&mut txn, store.as_ref(), None);
        assert!(result.is_ok());

        // Version should NOT have been incremented
        assert_eq!(manager.current_version(), v_before);
    }

    #[test]
    fn test_read_only_no_lock_contention() {
        // Two concurrent read-only transactions on the same branch shouldn't block
        let store = Arc::new(ShardedStore::new());
        let manager = Arc::new(TransactionManager::new(0));
        let branch_id = BranchId::new();

        let manager1 = Arc::clone(&manager);
        let store1 = Arc::clone(&store);
        let manager2 = Arc::clone(&manager);
        let store2 = Arc::clone(&store);

        let h1 = std::thread::spawn(move || {
            let snapshot = store1.snapshot();
            let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
            manager1.commit(&mut txn, store1.as_ref(), None)
        });
        let h2 = std::thread::spawn(move || {
            let snapshot = store2.snapshot();
            let mut txn = TransactionContext::with_snapshot(2, branch_id, Box::new(snapshot));
            manager2.commit(&mut txn, store2.as_ref(), None)
        });

        assert!(h1.join().unwrap().is_ok());
        assert!(h2.join().unwrap().is_ok());
    }

    #[test]
    fn test_read_only_with_json_writes_takes_normal_path() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        let snapshot = store.snapshot();
        let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
        // No KV writes, but has json_writes → not fast-pathed
        use strata_core::primitives::json::{JsonPatch, JsonPath};
        txn.record_json_write(
            create_test_key(&create_test_namespace(branch_id), "doc"),
            JsonPatch::set_at(JsonPath::root(), serde_json::json!({"a": 1}).into()),
            0,
        );

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());

        // Version SHOULD have been incremented (not fast-pathed)
        assert!(manager.current_version() > v_before);
    }

    #[test]
    fn test_read_only_rejects_non_active() {
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        let snapshot = store.snapshot();
        let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
        txn.mark_aborted("test".to_string()).unwrap();

        let result = manager.commit(&mut txn, store.as_ref(), None);
        assert!(result.is_err());
        match result.unwrap_err() {
            CommitError::InvalidState(_) => {} // expected
            other => panic!("Expected InvalidState, got {:?}", other),
        }
    }

    // ========================================================================
    // Blind-Write Validation Skip Tests
    // ========================================================================

    #[test]
    fn test_blind_write_skips_validation() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "blind");

        // Blind write: put with no prior read
        let snapshot = store.snapshot();
        let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
        txn.put(key.clone(), Value::Int(42)).unwrap();
        assert!(txn.read_set.is_empty()); // Confirms it's a blind write

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());

        let stored = store.get(&key).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(42));
    }

    #[test]
    fn test_write_with_read_still_validates() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "readwrite");

        // Setup: store initial value
        {
            let snapshot = store.snapshot();
            let mut setup = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
            setup.put(key.clone(), Value::Int(1)).unwrap();
            manager
                .commit(&mut setup, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: read then write (not a blind write)
        let snapshot = store.snapshot();
        let mut txn = TransactionContext::with_snapshot(2, branch_id, Box::new(snapshot));
        let _ = txn.get(&key).unwrap();
        txn.put(key.clone(), Value::Int(2)).unwrap();
        assert!(!txn.read_set.is_empty()); // Has reads → goes through validation

        // Concurrent update
        {
            let snapshot2 = store.snapshot();
            let mut txn2 = TransactionContext::with_snapshot(3, branch_id, Box::new(snapshot2));
            txn2.put(key.clone(), Value::Int(99)).unwrap();
            manager
                .commit(&mut txn2, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1 should fail validation (read-write conflict)
        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_err());
    }

    #[test]
    fn test_write_with_cas_still_validates() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "cas_key");

        // CAS operation → not a blind write, should validate
        let snapshot = store.snapshot();
        let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
        txn.cas(key.clone(), 0, Value::Int(1)).unwrap();
        assert!(!txn.cas_set.is_empty());

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_with_json_snapshot_still_validates() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(ShardedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "json_doc");

        // Has json_snapshot_versions → should go through full validation path
        // Use version 0 (key doesn't exist) so validation passes
        let snapshot = store.snapshot();
        let mut txn = TransactionContext::with_snapshot(1, branch_id, Box::new(snapshot));
        txn.put(key.clone(), Value::Int(1)).unwrap();
        txn.record_json_snapshot_version(key.clone(), 0);

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());
        // Verify it went through the normal path (version incremented)
        assert!(manager.current_version() > 0);
    }
}
