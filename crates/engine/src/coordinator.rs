//! Transaction coordinator for managing transaction lifecycle
//!
//! Per spec Section 6.1:
//! - Single monotonic counter for the entire database
//! - Incremented on each COMMIT (not each write)
//!
//! The TransactionCoordinator wraps TransactionManager and adds:
//! - Active transaction tracking
//! - Transaction metrics (started, committed, aborted)
//! - Commit rate calculation

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strata_concurrency::{RecoveryResult, TransactionContext, TransactionManager};
use strata_core::traits::Storage;
use strata_core::types::BranchId;
use strata_core::StrataError;
use strata_core::StrataResult;
use strata_durability::wal::WalWriter;
use strata_storage::ShardedStore;
use tracing::{debug, info, warn};

/// Transaction coordinator for the database
///
/// Manages transaction lifecycle, ID allocation, version tracking, and metrics.
/// Per spec Section 6.1: Single monotonic counter for the entire database.
///
/// # Memory Ordering
///
/// The metric counters (active_count, total_started, total_committed, total_aborted)
/// use Relaxed ordering intentionally because:
/// 1. They are purely observational metrics for monitoring/debugging
/// 2. They do not synchronize any other memory operations
/// 3. Approximate counts are acceptable for metrics purposes
/// 4. The atomic operations (fetch_add/fetch_sub) guarantee no torn reads/writes
pub struct TransactionCoordinator {
    /// Transaction manager for ID/version allocation
    manager: TransactionManager,
    /// Active transaction count (for metrics) - uses Relaxed ordering
    active_count: AtomicU64,
    /// Total transactions started - uses Relaxed ordering
    total_started: AtomicU64,
    /// Total transactions committed - uses Relaxed ordering
    total_committed: AtomicU64,
    /// Total transactions aborted - uses Relaxed ordering
    total_aborted: AtomicU64,
}

impl TransactionCoordinator {
    /// Create new coordinator with initial version
    ///
    /// # Arguments
    /// * `initial_version` - Starting version (typically from storage or recovery)
    pub fn new(initial_version: u64) -> Self {
        Self {
            manager: TransactionManager::new(initial_version),
            active_count: AtomicU64::new(0),
            total_started: AtomicU64::new(0),
            total_committed: AtomicU64::new(0),
            total_aborted: AtomicU64::new(0),
        }
    }

    /// Create coordinator from recovery result
    ///
    /// Initializes the coordinator with the version AND max_txn_id from recovery,
    /// ensuring new transactions get monotonically increasing versions and IDs.
    ///
    /// CRITICAL: Both final_version AND max_txn_id must be restored to ensure:
    /// - Versions are monotonically increasing (final_version)
    /// - Transaction IDs are unique across sessions (max_txn_id)
    ///
    /// # Arguments
    /// * `result` - Recovery result containing final version and max_txn_id
    pub fn from_recovery(result: &RecoveryResult) -> Self {
        Self {
            manager: TransactionManager::with_txn_id(
                result.stats.final_version,
                result.stats.max_txn_id,
            ),
            active_count: AtomicU64::new(0),
            total_started: AtomicU64::new(0),
            total_committed: AtomicU64::new(0),
            total_aborted: AtomicU64::new(0),
        }
    }

    /// Start a new transaction
    ///
    /// Creates a TransactionContext with a snapshot of the current storage state.
    /// Increments active count and total started metrics.
    ///
    /// # Arguments
    /// * `branch_id` - BranchId for namespace isolation
    /// * `storage` - Storage to create snapshot from
    ///
    /// # Returns
    /// * `TransactionContext` - Active transaction ready for operations
    pub fn start_transaction(
        &self,
        branch_id: BranchId,
        storage: &Arc<ShardedStore>,
    ) -> TransactionContext {
        let txn_id = self.manager.next_txn_id();
        let snapshot = storage.create_snapshot();

        self.active_count.fetch_add(1, Ordering::Relaxed);
        self.total_started.fetch_add(1, Ordering::Relaxed);

        debug!(target: "strata::txn", branch_id = %branch_id, "Transaction started");

        TransactionContext::with_snapshot(txn_id, branch_id, Box::new(snapshot))
    }

    /// Allocate commit version
    ///
    /// Per spec Section 6.1: Version incremented ONCE for the whole transaction.
    /// All keys in a transaction get the same commit version.
    pub fn allocate_commit_version(&self) -> u64 {
        self.manager.allocate_version()
    }

    /// Commit a transaction through the concurrency layer
    ///
    /// Delegates the full commit protocol to TransactionManager:
    /// - Per-run commit locking (TOCTOU prevention)
    /// - Validation (first-committer-wins)
    /// - Version allocation
    /// - WAL writing (when WAL is provided)
    /// - Storage application
    ///
    /// This method also handles:
    /// - Recording commit/abort metrics
    /// - Converting CommitError to StrataError
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit (must be in Active state)
    /// * `store` - Storage to validate against and apply writes to
    /// * `wal` - Optional WAL for durability. Pass `None` for ephemeral databases
    ///   or when durability is not required.
    ///
    /// # Returns
    /// * `Ok(commit_version)` - Transaction committed successfully
    /// * `Err(StrataError)` - Validation conflict, WAL error, or invalid state
    pub fn commit<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        wal: Option<&mut WalWriter>,
    ) -> StrataResult<u64> {
        match self.manager.commit(txn, store, wal) {
            Ok(version) => {
                self.record_commit();
                info!(target: "strata::txn", "Transaction committed");
                Ok(version)
            }
            Err(e) => {
                self.record_abort();
                warn!(target: "strata::txn", error = %e, "Transaction aborted");
                Err(StrataError::from(e))
            }
        }
    }

    /// Record transaction start
    ///
    /// Increments active count and total started count.
    /// Used by pooled transaction API that manages context creation separately.
    pub fn record_start(&self) {
        self.active_count.fetch_add(1, Ordering::Relaxed);
        self.total_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record transaction commit
    ///
    /// Decrements active count (saturating at 0) and increments committed count.
    pub fn record_commit(&self) {
        // Use fetch_update for saturating decrement to prevent underflow
        let _ = self
            .active_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                Some(x.saturating_sub(1))
            });
        self.total_committed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record transaction abort
    ///
    /// Decrements active count and increments aborted count.
    pub fn record_abort(&self) {
        // Use fetch_update for saturating decrement to prevent underflow
        let _ = self
            .active_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                Some(x.saturating_sub(1))
            });
        self.total_aborted.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current global version
    pub fn current_version(&self) -> u64 {
        self.manager.current_version()
    }

    /// Get next transaction ID (for internal use)
    pub fn next_txn_id(&self) -> u64 {
        self.manager.next_txn_id()
    }

    /// Remove the per-branch commit lock for a deleted branch.
    ///
    /// Delegates to `TransactionManager::remove_branch_lock` to prevent
    /// unbounded growth of the commit_locks map when branches are deleted.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) {
        self.manager.remove_branch_lock(branch_id);
    }

    /// Advance the version counter to at least `v`.
    ///
    /// Used during multi-process refresh to catch up with other processes.
    pub fn catch_up_version(&self, v: u64) {
        self.manager.catch_up_version(v);
    }

    /// Advance the txn_id counter to at least `id + 1`.
    ///
    /// Used during multi-process refresh to avoid ID collisions.
    pub fn catch_up_txn_id(&self, id: u64) {
        self.manager.catch_up_txn_id(id);
    }

    /// Commit a transaction with an externally-allocated version.
    ///
    /// Used by the coordinated commit path where versions are allocated
    /// from the shared counter file under the WAL file lock.
    pub fn commit_with_version<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        wal: Option<&mut WalWriter>,
        version: u64,
    ) -> StrataResult<u64> {
        match self.manager.commit_with_version(txn, store, wal, version) {
            Ok(v) => {
                self.record_commit();
                info!(target: "strata::txn", version = v, "Coordinated commit succeeded");
                Ok(v)
            }
            Err(e) => {
                self.record_abort();
                warn!(target: "strata::txn", error = %e, "Coordinated commit aborted");
                Err(StrataError::from(e))
            }
        }
    }

    /// Get transaction metrics
    ///
    /// Returns current snapshot of transaction statistics.
    pub fn metrics(&self) -> TransactionMetrics {
        let started = self.total_started.load(Ordering::Relaxed);
        let committed = self.total_committed.load(Ordering::Relaxed);

        TransactionMetrics {
            active_count: self.active_count.load(Ordering::Relaxed),
            total_started: started,
            total_committed: committed,
            total_aborted: self.total_aborted.load(Ordering::Relaxed),
            commit_rate: if started > 0 {
                committed as f64 / started as f64
            } else {
                0.0
            },
        }
    }

    /// Get current active transaction count
    pub fn active_count(&self) -> u64 {
        self.active_count.load(Ordering::SeqCst)
    }

    /// Wait for all active transactions to complete
    ///
    /// Spins with short sleeps until active_count reaches 0.
    /// Used during shutdown to ensure all in-flight transactions
    /// complete before flushing the WAL.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait
    ///
    /// # Returns
    /// * `true` if all transactions completed within timeout
    /// * `false` if timeout expired with transactions still active
    pub fn wait_for_idle(&self, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        let sleep_duration = std::time::Duration::from_millis(1);

        while self.active_count.load(Ordering::SeqCst) > 0 {
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(sleep_duration);
        }
        true
    }
}

/// Transaction metrics
///
/// Provides statistics about transaction lifecycle.
#[derive(Debug, Clone)]
pub struct TransactionMetrics {
    /// Number of currently active transactions
    pub active_count: u64,
    /// Total number of transactions started
    pub total_started: u64,
    /// Total number of transactions committed
    pub total_committed: u64,
    /// Total number of transactions aborted
    pub total_aborted: u64,
    /// Commit success rate (committed / started)
    pub commit_rate: f64,
}

impl TransactionMetrics {
    /// Total transactions that completed (committed + aborted)
    pub fn total_completed(&self) -> u64 {
        self.total_committed + self.total_aborted
    }

    /// Abort rate (aborted / started)
    pub fn abort_rate(&self) -> f64 {
        if self.total_started > 0 {
            self.total_aborted as f64 / self.total_started as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_storage() -> Arc<ShardedStore> {
        Arc::new(ShardedStore::new())
    }

    #[test]
    fn test_coordinator_new() {
        let coordinator = TransactionCoordinator::new(0);
        assert_eq!(coordinator.current_version(), 0);

        let metrics = coordinator.metrics();
        assert_eq!(metrics.active_count, 0);
        assert_eq!(metrics.total_started, 0);
        assert_eq!(metrics.total_committed, 0);
        assert_eq!(metrics.total_aborted, 0);
    }

    #[test]
    fn test_coordinator_from_recovery() {
        use strata_concurrency::RecoveryStats;

        let stats = RecoveryStats {
            txns_replayed: 5,
            incomplete_txns: 1,
            aborted_txns: 0,
            writes_applied: 10,
            deletes_applied: 2,
            final_version: 100,
            max_txn_id: 6,
            from_checkpoint: false,
        };

        let result = RecoveryResult {
            storage: ShardedStore::new(),
            txn_manager: TransactionManager::new(100),
            stats,
        };

        let coordinator = TransactionCoordinator::from_recovery(&result);
        assert_eq!(coordinator.current_version(), 100);
    }

    #[test]
    fn test_start_transaction_updates_metrics() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        let _txn1 = coordinator.start_transaction(branch_id, &storage);
        let _txn2 = coordinator.start_transaction(branch_id, &storage);

        let metrics = coordinator.metrics();
        assert_eq!(metrics.total_started, 2);
        assert_eq!(metrics.active_count, 2);
    }

    #[test]
    fn test_record_commit_updates_metrics() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        let _txn = coordinator.start_transaction(branch_id, &storage);
        coordinator.record_commit();

        let metrics = coordinator.metrics();
        assert_eq!(metrics.total_started, 1);
        assert_eq!(metrics.total_committed, 1);
        assert_eq!(metrics.active_count, 0);
        assert_eq!(metrics.commit_rate, 1.0);
    }

    #[test]
    fn test_record_abort_updates_metrics() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        let _txn = coordinator.start_transaction(branch_id, &storage);
        coordinator.record_abort();

        let metrics = coordinator.metrics();
        assert_eq!(metrics.total_started, 1);
        assert_eq!(metrics.total_aborted, 1);
        assert_eq!(metrics.active_count, 0);
        assert_eq!(metrics.commit_rate, 0.0);
    }

    #[test]
    fn test_version_monotonic() {
        let coordinator = TransactionCoordinator::new(100);

        let v1 = coordinator.allocate_commit_version();
        let v2 = coordinator.allocate_commit_version();
        let v3 = coordinator.allocate_commit_version();

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert_eq!(v1, 101);
        assert_eq!(v2, 102);
        assert_eq!(v3, 103);
    }

    #[test]
    fn test_metrics_helpers() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Start 4 transactions
        for _ in 0..4 {
            let _txn = coordinator.start_transaction(branch_id, &storage);
        }

        // 3 commit, 1 abort
        coordinator.record_commit();
        coordinator.record_commit();
        coordinator.record_commit();
        coordinator.record_abort();

        let metrics = coordinator.metrics();
        assert_eq!(metrics.total_completed(), 4);
        assert_eq!(metrics.abort_rate(), 0.25);
        assert_eq!(metrics.commit_rate, 0.75);
    }

    #[test]
    fn test_mixed_transactions() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Simulate realistic usage
        let _txn1 = coordinator.start_transaction(branch_id, &storage);
        let _txn2 = coordinator.start_transaction(branch_id, &storage);

        assert_eq!(coordinator.metrics().active_count, 2);

        coordinator.record_commit(); // txn1 commits

        assert_eq!(coordinator.metrics().active_count, 1);
        assert_eq!(coordinator.metrics().total_committed, 1);

        let _txn3 = coordinator.start_transaction(branch_id, &storage);

        assert_eq!(coordinator.metrics().active_count, 2);
        assert_eq!(coordinator.metrics().total_started, 3);

        coordinator.record_abort(); // txn2 aborts
        coordinator.record_commit(); // txn3 commits

        let metrics = coordinator.metrics();
        assert_eq!(metrics.active_count, 0);
        assert_eq!(metrics.total_started, 3);
        assert_eq!(metrics.total_committed, 2);
        assert_eq!(metrics.total_aborted, 1);
    }

    // ========== wait_for_idle Tests ==========

    #[test]
    fn test_wait_for_idle_no_active_transactions() {
        let coordinator = TransactionCoordinator::new(0);

        // No transactions active, should return immediately
        let result = coordinator.wait_for_idle(std::time::Duration::from_millis(100));
        assert!(
            result,
            "wait_for_idle should return true when no transactions are active"
        );
    }

    #[test]
    fn test_wait_for_idle_timeout_with_active_transaction() {
        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Start a transaction but don't complete it
        let _txn = coordinator.start_transaction(branch_id, &storage);
        assert_eq!(coordinator.active_count(), 1);

        // Wait with a short timeout - should return false
        let start = std::time::Instant::now();
        let result = coordinator.wait_for_idle(std::time::Duration::from_millis(50));
        let elapsed = start.elapsed();

        assert!(!result, "wait_for_idle should return false on timeout");
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "Should have waited at least 50ms, waited {:?}",
            elapsed
        );
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "Should not have waited too long, waited {:?}",
            elapsed
        );
    }

    #[test]
    fn test_wait_for_idle_transaction_completes_before_timeout() {
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Start a transaction
        let _txn = coordinator.start_transaction(branch_id, &storage);

        // Spawn a thread to complete the transaction after a short delay
        let coordinator_clone = Arc::clone(&coordinator);
        let completer = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(25));
            coordinator_clone.record_commit();
        });

        // Wait for idle with a longer timeout
        let start = std::time::Instant::now();
        let result = coordinator.wait_for_idle(std::time::Duration::from_millis(200));
        let elapsed = start.elapsed();

        completer.join().unwrap();

        assert!(
            result,
            "wait_for_idle should return true when transaction completes"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "Should have returned early when transaction completed, waited {:?}",
            elapsed
        );
    }

    #[test]
    fn test_wait_for_idle_multiple_transactions_complete() {
        use std::sync::Barrier;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Start 5 transactions
        for _ in 0..5 {
            let _txn = coordinator.start_transaction(branch_id, &storage);
        }
        assert_eq!(coordinator.active_count(), 5);

        // Spawn threads to complete transactions with staggered timing
        let barrier = Arc::new(Barrier::new(6)); // 5 completers + 1 waiter
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let coord = Arc::clone(&coordinator);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    thread::sleep(std::time::Duration::from_millis(10 * (i + 1) as u64));
                    coord.record_commit();
                })
            })
            .collect();

        // Wait for barrier, then wait for idle
        barrier.wait();
        let result = coordinator.wait_for_idle(std::time::Duration::from_millis(500));

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(
            result,
            "wait_for_idle should return true when all transactions complete"
        );
        assert_eq!(coordinator.active_count(), 0);
        assert_eq!(coordinator.metrics().total_committed, 5);
    }

    #[test]
    fn test_wait_for_idle_zero_timeout() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Start a transaction
        let _txn = coordinator.start_transaction(branch_id, &storage);

        // Zero timeout should return false immediately
        let start = std::time::Instant::now();
        let result = coordinator.wait_for_idle(std::time::Duration::ZERO);
        let elapsed = start.elapsed();

        assert!(
            !result,
            "wait_for_idle with zero timeout should return false"
        );
        // Should return very quickly (within a few milliseconds)
        assert!(
            elapsed < std::time::Duration::from_millis(10),
            "Zero timeout should return quickly, took {:?}",
            elapsed
        );
    }

    #[test]
    fn test_wait_for_idle_concurrent_start_and_complete() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Spawn a thread that rapidly starts and completes transactions
        let coord_clone = Arc::clone(&coordinator);
        let storage_clone = Arc::clone(&storage);
        let stop_clone = Arc::clone(&stop_flag);
        let worker = thread::spawn(move || {
            let mut completed = 0;
            while !stop_clone.load(Ordering::SeqCst) {
                let _txn = coord_clone.start_transaction(branch_id, &storage_clone);
                thread::yield_now();
                coord_clone.record_commit();
                completed += 1;
                if completed >= 50 {
                    break;
                }
            }
            completed
        });

        // Try to catch a moment when transactions are idle
        thread::sleep(std::time::Duration::from_millis(10));
        stop_flag.store(true, Ordering::SeqCst);

        // Give the worker time to finish
        let completed = worker.join().unwrap();

        // After worker stops, wait for idle should succeed
        let result = coordinator.wait_for_idle(std::time::Duration::from_millis(100));
        assert!(result, "Should eventually reach idle state");
        assert!(
            completed > 0,
            "Worker should have completed some transactions"
        );
    }

    #[test]
    fn test_active_count_accuracy_under_concurrent_load() {
        use std::sync::Barrier;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let barrier = Arc::new(Barrier::new(10));

        // 10 threads start transactions concurrently, then 10 threads complete them
        let mut handles = Vec::new();

        // Starters
        for _ in 0..10 {
            let coord = Arc::clone(&coordinator);
            let stor = Arc::clone(&storage);
            let barr = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barr.wait();
                let _txn = coord.start_transaction(branch_id, &stor);
                // Don't record_commit - leave active
            }));
        }

        // Wait for starters to finish
        for handle in handles {
            handle.join().unwrap();
        }

        // All 10 should be active
        assert_eq!(coordinator.active_count(), 10);
        assert_eq!(coordinator.metrics().total_started, 10);

        // Now complete them all concurrently
        let barrier2 = Arc::new(Barrier::new(10));
        let mut completers = Vec::new();

        for _ in 0..10 {
            let coord = Arc::clone(&coordinator);
            let barr = Arc::clone(&barrier2);
            completers.push(thread::spawn(move || {
                barr.wait();
                coord.record_commit();
            }));
        }

        for handle in completers {
            handle.join().unwrap();
        }

        // All should be complete
        assert_eq!(coordinator.active_count(), 0);
        assert_eq!(coordinator.metrics().total_committed, 10);
    }

    #[test]
    fn test_from_recovery_restores_txn_id() {
        use strata_concurrency::RecoveryStats;

        let stats = RecoveryStats {
            txns_replayed: 10,
            incomplete_txns: 2,
            aborted_txns: 1,
            writes_applied: 50,
            deletes_applied: 5,
            final_version: 500,
            max_txn_id: 15,
            from_checkpoint: false,
        };

        let result = RecoveryResult {
            storage: ShardedStore::new(),
            txn_manager: TransactionManager::new(500),
            stats,
        };

        let coordinator = TransactionCoordinator::from_recovery(&result);

        // Version should be restored
        assert_eq!(coordinator.current_version(), 500);

        // Next txn_id should be > max_txn_id from recovery
        let next_id = coordinator.next_txn_id();
        assert!(
            next_id > 15,
            "Next txn_id ({}) should be > max_txn_id from recovery (15)",
            next_id
        );
    }

    // ========================================================================
    // ADVERSARIAL TESTS - Bug Hunting
    // ========================================================================

    /// Verify active_count saturates at 0 instead of underflowing
    ///
    /// Previously, calling record_commit/abort more times than record_start
    /// would cause underflow (panic in debug, wrap in release).
    /// Now it saturates at 0 for defensive safety.
    #[test]
    fn test_active_count_saturates_at_zero() {
        let coordinator = TransactionCoordinator::new(0);

        // Start one transaction
        coordinator.record_start();
        assert_eq!(coordinator.active_count(), 1);

        // Commit it
        coordinator.record_commit();
        assert_eq!(coordinator.active_count(), 0);

        // Extra commits should saturate at 0, not underflow
        coordinator.record_commit();
        assert_eq!(
            coordinator.active_count(),
            0,
            "Should saturate at 0, not underflow"
        );

        coordinator.record_commit();
        assert_eq!(
            coordinator.active_count(),
            0,
            "Still 0 after multiple extra commits"
        );

        // Same for abort
        coordinator.record_abort();
        assert_eq!(coordinator.active_count(), 0, "Abort also saturates at 0");
    }

    /// BUG HUNT: Metrics consistency under high concurrency
    ///
    /// Since metrics use Relaxed ordering, they might show temporarily
    /// inconsistent values during concurrent operations.
    #[test]
    fn test_metrics_eventual_consistency() {
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();

        let iterations = 100;
        let started = Arc::new(AtomicUsize::new(0));
        let committed = Arc::new(AtomicUsize::new(0));

        // Spawn threads that start and commit transactions
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let coord = Arc::clone(&coordinator);
                let stor = Arc::clone(&storage);
                let started = Arc::clone(&started);
                let committed = Arc::clone(&committed);

                thread::spawn(move || {
                    for _ in 0..iterations {
                        let _txn = coord.start_transaction(branch_id, &stor);
                        started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        // Small delay to increase contention
                        thread::yield_now();

                        coord.record_commit();
                        committed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // After all threads complete, metrics should be consistent
        let metrics = coordinator.metrics();
        let total_expected = iterations * 4;

        assert_eq!(
            metrics.total_started, total_expected as u64,
            "Total started should match actual starts"
        );
        assert_eq!(
            metrics.total_committed, total_expected as u64,
            "Total committed should match actual commits"
        );
        assert_eq!(
            metrics.active_count, 0,
            "No transactions should be active after all complete"
        );
    }

    /// BUG HUNT: Version allocation monotonicity under concurrent allocations
    ///
    /// Multiple threads allocating versions should always get strictly
    /// increasing values with no duplicates.
    #[test]
    fn test_version_allocation_no_duplicates() {
        use parking_lot::Mutex;
        use std::collections::HashSet;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let versions = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let coord = Arc::clone(&coordinator);
                let vers = Arc::clone(&versions);

                thread::spawn(move || {
                    let mut local_versions = Vec::new();
                    for _ in 0..100 {
                        let v = coord.allocate_commit_version();
                        local_versions.push(v);
                    }
                    vers.lock().extend(local_versions);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let all_versions = versions.lock();
        let unique: HashSet<_> = all_versions.iter().collect();

        assert_eq!(
            all_versions.len(),
            unique.len(),
            "BUG: Duplicate versions allocated! Total: {}, Unique: {}",
            all_versions.len(),
            unique.len()
        );

        // Verify all versions are > 0 (initial version)
        for v in all_versions.iter() {
            assert!(*v > 0, "Version should be > initial version 0");
        }
    }

    /// BUG HUNT: Transaction ID monotonicity across concurrent allocations
    ///
    /// Similar to version allocation, transaction IDs must be unique.
    #[test]
    fn test_txn_id_allocation_no_duplicates() {
        use parking_lot::Mutex;
        use std::collections::HashSet;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let txn_ids = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let coord = Arc::clone(&coordinator);
                let ids = Arc::clone(&txn_ids);

                thread::spawn(move || {
                    let mut local_ids = Vec::new();
                    for _ in 0..100 {
                        let id = coord.next_txn_id();
                        local_ids.push(id);
                    }
                    ids.lock().extend(local_ids);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let all_ids = txn_ids.lock();
        let unique: HashSet<_> = all_ids.iter().collect();

        assert_eq!(
            all_ids.len(),
            unique.len(),
            "BUG: Duplicate transaction IDs! Total: {}, Unique: {}",
            all_ids.len(),
            unique.len()
        );
    }

    /// BUG HUNT: Commit rate calculation with zero started
    ///
    /// The commit_rate calculation divides by total_started.
    /// Verify it handles zero gracefully.
    #[test]
    fn test_commit_rate_with_zero_started() {
        let coordinator = TransactionCoordinator::new(0);

        let metrics = coordinator.metrics();

        // Should not panic, should return 0.0
        assert_eq!(metrics.commit_rate, 0.0);
        assert_eq!(metrics.abort_rate(), 0.0);
    }

    /// BUG HUNT: wait_for_idle with rapid start/stop cycles
    ///
    /// If transactions start and stop rapidly, wait_for_idle might
    /// see active_count as 0 briefly even though more transactions
    /// are about to start.
    #[test]
    fn test_wait_for_idle_spurious_return() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let should_stop = Arc::new(AtomicBool::new(false));

        // Worker that rapidly starts and commits transactions
        let coord_clone = Arc::clone(&coordinator);
        let stor_clone = Arc::clone(&storage);
        let stop_clone = Arc::clone(&should_stop);
        let worker = thread::spawn(move || {
            let mut count = 0;
            while !stop_clone.load(Ordering::SeqCst) && count < 50 {
                let _txn = coord_clone.start_transaction(branch_id, &stor_clone);
                // Very short delay
                coord_clone.record_commit();
                count += 1;
            }
            count
        });

        // Try to catch a zero-crossing
        let mut idle_seen = false;
        for _ in 0..100 {
            if coordinator.active_count() == 0 {
                idle_seen = true;
            }
            thread::yield_now();
        }

        should_stop.store(true, Ordering::SeqCst);
        let completed = worker.join().unwrap();

        // We should have seen idle at least once (between rapid transactions)
        // This documents that wait_for_idle could return during a brief idle window
        assert!(
            idle_seen || completed == 0,
            "Should see idle state between rapid transactions"
        );
    }

    /// Issue #1047: Verify that parking_lot::Mutex used for result collection
    /// in concurrent tests does not cascade panics from poisoned locks.
    ///
    /// With std::sync::Mutex, if one worker thread panics while holding the lock,
    /// all other threads calling .lock().unwrap() would also panic. parking_lot::Mutex
    /// does not poison, so surviving threads can still collect results.
    #[test]
    fn test_issue_1047_concurrent_collection_survives_thread_panic() {
        use parking_lot::Mutex;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let results = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let coord = Arc::clone(&coordinator);
                let res = Arc::clone(&results);

                thread::spawn(move || {
                    let mut local = Vec::new();
                    for _ in 0..10 {
                        let v = coord.allocate_commit_version();
                        local.push(v);
                    }

                    // Thread 0 panics after pushing to the shared mutex
                    res.lock().extend(local);
                    if i == 0 {
                        panic!("Simulated worker panic after collecting results");
                    }
                })
            })
            .collect();

        // Collect results, ignoring panicked thread
        for h in handles {
            let _ = h.join(); // Don't unwrap — thread 0 panicked
        }

        // With parking_lot, we can still access the mutex despite thread 0's panic
        let collected = results.lock();
        // All 4 threads pushed 10 versions each before any panic
        assert_eq!(
            collected.len(),
            40,
            "All threads should have collected results before panic"
        );
    }
}
