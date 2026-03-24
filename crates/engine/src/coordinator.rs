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

use parking_lot::Mutex as ParkingMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strata_concurrency::{RecoveryResult, TransactionContext, TransactionManager};
use strata_core::traits::Storage;
use strata_core::types::BranchId;
use strata_core::StrataError;
use strata_core::StrataResult;
use strata_durability::wal::WalWriter;
use strata_storage::SegmentedStore;
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
    /// Lock-free GC safe point: the version at which all prior transactions
    /// have completed. Advances via `fetch_max` only when active_count drains
    /// to 0, ensuring it is always ≤ the minimum active snapshot version.
    /// Initialized to 0 (no drain has occurred yet).
    gc_safe_version: AtomicU64,
    /// Maximum entries in a transaction's write buffer (0 = unlimited).
    max_write_buffer_entries: usize,
    /// Effective transaction timeout. Defaults to `TRANSACTION_TIMEOUT`.
    transaction_timeout: Duration,
}

impl TransactionCoordinator {
    /// Maximum allowed transaction duration before commit is rejected.
    ///
    /// Transactions that exceed this duration will fail at commit time
    /// with a `TransactionTimeout` error. This prevents long-running
    /// transactions from blocking GC indefinitely.
    pub const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes
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
            gc_safe_version: AtomicU64::new(0),
            max_write_buffer_entries: 0,
            transaction_timeout: Self::TRANSACTION_TIMEOUT,
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
            gc_safe_version: AtomicU64::new(0),
            max_write_buffer_entries: 0,
            transaction_timeout: Self::TRANSACTION_TIMEOUT,
        }
    }

    /// Create coordinator from recovery result with write buffer limits.
    pub fn from_recovery_with_limits(
        result: &RecoveryResult,
        max_write_buffer_entries: usize,
    ) -> Self {
        Self {
            manager: TransactionManager::with_txn_id(
                result.stats.final_version,
                result.stats.max_txn_id,
            ),
            active_count: AtomicU64::new(0),
            total_started: AtomicU64::new(0),
            total_committed: AtomicU64::new(0),
            total_aborted: AtomicU64::new(0),
            gc_safe_version: AtomicU64::new(0),
            max_write_buffer_entries,
            transaction_timeout: Self::TRANSACTION_TIMEOUT,
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
        storage: &Arc<SegmentedStore>,
    ) -> StrataResult<TransactionContext> {
        let txn_id = self.manager.next_txn_id().map_err(StrataError::from)?;

        // Register in active_count BEFORE creating the snapshot so that a
        // concurrent drain (active_count → 0) cannot set gc_safe_version
        // past our about-to-be-created snapshot version.
        self.active_count.fetch_add(1, Ordering::Relaxed);
        self.total_started.fetch_add(1, Ordering::Relaxed);

        debug!(target: "strata::txn", branch_id = %branch_id, "Transaction started");

        let mut txn = TransactionContext::with_store(txn_id, branch_id, Arc::clone(storage));
        txn.set_max_write_entries(self.max_write_buffer_entries);
        Ok(txn)
    }

    /// Enforce the transaction timeout. If expired, records an abort
    /// (decrementing active_count) and returns `TransactionTimeout`.
    fn enforce_timeout(&self, txn: &TransactionContext) -> StrataResult<()> {
        if txn.is_expired(self.transaction_timeout) {
            let elapsed = txn.elapsed();
            warn!(
                target: "strata::txn",
                txn_id = txn.txn_id,
                elapsed_ms = elapsed.as_millis() as u64,
                timeout_ms = self.transaction_timeout.as_millis() as u64,
                "Transaction expired — rejecting commit"
            );
            self.record_abort(txn.txn_id);
            return Err(StrataError::transaction_timeout(elapsed.as_millis() as u64));
        }
        Ok(())
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
        self.enforce_timeout(txn)?;
        let txn_id = txn.txn_id;
        self.handle_commit_result(txn_id, self.manager.commit(txn, store, wal))
    }

    /// Commit a transaction with narrow WAL lock scope.
    ///
    /// Same as `commit()` but takes `Arc<Mutex<WalWriter>>` so the WAL lock
    /// is held only for the append I/O, not for validation or storage apply.
    pub fn commit_with_wal_arc<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        wal_arc: Option<&Arc<ParkingMutex<WalWriter>>>,
    ) -> StrataResult<u64> {
        self.enforce_timeout(txn)?;
        let txn_id = txn.txn_id;
        self.handle_commit_result(
            txn_id,
            self.manager.commit_with_wal_arc(txn, store, wal_arc),
        )
    }

    /// Handle the result of a commit attempt: record metrics and convert errors.
    fn handle_commit_result(
        &self,
        txn_id: u64,
        result: Result<u64, strata_concurrency::CommitError>,
    ) -> StrataResult<u64> {
        match result {
            Ok(version) => {
                self.record_commit(txn_id);
                info!(target: "strata::txn", version, "Transaction committed");
                Ok(version)
            }
            Err(e) => {
                self.record_abort(txn_id);
                warn!(target: "strata::txn", error = %e, "Transaction aborted");
                Err(StrataError::from(e))
            }
        }
    }

    /// Record transaction start
    ///
    /// Increments active count and total started count.
    ///
    /// # Arguments
    /// * `_txn_id` - Unique transaction ID (unused, kept for API compat)
    /// * `_start_version` - Snapshot version at transaction start (unused)
    pub fn record_start(&self, _txn_id: u64, _start_version: u64) {
        self.active_count.fetch_add(1, Ordering::Relaxed);
        self.total_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record transaction commit
    ///
    /// Decrements active count, increments committed count,
    /// and advances `gc_safe_version` when all transactions have drained.
    ///
    /// # Panics (debug only)
    /// Debug-asserts that `active_count > 0`. A zero active_count at this
    /// point indicates a bug (missing `record_start`).
    ///
    /// # Arguments
    /// * `_txn_id` - Transaction ID (unused, kept for API compat)
    pub fn record_commit(&self, _txn_id: u64) {
        // Capture version BEFORE the decrement so gc_safe_version is always
        // ≤ the snapshot version of any concurrently-starting transaction.
        // A transaction that starts between our version read and the
        // decrement will have a snapshot version ≥ drain_version.
        let drain_version = self.manager.current_version();

        // Single LOCK XADD instruction — no CAS loop. Underflow is a bug
        // (record_start always precedes record_commit), so debug-assert.
        let prev = self.active_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "active_count underflow in record_commit");
        self.total_committed.fetch_add(1, Ordering::Relaxed);
        if prev == 1 {
            // All transactions drained — advance GC safe point
            self.gc_safe_version
                .fetch_max(drain_version, Ordering::Release);
        }
    }

    /// Record transaction abort
    ///
    /// Decrements active count, increments aborted count,
    /// and advances `gc_safe_version` when all transactions have drained.
    ///
    /// # Panics (debug only)
    /// Debug-asserts that `active_count > 0`. A zero active_count at this
    /// point indicates a bug (missing `record_start`).
    ///
    /// # Arguments
    /// * `_txn_id` - Transaction ID (unused, kept for API compat)
    pub fn record_abort(&self, _txn_id: u64) {
        // Capture version BEFORE the decrement (same rationale as record_commit)
        let drain_version = self.manager.current_version();

        // Single LOCK XADD instruction — no CAS loop. Underflow is a bug
        // (record_start always precedes record_abort), so debug-assert.
        let prev = self.active_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "active_count underflow in record_abort");
        self.total_aborted.fetch_add(1, Ordering::Relaxed);
        if prev == 1 {
            // All transactions drained — advance GC safe point
            self.gc_safe_version
                .fetch_max(drain_version, Ordering::Release);
        }
    }

    /// Get current global version
    pub fn current_version(&self) -> u64 {
        self.manager.current_version()
    }

    /// Ensure the version counter is at least `floor` (#1726).
    ///
    /// Called after segment recovery to prevent version collisions when the
    /// WAL has been fully compacted.
    pub fn bump_version_floor(&self, floor: u64) {
        self.manager.bump_version_floor(floor);
    }

    /// Get the current version after draining all in-flight commits (#1710).
    ///
    /// Safe to use as a checkpoint watermark — no version ≤ the returned
    /// value has storage application still in progress.
    pub fn quiesced_version(&self) -> u64 {
        self.manager.quiesced_version()
    }

    /// Get next transaction ID (for internal use)
    pub fn next_txn_id(&self) -> StrataResult<u64> {
        self.manager.next_txn_id().map_err(StrataError::from)
    }

    /// Get the configured max write buffer entries limit.
    pub fn max_write_buffer_entries(&self) -> usize {
        self.max_write_buffer_entries
    }

    /// Set the max write buffer entries limit.
    pub fn set_max_write_buffer_entries(&mut self, max: usize) {
        self.max_write_buffer_entries = max;
    }

    /// Remove the per-branch commit lock for a deleted branch.
    ///
    /// Delegates to `TransactionManager::remove_branch_lock` to prevent
    /// unbounded growth of the commit_locks map when branches are deleted.
    /// Returns `true` if removed (or didn't exist), `false` if skipped
    /// because a concurrent commit is in-flight.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) -> bool {
        self.manager.remove_branch_lock(branch_id)
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
        self.enforce_timeout(txn)?;
        let txn_id = txn.txn_id;
        self.handle_commit_result(
            txn_id,
            self.manager.commit_with_version(txn, store, wal, version),
        )
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
        self.active_count.load(Ordering::Relaxed)
    }

    /// Conservative lower bound on the minimum snapshot version held by
    /// any active transaction.
    ///
    /// Returns `None` when no GC constraint exists (no drain has ever
    /// occurred). The returned value may be lower than the true minimum —
    /// GC is conservative, never pruning versions that an active
    /// transaction might need.
    pub fn min_active_version(&self) -> Option<u64> {
        let v = self.gc_safe_version.load(Ordering::Acquire);
        if v == 0 {
            None
        } else {
            Some(v)
        }
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
impl TransactionCoordinator {
    /// Allocate commit version (test-only)
    pub fn allocate_commit_version(&self) -> StrataResult<u64> {
        self.manager.allocate_version().map_err(StrataError::from)
    }

    /// Override the transaction timeout (test-only).
    pub fn set_transaction_timeout(&mut self, timeout: Duration) {
        self.transaction_timeout = timeout;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_storage() -> Arc<SegmentedStore> {
        Arc::new(SegmentedStore::new())
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
            storage: SegmentedStore::new(),
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

        let _txn1 = coordinator.start_transaction(branch_id, &storage).unwrap();
        let _txn2 = coordinator.start_transaction(branch_id, &storage).unwrap();

        let metrics = coordinator.metrics();
        assert_eq!(metrics.total_started, 2);
        assert_eq!(metrics.active_count, 2);
    }

    #[test]
    fn test_record_commit_updates_metrics() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        let txn = coordinator.start_transaction(branch_id, &storage).unwrap();
        coordinator.record_commit(txn.txn_id);

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

        let txn = coordinator.start_transaction(branch_id, &storage).unwrap();
        coordinator.record_abort(txn.txn_id);

        let metrics = coordinator.metrics();
        assert_eq!(metrics.total_started, 1);
        assert_eq!(metrics.total_aborted, 1);
        assert_eq!(metrics.active_count, 0);
        assert_eq!(metrics.commit_rate, 0.0);
    }

    #[test]
    fn test_version_monotonic() {
        let coordinator = TransactionCoordinator::new(100);

        let v1 = coordinator.allocate_commit_version().unwrap();
        let v2 = coordinator.allocate_commit_version().unwrap();
        let v3 = coordinator.allocate_commit_version().unwrap();

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

        // Start 4 transactions, collect txn_ids
        let mut txn_ids = Vec::new();
        for _ in 0..4 {
            let txn = coordinator.start_transaction(branch_id, &storage).unwrap();
            txn_ids.push(txn.txn_id);
        }

        // 3 commit, 1 abort
        coordinator.record_commit(txn_ids[0]);
        coordinator.record_commit(txn_ids[1]);
        coordinator.record_commit(txn_ids[2]);
        coordinator.record_abort(txn_ids[3]);

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
        let txn1 = coordinator.start_transaction(branch_id, &storage).unwrap();
        let txn2 = coordinator.start_transaction(branch_id, &storage).unwrap();

        assert_eq!(coordinator.metrics().active_count, 2);

        coordinator.record_commit(txn1.txn_id); // txn1 commits

        assert_eq!(coordinator.metrics().active_count, 1);
        assert_eq!(coordinator.metrics().total_committed, 1);

        let txn3 = coordinator.start_transaction(branch_id, &storage).unwrap();

        assert_eq!(coordinator.metrics().active_count, 2);
        assert_eq!(coordinator.metrics().total_started, 3);

        coordinator.record_abort(txn2.txn_id); // txn2 aborts
        coordinator.record_commit(txn3.txn_id); // txn3 commits

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
        let _txn = coordinator.start_transaction(branch_id, &storage).unwrap();
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
        let txn = coordinator.start_transaction(branch_id, &storage).unwrap();
        let txn_id = txn.txn_id;

        // Spawn a thread to complete the transaction after a short delay
        let coordinator_clone = Arc::clone(&coordinator);
        let completer = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(25));
            coordinator_clone.record_commit(txn_id);
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

        // Start 5 transactions, collect txn_ids
        let mut txn_ids = Vec::new();
        for _ in 0..5 {
            let txn = coordinator.start_transaction(branch_id, &storage).unwrap();
            txn_ids.push(txn.txn_id);
        }
        assert_eq!(coordinator.active_count(), 5);

        // Spawn threads to complete transactions with staggered timing
        let barrier = Arc::new(Barrier::new(6)); // 5 completers + 1 waiter
        let handles: Vec<_> = txn_ids
            .into_iter()
            .enumerate()
            .map(|(i, txn_id)| {
                let coord = Arc::clone(&coordinator);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    thread::sleep(std::time::Duration::from_millis(10 * (i + 1) as u64));
                    coord.record_commit(txn_id);
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
        let _txn = coordinator.start_transaction(branch_id, &storage).unwrap();

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
        let storage = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Spawn a thread that rapidly starts and completes transactions
        let coord_clone = Arc::clone(&coordinator);
        let storage_clone = Arc::clone(&storage);
        let stop_clone = Arc::clone(&stop_flag);
        let worker = thread::spawn(move || {
            let mut completed = 0;
            while !stop_clone.load(Ordering::SeqCst) {
                let txn = coord_clone
                    .start_transaction(branch_id, &storage_clone)
                    .unwrap();
                thread::yield_now();
                coord_clone.record_commit(txn.txn_id);
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
        use parking_lot::Mutex;
        use std::sync::Barrier;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let storage = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let barrier = Arc::new(Barrier::new(10));
        let txn_ids = Arc::new(Mutex::new(Vec::new()));

        // 10 threads start transactions concurrently, then 10 threads complete them
        let mut handles = Vec::new();

        // Starters
        for _ in 0..10 {
            let coord = Arc::clone(&coordinator);
            let stor = Arc::clone(&storage);
            let barr = Arc::clone(&barrier);
            let ids = Arc::clone(&txn_ids);
            handles.push(thread::spawn(move || {
                barr.wait();
                let txn = coord.start_transaction(branch_id, &stor).unwrap();
                ids.lock().push(txn.txn_id);
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
        let collected_ids = txn_ids.lock().clone();
        let mut completers = Vec::new();

        for txn_id in collected_ids {
            let coord = Arc::clone(&coordinator);
            let barr = Arc::clone(&barrier2);
            completers.push(thread::spawn(move || {
                barr.wait();
                coord.record_commit(txn_id);
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
            storage: SegmentedStore::new(),
            txn_manager: TransactionManager::new(500),
            stats,
        };

        let coordinator = TransactionCoordinator::from_recovery(&result);

        // Version should be restored
        assert_eq!(coordinator.current_version(), 500);

        // Next txn_id should be > max_txn_id from recovery
        let next_id = coordinator.next_txn_id().unwrap();
        assert!(
            next_id > 15,
            "Next txn_id ({}) should be > max_txn_id from recovery (15)",
            next_id
        );
    }

    // ========================================================================
    // ADVERSARIAL TESTS - Bug Hunting
    // ========================================================================

    /// Verify active_count tracks correctly through balanced start/commit/abort
    /// cycles and that GC safe point advances on drain.
    #[test]
    fn test_active_count_balanced_start_commit() {
        let coordinator = TransactionCoordinator::new(0);

        // Start one transaction, commit it
        coordinator.record_start(100, 0);
        assert_eq!(coordinator.active_count(), 1);
        coordinator.record_commit(100);
        assert_eq!(coordinator.active_count(), 0);

        // Start and abort
        coordinator.record_start(101, 0);
        assert_eq!(coordinator.active_count(), 1);
        coordinator.record_abort(101);
        assert_eq!(coordinator.active_count(), 0);

        // Interleaved: 3 starts, then 2 commits + 1 abort
        coordinator.record_start(200, 0);
        coordinator.record_start(201, 0);
        coordinator.record_start(202, 0);
        assert_eq!(coordinator.active_count(), 3);

        coordinator.record_commit(200);
        assert_eq!(coordinator.active_count(), 2);
        coordinator.record_abort(201);
        assert_eq!(coordinator.active_count(), 1);
        // Last one drains — should be 0 after
        coordinator.record_commit(202);
        assert_eq!(coordinator.active_count(), 0);
    }

    /// Verify that GC safe point advances when active_count drains to 0
    /// via fetch_sub (prev == 1 triggers drain detection).
    #[test]
    fn test_gc_safe_point_advances_after_fetch_sub_drain() {
        let coordinator = TransactionCoordinator::new(0);

        // Bump version so gc_safe_version can advance
        let _ = coordinator.allocate_commit_version().unwrap(); // version → 1

        // Start and commit a single transaction — drains active_count
        coordinator.record_start(1, 0);
        coordinator.record_commit(1);

        // GC safe point should have advanced (drain_version = 1)
        assert!(
            coordinator.min_active_version().is_some(),
            "GC safe point should advance after drain"
        );
        assert_eq!(coordinator.min_active_version().unwrap(), 1);

        // Now with multiple transactions: only the LAST drain advances
        let _ = coordinator.allocate_commit_version().unwrap(); // version → 2
        coordinator.record_start(2, 0);
        coordinator.record_start(3, 0);
        // Commit first — active_count goes from 2 → 1, prev=2, no drain
        coordinator.record_commit(2);
        // GC safe point should still be at 1 (no drain yet)
        assert_eq!(coordinator.min_active_version().unwrap(), 1);
        // Commit second — active_count goes from 1 → 0, prev=1, drain!
        coordinator.record_commit(3);
        assert_eq!(coordinator.min_active_version().unwrap(), 2);
    }

    /// Verify concurrent start/commit cycles don't corrupt active_count
    /// with the fetch_sub implementation (no CAS loop).
    #[test]
    fn test_active_count_fetch_sub_concurrent_correctness() {
        use std::sync::Arc;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(0));
        let num_threads = 8;
        let iterations = 500;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let coord = Arc::clone(&coordinator);
                thread::spawn(move || {
                    for i in 0..iterations {
                        let id = (t * iterations + i) as u64;
                        coord.record_start(id, 0);
                        // Small amount of "work"
                        std::hint::black_box(id);
                        if i % 3 == 0 {
                            coord.record_abort(id);
                        } else {
                            coord.record_commit(id);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // All transactions balanced — active_count must be exactly 0
        assert_eq!(
            coordinator.active_count(),
            0,
            "active_count must be 0 after all balanced start/commit|abort cycles"
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "active_count underflow in record_commit")]
    fn test_active_count_underflow_panics_in_debug_commit() {
        let coordinator = TransactionCoordinator::new(0);
        // Commit without a preceding start — should panic in debug builds
        coordinator.record_commit(999);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "active_count underflow in record_abort")]
    fn test_active_count_underflow_panics_in_debug_abort() {
        let coordinator = TransactionCoordinator::new(0);
        // Abort without a preceding start — should panic in debug builds
        coordinator.record_abort(999);
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
        let storage = Arc::new(SegmentedStore::new());
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
                        let txn = coord.start_transaction(branch_id, &stor).unwrap();
                        started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        // Small delay to increase contention
                        thread::yield_now();

                        coord.record_commit(txn.txn_id);
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
                        let v = coord.allocate_commit_version().unwrap();
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
                        let id = coord.next_txn_id().unwrap();
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
        let storage = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let should_stop = Arc::new(AtomicBool::new(false));

        // Worker that rapidly starts and commits transactions
        let coord_clone = Arc::clone(&coordinator);
        let stor_clone = Arc::clone(&storage);
        let stop_clone = Arc::clone(&should_stop);
        let worker = thread::spawn(move || {
            let mut count = 0;
            while !stop_clone.load(Ordering::SeqCst) && count < 50 {
                let txn = coord_clone
                    .start_transaction(branch_id, &stor_clone)
                    .unwrap();
                // Very short delay
                coord_clone.record_commit(txn.txn_id);
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
                        let v = coord.allocate_commit_version().unwrap();
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

    // ========================================================================
    // GC Safe Version Tests
    // ========================================================================

    #[test]
    fn test_min_active_version_empty() {
        let coordinator = TransactionCoordinator::new(0);
        // No transactions ever started → gc_safe_version is 0 (sentinel) → returns None
        assert_eq!(coordinator.min_active_version(), None);
    }

    #[test]
    fn test_min_active_version_tracks_correctly() {
        let coordinator = TransactionCoordinator::new(10);

        // Register 3 transactions
        coordinator.record_start(1, 10);
        coordinator.record_start(2, 10);
        coordinator.record_start(3, 10);

        // While transactions are active, gc_safe_version hasn't advanced yet
        // (no full drain has occurred), so min_active_version returns None
        assert_eq!(coordinator.min_active_version(), None);

        // Committing first two doesn't cause a full drain (active_count stays > 0)
        coordinator.record_commit(1);
        assert_eq!(
            coordinator.min_active_version(),
            None,
            "partial drain should not advance gc_safe_version"
        );

        coordinator.record_commit(2);
        assert_eq!(
            coordinator.min_active_version(),
            None,
            "still one active txn, no drain"
        );

        // Committing last → full drain → gc_safe_version advances to current_version (10)
        coordinator.record_commit(3);
        assert_eq!(
            coordinator.min_active_version(),
            Some(10),
            "full drain should set gc_safe_version to current_version"
        );
    }

    #[test]
    fn test_min_active_version_after_commit() {
        let coordinator = TransactionCoordinator::new(5);

        // Start and fully drain a transaction → gc_safe_version advances
        coordinator.record_start(1, 5);
        coordinator.record_commit(1);

        // gc_safe_version should be exactly 5 (current_version at drain time)
        assert_eq!(
            coordinator.min_active_version(),
            Some(5),
            "gc_safe_version should be 5 after drain at version 5"
        );

        // Allocate some versions to advance current_version
        let _ = coordinator.allocate_commit_version(); // 6
        let _ = coordinator.allocate_commit_version(); // 7

        // gc_safe_version should still be 5 (no new drain)
        assert_eq!(
            coordinator.min_active_version(),
            Some(5),
            "allocating versions without drain should not change gc_safe_version"
        );

        // Start and drain again → gc_safe_version advances to 7
        coordinator.record_start(2, 7);
        coordinator.record_commit(2);

        assert_eq!(
            coordinator.min_active_version(),
            Some(7),
            "gc_safe_version should advance to 7 after second drain"
        );
    }

    #[test]
    fn test_active_versions_cleaned_on_abort() {
        // Use non-zero initial version so drain produces a non-sentinel gc_safe_version
        let coordinator = TransactionCoordinator::new(5);

        // Start a transaction then abort it — triggers drain
        coordinator.record_start(1, 5);
        assert_eq!(coordinator.active_count(), 1);

        coordinator.record_abort(1);
        assert_eq!(coordinator.active_count(), 0);

        // After abort with full drain, gc_safe_version should be exactly 5
        assert_eq!(
            coordinator.min_active_version(),
            Some(5),
            "abort drain should set gc_safe_version to current_version (5)"
        );
    }

    #[test]
    fn test_gc_safe_version_advances_on_full_drain() {
        let coordinator = TransactionCoordinator::new(10);

        // Start 3 transactions at version 10
        coordinator.record_start(1, 10);
        coordinator.record_start(2, 10);
        coordinator.record_start(3, 10);

        // Partial commits don't trigger drain
        coordinator.record_commit(1);
        assert_eq!(
            coordinator.min_active_version(),
            None,
            "2 still active, no drain"
        );

        coordinator.record_commit(2);
        assert_eq!(
            coordinator.min_active_version(),
            None,
            "1 still active, no drain"
        );

        // Final commit → full drain → gc_safe_version = 10
        coordinator.record_commit(3);
        assert_eq!(
            coordinator.min_active_version(),
            Some(10),
            "full drain should set gc_safe_version to 10"
        );
    }

    #[test]
    fn test_gc_safe_version_conservative_while_active() {
        let coordinator = TransactionCoordinator::new(5);

        // Drain once to establish a gc_safe_version
        coordinator.record_start(1, 5);
        coordinator.record_commit(1);
        assert_eq!(coordinator.min_active_version(), Some(5));

        // Advance version and start new transaction
        let _ = coordinator.allocate_commit_version(); // 6
        let _ = coordinator.allocate_commit_version(); // 7

        coordinator.record_start(2, 7);

        // While txn 2 is active, gc_safe_version stays at the old value
        // (no new drain has occurred)
        assert_eq!(
            coordinator.min_active_version(),
            Some(5),
            "gc_safe_version must not advance while txns are active"
        );

        // Commit txn 2 → drain → gc_safe_version advances to 7
        coordinator.record_commit(2);
        assert_eq!(
            coordinator.min_active_version(),
            Some(7),
            "gc_safe_version should advance after second drain"
        );
    }

    /// Test gc_safe_version monotonicity: multiple drain cycles with
    /// increasing versions should produce monotonically increasing safe points.
    #[test]
    fn test_gc_safe_version_monotonically_increases() {
        let coordinator = TransactionCoordinator::new(1);

        let mut last_safe = 0u64;

        for round in 0..5 {
            // Start and drain a transaction
            coordinator.record_start(round + 1, 0);
            let _ = coordinator.allocate_commit_version();
            coordinator.record_commit(round + 1);

            if let Some(v) = coordinator.min_active_version() {
                assert!(
                    v >= last_safe,
                    "gc_safe_version went backwards: {} < {} at round {}",
                    v,
                    last_safe,
                    round
                );
                last_safe = v;
            }
        }

        assert!(last_safe > 0, "should have advanced at least once");
    }

    /// Concurrent start/commit with gc_safe_version: verify gc_safe_version
    /// never exceeds the snapshot version of any active transaction.
    #[test]
    fn test_gc_safe_version_concurrent_safety() {
        use std::sync::Barrier;
        use std::thread;

        let coordinator = Arc::new(TransactionCoordinator::new(100));
        let storage = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();

        // Allocate some versions first so gc_safe_version has something to track
        for _ in 0..10 {
            let _ = coordinator.allocate_commit_version();
        }

        let barrier = Arc::new(Barrier::new(8));

        // 8 threads each do 200 start/commit cycles
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let coord = Arc::clone(&coordinator);
                let stor = Arc::clone(&storage);
                let barr = Arc::clone(&barrier);

                thread::spawn(move || {
                    barr.wait();
                    for _ in 0..200 {
                        let txn = coord.start_transaction(branch_id, &stor).unwrap();
                        // Simulate brief work
                        thread::yield_now();
                        coord.record_commit(txn.txn_id);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // After all threads complete:
        // 1. active_count must be 0
        assert_eq!(
            coordinator.active_count(),
            0,
            "all txns should have completed"
        );
        // 2. gc_safe_version should have advanced (Some, not None)
        let v = coordinator.min_active_version();
        assert!(
            v.is_some(),
            "gc_safe_version should have advanced after concurrent drains"
        );
        // 3. gc_safe_version should be ≤ current_version (never overshoots)
        let current = coordinator.current_version();
        assert!(
            v.unwrap() <= current,
            "gc_safe_version ({}) must not exceed current_version ({})",
            v.unwrap(),
            current
        );
        // 4. Metrics should be consistent
        let metrics = coordinator.metrics();
        assert_eq!(metrics.total_started, 8 * 200);
        assert_eq!(metrics.total_committed, 8 * 200);
    }

    /// Issue #1728: TRANSACTION_TIMEOUT must be enforced at commit time.
    /// An expired transaction must be rejected with TransactionTimeout error
    /// and its active_count must be decremented (preventing GC stall).
    #[test]
    fn test_issue_1728_expired_transaction_rejected_at_commit() {
        use std::time::Duration;

        let mut coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Set a very short timeout for testing
        coordinator.set_transaction_timeout(Duration::from_millis(1));

        let mut txn = coordinator.start_transaction(branch_id, &storage).unwrap();
        assert_eq!(coordinator.active_count(), 1);

        // Wait for the transaction to expire
        std::thread::sleep(Duration::from_millis(5));
        assert!(txn.is_expired(Duration::from_millis(1)));

        // Commit should be rejected with TransactionTimeout
        let result = coordinator.commit_with_wal_arc(&mut txn, storage.as_ref(), None);
        assert!(result.is_err(), "expired transaction must be rejected");
        let err = result.unwrap_err();
        assert!(
            matches!(err, StrataError::TransactionTimeout { .. }),
            "expected TransactionTimeout, got: {err}"
        );

        // active_count must have been decremented (the whole point of #1728)
        assert_eq!(
            coordinator.active_count(),
            0,
            "expired transaction must decrement active_count to unblock GC"
        );
    }

    /// Issue #1728: A non-expired transaction must still commit successfully.
    #[test]
    fn test_issue_1728_non_expired_transaction_commits() {
        let coordinator = TransactionCoordinator::new(0);
        let storage = create_test_storage();
        let branch_id = BranchId::new();

        // Default timeout is 5 minutes — transaction is not expired
        let mut txn = coordinator.start_transaction(branch_id, &storage).unwrap();

        let result = coordinator.commit_with_wal_arc(&mut txn, storage.as_ref(), None);
        assert!(
            result.is_ok(),
            "non-expired transaction must commit: {result:?}"
        );
        assert_eq!(coordinator.active_count(), 0);
    }

    /// Test that interleaved start/commit from multiple threads with
    /// version allocations in between results in a valid gc_safe_version
    /// that is always ≤ current_version.
    #[test]
    fn test_gc_safe_version_with_interleaved_version_bumps() {
        let coordinator = Arc::new(TransactionCoordinator::new(1));
        let storage = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();

        // Phase 1: Start 3 transactions, allocate versions between them
        let txn1 = coordinator.start_transaction(branch_id, &storage).unwrap();
        let _ = coordinator.allocate_commit_version(); // version 2
        let txn2 = coordinator.start_transaction(branch_id, &storage).unwrap();
        let _ = coordinator.allocate_commit_version(); // version 3
        let txn3 = coordinator.start_transaction(branch_id, &storage).unwrap();

        assert_eq!(coordinator.active_count(), 3);
        assert_eq!(coordinator.min_active_version(), None); // no drain yet

        // Phase 2: Commit in reverse order — txn3 first, txn1 last
        coordinator.record_commit(txn3.txn_id);
        assert_eq!(coordinator.min_active_version(), None); // still 2 active

        coordinator.record_commit(txn2.txn_id);
        assert_eq!(coordinator.min_active_version(), None); // still 1 active

        coordinator.record_commit(txn1.txn_id); // drain!

        // gc_safe_version should be exactly current_version at drain time
        let v = coordinator.min_active_version().unwrap();
        let current = coordinator.current_version();
        assert!(
            v <= current,
            "gc_safe_version ({}) must be ≤ current_version ({})",
            v,
            current
        );
        assert_eq!(
            v, 3,
            "gc_safe_version should be 3 (current version at drain time)"
        );
    }
}
