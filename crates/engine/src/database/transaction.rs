//! Transaction API and write backpressure.

use parking_lot::Mutex as ParkingMutex;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::types::BranchId;
use strata_core::{StrataError, StrataResult};
use strata_durability::wal::DurabilityMode;
use strata_durability::{ManifestManager, WalOnlyCompactor};
use strata_storage::SegmentedStore;

use super::Database;
use crate::transaction::TransactionPool;

impl Database {
    // Transaction API
    // ========================================================================

    /// Check if the database is accepting transactions.
    fn check_accepting(&self) -> StrataResult<()> {
        if !self.accepting_transactions.load(Ordering::Acquire) {
            return Err(StrataError::invalid_input(
                "Database is shutting down".to_string(),
            ));
        }
        Ok(())
    }

    /// Flush frozen memtables synchronously and schedule compaction in the background.
    ///
    /// Flush runs inline to free memtable memory. Compaction and materialization
    /// are deferred to the background scheduler so the writer thread is not
    /// blocked by potentially slow I/O (#1736).
    fn schedule_flush_if_needed(&self) {
        if self.storage.segments_dir().is_none() {
            return;
        }

        // Update snapshot floor so compaction respects active snapshots (#1697).
        self.storage.set_snapshot_floor(self.gc_safe_point());

        // 1. Flush frozen memtables synchronously (frees memtable memory).
        let branches = self.storage.branches_needing_flush();
        for branch_id in &branches {
            loop {
                match self.storage.flush_oldest_frozen(branch_id) {
                    Ok(true) => {
                        Self::update_flush_watermark(&self.storage, &self.data_dir, &self.wal_dir);
                    }
                    Ok(false) => break,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::flush",
                            ?branch_id,
                            error = %e,
                            "flush failed"
                        );
                        break;
                    }
                }
            }
        }

        // 2. Schedule compaction and materialization on background thread.
        self.schedule_background_compaction();
    }

    /// Submit a compaction + materialization task to the background scheduler.
    ///
    /// At most one background compaction task is in flight at a time. If one is
    /// already running, this call is a no-op — the running task will pick up any
    /// newly-flushed L0 segments.
    fn schedule_background_compaction(&self) {
        if self
            .compaction_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let storage = Arc::clone(&self.storage);
        let write_stall_cv = Arc::clone(&self.write_stall_cv);
        let flag = Arc::clone(&self.compaction_in_flight);

        if self
            .scheduler
            .submit(crate::background::TaskPriority::High, move || {
                // Compact all branches that are over target.
                let branch_ids = storage.branch_ids();
                for branch_id in &branch_ids {
                    loop {
                        match storage.pick_and_compact(branch_id, 0) {
                            Ok(Some(_)) => {
                                write_stall_cv.notify_all();
                            }
                            Ok(None) => break,
                            Err(e) => {
                                tracing::warn!(
                                    target: "strata::compact",
                                    ?branch_id,
                                    error = %e,
                                    "background compaction failed"
                                );
                                break;
                            }
                        }
                    }
                }

                // Materialize inherited layers that exceed depth limit (#1704).
                for branch_id in storage.branches_needing_materialization() {
                    let layer_count = storage.inherited_layer_count(&branch_id);
                    if layer_count > 0 {
                        let deepest = layer_count - 1;
                        match storage.materialize_layer(&branch_id, deepest) {
                            Ok(result) => {
                                tracing::info!(
                                    target: "strata::materialize",
                                    ?branch_id,
                                    entries = result.entries_materialized,
                                    segments = result.segments_created,
                                    "materialized inherited layer"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "strata::materialize",
                                    ?branch_id,
                                    error = %e,
                                    "materialization failed"
                                );
                            }
                        }
                    }
                }

                flag.store(false, Ordering::Release);
            })
            .is_err()
        {
            // Submit failed (queue full or shutdown) — clear the flag so future
            // compaction attempts are not permanently blocked.
            self.compaction_in_flight.store(false, Ordering::Release);
        }
    }

    /// Apply write backpressure when memtable memory exceeds safe limits.
    ///
    /// RocksDB-style write backpressure based on L0 file count and memtable
    /// pressure.  Called after every write commit, outside any storage guards.
    ///
    /// Three tiers (matching RocksDB semantics):
    /// 1. L0 count >= `l0_stop_writes_trigger` → wait on condvar until
    ///    compaction signals (complete stall).
    /// 2. L0 count >= `l0_slowdown_writes_trigger` → yield 1 ms per write.
    /// 3. Memtable bytes > threshold → yield 1 ms (OOM protection).
    #[inline]
    fn maybe_apply_write_backpressure(&self) -> StrataResult<()> {
        let cfg = self.config.read();

        // L0-based stalling (protects read latency)
        let l0_stop = cfg.storage.l0_stop_writes_trigger;
        let l0_slow = cfg.storage.l0_slowdown_writes_trigger;
        let timeout_ms = cfg.storage.write_stall_timeout_ms;
        drop(cfg); // release config lock before potential sleep

        let l0_count = self.storage.max_l0_segment_count();

        if l0_stop > 0 && l0_count >= l0_stop {
            // Complete stall: wait on condvar until compaction drains L0.
            // Also trigger compaction in case none is running.
            self.schedule_flush_if_needed();
            let stall_start = std::time::Instant::now();
            let timeout = std::time::Duration::from_millis(timeout_ms);

            let mut guard = self.write_stall_mu.lock();
            // Re-check after acquiring lock (compaction may have finished)
            while self.storage.max_l0_segment_count() >= l0_stop {
                if timeout_ms > 0 && stall_start.elapsed() >= timeout {
                    let current_l0 = self.storage.max_l0_segment_count();
                    tracing::warn!(
                        target: "strata::backpressure",
                        stall_ms = stall_start.elapsed().as_millis() as u64,
                        l0_count = current_l0,
                        "Write stall timeout — L0 compaction cannot keep up"
                    );
                    return Err(StrataError::write_stall_timeout(
                        stall_start.elapsed().as_millis() as u64,
                        current_l0,
                    ));
                }
                self.write_stall_cv
                    .wait_for(&mut guard, std::time::Duration::from_millis(10));
            }
            return Ok(());
        }

        if l0_slow > 0 && l0_count >= l0_slow {
            std::thread::sleep(std::time::Duration::from_millis(1));
            return Ok(());
        }

        // Memtable-based stalling (protects memory usage)
        let cfg = self.config.read();
        let wbs = cfg.storage.write_buffer_size as u64;
        let max_frozen = cfg.storage.max_immutable_memtables as u64;
        drop(cfg);

        if wbs > 0 {
            let threshold = wbs * (max_frozen + 2);
            let current = self.storage.total_memtable_bytes();
            if current > threshold {
                std::thread::sleep(std::time::Duration::from_millis(1));
                return Ok(());
            }
        }

        // Segment metadata stalling: bloom + index pinned outside block cache.
        // When this overhead exceeds the block cache budget, stall to let
        // compaction merge segments and reduce metadata footprint.
        // Use the resolved global capacity (handles auto-detect when config is 0).
        let cache_cap = strata_storage::block_cache::global_capacity() as u64;
        if cache_cap > 0 {
            let seg_meta = self.storage.total_segment_metadata_bytes();
            if seg_meta > cache_cap {
                self.schedule_flush_if_needed();
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }

        Ok(())
    }

    /// Update the MANIFEST flush watermark and truncate WAL segments below it.
    ///
    /// Computes the global flush watermark as the minimum `max_flushed_commit`
    /// across all branches. WAL segments fully below this watermark are safe
    /// to delete because their data is in segments.
    ///
    /// Best-effort: errors are logged, not propagated.
    fn update_flush_watermark(storage: &SegmentedStore, data_dir: &Path, wal_dir: &Path) {
        // Compute global flush watermark: min of max_flushed_commit across all branches
        let branch_ids = storage.branch_ids();
        if branch_ids.is_empty() {
            return;
        }

        // Compute watermark: min of max_flushed_commit across branches
        // that participate in the flush pipeline (have segments).
        // Branches with no segments (e.g. _system_) are excluded — their
        // data is small and replayed from WAL on recovery.
        let mut watermark = u64::MAX;
        let mut has_any_segments = false;
        for bid in &branch_ids {
            if let Some(commit) = storage.max_flushed_commit(bid) {
                if commit > 0 {
                    watermark = watermark.min(commit);
                    has_any_segments = true;
                }
            }
            // Branches with no segments are intentionally excluded
        }
        if !has_any_segments || watermark == u64::MAX {
            return;
        }

        // Update MANIFEST
        let manifest_path = data_dir.join("MANIFEST");
        let mut mgr = match ManifestManager::load(manifest_path) {
            Ok(mgr) => mgr,
            Err(e) => {
                tracing::debug!(
                    target: "strata::flush",
                    error = %e,
                    "No MANIFEST found, skipping WAL truncation"
                );
                return;
            }
        };

        if let Err(e) = mgr.set_flush_watermark(watermark) {
            tracing::warn!(
                target: "strata::flush",
                error = %e,
                watermark,
                "Failed to update flush watermark in MANIFEST"
            );
            return; // Don't truncate WAL if watermark wasn't persisted
        }

        // Truncate WAL segments below watermark
        let manifest_arc = Arc::new(ParkingMutex::new(mgr));
        let compactor = WalOnlyCompactor::new(wal_dir.to_path_buf(), manifest_arc);
        match compactor.compact() {
            Ok(info) => {
                if info.wal_segments_removed > 0 {
                    tracing::debug!(
                        target: "strata::wal",
                        segments_removed = info.wal_segments_removed,
                        bytes_reclaimed = info.reclaimed_bytes,
                        watermark,
                        "WAL segments truncated after flush"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "strata::wal",
                    error = %e,
                    "WAL compaction failed after flush"
                );
            }
        }
    }

    /// Execute one transaction attempt: commit on success, abort on error.
    ///
    /// Handles the commit-or-abort decision and coordinator bookkeeping.
    /// The caller is responsible for calling `end_transaction()` afterward.
    ///
    /// Returns `(closure_result, commit_version)` on success.
    fn run_single_attempt<T>(
        &self,
        txn: &mut TransactionContext,
        result: StrataResult<T>,
        durability: DurabilityMode,
    ) -> StrataResult<(T, u64)> {
        match result {
            Ok(value) => {
                let had_writes = !txn.is_read_only();
                // Admission control: reject writes when L0 is saturated (#1924).
                // Must run BEFORE commit so the caller gets a clean error and
                // no data is committed that the caller doesn't know about.
                if had_writes {
                    if let Err(e) = self.maybe_apply_write_backpressure() {
                        let _ = txn.mark_aborted(format!("Write stall: {}", e));
                        self.coordinator.record_abort(txn.txn_id);
                        return Err(e);
                    }
                }
                let commit_version = self.commit_internal(txn, durability)?;
                // Schedule flush only for write transactions (reads skip entirely)
                if had_writes {
                    self.schedule_flush_if_needed();
                }
                Ok((value, commit_version))
            }
            Err(e) => {
                let _ = txn.mark_aborted(format!("Closure error: {}", e));
                self.coordinator.record_abort(txn.txn_id);
                Err(e)
            }
        }
    }

    /// Execute a transaction with the given closure
    ///
    /// Per spec Section 4:
    /// - Creates TransactionContext with snapshot
    /// - Executes closure with transaction
    /// - Validates and commits on success
    /// - Aborts on error
    ///
    /// # Arguments
    /// * `branch_id` - BranchId for namespace isolation
    /// * `f` - Closure that performs transaction operations
    ///
    /// # Returns
    /// * `Ok(T)` - Closure return value on successful commit
    /// * `Err` - On validation conflict or closure error
    ///
    /// # Example
    /// ```text
    /// let result = db.transaction(branch_id, |txn| {
    ///     let val = txn.get(&key)?;
    ///     txn.put(key, new_value)?;
    ///     Ok(val)
    /// })?;
    /// ```
    pub fn transaction<F, T>(&self, branch_id: BranchId, f: F) -> StrataResult<T>
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<T>,
    {
        self.check_accepting()?;
        let mut txn = self.begin_transaction(branch_id)?;
        let result = f(&mut txn);
        let outcome = self.run_single_attempt(&mut txn, result, self.durability_mode);
        self.end_transaction(txn);
        outcome.map(|(value, _)| value)
    }

    /// Execute a transaction and return both the result and commit version
    ///
    /// Like `transaction()` but also returns the commit version assigned to all writes.
    /// Use this when you need to know the version created by write operations.
    ///
    /// # Returns
    /// * `Ok((T, u64))` - Closure result and commit version
    /// * `Err` - On validation conflict or closure error
    ///
    /// # Example
    /// ```text
    /// let (result, commit_version) = db.transaction_with_version(branch_id, |txn| {
    ///     txn.put(key, value)?;
    ///     Ok("success")
    /// })?;
    /// // commit_version now contains the version assigned to the put
    /// ```
    pub(crate) fn transaction_with_version<F, T>(
        &self,
        branch_id: BranchId,
        f: F,
    ) -> StrataResult<(T, u64)>
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<T>,
    {
        self.check_accepting()?;
        let mut txn = self.begin_transaction(branch_id)?;
        let result = f(&mut txn);
        let outcome = self.run_single_attempt(&mut txn, result, self.durability_mode);
        self.end_transaction(txn);
        outcome
    }

    /// Begin a new transaction (for manual control)
    ///
    /// Returns a TransactionContext that must be manually committed or aborted.
    /// Prefer `transaction()` closure API for automatic handling.
    ///
    /// Uses thread-local pool to avoid allocation overhead after warmup.
    /// Call `end_transaction()` after commit/abort to return context to pool.
    ///
    /// # Arguments
    /// * `branch_id` - BranchId for namespace isolation
    ///
    /// # Returns
    /// * `TransactionContext` - Active transaction ready for operations
    ///
    /// # Example
    /// ```text
    /// let mut txn = db.begin_transaction(branch_id)?;
    /// txn.put(key, value)?;
    /// db.commit_transaction(&mut txn)?;
    /// db.end_transaction(txn); // Return to pool
    /// ```
    pub fn begin_transaction(&self, branch_id: BranchId) -> StrataResult<TransactionContext> {
        self.check_accepting()?;
        let txn_id = self.coordinator.next_txn_id()?;
        // Use storage.version() as the snapshot (includes own thread's commits),
        // but wait for visible_version to catch up so all data at versions
        // ≤ snapshot is fully applied. This prevents the cross-branch snapshot
        // gap (#1913) without breaking same-thread sequential operations.
        let snapshot_version = self.storage.version();
        let mut spins = 0u32;
        while self.coordinator.visible_version() < snapshot_version {
            spins += 1;
            if spins < 16 {
                std::hint::spin_loop();
            } else if spins < 1000 {
                std::thread::yield_now();
            } else {
                // Safety valve: a panicked commit thread could leave a version
                // permanently pending, stalling visible_version forever.
                //
                // IMPORTANT: we must NOT fall back to visible_version here.
                // visible_version can lag behind storage.version() due to
                // slow (not panicked) commits on other branches. Regressing
                // the snapshot below our own prior commits causes spurious
                // OCC conflicts — the version-bounded snapshot read returns
                // an older version than the unbounded validation check sees,
                // breaking branch isolation.
                //
                // Instead, keep the original snapshot_version. The gap only
                // affects cross-branch reads where some version V < snapshot
                // hasn't been applied yet. Per-branch transactions (the common
                // case) are unaffected because our own branch's data is fully
                // applied synchronously. Cross-branch transactions (delete,
                // fork) hold the commit lock or quiesce guard, which drains
                // all pending versions before proceeding.
                break;
            }
        }
        self.coordinator.record_start(txn_id, snapshot_version);

        let mut txn = TransactionPool::acquire(txn_id, branch_id, Some(Arc::clone(&self.storage)));
        // Override start_version set by pool acquire (which reads storage.version()
        // directly, bypassing the visible_version wait) (#1913).
        txn.start_version = snapshot_version;
        txn.set_max_write_entries(self.coordinator.max_write_buffer_entries());
        Ok(txn)
    }

    /// Begin a read-only transaction
    ///
    /// Returns a transaction that rejects writes and skips read-set tracking,
    /// saving memory on large scan workloads.
    pub fn begin_read_only_transaction(
        &self,
        branch_id: BranchId,
    ) -> StrataResult<TransactionContext> {
        let mut txn = self.begin_transaction(branch_id)?;
        txn.set_read_only(true);
        Ok(txn)
    }

    /// End a transaction (return to pool)
    ///
    /// Returns the transaction context to the thread-local pool for reuse.
    /// This avoids allocation overhead on subsequent transactions.
    ///
    /// Should be called after `commit_transaction()` or after aborting.
    /// The closure API (`transaction()`) calls this automatically.
    ///
    /// # Arguments
    /// * `ctx` - Transaction context to return to pool
    ///
    /// # Example
    /// ```text
    /// let mut txn = db.begin_transaction(branch_id)?;
    /// txn.put(key, value)?;
    /// db.commit_transaction(&mut txn)?;
    /// db.end_transaction(txn); // Return to pool for reuse
    /// ```
    pub fn end_transaction(&self, ctx: TransactionContext) {
        TransactionPool::release(ctx);
    }

    /// Commit a transaction
    ///
    /// Per spec commit sequence:
    /// 1. Validate (conflict detection)
    /// 2. Allocate commit version
    /// 3. Write to WAL (BeginTxn, Writes, CommitTxn)
    /// 4. Apply to storage
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit
    ///
    /// # Returns
    /// * `Ok(commit_version)` - Transaction committed successfully, returns commit version
    /// * `Err(TransactionConflict)` - Validation failed, transaction aborted
    ///
    /// # Errors
    /// - `TransactionConflict` - Read-write or CAS conflict detected
    /// - `InvalidState` - Transaction not in Active state
    ///
    /// # Contract
    /// Returns the commit version (u64) assigned to all writes in this transaction.
    pub fn commit_transaction(&self, txn: &mut TransactionContext) -> StrataResult<u64> {
        let had_writes = !txn.is_read_only();
        // Admission control: reject writes when L0 is saturated (#1924).
        // Must run BEFORE commit so the caller gets a clean error.
        if had_writes {
            if let Err(e) = self.maybe_apply_write_backpressure() {
                let _ = txn.mark_aborted(format!("Write stall: {}", e));
                self.coordinator.record_abort(txn.txn_id);
                return Err(e);
            }
        }
        let version = self.commit_internal(txn, self.durability_mode)?;
        if had_writes {
            self.schedule_flush_if_needed();
        }
        Ok(version)
    }

    /// Internal commit implementation shared by commit_transaction and transaction closures
    ///
    /// Delegates the commit protocol to the concurrency layer (TransactionManager)
    /// via the TransactionCoordinator. The engine is responsible only for:
    /// - Determining whether to pass the WAL (based on durability mode + persistence)
    ///
    /// The concurrency layer handles:
    /// - Per-run commit locking (TOCTOU prevention)
    /// - Validation (first-committer-wins)
    /// - Version allocation
    /// - WAL writing (when WAL reference is provided)
    /// - Storage application
    /// - Fsync (WAL::append handles fsync based on its DurabilityMode)
    fn commit_internal(
        &self,
        txn: &mut TransactionContext,
        durability: DurabilityMode,
    ) -> StrataResult<u64> {
        if self.follower {
            return Err(StrataError::internal(
                "cannot commit: database opened in follower mode (read-only)",
            ));
        }

        let wal_arc = if durability.requires_wal() {
            self.wal_writer.as_ref()
        } else {
            None
        };

        self.coordinator
            .commit_with_wal_arc(txn, self.storage.as_ref(), wal_arc)
    }
}
