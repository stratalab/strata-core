//! Transaction API and write backpressure.

use crate::{StrataError, StrataResult};
use parking_lot::Mutex as ParkingMutex;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use strata_core::{EntityRef, Version};
use strata_durability::wal::DurabilityMode;
use strata_durability::{ManifestManager, WalOnlyCompactor};
use strata_storage::{SegmentedStore, TransactionContext};

use super::Database;
use crate::branch_ops::branch_control_store::{active_ptr_key, BranchControlStore};
use crate::transaction::json_state::{JsonTxnState, MaterializedJsonWrite};
use crate::transaction::{Transaction, TransactionPool};

/// Return freed heap pages to the OS.
///
/// After memtable flush or compaction, glibc's allocator retains freed
/// SkipMap pages in its free lists — they stay in VmRSS even though they're
/// logically freed. `malloc_trim(0)` returns those pages to the OS,
/// immediately reducing RSS. This is critical for memory-constrained
/// deployments (Pi Zero, drones, edge devices). See issue #2184.
///
/// Thread-safe: `malloc_trim` acquires arena locks internally.
/// Cost: O(free chunks), typically ~hundreds of μs — negligible vs flush I/O.
#[cfg(target_os = "linux")]
fn release_freed_memory() {
    // SAFETY: malloc_trim is a well-defined glibc function with no
    // preconditions. The argument 0 means "return as much as possible".
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(not(target_os = "linux"))]
fn release_freed_memory() {
    // macOS and other allocators return pages eagerly; no action needed.
}

/// Maximum consecutive no-work rounds before the compaction chain releases
/// the in-flight flag. During bulk loads, new L0 files arrive every ~2-3s.
/// Keeping the chain alive avoids the 500ms+ scheduler queue latency of
/// re-submitting through `schedule_background_compaction`.
const MAX_IDLE_ROUNDS: u32 = 5;

/// One round of the self-re-scheduling compaction chain.
///
/// Each invocation picks the highest-scoring compaction across all branches,
/// executes it, then re-submits itself. When no work is found, the chain
/// stays alive for a few more rounds (re-submitting idle checks through the
/// scheduler) to catch newly-flushed L0 files without the re-submission
/// latency. After `MAX_IDLE_ROUNDS` consecutive no-work rounds, the chain
/// releases `compaction_in_flight` and stops.
fn compaction_round(
    storage: Arc<SegmentedStore>,
    write_stall_cv: Arc<parking_lot::Condvar>,
    flag: Arc<std::sync::atomic::AtomicBool>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    scheduler: Arc<crate::background::BackgroundScheduler>,
    idle_count: u32,
) {
    if cancelled.load(Ordering::Acquire) {
        flag.store(false, Ordering::Release);
        return;
    }

    let did_work = pick_and_run_one(&storage, &write_stall_cv);

    if did_work {
        resubmit_chain(storage, write_stall_cv, flag, cancelled, scheduler, 0);
    } else if idle_count < MAX_IDLE_ROUNDS {
        if idle_count == 0 {
            if !cancelled.load(Ordering::Acquire) {
                run_materialization(&storage);
            }
            release_freed_memory();
        }
        resubmit_chain(
            storage,
            write_stall_cv,
            flag,
            cancelled,
            scheduler,
            idle_count + 1,
        );
    } else {
        // Exceeded idle limit — release the flag.
        flag.store(false, Ordering::Release);
    }
}

/// Re-submit the compaction chain to the scheduler.
fn resubmit_chain(
    storage: Arc<SegmentedStore>,
    write_stall_cv: Arc<parking_lot::Condvar>,
    flag: Arc<std::sync::atomic::AtomicBool>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    scheduler: Arc<crate::background::BackgroundScheduler>,
    idle_count: u32,
) {
    let s = Arc::clone(&storage);
    let w = Arc::clone(&write_stall_cv);
    let f = Arc::clone(&flag);
    let c = Arc::clone(&cancelled);
    let sch = Arc::clone(&scheduler);
    if scheduler
        .submit(crate::background::TaskPriority::High, move || {
            compaction_round(s, w, f, c, sch, idle_count);
        })
        .is_err()
    {
        flag.store(false, Ordering::Release);
    }
}

/// Pick the single highest-scoring compaction across all branches and execute it.
///
/// Returns `true` if a compaction was performed.
fn pick_and_run_one(storage: &SegmentedStore, write_stall_cv: &parking_lot::Condvar) -> bool {
    let mut best_score = 0.0f64;
    let mut best_branch = None;

    for branch_id in storage.branch_ids() {
        let scores = storage.compute_compaction_scores(&branch_id);
        if let Some(top) = scores.first() {
            if top.score >= 1.0 && top.score > best_score {
                best_score = top.score;
                best_branch = Some(branch_id);
            }
        }
    }

    if let Some(branch_id) = best_branch {
        match storage.pick_and_compact(&branch_id, CommitVersion::ZERO) {
            Ok(Some(_result)) => {
                write_stall_cv.notify_all();
                true
            }
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(
                    target: "strata::compact",
                    ?branch_id,
                    error = %e,
                    "background compaction failed"
                );
                false
            }
        }
    } else {
        false
    }
}

/// Run materialization for branches with deep inherited layer chains (#1704).
fn run_materialization(storage: &SegmentedStore) {
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
}

impl Database {
    // Transaction API
    // ========================================================================

    /// Check if the database is accepting transactions.
    ///
    /// Returns an error if:
    /// - The database is shutting down (`InvalidInput`)
    /// - The WAL writer is halted due to sync failure (`WriterHalted`)
    fn check_accepting(&self) -> StrataResult<()> {
        if let Some(err) = self.storage_publish_error() {
            return Err(err);
        }

        if !self.accepting_transactions.load(Ordering::Acquire) {
            // Check if writer is halted (more specific error)
            if let Some(err) = self.writer_halted_error() {
                return Err(err);
            }

            // Not halted, must be shutting down
            return Err(StrataError::invalid_input(
                "Database is shutting down".to_string(),
            ));
        }
        Ok(())
    }

    /// Synchronous flush of all frozen memtables. Used only during write
    /// backpressure to ensure L0 count is accurate before stall decisions.
    /// Normal writes use the async path in `schedule_flush_if_needed`.
    fn flush_frozen_sync(&self) -> StrataResult<()> {
        let branches = self.storage.branches_needing_flush();
        for branch_id in &branches {
            loop {
                match self.storage.flush_oldest_frozen(branch_id) {
                    Ok(true) => {
                        Self::update_flush_watermark(
                            &self.storage,
                            &self.data_dir,
                            &self.wal_dir,
                            self.wal_codec.as_ref(),
                        );
                    }
                    Ok(false) => break,
                    Err(e) => {
                        let message = format!("sync flush failed for branch {branch_id}: {e}");
                        self.storage.latch_publish_health(message.clone());
                        tracing::warn!(
                            target: "strata::flush",
                            ?branch_id,
                            error = %e,
                            "sync flush failed"
                        );
                        return Err(StrataError::storage_with_source(message, e));
                    }
                }
            }
        }
        release_freed_memory();
        Ok(())
    }

    /// Schedule flush and compaction on the background thread.
    ///
    /// Frozen memtable flush is deferred to the background scheduler to avoid
    /// blocking the writer thread with SST build + disk I/O. Writes stall only
    /// when L0 segment count exceeds `l0_stop_writes_trigger` (backpressure).
    fn schedule_flush_if_needed(&self) {
        if self.storage.segments_dir().is_none() {
            return;
        }

        // Update snapshot floor so compaction respects active snapshots (#1697).
        self.storage.set_snapshot_floor(self.gc_safe_point());

        // Coalesce flush scheduling: skip if a flush task is already in flight.
        // The in-flight task drains ALL frozen memtables (loop in the closure),
        // so submitting redundant tasks during fast ingest just wastes scheduler
        // queue capacity.
        //
        // Wake-up bit pattern: the in-flight task re-checks for new frozen
        // memtables after clearing its flag, in case a writer raced in between
        // the final drain and the flag clear (TOCTOU). This guarantees no
        // frozen memtable is permanently orphaned.
        if self.storage.total_frozen_count() > 0
            && self
                .flush_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            let storage = Arc::clone(&self.storage);
            let data_dir = self.data_dir.clone();
            let wal_dir = self.wal_dir.clone();
            let flush_flag = Arc::clone(&self.flush_in_flight);
            let wal_codec = strata_durability::codec::clone_codec(self.wal_codec.as_ref());
            let _ = self
                .scheduler
                .submit(crate::background::TaskPriority::High, move || {
                    // Drain all known frozen memtables, with a bounded number
                    // of rounds to handle the TOCTOU race where a writer
                    // creates a new frozen memtable between branches_needing_flush
                    // enumeration and drain completion. In practice 1-2 rounds.
                    const MAX_ROUNDS: usize = 32;
                    for _round in 0..MAX_ROUNDS {
                        let branches = storage.branches_needing_flush();
                        if branches.is_empty() {
                            break;
                        }
                        for branch_id in &branches {
                            loop {
                                match storage.flush_oldest_frozen(branch_id) {
                                    Ok(true) => {
                                        Self::update_flush_watermark(
                                            &storage,
                                            &data_dir,
                                            &wal_dir,
                                            wal_codec.as_ref(),
                                        );
                                    }
                                    Ok(false) => break,
                                    Err(e) => {
                                        storage.latch_publish_health(format!(
                                            "background flush failed for branch {branch_id}: {e}"
                                        ));
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
                    }

                    // Clear the in-flight flag. If a writer raced in between
                    // our last check and now, the next write's
                    // schedule_flush_if_needed will see the flag clear and
                    // submit a fresh task.
                    flush_flag.store(false, Ordering::Release);

                    // Return freed memtable pages to the OS (#2184).
                    release_freed_memory();
                });
        }

        // Always check compaction — also runs materialization for COW branches.
        self.schedule_background_compaction();
    }

    /// Submit a compaction task to the background scheduler.
    ///
    /// At most one compaction chain is in flight at a time. Each task in the
    /// chain performs ONE compaction (highest-scoring branch × level), then
    /// re-submits itself if more work exists. This matches RocksDB's model:
    /// one compaction per task, re-evaluate between rounds.
    pub(super) fn schedule_background_compaction(&self) {
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
        let cancelled = Arc::clone(&self.compaction_cancelled);
        let scheduler = Arc::clone(&self.scheduler);
        let scheduler2 = Arc::clone(&scheduler);

        if scheduler
            .submit(crate::background::TaskPriority::High, move || {
                compaction_round(storage, write_stall_cv, flag, cancelled, scheduler2, 0);
            })
            .is_err()
        {
            self.compaction_in_flight.store(false, Ordering::Release);
        }
    }

    /// Apply write backpressure when memtable memory exceeds safe limits.
    ///
    /// How often to run expensive backpressure checks (memtable bytes, segment
    /// metadata). These values change only on flush/compaction, not per write.
    /// Checking every write wastes 200-300μs iterating all segments.
    const BACKPRESSURE_EXPENSIVE_INTERVAL: u64 = 64;

    /// RocksDB-style write backpressure based on L0 file count and memtable
    /// pressure. Called after every write commit, outside any storage guards.
    ///
    /// Three tiers (matching RocksDB semantics):
    ///
    /// 1. L0 count >= `l0_stop_writes_trigger` — wait on condvar until
    ///    compaction signals (complete stall).
    /// 2. L0 count >= `l0_slowdown_writes_trigger` — yield 1 ms per write.
    /// 3. Memtable bytes > threshold — yield 1 ms (OOM protection).
    fn maybe_apply_write_backpressure(&self) -> StrataResult<()> {
        let cfg = self.config.read();

        // L0-based stalling (protects read latency) — always check when active.
        let l0_stop = cfg.storage.l0_stop_writes_trigger;
        let l0_slow = cfg.storage.l0_slowdown_writes_trigger;
        let timeout_ms = cfg.storage.write_stall_timeout_ms;
        drop(cfg); // release config lock before potential sleep

        // L0 checks run every write when thresholds are active (correctness).
        if l0_stop > 0 || l0_slow > 0 {
            // If frozen memtables are pending, drain them synchronously so
            // L0 count is accurate for backpressure.
            if self.storage.total_frozen_count() > 0 {
                self.flush_frozen_sync()?;
            }
            let l0_count = self.storage.max_l0_segment_count();

            if l0_stop > 0 && l0_count >= l0_stop {
                // Complete stall: wait on condvar until compaction drains L0.
                self.schedule_background_compaction();
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
        }

        // Expensive checks: memtable bytes and segment metadata.
        // These iterate all branches/segments so we sample every N writes.
        let count = self.backpressure_counter.fetch_add(1, Ordering::Relaxed);
        if count % Self::BACKPRESSURE_EXPENSIVE_INTERVAL != 0 {
            return Ok(());
        }

        // Memtable-based stalling (protects memory usage)
        let cfg = self.config.read();
        let wbs = cfg.storage.effective_write_buffer_size() as u64;
        let max_frozen = cfg.storage.effective_max_immutable_memtables() as u64;
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
    /// `wal_codec` threads the same codec the runtime uses so retention
    /// decisions on non-identity-codec databases don't diverge from the
    /// shipped v3 envelope format (D2 / DG-001).
    ///
    /// Best-effort: errors are logged, not propagated.
    fn update_flush_watermark(
        storage: &SegmentedStore,
        data_dir: &Path,
        wal_dir: &Path,
        wal_codec: &dyn strata_durability::codec::StorageCodec,
    ) {
        // Compute global flush watermark: min of max_flushed_commit across all branches
        let branch_ids = storage.branch_ids();
        if branch_ids.is_empty() {
            return;
        }

        // Compute watermark: min of max_flushed_commit across branches
        // that participate in the flush pipeline (have segments).
        // Branches with no segments (e.g. _system_) are excluded — their
        // data is small and replayed from WAL on recovery.
        let mut watermark = CommitVersion::MAX;
        let mut has_any_segments = false;
        for bid in &branch_ids {
            if let Some(commit) = storage.max_flushed_commit(bid) {
                if commit > CommitVersion::ZERO {
                    watermark = watermark.min(commit);
                    has_any_segments = true;
                }
            }
            // Branches with no segments are intentionally excluded
        }
        if !has_any_segments || watermark == CommitVersion::MAX {
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
                watermark = watermark.as_u64(),
                "Failed to update flush watermark in MANIFEST"
            );
            return; // Don't truncate WAL if watermark wasn't persisted
        }

        // Truncate WAL segments below watermark
        let manifest_arc = Arc::new(ParkingMutex::new(mgr));
        let compactor = WalOnlyCompactor::new(wal_dir.to_path_buf(), manifest_arc)
            .with_codec(strata_durability::codec::clone_codec(wal_codec));
        match compactor.compact() {
            Ok(info) => {
                if info.wal_segments_removed > 0 {
                    tracing::debug!(
                        target: "strata::wal",
                        segments_removed = info.wal_segments_removed,
                        bytes_reclaimed = info.reclaimed_bytes,
                        watermark = watermark.as_u64(),
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
    /// Returns `(closure_result, commit_version)` on success.
    fn run_single_attempt<T>(
        &self,
        txn: &mut TransactionContext,
        result: StrataResult<T>,
    ) -> StrataResult<(T, u64)> {
        match result {
            Ok(value) => {
                let commit_version = self.commit_transaction_context(txn)?;
                Ok((value, commit_version))
            }
            Err(e) => {
                self.abort_transaction_in_place(txn, format!("Closure error: {}", e));
                Err(e)
            }
        }
    }

    /// Mark a transaction aborted, optionally update coordinator state, and
    /// run subsystem cleanup observers.
    fn finalize_aborted_transaction_in_place(
        &self,
        txn: &mut TransactionContext,
        reason: impl Into<String>,
        record_coordinator_abort: bool,
    ) {
        if txn.is_active() {
            let _ = txn.mark_aborted(reason.into());
            if record_coordinator_abort {
                self.coordinator.record_abort(txn.txn_id);
            }
        }

        let info = super::AbortInfo {
            txn_id: txn.txn_id,
            branch_id: txn.branch_id,
        };
        self.abort_observers().notify(&info);
    }

    /// Mark a transaction aborted and run subsystem cleanup observers.
    pub(crate) fn abort_transaction_in_place(
        &self,
        txn: &mut TransactionContext,
        reason: impl Into<String>,
    ) {
        self.finalize_aborted_transaction_in_place(txn, reason, true);
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
        let mut txn = self.begin_transaction_context(branch_id)?;
        let result = f(&mut txn);
        let outcome = self.run_single_attempt(&mut txn, result);
        self.end_transaction_context(txn);
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
        let mut txn = self.begin_transaction_context(branch_id)?;
        let result = f(&mut txn);
        let outcome = self.run_single_attempt(&mut txn, result);
        self.end_transaction_context(txn);
        outcome
    }

    /// Begin a new transaction (for manual control)
    ///
    /// Returns an owned transaction handle that commits, aborts, and returns
    /// its context to the pool without executor/session state.
    ///
    /// # Arguments
    /// * `branch_id` - BranchId for namespace isolation
    ///
    /// # Returns
    /// * `Transaction` - Active transaction ready for operations
    ///
    /// # Example
    /// ```text
    /// let mut txn = db.begin_transaction(branch_id)?;
    /// txn.put(key, value)?;
    /// txn.commit()?;
    /// ```
    pub fn begin_transaction(&self, branch_id: BranchId) -> StrataResult<Transaction> {
        let txn = self.begin_transaction_context(branch_id)?;
        Ok(Transaction::new(Self::shared(self), txn))
    }

    /// Internal transaction-context constructor used by closure APIs.
    pub(crate) fn begin_transaction_context(
        &self,
        branch_id: BranchId,
    ) -> StrataResult<TransactionContext> {
        self.check_accepting()?;
        let txn_id = self.coordinator.next_txn_id()?;
        // Use storage.version() as the snapshot (includes own thread's commits),
        // but wait for visible_version to catch up so all data at versions
        // ≤ snapshot is fully applied. This prevents the cross-branch snapshot
        // gap (#1913) without breaking same-thread sequential operations.
        let snapshot_version = CommitVersion(self.storage.version());
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
        let branch_generation_guard = self.branch_generation_guard_for(branch_id)?;
        txn.set_branch_generation_guard(branch_generation_guard);
        let seeded = self.seed_branch_generation_read_guard(&mut txn)?;
        txn.set_branch_generation_guard_key(
            branch_generation_guard.map(|_| active_ptr_key(branch_id)),
            seeded,
        );
        txn.set_max_write_entries(self.coordinator.max_write_buffer_entries());
        Ok(txn)
    }

    /// Begin a read-only transaction
    ///
    /// Returns a transaction that rejects writes and skips read-set tracking,
    /// saving memory on large scan workloads.
    pub fn begin_read_only_transaction(&self, branch_id: BranchId) -> StrataResult<Transaction> {
        let txn = self.begin_read_only_transaction_context(branch_id)?;
        Ok(Transaction::new(Self::shared(self), txn))
    }

    /// Internal read-only context constructor used by engine internals.
    pub(crate) fn begin_read_only_transaction_context(
        &self,
        branch_id: BranchId,
    ) -> StrataResult<TransactionContext> {
        let mut txn = self.begin_transaction_context(branch_id)?;
        txn.set_read_only(true);
        Ok(txn)
    }

    /// End a transaction (return to pool)
    ///
    /// Returns the transaction context to the thread-local pool for reuse.
    /// This avoids allocation overhead on subsequent transactions.
    ///
    /// Internal pool-return helper for transaction contexts.
    pub(crate) fn end_transaction_context(&self, ctx: TransactionContext) {
        TransactionPool::release(ctx);
    }

    /// Validate and materialize engine-owned JSON semantics into generic
    /// transaction buffers before lower commit validation runs.
    ///
    /// The explicit snapshot-version probe below is only an early-bailout
    /// optimization. The correctness path is the subsequent call to
    /// `JsonTxnState::materialize` with `txn.get` as the loader: that read-set
    /// participation is what lets the lower OCC validator, which runs under the
    /// branch commit lock, catch concurrent writers that race after this
    /// preparation step begins.
    pub(crate) fn prepare_json_transaction_context(
        &self,
        txn: &mut TransactionContext,
        json_state: &mut JsonTxnState,
    ) -> StrataResult<()> {
        if txn.is_read_only_mode() && json_state.has_writes() {
            return Err(StrataError::invalid_input(
                "Cannot write JSON in a read-only transaction".to_string(),
            ));
        }

        if !json_state.has_writes() {
            json_state.clear();
            return Ok(());
        }

        if let Some(conflict) = json_state.write_conflicts().into_iter().next() {
            let doc_id = conflict.key.user_key_string().ok_or_else(|| {
                StrataError::internal("JSON conflict requires a string document id")
            })?;
            return Err(StrataError::write_conflict(EntityRef::json(
                txn.branch_id,
                conflict.key.namespace.space.clone(),
                doc_id,
            )));
        }

        for (key, snapshot_version) in json_state.snapshot_versions() {
            let current_version = self
                .storage
                .get_version_only(key)?
                .unwrap_or(CommitVersion::ZERO);
            if current_version != *snapshot_version {
                let doc_id = key.user_key_string().ok_or_else(|| {
                    StrataError::internal("JSON OCC requires a string document id")
                })?;
                return Err(StrataError::version_conflict(
                    EntityRef::json(txn.branch_id, key.namespace.space.clone(), doc_id),
                    Version::txn(snapshot_version.as_u64()),
                    Version::txn(current_version.as_u64()),
                ));
            }
        }

        let materialized = json_state.materialize(|key| Ok(txn.get(key)?))?;
        for (key, write) in materialized {
            match write {
                MaterializedJsonWrite::Put(value) => {
                    txn.delete_set.remove(&key);
                    txn.write_set.insert(key, value);
                }
                MaterializedJsonWrite::Delete => {
                    txn.write_set.remove(&key);
                    txn.delete_set.insert(key);
                }
            }
        }
        json_state.clear();
        Ok(())
    }

    /// End a transaction explicitly.
    ///
    /// Dropping the owned transaction already returns it to the pool, so this
    /// method is just an explicit drop.
    pub fn end_transaction(&self, txn: Transaction) {
        drop(txn);
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
    pub(crate) fn commit_transaction_context(
        &self,
        txn: &mut TransactionContext,
    ) -> StrataResult<u64> {
        if let Err(e) = self.ensure_writer_healthy() {
            self.abort_transaction_in_place(txn, format!("Commit failed: {}", e));
            return Err(e);
        }

        if self.follower {
            let err = StrataError::internal(
                "cannot commit: database opened in follower mode (read-only)",
            );
            self.abort_transaction_in_place(txn, format!("Commit failed: {}", err));
            return Err(err);
        }

        let had_writes = !txn.is_read_only();
        if had_writes {
            if let Err(e) = self.validate_branch_generation_guard(txn) {
                self.abort_transaction_in_place(txn, format!("Commit failed: {}", e));
                return Err(e);
            }
        }
        // Admission control: reject writes when L0 is saturated (#1924).
        // Must run BEFORE commit so the caller gets a clean error.
        if had_writes {
            if let Err(e) = self.maybe_apply_write_backpressure() {
                self.abort_transaction_in_place(txn, format!("Write stall: {}", e));
                return Err(e);
            }
        }
        let version = match self.commit_internal(txn, self.current_durability_mode()) {
            Ok(version) => version,
            Err(e) => {
                let should_record_coordinator_abort =
                    txn.is_active() && matches!(&e, StrataError::WriterHalted { .. });
                // `TransactionCoordinator::commit*` already decremented
                // active_count on commit failure. Only mirror the terminal
                // state into the pooled TransactionContext here.
                self.finalize_aborted_transaction_in_place(
                    txn,
                    format!("Commit failed: {}", e),
                    should_record_coordinator_abort,
                );
                return Err(e);
            }
        };
        if had_writes {
            self.schedule_flush_if_needed();
        }
        Ok(version.as_u64())
    }

    fn branch_generation_guard_for(&self, branch_id: BranchId) -> StrataResult<Option<u64>> {
        BranchControlStore::new(Self::shared(self)).active_generation_for_id(branch_id)
    }

    fn seed_branch_generation_read_guard(
        &self,
        txn: &mut TransactionContext,
    ) -> StrataResult<bool> {
        if txn.branch_generation_guard.is_none() {
            return Ok(false);
        }

        if txn.branch_id == BranchId::from_bytes([0u8; 16]) {
            return Ok(false);
        }

        let key = active_ptr_key(txn.branch_id);
        let version = self
            .storage
            .get_versioned(&key, txn.start_version)?
            .map(|entry| CommitVersion(entry.version.as_u64()))
            .unwrap_or(CommitVersion::ZERO);
        txn.read_set.insert(key, version);
        Ok(true)
    }

    fn validate_branch_generation_guard(&self, txn: &TransactionContext) -> StrataResult<()> {
        if !txn.branch_generation_guard_is_seeded() {
            return Ok(());
        }
        let Some(expected_generation) = txn.branch_generation_guard else {
            return Ok(());
        };

        let current_generation =
            BranchControlStore::new(Self::shared(self)).active_generation_for_id(txn.branch_id)?;
        match current_generation {
            Some(current_generation) if current_generation == expected_generation => Ok(()),
            Some(current_generation) => Err(StrataError::TransactionAborted {
                reason: format!(
                    "Branch {} lifecycle advanced from generation {} to {}",
                    txn.branch_id, expected_generation, current_generation
                ),
            }),
            None => Err(StrataError::TransactionAborted {
                reason: format!(
                    "Branch {} lifecycle no longer has active generation {}",
                    txn.branch_id, expected_generation
                ),
            }),
        }
    }

    /// Commit a transaction through the database handle.
    pub fn commit_transaction(&self, txn: &mut Transaction) -> StrataResult<u64> {
        txn.commit()
    }

    /// Internal commit implementation shared by commit_transaction and transaction closures
    ///
    /// Delegates the generic commit protocol to the storage-owned transaction
    /// manager via the `TransactionCoordinator`. When durability is enabled,
    /// the coordinator routes through the concurrency shell only for the
    /// WAL-before-apply step.
    ///
    /// The engine is responsible only for:
    /// - Determining whether to pass the WAL (based on durability mode + persistence)
    ///
    /// The lower runtime handles:
    /// - Per-run commit locking (TOCTOU prevention)
    /// - Validation (first-committer-wins)
    /// - Version allocation
    /// - WAL writing (when WAL reference is provided)
    /// - Storage application
    /// - Fsync (WAL::append handles fsync based on its DurabilityMode)
    /// - Observer notification (best-effort, errors logged not propagated)
    fn commit_internal(
        &self,
        txn: &mut TransactionContext,
        durability: DurabilityMode,
    ) -> StrataResult<CommitVersion> {
        self.ensure_writer_healthy()?;

        // Capture info needed for observer notification before commit
        // (txn state may be modified during commit)
        let txn_id = txn.txn_id;
        let branch_id = txn.branch_id;
        let entry_count = txn.write_set.len() + txn.delete_set.len() + txn.cas_set.len();

        let wal_arc = if durability.requires_wal() {
            self.wal_writer.as_ref()
        } else {
            None
        };

        let commit_version =
            self.coordinator
                .commit_with_wal_arc(txn, self.storage.as_ref(), wal_arc)?;

        // Notify commit observers (best-effort, errors logged not propagated)
        if entry_count > 0 {
            let info = super::CommitInfo {
                txn_id,
                branch_id,
                commit_version,
                entry_count,
                is_merge: false, // Regular commits are not merges
            };
            self.commit_observers().notify(&info);
        }

        Ok(commit_version)
    }

    /// Clone an `Arc<Database>` from an internal `&Database` reference.
    ///
    /// `Database` instances are always owned behind `Arc`; the public open
    /// paths never hand out stack-owned values. This lets the manual
    /// transaction handle retain shared ownership even when callers only have
    /// `&Database` (common in helper functions and integration tests).
    fn shared(this: &Self) -> Arc<Self> {
        let ptr = this as *const Self;
        // SAFETY: every live Database is allocated inside an Arc returned from
        // the open paths. We increment the strong count before reconstructing
        // the Arc so the original allocation remains owned.
        unsafe {
            Arc::increment_strong_count(ptr);
            Arc::from_raw(ptr)
        }
    }
}
