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
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::perf_time;
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
/// Transactions on different branches can commit in parallel, as SegmentedStore
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
    commit_locks: DashMap<BranchId, Arc<Mutex<()>>>,

    /// Quiesce lock for checkpoint safety (#1710).
    ///
    /// Commit paths acquire a **shared** (read) lock, allowing concurrent commits.
    /// `quiesced_version()` acquires an **exclusive** (write) lock, which blocks
    /// until every in-flight commit's `apply_writes` has completed. The returned
    /// version is then safe to use as a checkpoint watermark — no version ≤ that
    /// value has storage application still in progress.
    commit_quiesce: RwLock<()>,
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
            commit_quiesce: RwLock::new(()),
        }
    }

    /// Get current global version
    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Ensure the version counter is at least `floor` (#1726).
    ///
    /// Used after segment recovery to prevent version collisions when the WAL
    /// has been fully compacted and contains no records.  Segments may hold
    /// data at versions higher than what the WAL reports; this method bumps
    /// the counter so that new transactions start above all existing data.
    pub fn bump_version_floor(&self, floor: u64) {
        self.version.fetch_max(floor, Ordering::AcqRel);
    }

    /// Get the current version after draining all in-flight commits (#1710).
    ///
    /// Acquires the exclusive (write) side of `commit_quiesce`, which blocks
    /// until every concurrent commit has finished its `apply_writes`. The
    /// returned version is safe to use as a checkpoint watermark because no
    /// version ≤ this value has storage application still in progress.
    pub fn quiesced_version(&self) -> u64 {
        let _guard = self.commit_quiesce.write();
        self.version.load(Ordering::Acquire)
    }

    /// Allocate next transaction ID
    ///
    /// Uses a single `fetch_add` (LOCK XADD on x86) instead of a CAS loop.
    /// Overflow at `u64::MAX` is detected and repaired — practically unreachable
    /// (584 years at 1 billion txn/sec).
    ///
    /// # Overflow race tradeoff
    ///
    /// At `u64::MAX`, `fetch_add` wraps to 0 before the `store` repair runs.
    /// A concurrent caller could observe the wrapped value and return a
    /// duplicate ID. The old `fetch_update` (CAS loop) was atomic at this
    /// boundary but suffered thundering-herd contention at 16+ threads.
    /// Since reaching `u64::MAX` is physically impossible at any realistic
    /// throughput, we accept this theoretical race in exchange for
    /// contention-free allocation.
    pub fn next_txn_id(&self) -> std::result::Result<u64, CommitError> {
        let prev = self.next_txn_id.fetch_add(1, Ordering::AcqRel);
        if prev == u64::MAX {
            // Undo the wrapping increment: restore counter to u64::MAX
            self.next_txn_id.store(u64::MAX, Ordering::Release);
            return Err(CommitError::CounterOverflow(
                "transaction ID counter at u64::MAX".into(),
            ));
        }
        Ok(prev)
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
    /// Uses a single `fetch_add` (LOCK XADD on x86) instead of a CAS loop.
    /// Overflow at `u64::MAX` is detected and repaired — practically unreachable
    /// (584 years at 1 billion txn/sec). See `next_txn_id` for the overflow
    /// race tradeoff rationale.
    pub fn allocate_version(&self) -> std::result::Result<u64, CommitError> {
        let prev = self.version.fetch_add(1, Ordering::AcqRel);
        if prev == u64::MAX {
            // Undo the wrapping increment: restore counter to u64::MAX
            self.version.store(u64::MAX, Ordering::Release);
            return Err(CommitError::CounterOverflow(
                "version counter at u64::MAX".into(),
            ));
        }
        Ok(prev + 1)
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
    /// 0. Acquire quiesce read lock (checkpoint drains via write lock, #1710)
    /// 1. Acquire per-branch commit lock (prevents TOCTOU race within same branch)
    /// 2. Validate and mark committed (in-memory state transition)
    /// 3. Allocate commit version
    /// 4. Write to WAL if provided (BeginTxn, operations, CommitTxn)
    /// 5. Apply writes to storage
    /// 6. Release commit lock and quiesce lock
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
            return Ok(self.version.load(Ordering::Acquire));
        }

        // Acquire quiesce read lock so checkpoint's quiesced_version() can
        // drain all in-flight commits before reading the version counter (#1710).
        let _quiesce_guard = self.commit_quiesce.read();

        // Step 1: Acquire per-branch commit lock and validate.
        //
        // The lock MUST be held from validation through version allocation
        // and apply_writes — otherwise a concurrent transaction could validate
        // against stale state (TOCTOU).
        let commit_mutex = self
            .commit_locks
            .entry(txn.branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone(); // clone Arc, drop RefMut → releases shard lock (#1781)
        let _commit_guard = commit_mutex.lock();

        // Skip OCC validation for blind writes (no reads, no CAS, no JSON snapshots)
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
        let commit_version = self.allocate_version()?;

        // Step 2.5: Materialize JSON patches into write_set (#1739 OCC-H1)
        txn.materialize_json_writes()
            .map_err(|e| CommitError::WALError(format!("JSON materialization failed: {}", e)))?;

        // Step 3: Write to WAL (durability) - only for transactions with mutations.
        // After materialization, json_writes are in write_set, so is_read_only() covers all cases.
        let has_mutations = !txn.is_read_only();
        if has_mutations {
            if let Some(wal) = wal.as_mut() {
                let payload = TransactionPayload::from_transaction(txn, commit_version);
                // Use commit_version as the WAL record ordering key (#1696).
                // Watermarks are set from the commit_version space, so the WAL
                // record must carry commit_version for correct replay filtering.
                let record = WalRecord::new(
                    commit_version,
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
                // WAL says committed but storage failed — data is durable
                // but NOT visible to reads until restart replays WAL (#1725).
                tracing::error!(
                    target: "strata::txn",
                    txn_id = txn.txn_id,
                    commit_version = commit_version,
                    error = %e,
                    "Storage application failed after WAL commit - will be recovered on restart"
                );
                return Err(CommitError::DurableButNotVisible(format!(
                    "Storage application failed after WAL commit: {}",
                    e
                )));
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

    /// Commit a transaction with narrow WAL lock scope.
    ///
    /// Same commit protocol as `commit()`, but takes `Arc<Mutex<WalWriter>>`
    /// instead of `&mut WalWriter`. This allows pre-serializing the WAL record
    /// (CRC + allocations) **outside** the WAL lock, then briefly locking only
    /// for the append I/O.
    ///
    /// # Lock ordering
    ///
    /// quiesce read lock → branch lock → WAL lock (acquired inside, released before storage apply).
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit (must be in Active state)
    /// * `store` - Storage to validate against and apply writes to
    /// * `wal_arc` - Optional WAL for durability. Pass `None` for ephemeral
    ///   databases or when durability is not required.
    pub fn commit_with_wal_arc<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        wal_arc: Option<&Arc<Mutex<WalWriter>>>,
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
            return Ok(self.version.load(Ordering::Acquire));
        }

        #[cfg(feature = "perf-trace")]
        let commit_start = std::time::Instant::now();
        #[allow(unused_mut, unused_variables)]
        let mut trace = strata_core::instrumentation::PerfTrace::new();

        // Acquire quiesce read lock so checkpoint's quiesced_version() can
        // drain all in-flight commits before reading the version counter (#1710).
        let _quiesce_guard = self.commit_quiesce.read();

        // Step 1: Acquire per-branch commit lock. Must be held from validation
        // through version allocation and apply_writes for ALL writers (including
        // blind writes) to prevent a concurrent writer from advancing the store
        // version while another transaction is mid-apply (#1708).
        let commit_mutex = self
            .commit_locks
            .entry(txn.branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone(); // clone Arc, drop RefMut → releases shard lock (#1781)
        let _commit_guard = commit_mutex.lock();

        // Skip OCC validation for blind writes (no reads, no CAS, no JSON snapshots)
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
            perf_time!(trace, read_set_validate_ns, {
                txn.commit(store)?;
            });
            tracing::debug!(target: "strata::txn", txn_id = txn.txn_id, "Validation passed");
        }

        let commit_version = self.allocate_version()?;

        // Materialize JSON patches into write_set (#1739 OCC-H1)
        txn.materialize_json_writes()
            .map_err(|e| CommitError::WALError(format!("JSON materialization failed: {}", e)))?;

        let has_mutations = !txn.is_read_only();
        if has_mutations {
            if let Some(wal_arc) = wal_arc {
                perf_time!(trace, wal_append_ns, {
                    let payload = TransactionPayload::from_transaction(txn, commit_version);
                    let timestamp = now_micros();
                    // Use commit_version as WAL record ordering key (#1696)
                    let record = WalRecord::new(
                        commit_version,
                        *txn.branch_id.as_bytes(),
                        timestamp,
                        payload.to_bytes(),
                    );
                    let record_bytes = record.to_bytes();
                    {
                        let mut wal = wal_arc.lock();
                        if let Err(e) =
                            wal.append_pre_serialized(&record_bytes, commit_version, timestamp)
                        {
                            txn.status = TransactionStatus::Aborted {
                                reason: format!("WAL write failed: {}", e),
                            };
                            return Err(CommitError::WALError(e.to_string()));
                        }
                    }
                });
                tracing::debug!(target: "strata::txn", txn_id = txn.txn_id, commit_version, "WAL durable");
            }
        }

        perf_time!(trace, write_set_apply_ns, {
            if let Err(e) = txn.apply_writes(store, commit_version) {
                if wal_arc.is_some() {
                    tracing::error!(
                        target: "strata::txn",
                        txn_id = txn.txn_id,
                        commit_version = commit_version,
                        error = %e,
                        "Storage application failed after WAL commit - will be recovered on restart"
                    );
                    return Err(CommitError::DurableButNotVisible(format!(
                        "Storage application failed after WAL commit: {}",
                        e
                    )));
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
        });

        #[cfg(feature = "perf-trace")]
        {
            trace.commit_total_ns = commit_start.elapsed().as_nanos() as u64;
            trace.keys_read = txn.read_set.len();
            trace.keys_written = txn.write_set.len();
            tracing::info!(
                target: "strata::perf",
                txn_id = txn.txn_id,
                commit_version,
                total_us = trace.commit_total_ns / 1000,
                validate_us = trace.read_set_validate_ns / 1000,
                wal_us = trace.wal_append_ns / 1000,
                apply_us = trace.write_set_apply_ns / 1000,
                keys_read = trace.keys_read,
                keys_written = trace.keys_written,
                "commit perf trace"
            );
        }

        Ok(commit_version)
    }

    /// Remove the per-branch commit lock for a deleted branch.
    ///
    /// Called during branch deletion to prevent unbounded growth of the
    /// `commit_locks` map. Always removes the entry and returns `true`.
    ///
    /// If a concurrent commit is in-flight, it holds a cloned `Arc<Mutex>`
    /// and is unaffected by the removal — the Mutex stays alive via the Arc
    /// and the commit completes normally. The next commit on this branch
    /// (if any) will recreate the entry via `entry().or_insert_with()`.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) -> bool {
        // DashMap::remove() atomically acquires the shard write lock and
        // removes the entry.  A concurrent commit may still hold the
        // Arc<Mutex> it cloned earlier — that's fine, the Mutex stays
        // alive via the Arc and the commit completes normally.  The next
        // commit on this branch will recreate the entry via
        // `entry().or_insert_with()`.  (#1621, #1781)
        self.commit_locks.remove(branch_id);
        true
    }

    /// Advance the version counter to at least `v`.
    ///
    /// Used during multi-process refresh to ensure the local version counter
    /// reflects writes applied from other processes' WAL entries.
    /// Uses `fetch_max` so the counter never goes backward.
    pub fn catch_up_version(&self, v: u64) {
        self.version.fetch_max(v, Ordering::AcqRel);
    }

    /// Advance the next_txn_id counter to at least `id + 1`.
    ///
    /// Used during multi-process refresh to ensure locally-allocated transaction
    /// IDs never collide with IDs already used by other processes.
    pub fn catch_up_txn_id(&self, id: u64) {
        self.next_txn_id.fetch_max(id + 1, Ordering::AcqRel);
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

        // Acquire quiesce read lock so checkpoint's quiesced_version() can
        // drain all in-flight commits before reading the version counter (#1710).
        let _quiesce_guard = self.commit_quiesce.read();

        // Acquire per-branch lock — must be held through validation + apply.
        let commit_mutex = self
            .commit_locks
            .entry(txn.branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone(); // clone Arc, drop RefMut → releases shard lock (#1781)
        let _commit_guard = commit_mutex.lock();

        // Skip OCC validation for blind writes (no reads, no CAS, no JSON snapshots)
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
        self.version.fetch_max(commit_version, Ordering::AcqRel);

        // Materialize JSON patches into write_set (#1739 OCC-H1)
        txn.materialize_json_writes()
            .map_err(|e| CommitError::WALError(format!("JSON materialization failed: {}", e)))?;

        // Write to WAL
        let has_mutations = !txn.is_read_only();
        if has_mutations {
            if let Some(wal) = wal.as_mut() {
                let payload = TransactionPayload::from_transaction(txn, commit_version);
                // Use commit_version as WAL record ordering key (#1696)
                let record = WalRecord::new(
                    commit_version,
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
                return Err(CommitError::DurableButNotVisible(format!(
                    "Storage application failed after WAL commit: {}",
                    e
                )));
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

    /// Commit a transaction in bulk load mode — skips WAL for speed.
    ///
    /// Identical to `commit()` with `wal: None`. Data is applied to storage but
    /// is NOT durable until the caller flushes memtables to disk. Suitable for
    /// large initial imports where the source data can be re-read on failure.
    pub fn commit_bulk_load<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
    ) -> std::result::Result<u64, CommitError> {
        self.commit(txn, store, None)
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
    use crate::{JsonStoreExt, TransactionContext};
    use parking_lot::Mutex as ParkingMutex;
    use std::sync::Arc;
    use strata_core::types::{Key, Namespace};
    use strata_core::value::Value;
    use strata_durability::codec::IdentityCodec;
    use strata_durability::wal::{DurabilityMode, WalConfig, WalReader};
    use strata_storage::SegmentedStore;
    use tempfile::TempDir;

    fn create_test_namespace(branch_id: BranchId) -> Arc<Namespace> {
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    fn create_test_key(ns: &Arc<Namespace>, name: &str) -> Key {
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
        assert_eq!(manager.allocate_version().unwrap(), 1);
        assert_eq!(manager.allocate_version().unwrap(), 2);
        assert_eq!(manager.allocate_version().unwrap(), 3);
        assert_eq!(manager.current_version(), 3);
    }

    #[test]
    fn test_next_txn_id_increments() {
        // TransactionManager::new(0) calls with_txn_id(0, 0), which sets next_txn_id = 0 + 1 = 1
        let manager = TransactionManager::new(0);
        assert_eq!(manager.next_txn_id().unwrap(), 1);
        assert_eq!(manager.next_txn_id().unwrap(), 2);
        assert_eq!(manager.next_txn_id().unwrap(), 3);
    }

    #[test]
    fn test_with_txn_id_starts_from_max_plus_one() {
        let manager = TransactionManager::with_txn_id(50, 100);
        assert_eq!(manager.current_version(), 50);
        assert_eq!(manager.next_txn_id().unwrap(), 101); // max_txn_id + 1
    }

    #[test]
    fn test_next_txn_id_overflow_returns_error() {
        // with_txn_id(0, max_txn_id) sets next_txn_id = max_txn_id + 1
        // So with_txn_id(0, u64::MAX - 2) → next_txn_id starts at u64::MAX - 1
        let manager = TransactionManager::with_txn_id(0, u64::MAX - 2);
        // First call: returns u64::MAX - 1, counter advances to u64::MAX
        assert!(manager.next_txn_id().is_ok());
        // Second call fails: counter is at u64::MAX, cannot increment
        assert!(manager.next_txn_id().is_err());
    }

    #[test]
    fn test_allocate_version_overflow_returns_error() {
        // Version starts at u64::MAX - 1
        let manager = TransactionManager::with_txn_id(u64::MAX - 1, 0);
        // First call: version advances from MAX-1 to MAX, returns MAX
        assert!(manager.allocate_version().is_ok());
        // Second call fails: version is at u64::MAX, cannot increment
        assert!(manager.allocate_version().is_err());
    }

    #[test]
    fn test_per_branch_commit_locks_allow_parallel_different_branches() {
        // This test verifies that commits on different branches can proceed in parallel
        // by checking that both commits complete and produce unique versions
        // Note: WalWriter requires &mut so we use a Mutex for shared access from threads
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = Arc::new(TransactionManager::new(0));

        let branch_id1 = BranchId::new();
        let branch_id2 = BranchId::new();
        let ns1 = create_test_namespace(branch_id1);
        let ns2 = create_test_namespace(branch_id2);
        let key1 = create_test_key(&ns1, "key1");
        let key2 = create_test_key(&ns2, "key2");

        // Prepare transactions
        let mut txn1 = TransactionContext::with_store(1, branch_id1, Arc::clone(&store));
        txn1.put(key1.clone(), Value::Int(1)).unwrap();

        let mut txn2 = TransactionContext::with_store(2, branch_id2, Arc::clone(&store));
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
        assert!((1..=2).contains(&v1));
        assert!((1..=2).contains(&v2));
        assert_ne!(v1, v2); // Versions must be unique

        // Both keys should be in storage
        assert!(store.get_versioned(&key1, u64::MAX).unwrap().is_some());
        assert!(store.get_versioned(&key2, u64::MAX).unwrap().is_some());
    }

    #[test]
    fn test_same_branch_commits_serialize() {
        // This test verifies that commits on the same branch are serialized
        // (one completes before the other starts its critical section)
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key1 = create_test_key(&ns, "key1");
        let key2 = create_test_key(&ns, "key2");

        // Commit first transaction
        {
            let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
            txn.put(key1.clone(), Value::Int(100)).unwrap();
            let v = manager
                .commit(&mut txn, store.as_ref(), Some(&mut wal))
                .unwrap();
            assert_eq!(v, 1);
        }

        // Commit second transaction on same branch
        {
            let mut txn = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
            txn.put(key2.clone(), Value::Int(200)).unwrap();
            let v = manager
                .commit(&mut txn, store.as_ref(), Some(&mut wal))
                .unwrap();
            assert_eq!(v, 2);
        }

        // Both values should be present with correct versions
        let v1 = store.get_versioned(&key1, u64::MAX).unwrap().unwrap();
        assert_eq!(v1.value, Value::Int(100));
        assert_eq!(v1.version.as_u64(), 1);

        let v2 = store.get_versioned(&key2, u64::MAX).unwrap().unwrap();
        assert_eq!(v2.value, Value::Int(200));
        assert_eq!(v2.version.as_u64(), 2);
    }

    #[test]
    fn test_many_parallel_commits_different_branches() {
        // Stress test: many parallel commits on different branches
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
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

                let mut txn = TransactionContext::with_store(
                    i as u64 + 1,
                    branch_id,
                    Arc::clone(&store_clone),
                );
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_alice = create_test_key(&ns, "user:alice");
        let key_bob = create_test_key(&ns, "user:bob");
        let prefix = create_test_key(&ns, "user:");

        // Setup: Create initial data with alice and bob
        {
            let mut setup_txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
            setup_txn.put(key_alice.clone(), Value::Int(100)).unwrap();
            setup_txn.put(key_bob.clone(), Value::Int(200)).unwrap();
            manager
                .commit(&mut setup_txn, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: Delete alice (blind), then scan
        let mut txn1 = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
        txn1.delete(key_alice.clone()).unwrap(); // Blind delete
        let scan_result = txn1.scan_prefix(&prefix).unwrap();

        // T1 sees only bob (alice excluded due to delete_set)
        assert_eq!(scan_result.len(), 1);
        assert!(scan_result.iter().any(|(k, _)| k == &key_bob));

        // T2: Update alice and commit first
        {
            let mut txn2 = TransactionContext::with_store(3, branch_id, Arc::clone(&store));
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
        let final_value = store.get_versioned(&key_alice, u64::MAX).unwrap().unwrap();
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_alice = create_test_key(&ns, "user:alice");

        // Setup: Create alice
        {
            let mut setup_txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
            setup_txn.put(key_alice.clone(), Value::Int(100)).unwrap();
            manager
                .commit(&mut setup_txn, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: Blind delete alice (no read, no scan)
        let mut txn1 = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
        txn1.delete(key_alice.clone()).unwrap(); // Blind delete - no read_set entry

        // T2: Update alice and commit first
        {
            let mut txn2 = TransactionContext::with_store(3, branch_id, Arc::clone(&store));
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
        let final_value = store.get_versioned(&key_alice, u64::MAX).unwrap();
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        // Read-only transaction (just reads, no writes)
        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        // No operations — pure read-only
        let result = manager.commit(&mut txn, store.as_ref(), None);
        assert!(result.is_ok());

        // Version should NOT have been incremented
        assert_eq!(manager.current_version(), v_before);
    }

    #[test]
    fn test_read_only_no_lock_contention() {
        // Two concurrent read-only transactions on the same branch shouldn't block
        let store = Arc::new(SegmentedStore::new());
        let manager = Arc::new(TransactionManager::new(0));
        let branch_id = BranchId::new();

        let manager1 = Arc::clone(&manager);
        let store1 = Arc::clone(&store);
        let manager2 = Arc::clone(&manager);
        let store2 = Arc::clone(&store);

        let h1 = std::thread::spawn(move || {
            let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store1));
            manager1.commit(&mut txn, store1.as_ref(), None)
        });
        let h2 = std::thread::spawn(move || {
            let mut txn = TransactionContext::with_store(2, branch_id, Arc::clone(&store2));
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "blind");

        // Blind write: put with no prior read
        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(42)).unwrap();
        assert!(txn.read_set.is_empty()); // Confirms it's a blind write

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());

        let stored = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(42));
    }

    #[test]
    fn test_write_with_read_still_validates() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "readwrite");

        // Setup: store initial value
        {
            let mut setup = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
            setup.put(key.clone(), Value::Int(1)).unwrap();
            manager
                .commit(&mut setup, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: read then write (not a blind write)
        let mut txn = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
        let _ = txn.get(&key).unwrap();
        txn.put(key.clone(), Value::Int(2)).unwrap();
        assert!(!txn.read_set.is_empty()); // Has reads → goes through validation

        // Concurrent update
        {
            let mut txn2 = TransactionContext::with_store(3, branch_id, Arc::clone(&store));
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "cas_key");

        // CAS operation → not a blind write, should validate
        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
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
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "json_doc");

        // Has json_snapshot_versions → should go through full validation path
        // Use version 0 (key doesn't exist) so validation passes
        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(1)).unwrap();
        txn.record_json_snapshot_version(key.clone(), 0);

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());
        // Verify it went through the normal path (version incremented)
        assert!(manager.current_version() > 0);
    }

    // ========================================================================
    // commit_with_wal_arc Tests (narrow WAL lock scope)
    // ========================================================================

    #[test]
    fn test_commit_with_wal_arc_basic_with_recovery() {
        // Commit via narrow-lock path, then recover from WAL to prove
        // append_pre_serialized wrote valid, recoverable records.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "arc_basic");

        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(42)).unwrap();

        let v = manager
            .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
            .unwrap();
        assert_eq!(v, 1);

        // Verify in-memory storage
        let stored = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(42));
        assert_eq!(stored.version.as_u64(), 1);

        // Drop WAL to flush buffers, then recover from disk
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let result = recovery.recover().unwrap();

        // Recovery should find exactly 1 transaction
        assert_eq!(result.stats.txns_replayed, 1);
        assert_eq!(result.stats.final_version, 1);
        assert_eq!(result.stats.writes_applied, 1);

        // Recovered storage should contain the committed value
        let recovered = result
            .storage
            .get_versioned(&key, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.value, Value::Int(42));
        assert_eq!(recovered.version.as_u64(), 1);
    }

    #[test]
    fn test_commit_with_wal_arc_no_wal() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "no_wal");

        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(7)).unwrap();

        let v = manager
            .commit_with_wal_arc(&mut txn, store.as_ref(), None)
            .unwrap();
        assert_eq!(v, 1);

        let stored = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(7));
    }

    #[test]
    fn test_commit_with_wal_arc_many_parallel_branches() {
        // Stress test: 10 threads, each on a different branch, sharing one WAL Arc.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = Arc::new(TransactionManager::new(0));

        let num_threads = 10;
        let mut handles = Vec::new();

        for i in 0..num_threads {
            let m = Arc::clone(&manager);
            let s = Arc::clone(&store);
            let w = Arc::clone(&wal);

            handles.push(std::thread::spawn(move || {
                let branch_id = BranchId::new();
                let ns = create_test_namespace(branch_id);
                let key = create_test_key(&ns, &format!("key_{}", i));

                let mut txn =
                    TransactionContext::with_store(i as u64 + 1, branch_id, Arc::clone(&s));
                txn.put(key, Value::Int(i as i64)).unwrap();

                m.commit_with_wal_arc(&mut txn, s.as_ref(), Some(&w))
            }));
        }

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

        // Recover from WAL to verify all 10 records are well-formed
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let result = recovery.recover().unwrap();
        assert_eq!(result.stats.txns_replayed, num_threads);
        assert_eq!(result.stats.final_version, num_threads as u64);
    }

    #[test]
    fn test_commit_with_wal_arc_validation_conflict_not_in_wal() {
        // Key invariant: validation failure must abort BEFORE WAL write.
        // Verify by recovering and counting WAL records.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "conflict");

        // Setup: store initial value (WAL record #1)
        {
            let mut setup = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
            setup.put(key.clone(), Value::Int(1)).unwrap();
            manager
                .commit_with_wal_arc(&mut setup, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // T1: read-modify-write (will conflict)
        let mut txn1 = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
        let _ = txn1.get(&key).unwrap(); // Creates read_set entry
        txn1.put(key.clone(), Value::Int(10)).unwrap();

        // T2: concurrent blind write, commits first (WAL record #2)
        {
            let mut txn2 = TransactionContext::with_store(3, branch_id, Arc::clone(&store));
            txn2.put(key.clone(), Value::Int(99)).unwrap();
            manager
                .commit_with_wal_arc(&mut txn2, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // T1 should fail validation
        let result = manager.commit_with_wal_arc(&mut txn1, store.as_ref(), Some(&wal));
        assert!(result.is_err(), "Should detect read-write conflict");

        // T2's value should be preserved in storage
        let stored = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(99));

        // KEY ASSERTION: WAL should contain exactly 2 records (setup + T2).
        // T1 was aborted before WAL write, so its record must NOT be in WAL.
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let result = recovery.recover().unwrap();
        assert_eq!(
            result.stats.txns_replayed, 2,
            "Aborted txn must not appear in WAL"
        );

        // Recovered value should be T2's write
        let recovered = result
            .storage
            .get_versioned(&key, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.value, Value::Int(99));
    }

    #[test]
    fn test_commit_with_wal_arc_read_only_fast_path() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        // Read-only transaction: no writes
        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        let result = manager.commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal));
        assert!(result.is_ok());

        // Version should NOT have been incremented (fast-pathed, skips WAL entirely)
        assert_eq!(manager.current_version(), v_before);

        // WAL should be empty — no records written for read-only txn
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let result = recovery.recover().unwrap();
        assert_eq!(result.stats.txns_replayed, 0);
    }

    #[test]
    fn test_commit_with_wal_arc_with_json_writes_takes_normal_path() {
        // A txn with no KV writes but json_writes should still go through
        // the full path (version alloc + WAL write), not the read-only fast path.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        let v_before = manager.current_version();

        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        // No KV writes, but has json_writes → not fast-pathed
        use strata_core::primitives::json::{JsonPatch, JsonPath};
        txn.record_json_write(
            create_test_key(&ns, "doc"),
            JsonPatch::set_at(JsonPath::root(), serde_json::json!({"a": 1}).into()),
            0,
        );

        let result = manager.commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal));
        assert!(result.is_ok());

        // Version SHOULD have been incremented (not fast-pathed)
        assert!(manager.current_version() > v_before);

        // WAL should have a record
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let rec_result = recovery.recover().unwrap();
        assert_eq!(rec_result.stats.txns_replayed, 1);
    }

    #[test]
    fn test_commit_with_wal_arc_multiple_operations_recovery() {
        // Transaction with puts + deletes, verify recovery roundtrip.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_a = create_test_key(&ns, "multi_a");
        let key_b = create_test_key(&ns, "multi_b");
        let key_c = create_test_key(&ns, "multi_c");

        // Setup: write key_a and key_b
        {
            let mut setup = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
            setup.put(key_a.clone(), Value::Int(10)).unwrap();
            setup.put(key_b.clone(), Value::Int(20)).unwrap();
            manager
                .commit_with_wal_arc(&mut setup, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // Transaction: delete key_a, update key_b, insert key_c
        {
            let mut txn = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
            txn.delete(key_a.clone()).unwrap();
            txn.put(key_b.clone(), Value::Int(200)).unwrap();
            txn.put(key_c.clone(), Value::Bytes(b"hello".to_vec()))
                .unwrap();
            manager
                .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // Verify in-memory state
        assert!(store.get_versioned(&key_a, u64::MAX).unwrap().is_none());
        assert_eq!(
            store
                .get_versioned(&key_b, u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(200)
        );
        assert_eq!(
            store
                .get_versioned(&key_c, u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Bytes(b"hello".to_vec())
        );

        // Recover from WAL and verify all operations replayed correctly
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let result = recovery.recover().unwrap();
        assert_eq!(result.stats.txns_replayed, 2);

        // key_a: was written in txn1 then deleted in txn2
        assert!(result
            .storage
            .get_versioned(&key_a, u64::MAX)
            .unwrap()
            .is_none());
        // key_b: updated from 20 → 200
        assert_eq!(
            result
                .storage
                .get_versioned(&key_b, u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(200)
        );
        // key_c: newly inserted
        assert_eq!(
            result
                .storage
                .get_versioned(&key_c, u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Bytes(b"hello".to_vec())
        );
    }

    #[test]
    fn test_commit_with_wal_arc_sequential_same_branch() {
        // Two sequential commits on the same branch verify that the branch lock
        // and WAL lock ordering work correctly without deadlock.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key1 = create_test_key(&ns, "seq1");
        let key2 = create_test_key(&ns, "seq2");

        {
            let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
            txn.put(key1.clone(), Value::Int(100)).unwrap();
            let v = manager
                .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
                .unwrap();
            assert_eq!(v, 1);
        }

        {
            let mut txn = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
            txn.put(key2.clone(), Value::Int(200)).unwrap();
            let v = manager
                .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
                .unwrap();
            assert_eq!(v, 2);
        }

        // Both values present with correct versions
        let v1 = store.get_versioned(&key1, u64::MAX).unwrap().unwrap();
        assert_eq!(v1.value, Value::Int(100));
        assert_eq!(v1.version.as_u64(), 1);

        let v2 = store.get_versioned(&key2, u64::MAX).unwrap().unwrap();
        assert_eq!(v2.value, Value::Int(200));
        assert_eq!(v2.version.as_u64(), 2);
    }

    // ========================================================================
    // E-8: remove_branch_lock coordination tests
    // ========================================================================

    #[test]
    fn test_remove_branch_lock_succeeds_when_not_held() {
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        // Insert a lock entry by accessing it (simulates a prior commit)
        manager
            .commit_locks
            .entry(branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())));
        assert!(manager.commit_locks.contains_key(&branch_id));

        // Removal should succeed since no one holds the lock
        assert!(manager.remove_branch_lock(&branch_id));
        assert!(!manager.commit_locks.contains_key(&branch_id));
    }

    #[test]
    fn test_remove_branch_lock_nonexistent_succeeds() {
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();

        // No lock entry exists — removal should return true
        assert!(manager.remove_branch_lock(&branch_id));
    }

    #[test]
    fn test_remove_branch_lock_serializes_with_commit() {
        // Verify that remove_branch_lock removes the entry and that the
        // next commit recreates it via entry().or_insert_with().
        let manager = Arc::new(TransactionManager::new(0));
        let branch_id = BranchId::new();

        // Insert a lock entry via the same path as commit()
        {
            let _entry = manager
                .commit_locks
                .entry(branch_id)
                .or_insert_with(|| Arc::new(Mutex::new(())));
            // entry guard dropped here, releasing shard lock
        }
        assert!(manager.commit_locks.contains_key(&branch_id));

        // Removal should succeed since no commit is in-flight
        assert!(manager.remove_branch_lock(&branch_id));
        assert!(!manager.commit_locks.contains_key(&branch_id));

        // Next commit should recreate the entry via or_insert_with
        {
            let _entry = manager
                .commit_locks
                .entry(branch_id)
                .or_insert_with(|| Arc::new(Mutex::new(())));
        }
        assert!(manager.commit_locks.contains_key(&branch_id));
    }

    // ========================================================================
    // Bulk load commit tests (Epic 8d)
    // ========================================================================

    #[test]
    fn commit_bulk_load_applies_writes() {
        let manager = TransactionManager::new(0);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "bulk_key");

        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(42)).unwrap();

        let version = manager.commit_bulk_load(&mut txn, store.as_ref()).unwrap();
        assert!(version > 0);

        // Data should be readable
        let result = store.get_versioned(&key, u64::MAX).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Value::Int(42));
    }

    #[test]
    fn commit_bulk_load_no_wal_records() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        let manager = TransactionManager::new(0);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Create WAL for reference
        let mut wal = create_test_wal(&wal_dir);

        // Bulk load commit — should NOT write to WAL
        let key = create_test_key(&ns, "no_wal");
        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(1)).unwrap();
        manager.commit_bulk_load(&mut txn, store.as_ref()).unwrap();

        // Normal commit — writes to WAL
        let key2 = create_test_key(&ns, "with_wal");
        let mut txn2 = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
        txn2.put(key2.clone(), Value::Int(2)).unwrap();
        manager
            .commit(&mut txn2, store.as_ref(), Some(&mut wal))
            .unwrap();

        // WAL should have exactly 1 record (from the normal commit)
        let reader = strata_durability::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        assert_eq!(
            result.records.len(),
            1,
            "Bulk load should not write WAL records"
        );
    }

    #[test]
    fn commit_bulk_load_normal_commits_after() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        let manager = TransactionManager::new(0);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let mut wal = create_test_wal(&wal_dir);

        // Bulk load
        let key1 = create_test_key(&ns, "k1");
        let mut txn1 = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn1.put(key1.clone(), Value::Int(1)).unwrap();
        manager.commit_bulk_load(&mut txn1, store.as_ref()).unwrap();

        // Normal commit after bulk load
        let key2 = create_test_key(&ns, "k2");
        let mut txn2 = TransactionContext::with_store(2, branch_id, Arc::clone(&store));
        txn2.put(key2.clone(), Value::Int(2)).unwrap();
        manager
            .commit(&mut txn2, store.as_ref(), Some(&mut wal))
            .unwrap();

        // Both values readable
        assert_eq!(
            store.get_versioned(&key1, u64::MAX).unwrap().unwrap().value,
            Value::Int(1)
        );
        assert_eq!(
            store.get_versioned(&key2, u64::MAX).unwrap().unwrap().value,
            Value::Int(2)
        );
    }

    /// Issue #1696: WAL records must carry commit_version (not start-time txn_id)
    /// as their ordering key. Without this, a long-running transaction whose
    /// txn_id < checkpoint watermark would be skipped during WAL replay,
    /// silently losing committed data.
    ///
    /// This test verifies that WalRecord.txn_id carries the commit_version:
    /// T1 starts (txn_id=1), T2 starts and commits (advancing version),
    /// then T1 commits. T1's WAL record must carry its commit_version (2),
    /// not its start-time txn_id (1).
    #[test]
    fn test_issue_1696_wal_record_carries_commit_version() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // T1 starts first → gets txn_id=1
        let mut txn1 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn1.put(create_test_key(&ns, "k1"), Value::Int(1)).unwrap();
        assert_eq!(txn1.txn_id, 1, "T1 should get txn_id=1");

        // T2 starts → gets txn_id=2, and commits first → gets commit_version=1
        let mut txn2 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn2.put(create_test_key(&ns, "k2"), Value::Int(2)).unwrap();
        let v2 = manager
            .commit(&mut txn2, store.as_ref(), Some(&mut wal))
            .unwrap();
        assert_eq!(v2, 1, "T2 should get commit_version=1");

        // T1 commits second → gets commit_version=2
        let v1 = manager
            .commit(&mut txn1, store.as_ref(), Some(&mut wal))
            .unwrap();
        assert_eq!(v1, 2, "T1 should get commit_version=2");

        // Read WAL records back and verify ordering keys
        wal.flush().unwrap();
        let reader = WalReader::new();
        let records = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        assert_eq!(records.len(), 2, "Should have 2 WAL records");

        // T2's record (written first): txn_id should be commit_version=1
        assert_eq!(
            records[0].txn_id, 1,
            "T2's WAL record should carry commit_version=1 (not txn_id=2)"
        );

        // T1's record (written second): txn_id should be commit_version=2
        // BUG (pre-fix): would carry txn_id=1 (start-time ID)
        // This would be skipped by any watermark >= 1, losing T1's data
        assert_eq!(
            records[1].txn_id, 2,
            "T1's WAL record must carry commit_version=2 (not start-time txn_id=1)"
        );
    }

    // ========================================================================
    // Issue #1708: Blind-write fast path in commit_with_wal_arc must acquire
    // the per-branch lock to prevent interleaving with concurrent writers.
    // ========================================================================

    #[test]
    fn test_issue_1708_blind_write_acquires_branch_lock() {
        // commit() and commit_with_version() correctly acquire the per-branch
        // lock for all transactions (including blind writes), then skip only
        // validation. commit_with_wal_arc() must do the same.
        //
        // This test verifies that commit_with_wal_arc() creates the commit_locks
        // entry for blind writes — proving the lock path is taken.
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "blind_key");

        // No lock entry should exist initially
        assert!(!manager.commit_locks.contains_key(&branch_id));

        // Commit a blind write via commit_with_wal_arc
        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key, Value::Int(42)).unwrap();
        assert!(txn.read_set.is_empty(), "Must be a blind write");

        manager
            .commit_with_wal_arc(&mut txn, store.as_ref(), None)
            .unwrap();

        // After the commit, the branch lock entry must exist — proving the
        // blind-write path acquired the per-branch lock.
        assert!(
            manager.commit_locks.contains_key(&branch_id),
            "commit_with_wal_arc blind-write path must acquire the per-branch lock"
        );
    }

    #[test]
    fn test_issue_1708_commit_already_acquires_lock_for_blind_writes() {
        // Verify that commit() (the non-arc variant) already acquires the
        // per-branch lock for blind writes. This establishes the expected
        // behavior that commit_with_wal_arc must also follow.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "blind_key");

        assert!(!manager.commit_locks.contains_key(&branch_id));

        let mut txn = TransactionContext::with_store(1, branch_id, Arc::clone(&store));
        txn.put(key, Value::Int(42)).unwrap();
        assert!(txn.read_set.is_empty(), "Must be a blind write");

        manager
            .commit(&mut txn, store.as_ref(), Some(&mut wal))
            .unwrap();

        // commit() correctly acquires the lock for blind writes
        assert!(
            manager.commit_locks.contains_key(&branch_id),
            "commit() acquires per-branch lock even for blind writes"
        );
    }

    /// Storage wrapper that delays apply_writes_atomic and signals when it starts.
    /// Used to create a deterministic window where a commit is in-flight.
    struct DelayedStorage {
        inner: SegmentedStore,
        /// Signaled when apply_writes_atomic starts (version already allocated).
        apply_started: Arc<std::sync::Barrier>,
        /// How long to delay apply_writes_atomic.
        delay: std::time::Duration,
    }

    impl Storage for DelayedStorage {
        fn get_versioned(
            &self,
            key: &Key,
            max_version: u64,
        ) -> strata_core::error::StrataResult<Option<strata_core::contract::VersionedValue>>
        {
            self.inner.get_versioned(key, max_version)
        }

        fn get_history(
            &self,
            key: &Key,
            limit: Option<usize>,
            before_version: Option<u64>,
        ) -> strata_core::error::StrataResult<Vec<strata_core::contract::VersionedValue>> {
            self.inner.get_history(key, limit, before_version)
        }

        fn scan_prefix(
            &self,
            prefix: &Key,
            max_version: u64,
        ) -> strata_core::error::StrataResult<Vec<(Key, strata_core::contract::VersionedValue)>>
        {
            self.inner.scan_prefix(prefix, max_version)
        }

        fn current_version(&self) -> u64 {
            self.inner.current_version()
        }

        fn put_with_version_mode(
            &self,
            key: Key,
            value: Value,
            version: u64,
            ttl: Option<std::time::Duration>,
            mode: strata_core::traits::WriteMode,
        ) -> strata_core::error::StrataResult<()> {
            self.inner
                .put_with_version_mode(key, value, version, ttl, mode)
        }

        fn delete_with_version(
            &self,
            key: &Key,
            version: u64,
        ) -> strata_core::error::StrataResult<()> {
            self.inner.delete_with_version(key, version)
        }

        fn apply_writes_atomic(
            &self,
            writes: Vec<(Key, Value, strata_core::traits::WriteMode)>,
            deletes: Vec<Key>,
            version: u64,
            put_ttls: &[u64],
        ) -> strata_core::error::StrataResult<()> {
            // Signal that apply has started (version already allocated)
            self.apply_started.wait();
            // Delay to widen the in-flight window
            std::thread::sleep(self.delay);
            self.inner
                .apply_writes_atomic(writes, deletes, version, put_ttls)
        }
    }

    /// Issue #1710: quiesced_version() must drain in-flight commits before
    /// returning the version, ensuring the returned value is safe for use
    /// as a checkpoint watermark.
    ///
    /// This test creates a commit with a delayed storage backend. While the
    /// commit is in-flight (version allocated but apply_writes not complete):
    /// - current_version() returns the in-flight version (unsafe for watermark)
    /// - quiesced_version() blocks until the commit finishes, then returns the
    ///   version (safe for watermark)
    #[test]
    fn test_issue_1710_quiesced_version_drains_inflight_commits() {
        let apply_started = Arc::new(std::sync::Barrier::new(2));
        let store = Arc::new(DelayedStorage {
            inner: SegmentedStore::new(),
            apply_started: Arc::clone(&apply_started),
            delay: std::time::Duration::from_millis(200),
        });
        let manager = Arc::new(TransactionManager::new(0));

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "test_key");

        // Prepare a blind-write transaction (skips validation, no store reads)
        let mut txn = TransactionContext::new(1, branch_id, 0);
        txn.put(key, Value::Int(42)).unwrap();

        let manager_clone = Arc::clone(&manager);
        let store_clone = Arc::clone(&store);

        // Spawn commit thread — will block in apply_writes_atomic
        let commit_handle = std::thread::spawn(move || {
            manager_clone
                .commit(&mut txn, store_clone.as_ref(), None)
                .unwrap()
        });

        // Wait until apply_writes_atomic has started (version is allocated)
        apply_started.wait();

        // current_version() returns the in-flight version — unsafe for watermark
        let current = manager.current_version();
        assert_eq!(
            current, 1,
            "current_version() reflects allocated-but-unapplied version"
        );

        // quiesced_version() must block until the commit finishes
        let start = std::time::Instant::now();
        let quiesced = manager.quiesced_version();
        let elapsed = start.elapsed();

        // The commit should have completed (200ms delay)
        assert_eq!(
            quiesced, 1,
            "quiesced_version returns version after commit completes"
        );
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "quiesced_version() must have blocked waiting for in-flight commit \
             (elapsed: {:?}, expected >= 50ms)",
            elapsed
        );

        let committed_version = commit_handle.join().unwrap();
        assert_eq!(committed_version, 1);
    }

    /// Issue #1710 stress variant: concurrent commits on multiple branches
    /// with interleaved quiesced_version() calls.
    #[test]
    #[ignore] // Deadlock: DashMap shard lock held through barrier — see #1781
    fn test_issue_1710_quiesced_version_concurrent_multi_branch() {
        let apply_started = Arc::new(std::sync::Barrier::new(5)); // 4 writers + 1 reader
        let store = Arc::new(DelayedStorage {
            inner: SegmentedStore::new(),
            apply_started: Arc::clone(&apply_started),
            delay: std::time::Duration::from_millis(100),
        });
        let manager = Arc::new(TransactionManager::new(0));

        let num_writers = 4;
        let mut handles = Vec::new();

        for i in 0..num_writers {
            let manager_clone = Arc::clone(&manager);
            let store_clone = Arc::clone(&store);
            let branch_id = BranchId::new();
            let ns = create_test_namespace(branch_id);
            let key = create_test_key(&ns, &format!("key_{}", i));

            handles.push(std::thread::spawn(move || {
                let mut txn = TransactionContext::new(i as u64 + 1, branch_id, 0);
                txn.put(key, Value::Int(i as i64)).unwrap();
                manager_clone
                    .commit(&mut txn, store_clone.as_ref(), None)
                    .unwrap()
            }));
        }

        // Wait until all commits have entered apply_writes_atomic
        apply_started.wait();

        // quiesced_version() must wait for all 4 commits to finish
        let quiesced = manager.quiesced_version();

        // All 4 versions should be allocated and applied
        assert_eq!(
            quiesced, 4,
            "quiesced_version must reflect all completed commits"
        );

        for h in handles {
            h.join().unwrap();
        }
    }

    /// Storage wrapper that always fails on apply_writes_atomic.
    /// Used to simulate storage failure after WAL commit.
    struct FailingApplyStorage {
        inner: SegmentedStore,
    }

    impl Storage for FailingApplyStorage {
        fn get_versioned(
            &self,
            key: &Key,
            max_version: u64,
        ) -> strata_core::error::StrataResult<Option<strata_core::contract::VersionedValue>>
        {
            self.inner.get_versioned(key, max_version)
        }

        fn get_history(
            &self,
            key: &Key,
            limit: Option<usize>,
            before_version: Option<u64>,
        ) -> strata_core::error::StrataResult<Vec<strata_core::contract::VersionedValue>> {
            self.inner.get_history(key, limit, before_version)
        }

        fn scan_prefix(
            &self,
            prefix: &Key,
            max_version: u64,
        ) -> strata_core::error::StrataResult<Vec<(Key, strata_core::contract::VersionedValue)>>
        {
            self.inner.scan_prefix(prefix, max_version)
        }

        fn current_version(&self) -> u64 {
            self.inner.current_version()
        }

        fn put_with_version_mode(
            &self,
            key: Key,
            value: Value,
            version: u64,
            ttl: Option<std::time::Duration>,
            mode: strata_core::traits::WriteMode,
        ) -> strata_core::error::StrataResult<()> {
            self.inner
                .put_with_version_mode(key, value, version, ttl, mode)
        }

        fn delete_with_version(
            &self,
            key: &Key,
            version: u64,
        ) -> strata_core::error::StrataResult<()> {
            self.inner.delete_with_version(key, version)
        }

        fn apply_writes_atomic(
            &self,
            _writes: Vec<(Key, Value, strata_core::traits::WriteMode)>,
            _deletes: Vec<Key>,
            _version: u64,
            _put_ttls: &[u64],
        ) -> strata_core::error::StrataResult<()> {
            Err(strata_core::error::StrataError::storage(
                "simulated storage failure",
            ))
        }
    }

    /// Issue #1725: apply_writes failure after WAL commit must NOT return Ok.
    ///
    /// When apply_writes fails after the WAL record is written, the data is
    /// durable (will be recovered on restart) but NOT visible to reads in
    /// the current process. Returning Ok misleads the caller into thinking
    /// the data is immediately readable.
    #[test]
    fn test_issue_1725_apply_writes_failure_after_wal_returns_error() {
        let dir = TempDir::new().unwrap();
        let mut wal = create_test_wal(dir.path());
        let real_store = Arc::new(SegmentedStore::new());
        let failing_store = FailingApplyStorage {
            inner: SegmentedStore::new(),
        };
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "invisible_key");

        // Create a transaction with a write (blind write — skips validation)
        let mut txn = TransactionContext::with_store(1, branch_id, real_store);
        txn.put(key.clone(), Value::Int(42)).unwrap();

        // Commit should return an error because apply_writes fails,
        // even though WAL write succeeds.
        let result = manager.commit(&mut txn, &failing_store, Some(&mut wal));
        assert!(
            result.is_err(),
            "commit() must return Err when apply_writes fails after WAL write, got Ok({:?})",
            result.unwrap()
        );

        // Verify it's the right error variant
        let err = result.unwrap_err();
        assert!(
            matches!(err, CommitError::DurableButNotVisible(_)),
            "Expected DurableButNotVisible error, got: {:?}",
            err
        );
    }

    /// Issue #1725: Same test for the commit_with_wal_arc path.
    #[test]
    fn test_issue_1725_apply_writes_failure_after_wal_arc_returns_error() {
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(ParkingMutex::new(create_test_wal(dir.path())));
        let real_store = Arc::new(SegmentedStore::new());
        let failing_store = FailingApplyStorage {
            inner: SegmentedStore::new(),
        };
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "invisible_key");

        let mut txn = TransactionContext::with_store(2, branch_id, real_store);
        txn.put(key.clone(), Value::Int(43)).unwrap();

        let result = manager.commit_with_wal_arc(&mut txn, &failing_store, Some(&wal));
        assert!(
            result.is_err(),
            "commit_with_wal_arc() must return Err when apply_writes fails after WAL write"
        );

        let err = result.unwrap_err();
        assert!(
            matches!(err, CommitError::DurableButNotVisible(_)),
            "Expected DurableButNotVisible error, got: {:?}",
            err
        );
    }

    /// Issue #1725: Same test for the commit_with_version path.
    #[test]
    fn test_issue_1725_apply_writes_failure_after_wal_version_returns_error() {
        let dir = TempDir::new().unwrap();
        let mut wal = create_test_wal(dir.path());
        let real_store = Arc::new(SegmentedStore::new());
        let failing_store = FailingApplyStorage {
            inner: SegmentedStore::new(),
        };
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "invisible_key");

        let mut txn = TransactionContext::with_store(3, branch_id, real_store);
        txn.put(key.clone(), Value::Int(44)).unwrap();

        let result = manager.commit_with_version(&mut txn, &failing_store, Some(&mut wal), 42);
        assert!(
            result.is_err(),
            "commit_with_version() must return Err when apply_writes fails after WAL write"
        );

        let err = result.unwrap_err();
        assert!(
            matches!(err, CommitError::DurableButNotVisible(_)),
            "Expected DurableButNotVisible error, got: {:?}",
            err
        );
    }

    // ========================================================================
    // Issue #1739: OCC-H1 — JSON mutation path validates but never persists.
    // json_writes are ignored by apply_writes() and TransactionPayload.
    // ========================================================================

    #[test]
    fn test_issue_1739_json_writes_persisted_to_storage() {
        use strata_core::primitives::json::JsonPath;

        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let json_key = Key::new_json(ns.clone(), "doc1");

        // Step 1: Store a base JSON document via a regular KV put.
        let base_doc = serde_json::json!({"name": "alice", "age": 30});
        let base_bytes = rmp_serde::to_vec(&base_doc).unwrap();
        let mut txn0 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn0.put(json_key.clone(), Value::Bytes(base_bytes))
            .unwrap();
        manager.commit(&mut txn0, store.as_ref(), None).unwrap();

        // Step 2: Start a JSON-only transaction that modifies the document.
        let mut txn1 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn1.json_set(
            &json_key,
            &"age".parse::<JsonPath>().unwrap(),
            serde_json::json!(31).into(),
        )
        .unwrap();

        // Commit the JSON-only transaction.
        let commit_v = manager.commit(&mut txn1, store.as_ref(), None).unwrap();

        // Step 3: Read back from storage and verify the document was updated.
        let result = store.get_versioned(&json_key, commit_v).unwrap();
        assert!(
            result.is_some(),
            "JSON document must exist in storage after json_set commit"
        );
        let vv = result.unwrap();
        let stored_bytes = match &vv.value {
            Value::Bytes(b) => b,
            other => panic!("Expected Value::Bytes, got {:?}", other),
        };
        let stored_doc: serde_json::Value = rmp_serde::from_slice(stored_bytes).unwrap();
        assert_eq!(
            stored_doc,
            serde_json::json!({"name": "alice", "age": 31}),
            "JSON document must reflect the json_set patch"
        );
    }

    #[test]
    fn test_issue_1739_json_writes_included_in_wal_payload() {
        use strata_core::primitives::json::JsonPath;

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let json_key = Key::new_json(ns.clone(), "doc1");

        // Store a base JSON document.
        let base_doc = serde_json::json!({"x": 1});
        let base_bytes = rmp_serde::to_vec(&base_doc).unwrap();
        let mut txn0 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn0.put(json_key.clone(), Value::Bytes(base_bytes))
            .unwrap();
        manager
            .commit(&mut txn0, store.as_ref(), Some(&mut wal))
            .unwrap();

        // JSON-only transaction.
        let mut txn1 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn1.json_set(
            &json_key,
            &"x".parse::<JsonPath>().unwrap(),
            serde_json::json!(99).into(),
        )
        .unwrap();
        let commit_v = manager
            .commit(&mut txn1, store.as_ref(), Some(&mut wal))
            .unwrap();

        // Read WAL and verify the payload contains the JSON put.
        wal.flush().unwrap();
        let reader = WalReader::new();
        let records = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        // Record[0] = base doc put, Record[1] = json_set commit
        assert!(records.len() >= 2, "Expected at least 2 WAL records");
        let payload = TransactionPayload::from_bytes(&records[1].writeset).unwrap();
        assert_eq!(payload.version, commit_v);
        assert!(
            !payload.puts.is_empty(),
            "WAL payload must contain the materialized JSON put"
        );
        assert_eq!(
            payload.puts[0].0, json_key,
            "WAL payload put key must match the JSON document key"
        );
        // Verify the WAL value is the correctly patched document, not just any bytes
        let wal_bytes = match &payload.puts[0].1 {
            Value::Bytes(b) => b,
            other => panic!("Expected Value::Bytes in WAL payload, got {:?}", other),
        };
        let wal_doc: serde_json::Value = rmp_serde::from_slice(wal_bytes).unwrap();
        assert_eq!(
            wal_doc,
            serde_json::json!({"x": 99}),
            "WAL payload must contain the correctly patched document"
        );
    }

    // ========================================================================
    // Issue #1739: OCC-M1 — Same-key put + CAS must be rejected.
    // ========================================================================

    #[test]
    fn test_issue_1739_cas_rejects_key_already_in_write_set() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "k1");

        let mut txn = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        // First: put(K, v1)
        txn.put(key.clone(), Value::Int(1)).unwrap();
        // Then: cas(K, ...) on the same key — must be rejected
        let result = txn.cas(key.clone(), 0, Value::Int(2));
        assert!(
            result.is_err(),
            "cas() must reject a key already in write_set"
        );
    }

    #[test]
    fn test_issue_1739_put_rejects_key_already_in_cas_set() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "k1");

        let mut txn = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        // First: cas(K, 0, v1)
        txn.cas(key.clone(), 0, Value::Int(1)).unwrap();
        // Then: put(K, v2) on the same key — must be rejected
        let result = txn.put(key.clone(), Value::Int(2));
        assert!(
            result.is_err(),
            "put() must reject a key already in cas_set"
        );
    }

    #[test]
    fn test_issue_1739_delete_rejects_key_already_in_cas_set() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "k1");

        let mut txn = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        // First: cas(K, 0, v1)
        txn.cas(key.clone(), 0, Value::Int(1)).unwrap();
        // Then: delete(K) on the same key — must be rejected
        let result = txn.delete(key.clone());
        assert!(
            result.is_err(),
            "delete() must reject a key already in cas_set"
        );
    }

    #[test]
    fn test_issue_1739_cas_rejects_key_already_in_delete_set() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "k1");

        let mut txn = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        // First: delete(K)
        txn.delete(key.clone()).unwrap();
        // Then: cas(K, ...) on the same key — must be rejected
        let result = txn.cas(key.clone(), 0, Value::Int(2));
        assert!(
            result.is_err(),
            "cas() must reject a key already in delete_set"
        );
    }

    #[test]
    fn test_issue_1739_cas_with_read_rejects_key_already_in_write_set() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(0);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "k1");

        let mut txn = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn.put(key.clone(), Value::Int(1)).unwrap();
        let result = txn.cas_with_read(key.clone(), 0, Value::Int(2));
        assert!(
            result.is_err(),
            "cas_with_read() must reject a key already in write_set"
        );
    }
}
