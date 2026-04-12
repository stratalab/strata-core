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

use crate::payload;
use crate::{CommitError, TransactionContext, TransactionStatus};
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::perf_time;
use strata_core::traits::Storage;
use strata_core::types::BranchId;
use strata_durability::now_micros;
use strata_durability::wal::WalWriter;

// Thread-local reusable buffers for WAL record serialization.
// After warmup, these grow to accommodate the largest record and are
// reused with zero heap allocations per commit.
thread_local! {
    static WAL_RECORD_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
    static MSGPACK_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

// ── Commit-path profiling ───────────────────────────────────────────────────
// Enabled by setting STRATA_PROFILE_COMMIT=1 environment variable.
// Prints a breakdown every PROFILE_INTERVAL commits.

use std::sync::atomic::AtomicBool;
use std::time::Instant;

static COMMIT_PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);
static COMMIT_PROFILE_CHECKED: AtomicBool = AtomicBool::new(false);

fn commit_profile_enabled() -> bool {
    if !COMMIT_PROFILE_CHECKED.load(Ordering::Relaxed) {
        let enabled = std::env::var("STRATA_PROFILE_COMMIT").is_ok();
        COMMIT_PROFILE_ENABLED.store(enabled, Ordering::Relaxed);
        COMMIT_PROFILE_CHECKED.store(true, Ordering::Relaxed);
    }
    COMMIT_PROFILE_ENABLED.load(Ordering::Relaxed)
}

const PROFILE_INTERVAL: u64 = 10_000;

struct CommitProfile {
    count: u64,
    lock_acquire_ns: u64,
    validation_ns: u64,
    version_alloc_ns: u64,
    wal_serialize_ns: u64,
    wal_mutex_ns: u64,
    wal_io_ns: u64,
    apply_ns: u64,
    mark_version_ns: u64,
    total_ns: u64,
}

thread_local! {
    static COMMIT_PROF: RefCell<CommitProfile> = const { RefCell::new(CommitProfile {
        count: 0, lock_acquire_ns: 0, validation_ns: 0, version_alloc_ns: 0,
        wal_serialize_ns: 0, wal_mutex_ns: 0, wal_io_ns: 0,
        apply_ns: 0, mark_version_ns: 0, total_ns: 0,
    }) };
}

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

    /// Highest version V where all data at versions ≤ V is fully applied (#1913).
    ///
    /// Unlike `version` (which reflects allocated versions), this only advances
    /// when the contiguous prefix of applied versions grows. Transactions use
    /// this for their snapshot to avoid seeing a state that never existed
    /// (e.g., branch B committed but branch A's earlier version not yet applied).
    visible_version: AtomicU64,

    /// Set of allocated but not-yet-applied versions (#1913).
    ///
    /// When `allocate_version()` returns V, V is inserted here. When
    /// `mark_version_applied(V)` is called, V is removed and `visible_version`
    /// is advanced if the contiguous applied prefix grew.
    pending_versions: Mutex<BTreeSet<u64>>,

    /// Branches currently being deleted (#1916).
    ///
    /// Commit paths check this set after acquiring the per-branch lock and
    /// reject commits on branches that are mid-deletion. This prevents
    /// concurrent commits from resurrecting data on a deleted branch.
    deleting_branches: DashSet<BranchId>,
}

/// Describes how the WAL record should be written during commit.
///
/// Used by [`TransactionManager::commit_inner`] to abstract over the three
/// WAL passing styles: direct reference, shared Arc, or no WAL.
enum WalMode<'a> {
    /// No WAL — ephemeral/cache mode.
    None,
    /// Direct `&mut WalWriter` reference.
    Direct(&'a mut WalWriter),
    /// Shared `Arc<Mutex<WalWriter>>` — pre-serializes outside the lock.
    Shared(&'a Arc<Mutex<WalWriter>>),
}

impl WalMode<'_> {
    fn has_wal(&self) -> bool {
        !matches!(self, WalMode::None)
    }
}

impl TransactionManager {
    /// Create a new transaction manager
    ///
    /// # Arguments
    /// * `initial_version` - Starting version (typically from recovery's final_version)
    pub fn new(initial_version: CommitVersion) -> Self {
        Self::with_txn_id(initial_version, TxnId::ZERO)
    }

    /// Create a new transaction manager with specific starting txn_id
    ///
    /// This is used during recovery to ensure new transactions get unique IDs
    /// that don't conflict with transactions already in the WAL.
    ///
    /// # Arguments
    /// * `initial_version` - Starting version (from recovery's final_version)
    /// * `max_txn_id` - Maximum txn_id seen in WAL (new transactions start at max_txn_id + 1)
    pub fn with_txn_id(initial_version: CommitVersion, max_txn_id: TxnId) -> Self {
        TransactionManager {
            version: AtomicU64::new(initial_version.as_u64()),
            // Start next_txn_id at max_txn_id + 1 to avoid conflicts
            next_txn_id: AtomicU64::new(max_txn_id.as_u64() + 1),
            commit_locks: DashMap::new(),
            commit_quiesce: RwLock::new(()),
            visible_version: AtomicU64::new(initial_version.as_u64()),
            pending_versions: Mutex::new(BTreeSet::new()),
            deleting_branches: DashSet::new(),
        }
    }

    /// Get current global version
    pub fn current_version(&self) -> CommitVersion {
        CommitVersion(self.version.load(Ordering::Acquire))
    }

    /// Get the highest version where all data at versions ≤ V is fully applied.
    ///
    /// Unlike `current_version()` (which reflects allocated versions), this
    /// returns a version safe for snapshot reads: no in-flight `apply_writes`
    /// exists at any version ≤ the returned value.
    ///
    /// This prevents the cross-branch snapshot gap described in #1913:
    /// if branch A allocates version N and branch B allocates N+1, B can
    /// finish apply before A. A reader using `current_version()` (= N+1)
    /// would miss A's writes. `visible_version()` only advances to N+1
    /// once both A and B have completed their applies.
    pub fn visible_version(&self) -> CommitVersion {
        CommitVersion(self.visible_version.load(Ordering::Acquire))
    }

    /// Mark a version as fully applied to storage.
    ///
    /// Called after `apply_writes_atomic` completes (or after a version is
    /// allocated but the commit fails, creating a version gap). Removes the
    /// version from the in-flight set and advances `visible_version` if the
    /// contiguous applied prefix has grown.
    pub fn mark_version_applied(&self, version: CommitVersion) {
        let mut pending = self.pending_versions.lock();
        pending.remove(&version.as_u64());
        let new_visible = match pending.iter().next() {
            // Some versions still in-flight: safe up to (lowest_pending - 1)
            Some(&min_pending) => min_pending.saturating_sub(1),
            // All allocated versions applied: safe up to last allocated
            // (counter value = last allocated, since allocate_version
            // returns prev+1 and sets counter to prev+1).
            None => self.version.load(Ordering::Acquire),
        };
        self.visible_version
            .fetch_max(new_visible, Ordering::AcqRel);
    }

    /// Ensure the version counter is at least `floor` (#1726).
    ///
    /// Used after segment recovery to prevent version collisions when the WAL
    /// has been fully compacted and contains no records.  Segments may hold
    /// data at versions higher than what the WAL reports; this method bumps
    /// the counter so that new transactions start above all existing data.
    pub fn bump_version_floor(&self, floor: CommitVersion) {
        self.version.fetch_max(floor.as_u64(), Ordering::AcqRel);
        // Recovery data at versions ≤ floor is already in storage,
        // so visible_version must reflect this (#1913).
        self.visible_version.fetch_max(floor.as_u64(), Ordering::AcqRel);
    }

    /// Get the current version after draining all in-flight commits (#1710).
    ///
    /// Acquires the exclusive (write) side of `commit_quiesce`, which blocks
    /// until every concurrent commit has finished its `apply_writes`. The
    /// returned version is safe to use as a checkpoint watermark because no
    /// version ≤ this value has storage application still in progress.
    pub fn quiesced_version(&self) -> u64 {
        // Lock Level 1 (exclusive): drains all in-flight commits.
        let _guard = self.commit_quiesce.write();
        self.version.load(Ordering::Acquire)
    }

    /// Acquire the commit quiesce lock, blocking until all in-flight commits
    /// have finished their `apply_writes`.
    ///
    /// The returned guard prevents new commits from starting (they need the
    /// shared read side). Hold it across operations that require a stable
    /// storage version, such as `fork_branch` (#2105).
    pub fn quiesce_commits(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.commit_quiesce.write()
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
    pub fn next_txn_id(&self) -> std::result::Result<TxnId, CommitError> {
        let prev = self.next_txn_id.fetch_add(1, Ordering::AcqRel);
        if prev == u64::MAX {
            // Undo the wrapping increment: restore counter to u64::MAX
            self.next_txn_id.store(u64::MAX, Ordering::Release);
            return Err(CommitError::CounterOverflow(
                "transaction ID counter at u64::MAX".into(),
            ));
        }
        Ok(TxnId(prev))
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
    pub fn allocate_version(&self) -> std::result::Result<CommitVersion, CommitError> {
        let mut pending = self.pending_versions.lock();
        let prev = self.version.fetch_add(1, Ordering::AcqRel);
        if prev == u64::MAX {
            // Undo the wrapping increment: restore counter to u64::MAX
            self.version.store(u64::MAX, Ordering::Release);
            return Err(CommitError::CounterOverflow(
                "version counter at u64::MAX".into(),
            ));
        }
        let version = prev + 1;
        pending.insert(version);
        Ok(CommitVersion(version))
    }

    /// Core commit implementation shared by all public commit methods.
    ///
    /// Implements the full commit protocol: read-only fast path, quiesce lock,
    /// per-branch lock, OCC validation, version allocation, JSON materialization,
    /// WAL write, storage application, and visible-version advancement.
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit (must be in Active state)
    /// * `store` - Storage to validate against and apply writes to
    /// * `wal_mode` - How to write the WAL record (direct, shared, or none)
    /// * `external_version` - If `Some(v)`, use `v` instead of allocating from
    ///   the local counter (used by multi-process coordinated commits)
    fn commit_inner<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        wal_mode: WalMode<'_>,
        external_version: Option<CommitVersion>,
    ) -> std::result::Result<CommitVersion, CommitError> {
        // Fast path: read-only transactions skip lock, validation, version alloc, WAL, apply
        if txn.is_read_only() && txn.json_writes().is_empty() {
            if !txn.is_active() {
                return Err(CommitError::InvalidState(format!(
                    "Cannot commit transaction {} from {:?} state - must be Active",
                    txn.txn_id, txn.status
                )));
            }
            txn.status = TransactionStatus::Committed;
            return Ok(external_version.unwrap_or_else(|| CommitVersion(self.version.load(Ordering::Acquire))));
        }

        let profiling = commit_profile_enabled();
        let t_total = Instant::now();

        #[cfg(feature = "perf-trace")]
        let commit_start = std::time::Instant::now();
        #[allow(unused_mut, unused_variables)]
        let mut trace = strata_core::instrumentation::PerfTrace::new();

        // Lock Level 1 (shared): Acquire quiesce read lock so checkpoint's
        // quiesced_version() can drain all in-flight commits before reading
        // the version counter (#1710).
        let t0 = Instant::now();
        let _quiesce_guard = self.commit_quiesce.read();

        // Lock Level 5 → Level 2: Acquire per-branch commit lock.
        // DashMap shard guard (Level 5) is dropped by the .clone() before
        // we lock the Mutex (Level 2). Must be held from validation through
        // version allocation and apply_writes (prevents TOCTOU, #1708, #1781).
        let commit_mutex = self
            .commit_locks
            .entry(txn.branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone(); // clone Arc, drop RefMut → releases shard lock (#1781)
        let _commit_guard = commit_mutex.lock(); // Lock Level 2
        let lock_acquire_ns = t0.elapsed().as_nanos() as u64;

        // Reject commits on branches that are mid-deletion (#1916).
        if self.deleting_branches.contains(&txn.branch_id) {
            return Err(CommitError::BranchDeleting(txn.branch_id));
        }

        // Skip OCC validation for blind writes (no reads, no CAS, no JSON snapshots)
        let t0 = Instant::now();
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
            tracing::debug!(target: "strata::txn", txn_id = txn.txn_id.as_u64(), "Validation passed");
        }
        let validation_ns = t0.elapsed().as_nanos() as u64;

        // Version allocation: either from local counter or externally provided
        let t0 = Instant::now();
        let commit_version = match external_version {
            None => self.allocate_version()?,
            Some(v) => {
                // Register external version as pending BEFORE advancing the counter.
                // If we advance first, a concurrent mark_version_applied could see
                // the higher counter with an empty pending set and advance
                // visible_version to include this not-yet-applied version (#1913).
                let mut pending = self.pending_versions.lock();
                self.version.fetch_max(v.as_u64(), Ordering::AcqRel);
                pending.insert(v.as_u64());
                v
            }
        };
        let version_alloc_ns = t0.elapsed().as_nanos() as u64;

        // Materialize JSON patches into write_set (#1739 OCC-H1)
        if let Err(e) = txn.materialize_json_writes() {
            self.mark_version_applied(commit_version);
            return Err(CommitError::WALError(format!(
                "JSON materialization failed: {}",
                e
            )));
        }

        // WAL write (durability)
        //
        // Uses fused zero-clone serialization: iterates write_set/delete_set
        // by reference (no Key/Value clones), serializes msgpack + WAL envelope
        // into thread-local reusable buffers (zero alloc after warmup).
        let has_wal = wal_mode.has_wal();
        let has_mutations = !txn.is_read_only();
        let mut wal_serialize_ns = 0u64;
        let mut wal_mutex_ns = 0u64;
        let mut wal_io_ns = 0u64;
        if has_mutations {
            perf_time!(trace, wal_append_ns, {
                match wal_mode {
                    WalMode::Direct(wal) => {
                        let timestamp = now_micros();
                        let result = WAL_RECORD_BUF.with(|rec_cell| {
                            MSGPACK_BUF.with(|msg_cell| {
                                let mut rec_buf = rec_cell.borrow_mut();
                                let mut msg_buf = msg_cell.borrow_mut();
                                let ts = Instant::now();
                                payload::serialize_wal_record_into(
                                    &mut rec_buf,
                                    &mut msg_buf,
                                    txn,
                                    commit_version.as_u64(),
                                    commit_version.as_u64(),
                                    *txn.branch_id.as_bytes(),
                                    timestamp,
                                );
                                wal_serialize_ns = ts.elapsed().as_nanos() as u64;
                                // Direct mode: no separate mutex
                                let tw = Instant::now();
                                let r =
                                    wal.append_pre_serialized(&rec_buf, commit_version.as_u64(), timestamp);
                                wal_io_ns = tw.elapsed().as_nanos() as u64;
                                r
                            })
                        });
                        if let Err(e) = result {
                            txn.status = TransactionStatus::Aborted {
                                reason: format!("WAL write failed: {}", e),
                            };
                            self.mark_version_applied(commit_version);
                            return Err(CommitError::WALError(e.to_string()));
                        }
                        tracing::debug!(target: "strata::txn", txn_id = txn.txn_id.as_u64(), commit_version = commit_version.as_u64(), "WAL durable");
                    }
                    WalMode::Shared(wal_arc) => {
                        let timestamp = now_micros();
                        let result = WAL_RECORD_BUF.with(|rec_cell| {
                            MSGPACK_BUF.with(|msg_cell| {
                                let mut rec_buf = rec_cell.borrow_mut();
                                let mut msg_buf = msg_cell.borrow_mut();
                                let ts = Instant::now();
                                payload::serialize_wal_record_into(
                                    &mut rec_buf,
                                    &mut msg_buf,
                                    txn,
                                    commit_version.as_u64(),
                                    commit_version.as_u64(),
                                    *txn.branch_id.as_bytes(),
                                    timestamp,
                                );
                                wal_serialize_ns = ts.elapsed().as_nanos() as u64;
                                let tm = Instant::now();
                                let mut wal = wal_arc.lock(); // Lock Level 4: WAL append
                                wal_mutex_ns = tm.elapsed().as_nanos() as u64;
                                let tw = Instant::now();
                                let r =
                                    wal.append_pre_serialized(&rec_buf, commit_version.as_u64(), timestamp);
                                wal_io_ns = tw.elapsed().as_nanos() as u64;
                                r
                            })
                        });
                        if let Err(e) = result {
                            txn.status = TransactionStatus::Aborted {
                                reason: format!("WAL write failed: {}", e),
                            };
                            self.mark_version_applied(commit_version);
                            return Err(CommitError::WALError(e.to_string()));
                        }
                        tracing::debug!(target: "strata::txn", txn_id = txn.txn_id.as_u64(), commit_version = commit_version.as_u64(), "WAL durable");
                    }
                    WalMode::None => {}
                }
            });
        }

        // Apply to storage
        let t0 = Instant::now();
        perf_time!(trace, write_set_apply_ns, {
            if let Err(e) = txn.apply_writes(store, commit_version) {
                self.mark_version_applied(commit_version);
                if has_wal {
                    tracing::error!(
                        target: "strata::txn",
                        txn_id = txn.txn_id.as_u64(),
                        commit_version = commit_version.as_u64(),
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
        let apply_ns = t0.elapsed().as_nanos() as u64;

        // Version fully applied — advance visible_version (#1913)
        let t0 = Instant::now();
        self.mark_version_applied(commit_version);
        let mark_version_ns = t0.elapsed().as_nanos() as u64;

        // Profile accumulation
        if profiling {
            let total_ns = t_total.elapsed().as_nanos() as u64;
            COMMIT_PROF.with(|p| {
                let mut p = p.borrow_mut();
                p.count += 1;
                p.lock_acquire_ns += lock_acquire_ns;
                p.validation_ns += validation_ns;
                p.version_alloc_ns += version_alloc_ns;
                p.wal_serialize_ns += wal_serialize_ns;
                p.wal_mutex_ns += wal_mutex_ns;
                p.wal_io_ns += wal_io_ns;
                p.apply_ns += apply_ns;
                p.mark_version_ns += mark_version_ns;
                p.total_ns += total_ns;

                if p.count % PROFILE_INTERVAL == 0 {
                    let n = PROFILE_INTERVAL as f64;
                    let wal_total = p.wal_serialize_ns + p.wal_mutex_ns + p.wal_io_ns;
                    eprintln!(
                        "[commit-profile] {} commits | avg(us): \
                         total={:.1}  branch_lock={:.1}  validate={:.1}  ver_alloc={:.1}  \
                         wal[ser={:.1} mutex={:.1} io={:.1} sum={:.1}]  \
                         apply={:.1}  mark_ver={:.1}",
                        p.count,
                        p.total_ns as f64 / n / 1000.0,
                        p.lock_acquire_ns as f64 / n / 1000.0,
                        p.validation_ns as f64 / n / 1000.0,
                        p.version_alloc_ns as f64 / n / 1000.0,
                        p.wal_serialize_ns as f64 / n / 1000.0,
                        p.wal_mutex_ns as f64 / n / 1000.0,
                        p.wal_io_ns as f64 / n / 1000.0,
                        wal_total as f64 / n / 1000.0,
                        p.apply_ns as f64 / n / 1000.0,
                        p.mark_version_ns as f64 / n / 1000.0,
                    );
                    // Reset for next interval
                    p.lock_acquire_ns = 0;
                    p.validation_ns = 0;
                    p.version_alloc_ns = 0;
                    p.wal_serialize_ns = 0;
                    p.wal_mutex_ns = 0;
                    p.wal_io_ns = 0;
                    p.apply_ns = 0;
                    p.mark_version_ns = 0;
                    p.total_ns = 0;
                }
            });
        }

        #[cfg(feature = "perf-trace")]
        {
            trace.commit_total_ns = commit_start.elapsed().as_nanos() as u64;
            trace.keys_read = txn.read_set.len();
            trace.keys_written = txn.write_set.len();
            tracing::info!(
                target: "strata::perf",
                txn_id = txn.txn_id.as_u64(),
                commit_version = commit_version.as_u64(),
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
    pub fn commit<S: Storage>(
        &self,
        txn: &mut TransactionContext,
        store: &S,
        wal: Option<&mut WalWriter>,
    ) -> std::result::Result<CommitVersion, CommitError> {
        let wal_mode = match wal {
            Some(w) => WalMode::Direct(w),
            None => WalMode::None,
        };
        self.commit_inner(txn, store, wal_mode, None)
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
    /// Level 1 (shared) → Level 2 → Level 4 (brief).
    /// See [`lock_ordering`](crate::lock_ordering) for the full hierarchy.
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
    ) -> std::result::Result<CommitVersion, CommitError> {
        let wal_mode = match wal_arc {
            Some(arc) => WalMode::Shared(arc),
            None => WalMode::None,
        };
        self.commit_inner(txn, store, wal_mode, None)
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

    /// Mark a branch as being deleted (#1916).
    ///
    /// Subsequent commits on this branch will be rejected with
    /// `CommitError::BranchDeleting` after they acquire the per-branch lock.
    pub fn mark_branch_deleting(&self, branch_id: &BranchId) {
        self.deleting_branches.insert(*branch_id);
    }

    /// Check if a branch is currently marked as deleting (#2108).
    pub fn is_branch_deleting(&self, branch_id: &BranchId) -> bool {
        self.deleting_branches.contains(branch_id)
    }

    /// Remove the deleting mark for a branch (#1916).
    pub fn unmark_branch_deleting(&self, branch_id: &BranchId) {
        self.deleting_branches.remove(branch_id);
    }

    /// Clone the `Arc<Mutex<()>>` for a branch's commit lock (#1916).
    ///
    /// The caller can lock this to serialize with (and drain) in-flight
    /// commits on the branch. Used by `delete_branch` to ensure no commit
    /// is mid-apply before scanning and deleting branch data.
    pub fn branch_commit_lock(&self, branch_id: &BranchId) -> Arc<Mutex<()>> {
        self.commit_locks
            .entry(*branch_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Advance the version counter to at least `v`.
    ///
    /// Used during multi-process refresh to ensure the local version counter
    /// reflects writes applied from other processes' WAL entries.
    /// Uses `fetch_max` so the counter never goes backward.
    pub fn catch_up_version(&self, v: u64) {
        self.version.fetch_max(v, Ordering::AcqRel);
        // Data at version v was applied by another process, so it is
        // already visible in storage (#1913).
        self.visible_version.fetch_max(v, Ordering::AcqRel);
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
        wal: Option<&mut WalWriter>,
        version: CommitVersion,
    ) -> std::result::Result<CommitVersion, CommitError> {
        let wal_mode = match wal {
            Some(w) => WalMode::Direct(w),
            None => WalMode::None,
        };
        self.commit_inner(txn, store, wal_mode, Some(version))
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new(CommitVersion::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::TransactionPayload;
    use crate::{JsonStoreExt, TransactionContext};
    use parking_lot::Mutex as ParkingMutex;
    use std::sync::Arc;
    use strata_core::id::{CommitVersion, TxnId};
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
        let manager = TransactionManager::new(CommitVersion(100));
        assert_eq!(manager.current_version(), CommitVersion(100));
    }

    #[test]
    fn test_allocate_version_increments() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        assert_eq!(manager.allocate_version().unwrap(), CommitVersion(1));
        assert_eq!(manager.allocate_version().unwrap(), CommitVersion(2));
        assert_eq!(manager.allocate_version().unwrap(), CommitVersion(3));
        assert_eq!(manager.current_version(), CommitVersion(3));
    }

    #[test]
    fn test_next_txn_id_increments() {
        // TransactionManager::new(CommitVersion::ZERO) calls with_txn_id(0, 0), which sets next_txn_id = 0 + 1 = 1
        let manager = TransactionManager::new(CommitVersion::ZERO);
        assert_eq!(manager.next_txn_id().unwrap(), TxnId(1));
        assert_eq!(manager.next_txn_id().unwrap(), TxnId(2));
        assert_eq!(manager.next_txn_id().unwrap(), TxnId(3));
    }

    #[test]
    fn test_with_txn_id_starts_from_max_plus_one() {
        let manager = TransactionManager::with_txn_id(CommitVersion(50), TxnId(100));
        assert_eq!(manager.current_version(), CommitVersion(50));
        assert_eq!(manager.next_txn_id().unwrap(), TxnId(101)); // max_txn_id + 1
    }

    #[test]
    fn test_next_txn_id_overflow_returns_error() {
        // with_txn_id(0, max_txn_id) sets next_txn_id = max_txn_id + 1
        // So with_txn_id(0, u64::MAX - 2) → next_txn_id starts at u64::MAX - 1
        let manager = TransactionManager::with_txn_id(CommitVersion::ZERO, TxnId(u64::MAX - 2));
        // First call: returns u64::MAX - 1, counter advances to u64::MAX
        assert!(manager.next_txn_id().is_ok());
        // Second call fails: counter is at u64::MAX, cannot increment
        assert!(manager.next_txn_id().is_err());
    }

    #[test]
    fn test_allocate_version_overflow_returns_error() {
        // Version starts at u64::MAX - 1
        let manager = TransactionManager::with_txn_id(CommitVersion(u64::MAX - 1), TxnId::ZERO);
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
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));

        let branch_id1 = BranchId::new();
        let branch_id2 = BranchId::new();
        let ns1 = create_test_namespace(branch_id1);
        let ns2 = create_test_namespace(branch_id2);
        let key1 = create_test_key(&ns1, "key1");
        let key2 = create_test_key(&ns2, "key2");

        // Prepare transactions
        let mut txn1 = TransactionContext::with_store(TxnId(1), branch_id1, Arc::clone(&store));
        txn1.put(key1.clone(), Value::Int(1)).unwrap();

        let mut txn2 = TransactionContext::with_store(TxnId(2), branch_id2, Arc::clone(&store));
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
        assert!((CommitVersion(1)..=CommitVersion(2)).contains(&v1));
        assert!((CommitVersion(1)..=CommitVersion(2)).contains(&v2));
        assert_ne!(v1, v2); // Versions must be unique

        // Both keys should be in storage
        assert!(store.get_versioned(&key1, CommitVersion::MAX).unwrap().is_some());
        assert!(store.get_versioned(&key2, CommitVersion::MAX).unwrap().is_some());
    }

    #[test]
    fn test_same_branch_commits_serialize() {
        // This test verifies that commits on the same branch are serialized
        // (one completes before the other starts its critical section)
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key1 = create_test_key(&ns, "key1");
        let key2 = create_test_key(&ns, "key2");

        // Commit first transaction
        {
            let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
            txn.put(key1.clone(), Value::Int(100)).unwrap();
            let v = manager
                .commit(&mut txn, store.as_ref(), Some(&mut wal))
                .unwrap();
            assert_eq!(v, CommitVersion(1));
        }

        // Commit second transaction on same branch
        {
            let mut txn = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
            txn.put(key2.clone(), Value::Int(200)).unwrap();
            let v = manager
                .commit(&mut txn, store.as_ref(), Some(&mut wal))
                .unwrap();
            assert_eq!(v, CommitVersion(2));
        }

        // Both values should be present with correct versions
        let v1 = store.get_versioned(&key1, CommitVersion::MAX).unwrap().unwrap();
        assert_eq!(v1.value, Value::Int(100));
        assert_eq!(v1.version.as_u64(), 1);

        let v2 = store.get_versioned(&key2, CommitVersion::MAX).unwrap().unwrap();
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
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));

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
                    TxnId(i as u64 + 1),
                    branch_id,
                    Arc::clone(&store_clone),
                );
                txn.put(key, Value::Int(i as i64)).unwrap();

                let mut guard = wal_clone.lock();
                manager_clone.commit(&mut txn, store_clone.as_ref(), Some(&mut *guard))
            }));
        }

        // All commits should succeed
        let versions: Vec<CommitVersion> = handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect();

        // All versions should be unique and in range 1..=num_threads
        let mut sorted = versions.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), num_threads);
        assert_eq!(sorted[0], CommitVersion(1));
        assert_eq!(sorted[num_threads - 1], CommitVersion(num_threads as u64));
    }

    /// Test that scan_prefix tracks deleted keys in read_set for conflict detection.
    #[test]
    fn test_scan_prefix_deleted_key_conflict_detection() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_alice = create_test_key(&ns, "user:alice");
        let key_bob = create_test_key(&ns, "user:bob");
        let prefix = create_test_key(&ns, "user:");

        // Setup: Create initial data with alice and bob
        {
            let mut setup_txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
            setup_txn.put(key_alice.clone(), Value::Int(100)).unwrap();
            setup_txn.put(key_bob.clone(), Value::Int(200)).unwrap();
            manager
                .commit(&mut setup_txn, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: Delete alice (blind), then scan
        let mut txn1 = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
        txn1.delete(key_alice.clone()).unwrap(); // Blind delete
        let scan_result = txn1.scan_prefix(&prefix).unwrap();

        // T1 sees only bob (alice excluded due to delete_set)
        assert_eq!(scan_result.len(), 1);
        assert!(scan_result.iter().any(|(k, _)| k == &key_bob));

        // T2: Update alice and commit first
        {
            let mut txn2 = TransactionContext::with_store(TxnId(3), branch_id, Arc::clone(&store));
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
        let final_value = store.get_versioned(&key_alice, CommitVersion::MAX).unwrap().unwrap();
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
        let manager = TransactionManager::new(CommitVersion::ZERO);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_alice = create_test_key(&ns, "user:alice");

        // Setup: Create alice
        {
            let mut setup_txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
            setup_txn.put(key_alice.clone(), Value::Int(100)).unwrap();
            manager
                .commit(&mut setup_txn, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: Blind delete alice (no read, no scan)
        let mut txn1 = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
        txn1.delete(key_alice.clone()).unwrap(); // Blind delete - no read_set entry

        // T2: Update alice and commit first
        {
            let mut txn2 = TransactionContext::with_store(TxnId(3), branch_id, Arc::clone(&store));
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
        let final_value = store.get_versioned(&key_alice, CommitVersion::MAX).unwrap();
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        // Read-only transaction (just reads, no writes)
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));
        let branch_id = BranchId::new();

        let manager1 = Arc::clone(&manager);
        let store1 = Arc::clone(&store);
        let manager2 = Arc::clone(&manager);
        let store2 = Arc::clone(&store);

        let h1 = std::thread::spawn(move || {
            let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store1));
            manager1.commit(&mut txn, store1.as_ref(), None)
        });
        let h2 = std::thread::spawn(move || {
            let mut txn = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store2));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "blind");

        // Blind write: put with no prior read
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(42)).unwrap();
        assert!(txn.read_set.is_empty()); // Confirms it's a blind write

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());

        let stored = store.get_versioned(&key, CommitVersion::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(42));
    }

    #[test]
    fn test_write_with_read_still_validates() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let mut wal = create_test_wal(&wal_dir);
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "readwrite");

        // Setup: store initial value
        {
            let mut setup = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
            setup.put(key.clone(), Value::Int(1)).unwrap();
            manager
                .commit(&mut setup, store.as_ref(), Some(&mut wal))
                .unwrap();
        }

        // T1: read then write (not a blind write)
        let mut txn = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
        let _ = txn.get(&key).unwrap();
        txn.put(key.clone(), Value::Int(2)).unwrap();
        assert!(!txn.read_set.is_empty()); // Has reads → goes through validation

        // Concurrent update
        {
            let mut txn2 = TransactionContext::with_store(TxnId(3), branch_id, Arc::clone(&store));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "cas_key");

        // CAS operation → not a blind write, should validate
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "json_doc");

        // Has json_snapshot_versions → should go through full validation path
        // Use version 0 (key doesn't exist) so validation passes
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(1)).unwrap();
        txn.record_json_snapshot_version(key.clone(), 0);

        let result = manager.commit(&mut txn, store.as_ref(), Some(&mut wal));
        assert!(result.is_ok());
        // Verify it went through the normal path (version incremented)
        assert!(manager.current_version() > CommitVersion::ZERO);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "arc_basic");

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(42)).unwrap();

        let v = manager
            .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
            .unwrap();
        assert_eq!(v, CommitVersion(1));

        // Verify in-memory storage
        let stored = store.get_versioned(&key, CommitVersion::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(42));
        assert_eq!(stored.version.as_u64(), 1);

        // Drop WAL to flush buffers, then recover from disk
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let result = recovery.recover().unwrap();

        // Recovery should find exactly 1 transaction
        assert_eq!(result.stats.txns_replayed, 1);
        assert_eq!(result.stats.final_version, CommitVersion(1));
        assert_eq!(result.stats.writes_applied, 1);

        // Recovered storage should contain the committed value
        let recovered = result
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.value, Value::Int(42));
        assert_eq!(recovered.version.as_u64(), 1);
    }

    #[test]
    fn test_commit_with_wal_arc_no_wal() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "no_wal");

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(7)).unwrap();

        let v = manager
            .commit_with_wal_arc(&mut txn, store.as_ref(), None)
            .unwrap();
        assert_eq!(v, CommitVersion(1));

        let stored = store.get_versioned(&key, CommitVersion::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(7));
    }

    #[test]
    fn test_commit_with_wal_arc_many_parallel_branches() {
        // Stress test: 10 threads, each on a different branch, sharing one WAL Arc.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));

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
                    TransactionContext::with_store(TxnId(i as u64 + 1), branch_id, Arc::clone(&s));
                txn.put(key, Value::Int(i as i64)).unwrap();

                m.commit_with_wal_arc(&mut txn, s.as_ref(), Some(&w))
            }));
        }

        let versions: Vec<CommitVersion> = handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect();

        // All versions should be unique and in range 1..=num_threads
        let mut sorted = versions.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), num_threads);
        assert_eq!(sorted[0], CommitVersion(1));
        assert_eq!(sorted[num_threads - 1], CommitVersion(num_threads as u64));

        // Recover from WAL to verify all 10 records are well-formed
        drop(wal);
        let recovery = crate::RecoveryCoordinator::new(wal_dir);
        let result = recovery.recover().unwrap();
        assert_eq!(result.stats.txns_replayed, num_threads);
        assert_eq!(result.stats.final_version, CommitVersion(num_threads as u64));
    }

    #[test]
    fn test_commit_with_wal_arc_validation_conflict_not_in_wal() {
        // Key invariant: validation failure must abort BEFORE WAL write.
        // Verify by recovering and counting WAL records.
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "conflict");

        // Setup: store initial value (WAL record #1)
        {
            let mut setup = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
            setup.put(key.clone(), Value::Int(1)).unwrap();
            manager
                .commit_with_wal_arc(&mut setup, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // T1: read-modify-write (will conflict)
        let mut txn1 = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
        let _ = txn1.get(&key).unwrap(); // Creates read_set entry
        txn1.put(key.clone(), Value::Int(10)).unwrap();

        // T2: concurrent blind write, commits first (WAL record #2)
        {
            let mut txn2 = TransactionContext::with_store(TxnId(3), branch_id, Arc::clone(&store));
            txn2.put(key.clone(), Value::Int(99)).unwrap();
            manager
                .commit_with_wal_arc(&mut txn2, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // T1 should fail validation
        let result = manager.commit_with_wal_arc(&mut txn1, store.as_ref(), Some(&wal));
        assert!(result.is_err(), "Should detect read-write conflict");

        // T2's value should be preserved in storage
        let stored = store.get_versioned(&key, CommitVersion::MAX).unwrap().unwrap();
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
            .get_versioned(&key, CommitVersion::MAX)
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();

        let v_before = manager.current_version();

        // Read-only transaction: no writes
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        let v_before = manager.current_version();

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key_a = create_test_key(&ns, "multi_a");
        let key_b = create_test_key(&ns, "multi_b");
        let key_c = create_test_key(&ns, "multi_c");

        // Setup: write key_a and key_b
        {
            let mut setup = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
            setup.put(key_a.clone(), Value::Int(10)).unwrap();
            setup.put(key_b.clone(), Value::Int(20)).unwrap();
            manager
                .commit_with_wal_arc(&mut setup, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // Transaction: delete key_a, update key_b, insert key_c
        {
            let mut txn = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
            txn.delete(key_a.clone()).unwrap();
            txn.put(key_b.clone(), Value::Int(200)).unwrap();
            txn.put(key_c.clone(), Value::Bytes(b"hello".to_vec()))
                .unwrap();
            manager
                .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
                .unwrap();
        }

        // Verify in-memory state
        assert!(store.get_versioned(&key_a, CommitVersion::MAX).unwrap().is_none());
        assert_eq!(
            store
                .get_versioned(&key_b, CommitVersion::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(200)
        );
        assert_eq!(
            store
                .get_versioned(&key_c, CommitVersion::MAX)
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
            .get_versioned(&key_a, CommitVersion::MAX)
            .unwrap()
            .is_none());
        // key_b: updated from 20 → 200
        assert_eq!(
            result
                .storage
                .get_versioned(&key_b, CommitVersion::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(200)
        );
        // key_c: newly inserted
        assert_eq!(
            result
                .storage
                .get_versioned(&key_c, CommitVersion::MAX)
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key1 = create_test_key(&ns, "seq1");
        let key2 = create_test_key(&ns, "seq2");

        {
            let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
            txn.put(key1.clone(), Value::Int(100)).unwrap();
            let v = manager
                .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
                .unwrap();
            assert_eq!(v, CommitVersion(1));
        }

        {
            let mut txn = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
            txn.put(key2.clone(), Value::Int(200)).unwrap();
            let v = manager
                .commit_with_wal_arc(&mut txn, store.as_ref(), Some(&wal))
                .unwrap();
            assert_eq!(v, CommitVersion(2));
        }

        // Both values present with correct versions
        let v1 = store.get_versioned(&key1, CommitVersion::MAX).unwrap().unwrap();
        assert_eq!(v1.value, Value::Int(100));
        assert_eq!(v1.version.as_u64(), 1);

        let v2 = store.get_versioned(&key2, CommitVersion::MAX).unwrap().unwrap();
        assert_eq!(v2.value, Value::Int(200));
        assert_eq!(v2.version.as_u64(), 2);
    }

    // ========================================================================
    // E-8: remove_branch_lock coordination tests
    // ========================================================================

    #[test]
    fn test_remove_branch_lock_succeeds_when_not_held() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();

        // No lock entry exists — removal should return true
        assert!(manager.remove_branch_lock(&branch_id));
    }

    #[test]
    fn test_remove_branch_lock_serializes_with_commit() {
        // Verify that remove_branch_lock removes the entry and that the
        // next commit recreates it via entry().or_insert_with().
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));
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
    // No-WAL commit tests (formerly bulk-load, Epic 8d)
    // ========================================================================

    #[test]
    fn commit_no_wal_applies_writes() {
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "bulk_key");

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(42)).unwrap();

        let version = manager.commit(&mut txn, store.as_ref(), None).unwrap();
        assert!(version > CommitVersion::ZERO);

        // Data should be readable
        let result = store.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Value::Int(42));
    }

    #[test]
    fn commit_no_wal_skips_wal_records() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Create WAL for reference
        let mut wal = create_test_wal(&wal_dir);

        // No-WAL commit — should NOT write to WAL
        let key = create_test_key(&ns, "no_wal");
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn.put(key.clone(), Value::Int(1)).unwrap();
        manager.commit(&mut txn, store.as_ref(), None).unwrap();

        // Normal commit — writes to WAL
        let key2 = create_test_key(&ns, "with_wal");
        let mut txn2 = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
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
            "No-WAL commit should not write WAL records"
        );
    }

    #[test]
    fn commit_no_wal_then_normal_commits() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let store = Arc::new(SegmentedStore::new());
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let mut wal = create_test_wal(&wal_dir);

        // No-WAL commit
        let key1 = create_test_key(&ns, "k1");
        let mut txn1 = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
        txn1.put(key1.clone(), Value::Int(1)).unwrap();
        manager.commit(&mut txn1, store.as_ref(), None).unwrap();

        // Normal commit after no-WAL commit
        let key2 = create_test_key(&ns, "k2");
        let mut txn2 = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
        txn2.put(key2.clone(), Value::Int(2)).unwrap();
        manager
            .commit(&mut txn2, store.as_ref(), Some(&mut wal))
            .unwrap();

        // Both values readable
        assert_eq!(
            store.get_versioned(&key1, CommitVersion::MAX).unwrap().unwrap().value,
            Value::Int(1)
        );
        assert_eq!(
            store.get_versioned(&key2, CommitVersion::MAX).unwrap().unwrap().value,
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
        let manager = TransactionManager::new(CommitVersion::ZERO);

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // T1 starts first → gets txn_id=1
        let mut txn1 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        txn1.put(create_test_key(&ns, "k1"), Value::Int(1)).unwrap();
        assert_eq!(txn1.txn_id, TxnId(1), "T1 should get txn_id=1");

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
        assert_eq!(v2, CommitVersion(1), "T2 should get commit_version=1");

        // T1 commits second → gets commit_version=2
        let v1 = manager
            .commit(&mut txn1, store.as_ref(), Some(&mut wal))
            .unwrap();
        assert_eq!(v1, CommitVersion(2), "T1 should get commit_version=2");

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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "blind_key");

        // No lock entry should exist initially
        assert!(!manager.commit_locks.contains_key(&branch_id));

        // Commit a blind write via commit_with_wal_arc
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "blind_key");

        assert!(!manager.commit_locks.contains_key(&branch_id));

        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
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
            max_version: CommitVersion,
        ) -> strata_core::error::StrataResult<Option<strata_core::contract::VersionedValue>>
        {
            self.inner.get_versioned(key, max_version)
        }

        fn get_history(
            &self,
            key: &Key,
            limit: Option<usize>,
            before_version: Option<CommitVersion>,
        ) -> strata_core::error::StrataResult<Vec<strata_core::contract::VersionedValue>> {
            self.inner.get_history(key, limit, before_version)
        }

        fn scan_prefix(
            &self,
            prefix: &Key,
            max_version: CommitVersion,
        ) -> strata_core::error::StrataResult<Vec<(Key, strata_core::contract::VersionedValue)>>
        {
            self.inner.scan_prefix(prefix, max_version)
        }

        fn current_version(&self) -> CommitVersion {
            self.inner.current_version()
        }

        fn put_with_version_mode(
            &self,
            key: Key,
            value: Value,
            version: CommitVersion,
            ttl: Option<std::time::Duration>,
            mode: strata_core::traits::WriteMode,
        ) -> strata_core::error::StrataResult<()> {
            self.inner
                .put_with_version_mode(key, value, version, ttl, mode)
        }

        fn delete_with_version(
            &self,
            key: &Key,
            version: CommitVersion,
        ) -> strata_core::error::StrataResult<()> {
            self.inner.delete_with_version(key, version)
        }

        fn apply_writes_atomic(
            &self,
            writes: Vec<(Key, Value, strata_core::traits::WriteMode)>,
            deletes: Vec<Key>,
            version: CommitVersion,
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
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "test_key");

        // Prepare a blind-write transaction (skips validation, no store reads)
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion::ZERO);
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
            current,
            CommitVersion(1),
            "current_version() reflects allocated-but-unapplied version"
        );

        // quiesced_version() must block until the commit finishes
        let start = std::time::Instant::now();
        let quiesced = manager.quiesced_version();
        let elapsed = start.elapsed();

        // The commit should have completed (200ms delay)
        assert_eq!(
            quiesced,
            1,
            "quiesced_version returns version after commit completes"
        );
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "quiesced_version() must have blocked waiting for in-flight commit \
             (elapsed: {:?}, expected >= 50ms)",
            elapsed
        );

        let committed_version = commit_handle.join().unwrap();
        assert_eq!(committed_version, CommitVersion(1));
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
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));

        let num_writers = 4;
        let mut handles = Vec::new();

        for i in 0..num_writers {
            let manager_clone = Arc::clone(&manager);
            let store_clone = Arc::clone(&store);
            let branch_id = BranchId::new();
            let ns = create_test_namespace(branch_id);
            let key = create_test_key(&ns, &format!("key_{}", i));

            handles.push(std::thread::spawn(move || {
                let mut txn = TransactionContext::new(TxnId(i as u64 + 1), branch_id, CommitVersion::ZERO);
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
            max_version: CommitVersion,
        ) -> strata_core::error::StrataResult<Option<strata_core::contract::VersionedValue>>
        {
            self.inner.get_versioned(key, max_version)
        }

        fn get_history(
            &self,
            key: &Key,
            limit: Option<usize>,
            before_version: Option<CommitVersion>,
        ) -> strata_core::error::StrataResult<Vec<strata_core::contract::VersionedValue>> {
            self.inner.get_history(key, limit, before_version)
        }

        fn scan_prefix(
            &self,
            prefix: &Key,
            max_version: CommitVersion,
        ) -> strata_core::error::StrataResult<Vec<(Key, strata_core::contract::VersionedValue)>>
        {
            self.inner.scan_prefix(prefix, max_version)
        }

        fn current_version(&self) -> CommitVersion {
            self.inner.current_version()
        }

        fn put_with_version_mode(
            &self,
            key: Key,
            value: Value,
            version: CommitVersion,
            ttl: Option<std::time::Duration>,
            mode: strata_core::traits::WriteMode,
        ) -> strata_core::error::StrataResult<()> {
            self.inner
                .put_with_version_mode(key, value, version, ttl, mode)
        }

        fn delete_with_version(
            &self,
            key: &Key,
            version: CommitVersion,
        ) -> strata_core::error::StrataResult<()> {
            self.inner.delete_with_version(key, version)
        }

        fn apply_writes_atomic(
            &self,
            _writes: Vec<(Key, Value, strata_core::traits::WriteMode)>,
            _deletes: Vec<Key>,
            _version: CommitVersion,
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "invisible_key");

        // Create a transaction with a write (blind write — skips validation)
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, real_store);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "invisible_key");

        let mut txn = TransactionContext::with_store(TxnId(2), branch_id, real_store);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = create_test_key(&ns, "invisible_key");

        let mut txn = TransactionContext::with_store(TxnId(3), branch_id, real_store);
        txn.put(key.clone(), Value::Int(44)).unwrap();

        let result = manager.commit_with_version(&mut txn, &failing_store, Some(&mut wal), CommitVersion(42));
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
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
        assert_eq!(payload.version, commit_v.as_u64());
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
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
        let manager = TransactionManager::new(CommitVersion::ZERO);
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

    /// Issue #1913: Cross-branch snapshot ordering.
    ///
    /// When branch A allocates version N and branch B allocates N+1, B can
    /// finish apply_writes before A. A reader using `current_version()` as its
    /// snapshot version would see B's writes but miss A's writes (version gap).
    ///
    /// `visible_version()` must NOT advance past N-1 while version N is
    /// still in-flight, ensuring no reader sees a state that never existed.
    #[test]
    fn test_issue_1913_cross_branch_snapshot_ordering() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);

        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_a = create_test_namespace(branch_a);
        let ns_b = create_test_namespace(branch_b);
        let key_a = create_test_key(&ns_a, "key_a");
        let key_b = create_test_key(&ns_b, "key_b");

        // Prepare two transactions on different branches
        let mut txn_a = TransactionContext::with_store(TxnId(1), branch_a, Arc::clone(&store));
        txn_a.put(key_a.clone(), Value::Int(100)).unwrap();
        txn_a.status = TransactionStatus::Committed; // skip validation

        let mut txn_b = TransactionContext::with_store(TxnId(2), branch_b, Arc::clone(&store));
        txn_b.put(key_b.clone(), Value::Int(200)).unwrap();
        txn_b.status = TransactionStatus::Committed;

        // Step 1: Both branches allocate versions (simulates parallel allocation)
        let version_a = manager.allocate_version().unwrap(); // 1
        let version_b = manager.allocate_version().unwrap(); // 2
        assert_eq!(version_a, CommitVersion(1));
        assert_eq!(version_b, CommitVersion(2));

        // Step 2: Branch B applies FIRST (out-of-order, the faster branch)
        txn_b.apply_writes(store.as_ref(), version_b).unwrap();
        manager.mark_version_applied(version_b);

        // Step 3: visible_version must NOT include version_a (still in-flight)
        // A safe snapshot must be < version_a to prevent reading a state
        // where B is committed but A is not yet visible.
        let vis = manager.visible_version();
        assert!(
            vis < version_a,
            "visible_version ({}) must be < in-flight version {} \
             to prevent snapshot gap (issue #1913). \
             store.version()={}, current_version()={}",
            vis,
            version_a,
            store.version(),
            manager.current_version(),
        );

        // Step 4: Now branch A applies (slow branch completes)
        txn_a.apply_writes(store.as_ref(), version_a).unwrap();
        manager.mark_version_applied(version_a);

        // Step 5: After both are applied, visible_version must include both
        let vis_final = manager.visible_version();
        assert!(
            vis_final >= version_b,
            "After all versions applied, visible_version ({}) must be >= {}",
            vis_final,
            version_b,
        );

        // Step 6: Verify data is actually readable at the final visible version
        let result_a = store.get_versioned(&key_a, vis_final).unwrap();
        let result_b = store.get_versioned(&key_b, vis_final).unwrap();
        assert!(
            result_a.is_some(),
            "key_a must be visible at version {}",
            vis_final
        );
        assert!(
            result_b.is_some(),
            "key_b must be visible at version {}",
            vis_final
        );
    }

    /// Issue #1913 concurrent stress test: many parallel commits must produce
    /// a visible_version that is safe for snapshot reads at all times.
    #[test]
    fn test_issue_1913_cross_branch_snapshot_ordering_concurrent() {
        use std::sync::Barrier;

        let store = Arc::new(SegmentedStore::new());
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));
        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let store = Arc::clone(&store);
                let manager = Arc::clone(&manager);
                let barrier = Arc::clone(&barrier);

                std::thread::spawn(move || {
                    let branch_id = BranchId::new();
                    let ns = create_test_namespace(branch_id);
                    let key = create_test_key(&ns, &format!("key_{}", i));

                    let mut txn =
                        TransactionContext::with_store(TxnId(i as u64 + 1), branch_id, Arc::clone(&store));
                    txn.put(key, Value::Int(i as i64)).unwrap();
                    txn.status = TransactionStatus::Committed;

                    let version = manager.allocate_version().unwrap();

                    // Synchronize: all threads allocate before any applies
                    barrier.wait();

                    // Apply writes (order is non-deterministic)
                    txn.apply_writes(store.as_ref(), version).unwrap();
                    manager.mark_version_applied(version);

                    version
                })
            })
            .collect();

        let versions: Vec<CommitVersion> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let max_version = *versions.iter().max().unwrap();

        // After all threads complete, visible_version must include all versions
        let vis = manager.visible_version();
        assert!(
            vis >= max_version,
            "After all {} commits applied, visible_version ({:?}) must be >= max_version ({:?})",
            num_threads,
            vis,
            max_version,
        );

        // At no point during execution should visible_version have been
        // higher than the lowest unapplied version - 1. We can't verify
        // the invariant at every instant, but the final state must be correct.
    }

    /// Issue #1913: version gap — a failed commit must not block visible_version.
    ///
    /// If version N is allocated but the commit fails (WAL error, etc.),
    /// mark_version_applied(N) must still advance visible_version past N
    /// so subsequent versions become visible.
    #[test]
    fn test_issue_1913_version_gap_does_not_block_visible() {
        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);

        let branch = BranchId::new();
        let ns = create_test_namespace(branch);
        let key = create_test_key(&ns, "key1");

        // Version 1: allocated and applied normally
        let v1 = manager.allocate_version().unwrap();
        {
            let mut txn = TransactionContext::with_store(TxnId(1), branch, Arc::clone(&store));
            txn.put(key.clone(), Value::Int(1)).unwrap();
            txn.status = TransactionStatus::Committed;
            txn.apply_writes(store.as_ref(), v1).unwrap();
        }
        manager.mark_version_applied(v1);
        assert_eq!(manager.visible_version(), v1);

        // Version 2: allocated but commit FAILS (simulates WAL error)
        let v2 = manager.allocate_version().unwrap();
        // No apply_writes — the commit failed. Just unregister the gap.
        manager.mark_version_applied(v2);

        // visible_version must advance past the gap
        assert_eq!(
            manager.visible_version(),
            v2,
            "visible_version must advance past version gap (failed commit at v{})",
            v2,
        );

        // Version 3: normal commit after the gap
        let v3 = manager.allocate_version().unwrap();
        {
            let mut txn = TransactionContext::with_store(TxnId(3), branch, Arc::clone(&store));
            txn.put(key.clone(), Value::Int(3)).unwrap();
            txn.status = TransactionStatus::Committed;
            txn.apply_writes(store.as_ref(), v3).unwrap();
        }
        manager.mark_version_applied(v3);
        assert_eq!(manager.visible_version(), v3);
    }

    /// Issue #1915: json_exists(), json_get(), and json_set() don't record reads
    /// for non-existent documents, making OCC blind to create-if-absent races.
    ///
    /// Scenario: T1 and T2 both call json_exists() → false, then json_set() to
    /// create the doc. Both should record snapshot_version=0 for the missing doc.
    /// T1 commits first (creating the doc). T2's commit must detect the conflict
    /// because the doc now exists (version > 0) but T2 recorded version 0.
    #[test]
    fn test_issue_1915_json_missing_doc_read_invisible_to_occ() {
        use strata_core::primitives::json::JsonPath;

        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let json_key = Key::new_json(ns.clone(), "unique_doc");

        // T1: check existence → false, then set the document
        let mut txn1 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        let exists1 = txn1.json_exists(&json_key).unwrap();
        assert!(!exists1, "doc should not exist yet");

        // T1 creates the document via json_set at root
        let doc = serde_json::json!({"from": "txn1"});
        txn1.json_set(&json_key, &JsonPath::root(), doc.into())
            .unwrap();

        // T2: also check existence → false, then set the document
        let mut txn2 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        let exists2 = txn2.json_exists(&json_key).unwrap();
        assert!(!exists2, "doc should not exist yet for T2 either");

        let doc2 = serde_json::json!({"from": "txn2"});
        txn2.json_set(&json_key, &JsonPath::root(), doc2.into())
            .unwrap();

        // T1 commits first — should succeed
        let v1 = manager.commit(&mut txn1, store.as_ref(), None);
        assert!(v1.is_ok(), "T1 commit should succeed: {:?}", v1.err());

        // T2 commits second — MUST fail with conflict because the doc now exists
        // but T2 observed it as missing (snapshot_version=0, current_version > 0)
        let v2 = manager.commit(&mut txn2, store.as_ref(), None);
        assert!(
            v2.is_err(),
            "T2 commit must fail: json_exists() saw missing doc but T1 created it"
        );
    }

    /// Issue #1915: json_get() for a non-existent document must record
    /// snapshot_version=0 so that a concurrent create is detected.
    #[test]
    fn test_issue_1915_json_get_missing_doc_records_read() {
        use strata_core::primitives::json::JsonPath;

        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let json_key = Key::new_json(ns.clone(), "phantom_doc");

        // T1: json_get on non-existent doc → None, then write something
        let mut txn1 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        let result = txn1.json_get(&json_key, &JsonPath::root()).unwrap();
        assert!(result.is_none(), "doc should not exist");

        // T1 creates the doc
        let doc = serde_json::json!({"data": 1});
        txn1.json_set(&json_key, &JsonPath::root(), doc.into())
            .unwrap();

        // T2: also json_get on non-existent doc → None, then create
        let mut txn2 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        let result2 = txn2.json_get(&json_key, &JsonPath::root()).unwrap();
        assert!(result2.is_none(), "doc should not exist for T2");

        let doc2 = serde_json::json!({"data": 2});
        txn2.json_set(&json_key, &JsonPath::root(), doc2.into())
            .unwrap();

        // T1 commits
        let v1 = manager.commit(&mut txn1, store.as_ref(), None);
        assert!(v1.is_ok(), "T1 commit should succeed");

        // T2 must fail — json_get saw doc as absent, but T1 created it
        let v2 = manager.commit(&mut txn2, store.as_ref(), None);
        assert!(
            v2.is_err(),
            "T2 commit must fail: json_get() saw missing doc but T1 created it"
        );
    }

    /// Issue #1915: json_set() on a non-existent document must record
    /// snapshot_version=0 so that concurrent creation is detected.
    #[test]
    fn test_issue_1915_json_set_missing_doc_records_read() {
        use strata_core::primitives::json::JsonPath;

        let store = Arc::new(SegmentedStore::new());
        let manager = TransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let json_key = Key::new_json(ns.clone(), "set_race_doc");

        // T1: json_set on a non-existent doc (blind create)
        let mut txn1 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        let doc = serde_json::json!({"from": "txn1"});
        txn1.json_set(&json_key, &JsonPath::root(), doc.into())
            .unwrap();

        // T2: also json_set on the same non-existent doc
        let mut txn2 = TransactionContext::with_store(
            manager.next_txn_id().unwrap(),
            branch_id,
            Arc::clone(&store),
        );
        let doc2 = serde_json::json!({"from": "txn2"});
        txn2.json_set(&json_key, &JsonPath::root(), doc2.into())
            .unwrap();

        // T1 commits first
        let v1 = manager.commit(&mut txn1, store.as_ref(), None);
        assert!(v1.is_ok(), "T1 commit should succeed");

        // T2 must fail — both read the doc as absent and tried to create it
        let v2 = manager.commit(&mut txn2, store.as_ref(), None);
        assert!(
            v2.is_err(),
            "T2 commit must fail: json_set() on missing doc but T1 created it"
        );
    }

    // ========================================================================
    // Issue #1912 — Lock Ordering Tests
    // ========================================================================

    /// Stress test exercising all lock levels concurrently:
    ///   Level 1: Quiesce RwLock (shared by commits, exclusive by checkpoint)
    ///   Level 2: Per-branch commit Mutex
    ///   Level 4: WAL Mutex (via commit_with_wal_arc)
    ///   Level 5: DashMap shard guards (implicit in commit_locks access)
    ///
    /// A deadlock here means the lock ordering is violated.
    #[test]
    fn test_issue_1912_lock_ordering_concurrent() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::sync::Barrier;

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));

        let num_committers = 8;
        let commits_per_thread = 50;
        let done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(num_committers + 1)); // +1 for checkpoint thread

        // Checkpoint thread: repeatedly calls quiesced_version() (Level 1 exclusive)
        let checkpoint_done = Arc::clone(&done);
        let checkpoint_mgr = Arc::clone(&manager);
        let checkpoint_barrier = Arc::clone(&barrier);
        let checkpoint_handle = std::thread::spawn(move || {
            checkpoint_barrier.wait();
            let mut checkpoint_count = 0u64;
            while !checkpoint_done.load(AtomicOrdering::Relaxed) {
                let _v = checkpoint_mgr.quiesced_version();
                checkpoint_count += 1;
                // Yield to let commit threads make progress
                std::thread::yield_now();
            }
            checkpoint_count
        });

        // Commit threads: exercise Level 1 (shared) → Level 2 → Level 4 → Level 5
        let mut commit_handles = Vec::new();
        for i in 0..num_committers {
            let mgr = Arc::clone(&manager);
            let st = Arc::clone(&store);
            let w = Arc::clone(&wal);
            let b = Arc::clone(&barrier);

            commit_handles.push(std::thread::spawn(move || {
                let branch_id = BranchId::new();
                let ns = create_test_namespace(branch_id);
                b.wait();
                for j in 0..commits_per_thread {
                    let key = create_test_key(&ns, &format!("k_{}_{}", i, j));
                    let mut txn = TransactionContext::with_store(
                        TxnId((i * commits_per_thread + j) as u64 + 1),
                        branch_id,
                        Arc::clone(&st),
                    );
                    txn.put(key, Value::Int((i * commits_per_thread + j) as i64))
                        .unwrap();
                    // Use commit_with_wal_arc to exercise WAL lock (Level 4)
                    mgr.commit_with_wal_arc(&mut txn, st.as_ref(), Some(&w))
                        .unwrap();
                }
            }));
        }

        // Wait for all commit threads
        for h in commit_handles {
            h.join()
                .expect("Commit thread panicked — possible deadlock");
        }

        // Signal checkpoint thread to stop
        done.store(true, AtomicOrdering::Relaxed);
        let checkpoint_count = checkpoint_handle.join().unwrap();

        // Verify: all commits succeeded, checkpoint ran multiple times
        let final_version = manager.current_version();
        assert_eq!(
            final_version,
            CommitVersion((num_committers * commits_per_thread) as u64),
            "All commits should have succeeded"
        );
        assert!(
            checkpoint_count > 0,
            "Checkpoint thread should have run at least once"
        );
    }

    /// Same-branch variant: multiple threads commit to the SAME branch
    /// while checkpoint runs concurrently. This exercises the per-branch
    /// Mutex serialization (Level 2) under contention with Level 1 exclusive.
    #[test]
    fn test_issue_1912_lock_ordering_same_branch_stress() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::sync::Barrier;

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let wal = Arc::new(ParkingMutex::new(create_test_wal(&wal_dir)));
        let store = Arc::new(SegmentedStore::new());
        let manager = Arc::new(TransactionManager::new(CommitVersion::ZERO));

        let branch_id = BranchId::new();
        let num_committers = 4;
        let commits_per_thread = 100;
        let done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(num_committers + 1));

        // Checkpoint thread
        let checkpoint_done = Arc::clone(&done);
        let checkpoint_mgr = Arc::clone(&manager);
        let checkpoint_barrier = Arc::clone(&barrier);
        let checkpoint_handle = std::thread::spawn(move || {
            checkpoint_barrier.wait();
            while !checkpoint_done.load(AtomicOrdering::Relaxed) {
                let _v = checkpoint_mgr.quiesced_version();
                std::thread::yield_now();
            }
        });

        // Commit threads — all on the same branch (blind writes, so no validation conflicts)
        let mut handles = Vec::new();
        for i in 0..num_committers {
            let mgr = Arc::clone(&manager);
            let st = Arc::clone(&store);
            let w = Arc::clone(&wal);
            let b = Arc::clone(&barrier);

            handles.push(std::thread::spawn(move || {
                let ns = create_test_namespace(branch_id);
                b.wait();
                for j in 0..commits_per_thread {
                    let key = create_test_key(&ns, &format!("sb_{}_{}", i, j));
                    let mut txn = TransactionContext::with_store(
                        TxnId((i * commits_per_thread + j) as u64 + 1),
                        branch_id,
                        Arc::clone(&st),
                    );
                    txn.put(key, Value::Int(j as i64)).unwrap();
                    mgr.commit_with_wal_arc(&mut txn, st.as_ref(), Some(&w))
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.join()
                .expect("Commit thread panicked — possible deadlock");
        }

        done.store(true, AtomicOrdering::Relaxed);
        checkpoint_handle.join().unwrap();

        let final_version = manager.current_version();
        assert_eq!(
            final_version,
            CommitVersion((num_committers * commits_per_thread) as u64),
            "All same-branch commits should have succeeded"
        );
    }
}
