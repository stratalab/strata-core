//! Durability-aware commit adapter for the storage transaction manager.
//!
//! Provides atomic commit by orchestrating:
//! 1. Generic validation/version allocation through `strata-storage`
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

use super::payload;
use crate::durability::now_micros;
use crate::durability::wal::WalWriter;
use crate::{
    CommitError, Storage, TransactionContext, TransactionManager as StorageTransactionManager,
};
use parking_lot::Mutex;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};

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

use std::time::Instant;

static COMMIT_PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);
static COMMIT_PROFILE_CHECKED: AtomicBool = AtomicBool::new(false);

// Apply-failure injection is thread-local so parallel tests cannot drain
// each other's armed injections.
#[cfg(feature = "fault-injection")]
thread_local! {
    static APPLY_FAILURE_INJECTION: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[cfg(feature = "fault-injection")]
pub(crate) fn inject_apply_failure_once(reason: impl Into<String>) {
    APPLY_FAILURE_INJECTION.with(|slot| {
        *slot.borrow_mut() = Some(reason.into());
    });
}

#[cfg(feature = "fault-injection")]
pub(crate) fn clear_apply_failure_injection() {
    APPLY_FAILURE_INJECTION.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[cfg(feature = "fault-injection")]
fn maybe_take_apply_failure_injection() -> Option<String> {
    APPLY_FAILURE_INJECTION.with(|slot| slot.borrow_mut().take())
}

#[cfg(feature = "perf-trace")]
macro_rules! perf_time {
    ($trace:ident, $field:ident, $expr:block) => {{
        let start = Instant::now();
        let result = $expr;
        $trace.$field = start.elapsed().as_nanos() as u64;
        result
    }};
}

#[cfg(not(feature = "perf-trace"))]
macro_rules! perf_time {
    ($trace:ident, $field:ident, $expr:block) => {{
        $expr
    }};
}

#[cfg(feature = "perf-trace")]
#[derive(Default)]
struct PerfTrace {
    commit_total_ns: u64,
    // The storage-owned manager now encapsulates validation/version/apply.
    // This adapter only has direct visibility into the WAL-specific phase.
    wal_total_ns: u64,
    keys_read: usize,
    keys_written: usize,
}

#[cfg(not(feature = "perf-trace"))]
#[derive(Default)]
struct PerfTrace;

fn writer_halted_commit_error(wal: &WalWriter) -> Option<CommitError> {
    wal.bg_error().map(|bg_error| CommitError::WriterHalted {
        reason: bg_error.message.clone(),
        first_observed_at: bg_error.first_observed_at,
    })
}

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
    // Keep this profile limited to timings the adapter actually owns so the
    // emitted numbers cannot silently drift to zero during refactors.
    wal_serialize_ns: u64,
    wal_mutex_ns: u64,
    wal_io_ns: u64,
    total_ns: u64,
}

thread_local! {
    static COMMIT_PROF: RefCell<CommitProfile> = const { RefCell::new(CommitProfile {
        count: 0, wal_serialize_ns: 0, wal_mutex_ns: 0, wal_io_ns: 0, total_ns: 0,
    }) };
}

/// Describes how the WAL record should be written during commit.
///
/// Used by the durability adapter to abstract over the three
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

/// Core durability-aware commit implementation shared by the shell entry
/// points below.
fn commit_inner<S: Storage>(
    manager: &StorageTransactionManager,
    txn: &mut TransactionContext,
    store: &S,
    wal_mode: WalMode<'_>,
    external_version: Option<CommitVersion>,
) -> std::result::Result<CommitVersion, CommitError> {
    let profiling = commit_profile_enabled();
    let t_total = Instant::now();

    #[cfg(feature = "perf-trace")]
    let commit_start = std::time::Instant::now();
    #[allow(unused_mut, unused_variables)]
    let mut trace = PerfTrace::default();

    // WAL write (durability)
    //
    // Uses fused zero-clone serialization: iterates write_set/delete_set
    // by reference (no Key/Value clones), serializes msgpack + WAL envelope
    // into thread-local reusable buffers (zero alloc after warmup).
    let has_wal = wal_mode.has_wal();
    let mut wal_serialize_ns = 0u64;
    let mut wal_mutex_ns = 0u64;
    let mut wal_io_ns = 0u64;
    let commit_version = manager.commit_with_hook(txn, store, external_version, has_wal, |txn, commit_version| {
        perf_time!(trace, wal_total_ns, {
            match wal_mode {
                WalMode::Direct(wal) => {
                    let timestamp = now_micros();
                    let wal_txn_id = TxnId(commit_version.as_u64());
                    let result = WAL_RECORD_BUF.with(|rec_cell| {
                        MSGPACK_BUF.with(|msg_cell| {
                            let mut rec_buf = rec_cell.borrow_mut();
                            let mut msg_buf = msg_cell.borrow_mut();
                            let ts = Instant::now();
                            payload::serialize_wal_record_into(
                                &mut rec_buf,
                                &mut msg_buf,
                                txn,
                                commit_version,
                                wal_txn_id,
                                *txn.branch_id.as_bytes(),
                                timestamp,
                            );
                            wal_serialize_ns = ts.elapsed().as_nanos() as u64;
                            if let Some(err) = writer_halted_commit_error(wal) {
                                return Err(err);
                            }
                            let tw = Instant::now();
                            wal.append_pre_serialized(&rec_buf, wal_txn_id, timestamp)
                                .map_err(|e| CommitError::WALError(e.to_string()))?;
                            wal_io_ns = tw.elapsed().as_nanos() as u64;
                            Ok(())
                        })
                    });
                    result?;
                    tracing::debug!(target: "strata::txn", txn_id = txn.txn_id.as_u64(), commit_version = commit_version.as_u64(), "WAL durable");
                }
                WalMode::Shared(wal_arc) => {
                    let timestamp = now_micros();
                    let wal_txn_id = TxnId(commit_version.as_u64());
                    let result = WAL_RECORD_BUF.with(|rec_cell| {
                        MSGPACK_BUF.with(|msg_cell| {
                            let mut rec_buf = rec_cell.borrow_mut();
                            let mut msg_buf = msg_cell.borrow_mut();
                            let ts = Instant::now();
                            payload::serialize_wal_record_into(
                                &mut rec_buf,
                                &mut msg_buf,
                                txn,
                                commit_version,
                                wal_txn_id,
                                *txn.branch_id.as_bytes(),
                                timestamp,
                            );
                            wal_serialize_ns = ts.elapsed().as_nanos() as u64;
                            let tm = Instant::now();
                            let mut wal = wal_arc.lock();
                            wal_mutex_ns = tm.elapsed().as_nanos() as u64;
                            if let Some(err) = writer_halted_commit_error(&wal) {
                                return Err(err);
                            }
                            let tw = Instant::now();
                            wal.append_pre_serialized(&rec_buf, wal_txn_id, timestamp)
                                .map_err(|e| CommitError::WALError(e.to_string()))?;
                            wal_io_ns = tw.elapsed().as_nanos() as u64;
                            Ok(())
                        })
                    });
                    result?;
                    tracing::debug!(target: "strata::txn", txn_id = txn.txn_id.as_u64(), commit_version = commit_version.as_u64(), "WAL durable");
                }
                WalMode::None => {}
            }
        });

        #[cfg(feature = "fault-injection")]
        if let Some(reason) = maybe_take_apply_failure_injection() {
            if has_wal {
                tracing::error!(
                    target: "strata::txn",
                    txn_id = txn.txn_id.as_u64(),
                    commit_version = commit_version.as_u64(),
                    reason,
                    "Injected storage-apply failure after WAL commit"
                );
                return Err(CommitError::DurableButNotVisible {
                    txn_id: txn.txn_id.as_u64(),
                    commit_version: commit_version.as_u64(),
                    reason,
                });
            }

            return Err(CommitError::WALError(reason));
        }

        Ok(())
    })?;

    // Profile accumulation
    if profiling {
        let total_ns = t_total.elapsed().as_nanos() as u64;
        COMMIT_PROF.with(|p| {
            let mut p = p.borrow_mut();
            p.count += 1;
            p.wal_serialize_ns += wal_serialize_ns;
            p.wal_mutex_ns += wal_mutex_ns;
            p.wal_io_ns += wal_io_ns;
            p.total_ns += total_ns;

            if p.count % PROFILE_INTERVAL == 0 {
                let n = PROFILE_INTERVAL as f64;
                let wal_total = p.wal_serialize_ns + p.wal_mutex_ns + p.wal_io_ns;
                eprintln!(
                    "[commit-profile] {} commits | avg(us): total={:.1}  \
                     wal[ser={:.1} mutex={:.1} io={:.1} sum={:.1}]",
                    p.count,
                    p.total_ns as f64 / n / 1000.0,
                    p.wal_serialize_ns as f64 / n / 1000.0,
                    p.wal_mutex_ns as f64 / n / 1000.0,
                    p.wal_io_ns as f64 / n / 1000.0,
                    wal_total as f64 / n / 1000.0,
                );
                // Reset for next interval
                p.wal_serialize_ns = 0;
                p.wal_mutex_ns = 0;
                p.wal_io_ns = 0;
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
            wal_us = trace.wal_total_ns / 1000,
            keys_read = trace.keys_read,
            keys_written = trace.keys_written,
            "commit perf trace"
        );
    }

    Ok(commit_version)
}

/// Commit a transaction through the durability shell with an optional direct
/// WAL writer.
pub fn commit_with_wal<S: Storage>(
    manager: &StorageTransactionManager,
    txn: &mut TransactionContext,
    store: &S,
    wal: Option<&mut WalWriter>,
) -> std::result::Result<CommitVersion, CommitError> {
    let wal_mode = match wal {
        Some(w) => WalMode::Direct(w),
        None => WalMode::None,
    };
    commit_inner(manager, txn, store, wal_mode, None)
}

/// Commit a transaction through the durability shell with an optional shared
/// WAL writer.
pub fn commit_with_wal_arc<S: Storage>(
    manager: &StorageTransactionManager,
    txn: &mut TransactionContext,
    store: &S,
    wal_arc: Option<&Arc<Mutex<WalWriter>>>,
) -> std::result::Result<CommitVersion, CommitError> {
    let wal_mode = match wal_arc {
        Some(arc) => WalMode::Shared(arc),
        None => WalMode::None,
    };
    commit_inner(manager, txn, store, wal_mode, None)
}

/// Commit a transaction through the durability shell using an externally
/// allocated version.
pub fn commit_with_version<S: Storage>(
    manager: &StorageTransactionManager,
    txn: &mut TransactionContext,
    store: &S,
    wal: Option<&mut WalWriter>,
    version: CommitVersion,
) -> std::result::Result<CommitVersion, CommitError> {
    let wal_mode = match wal {
        Some(w) => WalMode::Direct(w),
        None => WalMode::None,
    };
    commit_inner(manager, txn, store, wal_mode, Some(version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::codec::IdentityCodec;
    use crate::durability::wal::{DurabilityMode, WalConfig, WalReader};
    use crate::durability::TransactionPayload;
    use crate::{Key, Namespace, SegmentedStore};
    use std::io;
    use strata_core::value::Value;
    use strata_core::BranchId;
    use tempfile::tempdir;

    fn test_key(branch_id: BranchId, name: &str) -> Key {
        Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), name)
    }

    fn test_writer(wal_dir: &std::path::Path) -> WalWriter {
        WalWriter::new(
            wal_dir.to_path_buf(),
            [7u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            Box::new(IdentityCodec),
        )
        .unwrap()
    }

    fn read_single_payload(wal_dir: &std::path::Path) -> TransactionPayload {
        let reader = WalReader::new();
        let result = reader.read_all(wal_dir).unwrap();
        assert_eq!(result.records.len(), 1);
        TransactionPayload::from_bytes(&result.records[0].writeset).unwrap()
    }

    #[test]
    fn commit_with_wal_direct_is_storage_owned_and_replayable() {
        let store = Arc::new(SegmentedStore::new());
        let manager = StorageTransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let key = test_key(branch_id, "direct");
        let tmp = tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let mut writer = test_writer(&wal_dir);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion::ZERO);
        txn.put(key.clone(), Value::Int(11)).unwrap();

        let version =
            commit_with_wal(&manager, &mut txn, store.as_ref(), Some(&mut writer)).unwrap();
        writer.flush().unwrap();

        let stored = store
            .get_versioned(&key, version)
            .unwrap()
            .expect("stored value missing");
        assert_eq!(stored.value, Value::Int(11));

        let payload = read_single_payload(&wal_dir);
        assert_eq!(payload.version, version.as_u64());
        assert_eq!(payload.puts, vec![(key, Value::Int(11))]);
        assert!(payload.deletes.is_empty());
    }

    #[test]
    fn commit_with_wal_arc_shared_writer_is_storage_owned_and_replayable() {
        let store = Arc::new(SegmentedStore::new());
        let manager = StorageTransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let key = test_key(branch_id, "shared");
        let tmp = tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let writer = Arc::new(Mutex::new(test_writer(&wal_dir)));
        let mut txn = TransactionContext::new(TxnId(2), branch_id, CommitVersion::ZERO);
        txn.put(key.clone(), Value::Int(22)).unwrap();

        let version =
            commit_with_wal_arc(&manager, &mut txn, store.as_ref(), Some(&writer)).unwrap();
        writer.lock().flush().unwrap();

        let stored = store
            .get_versioned(&key, version)
            .unwrap()
            .expect("stored value missing");
        assert_eq!(stored.value, Value::Int(22));

        let payload = read_single_payload(&wal_dir);
        assert_eq!(payload.version, version.as_u64());
        assert_eq!(payload.puts, vec![(key, Value::Int(22))]);
    }

    #[test]
    fn commit_with_version_preserves_external_version_in_storage_and_wal() {
        let store = Arc::new(SegmentedStore::new());
        let manager = StorageTransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let key = test_key(branch_id, "external");
        let tmp = tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let mut writer = test_writer(&wal_dir);
        let mut txn = TransactionContext::new(TxnId(3), branch_id, CommitVersion::ZERO);
        txn.put(key.clone(), Value::Int(33)).unwrap();
        let forced = CommitVersion(42);

        let version = commit_with_version(
            &manager,
            &mut txn,
            store.as_ref(),
            Some(&mut writer),
            forced,
        )
        .unwrap();
        writer.flush().unwrap();

        assert_eq!(version, forced);
        assert_eq!(manager.current_version(), forced);
        assert_eq!(manager.visible_version(), forced);

        let stored = store
            .get_versioned(&key, forced)
            .unwrap()
            .expect("stored value missing");
        assert_eq!(stored.value, Value::Int(33));

        let reader = WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].txn_id, TxnId(42));
        let payload = TransactionPayload::from_bytes(&result.records[0].writeset).unwrap();
        assert_eq!(payload.version, 42);
    }

    #[test]
    fn commit_with_wal_reports_writer_halted_without_applying_storage() {
        let store = Arc::new(SegmentedStore::new());
        let manager = StorageTransactionManager::new(CommitVersion::ZERO);
        let branch_id = BranchId::new();
        let key = test_key(branch_id, "halted");
        let tmp = tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let mut writer = test_writer(&wal_dir);
        writer.record_sync_failure(io::Error::other("disk full"));
        let mut txn = TransactionContext::new(TxnId(4), branch_id, CommitVersion::ZERO);
        txn.put(key.clone(), Value::Int(44)).unwrap();

        let err =
            commit_with_wal(&manager, &mut txn, store.as_ref(), Some(&mut writer)).unwrap_err();
        assert!(matches!(err, CommitError::WriterHalted { .. }));
        assert!(store
            .get_versioned(&key, CommitVersion(1))
            .unwrap()
            .is_none());
    }
}
