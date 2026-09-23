//! TCP4.12b — elle-style concurrent isolation histories (the #2682 class).
//!
//! 4.12a checked single-session lineage/temporal histories offline. This lane
//! adds the concurrent half at the only layer with concurrent sessions on one
//! runtime (the storage API — the engine is single-session by construction,
//! see `engine/tests/branch_faults.rs`). It targets **read atomicity**: the
//! fractured/torn-read anomaly #2682 was investigated (loom, 4.3a–c) and closed
//! as verified-sound, so this lane must run CLEAN on the live path and prove its
//! oracle's power with sabotage twins.
//!
//! Traceability co-design: a set of `LINKED_KEYS` keys is written only ever
//! *together*, in one atomic batch, all to the same globally-unique stamp (a
//! monotonic `u64` batch number). So at any consistent snapshot every linked key
//! holds the SAME stamp, and a single-snapshot multi-key read (`scan_prefix`)
//! that observes two different stamps — or a partial linked set — witnessed a
//! torn batch. Stamps are also recorded per committer, so an observed stamp that
//! was never committed is a phantom-value anomaly.
//!
//! Offline checking mirrors 4.12a: readers record observations, writers record
//! committed stamps, and [`check_concurrent_history`] judges the merged history
//! after the run — enabling the sabotage twins. The live run is non
//! deterministic (interleaving is the fuzzed dimension, as in `stress_random`),
//! but the read-atomicity invariant is interleaving-independent.
//!
//! Full Adya SSG cycle inference (write/read/anti-dependency graph) is further
//! headroom; read-atomicity + phantom-value are the #2682-relevant subset.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::*;
use crate::testkit::{leak_static, ProgressTicker, ProgressWatchdog};

/// Keys written only ever together, atomically, all to the same stamp.
const LINKED_KEYS: u8 = 4;
const STAMP_LEN: usize = 8;
const DEFAULT_WRITERS: usize = 3;
const DEFAULT_READERS: usize = 3;
const DEFAULT_OPS: usize = 200;
const DEFAULT_SEED: u64 = 0x11c0_ffee_5eed_0b12;
const MAX_BACKPRESSURE_RETRIES: usize = 200;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn linked_prefix() -> StorageKey {
    key(b"lh/")
}

fn linked_key(index: u8) -> StorageKey {
    key(format!("lh/{index}").as_bytes())
}

/// One reader's single-snapshot observation of the linked-key set: the stamp
/// found on each present linked key (`(key_index, stamp)`), decoded from the
/// scanned row values.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ScanObservation {
    reader: usize,
    index: usize,
    linked: Vec<(u8, u64)>,
}

/// Read-atomicity anomalies the offline checker can report.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ConcurrentAnomaly {
    /// A single-snapshot multi-key read saw the linked set in an inconsistent
    /// state — different stamps across keys, or a partial linked set. The
    /// #2682 torn/fractured-read shape.
    FracturedRead {
        reader: usize,
        index: usize,
        observed: Vec<(u8, u64)>,
    },
    /// A read observed a stamp no committer ever wrote — a value conjured from
    /// a torn or fabricated snapshot.
    PhantomValue {
        reader: usize,
        index: usize,
        stamp: u64,
    },
}

/// Non-vacuity counters for one checked concurrent history.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CheckStats {
    scans: usize,
    non_empty_scans: usize,
}

/// Judge a merged concurrent history offline: every single-snapshot linked read
/// must be atomic (all `LINKED_KEYS` present and equal, or all absent), and
/// every observed stamp must have been committed. Linear in the history length.
fn check_concurrent_history(
    observations: &[ScanObservation],
    committed: &HashSet<u64>,
) -> Result<CheckStats, ConcurrentAnomaly> {
    let mut stats = CheckStats::default();
    for observation in observations {
        stats.scans += 1;
        if observation.linked.is_empty() {
            continue; // linked set not yet created — a consistent empty snapshot
        }
        stats.non_empty_scans += 1;

        let all_present = observation.linked.len() == LINKED_KEYS as usize;
        let first = observation.linked[0].1;
        let all_equal = observation.linked.iter().all(|(_, stamp)| *stamp == first);
        if !all_present || !all_equal {
            return Err(ConcurrentAnomaly::FracturedRead {
                reader: observation.reader,
                index: observation.index,
                observed: observation.linked.clone(),
            });
        }
        if !committed.contains(&first) {
            return Err(ConcurrentAnomaly::PhantomValue {
                reader: observation.reader,
                index: observation.index,
                stamp: first,
            });
        }
    }
    Ok(stats)
}

// --- live harness -------------------------------------------------------

fn commit_linked_batch(
    runtime: &StorageRuntime<'static>,
    stamp: u64,
    context: &str,
) -> CommitSummary {
    let value = stamp.to_be_bytes().to_vec();
    let mutations: Vec<CommitMutation> = (0..LINKED_KEYS)
        .map(|index| CommitMutation::Put {
            storage_space: background_space(),
            key: linked_key(index),
            value: StorageValue::new(value.clone()),
            ttl: None,
        })
        .collect();
    let mut attempts = 0;
    loop {
        let batch = CommitBatch::new(
            StorageRuntime::default_branch_id_for_test(),
            mutations.clone(),
            CommitOptions::default(),
        )
        .expect("valid linked batch");
        match runtime.commit(&batch) {
            Ok(summary) => return summary,
            Err(error) => {
                // Only documented backpressure retries; any other refusal is an
                // unexpected-error finding (gate 8(a)).
                assert!(
                    matches!(error.class(), StorageApiErrorClass::ResourceExhausted),
                    "unexpected error class {:?} ({}) at {context}: {error}",
                    error.class(),
                    error.code()
                );
                attempts += 1;
                assert!(
                    attempts <= MAX_BACKPRESSURE_RETRIES,
                    "backpressure did not clear after {MAX_BACKPRESSURE_RETRIES} retries at {context}"
                );
                std::thread::yield_now();
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
    }
}

/// One writer: commit `ops` atomic linked batches, each stamped with a globally
/// unique monotonic batch number; return the stamps it committed.
fn run_writer(
    runtime: &StorageRuntime<'static>,
    writer: usize,
    ops: usize,
    counter: &AtomicU64,
    first_commit: &AtomicBool,
    ticker: &ProgressTicker,
) -> Vec<u64> {
    let mut committed = Vec::with_capacity(ops);
    for index in 0..ops {
        let stamp = counter.fetch_add(1, Ordering::Relaxed);
        commit_linked_batch(runtime, stamp, &format!("writer {writer}"));
        committed.push(stamp);
        if index == 0 {
            // #3009: a linked batch is now committed and visible. Release the
            // gate so readers can begin scanning a populated set; the Release
            // pairs with each reader's Acquire load, so the committed rows are
            // visible to the first scan that passes the gate.
            first_commit.store(true, Ordering::Release);
        }
        ticker.tick();
    }
    committed
}

/// One reader: take `ops` single-snapshot reads of the linked set and record
/// each observation.
fn run_reader(
    runtime: &StorageRuntime<'static>,
    reader: usize,
    ops: usize,
    first_commit: &AtomicBool,
    ticker: &ProgressTicker,
) -> Vec<ScanObservation> {
    let branch = StorageRuntime::default_branch_id_for_test();
    // #3009: begin scanning only after the first linked batch is committed. A
    // committed batch never empties the set, so every scan from here observes a
    // populated snapshot and the read-atomicity oracle judges it — the concurrent
    // scans are non-vacuous deterministically rather than by luck of the
    // interleaving. Readers still race writers 2..N: the gate opens on the FIRST
    // commit, not the last. The watchdog ticker bounds the wait.
    while !first_commit.load(Ordering::Acquire) {
        std::thread::yield_now();
        ticker.tick();
    }
    let mut observations = Vec::with_capacity(ops);
    for index in 0..ops {
        let outcome = runtime
            .scan_prefix(&PrefixScanReadRequest::new(
                branch,
                background_space(),
                linked_prefix(),
                ReadBound::Latest,
                None,
            ))
            .unwrap_or_else(|error| panic!("reader {reader} scan failed: {error}"));
        let mut linked: Vec<(u8, u64)> = outcome
            .rows()
            .iter()
            .filter(|row| !row.is_tombstone())
            .filter_map(|row| {
                let suffix = row.key().as_bytes().strip_prefix(b"lh/")?;
                let key_index: u8 = std::str::from_utf8(suffix).ok()?.parse().ok()?;
                let bytes = row.value()?.as_bytes();
                let stamp = u64::from_be_bytes(<[u8; STAMP_LEN]>::try_from(bytes).ok()?);
                Some((key_index, stamp))
            })
            .collect();
        linked.sort_unstable();
        observations.push(ScanObservation {
            reader,
            index,
            linked,
        });
        ticker.tick();
    }
    observations
}

/// Run a concurrent writers×readers workload over one shared durable runtime
/// and return the merged history (all reader observations, the union of
/// committed stamps).
fn run_concurrent_history(
    root: std::path::PathBuf,
    writers: usize,
    readers: usize,
    ops: usize,
) -> (Vec<ScanObservation>, HashSet<u64>) {
    let backend = leak_static(StorageBackend::local_fs(root));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_growth_policy(StorageWalGrowthPolicy::thresholds(32 * 1024, 4, 30))
            .with_wal_segment_size_for_test(16 * 1024),
        backend,
    )
    .expect("open shared concurrent-history runtime")
    .into_runtime();

    let counter = AtomicU64::new(0);
    // #3009: opened by the first writer to commit a batch; readers wait on it so
    // their concurrent scans are guaranteed to observe a populated linked set.
    let first_commit = AtomicBool::new(false);
    let watchdog = ProgressWatchdog::arm(
        "concurrent_history",
        std::time::Duration::from_secs(120),
        || "concurrent-history workload stalled".to_owned(),
    );
    let ticker = watchdog.ticker();

    let mut all_observations: Vec<ScanObservation> = Vec::new();
    let mut committed: HashSet<u64> = HashSet::new();
    std::thread::scope(|scope| {
        let mut writer_handles = Vec::new();
        let mut reader_handles = Vec::new();
        for writer in 0..writers {
            let runtime = &runtime;
            let counter = &counter;
            let first_commit = &first_commit;
            let ticker = &ticker;
            writer_handles
                .push(scope.spawn(move || {
                    run_writer(runtime, writer, ops, counter, first_commit, ticker)
                }));
        }
        for reader in 0..readers {
            let runtime = &runtime;
            let first_commit = &first_commit;
            let ticker = &ticker;
            reader_handles
                .push(scope.spawn(move || run_reader(runtime, reader, ops, first_commit, ticker)));
        }
        for handle in writer_handles {
            committed.extend(handle.join().expect("writer joined"));
        }
        for handle in reader_handles {
            all_observations.extend(handle.join().expect("reader joined"));
        }
    });

    // #3009: no post-join crutch read. #3002 added one final observation after
    // every writer joined, because a concurrent scan could legitimately land
    // before the first commit and the whole run then judged nothing concurrent.
    // The write-then-signal gate removes that possibility at the source — every
    // recorded observation is a concurrent scan taken after the first commit, so
    // the merged history is non-vacuous by construction, not by a sequential
    // read appended afterwards.

    runtime.wait_background_idle_for_test();
    let status = runtime.maintenance_status().expect("maintenance status");
    assert!(
        status.recent_failures().is_empty(),
        "background maintenance recorded unexpected failures: {:?}",
        status.recent_failures()
    );
    (all_observations, committed)
}

#[cfg(feature = "localfs")]
#[test]
fn concurrent_linked_reads_are_atomic_on_one_shared_database() {
    let writers = env_usize("STRATA_CONCURRENT_HISTORY_WRITERS", DEFAULT_WRITERS);
    let readers = env_usize("STRATA_CONCURRENT_HISTORY_READERS", DEFAULT_READERS);
    let ops = env_usize("STRATA_CONCURRENT_HISTORY_OPS", DEFAULT_OPS);
    let _seed = env_u64("STRATA_CONCURRENT_HISTORY_SEED", DEFAULT_SEED);

    let root = temp_dir_for_api_test("concurrent-history");
    let (observations, committed) = run_concurrent_history(root, writers, readers, ops);

    let stats = check_concurrent_history(&observations, &committed).unwrap_or_else(|anomaly| {
        panic!("concurrent history anomaly: {anomaly:?}");
    });
    // #3009: non-vacuity is now DETERMINISTIC, not hoped-for. Every recorded scan
    // is a concurrent reader scan (no post-join crutch), so `scans` is exactly
    // `readers * ops`; and the signal gate means every one was taken after the
    // first commit, so every one observed a populated, atomic snapshot the oracle
    // judged. If the gate regressed, early pre-commit scans would make
    // `non_empty_scans < scans` and this reds instead of passing vacuously.
    assert_eq!(
        stats.scans,
        readers.saturating_mul(ops),
        "every recorded scan must be a concurrent reader scan (no post-join crutch)"
    );
    assert_eq!(
        stats.non_empty_scans, stats.scans,
        "the write-then-signal gate must make every concurrent scan non-vacuous"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(reader: usize, index: usize, linked: &[(u8, u64)]) -> ScanObservation {
        ScanObservation {
            reader,
            index,
            linked: linked.to_vec(),
        }
    }

    #[test]
    fn checker_accepts_atomic_reads() {
        let committed: HashSet<u64> = [7, 8, 9].into_iter().collect();
        let observations = vec![
            obs(0, 0, &[]), // empty snapshot, consistent
            obs(0, 1, &[(0, 8), (1, 8), (2, 8), (3, 8)]),
            obs(1, 0, &[(0, 9), (1, 9), (2, 9), (3, 9)]),
        ];
        let stats = check_concurrent_history(&observations, &committed).expect("clean");
        assert_eq!(stats.scans, 3);
        assert_eq!(stats.non_empty_scans, 2);
    }

    #[test]
    fn checker_catches_a_fractured_read_unequal_stamps() {
        // The #2682 shape: a single-snapshot read sees two keys at the new
        // batch and two still at the old — a torn batch.
        let committed: HashSet<u64> = [4, 5].into_iter().collect();
        let observations = vec![obs(2, 3, &[(0, 5), (1, 5), (2, 4), (3, 4)])];
        match check_concurrent_history(&observations, &committed)
            .expect_err("torn batch is an anomaly")
        {
            ConcurrentAnomaly::FracturedRead { reader, index, .. } => {
                assert_eq!((reader, index), (2, 3));
            }
            other @ ConcurrentAnomaly::PhantomValue { .. } => {
                panic!("expected FracturedRead, got {other:?}")
            }
        }
    }

    #[test]
    fn checker_catches_a_partial_linked_set() {
        // A read that sees some linked keys present and others absent is also
        // fractured (the atomic batch writes all of them together).
        let committed: HashSet<u64> = [5].into_iter().collect();
        let observations = vec![obs(0, 0, &[(0, 5), (1, 5)])]; // only 2 of 4
        assert!(matches!(
            check_concurrent_history(&observations, &committed),
            Err(ConcurrentAnomaly::FracturedRead { .. })
        ));
    }

    #[test]
    fn checker_catches_a_phantom_value() {
        // All keys agree, but on a stamp no writer committed.
        let committed: HashSet<u64> = [1, 2].into_iter().collect();
        let observations = vec![obs(1, 9, &[(0, 3), (1, 3), (2, 3), (3, 3)])];
        match check_concurrent_history(&observations, &committed)
            .expect_err("phantom stamp is an anomaly")
        {
            ConcurrentAnomaly::PhantomValue { stamp, .. } => assert_eq!(stamp, 3),
            other @ ConcurrentAnomaly::FracturedRead { .. } => {
                panic!("expected PhantomValue, got {other:?}")
            }
        }
    }

    #[cfg(feature = "localfs")]
    #[test]
    #[ignore = "nightly soak; scale via STRATA_CONCURRENT_HISTORY_{WRITERS,READERS,OPS}"]
    fn concurrent_history_soak() {
        let writers = env_usize("STRATA_CONCURRENT_HISTORY_WRITERS", 6);
        let readers = env_usize("STRATA_CONCURRENT_HISTORY_READERS", 6);
        let ops = env_usize("STRATA_CONCURRENT_HISTORY_OPS", 2000);
        let root = temp_dir_for_api_test("concurrent-history-soak");
        let (observations, committed) = run_concurrent_history(root, writers, readers, ops);
        let stats =
            check_concurrent_history(&observations, &committed).expect("soak history is clean");
        // #3009: deterministic non-vacuity — every concurrent scan populated.
        assert_eq!(stats.scans, readers.saturating_mul(ops));
        assert_eq!(stats.non_empty_scans, stats.scans);
    }
}
