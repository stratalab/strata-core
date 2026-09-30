//! BS2.4b — S3 concurrency-stress gate: the milestone's assurance-level test.
//!
//! N reader threads share one runtime with a committing writer (enabled by BS2.4b relaxing
//! `commit`/`branch` to `&self`) and must observe an atomic, monotonic, off-lock view: a committed
//! batch is all-or-none, the visible version never regresses per reader, and the writer reads its
//! own writes. Two complementary tests:
//! - `off_lock_readers_race_a_committing_writer` — **cache** mode (immediate visibility, no
//!   durability backpressure) stresses the protocol core — commit atomicity + the visible bound —
//!   at high rate.
//! - `off_lock_fork_and_materialize_under_concurrent_reads` — **`DurableLocal` + Background** (real
//!   worker threads) exercises C4 branching under load: fork mid-stress (O(1), correct inherited
//!   reads), parent/child concurrent reads, and a materialization (inherited→owned structural
//!   install) that must never tear a concurrent child read.

#![cfg(feature = "localfs")]

use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Barrier;
use std::time::{Duration, Instant};

const STRESS_KEYS: usize = 8;
const STRESS_READERS: usize = 3;
const STRESS_BATCHES: u64 = 400;

/// A `StorageRuntime` must be `Send + Sync` so a shared handle can be driven by reader + writer
/// threads concurrently (the enabling property of BS2.4b's `&self` commit/branch).
#[test]
fn storage_runtime_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StorageRuntime<'static>>();
}

/// Cache mode: immediate visibility (clean read-your-writes) and no durability backpressure, so the
/// main read/write race stresses the off-lock protocol's core (commit atomicity + the visible bound)
/// at high rate. Structural-change coverage (fork/materialization under load) is the durable C4 test.
fn open_cache() -> StorageRuntime<'static> {
    StorageRuntime::open_ephemeral()
        .expect("open ephemeral cache runtime")
        .into_runtime()
}

fn open_durable_background(name: &str) -> StorageRuntime<'static> {
    let root = temp_dir_for_api_test(name);
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root));
    StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            // Default (large) budget so commits are not throttled by budget backpressure; the small
            // WAL growth thresholds still force frequent flush/compaction under the write load.
            .with_wal_growth_policy(StorageWalGrowthPolicy::thresholds(8 * 1024, 2, 3))
            .with_wal_segment_size_for_test(2048)
            .with_background_max_tasks_per_wake(64)
            .with_background_max_runtime_per_wake(Duration::from_millis(250)),
        backend,
    )
    .expect("open durable-local background runtime")
    .into_runtime()
}

/// One batch that overwrites the whole key set: key `stress-{i}` -> value `batch_no || i`, all under
/// a single commit version, so a reader observes the batch all-or-none.
fn stress_batch(branch: BranchId, batch_no: u64, num_keys: usize) -> CommitBatch {
    let mut mutations = Vec::with_capacity(num_keys);
    for i in 0..num_keys {
        let mut value = batch_no.to_be_bytes().to_vec();
        value.extend_from_slice(&(i as u64).to_be_bytes());
        mutations.push(CommitMutation::Put {
            storage_space: background_space(),
            key: key(format!("stress-{i:04}").as_bytes()),
            value: StorageValue::new(value),
            ttl: None,
        });
    }
    CommitBatch::new(
        branch,
        mutations,
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid stress batch")
}

/// Latest-scan the key set and enforce atomicity + completeness. Returns the batch number every row
/// agrees on, or `None` before the first full batch is visible on this branch. Panics on a torn read
/// (mixed batch numbers, or a partial key set) — the core S3 invariant.
fn read_checked(runtime: &StorageRuntime<'_>, branch: BranchId, num_keys: usize) -> Option<u64> {
    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch,
            background_space(),
            key(b"stress-"),
            ReadBound::Latest,
            None,
        ))
        .expect("off-lock latest prefix scan");
    let rows = scan.rows();
    if rows.len() != num_keys {
        // A committed batch writes every key under one version, so a Latest scan sees either 0 rows
        // (before the first batch) or the full set — a partial set is itself a torn read.
        assert!(
            rows.is_empty(),
            "torn read: {} of {num_keys} keys visible",
            rows.len()
        );
        return None;
    }
    let mut batch_no = None;
    let mut indices = Vec::with_capacity(num_keys);
    for row in rows {
        let value = row.value().expect("stress rows are puts").as_bytes();
        assert_eq!(value.len(), 16, "unexpected stress value width");
        let observed = u64::from_be_bytes(value[0..8].try_into().expect("8 bytes"));
        let index = u64::from_be_bytes(value[8..16].try_into().expect("8 bytes"));
        match batch_no {
            None => batch_no = Some(observed),
            Some(seen) => assert_eq!(observed, seen, "torn read: mixed batch numbers in one scan"),
        }
        indices.push(index);
    }
    indices.sort_unstable();
    assert_eq!(
        indices,
        (0..num_keys as u64).collect::<Vec<_>>(),
        "incomplete key set in one scan"
    );
    batch_no
}

fn reader_loop(runtime: &StorageRuntime<'_>, branch: BranchId, done: &AtomicBool, start: &Barrier) {
    start.wait();
    let mut max_batch = 0u64;
    let mut point_max = 0u64;
    let mut reads = 0u64;
    while !done.load(Ordering::Acquire) {
        if let Some(observed) = read_checked(runtime, branch, STRESS_KEYS) {
            assert!(
                observed >= max_batch,
                "visible version regressed for a reader (scan): {observed} < {max_batch}"
            );
            max_batch = observed;
            reads += 1;
        }
        // BS2.5: also race the UNPINNED point path (`read_point`) against the committing writer —
        // its version must never regress. This is the direct concurrency gate for the pin removal.
        if let Some(point) = read_point_batch(runtime, branch) {
            assert!(
                point >= point_max,
                "visible version regressed for a reader (unpinned point): {point} < {point_max}"
            );
            point_max = point;
        }
        // Cooperate so the (durable) writer and the maintenance workers make progress on a busy box
        // instead of being starved by a hot read loop.
        std::thread::yield_now();
    }
    assert!(reads > 0, "reader observed no complete batches");
}

/// Cheap point read of the first key — its batch number (`None` before any batch is visible).
fn read_point_batch(runtime: &StorageRuntime<'_>, branch: BranchId) -> Option<u64> {
    let outcome = runtime
        .read_point(&PointReadRequest::new(
            branch,
            background_space(),
            key(b"stress-0000"),
            ReadBound::Latest,
        ))
        .expect("off-lock latest point read");
    let row = outcome.row()?;
    let value = row.value().expect("stress rows are puts").as_bytes();
    Some(u64::from_be_bytes(value[0..8].try_into().expect("8 bytes")))
}

/// Sets `done` when dropped — including when the writer panics. The readers
/// spin until `done`, so a writer that dies without it would hang the whole
/// scope (the run then times out instead of failing on the writer's panic).
struct ReleaseReaders<'a>(&'a AtomicBool);

impl Drop for ReleaseReaders<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn writer_loop(runtime: &StorageRuntime<'_>, branch: BranchId, done: &AtomicBool, start: &Barrier) {
    let _release_readers = ReleaseReaders(done);
    start.wait();
    for batch_no in 1..STRESS_BATCHES {
        runtime
            .commit(&stress_batch(branch, batch_no, STRESS_KEYS))
            .expect("commit stress batch");
        // Read-your-writes: after the ack, the writer must see at least its own batch (cheap point
        // read so the writer isn't itself bottlenecked scanning a growing LSM every iteration).
        let seen = read_point_batch(runtime, branch).expect("writer read-your-writes");
        assert!(
            seen >= batch_no,
            "read-your-writes violated: saw {seen} after committing {batch_no}"
        );
    }
    done.store(true, Ordering::Release);
}

#[test]
fn off_lock_readers_race_a_committing_writer() {
    let mut runtime = open_cache();
    let branch = StorageRuntime::default_branch_id_for_test();
    // Seed batch 0 so every reader always observes a complete key set.
    runtime
        .commit(&stress_batch(branch, 0, STRESS_KEYS))
        .expect("seed batch 0");

    let done = AtomicBool::new(false);
    let start = Barrier::new(STRESS_READERS + 1);

    std::thread::scope(|scope| {
        for _ in 0..STRESS_READERS {
            scope.spawn(|| reader_loop(&runtime, branch, &done, &start));
        }
        scope.spawn(|| writer_loop(&runtime, branch, &done, &start));
    });

    // Every reader saw only atomic, monotonic batches; the final visible state is the last commit.
    let final_batch = read_checked(&runtime, branch, STRESS_KEYS).expect("final read");
    assert_eq!(
        final_batch,
        STRESS_BATCHES - 1,
        "final visible batch mismatch"
    );
    runtime.close().expect("close cache runtime");
}

// ---- BS5.0 — multi-writer correctness (the write-concurrency milestone's S3 gate) ----

const MULTI_WRITERS: usize = 4;
const MULTI_KEYS: usize = 4;

/// One batch overwriting writer `w`'s own fixed key set (`mw{w}-{i}` → `batch_no || i`), all under
/// one commit version — readers must observe each writer's set all-or-none.
fn multi_writer_batch(branch: BranchId, writer: usize, batch_no: u64) -> CommitBatch {
    let mut mutations = Vec::with_capacity(MULTI_KEYS);
    for i in 0..MULTI_KEYS {
        let mut value = batch_no.to_be_bytes().to_vec();
        value.extend_from_slice(&(i as u64).to_be_bytes());
        mutations.push(CommitMutation::Put {
            storage_space: background_space(),
            key: key(format!("mw{writer}-{i:04}").as_bytes()),
            value: StorageValue::new(value),
            ttl: None,
        });
    }
    CommitBatch::new(
        branch,
        mutations,
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid multi-writer batch")
}

/// Latest-scan one writer's key set; enforce atomicity (homogeneous batch number, complete set).
/// `None` before the writer's first batch is visible.
fn multi_read_checked(
    runtime: &StorageRuntime<'_>,
    branch: BranchId,
    writer: usize,
) -> Option<u64> {
    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch,
            background_space(),
            key(format!("mw{writer}-").as_bytes()),
            ReadBound::Latest,
            None,
        ))
        .expect("off-lock latest prefix scan");
    let rows = scan.rows();
    if rows.len() != MULTI_KEYS {
        assert!(
            rows.is_empty(),
            "torn read: {} of {MULTI_KEYS} keys visible for writer {writer}",
            rows.len()
        );
        return None;
    }
    let mut batch_no = None;
    for row in rows {
        let value = row.value().expect("multi-writer rows are puts").as_bytes();
        let observed = u64::from_be_bytes(value[0..8].try_into().expect("8 bytes"));
        match batch_no {
            None => batch_no = Some(observed),
            Some(seen) => assert_eq!(
                observed, seen,
                "torn read: mixed batch numbers for writer {writer}"
            ),
        }
    }
    batch_no
}

/// N writers share one `&runtime` on ONE branch (distinct key spaces, no conflict overlap) while
/// checker threads verify atomicity and monotonicity per writer. The invariants BS5's write groups
/// must preserve, pinned before any protocol change:
/// - each writer's acked commit versions are strictly monotonic;
/// - acked versions are globally unique across writers (the allocator's monotonic guarantee);
/// - read-your-writes after every ack;
/// - readers observe each writer's batches atomically and never see a batch number regress;
/// - after the drain, every writer's final batch is the visible state of its key set.
fn run_multi_writer_stress(runtime: &StorageRuntime<'static>, branch: BranchId, batches: u64) {
    run_multi_writer_stress_on(runtime, &[branch; MULTI_WRITERS], batches);
}

/// The multi-writer stress with a branch PER WRITER slot: `branches[w]` is
/// writer `w`'s target. Distinct branches drive the BS5.4 deferred-apply
/// group path (each first-of-branch member's apply runs on its own parked
/// thread); a shared branch drives the eager path. Invariants are identical.
fn run_multi_writer_stress_on(
    runtime: &StorageRuntime<'static>,
    branches: &[BranchId; MULTI_WRITERS],
    batches: u64,
) {
    // Seed batch 0 for every writer so checkers always see complete sets.
    for (writer, branch) in branches.iter().enumerate() {
        runtime
            .commit(&multi_writer_batch(*branch, writer, 0))
            .expect("seed writer batch 0");
    }

    let done = AtomicBool::new(false);
    let start = Barrier::new(MULTI_WRITERS + 2);
    let mut acked_versions: Vec<Vec<u64>> = Vec::new();

    std::thread::scope(|scope| {
        let mut writer_handles = Vec::with_capacity(MULTI_WRITERS);
        for (writer, &branch) in branches.iter().enumerate() {
            let (runtime, start) = (&runtime, &start);
            writer_handles.push(scope.spawn(move || {
                start.wait();
                let mut versions =
                    Vec::with_capacity(usize::try_from(batches).expect("small batch count"));
                let mut last_version = 0u64;
                for batch_no in 1..=batches {
                    let summary = runtime
                        .commit(&multi_writer_batch(branch, writer, batch_no))
                        .expect("multi-writer commit");
                    let version = summary.commit_version().as_u64();
                    assert!(
                        version > last_version,
                        "writer {writer}: acked version regressed ({version} <= {last_version})"
                    );
                    last_version = version;
                    versions.push(version);
                    // Read-your-writes after the ack.
                    let seen = multi_read_checked(runtime, branch, writer)
                        .expect("writer sees its own key set");
                    assert!(
                        seen >= batch_no,
                        "writer {writer}: read-your-writes violated ({seen} < {batch_no}, \
                         version {version})"
                    );
                }
                versions
            }));
        }
        // Two checkers race the writers, each verifying every writer's set stays atomic and
        // its observed batch number never regresses.
        for _ in 0..2 {
            let (runtime, done, start) = (&runtime, &done, &start);
            scope.spawn(move || {
                start.wait();
                let mut max_seen = [0u64; MULTI_WRITERS];
                while !done.load(Ordering::Acquire) {
                    for (writer, max_batch) in max_seen.iter_mut().enumerate() {
                        if let Some(observed) =
                            multi_read_checked(runtime, branches[writer], writer)
                        {
                            assert!(
                                observed >= *max_batch,
                                "checker: writer {writer} batch regressed ({observed} < {max_batch})"
                            );
                            *max_batch = observed;
                        }
                    }
                    std::thread::yield_now();
                }
            });
        }
        // Collect join results BEFORE unwrapping: a panicked writer must still release the
        // checkers (via `done`) or the scope would hang waiting on their spin loops and mask
        // the real failure.
        let joined: Vec<_> = writer_handles
            .into_iter()
            .map(std::thread::ScopedJoinHandle::join)
            .collect();
        done.store(true, Ordering::Release);
        for result in joined {
            acked_versions.push(result.expect("writer thread"));
        }
    });

    // Global uniqueness across writers: the allocator never hands two commits the same version.
    let mut all_versions: Vec<u64> = acked_versions.iter().flatten().copied().collect();
    all_versions.sort_unstable();
    let before = all_versions.len();
    all_versions.dedup();
    assert_eq!(
        before,
        all_versions.len(),
        "duplicate acked commit versions"
    );

    // Every writer's final batch is the visible state of its key set.
    for (writer, branch) in branches.iter().enumerate() {
        assert_eq!(
            multi_read_checked(runtime, *branch, writer),
            Some(batches),
            "writer {writer}: final visible batch mismatch"
        );
    }
}

/// Cache mode: the protocol core (atomic batches, monotonic unique versions, read-your-writes)
/// under 4 concurrent writers at high rate.
#[test]
fn off_lock_concurrent_writers_share_one_cache_runtime() {
    let mut runtime = open_cache();
    let branch = StorageRuntime::default_branch_id_for_test();
    run_multi_writer_stress(&runtime, branch, 60);
    runtime.close().expect("close cache runtime");
}

/// Durable mode with real background workers: the same invariants while WAL appends, flushes, and
/// compactions run underneath — the shape BS5.1's write groups must keep intact.
#[test]
fn off_lock_concurrent_writers_share_one_durable_runtime() {
    let mut runtime = open_durable_background("off-lock-multi-writer");
    let branch = StorageRuntime::default_branch_id_for_test();
    // Fewer batches than the cache variant: this runtime config checkpoints every ~3
    // commits and rotates 2 KB WAL segments, so per-commit cost is deliberately heavy.
    run_multi_writer_stress(&runtime, branch, 40);
    runtime.close().expect("close durable runtime");
}

/// Durable mode, one branch PER WRITER: every write group spans distinct
/// branches, so each member's memtable apply is deferred and runs on its own
/// parked thread (BS5.4c parallel appliers) — the checkout/restore, barrier,
/// and per-branch publish must preserve the exact single-branch invariants.
#[test]
fn off_lock_concurrent_writers_on_distinct_branches_share_one_durable_runtime() {
    let mut runtime = open_durable_background("off-lock-multi-branch-writer");
    let default_branch = StorageRuntime::default_branch_id_for_test();
    let mut branches = [default_branch; MULTI_WRITERS];
    for (writer, slot) in branches.iter_mut().enumerate().skip(1) {
        let branch = branch_id(0xB0 + u8::try_from(writer).expect("small writer index"));
        runtime
            .branch(&BranchRequest::new(
                branch,
                BranchAction::Create,
                Some(BranchGeneration::new(1)),
            ))
            .expect("create writer branch");
        *slot = branch;
    }
    run_multi_writer_stress_on(&runtime, &branches, 40);
    runtime.close().expect("close durable runtime");
}

#[test]
fn off_lock_fork_and_materialize_under_concurrent_reads() {
    const SEEDED: u64 = 8;
    let mut runtime = open_durable_background("off-lock-c4");
    let parent = StorageRuntime::default_branch_id_for_test();
    let child = branch_id(0x5C);
    // Seed + flush the parent so the child inherits real (flushed) tables to materialize.
    for batch_no in 0..SEEDED {
        runtime
            .commit(&stress_batch(parent, batch_no, STRESS_KEYS))
            .expect("seed parent");
    }
    runtime.wait_background_idle_for_test();
    let latest = SEEDED - 1;

    let done = AtomicBool::new(false);

    std::thread::scope(|scope| {
        // (vi) Concurrent parent readers — the parent is untouched by the child's fork/materialize.
        for _ in 0..2 {
            scope.spawn(|| {
                while !done.load(Ordering::Acquire) {
                    if let Some(observed) = read_checked(&runtime, parent, STRESS_KEYS) {
                        assert_eq!(observed, latest, "parent tore during child branching");
                    }
                }
            });
        }
        // C4 driver: fork mid-stress, read the child, materialize it while reading.
        scope.spawn(|| {
            let _release_readers = ReleaseReaders(&done);
            // (v) fork is O(1) / latency-bounded (inherit-by-reference, no table copy).
            let fork_start = Instant::now();
            runtime
                .branch(&BranchRequest::new(
                    child,
                    BranchAction::ForkCurrent { source: parent },
                    Some(BranchGeneration::new(1)),
                ))
                .expect("fork child");
            assert!(
                fork_start.elapsed() < Duration::from_secs(1),
                "fork was not O(1): {:?}",
                fork_start.elapsed()
            );
            // The child immediately serves the inherited data correctly.
            let inherited =
                read_checked(&runtime, child, STRESS_KEYS).expect("child inherited read");
            assert_eq!(
                inherited, latest,
                "child did not inherit the parent's latest batch"
            );

            // (vii) materialize the child (inherited -> owned) while reading it; never tears.
            runtime
                .enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Materialize,
                    MaintenanceScope::Branch(child),
                ))
                .expect("enqueue materialize");
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let seen =
                    read_checked(&runtime, child, STRESS_KEYS).expect("child read while draining");
                assert_eq!(seen, latest, "child read tore during materialization");
                let layout = runtime
                    .branch_source_layout_for_test(child)
                    .expect("child source layout");
                if layout.inherited_total_tables() == 0 {
                    break; // materialization drained
                }
                assert!(
                    Instant::now() < deadline,
                    "materialization did not drain within the deadline"
                );
            }
            done.store(true, Ordering::Release);
        });
    });

    runtime.wait_background_idle_for_test();
    let layout = runtime
        .branch_source_layout_for_test(child)
        .expect("child source layout");
    assert_eq!(
        layout.inherited_total_tables(),
        0,
        "child still inherits after materialization"
    );
    assert!(
        layout.owned_total_tables() > 0,
        "child has no owned tables after materialization"
    );
    // Both branches read consistently at the end.
    assert_eq!(read_checked(&runtime, parent, STRESS_KEYS), Some(latest));
    assert_eq!(read_checked(&runtime, child, STRESS_KEYS), Some(latest));
    runtime.close().expect("close durable runtime");
}

// ---- L6 families #23 / #24 — COW correctness under concurrent branch ops ----
// The single-threaded branch suite proves fork/clear/pinned-view semantics in
// isolation. These prove they hold while writers and background maintenance run
// concurrently: a fork must capture a consistent COW snapshot mid-write, and a
// reader must never tear across a background flush/compaction boundary.

use std::sync::atomic::AtomicU64;

const FORK_ITERATIONS: u64 = 200;

/// Fork the write branch into a fresh child, and immediately verify the child
/// shows one atomic batch (never a torn read) — the COW snapshot must be
/// internally consistent no matter which instant of the writer's stream it
/// captured. Returns the number of forks that observed a seeded state.
fn forker_loop(
    runtime: &StorageRuntime<'_>,
    source: BranchId,
    child_seq: &AtomicU64,
    start: &Barrier,
) -> u64 {
    start.wait();
    let mut observed = 0;
    for _ in 0..FORK_ITERATIONS {
        let seq = child_seq.fetch_add(1, Ordering::Relaxed);
        let mut bytes = [0xC0u8; BranchId::BYTE_LEN];
        bytes[..8].copy_from_slice(&seq.to_be_bytes());
        let child = BranchId::from_bytes(bytes);

        runtime
            .branch(&crate::api::BranchRequest::new(
                child,
                crate::api::BranchAction::ForkCurrent { source },
                Some(crate::api::BranchGeneration::new(1)),
            ))
            .expect("fork current under concurrent writes");

        // read_checked panics on a torn read (mixed batch numbers or a partial
        // key set). A consistent COW fork can only ever show one whole batch.
        if read_checked(runtime, child, STRESS_KEYS).is_some() {
            observed += 1;
        }
    }
    observed
}

/// #23: a branch forked while a writer hammers it must capture a consistent
/// copy-on-write snapshot — every fork sees exactly one atomic batch, never a
/// mixture of the in-flight write and the prior state.
#[test]
fn fork_captures_a_consistent_snapshot_while_a_writer_races() {
    let mut runtime = open_cache();
    let source = StorageRuntime::default_branch_id_for_test();
    // Seed batch 0 so a fork that lands after it always sees a full key set.
    runtime
        .commit(&stress_batch(source, 0, STRESS_KEYS))
        .expect("seed batch 0");

    let done = AtomicBool::new(false);
    let child_seq = AtomicU64::new(0);
    let start = Barrier::new(2);
    let mut forks_with_state = 0;

    std::thread::scope(|scope| {
        let forker = scope.spawn(|| forker_loop(&runtime, source, &child_seq, &start));
        // Writer streams atomic batches into the source branch throughout.
        writer_loop(&runtime, source, &done, &start);
        forks_with_state = forker.join().expect("forker thread");
    });

    // The forker ran to completion without a single torn read (any tear panics
    // inside read_checked), and observed real seeded state — not a vacuous pass.
    assert!(
        forks_with_state > 0,
        "no fork observed a seeded snapshot ({forks_with_state} of {FORK_ITERATIONS})"
    );
    runtime.close().expect("close cache runtime");
}

/// #24: readers holding pinned views must never tear while background flush and
/// compaction rewrite the branch's LSM underneath them. The durable-background
/// runtime forces frequent flush/compaction (tiny WAL thresholds) concurrently
/// with the reader/writer race.
#[test]
fn pinned_reads_never_tear_across_background_flush_and_compaction() {
    let mut runtime = open_durable_background("l6_pinned_reads_vs_maintenance");
    let branch = StorageRuntime::default_branch_id_for_test();
    runtime
        .commit(&stress_batch(branch, 0, STRESS_KEYS))
        .expect("seed batch 0");

    let done = AtomicBool::new(false);
    let start = Barrier::new(STRESS_READERS + 1);

    std::thread::scope(|scope| {
        for _ in 0..STRESS_READERS {
            // reader_loop scans Latest repeatedly; read_checked panics on any
            // torn read, including one straddling a flush or compaction rewrite.
            scope.spawn(|| reader_loop(&runtime, branch, &done, &start));
        }
        scope.spawn(|| writer_loop(&runtime, branch, &done, &start));
    });

    // Final state is the writer's last batch, intact after all the background
    // flush/compaction churn.
    let final_batch = read_checked(&runtime, branch, STRESS_KEYS).expect("final read");
    assert_eq!(
        final_batch,
        STRESS_BATCHES - 1,
        "final visible batch mismatch"
    );
    runtime.close().expect("close durable background runtime");
}
