use super::*;

// ---- Delta-checkpoint crash-consistency and scale gates ----
//
// J0/J1/J2 bounded the checkpoint snapshot to the active+frozen delta (owned-level rows stay in
// their table objects and are reconstructed from the table manifest at recovery), removing the
// >64 MiB full-superset snapshot crash. These tests prove the durability ordering and crash windows
// on the real filesystem, plus the scale repro the delta checkpoint was built to fix. The logical
// cases (publish-failure classification, orphan-snapshot recovery, partial-truncation) are already
// covered by the in-memory checkpoint suite in lifecycle/tests/checkpoint.rs; these add the real
// fsync-fault crash gates and the scale loads.

// Force a checkpoint inline. Under DeterministicInline the maintenance task drains synchronously, so
// this drives the full publish path (snapshot create + manifest update + optional WAL truncation).
// The snapshot id is pinned so a targeted fault can address the snapshot object deterministically.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
fn force_checkpoint_inline(
    runtime: &mut StorageRuntime<'static>,
    snapshot_id: u64,
    truncate_wal: bool,
) -> StorageApiResult<()> {
    runtime
        .enqueue_lifecycle_maintenance_for_test(
            crate::lifecycle::MaintenanceTaskRequest::checkpoint_with_options(
                crate::lifecycle::MaintenanceCheckpointOptions::new(
                    Some(snapshot_id),
                    truncate_wal,
                ),
            ),
        )
        .map(|_| ())
}

#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
fn durable_checkpoint_snapshot_id(runtime: &StorageRuntime<'_>) -> Option<u64> {
    runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("diagnostics")
        .checkpoint()
        .snapshot_id()
}

// Sampled point reads for the large scale tests (a full prefix scan would materialize millions of
// rows). Confirms the boundary rows recovered with the expected value.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
fn assert_sampled_point_reads(
    runtime: &StorageRuntime<'_>,
    key_prefix: &str,
    total_rows: usize,
    expected_value: &[u8],
) {
    for index in [0, total_rows / 2, total_rows - 1] {
        let key_string = format!("{key_prefix}{index:08}");
        let point = runtime
            .read_point(&PointReadRequest::new(
                StorageRuntime::default_branch_id_for_test(),
                background_space(),
                key(key_string.as_bytes()),
                ReadBound::Latest,
            ))
            .unwrap_or_else(|error| panic!("sampled point read {key_string} failed: {error}"));
        let row = point
            .row()
            .unwrap_or_else(|| panic!("sampled point read {key_string} missed after recovery"));
        assert_eq!(
            row.value().map(StorageValue::as_bytes),
            Some(expected_value),
            "sampled point read {key_string} recovered the wrong value"
        );
    }
}

// A checkpoint whose snapshot publish fails at the fsync must leave no live snapshot, must not
// truncate the WAL, and must recover every committed row from the durable manifest + retained WAL —
// matching a synchronous baseline. Exercised for both fault shapes (before the snapshot is visible,
// and after it is visible but before its durability is confirmed) and with WAL truncation requested
// (truncation must stay gated behind snapshot + manifest durability).
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
fn assert_faulted_checkpoint_recovers(
    name: &str,
    truncate_wal: bool,
    arm_snapshot_fault: impl FnOnce(&'static StorageBackend, u64),
) {
    let root = temp_dir_for_api_test(name);
    let value: &[u8] = b"checkpoint-crash-recovery";
    let prefix = "ckpt-crash-";
    let total_rows = 16usize;
    let snapshot_id = 1u64;

    {
        let backend: &'static StorageBackend =
            crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::DeterministicInline,
                ),
            backend,
        )
        .expect("durable inline open")
        .into_runtime();
        runtime
            .commit(&background_put_batch_range(prefix, 0, total_rows, value))
            .expect("commit rows before the checkpoint");
        // The rows are durable in the WAL; the checkpoint snapshot publish is what we fault.
        arm_snapshot_fault(backend, snapshot_id);
        // The checkpoint publish fails at the snapshot fsync and records recovery-health debt; the
        // crash is the drop below, before any retry.
        let _ = force_checkpoint_inline(&mut runtime, snapshot_id, truncate_wal);
        assert_eq!(
            durable_checkpoint_snapshot_id(&runtime),
            None,
            "a faulted checkpoint must not make a snapshot live"
        );
        drop(runtime);
    }

    {
        let reopened = StorageRuntime::open_local(root).expect("reopen after crash");
        assert_eq!(
            reopened.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = reopened.into_runtime();
        // Recovery falls back to the durable manifest + retained WAL — only possible if the WAL was
        // not truncated ahead of a durable snapshot.
        assert_background_closed_loop_reads(&runtime, prefix, total_rows, value);
    }
}

#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn checkpoint_snapshot_fsync_fault_before_visibility_recovers_committed_rows() {
    assert_faulted_checkpoint_recovers(
        "checkpoint-snapshot-fault-before-visibility",
        false,
        |backend, snapshot_id| backend.inject_snapshot_publish_fault(snapshot_id, true),
    );
}

#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn checkpoint_snapshot_fsync_fault_visible_unconfirmed_recovers_committed_rows() {
    assert_faulted_checkpoint_recovers(
        "checkpoint-snapshot-fault-visible-unconfirmed",
        false,
        |backend, snapshot_id| backend.inject_snapshot_publish_fault(snapshot_id, false),
    );
}

// WAL truncation is requested, but the snapshot publish fault fails the checkpoint before the
// snapshot (and covering manifest) are durable, so truncation must not run — recovery still restores
// every row from the retained WAL.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn checkpoint_wal_truncation_gated_by_snapshot_durability_recovers_committed_rows() {
    assert_faulted_checkpoint_recovers(
        "checkpoint-truncation-gate",
        true,
        |backend, snapshot_id| backend.inject_snapshot_publish_fault(snapshot_id, true),
    );
}

// A clean checkpoint makes a snapshot live; committing more rows afterward and crashing before any
// WAL truncation exercises the unified-recovery combination: the checkpoint delta (rows at or below
// the watermark) plus the replayed WAL tail (rows above it). Every row recovers.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn clean_checkpoint_then_crash_recovers_delta_and_wal_tail() {
    let root = temp_dir_for_api_test("checkpoint-clean-then-crash");
    let value: &[u8] = b"checkpoint-delta-and-tail";
    let prefix = "ckpt-clean-";
    let checkpoint_rows = 16usize;
    let total_rows = 24usize;

    {
        let backend: &'static StorageBackend =
            crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::DeterministicInline,
                ),
            backend,
        )
        .expect("durable inline open")
        .into_runtime();
        runtime
            .commit(&background_put_batch_range(
                prefix,
                0,
                checkpoint_rows,
                value,
            ))
            .expect("commit pre-checkpoint rows");
        force_checkpoint_inline(&mut runtime, 1, false).expect("clean checkpoint drains inline");
        assert_eq!(
            durable_checkpoint_snapshot_id(&runtime),
            Some(1),
            "a clean checkpoint must make its snapshot live"
        );
        runtime
            .commit(&background_put_batch_range(
                prefix,
                checkpoint_rows,
                total_rows,
                value,
            ))
            .expect("commit the WAL tail above the checkpoint watermark");
        drop(runtime);
    }

    {
        let reopened = StorageRuntime::open_local(root).expect("reopen after crash");
        assert_eq!(
            reopened.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = reopened.into_runtime();
        assert_background_closed_loop_reads(&runtime, prefix, total_rows, value);
    }
}

// Commit durability is independent of checkpointing: rows are visible immediately after the commit
// acknowledges and survive a crash with no checkpoint at all, recovered from WAL replay alone.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn committed_rows_are_durable_via_wal_before_any_checkpoint() {
    let root = temp_dir_for_api_test("checkpoint-wal-durability");
    let value: &[u8] = b"wal-durability";
    let prefix = "wal-durable-";
    let total_rows = 12usize;

    {
        let backend: &'static StorageBackend =
            crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::DeterministicInline,
                ),
            backend,
        )
        .expect("durable inline open")
        .into_runtime();
        runtime
            .commit(&background_put_batch_range(prefix, 0, total_rows, value))
            .expect("commit rows");
        // Visible immediately after the commit acknowledges (read-after-write).
        assert_background_closed_loop_reads(&runtime, prefix, total_rows, value);
        assert_eq!(
            durable_checkpoint_snapshot_id(&runtime),
            None,
            "no checkpoint should have run"
        );
        drop(runtime);
    }

    {
        let reopened = StorageRuntime::open_local(root).expect("reopen after crash");
        assert_eq!(
            reopened.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = reopened.into_runtime();
        assert_background_closed_loop_reads(&runtime, prefix, total_rows, value);
    }
}

// Scale gate: a database whose durable owned-level state exceeds the 64 MiB snapshot encode ceiling
// still checkpoints successfully, because the delta is bounded by the active backlog rather than the
// total database size. A full-superset snapshot of this database would be rejected by the ceiling.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[ignore = "loads >64 MiB of durable state; run manually with --ignored (prefer --release)"]
#[test]
fn durable_state_over_encode_ceiling_checkpoints_via_bounded_delta() {
    let root = temp_dir_for_api_test("checkpoint-over-ceiling");
    let value = vec![0xABu8; 8 * 1024]; // 8 KiB values
    let owned_rows = 10_000usize; // ~80 MiB of durable owned-level state, well over the 64 MiB ceiling
    let backlog_rows = 8usize;
    let prefix = "ceiling-";

    {
        let backend: &'static StorageBackend =
            crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::DeterministicInline,
                ),
            backend,
        )
        .expect("durable inline open")
        .into_runtime();
        // Load >64 MiB and flush it all to owned levels, so it is durable in table objects (and
        // reconstructed from the manifest at recovery) rather than carried in the checkpoint delta.
        let chunk = 500usize;
        let mut start = 0;
        while start < owned_rows {
            let end = (start + chunk).min(owned_rows);
            runtime
                .commit(&background_put_batch_range(prefix, start, end, &value))
                .expect("commit owned-level chunk");
            runtime
                .flush_default_branch_for_test()
                .expect("flush chunk to owned levels");
            start = end;
        }
        // A small active backlog — all the checkpoint delta should contain.
        runtime
            .commit(&background_put_batch_range(
                prefix,
                owned_rows,
                owned_rows + backlog_rows,
                &value,
            ))
            .expect("commit the active backlog");
        force_checkpoint_inline(&mut runtime, 1, false)
            .expect("a bounded-delta checkpoint over a >64 MiB database must succeed");
        assert_eq!(
            durable_checkpoint_snapshot_id(&runtime),
            Some(1),
            "the bounded-delta checkpoint must make its snapshot live"
        );
        runtime
            .close()
            .expect("close after the over-ceiling checkpoint");
    }

    {
        let reopened = StorageRuntime::open_local(root).expect("reopen the over-ceiling database");
        assert_eq!(
            reopened.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = reopened.into_runtime();
        assert_sampled_point_reads(&runtime, prefix, owned_rows + backlog_rows, &value);
    }
}

// Scale gate (the original repro): a 10M-row load that forces checkpoints against a large backlog
// completes without rejection, and the database recovers after a crash — the regime that crashed
// before the delta-checkpoint fix. Settle-to-quiescence is exercised here as recovery-correctness
// (recovered reads match the pre-crash baseline); the fully-drained-L0 quiescence comparison is
// deferred to the billion-scale cycle.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[ignore = "loads 10M rows; run manually with --ignored --release"]
#[test]
fn forced_checkpoint_at_scale_load_and_recovery() {
    let root = temp_dir_for_api_test("checkpoint-scale-10m");
    let value: &[u8] = &[0x5Au8; 16];
    let total_rows = 10_000_000usize;
    let batch = 1_000usize;
    let prefix = "scale-";

    {
        let backend: &'static StorageBackend =
            crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        // Background workers (the realistic regime, as in the L9 load benchmark): maintenance drains
        // concurrently, so the writer is paced under sustained pressure rather than rejected. A
        // commits-based WAL-growth threshold forces checkpoints against the large backlog throughout
        // the load — the regime that crashed before the delta-checkpoint fix.
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_background_worker_count(default_background_worker_count())
                .with_wal_growth_policy(StorageWalGrowthPolicy::Thresholds {
                    max_retained_wal_bytes: u64::MAX,
                    max_retained_wal_segments: usize::MAX,
                    // Counts commit *calls* (one per batch); ~10k batches here, so checkpoint every
                    // ~1k batches (~1M rows) keeps a steady checkpoint cadence under the backlog.
                    max_commits_since_checkpoint: 1_000,
                }),
            backend,
        )
        .expect("durable background open")
        .into_runtime();
        let mut start = 0;
        while start < total_rows {
            let end = (start + batch).min(total_rows);
            runtime
                .commit(&background_put_batch_range(prefix, start, end, value))
                .expect("commit scale batch");
            start = end;
        }
        // Force a final checkpoint so at least one is guaranteed to land regardless of the
        // WAL-growth cadence, then settle best-effort: give the workers time to drain queued
        // checkpoints/flushes. We do not require full L0 quiescence here — that comparison is
        // deferred to the billion-scale cycle (the off-lock-slot drain gap noted in the L8K findings).
        runtime
            .enqueue_lifecycle_maintenance_for_test(
                crate::lifecycle::MaintenanceTaskRequest::checkpoint(),
            )
            .expect("enqueue final checkpoint");
        let _ = runtime.wait_background_idle_until_for_test(std::time::Duration::from_secs(60));
        assert!(
            durable_checkpoint_snapshot_id(&runtime).is_some(),
            "at least one checkpoint must execute during the scale load"
        );
        drop(runtime);
    }

    {
        let reopened = StorageRuntime::open_local(root).expect("reopen the scale database");
        assert_eq!(
            reopened.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = reopened.into_runtime();
        // Settle-to-quiescence recovery (recovery-correctness): recovered reads match the pre-crash
        // baseline after a 10M load with executed checkpoints.
        assert_sampled_point_reads(&runtime, prefix, total_rows, value);
    }
}

/// #3499 cross-version guard: a store whose tables were written UNCOMPRESSED
/// must reopen and read correctly under a compression-enabled config, and a
/// later compressed flush must coexist with the old uncompressed tables (the
/// per-block codec is self-describing). Proves enabling compression never
/// strands existing data, and that mixed uncompressed+Zstd tables read back.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn uncompressed_tables_read_back_under_compression_and_mix() {
    let root = temp_dir_for_api_test("xversion-compression");
    let value_a: &[u8] = b"uncompressed-era-value-aaaaaaaa";
    let value_b: &[u8] = b"compressed-era-value-bbbbbbbbbb";
    let (prefix_a, prefix_b) = ("vA-", "vB-");
    let rows = 64usize;

    let open = |root: std::path::PathBuf, compression: crate::format::TableCompression| {
        let backend: &'static StorageBackend =
            crate::testkit::leak_static(StorageBackend::local_fs(root));
        StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::DeterministicInline,
                )
                .with_table_compression_for_test(compression),
            backend,
        )
        .expect("durable inline open")
        .into_runtime()
    };
    let flush_then_checkpoint = |runtime: &mut StorageRuntime<'static>, snapshot_id: u64| {
        runtime
            .enqueue_lifecycle_maintenance_for_test(
                crate::lifecycle::MaintenanceTaskRequest::flush(
                    StorageRuntime::default_branch_id_for_test(),
                ),
            )
            .expect("flush enqueue");
        // Truncate the WAL so recovery must serve era-1 rows from the table
        // blocks, not the log — the point of the cross-version read.
        force_checkpoint_inline(runtime, snapshot_id, true).expect("checkpoint");
    };

    // Era 1 — uncompressed tables.
    {
        let mut runtime = open(root.clone(), crate::format::TableCompression::Uncompressed);
        runtime
            .commit(&background_put_batch_range(prefix_a, 0, rows, value_a))
            .expect("commit era-1");
        flush_then_checkpoint(&mut runtime, 1);
        drop(runtime);
    }
    // Era 2 — reopen with Zstd; read era-1 from uncompressed tables; write + flush era-2 (Zstd).
    {
        let mut runtime = open(root.clone(), crate::format::TableCompression::Zstd);
        assert_background_closed_loop_reads(&runtime, prefix_a, rows, value_a);
        runtime
            .commit(&background_put_batch_range(prefix_b, 0, rows, value_b))
            .expect("commit era-2");
        flush_then_checkpoint(&mut runtime, 2);
        drop(runtime);
    }
    // Era 3 — reopen; both eras (mixed uncompressed + Zstd tables) read correctly.
    // A fresh-created store would answer these reads with zero rows, so the read
    // assertions themselves prove the reopen recovered the prior eras.
    {
        let outcome = StorageRuntime::open_local(root).expect("reopen");
        assert_eq!(
            outcome.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = outcome.into_runtime();
        assert_background_closed_loop_reads(&runtime, prefix_a, rows, value_a);
        assert_background_closed_loop_reads(&runtime, prefix_b, rows, value_b);
    }
}
