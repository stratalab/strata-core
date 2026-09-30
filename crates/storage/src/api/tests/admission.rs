use super::*;

// ----------------------------------------------------------------------------
// Durable write-admission liveness.
//
// Durable mode must survive a sustained mutating load that outpaces maintenance
// by bounded backpressure, never by rejecting commits while maintenance is
// alive — and a genuinely dead/stuck executor must still surface a typed,
// bounded failure rather than hang. These tests drive the durable inline
// background driver under a manual clock so the stall watchdog, wait slice, and
// progress reset are all evaluated deterministically without real time.
// ----------------------------------------------------------------------------

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
fn open_durable_inline_for_admission_test(name: &str) -> StorageRuntime<'static> {
    let backend =
        crate::testkit::leak_static(StorageBackend::local_fs(temp_dir_for_api_test(name)));
    StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::DeterministicInline,
            ),
        backend,
    )
    .expect("durable deterministic-inline open should use owned inline background driver")
    .into_runtime()
}

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
fn seed_frozen_backlog(runtime: &mut StorageRuntime, prefix: &str, count: usize) {
    for index in 0..count {
        let key = format!("{prefix}-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(key.as_bytes(), 0))
            .expect("seed active row before rotation");
        runtime
            .rotate_default_branch_for_test()
            .expect("rotate active into a frozen table");
    }
}

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
#[test]
fn durable_write_admission_liveness_completes_overload_and_records_manifest_persist() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = open_durable_inline_for_admission_test("durable-liveness-overload");
    assert!(
        runtime.set_background_block_wait_for_test(
            std::time::Duration::from_millis(25),
            std::time::Duration::from_millis(250),
            1,
        ),
        "durable inline background runtime should expose test block wait limits"
    );

    seed_frozen_backlog(&mut runtime, "durable-liveness-frozen", 16);

    // Blocking FrozenBacklog pressure. Previously this was converted into a
    // retryable rejection; the writer must instead be paced until maintenance
    // drains the backlog, then admitted.
    runtime
        .commit(&background_put_batch(
            b"durable-liveness-followup",
            b"value".to_vec(),
        ))
        .expect("sustained backlog must pace the writer and complete, never reject");

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_write_admission_wait_attempts() >= 1);
    assert_eq!(
        perf.lifecycle_write_admission_wait_timeouts(),
        0,
        "a live, progressing executor must never time out the writer"
    );
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
    // The background flush published a table manifest off-lock: the manifest fsync now runs with
    // the global runtime lock released, inside the off-lock publish window rather than the
    // publish-lock window. The manifest-persist sub-cost is the fsync itself; it is a subset of
    // the off-lock window that wraps it.
    assert!(
        perf.lifecycle_background_publish_offlock_ns() > 0,
        "durable flush publish must record off-lock publish time"
    );
    assert!(
        perf.lifecycle_background_publish_manifest_persist_ns() > 0,
        "durable flush publish must still record the manifest-persist (fsync) sub-cost"
    );
    assert!(
        perf.lifecycle_background_publish_manifest_persist_ns()
            <= perf.lifecycle_background_publish_offlock_ns(),
        "the manifest fsync is a subset of the off-lock publish window that wraps it"
    );
}

#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
fn durable_manifest_next_sequence(runtime: &StorageRuntime<'_>) -> Option<u64> {
    runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("diagnostics")
        .table_manifest()
        .next_manifest_sequence()
}

#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn off_lock_manifest_fsync_fault_before_visibility_recovers_committed_rows() {
    assert_off_lock_manifest_fsync_fault_recovers(true, "offlock-fault-before-visibility");
}

#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[test]
fn off_lock_manifest_fsync_fault_visible_unconfirmed_recovers_committed_rows() {
    assert_off_lock_manifest_fsync_fault_recovers(false, "offlock-fault-visible-unconfirmed");
}

// A manifest fsync failure during the off-lock publish leaves the table visible in memory but the
// durable manifest at its prior sequence. After a crash, recovery rebuilds owned levels from the
// prior durable manifest and replays the retained WAL, so every committed row is restored —
// matching a synchronous-publish baseline. Exercised for both fault shapes: before the manifest
// becomes visible (temp-file sync) and after it is visible but before durability is confirmed
// (parent-directory sync).
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
fn assert_off_lock_manifest_fsync_fault_recovers(before_visibility: bool, name: &str) {
    let root = temp_dir_for_api_test(name);
    let branch = StorageRuntime::default_branch_id_for_test();
    let value: &[u8] = b"off-lock-recovery";
    let baseline_rows = 8usize;
    let total_rows = 16usize;

    // Phase 1: commit a baseline set and flush it durably so a real table manifest is published.
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
        .expect("baseline durable open")
        .into_runtime();
        runtime
            .commit(&background_put_batch_range(
                "offlock-fault-",
                0,
                baseline_rows,
                value,
            ))
            .expect("baseline commit");
        runtime
            .rotate_default_branch_for_test()
            .expect("rotate baseline active into frozen");
        runtime
            .enqueue_lifecycle_maintenance_for_test(
                crate::lifecycle::MaintenanceTaskRequest::flush(branch),
            )
            .expect("drive baseline flush through the background publish path");
        runtime.close().expect("baseline close");
    }

    // Phase 2: reopen, confirm the baseline manifest is durable, commit more rows, arm the
    // manifest fsync fault, flush so the manifest publish fails, then drop WITHOUT closing — a
    // crash between the in-memory version swap and the durable manifest persist.
    let durable_sequence_before;
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
        .expect("reopen for fault")
        .into_runtime();
        durable_sequence_before = durable_manifest_next_sequence(&runtime);
        assert!(
            durable_sequence_before.is_some_and(|sequence| sequence > 1),
            "baseline flush must publish a durable table manifest before the fault"
        );
        runtime
            .commit(&background_put_batch_range(
                "offlock-fault-",
                baseline_rows,
                total_rows,
                value,
            ))
            .expect("post-baseline commit");
        runtime
            .rotate_default_branch_for_test()
            .expect("rotate post-baseline active into frozen");
        backend.inject_manifest_publish_fault(branch, before_visibility);
        // The off-lock manifest publish converts the fsync fault into recovery-health debt, so the
        // task drains inline and completes; the crash is the drop below, before any recovery retry.
        runtime
            .enqueue_lifecycle_maintenance_for_test(
                crate::lifecycle::MaintenanceTaskRequest::flush(branch),
            )
            .expect("post-baseline flush drains inline despite the manifest publish fault");
        drop(runtime);
    }

    // Phase 3: reopen and assert the durable manifest did NOT advance (the faulted publish left no
    // durable manifest — proof the fault fired) and that every committed row recovered.
    {
        let reopened = reopen_after_drop(|| StorageRuntime::open_local(root.clone()))
            .expect("reopen after crash");
        assert_eq!(
            reopened.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = reopened.into_runtime();
        let durable_sequence_after = durable_manifest_next_sequence(&runtime);
        if before_visibility {
            // The temp-file fsync failed before the manifest rename, so the new manifest never
            // became visible: recovery falls back to the prior durable manifest plus the retained
            // WAL and the durable sequence is unchanged.
            assert_eq!(
                durable_sequence_after, durable_sequence_before,
                "a manifest fsync fault before visibility must not advance the durable manifest"
            );
        } else {
            // The rename succeeded (the manifest is visible) before the parent-directory fsync
            // failed, so the new manifest may be durable; either way the sequence must never regress.
            assert!(
                durable_sequence_after >= durable_sequence_before,
                "the durable manifest sequence must never regress across a faulted publish and crash"
            );
        }
        assert_background_closed_loop_reads(&runtime, "offlock-fault-", total_rows, value);
    }
}

// Same-branch flush and compaction publish through the per-branch off-lock slot under real
// background worker threads. This is timing-dependent, so it is #[ignore]'d; the deterministic
// recovery and sequence-monotonicity tests are the gate. It confirms that under genuine
// concurrency the per-branch publish slot keeps the durable manifest sequence monotonic (never
// regressing) and that every committed row survives a reopen.
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
#[ignore = "timing-dependent concurrency smoke; run manually with --ignored"]
#[test]
fn concurrent_same_branch_flush_and_compaction_preserve_manifest_monotonicity() {
    let root = temp_dir_for_api_test("concurrent-same-branch-publish");
    let branch = StorageRuntime::default_branch_id_for_test();
    let value: &[u8] = b"concurrent-publish";
    let total_rows = 32usize;
    let wait = std::time::Duration::from_secs(10);

    let durable_sequence_before;
    {
        let backend: &'static StorageBackend =
            crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_background_worker_count(2),
            backend,
        )
        .expect("threaded durable open")
        .into_runtime();

        // Two generations of same-branch flush + compaction, raced across the worker threads.
        for generation in 0..2 {
            let start = generation * (total_rows / 2);
            let end = start + (total_rows / 2);
            runtime
                .commit(&background_put_batch_range(
                    "concurrent-",
                    start,
                    end,
                    value,
                ))
                .expect("commit generation rows");
            runtime
                .rotate_default_branch_for_test()
                .expect("rotate generation active into frozen");
            runtime
                .enqueue_lifecycle_maintenance_for_test(
                    crate::lifecycle::MaintenanceTaskRequest::flush(branch),
                )
                .expect("enqueue same-branch flush");
            runtime
                .enqueue_lifecycle_maintenance_for_test(
                    crate::lifecycle::MaintenanceTaskRequest::compaction(branch, 0),
                )
                .expect("enqueue same-branch compaction");
            assert!(
                runtime.wait_background_idle_until_for_test(wait).is_some(),
                "background workers must reach idle within the smoke timeout"
            );
        }
        durable_sequence_before = durable_manifest_next_sequence(&runtime);
        runtime.close().expect("threaded close");
    }

    {
        let reopened = StorageRuntime::open_local(root).expect("reopen threaded database");
        assert_eq!(
            reopened.summary().disposition(),
            StorageOpenDisposition::OpenedExisting
        );
        let runtime = reopened.into_runtime();
        assert!(
            durable_manifest_next_sequence(&runtime) >= durable_sequence_before,
            "the durable manifest sequence must never regress across concurrent publishes"
        );
        assert_background_closed_loop_reads(&runtime, "concurrent-", total_rows, value);
    }
}

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
#[test]
fn durable_write_admission_liveness_resets_stall_deadline_on_progress() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = open_durable_inline_for_admission_test("durable-liveness-reset");
    // One maintenance task per drain round so the writer is paced across more
    // than one wait slice while maintenance drains the backlog.
    assert!(
        runtime.set_background_drain_limits_for_test(1, std::time::Duration::from_secs(1)),
        "durable inline background runtime should expose test drain limits"
    );
    assert!(runtime.set_background_block_wait_for_test(
        std::time::Duration::from_millis(25),
        std::time::Duration::from_millis(250),
        1,
    ));

    seed_frozen_backlog(&mut runtime, "durable-reset-frozen", 16);

    // Blocking pressure. The commit must be paced through several wait slices and
    // complete; each maintenance completion / backlog reduction resets the stall
    // watchdog. (The paired dead-executor test proves the watchdog still fires
    // when there is no progress, so success here is gated on real liveness, not
    // an absolute clock.)
    runtime
        .commit(&background_put_batch(
            b"durable-reset-followup",
            b"value".to_vec(),
        ))
        .expect("maintenance progress must keep resetting the watchdog and complete");

    let perf = crate::observability::perf_trace::snapshot();
    assert!(
        perf.lifecycle_write_admission_wait_attempts() >= 1,
        "the commit must actually have waited on pressure"
    );
    assert!(
        perf.lifecycle_write_admission_block_wait_ns() > 0,
        "the writer was paced (block-waited), not admitted immediately"
    );
    assert_eq!(
        perf.lifecycle_write_admission_wait_timeouts(),
        0,
        "progress must prevent the watchdog from firing"
    );
    assert!(
        perf.lifecycle_write_admission_wait_progress_resets() >= 1,
        "maintenance progress must reset the stall watchdog at least once"
    );
}

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
#[test]
fn durable_write_admission_liveness_dead_executor_rejects_after_bounded_window() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = open_durable_inline_for_admission_test("durable-liveness-dead");
    // No task may run: the executor makes zero progress, modelling a dead/stuck
    // executor.
    assert!(runtime.set_background_drain_limits_for_test(0, std::time::Duration::from_millis(25)));
    assert!(runtime.set_background_block_wait_for_test(
        std::time::Duration::from_millis(25),
        std::time::Duration::from_millis(250),
        1,
    ));

    seed_frozen_backlog(&mut runtime, "durable-dead-frozen", 16);

    let before = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");
    let error = runtime
        .commit(&background_put_batch(
            b"durable-dead-followup",
            b"value".to_vec(),
        ))
        .expect_err("a dead executor must surface a bounded typed failure, not hang");
    let after = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");

    assert!(
        matches!(
            error,
            StorageApiError::StoragePressure {
                severity: CommitAdmissionPressureSeverity::Blocking,
                retryable: true,
                ..
            }
        ),
        "expected a typed retryable blocking storage-pressure rejection"
    );
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.storage_pressure"
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_write_admission_wait_timeouts(), 1);
    assert_eq!(
        perf.lifecycle_write_admission_wait_progress_resets(),
        0,
        "a dead executor never makes progress, so the watchdog never resets"
    );
    assert!(
        after.saturating_duration_since(before) >= std::time::Duration::from_millis(250),
        "the backstop must wait the full liveness window before failing"
    );
}

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
#[test]
fn durable_write_admission_liveness_level_zero_backlog_completes_via_forced_compaction() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = open_durable_inline_for_admission_test("durable-liveness-l0");
    assert!(runtime.set_background_block_wait_for_test(
        std::time::Duration::from_millis(25),
        std::time::Duration::from_millis(500),
        1,
    ));

    // Build a blocking L0 backlog: each flush turns one frozen memtable into one owned level-zero
    // table. The backlog must cross LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD to reach
    // `BlockMutatingAdmission` severity — L0 tables are cheap on-disk objects, so they block admission
    // far later than a frozen-memtable backlog (FROZEN_BLOCKING_FLUSH_THRESHOLD = 4). Referencing the
    // constant keeps the test from silently under-shooting the threshold if it is retuned.
    let l0_backlog = crate::lifecycle::LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD + 4;
    for index in 0..l0_backlog {
        let key = format!("durable-level-zero-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(key.as_bytes(), index as u64 + 1))
            .expect("seed active row before flush");
        runtime
            .flush_default_branch_for_test()
            .expect("flush a frozen memtable into a level-zero table");
    }

    // LevelZeroTableBacklog blocking pressure. The wait path must have an
    // L0->L1 compaction enqueued (symmetric to the FrozenBacklog forced flush)
    // so the writer is paced on real compaction progress, not rejected.
    runtime
        .commit(&background_put_batch(
            b"durable-level-zero-followup",
            b"value".to_vec(),
        ))
        .expect("level-zero backlog must enqueue compaction and complete");

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_write_admission_wait_attempts() >= 1);
    assert_eq!(
        perf.lifecycle_write_admission_wait_timeouts(),
        0,
        "level-zero backlog with live compaction must complete, never reject"
    );
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_admission_changes_do_not_disturb_cache_absence_counters() {
    // Cache regression guard: the durable liveness/publish changes must not leak
    // background maintenance, admission waits, or manifest persistence into the
    // volatile cache path.
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let runtime = StorageRuntime::open(StorageOpenOptions::cache())
        .expect("cache open")
        .into_runtime();
    for index in 0..64 {
        let key = format!("durable-cache-regression-{index}");
        runtime
            .commit(&background_put_batch(key.as_bytes(), b"value".to_vec()))
            .expect("cache commit");
    }

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_tasks_completed(), 0);
    assert_eq!(perf.lifecycle_write_admission_wait_attempts(), 0);
    assert_eq!(perf.lifecycle_write_admission_wait_progress_resets(), 0);
    assert_eq!(perf.lifecycle_background_publish_manifest_persist_ns(), 0);
}
