use super::*;

#[test]
fn flush_watermark_request_rejects_zero_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let shell = assemble_shell(branch_id(0x01), backend).expect("shell");
    assert_eq!(
        persist_flush_watermark(
            shell.services().manifest(),
            CommitVersion::new(1),
            CommitVersion::ZERO,
            &LifecycleFlushWatermarkProof::CheckpointCovered {
                snapshot_watermark: CommitVersion::new(1),
            },
        ),
        Err(LifecycleError::WalRetentionProofIncomplete {
            reason: "flush watermark candidate must be nonzero",
        })
    );
}

#[test]
fn checkpoint_snapshot_id_must_advance_past_recovered_manifest_snapshot_id() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x2a);
    let shell = assemble_shell(branch, backend).expect("shell");
    let mut state = BranchLocalState::empty(branch);
    state
        .append_committed_row(put_row(branch, 1, b"snapshot-advance", b"value"))
        .expect("append row");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(7, CommitVersion::new(1))
        .expect("snapshot facts");
    let request =
        LifecycleCheckpointRequest::new(branch, 7, Timestamp::from_micros(10)).expect("request");

    let error = checkpoint_durable_branch(
        &state,
        shell.services(),
        shell.guard_set(),
        || CommitVersion::new(1),
        &request,
    )
    .expect_err("stale snapshot id rejects");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.checkpoint_publication"
    );
    assert_eq!(backend.snapshot_objects(), Vec::<ObjectName>::new());
}

#[test]
fn checkpoint_outcome_converts_completed_snapshot_to_maintenance_success() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x2b);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"maintenance-success", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(11)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("checkpoint");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::Checkpoint);
    assert!(!maintenance.retryable());
}

#[test]
fn checkpoint_debug_output_does_not_include_payload_bytes() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x2c);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"debug-redaction", b"super-secret-payload"),
            generation_guard(),
        )
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(12)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("checkpoint");
    let debug = format!("{outcome:?}");

    assert!(!debug.contains("super-secret-payload"));
    assert!(debug.contains("Completed"));
}

#[test]
fn checkpoint_recovery_ignores_opaque_snapshot_sections() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x38);
    let key = physical_key(branch, b"unsupported-section");
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"unsupported-section", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(16))
        .expect("request")
        // Kind 0x7F is unassigned: recovery must ignore sections it does not
        // understand (kind 2 became the retained-timeline section in W3.1b —
        // a malformed ASSIGNED kind now fails recovery closed instead).
        .with_extra_sections(vec![crate::format::SnapshotSection::new(
            0x7F,
            b"unsupported".to_vec(),
        )
        .expect("extra section")]);

    runtime.checkpoint(&request).expect("checkpoint");
    drop(runtime);
    let mut shell = assemble_shell(branch, backend).expect("recovery shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery ignores opaque section");
    let reopened = shell.complete_recovery(&outcome).expect("open runtime");

    assert_eq!(outcome.checkpoint().section_count(), 3);
    assert_eq!(
        reopened
            .read_view()
            .expect("read view")
            .latest(&key)
            .expect("latest read")
            .expect("row")
            .row()
            .value(),
        b"value"
    );
}

#[test]
fn checkpoint_rows_exclude_materialized_owned_rows() {
    let source = branch_id(0x2d);
    let child = branch_id(0x2e);
    let mut source_state = BranchLocalState::empty(source);
    source_state
        .append_committed_row(put_row(source, 1, b"materialized-row", b"value"))
        .expect("append source row");
    source_state.rotate_active();
    flush_cache_branch(&mut source_state, &flush_request(source)).expect("flush source row");
    let (mut child_state, fork) = source_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let (handle, _) = child_state
        .mark_inherited_layer_materializing(0)
        .expect("materialization handle");
    let request = BranchMaterializationRequest::from_handle(handle, "checkpoint-replacement")
        .expect("materialization request");

    child_state
        .materialize_inherited_layer(&request)
        .expect("materialize layer");
    let rows = child_state
        .checkpoint_rows(fork.fork_version())
        .expect("checkpoint rows");

    // Materialization installs the inherited rows as owned-level tables, which are
    // durable and recorded in the table manifest. The checkpoint delta therefore
    // excludes them (a durable runtime recovers them from the manifest); the child
    // has no active/frozen rows, so the delta is empty.
    assert!(rows.is_empty());
}

#[test]
fn checkpoint_rows_require_inherited_layers_to_be_materialized() {
    let source = branch_id(0x39);
    let child = branch_id(0x3a);
    let mut source_state = BranchLocalState::empty(source);
    source_state
        .append_committed_row(put_row(source, 1, b"inherited-row", b"value"))
        .expect("append source row");
    source_state.rotate_active();
    flush_cache_branch(&mut source_state, &flush_request(source)).expect("flush source row");
    let (child_state, fork) = source_state
        .fork_into_empty_child(child)
        .expect("fork child");

    let error = child_state
        .checkpoint_rows(fork.fork_version())
        .expect_err("live inherited layer rejects");

    assert_eq!(
        error,
        crate::branch::error::BranchRuntimeError::InvalidBranchState {
            reason: "checkpoint requires inherited layers to be materialized first",
        }
    );
}

#[test]
fn checkpoint_recovery_round_trip_after_frozen_flush() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x2f);
    let mut runtime = open_runtime(branch, backend);
    let first = physical_key(branch, b"flushed-before-checkpoint");
    let second = physical_key(branch, b"active-before-checkpoint");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"flushed-before-checkpoint", b"first"),
            generation_guard(),
        )
        .expect("first commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .flush_frozen(&flush_request(branch))
        .expect("flush frozen");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"active-before-checkpoint", b"second"),
            generation_guard(),
        )
        .expect("second commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(13)).expect("request");
    let outcome = runtime.checkpoint(&request).expect("checkpoint");
    // J2: the checkpoint is a bounded delta. Only the still-active `second` commit
    // (its user row + 2 timeline rows = 3) is emitted; the flushed `first` commit's
    // rows are owned and excluded (recovery restores them from the table manifest).
    // Pre-J2 this would have been 6 (both commits' rows); W3.1c removed the
    // two timeline rows.
    assert_eq!(outcome.row_count(), 1);
    drop(runtime);

    let reopened = open_runtime(branch, backend);
    let view = reopened.read_view().expect("read view");

    assert_eq!(
        view.latest(&first)
            .expect("first read")
            .expect("first row")
            .row()
            .value(),
        b"first"
    );
    assert_eq!(
        view.latest(&second)
            .expect("second read")
            .expect("second row")
            .row()
            .value(),
        b"second"
    );
}

#[test]
fn delta_checkpoint_records_flush_boundary_not_visible_version() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x2e);
    let mut runtime = open_runtime(branch, backend);
    // The first commit is flushed into a durable owned table (the base, commit 1)...
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"flushed", b"first"),
            generation_guard(),
        )
        .expect("first commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .flush_frozen(&flush_request(branch))
        .expect("flush frozen");
    // ...while the second commit stays active, so the checkpoint snapshot is a bounded delta
    // over the flush boundary (commit 1).
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"active", b"second"),
            generation_guard(),
        )
        .expect("second commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(13)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("checkpoint");

    assert_eq!(outcome.checkpoint_watermark(), Some(CommitVersion::new(2)));
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("manifest");
    assert_eq!(manifest.snapshot_watermark(), Some(2));
    // The delta base floor is the flush boundary (commit 1), NOT the snapshot watermark
    // (commit 2). Recording the watermark would hide that the snapshot needs the
    // table-manifest base for [1..1], letting a lost base recover an orphaned delta.
    assert_eq!(
        manifest.flushed_through_commit_id(),
        Some(CommitVersion::new(1))
    );
}

#[test]
fn full_checkpoint_leaves_flush_boundary_unset() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x2d);
    let mut runtime = open_runtime(branch, backend);
    // No flush: the checkpoint snapshot is full and self-contained, so it records no base.
    runtime
        .execute_durable_commit(durable_batch(branch, b"only", b"value"), generation_guard())
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(13)).expect("request");

    runtime.checkpoint(&request).expect("checkpoint");

    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("manifest");
    assert_eq!(manifest.snapshot_watermark(), Some(1));
    assert_eq!(manifest.flushed_through_commit_id(), None);
}

#[test]
fn checkpoint_with_truncation_does_not_truncate_after_partial_checkpoint() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x30);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"partial-no-delete", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_replacement_on_call(2);
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(14))
        .expect("request")
        .with_wal_truncation_after_checkpoint(true);
    let list_calls_before = backend.list_calls();

    let outcome = runtime.checkpoint(&request).expect("partial checkpoint");

    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::SnapshotPublishedManifestNotUpdated
    );
    assert!(outcome.wal_truncation().is_none());
    assert_eq!(backend.list_calls(), list_calls_before);
}

#[test]
fn wal_truncation_no_segments_is_completed_noop() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x31);
    let mut runtime = open_runtime(branch, backend);
    let request = WalRetentionProof::snapshot_watermark(CommitVersion::new(1));

    let outcome = runtime.truncate_wal(request).expect("truncation");

    assert!(outcome.completed_cleanly());
    assert_eq!(outcome.deleted_segments(), 0);
    assert_eq!(outcome.failed_segments(), 0);
    assert_eq!(outcome.protected_segments(), 1);
}

#[test]
fn wal_truncation_service_error_has_stable_code_and_source() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x32);
    let mut runtime = open_runtime(branch, backend);
    backend.fail_wal_listing();
    let request = WalRetentionProof::snapshot_watermark(CommitVersion::new(1));

    let error = runtime
        .truncate_wal(request)
        .expect_err("listing failure rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
}

#[test]
fn wal_truncation_deletes_covered_segments_and_keeps_active_segment() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x33);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    let active = runtime.services().wal().active_segment_id();
    let request = WalRetentionProof::snapshot_watermark(CommitVersion::new(24));

    let outcome = runtime.truncate_wal(request).expect("truncation");

    assert!(active > 1, "test setup must rotate segments");
    assert!(outcome.completed_cleanly());
    assert!(outcome.deleted_segments() > 0);
    assert!(outcome.protected_segments() >= 1);
    assert_eq!(outcome.failed_segments(), 0);
}

#[test]
fn wal_truncation_from_table_manifest_flush_watermark_deletes_covered_segments() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3d);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    let active = runtime.services().wal().active_segment_id();
    let request = WalRetentionProof::flush_watermark(CommitVersion::new(24));

    let outcome = runtime.truncate_wal(request).expect("truncation");

    assert!(active > 1, "test setup must rotate segments");
    assert!(outcome.completed_cleanly());
    assert!(outcome.deleted_segments() > 0);
    assert!(outcome.protected_segments() >= 1);
    assert_eq!(outcome.failed_segments(), 0);
}

#[test]
fn wal_truncation_delete_failure_records_health_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x34);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    backend.fail_delete();
    let request = WalRetentionProof::flush_watermark(CommitVersion::new(24));

    let outcome = runtime.truncate_wal(request).expect("truncation report");

    assert!(outcome.completed_with_health_debt());
    assert_eq!(outcome.deleted_segments(), 0);
    assert!(outcome.failed_segments() > 0);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Failed
    );
}

#[test]
fn checkpoint_with_truncation_recovery_round_trip_after_delete() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x35);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    let key = dynamic_physical_key(branch, b"rotated-commit-23".to_vec());
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(15))
        .expect("request")
        .with_wal_truncation_after_checkpoint(true);

    let outcome = runtime.checkpoint(&request).expect("checkpoint");
    // Call-site coverage for the checkpoint follow-up WAL-truncation ledger hook
    // (space-reclamation contract §3.5): the truncation rode on the checkpoint
    // outcome rather than a standalone WalTruncation task, and still reached the
    // WAL-truncation family, with the segments it deleted recorded as the pass's
    // durable state changes. The unit `a_checkpoint_follow_up_truncation_is_a_
    // wal_truncation_event` classifies a synthetic outcome; this pins the wiring
    // from a real `checkpoint(...)` through `record_reclaim` into the ledger.
    let deleted_segments = outcome
        .wal_truncation()
        .expect("truncation outcome")
        .deleted_segments();
    let truncation = runtime
        .reclaim_ledger()
        .last(crate::lifecycle::ReclaimFamily::WalTruncation)
        .expect("checkpoint follow-up truncation recorded");
    assert_eq!(
        truncation.outcome(),
        crate::lifecycle::ReclaimOutcome::Reclaimed,
        "{truncation:?}"
    );
    assert_eq!(truncation.deferral(), None, "{truncation:?}");
    assert_eq!(
        truncation.state_changes(),
        deleted_segments,
        "{truncation:?}"
    );
    assert_eq!(
        truncation.objects_affected(),
        deleted_segments,
        "{truncation:?}"
    );
    assert!(
        runtime.reclaim_ledger().totals().reclaimed_passes() >= 1,
        "{:?}",
        runtime.reclaim_ledger()
    );
    drop(runtime);
    let reopened = open_runtime_with_wal_segment_size(branch, backend, 1024);
    let visible = reopened
        .read_view()
        .expect("read view")
        .latest(&key)
        .expect("read latest")
        .expect("visible row");

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert!(
        outcome
            .wal_truncation()
            .expect("truncation outcome")
            .deleted_segments()
            > 0
    );
    assert_eq!(visible.row().value(), vec![23_u8; 128].as_slice());
    assert_eq!(reopened.visible_version(), CommitVersion::new(24));
}

#[test]
fn checkpoint_recovery_replays_tail_after_checkpoint_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x39);
    let mut runtime = open_runtime(branch, backend);
    let before = physical_key(branch, b"before-checkpoint");
    let after = physical_key(branch, b"after-checkpoint");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"before-checkpoint", b"before"),
            generation_guard(),
        )
        .expect("first commit");
    runtime
        .checkpoint(
            &LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(17))
                .expect("request"),
        )
        .expect("checkpoint");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"after-checkpoint", b"after"),
            generation_guard(),
        )
        .expect("tail commit");
    drop(runtime);

    let reopened = open_runtime(branch, backend);
    let view = reopened.read_view().expect("read view");

    assert_eq!(
        view.latest(&before)
            .expect("before read")
            .expect("before row")
            .row()
            .value(),
        b"before"
    );
    assert_eq!(
        view.latest(&after)
            .expect("after read")
            .expect("after row")
            .row()
            .value(),
        b"after"
    );
    assert_eq!(reopened.visible_version(), CommitVersion::new(2));
}

#[test]
fn checkpoint_recovery_ignores_duplicate_record_at_checkpoint_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3a);
    let mut runtime = open_runtime(branch, backend);
    let key = physical_key(branch, b"duplicate-boundary");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"duplicate-boundary", b"value"),
            generation_guard(),
        )
        .expect("commit");
    runtime
        .checkpoint(
            &LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(18))
                .expect("request"),
        )
        .expect("checkpoint");
    drop(runtime);

    let reopened = open_runtime(branch, backend);
    let history = reopened
        .read_view()
        .expect("read view")
        .history(&key, crate::branch::read::BranchHistoryOptions::all())
        .expect("history");

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].row().value(), b"value");
    assert_eq!(reopened.visible_version(), CommitVersion::new(1));
}

#[test]
fn queued_wal_truncation_task_failure_adds_health_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x36);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(24))
        .expect("snapshot facts");
    backend.fail_delete();
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue");

    let maintenance = runtime
        .run_next_wal_truncation_maintenance()
        .expect("run")
        .expect("maintenance");

    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
    assert!(maintenance.recovery_health().is_some());
    assert!(maintenance.retryable());
    assert!(backend.delete_calls() > 0);
}

#[test]
fn maintenance_task_reports_health_debt_on_wal_truncation_failure() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x37);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    // Snapshot and flush both at v24 so the WAL maintenance task selects the
    // typed flush-watermark proof (flush >= snapshot path in
    // wal_truncation_request_from_maintenance_task).
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(24))
        .expect("snapshot facts");
    runtime
        .services()
        .manifest()
        .persist_flush_watermark(CommitVersion::new(24))
        .expect("flush watermark");
    backend.fail_delete();
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue");

    let maintenance = runtime
        .run_next_wal_truncation_maintenance()
        .expect("run")
        .expect("maintenance");

    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
    assert!(maintenance.recovery_health().is_some());
    assert!(maintenance.retryable());
    assert!(backend.delete_calls() > 0);
}

#[test]
fn wal_truncation_keeps_segment_with_record_above_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3b);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_with_value_len(&mut runtime, branch, 12, 1);
    let request = WalRetentionProof::snapshot_watermark(CommitVersion::new(1));

    let outcome = runtime.truncate_wal(request).expect("truncation");

    assert!(outcome.completed_cleanly());
    assert_eq!(outcome.deleted_segments(), 0);
    assert!(outcome.protected_segments() > 1);
}

#[test]
fn wal_truncation_keeps_segment_with_record_above_table_manifest_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3e);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_with_value_len(&mut runtime, branch, 12, 1);
    let request = WalRetentionProof::flush_watermark(CommitVersion::new(1));

    let outcome = runtime.truncate_wal(request).expect("truncation");

    assert!(outcome.completed_cleanly());
    assert_eq!(outcome.deleted_segments(), 0);
    assert!(outcome.protected_segments() > 1);
}

#[test]
fn wal_truncation_keeps_active_segment_under_table_manifest_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3f);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    let active = runtime.services().wal().active_segment_id();
    let request = WalRetentionProof::flush_watermark(CommitVersion::new(24));

    let outcome = runtime.truncate_wal(request).expect("truncation");

    assert!(active > 1, "test setup must rotate segments");
    assert!(outcome.completed_cleanly());
    assert!(outcome.protected_segments() >= 1);
}

#[test]
fn wal_truncation_keeps_segment_newer_than_active_segment() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let config = crate::service::WalServiceConfig::new(1024);
    // Open the active writer FIRST (empty directory -> segment 1); the future
    // segment is created afterwards, as a concurrent handle would. Opening
    // the active writer after segment 2 exists would reconcile it to the
    // on-disk tail (#2555), which is exactly what this boundary test must
    // not depend on.
    let active_service = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        1,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("active segment");
    let _future_service = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        2,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("future segment");
    let request = WalRetentionProof::snapshot_watermark(CommitVersion::new(1));

    let outcome = truncate_wal(&active_service, request).expect("truncation");

    assert!(outcome.completed_cleanly());
    assert_eq!(outcome.deleted_segments(), 0);
    assert_eq!(outcome.protected_segments(), 2);
}

#[test]
fn wal_truncation_keeps_newer_than_active_segment() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let config = crate::service::WalServiceConfig::new(1024);
    // Active writer first, future segment second — see
    // `wal_truncation_keeps_segment_newer_than_active_segment` (#2555).
    let active_service = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        1,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("active segment");
    let _future_service = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        2,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("future segment");
    let request = WalRetentionProof::flush_watermark(CommitVersion::new(1));

    let outcome = truncate_wal(&active_service, request).expect("truncation");

    assert!(outcome.completed_cleanly());
    assert_eq!(outcome.deleted_segments(), 0);
    assert_eq!(outcome.protected_segments(), 2);
}

#[test]
fn wal_truncation_handles_empty_segment_through_service_report() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let config = crate::service::WalServiceConfig::new(1024);
    let _covered_service = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        1,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("covered segment");
    let active_service = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        2,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("active segment");
    let request = WalRetentionProof::snapshot_watermark(CommitVersion::new(1));

    let outcome = truncate_wal(&active_service, request).expect("truncation");

    assert!(outcome.completed_cleanly());
    assert_eq!(outcome.deleted_segments(), 1);
    assert_eq!(outcome.protected_segments(), 1);
}

#[test]
fn wal_truncation_partial_delete_report_is_not_clean_reclaim() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let config = crate::service::WalServiceConfig::new(1024);
    let _first = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        1,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("first segment");
    let _second = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        2,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("second segment");
    let active = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        3,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("active segment");
    backend.fail_delete_on_call(2);
    let request = WalRetentionProof::snapshot_watermark(CommitVersion::new(1));

    let outcome = truncate_wal(&active, request).expect("truncation");

    assert!(outcome.completed_with_health_debt());
    assert_eq!(outcome.deleted_segments(), 1);
    assert_eq!(outcome.failed_segments(), 1);
    assert_eq!(outcome.protected_segments(), 1);
    assert!(outcome.recovery_health().is_some());
}

#[test]
fn wal_truncation_partial_delete_report_preserves_source_chain() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let config = crate::service::WalServiceConfig::new(1024);
    let _first = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        1,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("first segment");
    let _second = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        2,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("second segment");
    let active = crate::service::WalService::open(
        backend,
        DATABASE_ID,
        3,
        crate::config::mode::DurabilityPolicy::Standard,
        config,
    )
    .expect("active segment");
    backend.fail_delete_on_call(2);
    let request = WalRetentionProof::flush_watermark(CommitVersion::new(1));

    let outcome = truncate_wal(&active, request).expect("truncation");

    assert!(outcome.completed_with_health_debt());
    assert_eq!(outcome.deleted_segments(), 1);
    assert_eq!(outcome.failed_segments(), 1);
    assert_eq!(outcome.protected_segments(), 1);
    assert!(outcome.recovery_health().is_some());
}

#[test]
fn checkpoint_with_truncation_keeps_tail_records_after_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3c);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 12);
    runtime
        .checkpoint(
            &LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(19))
                .expect("request")
                .with_wal_truncation_after_checkpoint(true),
        )
        .expect("checkpoint");
    commit_many_range(&mut runtime, branch, 12, 16, 128);
    let key = dynamic_physical_key(branch, b"rotated-commit-15".to_vec());
    drop(runtime);

    let reopened = open_runtime_with_wal_segment_size(branch, backend, 1024);
    let visible = reopened
        .read_view()
        .expect("read view")
        .latest(&key)
        .expect("read latest")
        .expect("tail row");

    assert_eq!(visible.row().value(), vec![15_u8; 128].as_slice());
    assert_eq!(reopened.visible_version(), CommitVersion::new(16));
}

#[test]
fn checkpoint_with_truncation_records_checkpoint_and_delete_facts() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3d);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(20))
        .expect("request")
        .with_flush_watermark_after_checkpoint(true)
        .with_wal_truncation_after_checkpoint(true);

    let outcome = runtime.checkpoint(&request).expect("checkpoint");
    let truncation = outcome.wal_truncation().expect("truncation");

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(outcome.snapshot_id(), Some(1));
    assert_eq!(
        outcome
            .flush_watermark()
            .expect("flush outcome")
            .persisted_watermark(),
        Some(CommitVersion::new(24))
    );
    assert!(truncation.deleted_segments() > 0);
    assert_eq!(truncation.failed_segments(), 0);
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Completed
    );
}

#[test]
fn cache_checkpoint_task_is_rejected_before_durable_objects() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3e);
    let mut runtime = cache_runtime(branch, backend);
    let error = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect_err("checkpoint task rejected");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(backend.event_count(), 0);
}

#[test]
fn cache_wal_truncation_task_is_rejected_before_durable_objects() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3f);
    let mut runtime = cache_runtime(branch, backend);
    let error = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect_err("truncation task rejected");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(backend.event_count(), 0);
}

#[test]
fn queued_checkpoint_options_control_snapshot_and_log_truncation() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x40);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 1024);
    commit_many_to_rotate(&mut runtime, branch, 24);
    let enqueue = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint_with_options(
            MaintenanceCheckpointOptions::new(Some(9), true),
        ))
        .expect("enqueue");

    let maintenance = runtime
        .run_next_checkpoint_maintenance()
        .expect("run")
        .expect("maintenance");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("database record")
            .snapshot_id(),
        Some(9)
    );
    assert!(backend.delete_calls() > 0);

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("second enqueue");
    runtime
        .run_next_checkpoint_maintenance()
        .expect("run second")
        .expect("second maintenance");
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest after second")
            .snapshot_id(),
        Some(10)
    );
}

#[test]
fn checkpoint_maintenance_task_deferred_when_no_visible_rows() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x37);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("enqueue");

    let maintenance = runtime
        .run_next_checkpoint_maintenance()
        .expect("run")
        .expect("maintenance");

    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(backend.snapshot_objects(), Vec::<ObjectName>::new());
    assert_eq!(runtime.maintenance_status().stats().deferred(), 1);
}

fn cache_runtime(
    branch: BranchId,
    backend: &CheckpointTestBackend,
) -> LifecycleCacheRuntime<CommitManualTimestampSource> {
    LifecycleCacheRuntime::open(
        LifecycleCacheOpenRequest::new(
            StorageOpenPlan::new(
                StorageMode::Cache,
                LifecycleCodecId::identity(),
                RecoveryStrictness::Strict,
                LifecycleConfig::default(),
            )
            .expect("open plan"),
            branch,
            CommitBranchGeneration::new(1).expect("generation"),
        )
        .expect("cache request"),
        backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("cache runtime")
}

fn commit_many_to_rotate(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    count: u8,
) {
    commit_many_range(runtime, branch, 0, count, 128);
    assert!(
        runtime.services().wal().active_segment_id() > 1,
        "test setup must rotate the log"
    );
}

fn commit_many_with_value_len(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    count: u8,
    value_len: usize,
) {
    for index in 0..count {
        runtime
            .execute_durable_commit(
                dynamic_durable_batch(
                    branch,
                    format!("rotated-commit-{index}").into_bytes(),
                    vec![index; value_len],
                ),
                generation_guard(),
            )
            .expect("rotating commit");
    }
}

fn commit_many_range(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    start: u8,
    end: u8,
    value_len: usize,
) {
    for index in start..end {
        runtime
            .execute_durable_commit(
                dynamic_durable_batch(
                    branch,
                    format!("rotated-commit-{index}").into_bytes(),
                    vec![index; value_len],
                ),
                generation_guard(),
            )
            .expect("range commit");
    }
}

fn dynamic_durable_batch(branch: BranchId, user_key: Vec<u8>, value: Vec<u8>) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            dynamic_physical_key(branch, user_key),
            value,
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::new(
            CommitDurabilityMode::Standard,
            CommitConflictValidationMode::Validate,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::RuntimeGenerated,
            CommitOrigin::StorageRuntime,
        ),
    )
}

fn dynamic_physical_key(branch: BranchId, user_key: Vec<u8>) -> PhysicalKey {
    PhysicalKey::new(
        branch,
        "checkpoint",
        StorageSpaceId::engine(0x31).expect("space"),
        user_key,
    )
    .expect("physical key")
}
