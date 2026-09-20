use super::*;
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendError, BackendErrorKind, BackendMetadata,
    BackendRange, BackendResult, BackendWriterGuard, PublishDurability, PublishError, PublishMode,
    PublishOutcome, PublishResult, DURABLE_LOCAL_MODE_REQUIREMENTS,
};
use crate::branch::config::BranchRuntimeConfig;
use crate::commit::{
    CommitBatch, CommitBatchOptions, CommitBranchGeneration, CommitBranchGenerationGuard,
    CommitConflictValidationMode, CommitDuplicateKeyPolicy, CommitDurabilityClass,
    CommitDurabilityMode, CommitExpiry, CommitManualTimestampSource, CommitMutation, CommitOrigin,
    CommitRetentionHint, CommitRuntimeConfig, CommitRuntimeError, CommitStamp, CommitTimelineEntry,
    CommitTimelineRows, CommitTimestampPolicy, CommitUnresolvedDurable, CommitValidationFacts,
};
use crate::format::{
    encode_manifest, encode_wal_record, encode_wal_record_envelope, encode_wal_segment_header,
    DatabaseManifest, WalCommitPayload, WalRecord, WalRecordEnvelope, WalSegmentHeader,
    SNAPSHOT_ROW_SECTION_KIND,
};
use crate::layout::ObjectLayout;
use crate::object::{ObjectName, ObjectPrefix};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::service::{
    SnapshotPublishRequest, SnapshotService, TableObjectService, WalServiceConfig,
};
use crate::table::{ImmutableTableBuilder, TableBuilderConfig, TableIdentity, TableRow};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use strata_core::{BranchId, CommitVersion, Timestamp};

const DATABASE_ID: [u8; 16] = [0x9a; 16];

#[test]
fn recovery_empty_database_returns_healthy_package_without_replay() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x31);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(outcome.health(), &RecoveryHealth::Healthy);
    assert_eq!(outcome.checkpoint().snapshot_id(), None);
    assert_eq!(outcome.checkpoint().trusted_watermark(), None);
    assert_eq!(outcome.checkpoint().section_count(), 0);
    assert_eq!(outcome.checkpoint().row_count(), 0);
    assert!(outcome.checkpoint().install_outcome().is_none());
    assert_eq!(outcome.wal().replay_start(), CommitVersion::ZERO);
    assert_eq!(outcome.wal().record_count(), 0);
    assert!(outcome.wal().truncation().is_none());
    assert!(outcome.wal().repair().is_none());
    assert!(outcome.quarantine().object().is_some());
    assert!(!outcome.quarantine().is_present());
    assert_eq!(outcome.quarantine().byte_count(), 0);
    assert_eq!(outcome.quarantine().entry_count(), 0);
    assert_eq!(outcome.tables().validated_count(), 0);
    assert_eq!(shell.state(), LifecycleState::Recovering);
    assert_eq!(shell.visible_version(), CommitVersion::ZERO);
    assert!(shell.branch_state().is_empty());
    assert!(shell.admit_ordinary_read().is_err());
    assert!(shell.admit_commit().is_err());
}

#[test]
fn bootstrap_empty_recovery_opens_durable_runtime_with_zero_visibility() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new_unseeded());
    let branch = branch_id(0x44);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    let mut runtime = shell
        .complete_recovery(&outcome)
        .expect("bootstrap recovery");

    assert_eq!(runtime.state(), LifecycleState::Open);
    assert_eq!(
        runtime.open_plan().storage_mode(),
        StorageMode::DurableLocalStandard
    );
    assert_eq!(
        runtime.open_outcome().mode(),
        StorageMode::DurableLocalStandard
    );
    assert_eq!(
        runtime.open_outcome().disposition(),
        StorageOpenDisposition::Created
    );
    assert_eq!(
        runtime.open_outcome().recovered_visible_version(),
        Some(CommitVersion::ZERO)
    );
    assert!(runtime.open_outcome().recovery_health().is_healthy());
    assert!(runtime.open_outcome().maintenance_ready());
    assert_eq!(runtime.visible_version(), CommitVersion::ZERO);
    assert_eq!(runtime.bootstrap_report().records_seen(), 0);
    assert_eq!(
        runtime.allocator().version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert!(runtime.branch_state().is_empty());
    assert_eq!(runtime.services().wal().active_segment_id(), 1);
    assert_eq!(runtime.unresolved_durable().expect("gate"), None);
    assert_eq!(runtime.bootstrap_report().gates_cleared(), 0);

    let commit_outcome = runtime
        .execute_durable_commit(
            durable_standard_batch(branch, b"post-open", b"value"),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("post-open durable commit");
    assert_eq!(commit_outcome.commit_version(), Some(CommitVersion::new(1)));
    assert_eq!(runtime.visible_version(), CommitVersion::new(1));
}

#[test]
fn bootstrap_runtime_can_enqueue_and_run_health_collection_maintenance() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new_unseeded());
    let branch = branch_id(0x5f);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let mut runtime = shell
        .complete_recovery(&outcome)
        .expect("bootstrap recovery");

    let enqueue = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::health_collection())
        .expect("enqueue health collection");
    assert!(enqueue.was_enqueued());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);

    let mut runner = MaintenanceTestRunner;
    let maintenance = runtime
        .run_next_maintenance(&mut runner)
        .expect("run maintenance")
        .expect("maintenance outcome");

    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        maintenance.task_kind(),
        MaintenanceTaskKind::HealthCollection
    );
    assert!(maintenance.task_id().is_some());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(runtime.maintenance_status().stats().completed(), 1);
}

#[test]
fn bootstrap_checkpoint_only_recovery_publishes_visible_and_catches_allocator() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x45);
    let checkpoint_row = put_row(branch, 2, b"checkpoint-bootstrap", b"checkpoint-value");
    publish_snapshot(
        backend,
        8,
        CommitVersion::new(3),
        std::slice::from_ref(&checkpoint_row),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(8), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("checkpoint recovery outcome");
    assert_eq!(shell.visible_version(), CommitVersion::ZERO);

    let runtime = shell
        .complete_recovery(&outcome)
        .expect("checkpoint bootstrap");

    assert_eq!(runtime.state(), LifecycleState::Open);
    assert_eq!(runtime.visible_version(), CommitVersion::new(3));
    assert_eq!(
        runtime.open_outcome().recovered_visible_version(),
        Some(CommitVersion::new(3))
    );
    assert_eq!(runtime.bootstrap_report().records_seen(), 0);
    assert!(runtime
        .bootstrap_report()
        .checkpoint_visible_publish()
        .is_some());
    assert_eq!(
        runtime.allocator().version_allocator().last_allocated(),
        CommitVersion::new(3)
    );
    assert_eq!(
        runtime.allocator().timestamp_guard().last_allocated(),
        Some(Timestamp::from_micros(200))
    );
    assert_eq!(runtime.open_outcome().database_id(), Some(&DATABASE_ID));
    assert_eq!(runtime.open_outcome().codec_id(), Some("identity"));
    assert!(runtime.open_outcome().checkpoint().is_some());
    assert!(runtime.open_outcome().wal().is_some());
    assert!(runtime.open_outcome().tables().is_some());
    assert!(runtime.open_outcome().quarantine().is_some());
    assert!(runtime.open_outcome().bootstrap().is_some());
    assert_eq!(runtime.open_outcome().stats().open_attempts(), 1);
    let read_view = runtime.read_view().expect("open read view");
    let visible = read_view
        .latest(checkpoint_row.physical_key())
        .expect("latest")
        .expect("checkpoint row visible after bootstrap");
    assert_eq!(visible.row(), &checkpoint_row);
}

#[test]
fn recovery_ignores_orphan_snapshot_when_manifest_has_no_checkpoint_fact() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x63);
    let orphan_row = put_row(branch, 4, b"orphan-snapshot", b"ignored");
    publish_snapshot(
        backend,
        13,
        CommitVersion::new(4),
        std::slice::from_ref(&orphan_row),
    );
    let orphan_object = ObjectLayout::snapshot(13).expect("orphan snapshot object");
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(outcome.health(), &RecoveryHealth::Healthy);
    assert_eq!(outcome.checkpoint().snapshot_id(), None);
    assert_eq!(outcome.checkpoint().trusted_watermark(), None);
    assert_eq!(outcome.checkpoint().row_count(), 0);
    assert!(outcome.checkpoint().install_outcome().is_none());
    assert!(backend.object_bytes(&orphan_object).is_some());
    assert!(shell.branch_state().is_empty());
    assert!(shell
        .branch_state()
        .capture_read_view()
        .expect("read view")
        .latest(orphan_row.physical_key())
        .expect("orphan read")
        .is_none());
}

#[test]
fn bootstrap_replays_wal_tail_through_commit_runtime() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x46);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 4, b"bootstrap-tail", b"tail-value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append WAL record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("WAL recovery outcome");

    let runtime = shell.complete_recovery(&outcome).expect("WAL bootstrap");

    assert_eq!(runtime.state(), LifecycleState::Open);
    assert_eq!(runtime.visible_version(), CommitVersion::new(4));
    assert_eq!(runtime.bootstrap_report().records_seen(), 1);
    assert_eq!(runtime.bootstrap_report().records_applied(), 1);
    assert_eq!(runtime.bootstrap_report().records_already_applied(), 0);
    assert_eq!(runtime.bootstrap_report().rows_checked(), 3);
    assert_eq!(runtime.bootstrap_report().rows_applied(), 3);
    assert_eq!(
        runtime.allocator().version_allocator().last_allocated(),
        CommitVersion::new(4)
    );
    assert_eq!(
        runtime.allocator().timestamp_guard().last_allocated(),
        Some(Timestamp::from_micros(400))
    );
    let read_view = runtime.read_view().expect("open read view");
    let visible = read_view
        .latest(&physical_key(branch, b"bootstrap-tail"))
        .expect("latest")
        .expect("replayed row visible");
    assert_eq!(visible.row().value(), b"tail-value");
}

#[test]
fn bootstrap_rejects_timeline_only_wal_payload_before_open() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x47);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = timeline_only_wal_record(branch, 5);
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append timeline-only WAL record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("WAL recovery outcome");

    let error = shell
        .complete_recovery(&outcome)
        .expect_err("timeline-only replay rejects");

    assert_commit_runtime_source(
        &error,
        &CommitRuntimeError::InvalidCommitState {
            reason: "replay payload is missing user mutation rows",
        },
    );
}

#[test]
fn bootstrap_accepts_log_record_without_timeline_rows_before_open() {
    // W3.1c: commits no longer materialize timeline rows — a user-only WAL
    // record is the normal shape and replays cleanly at open (the record's
    // stamp is the timeline fact, observed into the retained index).
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x48);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = user_only_wal_record(branch, 5, b"missing-timeline", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append user-only WAL record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("WAL recovery outcome");

    let reopened = shell
        .complete_recovery(&outcome)
        .expect("stamp-only record replays and opens");
    assert_eq!(
        reopened.branch_state().max_commit_version(),
        Some(CommitVersion::new(5))
    );
}

/// W3.1c: corrupt legacy timeline rows are decoded in exactly one place now —
/// the recovery-time completeness bridge — and fail the open closed instead
/// of seeding a wrong index. (Pre-W3.1c this surfaced at read time; the two
/// api-level corruption tests moved here with the decode path.)
#[test]
fn timeline_bridge_rejects_corrupt_legacy_timeline_rows() {
    let branch = branch_id(0x52);
    let mut state = crate::branch::state::BranchLocalState::new(
        branch,
        crate::branch::config::BranchRuntimeConfig::default(),
    )
    .expect("state");
    let bad_key = PhysicalKey::new(
        branch,
        crate::commit::COMMIT_TIMELINE_SPACE,
        StorageSpaceId::COMMIT_TIMELINE,
        b"ts-v1\0short".to_vec(),
    )
    .expect("timeline key");
    state
        .append_committed_row(StorageRow::put(
            bad_key,
            CommitVersion::new(99),
            Timestamp::from_micros(99),
            Timestamp::EPOCH,
            99_u64.to_be_bytes().to_vec(),
        ))
        .expect("append corrupt legacy timeline row");
    // The index never saw a completeness seed, so the bridge must scan — and
    // the scan's decode fails closed on the malformed key.
    let error = crate::lifecycle::recovery::ensure_branch_timeline_complete(&state)
        .expect_err("corrupt legacy timeline row fails the bridge closed");
    assert!(matches!(
        error,
        LifecycleError::LowerLayer { .. } | LifecycleError::TimelineRecoveryMismatch { .. }
    ));
}

#[test]
fn bootstrap_rejects_recovered_log_record_for_unopened_branch() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x49);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch_id(0x4a), 3, b"foreign-branch", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append foreign branch record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        std::slice::from_ref(&record)
    );
    assert_eq!(
        shell
            .complete_recovery(&outcome)
            .expect_err("foreign branch recovered record rejects"),
        LifecycleError::RecoveryFailed {
            reason: "recovered WAL package references an unknown branch",
        }
    );
}

/// #3319 S3b at the storage level (the mutation gate judges storage mutants
/// with storage tests only): a budgeted replay of a WAL several times the
/// rotation threshold must DRAIN the replayed backlog to durable L0 tables
/// mid-bootstrap — not accumulate it as memtables — and the recovered
/// branch must serve every replayed row. This observes the whole
/// `flush_replayed_state_if_over_threshold` seam: the half-threshold
/// pre-rotation (a full-threshold seal overflows the artifact pool and
/// fails the open), the drain trigger, and the fail-closed error path (a
/// drain failure aborts bootstrap, which this test's success shape pins in
/// reverse).
#[test]
fn budgeted_replay_drains_the_backlog_to_durable_tables() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4e);
    let budget = StorageRuntimeBudget::from_total_bytes(16 * 1024 * 1024).expect("budget");
    let branch_config = branch_config_with_storage_budget(BranchRuntimeConfig::default(), budget)
        .expect("budget branch config");
    let rotation_bytes = branch_config.active_rotation_bytes();
    let mut shell = assemble_shell_with_branch_config(
        open_plan_with_budget(RecoveryStrictness::Strict, budget),
        branch,
        backend,
        branch_config,
    )
    .expect("budgeted durable shell");
    // A replayed working set several times the rotation threshold.
    let record_count: u64 = 64;
    let value: &'static [u8] = Box::leak(vec![b'x'; 96 * 1024].into_boxed_slice());
    for version in 1..=record_count {
        let key: &'static [u8] =
            Box::leak(format!("bulk-{version:03}").into_bytes().into_boxed_slice());
        let record = wal_record(branch, version, key, value);
        shell
            .services_mut()
            .wal_mut()
            .append(&record)
            .expect("append bulk record");
    }
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    let runtime = shell.complete_recovery(&outcome).expect("budgeted bootstrap");

    let state = runtime
        .branch_catalog()
        .branch_state(branch)
        .expect("branch state");
    assert!(
        state.owned_table_count() >= 2,
        "the replayed backlog must drain to durable L0 tables (got {})",
        state.owned_table_count()
    );
    assert_eq!(
        state.frozen_table_count(),
        0,
        "nothing stays frozen once the drain runs"
    );
    assert!(
        state.active().approximate_size_bytes() < rotation_bytes,
        "the residual active memtable stays under the rotation threshold"
    );
    let view = state.capture_read_view().expect("read view");
    for version in [1, record_count / 2, record_count] {
        let key = format!("bulk-{version:03}");
        let row = view
            .latest(&physical_key(branch, Box::leak(key.into_bytes().into_boxed_slice())))
            .expect("read")
            .unwrap_or_else(|| panic!("replayed row {version} lost by the budgeted replay"));
        assert_eq!(row.row().value(), value, "row {version} value intact");
    }
}

/// #2567 S3a (#3319): the streamed bootstrap replay HONORS the contiguity
/// fence — records above `replay_ceiling` are the orphaned tail recovery
/// deliberately drops (recovering past a lost table-manifest base), and they
/// must neither be counted nor reach the branch state. The fence is lowered
/// directly (the test seam) so enforcement is observable without fabricating
/// a whole orphaned-delta store.
#[test]
fn bootstrap_replay_honors_the_contiguity_ceiling() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4c);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    for (version, key) in [
        (1, b"first".as_slice()),
        (2, b"second"),
        (3, b"above-fence"),
    ] {
        let record = wal_record(branch, version, key, b"value");
        shell
            .services_mut()
            .wal_mut()
            .append(&record)
            .expect("append record");
    }
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let mut outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    outcome
        .wal_mut_for_test()
        .set_replay_ceiling_for_test(Some(CommitVersion::new(2)));

    assert_eq!(
        outcome.wal().record_count(),
        3,
        "the count is pre-fence: every record above replay_start was visited"
    );
    let replayable = outcome
        .wal()
        .collect_replay_records_for_test(shell.services().wal())
        .expect("collect replay records");
    assert_eq!(
        replayable
            .iter()
            .map(|record| record.commit_version().as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2],
        "the collector mirrors the fence bootstrap enforces"
    );
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    assert_eq!(
        runtime.bootstrap_report().records_seen(),
        2,
        "the record above the fence must never reach replay"
    );
    let view = runtime
        .branch_catalog()
        .branch_state(branch)
        .expect("branch state")
        .capture_read_view()
        .expect("read view");
    assert!(
        view.latest(&physical_key(branch, b"second"))
            .expect("read second")
            .is_some(),
        "records at or below the fence replay"
    );
    assert!(
        view.latest(&physical_key(branch, b"above-fence"))
            .expect("read above-fence")
            .is_none(),
        "the orphaned tail above the fence must not be replayed"
    );
}

#[test]
fn bootstrap_rejects_recovered_log_records_not_strictly_ordered() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4b);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let newer = wal_record(branch, 5, b"newer", b"value");
    let older = wal_record(branch, 4, b"older", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&newer)
        .expect("append newer record");
    shell
        .services_mut()
        .wal_mut()
        .append(&older)
        .expect("append older record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        &[newer, older]
    );
    assert_eq!(
        shell
            .complete_recovery(&outcome)
            .expect_err("non-increasing recovered record package rejects"),
        LifecycleError::RecoveryFailed {
            reason: "recovered WAL package must be strictly ordered",
        }
    );
}

#[test]
fn bootstrap_rejects_recovered_log_records_with_duplicate_commit_versions() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x60);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let first = wal_record(branch, 5, b"duplicate-first", b"value");
    let second = wal_record(branch, 5, b"duplicate-second", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&first)
        .expect("append first record");
    shell
        .services_mut()
        .wal_mut()
        .append(&second)
        .expect("append duplicate-version record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        &[first, second]
    );
    assert_eq!(
        shell
            .complete_recovery(&outcome)
            .expect_err("duplicate recovered record package rejects"),
        LifecycleError::RecoveryFailed {
            reason: "recovered WAL package must be strictly ordered",
        }
    );
}

#[test]
fn bootstrap_preserves_degraded_recovery_health_while_replaying_tail() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4c);
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(9), Some(7), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(lossy_open_plan(), branch, backend).expect("durable shell");
    let replayed = wal_record(branch, 5, b"degraded-tail", b"tail-value");
    shell
        .services_mut()
        .wal_mut()
        .append(&replayed)
        .expect("append replayed record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("degraded recovery outcome");
    assert!(matches!(
        outcome.health(),
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            faults
        } if faults.iter().any(|fault| fault.kind() == RecoveryFaultKind::MissingSnapshotObject)
    ));

    let runtime = shell
        .complete_recovery(&outcome)
        .expect("degraded recovery opens");

    assert_eq!(runtime.state(), LifecycleState::Open);
    assert_eq!(runtime.visible_version(), CommitVersion::new(5));
    assert_eq!(runtime.bootstrap_report().records_seen(), 1);
    assert_eq!(runtime.bootstrap_report().records_applied(), 1);
    assert_eq!(
        runtime.open_outcome().recovery_health(),
        runtime.bootstrap_report().recovery_health(),
    );
    assert!(matches!(
        runtime.open_outcome().recovery_health(),
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            faults
        } if faults.iter().any(|fault| fault.kind() == RecoveryFaultKind::MissingSnapshotObject)
    ));
}

#[test]
fn bootstrap_replay_is_idempotent_for_exactly_installed_rows() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4d);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 4, b"already-installed", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append WAL record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    shell
        .branch_state_mut()
        .append_committed_rows_atomically(record.commit_payload().rows().to_vec())
        .expect("seed exact installed rows");

    let runtime = shell
        .complete_recovery(&outcome)
        .expect("idempotent bootstrap");

    assert_eq!(runtime.visible_version(), CommitVersion::new(4));
    assert_eq!(runtime.bootstrap_report().records_seen(), 1);
    assert_eq!(runtime.bootstrap_report().records_applied(), 0);
    assert_eq!(runtime.bootstrap_report().records_already_applied(), 1);
    assert_eq!(runtime.bootstrap_report().rows_checked(), 3);
    assert_eq!(runtime.bootstrap_report().rows_applied(), 0);
}

#[test]
fn bootstrap_replay_clears_matching_unresolved_durable_gate() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4e);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 6, b"gate-cleared", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append WAL record");
    let stamp = CommitStamp::new(branch, CommitVersion::new(6), Timestamp::from_micros(600))
        .expect("stamp");
    let unresolved = CommitUnresolvedDurable::durable_not_applied_with_facts(
        stamp,
        CommitDurabilityClass::Standard,
        "seeded unresolved durable fact",
    )
    .expect("unresolved durable");
    shell
        .durable_gate()
        .record_unresolved(unresolved)
        .expect("seed unresolved gate");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    let runtime = shell.complete_recovery(&outcome).expect("gate bootstrap");

    assert_eq!(runtime.bootstrap_report().records_seen(), 1);
    assert_eq!(runtime.bootstrap_report().records_applied(), 1);
    assert_eq!(runtime.bootstrap_report().gates_cleared(), 1);
    assert_eq!(runtime.unresolved_durable().expect("gate state"), None);
    assert_eq!(runtime.visible_version(), CommitVersion::new(6));
}

#[test]
fn bootstrap_replay_uses_always_durability_for_always_mode() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x50);
    let mut shell = assemble_shell(
        open_plan_for_mode(StorageMode::DurableLocalAlways, RecoveryStrictness::Strict),
        branch,
        backend,
    )
    .expect("durable shell");
    let record = wal_record(branch, 6, b"always-gate-cleared", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append WAL record");
    let stamp = CommitStamp::new(branch, CommitVersion::new(6), Timestamp::from_micros(600))
        .expect("stamp");
    let unresolved = CommitUnresolvedDurable::durable_not_applied_with_facts(
        stamp,
        CommitDurabilityClass::Always,
        "seeded always unresolved durable fact",
    )
    .expect("unresolved durable");
    shell
        .durable_gate()
        .record_unresolved(unresolved)
        .expect("seed unresolved gate");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    let runtime = shell.complete_recovery(&outcome).expect("always bootstrap");

    assert_eq!(
        runtime.open_outcome().mode(),
        StorageMode::DurableLocalAlways
    );
    assert_eq!(runtime.bootstrap_report().records_seen(), 1);
    assert_eq!(runtime.bootstrap_report().records_applied(), 1);
    assert_eq!(runtime.bootstrap_report().gates_cleared(), 1);
    assert_eq!(runtime.unresolved_durable().expect("gate state"), None);
    assert_eq!(runtime.visible_version(), CommitVersion::new(6));
}

#[test]
fn bootstrap_replay_rejects_mismatched_unresolved_durable_gate() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4f);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 6, b"blocked-by-gate", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append WAL record");
    let mismatched_stamp =
        CommitStamp::new(branch, CommitVersion::new(7), Timestamp::from_micros(700))
            .expect("stamp");
    let unresolved = CommitUnresolvedDurable::durable_not_applied_with_facts(
        mismatched_stamp,
        CommitDurabilityClass::Standard,
        "different unresolved durable fact",
    )
    .expect("unresolved durable");
    shell
        .durable_gate()
        .record_unresolved(unresolved)
        .expect("seed unresolved gate");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    let error = shell
        .try_bootstrap_commit_runtime_for_test(&outcome)
        .expect_err("mismatched gate blocks replay");

    assert_commit_runtime_source(
        &error,
        &CommitRuntimeError::UnresolvedDurableCommit {
            branch_id: branch,
            commit_version: CommitVersion::new(7),
            reason: "different unresolved durable commit blocks replay",
        },
    );
    assert_eq!(shell.state(), LifecycleState::Failed);
    assert_eq!(shell.visible_version(), CommitVersion::ZERO);
    assert!(shell.branch_state().is_empty());
    assert_eq!(
        shell.unresolved_durable().expect("gate state"),
        Some(unresolved)
    );
    assert!(shell.admit_commit().is_err());
    assert!(shell.admit_recovery_step().is_err());
}

#[test]
fn recovery_loads_checkpoint_installs_rows_and_packages_only_wal_tail() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x32);
    let checkpoint_row = put_row(branch, 3, b"checkpoint", b"checkpoint-value");
    publish_snapshot(
        backend,
        5,
        CommitVersion::new(3),
        std::slice::from_ref(&checkpoint_row),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            // Full self-contained snapshot at watermark 3 (no flush base): `flushed_through`
            // stays None. The WAL record at commit 2 is superseded by the snapshot watermark,
            // not by a flushed table-manifest base. Recovery of a delta checkpoint over a
            // table-manifest base (where `flushed_through` is set) is covered by the
            // flush-watermark tests; a set `flushed_through` with the base absent is the
            // orphaned-delta case covered by the SplitRename regression.
            .with_recovery_facts(1, Some(3), Some(5), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let skipped = wal_record(branch, 2, b"skipped", b"old");
    let replayed = wal_record(branch, 4, b"tail", b"tail-value");
    shell
        .services_mut()
        .wal_mut()
        .append(&skipped)
        .expect("append skipped record");
    shell
        .services_mut()
        .wal_mut()
        .append(&replayed)
        .expect("append replayed record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(outcome.health(), &RecoveryHealth::Healthy);
    assert_eq!(outcome.checkpoint().snapshot_id(), Some(5));
    assert_eq!(
        outcome.checkpoint().trusted_watermark(),
        Some(CommitVersion::new(3))
    );
    assert_eq!(outcome.checkpoint().section_count(), 1);
    assert_eq!(outcome.checkpoint().row_count(), 1);
    assert!(outcome.checkpoint().install_outcome().is_some());
    assert_eq!(outcome.wal().replay_start(), CommitVersion::new(3));
    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        std::slice::from_ref(&replayed)
    );
    assert!(outcome.wal().truncation().is_none());
    assert!(outcome.wal().repair().is_none());

    let view = shell.branch_state().capture_read_view().expect("read view");
    let visible = view
        .latest(checkpoint_row.physical_key())
        .expect("latest")
        .expect("visible checkpoint row");
    assert_eq!(visible.row(), &checkpoint_row);
    assert_eq!(shell.visible_version(), CommitVersion::ZERO);
}

#[test]
fn recovery_keeps_checkpoint_covered_wal_segment_without_replay_or_cleanup() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x61);
    let checkpoint_row = put_row(branch, 3, b"checkpointed", b"checkpoint-value");
    publish_snapshot(
        backend,
        12,
        CommitVersion::new(3),
        std::slice::from_ref(&checkpoint_row),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(2, Some(3), Some(12), None)
            .expect("database root facts"),
    );
    let covered = wal_record(branch, 2, b"covered-wal", b"old");
    let replayed = wal_record(branch, 4, b"active-wal", b"tail");
    let covered_object = ObjectLayout::wal_segment(1).expect("covered WAL object");
    let active_object = ObjectLayout::wal_segment(2).expect("active WAL object");
    backend.write_raw(covered_object.clone(), wal_segment_bytes(1, &[covered]));
    backend.write_raw(
        active_object,
        wal_segment_bytes(2, std::slice::from_ref(&replayed)),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(outcome.health(), &RecoveryHealth::Healthy);
    assert_eq!(outcome.wal().replay_start(), CommitVersion::new(3));
    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        std::slice::from_ref(&replayed)
    );
    assert!(outcome.wal().truncation().is_none());
    assert!(outcome.wal().repair().is_none());
    assert!(
        backend.object_bytes(&covered_object).is_some(),
        "recovery must not delete checkpoint-covered WAL segments"
    );
}

#[test]
fn recovery_does_not_install_checkpoint_when_later_wal_read_fails() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x37);
    let checkpoint_row = put_row(branch, 3, b"checkpoint", b"checkpoint-value");
    publish_snapshot(
        backend,
        5,
        CommitVersion::new(3),
        std::slice::from_ref(&checkpoint_row),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(5), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    backend.fail_read_object(ObjectLayout::wal_segment(1).expect("active log object"));
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("log read failure rejects");

    assert!(matches!(
        error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "WAL recovery read failed",
            ..
        }
    ));
    assert!(error.source().is_some());
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_repairs_latest_partial_log_tail_with_data_loss_fault_when_lossy() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x40);
    let mut shell = assemble_shell(lossy_open_plan(), branch, backend).expect("durable shell");
    let record = wal_record(branch, 2, b"valid", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append valid record");
    let wal_object = ObjectLayout::wal_segment(1).expect("active log object");
    let valid_end = backend
        .object_bytes(&wal_object)
        .expect("valid WAL bytes")
        .len() as u64;
    let partial = b"partial";
    backend.append_raw(wal_object.clone(), partial);
    let object_size = valid_end + partial.len() as u64;
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("partial tail recovery");

    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        std::slice::from_ref(&record)
    );
    let truncation = outcome.wal().truncation().expect("truncation fact");
    assert_eq!(truncation.segment_id(), 1);
    assert_eq!(truncation.valid_end_offset(), valid_end);
    assert_eq!(truncation.object_size(), object_size);
    let repair = outcome.wal().repair().expect("repair fact");
    assert_eq!(repair.segment_id(), 1);
    assert_eq!(repair.valid_end_offset(), valid_end);
    assert_eq!(repair.removed_bytes(), partial.len() as u64);
    assert_eq!(
        backend
            .object_bytes(&wal_object)
            .expect("repaired WAL bytes")
            .len() as u64,
        valid_end
    );
    assert!(matches!(
        outcome.health(),
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            faults
        } if faults.iter().any(|fault| fault.kind() == RecoveryFaultKind::WalTailRepairFailed)
    ));
}

#[test]
fn recovery_rejects_non_latest_partial_wal_tail_as_corruption() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x62);
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(2, None, None, None)
            .expect("database root facts"),
    );
    let first = wal_record(branch, 2, b"non-latest-valid", b"value");
    let first_object = ObjectLayout::wal_segment(1).expect("first WAL object");
    let second_object = ObjectLayout::wal_segment(2).expect("second WAL object");
    let mut partial_first = wal_segment_bytes(1, std::slice::from_ref(&first));
    partial_first.extend_from_slice(b"partial");
    backend.write_raw(first_object.clone(), partial_first.clone());
    backend.write_raw(second_object, wal_segment_bytes(2, &[]));
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("non-latest partial tail must fail closed");

    // A partial tail on a non-latest segment is unrepairable durable corruption
    // (only the latest segment may carry a torn tail), so recovery refuses with
    // a permanent, non-retryable corruption error rather than a transient
    // lower-layer read failure.
    assert!(matches!(error, LifecycleError::RecoveryCorruption { .. }));
    assert_eq!(error.code(), "corruption.lifecycle.recovery_corruption");
    assert_eq!(
        backend
            .object_bytes(&first_object)
            .expect("first WAL bytes"),
        partial_first,
        "non-latest partial tails must not be repaired during recovery"
    );
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_rejects_checkpoint_row_newer_than_snapshot_watermark() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x38);
    let checkpoint_row = put_row(branch, 4, b"too-new", b"value");
    publish_snapshot(
        backend,
        6,
        CommitVersion::new(3),
        std::slice::from_ref(&checkpoint_row),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(6), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    assert_eq!(
        LifecycleRecoveryRuntime::new(&mut shell).recover(&request),
        Err(LifecycleError::RecoveryFailed {
            reason: "checkpoint row commit version exceeds snapshot watermark",
        })
    );
    assert!(shell.branch_state().is_empty());
}

#[test]
fn database_manifest_rejects_zero_snapshot_id_before_recovery() {
    assert!(
        DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(0), None)
            .is_err(),
        "zero snapshot ids must be rejected before lifecycle trusts manifest recovery facts"
    );
}

#[test]
fn recovery_rejects_snapshot_section_count_above_request_limit() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4a);
    let first = put_row(branch, 2, b"first-section", b"value");
    let second = put_row(branch, 3, b"second-section", b"value");
    SnapshotService::new(backend)
        .publish_create(SnapshotPublishRequest::new(
            10,
            CommitVersion::new(3),
            Timestamp::from_micros(7_000),
            DATABASE_ID,
            "identity",
            vec![
                encode_checkpoint_row_section(std::slice::from_ref(&first))
                    .expect("first row section"),
                encode_checkpoint_row_section(std::slice::from_ref(&second))
                    .expect("second row section"),
            ],
        ))
        .expect("publish multi-section snapshot");
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(10), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request = LifecycleRecoveryRequest::new(
        RecoveryStrictness::Strict,
        shell.open_plan().lifecycle_config().max_recovery_faults(),
        1,
        "limited-section-checkpoint",
    )
    .expect("recovery request");

    assert_eq!(
        LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect_err("too many snapshot sections reject"),
        LifecycleError::RecoveryFailed {
            reason: "snapshot section count exceeds lifecycle recovery limit",
        }
    );
}

#[test]
fn manifest_decode_rejects_large_section_count_before_allocation() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x4b);
    let alpha = put_row(branch, 4, b"section-alpha", b"value");
    let beta = put_row(branch, 5, b"section-beta", b"value");
    let gamma = put_row(branch, 6, b"section-gamma", b"value");
    SnapshotService::new(backend)
        .publish_create(SnapshotPublishRequest::new(
            11,
            CommitVersion::new(6),
            Timestamp::from_micros(7_000),
            DATABASE_ID,
            "identity",
            vec![
                encode_checkpoint_row_section(std::slice::from_ref(&alpha)).expect("alpha section"),
                encode_checkpoint_row_section(std::slice::from_ref(&beta)).expect("beta section"),
                encode_checkpoint_row_section(std::slice::from_ref(&gamma)).expect("gamma section"),
            ],
        ))
        .expect("publish multi-section snapshot");
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(6), Some(11), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request = LifecycleRecoveryRequest::new(
        RecoveryStrictness::Strict,
        shell.open_plan().lifecycle_config().max_recovery_faults(),
        2,
        "bounded-sections",
    )
    .expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("section count over limit must reject before row decode");

    assert_eq!(
        error,
        LifecycleError::RecoveryFailed {
            reason: "snapshot section count exceeds lifecycle recovery limit",
        }
    );
    // No checkpoint rows were decoded into branch state — the count check ran
    // before `decode_checkpoint_rows` allocated any row vector.
    assert!(shell.branch_state().is_empty());
    assert_eq!(shell.visible_version(), CommitVersion::ZERO);
}

#[test]
fn recovery_rejects_checkpoint_rows_for_unknown_branch() {
    // Inject a checkpoint whose rows reference a branch_id that is not in
    // the rebuilt catalog. Decode partitions the row out as a non-seeded
    // row; `complete_recovery` rejects it post-catalog-build because no
    // catalog entry exists for the referenced branch.
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x39);
    let other_branch_row = put_row(branch_id(0x3a), 3, b"other", b"value");
    publish_snapshot(
        backend,
        7,
        CommitVersion::new(3),
        std::slice::from_ref(&other_branch_row),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(7), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery decodes the checkpoint");
    assert_eq!(outcome.checkpoint().non_seeded_rows().len(), 1);
    assert_eq!(
        shell
            .complete_recovery(&outcome)
            .expect_err("checkpoint row for unknown branch must reject"),
        LifecycleError::RecoveryFailed {
            reason: "checkpoint references an unknown branch",
        }
    );
}

#[test]
fn recovery_rejects_flush_watermark_without_recovered_table_state() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x3b);
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, None, None, Some(CommitVersion::new(4)))
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    assert_eq!(
        LifecycleRecoveryRuntime::new(&mut shell).recover(&request),
        Err(LifecycleError::RecoveryFailed {
            reason: "manifest flush watermark requires recovered flushed table state",
        })
    );
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_rejects_ad_hoc_table_object_references() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x42);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .expect("recovery request")
        .with_table_object_references(1);

    assert_eq!(
        LifecycleRecoveryRuntime::new(&mut shell).recover(&request),
        Err(LifecycleError::RecoveryFailed {
            reason: "table object recovery references require a table manifest",
        })
    );
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_rejects_table_object_references_without_manifest() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x3d);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .expect("recovery request")
        .with_table_object_references(1);

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("missing table rejects");

    assert_eq!(
        error,
        LifecycleError::RecoveryFailed {
            reason: "table object recovery references require a table manifest",
        }
    );
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_rejects_table_references_before_wal_tail_repair() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x43);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 3, b"valid", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append valid record");
    let wal_object = ObjectLayout::wal_segment(1).expect("active log object");
    backend.append_raw(wal_object.clone(), b"partial");
    let before = backend.object_bytes(&wal_object).expect("wal bytes before");
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .expect("recovery request")
        .with_table_object_references(1);

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("missing table rejects before WAL repair");

    assert_eq!(
        error,
        LifecycleError::RecoveryFailed {
            reason: "table object recovery references require a table manifest",
        }
    );
    assert_eq!(
        backend.object_bytes(&wal_object).expect("wal bytes after"),
        before,
        "WAL tail must not be repaired after table validation already failed",
    );
}

#[test]
fn recovery_validates_quarantine_before_wal_tail_repair() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x49);
    let mut shell = assemble_shell(lossy_open_plan(), branch, backend).expect("durable shell");
    let record = wal_record(branch, 3, b"valid-before-quarantine", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append valid record");
    let wal_object = ObjectLayout::wal_segment(1).expect("active log object");
    backend.append_raw(wal_object.clone(), b"partial");
    let before = backend.object_bytes(&wal_object).expect("wal bytes before");
    backend.fail_read_object(
        ObjectLayout::quarantine_manifest(&branch.to_string()).expect("quarantine object"),
    );
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("quarantine failure rejects before WAL repair");

    assert!(matches!(
        error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "quarantine inventory recovery failed",
            ..
        }
    ));
    assert_eq!(
        backend.object_bytes(&wal_object).expect("wal bytes after"),
        before,
        "WAL tail must not be repaired after quarantine recovery already failed",
    );
}

#[test]
fn recovery_degrades_quarantine_inventory_mismatch_only_when_explicitly_lossy() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x3e);
    backend.write_raw(
        ObjectLayout::quarantine_manifest(&branch.to_string()).expect("inventory object"),
        b"not inventory".to_vec(),
    );
    let mut strict_shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("strict shell");
    let strict_request =
        LifecycleRecoveryRequest::from_open_plan(strict_shell.open_plan()).expect("strict request");
    let strict_error = LifecycleRecoveryRuntime::new(&mut strict_shell)
        .recover(&strict_request)
        .expect_err("strict inventory mismatch rejects");
    assert!(matches!(
        strict_error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "quarantine inventory recovery failed",
            ..
        }
    ));
    drop(strict_shell);

    let mut lossy_shell = assemble_shell(lossy_open_plan(), branch, backend).expect("lossy shell");
    let lossy_request =
        LifecycleRecoveryRequest::from_open_plan(lossy_shell.open_plan()).expect("lossy request");
    let outcome = LifecycleRecoveryRuntime::new(&mut lossy_shell)
        .recover(&lossy_request)
        .expect("lossy inventory mismatch degrades");

    assert!(matches!(
        outcome.health(),
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::Telemetry,
            faults
        } if faults.iter().any(
            |fault| fault.kind() == RecoveryFaultKind::QuarantineInventoryMismatch
        )
    ));
    assert_eq!(
        outcome.quarantine().object(),
        Some(&ObjectLayout::quarantine_manifest(&branch.to_string()).expect("quarantine object"))
    );
    assert!(!outcome.quarantine().is_present());
    assert_eq!(outcome.quarantine().byte_count(), 0);
    assert_eq!(outcome.quarantine().entry_count(), 0);
}

#[test]
fn recovery_rejects_missing_snapshot_in_strict_mode() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x33);
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(9), Some(6), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("strict missing snapshot rejects");

    // #2754: a manifest that attests a snapshot whose objects are gone is
    // permanent loss under strict recovery — a non-retryable recovery
    // corruption, not the transient lower-layer outage that advised an endless
    // retry. The lossy direction control
    // (`bootstrap_preserves_degraded_recovery_health_while_replaying_tail`)
    // still degrades instead of refusing.
    assert_eq!(
        error,
        LifecycleError::recovery_corruption(
            "manifest attests a snapshot but its objects are missing"
        )
    );
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_rejects_corrupt_manifest_listed_snapshot_without_installing_rows() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x64);
    let snapshot_object = ObjectLayout::snapshot(14).expect("snapshot object");
    backend.write_raw(snapshot_object.clone(), b"not a snapshot".to_vec());
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(4), Some(14), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("corrupt snapshot rejects");

    assert!(matches!(
        error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "snapshot decode failed",
            ..
        }
    ));
    assert!(error.source().is_some());
    assert_eq!(
        backend
            .object_bytes(&snapshot_object)
            .expect("corrupt snapshot bytes"),
        b"not a snapshot"
    );
    assert!(shell.branch_state().is_empty());
    assert_eq!(shell.visible_version(), CommitVersion::ZERO);
}

#[test]
fn recovery_allows_explicit_lossy_missing_snapshot_without_trusting_watermark() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x34);
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(9), Some(7), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(lossy_open_plan(), branch, backend).expect("durable shell");
    let replayed = wal_record(branch, 5, b"lossy-tail", b"tail-value");
    shell
        .services_mut()
        .wal_mut()
        .append(&replayed)
        .expect("append replayed record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    assert_eq!(
        request.strictness(),
        RecoveryStrictness::AllowExplicitLossyFallback
    );
    assert!(request.max_faults() > 0);
    assert!(request.max_snapshot_sections() > 0);

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("lossy recovery outcome");

    assert!(matches!(
        outcome.health(),
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            faults
        } if faults.iter().any(|fault| fault.kind() == RecoveryFaultKind::MissingSnapshotObject)
    ));
    assert_eq!(outcome.checkpoint().snapshot_id(), Some(7));
    assert_eq!(outcome.checkpoint().trusted_watermark(), None);
    assert_eq!(outcome.wal().replay_start(), CommitVersion::ZERO);
    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        &[replayed]
    );
}

#[test]
fn lossy_missing_snapshot_allows_uncertain_flush_watermark_as_degraded_data_loss() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x36);
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(9), Some(7), Some(CommitVersion::new(6)))
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(lossy_open_plan(), branch, backend).expect("durable shell");
    let replayed = wal_record(branch, 5, b"lossy-flush-tail", b"tail-value");
    shell
        .services_mut()
        .wal_mut()
        .append(&replayed)
        .expect("append replayed record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("lossy recovery with missing checkpoint");

    assert!(matches!(
        outcome.health(),
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            faults
        } if faults.iter().any(|fault| fault.kind() == RecoveryFaultKind::MissingSnapshotObject)
    ));
    assert_eq!(outcome.checkpoint().snapshot_id(), Some(7));
    assert_eq!(outcome.checkpoint().trusted_watermark(), None);
    assert_eq!(outcome.wal().replay_start(), CommitVersion::ZERO);
    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records"),
        &[replayed]
    );
}

#[test]
fn recovery_request_rejects_lossy_when_open_plan_is_strict() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x35);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request = LifecycleRecoveryRequest::new(
        RecoveryStrictness::AllowExplicitLossyFallback,
        1,
        1,
        "lossy-request",
    )
    .expect("request");

    assert_eq!(
        LifecycleRecoveryRuntime::new(&mut shell).recover(&request),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "lossy recovery request requires lossy open plan",
        })
    );
}

#[test]
fn recovery_request_validates_limits_and_checkpoint_identity() {
    assert_eq!(
        LifecycleRecoveryRequest::new(RecoveryStrictness::Strict, 0, 1, "valid"),
        Err(LifecycleError::InvalidConfig {
            field: "max_faults",
            reason: "must be nonzero",
        })
    );
    assert_eq!(
        LifecycleRecoveryRequest::new(RecoveryStrictness::Strict, 1, 0, "valid"),
        Err(LifecycleError::InvalidConfig {
            field: "max_snapshot_sections",
            reason: "must be nonzero",
        })
    );
    assert!(matches!(
        LifecycleRecoveryRequest::new(RecoveryStrictness::Strict, 1, 1, ""),
        Err(LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::TableRuntime,
            reason: "invalid recovery table identity",
            ..
        })
    ));
}

#[test]
fn checkpoint_row_section_round_trips_and_rejects_trailing_bytes() {
    let row = put_row(branch_id(0x36), 11, b"section", b"value");
    let section =
        encode_checkpoint_row_section(std::slice::from_ref(&row)).expect("row checkpoint section");
    assert_eq!(section.section_kind(), SNAPSHOT_ROW_SECTION_KIND);
    let container = crate::format::SnapshotContainer::new(
        crate::format::SnapshotHeader::new(
            8,
            CommitVersion::new(11),
            Timestamp::from_micros(1_100),
            DATABASE_ID,
            "identity",
        )
        .expect("header"),
        vec![section.clone()],
    );
    assert_eq!(container.sections()[0], section);

    let mut trailing = section.payload().to_vec();
    trailing.push(0);
    let invalid = crate::format::SnapshotSection::new(SNAPSHOT_ROW_SECTION_KIND, trailing)
        .expect("invalid row section shape");
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x36);
    let snapshot = crate::format::SnapshotContainer::new(
        crate::format::SnapshotHeader::new(
            8,
            CommitVersion::new(11),
            Timestamp::from_micros(1_100),
            DATABASE_ID,
            "identity",
        )
        .expect("header"),
        vec![invalid],
    );
    let snapshot_bytes = crate::format::encode_snapshot_container(&snapshot).expect("snapshot");
    backend.write_raw(ObjectLayout::snapshot(8).expect("snapshot"), snapshot_bytes);
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(11), Some(8), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("trailing row section rejects");
    // A malformed snapshot section is permanent durable corruption, not a
    // transient lower-layer read failure; recovery refuses with a non-retryable
    // corruption error that still preserves the underlying decode source.
    assert!(matches!(error, LifecycleError::RecoveryCorruption { .. }));
    assert!(error.source().is_some());
}

#[test]
fn checkpoint_row_section_rejects_declared_rows_without_length_prefixes() {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"STRR");
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(&u32::MAX.to_le_bytes());
    let invalid = crate::format::SnapshotSection::new(SNAPSHOT_ROW_SECTION_KIND, payload)
        .expect("invalid row section shape");
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x3c);
    let snapshot = crate::format::SnapshotContainer::new(
        crate::format::SnapshotHeader::new(
            9,
            CommitVersion::new(11),
            Timestamp::from_micros(1_100),
            DATABASE_ID,
            "identity",
        )
        .expect("header"),
        vec![invalid],
    );
    let snapshot_bytes = crate::format::encode_snapshot_container(&snapshot).expect("snapshot");
    backend.write_raw(
        ObjectLayout::snapshot(9).expect("snapshot object"),
        snapshot_bytes,
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(11), Some(9), None)
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("impossible row count rejects");

    // A malformed snapshot section is permanent durable corruption, not a
    // transient lower-layer read failure; recovery refuses with a non-retryable
    // corruption error that still preserves the underlying decode source.
    assert!(matches!(error, LifecycleError::RecoveryCorruption { .. }));
    assert!(error.source().is_some());
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_decode_over_budget_fails_closed() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x8d);
    let checkpoint_row = put_row(branch, 3, b"budgeted-checkpoint", b"checkpoint-value");
    publish_snapshot(
        backend,
        10,
        CommitVersion::new(3),
        std::slice::from_ref(&checkpoint_row),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(10), None)
            .expect("database root facts"),
    );
    let mut parts = StorageRuntimeBudgetParts {
        block_cache_bytes: 0,
        table_reader_bytes: 8 * 1024,
        active_mutable_bytes: 8 * 1024,
        frozen_mutable_bytes: 8 * 1024,
        maintenance_queue_bytes: 1024,
        generated_artifact_bytes: 1,
        manifest_catalog_bytes: 1024,
        max_open_readers: 4,
        max_frozen_tables: 4,
        max_pending_maintenance_tasks: 4,
        ..StorageRuntimeBudgetParts::default()
    };
    parts.total_bytes = parts.block_cache_bytes
        + parts.table_reader_bytes
        + parts.active_mutable_bytes
        + parts.frozen_mutable_bytes
        + parts.maintenance_queue_bytes
        + parts.generated_artifact_bytes
        + parts.manifest_catalog_bytes;
    let budget = StorageRuntimeBudget::from_parts(parts).expect("budget");
    let mut shell = assemble_shell(
        open_plan_with_budget(RecoveryStrictness::Strict, budget),
        branch,
        backend,
    )
    .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("checkpoint decode budget rejects");

    assert!(matches!(
        error,
        LifecycleError::StorageBudgetExceeded {
            pool: StorageBudgetPool::GeneratedArtifact,
            used_bytes: 0,
            limit_bytes: 1,
            ..
        }
    ));
    assert_eq!(error.code(), "resource_exhausted.lifecycle.storage_budget");
    assert!(shell.branch_state().is_empty());
}

#[test]
fn recovery_rebuilds_multiple_branch_descriptors() {
    // Open a durable runtime, create two additional branches, drop the
    // runtime, reopen on the same backend and verify all three branches
    // (initial + two created) survive in the catalog after recovery.
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x31);
    let new_a = branch_id(0x41);
    let new_b = branch_id(0x42);

    // First open: seed initial branch, create two additional branches.
    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        runtime
            .create_branch(
                new_a,
                CommitBranchGeneration::new(1).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create new_a");
        runtime
            .create_branch(
                new_b,
                CommitBranchGeneration::new(1).expect("generation"),
                Some(CommitVersion::new(3)),
            )
            .expect("create new_b");
        assert_eq!(runtime.list_branches(false).len(), 3);
    }

    // Second open: recovery should rebuild catalog from manifest.
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let descriptors = runtime.list_branches(true);
    let mut ids = descriptors
        .iter()
        .map(|d| d.branch_id())
        .collect::<Vec<_>>();
    ids.sort_by_key(|id| *id.as_bytes());
    assert_eq!(ids, vec![initial, new_a, new_b]);
}

#[test]
fn recovery_deleted_marker_outranks_older_table_manifest() {
    // Open, create a branch, delete it, drop runtime. Reopen and verify
    // the branch is in Deleted status (not resurrected as Active).
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x32);
    let deleted = branch_id(0x53);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");
        runtime
            .create_branch(
                deleted,
                CommitBranchGeneration::new(2).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create");
        runtime
            .delete_branch(
                deleted,
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(2).expect("generation"),
                ),
                Some(CommitVersion::new(3)),
            )
            .expect("delete");
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let with_deleted = runtime.list_branches(true);
    let deleted_entry = with_deleted
        .iter()
        .find(|d| d.branch_id() == deleted)
        .expect("deleted entry survives recovery");
    assert_eq!(
        deleted_entry.status(),
        crate::lifecycle::LifecycleBranchStatus::Deleted
    );
    // Active list excludes the deleted branch.
    assert!(runtime
        .list_branches(false)
        .iter()
        .all(|d| d.branch_id() != deleted));
}

#[test]
fn recovery_newer_generation_outranks_older_deleted_marker() {
    // Open, create-then-delete a branch at gen 1, recreate at gen 2,
    // drop runtime. Reopen and verify the branch is Active at gen 2.
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x33);
    let target = branch_id(0x60);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");
        runtime
            .create_branch(
                target,
                CommitBranchGeneration::new(1).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create gen 1");
        runtime
            .delete_branch(
                target,
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
                Some(CommitVersion::new(3)),
            )
            .expect("delete gen 1");
        runtime
            .create_branch(
                target,
                CommitBranchGeneration::new(2).expect("generation"),
                Some(CommitVersion::new(4)),
            )
            .expect("recreate gen 2");
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let active = runtime
        .list_branches(false)
        .into_iter()
        .find(|d| d.branch_id() == target)
        .expect("target is active after recovery");
    assert_eq!(
        active.generation(),
        CommitBranchGeneration::new(2).expect("generation"),
        "newer generation survives recovery"
    );
    assert_eq!(
        active.status(),
        crate::lifecycle::LifecycleBranchStatus::Active
    );
}

/// #2850: the catalog version-anchor truth table — each arm (`created_at`,
/// fork anchor, deletion watermark) contributes independently, the maximum
/// wins, and an anchor-free descriptor contributes ZERO. The fork arm stands
/// alone when `created_at` is `None` (pre-#2832 catalogs).
#[test]
fn descriptor_version_anchor_truth_table() {
    use crate::lifecycle::{
        descriptor_version_anchor, LifecycleBranchDescriptor, LifecycleBranchParent,
    };

    let v = CommitVersion::new;
    let generation = CommitBranchGeneration::new(1).expect("nonzero generation");
    let id = branch_id(0x54);
    let parent_id = branch_id(0x55);

    // Anchor-free: no created_at, no parent, no deletion.
    let bare = LifecycleBranchDescriptor::active(id, generation, None);
    assert_eq!(descriptor_version_anchor(&bare), CommitVersion::ZERO);

    // created_at alone.
    let created = LifecycleBranchDescriptor::active(id, generation, Some(v(7)));
    assert_eq!(descriptor_version_anchor(&created), v(7));

    // Fork anchor alone (created_at None — the pre-#2832 catalog shape).
    let forked = LifecycleBranchDescriptor::active(id, generation, None)
        .with_parent(LifecycleBranchParent::new(parent_id, v(9)));
    assert_eq!(descriptor_version_anchor(&forked), v(9));

    // Deletion watermark dominates the creation stamp on a dead descriptor.
    let deleted =
        LifecycleBranchDescriptor::active(id, generation, Some(v(3))).with_deleted_at(Some(v(12)));
    assert_eq!(descriptor_version_anchor(&deleted), v(12));

    // Maximum wins across all three arms together.
    let all = LifecycleBranchDescriptor::active(id, generation, Some(v(15)))
        .with_parent(LifecycleBranchParent::new(parent_id, v(9)))
        .with_deleted_at(Some(v(11)));
    assert_eq!(descriptor_version_anchor(&all), v(15));
}

#[test]
fn base_restore_generation_fence_truth_table() {
    // #2830: the base-restore fence — parentless only (fork-inherited rows
    // legitimately sit at or below created_at), `<=` boundary, None fences
    // nothing.
    use crate::lifecycle::parentless_content_predates_generation;
    let v = CommitVersion::new;
    assert!(parentless_content_predates_generation(
        false,
        Some(v(10)),
        v(9)
    ));
    assert!(parentless_content_predates_generation(
        false,
        Some(v(10)),
        v(10)
    ));
    assert!(!parentless_content_predates_generation(
        false,
        Some(v(10)),
        v(11)
    ));
    assert!(!parentless_content_predates_generation(
        true,
        Some(v(10)),
        v(9)
    ));
    assert!(!parentless_content_predates_generation(false, None, v(1)));
}

/// #2847: the non-seeded COMBINE arm's three behaviors on an occupied
/// (manifest-recovered) branch state — byte-identical checkpoint rows drop
/// as duplicates, uncovered rows append to the active memtable, and
/// divergent bytes at the same internal key fail closed.
#[test]
fn non_seeded_checkpoint_combine_dedups_appends_and_fails_closed() {
    use crate::branch::state::snapshot::{
        install_snapshot_rows_into_branches, BranchSnapshotInstallRequest,
    };
    use crate::branch::state::BranchLocalState;
    use crate::lifecycle::combine_non_seeded_checkpoint_rows;

    let branch = branch_id(0x53);
    let row = |key: &'static [u8], version: u64, value: u8| {
        StorageRow::put(
            physical_key(branch, key),
            CommitVersion::new(version),
            Timestamp::from_micros(version),
            Timestamp::EPOCH,
            vec![value],
        )
    };
    // Stand-in for the manifest-recovered base: snapshot-install two rows
    // into an empty state so they land as owned L0 tables.
    let mut states = vec![BranchLocalState::new(
        branch,
        crate::branch::config::BranchRuntimeConfig::default(),
    )
    .expect("empty state")];
    let request = BranchSnapshotInstallRequest::from_rows(
        "combine-arm-test-seed",
        vec![row(b"covered", 5, 1), row(b"covered-two", 6, 2)],
    )
    .expect("install request");
    install_snapshot_rows_into_branches(&mut states, &request).expect("build the occupied base");
    let staged = states.pop().expect("staged state");
    assert!(!staged.is_empty(), "base must be occupied");

    // Duplicate + fresh: the duplicate drops, the fresh row appends.
    let mut combined = staged.clone();
    combine_non_seeded_checkpoint_rows(
        &mut combined,
        vec![row(b"covered", 5, 1), row(b"tail", 7, 3)],
    )
    .expect("byte-identical overlap combines");
    let view = combined.capture_read_view().expect("view");
    let tail = view
        .at_version(&physical_key(branch, b"tail"), CommitVersion::new(7))
        .expect("read tail")
        .expect("tail row present");
    assert_eq!(tail.row().value(), &[3], "uncovered row appended");
    let covered = view
        .at_version(&physical_key(branch, b"covered"), CommitVersion::new(5))
        .expect("read covered")
        .expect("covered row present");
    assert_eq!(covered.row().value(), &[1], "base row intact after dedup");

    // Divergent bytes at the same internal key fail closed.
    let mut poisoned = staged.clone();
    let error = combine_non_seeded_checkpoint_rows(&mut poisoned, vec![row(b"covered", 5, 9)])
        .expect_err("divergent bytes at the same internal key must fail closed");
    assert!(
        matches!(
            error,
            LifecycleError::TableManifestCheckpointConflict { .. }
        ),
        "unexpected error: {error:?}"
    );

    // A fully-covered input is a legal no-op: nothing appends, state intact.
    let mut untouched = staged.clone();
    combine_non_seeded_checkpoint_rows(
        &mut untouched,
        vec![row(b"covered", 5, 1), row(b"covered-two", 6, 2)],
    )
    .expect("fully-covered input combines as a no-op");
    let view = untouched.capture_read_view().expect("view");
    assert!(
        view.at_version(&physical_key(branch, b"covered"), CommitVersion::new(5))
            .expect("read covered")
            .is_some(),
        "base rows survive the no-op combine"
    );

    // A duplicated internal key WITHIN the checkpoint's own rows fails
    // closed (silent last-wins would drop a row from a corrupt snapshot).
    let mut duplicated = staged.clone();
    let error = combine_non_seeded_checkpoint_rows(
        &mut duplicated,
        vec![row(b"tail", 7, 3), row(b"tail", 7, 4)],
    )
    .expect_err("duplicate internal key in the checkpoint rows must fail closed");
    assert!(
        matches!(error, LifecycleError::RecoveryFailed { .. }),
        "unexpected error: {error:?}"
    );
}

#[test]
fn generation_fence_truth_table() {
    // #2826: the WAL-replay generation fence. `<=` is the boundary — a
    // predecessor's last record can share the exact version the successor
    // recorded as visible-at-creation; own records are strictly above it.
    use crate::lifecycle::record_predates_current_generation;
    let v = CommitVersion::new;
    assert!(record_predates_current_generation(v(9), Some(v(10))));
    assert!(record_predates_current_generation(v(10), Some(v(10))));
    assert!(!record_predates_current_generation(v(11), Some(v(10))));
    assert!(!record_predates_current_generation(v(1), None));
}

#[test]
fn recovery_rebuilds_active_branch_states() {
    // Open a durable runtime, create a non-seeded branch, commit a row to
    // it (durable WAL append), drop the runtime, reopen on the same
    // backend, and verify the row survives via per-branch WAL replay.
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x34);
    let new_branch = branch_id(0x44);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        runtime
            .create_branch(
                new_branch,
                CommitBranchGeneration::new(1).expect("generation"),
                // #2826: truthful stamp — nothing is visible yet, and a
                // future-dated created_at would fence the branch's own
                // replayed records (production always stamps
                // `current_visible`, which the allocator stays above).
                None,
            )
            .expect("create branch");
        runtime
            .execute_durable_commit(
                durable_standard_batch(new_branch, b"non-seeded-row", b"value"),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit to new branch");
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let restored_state = runtime
        .branch_catalog()
        .branch_state(new_branch)
        .expect("non-seeded branch state survives");
    let view = restored_state
        .capture_read_view()
        .expect("non-seeded branch read view");
    let row = view
        .latest(&physical_key(new_branch, b"non-seeded-row"))
        .expect("read view")
        .expect("row visible on non-seeded branch after recovery");
    assert_eq!(row.row().commit_version(), CommitVersion::new(1));
    assert_eq!(row.row().value(), b"value");
}

#[test]
fn recovery_rejects_wal_row_for_deleted_generation() {
    // Open, create a branch, delete it (durable tombstone), drop. Inject a
    // WAL record stamped for the deleted branch_id into the backend and
    // reopen — recovery must refuse to resurrect the deleted branch.
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x35);
    let deleted = branch_id(0x45);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");
        runtime
            .create_branch(
                deleted,
                CommitBranchGeneration::new(2).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create branch");
        runtime
            .delete_branch(
                deleted,
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(2).expect("generation"),
                ),
                Some(CommitVersion::new(3)),
            )
            .expect("delete branch");
    }

    // Inject a WAL record stamped for the deleted branch.
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let resurrected = wal_record(deleted, 5, b"resurrect-attempt", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&resurrected)
        .expect("append wal record for deleted branch");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");

    assert_eq!(
        shell
            .complete_recovery(&outcome)
            .expect_err("deleted-branch WAL must reject"),
        LifecycleError::RecoveryFailed {
            reason: "recovered WAL package references a deleted branch",
        }
    );
}

#[test]
fn recovery_rebuilds_fork_at_history_version() {
    // Open, commit a row to the seeded branch, fork a child at the visible
    // history version, drop. Reopen and verify the child descriptor's
    // parent metadata (source branch + fork version) survives recovery.
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x36);
    let child = branch_id(0x46);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"fork-source-row", b"value"),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit to seeded branch");
        runtime
            .fork_at_retained_version(
                initial,
                child,
                CommitBranchGeneration::new(1).expect("generation"),
                CommitVersion::new(1),
                CommitVersion::ZERO,
                // #2826: visible-at-fork (one commit so far) — the recovery
                // generation fence consumes this; the fork POINT is asserted
                // separately via `parent.fork_version`.
                Some(CommitVersion::new(1)),
            )
            .expect("fork child at history version");
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let child_descriptor = runtime
        .list_branches(false)
        .into_iter()
        .find(|d| d.branch_id() == child)
        .expect("child descriptor survives recovery");
    let parent = child_descriptor
        .parent()
        .expect("forked child has parent metadata");
    assert_eq!(parent.source_branch_id(), initial);
    assert_eq!(parent.fork_version(), CommitVersion::new(1));
    assert_eq!(
        child_descriptor.created_at(),
        Some(CommitVersion::new(1)),
        "fork created_at records visible-at-fork (#2826)",
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn recovery_table_manifest_multi_branch_rows_round_trip() {
    // Inject a BranchCatalogManifest with two active branches plus a
    // TableManifest per branch carrying owned rows. Open the database
    // afresh and verify both branches' row state is rebuilt from their
    // per-branch TableManifests.
    use crate::format::{
        encode_table_manifest, table_row_split_extension_section, BranchCatalogEntry,
        BranchCatalogManifest, BranchCatalogStatus, TableManifest, TableManifestLevel,
        TableRowSplit,
    };
    use crate::layout::ObjectLayout;

    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x37);
    let extra = branch_id(0x47);

    // Publish two table objects (one per branch).
    let initial_table = publish_table_for_recovery(
        backend,
        initial,
        0,
        "initial-table",
        &[put_row(initial, 1, b"initial-row", b"initial-value")],
    );
    let extra_table = publish_table_for_recovery(
        backend,
        extra,
        0,
        "extra-table",
        &[put_row(extra, 1, b"extra-row", b"extra-value")],
    );

    // Build + publish per-branch TableManifests.
    let initial_manifest = TableManifest::new(
        initial,
        None,
        4,
        vec![TableManifestLevel::new(
            crate::branch::facts::BranchLevel::ZERO,
            vec![initial_table.clone()],
        )
        .expect("initial level")],
        Vec::new(),
        vec![
            table_row_split_extension_section(&[TableRowSplit::new(1, 0)])
                .expect("row-split section"),
        ],
    )
    .expect("initial table manifest");
    let extra_manifest = TableManifest::new(
        extra,
        None,
        5,
        vec![TableManifestLevel::new(
            crate::branch::facts::BranchLevel::ZERO,
            vec![extra_table.clone()],
        )
        .expect("extra level")],
        Vec::new(),
        vec![
            table_row_split_extension_section(&[TableRowSplit::new(1, 0)])
                .expect("row-split section"),
        ],
    )
    .expect("extra table manifest");
    backend.write_raw(
        ObjectLayout::branch_table_manifest(&initial.to_string()).expect("initial manifest object"),
        encode_table_manifest(&initial_manifest).expect("initial manifest bytes"),
    );
    backend.write_raw(
        ObjectLayout::branch_table_manifest(&extra.to_string()).expect("extra manifest object"),
        encode_table_manifest(&extra_manifest).expect("extra manifest bytes"),
    );

    // Publish a BranchCatalogManifest listing both branches as Active.
    let initial_entry = BranchCatalogEntry::new(initial, 1, BranchCatalogStatus::Active)
        .expect("initial entry")
        .with_created_at(1)
        .expect("initial created_at");
    // #2830: no created_at — the hand-crafted manifest owns a v1 row, and a
    // branch created at visible 2 cannot own rows at or below 2 (the base
    // restore now fences on exactly that impossibility).
    let extra_entry =
        BranchCatalogEntry::new(extra, 1, BranchCatalogStatus::Active).expect("extra entry");
    let catalog_manifest =
        BranchCatalogManifest::new(DATABASE_ID, 1, vec![initial_entry, extra_entry])
            .expect("branch catalog manifest");
    backend.write_raw(
        ObjectLayout::branch_catalog_manifest().expect("branch catalog manifest object"),
        crate::format::encode_branch_catalog_manifest(&catalog_manifest)
            .expect("branch catalog manifest bytes"),
    );

    // Open and recover; the catalog manifest replay attaches the extra
    // branch and `recover_per_branch_table_manifests` installs both
    // branches' rows from the per-branch TableManifests.
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let initial_state = runtime
        .branch_catalog()
        .branch_state(initial)
        .expect("initial branch state");
    let initial_view = initial_state.capture_read_view().expect("initial view");
    let initial_row = initial_view
        .latest(&physical_key(initial, b"initial-row"))
        .expect("initial read view")
        .expect("initial row recovered");
    assert_eq!(initial_row.row().value(), b"initial-value");

    let extra_state = runtime
        .branch_catalog()
        .branch_state(extra)
        .expect("extra branch state");
    let extra_view = extra_state.capture_read_view().expect("extra view");
    let extra_row = extra_view
        .latest(&physical_key(extra, b"extra-row"))
        .expect("extra read view")
        .expect("extra row recovered");
    assert_eq!(extra_row.row().value(), b"extra-value");
}

/// #2830 direction control at the fence's own granularity: a parentless
/// branch WITH `created_at` stamped whose hand-crafted manifest owns rows
/// ABOVE the creation point must still install — the stale-manifest skip
/// consults `manifest_max_commit_version`, and this shape (no snapshot, no
/// WAL to heal a wrong skip) is the one that kills its stub mutants.
#[test]
fn manifest_above_created_at_installs_for_stamped_parentless_branch() {
    use crate::format::{
        encode_table_manifest, table_row_split_extension_section, BranchCatalogEntry,
        BranchCatalogManifest, BranchCatalogStatus, TableManifest, TableManifestLevel,
        TableRowSplit,
    };
    use crate::layout::ObjectLayout;

    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x38);
    let extra = branch_id(0x48);

    let extra_table = publish_table_for_recovery(
        backend,
        extra,
        0,
        "stamped-extra-table",
        &[put_row(extra, 2, b"stamped-row", b"stamped-value")],
    );
    let extra_manifest = TableManifest::new(
        extra,
        None,
        3,
        vec![TableManifestLevel::new(
            crate::branch::facts::BranchLevel::ZERO,
            vec![extra_table.clone()],
        )
        .expect("extra level")],
        Vec::new(),
        vec![
            table_row_split_extension_section(&[TableRowSplit::new(1, 0)])
                .expect("row-split section"),
        ],
    )
    .expect("extra table manifest");
    backend.write_raw(
        ObjectLayout::branch_table_manifest(&extra.to_string()).expect("extra manifest object"),
        encode_table_manifest(&extra_manifest).expect("extra manifest bytes"),
    );

    let initial_entry =
        BranchCatalogEntry::new(initial, 1, BranchCatalogStatus::Active).expect("initial entry");
    // created_at 1 with a v2 row: the manifest is ABOVE the creation point,
    // so the generation fence must let it through.
    let extra_entry = BranchCatalogEntry::new(extra, 1, BranchCatalogStatus::Active)
        .expect("extra entry")
        .with_created_at(1)
        .expect("extra created_at");
    let catalog_manifest =
        BranchCatalogManifest::new(DATABASE_ID, 1, vec![initial_entry, extra_entry])
            .expect("branch catalog manifest");
    backend.write_raw(
        ObjectLayout::branch_catalog_manifest().expect("branch catalog manifest object"),
        crate::format::encode_branch_catalog_manifest(&catalog_manifest)
            .expect("branch catalog manifest bytes"),
    );

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let extra_state = runtime
        .branch_catalog()
        .branch_state(extra)
        .expect("extra branch state");
    let extra_view = extra_state.capture_read_view().expect("extra view");
    let extra_row = extra_view
        .latest(&physical_key(extra, b"stamped-row"))
        .expect("extra read view")
        .expect("above-created_at manifest row must install");
    assert_eq!(extra_row.row().value(), b"stamped-value");
}

#[test]
fn recovery_checkpoint_multi_branch_rows_round_trip() {
    // Open a durable runtime, create a non-seeded branch, commit a row to
    // each branch, trigger a checkpoint, drop the runtime, reopen, and
    // verify both rows survive via per-branch checkpoint row dispatch.
    use crate::lifecycle::LifecycleCheckpointRequest;
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x38);
    let extra = branch_id(0x48);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        runtime
            .create_branch(
                extra,
                CommitBranchGeneration::new(1).expect("generation"),
                // #2830: truthful stamp — nothing is visible yet, and a
                // future-dated created_at fences the branch's own rows out
                // of the checkpoint restore (production stamps
                // `current_visible`).
                None,
            )
            .expect("create extra branch");
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"initial-row", b"initial-value"),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit to initial");
        runtime
            .execute_durable_commit(
                durable_standard_batch(extra, b"extra-row", b"extra-value"),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit to extra");

        let checkpoint_request =
            LifecycleCheckpointRequest::new(initial, 1, Timestamp::from_micros(9_500))
                .expect("checkpoint request");
        let outcome = runtime
            .checkpoint(&checkpoint_request)
            .expect("checkpoint succeeds");
        assert_eq!(
            outcome.status(),
            crate::lifecycle::LifecycleCheckpointStatus::Completed
        );
        assert!(
            outcome.row_count() >= 2,
            "checkpoint must include both rows"
        );
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let initial_state = runtime
        .branch_catalog()
        .branch_state(initial)
        .expect("initial branch state");
    let initial_view = initial_state.capture_read_view().expect("initial view");
    let initial_row = initial_view
        .latest(&physical_key(initial, b"initial-row"))
        .expect("initial read view")
        .expect("initial row recovered from checkpoint");
    assert_eq!(initial_row.row().value(), b"initial-value");

    let extra_state = runtime
        .branch_catalog()
        .branch_state(extra)
        .expect("extra branch state");
    let extra_view = extra_state.capture_read_view().expect("extra view");
    let extra_row = extra_view
        .latest(&physical_key(extra, b"extra-row"))
        .expect("extra read view")
        .expect("extra row recovered from checkpoint");
    assert_eq!(extra_row.row().value(), b"extra-value");
}

// Guard regression for the multi-branch orphaned-delta recovery gap: the seed-155 orphan detector
// only consults the SEEDED branch, so a snapshot taken while a NON-seeded branch holds a durable
// table-manifest base would recover a non-contiguous gap if a crash dropped that branch's manifest
// (recovery rebuilds non-seeded branches from {snapshot delta + per-branch manifest} without
// replaying the WAL below the snapshot watermark). The guard makes the checkpoint DEFER in that
// configuration, so the rows stay in the WAL and a full replay recovers every branch cleanly even
// after the manifest is dropped. The per-branch fix that lifts the guard (a durable per-branch
// flushed-branch set + per-branch recovery, re-enabling the checkpoint) is tracked in
// docs/architecture/archive/implementation-plans/storage-testing/multi-branch-orphaned-delta-recovery-gap.md.
#[allow(
    clippy::too_many_lines,
    reason = "multi-branch durability scenario: two branches flushed, checkpoint defers, crash, reopen-and-verify"
)]
#[test]
fn multi_branch_checkpoint_defers_so_lost_non_seeded_manifest_recovers_cleanly() {
    // Multi-branch durability: both branches flush (owned tables + per-branch table manifests),
    // each then takes an active delta. A checkpoint is requested, but because the non-seeded
    // branch holds a durable base the checkpoint DEFERS (the multi-branch guard) — no snapshot is
    // recorded that would advance the WAL-replay floor past the non-seeded base. A crash then
    // drops the non-seeded branch's table manifest. Recovery replays the full WAL and recovers
    // both branches' rows cleanly: no gap, no loss.
    use crate::lifecycle::{
        FlushTableIdentitySeed, FlushTableObjectId, LifecycleCheckpointRequest,
    };
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x39);
    let extra = branch_id(0x49);
    let guard =
        || CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation"));
    let flush_req = |branch, seed: &str, object: &str| {
        FlushFrozenRequest::new(
            branch,
            None,
            FlushTableIdentitySeed::new(seed).expect("seed"),
            FlushTableObjectId::new(object).expect("object id"),
        )
        .expect("flush request")
    };

    {
        let mut shell = assemble_shell(lossy_open_plan(), initial, backend).expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        runtime
            .create_branch(
                extra,
                CommitBranchGeneration::new(1).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create extra branch");

        // Seeded branch: base -> rotate -> flush (owned + manifest, KEPT) -> active delta. Its
        // manifest survives, so the orphan detector sees a present seeded stage and stands down.
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"initial-base", b"initial-base-value"),
                guard(),
            )
            .expect("commit initial base");
        runtime
            .rotate_active_for_maintenance()
            .expect("rotate initial");
        runtime
            .flush_frozen(&flush_req(initial, "initial-seed", "initial-object"))
            .expect("flush initial");
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"initial-delta", b"initial-delta-value"),
                guard(),
            )
            .expect("commit initial delta");

        // Non-seeded branch: base -> rotate -> flush (owned + manifest, to be DROPPED) -> delta.
        runtime
            .execute_durable_commit(
                durable_standard_batch(extra, b"extra-base", b"extra-base-value"),
                guard(),
            )
            .expect("commit extra base");
        runtime
            .rotate_active_for_branch_for_maintenance(extra)
            .expect("rotate extra");
        runtime
            .flush_frozen(&flush_req(extra, "extra-seed", "extra-object"))
            .expect("flush extra");
        runtime
            .execute_durable_commit(
                durable_standard_batch(extra, b"extra-delta", b"extra-delta-value"),
                guard(),
            )
            .expect("commit extra delta");

        let checkpoint_request =
            LifecycleCheckpointRequest::new(initial, 1, Timestamp::from_micros(9_500))
                .expect("checkpoint request");
        let outcome = runtime
            .checkpoint(&checkpoint_request)
            .expect("checkpoint runs");
        // The multi-branch guard fires: the non-seeded branch holds a durable base, so the
        // checkpoint defers rather than recording a snapshot recovery could not undo.
        assert_eq!(
            outcome.status(),
            crate::lifecycle::LifecycleCheckpointStatus::DeferredNonSeededBranchBase
        );
    }

    // Crash: drop ONLY the non-seeded branch's table manifest.
    let extra_manifest = crate::layout::ObjectLayout::branch_table_manifest(&extra.to_string())
        .expect("extra manifest layout");
    backend
        .delete_object(&extra_manifest)
        .expect("drop extra manifest");

    let mut shell = assemble_shell(lossy_open_plan(), initial, backend).expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let extra_state = runtime
        .branch_catalog()
        .branch_state(extra)
        .expect("extra branch state");
    let extra_view = extra_state.capture_read_view().expect("extra view");
    let base_present = extra_view
        .latest(&physical_key(extra, b"extra-base"))
        .expect("read extra-base")
        .is_some();
    let delta_present = extra_view
        .latest(&physical_key(extra, b"extra-delta"))
        .expect("read extra-delta")
        .is_some();
    // The deferred checkpoint left every commit in the WAL, so a full replay recovers the
    // non-seeded branch completely even though its table manifest was dropped: both the flushed
    // base and the later delta are present, with no gap.
    assert!(
        base_present && delta_present,
        "non-seeded branch did not recover cleanly after its manifest was dropped \
         (base_present={base_present}, delta_present={delta_present})",
    );
}

// Close-drain companion to the guard regression above. The multi-branch guard is enforced at
// background CLAIM time and on the synchronous checkpoint path — but a claimed task whose worker
// dies mid-build (shutdown detaches workers on timeout; a panicked worker leaves its task active
// for the close retry) is re-run by `drain_active_for_close` through the close runner, which
// publishes via the single-branch collector with NO guard re-check. Two failures follow: the
// snapshot omits every non-seeded branch's delta rows while its watermark still advances the
// WAL-replay floor past them (silent loss on a clean close+reopen, no crash needed), and the
// recorded snapshot is exactly the one the guard exists to prevent (a crash dropping the
// non-seeded manifest then recovers a silent gap).
#[allow(
    clippy::too_many_lines,
    reason = "multi-branch close-drain durability scenario: stranded checkpoint task, close, reopen-and-verify"
)]
#[test]
fn close_drained_checkpoint_does_not_bypass_the_multi_branch_guard() {
    use crate::lifecycle::{
        FlushTableIdentitySeed, FlushTableObjectId, MaintenanceTask, MaintenanceTaskRequest,
    };
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x3b);
    let extra = branch_id(0x4b);
    let guard =
        || CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation"));
    let flush_req = |branch, seed: &str, object: &str| {
        FlushFrozenRequest::new(
            branch,
            None,
            FlushTableIdentitySeed::new(seed).expect("seed"),
            FlushTableObjectId::new(object).expect("object id"),
        )
        .expect("flush request")
    };

    {
        let mut shell = assemble_shell(lossy_open_plan(), initial, backend).expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        runtime
            .create_branch(
                extra,
                CommitBranchGeneration::new(1).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create extra branch");

        // Seeded branch: base -> rotate -> flush -> active delta.
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"initial-base", b"initial-base-value"),
                guard(),
            )
            .expect("commit initial base");
        runtime
            .rotate_active_for_maintenance()
            .expect("rotate initial");
        runtime
            .flush_frozen(&flush_req(initial, "initial-seed", "initial-object"))
            .expect("flush initial");
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"initial-delta", b"initial-delta-value"),
                guard(),
            )
            .expect("commit initial delta");

        // Non-seeded branch base row. At THIS point a background worker claims a
        // checkpoint task: no non-seeded branch holds a durable base yet, so the
        // claim-time guard passes. The worker then dies mid-build — model the
        // stranded claim with the active-task hook the close drain services.
        runtime
            .execute_durable_commit(
                durable_standard_batch(extra, b"extra-base", b"extra-base-value"),
                guard(),
            )
            .expect("commit extra base");
        let stranded = MaintenanceTask::new_for_test(77, MaintenanceTaskRequest::checkpoint())
            .expect("stranded checkpoint task");
        runtime.set_active_maintenance_for_test(stranded);

        // The non-seeded base lands inside the stranded build's window: rotate +
        // flush (owned table + per-branch manifest), then an active delta that
        // only the WAL holds.
        runtime
            .rotate_active_for_branch_for_maintenance(extra)
            .expect("rotate extra");
        runtime
            .flush_frozen(&flush_req(extra, "extra-seed", "extra-object"))
            .expect("flush extra");
        runtime
            .execute_durable_commit(
                durable_standard_batch(extra, b"extra-delta", b"extra-delta-value"),
                guard(),
            )
            .expect("commit extra delta");

        // Clean close: `drain_active_for_close` re-runs the stranded checkpoint
        // through the close runner. The guard contract requires it to DEFER —
        // a published seeded-only snapshot would advance the replay floor past
        // the non-seeded rows the snapshot does not carry.
        runtime.close().expect("clean close");
    }

    // No crash, nothing dropped: a clean close followed by a clean reopen must
    // recover every branch completely.
    let mut shell = assemble_shell(lossy_open_plan(), initial, backend).expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let extra_state = runtime
        .branch_catalog()
        .branch_state(extra)
        .expect("extra branch state");
    let extra_view = extra_state.capture_read_view().expect("extra view");
    let base_present = extra_view
        .latest(&physical_key(extra, b"extra-base"))
        .expect("read extra-base")
        .is_some();
    let delta_present = extra_view
        .latest(&physical_key(extra, b"extra-delta"))
        .expect("read extra-delta")
        .is_some();
    assert!(
        base_present && delta_present,
        "non-seeded branch lost rows across a clean close+reopen because a close-drained \
         checkpoint bypassed the multi-branch guard \
         (base_present={base_present}, delta_present={delta_present})",
    );
}

// Boundary pin for the multi-branch guard: deleting the flushed non-seeded branch releases the
// guard (its durable tombstone means recovery no longer needs that branch's base), the next
// checkpoint COMPLETES, and a crash that drops the deleted branch's leftover table manifest
// recovers cleanly — the seeded branch keeps every row and the tombstoned branch stays deleted
// (no resurrection, no gap). Pins where the guard's protection legitimately ends, so a future
// change that widens or narrows `non_seeded_branch_has_durable_base` shows up here.
#[allow(
    clippy::too_many_lines,
    reason = "multi-branch durability scenario: flush both, delete non-seeded, checkpoint, crash, reopen-and-verify"
)]
#[test]
fn deleting_the_flushed_non_seeded_branch_releases_the_checkpoint_guard() {
    use crate::lifecycle::{
        FlushTableIdentitySeed, FlushTableObjectId, LifecycleCheckpointRequest,
    };
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x3c);
    let extra = branch_id(0x4c);
    let guard =
        || CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation"));
    let flush_req = |branch, seed: &str, object: &str| {
        FlushFrozenRequest::new(
            branch,
            None,
            FlushTableIdentitySeed::new(seed).expect("seed"),
            FlushTableObjectId::new(object).expect("object id"),
        )
        .expect("flush request")
    };

    {
        let mut shell = assemble_shell(lossy_open_plan(), initial, backend).expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        runtime
            .create_branch(
                extra,
                CommitBranchGeneration::new(1).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create extra branch");

        // Seeded branch: base -> rotate -> flush -> active delta.
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"initial-base", b"initial-base-value"),
                guard(),
            )
            .expect("commit initial base");
        runtime
            .rotate_active_for_maintenance()
            .expect("rotate initial");
        runtime
            .flush_frozen(&flush_req(initial, "initial-seed", "initial-object"))
            .expect("flush initial");
        runtime
            .execute_durable_commit(
                durable_standard_batch(initial, b"initial-delta", b"initial-delta-value"),
                guard(),
            )
            .expect("commit initial delta");

        // Non-seeded branch: base -> rotate -> flush (durable base -> guard arms).
        runtime
            .execute_durable_commit(
                durable_standard_batch(extra, b"extra-base", b"extra-base-value"),
                guard(),
            )
            .expect("commit extra base");
        runtime
            .rotate_active_for_branch_for_maintenance(extra)
            .expect("rotate extra");
        runtime
            .flush_frozen(&flush_req(extra, "extra-seed", "extra-object"))
            .expect("flush extra");

        // Guard armed: the checkpoint defers while the flushed non-seeded branch lives.
        let deferred_request =
            LifecycleCheckpointRequest::new(initial, 1, Timestamp::from_micros(9_400))
                .expect("checkpoint request");
        let deferred = runtime
            .checkpoint(&deferred_request)
            .expect("checkpoint runs");
        assert_eq!(
            deferred.status(),
            crate::lifecycle::LifecycleCheckpointStatus::DeferredNonSeededBranchBase
        );

        // Delete the non-seeded branch: the tombstone is durably published with the
        // branch catalog, so recovery no longer needs (or reads) that branch's base.
        runtime
            .delete_branch(extra, guard(), Some(CommitVersion::new(6)))
            .expect("delete extra branch");

        // Guard released: the same checkpoint now completes.
        let completed_request =
            LifecycleCheckpointRequest::new(initial, 2, Timestamp::from_micros(9_500))
                .expect("checkpoint request");
        let completed = runtime
            .checkpoint(&completed_request)
            .expect("checkpoint runs");
        assert_eq!(
            completed.status(),
            crate::lifecycle::LifecycleCheckpointStatus::Completed,
            "deleting the flushed non-seeded branch must release the checkpoint guard",
        );
    }

    // Crash: drop the deleted branch's leftover table manifest (retention may or may
    // not have reclaimed it yet — recovery must not care either way).
    let extra_manifest = crate::layout::ObjectLayout::branch_table_manifest(&extra.to_string())
        .expect("extra manifest layout");
    let _ = backend.delete_object(&extra_manifest);

    let mut shell = assemble_shell(lossy_open_plan(), initial, backend).expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    // Seeded branch: fully recovered through {manifest base + snapshot delta}.
    let initial_state = runtime
        .branch_catalog()
        .branch_state(initial)
        .expect("initial branch state");
    let initial_view = initial_state.capture_read_view().expect("initial view");
    assert!(
        initial_view
            .latest(&physical_key(initial, b"initial-base"))
            .expect("read initial-base")
            .is_some(),
        "seeded base must survive the checkpoint taken after the delete",
    );
    assert!(
        initial_view
            .latest(&physical_key(initial, b"initial-delta"))
            .expect("read initial-delta")
            .is_some(),
        "seeded delta must survive the checkpoint taken after the delete",
    );

    // Tombstoned branch: stays deleted — no resurrection through the snapshot,
    // the WAL, or its (dropped) table manifest.
    assert!(
        runtime.branch_catalog().branch_state(extra).is_err(),
        "deleted branch must not resurrect on recovery",
    );
}

#[test]
fn recovery_rebuilds_inherited_layers() {
    // Open a durable runtime, commit + rotate + flush a row to the seeded
    // branch so its table manifest carries owned levels, fork a child,
    // commit + rotate + flush a row to the child so its table manifest
    // carries both owned levels AND inherited layers pointing back to
    // the parent. Drop, reopen, and verify the child's inherited layer
    // facts survive.
    use crate::lifecycle::{FlushTableIdentitySeed, FlushTableObjectId};
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let parent = branch_id(0x3a);
    let child = branch_id(0x4a);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), parent, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        // Commit + rotate + flush parent.
        runtime
            .execute_durable_commit(
                durable_standard_batch(parent, b"parent-row", b"parent-value"),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit to parent");
        runtime
            .rotate_active_for_maintenance()
            .expect("rotate parent active");
        let parent_flush = FlushFrozenRequest::new(
            parent,
            None,
            FlushTableIdentitySeed::new("parent-flush-seed").expect("seed"),
            FlushTableObjectId::new("parent-flush-object").expect("object id"),
        )
        .expect("parent flush request");
        let parent_outcome = runtime
            .flush_frozen(&parent_flush)
            .expect("parent flush succeeds");
        assert!(
            parent_outcome.completed(),
            "parent flush must publish the manifest",
        );

        // Fork child from parent. Parent's active+frozen must both be
        // empty; the flush above moved the row into owned_levels.
        let fork_outcome = runtime
            .fork_current(
                parent,
                child,
                CommitBranchGeneration::new(1).expect("generation"),
                None,
            )
            .expect("fork child");
        assert!(
            fork_outcome.inherited_layer_count() > 0,
            "child must inherit at least one layer",
        );

        // Commit + rotate + flush child so its table manifest is
        // published with both owned and inherited content.
        runtime
            .execute_durable_commit(
                durable_standard_batch(child, b"child-row", b"child-value"),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit to child");
        runtime
            .rotate_active_for_branch_for_maintenance(child)
            .expect("rotate child active");
        let child_flush = FlushFrozenRequest::new(
            child,
            None,
            FlushTableIdentitySeed::new("child-flush-seed").expect("seed"),
            FlushTableObjectId::new("child-flush-object").expect("object id"),
        )
        .expect("child flush request");
        let child_outcome = runtime
            .flush_frozen(&child_flush)
            .expect("child flush succeeds");
        assert!(
            child_outcome.completed(),
            "child flush must publish the manifest",
        );
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), parent, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let child_state = runtime
        .branch_catalog()
        .branch_state(child)
        .expect("child branch state");
    assert!(
        child_state.inherited_layer_count() > 0,
        "child must recover with at least one inherited layer",
    );
    assert!(
        child_state.owned_table_count() > 0,
        "child must recover its own owned tables",
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn recovery_rebuilds_cow_historical_fork() {
    // fork-cow.2: commit two versions to the seeded branch, rotate + flush so both are sealed in an
    // owned table, then fork a child at the OLD version (V = 1 < current = 2). With every `<= V` row
    // durable and the source carrying no inherited layers, the fork is copy-on-write — the child
    // references the parent's straddle owned table via one inherited layer and materializes nothing.
    // Drop, reopen, and verify the straddle inherited layer survives recovery with no child-owned tables.
    use crate::lifecycle::{FlushTableIdentitySeed, FlushTableObjectId};
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let parent = branch_id(0x3b);
    let child = branch_id(0x4b);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), parent, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        for value in [b"v1".as_slice(), b"v2".as_slice()] {
            runtime
                .execute_durable_commit(
                    durable_standard_batch(parent, b"history-key", value),
                    CommitBranchGenerationGuard::exact(
                        CommitBranchGeneration::new(1).expect("generation"),
                    ),
                )
                .expect("commit to parent");
        }
        runtime
            .rotate_active_for_maintenance()
            .expect("rotate parent active");
        let parent_flush = FlushFrozenRequest::new(
            parent,
            None,
            FlushTableIdentitySeed::new("cow-parent-flush-seed").expect("seed"),
            FlushTableObjectId::new("cow-parent-flush-object").expect("object id"),
        )
        .expect("parent flush request");
        assert!(
            runtime
                .flush_frozen(&parent_flush)
                .expect("parent flush succeeds")
                .completed(),
            "parent flush must publish the manifest",
        );

        // Fork at the OLD version (V = 1 < visible 2): the `<= 1` row is durable and the parent has no
        // inherited layers, so this is a copy-on-write historical fork over a straddle owned table.
        let fork_outcome = runtime
            .fork_at_retained_version(
                parent,
                child,
                CommitBranchGeneration::new(1).expect("generation"),
                CommitVersion::new(1),
                CommitVersion::ZERO,
                None,
            )
            .expect("fork child at history version");
        assert!(
            fork_outcome.inherited_layer_count() > 0,
            "COW historical fork must create a straddle inherited layer",
        );
        assert!(
            runtime
                .branch_catalog()
                .branch_state(child)
                .expect("child branch state")
                .owned_table_count()
                == 0,
            "the COW fork child owns no materialized tables at fork time",
        );

        // Commit + rotate + flush the child so its table manifest publishes with the straddle inherited
        // layer (a child persists its inherited layers only when its own manifest is written).
        runtime
            .execute_durable_commit(
                durable_standard_batch(child, b"child-row", b"child-value"),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit to child");
        runtime
            .rotate_active_for_branch_for_maintenance(child)
            .expect("rotate child active");
        let child_flush = FlushFrozenRequest::new(
            child,
            None,
            FlushTableIdentitySeed::new("cow-child-flush-seed").expect("seed"),
            FlushTableObjectId::new("cow-child-flush-object").expect("object id"),
        )
        .expect("child flush request");
        assert!(
            runtime
                .flush_frozen(&child_flush)
                .expect("child flush succeeds")
                .completed(),
            "child flush must publish the manifest",
        );
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), parent, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let child_state = runtime
        .branch_catalog()
        .branch_state(child)
        .expect("child branch state");
    assert!(
        child_state.inherited_layer_count() > 0,
        "COW child must recover its straddle inherited layer",
    );
}

#[test]
fn recovery_preserves_branch_release_facts() {
    // Open, create a branch, delete it (pushes a release plan into the
    // in-memory buffer and publishes the pending-releases manifest),
    // drop the runtime *without* running retention. Reopen and verify
    // the buffer comes back with the same released branch_id.
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let initial = branch_id(0x3b);
    let released = branch_id(0x4b);

    {
        let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
            .expect("durable shell");
        let request =
            LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
        let outcome = LifecycleRecoveryRuntime::new(&mut shell)
            .recover(&request)
            .expect("recovery outcome");
        let mut runtime = shell.complete_recovery(&outcome).expect("bootstrap");

        runtime
            .create_branch(
                released,
                CommitBranchGeneration::new(1).expect("generation"),
                Some(CommitVersion::new(2)),
            )
            .expect("create branch");
        runtime
            .delete_branch(
                released,
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
                Some(CommitVersion::new(3)),
            )
            .expect("delete branch");
        assert_eq!(runtime.pending_releases().len(), 1);
        assert_eq!(runtime.pending_releases()[0].released_branch_id(), released,);
    }

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), initial, backend)
        .expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let runtime = shell.complete_recovery(&outcome).expect("bootstrap");

    let reloaded = runtime.pending_releases();
    assert_eq!(
        reloaded.len(),
        1,
        "pending releases buffer survived restart",
    );
    assert_eq!(reloaded[0].released_branch_id(), released);
}

fn publish_table_for_recovery(
    backend: &'static RecoveryTestBackend,
    branch: BranchId,
    level: u8,
    identity: &str,
    rows: &[StorageRow],
) -> crate::format::TableManifestTableRef {
    use crate::format::{
        TableManifestTableBounds, TableManifestTableFacts, TableManifestTableProvenance,
        TableManifestTableRef,
    };
    use crate::table::{ImmutableTableReader, TablePhysicalKeyBytes, TableReaderConfig};
    let identity = TableIdentity::new(identity).expect("table identity");
    let bytes = table_bytes(identity.clone(), rows);
    let object_facts = TableObjectService::new(backend)
        .publish_create(
            &branch.to_string(),
            u32::from(level),
            identity.as_str(),
            &bytes,
        )
        .expect("publish table object");
    let reader =
        ImmutableTableReader::open_bytes(identity.clone(), bytes, TableReaderConfig::default())
            .expect("table reader");
    let table_rows = reader.rows();
    let (timestamp_min, timestamp_max) = {
        let mut timestamps = table_rows.iter().map(TableRow::commit_timestamp);
        match timestamps.next() {
            Some(first) => {
                let (min, max) =
                    timestamps.fold((first, first), |(min, max), ts| (min.min(ts), max.max(ts)));
                (Some(min), Some(max))
            }
            None => (None, None),
        }
    };
    let facts = TableManifestTableFacts::new(
        reader.facts().byte_count(),
        reader.facts().row_count(),
        reader.facts().data_block_count(),
        reader.facts().commit_range().min(),
        reader.facts().commit_range().max(),
        timestamp_min,
        timestamp_max,
    )
    .expect("table manifest facts");
    let bounds = {
        let first = table_rows.first().expect("table has at least one row");
        let mut physical_first = TablePhysicalKeyBytes::from_row(first.row());
        let mut physical_last = physical_first.clone();
        let mut internal_first = first.key().clone();
        let mut internal_last = internal_first.clone();
        for row in table_rows.iter().skip(1) {
            let physical = TablePhysicalKeyBytes::from_row(row.row());
            if physical < physical_first {
                physical_first = physical.clone();
            }
            if physical > physical_last {
                physical_last = physical;
            }
            if row.key() < &internal_first {
                internal_first = row.key().clone();
            }
            if row.key() > &internal_last {
                internal_last = row.key().clone();
            }
        }
        TableManifestTableBounds::new(
            physical_first.as_slice().to_vec(),
            physical_last.as_slice().to_vec(),
            internal_first.as_slice().to_vec(),
            internal_last.as_slice().to_vec(),
        )
        .expect("table manifest bounds")
    };
    TableManifestTableRef::new(
        identity,
        object_facts.object().clone(),
        0,
        facts,
        bounds,
        TableManifestTableProvenance::Flush,
    )
    .expect("table manifest ref")
}

fn assemble_shell(
    plan: StorageOpenPlan,
    branch: BranchId,
    backend: &'static RecoveryTestBackend,
) -> LifecycleResult<LifecycleDurableLocalShell<'static>> {
    assemble_shell_with_branch_config(plan, branch, backend, BranchRuntimeConfig::default())
}

/// #3319 S3b: the replay-flush tests need the branch config PRODUCTION
/// derives from the budget (the rotation threshold), not the default.
fn assemble_shell_with_branch_config(
    plan: StorageOpenPlan,
    branch: BranchId,
    backend: &'static RecoveryTestBackend,
    branch_config: BranchRuntimeConfig,
) -> LifecycleResult<LifecycleDurableLocalShell<'static>> {
    LifecycleDurableLocalShell::assemble(
        LifecycleDurableLocalOpenRequest::new(
            plan,
            DATABASE_ID,
            branch,
            CommitBranchGeneration::new(1).expect("generation"),
            branch_config,
            CommitRuntimeConfig::default(),
            WalServiceConfig::default(),
        )?,
        backend,
        timestamp_source(),
    )
}

fn open_plan(recovery_policy: RecoveryStrictness) -> StorageOpenPlan {
    open_plan_for_mode(StorageMode::DurableLocalStandard, recovery_policy)
}

fn open_plan_for_mode(mode: StorageMode, recovery_policy: RecoveryStrictness) -> StorageOpenPlan {
    StorageOpenPlan::new(
        mode,
        LifecycleCodecId::identity(),
        recovery_policy,
        LifecycleConfig::default(),
    )
    .expect("open plan")
}

fn open_plan_with_budget(
    recovery_policy: RecoveryStrictness,
    budget: StorageRuntimeBudget,
) -> StorageOpenPlan {
    let config = LifecycleConfig::default()
        .with_storage_budget(budget)
        .expect("budget config");
    StorageOpenPlan::new(
        StorageMode::DurableLocalStandard,
        LifecycleCodecId::identity(),
        recovery_policy,
        config,
    )
    .expect("open plan")
}

fn lossy_open_plan() -> StorageOpenPlan {
    let config = LifecycleConfig::new(
        1024,
        16,
        LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
        LifecycleLossyRecoveryPolicy::ExplicitlyAllowed,
    )
    .expect("lossy lifecycle config");
    StorageOpenPlan::new(
        StorageMode::DurableLocalStandard,
        LifecycleCodecId::identity(),
        RecoveryStrictness::AllowExplicitLossyFallback,
        config,
    )
    .expect("lossy open plan")
}

fn publish_snapshot(
    backend: &'static RecoveryTestBackend,
    snapshot_id: u64,
    watermark: CommitVersion,
    rows: &[StorageRow],
) {
    SnapshotService::new(backend)
        .publish_create(SnapshotPublishRequest::new(
            snapshot_id,
            watermark,
            Timestamp::from_micros(7_000),
            DATABASE_ID,
            "identity",
            vec![encode_checkpoint_row_section(rows).expect("row section")],
        ))
        .expect("publish snapshot");
}

fn write_manifest(backend: &'static RecoveryTestBackend, manifest: &DatabaseManifest) {
    backend.write_raw(
        ObjectLayout::database_manifest().expect("database root object"),
        encode_manifest(manifest).expect("database root bytes"),
    );
    // A checkpoint-attested manifest implies a WAL exists (#2765: assembly
    // refuses attested stores with no segments). Plant a header-only active
    // segment when the fixture has not staged one; deliberate plants (before
    // or after) are never clobbered.
    if manifest.snapshot_watermark().is_some() {
        let segment =
            ObjectLayout::wal_segment(manifest.active_wal_segment()).expect("segment object");
        if backend.object_metadata(&segment).is_err() {
            backend.write_raw(
                segment,
                wal_segment_bytes(manifest.active_wal_segment(), &[]),
            );
        }
    }
}

fn wal_segment_bytes(segment_id: u64, records: &[WalRecord]) -> Vec<u8> {
    let mut bytes = encode_wal_segment_header(&WalSegmentHeader::new(segment_id, DATABASE_ID));
    for record in records {
        bytes.extend_from_slice(&wal_record_frame(record));
    }
    bytes
}

fn wal_record_frame(record: &WalRecord) -> Vec<u8> {
    let record_bytes = encode_wal_record(record).expect("encode WAL record");
    let envelope = WalRecordEnvelope::new(record_bytes).expect("WAL record envelope");
    encode_wal_record_envelope(&envelope).expect("encode WAL record envelope")
}

fn wal_record(
    branch: BranchId,
    version: u64,
    user_key: &'static [u8],
    value: &'static [u8],
) -> WalRecord {
    let commit_version = CommitVersion::new(version);
    let timestamp = Timestamp::from_micros(version * 100);
    let stamp = CommitStamp::new(branch, commit_version, timestamp).expect("stamp");
    let row = StorageRow::put(
        physical_key(branch, user_key),
        commit_version,
        timestamp,
        Timestamp::EPOCH,
        value.to_vec(),
    );
    let timeline_rows =
        CommitTimelineRows::from_entry(CommitTimelineEntry::from_stamp(stamp).expect("entry"))
            .expect("timeline rows")
            .into_rows();
    let payload = WalCommitPayload::new(vec![
        row,
        timeline_rows[0].clone(),
        timeline_rows[1].clone(),
    ])
    .expect("payload");
    WalRecord::new(commit_version, branch, timestamp, payload).expect("record")
}

fn timeline_only_wal_record(branch: BranchId, version: u64) -> WalRecord {
    let commit_version = CommitVersion::new(version);
    let timestamp = Timestamp::from_micros(version * 100);
    let stamp = CommitStamp::new(branch, commit_version, timestamp).expect("stamp");
    let timeline_rows =
        CommitTimelineRows::from_entry(CommitTimelineEntry::from_stamp(stamp).expect("entry"))
            .expect("timeline rows")
            .into_rows();
    let payload = WalCommitPayload::new(timeline_rows.into()).expect("payload");
    WalRecord::new(commit_version, branch, timestamp, payload).expect("record")
}

fn user_only_wal_record(
    branch: BranchId,
    version: u64,
    user_key: &'static [u8],
    value: &'static [u8],
) -> WalRecord {
    let commit_version = CommitVersion::new(version);
    let timestamp = Timestamp::from_micros(version * 100);
    let row = StorageRow::put(
        physical_key(branch, user_key),
        commit_version,
        timestamp,
        Timestamp::EPOCH,
        value.to_vec(),
    );
    let payload = WalCommitPayload::new(vec![row]).expect("payload");
    WalRecord::new(commit_version, branch, timestamp, payload).expect("record")
}

fn put_row(
    branch: BranchId,
    version: u64,
    user_key: &'static [u8],
    value: &'static [u8],
) -> StorageRow {
    StorageRow::put(
        physical_key(branch, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(version * 100),
        Timestamp::EPOCH,
        value.to_vec(),
    )
}

fn table_bytes(identity: TableIdentity, rows: &[StorageRow]) -> Vec<u8> {
    let rows = rows.iter().cloned().map(TableRow::new).collect::<Vec<_>>();
    ImmutableTableBuilder::new(TableBuilderConfig::default())
        .expect("table builder")
        .build_from_rows(identity, &rows)
        .expect("build table")
        .into_bytes()
}

fn physical_key(branch: BranchId, user_key: &'static [u8]) -> PhysicalKey {
    PhysicalKey::new(
        branch,
        "lifecycle",
        StorageSpaceId::engine(0x20).expect("space"),
        user_key.to_vec(),
    )
    .expect("physical key")
}

fn durable_standard_batch(
    branch: BranchId,
    user_key: &'static [u8],
    value: &'static [u8],
) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            physical_key(branch, user_key),
            value.to_vec(),
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

fn assert_commit_runtime_source(error: &LifecycleError, expected: &CommitRuntimeError) {
    assert!(matches!(
        error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::CommitRuntime,
            reason: "commit runtime failed",
            ..
        }
    ));
    let source = error.source().expect("commit source");
    let commit_error = source
        .downcast_ref::<CommitRuntimeError>()
        .expect("commit runtime source");
    assert_eq!(commit_error, expected);
}

fn branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; 16])
}

fn timestamp_source() -> CommitManualTimestampSource {
    CommitManualTimestampSource::new(Timestamp::from_micros(8_000))
}

#[derive(Debug)]
struct RecoveryTestBackend {
    capabilities: BackendCapabilities,
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    fail_reads: Mutex<BTreeSet<ObjectName>>,
    lock_held: Arc<AtomicBool>,
}

impl RecoveryTestBackend {
    fn new() -> Self {
        let backend = Self {
            capabilities: BackendCapabilities::from_slice(DURABLE_LOCAL_MODE_REQUIREMENTS),
            objects: Mutex::new(BTreeMap::new()),
            fail_reads: Mutex::new(BTreeSet::new()),
            lock_held: Arc::new(AtomicBool::new(false)),
        };
        // #3015: assemble no longer fabricates a database manifest over
        // durable residue, so every staged store carries the same empty
        // manifest the old fabrication used to produce. Tests that publish
        // their own manifest simply overwrite this seed.
        let manifest = DatabaseManifest::new(DATABASE_ID, "identity").expect("database manifest");
        backend.write_raw(
            ObjectLayout::database_manifest().expect("manifest object"),
            encode_manifest(&manifest).expect("database manifest bytes"),
        );
        backend
    }

    /// A genuinely empty backend — for the fresh-store bootstrap tests that
    /// assert the CREATED disposition and zero visibility.
    fn new_unseeded() -> Self {
        Self {
            capabilities: BackendCapabilities::from_slice(DURABLE_LOCAL_MODE_REQUIREMENTS),
            objects: Mutex::new(BTreeMap::new()),
            fail_reads: Mutex::new(BTreeSet::new()),
            lock_held: Arc::new(AtomicBool::new(false)),
        }
    }

    fn write_raw(&self, object: ObjectName, bytes: Vec<u8>) {
        self.objects.lock().expect("objects").insert(object, bytes);
    }

    fn append_raw(&self, object: ObjectName, bytes: &[u8]) {
        self.objects
            .lock()
            .expect("objects")
            .entry(object)
            .or_default()
            .extend_from_slice(bytes);
    }

    fn object_bytes(&self, object: &ObjectName) -> Option<Vec<u8>> {
        self.objects.lock().expect("objects").get(object).cloned()
    }

    fn fail_read_object(&self, object: ObjectName) {
        self.fail_reads
            .lock()
            .expect("read failures")
            .insert(object);
    }
}

impl Backend for RecoveryTestBackend {
    fn capabilities(&self) -> BackendCapabilities {
        self.capabilities
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        if self
            .fail_reads
            .lock()
            .expect("read failures")
            .contains(name)
        {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "injected read failure",
            ));
        }
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        let bytes = self.read_object(name)?;
        let start = usize::try_from(range.offset()).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end_offset().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
        Ok(bytes[start.min(bytes.len())..end.min(bytes.len())].to_vec())
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("objects")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        let removed = self.objects.lock().expect("objects").remove(name).is_some();
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        let mut names: Vec<_> = self
            .objects
            .lock()
            .expect("objects")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        if self.lock_held.swap(true, Ordering::SeqCst) {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "writer lock already held",
            ));
        }
        Ok(BackendWriterGuard::new(
            name.clone(),
            HeldWriterLock {
                locked: Arc::clone(&self.lock_held),
            },
        ))
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        let mut objects = self.objects.lock().expect("objects");
        let object = objects.entry(name.clone()).or_default();
        let start_offset = object.len() as u64;
        object.extend_from_slice(bytes);
        Ok(BackendAppend::new(
            start_offset,
            bytes.len() as u64,
            BackendMetadata::new(object.len() as u64, None),
        ))
    }

    fn sync_object(&self, _name: &ObjectName) -> crate::backend::BackendResult<()> {
        Ok(())
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        let mut objects = self.objects.lock().expect("objects");
        if mode == PublishMode::Create && objects.contains_key(name) {
            return Err(PublishError::precondition_failed(
                name,
                "object already exists",
            ));
        }
        objects.insert(name.clone(), bytes.to_vec());
        Ok(PublishOutcome::new(
            name.clone(),
            BackendMetadata::new(bytes.len() as u64, None),
            PublishDurability::Durable,
        ))
    }
}

struct HeldWriterLock {
    locked: Arc<AtomicBool>,
}

impl Drop for HeldWriterLock {
    fn drop(&mut self) {
        self.locked.store(false, Ordering::SeqCst);
    }
}

struct MaintenanceTestRunner;

impl MaintenanceTaskRunner for MaintenanceTestRunner {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        Ok(MaintenanceOutcome::new(
            task.kind(),
            MaintenanceOutcomeStatus::Completed,
        ))
    }
}

/// A torn FINAL record is the mid-append crash artifact: its write never
/// completed, so its group sync never ran and no ack was issued — the
/// write-ordering contract keeps every durable reference behind synced WAL.
/// Strict recovery therefore repairs it without recording data loss; the
/// commit-watermark verification below stays the attestation backstop.
#[test]
fn recovery_repairs_latest_partial_log_tail_in_strict_mode_without_faults() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x43);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 2, b"valid", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append valid record");
    let wal_object = ObjectLayout::wal_segment(1).expect("active log object");
    let valid_end = backend
        .object_bytes(&wal_object)
        .expect("intact WAL bytes")
        .len() as u64;
    backend.append_raw(wal_object.clone(), b"partial");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("strict recovery repairs the torn unacknowledged tail");

    let repair = outcome.wal().repair().expect("repair is recorded");
    assert_eq!(repair.removed_bytes(), b"partial".len() as u64);
    assert_eq!(
        backend
            .object_bytes(&wal_object)
            .expect("repaired WAL bytes")
            .len() as u64,
        valid_end,
        "repair removes exactly the torn suffix"
    );
    assert_eq!(
        outcome.health(),
        &RecoveryHealth::Healthy,
        "an unacknowledged torn tail is not data loss"
    );
    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records")
            .len(),
        1,
        "the intact record survives the repair"
    );
}

/// The attestation backstop: when the durable commit watermark attests a
/// version the torn tail was carrying, the repair may not silently drop it —
/// strict recovery must refuse as corruption (the #2690 watermark contract).
#[test]
fn strict_recovery_still_refuses_a_torn_tail_the_commit_watermark_attests() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    backend.write_raw(
        ObjectLayout::wal_watermark().expect("watermark object"),
        crate::format::encode_wal_watermark(3).expect("watermark bytes"),
    );
    let branch = branch_id(0x44);
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 2, b"valid", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append valid record");
    let wal_object = ObjectLayout::wal_segment(1).expect("active log object");
    backend.append_raw(wal_object.clone(), b"partial");
    let before = backend.object_bytes(&wal_object).expect("WAL bytes before");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("attested commits above the repaired tail must refuse");
    assert_eq!(
        error.code(),
        "corruption.lifecycle.recovery_corruption",
        "the watermark backstop classifies the loss as corruption: {error:?}"
    );
    assert_eq!(
        backend.object_bytes(&wal_object).expect("WAL bytes after"),
        before,
        "a refused recovery must not mutate the torn tail — the bytes are forensic evidence"
    );
}

/// #2567 S3a: the attestation backstop sees only the REPLAYED max under the
/// contiguity fence. An orphaned-delta recovery drops the tail above the
/// fence; a durable commit watermark attested INSIDE that dropped tail must
/// still refuse (strict) — letting the dropped records satisfy the #2690
/// check would silently reopen without attested commits.
#[test]
fn strict_recovery_refuses_attested_commits_hidden_in_the_orphaned_tail() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    // Attested through 4; the only surviving WAL record is version 4, above
    // the gap at 1..=3, so the contiguity fence drops it.
    backend.write_raw(
        ObjectLayout::wal_watermark().expect("watermark object"),
        crate::format::encode_wal_watermark(4).expect("watermark bytes"),
    );
    let branch = branch_id(0x4d);
    // An orphaned delta: rowless snapshot at watermark 3 whose recorded
    // table-manifest base floor (2) sits below it, and no per-branch table
    // manifest survives — recovery must fall back to the WAL-contiguous
    // prefix (empty here).
    let snapshot = crate::format::SnapshotContainer::new(
        crate::format::SnapshotHeader::new(
            8,
            CommitVersion::new(3),
            Timestamp::from_micros(1_100),
            DATABASE_ID,
            "identity",
        )
        .expect("header"),
        Vec::new(),
    );
    backend.write_raw(
        ObjectLayout::snapshot(8).expect("snapshot object"),
        crate::format::encode_snapshot_container(&snapshot).expect("snapshot bytes"),
    );
    write_manifest(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .expect("database root")
            .with_recovery_facts(1, Some(3), Some(8), Some(CommitVersion::new(2)))
            .expect("database root facts"),
    );
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)
        .expect("durable shell");
    let record = wal_record(branch, 4, b"above-gap", b"value");
    shell
        .services_mut()
        .wal_mut()
        .append(&record)
        .expect("append above-gap record");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("attested commits inside the dropped orphan tail must refuse");

    assert_eq!(
        error.code(),
        "corruption.lifecycle.recovery_corruption",
        "the watermark backstop, not a fault demotion, must refuse: {error:?}"
    );
}

/// The #2765 forbidden state (checkpoint-attested manifest, zero WAL
/// segments) reached through legal torn-rename crash semantics: strict mode
/// refuses (pinned below and at the wire), but explicit lossy recovery is
/// the operator's informed reopen path — it must degrade with a recorded
/// data-loss fault and recover what the checkpoint holds, not refuse.
#[test]
fn lossy_recovery_degrades_when_checkpoint_attested_wal_chain_is_missing() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x45);
    // Raw manifest write: `write_manifest` deliberately plants a header-only
    // segment for attested manifests (#2765 fixtures); the forbidden state
    // needs the attestation WITHOUT any segment object.
    backend.write_raw(
        ObjectLayout::database_manifest().expect("database root object"),
        encode_manifest(
            &DatabaseManifest::new(DATABASE_ID, "identity")
                .expect("database root")
                .with_recovery_facts(1, Some(9), Some(7), None)
                .expect("database root facts"),
        )
        .expect("database root bytes"),
    );

    let mut shell = assemble_shell(lossy_open_plan(), branch, backend)
        .expect("lossy assembly proceeds past the missing WAL chain");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("lossy recovery degrades instead of refusing");

    assert!(
        matches!(
            outcome.health(),
            RecoveryHealth::Degraded {
                class: RecoveryDegradationClass::DataLoss,
                faults
            } if faults
                .iter()
                .any(|fault| fault.kind() == RecoveryFaultKind::WalCommittedSuffixMissing)
        ),
        "the missing WAL chain must surface as a data-loss fault: {:?}",
        outcome.health()
    );
}

/// Direction control: the strict arm of the #2765 guard is unchanged — a
/// checkpoint-attested store with no WAL segments still refuses to open.
#[test]
fn strict_open_still_refuses_a_checkpoint_attested_store_with_no_wal() {
    let backend: &'static RecoveryTestBackend =
        crate::testkit::leak_static(RecoveryTestBackend::new());
    let branch = branch_id(0x46);
    backend.write_raw(
        ObjectLayout::database_manifest().expect("database root object"),
        encode_manifest(
            &DatabaseManifest::new(DATABASE_ID, "identity")
                .expect("database root")
                .with_recovery_facts(1, Some(9), Some(7), None)
                .expect("database root facts"),
        )
        .expect("database root bytes"),
    );

    let Err(error) = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend) else {
        panic!("strict assembly must refuse the gutted store");
    };
    assert_eq!(error.code(), "corruption.lifecycle.recovery_corruption");
}
