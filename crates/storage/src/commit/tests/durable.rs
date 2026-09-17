use super::*;
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::read::BranchReadView;
use crate::branch::state::BranchLocalState;
use crate::row::{PhysicalKey, StorageRow};
use std::error::Error as _;

#[test]
fn durable_standard_commit_appends_wal_record_then_applies_rows_and_publishes_visible() {
    let branch = branch_id(51);
    let key = physical_key(branch, 0x20, b"durable-standard".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            key.clone(),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    let outcome = fixture.execute(batch).expect("standard commit succeeds");

    assert_eq!(outcome.kind(), CommitOutcomeKind::Visible);
    assert_eq!(outcome.phase(), CommitPhase::Visible);
    assert_eq!(outcome.durability(), CommitDurabilityClass::Standard);
    assert_eq!(outcome.commit_version(), Some(CommitVersion::new(1)));
    assert_eq!(
        outcome.commit_timestamp(),
        Some(Timestamp::from_micros(1_000))
    );
    assert_eq!(
        outcome.visibility_facts(),
        CommitVisibilityFacts::new(
            Some(CommitVersion::new(1)),
            Some(CommitVersion::new(1)),
            Some(CommitVersion::new(1)),
            Some(CommitVersion::new(1)),
            Some(CommitVersion::new(1)),
        )
        .expect("durable visible facts")
    );
    assert_eq!(fixture.visible.visible_version(), CommitVersion::new(1));
    assert_eq!(fixture.wal.records.len(), 1);
    let record = &fixture.wal.records[0];
    assert_eq!(record.branch_id(), branch);
    assert_eq!(record.commit_version(), CommitVersion::new(1));
    assert_eq!(record.commit_timestamp(), Timestamp::from_micros(1_000));
    assert_eq!(record.commit_payload().rows().len(), 1);
    assert_eq!(record.commit_payload().rows()[0].physical_key(), &key);
    assert_eq!(
        record.commit_payload().rows()[0].commit_version(),
        CommitVersion::new(1)
    );
    assert_record_rows_share_commit_facts(
        record,
        branch,
        CommitVersion::new(1),
        Timestamp::from_micros(1_000),
    );
    assert_payload_contains_timeline_rows(record);

    let view = fixture.state.capture_read_view().expect("read view");
    assert_eq!(
        view.latest(&key)
            .expect("latest read")
            .expect("row")
            .row()
            .value(),
        b"value"
    );
    let timeline = timeline_view(&fixture.state, branch);
    assert_eq!(
        timeline
            .version_at_or_before(Timestamp::from_micros(1_000))
            .matched_version(),
        Some(CommitVersion::new(1))
    );
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_blind_commit_does_not_capture_conflict_read_view() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(97);
    let key = physical_key(branch, 0x20, b"blind-durable".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            key,
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    fixture
        .execute(batch)
        .expect("blind durable commit succeeds");

    let perf = crate::observability::perf_trace::snapshot();

    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 1);
    assert_eq!(perf.commit_conflict_validation_without_source(), 1);
    assert_eq!(perf.commit_conflict_validation_with_source(), 0);
    assert_eq!(perf.commit_conflict_read_facts_checked(), 0);
    assert_eq!(perf.commit_conflict_cas_facts_checked(), 0);
    assert_eq!(perf.commit_conflicts_detected(), 0);
    assert_eq!(perf.commit_batches_prepared(), 1);
    assert_eq!(perf.commit_user_mutation_rows(), 1);
    // W3.1c: timeline rows are elided — the retained index derives them.
    assert_eq!(perf.commit_timeline_rows_prepared(), 0);
    assert_eq!(perf.commit_rows_prepared(), 1);
    assert_eq!(perf.commit_wal_records_built(), 1);
    assert_eq!(perf.commit_wal_record_rows(), 1);
    assert_eq!(perf.commit_wal_appends(), 1);
    assert_eq!(perf.commit_wal_append_bytes(), 128);
    assert_eq!(perf.commit_visible_publish_attempts(), 1);
    assert_eq!(perf.commit_visible_publish_successes(), 1);
    assert_eq!(perf.commit_visible_publish_failures(), 0);
    assert_eq!(perf.commit_admission_pressure_facts(), 1);
    assert_eq!(perf.commit_admission_under_pressure(), 0);
    assert_eq!(perf.commit_admission_accepted_under_pressure(), 0);
    assert_eq!(perf.commit_admission_requires_maintenance(), 0);
    assert_eq!(perf.commit_admission_mutations(), 1);
    assert!(perf.commit_admission_approx_bytes() > 0);
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
    assert_eq!(perf.commit_unresolved_gate_rejected_unresolved(), 0);
    assert_eq!(perf.commit_branch_registry_lookups(), 1);
    assert_eq!(perf.commit_branch_registry_descriptors_scanned(), 1);
    assert_eq!(perf.commit_branch_guard_attempts(), 1);
    assert_eq!(perf.commit_branch_guard_acquired(), 1);
    assert_eq!(perf.commit_branch_guard_rejected(), 0);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.read_view_rows_cloned(), 0);
    assert_eq!(perf.read_view_validation_rows_scanned(), 0);
    assert_eq!(fixture.wal.records.len(), 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_commit_records_admission_pressure_before_wal_work() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(108);
    let config = CommitRuntimeConfig::default()
        .with_admission_pressure_thresholds(
            CommitAdmissionPressureThresholds::new(Some(1), None, Some(1), None)
                .expect("thresholds"),
        )
        .expect("config");
    let mut fixture = DurableFixture::new(
        branch,
        config,
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-pressure".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    fixture
        .execute(batch)
        .expect("pressure-marked durable commit succeeds");

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_admission_pressure_facts(), 1);
    assert_eq!(perf.commit_admission_under_pressure(), 1);
    assert_eq!(perf.commit_admission_accepted_under_pressure(), 1);
    assert_eq!(perf.commit_admission_requires_maintenance(), 1);
    assert_eq!(perf.commit_admission_mutations(), 1);
    assert!(perf.commit_admission_approx_bytes() > 0);
    assert_eq!(perf.commit_wal_records_built(), 1);
    assert_eq!(perf.commit_wal_appends(), 1);
    assert_eq!(perf.commit_visible_publish_successes(), 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_commit_records_byte_admission_pressure_before_wal_work() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(109);
    let config = CommitRuntimeConfig::default()
        .with_admission_pressure_thresholds(
            CommitAdmissionPressureThresholds::new(None, Some(64), None, Some(128))
                .expect("thresholds"),
        )
        .expect("config");
    let mut fixture = DurableFixture::new(
        branch,
        config,
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-byte-pressure".to_vec()),
            vec![0x42; 256],
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    fixture
        .execute(batch)
        .expect("byte pressure-marked durable commit succeeds");

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_admission_pressure_facts(), 1);
    assert_eq!(perf.commit_admission_under_pressure(), 1);
    assert_eq!(perf.commit_admission_accepted_under_pressure(), 1);
    assert_eq!(perf.commit_admission_requires_maintenance(), 1);
    assert_eq!(perf.commit_admission_mutations(), 1);
    assert!(perf.commit_admission_approx_bytes() >= 256);
    assert_eq!(perf.commit_wal_records_built(), 1);
    assert_eq!(perf.commit_wal_appends(), 1);
    assert_eq!(perf.commit_visible_publish_successes(), 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_blind_delete_does_not_capture_conflict_read_view() {
    let branch = branch_id(104);
    let key = physical_key(branch, 0x20, b"blind-delete-durable".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture.seed_visible_row(StorageRow::put(
        key.clone(),
        CommitVersion::new(1),
        Timestamp::from_micros(1_000),
        Timestamp::EPOCH,
        b"old".to_vec(),
    ));
    fixture.catch_up_to(CommitVersion::new(1), Timestamp::from_micros(1_000));
    fixture.visible = VisibleVersionTracker::new(CommitVersion::new(1));
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::delete(key)],
    );

    fixture
        .execute(batch)
        .expect("blind durable delete succeeds");

    let perf = crate::observability::perf_trace::snapshot();

    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 1);
    assert_eq!(perf.commit_conflict_validation_without_source(), 1);
    assert_eq!(perf.commit_conflict_validation_with_source(), 0);
    assert_eq!(perf.commit_conflict_read_facts_checked(), 0);
    assert_eq!(perf.commit_conflict_cas_facts_checked(), 0);
    assert_eq!(perf.commit_conflicts_detected(), 0);
    assert_eq!(perf.commit_batches_prepared(), 1);
    assert_eq!(perf.commit_user_mutation_rows(), 1);
    // W3.1c: timeline rows are elided — the retained index derives them.
    assert_eq!(perf.commit_timeline_rows_prepared(), 0);
    assert_eq!(perf.commit_rows_prepared(), 1);
    assert_eq!(perf.commit_wal_records_built(), 1);
    assert_eq!(perf.commit_wal_record_rows(), 1);
    assert_eq!(perf.commit_wal_appends(), 1);
    assert_eq!(perf.commit_visible_publish_attempts(), 1);
    assert_eq!(perf.commit_visible_publish_successes(), 1);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.read_view_rows_cloned(), 0);
    assert_eq!(perf.read_view_validation_rows_scanned(), 0);
    assert_eq!(fixture.wal.records.len(), 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_cas_commit_still_captures_read_view_and_rejects_before_wal_append() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(98);
    let key = physical_key(branch, 0x20, b"durable-stale-cas".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture.seed_visible_row(StorageRow::put(
        key.clone(),
        CommitVersion::new(1),
        Timestamp::from_micros(1_000),
        Timestamp::EPOCH,
        b"old".to_vec(),
    ));
    fixture.catch_up_to(CommitVersion::new(1), Timestamp::from_micros(1_000));
    fixture.visible = VisibleVersionTracker::new(CommitVersion::new(1));
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-stale-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            Vec::new(),
            vec![CommitCasFact::new(key, CommitObservedVersion::Missing)],
        ),
    );

    assert!(matches!(
        fixture.execute(batch),
        Err(CommitRuntimeError::CommitConflict { conflict })
            if conflict.kind() == CommitConflictKind::Cas
    ));

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.conflict_sources_built(), 1);
    assert_eq!(perf.commit_conflict_validation_calls(), 1);
    assert_eq!(perf.commit_conflict_validation_without_source(), 0);
    assert_eq!(perf.commit_conflict_validation_with_source(), 1);
    assert_eq!(perf.commit_conflict_read_facts_checked(), 0);
    assert_eq!(perf.commit_conflict_cas_facts_checked(), 1);
    assert_eq!(perf.commit_conflicts_detected(), 1);
    assert_eq!(perf.commit_batches_prepared(), 0);
    assert_eq!(perf.commit_wal_records_built(), 0);
    assert_eq!(perf.commit_wal_appends(), 0);
    assert_eq!(perf.commit_visible_publish_attempts(), 0);
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
    assert_eq!(perf.commit_branch_registry_lookups(), 1);
    assert_eq!(perf.commit_branch_registry_descriptors_scanned(), 1);
    assert_eq!(perf.commit_branch_guard_attempts(), 1);
    assert_eq!(perf.commit_branch_guard_acquired(), 1);
    assert_eq!(perf.read_view_captures(), 1);
    assert!(perf.read_view_source_handles_cloned() > 0);
    assert_eq!(perf.read_view_rows_cloned(), 0);
    assert_eq!(perf.read_view_row_clone_bytes(), 0);
    assert_eq!(perf.read_view_validation_rows_scanned(), 0);
    assert!(fixture.wal.records.is_empty());
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::new(1)
    );
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_read_and_cas_validation_perf_trace_builds_one_source_before_wal() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(99);
    let read_key = physical_key(branch, 0x20, b"durable-read-pass".to_vec());
    let cas_key = physical_key(branch, 0x20, b"durable-cas-pass".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture.seed_visible_row(StorageRow::put(
        read_key.clone(),
        CommitVersion::new(1),
        Timestamp::from_micros(1_000),
        Timestamp::EPOCH,
        b"read-old".to_vec(),
    ));
    fixture.seed_visible_row(StorageRow::put(
        cas_key.clone(),
        CommitVersion::new(2),
        Timestamp::from_micros(2_000),
        Timestamp::EPOCH,
        b"cas-old".to_vec(),
    ));
    fixture.catch_up_to(CommitVersion::new(2), Timestamp::from_micros(2_000));
    fixture.visible = VisibleVersionTracker::new(CommitVersion::new(2));
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-validated-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                read_key,
                CommitObservedVersion::Present(CommitVersion::new(1)),
            )],
            vec![CommitCasFact::new(
                cas_key,
                CommitObservedVersion::Present(CommitVersion::new(2)),
            )],
        ),
    );

    fixture
        .execute(batch)
        .expect("validated durable commit succeeds");

    let perf = crate::observability::perf_trace::snapshot();

    assert_eq!(perf.conflict_sources_built(), 1);
    assert_eq!(perf.commit_conflict_validation_calls(), 1);
    assert_eq!(perf.commit_conflict_validation_without_source(), 0);
    assert_eq!(perf.commit_conflict_validation_with_source(), 1);
    assert_eq!(perf.commit_conflict_read_facts_checked(), 1);
    assert_eq!(perf.commit_conflict_cas_facts_checked(), 1);
    assert_eq!(perf.commit_conflicts_detected(), 0);
    let validation_fact_count = 2;
    assert_eq!(perf.point_active_probes(), validation_fact_count);
    assert_eq!(perf.point_frozen_probes(), 0);
    assert_eq!(perf.point_owned_l0_table_probes(), 0);
    assert_eq!(perf.point_owned_nonzero_table_probes(), 0);
    assert_eq!(perf.point_inherited_l0_table_probes(), 0);
    assert_eq!(perf.point_inherited_nonzero_table_probes(), 0);
    assert!(perf.point_table_seeks() <= validation_fact_count);
    assert!(perf.point_rows_visited() <= validation_fact_count);
    assert!(perf.point_candidates_materialized() <= validation_fact_count);
    assert!(perf.table_point_lookup_key_builds() <= validation_fact_count);
    assert_eq!(perf.commit_batches_prepared(), 1);
    assert_eq!(perf.commit_user_mutation_rows(), 1);
    // W3.1c: timeline rows are elided — the retained index derives them.
    assert_eq!(perf.commit_timeline_rows_prepared(), 0);
    assert_eq!(perf.commit_rows_prepared(), 1);
    assert_eq!(perf.commit_wal_records_built(), 1);
    assert_eq!(perf.commit_wal_record_rows(), 1);
    assert_eq!(perf.commit_wal_appends(), 1);
    assert_eq!(perf.commit_wal_append_bytes(), 128);
    assert_eq!(perf.commit_visible_publish_attempts(), 1);
    assert_eq!(perf.commit_visible_publish_successes(), 1);
    assert_eq!(perf.commit_unresolved_records(), 0);
    assert_eq!(perf.read_view_captures(), 1);
    assert_eq!(fixture.wal.records.len(), 1);
}

#[cfg(all(feature = "localfs", unix))]
#[test]
fn durable_standard_commit_appends_through_real_wal_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = crate::backend::local_fs::LocalFsBackend::new(dir.path());
    let branch = branch_id(58);
    let key = physical_key(branch, 0x20, b"real-wal".to_vec());
    let timestamp = Timestamp::from_micros(1_000);
    let mut registry = CommitBranchRegistry::new();
    registry
        .register_active(
            branch,
            CommitBranchGeneration::new(1).expect("branch generation"),
        )
        .expect("register branch");
    let guard_set = CommitBranchGuardSet::new();
    let mut allocator = CommitFactAllocator::new(
        CommitVersionAllocator::default(),
        CommitTimestampGuard::default(),
        CommitManualTimestampSource::new(timestamp),
    );
    let mut state =
        BranchLocalState::new(branch, BranchRuntimeConfig::default()).expect("branch state");
    let mut visible = FailingVisiblePublisher::new(false);
    let durable_gate = CommitUnresolvedDurableGate::new();
    let mut wal = crate::service::WalService::open(
        &backend,
        [0x58; 16],
        1,
        DurabilityPolicy::Standard,
        crate::service::WalServiceConfig::default(),
    )
    .expect("open real WAL service");
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            key.clone(),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    #[cfg(feature = "perf-trace")]
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let outcome = CommitDurableRuntime::new(
        &CommitRuntimeConfig::default(),
        &registry,
        &guard_set,
        &mut allocator,
        &mut state,
        &mut visible,
        &mut wal,
        &durable_gate,
    )
    .execute(
        batch,
        CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
    )
    .expect("durable commit through real WAL service");

    assert_eq!(outcome.kind(), CommitOutcomeKind::Visible);
    assert_eq!(visible.visible_version(), CommitVersion::new(1));
    let read = wal.read_all().expect("read real WAL service");
    assert_eq!(read.records().len(), 1);
    assert_record_rows_share_commit_facts(
        &read.records()[0],
        branch,
        CommitVersion::new(1),
        timestamp,
    );
    assert_payload_contains_timeline_rows(&read.records()[0]);
    assert_eq!(
        state
            .capture_read_view()
            .expect("read view")
            .latest(&key)
            .expect("latest read")
            .expect("row")
            .row()
            .value(),
        b"value"
    );
    #[cfg(feature = "perf-trace")]
    assert_real_wal_commit_perf(&crate::observability::perf_trace::snapshot(), 1);
}

#[cfg(all(feature = "localfs", unix))]
#[test]
fn durable_always_commit_appends_through_real_wal_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = crate::backend::local_fs::LocalFsBackend::new(dir.path());
    let branch = branch_id(59);
    let key = physical_key(branch, 0x20, b"real-durable-always".to_vec());
    let timestamp = Timestamp::from_micros(1_000);
    let mut registry = CommitBranchRegistry::new();
    registry
        .register_active(
            branch,
            CommitBranchGeneration::new(1).expect("branch generation"),
        )
        .expect("register branch");
    let guard_set = CommitBranchGuardSet::new();
    let mut allocator = CommitFactAllocator::new(
        CommitVersionAllocator::default(),
        CommitTimestampGuard::default(),
        CommitManualTimestampSource::new(timestamp),
    );
    let mut state =
        BranchLocalState::new(branch, BranchRuntimeConfig::default()).expect("branch state");
    let mut visible = VisibleVersionTracker::default();
    let durable_gate = CommitUnresolvedDurableGate::new();
    let mut wal = crate::service::WalService::open(
        &backend,
        [0x59; 16],
        1,
        DurabilityPolicy::Always,
        crate::service::WalServiceConfig::default(),
    )
    .expect("open always WAL service");
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Always,
        vec![CommitMutation::put(
            key,
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    #[cfg(feature = "perf-trace")]
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let outcome = CommitDurableRuntime::new(
        &CommitRuntimeConfig::default(),
        &registry,
        &guard_set,
        &mut allocator,
        &mut state,
        &mut visible,
        &mut wal,
        &durable_gate,
    )
    .execute(
        batch,
        CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
    )
    .expect("always durable commit through real WAL service");

    assert_eq!(outcome.kind(), CommitOutcomeKind::Visible);
    assert_eq!(outcome.durability(), CommitDurabilityClass::Always);
    let read = wal.read_all().expect("read always WAL service");
    assert_eq!(read.records().len(), 1);
    assert_record_rows_share_commit_facts(
        &read.records()[0],
        branch,
        CommitVersion::new(1),
        timestamp,
    );
    assert_payload_contains_timeline_rows(&read.records()[0]);
    #[cfg(feature = "perf-trace")]
    assert_real_wal_commit_perf(&crate::observability::perf_trace::snapshot(), 1);
}

#[test]
fn durable_standard_delete_appends_tombstone_and_hides_latest() {
    let branch = branch_id(60);
    let key = physical_key(branch, 0x20, b"durable-delete".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture.seed_visible_row(StorageRow::put(
        key.clone(),
        CommitVersion::new(1),
        Timestamp::from_micros(900),
        Timestamp::EPOCH,
        b"old".to_vec(),
    ));
    fixture.catch_up_to(CommitVersion::new(1), Timestamp::from_micros(900));
    fixture.visible = VisibleVersionTracker::new(CommitVersion::new(1));

    let outcome = fixture
        .execute(durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::delete(key.clone())],
        ))
        .expect("durable delete succeeds");

    assert_eq!(outcome.commit_version(), Some(CommitVersion::new(2)));
    assert_eq!(outcome.mutation_counts().puts(), 0);
    assert_eq!(outcome.mutation_counts().deletes(), 1);
    assert_eq!(outcome.mutation_counts().timeline_rows(), 0);
    let record = &fixture.wal.records[0];
    assert_eq!(record.commit_payload().rows()[0].physical_key(), &key);
    assert!(record.commit_payload().rows()[0].is_tombstone());
    assert_record_rows_share_commit_facts(
        record,
        branch,
        CommitVersion::new(2),
        Timestamp::from_micros(1_000),
    );
    assert!(fixture
        .state
        .capture_read_view()
        .expect("read view")
        .latest(&key)
        .expect("latest read")
        .is_none());
}

#[test]
fn durable_standard_mixed_batch_writes_user_rows_and_counts() {
    let branch = branch_id(61);
    let first = physical_key(branch, 0x20, b"mixed-first".to_vec());
    let deleted = physical_key(branch, 0x21, b"mixed-delete".to_vec());
    let second = physical_key(branch, 0x22, b"mixed-second".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );

    let outcome = fixture
        .execute(durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![
                CommitMutation::put(
                    first.clone(),
                    b"first".to_vec(),
                    CommitExpiry::None,
                    CommitRetentionHint::Append,
                ),
                CommitMutation::delete(deleted.clone()),
                CommitMutation::put(
                    second.clone(),
                    b"second".to_vec(),
                    CommitExpiry::None,
                    CommitRetentionHint::Append,
                ),
            ],
        ))
        .expect("mixed durable commit succeeds");

    assert_eq!(outcome.mutation_counts().puts(), 2);
    assert_eq!(outcome.mutation_counts().deletes(), 1);
    assert_eq!(outcome.mutation_counts().timeline_rows(), 0);
    let payload = fixture.wal.records[0].commit_payload().rows();
    assert_eq!(payload.len(), 3);
    assert_eq!(payload[0].physical_key(), &first);
    assert!(!payload[0].is_tombstone());
    assert_eq!(payload[1].physical_key(), &deleted);
    assert!(payload[1].is_tombstone());
    assert_eq!(payload[2].physical_key(), &second);
    assert!(!payload[2].is_tombstone());
    let view = fixture.state.capture_read_view().expect("read view");
    assert!(view
        .latest(&first)
        .expect("first read")
        .is_some_and(|row| row.row().value() == b"first"));
    assert!(view.latest(&deleted).expect("deleted read").is_none());
    assert!(view
        .latest(&second)
        .expect("second read")
        .is_some_and(|row| row.row().value() == b"second"));
}

#[test]
fn durable_always_commit_requires_forced_wal_append_fact() {
    let branch = branch_id(52);
    let key = physical_key(branch, 0x20, b"durable-always".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Always,
        FakeWalMode::Succeed {
            forced_durable: true,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Always,
        vec![CommitMutation::put(
            key,
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    let outcome = fixture.execute(batch).expect("always commit succeeds");

    assert_eq!(outcome.durability(), CommitDurabilityClass::Always);
    assert_eq!(fixture.wal.records.len(), 1);
    assert_record_rows_share_commit_facts(
        &fixture.wal.records[0],
        branch,
        CommitVersion::new(1),
        Timestamp::from_micros(1_000),
    );
}

#[test]
fn durable_always_commit_rejects_unforced_success_before_apply_phase() {
    let branch = branch_id(53);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Always,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Always,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"unforced".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert!(matches!(
        fixture.execute(batch),
        Err(CommitRuntimeError::DurabilityUncertain {
            branch_id: failed_branch,
            commit_version,
            reason: "always durability requires a forced WAL append",
            ..
        }) if failed_branch == branch && commit_version == CommitVersion::new(1)
    ));
    assert_eq!(fixture.wal.records.len(), 1);
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
    let _guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("guard can be reacquired after unforced durable failure");
}

#[test]
fn durable_read_only_batch_rejects_before_allocation_or_wal_append() {
    let branch = branch_id(62);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );

    assert_eq!(
        fixture.execute(CommitBatch::read_only_diagnostic(
            branch,
            CommitValidationFacts::empty(),
            CommitBatchOptions::new(
                CommitDurabilityMode::Standard,
                CommitConflictValidationMode::Validate,
                CommitDuplicateKeyPolicy::Reject,
                CommitTimestampPolicy::RuntimeGenerated,
                CommitOrigin::StorageRuntime,
            ),
        )),
        Err(CommitRuntimeError::InvalidBatch {
            reason: "durable commit executor requires mutating batch",
        })
    );
    assert_unallocated_unattempted(&fixture);
}

#[test]
fn durable_commit_rejects_cache_mode_before_allocation_or_wal_append() {
    let branch = branch_id(54);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Cache,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"cache".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::DurabilityUnavailable {
            reason: "durable commit executor requires durable mode",
        })
    );
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert_eq!(fixture.wal.records.len(), 0);
    assert_eq!(fixture.state.active_row_count(), 0);
}

#[test]
fn durable_branch_mismatch_rejects_before_allocation_or_wal_append() {
    let branch = branch_id(69);
    let other = branch_id(70);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        other,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(other, 0x20, b"wrong-branch".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::BranchMismatch {
            expected: other,
            actual: branch,
        })
    );
    assert_unallocated_unattempted(&fixture);
}

#[test]
fn durable_generation_mismatch_rejects_before_allocation_or_wal_append() {
    let branch = branch_id(63);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"stale-generation".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert_eq!(
        fixture.execute_with_generation(
            batch,
            CommitBranchGeneration::new(2).expect("stale generation"),
        ),
        Err(CommitRuntimeError::BranchGenerationMismatch {
            branch_id: branch,
            expected: 1,
            actual: 2,
        })
    );
    assert_unallocated_unattempted(&fixture);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_generation_mismatch_perf_trace_stops_before_source_capture_and_wal() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(77);
    let read_key = physical_key(branch, 0x20, b"durable-stale-generation-read-fact".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-stale-generation-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                read_key,
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
    );

    assert_eq!(
        fixture.execute_with_generation(
            batch,
            CommitBranchGeneration::new(2).expect("stale generation"),
        ),
        Err(CommitRuntimeError::BranchGenerationMismatch {
            branch_id: branch,
            expected: 1,
            actual: 2,
        })
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
    assert_eq!(perf.commit_branch_registry_lookups(), 1);
    assert_eq!(perf.commit_branch_registry_descriptors_scanned(), 1);
    assert_eq!(perf.commit_branch_guard_attempts(), 0);
    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 0);
    assert_eq!(perf.commit_batches_prepared(), 0);
    assert_eq!(perf.commit_wal_records_built(), 0);
    assert_eq!(perf.commit_wal_appends(), 0);
    assert_eq!(perf.commit_visible_publish_attempts(), 0);
    assert_unallocated_unattempted(&fixture);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_deleting_branch_perf_trace_stops_before_source_capture_and_wal() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(79);
    let read_key = physical_key(branch, 0x20, b"durable-deleting-branch-read-fact".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture
        .registry
        .mark_deleting(branch)
        .expect("mark branch deleting");
    crate::observability::perf_trace::reset();
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-deleting-branch-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                read_key,
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::BranchNotWritable {
            branch_id: branch,
            reason: "branch is deleting",
        })
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
    assert_eq!(perf.commit_branch_registry_lookups(), 1);
    assert_eq!(perf.commit_branch_registry_descriptors_scanned(), 1);
    assert_eq!(perf.commit_branch_guard_attempts(), 0);
    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 0);
    assert_eq!(perf.commit_batches_prepared(), 0);
    assert_eq!(perf.commit_wal_records_built(), 0);
    assert_eq!(perf.commit_wal_appends(), 0);
    assert_eq!(perf.commit_visible_publish_attempts(), 0);
    assert_unallocated_unattempted(&fixture);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_quiesce_perf_trace_stops_before_source_capture_and_wal() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(80);
    let read_key = physical_key(branch, 0x20, b"durable-quiesce-read-fact".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let _quiesce = fixture
        .guard_set
        .try_begin_quiesce()
        .expect("begin quiesce");
    crate::observability::perf_trace::reset();
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-quiesce-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                read_key,
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::CommitQuiesceUnavailable {
            reason: "commit quiesce is active",
        })
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
    assert_eq!(perf.commit_branch_registry_lookups(), 1);
    assert_eq!(perf.commit_branch_registry_descriptors_scanned(), 1);
    assert_eq!(perf.commit_branch_guard_attempts(), 1);
    assert_eq!(perf.commit_branch_guard_acquired(), 0);
    assert_eq!(perf.commit_branch_guard_rejected(), 1);
    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 0);
    assert_eq!(perf.commit_batches_prepared(), 0);
    assert_eq!(perf.commit_wal_records_built(), 0);
    assert_eq!(perf.commit_wal_appends(), 0);
    assert_eq!(perf.commit_visible_publish_attempts(), 0);
    assert_unallocated_unattempted(&fixture);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_missing_branch_perf_trace_stops_before_source_capture_and_wal() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(75);
    let read_key = physical_key(branch, 0x20, b"durable-missing-branch-read-fact".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture.registry = CommitBranchRegistry::new();
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-missing-branch-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                read_key,
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::BranchNotFound { branch_id: branch })
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
    assert_eq!(perf.commit_branch_registry_lookups(), 1);
    assert_eq!(perf.commit_branch_registry_descriptors_scanned(), 0);
    assert_eq!(perf.commit_branch_guard_attempts(), 0);
    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 0);
    assert_eq!(perf.commit_batches_prepared(), 0);
    assert_eq!(perf.commit_wal_records_built(), 0);
    assert_eq!(perf.commit_wal_appends(), 0);
    assert_eq!(perf.commit_visible_publish_attempts(), 0);
    assert_unallocated_unattempted(&fixture);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_malformed_validation_fact_rejects_before_admission_source_or_wal() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(76);
    let key = physical_key(branch, 0x20, b"durable-malformed-validation-fact".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-malformed-validation-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            Vec::new(),
            vec![CommitCasFact::new(
                key,
                CommitObservedVersion::Present(CommitVersion::ZERO),
            )],
        ),
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::InvalidValidationFacts {
            reason: "missing observed version must use Missing",
        })
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 0);
    assert_eq!(perf.commit_branch_registry_lookups(), 0);
    assert_eq!(perf.commit_branch_guard_attempts(), 0);
    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 0);
    assert_eq!(perf.commit_batches_prepared(), 0);
    assert_eq!(perf.commit_wal_records_built(), 0);
    assert_eq!(perf.commit_wal_appends(), 0);
    assert_eq!(perf.commit_visible_publish_attempts(), 0);
    assert_unallocated_unattempted(&fixture);
}

#[test]
fn durable_guard_contention_rejects_before_allocation_or_wal_append() {
    let branch = branch_id(64);
    let read_key = physical_key(branch, 0x20, b"durable-guard-read-fact".to_vec());
    let config = CommitRuntimeConfig::default()
        .with_admission_pressure_thresholds(
            CommitAdmissionPressureThresholds::new(Some(1), None, Some(1), None)
                .expect("thresholds"),
        )
        .expect("config");
    let mut fixture = DurableFixture::new(
        branch,
        config,
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let held_guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("held guard");
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"guard-contention".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                read_key,
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
    );
    #[cfg(feature = "perf-trace")]
    let _capture = crate::observability::perf_trace::begin_test_capture();

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::BranchGuardUnavailable {
            branch_id: branch,
            reason: "branch commit guard is already active",
        })
    );
    assert_unallocated_unattempted(&fixture);
    #[cfg(feature = "perf-trace")]
    {
        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
        assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
        assert_eq!(perf.commit_branch_registry_lookups(), 1);
        assert_eq!(perf.commit_branch_registry_descriptors_scanned(), 1);
        assert_eq!(perf.commit_branch_guard_attempts(), 1);
        assert_eq!(perf.commit_branch_guard_acquired(), 0);
        assert_eq!(perf.commit_branch_guard_rejected(), 1);
        assert_eq!(perf.commit_admission_pressure_facts(), 1);
        assert_eq!(perf.commit_admission_under_pressure(), 1);
        assert_eq!(perf.commit_admission_accepted_under_pressure(), 0);
        assert_eq!(perf.commit_admission_requires_maintenance(), 1);
        assert_eq!(perf.commit_admission_mutations(), 1);
        assert!(perf.commit_admission_approx_bytes() > 0);
        assert_eq!(perf.conflict_sources_built(), 0);
        assert_eq!(perf.read_view_captures(), 0);
        assert_eq!(perf.commit_conflict_validation_calls(), 0);
        assert_eq!(perf.commit_batches_prepared(), 0);
        assert_eq!(perf.commit_wal_records_built(), 0);
        assert_eq!(perf.commit_wal_appends(), 0);
        assert_eq!(perf.commit_visible_publish_attempts(), 0);
    }
    drop(held_guard);
}

#[test]
fn durable_rejects_unpublished_branch_rows_before_allocation_or_wal_append() {
    #[cfg(feature = "perf-trace")]
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(65);
    let hidden_key = physical_key(branch, 0x20, b"durable-hidden".to_vec());
    let config = CommitRuntimeConfig::default()
        .with_admission_pressure_thresholds(
            CommitAdmissionPressureThresholds::new(Some(1), None, Some(1), None)
                .expect("thresholds"),
        )
        .expect("config");
    let mut fixture = DurableFixture::new(
        branch,
        config,
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture
        .state
        .append_committed_row(StorageRow::put(
            hidden_key,
            CommitVersion::new(2),
            Timestamp::from_micros(2_000),
            Timestamp::EPOCH,
            b"hidden".to_vec(),
        ))
        .expect("seed unpublished row");
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-after-hidden".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::InvalidCommitState {
            reason: "branch has applied rows above current visible version",
        })
    );
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert_eq!(fixture.wal.append_attempts, 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
    #[cfg(feature = "perf-trace")]
    {
        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.commit_admission_pressure_facts(), 1);
        assert_eq!(perf.commit_admission_under_pressure(), 1);
        assert_eq!(perf.commit_admission_accepted_under_pressure(), 0);
        assert_eq!(perf.commit_admission_requires_maintenance(), 1);
        assert_eq!(perf.commit_admission_mutations(), 1);
        assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
        assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
        assert_eq!(perf.commit_branch_guard_attempts(), 1);
        assert_eq!(perf.commit_branch_guard_acquired(), 1);
        assert_eq!(perf.commit_branch_guard_rejected(), 0);
        assert_eq!(perf.commit_conflict_validation_calls(), 0);
        assert_eq!(perf.commit_batches_prepared(), 0);
        assert_eq!(perf.commit_wal_records_built(), 0);
        assert_eq!(perf.commit_wal_appends(), 0);
        assert_eq!(perf.commit_visible_publish_attempts(), 0);
    }
    let guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("guard released after unpublished-row rejection");
    drop(guard);
}

#[test]
fn durable_timestamp_source_failure_rejects_before_version_allocation_or_wal_append() {
    let branch = branch_id(71);
    let mut registry = CommitBranchRegistry::new();
    registry
        .register_active(
            branch,
            CommitBranchGeneration::new(1).expect("branch generation"),
        )
        .expect("register branch");
    let guard_set = CommitBranchGuardSet::new();
    let mut allocator = CommitFactAllocator::new(
        CommitVersionAllocator::default(),
        CommitTimestampGuard::default(),
        FailingTimestampSource,
    );
    let mut state =
        BranchLocalState::new(branch, BranchRuntimeConfig::default()).expect("branch state");
    let mut visible = VisibleVersionTracker::default();
    let durable_gate = CommitUnresolvedDurableGate::new();
    let mut wal = RecordingWalAppender::new(
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );

    assert_eq!(
        CommitDurableRuntime::new(
            &CommitRuntimeConfig::default(),
            &registry,
            &guard_set,
            &mut allocator,
            &mut state,
            &mut visible,
            &mut wal,
            &durable_gate,
        )
        .execute(
            durable_batch(
                branch,
                CommitDurabilityMode::Standard,
                vec![CommitMutation::put(
                    physical_key(branch, 0x20, b"timestamp-failure".to_vec()),
                    b"value".to_vec(),
                    CommitExpiry::None,
                    CommitRetentionHint::Append,
                )],
            ),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        ),
        Err(CommitRuntimeError::TimestampUnavailable {
            reason: "injected timestamp failure",
            source: None,
        })
    );
    assert_eq!(
        allocator.version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert_eq!(wal.append_attempts, 0);
    assert_eq!(state.active_row_count(), 0);
    assert_eq!(visible.visible_version(), CommitVersion::ZERO);
}

#[test]
fn durable_conflict_rejects_before_allocation_or_wal_append() {
    let branch = branch_id(65);
    let key = physical_key(branch, 0x20, b"durable-conflict".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture.seed_visible_row(StorageRow::put(
        key.clone(),
        CommitVersion::new(1),
        Timestamp::from_micros(900),
        Timestamp::EPOCH,
        b"existing".to_vec(),
    ));
    fixture.catch_up_to(CommitVersion::new(1), Timestamp::from_micros(900));
    fixture.visible = VisibleVersionTracker::new(CommitVersion::new(1));
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            key.clone(),
            b"new".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(key, CommitObservedVersion::Missing)],
            Vec::new(),
        ),
    );

    assert!(matches!(
        fixture.execute(batch),
        Err(CommitRuntimeError::CommitConflict { conflict })
            if conflict.kind() == CommitConflictKind::ReadSet
    ));
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::new(1)
    );
    assert_eq!(fixture.wal.append_attempts, 0);
    assert_eq!(fixture.state.active_row_count(), 1);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::new(1));
    let branch_guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("branch guard released after durable conflict");
    drop(branch_guard);
}

#[test]
fn durable_row_counts_match_user_mutations_exactly() {
    // W3.1c: commits stage user rows only, so the after-allocation row-limit
    // failure is unconstructible (config validation requires the row limit to
    // be at least the mutation limit, and rows == mutations). Pin the
    // invariant that retired it on the durable path.
    let branch = branch_id(66);
    let config =
        CommitRuntimeConfig::new(1, 1, 1, CommitReadOnlyDiagnostics::Enabled).expect("config");
    let mut fixture = DurableFixture::new(
        branch,
        config,
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let key = physical_key(branch, 0x20, b"row-limit".to_vec());
    let outcome = fixture
        .execute(durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                key,
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        ))
        .expect("single put fits a one-row limit without timeline padding");
    assert_eq!(outcome.mutation_counts().puts(), 1);
    assert_eq!(outcome.mutation_counts().timeline_rows(), 0);
    assert_eq!(fixture.wal.records[0].commit_payload().rows().len(), 1);
    assert_eq!(fixture.state.active_row_count(), 1);
}

#[test]
fn durable_version_overflow_rejects_before_wal_append() {
    let branch = branch_id(67);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture.catch_up_to(CommitVersion::MAX, Timestamp::from_micros(1_000));

    assert_eq!(
        fixture.execute(durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                physical_key(branch, 0x20, b"overflow".to_vec()),
                b"super-secret-visible-value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        )),
        Err(CommitRuntimeError::VersionAllocatorOverflow {
            last_allocated: CommitVersion::MAX,
        })
    );
    assert_eq!(fixture.wal.append_attempts, 0);
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
}

#[test]
fn durable_commit_rejects_policy_mismatch_before_allocation_or_wal_append() {
    let branch = branch_id(55);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Always,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"mismatch".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert_eq!(
        fixture.execute(batch),
        Err(CommitRuntimeError::DurabilityUnavailable {
            reason: "durable commit executor requires matching WAL durability policy",
        })
    );
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert_eq!(fixture.wal.records.len(), 0);
    assert_eq!(fixture.state.active_row_count(), 0);
}

#[test]
fn durable_clean_wal_failure_leaves_no_visible_rows_but_allocation_may_gap() {
    #[cfg(feature = "perf-trace")]
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(56);
    let key = physical_key(branch, 0x20, b"clean-durable-failure".to_vec());
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::CleanFailure,
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            key.clone(),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert!(matches!(
        fixture.execute(batch),
        Err(CommitRuntimeError::LowerLayer {
            layer: CommitLowerLayer::WalService,
            reason: "injected clean WAL failure",
            ..
        })
    ));
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::new(1)
    );
    assert_eq!(fixture.wal.records.len(), 0);
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
    assert_eq!(fixture.durable_gate.unresolved().expect("gate read"), None);
    assert!(fixture
        .state
        .capture_read_view()
        .expect("read view")
        .latest(&key)
        .expect("latest read")
        .is_none());
    #[cfg(feature = "perf-trace")]
    {
        let perf = crate::observability::perf_trace::snapshot();

        assert_eq!(perf.commit_batches_prepared(), 1);
        assert_eq!(perf.commit_user_mutation_rows(), 1);
        // W3.1c: timeline rows are elided — the retained index derives them.
        assert_eq!(perf.commit_timeline_rows_prepared(), 0);
        assert_eq!(perf.commit_rows_prepared(), 1);
        assert_eq!(perf.commit_wal_records_built(), 1);
        assert_eq!(perf.commit_wal_record_rows(), 1);
        assert_eq!(perf.commit_wal_appends(), 1);
        assert_eq!(perf.commit_wal_append_bytes(), 0);
        assert_eq!(perf.commit_visible_publish_attempts(), 0);
        assert_eq!(perf.commit_unresolved_records(), 0);
        assert_eq!(perf.commit_unresolved_durable_not_applied_records(), 0);
        assert_eq!(perf.commit_unresolved_applied_not_visible_records(), 0);
    }
    let branch_guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("branch guard released after clean WAL failure");
    drop(branch_guard);

    fixture.wal.mode = FakeWalMode::Succeed {
        forced_durable: false,
    };
    let retry = durable_batch(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            key,
            b"retry".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );
    let outcome = fixture.execute(retry).expect("retry succeeds");
    assert_eq!(outcome.commit_version(), Some(CommitVersion::new(2)));
    assert_eq!(fixture.wal.records.len(), 1);
    assert_eq!(fixture.durable_gate.unresolved().expect("gate read"), None);
}

#[test]
fn durable_clean_wal_failure_preserves_source_chain() {
    let branch = branch_id(68);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::CleanFailureWithSource,
    );

    let error = fixture
        .execute(durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                physical_key(branch, 0x20, b"sourced-failure".to_vec()),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        ))
        .expect_err("sourced WAL failure");

    assert!(matches!(
        error,
        CommitRuntimeError::LowerLayer {
            layer: CommitLowerLayer::WalService,
            reason: "injected sourced WAL failure",
            ..
        }
    ));
    assert_eq!(
        error.source().map(ToString::to_string),
        Some("injected WAL source".to_string())
    );
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
    assert_eq!(fixture.durable_gate.unresolved().expect("gate read"), None);
}

#[test]
fn durable_writer_halted_failure_is_clean_durability_unavailable() {
    let branch = branch_id(72);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::WriterHalted,
    );

    assert_eq!(
        fixture.execute(durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                physical_key(branch, 0x20, b"writer-halted".to_vec()),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        )),
        Err(CommitRuntimeError::DurabilityUnavailable {
            reason: "WAL writer is halted; reopen before appending",
        })
    );
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::new(1)
    );
    assert_eq!(fixture.wal.append_attempts, 1);
    assert_eq!(fixture.wal.records.len(), 0);
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
    assert_eq!(fixture.durable_gate.unresolved().expect("gate read"), None);
    let branch_guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("branch guard released after writer halt");
    drop(branch_guard);
}

#[test]
fn durable_uncertain_wal_failure_is_distinct_and_leaves_no_visible_rows() {
    let branch = branch_id(57);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Always,
        FakeWalMode::UncertainFailure,
    );
    let batch = durable_batch(
        branch,
        CommitDurabilityMode::Always,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"uncertain".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
    );

    assert!(matches!(
        fixture.execute(batch),
        Err(CommitRuntimeError::DurabilityUncertain {
            branch_id: failed_branch,
            commit_version,
            reason: "WAL append durability is uncertain",
            ..
        }) if failed_branch == branch && commit_version == CommitVersion::new(1)
    ));
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::new(1)
    );
    assert_eq!(fixture.wal.records.len(), 0);
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
    assert_eq!(fixture.durable_gate.unresolved().expect("gate read"), None);
    let _guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("guard can be reacquired after uncertain WAL failure");
}

#[test]
fn durable_apply_failure_after_wal_success_records_durable_not_applied_gate() {
    #[cfg(feature = "perf-trace")]
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(73);
    let key = physical_key(branch, 0x20, b"apply-failure".to_vec());
    let mut registry = CommitBranchRegistry::new();
    register_active_branch(&mut registry, branch);
    let guard_set = CommitBranchGuardSet::new();
    let mut allocator = CommitFactAllocator::new(
        CommitVersionAllocator::default(),
        CommitTimestampGuard::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
    );
    let mut state = FailingApplyTarget::new(branch, true);
    let mut visible = FailingVisiblePublisher::new(false);
    let mut wal = RecordingWalAppender::new(
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let durable_gate = CommitUnresolvedDurableGate::new();

    let error = CommitDurableRuntime::new(
        &CommitRuntimeConfig::default(),
        &registry,
        &guard_set,
        &mut allocator,
        &mut state,
        &mut visible,
        &mut wal,
        &durable_gate,
    )
    .execute(
        durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                key,
                b"super-secret-apply-value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        ),
        CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
    )
    .expect_err("apply failure is classified after WAL success");

    assert_apply_failure_error_and_wal(&error, &wal, branch);
    assert_apply_failure_state_and_source(&error, &state, &visible);
    let unresolved = durable_gate
        .unresolved()
        .expect("gate read")
        .expect("unresolved durable fact");
    assert_durable_not_applied_unresolved(unresolved, branch);
    let branch_guard = guard_set
        .try_acquire_branch_guard(branch)
        .expect("branch guard released after gate record");
    drop(branch_guard);
    #[cfg(feature = "perf-trace")]
    {
        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
        assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
        assert_eq!(perf.commit_unresolved_records(), 1);
        assert_eq!(perf.commit_unresolved_durable_not_applied_records(), 1);
        assert_eq!(perf.commit_unresolved_applied_not_visible_records(), 0);
        assert_eq!(perf.commit_wal_records_built(), 1);
        assert_eq!(perf.commit_wal_record_rows(), 1);
        assert_eq!(perf.commit_wal_appends(), 1);
        assert_eq!(perf.commit_wal_append_bytes(), 128);
        assert_eq!(perf.commit_visible_publish_attempts(), 0);
    }
    assert_unresolved_gate_blocks_other_durable_branch(
        &mut registry,
        &guard_set,
        &mut allocator,
        &mut wal,
        &durable_gate,
        branch,
    );
}

#[test]
fn durable_visibility_failure_after_apply_records_applied_not_visible_gate() {
    #[cfg(feature = "perf-trace")]
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(74);
    let key = physical_key(branch, 0x20, b"visible-failure".to_vec());
    let mut registry = CommitBranchRegistry::new();
    register_active_branch(&mut registry, branch);
    let guard_set = CommitBranchGuardSet::new();
    let mut allocator = CommitFactAllocator::new(
        CommitVersionAllocator::default(),
        CommitTimestampGuard::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
    );
    let mut state = FailingApplyTarget::new(branch, false);
    let mut visible = FailingVisiblePublisher::new(true);
    let mut wal = RecordingWalAppender::new(
        DurabilityPolicy::Always,
        FakeWalMode::Succeed {
            forced_durable: true,
        },
    );
    let durable_gate = CommitUnresolvedDurableGate::new();

    let error = CommitDurableRuntime::new(
        &CommitRuntimeConfig::default(),
        &registry,
        &guard_set,
        &mut allocator,
        &mut state,
        &mut visible,
        &mut wal,
        &durable_gate,
    )
    .execute(
        durable_batch(
            branch,
            CommitDurabilityMode::Always,
            vec![CommitMutation::put(
                key.clone(),
                b"super-secret-visible-value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        ),
        CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
    )
    .expect_err("visibility failure is classified after apply");

    assert_visibility_failure_error_and_state(&error, &state, &visible, &wal, branch, &key);
    let unresolved = durable_gate
        .unresolved()
        .expect("gate read")
        .expect("unresolved durable fact");
    assert_applied_not_visible_unresolved(unresolved, branch);
    #[cfg(feature = "perf-trace")]
    {
        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
        assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 1);
        assert_eq!(perf.commit_unresolved_records(), 1);
        assert_eq!(perf.commit_unresolved_durable_not_applied_records(), 0);
        assert_eq!(perf.commit_unresolved_applied_not_visible_records(), 1);
        assert_eq!(perf.commit_wal_records_built(), 1);
        assert_eq!(perf.commit_wal_record_rows(), 1);
        assert_eq!(perf.commit_wal_appends(), 1);
        assert_eq!(perf.commit_wal_append_bytes(), 128);
        assert_eq!(perf.commit_visible_publish_attempts(), 1);
        assert_eq!(perf.commit_visible_publish_successes(), 0);
        assert_eq!(perf.commit_visible_publish_failures(), 1);
    }
    assert_unresolved_gate_blocks_durable_retry(
        &registry,
        &guard_set,
        &mut allocator,
        &mut state,
        &mut visible,
        &mut wal,
        &durable_gate,
    );
}

#[test]
fn unresolved_durable_gate_blocks_durable_commit_before_allocation_and_wal_append() {
    let branch = branch_id(75);
    let mut fixture = DurableFixture::new(
        branch,
        CommitRuntimeConfig::default(),
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture
        .durable_gate
        .record_unresolved(unresolved_durable_fact(branch, CommitVersion::new(9)))
        .expect("record unresolved durable fact");

    let error = fixture
        .execute(durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                physical_key(branch, 0x20, b"blocked".to_vec()),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        ))
        .expect_err("unresolved durable gate blocks commit");

    assert!(matches!(
        error,
        CommitRuntimeError::UnresolvedDurableCommit {
            branch_id: blocked_branch,
            commit_version,
            ..
        } if blocked_branch == branch && commit_version == CommitVersion::new(9)
    ));
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert_eq!(fixture.wal.append_attempts, 0);
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
    let _guard = fixture
        .guard_set
        .try_acquire_branch_guard(branch)
        .expect("branch guard released after blocked durable commit");
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_unresolved_gate_perf_trace_stops_before_source_capture_and_wal() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(78);
    let read_key = physical_key(branch, 0x20, b"durable-unresolved-gate-read-fact".to_vec());
    let config = CommitRuntimeConfig::default()
        .with_admission_pressure_thresholds(
            CommitAdmissionPressureThresholds::new(Some(1), None, Some(1), None)
                .expect("thresholds"),
        )
        .expect("config");
    let mut fixture = DurableFixture::new(
        branch,
        config,
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    fixture
        .durable_gate
        .record_unresolved(unresolved_durable_fact(branch, CommitVersion::new(9)))
        .expect("record unresolved durable fact");
    crate::observability::perf_trace::reset();
    let batch = durable_batch_with_validation(
        branch,
        CommitDurabilityMode::Standard,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"durable-unresolved-gate-write".to_vec()),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                read_key,
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
    );

    assert!(matches!(
        fixture.execute(batch),
        Err(CommitRuntimeError::UnresolvedDurableCommit {
            branch_id: blocked_branch,
            commit_version,
            ..
        }) if blocked_branch == branch && commit_version == CommitVersion::new(9)
    ));

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_admission_pressure_facts(), 1);
    assert_eq!(perf.commit_admission_under_pressure(), 1);
    assert_eq!(perf.commit_admission_accepted_under_pressure(), 0);
    assert_eq!(perf.commit_admission_requires_maintenance(), 1);
    assert_eq!(perf.commit_admission_mutations(), 1);
    assert!(perf.commit_admission_approx_bytes() > 0);
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 0);
    assert_eq!(perf.commit_unresolved_gate_rejected_unresolved(), 1);
    assert_eq!(perf.commit_branch_registry_lookups(), 0);
    assert_eq!(perf.commit_branch_guard_attempts(), 0);
    assert_eq!(perf.conflict_sources_built(), 0);
    assert_eq!(perf.read_view_captures(), 0);
    assert_eq!(perf.commit_conflict_validation_calls(), 0);
    assert_eq!(perf.commit_batches_prepared(), 0);
    assert_eq!(perf.commit_wal_records_built(), 0);
    assert_eq!(perf.commit_wal_appends(), 0);
    assert_eq!(perf.commit_visible_publish_attempts(), 0);
    assert_unallocated_unattempted(&fixture);
}

#[test]
fn durable_commit_proceeds_alongside_open_admission_span_same_branch() {
    // BS5.2: the pipelined commit path keeps an admission span open across its
    // off-lock covering fsync, so a same-branch commit admitted while that
    // span is in flight must execute to completion — the span no longer
    // serializes admission; only an unresolved fact does.
    let branch = branch_id(76);
    let config = CommitRuntimeConfig::default();
    let mut registry = CommitBranchRegistry::new();
    register_active_branch(&mut registry, branch);
    let guard_set = CommitBranchGuardSet::new();
    let mut allocator = CommitFactAllocator::new(
        CommitVersionAllocator::default(),
        CommitTimestampGuard::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
    );
    let mut state = BranchLocalState::new(branch, BranchRuntimeConfig::default()).expect("state");
    let mut visible = VisibleVersionTracker::default();
    let mut wal = RecordingWalAppender::new(
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let durable_gate = CommitUnresolvedDurableGate::new();
    let active_admission = durable_gate
        .admit_mutating_commit()
        .expect("in-flight pipelined span");

    let outcome = CommitDurableRuntime::new(
        &config,
        &registry,
        &guard_set,
        &mut allocator,
        &mut state,
        &mut visible,
        &mut wal,
        &durable_gate,
    )
    .execute(
        durable_batch(
            branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                physical_key(branch, 0x20, b"same-branch-active-durable".to_vec()),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        ),
        CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
    )
    .expect("commit proceeds alongside the open span");

    assert_eq!(outcome.commit_version(), Some(CommitVersion::new(1)));
    assert_eq!(wal.append_attempts, 1);
    assert!(state.active_row_count() > 0);
    assert_eq!(visible.visible_version(), CommitVersion::new(1));
    assert_eq!(durable_gate.unresolved().expect("gate read"), None);
    drop(active_admission);
    assert!(durable_gate.require_open_for_mutation().is_ok());
}

#[test]
fn durable_commit_proceeds_alongside_open_admission_span_other_branch() {
    // BS5.2: cross-branch variant of the open-span test above — a commit to a
    // DIFFERENT branch admitted while a pipelined span is in flight executes
    // to completion.
    let active_branch = branch_id(76);
    let target_branch = branch_id(77);
    let config = CommitRuntimeConfig::default();
    let mut registry = CommitBranchRegistry::new();
    register_active_branch(&mut registry, active_branch);
    register_active_branch(&mut registry, target_branch);
    let guard_set = CommitBranchGuardSet::new();
    let mut allocator = CommitFactAllocator::new(
        CommitVersionAllocator::default(),
        CommitTimestampGuard::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
    );
    let mut state =
        BranchLocalState::new(target_branch, BranchRuntimeConfig::default()).expect("state");
    let mut visible = VisibleVersionTracker::default();
    let mut wal = RecordingWalAppender::new(
        DurabilityPolicy::Standard,
        FakeWalMode::Succeed {
            forced_durable: false,
        },
    );
    let durable_gate = CommitUnresolvedDurableGate::new();
    let active_admission = durable_gate
        .admit_mutating_commit()
        .expect("in-flight pipelined span");

    let outcome = CommitDurableRuntime::new(
        &config,
        &registry,
        &guard_set,
        &mut allocator,
        &mut state,
        &mut visible,
        &mut wal,
        &durable_gate,
    )
    .execute(
        durable_batch(
            target_branch,
            CommitDurabilityMode::Standard,
            vec![CommitMutation::put(
                physical_key(target_branch, 0x20, b"alongside-active-durable".to_vec()),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
        ),
        CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
    )
    .expect("cross-branch commit proceeds alongside the open span");

    assert_eq!(outcome.commit_version(), Some(CommitVersion::new(1)));
    assert_eq!(wal.append_attempts, 1);
    assert!(state.active_row_count() > 0);
    assert_eq!(visible.visible_version(), CommitVersion::new(1));
    assert_eq!(durable_gate.unresolved().expect("gate read"), None);
    drop(active_admission);
    assert!(durable_gate.require_open_for_mutation().is_ok());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FakeWalMode {
    Succeed { forced_durable: bool },
    CleanFailure,
    CleanFailureWithSource,
    WriterHalted,
    UncertainFailure,
}

#[derive(Debug)]
struct RecordingWalAppender {
    policy: DurabilityPolicy,
    mode: FakeWalMode,
    records: Vec<WalRecord>,
    append_attempts: usize,
}

impl RecordingWalAppender {
    fn new(policy: DurabilityPolicy, mode: FakeWalMode) -> Self {
        Self {
            policy,
            mode,
            records: Vec::new(),
            append_attempts: 0,
        }
    }
}

impl CommitWalAppender for RecordingWalAppender {
    fn durability_policy(&self) -> DurabilityPolicy {
        self.policy
    }

    fn append_commit_record(
        &mut self,
        record: &WalRecord,
    ) -> Result<CommitWalAppendFacts, CommitWalAppendError> {
        self.append_attempts = self.append_attempts.saturating_add(1);
        match self.mode {
            FakeWalMode::Succeed { forced_durable } => {
                self.records.push(record.clone());
                Ok(CommitWalAppendFacts::new(0, 36, 128, 128, forced_durable))
            }
            FakeWalMode::CleanFailure => Err(CommitWalAppendError::clean(
                CommitRuntimeError::lower_layer(
                    CommitLowerLayer::WalService,
                    "injected clean WAL failure",
                ),
            )),
            FakeWalMode::CleanFailureWithSource => Err(CommitWalAppendError::clean(
                CommitRuntimeError::lower_layer_with(
                    CommitLowerLayer::WalService,
                    "injected sourced WAL failure",
                    InjectedWalSource,
                ),
            )),
            FakeWalMode::WriterHalted => Err(CommitWalAppendError::clean(
                CommitRuntimeError::DurabilityUnavailable {
                    reason: "WAL writer is halted; reopen before appending",
                },
            )),
            FakeWalMode::UncertainFailure => Err(CommitWalAppendError::uncertain(
                CommitRuntimeError::lower_layer(
                    CommitLowerLayer::WalService,
                    "injected uncertain WAL failure",
                ),
            )),
        }
    }
}

#[derive(Debug)]
struct FailingApplyTarget {
    state: BranchLocalState,
    fail_append: bool,
}

impl FailingApplyTarget {
    fn new(branch: BranchId, fail_append: bool) -> Self {
        Self {
            state: {
                let state =
                    BranchLocalState::new(branch, BranchRuntimeConfig::default()).expect("state");
                // W3.1c: fixture branches are born empty in-process.
                state.retained_timeline().mark_complete_from_birth();
                state
            },
            fail_append,
        }
    }
}

impl CommitBranchApplyTarget for FailingApplyTarget {
    fn branch_id(&self) -> BranchId {
        self.state.branch_id()
    }

    fn max_commit_version(&self) -> Option<CommitVersion> {
        self.state.max_commit_version()
    }

    fn capture_read_view(&self) -> CommitRuntimeResult<BranchReadView> {
        self.state.capture_read_view().map_err(|source| {
            CommitRuntimeError::lower_layer_with(
                CommitLowerLayer::BranchRuntime,
                "branch read view capture failed",
                source,
            )
        })
    }

    fn append_committed_rows_atomically(
        &mut self,
        rows: Vec<StorageRow>,
    ) -> CommitRuntimeResult<()> {
        if self.fail_append {
            return Err(CommitRuntimeError::lower_layer_with(
                CommitLowerLayer::BranchRuntime,
                "injected branch apply failure",
                InjectedBranchApplySource,
            ));
        }
        self.state
            .append_committed_rows_atomically(rows)
            .map(|_| ())
            .map_err(|source| {
                CommitRuntimeError::lower_layer_with(
                    CommitLowerLayer::BranchRuntime,
                    "branch state rejected commit rows",
                    source,
                )
            })
    }
}

#[derive(Debug)]
struct FailingVisiblePublisher {
    tracker: VisibleVersionTracker,
    fail_publish: bool,
    publish_attempts: usize,
}

impl FailingVisiblePublisher {
    const fn new(fail_publish: bool) -> Self {
        Self {
            tracker: VisibleVersionTracker::new(CommitVersion::ZERO),
            fail_publish,
            publish_attempts: 0,
        }
    }
}

impl CommitVisiblePublisher for FailingVisiblePublisher {
    fn visible_version(&self) -> CommitVersion {
        self.tracker.visible_version()
    }

    fn publish_from_facts(
        &mut self,
        facts: CommitVisibilityFacts,
    ) -> CommitRuntimeResult<VisibleVersionPublish> {
        self.publish_attempts = self.publish_attempts.saturating_add(1);
        if self.fail_publish {
            return Err(CommitRuntimeError::InvalidCommitState {
                reason: "injected visible publication failure",
            });
        }
        self.tracker.publish_from_facts(facts)
    }
}

struct DurableFixture {
    config: CommitRuntimeConfig,
    registry: CommitBranchRegistry,
    guard_set: CommitBranchGuardSet,
    allocator: CommitFactAllocator<CommitManualTimestampSource>,
    state: BranchLocalState,
    visible: VisibleVersionTracker,
    wal: RecordingWalAppender,
    durable_gate: CommitUnresolvedDurableGate,
}

impl DurableFixture {
    fn new(
        branch: BranchId,
        config: CommitRuntimeConfig,
        policy: DurabilityPolicy,
        wal_mode: FakeWalMode,
    ) -> Self {
        let mut registry = CommitBranchRegistry::new();
        registry
            .register_active(
                branch,
                CommitBranchGeneration::new(1).expect("branch generation"),
            )
            .expect("register branch");
        Self {
            config,
            registry,
            guard_set: CommitBranchGuardSet::new(),
            allocator: CommitFactAllocator::new(
                CommitVersionAllocator::default(),
                CommitTimestampGuard::default(),
                CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
            ),
            state: {
                let state =
                    BranchLocalState::new(branch, BranchRuntimeConfig::default()).expect("state");
                // W3.1c: fixture branches are born empty in-process.
                state.retained_timeline().mark_complete_from_birth();
                state
            },
            visible: VisibleVersionTracker::default(),
            wal: RecordingWalAppender::new(policy, wal_mode),
            durable_gate: CommitUnresolvedDurableGate::new(),
        }
    }

    fn execute(&mut self, batch: CommitBatch) -> CommitRuntimeResult<CommitOutcome> {
        self.execute_with_generation(batch, CommitBranchGeneration::new(1).expect("generation"))
    }

    fn execute_with_generation(
        &mut self,
        batch: CommitBatch,
        generation: CommitBranchGeneration,
    ) -> CommitRuntimeResult<CommitOutcome> {
        CommitDurableRuntime::new(
            &self.config,
            &self.registry,
            &self.guard_set,
            &mut self.allocator,
            &mut self.state,
            &mut self.visible,
            &mut self.wal,
            &self.durable_gate,
        )
        .execute(batch, CommitBranchGenerationGuard::exact(generation))
    }

    fn seed_visible_row(&mut self, row: StorageRow) {
        self.state.append_committed_row(row).expect("seed row");
    }

    fn catch_up_to(&mut self, version: CommitVersion, timestamp: Timestamp) {
        self.allocator.catch_up_to_recovered_version(version);
        self.allocator.catch_up_to_recovered_timestamp(timestamp);
    }
}

fn durable_batch(
    branch: BranchId,
    durability: CommitDurabilityMode,
    mutations: Vec<CommitMutation>,
) -> CommitBatch {
    durable_batch_with_validation(
        branch,
        durability,
        mutations,
        CommitValidationFacts::empty(),
    )
}

fn durable_batch_with_validation(
    branch: BranchId,
    durability: CommitDurabilityMode,
    mutations: Vec<CommitMutation>,
    validation: CommitValidationFacts,
) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        mutations,
        validation,
        CommitBatchOptions::new(
            durability,
            CommitConflictValidationMode::Validate,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::RuntimeGenerated,
            CommitOrigin::StorageRuntime,
        ),
    )
}

fn register_active_branch(registry: &mut CommitBranchRegistry, branch: BranchId) {
    registry
        .register_active(
            branch,
            CommitBranchGeneration::new(1).expect("branch generation"),
        )
        .expect("register branch");
}

fn assert_unallocated_unattempted(fixture: &DurableFixture) {
    assert_eq!(
        fixture.allocator.version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert_eq!(fixture.wal.append_attempts, 0);
    assert_eq!(fixture.state.active_row_count(), 0);
    assert_eq!(fixture.visible.visible_version(), CommitVersion::ZERO);
}

fn timeline_view(state: &BranchLocalState, branch: BranchId) -> CommitTimelineView {
    // W3.1c: commits no longer materialize timeline rows — the retained
    // index observed at apply is the timeline's source.
    let entries = state
        .retained_timeline()
        .materialized_entries(None)
        .expect("fixture branches are complete from birth")
        .iter()
        .map(|entry| {
            CommitTimelineEntry::new(branch, entry.commit_version(), entry.commit_timestamp())
                .expect("retained entry")
        })
        .collect::<Vec<_>>();
    CommitTimelineView::from_entries(branch, entries)
}

fn assert_record_rows_share_commit_facts(
    record: &WalRecord,
    branch: BranchId,
    version: CommitVersion,
    timestamp: Timestamp,
) {
    assert_eq!(record.branch_id(), branch);
    assert_eq!(record.commit_version(), version);
    assert_eq!(record.commit_timestamp(), timestamp);
    for row in record.commit_payload().rows() {
        assert_eq!(row.physical_key().branch_id(), branch);
        assert_eq!(row.commit_version(), version);
        assert_eq!(row.commit_timestamp(), timestamp);
    }
}

/// W3.1c: commits no longer materialize timeline rows — WAL payloads carry
/// user rows only (the stamp on the record is the timeline fact).
fn assert_payload_contains_timeline_rows(record: &WalRecord) {
    let timeline_rows = record
        .commit_payload()
        .rows()
        .iter()
        .filter(|row| row.physical_key().storage_space_id() == StorageSpaceId::COMMIT_TIMELINE)
        .count();
    assert_eq!(timeline_rows, 0);
}

#[cfg(feature = "perf-trace")]
#[cfg(all(feature = "localfs", unix))]
fn assert_real_wal_commit_perf(
    perf: &crate::observability::perf_trace::StoragePerfSnapshot,
    user_rows: u64,
) {
    // W3.1c: timeline rows are elided — the WAL payload is the user rows.
    assert_eq!(perf.commit_batches_prepared(), 1);
    assert_eq!(perf.commit_user_mutation_rows(), user_rows);
    assert_eq!(perf.commit_timeline_rows_prepared(), 0);
    assert_eq!(perf.commit_rows_prepared(), user_rows);
    assert_eq!(perf.commit_wal_records_built(), 1);
    assert_eq!(perf.commit_wal_record_rows(), user_rows);
    assert_eq!(perf.commit_wal_appends(), 1);
    assert_eq!(perf.append_rows_applied(), user_rows);
    assert!(perf.commit_wal_append_bytes() > 0);
    assert!(perf.commit_wal_record_bytes() > 0);
    assert!(perf.commit_wal_payload_bytes() > 0);
    assert!(perf.commit_wal_row_encode_bytes() > 0);
    assert!(perf.commit_wal_append_bytes() >= perf.commit_wal_record_bytes());
    assert!(perf.commit_wal_record_bytes() > perf.commit_wal_payload_bytes());
    assert!(perf.commit_wal_payload_bytes() > perf.commit_wal_row_encode_bytes());
    assert!(
        perf.commit_wal_encode_buffer_allocations() + perf.commit_wal_encode_buffer_reuses() >= 4
    );
    assert!(perf.commit_wal_encode_buffer_reuses() > 0);
}

fn assert_apply_failure_error_and_wal(
    error: &CommitRuntimeError,
    wal: &RecordingWalAppender,
    branch: BranchId,
) {
    assert!(matches!(
        error,
        CommitRuntimeError::DurableButNotVisible {
            branch_id: failed_branch,
            commit_version,
            reason: "branch state rejected durable commit rows after WAL append",
            ..
        } if *failed_branch == branch && *commit_version == CommitVersion::new(1)
    ));
    assert_eq!(wal.records.len(), 1);
    assert_record_rows_share_commit_facts(
        &wal.records[0],
        branch,
        CommitVersion::new(1),
        Timestamp::from_micros(1_000),
    );
    assert_payload_contains_timeline_rows(&wal.records[0]);
}

fn assert_apply_failure_state_and_source(
    error: &CommitRuntimeError,
    state: &FailingApplyTarget,
    visible: &FailingVisiblePublisher,
) {
    assert_eq!(state.state.active_row_count(), 0);
    assert_eq!(visible.publish_attempts, 0);
    assert_eq!(visible.tracker.visible_version(), CommitVersion::ZERO);
    assert_eq!(
        error
            .source()
            .expect("durable error source")
            .source()
            .expect("nested apply source")
            .to_string(),
        "injected branch apply source"
    );
}

fn assert_durable_not_applied_unresolved(unresolved: CommitUnresolvedDurable, branch: BranchId) {
    assert!(!format!("{unresolved:?}").contains("super-secret-apply-value"));
    assert_eq!(
        unresolved.kind(),
        CommitUnresolvedDurableKind::DurableNotApplied
    );
    assert_eq!(unresolved.branch_id(), branch);
    assert_eq!(unresolved.commit_version(), CommitVersion::new(1));
    assert_eq!(unresolved.durability(), CommitDurabilityClass::Standard);
    assert_eq!(
        unresolved.visibility_facts(),
        CommitVisibilityFacts::new(
            Some(CommitVersion::new(1)),
            Some(CommitVersion::new(1)),
            None,
            None,
            None,
        )
        .expect("durable-not-applied facts")
    );
}

fn assert_unresolved_gate_blocks_other_durable_branch(
    registry: &mut CommitBranchRegistry,
    guard_set: &CommitBranchGuardSet,
    allocator: &mut CommitFactAllocator<CommitManualTimestampSource>,
    wal: &mut RecordingWalAppender,
    durable_gate: &CommitUnresolvedDurableGate,
    blocked_branch: BranchId,
) {
    let other_branch = branch_id(76);
    register_active_branch(registry, other_branch);
    let mut other_state = FailingApplyTarget::new(other_branch, false);
    let mut other_visible = FailingVisiblePublisher::new(false);

    assert!(matches!(
        CommitDurableRuntime::new(
            &CommitRuntimeConfig::default(),
            registry,
            guard_set,
            allocator,
            &mut other_state,
            &mut other_visible,
            wal,
            durable_gate,
        )
        .execute(
            durable_batch(
                other_branch,
                CommitDurabilityMode::Standard,
                vec![CommitMutation::put(
                    physical_key(other_branch, 0x20, b"other-blocked".to_vec()),
                    b"value".to_vec(),
                    CommitExpiry::None,
                    CommitRetentionHint::Append,
                )],
            ),
            CommitBranchGenerationGuard::exact(
                CommitBranchGeneration::new(1).expect("generation"),
            ),
        ),
        Err(CommitRuntimeError::UnresolvedDurableCommit {
            branch_id,
            commit_version,
            ..
        }) if branch_id == blocked_branch && commit_version == CommitVersion::new(1)
    ));
    assert_eq!(wal.append_attempts, 1);
    assert_eq!(
        allocator.version_allocator().last_allocated(),
        CommitVersion::new(1)
    );
    assert_eq!(other_state.state.active_row_count(), 0);
    assert_eq!(other_visible.publish_attempts, 0);
}

fn assert_visibility_failure_error_and_state(
    error: &CommitRuntimeError,
    state: &FailingApplyTarget,
    visible: &FailingVisiblePublisher,
    wal: &RecordingWalAppender,
    branch: BranchId,
    key: &PhysicalKey,
) {
    assert!(matches!(
        error,
        CommitRuntimeError::DurableButNotVisible {
            branch_id: failed_branch,
            commit_version,
            reason: "injected visible publication failure",
            ..
        } if *failed_branch == branch && *commit_version == CommitVersion::new(1)
    ));
    assert_eq!(state.state.active_row_count(), 1);
    assert_eq!(visible.tracker.visible_version(), CommitVersion::ZERO);
    assert_eq!(visible.publish_attempts, 1);
    assert_eq!(
        error.source().expect("durable error source").to_string(),
        "commit state is invalid: injected visible publication failure"
    );
    assert_eq!(wal.records.len(), 1);
    assert_record_rows_share_commit_facts(
        &wal.records[0],
        branch,
        CommitVersion::new(1),
        Timestamp::from_micros(1_000),
    );
    assert_payload_contains_timeline_rows(&wal.records[0]);
    let read_view = state.state.capture_read_view().expect("read view");
    assert_eq!(
        read_view
            .latest(key)
            .expect("latest read")
            .expect("visible row")
            .row()
            .value(),
        b"super-secret-visible-value"
    );
}

fn assert_applied_not_visible_unresolved(unresolved: CommitUnresolvedDurable, branch: BranchId) {
    assert!(!format!("{unresolved:?}").contains("super-secret-visible-value"));
    assert_eq!(
        unresolved.kind(),
        CommitUnresolvedDurableKind::AppliedNotVisible
    );
    assert_eq!(unresolved.branch_id(), branch);
    assert_eq!(unresolved.commit_version(), CommitVersion::new(1));
    assert_eq!(unresolved.durability(), CommitDurabilityClass::Always);
    assert_eq!(
        unresolved.visibility_facts(),
        CommitVisibilityFacts::new(
            Some(CommitVersion::new(1)),
            Some(CommitVersion::new(1)),
            Some(CommitVersion::new(1)),
            None,
            Some(CommitVersion::new(1)),
        )
        .expect("applied-not-visible facts")
    );
    assert_eq!(unresolved.reason(), "injected visible publication failure");
}

fn assert_unresolved_gate_blocks_durable_retry(
    registry: &CommitBranchRegistry,
    guard_set: &CommitBranchGuardSet,
    allocator: &mut CommitFactAllocator<CommitManualTimestampSource>,
    state: &mut FailingApplyTarget,
    visible: &mut FailingVisiblePublisher,
    wal: &mut RecordingWalAppender,
    durable_gate: &CommitUnresolvedDurableGate,
) {
    let branch = state.branch_id();
    assert!(matches!(
        CommitDurableRuntime::new(
            &CommitRuntimeConfig::default(),
            registry,
            guard_set,
            allocator,
            state,
            visible,
            wal,
            durable_gate,
        )
        .execute(
            durable_batch(
                branch,
                CommitDurabilityMode::Always,
                vec![CommitMutation::put(
                    physical_key(branch, 0x20, b"retry-blocked".to_vec()),
                    b"value".to_vec(),
                    CommitExpiry::None,
                    CommitRetentionHint::Append,
                )],
            ),
            CommitBranchGenerationGuard::exact(
                CommitBranchGeneration::new(1).expect("generation"),
            ),
        ),
        Err(CommitRuntimeError::UnresolvedDurableCommit {
            branch_id,
            commit_version,
            ..
        }) if branch_id == branch && commit_version == CommitVersion::new(1)
    ));
    assert_eq!(wal.append_attempts, 1);
    assert_eq!(
        allocator.version_allocator().last_allocated(),
        CommitVersion::new(1)
    );
}

fn unresolved_durable_fact(branch: BranchId, version: CommitVersion) -> CommitUnresolvedDurable {
    CommitUnresolvedDurable::durable_not_applied_with_facts(
        CommitStamp::new(branch, version, Timestamp::from_micros(9_000)).expect("stamp"),
        CommitDurabilityClass::Standard,
        "seed unresolved durable fact",
    )
    .expect("unresolved durable fact")
}

#[derive(Debug)]
struct InjectedWalSource;

impl fmt::Display for InjectedWalSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("injected WAL source")
    }
}

impl std::error::Error for InjectedWalSource {}

#[derive(Debug)]
struct InjectedBranchApplySource;

impl fmt::Display for InjectedBranchApplySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("injected branch apply source")
    }
}

impl std::error::Error for InjectedBranchApplySource {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FailingTimestampSource;

impl CommitTimestampSource for FailingTimestampSource {
    fn next_timestamp(&mut self) -> CommitRuntimeResult<Timestamp> {
        Err(CommitRuntimeError::timestamp_unavailable(
            "injected timestamp failure",
        ))
    }
}
