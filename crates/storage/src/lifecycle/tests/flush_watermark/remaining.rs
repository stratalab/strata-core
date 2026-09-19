use super::*;
use crate::backend::memory::MemoryBackend;
use crate::format::encode_table_manifest;
use crate::service::{WalRetentionProof, WalRetentionProofSource};

#[test]
fn table_manifest_flush_proof_accepts_coverage_above_checkpoint() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb0);
    let rows = rows_for_versions(branch, 4..=6, b"above-checkpoint");
    let manifest = durable_manifest(backend, branch, "above-checkpoint", &rows);
    let state = state_with_installed_table(branch, "above-checkpoint", &rows);

    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(6),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    proof
        .validate_extends_checkpoint(CommitVersion::new(3))
        .expect("table manifest extends checkpoint");
}

#[test]
fn table_manifest_flush_proof_rejects_table_object_without_manifest() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb1);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");
    let _manifest = unpublished_manifest(
        backend,
        branch,
        "object-without-manifest",
        &[put_row(branch, 1, b"object-only", b"value")],
        TableManifestTableProvenance::Flush,
    );

    let error = runtime
        .persist_table_manifest_flush_watermark(CommitVersion::new(1))
        .expect_err("table object alone is not proof");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_rejects_manifest_publish_uncertain() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb2);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");
    let manifest = durable_manifest(
        backend,
        branch,
        "uncertain-manifest",
        &[put_row(branch, 1, b"uncertain", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");
    backend.uncertain_manifest_replacement_on_call(2);

    let error = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(1),
        CommitVersion::new(1),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect_err("uncertain publish rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
}

#[test]
fn table_manifest_flush_proof_rejects_candidate_above_visible_version() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb3);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(4))
        .expect("snapshot facts");
    let rows = rows_for_versions(branch, 5..=6, b"above-visible");
    let manifest = durable_manifest(backend, branch, "above-visible", &rows);
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(6),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let error = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(6),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect_err("above visible rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_rejects_zero_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb4);
    let manifest = durable_manifest(
        backend,
        branch,
        "zero-candidate",
        &[put_row(branch, 1, b"zero", b"value")],
    );

    let error = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::ZERO,
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect_err("zero candidate rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_coverage_includes_user_rows() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb5);
    let manifest = durable_manifest(
        backend,
        branch,
        "user-family",
        &[put_row(branch, 1, b"user", b"value")],
    );

    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    assert!(proof.branch_coverages()[0].row_families().user_rows());
}

#[test]
fn table_manifest_coverage_includes_tombstones() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb6);
    let row = tombstone_row(branch, 1, b"deleted");
    let manifest = durable_manifest(backend, branch, "tombstone-family", &[row]);

    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    assert!(proof.branch_coverages()[0].row_families().tombstones());
}

#[test]
fn table_manifest_coverage_includes_timeline_rows() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb7);
    let timestamp = Timestamp::from_micros(700);
    let timeline = CommitTimelineRows::from_entry(
        CommitTimelineEntry::new(branch, CommitVersion::new(1), timestamp).expect("timeline"),
    )
    .expect("timeline rows")
    .into_rows()
    .to_vec();
    let manifest = durable_manifest(backend, branch, "timeline-family", &timeline);

    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    assert!(proof.branch_coverages()[0].row_families().timeline_rows());
}

#[test]
fn table_manifest_coverage_includes_materialized_replacement_rows() {
    let branch = branch_id(0xb8);
    let coverage = LifecycleTableManifestBranchCoverage::new(
        branch,
        CommitVersion::new(1),
        CommitVersion::new(1),
        ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object"),
        1,
        LifecycleTableManifestCoverageFamilies::complete(),
    )
    .expect("coverage");

    assert!(coverage.row_families().materialized_replacements());
}

#[test]
fn table_manifest_coverage_includes_inherited_layer_rows() {
    let branch = branch_id(0xb9);
    let coverage = LifecycleTableManifestBranchCoverage::new(
        branch,
        CommitVersion::new(1),
        CommitVersion::new(1),
        ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object"),
        1,
        LifecycleTableManifestCoverageFamilies::complete(),
    )
    .expect("coverage");

    assert!(coverage.row_families().inherited_layers());
}

#[test]
fn table_manifest_coverage_rejects_inherited_layer_gap() {
    let branch = branch_id(0xba);
    let error = LifecycleTableManifestBranchCoverage::new(
        branch,
        CommitVersion::new(1),
        CommitVersion::new(5),
        ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object"),
        1,
        LifecycleTableManifestCoverageFamilies::complete().without_inherited_layers(),
    )
    .expect_err("inherited gap rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn flush_watermark_below_current_is_subsumed_as_already_persisted() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xbb);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(5))
        .expect("snapshot facts");
    shell
        .services()
        .manifest()
        .persist_flush_watermark(CommitVersion::new(5))
        .expect("existing watermark");
    let manifest = durable_manifest(
        backend,
        branch,
        "below-current",
        &[put_row(branch, 4, b"below", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(4),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let outcome = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(4),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect("below current is subsumed");

    // Coverage is monotone: a candidate the persisted watermark already
    // passed is a no-op success, not an error — a queued background task
    // whose goal a concurrent checkpoint follow-up outran must not record a
    // failure for work that is already done.
    assert!(outcome.was_already_persisted());
}

#[test]
fn flush_watermark_equal_to_current_is_noop() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xbc);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(5, CommitVersion::new(5))
        .expect("snapshot facts");
    shell
        .services()
        .manifest()
        .persist_flush_watermark(CommitVersion::new(5))
        .expect("existing watermark");

    let outcome = persist_flush_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::AlreadyPersisted,
    )
    .expect("noop");

    assert!(outcome.was_already_persisted());
}

#[test]
fn table_manifest_flush_watermark_extends_existing_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd4);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_flush_watermark(CommitVersion::new(1))
        .expect("existing watermark");
    let row = put_row(branch, 2, b"extension", b"value");
    let manifest = durable_manifest(backend, branch, "extension", std::slice::from_ref(&row));
    let state = state_with_installed_table(branch, "extension", std::slice::from_ref(&row));
    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(2),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let outcome = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(2),
        CommitVersion::new(2),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect("persist");

    assert!(outcome.was_persisted());
    assert_eq!(
        shell
            .services()
            .manifest()
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        Some(CommitVersion::new(2))
    );
}

#[test]
fn flush_watermark_persist_failure_prevents_wal_truncation() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xbd);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(4))
        .expect("snapshot facts");
    let row = put_row(branch, 5, b"persist-failure", b"value");
    let manifest = durable_manifest(
        backend,
        branch,
        "persist-failure",
        std::slice::from_ref(&row),
    );
    let state = state_with_installed_table(branch, "persist-failure", std::slice::from_ref(&row));
    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(5),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");
    backend.fail_manifest_replacement_on_call(2);

    let error = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect_err("persist fails");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn flush_watermark_success_records_manifest_fact() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xbe);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(4))
        .expect("snapshot facts");
    let manifest = durable_manifest(
        backend,
        branch,
        "manifest-fact",
        &[put_row(branch, 5, b"manifest-fact", b"value")],
    );
    let row = put_row(branch, 5, b"manifest-fact", b"value");
    let state = state_with_installed_table(branch, "manifest-fact", std::slice::from_ref(&row));
    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(5),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect("persist");

    let database = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    assert_eq!(
        database.flushed_through_commit_id(),
        Some(CommitVersion::new(5))
    );
}

#[test]
fn flush_watermark_success_does_not_mutate_branch_state() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xbf);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(4))
        .expect("snapshot facts");
    let manifest = durable_manifest(
        backend,
        branch,
        "branch-unchanged",
        &[put_row(branch, 5, b"branch-unchanged", b"value")],
    );
    let row = put_row(branch, 5, b"branch-unchanged", b"value");
    let state = state_with_installed_table(branch, "branch-unchanged", std::slice::from_ref(&row));
    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(5),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");
    let before = shell.branch_state().facts();

    persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect("persist");

    assert_eq!(shell.branch_state().facts(), before);
}

#[test]
fn cache_mode_rejects_table_manifest_flush_watermark() {
    let backend: &'static MemoryBackend = crate::testkit::leak_static(MemoryBackend::new());
    let branch = branch_id(0xc0);
    let mut runtime = LifecycleCacheRuntime::open(
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
        CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
    )
    .expect("cache runtime");

    let error = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect_err("durable-only maintenance rejects");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
}

#[test]
fn recovery_rejects_missing_table_manifest_for_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc1);
    seed_checkpoint_snapshot(
        backend,
        1,
        CommitVersion::new(4),
        &rows_for_versions(branch, 1..=4, b"missing-manifest"),
    );
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(4)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("missing manifest rejects");

    assert_eq!(error.code(), "corruption.lifecycle.recovery");
}

#[test]
fn recovery_rejects_corrupt_table_manifest_for_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc2);
    let checkpoint_rows = rows_for_versions(branch, 1..=4, b"corrupt-manifest");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(4), &checkpoint_rows);
    let mut manifest_rows = checkpoint_rows;
    manifest_rows.push(put_row(branch, 5, b"corrupt", b"value"));
    durable_manifest(backend, branch, "corrupt-manifest", &manifest_rows);
    backend
        .write_object(
            &ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object"),
            b"not a table manifest",
        )
        .expect("corrupt manifest");
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(4)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("corrupt manifest rejects");

    assert_eq!(error.code(), "corruption.lifecycle.table_manifest");
}

#[test]
fn recovery_rejects_table_object_mismatch_for_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc3);
    let checkpoint_rows = rows_for_versions(branch, 1..=4, b"object-mismatch");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(4), &checkpoint_rows);
    let mut manifest_rows = checkpoint_rows;
    manifest_rows.push(put_row(branch, 5, b"object-mismatch", b"value"));
    let manifest = durable_manifest(backend, branch, "object-mismatch", &manifest_rows);
    let object = manifest.levels()[0].tables()[0].object().clone();
    backend
        .write_object(&object, b"not the published table")
        .expect("corrupt table object");
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(4)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("table object mismatch rejects");

    assert_eq!(error.code(), "corruption.lifecycle.table_manifest");
}

#[test]
fn recovery_uses_table_manifest_flush_watermark_as_replay_start_after_validation() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc4);
    let checkpoint_rows = rows_for_versions(branch, 1..=4, b"replay-start");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(4), &checkpoint_rows);
    let mut manifest_rows = checkpoint_rows;
    manifest_rows.push(put_row(branch, 5, b"manifest-row", b"value"));
    durable_manifest(backend, branch, "replay-start", &manifest_rows);
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(4)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services_mut()
        .wal_mut()
        .append(&wal_record(branch, 5, b"manifest-row", b"value"))
        .expect("append duplicate");
    shell
        .services_mut()
        .wal_mut()
        .append(&wal_record(branch, 6, b"tail-row", b"tail"))
        .expect("append tail");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recover");

    assert_eq!(outcome.wal().replay_start(), CommitVersion::new(5));
    let replayable = outcome
        .wal()
        .collect_replay_records_for_test(shell.services().wal())
        .expect("collect replay records");
    assert_eq!(replayable.len(), 1);
    assert_eq!(replayable[0].commit_version(), CommitVersion::new(6));
}

#[test]
fn recovery_replays_wal_tail_above_table_manifest_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc5);
    let checkpoint_rows = rows_for_versions(branch, 1..=4, b"tail-replay");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(4), &checkpoint_rows);
    let mut manifest_rows = checkpoint_rows;
    manifest_rows.push(put_row(branch, 5, b"manifest-row", b"value"));
    durable_manifest(backend, branch, "tail-replay", &manifest_rows);
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(4)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services_mut()
        .wal_mut()
        .append(&wal_record(branch, 6, b"tail", b"tail-value"))
        .expect("append tail");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recover");

    assert_eq!(
        outcome
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .expect("collect replay records")[0]
            .commit_version(),
        CommitVersion::new(6)
    );
}

#[test]
fn recovery_ignores_duplicate_record_at_table_manifest_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc6);
    let checkpoint_rows = rows_for_versions(branch, 1..=4, b"duplicate-watermark");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(4), &checkpoint_rows);
    let manifest_row = put_row(branch, 5, b"duplicate", b"manifest");
    let mut manifest_rows = checkpoint_rows;
    manifest_rows.push(manifest_row.clone());
    durable_manifest(backend, branch, "duplicate-watermark", &manifest_rows);
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(4)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services_mut()
        .wal_mut()
        .append(&wal_record(branch, 5, b"duplicate", b"manifest"))
        .expect("append duplicate");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recover");

    let history = shell
        .branch_state()
        .capture_read_view()
        .expect("read view")
        .history(
            manifest_row.physical_key(),
            crate::branch::read::BranchHistoryOptions::all(),
        )
        .expect("history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].row().value(), b"manifest");
}

#[test]
fn recovery_after_truncation_restores_history_reads_within_retained_bounds() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc7);
    let key = physical_key(branch, b"history");
    let checkpoint_row = put_row(branch, 1, b"history", b"old");
    seed_checkpoint_snapshot(
        backend,
        1,
        CommitVersion::new(1),
        std::slice::from_ref(&checkpoint_row),
    );
    let mut manifest_rows = vec![checkpoint_row.clone()];
    manifest_rows.extend(rows_for_versions(branch, 2..=4, b"history-fill"));
    manifest_rows.push(put_row(branch, 5, b"history", b"new"));
    durable_manifest(backend, branch, "history-restore", &manifest_rows);
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(1)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recover");

    let history = shell
        .branch_state()
        .capture_read_view()
        .expect("read view")
        .history(&key, crate::branch::read::BranchHistoryOptions::all())
        .expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].row().value(), b"new");
    assert_eq!(history[1].row().value(), b"old");
}

#[test]
fn policy_downgrade_blocks_table_manifest_flush_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc8);
    let manifest = durable_manifest(
        backend,
        branch,
        "policy-downgrade",
        &[put_row(branch, 1, b"policy", b"value")],
    );
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::PolicyDowngrade,
        vec![RecoveryFault::new(RecoveryFaultKind::NoManifestFallback, "fallback").expect("fault")],
    )
    .expect("health");

    let error = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &health,
    )
    .expect_err("policy downgrade blocks");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn data_loss_blocks_table_manifest_flush_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xc9);
    let manifest = durable_manifest(
        backend,
        branch,
        "data-loss",
        &[put_row(branch, 1, b"data-loss", b"value")],
    );
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingTableObject, "missing table")
                .expect("fault"),
        ],
    )
    .expect("health");

    let error = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &health,
    )
    .expect_err("data loss blocks");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn telemetry_health_allows_unrelated_table_manifest_flush_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xca);
    let manifest = durable_manifest(
        backend,
        branch,
        "telemetry-health",
        &[put_row(branch, 1, b"telemetry", b"value")],
    );
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![RecoveryFault::new(RecoveryFaultKind::IoFailure, "telemetry").expect("fault")],
    )
    .expect("health");

    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &health,
    )
    .expect("telemetry allows proof");

    assert_eq!(proof.recovery_health_epoch(), 1);
}

#[test]
fn table_manifest_reachability_debt_blocks_flush_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xcb);
    let manifest = durable_manifest(
        backend,
        branch,
        "reachability-debt",
        &[put_row(branch, 1, b"reachability", b"value")],
    );
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingTableObject, "table reachability")
                .expect("fault"),
        ],
    )
    .expect("health");

    assert!(
        LifecycleTableManifestFlushCoverageProof::from_table_manifests(
            CommitVersion::new(1),
            &[manifest],
            &health,
        )
        .is_err()
    );
}

#[test]
fn quarantine_inventory_mismatch_blocks_flush_proof_when_relevant() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xcc);
    let manifest = durable_manifest(
        backend,
        branch,
        "quarantine-mismatch",
        &[put_row(branch, 1, b"quarantine", b"value")],
    );
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::PolicyDowngrade,
        vec![
            RecoveryFault::new(RecoveryFaultKind::QuarantineInventoryMismatch, "quarantine")
                .expect("fault"),
        ],
    )
    .expect("health");

    assert!(
        LifecycleTableManifestFlushCoverageProof::from_table_manifests(
            CommitVersion::new(1),
            &[manifest],
            &health,
        )
        .is_err()
    );
}

#[test]
fn missing_branch_lifecycle_fact_blocks_absence_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    crate::lifecycle::tests::checkpoint::shared::seed_database_manifest(backend);
    let branch = branch_id(0xcd);
    let missing = branch_id(0xce);
    let manifest = durable_manifest(
        backend,
        branch,
        "missing-lifecycle-fact",
        &[put_row(branch, 1, b"branch", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");
    let required_branch_epochs = [
        (branch, proof.branch_coverages()[0].manifest_sequence()),
        (missing, 1),
    ];

    let error = persist_table_manifest_watermark_with_branch_epochs(
        shell.services().manifest(),
        CommitVersion::new(1),
        CommitVersion::new(1),
        &proof,
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &required_branch_epochs,
    )
    .expect_err("missing branch rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn branch_absence_does_not_advance_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xcf);
    let missing = branch_id(0xd0);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");
    let manifest = durable_manifest(
        backend,
        branch,
        "branch-absence",
        &[put_row(branch, 1, b"branch", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(1),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");
    let required_branch_epochs = [
        (branch, proof.branch_coverages()[0].manifest_sequence()),
        (missing, 1),
    ];

    let error = persist_table_manifest_watermark_with_branch_epochs(
        shell.services().manifest(),
        CommitVersion::new(1),
        CommitVersion::new(1),
        &proof,
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &required_branch_epochs,
    )
    .expect_err("missing branch coverage rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        None
    );
}

#[test]
fn maintenance_task_can_request_table_manifest_flush_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd1);
    let mut runtime = runtime_with_flushed_table(branch, backend, b"maintenance-request");
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("enqueue");

    let outcome = runtime
        .run_next_flush_watermark_maintenance()
        .expect("run")
        .expect("maintenance outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::FlushWatermark);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(outcome.state_changes(), 1);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        Some(CommitVersion::new(1))
    );
}

#[test]
fn maintenance_task_coalesces_table_manifest_flush_watermark_to_newest_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd2);
    let mut runtime = open_runtime(branch, backend);

    let first = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("first enqueue");
    let second = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("second enqueue");
    let third = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(2),
        ))
        .expect("third enqueue");

    assert!(first.was_enqueued());
    assert!(second.was_coalesced());
    assert!(third.was_coalesced());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
    assert_eq!(
        runtime.pending_flush_watermark_candidate_for_test(),
        Some(CommitVersion::new(2))
    );
}

#[test]
fn background_flush_watermark_uses_highest_provable_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd3);
    let mut runtime = runtime_with_flushed_table(branch, backend, b"provable-candidate");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(2),
        ))
        .expect("enqueue watermark");

    let outcome = runtime
        .run_next_background_flush_watermark_maintenance()
        .expect("run")
        .expect("background watermark outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::FlushWatermark);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        Some(CommitVersion::new(1))
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn background_flush_watermark_uses_checkpoint_covered_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xda);
    let mut runtime = runtime_with_flushed_table(branch, backend, b"checkpoint-covered");
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(9, CommitVersion::new(4))
        .expect("snapshot facts");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("enqueue watermark");

    let outcome = runtime
        .run_next_background_flush_watermark_maintenance()
        .expect("run")
        .expect("background watermark outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::FlushWatermark);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        Some(CommitVersion::new(1))
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn flush_watermark_no_longer_waits_for_queued_flush_work() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd7);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(2),
        ))
        .expect("enqueue watermark");

    // Flush-watermark is no longer gated by a queued flush task (that gate self-starved the
    // watermark advance under sustained flush pressure). It runs against the current table
    // manifest; with no coverage yet it defers, leaving the flush task for its turn.
    let outcome = runtime
        .run_next_flush_watermark_maintenance()
        .expect("run watermark")
        .expect("watermark maintenance");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::FlushWatermark);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
    assert_eq!(runtime.maintenance_status().stats().deferred(), 1);
}

#[test]
fn background_flush_watermark_waits_for_table_manifest_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd8);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(2),
        ))
        .expect("enqueue watermark");

    let outcome = runtime
        .run_next_background_flush_watermark_maintenance()
        .expect("run background watermark");

    assert_eq!(outcome, None);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
    assert_eq!(runtime.maintenance_status().stats().started(), 0);
}

#[test]
fn maintenance_task_reports_deferred_when_table_coverage_missing() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd3);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("enqueue");

    let outcome = runtime
        .run_next_flush_watermark_maintenance()
        .expect("run")
        .expect("maintenance outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::FlushWatermark);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        None
    );
}

#[test]
fn maintenance_task_does_not_run_table_manifest_watermark_after_close_begins() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd4);
    let mut runtime = runtime_with_flushed_table(branch, backend, b"close-cancel");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("enqueue");

    let outcome = runtime.close().expect("close");

    assert_eq!(outcome.status(), CloseOutcomeStatus::Complete);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(runtime.maintenance_status().stats().canceled(), 1);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        None
    );
}

#[test]
fn maintenance_task_preserves_stats_for_watermark_and_truncation() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd5);
    let mut runtime = runtime_with_flushed_table(branch, backend, b"stats");
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("enqueue watermark");
    let outcome = runtime
        .run_next_flush_watermark_maintenance()
        .expect("run")
        .expect("watermark outcome");

    assert_eq!(outcome.stats().maintenance_tasks(), 1);
    assert_eq!(runtime.maintenance_status().stats().completed(), 1);
}

#[test]
fn maintenance_task_does_not_claim_checkpoint_execution() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd6);
    let mut runtime = runtime_with_flushed_table(branch, backend, b"not-checkpoint");
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(1),
        ))
        .expect("enqueue");

    let outcome = runtime
        .run_next_flush_watermark_maintenance()
        .expect("run")
        .expect("maintenance outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::FlushWatermark);
    assert!(backend.snapshot_objects().is_empty());
}

#[test]
fn wal_truncation_from_table_manifest_flush_watermark_uses_typed_proof() {
    let proof = WalRetentionProof::flush_watermark(CommitVersion::new(9));

    assert_eq!(proof.source(), WalRetentionProofSource::FlushWatermark);
    assert_eq!(proof.covered_through(), CommitVersion::new(9));
}

#[test]
fn table_manifest_watermark_publish_uses_canonical_manifest_bytes() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd1);
    let manifest = durable_manifest(
        backend,
        branch,
        "canonical-published",
        &[put_row(branch, 1, b"canonical", b"value")],
    );
    let object = ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object");
    let bytes = backend.read_object(&object).expect("read manifest");

    assert_eq!(
        bytes,
        encode_table_manifest(&manifest).expect("canonical bytes")
    );
}

fn unpublished_manifest(
    backend: &'static CheckpointTestBackend,
    branch: BranchId,
    identity: &str,
    rows: &[StorageRow],
    provenance: TableManifestTableProvenance,
) -> TableManifest {
    let identity = TableIdentity::new(identity).expect("table identity");
    let bytes = table_bytes(identity.clone(), rows);
    let facts = TableObjectService::new(backend)
        .publish_create(&branch.to_string(), 0, identity.as_str(), &bytes)
        .expect("publish table object");
    let reader =
        ImmutableTableReader::open_bytes(identity.clone(), bytes, TableReaderConfig::default())
            .expect("table reader");
    let reference = TableManifestTableRef::new(
        identity,
        facts.object().clone(),
        0,
        table_manifest_facts(&reader),
        table_manifest_bounds(reader.rows()),
        provenance,
    )
    .expect("table reference");
    let extras =
        crate::table::TableSummaryExtras::from_rows(reader.rows()).expect("recovery test extras");
    let row_split = TableRowSplit::new(extras.put_rows(), extras.tombstone_rows());
    TableManifest::new(
        branch,
        None,
        1,
        vec![TableManifestLevel::new(BranchLevel::ZERO, vec![reference]).expect("level")],
        Vec::new(),
        vec![table_row_split_extension_section(&[row_split]).expect("row-split section")],
    )
    .expect("manifest")
}

fn tombstone_row(branch: BranchId, version: u64, user_key: &'static [u8]) -> StorageRow {
    StorageRow::tombstone(
        physical_key(branch, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(version * 100),
    )
}

fn state_with_installed_table(
    branch: BranchId,
    identity: &str,
    rows: &[StorageRow],
) -> BranchLocalState {
    let mut state = BranchLocalState::empty(branch);
    install_l0_table_for_test(&mut state, branch, identity, rows);
    state
}

fn runtime_with_flushed_table(
    branch: BranchId,
    backend: &'static CheckpointTestBackend,
    key: &'static [u8],
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    let suffix = std::str::from_utf8(key).expect("test key is utf8");
    runtime
        .flush_frozen(&flush_request_for_test(branch, suffix))
        .expect("flush");
    runtime
}

// Builds the same PhysicalKey shape that `shared::durable_batch` produces. We
// re-construct it inline rather than importing the shared helper because the
// shared physical-key constants ("checkpoint" table, engine space 0x30) are
// the contract between `durable_batch` writes and post-recovery reads.
fn shared_physical_key(branch: BranchId, user_key: &'static [u8]) -> PhysicalKey {
    PhysicalKey::new(
        branch,
        "checkpoint",
        StorageSpaceId::engine(0x30).expect("space"),
        user_key.to_vec(),
    )
    .expect("physical key")
}

#[test]
fn end_to_end_publish_persist_truncate_recover() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd7);

    // Phase 1: commit v1, run a real checkpoint at v1 (publishes a snapshot
    // object), then commit v2 above the checkpoint.
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"end-to-end-1", b"value-1"),
            generation_guard(),
        )
        .expect("first commit");
    runtime
        .checkpoint(
            &LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(10))
                .expect("checkpoint request"),
        )
        .expect("checkpoint");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"end-to-end-2", b"value-2"),
            generation_guard(),
        )
        .expect("second commit");

    // Phase 2: rotate and flush. Publishes a table object containing both
    // rows and a branch table manifest covering versions 1 and 2.
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .flush_frozen(&flush_request_for_test(branch, "end-to-end"))
        .expect("flush");

    // Phase 3: persist the table-manifest-backed flush watermark at v2.
    // The checkpoint sits at v1, so this genuinely extends coverage.
    let persist = runtime
        .persist_table_manifest_flush_watermark(CommitVersion::new(2))
        .expect("persist table-manifest flush watermark");
    assert!(persist.was_persisted());
    assert_eq!(persist.persisted_watermark(), Some(CommitVersion::new(2)));

    // Phase 4: the WAL truncation maintenance task picks the typed
    // flush-watermark proof (flush >= snapshot path in
    // wal_truncation_request_from_maintenance_task) and applies it through
    // the WAL service.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue truncation");
    let truncation = runtime
        .run_next_wal_truncation_maintenance()
        .expect("run truncation")
        .expect("truncation outcome");
    assert_eq!(truncation.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(truncation.task_kind(), MaintenanceTaskKind::WalTruncation);

    // Phase 5: drop and reopen. Recovery must accept the persisted flush
    // watermark above the checkpoint, apply the table manifest as the
    // recovery base, and serve both writes.
    drop(runtime);
    let reopened = open_runtime(branch, backend);
    let view = reopened.read_view().expect("read view");
    assert_eq!(
        view.latest(&shared_physical_key(branch, b"end-to-end-1"))
            .expect("read v1")
            .expect("v1 row")
            .row()
            .value(),
        b"value-1"
    );
    assert_eq!(
        view.latest(&shared_physical_key(branch, b"end-to-end-2"))
            .expect("read v2")
            .expect("v2 row")
            .row()
            .value(),
        b"value-2"
    );
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .flushed_through_commit_id(),
        Some(CommitVersion::new(2))
    );
}

#[test]
fn flush_completion_advances_watermark_and_reclaims_wal_without_checkpoint() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xf1);
    let mut runtime = open_runtime(branch, backend);

    // Commit, rotate, and flush to L0 through the maintenance path — no checkpoint anywhere.
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"flush-reclaim", b"value"),
            generation_guard(),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    let flush = runtime
        .run_next_flush_maintenance()
        .expect("run flush")
        .expect("flush outcome");
    assert_eq!(flush.status(), MaintenanceOutcomeStatus::Completed);

    // BS5.3: flush-driven reclaim is asynchronous — the flush enqueued the coalescing
    // flush-watermark task (whose coverage scan runs off-lock) instead of persisting the
    // watermark inline under the runtime lock. Drain it before asserting the advance.
    runtime
        .run_next_flush_watermark_maintenance()
        .expect("run flush watermark")
        .expect("watermark outcome");

    // Flush-driven reclaim advanced the flush watermark with no checkpoint: the snapshot
    // watermark stays unset, yet flushed_through_commit_id now covers the flushed commit.
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("manifest");
    assert_eq!(manifest.snapshot_watermark(), None);
    assert_eq!(
        manifest.flushed_through_commit_id(),
        Some(CommitVersion::new(1))
    );

    // Reclaim also enqueued a WAL-truncation task. It runs against the freshly advanced
    // watermark (the flush-watermark retention proof) and completes, rather than deferring
    // for "no retention proof" as it would before any watermark advanced.
    let truncation = runtime
        .run_next_wal_truncation_maintenance()
        .expect("run truncation")
        .expect("truncation outcome");
    assert_eq!(truncation.task_kind(), MaintenanceTaskKind::WalTruncation);
    assert_eq!(truncation.status(), MaintenanceOutcomeStatus::Completed);
}
