use super::*;
use crate::backend::Backend;
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::facts::{BranchLevel, BranchTableDescriptor};
use crate::branch::read::BranchOwnedTable;
use crate::branch::state::BranchLocalState;
use crate::commit::{
    CommitBranchGeneration, CommitManualTimestampSource, CommitRuntimeConfig, CommitTimelineEntry,
    CommitTimelineRows,
};
use crate::format::{
    encode_manifest, encode_wal_segment_header, table_row_split_extension_section,
    DatabaseManifest, TableManifest, TableManifestLevel, TableManifestTableBounds,
    TableManifestTableFacts, TableManifestTableProvenance, TableManifestTableRef, TableRowSplit,
    WalCommitPayload, WalRecord, WalSegmentHeader,
};
use crate::layout::ObjectLayout;
use crate::lifecycle::encode_checkpoint_row_section;
use crate::lifecycle::tests::checkpoint::shared::{
    branch_id, durable_batch, generation_guard, open_runtime, CheckpointTestBackend,
};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::service::{DatabaseManifestService, TableManifestService, TableObjectService};
use crate::service::{SnapshotPublishRequest, SnapshotService};
use crate::table::{
    sort_table_rows_by_key, ImmutableTableBuilder, ImmutableTableReader, TableIdentity,
    TablePhysicalKeyBytes, TableReaderConfig, TableRow,
};
use strata_core::{BranchId, CommitVersion, Timestamp};

mod remaining;

const DATABASE_ID: [u8; 16] = [0x7d; 16];

fn required_branch_epochs_for(
    proof: &LifecycleTableManifestFlushCoverageProof,
) -> Vec<(BranchId, u64)> {
    proof
        .branch_coverages()
        .iter()
        .map(|coverage| (coverage.branch_id(), coverage.manifest_sequence()))
        .collect()
}

fn persist_table_manifest_watermark(
    manifest: &DatabaseManifestService<'_>,
    visible_version: CommitVersion,
    candidate: CommitVersion,
    proof: &LifecycleFlushWatermarkProof,
    coverage: &LifecycleTableManifestFlushCoverageProof,
) -> LifecycleResult<LifecycleFlushWatermarkOutcome> {
    persist_flush_watermark_with_table_manifest_proof(
        manifest,
        visible_version,
        candidate,
        proof,
        coverage.manifest_epoch(),
        coverage.recovery_health_epoch(),
        &required_branch_epochs_for(coverage),
    )
}

fn persist_table_manifest_watermark_with_branch_epochs(
    manifest: &DatabaseManifestService<'_>,
    visible_version: CommitVersion,
    candidate: CommitVersion,
    coverage: &LifecycleTableManifestFlushCoverageProof,
    proof: &LifecycleFlushWatermarkProof,
    required_branch_epochs: &[(BranchId, u64)],
) -> LifecycleResult<LifecycleFlushWatermarkOutcome> {
    persist_flush_watermark_with_table_manifest_proof(
        manifest,
        visible_version,
        candidate,
        proof,
        coverage.manifest_epoch(),
        coverage.recovery_health_epoch(),
        required_branch_epochs,
    )
}

#[test]
fn table_manifest_flush_proof_accepts_exact_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x90);
    let manifest = durable_manifest(
        backend,
        branch,
        "exact-coverage",
        &[put_row(branch, 5, b"exact", b"value")],
    );

    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(5),
        std::slice::from_ref(&manifest),
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    assert_eq!(proof.candidate(), CommitVersion::new(5));
    assert_eq!(proof.manifest_epoch(), manifest.manifest_sequence());
    assert_eq!(proof.recovery_health_epoch(), 1);
    assert_eq!(proof.branch_coverages().len(), 1);
    assert_eq!(proof.branch_coverages()[0].branch_id(), branch);
    assert_eq!(
        proof.branch_coverages()[0].covered_max(),
        CommitVersion::new(5)
    );
    assert!(proof.branch_coverages()[0].row_families().user_rows());
}

#[test]
fn table_manifest_flush_proof_rejects_missing_branch_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x91);
    let missing = branch_id(0x92);
    let manifest = durable_manifest(
        backend,
        branch,
        "missing-branch",
        &[put_row(branch, 3, b"covered", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(3),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let error = proof
        .validate_required_branches(&[branch, missing])
        .expect_err("missing branch rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_rejects_stale_manifest_epoch() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa0);
    let manifest = durable_manifest(
        backend,
        branch,
        "stale-manifest",
        &[put_row(branch, 3, b"covered", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(3),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let error = proof
        .validate_current_epochs(2, proof.recovery_health_epoch())
        .expect_err("stale manifest epoch rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_rejects_stale_recovery_health_epoch() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa1);
    let manifest = durable_manifest(
        backend,
        branch,
        "stale-health",
        &[put_row(branch, 3, b"covered", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(3),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let error = proof
        .validate_current_epochs(proof.manifest_epoch(), 2)
        .expect_err("stale health epoch rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_rejects_stale_branch_epoch() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let first_branch = branch_id(0xa7);
    let second_branch = branch_id(0xa8);
    let first = durable_manifest(
        backend,
        first_branch,
        "branch-epoch-first",
        &[put_row(first_branch, 4, b"first", b"value")],
    );
    let second = durable_manifest(
        backend,
        second_branch,
        "branch-epoch-second",
        &[put_row(second_branch, 4, b"second", b"value")],
    );
    let proof = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(4),
        &[first.clone(), second.clone()],
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let error = proof
        .validate_current_branch_epochs(&[
            (first_branch, first.manifest_sequence() + 1),
            (second_branch, second.manifest_sequence()),
        ])
        .expect_err("stale branch epoch rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_is_deterministic_for_shuffled_inputs() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let first_branch = branch_id(0xa2);
    let second_branch = branch_id(0xa3);
    let first = durable_manifest(
        backend,
        first_branch,
        "shuffle-first",
        &[put_row(first_branch, 4, b"first", b"value")],
    );
    let second = durable_manifest(
        backend,
        second_branch,
        "shuffle-second",
        &[put_row(second_branch, 4, b"second", b"value")],
    );

    let forward = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(4),
        &[first.clone(), second.clone()],
        &RecoveryHealth::Healthy,
    )
    .expect("forward proof");
    let reverse = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(4),
        &[second, first],
        &RecoveryHealth::Healthy,
    )
    .expect("reverse proof");

    assert_eq!(forward, reverse);
}

#[test]
fn table_manifest_flush_proof_rejects_active_rows_below_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x93);
    let manifest = durable_manifest(
        backend,
        branch,
        "active-gap",
        &[put_row(branch, 4, b"covered", b"value")],
    );
    let mut state = BranchLocalState::empty(branch);
    state
        .append_committed_row(put_row(branch, 4, b"active", b"value"))
        .expect("append active row");

    let error = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(4),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect_err("active rows reject");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_rejects_frozen_rows_below_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x94);
    let manifest = durable_manifest(
        backend,
        branch,
        "frozen-gap",
        &[put_row(branch, 4, b"covered", b"value")],
    );
    let mut state = BranchLocalState::empty(branch);
    state
        .append_committed_row(put_row(branch, 4, b"frozen", b"value"))
        .expect("append row");
    state.rotate_active();

    let error = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(4),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect_err("frozen rows reject");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn table_manifest_flush_proof_allows_mutable_rows_above_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa6);
    let covered = put_row(branch, 4, b"covered", b"value");
    let manifest = durable_manifest(
        backend,
        branch,
        "mutable-above-candidate",
        std::slice::from_ref(&covered),
    );
    let mut state = BranchLocalState::empty(branch);
    install_l0_table_for_test(&mut state, branch, "mutable-above-candidate", &[covered]);
    state
        .append_committed_row(put_row(branch, 5, b"active-above", b"value"))
        .expect("append active row above candidate");

    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(4),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("active rows above candidate do not block proof");

    assert_eq!(
        proof.branch_coverages()[0].covered_max(),
        CommitVersion::new(4)
    );
}

#[test]
fn table_manifest_coverage_rejects_timeline_gap() {
    let branch = branch_id(0x95);
    let coverage = LifecycleTableManifestBranchCoverage::new(
        branch,
        CommitVersion::new(1),
        CommitVersion::new(5),
        ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object"),
        1,
        LifecycleTableManifestCoverageFamilies::complete().without_timeline_rows(),
    );

    assert_eq!(
        coverage.expect_err("timeline gap rejects").code(),
        "failed_precondition.lifecycle.wal_retention"
    );
}

#[test]
fn table_manifest_coverage_rejects_tombstone_gap() {
    let branch = branch_id(0x96);
    let coverage = LifecycleTableManifestBranchCoverage::new(
        branch,
        CommitVersion::new(1),
        CommitVersion::new(5),
        ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object"),
        1,
        LifecycleTableManifestCoverageFamilies::complete().without_tombstones(),
    );

    assert_eq!(
        coverage.expect_err("tombstone gap rejects").code(),
        "failed_precondition.lifecycle.wal_retention"
    );
}

#[test]
fn unsafe_recovery_health_blocks_table_manifest_flush_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa4);
    let manifest = durable_manifest(
        backend,
        branch,
        "unsafe-health",
        &[put_row(branch, 4, b"covered", b"value")],
    );
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingTableObject, "missing table")
                .expect("fault"),
        ],
    )
    .expect("degraded health");

    let error = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(4),
        &[manifest],
        &health,
    )
    .expect_err("unsafe health rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn flush_watermark_persists_from_table_manifest_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x97);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(4))
        .expect("snapshot facts");
    let table_row = put_row(branch, 5, b"flushed", b"value");
    let manifest = durable_manifest(
        backend,
        branch,
        "persist-table-proof",
        std::slice::from_ref(&table_row),
    );
    let mut state = BranchLocalState::empty(branch);
    install_l0_table_for_test(&mut state, branch, "persist-table-proof", &[table_row]);
    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(5),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let outcome = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect("persist watermark");

    assert!(outcome.was_persisted());
    let database = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    assert_eq!(
        database.flushed_through_commit_id(),
        Some(CommitVersion::new(5))
    );
    assert_eq!(database.snapshot_watermark(), Some(4));
}

#[test]
fn flush_watermark_persists_from_table_manifest_coverage_without_checkpoint() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb0);
    let shell = assemble_shell(branch, backend).expect("shell");
    let table_rows = rows_for_versions(branch, 1..=5, b"genesis-proof");
    let manifest = durable_manifest(backend, branch, "persist-genesis-proof", &table_rows);
    let mut state = BranchLocalState::empty(branch);
    install_l0_table_for_test(&mut state, branch, "persist-genesis-proof", &table_rows);
    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(5),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let outcome = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
        &proof,
    )
    .expect("persist watermark");

    assert!(outcome.was_persisted());
    let database = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    assert_eq!(
        database.flushed_through_commit_id(),
        Some(CommitVersion::new(5))
    );
    assert_eq!(database.snapshot_watermark(), None);
}

#[test]
fn flush_watermark_persists_from_combined_checkpoint_and_table_manifest_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x98);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(3, CommitVersion::new(3))
        .expect("snapshot facts");
    let table_rows = rows_for_versions(branch, 4..=5, b"combined");
    let manifest = durable_manifest(backend, branch, "combined-proof", &table_rows);
    let mut state = BranchLocalState::empty(branch);
    install_l0_table_for_test(&mut state, branch, "combined-proof", &table_rows);
    let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest(
        CommitVersion::new(5),
        &state,
        &manifest,
        &RecoveryHealth::Healthy,
    )
    .expect("proof");

    let outcome = persist_table_manifest_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::Combined {
            checkpoint: CommitVersion::new(3),
            table_manifest: proof.clone(),
        },
        &proof,
    )
    .expect("persist combined watermark");

    assert_eq!(outcome.persisted_watermark(), Some(CommitVersion::new(5)));
}

#[test]
fn flush_watermark_rejects_table_manifest_candidate_above_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x99);
    let manifest = durable_manifest(
        backend,
        branch,
        "above-coverage",
        &[put_row(branch, 4, b"table", b"value")],
    );

    let error = LifecycleTableManifestFlushCoverageProof::from_table_manifests(
        CommitVersion::new(5),
        &[manifest],
        &RecoveryHealth::Healthy,
    )
    .expect_err("coverage gap rejects");

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn flush_watermark_success_does_not_publish_table_manifest() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9a);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(4))
        .expect("snapshot facts");
    let table_row = put_row(branch, 5, b"table", b"value");
    let manifest = durable_manifest(
        backend,
        branch,
        "no-publish",
        std::slice::from_ref(&table_row),
    );
    let before = TableManifestService::new(backend)
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");
    let mut state = BranchLocalState::empty(branch);
    install_l0_table_for_test(&mut state, branch, "no-publish", &[table_row]);
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
    .expect("persist watermark");

    let after = TableManifestService::new(backend)
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");
    assert_eq!(after, before);
}

#[test]
fn durable_runtime_persists_table_manifest_flush_watermark_after_flush() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa5);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"runtime-watermark", b"value"),
            generation_guard(),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .flush_frozen(&flush_request_for_test(branch, "runtime-watermark"))
        .expect("flush");
    runtime
        .services()
        .manifest()
        .persist_snapshot_facts(1, CommitVersion::new(1))
        .expect("snapshot facts");

    let outcome = runtime
        .persist_table_manifest_flush_watermark(CommitVersion::new(1))
        .expect("persist table-backed watermark");

    assert!(outcome.was_persisted());
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("database manifest")
            .flushed_through_commit_id(),
        Some(CommitVersion::new(1))
    );
}

#[test]
fn recovery_accepts_flush_watermark_above_checkpoint_when_table_manifest_covers() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9b);
    let checkpoint_rows = rows_for_versions(branch, 1..=4, b"checkpoint-covered");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(4), &checkpoint_rows);
    let mut manifest_rows = checkpoint_rows.clone();
    let table_row = put_row(branch, 5, b"covered", b"table");
    manifest_rows.push(table_row.clone());
    durable_manifest(backend, branch, "recovery-covered", &manifest_rows);
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
        .append(&wal_record(branch, 5, b"covered", b"table"))
        .expect("append boundary");
    shell
        .services_mut()
        .wal_mut()
        .append(&wal_record(branch, 6, b"tail", b"wal"))
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
    let view = shell.branch_state().capture_read_view().expect("read view");
    assert_eq!(
        view.latest(table_row.physical_key())
            .expect("read table row")
            .expect("table row")
            .row()
            .value(),
        b"table"
    );
}

#[test]
fn recovery_accepts_flush_watermark_without_checkpoint_when_table_manifest_covers() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb1);
    let mut manifest_rows = rows_for_versions(branch, 1..=4, b"genesis-covered");
    let table_row = put_row(branch, 5, b"genesis", b"table");
    manifest_rows.push(table_row.clone());
    durable_manifest(backend, branch, "recovery-genesis-covered", &manifest_rows);
    seed_database_manifest(backend, None, None, Some(CommitVersion::new(5)));
    let mut shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services_mut()
        .wal_mut()
        .append(&wal_record(branch, 5, b"genesis", b"table"))
        .expect("append boundary");
    shell
        .services_mut()
        .wal_mut()
        .append(&wal_record(branch, 6, b"tail", b"wal"))
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
    let view = shell.branch_state().capture_read_view().expect("read view");
    assert_eq!(
        view.latest(table_row.physical_key())
            .expect("read table row")
            .expect("table row")
            .row()
            .value(),
        b"table"
    );
}

#[test]
fn recovery_rejects_flush_watermark_above_table_manifest_coverage() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9c);
    let checkpoint_rows = rows_for_versions(branch, 1..=3, b"checkpoint-gap");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(3), &checkpoint_rows);
    durable_manifest(
        backend,
        branch,
        "recovery-gap",
        &[put_row(branch, 4, b"covered", b"table")],
    );
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(3)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("coverage gap rejects");

    assert_eq!(error.code(), "corruption.lifecycle.recovery");
}

#[test]
fn recovery_after_truncation_restores_latest_reads() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9d);
    let checkpoint_rows = rows_for_versions(branch, 1..=4, b"checkpoint-latest");
    seed_checkpoint_snapshot(backend, 1, CommitVersion::new(4), &checkpoint_rows);
    let mut manifest_rows = checkpoint_rows;
    let table_row = put_row(branch, 5, b"restored", b"table");
    manifest_rows.push(table_row.clone());
    durable_manifest(backend, branch, "recovery-latest", &manifest_rows);
    seed_database_manifest(
        backend,
        Some(CommitVersion::new(4)),
        Some(1),
        Some(CommitVersion::new(5)),
    );
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recover");

    let view = shell.branch_state().capture_read_view().expect("read view");
    assert!(view
        .latest(&physical_key(branch, b"restored"))
        .expect("read")
        .is_some());
}

fn durable_manifest(
    backend: &'static CheckpointTestBackend,
    branch: BranchId,
    identity: &str,
    rows: &[StorageRow],
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
        TableManifestTableProvenance::Flush,
    )
    .expect("table reference");
    let extras =
        crate::table::TableSummaryExtras::from_rows(reader.rows()).expect("recovery test extras");
    let row_split = TableRowSplit::new(extras.put_rows(), extras.tombstone_rows());
    let manifest = TableManifest::new(
        branch,
        None,
        1,
        vec![TableManifestLevel::new(BranchLevel::ZERO, vec![reference]).expect("level")],
        Vec::new(),
        vec![table_row_split_extension_section(&[row_split]).expect("row-split section")],
    )
    .expect("manifest");
    TableManifestService::new(backend)
        .publish_replace_manifest(branch, &manifest)
        .expect("publish table manifest");
    manifest
}

fn seed_database_manifest(
    backend: &'static CheckpointTestBackend,
    snapshot_watermark: Option<CommitVersion>,
    snapshot_id: Option<u64>,
    flush_watermark: Option<CommitVersion>,
) {
    let manifest = DatabaseManifest::new(DATABASE_ID, "identity")
        .expect("database manifest")
        .with_recovery_facts(
            1,
            snapshot_watermark.map(CommitVersion::as_u64),
            snapshot_id,
            flush_watermark,
        )
        .expect("database manifest facts");
    let bytes = encode_manifest(&manifest).expect("database manifest bytes");
    backend
        .write_object(
            &ObjectLayout::database_manifest().expect("database manifest object"),
            &bytes,
        )
        .expect("write database manifest");
    // #2765: an attested manifest requires a WAL segment on disk; plant a
    // header-only active segment when the fixture has not staged one.
    if manifest.snapshot_watermark().is_some() {
        let segment = ObjectLayout::wal_segment(1).expect("segment object");
        if backend.object_metadata(&segment).is_err() {
            let header = encode_wal_segment_header(&WalSegmentHeader::new(1, DATABASE_ID));
            backend
                .write_object(&segment, &header)
                .expect("write empty wal segment");
        }
    }
}

fn seed_checkpoint_snapshot(
    backend: &'static CheckpointTestBackend,
    snapshot_id: u64,
    watermark: CommitVersion,
    rows: &[StorageRow],
) {
    SnapshotService::new(backend)
        .publish_create(SnapshotPublishRequest::new(
            snapshot_id,
            watermark,
            Timestamp::from_micros(watermark.as_u64() * 100),
            DATABASE_ID,
            "identity",
            vec![encode_checkpoint_row_section(rows).expect("checkpoint row section")],
        ))
        .expect("publish checkpoint snapshot");
}

fn assemble_shell(
    branch: BranchId,
    backend: &'static CheckpointTestBackend,
) -> LifecycleResult<LifecycleDurableLocalShell<'static>> {
    LifecycleDurableLocalShell::assemble(
        LifecycleDurableLocalOpenRequest::new(
            StorageOpenPlan::new(
                StorageMode::DurableLocalStandard,
                LifecycleCodecId::identity(),
                RecoveryStrictness::Strict,
                LifecycleConfig::default(),
            )
            .expect("open plan"),
            DATABASE_ID,
            branch,
            CommitBranchGeneration::new(1).expect("generation"),
            BranchRuntimeConfig::default(),
            CommitRuntimeConfig::default(),
            crate::service::WalServiceConfig::default(),
        )?,
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
}

// Safety guard for the lock-convoy fix: the interval-bounded coverage scan (which
// skips non-overlapping tables via `commit_range()` and collects only `[floor,
// candidate]`) must produce exactly the same versions as a full row scan filtered to
// the interval, and `branch_checkpoint_flush_boundary` the same max — for every
// floor/candidate/visible. A divergence here is a recovery-coverage correctness bug.
// See docs/design/performance/durable-background-lock-convoy.md.
#[test]
fn bounded_coverage_scan_matches_full_scan_over_interval() {
    use crate::lifecycle::checkpoint::{
        branch_checkpoint_flush_boundary, branch_durable_commit_versions_in_interval,
    };

    let branch = branch_id(0xc1);
    let mut state = BranchLocalState::empty(branch);
    // Tables that sit below / straddle / above the swept domain, plus an inter-table
    // gap (9->15, 16->30) and a cross-table duplicate version (7 in both b and c).
    let tables: &[(&str, std::ops::RangeInclusive<u64>, &'static [u8])] = &[
        ("scan-a", 1..=3, b"scan-a"),
        ("scan-b", 5..=7, b"scan-b"),
        ("scan-c", 7..=9, b"scan-c"),
        ("scan-d", 15..=16, b"scan-d"),
        ("scan-e", 30..=31, b"scan-e"),
    ];
    let mut all_versions: Vec<u64> = Vec::new();
    for &(identity, ref range, prefix) in tables {
        let rows = rows_for_versions(branch, range.clone(), prefix);
        all_versions.extend(range.clone());
        install_l0_table_for_test(&mut state, branch, identity, &rows);
    }
    all_versions.sort_unstable();
    all_versions.dedup();

    for floor in 0..=33u64 {
        for candidate in 0..=33u64 {
            let bounded: Vec<u64> = branch_durable_commit_versions_in_interval(
                state.owned_levels(),
                state.inherited_layers(),
                CommitVersion::new(floor),
                CommitVersion::new(candidate),
            )
            .into_iter()
            .map(CommitVersion::as_u64)
            .collect();
            let reference: Vec<u64> = all_versions
                .iter()
                .copied()
                .filter(|version| *version >= floor && *version <= candidate)
                .collect();
            assert_eq!(
                bounded, reference,
                "interval [{floor}, {candidate}] mismatch"
            );
        }
        let visible = floor;
        let expected_boundary = all_versions
            .iter()
            .copied()
            .filter(|version| *version <= visible)
            .max();
        let actual_boundary = branch_checkpoint_flush_boundary(
            state.owned_levels(),
            state.inherited_layers(),
            CommitVersion::new(visible),
            &|_| true,
        )
        .map(CommitVersion::as_u64);
        assert_eq!(
            actual_boundary, expected_boundary,
            "boundary at visible={visible}"
        );
    }
}

fn install_l0_table_for_test(
    state: &mut BranchLocalState,
    branch: BranchId,
    identity: &str,
    rows: &[StorageRow],
) {
    let identity = TableIdentity::new(identity).expect("table identity");
    let bytes = table_bytes(identity.clone(), rows);
    let reader =
        ImmutableTableReader::open_bytes(identity.clone(), bytes, TableReaderConfig::default())
            .expect("table reader");
    let descriptor =
        BranchTableDescriptor::new(identity, reader.facts().clone(), BranchLevel::ZERO)
            .expect("table descriptor");
    let extras =
        crate::table::TableSummaryExtras::from_rows(reader.rows()).expect("table summary extras");
    let table = BranchOwnedTable::new(branch, descriptor, reader, extras).expect("branch table");
    state.install_l0_table(table).expect("install table");
}

fn rows_for_versions(
    branch: BranchId,
    versions: std::ops::RangeInclusive<u64>,
    key_prefix: &'static [u8],
) -> Vec<StorageRow> {
    versions
        .map(|version| {
            let mut key = key_prefix.to_vec();
            key.push(u8::try_from(version).expect("test version fits in u8"));
            StorageRow::put(
                physical_key_from_vec(branch, key),
                CommitVersion::new(version),
                Timestamp::from_micros(version * 100),
                Timestamp::EPOCH,
                format!("value-{version}").into_bytes(),
            )
        })
        .collect()
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

fn physical_key(branch: BranchId, user_key: &'static [u8]) -> PhysicalKey {
    physical_key_from_vec(branch, user_key.to_vec())
}

fn physical_key_from_vec(branch: BranchId, user_key: Vec<u8>) -> PhysicalKey {
    PhysicalKey::new(
        branch,
        "flush-watermark",
        StorageSpaceId::engine(0x35).expect("space"),
        user_key,
    )
    .expect("physical key")
}

fn flush_request_for_test(branch: BranchId, suffix: &str) -> FlushFrozenRequest {
    FlushFrozenRequest::new(
        branch,
        None,
        FlushTableIdentitySeed::new(format!("watermark-flush-{suffix}")).expect("seed"),
        FlushTableObjectId::new(format!("watermark-object-{suffix}")).expect("object"),
    )
    .expect("flush request")
}

fn table_manifest_facts(reader: &ImmutableTableReader) -> TableManifestTableFacts {
    let (timestamp_min, timestamp_max) = timestamp_bounds(reader.rows());
    TableManifestTableFacts::new(
        reader.facts().byte_count(),
        reader.facts().row_count(),
        reader.facts().data_block_count(),
        reader.facts().commit_range().min(),
        reader.facts().commit_range().max(),
        timestamp_min,
        timestamp_max,
    )
    .expect("table manifest facts")
}

fn table_manifest_bounds(rows: &[TableRow]) -> TableManifestTableBounds {
    let first = rows.first().expect("non-empty table rows");
    let mut physical_first = TablePhysicalKeyBytes::from_row(first.row());
    let mut physical_last = physical_first.clone();
    let mut internal_first = first.key().clone();
    let mut internal_last = internal_first.clone();
    for row in rows.iter().skip(1) {
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
}

fn timestamp_bounds(rows: &[TableRow]) -> (Option<Timestamp>, Option<Timestamp>) {
    let mut timestamps = rows.iter().map(TableRow::commit_timestamp);
    let Some(first) = timestamps.next() else {
        return (None, None);
    };
    let (min, max) = timestamps.fold((first, first), |(min, max), timestamp| {
        (min.min(timestamp), max.max(timestamp))
    });
    (Some(min), Some(max))
}

fn table_bytes(identity: TableIdentity, rows: &[StorageRow]) -> Vec<u8> {
    let mut table_rows = rows.iter().cloned().map(TableRow::new).collect::<Vec<_>>();
    sort_table_rows_by_key(&mut table_rows);
    ImmutableTableBuilder::new(crate::table::TableBuilderConfig::default())
        .expect("table builder")
        .build_from_rows(identity, &table_rows)
        .expect("build table")
        .into_bytes()
}

fn wal_record(
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
    let timeline = CommitTimelineRows::from_entry(
        CommitTimelineEntry::new(branch, commit_version, timestamp).expect("timeline"),
    )
    .expect("timeline rows")
    .into_rows();
    WalRecord::new(
        commit_version,
        branch,
        timestamp,
        WalCommitPayload::new([vec![row], timeline.to_vec()].concat()).expect("payload"),
    )
    .expect("wal record")
}
