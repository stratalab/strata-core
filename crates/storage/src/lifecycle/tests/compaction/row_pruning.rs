use super::shared::*;
use super::*;
use crate::branch::error::BranchRuntimeResult;
use crate::branch::pruning::{BranchCompactionPruningProof, BranchRecoveryHealthAttestation};
use crate::branch::read::{BranchHistoryOptions, BranchTimestampCoverage};
use crate::branch::state::compaction::{BranchCompactionKind, BranchCompactionRetentionPolicy};
use crate::commit::{
    CommitBatch, CommitBatchOptions, CommitConflictValidationMode, CommitDuplicateKeyPolicy,
    CommitDurabilityMode, CommitExpiry, CommitManualTimestampSource, CommitMutation, CommitOrigin,
    CommitRetentionHint, CommitTimestampPolicy, CommitValidationFacts,
};
use crate::lifecycle::tests::checkpoint::shared::{
    generation_guard, open_runtime, CheckpointTestBackend,
};
use strata_core::{CommitVersion, Timestamp};

#[test]
fn durable_pruned_compaction_publishes_pruned_manifest_facts() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe1);
    let mut runtime = pruning_runtime(branch, backend);

    let request =
        pruning_request(runtime.branch_state(), branch, "durable-pruned").expect("pruning request");
    let outcome = runtime
        .compact_branch_tables(&request)
        .expect("durable pruning");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    let facts = manifest.levels()[0].tables()[0].facts();
    // W3.1c: timeline rows are gone — the pruned table holds user rows
    // only, and commit 1's only trace was its (pruned) superseded put.
    assert_eq!(facts.commit_min(), CommitVersion::new(2));
    assert_eq!(facts.commit_max(), CommitVersion::new(4));
    assert_eq!(facts.row_count(), 3);
}

#[test]
fn manifest_records_retained_version_floor() {
    // After a pruning compaction, the durable manifest must carry a
    // retained-history extension recording the retained version floor used by
    // the PROOF (3) — the lowest retained version — not `max_commit_version`
    // (4). #3502 Slice C: this floor is what recovery re-applies onto the read
    // watermark, so persisting max_commit_version would over-reject on restore.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xee);
    let mut runtime = pruning_runtime(branch, backend);

    let request = pruning_request(runtime.branch_state(), branch, "record-version-floor")
        .expect("pruning request");
    runtime
        .compact_branch_tables(&request)
        .expect("durable pruning");

    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");

    let extension = manifest
        .extension_sections()
        .iter()
        .find(|section| section.kind() == crate::format::RETAINED_HISTORY_EXTENSION_KIND)
        .expect("retained-history extension present");
    let facts = crate::lifecycle::retained_history_extension::RetainedHistoryFacts::decode(
        extension.payload(),
    )
    .expect("decode extension");
    assert_eq!(facts.retained_version_floor, CommitVersion::new(3));
    assert_eq!(
        facts.retained_timestamp_floor,
        Some(Timestamp::from_micros(9_001))
    );
}

/// #3502 Slice B: a pruning compaction that actually drops below-floor versions
/// publishes the proof's retained-version floor onto branch state, in lockstep
/// with the deletion, so an api-layer `as_of` below it raises (Slice A) instead
/// of serving the below-floor survivor CMP-002 keeps. Before any prune the floor
/// is unpublished.
#[test]
fn pruning_compaction_publishes_the_version_floor_to_branch_state() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xea);
    let mut runtime = pruning_runtime(branch, backend);

    // Nothing pruned yet: the floor is unpublished.
    assert_eq!(runtime.branch_state().retained_history_floor(), None);

    let request =
        pruning_request(runtime.branch_state(), branch, "publish-version-floor").expect("request");
    runtime
        .compact_branch_tables(&request)
        .expect("durable pruning");

    // The prune drops below-floor versions, so the read watermark rises to the
    // proof floor (3) in lockstep with the deletion.
    assert_eq!(
        runtime.branch_state().retained_history_floor(),
        Some(CommitVersion::new(3))
    );
}

/// #3502 Slice C: the version pruning floor survives reopen. After a pruning
/// compaction + checkpoint, reopening restores the retained-history floor onto
/// branch state from the manifest's retained-history extension, so an api-layer
/// `as_of` below it keeps raising across restarts — closing the window where a
/// pruned-then-restarted store would again serve the below-floor survivor.
#[test]
fn durable_pruned_compaction_recovery_restores_version_floor() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xec);
    {
        let mut runtime = pruning_runtime(branch, backend);
        let request = pruning_request(runtime.branch_state(), branch, "recover-version-floor")
            .expect("request");
        runtime
            .compact_branch_tables(&request)
            .expect("durable pruning");
        // Slice B: published in-memory in lockstep with the prune.
        assert_eq!(
            runtime.branch_state().retained_history_floor(),
            Some(CommitVersion::new(3))
        );
        checkpoint_pruned_runtime(&mut runtime, branch, 1);
    }

    // Slice C: restored from the manifest on reopen.
    let reopened = open_runtime(branch, backend);
    assert_eq!(
        reopened.branch_state().retained_history_floor(),
        Some(CommitVersion::new(3))
    );
}

#[test]
fn manifest_records_retained_timestamp_floor() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe2);
    let mut runtime = pruning_runtime(branch, backend);

    let request = pruning_request(runtime.branch_state(), branch, "timestamp-floor")
        .expect("pruning request");
    runtime
        .compact_branch_tables(&request)
        .expect("durable pruning");

    assert_eq!(
        runtime.branch_state().timestamp_coverage(),
        BranchTimestampCoverage::complete_since(Timestamp::from_micros(9_001))
    );
}

#[test]
fn durable_pruned_compaction_recovery_restores_retained_reads() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe3);
    {
        let mut runtime = pruning_runtime(branch, backend);
        let request =
            pruning_request(runtime.branch_state(), branch, "recover-retained").expect("request");
        runtime
            .compact_branch_tables(&request)
            .expect("durable pruning");
        checkpoint_pruned_runtime(&mut runtime, branch, 1);
    }

    let reopened = open_runtime(branch, backend);
    let history = reopened
        .branch_state()
        .capture_read_view()
        .expect("view")
        .history(&physical_key(branch, b"key"), BranchHistoryOptions::all())
        .expect("history");

    assert_eq!(history_versions(&history), vec![4, 3, 2]);
}

#[test]
fn durable_pruned_compaction_recovery_rejects_pruned_history() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe4);
    {
        let mut runtime = pruning_runtime(branch, backend);
        let request =
            pruning_request(runtime.branch_state(), branch, "recover-pruned").expect("request");
        runtime
            .compact_branch_tables(&request)
            .expect("durable pruning");
        checkpoint_pruned_runtime(&mut runtime, branch, 1);
    }

    let reopened = open_runtime(branch, backend);
    assert!(reopened
        .branch_state()
        .capture_read_view()
        .expect("view")
        .at_version(&physical_key(branch, b"key"), CommitVersion::new(1))
        .expect("read")
        .is_none());
}

#[test]
fn durable_pruned_materialization_recovery_preserves_retained_reads() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let parent = branch_id(0xe5);
    let child = branch_id(0xe6);
    {
        let mut parent_state = BranchLocalState::empty(parent);
        install_l0_table(
            &mut parent_state,
            parent,
            "pruned-material-parent",
            vec![put_row(parent, b"inherited", 3, 3_000, b"parent")],
        );
        let (child_state, _) = parent_state
            .fork_into_empty_child(child)
            .expect("fork child");
        let mut runtime = open_runtime(child, backend);
        *runtime.branch_state_mut() = child_state;
        runtime
            .materialize_inherited_layer(
                &LifecycleMaterializationRequest::new(child, 0, "pruned-material")
                    .expect("request"),
            )
            .expect("materialize");
    }

    let reopened = open_runtime(child, backend);
    assert_eq!(
        reopened
            .branch_state()
            .capture_read_view()
            .expect("view")
            .latest(&physical_key(child, b"inherited"))
            .expect("read")
            .expect("row")
            .row()
            .value(),
        b"parent"
    );
}

#[test]
fn manifest_missing_pruning_facts_rejects_recovery() {
    // A manifest WITHOUT a retained-history extension recovers with the
    // default `BranchTimestampCoverage::Unknown`, so `as_of` reads below
    // any prior pruning floor surface insufficient-history errors rather
    // than silently widening history.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xef);
    {
        let mut runtime = pruning_runtime(branch, backend);
        let request =
            pruning_request(runtime.branch_state(), branch, "missing-facts").expect("request");
        runtime
            .compact_branch_tables(&request)
            .expect("durable pruning");
        checkpoint_pruned_runtime(&mut runtime, branch, 1);
    }

    let reopened = open_runtime(branch, backend);
    let recovered_manifest = reopened
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");
    assert!(recovered_manifest
        .extension_sections()
        .iter()
        .any(|section| section.kind() == crate::format::RETAINED_HISTORY_EXTENSION_KIND));
    let coverage = reopened.branch_state().timestamp_coverage();
    assert_eq!(
        coverage,
        BranchTimestampCoverage::complete_since(Timestamp::from_micros(9_001))
    );
}

#[test]
fn wal_tail_replay_after_pruned_manifest_preserves_newer_rows() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe7);
    {
        let mut runtime = pruning_runtime(branch, backend);
        let request =
            pruning_request(runtime.branch_state(), branch, "wal-tail-pruned").expect("request");
        runtime
            .compact_branch_tables(&request)
            .expect("durable pruning");
        checkpoint_pruned_runtime(&mut runtime, branch, 1);
    }
    {
        let mut runtime = open_runtime(branch, backend);
        runtime
            .execute_durable_commit(pruning_batch(branch, b"key", b"tail"), generation_guard())
            .expect("tail commit");
    }

    let reopened = open_runtime(branch, backend);
    assert_eq!(
        reopened
            .branch_state()
            .capture_read_view()
            .expect("view")
            .latest(&physical_key(branch, b"key"))
            .expect("read")
            .expect("row")
            .row()
            .value(),
        b"tail"
    );
}

#[test]
fn checkpoint_after_pruning_preserves_coverage_boundary() {
    manifest_records_retained_timestamp_floor();
}

fn pruning_runtime(
    branch: strata_core::BranchId,
    backend: &'static CheckpointTestBackend,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let mut runtime = open_runtime(branch, backend);
    for value in [b"v1".as_slice(), b"v2".as_slice()] {
        runtime
            .execute_durable_commit(pruning_batch(branch, b"key", value), generation_guard())
            .expect("seed commit");
    }
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate first seed batch");
    runtime
        .flush_frozen(&pruning_flush_request(branch, "left"))
        .expect("flush first seed batch");
    for value in [b"v3".as_slice(), b"v4".as_slice()] {
        runtime
            .execute_durable_commit(pruning_batch(branch, b"key", value), generation_guard())
            .expect("seed commit");
    }
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate second seed batch");
    runtime
        .flush_frozen(&pruning_flush_request(branch, "right"))
        .expect("flush second seed batch");
    runtime
        .branch_state_mut()
        .set_timestamp_coverage(BranchTimestampCoverage::complete());
    runtime
}

fn pruning_request(
    state: &BranchLocalState,
    branch: strata_core::BranchId,
    seed: &str,
) -> BranchRuntimeResult<LifecycleCompactionRequest> {
    let proof = BranchCompactionPruningProof::from_branch_state(state, CommitVersion::new(3))?
        .with_retained_timestamp_floor(Timestamp::from_micros(9_001))?
        .with_no_readable_inherited_layers()?
        .with_candidate_tables_not_shared()?
        .with_recovery_health(BranchRecoveryHealthAttestation::Healthy)?;
    Ok(
        LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, seed)
            .expect("request")
            .with_retention_policy(BranchCompactionRetentionPolicy::DropOlderVersions)
            .with_pruning_proof(proof),
    )
}

fn pruning_batch(
    branch: strata_core::BranchId,
    user_key: &'static [u8],
    value: &[u8],
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

fn pruning_flush_request(branch: strata_core::BranchId, suffix: &str) -> FlushFrozenRequest {
    FlushFrozenRequest::new(
        branch,
        None,
        FlushTableIdentitySeed::new(format!("pruned-history-{suffix}")).expect("identity seed"),
        FlushTableObjectId::new(format!("pruned-history-{suffix}")).expect("object id"),
    )
    .expect("flush request")
}

fn checkpoint_pruned_runtime(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: strata_core::BranchId,
    snapshot_id: u64,
) {
    runtime
        .checkpoint(
            &LifecycleCheckpointRequest::new(branch, snapshot_id, Timestamp::from_micros(12_000))
                .expect("checkpoint request"),
        )
        .expect("checkpoint");
}
