use super::*;
use crate::backend::memory::MemoryBackend;
use crate::backend::{
    Backend, BackendCapabilities, BackendCapability, BackendError, BackendErrorKind,
    BackendMetadata, BackendRange, BackendResult, BASIC_OBJECT_BACKEND_CAPABILITIES,
};
use crate::format::DatabaseManifest;
use crate::layout::ObjectLayout;
#[cfg(feature = "perf-trace")]
use crate::lifecycle::retention::reject_implicit_snapshot_floor_advancement;
use crate::lifecycle::retention::{
    retention_outcome_for_delegated_families, retention_outcome_for_scope, snapshot_mode_decision,
    table_quarantine_candidate,
};
use crate::object::{ObjectName, ObjectPrefix};
use crate::service::{
    reconcilable_orphan, superseded_snapshot, SnapshotPruneMode, SnapshotService,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use strata_core::{BranchId, CommitVersion, Timestamp};

use super::checkpoint::shared::{
    branch_id as durable_branch_id, durable_batch, generation_guard, open_runtime, physical_key,
    CheckpointBackendEvent, CheckpointTestBackend,
};
use crate::commit::CommitManualTimestampSource;
use crate::lifecycle::durable::LifecycleDurableLocalRuntime;

const DATABASE_ID: [u8; 16] = [0x4d; 16];

#[test]
fn retention_request_accepts_zero_snapshot_retain_as_clamped_policy() {
    let request = LifecycleRetentionRequest::snapshot_pruning(0);

    assert_eq!(request.retain_newest_snapshots(), 0);
    assert_eq!(request.effective_retain_newest_snapshots(), 1);
}

#[test]
fn retention_request_rejects_empty_scope_when_required() {
    for scope in [
        LifecycleRetentionScope::Global,
        LifecycleRetentionScope::SnapshotObjects,
        LifecycleRetentionScope::WalObjects,
        LifecycleRetentionScope::QuarantineObjects,
        LifecycleRetentionScope::TableObjects {
            branch_id: BranchId::from_bytes([0x11; 16]),
        },
    ] {
        let request = LifecycleRetentionRequest::new(scope, 1);

        assert_eq!(request.scope(), scope);
    }
}

#[test]
fn retention_request_rejects_product_vocabulary_scope() {
    let debug = format!("{:?}", LifecycleRetentionRequest::global(2));

    for forbidden in ["Database::open", "VersionedValue", "StrataHub", "EntityRef"] {
        assert!(!debug.contains(forbidden));
    }
}

#[cfg(feature = "perf-trace")]
#[test]
fn implicit_snapshot_floor_advancement_is_rejected_and_counted() {
    let _capture = crate::observability::perf_trace::begin_test_capture();

    let error = reject_implicit_snapshot_floor_advancement(None, CommitVersion::new(7))
        .expect_err("implicit floor advancement");
    let perf = crate::observability::perf_trace::snapshot();

    assert_eq!(error.code(), "failed_precondition.lifecycle.retention");
    assert_eq!(perf.lifecycle_snapshot_floor_implicit_rejections(), 1);
    assert_eq!(perf.lifecycle_snapshot_floor_advancements(), 0);
    assert_eq!(perf.lifecycle_snapshot_pruning_with_proof(), 0);
}

#[test]
fn snapshot_pruning_request_rejects_zero_live_snapshot_id() {
    let proof = LifecycleRetentionProof::new(
        LifecycleRetentionProofStatus::Complete,
        RecoveryHealth::Healthy,
        Some(0),
        Some(CommitVersion::new(7)),
        Some(CommitVersion::new(7)),
        None,
    );

    let error = LifecycleSnapshotPruningRequest::new(proof, 1).expect_err("zero live snapshot");

    assert_eq!(error.code(), "invalid_argument.lifecycle.config");
}

#[test]
fn retention_outcome_reports_retained_pruned_skipped_and_delegated_counts() {
    let snapshot = ObjectLayout::snapshot(1).expect("snapshot");
    let retained = ObjectName::new("tables/branch/retained-table").expect("table object");
    let proof = complete_retention_proof(1, 7);
    let outcome = LifecycleRetentionOutcome::from_decisions(
        proof,
        vec![
            LifecycleRetentionDecisionRecord::snapshot(
                snapshot,
                RetentionDecision::PruneCandidate,
                LifecycleRetentionDecisionReason::SnapshotPruneCandidate,
            ),
            LifecycleRetentionDecisionRecord::table(
                retained,
                RetentionDecision::Retain,
                LifecycleRetentionDecisionReason::ReachableTable,
            ),
            LifecycleRetentionDecisionRecord::delegated(
                LifecycleRetentionObjectFamily::Wal,
                LifecycleRetentionDecisionReason::DelegatedToWalTruncation,
            ),
        ],
        64,
    )
    .expect("outcome");

    assert_eq!(outcome.objects_pruned(), 1);
    assert_eq!(outcome.objects_retained(), 1);
    assert_eq!(outcome.objects_skipped(), 1);
    assert_eq!(outcome.reclaimed_bytes(), 64);
    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.recovery_health(), None);
}

#[test]
fn retention_outcome_reports_affected_object_names() {
    let snapshot = ObjectLayout::snapshot(4).expect("snapshot");
    let object = snapshot.to_string();
    let outcome = LifecycleRetentionOutcome::from_decisions(
        complete_retention_proof(4, 7),
        vec![LifecycleRetentionDecisionRecord::snapshot(
            snapshot,
            RetentionDecision::PruneCandidate,
            LifecycleRetentionDecisionReason::SnapshotPruneCandidate,
        )],
        0,
    )
    .expect("outcome")
    .maintenance_outcome();

    assert_eq!(outcome.affected_object_names(), [object]);
    assert_eq!(outcome.affected_objects(), 1);
}

#[test]
fn retention_outcome_reports_reclaimed_bytes_when_known() {
    let outcome =
        LifecycleRetentionOutcome::from_decisions(complete_retention_proof(1, 7), Vec::new(), 512)
            .expect("outcome")
            .maintenance_outcome();

    assert_eq!(outcome.bytes_reclaimed(), 512);
}

#[test]
fn retention_outcome_debug_uses_storage_vocabulary() {
    let debug = format!(
        "{:?}",
        LifecycleRetentionOutcome::from_decisions(
            complete_retention_proof(1, 7),
            vec![LifecycleRetentionDecisionRecord::delegated(
                LifecycleRetentionObjectFamily::Wal,
                LifecycleRetentionDecisionReason::DelegatedToWalTruncation,
            )],
            0,
        )
        .expect("outcome")
    );

    assert!(debug.contains("Wal"));
    for forbidden in ["Database::open", "VersionedValue", "StrataHub", "EntityRef"] {
        assert!(!debug.contains(forbidden));
    }
}

#[test]
fn retention_outcome_converts_incomplete_proof_to_deferred_maintenance() {
    let outcome = LifecycleRetentionOutcome::from_decisions(
        incomplete_retention_proof("manifest_snapshot"),
        Vec::new(),
        0,
    )
    .expect("outcome")
    .maintenance_outcome();

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        outcome.reason_class(),
        Some(MaintenanceOutcomeReasonClass::Deferred)
    );
    assert_eq!(outcome.stats().retention_blocks(), 1);
}

#[test]
fn snapshot_pruning_outcome_converts_delete_failure_to_health_debt() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    backend.fail_delete_on_call(1);
    let outcome = snapshot_pruning(backend, 3, 1).maintenance_outcome();

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(outcome.state_changes(), 1);
    assert_eq!(outcome.stats().recovery_faults(), 1);
}

#[test]
fn retention_proof_incomplete_without_manifest_snapshot_when_snapshots_exist() {
    let request = LifecycleRetentionRequest::snapshot_pruning(2);
    let proof = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 1);

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("manifest_snapshot"));
}

#[test]
fn retention_proof_incomplete_without_manifest_snapshot_even_when_listing_empty() {
    let request = LifecycleRetentionRequest::snapshot_pruning(2);
    let proof = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 0);

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("manifest_snapshot"));
}

#[test]
fn retention_proof_incomplete_without_branch_reachability_for_tables() {
    let request = LifecycleRetentionRequest::new(
        LifecycleRetentionScope::TableObjects {
            branch_id: BranchId::from_bytes([0x7b; 16]),
        },
        1,
    );
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 0);

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("table_reachability"));
}

#[test]
fn retention_proof_complete_with_manifest_snapshot_and_healthy_recovery() {
    let request = LifecycleRetentionRequest::snapshot_pruning(2);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(2, 11)),
        &RecoveryHealth::Healthy,
        2,
    );

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Complete);
    assert_eq!(proof.live_snapshot_id(), Some(2));
    assert_eq!(proof.snapshot_watermark(), Some(CommitVersion::new(11)));
    assert_eq!(proof.missing_fact(), None);
}

#[test]
fn retention_proof_incomplete_without_quarantine_inventory_for_purge_scope() {
    let request = LifecycleRetentionRequest::new(LifecycleRetentionScope::QuarantineObjects, 1);
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 0);

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("quarantine_inventory"));
}

#[test]
fn retention_proof_blocks_on_policy_downgrade_recovery_health() {
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(1, 7)),
        &policy_downgrade_health(),
        1,
    );

    assert_eq!(
        proof.status(),
        LifecycleRetentionProofStatus::BlockedByRecoveryHealth
    );
    assert_eq!(proof.missing_fact(), Some("recovery_health"));
}

#[test]
fn retention_proof_allows_policy_downgrade_for_telemetry_only_scope() {
    let request = LifecycleRetentionRequest::new(LifecycleRetentionScope::WalObjects, 1);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(1, 7)),
        &policy_downgrade_health(),
        1,
    );

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("wal_retention_proof"));
}

#[test]
fn retention_proof_allows_telemetry_degraded_recovery_when_unrelated() {
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(1, 7)),
        &telemetry_degraded_health(),
        1,
    );

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Complete);
}

#[test]
fn retention_proof_records_missing_fact_family() {
    for (scope, expected) in [
        (
            LifecycleRetentionScope::SnapshotObjects,
            "manifest_snapshot",
        ),
        (LifecycleRetentionScope::WalObjects, "wal_retention_proof"),
        (
            LifecycleRetentionScope::QuarantineObjects,
            "quarantine_inventory",
        ),
        (
            LifecycleRetentionScope::TableObjects {
                branch_id: BranchId::from_bytes([0x7c; 16]),
            },
            "table_reachability",
        ),
    ] {
        let request = LifecycleRetentionRequest::new(scope, 1);
        let proof = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 0);

        assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
        assert_eq!(proof.missing_fact(), Some(expected));
    }
}

#[test]
fn retention_proof_does_not_upgrade_runtime_reachability_to_durable_truth() {
    let request = LifecycleRetentionRequest::new(
        LifecycleRetentionScope::TableObjects {
            branch_id: BranchId::from_bytes([0x7d; 16]),
        },
        1,
    );
    let proof = build_retention_proof(
        &request,
        Some(&manifest(1, 7)),
        &telemetry_degraded_health(),
        0,
    );

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("table_reachability"));
}

#[test]
fn retention_proof_is_deterministic_for_shuffled_input_facts() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3, 1, 2]));
    let snapshots = SnapshotService::new(backend)
        .list_snapshots()
        .expect("snapshots");
    let mut reversed = snapshots.clone();
    reversed.reverse();
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(3, 7)),
        &RecoveryHealth::Healthy,
        snapshots.len(),
    );

    let ordered =
        retention_outcome_for_scope(&request, proof.clone(), &snapshots).expect("ordered");
    let shuffled = retention_outcome_for_scope(&request, proof, &reversed).expect("shuffled");

    assert_eq!(ordered.decisions(), shuffled.decisions());
}

#[test]
fn snapshot_pruning_request_rejects_complete_proof_without_live_snapshot() {
    let proof = LifecycleRetentionProof::new(
        LifecycleRetentionProofStatus::Complete,
        RecoveryHealth::Healthy,
        None,
        Some(CommitVersion::new(7)),
        Some(CommitVersion::new(7)),
        None,
    );

    let error = LifecycleSnapshotPruningRequest::new(proof, 1).expect_err("missing live snapshot");

    assert_eq!(error.code(), "failed_precondition.lifecycle.retention");
}

#[test]
fn incomplete_snapshot_pruning_proof_defers_before_backend_access() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1]));
    let request = LifecycleRetentionRequest::snapshot_pruning(2);
    let proof = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 1);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let outcome = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome");

    assert!(outcome.deferred_incomplete_proof());
    assert_eq!(backend.list_calls(), 0);
    assert_eq!(backend.delete_calls(), 0);
    assert!(outcome.recovery_health().is_some());
}

#[test]
fn retention_proof_blocks_data_loss_before_backend_access() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2]));
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingSnapshotObject, "missing").expect("fault"),
        ],
    )
    .expect("health");
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &health, 0);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let outcome = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome");

    assert!(outcome.blocked_by_recovery_health());
    assert_eq!(backend.list_calls(), 0);
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn snapshot_pruning_retains_live_snapshot_outside_newest_window() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3, 4]));
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 4);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let outcome = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome");

    assert!(outcome.completed());
    assert_eq!(snapshot_ids(outcome.deleted()), [2, 3]);
    assert_eq!(snapshot_ids(outcome.protected()), [1, 4]);
    assert_eq!(backend.remaining_snapshot_ids(), [1, 4]);
}

#[test]
fn snapshot_pruning_retains_live_manifest_snapshot() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let outcome = snapshot_pruning(backend, 2, 1);

    assert_eq!(snapshot_ids(outcome.deleted()), [1]);
    assert!(snapshot_ids(outcome.protected()).contains(&2));
    assert_eq!(backend.remaining_snapshot_ids(), [2, 3]);
}

#[test]
fn snapshot_pruning_retains_configured_newest_snapshots() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3, 4]));
    let outcome = snapshot_pruning(backend, 4, 2);

    assert_eq!(snapshot_ids(outcome.deleted()), [1, 2]);
    assert_eq!(snapshot_ids(outcome.protected()), [3, 4]);
}

#[test]
fn snapshot_pruning_deletes_old_non_live_snapshots() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let outcome = snapshot_pruning(backend, 3, 1);

    assert!(outcome.completed());
    assert_eq!(snapshot_ids(outcome.deleted()), [1, 2]);
    assert_eq!(backend.delete_calls(), 2);
}

#[cfg(feature = "perf-trace")]
#[test]
fn snapshot_pruning_with_proof_records_counters_without_floor_advancement() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));

    let outcome = snapshot_pruning(backend, 3, 1);
    let perf = crate::observability::perf_trace::snapshot();

    assert!(outcome.completed());
    assert_eq!(snapshot_ids(outcome.deleted()), [1, 2]);
    assert_eq!(snapshot_ids(outcome.protected()), [3]);
    assert_eq!(perf.lifecycle_snapshot_pruning_with_proof(), 1);
    assert_eq!(perf.lifecycle_snapshot_pruning_deleted(), 2);
    assert_eq!(perf.lifecycle_snapshot_pruning_protected(), 1);
    assert_eq!(perf.lifecycle_snapshot_pruning_failed(), 0);
    assert_eq!(perf.lifecycle_snapshot_floor_advancements(), 0);
    assert_eq!(perf.lifecycle_snapshot_floor_implicit_rejections(), 0);
}

#[cfg(feature = "perf-trace")]
#[test]
fn snapshot_pruning_with_proof_records_failed_delete_counter() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    backend.fail_delete_on_call(1);

    let outcome = snapshot_pruning(backend, 3, 1);
    let perf = crate::observability::perf_trace::snapshot();

    assert!(outcome.completed_with_health_debt());
    assert_eq!(snapshot_ids(outcome.deleted()), [2]);
    assert_eq!(snapshot_ids(outcome.protected()), [3]);
    assert_eq!(outcome.failed().len(), 1);
    assert_eq!(perf.lifecycle_snapshot_pruning_with_proof(), 1);
    assert_eq!(perf.lifecycle_snapshot_pruning_deleted(), 1);
    assert_eq!(perf.lifecycle_snapshot_pruning_protected(), 1);
    assert_eq!(perf.lifecycle_snapshot_pruning_failed(), 1);
    assert_eq!(perf.lifecycle_snapshot_floor_advancements(), 0);
    assert_eq!(perf.lifecycle_snapshot_floor_implicit_rejections(), 0);
}

#[cfg(feature = "perf-trace")]
#[test]
fn generated_snapshot_pruning_proof_sweep_preserves_retained_snapshots() {
    let _capture = crate::observability::perf_trace::begin_test_capture();

    for (case, live_snapshot_id, snapshot_watermark, retain_newest, snapshots) in [
        ("stale-watermark", 3, 1, 1, [1, 2, 3, 4, 5]),
        ("exact-watermark", 3, 7, 2, [1, 2, 3, 4, 5]),
        ("future-watermark", 4, 99, 3, [1, 2, 3, 4, 5]),
    ] {
        crate::observability::perf_trace::reset();
        let backend: &'static RetentionBackend =
            crate::testkit::leak_static(RetentionBackend::with_snapshots(snapshots));
        let request = LifecycleRetentionRequest::snapshot_pruning(retain_newest);
        let proof = build_retention_proof_from_facts(
            &request,
            Some(live_snapshot_id),
            Some(snapshot_watermark),
            Some(CommitVersion::new(snapshot_watermark)),
            &RecoveryHealth::Healthy,
            snapshots.len(),
        );
        let pruning =
            LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
                .expect(case);

        let outcome = prune_snapshots_with_proof(
            &SnapshotService::new(backend),
            &pruning,
            Some(&std::collections::BTreeSet::new()),
        )
        .expect(case);
        let protected = snapshot_ids(outcome.protected());
        let deleted = snapshot_ids(outcome.deleted());
        let perf = crate::observability::perf_trace::snapshot();

        assert!(
            outcome.completed(),
            "proof-backed pruning should complete for {case}"
        );
        assert!(
            protected.contains(&live_snapshot_id),
            "live snapshot must be protected for {case}"
        );
        assert!(
            !deleted.contains(&live_snapshot_id),
            "live snapshot must not be deleted for {case}"
        );
        assert_eq!(outcome.failed().len(), 0, "{case}");
        assert_eq!(perf.lifecycle_snapshot_pruning_with_proof(), 1, "{case}");
        assert_eq!(
            perf.lifecycle_snapshot_pruning_deleted(),
            deleted.len() as u64,
            "{case}"
        );
        assert_eq!(
            perf.lifecycle_snapshot_pruning_protected(),
            protected.len() as u64,
            "{case}"
        );
        assert_eq!(perf.lifecycle_snapshot_pruning_failed(), 0, "{case}");
        assert_eq!(perf.lifecycle_snapshot_floor_advancements(), 0, "{case}");
        assert_eq!(
            perf.lifecycle_snapshot_floor_implicit_rejections(),
            0,
            "{case}"
        );
    }
}

#[test]
fn snapshot_pruning_noops_when_under_retain_count() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2]));
    let outcome = snapshot_pruning(backend, 2, 3);

    assert!(outcome.completed_noop());
    assert_eq!(outcome.deleted(), []);
    assert_eq!(snapshot_ids(outcome.protected()), [1, 2]);
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn snapshot_pruning_is_idempotent_after_success() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let first = snapshot_pruning(backend, 3, 1);
    let second = snapshot_pruning(backend, 3, 1);

    assert_eq!(snapshot_ids(first.deleted()), [1, 2]);
    assert!(second.completed_noop());
    assert_eq!(second.deleted(), []);
    assert_eq!(backend.remaining_snapshot_ids(), [3]);
}

#[test]
fn snapshot_pruning_clamps_zero_retain_count_to_one() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let request = LifecycleRetentionRequest::snapshot_pruning(0);
    let proof = build_retention_proof(&request, Some(&manifest(3, 7)), &RecoveryHealth::Healthy, 3);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let outcome = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome");

    assert_eq!(snapshot_ids(outcome.deleted()), [1, 2]);
    assert_eq!(snapshot_ids(outcome.protected()), [3]);
    assert_eq!(backend.remaining_snapshot_ids(), [3]);
}

#[test]
fn snapshot_pruning_malformed_listed_snapshot_fails_closed() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2]));
    backend.insert_object(
        ObjectName::new("snapshots/not-a-valid-id").expect("malformed snapshot family object"),
        b"ambiguous".to_vec(),
    );
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(2, 7)), &RecoveryHealth::Healthy, 2);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let error = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect_err("malformed listed snapshot fails closed");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn snapshot_pruning_does_not_mutate_manifest_snapshot_facts() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(3, 7)), &RecoveryHealth::Healthy, 3);
    let pruning =
        LifecycleSnapshotPruningRequest::new(proof.clone(), request.retain_newest_snapshots())
            .expect("pruning request");

    let outcome = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome");

    assert!(outcome.completed());
    assert_eq!(pruning.proof(), &proof);
    assert_eq!(pruning.live_snapshot_id(), Some(3));
}

#[test]
fn snapshot_pruning_does_not_create_wal_retention_proof() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let outcome = snapshot_pruning(backend, 3, 1).maintenance_outcome();

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::SnapshotPruning);
    assert!(outcome
        .affected_object_names()
        .iter()
        .all(|object| object.starts_with("snapshots/")));
}

#[test]
fn snapshot_pruning_object_candidate_mode_requires_declared_delete_capability() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2]));
    backend.omit_delete_capability();
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(2, 7)), &RecoveryHealth::Healthy, 2);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let error = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect_err("delete capability required");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert_eq!(backend.list_calls(), 0);
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn snapshot_pruning_delete_failure_records_health_debt_and_continues() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    backend.fail_delete_on_call(1);
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(3, 7)), &RecoveryHealth::Healthy, 3);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let outcome = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome");

    assert!(outcome.completed_with_health_debt());
    assert_eq!(snapshot_ids(outcome.deleted()), [2]);
    assert_eq!(snapshot_ids(outcome.protected()), [3]);
    assert_eq!(outcome.failed().len(), 1);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(backend.remaining_snapshot_ids(), [1, 3]);
}

#[test]
fn snapshot_pruning_emits_one_fault_per_failed_deletion() {
    // Five snapshots, all but the live one (5) need pruning. Inject
    // delete failures on calls 1 and 2 (snapshots 1 and 2 — the two
    // oldest pruning candidates). Snapshot 3 should still get deleted.
    // The health debt must surface exactly TWO RecoveryFault entries —
    // one per failed deletion — so `fault_count` reflects the real
    // backlog rather than collapsing to a single aggregated fault.
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3, 5]));
    backend.fail_delete_calls([1, 2]);
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(5, 7)), &RecoveryHealth::Healthy, 4);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let outcome = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome");

    assert!(outcome.completed_with_health_debt());
    assert_eq!(snapshot_ids(outcome.deleted()), [3]);
    assert_eq!(snapshot_ids(outcome.protected()), [5]);
    assert_eq!(outcome.failed().len(), 2);
    let health = outcome.recovery_health().expect("health debt");
    assert_eq!(
        health.fault_count(),
        2,
        "snapshot pruning must emit one RecoveryFault per failed deletion",
    );
}

#[test]
fn snapshot_pruning_list_failure_preserves_service_source_chain() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2]));
    backend.fail_listing();
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(2, 7)), &RecoveryHealth::Healthy, 2);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let error = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect_err("list failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn table_object_retention_classifies_quarantine_candidate_without_backend_delete() {
    let object = ObjectName::new("tables/branch/retired-table").expect("object");
    let proof = LifecycleRetentionProof::new(
        LifecycleRetentionProofStatus::Complete,
        RecoveryHealth::Healthy,
        Some(1),
        Some(CommitVersion::new(7)),
        Some(CommitVersion::new(7)),
        None,
    );
    let outcome = LifecycleRetentionOutcome::from_decisions(
        proof,
        vec![table_quarantine_candidate(object.clone())],
        0,
    )
    .expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.objects_pruned(), 0);
    assert_eq!(outcome.decisions()[0].object(), Some(&object));
    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::QuarantineCandidate
    );
}

#[test]
fn reachable_table_object_is_retained() {
    let object = ObjectName::new("tables/branch/live-table").expect("object");
    let outcome = LifecycleRetentionOutcome::from_decisions(
        complete_retention_proof(1, 7),
        vec![LifecycleRetentionDecisionRecord::table(
            object.clone(),
            RetentionDecision::Retain,
            LifecycleRetentionDecisionReason::ReachableTable,
        )],
        0,
    )
    .expect("outcome");

    assert_eq!(outcome.objects_retained(), 1);
    assert_eq!(outcome.decisions()[0].object(), Some(&object));
}

#[test]
fn replaced_unreachable_table_object_is_quarantine_candidate() {
    let object = ObjectName::new("tables/branch/replaced-table").expect("object");
    let decision = table_quarantine_candidate(object.clone());

    assert_eq!(decision.object(), Some(&object));
    assert_eq!(decision.family(), LifecycleRetentionObjectFamily::Table);
    assert_eq!(decision.decision(), RetentionDecision::QuarantineCandidate);
}

#[test]
fn table_object_with_incomplete_reachability_is_deferred_as_unsupported_scope() {
    // Table-object retention is unsupported regardless of proof
    // completeness — retention does not own branch reachability facts.
    // The outcome must surface `DeferredUnsupportedScope` even when the
    // proof itself happens to be incomplete; reporting
    // `DeferredIncompleteProof` here would mislead callers into
    // retrying once the proof completes, when in reality table
    // retention is permanently a deferred scope for retention.
    let request = LifecycleRetentionRequest::new(
        LifecycleRetentionScope::TableObjects {
            branch_id: BranchId::from_bytes([0x44; 16]),
        },
        1,
    );
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 0);
    let outcome = retention_outcome_for_scope(&request, proof, &[]).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::DeferredUnsupportedScope
    );
    assert_eq!(outcome.objects_pruned(), 0);
    assert_eq!(outcome.decisions(), &[]);
}

#[test]
fn table_object_retention_with_complete_proof_defers_without_silent_success() {
    let request = LifecycleRetentionRequest::new(
        LifecycleRetentionScope::TableObjects {
            branch_id: BranchId::from_bytes([0x45; 16]),
        },
        1,
    );
    let outcome = retention_outcome_for_scope(&request, complete_retention_proof(1, 7), &[])
        .expect("outcome");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::DeferredUnsupportedScope
    );
    assert_eq!(outcome.decisions(), &[]);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        maintenance.reason(),
        Some("retention scope not supported by generic path")
    );
}

#[test]
fn table_object_from_materialization_replacement_preserves_source_identity() {
    let object = ObjectName::new("tables/materialized/source-7/table-1").expect("object");
    let decision = table_quarantine_candidate(object.clone());

    assert_eq!(decision.object(), Some(&object));
    assert!(decision
        .object()
        .expect("object")
        .as_str()
        .contains("source-7"));
}

#[test]
fn table_retention_selection_records_reachability_dependency_reasons() {
    let owned = ObjectName::new("tables/branch/owned-live").expect("owned object");
    let inherited = ObjectName::new("tables/branch/inherited-live").expect("inherited object");
    let materialized = ObjectName::new("tables/branch/materialized-live").expect("materialized");
    let shared = ObjectName::new("tables/branch/shared-live").expect("shared object");
    let already_quarantined =
        ObjectName::new("tables/branch/already-quarantined").expect("quarantined object");
    let outcome = LifecycleRetentionOutcome::from_decisions(
        complete_retention_proof(1, 7),
        vec![
            LifecycleRetentionDecisionRecord::table(
                owned.clone(),
                RetentionDecision::Retain,
                LifecycleRetentionDecisionReason::ReachableTable,
            ),
            LifecycleRetentionDecisionRecord::table(
                inherited.clone(),
                RetentionDecision::Retain,
                LifecycleRetentionDecisionReason::ReachableInheritedTable,
            ),
            LifecycleRetentionDecisionRecord::table(
                materialized.clone(),
                RetentionDecision::Retain,
                LifecycleRetentionDecisionReason::ReachableMaterializedTable,
            ),
            LifecycleRetentionDecisionRecord::table(
                shared.clone(),
                RetentionDecision::Retain,
                LifecycleRetentionDecisionReason::ReachableSharedTable,
            ),
            LifecycleRetentionDecisionRecord::table(
                already_quarantined.clone(),
                RetentionDecision::SkipUntilProof,
                LifecycleRetentionDecisionReason::TableAlreadyQuarantined,
            ),
        ],
        0,
    )
    .expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.objects_retained(), 4);
    assert_eq!(outcome.objects_skipped(), 1);
    for (object, reason) in [
        (owned, LifecycleRetentionDecisionReason::ReachableTable),
        (
            inherited,
            LifecycleRetentionDecisionReason::ReachableInheritedTable,
        ),
        (
            materialized,
            LifecycleRetentionDecisionReason::ReachableMaterializedTable,
        ),
        (
            shared,
            LifecycleRetentionDecisionReason::ReachableSharedTable,
        ),
        (
            already_quarantined,
            LifecycleRetentionDecisionReason::TableAlreadyQuarantined,
        ),
    ] {
        assert!(outcome
            .decisions()
            .iter()
            .any(|decision| { decision.object() == Some(&object) && decision.reason() == reason }));
    }
}

#[test]
fn table_object_decision_lists_branch_and_table_identity() {
    let object = ObjectName::new("tables/branch-a/table-b").expect("object");
    let outcome = LifecycleRetentionOutcome::from_decisions(
        complete_retention_proof(1, 7),
        vec![table_quarantine_candidate(object.clone())],
        0,
    )
    .expect("outcome")
    .maintenance_outcome();

    assert_eq!(outcome.affected_object_names(), [object.to_string()]);
}

#[test]
fn table_object_retention_never_calls_backend_delete() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::default());
    let _decision =
        table_quarantine_candidate(ObjectName::new("tables/branch/table").expect("object"));

    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn table_object_retention_never_calls_quarantine_mutation() {
    let outcome = LifecycleRetentionOutcome::from_decisions(
        complete_retention_proof(1, 7),
        vec![table_quarantine_candidate(
            ObjectName::new("tables/branch/table").expect("object"),
        )],
        0,
    )
    .expect("outcome");

    assert!(outcome
        .decisions()
        .iter()
        .all(|decision| decision.decision() != RetentionDecision::PurgeCandidate));
}

#[test]
fn table_object_retention_delegates_purge_to_later_repair_slice() {
    let outcome =
        retention_outcome_for_delegated_families(complete_retention_proof(1, 7)).expect("outcome");

    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.family() == LifecycleRetentionObjectFamily::Quarantine));
    assert!(outcome
        .decisions()
        .iter()
        .all(|decision| decision.decision() == RetentionDecision::SkipUntilProof));
}

#[test]
fn table_object_retention_preserves_compaction_checkpoint_debt() {
    let outcome = LifecycleRetentionOutcome::from_decisions(
        complete_retention_proof(1, 7),
        vec![LifecycleRetentionDecisionRecord::delegated(
            LifecycleRetentionObjectFamily::Quarantine,
            LifecycleRetentionDecisionReason::DelegatedToQuarantine,
        )],
        0,
    )
    .expect("outcome")
    .maintenance_outcome()
    .with_checkpoint_required(true);

    assert!(outcome.checkpoint_required());
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
}

#[test]
fn table_object_retention_ignores_product_branch_attribution() {
    let debug = format!(
        "{:?}",
        table_quarantine_candidate(ObjectName::new("tables/raw-branch/table").expect("object"))
    );

    for forbidden in ["Database::open", "VersionedValue", "StrataHub", "EntityRef"] {
        assert!(!debug.contains(forbidden));
    }
}

#[test]
fn retention_scope_snapshot_decisions_respect_live_and_newest_windows() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let snapshots = SnapshotService::new(backend)
        .list_snapshots()
        .expect("snapshots");
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(1, 7)),
        &RecoveryHealth::Healthy,
        snapshots.len(),
    );

    let outcome = retention_outcome_for_scope(&request, proof, &snapshots).expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.objects_pruned(), 1);
    assert_eq!(outcome.objects_retained(), 2);
    assert!(outcome.decisions().iter().any(|decision| {
        decision.reason() == LifecycleRetentionDecisionReason::LiveManifestSnapshot
    }));
    assert!(outcome.decisions().iter().any(|decision| {
        decision.reason() == LifecycleRetentionDecisionReason::NewestSnapshotWindow
    }));
    assert!(outcome.decisions().iter().any(|decision| {
        decision.reason() == LifecycleRetentionDecisionReason::SnapshotPruneCandidate
    }));
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn global_retention_scope_includes_snapshot_and_delegated_decisions() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let snapshots = SnapshotService::new(backend)
        .list_snapshots()
        .expect("snapshots");
    let request = LifecycleRetentionRequest::global(1);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(3, 7)),
        &RecoveryHealth::Healthy,
        snapshots.len(),
    );

    let outcome = retention_outcome_for_scope(&request, proof, &snapshots).expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.recovery_health(), None);
    assert_eq!(outcome.objects_pruned(), 2);
    assert_eq!(outcome.objects_skipped(), 2);
    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.family() == LifecycleRetentionObjectFamily::Snapshot));
    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.family() == LifecycleRetentionObjectFamily::Wal));
    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.family() == LifecycleRetentionObjectFamily::Quarantine));
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn retention_delegates_wal_and_quarantine_families() {
    let proof = LifecycleRetentionProof::new(
        LifecycleRetentionProofStatus::Complete,
        RecoveryHealth::Healthy,
        Some(1),
        Some(CommitVersion::new(7)),
        Some(CommitVersion::new(7)),
        None,
    );
    let outcome = retention_outcome_for_delegated_families(proof).expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.recovery_health(), None);
    assert_eq!(outcome.objects_skipped(), 2);
    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.family() == LifecycleRetentionObjectFamily::Wal));
    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.family() == LifecycleRetentionObjectFamily::Quarantine));
}

#[test]
fn wal_objects_are_delegated_to_checkpoint_truncation() {
    let request = LifecycleRetentionRequest::new(LifecycleRetentionScope::WalObjects, 1);
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 0);
    let outcome = retention_outcome_for_scope(&request, proof, &[]).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::DeferredIncompleteProof
    );
    assert_eq!(outcome.objects_skipped(), 1);
    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.reason()
            == LifecycleRetentionDecisionReason::DelegatedToWalTruncation));
}

#[test]
fn wal_retention_without_checkpoint_or_flush_proof_is_incomplete() {
    let request = LifecycleRetentionRequest::new(LifecycleRetentionScope::WalObjects, 1);
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 0);

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("wal_retention_proof"));
}

#[test]
fn wal_delegation_does_not_list_segments() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1]));
    let _outcome =
        retention_outcome_for_delegated_families(complete_retention_proof(1, 7)).expect("outcome");

    assert_eq!(backend.list_calls(), 0);
}

#[test]
fn wal_delegation_does_not_delete_segments() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1]));
    let _outcome =
        retention_outcome_for_delegated_families(complete_retention_proof(1, 7)).expect("outcome");

    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn quarantine_objects_are_delegated_to_quarantine_slice() {
    let request = LifecycleRetentionRequest::new(LifecycleRetentionScope::QuarantineObjects, 1);
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 0);
    let outcome = retention_outcome_for_scope(&request, proof, &[]).expect("outcome");

    assert_eq!(outcome.objects_skipped(), 1);
    assert!(outcome.decisions().iter().any(
        |decision| decision.reason() == LifecycleRetentionDecisionReason::DelegatedToQuarantine
    ));
}

#[test]
fn purge_request_is_deferred_without_fresh_safe_proof() {
    let request = LifecycleRetentionRequest::new(LifecycleRetentionScope::QuarantineObjects, 1);
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &RecoveryHealth::Healthy, 0);
    let outcome = retention_outcome_for_scope(&request, proof, &[])
        .expect("quarantine retention outcome")
        .maintenance_outcome();

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        outcome.reason_class(),
        Some(MaintenanceOutcomeReasonClass::Deferred)
    );
}

#[test]
fn purge_request_does_not_delete_inventory_objects() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::default());
    let _outcome =
        retention_outcome_for_delegated_families(complete_retention_proof(1, 7)).expect("outcome");

    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn retention_delegation_does_not_create_phantom_health_debt() {
    let outcome =
        retention_outcome_for_delegated_families(complete_retention_proof(1, 7)).expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.recovery_health(), None);
    assert!(outcome
        .decisions()
        .iter()
        .any(|decision| decision.family() == LifecycleRetentionObjectFamily::Wal));
}

#[test]
fn global_retention_task_prunes_snapshots_through_durable_maintenance() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0x9b);
    let mut runtime = open_runtime(branch, backend);

    for (snapshot_id, key) in [
        (1, b"retention-key-a" as &'static [u8]),
        (2, b"retention-key-b" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        runtime
            .enqueue_maintenance(MaintenanceTaskRequest::checkpoint_with_options(
                MaintenanceCheckpointOptions::new(Some(snapshot_id), false),
            ))
            .expect("enqueue checkpoint");
        let checkpoint = runtime
            .run_next_checkpoint_maintenance()
            .expect("run checkpoint")
            .expect("checkpoint");
        assert_eq!(checkpoint.status(), MaintenanceOutcomeStatus::Completed);
        // Slice 5: a completed checkpoint chains its own superseded prune;
        // serve it so the explicit retention task below is what prunes.
        drain_snapshot_prune(&mut runtime);
    }
    assert_eq!(backend.snapshot_objects().len(), 1);
    // The superseded snapshot and (#3643) its timeline tail.
    assert_eq!(backend.delete_calls(), 2);
    // A superseded snapshot object the chain never saw (a prior session's).
    let live_bytes = backend
        .object_snapshot()
        .remove(&snapshot_object(2))
        .expect("live snapshot bytes");
    backend.replace_object_bytes(&snapshot_object(1), live_bytes);
    assert_eq!(backend.snapshot_objects().len(), 2);

    let enqueue = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::retention(1))
        .expect("enqueue retention");
    let maintenance = runtime
        .run_next_retention_maintenance()
        .expect("run retention")
        .expect("retention");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::Retention);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(maintenance.state_changes(), 1);
    // One more delete: the newest-N verb prunes no timeline segment (#3643).
    assert_eq!(backend.delete_calls(), 3);
    assert_eq!(backend.snapshot_objects().len(), 1);
}

#[test]
fn branch_retention_task_classifies_table_objects_through_durable_maintenance() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0x9d);
    let mut runtime = open_runtime(branch, backend);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"retention-table-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate for flush");
    let flush = runtime
        .flush_frozen(&retention_flush_request(branch))
        .expect("flush frozen rows");
    let table_object = flush
        .table_object()
        .expect("flushed table object")
        .to_string();

    let request = MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Retention,
        MaintenanceTaskPriority::Low,
        MaintenanceTaskScope::Branch(branch),
        MaintenanceTaskPolicy::coalescing(),
    )
    .expect("branch table retention request");
    let enqueue = runtime
        .enqueue_maintenance(request)
        .expect("enqueue branch retention");
    let maintenance = runtime
        .run_next_retention_maintenance()
        .expect("run branch retention")
        .expect("retention");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert!(maintenance
        .affected_object_names()
        .iter()
        .any(|object| object == &table_object));
    assert_eq!(backend.delete_calls(), 0);
}

fn retention_flush_request(branch: BranchId) -> FlushFrozenRequest {
    FlushFrozenRequest::new(
        branch,
        None,
        FlushTableIdentitySeed::new(format!("retention-flush-{branch}")).expect("seed"),
        FlushTableObjectId::new(format!("retention-object-{branch}")).expect("object"),
    )
    .expect("flush request")
}

#[test]
fn prove_retention_respects_snapshot_scope_without_deleting() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0x9c);
    let mut runtime = open_runtime(branch, backend);

    for (snapshot_id, key) in [
        (1, b"proof-scope-key-a" as &'static [u8]),
        (2, b"proof-scope-key-b" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        runtime
            .enqueue_maintenance(MaintenanceTaskRequest::checkpoint_with_options(
                MaintenanceCheckpointOptions::new(Some(snapshot_id), false),
            ))
            .expect("enqueue checkpoint");
        runtime
            .run_next_checkpoint_maintenance()
            .expect("run checkpoint")
            .expect("checkpoint");
    }

    let outcome = runtime
        .prove_retention(&LifecycleRetentionRequest::snapshot_pruning(1))
        .expect("retention proof");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.objects_pruned(), 1);
    assert_eq!(outcome.objects_retained(), 1);
    assert!(outcome
        .decisions()
        .iter()
        .all(|decision| decision.family() == LifecycleRetentionObjectFamily::Snapshot));
    assert_eq!(backend.delete_calls(), 0);
    assert_eq!(backend.snapshot_objects().len(), 2);
}

#[test]
fn snapshot_pruning_tasks_coalesce_by_retain_policy() {
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    let state = open_state();

    let first = executor
        .enqueue(state, MaintenanceTaskRequest::snapshot_pruning(1))
        .expect("first");
    let second = executor
        .enqueue(state, MaintenanceTaskRequest::snapshot_pruning(2))
        .expect("second");
    let third = executor
        .enqueue(state, MaintenanceTaskRequest::snapshot_pruning(1))
        .expect("third");

    assert!(first.was_enqueued());
    assert!(second.was_enqueued());
    assert!(third.was_coalesced());
    assert_eq!(executor.status().pending_tasks(), 2);
}

#[test]
fn snapshot_pruning_task_builds_snapshot_scope() {
    let task = MaintenanceTask::new_for_test(1, MaintenanceTaskRequest::snapshot_pruning(3))
        .expect("task");
    let request = retention_request_from_maintenance_task(&task).expect("retention request");

    assert_eq!(request.scope(), LifecycleRetentionScope::SnapshotObjects);
    assert_eq!(request.retain_newest_snapshots(), 3);
}

#[test]
fn retention_task_builds_retention_scope() {
    let task =
        MaintenanceTask::new_for_test(1, MaintenanceTaskRequest::retention(4)).expect("task");
    let request = retention_request_from_maintenance_task(&task).expect("retention request");

    assert_eq!(request.scope(), LifecycleRetentionScope::Global);
    assert_eq!(request.retain_newest_snapshots(), 4);
}

#[test]
fn snapshot_pruning_task_rejected_before_open() {
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let error = executor
        .enqueue(
            LifecycleStateMachine::new(),
            MaintenanceTaskRequest::snapshot_pruning(1),
        )
        .expect_err("not open");

    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn retention_task_rejected_while_closing() {
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let error = executor
        .enqueue(closing_state(), MaintenanceTaskRequest::retention(1))
        .expect_err("closing rejects ordinary work");

    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn retention_task_coalesces_by_scope() {
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let state = open_state();

    let first = executor
        .enqueue(state, MaintenanceTaskRequest::retention(2))
        .expect("first");
    let second = executor
        .enqueue(state, MaintenanceTaskRequest::retention(2))
        .expect("second");

    assert!(second.was_coalesced());
    assert_eq!(second.task_id(), first.task_id());
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn snapshot_pruning_task_failure_adds_health_debt() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    backend.fail_delete_on_call(1);
    let outcome = snapshot_pruning(backend, 3, 1).maintenance_outcome();

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(outcome.stats().recovery_faults(), 1);
}

#[test]
fn retention_task_incomplete_proof_returns_deferred() {
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 0);
    let outcome = retention_outcome_for_scope(&request, proof, &[])
        .expect("outcome")
        .maintenance_outcome();

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(outcome.reason(), Some("retention proof is incomplete"));
}

#[test]
fn retention_task_blocked_by_recovery_health_returns_failed_or_deferred_by_policy() {
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(1, 7)), &data_loss_health(), 1);
    let outcome = retention_outcome_for_scope(&request, proof, &[])
        .expect("outcome")
        .maintenance_outcome();

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(outcome.reason(), Some("recovery health blocks retention"));
    assert_eq!(outcome.stats().retention_blocks(), 1);
}

#[test]
fn retention_task_skips_unrelated_pending_tasks() {
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let mut runner = PanicRunner;
    executor
        .enqueue(
            open_state(),
            MaintenanceTaskRequest::flush(BranchId::from_bytes([0x33; 16])),
        )
        .expect("enqueue unrelated task");

    let outcome = executor
        .run_next_matching(open_state(), &mut runner, |task| {
            matches!(
                task.kind(),
                MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention
            )
        })
        .expect("run matching");

    assert_eq!(outcome, None);
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn cache_runtime_rejects_durable_retention_tasks_before_backend_access() {
    let backend: &'static MemoryBackend = crate::testkit::leak_static(MemoryBackend::new());
    let mut runtime = LifecycleCacheRuntime::open(
        LifecycleCacheOpenRequest::new(
            StorageOpenPlan::new(
                StorageMode::Cache,
                LifecycleCodecId::identity(),
                RecoveryStrictness::Strict,
                LifecycleConfig::default(),
            )
            .expect("open plan"),
            BranchId::from_bytes([0x9a; 16]),
            crate::commit::CommitBranchGeneration::new(1).expect("generation"),
        )
        .expect("request"),
        backend,
        crate::branch::config::BranchRuntimeConfig::default(),
        crate::commit::CommitRuntimeConfig::default(),
        crate::commit::CommitManualTimestampSource::new(strata_core::Timestamp::from_micros(10)),
    )
    .expect("runtime");

    let error = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::snapshot_pruning(1))
        .expect_err("cache rejects durable work");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
}

#[test]
fn retention_incomplete_error_has_stable_code() {
    let error = LifecycleError::WalRetentionProofIncomplete {
        reason: "retention proof is incomplete",
    };

    assert_eq!(error.code(), "failed_precondition.lifecycle.wal_retention");
}

#[test]
fn retention_blocked_error_has_stable_code() {
    let error = LifecycleError::RetentionBlocked {
        reason: "recovery health blocks retention",
    };

    assert_eq!(error.code(), "failed_precondition.lifecycle.retention");
}

#[test]
fn snapshot_pruning_service_error_preserves_source() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2]));
    backend.fail_listing();
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(2, 7)), &RecoveryHealth::Healthy, 2);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let error = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect_err("list failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
}

#[test]
fn snapshot_pruning_delete_failure_preserves_backend_error() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    backend.fail_delete_on_call(1);
    let outcome = snapshot_pruning(backend, 3, 1);

    assert_eq!(outcome.failed().len(), 1);
    assert_eq!(
        outcome.failed()[0].source().kind(),
        BackendErrorKind::Unavailable
    );
}

#[test]
fn cache_retention_unsupported_uses_storage_error_code() {
    let backend: &'static MemoryBackend = crate::testkit::leak_static(MemoryBackend::new());
    let mut runtime = LifecycleCacheRuntime::open(
        LifecycleCacheOpenRequest::new(
            StorageOpenPlan::new(
                StorageMode::Cache,
                LifecycleCodecId::identity(),
                RecoveryStrictness::Strict,
                LifecycleConfig::default(),
            )
            .expect("open plan"),
            BranchId::from_bytes([0x9d; 16]),
            crate::commit::CommitBranchGeneration::new(1).expect("generation"),
        )
        .expect("request"),
        backend,
        crate::branch::config::BranchRuntimeConfig::default(),
        crate::commit::CommitRuntimeConfig::default(),
        crate::commit::CommitManualTimestampSource::new(strata_core::Timestamp::from_micros(10)),
    )
    .expect("runtime");

    let error = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::retention(1))
        .expect_err("cache rejects retention");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
}

#[test]
fn retention_error_display_does_not_include_object_payload_bytes() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2]));
    backend.fail_listing();
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(2, 7)), &RecoveryHealth::Healthy, 2);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");

    let error = prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect_err("list failure");
    let display = error.to_string();

    assert!(!display.contains("snapshot-1"));
    assert!(!display.contains("snapshot-2"));
}

fn open_state() -> LifecycleStateMachine {
    let mut state = LifecycleStateMachine::new();
    state
        .transition(LifecycleTransitionTrigger::OpenRequested)
        .expect("open requested");
    state
        .transition(LifecycleTransitionTrigger::CacheOpenReady)
        .expect("open ready");
    state
}

fn closing_state() -> LifecycleStateMachine {
    let mut state = open_state();
    state
        .transition(LifecycleTransitionTrigger::CloseRequested)
        .expect("close requested");
    state
}

fn manifest(snapshot_id: u64, snapshot_watermark: u64) -> DatabaseManifest {
    DatabaseManifest::new(DATABASE_ID, "identity")
        .expect("manifest")
        .with_recovery_facts(
            1,
            Some(snapshot_watermark),
            Some(snapshot_id),
            Some(CommitVersion::new(snapshot_watermark)),
        )
        .expect("recovery facts")
}

fn complete_retention_proof(
    live_snapshot_id: u64,
    snapshot_watermark: u64,
) -> LifecycleRetentionProof {
    LifecycleRetentionProof::new(
        LifecycleRetentionProofStatus::Complete,
        RecoveryHealth::Healthy,
        Some(live_snapshot_id),
        Some(CommitVersion::new(snapshot_watermark)),
        Some(CommitVersion::new(snapshot_watermark)),
        None,
    )
}

fn incomplete_retention_proof(missing_fact: &'static str) -> LifecycleRetentionProof {
    LifecycleRetentionProof::new(
        LifecycleRetentionProofStatus::Incomplete,
        RecoveryHealth::Healthy,
        None,
        None,
        None,
        Some(missing_fact),
    )
}

fn snapshot_pruning(
    backend: &RetentionBackend,
    live_snapshot_id: u64,
    retain_newest: usize,
) -> LifecycleSnapshotPruningOutcome {
    let request = LifecycleRetentionRequest::snapshot_pruning(retain_newest);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(live_snapshot_id, 7)),
        &RecoveryHealth::Healthy,
        backend.remaining_snapshot_ids().len(),
    );
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .expect("pruning request");
    prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome")
}

fn data_loss_health() -> RecoveryHealth {
    RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingSnapshotObject, "missing").expect("fault"),
        ],
    )
    .expect("health")
}

fn policy_downgrade_health() -> RecoveryHealth {
    RecoveryHealth::degraded(
        RecoveryDegradationClass::PolicyDowngrade,
        vec![RecoveryFault::new(RecoveryFaultKind::NoManifestFallback, "lossy").expect("fault")],
    )
    .expect("health")
}

fn telemetry_degraded_health() -> RecoveryHealth {
    RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![
            RecoveryFault::new(RecoveryFaultKind::WalTailRepairFailed, "telemetry").expect("fault"),
        ],
    )
    .expect("health")
}

fn snapshot_ids(snapshots: &[crate::service::SnapshotObject]) -> Vec<u64> {
    snapshots
        .iter()
        .map(crate::service::SnapshotObject::snapshot_id)
        .collect()
}

struct PanicRunner;

impl MaintenanceTaskRunner for PanicRunner {
    fn run_task(&mut self, _task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        panic!("retention skip test must not run unrelated tasks");
    }
}

#[derive(Debug, Default)]
struct RetentionBackend {
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    fail_list: AtomicBool,
    fail_delete_call: AtomicUsize,
    // Set of delete-call ordinals (1-based) that must fail. Used by
    // multi-failure tests that need to exercise more than one rejection
    // in a single pruning sweep.
    fail_delete_calls: Mutex<BTreeSet<usize>>,
    delete_calls: AtomicUsize,
    list_calls: AtomicUsize,
    omit_delete_capability: AtomicBool,
    /// #3643: names the listing reports but no object backs — an object that
    /// vanished between a prune's listing and its delete.
    phantoms: Mutex<BTreeSet<ObjectName>>,
    /// #3643: every delete removes the object but reports it non-durable.
    non_durable_deletes: AtomicBool,
}

impl RetentionBackend {
    fn insert_timeline_segment(&self, sealing_snapshot_id: u64, ordinal: u64) {
        self.insert_object(
            timeline_segment_name(sealing_snapshot_id, ordinal),
            b"segment".to_vec(),
        );
    }

    fn insert_phantom(&self, name: ObjectName) {
        self.phantoms.lock().expect("phantoms").insert(name);
    }

    fn remaining_timeline_segments(&self) -> Vec<ObjectName> {
        let prefix = ObjectLayout::timeline_prefix().expect("prefix");
        self.objects
            .lock()
            .expect("objects")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect()
    }

    fn with_snapshots<const N: usize>(ids: [u64; N]) -> Self {
        let backend = Self::default();
        for id in ids {
            backend.insert_snapshot(id);
        }
        backend
    }

    fn insert_snapshot(&self, id: u64) {
        self.objects.lock().expect("objects").insert(
            ObjectLayout::snapshot(id).expect("snapshot object"),
            format!("snapshot-{id}").into_bytes(),
        );
    }

    fn insert_object(&self, name: ObjectName, bytes: Vec<u8>) {
        self.objects.lock().expect("objects").insert(name, bytes);
    }

    fn omit_delete_capability(&self) {
        self.omit_delete_capability.store(true, Ordering::SeqCst);
    }

    fn fail_listing(&self) {
        self.fail_list.store(true, Ordering::SeqCst);
    }

    fn fail_delete_on_call(&self, call: usize) {
        self.fail_delete_call.store(call, Ordering::SeqCst);
    }

    fn fail_delete_calls(&self, calls: impl IntoIterator<Item = usize>) {
        let mut slot = self.fail_delete_calls.lock().expect("fail delete calls");
        slot.extend(calls);
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }

    fn delete_calls(&self) -> usize {
        self.delete_calls.load(Ordering::SeqCst)
    }

    fn remaining_snapshot_ids(&self) -> Vec<u64> {
        let mut ids = self
            .objects
            .lock()
            .expect("objects")
            .keys()
            .filter_map(|object| {
                object
                    .as_str()
                    .rsplit_once('/')
                    .and_then(|(_, id)| u64::from_str_radix(id, 16).ok())
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }
}

impl Backend for RetentionBackend {
    fn capabilities(&self) -> BackendCapabilities {
        if self.omit_delete_capability.load(Ordering::SeqCst) {
            BackendCapabilities::from_slice(&[
                BackendCapability::ReadObject,
                BackendCapability::ReadRange,
                BackendCapability::WriteObject,
                BackendCapability::ListPrefix,
                BackendCapability::ObjectMetadata,
            ])
        } else {
            BackendCapabilities::from_slice(BASIC_OBJECT_BACKEND_CAPABILITIES)
        }
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
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
        let call = self
            .delete_calls
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        if self.fail_delete_call.load(Ordering::SeqCst) == call
            || self
                .fail_delete_calls
                .lock()
                .expect("fail delete calls")
                .contains(&call)
        {
            return crate::backend::failed_delete_result(
                name,
                BackendError::new(BackendErrorKind::Unavailable, "injected delete failure"),
            );
        }
        if self.phantoms.lock().expect("phantoms").remove(name) {
            return crate::backend::failed_delete_result(
                name,
                BackendError::new(BackendErrorKind::NotFound, "object vanished"),
            );
        }
        let removed = self.objects.lock().expect("objects").remove(name).is_some();
        if self.non_durable_deletes.load(Ordering::SeqCst) {
            return Ok(crate::backend::DeleteOutcome::from_removed(
                name.clone(),
                crate::backend::DeleteDurability::NonDurable,
                removed,
            ));
        }
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_list.load(Ordering::SeqCst) {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "injected list failure",
            ));
        }
        let mut names = self
            .objects
            .lock()
            .expect("objects")
            .keys()
            .chain(self.phantoms.lock().expect("phantoms").iter())
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect::<Vec<_>>();
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
}

/// Every retention status maps to exactly the typed deferral the reclaim
/// ledger reports; both completed statuses carry none (space-reclamation
/// contract §3.5; rule 39: typed reasons, never prose).
#[test]
fn retention_deferral_reason_truth_table() {
    use crate::lifecycle::retention::{retention_deferral_reason, LifecycleRetentionStatus};
    let cases = [
        (
            LifecycleRetentionStatus::DeferredIncompleteProof,
            Some(MaintenanceDeferralReason::IncompleteProof),
        ),
        (
            LifecycleRetentionStatus::DeferredUnsupportedScope,
            Some(MaintenanceDeferralReason::UnsupportedScope),
        ),
        (
            LifecycleRetentionStatus::BlockedByRecoveryHealth,
            Some(MaintenanceDeferralReason::RecoveryHealth),
        ),
        (LifecycleRetentionStatus::Completed, None),
        (LifecycleRetentionStatus::CompletedWithHealthDebt, None),
    ];
    for (status, expected) in cases {
        assert_eq!(retention_deferral_reason(status), expected, "{status:?}");
    }
}

/// A retention proof status maps to the typed deferral of a snapshot prune;
/// a complete proof carries none.
#[test]
fn proof_deferral_reason_truth_table() {
    use crate::lifecycle::retention::{proof_deferral_reason, LifecycleRetentionProofStatus};
    let cases = [
        (
            LifecycleRetentionProofStatus::Incomplete,
            Some(MaintenanceDeferralReason::IncompleteProof),
        ),
        (
            LifecycleRetentionProofStatus::BlockedByRecoveryHealth,
            Some(MaintenanceDeferralReason::RecoveryHealth),
        ),
        (LifecycleRetentionProofStatus::Complete, None),
    ];
    for (status, expected) in cases {
        assert_eq!(proof_deferral_reason(status), expected, "{status:?}");
    }
}

// ---------------------------------------------------------------------------
// Space-reclamation contract §3.4 (slice 5, #3592): proof-driven prune modes.
// ---------------------------------------------------------------------------

#[test]
fn superseded_snapshot_truth_table() {
    for (snapshot_id, live_snapshot_id, expected) in [
        (1, 3, true),
        (2, 3, true),
        (3, 3, false),
        (4, 3, false),
        (u64::MAX, 3, false),
        (1, 1, false),
    ] {
        assert_eq!(
            superseded_snapshot(snapshot_id, live_snapshot_id),
            expected,
            "snapshot {snapshot_id} against live {live_snapshot_id}"
        );
    }
}

#[test]
fn reconcilable_orphan_truth_table() {
    for (snapshot_id, attested_snapshot_id, expected) in [
        (1, 3, true),
        (2, 3, true),
        (3, 3, false),
        (4, 3, true),
        (u64::MAX, 3, true),
        (1, 1, false),
    ] {
        assert_eq!(
            reconcilable_orphan(snapshot_id, attested_snapshot_id),
            expected,
            "snapshot {snapshot_id} against attested {attested_snapshot_id}"
        );
    }
}

type ModeDecisionRow = (
    &'static str,
    u64,
    Option<u64>,
    bool,
    (RetentionDecision, LifecycleRetentionDecisionReason),
);

fn assert_mode_decisions<const N: usize>(mode: SnapshotPruneMode, rows: [ModeDecisionRow; N]) {
    for (case, snapshot_id, live_snapshot_id, newest_retained, expected) in rows {
        assert_eq!(
            snapshot_mode_decision(mode, snapshot_id, live_snapshot_id, newest_retained),
            expected,
            "{mode:?}: {case}"
        );
    }
}

#[test]
fn snapshot_mode_decision_truth_table_newest_window() {
    use LifecycleRetentionDecisionReason as Reason;
    use RetentionDecision::{PruneCandidate, Retain};

    assert_mode_decisions(
        SnapshotPruneMode::RetainNewest,
        [
            (
                "keeps the window",
                1,
                Some(3),
                true,
                (Retain, Reason::NewestSnapshotWindow),
            ),
            (
                "prunes outside it",
                1,
                Some(3),
                false,
                (PruneCandidate, Reason::SnapshotPruneCandidate),
            ),
            (
                "needs no live id",
                1,
                None,
                false,
                (PruneCandidate, Reason::SnapshotPruneCandidate),
            ),
        ],
    );
}

#[test]
fn snapshot_mode_decision_truth_table_superseded() {
    use LifecycleRetentionDecisionReason as Reason;
    use RetentionDecision::{PruneCandidate, Retain};

    assert_mode_decisions(
        SnapshotPruneMode::Superseded,
        [
            (
                "below live",
                2,
                Some(3),
                false,
                (PruneCandidate, Reason::SupersededSnapshot),
            ),
            (
                "ignores the newest window",
                2,
                Some(3),
                true,
                (PruneCandidate, Reason::SupersededSnapshot),
            ),
            (
                "at live",
                3,
                Some(3),
                false,
                (Retain, Reason::AboveLiveSnapshot),
            ),
            (
                "above live",
                5,
                Some(3),
                false,
                (Retain, Reason::AboveLiveSnapshot),
            ),
            (
                "without a live id",
                1,
                None,
                false,
                (Retain, Reason::ProofIncomplete),
            ),
        ],
    );
}

#[test]
fn snapshot_mode_decision_truth_table_reconcile_to_attested() {
    use LifecycleRetentionDecisionReason as Reason;
    use RetentionDecision::{PruneCandidate, Retain};

    assert_mode_decisions(
        SnapshotPruneMode::ReconcileToAttested,
        [
            (
                "below attested",
                2,
                Some(3),
                false,
                (PruneCandidate, Reason::NonAttestedSnapshot),
            ),
            (
                "above attested, newest window ignored",
                5,
                Some(3),
                true,
                (PruneCandidate, Reason::NonAttestedSnapshot),
            ),
            (
                "at attested",
                3,
                Some(3),
                false,
                (Retain, Reason::LiveManifestSnapshot),
            ),
            (
                "without an attested id",
                1,
                None,
                false,
                (Retain, Reason::ProofIncomplete),
            ),
        ],
    );
}

fn proof_driven_pruning(
    backend: &RetentionBackend,
    live_snapshot_id: u64,
    mode: SnapshotPruneMode,
    retain_newest: usize,
) -> LifecycleSnapshotPruningOutcome {
    let request =
        LifecycleRetentionRequest::snapshot_pruning(retain_newest).with_snapshot_prune_mode(mode);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(live_snapshot_id, 7)),
        &RecoveryHealth::Healthy,
        backend.remaining_snapshot_ids().len(),
    );
    let pruning =
        LifecycleSnapshotPruningRequest::for_request(proof, &request).expect("pruning request");
    prune_snapshots_with_proof(
        &SnapshotService::new(backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .expect("outcome")
}

#[test]
fn superseded_prune_deletes_below_the_live_id_and_protects_at_or_above_it() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3, 5]));

    let outcome = proof_driven_pruning(backend, 3, SnapshotPruneMode::Superseded, 1);

    assert!(outcome.completed());
    assert_eq!(snapshot_ids(outcome.deleted()), [1, 2]);
    assert_eq!(snapshot_ids(outcome.protected()), [3, 5]);
    assert!(outcome.failed().is_empty());
    assert_eq!(backend.remaining_snapshot_ids(), [3, 5]);
    assert_eq!(backend.delete_calls(), 2);
}

#[test]
fn superseded_prune_ignores_the_newest_window_the_explicit_verb_honors() {
    let proof_driven: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
    let explicit: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));

    let superseded = proof_driven_pruning(proof_driven, 3, SnapshotPruneMode::Superseded, 10);
    let newest_window = snapshot_pruning(explicit, 3, 10);

    assert_eq!(snapshot_ids(superseded.deleted()), [1, 2]);
    assert_eq!(proof_driven.remaining_snapshot_ids(), [3]);
    assert!(newest_window.deleted().is_empty());
    assert_eq!(explicit.remaining_snapshot_ids(), [1, 2, 3]);
}

#[test]
fn reconcile_prune_deletes_every_snapshot_but_the_attested_one() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3, 5]));

    let outcome = proof_driven_pruning(backend, 3, SnapshotPruneMode::ReconcileToAttested, 1);

    assert!(outcome.completed());
    assert_eq!(snapshot_ids(outcome.deleted()), [1, 2, 5]);
    assert_eq!(snapshot_ids(outcome.protected()), [3]);
    assert_eq!(backend.remaining_snapshot_ids(), [3]);
    assert_eq!(backend.delete_calls(), 3);
}

/// An incomplete proof defers before touching the backend — except the one
/// narrow arm #3643 added (re-review P2): the open reconcile with NO attested
/// snapshot reconciles only the timeline family (it lists segments; it never
/// touches a snapshot), and still defers when the referenced set is unknown.
#[test]
fn proof_driven_prunes_defer_on_an_incomplete_proof_before_backend_access() {
    {
        let mode = SnapshotPruneMode::Superseded;
        let backend: &'static RetentionBackend =
            crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
        let request = LifecycleRetentionRequest::snapshot_pruning(1).with_snapshot_prune_mode(mode);
        // No manifest snapshot facts: the proof is incomplete.
        let proof = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 3);
        let pruning =
            LifecycleSnapshotPruningRequest::for_request(proof, &request).expect("pruning request");

        let outcome = prune_snapshots_with_proof(
            &SnapshotService::new(backend),
            &pruning,
            Some(&std::collections::BTreeSet::new()),
        )
        .expect("outcome");

        assert!(outcome.deferred_incomplete_proof(), "{mode:?}");
        assert_eq!(backend.list_calls(), 0, "{mode:?}");
        assert_eq!(backend.delete_calls(), 0, "{mode:?}");
        assert_eq!(backend.remaining_snapshot_ids(), [1, 2, 3], "{mode:?}");
    }

    let reconcile = |referenced: Option<&std::collections::BTreeSet<_>>| {
        let backend: &'static RetentionBackend =
            crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
        backend.insert_timeline_segment(1, 0);
        let request = LifecycleRetentionRequest::snapshot_pruning(1)
            .with_snapshot_prune_mode(SnapshotPruneMode::ReconcileToAttested);
        let proof = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 3);
        let pruning =
            LifecycleSnapshotPruningRequest::for_request(proof, &request).expect("pruning request");
        let outcome =
            prune_snapshots_with_proof(&SnapshotService::new(backend), &pruning, referenced)
                .expect("outcome");
        (backend, outcome)
    };
    let (backend, outcome) = reconcile(Some(&std::collections::BTreeSet::new()));
    assert!(outcome.completed(), "the snapshot-less reconcile runs");
    assert!(outcome.deleted().is_empty(), "no snapshot is touched");
    assert_eq!(backend.remaining_snapshot_ids(), [1, 2, 3]);
    assert!(
        backend.remaining_timeline_segments().is_empty(),
        "the orphan segment went"
    );
    assert_eq!(backend.delete_calls(), 1);

    let (backend, outcome) = reconcile(None);
    assert!(
        outcome.deferred_incomplete_proof(),
        "unknown references: defer"
    );
    assert_eq!(backend.list_calls(), 0);
    assert_eq!(backend.remaining_timeline_segments().len(), 1);
}

#[test]
fn proof_driven_prunes_are_blocked_by_lossy_recovery() {
    for (mode, health) in [
        (SnapshotPruneMode::Superseded, data_loss_health()),
        (SnapshotPruneMode::ReconcileToAttested, data_loss_health()),
        (SnapshotPruneMode::Superseded, policy_downgrade_health()),
        (
            SnapshotPruneMode::ReconcileToAttested,
            policy_downgrade_health(),
        ),
    ] {
        let backend: &'static RetentionBackend =
            crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3]));
        let request = LifecycleRetentionRequest::snapshot_pruning(1).with_snapshot_prune_mode(mode);
        let proof = build_retention_proof(&request, Some(&manifest(3, 7)), &health, 3);
        let pruning =
            LifecycleSnapshotPruningRequest::for_request(proof, &request).expect("pruning request");

        let outcome = prune_snapshots_with_proof(
            &SnapshotService::new(backend),
            &pruning,
            Some(&std::collections::BTreeSet::new()),
        )
        .expect("outcome");

        assert!(outcome.blocked_by_recovery_health(), "{mode:?} {health:?}");
        assert_eq!(backend.list_calls(), 0, "{mode:?} {health:?}");
        assert_eq!(backend.delete_calls(), 0, "{mode:?} {health:?}");
        assert_eq!(backend.remaining_snapshot_ids(), [1, 2, 3], "{mode:?}");
    }
}

#[test]
fn pruning_request_for_request_carries_the_mode_and_matches_the_newest_window_verb() {
    let proof = complete_retention_proof(3, 7);
    let newest_window = LifecycleRetentionRequest::snapshot_pruning(2);
    let superseded = LifecycleRetentionRequest::snapshot_pruning(2)
        .with_snapshot_prune_mode(SnapshotPruneMode::Superseded);

    let from_request = LifecycleSnapshotPruningRequest::for_request(proof.clone(), &newest_window)
        .expect("newest-window request");
    let explicit = LifecycleSnapshotPruningRequest::new(proof.clone(), 2).expect("explicit verb");
    let proof_driven = LifecycleSnapshotPruningRequest::for_request(proof, &superseded)
        .expect("superseded request");

    assert_eq!(from_request, explicit);
    assert_eq!(
        from_request.snapshot_prune_mode(),
        SnapshotPruneMode::RetainNewest
    );
    assert_eq!(
        proof_driven.snapshot_prune_mode(),
        SnapshotPruneMode::Superseded
    );
    assert_eq!(proof_driven.live_snapshot_id(), Some(3));
    assert_eq!(proof_driven.effective_retain_newest(), 2);
}

#[test]
fn maintenance_task_carries_the_snapshot_prune_mode_into_the_retention_request() {
    for (request, expected_mode) in [
        (
            MaintenanceTaskRequest::snapshot_pruning(3),
            SnapshotPruneMode::RetainNewest,
        ),
        (
            MaintenanceTaskRequest::snapshot_pruning_with_mode(SnapshotPruneMode::Superseded),
            SnapshotPruneMode::Superseded,
        ),
        (
            MaintenanceTaskRequest::snapshot_pruning_with_mode(
                SnapshotPruneMode::ReconcileToAttested,
            ),
            SnapshotPruneMode::ReconcileToAttested,
        ),
    ] {
        let task = MaintenanceTask::new_for_test(1, request).expect("task");
        let retention = retention_request_from_maintenance_task(&task).expect("retention request");

        assert_eq!(retention.scope(), LifecycleRetentionScope::SnapshotObjects);
        assert_eq!(retention.snapshot_prune_mode(), expected_mode);
        assert_eq!(
            task.retention_options()
                .expect("retention options")
                .snapshot_prune_mode(),
            expected_mode
        );
    }
}

#[test]
fn proof_driven_prunes_coalesce_by_mode_and_never_into_the_explicit_verb() {
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    let state = open_state();

    let newest_window = executor
        .enqueue(state, MaintenanceTaskRequest::snapshot_pruning(1))
        .expect("newest window");
    let superseded = executor
        .enqueue(
            state,
            MaintenanceTaskRequest::snapshot_pruning_with_mode(SnapshotPruneMode::Superseded),
        )
        .expect("superseded");
    let reconcile = executor
        .enqueue(
            state,
            MaintenanceTaskRequest::snapshot_pruning_with_mode(
                SnapshotPruneMode::ReconcileToAttested,
            ),
        )
        .expect("reconcile");
    let superseded_again = executor
        .enqueue(
            state,
            MaintenanceTaskRequest::snapshot_pruning_with_mode(SnapshotPruneMode::Superseded),
        )
        .expect("superseded again");

    assert!(newest_window.was_enqueued());
    assert!(superseded.was_enqueued());
    assert!(reconcile.was_enqueued());
    assert!(superseded_again.was_coalesced());
    assert_eq!(executor.status().pending_tasks(), 3);
}

#[test]
fn retention_decisions_follow_the_prune_mode() {
    use LifecycleRetentionDecisionReason as Reason;

    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([1, 2, 3, 5]));
    let snapshots = SnapshotService::new(backend)
        .list_snapshots()
        .expect("snapshots");

    for (mode, pruned, retained, expected_reasons) in [
        (
            SnapshotPruneMode::Superseded,
            2,
            2,
            vec![
                (Reason::SupersededSnapshot, 2),
                (Reason::LiveManifestSnapshot, 1),
                (Reason::AboveLiveSnapshot, 1),
            ],
        ),
        (
            SnapshotPruneMode::ReconcileToAttested,
            3,
            1,
            vec![
                (Reason::NonAttestedSnapshot, 3),
                (Reason::LiveManifestSnapshot, 1),
            ],
        ),
        (
            SnapshotPruneMode::RetainNewest,
            2,
            2,
            vec![
                (Reason::SnapshotPruneCandidate, 2),
                (Reason::LiveManifestSnapshot, 1),
                (Reason::NewestSnapshotWindow, 1),
            ],
        ),
    ] {
        let request = LifecycleRetentionRequest::snapshot_pruning(1).with_snapshot_prune_mode(mode);
        let proof = build_retention_proof(
            &request,
            Some(&manifest(3, 7)),
            &RecoveryHealth::Healthy,
            snapshots.len(),
        );

        let outcome =
            retention_outcome_for_scope(&request, proof, &snapshots).expect("retention outcome");

        assert_eq!(
            outcome.status(),
            LifecycleRetentionStatus::Completed,
            "{mode:?}"
        );
        assert_eq!(outcome.objects_pruned(), pruned, "{mode:?}");
        assert_eq!(outcome.objects_retained(), retained, "{mode:?}");
        for (reason, count) in expected_reasons {
            assert_eq!(
                outcome
                    .decisions()
                    .iter()
                    .filter(|decision| decision.reason() == reason)
                    .count(),
                count,
                "{mode:?} {reason:?}"
            );
        }
        // The decision path and the deleter agree: nothing was deleted here.
        assert_eq!(backend.delete_calls(), 0);

        // The Global scope carries the same mode into its snapshot decisions
        // (its WAL and quarantine families stay delegated).
        let global = LifecycleRetentionRequest::global(1).with_snapshot_prune_mode(mode);
        let proof = build_retention_proof(
            &global,
            Some(&manifest(3, 7)),
            &RecoveryHealth::Healthy,
            snapshots.len(),
        );
        let outcome =
            retention_outcome_for_scope(&global, proof, &snapshots).expect("global outcome");
        assert_eq!(outcome.objects_pruned(), pruned, "global {mode:?}");
        assert_eq!(
            outcome
                .decisions()
                .iter()
                .filter(|decision| decision.family() == LifecycleRetentionObjectFamily::Snapshot)
                .count(),
            snapshots.len(),
            "global {mode:?}"
        );
    }
}

// --- checkpoint chaining and the open reconcile, through the durable runtime ---

fn checkpoint_through_sync_runner(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    snapshot_id: u64,
) -> MaintenanceOutcome {
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint_with_options(
            MaintenanceCheckpointOptions::new(Some(snapshot_id), false),
        ))
        .expect("enqueue checkpoint");
    runtime
        .run_next_checkpoint_maintenance()
        .expect("run checkpoint")
        .expect("checkpoint outcome")
}

fn pending_snapshot_prunes(
    runtime: &LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
) -> usize {
    runtime
        .pending_maintenance_kinds_for_test()
        .iter()
        .filter(|kind| **kind == MaintenanceTaskKind::SnapshotPruning)
        .count()
}

/// The retention lane serves marks and prunes in queue order; run it until
/// the queued snapshot prune has completed and return that outcome.
fn drain_snapshot_prune(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
) -> MaintenanceOutcome {
    while let Some(outcome) = runtime
        .run_next_retention_maintenance()
        .expect("retention lane")
    {
        if outcome.task_kind() == MaintenanceTaskKind::SnapshotPruning {
            return outcome;
        }
    }
    panic!("no snapshot prune was queued");
}

fn snapshot_object(snapshot_id: u64) -> ObjectName {
    ObjectLayout::snapshot(snapshot_id).expect("snapshot object")
}

fn sorted_snapshot_objects<const N: usize>(ids: [u64; N]) -> Vec<ObjectName> {
    let mut objects = ids.map(snapshot_object).to_vec();
    objects.sort();
    objects
}

fn manifest_replacements_so_far(backend: &CheckpointTestBackend) -> usize {
    backend
        .events()
        .iter()
        .filter(|event| matches!(event, CheckpointBackendEvent::DatabaseRecordReplace))
        .count()
}

fn attested_snapshot_id(backend: &CheckpointTestBackend) -> Option<u64> {
    crate::service::DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest")
        .snapshot_id()
}

#[test]
fn completed_checkpoints_chain_a_superseded_prune_that_leaves_one_snapshot() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa1);
    let mut runtime = open_runtime(branch, backend);
    assert_eq!(
        pending_snapshot_prunes(&runtime),
        0,
        "a created store owes no prune"
    );

    for (snapshot_id, key) in [
        (1, b"chain-a" as &'static [u8]),
        (2, b"chain-b" as &'static [u8]),
        (3, b"chain-c" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        let checkpoint = checkpoint_through_sync_runner(&mut runtime, snapshot_id);
        assert_eq!(checkpoint.status(), MaintenanceOutcomeStatus::Completed);
        assert_eq!(
            pending_snapshot_prunes(&runtime),
            1,
            "a completed checkpoint queues exactly one prune"
        );

        let prune = drain_snapshot_prune(&mut runtime);

        assert_eq!(prune.status(), MaintenanceOutcomeStatus::Completed);
        // The superseded snapshot and (#3643) the timeline tail it sealed.
        assert_eq!(
            prune.state_changes(),
            2 * usize::from(snapshot_id > 1),
            "the prune deletes the one superseded snapshot"
        );
        assert_eq!(
            backend.snapshot_objects(),
            vec![snapshot_object(snapshot_id)]
        );
        assert_eq!(attested_snapshot_id(backend), Some(snapshot_id));
        assert_eq!(pending_snapshot_prunes(&runtime), 0);
    }
}

#[test]
fn superseded_prune_protects_the_live_snapshot_and_any_id_at_or_above_it() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa2);
    let mut runtime = open_runtime(branch, backend);
    for (snapshot_id, key) in [
        (1, b"above-a" as &'static [u8]),
        (2, b"above-b" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        let checkpoint = checkpoint_through_sync_runner(&mut runtime, snapshot_id);
        assert_eq!(checkpoint.status(), MaintenanceOutcomeStatus::Completed);
    }
    // Two completed checkpoints, one coalesced prune still queued.
    assert_eq!(pending_snapshot_prunes(&runtime), 1);
    // A snapshot object above the live id: a publish whose manifest re-point
    // has not landed. Its bytes are the live snapshot's so it lists as one.
    let live_bytes = backend
        .object_snapshot()
        .remove(&snapshot_object(2))
        .expect("live snapshot bytes");
    backend.replace_object_bytes(&snapshot_object(5), live_bytes);
    assert_eq!(
        backend.snapshot_objects(),
        sorted_snapshot_objects([1, 2, 5])
    );

    let prune = drain_snapshot_prune(&mut runtime);

    assert_eq!(prune.status(), MaintenanceOutcomeStatus::Completed);
    // Snapshot 1 and (#3643) the timeline tail it sealed.
    assert_eq!(prune.state_changes(), 2);
    assert_eq!(backend.snapshot_objects(), sorted_snapshot_objects([2, 5]));
    assert_eq!(attested_snapshot_id(backend), Some(2));
}

#[test]
fn explicit_checkpoint_verb_queues_a_prune_only_when_it_completes() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa3);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"verb-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(41)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("checkpoint");

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(pending_snapshot_prunes(&runtime), 1);
    let prune = drain_snapshot_prune(&mut runtime);
    assert_eq!(prune.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(backend.snapshot_objects(), vec![snapshot_object(1)]);
}

#[test]
fn uncertain_or_failed_checkpoints_queue_no_prune() {
    for (case, uncertain) in [
        ("uncertain manifest re-point", true),
        ("failed manifest re-point", false),
    ] {
        let backend: &'static CheckpointTestBackend =
            crate::testkit::leak_static(CheckpointTestBackend::new());
        let branch = durable_branch_id(0xa4);
        let mut runtime = open_runtime(branch, backend);
        runtime
            .execute_durable_commit(
                durable_batch(branch, b"no-prune-key", b"value"),
                generation_guard(),
            )
            .expect("commit");
        // The checkpoint replaces the manifest twice (active-WAL-segment
        // persist, then the re-point); fault the re-point.
        let re_point = manifest_replacements_so_far(backend) + 2;
        if uncertain {
            backend.uncertain_manifest_replacement_on_call(re_point);
        } else {
            backend.fail_manifest_replacement_on_call(re_point);
        }
        let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(42))
            .expect("request");

        let outcome = runtime.checkpoint(&request).expect(case);

        let expected = if uncertain {
            LifecycleCheckpointStatus::SnapshotVisibilityUncertain
        } else {
            LifecycleCheckpointStatus::SnapshotPublishedManifestNotUpdated
        };
        assert_eq!(outcome.status(), expected, "{case}");
        assert_eq!(pending_snapshot_prunes(&runtime), 0, "{case}");
    }

    // The same refusal through the sync runner.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa5);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"no-prune-runner-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_replacement_on_call(manifest_replacements_so_far(backend) + 2);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint_with_options(
            MaintenanceCheckpointOptions::new(Some(1), false),
        ))
        .expect("enqueue checkpoint");

    let outcome = runtime
        .run_next_checkpoint_maintenance()
        .expect("run checkpoint")
        .expect("checkpoint outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Failed);
    assert_eq!(attested_snapshot_id(backend), None);
    assert_eq!(pending_snapshot_prunes(&runtime), 0);
}

#[test]
fn reopen_reconciles_the_snapshot_family_to_the_attested_id() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa6);
    let mut runtime = open_runtime(branch, backend);
    for (snapshot_id, key) in [
        (1, b"reopen-a" as &'static [u8]),
        (2, b"reopen-b" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        let checkpoint = checkpoint_through_sync_runner(&mut runtime, snapshot_id);
        assert_eq!(checkpoint.status(), MaintenanceOutcomeStatus::Completed);
    }
    // The session ends with its prune undrained, and a crash orphan above the
    // attested id (a publish the manifest never attested) is left behind.
    let live_bytes = backend
        .object_snapshot()
        .remove(&snapshot_object(2))
        .expect("live snapshot bytes");
    backend.replace_object_bytes(&snapshot_object(5), live_bytes);
    assert_eq!(
        backend.snapshot_objects(),
        sorted_snapshot_objects([1, 2, 5])
    );
    drop(runtime);

    let mut reopened = open_runtime(branch, backend);

    assert_eq!(
        pending_snapshot_prunes(&reopened),
        1,
        "a reopen queues one reconcile"
    );
    assert_eq!(
        backend.snapshot_objects(),
        sorted_snapshot_objects([1, 2, 5]),
        "open reclaims nothing inline"
    );
    let prune = drain_snapshot_prune(&mut reopened);
    assert_eq!(prune.status(), MaintenanceOutcomeStatus::Completed);
    // Snapshots 1 and 5, and (#3643) the timeline tail snapshot 1 sealed
    // (snapshot 5 was planted by hand and sealed none).
    assert_eq!(prune.state_changes(), 3);
    assert_eq!(backend.snapshot_objects(), vec![snapshot_object(2)]);
    assert_eq!(attested_snapshot_id(backend), Some(2));
    assert_eq!(pending_snapshot_prunes(&reopened), 0);
}

#[test]
fn a_snapshot_published_without_its_manifest_repoint_is_reconciled_at_the_next_open() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa7);
    let first_key = physical_key(branch, b"orphan-newest-a");
    let second_key = physical_key(branch, b"orphan-newest-b");
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"orphan-newest-a", b"value-a"),
            generation_guard(),
        )
        .expect("first commit");
    let first = checkpoint_through_sync_runner(&mut runtime, 1);
    assert_eq!(first.status(), MaintenanceOutcomeStatus::Completed);
    drain_snapshot_prune(&mut runtime);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"orphan-newest-b", b"value-b"),
            generation_guard(),
        )
        .expect("second commit");
    // The second checkpoint replaces the manifest twice: the active-WAL-segment
    // persist first, then the snapshot re-point. Fail the re-point, so the
    // snapshot object is published but never attested.
    backend.fail_manifest_replacement_on_call(manifest_replacements_so_far(backend) + 2);
    let request =
        LifecycleCheckpointRequest::new(branch, 2, Timestamp::from_micros(43)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("partial checkpoint");

    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::SnapshotPublishedManifestNotUpdated
    );
    assert_eq!(
        pending_snapshot_prunes(&runtime),
        0,
        "no prune without a completed checkpoint"
    );
    assert_eq!(backend.snapshot_objects(), sorted_snapshot_objects([1, 2]));
    assert_eq!(attested_snapshot_id(backend), Some(1));
    drop(runtime);

    let mut reopened = open_runtime(branch, backend);
    let prune = drain_snapshot_prune(&mut reopened);

    assert_eq!(prune.status(), MaintenanceOutcomeStatus::Completed);
    // The unattested snapshot and (#3643) the timeline tail it sealed.
    assert_eq!(prune.state_changes(), 2);
    assert_eq!(backend.snapshot_objects(), vec![snapshot_object(1)]);
    assert_eq!(attested_snapshot_id(backend), Some(1));
    let view = reopened.read_view().expect("view");
    for (key, value) in [(&first_key, b"value-a" as &[u8]), (&second_key, b"value-b")] {
        assert_eq!(
            view.latest(key)
                .expect("read")
                .expect("visible")
                .row()
                .value(),
            value,
            "the orphan snapshot's rows are recovered from the WAL"
        );
    }
}

#[test]
fn a_failed_snapshot_delete_keeps_the_live_snapshot_and_leaves_the_rest_for_the_next_prune() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa8);
    let mut runtime = open_runtime(branch, backend);
    for (snapshot_id, key) in [
        (1, b"fault-a" as &'static [u8]),
        (2, b"fault-b" as &'static [u8]),
        (3, b"fault-c" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        let checkpoint = checkpoint_through_sync_runner(&mut runtime, snapshot_id);
        assert_eq!(checkpoint.status(), MaintenanceOutcomeStatus::Completed);
    }
    assert_eq!(
        backend.snapshot_objects(),
        sorted_snapshot_objects([1, 2, 3])
    );
    // The prune lists ascending: the first delete (snapshot 1) fails.
    backend.fail_delete_on_call(backend.delete_calls() + 1);

    let prune = drain_snapshot_prune(&mut runtime);

    assert_eq!(prune.status(), MaintenanceOutcomeStatus::Completed);
    // Snapshot 2, plus (#3643) the timeline tails of snapshots 1 and 2: the
    // failed snapshot delete does not keep its unreferenced tail.
    assert_eq!(prune.state_changes(), 3);
    assert!(
        prune.recovery_health().is_some(),
        "the failed delete is health debt"
    );
    assert_eq!(backend.snapshot_objects(), sorted_snapshot_objects([1, 3]));
    assert_eq!(attested_snapshot_id(backend), Some(3));

    // The next prune finishes the job.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::snapshot_pruning_with_mode(
            SnapshotPruneMode::Superseded,
        ))
        .expect("enqueue prune");
    let retry = drain_snapshot_prune(&mut runtime);

    assert_eq!(retry.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(retry.state_changes(), 1);
    assert_eq!(backend.snapshot_objects(), vec![snapshot_object(3)]);
}

/// #3643 (re-review P2): a never-checkpointed store's reopen queues the
/// reconcile too — it cannot prune snapshots (none is attested) but it reclaims
/// any timeline segment a dying first checkpoint left. It completes without
/// telemetry debt and, with nothing to reclaim, deletes nothing.
#[test]
fn reopen_of_a_never_checkpointed_store_queues_a_segment_only_reconcile() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xa9);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"never-checkpointed", b"value"),
            generation_guard(),
        )
        .expect("commit");
    assert_eq!(attested_snapshot_id(backend), None);
    drop(runtime);

    let mut reopened = open_runtime(branch, backend);

    assert_eq!(pending_snapshot_prunes(&reopened), 1);
    let mut reconciled = 0;
    while let Some(outcome) = reopened
        .run_next_retention_maintenance()
        .expect("retention lane")
    {
        if outcome.task_kind() == MaintenanceTaskKind::SnapshotPruning {
            reconciled += 1;
            assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
            assert_eq!(outcome.state_changes(), 0, "nothing to reclaim");
            assert!(outcome.recovery_health().is_none(), "no telemetry debt");
        }
    }
    assert_eq!(reconciled, 1);
    assert!(backend.snapshot_objects().is_empty());
}

/// #3612: the first snapshot id a reopened runtime allocates is one past the
/// attested id AND past any object on disk.
#[test]
fn next_checkpoint_snapshot_id_after_open_truth_table() {
    use crate::lifecycle::checkpoint::next_checkpoint_snapshot_id_after_open;
    for (attested, listed, expected) in [
        (None, None, Some(1)),
        (Some(1), None, Some(2)),
        (None, Some(1), Some(2)),
        (Some(1), Some(1), Some(2)),
        (Some(1), Some(2), Some(3)),
        (Some(3), Some(2), Some(4)),
        (Some(u64::MAX), None, None),
        (None, Some(u64::MAX), None),
    ] {
        assert_eq!(
            next_checkpoint_snapshot_id_after_open(attested, listed),
            expected,
            "attested={attested:?} listed={listed:?}"
        );
    }
}

/// A crash between the snapshot publish and the manifest re-point leaves an
/// orphan at attested+1. The next open seeds its allocator past every object
/// on disk (#3612), so a runtime-allocated checkpoint completes at once and
/// attests attested+2; the reconcile then removes both the superseded and the
/// orphan object.
#[test]
fn reopen_allocates_past_the_snapshot_id_a_crash_orphan_occupies() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xaa);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"occupied-a", b"value-a"),
            generation_guard(),
        )
        .expect("first commit");
    let first = checkpoint_through_sync_runner(&mut runtime, 1);
    assert_eq!(first.status(), MaintenanceOutcomeStatus::Completed);
    drain_snapshot_prune(&mut runtime);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"occupied-b", b"value-b"),
            generation_guard(),
        )
        .expect("second commit");
    backend.fail_manifest_replacement_on_call(manifest_replacements_so_far(backend) + 2);
    let request =
        LifecycleCheckpointRequest::new(branch, 2, Timestamp::from_micros(44)).expect("request");
    let outcome = runtime.checkpoint(&request).expect("partial checkpoint");
    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::SnapshotPublishedManifestNotUpdated
    );
    drop(runtime);

    let mut reopened = open_runtime(branch, backend);
    assert_eq!(backend.snapshot_objects(), sorted_snapshot_objects([1, 2]));
    reopened
        .execute_durable_commit(
            durable_batch(branch, b"occupied-c", b"value-c"),
            generation_guard(),
        )
        .expect("third commit");
    // The allocator was seeded past the orphan: the checkpoint completes at
    // once, before any reconcile, at id 3.
    reopened
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint_with_options(
            MaintenanceCheckpointOptions::new(None, false),
        ))
        .expect("enqueue checkpoint");
    let completed = reopened
        .run_next_checkpoint_maintenance()
        .expect("run checkpoint")
        .expect("checkpoint outcome");
    assert_eq!(
        completed.status(),
        MaintenanceOutcomeStatus::Completed,
        "{completed:?}"
    );
    assert_eq!(attested_snapshot_id(backend), Some(3));
    assert_eq!(
        backend.snapshot_objects(),
        sorted_snapshot_objects([1, 2, 3])
    );

    // The reconcile (queued at open) removes the superseded object and the
    // orphan alike; the chained superseded prune coalesces with it.
    let prune = drain_snapshot_prune(&mut reopened);
    assert_eq!(prune.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(backend.snapshot_objects(), vec![snapshot_object(3)]);
}

/// The reopen reconcile can never run against a family whose attested object
/// is gone: recovery refuses the store first (`corruption.lifecycle.
/// recovery_corruption`), so the remaining objects — the superseded one and
/// a crash orphan — are left exactly as found for the operator.
#[test]
fn reopen_with_the_attested_snapshot_missing_is_refused_before_any_prune() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xab);
    let mut runtime = open_runtime(branch, backend);
    for (snapshot_id, key) in [
        (1, b"missing-a" as &'static [u8]),
        (2, b"missing-b" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        let checkpoint = checkpoint_through_sync_runner(&mut runtime, snapshot_id);
        assert_eq!(checkpoint.status(), MaintenanceOutcomeStatus::Completed);
    }
    let live_bytes = backend
        .object_snapshot()
        .remove(&snapshot_object(2))
        .expect("live snapshot bytes");
    backend.replace_object_bytes(&snapshot_object(5), live_bytes);
    drop(runtime);
    backend
        .delete_object(&snapshot_object(2))
        .expect("delete attested snapshot");
    assert_eq!(backend.snapshot_objects(), sorted_snapshot_objects([1, 5]));
    let deletes_before = backend.delete_calls();

    let mut shell = super::checkpoint::shared::assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("a missing attested snapshot refuses recovery");

    assert_eq!(error.code(), "corruption.lifecycle.recovery_corruption");
    assert_eq!(backend.delete_calls(), deletes_before);
    assert_eq!(backend.snapshot_objects(), sorted_snapshot_objects([1, 5]));
}

/// Slice 5 (#3592): under data-loss recovery health the prune never lists
/// the family — it defers as a snapshot-prune outcome carrying the health,
/// and every object stays put for the operator.
#[test]
fn superseded_prune_defers_under_data_loss_health_without_listing() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = durable_branch_id(0xac);
    let mut runtime = open_runtime(branch, backend);
    for (snapshot_id, key) in [
        (1, b"data-loss-a" as &'static [u8]),
        (2, b"data-loss-b" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        let checkpoint = checkpoint_through_sync_runner(&mut runtime, snapshot_id);
        assert_eq!(checkpoint.status(), MaintenanceOutcomeStatus::Completed);
    }
    assert_eq!(pending_snapshot_prunes(&runtime), 1);
    runtime.record_recovery_health_for_test(&data_loss_health());
    let listings_before = backend
        .events()
        .iter()
        .filter(|event| matches!(event, CheckpointBackendEvent::ObjectList))
        .count();
    let deletes_before = backend.delete_calls();

    let prune = drain_snapshot_prune(&mut runtime);

    assert_eq!(prune.task_kind(), MaintenanceTaskKind::SnapshotPruning);
    assert_eq!(prune.status(), MaintenanceOutcomeStatus::Deferred);
    assert!(
        prune.recovery_health().is_some(),
        "the deferral carries the health"
    );
    assert_eq!(prune.state_changes(), 0);
    assert_eq!(
        backend
            .events()
            .iter()
            .filter(|event| matches!(event, CheckpointBackendEvent::ObjectList))
            .count(),
        listings_before,
        "a blocked prune never lists the snapshot family"
    );
    assert_eq!(backend.delete_calls(), deletes_before);
    assert_eq!(backend.snapshot_objects(), sorted_snapshot_objects([1, 2]));
}

/// #3643: the newest-N verb carries no segment prune (it keeps older
/// snapshots on purpose); each proof-driven mode carries its own rule.
#[test]
fn timeline_segment_prune_mode_truth_table() {
    use crate::lifecycle::retention::timeline_segment_prune_mode;
    use crate::service::TimelineSegmentPruneMode;
    assert_eq!(
        timeline_segment_prune_mode(SnapshotPruneMode::RetainNewest),
        None
    );
    assert_eq!(
        timeline_segment_prune_mode(SnapshotPruneMode::Superseded),
        Some(TimelineSegmentPruneMode::Superseded)
    );
    assert_eq!(
        timeline_segment_prune_mode(SnapshotPruneMode::ReconcileToAttested),
        Some(TimelineSegmentPruneMode::ReconcileToAttested)
    );
}

fn timeline_segment_name(sealing_snapshot_id: u64, ordinal: u64) -> ObjectName {
    ObjectLayout::timeline_segment(crate::layout::TimelineSegmentId {
        sealing_snapshot_id,
        ordinal,
    })
    .expect("segment name")
}

/// #3643: a proof-driven prune with the live snapshot referencing `referenced`.
fn segment_pruning(
    backend: &RetentionBackend,
    live_snapshot_id: u64,
    mode: SnapshotPruneMode,
    referenced: &[(u64, u64)],
) -> LifecycleSnapshotPruningOutcome {
    let request = LifecycleRetentionRequest::snapshot_pruning(1).with_snapshot_prune_mode(mode);
    let proof = build_retention_proof(
        &request,
        Some(&manifest(live_snapshot_id, 7)),
        &RecoveryHealth::Healthy,
        backend.remaining_snapshot_ids().len(),
    );
    let pruning =
        LifecycleSnapshotPruningRequest::for_request(proof, &request).expect("pruning request");
    let referenced = referenced
        .iter()
        .map(
            |(sealing_snapshot_id, ordinal)| crate::layout::TimelineSegmentId {
                sealing_snapshot_id: *sealing_snapshot_id,
                ordinal: *ordinal,
            },
        )
        .collect();
    prune_snapshots_with_proof(&SnapshotService::new(backend), &pruning, Some(&referenced))
        .expect("outcome")
}

/// #3643: `Superseded` deletes exactly the segments the live snapshot does not
/// reference AND that were sealed below it; a prune that deletes only a
/// segment reclaimed something (it is not a no-op).
#[test]
fn superseded_prune_deletes_only_unreferenced_segments_sealed_below_live() {
    let backend: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3]));
    for (sealing, ordinal) in [(1, 0), (2, 0), (3, 0), (5, 0)] {
        backend.insert_timeline_segment(sealing, ordinal);
    }

    let outcome = segment_pruning(backend, 3, SnapshotPruneMode::Superseded, &[(2, 0)]);

    assert!(outcome.completed());
    assert!(outcome.deleted().is_empty(), "the live snapshot stays");
    assert_eq!(
        backend.remaining_timeline_segments(),
        [(2, 0), (3, 0), (5, 0)].map(|(s, o)| timeline_segment_name(s, o))
    );
    assert!(!outcome.completed_noop(), "a segment-only prune reclaimed");
    assert!(!outcome.completed_with_health_debt());
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.state_changes(), 1);
    assert_eq!(maintenance.bytes_reclaimed(), b"segment".len() as u64);
}

/// #3643: a prune with nothing to delete among snapshots or segments is a
/// no-op, and `ReconcileToAttested` deletes every unreferenced segment.
#[test]
fn segment_prune_modes_on_both_sides_of_their_rules() {
    let quiet: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3]));
    quiet.insert_timeline_segment(3, 0);
    let outcome = segment_pruning(quiet, 3, SnapshotPruneMode::Superseded, &[(3, 0)]);
    assert!(outcome.completed_noop());

    let reconciled: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3]));
    for (sealing, ordinal) in [(1, 0), (3, 0), (5, 0)] {
        reconciled.insert_timeline_segment(sealing, ordinal);
    }
    let outcome = segment_pruning(
        reconciled,
        3,
        SnapshotPruneMode::ReconcileToAttested,
        &[(3, 0)],
    );
    assert_eq!(
        reconciled.remaining_timeline_segments(),
        [timeline_segment_name(3, 0)]
    );
    assert_eq!(outcome.maintenance_outcome().state_changes(), 2);

    // The newest-N verb deletes no segment.
    let explicit: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3]));
    explicit.insert_timeline_segment(1, 0);
    segment_pruning(explicit, 3, SnapshotPruneMode::RetainNewest, &[]);
    assert_eq!(explicit.remaining_timeline_segments().len(), 1);
}

/// #3643: a segment that vanished between the listing and its delete is
/// neither deleted nor a failure; one whose delete fails is health debt.
#[test]
fn a_vanished_segment_is_not_a_failure_but_a_failed_delete_is_health_debt() {
    let vanished: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3]));
    vanished.insert_phantom(timeline_segment_name(1, 0));
    let outcome = segment_pruning(vanished, 3, SnapshotPruneMode::Superseded, &[]);
    assert!(outcome.completed_noop(), "nothing deleted, nothing failed");
    assert!(outcome.recovery_health().is_none());

    let failing: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3]));
    failing.insert_timeline_segment(1, 0);
    failing.fail_delete_on_call(1);
    let outcome = segment_pruning(failing, 3, SnapshotPruneMode::Superseded, &[]);
    assert!(outcome.completed_with_health_debt());
    assert!(!outcome.completed_noop());
    assert_eq!(
        outcome.recovery_health().map(RecoveryHealth::fault_count),
        Some(1)
    );
    assert_eq!(failing.remaining_timeline_segments().len(), 1);

    // A delete the backend cannot confirm durable is not a reclaim: it is
    // health debt, and no bytes are credited for it.
    let non_durable: &'static RetentionBackend =
        crate::testkit::leak_static(RetentionBackend::with_snapshots([3]));
    non_durable.insert_timeline_segment(1, 0);
    non_durable
        .non_durable_deletes
        .store(true, Ordering::SeqCst);
    let outcome = segment_pruning(non_durable, 3, SnapshotPruneMode::Superseded, &[]);
    assert!(outcome.completed_with_health_debt());
    assert_eq!(
        outcome.recovery_health().map(RecoveryHealth::fault_count),
        Some(1)
    );
    assert_eq!(outcome.maintenance_outcome().bytes_reclaimed(), 0);
}

/// #3643 (re-review P2): the snapshot-less segment reconcile acts only for the
/// open reconcile mode, on a proof incomplete solely because no snapshot is
/// attested — never on a complete proof, a health-blocked one, another missing
/// fact, or a proof naming a live snapshot.
#[test]
fn snapshotless_segment_reconcile_truth_table() {
    use crate::lifecycle::retention::{snapshotless_segment_reconcile, LifecycleRetentionProof};
    let proof = |status, live: Option<u64>, missing: Option<&'static str>| {
        LifecycleRetentionProof::new(status, RecoveryHealth::Healthy, live, None, None, missing)
    };
    let incomplete = LifecycleRetentionProofStatus::Incomplete;
    for (proof, mode, expected) in [
        (
            proof(incomplete, None, Some("manifest_snapshot")),
            SnapshotPruneMode::ReconcileToAttested,
            true,
        ),
        (
            proof(incomplete, None, Some("manifest_snapshot")),
            SnapshotPruneMode::Superseded,
            false,
        ),
        (
            proof(incomplete, None, Some("manifest_snapshot")),
            SnapshotPruneMode::RetainNewest,
            false,
        ),
        (
            proof(incomplete, Some(3), Some("manifest_snapshot")),
            SnapshotPruneMode::ReconcileToAttested,
            false,
        ),
        (
            proof(incomplete, None, Some("quarantine_inventory")),
            SnapshotPruneMode::ReconcileToAttested,
            false,
        ),
        (
            proof(LifecycleRetentionProofStatus::Complete, None, None),
            SnapshotPruneMode::ReconcileToAttested,
            false,
        ),
        (
            proof(
                LifecycleRetentionProofStatus::BlockedByRecoveryHealth,
                None,
                Some("recovery_health"),
            ),
            SnapshotPruneMode::ReconcileToAttested,
            false,
        ),
    ] {
        assert_eq!(
            snapshotless_segment_reconcile(&proof, mode),
            expected,
            "{proof:?} {mode:?}"
        );
    }
}
