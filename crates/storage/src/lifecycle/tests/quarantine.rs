use super::checkpoint::shared::{open_runtime as open_durable_runtime, CheckpointTestBackend};
use super::*;
use crate::backend::{
    Backend, BackendCapabilities, BackendCapability, BackendError, BackendErrorKind,
    BackendMetadata, BackendRange, BackendResult, DeleteDurability, DeleteOutcome,
    PublishDurability, PublishError, PublishFailureKind, PublishMode, PublishOutcome,
};
use crate::layout::{ObjectFamily, ObjectLayout};
use crate::object::{ObjectName, ObjectPrefix};
use crate::service::QuarantineService;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;
use strata_core::{BranchId, Timestamp};

const DATABASE_ID: [u8; 16] = [0x6d; 16];

#[test]
fn quarantine_proof_complete_from_candidate_and_blocks_unsafe_health() {
    let safe = LifecycleQuarantineProof::from_retention_decision(
        RetentionDecision::QuarantineCandidate,
        RecoveryHealth::Healthy,
    );
    assert_eq!(safe.status(), LifecycleQuarantineProofStatus::CompleteSafe);
    assert!(!safe.source_reachable());

    let referenced = LifecycleQuarantineProof::from_retention_decision(
        RetentionDecision::Retain,
        RecoveryHealth::Healthy,
    );
    assert_eq!(
        referenced.status(),
        LifecycleQuarantineProofStatus::Referenced
    );
    assert!(referenced.source_reachable());

    let blocked = LifecycleQuarantineProof::from_retention_decision(
        RetentionDecision::QuarantineCandidate,
        RecoveryHealth::degraded(
            RecoveryDegradationClass::DataLoss,
            vec![
                RecoveryFault::new(RecoveryFaultKind::MissingTableObject, "missing")
                    .expect("fault"),
            ],
        )
        .expect("health"),
    );
    assert_eq!(
        blocked.status(),
        LifecycleQuarantineProofStatus::BlockedByRecoveryHealth
    );
    assert_eq!(blocked.missing_fact(), Some("recovery_health"));
}

#[test]
fn quarantine_incomplete_proof_defers_without_backend_access() {
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source_object(), b"table-bytes"),
    );
    let request = quarantine_request(
        LifecycleQuarantineProof::from_retention_decision(
            RetentionDecision::SkipUntilProof,
            RecoveryHealth::Healthy,
        ),
        source_object(),
    );

    let outcome = quarantine_object(&QuarantineService::new(backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::DeferredIncompleteProof
    );
    assert_eq!(backend.operations(), Vec::<Operation>::new());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Deferred
    );
}

#[test]
fn quarantine_request_rejects_reserved_source_and_identity_shapes() {
    let proof = LifecycleQuarantineProof::safe(RecoveryHealth::Healthy);
    let source = source_object();

    assert_eq!(
        LifecycleQuarantineRequest::new(
            branch_id(),
            DATABASE_ID,
            LifecycleCodecId::identity(),
            ObjectLayout::quarantine_inventory_object_id(),
            source.clone(),
            Timestamp::from_micros(5),
            proof.clone(),
        )
        .expect_err("reserved object id rejects"),
        LifecycleError::InvalidConfig {
            field: "quarantine_object_id",
            reason: "must not be the quarantine inventory object id",
        }
    );

    assert_eq!(
        LifecycleQuarantineRequest::new(
            branch_id(),
            DATABASE_ID,
            LifecycleCodecId::identity(),
            "",
            source_object(),
            Timestamp::from_micros(5),
            proof.clone(),
        )
        .expect_err("empty object id rejects"),
        LifecycleError::InvalidConfig {
            field: "quarantine_object_id",
            reason: "must not be empty",
        }
    );

    assert_eq!(
        LifecycleQuarantineRequest::new(
            branch_id(),
            DATABASE_ID,
            LifecycleCodecId::identity(),
            "candidate",
            source_object(),
            Timestamp::EPOCH,
            proof.clone(),
        )
        .expect_err("epoch timestamp rejects"),
        LifecycleError::InvalidConfig {
            field: "quarantine_timestamp",
            reason: "must not be epoch",
        }
    );

    assert_eq!(
        LifecycleQuarantineRequest::new(
            branch_id(),
            [0; 16],
            LifecycleCodecId::identity(),
            "candidate",
            source,
            Timestamp::from_micros(5),
            proof.clone(),
        )
        .expect_err("zero database id rejects"),
        LifecycleError::InvalidConfig {
            field: "database_id",
            reason: "must not be zero",
        }
    );

    let quarantine_source =
        ObjectLayout::quarantine_object(&branch_id().to_string(), "candidate").expect("object");
    assert_eq!(
        LifecycleQuarantineRequest::new(
            branch_id(),
            DATABASE_ID,
            LifecycleCodecId::identity(),
            "candidate",
            quarantine_source,
            Timestamp::from_micros(5),
            proof,
        )
        .expect_err("quarantine source rejects"),
        LifecycleError::InvalidConfig {
            field: "source_object",
            reason: "must name a non-quarantine storage object",
        }
    );
}

#[test]
fn quarantine_proof_allows_unrelated_telemetry_debt() {
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![
            RecoveryFault::new(RecoveryFaultKind::WalTailRepairFailed, "tail repaired")
                .expect("fault"),
        ],
    )
    .expect("telemetry health");

    let proof = LifecycleQuarantineProof::safe(health);

    assert_eq!(proof.status(), LifecycleQuarantineProofStatus::CompleteSafe);
}

#[test]
fn quarantine_proof_blocks_when_telemetry_debt_targets_candidate_branch() {
    let candidate = strata_core::BranchId::from_bytes([0x66; 16]);
    let other = strata_core::BranchId::from_bytes([0x77; 16]);

    let health_targeting_other = RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![RecoveryFault::new(
            RecoveryFaultKind::QuarantineInventoryMismatch,
            "branch-scoped telemetry debt",
        )
        .expect("fault")
        .with_affected_branch(other)],
    )
    .expect("telemetry health");
    let unrelated = LifecycleQuarantineProof::safe_for_candidate(health_targeting_other, candidate);
    assert_eq!(
        unrelated.status(),
        LifecycleQuarantineProofStatus::CompleteSafe,
        "telemetry debt naming a different branch must not block this candidate"
    );

    let health_targeting_candidate = RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![RecoveryFault::new(
            RecoveryFaultKind::QuarantineInventoryMismatch,
            "branch-scoped telemetry debt",
        )
        .expect("fault")
        .with_affected_branch(candidate)],
    )
    .expect("telemetry health");
    let related =
        LifecycleQuarantineProof::safe_for_candidate(health_targeting_candidate, candidate);
    assert_eq!(
        related.status(),
        LifecycleQuarantineProofStatus::BlockedByRecoveryHealth,
        "telemetry debt naming the candidate's branch must block reclaim",
    );
}

#[test]
fn quarantine_referenced_proof_defers_without_backend_access() {
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source_object(), b"table-bytes"),
    );
    let request = quarantine_request(
        LifecycleQuarantineProof::from_retention_decision(
            RetentionDecision::Retain,
            RecoveryHealth::Healthy,
        ),
        source_object(),
    );

    let outcome = quarantine_object(&QuarantineService::new(backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::DeferredReferenced
    );
    assert_eq!(backend.operations(), Vec::<Operation>::new());
}

#[test]
fn quarantine_stages_inventory_copy_and_source_delete_in_order() {
    let source = source_object();
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source.clone(), b"table-bytes"),
    );
    let request = quarantine_request(
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
        source.clone(),
    );

    let outcome = quarantine_object(&QuarantineService::new(backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::QuarantinedSourceDeleted
    );
    assert!(!backend.contains(&source));
    assert!(outcome.quarantine_object().is_some());
    assert_eq!(
        backend.operations(),
        vec![
            Operation::Metadata(outcome.quarantine_object().expect("object").clone()),
            Operation::Metadata(source.clone()),
            Operation::Publish(
                outcome.inventory_object().expect("inventory").clone(),
                PublishMode::Replace,
            ),
            Operation::Publish(
                outcome.quarantine_object().expect("object").clone(),
                PublishMode::Create,
            ),
            Operation::Delete(source),
        ]
    );
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(maintenance.bytes_reclaimed(), b"table-bytes".len() as u64);
}

#[test]
fn quarantine_inventory_publish_failure_does_not_copy_or_delete_source() {
    let source = source_object();
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source.clone(), b"table-bytes"),
    );
    backend.fail_next_publish(PublishFailureKind::FailedBeforeVisibility);
    let request = quarantine_request(
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
        source.clone(),
    );

    let outcome = quarantine_object(&QuarantineService::new(backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::InventoryPublishFailed
    );
    assert!(backend.contains(&source));
    assert_eq!(
        backend
            .operations()
            .into_iter()
            .filter(|operation| matches!(operation, Operation::Delete(_)))
            .count(),
        0
    );
    assert!(outcome.source_error().is_some());
}

#[test]
fn quarantine_inventory_mismatch_blocks_followup_purge() {
    let source = source_object();
    let inventory =
        ObjectLayout::quarantine_manifest(&branch_id().to_string()).expect("inventory object");
    let backend = QuarantineTestBackend::durable()
        .with_object(source.clone(), b"table-bytes")
        .with_object(inventory, b"not-an-inventory");
    let request = quarantine_request(
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
        source,
    );

    let outcome = quarantine_object(&QuarantineService::new(&backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::InventoryMismatch
    );
    let health = outcome.recovery_health().expect("inventory health debt");
    assert!(matches!(
        health,
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::PolicyDowngrade,
            faults,
        } if faults.iter().any(|fault| fault.kind() == RecoveryFaultKind::QuarantineInventoryMismatch)
    ));
    let purge = purge_quarantine(
        &QuarantineService::new(&backend),
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
        &fresh_purge_proof(
            &QuarantineService::new(&QuarantineTestBackend::durable()),
            health.clone(),
        ),
    )
    .expect("purge outcome");

    assert_eq!(
        purge.status(),
        LifecyclePurgeStatus::BlockedByRecoveryHealth
    );

    let runtime_backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_durable_runtime(branch_id(), runtime_backend);
    runtime.record_recovery_health_for_test(health);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::purge_quarantine(branch_id()))
        .expect("enqueue purge");
    let runtime_purge = runtime
        .run_next_purge_maintenance()
        .expect("run purge")
        .expect("purge outcome");

    assert_eq!(runtime_purge.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(runtime_purge.reason(), Some("recovery health blocks purge"));
}

#[test]
fn quarantine_source_delete_failure_reports_retryable_health_debt() {
    let source = source_object();
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source.clone(), b"table-bytes"),
    );
    backend.fail_delete(source.clone(), BackendErrorKind::Interrupted);
    let request = quarantine_request(
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
        source,
    );

    let outcome = quarantine_object(&QuarantineService::new(backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed
    );
    assert!(outcome.source_error().is_some());
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
    assert!(maintenance.retryable());
    assert!(maintenance.recovery_health().is_some());
}

#[test]
fn quarantine_non_durable_source_delete_reports_retryable_health_debt() {
    let source = source_object();
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source.clone(), b"table-bytes"),
    );
    backend.make_delete_non_durable(source.clone());
    let request = quarantine_request(
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
        source.clone(),
    );

    let outcome = quarantine_object(&QuarantineService::new(backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed
    );
    assert!(outcome.source_error().is_some());
    assert!(!backend.contains(&source));
    assert!(backend.contains(outcome.quarantine_object().expect("quarantine object")));
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
    assert!(maintenance.retryable());
    assert!(maintenance.recovery_health().is_some());
}

#[test]
fn quarantine_existing_matching_entry_is_idempotent_and_retries_source_delete() {
    let source = source_object();
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source.clone(), b"table-bytes"),
    );
    let service = QuarantineService::new(backend);
    let request = quarantine_request(
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
        source.clone(),
    );
    let first = quarantine_object(&service, &request);
    assert_eq!(
        first.status(),
        LifecycleQuarantineStatus::QuarantinedSourceDeleted
    );

    backend
        .objects
        .lock()
        .expect("backend lock")
        .insert(source, b"table-bytes".to_vec());
    let retry = quarantine_object(&service, &request);

    assert_eq!(
        retry.status(),
        LifecycleQuarantineStatus::SourceDeleteRetried
    );
    assert_eq!(retry.quarantine_object(), first.quarantine_object());
}

#[test]
fn quarantine_missing_source_is_transient_service_failure_not_publish_failure() {
    // Missing-source maps to `ServiceTransient` after the
    // ServiceFailed split. The source may have been deleted by a
    // concurrent operation; a retry could succeed if the source returns,
    // so the maintenance outcome must report Failed-but-retryable.
    let source = source_object();
    let backend: &'static QuarantineTestBackend =
        crate::testkit::leak_static(QuarantineTestBackend::durable());
    let request = quarantine_request(
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
        source,
    );

    let outcome = quarantine_object(&QuarantineService::new(backend), &request);

    assert_eq!(
        outcome.status(),
        LifecycleQuarantineStatus::ServiceTransient,
        "missing source must classify as transient backend failure",
    );
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
    assert!(
        maintenance.retryable(),
        "transient service failures must be retryable",
    );
    assert_eq!(
        maintenance.reason(),
        Some("quarantine service transient failure"),
    );
    assert!(maintenance.source_error().is_some());
}

#[test]
fn purge_requires_fresh_proof_before_backend_access() {
    let backend: &'static QuarantineTestBackend =
        crate::testkit::leak_static(QuarantineTestBackend::durable());
    let outcome = purge_quarantine(
        &QuarantineService::new(backend),
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
        &LifecyclePurgeProof::stale(RecoveryHealth::Healthy),
    )
    .expect("purge outcome");

    assert_eq!(outcome.status(), LifecyclePurgeStatus::StaleProof);
    assert_eq!(backend.operations(), Vec::<Operation>::new());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Deferred
    );
}

#[test]
fn purge_request_rejects_missing_database_id_before_backend_access() {
    let backend: &'static QuarantineTestBackend =
        crate::testkit::leak_static(QuarantineTestBackend::durable());
    let service = QuarantineService::new(backend);
    assert_eq!(
        purge_quarantine(
            &service,
            branch_id(),
            [0; 16],
            &LifecycleCodecId::identity(),
            &fresh_purge_proof(&service, RecoveryHealth::Healthy),
        ),
        Err(LifecycleError::InvalidConfig {
            field: "database_id",
            reason: "must not be zero",
        })
    );
}

#[test]
fn purge_blocked_recovery_health_defers_before_backend_access() {
    let backend: &'static QuarantineTestBackend =
        crate::testkit::leak_static(QuarantineTestBackend::durable());
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::PolicyDowngrade,
        vec![RecoveryFault::new(
            RecoveryFaultKind::QuarantineInventoryMismatch,
            "quarantine mismatch",
        )
        .expect("fault")],
    )
    .expect("health");
    let outcome = purge_quarantine(
        &QuarantineService::new(backend),
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
        &fresh_purge_proof(&QuarantineService::new(backend), health),
    )
    .expect("purge outcome");

    assert_eq!(
        outcome.status(),
        LifecyclePurgeStatus::BlockedByRecoveryHealth
    );
    assert_eq!(backend.operations(), Vec::<Operation>::new());
}

#[test]
fn purge_deletes_inventory_listed_quarantine_objects() {
    let source = source_object();
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source, b"table-bytes"),
    );
    let service = QuarantineService::new(backend);
    let quarantine = quarantine_object(
        &service,
        &quarantine_request(
            LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
            source_object(),
        ),
    );
    let quarantine_object = quarantine.quarantine_object().expect("object").clone();

    let outcome = purge_quarantine(
        &service,
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
        &fresh_purge_proof(&service, RecoveryHealth::Healthy),
    )
    .expect("purge outcome");

    assert_eq!(outcome.status(), LifecyclePurgeStatus::Completed);
    assert_eq!(
        outcome.deleted_objects(),
        std::slice::from_ref(&quarantine_object)
    );
    assert!(!backend.contains(&quarantine_object));
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Completed
    );
    assert_eq!(outcome.reclaimed_bytes(), b"table-bytes".len() as u64);
    assert_eq!(
        outcome.maintenance_outcome().bytes_reclaimed(),
        b"table-bytes".len() as u64
    );
}

#[test]
fn purge_delete_failure_retains_entry_and_preserves_source_error() {
    let source = source_object();
    let backend: &'static QuarantineTestBackend = crate::testkit::leak_static(
        QuarantineTestBackend::durable().with_object(source, b"table-bytes"),
    );
    let service = QuarantineService::new(backend);
    let quarantine = quarantine_object(
        &service,
        &quarantine_request(
            LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
            source_object(),
        ),
    );
    let quarantine_object = quarantine.quarantine_object().expect("object").clone();
    backend.fail_delete(quarantine_object.clone(), BackendErrorKind::Interrupted);
    let outcome = purge_quarantine(
        &service,
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
        &fresh_purge_proof(&service, RecoveryHealth::Healthy),
    )
    .expect("purge outcome");

    assert_eq!(
        outcome.status(),
        LifecyclePurgeStatus::CompletedWithHealthDebt
    );
    assert_eq!(
        outcome.failed_objects(),
        std::slice::from_ref(&quarantine_object)
    );
    assert_eq!(outcome.retained_entries().len(), 1);
    assert!(outcome.source_error().is_some());
    assert!(outcome.maintenance_outcome().source_error().is_some());
}

#[test]
fn repair_reports_unlisted_quarantine_object_as_health_debt() {
    let backend = QuarantineTestBackend::durable().with_object(
        ObjectLayout::quarantine_object(&branch_id().to_string(), "orphan").expect("object"),
        b"orphaned",
    );
    let outcome = repair_branch_quarantine(
        &QuarantineService::new(&backend),
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
    )
    .expect("repair");

    assert!(outcome.completed_with_health_debt());
    assert_eq!(outcome.unlisted_objects(), 1);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Completed
    );
}

#[test]
fn repair_family_reports_malformed_quarantine_object_facts() {
    let backend = QuarantineTestBackend::durable()
        .with_object(malformed_family_quarantine_object(), b"malformed");
    let outcome = repair_quarantine_family(
        &QuarantineService::new(&backend),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
    )
    .expect("repair");

    assert!(outcome.completed_with_health_debt());
    assert_eq!(outcome.report_count(), 0);
    assert_eq!(outcome.malformed_objects(), 1);
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Completed
    );
    assert_eq!(outcome.maintenance_outcome().affected_objects(), 1);
}

#[test]
fn repair_request_rejects_missing_database_id_before_backend_access() {
    assert_eq!(
        repair_branch_quarantine(
            &QuarantineService::new(&QuarantineTestBackend::durable()),
            branch_id(),
            [0; 16],
            &LifecycleCodecId::identity(),
        ),
        Err(LifecycleError::InvalidConfig {
            field: "database_id",
            reason: "must not be zero",
        })
    );
    assert_eq!(
        repair_quarantine_family(
            &QuarantineService::new(&QuarantineTestBackend::durable()),
            [0; 16],
            &LifecycleCodecId::identity(),
        ),
        Err(LifecycleError::InvalidConfig {
            field: "database_id",
            reason: "must not be zero",
        })
    );
}

#[test]
fn repair_backend_unavailable_preserves_source_chain() {
    let backend: &'static QuarantineTestBackend =
        crate::testkit::leak_static(QuarantineTestBackend::durable());
    backend.fail_list(BackendErrorKind::Interrupted);
    let outcome = repair_branch_quarantine(
        &QuarantineService::new(backend),
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
    )
    .expect("repair");

    assert!(outcome.backend_unavailable());
    assert!(outcome.source_error().is_some());
    assert!(outcome.maintenance_outcome().source_error().is_some());
}

#[test]
fn repair_mutation_is_rejected_before_backend_access() {
    let backend: &'static QuarantineTestBackend =
        crate::testkit::leak_static(QuarantineTestBackend::durable());
    let error = crate::lifecycle::quarantine::repair_branch_quarantine_with_mutation_for_test(
        &QuarantineService::new(backend),
        branch_id(),
        DATABASE_ID,
        &LifecycleCodecId::identity(),
    )
    .expect_err("mutation reject");

    assert_eq!(
        error,
        LifecycleError::QuarantineRepairInconclusive {
            reason: "mutating quarantine repair is not supported",
            source: None,
        }
    );
    assert_eq!(backend.operations(), Vec::<Operation>::new());
}

#[test]
fn purge_and_repair_maintenance_requests_preserve_branch_scope() {
    let purge = MaintenanceTaskRequest::purge_quarantine(branch_id());
    let repair = MaintenanceTaskRequest::repair_quarantine(branch_id());

    assert_eq!(purge.kind(), MaintenanceTaskKind::Purge);
    assert_eq!(purge.scope(), MaintenanceTaskScope::Branch(branch_id()));
    assert_eq!(repair.kind(), MaintenanceTaskKind::Repair);
    assert_eq!(repair.scope(), MaintenanceTaskScope::Branch(branch_id()));
}

#[test]
fn queued_quarantine_task_drains_as_deferred_without_service_payload() {
    let task =
        MaintenanceTask::new_for_test(1, MaintenanceTaskRequest::quarantine()).expect("task");
    let outcome = quarantine_task_without_request();

    assert_eq!(task.kind(), MaintenanceTaskKind::Quarantine);
    assert_eq!(task.scope(), MaintenanceTaskScope::Quarantine);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        outcome.reason(),
        Some("quarantine task requires an explicit quarantine request")
    );
}

#[test]
fn durable_quarantine_runs_through_runtime_maintenance_surface() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_durable_runtime(branch_id(), backend);

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::quarantine())
        .expect("enqueue quarantine");
    let outcome = runtime
        .run_next_quarantine_maintenance()
        .expect("run quarantine")
        .expect("outcome");

    // The quarantine task is the table-object sweep: with nothing unreachable it completes
    // trivially (it no longer defers awaiting an "explicit quarantine request" — the sweep
    // computes its own mark).
    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Quarantine);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(outcome.reason(), Some("no unreachable table objects"));
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn durable_purge_runs_through_runtime_maintenance_surface() {
    let branch = branch_id();
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    super::checkpoint::shared::seed_database_manifest(backend);
    let source =
        ObjectLayout::table_object(&branch.to_string(), 0, "runtime-purge").expect("source object");
    backend
        .write_object(&source, b"runtime-table")
        .expect("source write");
    let mut runtime = open_durable_runtime(branch, backend);
    // Recovery over the staged (existing) store may schedule its own
    // maintenance; the assertion below is relative to that baseline.
    let baseline_pending = runtime.maintenance_status().pending_tasks();
    let request = LifecycleQuarantineRequest::from_source_object(
        branch,
        [0x7d; 16],
        LifecycleCodecId::identity(),
        source,
        Timestamp::from_micros(15),
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
    )
    .expect("quarantine request");
    let staged = runtime
        .quarantine_object(&request)
        .expect("runtime quarantine");
    assert_eq!(
        staged.status(),
        LifecycleQuarantineStatus::QuarantinedSourceDeleted
    );

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::purge_quarantine(branch))
        .expect("enqueue purge");
    let outcome = runtime
        .run_next_purge_maintenance()
        .expect("run purge")
        .expect("outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Purge);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(outcome.bytes_reclaimed(), b"runtime-table".len() as u64);
    assert_eq!(
        runtime.maintenance_status().pending_tasks(),
        baseline_pending
    );
}

#[test]
fn durable_purge_uses_current_recovery_health_not_open_snapshot() {
    let branch = branch_id();
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_durable_runtime(branch, backend);
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::PolicyDowngrade,
        vec![RecoveryFault::new(
            RecoveryFaultKind::QuarantineInventoryMismatch,
            "runtime inventory mismatch",
        )
        .expect("fault")],
    )
    .expect("health");
    runtime.record_recovery_health_for_test(&health);
    assert_eq!(runtime.current_recovery_health(), &health);

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::purge_quarantine(branch))
        .expect("enqueue purge");
    let outcome = runtime
        .run_next_purge_maintenance()
        .expect("run purge")
        .expect("outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Purge);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(outcome.reason(), Some("recovery health blocks purge"));
}

#[test]
fn durable_repair_runs_through_runtime_maintenance_surface() {
    let branch = branch_id();
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    super::checkpoint::shared::seed_database_manifest(backend);
    let orphan =
        ObjectLayout::quarantine_object(&branch.to_string(), "runtime-orphan").expect("object");
    backend
        .write_object(&orphan, b"orphan")
        .expect("orphan write");
    let mut runtime = open_durable_runtime(branch, backend);
    // Recovery over the staged (existing) store may schedule its own
    // maintenance; the assertion below is relative to that baseline.
    let baseline_pending = runtime.maintenance_status().pending_tasks();

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::repair_quarantine(branch))
        .expect("enqueue repair");
    let outcome = runtime
        .run_next_quarantine_repair_maintenance()
        .expect("run repair")
        .expect("outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Repair);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(
        runtime.maintenance_status().pending_tasks(),
        baseline_pending
    );
}

#[test]
fn quarantine_errors_have_stable_codes() {
    assert_eq!(
        LifecycleError::QuarantineProofBlocked { reason: "blocked" }.code(),
        "failed_precondition.lifecycle.quarantine"
    );
    assert_eq!(
        LifecycleError::PurgeProofBlocked { reason: "stale" }.code(),
        "failed_precondition.lifecycle.purge"
    );
    assert_eq!(
        LifecycleError::QuarantineRepairInconclusive {
            reason: "repair",
            source: None,
        }
        .code(),
        "failed_precondition.lifecycle.quarantine_repair"
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Operation {
    Metadata(ObjectName),
    Publish(ObjectName, PublishMode),
    Delete(ObjectName),
}

#[derive(Debug)]
struct QuarantineTestBackend {
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    delete_failures: Mutex<BTreeMap<ObjectName, BackendErrorKind>>,
    non_durable_deletes: Mutex<BTreeSet<ObjectName>>,
    publish_failures: Mutex<VecDeque<PublishFailureKind>>,
    list_failure: Mutex<Option<BackendErrorKind>>,
    operations: Mutex<Vec<Operation>>,
}

impl QuarantineTestBackend {
    fn durable() -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            delete_failures: Mutex::new(BTreeMap::new()),
            non_durable_deletes: Mutex::new(BTreeSet::new()),
            publish_failures: Mutex::new(VecDeque::new()),
            list_failure: Mutex::new(None),
            operations: Mutex::new(Vec::new()),
        }
    }

    fn with_object(self, object: ObjectName, bytes: &[u8]) -> Self {
        self.objects
            .lock()
            .expect("backend lock")
            .insert(object, bytes.to_vec());
        self
    }

    fn fail_delete(&self, object: ObjectName, kind: BackendErrorKind) {
        self.delete_failures
            .lock()
            .expect("backend lock")
            .insert(object, kind);
    }

    fn make_delete_non_durable(&self, object: ObjectName) {
        self.non_durable_deletes
            .lock()
            .expect("backend lock")
            .insert(object);
    }

    fn fail_next_publish(&self, kind: PublishFailureKind) {
        self.publish_failures
            .lock()
            .expect("backend lock")
            .push_back(kind);
    }

    fn fail_list(&self, kind: BackendErrorKind) {
        *self.list_failure.lock().expect("backend lock") = Some(kind);
    }

    fn contains(&self, object: &ObjectName) -> bool {
        self.objects
            .lock()
            .expect("backend lock")
            .contains_key(object)
    }

    fn operations(&self) -> Vec<Operation> {
        self.operations.lock().expect("backend lock").clone()
    }
}

impl Backend for QuarantineTestBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::from_slice(&[
            BackendCapability::ReadObject,
            BackendCapability::ReadRange,
            BackendCapability::WriteObject,
            BackendCapability::DeleteObject,
            BackendCapability::ListPrefix,
            BackendCapability::ObjectMetadata,
            BackendCapability::DurablePublish,
            BackendCapability::DurableSync,
        ])
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.objects
            .lock()
            .expect("backend lock")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        let bytes = self.read_object(name)?;
        let start = usize::try_from(range.offset())
            .map_err(|_| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
        let end = usize::try_from(range.end_offset().unwrap_or(u64::MAX))
            .map_err(|_| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
        Ok(bytes[start.min(bytes.len())..end.min(bytes.len())].to_vec())
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("backend lock")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        self.operations
            .lock()
            .expect("backend lock")
            .push(Operation::Delete(name.clone()));
        if let Some(kind) = self
            .delete_failures
            .lock()
            .expect("backend lock")
            .get(name)
            .copied()
        {
            return crate::backend::failed_delete_result(
                name,
                BackendError::new(kind, "delete failed"),
            );
        }
        let durability = if self
            .non_durable_deletes
            .lock()
            .expect("backend lock")
            .remove(name)
        {
            DeleteDurability::NonDurable
        } else {
            DeleteDurability::Durable
        };
        let removed = self
            .objects
            .lock()
            .expect("backend lock")
            .remove(name)
            .is_some();
        Ok(DeleteOutcome::from_removed(
            name.clone(),
            durability,
            removed,
        ))
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        if let Some(kind) = *self.list_failure.lock().expect("backend lock") {
            return Err(BackendError::new(kind, "list failed"));
        }
        let mut names: Vec<_> = self
            .objects
            .lock()
            .expect("backend lock")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.operations
            .lock()
            .expect("backend lock")
            .push(Operation::Metadata(name.clone()));
        self.objects
            .lock()
            .expect("backend lock")
            .get(name)
            .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> Result<PublishOutcome, PublishError> {
        self.operations
            .lock()
            .expect("backend lock")
            .push(Operation::Publish(name.clone(), mode));
        if let Some(kind) = self
            .publish_failures
            .lock()
            .expect("backend lock")
            .pop_front()
        {
            return Err(PublishError::new(
                name.clone(),
                kind,
                BackendError::new(BackendErrorKind::Interrupted, "publish failed"),
            ));
        }
        let mut objects = self.objects.lock().expect("backend lock");
        if mode == PublishMode::Create && objects.contains_key(name) {
            return Err(PublishError::new(
                name.clone(),
                PublishFailureKind::PreconditionFailed,
                BackendError::new(BackendErrorKind::AlreadyExists, "already exists"),
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

fn branch_id() -> BranchId {
    BranchId::from_bytes([0x42; 16])
}

fn source_object() -> ObjectName {
    ObjectLayout::table_object(&branch_id().to_string(), 0, "source").expect("table object")
}

fn malformed_family_quarantine_object() -> ObjectName {
    ObjectName::new(format!(
        "{}/{}/table0001",
        ObjectFamily::Quarantine.as_str(),
        "not-a-branch"
    ))
    .expect("malformed branch quarantine object")
}

fn quarantine_request(
    proof: LifecycleQuarantineProof,
    source_object: ObjectName,
) -> LifecycleQuarantineRequest {
    LifecycleQuarantineRequest::from_source_object(
        branch_id(),
        DATABASE_ID,
        LifecycleCodecId::identity(),
        source_object,
        Timestamp::from_micros(5),
        proof,
    )
    .expect("quarantine request")
}

fn fresh_purge_proof(
    service: &QuarantineService<'_>,
    recovery_health: RecoveryHealth,
) -> LifecyclePurgeProof {
    let token = service
        .load_inventory(
            branch_id(),
            DATABASE_ID,
            LifecycleCodecId::identity().as_str(),
        )
        .expect("inventory token")
        .token();
    LifecyclePurgeProof::fresh(recovery_health, token)
}

/// #2524 Fix B fallout: reclaim runs during load, so a sweep can stage new
/// inventory entries between a purge's token capture and its mutation. The
/// advanced token must DEFER the purge (the staging sweep enqueues its own
/// follow-up purge) — pre-fix it hard-failed the maintenance task with
/// health debt (the flaky `background_scale` closed-loop failure).
#[test]
fn purge_with_advanced_inventory_token_defers_without_health_debt() {
    let branch = branch_id();
    let backend: &'static QuarantineTestBackend =
        crate::testkit::leak_static(QuarantineTestBackend::durable());
    let service = QuarantineService::new(backend);

    // Stage object A: the inventory gains its first entry.
    let source_a = ObjectLayout::table_object(&branch.to_string(), 0, "raced-a").expect("object a");
    backend
        .write_object(&source_a, b"raced-table-a")
        .expect("source a write");
    let request_a = LifecycleQuarantineRequest::from_source_object(
        branch,
        DATABASE_ID,
        LifecycleCodecId::identity(),
        source_a,
        Timestamp::from_micros(10),
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
    )
    .expect("request a");
    assert_eq!(
        quarantine_object(&service, &request_a).status(),
        LifecycleQuarantineStatus::QuarantinedSourceDeleted
    );

    // A purge proof captures the CURRENT token...
    let stale_token = service
        .load_inventory(branch, DATABASE_ID, LifecycleCodecId::identity().as_str())
        .expect("inventory load")
        .token();
    let proof =
        LifecyclePurgeProof::fresh_for_candidate(RecoveryHealth::Healthy, stale_token, branch);

    // ...then a concurrent sweep stages object B, advancing the inventory.
    let source_b = ObjectLayout::table_object(&branch.to_string(), 0, "raced-b").expect("object b");
    backend
        .write_object(&source_b, b"raced-table-b")
        .expect("source b write");
    let request_b = LifecycleQuarantineRequest::from_source_object(
        branch,
        DATABASE_ID,
        LifecycleCodecId::identity(),
        source_b,
        Timestamp::from_micros(11),
        LifecycleQuarantineProof::safe(RecoveryHealth::Healthy),
    )
    .expect("request b");
    assert_eq!(
        quarantine_object(&service, &request_b).status(),
        LifecycleQuarantineStatus::QuarantinedSourceDeleted
    );

    let outcome = purge_quarantine(
        &service,
        branch,
        DATABASE_ID,
        &LifecycleCodecId::identity(),
        &proof,
    )
    .expect("purge with advanced token");

    assert_eq!(outcome.status(), LifecyclePurgeStatus::InventoryAdvanced);
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert!(
        maintenance.recovery_health().is_none(),
        "a raced purge is normal operation, not health debt",
    );
    assert_eq!(maintenance.bytes_reclaimed(), 0);
}

/// Every sweep status maps to exactly the typed deferral the reclaim ledger
/// reports, and completed or failed statuses map to none (space-reclamation
/// contract §3.5; rule 39: typed reasons, never prose).
#[test]
fn quarantine_deferral_reason_truth_table() {
    use crate::lifecycle::quarantine::quarantine_deferral_reason;
    let cases = [
        (
            LifecycleQuarantineStatus::DeferredReferenced,
            Some(MaintenanceDeferralReason::Referenced),
        ),
        (
            LifecycleQuarantineStatus::DeferredIncompleteProof,
            Some(MaintenanceDeferralReason::IncompleteProof),
        ),
        (
            LifecycleQuarantineStatus::BlockedByRecoveryHealth,
            Some(MaintenanceDeferralReason::RecoveryHealth),
        ),
        (LifecycleQuarantineStatus::QuarantinedSourceDeleted, None),
        (LifecycleQuarantineStatus::AlreadyQuarantined, None),
        (LifecycleQuarantineStatus::SourceDeleteRetried, None),
        (
            LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish,
            None,
        ),
        (
            LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed,
            None,
        ),
        (LifecycleQuarantineStatus::InventoryPublishFailed, None),
        (LifecycleQuarantineStatus::InventoryPublishUncertain, None),
        (LifecycleQuarantineStatus::QuarantinePublishFailed, None),
        (LifecycleQuarantineStatus::QuarantinePublishUncertain, None),
        (LifecycleQuarantineStatus::InventoryMismatch, None),
        (LifecycleQuarantineStatus::ServiceRejected, None),
        (LifecycleQuarantineStatus::ServiceTransient, None),
    ];
    for (status, expected) in cases {
        assert_eq!(quarantine_deferral_reason(status), expected, "{status:?}");
    }
}

/// Every purge status maps to exactly the typed deferral the reclaim ledger
/// reports; the completed statuses and the rewrite failure carry none.
#[test]
fn purge_deferral_reason_truth_table() {
    use crate::lifecycle::quarantine::purge_deferral_reason;
    let cases = [
        (
            LifecyclePurgeStatus::DeferredIncompleteProof,
            Some(MaintenanceDeferralReason::IncompleteProof),
        ),
        (
            LifecyclePurgeStatus::BlockedByRecoveryHealth,
            Some(MaintenanceDeferralReason::RecoveryHealth),
        ),
        (
            LifecyclePurgeStatus::StaleProof,
            Some(MaintenanceDeferralReason::StaleProof),
        ),
        (
            LifecyclePurgeStatus::InventoryAdvanced,
            Some(MaintenanceDeferralReason::InventoryAdvanced),
        ),
        (LifecyclePurgeStatus::Completed, None),
        (LifecyclePurgeStatus::CompletedNoop, None),
        (LifecyclePurgeStatus::CompletedWithHealthDebt, None),
        (LifecyclePurgeStatus::InventoryRewriteFailed, None),
    ];
    for (status, expected) in cases {
        assert_eq!(purge_deferral_reason(status), expected, "{status:?}");
    }
}
