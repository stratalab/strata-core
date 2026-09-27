use super::*;
use crate::branch::facts::BranchLevel;
use crate::format::{
    TableManifest, TableManifestInheritedLayer, TableManifestInheritedLayerStatus,
    TableManifestLevel, TableManifestTableBounds, TableManifestTableFacts,
    TableManifestTableProvenance, TableManifestTableRef,
};
use crate::layout::ObjectLayout;
use crate::object::ObjectName;
use crate::table::TableIdentity;
use strata_core::{BranchId, CommitVersion};

mod plan;

#[test]
fn manifest_referenced_table_object_is_retained() {
    let branch = branch_id(0x11);
    let live = table_object(branch, 0, "live0001");
    let request = request(
        branch,
        vec![manifest(
            branch,
            vec![table_ref(
                branch,
                0,
                "live0001",
                TableManifestTableProvenance::Flush,
            )],
            vec![],
        )],
        vec![entry(live.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(outcome.decisions()[0].object(), Some(&live));
    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ReachableTable
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn shared_table_object_is_retained_until_all_manifest_refs_drop() {
    let branch = branch_id(0x12);
    let other = branch_id(0x13);
    let shared = table_object(branch, 0, "shared0001");
    let request = request(
        branch,
        vec![
            manifest(
                branch,
                vec![table_ref(
                    branch,
                    0,
                    "shared0001",
                    TableManifestTableProvenance::Flush,
                )],
                vec![],
            ),
            manifest(
                other,
                vec![],
                vec![inherited_layer(
                    branch,
                    vec![table_ref(
                        branch,
                        0,
                        "shared0001",
                        TableManifestTableProvenance::Recovered,
                    )],
                )],
            ),
        ],
        vec![entry(shared.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ReachableSharedTable
    );
}

#[test]
fn inherited_layer_table_object_is_retained() {
    let child = branch_id(0x14);
    let parent = branch_id(0x15);
    let inherited = table_object(parent, 0, "parent0001");
    let layer = inherited_layer(
        parent,
        vec![table_ref(
            parent,
            0,
            "parent0001",
            TableManifestTableProvenance::Flush,
        )],
    );
    let request = request(
        child,
        vec![manifest(child, vec![], vec![layer])],
        vec![entry(inherited.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].object(), Some(&inherited));
    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ReachableInheritedTable
    );
}

#[test]
fn materialization_replacement_table_object_is_retained() {
    let branch = branch_id(0x16);
    let source = branch_id(0x17);
    let object = table_object(branch, 0, "mat0001");
    let provenance =
        TableManifestTableProvenance::materialization_replacement(source, CommitVersion::new(9))
            .expect("provenance");
    let request = request(
        branch,
        vec![manifest(
            branch,
            vec![table_ref(branch, 0, "mat0001", provenance)],
            vec![],
        )],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ReachableMaterializedTable
    );
}

#[test]
fn materialized_table_object_preserves_reason_when_shared() {
    // A branch materializes a replacement table object. Another branch
    // inherits that object via its own inherited layer. The classifier
    // must keep the Materialized reason since it carries stronger semantic
    // information than Shared — downgrading would misrepresent the
    // manifest's stated provenance to downstream consumers.
    let owner = branch_id(0x2f);
    let observer = branch_id(0x30);
    let source = branch_id(0x31);
    let object = table_object(owner, 0, "mat0002");
    let materialized =
        TableManifestTableProvenance::materialization_replacement(source, CommitVersion::new(9))
            .expect("provenance");
    let request = request(
        owner,
        vec![
            manifest(
                owner,
                vec![table_ref(owner, 0, "mat0002", materialized)],
                vec![],
            ),
            manifest(
                observer,
                vec![],
                vec![inherited_layer(
                    owner,
                    vec![table_ref(
                        owner,
                        0,
                        "mat0002",
                        TableManifestTableProvenance::Recovered,
                    )],
                )],
            ),
        ],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ReachableMaterializedTable
    );
}

#[test]
fn orphaned_table_object_becomes_quarantine_candidate_with_fresh_token() {
    let branch = branch_id(0x18);
    let orphan = table_object(branch, 0, "orphan0001");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::QuarantineCandidate
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::TableRequiresQuarantine
    );
    let token = outcome.quarantine_tokens().first().expect("token");
    assert_eq!(token.object(), &orphan);
    assert!(token.validates_for(&orphan, outcome.proof_context()));
    assert!(!token.validates_for(
        &table_object(branch, 0, "other0001"),
        outcome.proof_context()
    ));
}

/// COW invariant: an object reachable only from IN-MEMORY branch state (e.g. a fork child in
/// its manifest-publish crash window) is durably invisible to the manifest walk — the pinned
/// set must keep it live, flipping the would-be quarantine candidate to Retain.
#[test]
fn pinned_in_memory_object_is_retained_not_quarantined() {
    let branch = branch_id(0x1a);
    let orphan = table_object(branch, 0, "pinned0001");
    let unpinned = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    assert_eq!(
        table_object_retention_outcome(&unpinned)
            .expect("outcome")
            .decisions()[0]
            .decision(),
        RetentionDecision::QuarantineCandidate,
        "without the pin the object is unreachable and becomes a candidate",
    );

    let pinned = unpinned.with_pinned_objects(vec![orphan.clone()]);
    let outcome = table_object_retention_outcome(&pinned).expect("outcome");

    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert!(
        outcome.quarantine_tokens().is_empty(),
        "a pinned object must not mint a quarantine token",
    );
}

#[test]
fn stale_quarantine_token_rejects_changed_inventory_epoch() {
    let branch = branch_id(0x19);
    let orphan = table_object(branch, 0, "orphan0002");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");
    let token = outcome.quarantine_tokens().first().expect("token").clone();
    let changed = LifecycleTableObjectProofContext::new(
        branch,
        LifecycleTableObjectProofEpochs::new(1, 2, 1, 1).expect("epochs"),
        *outcome.proof_context().fingerprint(),
    );

    assert!(!token.validates_against(&changed));
}

#[test]
fn proof_token_rejects_manifest_epoch_change() {
    assert_token_rejects_epoch_change(
        LifecycleTableObjectProofEpochs::new(2, 1, 1, 1).expect("epochs"),
    );
}

#[test]
fn proof_token_rejects_quarantine_inventory_epoch_change() {
    assert_token_rejects_epoch_change(
        LifecycleTableObjectProofEpochs::new(1, 1, 2, 1).expect("epochs"),
    );
}

#[test]
fn proof_token_rejects_recovery_health_epoch_change() {
    assert_token_rejects_epoch_change(
        LifecycleTableObjectProofEpochs::new(1, 1, 1, 2).expect("epochs"),
    );
}

#[test]
fn proof_token_rejects_object_fingerprint_change() {
    let branch = branch_id(0x28);
    let orphan = table_object(branch, 0, "orphan0003");
    let original = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&original).expect("outcome");
    let token = outcome.quarantine_tokens().first().expect("token").clone();
    let changed = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan, 101)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let changed = table_object_retention_outcome(&changed).expect("changed");

    assert!(!token.validates_against(changed.proof_context()));
}

#[test]
fn quarantine_candidate_can_build_quarantine_proof() {
    let branch = branch_id(0x29);
    let object = table_object(branch, 0, "orphan0004");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");
    let proof = LifecycleQuarantineProof::from_retention_decision(
        outcome.decisions()[0].decision(),
        RecoveryHealth::Healthy,
    );

    assert_eq!(proof.status(), LifecycleQuarantineProofStatus::CompleteSafe);
}

#[test]
fn runtime_only_ref_does_not_make_object_live() {
    let branch = branch_id(0x2a);
    let runtime_ref_only = table_object(branch, 0, "runtime0001");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(runtime_ref_only, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::QuarantineCandidate
    );
}

/// An inventory entry that is NOT this branch's own (the request lists it only
/// among every branch's quarantined sources) is delegated to the branch that
/// staged it: never a fresh candidate from here.
#[test]
fn already_quarantined_table_object_is_not_requeued() {
    let branch = branch_id(0x1a);
    let object = table_object(branch, 0, "old0001");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::SkipUntilProof
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::TableAlreadyQuarantined
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

/// #3608: a source THIS branch's own quarantine inventory names, yet still on
/// disk — the first pass's source delete was refused — is a candidate again
/// with its own reason and a fresh proof token, so the sweep retries the delete
/// through the existing entry instead of skipping it forever.
#[test]
fn own_branch_quarantined_source_still_on_disk_is_a_retry_candidate() {
    let branch = branch_id(0x1b);
    let object = table_object(branch, 0, "retry0001");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    )
    .with_own_quarantined_objects(vec![object.clone()]);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].object(), Some(&object));
    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::QuarantineCandidate
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::QuarantinedSourceStillPresent
    );
    let token = outcome
        .quarantine_tokens()
        .first()
        .expect("the retry candidate carries a proof token");
    assert!(token.validates_for(&object, outcome.proof_context()));
}

/// The in-memory pin (#2553) outranks the retry: an own-branch quarantined
/// source that some branch still reaches in memory is retained, not retried.
#[test]
fn own_branch_quarantined_source_that_is_pinned_is_retained() {
    let branch = branch_id(0x1c);
    let object = table_object(branch, 0, "retry0002");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    )
    .with_own_quarantined_objects(vec![object.clone()])
    .with_pinned_objects(vec![object.clone()]);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn cross_branch_quarantined_object_is_not_requeued_from_other_branch() {
    let owner = branch_id(0x2d);
    let observer = branch_id(0x2e);
    let object = table_object(owner, 0, "shared0001");
    // The observing branch runs retention. The owning branch's manifest no
    // longer references the object (it's been moved to quarantine). The
    // observer's quarantine load picked up both branches' quarantined
    // objects so the classifier must delegate, not emit a fresh candidate.
    let request = request(
        observer,
        vec![
            manifest(observer, vec![], vec![]),
            manifest(owner, vec![], vec![]),
        ],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].object(), Some(&object));
    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::SkipUntilProof
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::TableAlreadyQuarantined
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn live_table_object_wins_over_stale_quarantine_inventory() {
    let branch = branch_id(0x2c);
    let object = table_object(branch, 0, "livequar0001");
    let request = request(
        branch,
        vec![manifest(
            branch,
            vec![table_ref(
                branch,
                0,
                "livequar0001",
                TableManifestTableProvenance::Flush,
            )],
            vec![],
        )],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].object(), Some(&object));
    assert_eq!(outcome.decisions()[0].decision(), RetentionDecision::Retain);
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ReachableTable
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn incomplete_manifest_proof_retains_all_inventory_until_retry() {
    let branch = branch_id(0x1b);
    let object = table_object(branch, 0, "candidate0001");
    let request = request(
        branch,
        vec![],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    )
    .with_manifest_complete(false);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::DeferredIncompleteProof
    );
    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::SkipUntilProof
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ProofIncomplete
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn missing_manifest_referenced_inventory_keeps_proof_incomplete() {
    let branch = branch_id(0x1c);
    let request = request(
        branch,
        vec![manifest(
            branch,
            vec![table_ref(
                branch,
                0,
                "missing0001",
                TableManifestTableProvenance::Flush,
            )],
            vec![],
        )],
        vec![],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::DeferredIncompleteProof
    );
    assert_eq!(outcome.decisions(), &[]);
}

#[test]
fn no_table_objects_returns_completed_empty_graph() {
    let branch = branch_id(0x25);
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert!(outcome.decisions().is_empty());
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn policy_downgrade_blocks_table_object_candidate() {
    let branch = branch_id(0x26);
    let object = table_object(branch, 0, "candidate0006");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::PolicyDowngrade,
            faults: vec![RecoveryFault::new(
                RecoveryFaultKind::NoManifestFallback,
                "lossy recovery",
            )
            .expect("fault")],
        },
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::BlockedByRecoveryHealth
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn data_loss_recovery_health_blocks_table_object_retention() {
    let branch = branch_id(0x1d);
    let object = table_object(branch, 0, "candidate0002");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            faults: vec![RecoveryFault::new(
                RecoveryFaultKind::MissingTableObject,
                "missing table object",
            )
            .expect("fault")],
        },
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::BlockedByRecoveryHealth
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::UnsafeRecoveryHealth
    );
    assert!(outcome.recovery_health().is_some());
}

#[test]
fn failed_health_blocks_table_object_candidate() {
    let branch = branch_id(0x27);
    let object = table_object(branch, 0, "candidate0007");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Failed {
            fault: RecoveryFault::new(RecoveryFaultKind::CorruptManifest, "manifest mismatch")
                .expect("fault"),
        },
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::BlockedByRecoveryHealth
    );
    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::SkipUntilProof
    );
}

#[test]
fn telemetry_degraded_recovery_health_can_still_classify_candidates() {
    let branch = branch_id(0x1e);
    let object = table_object(branch, 0, "candidate0003");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::Telemetry,
            faults: vec![RecoveryFault::new(
                RecoveryFaultKind::QuarantineInventoryMismatch,
                "telemetry mismatch",
            )
            .expect("fault")],
        },
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::QuarantineCandidate
    );
}

#[test]
fn telemetry_degraded_recovery_health_blocks_when_policy_disallows_it() {
    let branch = branch_id(0x1f);
    let object = table_object(branch, 0, "candidate0004");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::Telemetry,
            faults: vec![RecoveryFault::new(
                RecoveryFaultKind::QuarantineInventoryMismatch,
                "telemetry mismatch",
            )
            .expect("fault")],
        },
    )
    .with_telemetry_degraded_recovery_allowed(false);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::BlockedByRecoveryHealth
    );
}

#[test]
fn malformed_table_prefix_object_is_repair_candidate_without_token() {
    let branch = branch_id(0x20);
    let malformed = ObjectName::new(format!("tables/{branch}/lonely")).expect("object");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(malformed.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(outcome.decisions()[0].object(), Some(&malformed));
    assert_eq!(
        outcome.decisions()[0].decision(),
        RetentionDecision::RepairCandidate
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::MalformedTableObject
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn table_manifest_and_non_table_inventory_objects_are_ignored() {
    let branch = branch_id(0x21);
    let table_manifest = ObjectName::new(format!("tables/{branch}/manifest")).expect("object");
    let snapshot = ObjectName::new("snapshots/0000000000000001").expect("object");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(table_manifest, 100), entry(snapshot, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert!(outcome.decisions().is_empty());
}

#[test]
fn shuffled_inventory_produces_deterministic_decision_order() {
    let branch = branch_id(0x22);
    let first = table_object(branch, 0, "aaa0001");
    let second = table_object(branch, 0, "bbb0001");
    let left = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(second.clone(), 100), entry(first.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let right = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(first.clone(), 100), entry(second.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let left = table_object_retention_outcome(&left).expect("left");
    let right = table_object_retention_outcome(&right).expect("right");

    assert_eq!(left.decisions(), right.decisions());
    assert_eq!(
        left.proof_context().fingerprint(),
        right.proof_context().fingerprint()
    );
}

#[test]
fn duplicate_inventory_entry_is_rejected() {
    let branch = branch_id(0x23);
    let object = table_object(branch, 0, "dup0001");
    let result = LifecycleTableObjectRetentionRequest::new(
        branch,
        RecoveryHealth::Healthy,
        epochs(),
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100), entry(object, 100)],
        vec![],
    );

    assert!(matches!(
        result,
        Err(LifecycleError::InvalidConfig {
            field: "table_object_inventory",
            ..
        })
    ));
}

#[test]
fn zero_epoch_is_rejected() {
    let result = LifecycleTableObjectProofEpochs::new(0, 1, 1, 1);

    assert!(matches!(
        result,
        Err(LifecycleError::InvalidConfig {
            field: "manifest_epoch",
            ..
        })
    ));
}

#[test]
fn table_object_health_debt_is_empty_for_complete_healthy_outcome() {
    let branch = branch_id(0x24);
    let object = table_object(branch, 0, "candidate0005");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        table_object_retention_health_debt(&outcome).expect("health debt"),
        None
    );
}

fn request(
    branch_id: BranchId,
    manifests: Vec<TableManifest>,
    inventory: Vec<LifecycleTableObjectInventoryEntry>,
    quarantined_objects: Vec<ObjectName>,
    recovery_health: RecoveryHealth,
) -> LifecycleTableObjectRetentionRequest {
    LifecycleTableObjectRetentionRequest::new(
        branch_id,
        recovery_health,
        epochs(),
        manifests,
        inventory,
        quarantined_objects,
    )
    .expect("request")
}

fn assert_token_rejects_epoch_change(changed_epochs: LifecycleTableObjectProofEpochs) {
    let branch = branch_id(0x2b);
    let orphan = table_object(branch, 0, "orphan0005");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");
    let token = outcome.quarantine_tokens().first().expect("token").clone();
    let changed = LifecycleTableObjectProofContext::new(
        branch,
        changed_epochs,
        *outcome.proof_context().fingerprint(),
    );

    assert!(!token.validates_against(&changed));
}

fn manifest(
    branch_id: BranchId,
    tables: Vec<TableManifestTableRef>,
    inherited_layers: Vec<TableManifestInheritedLayer>,
) -> TableManifest {
    let levels = if tables.is_empty() {
        Vec::new()
    } else {
        vec![TableManifestLevel::new(BranchLevel::ZERO, tables).expect("level")]
    };
    TableManifest::new(branch_id, None, 1, levels, inherited_layers, Vec::new()).expect("manifest")
}

fn inherited_layer(
    source_branch_id: BranchId,
    tables: Vec<TableManifestTableRef>,
) -> TableManifestInheritedLayer {
    TableManifestInheritedLayer::new(
        0,
        source_branch_id,
        None,
        CommitVersion::new(7),
        TableManifestInheritedLayerStatus::Active,
        vec![TableManifestLevel::new(BranchLevel::ZERO, tables).expect("level")],
    )
    .expect("inherited layer")
}

fn table_ref(
    object_branch: BranchId,
    order: u32,
    object_id: &str,
    provenance: TableManifestTableProvenance,
) -> TableManifestTableRef {
    let commit = CommitVersion::new(u64::from(order) + 1);
    TableManifestTableRef::new(
        TableIdentity::new(format!("table-{object_id}")).expect("identity"),
        table_object(object_branch, 0, object_id),
        order,
        TableManifestTableFacts::new(100, 1, 1, commit, commit, None, None).expect("facts"),
        TableManifestTableBounds::new(
            format!("k{order:04}").into_bytes(),
            format!("k{order:04}z").into_bytes(),
            format!("i{order:04}").into_bytes(),
            format!("i{order:04}z").into_bytes(),
        )
        .expect("bounds"),
        provenance,
    )
    .expect("table ref")
}

fn table_object(branch_id: BranchId, level: u32, object_id: &str) -> ObjectName {
    ObjectLayout::table_object(&branch_id.to_string(), level, object_id).expect("object")
}

fn entry(object: ObjectName, byte_count: u64) -> LifecycleTableObjectInventoryEntry {
    LifecycleTableObjectInventoryEntry::new(object, byte_count).expect("entry")
}

fn branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; 16])
}

fn epochs() -> LifecycleTableObjectProofEpochs {
    LifecycleTableObjectProofEpochs::new(1, 1, 1, 1).expect("epochs")
}
