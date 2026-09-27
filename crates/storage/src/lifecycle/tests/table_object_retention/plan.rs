use super::*;

#[test]
fn table_object_inventory_lists_candidates_in_stable_order() {
    let branch = branch_id(0x61);
    let first = table_object(branch, 0, "aaa0001");
    let second = table_object(branch, 0, "bbb0001");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(second.clone(), 200), entry(first.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");
    let objects = outcome
        .decisions()
        .iter()
        .map(|decision| decision.object().expect("table object").clone())
        .collect::<Vec<_>>();

    assert_eq!(objects, vec![first, second]);
}

#[test]
fn table_object_inventory_rejects_malformed_object_name() {
    let branch = branch_id(0x62);
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
    assert!(
        outcome.quarantine_tokens().is_empty(),
        "malformed object must not receive a quarantine proof token"
    );
}

#[test]
fn table_object_inventory_failure_records_proof_incomplete() {
    let branch = branch_id(0x63);
    let object = table_object(branch, 0, "orphan0101");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Healthy,
    )
    .with_table_inventory_complete(false);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::DeferredIncompleteProof
    );
    assert_eq!(
        outcome.decisions()[0].reason(),
        LifecycleRetentionDecisionReason::ProofIncomplete
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn reachability_graph_retains_manifest_owned_table() {
    let (outcome, owned, _, _, _) = mixed_reachability_outcome();

    assert_decision(
        &outcome,
        &owned,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableTable,
    );
}

#[test]
fn reachability_graph_retains_inherited_layer_table() {
    let (outcome, _, inherited, _, _) = mixed_reachability_outcome();

    assert_decision(
        &outcome,
        &inherited,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableInheritedTable,
    );
}

#[test]
fn reachability_graph_retains_materialization_replacement_table() {
    let branch = branch_id(0x64);
    let source = branch_id(0x65);
    let object = table_object(branch, 0, "mat0101");
    let provenance =
        TableManifestTableProvenance::materialization_replacement(source, CommitVersion::new(9))
            .expect("provenance");
    let request = request(
        branch,
        vec![manifest(
            branch,
            vec![table_ref(branch, 0, "mat0101", provenance)],
            vec![],
        )],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableMaterializedTable,
    );
}

#[test]
fn reachability_graph_records_shared_object_reasons() {
    let (outcome, _, _, shared, _) = mixed_reachability_outcome();

    assert_decision(
        &outcome,
        &shared,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableSharedTable,
    );
}

#[test]
fn reachability_graph_ignores_non_table_object_families() {
    let branch = branch_id(0x66);
    let snapshot = ObjectName::new("snapshots/0000000000000042").expect("snapshot object");
    let manifest_object = ObjectName::new(format!("tables/{branch}/manifest")).expect("manifest");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(snapshot, 100), entry(manifest_object, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert!(outcome.decisions().is_empty());
}

#[test]
fn reachability_graph_is_deterministic_for_shuffled_inputs() {
    let branch = branch_id(0x67);
    let first = table_object(branch, 0, "aaa0101");
    let second = table_object(branch, 0, "bbb0101");
    let left = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(second.clone(), 200), entry(first.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let right = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(first, 100), entry(second, 200)],
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
fn reachability_graph_reports_manifest_object_names() {
    let (outcome, owned, inherited, shared, orphan) = mixed_reachability_outcome();
    let names = outcome
        .decisions()
        .iter()
        .filter_map(LifecycleRetentionDecisionRecord::object)
        .cloned()
        .collect::<Vec<_>>();

    assert!(names.contains(&owned));
    assert!(names.contains(&inherited));
    assert!(names.contains(&shared));
    assert!(names.contains(&orphan));
}

#[test]
fn manifest_referenced_object_is_retained_live() {
    // Single-manifest path with a single object referenced by a Flush
    // provenance. Asserts the base contract independent of the mixed
    // fixture used elsewhere.
    let branch = branch_id(0x86);
    let object = table_object(branch, 0, "live0101");
    let request = request(
        branch,
        vec![manifest(
            branch,
            vec![table_ref(
                branch,
                0,
                "live0101",
                TableManifestTableProvenance::Flush,
            )],
            vec![],
        )],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableTable,
    );
    assert_eq!(outcome.status(), LifecycleRetentionStatus::Completed);
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn shared_object_is_retained_until_all_refs_drop() {
    let branch = branch_id(0x68);
    let other = branch_id(0x69);
    let shared = table_object(branch, 0, "shared0101");
    let live = request(
        branch,
        vec![
            manifest(
                branch,
                vec![table_ref(
                    branch,
                    0,
                    "shared0101",
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
                        "shared0101",
                        TableManifestTableProvenance::Recovered,
                    )],
                )],
            ),
        ],
        vec![entry(shared.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let dropped = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(shared.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let live = table_object_retention_outcome(&live).expect("live");
    let dropped = table_object_retention_outcome(&dropped).expect("dropped");

    assert_decision(
        &live,
        &shared,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableSharedTable,
    );
    assert_decision(
        &dropped,
        &shared,
        RetentionDecision::QuarantineCandidate,
        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
    );
}

#[test]
fn inherited_layer_object_is_retained() {
    // Distinct from reachability_graph_retains_inherited_layer_table: that
    // test reuses the mixed fixture. This one uses a minimal child/parent
    // manifest pair where the child has NO owned tables, exercising the
    // pure inherited-layer code path.
    let child = branch_id(0x87);
    let parent = branch_id(0x88);
    let inherited = table_object(parent, 0, "parent0102");
    let request = request(
        child,
        vec![manifest(
            child,
            vec![],
            vec![inherited_layer(
                parent,
                vec![table_ref(
                    parent,
                    0,
                    "parent0102",
                    TableManifestTableProvenance::Flush,
                )],
            )],
        )],
        vec![entry(inherited.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &inherited,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableInheritedTable,
    );
}

#[test]
fn prefix_orphan_with_complete_safe_proof_is_quarantine_candidate() {
    let branch = branch_id(0x6a);
    let orphan = table_object(branch, 0, "orphan0102");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &orphan,
        RetentionDecision::QuarantineCandidate,
        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
    );
    assert_eq!(outcome.quarantine_tokens().len(), 1);
}

#[test]
fn prefix_orphan_with_incomplete_manifest_proof_is_retained() {
    let branch = branch_id(0x6b);
    let orphan = table_object(branch, 0, "orphan0103");
    let request = request(
        branch,
        vec![],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    )
    .with_manifest_complete(false);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &orphan,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::ProofIncomplete,
    );
}

#[test]
fn prefix_orphan_with_inventory_failure_is_retained() {
    // Distinct from inventory_failure_records_proof_incomplete: this
    // variant asserts the *retain* outcome (SkipUntilProof) rather than
    // just the proof status, and uses a fresh branch so the two tests
    // don't share fixture state.
    let branch = branch_id(0x89);
    let orphan = table_object(branch, 0, "orphan0117");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    )
    .with_table_inventory_complete(false);
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &orphan,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::ProofIncomplete,
    );
    assert!(
        outcome.quarantine_tokens().is_empty(),
        "incomplete-inventory path must not emit quarantine tokens"
    );
}

/// An entry named only by another branch's inventory is delegated to that
/// branch's sweep (the request lists no own-branch quarantined sources here).
#[test]
fn already_quarantined_object_is_delegated() {
    let branch = branch_id(0x6c);
    let object = table_object(branch, 0, "quar0101");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::TableAlreadyQuarantined,
    );
}

#[test]
fn unsupported_table_object_scope_is_not_completed_success() {
    // Defense-in-depth: the production maintenance runner short-circuits
    // TableObjects scope to the dedicated reachability path *before*
    // reaching `retention_outcome_for_scope`. But if a future caller
    // bypasses the runner and calls the generic retention outcome path
    // directly with TableObjects scope, the generic path must still
    // refuse — it doesn't have the reachability facts to make a safe
    // decision. The production wiring is exercised by
    // `branch_retention_task_classifies_table_objects_through_durable_maintenance`
    // in the retention integration suite; this unit test guards the
    // generic fallback shape.
    let branch = branch_id(0x6d);
    let request = LifecycleRetentionRequest::new(
        LifecycleRetentionScope::TableObjects { branch_id: branch },
        1,
    );

    for proof_status in [
        LifecycleRetentionProofStatus::Complete,
        LifecycleRetentionProofStatus::Incomplete,
        LifecycleRetentionProofStatus::BlockedByRecoveryHealth,
    ] {
        let proof = LifecycleRetentionProof::new(
            proof_status,
            RecoveryHealth::Healthy,
            Some(1),
            Some(CommitVersion::new(7)),
            Some(CommitVersion::new(7)),
            None,
        );

        let outcome = retention_outcome_for_scope(&request, proof, &[]).expect("outcome");

        assert_eq!(
            outcome.status(),
            LifecycleRetentionStatus::DeferredUnsupportedScope,
            "proof status {proof_status:?} must still defer the generic path"
        );
    }

    // Also assert the production runner routes TableObjects scope through
    // the dedicated path: a branch-scoped Retention maintenance task is
    // valid (`scope_matches_kind` accepts the combination), and
    // `retention_request_from_maintenance_task` builds a TableObjects
    // retention request from it.
    let task =
        MaintenanceTask::new_for_test(1, branch_table_retention_task(branch)).expect("test task");
    let derived =
        crate::lifecycle::retention_request_from_maintenance_task(&task).expect("derive request");
    assert_eq!(
        derived.scope(),
        LifecycleRetentionScope::TableObjects { branch_id: branch },
        "branch retention task must route to TableObjects scope"
    );
}

#[test]
fn malformed_table_object_name_returns_repair_candidate() {
    // Pair to inventory_rejects_malformed_object_name but uses a malformed
    // object name with a different shape (extra path component) so this
    // test catches mis-parses of differently-malformed names.
    let branch = branch_id(0x8a);
    let malformed = ObjectName::new(format!("tables/{branch}/L0/extra/garbage")).expect("object");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(malformed.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &malformed,
        RetentionDecision::RepairCandidate,
        LifecycleRetentionDecisionReason::MalformedTableObject,
    );
    assert!(
        outcome.quarantine_tokens().is_empty(),
        "repair candidate must not receive a quarantine proof token"
    );
}

#[test]
fn healthy_recovery_allows_quarantine_candidate() {
    // Confirm the healthy-recovery health gate is permissive: with
    // RecoveryHealth::Healthy a clean prefix orphan reaches the
    // QuarantineCandidate decision AND emits a freshly-bound proof token
    // that validates against the outcome's context.
    let branch = branch_id(0x8b);
    let orphan = table_object(branch, 0, "orphan0118");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &orphan,
        RetentionDecision::QuarantineCandidate,
        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
    );
    let token = outcome.quarantine_tokens().first().expect("token");
    assert!(token.validates_for(&orphan, outcome.proof_context()));
}

#[test]
fn telemetry_health_allows_unrelated_table_object_candidate() {
    let branch = branch_id(0x6e);
    let object = table_object(branch, 0, "orphan0104");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        telemetry_degraded_health(),
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::QuarantineCandidate,
        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
    );
}

#[test]
fn unsafe_health_records_health_debt() {
    let branch = branch_id(0x6f);
    let object = table_object(branch, 0, "orphan0105");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        data_loss_health(),
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");
    let debt = table_object_retention_health_debt(&outcome)
        .expect("health debt")
        .expect("debt");

    assert!(matches!(
        debt,
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            ..
        }
    ));
}

#[test]
fn unsafe_health_retains_all_candidates() {
    let branch = branch_id(0x70);
    let object = table_object(branch, 0, "orphan0106");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        data_loss_health(),
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::UnsafeRecoveryHealth,
    );
    assert!(outcome.quarantine_tokens().is_empty());
}

#[test]
fn health_generation_change_stales_proof_token() {
    // Distinct from proof_token_rejects_recovery_health_epoch_change:
    // produces a real second outcome with degraded health (rather than a
    // hand-mutated epoch context) and asserts the fresh token from the
    // healthy outcome does not validate against the degraded outcome's
    // context.
    let branch = branch_id(0x8c);
    let orphan = table_object(branch, 0, "orphan0119");
    let healthy = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let degraded = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(orphan.clone(), 100)],
        vec![],
        telemetry_degraded_health(),
    );

    let healthy_outcome = table_object_retention_outcome(&healthy).expect("healthy");
    let degraded_outcome = table_object_retention_outcome(&degraded).expect("degraded");
    let healthy_token = healthy_outcome
        .quarantine_tokens()
        .first()
        .expect("healthy token")
        .clone();

    assert_ne!(
        healthy_outcome.proof_context().fingerprint(),
        degraded_outcome.proof_context().fingerprint(),
        "health change must reshape the fingerprint"
    );
    assert!(
        !healthy_token.validates_for(&orphan, degraded_outcome.proof_context()),
        "token bound to healthy proof must not validate against degraded proof"
    );
}

#[test]
fn proof_token_includes_manifest_epoch() {
    // The token's manifest epoch must be bound into the context fingerprint
    // so the context generated from a different manifest epoch produces a
    // different fingerprint — proves the epoch isn't carried as a side
    // field that the fingerprint ignores.
    let (token, _, context) = fresh_token_with_context();
    let mutated_context = LifecycleTableObjectProofContext::new(
        context.branch_id(),
        LifecycleTableObjectProofEpochs::new(
            context.epochs().manifest_epoch().wrapping_add(1),
            context.epochs().table_inventory_epoch(),
            context.epochs().quarantine_inventory_epoch(),
            context.epochs().recovery_health_epoch(),
        )
        .expect("epochs"),
        *context.fingerprint(),
    );

    assert_eq!(token.epochs().manifest_epoch(), 1);
    assert!(!token.validates_against(&mutated_context));
}

#[test]
fn proof_token_includes_table_inventory_epoch() {
    let (token, _, context) = fresh_token_with_context();
    let mutated_context = LifecycleTableObjectProofContext::new(
        context.branch_id(),
        LifecycleTableObjectProofEpochs::new(
            context.epochs().manifest_epoch(),
            context.epochs().table_inventory_epoch().wrapping_add(1),
            context.epochs().quarantine_inventory_epoch(),
            context.epochs().recovery_health_epoch(),
        )
        .expect("epochs"),
        *context.fingerprint(),
    );

    assert_eq!(token.epochs().table_inventory_epoch(), 1);
    assert!(!token.validates_against(&mutated_context));
}

#[test]
fn proof_token_includes_quarantine_inventory_epoch() {
    let (token, _, context) = fresh_token_with_context();
    let mutated_context = LifecycleTableObjectProofContext::new(
        context.branch_id(),
        LifecycleTableObjectProofEpochs::new(
            context.epochs().manifest_epoch(),
            context.epochs().table_inventory_epoch(),
            context
                .epochs()
                .quarantine_inventory_epoch()
                .wrapping_add(1),
            context.epochs().recovery_health_epoch(),
        )
        .expect("epochs"),
        *context.fingerprint(),
    );

    assert_eq!(token.epochs().quarantine_inventory_epoch(), 1);
    assert!(!token.validates_against(&mutated_context));
}

#[test]
fn proof_token_includes_recovery_health_epoch() {
    let (token, _, context) = fresh_token_with_context();
    let mutated_context = LifecycleTableObjectProofContext::new(
        context.branch_id(),
        LifecycleTableObjectProofEpochs::new(
            context.epochs().manifest_epoch(),
            context.epochs().table_inventory_epoch(),
            context.epochs().quarantine_inventory_epoch(),
            context.epochs().recovery_health_epoch().wrapping_add(1),
        )
        .expect("epochs"),
        *context.fingerprint(),
    );

    assert_eq!(token.epochs().recovery_health_epoch(), 1);
    assert!(!token.validates_against(&mutated_context));
}

#[test]
fn proof_token_includes_object_fingerprint() {
    let (token, object, context) = fresh_token_with_context();

    assert!(token.validates_for(&object, &context));
    assert!(!token.validates_for(&table_object(branch_id(0x71), 0, "other0101"), &context));
}

#[test]
fn proof_token_rejects_table_inventory_epoch_change() {
    assert_token_rejects_epoch_change(
        LifecycleTableObjectProofEpochs::new(1, 2, 1, 1).expect("epochs"),
    );
}

#[test]
fn quarantine_candidate_can_build_reclaim_request() {
    // Asserts the reverse direction from quarantine_candidate_can_build_
    // quarantine_proof: starting from a QuarantineCandidate decision +
    // proof token, the candidate flows into a complete-safe
    // LifecycleQuarantineProof under healthy recovery. This is the
    // reachability/quarantine handoff contract.
    let branch = branch_id(0x8d);
    let object = table_object(branch, 0, "orphan0120");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");
    let candidate = outcome
        .decisions()
        .iter()
        .find(|decision| decision.object() == Some(&object))
        .expect("candidate decision");
    assert_eq!(candidate.decision(), RetentionDecision::QuarantineCandidate);

    let proof = LifecycleQuarantineProof::from_retention_decision(
        candidate.decision(),
        RecoveryHealth::Healthy,
    );

    assert_eq!(proof.status(), LifecycleQuarantineProofStatus::CompleteSafe);
    let token = outcome.quarantine_tokens().first().expect("token");
    assert!(token.validates_for(&object, outcome.proof_context()));
}

/// #3608: an own-branch entry whose source is still on disk is a retry
/// candidate on EVERY run, with a fresh proof token each time — the sweep's
/// existing-entry path retries the delete, and a still-refused delete leaves it
/// a candidate for the next run.
#[test]
fn own_branch_quarantined_source_is_a_retry_candidate_on_every_run() {
    let branch = branch_id(0x8f);
    let object = table_object(branch, 0, "quar0103");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    )
    .with_own_quarantined_objects(vec![object.clone()]);
    let first = table_object_retention_outcome(&request).expect("first run");
    let second = table_object_retention_outcome(&request).expect("second run");

    for outcome in [&first, &second] {
        assert_decision(
            outcome,
            &object,
            RetentionDecision::QuarantineCandidate,
            LifecycleRetentionDecisionReason::QuarantinedSourceStillPresent,
        );
        assert_eq!(
            outcome.quarantine_tokens().len(),
            1,
            "the retry candidate carries exactly one proof token"
        );
    }
}

#[test]
fn already_quarantined_object_is_not_requarantined() {
    // Distinct from already_quarantined_object_is_delegated: asserts the
    // *absence* of a fresh proof token (downstream quarantine would
    // otherwise see two tokens for the same object on consecutive
    // retention runs). The entry is another branch's (no own-branch
    // quarantined sources on the request), so it stays delegated.
    let branch = branch_id(0x8e);
    let object = table_object(branch, 0, "quar0102");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    );
    let first = table_object_retention_outcome(&request).expect("first run");
    let second = table_object_retention_outcome(&request).expect("second run");

    for outcome in [&first, &second] {
        assert_decision(
            outcome,
            &object,
            RetentionDecision::SkipUntilProof,
            LifecycleRetentionDecisionReason::TableAlreadyQuarantined,
        );
        assert!(
            outcome.quarantine_tokens().is_empty(),
            "already-quarantined object must not produce a quarantine token"
        );
    }
}

#[test]
fn quarantine_inventory_mismatch_blocks_candidate() {
    let branch = branch_id(0x72);
    let object = table_object(branch, 0, "orphan0107");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        telemetry_degraded_health(),
    )
    .with_telemetry_degraded_recovery_allowed(false);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::BlockedByRecoveryHealth
    );
    assert_decision(
        &outcome,
        &object,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::UnsafeRecoveryHealth,
    );
}

#[test]
fn quarantine_inventory_mismatch_records_repair_fact() {
    let branch = branch_id(0x73);
    let object = table_object(branch, 0, "orphan0108");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        telemetry_degraded_health(),
    )
    .with_telemetry_degraded_recovery_allowed(false);
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert!(matches!(
        outcome.recovery_health(),
        Some(RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::Telemetry,
            ..
        })
    ));
}

#[test]
fn reachability_does_not_call_quarantine_mutation() {
    // Source-level assertion: the table_reachability source must not
    // reference any quarantine mutation surface symbol. Static check
    // catches future regressions where the runtime tries to short-circuit
    // the quarantine maintenance slice by reaching into the quarantine
    // service. The terms are scoped
    // to call sites (trailing `(`) so the `quarantine_inventory` proof
    // field — a passive data carrier — doesn't match.
    let text = read_lifecycle_source("table_reachability.rs");
    for forbidden in [
        "quarantine_object(",
        "purge_quarantine(",
        "repair_branch_quarantine(",
        "repair_quarantine_family(",
        "QuarantineService::",
        "publish_quarantine(",
        "stage_quarantine(",
    ] {
        assert!(
            !text.contains(forbidden),
            "table_reachability.rs unexpectedly references {forbidden}"
        );
    }
}

#[test]
fn reachability_does_not_call_purge() {
    // Source-level assertion paired with the no-mutation runtime tests.
    let text = read_lifecycle_source("table_reachability.rs");
    for forbidden in [
        "purge_quarantine",
        "purge_object",
        "delete_object",
        "prune_snapshots",
        "truncate_wal",
    ] {
        assert!(
            !text.contains(forbidden),
            "table_reachability.rs unexpectedly references {forbidden}"
        );
    }
}

#[test]
fn candidate_state_changes_are_visible_in_maintenance_outcome() {
    let branch = branch_id(0x74);
    let object = table_object(branch, 0, "orphan0109");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request)
        .expect("outcome")
        .retention()
        .maintenance_outcome();

    assert_eq!(outcome.affected_object_names(), [object.to_string()]);
    assert_eq!(outcome.affected_objects(), 1);
}

#[test]
fn cache_table_object_retention_returns_unsupported() {
    let mut runtime = cache_runtime(branch_id(0x75));
    let error = runtime
        .enqueue_maintenance(branch_table_retention_task(branch_id(0x75)))
        .expect_err("cache rejects table retention");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
}

#[test]
fn cache_table_object_retention_does_not_list_objects() {
    // The cache runtime never holds a backend reference after open, so it
    // physically cannot call list_prefix at retention time. Assert this
    // structurally: cache.rs must not contain any list_prefix call site.
    let cache_text = read_lifecycle_source("cache.rs");
    assert!(
        !cache_text.contains("list_prefix"),
        "cache.rs unexpectedly calls list_prefix"
    );
    assert!(
        !cache_text.contains("list_inventory"),
        "cache.rs unexpectedly calls list_inventory"
    );
}

#[test]
fn cache_table_object_retention_does_not_construct_table_manifest_service() {
    // Cache must not construct or reference TableManifestService (the only
    // service that loads durable manifests). Source-level assertion is
    // load-bearing — a runtime spy would need to be wired into a different
    // backend impl, but the architectural rule is "cache never touches
    // these symbols at all".
    let cache_text = read_lifecycle_source("cache.rs");
    assert!(
        !cache_text.contains("TableManifestService"),
        "cache.rs unexpectedly references TableManifestService"
    );
    assert!(
        !cache_text.contains("load_all_current"),
        "cache.rs unexpectedly calls load_all_current"
    );
}

#[test]
fn cache_table_object_retention_does_not_claim_durable_reachability() {
    let request = LifecycleRetentionRequest::new(
        LifecycleRetentionScope::TableObjects {
            branch_id: branch_id(0x76),
        },
        1,
    );
    let proof = build_retention_proof(
        &request,
        Some(&manifest_db(1, 7)),
        &RecoveryHealth::Healthy,
        0,
    );

    assert_eq!(proof.status(), LifecycleRetentionProofStatus::Incomplete);
    assert_eq!(proof.missing_fact(), Some("table_reachability"));
}

#[test]
fn cache_table_object_retention_outcome_names_mode() {
    let mut runtime = cache_runtime(branch_id(0x77));
    let error = runtime
        .enqueue_maintenance(branch_table_retention_task(branch_id(0x77)))
        .expect_err("cache rejects table retention");

    assert!(error.to_string().contains("volatile runtime"));
}

#[test]
fn table_object_retention_does_not_delete_candidate_object() {
    let outcome = candidate_outcome();
    assert_no_direct_mutation(&outcome);
    // Deletion would show up as a pruned-object count. Classification must
    // emit a QuarantineCandidate decision without recording an
    // already-deleted object.
    assert_eq!(outcome.retention().objects_pruned(), 0);
    assert_eq!(outcome.retention().reclaimed_bytes(), 0);
}

#[test]
fn table_object_retention_does_not_move_candidate_to_quarantine() {
    let outcome = candidate_outcome();
    assert_no_direct_mutation(&outcome);
    // Moving an object into quarantine is a state change visible in the
    // maintenance outcome's state-change count. Classification is pure;
    // no state mutation must be recorded.
    assert_eq!(outcome.retention().maintenance_outcome().state_changes(), 0);
}

#[test]
fn table_object_retention_does_not_rewrite_quarantine_inventory() {
    let outcome = candidate_outcome();
    assert_no_direct_mutation(&outcome);
    // Quarantine inventory mutation would surface as an affected object
    // under the quarantine namespace. Reachability never names a
    // quarantine inventory object.
    for name in outcome
        .retention()
        .maintenance_outcome()
        .affected_object_names()
    {
        assert!(
            !name.starts_with("quarantine/"),
            "reachability decision named quarantine inventory object: {name}"
        );
    }
}

#[test]
fn table_object_retention_does_not_update_database_manifest() {
    let outcome = candidate_outcome();
    assert_no_direct_mutation(&outcome);
    // The database manifest is bumped via the checkpoint_required signal.
    // Reachability is a classification-only slice and must not set it.
    assert!(!outcome
        .retention()
        .maintenance_outcome()
        .checkpoint_required());
}

#[test]
fn table_object_retention_does_not_truncate_wal() {
    let outcome = candidate_outcome();
    assert_no_direct_mutation(&outcome);
    // WAL truncation reports bytes_reclaimed > 0 and "wal_segments/*"
    // affected objects. Reachability never reports either.
    assert_eq!(
        outcome.retention().maintenance_outcome().bytes_reclaimed(),
        0
    );
    for name in outcome
        .retention()
        .maintenance_outcome()
        .affected_object_names()
    {
        assert!(
            !name.starts_with("meta/wal/"),
            "reachability decision named WAL segment object: {name}"
        );
    }
}

#[test]
fn table_object_retention_does_not_prune_snapshots() {
    let outcome = candidate_outcome();
    assert_no_direct_mutation(&outcome);
    // Snapshot pruning is owned by a different family. Reachability must
    // not emit decisions for the snapshot family or name snapshot objects.
    for decision in outcome.retention().decisions() {
        assert_eq!(
            decision.family(),
            crate::lifecycle::LifecycleRetentionObjectFamily::Table,
            "non-table family decision leaked through reachability"
        );
    }
    for name in outcome
        .retention()
        .maintenance_outcome()
        .affected_object_names()
    {
        assert!(
            !name.starts_with("snapshots/"),
            "reachability decision named snapshot object: {name}"
        );
    }
}

#[test]
fn corrupt_manifest_health_blocks_orphan_candidate() {
    let branch = branch_id(0x78);
    let object = table_object(branch, 0, "orphan0110");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Failed {
            fault: RecoveryFault::new(RecoveryFaultKind::CorruptManifest, "corrupt manifest")
                .expect("fault"),
        },
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::UnsafeRecoveryHealth,
    );
}

#[test]
fn missing_manifest_health_blocks_orphan_candidate() {
    let branch = branch_id(0x79);
    let object = table_object(branch, 0, "orphan0111");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        data_loss_health(),
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::UnsafeRecoveryHealth,
    );
}

#[test]
fn manifest_ref_without_runtime_ref_still_retains_object() {
    // The reachability slice consults only durable manifest facts —
    // there is no runtime
    // refcount input. This test stresses that: build a request that
    // doesn't mention any runtime fact at all, just manifest + inventory,
    // and assert retention still recognizes the object as live.
    let branch = branch_id(0x8f);
    let live = table_object(branch, 0, "live0102");
    let request = request(
        branch,
        vec![manifest(
            branch,
            vec![table_ref(
                branch,
                0,
                "live0102",
                TableManifestTableProvenance::Flush,
            )],
            vec![],
        )],
        vec![entry(live.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &live,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableTable,
    );
    // Pair the assertion with a static check that table_reachability.rs
    // doesn't depend on a runtime refcount registry. The plan codifies
    // runtime refs as "acceleration, not durable truth"; this guard
    // catches any future commit that wires one in.
    let text = read_lifecycle_source("table_reachability.rs");
    for forbidden in ["SegmentRefRegistry", "RefRegistry", "refcount"] {
        assert!(
            !text.contains(forbidden),
            "table_reachability.rs unexpectedly references runtime refcount {forbidden}"
        );
    }
}

#[test]
fn deleted_parent_orphan_storage_is_not_clean_success() {
    let branch = branch_id(0x7a);
    let parent = branch_id(0x7b);
    let inherited = table_object(parent, 0, "missing0112");
    let layer = inherited_layer(
        parent,
        vec![table_ref(
            parent,
            0,
            "missing0112",
            TableManifestTableProvenance::Flush,
        )],
    );
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![layer])],
        vec![],
        vec![],
        RecoveryHealth::Healthy,
    );

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_eq!(
        outcome.status(),
        LifecycleRetentionStatus::DeferredIncompleteProof
    );
    assert!(outcome.decisions().is_empty());
    assert!(outcome.quarantine_tokens().is_empty());
    assert!(inherited.as_str().contains("missing0112"));
}

#[test]
fn quarantine_only_directory_does_not_recreate_manifest() {
    let branch = branch_id(0x7c);
    let object = table_object(branch, 0, "quar0113");
    let request = request(
        branch,
        vec![],
        vec![entry(object.clone(), 100)],
        vec![object.clone()],
        RecoveryHealth::Healthy,
    )
    .with_manifest_complete(false);

    let outcome = table_object_retention_outcome(&request).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::SkipUntilProof,
        LifecycleRetentionDecisionReason::ProofIncomplete,
    );
}

#[test]
fn shared_object_survives_one_branch_manifest_drop() {
    // Three references: branch X (owned), branch Y (inherited), branch Z
    // (inherited). Drop branch Z. The object stays retained because two
    // manifest refs remain. This tests partial-drop semantics that the
    // shared-reasons fixture doesn't probe.
    let owner = branch_id(0x91);
    let observer_y = branch_id(0x92);
    let object = table_object(owner, 0, "shared0103");
    let request = request(
        owner,
        vec![
            manifest(
                owner,
                vec![table_ref(
                    owner,
                    0,
                    "shared0103",
                    TableManifestTableProvenance::Flush,
                )],
                vec![],
            ),
            manifest(
                observer_y,
                vec![],
                vec![inherited_layer(
                    owner,
                    vec![table_ref(
                        owner,
                        0,
                        "shared0103",
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

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::Retain,
        LifecycleRetentionDecisionReason::ReachableSharedTable,
    );
}

#[test]
fn shared_object_becomes_candidate_after_all_manifest_refs_drop() {
    // Counterpart to shared_object_survives_one_branch_manifest_drop:
    // dropping ALL manifest refs releases the object to candidate. Use
    // distinct branches and an inventory size > 1 to keep classifier
    // ordering exercised.
    let owner = branch_id(0x93);
    let observer = branch_id(0x94);
    let object = table_object(owner, 0, "shared0104");
    let companion = table_object(owner, 0, "companion0001");

    let dropped = request(
        owner,
        vec![
            manifest(owner, vec![], vec![]),
            manifest(observer, vec![], vec![]),
        ],
        vec![entry(object.clone(), 100), entry(companion.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&dropped).expect("outcome");

    assert_decision(
        &outcome,
        &object,
        RetentionDecision::QuarantineCandidate,
        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
    );
    assert_decision(
        &outcome,
        &companion,
        RetentionDecision::QuarantineCandidate,
        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
    );
    assert_eq!(outcome.quarantine_tokens().len(), 2);
}

fn mixed_reachability_outcome() -> (
    LifecycleTableObjectRetentionOutcome,
    ObjectName,
    ObjectName,
    ObjectName,
    ObjectName,
) {
    let branch = branch_id(0x80);
    let parent = branch_id(0x81);
    let other = branch_id(0x82);
    let owned = table_object(branch, 0, "owned0101");
    let inherited_object = table_object(parent, 0, "parent0101");
    let shared = table_object(branch, 0, "shared0102");
    let orphan = table_object(branch, 0, "orphan0114");
    let request = request(
        branch,
        vec![
            manifest(
                branch,
                vec![
                    table_ref(branch, 0, "owned0101", TableManifestTableProvenance::Flush),
                    table_ref(branch, 1, "shared0102", TableManifestTableProvenance::Flush),
                ],
                vec![inherited_layer(
                    parent,
                    vec![table_ref(
                        parent,
                        0,
                        "parent0101",
                        TableManifestTableProvenance::Flush,
                    )],
                )],
            ),
            manifest(
                other,
                vec![],
                vec![inherited_layer(
                    branch,
                    vec![table_ref(
                        branch,
                        0,
                        "shared0102",
                        TableManifestTableProvenance::Recovered,
                    )],
                )],
            ),
        ],
        vec![
            entry(owned.clone(), 100),
            entry(inherited_object.clone(), 100),
            entry(shared.clone(), 100),
            entry(orphan.clone(), 100),
        ],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");
    (outcome, owned, inherited_object, shared, orphan)
}

fn candidate_outcome() -> LifecycleTableObjectRetentionOutcome {
    let branch = branch_id(0x83);
    let object = table_object(branch, 0, "orphan0115");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object, 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    table_object_retention_outcome(&request).expect("outcome")
}

fn read_lifecycle_source(file_name: &str) -> String {
    let path = format!("{}/src/lifecycle/{}", env!("CARGO_MANIFEST_DIR"), file_name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read lifecycle source {file_name}: {error}"))
}

fn fresh_token_with_context() -> (
    LifecycleTableObjectProofToken,
    ObjectName,
    LifecycleTableObjectProofContext,
) {
    let branch = branch_id(0x84);
    let object = table_object(branch, 0, "orphan0116");
    let request = request(
        branch,
        vec![manifest(branch, vec![], vec![])],
        vec![entry(object.clone(), 100)],
        vec![],
        RecoveryHealth::Healthy,
    );
    let outcome = table_object_retention_outcome(&request).expect("outcome");
    (
        outcome.quarantine_tokens().first().expect("token").clone(),
        object,
        outcome.proof_context().clone(),
    )
}

fn assert_decision(
    outcome: &LifecycleTableObjectRetentionOutcome,
    object: &ObjectName,
    decision: RetentionDecision,
    reason: LifecycleRetentionDecisionReason,
) {
    let record = outcome
        .decisions()
        .iter()
        .find(|record| record.object() == Some(object))
        .expect("decision for object");
    assert_eq!(record.decision(), decision);
    assert_eq!(record.reason(), reason);
}

fn assert_no_direct_mutation(outcome: &LifecycleTableObjectRetentionOutcome) {
    assert!(outcome.decisions().iter().all(|decision| {
        !matches!(
            decision.decision(),
            RetentionDecision::PruneCandidate | RetentionDecision::PurgeCandidate
        )
    }));
}

fn branch_table_retention_task(branch_id: BranchId) -> MaintenanceTaskRequest {
    MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Retention,
        MaintenanceTaskPriority::Low,
        MaintenanceTaskScope::Branch(branch_id),
        MaintenanceTaskPolicy::coalescing(),
    )
    .expect("branch table retention task")
}

fn cache_runtime(branch_id: BranchId) -> LifecycleCacheRuntime {
    let backend = crate::backend::memory::MemoryBackend::new();
    LifecycleCacheRuntime::open(
        LifecycleCacheOpenRequest::new(
            StorageOpenPlan::new(
                StorageMode::Cache,
                LifecycleCodecId::identity(),
                RecoveryStrictness::Strict,
                LifecycleConfig::default(),
            )
            .expect("open plan"),
            branch_id,
            crate::commit::CommitBranchGeneration::new(1).expect("generation"),
        )
        .expect("request"),
        &backend,
        crate::branch::config::BranchRuntimeConfig::default(),
        crate::commit::CommitRuntimeConfig::default(),
        crate::commit::CommitManualTimestampSource::new(strata_core::Timestamp::from_micros(10)),
    )
    .expect("runtime")
}

fn manifest_db(snapshot_id: u64, snapshot_watermark: u64) -> crate::format::DatabaseManifest {
    crate::format::DatabaseManifest::new([0x77; 16], "identity")
        .expect("manifest")
        .with_recovery_facts(
            1,
            Some(snapshot_watermark),
            Some(snapshot_id),
            Some(CommitVersion::new(snapshot_watermark)),
        )
        .expect("recovery facts")
}

fn telemetry_degraded_health() -> RecoveryHealth {
    RecoveryHealth::Degraded {
        class: RecoveryDegradationClass::Telemetry,
        faults: vec![RecoveryFault::new(
            RecoveryFaultKind::QuarantineInventoryMismatch,
            "inventory mismatch",
        )
        .expect("fault")],
    }
}

fn data_loss_health() -> RecoveryHealth {
    RecoveryHealth::Degraded {
        class: RecoveryDegradationClass::DataLoss,
        faults: vec![
            RecoveryFault::new(RecoveryFaultKind::MissingTableObject, "missing object")
                .expect("fault"),
        ],
    }
}
