#![allow(clippy::too_many_lines)]

use super::shared::*;
use super::*;
use crate::backend::Backend;
use crate::branch::facts::BranchLevel;
use crate::branch::read::{
    BranchHistoryOptions, BranchReadBound, BranchScanBounds, BranchUserKeyBound,
};
use crate::branch::state::compaction::{
    BranchCompactionKind, BranchCompactionRequest, BranchCompactionRetentionPolicy,
};
use crate::branch::state::BranchLocalState;
use crate::commit::{CommitBranchGeneration, CommitManualTimestampSource};
#[cfg(feature = "perf-trace")]
use crate::lifecycle::tests::checkpoint::shared::open_runtime_with_lifecycle_config;
use crate::lifecycle::tests::checkpoint::shared::{
    assemble_shell, durable_batch, generation_guard, open_runtime,
    physical_key as checkpoint_physical_key, CheckpointTestBackend,
};
use crate::row::StorageSpaceId;
use strata_core::{CommitVersion, Timestamp};

#[test]
fn durable_rewrite_rejects_cache_durable_publication_request() {
    let branch = branch_id(0xb0);
    let mut state = BranchLocalState::empty(branch);
    install_compaction_inputs(&mut state, branch, "cache-durable");

    let outcome = compact_cache_branch(&mut state, &compaction_request(branch, "cache-durable"))
        .expect("cache compaction");

    assert_eq!(outcome.status(), LifecycleCompactionStatus::Completed);
    assert!(outcome.durable_output_objects().is_empty());
    assert!(!outcome.checkpoint_required());
    assert!(outcome.recovery_health().is_none());
}

#[test]
fn durable_rewrite_rejects_before_open() {
    let branch = branch_id(0xb1);
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");

    let error = executor
        .enqueue(
            LifecycleStateMachine::new(),
            MaintenanceTaskRequest::compaction(branch, 0),
        )
        .expect_err("closed admission");

    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn durable_rewrite_rejects_while_closing() {
    let branch = branch_id(0xb2);
    let mut runtime = cache_runtime(branch);
    runtime
        .force_close_requested_for_test()
        .expect("close requested");

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "closing"))
        .expect_err("closing admission");

    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(runtime.state(), LifecycleState::Closing);
}

#[test]
fn durable_rewrite_rejects_empty_output_seed() {
    let branch = branch_id(0xb3);

    let error = LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "")
        .expect_err("empty seed");

    assert_eq!(error.code(), "failed_precondition.lifecycle.branch_runtime");
}

#[test]
fn durable_rewrite_rejects_path_like_output_seed() {
    let branch = branch_id(0xb4);

    let error =
        LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "bad/seed")
            .expect_err("path-like seed");

    assert_eq!(error.code(), "failed_precondition.lifecycle.branch_runtime");
}

#[test]
fn durable_rewrite_rejects_pruning_policy_without_retention_proof() {
    let branch = branch_id(0xb5);
    let mut state = BranchLocalState::empty(branch);
    install_compaction_inputs(&mut state, branch, "prune-request");
    let request =
        BranchCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "prune-output")
            .expect("branch request")
            .with_retention_policy(BranchCompactionRetentionPolicy::DropTombstones);

    let error = state
        .compact_branch_owned_tables(&request)
        .expect_err("retention proof required");

    assert!(matches!(
        error,
        crate::branch::error::BranchRuntimeError::InvalidCompaction { .. }
    ));
    assert_eq!(state.owned_table_count(), 2);
}

#[test]
fn durable_rewrite_uses_ordinary_maintenance_admission() {
    let branch = branch_id(0xb6);
    let mut runtime = cache_runtime(branch);
    runtime
        .force_close_requested_for_test()
        .expect("close requested");

    let error = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect_err("ordinary maintenance rejected while closing");

    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn durable_rewrite_releases_admission_after_publish_failure() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb7);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "admission-release");
    backend.fail_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "admission-release"))
        .expect_err("publish failure");
    let close = runtime.close().expect("close after failed rewrite");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.rewrite_publication"
    );
    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
}

#[test]
fn durable_compaction_publishes_output_before_install() {
    let (backend, outcome) = successful_compaction(branch_id(0xb8), "publish-before-install");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert_eq!(backend.table_object_create_calls(), 1);
    assert_eq!(backend.table_manifest_replace_calls(), 1);
}

#[test]
fn durable_compaction_reopens_output_before_install() {
    let branch = branch_id(0xb9);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "reopen-before-install");

    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, "reopen-before-install"))
        .expect("durable compaction");
    let manifest = runtime
        .table_catalog()
        .build_manifest(runtime.branch_state())
        .expect("catalog manifest");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert_eq!(
        manifest
            .levels()
            .iter()
            .map(|level| level.tables().len())
            .sum::<usize>(),
        1
    );
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
}

#[test]
fn durable_compaction_validates_output_facts_before_install() {
    let branch = branch_id(0xba);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(
        runtime.branch_state_mut(),
        branch,
        "validate-before-install",
    );
    backend.corrupt_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "validate-before-install"))
        .expect_err("corrupt reopened output");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
    // The FRESH output's byte-exact validation is the stage that catches a
    // corrupt publish — not a later reopen. A valid-format object with wrong
    // content would reopen cleanly, so skipping this stage for fresh outputs
    // would install wrong bytes (#3382 pinned the `!adopted` gate).
    assert!(
        matches!(
            &error,
            LifecycleError::RewritePublicationOrphaned { reason, .. }
                if *reason == "table rewrite published output before byte-exact validation failed"
        ),
        "expected the byte-exact validation stage, got: {error:?}"
    );
    assert_eq!(runtime.branch_state().owned_table_count(), 2);
}

#[test]
fn durable_compaction_installs_only_after_all_outputs_validate() {
    let branch = branch_id(0xbb);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_many_inputs(runtime.branch_state_mut(), branch, "all-validate", 6);
    backend.corrupt_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "all-validate"))
        .expect_err("validation failure");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
    assert_eq!(runtime.branch_state().owned_table_count(), 6);
}

#[test]
fn durable_compaction_no_candidate_is_deferred() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xfa);
    let mut runtime = open_runtime(branch, backend);

    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, "no-candidate"))
        .expect("no candidate");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::DeferredNoCandidate
    );
    assert_eq!(backend.table_object_create_calls(), 0);
    assert_eq!(backend.table_manifest_replace_calls(), 0);
}

#[test]
fn durable_compaction_manifest_includes_outputs() {
    let branch = branch_id(0xbc);
    let (backend, _) = successful_compaction(branch, "manifest-outputs");
    let runtime = open_runtime(branch, backend);
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");

    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert!(manifest.levels()[0].tables()[0]
        .table_identity()
        .as_str()
        .contains("manifest-outputs"));
}

#[test]
fn durable_compaction_manifest_excludes_replaced_inputs() {
    let branch = branch_id(0xbd);
    let (backend, _) = successful_compaction(branch, "manifest-excludes");
    let runtime = open_runtime(branch, backend);
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");
    let table_identity = manifest.levels()[0].tables()[0].table_identity().as_str();

    assert!(!table_identity.contains("manifest-excludes-left"));
    assert!(!table_identity.contains("manifest-excludes-right"));
}

#[test]
fn durable_compaction_metadata_promotion_updates_manifest_without_table_publish() {
    let branch = branch_id(0x6b);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-promote-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-promote-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    let first_nonzero_level = BranchLevel::new(1);
    let target_level = BranchLevel::new(2);

    let rewrite_request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0ToLevelOne,
        "durable-promote-rewrite",
    )
    .expect("rewrite request");
    let rewrite_outcome = runtime
        .compact_branch_tables(&rewrite_request)
        .expect("durable rewrite");
    let table_object_calls_before_promotion = backend.table_object_create_calls();
    let manifest_calls_before_promotion = backend.table_manifest_replace_calls();

    let promotion_request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactLevel {
            level: first_nonzero_level,
            table_index: 0,
        },
        "durable-promote",
    )
    .expect("promotion request");
    let outcome = runtime
        .compact_branch_tables(&promotion_request)
        .expect("metadata promotion");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");
    let branch_outcome = outcome.branch_outcome();

    assert_eq!(
        rewrite_outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert!(outcome.plan().is_metadata_promotion());
    assert!(outcome.durable_output_objects().is_empty());
    assert!(branch_outcome.table_report().is_none());
    assert_eq!(branch_outcome.removed_refs().len(), 1);
    assert_eq!(branch_outcome.output_refs().len(), 1);
    assert_eq!(
        branch_outcome.removed_refs()[0].table_identity(),
        branch_outcome.output_refs()[0].table_identity()
    );
    assert_eq!(
        backend.table_object_create_calls(),
        table_object_calls_before_promotion
    );
    assert_eq!(
        backend.table_manifest_replace_calls(),
        manifest_calls_before_promotion + 1
    );
    assert!(
        runtime.branch_state().owned_levels()[usize::from(first_nonzero_level.raw())].is_empty()
    );
    assert_eq!(
        runtime.branch_state().owned_levels()[usize::from(target_level.raw())].len(),
        1
    );
    assert_eq!(manifest.levels().len(), 1);
    assert_eq!(manifest.levels()[0].level(), target_level);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert_eq!(
        manifest.levels()[0].tables()[0].table_identity(),
        branch_outcome.output_refs()[0].table_identity()
    );

    let promoted_object = manifest.levels()[0].tables()[0].object().clone();
    let retention = runtime
        .prove_retention(&LifecycleRetentionRequest::new(
            LifecycleRetentionScope::TableObjects { branch_id: branch },
            1,
        ))
        .expect("table object retention proof");
    let table_decisions = retention
        .decisions()
        .iter()
        .filter(|decision| decision.object().is_some())
        .collect::<Vec<_>>();
    assert_eq!(retention.status(), LifecycleRetentionStatus::Completed);
    assert_eq!(table_decisions.len(), 1);
    assert_eq!(table_decisions[0].object(), Some(&promoted_object));
    assert_eq!(table_decisions[0].decision(), RetentionDecision::Retain);
    assert_eq!(
        table_decisions[0].reason(),
        LifecycleRetentionDecisionReason::ReachableTable
    );

    let promoted_identity = branch_outcome.output_refs()[0].table_identity().clone();
    drop(runtime);
    let mut reopened = open_runtime(branch, backend);
    assert!(
        reopened.branch_state().owned_levels()[usize::from(first_nonzero_level.raw())].is_empty()
    );
    assert_eq!(
        reopened.branch_state().owned_levels()[usize::from(target_level.raw())].len(),
        1
    );
    assert_eq!(
        reopened.branch_state().owned_levels()[usize::from(target_level.raw())][0]
            .descriptor()
            .identity(),
        &promoted_identity
    );
    assert_eq!(
        latest_value_from_state(reopened.branch_state(), branch, b"right"),
        Some(b"right".to_vec())
    );

    let reopened_retention = reopened
        .prove_retention(&LifecycleRetentionRequest::new(
            LifecycleRetentionScope::TableObjects { branch_id: branch },
            1,
        ))
        .expect("reopened table object retention proof");
    let reopened_table_decisions = reopened_retention
        .decisions()
        .iter()
        .filter(|decision| decision.object().is_some())
        .collect::<Vec<_>>();
    assert_eq!(
        reopened_retention.status(),
        LifecycleRetentionStatus::Completed
    );
    assert_eq!(reopened_table_decisions.len(), 1);
    assert_eq!(reopened_table_decisions[0].object(), Some(&promoted_object));
    assert_eq!(
        reopened_table_decisions[0].decision(),
        RetentionDecision::Retain
    );
    assert_eq!(
        reopened_table_decisions[0].reason(),
        LifecycleRetentionDecisionReason::ReachableTable
    );
}

#[test]
fn durable_compaction_metadata_promotion_manifest_failure_uses_previous_manifest_on_reopen() {
    let branch = branch_id(0x6c);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "promotion-debt-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "promotion-debt-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    let first_nonzero_level = BranchLevel::new(1);
    let target_level = BranchLevel::new(2);

    let rewrite_request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0ToLevelOne,
        "promotion-debt-rewrite",
    )
    .expect("rewrite request");
    runtime
        .compact_branch_tables(&rewrite_request)
        .expect("durable rewrite");
    assert_eq!(backend.table_manifest_replace_calls(), 1);
    let table_object_calls_before_promotion = backend.table_object_create_calls();
    backend.fail_table_manifest_replacement_on_call(2);

    let promotion_request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactLevel {
            level: first_nonzero_level,
            table_index: 0,
        },
        "promotion-debt",
    )
    .expect("promotion request");
    let outcome = runtime
        .compact_branch_tables(&promotion_request)
        .expect("manifest debt");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedManifestDebt
    );
    assert!(outcome.recovery_health().is_some());
    assert_eq!(
        backend.table_object_create_calls(),
        table_object_calls_before_promotion
    );
    assert_eq!(backend.table_manifest_replace_calls(), 2);
    assert!(
        runtime.branch_state().owned_levels()[usize::from(first_nonzero_level.raw())].is_empty()
    );
    assert_eq!(
        runtime.branch_state().owned_levels()[usize::from(target_level.raw())].len(),
        1
    );
    assert_eq!(
        latest_value_from_state(runtime.branch_state(), branch, b"right"),
        Some(b"right".to_vec())
    );

    drop(runtime);
    let reopened = open_runtime(branch, backend);
    assert_eq!(
        reopened.branch_state().owned_levels()[usize::from(first_nonzero_level.raw())].len(),
        1
    );
    assert!(reopened.branch_state().owned_levels()[usize::from(target_level.raw())].is_empty());
    assert_eq!(
        latest_value_from_state(reopened.branch_state(), branch, b"right"),
        Some(b"right".to_vec())
    );
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_explicit_compaction_drain_obeys_io_budget_policy_before_publish() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let branch = branch_id(0x6d);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let config = LifecycleConfig::default()
        .with_compaction_io_policy(LifecycleCompactionIoPolicy::per_task_byte_budget(1))
        .expect("compaction IO policy");
    let mut runtime = open_runtime_with_lifecycle_config(branch, backend, config);
    for index in 0..4 {
        install_l0_table(
            runtime.branch_state_mut(),
            branch,
            &format!("durable-explicit-budget-l0-{index}"),
            vec![put_row(
                branch,
                format!("durable-explicit-budget-l0-{index}").as_bytes(),
                index + 1,
                (index + 1) * 1_000,
                &[0x6d; 1024],
            )],
        );
    }
    crate::observability::perf_trace::reset();

    let outcome = runtime
        .compact_branch_tables_to_fixed_point(
            &LifecycleCompactionDrainRequest::new(branch, "durable-explicit-budget")
                .expect("drain request"),
        )
        .expect("explicit durable compaction drain");
    let maintenance = outcome.maintenance_outcome();
    let perf = crate::observability::perf_trace::snapshot();

    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        maintenance.reason(),
        Some("compaction IO byte budget deferred table rewrite")
    );
    assert!(maintenance.retryable());
    assert_eq!(outcome.operations_attempted(), 1);
    assert_eq!(outcome.operations_installed(), 0);
    assert_eq!(runtime.branch_state().owned_levels()[0].len(), 4);
    assert_eq!(backend.table_object_create_calls(), 0);
    assert_eq!(perf.lifecycle_compaction_io_budget_deferrals(), 1);
    assert_eq!(perf.lifecycle_compaction_operations_completed(), 0);
}

#[test]
fn durable_compaction_catalog_marks_replaced_inputs_retained() {
    let branch = branch_id(0xbe);
    let (_, outcome) = successful_compaction(branch, "retained-inputs");
    let maintenance = outcome.maintenance_outcome();
    let names = maintenance.affected_object_names().to_vec();

    // One published output ObjectName + two retained input identities, no
    // duplicate entries per logical object.
    assert_eq!(names.len(), 3);
    assert_eq!(maintenance.affected_objects(), 3);
    assert!(names.iter().any(|name| name.contains("retained-inputs")));
    assert!(names
        .iter()
        .any(|name| name.contains("retained-inputs-left")));
    assert!(names
        .iter()
        .any(|name| name.contains("retained-inputs-right")));
}

#[test]
fn durable_compaction_output_identities_are_retry_stable() {
    let branch = branch_id(0xbf);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    {
        let mut runtime = open_runtime(branch, backend);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "stable-output");
        runtime
            .compact_branch_tables(&compaction_request(branch, "stable-output"))
            .expect("first compaction");
    }
    let first_objects = backend.table_object_names();
    {
        let mut runtime = open_runtime(branch, backend);
        *runtime.branch_state_mut() = BranchLocalState::empty(branch);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "stable-output");
        runtime
            .compact_branch_tables(&compaction_request(branch, "stable-output"))
            .expect("retry compaction");
    }

    assert_eq!(backend.table_object_names(), first_objects);
}

#[test]
fn adopted_rewrite_output_vanishing_mid_build_is_a_sweep_race_not_a_failure() {
    let branch = branch_id(0xf1);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    {
        // The abandoned earlier attempt: same inputs, output published; the
        // discarded runtime stands in for the install that never landed.
        let mut runtime = open_runtime(branch, backend);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "swept-adopt");
        runtime
            .compact_branch_tables(&compaction_request(branch, "swept-adopt"))
            .expect("first compaction");
    }
    let object = backend
        .table_object_names()
        .into_iter()
        .next()
        .expect("published output");
    let mut runtime = open_runtime(branch, backend);
    *runtime.branch_state_mut() = BranchLocalState::empty(branch);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "swept-adopt");
    // The retry adopts the existing content-identical output; a concurrent
    // table-object sweep — for which the abandoned output is legitimate
    // garbage — unlinks it between the adoption and the build's read of it.
    backend.vanish_object_on_next_read(object);

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "swept-adopt"))
        .expect_err("adopted output swept mid-build is the benign sweep race");

    assert_eq!(
        error.code(),
        "unavailable.lifecycle.rewrite_output_sweep_race"
    );
}

#[test]
fn adopted_rewrite_output_with_conflicting_bytes_still_fails_closed() {
    let branch = branch_id(0xf2);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    {
        let mut runtime = open_runtime(branch, backend);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "conflict-adopt");
        runtime
            .compact_branch_tables(&compaction_request(branch, "conflict-adopt"))
            .expect("first compaction");
    }
    let object = backend
        .table_object_names()
        .into_iter()
        .next()
        .expect("published output");
    let mut runtime = open_runtime(branch, backend);
    *runtime.branch_state_mut() = BranchLocalState::empty(branch);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "conflict-adopt");
    // The existing object is present but holds different bytes (corrupted
    // after open, so recovery's own fact verification does not fire first): a
    // byte conflict on a content-deterministic id is corruption, not the
    // sweep race — it must keep failing closed.
    backend.replace_object_bytes(&object, b"conflicting bytes".to_vec());

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "conflict-adopt"))
        .expect_err("conflicting adopted bytes fail closed");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.rewrite_publication"
    );
}

#[test]
fn fresh_rewrite_output_vanishing_mid_build_still_fails_closed() {
    // Learn the deterministic output name from a scout run on a separate
    // backend (identical inputs derive an identical name).
    let branch = branch_id(0xf3);
    let scout_backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    {
        let mut scout = open_runtime(branch, scout_backend);
        install_compaction_inputs(scout.branch_state_mut(), branch, "fresh-vanish");
        scout
            .compact_branch_tables(&compaction_request(branch, "fresh-vanish"))
            .expect("scout compaction");
    }
    let object = scout_backend
        .table_object_names()
        .into_iter()
        .next()
        .expect("published output");
    // A FRESH publish whose object disappears before the build's read is not
    // the adoption race — nothing else may touch a reserved fresh output, so
    // this keeps failing closed as a publication orphan.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "fresh-vanish");
    backend.vanish_object_on_next_read(object);

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "fresh-vanish"))
        .expect_err("fresh vanish fails closed");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
}

#[test]
fn durable_materialization_binds_handle_before_output_publish() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xc1);
    let mut runtime = materialization_runtime(child, branch_id(0xc0), backend);

    let outcome = runtime
        .materialize_inherited_layer(&materialization_request(child, "handle-before-publish"))
        .expect("materialization");

    assert!(outcome.materialization_handle().is_some());
    assert!(outcome.reachability_snapshot().is_some());
    assert_eq!(backend.table_object_create_calls(), 1);
}

#[test]
fn durable_materialization_publishes_replacement_before_layer_removal() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xc3);
    let mut runtime = materialization_runtime(child, branch_id(0xc2), backend);

    let outcome = runtime
        .materialize_inherited_layer(&materialization_request(child, "replacement-before-remove"))
        .expect("materialization");

    assert_eq!(
        outcome.status(),
        LifecycleMaterializationStatus::CompletedDurable
    );
    assert_eq!(backend.table_object_create_calls(), 1);
    assert_eq!(runtime.branch_state().inherited_layer_count(), 0);
}

#[test]
fn durable_materialization_reopens_replacement_before_install() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xc5);
    let mut runtime = materialization_runtime(child, branch_id(0xc4), backend);

    let outcome = runtime
        .materialize_inherited_layer(&materialization_request(child, "reopen-replacement"))
        .expect("materialization");

    assert_eq!(
        outcome.status(),
        LifecycleMaterializationStatus::CompletedDurable
    );
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
}

#[test]
fn durable_materialization_validates_replacement_facts() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xc7);
    let mut runtime = materialization_runtime(child, branch_id(0xc6), backend);
    backend.corrupt_table_object_create_on_call(1);

    let error = runtime
        .materialize_inherited_layer(&materialization_request(child, "validate-replacement"))
        .expect_err("corrupt replacement");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
    assert_eq!(runtime.branch_state().inherited_layer_count(), 1);
}

#[test]
fn durable_materialization_manifest_removes_inherited_layer() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xc9);
    let mut runtime = materialization_runtime(child, branch_id(0xc8), backend);

    runtime
        .materialize_inherited_layer(&materialization_request(child, "manifest-removes-layer"))
        .expect("materialization");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load manifest")
        .expect("manifest");

    assert!(manifest.inherited_layers().is_empty());
}

#[test]
fn durable_materialization_manifest_includes_replacements() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xcb);
    let mut runtime = materialization_runtime(child, branch_id(0xca), backend);

    let outcome = runtime
        .materialize_inherited_layer(&materialization_request(child, "manifest-replacements"))
        .expect("materialization");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load manifest")
        .expect("manifest");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert!(manifest.levels()[0].tables()[0]
        .table_identity()
        .as_str()
        .contains("manifest-replacements"));
    // One published replacement ObjectName, no duplicate identity entry.
    assert_eq!(maintenance.affected_object_names().len(), 1);
    assert_eq!(maintenance.affected_objects(), 1);
}

#[test]
fn durable_materialization_preserves_child_local_precedence() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let parent = branch_id(0xcc);
    let child = branch_id(0xcd);
    let mut runtime = open_runtime(child, backend);
    *runtime.branch_state_mut() = materialization_read_state(parent, child);
    let shared = physical_key(child, b"shared");

    runtime
        .materialize_inherited_layer(&materialization_request(child, "child-precedence"))
        .expect("materialization");
    let visible = runtime
        .branch_state()
        .capture_read_view()
        .expect("view")
        .latest(&shared)
        .expect("latest")
        .expect("visible")
        .row()
        .clone();

    assert_eq!(visible.value(), b"child-shared");
}

#[test]
fn durable_materialization_retry_after_removed_layer_uses_source_identity() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xcf);
    let mut runtime = materialization_runtime(child, branch_id(0xce), backend);
    let first = runtime
        .materialize_inherited_layer(&materialization_request(child, "source-retry"))
        .expect("first materialization");
    let handle = first
        .materialization_handle()
        .expect("materialization handle");

    let retry = runtime
        .materialize_inherited_layer(
            &LifecycleMaterializationRequest::from_handle(handle, "source-retry").expect("retry"),
        )
        .expect("retry materialization");

    assert_eq!(
        retry.status(),
        LifecycleMaterializationStatus::AlreadyMaterialized
    );
}

#[test]
fn durable_materialization_rejects_stale_layer_index_task() {
    let branch = branch_id(0xd0);
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");

    let queued = executor
        .enqueue(
            open_state(),
            MaintenanceTaskRequest::materialization_layer(branch, 0),
        )
        .expect("enqueue");
    let mut state = BranchLocalState::empty(branch);
    let mut runner = TestMaterializationRunner { branch: &mut state };
    let outcome = executor
        .run_next(open_state(), &mut runner)
        .expect("run")
        .expect("outcome");

    assert_eq!(outcome.task_id(), Some(queued.task_id()));
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
}

#[test]
fn durable_compaction_preserves_latest_reads() {
    let branch = branch_id(0xd1);
    let (before, after) = compact_read_parity(branch);

    assert_eq!(
        latest_value(&before, branch, b"scan-a"),
        latest_value(&after, branch, b"scan-a")
    );
}

#[test]
fn durable_compaction_preserves_history_reads() {
    let branch = branch_id(0xd2);
    let (before, after) = compact_read_parity(branch);
    let key = physical_key(branch, b"history");

    assert_eq!(
        history_versions(
            &before
                .history(&key, BranchHistoryOptions::all())
                .expect("before")
        ),
        history_versions(
            &after
                .history(&key, BranchHistoryOptions::all())
                .expect("after")
        )
    );
}

#[test]
fn durable_compaction_preserves_prefix_scans() {
    let branch = branch_id(0xd3);
    let (before, after) = compact_read_parity(branch);
    let prefix = physical_key(branch, b"scan-");

    assert_eq!(
        scan_user_keys(
            &before
                .scan_prefix(
                    &BranchScanBounds::prefix(&prefix),
                    BranchReadBound::latest()
                )
                .expect("before prefix")
        ),
        scan_user_keys(
            &after
                .scan_prefix(
                    &BranchScanBounds::prefix(&prefix),
                    BranchReadBound::latest()
                )
                .expect("after prefix")
        )
    );
}

#[test]
fn durable_compaction_preserves_range_scans() {
    let branch = branch_id(0xd4);
    let (before, after) = compact_read_parity(branch);
    let bounds = BranchScanBounds::range(
        branch,
        "rewrite",
        StorageSpaceId::engine(0x51).expect("space"),
        BranchUserKeyBound::Included(b"scan-a".to_vec()),
        BranchUserKeyBound::Included(b"scan-z".to_vec()),
    )
    .expect("bounds");

    assert_eq!(
        scan_user_keys(
            &before
                .scan_range(&bounds, BranchReadBound::latest())
                .expect("before range")
        ),
        scan_user_keys(
            &after
                .scan_range(&bounds, BranchReadBound::latest())
                .expect("after range")
        )
    );
}

#[test]
fn durable_compaction_preserves_tombstones() {
    let branch = branch_id(0xd5);
    let (_, after) = compact_read_parity(branch);
    let key = physical_key(branch, b"history");

    assert!(after
        .history(&key, BranchHistoryOptions::all())
        .expect("history")
        .iter()
        .any(|row| row.row().is_tombstone()));
}

#[test]
fn durable_compaction_preserves_ttl_expired_rows_under_keep_all() {
    let branch = branch_id(0xd6);
    let (_, after) = compact_read_parity(branch);
    let key = physical_key(branch, b"scan-b");

    assert!(after
        .history(&key, BranchHistoryOptions::all())
        .expect("history")
        .iter()
        .any(|row| row.row().expires_at() == Timestamp::from_micros(4_500)));
}

#[test]
fn durable_compaction_preserves_commit_timestamps() {
    let branch = branch_id(0xd7);
    let (_, after) = compact_read_parity(branch);

    assert_eq!(
        after
            .latest(&physical_key(branch, b"scan-a"))
            .expect("latest")
            .expect("visible")
            .row()
            .commit_timestamp(),
        Timestamp::from_micros(3_000)
    );
}

#[test]
fn durable_materialization_preserves_latest_reads() {
    let child = branch_id(0xd9);
    let before_after = materialization_read_parity(branch_id(0xd8), child);

    assert_eq!(
        latest_value(&before_after.0, child, b"scan-a"),
        latest_value(&before_after.1, child, b"scan-a")
    );
}

#[test]
fn durable_materialization_preserves_history_reads() {
    let child = branch_id(0xdb);
    let (before, after) = materialization_read_parity(branch_id(0xda), child);
    let key = physical_key(child, b"history");

    assert_eq!(
        history_versions(
            &before
                .history(&key, BranchHistoryOptions::all())
                .expect("before")
        ),
        history_versions(
            &after
                .history(&key, BranchHistoryOptions::all())
                .expect("after")
        )
    );
}

#[test]
fn durable_materialization_preserves_fork_version_gate() {
    let child = branch_id(0xdd);
    let (_, after) = materialization_read_parity(branch_id(0xdc), child);

    assert!(after
        .latest(&physical_key(child, b"post-fork"))
        .expect("post-fork")
        .is_none());
}

#[test]
fn rewrite_output_publish_uncertain_reports_health_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xde);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "uncertain-debt");
    backend.uncertain_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "uncertain-debt"))
        .expect_err("uncertain publish");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication"
    );
    assert_eq!(runtime.branch_state().owned_table_count(), 2);
}

#[test]
fn rewrite_output_reopen_failure_leaves_reads_unchanged() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xdf);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "reopen-unchanged");
    let before = latest_value_from_state(runtime.branch_state(), branch, b"right");
    backend.corrupt_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "reopen-unchanged"))
        .expect_err("reopen failure");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
    assert_eq!(
        latest_value_from_state(runtime.branch_state(), branch, b"right"),
        before
    );
}

#[test]
fn rewrite_output_fact_mismatch_leaves_reads_unchanged() {
    let branch = branch_id(0xe0);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "fact-mismatch");
    runtime
        .compact_branch_tables(&compaction_request(branch, "fact-mismatch"))
        .expect("first publish");
    let object = backend.table_object_names().pop().expect("object");
    backend
        .write_object(&object, b"not the expected table bytes")
        .expect("overwrite table object");
    *runtime.branch_state_mut() = BranchLocalState::empty(branch);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "fact-mismatch");
    let before = latest_value_from_state(runtime.branch_state(), branch, b"right");

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "fact-mismatch"))
        .expect_err("mismatched existing bytes");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.rewrite_publication"
    );
    assert_eq!(
        latest_value_from_state(runtime.branch_state(), branch, b"right"),
        before
    );
}

#[test]
fn rewrite_install_failure_after_publish_names_orphan_outputs() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe1);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "install-collision");
    let output_identity =
        predicted_compaction_output_identity(&runtime, branch, "install-collision");
    install_owned_table(
        runtime.branch_state_mut(),
        branch,
        BranchLevel::new(1),
        output_identity.as_str(),
        vec![put_row(branch, b"collision", 99, 99_000, b"colliding")],
    );

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "install-collision"))
        .expect_err("install failure after publish");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
    match error {
        LifecycleError::RewritePublicationOrphaned { objects, .. } => {
            assert_eq!(objects.len(), 1);
            assert!(objects[0].contains(output_identity.as_str()));
        }
        other => panic!("unexpected error shape: {other:?}"),
    }
    assert_eq!(backend.table_object_names().len(), 1);
}

#[test]
fn rewrite_install_failure_after_publish_does_not_delete_outputs() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe2);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(
        runtime.branch_state_mut(),
        branch,
        "install-collision-no-delete",
    );
    let output_identity =
        predicted_compaction_output_identity(&runtime, branch, "install-collision-no-delete");
    install_owned_table(
        runtime.branch_state_mut(),
        branch,
        BranchLevel::new(1),
        output_identity.as_str(),
        vec![put_row(branch, b"collision", 99, 99_000, b"colliding")],
    );

    let _ = runtime
        .compact_branch_tables(&compaction_request(branch, "install-collision-no-delete"))
        .expect_err("install failure after publish");

    assert_eq!(backend.table_object_names().len(), 1);
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn rewrite_manifest_publish_failure_after_install_keeps_new_reads_visible() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe3);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(
        runtime.branch_state_mut(),
        branch,
        "manifest-failure-visible",
    );
    backend.fail_table_manifest_replacement_on_call(1);

    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, "manifest-failure-visible"))
        .expect("manifest debt");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedManifestDebt
    );
    assert_eq!(
        latest_value_from_state(runtime.branch_state(), branch, b"right"),
        Some(b"new".to_vec())
    );
}

#[test]
fn rewrite_manifest_publish_failure_after_install_reports_manifest_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe4);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "manifest-failure-debt");
    backend.fail_table_manifest_replacement_on_call(1);

    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, "manifest-failure-debt"))
        .expect("manifest debt");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedManifestDebt
    );
    assert!(outcome.recovery_health().is_some());
    assert!(outcome.checkpoint_required());
}

#[test]
fn durable_fixed_point_drain_stops_after_manifest_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xeb);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "drain-manifest-debt");
    backend.fail_table_manifest_replacement_on_call(1);
    let request =
        LifecycleCompactionDrainRequest::new(branch, "drain-manifest-debt").expect("request");

    let outcome = runtime
        .compact_branch_tables_to_fixed_point(&request)
        .expect("drain outcome");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(outcome.operations_attempted(), 1);
    assert_eq!(outcome.operations_installed(), 1);
    assert_eq!(outcome.table_rewrites(), 1);
    assert_eq!(outcome.metadata_promotions(), 0);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert!(maintenance.source_error().is_some());
    assert!(maintenance.checkpoint_required());
    assert_eq!(backend.table_manifest_replace_calls(), 1);
    assert!(runtime.branch_state().owned_levels()[0].is_empty());
    assert_eq!(runtime.branch_state().owned_levels()[1].len(), 1);
    assert!(runtime.branch_state().owned_levels()[2].is_empty());
}

#[test]
fn rewrite_manifest_publish_uncertain_after_install_reports_uncertainty() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe5);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(
        runtime.branch_state_mut(),
        branch,
        "manifest-uncertain-debt",
    );
    backend.uncertain_table_manifest_replacement_on_call(1);

    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, "manifest-uncertain-debt"))
        .expect("manifest uncertainty");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedManifestDebt
    );
    assert_eq!(
        outcome.failure().expect("failure").code(),
        "ambiguous_commit.lifecycle.table_manifest_publication"
    );
}

#[test]
fn rewrite_retry_after_manifest_failure_reuses_catalog_entries() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xe7);
    let mut runtime = materialization_runtime(child, branch_id(0xe6), backend);
    backend.fail_table_manifest_replacement_on_call(1);
    let first = runtime
        .materialize_inherited_layer(&materialization_request(child, "manifest-retry"))
        .expect("first");
    let handle = first
        .materialization_handle()
        .expect("materialization handle");
    let first_objects = backend.table_object_names();

    let retry = runtime
        .materialize_inherited_layer(
            &LifecycleMaterializationRequest::from_handle(handle, "manifest-retry").expect("retry"),
        )
        .expect("retry");

    assert_eq!(
        retry.status(),
        LifecycleMaterializationStatus::AlreadyMaterialized
    );
    assert_eq!(backend.table_object_names(), first_objects);
}

#[test]
fn rewrite_retry_after_output_publish_collision_rejects_conflict() {
    let branch = branch_id(0xe8);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "collision");
    runtime
        .compact_branch_tables(&compaction_request(branch, "collision"))
        .expect("first publish");
    let object = backend.table_object_names().pop().expect("object");
    backend
        .write_object(&object, b"conflicting table bytes")
        .expect("overwrite table object");
    *runtime.branch_state_mut() = BranchLocalState::empty(branch);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "collision");

    let error = runtime
        .compact_branch_tables(&compaction_request(branch, "collision"))
        .expect_err("conflicting object");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.rewrite_publication"
    );
}

#[test]
fn rewrite_stale_candidate_after_publish_fails_without_resurrection() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xe9);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "stale-resurrection");

    let _ = publish_output_then_stale_install(&mut runtime, branch, "stale-resurrection")
        .expect_err("stale install");

    assert_eq!(runtime.branch_state().owned_table_count(), 3);
    assert_eq!(
        latest_value_from_state(runtime.branch_state(), branch, b"right"),
        Some(b"new".to_vec())
    );
}

#[test]
fn recovery_after_manifest_publish_failure_uses_previous_manifest_or_wal() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xea);
    {
        let mut runtime = open_runtime(branch, backend);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "first-manifest");
        runtime
            .compact_branch_tables(&compaction_request(branch, "first-manifest"))
            .expect("first durable rewrite");
        *runtime.branch_state_mut() = BranchLocalState::empty(branch);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "failed-manifest");
        backend.fail_table_manifest_replacement_on_call(2);
        runtime
            .compact_branch_tables(&compaction_request(branch, "failed-manifest"))
            .expect("manifest debt");
    }

    let reopened = open_runtime(branch, backend);

    assert_eq!(reopened.branch_state().owned_table_count(), 1);
    assert!(reopened
        .table_catalog()
        .build_manifest(reopened.branch_state())
        .expect("manifest")
        .levels()[0]
        .tables()[0]
        .table_identity()
        .as_str()
        .contains("first-manifest"));
}

#[test]
fn recovery_after_install_before_manifest_records_health_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xeb);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(
        runtime.branch_state_mut(),
        branch,
        "install-before-manifest",
    );
    backend.fail_table_manifest_replacement_on_call(1);

    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, "install-before-manifest"))
        .expect("manifest debt");

    assert!(outcome.recovery_health().is_some());
    assert!(outcome.maintenance_outcome().source_error().is_some());
}

#[test]
fn recovery_rejects_corrupt_rewrite_output_listed_by_manifest() {
    let branch = branch_id(0xec);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    {
        let mut runtime = open_runtime(branch, backend);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "recover-corrupt");
        runtime
            .compact_branch_tables(&compaction_request(branch, "recover-corrupt"))
            .expect("durable rewrite");
    }
    let object = backend.table_object_names().pop().expect("object");
    backend
        .write_object(&object, b"corrupt table bytes")
        .expect("corrupt table object");
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("corrupt manifest-listed object");

    assert_eq!(error.code(), "corruption.lifecycle.table_manifest");
}

#[test]
fn recovery_rejects_missing_rewrite_output_listed_by_manifest() {
    let branch = branch_id(0xed);
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    {
        let mut runtime = open_runtime(branch, backend);
        install_compaction_inputs(runtime.branch_state_mut(), branch, "recover-missing");
        runtime
            .compact_branch_tables(&compaction_request(branch, "recover-missing"))
            .expect("durable rewrite");
    }
    let object = backend.table_object_names().pop().expect("object");
    backend.delete_object(&object).expect("delete table object");
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");

    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("missing manifest-listed object");

    assert_eq!(error.code(), "corruption.lifecycle.table_manifest");
}

#[test]
fn recovery_preserves_reads_after_wal_tail_replay() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xee);
    {
        let mut runtime = open_runtime(branch, backend);
        runtime
            .execute_durable_commit(
                durable_batch(branch, b"checkpoint-key", b"checkpoint-value"),
                generation_guard(),
            )
            .expect("checkpoint commit");
        runtime
            .checkpoint(
                &LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(10_000))
                    .expect("checkpoint request"),
            )
            .expect("checkpoint");
        runtime
            .execute_durable_commit(
                durable_batch(branch, b"tail-key", b"tail-value"),
                generation_guard(),
            )
            .expect("commit");
    }

    let reopened = open_runtime(branch, backend);

    let view = reopened.read_view().expect("read view");
    let visible = view
        .latest(&checkpoint_physical_key(branch, b"tail-key"))
        .expect("latest")
        .expect("tail row");
    assert_eq!(visible.row().value(), b"tail-value");
}

#[test]
fn durable_rewrite_completion_does_not_directly_persist_flush_watermark() {
    let branch = branch_id(0xef);
    let (backend, _) = successful_compaction(branch, "no-watermark");
    let runtime = open_runtime(branch, backend);
    let manifest = runtime
        .services()
        .manifest()
        .load_current()
        .expect("database manifest")
        .expect("manifest");

    assert_eq!(manifest.flushed_through_commit_id(), None);
}

#[test]
fn durable_rewrite_completion_does_not_directly_truncate_wal() {
    let branch = branch_id(0xf0);
    let (backend, _) = successful_compaction(branch, "no-log-delete");

    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn durable_rewrite_manifest_facts_can_build_flush_coverage_candidate() {
    let branch = branch_id(0xf1);
    let (backend, _) = successful_compaction(branch, "coverage-facts");
    let runtime = open_runtime(branch, backend);
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");

    assert_eq!(
        manifest.levels()[0].tables()[0].facts().commit_max(),
        CommitVersion::new(2)
    );
}

#[test]
fn durable_rewrite_manifest_failure_cannot_build_flush_coverage_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xf2);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "no-coverage");
    backend.fail_table_manifest_replacement_on_call(1);

    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, "no-coverage"))
        .expect("manifest debt");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedManifestDebt
    );
    assert!(runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .is_none());
}

#[test]
fn durable_rewrite_checkpoint_debt_reduced_only_after_manifest_success() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xf3);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "debt-first");
    backend.fail_table_manifest_replacement_on_call(1);
    let failed = runtime
        .compact_branch_tables(&compaction_request(branch, "debt-first"))
        .expect("manifest debt");
    *runtime.branch_state_mut() = BranchLocalState::empty(branch);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "debt-second");
    let completed = runtime
        .compact_branch_tables(&compaction_request(branch, "debt-second"))
        .expect("manifest success");

    assert!(failed.checkpoint_required());
    assert!(!completed.checkpoint_required());
    assert_eq!(
        completed.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
}

#[test]
fn durable_rewrite_does_not_delete_replaced_inputs() {
    let branch = branch_id(0xf4);
    let (backend, _) = successful_compaction(branch, "no-delete-inputs");

    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn durable_rewrite_does_not_quarantine_replaced_inputs() {
    let branch = branch_id(0xf5);
    let (backend, outcome) = successful_compaction(branch, "no-quarantine-inputs");

    assert_eq!(backend.delete_calls(), 0);
    assert!(outcome.recovery_health().is_none());
}

#[test]
fn durable_rewrite_does_not_delete_published_orphan_outputs() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xf6);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "no-delete-orphan");
    backend.corrupt_table_object_create_on_call(1);
    let _ = runtime
        .compact_branch_tables(&compaction_request(branch, "no-delete-orphan"))
        .expect_err("orphan output");

    assert_eq!(backend.table_object_names().len(), 1);
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn durable_rewrite_does_not_prune_old_versions() {
    let branch = branch_id(0xf7);
    let (_, after) = compact_read_parity(branch);
    let key = physical_key(branch, b"history");

    assert!(history_versions(
        &after
            .history(&key, BranchHistoryOptions::all())
            .expect("history")
    )
    .contains(&1));
}

#[test]
fn durable_rewrite_does_not_prune_tombstones() {
    durable_compaction_preserves_tombstones();
}

#[test]
fn durable_rewrite_does_not_prune_ttl_expired_rows() {
    durable_compaction_preserves_ttl_expired_rows_under_keep_all();
}

#[test]
fn durable_rewrite_does_not_call_quarantine_service() {
    let branch = branch_id(0xf8);
    let (_, outcome) = successful_compaction(branch, "no-quarantine-service");

    assert!(outcome.recovery_health().is_none());
}

#[test]
fn durable_rewrite_does_not_call_purge() {
    let branch = branch_id(0xf9);
    let (backend, _) = successful_compaction(branch, "no-purge");

    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn queued_durable_compaction_publishes_manifest_through_maintenance_runner() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xfb);
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, "queued-compaction");
    let queued = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue compaction");

    let outcome = runtime
        .run_next_compaction_maintenance()
        .expect("run compaction")
        .expect("compaction outcome");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");

    assert_eq!(outcome.task_id(), Some(queued.task_id()));
    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Compaction);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(!outcome.checkpoint_required());
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert_eq!(backend.table_object_create_calls(), 1);
    assert_eq!(backend.table_manifest_replace_calls(), 1);
}

#[test]
fn queued_durable_compaction_does_not_resubmit_after_manifest_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xfe);
    let mut runtime = open_runtime(branch, backend);
    for index in 0..5 {
        install_owned_table(
            runtime.branch_state_mut(),
            branch,
            BranchLevel::new(1),
            &format!("queued-debt-chain-input-{index}"),
            vec![put_row(
                branch,
                format!("queued-debt-chain-{index}").as_bytes(),
                index + 1,
                (index + 1) * 1_000,
                b"value",
            )],
        );
    }
    backend.fail_table_manifest_replacement_on_call(1);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 1))
        .expect("enqueue compaction");

    let outcome = runtime
        .run_next_compaction_maintenance()
        .expect("run compaction")
        .expect("compaction outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Compaction);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(outcome.source_error().is_some());
    assert!(outcome.recovery_health().is_some());
    assert_eq!(runtime.branch_state().owned_levels()[1].len(), 4);
    assert_eq!(runtime.branch_state().owned_levels()[2].len(), 1);
    // No compaction/materialization is resubmitted after manifest debt. The completed compaction
    // does enqueue the table-object GC mark (Retention) — the designed reclaim trigger, not a
    // rewrite resubmission.
    assert!(runtime.maintenance_status().pending_tasks() <= 1);
    assert!(!runtime
        .pending_maintenance_kinds_for_test()
        .iter()
        .any(|kind| matches!(
            kind,
            MaintenanceTaskKind::Compaction | MaintenanceTaskKind::Materialization
        )));
}

#[test]
fn queued_durable_materialization_publishes_manifest_through_maintenance_runner() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let child = branch_id(0xfd);
    let parent = branch_id(0xfc);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "queued-materialization-parent",
        vec![put_row(parent, b"inherited", 3, 3_000, b"parent")],
    );
    let (child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let mut runtime = open_runtime(child, backend);
    *runtime.branch_state_mut() = child_state;
    let queued = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::materialization_layer(child, 0))
        .expect("enqueue materialization");

    let outcome = runtime
        .run_next_materialization_maintenance()
        .expect("run materialization")
        .expect("materialization outcome");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load manifest")
        .expect("manifest");

    assert_eq!(outcome.task_id(), Some(queued.task_id()));
    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Materialization);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(!outcome.checkpoint_required());
    assert_eq!(runtime.branch_state().inherited_layer_count(), 0);
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert!(manifest.inherited_layers().is_empty());
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert_eq!(backend.table_object_create_calls(), 1);
    assert_eq!(backend.table_manifest_replace_calls(), 1);
}

#[test]
fn queued_durable_compaction_of_lone_cow_layer_publishes_without_resubmitting_materialization() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    // fork-cow.3: a branch carrying a single COW inherited layer is healthy — its durable compaction
    // publishes normally and does not resubmit materialization (the lone layer stays lazy).
    let parent = branch_id(0xfa);
    let child = branch_id(0xff);
    let mut runtime = open_runtime(parent, backend);
    install_compaction_inputs(runtime.branch_state_mut(), parent, "durable-chain-parent");
    runtime
        .compact_branch_tables(&compaction_request(parent, "durable-chain-parent"))
        .expect("publish durable parent");
    runtime
        .fork_current(
            parent,
            child,
            CommitBranchGeneration::new(2).expect("child generation"),
            None,
        )
        .expect("fork child");
    let child_generation = runtime
        .branch_catalog()
        .registry()
        .lookup(child)
        .expect("child descriptor")
        .generation();
    let child_state = runtime
        .branch_catalog_mut_for_test()
        .branch_state_mut(
            child,
            crate::commit::CommitBranchGenerationGuard::exact(child_generation),
        )
        .expect("child state");
    for index in 0..4 {
        install_l0_table(
            child_state,
            child,
            &format!("durable-chain-l0-{index}"),
            vec![put_row(
                child,
                format!("durable-chain-{index}").as_bytes(),
                index + 10,
                (index + 10) * 1_000,
                b"value",
            )],
        );
    }
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(child, 0))
        .expect("enqueue compaction");

    let compaction = runtime
        .run_next_compaction_maintenance()
        .expect("run compaction")
        .expect("compaction outcome");
    assert_eq!(compaction.task_kind(), MaintenanceTaskKind::Compaction);
    assert_eq!(compaction.status(), MaintenanceOutcomeStatus::Completed);
    assert!(
        compaction.source_error().is_none(),
        "unexpected compaction source error: {:?}",
        compaction.source_error()
    );
    assert!(compaction.recovery_health().is_none());
    assert_eq!(
        runtime
            .branch_catalog()
            .branch_state(child)
            .expect("child state")
            .inherited_layer_count(),
        1
    );
    // A lone COW inherited layer is healthy: no materialization is resubmitted after the
    // compaction. The completed compaction does enqueue the table-object GC mark (Retention) —
    // the designed reclaim trigger, not a rewrite resubmission.
    assert!(!runtime
        .pending_maintenance_kinds_for_test()
        .iter()
        .any(|kind| matches!(
            kind,
            MaintenanceTaskKind::Compaction | MaintenanceTaskKind::Materialization
        )));
    assert!(
        runtime
            .run_next_materialization_maintenance()
            .expect("poll materialization")
            .is_none(),
        "a lone COW inherited layer must not schedule materialization"
    );
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load manifest")
        .expect("manifest");
    assert_eq!(manifest.inherited_layers().len(), 1);
}

struct TestMaterializationRunner<'a> {
    branch: &'a mut BranchLocalState,
}

impl MaintenanceTaskRunner for TestMaterializationRunner<'_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let request = materialization_request_from_maintenance_task(task)?;
        Ok(materialize_cache_branch(self.branch, &request)?.maintenance_outcome())
    }
}

fn successful_compaction(
    branch: strata_core::BranchId,
    seed: &str,
) -> (&'static CheckpointTestBackend, LifecycleCompactionOutcome) {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    install_compaction_inputs(runtime.branch_state_mut(), branch, seed);
    let outcome = runtime
        .compact_branch_tables(&compaction_request(branch, seed))
        .expect("durable compaction");
    drop(runtime);
    (backend, outcome)
}

fn materialization_runtime(
    child: strata_core::BranchId,
    parent: strata_core::BranchId,
    backend: &'static CheckpointTestBackend,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "materialization-parent",
        vec![put_row(parent, b"inherited", 3, 3_000, b"parent")],
    );
    let (child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let mut runtime = open_runtime(child, backend);
    *runtime.branch_state_mut() = child_state;
    runtime
}

fn compaction_request(branch: strata_core::BranchId, seed: &str) -> LifecycleCompactionRequest {
    LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, seed)
        .expect("compaction request")
}

fn materialization_request(
    child: strata_core::BranchId,
    seed: &str,
) -> LifecycleMaterializationRequest {
    LifecycleMaterializationRequest::new(child, 0, seed).expect("materialization request")
}

fn install_compaction_inputs(
    state: &mut BranchLocalState,
    branch: strata_core::BranchId,
    seed: &str,
) {
    install_l0_table(
        state,
        branch,
        &format!("{seed}-left"),
        vec![put_row(branch, b"left", 1, 1_000, b"old")],
    );
    install_l0_table(
        state,
        branch,
        &format!("{seed}-right"),
        vec![put_row(branch, b"right", 2, 2_000, b"new")],
    );
}

fn install_many_inputs(
    state: &mut BranchLocalState,
    branch: strata_core::BranchId,
    seed: &str,
    count: usize,
) {
    for index in 0..count {
        install_l0_table(
            state,
            branch,
            &format!("{seed}-{index}"),
            vec![put_row(
                branch,
                format!("key-{index}").as_bytes(),
                u64::try_from(index + 1).expect("version"),
                u64::try_from(index + 1).expect("timestamp") * 1_000,
                b"value",
            )],
        );
    }
}

fn compact_read_parity(
    branch: strata_core::BranchId,
) -> (
    crate::branch::read::BranchReadView,
    crate::branch::read::BranchReadView,
) {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(branch, backend);
    *runtime.branch_state_mut() = read_shape_state(branch);
    let before = runtime.branch_state().capture_read_view().expect("before");
    runtime
        .compact_branch_tables(&compaction_request(branch, "read-parity-exact"))
        .expect("durable compaction");
    let after = runtime.branch_state().capture_read_view().expect("after");
    (before, after)
}

fn materialization_read_parity(
    parent: strata_core::BranchId,
    child: strata_core::BranchId,
) -> (
    crate::branch::read::BranchReadView,
    crate::branch::read::BranchReadView,
) {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut runtime = open_runtime(child, backend);
    *runtime.branch_state_mut() = materialization_read_state(parent, child);
    let before = runtime.branch_state().capture_read_view().expect("before");
    runtime
        .materialize_inherited_layer(&materialization_request(child, "material-read-exact"))
        .expect("materialization");
    let after = runtime.branch_state().capture_read_view().expect("after");
    (before, after)
}

fn latest_value(
    view: &crate::branch::read::BranchReadView,
    branch: strata_core::BranchId,
    key: &[u8],
) -> Option<Vec<u8>> {
    view.latest(&physical_key(branch, key))
        .expect("latest")
        .map(|row| row.row().value().to_vec())
}

fn latest_value_from_state(
    state: &BranchLocalState,
    branch: strata_core::BranchId,
    key: &[u8],
) -> Option<Vec<u8>> {
    let view = state.capture_read_view().expect("view");
    latest_value(&view, branch, key)
}

fn predicted_compaction_output_identity(
    runtime: &LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: strata_core::BranchId,
    seed: &str,
) -> crate::table::TableIdentity {
    let branch_request =
        BranchCompactionRequest::new(branch, BranchCompactionKind::CompactL0, seed)
            .expect("branch request");
    let plan = runtime
        .branch_state()
        .plan_branch_compaction(&branch_request)
        .expect("plan");
    let (artifacts, _) = runtime
        .branch_state()
        .prepare_branch_compaction_plan(&branch_request, &plan)
        .expect("prepare")
        .expect("prepared output");
    artifacts[0].facts().identity().clone()
}

fn publish_output_then_stale_install(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: strata_core::BranchId,
    seed: &str,
) -> crate::branch::error::BranchRuntimeResult<
    crate::branch::state::compaction::BranchCompactionOutcome,
> {
    let branch_request =
        BranchCompactionRequest::new(branch, BranchCompactionKind::CompactL0, seed)
            .expect("branch request");
    let plan = runtime
        .branch_state()
        .plan_branch_compaction(&branch_request)
        .expect("plan");
    let (artifacts, _) = runtime
        .branch_state()
        .prepare_branch_compaction_plan(&branch_request, &plan)
        .expect("prepare")
        .expect("prepared output");
    let artifact = artifacts[0].clone();
    runtime
        .services()
        .table_object()
        .publish_create(
            &branch.to_string(),
            u32::from(BranchLevel::ZERO.raw()),
            artifact.facts().identity().as_str(),
            artifact.bytes(),
        )
        .expect("publish output");
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        &format!("{seed}-interfering"),
        vec![put_row(branch, b"interfering", 99, 99_000, b"interfering")],
    );
    runtime
        .branch_state_mut()
        .install_branch_compaction_plan(&branch_request, &plan)
}
