use super::shared::*;
use super::*;
use crate::branch::error::BranchRuntimeError;
use crate::branch::facts::{BranchTableReferenceKind, InheritedLayerStatus};
use crate::branch::read::{BranchHistoryOptions, BranchReadBound, BranchScanBounds};
use crate::branch::state::compaction::{BranchCompactionKind, BranchCompactionRequest};
use crate::branch::state::materialization::{
    BranchMaterializationHandle, BranchMaterializationRecovery,
};
use crate::branch::state::BranchLocalState;
use crate::lifecycle::compaction::{
    current_compaction_request_from_maintenance_task, defer_compaction_for_resource_policy,
    stale_compaction_maintenance_outcome, table_rewrite_outcome_was_flush_preempted,
    FROZEN_BLOCKING_FLUSH_THRESHOLD,
};
use crate::lifecycle::tests::checkpoint::shared::{
    open_runtime, CheckpointBackendEvent, CheckpointTestBackend,
};
use crate::table::{
    sort_table_rows_by_key, ImmutableTableBuilder, TableBuilderConfig, TableIdentity, TableRow,
};
use strata_core::{CommitVersion, Timestamp};

#[test]
fn compaction_flush_preemption_gate_yields_only_at_the_blocking_threshold() {
    let branch = branch_id(0x72);
    let request =
        LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "a3-gate")
            .expect("compaction request");

    let with_frozen = |count: usize| -> BranchLocalState {
        let mut state = BranchLocalState::empty(branch);
        for i in 0..count {
            let n = u64::try_from(i).expect("index fits");
            state
                .append_committed_row(put_row(
                    branch,
                    format!("frozen-{i}").as_bytes(),
                    n + 1,
                    (n + 1) * 1_000,
                    b"value",
                ))
                .expect("append");
            state.rotate_active();
        }
        assert_eq!(state.frozen_table_count(), count);
        state
    };

    // Below the blocking threshold: compaction is NOT flush-preempted. With the
    // Unlimited io policy the gate has no other reason to defer, so it returns None.
    for frozen in 1..FROZEN_BLOCKING_FLUSH_THRESHOLD {
        let state = with_frozen(frozen);
        assert!(
            defer_compaction_for_resource_policy(
                &state,
                &request,
                crate::lifecycle::config::LifecycleCompactionIoPolicy::Unlimited,
            )
            .expect("defer")
            .is_none(),
            "frozen={frozen} (< {FROZEN_BLOCKING_FLUSH_THRESHOLD}) must not flush-preempt compaction"
        );
    }

    // At the blocking threshold: flush is blocking admission, so compaction yields.
    let state = with_frozen(FROZEN_BLOCKING_FLUSH_THRESHOLD);
    let outcome = defer_compaction_for_resource_policy(
        &state,
        &request,
        crate::lifecycle::config::LifecycleCompactionIoPolicy::Unlimited,
    )
    .expect("defer")
    .expect("compaction is flush-preempted at the blocking threshold");
    assert!(table_rewrite_outcome_was_flush_preempted(&outcome));
}

#[test]
fn table_rewrite_requests_reject_bad_components_and_wrong_branch_execution() {
    let branch = branch_id(0x70);
    let other = branch_id(0x71);
    assert!(LifecycleMaterializationRequest::new(branch, 0, "").is_err());

    let mut state = BranchLocalState::empty(branch);
    let compaction =
        LifecycleCompactionRequest::new(other, BranchCompactionKind::CompactL0, "wrong-branch")
            .expect("compaction request");
    let materialization = LifecycleMaterializationRequest::new(other, 0, "wrong-materialization")
        .expect("materialization request");

    let compaction_error =
        compact_cache_branch(&mut state, &compaction).expect_err("wrong branch compaction");
    let materialization_error = materialize_cache_branch(&mut state, &materialization)
        .expect_err("wrong branch materialization");

    assert_eq!(
        compaction_error.code(),
        "failed_precondition.lifecycle.branch_runtime"
    );
    assert_eq!(
        materialization_error.code(),
        "failed_precondition.lifecycle.branch_runtime"
    );
    assert!(state.is_empty());
}

#[test]
fn compaction_chain_tasks_coalesce_per_branch_and_level() {
    let branch = branch_id(0x72);
    let other = branch_id(0x73);
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");

    let first = executor
        .enqueue(open_state(), MaintenanceTaskRequest::compaction(branch, 0))
        .expect("first compaction");
    let duplicate = executor
        .enqueue(open_state(), MaintenanceTaskRequest::compaction(branch, 0))
        .expect("duplicate compaction");
    let other_level = executor
        .enqueue(open_state(), MaintenanceTaskRequest::compaction(branch, 1))
        .expect("other level");
    let other_branch = executor
        .enqueue(open_state(), MaintenanceTaskRequest::compaction(other, 0))
        .expect("other branch");
    let materialization = executor
        .enqueue(
            open_state(),
            MaintenanceTaskRequest::materialization_layer(branch, 0),
        )
        .expect("materialization");
    let duplicate_materialization = executor
        .enqueue(
            open_state(),
            MaintenanceTaskRequest::materialization_layer(branch, 0),
        )
        .expect("duplicate materialization");

    assert_eq!(duplicate.task_id(), first.task_id());
    assert!(duplicate.was_coalesced());
    // Per-(branch, level) coalescing: a different level on the same branch is now its own
    // pending task (previously collapsed into the branch's single compaction task), so
    // concurrent workers have distinct non-conflicting levels to pick.
    assert_ne!(other_level.task_id(), first.task_id());
    assert!(!other_level.was_coalesced());
    assert_ne!(other_branch.task_id(), first.task_id());
    assert_eq!(
        duplicate_materialization.task_id(),
        materialization.task_id()
    );
    assert_eq!(executor.status().pending_tasks(), 4);
    assert_eq!(executor.stats().coalesced(), 2);
}

#[test]
fn queued_table_rewrites_are_blocked_after_close_without_mutating_branch() {
    let branch = branch_id(0x74);
    let mut runtime = cache_runtime(branch);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue compaction");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::materialization_layer(branch, 0))
        .expect("enqueue materialization");

    let close = runtime.close().expect("close");
    let compaction_error = runtime
        .run_next_compaction_maintenance()
        .expect_err("closed compaction");
    let materialization_error = runtime
        .run_next_materialization_maintenance()
        .expect_err("closed materialization");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        compaction_error.code(),
        "failed_precondition.lifecycle.state"
    );
    assert_eq!(
        materialization_error.code(),
        "failed_precondition.lifecycle.state"
    );
    assert_eq!(runtime.state(), LifecycleState::Closed);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert!(runtime.branch_state().is_empty());
}

#[test]
fn failed_table_rewrite_attempt_does_not_close_runtime() {
    let branch = branch_id(0x75);
    let other = branch_id(0x76);
    let mut runtime = cache_runtime(branch);
    let request =
        LifecycleCompactionRequest::new(other, BranchCompactionKind::CompactL0, "runtime-error")
            .expect("request");

    let error = runtime
        .compact_branch_tables(&request)
        .expect_err("wrong branch");

    assert_eq!(error.code(), "not_found.lifecycle.branch");
    assert_eq!(runtime.state(), LifecycleState::Open);
    assert!(runtime.branch_state().is_empty());
}

#[test]
fn compaction_preserves_branch_timestamp_and_value_facts() {
    let branch = branch_id(0x77);
    let mut state = BranchLocalState::empty(branch);
    install_l0_table(
        &mut state,
        branch,
        "fact-left",
        vec![put_row(branch, b"fact", 1, 1_000, b"old")],
    );
    install_l0_table(
        &mut state,
        branch,
        "fact-right",
        vec![put_row(branch, b"fact", 2, 2_000, b"new")],
    );
    let key = physical_key(branch, b"fact");

    let outcome = compact_cache_branch(
        &mut state,
        &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "fact-rewrite")
            .expect("request"),
    )
    .expect("outcome");
    let visible = state
        .capture_read_view()
        .expect("view")
        .latest(&key)
        .expect("read")
        .expect("row")
        .row()
        .clone();

    assert_eq!(outcome.status(), LifecycleCompactionStatus::Completed);
    assert_eq!(visible.physical_key().branch_id(), branch);
    assert_eq!(visible.commit_timestamp(), Timestamp::from_micros(2_000));
    assert_eq!(visible.value(), b"new");
}

#[test]
fn materialization_handle_binds_source_identity_and_reports_snapshot_facts() {
    let parent = branch_id(0x78);
    let child = branch_id(0x79);
    let wrong_source = branch_id(0x7a);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "handle-parent",
        vec![put_row(parent, b"key", 3, 3_000, b"value")],
    );
    let (mut child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let (handle, _) = child_state
        .mark_inherited_layer_materializing(0)
        .expect("materialization handle");
    let mismatched = BranchMaterializationHandle::new(
        child,
        wrong_source,
        handle.fork_version(),
        handle.layer_index(),
    );
    let request = LifecycleMaterializationRequest::from_handle(handle, "handle-materialization")
        .expect("request");
    let bad_request =
        LifecycleMaterializationRequest::from_handle(mismatched, "bad-handle").expect("bad");

    let error = materialize_cache_branch(&mut child_state.clone(), &bad_request)
        .expect_err("mismatched source");
    let outcome = materialize_cache_branch(&mut child_state, &request).expect("materialized");

    assert_eq!(error.code(), "failed_precondition.lifecycle.branch_runtime");
    let branch_outcome = outcome.branch_outcome().expect("branch outcome");
    assert_eq!(
        outcome
            .materialization_handle()
            .expect("materialization handle"),
        handle
    );
    assert_eq!(branch_outcome.source_branch_id(), parent);
    assert_eq!(branch_outcome.fork_version(), handle.fork_version());
    assert_eq!(
        branch_outcome.recovery(),
        BranchMaterializationRecovery::ReplacementVisibleLayerRemoved
    );
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.affected_objects(), 1);
    assert_eq!(maintenance.affected_object_names().len(), 1);
    assert!(maintenance.affected_object_names()[0].contains("handle-materialization"));
}

#[test]
fn materialization_handle_marks_active_layer_before_materializing() {
    let parent = branch_id(0x8a);
    let child = branch_id(0x8b);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "handle-active-parent",
        vec![put_row(parent, b"key", 3, 3_000, b"value")],
    );
    let (mut child_state, fork) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let handle = BranchMaterializationHandle::new(child, parent, fork.fork_version(), 0);
    let request =
        LifecycleMaterializationRequest::from_handle(handle, "handle-active").expect("request");

    let outcome = materialize_cache_branch(&mut child_state, &request).expect("materialized");

    assert!(outcome
        .reachability_snapshot()
        .expect("reachability snapshot")
        .table_refs()
        .iter()
        .all(|table_ref| matches!(
            table_ref.reference_kind(),
            BranchTableReferenceKind::MaterializingSource { .. }
        )));
    assert_eq!(child_state.inherited_layer_count(), 0);
}

#[test]
fn queued_materialization_uses_bound_source_after_layer_reindex() {
    let far = branch_id(0x8c);
    let near = branch_id(0x8d);
    let child = branch_id(0x8e);
    let mut far_state = BranchLocalState::empty(far);
    install_l0_table(
        &mut far_state,
        far,
        "queued-far-source",
        vec![put_row(far, b"far", 1, 1_000, b"far")],
    );
    let (mut near_state, _) = far_state.fork_into_empty_child(near).expect("fork near");
    install_l0_table(
        &mut near_state,
        near,
        "queued-near-source",
        vec![put_row(near, b"near", 2, 2_000, b"near")],
    );
    let (mut child_state, _) = near_state.fork_into_empty_child(child).expect("fork child");
    assert_eq!(child_state.inherited_layer_count(), 2);

    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    let enqueued = executor
        .enqueue_with_binding(
            open_state(),
            MaintenanceTaskRequest::materialization_layer(child, 1),
            |request| bind_materialization_task_for_enqueue(&mut child_state, request),
        )
        .expect("enqueue materialization");

    let near_outcome = materialize_cache_branch(
        &mut child_state,
        &LifecycleMaterializationRequest::new(child, 0, "queued-near").expect("near request"),
    )
    .expect("near materialization");
    assert_eq!(
        near_outcome
            .branch_outcome()
            .expect("near branch outcome")
            .source_branch_id(),
        near
    );
    assert_eq!(child_state.inherited_layer_count(), 1);

    let mut runner = TestMaterializationRunner {
        branch: &mut child_state,
    };
    let maintenance = executor
        .run_next(open_state(), &mut runner)
        .expect("run queued")
        .expect("queued outcome");

    assert_eq!(maintenance.task_id(), Some(enqueued.task_id()));
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(child_state.inherited_layer_count(), 0);
    let view = child_state.capture_read_view().expect("view");
    assert_eq!(
        view.latest(&physical_key(child, b"far"))
            .expect("far read")
            .expect("far visible")
            .row()
            .value(),
        b"far"
    );
    assert_eq!(
        view.latest(&physical_key(child, b"near"))
            .expect("near read")
            .expect("near visible")
            .row()
            .value(),
        b"near"
    );
}

#[test]
fn materialization_enqueue_capacity_failure_does_not_mark_layer_materializing() {
    let parent = branch_id(0x8f);
    let child = branch_id(0x90);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "capacity-parent",
        vec![put_row(parent, b"key", 1, 1_000, b"value")],
    );
    let (mut child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let mut executor = LifecycleMaintenanceExecutor::new(1).expect("executor");
    executor
        .enqueue(open_state(), MaintenanceTaskRequest::flush(child))
        .expect("fill queue");

    let error = executor
        .enqueue_with_binding(
            open_state(),
            MaintenanceTaskRequest::materialization_layer(child, 0),
            |request| bind_materialization_task_for_enqueue(&mut child_state, request),
        )
        .expect_err("queue is full");

    assert_eq!(
        error.code(),
        "resource_exhausted.lifecycle.maintenance_queue"
    );
    assert_eq!(
        child_state.inherited_layers()[0].status(),
        InheritedLayerStatus::Active
    );
}

#[test]
fn materialization_preserves_range_scan_and_child_immutable_precedence() {
    let parent = branch_id(0x7b);
    let child = branch_id(0x7c);
    let mut child_state = materialization_read_state(parent, child);
    let shared_key = physical_key(child, b"shared");
    let lower = physical_key(child, b"scan-a");
    let upper = physical_key(child, b"scan-z");
    let before = child_state.capture_read_view().expect("before");
    let before_shared = before
        .latest(&shared_key)
        .expect("before shared")
        .expect("shared")
        .row()
        .clone();
    let before_range = scan_user_keys(
        &before
            .scan_range(
                &BranchScanBounds::closed(&lower, &upper).expect("range"),
                BranchReadBound::latest(),
            )
            .expect("range rows"),
    );

    let outcome = materialize_read_state(child, &mut child_state);
    assert_eq!(outcome.status(), LifecycleMaterializationStatus::Completed);
    let after = child_state.capture_read_view().expect("after");

    assert_eq!(
        after
            .latest(&shared_key)
            .expect("after shared")
            .expect("shared")
            .row(),
        &before_shared
    );
    assert_eq!(
        scan_user_keys(
            &after
                .scan_range(
                    &BranchScanBounds::closed(&lower, &upper).expect("range"),
                    BranchReadBound::latest(),
                )
                .expect("range rows")
        ),
        before_range
    );
}

#[test]
fn materializing_lone_inherited_layer_stays_below_gate_without_mutating_reads() {
    // fork-cow.3: a lone inherited layer is a healthy COW fork below the proactive materialization gate,
    // so it reports no InheritedLayerBacklog pressure even mid-materialization — but marking it
    // Materializing must never change what reads see.
    let parent = branch_id(0x7d);
    let child = branch_id(0x7e);
    let mut child_state = materialization_read_state(parent, child);
    let key = physical_key(child, b"scan-a");
    let before = child_state
        .capture_read_view()
        .expect("before")
        .latest(&key)
        .expect("read")
        .expect("visible")
        .row()
        .clone();
    child_state
        .mark_inherited_layer_materializing(0)
        .expect("mark materializing");

    let pressure = collect_storage_pressure(&child_state, empty_maintenance_status());
    let after = child_state
        .capture_read_view()
        .expect("after")
        .latest(&key)
        .expect("read")
        .expect("visible")
        .row()
        .clone();

    assert_eq!(pressure.reason(), LifecycleStoragePressureReason::None);
    assert_eq!(child_state.inherited_layer_count(), 1);
    assert_eq!(before, after);
}

#[test]
fn table_rewrite_outcomes_count_deferred_completed_and_affected_objects() {
    let branch = branch_id(0x7f);
    let mut state = BranchLocalState::empty(branch);
    let deferred = compact_cache_branch(
        &mut state,
        &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "empty-count")
            .expect("request"),
    )
    .expect("deferred")
    .maintenance_outcome();
    assert_eq!(deferred.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(deferred.stats().maintenance_tasks(), 1);

    install_l0_table(
        &mut state,
        branch,
        "count-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        &mut state,
        branch,
        "count-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    let completed = compact_cache_branch(
        &mut state,
        &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "count-rewrite")
            .expect("request"),
    )
    .expect("completed")
    .maintenance_outcome();

    assert_eq!(completed.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(completed.affected_objects(), 3);
    assert_eq!(completed.affected_object_names().len(), 3);
    assert!(completed
        .affected_object_names()
        .iter()
        .any(|identity| identity.contains("count-left")));
    assert_eq!(completed.bytes_reclaimed(), 0);
    assert!(!completed.retryable());
    assert!(completed.recovery_health().is_none());
}

#[test]
fn queued_compaction_recomputes_after_prior_rewrite_without_resurrection() {
    let branch = branch_id(0x84);
    let mut state = BranchLocalState::empty(branch);
    install_l0_table(
        &mut state,
        branch,
        "stale-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        &mut state,
        branch,
        "stale-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    let queued = executor
        .enqueue(open_state(), MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue");
    let direct = compact_cache_branch(
        &mut state,
        &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "stale-direct")
            .expect("direct request"),
    )
    .expect("direct compaction");
    assert_eq!(direct.status(), LifecycleCompactionStatus::Completed);
    assert_eq!(state.owned_table_count(), 1);

    let mut runner = TestCompactionRunner { branch: &mut state };
    let queued_outcome = executor
        .run_next(open_state(), &mut runner)
        .expect("run queued")
        .expect("queued outcome");

    assert_eq!(queued_outcome.task_id(), Some(queued.task_id()));
    assert_eq!(queued_outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(state.owned_levels()[0].is_empty());
    assert_eq!(state.owned_levels()[1].len(), 1);
    assert_eq!(state.owned_table_count(), 1);
    let view = state.capture_read_view().expect("view");
    assert_eq!(
        view.latest(&physical_key(branch, b"left"))
            .expect("left read")
            .expect("left visible")
            .row()
            .value(),
        b"left"
    );
    assert_eq!(
        view.latest(&physical_key(branch, b"right"))
            .expect("right read")
            .expect("right visible")
            .row()
            .value(),
        b"right"
    );
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn pressure_debug_output_uses_storage_vocabulary() {
    let branch = branch_id(0x80);
    let mut state = BranchLocalState::empty(branch);
    install_l0_table(
        &mut state,
        branch,
        "vocabulary-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        &mut state,
        branch,
        "vocabulary-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );

    let debug = format!(
        "{:?}",
        collect_storage_pressure(&state, empty_maintenance_status())
    );

    for forbidden in ["write stall", "customer", "tenant", "redis", "query"] {
        assert!(!debug.to_ascii_lowercase().contains(forbidden));
    }
}

#[test]
fn storage_pressure_blocks_mutating_admission_for_large_table_backlog() {
    let branch = branch_id(0x82);
    let mut state = BranchLocalState::empty(branch);
    for index in 0_u64..36 {
        install_l0_table(
            &mut state,
            branch,
            &format!("blocking-backlog-{index}"),
            vec![put_row(
                branch,
                format!("key-{index}").as_bytes(),
                index + 1,
                1_000 + index,
                b"value",
            )],
        );
    }

    let pressure = collect_storage_pressure(&state, empty_maintenance_status());

    assert_eq!(
        pressure.severity(),
        LifecycleStoragePressureSeverity::BlockMutatingAdmission
    );
    assert_eq!(
        pressure.reason(),
        LifecycleStoragePressureReason::LevelZeroTableBacklog
    );
    assert_eq!(pressure.level_zero_tables(), 36);
    assert!(matches!(
        pressure.suggested_task().map(MaintenanceTaskRequest::kind),
        Some(MaintenanceTaskKind::Compaction)
    ));
}

#[test]
fn branch_pruning_policies_remain_below_lifecycle_until_retention_proof_exists() {
    let branch = branch_id(0x81);
    let mut state = BranchLocalState::empty(branch);
    install_l0_table(
        &mut state,
        branch,
        "prune-left",
        vec![put_row(branch, b"key", 1, 1_000, b"old")],
    );
    install_l0_table(
        &mut state,
        branch,
        "prune-right",
        vec![put_row(branch, b"key", 2, 2_000, b"new")],
    );

    let request = BranchCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0,
        "prune-rewrite",
    )
    .expect("request")
    .with_retention_policy(
        crate::branch::state::compaction::BranchCompactionRetentionPolicy::DropOlderVersions,
    );
    let error = state
        .compact_branch_owned_tables(&request)
        .expect_err("retention proof required");

    assert!(matches!(
        error,
        BranchRuntimeError::InvalidCompaction { .. }
    ));
    assert_eq!(state.owned_table_count(), 2);
}

#[test]
fn materialization_branch_errors_preserve_source_chain_and_code() {
    let state_branch = branch_id(0x82);
    let request_branch = branch_id(0x83);
    let mut state = BranchLocalState::empty(state_branch);
    let request = LifecycleMaterializationRequest::new(request_branch, 0, "materialization-error")
        .expect("request");

    let error = materialize_cache_branch(&mut state, &request).expect_err("wrong branch");

    assert_eq!(error.code(), "failed_precondition.lifecycle.branch_runtime");
    match error {
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::BranchRuntime,
            source: Some(_),
            ..
        } => {}
        other => panic!("unexpected error shape: {other:?}"),
    }
}

#[test]
fn durable_compaction_publishes_manifest_after_install() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x91);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-publish-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-publish-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    let request =
        LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "durable-publish")
            .expect("request");

    let outcome = runtime
        .compact_branch_tables(&request)
        .expect("durable compaction");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load table manifest")
        .expect("manifest");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert!(!outcome.checkpoint_required());
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert_eq!(manifest.levels().len(), 1);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert!(manifest.levels()[0].tables()[0]
        .table_identity()
        .as_str()
        .contains("durable-publish"));
    assert!(runtime
        .table_catalog()
        .build_manifest(runtime.branch_state())
        .is_ok());
}

#[test]
fn durable_compaction_manifest_failure_reports_debt_after_install() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x92);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-debt-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-debt-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    backend.fail_table_manifest_replacement_on_call(1);
    let request =
        LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "durable-debt")
            .expect("request");

    let outcome = runtime
        .compact_branch_tables(&request)
        .expect("durable compaction");
    let view = runtime.branch_state().capture_read_view().expect("view");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedManifestDebt
    );
    assert!(outcome.checkpoint_required());
    assert!(outcome.recovery_health().is_some());
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert_eq!(
        view.latest(&physical_key(branch, b"right"))
            .expect("read")
            .expect("visible")
            .row()
            .value(),
        b"right"
    );
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Completed
    );
    assert!(outcome.maintenance_outcome().source_error().is_some());
}

#[test]
fn durable_compaction_rejects_existing_output_with_conflicting_bytes() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9a);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-collision-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "durable-collision-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    let request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0,
        "durable-collision",
    )
    .expect("request");
    let branch_request = request.branch_request().expect("branch request");
    let plan = runtime
        .branch_state()
        .plan_branch_compaction(&branch_request)
        .expect("plan");
    // #3469: the output identity is content-complete (builder/compaction
    // config, budget-derived split target, cut hints, policy), so learn the
    // exact durable-path name from a scout run over identical inputs on a
    // separate backend rather than re-deriving it from a bare request.
    let scout_backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let mut scout = open_runtime(branch, scout_backend);
    *scout.branch_state_mut() = runtime.branch_state().clone();
    let scout_outcome = scout
        .compact_branch_tables(&request)
        .expect("scout compaction");
    let scout_object = scout_outcome.durable_output_objects()[0].clone();
    let identity = TableIdentity::new(
        scout_object
            .as_str()
            .rsplit('/')
            .next()
            .expect("object leaf"),
    )
    .expect("scout identity");
    let mut wrong_rows = vec![
        TableRow::new(put_row(branch, b"xxxx", 1, 1_000, b"left")),
        TableRow::new(put_row(branch, b"yyyyy", 2, 2_000, b"right")),
    ];
    sort_table_rows_by_key(&mut wrong_rows);
    let wrong_bytes = ImmutableTableBuilder::new(TableBuilderConfig::default())
        .expect("builder")
        .build_from_rows(
            TableIdentity::new(identity.as_str()).expect("identity"),
            &wrong_rows,
        )
        .expect("wrong artifact")
        .into_bytes();
    runtime
        .services()
        .table_object()
        .publish_create(
            &branch.to_string(),
            u32::from(plan.output_level().expect("output level").raw()),
            identity.as_str(),
            &wrong_bytes,
        )
        .expect("publish conflicting object");

    let error = runtime
        .compact_branch_tables(&request)
        .expect_err("conflicting existing object");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.rewrite_publication"
    );
    assert_eq!(runtime.branch_state().owned_table_count(), 2);
    assert!(runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .is_none());
}

#[test]
fn durable_materialization_publishes_manifest_after_layer_removal() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let parent = branch_id(0x93);
    let child = branch_id(0x94);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "durable-materialization-parent",
        vec![put_row(parent, b"inherited", 3, 3_000, b"parent")],
    );
    let (child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let mut runtime = open_runtime(child, backend);
    *runtime.branch_state_mut() = child_state;
    let request =
        LifecycleMaterializationRequest::new(child, 0, "durable-materialization").expect("request");

    let outcome = runtime
        .materialize_inherited_layer(&request)
        .expect("materialization");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load table manifest")
        .expect("manifest");

    assert_eq!(
        outcome.status(),
        LifecycleMaterializationStatus::CompletedDurable
    );
    assert_eq!(runtime.branch_state().inherited_layer_count(), 0);
    assert_eq!(manifest.inherited_layers().len(), 0);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert!(manifest.levels()[0].tables()[0]
        .table_identity()
        .as_str()
        .contains("durable-materialization"));
    assert_eq!(
        runtime
            .branch_state()
            .capture_read_view()
            .expect("view")
            .latest(&physical_key(child, b"inherited"))
            .expect("read")
            .expect("visible")
            .row()
            .value(),
        b"parent"
    );
}

#[test]
fn durable_materialization_retry_after_manifest_debt_publishes_manifest() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let parent = branch_id(0x9b);
    let child = branch_id(0x9c);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "durable-materialization-retry-parent",
        vec![put_row(parent, b"inherited", 3, 3_000, b"parent")],
    );
    let (child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let mut runtime = open_runtime(child, backend);
    *runtime.branch_state_mut() = child_state;
    backend.fail_table_manifest_replacement_on_call(1);
    let request = LifecycleMaterializationRequest::new(child, 0, "durable-materialization-retry")
        .expect("request");
    let first = runtime
        .materialize_inherited_layer(&request)
        .expect("first materialization");
    let handle = first
        .materialization_handle()
        .expect("materialization handle");
    assert_eq!(
        first.status(),
        LifecycleMaterializationStatus::CompletedManifestDebt
    );
    assert_eq!(runtime.branch_state().inherited_layer_count(), 0);
    assert!(runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load failed manifest")
        .is_none());

    let retry =
        LifecycleMaterializationRequest::from_handle(handle, "durable-materialization-retry")
            .expect("retry request");
    let second = runtime
        .materialize_inherited_layer(&retry)
        .expect("retry materialization");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load manifest")
        .expect("manifest");

    assert_eq!(
        second.status(),
        LifecycleMaterializationStatus::AlreadyMaterialized
    );
    assert_eq!(manifest.inherited_layers().len(), 0);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
}

#[test]
fn durable_compaction_publishes_output_before_manifest_and_reopens_before_install() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9d);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "order-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "order-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );

    let outcome = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "order")
                .expect("request"),
        )
        .expect("durable compaction");
    let events = backend.events();
    let output_event = event_index(&events, CheckpointBackendEvent::TableObjectCreate);
    let manifest_event = event_index(&events, CheckpointBackendEvent::TableManifestReplace);

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert!(output_event < manifest_event);
    assert_eq!(backend.table_object_create_calls(), 1);
    assert_eq!(backend.table_manifest_replace_calls(), 1);
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert!(runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .is_some());
}

#[test]
fn durable_compaction_manifest_includes_outputs_and_excludes_replaced_inputs() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9e);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "manifest-old-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "manifest-old-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );

    let outcome = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(
                branch,
                BranchCompactionKind::CompactL0,
                "manifest-new",
            )
            .expect("request"),
        )
        .expect("durable compaction");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .expect("manifest");
    let maintenance = outcome.maintenance_outcome();
    let names = maintenance.affected_object_names();

    assert_eq!(manifest.levels().len(), 1);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    let output_identity = manifest.levels()[0].tables()[0].table_identity().as_str();
    assert!(output_identity.contains("manifest-new"));
    assert!(!output_identity.contains("manifest-old-left"));
    assert!(!output_identity.contains("manifest-old-right"));
    assert!(names.iter().any(|name| name.contains("manifest-new")));
    assert!(names.iter().any(|name| name.contains("manifest-old-left")));
    assert!(names.iter().any(|name| name.contains("manifest-old-right")));
    assert!(runtime
        .table_catalog()
        .build_manifest(runtime.branch_state())
        .is_ok());
}

#[test]
fn durable_compaction_no_candidate_is_deferred_without_publication() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x9f);
    let mut runtime = open_runtime(branch, backend);

    let outcome = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "empty")
                .expect("request"),
        )
        .expect("durable compaction");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::DeferredNoCandidate
    );
    assert_eq!(backend.table_object_create_calls(), 0);
    assert_eq!(backend.table_manifest_replace_calls(), 0);
    assert!(runtime.branch_state().is_empty());
}

#[test]
fn durable_materialization_manifest_includes_replacements_and_removes_inherited_layer() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let parent = branch_id(0xa0);
    let child = branch_id(0xa1);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "manifest-material-parent",
        vec![put_row(parent, b"inherited", 3, 3_000, b"parent")],
    );
    let (child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    let mut runtime = open_runtime(child, backend);
    *runtime.branch_state_mut() = child_state;

    let outcome = runtime
        .materialize_inherited_layer(
            &LifecycleMaterializationRequest::new(child, 0, "manifest-material").expect("request"),
        )
        .expect("durable materialization");
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .expect("load manifest")
        .expect("manifest");

    assert_eq!(
        outcome.status(),
        LifecycleMaterializationStatus::CompletedDurable
    );
    assert_eq!(manifest.inherited_layers().len(), 0);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    assert!(manifest.levels()[0].tables()[0]
        .table_identity()
        .as_str()
        .contains("manifest-material"));
    assert_eq!(backend.table_object_create_calls(), 1);
}

#[test]
fn durable_compaction_preserves_reads_tombstones_timestamps_and_ttl_rows() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa2);
    let mut runtime = open_runtime(branch, backend);
    *runtime.branch_state_mut() = read_shape_state(branch);
    let history_key = physical_key(branch, b"history");
    let scan_prefix = physical_key(branch, b"scan-");
    let expiring_key = physical_key(branch, b"scan-b");
    let before = runtime.branch_state().capture_read_view().expect("before");
    let before_history = history_versions(
        &before
            .history(&history_key, BranchHistoryOptions::all())
            .expect("history"),
    );
    let before_prefix = scan_user_keys(
        &before
            .scan_prefix(
                &BranchScanBounds::prefix(&scan_prefix),
                BranchReadBound::latest(),
            )
            .expect("prefix"),
    );

    let outcome = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(
                branch,
                BranchCompactionKind::CompactL0,
                "read-parity",
            )
            .expect("request"),
        )
        .expect("durable compaction");
    let after = runtime.branch_state().capture_read_view().expect("after");

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedDurable
    );
    assert_eq!(
        history_versions(
            &after
                .history(&history_key, BranchHistoryOptions::all())
                .expect("history")
        ),
        before_history
    );
    assert_eq!(
        scan_user_keys(
            &after
                .scan_prefix(
                    &BranchScanBounds::prefix(&scan_prefix),
                    BranchReadBound::latest(),
                )
                .expect("prefix")
        ),
        before_prefix
    );
    let history = after
        .history(&history_key, BranchHistoryOptions::all())
        .expect("history");
    assert!(history.iter().any(|row| row.row().is_tombstone()));
    assert!(after
        .history(&expiring_key, BranchHistoryOptions::all())
        .expect("expired history")
        .iter()
        .any(|row| row.row().expires_at() == Timestamp::from_micros(4_500)));
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
fn durable_materialization_preserves_reads_and_fork_gate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let parent = branch_id(0xa3);
    let child = branch_id(0xa4);
    let mut runtime = open_runtime(child, backend);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "material-read-parent",
        vec![
            put_row(parent, b"inherited", 1, 1_000, b"parent"),
            put_row(parent, b"shared", 2, 2_000, b"parent-shared"),
        ],
    );
    let (mut child_state, _) = parent_state
        .fork_into_empty_child(child)
        .expect("fork child");
    parent_state
        .append_committed_row(put_row(parent, b"post-fork", 3, 3_000, b"late-parent"))
        .expect("append post-fork parent");
    child_state
        .append_committed_row(put_row(child, b"shared", 4, 4_000, b"child-shared"))
        .expect("append child row");
    *runtime.branch_state_mut() = child_state;
    let shared_key = physical_key(child, b"shared");
    let post_fork_key = physical_key(child, b"post-fork");
    let before = runtime.branch_state().capture_read_view().expect("before");
    let before_shared = before
        .latest(&shared_key)
        .expect("shared")
        .expect("visible")
        .row()
        .clone();
    assert!(before.latest(&post_fork_key).expect("post fork").is_none());

    let outcome = runtime
        .materialize_inherited_layer(
            &LifecycleMaterializationRequest::new(child, 0, "material-read-parity")
                .expect("request"),
        )
        .expect("durable materialization");
    let after = runtime.branch_state().capture_read_view().expect("after");

    assert_eq!(
        outcome.status(),
        LifecycleMaterializationStatus::CompletedDurable
    );
    assert_eq!(
        after
            .latest(&shared_key)
            .expect("shared")
            .expect("visible")
            .row(),
        &before_shared
    );
    assert!(after.latest(&post_fork_key).expect("post fork").is_none());
}

#[test]
fn rewrite_output_publish_failure_leaves_reads_unchanged() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa5);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "publish-fail-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "publish-fail-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    let key = physical_key(branch, b"right");
    let before = runtime
        .branch_state()
        .capture_read_view()
        .expect("before")
        .latest(&key)
        .expect("read")
        .expect("visible")
        .row()
        .clone();
    backend.fail_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(
                branch,
                BranchCompactionKind::CompactL0,
                "publish-fail",
            )
            .expect("request"),
        )
        .expect_err("publish failure");
    let after = runtime
        .branch_state()
        .capture_read_view()
        .expect("after")
        .latest(&key)
        .expect("read")
        .expect("visible")
        .row()
        .clone();

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.rewrite_publication"
    );
    assert_eq!(before, after);
    assert_eq!(runtime.branch_state().owned_table_count(), 2);
    assert!(runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .expect("load manifest")
        .is_none());
}

#[test]
fn rewrite_output_publish_uncertain_names_possibly_visible_object() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa6);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "publish-uncertain-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "publish-uncertain-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    backend.uncertain_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(
                branch,
                BranchCompactionKind::CompactL0,
                "publish-uncertain",
            )
            .expect("request"),
        )
        .expect_err("uncertain publish");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication"
    );
    match error {
        LifecycleError::RewritePublicationUncertain { objects, .. } => {
            assert_eq!(objects.len(), 1);
            assert!(objects[0].contains("publish-uncertain"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(runtime.branch_state().owned_table_count(), 2);
}

#[test]
fn rewrite_output_reopen_failure_leaves_reads_unchanged_and_names_orphan() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa7);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "reopen-fail-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "reopen-fail-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    backend.corrupt_table_object_create_on_call(1);

    let error = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(
                branch,
                BranchCompactionKind::CompactL0,
                "reopen-fail",
            )
            .expect("request"),
        )
        .expect_err("reopen failure");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
    match error {
        LifecycleError::RewritePublicationOrphaned { objects, .. } => {
            assert_eq!(objects.len(), 1);
            assert!(objects[0].contains("reopen-fail"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(runtime.branch_state().owned_table_count(), 2);
    assert_eq!(backend.table_object_names().len(), 1);
    assert_eq!(backend.delete_calls(), 0);
}

#[test]
fn rewrite_manifest_publish_uncertain_after_install_reports_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa8);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "manifest-uncertain-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "manifest-uncertain-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    backend.uncertain_table_manifest_replacement_on_call(1);

    let outcome = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(
                branch,
                BranchCompactionKind::CompactL0,
                "manifest-uncertain",
            )
            .expect("request"),
        )
        .expect("manifest uncertainty is forward progress debt");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(
        outcome.status(),
        LifecycleCompactionStatus::CompletedManifestDebt
    );
    assert!(outcome.checkpoint_required());
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert!(maintenance.recovery_health().is_some());
    assert_eq!(
        maintenance.source_error().expect("source error").code(),
        "ambiguous_commit.lifecycle.table_manifest_publication"
    );
}

#[test]
fn recovery_after_durable_compaction_uses_manifest_outputs() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa9);
    {
        let mut runtime = open_runtime(branch, backend);
        install_l0_table(
            runtime.branch_state_mut(),
            branch,
            "recover-compact-left",
            vec![put_row(branch, b"key", 1, 1_000, b"old")],
        );
        install_l0_table(
            runtime.branch_state_mut(),
            branch,
            "recover-compact-right",
            vec![put_row(branch, b"key", 2, 2_000, b"new")],
        );
        runtime
            .compact_branch_tables(
                &LifecycleCompactionRequest::new(
                    branch,
                    BranchCompactionKind::CompactL0,
                    "recover-compact",
                )
                .expect("request"),
            )
            .expect("durable compaction");
    }

    let reopened = open_runtime(branch, backend);
    let visible = reopened
        .branch_state()
        .capture_read_view()
        .expect("view")
        .latest(&physical_key(branch, b"key"))
        .expect("read")
        .expect("visible")
        .row()
        .clone();

    assert_eq!(visible.value(), b"new");
    assert_eq!(reopened.branch_state().owned_table_count(), 1);
    assert_eq!(
        reopened
            .table_catalog()
            .build_manifest(reopened.branch_state())
            .expect("manifest")
            .levels()[0]
            .tables()
            .len(),
        1
    );
}

#[test]
fn recovery_after_durable_materialization_uses_manifest_replacements() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let parent = branch_id(0xaa);
    let child = branch_id(0xab);
    {
        let mut parent_state = BranchLocalState::empty(parent);
        install_l0_table(
            &mut parent_state,
            parent,
            "recover-material-parent",
            vec![put_row(parent, b"inherited", 3, 3_000, b"parent")],
        );
        let (child_state, _) = parent_state
            .fork_into_empty_child(child)
            .expect("fork child");
        let mut runtime = open_runtime(child, backend);
        *runtime.branch_state_mut() = child_state;
        runtime
            .materialize_inherited_layer(
                &LifecycleMaterializationRequest::new(child, 0, "recover-material")
                    .expect("request"),
            )
            .expect("durable materialization");
    }

    let reopened = open_runtime(child, backend);
    let visible = reopened
        .branch_state()
        .capture_read_view()
        .expect("view")
        .latest(&physical_key(child, b"inherited"))
        .expect("read")
        .expect("visible")
        .row()
        .clone();

    assert_eq!(visible.value(), b"parent");
    assert_eq!(reopened.branch_state().inherited_layer_count(), 0);
    assert_eq!(reopened.branch_state().owned_table_count(), 1);
    assert_eq!(
        reopened
            .table_catalog()
            .build_manifest(reopened.branch_state())
            .expect("manifest")
            .levels()[0]
            .tables()
            .len(),
        1
    );
}

#[test]
fn recovery_after_output_publish_before_install_ignores_orphan_output() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xac);
    {
        let mut runtime = open_runtime(branch, backend);
        install_l0_table(
            runtime.branch_state_mut(),
            branch,
            "recover-orphan-left",
            vec![put_row(branch, b"key", 1, 1_000, b"old")],
        );
        install_l0_table(
            runtime.branch_state_mut(),
            branch,
            "recover-orphan-right",
            vec![put_row(branch, b"key", 2, 2_000, b"new")],
        );
        backend.corrupt_table_object_create_on_call(1);
        let error = runtime
            .compact_branch_tables(
                &LifecycleCompactionRequest::new(
                    branch,
                    BranchCompactionKind::CompactL0,
                    "recover-orphan",
                )
                .expect("request"),
            )
            .expect_err("orphaned output");
        assert_eq!(
            error.code(),
            "ambiguous_commit.lifecycle.rewrite_publication_orphan"
        );
        assert_eq!(backend.table_object_names().len(), 1);
    }

    let reopened = open_runtime(branch, backend);

    assert!(reopened.branch_state().is_empty());
    assert!(reopened
        .table_catalog()
        .build_manifest(reopened.branch_state())
        .expect("manifest")
        .levels()
        .is_empty());
}

#[test]
fn durable_rewrite_completion_does_not_persist_flush_watermark_or_truncate_wal() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xad);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "boundary-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "boundary-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );

    runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "boundary")
                .expect("request"),
        )
        .expect("durable compaction");
    let manifest = runtime
        .services()
        .manifest()
        .load_current()
        .expect("load database manifest")
        .expect("database manifest");

    assert_eq!(manifest.flushed_through_commit_id(), None);
    assert_eq!(backend.delete_calls(), 0);
    assert!(!backend
        .events()
        .iter()
        .any(|event| matches!(event, CheckpointBackendEvent::ObjectDelete)));
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_rewrite_uses_build_facts_and_lazy_reader_reopen() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xaf);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "fast-publish-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "fast-publish-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    crate::observability::perf_trace::reset();

    runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(
                branch,
                BranchCompactionKind::CompactL0,
                "fast-publish",
            )
            .expect("request"),
        )
        .expect("durable compaction");

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.table_compaction_output_tables_built(), 1);
    assert_eq!(perf.table_build_facts_from_streaming_metadata(), 1);
    assert_eq!(perf.table_rewrite_redundant_fact_decodes_avoided(), 1);
    // BS4.4l: the durable output now reopens lazily (metadata-only, disk-resident) instead of the eager
    // row-reuse handoff, and must not fully materialize.
    assert_eq!(perf.table_rewrite_reader_reopens_performed(), 1);
    assert_eq!(perf.table_lazy_full_materializations(), 0);
    assert_eq!(runtime.branch_state().owned_levels()[0].len(), 1);
}

#[test]
fn durable_rewrite_manifest_success_can_build_flush_coverage_candidate() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xae);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "coverage-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "coverage-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );

    runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "coverage")
                .expect("request"),
        )
        .expect("durable compaction");
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
fn durable_rewrite_does_not_delete_or_quarantine_replaced_or_orphaned_objects() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xaf);
    let mut runtime = open_runtime(branch, backend);
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "no-cleanup-left",
        vec![put_row(branch, b"left", 1, 1_000, b"left")],
    );
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "no-cleanup-right",
        vec![put_row(branch, b"right", 2, 2_000, b"right")],
    );
    backend.corrupt_table_object_create_on_call(1);
    let error = runtime
        .compact_branch_tables(
            &LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, "no-cleanup")
                .expect("request"),
        )
        .expect_err("orphaned output");

    assert_eq!(
        error.code(),
        "ambiguous_commit.lifecycle.rewrite_publication_orphan"
    );
    assert_eq!(backend.table_object_names().len(), 1);
    assert_eq!(backend.delete_calls(), 0);
    assert!(!backend
        .events()
        .iter()
        .any(|event| matches!(event, CheckpointBackendEvent::ObjectDelete)));
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

struct TestCompactionRunner<'a> {
    branch: &'a mut BranchLocalState,
}

impl MaintenanceTaskRunner for TestCompactionRunner<'_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let Some(request) = current_compaction_request_from_maintenance_task(task, self.branch)?
        else {
            return Ok(stale_compaction_maintenance_outcome());
        };
        Ok(compact_cache_branch(self.branch, &request)?.maintenance_outcome())
    }
}

fn event_index(events: &[CheckpointBackendEvent], expected: CheckpointBackendEvent) -> usize {
    events
        .iter()
        .position(|event| *event == expected)
        .expect("event must be present")
}
