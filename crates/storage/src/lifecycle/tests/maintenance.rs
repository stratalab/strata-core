use super::*;

mod shared;

use shared::*;

#[test]
fn maintenance_task_request_validates_kind_scope_pairs() {
    assert!(MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Flush,
        MaintenanceTaskPriority::Normal,
        MaintenanceTaskScope::Branch(branch_id(1)),
        MaintenanceTaskPolicy::coalescing(),
    )
    .is_ok());
    assert!(MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Flush,
        MaintenanceTaskPriority::Normal,
        MaintenanceTaskScope::Global,
        MaintenanceTaskPolicy::coalescing(),
    )
    .is_ok());
    assert_eq!(
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::Flush,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::Wal,
            MaintenanceTaskPolicy::coalescing(),
        ),
        Err(LifecycleError::MaintenanceTaskFailed {
            reason: "maintenance task scope does not match task kind",
        })
    );
    assert_eq!(
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::Flush,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::Global,
            MaintenanceTaskPolicy::drain_before_close(),
        ),
        Err(LifecycleError::MaintenanceTaskFailed {
            reason: "global flush tasks cannot be drained during close",
        })
    );
}

#[test]
fn maintenance_task_requests_accept_every_supported_kind_and_scope() {
    for (kind, scope) in valid_kind_scopes() {
        let request = MaintenanceTaskRequest::new(
            kind,
            MaintenanceTaskPriority::Normal,
            scope,
            MaintenanceTaskPolicy::ordinary(),
        )
        .expect("valid task request");
        assert_eq!(request.kind(), kind);
        assert_eq!(request.scope(), scope);
    }
}

#[test]
fn maintenance_task_ids_and_sequences_are_monotonic() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");

    let first = executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("first");
    let second = executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::ordinary(),
            ),
        )
        .expect("second");

    assert_eq!(first.task_id().get(), 1);
    assert_eq!(second.task_id().get(), 2);
    assert_eq!(executor.pending_tasks()[0].sequence(), 1);
    assert_eq!(executor.pending_tasks()[1].sequence(), 2);
}

#[test]
fn executor_does_not_depend_on_map_iteration_order() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    for request in [
        health_request(MaintenanceTaskPolicy::ordinary()),
        repair_request(
            MaintenanceTaskPriority::Critical,
            MaintenanceTaskPolicy::ordinary(),
        ),
        MaintenanceTaskRequest::flush(branch_id(9)),
        repair_request(
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskPolicy::ordinary(),
        ),
    ] {
        executor.enqueue(open, request).expect("enqueue");
    }
    let mut runner = RecordingRunner::completed();

    let first = executor
        .run_next(open, &mut runner)
        .expect("run first")
        .expect("first");
    let second = executor
        .run_next(open, &mut runner)
        .expect("run second")
        .expect("second");
    let third = executor
        .run_next(open, &mut runner)
        .expect("run third")
        .expect("third");

    assert_eq!(first.task_kind(), MaintenanceTaskKind::Repair);
    assert_eq!(second.task_kind(), MaintenanceTaskKind::HealthCollection);
    assert_eq!(third.task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn maintenance_policy_and_coalesce_key_preserve_storage_scope() {
    let branch = branch_id(3);
    let request = MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Compaction,
        MaintenanceTaskPriority::High,
        MaintenanceTaskScope::TableLevel {
            branch_id: branch,
            level: 2,
        },
        MaintenanceTaskPolicy::coalescing_drain_before_close(),
    )
    .expect("request");

    assert_eq!(request.priority(), MaintenanceTaskPriority::High);
    assert_eq!(
        request.policy().close_policy(),
        MaintenanceClosePolicy::DrainBeforeClose
    );
    assert!(request.policy().coalesces());
    assert_eq!(
        request.coalesce_key(),
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::Compaction,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::TableLevel {
                branch_id: branch,
                level: 2,
            },
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("matching request")
        .coalesce_key()
    );
    assert_eq!(
        MaintenanceTaskPolicy::cancel_before_close().close_policy(),
        MaintenanceClosePolicy::CancelBeforeClose
    );
    assert!(!MaintenanceTaskPolicy::ordinary().coalesces());
}

#[test]
fn maintenance_debug_output_uses_storage_vocabulary() {
    let request = MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Checkpoint,
        MaintenanceTaskPriority::Critical,
        MaintenanceTaskScope::Checkpoint,
        MaintenanceTaskPolicy::coalescing(),
    )
    .expect("request");
    let debug = format!("{request:?}");

    assert!(debug.contains("Checkpoint"));
    for forbidden in [
        "Database::open",
        "manual maintenance command",
        "StrataHub",
        "VersionedValue",
        "EntityRef",
    ] {
        assert!(!debug.contains(forbidden));
    }
}

#[test]
fn maintenance_enqueue_requires_open_and_enforces_capacity() {
    let mut executor = LifecycleMaintenanceExecutor::new(1).expect("executor");
    let request = health_request(MaintenanceTaskPolicy::ordinary());

    assert!(matches!(
        executor.enqueue(LifecycleStateMachine::new(), request),
        Err(LifecycleError::InvalidLifecycleState { .. })
    ));
    assert_eq!(executor.status().pending_tasks(), 0);

    let open = open_state();
    let first = executor.enqueue(open, request).expect("first enqueue");
    assert!(first.was_enqueued());
    assert_eq!(first.pending_tasks(), 1);

    let error = executor
        .enqueue(open, request)
        .expect_err("capacity rejects second pending task");
    assert_eq!(
        error,
        LifecycleError::MaintenanceQueueFull {
            reason: "maintenance queue is full",
        }
    );
    assert_eq!(executor.stats().queue_full(), 1);
    assert_eq!(executor.stats().max_pending_tasks(), 1);
}

#[test]
fn maintenance_admission_rejects_ordinary_work_outside_open() {
    let request = health_request(MaintenanceTaskPolicy::ordinary());
    for state in [
        new_state(),
        opening_state(),
        recovering_state(),
        closing_state(),
        closed_state(),
        failed_state(),
    ] {
        let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
        let error = executor
            .enqueue(state, request)
            .expect_err("ordinary maintenance rejected");

        assert_eq!(error.code(), "failed_precondition.lifecycle.state");
        assert_eq!(executor.status().pending_tasks(), 0);
        assert_eq!(executor.stats(), LifecycleMaintenanceStats::default());
    }
}

#[test]
fn maintenance_close_drain_requires_closing_and_ordinary_run_requires_open() {
    let open = open_state();
    let closing = closing_state();
    let mut runner = RecordingRunner::completed();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("enqueue");

    assert!(matches!(
        executor
            .drain_for_close(open, &mut runner)
            .expect_err("drain rejected outside closing"),
        LifecycleError::InvalidLifecycleState { .. }
    ));
    assert_eq!(executor.status().pending_tasks(), 1);
    assert!(matches!(
        executor
            .run_next(closing, &mut runner)
            .expect_err("ordinary run rejected while closing"),
        LifecycleError::InvalidLifecycleState { .. }
    ));
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn lifecycle_health_query_is_admitted_in_every_state() {
    for state in [
        LifecycleState::New,
        LifecycleState::Opening,
        LifecycleState::Recovering,
        LifecycleState::Open,
        LifecycleState::Closing,
        LifecycleState::Closed,
        LifecycleState::Failed,
    ] {
        assert!(
            LifecycleStateMachine::admit_state(state, LifecycleOperationKind::HealthQuery)
                .is_allowed(),
            "health query rejected in {state:?}",
        );
    }
}

#[test]
fn maintenance_queue_depth_allows_exact_capacity() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");

    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("first");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::ordinary(),
            ),
        )
        .expect("second reaches capacity");

    assert_eq!(executor.status().pending_tasks(), 2);
    assert_eq!(executor.stats().queue_full(), 0);
}

#[test]
fn maintenance_executor_orders_by_priority_then_fifo() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    let low = repair_request(
        MaintenanceTaskPriority::Low,
        MaintenanceTaskPolicy::ordinary(),
    );
    let high = repair_request(
        MaintenanceTaskPriority::High,
        MaintenanceTaskPolicy::ordinary(),
    );
    let normal = repair_request(
        MaintenanceTaskPriority::Normal,
        MaintenanceTaskPolicy::ordinary(),
    );

    executor.enqueue(open, low).expect("low");
    executor.enqueue(open, normal).expect("normal");
    executor.enqueue(open, high).expect("high");

    let mut runner = RecordingRunner::completed();
    let first = executor
        .run_next(open, &mut runner)
        .expect("run")
        .expect("task");
    let second = executor
        .run_next(open, &mut runner)
        .expect("run")
        .expect("task");
    let third = executor
        .run_next(open, &mut runner)
        .expect("run")
        .expect("task");

    assert_eq!(first.task_id().expect("task id").get(), 3);
    assert_eq!(second.task_id().expect("task id").get(), 2);
    assert_eq!(third.task_id().expect("task id").get(), 1);
    assert_eq!(executor.stats().completed(), 3);
}

#[test]
fn maintenance_executor_preserves_fifo_for_equal_priority() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    for index in 0..3 {
        executor
            .enqueue(
                open,
                MaintenanceTaskRequest::new(
                    MaintenanceTaskKind::Flush,
                    MaintenanceTaskPriority::Normal,
                    MaintenanceTaskScope::Branch(branch_id(index)),
                    MaintenanceTaskPolicy::ordinary(),
                )
                .expect("flush"),
            )
            .expect("enqueue");
    }

    let mut runner = RecordingRunner::completed();
    let ids = run_all_task_ids(open, &mut executor, &mut runner);

    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn maintenance_executor_order_survives_coalescing_and_canceling() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(6).expect("executor");
    let coalescing_flush = MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Flush,
        MaintenanceTaskPriority::Normal,
        MaintenanceTaskScope::Branch(branch_id(11)),
        MaintenanceTaskPolicy::coalescing(),
    )
    .expect("flush");
    executor.enqueue(open, coalescing_flush).expect("flush");
    executor
        .enqueue(open, coalescing_flush)
        .expect("coalesced flush");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::cancel_before_close(),
            ),
        )
        .expect("cancelable");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("health");

    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("cancel pending");
    assert_eq!(cancel.canceled_tasks(), 3);

    let mut runner = RecordingRunner::completed();
    let ids = run_all_task_ids(open, &mut executor, &mut runner);

    assert!(ids.is_empty());
    assert_eq!(executor.stats().coalesced(), 1);
}

#[test]
fn maintenance_executor_coalesces_pending_tasks_by_key() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(1).expect("executor");
    let request = MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Flush,
        MaintenanceTaskPriority::Normal,
        MaintenanceTaskScope::Branch(branch_id(9)),
        MaintenanceTaskPolicy::coalescing(),
    )
    .expect("flush request");

    let first = executor.enqueue(open, request).expect("first");
    let second = executor.enqueue(open, request).expect("coalesced");

    assert!(second.was_coalesced());
    assert_eq!(second.task_id(), first.task_id());
    assert_eq!(executor.status().pending_tasks(), 1);
    assert_eq!(executor.stats().coalesced(), 1);
    assert_eq!(executor.stats().queue_full(), 0);
}

#[test]
fn maintenance_executor_coalesces_each_coalescing_scope_independently() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    for request in [
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::Checkpoint,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::Checkpoint,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("checkpoint"),
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::WalTruncation,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::Wal,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("truncation"),
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::Compaction,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::TableLevel {
                branch_id: branch_id(12),
                level: 1,
            },
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("compaction"),
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::Materialization,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::InheritedLayer {
                branch_id: branch_id(13),
                layer_index: 0,
            },
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("materialization"),
    ] {
        let first = executor.enqueue(open, request).expect("first");
        let duplicate = executor.enqueue(open, request).expect("duplicate");
        assert!(duplicate.was_coalesced());
        assert_eq!(duplicate.task_id(), first.task_id());
    }

    let second_layer = executor
        .enqueue(
            open,
            MaintenanceTaskRequest::materialization_layer(branch_id(13), 1),
        )
        .expect("second layer");
    assert!(second_layer.was_enqueued());
    assert_eq!(executor.status().pending_tasks(), 5);
    assert_eq!(executor.stats().coalesced(), 4);
}

#[test]
fn checkpoint_tasks_coalesce_across_global_and_checkpoint_scope() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(1).expect("executor");
    let checkpoint_scoped = MaintenanceTaskRequest::checkpoint();
    let global_scoped = MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Checkpoint,
        MaintenanceTaskPriority::High,
        MaintenanceTaskScope::Global,
        MaintenanceTaskPolicy::coalescing(),
    )
    .expect("global checkpoint");

    let first = executor.enqueue(open, checkpoint_scoped).expect("first");
    let second = executor.enqueue(open, global_scoped).expect("second");

    assert!(second.was_coalesced());
    assert_eq!(second.task_id(), first.task_id());
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn checkpoint_tasks_do_not_coalesce_across_different_options() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    let first = executor
        .enqueue(open, MaintenanceTaskRequest::checkpoint())
        .expect("plain checkpoint");
    let explicit = MaintenanceTaskRequest::checkpoint_with_options(
        MaintenanceCheckpointOptions::new(Some(42), true),
    );
    let second = executor
        .enqueue(open, explicit)
        .expect("explicit checkpoint");
    let third = executor
        .enqueue(open, explicit)
        .expect("duplicate explicit");

    assert!(first.was_enqueued());
    assert!(second.was_enqueued());
    assert_ne!(second.task_id(), first.task_id());
    assert!(third.was_coalesced());
    assert_eq!(third.task_id(), second.task_id());
    assert_eq!(executor.status().pending_tasks(), 2);
    assert_eq!(executor.stats().coalesced(), 1);
}

/// The mechanism behind the #2953 / #3182 drain flake, made deterministic.
///
/// `next_startable_task_index` filters out tasks whose lane is already at
/// capacity, so a queued task sharing a lane with an ACTIVE one is
/// pending-but-unstartable. `run_next_matching` then returns `None`, which is
/// what ends `drain_maintenance`'s `while let Some(..)` loop — so a drain can
/// return with a non-empty queue while making no progress at all.
///
/// No round count can clear that: the blocker is a held lane, not elapsed time,
/// which is why raising the bound (#2868, #3209) did not fix the flake. A
/// caller judging "the queue stopped shrinking" as CHURN must first ask whether
/// something is in flight.
#[test]
fn a_queued_task_whose_lane_is_active_leaves_the_drain_with_work_it_cannot_start() {
    let open = open_state();
    let active =
        MaintenanceTask::new_for_test(7, health_request(MaintenanceTaskPolicy::ordinary()))
            .expect("active task");
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    // Same kind, so the same lane: Health is single-occupancy.
    executor.set_active_for_test(active);
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("queued task");

    let mut runner = RecordingRunner::completed();
    let ran = executor
        .run_next_matching(open, &mut runner, |_| true)
        .expect("running must not fail");

    // Nothing ran, yet the queue is not empty: this is the exact observation
    // the flaky test misreads as churn.
    assert!(ran.is_none(), "the queued task's lane is occupied");
    let status = executor.status();
    assert_eq!(status.pending_tasks(), 1);
    assert_eq!(
        status.active_task(),
        Some(active.id()),
        "the in-flight task is observable, which is what distinguishes \
         blocked-but-progressing from churn"
    );
}

#[test]
fn cancel_pending_does_not_cancel_active_task() {
    let closing = closing_state();
    let active = MaintenanceTask::new_for_test(
        7,
        health_request(MaintenanceTaskPolicy::cancel_before_close()),
    )
    .expect("active task");
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor.set_active_for_test(active);
    executor
        .enqueue(
            open_state(),
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::cancel_before_close(),
            ),
        )
        .expect("pending task");

    let canceled = executor
        .cancel_pending_for_close(closing)
        .expect("cancel pending");

    assert_eq!(canceled.canceled_tasks(), 1);
    assert_eq!(executor.status().active_task(), Some(active.id()));
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn maintenance_executor_does_not_coalesce_non_coalescing_requests() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    for request in [
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::Purge,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::Quarantine,
            MaintenanceTaskPolicy::ordinary(),
        )
        .expect("purge"),
        repair_request(
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskPolicy::ordinary(),
        ),
    ] {
        let first = executor.enqueue(open, request).expect("first");
        let second = executor.enqueue(open, request).expect("second");
        assert!(first.was_enqueued());
        assert!(second.was_enqueued());
        assert_ne!(first.task_id(), second.task_id());
    }

    assert_eq!(executor.status().pending_tasks(), 4);
    assert_eq!(executor.stats().coalesced(), 0);
}

#[test]
fn maintenance_executor_does_not_coalesce_active_task() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let request = MaintenanceTaskRequest::new(
        MaintenanceTaskKind::Flush,
        MaintenanceTaskPriority::Normal,
        MaintenanceTaskScope::Branch(branch_id(10)),
        MaintenanceTaskPolicy::coalescing(),
    )
    .expect("flush request");
    executor.enqueue(open, request).expect("first");

    let mut fault = FailAt::new(MaintenanceFaultPoint::AtTaskStart);
    let mut runner = RecordingRunner::completed();
    assert!(executor
        .run_next_with_fault(open, &mut runner, &mut fault)
        .is_err());

    let second = executor.enqueue(open, request).expect("new pending task");
    assert!(second.was_enqueued());
    assert_eq!(second.task_id().get(), 2);
}

#[test]
fn maintenance_executor_starts_flush_while_rewrite_is_active() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    let rewrite = MaintenanceTaskRequest::compaction(branch_id(10), 0);
    let flush = MaintenanceTaskRequest::flush(branch_id(10));
    executor.enqueue(open, rewrite).expect("rewrite");
    executor.enqueue(open, flush).expect("flush");

    let active_rewrite = executor
        .start_next_matching(open, |task| task.kind() == MaintenanceTaskKind::Compaction)
        .expect("start rewrite")
        .expect("rewrite task");
    assert_eq!(active_rewrite.kind(), MaintenanceTaskKind::Compaction);

    let active_flush = executor
        .start_next_matching(open, |task| task.kind() == MaintenanceTaskKind::Flush)
        .expect("start flush")
        .expect("flush task");

    assert_eq!(active_flush.kind(), MaintenanceTaskKind::Flush);
    assert_eq!(executor.status().active_tasks(), 2);
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn maintenance_executor_serializes_rewrite_lane() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    executor
        .enqueue(open, MaintenanceTaskRequest::compaction(branch_id(10), 0))
        .expect("compaction");
    executor
        .enqueue(open, MaintenanceTaskRequest::materialization(branch_id(11)))
        .expect("materialization");

    let active = executor
        .start_next_matching(open, |_| true)
        .expect("start rewrite")
        .expect("active rewrite");
    assert_eq!(active.kind(), MaintenanceTaskKind::Compaction);

    let blocked = executor
        .start_next_matching(open, |_| true)
        .expect("same lane is skipped");
    assert_eq!(blocked, None);
    assert_eq!(executor.status().active_tasks(), 1);
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn maintenance_executor_clears_active_after_runner_error() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");

    let mut runner = ErrorRunner;
    let error = executor
        .run_next(open, &mut runner)
        .expect_err("runner error");

    assert_eq!(error.code(), "io.lifecycle.backend");
    assert!(error.source().is_some());
    assert_eq!(executor.status().active_task(), None);
    assert_eq!(executor.stats().failed(), 1);
}

#[test]
fn maintenance_executor_run_empty_queue_returns_no_work_without_stats() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let mut runner = RecordingRunner::completed();

    assert_eq!(
        executor.run_next(open, &mut runner).expect("empty run"),
        None
    );
    assert_eq!(executor.stats(), LifecycleMaintenanceStats::default());
}

#[test]
fn maintenance_executor_records_deferred_and_preserves_effects() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");

    let mut runner = EffectsRunner {
        status: MaintenanceOutcomeStatus::Deferred,
        affected_objects: 3,
        bytes_reclaimed: 99,
        retryable: true,
    };
    let outcome = executor
        .run_next(open, &mut runner)
        .expect("run")
        .expect("task");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(outcome.affected_objects(), 3);
    assert_eq!(outcome.bytes_reclaimed(), 99);
    assert!(outcome.retryable());
    assert_eq!(executor.stats().started(), 1);
    assert_eq!(executor.stats().deferred(), 1);
    assert_eq!(executor.status().active_task(), None);
}

#[test]
fn maintenance_executor_converts_after_run_fault_to_failed_outcome() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");

    let mut runner = RecordingRunner::completed();
    let mut fault = FailAt::new(MaintenanceFaultPoint::AfterTaskRun);
    let outcome = executor
        .run_next_with_fault(open, &mut runner, &mut fault)
        .expect("after-run fault is reported as failed outcome")
        .expect("task outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Failed);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(executor.status().active_task(), None);
    assert_eq!(executor.stats().failed(), 1);
}

#[test]
fn maintenance_executor_attaches_health_debt_to_failed_outcome() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");

    let mut runner = RecordingRunner::failed();
    let outcome = executor
        .run_next(open, &mut runner)
        .expect("failed outcome")
        .expect("task outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Failed);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(executor.stats().failed(), 1);
}

#[test]
fn maintenance_executor_counts_canceled_outcomes_as_canceled() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");

    let mut runner = RecordingRunner::canceled();
    let outcome = executor
        .run_next(open, &mut runner)
        .expect("canceled outcome")
        .expect("task outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Canceled);
    assert_eq!(executor.stats().canceled(), 1);
    assert_eq!(executor.stats().deferred(), 0);
}

#[test]
fn maintenance_executor_cancel_and_drain_respect_close_policy() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("drain");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::cancel_before_close(),
            ),
        )
        .expect("cancel");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Low,
                MaintenanceTaskPolicy::ordinary(),
            ),
        )
        .expect("ordinary");

    let mut runner = RecordingRunner::completed();
    let drain = executor
        .drain_for_close(closing, &mut runner)
        .expect("drain for close");
    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(drain.outcomes().len(), 1);
    assert_eq!(executor.status().pending_tasks(), 2);

    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("cancel for close");
    assert_eq!(cancel.canceled_tasks(), 2);
    assert_eq!(executor.status().pending_tasks(), 0);
    assert_eq!(executor.stats().drained(), 1);
    assert_eq!(executor.stats().canceled(), 2);
}

#[test]
fn maintenance_executor_drain_error_keeps_task_pending_for_retry() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("drain");
    let original_task_id = executor.pending_tasks()[0].id();
    let mut runner = ErrorRunner;

    let error = executor
        .drain_for_close(closing, &mut runner)
        .expect_err("drain failure");

    assert_eq!(error.code(), "io.lifecycle.backend");
    assert_eq!(executor.status().pending_tasks(), 1);
    assert_eq!(executor.pending_tasks()[0].id(), original_task_id);
    assert_eq!(executor.stats().failed(), 1);
    let mut retry_runner = RecordingRunner::completed();
    let retry = executor
        .drain_for_close(closing, &mut retry_runner)
        .expect("retry drain");
    assert_eq!(retry.drained_tasks(), 1);
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn close_drain_preserves_task_order() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Low,
                MaintenanceTaskPolicy::drain_before_close(),
            ),
        )
        .expect("low priority");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("normal priority");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::High,
                MaintenanceTaskPolicy::drain_before_close(),
            ),
        )
        .expect("high priority");
    let mut runner = RecordingRunner::completed();

    let drain = executor
        .drain_for_close(closing, &mut runner)
        .expect("drain for close");
    let ids = drain
        .outcomes()
        .iter()
        .map(|outcome| outcome.task_id().expect("task id").get())
        .collect::<Vec<_>>();

    assert_eq!(ids, vec![3, 2, 1]);
    assert_eq!(drain.drained_tasks(), 3);
    assert_eq!(drain.stats().drained(), 3);
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn close_retry_after_drain_failure_does_not_rerun_completed_tasks() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("first drain task");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::drain_before_close(),
            ),
        )
        .expect("second drain task");
    let first_id = executor.pending_tasks()[0].id();
    let second_id = executor.pending_tasks()[1].id();
    let mut runner = FailsAfterCompletions::new(1);

    let error = executor
        .drain_for_close(closing, &mut runner)
        .expect_err("second task fails");

    assert_eq!(error.code(), "io.lifecycle.backend");
    assert_eq!(executor.stats().completed(), 1);
    assert_eq!(executor.stats().failed(), 1);
    assert_eq!(executor.status().pending_tasks(), 1);
    assert_eq!(executor.pending_tasks()[0].id(), second_id);
    assert_ne!(executor.pending_tasks()[0].id(), first_id);

    let mut retry_runner = RecordingRunner::completed();
    let retry = executor
        .drain_for_close(closing, &mut retry_runner)
        .expect("retry drain");
    assert_eq!(retry.drained_tasks(), 1);
    assert_eq!(retry.outcomes()[0].task_id(), Some(second_id));
    assert_eq!(executor.status().pending_tasks(), 0);
    assert_eq!(executor.stats().completed(), 2);
}

#[test]
fn maintenance_executor_empty_drain_and_cancel_are_idempotent() {
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let mut runner = RecordingRunner::completed();

    let drain = executor
        .drain_for_close(closing, &mut runner)
        .expect("empty drain");
    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("empty cancel");

    assert_eq!(drain.drained_tasks(), 0);
    assert_eq!(cancel.canceled_tasks(), 0);
    assert_eq!(executor.stats(), LifecycleMaintenanceStats::default());
}

#[test]
fn maintenance_executor_cancel_keeps_only_drain_required_tasks() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(4).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::cancel_before_close()),
        )
        .expect("cancelable");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::drain_before_close(),
            ),
        )
        .expect("drain");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Low,
                MaintenanceTaskPolicy::ordinary(),
            ),
        )
        .expect("ordinary");

    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("cancel close");

    assert_eq!(cancel.canceled_tasks(), 2);
    assert_eq!(
        executor
            .pending_tasks()
            .iter()
            .map(|task| task.policy().close_policy())
            .collect::<Vec<_>>(),
        vec![MaintenanceClosePolicy::DrainBeforeClose]
    );
}

#[test]
fn close_cancel_sweep_removes_cancel_before_close_tasks() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::cancel_before_close()),
        )
        .expect("enqueue");

    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("cancel close");

    assert_eq!(cancel.canceled_tasks(), 1);
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn close_cancel_sweep_removes_or_defers_ordinary_tasks_by_contract() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");

    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("cancel close");

    assert_eq!(cancel.canceled_tasks(), 1);
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn close_drain_runs_drain_before_close_tasks() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("enqueue");
    let mut runner = RecordingRunner::completed();

    let drain = executor
        .drain_for_close(closing, &mut runner)
        .expect("drain");

    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(
        drain.outcomes()[0].status(),
        MaintenanceOutcomeStatus::Completed
    );
    assert_eq!(executor.status().pending_tasks(), 0);
}

#[test]
fn close_drain_failure_returns_typed_close_error() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("enqueue");
    let mut runner = ErrorRunner;

    let error = executor
        .drain_for_close(closing, &mut runner)
        .expect_err("drain error");

    assert_eq!(error.code(), "io.lifecycle.backend");
    assert!(error.source().is_some());
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn close_does_not_cancel_active_task_by_queue_removal() {
    let closing = closing_state();
    let active = MaintenanceTask::new_for_test(
        11,
        health_request(MaintenanceTaskPolicy::cancel_before_close()),
    )
    .expect("active task");
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor.set_active_for_test(active);

    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("cancel close");

    assert_eq!(cancel.canceled_tasks(), 0);
    assert_eq!(executor.status().active_task(), Some(active.id()));
}

#[test]
fn close_failure_before_cancel_leaves_tasks_pending() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");

    let error = executor
        .cancel_pending_for_close(open)
        .expect_err("cancel outside closing");

    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn close_failure_after_cancel_before_drain_reports_canceled_count() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(3).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("ordinary");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::drain_before_close(),
            ),
        )
        .expect("drain");

    let cancel = executor
        .cancel_pending_for_close(closing)
        .expect("cancel close");
    let mut runner = ErrorRunner;
    let error = executor
        .drain_for_close(closing, &mut runner)
        .expect_err("drain fails");

    assert_eq!(cancel.canceled_tasks(), 1);
    assert_eq!(error.code(), "io.lifecycle.backend");
    assert_eq!(executor.stats().canceled(), 1);
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn close_failure_during_drain_preserves_completed_drain_facts() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(3).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("first");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::drain_before_close(),
            ),
        )
        .expect("second");
    let mut runner = FailsAfterCompletions::new(1);

    let error = executor
        .drain_for_close(closing, &mut runner)
        .expect_err("second task fails");

    assert_eq!(error.code(), "io.lifecycle.backend");
    assert_eq!(executor.stats().completed(), 1);
    assert_eq!(executor.stats().drained(), 1);
    assert_eq!(executor.status().pending_tasks(), 1);
}

#[test]
fn close_retry_does_not_restart_completed_ordinary_work() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(3).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("first");
    executor
        .enqueue(
            open,
            repair_request(
                MaintenanceTaskPriority::Normal,
                MaintenanceTaskPolicy::drain_before_close(),
            ),
        )
        .expect("second");
    let mut runner = FailsAfterCompletions::new(1);
    assert!(executor.drain_for_close(closing, &mut runner).is_err());
    let completed_before_retry = executor.stats().completed();

    let mut retry_runner = RecordingRunner::completed();
    executor
        .drain_for_close(closing, &mut retry_runner)
        .expect("retry");

    assert_eq!(completed_before_retry, 1);
    assert_eq!(executor.stats().completed(), 2);
}

#[test]
fn close_recovery_health_debt_is_not_lost_on_failure() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, health_request(MaintenanceTaskPolicy::ordinary()))
        .expect("enqueue");
    let mut runner = RecordingRunner::failed();

    let outcome = executor
        .run_next(open, &mut runner)
        .expect("run")
        .expect("outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Failed);
    assert!(outcome.recovery_health().is_some());
}

#[test]
fn maintenance_executor_records_drain_fault_without_removing_pending_task() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
        )
        .expect("drain");

    let mut runner = RecordingRunner::completed();
    let mut fault = FailAt::new(MaintenanceFaultPoint::DuringDrain);
    let error = executor
        .drain_for_close_with_fault(closing, &mut runner, &mut fault)
        .expect_err("drain fault");

    assert_eq!(
        error,
        LifecycleError::MaintenanceFailed {
            reason: "injected maintenance fault",
        }
    );
    assert_eq!(executor.status().pending_tasks(), 1);
    assert_eq!(executor.status().active_task(), None);
    assert_eq!(executor.stats().failed(), 1);
    assert_eq!(executor.stats().drained(), 0);
    let failures = executor.recent_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(
        failures[0].task_kind(),
        MaintenanceTaskKind::HealthCollection
    );
    assert_eq!(
        failures[0].source_error_code(),
        Some("failed_precondition.lifecycle.maintenance")
    );
}

#[test]
fn active_task_failure_during_close_drain_records_kind_and_error_code() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    executor
        .enqueue(open, MaintenanceTaskRequest::flush(branch_id(0x63)))
        .expect("enqueue flush");
    executor
        .start_next_matching(open, |task| task.kind() == MaintenanceTaskKind::Flush)
        .expect("start ok")
        .expect("flush starts");

    let mut runner = FailsAfterCompletions::new(0);
    executor
        .drain_active_for_close(closing, &mut runner)
        .expect_err("active drain fails");

    assert_eq!(executor.stats().failed(), 1);
    let failures = executor.recent_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(
        failures[0].source_error_code(),
        Some("io.lifecycle.backend")
    );
}

#[test]
fn maintenance_fault_before_enqueue_leaves_queue_unchanged() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let mut fault = FailAt::new(MaintenanceFaultPoint::BeforeEnqueue);

    let error = executor
        .enqueue_with_fault(
            open,
            health_request(MaintenanceTaskPolicy::ordinary()),
            &mut fault,
        )
        .expect_err("before enqueue fault");

    assert_eq!(
        error,
        LifecycleError::MaintenanceFailed {
            reason: "injected maintenance fault",
        }
    );
    assert_eq!(executor.status().pending_tasks(), 0);
    assert_eq!(executor.stats(), LifecycleMaintenanceStats::default());
}

#[test]
fn maintenance_fault_after_enqueue_keeps_pending_task_observable() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let mut fault = FailAt::new(MaintenanceFaultPoint::AfterEnqueue);

    let error = executor
        .enqueue_with_fault(
            open,
            health_request(MaintenanceTaskPolicy::ordinary()),
            &mut fault,
        )
        .expect_err("after enqueue fault");

    assert_eq!(
        error,
        LifecycleError::MaintenanceFailed {
            reason: "injected maintenance fault",
        }
    );
    assert_eq!(executor.status().pending_tasks(), 1);
    assert_eq!(executor.stats().enqueued(), 1);
    assert_eq!(executor.stats().max_pending_tasks(), 1);
}

#[test]
fn maintenance_fault_hooks_fire_in_deterministic_order() {
    let open = open_state();
    let closing = closing_state();
    let mut executor = LifecycleMaintenanceExecutor::new(2).expect("executor");
    let mut hook = RecordingFaultHook::default();
    executor
        .enqueue_with_fault(
            open,
            health_request(MaintenanceTaskPolicy::drain_before_close()),
            &mut hook,
        )
        .expect("enqueue");
    let mut runner = RecordingRunner::completed();
    executor
        .drain_for_close_with_fault(closing, &mut runner, &mut hook)
        .expect("drain");

    assert_eq!(
        hook.points,
        vec![
            MaintenanceFaultPoint::BeforeEnqueue,
            MaintenanceFaultPoint::AfterEnqueue,
            MaintenanceFaultPoint::DuringDrain,
            MaintenanceFaultPoint::AtTaskStart,
            MaintenanceFaultPoint::AfterTaskRun,
        ]
    );
    assert_eq!(hook.task_ids, vec![1, 1, 1, 1]);
}

#[test]
fn maintenance_ready_policy_tracks_recovery_health_class() {
    assert!(maintenance_ready_for_recovery_health(
        &RecoveryHealth::Healthy
    ));
    let telemetry = RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![RecoveryFault::new(RecoveryFaultKind::IoFailure, "telemetry").expect("fault")],
    )
    .expect("telemetry degraded health");
    assert!(maintenance_ready_for_recovery_health(&telemetry));

    let data_loss = RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingTableObject, "missing table")
                .expect("fault"),
        ],
    )
    .expect("data loss health");
    assert!(!maintenance_ready_for_recovery_health(&data_loss));
    let policy_downgrade = RecoveryHealth::degraded(
        RecoveryDegradationClass::PolicyDowngrade,
        vec![RecoveryFault::new(RecoveryFaultKind::IoFailure, "policy").expect("fault")],
    )
    .expect("policy degraded health");
    assert!(!maintenance_ready_for_recovery_health(&policy_downgrade));
    let failed = RecoveryHealth::failed(
        RecoveryFault::new(RecoveryFaultKind::IoFailure, "failed").expect("fault"),
    );
    assert!(!maintenance_ready_for_recovery_health(&failed));
    assert_eq!(
        MaintenanceOutcome::new(
            MaintenanceTaskKind::Repair,
            MaintenanceOutcomeStatus::Canceled,
        )
        .status(),
        MaintenanceOutcomeStatus::Canceled,
    );
}

struct FailsAfterCompletions {
    remaining_successes: usize,
}

impl FailsAfterCompletions {
    const fn new(remaining_successes: usize) -> Self {
        Self {
            remaining_successes,
        }
    }
}

impl MaintenanceTaskRunner for FailsAfterCompletions {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        if self.remaining_successes == 0 {
            return Err(LifecycleError::lower_layer_with(
                LifecycleLowerLayer::Backend,
                "maintenance runner failed",
                DrainFailureSource,
            ));
        }
        self.remaining_successes -= 1;
        Ok(MaintenanceOutcome::new(
            task.kind(),
            MaintenanceOutcomeStatus::Completed,
        ))
    }
}

#[derive(Debug)]
struct DrainFailureSource;

impl std::fmt::Display for DrainFailureSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("drain failure source")
    }
}

impl std::error::Error for DrainFailureSource {}

struct FailedOutcomeRunner;

impl MaintenanceTaskRunner for FailedOutcomeRunner {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        Ok(
            MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                .with_reason("scripted runner failure")
                .with_source_error(LifecycleError::lower_layer(
                    LifecycleLowerLayer::Backend,
                    "scripted backend failure",
                )),
        )
    }
}

#[test]
fn runner_error_records_the_failed_task_kind_and_error_code() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    executor
        .enqueue(open, MaintenanceTaskRequest::flush(branch_id(0x61)))
        .expect("enqueue flush");
    let mut runner = FailsAfterCompletions::new(0);
    assert!(executor.run_next(open, &mut runner).is_err());

    let status = executor.status();
    assert_eq!(status.stats().failed(), 1);
    let failures = executor.recent_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(failures[0].reason(), None);
    assert_eq!(
        failures[0].source_error_code(),
        Some("io.lifecycle.backend")
    );
}

#[test]
fn failed_outcome_records_kind_reason_and_source_error_code() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    executor
        .enqueue(open, MaintenanceTaskRequest::checkpoint())
        .expect("enqueue checkpoint");
    let mut runner = FailedOutcomeRunner;
    let outcome = executor
        .run_next(open, &mut runner)
        .expect("run reports the failed outcome")
        .expect("a task ran");
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Failed);

    let status = executor.status();
    assert_eq!(status.stats().failed(), 1);
    let failures = executor.recent_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].task_kind(), MaintenanceTaskKind::Checkpoint);
    assert_eq!(failures[0].reason(), Some("scripted runner failure"));
    assert_eq!(
        failures[0].source_error_code(),
        Some("io.lifecycle.backend")
    );
}

#[test]
fn failure_records_are_bounded_and_keep_the_newest() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    let ordered_kinds = [
        MaintenanceTaskKind::Flush,
        MaintenanceTaskKind::Checkpoint,
        MaintenanceTaskKind::WalTruncation,
        MaintenanceTaskKind::Quarantine,
        MaintenanceTaskKind::Compaction,
    ];
    executor
        .enqueue(open, MaintenanceTaskRequest::flush(branch_id(0x62)))
        .expect("enqueue flush");
    executor
        .enqueue(open, MaintenanceTaskRequest::checkpoint())
        .expect("enqueue checkpoint");
    executor
        .enqueue(open, MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue wal truncation");
    executor
        .enqueue(open, MaintenanceTaskRequest::quarantine())
        .expect("enqueue quarantine");
    executor
        .enqueue(open, MaintenanceTaskRequest::compaction(branch_id(0x62), 0))
        .expect("enqueue compaction");
    let mut runner = FailsAfterCompletions::new(0);
    for kind in ordered_kinds {
        assert!(executor
            .run_next_matching(open, &mut runner, |task| task.kind() == kind)
            .is_err());
    }

    let status = executor.status();
    assert_eq!(status.stats().failed(), ordered_kinds.len());
    let failures = executor.recent_failures();
    assert_eq!(failures.len(), 4, "failure records are bounded");
    let recorded_kinds: Vec<_> = failures.iter().map(|record| record.task_kind()).collect();
    assert_eq!(
        recorded_kinds,
        vec![
            MaintenanceTaskKind::Checkpoint,
            MaintenanceTaskKind::WalTruncation,
            MaintenanceTaskKind::Quarantine,
            MaintenanceTaskKind::Compaction,
        ],
        "oldest record is evicted; survivors keep failure order"
    );
}

#[test]
fn rewrite_lane_cap_admits_up_to_the_cap() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    executor.set_rewrite_lane_cap(2);
    let branch = branch_id(0x51);
    // Non-conflicting levels; per-(branch, level) coalescing keeps them distinct tasks.
    for level in [0_u8, 2, 4] {
        executor
            .enqueue(open, MaintenanceTaskRequest::compaction(branch, level))
            .expect("enqueue");
    }
    let is_compaction = |task: &MaintenanceTask| task.kind() == MaintenanceTaskKind::Compaction;
    let first = executor
        .start_next_matching(open, is_compaction)
        .expect("start ok")
        .expect("first rewrite starts");
    let second = executor
        .start_next_matching(open, is_compaction)
        .expect("start ok")
        .expect("second rewrite starts under cap 2");
    let third = executor
        .start_next_matching(open, is_compaction)
        .expect("start ok");
    assert_ne!(first.id(), second.id());
    assert!(third.is_none(), "cap 2 blocks a third concurrent rewrite");
    assert_eq!(executor.status().active_tasks(), 2);
}

#[test]
fn default_rewrite_lane_cap_serializes_rewrites() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    // Default cap is 1 — legacy single-lane behavior, unchanged.
    let branch = branch_id(0x52);
    for level in [0_u8, 2] {
        executor
            .enqueue(open, MaintenanceTaskRequest::compaction(branch, level))
            .expect("enqueue");
    }
    let is_compaction = |task: &MaintenanceTask| task.kind() == MaintenanceTaskKind::Compaction;
    executor
        .start_next_matching(open, is_compaction)
        .expect("start ok")
        .expect("first rewrite starts");
    let second = executor
        .start_next_matching(open, is_compaction)
        .expect("start ok");
    assert!(
        second.is_none(),
        "default cap 1 keeps the Rewrite lane single"
    );
    assert_eq!(executor.status().active_tasks(), 1);
}

#[test]
fn rewrite_conflict_is_same_branch_adjacent_level() {
    let open = open_state();
    let mut executor = LifecycleMaintenanceExecutor::new(8).expect("executor");
    executor.set_rewrite_lane_cap(4);
    let branch = branch_id(0x60);
    let other = branch_id(0x61);
    executor
        .enqueue(open, MaintenanceTaskRequest::compaction(branch, 3))
        .expect("enqueue active");
    let active = executor
        .start_next_matching(open, |task| task.kind() == MaintenanceTaskKind::Compaction)
        .expect("start ok")
        .expect("active rewrite");
    assert_eq!(
        active.scope(),
        MaintenanceTaskScope::TableLevel {
            branch_id: branch,
            level: 3,
        }
    );

    for request in [
        MaintenanceTaskRequest::compaction(branch, 4),
        MaintenanceTaskRequest::compaction(branch, 5),
        MaintenanceTaskRequest::compaction(other, 3),
        MaintenanceTaskRequest::materialization_layer(branch, 0),
    ] {
        executor.enqueue(open, request).expect("enqueue candidate");
    }
    let peek_scope = |scope: MaintenanceTaskScope| {
        executor
            .next_matching_task(move |task| task.scope() == scope)
            .expect("queued candidate")
    };
    let adjacent = peek_scope(MaintenanceTaskScope::TableLevel {
        branch_id: branch,
        level: 4,
    });
    let non_adjacent = peek_scope(MaintenanceTaskScope::TableLevel {
        branch_id: branch,
        level: 5,
    });
    let cross_branch = peek_scope(MaintenanceTaskScope::TableLevel {
        branch_id: other,
        level: 3,
    });
    let materialization = executor
        .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Materialization)
        .expect("queued materialization");

    assert!(
        executor.rewrite_conflicts_with_active(adjacent),
        "same branch, |3-4|=1 conflicts"
    );
    assert!(
        !executor.rewrite_conflicts_with_active(non_adjacent),
        "same branch, |3-5|=2 does not conflict"
    );
    assert!(
        !executor.rewrite_conflicts_with_active(cross_branch),
        "different branch never conflicts"
    );
    assert!(
        executor.rewrite_conflicts_with_active(materialization),
        "materialization conflicts with any same-branch rewrite"
    );
}
