use super::*;

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn deterministic_inline_uses_background_drive_path_without_worker_threads() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        ),
    )
    .expect("deterministic inline cache open")
    .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();

    runtime
        .commit(&background_put_batch(
            b"deterministic-inline-background-drive",
            b"value".to_vec(),
        ))
        .expect("seed active row");
    runtime
        .rotate_default_branch_for_test()
        .expect("rotate active table");
    runtime
        .enqueue_lifecycle_maintenance_for_test(crate::lifecycle::MaintenanceTaskRequest::flush(
            branch,
        ))
        .expect("enqueue flush through background drive path");

    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.background_worker_count(), 0);
    assert!(status.background_tasks_completed() >= 1);

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_runtimes_created(), 1);
    assert_eq!(perf.lifecycle_background_runtime_workers_created(), 0);
    assert!(perf.lifecycle_background_drain_rounds() >= 1);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
}

#[cfg(feature = "perf-trace")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct InlineReplayFacts {
    maintenance_task_order: Vec<&'static str>,
    queue_trajectory: Vec<usize>,
    pending_tasks: usize,
    background_worker_count: usize,
    background_tasks_completed: u64,
    owned_l0_tables: usize,
    owned_l1_tables: usize,
    visible_value: Vec<u8>,
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn inline_executor_replays_fixed_background_scenario_deterministically() {
    let first = run_inline_replay_scenario();
    let second = run_inline_replay_scenario();

    assert_eq!(first, second);
    assert_eq!(first.pending_tasks, 0);
    assert_eq!(first.background_worker_count, 0);
    assert!(first.background_tasks_completed >= 1);
    assert_eq!(first.maintenance_task_order, vec!["flush", "compaction"]);
    assert_eq!(first.owned_l0_tables, 0);
    assert_eq!(first.owned_l1_tables, 1);
}

#[cfg(feature = "perf-trace")]
fn run_inline_replay_scenario() -> InlineReplayFacts {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        ),
    )
    .expect("inline cache open")
    .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();
    let mut queue_trajectory = vec![runtime
        .maintenance_status()
        .expect("initial maintenance status")
        .pending_tasks()];
    let mut maintenance_task_order = Vec::new();

    runtime
        .commit(&background_put_batch(
            b"inline-replay-background-flush",
            b"value".to_vec(),
        ))
        .expect("seed background flush row");
    runtime
        .rotate_default_branch_for_test()
        .expect("rotate active table before background flush");
    runtime
        .enqueue_lifecycle_maintenance_for_test(crate::lifecycle::MaintenanceTaskRequest::flush(
            branch,
        ))
        .expect("enqueue flush through background drive path");
    maintenance_task_order.push("flush");
    queue_trajectory.push(
        runtime
            .maintenance_status()
            .expect("post-flush maintenance status")
            .pending_tasks(),
    );

    for index in 0..2 {
        let key = format!("inline-replay-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(
                key.as_bytes(),
                u64::try_from(index + 1).expect("index fits"),
            ))
            .expect("seed raw active row");
        runtime
            .flush_default_branch_for_test()
            .expect("flush raw row into L0 table");
        queue_trajectory.push(
            runtime
                .maintenance_status()
                .expect("post-flush maintenance status")
                .pending_tasks(),
        );
    }
    runtime
        .enqueue_lifecycle_maintenance_for_test(
            crate::lifecycle::MaintenanceTaskRequest::compaction(branch, 0),
        )
        .expect("enqueue compaction through background drive path");
    maintenance_task_order.push("compaction");
    queue_trajectory.push(
        runtime
            .maintenance_status()
            .expect("post-background maintenance status")
            .pending_tasks(),
    );

    let status = runtime.maintenance_status().expect("maintenance status");
    let layout = runtime
        .branch_source_layout_for_test(branch)
        .expect("source layout");
    let read = runtime
        .read_point(&PointReadRequest::new(
            branch,
            background_space(),
            key(b"inline-replay-1"),
            ReadBound::Latest,
        ))
        .expect("read compacted inline replay row");
    let visible_value = read
        .row()
        .expect("visible inline replay row")
        .value()
        .expect("visible inline replay value")
        .as_bytes()
        .to_vec();
    InlineReplayFacts {
        maintenance_task_order,
        queue_trajectory,
        pending_tasks: status.pending_tasks(),
        background_worker_count: status.background_worker_count(),
        background_tasks_completed: status.background_tasks_completed(),
        owned_l0_tables: layout.owned_l0_tables(),
        owned_l1_tables: background_owned_table_count_at(&layout, 1),
        visible_value,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BackgroundCompactionParityFacts {
    completed_tasks: Vec<&'static str>,
    pending_tasks: usize,
    owned_l0_tables: usize,
    owned_l1_tables: usize,
    visible_value: Vec<u8>,
}

#[test]
fn threaded_and_inline_background_executors_converge_on_compaction_shape() {
    let threaded = run_background_compaction_parity_scenario(
        StorageMaintenanceSchedulingPolicy::Background,
        b"threaded-inline-parity-threaded",
    );
    let inline = run_background_compaction_parity_scenario(
        StorageMaintenanceSchedulingPolicy::DeterministicInline,
        b"threaded-inline-parity-inline",
    );

    assert_eq!(threaded, inline);
}

fn run_background_compaction_parity_scenario(
    policy: StorageMaintenanceSchedulingPolicy,
    key_prefix: &[u8],
) -> BackgroundCompactionParityFacts {
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(policy),
    )
    .expect("cache open")
    .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();

    for index in 0..2 {
        let mut key_bytes = key_prefix.to_vec();
        key_bytes.extend_from_slice(format!("-{index}").as_bytes());
        runtime
            .append_raw_row_for_test(background_raw_row(
                &key_bytes,
                u64::try_from(index + 1).expect("index fits"),
            ))
            .expect("seed raw active row");
        runtime
            .flush_default_branch_for_test()
            .expect("flush raw row into L0 table");
    }
    runtime
        .enqueue_lifecycle_maintenance_for_test(
            crate::lifecycle::MaintenanceTaskRequest::compaction(branch, 0),
        )
        .expect("enqueue compaction through background drive path");
    runtime.wait_background_idle_for_test();

    let status = runtime.maintenance_status().expect("maintenance status");
    let layout = runtime
        .branch_source_layout_for_test(branch)
        .expect("source layout");
    let mut read_key = key_prefix.to_vec();
    read_key.extend_from_slice(b"-1");
    let read = runtime
        .read_point(&PointReadRequest::new(
            branch,
            background_space(),
            key(&read_key),
            ReadBound::Latest,
        ))
        .expect("read compacted parity row");
    let visible_value = read
        .row()
        .expect("visible parity row")
        .value()
        .expect("visible parity value")
        .as_bytes()
        .to_vec();

    BackgroundCompactionParityFacts {
        completed_tasks: vec!["compaction"],
        pending_tasks: status.pending_tasks(),
        owned_l0_tables: layout.owned_l0_tables(),
        owned_l1_tables: background_owned_table_count_at(&layout, 1),
        visible_value,
    }
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn deterministic_inline_urgent_pressure_with_progress_does_not_sleep() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache()
            .with_storage_budget_for_test(
                crate::lifecycle::StorageRuntimeBudget::low_memory_test_profile(),
            )
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::DeterministicInline,
            ),
    )
    .expect("low-memory deterministic-inline cache open")
    .into_runtime();

    runtime
        .commit(&background_put_batch(
            b"inline-urgent-clock-seed",
            b"value".to_vec(),
        ))
        .expect("seed active row");
    runtime
        .rotate_default_branch_for_test()
        .expect("create frozen urgent pressure");
    let before = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");
    runtime
        .commit(&background_put_batch(
            b"inline-urgent-clock-followup",
            b"value".to_vec(),
        ))
        .expect("urgent commit should be accepted after background progress");
    let after = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(
        after.saturating_duration_since(before),
        std::time::Duration::ZERO,
        "urgent admission must not sleep the writer; background pressure is paced by the blocking wait-loop, not a per-commit slowdown"
    );
    assert_eq!(perf.lifecycle_inline_maintenance_attempts(), 0);
    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.background_worker_count(), 0);
    assert!(status.background_tasks_completed() >= 1);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn deterministic_inline_block_pressure_wait_uses_manual_clock_executor() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        ),
    )
    .expect("deterministic-inline cache open")
    .into_runtime();
    assert!(
        runtime.set_background_block_wait_for_test(
            std::time::Duration::from_millis(25),
            std::time::Duration::from_millis(250),
            1,
        ),
        "deterministic-inline background runtime should expose test block wait limits"
    );

    for index in 0..36 {
        let key = format!("inline-block-clock-seed-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(key.as_bytes(), 0))
            .expect("seed raw active row before rotation");
        runtime
            .rotate_default_branch_for_test()
            .expect("create level-zero table");
    }
    let before = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");
    runtime
        .commit(&background_put_batch(
            b"inline-block-clock-followup",
            b"value".to_vec(),
        ))
        .expect("block pressure should wait for inline background progress and retry");
    let after = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_write_admission_wait_attempts() >= 1);
    assert_eq!(perf.lifecycle_write_admission_wait_timeouts(), 0);
    assert!(perf.lifecycle_write_admission_block_wait_ns() > 0);
    assert!(after > before);
    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.background_worker_count(), 0);
    assert!(status.background_tasks_completed() >= 1);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn deterministic_inline_block_pressure_deadline_uses_manual_clock_without_progress() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        ),
    )
    .expect("deterministic-inline cache open")
    .into_runtime();
    assert!(
        runtime.set_background_drain_limits_for_test(0, std::time::Duration::from_millis(25)),
        "deterministic-inline background runtime should expose test drain limits"
    );
    assert!(
        runtime.set_background_block_wait_for_test(
            std::time::Duration::from_millis(25),
            std::time::Duration::from_millis(250),
            1,
        ),
        "deterministic-inline background runtime should expose test block wait limits"
    );

    for index in 0..36 {
        let key = format!("inline-block-deadline-seed-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(key.as_bytes(), 0))
            .expect("seed raw active row before rotation");
        runtime
            .rotate_default_branch_for_test()
            .expect("create level-zero table");
    }
    let before = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");
    let error = runtime
        .commit(&background_put_batch(
            b"inline-block-deadline-followup",
            b"value".to_vec(),
        ))
        .expect_err("manual-clock block wait should hit deterministic deadline");
    let after = runtime
        .background_now_for_test()
        .expect("inline background runtime exposes manual clock");

    assert!(matches!(
        error,
        StorageApiError::StoragePressure {
            severity: CommitAdmissionPressureSeverity::Blocking,
            retryable: true,
            ..
        }
    ));
    assert!(
        after.saturating_duration_since(before) >= std::time::Duration::from_millis(250),
        "manual clock should advance to the block wait deadline"
    );
    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_write_admission_wait_attempts() >= 1);
    assert_eq!(perf.lifecycle_write_admission_wait_timeouts(), 1);
    assert!(perf.lifecycle_write_admission_block_wait_ns() > 0);
    let status = runtime.maintenance_status().expect("maintenance status");
    assert!(status.pending_tasks() >= 1);
    assert_eq!(status.background_worker_count(), 0);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn deterministic_inline_manual_clock_runtime_limit_stops_and_resumes_drain_round() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        ),
    )
    .expect("deterministic-inline cache open")
    .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();
    assert!(
        runtime.set_background_drain_limits_for_test(usize::MAX, std::time::Duration::ZERO),
        "deterministic-inline background runtime should expose test drain limits"
    );

    runtime
        .commit(&background_put_batch(
            b"inline-runtime-limit-seed",
            b"value".to_vec(),
        ))
        .expect("seed active row");
    runtime
        .rotate_default_branch_for_test()
        .expect("create frozen table");
    runtime
        .enqueue_lifecycle_maintenance_for_test(crate::lifecycle::MaintenanceTaskRequest::flush(
            branch,
        ))
        .expect("enqueue flush with zero runtime budget");
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .pending_tasks(),
        1
    );

    assert!(
        runtime.set_background_drain_limits_for_test(usize::MAX, std::time::Duration::from_secs(1)),
        "deterministic-inline background runtime should expose test drain limits"
    );
    runtime.submit_stale_background_wake_for_test();
    runtime.wait_background_idle_for_test();
    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.background_worker_count(), 0);
    assert!(status.background_tasks_completed() >= 2);

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_background_drain_rounds() >= 2);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn disabled_maintenance_policy_skips_api_post_commit_enqueue_and_worker_wake() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let runtime = StorageRuntime::open(
        StorageOpenOptions::cache()
            .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::Disabled),
    )
    .expect("disabled cache open")
    .into_runtime();
    let batch = CommitBatch::new(
        StorageRuntime::default_branch_id_for_test(),
        vec![CommitMutation::Put {
            storage_space: StorageSpaceId::new(vec![0x20]).expect("engine storage space"),
            key: key(b"disabled-maintenance"),
            value: StorageValue::new(b"value".to_vec()),
            ttl: None,
        }],
        CommitOptions::default(),
    )
    .expect("valid commit batch");

    runtime.commit(&batch).expect("disabled commit");

    let queue = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(queue.pending_tasks(), 0);
    assert_eq!(queue.background_worker_count(), 0);

    // Cache no longer evaluates post-commit source-table maintenance at all,
    // so even the disabled-policy counters stay at zero.
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_post_commit_maintenance_evaluations(), 0);
    assert_eq!(perf.lifecycle_post_commit_maintenance_disabled(), 0);
    assert_eq!(perf.lifecycle_post_commit_maintenance_tasks_enqueued(), 0);
    assert_eq!(perf.lifecycle_background_runtimes_created(), 0);
}

#[test]
fn maintenance_status_reports_queue_state_without_background_worker_in_manual_mode() {
    let runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
        ),
    )
    .expect("manual maintenance cache open")
    .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();

    let queue = runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch),
        ))
        .expect("enqueue maintenance");

    assert_eq!(queue.pending_tasks(), 1);
    assert_eq!(queue.enqueued(), 1);
    assert_eq!(queue.background_worker_count(), 0);
    assert_eq!(queue.background_queue_depth(), 0);
    assert_eq!(queue.background_active_tasks(), 0);

    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 1);
    assert_eq!(status.background_worker_count(), 0);
}

#[test]
fn background_manual_mode_run_next_maintenance_drains_explicit_work_without_worker() {
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
        ),
    )
    .expect("manual maintenance cache open")
    .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();

    runtime
        .commit(&background_put_batch(b"manual-run-next", b"value".to_vec()))
        .expect("seed active table data");
    runtime
        .rotate_default_branch_for_test()
        .expect("rotate active table into frozen source");

    let queue = runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch),
        ))
        .expect("enqueue manual flush maintenance");
    assert_eq!(queue.pending_tasks(), 1);
    assert_eq!(queue.background_worker_count(), 0);

    let outcome = runtime
        .run_next_maintenance()
        .expect("manual run-next maintenance")
        .expect("queued flush maintenance outcome");
    assert_eq!(outcome.task(), MaintenanceTask::Flush);
    assert_eq!(outcome.scope(), MaintenanceScope::Branch(branch));
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);

    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.background_worker_count(), 0);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_explicit_enqueue_wakes_and_drains_queue() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime =
        StorageRuntime::open(StorageOpenOptions::cache().with_background_worker_count(1))
            .expect("single-worker background cache open")
            .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();
    runtime
        .commit(&background_put_batch(
            b"background-flush",
            b"value".to_vec(),
        ))
        .expect("seed active row");
    runtime
        .rotate_default_branch_for_test()
        .expect("rotate active table into frozen source");

    let queue = runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch),
        ))
        .expect("enqueue background maintenance");
    assert_eq!(queue.pending_tasks(), 1);

    runtime.wait_background_idle_for_test();

    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.background_worker_count(), 1);

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_background_wake_submitted() >= 1);
    assert!(perf.lifecycle_background_drain_rounds() >= 1);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
    assert!(perf.lifecycle_background_task_snapshot_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_unlocked_build_ns() > 0);
    assert!(perf.lifecycle_background_task_publish_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_total_ns() > 0);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_compaction_enqueue_wakes_and_drains_table_rewrite() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime =
        StorageRuntime::open(StorageOpenOptions::cache().with_background_worker_count(1))
            .expect("background cache open")
            .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();

    for index in 0..2 {
        let key = format!("background-compact-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(
                key.as_bytes(),
                u64::try_from(index + 1).expect("index fits"),
            ))
            .expect("seed raw active row");
        runtime
            .flush_default_branch_for_test()
            .expect("flush raw row into L0 table");
    }
    let before = runtime
        .branch_source_layout_for_test(branch)
        .expect("pre-compaction source layout");
    assert_eq!(before.owned_l0_tables(), 2);

    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Compact,
            MaintenanceScope::Branch(branch),
        ))
        .expect("enqueue background compaction");
    runtime.wait_background_idle_for_test();

    let after = runtime
        .branch_source_layout_for_test(branch)
        .expect("post-compaction source layout");
    assert_eq!(after.owned_l0_tables(), 0);
    assert_eq!(background_owned_table_count_at(&after, 1), 1);
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .pending_tasks(),
        0
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_background_wake_submitted() >= 1);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
    assert!(perf.lifecycle_background_task_snapshot_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_unlocked_build_ns() > 0);
    assert!(perf.lifecycle_background_task_publish_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_total_ns() > 0);
    assert!(perf.lifecycle_compaction_operations_completed() >= 1);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_materialization_enqueue_wakes_and_drains_table_rewrite() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime =
        StorageRuntime::open(StorageOpenOptions::cache().with_background_worker_count(1))
            .expect("single-worker background cache open")
            .into_runtime();
    let parent = StorageRuntime::default_branch_id_for_test();
    let child = branch_id(0x91);

    runtime
        .append_raw_row_for_test(background_raw_row(b"background-materialize", 1))
        .expect("seed parent row");
    runtime
        .flush_default_branch_for_test()
        .expect("flush parent row into an inheritable table");
    runtime
        .fork_default_branch_for_test(child)
        .expect("fork child from parent");

    let before = runtime
        .branch_source_layout_for_test(child)
        .expect("pre-materialization child layout");
    assert_eq!(before.inherited_layers(), 1);
    assert_eq!(before.inherited_total_tables(), 1);

    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Materialize,
            MaintenanceScope::Branch(child),
        ))
        .expect("enqueue background materialization");
    runtime.wait_background_idle_for_test();

    let after = runtime
        .branch_source_layout_for_test(child)
        .expect("post-materialization child layout");
    assert_eq!(after.inherited_total_tables(), 0);
    assert_eq!(after.owned_total_tables(), 1);
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .pending_tasks(),
        0
    );
    let read = runtime
        .read_point(&PointReadRequest::new(
            child,
            background_space(),
            key(b"background-materialize"),
            ReadBound::Latest,
        ))
        .expect("read materialized child row");
    assert_eq!(
        read.row()
            .expect("materialized row")
            .value()
            .expect("value")
            .as_bytes(),
        b"value"
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_background_wake_submitted() >= 1);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
    assert!(perf.lifecycle_background_task_snapshot_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_unlocked_build_ns() > 0);
    assert!(perf.lifecycle_background_task_publish_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_total_ns() > 0);
    assert!(perf.branch_materialization_output_tables() >= 1);

    let parent_layout = runtime
        .branch_source_layout_for_test(parent)
        .expect("parent layout");
    assert_eq!(parent_layout.owned_total_tables(), 1);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_duplicate_enqueue_coalesces_wake_while_worker_busy() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    let _capture = crate::observability::perf_trace::begin_test_capture();
    let runtime = StorageRuntime::open(StorageOpenOptions::cache().with_background_worker_count(1))
        .expect("single-worker background cache open")
        .into_runtime();
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let observed_open = Arc::new(AtomicBool::new(false));
    assert!(
        runtime.submit_runtime_state_background_probe_for_test(
            Arc::clone(&ready),
            Arc::clone(&release),
            Arc::clone(&observed_open),
        ),
        "background runtime should accept probe"
    );
    ready.wait();

    let branch = StorageRuntime::default_branch_id_for_test();
    for _ in 0..2 {
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(branch),
            ))
            .expect("enqueue duplicate background maintenance");
    }

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_background_wake_submitted() >= 1);
    assert!(perf.lifecycle_background_wake_coalesced() >= 1);

    release.wait();
    runtime.wait_background_idle_for_test();
    assert!(observed_open.load(Ordering::Acquire));
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .pending_tasks(),
        0
    );
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_foreground_wait_counter_records_short_runtime_lock_waits() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    let _capture = crate::observability::perf_trace::begin_test_capture();
    let runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let observed_open = Arc::new(AtomicBool::new(false));
    assert!(
        runtime.submit_runtime_state_background_probe_for_test(
            Arc::clone(&ready),
            Arc::clone(&release),
            Arc::clone(&observed_open),
        ),
        "background runtime should accept probe"
    );
    ready.wait();

    crate::observability::perf_trace::reset();
    let commit_start = std::time::Instant::now();
    runtime
        .commit(&background_put_batch(
            b"foreground-wait-counter",
            b"value".to_vec(),
        ))
        .expect("foreground commit while worker is in unlocked wait phase");
    assert!(
        commit_start.elapsed() < std::time::Duration::from_secs(1),
        "foreground commit must not wait for the background worker's unlocked phase"
    );
    let perf = crate::observability::perf_trace::snapshot();
    assert!(
        perf.lifecycle_foreground_wait_background_lock_ns() > 0,
        "foreground runtime lock waits must be measured"
    );

    release.wait();
    runtime.wait_background_idle_for_test();
    assert!(observed_open.load(Ordering::Acquire));
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_stale_wake_records_noop_without_pending_task() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();

    runtime.submit_stale_background_wake_for_test();
    runtime.wait_background_idle_for_test();

    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_stale_wake_noop(), 1);
    assert_eq!(perf.lifecycle_background_drain_rounds(), 1);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_wake_after_shutdown_records_rejected_without_running_task() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();

    runtime.shutdown_background_for_test();
    runtime.submit_stale_background_wake_for_test();

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_wake_rejected(), 1);
    assert_eq!(
        perf.lifecycle_background_submit_after_shutdown_rejected(),
        1
    );
    assert_eq!(perf.lifecycle_background_shutdowns(), 1);
    assert_eq!(
        perf.lifecycle_background_shutdown_joined_workers(),
        default_background_worker_count() as u64
    );
    assert_eq!(perf.lifecycle_background_stale_wake_noop(), 0);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_close_summary_includes_shutdown_stats() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();

    let close = runtime.close().expect("close background runtime");

    assert_eq!(close.state(), StorageRuntimeState::Closed);
    assert_eq!(
        close.background_worker_count(),
        default_background_worker_count()
    );
    assert_eq!(close.background_queue_depth(), 0);
    assert_eq!(close.background_active_tasks(), 0);
    assert_eq!(close.background_tasks_completed(), 0);
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_shutdowns(), 1);
    assert_eq!(
        perf.lifecycle_background_shutdown_joined_workers(),
        default_background_worker_count() as u64
    );
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_repeated_close_preserves_prior_background_facts() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();

    let first = runtime.close().expect("first close");
    let second = runtime.close().expect("second close");

    assert!(!first.idempotent());
    assert!(second.idempotent());
    assert_eq!(second.state(), first.state());
    assert_eq!(
        second.background_worker_count(),
        first.background_worker_count()
    );
    assert_eq!(
        second.background_queue_depth(),
        first.background_queue_depth()
    );
    assert_eq!(
        second.background_active_tasks(),
        first.background_active_tasks()
    );
    assert_eq!(
        second.background_tasks_completed(),
        first.background_tasks_completed()
    );
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_shutdowns(), 1);
    assert_eq!(
        perf.lifecycle_background_shutdown_joined_workers(),
        default_background_worker_count() as u64
    );
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_close_with_active_task_obeys_shutdown_deadline() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let observed_open = Arc::new(AtomicBool::new(false));
    assert!(
        runtime.submit_runtime_state_background_probe_for_test(
            Arc::clone(&ready),
            Arc::clone(&release),
            Arc::clone(&observed_open),
        ),
        "background runtime should accept probe"
    );
    ready.wait();

    let start = std::time::Instant::now();
    let close = runtime
        .close_with_options(
            StorageCloseOptions::graceful()
                .with_background_shutdown_timeout(std::time::Duration::from_millis(1)),
        )
        .expect("close detaches stuck background worker after deadline");
    assert!(
        start.elapsed() < std::time::Duration::from_secs(1),
        "close must not hang behind an active background worker"
    );
    assert_eq!(close.state(), StorageRuntimeState::Closed);
    assert_eq!(
        close.background_worker_count(),
        default_background_worker_count()
    );
    assert_eq!(close.background_active_tasks(), 1);
    release.wait();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while crate::observability::perf_trace::snapshot()
        .lifecycle_background_shutdown_executor_tasks_completed()
        == 0
    {
        assert!(
            std::time::Instant::now() < deadline,
            "detached background task completion was not counted after shutdown"
        );
        std::thread::yield_now();
    }

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_shutdowns(), 1);
    assert_eq!(perf.lifecycle_background_shutdown_joined_workers(), 0);
    assert_eq!(
        perf.lifecycle_background_shutdown_detached_workers(),
        default_background_worker_count() as u64
    );
    assert_eq!(
        perf.lifecycle_background_shutdown_executor_tasks_completed(),
        1
    );
    assert!(
        !observed_open.load(Ordering::Acquire),
        "detached probe must observe the lifecycle runtime closed after timeout close returns"
    );
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_close_succeeds_after_pre_shutdown_background_panic() {
    use std::sync::{Arc, Barrier};

    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));

    assert!(
        runtime.submit_panicking_background_task_for_test(Arc::clone(&ready), Arc::clone(&release)),
        "background runtime should accept panic probe"
    );
    ready.wait();
    release.wait();
    runtime.wait_background_idle_for_test();

    let before_close = crate::observability::perf_trace::snapshot();
    assert_eq!(before_close.lifecycle_background_worker_panics(), 1);
    assert_eq!(
        before_close.lifecycle_background_shutdown_executor_tasks_completed(),
        0
    );

    let close = runtime
        .close()
        .expect("ordinary pre-shutdown background panic must not poison close");

    assert_eq!(close.state(), StorageRuntimeState::Closed);
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_worker_panics(), 1);
    assert_eq!(perf.lifecycle_background_shutdowns(), 1);
    assert_eq!(
        perf.lifecycle_background_shutdown_joined_workers(),
        default_background_worker_count() as u64
    );
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_close_reports_shutdown_panic_once_then_retry_closes() {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();
    let shutdown_requested = runtime
        .background_shutdown_requested_flag_for_test()
        .expect("background runtime exposes shutdown flag in tests");
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));

    assert!(
        runtime.submit_panicking_background_task_for_test(Arc::clone(&ready), Arc::clone(&release)),
        "background runtime should accept panic probe"
    );
    ready.wait();

    let release_thread = {
        let shutdown_requested = Arc::clone(&shutdown_requested);
        let release = Arc::clone(&release);
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(1);
            while !shutdown_requested.load(Ordering::Acquire) {
                assert!(
                    Instant::now() < deadline,
                    "close did not request background shutdown"
                );
                std::thread::yield_now();
            }
            release.wait();
        })
    };

    let error = runtime
        .close()
        .expect_err("shutdown-time background panic must fail the first close");
    release_thread.join().expect("release thread completes");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(error.code(), "failed_precondition.storage_api.maintenance");
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_worker_panics(), 1);
    assert_eq!(perf.lifecycle_background_shutdowns(), 1);
    let completed_after_shutdown = perf.lifecycle_background_shutdown_executor_tasks_completed();
    assert!(
        completed_after_shutdown >= 1,
        "shutdown must count at least the completed panic probe, got {completed_after_shutdown}"
    );

    let retry = runtime
        .close()
        .expect("retry close after reported shutdown panic");
    assert_eq!(retry.state(), StorageRuntimeState::Closed);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn dropping_open_background_runtime_requests_shutdown() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    {
        let runtime = StorageRuntime::open_cache()
            .expect("background cache open")
            .into_runtime();
        assert!(runtime
            .background_shutdown_requested_flag_for_test()
            .is_some());
    }

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_shutdowns(), 1);
    assert_eq!(perf.lifecycle_background_shutdown_joined_workers(), 0);
}

#[cfg(feature = "perf-trace")]
#[test]
fn background_close_cancels_ordinary_lifecycle_tasks_and_records_counter() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();

    let queue = runtime
        .enqueue_lifecycle_maintenance_for_test(
            crate::lifecycle::MaintenanceTaskRequest::health_collection(),
        )
        .expect("enqueue ordinary health-collection task");
    assert_eq!(queue.pending_tasks(), 1);

    let close = runtime.close().expect("close cancels ordinary task");

    assert_eq!(close.state(), StorageRuntimeState::Closed);
    assert!(close.maintenance_drained());
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_shutdown_canceled_tasks(), 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn background_close_drains_required_lifecycle_tasks_and_records_counter() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();

    let task = crate::lifecycle::MaintenanceTaskRequest::new(
        crate::lifecycle::MaintenanceTaskKind::HealthCollection,
        crate::lifecycle::MaintenanceTaskPriority::Normal,
        crate::lifecycle::MaintenanceTaskScope::Global,
        crate::lifecycle::MaintenanceTaskPolicy::drain_before_close(),
    )
    .expect("drain-required task request");
    let queue = runtime
        .enqueue_lifecycle_maintenance_for_test(task)
        .expect("enqueue drain-required health task");
    assert_eq!(queue.pending_tasks(), 1);

    let close = runtime
        .close()
        .expect("close drains required lifecycle task");

    assert_eq!(close.state(), StorageRuntimeState::Closed);
    assert!(close.maintenance_drained());
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_background_shutdown_drained_tasks(), 1);
    assert_eq!(perf.lifecycle_background_shutdown_canceled_tasks(), 0);
}

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
#[test]
fn background_wal_growth_checkpoint_wakes_and_drains() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(temp_dir_for_api_test(
        "background-wal-growth",
    )));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_growth_policy(StorageWalGrowthPolicy::thresholds(u64::MAX, usize::MAX, 1)),
        backend,
    )
    .expect("durable background open")
    .into_runtime();

    runtime
        .commit(&background_put_batch(b"wal-growth", b"value".to_vec()))
        .expect("commit below WAL growth threshold");
    let commit = runtime
        .commit(&background_put_batch(b"wal-growth-next", b"value".to_vec()))
        .expect("commit crossing WAL growth threshold");
    runtime.wait_background_idle_for_test();

    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    let report = runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("diagnostics");
    assert_eq!(report.wal_growth().state(), DiagnosticsFactState::Known);
    assert!(matches!(
        report
            .wal_growth()
            .last_status()
            .map(MaintenanceWalGrowthSummary::status),
        Some(
            MaintenanceWalGrowthStatus::MaintenanceEnqueued
                | MaintenanceWalGrowthStatus::MaintenanceCoalesced
        )
    ));
    assert_eq!(report.checkpoint().state(), DiagnosticsFactState::Known);
    assert!(
        report
            .checkpoint()
            .flush_watermark()
            .is_some_and(|watermark| watermark >= commit.commit_version()),
        "background flush watermark did not advance to the WAL-growth commit"
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_background_wake_submitted() >= 1);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
    assert!(perf.lifecycle_background_task_snapshot_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_unlocked_build_ns() > 0);
    assert!(perf.lifecycle_background_task_publish_lock_ns() > 0);
}

#[cfg(all(feature = "localfs", feature = "perf-trace"))]
#[test]
fn background_wal_truncation_runs_retention_scan_outside_runtime_lock() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime =
        StorageRuntime::open_local(temp_dir_for_api_test("background-wal-truncation-split"))
            .expect("durable background open")
            .into_runtime();

    runtime
        .commit(&background_put_batch(
            b"wal-truncation-seed",
            b"value".to_vec(),
        ))
        .expect("seed durable WAL row");
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("publish checkpoint retention proof");
    runtime
        .enqueue_lifecycle_maintenance_for_test(
            crate::lifecycle::MaintenanceTaskRequest::wal_truncation(),
        )
        .expect("enqueue internal WAL truncation");
    runtime.wait_background_idle_for_test();

    let status = runtime.maintenance_status().expect("maintenance status");
    assert_eq!(status.pending_tasks(), 0);

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_background_task_snapshot_lock_ns() > 0);
    assert!(perf.lifecycle_background_task_unlocked_build_ns() > 0);
    assert!(perf.lifecycle_background_task_publish_lock_ns() > 0);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn sustained_background_overload_paces_writer_via_block_wait() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open(
        StorageOpenOptions::cache()
            .with_storage_budget_for_test(
                crate::lifecycle::StorageRuntimeBudget::low_memory_test_profile(),
            )
            .with_background_worker_count(1)
            .with_background_max_tasks_per_wake(1)
            .with_background_max_runtime_per_wake(std::time::Duration::from_millis(1)),
    )
    .expect("single-worker low-memory background cache open")
    .into_runtime();

    runtime
        .commit(&background_put_batch(
            b"sustained-overload-seed",
            b"value".to_vec(),
        ))
        .expect("seed active row");
    runtime
        .rotate_default_branch_for_test()
        .expect("create initial pressure");

    for index in 0..96 {
        let key = format!("sustained-overload-commit-{index:04}");
        runtime
            .commit(&background_put_batch(key.as_bytes(), vec![0x61; 256]))
            .unwrap_or_else(|error| {
                panic!("sustained overload commit {index} failed permanently: {error}")
            });
    }

    runtime.wait_background_idle_for_test();

    let status = runtime
        .maintenance_status()
        .expect("sustained overload maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(
        status.failed(),
        0,
        "sustained overload failures: {status:?}"
    );
    assert_eq!(
        status.queue_full(),
        0,
        "sustained overload filled lifecycle queue: {status:?}"
    );
    let report = runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("sustained overload diagnostics");
    assert_ne!(
        report.pressure().severity(),
        DiagnosticsStoragePressureSeverity::BlockMutatingAdmission,
        "sustained overload left storage in blocking pressure"
    );

    let perf = crate::observability::perf_trace::snapshot();
    assert!(
        perf.lifecycle_write_admission_wait_attempts() > 0,
        "sustained overload did not enter block-wait relief"
    );
    assert_eq!(perf.lifecycle_write_admission_wait_timeouts(), 0);
    assert!(perf.lifecycle_background_tasks_completed() > 0);
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_block_pressure_waits_for_flush_progress_before_retry() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime = StorageRuntime::open_cache()
        .expect("background cache open")
        .into_runtime();

    for index in 0..4 {
        let key = format!("block-seed-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(key.as_bytes(), 0))
            .expect("seed raw active row before rotation");
        runtime
            .rotate_default_branch_for_test()
            .expect("create frozen table");
    }
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status before block")
            .pending_tasks(),
        0
    );

    runtime
        .commit(&background_put_batch(b"block-followup", b"value".to_vec()))
        .expect("block pressure should wait and retry after background flush");
    runtime.wait_background_idle_for_test();

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_write_admission_wait_attempts() >= 1);
    assert_eq!(perf.lifecycle_write_admission_wait_timeouts(), 0);
    assert!(perf.lifecycle_write_admission_block_wait_ns() > 0);
    assert!(perf.lifecycle_background_tasks_completed() >= 1);
    assert!(perf.lifecycle_pressure_clear_wakes() >= 1);
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .pending_tasks(),
        0
    );
}

#[cfg(feature = "perf-trace")]
#[ignore = "L8G: cache has no background/inline maintenance executor or source-shape admission; durable executor/admission coverage is owned by L8H"]
#[test]
fn background_block_pressure_wait_has_deadline_when_worker_is_busy() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    let _capture = crate::observability::perf_trace::begin_test_capture();
    let mut runtime =
        StorageRuntime::open(StorageOpenOptions::cache().with_background_worker_count(1))
            .expect("single-worker background cache open")
            .into_runtime();
    assert!(
        runtime.set_background_block_wait_for_test(
            Duration::from_millis(25),
            Duration::from_millis(250),
            1,
        ),
        "background runtime should expose test block wait limits"
    );

    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let observed_open = Arc::new(AtomicBool::new(false));
    assert!(
        runtime.submit_runtime_state_background_probe_for_test(
            Arc::clone(&ready),
            Arc::clone(&release),
            Arc::clone(&observed_open),
        ),
        "background runtime should accept probe"
    );
    ready.wait();

    for index in 0..36 {
        let key = format!("block-timeout-seed-{index}");
        runtime
            .append_raw_row_for_test(background_raw_row(key.as_bytes(), 0))
            .expect("seed raw active row before rotation");
        runtime
            .rotate_default_branch_for_test()
            .expect("create level-zero table");
    }

    let start = Instant::now();
    let error = runtime
        .commit(&background_put_batch(b"block-timeout", b"value".to_vec()))
        .expect_err("blocked background worker should leave pressure rejected");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "pressure wait ignored its bounded deadline"
    );
    assert!(matches!(
        error,
        StorageApiError::StoragePressure {
            severity: CommitAdmissionPressureSeverity::Blocking,
            retryable: true,
            ..
        }
    ));

    let perf = crate::observability::perf_trace::snapshot();
    assert!(perf.lifecycle_write_admission_wait_attempts() >= 1);
    assert_eq!(perf.lifecycle_write_admission_wait_timeouts(), 1);
    assert!(perf.lifecycle_write_admission_block_wait_ns() > 0);

    release.wait();
    let stats = runtime
        .wait_background_idle_until_for_test(Duration::from_millis(250))
        .expect("background runtime should report bounded cleanup stats");
    assert!(
        stats.tasks_completed >= 1 || observed_open.load(Ordering::Acquire),
        "probe should finish once released"
    );
    assert!(observed_open.load(Ordering::Acquire));
}

/// The writer-lock release contract of drop versus close (#2837, #3546).
///
/// Dropping a durable runtime bounds background shutdown (the 250 ms close
/// timeout) and detaches a worker that misses it; the detached worker holds
/// the runtime, and with it the writer lock, until its task finishes. `close`
/// releases the writer lock explicitly before it returns, whatever the workers
/// are doing. A same-process reopen after a DROP must therefore wait for the
/// release; a reopen after a CLOSE never has to. The worker is parked on a
/// barrier, so both halves are deterministic.
#[cfg(feature = "localfs")]
#[test]
fn a_detached_worker_holds_the_writer_lock_past_drop_but_not_past_close() {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    fn open(backend: &'static StorageBackend) -> StorageApiResult<StorageRuntime<'static>> {
        StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            backend,
        )
        .map(StorageOpenOutcome::into_runtime)
    }

    fn park_a_worker(runtime: &StorageRuntime<'static>) -> Arc<Barrier> {
        let ready = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        assert!(
            runtime.submit_runtime_state_background_probe_for_test(
                Arc::clone(&ready),
                Arc::clone(&release),
                Arc::new(AtomicBool::new(false)),
            ),
            "the durable runtime runs a background worker"
        );
        ready.wait();
        release
    }

    let backend = crate::testkit::leak_static(StorageBackend::local_fs(temp_dir_for_api_test(
        "drop-vs-close-writer-lock",
    )));

    // Drop: the parked worker is detached and still holds the lock.
    let release = {
        let runtime = open(backend).expect("durable open");
        park_a_worker(&runtime)
    };
    let error = open(backend)
        .map(drop)
        .expect_err("a detached worker keeps the dropped runtime's writer lock");
    assert_eq!(error.code(), "failed_precondition.storage_api.writer_lock");
    release.wait();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut runtime = loop {
        match open(backend) {
            Ok(runtime) => break runtime,
            Err(StorageApiError::WriterLockHeld) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the detached worker never released the writer lock"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("reopen after the worker finished: {error:?}"),
        }
    };

    // Close: the same parked worker, but the lock is released on return.
    let release = park_a_worker(&runtime);
    runtime
        .close()
        .expect("close detaches the parked worker after its deadline");
    let reopened = open(backend).expect("close released the writer lock before returning");
    release.wait();
    drop(reopened);
}
