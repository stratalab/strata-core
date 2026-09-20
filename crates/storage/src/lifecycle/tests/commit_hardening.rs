use super::checkpoint::shared::{
    branch_id, durable_batch, generation_guard, CheckpointBackendEvent, CheckpointTestBackend,
};
use super::*;
use crate::backend::memory::MemoryBackend;
use crate::branch::config::BranchRuntimeConfig;
use crate::commit::{
    CommitBatch, CommitBatchOptions, CommitBranchGeneration, CommitConflictValidationMode,
    CommitDuplicateKeyPolicy, CommitDurabilityMode, CommitExpiry, CommitManualTimestampSource,
    CommitMutation, CommitOrigin, CommitRetentionHint, CommitRuntimeConfig, CommitTimestampPolicy,
    CommitValidationFacts,
};
use crate::row::{PhysicalKey, StorageSpaceId};
use crate::service::{WalGrowthFacts, WalServiceConfig};
use strata_core::{BranchId, Timestamp};

const DATABASE_ID: [u8; 16] = [0x7a; 16];

#[test]
fn wal_growth_policy_triggers_on_each_threshold_deterministically() {
    let policy = LifecycleWalGrowthPolicy::new(100, 2, Some(3));
    let by_bytes = WalGrowthFacts::new_for_policy(1, 101, 1, 101, 0, 0);
    let by_segments = WalGrowthFacts::new_for_policy(3, 90, 3, 30, 0, 0);
    let by_commits = WalGrowthFacts::new_for_policy(1, 90, 1, 90, 0, 0);

    assert_eq!(
        policy.trigger_for(by_bytes, 0),
        Some(LifecycleWalGrowthTrigger::RetainedBytes)
    );
    assert_eq!(
        policy.trigger_for(by_segments, 0),
        Some(LifecycleWalGrowthTrigger::RetainedSegments)
    );
    assert_eq!(
        policy.trigger_for(by_commits, 4),
        Some(LifecycleWalGrowthTrigger::CommitsSinceCheckpoint)
    );
    assert_eq!(policy.trigger_for(by_commits, 3), None);
    assert_eq!(
        LifecycleWalGrowthPolicy::disabled().trigger_for(by_bytes, 99),
        None
    );
}

#[test]
fn default_wal_growth_policy_is_size_driven_not_commit_count_driven() {
    let policy = LifecycleWalGrowthPolicy::default();
    assert!(policy.max_commits_since_checkpoint().is_none());

    // Below the byte/segment bounds, no commit count — however large — forces a
    // checkpoint under the default (size-driven) policy. This is the fix: tiny
    // commits no longer checkpoint every N commits.
    let below_bounds = WalGrowthFacts::new_for_policy(1, 1_000, 1, 1_000, 0, 0);
    assert_eq!(policy.trigger_for(below_bounds, u64::MAX), None);
    assert_eq!(
        policy.backpressure_trigger_for(below_bounds, u64::MAX),
        None
    );

    // The byte bound still fires under the default policy.
    let over_bytes = WalGrowthFacts::new_for_policy(1, 256 * 1024 * 1024 + 1, 1, 1_000, 0, 0);
    assert_eq!(
        policy.trigger_for(over_bytes, 0),
        Some(LifecycleWalGrowthTrigger::RetainedBytes)
    );
}

#[test]
fn wal_growth_hard_cap_is_byte_based_and_off_when_policy_disabled() {
    // Cap above the reclaim trigger; the hard cap is purely byte-based, so the segment count
    // is irrelevant (`under_cap` carries many segments yet stays under the cap).
    let policy = LifecycleWalGrowthPolicy::new(100, 2, None).with_max_total_wal_bytes(1_000);
    let at_cap = WalGrowthFacts::new_for_policy(64, 1_000, 1, 0, 0, 0);
    let over_cap = WalGrowthFacts::new_for_policy(1, 1_001, 1, 0, 0, 0);

    assert!(!policy.hard_cap_exceeded(at_cap));
    assert!(policy.hard_cap_exceeded(over_cap));
    // A disabled WAL-growth policy never enforces the hard cap.
    assert!(!LifecycleWalGrowthPolicy::disabled()
        .with_max_total_wal_bytes(1_000)
        .hard_cap_exceeded(over_cap));
}

#[test]
fn automatic_checkpoint_does_not_trigger_below_threshold() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x91);
    let policy = LifecycleWalGrowthPolicy::new(u64::MAX, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"below-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");

    assert_eq!(outcome.status(), LifecycleWalGrowthStatus::BelowThreshold);
    assert!(outcome.trigger().is_none());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn automatic_checkpoint_triggers_when_wal_bytes_exceed_threshold() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x92);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"bytes-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");

    assert_eq!(
        outcome.status(),
        LifecycleWalGrowthStatus::MaintenanceEnqueued
    );
    assert_eq!(
        outcome.trigger(),
        Some(LifecycleWalGrowthTrigger::RetainedBytes)
    );
    assert!(outcome.facts().retained_bytes() > 1);
    assert!(outcome.facts().retained_segments() >= 1);
    assert!(outcome.facts().active_segment_id() >= 1);
    assert!(outcome.facts().active_segment_size() > 0);
    assert!(outcome.facts().dirty_bytes() > 0);
    assert!(outcome.facts().dirty_records() > 0);
    assert_eq!(outcome.commits_since_checkpoint(), 1);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 4);
    assert!(backend.snapshot_objects().is_empty());
    assert_eq!(backend.delete_calls(), 0);

    let outcomes = drain_wal_growth_maintenance(&mut runtime);
    assert!(outcomes
        .iter()
        .any(|outcome| outcome.task_kind() == MaintenanceTaskKind::Checkpoint));
    assert!(!backend.snapshot_objects().is_empty());
    // Reclaim rotation (#3494): the checkpoint covered the whole single-segment
    // WAL, so the truncation pass seals the active segment and releases it —
    // before the fix this drain deleted nothing and the WAL was retained
    // forever.
    assert!(backend.delete_calls() > 0);
}

#[test]
fn automatic_checkpoint_triggers_when_retained_segments_exceed_threshold() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x97);
    let policy = LifecycleWalGrowthPolicy::new(u64::MAX, 1, None);
    let mut runtime = open_durable_runtime_with_wal_segment_size(branch, backend, policy, 4096);

    for index in 0..8 {
        runtime
            .execute_durable_commit(
                dynamic_durable_batch(
                    branch,
                    format!("segment-policy-{index}").into_bytes(),
                    vec![index; 3600],
                ),
                generation_guard(),
            )
            .expect("durable commit");
        if runtime.services().wal().active_segment_id() > 1 {
            break;
        }
    }
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");

    assert_eq!(
        outcome.status(),
        LifecycleWalGrowthStatus::MaintenanceEnqueued
    );
    assert_eq!(
        outcome.trigger(),
        Some(LifecycleWalGrowthTrigger::RetainedSegments)
    );
    assert!(outcome.facts().retained_segments() > 1);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 4);
}

#[test]
fn wal_growth_policy_defers_on_unmaterialized_inherited_layers_without_churn() {
    // TCP4.14 choreography: the growth-policy (enqueue) side of the fresh-
    // fork window, plus the anti-churn oracle — a structurally-deferred
    // state must never feed the executor (#2792's signature was thousands
    // of deferral completions per second; here the queue must stay EMPTY
    // across repeated triggered evaluations, not merely drain).
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0x77);
    let extra = branch_id(0x78);
    let policy = LifecycleWalGrowthPolicy::new(u64::MAX, usize::MAX, Some(3));
    let mut runtime = open_durable_runtime(initial, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(initial, b"initial-base", b"initial-base-value"),
            generation_guard(),
        )
        .expect("commit initial base");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate initial");
    runtime
        .flush_frozen(
            &FlushFrozenRequest::new(
                initial,
                None,
                FlushTableIdentitySeed::new("initial-seed").expect("seed"),
                FlushTableObjectId::new("initial-object").expect("object id"),
            )
            .expect("flush request"),
        )
        .expect("flush initial");
    runtime
        .fork_current(
            initial,
            extra,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("fork extra");

    let mut triggered = 0;
    for round in 0..6u8 {
        runtime
            .execute_durable_commit(
                durable_batch(
                    extra,
                    crate::testkit::leak_static(format!("churn-{round}").into_bytes()).as_slice(),
                    b"value",
                ),
                generation_guard(),
            )
            .expect("commit over trigger");
        let outcome = runtime
            .last_wal_growth_outcome()
            .expect("automatic policy outcome");
        if outcome.trigger().is_some() {
            triggered += 1;
            assert_eq!(outcome.status(), LifecycleWalGrowthStatus::Deferred);
        }
        assert_eq!(
            runtime.maintenance_status().pending_tasks(),
            0,
            "round {round}: a structurally-deferred checkpoint reached the queue"
        );
    }
    assert!(triggered >= 3, "vacuous: the trigger never fired");
}

#[test]
fn checkpoint_reports_the_unmaterialized_inherited_layers_deferral_distinctly() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0x75);
    let extra = branch_id(0x76);
    let mut runtime = open_durable_runtime(initial, backend, LifecycleWalGrowthPolicy::disabled());

    // Seal an owned table on the seeded branch so the fork below is COW:
    // the child carries inherited layers and owns nothing.
    runtime
        .execute_durable_commit(
            durable_batch(initial, b"initial-base", b"initial-base-value"),
            generation_guard(),
        )
        .expect("commit initial base");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate initial");
    runtime
        .flush_frozen(
            &FlushFrozenRequest::new(
                initial,
                None,
                FlushTableIdentitySeed::new("initial-seed").expect("seed"),
                FlushTableObjectId::new("initial-object").expect("object id"),
            )
            .expect("flush request"),
        )
        .expect("flush initial");
    runtime
        .fork_current(
            initial,
            extra,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("fork extra");

    let request = LifecycleCheckpointRequest::new(initial, 1, Timestamp::from_micros(9_500))
        .expect("checkpoint request");
    let outcome = runtime.checkpoint(&request).expect("checkpoint runs");
    // The distinct status and reason, not the generic deferral: the fresh
    // fork's unmaterialized inherited layers are their own structural state.
    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::DeferredUnmaterializedInheritedLayers
    );
    assert_eq!(
        outcome.maintenance_outcome().reason(),
        Some("checkpoint deferred: branch holds unmaterialized inherited layers")
    );
}

#[test]
fn wal_growth_policy_defers_while_a_non_seeded_branch_holds_a_durable_base() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0x71);
    let extra = branch_id(0x72);
    // Commit-count trigger only, sized so every setup commit stays below
    // threshold and no earlier enqueue can mask the deferral with a coalesce.
    let policy = LifecycleWalGrowthPolicy::new(u64::MAX, usize::MAX, Some(3));
    let mut runtime = open_durable_runtime(initial, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(initial, b"initial-base", b"initial-base-value"),
            generation_guard(),
        )
        .expect("commit initial base");
    runtime
        .create_branch(
            extra,
            CommitBranchGeneration::new(1).expect("generation"),
            Some(CommitVersion::new(2)),
        )
        .expect("create extra");

    // Latch the multi-branch checkpoint guard: the non-seeded branch gains a
    // durable table base (base commit -> rotate -> flush).
    runtime
        .execute_durable_commit(
            durable_batch(extra, b"extra-base", b"extra-base-value"),
            generation_guard(),
        )
        .expect("commit extra base");
    runtime
        .rotate_active_for_branch_for_maintenance(extra)
        .expect("rotate extra");
    runtime
        .flush_frozen(
            &FlushFrozenRequest::new(
                extra,
                None,
                FlushTableIdentitySeed::new("extra-seed").expect("seed"),
                FlushTableObjectId::new("extra-object").expect("object id"),
            )
            .expect("flush request"),
        )
        .expect("flush extra");
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);

    // These commits cross the trigger. The checkpoint the policy would
    // enqueue is structurally deferred by the multi-branch guard, so the
    // evaluation must defer instead of feeding the executor tasks that can
    // only churn.
    runtime
        .execute_durable_commit(
            durable_batch(extra, b"extra-delta", b"extra-delta-value"),
            generation_guard(),
        )
        .expect("commit extra delta");
    runtime
        .execute_durable_commit(
            durable_batch(extra, b"extra-delta-two", b"extra-delta-two-value"),
            generation_guard(),
        )
        .expect("commit extra delta two");
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");
    assert_eq!(outcome.status(), LifecycleWalGrowthStatus::Deferred);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn wal_growth_policy_still_enqueues_when_no_non_seeded_branch_has_a_durable_base() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0x73);
    let extra = branch_id(0x74);
    let policy = LifecycleWalGrowthPolicy::new(u64::MAX, usize::MAX, Some(3));
    let mut runtime = open_durable_runtime(initial, backend, policy);

    // The non-seeded branch exists and has rows, but no durable table base:
    // mere branch existence must not defer checkpoint scheduling.
    runtime
        .execute_durable_commit(
            durable_batch(initial, b"initial-base", b"initial-base-value"),
            generation_guard(),
        )
        .expect("commit initial base");
    runtime
        .create_branch(
            extra,
            CommitBranchGeneration::new(1).expect("generation"),
            Some(CommitVersion::new(2)),
        )
        .expect("create extra");
    runtime
        .execute_durable_commit(
            durable_batch(extra, b"extra-base", b"extra-base-value"),
            generation_guard(),
        )
        .expect("commit extra base");
    runtime
        .execute_durable_commit(
            durable_batch(extra, b"extra-delta", b"extra-delta-value"),
            generation_guard(),
        )
        .expect("commit extra delta");
    runtime
        .execute_durable_commit(
            durable_batch(extra, b"extra-delta-two", b"extra-delta-two-value"),
            generation_guard(),
        )
        .expect("commit extra delta two");
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");
    assert_eq!(
        outcome.status(),
        LifecycleWalGrowthStatus::MaintenanceEnqueued
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 4);
}

#[test]
fn automatic_checkpoint_coalesces_existing_checkpoint_task() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x93);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"coalesce-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let first = runtime
        .last_wal_growth_outcome()
        .cloned()
        .expect("automatic policy outcome");
    let second = runtime.evaluate_wal_growth_policy().expect("policy retry");

    assert_eq!(
        first.status(),
        LifecycleWalGrowthStatus::MaintenanceEnqueued
    );
    assert_eq!(
        second.status(),
        LifecycleWalGrowthStatus::MaintenanceCoalesced
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 4);
    assert_eq!(
        first.enqueue().map(|outcome| outcome.task_id()),
        second.enqueue().map(|outcome| outcome.task_id())
    );
}

#[test]
fn automatic_checkpoint_uses_existing_maintenance_executor() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x8d);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"executor-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let enqueue = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome")
        .enqueue()
        .copied()
        .expect("maintenance enqueue");

    assert!(enqueue.was_enqueued());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 4);
    assert!(backend.snapshot_objects().is_empty());

    let outcomes = drain_wal_growth_maintenance(&mut runtime);
    let maintenance = outcomes
        .iter()
        .find(|outcome| outcome.task_id() == Some(enqueue.task_id()))
        .expect("returned task outcome");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::WalTruncation);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert!(!backend.snapshot_objects().is_empty());
}

#[test]
fn automatic_checkpoint_failure_records_health_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x98);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    backend.fail_wal_listing();
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"failure-policy", b"value"),
            generation_guard(),
        )
        .expect("commit stays successful");
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");

    assert_eq!(outcome.status(), LifecycleWalGrowthStatus::Deferred);
    assert!(outcome.recovery_health().is_some());
    assert!(outcome.source_error().is_some());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn automatic_checkpoint_disable_requires_explicit_config() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x99);
    let mut runtime = open_durable_runtime(branch, backend, LifecycleWalGrowthPolicy::disabled());

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"disabled-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");

    assert_eq!(outcome.status(), LifecycleWalGrowthStatus::Disabled);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn automatic_checkpoint_deferred_while_quiesce_active() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x94);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"quiesce-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    assert_eq!(runtime.maintenance_status().pending_tasks(), 4);
    drain_wal_growth_maintenance(&mut runtime);
    let quiesce = runtime
        .guard_set()
        .try_begin_quiesce()
        .expect("begin quiesce");
    let outcome = runtime
        .evaluate_wal_growth_policy()
        .expect("policy evaluation");

    assert_eq!(outcome.status(), LifecycleWalGrowthStatus::Deferred);
    assert!(outcome.recovery_health().is_none());
    assert!(outcome.source_error().is_some());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    drop(quiesce);
    let retry = runtime.evaluate_wal_growth_policy().expect("policy retry");
    assert_eq!(
        retry.status(),
        LifecycleWalGrowthStatus::MaintenanceEnqueued
    );
}

#[test]
fn automatic_checkpoint_deferred_while_close_in_progress() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x8e);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"close-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    drain_wal_growth_maintenance(&mut runtime);
    runtime
        .force_close_requested_for_test()
        .expect("close requested");
    let outcome = runtime
        .evaluate_wal_growth_policy()
        .expect("policy evaluation");

    assert_eq!(outcome.status(), LifecycleWalGrowthStatus::Deferred);
    assert!(outcome.recovery_health().is_none());
    assert_eq!(
        outcome.source_error().expect("source error").code(),
        "failed_precondition.lifecycle.state"
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn automatic_checkpoint_deferred_while_recovery_in_progress() {
    let mut state = LifecycleStateMachine::new();
    state
        .transition(LifecycleTransitionTrigger::OpenRequested)
        .expect("opening");
    state
        .transition(LifecycleTransitionTrigger::DurableRecoveryRequired)
        .expect("recovering");

    let error = policy_admission_error(state).expect("recovery rejects maintenance");

    assert_eq!(state.state(), LifecycleState::Recovering);
    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
}

#[test]
fn automatic_checkpoint_cache_mode_reports_no_durable_action() {
    let branch = branch_id(0x95);
    let backend: &'static MemoryBackend = crate::testkit::leak_static(MemoryBackend::new());
    let runtime = LifecycleCacheRuntime::open(
        LifecycleCacheOpenRequest::new(
            StorageOpenPlan::new(
                StorageMode::Cache,
                LifecycleCodecId::identity(),
                RecoveryStrictness::Strict,
                LifecycleConfig::default(),
            )
            .expect("cache plan"),
            branch,
            CommitBranchGeneration::new(1).expect("generation"),
        )
        .expect("cache request"),
        backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
    )
    .expect("cache runtime");

    let outcome = runtime.evaluate_wal_growth_policy();
    assert_eq!(outcome.status(), LifecycleWalGrowthStatus::NoDurableAction);
    assert_eq!(outcome.facts(), WalGrowthFacts::empty());
}

#[test]
fn wal_growth_pressure_facts_are_visible_to_public_boundary() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x8f);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"facts-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let outcome = runtime
        .last_wal_growth_outcome()
        .expect("automatic policy outcome");
    let facts = outcome.facts();

    assert_eq!(
        outcome.status(),
        LifecycleWalGrowthStatus::MaintenanceEnqueued
    );
    assert_eq!(
        outcome.trigger(),
        Some(LifecycleWalGrowthTrigger::RetainedBytes)
    );
    assert_eq!(outcome.commits_since_checkpoint(), 1);
    assert!(facts.retained_bytes() > 0);
    assert!(facts.retained_segments() > 0);
    assert!(facts.active_segment_size() > 0);
    assert!(facts.dirty_bytes() > 0);
    assert!(facts.dirty_records() > 0);
}

#[test]
fn automatic_checkpoint_policy_is_deterministic_without_background_thread() {
    let first_backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let second_backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x8a);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut first = open_durable_runtime(branch, first_backend, policy);
    let mut second = open_durable_runtime(branch, second_backend, policy);

    for runtime in [&mut first, &mut second] {
        runtime
            .execute_durable_commit(
                durable_batch(branch, b"deterministic-policy", b"value"),
                generation_guard(),
            )
            .expect("durable commit");
    }
    let first_outcome = first
        .last_wal_growth_outcome()
        .expect("first automatic policy outcome");
    let second_outcome = second
        .last_wal_growth_outcome()
        .expect("second automatic policy outcome");

    assert_eq!(first_outcome.status(), second_outcome.status());
    assert_eq!(first_outcome.trigger(), second_outcome.trigger());
    assert_eq!(
        first_outcome.commits_since_checkpoint(),
        second_outcome.commits_since_checkpoint()
    );
    assert_eq!(first_outcome.facts(), second_outcome.facts());
    assert_eq!(first.maintenance_status().pending_tasks(), 4);
    assert_eq!(second.maintenance_status().pending_tasks(), 4);
    assert!(first_backend.snapshot_objects().is_empty());
    assert!(second_backend.snapshot_objects().is_empty());
}

// The cached retention watermark must be invalidated when a checkpoint advances
// the manifest snapshot watermark, so commits_since_checkpoint recomputes against
// the new watermark instead of climbing forever from the stale base. Uses the
// commit-count trigger so a stale cache would keep re-triggering and the assert
// would catch it.
#[test]
fn checkpoint_invalidates_cached_retention_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb1);
    // Only the commit-count trigger is active (bytes/segments effectively off).
    let policy = LifecycleWalGrowthPolicy::new(u64::MAX, usize::MAX, Some(3));
    let mut runtime = open_durable_runtime(branch, backend, policy);

    for index in 0..4 {
        runtime
            .execute_durable_commit(
                dynamic_durable_batch(branch, format!("k{index}").into_bytes(), b"v".to_vec()),
                generation_guard(),
            )
            .expect("durable commit");
    }
    // The 4th commit crosses the commit-count threshold (3) and enqueues a
    // checkpoint; commits_since_checkpoint is 4 (no checkpoint applied yet).
    let before = runtime
        .last_wal_growth_outcome()
        .expect("policy outcome")
        .commits_since_checkpoint();
    assert_eq!(before, 4);

    // Run the checkpoint: it advances snapshot_watermark to the visible version
    // and must invalidate the cached watermark.
    drain_wal_growth_maintenance(&mut runtime);

    runtime
        .execute_durable_commit(
            dynamic_durable_batch(branch, b"after".to_vec(), b"v".to_vec()),
            generation_guard(),
        )
        .expect("durable commit after checkpoint");
    // visible=5, watermark advanced to 4 by the checkpoint → 5 - 4 = 1. A missed
    // invalidation would leave the watermark at 0 and report 5.
    let after = runtime
        .last_wal_growth_outcome()
        .expect("policy outcome")
        .commits_since_checkpoint();
    assert_eq!(after, 1);
}

// The cached retention watermark must equal a fresh manifest read after a
// checkpoint advances it — locks the cache to ground truth.
#[test]
fn cached_retention_watermark_matches_manifest_after_checkpoint() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xb2);
    let policy = LifecycleWalGrowthPolicy::new(u64::MAX, usize::MAX, Some(2));
    let mut runtime = open_durable_runtime(branch, backend, policy);

    for index in 0..3 {
        runtime
            .execute_durable_commit(
                dynamic_durable_batch(branch, format!("k{index}").into_bytes(), b"v".to_vec()),
                generation_guard(),
            )
            .expect("durable commit");
    }
    drain_wal_growth_maintenance(&mut runtime);
    runtime
        .execute_durable_commit(
            dynamic_durable_batch(branch, b"after".to_vec(), b"v".to_vec()),
            generation_guard(),
        )
        .expect("durable commit after checkpoint");

    let manifest = runtime
        .services()
        .manifest()
        .load_required()
        .expect("manifest");
    let expected = crate::lifecycle::commits_since_checkpoint(
        runtime.visible_version(),
        crate::lifecycle::wal_retention_watermark(
            manifest.snapshot_watermark().map(CommitVersion::new),
            manifest.flushed_through_commit_id(),
        ),
    );
    let reported = runtime
        .last_wal_growth_outcome()
        .expect("policy outcome")
        .commits_since_checkpoint();
    assert_eq!(reported, expected);
}

fn open_durable_runtime(
    branch: BranchId,
    backend: &'static CheckpointTestBackend,
    policy: LifecycleWalGrowthPolicy,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    open_durable_runtime_with_wal_segment_size(
        branch,
        backend,
        policy,
        WalServiceConfig::default().segment_size(),
    )
}

fn open_durable_runtime_with_wal_segment_size(
    branch: BranchId,
    backend: &'static CheckpointTestBackend,
    policy: LifecycleWalGrowthPolicy,
    segment_size: u64,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let lifecycle_config = LifecycleConfig::default()
        .with_wal_growth_policy(policy)
        .expect("lifecycle config");
    let request = LifecycleDurableLocalOpenRequest::new(
        StorageOpenPlan::new(
            StorageMode::DurableLocalStandard,
            LifecycleCodecId::identity(),
            RecoveryStrictness::Strict,
            lifecycle_config,
        )
        .expect("open plan"),
        DATABASE_ID,
        branch,
        CommitBranchGeneration::new(1).expect("generation"),
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        WalServiceConfig::new(segment_size),
    )
    .expect("durable request");
    let mut shell = LifecycleDurableLocalShell::assemble(
        request,
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    shell.complete_recovery(&recovery).expect("runtime")
}

fn dynamic_durable_batch(branch: BranchId, user_key: Vec<u8>, value: Vec<u8>) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            dynamic_physical_key(branch, user_key),
            value,
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

fn dynamic_physical_key(branch: BranchId, user_key: Vec<u8>) -> PhysicalKey {
    PhysicalKey::new(
        branch,
        "commit-hardening",
        StorageSpaceId::engine(0x35).expect("engine storage space"),
        user_key,
    )
    .expect("physical key")
}

fn drain_wal_growth_maintenance(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
) -> Vec<MaintenanceOutcome> {
    let mut outcomes = Vec::new();
    for _ in 0..8 {
        if runtime.maintenance_status().pending_tasks() == 0 {
            break;
        }
        if let Some(outcome) = runtime
            .run_next_flush_maintenance()
            .expect("flush maintenance")
        {
            outcomes.push(outcome);
            continue;
        }
        if let Some(outcome) = runtime
            .run_next_checkpoint_maintenance()
            .expect("checkpoint maintenance")
        {
            outcomes.push(outcome);
            continue;
        }
        if let Some(outcome) = runtime
            .run_next_flush_watermark_maintenance()
            .expect("flush watermark maintenance")
        {
            outcomes.push(outcome);
            continue;
        }
        if let Some(outcome) = runtime
            .run_next_wal_truncation_maintenance()
            .expect("WAL truncation maintenance")
        {
            outcomes.push(outcome);
            continue;
        }
        panic!("pending WAL growth maintenance did not match a WAL runner");
    }
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    outcomes
}

#[test]
fn automatic_checkpoint_does_not_truncate_wal_without_retention_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x96);
    let policy = LifecycleWalGrowthPolicy::disabled();
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_batch(branch, b"proof-policy", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue direct WAL truncation");
    let maintenance = runtime
        .run_next_wal_truncation_maintenance()
        .expect("WAL truncation runner")
        .expect("WAL truncation outcome");

    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(backend.delete_calls(), 0);
    assert!(!backend
        .events()
        .iter()
        .any(|event| matches!(event, CheckpointBackendEvent::ObjectDelete)));
}

#[test]
fn automatic_checkpoint_truncates_wal_only_after_checkpoint_or_table_manifest_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x8b);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime_with_wal_segment_size(branch, backend, policy, 4096);

    for index in 0..8 {
        runtime
            .execute_durable_commit(
                dynamic_durable_batch(
                    branch,
                    format!("proof-rotation-{index}").into_bytes(),
                    vec![index; 3600],
                ),
                generation_guard(),
            )
            .expect("durable commit");
        if runtime.maintenance_status().pending_tasks() > 0 {
            let outcomes = drain_wal_growth_maintenance(&mut runtime);
            assert!(outcomes.iter().any(|outcome| {
                matches!(
                    outcome.status(),
                    MaintenanceOutcomeStatus::Completed | MaintenanceOutcomeStatus::Deferred
                )
            }));
        }
        if runtime.services().wal().active_segment_id() > 1 && backend.delete_calls() > 0 {
            break;
        }
    }
    assert!(
        runtime.services().wal().active_segment_id() > 1,
        "test setup must rotate the log"
    );

    assert!(backend.delete_calls() > 0);
}

fn durable_sized_batch(branch: BranchId, user_key: Vec<u8>, value: Vec<u8>) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            dynamic_physical_key(branch, user_key),
            value,
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

#[test]
fn wal_truncation_reclaims_a_covered_active_segment() {
    // #3494: a database whose WAL never crosses the segment-rotation threshold
    // holds ONE active segment forever, and `delete_covered_segments` protects
    // the active id — so however much of the WAL a checkpoint covered, not one
    // byte was ever released. Reclaim must rotate a fully-covered active
    // segment so truncation can free it.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa1);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    for index in 0u8..4 {
        runtime
            .execute_durable_commit(
                durable_sized_batch(
                    branch,
                    format!("reclaim-{index}").into_bytes(),
                    vec![index; 8 * 1024],
                ),
                generation_guard(),
            )
            .expect("durable commit");
    }
    let before = runtime.current_wal_growth_facts().expect("facts before");
    assert!(before.retained_bytes() > 0, "commits reached the WAL");

    drain_wal_growth_maintenance(&mut runtime);

    let after = runtime.current_wal_growth_facts().expect("facts after");
    assert!(
        after.active_segment_id() > before.active_segment_id(),
        "reclaim rotates the covered active segment (id {} -> {})",
        before.active_segment_id(),
        after.active_segment_id()
    );
    assert!(
        after.retained_bytes() < before.retained_bytes() / 4,
        "covered WAL bytes are released ({} -> {})",
        before.retained_bytes(),
        after.retained_bytes()
    );
}

/// Like [`drain_wal_growth_maintenance`], plus the compaction work a real
/// flush cadence schedules — the bulk-import test below flushes repeatedly,
/// which the strict four-kind helper (sized for one growth sequence) rejects.
fn drain_bulk_import_maintenance(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
) {
    for _ in 0..32 {
        if runtime.maintenance_status().pending_tasks() == 0 {
            return;
        }
        if runtime
            .run_next_flush_maintenance()
            .expect("flush maintenance")
            .is_some()
            || runtime
                .run_next_checkpoint_maintenance()
                .expect("checkpoint maintenance")
                .is_some()
            || runtime
                .run_next_flush_watermark_maintenance()
                .expect("flush watermark maintenance")
                .is_some()
            || runtime
                .run_next_wal_truncation_maintenance()
                .expect("WAL truncation maintenance")
                .is_some()
            || runtime
                .run_next_compaction_maintenance()
                .expect("compaction maintenance")
                .is_some()
            || runtime
                .run_next_table_rewrite_maintenance()
                .expect("table rewrite maintenance")
                .is_some()
            || runtime
                .run_next_retention_maintenance()
                .expect("retention maintenance")
                .is_some()
            || runtime
                .run_next_purge_maintenance()
                .expect("purge maintenance")
                .is_some()
            || runtime
                .run_next_materialization_maintenance()
                .expect("materialization maintenance")
                .is_some()
            || runtime
                .run_next_quarantine_maintenance()
                .expect("quarantine maintenance")
                .is_some()
        {
            continue;
        }
        panic!("pending bulk-import maintenance did not match a runner");
    }
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn covered_wal_is_reclaimed_as_flushes_advance_the_watermark() {
    // #3494: once flushes advance the retention watermark — as a long-running
    // process's background maintenance does during operation — the covered WAL
    // must be RECLAIMED rather than accumulate. This drives that flush cadence
    // explicitly and asserts the WAL converges instead of growing with the
    // data. (The measured control: with the reclaim rotation disabled this WAL
    // grows monotonically; the separate question of making the DEFAULT policy
    // advance the watermark autonomously for a small database is deferred to
    // the scale-aware-trigger slice — a checkpoint-cadence change with its own
    // write-amp tradeoff, not needed to reclaim what a flush has already
    // covered.)
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa2);
    let mut runtime = open_durable_runtime(branch, backend, LifecycleWalGrowthPolicy::default());

    let mut peak_retained = 0u64;
    for index in 0u16..24 {
        let outcome = runtime
            .execute_durable_commit(
                durable_sized_batch(
                    branch,
                    format!("small-db-{index}").into_bytes(),
                    vec![u8::try_from(index % 251).expect("byte"); 1024 * 1024],
                ),
                generation_guard(),
            )
            .expect("durable commit");
        peak_retained = peak_retained.max(
            runtime
                .current_wal_growth_facts()
                .expect("facts")
                .retained_bytes(),
        );
        // Every fourth commit, flush and advance the coverage watermark — the
        // work a long-running process's maintenance loop does during idle,
        // sized down so the test does not depend on the host memory budget.
        if index % 4 == 3 {
            runtime
                .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
                .expect("enqueue flush");
            runtime
                .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
                    outcome
                        .commit_version()
                        .expect("mutating commit has a version"),
                ))
                .expect("enqueue flush watermark");
            drain_bulk_import_maintenance(&mut runtime);
        }
    }

    let after = runtime.current_wal_growth_facts().expect("facts after");
    // The WAL accumulated real bytes between reclaims (several 1 MiB commits)…
    assert!(
        peak_retained > 3 * 1024 * 1024,
        "test setup must accumulate WAL between reclaims (peak {peak_retained})"
    );
    // …and converged instead of growing with the data: retained WAL ends far
    // below the 24 MiB written, which the pre-fix active-id protection left
    // retained forever.
    assert!(
        after.retained_bytes() < 16 * 1024 * 1024 && after.retained_bytes() < peak_retained,
        "covered WAL is reclaimed as the watermark advances \
         (peak {peak_retained}, retained {})",
        after.retained_bytes()
    );
    assert!(
        after.active_segment_id() > 1,
        "reclaim rotated the active segment"
    );
    assert!(backend.delete_calls() > 0, "covered segments were released");
}

#[test]
fn wal_truncation_leaves_an_uncovered_active_tail_in_place() {
    // Direction control for #3494's reclaim rotation: commits ABOVE the
    // coverage watermark are the only copy of that data — the truncation pass
    // must neither rotate nor release the active segment that holds them.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa3);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_sized_batch(branch, b"covered".to_vec(), vec![0xc0; 8 * 1024]),
            generation_guard(),
        )
        .expect("durable commit");
    drain_wal_growth_maintenance(&mut runtime);
    let reclaimed = runtime.current_wal_growth_facts().expect("facts");

    // A fresh commit lands in the new active segment, above the watermark.
    runtime
        .execute_durable_commit(
            durable_sized_batch(branch, b"fresh-tail".to_vec(), vec![0xc1; 8 * 1024]),
            generation_guard(),
        )
        .expect("durable commit");
    let deletes_before = backend.delete_calls();
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue WAL truncation");
    runtime
        .run_next_wal_truncation_maintenance()
        .expect("WAL truncation runner")
        .expect("WAL truncation outcome");

    let after = runtime.current_wal_growth_facts().expect("facts after");
    assert_eq!(
        after.active_segment_id(),
        reclaimed.active_segment_id(),
        "an uncovered tail must keep its active segment"
    );
    assert!(after.retained_bytes() > 0, "the fresh tail is retained");
    assert_eq!(
        backend.delete_calls(),
        deletes_before,
        "nothing is released while the tail is uncovered"
    );
}
#[test]
fn background_wal_truncation_start_rotates_a_covered_active_segment() {
    // The background truncation stages its deletes off-lock on a retention
    // clone, so the #3494 reclaim rotation must happen at the ON-LOCK start —
    // the sealed segment is immutable from that point, which is exactly what
    // the off-lock delete pass is allowed to touch.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xa4);
    let policy = LifecycleWalGrowthPolicy::new(1, usize::MAX, None);
    let mut runtime = open_durable_runtime(branch, backend, policy);

    runtime
        .execute_durable_commit(
            durable_sized_batch(branch, b"bg-covered".to_vec(), vec![0xb6; 8 * 1024]),
            generation_guard(),
        )
        .expect("durable commit");
    // Run the enqueued sequence up to — but not including — the truncation
    // task, so coverage is proven while the truncation is still pending.
    runtime
        .run_next_flush_maintenance()
        .expect("flush maintenance")
        .expect("flush outcome");
    runtime
        .run_next_checkpoint_maintenance()
        .expect("checkpoint maintenance")
        .expect("checkpoint outcome");
    runtime
        .run_next_flush_watermark_maintenance()
        .expect("flush watermark maintenance")
        .expect("flush watermark outcome");
    let before = runtime.current_wal_growth_facts().expect("facts before");

    let step = runtime
        .start_next_background_wal_truncation_maintenance()
        .expect("start background truncation")
        .expect("truncation task is pending");
    assert!(
        matches!(step, DurableBackgroundMaintenanceStep::Build(_)),
        "truncation stages its deletes off-lock"
    );
    let after = runtime.current_wal_growth_facts().expect("facts after");
    assert!(
        after.active_segment_id() > before.active_segment_id(),
        "the covered active segment is sealed at the on-lock start ({} -> {})",
        before.active_segment_id(),
        after.active_segment_id()
    );
}
