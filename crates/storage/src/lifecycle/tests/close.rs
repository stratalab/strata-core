use super::*;
use std::error::Error;
use std::fmt;

#[test]
fn close_request_default_is_valid() {
    let config = LifecycleConfig::default();

    config.validate().expect("default close config");
    assert_eq!(
        config.close_timeout_policy(),
        LifecycleCloseTimeoutPolicy::ReturnTypedTimeout
    );
}

#[test]
fn close_request_rejects_invalid_timeout_policy() {
    let error = LifecycleConfig::new(
        0,
        1,
        LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
        LifecycleLossyRecoveryPolicy::Disabled,
    )
    .expect_err("invalid close config");

    assert_eq!(error.code(), "invalid_argument.lifecycle.config");
}

#[test]
fn close_outcome_reports_prior_and_final_state() {
    let complete = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::durable_complete(false));
    let prior = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
        .with_close_fact(LifecycleCloseFact::AlreadyClosed)
        .with_close_effects(CloseOutcomeEffects::durable_complete(true));

    complete.validate().expect("complete outcome");
    prior.validate().expect("idempotent outcome");
    assert_eq!(complete.close_fact(), Some(LifecycleCloseFact::Complete));
    assert_eq!(prior.close_fact(), Some(LifecycleCloseFact::AlreadyClosed));
    assert!(prior.prior_final());
}

#[test]
fn close_outcome_reports_canceled_and_drained_task_counts() {
    let close = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::volatile_complete(false))
        .with_stats(LifecycleStats::new(0, 0, 7, 0, 1));

    close.validate().expect("close outcome");
    assert_eq!(close.stats().maintenance_tasks(), 7);
    assert_eq!(close.stats().close_attempts(), 1);
}

#[test]
fn close_outcome_reports_quiesce_sync_and_guard_effects() {
    let durable = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::durable_complete(false));
    let cache = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::volatile_complete(false));

    durable.validate().expect("durable outcome");
    cache.validate().expect("cache outcome");
    assert!(durable.commits_quiesced());
    assert!(durable.maintenance_drained());
    assert!(durable.durable_synced());
    assert!(durable.guards_released());
    assert!(!cache.durable_synced());
}

#[test]
fn close_error_preserves_source_error_chain() {
    let error = LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "WAL service failed",
        CloseSourceError,
    );

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert_eq!(
        error.source().expect("source").to_string(),
        "close source error"
    );
}

#[test]
fn close_outcome_complete_requires_closed_final_state() {
    let wrong_phase = CloseOutcome::new(ClosePhase::SyncDurableState, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::durable_complete(false));
    let missing_effects = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete);

    assert_eq!(
        wrong_phase.validate().expect_err("wrong phase").code(),
        "failed_precondition.lifecycle.close"
    );
    assert_eq!(
        missing_effects
            .validate()
            .expect_err("missing effects")
            .code(),
        "failed_precondition.lifecycle.close"
    );
}

#[test]
fn close_outcome_timeout_is_retryable() {
    let timeout = CloseOutcome::new(ClosePhase::QuiesceCommits, CloseOutcomeStatus::Timeout)
        .with_close_fact(LifecycleCloseFact::RetryPending);

    timeout.validate().expect("timeout outcome");
    assert_eq!(timeout.status(), CloseOutcomeStatus::Timeout);
    assert_eq!(timeout.close_fact(), Some(LifecycleCloseFact::RetryPending));
}

#[test]
fn close_outcome_idempotent_requires_prior_final_fact() {
    let missing_prior = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
        .with_close_fact(LifecycleCloseFact::AlreadyClosed)
        .with_close_effects(CloseOutcomeEffects::durable_complete(false));
    let wrong_fact = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::durable_complete(true));

    assert_eq!(
        missing_prior.validate().expect_err("missing prior").code(),
        "failed_precondition.lifecycle.close"
    );
    assert_eq!(
        wrong_fact.validate().expect_err("wrong fact").code(),
        "failed_precondition.lifecycle.close"
    );
}

#[test]
fn close_error_codes_are_stable() {
    assert_eq!(
        LifecycleError::CloseFailed {
            reason: "close failed"
        }
        .code(),
        "failed_precondition.lifecycle.close"
    );
    assert_eq!(
        LifecycleError::CloseTimeout {
            phase: ClosePhase::QuiesceCommits,
            reason: "timeout",
        }
        .code(),
        "failed_precondition.lifecycle.close_timeout"
    );
}

#[test]
fn close_from_open_transitions_to_closing_before_side_effects() {
    let mut machine = machine_in_state_for_close(LifecycleState::Open);

    let transition = machine
        .transition(LifecycleTransitionTrigger::CloseRequested)
        .expect("close requested");

    assert_eq!(transition.from(), LifecycleState::Open);
    assert_eq!(transition.to(), LifecycleState::Closing);
    assert_eq!(transition.close_fact(), Some(LifecycleCloseFact::Requested));
    assert_eq!(machine.state(), LifecycleState::Closing);
}

#[test]
fn close_from_closed_is_idempotent() {
    let mut machine = machine_in_state_for_close(LifecycleState::Closed);

    let transition = machine
        .transition(LifecycleTransitionTrigger::CloseRetried)
        .expect("closed retry");

    assert!(transition.is_idempotent());
    assert_eq!(
        transition.close_fact(),
        Some(LifecycleCloseFact::AlreadyClosed)
    );
    assert_eq!(machine.state(), LifecycleState::Closed);
}

#[test]
fn close_from_new_rejects() {
    assert_close_admission_rejects(LifecycleState::New);
}

#[test]
fn close_from_opening_rejects() {
    assert_close_admission_rejects(LifecycleState::Opening);
}

#[test]
fn close_from_recovering_rejects() {
    assert_close_admission_rejects(LifecycleState::Recovering);
}

#[test]
fn close_from_failed_rejects_unless_retryable_close_failure() {
    assert_close_admission_rejects(LifecycleState::Failed);
    let mut machine = machine_in_state_for_close(LifecycleState::Failed);

    assert_eq!(
        machine.transition(LifecycleTransitionTrigger::CloseRetried),
        Err(LifecycleError::InvalidLifecycleState {
            reason: "invalid lifecycle transition",
        })
    );
}

#[test]
fn close_retry_pending_allows_retry() {
    let mut machine = machine_in_state_for_close(LifecycleState::Closing);

    let transition = machine
        .transition(LifecycleTransitionTrigger::CloseRetried)
        .expect("close retry");

    assert_eq!(transition.to(), LifecycleState::Closing);
    assert_eq!(
        transition.close_fact(),
        Some(LifecycleCloseFact::RetryPending)
    );
    assert_eq!(machine.close_fact(), Some(LifecycleCloseFact::RetryPending));
}

#[test]
fn close_requested_blocks_ordinary_maintenance() {
    assert_rejected_after_close_requested(LifecycleOperationKind::OrdinaryMaintenance);
}

#[test]
fn close_requested_blocks_mutating_commits() {
    assert_rejected_after_close_requested(LifecycleOperationKind::Commit);
}

#[test]
fn diagnostic_health_query_allowed_after_close() {
    for state in [LifecycleState::Closing, LifecycleState::Closed] {
        assert!(
            LifecycleStateMachine::admit_state(state, LifecycleOperationKind::HealthQuery)
                .is_allowed()
        );
    }
}

#[test]
fn close_after_failed_nonretryable_state_rejects() {
    // `machine_in_state_for_close(Failed)` produces Failed-from-Opening
    // (see helper). That state must reject Close because the failure was
    // not raised by close-class work and the close path does not own
    // retry for it. The rejection reason names the rule precisely so
    // callers can route Open-failure recovery elsewhere.
    let machine = machine_in_state_for_close(LifecycleState::Failed);

    assert_eq!(
        machine.admit(LifecycleOperationKind::Close),
        LifecycleOperationAdmission::Rejected {
            reason: "Failed admits Close only when the prior failure was raised during close",
        }
    );
}

#[test]
fn close_after_failed_from_closing_admits_retry() {
    // The counterpart: when the prior failure WAS raised by close-class
    // work, Failed admits Close so the runtime can clean up without
    // leaking durable handles through Drop.
    let mut machine = machine_in_state_for_close(LifecycleState::Closing);
    machine
        .transition(LifecycleTransitionTrigger::PhaseFailed {
            reason: "close failed mid-sync",
        })
        .expect("close-class failure");
    assert_eq!(machine.state(), LifecycleState::Failed);

    let admission = machine.admit(LifecycleOperationKind::Close);
    assert!(
        admission.is_allowed(),
        "Failed-from-Closing must admit Close: {admission:?}"
    );
}

#[test]
fn close_timeout_leaves_runtime_retryable() {
    let mut machine = machine_in_state_for_close(LifecycleState::Closing);

    let retry = machine
        .transition(LifecycleTransitionTrigger::CloseRetried)
        .expect("retry pending");

    assert_eq!(retry.to(), LifecycleState::Closing);
    assert_eq!(machine.state(), LifecycleState::Closing);
    assert_eq!(machine.close_fact(), Some(LifecycleCloseFact::RetryPending));
}

#[test]
fn double_close_after_success_returns_prior_final_outcome() {
    let prior = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
        .with_close_fact(LifecycleCloseFact::AlreadyClosed)
        .with_close_effects(CloseOutcomeEffects::durable_complete(true));

    prior.validate().expect("prior close");
    assert!(prior.prior_final());
    assert_eq!(prior.close_fact(), Some(LifecycleCloseFact::AlreadyClosed));
}

#[test]
fn close_drain_timeout_returns_timeout_and_retry_fact() {
    let timeout = CloseOutcome::new(ClosePhase::DrainMaintenance, CloseOutcomeStatus::Timeout)
        .with_close_fact(LifecycleCloseFact::RetryPending);

    timeout.validate().expect("drain timeout");
    assert_eq!(timeout.status(), CloseOutcomeStatus::Timeout);
    assert_eq!(timeout.close_fact(), Some(LifecycleCloseFact::RetryPending));
}

#[test]
fn close_outcome_includes_maintenance_stats() {
    let close = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::durable_complete(false))
        .with_stats(LifecycleStats::new(0, 0, 3, 0, 1));

    close.validate().expect("close outcome");
    assert_eq!(close.stats().maintenance_tasks(), 3);
}

#[test]
fn close_outcome_includes_completed_drain_task_stats() {
    let close = CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::durable_complete(false))
        .with_stats(LifecycleStats::new(0, 0, 2, 0, 1));

    close.validate().expect("close outcome");
    assert_eq!(close.stats().maintenance_tasks(), 2);
}

fn assert_close_admission_rejects(state: LifecycleState) {
    assert_eq!(
        LifecycleStateMachine::admit_state(state, LifecycleOperationKind::Close),
        LifecycleOperationAdmission::Rejected {
            reason: "operation is not admitted in current lifecycle state",
        }
    );
}

fn assert_rejected_after_close_requested(operation: LifecycleOperationKind) {
    let machine = machine_in_state_for_close(LifecycleState::Closing);

    assert_eq!(
        machine.admit(operation),
        LifecycleOperationAdmission::Rejected {
            reason: "operation is not admitted in current lifecycle state",
        }
    );
}

fn machine_in_state_for_close(state: LifecycleState) -> LifecycleStateMachine {
    let mut machine = LifecycleStateMachine::new();
    match state {
        LifecycleState::New => {}
        LifecycleState::Opening => {
            machine
                .transition(LifecycleTransitionTrigger::OpenRequested)
                .expect("opening");
        }
        LifecycleState::Recovering => {
            machine
                .transition(LifecycleTransitionTrigger::OpenRequested)
                .expect("opening");
            machine
                .transition(LifecycleTransitionTrigger::DurableRecoveryRequired)
                .expect("recovering");
        }
        LifecycleState::Open => {
            machine
                .transition(LifecycleTransitionTrigger::OpenRequested)
                .expect("opening");
            machine
                .transition(LifecycleTransitionTrigger::CacheOpenReady)
                .expect("open");
        }
        LifecycleState::Closing => {
            machine = machine_in_state_for_close(LifecycleState::Open);
            machine
                .transition(LifecycleTransitionTrigger::CloseRequested)
                .expect("closing");
        }
        LifecycleState::Closed => {
            machine = machine_in_state_for_close(LifecycleState::Closing);
            machine
                .transition(LifecycleTransitionTrigger::CloseCompleted)
                .expect("closed");
        }
        LifecycleState::Failed => {
            machine = machine_in_state_for_close(LifecycleState::Opening);
            machine
                .transition(LifecycleTransitionTrigger::PhaseFailed { reason: "failed" })
                .expect("failed");
        }
    }
    machine
}

#[derive(Debug)]
struct CloseSourceError;

impl fmt::Display for CloseSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("close source error")
    }
}

impl Error for CloseSourceError {}

/// Space-reclamation contract §3.1 (slice 3): the drive's continue predicate.
#[test]
fn close_reclaim_should_continue_truth_table() {
    use std::time::Duration;
    let budget = Duration::from_millis(500);
    for (elapsed, debt_remaining, sweep_deferred, expected) in [
        (Duration::ZERO, true, false, true),
        (Duration::from_millis(499), true, false, true),
        (Duration::from_millis(500), true, false, false),
        (Duration::from_millis(501), true, false, false),
        (Duration::ZERO, false, false, false),
        (Duration::ZERO, true, true, false),
        (Duration::ZERO, false, true, false),
        (Duration::from_millis(501), false, true, false),
    ] {
        assert_eq!(
            close_reclaim_should_continue(elapsed, budget, debt_remaining, sweep_deferred),
            expected,
            "elapsed={elapsed:?} debt_remaining={debt_remaining} sweep_deferred={sweep_deferred}"
        );
    }
    assert!(
        !close_reclaim_should_continue(Duration::ZERO, Duration::ZERO, true, false),
        "a zero budget runs no round"
    );
}

/// Space-reclamation contract §3.1 (slice 7): the close-time checkpoint
/// decision — the budget gate first, then the registry's structural
/// deferral, then whether anything sits above the retention watermark.
#[test]
fn close_checkpoint_decision_truth_table() {
    use crate::lifecycle::checkpoint::CheckpointStructuralDeferral;
    use std::time::Duration;
    let bounded = LifecycleCloseReclaimBudget::Bounded(Duration::from_millis(500));
    let disabled = LifecycleCloseReclaimBudget::Disabled;
    let fork = CheckpointStructuralDeferral::UnmaterializedInheritedLayers;
    for (budget, structural, commits, expected) in [
        (
            disabled,
            None,
            0,
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::Disabled),
        ),
        (
            disabled,
            None,
            7,
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::Disabled),
        ),
        (
            disabled,
            Some(fork),
            7,
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::Disabled),
        ),
        (
            bounded,
            Some(fork),
            7,
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::Structural(fork)),
        ),
        (
            bounded,
            Some(fork),
            0,
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::Structural(fork)),
        ),
        (
            bounded,
            None,
            0,
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::NothingNew),
        ),
        (bounded, None, 1, CloseCheckpointDecision::Publish),
        (bounded, None, u64::MAX, CloseCheckpointDecision::Publish),
    ] {
        assert_eq!(
            close_checkpoint_decision(budget, structural, commits),
            expected,
            "budget={budget:?} structural={structural:?} commits={commits}"
        );
    }
    // The checkpoint is not time-bounded: a zero object-reclaim budget still
    // admits it (the budget's only role here is the on/off gate).
    assert_eq!(
        close_checkpoint_decision(
            LifecycleCloseReclaimBudget::Bounded(Duration::ZERO),
            None,
            1
        ),
        CloseCheckpointDecision::Publish
    );
}
