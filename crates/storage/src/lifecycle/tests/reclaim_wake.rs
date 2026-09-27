//! Space-reclamation contract §3.1 (slice 8): the executor's "reclaim owed"
//! fact, the suspected-debt estimate, the one-idle-wake-per-quiet-period rule
//! and the config knobs behind them.

use super::*;
use strata_core::{BranchId, CommitVersion};

fn executor() -> LifecycleMaintenanceExecutor {
    LifecycleMaintenanceExecutor::new(8).expect("executor")
}

fn mark_outcome() -> MaintenanceOutcome {
    MaintenanceOutcome::new(
        MaintenanceTaskKind::Retention,
        MaintenanceOutcomeStatus::Completed,
    )
    .with_task_scope(MaintenanceTaskScope::Branch(BranchId::from_bytes(
        [0x51; 16],
    )))
}

fn sweep_outcome(
    status: MaintenanceOutcomeStatus,
    staged: usize,
    faults: usize,
) -> MaintenanceOutcome {
    MaintenanceOutcome::new(MaintenanceTaskKind::Quarantine, status)
        .with_effects(staged, 0, false)
        .with_state_changes(staged)
        .with_stats(LifecycleStats::new(0, faults, 1, 0, 0))
}

fn purge_outcome(deleted: usize) -> MaintenanceOutcome {
    MaintenanceOutcome::new(
        MaintenanceTaskKind::Purge,
        MaintenanceOutcomeStatus::Completed,
    )
    .with_effects(deleted, 4096, false)
    .with_state_changes(deleted)
}

#[test]
fn a_publish_that_drops_refs_owes_a_sweep_and_grows_the_suspected_debt() {
    let mut executor = executor();
    assert!(!executor.reclaim_owed());
    assert_eq!(executor.suspected_debt_objects(), 0);

    executor.note_reclaim_debt(3);
    executor.note_reclaim_debt(2);

    assert!(executor.reclaim_owed());
    assert_eq!(executor.suspected_debt_objects(), 5);
}

#[test]
fn a_mark_owes_and_a_clean_sweep_clears_the_debt() {
    let mut executor = executor();
    executor.note_reclaim_debt(4);
    executor.record_reclaim(&mark_outcome());
    assert!(executor.reclaim_owed());

    executor.record_reclaim(&sweep_outcome(MaintenanceOutcomeStatus::Completed, 0, 0));

    assert!(
        !executor.reclaim_owed(),
        "a sweep with no candidates and no faults is clean"
    );
    assert_eq!(executor.suspected_debt_objects(), 0);
    // A later mark owes again.
    executor.record_reclaim(&mark_outcome());
    assert!(executor.reclaim_owed());
}

#[test]
fn a_deferred_faulting_or_staging_sweep_keeps_the_debt_owed() {
    for (status, staged, faults) in [
        (MaintenanceOutcomeStatus::Deferred, 0, 0),
        (MaintenanceOutcomeStatus::Completed, 0, 1),
        (MaintenanceOutcomeStatus::Completed, 2, 0),
        (MaintenanceOutcomeStatus::Failed, 0, 0),
    ] {
        let mut executor = executor();
        executor.note_reclaim_debt(2);
        executor.record_reclaim(&sweep_outcome(status, staged, faults));
        assert!(
            executor.reclaim_owed(),
            "status={status:?} staged={staged} faults={faults}"
        );
        assert_eq!(executor.suspected_debt_objects(), 2);
    }
}

#[test]
fn a_purge_pays_down_the_suspected_debt_without_settling_it() {
    let mut executor = executor();
    executor.note_reclaim_debt(3);
    executor.record_reclaim(&purge_outcome(2));
    assert_eq!(executor.suspected_debt_objects(), 1);
    assert!(
        executor.reclaim_owed(),
        "only a clean sweep settles the debt"
    );
    executor.record_reclaim(&purge_outcome(5));
    assert_eq!(executor.suspected_debt_objects(), 0, "saturating");
    assert!(executor.reclaim_owed());
}

#[test]
fn one_idle_wake_per_quiet_period_until_another_wake_starts_a_new_one() {
    let mut executor = executor();
    assert!(
        !executor.idle_wake_pending(),
        "nothing owed, nothing to arm"
    );
    executor.note_reclaim_debt(1);
    assert!(executor.idle_wake_pending());

    executor.record_wake(ReclaimWakeOrigin::Idle, CommitVersion::new(7));
    assert!(!executor.idle_wake_pending(), "the idle wake is spent");
    assert_eq!(
        executor.reclaim_ledger().last_idle_wake(),
        Some(CommitVersion::new(7))
    );

    executor.record_wake(ReclaimWakeOrigin::Ordinary, CommitVersion::new(8));
    assert!(
        executor.idle_wake_pending(),
        "activity starts a new quiet period"
    );
    executor.record_wake(ReclaimWakeOrigin::Idle, CommitVersion::new(8));
    assert!(!executor.idle_wake_pending());
    executor.record_wake(ReclaimWakeOrigin::Open, CommitVersion::new(9));
    assert!(
        executor.idle_wake_pending(),
        "the open wake also starts a period"
    );
    assert_eq!(
        executor.reclaim_ledger().last_open_wake(),
        Some(CommitVersion::new(9))
    );
    assert_eq!(executor.reclaim_ledger().idle_wakes(), 2);

    // Settled debt arms nothing, spent or not.
    executor.record_reclaim(&sweep_outcome(MaintenanceOutcomeStatus::Completed, 0, 0));
    assert!(!executor.idle_wake_pending());
}

#[test]
fn slice_8_config_knobs_default_nonzero_and_reject_zero() {
    let config = LifecycleConfig::default();
    assert_eq!(config.quiescence_debounce_millis(), 1_000);
    assert_eq!(config.low_tier_debt_threshold_objects(), 16);
    let tuned = config
        .with_quiescence_debounce_millis(250)
        .expect("valid debounce")
        .with_low_tier_debt_threshold_objects(1)
        .expect("valid threshold");
    assert_eq!(tuned.quiescence_debounce_millis(), 250);
    assert_eq!(tuned.low_tier_debt_threshold_objects(), 1);
    assert!(matches!(
        config.with_quiescence_debounce_millis(0),
        Err(LifecycleError::InvalidConfig {
            field: "quiescence_debounce_millis",
            ..
        })
    ));
    assert!(matches!(
        config.with_low_tier_debt_threshold_objects(0),
        Err(LifecycleError::InvalidConfig {
            field: "low_tier_debt_threshold_objects",
            ..
        })
    ));
}
