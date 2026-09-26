//! Per-runtime reclaim ledger: the last outcome of every space-reclamation
//! family, classified from the maintenance outcomes the runners already emit.
//!
//! The ledger is the product-facing record of reclamation (space-reclamation
//! contract, `docs/design/3596-space-reclamation-contract.md` §3.5). It is
//! owned by the maintenance executor, so it is per database and never process
//! global (CLAUDE.md rule 9); the `perf-trace` counters remain the cross-database
//! performance lane. Every task completion — foreground, background off-lock
//! stage, and close drain — passes through the executor's outcome recording,
//! which is the single call site that feeds this ledger.

use super::{
    MaintenanceDeferralReason, MaintenanceOutcome, MaintenanceOutcomeStatus, MaintenanceTaskKind,
    MaintenanceTaskScope,
};

/// One space-reclamation family. Each has exactly one slot in the ledger.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimFamily {
    /// The table-object retention mark (`Retention` task with a branch scope):
    /// decides which unreferenced table objects the sweep may stage.
    TableObjectMark,
    /// The quarantine sweep: stages unreferenced table objects into quarantine.
    TableObjectSweep,
    /// The quarantine purge: physically deletes staged objects.
    QuarantinePurge,
    /// Snapshot pruning, whether requested directly or through a
    /// retention-scoped or global retention pass.
    SnapshotPrune,
    /// WAL truncation below the retention watermark.
    WalTruncation,
}

/// How a reclaim pass ended, independent of family.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimOutcome {
    /// The pass completed and released bytes or changed durable state.
    Reclaimed,
    /// The pass completed and found nothing to reclaim.
    Nothing,
    /// The pass deferred; `ReclaimEvent::deferral` says why when the runner
    /// classified it.
    Deferred,
    Failed,
    Canceled,
}

/// One recorded reclaim pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReclaimEvent {
    outcome: ReclaimOutcome,
    deferral: Option<MaintenanceDeferralReason>,
    bytes_reclaimed: u64,
    objects_affected: usize,
    state_changes: usize,
}

impl ReclaimEvent {
    pub(crate) const fn outcome(self) -> ReclaimOutcome {
        self.outcome
    }

    pub(crate) const fn deferral(self) -> Option<MaintenanceDeferralReason> {
        self.deferral
    }

    pub(crate) const fn bytes_reclaimed(self) -> u64 {
        self.bytes_reclaimed
    }

    pub(crate) const fn objects_affected(self) -> usize {
        self.objects_affected
    }

    pub(crate) const fn state_changes(self) -> usize {
        self.state_changes
    }
}

/// Running totals over every recorded pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReclaimTotals {
    events: u64,
    bytes_reclaimed: u64,
    reclaimed_passes: u64,
    deferred_passes: u64,
}

impl ReclaimTotals {
    pub(crate) const fn events(self) -> u64 {
        self.events
    }

    pub(crate) const fn bytes_reclaimed(self) -> u64 {
        self.bytes_reclaimed
    }

    pub(crate) const fn reclaimed_passes(self) -> u64 {
        self.reclaimed_passes
    }

    pub(crate) const fn deferred_passes(self) -> u64 {
        self.deferred_passes
    }
}

/// The last event of every family plus running totals. `Default` is the
/// empty ledger of a runtime that has run no reclaim pass yet.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReclaimLedger {
    last_mark: Option<ReclaimEvent>,
    last_sweep: Option<ReclaimEvent>,
    last_purge: Option<ReclaimEvent>,
    last_snapshot_prune: Option<ReclaimEvent>,
    last_wal_truncation: Option<ReclaimEvent>,
    totals: ReclaimTotals,
}

impl ReclaimLedger {
    pub(crate) fn record(&mut self, family: ReclaimFamily, event: ReclaimEvent) {
        let slot = match family {
            ReclaimFamily::TableObjectMark => &mut self.last_mark,
            ReclaimFamily::TableObjectSweep => &mut self.last_sweep,
            ReclaimFamily::QuarantinePurge => &mut self.last_purge,
            ReclaimFamily::SnapshotPrune => &mut self.last_snapshot_prune,
            ReclaimFamily::WalTruncation => &mut self.last_wal_truncation,
        };
        *slot = Some(event);
        self.totals.events = self.totals.events.saturating_add(1);
        self.totals.bytes_reclaimed = self
            .totals
            .bytes_reclaimed
            .saturating_add(event.bytes_reclaimed);
        match event.outcome {
            ReclaimOutcome::Reclaimed => {
                self.totals.reclaimed_passes = self.totals.reclaimed_passes.saturating_add(1);
            }
            ReclaimOutcome::Deferred => {
                self.totals.deferred_passes = self.totals.deferred_passes.saturating_add(1);
            }
            ReclaimOutcome::Nothing | ReclaimOutcome::Failed | ReclaimOutcome::Canceled => {}
        }
    }

    pub(crate) const fn last(&self, family: ReclaimFamily) -> Option<ReclaimEvent> {
        match family {
            ReclaimFamily::TableObjectMark => self.last_mark,
            ReclaimFamily::TableObjectSweep => self.last_sweep,
            ReclaimFamily::QuarantinePurge => self.last_purge,
            ReclaimFamily::SnapshotPrune => self.last_snapshot_prune,
            ReclaimFamily::WalTruncation => self.last_wal_truncation,
        }
    }

    pub(crate) const fn totals(&self) -> ReclaimTotals {
        self.totals
    }
}

/// The reclaim family a completed task belongs to, or `None` for tasks that
/// are not reclamation (flush, compaction, checkpoint, ...). A `Retention`
/// task is the table-object mark only when branch-scoped; the retention and
/// global scopes prune snapshots.
pub(crate) const fn reclaim_family(
    kind: MaintenanceTaskKind,
    scope: Option<MaintenanceTaskScope>,
) -> Option<ReclaimFamily> {
    match kind {
        MaintenanceTaskKind::Retention => match scope {
            Some(MaintenanceTaskScope::Branch(_)) => Some(ReclaimFamily::TableObjectMark),
            _ => Some(ReclaimFamily::SnapshotPrune),
        },
        MaintenanceTaskKind::SnapshotPruning => Some(ReclaimFamily::SnapshotPrune),
        MaintenanceTaskKind::Quarantine => Some(ReclaimFamily::TableObjectSweep),
        MaintenanceTaskKind::Purge => Some(ReclaimFamily::QuarantinePurge),
        MaintenanceTaskKind::WalTruncation => Some(ReclaimFamily::WalTruncation),
        MaintenanceTaskKind::Flush
        | MaintenanceTaskKind::Compaction
        | MaintenanceTaskKind::Materialization
        | MaintenanceTaskKind::Checkpoint
        | MaintenanceTaskKind::FlushWatermark
        | MaintenanceTaskKind::Repair
        | MaintenanceTaskKind::HealthCollection
        | MaintenanceTaskKind::CachePreheat => None,
    }
}

/// Classify a maintenance outcome as a reclaim event. A completed pass that
/// released bytes or changed durable state is `Reclaimed`; a completed pass
/// with neither is `Nothing`.
pub(crate) const fn classify_reclaim_outcome(
    status: MaintenanceOutcomeStatus,
    bytes_reclaimed: u64,
    state_changes: usize,
) -> ReclaimOutcome {
    match status {
        MaintenanceOutcomeStatus::Completed => {
            if bytes_reclaimed > 0 || state_changes > 0 {
                ReclaimOutcome::Reclaimed
            } else {
                ReclaimOutcome::Nothing
            }
        }
        MaintenanceOutcomeStatus::Deferred => ReclaimOutcome::Deferred,
        MaintenanceOutcomeStatus::Failed => ReclaimOutcome::Failed,
        MaintenanceOutcomeStatus::Canceled => ReclaimOutcome::Canceled,
    }
}

/// The ledger entry for a WAL truncation that ran as a checkpoint's follow-up,
/// or `None` when the outcome carries no follow-up.
pub(crate) fn classify_reclaim_follow_up(
    outcome: &MaintenanceOutcome,
) -> Option<(ReclaimFamily, ReclaimEvent)> {
    let follow_up = outcome.wal_truncation_follow_up()?;
    let status = if follow_up.completed() {
        MaintenanceOutcomeStatus::Completed
    } else {
        MaintenanceOutcomeStatus::Failed
    };
    Some((
        ReclaimFamily::WalTruncation,
        ReclaimEvent {
            outcome: classify_reclaim_outcome(status, 0, follow_up.deleted_segments()),
            deferral: None,
            bytes_reclaimed: 0,
            objects_affected: follow_up.deleted_segments(),
            state_changes: follow_up.deleted_segments(),
        },
    ))
}

/// The ledger entry for a finished maintenance task, or `None` when the task
/// is not a reclaim family.
pub(crate) fn classify_reclaim(
    outcome: &MaintenanceOutcome,
) -> Option<(ReclaimFamily, ReclaimEvent)> {
    let family = reclaim_family(outcome.task_kind(), outcome.task_scope())?;
    let event = ReclaimEvent {
        outcome: classify_reclaim_outcome(
            outcome.status(),
            outcome.bytes_reclaimed(),
            outcome.state_changes(),
        ),
        deferral: outcome.deferral_reason(),
        bytes_reclaimed: outcome.bytes_reclaimed(),
        objects_affected: outcome.affected_objects(),
        state_changes: outcome.state_changes(),
    };
    Some((family, event))
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::BranchId;

    fn branch() -> BranchId {
        BranchId::from_bytes([0x11; 16])
    }

    #[test]
    fn reclaim_family_maps_every_task_kind() {
        let cases: [(
            MaintenanceTaskKind,
            Option<MaintenanceTaskScope>,
            Option<ReclaimFamily>,
        ); 14] = [
            (
                MaintenanceTaskKind::Retention,
                Some(MaintenanceTaskScope::Branch(branch())),
                Some(ReclaimFamily::TableObjectMark),
            ),
            (
                MaintenanceTaskKind::Retention,
                Some(MaintenanceTaskScope::Retention),
                Some(ReclaimFamily::SnapshotPrune),
            ),
            (
                MaintenanceTaskKind::Retention,
                Some(MaintenanceTaskScope::Global),
                Some(ReclaimFamily::SnapshotPrune),
            ),
            (
                MaintenanceTaskKind::Retention,
                None,
                Some(ReclaimFamily::SnapshotPrune),
            ),
            (
                MaintenanceTaskKind::SnapshotPruning,
                Some(MaintenanceTaskScope::Retention),
                Some(ReclaimFamily::SnapshotPrune),
            ),
            (
                MaintenanceTaskKind::Quarantine,
                Some(MaintenanceTaskScope::Quarantine),
                Some(ReclaimFamily::TableObjectSweep),
            ),
            (
                MaintenanceTaskKind::Purge,
                Some(MaintenanceTaskScope::Branch(branch())),
                Some(ReclaimFamily::QuarantinePurge),
            ),
            (
                MaintenanceTaskKind::WalTruncation,
                Some(MaintenanceTaskScope::Wal),
                Some(ReclaimFamily::WalTruncation),
            ),
            (
                MaintenanceTaskKind::Flush,
                Some(MaintenanceTaskScope::Branch(branch())),
                None,
            ),
            (
                MaintenanceTaskKind::Compaction,
                Some(MaintenanceTaskScope::Branch(branch())),
                None,
            ),
            (MaintenanceTaskKind::Materialization, None, None),
            (
                MaintenanceTaskKind::Checkpoint,
                Some(MaintenanceTaskScope::Checkpoint),
                None,
            ),
            (
                MaintenanceTaskKind::FlushWatermark,
                Some(MaintenanceTaskScope::Wal),
                None,
            ),
            (
                MaintenanceTaskKind::Repair,
                Some(MaintenanceTaskScope::Quarantine),
                None,
            ),
        ];
        for (kind, scope, expected) in cases {
            assert_eq!(
                reclaim_family(kind, scope),
                expected,
                "{kind:?} / {scope:?}"
            );
        }
        assert_eq!(
            reclaim_family(
                MaintenanceTaskKind::HealthCollection,
                Some(MaintenanceTaskScope::Global)
            ),
            None
        );
        assert_eq!(
            reclaim_family(
                MaintenanceTaskKind::CachePreheat,
                Some(MaintenanceTaskScope::Global)
            ),
            None
        );
    }

    #[test]
    fn classify_reclaim_outcome_truth_table() {
        let cases = [
            (
                MaintenanceOutcomeStatus::Completed,
                0,
                0,
                ReclaimOutcome::Nothing,
            ),
            (
                MaintenanceOutcomeStatus::Completed,
                1,
                0,
                ReclaimOutcome::Reclaimed,
            ),
            (
                MaintenanceOutcomeStatus::Completed,
                0,
                1,
                ReclaimOutcome::Reclaimed,
            ),
            (
                MaintenanceOutcomeStatus::Completed,
                7,
                3,
                ReclaimOutcome::Reclaimed,
            ),
            (
                MaintenanceOutcomeStatus::Deferred,
                0,
                0,
                ReclaimOutcome::Deferred,
            ),
            (
                MaintenanceOutcomeStatus::Deferred,
                9,
                9,
                ReclaimOutcome::Deferred,
            ),
            (
                MaintenanceOutcomeStatus::Failed,
                0,
                0,
                ReclaimOutcome::Failed,
            ),
            (
                MaintenanceOutcomeStatus::Failed,
                5,
                0,
                ReclaimOutcome::Failed,
            ),
            (
                MaintenanceOutcomeStatus::Canceled,
                0,
                0,
                ReclaimOutcome::Canceled,
            ),
        ];
        for (status, bytes, changes, expected) in cases {
            assert_eq!(
                classify_reclaim_outcome(status, bytes, changes),
                expected,
                "{status:?} bytes={bytes} changes={changes}"
            );
        }
    }

    #[test]
    fn classify_reclaim_carries_the_outcome_facts_and_skips_non_reclaim_tasks() {
        let sweep = MaintenanceOutcome::new(
            MaintenanceTaskKind::Quarantine,
            MaintenanceOutcomeStatus::Completed,
        )
        .with_task_scope(MaintenanceTaskScope::Quarantine)
        .with_effects(4, 4096, false)
        .with_state_changes(2);
        let (family, event) = classify_reclaim(&sweep).expect("sweep is a reclaim family");
        assert_eq!(family, ReclaimFamily::TableObjectSweep);
        assert_eq!(event.outcome(), ReclaimOutcome::Reclaimed);
        assert_eq!(event.bytes_reclaimed(), 4096);
        assert_eq!(event.objects_affected(), 4);
        assert_eq!(event.state_changes(), 2);
        assert_eq!(event.deferral(), None);

        let deferred = MaintenanceOutcome::new(
            MaintenanceTaskKind::Quarantine,
            MaintenanceOutcomeStatus::Deferred,
        )
        .with_task_scope(MaintenanceTaskScope::Quarantine)
        .with_deferral_reason(MaintenanceDeferralReason::ReaderPinned);
        let (_, event) = classify_reclaim(&deferred).expect("deferred sweep");
        assert_eq!(event.outcome(), ReclaimOutcome::Deferred);
        assert_eq!(
            event.deferral(),
            Some(MaintenanceDeferralReason::ReaderPinned)
        );

        let flush = MaintenanceOutcome::new(
            MaintenanceTaskKind::Flush,
            MaintenanceOutcomeStatus::Completed,
        )
        .with_task_scope(MaintenanceTaskScope::Branch(branch()))
        .with_effects(1, 100, false);
        assert_eq!(classify_reclaim(&flush), None);
        assert_eq!(classify_reclaim_follow_up(&flush), None);
    }

    #[test]
    fn a_checkpoint_follow_up_truncation_is_a_wal_truncation_event() {
        let checkpoint = MaintenanceOutcome::new(
            MaintenanceTaskKind::Checkpoint,
            MaintenanceOutcomeStatus::Completed,
        )
        .with_task_scope(MaintenanceTaskScope::Checkpoint);
        // The checkpoint itself is not a reclaim family ...
        assert_eq!(classify_reclaim(&checkpoint), None);
        assert_eq!(classify_reclaim_follow_up(&checkpoint), None);

        // ... but its follow-up truncation is, classified by the segments it deleted.
        let with_deletes = checkpoint
            .clone()
            .with_wal_truncation_follow_up(super::super::WalTruncationFollowUp::new(true, 3));
        let (family, event) = classify_reclaim_follow_up(&with_deletes).expect("follow-up");
        assert_eq!(family, ReclaimFamily::WalTruncation);
        assert_eq!(event.outcome(), ReclaimOutcome::Reclaimed);
        assert_eq!(event.state_changes(), 3);
        assert_eq!(event.objects_affected(), 3);

        let nothing = checkpoint
            .clone()
            .with_wal_truncation_follow_up(super::super::WalTruncationFollowUp::new(true, 0));
        let (_, event) = classify_reclaim_follow_up(&nothing).expect("follow-up");
        assert_eq!(event.outcome(), ReclaimOutcome::Nothing);

        let failed = checkpoint
            .with_wal_truncation_follow_up(super::super::WalTruncationFollowUp::new(false, 1));
        let (_, event) = classify_reclaim_follow_up(&failed).expect("follow-up");
        assert_eq!(event.outcome(), ReclaimOutcome::Failed);
    }

    #[test]
    fn ledger_keeps_the_last_event_per_family_and_running_totals() {
        let mut ledger = ReclaimLedger::default();
        assert_eq!(ledger, ReclaimLedger::default());
        let reclaimed = ReclaimEvent {
            outcome: ReclaimOutcome::Reclaimed,
            deferral: None,
            bytes_reclaimed: 10,
            objects_affected: 1,
            state_changes: 1,
        };
        let deferred = ReclaimEvent {
            outcome: ReclaimOutcome::Deferred,
            deferral: Some(MaintenanceDeferralReason::IncompleteProof),
            bytes_reclaimed: 0,
            objects_affected: 0,
            state_changes: 0,
        };
        let nothing = ReclaimEvent {
            outcome: ReclaimOutcome::Nothing,
            deferral: None,
            bytes_reclaimed: 0,
            objects_affected: 0,
            state_changes: 0,
        };
        ledger.record(ReclaimFamily::TableObjectSweep, reclaimed);
        ledger.record(ReclaimFamily::TableObjectSweep, deferred);
        ledger.record(ReclaimFamily::QuarantinePurge, reclaimed);
        ledger.record(ReclaimFamily::WalTruncation, nothing);

        assert_eq!(ledger.last(ReclaimFamily::TableObjectSweep), Some(deferred));
        assert_eq!(ledger.last(ReclaimFamily::QuarantinePurge), Some(reclaimed));
        assert_eq!(ledger.last(ReclaimFamily::WalTruncation), Some(nothing));
        assert_eq!(ledger.last(ReclaimFamily::TableObjectMark), None);
        assert_eq!(ledger.last(ReclaimFamily::SnapshotPrune), None);
        let totals = ledger.totals();
        assert_eq!(totals.events(), 4);
        assert_eq!(totals.bytes_reclaimed(), 20);
        assert_eq!(totals.reclaimed_passes(), 2);
        assert_eq!(totals.deferred_passes(), 1);
    }
}
