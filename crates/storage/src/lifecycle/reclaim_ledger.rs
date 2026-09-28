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
use strata_core::CommitVersion;

/// What woke the background worker for a drain round (space-reclamation
/// contract §3.1, slice 8). The reclaim ledger records the open-time and
/// idle-time wakes; ordinary wakes (commits, enqueues, growth triggers) are
/// the steady state and are not recorded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimWakeOrigin {
    /// A commit, an enqueue, a growth trigger or a drain's own re-arm.
    Ordinary,
    /// The reclaim-only wake armed at open (slice 4).
    Open,
    /// The quiescence wake armed after a drain round ended with reclaim owed.
    Idle,
}

/// How a reclaim pass moves the runtime's "reclaim owed" fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimOwedTransition {
    /// Debt remains (or was just discovered): a sweep is still due.
    Owed,
    /// A sweep proved the table-object family clean: nothing is owed.
    Clear,
    /// The pass says nothing about table-object debt.
    Unchanged,
}

/// The single rule that moves `reclaim_owed`. A mark always chains a sweep
/// (owed until the sweep proves otherwise); a sweep that completes with no
/// candidates and no faults is the one clean signal; a deferred, failed,
/// capped or faulting sweep leaves the debt owed. The purge, the snapshot
/// prune and the WAL truncation are not table-object evidence.
pub(crate) const fn reclaim_owed_transition(
    family: ReclaimFamily,
    outcome: ReclaimOutcome,
    faults: usize,
) -> ReclaimOwedTransition {
    match (family, outcome) {
        (ReclaimFamily::TableObjectSweep, ReclaimOutcome::Nothing) => {
            if faults == 0 {
                ReclaimOwedTransition::Clear
            } else {
                ReclaimOwedTransition::Owed
            }
        }
        (ReclaimFamily::TableObjectMark | ReclaimFamily::TableObjectSweep, _) => {
            ReclaimOwedTransition::Owed
        }
        (
            ReclaimFamily::QuarantinePurge
            | ReclaimFamily::SnapshotPrune
            | ReclaimFamily::WalTruncation,
            _,
        ) => ReclaimOwedTransition::Unchanged,
    }
}

/// #3645: whether reclaim is waiting on a held reader — debt is owed and the
/// last table-object sweep deferred because a reader pinned the manifest. Only
/// then can a reader's release be the event that unblocks reclaim.
pub(crate) fn reclaim_waits_on_reader(owed: bool, last_sweep: Option<ReclaimPass>) -> bool {
    owed && last_sweep.is_some_and(|pass| {
        pass.outcome() == ReclaimOutcome::Deferred
            && pass.deferral() == Some(MaintenanceDeferralReason::ReaderPinned)
    })
}

/// #3619: whether a family's reported bytes leave the disk. The sweep only
/// moves an object into quarantine (its bytes are freed later by the purge,
/// which reports them again), so counting it too would report the same space
/// twice; the mark frees nothing.
pub(crate) const fn family_frees_disk(family: ReclaimFamily) -> bool {
    match family {
        ReclaimFamily::QuarantinePurge
        | ReclaimFamily::SnapshotPrune
        | ReclaimFamily::WalTruncation => true,
        ReclaimFamily::TableObjectMark | ReclaimFamily::TableObjectSweep => false,
    }
}

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
    /// The pass deferred; `ReclaimPass::deferral` says why when the runner
    /// classified it.
    Deferred,
    Failed,
    Canceled,
}

/// One recorded reclaim pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReclaimPass {
    outcome: ReclaimOutcome,
    deferral: Option<MaintenanceDeferralReason>,
    bytes_reclaimed: u64,
    objects_affected: usize,
    state_changes: usize,
}

impl ReclaimPass {
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
    passes: u64,
    bytes_reclaimed: u64,
    reclaimed_passes: u64,
    deferred_passes: u64,
}

impl ReclaimTotals {
    pub(crate) const fn passes(self) -> u64 {
        self.passes
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
    last_mark: Option<ReclaimPass>,
    last_sweep: Option<ReclaimPass>,
    last_purge: Option<ReclaimPass>,
    last_snapshot_prune: Option<ReclaimPass>,
    last_wal_truncation: Option<ReclaimPass>,
    totals: ReclaimTotals,
    /// The visible version when the open-time reclaim wake's round started.
    last_open_wake: Option<CommitVersion>,
    /// The visible version when the last idle (quiescence) wake's round started.
    last_idle_wake: Option<CommitVersion>,
    open_wakes: u64,
    idle_wakes: u64,
}

impl ReclaimLedger {
    /// Record a drain round's wake origin (slice 8). Ordinary wakes are the
    /// steady state and leave the ledger untouched.
    pub(crate) fn record_wake(&mut self, origin: ReclaimWakeOrigin, at_version: CommitVersion) {
        match origin {
            ReclaimWakeOrigin::Ordinary => {}
            ReclaimWakeOrigin::Open => {
                self.last_open_wake = Some(at_version);
                self.open_wakes = self.open_wakes.saturating_add(1);
            }
            ReclaimWakeOrigin::Idle => {
                self.last_idle_wake = Some(at_version);
                self.idle_wakes = self.idle_wakes.saturating_add(1);
            }
        }
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the footprint surface (slice 9) reports the wakes"
        )
    )]
    pub(crate) const fn last_open_wake(&self) -> Option<CommitVersion> {
        self.last_open_wake
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the footprint surface (slice 9) reports the wakes"
        )
    )]
    pub(crate) const fn last_idle_wake(&self) -> Option<CommitVersion> {
        self.last_idle_wake
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the footprint surface (slice 9) reports the wakes"
        )
    )]
    pub(crate) const fn open_wakes(&self) -> u64 {
        self.open_wakes
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the footprint surface (slice 9) reports the wakes"
        )
    )]
    pub(crate) const fn idle_wakes(&self) -> u64 {
        self.idle_wakes
    }

    pub(crate) fn record(&mut self, family: ReclaimFamily, event: ReclaimPass) {
        let slot = match family {
            ReclaimFamily::TableObjectMark => &mut self.last_mark,
            ReclaimFamily::TableObjectSweep => &mut self.last_sweep,
            ReclaimFamily::QuarantinePurge => &mut self.last_purge,
            ReclaimFamily::SnapshotPrune => &mut self.last_snapshot_prune,
            ReclaimFamily::WalTruncation => &mut self.last_wal_truncation,
        };
        *slot = Some(event);
        self.totals.passes = self.totals.passes.saturating_add(1);
        if family_frees_disk(family) {
            self.totals.bytes_reclaimed = self
                .totals
                .bytes_reclaimed
                .saturating_add(event.bytes_reclaimed);
        }
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

    pub(crate) const fn last(&self, family: ReclaimFamily) -> Option<ReclaimPass> {
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
) -> Option<(ReclaimFamily, ReclaimPass)> {
    let follow_up = outcome.wal_truncation_follow_up()?;
    let status = if follow_up.completed() {
        MaintenanceOutcomeStatus::Completed
    } else {
        MaintenanceOutcomeStatus::Failed
    };
    Some((
        ReclaimFamily::WalTruncation,
        ReclaimPass {
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
) -> Option<(ReclaimFamily, ReclaimPass)> {
    let family = reclaim_family(outcome.task_kind(), outcome.task_scope())?;
    let event = ReclaimPass {
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
        let reclaimed = ReclaimPass {
            outcome: ReclaimOutcome::Reclaimed,
            deferral: None,
            bytes_reclaimed: 10,
            objects_affected: 1,
            state_changes: 1,
        };
        let deferred = ReclaimPass {
            outcome: ReclaimOutcome::Deferred,
            deferral: Some(MaintenanceDeferralReason::IncompleteProof),
            bytes_reclaimed: 0,
            objects_affected: 0,
            state_changes: 0,
        };
        let nothing = ReclaimPass {
            outcome: ReclaimOutcome::Nothing,
            deferral: None,
            bytes_reclaimed: 0,
            objects_affected: 0,
            state_changes: 0,
        };
        // The empty ledger reports zero everywhere (a constant-returning
        // accessor would be caught here and by the counts below).
        let empty = ledger.totals();
        assert_eq!(empty.passes(), 0);
        assert_eq!(empty.bytes_reclaimed(), 0);
        assert_eq!(empty.reclaimed_passes(), 0);
        assert_eq!(empty.deferred_passes(), 0);

        ledger.record(ReclaimFamily::TableObjectSweep, reclaimed);
        ledger.record(ReclaimFamily::TableObjectSweep, deferred);
        ledger.record(ReclaimFamily::QuarantinePurge, reclaimed);
        ledger.record(ReclaimFamily::WalTruncation, nothing);
        ledger.record(ReclaimFamily::SnapshotPrune, deferred);

        assert_eq!(ledger.last(ReclaimFamily::TableObjectSweep), Some(deferred));
        assert_eq!(ledger.last(ReclaimFamily::QuarantinePurge), Some(reclaimed));
        assert_eq!(ledger.last(ReclaimFamily::WalTruncation), Some(nothing));
        assert_eq!(ledger.last(ReclaimFamily::SnapshotPrune), Some(deferred));
        assert_eq!(ledger.last(ReclaimFamily::TableObjectMark), None);
        let totals = ledger.totals();
        assert_eq!(totals.passes(), 5);
        // #3619: only the families that free disk count toward the total; the
        // sweep's staged bytes are counted once, by the purge.
        assert_eq!(totals.bytes_reclaimed(), 10);
        assert_eq!(totals.reclaimed_passes(), 2);
        assert_eq!(totals.deferred_passes(), 2);
    }

    /// #3619: which families' bytes leave the disk.
    #[test]
    fn family_frees_disk_truth_table() {
        assert!(!family_frees_disk(ReclaimFamily::TableObjectMark));
        assert!(!family_frees_disk(ReclaimFamily::TableObjectSweep));
        assert!(family_frees_disk(ReclaimFamily::QuarantinePurge));
        assert!(family_frees_disk(ReclaimFamily::SnapshotPrune));
        assert!(family_frees_disk(ReclaimFamily::WalTruncation));
    }

    /// #3645: reclaim waits on a reader only when debt is owed and the last
    /// sweep deferred because a reader pinned the manifest.
    #[test]
    fn reclaim_waits_on_reader_truth_table() {
        let sweep = |status, reason: Option<MaintenanceDeferralReason>| {
            let outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Quarantine, status)
                .with_task_scope(MaintenanceTaskScope::Quarantine);
            let outcome = match reason {
                Some(reason) => outcome.with_deferral_reason(reason),
                None => outcome,
            };
            classify_reclaim(&outcome)
                .expect("sweep is a reclaim family")
                .1
        };
        let reader = sweep(
            MaintenanceOutcomeStatus::Deferred,
            Some(MaintenanceDeferralReason::ReaderPinned),
        );
        let referenced = sweep(
            MaintenanceOutcomeStatus::Deferred,
            Some(MaintenanceDeferralReason::Referenced),
        );
        let unclassified = sweep(MaintenanceOutcomeStatus::Deferred, None);
        let completed = sweep(MaintenanceOutcomeStatus::Completed, None);
        assert_eq!(
            reader.deferral(),
            Some(MaintenanceDeferralReason::ReaderPinned)
        );
        for (owed, last, expected) in [
            (true, Some(reader), true),
            (false, Some(reader), false),
            (true, Some(referenced), false),
            (true, Some(unclassified), false),
            (true, Some(completed), false),
            (true, None, false),
            (false, None, false),
        ] {
            assert_eq!(
                super::reclaim_waits_on_reader(owed, last),
                expected,
                "owed={owed} last={last:?}"
            );
        }
    }

    /// Space-reclamation contract §3.1 (slice 8): the one rule that moves
    /// `reclaim_owed`.
    #[test]
    fn reclaim_owed_transition_truth_table() {
        use ReclaimOwedTransition::{Clear, Owed, Unchanged};
        for (family, outcome, faults, expected) in [
            (
                ReclaimFamily::TableObjectMark,
                ReclaimOutcome::Nothing,
                0,
                Owed,
            ),
            (
                ReclaimFamily::TableObjectMark,
                ReclaimOutcome::Reclaimed,
                0,
                Owed,
            ),
            (
                ReclaimFamily::TableObjectMark,
                ReclaimOutcome::Deferred,
                0,
                Owed,
            ),
            (
                ReclaimFamily::TableObjectSweep,
                ReclaimOutcome::Nothing,
                0,
                Clear,
            ),
            (
                ReclaimFamily::TableObjectSweep,
                ReclaimOutcome::Nothing,
                1,
                Owed,
            ),
            (
                ReclaimFamily::TableObjectSweep,
                ReclaimOutcome::Reclaimed,
                0,
                Owed,
            ),
            (
                ReclaimFamily::TableObjectSweep,
                ReclaimOutcome::Deferred,
                0,
                Owed,
            ),
            (
                ReclaimFamily::TableObjectSweep,
                ReclaimOutcome::Failed,
                0,
                Owed,
            ),
            (
                ReclaimFamily::TableObjectSweep,
                ReclaimOutcome::Canceled,
                0,
                Owed,
            ),
            (
                ReclaimFamily::QuarantinePurge,
                ReclaimOutcome::Reclaimed,
                0,
                Unchanged,
            ),
            (
                ReclaimFamily::QuarantinePurge,
                ReclaimOutcome::Nothing,
                0,
                Unchanged,
            ),
            (
                ReclaimFamily::SnapshotPrune,
                ReclaimOutcome::Reclaimed,
                0,
                Unchanged,
            ),
            (
                ReclaimFamily::WalTruncation,
                ReclaimOutcome::Deferred,
                0,
                Unchanged,
            ),
        ] {
            assert_eq!(
                reclaim_owed_transition(family, outcome, faults),
                expected,
                "family={family:?} outcome={outcome:?} faults={faults}"
            );
        }
    }

    /// Only the open and idle wakes are ledger facts; an ordinary wake leaves
    /// the ledger untouched.
    #[test]
    fn record_wake_keeps_the_open_and_idle_wakes_only() {
        let mut ledger = ReclaimLedger::default();
        assert_eq!(ledger.last_open_wake(), None);
        assert_eq!(ledger.last_idle_wake(), None);

        ledger.record_wake(ReclaimWakeOrigin::Ordinary, CommitVersion::new(3));
        assert_eq!(ledger.last_open_wake(), None);
        assert_eq!(ledger.last_idle_wake(), None);
        assert_eq!((ledger.open_wakes(), ledger.idle_wakes()), (0, 0));

        ledger.record_wake(ReclaimWakeOrigin::Open, CommitVersion::new(4));
        assert_eq!(ledger.last_open_wake(), Some(CommitVersion::new(4)));
        assert_eq!(ledger.last_idle_wake(), None);
        assert_eq!((ledger.open_wakes(), ledger.idle_wakes()), (1, 0));

        ledger.record_wake(ReclaimWakeOrigin::Idle, CommitVersion::new(5));
        ledger.record_wake(ReclaimWakeOrigin::Idle, CommitVersion::new(6));
        assert_eq!(ledger.last_open_wake(), Some(CommitVersion::new(4)));
        assert_eq!(ledger.last_idle_wake(), Some(CommitVersion::new(6)));
        assert_eq!((ledger.open_wakes(), ledger.idle_wakes()), (1, 2));
        assert_eq!(ledger.totals().passes(), 0, "wakes are not reclaim passes");
    }
}
