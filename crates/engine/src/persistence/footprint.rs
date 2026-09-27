//! The storage footprint's translation (space-reclamation contract §3.5):
//! storage's diagnostics report types into the engine's D4 footprint types.
//! The only place outside the adapter that names storage types, and the
//! only place the mapping is written.

use strata_storage::api::{
    DiagnosticsDetail, DiagnosticsFactState, DiagnosticsOutcome, DiagnosticsReclaimDeferral,
    DiagnosticsReclaimOutcome, DiagnosticsReclaimPass, DiagnosticsScope,
};

use crate::api::{
    FootprintDetail, ReclaimDeferralReason, ReclaimOutcome, ReclaimPass, StorageFootprint,
    StorageReclaimStatus,
};
use crate::diagnostics::EngineError;

use super::StoragePersistence;

impl StoragePersistence {
    /// The database's footprint at the requested tier, or `None` when the
    /// runtime holds no durable objects to measure (cache mode).
    pub(crate) fn storage_footprint(
        &self,
        detail: FootprintDetail,
    ) -> Result<Option<StorageFootprint>, EngineError> {
        let outcome =
            self.storage_diagnostics(DiagnosticsScope::Global, diagnostics_detail_for(detail))?;
        Ok(storage_footprint_from_diagnostics(&outcome, detail))
    }
}

/// The storage tier a footprint request gathers at.
pub(crate) const fn diagnostics_detail_for(detail: FootprintDetail) -> DiagnosticsDetail {
    match detail {
        FootprintDetail::Live => DiagnosticsDetail::Live,
        FootprintDetail::Audit => DiagnosticsDetail::Audit,
    }
}

/// The engine's name for a storage reclaim outcome; `None` for a variant this
/// build does not know.
pub(crate) const fn reclaim_outcome_from_storage(
    outcome: DiagnosticsReclaimOutcome,
) -> Option<ReclaimOutcome> {
    match outcome {
        DiagnosticsReclaimOutcome::Reclaimed => Some(ReclaimOutcome::Reclaimed),
        DiagnosticsReclaimOutcome::Nothing => Some(ReclaimOutcome::Nothing),
        DiagnosticsReclaimOutcome::Deferred => Some(ReclaimOutcome::Deferred),
        DiagnosticsReclaimOutcome::Failed => Some(ReclaimOutcome::Failed),
        DiagnosticsReclaimOutcome::Canceled => Some(ReclaimOutcome::Canceled),
        _ => None,
    }
}

/// The engine's name for a storage reclaim deferral; `None` for a variant
/// this build does not know.
pub(crate) const fn reclaim_deferral_from_storage(
    deferral: DiagnosticsReclaimDeferral,
) -> Option<ReclaimDeferralReason> {
    match deferral {
        DiagnosticsReclaimDeferral::ReaderPinned => Some(ReclaimDeferralReason::ReaderPinned),
        DiagnosticsReclaimDeferral::Referenced => Some(ReclaimDeferralReason::Referenced),
        DiagnosticsReclaimDeferral::IncompleteProof => Some(ReclaimDeferralReason::IncompleteProof),
        DiagnosticsReclaimDeferral::StaleProof => Some(ReclaimDeferralReason::StaleProof),
        DiagnosticsReclaimDeferral::RecoveryHealth => Some(ReclaimDeferralReason::RecoveryHealth),
        DiagnosticsReclaimDeferral::InventoryAdvanced => {
            Some(ReclaimDeferralReason::InventoryAdvanced)
        }
        DiagnosticsReclaimDeferral::UnsupportedScope => {
            Some(ReclaimDeferralReason::UnsupportedScope)
        }
        _ => None,
    }
}

/// One storage reclaim pass in engine terms; `None` when its outcome is a
/// variant this build does not know.
pub(crate) fn reclaim_pass_from_storage(pass: DiagnosticsReclaimPass) -> Option<ReclaimPass> {
    let outcome = reclaim_outcome_from_storage(pass.outcome())?;
    Some(ReclaimPass {
        outcome,
        deferral: pass.deferral().and_then(reclaim_deferral_from_storage),
        bytes_reclaimed: pass.bytes_reclaimed(),
        objects_affected: count(pass.objects_affected()),
        state_changes: count(pass.state_changes()),
    })
}

/// The WAL's bytes on disk: the audit's listing-backed split (reclaimable
/// plus tail, every segment) when it was gathered, else the runtime's cached
/// retained bytes (which include the active segment and are cold until the
/// first commit of a session).
pub(crate) fn footprint_wal_bytes(
    wal_reclaimable_bytes: Option<u64>,
    wal_tail_bytes: Option<u64>,
    wal_retained_bytes: Option<u64>,
) -> Option<u64> {
    match (wal_reclaimable_bytes, wal_tail_bytes) {
        (Some(reclaimable), Some(tail)) => Some(reclaimable.saturating_add(tail)),
        _ => wal_retained_bytes,
    }
}

/// Every byte on disk — catalogued tables, unreferenced and quarantined
/// objects, snapshots, and the WAL — when every part is known.
pub(crate) fn footprint_total_bytes(
    live_table_bytes: u64,
    unreferenced_bytes: Option<u64>,
    quarantined_bytes: Option<u64>,
    snapshot_bytes: Option<u64>,
    wal_bytes: Option<u64>,
) -> Option<u64> {
    Some(
        live_table_bytes
            .saturating_add(unreferenced_bytes?)
            .saturating_add(quarantined_bytes?)
            .saturating_add(snapshot_bytes?)
            .saturating_add(wal_bytes?),
    )
}

/// A storage count on the wire-friendly width.
fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// The footprint a diagnostics outcome carries, or `None` when the runtime
/// holds no durable objects to measure (cache mode) or could not answer.
pub(crate) fn storage_footprint_from_diagnostics(
    outcome: &DiagnosticsOutcome,
    detail: FootprintDetail,
) -> Option<StorageFootprint> {
    let footprint = outcome.footprint();
    if footprint.state() != DiagnosticsFactState::Known {
        return None;
    }
    let quarantine = outcome.quarantine();
    let reclaim = outcome.reclaim();
    let live_table_bytes = footprint.live_table_bytes()?;
    let unreferenced_bytes = footprint.unreferenced_bytes();
    let quarantined_bytes = quarantine.quarantined_bytes();
    let snapshot_bytes = footprint.snapshot_bytes();
    let wal_retained_bytes = footprint.wal_retained_bytes();
    let wal_active_bytes = footprint.wal_active_bytes();
    let wal_reclaimable_bytes = footprint.wal_reclaimable_bytes();
    let wal_tail_bytes = footprint.wal_tail_bytes();
    Some(StorageFootprint {
        detail,
        live_table_objects: count(footprint.live_table_objects()?),
        live_table_bytes,
        wal_retained_bytes,
        wal_active_bytes,
        wal_retained_segments: footprint.wal_retained_segments().map(count),
        wal_retention_watermark: footprint.wal_retention_watermark(),
        unreferenced_objects: footprint.unreferenced_objects().map(count),
        unreferenced_bytes,
        quarantined_objects: quarantine.quarantined_objects().map(count),
        quarantined_bytes,
        snapshot_objects: footprint.snapshot_objects().map(count),
        snapshot_bytes,
        superseded_snapshots: footprint.superseded_snapshots().map(count),
        superseded_snapshot_bytes: footprint.superseded_snapshot_bytes(),
        wal_reclaimable_bytes,
        wal_tail_bytes,
        total_bytes: footprint_total_bytes(
            live_table_bytes,
            unreferenced_bytes,
            quarantined_bytes,
            snapshot_bytes,
            footprint_wal_bytes(wal_reclaimable_bytes, wal_tail_bytes, wal_retained_bytes),
        ),
        reclaim: StorageReclaimStatus {
            last_mark: reclaim.last_mark().and_then(reclaim_pass_from_storage),
            last_sweep: reclaim.last_sweep().and_then(reclaim_pass_from_storage),
            last_purge: reclaim.last_purge().and_then(reclaim_pass_from_storage),
            last_snapshot_prune: reclaim
                .last_snapshot_prune()
                .and_then(reclaim_pass_from_storage),
            last_wal_truncation: reclaim
                .last_wal_truncation()
                .and_then(reclaim_pass_from_storage),
            total_passes: reclaim.total_passes(),
            total_bytes_reclaimed: reclaim.total_bytes_reclaimed(),
            reclaimed_passes: reclaim.reclaimed_passes(),
            deferred_passes: reclaim.deferred_passes(),
            pending_reclaim_tasks: reclaim.pending_reclaim_tasks().map(count),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_detail_for_truth_table() {
        assert_eq!(
            diagnostics_detail_for(FootprintDetail::Live),
            DiagnosticsDetail::Live
        );
        assert_eq!(
            diagnostics_detail_for(FootprintDetail::Audit),
            DiagnosticsDetail::Audit
        );
        assert_eq!(FootprintDetail::default(), FootprintDetail::Live);
    }

    #[test]
    fn reclaim_outcome_from_storage_truth_table() {
        for (storage, expected) in [
            (
                DiagnosticsReclaimOutcome::Reclaimed,
                ReclaimOutcome::Reclaimed,
            ),
            (DiagnosticsReclaimOutcome::Nothing, ReclaimOutcome::Nothing),
            (
                DiagnosticsReclaimOutcome::Deferred,
                ReclaimOutcome::Deferred,
            ),
            (DiagnosticsReclaimOutcome::Failed, ReclaimOutcome::Failed),
            (
                DiagnosticsReclaimOutcome::Canceled,
                ReclaimOutcome::Canceled,
            ),
        ] {
            assert_eq!(reclaim_outcome_from_storage(storage), Some(expected));
        }
    }

    #[test]
    fn reclaim_deferral_from_storage_truth_table() {
        for (storage, expected) in [
            (
                DiagnosticsReclaimDeferral::ReaderPinned,
                ReclaimDeferralReason::ReaderPinned,
            ),
            (
                DiagnosticsReclaimDeferral::Referenced,
                ReclaimDeferralReason::Referenced,
            ),
            (
                DiagnosticsReclaimDeferral::IncompleteProof,
                ReclaimDeferralReason::IncompleteProof,
            ),
            (
                DiagnosticsReclaimDeferral::StaleProof,
                ReclaimDeferralReason::StaleProof,
            ),
            (
                DiagnosticsReclaimDeferral::RecoveryHealth,
                ReclaimDeferralReason::RecoveryHealth,
            ),
            (
                DiagnosticsReclaimDeferral::InventoryAdvanced,
                ReclaimDeferralReason::InventoryAdvanced,
            ),
            (
                DiagnosticsReclaimDeferral::UnsupportedScope,
                ReclaimDeferralReason::UnsupportedScope,
            ),
        ] {
            assert_eq!(reclaim_deferral_from_storage(storage), Some(expected));
        }
    }

    #[test]
    fn footprint_wal_bytes_truth_table() {
        assert_eq!(footprint_wal_bytes(Some(10), Some(5), Some(99)), Some(15));
        assert_eq!(footprint_wal_bytes(Some(10), Some(5), None), Some(15));
        assert_eq!(footprint_wal_bytes(None, Some(5), Some(99)), Some(99));
        assert_eq!(footprint_wal_bytes(Some(10), None, Some(99)), Some(99));
        assert_eq!(footprint_wal_bytes(None, None, None), None);
        assert_eq!(
            footprint_wal_bytes(Some(u64::MAX), Some(1), None),
            Some(u64::MAX),
            "the split saturates"
        );
    }

    #[test]
    fn footprint_total_bytes_truth_table() {
        assert_eq!(
            footprint_total_bytes(100, Some(20), Some(3), Some(400), Some(5000)),
            Some(5523)
        );
        assert_eq!(
            footprint_total_bytes(0, Some(0), Some(0), Some(0), Some(0)),
            Some(0)
        );
        assert_eq!(
            footprint_total_bytes(u64::MAX, Some(1), Some(1), Some(1), Some(1)),
            Some(u64::MAX),
            "the total saturates"
        );
        for missing in 0..4 {
            let part = |index: usize| (index != missing).then_some(7);
            assert_eq!(
                footprint_total_bytes(1, part(0), part(1), part(2), part(3)),
                None,
                "an unknown part {missing} leaves the total unknown"
            );
        }
    }

    #[test]
    fn count_maps_to_the_wire_width() {
        assert_eq!(count(0), 0);
        assert_eq!(count(7), 7);
        assert_eq!(
            count(usize::MAX),
            u64::try_from(usize::MAX).unwrap_or(u64::MAX)
        );
    }
}
