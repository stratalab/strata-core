//! Storage footprint (space-reclamation contract §3.5): the on-disk facts the
//! storage layer computes for a durable database, surfaced through one
//! canonical engine path. The facts are database-global — every branch's
//! tables share one catalogue, one WAL and one reclaim ledger — so the branch
//! a caller names only has to exist. The persistence layer owns the
//! translation from storage's own report types.

use strata_core::CommitVersion;

use crate::diagnostics::EngineError;
use crate::persistence::StoragePersistence;

/// The registered code a footprint request raises on a database that holds
/// no durable objects (cache mode).
const UNSUPPORTED_CODE: &str = "unsupported.engine.persistence_capability";

/// How much of the footprint to gather.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum FootprintDetail {
    /// The facts the runtime already holds — live table bytes, WAL retained
    /// and active bytes, the reclaim ledger. No backend I/O.
    #[default]
    Live,
    /// Adds the listings and stats the reclaim runners perform: unreferenced
    /// and quarantined objects, snapshot objects and the superseded set, the
    /// WAL's reclaimable-versus-tail split.
    Audit,
}

/// How a reclaim pass ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReclaimOutcome {
    /// The pass released bytes.
    Reclaimed,
    /// The pass ran and found nothing to release.
    Nothing,
    /// The pass deferred; [`ReclaimDeferralReason`] says why.
    Deferred,
    /// The pass failed.
    Failed,
    /// The pass was canceled before it ran.
    Canceled,
}

/// Why a reclaim pass deferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReclaimDeferralReason {
    /// A reader still pins the objects.
    ReaderPinned,
    /// The objects are still referenced.
    Referenced,
    /// The retention proof was incomplete.
    IncompleteProof,
    /// The retention proof was stale.
    StaleProof,
    /// Recovery health forbids reclaim.
    RecoveryHealth,
    /// The quarantine inventory advanced under the pass.
    InventoryAdvanced,
    /// The requested scope is not supported.
    UnsupportedScope,
}

/// One recorded reclaim pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReclaimPass {
    /// How the pass ended.
    pub outcome: ReclaimOutcome,
    /// Why it deferred, when it did.
    pub deferral: Option<ReclaimDeferralReason>,
    /// Bytes the pass reported: for a sweep, the bytes it moved into
    /// quarantine; for every other family, the bytes it removed from disk.
    pub bytes_reclaimed: u64,
    /// Objects the pass touched.
    pub objects_affected: u64,
    /// Durable state changes the pass made.
    pub state_changes: u64,
}

/// The reclaim ledger: the last pass of every reclaim family, running totals
/// and the reclaim work still queued.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StorageReclaimStatus {
    /// The last table-object mark.
    pub last_mark: Option<ReclaimPass>,
    /// The last quarantine sweep.
    pub last_sweep: Option<ReclaimPass>,
    /// The last quarantine purge.
    pub last_purge: Option<ReclaimPass>,
    /// The last snapshot prune.
    pub last_snapshot_prune: Option<ReclaimPass>,
    /// The last WAL truncation.
    pub last_wal_truncation: Option<ReclaimPass>,
    /// Passes recorded since the database opened.
    pub total_passes: u64,
    /// Bytes removed from disk since the database opened: purges, snapshot
    /// prunes and WAL truncations. A sweep only moves bytes into quarantine,
    /// so they are counted once, when the purge deletes them.
    pub total_bytes_reclaimed: u64,
    /// Passes that reclaimed something.
    pub reclaimed_passes: u64,
    /// Passes that deferred.
    pub deferred_passes: u64,
    /// Reclaim tasks still queued, when known.
    pub pending_reclaim_tasks: Option<u64>,
}

/// The database's on-disk footprint. Audit-tier fields are `None` on a
/// [`FootprintDetail::Live`] request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StorageFootprint {
    /// The tier the facts were gathered at.
    pub detail: FootprintDetail,
    /// Table objects in the table family: every object a manifest or a live
    /// branch still uses, plus any a compaction superseded that the reclaim
    /// sweep has not yet moved to quarantine (the audit counts those under
    /// `unreferenced_*` as well).
    pub live_table_objects: u64,
    /// Bytes of those table objects.
    pub live_table_bytes: u64,
    /// Bytes of the retained WAL segments, including the active one, when the
    /// runtime's growth facts are warm.
    pub wal_retained_bytes: Option<u64>,
    /// Bytes of the active WAL segment, when known.
    pub wal_active_bytes: Option<u64>,
    /// Retained WAL segments, including the active one, when known.
    pub wal_retained_segments: Option<u64>,
    /// The version at and below which the WAL is reclaimable, when known.
    pub wal_retention_watermark: Option<CommitVersion>,
    /// Table objects referenced by nothing (audit).
    pub unreferenced_objects: Option<u64>,
    /// Bytes of those objects (audit).
    pub unreferenced_bytes: Option<u64>,
    /// Objects held in quarantine (audit).
    pub quarantined_objects: Option<u64>,
    /// Bytes of those objects (audit).
    pub quarantined_bytes: Option<u64>,
    /// Checkpoint snapshot objects on disk (audit).
    pub snapshot_objects: Option<u64>,
    /// Bytes of those snapshots (audit).
    pub snapshot_bytes: Option<u64>,
    /// Snapshots the attested one supersedes (audit).
    pub superseded_snapshots: Option<u64>,
    /// Bytes of those snapshots (audit).
    pub superseded_snapshot_bytes: Option<u64>,
    /// WAL bytes below the retention watermark a truncation may delete (audit).
    pub wal_reclaimable_bytes: Option<u64>,
    /// WAL bytes above the retention watermark (audit).
    pub wal_tail_bytes: Option<u64>,
    /// Every byte the database holds on disk — table objects, quarantined
    /// objects, snapshots and the WAL — when every part is known (audit; the
    /// WAL from the audit's own listing, so a read-only open with cold live
    /// facts still totals). Between a compaction and the sweep that follows
    /// it, a superseded object counts both as a table object and as
    /// unreferenced.
    pub total_bytes: Option<u64>,
    /// The reclaim ledger.
    pub reclaim: StorageReclaimStatus,
}

/// The one canonical path: the persistence layer's footprint at the
/// requested tier. A database without durable objects (cache mode) is
/// refused with the registered unsupported code.
pub(crate) fn storage_footprint(
    persistence: &StoragePersistence,
    detail: FootprintDetail,
) -> Result<StorageFootprint, EngineError> {
    persistence.storage_footprint(detail)?.ok_or_else(|| {
        EngineError::unsupported(
            UNSUPPORTED_CODE,
            "the storage footprint needs a durable database; a cache database holds no durable objects",
        )
    })
}
