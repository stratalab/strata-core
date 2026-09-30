//! Space-reclamation contract §3.5 (slice 2): the durable runtime's footprint
//! facts. The live tier reads only state the runtime already holds — the
//! table catalog, the WAL service's sealed-retention cache, the retention
//! watermark cache and the maintenance queue — and never touches the backend.
//! The audit tier performs the listings and stats the reclaim runners would:
//! a report-only table-object mark carrying the sweep's own pins, the snapshot
//! family with sizes, the quarantine inventories, and the WAL segments
//! classified exactly as a covered-segment delete pass would.

use std::collections::BTreeMap;

use strata_core::CommitVersion;

use super::bootstrap::CachedRetentionWatermark;
use super::maintenance::{
    durable_quarantine_service_error, manifest_error, snapshot_error,
    table_object_retention_request, wal_error,
};
use super::LifecycleDurableLocalRuntime;
use crate::lifecycle::reclaim_ledger::reclaim_family;
use crate::lifecycle::{
    table_object_retention_outcome, wal_retention_watermark, LifecycleResult,
    LifecycleRetentionStatus, RecoveryHealth, RetentionDecision,
};
use crate::object::ObjectName;
use crate::service::{superseded_snapshot, WalGrowthFacts};

/// The retention watermark as the runtime's cache knows it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleFootprintWatermark {
    /// The cache is cold (never read, or invalidated by a checkpoint or flush
    /// completion); the live tier does not read the manifest to warm it.
    Cold,
    /// The cached watermark; `None` for a store that never checkpointed or
    /// flushed.
    Known(Option<CommitVersion>),
}

/// Live-tier facts: everything the runtime knows without backend I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleFootprintLiveFacts {
    live_table_objects: usize,
    live_table_bytes: u64,
    /// `None` while the WAL service's sealed-retention cache is cold.
    wal: Option<WalGrowthFacts>,
    wal_retention_watermark: LifecycleFootprintWatermark,
    pending_reclaim_tasks: usize,
}

impl LifecycleFootprintLiveFacts {
    pub(crate) const fn live_table_objects(&self) -> usize {
        self.live_table_objects
    }

    pub(crate) const fn live_table_bytes(&self) -> u64 {
        self.live_table_bytes
    }

    pub(crate) const fn wal(&self) -> Option<WalGrowthFacts> {
        self.wal
    }

    pub(crate) const fn wal_retention_watermark(&self) -> LifecycleFootprintWatermark {
        self.wal_retention_watermark
    }

    pub(crate) const fn pending_reclaim_tasks(&self) -> usize {
        self.pending_reclaim_tasks
    }
}

/// Audit-tier facts: the listings and stats the reclaim runners would perform.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleFootprintAuditFacts {
    /// `(objects, bytes)` the mark classified as unreferenced; `None` when the
    /// mark's proof was not complete (nothing can be called unreferenced).
    unreferenced: Option<(usize, u64)>,
    /// #3646: the part of `unreferenced` the table catalogue still records
    /// (a superseded input the sweep has not moved yet). The Live tier counts
    /// these too, so the audit subtracts them to keep the families disjoint.
    catalogued_unreferenced: (usize, u64),
    quarantined_objects: usize,
    quarantined_bytes: u64,
    snapshot_objects: usize,
    snapshot_bytes: u64,
    superseded_snapshots: usize,
    superseded_snapshot_bytes: u64,
    /// #3643: the sealed timeline segments on disk, and the part the next
    /// `Superseded` snapshot prune would delete.
    timeline_segments: (usize, u64),
    superseded_timeline_segments: (usize, u64),
    wal_reclaimable_bytes: u64,
    wal_tail_bytes: u64,
}

impl LifecycleFootprintAuditFacts {
    pub(crate) const fn timeline_segments(&self) -> (usize, u64) {
        self.timeline_segments
    }

    pub(crate) const fn superseded_timeline_segments(&self) -> (usize, u64) {
        self.superseded_timeline_segments
    }

    pub(crate) const fn unreferenced(&self) -> Option<(usize, u64)> {
        self.unreferenced
    }

    pub(crate) const fn catalogued_unreferenced(&self) -> (usize, u64) {
        self.catalogued_unreferenced
    }

    pub(crate) const fn quarantined_objects(&self) -> usize {
        self.quarantined_objects
    }

    pub(crate) const fn quarantined_bytes(&self) -> u64 {
        self.quarantined_bytes
    }

    pub(crate) const fn snapshot_objects(&self) -> usize {
        self.snapshot_objects
    }

    pub(crate) const fn snapshot_bytes(&self) -> u64 {
        self.snapshot_bytes
    }

    pub(crate) const fn superseded_snapshots(&self) -> usize {
        self.superseded_snapshots
    }

    pub(crate) const fn superseded_snapshot_bytes(&self) -> u64 {
        self.superseded_snapshot_bytes
    }

    pub(crate) const fn wal_reclaimable_bytes(&self) -> u64 {
        self.wal_reclaimable_bytes
    }

    pub(crate) const fn wal_tail_bytes(&self) -> u64 {
        self.wal_tail_bytes
    }
}

/// How the audit classifies one WAL segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalSegmentClass {
    /// A covered-segment delete pass would delete it: sealed below the
    /// active segment, and every record it holds is at or below the proof.
    Reclaimable,
    /// The pass keeps it: the active segment and everything above it, a
    /// sealed segment with a record above the proof, or any segment while no
    /// proof exists.
    Tail,
}

/// Mirrors `WalService::delete_covered_segments` on the facts
/// `WalService::segment_coverage` reports: `sealed_max_commit` is `None` for
/// the active segment and everything above it.
pub(crate) fn classify_wal_segment(
    sealed_max_commit: Option<CommitVersion>,
    covered_through: Option<CommitVersion>,
) -> WalSegmentClass {
    match (sealed_max_commit, covered_through) {
        (Some(max_commit), Some(covered)) if max_commit <= covered => WalSegmentClass::Reclaimable,
        _ => WalSegmentClass::Tail,
    }
}

/// One family's audit tally.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Tally {
    objects: usize,
    bytes: u64,
}

impl Tally {
    fn add(&mut self, bytes: u64) {
        self.objects = self.objects.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
    }
}

impl<S> LifecycleDurableLocalRuntime<'_, S> {
    /// The live footprint: no backend I/O, ever.
    pub(crate) fn footprint_live_facts(&self) -> LifecycleFootprintLiveFacts {
        let wal_retention_watermark = match self.retention_watermark.get() {
            CachedRetentionWatermark::Known(watermark) => {
                LifecycleFootprintWatermark::Known(watermark)
            }
            CachedRetentionWatermark::Unknown => LifecycleFootprintWatermark::Cold,
        };
        LifecycleFootprintLiveFacts {
            live_table_objects: self.table_catalog.object_count(),
            live_table_bytes: self.table_catalog.total_object_bytes(),
            wal: self.services.wal().cached_growth_facts(),
            wal_retention_watermark,
            pending_reclaim_tasks: self
                .maintenance
                .pending_tasks()
                .iter()
                .filter(|task| reclaim_family(task.kind(), Some(task.scope())).is_some())
                .count(),
        }
    }

    /// The audit footprint: the reclaim runners' own listings, read-only.
    /// Nothing is enqueued, staged, deleted or recorded.
    pub(crate) fn footprint_audit_facts(&self) -> LifecycleResult<LifecycleFootprintAuditFacts> {
        let health = self.current_recovery_health.clone();
        let unreferenced = self.audit_unreferenced_table_objects(&health)?;
        let catalogued_unreferenced = unreferenced.as_ref().map_or((0, 0), |(_, catalogued)| {
            (catalogued.objects, catalogued.bytes)
        });
        let unreferenced = unreferenced.map(|(all, _)| all);
        let quarantined = self.audit_quarantine_inventories()?;
        let manifest = self
            .services
            .manifest()
            .load_current()
            .map_err(manifest_error)?;
        let attested = manifest
            .as_ref()
            .and_then(crate::format::DatabaseManifest::snapshot_id);
        let covered_through = manifest.as_ref().and_then(|manifest| {
            wal_retention_watermark(
                manifest.snapshot_watermark().map(CommitVersion::new),
                manifest.flushed_through_commit_id(),
            )
        });
        let (snapshots, superseded) = self.audit_snapshots(attested)?;
        let (segments, superseded_segments) = self.audit_timeline_segments(attested)?;
        let (reclaimable, tail) = self.audit_wal_segments(covered_through)?;
        Ok(LifecycleFootprintAuditFacts {
            unreferenced: unreferenced.map(|tally| (tally.objects, tally.bytes)),
            catalogued_unreferenced,
            quarantined_objects: quarantined.objects,
            quarantined_bytes: quarantined.bytes,
            snapshot_objects: snapshots.objects,
            snapshot_bytes: snapshots.bytes,
            superseded_snapshots: superseded.objects,
            superseded_snapshot_bytes: superseded.bytes,
            timeline_segments: (segments.objects, segments.bytes),
            superseded_timeline_segments: (superseded_segments.objects, superseded_segments.bytes),
            wal_reclaimable_bytes: reclaimable,
            wal_tail_bytes: tail,
        })
    }

    /// A report-only mark with the sweep's own pins (#2553): without them an
    /// object reachable only from in-memory branch state would count as
    /// unreferenced, and the audit would report debt the sweep never stages.
    /// `None` when the mark's proof is not complete. The second tally is the
    /// subset the table catalogue still records (#3646).
    fn audit_unreferenced_table_objects(
        &self,
        health: &RecoveryHealth,
    ) -> LifecycleResult<Option<(Tally, Tally)>> {
        let request = table_object_retention_request(
            &self.services,
            self.initial_branch_id,
            health,
            &self.reclaim_pinned_table_objects(),
        )?;
        let outcome = table_object_retention_outcome(&request)?;
        if outcome.retention().status() != LifecycleRetentionStatus::Completed {
            return Ok(None);
        }
        let sizes: BTreeMap<&ObjectName, u64> = request
            .inventory()
            .iter()
            .map(|entry| (entry.object(), entry.byte_count()))
            .collect();
        let mut tally = Tally::default();
        let mut catalogued = Tally::default();
        for object in outcome
            .decisions()
            .iter()
            .filter(|decision| decision.decision() == RetentionDecision::QuarantineCandidate)
            .filter_map(|decision| decision.object())
        {
            // Every candidate comes from the inventory the mark listed, so a
            // missing size can only be a listing race.
            let bytes = sizes.get(object).copied().unwrap_or(0);
            tally.add(bytes);
            if self.table_catalog.contains_object(object) {
                catalogued.add(bytes);
            }
        }
        Ok(Some((tally, catalogued)))
    }

    /// Every branch's quarantine inventory: the staged objects and their
    /// recorded sizes (one small read per branch, no per-object stat).
    fn audit_quarantine_inventories(&self) -> LifecycleResult<Tally> {
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = self.services.assembly_facts().codec_id();
        let mut tally = Tally::default();
        for descriptor in self.branch_catalog.list_branches(true) {
            let load = self
                .services
                .quarantine()
                .load_inventory(descriptor.branch_id(), database_id, codec_id)
                .map_err(durable_quarantine_service_error)?;
            for entry in load.inventory().entries() {
                tally.add(entry.byte_count());
            }
        }
        Ok(tally)
    }

    /// The snapshot family with sizes, and the part of it the attested id
    /// supersedes (space-reclamation contract §3.4).
    fn audit_snapshots(&self, attested: Option<u64>) -> LifecycleResult<(Tally, Tally)> {
        let mut all = Tally::default();
        let mut superseded = Tally::default();
        for (snapshot, bytes) in self
            .services
            .snapshot()
            .list_snapshot_sizes()
            .map_err(snapshot_error)?
        {
            all.add(bytes);
            if attested.is_some_and(|live| superseded_snapshot(snapshot.snapshot_id(), live)) {
                superseded.add(bytes);
            }
        }
        Ok((all, superseded))
    }

    /// #3643: the sealed timeline segments with sizes, and the part the next
    /// `Superseded` snapshot prune would delete (unreferenced by the live
    /// snapshot and sealed below it).
    fn audit_timeline_segments(&self, attested: Option<u64>) -> LifecycleResult<(Tally, Tally)> {
        let mut all = Tally::default();
        let mut superseded = Tally::default();
        // Resolved against the attested snapshot (never a stale cache); when
        // its references cannot be read, nothing is counted as superseded.
        let referenced = self.services.timeline_segments_referenced_by(attested);
        for (segment, bytes) in self
            .services
            .snapshot()
            .list_timeline_segment_sizes()
            .map_err(snapshot_error)?
        {
            all.add(bytes);
            if let (Some(live), Some(referenced)) = (attested, referenced.as_ref()) {
                if crate::service::timeline_segment_is_dead(
                    crate::service::TimelineSegmentPruneMode::Superseded,
                    segment,
                    live,
                    referenced,
                ) {
                    superseded.add(bytes);
                }
            }
        }
        Ok((all, superseded))
    }

    /// Every WAL segment classified as a covered-segment delete pass would
    /// treat it: `(reclaimable bytes, tail bytes)`.
    fn audit_wal_segments(
        &self,
        covered_through: Option<CommitVersion>,
    ) -> LifecycleResult<(u64, u64)> {
        let (mut reclaimable, mut tail) = (0u64, 0u64);
        for segment in self.services.wal().segment_coverage().map_err(wal_error)? {
            match classify_wal_segment(segment.sealed_max_commit(), covered_through) {
                WalSegmentClass::Reclaimable => {
                    reclaimable = reclaimable.saturating_add(segment.bytes());
                }
                WalSegmentClass::Tail => tail = tail.saturating_add(segment.bytes()),
            }
        }
        Ok((reclaimable, tail))
    }
}
