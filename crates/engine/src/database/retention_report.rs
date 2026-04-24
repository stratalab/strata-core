//! B5.2 — `Database::retention_report()`.
//!
//! Engine-owned, branch-vocabulary retention attribution surface per
//! `docs/design/branching/branching-gc/branching-retention-contract.md`
//! §"Engine-facing attribution contract" and the convergence doc's
//! §"Retention report contract".
//!
//! Two-layer assembly:
//!
//! 1. **Storage layer** — derives own-segment, inherited-layer, and
//!    quarantined byte totals per on-disk branch directory via
//!    [`strata_storage::SegmentedStore::retention_snapshot`]. Storage
//!    does not know which `BranchRef` a given `BranchId` currently
//!    maps to.
//! 2. **Engine layer** — joins storage facts with visible
//!    (`Active | Archived`) `BranchControlStore` records so bytes are
//!    attributed to live `BranchRef` lifecycle instances. When a storage
//!    directory has no visible control record (e.g. a tombstoned parent
//!    whose segments are still referenced by a descendant's inherited
//!    layer), the entry appears in [`RetentionReport::orphan_storage`]
//!    rather than being fabricated into some live lifecycle.
//!
//! Per the convergence doc's §"Rule 2. Stale-visible and unavailable
//! are not the same." plus the surface matrix's hard-fail-degraded
//! classification, `retention_report()` returns
//! [`StrataError::RetentionReportUnavailable`] when recovery health
//! cannot sustain trustworthy manifest-derived attribution
//! (`DataLoss` or any `QuarantineInventoryMismatch`-driven
//! `PolicyDowngrade`). Reclaim blockage that is *attribution-safe* is
//! surfaced as [`ReclaimStatus`] inside a successful report — blocked
//! reclaim is a retention fact, not a fabrication.

use std::collections::HashMap;
use std::sync::Arc;

use strata_core::contract::PrimitiveType;
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use strata_core::{
    BranchControlRecord, BranchLifecycleStatus, BranchRef, PrimitiveDegradedReason, StrataError,
    StrataResult,
};
use strata_storage::{
    DegradationClass, RecoveryHealth, StorageBranchRetention, StorageError,
    StorageInheritedLayerInfo,
};

use crate::branch_ops::branch_control_store::BranchControlStore;
use crate::database::primitive_degradation::PrimitiveDegradationRegistry;
use crate::database::Database;

/// Reclaim readiness classification for a retention report.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReclaimStatus {
    /// Reclaim is allowed; recovery health permits the full protocol.
    Allowed,
    /// Reclaim is refused because the most recent recovery produced a
    /// non-`Telemetry` degradation (contract §"Recovery-health
    /// contract"). Attribution in the report body is still trustworthy.
    BlockedDegradedRecovery {
        /// The observed degradation class (e.g. `"PolicyDowngrade"`).
        class: String,
    },
}

/// Reason a retention blocker is attributed to this branch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetentionBlocker {
    /// This branch retains bytes through an inherited-layer manifest
    /// pointing at `source_branch_id` / `fork_version`. The current
    /// blocker attribution goes to the *live* branch that still holds
    /// the inherited-layer reference — per §"Canonical blocker
    /// attribution", that is the actionable owner of the retention
    /// debt, not the historical original writer.
    InheritedLayerRetention {
        /// Storage branch whose directory holds the retained bytes.
        source_branch_id: BranchId,
        /// Live lifecycle at `source_branch_id`, when one exists.
        source_branch: Option<BranchRef>,
        /// Fork point that bounds descendant visibility into the source.
        fork_version: CommitVersion,
        /// Bytes kept live by this layer reference.
        bytes: u64,
        /// `true` if the operator can drop this blocker by calling
        /// `materialize` on this branch. `false` when the source branch
        /// still has a live lifecycle — those bytes aren't
        /// materialize-reclaimable, they're genuinely shared.
        removable_by_materialization: bool,
    },
    /// This branch holds own-segment bytes that at least one
    /// descendant's inherited-layer manifest references. Not
    /// actionable by this branch alone.
    DescendantHolds {
        /// Total bytes held by descendants.
        bytes: u64,
    },
    /// One or more segments in this branch's `__quarantine__/` are
    /// pending Stage-5 purge but reclaim trust is degraded. The report
    /// surfaces the fact; recovery must be healthy before the purge
    /// can drain.
    QuarantinePending {
        /// Number of segments awaiting final purge.
        segment_count: usize,
        /// Total bytes awaiting final purge.
        bytes: u64,
    },
}

/// Per-branch retention entry attributed to a live `BranchRef`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BranchRetentionEntry {
    /// Generation-aware canonical identity.
    pub branch: BranchRef,
    /// User-facing branch name recorded on the control record.
    pub name: String,
    /// Lifecycle state at the time the report was produced.
    pub lifecycle: BranchLifecycleStatus,
    /// Own-segment bytes that no descendant's inherited layer references.
    pub exclusive_bytes: u64,
    /// Own-segment bytes that at least one descendant still references
    /// through an inherited layer (retention survives `clear_branch`
    /// until every descendant releases).
    pub shared_bytes: u64,
    /// Bytes visible to this branch via inherited-layer manifests;
    /// bytes physically live under the source branch's directory.
    pub inherited_layer_bytes: u64,
    /// Bytes in this branch's `__quarantine__/` awaiting Stage-5 purge.
    pub quarantined_bytes: u64,
    /// Retention blockers attributed to this branch.
    pub blockers: Vec<RetentionBlocker>,
}

/// Storage-only retention record whose on-disk directory has no live
/// control record to attribute it to.
///
/// This is where bytes end up when a parent has been deleted (control
/// record tombstoned) but descendants still reference its segments
/// through inherited layers — the contract's §"Parent delete with
/// surviving descendants" path.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OrphanStorageEntry {
    /// The storage directory's branch id.
    pub branch_id: BranchId,
    /// Own-segment bytes the directory still holds.
    pub bytes: u64,
    /// Bytes quarantined under this directory.
    pub quarantined_bytes: u64,
    /// Why this storage dir outlived its control record.
    pub reason: OrphanReason,
}

/// Classification for [`OrphanStorageEntry`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum OrphanReason {
    /// The control record was tombstoned but a descendant's
    /// inherited-layer manifest still references bytes under this
    /// directory. Retained per contract Invariant 7 + §"Parent
    /// delete".
    DescendantInheritance,
    /// No live control record and no inbound inherited-layer
    /// references observed — typically quarantine-only retention
    /// debt or a pre-control-store legacy branch directory.
    UntrackedLifecycle,
}

/// Aggregate totals across every entry + orphan in the report.
///
/// **Not a unique-bytes sum.** `shared_bytes` and `inherited_layer_bytes`
/// are two views of the same physical retention: bytes owned by one
/// branch but kept live by a descendant's inherited-layer reference are
/// counted once on the owner (as shared) and once on each descendant
/// that inherits (as inherited-layer). The view that matters depends on
/// what the operator is asking:
///
/// - "how many bytes am I retaining because of live branches?" →
///   `exclusive_bytes + shared_bytes` (owner view)
/// - "which branches are responsible for keeping retention alive?" →
///   iterate `branches[*].blockers`, not the totals
///
/// `quarantined_bytes` is disjoint from the other three (quarantined
/// files are physically separate from own segments).
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RetentionTotals {
    /// Sum of `exclusive_bytes` across all branch entries + orphan entries.
    pub exclusive_bytes: u64,
    /// Sum of `shared_bytes` across all branch entries.
    pub shared_bytes: u64,
    /// Sum of `inherited_layer_bytes` across all branch entries. Overlaps
    /// with `shared_bytes` — see struct-level note.
    pub inherited_layer_bytes: u64,
    /// Sum of `quarantined_bytes` across all branches + orphans.
    pub quarantined_bytes: u64,
}

/// A fail-closed degraded primitive state attributed to a live
/// branch lifecycle instance (B5.4).
///
/// Populated by joining the per-`Database`
/// [`PrimitiveDegradationRegistry`] with live `BranchControlRecord`
/// records: entries whose recorded generation no longer matches a live
/// lifecycle are filtered out (defense-in-depth against a missed
/// `clear_branch` call during same-name recreate).
///
/// Per the B5 convergence contract
/// (`docs/design/branching/branching-gc/branching-b5-convergence-and-observability.md`
/// §"Degraded-state closure targets"), this surface is the
/// branch-facing pull contract for named primitive degradation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DegradedPrimitiveEntry {
    /// Generation-aware branch identity.
    pub branch: BranchRef,
    /// User-facing branch name recorded on the control record.
    pub name: String,
    /// Which primitive subsystem owns the degraded surface.
    pub primitive: PrimitiveType,
    /// Primitive-level name (collection, index space, etc.).
    pub primitive_name: String,
    /// Typed reason for the degradation.
    pub reason: PrimitiveDegradedReason,
    /// Operator-facing free-form detail captured at mark time.
    pub detail: String,
}

/// Generation-aware branch-vocabulary retention attribution surface.
///
/// See the module-level doc and the B5 contract for the governing rules.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RetentionReport {
    /// Reclaim readiness classification.
    pub reclaim_status: ReclaimStatus,
    /// Per-branch entries keyed by live `BranchRef`.
    pub branches: Vec<BranchRetentionEntry>,
    /// Storage directories with no live lifecycle to attribute them to.
    pub orphan_storage: Vec<OrphanStorageEntry>,
    /// Aggregate totals across the entire report.
    pub totals: RetentionTotals,
    /// Named primitives in fail-closed degraded state (B5.4).
    ///
    /// This list is orthogonal to byte-accounting fields: a degraded
    /// primitive is a logical lifecycle fact, not a storage retention
    /// debt. Populated from the per-`Database`
    /// [`PrimitiveDegradationRegistry`]; entries whose generation does
    /// not match a live `BranchControlRecord` are filtered out
    /// (defense-in-depth against missed `clear_branch` on same-name
    /// recreate).
    pub degraded_primitives: Vec<DegradedPrimitiveEntry>,
}

impl Database {
    /// Generation-aware branch-vocabulary retention attribution.
    ///
    /// Manifest-derived: own-segment bytes, inherited-layer bytes,
    /// `__quarantine__/` inventory bytes, and reclaim readiness under
    /// the current recovery-health classification.
    ///
    /// Returns `Err(StrataError::RetentionReportUnavailable)` when
    /// recovery health is `DataLoss` (authoritative loss that would
    /// force attribution to fabricate) per the convergence doc's
    /// §"Surface matrix" hard-fail rule. `PolicyDowngrade` with a
    /// `QuarantineInventoryMismatch` fault also routes here because
    /// the disagreement blocks trustworthy quarantine attribution.
    /// Unmigrated-follower lineage unavailability (AD5) also routes here:
    /// storage bytes may be known, but generation-aware `BranchRef`
    /// attribution is not.
    /// Other `PolicyDowngrade` cases (e.g. no-manifest fallback) are
    /// reported as [`ReclaimStatus::BlockedDegradedRecovery`] inside a
    /// successful report: reclaim is paused but attribution remains
    /// trustworthy.
    ///
    /// Contract: `docs/design/branching/branching-gc/branching-retention-contract.md`
    /// §"Engine-facing attribution contract".
    pub fn retention_report(self: &Arc<Self>) -> StrataResult<RetentionReport> {
        let health = self.recovery_health();
        if let Some(cls) = report_refusal_class(&health) {
            return Err(StrataError::retention_report_unavailable(class_name(cls)));
        }

        let reclaim_status = match &health {
            RecoveryHealth::Healthy
            | RecoveryHealth::Degraded {
                class: DegradationClass::Telemetry,
                ..
            } => ReclaimStatus::Allowed,
            RecoveryHealth::Degraded { class, .. } => ReclaimStatus::BlockedDegradedRecovery {
                class: class_name(*class).to_string(),
            },
            // `RecoveryHealth` is `#[non_exhaustive]`; unknown variants
            // are already refused by `report_refusal_class`, so this
            // arm is defense-in-depth — report degraded.
            _ => ReclaimStatus::BlockedDegradedRecovery {
                class: "Unknown".to_string(),
            },
        };

        // Storage layer — manifest-derived per-directory facts.
        let storage_snapshot = match self.storage.retention_snapshot() {
            Ok(snapshot) => snapshot,
            Err(e) => {
                return Err(StrataError::retention_report_unavailable(
                    retention_snapshot_unavailable_class(&e),
                ))
            }
        };
        let snapshot_by_id: HashMap<BranchId, StorageBranchRetention> = storage_snapshot
            .into_iter()
            .map(|s| (s.branch_id, s))
            .collect();

        // Engine layer — visible (`Active | Archived`) BranchRef + name per
        // BranchId via BranchControlStore. If lifecycle identity is
        // unavailable (e.g. unmigrated follower AD5), fail closed rather than
        // fabricating every storage row into `orphan_storage`.
        let live_records = match BranchControlStore::new(self.clone()).list_visible() {
            Ok(records) => records,
            Err(e) if e.is_branch_lineage_unavailable() => {
                return Err(StrataError::retention_report_unavailable("PolicyDowngrade"))
            }
            Err(e) => return Err(e),
        };
        let live_by_id: HashMap<BranchId, BranchControlRecord> =
            live_records.into_iter().map(|r| (r.branch.id, r)).collect();

        // Collect which storage branches any live inherited-layer
        // points at — needed to classify orphan storage as
        // "DescendantInheritance" rather than "UntrackedLifecycle".
        let mut inherited_sources: HashMap<BranchId, bool> = HashMap::new();
        for snap in snapshot_by_id.values() {
            for layer in &snap.inherited_layers {
                inherited_sources.insert(layer.source_branch_id, true);
            }
        }

        // Build per-branch entries for every live control record.
        let mut branches: Vec<BranchRetentionEntry> = Vec::new();
        let mut totals = RetentionTotals::default();

        for record in live_by_id.values() {
            let branch_id = record.branch.id;
            let snap = snapshot_by_id
                .get(&branch_id)
                .cloned()
                .unwrap_or_else(|| StorageBranchRetention::empty(branch_id));

            let mut blockers: Vec<RetentionBlocker> = Vec::new();
            for layer in &snap.inherited_layers {
                blockers.push(inherited_layer_blocker(layer, &live_by_id));
            }
            if snap.shared_bytes > 0 {
                blockers.push(RetentionBlocker::DescendantHolds {
                    bytes: snap.shared_bytes,
                });
            }
            if snap.quarantined_segment_count > 0
                && matches!(
                    reclaim_status,
                    ReclaimStatus::BlockedDegradedRecovery { .. }
                )
            {
                blockers.push(RetentionBlocker::QuarantinePending {
                    segment_count: snap.quarantined_segment_count,
                    bytes: snap.quarantined_bytes,
                });
            }

            totals.exclusive_bytes += snap.exclusive_bytes;
            totals.shared_bytes += snap.shared_bytes;
            totals.inherited_layer_bytes += snap.inherited_layer_bytes;
            totals.quarantined_bytes += snap.quarantined_bytes;

            branches.push(BranchRetentionEntry {
                branch: record.branch,
                name: record.name.clone(),
                lifecycle: record.lifecycle,
                exclusive_bytes: snap.exclusive_bytes,
                shared_bytes: snap.shared_bytes,
                inherited_layer_bytes: snap.inherited_layer_bytes,
                quarantined_bytes: snap.quarantined_bytes,
                blockers,
            });
        }

        let mut orphan_storage: Vec<OrphanStorageEntry> = Vec::new();
        // Detached descendant-held bytes must surface as orphan storage even
        // when the same BranchId currently has a live control record. This is
        // the same-name recreate shape: the old lifecycle's bytes still live
        // under the deterministic storage directory, but they are not owned by
        // the new live manifest and must not be folded into the recreated
        // branch entry's `shared_bytes`.
        for snap in snapshot_by_id.values() {
            if snap.detached_shared_bytes == 0 || !live_by_id.contains_key(&snap.branch_id) {
                continue;
            }
            totals.shared_bytes += snap.detached_shared_bytes;
            orphan_storage.push(OrphanStorageEntry {
                branch_id: snap.branch_id,
                bytes: snap.detached_shared_bytes,
                quarantined_bytes: 0,
                reason: OrphanReason::DescendantInheritance,
            });
        }

        // Remaining storage directories have no live control record.
        for (branch_id, snap) in &snapshot_by_id {
            if live_by_id.contains_key(branch_id) {
                continue;
            }
            let total_bytes = snap.exclusive_bytes + snap.shared_bytes + snap.detached_shared_bytes;
            if total_bytes == 0 && snap.quarantined_bytes == 0 {
                continue;
            }
            let reason = if inherited_sources.contains_key(branch_id)
                || snap.shared_bytes > 0
                || snap.detached_shared_bytes > 0
            {
                OrphanReason::DescendantInheritance
            } else {
                OrphanReason::UntrackedLifecycle
            };
            totals.exclusive_bytes += snap.exclusive_bytes;
            totals.shared_bytes += snap.shared_bytes + snap.detached_shared_bytes;
            totals.quarantined_bytes += snap.quarantined_bytes;
            orphan_storage.push(OrphanStorageEntry {
                branch_id: *branch_id,
                bytes: total_bytes,
                quarantined_bytes: snap.quarantined_bytes,
                reason,
            });
        }

        // B5.4 — join primitive-degradation registry with live lifecycle.
        // Entries are dropped when:
        //   - no live control record exists for the BranchId (orphan
        //     degradation — the branch has been deleted; the physical
        //     cleanup hook also clears the registry, so this is
        //     defense-in-depth), or
        //   - the recorded generation does not match the live record
        //     (same-name recreate advanced past the old lifecycle).
        let mut degraded_primitives: Vec<DegradedPrimitiveEntry> = Vec::new();
        if let Ok(registry) = self.extension::<PrimitiveDegradationRegistry>() {
            for entry in registry.list() {
                let Some(record) = live_by_id.get(&entry.branch_ref.id) else {
                    continue;
                };
                if record.branch.generation != entry.branch_ref.generation {
                    continue;
                }
                degraded_primitives.push(DegradedPrimitiveEntry {
                    branch: record.branch,
                    name: record.name.clone(),
                    primitive: entry.primitive,
                    primitive_name: entry.name.clone(),
                    reason: entry.reason,
                    detail: entry.detail.clone(),
                });
            }
        }

        Ok(RetentionReport {
            reclaim_status,
            branches,
            orphan_storage,
            totals,
            degraded_primitives,
        })
    }
}

fn inherited_layer_blocker(
    layer: &StorageInheritedLayerInfo,
    live_by_id: &HashMap<BranchId, BranchControlRecord>,
) -> RetentionBlocker {
    let source_branch = live_by_id.get(&layer.source_branch_id).map(|r| r.branch);
    // Removable by materialize when the source has no live lifecycle;
    // otherwise the bytes are genuinely shared with a live source and
    // materialize on the descendant merely rewrites ownership, not
    // existence.
    let removable_by_materialization = source_branch.is_none();
    RetentionBlocker::InheritedLayerRetention {
        source_branch_id: layer.source_branch_id,
        source_branch,
        fork_version: layer.fork_version,
        bytes: layer.bytes,
        removable_by_materialization,
    }
}

/// Return `Some(class)` if recovery health is degraded enough that
/// attribution cannot be trusted and `retention_report()` must refuse
/// with [`StrataError::RetentionReportUnavailable`].
fn report_refusal_class(health: &RecoveryHealth) -> Option<DegradationClass> {
    match health {
        RecoveryHealth::Healthy
        | RecoveryHealth::Degraded {
            class: DegradationClass::Telemetry,
            ..
        } => None,
        RecoveryHealth::Degraded { class, faults } => {
            if matches!(class, DegradationClass::DataLoss) {
                return Some(*class);
            }
            // PolicyDowngrade is attribution-safe *except* when it
            // stems from a QuarantineInventoryMismatch — that
            // specifically says we cannot trust the quarantine side
            // of the attribution join.
            for fault in faults.iter() {
                if matches!(
                    fault,
                    strata_storage::RecoveryFault::QuarantineInventoryMismatch { .. }
                ) {
                    return Some(*class);
                }
            }
            None
        }
        // `RecoveryHealth` is `#[non_exhaustive]`; unknown variants
        // route to a defensive refusal rather than fabricating a
        // report.
        _ => Some(DegradationClass::DataLoss),
    }
}

fn class_name(class: DegradationClass) -> &'static str {
    match class {
        DegradationClass::DataLoss => "DataLoss",
        DegradationClass::PolicyDowngrade => "PolicyDowngrade",
        DegradationClass::Telemetry => "Telemetry",
        // `DegradationClass` is `#[non_exhaustive]`; unknown variants
        // stringify to a sentinel the caller can still discriminate.
        _ => "Unknown",
    }
}

fn retention_snapshot_unavailable_class(err: &StorageError) -> &'static str {
    match err {
        StorageError::QuarantineReconciliationFailed { .. } => "PolicyDowngrade",
        StorageError::RecoveryFault(
            strata_storage::RecoveryFault::QuarantineInventoryMismatch { .. },
        ) => "PolicyDowngrade",
        // Any other inability to read storage truth means the report cannot
        // safely attribute retained bytes. Surface the typed hard-fail
        // contract rather than leaking raw storage errors through this API.
        _ => "DataLoss",
    }
}
