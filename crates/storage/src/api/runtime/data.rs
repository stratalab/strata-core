use super::error::{branch_error, commit_error, map_lifecycle_error};
use super::{
    BranchHistoryOptions, BranchId, BranchReadBound, BranchReadView, BranchScanBounds,
    CommitAdmissionPressureReason, CommitAdmissionPressureSeverity, CommitAdmissionSummary,
    CommitBatch, CommitDurabilityClass, CommitDurabilitySummary, CommitExpectedVersion,
    CommitSummary, CommitTimelineEntry, CommitTimelineLookup, CommitTimelineMiss,
    CommitTimelineView, CommitVersion, FlushFrozenRequest, FlushTableIdentitySeed,
    FlushTableObjectId, LifecycleStoragePressureReason, LifecycleStoragePressureSeverity,
    LifecycleWriteAdmissionOutcome, LifecycleWriteAdmissionStatus, PhysicalKey, ReadBound,
    ReadLimit, ResolvedReadBound, RowStorageSpaceId, ScanReadOutcome, StorageApiError,
    StorageApiLowerLayer, StorageApiResult, StorageKey, StorageReadRow, StorageRow, StorageSpaceId,
    StorageValue, Timestamp, API_PHYSICAL_SPACE, COMMIT_TIMELINE_SPACE,
};
use crate::api::read::WallClockLookupOutcome;
use crate::api::StorageImmutableSource;
use crate::branch::read::BranchImmutableRowSource;
use crate::timeline_index::WallClockResolution;

pub(super) fn map_commit_summary(
    outcome: &crate::commit::CommitOutcome,
    admission: Option<LifecycleWriteAdmissionOutcome>,
) -> StorageApiResult<CommitSummary> {
    let commit_version = outcome
        .commit_version()
        .ok_or(StorageApiError::InvalidRuntimeState {
            reason: "commit did not allocate a commit version",
        })?;
    let commit_timestamp =
        outcome
            .commit_timestamp()
            .ok_or(StorageApiError::InvalidRuntimeState {
                reason: "commit did not allocate a commit timestamp",
            })?;
    let counts = outcome.mutation_counts();
    Ok(CommitSummary::with_commit_facts(
        outcome.branch_id(),
        commit_version,
        commit_timestamp,
        map_commit_durability(outcome.durability()),
        counts.puts(),
        counts.deletes(),
        counts.timeline_rows(),
        matches!(outcome.kind(), crate::commit::CommitOutcomeKind::Visible),
    )
    .with_committed_at(outcome.committed_at())
    .with_admission_summary(map_commit_admission_summary(admission)))
}

pub(super) const fn map_commit_admission_summary(
    admission: Option<LifecycleWriteAdmissionOutcome>,
) -> CommitAdmissionSummary {
    let Some(admission) = admission else {
        return CommitAdmissionSummary::accepted_clean(
            CommitAdmissionPressureSeverity::None,
            CommitAdmissionPressureReason::None,
            false,
        );
    };
    match admission.status() {
        LifecycleWriteAdmissionStatus::AcceptedClean => CommitAdmissionSummary::accepted_clean(
            map_commit_admission_pressure_severity(admission.pressure().severity()),
            map_commit_admission_pressure_reason(admission.pressure().reason()),
            admission.cleared_prior_rejection(),
        ),
        LifecycleWriteAdmissionStatus::AcceptedUnderPressure => {
            CommitAdmissionSummary::accepted_under_pressure(
                map_commit_admission_pressure_reason(admission.pressure().reason()),
                admission.cleared_prior_rejection(),
            )
            .with_inline_maintenance_driven(admission.inline_maintenance_driven())
        }
    }
}

pub(super) const fn map_commit_admission_pressure_severity(
    severity: LifecycleStoragePressureSeverity,
) -> CommitAdmissionPressureSeverity {
    match severity {
        LifecycleStoragePressureSeverity::None => CommitAdmissionPressureSeverity::None,
        LifecycleStoragePressureSeverity::Background => CommitAdmissionPressureSeverity::Background,
        LifecycleStoragePressureSeverity::Urgent => CommitAdmissionPressureSeverity::Urgent,
        LifecycleStoragePressureSeverity::BlockMutatingAdmission => {
            CommitAdmissionPressureSeverity::Blocking
        }
    }
}

pub(super) const fn map_commit_admission_pressure_reason(
    reason: LifecycleStoragePressureReason,
) -> CommitAdmissionPressureReason {
    match reason {
        LifecycleStoragePressureReason::None => CommitAdmissionPressureReason::None,
        LifecycleStoragePressureReason::ActiveMutableBytes => {
            CommitAdmissionPressureReason::ActiveMutableBytes
        }
        LifecycleStoragePressureReason::FrozenBacklog => {
            CommitAdmissionPressureReason::FrozenBacklog
        }
        LifecycleStoragePressureReason::LevelZeroTableBacklog => {
            CommitAdmissionPressureReason::LevelZeroTableBacklog
        }
        LifecycleStoragePressureReason::NonZeroLevelTableBacklog => {
            CommitAdmissionPressureReason::NonZeroLevelTableBacklog
        }
        LifecycleStoragePressureReason::InheritedLayerBacklog => {
            CommitAdmissionPressureReason::InheritedLayerBacklog
        }
        LifecycleStoragePressureReason::MaintenanceQueueBacklog => {
            CommitAdmissionPressureReason::MaintenanceQueueBacklog
        }
    }
}

pub(super) const fn map_commit_durability(
    durability: CommitDurabilityClass,
) -> CommitDurabilitySummary {
    match durability {
        CommitDurabilityClass::NotDurable => CommitDurabilitySummary::NotDurable,
        CommitDurabilityClass::Standard => CommitDurabilitySummary::Standard,
        CommitDurabilityClass::Always => CommitDurabilitySummary::Always,
        CommitDurabilityClass::Uncertain => CommitDurabilitySummary::Uncertain,
    }
}

pub(super) fn physical_key(
    branch_id: BranchId,
    storage_space: &StorageSpaceId,
    key: &StorageKey,
) -> StorageApiResult<PhysicalKey> {
    PhysicalKey::new(
        branch_id,
        API_PHYSICAL_SPACE,
        map_storage_space(storage_space)?,
        key.as_bytes().to_vec(),
    )
    .map_err(|error| {
        StorageApiError::lower_layer_with(
            StorageApiLowerLayer::Branch,
            "physical key construction failed",
            error,
        )
    })
}

pub(super) fn map_storage_space(
    storage_space: &StorageSpaceId,
) -> StorageApiResult<RowStorageSpaceId> {
    let bytes = storage_space.as_bytes();
    let [raw] = bytes else {
        return Err(StorageApiError::InvalidArgument {
            field: "storage_space",
            reason: "storage space must be a single engine-owned byte",
        });
    };
    RowStorageSpaceId::engine(*raw).map_err(|_| StorageApiError::InvalidArgument {
        field: "storage_space",
        reason: "storage space must use an engine-owned id",
    })
}

pub(super) fn read_row_from_storage(row: &StorageRow) -> StorageApiResult<StorageReadRow> {
    let storage_space = StorageSpaceId::new(vec![row.physical_key().storage_space_id().raw()])?;
    let key = StorageKey::new(row.physical_key().user_key().to_vec())?;
    let expires_at = (row.expires_at() != Timestamp::EPOCH).then_some(row.expires_at());
    let value = if row.is_tombstone() {
        None
    } else {
        Some(StorageValue::new(row.value().to_vec()))
    };
    Ok(StorageReadRow::new(
        storage_space,
        key,
        value,
        row.commit_version(),
        row.commit_timestamp(),
        expires_at,
        row.is_tombstone(),
    ))
}

/// B4: the move-based point-read exit — expiry/tombstone checks run on the
/// reference BEFORE this call; the row's key and value Vecs move straight
/// into the public row (no per-read key/value copies). Scan and history
/// paths keep the by-ref builder above.
pub(super) fn point_read_row_from_storage_owned(
    row: StorageRow,
) -> StorageApiResult<StorageReadRow> {
    let (space_id, user_key, value, commit_version, commit_timestamp, expires_at, tombstone) =
        row.into_read_parts();
    let storage_space = StorageSpaceId::new(vec![space_id.raw()])?;
    let key = StorageKey::new(user_key)?;
    let expires_at = (expires_at != Timestamp::EPOCH).then_some(expires_at);
    let value = if tombstone {
        None
    } else {
        Some(StorageValue::new(value))
    };
    Ok(StorageReadRow::new(
        storage_space,
        key,
        value,
        commit_version,
        commit_timestamp,
        expires_at,
        tombstone,
    ))
}

pub(super) fn read_row_from_storage_if_visible(
    row: &StorageRow,
    selected_timestamp: Option<Timestamp>,
) -> StorageApiResult<Option<StorageReadRow>> {
    if row_is_expired_at_selected_frontier(row, selected_timestamp) {
        Ok(None)
    } else {
        read_row_from_storage(row).map(Some)
    }
}

pub(super) fn row_is_expired_at_selected_frontier(
    row: &StorageRow,
    selected_timestamp: Option<Timestamp>,
) -> bool {
    selected_timestamp.is_some_and(|timestamp| {
        !row.is_tombstone() && row.expires_at() != Timestamp::EPOCH && row.expires_at() <= timestamp
    })
}

/// Cap a resolved (versioned) read bound at the visible version `V`, so an off-lock versioned read
/// never observes rows newer than the writer has acknowledged (an `applied_not_visible` commit, or
/// a batch mid-apply on another thread). A no-op when the requested version is already `≤ V`.
/// `resolve_read_bound` always yields `AtVersion` for a versioned request; the other arms are
/// defensive.
///
/// KNOWN GAP (deferred): when the version is capped down (`req > V`, reachable only in
/// `applied_not_visible` with a landed timeline entry), the caller keeps `selected_timestamp` at
/// `req`'s rather than the served version `V`'s — a narrow, conservative TTL-frontier over-hide.
/// The `AtVersion` vs `AtTimestamp` frontier semantics are subtle (for `AtTimestamp` the requested
/// time may be the correct frontier), so the fix is intentionally left to a focused later slice.
pub(super) fn cap_bound_at_visible(bound: BranchReadBound, visible: u64) -> BranchReadBound {
    match bound {
        BranchReadBound::AtVersion(version) => {
            BranchReadBound::at_version(CommitVersion::new(version.as_u64().min(visible)))
        }
        BranchReadBound::Latest => BranchReadBound::at_version(CommitVersion::new(visible)),
        BranchReadBound::AtTimestamp(_) => bound,
    }
}

pub(super) fn visible_tombstone_at_bound(
    view: &BranchReadView,
    key: &PhysicalKey,
    bound: BranchReadBound,
) -> StorageApiResult<Option<StorageReadRow>> {
    let rows = view
        .history(key, BranchHistoryOptions::all())
        .map_err(branch_error)?;
    for row in rows {
        if !row_matches_read_bound(row.row(), bound) {
            continue;
        }
        if row.row().is_tombstone() {
            // B4: tombstone rows exit by move too.
            return point_read_row_from_storage_owned(row.into_storage_row()).map(Some);
        }
        return Ok(None);
    }
    Ok(None)
}

pub(super) fn row_matches_read_bound(row: &StorageRow, bound: BranchReadBound) -> bool {
    match bound {
        BranchReadBound::Latest => true,
        BranchReadBound::AtVersion(version) => row.commit_version() <= version,
        BranchReadBound::AtTimestamp(timestamp) => row.commit_timestamp() <= timestamp,
    }
}

pub(super) fn map_scan_rows<'a>(
    rows: impl Iterator<Item = &'a StorageRow>,
    limit: Option<ReadLimit>,
    selected_timestamp: Option<Timestamp>,
) -> StorageApiResult<ScanReadOutcome> {
    let mut mapped = Vec::new();
    for row in rows {
        if limit.is_some_and(|limit| mapped.len() >= limit.get()) {
            break;
        }
        if let Some(read_row) = read_row_from_storage_if_visible(row, selected_timestamp)? {
            mapped.push(read_row);
        }
    }
    Ok(ScanReadOutcome::new(mapped))
}

pub(super) fn map_immutable_sources(
    sources: &[BranchImmutableRowSource],
    selected_timestamp: Option<Timestamp>,
) -> StorageApiResult<Vec<StorageImmutableSource>> {
    let mut mapped = Vec::with_capacity(sources.len());
    for source in sources {
        let mut rows = Vec::new();
        for row in source.rows() {
            if let Some(read_row) = read_row_from_storage_if_visible(row.row(), selected_timestamp)?
            {
                rows.push(read_row);
            }
        }
        if rows.is_empty() {
            continue;
        }
        mapped.push(StorageImmutableSource::new(
            source.source_id().to_owned(),
            source.source_branch_id(),
            source.source_generation(),
            source.fork_version_cap(),
            rows,
        ));
    }
    Ok(mapped)
}

pub(super) fn require_version_retained(
    view: &BranchReadView,
    version: CommitVersion,
) -> StorageApiResult<()> {
    let timeline = timeline_view_or_index(view)?;
    if timeline
        .bounds()
        .min_version()
        .is_some_and(|min_version| version < min_version)
    {
        return Err(StorageApiError::RetainedHistoryUnavailable {
            branch_id: view.branch_id(),
            reason: "commit version is outside retained history",
        });
    }
    Ok(())
}

pub(super) fn resolve_read_bound(
    view: &BranchReadView,
    bound: ReadBound,
) -> StorageApiResult<ResolvedReadBound> {
    match bound {
        ReadBound::Latest => Ok(ResolvedReadBound {
            branch_bound: BranchReadBound::Latest,
            selected_timestamp: None,
        }),
        ReadBound::AtVersion(version) => {
            let selected_timestamp = timeline_timestamp_for_version(view, version)?.ok_or(
                StorageApiError::RetainedHistoryUnavailable {
                    branch_id: view.branch_id(),
                    reason: "commit version is outside retained timeline history",
                },
            )?;
            Ok(ResolvedReadBound {
                branch_bound: BranchReadBound::AtVersion(version),
                selected_timestamp: Some(selected_timestamp),
            })
        }
        ReadBound::AtTimestamp(timestamp) => {
            let lookup = timeline_version_at_or_before(view, timestamp)?;
            match lookup.miss() {
                CommitTimelineMiss::Matched => Ok(ResolvedReadBound {
                    branch_bound: BranchReadBound::AtVersion(lookup.matched_version().ok_or(
                        StorageApiError::TimestampHistoryUnavailable {
                            branch_id: view.branch_id(),
                            reason: "timestamp lookup did not return a retained version",
                        },
                    )?),
                    selected_timestamp: Some(lookup.matched_timestamp().ok_or(
                        StorageApiError::TimestampHistoryUnavailable {
                            branch_id: view.branch_id(),
                            reason: "timestamp lookup did not return a retained timestamp",
                        },
                    )?),
                }),
                CommitTimelineMiss::BeforeRetainedHistory | CommitTimelineMiss::Empty => {
                    Err(StorageApiError::TimestampHistoryUnavailable {
                        branch_id: view.branch_id(),
                        reason: "timestamp is before retained timeline history",
                    })
                }
                CommitTimelineMiss::AfterLatestRetained => {
                    Err(StorageApiError::TimestampHistoryUnavailable {
                        branch_id: view.branch_id(),
                        reason: "timestamp is after latest retained timeline history",
                    })
                }
            }
        }
    }
}

/// W3.1a: `version_at_or_before` through the retained-timeline index when it
/// can prove equivalence with a scan of this view; otherwise the scan — which
/// then seeds the index so the next caller takes the fast path. Pinned views
/// clamp the index to their captured max commit version; live-snapshot views
/// (whose scans race forward with the shared memtable) serve the index tip.
pub(super) fn timeline_version_at_or_before(
    view: &BranchReadView,
    timestamp: Timestamp,
) -> StorageApiResult<CommitTimelineLookup> {
    let Some((index, live_active)) = view.retained_timeline() else {
        return Ok(timeline_view_from_read_view(view)?.version_at_or_before(timestamp));
    };
    if let Some(lookup) = index.lookup_at_or_before(timestamp, pinned_view_bound(view, live_active))
    {
        return Ok(retained_lookup_to_commit_lookup(timestamp, lookup));
    }
    let scanned = timeline_view_from_read_view(view)?;
    seed_retained_timeline(index, &scanned);
    Ok(scanned.version_at_or_before(timestamp))
}

/// #3112 S3a: resolve a wall-clock instant to a commit boundary.
///
/// Unlike every other timeline lookup here there is **no scan fallback**:
/// `committed_at` is commit-scoped and never written to timeline rows
/// (storage-format spec §10 req 13), so a scan cannot supply it even where the
/// scan itself succeeds. An index that cannot prove exactness therefore
/// refuses — falling through to logical semantics would silently answer a
/// different question than the one asked.
///
/// Each refusal carries its own `reason`, because they are different facts
/// about the store and a client acts on them differently: retry later, widen
/// the window, or stop asking in wall-clock terms on this branch.
pub(super) fn timeline_resolve_wall_clock(
    view: &BranchReadView,
    instant: Timestamp,
) -> StorageApiResult<WallClockLookupOutcome> {
    let unavailable = |reason: &'static str| StorageApiError::TimestampHistoryUnavailable {
        branch_id: view.branch_id(),
        reason,
    };
    let Some((index, live_active)) = view.retained_timeline() else {
        return Err(unavailable(
            "wall-clock history is unavailable on this branch",
        ));
    };
    let resolution = index
        .resolve_wall_clock(instant, pinned_view_bound(view, live_active))
        .ok_or_else(|| unavailable("wall-clock history is unavailable on this branch"))?;
    match resolution {
        WallClockResolution::Matched(entry) => Ok(WallClockLookupOutcome::new(
            entry.commit_version(),
            entry.commit_timestamp(),
            entry
                .committed_at()
                .ok_or_else(|| unavailable("resolved commit carries no wall-clock instant"))?,
        )),
        WallClockResolution::AfterLatestDated => Err(unavailable(
            "wall-clock instant is after the latest dated commit",
        )),
        WallClockResolution::BeforeDatedWithUndatedPrefix => Err(unavailable(
            "wall-clock instant is before the first dated commit; earlier history is undated",
        )),
        WallClockResolution::BeforeDatedHistory => Err(unavailable(
            "wall-clock instant is before the first dated commit",
        )),
        WallClockResolution::NoDatedHistory => Err(unavailable(
            "branch has no commits carrying a wall-clock instant",
        )),
        WallClockResolution::InconsistentDating => Err(unavailable(
            "branch timeline mixes dated and undated commits inconsistently",
        )),
    }
}

/// #3112 S4: the wall-clock instants for a batch of commit versions.
///
/// Instants live only in the retained-timeline index, so an unproven index
/// yields all-unknown rather than an error: history itself is still perfectly
/// readable, only its dates are missing. That is a weaker failure than a
/// wall-clock `as_of`, which refuses — asking "when did this happen" and
/// getting "unknown" is a usable answer; asking "what did it look like then"
/// and getting the wrong commit is not.
pub(super) fn timeline_committed_at_for_versions(
    view: &BranchReadView,
    versions: &[CommitVersion],
) -> Vec<Option<Timestamp>> {
    let unknown = || vec![None; versions.len()];
    let Some((index, live_active)) = view.retained_timeline() else {
        return unknown();
    };
    index
        .committed_at_for_versions(versions, pinned_view_bound(view, live_active))
        .unwrap_or_else(unknown)
}

/// W3.1a: `timestamp_for_version` with the same index-or-scan-and-seed shape.
pub(super) fn timeline_timestamp_for_version(
    view: &BranchReadView,
    version: CommitVersion,
) -> StorageApiResult<Option<Timestamp>> {
    let Some((index, live_active)) = view.retained_timeline() else {
        return Ok(timeline_view_from_read_view(view)?.timestamp_for_version(version));
    };
    match index.timestamp_for_version(version, pinned_view_bound(view, live_active)) {
        crate::timeline_index::RetainedVersionLookup::Found(timestamp) => {
            return Ok(Some(timestamp));
        }
        crate::timeline_index::RetainedVersionLookup::Absent => return Ok(None),
        crate::timeline_index::RetainedVersionLookup::Unproven => {}
    }
    let scanned = timeline_view_from_read_view(view)?;
    seed_retained_timeline(index, &scanned);
    Ok(scanned.timestamp_for_version(version))
}

/// W3.1c: materialize a `CommitTimelineView` for the cold public timeline
/// surfaces (lookups, bounds, fork-at-timestamp). Index-first: commits no
/// longer write timeline rows, so a provably complete index is the ONLY
/// current source; the scan remains exact for testkit views and legacy
/// pre-elision rows, and seeds the index when it runs.
pub(super) fn timeline_view_or_index(
    view: &BranchReadView,
) -> StorageApiResult<CommitTimelineView> {
    if let Some((index, live_active)) = view.retained_timeline() {
        if let Some(entries) = index.materialized_entries(pinned_view_bound(view, live_active)) {
            let entries = entries
                .iter()
                .map(|entry| {
                    CommitTimelineEntry::new(
                        view.branch_id(),
                        entry.commit_version(),
                        entry.commit_timestamp(),
                    )
                    .map_err(commit_error)
                })
                .collect::<StorageApiResult<Vec<_>>>()?;
            return Ok(CommitTimelineView::from_entries(view.branch_id(), entries));
        }
        let scanned = timeline_view_from_read_view(view)?;
        seed_retained_timeline(index, &scanned);
        return Ok(scanned);
    }
    timeline_view_from_read_view(view)
}

/// A pinned view answers as of its captured facts; an empty branch pins to
/// version zero (an empty index prefix), matching an empty scan.
fn pinned_view_bound(view: &BranchReadView, live_active: bool) -> Option<CommitVersion> {
    if live_active {
        None
    } else {
        Some(
            view.facts()
                .max_commit_version()
                .unwrap_or(CommitVersion::ZERO),
        )
    }
}

fn retained_lookup_to_commit_lookup(
    query_timestamp: Timestamp,
    lookup: crate::timeline_index::RetainedTimelineLookup,
) -> CommitTimelineLookup {
    use crate::timeline_index::RetainedTimelineLookup as Retained;
    match lookup {
        Retained::Matched(entry) => CommitTimelineLookup::from_retained_parts(
            query_timestamp,
            Some((entry.commit_version(), entry.commit_timestamp())),
            CommitTimelineMiss::Matched,
        ),
        Retained::AfterLatestRetained(entry) => CommitTimelineLookup::from_retained_parts(
            query_timestamp,
            Some((entry.commit_version(), entry.commit_timestamp())),
            CommitTimelineMiss::AfterLatestRetained,
        ),
        Retained::BeforeRetainedHistory => CommitTimelineLookup::from_retained_parts(
            query_timestamp,
            None,
            CommitTimelineMiss::BeforeRetainedHistory,
        ),
        Retained::Empty => CommitTimelineLookup::from_retained_parts(
            query_timestamp,
            None,
            CommitTimelineMiss::Empty,
        ),
    }
}

fn seed_retained_timeline(
    index: &std::sync::Arc<crate::timeline_index::RetainedCommitTimeline>,
    scanned: &CommitTimelineView,
) {
    let entries = scanned
        .entries_by_version()
        .iter()
        .map(|entry| {
            crate::timeline_index::RetainedTimelineEntry::new(
                entry.commit_version(),
                entry.commit_timestamp(),
            )
        })
        .collect::<Vec<_>>();
    index.seed_from_scan(&entries);
}

pub(super) fn timeline_view_from_read_view(
    view: &BranchReadView,
) -> StorageApiResult<CommitTimelineView> {
    // This intentionally rebuilds the timeline from branch rows today. The public
    // boundary should grow a retained timeline index/cache before high-cardinality
    // timestamp reads become a hot path.
    let bounds = BranchScanBounds::unbounded(
        view.branch_id(),
        COMMIT_TIMELINE_SPACE,
        RowStorageSpaceId::COMMIT_TIMELINE,
    )
    .map_err(branch_error)?;
    let timeline_rows = view
        .scan_range_including_tombstones(&bounds, BranchReadBound::Latest)
        .map_err(branch_error)?;
    CommitTimelineView::from_rows(
        view.branch_id(),
        timeline_rows
            .iter()
            .map(crate::branch::read::BranchHistoryRow::row),
    )
    .map_err(commit_error)
}

pub(super) fn map_api_commit_batch(
    batch: &CommitBatch,
    timestamp_base: Timestamp,
    timestamp_policy: crate::commit::CommitTimestampPolicy,
    durability: crate::commit::CommitDurabilityMode,
) -> StorageApiResult<crate::commit::CommitBatch> {
    let mut mutations = Vec::with_capacity(batch.mutations().len());
    for mutation in batch.mutations() {
        match mutation {
            crate::api::CommitMutation::Put {
                storage_space,
                key,
                value,
                ttl,
            } => mutations.push(crate::commit::CommitMutation::put(
                physical_key(batch.branch_id(), storage_space, key)?,
                value.as_bytes().to_vec(),
                map_expiry(timestamp_base, *ttl)?,
                crate::commit::CommitRetentionHint::Append,
            )),
            crate::api::CommitMutation::Delete { storage_space, key } => {
                mutations.push(crate::commit::CommitMutation::delete(physical_key(
                    batch.branch_id(),
                    storage_space,
                    key,
                )?));
            }
        }
    }

    let mut cas_set = Vec::with_capacity(batch.conditions().len());
    for condition in batch.conditions() {
        let expected = match condition.expected() {
            CommitExpectedVersion::Absent => crate::commit::CommitObservedVersion::Missing,
            CommitExpectedVersion::Present(version) => {
                crate::commit::CommitObservedVersion::Present(version)
            }
        };
        cas_set.push(crate::commit::CommitCasFact::new(
            physical_key(
                batch.branch_id(),
                condition.storage_space(),
                condition.key(),
            )?,
            expected,
        ));
    }

    let conflict_validation =
        if batch.options().conflict_check_required() || !batch.conditions().is_empty() {
            crate::commit::CommitConflictValidationMode::Validate
        } else {
            crate::commit::CommitConflictValidationMode::Skip
        };
    let options = crate::commit::CommitBatchOptions::new(
        durability,
        conflict_validation,
        crate::commit::CommitDuplicateKeyPolicy::Reject,
        timestamp_policy,
        crate::commit::CommitOrigin::StorageRuntime,
    )
    .with_committed_at(batch.options().committed_at());
    Ok(crate::commit::CommitBatch::mutating(
        batch.branch_id(),
        mutations,
        crate::commit::CommitValidationFacts::new(Vec::new(), cas_set),
        options,
    ))
}

pub(super) fn map_expiry(
    timestamp: Timestamp,
    ttl: Option<std::time::Duration>,
) -> StorageApiResult<crate::commit::CommitExpiry> {
    let Some(ttl) = ttl else {
        return Ok(crate::commit::CommitExpiry::None);
    };
    if ttl.is_zero() {
        return Err(StorageApiError::InvalidArgument {
            field: "ttl",
            reason: "ttl duration must be greater than zero",
        });
    }
    let ttl_micros =
        u64::try_from(ttl.as_micros()).map_err(|_| StorageApiError::InvalidArgument {
            field: "ttl",
            reason: "ttl duration is too large",
        })?;
    let expires_at = timestamp
        .as_micros()
        .checked_add(ttl_micros)
        .map(Timestamp::from_micros)
        .ok_or(StorageApiError::InvalidArgument {
            field: "ttl",
            reason: "ttl expiration overflows timestamp",
        })?;
    Ok(crate::commit::CommitExpiry::At(expires_at))
}

pub(super) fn flush_request_for_boundary(
    branch_id: BranchId,
) -> StorageApiResult<FlushFrozenRequest> {
    FlushFrozenRequest::new(
        branch_id,
        None,
        FlushTableIdentitySeed::new(format!("storage-boundary-flush-{branch_id}"))
            .map_err(map_lifecycle_error)?,
        FlushTableObjectId::new(format!("storage-boundary-flush-{branch_id}"))
            .map_err(map_lifecycle_error)?,
    )
    .map_err(map_lifecycle_error)
}
