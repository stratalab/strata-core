//! #3643: sealed retained-timeline segment objects
//! (`timeline/<sealing snapshot id>/<ordinal>`), the snapshot family's second
//! object kind. A checkpoint publishes its segments before its snapshot;
//! recovery loads the ones the attested snapshot references; the snapshot
//! prune reclaims the ones nothing live references.

use std::collections::BTreeSet;

use super::{
    read_snapshot_optional, require_capability, SnapshotService, SnapshotServiceError,
    SnapshotServiceResult,
};
use crate::backend::{Backend, BackendCapability, BackendErrorKind};
use crate::format::{
    decode_timeline_segment, encode_timeline_segment, DecodedTimelineSegment,
    SnapshotTimelineEntry, TimelineSegmentRef,
};
use crate::layout::{ObjectLayout, TimelineSegmentId};
use crate::object::ObjectName;
use crate::service::{durable_cleanup_succeeded, validate_publish_outcome, ObjectPublisher};

/// Which unreferenced segments a prune may delete, mirroring the snapshot
/// modes. `Superseded` spares anything sealed at or above the live snapshot id
/// (a checkpoint in flight seals under a higher id); `ReconcileToAttested`
/// runs only where no publish can be in flight and deletes every
/// unreferenced segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimelineSegmentPruneMode {
    Superseded,
    ReconcileToAttested,
}

/// #3643: whether a listed segment is dead. A segment the live snapshot
/// references is never dead; otherwise `Superseded` requires it be sealed by a
/// snapshot older than the live one.
pub(crate) fn timeline_segment_is_dead(
    mode: TimelineSegmentPruneMode,
    segment: TimelineSegmentId,
    live_snapshot_id: u64,
    referenced: &BTreeSet<TimelineSegmentId>,
) -> bool {
    if referenced.contains(&segment) {
        return false;
    }
    match mode {
        TimelineSegmentPruneMode::Superseded => segment.sealing_snapshot_id < live_snapshot_id,
        TimelineSegmentPruneMode::ReconcileToAttested => true,
    }
}

/// What a segment prune did: deletions with their bytes, and failures (left
/// for the next prune).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TimelineSegmentPruneReport {
    pub(crate) deleted: Vec<(TimelineSegmentId, u64)>,
    pub(crate) failed: usize,
}

impl TimelineSegmentPruneReport {
    pub(crate) fn reclaimed_bytes(&self) -> u64 {
        self.deleted
            .iter()
            .map(|(_, bytes)| *bytes)
            .fold(0, u64::saturating_add)
    }
}

impl SnapshotService<'_> {
    /// Publish one segment and return its reference. Replace, not create: a
    /// segment is sealed under a snapshot id above the live one, so nothing
    /// live can reference that name, and a checkpoint retried under the same
    /// id after a failure must be able to rewrite it.
    pub(crate) fn publish_timeline_segment(
        &self,
        id: TimelineSegmentId,
        entries: &[SnapshotTimelineEntry],
    ) -> SnapshotServiceResult<TimelineSegmentRef> {
        let object = timeline_segment_object(id)?;
        let bytes =
            encode_timeline_segment(entries).map_err(|source| SnapshotServiceError::Encode {
                object: object.clone(),
                snapshot_id: id.sealing_snapshot_id,
                source,
            })?;
        let decoded = decode_segment(&object, id, &bytes)?;
        let outcome = ObjectPublisher::new(&self.backend)
            .publish_durable_replace(&object, &bytes)
            .map_err(|source| SnapshotServiceError::Publish {
                snapshot_id: id.sealing_snapshot_id,
                source,
            })?;
        validate_publish_outcome(&object, bytes.len() as u64, &outcome).map_err(|mismatch| {
            SnapshotServiceError::InvalidPublishMetadata {
                object: mismatch.object().clone(),
                snapshot_id: id.sealing_snapshot_id,
                field: mismatch.field(),
            }
        })?;
        let ordinal =
            u32::try_from(id.ordinal).map_err(|_| SnapshotServiceError::InvalidSnapshotFact {
                snapshot_id: id.sealing_snapshot_id,
                field: "timeline_segment_ordinal",
            })?;
        segment_ref(id, ordinal, &decoded)
    }

    /// Load the segment a reference names and check it is exactly what the
    /// reference says: entry count, version range and CRC32.
    pub(crate) fn load_timeline_segment(
        &self,
        segment: &TimelineSegmentRef,
    ) -> SnapshotServiceResult<Vec<SnapshotTimelineEntry>> {
        let id = TimelineSegmentId {
            sealing_snapshot_id: segment.sealing_snapshot_id,
            ordinal: u64::from(segment.ordinal),
        };
        let object = timeline_segment_object(id)?;
        let bytes = read_snapshot_optional(&self.backend, &object, id.sealing_snapshot_id)?
            .ok_or_else(|| SnapshotServiceError::Missing {
                object: object.clone(),
                snapshot_id: id.sealing_snapshot_id,
            })?;
        let decoded = decode_segment(&object, id, &bytes)?;
        if segment_ref(id, segment.ordinal, &decoded)? != *segment {
            return Err(SnapshotServiceError::Decode {
                object,
                snapshot_id: id.sealing_snapshot_id,
                source: crate::format::FormatError::InvalidValue {
                    field: "timeline_segment_ref_mismatch",
                },
            });
        }
        Ok(decoded.entries)
    }

    /// Every listed segment. Objects outside the family are ignored; a
    /// malformed name inside it fails closed, like the snapshot listing.
    pub(crate) fn list_timeline_segments(
        &self,
    ) -> SnapshotServiceResult<Vec<(TimelineSegmentId, ObjectName)>> {
        require_capability(&self.backend, BackendCapability::ListPrefix)?;
        let prefix = ObjectLayout::timeline_prefix()
            .map_err(|source| SnapshotServiceError::Layout { source })?;
        let mut segments = Vec::new();
        for object in self
            .backend
            .list_prefix(&prefix)
            .map_err(|source| SnapshotServiceError::List { source })?
        {
            match ObjectLayout::classify_timeline_segment_object(&object) {
                Ok(Some(id)) => segments.push((id, object)),
                Ok(None) => {}
                Err(_) => {
                    return Err(SnapshotServiceError::InvalidListedObject {
                        object,
                        source: crate::backend::BackendError::new(
                            BackendErrorKind::InvalidObjectName,
                            "listed timeline segment object has a malformed name",
                        ),
                    })
                }
            }
        }
        segments.sort_by_key(|(id, _)| *id);
        Ok(segments)
    }

    /// Every listed segment with its on-disk size; one that vanishes between
    /// the listing and its stat (a concurrent prune) is dropped.
    pub(crate) fn list_timeline_segment_sizes(
        &self,
    ) -> SnapshotServiceResult<Vec<(TimelineSegmentId, u64)>> {
        require_capability(&self.backend, BackendCapability::ObjectMetadata)?;
        let mut sizes = Vec::new();
        for (id, object) in self.list_timeline_segments()? {
            match self.backend.object_metadata(&object) {
                Ok(metadata) => sizes.push((id, metadata.size_bytes())),
                Err(source) if source.kind() == BackendErrorKind::NotFound => {}
                Err(source) => return Err(SnapshotServiceError::List { source }),
            }
        }
        Ok(sizes)
    }

    /// Delete every listed segment `timeline_segment_is_dead` rules dead.
    pub(crate) fn prune_timeline_segments(
        &self,
        mode: TimelineSegmentPruneMode,
        live_snapshot_id: u64,
        referenced: &BTreeSet<TimelineSegmentId>,
    ) -> SnapshotServiceResult<TimelineSegmentPruneReport> {
        require_capability(&self.backend, BackendCapability::DeleteObject)?;
        let mut report = TimelineSegmentPruneReport::default();
        for (id, object) in self.list_timeline_segments()? {
            if !timeline_segment_is_dead(mode, id, live_snapshot_id, referenced) {
                continue;
            }
            let size = self
                .backend
                .object_metadata(&object)
                .map_or(0, |metadata| metadata.size_bytes());
            match self.backend.delete_object(&object) {
                Ok(outcome) if durable_cleanup_succeeded(&outcome) => {
                    report.deleted.push((id, size));
                }
                Err(source) if source.source_error().kind() == BackendErrorKind::NotFound => {}
                Ok(_) | Err(_) => report.failed += 1,
            }
        }
        Ok(report)
    }
}

fn timeline_segment_object(id: TimelineSegmentId) -> SnapshotServiceResult<ObjectName> {
    ObjectLayout::timeline_segment(id).map_err(|source| SnapshotServiceError::Layout { source })
}

fn decode_segment(
    object: &ObjectName,
    id: TimelineSegmentId,
    bytes: &[u8],
) -> SnapshotServiceResult<DecodedTimelineSegment> {
    decode_timeline_segment(bytes).map_err(|source| SnapshotServiceError::Decode {
        object: object.clone(),
        snapshot_id: id.sealing_snapshot_id,
        source,
    })
}

fn segment_ref(
    id: TimelineSegmentId,
    ordinal: u32,
    decoded: &DecodedTimelineSegment,
) -> SnapshotServiceResult<TimelineSegmentRef> {
    let (Some(first), Some(last)) = (decoded.entries.first(), decoded.entries.last()) else {
        return Err(SnapshotServiceError::InvalidSnapshotFact {
            snapshot_id: id.sealing_snapshot_id,
            field: "timeline_segment_entries",
        });
    };
    let entry_count = u32::try_from(decoded.entries.len()).map_err(|_| {
        SnapshotServiceError::InvalidSnapshotFact {
            snapshot_id: id.sealing_snapshot_id,
            field: "timeline_segment_entries",
        }
    })?;
    Ok(TimelineSegmentRef {
        sealing_snapshot_id: id.sealing_snapshot_id,
        ordinal,
        entry_count,
        first_version: first.commit_version,
        last_version: last.commit_version,
        crc32: decoded.crc32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(sealing: u64, ordinal: u64) -> TimelineSegmentId {
        TimelineSegmentId {
            sealing_snapshot_id: sealing,
            ordinal,
        }
    }

    /// #3643: a referenced segment is never dead; otherwise `Superseded`
    /// spares anything sealed at or above the live id (a checkpoint in flight)
    /// and `ReconcileToAttested` spares nothing.
    #[test]
    fn timeline_segment_is_dead_truth_table() {
        let referenced: BTreeSet<_> = [id(3, 0), id(5, 1)].into_iter().collect();
        for (mode, segment, expected) in [
            (TimelineSegmentPruneMode::Superseded, id(3, 0), false),
            (TimelineSegmentPruneMode::Superseded, id(5, 1), false),
            (TimelineSegmentPruneMode::Superseded, id(3, 1), true),
            (TimelineSegmentPruneMode::Superseded, id(4, 0), true),
            (TimelineSegmentPruneMode::Superseded, id(5, 0), false),
            (TimelineSegmentPruneMode::Superseded, id(6, 0), false),
            (
                TimelineSegmentPruneMode::ReconcileToAttested,
                id(3, 0),
                false,
            ),
            (
                TimelineSegmentPruneMode::ReconcileToAttested,
                id(3, 1),
                true,
            ),
            (
                TimelineSegmentPruneMode::ReconcileToAttested,
                id(5, 0),
                true,
            ),
            (
                TimelineSegmentPruneMode::ReconcileToAttested,
                id(6, 0),
                true,
            ),
        ] {
            assert_eq!(
                timeline_segment_is_dead(mode, segment, 5, &referenced),
                expected,
                "{mode:?} {segment:?}"
            );
        }
    }
}
