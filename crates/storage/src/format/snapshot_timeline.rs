//! W3.1b: the checkpoint snapshot's retained-timeline section.
//!
//! One section per checkpoint, holding per-branch `(commit_version,
//! commit_timestamp, committed_at)` groups — the persisted form of the retained-timeline
//! index (`timeline_index.rs`), written only for branches whose index is
//! complete at checkpoint time and restored at recovery so reopen never
//! rescans the timeline space. Layout (all little-endian):
//!
//! ```text
//! group_count: u32
//! repeated group:
//!   branch_id:   16 bytes
//!   entry_count: u32
//!   repeated entry: commit_version: u64, commit_timestamp_micros: u64,
//!                   committed_at_micros: u64   (kind 3 only; 0 = unknown)
//! ```
//!
//! The entry width is a property of the SECTION KIND (#3112 S2c): kind 3 is
//! current and carries the wall-clock `committed_at`; kind 2 predates the field
//! and is still decoded, with every instant unknown. A new kind rather than a
//! widened kind 2, because widening in place would misparse every checkpoint
//! written before this.
//!
//! Entries within a group MUST be strictly ascending by commit version —
//! validated at decode, so a corrupt section fails closed rather than seeding
//! a wrong index.

use super::{FormatError, SnapshotSection};
use strata_core::{BranchId, CommitVersion, Timestamp};

/// Current section kind: entries carry the wall-clock `committed_at` (#3112
/// S2c). A new kind rather than a widened kind 2, because widening in place
/// would misparse every checkpoint written before this — same backward-compat
/// stance as the WAL record's version 3.
pub(crate) const SNAPSHOT_TIMELINE_SECTION_KIND: u8 = 3;
/// The pre-`committed_at` section kind. Still decoded, with every instant
/// unknown; never written any more.
pub(crate) const SNAPSHOT_TIMELINE_SECTION_KIND_LEGACY: u8 = 2;

const GROUP_HEADER_BYTES: usize = BranchId::BYTE_LEN + 4;
/// Entry width by section kind: legacy is `(commit_version, commit_timestamp)`;
/// the current kind appends `committed_at`.
const ENTRY_BYTES_LEGACY: usize = 16;
pub(super) const ENTRY_BYTES: usize = 24;
/// Fail-closed ceiling mirroring the row section's materialization guard: a
/// section that decode would reject is never written.
const MAX_TIMELINE_ENTRIES: usize = 1 << 28;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotTimelineBranchGroup {
    pub(crate) branch_id: BranchId,
    pub(crate) entries: Vec<SnapshotTimelineEntry>,
}

/// One persisted timeline entry. A named struct rather than a tuple because
/// `commit_timestamp` (the logical commit clock) and `committed_at` (the wall
/// clock) are both `Timestamp` — positionally they would be trivial to swap,
/// and swapping them would silently corrupt as-of resolution (#3112 S2c).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotTimelineEntry {
    pub(crate) commit_version: CommitVersion,
    pub(crate) commit_timestamp: Timestamp,
    /// Wall-clock instant, `0` on the wire meaning unknown (the format's
    /// `optional_nonzero` convention). Always unknown when restored from a
    /// legacy kind-2 section, which predates the field.
    pub(crate) committed_at: Option<Timestamp>,
}

pub(crate) fn encode_snapshot_timeline_section(
    groups: &[SnapshotTimelineBranchGroup],
) -> Result<SnapshotSection, FormatError> {
    let group_count = u32::try_from(groups.len()).map_err(|_| FormatError::InvalidLength {
        field: "timeline_group_count",
    })?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&group_count.to_le_bytes());
    for group in groups {
        validate_group_entries(&group.entries)?;
        let entry_count =
            u32::try_from(group.entries.len()).map_err(|_| FormatError::InvalidLength {
                field: "timeline_entry_count",
            })?;
        payload.extend_from_slice(group.branch_id.as_bytes());
        payload.extend_from_slice(&entry_count.to_le_bytes());
        for entry in &group.entries {
            encode_timeline_entry(&mut payload, entry);
        }
    }
    SnapshotSection::new(SNAPSHOT_TIMELINE_SECTION_KIND, payload)
}

/// One current-kind entry: `commit_version`, `commit_timestamp`,
/// `committed_at` (0 = unknown, the format's `optional_nonzero` convention for
/// optional u64s, #3112 S2c). Shared with the #3643 timeline segment object,
/// whose entries are byte-identical.
pub(super) fn encode_timeline_entry(payload: &mut Vec<u8>, entry: &SnapshotTimelineEntry) {
    payload.extend_from_slice(&entry.commit_version.as_u64().to_le_bytes());
    payload.extend_from_slice(&entry.commit_timestamp.as_micros().to_le_bytes());
    payload.extend_from_slice(
        &entry
            .committed_at
            .map_or(0, Timestamp::as_micros)
            .to_le_bytes(),
    );
}

pub(crate) fn decode_snapshot_timeline_payload(
    payload: &[u8],
    section_kind: u8,
) -> Result<Vec<SnapshotTimelineBranchGroup>, FormatError> {
    // The entry width is a property of the section kind (#3112 S2c): a legacy
    // section's entries are 8 bytes narrower because they predate
    // `committed_at`. Reading one with the wrong width would silently
    // reinterpret every field, so an unknown kind fails closed here.
    let entry_bytes = match section_kind {
        SNAPSHOT_TIMELINE_SECTION_KIND => ENTRY_BYTES,
        SNAPSHOT_TIMELINE_SECTION_KIND_LEGACY => ENTRY_BYTES_LEGACY,
        _ => {
            return Err(FormatError::InvalidValue {
                field: "snapshot_timeline_section_kind",
            })
        }
    };
    let mut offset = 0usize;
    let group_count = read_u32(payload, &mut offset, "timeline_group_count")?;
    let mut groups = Vec::new();
    let mut total_entries = 0usize;
    for _ in 0..group_count {
        if payload.len() < offset.saturating_add(GROUP_HEADER_BYTES) {
            return Err(FormatError::InsufficientBytes {
                format: "snapshot_timeline_section",
                needed: offset.saturating_add(GROUP_HEADER_BYTES),
                actual: payload.len(),
            });
        }
        let mut branch_bytes = [0u8; BranchId::BYTE_LEN];
        branch_bytes.copy_from_slice(&payload[offset..offset + BranchId::BYTE_LEN]);
        offset += BranchId::BYTE_LEN;
        let entry_count = read_u32(payload, &mut offset, "timeline_entry_count")? as usize;
        total_entries =
            total_entries
                .checked_add(entry_count)
                .ok_or(FormatError::InvalidLength {
                    field: "timeline_entry_count",
                })?;
        if total_entries > MAX_TIMELINE_ENTRIES {
            return Err(FormatError::InvalidLength {
                field: "timeline_entry_count",
            });
        }
        let entries_end = offset
            .checked_add(entry_count.saturating_mul(entry_bytes))
            .ok_or(FormatError::InvalidLength {
                field: "timeline_entry_count",
            })?;
        if payload.len() < entries_end {
            return Err(FormatError::InsufficientBytes {
                format: "snapshot_timeline_section",
                needed: entries_end,
                actual: payload.len(),
            });
        }
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let version = read_u64(payload, &mut offset, "timeline_commit_version")?;
            let timestamp = read_u64(payload, &mut offset, "timeline_commit_timestamp")?;
            // A legacy kind-2 section predates the field: every instant is
            // unknown, which is a legal state (#3112 S2c).
            let committed_at = if entry_bytes == ENTRY_BYTES_LEGACY {
                None
            } else {
                let micros = read_u64(payload, &mut offset, "timeline_committed_at")?;
                (micros != 0).then(|| Timestamp::from_micros(micros))
            };
            entries.push(SnapshotTimelineEntry {
                commit_version: CommitVersion::new(version),
                commit_timestamp: Timestamp::from_micros(timestamp),
                committed_at,
            });
        }
        validate_group_entries(&entries)?;
        groups.push(SnapshotTimelineBranchGroup {
            branch_id: BranchId::from_bytes(branch_bytes),
            entries,
        });
    }
    if offset != payload.len() {
        return Err(FormatError::InvalidLength {
            field: "timeline_section_trailing_bytes",
        });
    }
    Ok(groups)
}

pub(super) fn validate_group_entries(entries: &[SnapshotTimelineEntry]) -> Result<(), FormatError> {
    let ascending = entries
        .windows(2)
        .all(|pair| pair[0].commit_version.as_u64() < pair[1].commit_version.as_u64());
    if !ascending {
        return Err(FormatError::InvalidValue {
            field: "timeline_entry_order",
        });
    }
    if entries
        .iter()
        .any(|entry| entry.commit_version == CommitVersion::ZERO)
    {
        return Err(FormatError::InvalidValue {
            field: "timeline_commit_version",
        });
    }
    Ok(())
}

fn read_u32(payload: &[u8], offset: &mut usize, field: &'static str) -> Result<u32, FormatError> {
    let end = offset
        .checked_add(4)
        .ok_or(FormatError::InvalidLength { field })?;
    if payload.len() < end {
        return Err(FormatError::InsufficientBytes {
            format: "snapshot_timeline_section",
            needed: end,
            actual: payload.len(),
        });
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&payload[*offset..end]);
    *offset = end;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(payload: &[u8], offset: &mut usize, field: &'static str) -> Result<u64, FormatError> {
    let end = offset
        .checked_add(8)
        .ok_or(FormatError::InvalidLength { field })?;
    if payload.len() < end {
        return Err(FormatError::InsufficientBytes {
            format: "snapshot_timeline_section",
            needed: end,
            actual: payload.len(),
        });
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[*offset..end]);
    *offset = end;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn branch(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; BranchId::BYTE_LEN])
    }

    /// Entries with no wall-clock instant (the common case in these tests).
    fn group(byte: u8, entries: &[(u64, u64)]) -> SnapshotTimelineBranchGroup {
        SnapshotTimelineBranchGroup {
            branch_id: branch(byte),
            entries: entries
                .iter()
                .map(|(version, ts)| SnapshotTimelineEntry {
                    commit_version: CommitVersion::new(*version),
                    commit_timestamp: Timestamp::from_micros(*ts),
                    committed_at: None,
                })
                .collect(),
        }
    }

    /// Entries carrying a wall-clock instant (#3112 S2c).
    fn group_with_instants(byte: u8, entries: &[(u64, u64, u64)]) -> SnapshotTimelineBranchGroup {
        SnapshotTimelineBranchGroup {
            branch_id: branch(byte),
            entries: entries
                .iter()
                .map(|(version, ts, instant)| SnapshotTimelineEntry {
                    commit_version: CommitVersion::new(*version),
                    commit_timestamp: Timestamp::from_micros(*ts),
                    committed_at: (*instant != 0).then(|| Timestamp::from_micros(*instant)),
                })
                .collect(),
        }
    }

    #[test]
    fn timeline_section_round_trips_multi_branch_groups() {
        let groups = vec![
            group(1, &[(1, 10), (2, 20), (5, 21)]),
            group(2, &[]),
            group(3, &[(7, 70)]),
        ];
        let section = encode_snapshot_timeline_section(&groups).expect("encode");
        assert_eq!(section.section_kind(), SNAPSHOT_TIMELINE_SECTION_KIND);
        let decoded =
            decode_snapshot_timeline_payload(section.payload(), SNAPSHOT_TIMELINE_SECTION_KIND)
                .expect("decode");
        assert_eq!(decoded, groups);
    }

    #[test]
    fn timeline_section_round_trips_wall_clock_instants() {
        // #3112 S2c: the current kind carries `committed_at`, with 0 meaning
        // unknown, so a mix of known and unknown must survive the round trip.
        let groups = vec![group_with_instants(
            1,
            &[(1, 10, 1_788_000_000_654_321), (2, 20, 0), (3, 30, 1)],
        )];
        let section = encode_snapshot_timeline_section(&groups).expect("encode");
        assert_eq!(section.section_kind(), SNAPSHOT_TIMELINE_SECTION_KIND);

        let decoded =
            decode_snapshot_timeline_payload(section.payload(), SNAPSHOT_TIMELINE_SECTION_KIND)
                .expect("decode");
        assert_eq!(decoded, groups);
        assert_eq!(
            decoded[0].entries[0].committed_at,
            Some(Timestamp::from_micros(1_788_000_000_654_321))
        );
        assert_eq!(decoded[0].entries[1].committed_at, None, "0 means unknown");
        assert_eq!(
            decoded[0].entries[2].committed_at,
            Some(Timestamp::from_micros(1)),
            "1 is a real instant, not the unknown sentinel"
        );
    }

    #[test]
    fn legacy_section_decodes_with_unknown_instants() {
        // #3112 S2c: a checkpoint written before `committed_at` existed uses
        // kind 2 with 8-byte-narrower entries. It MUST stay restorable, with
        // every instant unknown — reading it at the current width would
        // silently reinterpret every field.
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u32.to_le_bytes()); // group_count
        payload.extend_from_slice(branch(1).as_bytes());
        payload.extend_from_slice(&2u32.to_le_bytes()); // entry_count
        for (version, timestamp) in [(1u64, 10u64), (2, 20)] {
            payload.extend_from_slice(&version.to_le_bytes());
            payload.extend_from_slice(&timestamp.to_le_bytes());
        }

        let decoded =
            decode_snapshot_timeline_payload(&payload, SNAPSHOT_TIMELINE_SECTION_KIND_LEGACY)
                .expect("legacy section decodes");
        assert_eq!(decoded, vec![group(1, &[(1, 10), (2, 20)])]);
        assert!(decoded[0]
            .entries
            .iter()
            .all(|entry| entry.committed_at.is_none()));

        // Reading the same bytes at the CURRENT width must fail closed rather
        // than silently mis-slicing the entries.
        assert!(
            decode_snapshot_timeline_payload(&payload, SNAPSHOT_TIMELINE_SECTION_KIND).is_err(),
            "legacy bytes must not parse at the current entry width"
        );
        // An unknown kind is rejected outright.
        assert!(decode_snapshot_timeline_payload(&payload, 9).is_err());
    }

    #[test]
    fn timeline_section_rejects_disorder_zero_versions_and_trailing_bytes() {
        assert!(encode_snapshot_timeline_section(&[group(1, &[(2, 20), (1, 10)])]).is_err());
        assert!(encode_snapshot_timeline_section(&[group(1, &[(0, 0)])]).is_err());

        let section = encode_snapshot_timeline_section(&[group(1, &[(1, 10)])]).expect("encode");
        let mut payload = section.payload().to_vec();
        payload.push(0);
        assert!(
            decode_snapshot_timeline_payload(&payload, SNAPSHOT_TIMELINE_SECTION_KIND).is_err()
        );
        assert!(decode_snapshot_timeline_payload(
            &payload[..payload.len() - 3],
            SNAPSHOT_TIMELINE_SECTION_KIND,
        )
        .is_err());
    }
}
