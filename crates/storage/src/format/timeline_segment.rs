//! #3643: sealed retained-timeline segments and the snapshot section that
//! references them (design: `docs/design/3643-timeline-segments.md`).
//!
//! A branch's retained timeline is cut into fixed chunks of
//! [`TIMELINE_CHUNK_ENTRIES`] entries, aligned by entry count from the start of
//! the branch's history. Each chunk is an immutable **segment object**
//! (`timeline/<sealing snapshot id>/<ordinal>`), and a checkpoint snapshot
//! holds only a bounded **reference** to each, in section kind 5. The segment
//! and its reference are the two codecs here.
//!
//! Segment object (little-endian):
//!
//! ```text
//! magic "TLSG" | format_version u32 = 1 | entry_count u32 | reserved u32 = 0
//! first_version u64 | last_version u64
//! repeated entry (24 bytes, identical to a kind-3 timeline entry)
//! crc32 u32 over every preceding byte
//! ```
//!
//! Reference section, kind 5:
//!
//! ```text
//! group_count u32
//! repeated group (strictly ascending, unique by branch_id):
//!   branch_id 16 bytes | ref_count u32
//!   repeated ref (40 bytes):
//!     sealing_snapshot_id u64 | ordinal u32 | entry_count u32
//!     first_version u64 | last_version u64 | crc32 u32 | reserved u32 = 0
//! ```
//!
//! Within a group the refs cover the branch's entries in order: versions are
//! strictly ascending across refs, and every ref but the last covers exactly a
//! full chunk. A group with no refs is a branch whose complete timeline is
//! empty. Both codecs validate at encode and at decode, so a corrupt object or
//! section fails closed rather than seeding a wrong index.

use super::snapshot_timeline::{encode_timeline_entry, validate_group_entries, ENTRY_BYTES};
use super::{ByteReader, FormatError, SnapshotSection, SnapshotTimelineEntry};
use strata_core::{BranchId, CommitVersion, Timestamp};

/// Entries per sealed chunk: a full segment is about 1.5 MiB, far under the
/// snapshot payload cap, so every segment decode is independently bounded
/// (DUR-017).
pub(crate) const TIMELINE_CHUNK_ENTRIES: usize = 65_536;

/// Section kind 5: the per-branch segment references. Kinds 1–4 are the row,
/// retained-timeline (legacy and current) and durable-base sections.
pub(crate) const SNAPSHOT_TIMELINE_SEGMENTS_SECTION_KIND: u8 = 5;

const SEGMENT_FORMAT: &str = "timeline_segment";
const SEGMENT_SECTION_FORMAT: &str = "snapshot_timeline_segments_section";
const SEGMENT_MAGIC: &[u8; 4] = b"TLSG";
const SEGMENT_FORMAT_VERSION: u32 = 1;
const SEGMENT_HEADER_BYTES: usize = 32;
const SEGMENT_FOOTER_BYTES: usize = 4;
const GROUP_HEADER_BYTES: usize = BranchId::BYTE_LEN + 4;
const REF_BYTES: usize = 40;
/// Fail-closed ceiling on references across all groups: about 68 billion
/// retained commits. A section decode would reject is never written.
const MAX_SEGMENT_REFS: usize = 1 << 20;

/// One reference from a snapshot to a sealed segment object.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TimelineSegmentRef {
    pub(crate) sealing_snapshot_id: u64,
    pub(crate) ordinal: u32,
    pub(crate) entry_count: u32,
    pub(crate) first_version: CommitVersion,
    pub(crate) last_version: CommitVersion,
    /// The segment object's stored CRC32 footer.
    pub(crate) crc32: u32,
}

/// One branch's ordered segment references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotTimelineSegmentGroup {
    pub(crate) branch_id: BranchId,
    pub(crate) refs: Vec<TimelineSegmentRef>,
}

/// A decoded segment object: its entries and the CRC32 its footer carries
/// (what a reference must match).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedTimelineSegment {
    pub(crate) entries: Vec<SnapshotTimelineEntry>,
    pub(crate) crc32: u32,
}

/// Encode one segment object. Its entries must be one chunk: `1..=`
/// [`TIMELINE_CHUNK_ENTRIES`], strictly ascending and nonzero.
pub(crate) fn encode_timeline_segment(
    entries: &[SnapshotTimelineEntry],
) -> Result<Vec<u8>, FormatError> {
    validate_segment_entries(entries)?;
    let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
        return Err(segment_count_error());
    };
    let entry_count = u32::try_from(entries.len()).map_err(|_| segment_count_error())?;
    let mut bytes = SEGMENT_MAGIC.to_vec();
    bytes.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&entry_count.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&first.commit_version.as_u64().to_le_bytes());
    bytes.extend_from_slice(&last.commit_version.as_u64().to_le_bytes());
    for entry in entries {
        encode_timeline_entry(&mut bytes, entry);
    }
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

/// Decode one segment object, checking its footer before anything else, then
/// its header against its entries.
pub(crate) fn decode_timeline_segment(bytes: &[u8]) -> Result<DecodedTimelineSegment, FormatError> {
    let minimum = SEGMENT_HEADER_BYTES + SEGMENT_FOOTER_BYTES;
    if bytes.len() < minimum {
        return Err(FormatError::InsufficientBytes {
            format: SEGMENT_FORMAT,
            needed: minimum,
            actual: bytes.len(),
        });
    }
    let footer_offset = bytes.len() - SEGMENT_FOOTER_BYTES;
    let mut footer = [0u8; SEGMENT_FOOTER_BYTES];
    footer.copy_from_slice(&bytes[footer_offset..]);
    let stored_crc = u32::from_le_bytes(footer);
    let computed_crc = crc32fast::hash(&bytes[..footer_offset]);
    if stored_crc != computed_crc {
        return Err(FormatError::ChecksumMismatch {
            format: SEGMENT_FORMAT,
            expected: stored_crc,
            computed: computed_crc,
        });
    }

    let mut reader = ByteReader::new(SEGMENT_FORMAT, &bytes[..footer_offset]);
    if reader.read_exact(4)? != SEGMENT_MAGIC {
        return Err(FormatError::InvalidMagic {
            format: SEGMENT_FORMAT,
        });
    }
    let version = reader.read_u32_le()?;
    if version != SEGMENT_FORMAT_VERSION {
        return Err(FormatError::FutureFormat {
            format: SEGMENT_FORMAT,
            version,
            max_supported: SEGMENT_FORMAT_VERSION,
        });
    }
    let entry_count = reader.read_u32_le()? as usize;
    if entry_count == 0 || entry_count > TIMELINE_CHUNK_ENTRIES {
        return Err(segment_count_error());
    }
    if reader.read_u32_le()? != 0 {
        return Err(FormatError::InvalidValue {
            field: "timeline_segment_reserved",
        });
    }
    let first_version = reader.read_u64_le()?;
    let last_version = reader.read_u64_le()?;
    if reader.remaining() != entry_count * ENTRY_BYTES {
        return Err(FormatError::InvalidLength {
            field: "timeline_segment_entries",
        });
    }
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let version = reader.read_u64_le()?;
        let timestamp = reader.read_u64_le()?;
        let committed_at = reader.read_u64_le()?;
        entries.push(SnapshotTimelineEntry {
            commit_version: CommitVersion::new(version),
            commit_timestamp: Timestamp::from_micros(timestamp),
            committed_at: (committed_at != 0).then(|| Timestamp::from_micros(committed_at)),
        });
    }
    reader.finish()?;
    validate_segment_entries(&entries)?;
    let header_matches = entries
        .first()
        .is_some_and(|entry| entry.commit_version.as_u64() == first_version)
        && entries
            .last()
            .is_some_and(|entry| entry.commit_version.as_u64() == last_version);
    if !header_matches {
        return Err(FormatError::InvalidValue {
            field: "timeline_segment_version_range",
        });
    }
    Ok(DecodedTimelineSegment {
        entries,
        crc32: stored_crc,
    })
}

pub(crate) fn encode_snapshot_timeline_segments_section(
    groups: &[SnapshotTimelineSegmentGroup],
) -> Result<SnapshotSection, FormatError> {
    validate_segment_groups(groups)?;
    let group_count = u32::try_from(groups.len()).map_err(|_| FormatError::InvalidLength {
        field: "timeline_segment_group_count",
    })?;
    let mut payload = group_count.to_le_bytes().to_vec();
    for group in groups {
        let ref_count = u32::try_from(group.refs.len()).map_err(|_| ref_count_error())?;
        payload.extend_from_slice(group.branch_id.as_bytes());
        payload.extend_from_slice(&ref_count.to_le_bytes());
        for segment in &group.refs {
            payload.extend_from_slice(&segment.sealing_snapshot_id.to_le_bytes());
            payload.extend_from_slice(&segment.ordinal.to_le_bytes());
            payload.extend_from_slice(&segment.entry_count.to_le_bytes());
            payload.extend_from_slice(&segment.first_version.as_u64().to_le_bytes());
            payload.extend_from_slice(&segment.last_version.as_u64().to_le_bytes());
            payload.extend_from_slice(&segment.crc32.to_le_bytes());
            payload.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    SnapshotSection::new(SNAPSHOT_TIMELINE_SEGMENTS_SECTION_KIND, payload)
}

pub(crate) fn decode_snapshot_timeline_segments_payload(
    payload: &[u8],
) -> Result<Vec<SnapshotTimelineSegmentGroup>, FormatError> {
    let mut reader = ByteReader::new(SEGMENT_SECTION_FORMAT, payload);
    let group_count = reader.read_u32_le()? as usize;
    let mut groups = Vec::new();
    let mut total_refs = 0usize;
    for _ in 0..group_count {
        if reader.remaining() < GROUP_HEADER_BYTES {
            return Err(FormatError::InsufficientBytes {
                format: SEGMENT_SECTION_FORMAT,
                needed: GROUP_HEADER_BYTES,
                actual: reader.remaining(),
            });
        }
        let mut branch_bytes = [0u8; BranchId::BYTE_LEN];
        branch_bytes.copy_from_slice(reader.read_exact(BranchId::BYTE_LEN)?);
        let ref_count = reader.read_u32_le()? as usize;
        total_refs = total_refs
            .checked_add(ref_count)
            .filter(|total| *total <= MAX_SEGMENT_REFS)
            .ok_or_else(ref_count_error)?;
        if reader.remaining() < ref_count * REF_BYTES {
            return Err(FormatError::InsufficientBytes {
                format: SEGMENT_SECTION_FORMAT,
                needed: ref_count * REF_BYTES,
                actual: reader.remaining(),
            });
        }
        let mut refs = Vec::with_capacity(ref_count);
        for _ in 0..ref_count {
            let sealing_snapshot_id = reader.read_u64_le()?;
            let ordinal = reader.read_u32_le()?;
            let entry_count = reader.read_u32_le()?;
            let first_version = CommitVersion::new(reader.read_u64_le()?);
            let last_version = CommitVersion::new(reader.read_u64_le()?);
            let crc32 = reader.read_u32_le()?;
            if reader.read_u32_le()? != 0 {
                return Err(FormatError::InvalidValue {
                    field: "timeline_segment_ref_reserved",
                });
            }
            refs.push(TimelineSegmentRef {
                sealing_snapshot_id,
                ordinal,
                entry_count,
                first_version,
                last_version,
                crc32,
            });
        }
        groups.push(SnapshotTimelineSegmentGroup {
            branch_id: BranchId::from_bytes(branch_bytes),
            refs,
        });
    }
    reader.finish()?;
    validate_segment_groups(&groups)?;
    Ok(groups)
}

/// A segment holds one chunk: `1..=TIMELINE_CHUNK_ENTRIES` entries under the
/// kind-3 entry rules (strictly ascending, nonzero versions).
fn validate_segment_entries(entries: &[SnapshotTimelineEntry]) -> Result<(), FormatError> {
    if entries.is_empty() || entries.len() > TIMELINE_CHUNK_ENTRIES {
        return Err(segment_count_error());
    }
    validate_group_entries(entries)
}

/// Groups strictly ascending by branch id; every ref well-formed; within a
/// group, versions strictly ascending across refs and every ref but the last
/// a full chunk.
fn validate_segment_groups(groups: &[SnapshotTimelineSegmentGroup]) -> Result<(), FormatError> {
    let ascending = groups
        .windows(2)
        .all(|pair| pair[0].branch_id.as_bytes() < pair[1].branch_id.as_bytes());
    if !ascending {
        return Err(FormatError::InvalidValue {
            field: "timeline_segment_group_order",
        });
    }
    let total_refs: usize = groups.iter().map(|group| group.refs.len()).sum();
    if total_refs > MAX_SEGMENT_REFS {
        return Err(ref_count_error());
    }
    for group in groups {
        validate_group_refs(&group.refs)?;
    }
    Ok(())
}

fn validate_group_refs(refs: &[TimelineSegmentRef]) -> Result<(), FormatError> {
    for (index, segment) in refs.iter().enumerate() {
        let is_last = index + 1 == refs.len();
        if !segment_ref_is_well_formed(segment, is_last) {
            return Err(FormatError::InvalidValue {
                field: "timeline_segment_ref",
            });
        }
    }
    let contiguous = refs
        .windows(2)
        .all(|pair| pair[0].last_version.as_u64() < pair[1].first_version.as_u64());
    if !contiguous {
        return Err(FormatError::InvalidValue {
            field: "timeline_segment_ref_order",
        });
    }
    Ok(())
}

/// One reference in isolation: a nonzero sealing snapshot, a nonzero version
/// range that can hold `entry_count` strictly ascending versions, and a full
/// chunk unless it is the group's last (tail) reference.
pub(crate) fn segment_ref_is_well_formed(segment: &TimelineSegmentRef, is_last: bool) -> bool {
    let count = segment.entry_count as usize;
    let first = segment.first_version.as_u64();
    let last = segment.last_version.as_u64();
    let count_fits = if is_last {
        (1..=TIMELINE_CHUNK_ENTRIES).contains(&count)
    } else {
        count == TIMELINE_CHUNK_ENTRIES
    };
    segment.sealing_snapshot_id != 0
        && first != 0
        && first <= last
        && count_fits
        && last - first >= (count as u64).saturating_sub(1)
}

const fn segment_count_error() -> FormatError {
    FormatError::InvalidLength {
        field: "timeline_segment_entry_count",
    }
}

const fn ref_count_error() -> FormatError {
    FormatError::InvalidLength {
        field: "timeline_segment_ref_count",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(version: u64, instant: u64) -> SnapshotTimelineEntry {
        SnapshotTimelineEntry {
            commit_version: CommitVersion::new(version),
            commit_timestamp: Timestamp::from_micros(version * 10),
            committed_at: (instant != 0).then(|| Timestamp::from_micros(instant)),
        }
    }

    fn entries(first: u64, count: usize) -> Vec<SnapshotTimelineEntry> {
        (0..count as u64)
            .map(|offset| entry(first + offset, offset % 3))
            .collect()
    }

    /// Re-seal a hand-edited segment so a header or body defect is what the
    /// decoder sees, not the footer.
    fn reseal(mut bytes: Vec<u8>) -> Vec<u8> {
        let footer = bytes.len() - SEGMENT_FOOTER_BYTES;
        let crc = crc32fast::hash(&bytes[..footer]);
        bytes[footer..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn segment_ref(ordinal: u32, first: u64, count: u32) -> TimelineSegmentRef {
        TimelineSegmentRef {
            sealing_snapshot_id: 7,
            ordinal,
            entry_count: count,
            first_version: CommitVersion::new(first),
            last_version: CommitVersion::new(first + u64::from(count) - 1),
            crc32: 0xdead_beef,
        }
    }

    fn full() -> u32 {
        u32::try_from(TIMELINE_CHUNK_ENTRIES).expect("chunk fits u32")
    }

    #[test]
    fn segment_round_trips_one_entry_and_a_full_chunk() {
        for count in [1, 2, TIMELINE_CHUNK_ENTRIES] {
            let original = entries(5, count);
            let bytes = encode_timeline_segment(&original).expect("encode");
            assert_eq!(
                bytes.len(),
                SEGMENT_HEADER_BYTES + count * ENTRY_BYTES + SEGMENT_FOOTER_BYTES
            );
            let decoded = decode_timeline_segment(&bytes).expect("decode");
            assert_eq!(decoded.entries, original);
            let footer = bytes.len() - SEGMENT_FOOTER_BYTES;
            assert_eq!(
                decoded.crc32,
                u32::from_le_bytes(bytes[footer..].try_into().expect("footer"))
            );
        }
    }

    #[test]
    fn segment_encode_refuses_what_decode_would_reject() {
        for bad in [
            Vec::new(),
            entries(1, TIMELINE_CHUNK_ENTRIES + 1),
            vec![entry(3, 0), entry(3, 0)],
            vec![entry(4, 0), entry(3, 0)],
            vec![entry(0, 0)],
        ] {
            assert!(
                encode_timeline_segment(&bad).is_err(),
                "{} entries",
                bad.len()
            );
        }
    }

    #[test]
    fn segment_decode_checks_the_footer_first() {
        let mut bytes = encode_timeline_segment(&entries(1, 3)).expect("encode");
        bytes[SEGMENT_HEADER_BYTES] ^= 1;
        assert!(matches!(
            decode_timeline_segment(&bytes),
            Err(FormatError::ChecksumMismatch { .. })
        ));
        assert!(matches!(
            decode_timeline_segment(&bytes[..SEGMENT_HEADER_BYTES]),
            Err(FormatError::InsufficientBytes { .. })
        ));
    }

    #[test]
    fn segment_decode_rejects_each_header_defect() {
        let good = encode_timeline_segment(&entries(10, 3)).expect("encode");
        let edit = |offset: usize, value: &[u8]| {
            let mut bytes = good.clone();
            bytes[offset..offset + value.len()].copy_from_slice(value);
            decode_timeline_segment(&reseal(bytes))
        };
        assert!(matches!(
            edit(0, b"TLSX"),
            Err(FormatError::InvalidMagic { .. })
        ));
        assert!(matches!(
            edit(4, &2u32.to_le_bytes()),
            Err(FormatError::FutureFormat { version: 2, .. })
        ));
        let chunk_plus_one = u32::try_from(TIMELINE_CHUNK_ENTRIES + 1).expect("fits");
        for count in [0u32, chunk_plus_one] {
            assert!(
                matches!(
                    edit(8, &count.to_le_bytes()),
                    Err(FormatError::InvalidLength {
                        field: "timeline_segment_entry_count"
                    })
                ),
                "count {count}"
            );
        }
        for count in [2u32, 4] {
            assert!(
                matches!(
                    edit(8, &count.to_le_bytes()),
                    Err(FormatError::InvalidLength {
                        field: "timeline_segment_entries"
                    })
                ),
                "count {count}"
            );
        }
        // Exactly header and footer, no entries: long enough to read, and
        // refused by the entry count rather than by its length.
        let mut empty = good[..SEGMENT_HEADER_BYTES].to_vec();
        empty[8..12].copy_from_slice(&0u32.to_le_bytes());
        empty.extend_from_slice(&[0; SEGMENT_FOOTER_BYTES]);
        assert!(matches!(
            decode_timeline_segment(&reseal(empty)),
            Err(FormatError::InvalidLength {
                field: "timeline_segment_entry_count"
            })
        ));
        assert!(matches!(
            edit(12, &1u32.to_le_bytes()),
            Err(FormatError::InvalidValue {
                field: "timeline_segment_reserved"
            })
        ));
        for (offset, version) in [(16, 11u64), (24, 11u64)] {
            assert!(matches!(
                edit(offset, &version.to_le_bytes()),
                Err(FormatError::InvalidValue {
                    field: "timeline_segment_version_range"
                })
            ));
        }
        // An out-of-order body behind a consistent header still fails closed.
        let mut disordered = good.clone();
        let second = SEGMENT_HEADER_BYTES + ENTRY_BYTES;
        disordered[second..second + 8].copy_from_slice(&9u64.to_le_bytes());
        assert!(matches!(
            decode_timeline_segment(&reseal(disordered)),
            Err(FormatError::InvalidValue {
                field: "timeline_entry_order"
            })
        ));
    }

    #[test]
    fn segments_section_round_trips_groups() {
        let branch = |byte: u8| BranchId::from_bytes([byte; BranchId::BYTE_LEN]);
        let groups = vec![
            SnapshotTimelineSegmentGroup {
                branch_id: branch(1),
                refs: vec![],
            },
            SnapshotTimelineSegmentGroup {
                branch_id: branch(2),
                refs: vec![
                    segment_ref(0, 1, full()),
                    segment_ref(1, 1 + u64::from(full()), 3),
                ],
            },
        ];
        let section = encode_snapshot_timeline_segments_section(&groups).expect("encode");
        assert_eq!(
            section.section_kind(),
            SNAPSHOT_TIMELINE_SEGMENTS_SECTION_KIND
        );
        assert_eq!(
            section.payload().len(),
            4 + 2 * GROUP_HEADER_BYTES + 2 * REF_BYTES
        );
        assert_eq!(
            decode_snapshot_timeline_segments_payload(section.payload()).expect("decode"),
            groups
        );
    }

    /// A trailing group with no references ends exactly at the payload end.
    #[test]
    fn segments_section_round_trips_a_lone_empty_group() {
        let groups = vec![SnapshotTimelineSegmentGroup {
            branch_id: BranchId::from_bytes([9; BranchId::BYTE_LEN]),
            refs: vec![],
        }];
        let section = encode_snapshot_timeline_segments_section(&groups).expect("encode");
        assert_eq!(
            decode_snapshot_timeline_segments_payload(section.payload()).expect("decode"),
            groups
        );
    }

    /// The encode-side reference ceiling, on both sides: exactly the ceiling
    /// encodes, one more is refused before any payload is built.
    #[test]
    fn segments_section_encode_enforces_the_reference_ceiling() {
        let chunk = u64::from(full());
        let valid_refs = |count: usize| -> Vec<TimelineSegmentRef> {
            (0..count as u64)
                .map(|index| TimelineSegmentRef {
                    sealing_snapshot_id: 1,
                    ordinal: 0,
                    entry_count: full(),
                    first_version: CommitVersion::new(1 + index * chunk),
                    last_version: CommitVersion::new((index + 1) * chunk),
                    crc32: 0,
                })
                .collect()
        };
        let group = |refs| SnapshotTimelineSegmentGroup {
            branch_id: BranchId::from_bytes([1; BranchId::BYTE_LEN]),
            refs,
        };
        assert!(
            encode_snapshot_timeline_segments_section(&[group(valid_refs(MAX_SEGMENT_REFS))])
                .is_ok()
        );
        assert!(matches!(
            encode_snapshot_timeline_segments_section(&[group(valid_refs(MAX_SEGMENT_REFS + 1))]),
            Err(FormatError::InvalidLength {
                field: "timeline_segment_ref_count"
            })
        ));
    }

    #[test]
    fn segments_section_rejects_misordered_or_malformed_groups() {
        let branch = |byte: u8| BranchId::from_bytes([byte; BranchId::BYTE_LEN]);
        let group = |byte: u8, refs: Vec<TimelineSegmentRef>| SnapshotTimelineSegmentGroup {
            branch_id: branch(byte),
            refs,
        };
        for bad in [
            vec![group(2, vec![]), group(1, vec![])],
            vec![group(1, vec![]), group(1, vec![])],
            // A partial chunk before the tail.
            vec![group(1, vec![segment_ref(0, 1, 3), segment_ref(1, 10, 3)])],
            // Overlapping version ranges.
            vec![group(
                1,
                vec![segment_ref(0, 1, full()), segment_ref(1, 5, 3)],
            )],
            // Touching ranges: the tail starts at the full chunk's last version.
            vec![group(
                1,
                vec![
                    segment_ref(0, 1, full()),
                    segment_ref(1, u64::from(full()), 3),
                ],
            )],
        ] {
            assert!(encode_snapshot_timeline_segments_section(&bad).is_err());
        }

        let good =
            encode_snapshot_timeline_segments_section(&[group(1, vec![segment_ref(0, 1, 3)])])
                .expect("encode");
        let mut reserved = good.payload().to_vec();
        let last = reserved.len() - 4;
        reserved[last] = 1;
        assert!(matches!(
            decode_snapshot_timeline_segments_payload(&reserved),
            Err(FormatError::InvalidValue {
                field: "timeline_segment_ref_reserved"
            })
        ));
        let mut trailing = good.payload().to_vec();
        trailing.push(0);
        assert!(decode_snapshot_timeline_segments_payload(&trailing).is_err());
        assert!(decode_snapshot_timeline_segments_payload(&good.payload()[..10]).is_err());
        // One reference announced, half of it present: refused up front by
        // the length check, with exactly the bytes the references need.
        let mut short = 1u32.to_le_bytes().to_vec();
        short.extend_from_slice(branch(1).as_bytes());
        short.extend_from_slice(&1u32.to_le_bytes());
        short.extend_from_slice(&[0; REF_BYTES / 2]);
        assert_eq!(
            decode_snapshot_timeline_segments_payload(&short),
            Err(FormatError::InsufficientBytes {
                format: SEGMENT_SECTION_FORMAT,
                needed: REF_BYTES,
                actual: REF_BYTES / 2,
            })
        );
        let mut huge = 1u32.to_le_bytes().to_vec();
        huge.extend_from_slice(branch(1).as_bytes());
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_snapshot_timeline_segments_payload(&huge),
            Err(FormatError::InvalidLength {
                field: "timeline_segment_ref_count"
            })
        ));
    }

    /// The per-reference rule in isolation: every field that can make a ref
    /// unsatisfiable, on both sides of each boundary.
    #[test]
    fn segment_ref_is_well_formed_truth_table() {
        let full = full();
        let with = |edit: fn(&mut TimelineSegmentRef)| {
            let mut segment = segment_ref(0, 10, 3);
            edit(&mut segment);
            segment
        };
        let cases: [(TimelineSegmentRef, bool, bool); 12] = [
            (segment_ref(0, 10, 3), true, true),
            (segment_ref(0, 10, 3), false, false),
            (segment_ref(0, 10, full), false, true),
            (segment_ref(0, 10, full), true, true),
            (segment_ref(0, 10, 1), true, true),
            (with(|s| s.entry_count = 0), true, false),
            (with(|s| s.entry_count = u32::MAX), true, false),
            (with(|s| s.sealing_snapshot_id = 0), true, false),
            (with(|s| s.first_version = CommitVersion::ZERO), true, false),
            (
                with(|s| s.last_version = CommitVersion::new(9)),
                true,
                false,
            ),
            // Three versions need a span of at least two.
            (
                with(|s| s.last_version = CommitVersion::new(11)),
                true,
                false,
            ),
            (
                with(|s| s.last_version = CommitVersion::new(12)),
                true,
                true,
            ),
        ];
        for (segment, is_last, expected) in cases {
            assert_eq!(
                segment_ref_is_well_formed(&segment, is_last),
                expected,
                "{segment:?} last={is_last}"
            );
        }
    }
}
