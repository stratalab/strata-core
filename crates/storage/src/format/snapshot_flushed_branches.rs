//! The checkpoint snapshot's durable-base branch set (space-reclamation
//! contract §3.2, slice 11 of #3596).
//!
//! One section per checkpoint, recording every branch that held a DURABLE
//! table-manifest base — a durably catalogued owned table — when the delta was
//! collected. Those are exactly the branches whose flushed rows the snapshot
//! deltas OVER instead of containing, so recovery can tell a branch whose
//! table manifest is missing (an orphaned delta) from a branch the snapshot
//! carries in full. Layout (little-endian):
//!
//! ```text
//! branch_count: u32
//! repeated branch_id: 16 bytes, strictly ascending by byte order (unique)
//! ```
//!
//! Membership is narrower than "owns a table": a VOLATILE owned table (a
//! snapshot-install L0 with no durable catalog entry) has its rows captured
//! into the delta, so its branch is deliberately absent.
//!
//! Presence is the signal. A snapshot written before this section existed has
//! no set at all, and recovery keeps treating base membership as unknown; a
//! present section is authoritative, and an EMPTY set means every branch's
//! rows are fully contained in the snapshot. Ordering is validated at encode
//! and decode so a corrupt section fails closed rather than seeding a wrong
//! membership.

use super::{FormatError, SnapshotSection};
use strata_core::BranchId;

/// Section kind 4. Kinds 1–3 are the row and retained-timeline sections; kind
/// 0 is reserved.
pub(crate) const SNAPSHOT_FLUSHED_BRANCHES_SECTION_KIND: u8 = 4;

/// Fail-closed ceiling mirroring the timeline section's entry guard: a
/// section that decode would reject is never written.
const MAX_FLUSHED_BRANCHES: usize = 1 << 20;

pub(crate) fn encode_snapshot_flushed_branches_section(
    branches: &[BranchId],
) -> Result<SnapshotSection, FormatError> {
    validate_branch_order(branches)?;
    if branches.len() > MAX_FLUSHED_BRANCHES {
        return Err(FormatError::InvalidLength {
            field: "flushed_branch_count",
        });
    }
    let branch_count = u32::try_from(branches.len()).map_err(|_| FormatError::InvalidLength {
        field: "flushed_branch_count",
    })?;
    let mut payload = branch_count.to_le_bytes().to_vec();
    for branch in branches {
        payload.extend_from_slice(branch.as_bytes());
    }
    SnapshotSection::new(SNAPSHOT_FLUSHED_BRANCHES_SECTION_KIND, payload)
}

pub(crate) fn decode_snapshot_flushed_branches_payload(
    payload: &[u8],
) -> Result<Vec<BranchId>, FormatError> {
    if payload.len() < 4 {
        return Err(FormatError::InsufficientBytes {
            format: "snapshot_flushed_branches_section",
            needed: 4,
            actual: payload.len(),
        });
    }
    let mut count_bytes = [0u8; 4];
    count_bytes.copy_from_slice(&payload[..4]);
    let branch_count = u32::from_le_bytes(count_bytes) as usize;
    if branch_count > MAX_FLUSHED_BRANCHES {
        return Err(FormatError::InvalidLength {
            field: "flushed_branch_count",
        });
    }
    let needed = 4usize
        .checked_add(branch_count.saturating_mul(BranchId::BYTE_LEN))
        .ok_or(FormatError::InvalidLength {
            field: "flushed_branch_count",
        })?;
    if payload.len() < needed {
        return Err(FormatError::InsufficientBytes {
            format: "snapshot_flushed_branches_section",
            needed,
            actual: payload.len(),
        });
    }
    if payload.len() != needed {
        return Err(FormatError::InvalidLength {
            field: "flushed_branches_section_trailing_bytes",
        });
    }
    let branches: Vec<BranchId> = payload[4..needed]
        .chunks_exact(BranchId::BYTE_LEN)
        .map(|chunk| {
            let mut bytes = [0u8; BranchId::BYTE_LEN];
            bytes.copy_from_slice(chunk);
            BranchId::from_bytes(bytes)
        })
        .collect();
    validate_branch_order(&branches)?;
    Ok(branches)
}

/// Strictly ascending by byte order, which also rules out duplicates.
fn validate_branch_order(branches: &[BranchId]) -> Result<(), FormatError> {
    let ascending = branches
        .windows(2)
        .all(|pair| pair[0].as_bytes() < pair[1].as_bytes());
    if !ascending {
        return Err(FormatError::InvalidValue {
            field: "flushed_branch_order",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn branch(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; BranchId::BYTE_LEN])
    }

    #[test]
    fn flushed_branches_section_round_trips_empty_and_multi_branch_sets() {
        for branches in [
            vec![],
            vec![branch(1)],
            vec![branch(1), branch(2), branch(0xf0)],
        ] {
            let section = encode_snapshot_flushed_branches_section(&branches).expect("encode");
            assert_eq!(
                section.section_kind(),
                SNAPSHOT_FLUSHED_BRANCHES_SECTION_KIND
            );
            assert_eq!(
                section.payload().len(),
                4 + branches.len() * BranchId::BYTE_LEN
            );
            assert_eq!(
                decode_snapshot_flushed_branches_payload(section.payload()).expect("decode"),
                branches
            );
        }
    }

    #[test]
    fn flushed_branches_section_rejects_disorder_and_duplicates_both_ways() {
        assert_eq!(
            encode_snapshot_flushed_branches_section(&[branch(2), branch(1)]),
            Err(FormatError::InvalidValue {
                field: "flushed_branch_order"
            })
        );
        assert_eq!(
            encode_snapshot_flushed_branches_section(&[branch(1), branch(1)]),
            Err(FormatError::InvalidValue {
                field: "flushed_branch_order"
            })
        );

        let mut payload = 2u32.to_le_bytes().to_vec();
        payload.extend_from_slice(branch(2).as_bytes());
        payload.extend_from_slice(branch(1).as_bytes());
        assert_eq!(
            decode_snapshot_flushed_branches_payload(&payload),
            Err(FormatError::InvalidValue {
                field: "flushed_branch_order"
            })
        );
        let mut payload = 2u32.to_le_bytes().to_vec();
        payload.extend_from_slice(branch(3).as_bytes());
        payload.extend_from_slice(branch(3).as_bytes());
        assert_eq!(
            decode_snapshot_flushed_branches_payload(&payload),
            Err(FormatError::InvalidValue {
                field: "flushed_branch_order"
            })
        );
    }

    #[test]
    fn flushed_branches_section_rejects_truncation_trailing_bytes_and_impossible_counts() {
        let section =
            encode_snapshot_flushed_branches_section(&[branch(1), branch(2)]).expect("encode");
        let payload = section.payload().to_vec();

        assert!(matches!(
            decode_snapshot_flushed_branches_payload(&payload[..3]),
            Err(FormatError::InsufficientBytes { .. })
        ));
        assert!(matches!(
            decode_snapshot_flushed_branches_payload(&payload[..payload.len() - 1]),
            Err(FormatError::InsufficientBytes { .. })
        ));

        let mut trailing = payload.clone();
        trailing.push(0);
        assert_eq!(
            decode_snapshot_flushed_branches_payload(&trailing),
            Err(FormatError::InvalidLength {
                field: "flushed_branches_section_trailing_bytes"
            })
        );

        // A count above the ceiling is refused before any length arithmetic.
        let oversized = (u32::try_from(MAX_FLUSHED_BRANCHES).expect("fits") + 1).to_le_bytes();
        assert_eq!(
            decode_snapshot_flushed_branches_payload(&oversized),
            Err(FormatError::InvalidLength {
                field: "flushed_branch_count"
            })
        );
        // Exactly the ceiling passes the count guard and fails on bytes.
        let at_ceiling = u32::try_from(MAX_FLUSHED_BRANCHES)
            .expect("fits")
            .to_le_bytes();
        assert!(matches!(
            decode_snapshot_flushed_branches_payload(&at_ceiling),
            Err(FormatError::InsufficientBytes { .. })
        ));
    }

    /// The encode-side ceiling is exact: a set of exactly the ceiling encodes,
    /// one more is refused before any bytes are written.
    #[test]
    fn flushed_branches_section_encode_ceiling_is_exact() {
        let ascending = |count: usize| -> Vec<BranchId> {
            (0..count)
                .map(|index| {
                    let mut bytes = [0u8; BranchId::BYTE_LEN];
                    bytes[..8].copy_from_slice(&u64::try_from(index).expect("fits").to_be_bytes());
                    BranchId::from_bytes(bytes)
                })
                .collect()
        };
        assert!(encode_snapshot_flushed_branches_section(&ascending(MAX_FLUSHED_BRANCHES)).is_ok());
        assert_eq!(
            encode_snapshot_flushed_branches_section(&ascending(MAX_FLUSHED_BRANCHES + 1)),
            Err(FormatError::InvalidLength {
                field: "flushed_branch_count"
            })
        );
    }
}
