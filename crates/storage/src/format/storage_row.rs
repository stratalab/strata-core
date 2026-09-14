use super::{
    key::{
        append_physical_key, decode_physical_key, physical_key_encode_capacity,
        physical_key_encoded_len,
    },
    ByteReader, FormatError, STORAGE_ROW_FLAGS_NONE, STORAGE_ROW_FORMAT_VERSION,
};
use crate::row::{PhysicalKey, StorageRow};
use strata_core::{CommitVersion, Timestamp};

const STORAGE_ROW_FORMAT: &str = "storage_row";

pub(crate) fn encode_storage_row(row: &StorageRow) -> Result<Vec<u8>, FormatError> {
    let mut bytes = Vec::with_capacity(storage_row_encode_capacity(row));
    encode_storage_row_into(row, &mut bytes)?;
    Ok(bytes)
}

pub(crate) fn encode_storage_row_into(
    row: &StorageRow,
    bytes: &mut Vec<u8>,
) -> Result<(), FormatError> {
    bytes.clear();
    bytes.reserve(storage_row_encode_capacity(row));
    let value = row.value();
    let value_len =
        u32::try_from(value.len()).map_err(|_| FormatError::InvalidLength { field: "value" })?;

    bytes.push(STORAGE_ROW_FORMAT_VERSION);
    let key_len_offset = bytes.len();
    bytes.extend_from_slice(&[0; 4]);
    let key_start = bytes.len();
    append_physical_key(row.physical_key(), bytes);
    let physical_key_len =
        u32::try_from(bytes.len() - key_start).map_err(|_| FormatError::InvalidLength {
            field: "physical_key",
        })?;
    bytes[key_len_offset..key_len_offset + 4].copy_from_slice(&physical_key_len.to_le_bytes());
    // The row carries commit facts redundantly with the internal key. Recovery
    // and diagnostics can validate row bytes without re-parsing the key suffix.
    bytes.extend_from_slice(&row.commit_version().as_u64().to_le_bytes());
    bytes.extend_from_slice(&row.commit_timestamp().as_micros().to_le_bytes());
    bytes.extend_from_slice(&row.expires_at().as_micros().to_le_bytes());
    bytes.extend_from_slice(&STORAGE_ROW_FLAGS_NONE.to_le_bytes());
    bytes.push(u8::from(row.is_tombstone()));
    bytes.extend_from_slice(&value_len.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

pub(crate) fn encode_storage_row_with_physical_key_bytes_into(
    row: &StorageRow,
    physical_key_bytes: &[u8],
    bytes: &mut Vec<u8>,
) -> Result<(), FormatError> {
    bytes.clear();
    bytes.reserve(storage_row_len_from_parts(
        physical_key_bytes.len(),
        row.value().len(),
    ));
    let value = row.value();
    let physical_key_len =
        u32::try_from(physical_key_bytes.len()).map_err(|_| FormatError::InvalidLength {
            field: "physical_key",
        })?;
    let value_len =
        u32::try_from(value.len()).map_err(|_| FormatError::InvalidLength { field: "value" })?;

    bytes.push(STORAGE_ROW_FORMAT_VERSION);
    bytes.extend_from_slice(&physical_key_len.to_le_bytes());
    bytes.extend_from_slice(physical_key_bytes);
    bytes.extend_from_slice(&row.commit_version().as_u64().to_le_bytes());
    bytes.extend_from_slice(&row.commit_timestamp().as_micros().to_le_bytes());
    bytes.extend_from_slice(&row.expires_at().as_micros().to_le_bytes());
    bytes.extend_from_slice(&STORAGE_ROW_FLAGS_NONE.to_le_bytes());
    bytes.push(u8::from(row.is_tombstone()));
    bytes.extend_from_slice(&value_len.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

/// Exactly how many bytes `encode_storage_row_into` will write for a row with
/// this key and a value of `value_len` bytes.
///
/// Every field the encoder writes apart from the physical key and the value is
/// fixed-width, so those two determine the length. Commit admission uses this
/// to refuse a mutation no WAL commit payload row could hold — in cache mode
/// as well as durable, so both modes agree on what a legal write is (#3391).
/// `storage_row_encoded_len_matches_the_encoder` pins it against the encoder.
pub(crate) fn storage_row_encoded_len(key: &PhysicalKey, value_len: usize) -> usize {
    storage_row_len_from_parts(physical_key_encoded_len(key), value_len)
}

fn storage_row_encode_capacity(row: &StorageRow) -> usize {
    storage_row_len_from_parts(
        physical_key_encode_capacity(row.physical_key()),
        row.value().len(),
    )
}

/// Exact given an exact `physical_key_len`; a hint given the capacity hint.
fn storage_row_len_from_parts(physical_key_len: usize, value_len: usize) -> usize {
    1usize
        .saturating_add(4)
        .saturating_add(physical_key_len)
        .saturating_add(8)
        .saturating_add(8)
        .saturating_add(8)
        .saturating_add(4)
        .saturating_add(1)
        .saturating_add(4)
        .saturating_add(value_len)
}

pub(crate) fn decode_storage_row(bytes: &[u8]) -> Result<StorageRow, FormatError> {
    decode_storage_row_inner(bytes, None)
}

/// C3b: decode a row while cross-checking it against its block entry's
/// ENCODED internal key — the embedded physical-key region must equal the
/// key's physical prefix byte-for-byte (the encoding is canonical, so byte
/// equality is exactly semantic equality and strictly stronger than the
/// decoded-struct comparison it replaces) and the commit versions must
/// agree. Lets the trusted indexed probe skip the full internal-key decode
/// for entries it only compares.
pub(crate) fn decode_storage_row_matching_key(
    bytes: &[u8],
    expected_physical_key: &[u8],
    expected_commit_version: CommitVersion,
) -> Result<StorageRow, FormatError> {
    decode_storage_row_inner(
        bytes,
        Some((expected_physical_key, expected_commit_version)),
    )
}

fn decode_storage_row_inner(
    bytes: &[u8],
    expected: Option<(&[u8], CommitVersion)>,
) -> Result<StorageRow, FormatError> {
    let mut reader = ByteReader::new(STORAGE_ROW_FORMAT, bytes);
    let version = reader.read_u8()?;
    if version != STORAGE_ROW_FORMAT_VERSION {
        return Err(FormatError::InvalidVersion {
            format: STORAGE_ROW_FORMAT,
            version,
        });
    }

    let physical_key_len =
        usize::try_from(reader.read_u32_le()?).map_err(|_| FormatError::InvalidLength {
            field: "physical_key",
        })?;
    let physical_key_bytes = reader.read_exact(physical_key_len)?;
    if let Some((expected_key, _)) = expected {
        if physical_key_bytes != expected_key {
            return Err(FormatError::InvalidValue {
                field: "physical_key",
            });
        }
    }
    let physical_key = decode_physical_key(physical_key_bytes)?;
    let commit_version = CommitVersion::new(reader.read_u64_le()?);
    if let Some((_, expected_version)) = expected {
        if commit_version != expected_version {
            return Err(FormatError::InvalidValue {
                field: "commit_version",
            });
        }
    }
    let commit_timestamp = Timestamp::from_micros(reader.read_u64_le()?);
    let expires_at = Timestamp::from_micros(reader.read_u64_le()?);
    let row_flags = reader.read_u32_le()?;
    if row_flags != STORAGE_ROW_FLAGS_NONE {
        // Reserved flags fail closed until a later format version assigns
        // semantics to them.
        return Err(FormatError::UnsupportedFlags {
            format: STORAGE_ROW_FORMAT,
            flags: row_flags,
        });
    }

    let tombstone = match reader.read_u8()? {
        0 => false,
        1 => true,
        value => {
            return Err(FormatError::InvalidBool {
                field: "is_tombstone",
                value,
            });
        }
    };
    let value_len = usize::try_from(reader.read_u32_le()?)
        .map_err(|_| FormatError::InvalidLength { field: "value" })?;
    if tombstone {
        // Tombstones carry only deletion intent and commit facts. Expiry and
        // value bytes would make delete semantics ambiguous.
        if expires_at != Timestamp::EPOCH {
            return Err(FormatError::InvalidTombstonePayload { field: "expiry" });
        }
        if value_len != 0 {
            return Err(FormatError::InvalidTombstonePayload { field: "value" });
        }
        reader.finish()?;
        Ok(StorageRow::tombstone(
            physical_key,
            commit_version,
            commit_timestamp,
        ))
    } else {
        let value = reader.read_exact(value_len)?.to_vec();
        reader.finish()?;
        Ok(StorageRow::put(
            physical_key,
            commit_version,
            commit_timestamp,
            expires_at,
            value,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_storage_row, decode_storage_row_matching_key, encode_storage_row,
        storage_row_encoded_len, FormatError, STORAGE_ROW_FORMAT,
    };
    use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
    use strata_core::{BranchId, CommitVersion, Timestamp};

    fn physical_key() -> PhysicalKey {
        PhysicalKey::new(
            BranchId::from_bytes([7; BranchId::BYTE_LEN]),
            "default",
            StorageSpaceId::engine(0x20).expect("engine id"),
            b"alpha".to_vec(),
        )
        .expect("physical key")
    }

    /// C3b: the matching-key decode accepts the row's own encoded key and
    /// version, and fails closed on any drift between the block entry's key
    /// and the row's embedded facts (the byte-compare form of the
    /// decoded-struct equality it replaces).
    #[test]
    fn storage_row_matching_key_accepts_and_rejects_drift() {
        let row = StorageRow::put(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
            Timestamp::from_micros(99),
            b"value".to_vec(),
        );
        let bytes = encode_storage_row(&row).expect("encode row");
        let key_bytes = crate::format::encode_physical_key(row.physical_key());

        assert_eq!(
            decode_storage_row_matching_key(&bytes, &key_bytes, CommitVersion::new(42)),
            Ok(row)
        );
        let mut drifted_key = key_bytes.clone();
        *drifted_key.last_mut().expect("non-empty key") ^= 0xFF;
        assert_eq!(
            decode_storage_row_matching_key(&bytes, &drifted_key, CommitVersion::new(42)),
            Err(FormatError::InvalidValue {
                field: "physical_key",
            })
        );
        assert_eq!(
            decode_storage_row_matching_key(&bytes, &key_bytes, CommitVersion::new(43)),
            Err(FormatError::InvalidValue {
                field: "commit_version",
            })
        );
    }

    #[test]
    fn storage_row_put_round_trips() {
        let row = StorageRow::put(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
            Timestamp::from_micros(99),
            b"value".to_vec(),
        );

        assert_eq!(
            decode_storage_row(&encode_storage_row(&row).expect("encode row")),
            Ok(row)
        );
    }

    #[test]
    fn storage_row_tombstone_round_trips_without_value_or_expiry() {
        let row = StorageRow::tombstone(
            physical_key(),
            CommitVersion::new(43),
            Timestamp::from_micros(12),
        );

        assert_eq!(
            decode_storage_row(&encode_storage_row(&row).expect("encode row")),
            Ok(row)
        );
    }

    #[test]
    fn decode_rejects_unknown_row_format_version() {
        let row = StorageRow::put(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
            Timestamp::EPOCH,
            b"value".to_vec(),
        );
        let mut bytes = encode_storage_row(&row).expect("encode row");
        bytes[0] = 2;

        assert_eq!(
            decode_storage_row(&bytes),
            Err(FormatError::InvalidVersion {
                format: STORAGE_ROW_FORMAT,
                version: 2
            })
        );
    }

    #[test]
    fn decode_rejects_nonzero_reserved_flags() {
        let row = StorageRow::put(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
            Timestamp::EPOCH,
            b"value".to_vec(),
        );
        let mut bytes = encode_storage_row(&row).expect("encode row");
        let key_len = u32::from_le_bytes(bytes[1..5].try_into().expect("key len")) as usize;
        let flags_offset = 1 + 4 + key_len + 8 + 8 + 8;
        bytes[flags_offset] = 1;

        assert_eq!(
            decode_storage_row(&bytes),
            Err(FormatError::UnsupportedFlags {
                format: STORAGE_ROW_FORMAT,
                flags: 1
            })
        );
    }

    #[test]
    fn decode_rejects_invalid_tombstone_byte() {
        let row = StorageRow::put(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
            Timestamp::EPOCH,
            b"value".to_vec(),
        );
        let mut bytes = encode_storage_row(&row).expect("encode row");
        let key_len = u32::from_le_bytes(bytes[1..5].try_into().expect("key len")) as usize;
        let tombstone_offset = 1 + 4 + key_len + 8 + 8 + 8 + 4;
        bytes[tombstone_offset] = 7;

        assert_eq!(
            decode_storage_row(&bytes),
            Err(FormatError::InvalidBool {
                field: "is_tombstone",
                value: 7
            })
        );
    }

    #[test]
    fn decode_rejects_tombstone_payload_data() {
        let row = StorageRow::put(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
            Timestamp::EPOCH,
            b"value".to_vec(),
        );
        let mut bytes = encode_storage_row(&row).expect("encode row");
        let key_len = u32::from_le_bytes(bytes[1..5].try_into().expect("key len")) as usize;
        let tombstone_offset = 1 + 4 + key_len + 8 + 8 + 8 + 4;
        bytes[tombstone_offset] = 1;

        assert_eq!(
            decode_storage_row(&bytes),
            Err(FormatError::InvalidTombstonePayload { field: "value" })
        );
    }

    #[test]
    fn decode_rejects_tombstone_value_len_before_value_allocation() {
        let row = StorageRow::tombstone(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
        );
        let mut bytes = encode_storage_row(&row).expect("encode row");
        let key_len = u32::from_le_bytes(bytes[1..5].try_into().expect("key len")) as usize;
        let value_len_offset = 1 + 4 + key_len + 8 + 8 + 8 + 4 + 1;
        bytes[value_len_offset..value_len_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        assert_eq!(
            decode_storage_row(&bytes),
            Err(FormatError::InvalidTombstonePayload { field: "value" })
        );
    }

    #[test]
    fn decode_rejects_tombstone_expiry() {
        let row = StorageRow::tombstone(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
        );
        let mut bytes = encode_storage_row(&row).expect("encode row");
        let key_len = u32::from_le_bytes(bytes[1..5].try_into().expect("key len")) as usize;
        let expiry_offset = 1 + 4 + key_len + 8 + 8;
        bytes[expiry_offset] = 1;

        assert_eq!(
            decode_storage_row(&bytes),
            Err(FormatError::InvalidTombstonePayload { field: "expiry" })
        );
    }

    /// Commit admission decides whether a mutation fits a WAL commit payload
    /// row from this length alone, without encoding it. If the two ever
    /// disagree, admission either lets through a row the WAL cannot write or
    /// refuses one it can (#3391).
    #[test]
    fn storage_row_encoded_len_matches_the_encoder() {
        let cases: [(Vec<u8>, Vec<u8>); 5] = [
            (Vec::new(), Vec::new()),
            (b"alpha".to_vec(), b"value".to_vec()),
            (vec![0x00; 16], b"value".to_vec()),
            (vec![0x00, 0x41, 0x00], vec![0x00; 32]),
            ((0u8..=255).collect(), vec![7; 4096]),
        ];

        for (user_key, value) in cases {
            let key = PhysicalKey::new(
                BranchId::from_bytes([7; BranchId::BYTE_LEN]),
                "default",
                StorageSpaceId::engine(0x20).expect("engine id"),
                user_key.clone(),
            )
            .expect("physical key");
            let row = StorageRow::put(
                key.clone(),
                CommitVersion::new(42),
                Timestamp::from_micros(11),
                Timestamp::from_micros(99),
                value.clone(),
            );

            assert_eq!(
                storage_row_encoded_len(&key, value.len()),
                encode_storage_row(&row).expect("encode row").len(),
                "computed length disagrees with the encoder for key {user_key:?}"
            );
        }
    }

    /// A delete stamps a valueless tombstone, and admission sizes it the same
    /// way — so the key alone must account for the whole row.
    #[test]
    fn storage_row_encoded_len_matches_the_encoder_for_a_tombstone() {
        let row = StorageRow::tombstone(
            physical_key(),
            CommitVersion::new(42),
            Timestamp::from_micros(11),
        );

        assert_eq!(
            storage_row_encoded_len(row.physical_key(), 0),
            encode_storage_row(&row).expect("encode row").len()
        );
    }
}
