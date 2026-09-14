use super::{ByteReader, FormatError};
use crate::row::{InternalKey, PhysicalKey, RowError, StorageSpaceId};
use strata_core::{BranchId, CommitVersion};

const INTERNAL_KEY_SUFFIX_LEN: usize = 8;
/// The `00 00` `encode_escaped` appends to close the user key.
const ESCAPED_TERMINATOR_LEN: usize = 2;
const PHYSICAL_KEY_FORMAT: &str = "physical_key";
const INTERNAL_KEY_FORMAT: &str = "internal_key";

pub(crate) fn encode_physical_key(key: &PhysicalKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(physical_key_encode_capacity(key));
    append_physical_key(key, &mut bytes);
    bytes
}

pub(crate) fn append_physical_key(key: &PhysicalKey, bytes: &mut Vec<u8>) {
    // Layout: branch id, NUL-terminated space, one-byte storage-space id, then
    // an escaped user key. The escaped user key preserves arbitrary bytes while
    // still giving the physical key a single unambiguous terminator.
    bytes.extend_from_slice(key.branch_id().as_bytes());
    bytes.extend_from_slice(key.space().as_bytes());
    bytes.push(0x00);
    bytes.push(key.storage_space_id().raw());
    encode_escaped(key.user_key(), bytes);
}

pub(crate) fn decode_physical_key(bytes: &[u8]) -> Result<PhysicalKey, FormatError> {
    let (key, consumed) = decode_physical_key_prefix(bytes)?;
    if consumed == bytes.len() {
        Ok(key)
    } else {
        Err(FormatError::TrailingData {
            format: PHYSICAL_KEY_FORMAT,
            remaining: bytes.len() - consumed,
        })
    }
}

pub(crate) fn encode_internal_key(key: &InternalKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        physical_key_encode_capacity(key.physical_key()) + INTERNAL_KEY_SUFFIX_LEN,
    );
    append_internal_key_from_physical(key.physical_key(), key.commit_version(), &mut bytes);
    bytes
}

/// C3b: append the internal-key encoding of `(key, version)` without
/// materializing an `InternalKey` (whose construction clones the physical
/// key's space String and user-key Vec). Byte-identical to
/// `encode_internal_key` — the goldens pin it.
pub(crate) fn append_internal_key_from_physical(
    key: &PhysicalKey,
    version: CommitVersion,
    bytes: &mut Vec<u8>,
) {
    // Reserve the full encoding upfront: the escape encode pushes
    // byte-by-byte, and growing from empty reallocs several times per key
    // (measured as the allocator's top hot-path frames).
    bytes.reserve(physical_key_encode_capacity(key) + INTERNAL_KEY_SUFFIX_LEN);
    append_physical_key(key, bytes);
    // Store the bitwise inverse as big-endian so ordinary ascending byte order
    // returns newest commit versions first for the same physical key.
    bytes.extend_from_slice(&(!version.as_u64()).to_be_bytes());
}

/// C3b: parse ONLY the commit version from an encoded internal key — the
/// inverted big-endian suffix — without materializing the physical key. The
/// trusted indexed block probe reads versions per visited entry; the full
/// decode allocated a space String and user-key Vec it never used.
pub(crate) fn internal_key_commit_version(bytes: &[u8]) -> Result<CommitVersion, FormatError> {
    let split =
        bytes
            .len()
            .checked_sub(INTERNAL_KEY_SUFFIX_LEN)
            .ok_or(FormatError::InsufficientBytes {
                format: INTERNAL_KEY_FORMAT,
                needed: INTERNAL_KEY_SUFFIX_LEN,
                actual: bytes.len(),
            })?;
    let suffix = <[u8; INTERNAL_KEY_SUFFIX_LEN]>::try_from(&bytes[split..]).map_err(|_| {
        FormatError::InvalidLength {
            field: INTERNAL_KEY_FORMAT,
        }
    })?;
    Ok(CommitVersion::new(!u64::from_be_bytes(suffix)))
}

pub(crate) fn decode_internal_key(bytes: &[u8]) -> Result<InternalKey, FormatError> {
    if bytes.len() < BranchId::BYTE_LEN + 1 + 1 + 2 + INTERNAL_KEY_SUFFIX_LEN {
        return Err(FormatError::InsufficientBytes {
            format: INTERNAL_KEY_FORMAT,
            needed: BranchId::BYTE_LEN + 1 + 1 + 2 + INTERNAL_KEY_SUFFIX_LEN,
            actual: bytes.len(),
        });
    }

    let split =
        bytes
            .len()
            .checked_sub(INTERNAL_KEY_SUFFIX_LEN)
            .ok_or(FormatError::InvalidLength {
                field: INTERNAL_KEY_FORMAT,
            })?;
    let physical_key = decode_physical_key(&bytes[..split])?;
    let suffix = <[u8; INTERNAL_KEY_SUFFIX_LEN]>::try_from(&bytes[split..]).map_err(|_| {
        FormatError::InvalidLength {
            field: INTERNAL_KEY_FORMAT,
        }
    })?;
    let commit_version = CommitVersion::new(!u64::from_be_bytes(suffix));

    Ok(InternalKey::new(physical_key, commit_version))
}

fn decode_physical_key_prefix(bytes: &[u8]) -> Result<(PhysicalKey, usize), FormatError> {
    let mut reader = ByteReader::new(PHYSICAL_KEY_FORMAT, bytes);
    let branch_bytes = reader.read_exact(BranchId::BYTE_LEN)?;
    let branch_id = BranchId::try_from_slice(branch_bytes)
        .map_err(|_| FormatError::InvalidLength { field: "branch_id" })?;

    let space_start = BranchId::BYTE_LEN;
    let space_len = bytes[space_start..]
        .iter()
        .position(|byte| *byte == 0x00)
        .ok_or(FormatError::MissingTerminator { field: "space" })?;
    let space_bytes = reader.read_exact(space_len)?;
    reader.read_exact(1)?;
    let space = std::str::from_utf8(space_bytes)
        .map_err(|_| FormatError::InvalidUtf8 { field: "space" })?;
    // C3b: intern the well-known space names — every hot-path row decode
    // otherwise allocates a fresh short String for a name drawn from a
    // small fixed set. Unknown names still allocate (correct for user
    // spaces).
    let space: std::borrow::Cow<'static, str> = match space {
        "api" => std::borrow::Cow::Borrowed("api"),
        "default" => std::borrow::Cow::Borrowed("default"),
        "timeline" => std::borrow::Cow::Borrowed("timeline"),
        other => std::borrow::Cow::Owned(other.to_owned()),
    };

    let storage_space_id =
        StorageSpaceId::from_raw(reader.read_u8()?).map_err(format_error_from_row_error)?;
    let (user_key, consumed) = decode_escaped(&bytes[reader.cursor..], "user_key")?;
    reader.read_exact(consumed)?;

    PhysicalKey::new(branch_id, space, storage_space_id, user_key)
        .map(|key| (key, reader.cursor))
        .map_err(format_error_from_row_error)
}

fn encode_escaped(source: &[u8], target: &mut Vec<u8>) {
    // 00 00 terminates the user key; 00 01 represents a literal zero byte.
    for byte in source {
        if *byte == 0x00 {
            target.push(0x00);
            target.push(0x01);
        } else {
            target.push(*byte);
        }
    }
    target.push(0x00);
    target.push(0x00);
}

/// Exactly how many bytes `append_physical_key` will write for `key`.
///
/// Commit admission refuses a mutation whose encoded row cannot fit one WAL
/// commit payload row, so it needs the true length rather than the reserve
/// hint below: `encode_escaped` emits two bytes for every `0x00`, so a user
/// key of zero bytes encodes to nearly twice its length.
/// `physical_key_encoded_len_matches_the_encoder` pins this against the
/// encoder, so the widths below cannot drift away from what is written.
pub(crate) fn physical_key_encoded_len(key: &PhysicalKey) -> usize {
    physical_key_framing_len(key).saturating_add(escaped_encoded_len(key.user_key()))
}

/// Exactly how many bytes `append_internal_key_from_physical` will write for
/// `key` at any commit version — the physical key plus the fixed-width
/// inverted-version suffix.
///
/// This, not the user key's own length, is what the table format caps at
/// `MAX_TABLE_KEY_BYTES`. Commit admission bounds it so an oversized key is
/// refused at write time instead of being acknowledged and then failing the
/// branch's next rotation (#3396).
/// `internal_key_encoded_len_matches_the_encoder` pins it against the encoder.
pub(crate) fn internal_key_encoded_len(key: &PhysicalKey) -> usize {
    physical_key_encoded_len(key).saturating_add(INTERNAL_KEY_SUFFIX_LEN)
}

/// Buffer hint only: assumes no byte of the user key escapes, which is the
/// common case and keeps the reserve O(1) in the key length. Undercounting a
/// key that contains `0x00` costs a reallocation, never a wrong encoding —
/// use `physical_key_encoded_len` wherever the answer must be exact.
pub(crate) fn physical_key_encode_capacity(key: &PhysicalKey) -> usize {
    physical_key_framing_len(key)
        .saturating_add(key.user_key().len())
        .saturating_add(ESCAPED_TERMINATOR_LEN)
}

/// Branch id, NUL-terminated space, storage-space id — everything
/// `append_physical_key` writes ahead of the escaped user key.
fn physical_key_framing_len(key: &PhysicalKey) -> usize {
    BranchId::BYTE_LEN
        .saturating_add(key.space().len())
        .saturating_add(1)
        .saturating_add(1)
}

#[expect(
    clippy::naive_bytecount,
    reason = "one count per mutation over a user key, not a bulk scan — a SIMD dependency in the storage crate is not worth it here"
)]
fn escaped_encoded_len(source: &[u8]) -> usize {
    source
        .len()
        .saturating_add(source.iter().filter(|byte| **byte == 0x00).count())
        .saturating_add(ESCAPED_TERMINATOR_LEN)
}

fn decode_escaped(bytes: &[u8], field: &'static str) -> Result<(Vec<u8>, usize), FormatError> {
    let mut decoded = Vec::new();
    let mut cursor = 0;

    while cursor < bytes.len() {
        let byte = bytes[cursor];
        if byte != 0x00 {
            decoded.push(byte);
            cursor += 1;
            continue;
        }

        let next = bytes
            .get(cursor + 1)
            .ok_or(FormatError::MissingTerminator { field })?;
        match next {
            0x00 => return Ok((decoded, cursor + 2)),
            0x01 => {
                decoded.push(0x00);
                cursor += 2;
            }
            _ => return Err(FormatError::InvalidEscape { field }),
        }
    }

    Err(FormatError::MissingTerminator { field })
}

fn format_error_from_row_error(error: RowError) -> FormatError {
    match error {
        RowError::InvalidStorageSpaceId { raw } => FormatError::InvalidStorageSpaceId { raw },
        RowError::StorageReservedSpaceId { raw } => FormatError::StorageReservedSpaceId { raw },
        RowError::EmptySpace => FormatError::InvalidSpace { reason: "is empty" },
        RowError::SpaceContainsNul => FormatError::InvalidSpace {
            reason: "contains NUL",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        append_physical_key, decode_internal_key, decode_physical_key, encode_internal_key,
        encode_physical_key, internal_key_commit_version, internal_key_encoded_len,
        physical_key_encode_capacity, physical_key_encoded_len, FormatError, PHYSICAL_KEY_FORMAT,
    };
    use crate::row::{InternalKey, PhysicalKey, StorageSpaceId};
    use strata_core::{BranchId, CommitVersion};

    fn branch() -> BranchId {
        BranchId::from_bytes([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ])
    }

    fn key(user_key: impl Into<Vec<u8>>) -> PhysicalKey {
        PhysicalKey::new(
            branch(),
            "default",
            StorageSpaceId::engine(0x20).expect("engine id"),
            user_key,
        )
        .expect("physical key")
    }

    #[test]
    fn physical_key_round_trips_escaped_user_bytes() {
        let key = key([0x00, 0x41, 0x00]);
        let bytes = encode_physical_key(&key);

        assert_eq!(decode_physical_key(&bytes), Ok(key));
    }

    #[test]
    fn internal_key_sorts_newest_version_first_for_same_key() {
        let key = key(b"alpha".to_vec());
        let newer = encode_internal_key(&InternalKey::new(key.clone(), CommitVersion::new(42)));
        let older = encode_internal_key(&InternalKey::new(key, CommitVersion::new(7)));

        assert!(newer < older);
    }

    /// C3b: the suffix-only version parse agrees with the full decode for
    /// every version shape, without materializing the key.
    #[test]
    fn internal_key_commit_version_matches_full_decode() {
        for version in [0u64, 1, 7, 42, u64::MAX - 1, u64::MAX] {
            let internal = InternalKey::new(key(b"alpha".to_vec()), CommitVersion::new(version));
            let bytes = encode_internal_key(&internal);
            assert_eq!(
                internal_key_commit_version(&bytes),
                Ok(CommitVersion::new(version))
            );
            assert_eq!(
                decode_internal_key(&bytes).map(|key| key.commit_version()),
                Ok(CommitVersion::new(version))
            );
        }
    }

    /// C3b: inputs shorter than the version suffix fail closed.
    #[test]
    fn internal_key_commit_version_rejects_short_input() {
        assert!(matches!(
            internal_key_commit_version(&[0u8; 7]),
            Err(FormatError::InsufficientBytes { .. })
        ));
    }

    #[test]
    fn internal_key_round_trips_max_commit_version() {
        let key = InternalKey::new(key(b"alpha".to_vec()), CommitVersion::MAX);
        let bytes = encode_internal_key(&key);

        assert_eq!(decode_internal_key(&bytes), Ok(key));
    }

    #[test]
    fn decode_rejects_invalid_storage_space_id() {
        let mut bytes = encode_physical_key(&key(b"alpha".to_vec()));
        let id_offset = BranchId::BYTE_LEN + "default".len() + 1;
        bytes[id_offset] = 0x00;

        assert_eq!(
            decode_physical_key(&bytes),
            Err(FormatError::InvalidStorageSpaceId { raw: 0 })
        );
    }

    #[test]
    fn decode_rejects_trailing_key_bytes() {
        let mut bytes = encode_physical_key(&key(b"alpha".to_vec()));
        bytes.push(0xff);

        assert_eq!(
            decode_physical_key(&bytes),
            Err(FormatError::TrailingData {
                format: PHYSICAL_KEY_FORMAT,
                remaining: 1
            })
        );
    }

    /// Commit admission refuses an oversized row by computing its length
    /// instead of encoding it, so the computation has to agree with the
    /// encoder byte-for-byte — under-counting admits a row the WAL cannot
    /// write, over-counting refuses one it can (#3391).
    #[test]
    fn physical_key_encoded_len_matches_the_encoder() {
        let user_keys: [Vec<u8>; 6] = [
            Vec::new(),
            b"alpha".to_vec(),
            vec![0x00],
            vec![0x00; 64],
            vec![0x00, 0x41, 0x00, 0xff, 0x00],
            (0u8..=255).collect(),
        ];

        for user_key in user_keys {
            let key = key(user_key.clone());
            assert_eq!(
                physical_key_encoded_len(&key),
                encode_physical_key(&key).len(),
                "computed length disagrees with the encoder for {user_key:?}"
            );
        }
    }

    /// The capacity hint may under-reserve (a realloc) but must never claim
    /// more than the encoding needs, or every key over-allocates.
    #[test]
    fn physical_key_encode_capacity_never_exceeds_the_encoded_length() {
        for user_key in [Vec::new(), b"alpha".to_vec(), vec![0x00; 8]] {
            let key = key(user_key.clone());
            assert!(
                physical_key_encode_capacity(&key) <= physical_key_encoded_len(&key),
                "capacity hint over-reserves for {user_key:?}"
            );
        }
    }

    /// The table format caps the INTERNAL key, so admission must size the
    /// same thing the table builder measures — including the version suffix,
    /// which a check written against the physical key alone would miss (#3396).
    #[test]
    fn internal_key_encoded_len_matches_the_encoder() {
        let user_keys: [Vec<u8>; 5] = [
            Vec::new(),
            b"alpha".to_vec(),
            vec![0x00; 64],
            vec![0x00, 0x41, 0x00, 0xff, 0x00],
            (0u8..=255).collect(),
        ];

        for user_key in user_keys {
            let physical = key(user_key.clone());
            for version in [
                CommitVersion::ZERO,
                CommitVersion::new(7),
                CommitVersion::MAX,
            ] {
                let internal = InternalKey::new(physical.clone(), version);
                assert_eq!(
                    internal_key_encoded_len(&physical),
                    encode_internal_key(&internal).len(),
                    "computed length disagrees with the encoder for {user_key:?} at {version:?}"
                );
            }
        }
    }

    /// The hint's whole job is to let the common case encode in one
    /// allocation — the escape encode pushes byte by byte, and growing from
    /// empty reallocs several times per key on the hot path. That property is
    /// invisible to every behavioural test (a wrong hint still encodes
    /// correctly), so assert the allocation directly or nothing guards it.
    #[test]
    fn physical_key_encoding_does_not_reallocate_for_a_key_with_nothing_to_escape() {
        let key = key(b"alpha".to_vec());
        let mut bytes = Vec::with_capacity(physical_key_encode_capacity(&key));
        let reserved = bytes.capacity();
        append_physical_key(&key, &mut bytes);

        assert_eq!(
            bytes.capacity(),
            reserved,
            "encoding outgrew the reserve and reallocated"
        );
        assert_eq!(bytes.len(), reserved, "the reserve was larger than needed");
    }
}
