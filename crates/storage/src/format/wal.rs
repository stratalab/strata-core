use super::{
    ByteReader, FormatError, WAL_RECORD_ENVELOPE_HEADER_SIZE, WAL_RECORD_FORMAT_VERSION,
    WAL_RECORD_FORMAT_VERSION_V1, WAL_RECORD_MIN_LEN_AFTER_PREFIX, WAL_SEGMENT_BASE_HEADER_SIZE,
    WAL_SEGMENT_FORMAT_VERSION, WAL_SEGMENT_HEADER_SIZE,
};
use strata_core::{BranchId, CommitVersion, Timestamp};

mod commit_payload;

use commit_payload::MAX_WAL_COMMIT_PAYLOAD_BYTES;
pub(crate) use commit_payload::{
    decode_wal_commit_payload, encode_wal_commit_payload, encode_wal_commit_payload_into,
    wal_commit_payload_len, WalCommitPayload, MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES,
};

const WAL_ENVELOPE_FORMAT: &str = "wal_record_envelope";
const WAL_RECORD_FORMAT: &str = "wal_record";
const WAL_RECORD_LENGTH_FORMAT: &str = "wal_record_len";
const WAL_SEGMENT_FORMAT: &str = "wal_segment_header";
const WAL_SEGMENT_MAGIC: [u8; 4] = *b"STRA";
/// The `u32` record length written ahead of every encoded record.
const WAL_RECORD_LEN_PREFIX_BYTES: usize = 4;
/// Version, length CRC, commit version, branch id, commit timestamp and
/// `committed_at` — the fixed record header ahead of the commit payload.
const WAL_RECORD_FIXED_HEADER_BYTES: usize = 1 + 4 + 8 + BranchId::BYTE_LEN + 8 + 8;
/// The payload CRC that closes every record.
const WAL_RECORD_CRC_BYTES: usize = 4;

/// The record length the encoder writes in the length prefix for a commit
/// payload of `commit_payload_len` bytes: everything after the prefix itself.
fn wal_record_len_after_prefix(commit_payload_len: usize) -> Option<usize> {
    WAL_RECORD_FIXED_HEADER_BYTES
        .checked_add(commit_payload_len)?
        .checked_add(WAL_RECORD_CRC_BYTES)
}

/// The exact length of the enveloped frame the WAL appends for a commit
/// payload of `commit_payload_len` bytes: envelope header, record length
/// prefix, fixed record header, payload and CRC.
///
/// Admission's pre-allocation size check and the encoder both derive the
/// frame from this one layout (#3698). `None` only on `usize` overflow.
pub(crate) fn wal_record_frame_len(commit_payload_len: usize) -> Option<usize> {
    WAL_RECORD_ENVELOPE_HEADER_SIZE
        .checked_add(WAL_RECORD_LEN_PREFIX_BYTES)?
        .checked_add(wal_record_len_after_prefix(commit_payload_len)?)
}

/// The exact WAL frame length of a commit whose rows encode to `row_lens`
/// bytes each. Saturates at `u64::MAX` on overflow, which is above every
/// limit and therefore still refused.
pub(crate) fn wal_commit_record_frame_len<I>(row_lens: I) -> u64
where
    I: IntoIterator<Item = usize>,
{
    wal_commit_payload_len(row_lens)
        .and_then(wal_record_frame_len)
        .and_then(|len| u64::try_from(len).ok())
        .unwrap_or(u64::MAX)
}

/// The largest commit record frame a WAL with `segment_size`-byte segments
/// can append (#3698).
///
/// Two caps bind a frame and this is the tighter of them: a record never
/// spans segments, so the frame must fit in a segment after its header; and
/// the commit payload format caps the payload at
/// `MAX_WAL_COMMIT_PAYLOAD_BYTES`. The WAL append enforces the same value, so
/// a batch admitted against it is a batch the append accepts.
pub(crate) fn wal_record_frame_limit(segment_size: u64) -> u64 {
    let segment_room = segment_size.saturating_sub(WAL_SEGMENT_HEADER_SIZE as u64);
    let format_room = wal_record_frame_len(MAX_WAL_COMMIT_PAYLOAD_BYTES)
        .and_then(|len| u64::try_from(len).ok())
        .unwrap_or(u64::MAX);
    segment_room.min(format_room)
}

/// The production WAL segment size. The WAL service opens with it unless a
/// test seam overrides it, and commit admission sizes every record against
/// its limit in EVERY durability mode, so cache mode refuses what a default
/// durable database could not append (#3391, #3698).
pub(crate) const DEFAULT_WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// [`wal_record_frame_limit`] for the production segment.
pub(crate) fn default_wal_record_frame_limit() -> u64 {
    wal_record_frame_limit(DEFAULT_WAL_SEGMENT_SIZE)
}

/// The admission decision for a commit record's size: whether a frame of
/// `frame_len` bytes fits under `limit` (from [`wal_record_frame_limit`]).
/// A frame exactly at the limit fits.
pub(crate) const fn wal_record_frame_fits(frame_len: u64, limit: u64) -> bool {
    frame_len <= limit
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalSegmentHeader {
    segment_id: u64,
    database_id: [u8; 16],
}

impl WalSegmentHeader {
    pub(crate) const fn new(segment_id: u64, database_id: [u8; 16]) -> Self {
        Self {
            segment_id,
            database_id,
        }
    }

    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub(crate) const fn database_id(&self) -> &[u8; 16] {
        &self.database_id
    }
}

pub(crate) fn encode_wal_segment_header(header: &WalSegmentHeader) -> Vec<u8> {
    // The header CRC covers only fixed header facts. Record bytes are protected
    // by per-record envelopes so segment recovery can classify a partial tail
    // without trusting unrelated later bytes.
    let mut bytes = Vec::with_capacity(WAL_SEGMENT_HEADER_SIZE);
    bytes.extend_from_slice(&WAL_SEGMENT_MAGIC);
    bytes.extend_from_slice(&WAL_SEGMENT_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&header.segment_id().to_le_bytes());
    bytes.extend_from_slice(header.database_id());
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

pub(crate) fn decode_wal_segment_header(
    bytes: &[u8],
    expected_segment_id: Option<u64>,
) -> Result<(WalSegmentHeader, usize), FormatError> {
    if bytes.len() < WAL_SEGMENT_HEADER_SIZE {
        return Err(FormatError::InsufficientBytes {
            format: WAL_SEGMENT_FORMAT,
            needed: WAL_SEGMENT_HEADER_SIZE,
            actual: bytes.len(),
        });
    }

    let mut reader = ByteReader::new(WAL_SEGMENT_FORMAT, &bytes[..WAL_SEGMENT_BASE_HEADER_SIZE]);
    if reader.read_exact(4)? != WAL_SEGMENT_MAGIC {
        return Err(FormatError::InvalidMagic {
            format: WAL_SEGMENT_FORMAT,
        });
    }

    let version = reader.read_u32_le()?;
    match version {
        WAL_SEGMENT_FORMAT_VERSION => {}
        0 | 2 | 3 => {
            // These versions are intentionally rejected as known non-V1
            // development formats instead of being treated as future data.
            return Err(FormatError::PreV1Format {
                format: WAL_SEGMENT_FORMAT,
                version,
            });
        }
        version => {
            return Err(FormatError::FutureFormat {
                format: WAL_SEGMENT_FORMAT,
                version,
                max_supported: WAL_SEGMENT_FORMAT_VERSION,
            });
        }
    }

    let computed_crc = crc32fast::hash(&bytes[..WAL_SEGMENT_BASE_HEADER_SIZE]);
    let stored_crc = u32::from_le_bytes(
        bytes[WAL_SEGMENT_BASE_HEADER_SIZE..WAL_SEGMENT_HEADER_SIZE]
            .try_into()
            .map_err(|_| FormatError::InvalidLength {
                field: "wal_segment_crc32",
            })?,
    );
    if stored_crc != computed_crc {
        return Err(FormatError::ChecksumMismatch {
            format: WAL_SEGMENT_FORMAT,
            expected: stored_crc,
            computed: computed_crc,
        });
    }

    let segment_id = reader.read_u64_le()?;
    if expected_segment_id.is_some_and(|expected| expected != segment_id) {
        return Err(FormatError::InvalidValue {
            field: "segment_id",
        });
    }
    let database_id = reader.read_exact(16)?;
    let database_id =
        <[u8; 16]>::try_from(database_id).map_err(|_| FormatError::InvalidLength {
            field: "database_id",
        })?;
    reader.finish()?;

    Ok((
        WalSegmentHeader {
            segment_id,
            database_id,
        },
        WAL_SEGMENT_HEADER_SIZE,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalRecord {
    commit_version: CommitVersion,
    branch_id: BranchId,
    commit_timestamp: Timestamp,
    /// Wall-clock instant the commit was applied (#3112). `None` on a v1 record
    /// (written before the field existed) and on any commit whose instant is
    /// unknown. Never the MVCC clock — it takes no part in ordering.
    committed_at: Option<Timestamp>,
    commit_payload: WalCommitPayload,
}

impl WalRecord {
    pub(crate) fn new(
        commit_version: CommitVersion,
        branch_id: BranchId,
        commit_timestamp: Timestamp,
        commit_payload: WalCommitPayload,
    ) -> Result<Self, FormatError> {
        commit_payload.validate_outer_facts(commit_version, branch_id, commit_timestamp)?;
        Ok(Self {
            commit_version,
            branch_id,
            commit_timestamp,
            committed_at: None,
            commit_payload,
        })
    }

    /// Attaches the commit's wall-clock instant. Kept as a builder so `new`'s
    /// signature — and its call sites — stay put (#3112 S2).
    #[must_use]
    pub(crate) fn with_committed_at(mut self, committed_at: Option<Timestamp>) -> Self {
        self.committed_at = committed_at;
        self
    }

    pub(crate) const fn committed_at(&self) -> Option<Timestamp> {
        self.committed_at
    }

    pub(crate) const fn commit_version(&self) -> CommitVersion {
        self.commit_version
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn commit_timestamp(&self) -> Timestamp {
        self.commit_timestamp
    }

    pub(crate) fn commit_payload(&self) -> &WalCommitPayload {
        &self.commit_payload
    }

    pub(crate) fn into_commit_payload(self) -> WalCommitPayload {
        self.commit_payload
    }
}

pub(crate) fn encode_wal_record(record: &WalRecord) -> Result<Vec<u8>, FormatError> {
    let mut bytes = Vec::new();
    encode_wal_record_into(record, &mut bytes)?;
    Ok(bytes)
}

pub(crate) fn encode_wal_record_into(
    record: &WalRecord,
    bytes: &mut Vec<u8>,
) -> Result<(), FormatError> {
    let mut payload_bytes = Vec::new();
    let mut row_bytes = Vec::new();
    encode_wal_record_into_reusing(record, bytes, &mut payload_bytes, &mut row_bytes)
}

pub(crate) fn encode_wal_record_into_reusing(
    record: &WalRecord,
    bytes: &mut Vec<u8>,
    payload_bytes: &mut Vec<u8>,
    row_bytes: &mut Vec<u8>,
) -> Result<(), FormatError> {
    bytes.clear();
    encode_wal_commit_payload_into(record.commit_payload(), payload_bytes, row_bytes)?;
    // record_len includes the versioned payload and trailing payload CRC, but
    // excludes the 4-byte length prefix itself. The separate length CRC lets
    // readers reject impossible lengths before allocating or slicing payloads.
    // The fixed header carries `committed_at` since #3112 S2. The layout is
    // shared with admission's pre-allocation size check (#3698).
    let record_len =
        wal_record_len_after_prefix(payload_bytes.len()).ok_or(FormatError::InvalidLength {
            field: WAL_RECORD_FORMAT,
        })?;
    let record_len = u32::try_from(record_len).map_err(|_| FormatError::InvalidLength {
        field: WAL_RECORD_FORMAT,
    })?;
    let record_len_bytes = record_len.to_le_bytes();

    let capacity = WAL_RECORD_LEN_PREFIX_BYTES
        .checked_add(record_len as usize)
        .ok_or(FormatError::InvalidLength {
            field: WAL_RECORD_FORMAT,
        })?;
    bytes.reserve(capacity);
    bytes.extend_from_slice(&record_len_bytes);

    let payload_start = bytes.len();
    bytes.push(WAL_RECORD_FORMAT_VERSION);
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(&record.commit_version().as_u64().to_le_bytes());
    bytes.extend_from_slice(record.branch_id().as_bytes());
    bytes.extend_from_slice(&record.commit_timestamp().as_micros().to_le_bytes());
    // `committed_at`: 0 means unknown, following the format's existing
    // `optional_nonzero` convention for optional u64s (#3112 S2). A real
    // wall-clock instant is always past the epoch, so nothing legitimate
    // collides with the sentinel.
    bytes.extend_from_slice(
        &record
            .committed_at()
            .map_or(0, Timestamp::as_micros)
            .to_le_bytes(),
    );
    bytes.extend_from_slice(payload_bytes);

    let len_crc = crc32fast::hash(&record_len_bytes);
    bytes[payload_start + 1..payload_start + 5].copy_from_slice(&len_crc.to_le_bytes());

    let payload_crc = crc32fast::hash(&bytes[payload_start..]);
    bytes.extend_from_slice(&payload_crc.to_le_bytes());
    Ok(())
}

pub(crate) fn decode_wal_record(bytes: &[u8]) -> Result<(WalRecord, usize), FormatError> {
    if bytes.len() < 5 {
        return Err(FormatError::InsufficientBytes {
            format: WAL_RECORD_FORMAT,
            needed: 5,
            actual: bytes.len(),
        });
    }

    let record_len = read_wal_record_len(bytes)?;
    verify_wal_record_len_crc(bytes)?;
    validate_wal_record_len(record_len)?;

    // Only after the length CRC passes do we trust record_len enough to compute
    // the full frame boundary.
    let total_len = 4usize
        .checked_add(record_len)
        .ok_or(FormatError::InvalidLength {
            field: WAL_RECORD_LENGTH_FORMAT,
        })?;
    if bytes.len() < total_len {
        return Err(FormatError::InsufficientBytes {
            format: WAL_RECORD_FORMAT,
            needed: total_len,
            actual: bytes.len(),
        });
    }

    let payload = verify_wal_record_payload_crc(&bytes[4..total_len], record_len)?;
    validate_wal_record_version(payload[0])?;
    Ok((decode_wal_record_payload(payload)?, total_len))
}

fn read_wal_record_len(bytes: &[u8]) -> Result<usize, FormatError> {
    Ok(u32::from_le_bytes(
        bytes[..4]
            .try_into()
            .map_err(|_| FormatError::InvalidLength {
                field: WAL_RECORD_LENGTH_FORMAT,
            })?,
    ) as usize)
}

fn validate_wal_record_len(record_len: usize) -> Result<(), FormatError> {
    if record_len < WAL_RECORD_MIN_LEN_AFTER_PREFIX {
        return Err(FormatError::InvalidLength {
            field: WAL_RECORD_LENGTH_FORMAT,
        });
    }
    Ok(())
}

fn validate_wal_record_version(version: u8) -> Result<(), FormatError> {
    match version {
        // v1 predates `committed_at` and stays readable (#3112 S2); v3 is
        // current. 2 is deliberately skipped — it marks a pre-V1 record below.
        WAL_RECORD_FORMAT_VERSION | WAL_RECORD_FORMAT_VERSION_V1 => Ok(()),
        0 | 2 => Err(FormatError::PreV1Format {
            format: WAL_RECORD_FORMAT,
            version: u32::from(version),
        }),
        version => Err(FormatError::FutureFormat {
            format: WAL_RECORD_FORMAT,
            version: u32::from(version),
            max_supported: u32::from(WAL_RECORD_FORMAT_VERSION),
        }),
    }
}

fn verify_wal_record_len_crc(bytes: &[u8]) -> Result<(), FormatError> {
    if bytes.len() < 9 {
        return Err(FormatError::InsufficientBytes {
            format: WAL_RECORD_FORMAT,
            needed: 9,
            actual: bytes.len(),
        });
    }
    let stored_len_crc =
        u32::from_le_bytes(
            bytes[5..9]
                .try_into()
                .map_err(|_| FormatError::InvalidLength {
                    field: WAL_RECORD_LENGTH_FORMAT,
                })?,
        );
    let computed_len_crc = crc32fast::hash(&bytes[..4]);
    if stored_len_crc == computed_len_crc {
        Ok(())
    } else {
        Err(FormatError::ChecksumMismatch {
            format: WAL_RECORD_LENGTH_FORMAT,
            expected: stored_len_crc,
            computed: computed_len_crc,
        })
    }
}

fn verify_wal_record_payload_crc(
    payload_with_crc: &[u8],
    record_len: usize,
) -> Result<&[u8], FormatError> {
    // The payload CRC covers version, length CRC, commit facts, and user bytes;
    // a branch id or timestamp mutation is therefore detected the same way as a
    // payload mutation.
    let payload = &payload_with_crc[..record_len - 4];
    let stored_payload_crc =
        u32::from_le_bytes(payload_with_crc[record_len - 4..].try_into().map_err(|_| {
            FormatError::InvalidLength {
                field: "wal_record_crc32",
            }
        })?);
    let computed_payload_crc = crc32fast::hash(payload);
    if stored_payload_crc == computed_payload_crc {
        Ok(payload)
    } else {
        Err(FormatError::ChecksumMismatch {
            format: WAL_RECORD_FORMAT,
            expected: stored_payload_crc,
            computed: computed_payload_crc,
        })
    }
}

fn decode_wal_record_payload(payload: &[u8]) -> Result<WalRecord, FormatError> {
    let commit_version =
        CommitVersion::new(u64::from_le_bytes(payload[5..13].try_into().map_err(
            |_| FormatError::InvalidLength {
                field: "commit_version",
            },
        )?));
    let branch_id = BranchId::try_from_slice(&payload[13..29])
        .map_err(|_| FormatError::InvalidLength { field: "branch_id" })?;
    let commit_timestamp =
        Timestamp::from_micros(u64::from_le_bytes(payload[29..37].try_into().map_err(
            |_| FormatError::InvalidLength {
                field: "commit_timestamp",
            },
        )?));
    // v1 records end the fixed header at 37; v3 carries `committed_at` there
    // and starts the commit payload at 45 (#3112 S2). The version byte was
    // validated by `validate_wal_record_version` before this call.
    let (committed_at, payload_start) = if payload[0] == WAL_RECORD_FORMAT_VERSION {
        let raw = payload
            .get(37..45)
            .ok_or(FormatError::InvalidLength {
                field: "committed_at",
            })?
            .try_into()
            .map_err(|_| FormatError::InvalidLength {
                field: "committed_at",
            })?;
        // 0 means unknown, matching the format's `optional_nonzero` convention.
        let micros = u64::from_le_bytes(raw);
        let committed_at = (micros != 0).then(|| Timestamp::from_micros(micros));
        (committed_at, 45)
    } else {
        (None, 37)
    };
    let commit_payload = decode_wal_commit_payload(&payload[payload_start..])?;

    Ok(
        WalRecord::new(commit_version, branch_id, commit_timestamp, commit_payload)?
            .with_committed_at(committed_at),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalRecordEnvelope {
    encoded_record: Vec<u8>,
}

impl WalRecordEnvelope {
    pub(crate) fn new(encoded_record: impl Into<Vec<u8>>) -> Result<Self, FormatError> {
        let encoded_record = encoded_record.into();
        if encoded_record.is_empty() {
            return Err(FormatError::InvalidLength {
                field: WAL_ENVELOPE_FORMAT,
            });
        }
        Ok(Self { encoded_record })
    }

    pub(crate) fn encoded_record(&self) -> &[u8] {
        &self.encoded_record
    }
}

pub(crate) fn encode_wal_record_envelope(
    envelope: &WalRecordEnvelope,
) -> Result<Vec<u8>, FormatError> {
    let mut bytes = Vec::new();
    encode_wal_record_envelope_bytes_into(envelope.encoded_record(), &mut bytes)?;
    Ok(bytes)
}

pub(crate) fn encode_wal_record_envelope_bytes_into(
    encoded_record: &[u8],
    bytes: &mut Vec<u8>,
) -> Result<(), FormatError> {
    if encoded_record.is_empty() {
        return Err(FormatError::InvalidLength {
            field: WAL_ENVELOPE_FORMAT,
        });
    }
    let encoded_len =
        u32::try_from(encoded_record.len()).map_err(|_| FormatError::InvalidLength {
            field: WAL_ENVELOPE_FORMAT,
        })?;
    let encoded_len_bytes = encoded_len.to_le_bytes();
    // The envelope length has its own checksum so recovery can distinguish a
    // torn envelope header from an intact envelope around a corrupt record.
    let encoded_len_crc = crc32fast::hash(&encoded_len_bytes);

    let capacity = WAL_RECORD_ENVELOPE_HEADER_SIZE
        .checked_add(encoded_record.len())
        .ok_or(FormatError::InvalidLength {
            field: WAL_ENVELOPE_FORMAT,
        })?;
    bytes.clear();
    bytes.reserve(capacity);
    bytes.extend_from_slice(&encoded_len_bytes);
    bytes.extend_from_slice(&encoded_len_crc.to_le_bytes());
    bytes.extend_from_slice(encoded_record);
    Ok(())
}

pub(crate) fn decode_wal_record_envelope(
    bytes: &[u8],
) -> Result<(WalRecordEnvelope, usize), FormatError> {
    if bytes.len() < WAL_RECORD_ENVELOPE_HEADER_SIZE {
        return Err(FormatError::InsufficientBytes {
            format: WAL_ENVELOPE_FORMAT,
            needed: WAL_RECORD_ENVELOPE_HEADER_SIZE,
            actual: bytes.len(),
        });
    }

    let encoded_len =
        u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .map_err(|_| FormatError::InvalidLength {
                    field: WAL_ENVELOPE_FORMAT,
                })?,
        ) as usize;
    if encoded_len == 0 {
        return Err(FormatError::InvalidLength {
            field: WAL_ENVELOPE_FORMAT,
        });
    }
    let stored_len_crc =
        u32::from_le_bytes(
            bytes[4..8]
                .try_into()
                .map_err(|_| FormatError::InvalidLength {
                    field: WAL_ENVELOPE_FORMAT,
                })?,
        );
    let computed_len_crc = crc32fast::hash(&bytes[..4]);
    if stored_len_crc != computed_len_crc {
        return Err(FormatError::ChecksumMismatch {
            format: WAL_ENVELOPE_FORMAT,
            expected: stored_len_crc,
            computed: computed_len_crc,
        });
    }

    // A short envelope payload is a tail fact only when the WAL service knows
    // this object is the latest segment. The byte codec reports the raw
    // insufficiency and leaves that policy decision to the service.
    let total_len = WAL_RECORD_ENVELOPE_HEADER_SIZE
        .checked_add(encoded_len)
        .ok_or(FormatError::InvalidLength {
            field: WAL_ENVELOPE_FORMAT,
        })?;
    if bytes.len() < total_len {
        return Err(FormatError::InsufficientBytes {
            format: WAL_ENVELOPE_FORMAT,
            needed: total_len,
            actual: bytes.len(),
        });
    }

    Ok((
        WalRecordEnvelope {
            encoded_record: bytes[WAL_RECORD_ENVELOPE_HEADER_SIZE..total_len].to_vec(),
        },
        total_len,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        decode_wal_record, decode_wal_record_envelope, decode_wal_segment_header,
        encode_wal_record, encode_wal_record_envelope, encode_wal_record_envelope_bytes_into,
        encode_wal_record_into_reusing, encode_wal_segment_header, WalCommitPayload, WalRecord,
        WalRecordEnvelope, WalSegmentHeader, WAL_ENVELOPE_FORMAT, WAL_RECORD_FORMAT,
        WAL_RECORD_LENGTH_FORMAT, WAL_SEGMENT_FORMAT,
    };
    use super::{
        wal_commit_payload_len, wal_commit_record_frame_len, wal_record_frame_fits,
        wal_record_frame_len, wal_record_frame_limit, MAX_WAL_COMMIT_PAYLOAD_BYTES,
    };
    use crate::format::{
        storage_row_encoded_len, FormatError, WAL_RECORD_FORMAT_VERSION,
        WAL_SEGMENT_FORMAT_VERSION, WAL_SEGMENT_HEADER_SIZE,
    };
    use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
    use strata_core::{BranchId, CommitVersion, Timestamp};

    fn segment_header() -> WalSegmentHeader {
        WalSegmentHeader::new(
            5,
            [
                0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
                0x2e, 0x2f,
            ],
        )
    }

    fn branch_id() -> BranchId {
        BranchId::from_bytes([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ])
    }

    fn other_branch_id() -> BranchId {
        BranchId::from_bytes([0x42; BranchId::BYTE_LEN])
    }

    fn physical_key(user_key: &[u8]) -> PhysicalKey {
        physical_key_for_branch(branch_id(), user_key)
    }

    fn physical_key_for_branch(branch_id: BranchId, user_key: &[u8]) -> PhysicalKey {
        PhysicalKey::new(
            branch_id,
            "default",
            StorageSpaceId::engine(0x20).expect("engine storage space"),
            user_key.to_vec(),
        )
        .expect("physical key")
    }

    fn payload_for_facts(
        commit_version: CommitVersion,
        branch_id: BranchId,
        commit_timestamp: Timestamp,
    ) -> WalCommitPayload {
        WalCommitPayload::new(vec![StorageRow::put(
            physical_key_for_branch(branch_id, b"alpha"),
            commit_version,
            commit_timestamp,
            Timestamp::EPOCH,
            b"value".to_vec(),
        )])
        .expect("commit payload")
    }

    fn record(row_value: impl Into<Vec<u8>>) -> WalRecord {
        let commit_version = CommitVersion::new(41);
        let branch_id = branch_id();
        let commit_timestamp = Timestamp::from_micros(1_700_000_000_123_456);
        let row = StorageRow::put(
            physical_key(b"alpha"),
            commit_version,
            commit_timestamp,
            Timestamp::EPOCH,
            row_value,
        );
        let payload = WalCommitPayload::new(vec![row]).expect("commit payload");
        WalRecord::new(commit_version, branch_id, commit_timestamp, payload).expect("WAL record")
    }

    fn refresh_header_crc(bytes: &mut [u8]) {
        let crc = crc32fast::hash(&bytes[..32]);
        bytes[32..36].copy_from_slice(&crc.to_le_bytes());
    }

    fn refresh_record_len_crc(bytes: &mut [u8]) {
        let crc = crc32fast::hash(&bytes[..4]);
        bytes[5..9].copy_from_slice(&crc.to_le_bytes());
    }

    fn refresh_record_payload_crc(bytes: &mut [u8]) {
        let record_len =
            u32::from_le_bytes(bytes[..4].try_into().expect("record length bytes")) as usize;
        let payload_start = 4;
        let crc_start = payload_start + record_len - 4;
        let crc = crc32fast::hash(&bytes[payload_start..crc_start]);
        bytes[crc_start..crc_start + 4].copy_from_slice(&crc.to_le_bytes());
    }

    fn first_commit_payload_row_start(bytes: &[u8]) -> usize {
        // len prefix + version + len_crc + commit_version + branch_id
        // + commit_timestamp + committed_at (the v3 field, #3112 S2).
        let commit_payload_start = 4 + 1 + 4 + 8 + BranchId::BYTE_LEN + 8 + 8;
        let row_len_offset = commit_payload_start + 4 + 4 + 4;
        let row_len = u32::from_le_bytes(
            bytes[row_len_offset..row_len_offset + 4]
                .try_into()
                .expect("row length"),
        ) as usize;
        let row_start = row_len_offset + 4;
        assert!(row_len > 0);
        row_start
    }

    fn first_row_commit_version_offset(bytes: &[u8]) -> usize {
        let row_start = first_commit_payload_row_start(bytes);
        let key_len = u32::from_le_bytes(
            bytes[row_start + 1..row_start + 5]
                .try_into()
                .expect("physical key length"),
        ) as usize;
        row_start + 1 + 4 + key_len
    }

    #[test]
    fn wal_segment_header_round_trips() {
        let header = segment_header();
        let bytes = encode_wal_segment_header(&header);

        assert_eq!(bytes.len(), WAL_SEGMENT_HEADER_SIZE);
        assert_eq!(
            decode_wal_segment_header(&bytes, Some(header.segment_id())),
            Ok((header, WAL_SEGMENT_HEADER_SIZE))
        );
    }

    #[test]
    fn wal_segment_header_decode_consumes_header_only() {
        let header = segment_header();
        let mut bytes = encode_wal_segment_header(&header);
        bytes.extend_from_slice(b"record bytes");

        assert_eq!(
            decode_wal_segment_header(&bytes, Some(header.segment_id())),
            Ok((header, WAL_SEGMENT_HEADER_SIZE))
        );
    }

    #[test]
    fn wal_segment_header_rejects_invalid_magic() {
        let mut bytes = encode_wal_segment_header(&segment_header());
        bytes[0] = b'X';

        assert_eq!(
            decode_wal_segment_header(&bytes, None),
            Err(FormatError::InvalidMagic {
                format: WAL_SEGMENT_FORMAT
            })
        );
    }

    #[test]
    fn wal_segment_header_rejects_pre_v1_versions() {
        for version in [0u32, 2, 3] {
            let mut bytes = encode_wal_segment_header(&segment_header());
            bytes[4..8].copy_from_slice(&version.to_le_bytes());
            refresh_header_crc(&mut bytes);

            assert_eq!(
                decode_wal_segment_header(&bytes, None),
                Err(FormatError::PreV1Format {
                    format: WAL_SEGMENT_FORMAT,
                    version
                })
            );
        }
    }

    #[test]
    fn wal_segment_header_rejects_future_version() {
        let mut bytes = encode_wal_segment_header(&segment_header());
        bytes[4..8].copy_from_slice(&(WAL_SEGMENT_FORMAT_VERSION + 8).to_le_bytes());
        refresh_header_crc(&mut bytes);

        assert_eq!(
            decode_wal_segment_header(&bytes, None),
            Err(FormatError::FutureFormat {
                format: WAL_SEGMENT_FORMAT,
                version: WAL_SEGMENT_FORMAT_VERSION + 8,
                max_supported: WAL_SEGMENT_FORMAT_VERSION
            })
        );
    }

    #[test]
    fn wal_segment_header_rejects_checksum_mismatch() {
        let mut bytes = encode_wal_segment_header(&segment_header());
        bytes[20] ^= 0xff;

        assert!(matches!(
            decode_wal_segment_header(&bytes, None),
            Err(FormatError::ChecksumMismatch {
                format: WAL_SEGMENT_FORMAT,
                ..
            })
        ));
    }

    #[test]
    fn wal_segment_header_rejects_segment_id_mismatch() {
        let bytes = encode_wal_segment_header(&segment_header());

        assert_eq!(
            decode_wal_segment_header(&bytes, Some(6)),
            Err(FormatError::InvalidValue {
                field: "segment_id"
            })
        );
    }

    #[test]
    fn wal_segment_header_rejects_truncated_header() {
        let bytes = encode_wal_segment_header(&segment_header());

        assert_eq!(
            decode_wal_segment_header(&bytes[..WAL_SEGMENT_HEADER_SIZE - 1], None),
            Err(FormatError::InsufficientBytes {
                format: WAL_SEGMENT_FORMAT,
                needed: WAL_SEGMENT_HEADER_SIZE,
                actual: WAL_SEGMENT_HEADER_SIZE - 1
            })
        );
    }

    #[test]
    fn wal_record_round_trips_empty_row_value() {
        let record = record(Vec::new());
        let bytes = encode_wal_record(&record).expect("encode record");

        assert_eq!(decode_wal_record(&bytes), Ok((record, bytes.len())));
    }

    #[test]
    fn wal_record_round_trips_non_empty_row_value() {
        let record = record(b"payload".to_vec());
        let bytes = encode_wal_record(&record).expect("encode record");

        assert_eq!(decode_wal_record(&bytes), Ok((record, bytes.len())));
    }

    #[test]
    fn wal_reusable_record_and_envelope_encoding_matches_vec_helpers() {
        let record = record(b"payload".to_vec());
        let expected_record = encode_wal_record(&record).expect("encode record");
        let expected_envelope = encode_wal_record_envelope(
            &WalRecordEnvelope::new(expected_record.clone()).expect("envelope"),
        )
        .expect("encode envelope");
        let mut record_bytes = Vec::with_capacity(4096);
        let mut payload_bytes = Vec::with_capacity(4096);
        let mut row_bytes = Vec::with_capacity(4096);
        let mut envelope_bytes = Vec::with_capacity(4096);

        encode_wal_record_into_reusing(
            &record,
            &mut record_bytes,
            &mut payload_bytes,
            &mut row_bytes,
        )
        .expect("encode reusable record");
        encode_wal_record_envelope_bytes_into(&record_bytes, &mut envelope_bytes)
            .expect("encode reusable envelope");

        assert_eq!(record_bytes, expected_record);
        assert_eq!(envelope_bytes, expected_envelope);
    }

    #[test]
    fn wal_record_round_trips_put_and_tombstone_rows_in_order() {
        let commit_version = CommitVersion::new(41);
        let branch_id = branch_id();
        let commit_timestamp = Timestamp::from_micros(1_700_000_000_123_456);
        let rows = vec![
            StorageRow::put(
                physical_key(b"alpha"),
                commit_version,
                commit_timestamp,
                Timestamp::EPOCH,
                b"value".to_vec(),
            ),
            StorageRow::tombstone(physical_key(b"beta"), commit_version, commit_timestamp),
        ];
        let payload = WalCommitPayload::new(rows.clone()).expect("commit payload");
        let record =
            WalRecord::new(commit_version, branch_id, commit_timestamp, payload).expect("record");
        let bytes = encode_wal_record(&record).expect("encode record");

        let (decoded, consumed) = decode_wal_record(&bytes).expect("decode record");

        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.commit_payload().rows(), rows.as_slice());
        assert_eq!(decoded, record);
    }

    #[test]
    fn wal_record_preserves_duplicate_physical_keys_in_payload_order() {
        let commit_version = CommitVersion::new(41);
        let branch_id = branch_id();
        let commit_timestamp = Timestamp::from_micros(1_700_000_000_123_456);
        let duplicate_key = physical_key(b"duplicate");
        let rows = vec![
            StorageRow::put(
                duplicate_key.clone(),
                commit_version,
                commit_timestamp,
                Timestamp::EPOCH,
                b"first".to_vec(),
            ),
            StorageRow::put(
                duplicate_key,
                commit_version,
                commit_timestamp,
                Timestamp::EPOCH,
                b"second".to_vec(),
            ),
        ];
        let payload = WalCommitPayload::new(rows.clone()).expect("commit payload");
        let record =
            WalRecord::new(commit_version, branch_id, commit_timestamp, payload).expect("record");
        let bytes = encode_wal_record(&record).expect("encode record");

        let (decoded, consumed) = decode_wal_record(&bytes).expect("decode record");

        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.commit_payload().rows(), rows.as_slice());
    }

    #[test]
    fn wal_record_constructor_rejects_payload_outer_fact_mismatches() {
        let commit_version = CommitVersion::new(41);
        let branch_id = branch_id();
        let commit_timestamp = Timestamp::from_micros(1_700_000_000_123_456);
        let payload = payload_for_facts(commit_version, branch_id, commit_timestamp);

        assert_eq!(
            WalRecord::new(
                CommitVersion::new(42),
                branch_id,
                commit_timestamp,
                payload.clone()
            ),
            Err(FormatError::InvalidValue {
                field: "commit_version"
            })
        );
        assert_eq!(
            WalRecord::new(
                commit_version,
                other_branch_id(),
                commit_timestamp,
                payload.clone()
            ),
            Err(FormatError::InvalidValue { field: "branch_id" })
        );
        assert_eq!(
            WalRecord::new(
                commit_version,
                branch_id,
                Timestamp::from_micros(commit_timestamp.as_micros() + 1),
                payload
            ),
            Err(FormatError::InvalidValue {
                field: "commit_timestamp"
            })
        );
    }

    #[test]
    fn wal_record_decode_rejects_nested_payload_fact_mismatches_after_crc() {
        let mut commit_version_mismatch =
            encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        let version_offset = first_row_commit_version_offset(&commit_version_mismatch);
        commit_version_mismatch[version_offset..version_offset + 8]
            .copy_from_slice(&CommitVersion::new(42).as_u64().to_le_bytes());
        refresh_record_payload_crc(&mut commit_version_mismatch);
        assert_eq!(
            decode_wal_record(&commit_version_mismatch),
            Err(FormatError::InvalidValue {
                field: "commit_version"
            })
        );

        let mut branch_mismatch =
            encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        let row_start = first_commit_payload_row_start(&branch_mismatch);
        let branch_offset = row_start + 1 + 4;
        branch_mismatch[branch_offset] ^= 0xff;
        refresh_record_payload_crc(&mut branch_mismatch);
        assert_eq!(
            decode_wal_record(&branch_mismatch),
            Err(FormatError::InvalidValue { field: "branch_id" })
        );

        let mut timestamp_mismatch =
            encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        let timestamp_offset = first_row_commit_version_offset(&timestamp_mismatch) + 8;
        timestamp_mismatch[timestamp_offset..timestamp_offset + 8]
            .copy_from_slice(&Timestamp::from_micros(42).as_micros().to_le_bytes());
        refresh_record_payload_crc(&mut timestamp_mismatch);
        assert_eq!(
            decode_wal_record(&timestamp_mismatch),
            Err(FormatError::InvalidValue {
                field: "commit_timestamp"
            })
        );
    }

    #[test]
    fn wal_record_decode_consumes_one_record_from_sequence() {
        let first = record(b"first".to_vec());
        let second = record(b"second".to_vec());
        let first_bytes = encode_wal_record(&first).expect("first record");
        let second_bytes = encode_wal_record(&second).expect("second record");
        let mut sequence = first_bytes.clone();
        sequence.extend_from_slice(&second_bytes);

        assert_eq!(decode_wal_record(&sequence), Ok((first, first_bytes.len())));
    }

    #[test]
    fn wal_record_rejects_length_crc_mismatch_before_trusting_length() {
        let mut bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        bytes[0] = 0xff;

        assert!(matches!(
            decode_wal_record(&bytes),
            Err(FormatError::ChecksumMismatch {
                format: WAL_RECORD_LENGTH_FORMAT,
                ..
            })
        ));
    }

    #[test]
    fn wal_record_rejects_payload_crc_mismatch() {
        let mut bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        bytes[20] ^= 0xff;

        assert!(matches!(
            decode_wal_record(&bytes),
            Err(FormatError::ChecksumMismatch {
                format: WAL_RECORD_FORMAT,
                ..
            })
        ));
    }

    #[test]
    fn wal_record_rejects_pre_v1_development_version() {
        let mut bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        bytes[4] = 2;
        refresh_record_payload_crc(&mut bytes);

        assert_eq!(
            decode_wal_record(&bytes),
            Err(FormatError::PreV1Format {
                format: WAL_RECORD_FORMAT,
                version: 2
            })
        );
    }

    #[test]
    fn wal_record_rejects_future_version() {
        let mut bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        bytes[4] = WAL_RECORD_FORMAT_VERSION + 8;
        refresh_record_payload_crc(&mut bytes);

        assert_eq!(
            decode_wal_record(&bytes),
            Err(FormatError::FutureFormat {
                format: WAL_RECORD_FORMAT,
                version: u32::from(WAL_RECORD_FORMAT_VERSION + 8),
                max_supported: u32::from(WAL_RECORD_FORMAT_VERSION)
            })
        );
    }

    #[test]
    fn wal_record_rejects_too_small_record_length() {
        let mut bytes = encode_wal_record(&record(Vec::new())).expect("encode record");
        bytes[..4].copy_from_slice(&1u32.to_le_bytes());
        refresh_record_len_crc(&mut bytes);

        assert_eq!(
            decode_wal_record(&bytes),
            Err(FormatError::InvalidLength {
                field: WAL_RECORD_LENGTH_FORMAT
            })
        );
    }

    #[test]
    fn wal_record_rejects_truncated_record() {
        let bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");

        assert_eq!(
            decode_wal_record(&bytes[..bytes.len() - 1]),
            Err(FormatError::InsufficientBytes {
                format: WAL_RECORD_FORMAT,
                needed: bytes.len(),
                actual: bytes.len() - 1
            })
        );
    }

    #[test]
    fn wal_record_envelope_round_trips() {
        let record_bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        let envelope = WalRecordEnvelope::new(record_bytes.clone()).expect("envelope");
        let envelope_bytes = encode_wal_record_envelope(&envelope).expect("encode envelope");

        assert_eq!(
            decode_wal_record_envelope(&envelope_bytes),
            Ok((envelope, envelope_bytes.len()))
        );
    }

    #[test]
    fn wal_record_envelope_decode_consumes_one_frame_from_sequence() {
        let record_bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        let envelope = WalRecordEnvelope::new(record_bytes.clone()).expect("envelope");
        let envelope_bytes = encode_wal_record_envelope(&envelope).expect("encode envelope");
        let mut sequence = envelope_bytes.clone();
        sequence.extend_from_slice(&envelope_bytes);

        assert_eq!(
            decode_wal_record_envelope(&sequence),
            Ok((envelope, envelope_bytes.len()))
        );
    }

    #[test]
    fn wal_record_envelope_rejects_empty_encoded_record() {
        assert_eq!(
            WalRecordEnvelope::new(Vec::new()),
            Err(FormatError::InvalidLength {
                field: WAL_ENVELOPE_FORMAT
            })
        );

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&0u32.to_le_bytes()).to_le_bytes());

        assert_eq!(
            decode_wal_record_envelope(&bytes),
            Err(FormatError::InvalidLength {
                field: WAL_ENVELOPE_FORMAT
            })
        );
    }

    #[test]
    fn wal_record_envelope_rejects_length_crc_mismatch() {
        let record_bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        let envelope = WalRecordEnvelope::new(record_bytes).expect("envelope");
        let mut bytes = encode_wal_record_envelope(&envelope).expect("encode envelope");
        bytes[4] ^= 0xff;

        assert!(matches!(
            decode_wal_record_envelope(&bytes),
            Err(FormatError::ChecksumMismatch {
                format: WAL_ENVELOPE_FORMAT,
                ..
            })
        ));
    }

    #[test]
    fn wal_record_envelope_rejects_truncated_payload() {
        let record_bytes = encode_wal_record(&record(b"payload".to_vec())).expect("encode record");
        let envelope = WalRecordEnvelope::new(record_bytes).expect("envelope");
        let bytes = encode_wal_record_envelope(&envelope).expect("encode envelope");

        assert_eq!(
            decode_wal_record_envelope(&bytes[..bytes.len() - 1]),
            Err(FormatError::InsufficientBytes {
                format: WAL_ENVELOPE_FORMAT,
                needed: bytes.len(),
                actual: bytes.len() - 1
            })
        );
    }

    // ---- #3698: the pre-allocation record size and the encoder agree ----

    /// Encodes `record` exactly as the WAL append does and returns the
    /// enveloped frame length.
    fn encoded_frame_len(record: &WalRecord) -> usize {
        let mut record_bytes = Vec::new();
        let mut payload_bytes = Vec::new();
        let mut row_bytes = Vec::new();
        let mut frame = Vec::new();
        encode_wal_record_into_reusing(
            record,
            &mut record_bytes,
            &mut payload_bytes,
            &mut row_bytes,
        )
        .expect("encode record");
        encode_wal_record_envelope_bytes_into(&record_bytes, &mut frame).expect("encode envelope");
        frame.len()
    }

    fn record_of(rows: Vec<StorageRow>, committed_at: Option<Timestamp>) -> WalRecord {
        let commit_version = CommitVersion::new(41);
        let commit_timestamp = Timestamp::from_micros(1_700_000_000_123_456);
        WalRecord::new(
            commit_version,
            branch_id(),
            commit_timestamp,
            WalCommitPayload::new(rows).expect("commit payload"),
        )
        .expect("WAL record")
        .with_committed_at(committed_at)
    }

    /// The length admission computes from the rows alone.
    fn computed_frame_len(record: &WalRecord) -> u64 {
        wal_commit_record_frame_len(
            record
                .commit_payload()
                .rows()
                .iter()
                .map(|row| storage_row_encoded_len(row.physical_key(), row.value().len())),
        )
    }

    /// Admission refuses a batch from `wal_commit_record_frame_len` before
    /// any byte is encoded; if it disagreed with the encoder, admission would
    /// either refuse a record the WAL accepts or let through one it refuses
    /// after the version is allocated (#3698). Generated rows: puts and
    /// tombstones, keys that need escaping, empty and non-empty values, with
    /// and without a `committed_at`.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn wal_commit_record_frame_len_matches_the_encoder() {
        use proptest::collection::vec;
        use proptest::prelude::any;
        use proptest::prop_assert_eq;
        use proptest::test_runner::{Config, TestRunner};

        let row = (
            vec(any::<u8>(), 0..48),
            any::<bool>(),
            vec(any::<u8>(), 0..300),
        );
        let strategy = (vec(row, 1..=24), any::<bool>());
        let mut runner = TestRunner::new(Config {
            failure_persistence: None,
            ..Config::default()
        });
        runner
            .run(&strategy, |(specs, stamped)| {
                let rows = specs
                    .into_iter()
                    .enumerate()
                    .map(|(index, (mut user_key, tombstone, value))| {
                        // Distinct keys; the index byte keeps generated keys apart.
                        user_key.push(u8::try_from(index).expect("index fits u8"));
                        if tombstone {
                            StorageRow::tombstone(
                                physical_key(&user_key),
                                CommitVersion::new(41),
                                Timestamp::from_micros(1_700_000_000_123_456),
                            )
                        } else {
                            StorageRow::put(
                                physical_key(&user_key),
                                CommitVersion::new(41),
                                Timestamp::from_micros(1_700_000_000_123_456),
                                Timestamp::from_micros(9),
                                value,
                            )
                        }
                    })
                    .collect();
                let committed_at = stamped.then(|| Timestamp::from_micros(1_800_000_000_000_000));
                let record = record_of(rows, committed_at);
                prop_assert_eq!(
                    computed_frame_len(&record),
                    u64::try_from(encoded_frame_len(&record)).expect("frame fits u64")
                );
                Ok(())
            })
            .expect("frame length property");
    }

    /// Truth table for the limit: the tighter of the segment's room after its
    /// header and the frame of a maximal commit payload.
    #[test]
    fn wal_record_frame_limit_is_the_tighter_of_segment_and_payload_caps() {
        let header = WAL_SEGMENT_HEADER_SIZE as u64;
        let format_frame =
            u64::try_from(wal_record_frame_len(MAX_WAL_COMMIT_PAYLOAD_BYTES).expect("frame"))
                .expect("fits u64");
        let cases: [(u64, u64); 6] = [
            // Test-seam segment from the #3698 repro.
            (1024, 1024 - header),
            // Production segment: the segment binds, just below the payload cap.
            (64 * 1024 * 1024, 64 * 1024 * 1024 - header),
            // A segment exactly large enough for the maximal payload's frame.
            (format_frame + header, format_frame),
            // A larger segment: the payload format cap binds instead.
            (format_frame + header + 1, format_frame),
            (1024 * 1024 * 1024, format_frame),
            // Degenerate: no room at all, so every frame is refused.
            (header, 0),
        ];
        for (segment_size, expected) in cases {
            assert_eq!(
                wal_record_frame_limit(segment_size),
                expected,
                "segment size {segment_size}"
            );
        }
    }

    /// Truth table for the fit decision, at limit-1 / limit / limit+1.
    #[test]
    fn wal_record_frame_fits_truth_table() {
        let cases: [(u64, u64, bool); 7] = [
            (987, 988, true),
            (988, 988, true),
            (989, 988, false),
            (0, 0, true),
            (1, 0, false),
            (u64::MAX, u64::MAX, true),
            (u64::MAX, u64::MAX - 1, false),
        ];
        for (frame_len, limit, fits) in cases {
            assert_eq!(
                wal_record_frame_fits(frame_len, limit),
                fits,
                "frame {frame_len} against limit {limit}"
            );
        }
    }

    /// The payload format cap, the one limit the encoder enforces itself, at
    /// its exact boundary: a payload of exactly `MAX_WAL_COMMIT_PAYLOAD_BYTES`
    /// encodes, and its frame is exactly the limit a large segment reports;
    /// one byte more is refused by the encoder, and its computed frame is one
    /// over that limit. So the limit admission checks is the encoder's.
    #[test]
    fn wal_record_frame_limit_matches_the_encoder_at_the_payload_cap() {
        // Four 15 MiB rows, then a last row whose value absorbs the remainder
        // so the payload lands exactly on the cap. Every row stays under the
        // 16 MiB row cap, so only the whole-payload cap can refuse.
        let big = 15 * 1024 * 1024;
        let keys: [&[u8]; 5] = [b"a", b"b", b"c", b"d", b"e"];
        let row_len =
            |key: &[u8], value_len: usize| storage_row_encoded_len(&physical_key(key), value_len);
        let payload_len_without_last_value = wal_commit_payload_len(
            keys[..4]
                .iter()
                .map(|key| row_len(key, big))
                .chain([row_len(keys[4], 0)]),
        )
        .expect("payload len");
        let last_value = MAX_WAL_COMMIT_PAYLOAD_BYTES - payload_len_without_last_value;
        let limit = wal_record_frame_limit(u64::MAX);

        for (extra, accepted) in [(0usize, true), (1, false)] {
            let mut rows: Vec<StorageRow> = keys[..4]
                .iter()
                .map(|key| {
                    StorageRow::put(
                        physical_key(key),
                        CommitVersion::new(41),
                        Timestamp::from_micros(1_700_000_000_123_456),
                        Timestamp::EPOCH,
                        vec![0x5a; big],
                    )
                })
                .collect();
            rows.push(StorageRow::put(
                physical_key(keys[4]),
                CommitVersion::new(41),
                Timestamp::from_micros(1_700_000_000_123_456),
                Timestamp::EPOCH,
                vec![0x5a; last_value + extra],
            ));
            let record = record_of(rows, None);
            let computed = computed_frame_len(&record);
            assert_eq!(computed, limit + extra as u64, "extra {extra}");
            assert_eq!(
                wal_record_frame_fits(computed, limit),
                accepted,
                "extra {extra}"
            );

            let mut record_bytes = Vec::new();
            let mut payload_bytes = Vec::new();
            let mut row_bytes = Vec::new();
            let encoded = encode_wal_record_into_reusing(
                &record,
                &mut record_bytes,
                &mut payload_bytes,
                &mut row_bytes,
            );
            assert_eq!(encoded.is_ok(), accepted, "extra {extra}: {encoded:?}");
            if accepted {
                assert_eq!(computed, encoded_frame_len(&record) as u64);
            }
        }
    }
}
