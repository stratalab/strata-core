use super::super::{
    storage_row::{decode_storage_row, encode_storage_row_into},
    ByteReader, FormatError,
};
use crate::row::StorageRow;
use strata_core::{BranchId, CommitVersion, Timestamp};

const WAL_COMMIT_PAYLOAD_FORMAT: &str = "wal_commit_payload";
const WAL_COMMIT_PAYLOAD_MAGIC: [u8; 4] = *b"STCP";
const WAL_COMMIT_PAYLOAD_FORMAT_VERSION: u32 = 1;
const MAX_WAL_COMMIT_PAYLOAD_ROWS: usize = 4096;
const MAX_WAL_COMMIT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
/// The largest single encoded storage row a commit payload can carry.
///
/// Enforced here at encode time, and — because cache mode never reaches this
/// encoder (hard rule 14) — again at commit admission, so a write cache mode
/// accepts is a write durable mode accepts (#3391).
pub(crate) const MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalCommitPayload {
    rows: Vec<StorageRow>,
}

impl WalCommitPayload {
    pub(crate) fn new(rows: Vec<StorageRow>) -> Result<Self, FormatError> {
        validate_row_count(rows.len())?;
        Ok(Self { rows })
    }

    pub(crate) fn rows(&self) -> &[StorageRow] {
        &self.rows
    }

    pub(crate) fn into_rows(self) -> Vec<StorageRow> {
        self.rows
    }

    pub(crate) fn validate_outer_facts(
        &self,
        commit_version: CommitVersion,
        branch_id: BranchId,
        commit_timestamp: Timestamp,
    ) -> Result<(), FormatError> {
        for row in self.rows() {
            if row.commit_version() != commit_version {
                return Err(FormatError::InvalidValue {
                    field: "commit_version",
                });
            }
            if row.physical_key().branch_id() != branch_id {
                return Err(FormatError::InvalidValue { field: "branch_id" });
            }
            if row.commit_timestamp() != commit_timestamp {
                return Err(FormatError::InvalidValue {
                    field: "commit_timestamp",
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn encode_wal_commit_payload(
    payload: &WalCommitPayload,
) -> Result<Vec<u8>, FormatError> {
    let mut bytes = Vec::new();
    let mut row_bytes = Vec::new();
    encode_wal_commit_payload_into(payload, &mut bytes, &mut row_bytes)?;
    Ok(bytes)
}

pub(crate) fn encode_wal_commit_payload_into(
    payload: &WalCommitPayload,
    bytes: &mut Vec<u8>,
    row_bytes: &mut Vec<u8>,
) -> Result<(), FormatError> {
    validate_row_count(payload.rows().len())?;

    bytes.clear();
    bytes.extend_from_slice(&WAL_COMMIT_PAYLOAD_MAGIC);
    bytes.extend_from_slice(&WAL_COMMIT_PAYLOAD_FORMAT_VERSION.to_le_bytes());
    let row_count = u32::try_from(payload.rows().len())
        .map_err(|_| FormatError::InvalidLength { field: "row_count" })?;
    bytes.extend_from_slice(&row_count.to_le_bytes());

    for row in payload.rows() {
        encode_storage_row_into(row, row_bytes)?;
        validate_row_len(row_bytes.len())?;
        let row_len = u32::try_from(row_bytes.len())
            .map_err(|_| FormatError::InvalidLength { field: "row_len" })?;

        let required_len = bytes
            .len()
            .checked_add(4)
            .and_then(|len| len.checked_add(row_bytes.len()))
            .ok_or(FormatError::InvalidLength {
                field: WAL_COMMIT_PAYLOAD_FORMAT,
            })?;
        validate_payload_len(required_len)?;

        bytes.extend_from_slice(&row_len.to_le_bytes());
        bytes.extend_from_slice(row_bytes);
    }

    Ok(())
}

pub(crate) fn decode_wal_commit_payload(bytes: &[u8]) -> Result<WalCommitPayload, FormatError> {
    validate_payload_len(bytes.len())?;

    let mut reader = ByteReader::new(WAL_COMMIT_PAYLOAD_FORMAT, bytes);
    if reader.read_exact(4)? != WAL_COMMIT_PAYLOAD_MAGIC {
        return Err(FormatError::InvalidMagic {
            format: WAL_COMMIT_PAYLOAD_FORMAT,
        });
    }

    let version = reader.read_u32_le()?;
    match version {
        WAL_COMMIT_PAYLOAD_FORMAT_VERSION => {}
        0 => {
            return Err(FormatError::PreV1Format {
                format: WAL_COMMIT_PAYLOAD_FORMAT,
                version,
            });
        }
        version => {
            return Err(FormatError::FutureFormat {
                format: WAL_COMMIT_PAYLOAD_FORMAT,
                version,
                max_supported: WAL_COMMIT_PAYLOAD_FORMAT_VERSION,
            });
        }
    }

    let row_count = usize::try_from(reader.read_u32_le()?)
        .map_err(|_| FormatError::InvalidLength { field: "row_count" })?;
    validate_row_count(row_count)?;

    // Row count is bounded before allocation so malformed bytes cannot size the
    // decoded row vector directly.
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let row_len = usize::try_from(reader.read_u32_le()?)
            .map_err(|_| FormatError::InvalidLength { field: "row_len" })?;
        validate_row_len(row_len)?;
        let row_bytes = reader.read_exact(row_len)?;
        rows.push(decode_storage_row(row_bytes)?);
    }
    reader.finish()?;

    Ok(WalCommitPayload { rows })
}

fn validate_row_count(row_count: usize) -> Result<(), FormatError> {
    if row_count == 0 {
        return Err(FormatError::InvalidValue { field: "row_count" });
    }
    if row_count > MAX_WAL_COMMIT_PAYLOAD_ROWS {
        return Err(FormatError::InvalidLength { field: "row_count" });
    }
    Ok(())
}

fn validate_row_len(row_len: usize) -> Result<(), FormatError> {
    if row_len == 0 || row_len > MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES {
        return Err(FormatError::InvalidLength { field: "row_len" });
    }
    Ok(())
}

fn validate_payload_len(payload_len: usize) -> Result<(), FormatError> {
    if payload_len > MAX_WAL_COMMIT_PAYLOAD_BYTES {
        return Err(FormatError::InvalidLength {
            field: WAL_COMMIT_PAYLOAD_FORMAT,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        decode_wal_commit_payload, encode_wal_commit_payload, FormatError, WalCommitPayload,
        MAX_WAL_COMMIT_PAYLOAD_ROWS, MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES, WAL_COMMIT_PAYLOAD_FORMAT,
        WAL_COMMIT_PAYLOAD_FORMAT_VERSION, WAL_COMMIT_PAYLOAD_MAGIC,
    };
    use crate::format::storage_row::encode_storage_row;
    use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::collection::vec;
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::prelude::{any, prop_oneof, Just, Strategy};
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::prop_assert_eq;
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::test_runner::{Config, FileFailurePersistence, TestRunner};
    use strata_core::{BranchId, CommitVersion, Timestamp};

    fn branch_id() -> BranchId {
        BranchId::from_bytes([0x42; BranchId::BYTE_LEN])
    }

    fn other_branch_id() -> BranchId {
        BranchId::from_bytes([0x24; BranchId::BYTE_LEN])
    }

    fn physical_key(branch_id: BranchId, user_key: &[u8]) -> PhysicalKey {
        PhysicalKey::new(
            branch_id,
            "default",
            StorageSpaceId::engine(0x20).expect("engine storage space"),
            user_key.to_vec(),
        )
        .expect("physical key")
    }

    fn put_row(user_key: &[u8]) -> StorageRow {
        StorageRow::put(
            physical_key(branch_id(), user_key),
            CommitVersion::new(7),
            Timestamp::from_micros(1_700_000),
            Timestamp::from_micros(1_800_000),
            b"value".to_vec(),
        )
    }

    fn tombstone_row(user_key: &[u8]) -> StorageRow {
        StorageRow::tombstone(
            physical_key(branch_id(), user_key),
            CommitVersion::new(7),
            Timestamp::from_micros(1_700_000),
        )
    }

    fn payload_bytes_with_count(row_count: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&WAL_COMMIT_PAYLOAD_MAGIC);
        bytes.extend_from_slice(&WAL_COMMIT_PAYLOAD_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&row_count.to_le_bytes());
        bytes
    }

    fn payload_bytes_with_one_row_len(row_len: u32) -> Vec<u8> {
        let mut bytes = payload_bytes_with_count(1);
        bytes.extend_from_slice(&row_len.to_le_bytes());
        bytes
    }

    fn storage_row_flags_offset(row_bytes: &[u8]) -> usize {
        let physical_key_len =
            u32::from_le_bytes(row_bytes[1..5].try_into().expect("physical key len")) as usize;
        1 + 4 + physical_key_len + 8 + 8 + 8
    }

    fn storage_row_tombstone_offset(row_bytes: &[u8]) -> usize {
        storage_row_flags_offset(row_bytes) + 4
    }

    #[test]
    fn wal_commit_payload_one_put_row_round_trips() {
        let payload = WalCommitPayload::new(vec![put_row(b"alpha")]).expect("payload");
        let bytes = encode_wal_commit_payload(&payload).expect("encode payload");

        assert_eq!(decode_wal_commit_payload(&bytes), Ok(payload));
    }

    #[test]
    fn wal_commit_payload_preserves_put_and_tombstone_order() {
        let rows = vec![put_row(b"alpha"), tombstone_row(b"beta"), put_row(b"alpha")];
        let payload = WalCommitPayload::new(rows.clone()).expect("payload");

        let decoded = decode_wal_commit_payload(
            &encode_wal_commit_payload(&payload).expect("encode payload"),
        )
        .expect("decode payload");

        assert_eq!(decoded.rows(), rows.as_slice());
    }

    #[test]
    fn wal_commit_payload_rejects_empty_rows() {
        assert_eq!(
            WalCommitPayload::new(Vec::new()),
            Err(FormatError::InvalidValue { field: "row_count" })
        );
        assert_eq!(
            decode_wal_commit_payload(&payload_bytes_with_count(0)),
            Err(FormatError::InvalidValue { field: "row_count" })
        );
    }

    #[test]
    fn wal_commit_payload_rejects_invalid_magic() {
        let mut bytes = payload_bytes_with_count(1);
        bytes[..4].copy_from_slice(b"NOPE");

        assert_eq!(
            decode_wal_commit_payload(&bytes),
            Err(FormatError::InvalidMagic {
                format: WAL_COMMIT_PAYLOAD_FORMAT
            })
        );
    }

    #[test]
    fn wal_commit_payload_rejects_pre_v1_and_future_versions() {
        let mut pre_v1 = payload_bytes_with_count(1);
        pre_v1[4..8].copy_from_slice(&0_u32.to_le_bytes());
        assert_eq!(
            decode_wal_commit_payload(&pre_v1),
            Err(FormatError::PreV1Format {
                format: WAL_COMMIT_PAYLOAD_FORMAT,
                version: 0
            })
        );

        let mut future = payload_bytes_with_count(1);
        future[4..8].copy_from_slice(&9_u32.to_le_bytes());
        assert_eq!(
            decode_wal_commit_payload(&future),
            Err(FormatError::FutureFormat {
                format: WAL_COMMIT_PAYLOAD_FORMAT,
                version: 9,
                max_supported: WAL_COMMIT_PAYLOAD_FORMAT_VERSION
            })
        );
    }

    #[test]
    fn wal_commit_payload_rejects_row_count_before_allocation() {
        let excessive =
            u32::try_from(MAX_WAL_COMMIT_PAYLOAD_ROWS + 1).expect("test limit fits u32");

        assert_eq!(
            decode_wal_commit_payload(&payload_bytes_with_count(excessive)),
            Err(FormatError::InvalidLength { field: "row_count" })
        );
        assert_eq!(
            decode_wal_commit_payload(&payload_bytes_with_count(u32::MAX)),
            Err(FormatError::InvalidLength { field: "row_count" })
        );
    }

    #[test]
    fn wal_commit_payload_decodes_maximum_row_count_when_bytes_fit() {
        let rows = (0..MAX_WAL_COMMIT_PAYLOAD_ROWS)
            .map(|index| put_row(&index.to_le_bytes()))
            .collect::<Vec<_>>();
        let payload = WalCommitPayload::new(rows).expect("maximum row-count payload");
        let bytes = encode_wal_commit_payload(&payload).expect("encode maximum row-count payload");

        let decoded = decode_wal_commit_payload(&bytes).expect("decode maximum row-count payload");

        assert_eq!(decoded.rows().len(), MAX_WAL_COMMIT_PAYLOAD_ROWS);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn wal_commit_payload_rejects_zero_and_oversized_row_lengths_before_reading() {
        assert_eq!(
            decode_wal_commit_payload(&payload_bytes_with_one_row_len(0)),
            Err(FormatError::InvalidLength { field: "row_len" })
        );

        let excessive =
            u32::try_from(MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES + 1).expect("test limit fits u32");
        assert_eq!(
            decode_wal_commit_payload(&payload_bytes_with_one_row_len(excessive)),
            Err(FormatError::InvalidLength { field: "row_len" })
        );
    }

    #[test]
    fn wal_commit_payload_rejects_truncated_row_bytes() {
        let mut bytes = payload_bytes_with_one_row_len(3);
        bytes.extend_from_slice(&[1, 2]);

        assert_eq!(
            decode_wal_commit_payload(&bytes),
            Err(FormatError::InsufficientBytes {
                format: WAL_COMMIT_PAYLOAD_FORMAT,
                needed: 19,
                actual: 18
            })
        );
    }

    #[test]
    fn wal_commit_payload_propagates_nested_storage_row_errors() {
        let row = put_row(b"alpha");
        let mut row_bytes = encode_storage_row(&row).expect("encode row");
        row_bytes[0] = 9;

        let mut bytes = payload_bytes_with_one_row_len(
            u32::try_from(row_bytes.len()).expect("row len fits u32"),
        );
        bytes.extend_from_slice(&row_bytes);

        assert_eq!(
            decode_wal_commit_payload(&bytes),
            Err(FormatError::InvalidVersion {
                format: "storage_row",
                version: 9
            })
        );
    }

    #[test]
    fn wal_commit_payload_propagates_nested_storage_row_flag_errors() {
        let row = put_row(b"alpha");
        let mut row_bytes = encode_storage_row(&row).expect("encode row");
        let flags_offset = storage_row_flags_offset(&row_bytes);
        row_bytes[flags_offset] = 1;

        let mut bytes = payload_bytes_with_one_row_len(
            u32::try_from(row_bytes.len()).expect("row len fits u32"),
        );
        bytes.extend_from_slice(&row_bytes);

        assert_eq!(
            decode_wal_commit_payload(&bytes),
            Err(FormatError::UnsupportedFlags {
                format: "storage_row",
                flags: 1
            })
        );
    }

    #[test]
    fn wal_commit_payload_propagates_nested_tombstone_payload_errors() {
        let row = put_row(b"alpha");
        let mut row_bytes = encode_storage_row(&row).expect("encode row");
        let flags_offset = storage_row_flags_offset(&row_bytes);
        let expiry_offset = flags_offset - 8;
        row_bytes[expiry_offset..expiry_offset + 8]
            .copy_from_slice(&Timestamp::EPOCH.as_micros().to_le_bytes());
        let tombstone_offset = storage_row_tombstone_offset(&row_bytes);
        row_bytes[tombstone_offset] = 1;

        let mut bytes = payload_bytes_with_one_row_len(
            u32::try_from(row_bytes.len()).expect("row len fits u32"),
        );
        bytes.extend_from_slice(&row_bytes);

        assert_eq!(
            decode_wal_commit_payload(&bytes),
            Err(FormatError::InvalidTombstonePayload { field: "value" })
        );
    }

    #[test]
    fn wal_commit_payload_rejects_trailing_bytes() {
        let payload = WalCommitPayload::new(vec![put_row(b"alpha")]).expect("payload");
        let mut bytes = encode_wal_commit_payload(&payload).expect("encode payload");
        bytes.push(0);

        assert_eq!(
            decode_wal_commit_payload(&bytes),
            Err(FormatError::TrailingData {
                format: WAL_COMMIT_PAYLOAD_FORMAT,
                remaining: 1
            })
        );
    }

    #[test]
    fn wal_commit_payload_encoding_is_deterministic() {
        let payload = WalCommitPayload::new(vec![put_row(b"alpha"), tombstone_row(b"beta")])
            .expect("payload");

        assert_eq!(
            encode_wal_commit_payload(&payload),
            encode_wal_commit_payload(&payload)
        );
    }

    #[test]
    fn wal_commit_payload_validates_rows_against_outer_wal_facts() {
        let payload = WalCommitPayload::new(vec![put_row(b"alpha")]).expect("payload");
        assert_eq!(
            payload.validate_outer_facts(
                CommitVersion::new(7),
                branch_id(),
                Timestamp::from_micros(1_700_000)
            ),
            Ok(())
        );
        assert_eq!(
            payload.validate_outer_facts(
                CommitVersion::new(8),
                branch_id(),
                Timestamp::from_micros(1_700_000)
            ),
            Err(FormatError::InvalidValue {
                field: "commit_version"
            })
        );
        assert_eq!(
            payload.validate_outer_facts(
                CommitVersion::new(7),
                other_branch_id(),
                Timestamp::from_micros(1_700_000)
            ),
            Err(FormatError::InvalidValue { field: "branch_id" })
        );
        assert_eq!(
            payload.validate_outer_facts(
                CommitVersion::new(7),
                branch_id(),
                Timestamp::from_micros(1_700_001)
            ),
            Err(FormatError::InvalidValue {
                field: "commit_timestamp"
            })
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn row_strategy() -> impl Strategy<Value = StorageRow> {
        let value = vec(any::<u8>(), 0..=512);
        let user_key = prop_oneof![
            1 => Just(b"duplicate".to_vec()),
            3 => vec(any::<u8>(), 0..=64),
        ];
        (any::<bool>(), user_key, value).prop_map(|(tombstone, user_key, value)| {
            if tombstone {
                StorageRow::tombstone(
                    physical_key(branch_id(), &user_key),
                    CommitVersion::new(7),
                    Timestamp::from_micros(1_700_000),
                )
            } else {
                StorageRow::put(
                    physical_key(branch_id(), &user_key),
                    CommitVersion::new(7),
                    Timestamp::from_micros(1_700_000),
                    Timestamp::EPOCH,
                    value,
                )
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn wal_commit_payload_model_round_trips_generated_rows() {
        let strategy = vec(row_strategy(), 1..=64);
        let mut runner = TestRunner::new(Config {
            failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
                "proptest-regressions/wal_commit_payload.txt",
            ))),
            ..Config::default()
        });

        runner
            .run(&strategy, |rows| {
                let payload = WalCommitPayload::new(rows.clone()).expect("generated payload");
                let bytes = encode_wal_commit_payload(&payload).expect("encode generated payload");
                let decoded = decode_wal_commit_payload(&bytes).expect("decode generated payload");

                prop_assert_eq!(decoded.rows(), rows.as_slice());
                prop_assert_eq!(decoded, payload);
                Ok(())
            })
            .expect("WAL commit payload property");
    }

    /// The whole-payload cap is 64 MiB, and nothing else pins its magnitude:
    /// the row cap bounds each row, the row-count cap bounds how many, but a
    /// payload cap collapsed to roughly a megabyte would refuse ordinary
    /// multi-row commits and no other test would notice. Encode a payload far
    /// above any plausible wrong value and far below the real one.
    #[test]
    fn wal_commit_payload_admits_a_multi_megabyte_payload() {
        let row = |user_key: &[u8]| {
            StorageRow::put(
                physical_key(branch_id(), user_key),
                CommitVersion::new(7),
                Timestamp::from_micros(1_700_000),
                Timestamp::from_micros(1_800_000),
                vec![0x5a; 1024 * 1024],
            )
        };
        let payload = WalCommitPayload::new(vec![row(b"a"), row(b"b"), row(b"c"), row(b"d")])
            .expect("payload");

        let bytes = encode_wal_commit_payload(&payload).expect("4 MiB is well inside the cap");

        assert!(
            bytes.len() > 4 * 1024 * 1024,
            "fixture did not actually exceed a megabyte: {} bytes",
            bytes.len()
        );
        assert_eq!(
            decode_wal_commit_payload(&bytes).expect("decode"),
            payload,
            "the cap must admit on decode too"
        );
    }
}
