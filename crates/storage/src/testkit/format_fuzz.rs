use crate::format::fuzzing;
use crate::format::{decode_immutable_table, encode_immutable_table, TableCompression};
use crate::row::{InternalKey, PhysicalKey, StorageRow, StorageSpaceId};
use strata_core::{BranchId, CommitVersion, Timestamp};

use super::TestkitError;

/// Durable byte decoder available to storage fuzz targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormatDecoder {
    BranchCatalogManifest,
    Key,
    Manifest,
    PendingReleasesManifest,
    QuarantineInventory,
    RetainedHistoryExtensionPayload,
    SegmentMetadata,
    SnapshotEnvelope,
    /// The checkpoint's durable-base branch set section payload (kind 4).
    SnapshotFlushedBranchesPayload,
    SnapshotRowPayload,
    SnapshotTimelinePayload,
    StorageRow,
    TableArtifact,
    TableBlock,
    /// W2.6 (B1): the trusted (no-CRC) data-block frame decode.
    TableBlockTrusted,
    /// W2.3 (B3): the indexed point seek over a (payload, offsets) split.
    TableBlockIndexedSeek,
    TableManifest,
    WalCommitPayload,
    WalRecord,
    WalSegmentHeader,
    Watermark,
}

/// Result of routing arbitrary bytes through a durable format decoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormatDecodeOutcome {
    Accepted,
    Rejected,
}

/// Routes arbitrary bytes through one storage durable format decoder.
///
/// This function is intentionally exposed only through the hidden testkit
/// feature. It exists for fuzz targets and conformance probes, not as a
/// production format API.
pub fn decode_format_bytes(decoder: FormatDecoder, bytes: &[u8]) -> FormatDecodeOutcome {
    let accepted = match decoder {
        FormatDecoder::BranchCatalogManifest => fuzzing::decode_branch_catalog_manifest(bytes),
        FormatDecoder::Key => fuzzing::decode_key(bytes),
        FormatDecoder::Manifest => fuzzing::decode_manifest(bytes),
        FormatDecoder::PendingReleasesManifest => fuzzing::decode_pending_releases_manifest(bytes),
        FormatDecoder::QuarantineInventory => fuzzing::decode_quarantine_inventory(bytes),
        FormatDecoder::RetainedHistoryExtensionPayload => {
            fuzzing::decode_retained_history_extension_payload(bytes)
        }
        FormatDecoder::SegmentMetadata => fuzzing::decode_segment_metadata(bytes),
        FormatDecoder::SnapshotEnvelope => fuzzing::decode_snapshot_envelope(bytes),
        FormatDecoder::SnapshotFlushedBranchesPayload => {
            fuzzing::decode_snapshot_flushed_branches_payload(bytes)
        }
        FormatDecoder::SnapshotRowPayload => fuzzing::decode_snapshot_row_payload(bytes),
        FormatDecoder::SnapshotTimelinePayload => fuzzing::decode_snapshot_timeline_payload(bytes),
        FormatDecoder::StorageRow => fuzzing::decode_storage_row(bytes),
        FormatDecoder::TableArtifact => fuzzing::decode_table_artifact(bytes),
        FormatDecoder::TableBlock => fuzzing::decode_table_block(bytes),
        FormatDecoder::TableBlockTrusted => fuzzing::decode_table_block_trusted(bytes),
        FormatDecoder::TableBlockIndexedSeek => fuzzing::decode_table_block_indexed_seek(bytes),
        FormatDecoder::TableManifest => fuzzing::decode_table_manifest(bytes),
        FormatDecoder::WalCommitPayload => fuzzing::decode_wal_commit_payload(bytes),
        FormatDecoder::WalRecord => fuzzing::decode_wal_record(bytes),
        FormatDecoder::WalSegmentHeader => fuzzing::decode_wal_segment_header(bytes),
        FormatDecoder::Watermark => fuzzing::decode_watermark(bytes),
    };

    if accepted {
        FormatDecodeOutcome::Accepted
    } else {
        FormatDecodeOutcome::Rejected
    }
}

/// Runs one deterministic generated-table model case through table-format bytes.
///
/// The input is interpreted as a bounded script, not as durable bytes. This is
/// the integration-test and fuzz-adjacent route for checking generated table
/// artifacts without making row or table codec internals public production API.
pub fn check_table_format_model_script(script: &[u8]) -> Result<(), TestkitError> {
    let rows = table_model_rows(script);
    let split = if rows.len() == 1 {
        1
    } else {
        (rows.len() / 3).max(1)
    };

    for (rows_per_block, compression) in [
        (rows.len(), TableCompression::Uncompressed),
        (split, TableCompression::Uncompressed),
        (rows.len(), TableCompression::Zstd),
        (split, TableCompression::Zstd),
    ] {
        let bytes = encode_immutable_table(&rows, 4096, rows_per_block, compression)
            .map_err(|err| TestkitError::new(format!("encode generated table: {err}")))?;
        let decoded = decode_immutable_table(&bytes)
            .map_err(|err| TestkitError::new(format!("decode generated table: {err}")))?;
        if decoded.rows() != rows.as_slice() {
            return Err(TestkitError::new(
                "decoded rows drifted from generated model",
            ));
        }
        if decoded.header().row_count() != rows.len() as u64 {
            return Err(TestkitError::new("decoded table row count drifted"));
        }
        let expected_block_count = rows.chunks(rows_per_block).count();
        if decoded.header().data_block_count() as usize != expected_block_count
            || decoded.data_blocks().len() != expected_block_count
            || decoded.index().entry_count() != expected_block_count
        {
            return Err(TestkitError::new(
                "decoded table index/data-block count drifted",
            ));
        }
        if decoded.properties().row_count() != rows.len() as u64
            || decoded.properties().data_block_count() as usize != decoded.data_blocks().len()
        {
            return Err(TestkitError::new("decoded table properties drifted"));
        }
        let expected_commit_min = rows
            .iter()
            .map(StorageRow::commit_version)
            .min()
            .expect("generated rows are nonempty");
        let expected_commit_max = rows
            .iter()
            .map(StorageRow::commit_version)
            .max()
            .expect("generated rows are nonempty");
        let expected_min_key = table_model_internal_key(rows.first().expect("first row"));
        let expected_max_key = table_model_internal_key(rows.last().expect("last row"));
        if decoded.properties().commit_min() != expected_commit_min
            || decoded.properties().commit_max() != expected_commit_max
            || decoded.properties().min_key() != &expected_min_key
            || decoded.properties().max_key() != &expected_max_key
        {
            return Err(TestkitError::new(
                "decoded table property key or commit range drifted",
            ));
        }

        for (index_entry, expected_rows) in decoded
            .index()
            .entries()
            .iter()
            .zip(rows.chunks(rows_per_block))
        {
            let first_key = table_model_internal_key(&expected_rows[0]);
            let last_key = table_model_internal_key(&expected_rows[expected_rows.len() - 1]);
            if index_entry.first_key() != &first_key
                || index_entry.last_key() != &last_key
                || index_entry.row_count() as usize != expected_rows.len()
            {
                return Err(TestkitError::new("decoded table index key range drifted"));
            }
        }
    }

    Ok(())
}

fn table_model_rows(script: &[u8]) -> Vec<StorageRow> {
    let row_count = script
        .first()
        .map_or(1, |byte| usize::from(byte % 128).saturating_add(1));
    (0..row_count)
        .map(|index| {
            let control = script_byte(script, index * 4 + 1);
            let value_len = usize::from(script_byte(script, index * 4 + 2))
                + usize::from(script_byte(script, index * 4 + 3));
            let value_len = value_len.min(512);
            let value = vec![script_byte(script, index * 4 + 4); value_len];
            let version = u64::try_from(row_count - index).expect("bounded row count");
            let user_key = if index <= 1 && row_count > 1 {
                b"0000:duplicate".to_vec()
            } else {
                let mut key = format!("{index:04}:").into_bytes();
                key.push(control);
                key
            };
            table_model_row(user_key, version, control & 0x01 == 1, value)
        })
        .collect()
}

fn table_model_row(user_key: Vec<u8>, version: u64, tombstone: bool, value: Vec<u8>) -> StorageRow {
    let key = PhysicalKey::new(
        BranchId::from_bytes([0x44; BranchId::BYTE_LEN]),
        "default",
        StorageSpaceId::engine(0x20).expect("engine id"),
        user_key,
    )
    .expect("physical key");
    let version = CommitVersion::new(version);
    let timestamp = Timestamp::from_micros(1_700_000_000_000_000 + version.as_u64());
    if tombstone {
        StorageRow::tombstone(key, version, timestamp)
    } else {
        StorageRow::put(key, version, timestamp, Timestamp::EPOCH, value)
    }
}

fn table_model_internal_key(row: &StorageRow) -> InternalKey {
    InternalKey::new(row.physical_key().clone(), row.commit_version())
}

fn script_byte(script: &[u8], index: usize) -> u8 {
    script.get(index).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        check_table_format_model_script, decode_format_bytes, FormatDecodeOutcome, FormatDecoder,
    };

    #[test]
    fn format_fuzz_decoders_reject_empty_input_without_panicking() {
        for decoder in [
            FormatDecoder::BranchCatalogManifest,
            FormatDecoder::Key,
            FormatDecoder::Manifest,
            FormatDecoder::PendingReleasesManifest,
            FormatDecoder::QuarantineInventory,
            FormatDecoder::RetainedHistoryExtensionPayload,
            FormatDecoder::SegmentMetadata,
            FormatDecoder::SnapshotEnvelope,
            FormatDecoder::SnapshotRowPayload,
            FormatDecoder::SnapshotTimelinePayload,
            FormatDecoder::StorageRow,
            FormatDecoder::TableArtifact,
            FormatDecoder::TableBlock,
            FormatDecoder::TableBlockTrusted,
            FormatDecoder::TableBlockIndexedSeek,
            FormatDecoder::TableManifest,
            FormatDecoder::WalCommitPayload,
            FormatDecoder::WalRecord,
            FormatDecoder::WalSegmentHeader,
            FormatDecoder::Watermark,
        ] {
            assert_eq!(
                decode_format_bytes(decoder, &[]),
                FormatDecodeOutcome::Rejected
            );
        }
    }

    #[test]
    fn format_fuzz_decoders_accept_valid_table_seed_bytes() {
        for (decoder, bytes) in [
            (
                FormatDecoder::BranchCatalogManifest,
                include_bytes!("../../fuzz/corpus/format_branch_catalog_manifest/valid-empty")
                    .as_slice(),
            ),
            (
                FormatDecoder::BranchCatalogManifest,
                include_bytes!(
                    "../../fuzz/corpus/format_branch_catalog_manifest/valid-single-active"
                )
                .as_slice(),
            ),
            (
                FormatDecoder::BranchCatalogManifest,
                include_bytes!(
                    "../../fuzz/corpus/format_branch_catalog_manifest/valid-active-and-deleted"
                )
                .as_slice(),
            ),
            (
                FormatDecoder::BranchCatalogManifest,
                include_bytes!(
                    "../../fuzz/corpus/format_branch_catalog_manifest/valid-with-parent"
                )
                .as_slice(),
            ),
            (
                FormatDecoder::PendingReleasesManifest,
                include_bytes!("../../fuzz/corpus/format_pending_releases_manifest/valid-empty")
                    .as_slice(),
            ),
            (
                FormatDecoder::PendingReleasesManifest,
                include_bytes!("../../fuzz/corpus/format_pending_releases_manifest/valid-single")
                    .as_slice(),
            ),
            (
                FormatDecoder::PendingReleasesManifest,
                include_bytes!("../../fuzz/corpus/format_pending_releases_manifest/valid-multi")
                    .as_slice(),
            ),
            (
                FormatDecoder::RetainedHistoryExtensionPayload,
                include_bytes!(
                    "../../fuzz/corpus/format_retained_history_extension/valid-no-timestamp"
                )
                .as_slice(),
            ),
            (
                FormatDecoder::SnapshotRowPayload,
                include_bytes!("../../fuzz/corpus/format_snapshot_row_payload/valid-empty")
                    .as_slice(),
            ),
            (
                FormatDecoder::SnapshotTimelinePayload,
                include_bytes!("../../fuzz/corpus/format_snapshot_timeline_payload/valid-empty")
                    .as_slice(),
            ),
        ] {
            assert_eq!(
                decode_format_bytes(decoder, bytes),
                FormatDecodeOutcome::Accepted,
                "{decoder:?} should accept its valid seed"
            );
        }
    }

    #[test]
    fn table_format_model_script_checks_generated_artifacts() {
        check_table_format_model_script(&[3, 0, 5, 7, 9, 1, 2, 3, 4])
            .expect("generated table model");
    }
}
