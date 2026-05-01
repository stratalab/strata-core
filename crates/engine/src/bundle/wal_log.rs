//! WAL.branchlog reader and writer (v2 — TransactionPayload / msgpack)
//!
//! This module handles the binary format for branch-scoped transaction payloads
//! within a BranchBundle archive.
//!
//! ## Format (v2)
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Header (16 bytes)                                               │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ Magic: "STRATA_WAL" (10 bytes)                                  │
//! │ Version: u16 (2 bytes, LE) — must be 2                         │
//! │ Entry Count: u32 (4 bytes, LE)                                  │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ Entries (variable)                                              │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ For each entry:                                                 │
//! │   Length: u32 (4 bytes, LE)                                     │
//! │   Data: [u8; length] (msgpack-serialized BranchlogPayload)         │
//! │   CRC32: u32 (4 bytes, LE)                                      │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use super::error::{BranchBundleError, BranchBundleResult};
use super::types::{xxh3_hex, WAL_BRANCHLOG_MAGIC, WAL_BRANCHLOG_VERSION};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use strata_core::Value;
use strata_storage::Key;

/// Header size in bytes: magic (10) + version (2) + count (4)
const HEADER_SIZE: usize = 16;

// =============================================================================
// BranchlogPayload — the v2 record type
// =============================================================================

/// A single committed transaction's data for inclusion in a branch bundle.
///
/// This is the v2 replacement for the legacy `WALEntry`-based format.
/// Each `BranchlogPayload` represents one committed transaction with its
/// puts and deletes. Transaction framing (BeginTxn/CommitTxn) is not
/// needed because bundles only contain committed data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BranchlogPayload {
    /// The branch this payload belongs to
    pub branch_id: String,
    /// Commit version of this transaction
    pub version: u64,
    /// Key-value pairs written in this transaction
    pub puts: Vec<(Key, Value)>,
    /// Keys deleted in this transaction
    pub deletes: Vec<Key>,
}

impl BranchlogPayload {
    /// Serialize to MessagePack bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("BranchlogPayload serialization should not fail")
    }

    /// Deserialize from MessagePack bytes.
    pub fn from_bytes(bytes: &[u8]) -> BranchBundleResult<Self> {
        rmp_serde::from_slice(bytes).map_err(|e| {
            BranchBundleError::serialization(format!("msgpack decode BranchlogPayload: {}", e))
        })
    }
}

// =============================================================================
// Writer
// =============================================================================

/// Information about written WAL log
#[derive(Debug, Clone)]
pub struct WalLogInfo {
    /// Number of entries written
    pub entry_count: u64,
    /// Total bytes written (including header and per-entry overhead)
    pub bytes_written: u64,
    /// xxh3 checksum of the entire file content
    pub checksum: String,
}

/// Writer for WAL.branchlog format (v2)
pub struct WalLogWriter;

impl WalLogWriter {
    /// Write payloads to a writer in .branchlog v2 format
    ///
    /// Returns information about the written data including checksum.
    pub fn write<W: Write>(
        payloads: &[BranchlogPayload],
        mut writer: W,
    ) -> BranchBundleResult<WalLogInfo> {
        let mut buffer = Vec::new();

        // Write header
        buffer.extend_from_slice(WAL_BRANCHLOG_MAGIC);
        buffer.extend_from_slice(&WAL_BRANCHLOG_VERSION.to_le_bytes());
        buffer.extend_from_slice(&(payloads.len() as u32).to_le_bytes());

        // Write entries
        for payload in payloads {
            // Serialize entry with msgpack
            let entry_data = payload.to_bytes();

            // Calculate CRC32 of entry data
            let crc = crc32fast::hash(&entry_data);

            // Write: length + data + crc
            buffer.extend_from_slice(&(entry_data.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&entry_data);
            buffer.extend_from_slice(&crc.to_le_bytes());
        }

        // Calculate checksum of entire buffer
        let checksum = xxh3_hex(&buffer);

        // Write to output
        let bytes_written = buffer.len() as u64;
        writer.write_all(&buffer).map_err(BranchBundleError::from)?;

        Ok(WalLogInfo {
            entry_count: payloads.len() as u64,
            bytes_written,
            checksum,
        })
    }

    /// Write payloads to a Vec<u8>
    ///
    /// Convenience method for testing and in-memory operations.
    pub fn write_to_vec(
        payloads: &[BranchlogPayload],
    ) -> BranchBundleResult<(Vec<u8>, WalLogInfo)> {
        let mut buffer = Vec::new();
        let info = Self::write(payloads, &mut buffer)?;
        Ok((buffer, info))
    }
}

// =============================================================================
// Reader
// =============================================================================

/// Reader for WAL.branchlog format (v2)
pub struct WalLogReader;

impl WalLogReader {
    /// Read all payloads from a reader
    ///
    /// Validates the header and CRC32 of each entry.
    pub fn read<R: Read>(mut reader: R) -> BranchBundleResult<Vec<BranchlogPayload>> {
        // Read header
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| {
            BranchBundleError::invalid_bundle(format!("failed to read header: {}", e))
        })?;

        // Validate magic
        if &header[0..10] != WAL_BRANCHLOG_MAGIC {
            return Err(BranchBundleError::invalid_bundle(format!(
                "invalid magic: expected {:?}, got {:?}",
                WAL_BRANCHLOG_MAGIC,
                &header[0..10]
            )));
        }

        // Parse version
        let version = u16::from_le_bytes([header[10], header[11]]);
        if version != WAL_BRANCHLOG_VERSION {
            return Err(BranchBundleError::UnsupportedVersion {
                version: version as u32,
            });
        }

        // Parse entry count
        let entry_count = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        // Read entries
        let mut payloads = Vec::with_capacity(entry_count as usize);
        for i in 0..entry_count {
            payloads.push(read_single_entry(&mut reader, i as usize)?);
        }

        Ok(payloads)
    }

    /// Read payloads from a byte slice
    ///
    /// Convenience method for testing and in-memory operations.
    pub fn read_from_slice(data: &[u8]) -> BranchBundleResult<Vec<BranchlogPayload>> {
        Self::read(std::io::Cursor::new(data))
    }

    /// Validate a WAL.branchlog without fully parsing entries
    ///
    /// Checks header and entry CRCs without deserializing entry contents.
    /// Returns the entry count if valid.
    pub fn validate<R: Read>(mut reader: R) -> BranchBundleResult<u32> {
        // Read header
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| {
            BranchBundleError::invalid_bundle(format!("failed to read header: {}", e))
        })?;

        // Validate magic
        if &header[0..10] != WAL_BRANCHLOG_MAGIC {
            return Err(BranchBundleError::invalid_bundle("invalid magic"));
        }

        // Parse version
        let version = u16::from_le_bytes([header[10], header[11]]);
        if version != WAL_BRANCHLOG_VERSION {
            return Err(BranchBundleError::UnsupportedVersion {
                version: version as u32,
            });
        }

        // Parse entry count
        let entry_count = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        // Validate each entry's CRC without deserializing
        for i in 0..entry_count {
            validate_single_entry(&mut reader, i as usize)?;
        }

        Ok(entry_count)
    }

    /// Get header info without reading entries
    ///
    /// Returns (version, entry_count) if header is valid.
    pub fn read_header<R: Read>(mut reader: R) -> BranchBundleResult<(u16, u32)> {
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| {
            BranchBundleError::invalid_bundle(format!("failed to read header: {}", e))
        })?;

        // Validate magic
        if &header[0..10] != WAL_BRANCHLOG_MAGIC {
            return Err(BranchBundleError::invalid_bundle("invalid magic"));
        }

        let version = u16::from_le_bytes([header[10], header[11]]);
        let entry_count = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        Ok((version, entry_count))
    }
}

// =============================================================================
// Streaming Reader (for large files)
// =============================================================================

/// Iterator over BranchlogPayload entries for streaming reads
pub struct WalLogIterator<R: Read> {
    reader: R,
    remaining: u32,
    index: usize,
}

impl<R: Read> WalLogIterator<R> {
    /// Create a new iterator from a reader
    ///
    /// Reads and validates the header, then returns an iterator over entries.
    pub fn new(mut reader: R) -> BranchBundleResult<Self> {
        let (version, entry_count) = WalLogReader::read_header(&mut reader)?;

        if version != WAL_BRANCHLOG_VERSION {
            return Err(BranchBundleError::UnsupportedVersion {
                version: version as u32,
            });
        }

        Ok(Self {
            reader,
            remaining: entry_count,
            index: 0,
        })
    }

    /// Get the total number of entries
    pub fn entry_count(&self) -> u32 {
        self.remaining + self.index as u32
    }
}

impl<R: Read> Iterator for WalLogIterator<R> {
    type Item = BranchBundleResult<BranchlogPayload>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let result = read_single_entry(&mut self.reader, self.index);
        self.remaining -= 1;
        self.index += 1;

        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining as usize, Some(self.remaining as usize))
    }
}

// =============================================================================
// Internal helpers
// =============================================================================

/// Read a single entry from a reader (length + data + CRC)
fn read_single_entry<R: Read>(
    reader: &mut R,
    index: usize,
) -> BranchBundleResult<BranchlogPayload> {
    // Read length
    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .map_err(|e| BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("failed to read length: {}", e),
        })?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    // Sanity check
    if len > 100 * 1024 * 1024 {
        return Err(BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("entry length {} exceeds maximum", len),
        });
    }

    // Read data
    let mut data = vec![0u8; len];
    reader
        .read_exact(&mut data)
        .map_err(|e| BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("failed to read data: {}", e),
        })?;

    // Read and verify CRC
    let mut crc_bytes = [0u8; 4];
    reader
        .read_exact(&mut crc_bytes)
        .map_err(|e| BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("failed to read crc: {}", e),
        })?;
    let expected_crc = u32::from_le_bytes(crc_bytes);
    let actual_crc = crc32fast::hash(&data);

    if expected_crc != actual_crc {
        return Err(BranchBundleError::InvalidWalEntry {
            index,
            reason: format!(
                "CRC mismatch: expected {:08x}, got {:08x}",
                expected_crc, actual_crc
            ),
        });
    }

    // Deserialize
    BranchlogPayload::from_bytes(&data).map_err(|e| BranchBundleError::InvalidWalEntry {
        index,
        reason: format!("msgpack decode: {}", e),
    })
}

/// Validate a single entry's CRC without deserializing
fn validate_single_entry<R: Read>(reader: &mut R, index: usize) -> BranchBundleResult<()> {
    // Read length
    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .map_err(|e| BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("failed to read length: {}", e),
        })?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    // Sanity check
    if len > 100 * 1024 * 1024 {
        return Err(BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("entry length {} exceeds maximum", len),
        });
    }

    // Read entry data
    let mut data = vec![0u8; len];
    reader
        .read_exact(&mut data)
        .map_err(|e| BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("failed to read data: {}", e),
        })?;

    // Read and verify CRC32
    let mut crc_bytes = [0u8; 4];
    reader
        .read_exact(&mut crc_bytes)
        .map_err(|e| BranchBundleError::InvalidWalEntry {
            index,
            reason: format!("failed to read crc: {}", e),
        })?;
    let expected_crc = u32::from_le_bytes(crc_bytes);
    let actual_crc = crc32fast::hash(&data);

    if expected_crc != actual_crc {
        return Err(BranchBundleError::InvalidWalEntry {
            index,
            reason: format!(
                "CRC mismatch: expected {:08x}, got {:08x}",
                expected_crc, actual_crc
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::BranchId;
    use strata_core::Value;
    use strata_storage::{Key, Namespace, TypeTag};

    fn make_test_branch_id() -> (BranchId, String) {
        let branch_id = BranchId::new();
        let branch_id_str = branch_id.to_string();
        (branch_id, branch_id_str)
    }

    fn make_test_payloads(branch_id_str: &str) -> Vec<BranchlogPayload> {
        let branch_id = BranchId::from_string(branch_id_str).unwrap_or_default();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        vec![
            BranchlogPayload {
                branch_id: branch_id_str.to_string(),
                version: 1,
                puts: vec![
                    (
                        Key::new(ns.clone(), TypeTag::KV, b"key1".to_vec()),
                        Value::String("value1".to_string()),
                    ),
                    (
                        Key::new(ns.clone(), TypeTag::KV, b"key2".to_vec()),
                        Value::Int(42),
                    ),
                ],
                deletes: vec![],
            },
            BranchlogPayload {
                branch_id: branch_id_str.to_string(),
                version: 2,
                puts: vec![],
                deletes: vec![Key::new(ns, TypeTag::KV, b"key1".to_vec())],
            },
        ]
    }

    #[test]
    fn test_write_read_roundtrip() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        // Write
        let (data, info) = WalLogWriter::write_to_vec(&payloads).unwrap();

        assert_eq!(info.entry_count, 2);
        assert!(info.bytes_written > HEADER_SIZE as u64);
        assert!(!info.checksum.is_empty());

        // Read
        let read_payloads = WalLogReader::read_from_slice(&data).unwrap();

        assert_eq!(read_payloads.len(), payloads.len());
        for (original, read) in payloads.iter().zip(read_payloads.iter()) {
            assert_eq!(original, read);
        }
    }

    #[test]
    fn test_empty_entries() {
        let payloads: Vec<BranchlogPayload> = vec![];

        let (data, info) = WalLogWriter::write_to_vec(&payloads).unwrap();

        assert_eq!(info.entry_count, 0);
        assert_eq!(info.bytes_written, HEADER_SIZE as u64);

        let read_payloads = WalLogReader::read_from_slice(&data).unwrap();
        assert!(read_payloads.is_empty());
    }

    #[test]
    fn test_validate_only() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        let (data, _) = WalLogWriter::write_to_vec(&payloads).unwrap();

        // Validate without full parse
        let count = WalLogReader::validate(std::io::Cursor::new(&data)).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_read_header() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        let (data, _) = WalLogWriter::write_to_vec(&payloads).unwrap();

        let (version, count) = WalLogReader::read_header(std::io::Cursor::new(&data)).unwrap();
        assert_eq!(version, WAL_BRANCHLOG_VERSION);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_invalid_magic() {
        let mut data = vec![0u8; HEADER_SIZE];
        data[0..10].copy_from_slice(b"WRONG_MAGI");

        let result = WalLogReader::read_from_slice(&data);
        assert!(matches!(result, Err(BranchBundleError::InvalidBundle(_))));
    }

    #[test]
    fn test_corrupted_entry_crc() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        let (mut data, _) = WalLogWriter::write_to_vec(&payloads).unwrap();

        // Corrupt a byte in the first entry's data
        if data.len() > HEADER_SIZE + 10 {
            data[HEADER_SIZE + 10] ^= 0xFF;
        }

        let result = WalLogReader::read_from_slice(&data);
        assert!(matches!(
            result,
            Err(BranchBundleError::InvalidWalEntry { index: 0, .. })
        ));
    }

    #[test]
    fn test_streaming_iterator() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        let (data, _) = WalLogWriter::write_to_vec(&payloads).unwrap();

        let iter = WalLogIterator::new(std::io::Cursor::new(&data)).unwrap();
        assert_eq!(iter.entry_count(), 2);

        let read_payloads: Vec<BranchlogPayload> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(read_payloads.len(), payloads.len());
    }

    #[test]
    fn test_checksum_consistency() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        // Write twice, checksums should match
        let (data1, info1) = WalLogWriter::write_to_vec(&payloads).unwrap();
        let (data2, info2) = WalLogWriter::write_to_vec(&payloads).unwrap();

        assert_eq!(info1.checksum, info2.checksum);
        assert_eq!(data1, data2);
    }

    #[test]
    fn test_large_entry() {
        let branch_id = BranchId::new();
        let branch_id_str = branch_id.to_string();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Create payload with large value
        let large_value = "x".repeat(1024 * 1024); // 1MB string
        let payloads = vec![BranchlogPayload {
            branch_id: branch_id_str,
            version: 1,
            puts: vec![(
                Key::new(ns, TypeTag::KV, b"large_key".to_vec()),
                Value::String(large_value),
            )],
            deletes: vec![],
        }];

        let (data, info) = WalLogWriter::write_to_vec(&payloads).unwrap();
        assert!(info.bytes_written > 1024 * 1024);

        let read_payloads = WalLogReader::read_from_slice(&data).unwrap();
        assert_eq!(read_payloads.len(), 1);
        assert_eq!(payloads[0], read_payloads[0]);
    }

    // ========================================================================
    // Adversarial WAL.branchlog Tests
    // ========================================================================

    #[test]
    fn test_truncated_header() {
        // Only 5 bytes (less than HEADER_SIZE)
        let data = vec![0u8; 5];
        let result = WalLogReader::read_from_slice(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_version() {
        let mut data = vec![0u8; HEADER_SIZE];
        data[0..10].copy_from_slice(WAL_BRANCHLOG_MAGIC);
        // Set version to 999
        data[10..12].copy_from_slice(&999u16.to_le_bytes());
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // 0 entries

        let result = WalLogReader::read_from_slice(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_v1_version_rejected() {
        let mut data = vec![0u8; HEADER_SIZE];
        data[0..10].copy_from_slice(WAL_BRANCHLOG_MAGIC);
        // Set version to 1 (old format)
        data[10..12].copy_from_slice(&1u16.to_le_bytes());
        data[12..16].copy_from_slice(&0u32.to_le_bytes());

        let result = WalLogReader::read_from_slice(&data);
        assert!(matches!(
            result,
            Err(BranchBundleError::UnsupportedVersion { version: 1 })
        ));
    }

    #[test]
    fn test_entry_count_mismatch_more_declared() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        let (mut data, _) = WalLogWriter::write_to_vec(&payloads).unwrap();

        // Bump entry count to 999 (more than actual)
        data[12..16].copy_from_slice(&999u32.to_le_bytes());

        // Should fail when trying to read more entries than exist
        let result = WalLogReader::read_from_slice(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_iterator_stops_on_corruption() {
        let (_branch_id, branch_id_str) = make_test_branch_id();
        let payloads = make_test_payloads(&branch_id_str);

        let (mut data, _) = WalLogWriter::write_to_vec(&payloads).unwrap();

        // Corrupt a data byte in the second entry area
        let corrupt_pos = HEADER_SIZE + 30;
        if corrupt_pos < data.len() {
            data[corrupt_pos] ^= 0xFF;
        }

        let iter = WalLogIterator::new(std::io::Cursor::new(&data)).unwrap();
        let results: Vec<_> = iter.collect();

        // At least one entry should fail
        assert!(
            results.iter().any(|r| r.is_err()),
            "Iterator should encounter corruption"
        );
    }

    #[test]
    fn test_checksum_changes_with_different_data() {
        let branch_id = BranchId::new();
        let branch_id_str = branch_id.to_string();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        let payloads1 = vec![BranchlogPayload {
            branch_id: branch_id_str.clone(),
            version: 1,
            puts: vec![(
                Key::new(ns.clone(), TypeTag::KV, b"key1".to_vec()),
                Value::Int(1),
            )],
            deletes: vec![],
        }];

        let payloads2 = vec![BranchlogPayload {
            branch_id: branch_id_str,
            version: 1,
            puts: vec![(
                Key::new(ns, TypeTag::KV, b"key1".to_vec()),
                Value::Int(2), // Different value
            )],
            deletes: vec![],
        }];

        let (_, info1) = WalLogWriter::write_to_vec(&payloads1).unwrap();
        let (_, info2) = WalLogWriter::write_to_vec(&payloads2).unwrap();

        assert_ne!(
            info1.checksum, info2.checksum,
            "Different data should produce different checksums"
        );
    }

    #[test]
    fn test_payload_with_deletes() {
        let branch_id = BranchId::new();
        let branch_id_str = branch_id.to_string();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        let payloads = vec![BranchlogPayload {
            branch_id: branch_id_str,
            version: 5,
            puts: vec![(
                Key::new(ns.clone(), TypeTag::KV, b"kept".to_vec()),
                Value::String("still here".to_string()),
            )],
            deletes: vec![
                Key::new(ns.clone(), TypeTag::KV, b"removed1".to_vec()),
                Key::new(ns, TypeTag::KV, b"removed2".to_vec()),
            ],
        }];

        let (data, _) = WalLogWriter::write_to_vec(&payloads).unwrap();
        let read = WalLogReader::read_from_slice(&data).unwrap();

        assert_eq!(read.len(), 1);
        assert_eq!(read[0].puts.len(), 1);
        assert_eq!(read[0].deletes.len(), 2);
        assert_eq!(read[0].version, 5);
    }
}
