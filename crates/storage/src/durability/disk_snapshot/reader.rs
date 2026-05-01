//! Snapshot reader for recovery
//!
//! Loads and validates snapshot files for database recovery.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use crate::durability::codec::{CodecError, StorageCodec};
use crate::durability::format::primitive_tags;
use crate::durability::format::snapshot::{
    SectionHeader, SnapshotHeader, MIN_SUPPORTED_SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FORMAT_VERSION,
    SNAPSHOT_HEADER_SIZE, SNAPSHOT_MAGIC,
};

/// Snapshot reader for recovery
pub struct SnapshotReader {
    codec: Box<dyn StorageCodec>,
}

impl SnapshotReader {
    /// Create a new snapshot reader with the given codec
    pub fn new(codec: Box<dyn StorageCodec>) -> Self {
        SnapshotReader { codec }
    }

    /// Load a snapshot from file
    ///
    /// Validates magic bytes, format version, and codec ID.
    /// Returns the loaded snapshot data with all sections.
    pub fn load(&self, path: &Path) -> Result<LoadedSnapshot, SnapshotReadError> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        // Minimum size: header + codec ID (at least 1 byte) + CRC32 (4 bytes)
        if file_size < SNAPSHOT_HEADER_SIZE + 1 + 4 {
            return Err(SnapshotReadError::FileTooSmall { size: file_size });
        }

        let mut reader = BufReader::new(file);

        // Read and validate header
        let mut header_bytes = [0u8; SNAPSHOT_HEADER_SIZE];
        reader.read_exact(&mut header_bytes)?;

        let header =
            SnapshotHeader::from_bytes(&header_bytes).ok_or(SnapshotReadError::InvalidHeader)?;

        // Validate magic
        if header.magic != SNAPSHOT_MAGIC {
            return Err(SnapshotReadError::InvalidMagic {
                expected: SNAPSHOT_MAGIC,
                actual: header.magic,
            });
        }

        // Legacy-format detection runs before the generic header validation
        // so a pre-v2 snapshot produces the typed `LegacyFormat` diagnostic
        // the operator can act on, rather than collapsing into a generic
        // `HeaderValidation` string (parity with WAL).
        if header.format_version < MIN_SUPPORTED_SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotReadError::LegacyFormat {
                detected_version: header.format_version,
                supported_range: format!(
                    "this build requires snapshot format version {MIN_SUPPORTED_SNAPSHOT_FORMAT_VERSION}"
                ),
                remediation: "delete the offending `snap-*.chk` file and reopen \
                              — the database will replay from the next snapshot \
                              or the WAL tail."
                    .to_string(),
            });
        }

        // Validate header (future versions surface as `HeaderValidation`).
        header
            .validate()
            .map_err(|e| SnapshotReadError::HeaderValidation(e.to_string()))?;
        // Belt-and-suspenders: the only path that leaves format_version
        // untouched is equality with the current one.
        debug_assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);

        // Read codec ID
        let codec_id_len = header.codec_id_len as usize;
        let mut codec_id_bytes = vec![0u8; codec_id_len];
        reader.read_exact(&mut codec_id_bytes)?;
        let codec_id =
            String::from_utf8(codec_id_bytes).map_err(|_| SnapshotReadError::InvalidCodecId)?;

        // Validate codec ID matches
        if codec_id != self.codec.codec_id() {
            return Err(SnapshotReadError::CodecMismatch {
                expected: codec_id.clone(),
                actual: self.codec.codec_id().to_string(),
            });
        }

        // Read all remaining data for CRC validation
        let mut remaining_data = Vec::new();
        reader.read_to_end(&mut remaining_data)?;

        // Validate CRC (last 4 bytes)
        if remaining_data.len() < 4 {
            return Err(SnapshotReadError::FileTooSmall { size: file_size });
        }

        let stored_crc = u32::from_le_bytes(
            remaining_data[remaining_data.len() - 4..]
                .try_into()
                .unwrap(),
        );

        // Compute CRC of header + codec_id + sections
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_bytes);
        hasher.update(codec_id.as_bytes());
        hasher.update(&remaining_data[..remaining_data.len() - 4]);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(SnapshotReadError::CrcMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        // Parse sections
        let sections = self.parse_sections(&remaining_data[..remaining_data.len() - 4])?;

        Ok(LoadedSnapshot {
            header,
            codec_id,
            sections,
            crc: stored_crc,
        })
    }

    /// Parse sections from the data blob
    fn parse_sections(&self, data: &[u8]) -> Result<Vec<LoadedSection>, SnapshotReadError> {
        let mut sections = Vec::new();
        let mut cursor = 0;

        while cursor < data.len() {
            // Check if we have enough bytes for section header
            if cursor + SectionHeader::SIZE > data.len() {
                // Might be at the end with no more sections
                break;
            }

            let section_header_bytes: [u8; SectionHeader::SIZE] = data
                [cursor..cursor + SectionHeader::SIZE]
                .try_into()
                .unwrap();
            let section_header = SectionHeader::from_bytes(&section_header_bytes);
            cursor += SectionHeader::SIZE;

            // Validate primitive type
            if !primitive_tags::ALL_TAGS.contains(&section_header.primitive_type) {
                return Err(SnapshotReadError::InvalidPrimitiveType {
                    tag: section_header.primitive_type,
                });
            }

            // Check if we have enough data for the section
            let data_len = section_header.data_len as usize;
            if cursor + data_len > data.len() {
                return Err(SnapshotReadError::SectionDataTruncated {
                    primitive_type: section_header.primitive_type,
                    expected: data_len,
                    available: data.len() - cursor,
                });
            }

            let section_data = data[cursor..cursor + data_len].to_vec();
            cursor += data_len;

            sections.push(LoadedSection {
                primitive_type: section_header.primitive_type,
                data: section_data,
            });
        }

        Ok(sections)
    }

    /// Get the codec used by this reader
    #[cfg(test)]
    pub fn codec(&self) -> &dyn StorageCodec {
        self.codec.as_ref()
    }
}

/// Loaded snapshot data
#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    /// Snapshot header
    pub header: SnapshotHeader,
    /// Codec ID string
    pub codec_id: String,
    /// Loaded sections
    pub sections: Vec<LoadedSection>,
    /// CRC32 checksum
    pub crc: u32,
}

impl LoadedSnapshot {
    /// Get the snapshot ID
    pub fn snapshot_id(&self) -> u64 {
        self.header.snapshot_id
    }

    /// Get the watermark transaction ID
    pub fn watermark_txn(&self) -> u64 {
        self.header.watermark_txn
    }

    /// Get the creation timestamp
    #[cfg(test)]
    pub fn created_at(&self) -> u64 {
        self.header.created_at
    }

    /// Get the database UUID
    #[cfg(test)]
    pub fn database_uuid(&self) -> [u8; 16] {
        self.header.database_uuid
    }

    /// Find a section by primitive type
    #[cfg(test)]
    pub fn find_section(&self, primitive_type: u8) -> Option<&LoadedSection> {
        self.sections
            .iter()
            .find(|s| s.primitive_type == primitive_type)
    }

    /// Get all section types present
    #[cfg(test)]
    pub fn section_types(&self) -> Vec<u8> {
        self.sections.iter().map(|s| s.primitive_type).collect()
    }
}

/// Loaded section from snapshot
#[derive(Debug, Clone)]
pub struct LoadedSection {
    /// Primitive type tag
    pub primitive_type: u8,
    /// Section data (serialized primitive entries)
    pub data: Vec<u8>,
}

impl LoadedSection {
    /// Get the primitive type name
    #[cfg(test)]
    pub fn primitive_name(&self) -> &'static str {
        primitive_tags::tag_name(self.primitive_type)
    }
}

/// Errors that can occur when reading a snapshot
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SnapshotReadError {
    /// File is too small to be a valid snapshot
    #[error("Snapshot file too small: {size} bytes")]
    FileTooSmall {
        /// Actual file size
        size: usize,
    },
    /// Invalid snapshot header
    #[error("Invalid snapshot header")]
    InvalidHeader,
    /// Invalid magic bytes
    #[error("Invalid magic bytes: expected {expected:?}, got {actual:?}")]
    InvalidMagic {
        /// Expected magic bytes
        expected: [u8; 4],
        /// Actual magic bytes
        actual: [u8; 4],
    },
    /// Header validation failed
    #[error("Header validation failed: {0}")]
    HeaderValidation(String),
    /// Legacy snapshot format — rejected hard, even under lossy recovery.
    ///
    /// Produced when the snapshot's `format_version` is older than
    /// [`crate::durability::format::snapshot::MIN_SUPPORTED_SNAPSHOT_FORMAT_VERSION`].
    /// Surfaces unconditionally (strict and lossy open alike); the engine's
    /// open path re-raises as
    /// [`strata_engine::StrataError::LegacyFormat`] and the operator must
    /// delete the file manually before reopening. Mirrors the WAL
    /// `SegmentHeaderError::LegacyFormat` contract.
    #[error(
        "legacy snapshot format: found version {detected_version}. {supported_range}. {remediation}"
    )]
    LegacyFormat {
        /// Format version read from disk.
        detected_version: u32,
        /// Operator-facing description of the range this build accepts.
        supported_range: String,
        /// Operator remediation hint — filesystem action only.
        remediation: String,
    },
    /// Invalid codec ID
    #[error("Invalid codec ID (not valid UTF-8)")]
    InvalidCodecId,
    /// Codec mismatch
    #[error("Codec mismatch: snapshot uses '{expected}', reader uses '{actual}'")]
    CodecMismatch {
        /// Codec ID from snapshot
        expected: String,
        /// Codec ID from reader
        actual: String,
    },
    /// CRC mismatch
    #[error("CRC mismatch: stored={stored:#010x}, computed={computed:#010x}")]
    CrcMismatch {
        /// Stored CRC
        stored: u32,
        /// Computed CRC
        computed: u32,
    },
    /// Invalid primitive type
    #[error("Invalid primitive type tag: {tag:#04x}")]
    InvalidPrimitiveType {
        /// Invalid tag value
        tag: u8,
    },
    /// Section data truncated
    #[error("Section data truncated for primitive {primitive_type}: expected {expected} bytes, got {available}")]
    SectionDataTruncated {
        /// Primitive type
        primitive_type: u8,
        /// Expected data length
        expected: usize,
        /// Available data length
        available: usize,
    },
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Codec error
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::codec::IdentityCodec;
    use crate::durability::disk_snapshot::{SnapshotSection, SnapshotWriter};

    fn test_uuid() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    #[test]
    fn test_load_snapshot_basic() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Write a snapshot
        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![
            SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::EVENT, vec![0, 0, 0, 0]),
        ];

        let info = writer.create_snapshot(1, 100, sections).unwrap();

        // Read it back
        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let loaded = reader.load(&info.path).unwrap();

        assert_eq!(loaded.snapshot_id(), 1);
        assert_eq!(loaded.watermark_txn(), 100);
        assert_eq!(loaded.sections.len(), 2);
        assert_eq!(loaded.database_uuid(), test_uuid());
    }

    #[test]
    fn test_load_all_sections() {
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        // Create snapshot with all 6 primitive types
        let sections = vec![
            SnapshotSection::new(primitive_tags::KV, b"kv_data".to_vec()),
            SnapshotSection::new(primitive_tags::EVENT, b"event_data".to_vec()),
            SnapshotSection::new(primitive_tags::BRANCH, b"branch_data".to_vec()),
            SnapshotSection::new(primitive_tags::JSON, b"json_data".to_vec()),
            SnapshotSection::new(primitive_tags::VECTOR, b"vector_data".to_vec()),
            SnapshotSection::new(primitive_tags::GRAPH, b"graph_data".to_vec()),
        ];

        let info = writer.create_snapshot(1, 100, sections).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let loaded = reader.load(&info.path).unwrap();

        assert_eq!(loaded.sections.len(), 6);

        // Verify each section
        let kv = loaded.find_section(primitive_tags::KV).unwrap();
        assert_eq!(kv.data, b"kv_data");

        let event = loaded.find_section(primitive_tags::EVENT).unwrap();
        assert_eq!(event.data, b"event_data");

        let vector = loaded.find_section(primitive_tags::VECTOR).unwrap();
        assert_eq!(vector.data, b"vector_data");
    }

    #[test]
    fn test_crc_validation_investigate() {
        // This test investigates the file structure and corruption behavior
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])];
        let info = writer.create_snapshot(1, 100, sections).unwrap();

        let data = std::fs::read(&info.path).unwrap();

        // Document the file structure:
        // - Header: bytes 0-63 (64 bytes)
        // - Codec ID "identity": bytes 64-71 (8 bytes)
        // - Section header: bytes 72-80 (9 bytes)
        // - Section data: bytes 81-84 (4 bytes)
        // - CRC: bytes 85-88 (4 bytes)
        // Total: 89 bytes
        assert_eq!(data.len(), 89, "Expected file size");

        // Verify structure
        assert_eq!(&data[0..4], b"SNAP", "Magic bytes");
        assert_eq!(&data[64..72], b"identity", "Codec ID");

        // Test corrupting byte 70 (in codec ID "identity")
        // This should cause CodecMismatch, not CrcMismatch!
        let mut corrupted = data.clone();
        corrupted[70] ^= 0xFF; // Corrupts 'i' in "identity" -> makes it invalid UTF-8 or different codec
        std::fs::write(&info.path, &corrupted).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let result = reader.load(&info.path);

        // The original test expected CrcMismatch, but corrupting the codec ID
        // should cause either InvalidCodecId (if UTF-8 invalid) or CodecMismatch
        match &result {
            Err(SnapshotReadError::InvalidCodecId) => {
                // Expected - corrupted byte made invalid UTF-8
            }
            Err(SnapshotReadError::CodecMismatch { .. }) => {
                // Also valid - codec ID is valid UTF-8 but doesn't match
            }
            Err(SnapshotReadError::CrcMismatch { .. }) => {
                panic!("Got CrcMismatch but codec validation should have failed first");
            }
            other => {
                panic!("Unexpected result: {:?}", other);
            }
        }
    }

    #[test]
    fn test_crc_validation_section_data() {
        // Test CRC validation by corrupting section data (not codec ID)
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])];
        let info = writer.create_snapshot(1, 100, sections).unwrap();

        let mut data = std::fs::read(&info.path).unwrap();

        // Corrupt section data area (bytes 81-84), not the CRC (bytes 85-88)
        // This should trigger CRC mismatch
        data[82] ^= 0xFF;
        std::fs::write(&info.path, &data).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let result = reader.load(&info.path);

        assert!(
            matches!(result, Err(SnapshotReadError::CrcMismatch { .. })),
            "Expected CrcMismatch when corrupting section data, got: {:?}",
            result
        );
    }

    #[test]
    fn test_crc_validation_header() {
        // Test CRC validation by corrupting header data
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])];
        let info = writer.create_snapshot(1, 100, sections).unwrap();

        let mut data = std::fs::read(&info.path).unwrap();

        // Corrupt the watermark field (bytes 16-24 of header)
        // This should trigger CRC mismatch (header is included in CRC)
        data[20] ^= 0xFF;
        std::fs::write(&info.path, &data).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let result = reader.load(&info.path);

        assert!(
            matches!(result, Err(SnapshotReadError::CrcMismatch { .. })),
            "Expected CrcMismatch when corrupting header, got: {:?}",
            result
        );
    }

    #[test]
    fn test_invalid_magic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("bad.chk");

        // Create file with bad magic
        let mut data = vec![0u8; 100];
        data[0..4].copy_from_slice(b"BADM");
        std::fs::write(&path, &data).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let result = reader.load(&path);

        assert!(matches!(
            result,
            Err(SnapshotReadError::InvalidMagic { .. })
        ));
    }

    #[test]
    fn test_file_too_small() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("tiny.chk");

        // Create file that's too small
        std::fs::write(&path, b"SNAP").unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let result = reader.load(&path);

        assert!(matches!(
            result,
            Err(SnapshotReadError::FileTooSmall { .. })
        ));
    }

    #[test]
    fn test_snapshot_v1_rejected_as_legacy() {
        // Craft a 64-byte v1 header (magic + format_version=1 + enough
        // trailing body to clear the FileTooSmall guard). Parity with
        // the WAL legacy-format contract: pre-v2 snapshots surface as
        // `LegacyFormat`, not a generic `HeaderValidation`.
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("snap-000001.chk");

        let mut header = [0u8; SNAPSHOT_HEADER_SIZE];
        header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
        header[4..8].copy_from_slice(&1u32.to_le_bytes()); // format_version = 1
        header[8..16].copy_from_slice(&1u64.to_le_bytes()); // snapshot_id
        header[16..24].copy_from_slice(&100u64.to_le_bytes()); // watermark_txn
        header[24..32].copy_from_slice(&0u64.to_le_bytes()); // created_at
        header[32..48].copy_from_slice(&test_uuid()); // database_uuid
        header[48] = 0; // codec_id_len = 0 (empty codec id)

        // Pad body so the reader clears its minimum-size guard. The
        // contents after the header never get parsed because LegacyFormat
        // short-circuits — the filler bytes are arbitrary.
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(&[0u8; 8]); // filler

        std::fs::write(&path, &bytes).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        match reader.load(&path) {
            Err(SnapshotReadError::LegacyFormat {
                detected_version,
                supported_range,
                remediation,
            }) => {
                assert_eq!(detected_version, 1);
                assert!(
                    supported_range.contains(&MIN_SUPPORTED_SNAPSHOT_FORMAT_VERSION.to_string()),
                    "supported_range must name the required version, got: {supported_range}"
                );
                assert!(
                    remediation.contains("snap-"),
                    "remediation must name the snapshot file shape, got: {remediation}"
                );
            }
            other => panic!("expected LegacyFormat, got: {:?}", other),
        }
    }

    #[test]
    fn test_section_types() {
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![
            SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::VECTOR, vec![0, 0, 0, 0]),
        ];

        let info = writer.create_snapshot(1, 100, sections).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let loaded = reader.load(&info.path).unwrap();

        let types = loaded.section_types();
        assert_eq!(types, vec![primitive_tags::KV, primitive_tags::VECTOR]);
    }

    #[test]
    fn test_primitive_names() {
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![
            SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::EVENT, vec![0, 0, 0, 0]),
        ];

        let info = writer.create_snapshot(1, 100, sections).unwrap();

        let reader = SnapshotReader::new(Box::new(IdentityCodec));
        let loaded = reader.load(&info.path).unwrap();

        assert_eq!(loaded.sections[0].primitive_name(), "KV");
        assert_eq!(loaded.sections[1].primitive_name(), "Event");
    }
}
