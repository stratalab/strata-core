//! Snapshot Writer and Serialization
//!
//! This module implements snapshot writing with:
//! - Per-primitive serialization trait
//! - SnapshotWriter with CRC32 checksums
//! - Atomic write (temp file + rename)
//!
//! ## Key Principle
//!
//! Snapshots are **physical** (materialized state), not **semantic** (history).
//! They compress WAL effects but are not the history itself.
//!
//! ## Usage
//!
//! ```text
//! let header = SnapshotHeader::new(wal_offset, tx_count);
//! let sections = vec![
//!     PrimitiveSection::new(primitive_ids::KV, kv_data),
//!     PrimitiveSection::new(primitive_ids::JSON, json_data),
//!     // ... other primitives
//! ];
//!
//! let mut writer = SnapshotWriter::new();
//! let info = writer.write_atomic(&header, &sections, path)?;
//! ```

use crate::snapshot_types::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use tracing::{debug, info, warn};

// ============================================================================
// Snapshot Serializable Trait
// ============================================================================

/// Trait for primitives to implement snapshot serialization
///
/// Used internally by `serialize_all_primitives` and `deserialize_primitives`
/// helper functions. The snapshot system's actual write path uses hardcoded
/// per-primitive serialization in `CheckpointCoordinator`.
pub trait SnapshotSerializable {
    /// Serialize primitive state for snapshot
    ///
    /// Returns the serialized bytes for this primitive's state.
    fn snapshot_serialize(&self) -> Result<Vec<u8>, SnapshotError>;

    /// Deserialize primitive state from snapshot
    ///
    /// Restores the primitive's state from serialized bytes.
    fn snapshot_deserialize(&mut self, data: &[u8]) -> Result<(), SnapshotError>;

    /// Primitive type ID (for snapshot sections)
    ///
    /// Returns the primitive_ids::* constant for this primitive.
    fn primitive_type_id(&self) -> u8;
}

// ============================================================================
// Snapshot Writer
// ============================================================================

/// Snapshot writer with CRC32 checksum support
///
/// Writes snapshots atomically using temp file + rename pattern.
pub struct SnapshotWriter {
    hasher: crc32fast::Hasher,
}

impl SnapshotWriter {
    /// Create a new snapshot writer
    pub fn new() -> Self {
        SnapshotWriter {
            hasher: crc32fast::Hasher::new(),
        }
    }

    /// Write snapshot to file
    ///
    /// Writes header, primitive sections, and CRC32 checksum.
    /// Does NOT use atomic write - call `write_atomic` for that.
    pub fn write(
        &mut self,
        header: &SnapshotHeader,
        sections: &[PrimitiveSection],
        path: &Path,
    ) -> Result<SnapshotInfo, SnapshotError> {
        debug!(target: "strata::snapshot", path = %path.display(), "Writing snapshot");

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut file = File::create(path)?;
        self.hasher = crc32fast::Hasher::new();

        // Write header
        let header_bytes = header.to_bytes();
        file.write_all(&header_bytes)?;
        self.hasher.update(&header_bytes);

        // Write primitive count
        let count = sections.len() as u8;
        file.write_all(&[count])?;
        self.hasher.update(&[count]);

        // Write each section
        for section in sections {
            // Type (1 byte)
            file.write_all(&[section.primitive_type])?;
            self.hasher.update(&[section.primitive_type]);

            // Length (8 bytes)
            let len_bytes = (section.data.len() as u64).to_le_bytes();
            file.write_all(&len_bytes)?;
            self.hasher.update(&len_bytes);

            // Data
            file.write_all(&section.data)?;
            self.hasher.update(&section.data);
        }

        // Write CRC32
        let checksum = self.hasher.clone().finalize();
        file.write_all(&checksum.to_le_bytes())?;

        // Sync to disk
        file.sync_all()?;

        let size_bytes = std::fs::metadata(path)?.len();

        info!(
            target: "strata::snapshot",
            path = %path.display(),
            wal_offset = header.wal_offset,
            tx_count = header.transaction_count,
            size_bytes,
            sections = sections.len(),
            "Snapshot written successfully"
        );

        Ok(SnapshotInfo {
            path: path.to_path_buf(),
            timestamp_micros: header.timestamp_micros,
            wal_offset: header.wal_offset,
            size_bytes,
        })
    }

    /// Write snapshot atomically
    ///
    /// Uses temp file + rename pattern:
    /// 1. Write to temp file
    /// 2. Sync temp file
    /// 3. Rename temp to final (atomic on POSIX)
    ///
    /// If any step fails, temp file is cleaned up.
    pub fn write_atomic(
        &mut self,
        header: &SnapshotHeader,
        sections: &[PrimitiveSection],
        path: &Path,
    ) -> Result<SnapshotInfo, SnapshotError> {
        let temp_path = path.with_extension("snap.tmp");

        debug!(
            target: "strata::snapshot",
            final_path = %path.display(),
            temp_path = %temp_path.display(),
            "Starting atomic snapshot write"
        );

        // Clean up stale temp file if exists (from previous failed attempt)
        if temp_path.exists() {
            warn!(target: "strata::snapshot", path = %temp_path.display(), "Removing stale temp file");
            let _ = std::fs::remove_file(&temp_path);
        }

        // Write to temp
        let result = self.write(header, sections, &temp_path);

        match result {
            Ok(info) => {
                // Atomic rename
                match std::fs::rename(&temp_path, path) {
                    Ok(()) => {
                        debug!(target: "strata::snapshot", path = %path.display(), "Atomic rename completed");
                        Ok(SnapshotInfo {
                            path: path.to_path_buf(),
                            ..info
                        })
                    }
                    Err(e) => {
                        warn!(
                            target: "strata::snapshot",
                            temp_path = %temp_path.display(),
                            error = %e,
                            "Rename failed, cleaning up temp file"
                        );
                        let _ = std::fs::remove_file(&temp_path);
                        Err(SnapshotError::Io(e))
                    }
                }
            }
            Err(e) => {
                warn!(
                    target: "strata::snapshot",
                    temp_path = %temp_path.display(),
                    error = %e,
                    "Write failed, cleaning up temp file"
                );
                let _ = std::fs::remove_file(&temp_path);
                Err(e)
            }
        }
    }
}

impl Default for SnapshotWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Snapshot Reader
// ============================================================================

/// Snapshot reader for reading and validating snapshots
pub struct SnapshotReader;

impl SnapshotReader {
    /// Validate snapshot checksum from file
    pub fn validate_checksum(path: &Path) -> Result<(), SnapshotError> {
        let data = std::fs::read(path)?;
        Self::validate_checksum_from_bytes(&data)
    }

    /// Validate checksum from bytes
    pub fn validate_checksum_from_bytes(data: &[u8]) -> Result<(), SnapshotError> {
        if data.len() < MIN_SNAPSHOT_SIZE {
            return Err(SnapshotError::TooShort {
                expected: MIN_SNAPSHOT_SIZE,
                actual: data.len(),
            });
        }

        // Split content and checksum
        let (content, checksum_bytes) = data.split_at(data.len() - 4);

        // Parse stored checksum
        let stored = u32::from_le_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ]);

        // Compute checksum
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(content);
        let computed = hasher.finalize();

        if stored != computed {
            return Err(SnapshotError::ChecksumMismatch {
                expected: stored,
                actual: computed,
            });
        }

        Ok(())
    }

    /// Read snapshot header from file
    pub fn read_header(path: &Path) -> Result<SnapshotHeader, SnapshotError> {
        let data = std::fs::read(path)?;
        SnapshotHeader::from_bytes(&data)
    }

    /// Read complete snapshot envelope from file
    pub fn read_envelope(path: &Path) -> Result<SnapshotEnvelope, SnapshotError> {
        let data = std::fs::read(path)?;
        Self::parse_envelope(&data)
    }

    /// Parse snapshot envelope from bytes
    pub fn parse_envelope(data: &[u8]) -> Result<SnapshotEnvelope, SnapshotError> {
        // Validate checksum first
        Self::validate_checksum_from_bytes(data)?;

        // Parse header
        let header = SnapshotHeader::from_bytes(data)?;

        // Read primitive count
        if data.len() < SNAPSHOT_HEADER_SIZE + 1 {
            return Err(SnapshotError::TooShort {
                expected: SNAPSHOT_HEADER_SIZE + 1,
                actual: data.len(),
            });
        }

        let primitive_count = data[SNAPSHOT_HEADER_SIZE];
        let mut offset = SNAPSHOT_HEADER_SIZE + 1;
        let mut sections = Vec::with_capacity(primitive_count as usize);

        // Parse each section
        for i in 0..primitive_count {
            // Need at least type(1) + length(8)
            if offset + 9 > data.len() - 4 {
                // -4 for CRC32
                return Err(SnapshotError::TooShort {
                    expected: offset + 9,
                    actual: data.len() - 4,
                });
            }

            let primitive_type = data[offset];
            offset += 1;

            let length = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;

            // Check we have enough data
            if offset + length > data.len() - 4 {
                return Err(SnapshotError::TooShort {
                    expected: offset + length,
                    actual: data.len() - 4,
                });
            }

            let section_data = data[offset..offset + length].to_vec();
            offset += length;

            sections.push(PrimitiveSection {
                primitive_type,
                data: section_data,
            });

            debug!(
                target: "strata::snapshot",
                section = i,
                primitive_type, length, "Parsed snapshot section"
            );
        }

        // Parse checksum
        let checksum = u32::from_le_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]);

        Ok(SnapshotEnvelope {
            version: header.version,
            timestamp_micros: header.timestamp_micros,
            wal_offset: header.wal_offset,
            transaction_count: header.transaction_count,
            sections,
            checksum,
        })
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Serialize all primitives for snapshot
///
/// Helper function to collect all primitive sections into a vector.
pub fn serialize_all_primitives<K, J, E, S, R>(
    kv: &K,
    json: &J,
    event: &E,
    state: &S,
    run: &R,
) -> Result<Vec<PrimitiveSection>, SnapshotError>
where
    K: SnapshotSerializable,
    J: SnapshotSerializable,
    E: SnapshotSerializable,
    S: SnapshotSerializable,
    R: SnapshotSerializable,
{
    // Serialize each primitive - can't use vec![] macro due to fallible operations
    Ok(vec![
        PrimitiveSection {
            primitive_type: kv.primitive_type_id(),
            data: kv.snapshot_serialize()?,
        },
        PrimitiveSection {
            primitive_type: json.primitive_type_id(),
            data: json.snapshot_serialize()?,
        },
        PrimitiveSection {
            primitive_type: event.primitive_type_id(),
            data: event.snapshot_serialize()?,
        },
        PrimitiveSection {
            primitive_type: state.primitive_type_id(),
            data: state.snapshot_serialize()?,
        },
        PrimitiveSection {
            primitive_type: run.primitive_type_id(),
            data: run.snapshot_serialize()?,
        },
    ])
}

/// Deserialize primitives from snapshot sections
///
/// Restores state to all primitives from snapshot sections.
pub fn deserialize_primitives<K, J, E, S, R>(
    sections: &[PrimitiveSection],
    kv: &mut K,
    json: &mut J,
    event: &mut E,
    state: &mut S,
    run: &mut R,
) -> Result<(), SnapshotError>
where
    K: SnapshotSerializable,
    J: SnapshotSerializable,
    E: SnapshotSerializable,
    S: SnapshotSerializable,
    R: SnapshotSerializable,
{
    for section in sections {
        match section.primitive_type {
            primitive_ids::KV => kv.snapshot_deserialize(&section.data)?,
            primitive_ids::JSON => json.snapshot_deserialize(&section.data)?,
            primitive_ids::EVENT => event.snapshot_deserialize(&section.data)?,
            primitive_ids::STATE => state.snapshot_deserialize(&section.data)?,
            primitive_ids::BRANCH => run.snapshot_deserialize(&section.data)?,
            _ => {
                // Unknown primitive - log warning but continue
                // This allows forward compatibility with newer snapshots
                warn!(
                    target: "strata::snapshot",
                    primitive_type = section.primitive_type,
                    "Unknown primitive type in snapshot, skipping"
                );
            }
        }
    }
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_writer_new() {
        let writer = SnapshotWriter::new();
        // Just verify it creates without panic
        drop(writer);
    }

    #[test]
    fn test_snapshot_writer_default() {
        let writer = SnapshotWriter::default();
        drop(writer);
    }

    #[test]
    fn test_snapshot_write_empty() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections: Vec<PrimitiveSection> = vec![];

        let mut writer = SnapshotWriter::new();
        let info = writer.write(&header, &sections, &path).unwrap();

        assert!(path.exists());
        assert_eq!(info.wal_offset, 100);
        assert!(info.size_bytes > 0);
    }

    #[test]
    fn test_snapshot_write_with_sections() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![
            PrimitiveSection::new(primitive_ids::KV, vec![1, 2, 3]),
            PrimitiveSection::new(primitive_ids::JSON, vec![4, 5, 6, 7]),
        ];

        let mut writer = SnapshotWriter::new();
        let info = writer.write(&header, &sections, &path).unwrap();

        assert!(path.exists());
        assert_eq!(info.wal_offset, 100);

        // Validate checksum
        SnapshotReader::validate_checksum(&path).unwrap();
    }

    #[test]
    fn test_snapshot_write_creates_parent_dir() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nested").join("dir").join("test.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections: Vec<PrimitiveSection> = vec![];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        assert!(path.exists());
    }

    #[test]
    fn test_snapshot_checksum_validation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![PrimitiveSection::new(primitive_ids::KV, vec![1, 2, 3])];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        // Validation should pass
        SnapshotReader::validate_checksum(&path).unwrap();
    }

    #[test]
    fn test_snapshot_checksum_detects_corruption() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![PrimitiveSection::new(primitive_ids::KV, vec![1, 2, 3])];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        // Corrupt the file
        let mut data = std::fs::read(&path).unwrap();
        data[20] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        // Validation should fail
        let result = SnapshotReader::validate_checksum(&path);
        assert!(matches!(
            result,
            Err(SnapshotError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn test_snapshot_atomic_write() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("snapshot.snap");
        let temp_path = path.with_extension("snap.tmp");

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![PrimitiveSection::new(primitive_ids::KV, vec![1, 2, 3])];

        let mut writer = SnapshotWriter::new();
        writer.write_atomic(&header, &sections, &path).unwrap();

        // Final should exist, temp should not
        assert!(path.exists());
        assert!(!temp_path.exists());

        // Validate
        SnapshotReader::validate_checksum(&path).unwrap();
    }

    #[test]
    fn test_snapshot_atomic_write_cleanup_stale() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("snapshot.snap");
        let temp_path = path.with_extension("snap.tmp");

        // Create stale temp file
        std::fs::write(&temp_path, b"stale data").unwrap();
        assert!(temp_path.exists());

        let header = SnapshotHeader::new(100, 10);
        let sections: Vec<PrimitiveSection> = vec![];

        let mut writer = SnapshotWriter::new();
        writer.write_atomic(&header, &sections, &path).unwrap();

        // Temp should be gone, final should exist
        assert!(!temp_path.exists());
        assert!(path.exists());
    }

    #[test]
    fn test_snapshot_read_header() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        let header = SnapshotHeader::with_timestamp(12345, 100, 9999);
        let sections: Vec<PrimitiveSection> = vec![];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        let read_header = SnapshotReader::read_header(&path).unwrap();
        assert_eq!(read_header.wal_offset, 12345);
        assert_eq!(read_header.transaction_count, 100);
        assert_eq!(read_header.timestamp_micros, 9999);
    }

    #[test]
    fn test_snapshot_read_envelope() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        let header = SnapshotHeader::with_timestamp(12345, 100, 9999);
        let sections = vec![
            PrimitiveSection::new(primitive_ids::KV, vec![1, 2, 3]),
            PrimitiveSection::new(primitive_ids::JSON, vec![4, 5, 6, 7]),
        ];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        let envelope = SnapshotReader::read_envelope(&path).unwrap();
        assert_eq!(envelope.wal_offset, 12345);
        assert_eq!(envelope.transaction_count, 100);
        assert_eq!(envelope.sections.len(), 2);
        assert_eq!(envelope.sections[0].primitive_type, primitive_ids::KV);
        assert_eq!(envelope.sections[0].data, vec![1, 2, 3]);
        assert_eq!(envelope.sections[1].primitive_type, primitive_ids::JSON);
        assert_eq!(envelope.sections[1].data, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_snapshot_roundtrip_all_primitives() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        let header = SnapshotHeader::new(50000, 500);
        let sections = vec![
            PrimitiveSection::new(primitive_ids::KV, b"kv-data".to_vec()),
            PrimitiveSection::new(primitive_ids::JSON, b"json-data".to_vec()),
            PrimitiveSection::new(primitive_ids::EVENT, b"event-data".to_vec()),
            PrimitiveSection::new(primitive_ids::STATE, b"state-data".to_vec()),
            PrimitiveSection::new(primitive_ids::BRANCH, b"branch-data".to_vec()),
        ];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        let envelope = SnapshotReader::read_envelope(&path).unwrap();
        assert_eq!(envelope.sections.len(), 5);

        for (i, expected_type) in [
            primitive_ids::KV,
            primitive_ids::JSON,
            primitive_ids::EVENT,
            primitive_ids::STATE,
            primitive_ids::BRANCH,
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(envelope.sections[i].primitive_type, *expected_type);
        }
    }

    #[test]
    fn test_snapshot_large_section() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.snap");

        // Create a large section (1MB)
        let large_data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![PrimitiveSection::new(primitive_ids::KV, large_data.clone())];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        // Validate and read back
        SnapshotReader::validate_checksum(&path).unwrap();
        let envelope = SnapshotReader::read_envelope(&path).unwrap();
        assert_eq!(envelope.sections[0].data, large_data);
    }

    #[test]
    fn test_snapshot_checksum_from_bytes_too_short() {
        let data = vec![0u8; 10];
        let result = SnapshotReader::validate_checksum_from_bytes(&data);
        assert!(matches!(result, Err(SnapshotError::TooShort { .. })));
    }

    // Mock implementation of SnapshotSerializable for testing
    struct MockPrimitive {
        type_id: u8,
        data: Vec<u8>,
    }

    impl SnapshotSerializable for MockPrimitive {
        fn snapshot_serialize(&self) -> Result<Vec<u8>, SnapshotError> {
            Ok(self.data.clone())
        }

        fn snapshot_deserialize(&mut self, data: &[u8]) -> Result<(), SnapshotError> {
            self.data = data.to_vec();
            Ok(())
        }

        fn primitive_type_id(&self) -> u8 {
            self.type_id
        }
    }

    #[test]
    fn test_serialize_all_primitives() {
        let kv = MockPrimitive {
            type_id: primitive_ids::KV,
            data: vec![1],
        };
        let json = MockPrimitive {
            type_id: primitive_ids::JSON,
            data: vec![2],
        };
        let event = MockPrimitive {
            type_id: primitive_ids::EVENT,
            data: vec![3],
        };
        let state = MockPrimitive {
            type_id: primitive_ids::STATE,
            data: vec![4],
        };
        let run = MockPrimitive {
            type_id: primitive_ids::BRANCH,
            data: vec![6],
        };

        let sections = serialize_all_primitives(&kv, &json, &event, &state, &run).unwrap();

        assert_eq!(sections.len(), 5);
        assert_eq!(sections[0].data, vec![1]);
        assert_eq!(sections[4].data, vec![6]);
    }

    #[test]
    fn test_deserialize_primitives() {
        let sections = vec![
            PrimitiveSection::new(primitive_ids::KV, vec![10]),
            PrimitiveSection::new(primitive_ids::JSON, vec![20]),
            PrimitiveSection::new(primitive_ids::EVENT, vec![30]),
            PrimitiveSection::new(primitive_ids::STATE, vec![40]),
            PrimitiveSection::new(primitive_ids::BRANCH, vec![60]),
        ];

        let mut kv = MockPrimitive {
            type_id: primitive_ids::KV,
            data: vec![],
        };
        let mut json = MockPrimitive {
            type_id: primitive_ids::JSON,
            data: vec![],
        };
        let mut event = MockPrimitive {
            type_id: primitive_ids::EVENT,
            data: vec![],
        };
        let mut state = MockPrimitive {
            type_id: primitive_ids::STATE,
            data: vec![],
        };
        let mut run = MockPrimitive {
            type_id: primitive_ids::BRANCH,
            data: vec![],
        };

        deserialize_primitives(
            &sections, &mut kv, &mut json, &mut event, &mut state, &mut run,
        )
        .unwrap();

        assert_eq!(kv.data, vec![10]);
        assert_eq!(json.data, vec![20]);
        assert_eq!(event.data, vec![30]);
        assert_eq!(state.data, vec![40]);
        assert_eq!(run.data, vec![60]);
    }

    #[test]
    fn test_deserialize_unknown_primitive_skipped() {
        let sections = vec![
            PrimitiveSection::new(primitive_ids::KV, vec![10]),
            PrimitiveSection::new(99, vec![99]), // Unknown primitive
        ];

        let mut kv = MockPrimitive {
            type_id: primitive_ids::KV,
            data: vec![],
        };
        let mut json = MockPrimitive {
            type_id: primitive_ids::JSON,
            data: vec![],
        };
        let mut event = MockPrimitive {
            type_id: primitive_ids::EVENT,
            data: vec![],
        };
        let mut state = MockPrimitive {
            type_id: primitive_ids::STATE,
            data: vec![],
        };
        let mut run = MockPrimitive {
            type_id: primitive_ids::BRANCH,
            data: vec![],
        };

        // Should not fail, just skip unknown
        deserialize_primitives(
            &sections, &mut kv, &mut json, &mut event, &mut state, &mut run,
        )
        .unwrap();

        assert_eq!(kv.data, vec![10]);
        // Others should remain empty
        assert!(json.data.is_empty());
    }

    // ========================================================================
    // Adversarial Snapshot Tests
    // ========================================================================

    #[test]
    fn test_snapshot_read_header_from_truncated_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("truncated.snap");

        // Write a valid snapshot first
        let header = SnapshotHeader::new(100, 10);
        let mut writer = SnapshotWriter::new();
        writer.write(&header, &[], &path).unwrap();

        // Truncate to less than header size
        let mut data = std::fs::read(&path).unwrap();
        data.truncate(SNAPSHOT_HEADER_SIZE - 5);
        std::fs::write(&path, &data).unwrap();

        let result = SnapshotReader::read_header(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_read_envelope_from_corrupted_crc() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("corrupt_crc.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![PrimitiveSection::new(primitive_ids::KV, vec![1, 2, 3])];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        // Corrupt the CRC (last 4 bytes)
        let mut data = std::fs::read(&path).unwrap();
        let len = data.len();
        data[len - 1] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = SnapshotReader::read_envelope(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_read_envelope_with_zero_sections() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("no_sections.snap");

        let header = SnapshotHeader::new(0, 0);
        let mut writer = SnapshotWriter::new();
        writer.write(&header, &[], &path).unwrap();

        let envelope = SnapshotReader::read_envelope(&path).unwrap();
        assert!(envelope.sections.is_empty());
    }

    #[test]
    fn test_snapshot_write_overwrite_existing() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("overwrite.snap");

        // First write
        let header1 = SnapshotHeader::with_timestamp(100, 10, 1000);
        let mut writer = SnapshotWriter::new();
        writer.write(&header1, &[], &path).unwrap();

        // Second write (overwrite)
        let header2 = SnapshotHeader::with_timestamp(200, 20, 2000);
        writer.write(&header2, &[], &path).unwrap();

        // Should read the second header
        let read_header = SnapshotReader::read_header(&path).unwrap();
        assert_eq!(read_header.wal_offset, 200);
        assert_eq!(read_header.transaction_count, 20);
    }

    #[test]
    fn test_snapshot_validate_checksum_from_bytes_valid() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("valid.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![
            PrimitiveSection::new(primitive_ids::KV, vec![1, 2, 3]),
            PrimitiveSection::new(primitive_ids::JSON, vec![4, 5]),
        ];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        let data = std::fs::read(&path).unwrap();
        SnapshotReader::validate_checksum_from_bytes(&data).unwrap();
    }

    #[test]
    fn test_snapshot_corruption_at_different_positions() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("multi_corrupt.snap");

        let header = SnapshotHeader::new(100, 10);
        let sections = vec![PrimitiveSection::new(primitive_ids::KV, vec![0xAB; 100])];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        let original = std::fs::read(&path).unwrap();

        // Try corrupting at various positions (except the CRC itself)
        let positions = [0, 5, 10, 20, 30, 40, 50, 60, 70];
        for &pos in &positions {
            if pos >= original.len() - 4 {
                continue; // Skip CRC region
            }
            let mut corrupted = original.clone();
            corrupted[pos] ^= 0xFF;

            let result = SnapshotReader::validate_checksum_from_bytes(&corrupted);
            assert!(
                result.is_err(),
                "Corruption at position {} should be detected",
                pos
            );
        }
    }

    #[test]
    fn test_snapshot_with_all_six_primitive_types() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("all_six.snap");

        let header = SnapshotHeader::new(1000, 100);
        let sections = vec![
            PrimitiveSection::new(primitive_ids::KV, b"kv".to_vec()),
            PrimitiveSection::new(primitive_ids::JSON, b"json".to_vec()),
            PrimitiveSection::new(primitive_ids::EVENT, b"event".to_vec()),
            PrimitiveSection::new(primitive_ids::STATE, b"state".to_vec()),
            PrimitiveSection::new(primitive_ids::BRANCH, b"branch".to_vec()),
            PrimitiveSection::new(primitive_ids::VECTOR, b"vector".to_vec()),
        ];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        let envelope = SnapshotReader::read_envelope(&path).unwrap();
        assert_eq!(envelope.sections.len(), 6);
        assert_eq!(envelope.sections[5].primitive_type, primitive_ids::VECTOR);
        assert_eq!(envelope.sections[5].data, b"vector");
    }

    #[test]
    fn test_snapshot_empty_section_data_preserved() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("empty_data.snap");

        let header = SnapshotHeader::new(0, 0);
        let sections = vec![PrimitiveSection::new(primitive_ids::KV, vec![])];

        let mut writer = SnapshotWriter::new();
        writer.write(&header, &sections, &path).unwrap();

        let envelope = SnapshotReader::read_envelope(&path).unwrap();
        assert_eq!(envelope.sections.len(), 1);
        assert!(envelope.sections[0].data.is_empty());
    }
}
