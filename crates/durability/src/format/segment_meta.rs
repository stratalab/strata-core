//! Per-segment metadata sidecar files.
//!
//! Each closed WAL segment `wal-NNNNNN.seg` gets a companion `wal-NNNNNN.meta`
//! file containing 60 bytes of metadata: min/max timestamps, min/max txn_ids,
//! and record count. This enables O(1) segment coverage checks and efficient
//! time-range queries without scanning segment contents.
//!
//! # Binary Format (60 bytes)
//!
//! ```text
//! magic("STAM", 4) + version(4) + segment_number(8) + min_ts(8) + max_ts(8)
//! + min_txn_id(8) + max_txn_id(8) + record_count(8) + crc32(4) = 60 bytes
//! ```
//!
//! # Backward Compatibility
//!
//! `.meta` files are fully optional sidecar files — no WAL format version bump
//! is needed. Missing or corrupted `.meta` files are handled gracefully with
//! fallback to full-scan logic, and can be regenerated from segments during
//! recovery.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Magic bytes for segment metadata files.
pub const SEGMENT_META_MAGIC: &[u8; 4] = b"STAM";

/// Current format version for segment metadata.
pub const SEGMENT_META_VERSION: u32 = 1;

/// Total size of a serialized `SegmentMeta` in bytes.
pub const SEGMENT_META_SIZE: usize = 60;

/// Per-segment metadata tracking min/max timestamps, txn_ids, and record count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Segment number this metadata belongs to.
    pub segment_number: u64,
    /// Minimum timestamp (microseconds since epoch) across all records.
    /// `u64::MAX` when empty.
    pub min_timestamp: u64,
    /// Maximum timestamp (microseconds since epoch) across all records.
    /// `0` when empty.
    pub max_timestamp: u64,
    /// Minimum transaction ID across all records.
    /// `u64::MAX` when empty.
    pub min_txn_id: u64,
    /// Maximum transaction ID across all records.
    /// `0` when empty.
    pub max_txn_id: u64,
    /// Number of records tracked.
    pub record_count: u64,
}

impl SegmentMeta {
    /// Create a new empty metadata tracker for the given segment.
    ///
    /// All mins are set to `u64::MAX`, maxes to `0`, count to `0`.
    pub fn new_empty(segment_number: u64) -> Self {
        SegmentMeta {
            segment_number,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            min_txn_id: u64::MAX,
            max_txn_id: 0,
            record_count: 0,
        }
    }

    /// Update running min/max/count with a new record.
    pub fn track_record(&mut self, txn_id: u64, timestamp: u64) {
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);
        self.min_txn_id = self.min_txn_id.min(txn_id);
        self.max_txn_id = self.max_txn_id.max(txn_id);
        self.record_count += 1;
    }

    /// Returns `true` if no records have been tracked.
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Generate the path for a `.meta` sidecar file.
    pub fn meta_path(dir: &Path, segment_number: u64) -> PathBuf {
        dir.join(format!("wal-{:06}.meta", segment_number))
    }

    /// Write the metadata to a `.meta` file using write-fsync-rename.
    pub fn write_to_file(&self, dir: &Path) -> Result<(), SegmentMetaError> {
        let final_path = Self::meta_path(dir, self.segment_number);
        let temp_path = final_path.with_extension("meta.tmp");

        let bytes = self.to_bytes_full();

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(SegmentMetaError::Io)?;

        file.write_all(&bytes).map_err(SegmentMetaError::Io)?;
        file.sync_all().map_err(SegmentMetaError::Io)?;
        drop(file);

        std::fs::rename(&temp_path, &final_path).map_err(SegmentMetaError::Io)?;

        // Sync parent directory
        if let Some(parent) = final_path.parent() {
            if parent.exists() {
                let dir_fd = File::open(parent).map_err(SegmentMetaError::Io)?;
                dir_fd.sync_all().map_err(SegmentMetaError::Io)?;
            }
        }

        Ok(())
    }

    /// Read a `.meta` file for the given segment number.
    ///
    /// Returns `Ok(None)` if the file does not exist.
    pub fn read_from_file(
        dir: &Path,
        segment_number: u64,
    ) -> Result<Option<Self>, SegmentMetaError> {
        let path = Self::meta_path(dir, segment_number);
        match std::fs::read(&path) {
            Ok(data) => Ok(Some(Self::from_bytes(&data)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SegmentMetaError::Io(e)),
        }
    }

    /// Serialize to the full binary format with CRC.
    fn to_bytes_full(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(SEGMENT_META_SIZE);
        buf.extend_from_slice(SEGMENT_META_MAGIC);
        buf.extend_from_slice(&SEGMENT_META_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.segment_number.to_le_bytes());
        buf.extend_from_slice(&self.min_timestamp.to_le_bytes());
        buf.extend_from_slice(&self.max_timestamp.to_le_bytes());
        buf.extend_from_slice(&self.min_txn_id.to_le_bytes());
        buf.extend_from_slice(&self.max_txn_id.to_le_bytes());
        buf.extend_from_slice(&self.record_count.to_le_bytes());

        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize from bytes, validating magic, version, and CRC.
    pub fn from_bytes(data: &[u8]) -> Result<Self, SegmentMetaError> {
        if data.len() < SEGMENT_META_SIZE {
            return Err(SegmentMetaError::TooShort {
                expected: SEGMENT_META_SIZE,
                actual: data.len(),
            });
        }

        // Validate magic
        if &data[0..4] != SEGMENT_META_MAGIC {
            return Err(SegmentMetaError::InvalidMagic);
        }

        // Validate version
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != SEGMENT_META_VERSION {
            return Err(SegmentMetaError::UnsupportedVersion(version));
        }

        // Validate CRC (over first SEGMENT_META_SIZE - 4 bytes)
        let crc_offset = SEGMENT_META_SIZE - 4;
        let stored_crc =
            u32::from_le_bytes(data[crc_offset..SEGMENT_META_SIZE].try_into().unwrap());
        let computed_crc = crc32fast::hash(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(SegmentMetaError::ChecksumMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        Ok(SegmentMeta {
            segment_number: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            min_timestamp: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            max_timestamp: u64::from_le_bytes(data[24..32].try_into().unwrap()),
            min_txn_id: u64::from_le_bytes(data[32..40].try_into().unwrap()),
            max_txn_id: u64::from_le_bytes(data[40..48].try_into().unwrap()),
            record_count: u64::from_le_bytes(data[48..56].try_into().unwrap()),
        })
    }
}

/// Errors that can occur when reading or writing segment metadata.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SegmentMetaError {
    /// Data too short to contain a valid metadata header.
    #[error("segment meta too short: expected {expected} bytes, got {actual}")]
    TooShort {
        /// Expected minimum size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },

    /// Magic bytes do not match `STAM`.
    #[error("invalid segment meta magic bytes")]
    InvalidMagic,

    /// Unsupported format version.
    #[error("unsupported segment meta version: {0}")]
    UnsupportedVersion(u32),

    /// CRC32 checksum mismatch.
    #[error("segment meta checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        /// CRC stored in the file.
        stored: u32,
        /// CRC computed from the data.
        computed: u32,
    },

    /// I/O error during file operations.
    #[error("segment meta I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_new_empty() {
        let meta = SegmentMeta::new_empty(42);
        assert_eq!(meta.segment_number, 42);
        assert_eq!(meta.min_timestamp, u64::MAX);
        assert_eq!(meta.max_timestamp, 0);
        assert_eq!(meta.min_txn_id, u64::MAX);
        assert_eq!(meta.max_txn_id, 0);
        assert_eq!(meta.record_count, 0);
        assert!(meta.is_empty());
    }

    #[test]
    fn test_track_record() {
        let mut meta = SegmentMeta::new_empty(1);

        meta.track_record(10, 1000);
        assert_eq!(meta.min_timestamp, 1000);
        assert_eq!(meta.max_timestamp, 1000);
        assert_eq!(meta.min_txn_id, 10);
        assert_eq!(meta.max_txn_id, 10);
        assert_eq!(meta.record_count, 1);
        assert!(!meta.is_empty());

        meta.track_record(5, 2000);
        assert_eq!(meta.min_timestamp, 1000);
        assert_eq!(meta.max_timestamp, 2000);
        assert_eq!(meta.min_txn_id, 5);
        assert_eq!(meta.max_txn_id, 10);
        assert_eq!(meta.record_count, 2);

        meta.track_record(20, 500);
        assert_eq!(meta.min_timestamp, 500);
        assert_eq!(meta.max_timestamp, 2000);
        assert_eq!(meta.min_txn_id, 5);
        assert_eq!(meta.max_txn_id, 20);
        assert_eq!(meta.record_count, 3);
    }

    #[test]
    fn test_roundtrip_serialization() {
        let mut meta = SegmentMeta::new_empty(7);
        meta.track_record(100, 50000);
        meta.track_record(200, 60000);

        let bytes = meta.to_bytes_full();
        assert_eq!(bytes.len(), SEGMENT_META_SIZE);

        let decoded = SegmentMeta::from_bytes(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn test_roundtrip_empty() {
        let meta = SegmentMeta::new_empty(0);
        let bytes = meta.to_bytes_full();
        let decoded = SegmentMeta::from_bytes(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn test_from_bytes_too_short() {
        let err = SegmentMeta::from_bytes(&[0u8; 10]).unwrap_err();
        assert!(matches!(err, SegmentMetaError::TooShort { .. }));
    }

    #[test]
    fn test_from_bytes_invalid_magic() {
        let mut bytes = SegmentMeta::new_empty(1).to_bytes_full();
        bytes[0] = b'X';
        let err = SegmentMeta::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, SegmentMetaError::InvalidMagic));
    }

    #[test]
    fn test_from_bytes_unsupported_version() {
        let mut bytes = SegmentMeta::new_empty(1).to_bytes_full();
        // Overwrite version with 99
        bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
        let err = SegmentMeta::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, SegmentMetaError::UnsupportedVersion(99)));
    }

    #[test]
    fn test_from_bytes_checksum_mismatch() {
        let mut bytes = SegmentMeta::new_empty(1).to_bytes_full();
        // Corrupt a data byte
        bytes[10] ^= 0xFF;
        let err = SegmentMeta::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, SegmentMetaError::ChecksumMismatch { .. }));
    }

    #[test]
    fn test_meta_path() {
        let path = SegmentMeta::meta_path(Path::new("/tmp/wal"), 1);
        assert_eq!(path.to_str().unwrap(), "/tmp/wal/wal-000001.meta");

        let path = SegmentMeta::meta_path(Path::new("/tmp/wal"), 999999);
        assert_eq!(path.to_str().unwrap(), "/tmp/wal/wal-999999.meta");
    }

    #[test]
    fn test_write_and_read_file() {
        let dir = tempdir().unwrap();
        let mut meta = SegmentMeta::new_empty(3);
        meta.track_record(10, 5000);
        meta.track_record(20, 6000);

        meta.write_to_file(dir.path()).unwrap();

        let loaded = SegmentMeta::read_from_file(dir.path(), 3)
            .unwrap()
            .expect("should exist");
        assert_eq!(meta, loaded);
    }

    #[test]
    fn test_read_nonexistent_file() {
        let dir = tempdir().unwrap();
        let result = SegmentMeta::read_from_file(dir.path(), 99).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_write_overwrites_existing() {
        let dir = tempdir().unwrap();

        let mut meta1 = SegmentMeta::new_empty(1);
        meta1.track_record(5, 1000);
        meta1.write_to_file(dir.path()).unwrap();

        let mut meta2 = SegmentMeta::new_empty(1);
        meta2.track_record(10, 2000);
        meta2.track_record(20, 3000);
        meta2.write_to_file(dir.path()).unwrap();

        let loaded = SegmentMeta::read_from_file(dir.path(), 1)
            .unwrap()
            .expect("should exist");
        assert_eq!(meta2, loaded);
    }

    #[test]
    fn test_segment_meta_size_constant() {
        // Verify our constant matches actual serialized size:
        // magic(4) + version(4) + segment_number(8) + min_ts(8) + max_ts(8)
        //   + min_txn_id(8) + max_txn_id(8) + record_count(8) + crc32(4) = 60
        let meta = SegmentMeta::new_empty(1);
        let bytes = meta.to_bytes_full();
        assert_eq!(bytes.len(), SEGMENT_META_SIZE);
    }
}
