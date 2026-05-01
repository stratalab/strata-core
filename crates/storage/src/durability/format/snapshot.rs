//! Snapshot file format
//!
//! Snapshots are named `snap-NNNNNN.chk` where NNNNNN is zero-padded.
//! Each snapshot has a 64-byte header followed by primitive sections.
//!
//! # File Structure
//!
//! ```text
//! +------------------+ 0
//! | SnapshotHeader   | 64 bytes
//! +------------------+ 64
//! | Codec ID         | variable (header.codec_id_len bytes)
//! +------------------+
//! | Section 1        | SectionHeader + data
//! +------------------+
//! | Section 2        | SectionHeader + data
//! +------------------+
//! | ...              |
//! +------------------+
//! | Footer CRC32     | 4 bytes
//! +------------------+
//! ```

use std::path::{Path, PathBuf};

/// Magic bytes: "SNAP" (0x534E4150)
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"SNAP";

/// Snapshot format version.
///
/// v2 added retention metadata to the per-primitive DTOs (tombstone markers
/// on KV/JSON/Vector; `ttl_ms` on KV; explicit `branch_id` on Branch). Old
/// v1 snapshots are rejected on load — the T3-E5 follow-up deliberately
/// made a clean break rather than maintaining dual-format deserializers,
/// since no pre-release database ships with committed `.chk` fixtures and
/// any live database can be re-checkpointed after upgrade.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 2;

/// Oldest snapshot format version this build can read. Files with a
/// `format_version` below this value are rejected with
/// [`crate::durability::disk_snapshot::SnapshotReadError::LegacyFormat`] — mirroring the
/// WAL clean-break contract at
/// [`crate::durability::format::wal_record::MIN_SUPPORTED_SEGMENT_FORMAT_VERSION`].
/// Operators wipe the affected `snap-*.chk` file and reopen; the database
/// will replay from the next snapshot or the WAL tail.
pub const MIN_SUPPORTED_SNAPSHOT_FORMAT_VERSION: u32 = 2;

/// Snapshot header size in bytes
pub const SNAPSHOT_HEADER_SIZE: usize = 64;

/// Snapshot header (64 bytes)
///
/// The header contains essential metadata for snapshot validation and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Magic bytes: "SNAP" (0x534E4150)
    pub magic: [u8; 4],
    /// Format version for forward compatibility
    pub format_version: u32,
    /// Snapshot identifier (monotonically increasing)
    pub snapshot_id: u64,
    /// Watermark transaction ID (all txns <= this are included)
    pub watermark_txn: u64,
    /// Creation timestamp (microseconds since epoch)
    pub created_at: u64,
    /// Database UUID for validation
    pub database_uuid: [u8; 16],
    /// Codec ID length (followed by codec ID string in body)
    pub codec_id_len: u8,
    /// Reserved for future use
    pub reserved: [u8; 15],
}

impl SnapshotHeader {
    /// Create a new snapshot header
    pub fn new(
        snapshot_id: u64,
        watermark_txn: u64,
        created_at: u64,
        database_uuid: [u8; 16],
        codec_id_len: u8,
    ) -> Self {
        SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            format_version: SNAPSHOT_FORMAT_VERSION,
            snapshot_id,
            watermark_txn,
            created_at,
            database_uuid,
            codec_id_len,
            reserved: [0u8; 15],
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; SNAPSHOT_HEADER_SIZE] {
        let mut bytes = [0u8; SNAPSHOT_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.format_version.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.snapshot_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.watermark_txn.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.created_at.to_le_bytes());
        bytes[32..48].copy_from_slice(&self.database_uuid);
        bytes[48] = self.codec_id_len;
        bytes[49..64].copy_from_slice(&self.reserved);
        bytes
    }

    /// Parse header from bytes
    pub fn from_bytes(bytes: &[u8; SNAPSHOT_HEADER_SIZE]) -> Option<Self> {
        Some(SnapshotHeader {
            magic: bytes[0..4].try_into().ok()?,
            format_version: u32::from_le_bytes(bytes[4..8].try_into().ok()?),
            snapshot_id: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
            watermark_txn: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
            created_at: u64::from_le_bytes(bytes[24..32].try_into().ok()?),
            database_uuid: bytes[32..48].try_into().ok()?,
            codec_id_len: bytes[48],
            reserved: bytes[49..64].try_into().ok()?,
        })
    }

    /// Validate the header
    pub fn validate(&self) -> Result<(), SnapshotHeaderError> {
        if self.magic != SNAPSHOT_MAGIC {
            return Err(SnapshotHeaderError::InvalidMagic {
                expected: SNAPSHOT_MAGIC,
                actual: self.magic,
            });
        }
        if self.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotHeaderError::UnsupportedVersion {
                version: self.format_version,
                max_supported: SNAPSHOT_FORMAT_VERSION,
            });
        }
        Ok(())
    }
}

/// Errors that can occur when validating a snapshot header
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotHeaderError {
    /// Invalid magic bytes
    #[error("Invalid magic bytes: expected {expected:?}, got {actual:?}")]
    InvalidMagic {
        /// Expected magic bytes
        expected: [u8; 4],
        /// Actual magic bytes found
        actual: [u8; 4],
    },
    /// Unsupported format version
    #[error("Unsupported snapshot version {version}, max supported is {max_supported}")]
    UnsupportedVersion {
        /// Version found in the file
        version: u32,
        /// Maximum supported version
        max_supported: u32,
    },
}

/// Section header for each primitive type
///
/// Each section contains data for one primitive type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionHeader {
    /// Primitive type tag (see `primitive_tags` module)
    pub primitive_type: u8,
    /// Section data length in bytes
    pub data_len: u64,
}

impl SectionHeader {
    /// Section header size in bytes
    pub const SIZE: usize = 9;

    /// Create a new section header
    pub fn new(primitive_type: u8, data_len: u64) -> Self {
        SectionHeader {
            primitive_type,
            data_len,
        }
    }

    /// Serialize section header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0] = self.primitive_type;
        bytes[1..9].copy_from_slice(&self.data_len.to_le_bytes());
        bytes
    }

    /// Parse section header from bytes
    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        SectionHeader {
            primitive_type: bytes[0],
            data_len: u64::from_le_bytes(bytes[1..9].try_into().unwrap()),
        }
    }
}

/// Generate snapshot file path
///
/// Snapshots are named `snap-NNNNNN.chk` where NNNNNN is zero-padded.
pub fn snapshot_path(dir: &Path, snapshot_id: u64) -> PathBuf {
    dir.join(format!("snap-{:06}.chk", snapshot_id))
}

/// Parse snapshot ID from file name
///
/// Returns None if the file name doesn't match the expected format.
pub fn parse_snapshot_id(file_name: &str) -> Option<u64> {
    if !file_name.starts_with("snap-") || !file_name.ends_with(".chk") {
        return None;
    }
    let id_str = &file_name[5..file_name.len() - 4];
    id_str.parse().ok()
}

/// List all snapshot files in a directory, sorted by ID
pub fn list_snapshots(dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut snapshots = Vec::new();

    if !dir.exists() {
        return Ok(snapshots);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        if let Some(id) = parse_snapshot_id(&file_name) {
            snapshots.push((id, entry.path()));
        }
    }

    snapshots.sort_by_key(|(id, _)| *id);
    Ok(snapshots)
}

/// Find the latest snapshot in a directory
pub fn find_latest_snapshot(dir: &Path) -> std::io::Result<Option<(u64, PathBuf)>> {
    let snapshots = list_snapshots(dir)?;
    Ok(snapshots.into_iter().last())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::format::primitive_tags;

    #[test]
    fn test_snapshot_header_roundtrip() {
        let uuid = [1u8; 16];
        let header = SnapshotHeader::new(1, 100, 1234567890, uuid, 8);

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), SNAPSHOT_HEADER_SIZE);

        let parsed = SnapshotHeader::from_bytes(&bytes).unwrap();
        assert_eq!(header, parsed);
    }

    #[test]
    fn test_snapshot_header_magic() {
        let uuid = [1u8; 16];
        let header = SnapshotHeader::new(1, 100, 1234567890, uuid, 8);
        let bytes = header.to_bytes();

        assert_eq!(&bytes[0..4], b"SNAP");
    }

    #[test]
    fn test_snapshot_header_validation() {
        let uuid = [1u8; 16];
        let header = SnapshotHeader::new(1, 100, 1234567890, uuid, 8);
        assert!(header.validate().is_ok());

        // Invalid magic
        let mut bad_header = header.clone();
        bad_header.magic = *b"BADM";
        assert!(matches!(
            bad_header.validate(),
            Err(SnapshotHeaderError::InvalidMagic { .. })
        ));

        // Future version is rejected (exact match required).
        let mut future_header = header.clone();
        future_header.format_version = SNAPSHOT_FORMAT_VERSION + 1;
        assert!(matches!(
            future_header.validate(),
            Err(SnapshotHeaderError::UnsupportedVersion { .. })
        ));

        // Past version (pre-retention) is also rejected — the clean break
        // from v1 is deliberate; see module docs on SNAPSHOT_FORMAT_VERSION.
        let mut legacy_header = header.clone();
        legacy_header.format_version = 1;
        assert!(matches!(
            legacy_header.validate(),
            Err(SnapshotHeaderError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn test_section_header_roundtrip() {
        let header = SectionHeader::new(primitive_tags::KV, 1024);

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), SectionHeader::SIZE);

        let parsed = SectionHeader::from_bytes(&bytes);
        assert_eq!(header, parsed);
    }

    #[test]
    fn test_snapshot_path() {
        use std::path::Path;

        let path = snapshot_path(Path::new("/data"), 1);
        assert_eq!(path.to_str().unwrap(), "/data/snap-000001.chk");

        let path = snapshot_path(Path::new("/data"), 999999);
        assert_eq!(path.to_str().unwrap(), "/data/snap-999999.chk");
    }

    #[test]
    fn test_parse_snapshot_id() {
        assert_eq!(parse_snapshot_id("snap-000001.chk"), Some(1));
        assert_eq!(parse_snapshot_id("snap-999999.chk"), Some(999999));
        assert_eq!(parse_snapshot_id("snap-000100.chk"), Some(100));

        // Invalid formats
        assert_eq!(parse_snapshot_id("snapshot-000001.chk"), None);
        assert_eq!(parse_snapshot_id("snap-000001.bak"), None);
        assert_eq!(parse_snapshot_id("wal-000001.seg"), None);
    }

    #[test]
    fn test_primitive_tags() {
        assert_eq!(primitive_tags::tag_name(primitive_tags::KV), "KV");
        assert_eq!(primitive_tags::tag_name(primitive_tags::EVENT), "Event");
        assert_eq!(primitive_tags::tag_name(primitive_tags::BRANCH), "Branch");
        assert_eq!(primitive_tags::tag_name(primitive_tags::JSON), "Json");
        assert_eq!(primitive_tags::tag_name(primitive_tags::VECTOR), "Vector");
        assert_eq!(primitive_tags::tag_name(0xFF), "Unknown");
    }

    #[test]
    fn test_all_tags() {
        assert_eq!(primitive_tags::ALL_TAGS.len(), 6);
        assert_eq!(
            primitive_tags::ALL_TAGS,
            [
                primitive_tags::KV,
                primitive_tags::EVENT,
                primitive_tags::BRANCH,
                primitive_tags::JSON,
                primitive_tags::VECTOR,
                primitive_tags::GRAPH,
            ]
        );
    }

    #[test]
    fn test_list_snapshots() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();

        // Create some snapshot files
        std::fs::write(dir.join("snap-000001.chk"), b"test").unwrap();
        std::fs::write(dir.join("snap-000003.chk"), b"test").unwrap();
        std::fs::write(dir.join("snap-000002.chk"), b"test").unwrap();
        std::fs::write(dir.join("other.txt"), b"test").unwrap();

        let snapshots = list_snapshots(dir).unwrap();
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].0, 1);
        assert_eq!(snapshots[1].0, 2);
        assert_eq!(snapshots[2].0, 3);
    }

    #[test]
    fn test_find_latest_snapshot() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();

        // No snapshots
        assert!(find_latest_snapshot(dir).unwrap().is_none());

        // Add some snapshots
        std::fs::write(dir.join("snap-000001.chk"), b"test").unwrap();
        std::fs::write(dir.join("snap-000005.chk"), b"test").unwrap();
        std::fs::write(dir.join("snap-000003.chk"), b"test").unwrap();

        let (id, _) = find_latest_snapshot(dir).unwrap().unwrap();
        assert_eq!(id, 5);
    }
}
