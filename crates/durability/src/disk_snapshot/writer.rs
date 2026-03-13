//! Crash-safe snapshot writer
//!
//! Uses write-fsync-rename pattern for atomic snapshot creation.
//!
//! # Crash Safety
//!
//! The snapshot creation follows this pattern:
//! 1. Write to temporary file (.snap-NNNNNN.tmp)
//! 2. fsync the temporary file
//! 3. Atomic rename to final path (snap-NNNNNN.chk)
//! 4. fsync the parent directory
//!
//! This ensures that either the complete snapshot exists or it doesn't -
//! there's no possibility of a partial snapshot being visible.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::codec::StorageCodec;
use crate::format::snapshot::{snapshot_path, SectionHeader, SnapshotHeader};

#[cfg(test)]
use crate::format::snapshot::SNAPSHOT_FORMAT_VERSION;

/// Snapshot writer with crash-safe semantics
pub struct SnapshotWriter {
    snapshots_dir: PathBuf,
    codec: Box<dyn StorageCodec>,
    database_uuid: [u8; 16],
}

impl SnapshotWriter {
    /// Create a new snapshot writer
    ///
    /// Creates the snapshots directory if it doesn't exist.
    pub fn new(
        snapshots_dir: PathBuf,
        codec: Box<dyn StorageCodec>,
        database_uuid: [u8; 16],
    ) -> io::Result<Self> {
        std::fs::create_dir_all(&snapshots_dir)?;
        Ok(SnapshotWriter {
            snapshots_dir,
            codec,
            database_uuid,
        })
    }

    /// Get the snapshots directory
    pub fn snapshots_dir(&self) -> &Path {
        &self.snapshots_dir
    }

    /// Create a snapshot using crash-safe write pattern
    ///
    /// The process:
    /// 1. Write header, codec ID, sections, and footer CRC to temporary file
    /// 2. fsync temporary file
    /// 3. Atomic rename to final path
    /// 4. fsync parent directory
    ///
    /// Returns snapshot info on success.
    pub fn create_snapshot(
        &self,
        snapshot_id: u64,
        watermark_txn: u64,
        sections: Vec<SnapshotSection>,
    ) -> io::Result<SnapshotInfo> {
        let final_path = snapshot_path(&self.snapshots_dir, snapshot_id);
        let temp_path = self
            .snapshots_dir
            .join(format!(".snap-{:06}.tmp", snapshot_id));

        // Step 1: Write to temporary file
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let codec_id = self.codec.codec_id();
        let header = SnapshotHeader::new(
            snapshot_id,
            watermark_txn,
            created_at,
            self.database_uuid,
            codec_id.len() as u8,
        );

        // Write header
        file.write_all(&header.to_bytes())?;

        // Write codec ID
        file.write_all(codec_id.as_bytes())?;

        // Track all bytes for CRC
        let mut all_bytes = header.to_bytes().to_vec();
        all_bytes.extend_from_slice(codec_id.as_bytes());

        // Write sections
        for section in &sections {
            let section_header =
                SectionHeader::new(section.primitive_type, section.data.len() as u64);
            let section_header_bytes = section_header.to_bytes();
            file.write_all(&section_header_bytes)?;
            file.write_all(&section.data)?;

            all_bytes.extend_from_slice(&section_header_bytes);
            all_bytes.extend_from_slice(&section.data);
        }

        // Write footer CRC32
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&all_bytes);
        let crc = hasher.finalize();
        file.write_all(&crc.to_le_bytes())?;

        // Step 2: fsync the file
        file.sync_all()?;
        drop(file);

        // Step 3: Atomic rename
        std::fs::rename(&temp_path, &final_path)?;

        // Step 4: fsync parent directory (with single retry)
        fsync_directory(&self.snapshots_dir)?;

        Ok(SnapshotInfo {
            snapshot_id,
            watermark_txn,
            timestamp: created_at,
            path: final_path,
            crc,
        })
    }

    /// Clean up incomplete temporary files
    ///
    /// This should be called during recovery to remove any
    /// temporary snapshot files left behind by crashes.
    pub fn cleanup_temp_files(&self) -> io::Result<usize> {
        let mut count = 0;

        if !self.snapshots_dir.exists() {
            return Ok(0);
        }

        for entry in std::fs::read_dir(&self.snapshots_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(".snap-") && name.ends_with(".tmp") {
                std::fs::remove_file(entry.path())?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// Check if a temporary file exists for a given snapshot ID
    pub fn temp_file_exists(&self, snapshot_id: u64) -> bool {
        let temp_path = self
            .snapshots_dir
            .join(format!(".snap-{:06}.tmp", snapshot_id));
        temp_path.exists()
    }
}

/// Snapshot section data
#[derive(Debug, Clone)]
pub struct SnapshotSection {
    /// Primitive type tag (from primitive_tags module)
    pub primitive_type: u8,
    /// Serialized section data
    pub data: Vec<u8>,
}

impl SnapshotSection {
    /// Create a new snapshot section
    pub fn new(primitive_type: u8, data: Vec<u8>) -> Self {
        SnapshotSection {
            primitive_type,
            data,
        }
    }
}

/// Information about a created snapshot
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Snapshot identifier
    pub snapshot_id: u64,
    /// Watermark transaction ID (all txns <= this are included)
    pub watermark_txn: u64,
    /// Creation timestamp (microseconds since epoch)
    pub timestamp: u64,
    /// Path to the snapshot file
    pub path: PathBuf,
    /// CRC32 checksum of the snapshot contents
    pub crc: u32,
}

/// fsync a directory to ensure durability of rename operations.
/// Retries once on transient failure.
fn fsync_directory(dir_path: &Path) -> io::Result<()> {
    let do_sync = || -> io::Result<()> {
        let dir = File::open(dir_path)?;
        dir.sync_all()
    };
    match do_sync() {
        Ok(()) => Ok(()),
        Err(first_err) => {
            tracing::warn!(target: "strata::snapshot",
                error = %first_err, path = %dir_path.display(),
                "Directory fsync failed, retrying once");
            do_sync()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::IdentityCodec;
    use crate::format::primitive_tags;

    fn test_uuid() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    #[test]
    fn test_create_snapshot_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![
            SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0]), // Empty KV section
        ];

        let info = writer.create_snapshot(1, 100, sections).unwrap();

        assert_eq!(info.snapshot_id, 1);
        assert_eq!(info.watermark_txn, 100);
        assert!(info.path.exists());
        assert!(info.path.to_string_lossy().contains("snap-000001.chk"));
    }

    #[test]
    fn test_snapshot_file_format() {
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

        // Read and verify the file
        let data = std::fs::read(&info.path).unwrap();

        // Verify magic
        assert_eq!(&data[0..4], b"SNAP");

        // Verify format version
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        assert_eq!(version, SNAPSHOT_FORMAT_VERSION);

        // Verify snapshot ID
        let snapshot_id = u64::from_le_bytes(data[8..16].try_into().unwrap());
        assert_eq!(snapshot_id, 1);

        // Verify watermark
        let watermark = u64::from_le_bytes(data[16..24].try_into().unwrap());
        assert_eq!(watermark, 100);
    }

    #[test]
    fn test_no_temp_file_after_success() {
        let temp_dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])];

        writer.create_snapshot(1, 100, sections).unwrap();

        // Temporary file should not exist
        assert!(!writer.temp_file_exists(1));
    }

    #[test]
    fn test_cleanup_temp_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        // Create some temp files manually
        std::fs::write(temp_dir.path().join(".snap-000001.tmp"), b"test").unwrap();
        std::fs::write(temp_dir.path().join(".snap-000002.tmp"), b"test").unwrap();
        std::fs::write(temp_dir.path().join("snap-000003.chk"), b"test").unwrap();

        let count = writer.cleanup_temp_files().unwrap();

        assert_eq!(count, 2);
        assert!(!temp_dir.path().join(".snap-000001.tmp").exists());
        assert!(!temp_dir.path().join(".snap-000002.tmp").exists());
        assert!(temp_dir.path().join("snap-000003.chk").exists());
    }

    #[test]
    fn test_multiple_snapshots() {
        let temp_dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        // Create multiple snapshots
        for i in 1..=5 {
            let sections = vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])];
            let info = writer.create_snapshot(i, i * 100, sections).unwrap();
            assert_eq!(info.snapshot_id, i);
        }

        // All snapshots should exist
        for i in 1..=5 {
            let path = snapshot_path(temp_dir.path(), i);
            assert!(path.exists(), "Snapshot {} should exist", i);
        }
    }

    #[test]
    fn test_snapshot_with_all_sections() {
        let temp_dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        // Create snapshot with all 6 primitive types
        let sections = vec![
            SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::EVENT, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::STATE, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::BRANCH, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::JSON, vec![0, 0, 0, 0]),
            SnapshotSection::new(primitive_tags::VECTOR, vec![0, 0, 0, 0]),
        ];

        let info = writer.create_snapshot(1, 100, sections).unwrap();
        assert!(info.path.exists());
    }

    #[test]
    fn test_snapshot_crc() {
        let temp_dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let sections = vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])];

        let info = writer.create_snapshot(1, 100, sections).unwrap();

        // Read the file and verify CRC
        let data = std::fs::read(&info.path).unwrap();

        // CRC is the last 4 bytes
        let stored_crc = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());

        // Compute CRC of everything except the last 4 bytes
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data[..data.len() - 4]);
        let computed_crc = hasher.finalize();

        assert_eq!(stored_crc, computed_crc);
        assert_eq!(info.crc, computed_crc);
    }
}
