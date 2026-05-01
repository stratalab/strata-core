//! MANIFEST file format
//!
//! MANIFEST contains physical storage metadata only.
//! It is intentionally minimal to avoid semantic coupling.
//!
//! # Format
//!
//! ```text
//! +------------------+
//! | Magic: "STRM"    | 4 bytes
//! | Format Version   | 4 bytes (u32 LE)
//! | Database UUID    | 16 bytes
//! | Codec ID Length  | 4 bytes (u32 LE)
//! | Codec ID         | variable
//! | Active WAL Seg   | 8 bytes (u64 LE)
//! | Snapshot Watermark | 8 bytes (u64 LE, 0 = none)
//! | Snapshot ID      | 8 bytes (u64 LE, 0 = none)
//! | Flush Watermark  | 8 bytes (u64 LE, 0 = none) [v2+]
//! | CRC32            | 4 bytes
//! +------------------+
//! ```

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use strata_core::id::{CommitVersion, TxnId};

/// MANIFEST magic bytes: "STRM" (0x5354524D)
pub const MANIFEST_MAGIC: [u8; 4] = *b"STRM";

/// Current MANIFEST format version
pub const MANIFEST_FORMAT_VERSION: u32 = 2;

/// Oldest MANIFEST format version this build can read. Files with a
/// `format_version` below this value are rejected with
/// [`ManifestError::LegacyFormat`] — mirroring the WAL clean-break contract
/// at [`crate::durability::format::wal_record::MIN_SUPPORTED_SEGMENT_FORMAT_VERSION`].
/// Operators wipe the `manifest` file and reopen.
///
/// v2 added `flushed_through_commit_id` (required for delta-only WAL replay);
/// no shipped build since that bump persists v1.
pub const MIN_SUPPORTED_MANIFEST_FORMAT_VERSION: u32 = 2;

/// MANIFEST file structure
///
/// Contains physical storage metadata for database recovery:
/// - Database identifier (UUID)
/// - Codec configuration
/// - Active WAL segment
/// - Snapshot watermark
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// Format version for forward compatibility
    pub format_version: u32,
    /// Unique database identifier (generated on creation)
    pub database_uuid: [u8; 16],
    /// Codec identifier (e.g., "identity")
    pub codec_id: String,
    /// Current active WAL segment number
    pub active_wal_segment: u64,
    /// Latest snapshot watermark (if any)
    pub snapshot_watermark: Option<u64>,
    /// Latest snapshot identifier (if any)
    pub snapshot_id: Option<u64>,
    /// Highest commit_id that has been flushed to a KV segment (if any).
    /// Used for WAL truncation and delta-only WAL replay on recovery.
    pub flushed_through_commit_id: Option<u64>,
}

impl Manifest {
    /// Create a new MANIFEST for a fresh database
    pub fn new(database_uuid: [u8; 16], codec_id: String) -> Self {
        Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            database_uuid,
            codec_id,
            active_wal_segment: 1,
            snapshot_watermark: None,
            snapshot_id: None,
            flushed_through_commit_id: None,
        }
    }

    /// Serialize MANIFEST to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Magic
        bytes.extend_from_slice(&MANIFEST_MAGIC);

        // Format version — always write current version (upgrades v1 on persist)
        bytes.extend_from_slice(&MANIFEST_FORMAT_VERSION.to_le_bytes());

        // Database UUID
        bytes.extend_from_slice(&self.database_uuid);

        // Codec ID (length-prefixed)
        bytes.extend_from_slice(&(self.codec_id.len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.codec_id.as_bytes());

        // Active WAL segment
        bytes.extend_from_slice(&self.active_wal_segment.to_le_bytes());

        // Snapshot watermark (0 = none)
        let watermark = self.snapshot_watermark.unwrap_or(0);
        bytes.extend_from_slice(&watermark.to_le_bytes());

        // Snapshot ID (0 = none)
        let snapshot_id = self.snapshot_id.unwrap_or(0);
        bytes.extend_from_slice(&snapshot_id.to_le_bytes());

        // Flushed-through commit ID (0 = none) — v2+
        let flushed = self.flushed_through_commit_id.unwrap_or(0);
        bytes.extend_from_slice(&flushed.to_le_bytes());

        // CRC32 of all preceding bytes
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());

        bytes
    }

    /// Deserialize MANIFEST from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ManifestError> {
        // Minimum size (v2, empty codec): magic(4) + version(4) + uuid(16) +
        // codec_len(4) + wal_seg(8) + watermark(8) + snap_id(8) +
        // flushed_through_commit_id(8) + crc(4) = 64.
        if bytes.len() < 64 {
            return Err(ManifestError::TooShort);
        }

        // Check magic
        if bytes[0..4] != MANIFEST_MAGIC {
            return Err(ManifestError::InvalidMagic);
        }

        // Version check runs before CRC so a legacy-format MANIFEST produces
        // the typed `LegacyFormat` diagnostic the operator can act on,
        // rather than a generic `ChecksumMismatch` when the v1 layout CRC
        // disagrees with newer assumptions. Parity with WAL
        // `SegmentHeader::from_bytes_slice` (version before CRC).
        let format_version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if format_version < MIN_SUPPORTED_MANIFEST_FORMAT_VERSION {
            return Err(ManifestError::LegacyFormat {
                detected_version: format_version,
                supported_range: format!(
                    "this build requires MANIFEST format version {MIN_SUPPORTED_MANIFEST_FORMAT_VERSION}"
                ),
                remediation: "delete the `MANIFEST` file and reopen \
                              — the database will rebuild its metadata \
                              from the `wal/` and `segments/` directories."
                    .to_string(),
            });
        }
        if format_version > MANIFEST_FORMAT_VERSION {
            return Err(ManifestError::UnsupportedVersion {
                version: format_version,
                max_supported: MANIFEST_FORMAT_VERSION,
            });
        }

        // Verify CRC (last 4 bytes)
        let data = &bytes[..bytes.len() - 4];
        let stored_crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        let computed_crc = crc32fast::hash(data);
        if stored_crc != computed_crc {
            return Err(ManifestError::ChecksumMismatch {
                expected: stored_crc,
                computed: computed_crc,
            });
        }

        let mut cursor = 8;

        // Database UUID
        let database_uuid: [u8; 16] = bytes[cursor..cursor + 16].try_into().unwrap();
        cursor += 16;

        // Codec ID
        let codec_id_len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        if cursor + codec_id_len > bytes.len() - 4 {
            return Err(ManifestError::TooShort);
        }

        let codec_id = String::from_utf8(bytes[cursor..cursor + codec_id_len].to_vec())
            .map_err(|_| ManifestError::InvalidCodecId)?;
        cursor += codec_id_len;

        // Active WAL segment
        let active_wal_segment = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        // Snapshot watermark
        let watermark = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let snapshot_watermark = if watermark > 0 { Some(watermark) } else { None };

        // Snapshot ID
        let snapshot_id_val = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let snapshot_id = if snapshot_id_val > 0 {
            Some(snapshot_id_val)
        } else {
            None
        };

        // Flushed-through commit ID
        if cursor + 8 > bytes.len() - 4 {
            return Err(ManifestError::TooShort);
        }
        let flushed_val = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        let flushed_through_commit_id = if flushed_val > 0 {
            Some(flushed_val)
        } else {
            None
        };

        Ok(Manifest {
            format_version,
            database_uuid,
            codec_id,
            active_wal_segment,
            snapshot_watermark,
            snapshot_id,
            flushed_through_commit_id,
        })
    }
}

/// MANIFEST persistence manager
///
/// Handles atomic MANIFEST persistence using write-fsync-rename pattern.
pub struct ManifestManager {
    path: PathBuf,
    manifest: Manifest,
}

impl ManifestManager {
    /// Create a new MANIFEST manager (for new database)
    pub fn create(
        path: PathBuf,
        database_uuid: [u8; 16],
        codec_id: String,
    ) -> Result<Self, ManifestError> {
        let manifest = Manifest::new(database_uuid, codec_id);
        let manager = ManifestManager { path, manifest };
        manager.persist()?;
        Ok(manager)
    }

    /// Load existing MANIFEST
    pub fn load(path: PathBuf) -> Result<Self, ManifestError> {
        let bytes = std::fs::read(&path)?;
        let manifest = Manifest::from_bytes(&bytes)?;
        Ok(ManifestManager { path, manifest })
    }

    /// Check if a MANIFEST exists at the given path
    pub fn exists(path: &Path) -> bool {
        path.exists()
    }

    /// Persist MANIFEST atomically (write-fsync-rename)
    pub fn persist(&self) -> Result<(), ManifestError> {
        let temp_path = self.path.with_extension("tmp");

        // Write to temp file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;

        file.write_all(&self.manifest.to_bytes())?;
        file.sync_all()?;
        drop(file);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o600));
        }

        // Atomic rename
        std::fs::rename(&temp_path, &self.path)?;

        // Sync parent directory
        if let Some(parent) = self.path.parent() {
            if parent.exists() {
                let dir = File::open(parent)?;
                dir.sync_all()?;
            }
        }

        Ok(())
    }

    /// Get the MANIFEST path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the current manifest
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Get mutable access to the manifest (for updates)
    pub fn manifest_mut(&mut self) -> &mut Manifest {
        &mut self.manifest
    }

    /// Update active WAL segment and persist
    pub fn set_active_segment(&mut self, segment: u64) -> Result<(), ManifestError> {
        self.manifest.active_wal_segment = segment;
        self.persist()
    }

    /// Update snapshot watermark and persist
    pub fn set_snapshot_watermark(
        &mut self,
        snapshot_id: u64,
        watermark_txn: TxnId,
    ) -> Result<(), ManifestError> {
        self.manifest.snapshot_id = Some(snapshot_id);
        self.manifest.snapshot_watermark = Some(watermark_txn.as_u64());
        self.persist()
    }

    /// Update the flush watermark (highest commit_id flushed to segments) and persist.
    pub fn set_flush_watermark(&mut self, commit_id: CommitVersion) -> Result<(), ManifestError> {
        self.manifest.flushed_through_commit_id = Some(commit_id.as_u64());
        self.persist()
    }

    /// Clear snapshot info (for testing/reset)
    pub fn clear_snapshot(&mut self) -> Result<(), ManifestError> {
        self.manifest.snapshot_id = None;
        self.manifest.snapshot_watermark = None;
        self.persist()
    }
}

/// Errors that can occur with MANIFEST operations
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// MANIFEST file too short
    #[error("MANIFEST too short")]
    TooShort,

    /// Invalid magic bytes
    #[error("Invalid magic bytes")]
    InvalidMagic,

    /// Unsupported format version (created by a newer version of Strata)
    #[error(
        "unsupported MANIFEST format version {version} (this build supports up to {max_supported})"
    )]
    UnsupportedVersion {
        /// Version found in the file
        version: u32,
        /// Maximum version this build can read
        max_supported: u32,
    },

    /// Legacy MANIFEST format — rejected hard, even under lossy recovery.
    ///
    /// Produced when a file's `format_version` is older than
    /// [`MIN_SUPPORTED_MANIFEST_FORMAT_VERSION`]. Surfaces unconditionally
    /// (strict and lossy open alike); the engine's open path re-raises as
    /// [`strata_core::StrataError::LegacyFormat`] and the operator must
    /// delete the file manually before reopening. Mirrors the WAL
    /// `SegmentHeaderError::LegacyFormat` contract.
    #[error(
        "legacy MANIFEST format: found version {detected_version}. {supported_range}. {remediation}"
    )]
    LegacyFormat {
        /// Format version read from disk.
        detected_version: u32,
        /// Operator-facing description of the range this build accepts.
        supported_range: String,
        /// Operator remediation hint — filesystem action only.
        remediation: String,
    },

    /// Invalid codec ID (not valid UTF-8)
    #[error("Invalid codec ID")]
    InvalidCodecId,

    /// Checksum mismatch
    #[error("Checksum mismatch: expected {expected:08x}, computed {computed:08x}")]
    ChecksumMismatch {
        /// Expected CRC32 value (from file)
        expected: u32,
        /// Computed CRC32 value
        computed: u32,
    },

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_uuid() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    #[test]
    fn test_manifest_magic() {
        assert_eq!(MANIFEST_MAGIC, *b"STRM");
        assert_eq!(
            u32::from_be_bytes(MANIFEST_MAGIC),
            0x5354524D,
            "Magic should be 0x5354524D"
        );
    }

    #[test]
    fn test_manifest_new() {
        let manifest = Manifest::new(test_uuid(), "identity".to_string());

        assert_eq!(manifest.format_version, MANIFEST_FORMAT_VERSION);
        assert_eq!(manifest.database_uuid, test_uuid());
        assert_eq!(manifest.codec_id, "identity");
        assert_eq!(manifest.active_wal_segment, 1);
        assert_eq!(manifest.snapshot_watermark, None);
        assert_eq!(manifest.snapshot_id, None);
    }

    #[test]
    fn test_manifest_roundtrip() {
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            database_uuid: test_uuid(),
            codec_id: "identity".to_string(),
            active_wal_segment: 42,
            snapshot_watermark: Some(1000),
            snapshot_id: Some(5),
            flushed_through_commit_id: None,
        };

        let bytes = manifest.to_bytes();
        let parsed = Manifest::from_bytes(&bytes).unwrap();

        assert_eq!(manifest, parsed);
    }

    #[test]
    fn test_manifest_no_snapshot() {
        let manifest = Manifest::new(test_uuid(), "identity".to_string());

        let bytes = manifest.to_bytes();
        let parsed = Manifest::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.snapshot_watermark, None);
        assert_eq!(parsed.snapshot_id, None);
    }

    #[test]
    fn test_manifest_with_snapshot() {
        let mut manifest = Manifest::new(test_uuid(), "identity".to_string());
        manifest.snapshot_watermark = Some(500);
        manifest.snapshot_id = Some(3);
        manifest.active_wal_segment = 10;

        let bytes = manifest.to_bytes();
        let parsed = Manifest::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.snapshot_watermark, Some(500));
        assert_eq!(parsed.snapshot_id, Some(3));
        assert_eq!(parsed.active_wal_segment, 10);
    }

    #[test]
    fn test_manifest_invalid_magic() {
        let mut bytes = Manifest::new(test_uuid(), "identity".to_string()).to_bytes();
        bytes[0..4].copy_from_slice(b"XXXX");

        let result = Manifest::from_bytes(&bytes);
        assert!(matches!(result, Err(ManifestError::InvalidMagic)));
    }

    #[test]
    fn test_manifest_checksum_mismatch() {
        let mut bytes = Manifest::new(test_uuid(), "identity".to_string()).to_bytes();
        // Corrupt a byte in the middle
        bytes[10] ^= 0xFF;

        let result = Manifest::from_bytes(&bytes);
        assert!(matches!(
            result,
            Err(ManifestError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn test_manifest_too_short() {
        let result = Manifest::from_bytes(&[0u8; 10]);
        assert!(matches!(result, Err(ManifestError::TooShort)));
    }

    #[test]
    fn test_manifest_empty_codec() {
        let manifest = Manifest::new(test_uuid(), String::new());

        let bytes = manifest.to_bytes();
        let parsed = Manifest::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.codec_id, "");
    }

    #[test]
    fn test_manifest_long_codec() {
        let long_codec = "a".repeat(1000);
        let manifest = Manifest::new(test_uuid(), long_codec.clone());

        let bytes = manifest.to_bytes();
        let parsed = Manifest::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.codec_id, long_codec);
    }

    #[test]
    fn test_manifest_manager_create_and_load() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");

        // Create
        let manager =
            ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
                .unwrap();

        assert_eq!(manager.manifest().codec_id, "identity");
        assert_eq!(manager.manifest().active_wal_segment, 1);

        // Load
        let loaded = ManifestManager::load(manifest_path).unwrap();
        assert_eq!(loaded.manifest().codec_id, "identity");
        assert_eq!(loaded.manifest().database_uuid, test_uuid());
    }

    #[test]
    fn test_manifest_manager_update_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");

        let mut manager =
            ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
                .unwrap();

        manager.set_active_segment(5).unwrap();
        assert_eq!(manager.manifest().active_wal_segment, 5);

        // Reload and verify persistence
        let loaded = ManifestManager::load(manifest_path).unwrap();
        assert_eq!(loaded.manifest().active_wal_segment, 5);
    }

    #[test]
    fn test_manifest_manager_update_watermark() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");

        let mut manager =
            ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
                .unwrap();

        manager.set_snapshot_watermark(3, TxnId(1000)).unwrap();

        assert_eq!(manager.manifest().snapshot_id, Some(3));
        assert_eq!(manager.manifest().snapshot_watermark, Some(1000));

        // Reload and verify
        let loaded = ManifestManager::load(manifest_path).unwrap();
        assert_eq!(loaded.manifest().snapshot_id, Some(3));
        assert_eq!(loaded.manifest().snapshot_watermark, Some(1000));
    }

    #[test]
    fn test_manifest_manager_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");

        assert!(!ManifestManager::exists(&manifest_path));

        ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
            .unwrap();

        assert!(ManifestManager::exists(&manifest_path));
    }

    #[test]
    fn test_manifest_atomic_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");

        // Create initial
        let manager =
            ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
                .unwrap();

        // Verify no temp file exists after successful write
        let temp_path = manifest_path.with_extension("tmp");
        assert!(!temp_path.exists());

        // File should exist
        assert!(manifest_path.exists());

        // Should be loadable
        let _ = ManifestManager::load(manifest_path).unwrap();

        drop(manager);
    }

    #[test]
    fn test_manifest_v2_round_trip_with_flush_watermark() {
        let mut manifest = Manifest::new(test_uuid(), "identity".to_string());
        manifest.flushed_through_commit_id = Some(42);

        let bytes = manifest.to_bytes();
        let parsed = Manifest::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.flushed_through_commit_id, Some(42));
        assert_eq!(parsed.format_version, 2);
        assert_eq!(manifest, parsed);
    }

    #[test]
    fn test_manifest_v1_rejected_as_legacy() {
        // Build a v1-format manifest (no flush watermark field). Under the
        // D5 cutover this is rejected with `LegacyFormat` rather than
        // silently read as v1 with a defaulted flush watermark.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MANIFEST_MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes()); // format_version = 1
        bytes.extend_from_slice(&test_uuid());
        let codec = b"identity";
        bytes.extend_from_slice(&(codec.len() as u32).to_le_bytes());
        bytes.extend_from_slice(codec);
        bytes.extend_from_slice(&1u64.to_le_bytes()); // active_wal_segment
        bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_watermark
        bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_id
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());

        match Manifest::from_bytes(&bytes) {
            Err(ManifestError::LegacyFormat {
                detected_version,
                supported_range,
                remediation,
            }) => {
                assert_eq!(detected_version, 1);
                assert!(
                    supported_range.contains(&MIN_SUPPORTED_MANIFEST_FORMAT_VERSION.to_string()),
                    "supported_range must name the required version, got: {supported_range}"
                );
                assert!(
                    remediation.contains("MANIFEST"),
                    "remediation must name the MANIFEST file, got: {remediation}"
                );
            }
            other => panic!("expected LegacyFormat, got: {:?}", other),
        }
    }

    #[test]
    fn test_manifest_legacy_detected_before_crc() {
        // Craft v1 bytes with a deliberately bad CRC. The version check
        // runs first, so the caller sees `LegacyFormat` — not the generic
        // `ChecksumMismatch` a naive ordering would produce.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MANIFEST_MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&test_uuid());
        let codec = b"identity";
        bytes.extend_from_slice(&(codec.len() as u32).to_le_bytes());
        bytes.extend_from_slice(codec);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0xDEADBEEFu32.to_le_bytes()); // bogus CRC

        assert!(matches!(
            Manifest::from_bytes(&bytes),
            Err(ManifestError::LegacyFormat {
                detected_version: 1,
                ..
            })
        ));
    }

    #[test]
    fn test_manifest_manager_set_flush_watermark() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");

        let mut manager =
            ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
                .unwrap();

        manager.set_flush_watermark(CommitVersion(500)).unwrap();
        assert_eq!(manager.manifest().flushed_through_commit_id, Some(500));

        // Reload and verify persistence
        let loaded = ManifestManager::load(manifest_path).unwrap();
        assert_eq!(loaded.manifest().flushed_through_commit_id, Some(500));
    }

    #[test]
    fn test_manifest_new_has_no_flush_watermark() {
        let manifest = Manifest::new(test_uuid(), "identity".to_string());
        assert_eq!(manifest.flushed_through_commit_id, None);
    }

    #[test]
    fn test_manifest_clear_snapshot() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");

        let mut manager =
            ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
                .unwrap();

        // Set snapshot
        manager.set_snapshot_watermark(5, TxnId(2000)).unwrap();
        assert_eq!(manager.manifest().snapshot_id, Some(5));

        // Clear snapshot
        manager.clear_snapshot().unwrap();
        assert_eq!(manager.manifest().snapshot_id, None);
        assert_eq!(manager.manifest().snapshot_watermark, None);

        // Verify persistence
        let loaded = ManifestManager::load(manifest_path).unwrap();
        assert_eq!(loaded.manifest().snapshot_id, None);
    }

    #[test]
    fn test_manifest_rejects_future_version() {
        // Build a manifest with format_version = 99 (far beyond current)
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MANIFEST_MAGIC);
        bytes.extend_from_slice(&99u32.to_le_bytes()); // future version
        bytes.extend_from_slice(&test_uuid());
        let codec = b"identity";
        bytes.extend_from_slice(&(codec.len() as u32).to_le_bytes());
        bytes.extend_from_slice(codec);
        bytes.extend_from_slice(&1u64.to_le_bytes()); // active_wal_segment
        bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_watermark
        bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_id
        bytes.extend_from_slice(&0u64.to_le_bytes()); // flushed_through
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());

        let result = Manifest::from_bytes(&bytes);
        assert!(
            matches!(
                result,
                Err(ManifestError::UnsupportedVersion {
                    version: 99,
                    max_supported: 2
                })
            ),
            "Expected UnsupportedVersion error for future format version, got: {:?}",
            result,
        );
    }
}
