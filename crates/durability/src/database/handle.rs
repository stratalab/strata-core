//! Database storage handle
//!
//! The `DatabaseHandle` coordinates the storage layer components:
//! - MANIFEST for metadata
//! - WAL for durability
//! - Snapshots for point-in-time persistence
//! - Recovery for crash recovery
//!
//! This is a lower-level API used by the engine layer.

use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;

use crate::codec::{clone_codec, get_codec, StorageCodec};
use crate::disk_snapshot::{CheckpointCoordinator, CheckpointData};
use crate::format::{CheckpointInfo, Manifest, ManifestManager, SnapshotWatermark, WalRecord};
use crate::recovery::{RecoveryCoordinator, RecoveryError, RecoveryResult, RecoverySnapshot};
use crate::wal::WalWriter;

use super::config::DatabaseConfig;
use super::paths::{DatabasePathError, DatabasePaths};

/// Database storage handle
///
/// Coordinates storage layer components for a database.
/// This is used by the engine layer to manage persistence.
pub struct DatabaseHandle {
    /// Database paths
    paths: DatabasePaths,
    /// MANIFEST manager
    manifest: Arc<Mutex<ManifestManager>>,
    /// WAL writer
    wal_writer: Arc<Mutex<WalWriter>>,
    /// Checkpoint coordinator
    checkpoint_coordinator: Arc<Mutex<CheckpointCoordinator>>,
    /// Codec
    codec: Box<dyn StorageCodec>,
    /// Configuration
    config: DatabaseConfig,
    /// Database UUID
    database_uuid: [u8; 16],
}

impl DatabaseHandle {
    /// Create a new database at the specified path
    ///
    /// Creates the directory structure and initializes all components.
    pub fn create(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<Self, DatabaseHandleError> {
        let paths = DatabasePaths::from_root(path);

        if paths.exists() {
            return Err(DatabaseHandleError::AlreadyExists {
                path: paths.root().to_path_buf(),
            });
        }

        // Validate config
        config.validate()?;

        // Create directory structure
        paths.create_directories()?;

        // Generate UUID
        let database_uuid = generate_uuid();

        // Get codec
        let codec = get_codec(&config.codec_id)?;

        // Create MANIFEST
        let manifest =
            ManifestManager::create(paths.manifest(), database_uuid, config.codec_id.clone())?;

        // Create WAL writer
        let wal_writer = WalWriter::new(
            paths.wal_dir(),
            database_uuid,
            config.durability,
            config.wal_config.clone(),
            clone_codec(codec.as_ref()),
        )?;

        // Create checkpoint coordinator
        let checkpoint_coordinator = CheckpointCoordinator::new(
            paths.snapshots_dir(),
            clone_codec(codec.as_ref()),
            database_uuid,
        )?;

        Ok(DatabaseHandle {
            paths,
            manifest: Arc::new(Mutex::new(manifest)),
            wal_writer: Arc::new(Mutex::new(wal_writer)),
            checkpoint_coordinator: Arc::new(Mutex::new(checkpoint_coordinator)),
            codec,
            config,
            database_uuid,
        })
    }

    /// Open an existing database at the specified path
    ///
    /// Validates the database and prepares for operation.
    /// Note: Recovery should be performed separately using `recover()`.
    pub fn open(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<Self, DatabaseHandleError> {
        let paths = DatabasePaths::from_root(path);
        paths.validate()?;

        // Validate config
        config.validate()?;

        // Load manifest
        let manifest = ManifestManager::load(paths.manifest())?;
        let manifest_data = manifest.manifest();

        // Validate codec matches
        if manifest_data.codec_id != config.codec_id {
            return Err(DatabaseHandleError::CodecMismatch {
                database: manifest_data.codec_id.clone(),
                config: config.codec_id.clone(),
            });
        }

        let database_uuid = manifest_data.database_uuid;
        let codec = get_codec(&config.codec_id)?;

        // Create WAL writer (continues from active segment)
        let wal_writer = WalWriter::new(
            paths.wal_dir(),
            database_uuid,
            config.durability,
            config.wal_config.clone(),
            clone_codec(codec.as_ref()),
        )?;

        // Create checkpoint coordinator with existing watermark
        let watermark = if let (Some(snap_id), Some(watermark_txn), Some(ts)) = (
            manifest_data.snapshot_id,
            manifest_data.snapshot_watermark,
            Some(0u64), // We don't have the timestamp stored, use 0
        ) {
            SnapshotWatermark::with_values(snap_id, watermark_txn, ts)
        } else {
            SnapshotWatermark::new()
        };

        let checkpoint_coordinator = CheckpointCoordinator::with_watermark(
            paths.snapshots_dir(),
            clone_codec(codec.as_ref()),
            database_uuid,
            watermark,
        )?;

        Ok(DatabaseHandle {
            paths,
            manifest: Arc::new(Mutex::new(manifest)),
            wal_writer: Arc::new(Mutex::new(wal_writer)),
            checkpoint_coordinator: Arc::new(Mutex::new(checkpoint_coordinator)),
            codec,
            config,
            database_uuid,
        })
    }

    /// Open or create a database
    pub fn open_or_create(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<Self, DatabaseHandleError> {
        let paths = DatabasePaths::from_root(path.as_ref());

        if paths.exists() {
            Self::open(path, config)
        } else {
            Self::create(path, config)
        }
    }

    /// Perform recovery and return recovery information
    ///
    /// This should be called after `open()` to replay WAL records.
    /// The callback receives each WAL record for the caller to apply.
    pub fn recover<S, R>(
        &self,
        on_snapshot: S,
        on_record: R,
    ) -> Result<RecoveryResult, RecoveryError>
    where
        S: FnMut(RecoverySnapshot) -> Result<(), RecoveryError>,
        R: FnMut(&WalRecord) -> Result<(), RecoveryError>,
    {
        let coordinator = RecoveryCoordinator::new(
            self.paths.root().to_path_buf(),
            clone_codec(self.codec.as_ref()),
        );

        coordinator.recover(on_snapshot, on_record)
    }

    /// Append a WAL record
    pub fn append_wal(&self, record: &WalRecord) -> Result<(), DatabaseHandleError> {
        let mut wal = self.wal_writer.lock();
        wal.append(record)?;
        Ok(())
    }

    /// Flush WAL to disk
    pub fn flush_wal(&self) -> Result<(), DatabaseHandleError> {
        let mut wal = self.wal_writer.lock();
        wal.flush()?;
        Ok(())
    }

    /// Create a checkpoint with the given primitive data
    pub fn checkpoint(
        &self,
        watermark_txn: u64,
        data: CheckpointData,
    ) -> Result<CheckpointInfo, DatabaseHandleError> {
        // Create checkpoint
        let info = {
            let mut coordinator = self.checkpoint_coordinator.lock();
            coordinator.checkpoint(watermark_txn, data)?
        };

        // Update MANIFEST
        {
            let mut manifest = self.manifest.lock();
            manifest.set_snapshot_watermark(info.snapshot_id, info.watermark_txn)?;
        }

        Ok(info)
    }

    /// Close the database handle
    ///
    /// Flushes WAL and updates MANIFEST.
    pub fn close(self) -> Result<(), DatabaseHandleError> {
        // Flush WAL
        {
            let mut wal = self.wal_writer.lock();
            wal.flush()?;
        }

        // Update MANIFEST with current segment
        {
            let manifest = self.manifest.lock();
            // MANIFEST is already up to date from last operation
            drop(manifest);
        }

        Ok(())
    }

    /// Get the database UUID
    pub fn uuid(&self) -> [u8; 16] {
        self.database_uuid
    }

    /// Get the database path
    pub fn path(&self) -> &Path {
        self.paths.root()
    }

    /// Get the database paths
    pub fn paths(&self) -> &DatabasePaths {
        &self.paths
    }

    /// Get the database configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Get the current manifest
    pub fn manifest(&self) -> Manifest {
        let manifest = self.manifest.lock();
        manifest.manifest().clone()
    }

    /// Get the current watermark
    pub fn watermark(&self) -> Option<u64> {
        let coordinator = self.checkpoint_coordinator.lock();
        coordinator.watermark().watermark_txn()
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        // Best-effort cleanup on drop - log errors but don't panic
        let mut wal = self.wal_writer.lock();
        if let Err(e) = wal.flush() {
            warn!(
                target: "strata::wal",
                error = %e,
                path = %self.paths.wal_dir().display(),
                "DatabaseHandle WAL flush on drop failed - data may not be durable"
            );
        }
    }
}

/// Database handle errors
#[derive(Debug, thiserror::Error)]
pub enum DatabaseHandleError {
    /// Database already exists
    #[error("Database already exists at {path}")]
    AlreadyExists {
        /// Path where database exists
        path: PathBuf,
    },

    /// Database not found
    #[error("Database not found: {0}")]
    NotFound(#[from] DatabasePathError),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(#[from] super::config::ConfigError),

    /// Codec mismatch
    #[error("Codec mismatch: database uses {database}, config specifies {config}")]
    CodecMismatch {
        /// Codec ID in database
        database: String,
        /// Codec ID in config
        config: String,
    },

    /// Codec error
    #[error("Codec error: {0}")]
    Codec(#[from] crate::codec::CodecError),

    /// MANIFEST error
    #[error("MANIFEST error: {0}")]
    Manifest(#[from] crate::format::ManifestError),

    /// WAL error
    #[error("WAL error: {0}")]
    Wal(#[from] std::io::Error),

    /// Checkpoint error
    #[error("Checkpoint error: {0}")]
    Checkpoint(#[from] crate::disk_snapshot::CheckpointError),
}

/// Generate a UUID for a new database
fn generate_uuid() -> [u8; 16] {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let mut uuid = [0u8; 16];

    // Use timestamp for first 8 bytes
    let ts_bytes = now.as_nanos().to_le_bytes();
    uuid[0..8].copy_from_slice(&ts_bytes[0..8]);

    // Use random-ish data for remaining bytes
    // In production, use proper random generation
    let random_seed = (now.as_nanos() as u64).wrapping_mul(0x517cc1b727220a95);
    uuid[8..16].copy_from_slice(&random_seed.to_le_bytes());

    uuid
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();

        assert!(db_path.exists());
        assert!(db_path.join("MANIFEST").exists());
        assert!(db_path.join("WAL").exists());
        assert!(db_path.join("SNAPSHOTS").exists());
        assert_eq!(handle.config().codec_id, "identity");

        drop(handle);
    }

    #[test]
    fn test_create_already_exists() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let _handle1 = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();

        let result = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing());
        assert!(matches!(
            result,
            Err(DatabaseHandleError::AlreadyExists { .. })
        ));
    }

    #[test]
    fn test_open_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create first
        {
            let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();
            handle.close().unwrap();
        }

        // Then open
        let handle = DatabaseHandle::open(&db_path, DatabaseConfig::for_testing()).unwrap();
        assert_eq!(handle.config().codec_id, "identity");
    }

    #[test]
    fn test_open_not_found() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("nonexistent.db");

        let result = DatabaseHandle::open(&db_path, DatabaseConfig::for_testing());
        assert!(matches!(result, Err(DatabaseHandleError::NotFound(_))));
    }

    #[test]
    fn test_open_or_create_new() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let handle =
            DatabaseHandle::open_or_create(&db_path, DatabaseConfig::for_testing()).unwrap();
        assert!(db_path.exists());
        drop(handle);
    }

    #[test]
    fn test_open_or_create_existing() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create first
        {
            let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();
            handle.close().unwrap();
        }

        // Open or create should open
        let handle =
            DatabaseHandle::open_or_create(&db_path, DatabaseConfig::for_testing()).unwrap();
        assert!(db_path.exists());
        drop(handle);
    }

    #[test]
    fn test_codec_invalid() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create with identity codec
        {
            let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();
            handle.close().unwrap();
        }

        // Try to open with invalid codec - should fail during config validation
        let config = DatabaseConfig::for_testing().with_codec("nonexistent_codec");
        let result = DatabaseHandle::open(&db_path, config);

        // Config validation fails before we can check codec mismatch
        assert!(
            matches!(&result, Err(DatabaseHandleError::Config(_))),
            "Expected Config error for invalid codec"
        );

        // Verify the error message mentions the codec
        if let Err(DatabaseHandleError::Config(config_err)) = result {
            let msg = config_err.to_string();
            assert!(
                msg.contains("codec") || msg.contains("Codec"),
                "Error should mention codec: {}",
                msg
            );
        }
    }

    #[test]
    fn test_create_with_invalid_codec() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Try to create with invalid codec
        let config = DatabaseConfig::for_testing().with_codec("aes256_not_implemented");
        let result = DatabaseHandle::create(&db_path, config);

        assert!(
            matches!(&result, Err(DatabaseHandleError::Config(_))),
            "Expected Config error for invalid codec"
        );
    }

    #[test]
    fn test_codec_must_match() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create with identity codec
        let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();
        let manifest = handle.manifest();
        assert_eq!(manifest.codec_id, "identity");
        handle.close().unwrap();

        // Open with same codec should succeed
        let handle = DatabaseHandle::open(&db_path, DatabaseConfig::for_testing()).unwrap();
        assert_eq!(handle.config().codec_id, "identity");
        handle.close().unwrap();
    }

    #[test]
    fn test_append_and_flush_wal() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();

        let record = WalRecord::new(1, handle.uuid(), 12345, vec![1, 2, 3]);
        handle.append_wal(&record).unwrap();
        handle.flush_wal().unwrap();

        handle.close().unwrap();
    }

    #[test]
    fn test_checkpoint() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();

        let info = handle.checkpoint(100, CheckpointData::new()).unwrap();

        assert_eq!(info.snapshot_id, 1);
        assert_eq!(info.watermark_txn, 100);

        // Verify manifest was updated
        let manifest = handle.manifest();
        assert_eq!(manifest.snapshot_id, Some(1));
        assert_eq!(manifest.snapshot_watermark, Some(100));

        handle.close().unwrap();
    }

    #[test]
    fn test_uuid_unique() {
        let uuid1 = generate_uuid();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let uuid2 = generate_uuid();

        assert_ne!(uuid1, uuid2);
    }

    #[test]
    fn test_watermark() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let handle = DatabaseHandle::create(&db_path, DatabaseConfig::for_testing()).unwrap();

        // No watermark initially
        assert!(handle.watermark().is_none());

        // Create checkpoint
        handle.checkpoint(50, CheckpointData::new()).unwrap();

        // Now has watermark
        assert_eq!(handle.watermark(), Some(50));

        handle.close().unwrap();
    }
}
