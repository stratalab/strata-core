//! Durability layer for Strata
//!
//! This crate provides the on-disk building blocks consumed by the engine:
//!
//! - WAL: Segmented write-ahead log with one record per committed transaction
//! - Durability modes: Always, Standard (default), Cache
//! - Snapshot creation and loading (crash-safe disk snapshot I/O)
//! - Binary on-disk formats (segmented WAL, snapshots, manifest, writesets)
//! - Storage codec abstraction (identity, AES-GCM, future compression)
//! - WAL segment compaction and tombstone tracking
//! - Branch bundle import/export
//!
//! Recovery planning lives in `strata_concurrency::RecoveryCoordinator`,
//! which consumes the building blocks above.

// === Modules ===
pub mod branch_bundle; // Portable execution artifacts (BranchBundle)
pub mod codec; // Storage codec abstraction (identity, future encryption/compression)
pub mod compaction; // WAL segment cleanup and tombstone tracking
pub mod disk_snapshot; // Crash-safe snapshot I/O and checkpoint coordination
pub mod format; // Binary on-disk formats (WAL segments, snapshots, manifest, writesets)
pub mod layout; // Canonical database directory layout
pub mod wal; // WAL segment types, durability modes

// === Utilities ===

use std::time::{SystemTime, UNIX_EPOCH};

/// Get current time in microseconds since Unix epoch.
///
/// Returns 0 if system clock is before Unix epoch (clock went backwards).
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

// === Re-exports ===

pub use wal::DurabilityMode;

// BranchBundle types
pub use branch_bundle::{
    BranchBundleError, BranchBundleReader, BranchBundleResult, BranchBundleWriter,
    BranchExportInfo, BranchlogPayload, BundleBranchInfo, BundleContents, BundleManifest,
    BundleVerifyInfo, ExportOptions, ImportedBranchInfo, ReadBundleContents, WalLogInfo,
    WalLogIterator, WalLogReader, WalLogWriter, BRANCHBUNDLE_EXTENSION,
    BRANCHBUNDLE_FORMAT_VERSION,
};

// Codec
pub use codec::{clone_codec, get_codec, CodecError, IdentityCodec, StorageCodec};

// Disk snapshot
pub use disk_snapshot::{
    CheckpointCoordinator, CheckpointData, CheckpointError, LoadedSection, LoadedSnapshot,
    SnapshotInfo, SnapshotReadError, SnapshotReader, SnapshotSection, SnapshotWriter,
};

// Format types
pub use format::{
    // Snapshot format
    find_latest_snapshot,
    list_snapshots,
    parse_snapshot_id,
    primitive_tags,
    snapshot_path,
    BranchSnapshotEntry,
    // Watermark tracking
    CheckpointInfo,
    // Primitive serialization
    EventSnapshotEntry,
    JsonSnapshotEntry,
    KvSnapshotEntry,
    // MANIFEST format
    Manifest,
    ManifestError,
    ManifestManager,
    // WAL format
    Mutation,
    PrimitiveSerializeError,
    SectionHeader,
    SegmentHeader,
    // Segment metadata
    SegmentMeta,
    SegmentMetaError,
    SnapshotHeader,
    SnapshotHeaderError,
    SnapshotSerializer,
    SnapshotWatermark,
    VectorCollectionSnapshotEntry,
    VectorSnapshotEntry,
    WalRecord,
    WalRecordError,
    WalSegment,
    WatermarkError,
    Writeset,
    WritesetError,
    MANIFEST_FORMAT_VERSION,
    MANIFEST_MAGIC,
    SEGMENT_FORMAT_VERSION,
    SEGMENT_HEADER_SIZE,
    SEGMENT_HEADER_SIZE_V2,
    SEGMENT_MAGIC,
    SNAPSHOT_FORMAT_VERSION,
    SNAPSHOT_HEADER_SIZE,
    SNAPSHOT_MAGIC,
    WAL_RECORD_FORMAT_VERSION,
};

// Compaction
pub use compaction::{
    CompactInfo, CompactMode, CompactionError, Tombstone, TombstoneError, TombstoneIndex,
    TombstoneReason, WalOnlyCompactor,
};

// Layout
pub use layout::DatabaseLayout;

// WAL segmented types
pub use wal::{
    TruncateInfo, WalConfig, WalConfigError, WalCounters, WalDiskUsage, WalReader, WalReaderError,
    WalRecordIterator, WalWriter,
};

/// Engine-only durability integration helpers.
#[cfg(feature = "engine-internal")]
#[doc(hidden)]
pub mod __internal {
    use std::fs::File;
    use std::io;
    use std::time::SystemTime;

    use crate::wal::writer::{BgError as WriterBgError, SyncHandle as WriterSyncHandle};
    use crate::WalWriter;

    /// Snapshot of the last background sync failure.
    #[derive(Debug, Clone)]
    #[doc(hidden)]
    pub struct BackgroundSyncError {
        /// The underlying IO error category.
        kind: io::ErrorKind,
        /// Human-readable error message.
        message: String,
        /// Timestamp of the first observed failure in the current streak.
        first_observed_at: SystemTime,
        /// Number of consecutive failed sync attempts.
        failed_sync_count: u64,
    }

    impl From<&WriterBgError> for BackgroundSyncError {
        fn from(value: &WriterBgError) -> Self {
            Self {
                kind: value.kind,
                message: value.message.clone(),
                first_observed_at: value.first_observed_at,
                failed_sync_count: value.failed_sync_count,
            }
        }
    }

    /// Handle for an in-flight background sync.
    #[must_use = "BackgroundSyncHandle must be consumed via commit_background_sync or abort_background_sync"]
    #[doc(hidden)]
    pub struct BackgroundSyncHandle(pub(crate) WriterSyncHandle);

    impl BackgroundSyncHandle {
        /// Returns the cloned file descriptor to fsync outside the WAL lock.
        pub fn fd(&self) -> &File {
            self.0.fd()
        }
    }

    /// Engine-only extension trait for the three-phase background sync API.
    #[doc(hidden)]
    pub trait WalWriterEngineExt {
        /// Starts a background sync if one is due.
        fn begin_background_sync(&mut self) -> io::Result<Option<BackgroundSyncHandle>>;
        /// Commits a successful background sync.
        fn commit_background_sync(&mut self, handle: BackgroundSyncHandle) -> io::Result<()>;
        /// Aborts a failed background sync.
        fn abort_background_sync(&mut self, handle: BackgroundSyncHandle, error: io::Error);
        /// Returns the last background sync error, if any.
        fn bg_error(&self) -> Option<BackgroundSyncError>;
        /// Records a failed sync attempt without a background handle.
        fn record_sync_failure(&mut self, error: io::Error);
        /// Clears the last background sync error.
        fn clear_bg_error(&mut self);
        /// Returns whether a background sync is currently in flight.
        fn sync_in_flight(&self) -> bool;
        /// Refreshes the Standard-mode inline-sync deadline without clearing dirty state.
        fn refresh_inline_sync_deadline(&mut self);
    }

    impl BackgroundSyncError {
        /// Returns the underlying IO error category.
        pub fn kind(&self) -> io::ErrorKind {
            self.kind
        }

        /// Returns the human-readable error message.
        pub fn message(&self) -> &str {
            &self.message
        }

        /// Returns the first-observed timestamp for the current failure streak.
        pub fn first_observed_at(&self) -> SystemTime {
            self.first_observed_at
        }

        /// Returns the number of consecutive failed sync attempts.
        pub fn failed_sync_count(&self) -> u64 {
            self.failed_sync_count
        }
    }

    impl WalWriterEngineExt for WalWriter {
        fn begin_background_sync(&mut self) -> io::Result<Option<BackgroundSyncHandle>> {
            crate::wal::writer::WalWriter::begin_background_sync(self)
                .map(|handle| handle.map(BackgroundSyncHandle))
        }

        fn commit_background_sync(&mut self, handle: BackgroundSyncHandle) -> io::Result<()> {
            crate::wal::writer::WalWriter::commit_background_sync(self, handle.0)
        }

        fn abort_background_sync(&mut self, handle: BackgroundSyncHandle, error: io::Error) {
            crate::wal::writer::WalWriter::abort_background_sync(self, handle.0, error)
        }

        fn bg_error(&self) -> Option<BackgroundSyncError> {
            crate::wal::writer::WalWriter::bg_error(self).map(BackgroundSyncError::from)
        }

        fn record_sync_failure(&mut self, error: io::Error) {
            crate::wal::writer::WalWriter::record_sync_failure(self, error)
        }

        fn clear_bg_error(&mut self) {
            crate::wal::writer::WalWriter::clear_bg_error(self)
        }

        fn sync_in_flight(&self) -> bool {
            crate::wal::writer::WalWriter::sync_in_flight(self)
        }

        fn refresh_inline_sync_deadline(&mut self) {
            crate::wal::writer::WalWriter::refresh_inline_sync_deadline(self)
        }
    }
}
