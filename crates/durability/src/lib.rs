//! Durability layer for Strata
//!
//! This crate handles everything that touches disk:
//!
//! - WAL: Segmented write-ahead log with one record per committed transaction
//! - Durability modes: Always, Standard (default), Cache
//! - Snapshot creation and loading
//! - Recovery: Coordinator-based recovery (MANIFEST + snapshot + WAL)
//! - Binary on-disk formats (segmented WAL, snapshots, manifest)
//! - Storage codec abstraction (encryption/compression extension point)
//! - WAL segment compaction

#![warn(missing_docs)]
#![warn(clippy::all)]

// === Modules ===
pub mod branch_bundle; // Portable execution artifacts (BranchBundle)
pub mod codec; // Storage codec abstraction (identity, future encryption/compression)
pub mod compaction; // WAL segment cleanup and tombstone tracking
pub mod coordination; // WAL file lock + counter file for multi-process access
pub mod database; // Database handle, config, paths (DatabaseHandle, DatabaseConfig, etc.)
pub mod disk_snapshot; // Crash-safe snapshot I/O and checkpoint coordination
pub mod format; // Binary on-disk formats (WAL segments, snapshots, manifest, writesets)
pub mod recovery; // WAL replay logic
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
pub use codec::{get_codec, CodecError, IdentityCodec, StorageCodec};

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
    StateSnapshotEntry,
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

// Database lifecycle
pub use database::{
    ConfigError, DatabaseConfig, DatabaseHandle, DatabaseHandleError, DatabasePathError,
    DatabasePaths,
};

// WAL segmented types
pub use wal::{
    TruncateInfo, WalConfig, WalConfigError, WalCounters, WalDiskUsage, WalReader, WalReaderError,
    WalWriter,
};

// Recovery coordinator types
pub use recovery::{
    RecoveryCoordinator, RecoveryError, RecoveryPlan, RecoveryResult as SegmentedRecoveryResult,
    RecoverySnapshot,
};
pub use recovery::{WalReplayError, WalReplayer};
