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

// WAL segmented types
pub use wal::{
    TruncateInfo, WalConfig, WalConfigError, WalCounters, WalDiskUsage, WalReader, WalReaderError,
    WalRecordIterator, WalWriter,
};
