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
//! - Version retention policies
//! - Crash testing infrastructure

// Allow deprecated SnapshotSerializable usage (will be removed in future refactor)
#![allow(deprecated)]
#![warn(missing_docs)]
#![warn(clippy::all)]

// === Existing modules ===
pub mod branch_bundle; // Portable execution artifacts (BranchBundle)
pub mod recovery; // WAL replay logic
pub mod snapshot; // Snapshot writer and serialization
pub mod snapshot_types; // Snapshot envelope and header types
pub mod wal; // WAL segment types, durability modes

// === Modules moved from storage crate (Phase 1 consolidation) ===
pub mod codec; // Storage codec abstraction (identity, future encryption/compression)
pub mod compaction; // WAL segment cleanup and tombstone tracking
pub mod disk_snapshot; // Crash-safe snapshot I/O and checkpoint coordination
pub mod format; // Binary on-disk formats (WAL segments, snapshots, manifest, writesets)
pub mod retention; // Version retention policies (KeepAll, KeepLast, KeepFor, Composite)
pub mod testing; // Crash test harness and reference model

// === Multi-process WAL coordination ===
pub mod coordination; // WAL file lock + counter file for multi-process access

// === Phase 2: Database lifecycle coordination ===
pub mod database; // Database handle, config, paths (DatabaseHandle, DatabaseConfig, etc.)

// === Re-exports ===
pub use snapshot::{
    deserialize_primitives, serialize_all_primitives, SnapshotReader, SnapshotSerializable,
    SnapshotWriter,
};
pub use snapshot_types::{
    now_micros, primitive_ids, PrimitiveSection, SnapshotEnvelope, SnapshotError, SnapshotHeader,
    SnapshotInfo, SNAPSHOT_HEADER_SIZE, SNAPSHOT_MAGIC, SNAPSHOT_VERSION_1,
};
pub use wal::DurabilityMode;

// BranchBundle types
pub use branch_bundle::{
    BranchBundleError, BranchBundleReader, BranchBundleResult, BranchBundleWriter,
    BranchExportInfo, BranchlogPayload, BundleBranchInfo, BundleContents, BundleManifest,
    BundleVerifyInfo, ExportOptions, ImportedBranchInfo, ReadBundleContents, WalLogInfo,
    WalLogIterator, WalLogReader, WalLogWriter, BRANCHBUNDLE_EXTENSION,
    BRANCHBUNDLE_FORMAT_VERSION,
};

// === Re-exports from moved modules ===

// Codec
pub use codec::{get_codec, CodecError, IdentityCodec, StorageCodec};

// Disk snapshot
pub use disk_snapshot::{
    CheckpointCoordinator, CheckpointData, CheckpointError, LoadedSection, LoadedSnapshot,
    SnapshotInfo as DiskSnapshotInfo, SnapshotReadError, SnapshotReader as DiskSnapshotReader,
    SnapshotSection, SnapshotWriter as DiskSnapshotWriter,
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
    SnapshotHeader as FormatSnapshotHeader,
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
    SNAPSHOT_HEADER_SIZE as FORMAT_SNAPSHOT_HEADER_SIZE,
    SNAPSHOT_MAGIC as FORMAT_SNAPSHOT_MAGIC,
    WAL_RECORD_FORMAT_VERSION,
};

// Retention
pub use retention::{CompositeBuilder, RetentionPolicy, RetentionPolicyError};

// Compaction
pub use compaction::{
    CompactInfo, CompactMode, CompactionError, Tombstone, TombstoneError, TombstoneIndex,
    TombstoneReason, WalOnlyCompactor,
};

// Testing utilities
pub use testing::{
    CrashConfig, CrashPoint, CrashTestError, CrashTestResult, CrashType, DataState, Operation,
    ReferenceModel, StateMismatch, VerificationResult,
};

// === Phase 2 re-exports: Database lifecycle ===
pub use database::{
    ConfigError, DatabaseConfig, DatabaseHandle, DatabaseHandleError, DatabasePathError,
    DatabasePaths,
};

// WAL segmented types (new in Phase 2)
pub use wal::{
    TruncateInfo, WalConfig, WalConfigError, WalCounters, WalDiskUsage, WalReader, WalReaderError,
    WalWriter,
};

// Recovery coordinator types (new in Phase 2)
pub use recovery::{
    RecoveryCoordinator, RecoveryError, RecoveryPlan, RecoveryResult as SegmentedRecoveryResult,
    RecoverySnapshot,
};
pub use recovery::{WalReplayError, WalReplayer};
