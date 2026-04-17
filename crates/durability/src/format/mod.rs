//! On-disk byte formats for WAL, snapshots, and MANIFEST.
//!
//! This module centralizes all serialization logic for persistent storage.
//! Keeping serialization separate from operational logic (how WAL/snapshots
//! are managed) makes format evolution easier to manage.
//!
//! # Module Structure
//!
//! - `wal_record`: WAL segment header and record format
//! - `writeset`: Transaction writeset serialization
//! - `manifest`: MANIFEST file format (added in Epic 72)
//! - `snapshot`: Snapshot file format (added in Epic 71)

pub mod manifest;
pub mod primitive_tags;
pub mod primitives;
pub mod segment_meta;
pub mod snapshot;
pub mod wal_record;
pub mod watermark;
pub mod writeset;

pub use snapshot::{
    find_latest_snapshot, list_snapshots, parse_snapshot_id, snapshot_path, SectionHeader,
    SnapshotHeader, SnapshotHeaderError, SNAPSHOT_FORMAT_VERSION, SNAPSHOT_HEADER_SIZE,
    SNAPSHOT_MAGIC,
};
pub use wal_record::{
    SegmentHeader, SegmentHeaderError, WalRecord, WalRecordError, WalSegment, WalSegmentError,
    MIN_SUPPORTED_SEGMENT_FORMAT_VERSION, SEGMENT_FORMAT_VERSION, SEGMENT_HEADER_SIZE,
    SEGMENT_HEADER_SIZE_V2, SEGMENT_MAGIC, WAL_RECORD_FORMAT_VERSION,
};
pub use writeset::{Mutation, Writeset, WritesetError};

pub use manifest::{
    Manifest, ManifestError, ManifestManager, MANIFEST_FORMAT_VERSION, MANIFEST_MAGIC,
};
pub use primitives::{
    BranchSnapshotEntry, EventSnapshotEntry, JsonSnapshotEntry, KvSnapshotEntry,
    PrimitiveSerializeError, SnapshotSerializer, VectorCollectionSnapshotEntry,
    VectorSnapshotEntry,
};
pub use segment_meta::{
    SegmentMeta, SegmentMetaError, SEGMENT_META_MAGIC, SEGMENT_META_SIZE, SEGMENT_META_VERSION,
};
pub use watermark::{CheckpointInfo, SnapshotWatermark, WatermarkError};
