//! Storage-owned checkpoint and WAL compaction runtime.
//!
//! This module owns generic snapshot, MANIFEST, snapshot-pruning, and WAL
//! compaction mechanics. Callers remain responsible for lifecycle policy and
//! primitive checkpoint materialization.

use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use strata_core::id::{CommitVersion, TxnId};
use tracing::{info, warn};

use crate::durability::codec::StorageCodec;
use crate::durability::compaction::{CompactionError, WalOnlyCompactor};
use crate::durability::disk_snapshot::{CheckpointCoordinator, CheckpointData, CheckpointError};
use crate::durability::format::{
    list_snapshots, ManifestError, ManifestManager, SnapshotWatermark,
};
use crate::durability::layout::DatabaseLayout;
use crate::SegmentedStore;

/// Storage-level snapshot retention settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageSnapshotRetention {
    /// Number of newest snapshots to retain. Effective value is at least one.
    pub retain_count: usize,
}

impl StorageSnapshotRetention {
    /// Create a retention setting while preserving the storage minimum.
    pub fn new(retain_count: usize) -> Self {
        Self {
            retain_count: retain_count.max(1),
        }
    }

    fn effective_retain_count(self) -> usize {
        self.retain_count.max(1)
    }
}

/// Input for a storage-owned checkpoint run.
pub struct StorageCheckpointInput {
    /// Canonical database layout.
    pub layout: DatabaseLayout,
    /// Database UUID persisted in checkpoint snapshots and new MANIFEST files.
    pub database_uuid: [u8; 16],
    /// Codec used for the snapshot file.
    pub checkpoint_codec: Box<dyn StorageCodec>,
    /// Codec id to write if this run has to create a missing MANIFEST.
    pub manifest_create_codec_id: String,
    /// Already-materialized primitive checkpoint DTOs.
    pub checkpoint_data: CheckpointData,
    /// Transaction watermark covered by this checkpoint.
    pub watermark_txn: TxnId,
    /// Active WAL segment to persist into MANIFEST before checkpointing.
    pub active_wal_segment: NonZeroU64,
}

/// Raw checkpoint outcome returned to the engine/runtime caller.
#[derive(Debug)]
pub struct StorageCheckpointOutcome {
    /// Snapshot id written by the checkpoint coordinator.
    pub snapshot_id: u64,
    /// Transaction watermark covered by the snapshot.
    pub watermark_txn: TxnId,
    /// Snapshot creation timestamp from the snapshot writer.
    pub checkpoint_timestamp_micros: u64,
    /// Active WAL segment persisted into MANIFEST.
    pub active_wal_segment: u64,
}

/// Input for a storage-owned WAL compaction run.
pub struct StorageWalCompactionInput {
    /// Canonical database layout.
    pub layout: DatabaseLayout,
    /// Database UUID persisted in new MANIFEST files if one is missing.
    pub database_uuid: [u8; 16],
    /// Codec used to read WAL records when `.meta` sidecars are unavailable.
    pub wal_codec: Box<dyn StorageCodec>,
    /// Codec id to write if this run has to create a missing MANIFEST.
    pub manifest_create_codec_id: String,
    /// One-based active WAL segment observed by the caller, when a writer exists.
    pub active_wal_segment: Option<NonZeroU64>,
}

/// Input for syncing a storage MANIFEST without exposing MANIFEST mechanics to
/// engine wrappers.
pub struct StorageManifestSyncInput {
    /// Canonical database layout.
    pub layout: DatabaseLayout,
    /// Database UUID persisted in a new MANIFEST if one is missing.
    pub database_uuid: [u8; 16],
    /// Codec id to write if this run has to create a missing MANIFEST.
    pub manifest_create_codec_id: String,
    /// One-based active WAL segment observed by the caller, when a writer exists.
    pub active_wal_segment: Option<NonZeroU64>,
}

/// Input for best-effort WAL truncation after memtable flush.
pub struct StorageFlushWalTruncationInput<'a> {
    /// Canonical database layout.
    pub layout: DatabaseLayout,
    /// Segmented storage used to compute the global flush watermark.
    pub storage: &'a SegmentedStore,
    /// Codec used to read WAL records when `.meta` sidecars are unavailable.
    pub wal_codec: Box<dyn StorageCodec>,
}

/// Raw WAL compaction outcome returned to the engine/runtime caller.
#[derive(Debug)]
pub struct StorageWalCompactionOutcome {
    /// Number of WAL segment files removed.
    pub wal_segments_removed: usize,
    /// Bytes reclaimed from removed WAL segment files.
    pub reclaimed_bytes: u64,
    /// Effective MANIFEST watermark used for compaction.
    pub snapshot_watermark: Option<u64>,
}

/// Raw WAL truncation facts after a flush watermark update.
#[derive(Debug)]
pub struct StorageFlushWalTruncationOutcome {
    /// Flush watermark persisted to MANIFEST.
    pub flush_watermark: CommitVersion,
    /// Number of WAL segment files removed.
    pub wal_segments_removed: usize,
    /// Bytes reclaimed from removed WAL segment files.
    pub reclaimed_bytes: u64,
}

/// Storage-local checkpoint runtime errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageCheckpointError {
    /// Snapshot directory creation failed.
    #[error("failed to create snapshots directory {}: {source}", snapshots_dir.display())]
    CreateSnapshotsDir {
        /// Snapshots directory path.
        snapshots_dir: PathBuf,
        /// Underlying IO error.
        source: std::io::Error,
    },

    /// MANIFEST helper failed.
    #[error("{0}")]
    Manifest(#[from] StorageManifestRuntimeError),

    /// Checkpoint coordinator construction failed.
    #[error("checkpoint coordinator: {0}")]
    CheckpointCoordinator(#[source] std::io::Error),

    /// Checkpoint execution failed.
    #[error("checkpoint failed: {0}")]
    Checkpoint(#[source] CheckpointError),
}

/// Storage-local WAL compaction runtime errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageWalCompactionError {
    /// MANIFEST helper failed.
    #[error("{0}")]
    Manifest(#[from] StorageManifestRuntimeError),

    /// No snapshot or flush watermark exists for WAL compaction.
    #[error("No snapshot available for compaction")]
    NoSnapshot,

    /// WAL compaction failed.
    #[error("compaction failed: {0}")]
    Compaction(#[source] CompactionError),
}

/// Storage-local flush-time WAL truncation errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageFlushWalTruncationError {
    /// Existing MANIFEST could not be loaded.
    #[error("failed to load MANIFEST for flush WAL truncation: {source}")]
    LoadManifest {
        /// Underlying MANIFEST error.
        source: ManifestError,
    },

    /// Flush watermark could not be persisted.
    #[error("failed to persist flush watermark {watermark:?}: {source}")]
    SetFlushWatermark {
        /// Flush watermark that failed to persist.
        watermark: CommitVersion,
        /// Underlying MANIFEST error.
        source: ManifestError,
    },

    /// WAL compaction failed after the flush watermark was persisted.
    #[error("WAL compaction failed after flush: {source}")]
    Compaction {
        /// Underlying compaction error.
        source: CompactionError,
    },
}

/// Storage-local MANIFEST helper errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageManifestRuntimeError {
    /// Existing MANIFEST could not be loaded.
    #[error("failed to load MANIFEST: {source}")]
    Load {
        /// Underlying MANIFEST error.
        source: ManifestError,
    },

    /// Missing MANIFEST could not be created.
    #[error("failed to create MANIFEST: {source}")]
    Create {
        /// Underlying MANIFEST error.
        source: ManifestError,
    },

    /// Active WAL segment update could not be persisted.
    #[error("failed to persist MANIFEST active WAL segment: {source}")]
    PersistActiveSegment {
        /// Underlying MANIFEST error.
        source: ManifestError,
    },

    /// Snapshot watermark update could not be persisted.
    #[error("manifest update failed: {source}")]
    SetSnapshotWatermark {
        /// Underlying MANIFEST error.
        source: ManifestError,
    },

    /// MANIFEST fsync/persist could not complete.
    #[error("failed to persist MANIFEST: {source}")]
    Persist {
        /// Underlying MANIFEST error.
        source: ManifestError,
    },
}

/// Storage-local snapshot pruning errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageSnapshotPruneError {
    /// Live MANIFEST could not be loaded.
    #[error("prune_snapshots: failed to load MANIFEST: {source}")]
    ManifestLoad {
        /// Error from the MANIFEST layer.
        source: ManifestError,
    },

    /// Snapshot directory listing failed.
    #[error("prune_snapshots: failed to list snapshots in {}: {source}", snapshots_dir.display())]
    ListSnapshots {
        /// Snapshots directory path.
        snapshots_dir: PathBuf,
        /// Error from the filesystem layer.
        source: std::io::Error,
    },

    /// Snapshots directory could not be opened for fsync.
    #[error("prune_snapshots: failed to open snapshots dir for fsync: {source}")]
    OpenSnapshotsDir {
        /// Error from the filesystem layer.
        source: std::io::Error,
    },

    /// Snapshots directory fsync failed.
    #[error("prune_snapshots: failed to fsync snapshots dir: {source}")]
    FsyncSnapshotsDir {
        /// Error from the filesystem layer.
        source: std::io::Error,
    },
}

/// Run the storage-owned checkpoint mechanics.
pub fn run_storage_checkpoint(
    input: StorageCheckpointInput,
) -> Result<StorageCheckpointOutcome, StorageCheckpointError> {
    let StorageCheckpointInput {
        layout,
        database_uuid,
        checkpoint_codec,
        manifest_create_codec_id,
        checkpoint_data,
        watermark_txn,
        active_wal_segment,
    } = input;

    let snapshots_dir = layout.snapshots_dir().to_path_buf();
    std::fs::create_dir_all(&snapshots_dir).map_err(|source| {
        StorageCheckpointError::CreateSnapshotsDir {
            snapshots_dir: snapshots_dir.clone(),
            source,
        }
    })?;
    harden_snapshots_dir(&snapshots_dir);

    let mut manifest = load_or_create_manifest(&layout, database_uuid, manifest_create_codec_id)?;
    update_manifest_active_wal_segment(&mut manifest, active_wal_segment)?;
    let active_wal_segment = active_wal_segment.get();

    let existing_watermark = current_snapshot_watermark(&manifest);
    let mut coordinator = if let Some(watermark) = existing_watermark {
        CheckpointCoordinator::with_watermark(
            snapshots_dir,
            checkpoint_codec,
            database_uuid,
            watermark,
        )
        .map_err(StorageCheckpointError::CheckpointCoordinator)?
    } else {
        CheckpointCoordinator::new(snapshots_dir, checkpoint_codec, database_uuid)
            .map_err(StorageCheckpointError::CheckpointCoordinator)?
    };

    let info = coordinator
        .checkpoint(watermark_txn, checkpoint_data)
        .map_err(StorageCheckpointError::Checkpoint)?;

    set_manifest_snapshot_watermark(&mut manifest, info.snapshot_id, info.watermark_txn)?;

    Ok(StorageCheckpointOutcome {
        snapshot_id: info.snapshot_id,
        watermark_txn: info.watermark_txn,
        checkpoint_timestamp_micros: info.timestamp,
        active_wal_segment,
    })
}

/// Run the storage-owned WAL compaction mechanics.
pub fn compact_storage_wal(
    input: StorageWalCompactionInput,
) -> Result<StorageWalCompactionOutcome, StorageWalCompactionError> {
    let StorageWalCompactionInput {
        layout,
        database_uuid,
        wal_codec,
        manifest_create_codec_id,
        active_wal_segment,
    } = input;

    let writer_active_segment = active_wal_segment;
    let mut manifest = load_or_create_manifest(&layout, database_uuid, manifest_create_codec_id)?;
    if let Some(active_wal_segment) = writer_active_segment {
        update_manifest_active_wal_segment(&mut manifest, active_wal_segment)?;
    }
    let writer_active_segment = writer_active_segment.map(NonZeroU64::get);

    let compactor = WalOnlyCompactor::new(
        layout.wal_dir().to_path_buf(),
        Arc::new(Mutex::new(manifest)),
    )
    .with_codec(wal_codec);
    let compact_info = compactor
        .compact_with_active_override(writer_active_segment.unwrap_or(0))
        .map_err(map_wal_compaction_error)?;

    Ok(StorageWalCompactionOutcome {
        wal_segments_removed: compact_info.wal_segments_removed,
        reclaimed_bytes: compact_info.reclaimed_bytes,
        snapshot_watermark: compact_info.snapshot_watermark,
    })
}

/// Load or create the MANIFEST, refresh the active segment if supplied, and
/// persist it durably.
pub fn sync_storage_manifest(
    input: StorageManifestSyncInput,
) -> Result<(), StorageManifestRuntimeError> {
    let StorageManifestSyncInput {
        layout,
        database_uuid,
        manifest_create_codec_id,
        active_wal_segment,
    } = input;

    let mut manifest = load_or_create_manifest(&layout, database_uuid, manifest_create_codec_id)?;
    if let Some(active_wal_segment) = active_wal_segment {
        update_manifest_active_wal_segment(&mut manifest, active_wal_segment)?;
    } else {
        manifest
            .persist()
            .map_err(|source| StorageManifestRuntimeError::Persist { source })?;
    }
    Ok(())
}

/// Persist the storage flush watermark and truncate covered WAL segments.
///
/// Returns `Ok(None)` when no branch has flushed segment state yet. Callers
/// remain responsible for deciding whether errors are fatal or best-effort.
pub fn truncate_storage_wal_after_flush(
    input: StorageFlushWalTruncationInput<'_>,
) -> Result<Option<StorageFlushWalTruncationOutcome>, StorageFlushWalTruncationError> {
    let StorageFlushWalTruncationInput {
        layout,
        storage,
        wal_codec,
    } = input;

    let branch_ids = storage.branch_ids();
    if branch_ids.is_empty() {
        return Ok(None);
    }

    // Branches without flushed segments are excluded; their data remains
    // recoverable from WAL.
    let mut watermark = CommitVersion::MAX;
    let mut has_any_segments = false;
    for branch_id in &branch_ids {
        if let Some(commit) = storage.max_flushed_commit(branch_id) {
            if commit > CommitVersion::ZERO {
                watermark = watermark.min(commit);
                has_any_segments = true;
            }
        }
    }
    if !has_any_segments || watermark == CommitVersion::MAX {
        return Ok(None);
    }

    let mut manifest = ManifestManager::load(layout.manifest_path().to_path_buf())
        .map_err(|source| StorageFlushWalTruncationError::LoadManifest { source })?;
    manifest.set_flush_watermark(watermark).map_err(|source| {
        StorageFlushWalTruncationError::SetFlushWatermark { watermark, source }
    })?;

    let compactor = WalOnlyCompactor::new(
        layout.wal_dir().to_path_buf(),
        Arc::new(Mutex::new(manifest)),
    )
    .with_codec(wal_codec);
    let compact_info = compactor
        .compact()
        .map_err(|source| StorageFlushWalTruncationError::Compaction { source })?;

    Ok(Some(StorageFlushWalTruncationOutcome {
        flush_watermark: watermark,
        wal_segments_removed: compact_info.wal_segments_removed,
        reclaimed_bytes: compact_info.reclaimed_bytes,
    }))
}

fn map_wal_compaction_error(error: CompactionError) -> StorageWalCompactionError {
    match error {
        CompactionError::NoSnapshot => StorageWalCompactionError::NoSnapshot,
        other => StorageWalCompactionError::Compaction(other),
    }
}

/// Load an existing MANIFEST or create one with the provided codec id.
pub(crate) fn load_or_create_manifest(
    layout: &DatabaseLayout,
    database_uuid: [u8; 16],
    manifest_create_codec_id: String,
) -> Result<ManifestManager, StorageManifestRuntimeError> {
    let manifest_path = layout.manifest_path().to_path_buf();

    if ManifestManager::exists(&manifest_path) {
        ManifestManager::load(manifest_path)
            .map_err(|source| StorageManifestRuntimeError::Load { source })
    } else {
        ManifestManager::create(manifest_path, database_uuid, manifest_create_codec_id)
            .map_err(|source| StorageManifestRuntimeError::Create { source })
    }
}

/// Persist the active WAL segment into MANIFEST.
pub(crate) fn update_manifest_active_wal_segment(
    manifest: &mut ManifestManager,
    active_wal_segment: NonZeroU64,
) -> Result<(), StorageManifestRuntimeError> {
    manifest.manifest_mut().active_wal_segment = active_wal_segment.get();
    manifest
        .persist()
        .map_err(|source| StorageManifestRuntimeError::PersistActiveSegment { source })
}

/// Build the storage checkpoint watermark from a MANIFEST.
pub(crate) fn current_snapshot_watermark(manifest: &ManifestManager) -> Option<SnapshotWatermark> {
    let manifest = manifest.manifest();
    match (manifest.snapshot_id, manifest.snapshot_watermark) {
        (Some(snapshot_id), Some(snapshot_watermark)) => Some(SnapshotWatermark::with_values(
            snapshot_id,
            TxnId(snapshot_watermark),
            0,
        )),
        _ => None,
    }
}

/// Persist snapshot watermark facts into MANIFEST.
pub(crate) fn set_manifest_snapshot_watermark(
    manifest: &mut ManifestManager,
    snapshot_id: u64,
    watermark_txn: TxnId,
) -> Result<(), StorageManifestRuntimeError> {
    manifest
        .set_snapshot_watermark(snapshot_id, watermark_txn)
        .map_err(|source| StorageManifestRuntimeError::SetSnapshotWatermark { source })
}

/// Prune old snapshot files with storage-owned retention mechanics.
pub fn prune_storage_snapshots(
    layout: &DatabaseLayout,
    retention: StorageSnapshotRetention,
) -> Result<usize, StorageSnapshotPruneError> {
    let snapshots_dir = layout.snapshots_dir();
    if !snapshots_dir.exists() {
        return Ok(0);
    }

    let retain_count = retention.effective_retain_count();
    let manifest_path = layout.manifest_path().to_path_buf();
    let live_snapshot_id = if ManifestManager::exists(&manifest_path) {
        ManifestManager::load(manifest_path)
            .map_err(|source| StorageSnapshotPruneError::ManifestLoad { source })?
            .manifest()
            .snapshot_id
    } else {
        None
    };

    let snapshots = list_snapshots(snapshots_dir).map_err(|source| {
        StorageSnapshotPruneError::ListSnapshots {
            snapshots_dir: snapshots_dir.to_path_buf(),
            source,
        }
    })?;

    if snapshots.len() <= retain_count {
        return Ok(0);
    }

    let keep_from = snapshots.len().saturating_sub(retain_count);
    let mut deleted = 0usize;

    for (i, (id, path)) in snapshots.iter().enumerate() {
        if i >= keep_from {
            break;
        }
        if Some(*id) == live_snapshot_id {
            continue;
        }
        if let Err(error) = std::fs::remove_file(path) {
            warn!(
                target: "strata::durability",
                snapshot_id = id,
                path = ?path,
                error = %error,
                "Failed to delete pruned snapshot file"
            );
            continue;
        }
        deleted += 1;
    }

    if deleted > 0 {
        let dir_fd = std::fs::File::open(snapshots_dir)
            .map_err(|source| StorageSnapshotPruneError::OpenSnapshotsDir { source })?;
        dir_fd
            .sync_all()
            .map_err(|source| StorageSnapshotPruneError::FsyncSnapshotsDir { source })?;
        info!(
            target: "strata::durability",
            deleted,
            retained = retain_count,
            "Snapshot pruning complete"
        );
    }

    Ok(deleted)
}

#[cfg(unix)]
fn harden_snapshots_dir(snapshots_dir: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(snapshots_dir, std::fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
fn harden_snapshots_dir(_snapshots_dir: &std::path::Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::codec::{CodecError, IdentityCodec};
    use crate::durability::disk_snapshot::SnapshotReader;
    use crate::durability::format::{
        primitive_tags, snapshot_path, KvSnapshotEntry, SnapshotSerializer, WalRecord, WalSegment,
    };
    use crate::TypeTag;
    use strata_core::id::CommitVersion;
    use tempfile::TempDir;

    #[derive(Clone)]
    struct NamedIdentityCodec(&'static str);

    impl StorageCodec for NamedIdentityCodec {
        fn encode(&self, data: &[u8]) -> Vec<u8> {
            data.to_vec()
        }

        fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(data.to_vec())
        }

        fn codec_id(&self) -> &str {
            self.0
        }

        fn clone_box(&self) -> Box<dyn StorageCodec> {
            Box::new(self.clone())
        }
    }

    #[derive(Clone)]
    struct XorCodec(u8);

    impl StorageCodec for XorCodec {
        fn encode(&self, data: &[u8]) -> Vec<u8> {
            data.iter().map(|byte| byte ^ self.0).collect()
        }

        fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(self.encode(data))
        }

        fn codec_id(&self) -> &str {
            "xor-test"
        }

        fn clone_box(&self) -> Box<dyn StorageCodec> {
            Box::new(self.clone())
        }
    }

    fn layout(temp_dir: &TempDir) -> DatabaseLayout {
        DatabaseLayout::from_root(temp_dir.path().join("db"))
    }

    fn kv_checkpoint_data(user_key: &'static [u8], value: &'static [u8]) -> CheckpointData {
        CheckpointData::new().with_kv(vec![KvSnapshotEntry {
            branch_id: [0xAB; 16],
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: user_key.to_vec(),
            value: value.to_vec(),
            version: 7,
            timestamp: 700,
            ttl_ms: 0,
            is_tombstone: false,
        }])
    }

    fn checkpoint_input(
        layout: DatabaseLayout,
        checkpoint_data: CheckpointData,
        watermark_txn: u64,
        active_wal_segment: u64,
    ) -> StorageCheckpointInput {
        StorageCheckpointInput {
            layout,
            database_uuid: [0x11; 16],
            checkpoint_codec: Box::new(IdentityCodec),
            manifest_create_codec_id: "identity".to_string(),
            checkpoint_data,
            watermark_txn: TxnId(watermark_txn),
            active_wal_segment: NonZeroU64::new(active_wal_segment)
                .expect("active WAL segment must be non-zero"),
        }
    }

    fn wal_compaction_input(
        layout: DatabaseLayout,
        manifest_create_codec_id: &str,
        active_wal_segment: Option<NonZeroU64>,
    ) -> StorageWalCompactionInput {
        StorageWalCompactionInput {
            layout,
            database_uuid: [0x33; 16],
            wal_codec: Box::new(IdentityCodec),
            manifest_create_codec_id: manifest_create_codec_id.to_string(),
            active_wal_segment,
        }
    }

    fn active_segment(segment_number: u64) -> Option<NonZeroU64> {
        Some(NonZeroU64::new(segment_number).expect("active WAL segment must be non-zero"))
    }

    fn create_wal_segment_with_records(
        layout: &DatabaseLayout,
        segment_number: u64,
        txn_ids: &[u64],
    ) {
        create_wal_segment_with_records_using_codec(
            layout,
            segment_number,
            txn_ids,
            &IdentityCodec,
        );
    }

    fn create_wal_segment_with_records_using_codec(
        layout: &DatabaseLayout,
        segment_number: u64,
        txn_ids: &[u64],
        codec: &dyn StorageCodec,
    ) {
        std::fs::create_dir_all(layout.wal_dir()).unwrap();
        let mut segment = WalSegment::create(layout.wal_dir(), segment_number, [0x33; 16]).unwrap();

        for &txn_id in txn_ids {
            let record = WalRecord::new(
                TxnId(txn_id),
                [0x33; 16],
                txn_id * 1_000,
                vec![txn_id as u8; 8],
            );
            let encoded = codec.encode(&record.to_bytes());
            let outer_len = encoded.len() as u32;
            let outer_len_bytes = outer_len.to_le_bytes();
            let outer_len_crc = {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&outer_len_bytes);
                hasher.finalize()
            };

            segment.write(&outer_len_bytes).unwrap();
            segment.write(&outer_len_crc.to_le_bytes()).unwrap();
            segment.write(&encoded).unwrap();
        }

        segment.close().unwrap();
    }

    #[test]
    fn storage_snapshot_retention_clamps_zero_to_one() {
        let retention = StorageSnapshotRetention::new(0);

        assert_eq!(retention.retain_count, 1);
        assert_eq!(retention.effective_retain_count(), 1);
    }

    #[test]
    fn storage_checkpoint_creates_snapshot_and_updates_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        let outcome = run_storage_checkpoint(checkpoint_input(
            layout.clone(),
            kv_checkpoint_data(b"k1", b"v1"),
            42,
            3,
        ))
        .unwrap();

        assert_eq!(outcome.snapshot_id, 1);
        assert_eq!(outcome.watermark_txn, TxnId(42));
        assert_eq!(outcome.active_wal_segment, 3);

        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().active_wal_segment, 3);
        assert_eq!(manifest.manifest().snapshot_id, Some(1));
        assert_eq!(manifest.manifest().snapshot_watermark, Some(42));

        let snapshot = SnapshotReader::new(Box::new(IdentityCodec))
            .load(&snapshot_path(layout.snapshots_dir(), 1))
            .unwrap();
        assert_eq!(snapshot.header.snapshot_id, 1);
        assert_eq!(snapshot.header.watermark_txn, 42);
        assert_eq!(snapshot.sections.len(), 1);
        assert_eq!(snapshot.sections[0].primitive_type, primitive_tags::KV);

        let serializer = SnapshotSerializer::canonical_primitive_section();
        let kv = serializer
            .deserialize_kv(&snapshot.sections[0].data)
            .unwrap();
        assert_eq!(kv[0].user_key, b"k1");
        assert_eq!(kv[0].value, b"v1");
    }

    #[test]
    fn missing_manifest_uses_explicit_create_codec_id_not_checkpoint_codec() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        let outcome = run_storage_checkpoint(StorageCheckpointInput {
            layout: layout.clone(),
            database_uuid: [0x22; 16],
            checkpoint_codec: Box::new(NamedIdentityCodec("checkpoint-test-codec")),
            manifest_create_codec_id: "identity".to_string(),
            checkpoint_data: kv_checkpoint_data(b"k2", b"v2"),
            watermark_txn: TxnId(8),
            active_wal_segment: active_segment(4).unwrap(),
        })
        .unwrap();

        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().codec_id, "identity");

        let snapshot = SnapshotReader::new(Box::new(NamedIdentityCodec("checkpoint-test-codec")))
            .load(&snapshot_path(layout.snapshots_dir(), outcome.snapshot_id))
            .unwrap();
        assert_eq!(snapshot.codec_id, "checkpoint-test-codec");
    }

    #[test]
    fn storage_checkpoint_reconstructs_existing_snapshot_watermark() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        run_storage_checkpoint(checkpoint_input(
            layout.clone(),
            kv_checkpoint_data(b"k1", b"v1"),
            1,
            1,
        ))
        .unwrap();
        let outcome = run_storage_checkpoint(checkpoint_input(
            layout.clone(),
            kv_checkpoint_data(b"k2", b"v2"),
            2,
            1,
        ))
        .unwrap();

        assert_eq!(outcome.snapshot_id, 2);
        assert!(snapshot_path(layout.snapshots_dir(), 1).exists());
        assert!(snapshot_path(layout.snapshots_dir(), 2).exists());
    }

    #[test]
    fn storage_checkpoint_reports_corrupt_manifest_before_snapshot_write() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        std::fs::create_dir_all(layout.root()).unwrap();
        std::fs::write(layout.manifest_path(), b"not a manifest").unwrap();

        match run_storage_checkpoint(checkpoint_input(
            layout.clone(),
            kv_checkpoint_data(b"k1", b"v1"),
            1,
            1,
        )) {
            Err(StorageCheckpointError::Manifest(StorageManifestRuntimeError::Load { .. })) => {}
            other => panic!("expected corrupt MANIFEST load failure, got {other:?}"),
        }

        assert!(
            list_snapshots(layout.snapshots_dir()).unwrap().is_empty(),
            "checkpoint should fail before writing a snapshot when MANIFEST load fails"
        );
    }

    #[cfg(unix)]
    #[test]
    fn storage_checkpoint_hardens_snapshots_dir_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        run_storage_checkpoint(checkpoint_input(
            layout.clone(),
            kv_checkpoint_data(b"k1", b"v1"),
            1,
            1,
        ))
        .unwrap();

        let mode = std::fs::metadata(layout.snapshots_dir())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn prune_storage_snapshots_preserves_live_manifest_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        for id in 1..=5 {
            run_storage_checkpoint(checkpoint_input(
                layout.clone(),
                kv_checkpoint_data(b"k", b"v"),
                id,
                1,
            ))
            .unwrap();
        }

        {
            let mut manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
            manifest.set_snapshot_watermark(2, TxnId(5)).unwrap();
        }

        let pruned = prune_storage_snapshots(&layout, StorageSnapshotRetention::new(2)).unwrap();
        assert_eq!(pruned, 2);

        let kept: Vec<u64> = list_snapshots(layout.snapshots_dir())
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(kept, vec![2, 4, 5]);
    }

    #[test]
    fn prune_storage_snapshots_logs_delete_errors_and_continues() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        std::fs::create_dir_all(layout.snapshots_dir()).unwrap();
        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            [0x11; 16],
            "identity".to_string(),
        )
        .unwrap();
        manifest.set_snapshot_watermark(4, TxnId(4)).unwrap();

        let undeletable = snapshot_path(layout.snapshots_dir(), 1);
        std::fs::create_dir(&undeletable).unwrap();
        std::fs::write(snapshot_path(layout.snapshots_dir(), 2), b"old").unwrap();
        std::fs::write(snapshot_path(layout.snapshots_dir(), 3), b"older").unwrap();
        std::fs::write(snapshot_path(layout.snapshots_dir(), 4), b"newest").unwrap();

        let pruned = prune_storage_snapshots(&layout, StorageSnapshotRetention::new(1)).unwrap();
        assert_eq!(pruned, 2);
        assert!(undeletable.is_dir());
        assert!(!snapshot_path(layout.snapshots_dir(), 2).exists());
        assert!(!snapshot_path(layout.snapshots_dir(), 3).exists());
        assert!(snapshot_path(layout.snapshots_dir(), 4).exists());
    }

    #[test]
    fn prune_storage_snapshots_reports_manifest_load_errors() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);

        run_storage_checkpoint(checkpoint_input(
            layout.clone(),
            kv_checkpoint_data(b"k1", b"v1"),
            1,
            1,
        ))
        .unwrap();
        std::fs::write(layout.manifest_path(), b"not a manifest").unwrap();

        match prune_storage_snapshots(&layout, StorageSnapshotRetention::new(1)) {
            Err(StorageSnapshotPruneError::ManifestLoad { source }) => {
                let message = source.to_string();
                assert!(message.contains("too short") || message.contains("invalid magic"));
            }
            other => panic!("expected manifest load pruning error, got {other:?}"),
        }
    }

    #[test]
    fn storage_wal_compaction_creates_missing_manifest_before_no_snapshot_error() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);
        std::fs::create_dir_all(layout.root()).unwrap();

        match compact_storage_wal(wal_compaction_input(
            layout.clone(),
            "manifest-test-codec",
            active_segment(7),
        )) {
            Err(StorageWalCompactionError::NoSnapshot) => {}
            other => panic!("expected no-snapshot compaction error, got {other:?}"),
        }

        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().codec_id, "manifest-test-codec");
        assert_eq!(manifest.manifest().active_wal_segment, 7);
        assert_eq!(manifest.manifest().snapshot_watermark, None);
    }

    #[test]
    fn storage_manifest_sync_creates_and_refreshes_active_segment() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);
        std::fs::create_dir_all(layout.root()).unwrap();

        sync_storage_manifest(StorageManifestSyncInput {
            layout: layout.clone(),
            database_uuid: [0x44; 16],
            manifest_create_codec_id: "sync-test-codec".to_string(),
            active_wal_segment: active_segment(5),
        })
        .unwrap();

        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().codec_id, "sync-test-codec");
        assert_eq!(manifest.manifest().active_wal_segment, 5);

        sync_storage_manifest(StorageManifestSyncInput {
            layout: layout.clone(),
            database_uuid: [0x44; 16],
            manifest_create_codec_id: "ignored-on-existing".to_string(),
            active_wal_segment: active_segment(6),
        })
        .unwrap();

        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().codec_id, "sync-test-codec");
        assert_eq!(manifest.manifest().active_wal_segment, 6);
    }

    #[test]
    fn storage_wal_compaction_removes_covered_segments_with_active_override() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);
        std::fs::create_dir_all(layout.root()).unwrap();

        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            [0x33; 16],
            "identity".to_string(),
        )
        .unwrap();
        manifest.set_snapshot_watermark(1, TxnId(10)).unwrap();
        manifest.manifest_mut().active_wal_segment = 2;
        manifest.persist().unwrap();

        create_wal_segment_with_records(&layout, 1, &[1, 2]);
        create_wal_segment_with_records(&layout, 2, &[3, 4]);
        create_wal_segment_with_records(&layout, 3, &[5]);
        create_wal_segment_with_records(&layout, 4, &[11]);

        let outcome = compact_storage_wal(wal_compaction_input(
            layout.clone(),
            "identity",
            active_segment(4),
        ))
        .unwrap();

        assert_eq!(outcome.wal_segments_removed, 3);
        assert!(outcome.reclaimed_bytes > 0);
        assert_eq!(outcome.snapshot_watermark, Some(10));
        assert!(!WalSegment::segment_path(layout.wal_dir(), 1).exists());
        assert!(!WalSegment::segment_path(layout.wal_dir(), 2).exists());
        assert!(!WalSegment::segment_path(layout.wal_dir(), 3).exists());
        assert!(WalSegment::segment_path(layout.wal_dir(), 4).exists());

        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().active_wal_segment, 4);
    }

    #[test]
    fn storage_wal_compaction_full_scan_uses_supplied_codec() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);
        std::fs::create_dir_all(layout.root()).unwrap();

        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            [0x33; 16],
            "xor-test".to_string(),
        )
        .unwrap();
        manifest.set_snapshot_watermark(1, TxnId(10)).unwrap();
        manifest.manifest_mut().active_wal_segment = 3;
        manifest.persist().unwrap();

        let codec = XorCodec(0xA5);
        create_wal_segment_with_records_using_codec(&layout, 1, &[1, 2], &codec);

        let outcome = compact_storage_wal(StorageWalCompactionInput {
            layout: layout.clone(),
            database_uuid: [0x33; 16],
            wal_codec: Box::new(codec),
            manifest_create_codec_id: "xor-test".to_string(),
            active_wal_segment: active_segment(3),
        })
        .unwrap();

        assert_eq!(
            outcome.wal_segments_removed, 1,
            "codec-encoded segment without .meta should compact via codec-aware full scan"
        );
        assert_eq!(outcome.snapshot_watermark, Some(10));
        assert!(!WalSegment::segment_path(layout.wal_dir(), 1).exists());
    }

    #[test]
    fn storage_wal_compaction_without_active_observation_preserves_manifest_active() {
        let temp_dir = TempDir::new().unwrap();
        let layout = layout(&temp_dir);
        std::fs::create_dir_all(layout.root()).unwrap();

        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            [0x33; 16],
            "identity".to_string(),
        )
        .unwrap();
        manifest.set_flush_watermark(CommitVersion(10)).unwrap();
        manifest.manifest_mut().active_wal_segment = 3;
        manifest.persist().unwrap();

        create_wal_segment_with_records(&layout, 1, &[1, 2]);
        create_wal_segment_with_records(&layout, 2, &[3, 4]);
        create_wal_segment_with_records(&layout, 3, &[5]);

        let outcome =
            compact_storage_wal(wal_compaction_input(layout.clone(), "identity", None)).unwrap();

        assert_eq!(outcome.wal_segments_removed, 2);
        assert_eq!(outcome.snapshot_watermark, Some(10));
        assert!(!WalSegment::segment_path(layout.wal_dir(), 1).exists());
        assert!(!WalSegment::segment_path(layout.wal_dir(), 2).exists());
        assert!(WalSegment::segment_path(layout.wal_dir(), 3).exists());

        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().active_wal_segment, 3);
    }
}
