//! Checkpoint coordinator
//!
//! Coordinates snapshot creation with watermark tracking.
//! This is the main API for creating database checkpoints.

use std::path::{Path, PathBuf};

use crate::codec::StorageCodec;
use crate::disk_snapshot::{SnapshotSection, SnapshotWriter};
use crate::format::primitive_tags;
use crate::format::primitives::SnapshotSerializer;
use crate::format::watermark::{CheckpointInfo, SnapshotWatermark};

/// Checkpoint coordinator
///
/// Manages the lifecycle of checkpoints:
/// 1. Tracks watermark state
/// 2. Serializes primitive data
/// 3. Creates crash-safe snapshots
/// 4. Updates watermark after successful checkpoint
pub struct CheckpointCoordinator {
    snapshot_writer: SnapshotWriter,
    serializer: SnapshotSerializer,
    watermark: SnapshotWatermark,
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator
    pub fn new(
        snapshots_dir: PathBuf,
        codec: Box<dyn StorageCodec>,
        database_uuid: [u8; 16],
    ) -> std::io::Result<Self> {
        let serializer_codec: Box<dyn StorageCodec> = Box::new(crate::codec::IdentityCodec);
        let snapshot_writer = SnapshotWriter::new(snapshots_dir, codec, database_uuid)?;
        let serializer = SnapshotSerializer::new(serializer_codec);

        Ok(CheckpointCoordinator {
            snapshot_writer,
            serializer,
            watermark: SnapshotWatermark::new(),
        })
    }

    /// Create a new checkpoint coordinator with existing watermark state
    pub fn with_watermark(
        snapshots_dir: PathBuf,
        codec: Box<dyn StorageCodec>,
        database_uuid: [u8; 16],
        watermark: SnapshotWatermark,
    ) -> std::io::Result<Self> {
        let serializer_codec: Box<dyn StorageCodec> = Box::new(crate::codec::IdentityCodec);
        let snapshot_writer = SnapshotWriter::new(snapshots_dir, codec, database_uuid)?;
        let serializer = SnapshotSerializer::new(serializer_codec);

        Ok(CheckpointCoordinator {
            snapshot_writer,
            serializer,
            watermark,
        })
    }

    /// Get the current watermark state
    pub fn watermark(&self) -> &SnapshotWatermark {
        &self.watermark
    }

    /// Get the snapshots directory
    pub fn snapshots_dir(&self) -> &Path {
        self.snapshot_writer.snapshots_dir()
    }

    /// Create a checkpoint with the provided primitive data
    ///
    /// This is the main checkpoint API. It:
    /// 1. Determines the next snapshot ID
    /// 2. Serializes all provided primitive sections
    /// 3. Creates a crash-safe snapshot
    /// 4. Updates the watermark on success
    ///
    /// Returns `CheckpointInfo` on success.
    pub fn checkpoint(
        &mut self,
        watermark_txn: u64,
        data: CheckpointData,
    ) -> Result<CheckpointInfo, CheckpointError> {
        let snapshot_id = self.watermark.next_snapshot_id();

        // Serialize all primitives
        let mut sections = Vec::new();

        if let Some(kv) = data.kv {
            sections.push(SnapshotSection::new(
                primitive_tags::KV,
                self.serializer.serialize_kv(&kv),
            ));
        }

        if let Some(events) = data.events {
            sections.push(SnapshotSection::new(
                primitive_tags::EVENT,
                self.serializer.serialize_events(&events),
            ));
        }

        if let Some(runs) = data.branches {
            sections.push(SnapshotSection::new(
                primitive_tags::BRANCH,
                self.serializer.serialize_branches(&runs),
            ));
        }

        if let Some(json) = data.json {
            sections.push(SnapshotSection::new(
                primitive_tags::JSON,
                self.serializer.serialize_json(&json),
            ));
        }

        if let Some(vectors) = data.vectors {
            sections.push(SnapshotSection::new(
                primitive_tags::VECTOR,
                self.serializer.serialize_vectors(&vectors),
            ));
        }

        // Create the snapshot
        let snapshot_info = self
            .snapshot_writer
            .create_snapshot(snapshot_id, watermark_txn, sections)
            .map_err(CheckpointError::Io)?;

        // Update watermark on success
        self.watermark
            .set(snapshot_id, watermark_txn, snapshot_info.timestamp);

        Ok(CheckpointInfo::new(
            watermark_txn,
            snapshot_id,
            snapshot_info.timestamp,
        ))
    }

    /// Clean up temporary files from failed checkpoints
    pub fn cleanup(&self) -> std::io::Result<usize> {
        self.snapshot_writer.cleanup_temp_files()
    }

    /// Set watermark state (for recovery)
    pub fn set_watermark(&mut self, watermark: SnapshotWatermark) {
        self.watermark = watermark;
    }
}

/// Data to be included in a checkpoint
///
/// Each field is optional - only non-None fields will be included in the snapshot.
#[derive(Debug, Default)]
pub struct CheckpointData {
    /// KV primitive entries
    pub kv: Option<Vec<crate::format::primitives::KvSnapshotEntry>>,
    /// Event primitive entries
    pub events: Option<Vec<crate::format::primitives::EventSnapshotEntry>>,
    /// Branch primitive entries
    pub branches: Option<Vec<crate::format::primitives::BranchSnapshotEntry>>,
    /// JSON primitive entries
    pub json: Option<Vec<crate::format::primitives::JsonSnapshotEntry>>,
    /// Vector primitive entries
    pub vectors: Option<Vec<crate::format::primitives::VectorCollectionSnapshotEntry>>,
}

impl CheckpointData {
    /// Create empty checkpoint data
    pub fn new() -> Self {
        CheckpointData::default()
    }

    /// Set KV entries
    pub fn with_kv(mut self, entries: Vec<crate::format::primitives::KvSnapshotEntry>) -> Self {
        self.kv = Some(entries);
        self
    }

    /// Set Event entries
    pub fn with_events(
        mut self,
        entries: Vec<crate::format::primitives::EventSnapshotEntry>,
    ) -> Self {
        self.events = Some(entries);
        self
    }

    /// Set Branch entries
    pub fn with_branches(
        mut self,
        entries: Vec<crate::format::primitives::BranchSnapshotEntry>,
    ) -> Self {
        self.branches = Some(entries);
        self
    }

    /// Set JSON entries
    pub fn with_json(mut self, entries: Vec<crate::format::primitives::JsonSnapshotEntry>) -> Self {
        self.json = Some(entries);
        self
    }

    /// Set Vector entries
    pub fn with_vectors(
        mut self,
        entries: Vec<crate::format::primitives::VectorCollectionSnapshotEntry>,
    ) -> Self {
        self.vectors = Some(entries);
        self
    }
}

/// Errors that can occur during checkpoint creation
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// IO error during checkpoint
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::IdentityCodec;
    use crate::format::primitives::{EventSnapshotEntry, KvSnapshotEntry};

    fn test_uuid() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    #[test]
    fn test_checkpoint_empty() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut coordinator = CheckpointCoordinator::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let data = CheckpointData::new();
        let info = coordinator.checkpoint(100, data).unwrap();

        assert_eq!(info.snapshot_id, 1);
        assert_eq!(info.watermark_txn, 100);
        assert!(info.timestamp > 0);

        // Watermark should be updated
        assert_eq!(coordinator.watermark().snapshot_id(), Some(1));
        assert_eq!(coordinator.watermark().watermark_txn(), Some(100));
    }

    #[test]
    fn test_checkpoint_with_kv() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut coordinator = CheckpointCoordinator::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let kv_entries = vec![
            KvSnapshotEntry {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
                version: 1,
                timestamp: 1000,
            },
            KvSnapshotEntry {
                key: "key2".to_string(),
                value: b"value2".to_vec(),
                version: 2,
                timestamp: 2000,
            },
        ];

        let data = CheckpointData::new().with_kv(kv_entries);
        let info = coordinator.checkpoint(50, data).unwrap();

        assert_eq!(info.snapshot_id, 1);
        assert_eq!(info.watermark_txn, 50);
    }

    #[test]
    fn test_multiple_checkpoints() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut coordinator = CheckpointCoordinator::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        // First checkpoint
        let info1 = coordinator.checkpoint(100, CheckpointData::new()).unwrap();
        assert_eq!(info1.snapshot_id, 1);

        // Second checkpoint
        let info2 = coordinator.checkpoint(200, CheckpointData::new()).unwrap();
        assert_eq!(info2.snapshot_id, 2);

        // Third checkpoint
        let info3 = coordinator.checkpoint(300, CheckpointData::new()).unwrap();
        assert_eq!(info3.snapshot_id, 3);

        // Watermark should reflect latest
        assert_eq!(coordinator.watermark().snapshot_id(), Some(3));
        assert_eq!(coordinator.watermark().watermark_txn(), Some(300));
    }

    #[test]
    fn test_checkpoint_with_all_primitives() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut coordinator = CheckpointCoordinator::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        let data = CheckpointData::new()
            .with_kv(vec![KvSnapshotEntry {
                key: "k".to_string(),
                value: vec![],
                version: 1,
                timestamp: 0,
            }])
            .with_events(vec![EventSnapshotEntry {
                sequence: 1,
                payload: vec![],
                timestamp: 0,
            }]);

        let info = coordinator.checkpoint(100, data).unwrap();
        assert_eq!(info.snapshot_id, 1);
    }

    #[test]
    fn test_watermark_coverage() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut coordinator = CheckpointCoordinator::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        // Before checkpoint, nothing is covered
        assert!(!coordinator.watermark().is_covered(50));
        assert!(coordinator.watermark().needs_replay(50));

        // Create checkpoint at txn 100
        coordinator.checkpoint(100, CheckpointData::new()).unwrap();

        // Txns <= 100 are covered
        assert!(coordinator.watermark().is_covered(50));
        assert!(coordinator.watermark().is_covered(100));
        assert!(!coordinator.watermark().is_covered(101));

        // Txns > 100 need replay
        assert!(!coordinator.watermark().needs_replay(50));
        assert!(!coordinator.watermark().needs_replay(100));
        assert!(coordinator.watermark().needs_replay(101));
    }

    #[test]
    fn test_with_existing_watermark() {
        let temp_dir = tempfile::tempdir().unwrap();

        let existing_watermark = SnapshotWatermark::with_values(5, 500, 12345);

        let mut coordinator = CheckpointCoordinator::with_watermark(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
            existing_watermark,
        )
        .unwrap();

        // Next snapshot should be 6
        let info = coordinator.checkpoint(600, CheckpointData::new()).unwrap();
        assert_eq!(info.snapshot_id, 6);
    }

    #[test]
    fn test_cleanup() {
        let temp_dir = tempfile::tempdir().unwrap();

        let coordinator = CheckpointCoordinator::new(
            temp_dir.path().to_path_buf(),
            Box::new(IdentityCodec),
            test_uuid(),
        )
        .unwrap();

        // Create temp files manually
        std::fs::write(temp_dir.path().join(".snap-000001.tmp"), b"incomplete").unwrap();

        let cleaned = coordinator.cleanup().unwrap();
        assert_eq!(cleaned, 1);
    }
}
