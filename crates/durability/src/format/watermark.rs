//! Snapshot watermark tracking
//!
//! Tracks the relationship between snapshots and transaction IDs.
//! The watermark indicates which transactions are included in a snapshot.
//!
//! # Watermark Semantics
//!
//! - `snapshot_watermark`: All transactions with ID <= watermark are in the snapshot
//! - WAL entries with txn_id > watermark are needed for recovery
//! - WAL entries with txn_id <= watermark can be removed by compaction

/// Snapshot watermark state
///
/// Tracks the current snapshot and its watermark for recovery and compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotWatermark {
    /// Current snapshot ID (None if no snapshot exists)
    snapshot_id: Option<u64>,
    /// Transaction watermark (all txns <= this are in the snapshot)
    watermark_txn: Option<u64>,
    /// Timestamp when watermark was last updated
    updated_at: Option<u64>,
}

impl SnapshotWatermark {
    /// Create a new empty watermark state
    pub fn new() -> Self {
        SnapshotWatermark {
            snapshot_id: None,
            watermark_txn: None,
            updated_at: None,
        }
    }

    /// Create watermark state with initial values
    pub fn with_values(snapshot_id: u64, watermark_txn: u64, updated_at: u64) -> Self {
        SnapshotWatermark {
            snapshot_id: Some(snapshot_id),
            watermark_txn: Some(watermark_txn),
            updated_at: Some(updated_at),
        }
    }

    /// Update the watermark after a successful checkpoint
    pub fn set(&mut self, snapshot_id: u64, watermark_txn: u64, updated_at: u64) {
        self.snapshot_id = Some(snapshot_id);
        self.watermark_txn = Some(watermark_txn);
        self.updated_at = Some(updated_at);
    }

    /// Get the current snapshot ID
    pub fn snapshot_id(&self) -> Option<u64> {
        self.snapshot_id
    }

    /// Get the watermark transaction ID
    pub fn watermark_txn(&self) -> Option<u64> {
        self.watermark_txn
    }

    /// Get the timestamp when watermark was last updated
    pub fn updated_at(&self) -> Option<u64> {
        self.updated_at
    }

    /// Check if a transaction ID is covered by the snapshot
    ///
    /// Returns true if the transaction is included in the current snapshot.
    pub fn is_covered(&self, txn_id: u64) -> bool {
        match self.watermark_txn {
            Some(watermark) => txn_id <= watermark,
            None => false,
        }
    }

    /// Check if a transaction ID needs WAL replay
    ///
    /// Returns true if the transaction is NOT in the snapshot and needs replay.
    pub fn needs_replay(&self, txn_id: u64) -> bool {
        match self.watermark_txn {
            Some(watermark) => txn_id > watermark,
            None => true, // No snapshot, all transactions need replay
        }
    }

    /// Get the next snapshot ID
    pub fn next_snapshot_id(&self) -> u64 {
        self.snapshot_id.map(|id| id + 1).unwrap_or(1)
    }

    /// Serialize to bytes for persistence
    ///
    /// Format: has_data(1) + snapshot_id(8) + watermark_txn(8) + updated_at(8) = 25 bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(25);

        match (self.snapshot_id, self.watermark_txn, self.updated_at) {
            (Some(sid), Some(wtxn), Some(ts)) => {
                bytes.push(1); // has data
                bytes.extend_from_slice(&sid.to_le_bytes());
                bytes.extend_from_slice(&wtxn.to_le_bytes());
                bytes.extend_from_slice(&ts.to_le_bytes());
            }
            _ => {
                bytes.push(0); // no data
            }
        }

        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WatermarkError> {
        if bytes.is_empty() {
            return Err(WatermarkError::InsufficientData);
        }

        if bytes[0] == 0 {
            return Ok(SnapshotWatermark::new());
        }

        if bytes.len() < 25 {
            return Err(WatermarkError::InsufficientData);
        }

        let snapshot_id = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let watermark_txn = u64::from_le_bytes(bytes[9..17].try_into().unwrap());
        let updated_at = u64::from_le_bytes(bytes[17..25].try_into().unwrap());

        Ok(SnapshotWatermark::with_values(
            snapshot_id,
            watermark_txn,
            updated_at,
        ))
    }
}

impl Default for SnapshotWatermark {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur with watermark operations
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WatermarkError {
    /// Insufficient data for deserialization
    #[error("Insufficient data for watermark")]
    InsufficientData,
}

/// Information returned after a checkpoint operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// Transaction ID at checkpoint (watermark)
    pub watermark_txn: u64,
    /// Snapshot identifier
    pub snapshot_id: u64,
    /// Timestamp of checkpoint (microseconds since epoch)
    pub timestamp: u64,
}

impl CheckpointInfo {
    /// Create new checkpoint info
    pub fn new(watermark_txn: u64, snapshot_id: u64, timestamp: u64) -> Self {
        CheckpointInfo {
            watermark_txn,
            snapshot_id,
            timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_watermark() {
        let wm = SnapshotWatermark::new();
        assert_eq!(wm.snapshot_id(), None);
        assert_eq!(wm.watermark_txn(), None);
        assert_eq!(wm.updated_at(), None);
    }

    #[test]
    fn test_with_values() {
        let wm = SnapshotWatermark::with_values(5, 100, 1234567890);
        assert_eq!(wm.snapshot_id(), Some(5));
        assert_eq!(wm.watermark_txn(), Some(100));
        assert_eq!(wm.updated_at(), Some(1234567890));
    }

    #[test]
    fn test_set_watermark() {
        let mut wm = SnapshotWatermark::new();
        wm.set(1, 50, 1000);

        assert_eq!(wm.snapshot_id(), Some(1));
        assert_eq!(wm.watermark_txn(), Some(50));

        wm.set(2, 100, 2000);
        assert_eq!(wm.snapshot_id(), Some(2));
        assert_eq!(wm.watermark_txn(), Some(100));
    }

    #[test]
    fn test_is_covered() {
        let wm = SnapshotWatermark::with_values(1, 100, 0);

        assert!(wm.is_covered(1));
        assert!(wm.is_covered(50));
        assert!(wm.is_covered(100));
        assert!(!wm.is_covered(101));
        assert!(!wm.is_covered(200));
    }

    #[test]
    fn test_is_covered_no_snapshot() {
        let wm = SnapshotWatermark::new();

        // Nothing is covered without a snapshot
        assert!(!wm.is_covered(1));
        assert!(!wm.is_covered(100));
    }

    #[test]
    fn test_needs_replay() {
        let wm = SnapshotWatermark::with_values(1, 100, 0);

        assert!(!wm.needs_replay(1));
        assert!(!wm.needs_replay(100));
        assert!(wm.needs_replay(101));
        assert!(wm.needs_replay(200));
    }

    #[test]
    fn test_needs_replay_no_snapshot() {
        let wm = SnapshotWatermark::new();

        // Everything needs replay without a snapshot
        assert!(wm.needs_replay(1));
        assert!(wm.needs_replay(100));
    }

    #[test]
    fn test_next_snapshot_id() {
        let wm = SnapshotWatermark::new();
        assert_eq!(wm.next_snapshot_id(), 1);

        let wm = SnapshotWatermark::with_values(5, 100, 0);
        assert_eq!(wm.next_snapshot_id(), 6);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let wm = SnapshotWatermark::with_values(42, 1000, 9999);
        let bytes = wm.to_bytes();
        let parsed = SnapshotWatermark::from_bytes(&bytes).unwrap();

        assert_eq!(wm, parsed);
    }

    #[test]
    fn test_serialization_empty() {
        let wm = SnapshotWatermark::new();
        let bytes = wm.to_bytes();

        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], 0);

        let parsed = SnapshotWatermark::from_bytes(&bytes).unwrap();
        assert_eq!(wm, parsed);
    }

    #[test]
    fn test_serialization_with_data() {
        let wm = SnapshotWatermark::with_values(1, 100, 12345);
        let bytes = wm.to_bytes();

        assert_eq!(bytes.len(), 25);
        assert_eq!(bytes[0], 1); // has data flag
    }

    #[test]
    fn test_deserialization_insufficient_data() {
        let result = SnapshotWatermark::from_bytes(&[]);
        assert!(matches!(result, Err(WatermarkError::InsufficientData)));

        // Has data flag set but not enough bytes
        let result = SnapshotWatermark::from_bytes(&[1, 0, 0]);
        assert!(matches!(result, Err(WatermarkError::InsufficientData)));
    }

    #[test]
    fn test_checkpoint_info() {
        let info = CheckpointInfo::new(100, 5, 1234567890);

        assert_eq!(info.watermark_txn, 100);
        assert_eq!(info.snapshot_id, 5);
        assert_eq!(info.timestamp, 1234567890);
    }
}
