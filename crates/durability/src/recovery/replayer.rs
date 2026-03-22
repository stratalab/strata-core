//! WAL replay for recovery
//!
//! Replays WAL records to reconstruct database state.
//! Replay is deterministic and idempotent.
//!
//! # Replay Properties
//!
//! - **Deterministic**: Same records always produce the same state
//! - **Idempotent**: Multiple replays yield identical results
//! - **Ordered**: Records are applied in txn_id order
//!
//! # Usage
//!
//! ```text
//! let replayer = WalReplayer::new(wal_dir);
//! let stats = replayer.replay_after(Some(watermark), |record| {
//!     // Apply record to your state
//!     Ok(())
//! })?;
//! ```

use std::path::{Path, PathBuf};

use crate::format::WalRecord;
use crate::wal::reader::{WalReadResult, WalReaderError};
use crate::wal::WalReader;

/// WAL replay engine
///
/// Coordinates replay of WAL records with watermark filtering.
pub struct WalReplayer {
    wal_dir: PathBuf,
    reader: WalReader,
}

impl WalReplayer {
    /// Create a new WAL replayer
    pub fn new(wal_dir: PathBuf) -> Self {
        WalReplayer {
            wal_dir,
            reader: WalReader::new(),
        }
    }

    /// Get the WAL directory
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Get a reference to the underlying WAL reader
    pub fn reader(&self) -> &WalReader {
        &self.reader
    }

    /// List all WAL segments
    pub fn list_segments(&self) -> Result<Vec<u64>, WalReplayError> {
        self.reader
            .list_segments(&self.wal_dir)
            .map_err(WalReplayError::from)
    }

    /// Read all WAL records
    pub fn read_all(&self) -> Result<WalReadResult, WalReplayError> {
        self.reader
            .read_all(&self.wal_dir)
            .map_err(WalReplayError::from)
    }

    /// Replay WAL records after a given watermark
    ///
    /// Records with txn_id <= watermark are skipped.
    /// Records with txn_id > watermark are passed to the apply function.
    ///
    /// # Determinism
    ///
    /// Same records -> same state. The apply function receives records
    /// in the same order every time.
    ///
    /// # Idempotence
    ///
    /// Multiple replays -> same result. The apply function should handle
    /// re-applying records gracefully.
    pub fn replay_after<F>(
        &self,
        watermark: Option<u64>,
        mut apply_fn: F,
    ) -> Result<ReplayStats, WalReplayError>
    where
        F: FnMut(&WalRecord) -> Result<(), WalReplayError>,
    {
        let mut stats = ReplayStats::default();

        let segments = self.list_segments()?;
        stats.segments_read = segments.len();

        for segment_number in segments {
            let (records, _valid_end, _stop_reason, skipped_corrupted) = self
                .reader
                .read_segment(&self.wal_dir, segment_number)
                .map_err(WalReplayError::from)?;

            stats.records_skipped_corrupted += skipped_corrupted;

            for record in records {
                stats.records_read += 1;

                // Skip records at or before watermark
                if let Some(w) = watermark {
                    if record.txn_id <= w {
                        stats.records_skipped += 1;
                        continue;
                    }
                }

                apply_fn(&record)?;
                stats.records_applied += 1;
            }
        }

        if stats.records_skipped_corrupted > 0 {
            tracing::warn!(
                target: "strata::recovery",
                skipped = stats.records_skipped_corrupted,
                "Skipped corrupted WAL records during recovery"
            );
        }

        Ok(stats)
    }

    /// Replay all records without watermark filtering
    pub fn replay_all<F>(&self, apply_fn: F) -> Result<ReplayStats, WalReplayError>
    where
        F: FnMut(&WalRecord) -> Result<(), WalReplayError>,
    {
        self.replay_after(None, apply_fn)
    }

    /// Collect all records after a watermark (without applying)
    pub fn collect_after_watermark(
        &self,
        watermark: Option<u64>,
    ) -> Result<Vec<WalRecord>, WalReplayError> {
        let mut records = Vec::new();

        self.replay_after(watermark, |record| {
            records.push(record.clone());
            Ok(())
        })?;

        Ok(records)
    }

    /// Get the maximum transaction ID in the WAL
    pub fn max_txn_id(&self) -> Result<Option<u64>, WalReplayError> {
        self.reader
            .max_txn_id(&self.wal_dir)
            .map_err(WalReplayError::from)
    }
}

/// Statistics from WAL replay
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReplayStats {
    /// Number of segments read
    pub segments_read: usize,
    /// Total number of records read
    pub records_read: usize,
    /// Records skipped due to watermark
    pub records_skipped: usize,
    /// Records successfully applied
    pub records_applied: usize,
    /// Records skipped due to corruption (checksum mismatch)
    pub records_skipped_corrupted: usize,
}

impl ReplayStats {
    /// Create empty stats
    pub fn new() -> Self {
        ReplayStats::default()
    }

    /// Check if any records were applied
    pub fn has_records(&self) -> bool {
        self.records_applied > 0
    }

    /// Check if any records were skipped
    pub fn has_skipped(&self) -> bool {
        self.records_skipped > 0
    }
}

/// WAL replay errors
#[derive(Debug, thiserror::Error)]
pub enum WalReplayError {
    /// WAL reader error
    #[error("WAL reader error: {0}")]
    Reader(#[from] WalReaderError),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Apply error (from callback)
    #[error("Apply error: {0}")]
    Apply(String),
}

impl WalReplayError {
    /// Create an apply error from a message
    pub fn apply(msg: impl Into<String>) -> Self {
        WalReplayError::Apply(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{IdentityCodec, StorageCodec};
    use crate::format::WalSegment;
    use crate::wal::{DurabilityMode, WalConfig, WalWriter};
    use tempfile::tempdir;

    fn make_codec() -> Box<dyn StorageCodec> {
        Box::new(IdentityCodec)
    }

    fn write_records(wal_dir: &Path, records: &[WalRecord]) {
        let mut writer = WalWriter::new(
            wal_dir.to_path_buf(),
            [1u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            make_codec(),
        )
        .unwrap();

        for record in records {
            writer.append(record).unwrap();
        }

        writer.flush().unwrap();
    }

    #[test]
    fn test_replay_empty_wal() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let replayer = WalReplayer::new(wal_dir);
        let stats = replayer.replay_all(|_| Ok(())).unwrap();

        assert_eq!(stats.segments_read, 0);
        assert_eq!(stats.records_read, 0);
        assert_eq!(stats.records_applied, 0);
    }

    #[test]
    fn test_replay_all_records() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=5)
            .map(|i| WalRecord::new(i, [1u8; 16], i * 1000, vec![i as u8]))
            .collect();

        write_records(&wal_dir, &records);

        let replayer = WalReplayer::new(wal_dir);
        let mut applied = Vec::new();

        let stats = replayer
            .replay_all(|record| {
                applied.push(record.txn_id);
                Ok(())
            })
            .unwrap();

        assert_eq!(stats.records_read, 5);
        assert_eq!(stats.records_applied, 5);
        assert_eq!(stats.records_skipped, 0);
        assert_eq!(applied, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_replay_after_watermark() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=10)
            .map(|i| WalRecord::new(i, [1u8; 16], i * 1000, vec![]))
            .collect();

        write_records(&wal_dir, &records);

        let replayer = WalReplayer::new(wal_dir);
        let mut applied = Vec::new();

        let stats = replayer
            .replay_after(Some(5), |record| {
                applied.push(record.txn_id);
                Ok(())
            })
            .unwrap();

        assert_eq!(stats.records_read, 10);
        assert_eq!(stats.records_skipped, 5);
        assert_eq!(stats.records_applied, 5);
        assert_eq!(applied, vec![6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_replay_deterministic() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=5)
            .map(|i| WalRecord::new(i, [1u8; 16], i * 1000, vec![i as u8; 10]))
            .collect();

        write_records(&wal_dir, &records);

        // Replay multiple times
        let mut results = Vec::new();
        for _ in 0..3 {
            let replayer = WalReplayer::new(wal_dir.clone());
            let collected = replayer.collect_after_watermark(None).unwrap();
            results.push(collected);
        }

        // All replays should produce identical results
        assert!(results.windows(2).all(|w| w[0] == w[1]));
    }

    #[test]
    fn test_replay_idempotent() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(i, [1u8; 16], 0, vec![i as u8]))
            .collect();

        write_records(&wal_dir, &records);

        // Use a simple counter to simulate applying
        let mut apply_count = 0;
        let replayer = WalReplayer::new(wal_dir.clone());

        let stats1 = replayer
            .replay_all(|_| {
                apply_count += 1;
                Ok(())
            })
            .unwrap();

        // Replay again
        let stats2 = replayer
            .replay_all(|_| {
                apply_count += 1;
                Ok(())
            })
            .unwrap();

        // Same stats both times
        assert_eq!(stats1, stats2);
        assert_eq!(apply_count, 6); // 3 records * 2 replays
    }

    #[test]
    fn test_replay_stops_on_error() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=5)
            .map(|i| WalRecord::new(i, [1u8; 16], 0, vec![]))
            .collect();

        write_records(&wal_dir, &records);

        let replayer = WalReplayer::new(wal_dir);
        let mut count = 0;

        let result = replayer.replay_all(|record| {
            count += 1;
            if record.txn_id == 3 {
                Err(WalReplayError::apply("test error"))
            } else {
                Ok(())
            }
        });

        assert!(result.is_err());
        assert_eq!(count, 3); // Stopped at record 3
    }

    #[test]
    fn test_replay_watermark_at_boundary() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=5)
            .map(|i| WalRecord::new(i, [1u8; 16], 0, vec![]))
            .collect();

        write_records(&wal_dir, &records);

        let replayer = WalReplayer::new(wal_dir);

        // Watermark = 5 (all records)
        let stats = replayer.replay_after(Some(5), |_| Ok(())).unwrap();
        assert_eq!(stats.records_applied, 0);
        assert_eq!(stats.records_skipped, 5);

        // Watermark = 0 (no records)
        let replayer2 = WalReplayer::new(dir.path().join("wal"));
        let stats2 = replayer2.replay_after(Some(0), |_| Ok(())).unwrap();
        assert_eq!(stats2.records_applied, 5);
        assert_eq!(stats2.records_skipped, 0);
    }

    #[test]
    fn test_collect_after_watermark() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=10)
            .map(|i| WalRecord::new(i, [1u8; 16], 0, vec![i as u8]))
            .collect();

        write_records(&wal_dir, &records);

        let replayer = WalReplayer::new(wal_dir);
        let collected = replayer.collect_after_watermark(Some(7)).unwrap();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].txn_id, 8);
        assert_eq!(collected[1].txn_id, 9);
        assert_eq!(collected[2].txn_id, 10);
    }

    #[test]
    fn test_max_txn_id() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=10)
            .map(|i| WalRecord::new(i, [1u8; 16], 0, vec![]))
            .collect();

        write_records(&wal_dir, &records);

        let replayer = WalReplayer::new(wal_dir);
        let max = replayer.max_txn_id().unwrap();

        assert_eq!(max, Some(10));
    }

    #[test]
    fn test_replay_stats_methods() {
        let stats = ReplayStats {
            segments_read: 2,
            records_read: 100,
            records_skipped: 30,
            records_applied: 70,
            records_skipped_corrupted: 0,
        };

        assert!(stats.has_records());
        assert!(stats.has_skipped());

        let empty_stats = ReplayStats::new();
        assert!(!empty_stats.has_records());
        assert!(!empty_stats.has_skipped());
    }

    #[test]
    fn test_partial_record_handled() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write valid records
        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(i, [1u8; 16], 0, vec![i as u8]))
            .collect();
        write_records(&wal_dir, &records);

        // Append garbage (partial record)
        let segment_path = WalSegment::segment_path(&wal_dir, 1);
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .unwrap();
        file.write_all(&[0xFF; 20]).unwrap();

        // Replay should still work for valid records
        let replayer = WalReplayer::new(wal_dir);
        let mut applied = Vec::new();

        let stats = replayer
            .replay_all(|record| {
                applied.push(record.txn_id);
                Ok(())
            })
            .unwrap();

        assert_eq!(stats.records_applied, 3);
        assert_eq!(applied, vec![1, 2, 3]);
    }

    /// Issue #1696: WAL replay uses txn_id for watermark comparison, but
    /// watermarks are set from the commit_version space. A long-running
    /// transaction gets a low txn_id at start but a high commit_version
    /// at commit. If watermark is between them, the record is incorrectly
    /// skipped, losing committed data.
    ///
    /// The fix writes commit_version into WalRecord.txn_id, so the WAL
    /// ordering key matches the watermark domain. This test verifies that
    /// records with txn_id (now commit_version) above the watermark are
    /// correctly replayed, and records at or below are skipped.
    #[test]
    fn test_issue_1696_long_running_txn_not_skipped_by_watermark() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // After fix (#1696): WalRecord.txn_id carries commit_version.
        // Fast txns T2..T50 committed with versions 1..50 (already in checkpoint).
        // T1 started early but committed late with commit_version=100.
        // Its WAL record now carries txn_id=100 (the commit_version).
        let records = vec![
            // Already-checkpointed record (commit_version=30, at or below watermark)
            WalRecord::new(30, [1u8; 16], 30_000, vec![30]),
            // Long-running txn committed after checkpoint (commit_version=100)
            WalRecord::new(100, [1u8; 16], 100_000, vec![42]),
        ];

        write_records(&wal_dir, &records);

        let replayer = WalReplayer::new(wal_dir);
        let mut applied = Vec::new();

        // Watermark=50 from checkpoint. Records with commit_version <= 50
        // should be skipped. Records with commit_version > 50 must be replayed.
        let stats = replayer
            .replay_after(Some(50), |record| {
                applied.push(record.txn_id);
                Ok(())
            })
            .unwrap();

        assert_eq!(
            stats.records_applied, 1,
            "Record with commit_version=100 must NOT be skipped by watermark=50"
        );
        assert_eq!(
            stats.records_skipped, 1,
            "Record with commit_version=30 should be skipped"
        );
        assert_eq!(applied, vec![100]);
    }
}
