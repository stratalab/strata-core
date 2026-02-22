//! WAL writer with durability mode support.
//!
//! The writer handles appending WAL records to segments with proper
//! durability guarantees based on the configured mode.

use serde::{Deserialize, Serialize};

use super::DurabilityMode;
use crate::codec::StorageCodec;
use crate::format::segment_meta::SegmentMeta;
use crate::format::{WalRecord, WalSegment, SEGMENT_HEADER_SIZE_V2};
use crate::wal::config::WalConfig;
use crate::wal::reader::WalReader;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, info, warn};

/// Cumulative WAL operation counters.
///
/// These counters accumulate over the lifetime of the WalWriter
/// and are never reset. Use them to observe how many WAL operations
/// a workload triggers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WalCounters {
    /// Total WAL record appends (calls to append() that did work)
    pub wal_appends: u64,
    /// Total durability barrier (sync/fsync) calls
    pub sync_calls: u64,
    /// Total bytes written to WAL segments
    pub bytes_written: u64,
    /// Total nanoseconds spent in sync/fsync calls
    pub sync_nanos: u64,
}

/// WAL writer with configurable durability modes.
///
/// The writer manages WAL segments and handles record appending with
/// appropriate fsync behavior based on the durability mode.
///
/// # Durability Modes
///
/// - `Cache`: No persistence - records are not written to disk
/// - `Always`: fsync after every record - maximum durability
/// - `Standard`: fsync periodically based on time/count
///
/// # Segment Rotation
///
/// When the current segment exceeds the configured size limit, the writer
/// automatically rotates to a new segment. Closed segments are immutable.
pub struct WalWriter {
    /// Current active segment (None when DurabilityMode::Cache)
    segment: Option<WalSegment>,

    /// Durability mode
    durability: DurabilityMode,

    /// WAL directory
    wal_dir: PathBuf,

    /// Database UUID
    database_uuid: [u8; 16],

    /// Configuration
    config: WalConfig,

    /// Storage codec for encoding
    codec: Box<dyn StorageCodec>,

    /// Bytes written since last fsync (for Standard mode)
    bytes_since_sync: u64,

    /// Writes since last fsync (for Standard mode)
    writes_since_sync: usize,

    /// Last fsync time (for Standard mode)
    last_sync_time: Instant,

    /// Current segment number
    current_segment_number: u64,

    /// Whether there is data written but not yet fsynced
    has_unsynced_data: bool,

    /// In-memory metadata for the current active segment.
    /// `None` in Cache mode (no WAL persistence).
    current_segment_meta: Option<SegmentMeta>,

    /// Cumulative: total WAL record appends
    total_wal_appends: u64,
    /// Cumulative: total sync/fsync calls
    total_sync_calls: u64,
    /// Cumulative: total bytes written to WAL segments
    total_bytes_written: u64,
    /// Cumulative: total nanoseconds spent in sync/fsync calls
    total_sync_nanos: u64,
}

impl WalWriter {
    /// Create a new WAL writer.
    ///
    /// If the WAL directory contains existing segments, the writer will
    /// either open the last segment for appending or create a new one.
    pub fn new(
        wal_dir: PathBuf,
        database_uuid: [u8; 16],
        durability: DurabilityMode,
        config: WalConfig,
        codec: Box<dyn StorageCodec>,
    ) -> std::io::Result<Self> {
        // For Cache mode, don't create any files
        if !durability.requires_wal() {
            return Ok(WalWriter {
                segment: None,
                durability,
                wal_dir,
                database_uuid,
                config,
                codec,
                bytes_since_sync: 0,
                writes_since_sync: 0,
                last_sync_time: Instant::now(),
                current_segment_number: 0,
                current_segment_meta: None,
                has_unsynced_data: false,
                total_wal_appends: 0,
                total_sync_calls: 0,
                total_bytes_written: 0,
                total_sync_nanos: 0,
            });
        }

        // Ensure WAL directory exists
        std::fs::create_dir_all(&wal_dir)?;

        // Find the latest segment
        let latest_segment = Self::find_latest_segment(&wal_dir);

        let (segment, segment_number, is_reopened) = match latest_segment {
            Some(num) => {
                // Try to open existing segment for appending
                match WalSegment::open_append(&wal_dir, num) {
                    Ok(seg) => (seg, num, true),
                    Err(_) => {
                        // Segment might be corrupted or closed, create new one
                        let new_num = num + 1;
                        let seg = WalSegment::create(&wal_dir, new_num, database_uuid)?;
                        (seg, new_num, false)
                    }
                }
            }
            None => {
                // No existing segments, create first one
                let seg = WalSegment::create(&wal_dir, 1, database_uuid)?;
                (seg, 1, false)
            }
        };

        // Build initial segment metadata
        let current_segment_meta = if is_reopened {
            // Reopening an existing segment — rebuild metadata from its records
            Self::rebuild_meta_for_segment(&wal_dir, segment_number)
        } else {
            Some(SegmentMeta::new_empty(segment_number))
        };

        Ok(WalWriter {
            segment: Some(segment),
            durability,
            wal_dir,
            database_uuid,
            config,
            codec,
            bytes_since_sync: 0,
            writes_since_sync: 0,
            last_sync_time: Instant::now(),
            current_segment_number: segment_number,
            current_segment_meta,
            has_unsynced_data: false,
            total_wal_appends: 0,
            total_sync_calls: 0,
            total_bytes_written: 0,
            total_sync_nanos: 0,
        })
    }

    /// Append a record to the WAL.
    ///
    /// Respects the configured durability mode:
    /// - `Cache`: No-op, returns immediately
    /// - `Always`: Writes and fsyncs before returning
    /// - `Standard`: Writes, fsyncs periodically
    pub fn append(&mut self, record: &WalRecord) -> std::io::Result<()> {
        // Cache mode: no persistence
        if !self.durability.requires_wal() {
            return Ok(());
        }

        let segment = self
            .segment
            .as_mut()
            .expect("Segment should exist for non-Cache mode");

        // Serialize record
        let record_bytes = record.to_bytes();

        // Encode through codec
        let encoded = self.codec.encode(&record_bytes);

        // Check if we need to rotate before writing
        if segment.size() + encoded.len() as u64 > self.config.segment_size {
            self.rotate_segment()?;
        }

        // Write to segment
        let segment = self.segment.as_mut().unwrap();
        segment.write(&encoded)?;

        // Track metadata for the current segment
        if let Some(ref mut meta) = self.current_segment_meta {
            meta.track_record(record.txn_id, record.timestamp);
        }

        self.total_wal_appends += 1;
        self.total_bytes_written += encoded.len() as u64;

        debug!(target: "strata::wal", txn_id = record.txn_id, record_bytes = encoded.len(), segment = self.current_segment_number, "WAL record appended");

        self.bytes_since_sync += encoded.len() as u64;
        self.writes_since_sync += 1;
        self.has_unsynced_data = true;

        // Handle sync based on durability mode
        self.maybe_sync()?;

        Ok(())
    }

    /// Handle fsync based on durability mode.
    fn maybe_sync(&mut self) -> std::io::Result<()> {
        match self.durability {
            DurabilityMode::Always => {
                // Always sync immediately
                if let Some(ref mut segment) = self.segment {
                    let start = Instant::now();
                    segment.sync()?;
                    let elapsed = start.elapsed();
                    self.total_sync_calls += 1;
                    self.total_sync_nanos += elapsed.as_nanos() as u64;
                }
                self.reset_sync_counters();
            }
            DurabilityMode::Standard { .. } => {
                // Standard mode: fsync is deferred to the background flush thread (#969).
                // Data is already written to the BufWriter by append().
                // The background thread periodically calls sync_if_overdue().
            }
            DurabilityMode::Cache => {
                // No sync needed
            }
        }

        Ok(())
    }

    /// Reset sync tracking counters.
    fn reset_sync_counters(&mut self) {
        self.bytes_since_sync = 0;
        self.writes_since_sync = 0;
        self.last_sync_time = Instant::now();
        self.has_unsynced_data = false;
    }

    /// Rotate to a new segment.
    ///
    /// Closes the current segment (making it immutable) and creates a new one.
    fn rotate_segment(&mut self) -> std::io::Result<()> {
        let old_segment = self.current_segment_number;

        // Close current segment
        if let Some(ref mut segment) = self.segment {
            segment.close()?;
        }

        // Write .meta for the closed segment
        if let Some(ref meta) = self.current_segment_meta {
            if !meta.is_empty() {
                if let Err(e) = meta.write_to_file(&self.wal_dir) {
                    warn!(target: "strata::wal", segment = old_segment, error = %e, "Failed to write .meta sidecar");
                }
            }
        }

        // Create new segment
        self.current_segment_number += 1;
        let new_segment = WalSegment::create(
            &self.wal_dir,
            self.current_segment_number,
            self.database_uuid,
        )?;

        self.segment = Some(new_segment);
        self.current_segment_meta = Some(SegmentMeta::new_empty(self.current_segment_number));
        self.reset_sync_counters();

        info!(target: "strata::wal", old_segment, new_segment = self.current_segment_number, "WAL segment rotated");

        Ok(())
    }

    /// Force flush any buffered data to disk.
    ///
    /// This ensures all written records are persisted, regardless of
    /// durability mode settings.
    pub fn flush(&mut self) -> std::io::Result<()> {
        if let Some(ref mut segment) = self.segment {
            let start = Instant::now();
            segment.sync()?;
            let elapsed = start.elapsed();
            self.total_sync_calls += 1;
            self.total_sync_nanos += elapsed.as_nanos() as u64;
        }
        self.reset_sync_counters();
        debug!(target: "strata::wal", segment = self.current_segment_number, "WAL flushed");
        Ok(())
    }

    /// Sync if the batched interval has elapsed and there is unsynced data.
    ///
    /// Call this periodically (e.g., from a maintenance timer) to ensure
    /// Standard mode honors its `interval_ms` even when no new writes arrive.
    /// Returns `true` if a sync was performed.
    pub fn sync_if_overdue(&mut self) -> std::io::Result<bool> {
        if !self.has_unsynced_data {
            return Ok(false);
        }

        if let DurabilityMode::Standard { interval_ms, .. } = self.durability {
            if self.last_sync_time.elapsed().as_millis() as u64 >= interval_ms {
                if let Some(ref mut segment) = self.segment {
                    let start = Instant::now();
                    segment.sync()?;
                    let elapsed = start.elapsed();
                    self.total_sync_calls += 1;
                    self.total_sync_nanos += elapsed.as_nanos() as u64;
                }
                self.reset_sync_counters();
                debug!(target: "strata::wal", segment = self.current_segment_number, "WAL periodic sync");
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get the current segment number.
    pub fn current_segment(&self) -> u64 {
        self.current_segment_number
    }

    /// Get the current segment size in bytes.
    pub fn current_segment_size(&self) -> u64 {
        self.segment
            .as_ref()
            .map(|s: &WalSegment| s.size())
            .unwrap_or(SEGMENT_HEADER_SIZE_V2 as u64)
    }

    /// Get a snapshot of cumulative WAL counters.
    pub fn counters(&self) -> WalCounters {
        WalCounters {
            wal_appends: self.total_wal_appends,
            sync_calls: self.total_sync_calls,
            bytes_written: self.total_bytes_written,
            sync_nanos: self.total_sync_nanos,
        }
    }

    /// Get the WAL directory path.
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Get the in-memory metadata for the current active segment.
    ///
    /// Returns `None` in Cache mode.
    pub fn current_segment_meta(&self) -> Option<&SegmentMeta> {
        self.current_segment_meta.as_ref()
    }

    /// Rebuild metadata for an existing segment by scanning its records.
    ///
    /// Returns `Some(meta)` on success, or `Some(empty_meta)` if the segment
    /// cannot be read (best-effort).
    fn rebuild_meta_for_segment(wal_dir: &Path, segment_number: u64) -> Option<SegmentMeta> {
        let reader = WalReader::new(Box::new(crate::codec::IdentityCodec));
        match reader.read_segment(wal_dir, segment_number) {
            Ok((records, _, _, _)) => {
                let mut meta = SegmentMeta::new_empty(segment_number);
                for record in &records {
                    meta.track_record(record.txn_id, record.timestamp);
                }
                Some(meta)
            }
            Err(e) => {
                warn!(target: "strata::wal", segment = segment_number, error = %e, "Failed to rebuild segment meta, starting empty");
                Some(SegmentMeta::new_empty(segment_number))
            }
        }
    }

    /// Find the latest segment number in the WAL directory.
    fn find_latest_segment(dir: &Path) -> Option<u64> {
        std::fs::read_dir(dir)
            .ok()?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                if name.starts_with("wal-") && name.ends_with(".seg") {
                    // Extract segment number from "wal-NNNNNN.seg"
                    let num_str = &name[4..10];
                    num_str.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .max()
    }

    /// List all segment numbers in order.
    pub fn list_segments(&self) -> std::io::Result<Vec<u64>> {
        let mut segments = Vec::new();

        for entry in std::fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("wal-") && name.ends_with(".seg") {
                if let Ok(num) = name[4..10].parse::<u64>() {
                    segments.push(num);
                }
            }
        }

        segments.sort();
        Ok(segments)
    }

    /// Close the writer, ensuring all data is flushed.
    pub fn close(mut self) -> std::io::Result<()> {
        self.flush()?;

        // Write .meta for the current segment before closing
        if let Some(ref meta) = self.current_segment_meta {
            if !meta.is_empty() {
                if let Err(e) = meta.write_to_file(&self.wal_dir) {
                    warn!(target: "strata::wal", segment = self.current_segment_number, error = %e, "Failed to write .meta sidecar on close");
                }
            }
        }

        if let Some(ref mut segment) = self.segment {
            segment.close()?;
        }
        Ok(())
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if self.has_unsynced_data {
            if let Some(ref mut segment) = self.segment {
                let _ = segment.sync();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::IdentityCodec;
    use tempfile::tempdir;

    fn make_writer(dir: &Path, durability: DurabilityMode) -> WalWriter {
        WalWriter::new(
            dir.to_path_buf(),
            [1u8; 16],
            durability,
            WalConfig::for_testing(),
            Box::new(IdentityCodec),
        )
        .unwrap()
    }

    fn make_record(txn_id: u64) -> WalRecord {
        WalRecord::new(txn_id, [1u8; 16], 12345, vec![1, 2, 3])
    }

    #[test]
    fn test_inmemory_mode_no_files() {
        let dir = tempdir().unwrap();

        let mut writer = make_writer(dir.path(), DurabilityMode::Cache);
        writer.append(&make_record(1)).unwrap();
        writer.append(&make_record(2)).unwrap();

        // No files should be created
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn test_strict_mode_creates_segment() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);
        writer.append(&make_record(1)).unwrap();

        // Segment should exist
        assert!(WalSegment::segment_path(&wal_dir, 1).exists());
    }

    #[test]
    fn test_segment_rotation() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use very small segment size to force rotation
        let config = WalConfig::new()
            .with_segment_size(100) // Very small
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.clone(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            Box::new(IdentityCodec),
        )
        .unwrap();

        // Write enough records to trigger rotation
        for i in 0..10 {
            writer
                .append(&WalRecord::new(i, [1u8; 16], 0, vec![0; 50]))
                .unwrap();
        }

        // Should have multiple segments
        let segments = writer.list_segments().unwrap();
        assert!(
            segments.len() > 1,
            "Should have rotated to multiple segments"
        );
    }

    #[test]
    fn test_flush() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 10000,
                batch_size: 10000,
            },
        );

        writer.append(&make_record(1)).unwrap();
        writer.flush().unwrap();

        // File should be synced
        assert!(WalSegment::segment_path(&wal_dir, 1).exists());
    }

    #[test]
    fn test_close() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);
        writer.append(&make_record(1)).unwrap();
        writer.close().unwrap();

        // Should be able to reopen
        let writer2 = make_writer(&wal_dir, DurabilityMode::Always);
        assert!(writer2.current_segment() >= 1);
    }

    #[test]
    fn test_resume_existing_segment() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write some data
        {
            let mut writer = make_writer(&wal_dir, DurabilityMode::Always);
            writer.append(&make_record(1)).unwrap();
            writer.flush().unwrap();
            // Don't close, just drop
        }

        // Reopen and continue
        {
            let mut writer = make_writer(&wal_dir, DurabilityMode::Always);
            writer.append(&make_record(2)).unwrap();
            writer.flush().unwrap();
        }

        // Should have appended to existing or created new
        let writer = make_writer(&wal_dir, DurabilityMode::Always);
        assert!(writer.current_segment() >= 1);
    }

    #[test]
    fn test_batched_mode_sync_threshold() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalConfig::new()
            .with_segment_size(1024 * 1024)
            .with_buffered_sync_bytes(100); // Small threshold

        let mut writer = WalWriter::new(
            wal_dir.clone(),
            [1u8; 16],
            DurabilityMode::Standard {
                interval_ms: 10000,
                batch_size: 100,
            },
            config,
            Box::new(IdentityCodec),
        )
        .unwrap();

        // Write records to trigger sync
        for i in 0..20 {
            writer.append(&make_record(i)).unwrap();
        }

        // Segment should have data
        assert!(writer.current_segment_size() > SEGMENT_HEADER_SIZE_V2 as u64);
    }
}
