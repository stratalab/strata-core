//! WAL-only compaction
//!
//! Removes WAL segments that are fully covered by a snapshot watermark.
//! This is the safest compaction mode - it only removes data that is
//! guaranteed to be recoverable from the snapshot.
//!
//! # Algorithm
//!
//! 1. Get snapshot watermark (transaction ID) from MANIFEST
//! 2. List all WAL segments
//! 3. For each segment (except the active segment):
//!    - Read all records and find the highest txn_id
//!    - If highest txn_id <= watermark, segment is covered
//!    - Delete covered segments
//! 4. Track reclaimed bytes and segment count
//!
//! # Safety
//!
//! - Never removes the active segment
//! - Only removes segments fully covered by snapshot
//! - Requires a valid snapshot to exist

use crate::format::segment_meta::SegmentMeta;
use crate::format::{
    ManifestManager, SegmentHeader, WalRecord, WalRecordError, SEGMENT_HEADER_SIZE,
};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{CompactInfo, CompactMode, CompactionError};

/// WAL-only compactor
///
/// Removes WAL segments covered by snapshot watermark.
pub struct WalOnlyCompactor {
    wal_dir: PathBuf,
    manifest: Arc<Mutex<ManifestManager>>,
}

impl WalOnlyCompactor {
    /// Create a new WAL-only compactor
    pub fn new(wal_dir: PathBuf, manifest: Arc<Mutex<ManifestManager>>) -> Self {
        WalOnlyCompactor { wal_dir, manifest }
    }

    /// Perform WAL-only compaction
    ///
    /// Removes WAL segments whose highest txn_id <= snapshot watermark.
    /// Returns information about what was compacted.
    ///
    /// # Errors
    ///
    /// - `NoSnapshot`: No snapshot exists to compact against
    /// - `Io`: File system errors during segment access/deletion
    pub fn compact(&self) -> Result<CompactInfo, CompactionError> {
        self.compact_with_active_override(0)
    }

    /// Perform WAL-only compaction with an explicit active segment override.
    ///
    /// The `writer_active_segment` parameter provides the WAL writer's in-memory
    /// current segment number, which may be ahead of the MANIFEST value if
    /// rotations occurred since last recovery/checkpoint. The compactor uses
    /// `max(manifest_active, writer_active_segment)` as the safe boundary.
    ///
    /// # Errors
    ///
    /// - `NoSnapshot`: No snapshot exists to compact against
    /// - `Io`: File system errors during segment access/deletion
    pub fn compact_with_active_override(
        &self,
        writer_active_segment: u64,
    ) -> Result<CompactInfo, CompactionError> {
        info!(target: "strata::compaction", "WAL compaction started");
        let start_time = std::time::Instant::now();
        let mut info = CompactInfo::new(CompactMode::WALOnly);

        // Get effective watermark from MANIFEST — max of flush watermark and
        // snapshot watermark. Both sources produce retention-complete state
        // on reopen: segment flush persists records into on-disk SSTs, and
        // snapshot install (T3-E5 + follow-up) reinstalls every logical
        // entry including tombstones, TTL, and branch metadata. WAL segments
        // covered by either source are safe to delete. See
        // `effective_watermark` for the retention contract.
        let (watermark, manifest_active) = {
            let manifest = self.manifest.lock();
            let m = manifest.manifest();

            let watermark = effective_watermark(m).ok_or(CompactionError::NoSnapshot)?;
            let active_segment = m.active_wal_segment;

            (watermark, active_segment)
        };

        // Use the higher of manifest vs writer's in-memory segment number.
        // The writer may have rotated since the last MANIFEST update.
        let safe_active = manifest_active.max(writer_active_segment);

        info.snapshot_watermark = Some(watermark);

        // List all WAL segments
        let segments = self.list_segments()?;

        for segment_number in segments {
            // Never remove active segment or any segment at/above it
            if segment_number >= safe_active {
                continue;
            }

            // Check if segment is fully covered by snapshot
            match self.segment_covered_by_watermark(segment_number, watermark) {
                Ok(true) => {
                    let segment_path = segment_path(&self.wal_dir, segment_number);

                    match std::fs::metadata(&segment_path) {
                        Ok(metadata) => {
                            let segment_size = metadata.len();

                            if let Err(e) = std::fs::remove_file(&segment_path) {
                                // Log but continue - partial compaction is acceptable
                                warn!(
                                    target: "strata::compaction",
                                    segment = segment_number,
                                    error = %e,
                                    "Failed to remove WAL segment"
                                );
                                continue;
                            }

                            // Also remove the .meta sidecar (non-fatal if missing)
                            let meta_path = SegmentMeta::meta_path(&self.wal_dir, segment_number);
                            if meta_path.exists() {
                                if let Err(e) = std::fs::remove_file(&meta_path) {
                                    warn!(
                                        target: "strata::compaction",
                                        segment = segment_number,
                                        error = %e,
                                        "Failed to remove .meta sidecar"
                                    );
                                }
                            }

                            info.reclaimed_bytes += segment_size;
                            info.wal_segments_removed += 1;
                        }
                        Err(e) => {
                            // Segment might have been removed by another process
                            warn!(
                                target: "strata::compaction",
                                segment = segment_number,
                                error = %e,
                                "Failed to stat WAL segment"
                            );
                        }
                    }
                }
                Ok(false) => {
                    // Segment has records beyond watermark, keep it
                }
                Err(e) => {
                    // Error reading segment - skip it for safety
                    warn!(
                        target: "strata::compaction",
                        segment = segment_number,
                        error = %e,
                        "Failed to check WAL segment coverage"
                    );
                }
            }
        }

        info.duration_ms = start_time.elapsed().as_millis() as u64;
        info.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        info!(target: "strata::compaction", "WAL compaction completed");
        Ok(info)
    }

    /// List all WAL segment numbers in sorted order
    fn list_segments(&self) -> Result<Vec<u64>, CompactionError> {
        let mut segments = Vec::new();

        if !self.wal_dir.exists() {
            return Ok(segments);
        }

        for entry in std::fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();

            // Match wal-NNNNNN.seg pattern
            if name.starts_with("wal-") && name.ends_with(".seg") && name.len() == 14 {
                if let Ok(num) = name[4..10].parse::<u64>() {
                    segments.push(num);
                }
            }
        }

        segments.sort_unstable();
        Ok(segments)
    }

    /// Check if a segment is fully covered by the snapshot watermark.
    ///
    /// A segment is covered if its highest txn_id <= watermark.
    /// Tries `.meta` sidecar first for O(1) check; falls back to full scan.
    fn segment_covered_by_watermark(
        &self,
        segment_number: u64,
        watermark: u64,
    ) -> Result<bool, CompactionError> {
        // Try .meta sidecar first (O(1) check)
        match SegmentMeta::read_from_file(&self.wal_dir, segment_number) {
            Ok(Some(meta)) if meta.segment_number == segment_number => {
                if meta.is_empty() {
                    debug!(target: "strata::compaction", segment = segment_number, "Empty segment (via .meta), considered covered");
                    return Ok(true);
                }
                let covered = meta.max_txn_id.as_u64() <= watermark;
                debug!(target: "strata::compaction", segment = segment_number, max_txn_id = meta.max_txn_id.as_u64(), watermark, covered, "Coverage check via .meta");
                return Ok(covered);
            }
            Ok(Some(_)) => {
                warn!(target: "strata::compaction", segment = segment_number, "Segment meta has mismatched segment number, falling back to full scan");
            }
            Ok(None) => {
                // No .meta file — fall through to full scan
            }
            Err(e) => {
                warn!(target: "strata::compaction", segment = segment_number, error = %e, "Corrupted .meta sidecar, falling back to full scan");
            }
        }

        self.segment_covered_by_watermark_full_scan(segment_number, watermark)
    }

    /// Full-scan fallback: read all records to determine max txn_id.
    fn segment_covered_by_watermark_full_scan(
        &self,
        segment_number: u64,
        watermark: u64,
    ) -> Result<bool, CompactionError> {
        let segment_path = segment_path(&self.wal_dir, segment_number);
        let file_data = std::fs::read(&segment_path)?;

        // Validate segment header (need at least v1 header size)
        if file_data.len() < SEGMENT_HEADER_SIZE {
            return Err(CompactionError::internal(format!(
                "Segment {} too small for header",
                segment_number
            )));
        }

        let header = SegmentHeader::from_bytes_slice(&file_data).ok_or_else(|| {
            CompactionError::internal(format!("Invalid segment {} header", segment_number))
        })?;

        if !header.is_valid() {
            return Err(CompactionError::internal(format!(
                "Segment {} has invalid magic",
                segment_number
            )));
        }

        // Determine actual header size based on format version
        let actual_header_size = if header.format_version >= 2 {
            crate::format::SEGMENT_HEADER_SIZE_V2
        } else {
            SEGMENT_HEADER_SIZE
        };

        // Empty segment (just header) is considered covered
        if file_data.len() <= actual_header_size {
            return Ok(true);
        }

        // Find highest txn_id in segment
        let mut cursor = actual_header_size;
        let mut max_txn_id = 0u64;

        while cursor < file_data.len() {
            match WalRecord::from_bytes(&file_data[cursor..]) {
                Ok((record, consumed)) => {
                    max_txn_id = max_txn_id.max(record.txn_id.as_u64());
                    cursor += consumed;
                }
                Err(WalRecordError::InsufficientData) => {
                    break;
                }
                Err(WalRecordError::ChecksumMismatch { .. }) => {
                    break;
                }
                Err(e) => {
                    warn!(target: "strata::compaction", error = %e, cursor = cursor,
                        "Unexpected WAL record error during compaction scan, stopping");
                    break;
                }
            }
        }

        // Segment is covered if all records are at or below watermark
        Ok(max_txn_id <= watermark)
    }

    /// Get the WAL directory path
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }
}

/// Compute the effective watermark for WAL truncation.
///
/// Returns the highest txn id whose data is fully reconstructible on reopen
/// without the WAL: either flushed to on-disk SSTs (`flushed_through_commit_id`)
/// or captured in a retention-complete snapshot (`snapshot_watermark`).
/// A WAL segment covered by the max of these is safe to delete.
///
/// The T3-E5 follow-up made snapshot coverage trustworthy by wiring
/// tombstones, TTL, and Branch metadata through the checkpoint DTOs and
/// install decoder. Before that work, WAL retention could only trust the
/// flush watermark (the original `#1730` defensive hardening); after it,
/// either source is sufficient.
fn effective_watermark(manifest: &crate::format::Manifest) -> Option<u64> {
    match (
        manifest.flushed_through_commit_id,
        manifest.snapshot_watermark,
    ) {
        (Some(flushed), Some(snap)) => Some(flushed.max(snap)),
        (Some(flushed), None) => Some(flushed),
        (None, Some(snap)) => Some(snap),
        (None, None) => None,
    }
}

/// Generate segment file path
///
/// Format: `wal-NNNNNN.seg` where NNNNNN is zero-padded segment number.
fn segment_path(dir: &Path, segment_number: u64) -> PathBuf {
    dir.join(format!("wal-{:06}.seg", segment_number))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::WalSegment;
    use strata_core::id::{CommitVersion, TxnId};
    use tempfile::tempdir;

    fn test_uuid() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    fn setup_test_env() -> (tempfile::TempDir, PathBuf, Arc<Mutex<ManifestManager>>) {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("WAL");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let manifest_path = dir.path().join("MANIFEST");
        let manifest =
            ManifestManager::create(manifest_path, test_uuid(), "identity".to_string()).unwrap();

        (dir, wal_dir, Arc::new(Mutex::new(manifest)))
    }

    fn create_segment_with_records(
        wal_dir: &Path,
        segment_number: u64,
        txn_ids: &[u64],
    ) -> std::io::Result<()> {
        let mut segment = WalSegment::create(wal_dir, segment_number, test_uuid())?;

        for &txn_id in txn_ids {
            let record = WalRecord::new(
                TxnId(txn_id),
                test_uuid(),
                txn_id * 1000,
                vec![txn_id as u8; 10],
            );
            segment.write(&record.to_bytes())?;
        }

        segment.close()?;
        Ok(())
    }

    #[test]
    fn test_segment_path_format() {
        let path = segment_path(Path::new("/tmp/wal"), 1);
        assert_eq!(path.to_str().unwrap(), "/tmp/wal/wal-000001.seg");

        let path = segment_path(Path::new("/tmp/wal"), 999999);
        assert_eq!(path.to_str().unwrap(), "/tmp/wal/wal-999999.seg");
    }

    #[test]
    fn test_list_segments_empty() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        let compactor = WalOnlyCompactor::new(wal_dir, manifest);
        let segments = compactor.list_segments().unwrap();

        assert!(segments.is_empty());
    }

    #[test]
    fn test_list_segments() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create some segments
        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 3, &[4, 5, 6]).unwrap();
        create_segment_with_records(&wal_dir, 5, &[7, 8, 9]).unwrap();

        let compactor = WalOnlyCompactor::new(wal_dir, manifest);
        let segments = compactor.list_segments().unwrap();

        assert_eq!(segments, vec![1, 3, 5]);
    }

    #[test]
    fn test_compact_no_snapshot() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        let compactor = WalOnlyCompactor::new(wal_dir, manifest);
        let result = compactor.compact();

        assert!(matches!(result, Err(CompactionError::NoSnapshot)));
    }

    #[test]
    fn test_compact_removes_covered_segments() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create segments with increasing txn_ids
        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[4, 5, 6]).unwrap();
        create_segment_with_records(&wal_dir, 3, &[7, 8, 9]).unwrap();

        // Set flush watermark at txn 6 and active segment at 4
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(6)).unwrap();
            m.manifest_mut().active_wal_segment = 4;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact().unwrap();

        // Segments 1 and 2 should be removed (max txn 3 and 6 <= watermark 6)
        // Segment 3 should remain (max txn 9 > watermark 6)
        assert_eq!(info.wal_segments_removed, 2);
        assert!(info.reclaimed_bytes > 0);
        assert_eq!(info.snapshot_watermark, Some(6));

        // Verify files
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(!segment_path(&wal_dir, 2).exists());
        assert!(segment_path(&wal_dir, 3).exists());
    }

    #[test]
    fn test_compact_never_removes_active_segment() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create a segment
        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();

        // Set watermark high but active segment is 1
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(100)).unwrap();
            m.manifest_mut().active_wal_segment = 1; // Segment 1 is active
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact().unwrap();

        // Should not remove active segment
        assert_eq!(info.wal_segments_removed, 0);
        assert!(segment_path(&wal_dir, 1).exists());
    }

    #[test]
    fn test_compact_empty_wal() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Set flush watermark but no segments
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(100)).unwrap();
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir, manifest);
        let info = compactor.compact().unwrap();

        assert_eq!(info.wal_segments_removed, 0);
        assert_eq!(info.reclaimed_bytes, 0);
    }

    #[test]
    fn test_compact_empty_segment() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create an empty segment (just header)
        let segment = WalSegment::create(&wal_dir, 1, test_uuid()).unwrap();
        drop(segment);

        // Create another with records
        create_segment_with_records(&wal_dir, 2, &[1, 2, 3]).unwrap();

        // Set watermark and active segment
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(5)).unwrap();
            m.manifest_mut().active_wal_segment = 10;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact().unwrap();

        // Both segments should be removed (empty segment is always covered)
        assert_eq!(info.wal_segments_removed, 2);
    }

    #[test]
    fn test_segment_covered_by_watermark() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();

        let compactor = WalOnlyCompactor::new(wal_dir, manifest);

        // Watermark 3 should cover segment with max txn 3
        assert!(compactor.segment_covered_by_watermark(1, 3).unwrap());

        // Watermark 2 should not cover segment with max txn 3
        assert!(!compactor.segment_covered_by_watermark(1, 2).unwrap());

        // Watermark 10 should cover segment
        assert!(compactor.segment_covered_by_watermark(1, 10).unwrap());
    }

    #[test]
    fn test_compact_info_metrics() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[4, 5, 6]).unwrap();

        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(10)).unwrap();
            m.manifest_mut().active_wal_segment = 10;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir, manifest);
        let info = compactor.compact().unwrap();

        assert_eq!(info.mode, CompactMode::WALOnly);
        assert_eq!(info.wal_segments_removed, 2);
        assert!(info.reclaimed_bytes > 0);
        assert!(info.duration_ms < 10000); // Should complete in reasonable time
        assert!(info.timestamp > 0);
    }

    #[test]
    fn test_compact_removes_meta_with_segment() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create segments with records
        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[4, 5, 6]).unwrap();

        // Write .meta files for both segments
        let mut meta1 = SegmentMeta::new_empty(1);
        meta1.track_record(TxnId(1), 1000);
        meta1.track_record(TxnId(2), 2000);
        meta1.track_record(TxnId(3), 3000);
        meta1.write_to_file(&wal_dir).unwrap();

        let mut meta2 = SegmentMeta::new_empty(2);
        meta2.track_record(TxnId(4), 4000);
        meta2.track_record(TxnId(5), 5000);
        meta2.track_record(TxnId(6), 6000);
        meta2.write_to_file(&wal_dir).unwrap();

        // Verify .meta files exist
        assert!(SegmentMeta::meta_path(&wal_dir, 1).exists());
        assert!(SegmentMeta::meta_path(&wal_dir, 2).exists());

        // Set watermark to cover segment 1 only
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(3)).unwrap();
            m.manifest_mut().active_wal_segment = 10;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact().unwrap();

        assert_eq!(info.wal_segments_removed, 1);

        // Segment 1 and its .meta should be removed
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(!SegmentMeta::meta_path(&wal_dir, 1).exists());

        // Segment 2 and its .meta should remain
        assert!(segment_path(&wal_dir, 2).exists());
        assert!(SegmentMeta::meta_path(&wal_dir, 2).exists());
    }

    #[test]
    fn test_compact_uses_meta_for_coverage_check() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create segment with records
        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();

        // Write a .meta file (this avoids the full scan path)
        let mut meta = SegmentMeta::new_empty(1);
        meta.track_record(TxnId(1), 1000);
        meta.track_record(TxnId(2), 2000);
        meta.track_record(TxnId(3), 3000);
        meta.write_to_file(&wal_dir).unwrap();

        // Set watermark at exactly max_txn_id=3 and active segment high
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(3)).unwrap();
            m.manifest_mut().active_wal_segment = 10;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact().unwrap();

        // Should be removed (meta.max_txn_id=3 <= watermark=3)
        assert_eq!(info.wal_segments_removed, 1);
        assert!(!segment_path(&wal_dir, 1).exists());
    }

    // ========================================================================
    // D-6: Stale active segment guard tests
    // ========================================================================

    #[test]
    fn test_compact_with_stale_manifest_uses_override() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create segments 1-5, all with txn_ids <= 10 (covered by watermark)
        for seg in 1..=5 {
            create_segment_with_records(&wal_dir, seg, &[seg * 2]).unwrap();
        }

        // MANIFEST says active=3 (stale), but writer is really at segment 5.
        // safe_active = max(3, 5) = 5. Segments >= 5 are protected.
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(100)).unwrap(); // high watermark covers all
            m.manifest_mut().active_wal_segment = 3;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact_with_active_override(5).unwrap();

        // Segments 1-4 deleted (< safe_active=5, covered by watermark)
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(!segment_path(&wal_dir, 2).exists());
        assert!(!segment_path(&wal_dir, 3).exists());
        assert!(!segment_path(&wal_dir, 4).exists());

        // Segment 5 must NOT be deleted (it IS the active segment)
        assert!(segment_path(&wal_dir, 5).exists());
        assert_eq!(info.wal_segments_removed, 4);
    }

    #[test]
    fn test_compact_override_uses_max() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        // Create segments 1-5
        for seg in 1..=5 {
            create_segment_with_records(&wal_dir, seg, &[seg]).unwrap();
        }

        // MANIFEST active=5, override=3 → max(5,3)=5
        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(100)).unwrap();
            m.manifest_mut().active_wal_segment = 5;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact_with_active_override(3).unwrap();

        // Segments 1-4 removed, segment 5 (active per manifest) protected
        assert_eq!(info.wal_segments_removed, 4);
        assert!(segment_path(&wal_dir, 5).exists());
    }

    #[test]
    fn test_compact_zero_override_delegates_to_manifest() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[4, 5, 6]).unwrap();

        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(10)).unwrap();
            m.manifest_mut().active_wal_segment = 3;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);

        // Override=0 → max(3,0)=3 → same as original compact()
        let info = compactor.compact_with_active_override(0).unwrap();
        assert_eq!(info.wal_segments_removed, 2);
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(!segment_path(&wal_dir, 2).exists());
    }

    #[test]
    fn test_compact_override_interacts_with_watermark() {
        // Verifies that BOTH the watermark check and the active-segment guard
        // are required for deletion. A segment below safe_active that is NOT
        // covered by the watermark must survive.
        let (_dir, wal_dir, manifest) = setup_test_env();

        // seg 1: max_txn_id=3  (covered by watermark=5)
        // seg 2: max_txn_id=10 (NOT covered by watermark=5)
        // seg 3: max_txn_id=4  (covered by watermark=5)
        // seg 4: active segment (writer override)
        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[8, 9, 10]).unwrap();
        create_segment_with_records(&wal_dir, 3, &[4]).unwrap();
        create_segment_with_records(&wal_dir, 4, &[11]).unwrap();

        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(5)).unwrap();
            m.manifest_mut().active_wal_segment = 2; // stale
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        // Writer is really at segment 4 → safe_active = max(2, 4) = 4
        let info = compactor.compact_with_active_override(4).unwrap();

        // seg 1: < 4 AND covered → DELETED
        assert!(!segment_path(&wal_dir, 1).exists());
        // seg 2: < 4 BUT max_txn_id=10 > watermark=5 → KEPT
        assert!(segment_path(&wal_dir, 2).exists());
        // seg 3: < 4 AND covered → DELETED
        assert!(!segment_path(&wal_dir, 3).exists());
        // seg 4: >= safe_active → KEPT (active)
        assert!(segment_path(&wal_dir, 4).exists());

        assert_eq!(info.wal_segments_removed, 2);
    }

    // ========================================================================
    // Epic 5: flush watermark tests
    // ========================================================================

    #[test]
    fn test_wal_truncation_uses_flush_watermark() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[4, 5, 6]).unwrap();
        create_segment_with_records(&wal_dir, 3, &[7, 8, 9]).unwrap();

        // Set ONLY flush watermark (no snapshot), active segment high
        {
            let mut m = manifest.lock();
            m.manifest_mut().flushed_through_commit_id = Some(6);
            m.manifest_mut().active_wal_segment = 10;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor.compact().unwrap();

        assert_eq!(info.wal_segments_removed, 2);
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(!segment_path(&wal_dir, 2).exists());
        assert!(segment_path(&wal_dir, 3).exists());
    }

    #[test]
    fn test_wal_truncation_accepts_snapshot_watermark_alone() {
        // Inverted from the pre-retention `test_wal_truncation_ignores_
        // snapshot_watermark`. Once snapshot install became retention-
        // complete (tombstones, TTL, branch metadata all round-trip), WAL
        // segments covered by the snapshot watermark are safe to delete
        // even without a flush watermark — the snapshot is an authoritative
        // recovery source on reopen.
        let (_dir, wal_dir, manifest) = setup_test_env();

        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[4, 5, 6]).unwrap();

        // Set only snapshot watermark (no flush watermark). watermark=100
        // covers every record in segments 1 and 2.
        {
            let mut m = manifest.lock();
            m.set_snapshot_watermark(1, TxnId(100)).unwrap();
            m.manifest_mut().active_wal_segment = 10;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest);
        let info = compactor
            .compact()
            .expect("snapshot watermark alone must drive successful compaction post-retention");

        assert_eq!(info.wal_segments_removed, 2);
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(!segment_path(&wal_dir, 2).exists());
        assert_eq!(info.snapshot_watermark, Some(100));
    }

    #[test]
    fn test_wal_truncation_no_watermarks_returns_error() {
        let (_dir, wal_dir, manifest) = setup_test_env();

        create_segment_with_records(&wal_dir, 1, &[1, 2, 3]).unwrap();

        {
            let mut m = manifest.lock();
            m.manifest_mut().active_wal_segment = 10;
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir, manifest);
        let result = compactor.compact();
        assert!(matches!(result, Err(CompactionError::NoSnapshot)));
    }

    #[test]
    fn test_compact_stale_manifest_without_override_is_conservative() {
        // Demonstrates why the override matters: without it, the stale manifest
        // active=2 would prevent compacting segment 2 even though it's closed.
        // With override=4, segment 2 becomes eligible (but still needs watermark).
        let (_dir, wal_dir, manifest) = setup_test_env();

        create_segment_with_records(&wal_dir, 1, &[1, 2]).unwrap();
        create_segment_with_records(&wal_dir, 2, &[3, 4]).unwrap();
        create_segment_with_records(&wal_dir, 3, &[5]).unwrap();

        {
            let mut m = manifest.lock();
            m.set_flush_watermark(CommitVersion(10)).unwrap(); // covers everything
            m.manifest_mut().active_wal_segment = 2; // stale: writer is at 4
            m.persist().unwrap();
        }

        let compactor = WalOnlyCompactor::new(wal_dir.clone(), manifest.clone());

        // Without override (compact()): safe_active = max(2,0) = 2
        // Only segment 1 (< 2) is compacted
        let info_no_override = compactor.compact().unwrap();
        assert_eq!(info_no_override.wal_segments_removed, 1);
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(segment_path(&wal_dir, 2).exists()); // protected by stale manifest
        assert!(segment_path(&wal_dir, 3).exists()); // protected by stale manifest

        // Recreate segment 1 for the second run
        create_segment_with_records(&wal_dir, 1, &[1, 2]).unwrap();

        // With override=4: safe_active = max(2,4) = 4
        // Segments 1, 2, 3 (all < 4 and covered) are compacted
        let info_with_override = compactor.compact_with_active_override(4).unwrap();
        assert_eq!(info_with_override.wal_segments_removed, 3);
        assert!(!segment_path(&wal_dir, 1).exists());
        assert!(!segment_path(&wal_dir, 2).exists());
        assert!(!segment_path(&wal_dir, 3).exists());
    }
}
