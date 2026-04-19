//! WAL writer with durability mode support.
//!
//! The writer handles appending WAL records to segments with proper
//! durability guarantees based on the configured mode.

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use strata_core::id::TxnId;

use super::DurabilityMode;
use crate::codec::StorageCodec;
use crate::format::segment_meta::SegmentMeta;
use crate::format::{WalRecord, WalSegment, SEGMENT_HEADER_SIZE_V2};
use crate::wal::config::WalConfig;
use crate::wal::reader::WalReader;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};
use tracing::{debug, error, info, warn};

/// Per-record outer envelope overhead (v3+): `[u32 outer_len][u32 outer_len_crc]`.
///
/// Prefixed to every codec-encoded record by the writer so the reader
/// can find record boundaries on codec-encoded payloads (AES-GCM, ...)
/// that have no parseable length prefix inside the ciphertext.
/// See T3-E12 tracking doc §D1.
pub(crate) const WAL_RECORD_ENVELOPE_OVERHEAD: usize = 8;

/// WAL disk usage summary.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WalDiskUsage {
    /// Total bytes used by WAL segment files.
    pub total_bytes: u64,
    /// Number of WAL segment files.
    pub segment_count: usize,
}

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

/// Snapshot of the dirty WAL prefix captured at the start of a background sync.
#[derive(Debug, Clone)]
struct SyncSnapshot {
    bytes_since_sync: u64,
    writes_since_sync: usize,
    last_sync_time: Instant,
    has_unsynced_data: bool,
}

/// Handle representing an in-flight background sync.
#[must_use = "SyncHandle must be consumed via commit_background_sync or abort_background_sync"]
pub(crate) struct SyncHandle {
    fd: File,
    pending_snapshot: SyncSnapshot,
    consumed: bool,
}

impl SyncHandle {
    /// Returns the cloned file descriptor to fsync outside the WAL lock.
    pub(crate) fn fd(&self) -> &File {
        &self.fd
    }

    fn take_snapshot(mut self) -> SyncSnapshot {
        self.consumed = true;
        self.pending_snapshot.clone()
    }
}

impl Drop for SyncHandle {
    fn drop(&mut self) {
        if !self.consumed {
            #[cfg(debug_assertions)]
            panic!("SyncHandle dropped without commit_background_sync or abort_background_sync");

            #[cfg(not(debug_assertions))]
            error!(
                target: "strata::wal",
                "SyncHandle dropped without being consumed"
            );
        }
    }
}

/// Background sync error recorded after a failed out-of-lock fsync.
#[derive(Debug, Clone)]
pub(crate) struct BgError {
    /// The underlying IO error category.
    pub kind: io::ErrorKind,
    /// Human-readable error message.
    pub message: String,
    /// Timestamp of the first observed failure in the current streak.
    pub first_observed_at: SystemTime,
    /// Number of consecutive failed sync attempts.
    pub failed_sync_count: u64,
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

    /// Whether a background sync is currently in flight.
    sync_in_flight: bool,

    /// Background sync error recorded for Epic 2 halt/resume.
    bg_error: Option<BgError>,

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

    // ── Per-call profiling (STRATA_PROFILE_WAL=1) ──
    profile_wal: bool,
    profile_wal_calls: u64,
    profile_write_ns: u64,
    profile_sync_ns: u64,
    profile_sync_count: u64,
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
                sync_in_flight: false,
                bg_error: None,
                total_wal_appends: 0,
                total_sync_calls: 0,
                total_bytes_written: 0,
                total_sync_nanos: 0,
                profile_wal: false,
                profile_wal_calls: 0,
                profile_write_ns: 0,
                profile_sync_ns: 0,
                profile_sync_count: 0,
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
                    Err(e) => {
                        warn!(target: "strata::wal", segment = num, error = %e,
                            "Failed to reopen WAL segment, creating new segment");
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
            // Reopening an existing segment — rebuild metadata from its records.
            // T3-E12 §D3 Site 4: pass the codec so the reader decodes the
            // on-disk payloads for encrypted WALs; without this, rebuild
            // on an encrypted segment would fail and `.meta` would be empty.
            Self::rebuild_meta_for_segment(codec.as_ref(), &wal_dir, segment_number)
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
            sync_in_flight: false,
            bg_error: None,
            total_wal_appends: 0,
            total_sync_calls: 0,
            total_bytes_written: 0,
            total_sync_nanos: 0,
            profile_wal: std::env::var("STRATA_PROFILE_WAL").is_ok(),
            profile_wal_calls: 0,
            profile_write_ns: 0,
            profile_sync_ns: 0,
            profile_sync_count: 0,
        })
    }

    /// Append a record to the WAL.
    ///
    /// Respects the configured durability mode:
    /// - `Cache`: No-op, returns immediately
    /// - `Always`: Writes and fsyncs before returning
    /// - `Standard`: Writes, fsyncs periodically
    pub fn append(&mut self, record: &WalRecord) -> std::io::Result<()> {
        if !self.durability.requires_wal() {
            return Ok(());
        }

        let record_bytes = record.to_bytes();
        let encoded = self.codec.encode_cow(&record_bytes);
        self.append_inner(&encoded, record.txn_id, record.timestamp)?;
        self.maybe_sync()
    }

    /// Append a pre-serialized WAL record.
    ///
    /// Like `append()`, but accepts already-serialized record bytes (from
    /// `WalRecord::to_bytes()`). This avoids serialization + CRC under the
    /// WAL lock, reducing lock hold time on the hot path.
    pub fn append_pre_serialized(
        &mut self,
        record_bytes: &[u8],
        txn_id: TxnId,
        timestamp: u64,
    ) -> std::io::Result<()> {
        if !self.durability.requires_wal() {
            return Ok(());
        }

        let encoded = self.codec.encode_cow(record_bytes);
        self.append_inner(&encoded, txn_id, timestamp)?;
        self.maybe_sync()?;

        // Per-call profiling (env STRATA_PROFILE_WAL=1)
        if self.profile_wal {
            self.profile_wal_calls += 1;
            if self.profile_wal_calls % 10_000 == 0 {
                let n = 10_000f64;
                eprintln!(
                    "[wal-profile] {} appends | avg: write={:.1}us  sync={:.1}us  \
                     syncs={} ({:.1}% of appends) | total_bytes={}",
                    self.profile_wal_calls,
                    self.profile_write_ns as f64 / n / 1000.0,
                    self.profile_sync_ns as f64 / n / 1000.0,
                    self.profile_sync_count,
                    self.profile_sync_count as f64 / n * 100.0,
                    self.total_bytes_written,
                );
                self.profile_write_ns = 0;
                self.profile_sync_ns = 0;
                self.profile_sync_count = 0;
            }
        }
        Ok(())
    }

    /// Core append logic shared by `append()`, `append_pre_serialized()`,
    /// and `append_and_flush()`.
    ///
    /// Handles segment rotation, writing, metadata tracking, and counter
    /// updates. Callers are responsible for encoding and post-write sync
    /// behavior. The per-record outer envelope
    /// `[u32 outer_len][u32 outer_len_crc]` is written here so the three
    /// caller paths all produce the same on-disk layout (T3-E12 §D1).
    fn append_inner(
        &mut self,
        encoded: &[u8],
        txn_id: TxnId,
        timestamp: u64,
    ) -> std::io::Result<()> {
        let segment = self
            .segment
            .as_mut()
            .expect("Segment should exist for non-Cache mode");

        // Total on-disk bytes this append consumes: 8-byte envelope plus
        // the codec-encoded payload. Used for both the rotation check and
        // the accounting below.
        let total_on_disk = encoded.len() + WAL_RECORD_ENVELOPE_OVERHEAD;

        // Do not rotate away from a segment while a background sync is fsyncing
        // a cloned descriptor for that same segment. The size limit is a soft
        // threshold; we can rotate on the next append after the sync commits.
        if !self.sync_in_flight && segment.size() + total_on_disk as u64 > self.config.segment_size
        {
            self.rotate_segment()?;
        }

        // Construct the outer envelope: [u32 outer_len][u32 outer_len_crc].
        // `outer_len_crc` mirrors the inner `LenCRC` pattern — it protects
        // the outer length field from torn writes so a reader can detect
        // a mid-envelope crash (see `read_segment_from` parse loop).
        let outer_len: u32 = encoded.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "record too large for u32 outer_len",
            )
        })?;
        let outer_len_bytes = outer_len.to_le_bytes();
        let outer_len_crc = {
            let mut h = Hasher::new();
            h.update(&outer_len_bytes);
            h.finalize()
        };
        let outer_len_crc_bytes = outer_len_crc.to_le_bytes();

        // Write to segment: envelope header first, then encoded payload.
        // `WalSegment::write` uses a BufWriter internally so the three
        // writes coalesce into a single syscall.
        let segment = self.segment.as_mut().unwrap();
        let tw = if self.profile_wal {
            Some(Instant::now())
        } else {
            None
        };
        segment.write(&outer_len_bytes)?;
        segment.write(&outer_len_crc_bytes)?;
        segment.write(encoded)?;
        if let Some(tw) = tw {
            self.profile_write_ns += tw.elapsed().as_nanos() as u64;
        }

        // Track metadata for the current segment
        if let Some(ref mut meta) = self.current_segment_meta {
            meta.track_record(txn_id, timestamp);
        }

        self.total_wal_appends += 1;
        self.total_bytes_written += total_on_disk as u64;

        debug!(
            target: "strata::wal",
            txn_id = txn_id.as_u64(),
            record_bytes = encoded.len(),
            envelope_bytes = WAL_RECORD_ENVELOPE_OVERHEAD,
            segment = self.current_segment_number,
            "WAL record appended",
        );

        self.bytes_since_sync += total_on_disk as u64;
        self.writes_since_sync += 1;
        self.has_unsynced_data = true;

        Ok(())
    }

    /// Handle fsync based on durability mode.
    fn maybe_sync(&mut self) -> std::io::Result<()> {
        match self.durability {
            DurabilityMode::Always => {
                // Always sync immediately
                self.perform_sync()?;
                self.reset_sync_counters();
            }
            DurabilityMode::Standard { interval_ms, .. } => {
                // Standard mode: fsync is deferred to the background flush thread.
                // The only inline trigger is the interval_ms safety net — if the
                // background thread has stalled beyond the interval, we sync inline
                // to bound the durability window. No batch_size trigger: at high
                // write rates, batch_size fsyncs dominated throughput (6.5ms fsync
                // every 1000 writes = 40% overhead). The interval_ms bound is
                // sufficient — it caps the wall-clock durability window regardless
                // of write rate.
                if self.sync_in_flight {
                    return Ok(());
                }
                let elapsed_ms = self.last_sync_time.elapsed().as_millis() as u64;
                if self.has_unsynced_data && elapsed_ms >= interval_ms {
                    debug!(target: "strata::wal",
                        elapsed_ms,
                        interval_ms,
                        "Inline sync fallback — background thread did not sync within interval");
                    self.perform_sync()?;
                    self.reset_sync_counters();
                }
            }
            DurabilityMode::Cache => {
                // No sync needed
            }
        }

        Ok(())
    }

    /// Sync the active segment to disk, tracking timing metrics.
    fn perform_sync(&mut self) -> std::io::Result<()> {
        if let Some(ref mut segment) = self.segment {
            let start = Instant::now();
            segment.sync()?;
            let elapsed = start.elapsed();
            self.total_sync_calls += 1;
            self.total_sync_nanos += elapsed.as_nanos() as u64;
            if self.profile_wal {
                self.profile_sync_ns += elapsed.as_nanos() as u64;
                self.profile_sync_count += 1;
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
        self.bg_error = None;
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

        if self.current_segment_number > 1000 {
            warn!(target: "strata::wal", segments = self.current_segment_number,
                "WAL has over 1000 segments — consider running checkpoint() + compact()");
        }

        Ok(())
    }

    /// Force flush any buffered data to disk.
    ///
    /// This ensures all written records are persisted, regardless of
    /// durability mode settings.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.perform_sync()?;
        self.reset_sync_counters();
        self.flush_active_meta();
        debug!(target: "strata::wal", segment = self.current_segment_number, "WAL flushed");
        Ok(())
    }

    /// Sync if the batched interval has elapsed and there is unsynced data.
    ///
    /// Call this periodically (e.g., from a maintenance timer) to ensure
    /// Standard mode honors its `interval_ms` even when no new writes arrive.
    /// Returns `true` if a sync was performed.
    pub fn sync_if_overdue(&mut self) -> std::io::Result<bool> {
        if self.sync_in_flight || !self.has_unsynced_data {
            return Ok(false);
        }

        if let DurabilityMode::Standard { interval_ms, .. } = self.durability {
            if self.last_sync_time.elapsed().as_millis() as u64 >= interval_ms {
                self.perform_sync()?;
                self.reset_sync_counters();
                debug!(target: "strata::wal", segment = self.current_segment_number, "WAL periodic sync");
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Begin a background sync without clearing the dirty counters.
    pub(crate) fn begin_background_sync(&mut self) -> io::Result<Option<SyncHandle>> {
        if self.sync_in_flight || !self.has_unsynced_data {
            return Ok(None);
        }

        if let DurabilityMode::Standard { interval_ms, .. } = self.durability {
            if self.last_sync_time.elapsed().as_millis() as u64 >= interval_ms {
                if let Some(ref mut segment) = self.segment {
                    segment.flush_to_os()?;
                    let fd = segment.try_clone_fd()?;
                    let pending_snapshot = SyncSnapshot {
                        bytes_since_sync: self.bytes_since_sync,
                        writes_since_sync: self.writes_since_sync,
                        last_sync_time: self.last_sync_time,
                        has_unsynced_data: self.has_unsynced_data,
                    };
                    self.sync_in_flight = true;
                    return Ok(Some(SyncHandle {
                        fd,
                        pending_snapshot,
                        consumed: false,
                    }));
                }
            }
        }

        Ok(None)
    }

    /// Commit a successful background sync.
    ///
    /// The snapshot captured at begin-time is the synced prefix. Any writes
    /// appended while `sync_in_flight` was true remain dirty after commit.
    pub(crate) fn commit_background_sync(&mut self, handle: SyncHandle) -> io::Result<()> {
        let snapshot = handle.take_snapshot();
        self.sync_in_flight = false;

        if !snapshot.has_unsynced_data {
            return Err(io::Error::other(
                "background sync committed without a dirty snapshot",
            ));
        }

        if self.bytes_since_sync < snapshot.bytes_since_sync
            || self.writes_since_sync < snapshot.writes_since_sync
        {
            return Err(io::Error::other(
                "background sync snapshot exceeds current dirty counters",
            ));
        }

        self.bytes_since_sync -= snapshot.bytes_since_sync;
        self.writes_since_sync -= snapshot.writes_since_sync;
        self.last_sync_time = Instant::now().max(snapshot.last_sync_time);
        self.has_unsynced_data = self.bytes_since_sync > 0 || self.writes_since_sync > 0;
        self.bg_error = None;

        debug!(
            target: "strata::wal",
            remaining_bytes = self.bytes_since_sync,
            remaining_writes = self.writes_since_sync,
            "Background sync committed"
        );

        Ok(())
    }

    /// Abort a failed background sync while preserving the dirty counters.
    pub(crate) fn abort_background_sync(&mut self, handle: SyncHandle, error: io::Error) {
        let _snapshot = handle.take_snapshot();
        self.sync_in_flight = false;

        self.record_sync_failure(error);
    }

    /// Records a sync failure while preserving dirty counters.
    pub(crate) fn record_sync_failure(&mut self, error: io::Error) {
        let failed_sync_count = self
            .bg_error
            .as_ref()
            .map(|existing| existing.failed_sync_count.saturating_add(1))
            .unwrap_or(1);
        let first_observed_at = self
            .bg_error
            .as_ref()
            .map(|existing| existing.first_observed_at)
            .unwrap_or_else(SystemTime::now);

        self.bg_error = Some(BgError {
            kind: error.kind(),
            message: error.to_string(),
            first_observed_at,
            failed_sync_count,
        });

        error!(
            target: "strata::wal",
            error = %error,
            failed_sync_count,
            "Background sync failed; dirty counters preserved for retry"
        );
    }

    /// Returns the last background sync error, if any.
    pub(crate) fn bg_error(&self) -> Option<&BgError> {
        self.bg_error.as_ref()
    }

    /// Clears the last background sync error.
    pub(crate) fn clear_bg_error(&mut self) {
        self.bg_error = None;
    }

    /// Returns whether a background sync is currently in flight.
    pub(crate) fn sync_in_flight(&self) -> bool {
        self.sync_in_flight
    }

    /// Refresh the Standard-mode inline-sync deadline without clearing dirty state.
    ///
    /// The engine uses this after unrelated durable control-artifact writes
    /// (`strata.toml`, MANIFEST-like metadata) so their fsync cost does not
    /// make `maybe_sync` treat the background WAL thread as overdue.
    pub(crate) fn refresh_sync_deadline(&mut self) {
        self.last_sync_time = Instant::now();
    }

    /// Get the current durability mode.
    pub fn durability_mode(&self) -> DurabilityMode {
        self.durability
    }

    /// Switch the durability mode at runtime.
    ///
    /// Only Standard ↔ Always switching is supported. Switching to or from
    /// Cache is rejected because Cache mode has no WAL infrastructure.
    ///
    /// When switching to Always, any buffered unsynced data is flushed
    /// immediately to satisfy the stronger durability guarantee.
    pub fn set_durability_mode(&mut self, mode: DurabilityMode) -> std::io::Result<()> {
        if matches!(mode, DurabilityMode::Cache) || matches!(self.durability, DurabilityMode::Cache)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot switch to or from Cache mode at runtime",
            ));
        }
        let old = self.durability;
        self.durability = mode;
        // When switching to Always, flush any unsynced data immediately
        if matches!(mode, DurabilityMode::Always)
            && !matches!(old, DurabilityMode::Always)
            && self.has_unsynced_data
        {
            self.flush()?;
        }
        Ok(())
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

    /// Compute WAL disk usage by scanning the WAL directory.
    ///
    /// Returns zeros in Cache mode (no WAL files).
    pub fn wal_disk_usage(&self) -> WalDiskUsage {
        if !self.durability.requires_wal() {
            return WalDiskUsage::default();
        }

        let mut total_bytes = 0u64;
        let mut segment_count = 0usize;

        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if super::parse_segment_number(&name).is_some() {
                    if let Ok(metadata) = entry.metadata() {
                        total_bytes += metadata.len();
                        segment_count += 1;
                    }
                }
            }
        }

        WalDiskUsage {
            total_bytes,
            segment_count,
        }
    }

    /// Persist the active segment's .meta sidecar to disk.
    ///
    /// Best-effort optimization — if the write fails, recovery falls back
    /// to scanning the segment. Called periodically by the background flush
    /// thread and on explicit flush.
    pub fn flush_active_meta(&self) {
        if let Some(ref meta) = self.current_segment_meta {
            if !meta.is_empty() {
                if let Err(e) = meta.write_to_file(&self.wal_dir) {
                    debug!(target: "strata::wal",
                        segment = self.current_segment_number,
                        error = %e,
                        "Failed to flush active segment .meta (non-fatal)");
                }
            }
        }
    }

    /// Snapshot the active segment's metadata for out-of-lock writing.
    ///
    /// Returns `Some((meta_clone, wal_dir))` if there is metadata to flush,
    /// `None` otherwise. The caller can then write the meta file without
    /// holding the WAL lock.
    pub fn snapshot_active_meta(&self) -> Option<(crate::format::SegmentMeta, std::path::PathBuf)> {
        if let Some(ref meta) = self.current_segment_meta {
            if !meta.is_empty() {
                return Some((meta.clone(), self.wal_dir.clone()));
            }
        }
        None
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
    ///
    /// Takes `codec` as a parameter (rather than reading `self.codec`)
    /// so it can be called from both `WalWriter::new` — which runs
    /// BEFORE `self` exists — and from in-place segment-rotation paths
    /// that have a live `self`. T3-E12 §D3 Site 4: threading the codec
    /// here is load-bearing because on an encrypted WAL, reopening
    /// without a codec would fail to rebuild `.meta` sidecars, and
    /// follower-segment-skip via meta and compaction watermark-coverage
    /// would then use stale / empty metadata.
    fn rebuild_meta_for_segment(
        codec: &dyn StorageCodec,
        wal_dir: &Path,
        segment_number: u64,
    ) -> Option<SegmentMeta> {
        let reader = WalReader::new().with_codec(crate::codec::clone_codec(codec));
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
                super::parse_segment_number(&name)
            })
            .max()
    }

    /// List all segment numbers in order.
    pub fn list_segments(&self) -> std::io::Result<Vec<u64>> {
        let mut segments = Vec::new();

        for entry in std::fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(num) = super::parse_segment_number(&name) {
                segments.push(num);
            }
        }

        segments.sort();
        Ok(segments)
    }

    /// Re-sync with the WAL directory in case another process has written.
    ///
    /// Call this under the WAL file lock before appending in multi-process mode.
    /// If another process rotated the segment or appended to the current one,
    /// this method updates the local writer state accordingly.
    pub fn reopen_if_needed(&mut self) -> std::io::Result<()> {
        if !self.durability.requires_wal() {
            return Ok(());
        }

        let latest = Self::find_latest_segment(&self.wal_dir);
        match latest {
            Some(num) if num > self.current_segment_number => {
                // Another process rotated — write .meta for our old segment, then
                // open the new one for appending.
                if let Some(ref meta) = self.current_segment_meta {
                    if !meta.is_empty() {
                        let _ = meta.write_to_file(&self.wal_dir);
                    }
                }
                match WalSegment::open_append(&self.wal_dir, num) {
                    Ok(seg) => {
                        self.segment = Some(seg);
                        self.current_segment_number = num;
                        // T3-E12 §D3 Site 4: pass `self.codec` so rebuild
                        // works on encrypted WALs.
                        self.current_segment_meta =
                            Self::rebuild_meta_for_segment(self.codec.as_ref(), &self.wal_dir, num);
                    }
                    Err(e) => {
                        warn!(target: "strata::wal", segment = num, error = %e,
                            "Failed to reopen WAL segment, creating new segment");
                        let new_num = num + 1;
                        let seg = WalSegment::create(&self.wal_dir, new_num, self.database_uuid)?;
                        self.segment = Some(seg);
                        self.current_segment_number = new_num;
                        self.current_segment_meta = Some(SegmentMeta::new_empty(new_num));
                    }
                }
            }
            _ => {
                // Same segment — seek to end to pick up writes from other processes.
                if let Some(ref mut seg) = self.segment {
                    seg.seek_to_end()?;
                }
            }
        }
        Ok(())
    }

    /// Append a record and immediately flush for multi-process safety.
    ///
    /// This method:
    /// 1. Calls `reopen_if_needed()` to pick up other processes' writes
    /// 2. Serializes and writes the record
    /// 3. Forces a flush + fsync so the record is visible to other processes
    ///
    /// Should be called while holding the WAL file lock.
    pub fn append_and_flush(&mut self, record: &WalRecord) -> std::io::Result<()> {
        if !self.durability.requires_wal() {
            return Ok(());
        }

        self.reopen_if_needed()?;

        let record_bytes = record.to_bytes();
        let encoded = self.codec.encode_cow(&record_bytes);
        self.append_inner(&encoded, record.txn_id, record.timestamp)?;

        // Immediately flush for cross-process visibility
        self.flush()
    }

    /// Close the writer, ensuring all data is flushed.
    pub fn close(mut self) -> std::io::Result<()> {
        // flush() syncs segment data AND calls flush_active_meta()
        self.flush()?;

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
                if let Err(e) = segment.sync() {
                    error!(target: "strata::wal", error = %e,
                        "WAL sync on drop failed — data may not be durable");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::IdentityCodec;
    use strata_core::id::TxnId;
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

    fn make_writer_with_config(
        dir: &Path,
        durability: DurabilityMode,
        config: WalConfig,
    ) -> WalWriter {
        WalWriter::new(
            dir.to_path_buf(),
            [1u8; 16],
            durability,
            config,
            Box::new(IdentityCodec),
        )
        .unwrap()
    }

    fn make_record(txn_id: u64) -> WalRecord {
        WalRecord::new(TxnId(txn_id), [1u8; 16], 12345, vec![1, 2, 3])
    }

    fn make_record_with_payload(txn_id: u64, bytes: usize) -> WalRecord {
        WalRecord::new(TxnId(txn_id), [1u8; 16], 12345 + txn_id, vec![7; bytes])
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
                .append(&WalRecord::new(TxnId(i), [1u8; 16], 0, vec![0; 50]))
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

    // ========================================================================
    // Phase 2: Multi-process writer tests
    // ========================================================================

    #[test]
    fn test_two_writers_same_wal_dir_no_corruption() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Writer A writes records 1..5
        {
            let mut writer_a = make_writer(&wal_dir, DurabilityMode::Always);
            for i in 1..=5 {
                writer_a.append_and_flush(&make_record(i)).unwrap();
            }
        }

        // Writer B opens the same dir and writes records 6..10
        {
            let mut writer_b = make_writer(&wal_dir, DurabilityMode::Always);
            for i in 6..=10 {
                writer_b.append_and_flush(&make_record(i)).unwrap();
            }
        }

        // Read all records — should see all 10 in order
        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        let ids: Vec<u64> = result.records.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(ids, (1..=10).collect::<Vec<_>>());
    }

    #[test]
    fn test_writer_sees_other_writers_records_after_reopen() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Writer A writes record 1
        let mut writer_a = make_writer(&wal_dir, DurabilityMode::Always);
        writer_a.append_and_flush(&make_record(1)).unwrap();

        // Writer B writes record 2 (opens same dir)
        {
            let mut writer_b = make_writer(&wal_dir, DurabilityMode::Always);
            writer_b.append_and_flush(&make_record(2)).unwrap();
        }

        // Writer A calls reopen_if_needed then writes record 3
        writer_a.reopen_if_needed().unwrap();
        writer_a.append_and_flush(&make_record(3)).unwrap();

        // All 3 records should be readable
        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        let ids: Vec<u64> = result.records.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_writer_handles_segment_rotation_by_other() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use small segments to force rotation
        let config = WalConfig::new()
            .with_segment_size(100)
            .with_buffered_sync_bytes(50);

        let mut writer_a = WalWriter::new(
            wal_dir.clone(),
            [1u8; 16],
            DurabilityMode::Always,
            config.clone(),
            Box::new(IdentityCodec),
        )
        .unwrap();

        // Writer A writes one record
        writer_a
            .append_and_flush(&WalRecord::new(TxnId(1), [1u8; 16], 0, vec![0; 50]))
            .unwrap();
        let seg_a = writer_a.current_segment();

        // Writer B opens same dir and writes enough to rotate segments
        {
            let mut writer_b = WalWriter::new(
                wal_dir.clone(),
                [1u8; 16],
                DurabilityMode::Always,
                config,
                Box::new(IdentityCodec),
            )
            .unwrap();
            for i in 2..=5 {
                writer_b
                    .append_and_flush(&WalRecord::new(TxnId(i), [1u8; 16], 0, vec![0; 50]))
                    .unwrap();
            }
            assert!(
                writer_b.current_segment() > seg_a,
                "Writer B should have rotated"
            );
        }

        // Writer A should detect the new segment after reopen
        writer_a.reopen_if_needed().unwrap();
        writer_a
            .append_and_flush(&WalRecord::new(TxnId(99), [1u8; 16], 0, vec![0; 10]))
            .unwrap();

        // All records should be readable
        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        assert!(
            result.records.len() >= 6,
            "Should have at least 6 records, got {}",
            result.records.len()
        );
        // Record 99 should be last
        assert_eq!(result.records.last().unwrap().txn_id, TxnId(99));
    }

    #[test]
    fn test_reader_reads_interleaved_records_in_order() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Create alternating writes from two writers
        let mut writer_a = make_writer(&wal_dir, DurabilityMode::Always);
        writer_a.append_and_flush(&make_record(1)).unwrap();

        // Writer B opens, writes, closes
        {
            let mut writer_b = make_writer(&wal_dir, DurabilityMode::Always);
            writer_b.append_and_flush(&make_record(2)).unwrap();
        }

        // Writer A reopens and writes again
        writer_a.reopen_if_needed().unwrap();
        writer_a.append_and_flush(&make_record(3)).unwrap();

        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        let ids: Vec<u64> = result.records.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(ids, vec![1, 2, 3]);
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

    // ========================================================================
    // append_pre_serialized tests
    // ========================================================================

    #[test]
    fn test_pre_serialized_roundtrip_readable() {
        // Records written via append_pre_serialized must be readable by WalReader
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);

        let record = make_record(42);
        let record_bytes = record.to_bytes();
        writer
            .append_pre_serialized(&record_bytes, TxnId(42), 12345)
            .unwrap();
        writer.flush().unwrap();

        // Read back via WalReader
        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].txn_id, TxnId(42));
        assert_eq!(result.records[0].timestamp, 12345);
        assert_eq!(result.records[0].writeset, vec![1, 2, 3]);
    }

    #[test]
    fn test_pre_serialized_equivalent_to_append() {
        // append_pre_serialized must produce byte-identical output to append
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let wal_dir1 = dir1.path().join("wal");
        let wal_dir2 = dir2.path().join("wal");

        // Write via append()
        let mut writer1 = make_writer(&wal_dir1, DurabilityMode::Always);
        let record = make_record(7);
        writer1.append(&record).unwrap();
        writer1.flush().unwrap();

        // Write via append_pre_serialized()
        let mut writer2 = make_writer(&wal_dir2, DurabilityMode::Always);
        let record_bytes = record.to_bytes();
        writer2
            .append_pre_serialized(&record_bytes, record.txn_id, record.timestamp)
            .unwrap();
        writer2.flush().unwrap();

        // Read back both and compare
        let reader = crate::wal::WalReader::new();
        let result1 = reader.read_all(&wal_dir1).unwrap();
        let result2 = reader.read_all(&wal_dir2).unwrap();

        assert_eq!(result1.records.len(), 1);
        assert_eq!(result2.records.len(), 1);
        assert_eq!(result1.records[0], result2.records[0]);
    }

    #[test]
    fn test_pre_serialized_segment_rotation() {
        // append_pre_serialized must handle segment rotation correctly
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalConfig::new()
            .with_segment_size(100) // Very small to force rotation
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.clone(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            Box::new(IdentityCodec),
        )
        .unwrap();

        // Write enough records via pre_serialized to trigger rotation
        for i in 0..10u64 {
            let record = WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![0; 50]);
            let record_bytes = record.to_bytes();
            writer
                .append_pre_serialized(&record_bytes, TxnId(i), i * 1000)
                .unwrap();
        }

        // Should have rotated to multiple segments
        let segments = writer.list_segments().unwrap();
        assert!(
            segments.len() > 1,
            "Should have rotated, got {} segments",
            segments.len()
        );

        // All records should be readable
        writer.flush().unwrap();
        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        assert_eq!(result.records.len(), 10);
        for (i, rec) in result.records.iter().enumerate() {
            assert_eq!(rec.txn_id, TxnId(i as u64));
        }
    }

    #[test]
    fn test_pre_serialized_cache_mode_noop() {
        let dir = tempdir().unwrap();

        let mut writer = make_writer(dir.path(), DurabilityMode::Cache);
        let record = make_record(1);
        let record_bytes = record.to_bytes();
        writer
            .append_pre_serialized(&record_bytes, TxnId(1), 12345)
            .unwrap();

        // No files should be created
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn test_pre_serialized_counters_updated() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);

        let counters_before = writer.counters();
        assert_eq!(counters_before.wal_appends, 0);

        let record = make_record(1);
        let record_bytes = record.to_bytes();
        writer
            .append_pre_serialized(&record_bytes, TxnId(1), 12345)
            .unwrap();

        let counters_after = writer.counters();
        assert_eq!(counters_after.wal_appends, 1);
        assert!(counters_after.bytes_written > 0);
        // Always mode fsyncs on every append
        assert!(counters_after.sync_calls >= 1);
    }

    #[test]
    fn test_pre_serialized_interleaved_with_append() {
        // Mixing append() and append_pre_serialized() should produce
        // all records in order
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);

        // append()
        writer.append(&make_record(1)).unwrap();

        // append_pre_serialized()
        let r2 = make_record(2);
        writer
            .append_pre_serialized(&r2.to_bytes(), TxnId(2), 12345)
            .unwrap();

        // append()
        writer.append(&make_record(3)).unwrap();

        writer.flush().unwrap();

        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        let ids: Vec<u64> = result.records.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    // ========================================================================
    // D-1: Inline sync fallback tests
    // ========================================================================

    #[test]
    fn test_maybe_sync_inline_fallback_triggers() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use a very short interval so the deadline is easily exceeded
        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 1,
                batch_size: 10000,
            },
        );

        // Write a record to get data on disk
        writer.append(&make_record(1)).unwrap();

        // Explicitly set the state we need — the append above may or may not
        // have triggered the fallback (depending on machine speed), so force
        // the preconditions rather than relying on timing.
        writer.has_unsynced_data = true;
        writer.last_sync_time = Instant::now() - std::time::Duration::from_millis(100);

        let sync_calls_before = writer.total_sync_calls;
        writer.maybe_sync().unwrap();

        // Inline fallback should have performed a sync
        assert!(
            writer.total_sync_calls > sync_calls_before,
            "Expected inline sync fallback to trigger"
        );
        // has_unsynced_data should be reset by reset_sync_counters()
        assert!(!writer.has_unsynced_data);
        // bytes_since_sync and writes_since_sync should also be reset
        assert_eq!(writer.bytes_since_sync, 0);
        assert_eq!(writer.writes_since_sync, 0);
    }

    #[test]
    fn test_maybe_sync_noop_within_deadline() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 60_000, // 60s deadline
                batch_size: 10000,
            },
        );

        writer.append(&make_record(1)).unwrap();
        // Confirm precondition: data is unsynced
        assert!(
            writer.has_unsynced_data,
            "append should mark data as unsynced"
        );

        let sync_calls_before = writer.total_sync_calls;
        writer.maybe_sync().unwrap();

        // No sync should have occurred — we're well within the 60s deadline
        assert_eq!(writer.total_sync_calls, sync_calls_before);
        // Data should still be marked unsynced
        assert!(writer.has_unsynced_data);
    }

    #[test]
    fn test_maybe_sync_noop_no_unsynced_data() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 1,
                batch_size: 10000,
            },
        );

        // Don't write any records — has_unsynced_data must be false
        assert!(
            !writer.has_unsynced_data,
            "fresh writer should have no unsynced data"
        );

        // Force last_sync_time into the past so deadline is exceeded
        writer.last_sync_time = Instant::now() - std::time::Duration::from_millis(100);

        let sync_calls_before = writer.total_sync_calls;
        writer.maybe_sync().unwrap();

        // No sync even though deadline exceeded — no unsynced data
        assert_eq!(writer.total_sync_calls, sync_calls_before);
        assert!(!writer.has_unsynced_data);
    }

    #[test]
    fn test_issue_1715_inline_sync_at_configured_interval() {
        // Issue #1715: The inline sync safety net should trigger at interval_ms,
        // not 3×interval_ms. The data loss window must match the configured interval.
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let interval_ms = 50;
        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms,
                batch_size: 10000,
            },
        );

        // Write a record so there's data on disk
        writer.append(&make_record(1)).unwrap();

        // Set last_sync_time to just past interval_ms ago (but well under 3×interval_ms)
        writer.has_unsynced_data = true;
        writer.last_sync_time = Instant::now() - std::time::Duration::from_millis(interval_ms + 10);

        let sync_calls_before = writer.total_sync_calls;
        writer.maybe_sync().unwrap();

        // The inline safety net should trigger at interval_ms, not 3×interval_ms.
        // With the bug, this would NOT trigger because 60ms < 150ms (3×50ms).
        assert!(
            writer.total_sync_calls > sync_calls_before,
            "Inline sync should trigger at interval_ms ({}ms), not 3×interval_ms ({}ms)",
            interval_ms,
            interval_ms * 3,
        );
        assert!(!writer.has_unsynced_data, "Sync should reset unsynced flag");
    }

    // ========================================================================
    // D-7: Active segment .meta sidecar tests
    // ========================================================================

    #[test]
    fn test_flush_active_meta_writes_file() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);

        // Write some records
        for i in 1..=5 {
            writer
                .append(&WalRecord::new(
                    TxnId(i),
                    [1u8; 16],
                    i * 1000,
                    vec![i as u8],
                ))
                .unwrap();
        }

        // Explicitly flush active meta
        writer.flush_active_meta();

        // Verify .meta file exists
        let seg_num = writer.current_segment();
        let meta = SegmentMeta::read_from_file(&wal_dir, seg_num)
            .unwrap()
            .expect(".meta should exist after flush_active_meta");

        assert_eq!(meta.segment_number, seg_num);
        assert_eq!(meta.record_count, 5);
        assert_eq!(meta.min_txn_id, TxnId(1));
        assert_eq!(meta.max_txn_id, TxnId(5));
        // Verify timestamps are also tracked correctly
        assert_eq!(meta.min_timestamp, 1000);
        assert_eq!(meta.max_timestamp, 5000);
    }

    #[test]
    fn test_flush_active_meta_updates_on_new_records() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);

        // Write initial records and flush meta
        for i in 1..=3 {
            writer
                .append(&WalRecord::new(
                    TxnId(i),
                    [1u8; 16],
                    i * 1000,
                    vec![i as u8],
                ))
                .unwrap();
        }
        writer.flush_active_meta();

        let seg_num = writer.current_segment();
        let meta = SegmentMeta::read_from_file(&wal_dir, seg_num)
            .unwrap()
            .expect(".meta should exist");
        assert_eq!(meta.record_count, 3);
        assert_eq!(meta.max_txn_id, TxnId(3));
        assert_eq!(meta.min_timestamp, 1000);
        assert_eq!(meta.max_timestamp, 3000);

        // Write more records and flush again
        for i in 4..=7 {
            writer
                .append(&WalRecord::new(
                    TxnId(i),
                    [1u8; 16],
                    i * 1000,
                    vec![i as u8],
                ))
                .unwrap();
        }
        writer.flush_active_meta();

        let meta = SegmentMeta::read_from_file(&wal_dir, seg_num)
            .unwrap()
            .expect(".meta should exist after update");
        assert_eq!(meta.record_count, 7);
        assert_eq!(meta.min_txn_id, TxnId(1));
        assert_eq!(meta.max_txn_id, TxnId(7));
        assert_eq!(meta.min_timestamp, 1000);
        assert_eq!(meta.max_timestamp, 7000);
    }

    #[test]
    fn test_flush_active_meta_noop_cache_mode() {
        let dir = tempdir().unwrap();

        let mut writer = make_writer(dir.path(), DurabilityMode::Cache);
        writer.append(&make_record(1)).unwrap();
        writer.flush_active_meta();

        // No files should be created in Cache mode
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "Cache mode should not create any files"
        );
    }

    #[test]
    fn test_flush_writes_active_meta() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);

        // Write records
        for i in 1..=3 {
            writer
                .append(&WalRecord::new(
                    TxnId(i),
                    [1u8; 16],
                    i * 1000,
                    vec![i as u8],
                ))
                .unwrap();
        }

        // Call flush() — should also persist .meta
        writer.flush().unwrap();

        let seg_num = writer.current_segment();
        let meta = SegmentMeta::read_from_file(&wal_dir, seg_num)
            .unwrap()
            .expect(".meta should exist after flush()");

        assert_eq!(meta.segment_number, seg_num);
        assert_eq!(meta.record_count, 3);
        assert_eq!(meta.min_txn_id, TxnId(1));
        assert_eq!(meta.max_txn_id, TxnId(3));
    }

    // ========================================================================
    // D-9: WAL disk usage tests
    // ========================================================================

    #[test]
    fn test_wal_disk_usage_empty() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let writer = make_writer(&wal_dir, DurabilityMode::Always);
        let usage = writer.wal_disk_usage();

        // Fresh writer has 1 segment (the initial one)
        assert_eq!(usage.segment_count, 1);
        assert!(usage.total_bytes > 0, "Should include header bytes");
    }

    #[test]
    fn test_wal_disk_usage_after_writes() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(&wal_dir, DurabilityMode::Always);
        let usage_before = writer.wal_disk_usage();

        // Still 1 segment, just the header
        assert_eq!(usage_before.segment_count, 1);

        for i in 1..=10 {
            writer
                .append(&WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![0; 50]))
                .unwrap();
        }
        writer.flush().unwrap();

        let usage_after = writer.wal_disk_usage();
        assert!(
            usage_after.total_bytes > usage_before.total_bytes,
            "Bytes should increase after writes: before={}, after={}",
            usage_before.total_bytes,
            usage_after.total_bytes
        );
        // Still 1 segment (no rotation with default large segment size)
        assert_eq!(usage_after.segment_count, 1);
    }

    #[test]
    fn test_wal_disk_usage_after_rotation() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalConfig::new()
            .with_segment_size(100)
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.clone(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            Box::new(IdentityCodec),
        )
        .unwrap();

        let usage_before = writer.wal_disk_usage();
        assert_eq!(usage_before.segment_count, 1);

        // Write enough to force rotation
        for i in 0..10 {
            writer
                .append(&WalRecord::new(TxnId(i), [1u8; 16], 0, vec![0; 50]))
                .unwrap();
        }

        let usage = writer.wal_disk_usage();
        assert!(
            usage.segment_count >= 2,
            "Should have multiple segments after rotation, got {}",
            usage.segment_count
        );
        // Total bytes should include ALL segments (rotated + current)
        assert!(
            usage.total_bytes > usage_before.total_bytes,
            "Total bytes should include all segment files"
        );
    }

    #[test]
    fn test_wal_disk_usage_cache_mode() {
        let dir = tempdir().unwrap();

        let mut writer = make_writer(dir.path(), DurabilityMode::Cache);
        writer.append(&make_record(1)).unwrap();

        let usage = writer.wal_disk_usage();
        assert_eq!(usage.total_bytes, 0);
        assert_eq!(usage.segment_count, 0);
    }

    /// Issue #1711: Segment rotation must fsync the parent directory so the
    /// new segment's directory entry survives power loss. Without the directory
    /// fsync, the first record in a newly-rotated segment can be lost.
    ///
    /// This test exercises the full rotation path and verifies all segments
    /// (including rotated ones) are discoverable and readable afterward.
    #[test]
    fn test_issue_1711_rotation_fsyncs_parent_directory() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use very small segment size to force many rotations
        let config = WalConfig::new()
            .with_segment_size(100)
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.clone(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            Box::new(IdentityCodec),
        )
        .unwrap();

        // Write enough records to force several rotations
        for i in 1..=20 {
            writer
                .append(&WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![0; 50]))
                .unwrap();
        }

        let segments = writer.list_segments().unwrap();
        assert!(
            segments.len() >= 3,
            "Expected at least 3 segments from rotation, got {}",
            segments.len()
        );

        // Every segment file must be visible in the directory
        for seg_num in &segments {
            let path = WalSegment::segment_path(&wal_dir, *seg_num);
            assert!(
                path.exists(),
                "Segment {} directory entry must be durable",
                seg_num
            );
        }

        // All records must be readable through the reader
        let reader = crate::wal::WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();
        assert_eq!(
            result.records.len(),
            20,
            "All 20 records must survive rotation"
        );
    }

    #[test]
    fn test_standard_mode_no_inline_batch_sync() {
        // Standard mode no longer triggers inline fsync at batch_size.
        // Fsync is deferred to the background thread (interval_ms only).
        // This test verifies that writing batch_size records does NOT
        // trigger an inline sync when interval_ms hasn't elapsed.
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 600_000, // 10 minutes — will never elapse
                batch_size: 5,
            },
        );

        let sync_calls_before = writer.counters().sync_calls;

        // Write more than batch_size records
        for i in 0..20 {
            writer.append(&make_record(i)).unwrap();
        }

        let sync_calls_after = writer.counters().sync_calls;
        assert_eq!(
            sync_calls_after, sync_calls_before,
            "Standard mode should NOT inline-sync at batch_size when interval_ms hasn't elapsed. \
             sync_calls went from {} to {}",
            sync_calls_before, sync_calls_after,
        );
    }

    #[test]
    fn test_standard_mode_interval_triggers_sync() {
        // Standard mode fsyncs when interval_ms elapses (safety net).
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 1, // 1ms — will elapse almost immediately
                batch_size: 1_000_000,
            },
        );

        // Write a record, then sleep to let interval_ms elapse
        writer.append(&make_record(1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));

        let sync_calls_before = writer.total_sync_calls;
        // This write should trigger the interval safety net
        writer.append(&make_record(2)).unwrap();

        assert!(
            writer.total_sync_calls > sync_calls_before,
            "Standard mode should sync when interval_ms has elapsed"
        );
    }

    #[test]
    fn test_wal_disk_usage_ignores_non_segment_files() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let writer = make_writer(&wal_dir, DurabilityMode::Always);

        // Write some .meta and other files that should NOT be counted
        std::fs::write(wal_dir.join("wal-000001.meta"), b"metadata").unwrap();
        std::fs::write(wal_dir.join("MANIFEST"), b"manifest data").unwrap();
        std::fs::write(wal_dir.join("some_other_file.txt"), b"junk").unwrap();

        let usage = writer.wal_disk_usage();
        // Only the .seg file should be counted
        assert_eq!(usage.segment_count, 1);
    }

    #[test]
    fn test_begin_background_sync_snapshots_without_clearing_dirty_state() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 60_000,
                batch_size: 1_000,
            },
        );

        writer.append(&make_record(1)).unwrap();
        let bytes_before = writer.bytes_since_sync;
        let writes_before = writer.writes_since_sync;
        writer.last_sync_time = Instant::now() - std::time::Duration::from_secs(3600);

        let handle = writer
            .begin_background_sync()
            .unwrap()
            .expect("expected a sync handle");

        assert!(writer.sync_in_flight());
        assert!(writer.has_unsynced_data);
        assert_eq!(writer.bytes_since_sync, bytes_before);
        assert_eq!(writer.writes_since_sync, writes_before);

        writer.abort_background_sync(handle, io::Error::other("test cleanup"));
    }

    #[test]
    fn test_commit_background_sync_clears_synced_prefix_only() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 60_000,
                batch_size: 1_000,
            },
        );

        writer.append(&make_record(1)).unwrap();
        let synced_bytes = writer.bytes_since_sync;
        writer.last_sync_time = Instant::now() - std::time::Duration::from_secs(3600);

        let handle = writer
            .begin_background_sync()
            .unwrap()
            .expect("expected a sync handle");
        writer.append(&make_record(2)).unwrap();
        let dirty_after_late_write = writer.bytes_since_sync;

        handle.fd().sync_all().unwrap();
        writer.commit_background_sync(handle).unwrap();

        assert!(!writer.sync_in_flight());
        assert!(writer.has_unsynced_data);
        assert_eq!(writer.writes_since_sync, 1);
        assert_eq!(
            writer.bytes_since_sync,
            dirty_after_late_write - synced_bytes
        );
        assert!(writer.bg_error().is_none());
    }

    #[test]
    fn test_abort_background_sync_preserves_dirty_counters_and_records_error() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut writer = make_writer(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 60_000,
                batch_size: 1_000,
            },
        );

        writer.append(&make_record(1)).unwrap();
        let bytes_before = writer.bytes_since_sync;
        let writes_before = writer.writes_since_sync;
        writer.last_sync_time = Instant::now() - std::time::Duration::from_secs(3600);

        let handle = writer
            .begin_background_sync()
            .unwrap()
            .expect("expected a sync handle");
        writer.abort_background_sync(handle, io::Error::other("disk full"));

        assert!(!writer.sync_in_flight());
        assert!(writer.has_unsynced_data);
        assert_eq!(writer.bytes_since_sync, bytes_before);
        assert_eq!(writer.writes_since_sync, writes_before);

        let bg_error = writer.bg_error().expect("expected bg_error");
        assert_eq!(bg_error.kind, io::ErrorKind::Other);
        assert_eq!(bg_error.failed_sync_count, 1);
    }

    #[test]
    fn test_append_during_inflight_sync_does_not_rotate_away_from_synced_segment() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let record = make_record_with_payload(1, 32);
        let record_len = record.to_bytes().len() as u64;
        let config = WalConfig::for_testing()
            .with_segment_size(SEGMENT_HEADER_SIZE_V2 as u64 + record_len + 1);
        let mut writer = make_writer_with_config(
            &wal_dir,
            DurabilityMode::Standard {
                interval_ms: 60_000,
                batch_size: 1_000,
            },
            config,
        );

        writer.append(&record).unwrap();
        writer.last_sync_time = Instant::now() - std::time::Duration::from_secs(3600);
        let active_segment = writer.current_segment();
        let handle = writer
            .begin_background_sync()
            .unwrap()
            .expect("expected a sync handle");

        writer.append(&make_record_with_payload(2, 32)).unwrap();

        assert_eq!(
            writer.current_segment(),
            active_segment,
            "background sync in flight must pin the active segment"
        );

        handle.fd().sync_all().unwrap();
        writer.commit_background_sync(handle).unwrap();
        assert_eq!(writer.writes_since_sync, 1);
    }

    #[test]
    fn test_source_no_longer_contains_old_background_sync_api() {
        let source = include_str!("writer.rs");
        let old_api = ["pub fn ", "prepare_background_sync("].concat();
        assert!(
            !source.contains(&old_api),
            "writer.rs must not expose the old counter-reset sync API"
        );
    }

    #[test]
    fn test_three_phase_background_sync_api_is_not_public() {
        let source = include_str!("writer.rs");
        for public_signature in [
            ["pub", " struct SyncHandle"].concat(),
            ["pub", " struct BgError"].concat(),
            ["pub", " fn begin_background_sync("].concat(),
            ["pub", " fn commit_background_sync("].concat(),
            ["pub", " fn abort_background_sync("].concat(),
            ["pub", " fn bg_error("].concat(),
            ["pub", " fn record_sync_failure("].concat(),
            ["pub", " fn clear_bg_error("].concat(),
            ["pub", " fn sync_in_flight("].concat(),
        ] {
            assert!(
                !source.contains(&public_signature),
                "writer.rs should keep the three-phase sync API internal: {public_signature}"
            );
        }
    }
}
