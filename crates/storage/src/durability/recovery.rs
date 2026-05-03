//! Recovery infrastructure for transaction-aware database recovery
//!
//! Per spec Section 5 (Replay Semantics):
//! - Replays do NOT re-run conflict detection
//! - Replays apply commit decisions, not re-execute logic
//! - Replays are single-threaded
//! - Versions are preserved exactly
//!
//! ## Recovery Procedure
//!
//! 1. Plan recovery: read MANIFEST and validate codec identity (`plan_recovery`).
//! 2. Load snapshot (if present) and install decoded entries via `on_snapshot`.
//! 3. Scan segmented WAL directory for records.
//! 4. Each WalRecord = one committed transaction (TransactionPayload).
//! 5. Apply records via `on_record`; truncate partial WAL tail on the active segment.
//! 6. Return `RecoveryStats`; caller owns storage and txn-manager construction.

use std::path::PathBuf;

use crate::durability::codec::{clone_codec, StorageCodec};
use crate::durability::format::{snapshot_path, SegmentMeta, WalRecord, WalSegment};
use crate::durability::layout::DatabaseLayout;
use crate::durability::wal::{WalReader, WalReaderError};
use crate::durability::TransactionPayload;
use crate::durability::{
    LoadedSnapshot, ManifestError, ManifestManager, SnapshotReadError, SnapshotReader,
};
use crate::{SegmentedStore, StorageError, StorageResult};
use strata_core::id::{CommitVersion, TxnId};
use tracing::{info, warn};

/// Recovery plan summarizing MANIFEST state before WAL bytes are read.
///
/// Produced by [`RecoveryCoordinator::plan_recovery`]. Callers use this to:
///
/// - Detect fresh databases (no MANIFEST).
/// - Decide whether a snapshot load is needed (`snapshot_id.is_some()`).
/// - Compute the delta-WAL replay watermark (`snapshot_watermark`).
///
/// Codec validation happens inside `plan_recovery` and short-circuits with a
/// clear error before any WAL bytes are touched, so callers never observe a
/// plan with a mismatched codec.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoveryPlan {
    /// Whether the MANIFEST file existed when the plan was built.
    pub manifest_present: bool,
    /// Codec identifier recorded in the MANIFEST, if present.
    pub codec_id: Option<String>,
    /// Snapshot identifier recorded in the MANIFEST, if any.
    pub snapshot_id: Option<u64>,
    /// Snapshot watermark (highest txn id covered by snapshot), if any.
    pub snapshot_watermark: Option<u64>,
    /// Highest commit id flushed to on-disk segments, if recorded.
    pub flushed_through_commit_id: Option<u64>,
    /// Database UUID recorded in the MANIFEST, if present.
    pub database_uuid: Option<[u8; 16]>,
}

impl RecoveryPlan {
    /// Returns a fresh-database plan (no MANIFEST, no snapshot).
    fn fresh() -> Self {
        RecoveryPlan {
            manifest_present: false,
            codec_id: None,
            snapshot_id: None,
            snapshot_watermark: None,
            flushed_through_commit_id: None,
            database_uuid: None,
        }
    }

    /// Returns the snapshot watermark as a typed `TxnId`, if a snapshot is
    /// recorded in the MANIFEST.
    ///
    /// Delta-WAL replay semantics: after the snapshot is installed, records
    /// with `txn_id > snapshot_watermark` must be replayed. Records with
    /// `txn_id <= snapshot_watermark` are already reflected in the snapshot
    /// and can be skipped.
    ///
    /// Returns `None` when no snapshot is recorded (fresh database or
    /// snapshot-less reopen).
    pub fn snapshot_watermark(&self) -> Option<TxnId> {
        self.snapshot_watermark.map(TxnId)
    }
}

/// Coordinates database recovery after crash or restart.
///
/// The coordinator is a callback-driven driver:
///
/// 1. `plan_recovery` validates MANIFEST codec identity before any WAL read.
/// 2. `recover(on_snapshot, on_record)` streams snapshot install and WAL
///    replay through caller-supplied closures. The coordinator owns the
///    directory layout and record streaming; the caller owns storage
///    construction and application.
///
/// A `recover_into_memory_storage` convenience wrapper is kept for
/// in-crate tests and snapshot-less tooling paths that construct the
/// coordinator directly; storage recovery bootstrap uses the direct
/// callback API with a codec wired in.
pub struct RecoveryCoordinator {
    /// Canonical database layout.
    layout: DatabaseLayout,
    /// Write buffer size in bytes for the layout-rooted segmented store.
    write_buffer_size: usize,
    /// When true, WAL reader scans past corrupted regions instead of erroring.
    allow_lossy_recovery: bool,
    /// Storage codec used to decode snapshot payloads. When unset, the
    /// coordinator skips snapshot loading and falls back to full WAL
    /// replay. The `recover_into_memory_storage` wrapper does not
    /// install a codec; storage recovery bootstrap drives `recover`
    /// with a codec installed via `with_codec`.
    codec: Option<Box<dyn StorageCodec>>,
}

/// Typed failures from [`RecoveryCoordinator::plan_recovery`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CoordinatorPlanError {
    /// MANIFEST load failed while building the recovery plan.
    #[error("failed to load MANIFEST: {0}")]
    Manifest(#[source] ManifestError),

    /// MANIFEST codec id did not match the caller's configured codec id.
    #[error(
        "codec mismatch: database was created with '{stored}' but config specifies '{expected}'. \
         A database cannot be reopened with a different codec."
    )]
    CodecMismatch {
        /// Codec id recorded in the on-disk MANIFEST.
        stored: String,
        /// Codec id supplied by the caller.
        expected: String,
    },
}

/// Typed recovery failures produced by [`RecoveryCoordinator::recover_typed`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CoordinatorRecoveryError {
    /// `plan_recovery()` failed while resolving snapshot state.
    #[error(transparent)]
    Plan(CoordinatorPlanError),

    /// MANIFEST references a snapshot file that does not exist.
    #[error("MANIFEST references snapshot {snapshot_id} but {path} is missing")]
    SnapshotMissing {
        /// Snapshot id recorded in the MANIFEST.
        snapshot_id: u64,
        /// Expected snapshot file path.
        path: PathBuf,
    },

    /// Snapshot load failed after the MANIFEST pointed at the file.
    #[error("failed to load snapshot {snapshot_id} at {path}: {source}")]
    SnapshotRead {
        /// Snapshot id recorded in the MANIFEST.
        snapshot_id: u64,
        /// Snapshot file path.
        path: PathBuf,
        /// Underlying typed snapshot read error.
        #[source]
        source: SnapshotReadError,
    },

    /// WAL reader failed while scanning committed records.
    #[error("WAL read failed: {0}")]
    WalRead(#[source] WalReaderError),

    /// Transaction payload bytes inside a WAL record were invalid.
    #[error("Failed to decode transaction payload for txn {txn_id}: {detail}")]
    PayloadDecode {
        /// Transaction id of the record whose payload could not be decoded.
        txn_id: TxnId,
        /// Decoder error detail.
        detail: String,
    },

    /// Caller-supplied snapshot or record callback returned an error.
    #[error(transparent)]
    Callback(StorageError),
}

impl CoordinatorRecoveryError {
    /// Returns `true` when the error is a hard legacy-format rejection that
    /// must bypass lossy recovery.
    pub fn is_legacy_format(&self) -> bool {
        matches!(
            self,
            CoordinatorRecoveryError::SnapshotRead {
                source: SnapshotReadError::LegacyFormat { .. },
                ..
            } | CoordinatorRecoveryError::WalRead(WalReaderError::LegacyFormat { .. })
        )
    }

    /// Returns `true` when callers must bypass the lossy WAL fallback.
    ///
    /// Coordinator `Plan(...)` and snapshot load failures come from the
    /// MANIFEST / snapshot-planning step rather than WAL bytes, so recreating
    /// the in-memory store cannot heal them.
    pub fn should_bypass_lossy(&self) -> bool {
        matches!(
            self,
            CoordinatorRecoveryError::Plan(_)
                | CoordinatorRecoveryError::SnapshotMissing { .. }
                | CoordinatorRecoveryError::SnapshotRead { .. }
        ) || self.is_legacy_format()
    }
}

impl RecoveryCoordinator {
    /// Create a recovery coordinator from a canonical database layout.
    ///
    /// # Arguments
    /// * `layout` - Canonical database directory layout
    /// * `write_buffer_size` - Write buffer size in bytes for SegmentedStore
    pub fn new(layout: DatabaseLayout, write_buffer_size: usize) -> Self {
        RecoveryCoordinator {
            layout,
            write_buffer_size,
            allow_lossy_recovery: false,
            codec: None,
        }
    }

    /// Returns the coordinator's database layout.
    pub fn layout(&self) -> &DatabaseLayout {
        &self.layout
    }

    /// Enable lossy WAL recovery (scan past corrupted regions).
    pub fn with_lossy_recovery(mut self, allow: bool) -> Self {
        self.allow_lossy_recovery = allow;
        self
    }

    /// Install the storage codec used to load and validate snapshots during
    /// recovery. When unset, `recover()` does not attempt to load snapshots
    /// and `on_snapshot` never fires.
    pub fn with_codec(mut self, codec: Box<dyn StorageCodec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Truncate partial bytes at the tail of the last WAL segment.
    ///
    /// After a crash, the active segment may contain a partially-written record.
    /// If not removed, WalWriter reopens at EOF (past the partial bytes) and
    /// appends new records after the garbage. On the next recovery the reader
    /// stops at the partial record and never reaches the later committed data.
    fn truncate_partial_tail(&self, reader: &WalReader) {
        let wal_dir = self.layout.wal_dir();
        let segments = match reader.list_segments(wal_dir) {
            Ok(s) => s,
            Err(_) => return,
        };
        let last_seg = match segments.last() {
            Some(&n) => n,
            None => return,
        };

        let (_, valid_end, _, _) = match reader.read_segment(wal_dir, last_seg) {
            Ok(r) => r,
            Err(_) => return,
        };

        let seg_path = WalSegment::segment_path(wal_dir, last_seg);
        let file_len = match std::fs::metadata(&seg_path) {
            Ok(m) => m.len(),
            Err(_) => return,
        };

        if valid_end < file_len {
            let bytes_truncated = file_len - valid_end;
            match std::fs::OpenOptions::new().write(true).open(&seg_path) {
                Ok(file) => {
                    if let Err(e) = file.set_len(valid_end).and_then(|_| file.sync_all()) {
                        warn!(target: "strata::recovery",
                            segment = last_seg, error = %e,
                            "Failed to truncate partial WAL tail — next restart may re-encounter it");
                    } else {
                        info!(target: "strata::recovery",
                            segment = last_seg, bytes_truncated,
                            "Truncated partial WAL tail");
                    }
                }
                Err(e) => {
                    warn!(target: "strata::recovery",
                        segment = last_seg, error = %e,
                        "Failed to open WAL segment for truncation");
                }
            }
        }
    }

    /// Read the MANIFEST (if any) and validate that its codec identity
    /// matches `expected_codec_id` before any WAL bytes are touched.
    ///
    /// Returns a [`RecoveryPlan`] that summarizes what recovery will consume:
    /// snapshot identity, snapshot watermark, and the last flushed commit id.
    /// If the MANIFEST is absent the returned plan is the fresh-database plan.
    ///
    /// # Errors
    ///
    /// - [`CoordinatorPlanError::Manifest`] when the MANIFEST cannot be parsed.
    /// - [`CoordinatorPlanError::CodecMismatch`] when the on-disk codec id does
    ///   not match `expected_codec_id`.
    pub fn plan_recovery(
        &self,
        expected_codec_id: &str,
    ) -> Result<RecoveryPlan, CoordinatorPlanError> {
        let manifest_path = self.layout.manifest_path();
        if !ManifestManager::exists(manifest_path) {
            return Ok(RecoveryPlan::fresh());
        }
        let mgr = ManifestManager::load(manifest_path.to_path_buf())
            .map_err(CoordinatorPlanError::Manifest)?;
        let m = mgr.manifest();
        if m.codec_id != expected_codec_id {
            return Err(CoordinatorPlanError::CodecMismatch {
                stored: m.codec_id.clone(),
                expected: expected_codec_id.to_owned(),
            });
        }
        Ok(RecoveryPlan {
            manifest_present: true,
            codec_id: Some(m.codec_id.clone()),
            snapshot_id: m.snapshot_id,
            snapshot_watermark: m.snapshot_watermark,
            flushed_through_commit_id: m.flushed_through_commit_id,
            database_uuid: Some(m.database_uuid),
        })
    }

    /// Read the MANIFEST and, if a snapshot is recorded, load and return it
    /// after validating magic/CRC/codec via [`SnapshotReader`]. Returns
    /// `Ok(None)` when the MANIFEST has no snapshot recorded or is absent.
    ///
    /// Codec validation runs at two layers:
    /// 1. MANIFEST-level: `plan_recovery` rejects pre-WAL-read if the MANIFEST
    ///    codec doesn't match the caller-installed codec.
    /// 2. Snapshot-level: `SnapshotReader::load` rejects if the snapshot file's
    ///    embedded codec id doesn't match. These should align when the
    ///    MANIFEST and snapshot were written by the same process; snapshot
    fn load_snapshot_if_present(
        &self,
        codec: &dyn StorageCodec,
    ) -> Result<Option<LoadedSnapshot>, CoordinatorRecoveryError> {
        let plan = self
            .plan_recovery(codec.codec_id())
            .map_err(CoordinatorRecoveryError::Plan)?;
        let snapshot_id = match plan.snapshot_id {
            Some(id) => id,
            None => return Ok(None),
        };
        let path = snapshot_path(self.layout.snapshots_dir(), snapshot_id);
        if !path.exists() {
            return Err(CoordinatorRecoveryError::SnapshotMissing { snapshot_id, path });
        }
        let reader = SnapshotReader::new(clone_codec(codec));
        let snapshot =
            reader
                .load(&path)
                .map_err(|source| CoordinatorRecoveryError::SnapshotRead {
                    snapshot_id,
                    path: path.clone(),
                    source,
                })?;
        info!(
            target: "strata::recovery",
            snapshot_id,
            watermark = snapshot.watermark_txn(),
            sections = snapshot.sections.len(),
            "Loaded snapshot"
        );
        Ok(Some(snapshot))
    }

    /// Drive recovery through caller-supplied callbacks.
    ///
    /// The caller owns storage construction; the coordinator only:
    ///
    /// 1. Loads the snapshot declared in the MANIFEST (when a codec is
    ///    installed via `with_codec`) and invokes `on_snapshot` with the
    ///    decoded `LoadedSnapshot`. When no codec is installed the
    ///    coordinator skips snapshot loading and falls back to full WAL
    ///    replay; `on_snapshot` does not fire in that path.
    /// 2. Streams committed WAL records and invokes `on_record` for each,
    ///    preserving arrival order. Partial tails on the active segment
    ///    are truncated before return so reopens land on clean boundaries.
    /// 3. Returns `RecoveryStats` summarizing the replay.
    ///
    /// The coordinator decodes each WAL record once internally to populate
    /// `RecoveryStats::writes_applied`/`deletes_applied`/`final_version`;
    /// callers that need the decoded payload perform their own decode
    /// inside `on_record` to preserve the inline-decode invariant.
    ///
    /// Typed recovery driver used by the engine's recovery orchestration.
    ///
    /// [`recover`](Self::recover) is the canonical storage-owned callback API
    /// and forwards directly to this method.
    pub fn recover_typed<FS, FR>(
        self,
        on_snapshot: FS,
        mut on_record: FR,
    ) -> Result<RecoveryStats, CoordinatorRecoveryError>
    where
        FS: FnOnce(LoadedSnapshot) -> StorageResult<()>,
        FR: FnMut(&WalRecord) -> StorageResult<()>,
    {
        let mut stats = RecoveryStats::default();
        let mut max_version = 0u64;
        let mut max_txn_id = 0u64;

        // Snapshot loading: when the caller installed a codec via
        // `with_codec()`, the coordinator consults the MANIFEST for a
        // recorded snapshot and invokes `on_snapshot` with the decoded
        // `LoadedSnapshot`. When no codec is installed, snapshot loading
        // is skipped — the WAL-only replay path still produces
        // `RecoveryStats` and the `recover_into_memory_storage`
        // convenience wrapper depends on this no-codec default.
        //
        // The snapshot's `watermark_txn` is the max TxnId covered by the
        // snapshot and is folded into `max_txn_id` so the txn-id counter
        // bootstraps above it. The snapshot's max *commit version* is not
        // observable here — it lives inside the per-entry `version` fields
        // the callback installs — so callers who need `stats.final_version`
        // to include the snapshot must fold in their storage's post-install
        // `current_version()` themselves. Doing that here would require
        // the coordinator to reach into caller-owned state.
        //
        // `snapshot_watermark_txn` also drives delta-WAL replay below:
        // records with `txn_id <= snapshot_watermark_txn` are already
        // covered by the snapshot and must be skipped to avoid double-apply.
        let mut snapshot_watermark_txn: u64 = 0;
        if let Some(codec) = self.codec.as_deref() {
            if let Some(snapshot) = self.load_snapshot_if_present(codec)? {
                stats.from_checkpoint = true;
                snapshot_watermark_txn = snapshot.watermark_txn();
                max_txn_id = max_txn_id.max(snapshot_watermark_txn);
                on_snapshot(snapshot).map_err(CoordinatorRecoveryError::Callback)?;
            }
        }

        // If WAL dir doesn't exist, return an empty-replay plan.
        if !self.layout.wal_dir().exists() {
            stats.final_version = CommitVersion(max_version);
            stats.max_txn_id = TxnId(max_txn_id);
            return Ok(stats);
        }

        // Stream records from segmented WAL one segment at a time.
        // This bounds memory to O(largest_segment) instead of O(total_wal_size),
        // preventing OOM on large databases.
        let mut reader = WalReader::new();
        if self.allow_lossy_recovery {
            reader = reader.with_lossy_recovery();
        }
        // T3-E12 §D3 Site 1: thread the coordinator's codec so
        // codec-encoded payloads (AES-GCM etc.) decode correctly.
        // Without a codec installed, the reader treats each payload as
        // identity — the pre-T3-E12 behavior for non-encrypted databases.
        if let Some(c) = self.codec.as_deref() {
            reader = reader.with_codec(clone_codec(c));
        }
        let records_iter = reader
            .iter_all(self.layout.wal_dir())
            .map_err(CoordinatorRecoveryError::WalRead)?;

        for record_result in records_iter {
            let record = record_result.map_err(CoordinatorRecoveryError::WalRead)?;

            // Delta-WAL replay: records at or below the snapshot watermark
            // are already reflected in the installed snapshot. Skip them to
            // avoid double-apply of puts/deletes. `snapshot_watermark_txn`
            // is zero when no snapshot was loaded, which degenerates to
            // full WAL replay (the no-codec fallback path).
            //
            // We still account for the txn id in `max_txn_id` so the
            // downstream coordinator sees the true max id across both
            // sources.
            if snapshot_watermark_txn > 0 && record.txn_id.as_u64() <= snapshot_watermark_txn {
                max_txn_id = max_txn_id.max(record.txn_id.as_u64());
                stats.txns_skipped_below_watermark += 1;
                continue;
            }

            let payload = TransactionPayload::from_bytes(&record.writeset).map_err(|e| {
                CoordinatorRecoveryError::PayloadDecode {
                    txn_id: record.txn_id,
                    detail: e.to_string(),
                }
            })?;

            // Invoke the caller before bumping stats so a failing callback
            // never leaves behind inflated counts claiming work that was
            // never applied.
            on_record(&record).map_err(CoordinatorRecoveryError::Callback)?;

            max_txn_id = max_txn_id.max(record.txn_id.as_u64());
            max_version = max_version.max(payload.version);
            stats.writes_applied += payload.puts.len();
            stats.deletes_applied += payload.deletes.len();
            stats.txns_replayed += 1;
        }

        // Truncate partial WAL tail in the active (last) segment so that
        // WalWriter::new() reopens at a clean record boundary (#1741).
        self.truncate_partial_tail(&reader);

        // Rebuild missing or corrupt `.meta` sidecars from segment records
        // so subsequent compaction/read paths hit the O(1) fast path rather
        // than falling back to a full scan per segment. Best-effort: errors
        // are logged but never fail recovery.
        self.rebuild_missing_sidecars(&reader);

        stats.final_version = CommitVersion(max_version);
        stats.max_txn_id = TxnId(max_txn_id);

        Ok(stats)
    }

    /// Drive recovery through caller-supplied callbacks.
    ///
    /// Propagates errors from `on_snapshot`, `on_record`, and WAL reading.
    /// A hard error from `on_record` halts replay immediately.
    pub fn recover<FS, FR>(
        self,
        on_snapshot: FS,
        on_record: FR,
    ) -> Result<RecoveryStats, CoordinatorRecoveryError>
    where
        FS: FnOnce(LoadedSnapshot) -> StorageResult<()>,
        FR: FnMut(&WalRecord) -> StorageResult<()>,
    {
        self.recover_typed(on_snapshot, on_record)
    }

    /// Best-effort sidecar rebuild for closed WAL segments whose `.meta`
    /// file is missing or fails the header/CRC check.
    ///
    /// For each segment whose `.meta` cannot be read cleanly, this scans
    /// the segment records via [`WalReader::read_segment`] and writes a
    /// freshly-computed [`SegmentMeta`]. Existing valid sidecars are left
    /// untouched so re-running recovery is cheap.
    ///
    /// The active (last) segment is skipped because the WAL writer owns
    /// and lazily rebuilds the active sidecar on reopen; touching it here
    /// would race with writer state.
    ///
    /// Failures during scan or write are logged at `warn` and do not
    /// escalate — readers and compaction already fall back to full-segment
    /// scans when a sidecar is absent, so a failed rebuild only costs
    /// performance, not correctness.
    fn rebuild_missing_sidecars(&self, reader: &WalReader) {
        let wal_dir = self.layout.wal_dir();
        let segments = match reader.list_segments(wal_dir) {
            Ok(s) => s,
            Err(_) => return,
        };
        if segments.is_empty() {
            return;
        }
        // Skip the active (last) segment — the writer owns its sidecar.
        let closed = &segments[..segments.len().saturating_sub(1)];
        for &segment_number in closed {
            match SegmentMeta::read_from_file(wal_dir, segment_number) {
                Ok(Some(meta)) if meta.segment_number == segment_number => {
                    // Existing sidecar is structurally valid; leave it alone.
                    continue;
                }
                Ok(Some(_)) => {
                    warn!(
                        target: "strata::recovery",
                        segment = segment_number,
                        "Sidecar segment_number mismatch; rebuilding"
                    );
                }
                Ok(None) => {
                    // Missing — rebuild silently.
                }
                Err(e) => {
                    warn!(
                        target: "strata::recovery",
                        segment = segment_number,
                        error = %e,
                        "Sidecar corrupt; rebuilding"
                    );
                }
            }

            let (records, _, _, _) = match reader.read_segment(wal_dir, segment_number) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        target: "strata::recovery",
                        segment = segment_number,
                        error = %e,
                        "Failed to scan segment for sidecar rebuild; readers will fall back to full scan"
                    );
                    continue;
                }
            };
            let mut meta = SegmentMeta::new_empty(segment_number);
            for record in &records {
                meta.track_record(record.txn_id, record.timestamp);
            }
            if let Err(e) = meta.write_to_file(wal_dir) {
                warn!(
                    target: "strata::recovery",
                    segment = segment_number,
                    error = %e,
                    "Failed to persist rebuilt sidecar; readers will fall back to full scan"
                );
            } else {
                info!(
                    target: "strata::recovery",
                    segment = segment_number,
                    records = records.len(),
                    "Rebuilt missing WAL segment sidecar"
                );
            }
        }
    }

    /// Convenience wrapper that constructs a fresh `SegmentedStore` rooted
    /// at the layout's segments directory and applies all WAL records into
    /// it, returning the fully-materialized `RecoveryResult`.
    ///
    /// Used by in-crate tests and snapshot-less tooling paths that
    /// construct the coordinator directly. Database open drives
    /// storage recovery through `recovery_bootstrap`, so the shipped
    /// open path does not go through this wrapper. Snapshot loading is
    /// not performed here — this entry
    /// point intentionally drops any installed codec and uses full WAL
    /// replay only, keeping the convenience API deterministic for
    /// snapshot-less tests and tooling fixtures.
    pub fn recover_into_memory_storage(self) -> Result<RecoveryResult, CoordinatorRecoveryError> {
        let mut coordinator = self;
        // This convenience wrapper is deliberately WAL-only. If a caller
        // previously installed a codec for `recover()`, do not let that
        // trigger snapshot loading here: the wrapper has no storage-owned
        // snapshot install path and must therefore ignore the codec rather
        // than load-and-discard checkpoint state.
        coordinator.codec = None;
        let storage = SegmentedStore::with_dir(
            coordinator.layout.segments_dir().to_path_buf(),
            coordinator.write_buffer_size,
        );
        let stats = {
            let storage_ref = &storage;
            coordinator.recover(
                |_snapshot| Ok(()),
                |record| apply_wal_record_to_memory_storage(storage_ref, record),
            )?
        };
        Ok(RecoveryResult { storage, stats })
    }
}

/// Apply a WAL record to `SegmentedStore` using recovery-specific write paths
/// that preserve the original commit timestamp and the payload's TTL
/// (`#1619` / `#1740`).
///
/// This is the default apply used by
/// [`RecoveryCoordinator::recover_into_memory_storage`] and the storage
/// recovery bootstrap's callback-driven [`RecoveryCoordinator::recover`] path.
pub fn apply_wal_record_to_memory_storage(
    storage: &SegmentedStore,
    record: &WalRecord,
) -> StorageResult<()> {
    let payload = TransactionPayload::from_bytes(&record.writeset).map_err(|e| {
        StorageError::corruption(format!(
            "Failed to decode transaction payload for txn {}: {}",
            record.txn_id, e
        ))
    })?;
    let version = CommitVersion(payload.version);
    for (i, (key, value)) in payload.puts.iter().enumerate() {
        let ttl_ms = payload.put_ttls.get(i).copied().unwrap_or(0);
        storage.put_recovery_entry(
            key.clone(),
            value.clone(),
            version,
            record.timestamp,
            ttl_ms,
        )?;
    }
    for key in &payload.deletes {
        storage.delete_recovery_entry(key, version, record.timestamp)?;
    }
    Ok(())
}

/// Result of recovery operation
pub struct RecoveryResult {
    /// Recovered storage with all committed transactions applied
    pub storage: SegmentedStore,
    /// Statistics about the recovery process
    pub stats: RecoveryStats,
}

impl RecoveryResult {
    /// Create an empty recovery result with fresh storage and zero stats.
    ///
    /// Used as a fallback when recovery fails (e.g., corrupted snapshot or
    /// unreadable WAL), allowing the database to start with a clean state
    /// rather than refusing to open.
    pub fn empty() -> Self {
        RecoveryResult {
            storage: SegmentedStore::new(),
            stats: RecoveryStats::default(),
        }
    }
}

/// Statistics from recovery
///
/// Provides detailed information about what happened during recovery,
/// useful for debugging, monitoring, and verification.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Number of committed transactions replayed
    ///
    /// Each WalRecord = one committed transaction.
    pub txns_replayed: usize,

    /// Number of incomplete transactions discarded
    ///
    /// With the segmented WAL, incomplete transactions never produce a
    /// WalRecord (the record is only written after commit), so this is
    /// always 0. Partial records are silently skipped by the reader.
    pub incomplete_txns: usize,

    /// Number of aborted transactions discarded
    ///
    /// With the segmented WAL, aborted transactions never produce a
    /// WalRecord, so this is always 0.
    pub aborted_txns: usize,

    /// Number of write operations applied
    pub writes_applied: usize,

    /// Number of delete operations applied
    pub deletes_applied: usize,

    /// Final version after recovery.
    ///
    /// This is the highest version seen in the WAL, used to initialize the
    /// storage-owned transaction manager's version counter.
    pub final_version: CommitVersion,

    /// Maximum transaction ID seen in WAL.
    ///
    /// This is used to initialize the storage-owned transaction manager's
    /// `next_txn_id` counter so new transactions stay unique across restarts.
    pub max_txn_id: TxnId,

    /// Whether a snapshot was loaded during recovery. Set to `true` when
    /// `recover()` consulted the MANIFEST, found a snapshot reference, and
    /// handed the decoded payload to `on_snapshot`. Snapshot loading
    /// requires a codec to be installed via `with_codec()`.
    pub from_checkpoint: bool,

    /// Number of WAL records skipped during delta replay because their
    /// `txn_id` was at or below the snapshot watermark (already reflected
    /// in the installed snapshot). Always zero for WAL-only recovery.
    pub txns_skipped_below_watermark: usize,
}

impl RecoveryStats {
    /// Total operations applied (writes + deletes)
    pub fn total_operations(&self) -> usize {
        self.writes_applied + self.deletes_applied
    }

    /// Total transactions found (replayed + incomplete + aborted)
    pub fn total_transactions(&self) -> usize {
        self.txns_replayed + self.incomplete_txns + self.aborted_txns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as strata_storage;
    use crate::durability::TransactionPayload;
    use std::path::Path;
    use std::sync::Arc;
    use strata_core::id::{CommitVersion, TxnId};
    use strata_core::value::Value;
    use strata_core::BranchId;
    use strata_storage::durability::codec::IdentityCodec;
    use strata_storage::durability::format::WalRecord;
    use strata_storage::durability::now_micros;
    use strata_storage::durability::wal::{DurabilityMode, WalConfig, WalWriter};
    use strata_storage::{Key, Namespace, TransactionManager as StorageTransactionManager};
    use tempfile::TempDir;

    fn create_test_namespace(branch_id: BranchId) -> Arc<Namespace> {
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    fn create_test_wal(dir: &std::path::Path) -> WalWriter {
        WalWriter::new(
            dir.to_path_buf(),
            [0u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            Box::new(IdentityCodec),
        )
        .unwrap()
    }

    fn test_layout(root: &Path) -> DatabaseLayout {
        DatabaseLayout::from_root(root)
    }

    fn test_recovery(root: &Path) -> RecoveryCoordinator {
        RecoveryCoordinator::new(test_layout(root), 0)
    }

    /// Helper: write a committed transaction to the WAL
    fn write_txn(
        wal: &mut WalWriter,
        txn_id: u64,
        branch_id: BranchId,
        puts: Vec<(Key, Value)>,
        deletes: Vec<Key>,
        version: u64,
    ) {
        let payload = TransactionPayload {
            version,
            puts,
            deletes,
            put_ttls: vec![],
        };
        let record = WalRecord::new(
            TxnId(txn_id),
            *branch_id.as_bytes(),
            now_micros(),
            payload.to_bytes(),
        );
        wal.append(&record).unwrap();
        wal.flush().unwrap();
    }

    #[test]
    fn test_recovery_empty_wal() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        // Create empty WAL directory with an empty segment
        std::fs::create_dir_all(&wal_dir).unwrap();
        let _wal = create_test_wal(&wal_dir);

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 0);
        assert_eq!(result.stats.final_version, CommitVersion(0));
    }

    #[test]
    fn test_recovery_nonexistent_dir() {
        let temp_dir = TempDir::new().unwrap();
        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 0);
        assert_eq!(result.stats.final_version, CommitVersion(0));
    }

    #[test]
    fn test_recovery_committed_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "test_key");

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(key.clone(), Value::Int(42))],
                vec![],
                100,
            );
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 1);
        assert_eq!(result.stats.writes_applied, 1);
        assert_eq!(result.stats.final_version, CommitVersion(100));

        let stored = result
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(stored.value, Value::Int(42));
        assert_eq!(stored.version.as_u64(), 100);
    }

    #[test]
    fn test_recovery_version_preservation() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);

            // Transaction 1: version 100
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![
                    (Key::new_kv(ns.clone(), "key1"), Value::Int(1)),
                    (Key::new_kv(ns.clone(), "key2"), Value::Int(2)),
                ],
                vec![],
                100,
            );

            // Transaction 2: version 200
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "key3"), Value::Int(3))],
                vec![],
                200,
            );
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.final_version, CommitVersion(200));

        let key1 = Key::new_kv(ns.clone(), "key1");
        assert_eq!(
            result
                .storage
                .get_versioned(&key1, CommitVersion::MAX)
                .unwrap()
                .unwrap()
                .version
                .as_u64(),
            100
        );

        let key2 = Key::new_kv(ns.clone(), "key2");
        assert_eq!(
            result
                .storage
                .get_versioned(&key2, CommitVersion::MAX)
                .unwrap()
                .unwrap()
                .version
                .as_u64(),
            100
        );

        let key3 = Key::new_kv(ns.clone(), "key3");
        assert_eq!(
            result
                .storage
                .get_versioned(&key3, CommitVersion::MAX)
                .unwrap()
                .unwrap()
                .version
                .as_u64(),
            200
        );
    }

    #[test]
    fn test_recovery_determinism() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=5u64 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key{}", i)),
                        Value::Int(i as i64 * 10),
                    )],
                    vec![],
                    i * 100,
                );
            }
        }

        let coordinator = test_recovery(temp_dir.path());
        let result1 = coordinator.recover_into_memory_storage().unwrap();

        let coordinator = test_recovery(temp_dir.path());
        let result2 = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result1.stats.final_version, result2.stats.final_version);
        assert_eq!(result1.stats.txns_replayed, result2.stats.txns_replayed);
        assert_eq!(result1.stats.writes_applied, result2.stats.writes_applied);

        for i in 1..=5u64 {
            let key = Key::new_kv(ns.clone(), format!("key{}", i));
            let v1 = result1
                .storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .unwrap();
            let v2 = result2
                .storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(v1.value, v2.value);
            assert_eq!(v1.version, v2.version);
        }
    }

    #[test]
    fn test_recovery_with_deletes() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "deleted_key");

        {
            let mut wal = create_test_wal(&wal_dir);

            // Write then delete in separate transactions
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(key.clone(), Value::String("exists".to_string()))],
                vec![],
                100,
            );
            write_txn(&mut wal, 2, branch_id, vec![], vec![key.clone()], 101);
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.writes_applied, 1);
        assert_eq!(result.stats.deletes_applied, 1);

        // Key should be deleted
        assert!(result
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_recovery_stats_helpers() {
        let stats = RecoveryStats {
            txns_replayed: 5,
            incomplete_txns: 0,
            aborted_txns: 0,
            writes_applied: 10,
            deletes_applied: 3,
            final_version: CommitVersion(100),
            max_txn_id: TxnId(8),
            from_checkpoint: false,
            txns_skipped_below_watermark: 0,
        };

        assert_eq!(stats.total_operations(), 13);
        assert_eq!(stats.total_transactions(), 5);
    }

    #[test]
    fn test_recovery_coordinator_layout() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        std::fs::create_dir_all(&wal_dir).unwrap();
        let _wal = create_test_wal(&wal_dir);

        let layout = test_layout(temp_dir.path());
        let coordinator = RecoveryCoordinator::new(layout.clone(), 0);
        assert_eq!(coordinator.layout(), &layout);

        let result = coordinator.recover_into_memory_storage().unwrap();
        assert!(!result.stats.from_checkpoint);
    }

    // ========================================
    // Crash Scenario Tests
    // ========================================
    //
    // With the segmented WAL, crash scenarios are simpler:
    // - A WalRecord is only written for committed transactions
    // - Partial records at end of segment are silently skipped
    // - No BeginTxn/CommitTxn framing means no "incomplete" transactions

    #[test]
    fn test_crash_before_any_activity() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        std::fs::create_dir_all(&wal_dir).unwrap();
        let _wal = create_test_wal(&wal_dir);

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 0);
        assert_eq!(result.stats.final_version, CommitVersion(0));
        assert_eq!(result.stats.incomplete_txns, 0);
    }

    #[test]
    fn test_crash_after_commit_written() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "durable_key"),
                    Value::String("must_exist".to_string()),
                )],
                vec![],
                100,
            );
            // CRASH after commit marker written - record is durable
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 1);
        assert_eq!(result.stats.incomplete_txns, 0);

        let stored = result
            .storage
            .get_versioned(&Key::new_kv(ns, "durable_key"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(stored.value, Value::String("must_exist".to_string()));
        assert_eq!(stored.version.as_u64(), 100);
    }

    #[test]
    fn test_partial_record_at_end() {
        // Simulate crash mid-write by appending garbage after valid records
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "valid"), Value::Int(42))],
                vec![],
                100,
            );
        }

        // Append garbage to simulate crash mid-write of a second record
        let segment_path =
            strata_storage::durability::format::WalSegment::segment_path(&wal_dir, 1);
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .unwrap();
        file.write_all(&[0xFF; 20]).unwrap();

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        // Valid record should be recovered, garbage skipped
        assert_eq!(result.stats.txns_replayed, 1);
        let stored = result
            .storage
            .get_versioned(&Key::new_kv(ns, "valid"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(stored.value, Value::Int(42));
    }

    #[test]
    fn test_crash_recovery_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "key"), Value::Int(42))],
                vec![],
                100,
            );
        }

        let coordinator = test_recovery(temp_dir.path());
        let result1 = coordinator.recover_into_memory_storage().unwrap();

        let coordinator = test_recovery(temp_dir.path());
        let result2 = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result1.stats.txns_replayed, result2.stats.txns_replayed);
        assert_eq!(result1.stats.final_version, result2.stats.final_version);
        assert_eq!(result1.stats.writes_applied, result2.stats.writes_applied);

        let v1 = result1
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        let v2 = result2
            .storage
            .get_versioned(&Key::new_kv(ns, "key"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(v1.value, v2.value);
        assert_eq!(v1.version, v2.version);
    }

    #[test]
    fn test_recovery_version_counter() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns, "key"), Value::Int(1))],
                vec![],
                999,
            );
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.final_version, CommitVersion(999));
    }

    #[test]
    fn test_full_database_lifecycle_with_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=10u64 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key{}", i)),
                        Value::Int(i as i64 * 10),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 10);
        assert_eq!(result.stats.final_version, CommitVersion(10));

        for i in 1..=10u64 {
            let key = Key::new_kv(ns.clone(), format!("key{}", i));
            let stored = result
                .storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(stored.value, Value::Int(i as i64 * 10));
            assert_eq!(stored.version.as_u64(), i);
        }
    }

    #[test]
    fn test_recovery_mixed_operations_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);

            // Txn 1: Write key1
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key1"),
                    Value::String("initial".to_string()),
                )],
                vec![],
                1,
            );

            // Txn 2: Update key1
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key1"),
                    Value::String("updated".to_string()),
                )],
                vec![],
                2,
            );

            // Txn 3: Write key2 and delete it
            write_txn(
                &mut wal,
                3,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key2"),
                    Value::String("temp".to_string()),
                )],
                vec![Key::new_kv(ns.clone(), "key2")],
                3,
            );

            // Txn 4: Write key3
            write_txn(
                &mut wal,
                4,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "key3"), Value::Int(42))],
                vec![],
                5,
            );
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 4);
        assert_eq!(result.stats.writes_applied, 4);
        assert_eq!(result.stats.deletes_applied, 1);
        assert_eq!(result.stats.final_version, CommitVersion(5));

        // key1 should be "updated" at version 2
        let key1 = result
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key1"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(key1.value, Value::String("updated".to_string()));
        assert_eq!(key1.version.as_u64(), 2);

        // key2 should be deleted
        assert!(result
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key2"), CommitVersion::MAX)
            .unwrap()
            .is_none());

        // key3 should exist
        let key3 = result
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key3"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(key3.value, Value::Int(42));
        assert_eq!(key3.version.as_u64(), 5);
    }

    #[test]
    fn test_recovery_maintains_transaction_order() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for v in [100u64, 200, 300] {
                write_txn(
                    &mut wal,
                    v,
                    branch_id,
                    vec![(Key::new_kv(ns.clone(), "counter"), Value::Int(v as i64))],
                    vec![],
                    v,
                );
            }
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        let counter = result
            .storage
            .get_versioned(&Key::new_kv(ns, "counter"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(counter.value, Value::Int(300));
        assert_eq!(counter.version.as_u64(), 300);
    }

    #[test]
    fn test_new_transactions_after_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns, "existing"), Value::Int(100))],
                vec![],
                100,
            );
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        let txn_manager = StorageTransactionManager::with_txn_id(
            result.stats.final_version,
            result.stats.max_txn_id,
        );
        let new_txn_id = txn_manager.next_txn_id().unwrap();
        assert!(new_txn_id > TxnId::ZERO);
    }

    #[test]
    fn test_recovery_many_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let num_txns = 100u64;

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=num_txns {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key_{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, num_txns as usize);
        assert_eq!(result.stats.final_version, CommitVersion(num_txns));

        for i in [1, 50, 100] {
            let key = Key::new_kv(ns.clone(), format!("key_{}", i));
            let stored = result
                .storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(stored.value, Value::Int(i as i64));
        }
    }

    /// Verify that streaming recovery (iter_all) produces the same results
    /// as the prior bulk approach (read_all). This test uses iter_all
    /// indirectly via RecoveryCoordinator::recover(), which now uses iter_all.
    #[test]
    fn test_streaming_recovery_matches_read_all() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=20u64 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key{}", i)),
                        Value::Int(i as i64 * 10),
                    )],
                    vec![],
                    i,
                );
            }
        }

        // Verify recovery (which uses iter_all) returns correct results
        let coordinator = test_recovery(temp_dir.path());
        let result = coordinator.recover_into_memory_storage().unwrap();

        assert_eq!(result.stats.txns_replayed, 20);
        assert_eq!(result.stats.final_version, CommitVersion(20));
        assert_eq!(result.stats.writes_applied, 20);

        // Cross-check: read_all should yield the same records
        let reader = WalReader::new();
        let read_all_result = reader.read_all(&wal_dir).unwrap();
        assert_eq!(read_all_result.records.len(), 20);

        // Verify each key was written correctly
        for i in 1..=20u64 {
            let key = Key::new_kv(ns.clone(), format!("key{}", i));
            let stored = result
                .storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(stored.value, Value::Int(i as i64 * 10));
            assert_eq!(stored.version.as_u64(), i);
        }
    }

    /// Issue #1741: Partial WAL tail not truncated before reopen.
    ///
    /// Scenario: crash leaves partial bytes at the end of the active segment.
    /// After recovery, WalWriter reopens at EOF (past partial bytes) and
    /// appends new records. On second recovery, the reader stops at the
    /// partial record and the later committed records are lost.
    #[test]
    fn test_issue_1741_partial_tail_truncated_before_reopen() {
        use strata_storage::durability::format::WalSegment;

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Step 1: Write two valid committed records
        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key_before"),
                    Value::String("before_crash".into()),
                )],
                vec![],
                1,
            );
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key_also_before"),
                    Value::String("also_before".into()),
                )],
                vec![],
                2,
            );
        }

        // Step 2: Simulate crash — append partial garbage bytes to the segment
        {
            use std::io::Write;
            let seg_path = WalSegment::segment_path(&wal_dir, 1);
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&seg_path)
                .unwrap();
            // Write partial record: a valid-looking length prefix but incomplete data
            f.write_all(&[0xFF; 13]).unwrap();
            f.flush().unwrap();
        }

        // Step 3: First recovery — should replay the 2 valid records
        // and truncate the partial tail
        {
            let coordinator = test_recovery(temp_dir.path());
            let result = coordinator.recover_into_memory_storage().unwrap();
            assert_eq!(result.stats.txns_replayed, 2);
        }

        // Step 4: Reopen WAL writer (simulates engine restart after recovery)
        // and write a new committed record
        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                3,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key_after"),
                    Value::String("after_crash".into()),
                )],
                vec![],
                3,
            );
        }

        // Step 5: Second recovery — MUST see all 3 records.
        // Without the fix, the reader stops at the old partial bytes
        // and never reaches record 3.
        {
            let coordinator = test_recovery(temp_dir.path());
            let result = coordinator.recover_into_memory_storage().unwrap();

            assert_eq!(
                result.stats.txns_replayed, 3,
                "Second recovery must see the record written after crash recovery"
            );
            assert_eq!(result.stats.final_version, CommitVersion(3));

            // Verify the post-crash record is accessible
            let key_after = Key::new_kv(ns.clone(), "key_after");
            let stored = result
                .storage
                .get_versioned(&key_after, CommitVersion::MAX)
                .unwrap()
                .expect("key_after must be visible after second recovery");
            assert_eq!(stored.value, Value::String("after_crash".into()));
        }
    }

    // ========================================
    // plan_recovery + callback API tests
    // ========================================

    use strata_storage::durability::ManifestManager;

    /// A fresh database (no MANIFEST) yields the zero-valued plan.
    #[test]
    fn test_plan_recovery_fresh_database() {
        let temp_dir = TempDir::new().unwrap();
        let coordinator = test_recovery(temp_dir.path());

        let plan = coordinator.plan_recovery("identity").unwrap();

        assert!(!plan.manifest_present);
        assert_eq!(plan.codec_id, None);
        assert_eq!(plan.snapshot_id, None);
        assert_eq!(plan.snapshot_watermark(), None);
        assert_eq!(plan.flushed_through_commit_id, None);
        assert_eq!(plan.database_uuid, None);
    }

    /// An existing MANIFEST with a matching codec produces a populated plan.
    #[test]
    fn test_plan_recovery_accepts_matching_codec() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());
        let uuid = [7u8; 16];
        ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            uuid,
            "identity".to_string(),
        )
        .unwrap();

        let coordinator = RecoveryCoordinator::new(layout, 0);
        let plan = coordinator.plan_recovery("identity").unwrap();

        assert!(plan.manifest_present);
        assert_eq!(plan.codec_id.as_deref(), Some("identity"));
        assert_eq!(plan.database_uuid, Some(uuid));
    }

    /// plan_recovery rejects codec mismatch before any WAL bytes are read.
    #[test]
    fn test_plan_recovery_rejects_codec_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());
        ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            [0u8; 16],
            "identity".to_string(),
        )
        .unwrap();

        let coordinator = RecoveryCoordinator::new(layout, 0);
        let err = coordinator
            .plan_recovery("aes-gcm-256")
            .expect_err("codec mismatch must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("codec mismatch"),
            "expected codec mismatch error, got: {}",
            message
        );
        assert!(message.contains("identity"));
        assert!(message.contains("aes-gcm-256"));
    }

    /// plan_recovery reports snapshot identity and watermark so the
    /// coordinator's snapshot-install path can drive from the plan.
    #[test]
    fn test_plan_recovery_reports_snapshot_identity_and_watermark() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());
        let mut mgr = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            [1u8; 16],
            "identity".to_string(),
        )
        .unwrap();
        mgr.set_snapshot_watermark(42, TxnId(7_000)).unwrap();

        let coordinator = RecoveryCoordinator::new(layout, 0);
        let plan = coordinator.plan_recovery("identity").unwrap();

        assert_eq!(plan.snapshot_id, Some(42));
        assert_eq!(plan.snapshot_watermark(), Some(TxnId(7_000)));
    }

    /// The callback-driven `recover()` fires `on_record` once per WAL record
    /// in ascending txn-id order, does not fire `on_snapshot` when no codec
    /// is installed, and returns aggregated stats matching the
    /// `recover_into_memory_storage` convenience wrapper's semantics.
    #[test]
    fn test_recover_callback_fires_on_record_in_order() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1u64..=5 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("k{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i * 10,
                );
            }
        }

        let coordinator = test_recovery(temp_dir.path());
        let mut snapshot_fired = 0usize;
        let mut record_txns: Vec<u64> = Vec::new();
        let stats = coordinator
            .recover(
                |_snapshot| {
                    snapshot_fired += 1;
                    Ok(())
                },
                |record| {
                    record_txns.push(record.txn_id.as_u64());
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(
            snapshot_fired, 0,
            "on_snapshot must not fire without codec and snapshot"
        );
        assert_eq!(record_txns, vec![1, 2, 3, 4, 5]);
        assert_eq!(stats.txns_replayed, 5);
        assert_eq!(stats.writes_applied, 5);
        assert_eq!(stats.deletes_applied, 0);
        assert_eq!(stats.final_version, CommitVersion(50));
        assert_eq!(stats.max_txn_id, TxnId(5));
    }

    /// An error from `on_record` halts replay immediately and propagates out.
    #[test]
    fn test_recover_callback_propagates_on_record_error() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1u64..=3 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("k{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coordinator = test_recovery(temp_dir.path());
        let mut seen = 0usize;
        let result = coordinator.recover(
            |_| Ok(()),
            |_record| {
                seen += 1;
                if seen == 2 {
                    Err(StorageError::corruption("injected apply failure"))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err(), "injected error must propagate");
        assert_eq!(seen, 2, "replay must stop at the failing record");
    }

    /// Stats must reflect only fully-applied records: if `on_record` errors
    /// at record N, the first N-1 records count and no partial work for
    /// record N leaks into `writes_applied`/`deletes_applied`/`txns_replayed`.
    ///
    /// Exercises the invariant by collecting stats through a wrapper that
    /// re-runs the callback sequence and inspects cumulative state at the
    /// moment the injected failure fires.
    #[test]
    fn test_recover_callback_stats_reflect_only_applied_records() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Three records with distinct put/delete mixes so the test can
        // tell from stats how far replay got.
        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "a"), Value::Int(1))],
                vec![],
                10,
            );
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "b"), Value::Int(2))],
                vec![Key::new_kv(ns.clone(), "a")],
                20,
            );
            write_txn(
                &mut wal,
                3,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "c"), Value::Int(3))],
                vec![],
                30,
            );
        }

        // First pass: succeed for record 1, fail on record 2. The first
        // record must count in stats; the failing record must not.
        let coord = test_recovery(temp_dir.path());
        let mut seen = 0usize;
        let fail_on_second = coord.recover(
            |_| Ok(()),
            |_record| {
                seen += 1;
                if seen == 2 {
                    Err(StorageError::corruption("injected"))
                } else {
                    Ok(())
                }
            },
        );
        assert!(fail_on_second.is_err());

        // Second pass: succeed for all three. Stats must reflect the full
        // replay: 3 txns, 3 puts, 1 delete, final_version = 30, max_txn_id = 3.
        let coord = test_recovery(temp_dir.path());
        let stats = coord
            .recover(|_| Ok(()), |_record| Ok(()))
            .expect("clean replay");
        assert_eq!(stats.txns_replayed, 3);
        assert_eq!(stats.writes_applied, 3);
        assert_eq!(stats.deletes_applied, 1);
        assert_eq!(stats.final_version, CommitVersion(30));
        assert_eq!(stats.max_txn_id, TxnId(3));
    }

    /// A corrupt MANIFEST fails `plan_recovery()` loudly instead of returning
    /// a silent empty plan.
    #[test]
    fn test_plan_recovery_reports_corrupt_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());
        // Write garbage at the MANIFEST path so ManifestManager::load fails.
        std::fs::write(layout.manifest_path(), b"not-a-manifest").unwrap();

        let coordinator = RecoveryCoordinator::new(layout, 0);
        let err = coordinator
            .plan_recovery("identity")
            .expect_err("corrupt MANIFEST must fail plan_recovery");
        let message = err.to_string();
        assert!(
            message.to_lowercase().contains("manifest"),
            "error should mention MANIFEST, got: {}",
            message
        );
    }

    /// Snapshot load failures must keep the snapshot id and on-disk path in the
    /// surfaced error message so operators can delete the exact artifact.
    #[test]
    fn test_recover_reports_snapshot_id_and_path_on_snapshot_load_failure() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());
        seed_snapshot(&layout, 9, 100);

        let snap = strata_storage::durability::format::snapshot_path(layout.snapshots_dir(), 9);
        std::fs::write(&snap, b"bad").unwrap();

        let coord = RecoveryCoordinator::new(layout, 0).with_codec(Box::new(IdentityCodec));
        let err = coord
            .recover(|_| Ok(()), |_| Ok(()))
            .expect_err("corrupt snapshot must surface as a typed recovery error");
        let msg = err.to_string();
        assert!(
            msg.contains("failed to load snapshot 9"),
            "error should name the snapshot id, got: {}",
            msg
        );
        assert!(
            msg.contains(&snap.display().to_string()),
            "error should name the snapshot path, got: {}",
            msg
        );
    }

    /// Empty WAL produces zero stats and never fires either callback.
    #[test]
    fn test_recover_callback_empty_wal() {
        let temp_dir = TempDir::new().unwrap();
        let coordinator = test_recovery(temp_dir.path());
        let mut snapshot_fired = 0usize;
        let mut record_fired = 0usize;
        let stats = coordinator
            .recover(
                |_| {
                    snapshot_fired += 1;
                    Ok(())
                },
                |_| {
                    record_fired += 1;
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(snapshot_fired, 0);
        assert_eq!(record_fired, 0);
        assert_eq!(stats.txns_replayed, 0);
        assert_eq!(stats.final_version, CommitVersion::ZERO);
        assert_eq!(stats.max_txn_id, TxnId::ZERO);
    }

    /// The memory-storage convenience wrapper must produce identical stats to
    /// the callback API's default-apply path.
    #[test]
    fn test_recover_into_memory_storage_matches_callback_apply() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "k"), Value::Int(10))],
                vec![],
                100,
            );
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![],
                vec![Key::new_kv(ns.clone(), "k")],
                101,
            );
        }

        let coord_a = test_recovery(temp_dir.path());
        let result = coord_a.recover_into_memory_storage().unwrap();

        let coord_b = test_recovery(temp_dir.path());
        let stats_b = coord_b.recover(|_| Ok(()), |_| Ok(())).unwrap();

        assert_eq!(result.stats.txns_replayed, stats_b.txns_replayed);
        assert_eq!(result.stats.writes_applied, stats_b.writes_applied);
        assert_eq!(result.stats.deletes_applied, stats_b.deletes_applied);
        assert_eq!(result.stats.final_version, stats_b.final_version);
        assert_eq!(result.stats.max_txn_id, stats_b.max_txn_id);
    }

    // ========================================
    // Coordinator snapshot-loading tests
    // ========================================

    use strata_storage::durability::disk_snapshot::{SnapshotSection, SnapshotWriter};
    use strata_storage::durability::format::primitive_tags;

    /// Helper: seed a snapshot file and matching MANIFEST entry so the
    /// coordinator's snapshot-loading path can find them.
    fn seed_snapshot(layout: &DatabaseLayout, snapshot_id: u64, watermark: u64) {
        std::fs::create_dir_all(layout.snapshots_dir()).unwrap();
        let writer = SnapshotWriter::new(
            layout.snapshots_dir().to_path_buf(),
            Box::new(IdentityCodec),
            [4u8; 16],
        )
        .unwrap();
        // Minimal valid KV section (entry count = 0); coordinator only
        // cares that the file loads cleanly.
        let sections = vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])];
        writer
            .create_snapshot(snapshot_id, watermark, sections)
            .unwrap();

        let mut mgr = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            [4u8; 16],
            "identity".to_string(),
        )
        .unwrap();
        mgr.set_snapshot_watermark(snapshot_id, TxnId(watermark))
            .unwrap();
    }

    /// With a codec installed and a snapshot recorded in the MANIFEST, the
    /// coordinator loads the snapshot, fires `on_snapshot` once with the
    /// correct watermark, and reports `from_checkpoint = true`.
    #[test]
    fn test_recover_invokes_on_snapshot_when_codec_and_manifest_agree() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());
        seed_snapshot(&layout, 7, 42);

        let coord = RecoveryCoordinator::new(layout, 0).with_codec(Box::new(IdentityCodec));

        let mut snapshot_seen = Vec::<u64>::new();
        let stats = coord
            .recover(
                |snapshot| {
                    snapshot_seen.push(snapshot.watermark_txn());
                    Ok(())
                },
                |_| Ok(()),
            )
            .unwrap();

        assert_eq!(snapshot_seen, vec![42]);
        assert!(stats.from_checkpoint);
        assert_eq!(stats.max_txn_id, TxnId(42));
        // The coordinator does not bump final_version from snapshot load —
        // it stays at the max payload version seen in WAL replay (zero here).
        assert_eq!(stats.final_version, CommitVersion::ZERO);
    }

    /// With a codec installed but no MANIFEST on disk, snapshot loading is
    /// skipped silently and `on_snapshot` never fires.
    #[test]
    fn test_recover_skips_snapshot_when_manifest_absent() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());

        let coord = RecoveryCoordinator::new(layout, 0).with_codec(Box::new(IdentityCodec));

        let mut fired = 0usize;
        let stats = coord
            .recover(
                |_| {
                    fired += 1;
                    Ok(())
                },
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(fired, 0);
        assert!(!stats.from_checkpoint);
    }

    /// The convenience wrapper is intentionally WAL-only even if a codec is
    /// installed. A recorded snapshot watermark must not suppress replay
    /// unless the caller uses the callback API and installs the snapshot.
    #[test]
    fn test_recover_into_memory_storage_ignores_installed_codec_and_snapshot_ref() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let layout = test_layout(temp_dir.path());
        seed_snapshot(&layout, 9, 100);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1u64..=3 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("k{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let result = RecoveryCoordinator::new(layout, 0)
            .with_codec(Box::new(IdentityCodec))
            .recover_into_memory_storage()
            .unwrap();

        assert!(!result.stats.from_checkpoint);
        assert_eq!(result.stats.txns_replayed, 3);
        assert_eq!(result.stats.txns_skipped_below_watermark, 0);
        assert_eq!(result.stats.max_txn_id, TxnId(3));

        let restored = result
            .storage
            .get_versioned(&Key::new_kv(ns, "k3"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(restored.value, Value::Int(3));
    }

    /// Delta-WAL replay: when a snapshot with watermark=N is loaded, records
    /// with `txn_id <= N` are skipped (already in the snapshot), and only
    /// records with `txn_id > N` fire `on_record`. The skipped count surfaces
    /// in `RecoveryStats::txns_skipped_below_watermark` for observability.
    #[test]
    fn test_recover_delta_wal_skips_records_at_or_below_watermark() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Seed a snapshot whose watermark sits in the middle of the WAL.
        let layout = test_layout(temp_dir.path());
        seed_snapshot(&layout, 1, 5);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1u64..=10 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("k{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coord = RecoveryCoordinator::new(layout, 0).with_codec(Box::new(IdentityCodec));
        let mut replayed_txns: Vec<u64> = Vec::new();
        let stats = coord
            .recover(
                |_snapshot| Ok(()),
                |record| {
                    replayed_txns.push(record.txn_id.as_u64());
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(
            replayed_txns,
            vec![6, 7, 8, 9, 10],
            "only records strictly above the watermark must replay"
        );
        assert_eq!(stats.txns_replayed, 5);
        assert_eq!(stats.txns_skipped_below_watermark, 5);
        assert!(stats.from_checkpoint);
        assert_eq!(stats.max_txn_id, TxnId(10));
        assert_eq!(stats.writes_applied, 5);
    }

    /// When no snapshot is loaded (no codec), every WAL record replays
    /// regardless of txn id — the delta filter degenerates to full replay.
    #[test]
    fn test_recover_without_snapshot_replays_all_records() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1u64..=4 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("k{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coord = test_recovery(temp_dir.path());
        let mut replayed = 0usize;
        let stats = coord
            .recover(
                |_| Ok(()),
                |_| {
                    replayed += 1;
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(replayed, 4);
        assert_eq!(stats.txns_replayed, 4);
        assert_eq!(stats.txns_skipped_below_watermark, 0);
        assert!(!stats.from_checkpoint);
    }

    /// Snapshot watermark sits past every WAL record: every record is
    /// skipped, `on_record` never fires, replay reports zero applied.
    #[test]
    fn test_recover_delta_wal_all_records_covered_by_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        let layout = test_layout(temp_dir.path());
        seed_snapshot(&layout, 9, 100);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1u64..=3 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("k{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coord = RecoveryCoordinator::new(layout, 0).with_codec(Box::new(IdentityCodec));
        let mut replayed = 0usize;
        let stats = coord
            .recover(
                |_| Ok(()),
                |_| {
                    replayed += 1;
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(replayed, 0);
        assert_eq!(stats.txns_replayed, 0);
        assert_eq!(stats.txns_skipped_below_watermark, 3);
        // `max_txn_id` still reflects snapshot watermark and any skipped
        // records so the downstream counter seeds safely above all sources.
        assert_eq!(stats.max_txn_id, TxnId(100));
    }

    /// Missing `.meta` sidecars on closed WAL segments are rebuilt during
    /// recovery so subsequent compaction/read paths hit the O(1) fast path.
    /// The active segment is deliberately left alone (writer owns it); corrupt
    /// sidecars on closed segments are replaced with a freshly-computed one.
    #[test]
    fn test_recover_rebuilds_missing_sidecar_on_closed_segment() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Force segment rotation by writing records whose combined size
        // exceeds the test config's 64KB segment_size. Each record carries
        // a ~40KB Value::Bytes payload, so record 2 rotates the writer and
        // lands in segment 2 while segment 1 closes with record 1.
        let big_value = || Value::Bytes(vec![0u8; 40 * 1024]);
        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "a"), big_value())],
                vec![],
                1,
            );
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "b"), big_value())],
                vec![],
                2,
            );
        }

        // Confirm we actually produced two segments before touching sidecars.
        let seg1 = WalSegment::segment_path(&wal_dir, 1);
        let seg2 = WalSegment::segment_path(&wal_dir, 2);
        assert!(seg1.exists(), "segment 1 must exist");
        assert!(seg2.exists(), "segment 2 must exist (rotation required)");

        // Delete the sidecar for the closed segment (segment 1) so recovery
        // has something to rebuild.
        let closed_sidecar =
            strata_storage::durability::format::SegmentMeta::meta_path(&wal_dir, 1);
        if closed_sidecar.exists() {
            std::fs::remove_file(&closed_sidecar).unwrap();
        }
        assert!(!closed_sidecar.exists(), "precondition: sidecar removed");

        // Recovery should rebuild the closed-segment sidecar as a side effect.
        let coord = test_recovery(temp_dir.path());
        let _ = coord.recover(|_| Ok(()), |_| Ok(())).unwrap();

        assert!(
            closed_sidecar.exists(),
            "closed-segment sidecar must be rebuilt by recovery"
        );
        let rebuilt = strata_storage::durability::format::SegmentMeta::read_from_file(&wal_dir, 1)
            .expect("rebuilt sidecar must be readable")
            .expect("rebuilt sidecar must exist");
        assert_eq!(rebuilt.segment_number, 1);
        assert_eq!(rebuilt.record_count, 1);
        assert_eq!(rebuilt.max_txn_id, TxnId(1));
    }

    /// Corrupt sidecars on closed segments (fails header/CRC check) are
    /// replaced with a freshly-computed one. A rebuild must restore the
    /// correct metadata so the fast path can trust the sidecar again.
    #[test]
    fn test_recover_rebuilds_corrupt_sidecar_on_closed_segment() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Force two segments so the first is a closed segment the rebuild
        // path targets.
        let big_value = || Value::Bytes(vec![0u8; 40 * 1024]);
        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "a"), big_value())],
                vec![],
                1,
            );
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "b"), big_value())],
                vec![],
                2,
            );
        }

        // Overwrite the closed sidecar with bytes that will fail magic/CRC
        // validation in `SegmentMeta::from_bytes`.
        let closed_sidecar =
            strata_storage::durability::format::SegmentMeta::meta_path(&wal_dir, 1);
        std::fs::write(&closed_sidecar, b"CORRUPT_GARBAGE_NOT_A_VALID_META").unwrap();

        let coord = test_recovery(temp_dir.path());
        let _ = coord.recover(|_| Ok(()), |_| Ok(())).unwrap();

        let rebuilt = strata_storage::durability::format::SegmentMeta::read_from_file(&wal_dir, 1)
            .expect("rebuilt sidecar must be readable after corruption")
            .expect("rebuilt sidecar must exist");
        assert_eq!(rebuilt.segment_number, 1);
        assert_eq!(rebuilt.record_count, 1);
        assert_eq!(rebuilt.max_txn_id, TxnId(1));
    }

    /// The active (last) segment's sidecar is deliberately left alone by
    /// rebuild — the WAL writer owns it. Verify by deleting the active
    /// sidecar and checking it is NOT rebuilt by recovery.
    #[test]
    fn test_recover_does_not_rebuild_active_segment_sidecar() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        let big_value = || Value::Bytes(vec![0u8; 40 * 1024]);
        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "a"), big_value())],
                vec![],
                1,
            );
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "b"), big_value())],
                vec![],
                2,
            );
        }

        // Delete the active-segment sidecar (segment 2 is the tail here).
        let active_sidecar =
            strata_storage::durability::format::SegmentMeta::meta_path(&wal_dir, 2);
        if active_sidecar.exists() {
            std::fs::remove_file(&active_sidecar).unwrap();
        }

        let coord = test_recovery(temp_dir.path());
        let _ = coord.recover(|_| Ok(()), |_| Ok(())).unwrap();

        // Recovery must not have rebuilt the active sidecar — that is the
        // writer's responsibility at reopen time.
        assert!(
            !active_sidecar.exists(),
            "active segment sidecar must remain untouched by the recovery rebuild"
        );
    }

    /// MANIFEST references a snapshot id but the corresponding .chk file was
    /// deleted from disk. Recovery must fail loudly, not silently fall back to
    /// WAL-only or produce an empty plan.
    #[test]
    fn test_recover_rejects_missing_snapshot_file() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(temp_dir.path());
        seed_snapshot(&layout, 3, 100);

        // Remove the snapshot file the MANIFEST still points at.
        let snap = strata_storage::durability::format::snapshot_path(layout.snapshots_dir(), 3);
        std::fs::remove_file(&snap).unwrap();

        let coord = RecoveryCoordinator::new(layout, 0).with_codec(Box::new(IdentityCodec));

        let err = coord
            .recover(|_| Ok(()), |_| Ok(()))
            .expect_err("missing snapshot file must surface as corruption");
        let msg = err.to_string();
        assert!(
            msg.contains("MANIFEST references snapshot"),
            "error should mention MANIFEST ref, got: {}",
            msg
        );
        assert!(
            msg.contains("missing"),
            "error should state the file is missing, got: {}",
            msg
        );
    }
}
