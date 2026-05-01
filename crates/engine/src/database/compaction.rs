//! Flush, checkpoint, and compaction.

use crate::{StrataError, StrataResult};
use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};
use strata_storage::durability::__internal::WalWriterEngineExt;
use strata_storage::durability::{
    BranchSnapshotEntry, CheckpointCoordinator, CheckpointData, CheckpointError, CompactionError,
    EventSnapshotEntry, JsonSnapshotEntry, KvSnapshotEntry, ManifestError, ManifestManager,
    VectorCollectionSnapshotEntry, VectorSnapshotEntry, WalOnlyCompactor,
};
use strata_storage::{Key, TypeTag};
use tracing::info;

use super::{scan_dir_size, Database, DatabaseDiskUsage, PersistenceMode};

impl Database {
    // Flush
    // ========================================================================

    /// Flush WAL to disk
    ///
    /// Forces all buffered WAL entries to be written to disk.
    /// This is automatically done based on durability mode, but can
    /// be called manually to ensure durability at a specific point.
    ///
    /// For ephemeral databases, this is a no-op.
    ///
    /// Rejected while `shutdown_with_deadline` is in progress and after a
    /// successful shutdown (see [`Database::check_not_shutting_down`]).
    /// Internal callers (`shutdown_with_deadline`'s own final flush, the
    /// `Drop` fallback) call [`Database::flush_internal`] directly to
    /// bypass the guard on the close path itself.
    pub fn flush(&self) -> StrataResult<()> {
        self.check_not_shutting_down()?;
        self.flush_internal()
    }

    /// Unguarded flush body shared by the public `flush()`, the close
    /// barrier, and the `Drop` fallback. See `flush()` for semantics.
    pub(crate) fn flush_internal(&self) -> StrataResult<()> {
        if let Some(ref wal) = self.wal_writer {
            // Wait up to 10 seconds for any in-flight background sync to complete.
            // This is defensive against pathological cases (flush thread panic, etc.).
            const MAX_WAIT_MS: u64 = 10_000;
            let start = std::time::Instant::now();

            loop {
                let mut wal = wal.lock();
                if !wal.sync_in_flight() {
                    return wal.flush().map_err(StrataError::from);
                }

                // Preserve the in-flight background sync snapshot. The flush
                // thread will clear the flag once it can safely hand off.
                drop(wal);

                if start.elapsed().as_millis() as u64 >= MAX_WAIT_MS {
                    return Err(StrataError::internal(
                        "flush timed out waiting for background sync to complete",
                    ));
                }

                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        } else {
            // Ephemeral mode - no-op
            Ok(())
        }
    }

    // ========================================================================
    // Checkpoint & Compaction
    // ========================================================================

    /// Create a snapshot checkpoint of the current database state.
    ///
    /// Checkpoints serialize all primitive state to a crash-safe snapshot file
    /// and update the MANIFEST watermark. After a checkpoint, WAL compaction
    /// can safely remove segments covered by the snapshot.
    ///
    /// For ephemeral (cache) databases, this is a no-op.
    ///
    /// See: `docs/architecture/STORAGE_DURABILITY_ARCHITECTURE.md` Section 6.3
    pub fn checkpoint(&self) -> StrataResult<()> {
        self.check_not_shutting_down()?;
        if self.persistence_mode == PersistenceMode::Ephemeral || self.follower {
            return Ok(());
        }

        // Flush WAL first to ensure all buffered writes are on disk. If
        // `shutdown_with_deadline` has started between the guard above and
        // this line, the public `flush()` would reject — use it so the
        // in-progress shutdown wins and `checkpoint` fails cleanly rather
        // than racing further MANIFEST writes against shutdown's freeze
        // loop.
        self.flush()?;

        // Drain all in-flight commits so the watermark reflects only fully-
        // applied versions. Using current_version() here is unsafe because
        // allocate_version() bumps the counter before apply_writes() completes,
        // so a concurrent commit's version could be included in the watermark
        // while its storage writes are still in progress (#1710).
        let watermark_txn = self.coordinator.quiesced_version();

        // Collect data from storage
        let data = self.collect_checkpoint_data();

        // Create snapshots directory
        let snapshots_dir = self.data_dir.join("snapshots");
        std::fs::create_dir_all(&snapshots_dir).map_err(StrataError::from)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ =
                std::fs::set_permissions(&snapshots_dir, std::fs::Permissions::from_mode(0o700));
        }

        // Load or create MANIFEST
        let mut manifest = self.load_or_create_manifest()?;

        // Build watermark state from existing MANIFEST if present
        let existing_watermark = {
            let m = manifest.manifest();
            match (m.snapshot_id, m.snapshot_watermark) {
                (Some(sid), Some(wtxn)) => Some(
                    strata_storage::durability::SnapshotWatermark::with_values(sid, TxnId(wtxn), 0),
                ),
                _ => None,
            }
        };

        // Create CheckpointCoordinator with the configured codec and database UUID
        let db_uuid = self.database_uuid;
        let codec_id = self.config.read().storage.codec.clone();
        let codec = strata_storage::durability::get_codec(&codec_id)
            .map_err(|e| StrataError::internal(format!("checkpoint codec: {}", e)))?;
        let mut coordinator = if let Some(wm) = existing_watermark {
            CheckpointCoordinator::with_watermark(snapshots_dir, codec, db_uuid, wm)
                .map_err(|e| StrataError::internal(format!("checkpoint coordinator: {}", e)))?
        } else {
            CheckpointCoordinator::new(snapshots_dir, codec, db_uuid)
                .map_err(|e| StrataError::internal(format!("checkpoint coordinator: {}", e)))?
        };

        // Create the checkpoint
        let info =
            coordinator
                .checkpoint(TxnId(watermark_txn), data)
                .map_err(|e: CheckpointError| {
                    StrataError::internal(format!("checkpoint failed: {}", e))
                })?;

        // Update MANIFEST with snapshot watermark
        manifest
            .set_snapshot_watermark(info.snapshot_id, info.watermark_txn)
            .map_err(|e: ManifestError| {
                StrataError::internal(format!("manifest update failed: {}", e))
            })?;

        info!(
            target: "strata::db",
            snapshot_id = info.snapshot_id,
            watermark_txn = info.watermark_txn.as_u64(),
            "Checkpoint created"
        );

        if let Err(e) = self.prune_snapshots_once() {
            tracing::warn!(
                target: "strata::durability",
                error = %e,
                "Snapshot pruning failed after checkpoint (non-fatal)"
            );
        }

        Ok(())
    }

    /// Compact WAL segments that are no longer needed for recovery.
    ///
    /// Removes closed WAL segments whose max transaction ID is at or below the
    /// latest snapshot watermark. The active segment is never removed.
    ///
    /// A checkpoint must exist before compaction can run. For ephemeral (cache)
    /// databases, this is a no-op.
    ///
    /// See: `docs/architecture/STORAGE_DURABILITY_ARCHITECTURE.md` Section 5.6
    pub fn compact(&self) -> StrataResult<()> {
        self.check_not_shutting_down()?;
        if self.persistence_mode == PersistenceMode::Ephemeral || self.follower {
            return Ok(());
        }

        let wal_dir = self.data_dir.join("wal");

        // Load or create MANIFEST
        let manifest = self.load_or_create_manifest()?;
        let manifest_arc = Arc::new(parking_lot::Mutex::new(manifest));

        // Get the writer's in-memory segment number (may be ahead of MANIFEST)
        let writer_active = self
            .wal_writer
            .as_ref()
            .map(|w| w.lock().current_segment())
            .unwrap_or(0);

        // Create compactor and run with the writer's active segment override.
        // D2 / DG-001: thread the cached `wal_codec` so the `.meta`-miss
        // fallback parses records through the codec-aware reader rather
        // than the raw-byte path that bypasses both the v3 envelope and
        // the installed codec.
        let compactor = WalOnlyCompactor::new(wal_dir, manifest_arc).with_codec(
            strata_storage::durability::codec::clone_codec(self.wal_codec.as_ref()),
        );
        let compact_info = compactor
            .compact_with_active_override(writer_active)
            .map_err(|e: CompactionError| match e {
                CompactionError::NoSnapshot => StrataError::invalid_input(
                    "No checkpoint exists yet. Run checkpoint() before compact().".to_string(),
                ),
                other => StrataError::internal(format!("compaction failed: {}", other)),
            })?;

        info!(
            target: "strata::db",
            segments_removed = compact_info.wal_segments_removed,
            bytes_reclaimed = compact_info.reclaimed_bytes,
            "WAL compaction completed"
        );

        Ok(())
    }

    /// Compute database disk usage across WAL and snapshot directories.
    ///
    /// Returns zeros for ephemeral databases.
    pub fn disk_usage(&self) -> DatabaseDiskUsage {
        if self.persistence_mode == PersistenceMode::Ephemeral {
            return DatabaseDiskUsage::default();
        }

        let wal = self
            .wal_writer
            .as_ref()
            .map(|w| w.lock().wal_disk_usage())
            .unwrap_or_default();

        let snapshots_dir = self.data_dir.join("snapshots");
        let snapshot_bytes = scan_dir_size(&snapshots_dir);

        DatabaseDiskUsage {
            wal,
            snapshot_bytes,
        }
    }

    /// Collect all primitive data from storage for checkpointing.
    pub(super) fn collect_checkpoint_data(&self) -> CheckpointData {
        /// Lightweight deserialization shim for vector records in checkpoint.
        /// The full `VectorRecord` type lives in strata-vector.
        #[allow(dead_code)]
        #[derive(serde::Deserialize)]
        struct VectorRecord {
            vector_id: u64,
            #[serde(default)]
            embedding: Vec<f32>,
            metadata: Option<serde_json::Value>,
            version: u64,
            created_at: u64,
            updated_at: u64,
            #[serde(default)]
            source_ref: Option<strata_core::EntityRef>,
        }
        impl VectorRecord {
            fn from_bytes(data: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
                rmp_serde::from_slice(data)
            }
        }

        let mut kv_entries = Vec::new();
        let mut event_entries = Vec::new();
        let mut branch_entries = Vec::new();
        let mut json_entries = Vec::new();
        let mut vector_collections = Vec::new();

        for branch_id in self.storage.branch_ids() {
            // KV entries — use the version-scoped listing so tombstones survive
            // into the snapshot payload and retention metadata (timestamp,
            // ttl_ms) is preserved. `list_by_type_at_version(…, MAX)` returns
            // every logical key's latest version including deletions.
            for entry in
                self.storage
                    .list_by_type_at_version(&branch_id, TypeTag::KV, CommitVersion::MAX)
            {
                let value_bytes = if entry.is_tombstone {
                    Vec::new()
                } else {
                    serde_json::to_vec(&entry.value).unwrap_or_default()
                };
                kv_entries.push(KvSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: entry.key.namespace.space.clone(),
                    type_tag: TypeTag::KV.as_byte(),
                    user_key: entry.key.user_key.to_vec(),
                    value: value_bytes,
                    version: entry.commit_id.as_u64(),
                    timestamp: entry.timestamp_micros,
                    ttl_ms: entry.ttl_ms,
                    is_tombstone: entry.is_tombstone,
                });
            }

            // Graph entries share the KV section (discriminated by `type_tag`).
            // Same retention-complete treatment.
            for entry in
                self.storage
                    .list_by_type_at_version(&branch_id, TypeTag::Graph, CommitVersion::MAX)
            {
                let value_bytes = if entry.is_tombstone {
                    Vec::new()
                } else {
                    serde_json::to_vec(&entry.value).unwrap_or_default()
                };
                kv_entries.push(KvSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: entry.key.namespace.space.clone(),
                    type_tag: TypeTag::Graph.as_byte(),
                    user_key: entry.key.user_key.to_vec(),
                    value: value_bytes,
                    version: entry.commit_id.as_u64(),
                    timestamp: entry.timestamp_micros,
                    ttl_ms: entry.ttl_ms,
                    is_tombstone: entry.is_tombstone,
                });
            }

            // Event entries — events are append-only, so tombstones should not
            // appear. Stay on the live-only `list_by_type` for clarity; if a
            // future primitive change introduces event retraction, revisit.
            for (key, vv) in self.storage.list_by_type(&branch_id, TypeTag::Event) {
                // Skip metadata keys (internal implementation details, reconstructed on restore)
                if *key.user_key == *b"__meta__" || key.user_key.starts_with(b"__tidx__") {
                    continue;
                }
                let sequence = if key.user_key.len() == 8 {
                    u64::from_be_bytes((*key.user_key).try_into().unwrap_or([0; 8]))
                } else {
                    0
                };
                let payload = serde_json::to_vec(&vv.value).unwrap_or_default();
                event_entries.push(EventSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: key.namespace.space.clone(),
                    sequence,
                    payload,
                    version: vv.version.as_u64(),
                    timestamp: vv.timestamp.as_micros(),
                });
            }

            // Branch entries — retention-complete via version-scoped listing.
            // The DTO now carries `branch_id` explicitly so snapshot install
            // can dispatch. `__idx_` keys are derived indices and are
            // deliberately excluded from snapshots (rebuilt at read time).
            for entry in self.storage.list_by_type_at_version(
                &branch_id,
                TypeTag::Branch,
                CommitVersion::MAX,
            ) {
                if entry.key.user_key.starts_with(b"__idx_") {
                    continue;
                }
                let key_string = match entry.key.user_key_string() {
                    Some(key_string) => key_string,
                    None => continue,
                };
                let value = if entry.is_tombstone {
                    Vec::new()
                } else {
                    serde_json::to_vec(&entry.value).unwrap_or_default()
                };
                branch_entries.push(BranchSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    key: key_string,
                    value,
                    version: entry.commit_id.as_u64(),
                    timestamp: entry.timestamp_micros,
                    is_tombstone: entry.is_tombstone,
                });
            }

            // JSON entries — retention-complete with tombstone preservation.
            for entry in
                self.storage
                    .list_by_type_at_version(&branch_id, TypeTag::Json, CommitVersion::MAX)
            {
                let content = if entry.is_tombstone {
                    Vec::new()
                } else {
                    serde_json::to_vec(&entry.value).unwrap_or_default()
                };
                json_entries.push(JsonSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: entry.key.namespace.space.clone(),
                    doc_id: entry.key.user_key_string().unwrap_or_default(),
                    content,
                    version: entry.commit_id.as_u64(),
                    timestamp: entry.timestamp_micros,
                    is_tombstone: entry.is_tombstone,
                });
            }

            // Vector collection configs (stored under TypeTag::Vector with
            // __config__/ prefix). Per-vector rows get tombstone preservation
            // via the version-scoped listing; config entries don't need a
            // tombstone (collection lifecycle is branch-scoped, not per-row).
            for entry in self.storage.list_by_type_at_version(
                &branch_id,
                TypeTag::Vector,
                CommitVersion::MAX,
            ) {
                if entry.is_tombstone {
                    // Config tombstones indicate the collection was dropped;
                    // ignore here since any trailing vector rows below are
                    // tombstoned too and round-trip correctly on install.
                    continue;
                }
                let user_key_str = match entry.key.user_key_string() {
                    Some(s) => s,
                    None => continue,
                };
                let collection_name = match user_key_str.strip_prefix("__config__/") {
                    Some(name) => name.to_string(),
                    None => continue,
                };

                let config_bytes = match &entry.value {
                    strata_core::value::Value::Bytes(b) => b.clone(),
                    _ => serde_json::to_vec(&entry.value).unwrap_or_default(),
                };

                // Per-collection vector rows — also via version-scoped listing
                // so deleted vectors survive the checkpoint as tombstones.
                let ns = entry.key.namespace.clone();
                let prefix = Key::vector_collection_prefix(ns, &collection_name);
                let mut snapshot_vectors = Vec::new();
                for vec_entry in self.storage.list_by_type_at_version(
                    &branch_id,
                    TypeTag::Vector,
                    CommitVersion::MAX,
                ) {
                    if !vec_entry.key.starts_with(&prefix) {
                        continue;
                    }
                    let vec_key_str = vec_entry.key.user_key_string().unwrap_or_default();
                    let vector_key = vec_key_str
                        .strip_prefix(&format!("{}/", collection_name))
                        .unwrap_or(&vec_key_str)
                        .to_string();

                    if vec_entry.is_tombstone {
                        // Tombstoned vector row — preserve the delete through
                        // checkpoint. Embedding/metadata/raw_value are empty
                        // since the row no longer carries payload.
                        snapshot_vectors.push(VectorSnapshotEntry {
                            key: vector_key,
                            vector_id: 0,
                            embedding: Vec::new(),
                            metadata: Vec::new(),
                            raw_value: Vec::new(),
                            version: vec_entry.commit_id.as_u64(),
                            timestamp: vec_entry.timestamp_micros,
                            is_tombstone: true,
                        });
                        continue;
                    }

                    if let strata_core::value::Value::Bytes(bytes) = &vec_entry.value {
                        if let Ok(record) = VectorRecord::from_bytes(bytes) {
                            let metadata_bytes = record
                                .metadata
                                .as_ref()
                                .and_then(|m| serde_json::to_vec(m).ok())
                                .unwrap_or_default();
                            snapshot_vectors.push(VectorSnapshotEntry {
                                key: vector_key,
                                vector_id: record.vector_id,
                                embedding: record.embedding,
                                metadata: metadata_bytes,
                                raw_value: bytes.clone(),
                                version: vec_entry.commit_id.as_u64(),
                                timestamp: vec_entry.timestamp_micros,
                                is_tombstone: false,
                            });
                        }
                    }
                }

                vector_collections.push(VectorCollectionSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: entry.key.namespace.space.clone(),
                    name: collection_name,
                    config: config_bytes,
                    config_version: entry.commit_id.as_u64(),
                    config_timestamp: entry.timestamp_micros,
                    vectors: snapshot_vectors,
                });
            }
        }

        let mut data = CheckpointData::new();
        if !kv_entries.is_empty() {
            data = data.with_kv(kv_entries);
        }
        if !event_entries.is_empty() {
            data = data.with_events(event_entries);
        }
        if !branch_entries.is_empty() {
            data = data.with_branches(branch_entries);
        }
        if !json_entries.is_empty() {
            data = data.with_json(json_entries);
        }
        if !vector_collections.is_empty() {
            data = data.with_vectors(vector_collections);
        }
        data
    }

    /// Load an existing MANIFEST or create a new one.
    ///
    /// Also updates the active WAL segment from the current WAL writer.
    pub(super) fn load_or_create_manifest(&self) -> StrataResult<ManifestManager> {
        let manifest_path = self.data_dir.join("MANIFEST");

        let mut manifest = if ManifestManager::exists(&manifest_path) {
            ManifestManager::load(manifest_path).map_err(|e: ManifestError| {
                StrataError::internal(format!("failed to load MANIFEST: {}", e))
            })?
        } else {
            ManifestManager::create(manifest_path, self.database_uuid, "identity".to_string())
                .map_err(|e: ManifestError| {
                    StrataError::internal(format!("failed to create MANIFEST: {}", e))
                })?
        };

        // Update active WAL segment from the writer
        if let Some(ref wal) = self.wal_writer {
            let wal = wal.lock();
            let current_seg = wal.current_segment();
            manifest.manifest_mut().active_wal_segment = current_seg;
            manifest.persist().map_err(|e: ManifestError| {
                StrataError::internal(format!("failed to persist MANIFEST: {}", e))
            })?;
        }

        Ok(manifest)
    }
}
