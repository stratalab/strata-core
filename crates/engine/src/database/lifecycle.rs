//! GC, maintenance, follower refresh, and shutdown.

use std::sync::atomic::Ordering;
use strata_core::types::{BranchId, Key, TypeTag};
use strata_core::{StrataError, StrataResult};
use tracing::warn;

use super::{Database, PersistenceMode};

impl Database {
    // Branch Lifecycle Cleanup
    // ========================================================================

    /// Garbage-collect old versions before the given version number.
    ///
    /// Removes old versions from version chains across all entries in the branch.
    /// Returns the number of pruned versions.
    pub fn gc_versions_before(&self, branch_id: BranchId, min_version: u64) -> usize {
        self.storage.gc_branch(&branch_id, min_version)
    }

    /// Get the current global version from the coordinator.
    ///
    /// This is the highest version allocated so far and serves as
    /// a safe GC boundary when no active snapshots need older versions.
    pub fn current_version(&self) -> u64 {
        self.coordinator.current_version()
    }

    /// Compute the GC safe point — the oldest version that can be pruned.
    ///
    /// `safe_point = min(current_version - 1, min_active_version)`
    ///
    /// Versions strictly older than `safe_point` can be pruned from version
    /// chains. Returns 0 if no GC is safe (only one version or version 0).
    pub fn gc_safe_point(&self) -> u64 {
        let current = self.current_version();
        if current <= 1 {
            return 0;
        }
        let version_bound = current - 1;
        match self.coordinator.min_active_version() {
            Some(min_active) => version_bound.min(min_active),
            None => {
                // No drain has occurred yet (gc_safe_version == 0 sentinel).
                // If there are active transactions, they may hold snapshots
                // at any version — block GC entirely until the first drain.
                if self.coordinator.active_count() > 0 {
                    return 0;
                }
                version_bound
            }
        }
    }

    /// Run garbage collection across all branches using the computed safe point.
    ///
    /// This is NOT called automatically — it must be invoked explicitly.
    /// A future PR will add a background maintenance thread.
    ///
    /// Returns `(safe_point, total_versions_pruned)`.
    pub fn run_gc(&self) -> (u64, usize) {
        let safe_point = self.gc_safe_point();
        if safe_point == 0 {
            return (0, 0);
        }

        let mut total_pruned = 0;
        for branch_id in self.storage.branch_ids() {
            total_pruned += self.storage.gc_branch(&branch_id, safe_point);
        }

        if total_pruned > 0 {
            tracing::debug!(pruned = total_pruned, safe_point, "GC cycle complete");
        }

        (safe_point, total_pruned)
    }

    /// Run a full maintenance cycle: GC + TTL expiration.
    ///
    /// This is NOT called automatically. It must be invoked explicitly
    /// (e.g., from tests, CLI, or a future background thread).
    ///
    /// Returns `(safe_point, versions_pruned, ttl_entries_expired)`.
    pub fn run_maintenance(&self) -> (u64, usize, usize) {
        let (safe_point, pruned) = self.run_gc();
        let expired = self
            .storage
            .expire_ttl_keys(strata_core::Timestamp::now().as_micros());
        (safe_point, pruned, expired)
    }

    /// Remove the per-branch commit lock after a branch is deleted.
    ///
    /// This prevents unbounded growth of the commit_locks map in the
    /// TransactionManager when branches are repeatedly created and deleted.
    /// Returns `true` if removed (or didn't exist), `false` if skipped
    /// because a concurrent commit is in-flight.
    ///
    /// Should be called after `BranchIndex::delete_branch()` succeeds.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) -> bool {
        self.coordinator.remove_branch_lock(branch_id)
    }

    // ========================================================================
    // Follower Refresh
    // ========================================================================

    /// Refresh local storage by tailing new WAL entries from the primary.
    ///
    /// In follower mode, the primary process may have appended new WAL records
    /// since this instance last read the WAL. `refresh()` reads any new records
    /// and applies them to local in-memory storage, bringing this instance
    /// up-to-date with all committed writes.
    ///
    /// Returns the number of new records applied.
    ///
    /// For non-follower databases, this is a no-op returning 0.
    pub fn refresh(&self) -> StrataResult<usize> {
        if !self.follower || self.persistence_mode == PersistenceMode::Ephemeral {
            return Ok(0);
        }

        let watermark = self.wal_watermark.load(std::sync::atomic::Ordering::SeqCst);

        // Read all WAL records after our watermark
        let reader = strata_durability::wal::WalReader::new();
        let records = reader
            .read_all_after_watermark(&self.wal_dir, watermark)
            .map_err(|e| StrataError::storage(format!("WAL refresh read failed: {}", e)))?;

        if records.is_empty() {
            return Ok(0);
        }

        let mut applied = 0usize;
        let mut max_version = 0u64;
        let mut max_txn_id = watermark;

        // Get search index (if available) for incremental updates
        let search_index = self.extension::<crate::search::InvertedIndex>().ok();
        let search_enabled = search_index.as_ref().is_some_and(|idx| idx.is_enabled());

        // Get vector backend state (if available) for incremental updates
        let vector_state = self
            .extension::<crate::primitives::vector::store::VectorBackendState>()
            .ok();

        for record in &records {
            max_txn_id = max_txn_id.max(record.txn_id);

            let payload = strata_concurrency::TransactionPayload::from_bytes(&record.writeset)
                .map_err(|e| {
                    StrataError::storage(format!(
                        "Failed to decode WAL payload for txn {}: {}",
                        record.txn_id, e
                    ))
                })?;

            max_version = max_version.max(payload.version);

            // Pre-read vector records for deletes (must happen before storage mutations)
            let mut vector_delete_pre_reads: Vec<(
                Key,
                crate::primitives::vector::types::VectorRecord,
            )> = Vec::new();
            if vector_state.is_some() {
                for key in &payload.deletes {
                    if key.type_tag == TypeTag::Vector {
                        use strata_core::traits::Storage;
                        if let Ok(Some(vv)) = self.storage.get_versioned(key, u64::MAX) {
                            if let strata_core::value::Value::Bytes(ref bytes) = vv.value {
                                if let Ok(record) =
                                    crate::primitives::vector::types::VectorRecord::from_bytes(
                                        bytes,
                                    )
                                {
                                    vector_delete_pre_reads.push((key.clone(), record));
                                }
                            }
                        }
                    }
                }
            }

            // Apply puts and deletes atomically with original WAL timestamp
            // and TTL. Combines #1699 (preserve commit timestamp for time-travel),
            // #1707 (defer version bump until all entries installed),
            // #1734 (defer version bump until secondary indexes are updated),
            // and #1740 (preserve TTL through WAL replay).
            {
                let writes: Vec<_> = payload
                    .puts
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let deletes: Vec<_> = payload.deletes.to_vec();
                self.storage.apply_recovery_atomic(
                    writes,
                    deletes,
                    payload.version,
                    record.timestamp,
                    &payload.put_ttls,
                )?;
            }

            // --- Update BM25 search index ---
            if search_enabled {
                let index = search_index.as_ref().unwrap();

                for (key, value) in &payload.puts {
                    let branch_id = key.namespace.branch_id;
                    match key.type_tag {
                        TypeTag::KV => {
                            if let Some(text) = crate::search::extract_indexable_text(value) {
                                if let Some(user_key) = key.user_key_string() {
                                    let entity_ref = strata_core::EntityRef::Kv {
                                        branch_id,
                                        key: user_key,
                                    };
                                    index.index_document(&entity_ref, &text, None);
                                }
                            }
                        }
                        TypeTag::Event => {
                            if *key.user_key == *b"__meta__"
                                || key.user_key.starts_with(b"__tidx__")
                            {
                                continue;
                            }
                            if let Some(text) = crate::search::extract_indexable_text(value) {
                                if key.user_key.len() == 8 {
                                    let sequence = u64::from_be_bytes(
                                        (*key.user_key).try_into().unwrap_or([0; 8]),
                                    );
                                    let entity_ref = strata_core::EntityRef::Event {
                                        branch_id,
                                        sequence,
                                    };
                                    index.index_document(&entity_ref, &text, None);
                                }
                            }
                        }
                        _ => {}
                    }
                }

                for key in &payload.deletes {
                    let branch_id = key.namespace.branch_id;
                    match key.type_tag {
                        TypeTag::KV => {
                            if let Some(user_key) = key.user_key_string() {
                                let entity_ref = strata_core::EntityRef::Kv {
                                    branch_id,
                                    key: user_key,
                                };
                                index.remove_document(&entity_ref);
                            }
                        }
                        TypeTag::Event => {
                            if *key.user_key == *b"__meta__"
                                || key.user_key.starts_with(b"__tidx__")
                            {
                                continue;
                            }
                            if key.user_key.len() == 8 {
                                let sequence = u64::from_be_bytes(
                                    (*key.user_key).try_into().unwrap_or([0; 8]),
                                );
                                let branch_id = key.namespace.branch_id;
                                let entity_ref = strata_core::EntityRef::Event {
                                    branch_id,
                                    sequence,
                                };
                                index.remove_document(&entity_ref);
                            }
                        }
                        _ => {}
                    }
                }
            }

            // --- Update vector backends ---
            if let Some(ref vs) = vector_state {
                use strata_core::primitives::vector::{CollectionId, VectorId};

                // Vector puts: insert into backend
                for (key, value) in &payload.puts {
                    if key.type_tag != TypeTag::Vector {
                        continue;
                    }
                    let bytes = match value {
                        strata_core::value::Value::Bytes(b) => b,
                        _ => continue,
                    };
                    let record =
                        match crate::primitives::vector::types::VectorRecord::from_bytes(bytes) {
                            Ok(r) => r,
                            Err(_) => continue,
                        };
                    let user_key_str = match key.user_key_string() {
                        Some(s) => s,
                        None => continue,
                    };
                    let (collection, vector_key) = match user_key_str.split_once('/') {
                        Some(pair) => pair,
                        None => continue,
                    };
                    let branch_id = key.namespace.branch_id;
                    let cid = CollectionId::new(branch_id, collection);
                    let vid = VectorId::new(record.vector_id);

                    if let Some(mut backend) = vs.backends.get_mut(&cid) {
                        if let Err(e) =
                            backend.insert_with_timestamp(vid, &record.embedding, record.created_at)
                        {
                            tracing::warn!(
                                target: "strata::refresh",
                                collection = collection,
                                vector_key = vector_key,
                                error = %e,
                                "Vector insert failed during refresh"
                            );
                        }
                        backend.set_inline_meta(
                            vid,
                            crate::primitives::vector::types::InlineMeta {
                                key: vector_key.to_string(),
                                source_ref: record.source_ref.clone(),
                            },
                        );
                    } else {
                        tracing::debug!(
                            target: "strata::refresh",
                            collection = collection,
                            "Skipping vector insert for unknown collection (will be picked up on restart)"
                        );
                    }
                }

                // Vector deletes: remove from backend using pre-reads
                for (key, record) in &vector_delete_pre_reads {
                    let user_key_str = match key.user_key_string() {
                        Some(s) => s,
                        None => continue,
                    };
                    let collection = match user_key_str.split_once('/') {
                        Some((coll, _)) => coll,
                        None => continue,
                    };
                    let branch_id = key.namespace.branch_id;
                    let cid = CollectionId::new(branch_id, collection);
                    let vid = VectorId::new(record.vector_id);

                    if let Some(mut backend) = vs.backends.get_mut(&cid) {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as u64)
                            .unwrap_or(0);
                        if let Err(e) = backend.delete_with_timestamp(vid, now) {
                            tracing::warn!(
                                target: "strata::refresh",
                                collection = collection,
                                error = %e,
                                "Vector delete failed during refresh"
                            );
                        }
                        backend.remove_inline_meta(vid);
                    }
                }
            }

            // Issue #1734: Advance visible version AFTER secondary indexes are
            // updated, so readers never see KV data without corresponding
            // BM25/HNSW entries.
            self.storage.advance_version(payload.version);

            applied += 1;
        }

        // Advance local counters so new transactions get unique IDs/versions
        self.coordinator.catch_up_version(max_version);
        self.coordinator.catch_up_txn_id(max_txn_id);

        // Update watermark
        self.wal_watermark
            .store(max_txn_id, std::sync::atomic::Ordering::SeqCst);

        Ok(applied)
    }

    // Graceful Shutdown
    // ========================================================================

    /// Graceful shutdown - ensures all data is persisted
    ///
    /// Freeze all vector heaps to mmap files for crash-safe recovery.
    ///
    /// With lite KV records (embedding stripped), the mmap cache is required
    /// for the next recovery to reconstruct embeddings. This is called during
    /// shutdown and drop.
    pub(super) fn freeze_vector_heaps(&self) -> StrataResult<()> {
        use crate::primitives::vector::VectorBackendState;

        let data_dir = self.data_dir();
        if data_dir.as_os_str().is_empty() {
            return Ok(()); // Ephemeral database — no mmap
        }

        let state = match self.extension::<VectorBackendState>() {
            Ok(s) => s,
            Err(_) => return Ok(()), // No vector state registered
        };

        for entry in state.backends.iter() {
            let (cid, backend) = (entry.key(), entry.value());
            let branch_hex = format!("{:032x}", u128::from_be_bytes(*cid.branch_id.as_bytes()));
            let vec_path = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}.vec", cid.name));
            backend.freeze_heap_to_disk(&vec_path)?;

            // Also freeze graphs
            let gdir = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}_graphs", cid.name));
            backend.freeze_graphs_to_disk(&gdir)?;
        }
        Ok(())
    }

    /// Freeze the search index to disk for fast recovery on next open.
    pub(super) fn freeze_search_index(&self) -> StrataResult<()> {
        let data_dir = self.data_dir();
        if data_dir.as_os_str().is_empty() {
            return Ok(()); // Ephemeral database — no persistence
        }

        if let Ok(index) = self.extension::<crate::search::InvertedIndex>() {
            index.freeze_to_disk()?;
        }
        Ok(())
    }

    /// This method:
    /// 1. Stops accepting new transactions
    /// 2. Waits for pending operations to complete
    /// 3. Flushes WAL based on durability mode
    ///
    /// # Example
    ///
    /// ```text
    /// db.shutdown()?;
    /// assert!(!db.is_open());
    /// ```
    pub fn shutdown(&self) -> StrataResult<()> {
        // 1. Stop accepting new transactions
        self.accepting_transactions.store(false, Ordering::Release);

        // Followers have no background threads, WAL writer, or freeze targets.
        if self.follower {
            return Ok(());
        }

        // 2. Drain background tasks (embeddings etc.) before final WAL flush
        self.scheduler.drain();

        // 3. Wait for in-flight transactions to complete FIRST.
        //    Uses wait_for_idle() which polls with SeqCst ordering, ensuring
        //    visibility of active_count increments from record_start() on all
        //    architectures (fixes #1738 / 9.2.B).
        let timeout = std::time::Duration::from_secs(30);
        if !self.coordinator.wait_for_idle(timeout) {
            let remaining = self.coordinator.active_count();
            warn!(
                target: "strata::db",
                remaining,
                "Shutdown timed out waiting for {} active transaction(s) after {:?}",
                remaining,
                timeout,
            );
        }

        // 4. Signal the background flush thread to stop (after transactions are drained)
        self.flush_shutdown.store(true, Ordering::SeqCst);

        // Join the flush thread so it releases the WAL lock
        // (E-5: the thread performs a final sync before exiting)
        if let Some(handle) = self.flush_handle.lock().take() {
            let _ = handle.join();
        }

        // 5. Final flush to ensure all data is persisted
        self.flush()?;

        // 6. Freeze both vector heaps and search index. Attempt both even if
        // the first fails, so a vector freeze error doesn't also lose search data.
        let vec_result = self.freeze_vector_heaps();
        let search_result = self.freeze_search_index();
        self.shutdown_complete.store(true, Ordering::Release);
        vec_result?;
        search_result?;

        Ok(())
    }
}
