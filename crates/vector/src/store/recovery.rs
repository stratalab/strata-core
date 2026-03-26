//! WAL replay, post-merge reload, and recovery helpers.

use super::*;

impl VectorStore {
    /// Replay collection creation from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL - that would cause infinite loops during replay.
    ///
    /// Called by the global WAL replayer for committed VectorCollectionCreate entries.
    ///
    /// # Config Validation (Issue #452)
    ///
    /// If collection already exists, validates that the config matches.
    /// This catches WAL corruption or conflicting create entries.
    pub fn replay_create_collection(
        &self,
        branch_id: BranchId,
        name: &str,
        config: VectorConfig,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, name);

        // Check if collection already exists in backend
        let state = self.state()?;
        if let Some(existing_backend) = state.backends.get(&collection_id) {
            // Validate config matches (Issue #452)
            let existing_config = existing_backend.config();
            if existing_config.dimension != config.dimension {
                tracing::warn!(
                    target: "strata::vector",
                    collection = name,
                    existing_dim = existing_config.dimension,
                    wal_dim = config.dimension,
                    "Config mismatch during WAL replay: dimension differs"
                );
                return Err(VectorError::DimensionMismatch {
                    expected: existing_config.dimension,
                    got: config.dimension,
                });
            }
            if existing_config.metric != config.metric {
                tracing::warn!(
                    target: "strata::vector",
                    collection = name,
                    existing_metric = ?existing_config.metric,
                    wal_metric = ?config.metric,
                    "Config mismatch during WAL replay: metric differs"
                );
                return Err(VectorError::ConfigMismatch {
                    collection: name.to_string(),
                    field: "metric".to_string(),
                });
            }
            // Collection already exists with matching config - idempotent replay
            return Ok(());
        }

        // Initialize backend (no KV write - KV is replayed separately)
        let backend = self.backend_factory().create(&config);
        state.backends.insert(collection_id, backend);

        Ok(())
    }

    /// Replay collection deletion from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL.
    pub fn replay_delete_collection(&self, branch_id: BranchId, name: &str) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, name);

        // Remove in-memory backend
        let state = self.state()?;
        state.backends.remove(&collection_id);

        Ok(())
    }

    /// Replay vector upsert from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL.
    ///
    /// Uses `insert_with_id_and_timestamp` to maintain VectorId monotonicity
    /// (Invariant T4) and preserve temporal metadata for `search_at()` queries.
    ///
    /// Note: `_key`, `_metadata`, and `_source_ref` parameters are not used here because
    /// they are stored in the KV layer (via VectorRecord), which has its own WAL entries.
    /// This method only replays the embedding into the VectorHeap backend.
    #[allow(clippy::too_many_arguments)]
    pub fn replay_upsert(
        &self,
        branch_id: BranchId,
        collection: &str,
        _key: &str,
        vector_id: VectorId,
        embedding: &[f32],
        _metadata: Option<serde_json::Value>,
        _source_ref: Option<strata_core::EntityRef>,
        created_at: u64,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, collection);

        let state = self.state()?;
        let mut backend = state.backends.get_mut(&collection_id).ok_or_else(|| {
            VectorError::CollectionNotFound {
                name: collection.to_string(),
            }
        })?;

        // Use insert_with_id_and_timestamp to maintain VectorId monotonicity
        // and preserve temporal data for time-travel queries after recovery.
        backend.insert_with_id_and_timestamp(vector_id, embedding, created_at)?;

        Ok(())
    }

    /// Replay vector deletion from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL.
    ///
    /// Passes the original deletion timestamp to preserve temporal metadata
    /// for `search_at()` queries after recovery.
    pub fn replay_delete(
        &self,
        branch_id: BranchId,
        collection: &str,
        _key: &str,
        vector_id: VectorId,
        deleted_at: u64,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, collection);

        let state = self.state()?;
        if let Some(mut backend) = state.backends.get_mut(&collection_id) {
            backend.delete_with_timestamp(vector_id, deleted_at)?;
        }
        // Note: If collection doesn't exist, that's OK - it may have been deleted

        Ok(())
    }

    /// Reload all vector backends for a branch from KV state.
    ///
    /// Called after `merge_branches()` to ensure in-memory vector backends
    /// reflect the merged KV state. Without this, vectors merged at the KV
    /// level would be invisible to search until the next full recovery.
    ///
    /// For each vector collection in the target branch (across all spaces):
    /// 1. Creates a fresh backend from the factory
    /// 2. Scans all VectorRecords from KV (including newly merged ones)
    /// 3. Allocates new VectorIds to avoid collisions from independent branches
    /// 4. Updates KV records with the new VectorIds for consistency
    /// 5. Rebuilds the SegmentedHnswBackend (segments via `rebuild_index()`)
    /// 6. Replaces the existing in-memory backend
    ///
    /// ## VectorId Remapping
    ///
    /// When branches are created independently, each branch allocates VectorIds
    /// starting from 0. After merge, the target branch may contain VectorRecords
    /// with colliding IDs from different branches. This method allocates fresh
    /// IDs from the new backend's counter and updates KV records to match,
    /// ensuring the VectorId link between KV and backend is consistent.
    ///
    /// Collections on other branches are left untouched.
    pub fn post_merge_reload_vectors(&self, branch_id: BranchId) -> VectorResult<()> {
        self.post_merge_reload_vectors_from(branch_id, None)
    }

    /// Reload vector backends for a branch, optionally reading embeddings
    /// from a source branch's backend when KV records are lite.
    pub fn post_merge_reload_vectors_from(
        &self,
        branch_id: BranchId,
        source_branch_id: Option<BranchId>,
    ) -> VectorResult<()> {
        use strata_core::traits::Storage;

        let state = self.state()?;
        let factory = self.backend_factory();
        let version = self.db.storage().version();

        // Get all spaces for this branch (SpaceIndex.list always includes "default")
        let space_index = strata_engine::SpaceIndex::new(self.db.clone());
        let spaces = space_index
            .list(branch_id)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let mut total_collections = 0usize;
        let mut total_vectors = 0usize;

        for space in &spaces {
            let ns = self.namespace_for(branch_id, space);

            // Scan for vector config entries in this space
            let config_prefix = Key::new_vector_config_prefix(ns.clone());
            let config_entries = match self.db.storage().scan_prefix(&config_prefix, version) {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::warn!(
                        target: "strata::vector",
                        branch_id = %branch_id,
                        space = %space,
                        error = %e,
                        "Failed to scan vector configs during post-merge reload"
                    );
                    continue;
                }
            };

            for (key, versioned) in &config_entries {
                // Parse collection config (same pattern as recovery.rs)
                let config_bytes = match &versioned.value {
                    Value::Bytes(b) => b,
                    _ => continue,
                };

                let record = match CollectionRecord::from_bytes(config_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            key = ?key,
                            error = %e,
                            "Failed to decode collection record during post-merge reload, skipping"
                        );
                        continue;
                    }
                };

                let collection_name = match key.user_key_string() {
                    Some(raw) => raw.strip_prefix("__config__/").unwrap_or(&raw).to_string(),
                    None => continue,
                };

                let config: VectorConfig = match record.config.try_into() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to convert collection config during post-merge reload, skipping"
                        );
                        continue;
                    }
                };

                let collection_id = CollectionId::new(branch_id, &collection_name);

                // Grab the old backend (if any) so we can read embeddings
                // from it for legacy records that don't store them in KV.
                let old_backend = state.backends.remove(&collection_id).map(|(_, v)| v);

                // If we have a source branch, build a map from user_key → VectorId
                // for the source collection. This lets us determine which backend
                // to read from when VectorIds collide between source and target.
                let source_key_to_vid: BTreeMap<Vec<u8>, u64> = if let Some(src_bid) =
                    source_branch_id
                {
                    let src_ns = Arc::new(Namespace::for_branch_space(src_bid, space));
                    let src_prefix = Key::new_vector(src_ns, &collection_name, "");
                    if let Ok(src_entries) = self.db.storage().scan_prefix(&src_prefix, version) {
                        src_entries
                            .iter()
                            .filter_map(|(k, vv)| {
                                if let Value::Bytes(b) = &vv.value {
                                    VectorRecord::from_bytes(b)
                                        .ok()
                                        .map(|r| (k.user_key.to_vec(), r.vector_id))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        BTreeMap::new()
                    }
                } else {
                    BTreeMap::new()
                };

                // Create fresh backend
                let mut backend = factory.create(&config);

                // Scan all vector entries in this collection
                let vector_prefix = Key::new_vector(ns.clone(), &collection_name, "");
                let vector_entries = match self.db.storage().scan_prefix(&vector_prefix, version) {
                    Ok(entries) => entries,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to scan vectors during post-merge reload"
                        );
                        continue;
                    }
                };

                // Collect vectors that need VectorId remapping, then batch-update KV
                let mut kv_updates: Vec<(Key, Vec<u8>)> = Vec::new();
                let mut collection_vector_count = 0usize;
                let mut kv_remap_failed = false;

                for (vec_key, vec_versioned) in &vector_entries {
                    let vec_bytes = match &vec_versioned.value {
                        Value::Bytes(b) => b,
                        _ => continue,
                    };

                    let mut vec_record = match VectorRecord::from_bytes(vec_bytes) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                target: "strata::vector",
                                error = %e,
                                "Failed to decode vector record during post-merge reload, skipping"
                            );
                            continue;
                        }
                    };

                    // Resolve the embedding: use KV record if present, else
                    // fall back to the appropriate backend based on provenance.
                    let embedding = if !vec_record.embedding.is_empty() {
                        vec_record.embedding.clone()
                    } else {
                        let old_vid = VectorId(vec_record.vector_id);

                        // Determine provenance: did this key come from the source branch?
                        // Check by matching user_key + VectorId against source KV scan.
                        let is_from_source = source_key_to_vid
                            .get(&*vec_key.user_key)
                            .is_some_and(|&src_vid| src_vid == vec_record.vector_id);

                        if is_from_source {
                            if let Some(src_bid) = source_branch_id {
                                let src_cid = CollectionId::new(src_bid, &collection_name);
                                let src_emb = state
                                    .backends
                                    .get(&src_cid)
                                    .and_then(|b| b.get(old_vid).map(|e| e.to_vec()));
                                match src_emb {
                                    Some(emb) => emb,
                                    None => match old_backend.as_ref().and_then(|b| b.get(old_vid))
                                    {
                                        Some(emb) => emb.to_vec(),
                                        None => {
                                            tracing::warn!(
                                                target: "strata::vector",
                                                vector_id = vec_record.vector_id,
                                                "Empty-embedding record: embedding not found, skipping"
                                            );
                                            continue;
                                        }
                                    },
                                }
                            } else {
                                match old_backend.as_ref().and_then(|b| b.get(old_vid)) {
                                    Some(emb) => emb.to_vec(),
                                    None => {
                                        tracing::warn!(
                                            target: "strata::vector",
                                            vector_id = vec_record.vector_id,
                                            "Empty-embedding record: backend embedding not found, skipping"
                                        );
                                        continue;
                                    }
                                }
                            }
                        } else {
                            // Target-originated: try old target backend first, then source
                            match old_backend.as_ref().and_then(|b| b.get(old_vid)) {
                                Some(emb) => emb.to_vec(),
                                None => {
                                    if let Some(src_bid) = source_branch_id {
                                        let src_cid = CollectionId::new(src_bid, &collection_name);
                                        let src_emb = state
                                            .backends
                                            .get(&src_cid)
                                            .and_then(|b| b.get(old_vid).map(|e| e.to_vec()));
                                        match src_emb {
                                            Some(emb) => emb,
                                            None => {
                                                tracing::warn!(
                                                    target: "strata::vector",
                                                    vector_id = vec_record.vector_id,
                                                    "Empty-embedding record: embedding not found, skipping"
                                                );
                                                continue;
                                            }
                                        }
                                    } else {
                                        tracing::warn!(
                                            target: "strata::vector",
                                            vector_id = vec_record.vector_id,
                                            "Empty-embedding record: old backend embedding not found, skipping"
                                        );
                                        continue;
                                    }
                                }
                            }
                        }
                    };

                    // Allocate a fresh VectorId to avoid collisions between
                    // independently-created branches that share the same ID space.
                    let new_vid = backend.allocate_id();
                    let _ =
                        backend.insert_with_timestamp(new_vid, &embedding, vec_record.created_at);

                    // If the VectorId changed, queue a KV update so that
                    // get() can resolve the correct backend entry.
                    if new_vid.as_u64() != vec_record.vector_id {
                        vec_record.vector_id = new_vid.as_u64();
                        match vec_record.to_bytes() {
                            Ok(updated_bytes) => {
                                kv_updates.push((vec_key.clone(), updated_bytes));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "strata::vector",
                                    error = %e,
                                    "Failed to serialize remapped VectorRecord, skipping collection"
                                );
                                kv_remap_failed = true;
                                break;
                            }
                        }
                    }

                    collection_vector_count += 1;
                }

                // If serialization failed mid-way, skip this collection entirely.
                // The old backend (if any) remains; vectors will be properly loaded
                // on next full recovery.
                if kv_remap_failed {
                    continue;
                }

                // Batch-write remapped VectorIds back to KV.
                // CRITICAL: If this fails, do NOT install the backend — the
                // VectorIds in KV and backend would be mismatched, causing
                // get() to return wrong embeddings silently.
                if !kv_updates.is_empty() {
                    if let Err(e) = self.db.transaction(branch_id, |txn| {
                        for (k, bytes) in &kv_updates {
                            txn.put(k.clone(), Value::Bytes(bytes.clone()))?;
                        }
                        Ok(())
                    }) {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to update VectorIds in KV after merge reload, \
                             skipping collection to prevent data inconsistency"
                        );
                        continue;
                    }
                }

                // Rebuild index (segments for SegmentedHnsw, graph for HNSW)
                backend.rebuild_index();

                // Replace existing backend atomically
                state.backends.insert(collection_id, backend);

                total_collections += 1;
                total_vectors += collection_vector_count;
            }
        }

        if total_collections > 0 {
            info!(
                target: "strata::vector",
                branch_id = %branch_id,
                total_collections,
                total_vectors,
                "Post-merge vector reload complete"
            );
        }

        Ok(())
    }

    /// Get index type name and memory usage for a collection
    pub fn collection_backend_stats(
        &self,
        branch_id: BranchId,
        _space: &str,
        name: &str,
    ) -> Option<(&'static str, usize)> {
        let collection_id = CollectionId::new(branch_id, name);
        let state = self.state().ok()?;
        state
            .backends
            .get(&collection_id)
            .map(|b| (b.index_type_name(), b.memory_usage()))
    }

    /// Get access to the shared backend state (for recovery/snapshot)
    pub(crate) fn backends(&self) -> Result<Arc<VectorBackendState>, VectorError> {
        self.state()
    }

    /// Get access to the database (for snapshot operations)
    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    /// Internal helper to create vector KV key
    pub(crate) fn vector_key_internal(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> Key {
        Key::new_vector(self.namespace_for(branch_id, space), collection, key)
    }
}
