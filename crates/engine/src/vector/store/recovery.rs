//! Post-merge reload and recovery helpers.

use super::*;

impl VectorStore {
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
    ///
    /// This is the **full-branch** reload path used by tests and as a
    /// fallback for callers that don't know which collections were touched.
    /// The merge dispatch path (`VectorMergeHandler::post_commit`) calls
    /// `rebuild_collection_after_merge` per affected collection instead, so
    /// untouched collections aren't rebuilt.
    pub fn post_merge_reload_vectors(&self, branch_id: BranchId) -> VectorResult<()> {
        self.post_merge_reload_vectors_from(branch_id, None)
    }

    /// Reload vector backends for a branch, optionally reading embeddings
    /// from a source branch's backend when KV records are lite.
    ///
    /// Iterates every space and every collection in the branch and calls
    /// `rebuild_collection_after_merge` for each. Used by tests and as a
    /// fallback for callers that don't have a per-collection affected set.
    pub fn post_merge_reload_vectors_from(
        &self,
        branch_id: BranchId,
        source_branch_id: Option<BranchId>,
    ) -> VectorResult<()> {
        let version = CommitVersion(self.db.storage().version());

        // Get all spaces for this branch (SpaceIndex.list always includes "default")
        let space_index = crate::SpaceIndex::new(self.db.clone());
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

            for (key, _versioned) in &config_entries {
                let collection_name = match key.user_key_string() {
                    Some(raw) => raw.strip_prefix("__config__/").unwrap_or(&raw).to_string(),
                    None => continue,
                };

                match self.rebuild_collection_after_merge(
                    branch_id,
                    source_branch_id,
                    space,
                    &collection_name,
                ) {
                    Ok(vector_count) => {
                        total_collections += 1;
                        total_vectors += vector_count;
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to rebuild collection during post-merge reload"
                        );
                    }
                }
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

    /// Rebuild a single collection's HNSW backend from KV state.
    ///
    /// Called by `VectorMergeHandler::post_commit` for each `(space, collection)`
    /// pair touched by a merge, so untouched collections in the same branch
    /// are left alone (no full-branch sweep).
    ///
    /// Returns the number of vectors loaded into the rebuilt backend.
    /// Returns `Ok(0)` (with no backend swap) if the collection's config row
    /// is missing — the merge may have deleted it.
    ///
    /// Per-collection responsibilities:
    /// 1. Read the `__config__/{name}` KV row to recover the dimension/metric/backend type.
    /// 2. Build a fresh backend via the factory.
    /// 3. Scan every vector KV row under `{name}/`.
    /// 4. Resolve the embedding (KV record, source backend, or old target backend
    ///    for legacy lite records).
    /// 5. Allocate fresh `VectorId`s to avoid cross-branch collisions.
    /// 6. Write remapped `VectorId`s back to KV in one transaction.
    /// 7. Call `rebuild_index()` to materialize the HNSW graph.
    /// 8. Atomically swap the backend in the shared `VectorBackendState`.
    ///
    /// If KV write-back fails, the old backend is preserved and the rebuild
    /// is skipped (returns an error). Callers should log and continue;
    /// rebuilding the next merge or a full recovery will catch up.
    pub fn rebuild_collection_after_merge(
        &self,
        branch_id: BranchId,
        source_branch_id: Option<BranchId>,
        space: &str,
        collection_name: &str,
    ) -> VectorResult<usize> {
        let state = self.state()?;
        let version = CommitVersion(self.db.storage().version());
        let ns = self.namespace_for(branch_id, space);

        // ----------------------------------------------------------------
        // 1. Read collection config from KV
        // ----------------------------------------------------------------
        let config_key = Key::new_vector_config(ns.clone(), collection_name);
        let config_versioned = self
            .db
            .storage()
            .get_versioned(&config_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let config_bytes = match config_versioned.as_ref().map(|vv| &vv.value) {
            Some(Value::Bytes(b)) => b.clone(),
            _ => {
                // Config missing — collection was deleted by the merge.
                // Drop the in-memory backend if any (so search doesn't return
                // stale results) and return.
                let collection_id = CollectionId::new(branch_id, space, collection_name);
                state.backends.remove(&collection_id);
                return Ok(0);
            }
        };

        let record = CollectionRecord::from_bytes(&config_bytes).map_err(|e| {
            VectorError::Storage(format!(
                "Failed to decode collection record for '{}': {}",
                collection_name, e
            ))
        })?;

        let backend_type = record.backend_type();
        let config: VectorConfig = record
            .config
            .try_into()
            .map_err(|e: VectorError| VectorError::Storage(e.to_string()))?;

        let collection_id = CollectionId::new(branch_id, space, collection_name);

        // ----------------------------------------------------------------
        // 2. Take ownership of the old backend (so we can read embeddings
        //    from it for legacy records) and build a fresh one.
        // ----------------------------------------------------------------
        let old_backend = state.backends.remove(&collection_id).map(|(_, v)| v);

        // If we have a source branch, build a map from user_key → VectorId
        // for the source collection. This lets us determine which backend
        // to read from when VectorIds collide between source and target.
        let source_key_to_vid: BTreeMap<Vec<u8>, u64> = if let Some(src_bid) = source_branch_id {
            let src_ns = Arc::new(Namespace::for_branch_space(src_bid, space));
            let src_prefix = Key::new_vector(src_ns, collection_name, "");
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

        let factory = crate::IndexBackendFactory::from_type(backend_type);
        let mut backend = factory.create(&config);

        // ----------------------------------------------------------------
        // 3. Scan all vector entries in this collection
        //
        // If the scan fails, restore the old backend so search keeps
        // returning the pre-merge state — installing nothing would
        // silently empty the collection's in-memory index.
        // ----------------------------------------------------------------
        let vector_prefix = Key::new_vector(ns.clone(), collection_name, "");
        let vector_entries = match self.db.storage().scan_prefix(&vector_prefix, version) {
            Ok(entries) => entries,
            Err(e) => {
                if let Some(b) = old_backend {
                    state.backends.insert(collection_id, b);
                }
                return Err(VectorError::Storage(format!(
                    "Failed to scan vectors for '{}': {}",
                    collection_name, e
                )));
            }
        };

        // ----------------------------------------------------------------
        // 4. Resolve embeddings + allocate fresh VectorIds
        // ----------------------------------------------------------------
        let mut kv_updates: Vec<(Key, Vec<u8>)> = Vec::new();
        let mut collection_vector_count = 0usize;

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
                        "Failed to decode vector record during rebuild, skipping"
                    );
                    continue;
                }
            };

            // Resolve the embedding: use KV record if present, else fall
            // back to the appropriate backend based on provenance.
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
                        let src_cid = CollectionId::new(src_bid, space, collection_name);
                        let src_emb = state
                            .backends
                            .get(&src_cid)
                            .and_then(|b| b.get(old_vid).map(|e| e.to_vec()));
                        match src_emb {
                            Some(emb) => emb,
                            None => match old_backend.as_ref().and_then(|b| b.get(old_vid)) {
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
                                let src_cid = CollectionId::new(src_bid, space, collection_name);
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
            let _ = backend.insert_with_timestamp(new_vid, &embedding, vec_record.created_at);

            // Populate inline metadata so post-merge search resolves the
            // record without falling back to O(N) KV scans (Issue #1965).
            let collection_prefix = format!("{}/", collection_name);
            let vector_user_key = String::from_utf8(vec_key.user_key.to_vec())
                .ok()
                .and_then(|uk| uk.strip_prefix(&collection_prefix).map(|s| s.to_string()))
                .unwrap_or_default();
            backend.set_inline_meta(
                new_vid,
                crate::vector::types::InlineMeta {
                    key: vector_user_key,
                    source_ref: vec_record.source_ref.clone(),
                },
            );

            // If the VectorId changed, queue a KV update so that
            // get() can resolve the correct backend entry.
            if new_vid.as_u64() != vec_record.vector_id {
                vec_record.vector_id = new_vid.as_u64();
                match vec_record.to_bytes() {
                    Ok(updated_bytes) => {
                        kv_updates.push((vec_key.clone(), updated_bytes));
                    }
                    Err(e) => {
                        // Re-install the old backend so search doesn't lose
                        // the collection entirely, and surface the error.
                        if let Some(b) = old_backend {
                            state.backends.insert(collection_id, b);
                        }
                        return Err(VectorError::Storage(format!(
                            "Failed to serialize remapped VectorRecord for '{}': {}",
                            collection_name, e
                        )));
                    }
                }
            }

            collection_vector_count += 1;
        }

        // ----------------------------------------------------------------
        // 5. Batch-write remapped VectorIds back to KV.
        //
        // CRITICAL: If this fails, restore the old backend (so search keeps
        // returning the pre-merge state) and surface the error. Installing
        // the new backend after a failed write would leave KV and backend
        // VectorIds mismatched, causing get() to return wrong embeddings.
        // ----------------------------------------------------------------
        if !kv_updates.is_empty() {
            if let Err(e) = self.db.transaction(branch_id, |txn| {
                for (k, bytes) in &kv_updates {
                    txn.put(k.clone(), Value::Bytes(bytes.clone()))?;
                }
                Ok(())
            }) {
                if let Some(b) = old_backend {
                    state.backends.insert(collection_id, b);
                }
                return Err(VectorError::Storage(format!(
                    "Failed to write remapped VectorIds for '{}': {}",
                    collection_name, e
                )));
            }
        }

        // ----------------------------------------------------------------
        // 6. Rebuild index + atomic backend swap
        // ----------------------------------------------------------------
        backend.rebuild_index();
        state.backends.insert(collection_id, backend);

        Ok(collection_vector_count)
    }

    /// Get index type name and memory usage for a collection
    pub fn collection_backend_stats(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> Option<(&'static str, usize)> {
        let collection_id = CollectionId::new(branch_id, space, name);
        let state = self.state().ok()?;
        state
            .backends
            .get(&collection_id)
            .map(|b| (b.index_type_name(), b.memory_usage()))
    }
}
