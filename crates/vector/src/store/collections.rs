//! Collection lifecycle operations (create, delete, list, get).

use super::*;

impl VectorStore {
    /// Creates a collection with the specified configuration.
    /// The configuration (dimension, metric, dtype) is immutable after creation.
    ///
    /// # Errors
    /// - `CollectionAlreadyExists` if a collection with this name exists
    /// - `InvalidCollectionName` if name is invalid
    /// - `InvalidDimension` if dimension is 0
    pub fn create_collection(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
        config: VectorConfig,
    ) -> VectorResult<Versioned<CollectionInfo>> {
        self.create_collection_with_backend(
            branch_id,
            space,
            name,
            config,
            crate::IndexBackendType::default(),
        )
    }

    /// Creates a collection with an explicit backend type (Issue #1964).
    ///
    /// Use `IndexBackendType::BruteForce` for small collections (< 1000 vectors)
    /// to reduce memory overhead, or `SegmentedHnsw` (default) for larger workloads.
    pub fn create_collection_with_backend(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
        config: VectorConfig,
        backend_type: crate::IndexBackendType,
    ) -> VectorResult<Versioned<CollectionInfo>> {
        // Validate name
        validate_collection_name(name)?;

        // Validate config (dimension must be > 0 and <= MAX_DIMENSION)
        const MAX_DIMENSION: usize = 65536;
        if config.dimension == 0 {
            return Err(VectorError::InvalidDimension {
                dimension: config.dimension,
            });
        }
        if config.dimension > MAX_DIMENSION {
            return Err(VectorError::InvalidDimension {
                dimension: config.dimension,
            });
        }

        let collection_id = CollectionId::new(branch_id, space, name);

        // Check if collection already exists
        if self.collection_exists(branch_id, space, name)? {
            return Err(VectorError::CollectionAlreadyExists {
                name: name.to_string(),
            });
        }

        let now = now_micros();

        // Create collection record with backend type
        let record = CollectionRecord::with_backend_type(&config, backend_type);

        // Store config in KV
        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        let config_bytes = record.to_bytes()?;

        // Use transaction for atomic storage
        self.db
            .transaction(branch_id, |txn| {
                strata_engine::primitives::space::ensure_space_registered_in_txn(
                    txn, &branch_id, space,
                )?;
                txn.put(config_key.clone(), Value::Bytes(config_bytes.clone()))
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Initialize in-memory backend with the selected backend type
        self.init_backend_with_type(&collection_id, &config, backend_type)?;

        let info = CollectionInfo {
            name: name.to_string(),
            config,
            count: 0,
            created_at: now,
        };

        info!(target: "strata::vector", collection = name, dimension = info.config.dimension, branch_id = %branch_id, "Collection created");

        Ok(Versioned::with_timestamp(
            info,
            Version::counter(1),
            Timestamp::from_micros(now),
        ))
    }

    /// Delete a collection and all its vectors
    ///
    /// This is a destructive operation that:
    /// 1. Deletes all vectors in the collection
    /// 2. Deletes the collection configuration
    /// 3. Removes the in-memory backend
    /// 4. Wipes the on-disk `.vec` and `_graphs/` cache files
    ///
    /// # Errors
    /// - `CollectionNotFound` if collection doesn't exist
    ///
    /// # Concurrency note (latent race — read before changing)
    ///
    /// Steps 1-2 happen inside an MVCC transaction (atomic, isolated).
    /// Steps 3-4 are post-commit side-effects on the in-memory
    /// `backends` map and the on-disk cache files — they are NOT
    /// covered by the transaction's isolation boundary.
    ///
    /// As a result, a concurrent `create_collection` of the same name
    /// can interleave between this function's KV commit (step 2) and
    /// its disk cache purge (step 4):
    ///
    /// 1. T1 (delete) commits the KV txn — config key gone.
    /// 2. T2 (create) sees `collection_exists() == false`, commits a
    ///    new config record, initialises a fresh in-memory backend.
    /// 3. T1 wipes the `.vec` / `_graphs/` files at the path.
    /// 4. T2's eventual freeze re-writes the cache.
    ///
    /// **Today this race is benign** because:
    ///
    /// - **The cache is write-through.** Every vector that lives in
    ///   the on-disk cache is also in KV. A wiped cache is rebuilt by
    ///   `recover_from_db` (`crates/vector/src/recovery.rs`) by
    ///   scanning KV. Worst case: T2's first recovery after a crash
    ///   takes the slow KV-scan path instead of the mmap fast path.
    ///   No data loss.
    ///
    /// - **New collections start in `Owned` heap mode, not `Mmap`.**
    ///   T2's freshly-initialised backend does not alias the file we
    ///   are deleting in step 4, so the `remove_file` call cannot
    ///   pull memory out from under T2's heap.
    ///
    /// **Both invariants are load-bearing.** If a future change makes
    /// the cache write-back (vectors only in `.vec`, not in KV), or
    /// makes new collections start with a heap mmap'd from
    /// `{name}.vec`, this race becomes a real data-loss bug.
    ///
    /// The fix would be either (a) per-`CollectionId` lifecycle
    /// serialisation, or (b) generation-suffixed cache paths
    /// (`{name}_{created_at}.vec`) so a delete-then-recreate uses
    /// distinct files by construction. Neither is wired in today —
    /// the current code relies on the two invariants above. Any PR
    /// that touches the cache representation MUST re-audit this race
    /// or add one of those mechanisms.
    pub fn delete_collection(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, space, name);

        // Check if collection exists
        if !self.collection_exists(branch_id, space, name)? {
            return Err(VectorError::CollectionNotFound {
                name: name.to_string(),
            });
        }

        // Delete all vectors in the collection
        self.delete_all_vectors(branch_id, space, name)?;

        // Delete config from KV
        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        self.db
            .transaction(branch_id, |txn| txn.delete(config_key.clone()))
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Remove in-memory backend
        {
            let state = self.state()?;
            state.backends.remove(&collection_id);
        }

        // Wipe the on-disk `.vec` mmap and `_graphs/` cache so the next
        // recovery pass cannot resurrect this collection from a stale
        // cache. Best-effort: missing files / I/O failures are logged
        // but never returned (the primary delete already committed).
        // See the concurrency note on this function for the latent
        // race with concurrent `create_collection` of the same name.
        crate::recovery::purge_collection_disk_cache(self.db.data_dir(), &collection_id);

        info!(target: "strata::vector", collection = name, branch_id = %branch_id, "Collection deleted");

        Ok(())
    }

    /// Purge every collection in the in-memory backend state for
    /// `(branch_id, space)` and wipe each collection's on-disk caches.
    /// Returns the number of collections removed.
    ///
    /// **Best-effort cleanup hook for `space_delete --force`.** This
    /// helper reads `state.backends` directly (not the KV config keys)
    /// so callers may invoke it AFTER the primary-delete transaction
    /// has wiped the config rows — the in-memory state still has the
    /// `CollectionId` entries that the recovery pass loaded earlier,
    /// which is what we need to evict here.
    ///
    /// Disk cache purge is gated on a non-empty `data_dir` (ephemeral
    /// databases never write caches in the first place).
    ///
    /// # Concurrency note
    ///
    /// Same latent race as `delete_collection`: a concurrent
    /// `create_collection` for one of the target `CollectionId`s can
    /// interleave with this purge and have its freshly-written cache
    /// wiped. Today this is benign for the same two reasons listed on
    /// `delete_collection` (write-through cache + new heaps in
    /// `Owned` mode). Read that note before changing the cache
    /// representation.
    pub fn purge_collections_in_space(
        &self,
        branch_id: BranchId,
        space: &str,
    ) -> VectorResult<usize> {
        let state = self.state()?;
        let target_cids: Vec<CollectionId> = state
            .backends
            .iter()
            .filter(|entry| entry.key().branch_id == branch_id && entry.key().space == space)
            .map(|entry| entry.key().clone())
            .collect();

        let n = target_cids.len();
        let data_dir = self.db.data_dir().to_path_buf();
        for cid in &target_cids {
            state.backends.remove(cid);
            crate::recovery::purge_collection_disk_cache(&data_dir, cid);
        }
        if n > 0 {
            info!(
                target: "strata::vector",
                branch_id = %branch_id,
                space = space,
                purged = n,
                "Purged vector collections during space delete"
            );
        }
        Ok(n)
    }

    /// List all collections for a branch
    ///
    /// Returns CollectionInfo for each collection, including current vector count.
    /// Results are sorted by name for determinism (Invariant R4).
    pub fn list_collections(
        &self,
        branch_id: BranchId,
        space: &str,
    ) -> VectorResult<Vec<CollectionInfo>> {
        use strata_core::traits::Storage;

        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::new_vector_config_prefix(namespace);

        // Read at current version for consistency
        let version = CommitVersion(self.db.storage().version());
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let mut collections = Vec::new();

        for (key, versioned_value) in entries {
            // Extract collection name from key (user_key = "__config__/{name}")
            let raw = String::from_utf8(key.user_key.to_vec())
                .map_err(|e| VectorError::Serialization(e.to_string()))?;
            let name = raw.strip_prefix("__config__/").unwrap_or(&raw).to_string();

            // Deserialize the record from the stored bytes
            let bytes = match &versioned_value.value {
                Value::Bytes(b) => b.clone(),
                _ => {
                    return Err(VectorError::Serialization(
                        "Expected Bytes value for collection record".to_string(),
                    ))
                }
            };
            let record = CollectionRecord::from_bytes(&bytes)?;
            let config = VectorConfig::try_from(record.config)?;

            // Get current count from backend
            let collection_id = CollectionId::new(branch_id, space, &name);
            let count = self.get_collection_count(&collection_id, branch_id, space, &name)?;

            collections.push(CollectionInfo {
                name,
                config,
                count,
                created_at: record.created_at,
            });
        }

        // Sort by name for determinism
        collections.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(collections)
    }

    /// Check if a collection exists (internal)
    pub(crate) fn collection_exists(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<bool> {
        use strata_core::traits::Storage;

        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        let version = CommitVersion(self.db.storage().version());

        Ok(self
            .db
            .storage()
            .get_versioned(&config_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
            .is_some())
    }

    /// Get a single collection's info (internal - for snapshot/recovery)
    pub(crate) fn get_collection(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<Option<Versioned<CollectionInfo>>> {
        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);

        use strata_core::traits::Storage;
        let version = CommitVersion(self.db.storage().version());

        let Some(versioned_value) = self
            .db
            .storage()
            .get_versioned(&config_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };

        let bytes = match &versioned_value.value {
            Value::Bytes(b) => b.clone(),
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for collection record".to_string(),
                ))
            }
        };
        let record = CollectionRecord::from_bytes(&bytes)?;
        let config = VectorConfig::try_from(record.config)?;

        let collection_id = CollectionId::new(branch_id, space, name);
        let count = self.get_collection_count(&collection_id, branch_id, space, name)?;

        let info = CollectionInfo {
            name: name.to_string(),
            config,
            count,
            created_at: record.created_at,
        };

        Ok(Some(Versioned::with_timestamp(
            info,
            versioned_value.version,
            versioned_value.timestamp,
        )))
    }
}
