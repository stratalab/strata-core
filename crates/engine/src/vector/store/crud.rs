//! Vector CRUD and batch operations.

use super::*;
use crate::vector::ext::VectorStoreExt;
use strata_storage::TransactionContext;

impl VectorStore {
    /// Insert a vector (upsert semantics)
    ///
    /// If a vector with this key already exists, it is overwritten.
    /// This follows Rule 3 (Upsert Semantics).
    ///
    /// # Errors
    /// - `CollectionNotFound` if collection doesn't exist
    /// - `InvalidKey` if key is invalid
    /// - `DimensionMismatch` if embedding dimension doesn't match config
    pub fn insert(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
    ) -> VectorResult<Version> {
        self.insert_inner(branch_id, space, collection, key, embedding, metadata, None)
    }

    /// Common insert implementation used by both `insert()` and `system_insert_with_source()`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_inner(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
        source_ref: Option<EntityRef>,
    ) -> VectorResult<Version> {
        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        // Validate key and embedding BEFORE entering the transaction so that
        // validation errors are returned as proper VectorError variants (not
        // wrapped in StrataError by the transaction layer).
        validate_vector_key(key)?;
        if embedding.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(VectorError::InvalidEmbedding {
                reason: "embedding contains NaN or Infinity values".to_string(),
            });
        }
        let state = self.state()?;
        let collection_id = CollectionId::new(branch_id, space, collection);
        {
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let config = backend.config();
            if embedding.len() != config.dimension {
                return Err(VectorError::DimensionMismatch {
                    expected: config.dimension,
                    got: embedding.len(),
                });
            }
        }

        // Run the transactional KV write + VectorId allocation via ext trait.
        // The engine transaction extension queues backend maintenance for the
        // commit observer, so the HNSW update is applied exactly once after
        // successful commit.
        let version = self
            .db
            .transaction(branch_id, |txn| {
                crate::primitives::space::ensure_space_registered_in_txn(txn, &branch_id, space)?;
                txn.vector_upsert(
                    branch_id,
                    space,
                    collection,
                    key,
                    embedding,
                    metadata.clone(),
                    source_ref.clone(),
                    &state,
                )
                .map_err(|e| e.into_strata_error(branch_id))
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        debug!(target: "strata::vector", collection, branch_id = %branch_id, "Vector upserted");

        Ok(version)
    }

    /// Insert a vector using an existing transaction context.
    ///
    /// The KV write participates in the caller's transaction. In-memory vector
    /// index maintenance is queued on the database-owned backend state and is
    /// applied only by the engine commit observer after the transaction commits.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_in_transaction(
        &self,
        txn: &mut TransactionContext,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
    ) -> VectorResult<Version> {
        self.insert_in_transaction_inner(
            txn, branch_id, space, collection, key, embedding, metadata, None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_in_transaction_inner(
        &self,
        txn: &mut TransactionContext,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
        source_ref: Option<EntityRef>,
    ) -> VectorResult<Version> {
        self.ensure_collection_loaded(branch_id, space, collection)?;
        let state = self.state()?;
        txn.vector_upsert(
            branch_id, space, collection, key, embedding, metadata, source_ref, &state,
        )
    }

    /// Get a vector by key
    ///
    /// Returns the vector entry including embedding and metadata.
    /// Returns None if vector doesn't exist.
    pub fn get(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<Option<Versioned<VectorEntry>>> {
        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, space, collection);
        let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

        // Get record from KV with version info
        let version = CommitVersion(self.db.storage().version());
        let Some(versioned_value) = self
            .db
            .storage()
            .get_versioned(&kv_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };

        let bytes = match &versioned_value.value {
            Value::Bytes(b) => b,
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for vector record".to_string(),
                ))
            }
        };

        let record = VectorRecord::from_bytes(bytes)?;
        let vector_id = VectorId(record.vector_id);

        // Get embedding: prefer backend, fall back to KV record (ARCH-003).
        // The backend may be missing this vector if a crash or error occurred
        // between KV commit and backend update (Issue #1731).
        let state = self.state()?;
        let backend =
            state
                .backends
                .get(&collection_id)
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection.to_string(),
                })?;

        let embedding = if !record.embedding.is_empty() {
            record.embedding
        } else {
            // Backend is a fallback only for the current-time get() path, where
            // the backend embedding is guaranteed to match the current KV version.
            backend.get(vector_id).map(|e| e.to_vec()).ok_or_else(|| {
                VectorError::Internal(
                    "Embedding missing from both backend and KV record".to_string(),
                )
            })?
        };

        let entry = VectorEntry {
            key: key.to_string(),
            embedding,
            metadata: record.metadata,
            vector_id,
            version: Version::counter(record.version),
            source_ref: record.source_ref,
        };

        Ok(Some(Versioned::with_timestamp(
            entry,
            versioned_value.version,
            versioned_value.timestamp,
        )))
    }

    /// Get a vector using an existing transaction context.
    ///
    /// Reads from the transaction context, so uncommitted writes from the same
    /// transaction are visible.
    pub fn get_in_transaction(
        &self,
        txn: &mut TransactionContext,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<Option<VectorRecord>> {
        txn.vector_get(branch_id, space, collection, key)
    }

    /// Get a vector as of a past timestamp.
    ///
    /// Returns the vector if it existed at as_of_ts.
    /// This is a non-transactional read directly from the storage version chain.
    pub fn get_at(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        as_of_ts: u64,
    ) -> VectorResult<Option<VectorEntry>> {
        let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

        // Get historical record from storage
        let result = self
            .db
            .get_at_timestamp(&kv_key, as_of_ts)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let Some(vv) = result else {
            return Ok(None);
        };

        let bytes = match &vv.value {
            Value::Bytes(b) => b,
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for vector record".to_string(),
                ))
            }
        };

        let record = VectorRecord::from_bytes(bytes)?;

        // Use the embedding stored in the VectorRecord (historical snapshot).
        // The backend only holds the *current* embedding, which may differ if the
        // vector was re-upserted after as_of_ts.
        if record.embedding.is_empty() {
            return Err(VectorError::Internal(
                "Historical embedding unavailable (pre-embedding-storage record)".to_string(),
            ));
        }
        let embedding = record.embedding;

        Ok(Some(VectorEntry {
            key: key.to_string(),
            embedding,
            metadata: record.metadata,
            vector_id: VectorId(record.vector_id),
            version: strata_core::contract::Version::counter(record.version),
            source_ref: record.source_ref,
        }))
    }

    /// Get the full version history for a vector key.
    ///
    /// Returns `None` if the key has never existed. Index `[0]` is the latest
    /// version, `[1]` the previous, etc. Reads directly from the storage version
    /// chain (non-transactional), so this is unaffected by collection load state.
    ///
    /// Tombstones and pre-embedding-storage legacy records are skipped, so a
    /// single corrupt or partial entry does not break the whole history. If all
    /// entries are skipped, returns `None`.
    pub fn getv(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<Option<strata_core::VersionedHistory<VectorEntry>>> {
        let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

        let raw_history = self
            .db
            .storage()
            .get_history(&kv_key, None, None)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let mut versions: Vec<Versioned<VectorEntry>> = Vec::with_capacity(raw_history.len());
        for vv in raw_history {
            // Tombstones and any non-Bytes values are skipped: the version chain
            // includes delete markers, but a VectorEntry cannot be reconstructed
            // from them.
            let Value::Bytes(ref bytes) = vv.value else {
                continue;
            };
            let record = match VectorRecord::from_bytes(bytes) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        target: "strata::vector",
                        collection,
                        key,
                        error = %e,
                        "Skipping corrupt historical vector record in getv"
                    );
                    continue;
                }
            };
            // Legacy records pre-dating embedding storage cannot be reconstructed
            // with a historical embedding; skip them rather than fail the whole
            // history read.
            if record.embedding.is_empty() {
                warn!(
                    target: "strata::vector",
                    collection,
                    key,
                    "Skipping pre-embedding-storage historical vector record in getv"
                );
                continue;
            }
            let entry = VectorEntry {
                key: key.to_string(),
                embedding: record.embedding,
                metadata: record.metadata,
                vector_id: VectorId(record.vector_id),
                version: Version::counter(record.version),
                source_ref: record.source_ref,
            };
            versions.push(Versioned::with_timestamp(entry, vv.version, vv.timestamp));
        }

        Ok(strata_core::VersionedHistory::new(versions))
    }

    /// Delete a vector by key
    ///
    /// Returns true if the vector existed and was deleted.
    pub fn delete(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<bool> {
        // Ensure collection is loaded before delegating to ext trait
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let result = self.db.transaction(branch_id, |txn| {
            self.delete_in_transaction(txn, branch_id, space, collection, key)
                .map_err(|e| e.into_strata_error(branch_id))
        });
        let existed = match result {
            Ok(v) => v,
            Err(e) => return Err(VectorError::Storage(e.to_string())),
        };

        Ok(existed)
    }

    /// Delete a vector using an existing transaction context.
    ///
    /// Returns true if the vector existed and queues backend maintenance for
    /// the engine commit observer.
    pub fn delete_in_transaction(
        &self,
        txn: &mut TransactionContext,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<bool> {
        self.ensure_collection_loaded(branch_id, space, collection)?;
        let state = self.state()?;
        txn.vector_delete(branch_id, space, collection, key, &state)
    }

    /// Batch insert multiple vectors (upsert semantics)
    ///
    /// Acquires the write lock once, validates all entries, commits all KV writes,
    /// then updates the backend for each entry. Much more efficient than N individual inserts.
    ///
    /// # Errors
    /// - `CollectionNotFound` if collection doesn't exist
    /// - `DimensionMismatch` if any embedding has wrong dimension
    /// - `InvalidEmbedding` if any embedding contains NaN or Infinity
    /// - `InvalidKey` if any key is invalid
    pub fn batch_insert(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        entries: Vec<(String, Vec<f32>, Option<JsonValue>)>,
    ) -> VectorResult<Vec<Version>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Validate all entries before acquiring locks
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        for (key, embedding, _) in &entries {
            validate_vector_key(key)?;
            if embedding.iter().any(|v| v.is_nan() || v.is_infinite()) {
                return Err(VectorError::InvalidEmbedding {
                    reason: format!(
                        "embedding for key '{}' contains NaN or Infinity values",
                        key
                    ),
                });
            }
            if embedding.len() != config.dimension {
                return Err(VectorError::DimensionMismatch {
                    expected: config.dimension,
                    got: embedding.len(),
                });
            }
        }

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;
        let collection_id = CollectionId::new(branch_id, space, collection);

        // Acquire per-collection lock once for the entire batch
        let state = self.state()?;
        let mut backend = state.backends.get_mut(&collection_id).ok_or_else(|| {
            VectorError::CollectionNotFound {
                name: collection.to_string(),
            }
        })?;

        let mut versions = Vec::with_capacity(entries.len());
        let batch_count = entries.len();

        // Prepare all records and accumulate KV writes for a single transaction
        let mut kv_writes: Vec<(Key, Value)> = Vec::with_capacity(entries.len());
        let mut backend_updates: Vec<(VectorId, String, Vec<f32>, u64)> =
            Vec::with_capacity(entries.len());

        for (key, embedding, metadata) in entries {
            let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, &key);

            // Check existence
            let existing = self.get_vector_record_by_key(&kv_key)?;

            let (vector_id, record) = if let Some(existing_record) = existing {
                let mut updated = existing_record;
                updated.update(embedding.clone(), metadata);
                (VectorId(updated.vector_id), updated)
            } else {
                let vector_id = backend.allocate_id();
                let record = VectorRecord::new(vector_id, embedding.clone(), metadata);
                (vector_id, record)
            };

            let record_version = record.version;
            let record_bytes = record.to_bytes()?;
            kv_writes.push((kv_key, Value::Bytes(record_bytes)));
            backend_updates.push((vector_id, key, embedding, record.created_at));
            versions.push(Version::counter(record_version));
        }

        // Commit all KV writes in a single transaction
        self.db
            .transaction(branch_id, |txn| {
                crate::primitives::space::ensure_space_registered_in_txn(txn, &branch_id, space)?;
                for (key, value) in &kv_writes {
                    txn.put(key.clone(), value.clone())?;
                }
                Ok(())
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Update backend for each entry (after successful KV commit).
        // Backend failures are non-fatal since KV is already committed (Issue #1731).
        for (vector_id, key, embedding, created_at) in backend_updates {
            if let Err(e) = backend.insert_with_timestamp(vector_id, &embedding, created_at) {
                warn!(target: "strata::vector", collection, key, error = %e, "Backend insert failed after KV commit in batch");
            } else {
                backend.set_inline_meta(
                    vector_id,
                    crate::vector::types::InlineMeta {
                        key,
                        source_ref: None,
                    },
                );
            }
        }

        drop(backend);

        debug!(target: "strata::vector", collection, count = batch_count, branch_id = %branch_id, "Batch upsert completed");

        Ok(versions)
    }

    /// Batch get multiple vectors by key.
    ///
    /// Returns a `Vec` whose i-th element corresponds to `keys[i]`.
    /// Missing keys yield `None`. All reads share one collection lock acquisition.
    pub fn batch_get(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        keys: &[String],
    ) -> VectorResult<Vec<Option<Versioned<VectorEntry>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, space, collection);

        let version = CommitVersion(self.db.storage().version());

        let state = self.state()?;
        let backend =
            state
                .backends
                .get(&collection_id)
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection.to_string(),
                })?;

        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

            let versioned_value = self
                .db
                .storage()
                .get_versioned(&kv_key, version)
                .map_err(|e| VectorError::Storage(e.to_string()))?;

            let entry = match versioned_value {
                Some(vv) => {
                    let bytes = match &vv.value {
                        Value::Bytes(b) => b,
                        _ => {
                            return Err(VectorError::Serialization(
                                "Expected Bytes value for vector record".to_string(),
                            ))
                        }
                    };
                    let record = VectorRecord::from_bytes(bytes)?;
                    let vector_id = VectorId(record.vector_id);

                    let embedding = if !record.embedding.is_empty() {
                        record.embedding
                    } else {
                        backend.get(vector_id).map(|e| e.to_vec()).ok_or_else(|| {
                            VectorError::Internal(
                                "Embedding missing from both backend and KV record".to_string(),
                            )
                        })?
                    };

                    Some(Versioned::with_timestamp(
                        VectorEntry {
                            key: key.to_string(),
                            embedding,
                            metadata: record.metadata,
                            vector_id,
                            version: Version::counter(record.version),
                            source_ref: record.source_ref,
                        },
                        vv.version,
                        vv.timestamp,
                    ))
                }
                None => None,
            };
            results.push(entry);
        }

        Ok(results)
    }

    /// Batch delete multiple vectors by key.
    ///
    /// Returns a `Vec<bool>` where `results[i]` is `true` if `keys[i]` existed
    /// and was deleted. All deletes share one collection lock for atomicity.
    pub fn batch_delete(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        keys: &[String],
    ) -> VectorResult<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, space, collection);

        // Hold per-collection write lock for entire check-then-delete batch to
        // prevent TOCTOU race with concurrent insert (fixes #1572).
        let state = self.state()?;
        let mut backend = state.backends.get_mut(&collection_id).ok_or_else(|| {
            VectorError::CollectionNotFound {
                name: collection.to_string(),
            }
        })?;

        // Check existence and collect records before committing
        let mut kv_keys = Vec::with_capacity(keys.len());
        let mut vector_ids: Vec<Option<VectorId>> = Vec::with_capacity(keys.len());

        for key in keys {
            let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);
            match self.get_vector_record_by_key(&kv_key)? {
                Some(record) => {
                    let vector_id = VectorId(record.vector_id);
                    kv_keys.push(kv_key);
                    vector_ids.push(Some(vector_id));
                }
                None => {
                    vector_ids.push(None);
                }
            }
        }

        // Commit all deletes in a single transaction
        if !kv_keys.is_empty() {
            self.db
                .transaction(branch_id, |txn| {
                    for kv_key in &kv_keys {
                        txn.delete(kv_key.clone())?;
                    }
                    Ok(())
                })
                .map_err(|e| VectorError::Storage(e.to_string()))?;
        }

        // Update backend after KV commit succeeds
        let ts = now_micros();
        for vector_id in vector_ids.iter().flatten() {
            match backend.delete_with_timestamp(*vector_id, ts) {
                Ok(_) => {
                    backend.remove_inline_meta(*vector_id);
                }
                Err(e) => {
                    warn!(target: "strata::vector", collection, error = %e,
                        "Backend delete failed after KV delete in batch; search will filter via KV check");
                }
            }
        }

        let results: Vec<bool> = vector_ids.iter().map(|v| v.is_some()).collect();
        Ok(results)
    }

    /// List all vector keys in a collection.
    ///
    /// Returns just the user-facing key names (without internal prefixes).
    /// Useful for introspection and sampling.
    pub fn list_keys(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
    ) -> VectorResult<Vec<String>> {
        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, collection);

        let version = CommitVersion(self.db.storage().version());
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let mut keys = Vec::new();
        for (key, _) in entries {
            let user_key = String::from_utf8(key.user_key.to_vec()).unwrap_or_default();
            // Strip the collection prefix to get just the vector key
            let vector_key = user_key
                .strip_prefix(&format!("{}/", collection))
                .unwrap_or(&user_key)
                .to_string();
            keys.push(vector_key);
        }
        Ok(keys)
    }
}
