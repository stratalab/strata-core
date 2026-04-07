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
    ///
    /// # Errors
    /// - `CollectionNotFound` if collection doesn't exist
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

        info!(target: "strata::vector", collection = name, branch_id = %branch_id, "Collection deleted");

        Ok(())
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
        let version = self.db.storage().version();
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
        let version = self.db.storage().version();

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
        let version = self.db.storage().version();

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
