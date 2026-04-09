//! System collection operations (internal use only).
//!
//! All system collections live in the `_system_` space, invisible to users.

use super::*;
use strata_engine::system_space::SYSTEM_SPACE;

impl VectorStore {
    pub fn create_system_collection(
        &self,
        branch_id: BranchId,
        name: &str,
        config: VectorConfig,
    ) -> VectorResult<Versioned<CollectionInfo>> {
        use crate::collection::validate_system_collection_name;

        validate_system_collection_name(name)?;

        const MAX_DIMENSION: usize = 65536;
        if config.dimension == 0 || config.dimension > MAX_DIMENSION {
            return Err(VectorError::InvalidDimension {
                dimension: config.dimension,
            });
        }

        let collection_id = CollectionId::new(branch_id, SYSTEM_SPACE, name);

        if self.collection_exists(branch_id, SYSTEM_SPACE, name)? {
            return Err(VectorError::CollectionAlreadyExists {
                name: name.to_string(),
            });
        }

        let now = now_micros();
        let record = CollectionRecord::new(&config);
        let config_key = Key::new_vector_config(self.namespace_for(branch_id, SYSTEM_SPACE), name);
        let config_bytes = record.to_bytes()?;

        self.db
            .transaction(branch_id, |txn| {
                txn.put(config_key.clone(), Value::Bytes(config_bytes.clone()))
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        self.init_backend(&collection_id, &config)?;

        let info = CollectionInfo {
            name: name.to_string(),
            config,
            count: 0,
            created_at: now,
        };

        Ok(Versioned::with_timestamp(
            info,
            Version::counter(1),
            Timestamp::from_micros(now),
        ))
    }

    /// Insert into a system collection (internal use only)
    pub fn system_insert(
        &self,
        branch_id: BranchId,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
    ) -> VectorResult<Version> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.insert(
            branch_id,
            SYSTEM_SPACE,
            collection,
            key,
            embedding,
            metadata,
        )
    }

    /// Insert into a system collection with a source reference (internal use only)
    ///
    /// Like `system_insert` but also stores an `EntityRef` that traces the
    /// shadow embedding back to the originating record, enabling hybrid search.
    pub fn system_insert_with_source(
        &self,
        branch_id: BranchId,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
        source_ref: EntityRef,
    ) -> VectorResult<Version> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.insert_inner(
            branch_id,
            SYSTEM_SPACE,
            collection,
            key,
            embedding,
            metadata,
            Some(source_ref),
        )
    }

    /// Delete a system collection (idempotent — returns Ok if it doesn't exist).
    pub fn delete_system_collection(&self, branch_id: BranchId, name: &str) -> VectorResult<()> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(name)?;

        if !self.collection_exists(branch_id, SYSTEM_SPACE, name)? {
            return Ok(());
        }

        self.delete_all_vectors(branch_id, SYSTEM_SPACE, name)?;

        let config_key = Key::new_vector_config(self.namespace_for(branch_id, SYSTEM_SPACE), name);
        self.db
            .transaction(branch_id, |txn| txn.delete(config_key.clone()))
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        {
            let state = self.state()?;
            let collection_id = CollectionId::new(branch_id, SYSTEM_SPACE, name);
            state.backends.remove(&collection_id);
        }

        info!(target: "strata::vector", collection = name, "System collection deleted");
        Ok(())
    }

    /// Get the dimension of an existing system collection, or None if not found.
    pub fn system_collection_dimension(
        &self,
        branch_id: BranchId,
        collection: &str,
    ) -> VectorResult<Option<usize>> {
        Ok(self
            .get_collection(branch_id, SYSTEM_SPACE, collection)?
            .map(|v| v.value.config.dimension))
    }

    /// Search a system collection returning results with source references (internal use only)
    ///
    /// Returns `VectorMatchWithSource` which includes the `source_ref` and `version`
    /// from the original VectorRecord. Used by hybrid search to trace shadow vectors
    /// back to their originating records.
    pub fn system_search_with_sources(
        &self,
        branch_id: BranchId,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.search_with_sources(branch_id, SYSTEM_SPACE, collection, query, k)
    }

    /// Temporal system search returning results with source references.
    ///
    /// Mirrors `system_search_with_sources()` but resolves candidates via the
    /// version chain at `as_of_ts`. Used by hybrid search for point-in-time
    /// retrieval (`request.as_of`).
    pub fn system_search_at_with_sources(
        &self,
        branch_id: BranchId,
        collection: &str,
        query: &[f32],
        k: usize,
        as_of_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.search_at_with_sources(branch_id, SYSTEM_SPACE, collection, query, k, as_of_ts)
    }

    /// Temporal system search with sources, intersected with a time range.
    ///
    /// Mirrors `system_search_at_with_sources()` but additionally drops any
    /// candidate whose `record.created_at` falls outside `[start_ts, end_ts]`.
    /// Used by hybrid search when the request sets both `as_of` and
    /// `time_range`, so the vector branch matches the BM25 intersection
    /// contract from PR #2365.
    #[allow(clippy::too_many_arguments)]
    pub fn system_search_at_in_range_with_sources(
        &self,
        branch_id: BranchId,
        collection: &str,
        query: &[f32],
        k: usize,
        as_of_ts: u64,
        start_ts: u64,
        end_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.search_at_in_range_with_sources(
            branch_id,
            SYSTEM_SPACE,
            collection,
            query,
            k,
            as_of_ts,
            start_ts,
            end_ts,
        )
    }

    /// System search with sources, filtered by time range.
    ///
    /// Mirrors `system_search_with_sources()` but uses `search_in_range()` to
    /// restrict results to vectors created within the given timestamp range.
    pub fn system_search_with_sources_in_range(
        &self,
        branch_id: BranchId,
        collection: &str,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.search_with_sources_in_range(
            branch_id,
            SYSTEM_SPACE,
            collection,
            query,
            k,
            start_ts,
            end_ts,
        )
    }

    /// Delete from a system collection (internal use only)
    pub fn system_delete(
        &self,
        branch_id: BranchId,
        collection: &str,
        key: &str,
    ) -> VectorResult<bool> {
        use crate::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.delete(branch_id, SYSTEM_SPACE, collection, key)
    }
}
