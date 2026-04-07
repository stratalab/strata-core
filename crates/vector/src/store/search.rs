//! Vector search operations (search, search_at, search_with_sources).

use super::*;

impl VectorStore {
    /// Search for similar vectors
    ///
    /// Returns top-k vectors most similar to the query.
    /// Metadata filtering is applied as post-filter.
    ///
    /// # Invariants Satisfied
    /// - R1: Dimension validated against collection config
    /// - R2: Scores normalized to "higher = more similar"
    /// - R3: Deterministic order (backend + facade tie-breaking)
    /// - R5: Facade tie-break (score desc, key asc)
    /// - R10: Search is read-only (no mutations)
    pub fn search(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<MetadataFilter>,
    ) -> VectorResult<Vec<VectorMatch>> {
        let mut opts = SearchOptions::default();
        if let Some(f) = filter {
            opts.filter = Some(f);
        }
        self.search_with_options(branch_id, space, collection, query, k, opts)
    }

    /// Search with full control over filtering, metadata inclusion, and over-fetch
    /// behavior (Issues #1966, #1967).
    ///
    /// See [`SearchOptions`] for available knobs.
    pub fn search_with_options(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        opts: SearchOptions,
    ) -> VectorResult<Vec<VectorMatch>> {
        let start = std::time::Instant::now();

        // k=0 returns empty
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, space, collection);

        // Validate query dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        let mut matches = Vec::with_capacity(k);

        if opts.filter.is_none() {
            // No filter — fetch exactly k with O(1) inline meta lookup.
            // Each candidate is verified against KV to skip deleted vectors (Issue #1731).
            let namespace = self.namespace_for(branch_id, space);
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let candidates = backend.search(query, k);

            for (vector_id, score) in candidates {
                if let Some(meta) = backend.get_inline_meta(vector_id) {
                    let kv_key = Key::new_vector(namespace.clone(), collection, &meta.key);
                    // Issue #1967: optionally fetch metadata even when no filter
                    let metadata = if opts.include_metadata {
                        match self.get_vector_record_by_key(&kv_key)? {
                            Some(r) => r.metadata,
                            None => continue, // deleted
                        }
                    } else {
                        if self.get_vector_record_by_key(&kv_key)?.is_none() {
                            continue;
                        }
                        None
                    };
                    matches.push(VectorMatch {
                        key: meta.key.clone(),
                        score,
                        metadata,
                    });
                } else {
                    // Fallback to KV scan — implicitly skips deleted vectors
                    match self.get_key_and_metadata(branch_id, space, collection, vector_id) {
                        Ok((key, metadata)) => {
                            matches.push(VectorMatch {
                                key,
                                score,
                                metadata,
                            });
                        }
                        Err(_) => continue,
                    }
                }
            }
            drop(backend);
        } else {
            // Filter active — use adaptive over-fetch (Issue #1966: configurable multipliers).
            // Fall back to [1] if multipliers is empty so we still fetch at least k candidates.
            let default_multipliers = vec![1usize];
            let multipliers = if opts.overfetch_multipliers.is_empty() {
                &default_multipliers
            } else {
                &opts.overfetch_multipliers
            };
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let collection_size = backend.len();
            let namespace = self.namespace_for(branch_id, space);

            for &mult in multipliers {
                let fetch_k = (k * mult).min(collection_size);
                if fetch_k == 0 {
                    break;
                }

                let candidates = backend.search(query, fetch_k);

                matches.clear();
                for (vector_id, score) in candidates {
                    let (key, metadata) = if let Some(meta) = backend.get_inline_meta(vector_id) {
                        let kv_key = Key::new_vector(namespace.clone(), collection, &meta.key);
                        match self.get_vector_record_by_key(&kv_key)? {
                            Some(r) => (meta.key.clone(), r.metadata),
                            None => continue,
                        }
                    } else {
                        match self.get_key_and_metadata(branch_id, space, collection, vector_id) {
                            Ok(result) => result,
                            Err(_) => continue,
                        }
                    };

                    if let Some(ref f) = opts.filter {
                        if !f.matches(&metadata) {
                            continue;
                        }
                    }

                    matches.push(VectorMatch {
                        key,
                        score,
                        metadata,
                    });
                    if matches.len() >= k {
                        break;
                    }
                }

                if matches.len() >= k || fetch_k >= collection_size {
                    break;
                }
            }
            drop(backend);
        }

        // Facade-level tie-breaking (score desc, key asc) — Invariant R5
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        matches.truncate(k);

        debug!(target: "strata::vector", collection, k, results = matches.len(), duration_us = start.elapsed().as_micros() as u64, branch_id = %branch_id, "Vector search completed");

        Ok(matches)
    }

    /// Search for k nearest neighbors as of a given timestamp.
    ///
    /// Uses temporal filtering in the backend (HNSW nodes alive at as_of_ts)
    /// and historical metadata from the version chain.
    #[allow(clippy::too_many_arguments)]
    pub fn search_at(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<MetadataFilter>,
        as_of_ts: u64,
    ) -> VectorResult<Vec<VectorMatch>> {
        let start = std::time::Instant::now();

        // k=0 returns empty
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, space, collection);

        // Validate dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        let mut matches = Vec::with_capacity(k);

        if filter.is_none() {
            // No filter — fetch exactly k
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let candidates = backend.search_at(query, k, as_of_ts);
            drop(backend);

            for (vector_id, score) in candidates {
                if let Some((key, metadata)) = self.find_vector_key_metadata_at(
                    branch_id, space, collection, vector_id, as_of_ts,
                )? {
                    matches.push(VectorMatch {
                        key,
                        score,
                        metadata,
                    });
                }
            }
        } else {
            // Filter active — adaptive over-fetch [3, 6, 12]
            let multipliers = [3, 6, 12];
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let collection_size = backend.len();

            for &mult in &multipliers {
                let fetch_k = (k * mult).min(collection_size);
                if fetch_k == 0 {
                    break;
                }

                let candidates = backend.search_at(query, fetch_k, as_of_ts);

                matches.clear();
                for (vector_id, score) in candidates {
                    if let Some((key, metadata)) = self.find_vector_key_metadata_at(
                        branch_id, space, collection, vector_id, as_of_ts,
                    )? {
                        if let Some(ref f) = filter {
                            if !f.matches(&metadata) {
                                continue;
                            }
                        }
                        matches.push(VectorMatch {
                            key,
                            score,
                            metadata,
                        });
                        if matches.len() >= k {
                            break;
                        }
                    }
                }

                // If we have enough results or searched all vectors, stop
                if matches.len() >= k || fetch_k >= collection_size {
                    break;
                }
            }
            drop(backend);
        }

        // Apply facade-level tie-breaking (score desc, key asc)
        // This satisfies Invariant R5
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });
        matches.truncate(k);

        debug!(target: "strata::vector", collection, k, results = matches.len(),
               duration_us = start.elapsed().as_micros() as u64,
               as_of_ts, branch_id = %branch_id, "Temporal search completed");

        Ok(matches)
    }

    /// Find a vector's key and metadata by VectorId at a given timestamp (internal helper).
    fn find_vector_key_metadata_at(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        target_id: VectorId,
        as_of_ts: u64,
    ) -> VectorResult<Option<(String, Option<JsonValue>)>> {
        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, collection);
        let results = self
            .db
            .scan_prefix_at_timestamp(&prefix, as_of_ts)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        for (key, vv) in results {
            let bytes = match &vv.value {
                Value::Bytes(b) => b,
                _ => continue,
            };
            let record = match VectorRecord::from_bytes(bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };
            if VectorId(record.vector_id) == target_id {
                let user_key = String::from_utf8(key.user_key.to_vec()).unwrap_or_default();
                // Strip the collection prefix to get just the vector key
                let vector_key = user_key
                    .strip_prefix(&format!("{}/", collection))
                    .unwrap_or(&user_key)
                    .to_string();
                return Ok(Some((vector_key, record.metadata)));
            }
        }
        Ok(None)
    }

    /// Search returning results with source references (internal)
    ///
    /// Uses O(1) inline metadata lookup per candidate instead of O(n) KV prefix scans.
    pub(crate) fn search_with_sources(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, space, collection);

        // Validate query dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        // Search backend + resolve inline metadata under a single guard
        let state = self.state()?;
        let backend =
            state
                .backends
                .get(&collection_id)
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection.to_string(),
                })?;

        let candidates = backend.search(query, k);
        let namespace = self.namespace_for(branch_id, space);

        let mut matches: Vec<VectorMatchWithSource> = Vec::with_capacity(candidates.len());
        let mut fallback_candidates: Vec<(VectorId, f32)> = Vec::new();

        for &(vid, score) in &candidates {
            if let Some(meta) = backend.get_inline_meta(vid) {
                // Verify vector still exists in KV (Issue #1731)
                let kv_key = Key::new_vector(namespace.clone(), collection, &meta.key);
                if self.get_vector_record_by_key(&kv_key)?.is_none() {
                    continue;
                }
                matches.push(VectorMatchWithSource::new(
                    meta.key.clone(),
                    score,
                    None,
                    meta.source_ref.clone(),
                    0,
                ));
            } else {
                fallback_candidates.push((vid, score));
            }
        }

        drop(backend);

        // Resolve any candidates missing inline meta via KV fallback
        for (vid, score) in fallback_candidates {
            if let Ok((key, _metadata)) =
                self.get_key_and_metadata(branch_id, space, collection, vid)
            {
                matches.push(VectorMatchWithSource::new(key, score, None, None, 0));
            }
        }

        // Facade-level tie-breaking (score desc, key asc)
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        matches.truncate(k);

        Ok(matches)
    }

    /// Search returning results with source references, filtered by time range.
    ///
    /// Uses O(1) inline metadata lookup per candidate instead of O(n) KV prefix scans.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_with_sources_in_range(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, space, collection);

        // Validate query dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        // Search backend + resolve inline metadata under a single guard
        let state = self.state()?;
        let backend =
            state
                .backends
                .get(&collection_id)
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection.to_string(),
                })?;

        let candidates = backend.search_in_range(query, k, start_ts, end_ts);
        let namespace = self.namespace_for(branch_id, space);

        let mut matches: Vec<VectorMatchWithSource> = Vec::with_capacity(candidates.len());
        let mut fallback_candidates: Vec<(VectorId, f32)> = Vec::new();

        for &(vid, score) in &candidates {
            if let Some(meta) = backend.get_inline_meta(vid) {
                // Verify vector still exists in KV (Issue #1731)
                let kv_key = Key::new_vector(namespace.clone(), collection, &meta.key);
                if self.get_vector_record_by_key(&kv_key)?.is_none() {
                    continue;
                }
                matches.push(VectorMatchWithSource::new(
                    meta.key.clone(),
                    score,
                    None,
                    meta.source_ref.clone(),
                    0,
                ));
            } else {
                fallback_candidates.push((vid, score));
            }
        }

        drop(backend);

        // Resolve any candidates missing inline meta via KV fallback
        for (vid, score) in fallback_candidates {
            if let Ok((key, _metadata)) =
                self.get_key_and_metadata(branch_id, space, collection, vid)
            {
                matches.push(VectorMatchWithSource::new(key, score, None, None, 0));
            }
        }

        // Facade-level tie-breaking (score desc, key asc)
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        matches.truncate(k);

        Ok(matches)
    }
}
