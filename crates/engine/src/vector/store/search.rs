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
        let _refresh_guard = self.db.refresh_query_guard();

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
        let _refresh_guard = self.db.refresh_query_guard();

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

    /// Find a vector's full record by VectorId at a given timestamp (internal helper).
    ///
    /// Scans the collection prefix at `as_of_ts` and returns the historical
    /// `VectorRecord` for the matching `target_id` together with its
    /// collection-relative key. Used by both `search_at` (which only needs
    /// metadata) and `search_at_with_sources` (which needs `source_ref`).
    fn find_vector_record_at(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        target_id: VectorId,
        as_of_ts: u64,
    ) -> VectorResult<Option<(String, VectorRecord)>> {
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
                return Ok(Some((vector_key, record)));
            }
        }
        Ok(None)
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
        Ok(self
            .find_vector_record_at(branch_id, space, collection, target_id, as_of_ts)?
            .map(|(k, r)| (k, r.metadata)))
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
        let _refresh_guard = self.db.refresh_query_guard();
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
        let _refresh_guard = self.db.refresh_query_guard();
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

    /// Temporal search with sources, intersected with a `[start_ts, end_ts]`
    /// time range.
    ///
    /// Combines `search_at` visibility (only vectors alive at `as_of_ts`)
    /// with a `created_at` post-filter, mirroring the BM25 intersection
    /// semantics PR #2365 made the contract on the BM25 side. Used by
    /// hybrid retrieval when the request sets both `as_of` and `time_range`.
    ///
    /// Bounds are **inclusive on both ends**, matching the existing
    /// `HnswBackend::search_in_range` and `db.get_at_timestamp` upper-bound
    /// semantics so the four temporal modes agree on what `time_range` means.
    ///
    /// **Filter timestamp**: this uses `record.created_at` (the *original*
    /// vector creation time, carried over across `update_with_source` calls
    /// per `VectorRecord::update_with_source`). It is NOT the per-version
    /// commit timestamp. This mirrors the existing `search_in_range` HNSW
    /// convention, but differs from BM25 (which uses `versioned.timestamp`,
    /// the per-version commit time). For vectors that are inserted once and
    /// never updated — the common case — both interpretations coincide.
    /// Closing the BM25/vector convention gap is a separate issue (vectors
    /// would need a "latest update visible at as_of_ts" timestamp), out of
    /// scope for #2370.
    ///
    /// Like `search_at_with_sources`, this intentionally does NOT use the
    /// inline-meta fast path: historical correctness requires the version
    /// chain because `InlineMeta::source_ref` reflects current state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_at_in_range_with_sources(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        as_of_ts: u64,
        start_ts: u64,
        end_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        let _refresh_guard = self.db.refresh_query_guard();
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

        let candidates = {
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            backend.search_at(query, k, as_of_ts)
        };

        let mut matches: Vec<VectorMatchWithSource> = Vec::with_capacity(candidates.len());
        for (vid, score) in candidates {
            if let Some((key, record)) =
                self.find_vector_record_at(branch_id, space, collection, vid, as_of_ts)?
            {
                // Intersection post-filter: drop candidates whose creation
                // timestamp falls outside [start_ts, end_ts]. Inclusive on
                // both ends to match the BM25 in-range contract.
                if record.created_at < start_ts || record.created_at > end_ts {
                    continue;
                }
                matches.push(VectorMatchWithSource::new(
                    key,
                    score,
                    None, // substrate doesn't need metadata; matches search_with_sources
                    record.source_ref,
                    record.version,
                ));
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

    /// Temporal search returning results with source references (internal).
    ///
    /// Like `search_with_sources` but resolves candidates via the version
    /// chain at `as_of_ts`. Used by hybrid retrieval to attribute temporal
    /// matches back to their originating user space.
    ///
    /// Note: this intentionally does NOT use the inline-meta fast path.
    /// `InlineMeta::source_ref` reflects the *current* state of a vector,
    /// not the as-of state. `VectorRecord::update_with_source` exists, so
    /// the historical `source_ref` must come from the version chain to be
    /// correct under point-in-time queries.
    pub(crate) fn search_at_with_sources(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        as_of_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        let _refresh_guard = self.db.refresh_query_guard();
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

        let candidates = {
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            backend.search_at(query, k, as_of_ts)
        };

        let mut matches: Vec<VectorMatchWithSource> = Vec::with_capacity(candidates.len());
        for (vid, score) in candidates {
            if let Some((key, record)) =
                self.find_vector_record_at(branch_id, space, collection, vid, as_of_ts)?
            {
                matches.push(VectorMatchWithSource::new(
                    key,
                    score,
                    None, // substrate doesn't need metadata; matches search_with_sources
                    record.source_ref,
                    record.version,
                ));
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
