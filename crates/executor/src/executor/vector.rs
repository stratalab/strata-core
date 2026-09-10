use super::{
    commit_receipt, delete_effect, empty_presence_exists_results, empty_vector_batch_get_results,
    empty_vector_batch_results, engine_vector_metric, finish_presence_exists_results,
    finish_vector_batch_get_results, finish_vector_batch_results, optional_limit,
    optional_vector_key, optional_vector_metadata, presence_exists_failed, presence_exists_item,
    query_embedding, reject_duplicate_vector_keys, require_vector_collection_info, required_usize,
    resolve_vector_or_text, update_effect, upsert_effect, usize_to_u64, vector_batch_get_failed,
    vector_batch_get_item, vector_batch_item_failed, vector_batch_item_result,
    vector_bulk_delete_output, vector_collection, vector_collection_info,
    vector_dimension_mismatch_error, vector_embedding, vector_filter, vector_history_result,
    vector_index_diagnostics, vector_key, vector_key_page_output, vector_match,
    vector_metadata_patch, vector_upsert_entry, vector_versioned_data, vector_write_output,
    BatchVectorEntry, EngineEmbeddingModelId, EngineVectorCollectionName, EngineVectorConfig,
    Executor, ExecutorError, ExecutorResult, Maybe, Output, PageInfo, VectorDistanceMetric,
    VectorIndexQueryResult, VectorMetadataFilter, DEFAULT_VECTOR_LIST_LIMIT,
};
use strata_core::Timestamp;

/// Whether a text is being stored or searched with (D10).
///
/// Instruction-tuned embedders emit different vectors for the same words
/// depending on the role, so this has to be said rather than defaulted. It was
/// a `&'static str` matched inside `embed_text_with`, which put a real decision
/// inside glue that no CI lane can execute — the mutation gate found it by
/// deleting the query arm and nothing failed.
// Ungated: the upsert and query paths name this whether or not inference is
// compiled in — without it they still refuse a `text`, and the refusal needs
// the same signature. Only `input_type` touches an inference type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EmbedPurpose {
    /// Text being stored in the collection.
    Document,
    /// Text being searched with.
    Query,
}

#[cfg(feature = "inference")]
impl EmbedPurpose {
    /// The provider-facing input role.
    pub(super) fn input_type(self) -> strata_inference::InputType {
        match self {
            Self::Document => strata_inference::InputType::Document,
            Self::Query => strata_inference::InputType::Query,
        }
    }
}

impl Executor {
    pub(super) fn execute_vector_create_collection(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        dimension: u64,
        metric: VectorDistanceMetric,
        embedding_model: Option<String>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let config = EngineVectorConfig::new(
            required_usize(
                dimension,
                "invalid_argument.executor.vector_dimension",
                "vector dimension does not fit this platform",
            )?,
            engine_vector_metric(metric),
        )?;
        // Engine validates the identifier's shape; it cannot check the model
        // exists, because it never speaks to a provider (hard rules 2-3).
        let config = match embedding_model {
            Some(model) => config.with_embedding_model(EngineEmbeddingModelId::new(model)?),
            None => config,
        };
        let mut service = self.vector_service(branch, space)?;
        Ok(Output::VectorCollectionList {
            items: vec![vector_collection_info(
                &service.create_collection(collection, config)?,
            )],
            page: PageInfo::terminal(),
        })
    }

    pub(super) fn execute_vector_delete_collection(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let mut service = self.vector_service(branch, space)?;
        Ok(Output::Bool(service.delete_collection(&collection)?))
    }

    pub(super) fn execute_vector_list_collections(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> ExecutorResult<Output> {
        let mut service = self.vector_service(branch, space)?;
        Ok(Output::VectorCollectionList {
            items: service
                .list_collections()?
                .iter()
                .map(vector_collection_info)
                .collect(),
            page: PageInfo::terminal(),
        })
    }

    pub(super) fn execute_vector_collection_stats(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let mut service = self.vector_service(branch, space)?;
        let Some(info) = service.collection_info(&collection)? else {
            return Err(ExecutorError::new(
                "not_found.engine.vector_collection",
                "vector collection does not exist",
            ));
        };
        Ok(Output::VectorCollectionList {
            items: vec![vector_collection_info(&info)],
            page: PageInfo::terminal(),
        })
    }

    pub(super) fn execute_vector_set_embedding_model(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        model: String,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let model = EngineEmbeddingModelId::new(model)?;
        let mut service = self.vector_service(branch, space)?;
        Ok(Output::VectorCollectionList {
            items: vec![vector_collection_info(
                &service.declare_embedding_model(&collection, model)?,
            )],
            page: PageInfo::terminal(),
        })
    }

    pub(super) fn execute_vector_count(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.vector_service(branch, space)?;
        let count = if let Some(as_of) = as_of {
            service.count_at(&collection, as_of)?
        } else {
            service.count(&collection)?
        };
        Ok(Output::Uint(count))
    }

    pub(super) fn execute_vector_sample(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        count: Option<u64>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let count = optional_limit(count)?.unwrap_or(10);
        let mut service = self.vector_service(branch, space)?;
        let (total_count, entries) = service.sample(&collection, count)?;
        Ok(Output::VectorSampleResult {
            total_count,
            items: entries.iter().map(vector_versioned_data).collect(),
            page: PageInfo::terminal(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Embeds text with the model the collection was created with (D10).
    ///
    /// **This is orchestration in the executor, which is normally forbidden**
    /// (CLAUDE.md rule 7: engine owns semantics, executor is a thin adapter).
    /// It is here because engine cannot import inference — hard rules 2 and 3,
    /// and `strata-engine` has no inference dependency — and the layer that was
    /// meant to mediate this, intelligence, is deferred with no target release
    /// (#3171). Recorded as an explicit exception in
    /// `docs/design/inference-developer-experience.md` (D10).
    ///
    /// The embedding happens **before** the write. It can fail — no key, no
    /// network, model not installed — and when it does, nothing is written.
    /// The store that follows is the single commit.
    ///
    /// `as_of` is the snapshot the caller is reading — `None` for a write,
    /// which always lands at the head of the branch. The model is read from
    /// the same snapshot the vectors will be, so a text search at a timestamp
    /// is embedded with the model the collection recorded *then*, and refused
    /// if it recorded none; the engine says why.
    #[allow(clippy::needless_pass_by_value)]
    fn embed_with_collection_model(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: &EngineVectorCollectionName,
        text: String,
        purpose: EmbedPurpose,
        as_of: Option<Timestamp>,
    ) -> ExecutorResult<Vec<f64>> {
        // What a missing model means is the engine's call
        // (`failed_precondition.engine.embedding_model_missing`, naming the
        // command that declares one); this only carries the answer to the
        // provider.
        let model = {
            let mut service = self.vector_service(branch, space)?;
            match as_of {
                Some(as_of) => service.recorded_embedding_model_at(collection, as_of)?,
                None => service.recorded_embedding_model(collection)?,
            }
        };
        self.embed_text_with(collection, model.as_str(), text, purpose)
    }

    // One function with a feature-split body rather than two `#[cfg]`
    // variants: cargo-mutants mutates source text without evaluating cfg, so a
    // second fn for the no-inference build would earn mutants that the
    // featured mutation lane never compiles and no test can kill. A block
    // inside the body is not a mutation site.
    #[cfg_attr(
        not(feature = "inference"),
        allow(clippy::unused_self, clippy::needless_pass_by_value)
    )]
    fn embed_text_with(
        &mut self,
        collection: &EngineVectorCollectionName,
        model: &str,
        text: String,
        purpose: EmbedPurpose,
    ) -> ExecutorResult<Vec<f64>> {
        #[cfg(feature = "inference")]
        {
            use strata_inference::{EmbedInput, EmbeddingsRequest};

            let request = EmbeddingsRequest {
                input: EmbedInput::One(text),
                dimensions: None,
                normalize: None,
                // Instruction-tuned embedders produce different vectors for a
                // stored document and a search query; saying which this is
                // stops that difference from being silent. The mapping is a
                // pure function with a truth table — inside this method it
                // was a decision buried in glue that CI cannot execute.
                input_type: Some(purpose.input_type()),
                instruction: None,
            };
            // The spec came from the collection's record, not the command;
            // a resolution refusal names the collection so the caller knows
            // where to look (#3226). A failure past resolution carries no
            // resolver answer and so no collection either.
            let response = self
                .inference
                .embeddings(model, &request)
                .map_err(|error| {
                    ExecutorError::from_inference(&error, Some(collection.as_str()))
                })?;
            let item = response.data.into_iter().next().ok_or_else(|| {
                ExecutorError::new(
                    "inference.provider_malformed_response",
                    "the embedding provider returned no vector for the text",
                )
            })?;
            Ok(item.embedding.into_iter().map(f64::from).collect())
        }
        #[cfg(not(feature = "inference"))]
        {
            // The parameters exist so both builds have one signature; without
            // an inference runtime there is nothing to hand them to.
            let _ = (collection, model, text, purpose);
            Err(ExecutorError::new(
                "inference.unsupported_operation",
                "this build has no inference support, so `text` cannot be \
                 embedded; pass a vector directly",
            ))
        }
    }

    pub(super) fn execute_vector_upsert(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        key: String,
        vector: Vec<f64>,
        text: Option<String>,
        metadata: Option<serde_json::Value>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let key = vector_key(key)?;
        let vector = resolve_vector_or_text(vector, text, |text| {
            // A write lands at the head of the branch, so the model that
            // governs it is the one recorded now.
            self.embed_with_collection_model(
                branch,
                space,
                &collection,
                text,
                EmbedPurpose::Document,
                None,
            )
        })?;
        let embedding = vector_embedding(vector)?;
        let metadata = optional_vector_metadata(metadata)?;
        let mut service = self.vector_service(branch, space)?;
        let outcome = service.upsert(collection.clone(), key.clone(), embedding, metadata)?;
        Ok(vector_write_output(
            &collection,
            &key,
            outcome.commit(),
            outcome.vector_revision(),
            outcome.created(),
        ))
    }

    pub(super) fn execute_vector_get(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        key: String,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let key = vector_key(key)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.vector_service(branch, space)?;
        let value = if let Some(as_of) = as_of {
            service.get_at(&collection, &key, as_of)?
        } else {
            service.get_versioned(&collection, &key)?
        };
        Ok(Output::VectorData(Maybe::from_option(
            value.as_ref().map(vector_versioned_data),
        )))
    }

    pub(super) fn execute_vector_history(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        key: String,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let key = vector_key(key)?;
        let mut service = self.vector_service(branch, space)?;
        Ok(Output::VectorVersionHistory(
            service
                .history(&collection, &key)?
                .as_ref()
                .map(|history| vector_history_result(&key, history)),
        ))
    }

    pub(super) fn execute_vector_exists(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        key: String,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let key = vector_key(key)?;
        let mut service = self.vector_service(branch, space)?;
        Ok(Output::Bool(service.exists(&collection, &key)?))
    }

    pub(super) fn execute_vector_batch_exists(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        keys: Vec<String>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let mut service = self.vector_service(branch, space)?;
        // The `?` validates the collection exists (surfacing not-found) before
        // any per-key work, so an empty or all-invalid key list still reports a
        // missing collection; the returned info is unused, so it is discarded.
        let _ = require_vector_collection_info(&mut service, &collection)?;
        let mut results = empty_presence_exists_results(keys.len());
        let mut valid_keys = Vec::with_capacity(keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            match vector_key(key) {
                Ok(key) => valid_keys.push((index, key)),
                Err(error) => {
                    results[index] = Some(presence_exists_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_keys.is_empty() {
            return Ok(Output::VectorBatchExistsResults(
                finish_presence_exists_results(results),
            ));
        }
        let keys = valid_keys
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let exists = service.batch_exists(&collection, &keys)?;
        for ((index, _), exists) in valid_keys.into_iter().zip(exists) {
            results[index] = Some(presence_exists_item(usize_to_u64(index), exists));
        }
        Ok(Output::VectorBatchExistsResults(
            finish_presence_exists_results(results),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_vector_list_keys(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        prefix: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let prefix = optional_vector_key(prefix)?;
        let cursor = optional_vector_key(cursor)?;
        let limit = optional_limit(limit)?.unwrap_or(DEFAULT_VECTOR_LIST_LIMIT);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.vector_service(branch, space)?;
        let page = if let Some(as_of) = as_of {
            service.list_keys_at(&collection, prefix.as_ref(), cursor.as_ref(), limit, as_of)?
        } else {
            service.list_keys(&collection, prefix.as_ref(), cursor.as_ref(), limit)?
        };
        Ok(vector_key_page_output(&page))
    }

    pub(super) fn execute_vector_scan(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        start: Option<String>,
        limit: Option<u64>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let start = optional_vector_key(start)?;
        let limit = optional_limit(limit)?;
        let mut service = self.vector_service(branch, space)?;
        // Fetch one extra row to detect truncation and report has_more/cursor
        // honestly, like KvScan. The continuation cursor is the first unreturned
        // key; the inclusive start resumes with neither a gap nor an overlap.
        // `scan` validates the collection, so a missing one surfaces not-found
        // even for an empty page.
        let mut rows = service.scan(
            &collection,
            start.as_ref(),
            limit.map(|limit| limit.saturating_add(1)),
        )?;
        let page = match limit {
            Some(limit) if rows.len() > limit => {
                let cursor = rows[limit].key().as_str().to_owned();
                rows.truncate(limit);
                PageInfo::new(true, Some(cursor))
            }
            _ => PageInfo::terminal(),
        };
        Ok(Output::VectorScanResult {
            items: rows.iter().map(vector_versioned_data).collect(),
            page,
        })
    }

    pub(super) fn execute_vector_update_metadata(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        key: String,
        patch: serde_json::Value,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let key = vector_key(key)?;
        let patch = vector_metadata_patch(patch)?;
        let mut service = self.vector_service(branch, space)?;
        let outcome = service.update_metadata(&collection, key.clone(), &patch)?;
        Ok(Output::VectorMetadataUpdateResult {
            collection: collection.as_str().to_owned(),
            key: outcome.key().as_str().to_owned(),
            effect: update_effect(outcome.updated()),
            commit: outcome.commit().map(commit_receipt),
            vector_revision: outcome.vector_revision(),
        })
    }

    pub(super) fn execute_vector_delete(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        key: String,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let key = vector_key(key)?;
        let mut service = self.vector_service(branch, space)?;
        let outcome = service.delete(&collection, key)?;
        Ok(Output::VectorDeleteResult {
            collection: collection.as_str().to_owned(),
            key: outcome.key().as_str().to_owned(),
            effect: delete_effect(outcome.deleted()),
            commit: outcome.commit().map(commit_receipt),
        })
    }

    pub(super) fn execute_vector_delete_by_filter(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        filter: VectorMetadataFilter,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let filter = vector_filter(filter)?;
        let mut service = self.vector_service(branch, space)?;
        let outcome = service.delete_by_filter(&collection, &filter)?;
        Ok(vector_bulk_delete_output(&collection, outcome))
    }

    pub(super) fn execute_vector_delete_all(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let mut service = self.vector_service(branch, space)?;
        let outcome = service.delete_all(&collection)?;
        Ok(vector_bulk_delete_output(&collection, outcome))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_vector_query(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        query: Vec<f64>,
        text: Option<String>,
        k: u64,
        filter: Option<VectorMetadataFilter>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        // It comes before the embedding too: a text query is embedded with the
        // model the collection recorded at the snapshot being searched, so the
        // snapshot has to be known first.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        // A text query is embedded with the collection's own model, so a
        // caller never has to know which model wrote the collection — and
        // cannot pick the wrong one.
        let query = resolve_vector_or_text(query, text, |text| {
            self.embed_with_collection_model(
                branch,
                space,
                &collection,
                text,
                EmbedPurpose::Query,
                as_of,
            )
        })?;
        let query = query_embedding(query)?;
        let k = required_usize(
            k,
            "invalid_argument.executor.vector_limit",
            "vector match limit does not fit this platform",
        )?;
        let filter = filter.map(vector_filter).transpose()?;
        let mut service = self.vector_service(branch, space)?;
        let result = if let Some(as_of) = as_of {
            service.query_at(&collection, &query, k, filter.as_ref(), as_of)?
        } else {
            service.query(&collection, &query, k, filter.as_ref())?
        };
        Ok(Output::VectorMatches(
            result.matches().iter().map(vector_match).collect(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_vector_index_query(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        query: Vec<f64>,
        k: u64,
        filter: Option<VectorMetadataFilter>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let query = query_embedding(query)?;
        let k = required_usize(
            k,
            "invalid_argument.executor.vector_limit",
            "vector match limit does not fit this platform",
        )?;
        let filter = filter.map(vector_filter).transpose()?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.vector_service(branch, space)?;
        let (result, diagnostics) = if let Some(as_of) = as_of {
            service.query_at_with_index_diagnostics(
                &collection,
                &query,
                k,
                filter.as_ref(),
                as_of,
            )?
        } else {
            service.query_with_index_diagnostics(&collection, &query, k, filter.as_ref())?
        };
        Ok(Output::VectorIndexQuery(VectorIndexQueryResult::new(
            result.matches().iter().map(vector_match).collect(),
            vector_index_diagnostics(&diagnostics),
        )))
    }

    pub(super) fn execute_vector_batch_upsert(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        entries: Vec<BatchVectorEntry>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let mut service = self.vector_service(branch, space)?;
        let info = require_vector_collection_info(&mut service, &collection)?;
        let expected_dimension = info.config().dimension();
        let mut results = empty_vector_batch_results(entries.len());
        let mut valid_entries = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
            let key = entry.key().to_owned();
            match vector_upsert_entry(entry) {
                Ok(entry) if entry.embedding().dimension() == expected_dimension => {
                    valid_entries.push((index, entry, key));
                }
                Ok(entry) => {
                    results[index] = Some(vector_batch_item_failed(
                        usize_to_u64(index),
                        vector_dimension_mismatch_error(
                            expected_dimension,
                            entry.embedding().dimension(),
                        ),
                    ));
                }
                Err(error) => {
                    results[index] = Some(vector_batch_item_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_entries.is_empty() {
            return Ok(Output::VectorBatchUpsertResults(
                finish_vector_batch_results(results),
            ));
        }
        // Two items targeting the same key are rejected, matching KV's
        // whole-batch duplicate rule rather than silently applying last-wins.
        reject_duplicate_vector_keys(valid_entries.iter().map(|(_, _, key)| key.as_str()))?;
        let entries = valid_entries
            .iter()
            .map(|(_, entry, _)| entry.clone())
            .collect::<Vec<_>>();
        let outcome = service.batch_upsert(&collection, &entries)?;
        for ((index, _, _), (revision, created)) in valid_entries.into_iter().zip(
            outcome
                .vector_revisions()
                .iter()
                .copied()
                .zip(outcome.created().iter().copied()),
        ) {
            results[index] = Some(vector_batch_item_result(
                usize_to_u64(index),
                true,
                upsert_effect(!created),
                outcome.commit(),
                Some(revision),
            ));
        }
        Ok(Output::VectorBatchUpsertResults(
            finish_vector_batch_results(results),
        ))
    }

    pub(super) fn execute_vector_batch_get(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        keys: Vec<String>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let mut service = self.vector_service(branch, space)?;
        // The `?` validates the collection exists (surfacing not-found); the
        // returned info is unused on this path, so the value is discarded.
        let _ = require_vector_collection_info(&mut service, &collection)?;
        let mut results = empty_vector_batch_get_results(keys.len());
        let mut valid_keys = Vec::with_capacity(keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            match vector_key(key) {
                Ok(key) => valid_keys.push((index, key)),
                Err(error) => {
                    results[index] = Some(vector_batch_get_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_keys.is_empty() {
            return Ok(Output::VectorBatchGetResults(
                finish_vector_batch_get_results(results),
            ));
        }
        let keys = valid_keys
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let outcome = service.batch_get(&collection, &keys)?;
        for ((index, _), entry) in valid_keys.into_iter().zip(outcome.entries()) {
            results[index] = Some(vector_batch_get_item(
                usize_to_u64(index),
                entry.as_ref().map(vector_versioned_data),
            ));
        }
        Ok(Output::VectorBatchGetResults(
            finish_vector_batch_get_results(results),
        ))
    }

    pub(super) fn execute_vector_batch_delete(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        collection: String,
        keys: Vec<String>,
    ) -> ExecutorResult<Output> {
        let collection = vector_collection(collection)?;
        let mut service = self.vector_service(branch, space)?;
        // The `?` validates the collection exists (surfacing not-found); the
        // returned info is unused on this path, so the value is discarded.
        let _ = require_vector_collection_info(&mut service, &collection)?;
        let mut results = empty_vector_batch_results(keys.len());
        let mut valid_keys = Vec::with_capacity(keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            let raw = key.clone();
            match vector_key(key) {
                Ok(key) => valid_keys.push((index, key, raw)),
                Err(error) => {
                    results[index] = Some(vector_batch_item_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_keys.is_empty() {
            return Ok(Output::VectorBatchDeleteResults(
                finish_vector_batch_results(results),
            ));
        }
        reject_duplicate_vector_keys(valid_keys.iter().map(|(_, _, raw)| raw.as_str()))?;
        let keys = valid_keys
            .iter()
            .map(|(_, key, _)| key.clone())
            .collect::<Vec<_>>();
        let outcome = service.batch_delete(&collection, &keys)?;
        for ((index, _, _), deleted) in valid_keys
            .into_iter()
            .zip(outcome.deleted().iter().copied())
        {
            results[index] = Some(vector_batch_item_result(
                usize_to_u64(index),
                deleted,
                delete_effect(deleted),
                deleted.then(|| outcome.commit()).flatten(),
                None,
            ));
        }
        Ok(Output::VectorBatchDeleteResults(
            finish_vector_batch_results(results),
        ))
    }
}

#[cfg(all(test, feature = "inference"))]
mod embed_purpose_tests {
    use super::EmbedPurpose;
    use strata_inference::InputType;

    /// The document/query distinction, which the mutation gate caught as
    /// untested: deleting the query arm changed nothing observable, because
    /// the only test that reaches this code needs a real model and so cannot
    /// run in CI.
    ///
    /// Storing and searching must NOT map to the same role — that is the whole
    /// reason the parameter exists.
    #[test]
    fn storing_and_searching_map_to_different_roles() {
        assert_eq!(EmbedPurpose::Document.input_type(), InputType::Document);
        assert_eq!(EmbedPurpose::Query.input_type(), InputType::Query);
        assert_ne!(
            EmbedPurpose::Document.input_type(),
            EmbedPurpose::Query.input_type(),
            "collapsing these would silently embed queries as documents"
        );
    }
}
