use super::{
    bulk_delete_effect, commit_receipt, upsert_effect, usize_to_u64, BatchItem, BatchVectorEntry,
    CommitOutcome, EngineVectorBulkDeleteOutcome, EngineVectorCollectionInfo,
    EngineVectorCollectionName, EngineVectorDistanceMetric, EngineVectorEmbedding,
    EngineVectorEntry, EngineVectorFilter, EngineVectorFilterCondition, EngineVectorFilterOp,
    EngineVectorHistory, EngineVectorHistoryRow, EngineVectorIndexDiagnostics, EngineVectorKey,
    EngineVectorKeyPage, EngineVectorMetadata, EngineVectorMetadataPatch, EngineVectorScalar,
    EngineVectorSearchMatch, EngineVectorUpsertEntry, EngineVectorVersionedEntry, ExecutorError,
    ExecutorResult, MutationEffect, Output, OutputVectorCollectionInfo,
    OutputVectorIndexArtifactSource, OutputVectorIndexDiagnostics, PageInfo, Timestamp,
    VectorBatchGetItemResult, VectorBatchItemResult, VectorData, VectorDistanceMetric,
    VectorFilterOp, VectorHistoryItem, VectorHistoryResult, VectorMatch, VectorMetadataFilter,
    VectorScalar, VectorService, VectorVersionedData,
};

pub(super) fn vector_collection(name: String) -> ExecutorResult<EngineVectorCollectionName> {
    EngineVectorCollectionName::new(name).map_err(ExecutorError::from)
}

pub(super) fn vector_key(key: String) -> ExecutorResult<EngineVectorKey> {
    EngineVectorKey::new(key).map_err(ExecutorError::from)
}

pub(super) fn optional_vector_key(key: Option<String>) -> ExecutorResult<Option<EngineVectorKey>> {
    key.map(vector_key).transpose()
}

pub(super) fn vector_embedding(vector: Vec<f64>) -> ExecutorResult<EngineVectorEmbedding> {
    EngineVectorEmbedding::from_wire(vector).map_err(ExecutorError::from)
}

/// Builds an engine embedding for a *query* vector. Like a stored embedding it
/// is accepted at wire (f64) precision and validated before narrowing, so a
/// query component that underflows or overflows f32 is rejected rather than
/// silently searching against the zero vector.
pub(super) fn query_embedding(query: Vec<f64>) -> ExecutorResult<EngineVectorEmbedding> {
    EngineVectorEmbedding::from_wire(query).map_err(ExecutorError::from)
}

pub(super) fn optional_vector_metadata(
    metadata: Option<serde_json::Value>,
) -> ExecutorResult<Option<EngineVectorMetadata>> {
    metadata
        .map(EngineVectorMetadata::new)
        .transpose()
        .map_err(ExecutorError::from)
}

pub(super) fn vector_metadata_patch(
    value: serde_json::Value,
) -> ExecutorResult<EngineVectorMetadataPatch> {
    EngineVectorMetadataPatch::new(value).map_err(ExecutorError::from)
}

pub(super) const fn engine_vector_metric(
    metric: VectorDistanceMetric,
) -> EngineVectorDistanceMetric {
    match metric {
        VectorDistanceMetric::Cosine => EngineVectorDistanceMetric::Cosine,
        VectorDistanceMetric::Euclidean => EngineVectorDistanceMetric::Euclidean,
        VectorDistanceMetric::DotProduct => EngineVectorDistanceMetric::DotProduct,
    }
}

pub(super) const fn output_vector_metric(
    metric: EngineVectorDistanceMetric,
) -> VectorDistanceMetric {
    match metric {
        EngineVectorDistanceMetric::Cosine => VectorDistanceMetric::Cosine,
        EngineVectorDistanceMetric::Euclidean => VectorDistanceMetric::Euclidean,
        EngineVectorDistanceMetric::DotProduct => VectorDistanceMetric::DotProduct,
    }
}

pub(super) fn engine_vector_scalar(value: VectorScalar) -> EngineVectorScalar {
    match value {
        VectorScalar::Null => EngineVectorScalar::Null,
        VectorScalar::Bool(value) => EngineVectorScalar::Bool(value),
        VectorScalar::Number(value) => EngineVectorScalar::Number(value),
        VectorScalar::String(value) => EngineVectorScalar::String(value),
    }
}

pub(super) const fn engine_vector_filter_op(op: VectorFilterOp) -> EngineVectorFilterOp {
    match op {
        VectorFilterOp::Eq => EngineVectorFilterOp::Eq,
    }
}

pub(super) fn vector_filter(filter: VectorMetadataFilter) -> ExecutorResult<EngineVectorFilter> {
    let conditions = filter
        .into_conditions()
        .into_iter()
        .map(|condition| {
            let (field, op, value) = condition.into_parts();
            EngineVectorFilterCondition::new(
                field,
                engine_vector_filter_op(op),
                engine_vector_scalar(value),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(EngineVectorFilter::from_conditions(conditions))
}

pub(super) fn vector_upsert_entry(
    entry: BatchVectorEntry,
) -> ExecutorResult<EngineVectorUpsertEntry> {
    let (key, vector, metadata) = entry.into_parts();
    Ok(EngineVectorUpsertEntry::new(
        vector_key(key)?,
        vector_embedding(vector)?,
        optional_vector_metadata(metadata)?,
    ))
}

pub(super) fn vector_collection_info(
    info: &EngineVectorCollectionInfo,
) -> OutputVectorCollectionInfo {
    OutputVectorCollectionInfo::new(
        info.name().as_str().to_owned(),
        usize_to_u64(info.config().dimension()),
        output_vector_metric(info.config().metric()),
        info.count(),
    )
}

pub(super) fn require_vector_collection_info(
    service: &mut VectorService,
    collection: &EngineVectorCollectionName,
) -> ExecutorResult<EngineVectorCollectionInfo> {
    service.collection_info(collection)?.ok_or_else(|| {
        ExecutorError::not_found(
            "not_found.engine.vector_collection",
            "vector collection does not exist",
        )
    })
}

pub(super) fn vector_dimension_mismatch_error(expected: usize, actual: usize) -> ExecutorError {
    ExecutorError::invalid_input(
        "invalid_argument.executor.vector_dimension",
        format!("vector dimension mismatch: expected {expected}, got {actual}"),
    )
}

pub(super) fn vector_data(entry: &EngineVectorEntry) -> VectorData {
    VectorData::new(
        entry.embedding().as_slice().to_vec(),
        entry.metadata().map(|metadata| metadata.as_inner().clone()),
    )
}

pub(super) fn vector_versioned_data(entry: &EngineVectorVersionedEntry) -> VectorVersionedData {
    VectorVersionedData::new(
        entry.key().as_str().to_owned(),
        vector_data(entry.entry()),
        entry.version().as_u64(),
        entry.timestamp().as_micros(),
        entry.vector_revision(),
    )
}

pub(super) fn vector_history_result(
    key: &EngineVectorKey,
    history: &EngineVectorHistory,
) -> VectorHistoryResult {
    VectorHistoryResult::new(
        history
            .rows()
            .iter()
            .map(|row| vector_history_item(key, row))
            .collect(),
    )
}

pub(super) fn vector_history_item(
    key: &EngineVectorKey,
    row: &EngineVectorHistoryRow,
) -> VectorHistoryItem {
    VectorHistoryItem::new(
        key.as_str().to_owned(),
        row.entry().map(vector_data),
        row.version().as_u64(),
        row.timestamp().as_micros(),
        row.vector_revision(),
        row.is_tombstone(),
    )
    .with_committed_at(row.committed_at().map(Timestamp::as_micros))
}

pub(super) fn vector_match(value: &EngineVectorSearchMatch) -> VectorMatch {
    VectorMatch::new(
        value.entry().key().as_str().to_owned(),
        value.score(),
        value
            .entry()
            .metadata()
            .map(|metadata| metadata.as_inner().clone()),
    )
}

pub(super) fn vector_index_diagnostics(
    value: &EngineVectorIndexDiagnostics,
) -> OutputVectorIndexDiagnostics {
    OutputVectorIndexDiagnostics::new(
        value.collection().to_owned(),
        value.manifest_status().to_owned(),
        value.manifest_generation(),
        usize_to_u64(value.manifest_ref_count()),
        usize_to_u64(value.manifest_inherited_ref_count()),
        usize_to_u64(value.manifest_owned_ref_count()),
        usize_to_u64(value.manifest_active_delta_count()),
        value.policy_mode().to_owned(),
        usize_to_u64(value.collection_exact_threshold()),
        usize_to_u64(value.source_flat_threshold()),
        usize_to_u64(value.source_hnsw_threshold()),
        usize_to_u64(value.overfetch_factor()),
        value.filtered_underfill_fallback(),
        usize_to_u64(value.active_delta_seal_threshold()),
        usize_to_u64(value.hnsw_memory_budget_bytes()),
        usize_to_u64(value.source_candidate_limit()),
        value.resolved_index_kind_summary().to_owned(),
        value.exact_fallback_count(),
        usize_to_u64(value.hnsw_graph_builds()),
        usize_to_u64(value.indexed_source_count()),
        usize_to_u64(value.exact_source_count()),
        usize_to_u64(value.flat_source_count()),
        usize_to_u64(value.hnsw_source_count()),
        usize_to_u64(value.active_delta_source_count()),
        usize_to_u64(value.indexed_vector_count()),
        value.derived_bytes(),
        value.last_query_used_index(),
        value
            .last_query_fallback_reason()
            .map(std::borrow::ToOwned::to_owned),
        value
            .artifact_sources()
            .iter()
            .map(|source| {
                OutputVectorIndexArtifactSource::new(
                    source.artifact_id().to_owned(),
                    source.status().to_owned(),
                    source.searched(),
                )
            })
            .collect(),
    )
}

pub(super) fn vector_key_page_output(page: &EngineVectorKeyPage) -> Output {
    Output::VectorKeyPage {
        items: page
            .keys()
            .iter()
            .map(|key| key.as_str().to_owned())
            .collect(),
        page: PageInfo::new(
            page.has_more(),
            page.cursor().map(|cursor| cursor.as_str().to_owned()),
        ),
    }
}

pub(super) fn vector_write_output(
    collection: &EngineVectorCollectionName,
    key: &EngineVectorKey,
    outcome: CommitOutcome,
    vector_revision: u64,
    created: bool,
) -> Output {
    Output::VectorWriteResult {
        collection: collection.as_str().to_owned(),
        key: key.as_str().to_owned(),
        effect: upsert_effect(!created),
        commit: commit_receipt(outcome),
        vector_revision,
    }
}

pub(super) fn vector_bulk_delete_output(
    collection: &EngineVectorCollectionName,
    outcome: EngineVectorBulkDeleteOutcome,
) -> Output {
    let commit = outcome.commit();
    Output::VectorBulkDeleteResult {
        collection: collection.as_str().to_owned(),
        effect: bulk_delete_effect(outcome.deleted_count()),
        commit: commit.map(commit_receipt),
    }
}

pub(super) fn vector_batch_item_result(
    index: u64,
    applied: bool,
    effect: MutationEffect,
    outcome: Option<CommitOutcome>,
    vector_revision: Option<u64>,
) -> BatchItem<VectorBatchItemResult> {
    BatchItem::ok(
        index,
        applied,
        Some(effect),
        outcome.map(commit_receipt),
        VectorBatchItemResult::new(vector_revision),
    )
}

pub(super) fn vector_batch_item_failed(
    index: u64,
    error: ExecutorError,
) -> BatchItem<VectorBatchItemResult> {
    BatchItem::failed(
        index,
        Some(VectorBatchItemResult::new(None)),
        error.into_status(),
    )
}

pub(super) fn vector_batch_get_item(
    index: u64,
    value: Option<VectorVersionedData>,
) -> BatchItem<VectorBatchGetItemResult> {
    if value.is_some() {
        BatchItem::ok(
            index,
            false,
            None,
            None,
            VectorBatchGetItemResult::new(value),
        )
    } else {
        BatchItem::miss(index, VectorBatchGetItemResult::not_found())
    }
}

pub(super) fn vector_batch_get_failed(
    index: u64,
    error: ExecutorError,
) -> BatchItem<VectorBatchGetItemResult> {
    BatchItem::failed(
        index,
        Some(VectorBatchGetItemResult::not_found()),
        error.into_status(),
    )
}
