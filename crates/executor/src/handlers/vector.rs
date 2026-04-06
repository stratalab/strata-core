//! Vector command handlers (7 MVP).
//!
//! MVP: upsert, get, delete, search, create_collection, delete_collection, list_collections

use std::sync::Arc;

use strata_core::Value;

use crate::bridge::{
    extract_version, from_engine_metric, is_internal_collection, serde_json_to_value_public,
    to_core_branch_id, to_engine_filter, to_engine_metric, validate_key,
    validate_not_internal_collection, validate_vector, value_to_serde_json_public, Primitives,
};
use crate::convert::convert_result;
use crate::types::{
    BranchId, CollectionInfo, DistanceMetric, MetadataFilter, VectorData, VectorMatch,
    VersionedVectorData,
};
use crate::{Output, Result};

/// Convert an engine `VectorResult<T>` to an executor `Result<T>`.
///
/// Accepts the `branch_id` so that error messages reference the actual branch
/// instead of a placeholder UUID.
fn convert_vector_result<T>(
    r: std::result::Result<T, strata_vector::VectorError>,
    branch_id: strata_core::BranchId,
) -> Result<T> {
    convert_result(r.map_err(|e| e.into_strata_error(branch_id)))
}

/// Convert engine `VectorEntry` to executor `VersionedVectorData`.
fn to_versioned_vector_data(
    entry: &strata_vector::VectorEntry,
    version: u64,
    timestamp: u64,
) -> Result<VersionedVectorData> {
    let metadata = entry
        .metadata
        .clone()
        .map(serde_json_to_value_public)
        .transpose()
        .map_err(crate::Error::from)?;
    Ok(VersionedVectorData {
        key: entry.key.clone(),
        data: VectorData {
            embedding: entry.embedding.clone(),
            metadata,
        },
        version,
        timestamp,
    })
}

/// Enrich a DimensionMismatch error with collection context.
///
/// Identifies well-known embedding model dimensions and adds the collection name.
fn enrich_dimension_error(collection: &str, err: crate::Error) -> crate::Error {
    match err {
        crate::Error::DimensionMismatch {
            expected,
            actual,
            hint: None,
        } => {
            let model_hint = match expected {
                384 => "MiniLM (384-dim)",
                768 => "BERT/MPNet (768-dim)",
                1024 => "Large (1024-dim)",
                1536 => "OpenAI ada-002 (1536-dim)",
                3072 => "OpenAI text-embedding-3-large (3072-dim)",
                _ => "",
            };
            let actual_model = match actual {
                384 => " (MiniLM)",
                768 => " (BERT/MPNet)",
                1024 => " (Large)",
                1536 => " (OpenAI ada-002)",
                3072 => " (OpenAI text-embedding-3-large)",
                _ => "",
            };
            let hint = if model_hint.is_empty() {
                format!(
                    "Collection \"{}\" expects {}-dim vectors. Your vector is {}-dim{}.",
                    collection, expected, actual, actual_model
                )
            } else {
                format!(
                    "Collection \"{}\" uses {}. Your vector appears to be {}-dim{}.",
                    collection, model_hint, actual, actual_model
                )
            };
            crate::Error::DimensionMismatch {
                expected,
                actual,
                hint: Some(hint),
            }
        }
        other => other,
    }
}

/// Convert engine `VectorMatch` metadata to executor `VectorMatch`.
fn to_vector_match(m: strata_vector::VectorMatch) -> Result<VectorMatch> {
    let metadata = m
        .metadata
        .map(serde_json_to_value_public)
        .transpose()
        .map_err(crate::Error::from)?;
    Ok(VectorMatch {
        key: m.key,
        score: m.score,
        metadata,
    })
}

// =============================================================================
// Individual Handlers (7 MVP)
// =============================================================================

/// Handle VectorUpsert command.
pub fn vector_upsert(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
    vector: Vec<f32>,
    metadata: Option<Value>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;
    convert_result(validate_vector(&vector, &p.limits))?;

    let json_metadata = metadata
        .map(value_to_serde_json_public)
        .transpose()
        .map_err(crate::Error::from)?;
    let version = convert_vector_result(
        p.vector
            .insert(branch_id, &space, &collection, &key, &vector, json_metadata),
        branch_id,
    )
    .map_err(|e| enrich_dimension_error(&collection, e))?;
    Ok(Output::VectorWriteResult {
        collection,
        key,
        version: extract_version(&version),
    })
}

/// Handle VectorGet command.
pub fn vector_get(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;

    let result = convert_vector_result(
        p.vector.get(branch_id, &space, &collection, &key),
        branch_id,
    )?;
    match result {
        Some(versioned) => {
            let version = extract_version(&versioned.version);
            let timestamp: u64 = versioned.timestamp.into();
            let data = to_versioned_vector_data(&versioned.value, version, timestamp)?;
            Ok(Output::VectorData(Some(data)))
        }
        None => Ok(Output::VectorData(None)),
    }
}

/// Handle VectorGet with as_of timestamp (time-travel read).
pub fn vector_get_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
    as_of_ts: u64,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;

    let result = convert_vector_result(
        p.vector
            .get_at(branch_id, &space, &collection, &key, as_of_ts),
        branch_id,
    )?;
    match result {
        Some(entry) => {
            let version = extract_version(&entry.version);
            let metadata = entry
                .metadata
                .map(serde_json_to_value_public)
                .transpose()
                .map_err(crate::Error::from)?;
            Ok(Output::VectorData(Some(VersionedVectorData {
                key: entry.key,
                data: VectorData {
                    embedding: entry.embedding,
                    metadata,
                },
                version,
                timestamp: as_of_ts,
            })))
        }
        None => Ok(Output::VectorData(None)),
    }
}

/// Handle VectorGetv command — full version history for a vector key.
pub fn vector_getv(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;

    let result = convert_vector_result(
        p.vector.getv(branch_id, &space, &collection, &key),
        branch_id,
    )?;

    let mapped = result
        .map(|history| {
            history
                .into_versions()
                .into_iter()
                .map(|v| {
                    // Use the outer storage version (matches vector_get), not the
                    // inner VectorRecord counter.
                    let version = extract_version(&v.version);
                    let timestamp: u64 = v.timestamp.into();
                    to_versioned_vector_data(&v.value, version, timestamp)
                })
                .collect::<Result<Vec<VersionedVectorData>>>()
        })
        .transpose()?;

    Ok(Output::VectorVersionHistory(mapped))
}

/// Handle VectorDelete command.
pub fn vector_delete(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;
    let existed = convert_vector_result(
        p.vector.delete(branch_id, &space, &collection, &key),
        branch_id,
    )?;
    Ok(Output::VectorDeleteResult {
        collection,
        key,
        deleted: existed,
    })
}

/// Handle VectorQuery command.
#[allow(clippy::too_many_arguments)]
pub fn vector_query(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    query: Vec<f32>,
    k: u64,
    filter: Option<Vec<MetadataFilter>>,
    _metric: Option<DistanceMetric>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let engine_filter = filter.as_ref().and_then(|f| to_engine_filter(f));
    let matches = convert_vector_result(
        p.vector.search(
            branch_id,
            &space,
            &collection,
            &query,
            k as usize,
            engine_filter,
        ),
        branch_id,
    )
    .map_err(|e| enrich_dimension_error(&collection, e))?;

    let results: Result<Vec<VectorMatch>> = matches.into_iter().map(to_vector_match).collect();
    Ok(Output::VectorMatches(results?))
}

/// Handle VectorCreateCollection command.
pub fn vector_create_collection(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    dimension: u64,
    metric: DistanceMetric,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let storage_dtype = p.db.config().vector_storage_dtype();
    let config = convert_result(strata_core::primitives::VectorConfig::new_with_dtype(
        dimension as usize,
        to_engine_metric(metric),
        storage_dtype,
    ))?;
    let versioned = convert_vector_result(
        p.vector
            .create_collection(branch_id, &space, &collection, config),
        branch_id,
    )?;
    Ok(Output::Version(extract_version(&versioned.version)))
}

/// Handle VectorDeleteCollection command.
pub fn vector_delete_collection(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    // Try to delete - returns error if not found, which we convert to false
    match p.vector.delete_collection(branch_id, &space, &collection) {
        Ok(()) => Ok(Output::Bool(true)),
        Err(strata_vector::VectorError::CollectionNotFound { .. }) => Ok(Output::Bool(false)),
        Err(e) => Err(e.into_strata_error(branch_id).into()),
    }
}

/// Handle VectorListCollections command.
pub fn vector_list_collections(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let collections =
        convert_vector_result(p.vector.list_collections(branch_id, &space), branch_id)?;

    // Filter out internal collections (starting with '_')
    let infos: Vec<CollectionInfo> = collections
        .into_iter()
        .filter(|info| !is_internal_collection(&info.name))
        .map(|info| {
            let (index_type, memory_bytes) = p
                .vector
                .collection_backend_stats(branch_id, &space, &info.name)
                .map(|(it, mem)| (Some(it.to_string()), Some(mem as u64)))
                .unwrap_or((None, None));
            CollectionInfo {
                name: info.name,
                dimension: info.config.dimension,
                metric: from_engine_metric(info.config.metric),
                count: info.count as u64,
                index_type,
                memory_bytes,
            }
        })
        .collect();
    Ok(Output::VectorCollectionList(infos))
}

/// Handle VectorCollectionStats command.
pub fn vector_collection_stats(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let collections =
        convert_vector_result(p.vector.list_collections(branch_id, &space), branch_id)?;
    let collection_names: Vec<String> = collections.iter().map(|c| c.name.clone()).collect();
    let info = collections
        .into_iter()
        .find(|c| c.name == collection)
        .ok_or_else(|| {
            let hint =
                crate::suggest::format_hint("collections", &collection_names, &collection, 2);
            crate::Error::CollectionNotFound {
                collection: collection.clone(),
                hint,
            }
        })?;

    let (index_type, memory_bytes) = p
        .vector
        .collection_backend_stats(branch_id, &space, &info.name)
        .map(|(it, mem)| (Some(it.to_string()), Some(mem as u64)))
        .unwrap_or((None, None));

    let stats = CollectionInfo {
        name: info.name,
        dimension: info.config.dimension,
        metric: from_engine_metric(info.config.metric),
        count: info.count as u64,
        index_type,
        memory_bytes,
    };
    Ok(Output::VectorCollectionList(vec![stats]))
}

/// Handle VectorBatchUpsert command.
pub fn vector_batch_upsert(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    entries: Vec<crate::types::BatchVectorEntry>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let mut engine_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        convert_result(validate_key(&entry.key))?;
        convert_result(validate_vector(&entry.vector, &p.limits))?;
        let json_metadata = entry
            .metadata
            .map(value_to_serde_json_public)
            .transpose()
            .map_err(crate::Error::from)?;
        engine_entries.push((entry.key, entry.vector, json_metadata));
    }

    let versions = convert_vector_result(
        p.vector
            .batch_insert(branch_id, &space, &collection, engine_entries),
        branch_id,
    )
    .map_err(|e| enrich_dimension_error(&collection, e))?;

    let version_nums: Vec<u64> = versions.iter().map(extract_version).collect();
    Ok(Output::Versions(version_nums))
}

/// Handle VectorBatchGet command — get multiple vectors by key.
pub fn vector_batch_get(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    keys: Vec<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    if keys.is_empty() {
        return Ok(Output::BatchVectorGetResults(Vec::new()));
    }

    // Pre-validate all keys
    for key in &keys {
        convert_result(validate_key(key))?;
    }

    let engine_results = convert_vector_result(
        p.vector.batch_get(branch_id, &space, &collection, &keys),
        branch_id,
    )?;

    let mut results = Vec::with_capacity(engine_results.len());
    for result in engine_results {
        match result {
            Some(versioned) => {
                let version = extract_version(&versioned.version);
                let timestamp: u64 = versioned.timestamp.into();
                let data = to_versioned_vector_data(&versioned.value, version, timestamp)?;
                results.push(Some(data));
            }
            None => results.push(None),
        }
    }

    Ok(Output::BatchVectorGetResults(results))
}

/// Handle VectorBatchDelete command — delete multiple vectors by key.
pub fn vector_batch_delete(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    keys: Vec<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    if keys.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    // Pre-validate all keys
    for key in &keys {
        convert_result(validate_key(key))?;
    }

    let engine_results = convert_vector_result(
        p.vector.batch_delete(branch_id, &space, &collection, &keys),
        branch_id,
    )?;

    let results: Vec<crate::types::BatchItemResult> = engine_results
        .into_iter()
        .map(|deleted| crate::types::BatchItemResult {
            version: if deleted { Some(0) } else { None },
            error: None,
        })
        .collect();

    Ok(Output::BatchResults(results))
}

/// Handle VectorQuery with as_of timestamp (time-travel search).
#[allow(clippy::too_many_arguments)]
pub fn vector_query_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    query: Vec<f32>,
    k: u64,
    filter: Option<Vec<MetadataFilter>>,
    _metric: Option<DistanceMetric>,
    as_of_ts: u64,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let engine_filter = filter.as_ref().and_then(|f| to_engine_filter(f));
    let matches = convert_vector_result(
        p.vector.search_at(
            branch_id,
            &space,
            &collection,
            &query,
            k as usize,
            engine_filter,
            as_of_ts,
        ),
        branch_id,
    )
    .map_err(|e| enrich_dimension_error(&collection, e))?;

    let results: Result<Vec<VectorMatch>> = matches.into_iter().map(to_vector_match).collect();
    Ok(Output::VectorMatches(results?))
}

/// Handle VectorSample command — evenly-spaced sample of vector entries (metadata only).
pub fn vector_sample(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    count: usize,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let keys = convert_vector_result(
        p.vector.list_keys(branch_id, &space, &collection),
        branch_id,
    )?;
    let total = keys.len() as u64;
    let indices = super::sample_indices(keys.len(), count);
    let mut items = Vec::with_capacity(indices.len());
    for idx in indices {
        let key = &keys[idx];
        let result =
            convert_vector_result(p.vector.get(branch_id, &space, &collection, key), branch_id)?;
        if let Some(versioned) = result {
            // Return metadata only (no embedding — too large for discovery)
            let metadata = versioned
                .value
                .metadata
                .clone()
                .map(serde_json_to_value_public)
                .transpose()
                .map_err(crate::Error::from)?;
            let value = match metadata {
                Some(v) => v,
                None => Value::Null,
            };
            items.push(crate::types::SampleItem {
                key: key.clone(),
                value,
            });
        }
    }
    Ok(Output::SampleResult {
        total_count: total,
        items,
    })
}

/// Handle TimeRange command — get the available time range for a branch.
pub fn time_range(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let range = convert_result(p.db.time_range(branch_id))?;
    match range {
        Some((oldest, latest)) => Ok(Output::TimeRange {
            oldest_ts: Some(oldest),
            latest_ts: Some(latest),
        }),
        None => Ok(Output::TimeRange {
            oldest_ts: None,
            latest_ts: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge;

    #[test]
    fn test_to_core_branch_id_default() {
        let branch = BranchId::from("default");
        let core_id = bridge::to_core_branch_id(&branch).unwrap();
        assert_eq!(core_id.as_bytes(), &[0u8; 16]);
    }
}
