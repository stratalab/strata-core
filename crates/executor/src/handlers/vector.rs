use std::sync::Arc;

use strata_engine::{TransactionContext, VectorConfig};
use strata_vector::ext::VectorStoreExt;

use crate::bridge::{
    extract_version, from_engine_metric, require_branch_exists, serde_json_to_value_public,
    to_core_branch_id, to_engine_filter, to_engine_metric, validate_key,
    validate_not_internal_collection, validate_vector, value_to_serde_json_public, Primitives,
};
use crate::convert::convert_result;
use crate::{
    BatchItemResult, BatchVectorEntry, BranchId, CollectionInfo, DistanceMetric, Error,
    MetadataFilter, Output, Result, SampleItem, Value, VectorData, VectorMatch,
    VersionedVectorData,
};

fn convert_vector_result<T>(
    result: std::result::Result<T, impl Into<strata_engine::StrataError>>,
    _branch_id: strata_core::BranchId,
) -> Result<T> {
    result.map_err(|error| Error::from(error.into()))
}

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
        .map_err(Error::from)?;

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

fn enrich_dimension_error(collection: &str, error: Error) -> Error {
    match error {
        Error::DimensionMismatch {
            expected,
            actual,
            hint: None,
        } => Error::DimensionMismatch {
            expected,
            actual,
            hint: Some(format!(
                "Collection \"{}\" expects {}-dim vectors. Your vector is {}-dim.",
                collection, expected, actual
            )),
        },
        other => other,
    }
}

fn to_vector_match(vector_match: strata_vector::VectorMatch) -> Result<VectorMatch> {
    let metadata = vector_match
        .metadata
        .map(serde_json_to_value_public)
        .transpose()
        .map_err(Error::from)?;
    Ok(VectorMatch {
        key: vector_match.key,
        score: vector_match.score,
        metadata,
    })
}

pub(crate) fn upsert(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
    vector: Vec<f32>,
    metadata: Option<Value>,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;
    convert_result(validate_vector(&vector, &primitives.limits))?;

    let metadata = metadata
        .map(value_to_serde_json_public)
        .transpose()
        .map_err(Error::from)?;
    let version = convert_vector_result(
        primitives
            .vector
            .insert(branch_id, &space, &collection, &key, &vector, metadata),
        branch_id,
    )
    .map_err(|error| enrich_dimension_error(&collection, error))?;

    Ok(Output::VectorWriteResult {
        collection,
        key,
        version: extract_version(&version),
    })
}

pub(crate) fn get(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;

    let result = match as_of {
        Some(as_of) => {
            let entry = convert_vector_result(
                primitives
                    .vector
                    .get_at(branch_id, &space, &collection, &key, as_of),
                branch_id,
            )?;
            match entry {
                Some(entry) => {
                    let metadata = entry
                        .metadata
                        .map(serde_json_to_value_public)
                        .transpose()
                        .map_err(Error::from)?;
                    Some(VersionedVectorData {
                        key: entry.key,
                        data: VectorData {
                            embedding: entry.embedding,
                            metadata,
                        },
                        version: extract_version(&entry.version),
                        timestamp: as_of,
                    })
                }
                None => None,
            }
        }
        None => {
            let entry = convert_vector_result(
                primitives.vector.get(branch_id, &space, &collection, &key),
                branch_id,
            )?;
            entry
                .map(|versioned| {
                    let version = extract_version(&versioned.version);
                    let timestamp: u64 = versioned.timestamp.into();
                    to_versioned_vector_data(&versioned.value, version, timestamp)
                })
                .transpose()?
        }
    };

    Ok(Output::VectorData(result))
}

pub(crate) fn delete(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;

    let deleted = convert_vector_result(
        primitives
            .vector
            .delete(branch_id, &space, &collection, &key),
        branch_id,
    )?;
    Ok(Output::VectorDeleteResult {
        collection,
        key,
        deleted,
    })
}

pub(crate) fn getv(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_not_internal_collection(&collection))?;

    let history = convert_vector_result(
        primitives.vector.getv(branch_id, &space, &collection, &key),
        branch_id,
    )?;
    let mapped = history
        .map(|history| {
            history
                .into_versions()
                .into_iter()
                .map(|versioned| {
                    let version = extract_version(&versioned.version);
                    let timestamp: u64 = versioned.timestamp.into();
                    to_versioned_vector_data(&versioned.value, version, timestamp)
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    Ok(Output::VectorVersionHistory(mapped))
}

pub(crate) fn query(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    query: Vec<f32>,
    k: u64,
    filter: Option<Vec<MetadataFilter>>,
    metric: Option<DistanceMetric>,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;
    let engine_filter = filter
        .as_ref()
        .and_then(|filters| to_engine_filter(filters));

    let matches = match as_of {
        Some(as_of) => convert_vector_result(
            primitives.vector.search_at(
                branch_id,
                &space,
                &collection,
                &query,
                k as usize,
                engine_filter,
                as_of,
            ),
            branch_id,
        ),
        None => {
            let _ = metric.map(to_engine_metric);
            convert_vector_result(
                primitives.vector.search(
                    branch_id,
                    &space,
                    &collection,
                    &query,
                    k as usize,
                    engine_filter,
                ),
                branch_id,
            )
        }
    }
    .map_err(|error| enrich_dimension_error(&collection, error))?;

    let matches = matches
        .into_iter()
        .map(to_vector_match)
        .collect::<Result<Vec<_>>>()?;
    Ok(Output::VectorMatches(matches))
}

pub(crate) fn create_collection(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    dimension: u64,
    metric: DistanceMetric,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let storage_dtype = primitives.db.config().vector_storage_dtype();
    let config = convert_result(VectorConfig::new_with_dtype(
        dimension as usize,
        to_engine_metric(metric),
        storage_dtype,
    ))?;
    let version = convert_vector_result(
        primitives
            .vector
            .create_collection(branch_id, &space, &collection, config),
        branch_id,
    )?;

    Ok(Output::Version(extract_version(&version.version)))
}

pub(crate) fn delete_collection(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;
    let collections = convert_vector_result(
        primitives.vector.list_collections(branch_id, &space),
        branch_id,
    )?;
    if !collections.iter().any(|info| info.name == collection) {
        return Ok(Output::Bool(false));
    }

    convert_vector_result(
        primitives
            .vector
            .delete_collection(branch_id, &space, &collection),
        branch_id,
    )?;
    Ok(Output::Bool(true))
}

pub(crate) fn list_collections(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let collections = convert_vector_result(
        primitives.vector.list_collections(branch_id, &space),
        branch_id,
    )?;

    let collections = collections
        .into_iter()
        .filter(|info| !crate::bridge::is_internal_collection(&info.name))
        .map(|info| {
            let (index_type, memory_bytes) = primitives
                .vector
                .collection_backend_stats(branch_id, &space, &info.name)
                .map(|(index_type, memory)| (Some(index_type.to_string()), Some(memory as u64)))
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

    Ok(Output::VectorCollectionList(collections))
}

pub(crate) fn collection_stats(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let collections = convert_vector_result(
        primitives.vector.list_collections(branch_id, &space),
        branch_id,
    )?;
    let names: Vec<String> = collections.iter().map(|info| info.name.clone()).collect();
    let info = collections
        .into_iter()
        .find(|info| info.name == collection)
        .ok_or_else(|| Error::CollectionNotFound {
            collection: collection.clone(),
            hint: crate::suggest::format_hint("collections", &names, &collection, 2),
        })?;

    let (index_type, memory_bytes) = primitives
        .vector
        .collection_backend_stats(branch_id, &space, &info.name)
        .map(|(index_type, memory)| (Some(index_type.to_string()), Some(memory as u64)))
        .unwrap_or((None, None));
    Ok(Output::VectorCollectionList(vec![CollectionInfo {
        name: info.name,
        dimension: info.config.dimension,
        metric: from_engine_metric(info.config.metric),
        count: info.count as u64,
        index_type,
        memory_bytes,
    }]))
}

pub(crate) fn batch_upsert(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    entries: Vec<BatchVectorEntry>,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let mut engine_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        convert_result(validate_key(&entry.key))?;
        convert_result(validate_vector(&entry.vector, &primitives.limits))?;
        let metadata = entry
            .metadata
            .map(value_to_serde_json_public)
            .transpose()
            .map_err(Error::from)?;
        engine_entries.push((entry.key, entry.vector, metadata));
    }

    let versions = convert_vector_result(
        primitives
            .vector
            .batch_insert(branch_id, &space, &collection, engine_entries),
        branch_id,
    )
    .map_err(|error| enrich_dimension_error(&collection, error))?;

    Ok(Output::Versions(
        versions.iter().map(extract_version).collect::<Vec<_>>(),
    ))
}

pub(crate) fn batch_get(
    primitives: &Arc<Primitives>,
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

    for key in &keys {
        convert_result(validate_key(key))?;
    }

    let values = convert_vector_result(
        primitives
            .vector
            .batch_get(branch_id, &space, &collection, &keys),
        branch_id,
    )?;
    let values = values
        .into_iter()
        .map(|value| {
            value
                .map(|versioned| {
                    let version = extract_version(&versioned.version);
                    let timestamp: u64 = versioned.timestamp.into();
                    to_versioned_vector_data(&versioned.value, version, timestamp)
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Output::BatchVectorGetResults(values))
}

pub(crate) fn batch_delete(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    keys: Vec<String>,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;
    if keys.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    for key in &keys {
        convert_result(validate_key(key))?;
    }

    let deleted = convert_vector_result(
        primitives
            .vector
            .batch_delete(branch_id, &space, &collection, &keys),
        branch_id,
    )?;
    Ok(Output::BatchResults(
        deleted
            .into_iter()
            .map(|deleted| BatchItemResult {
                version: deleted.then_some(0),
                error: None,
            })
            .collect(),
    ))
}

pub(crate) fn sample(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    collection: String,
    count: usize,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_not_internal_collection(&collection))?;

    let keys = convert_vector_result(
        primitives.vector.list_keys(branch_id, &space, &collection),
        branch_id,
    )?;
    let total_count = keys.len() as u64;
    let indices = super::sample_indices(keys.len(), count);
    let mut items = Vec::with_capacity(indices.len());
    for index in indices {
        let key = &keys[index];
        let value = convert_vector_result(
            primitives.vector.get(branch_id, &space, &collection, key),
            branch_id,
        )?;
        if let Some(versioned) = value {
            let value = match versioned
                .value
                .metadata
                .clone()
                .map(serde_json_to_value_public)
                .transpose()
                .map_err(Error::from)?
            {
                Some(metadata) => metadata,
                None => Value::Null,
            };
            items.push(SampleItem {
                key: key.clone(),
                value,
            });
        }
    }

    Ok(Output::SampleResult { total_count, items })
}

pub(crate) fn execute_in_txn(
    primitives: &Arc<Primitives>,
    ctx: &mut TransactionContext,
    branch_id: strata_core::BranchId,
    space: &str,
    command: crate::Command,
) -> Result<Output> {
    match command {
        crate::Command::VectorUpsert {
            collection,
            key,
            vector,
            metadata,
            ..
        } => {
            convert_result(validate_not_internal_collection(&collection))?;
            convert_result(validate_key(&key))?;
            convert_result(validate_vector(&vector, &primitives.limits))?;
            primitives
                .vector
                .ensure_collection_loaded(branch_id, space, &collection)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let metadata = metadata
                .map(value_to_serde_json_public)
                .transpose()
                .map_err(Error::from)?;
            let state = primitives
                .vector
                .state()
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let (version, _) = ctx
                .vector_upsert(
                    branch_id,
                    space,
                    &collection,
                    &key,
                    &vector,
                    metadata,
                    None,
                    &state,
                )
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            Ok(Output::VectorWriteResult {
                collection,
                key,
                version: extract_version(&version),
            })
        }
        crate::Command::VectorDelete {
            collection, key, ..
        } => {
            convert_result(validate_not_internal_collection(&collection))?;
            convert_result(validate_key(&key))?;
            primitives
                .vector
                .ensure_collection_loaded(branch_id, space, &collection)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let state = primitives
                .vector
                .state()
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let (deleted, _) = ctx
                .vector_delete(branch_id, space, &collection, &key, &state)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            Ok(Output::VectorDeleteResult {
                collection,
                key,
                deleted,
            })
        }
        crate::Command::VectorGet {
            collection, key, ..
        } => {
            convert_result(validate_not_internal_collection(&collection))?;
            convert_result(validate_key(&key))?;
            let record = ctx
                .vector_get(branch_id, space, &collection, &key)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            match record {
                Some(record) => {
                    let version = extract_version(&strata_core::Version::counter(record.version));
                    let metadata = record
                        .metadata
                        .map(serde_json_to_value_public)
                        .transpose()
                        .map_err(Error::from)?;
                    Ok(Output::VectorData(Some(VersionedVectorData {
                        key,
                        data: VectorData {
                            embedding: record.embedding,
                            metadata,
                        },
                        version,
                        timestamp: record.updated_at,
                    })))
                }
                None => Ok(Output::VectorData(None)),
            }
        }
        other => Err(Error::Internal {
            reason: format!("unexpected vector transaction command: {}", other.name()),
            hint: None,
        }),
    }
}

fn prepare_space_write(primitives: &Arc<Primitives>, branch: &BranchId, space: &str) -> Result<()> {
    require_branch_exists(primitives, branch)?;
    let branch_id = to_core_branch_id(branch)?;
    convert_result(primitives.space.register(branch_id, space))?;
    Ok(())
}
