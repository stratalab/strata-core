use std::sync::Arc;

use strata_engine::primitives::json::index::IndexType;
use strata_engine::transaction_ops::TransactionOps as _;
use strata_engine::Transaction as EngineTransaction;
use strata_storage::Namespace;

use crate::bridge::{
    extract_version, json_to_value, parse_path, require_branch_exists, to_core_branch_id,
    validate_key, validate_value, value_to_json, Primitives,
};
use crate::convert::convert_result;
use crate::handlers::embed_runtime;
use crate::session::TxnSideEffects;
use crate::{
    BatchGetItemResult, BatchItemResult, BranchId, Output, Result, SampleItem, Value,
    VersionedValue,
};

fn serialize_json<T: serde::Serialize>(value: &T, what: &str) -> Result<Output> {
    let json = serde_json::to_string(value).map_err(|error| crate::Error::Internal {
        reason: format!("Failed to serialize {what}: {error}"),
        hint: None,
    })?;
    Ok(Output::Maybe(Some(Value::String(json))))
}

pub(crate) fn set(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    path: String,
    value: Value,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_value(&value, &primitives.limits))?;

    let path = convert_result(parse_path(&path))?;
    let value = convert_result(value_to_json(value))?;
    let (version, full_doc) = convert_result(
        primitives
            .json
            .set_or_create(&branch_id, &space, &key, &path, value),
    )?;
    embed_doc_value(primitives, branch_id, &space, &key, full_doc);

    Ok(Output::WriteResult {
        key,
        version: extract_version(&version),
    })
}

pub(crate) fn get(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    path: String,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let path = convert_result(parse_path(&path))?;

    match as_of {
        Some(as_of) => {
            let value = convert_result(
                primitives
                    .json
                    .get_at(&branch_id, &space, &key, &path, as_of),
            )?;
            Ok(Output::Maybe(match value {
                Some(value) => Some(convert_result(json_to_value(value))?),
                None => None,
            }))
        }
        None => {
            let value = convert_result(
                primitives
                    .json
                    .get_versioned(&branch_id, &space, &key, &path),
            )?;
            Ok(Output::MaybeVersioned(match value {
                Some(value) => Some(VersionedValue {
                    value: convert_result(json_to_value(value.value))?,
                    version: extract_version(&value.version),
                    timestamp: value.timestamp.into(),
                }),
                None => None,
            }))
        }
    }
}

pub(crate) fn delete(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    path: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let path = convert_result(parse_path(&path))?;

    if path.is_root() {
        let deleted = convert_result(primitives.json.destroy(&branch_id, &space, &key))?;
        if deleted {
            embed_runtime::maybe_remove_embedding(
                primitives,
                branch_id,
                &space,
                embed_runtime::SHADOW_JSON,
                &key,
            );
        }
        Ok(Output::DeleteResult { key, deleted })
    } else {
        match primitives
            .json
            .delete_at_path(&branch_id, &space, &key, &path)
        {
            Ok(_) => {
                embed_full_doc(primitives, branch_id, &space, &key);
                Ok(Output::DeleteResult { key, deleted: true })
            }
            Err(error) => {
                let error = crate::Error::from(error);
                let is_missing = matches!(&error, crate::Error::InvalidInput { reason, .. } if reason.contains("not found"));
                if is_missing {
                    Ok(Output::DeleteResult {
                        key,
                        deleted: false,
                    })
                } else {
                    Err(error)
                }
            }
        }
    }
}

pub(crate) fn getv(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let history = convert_result(primitives.json.getv(&branch_id, &space, &key))?;
    let mapped = history
        .map(|history| {
            history
                .into_versions()
                .into_iter()
                .map(|value| {
                    Ok(VersionedValue {
                        value: convert_result(json_to_value(value.value))?,
                        version: extract_version(&value.version),
                        timestamp: value.timestamp.into(),
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    Ok(Output::VersionHistory(mapped))
}

pub(crate) fn exists(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let exists = convert_result(primitives.json.exists(&branch_id, &space, &key))?;
    Ok(Output::Bool(exists))
}

pub(crate) fn batch_set(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::BatchJsonEntry>,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    if entries.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let mut results = vec![
        BatchItemResult {
            version: None,
            error: None,
        };
        entries.len()
    ];
    let mut valid_entries = Vec::with_capacity(entries.len());

    for (index, entry) in entries.into_iter().enumerate() {
        if let Err(error) = validate_key(&entry.key) {
            results[index].error = Some(error.to_string());
            continue;
        }
        if let Err(error) = validate_value(&entry.value, &primitives.limits) {
            results[index].error = Some(error.to_string());
            continue;
        }
        let path = match parse_path(&entry.path) {
            Ok(path) => path,
            Err(error) => {
                results[index].error = Some(error.to_string());
                continue;
            }
        };
        let value = match value_to_json(entry.value) {
            Ok(value) => value,
            Err(error) => {
                results[index].error = Some(error.to_string());
                continue;
            }
        };
        valid_entries.push((index, entry.key, path, value));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    let engine_entries: Vec<_> = valid_entries
        .iter()
        .map(|(_, key, path, value)| (key.clone(), path.clone(), value.clone()))
        .collect();
    let engine_results = convert_result(primitives.json.batch_set_or_create(
        &branch_id,
        &space,
        engine_entries,
    ))?;

    for (engine_index, (result_index, _, _, _)) in valid_entries.iter().enumerate() {
        results[*result_index].version = Some(extract_version(&engine_results[engine_index].0));
        embed_doc_value(
            primitives,
            branch_id,
            &space,
            &valid_entries[engine_index].1,
            engine_results[engine_index].1.clone(),
        );
    }

    Ok(Output::BatchResults(results))
}

pub(crate) fn batch_get(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::BatchJsonGetEntry>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if entries.is_empty() {
        return Ok(Output::BatchGetResults(Vec::new()));
    }

    let mut results = vec![
        BatchGetItemResult {
            value: None,
            version: None,
            timestamp: None,
            error: None,
        };
        entries.len()
    ];
    let mut valid_entries = Vec::with_capacity(entries.len());

    for (index, entry) in entries.into_iter().enumerate() {
        if let Err(error) = validate_key(&entry.key) {
            results[index].error = Some(error.to_string());
            continue;
        }
        let path = match parse_path(&entry.path) {
            Ok(path) => path,
            Err(error) => {
                results[index].error = Some(error.to_string());
                continue;
            }
        };
        valid_entries.push((index, entry.key, path));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchGetResults(results));
    }

    let engine_entries: Vec<_> = valid_entries
        .iter()
        .map(|(_, key, path)| (key.clone(), path.clone()))
        .collect();
    let engine_results = convert_result(primitives.json.batch_get(
        &branch_id,
        &space,
        &engine_entries,
    ))?;

    for (engine_index, (result_index, _, _)) in valid_entries.iter().enumerate() {
        if let Some(value) = engine_results[engine_index].clone() {
            results[*result_index].value = Some(convert_result(json_to_value(value.value))?);
            results[*result_index].version = Some(extract_version(&value.version));
            results[*result_index].timestamp = Some(value.timestamp.into());
        }
    }

    Ok(Output::BatchGetResults(results))
}

pub(crate) fn batch_delete(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::BatchJsonDeleteEntry>,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    if entries.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let mut results = vec![
        BatchItemResult {
            version: None,
            error: None,
        };
        entries.len()
    ];

    let root_delete = entries
        .iter()
        .all(|entry| entry.path.is_empty() || entry.path == "$");
    if root_delete {
        let mut valid_doc_ids = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
            if let Err(error) = validate_key(&entry.key) {
                results[index].error = Some(error.to_string());
                continue;
            }
            valid_doc_ids.push((index, entry.key));
        }

        if valid_doc_ids.is_empty() {
            return Ok(Output::BatchResults(results));
        }

        let doc_ids: Vec<String> = valid_doc_ids.iter().map(|(_, key)| key.clone()).collect();
        let destroyed =
            convert_result(primitives.json.batch_destroy(&branch_id, &space, &doc_ids))?;
        for (engine_index, (result_index, _)) in valid_doc_ids.iter().enumerate() {
            if destroyed[engine_index] {
                results[*result_index].version = Some(0);
                embed_runtime::maybe_remove_embedding(
                    primitives,
                    branch_id,
                    &space,
                    embed_runtime::SHADOW_JSON,
                    &valid_doc_ids[engine_index].1,
                );
            }
        }
        return Ok(Output::BatchResults(results));
    }

    let mut valid_entries = Vec::with_capacity(entries.len());
    for (index, entry) in entries.into_iter().enumerate() {
        if let Err(error) = validate_key(&entry.key) {
            results[index].error = Some(error.to_string());
            continue;
        }
        let path = match parse_path(&entry.path) {
            Ok(path) => path,
            Err(error) => {
                results[index].error = Some(error.to_string());
                continue;
            }
        };
        valid_entries.push((index, entry.key, path));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    let engine_entries: Vec<_> = valid_entries
        .iter()
        .map(|(_, key, path)| (key.clone(), path.clone()))
        .collect();
    let engine_results = convert_result(primitives.json.batch_delete_at_path(
        &branch_id,
        &space,
        &engine_entries,
    ))?;

    for (engine_index, (result_index, _, _)) in valid_entries.iter().enumerate() {
        match &engine_results[engine_index] {
            Ok(version) => {
                results[*result_index].version = Some(extract_version(version));
                embed_full_doc(
                    primitives,
                    branch_id,
                    &space,
                    &valid_entries[engine_index].1,
                );
            }
            Err(error) => results[*result_index].error = Some(error.to_string()),
        }
    }

    Ok(Output::BatchResults(results))
}

pub(crate) fn execute_in_txn(
    primitives: &Arc<Primitives>,
    ctx: &mut EngineTransaction,
    namespace: Arc<Namespace>,
    space: &str,
    command: crate::Command,
    effects: &mut TxnSideEffects,
) -> Result<Output> {
    match command {
        crate::Command::JsonGet { key, path, .. } => {
            convert_result(validate_key(&key))?;
            let mut txn = ctx.scoped(namespace);
            let path = convert_result(parse_path(&path))?;
            let result = txn.json_get(&key).map_err(crate::Error::from)?;
            Ok(Output::MaybeVersioned(match result {
                Some(value) => match strata_engine::get_at_path(&value.value, &path) {
                    Some(selected) => Some(VersionedValue {
                        value: convert_result(json_to_value(selected.clone()))?,
                        version: extract_version(&value.version),
                        timestamp: value.timestamp.into(),
                    }),
                    None => None,
                },
                None => None,
            }))
        }
        crate::Command::JsonExists { key, .. } => {
            convert_result(validate_key(&key))?;
            let mut txn = ctx.scoped(namespace);
            Ok(Output::Bool(
                txn.json_get(&key).map_err(crate::Error::from)?.is_some(),
            ))
        }
        crate::Command::JsonSet {
            key, path, value, ..
        } => {
            convert_result(validate_key(&key))?;
            convert_result(validate_value(&value, &primitives.limits))?;
            let mut txn = ctx.scoped(namespace);
            let path = convert_result(parse_path(&path))?;
            let value = convert_result(value_to_json(value))?;
            let version = txn
                .json_set(&key, &path, value)
                .map_err(crate::Error::from)?;
            effects.record_json(space, &key);
            Ok(Output::WriteResult {
                key,
                version: extract_version(&version),
            })
        }
        crate::Command::JsonDelete { key, path, .. } => {
            convert_result(validate_key(&key))?;
            let path = convert_result(parse_path(&path))?;
            if path.is_root() {
                let mut txn = ctx.scoped(namespace);
                let deleted = txn.json_delete(&key).map_err(crate::Error::from)?;
                if deleted {
                    effects.record_json(space, &key);
                }
                Ok(Output::DeleteResult { key, deleted })
            } else {
                let mut txn = ctx.scoped(namespace);
                let Some(mut doc) = txn
                    .json_get_path(&key, &strata_engine::JsonPath::root())
                    .map_err(crate::Error::from)?
                else {
                    return Ok(Output::DeleteResult {
                        key,
                        deleted: false,
                    });
                };

                let deleted = match strata_engine::delete_at_path(&mut doc, &path) {
                    Ok(Some(_)) => true,
                    Ok(None) | Err(strata_engine::JsonPathError::NotFound) => false,
                    Err(error) => {
                        return Err(crate::Error::InvalidInput {
                            reason: error.to_string(),
                            hint: None,
                        });
                    }
                };

                if deleted {
                    txn.json_set(&key, &strata_engine::JsonPath::root(), doc)
                        .map_err(crate::Error::from)?;
                    effects.record_json(space, &key);
                }

                Ok(Output::DeleteResult { key, deleted })
            }
        }
        crate::Command::JsonList {
            prefix,
            cursor,
            limit,
            ..
        } => {
            if let Some(prefix) = &prefix {
                if !prefix.is_empty() {
                    convert_result(validate_key(prefix))?;
                }
            }
            let mut txn = ctx.scoped(namespace);
            let mut keys = txn
                .json_list_keys(prefix.as_deref())
                .map_err(crate::Error::from)?;
            if let Some(ref cursor) = cursor {
                keys.retain(|key| key.as_str() > cursor.as_str());
            }
            let has_more = keys.len() > limit as usize;
            keys.truncate(limit as usize);
            let next_cursor = if has_more { keys.last().cloned() } else { None };
            Ok(Output::JsonListResult {
                keys,
                has_more,
                cursor: next_cursor,
            })
        }
        crate::Command::JsonBatchSet { entries, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                entries.len()
            ];
            let mut txn = ctx.scoped(namespace);
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = validate_value(&entry.value, &primitives.limits) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let path = match parse_path(&entry.path) {
                    Ok(path) => path,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                let value = match value_to_json(entry.value) {
                    Ok(value) => value,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                match txn.json_set(&entry.key, &path, value) {
                    Ok(version) => {
                        results[index].version = Some(extract_version(&version));
                        effects.record_json(space, &entry.key);
                    }
                    Err(error) => results[index].error = Some(error.to_string()),
                }
            }
            Ok(Output::BatchResults(results))
        }
        crate::Command::JsonBatchGet { entries, .. } => {
            let mut results = vec![
                BatchGetItemResult {
                    value: None,
                    version: None,
                    timestamp: None,
                    error: None,
                };
                entries.len()
            ];
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let output = match execute_in_txn(
                    primitives,
                    ctx,
                    namespace.clone(),
                    space,
                    crate::Command::JsonGet {
                        branch: None,
                        space: Some(space.to_string()),
                        key: entry.key,
                        path: entry.path,
                        as_of: None,
                    },
                    effects,
                ) {
                    Ok(output) => output,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                match output {
                    Output::Maybe(Some(value)) => results[index].value = Some(value),
                    Output::Maybe(None) => {}
                    Output::MaybeVersioned(Some(value)) => {
                        results[index].value = Some(value.value);
                        results[index].version = Some(value.version);
                        results[index].timestamp = Some(value.timestamp);
                    }
                    Output::MaybeVersioned(None) => {}
                    other => {
                        results[index].error = Some(format!(
                            "unexpected output for JsonBatchGet entry: {other:?}"
                        ));
                    }
                }
            }
            Ok(Output::BatchGetResults(results))
        }
        crate::Command::JsonBatchDelete { entries, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                entries.len()
            ];
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let output = match execute_in_txn(
                    primitives,
                    ctx,
                    namespace.clone(),
                    space,
                    crate::Command::JsonDelete {
                        branch: None,
                        space: Some(space.to_string()),
                        key: entry.key,
                        path: entry.path,
                    },
                    effects,
                ) {
                    Ok(output) => output,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                match output {
                    Output::DeleteResult { deleted, .. } if deleted => {
                        results[index].version = Some(0)
                    }
                    Output::DeleteResult { .. } => {}
                    other => {
                        results[index].error = Some(format!(
                            "unexpected output for JsonBatchDelete entry: {other:?}"
                        ));
                    }
                }
            }
            Ok(Output::BatchResults(results))
        }
        other => Err(crate::Error::Internal {
            reason: format!("unexpected JSON transaction command: {}", other.name()),
            hint: None,
        }),
    }
}

pub(crate) fn list(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: u64,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(prefix) = &prefix {
        if !prefix.is_empty() {
            convert_result(validate_key(prefix))?;
        }
    }

    let result = match as_of {
        Some(as_of) => convert_result(primitives.json.list_at(
            &branch_id,
            &space,
            prefix.as_deref(),
            as_of,
            cursor.as_deref(),
            limit as usize,
        ))?,
        None => convert_result(primitives.json.list(
            &branch_id,
            &space,
            prefix.as_deref(),
            cursor.as_deref(),
            limit as usize,
        ))?,
    };

    Ok(Output::JsonListResult {
        keys: result.doc_ids,
        has_more: result.next_cursor.is_some(),
        cursor: result.next_cursor,
    })
}

fn embed_doc_value(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    key: &str,
    json_value: strata_engine::JsonValue,
) {
    if let Ok(value) = json_to_value(json_value) {
        if let Some(text) = embed_runtime::extract_text(&value) {
            embed_runtime::maybe_embed_text(
                primitives,
                branch_id,
                space,
                embed_runtime::SHADOW_JSON,
                key,
                &text,
                strata_core::EntityRef::json(branch_id, space, key),
            );
        } else {
            embed_runtime::maybe_remove_embedding(
                primitives,
                branch_id,
                space,
                embed_runtime::SHADOW_JSON,
                key,
            );
        }
    } else {
        embed_runtime::maybe_remove_embedding(
            primitives,
            branch_id,
            space,
            embed_runtime::SHADOW_JSON,
            key,
        );
    }
}

fn embed_full_doc(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    key: &str,
) {
    use strata_engine::JsonPath;

    match primitives
        .json
        .get(&branch_id, space, key, &JsonPath::root())
    {
        Ok(Some(json_value)) => embed_doc_value(primitives, branch_id, space, key, json_value),
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                key = key,
                error = %error,
                "Failed to read document after JSON mutation"
            );
        }
    }
}

pub(crate) fn count(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let count = convert_result(primitives.json.count(&branch_id, &space, prefix.as_deref()))?;
    Ok(Output::Uint(count))
}

pub(crate) fn sample(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    count: usize,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let (total_count, sampled) = convert_result(primitives.json.sample(
        &branch_id,
        &space,
        prefix.as_deref(),
        count,
    ))?;
    let items = sampled
        .into_iter()
        .map(|(key, value)| {
            Ok(SampleItem {
                key,
                value: convert_result(json_to_value(value))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Output::SampleResult { total_count, items })
}

pub(crate) fn create_index(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    name: String,
    field_path: String,
    index_type: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    let index_type = match index_type.as_str() {
        "numeric" => IndexType::Numeric,
        "tag" => IndexType::Tag,
        "text" => IndexType::Text,
        other => {
            return Err(crate::Error::InvalidInput {
                reason: format!(
                    "Invalid index type '{}'. Must be 'numeric', 'tag', or 'text'",
                    other
                ),
                hint: None,
            });
        }
    };

    let definition = convert_result(primitives.json.create_index(
        &branch_id,
        &space,
        &name,
        &field_path,
        index_type,
    ))?;
    serialize_json(&definition, "IndexDef")
}

pub(crate) fn drop_index(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    name: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let existed = convert_result(primitives.json.drop_index(&branch_id, &space, &name))?;
    Ok(Output::Bool(existed))
}

pub(crate) fn list_indexes(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let indexes = convert_result(primitives.json.list_indexes(&branch_id, &space))?;
    serialize_json(&indexes, "index list")
}
