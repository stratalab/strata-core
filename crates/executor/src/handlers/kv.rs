use std::sync::Arc;

use strata_engine::transaction::context::Transaction as ScopedTransaction;
use strata_engine::transaction_ops::TransactionOps as _;
use strata_engine::TransactionContext;
use strata_storage::{Key, Namespace, TypeTag};

use crate::bridge::{
    extract_version, require_branch_exists, to_core_branch_id, to_versioned_value, validate_key,
    validate_value, Primitives,
};
use crate::convert::convert_result;
use crate::handlers::embed_runtime;
use crate::session::TxnSideEffects;
use crate::{BatchGetItemResult, BatchItemResult, BranchId, Output, Result, SampleItem};

fn page_keys(mut keys: Vec<String>, cursor: Option<&str>, limit: Option<u64>) -> Output {
    if let Some(cursor) = cursor {
        keys = keys
            .into_iter()
            .skip_while(|key| key.as_str() <= cursor)
            .collect();
    }

    match limit {
        Some(limit) => {
            let has_more = keys.len() > limit as usize;
            let page: Vec<String> = keys.into_iter().take(limit as usize).collect();
            let cursor = if has_more { page.last().cloned() } else { None };
            Output::KeysPage {
                keys: page,
                has_more,
                cursor,
            }
        }
        None => Output::Keys(keys),
    }
}

pub(crate) fn put(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    value: strata_core::Value,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_value(&value, &primitives.limits))?;
    let text = embed_runtime::extract_text(&value);

    let version = convert_result(primitives.kv.put(&branch_id, &space, &key, value))?;
    if let Some(text) = text.as_deref() {
        embed_runtime::maybe_embed_text(
            primitives,
            branch_id,
            &space,
            embed_runtime::SHADOW_KV,
            &key,
            text,
            strata_core::EntityRef::kv(branch_id, &space, &key),
        );
    } else {
        embed_runtime::maybe_remove_embedding(
            primitives,
            branch_id,
            &space,
            embed_runtime::SHADOW_KV,
            &key,
        );
    }
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
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;

    match as_of {
        Some(as_of) => {
            let value = convert_result(primitives.kv.get_at(&branch_id, &space, &key, as_of))?;
            Ok(Output::Maybe(value))
        }
        None => {
            let value =
                convert_result(primitives.kv.get_versioned_direct(&branch_id, &space, &key))?;
            Ok(Output::MaybeVersioned(value.map(to_versioned_value)))
        }
    }
}

pub(crate) fn delete(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let deleted = convert_result(primitives.kv.delete(&branch_id, &space, &key))?;
    if deleted {
        embed_runtime::maybe_remove_embedding(
            primitives,
            branch_id,
            &space,
            embed_runtime::SHADOW_KV,
            &key,
        );
    }
    Ok(Output::DeleteResult { key, deleted })
}

pub(crate) fn list(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<u64>,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(prefix) = &prefix {
        if !prefix.is_empty() {
            convert_result(validate_key(prefix))?;
        }
    }

    let output = match as_of {
        Some(as_of) => {
            let keys = convert_result(primitives.kv.list_at(
                &branch_id,
                &space,
                prefix.as_deref(),
                as_of,
            ))?;
            page_keys(keys, cursor.as_deref(), limit)
        }
        None => match limit {
            Some(limit) => {
                let fetched = convert_result(primitives.kv.list_with_cursor_limit(
                    &branch_id,
                    &space,
                    prefix.as_deref(),
                    cursor.as_deref(),
                    limit as usize,
                ))?;
                let has_more = fetched.len() > limit as usize;
                let page: Vec<String> = fetched.into_iter().take(limit as usize).collect();
                let cursor = if has_more { page.last().cloned() } else { None };
                Output::KeysPage {
                    keys: page,
                    has_more,
                    cursor,
                }
            }
            None => {
                let keys =
                    convert_result(primitives.kv.list(&branch_id, &space, prefix.as_deref()))?;
                page_keys(keys, cursor.as_deref(), None)
            }
        },
    };

    Ok(output)
}

pub(crate) fn scan(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    start: Option<String>,
    limit: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(start) = &start {
        convert_result(validate_key(start))?;
    }

    let results = convert_result(primitives.kv.scan(
        &branch_id,
        &space,
        start.as_deref(),
        limit.map(|v| v as usize),
    ))?;
    Ok(Output::KvScanResult(results))
}

pub(crate) fn batch_put(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::BatchKvEntry>,
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
        valid_entries.push((index, entry.key, entry.value));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    let embed_data: Vec<(usize, String, Option<String>)> = valid_entries
        .iter()
        .map(|(index, key, value)| (*index, key.clone(), embed_runtime::extract_text(value)))
        .collect();

    let engine_entries: Vec<(String, strata_core::Value)> = valid_entries
        .iter()
        .map(|(_, key, value)| (key.clone(), value.clone()))
        .collect();
    let engine_results =
        convert_result(primitives.kv.batch_put(&branch_id, &space, engine_entries))?;

    for (engine_index, (result_index, _, _)) in valid_entries.iter().enumerate() {
        match &engine_results[engine_index] {
            Ok(version) => {
                results[*result_index].version = Some(extract_version(version));
                if let Some(text) = embed_data[engine_index].2.as_deref() {
                    embed_runtime::maybe_embed_text(
                        primitives,
                        branch_id,
                        &space,
                        embed_runtime::SHADOW_KV,
                        &embed_data[engine_index].1,
                        text,
                        strata_core::EntityRef::kv(
                            branch_id,
                            &space,
                            embed_data[engine_index].1.as_str(),
                        ),
                    );
                } else {
                    embed_runtime::maybe_remove_embedding(
                        primitives,
                        branch_id,
                        &space,
                        embed_runtime::SHADOW_KV,
                        &embed_data[engine_index].1,
                    );
                }
            }
            Err(error) => results[*result_index].error = Some(error.clone()),
        }
    }

    Ok(Output::BatchResults(results))
}

pub(crate) fn batch_get(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    keys: Vec<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if keys.is_empty() {
        return Ok(Output::BatchGetResults(Vec::new()));
    }

    let mut results = vec![
        BatchGetItemResult {
            value: None,
            version: None,
            timestamp: None,
            error: None,
        };
        keys.len()
    ];
    let mut valid_keys = Vec::with_capacity(keys.len());

    for (index, key) in keys.into_iter().enumerate() {
        if let Err(error) = validate_key(&key) {
            results[index].error = Some(error.to_string());
            continue;
        }
        valid_keys.push((index, key));
    }

    if valid_keys.is_empty() {
        return Ok(Output::BatchGetResults(results));
    }

    let engine_keys: Vec<String> = valid_keys.iter().map(|(_, key)| key.clone()).collect();
    let engine_results = convert_result(primitives.kv.batch_get(&branch_id, &space, &engine_keys))?;

    for (engine_index, (result_index, _)) in valid_keys.iter().enumerate() {
        if let Some(value) = engine_results[engine_index].clone() {
            results[*result_index].value = Some(value.value);
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
    keys: Vec<String>,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    if keys.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let mut results = vec![
        BatchItemResult {
            version: None,
            error: None,
        };
        keys.len()
    ];
    let mut valid_keys = Vec::with_capacity(keys.len());

    for (index, key) in keys.into_iter().enumerate() {
        if let Err(error) = validate_key(&key) {
            results[index].error = Some(error.to_string());
            continue;
        }
        valid_keys.push((index, key));
    }

    if valid_keys.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    let engine_keys: Vec<String> = valid_keys.iter().map(|(_, key)| key.clone()).collect();
    let engine_results =
        convert_result(primitives.kv.batch_delete(&branch_id, &space, &engine_keys))?;

    for (engine_index, (result_index, _)) in valid_keys.iter().enumerate() {
        if engine_results[engine_index] {
            results[*result_index].version = Some(0);
            embed_runtime::maybe_remove_embedding(
                primitives,
                branch_id,
                &space,
                embed_runtime::SHADOW_KV,
                &valid_keys[engine_index].1,
            );
        }
    }

    Ok(Output::BatchResults(results))
}

pub(crate) fn batch_exists(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    keys: Vec<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if keys.is_empty() {
        return Ok(Output::BoolList(Vec::new()));
    }

    for key in &keys {
        convert_result(validate_key(key))?;
    }

    let results = convert_result(primitives.kv.batch_exists(&branch_id, &space, &keys))?;
    Ok(Output::BoolList(results))
}

pub(crate) fn getv(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let history = convert_result(primitives.kv.getv(&branch_id, &space, &key))?;
    let mapped = history.map(|history| {
        history
            .into_versions()
            .into_iter()
            .map(to_versioned_value)
            .collect()
    });
    Ok(Output::VersionHistory(mapped))
}

pub(crate) fn count(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(prefix) = &prefix {
        if !prefix.is_empty() {
            convert_result(validate_key(prefix))?;
        }
    }

    let count = convert_result(primitives.kv.count(&branch_id, &space, prefix.as_deref()))?;
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
    if let Some(prefix) = &prefix {
        if !prefix.is_empty() {
            convert_result(validate_key(prefix))?;
        }
    }

    let (total_count, sampled) = convert_result(primitives.kv.sample(
        &branch_id,
        &space,
        prefix.as_deref(),
        count,
    ))?;
    let items = sampled
        .into_iter()
        .map(|(key, value)| SampleItem { key, value })
        .collect();
    Ok(Output::SampleResult { total_count, items })
}

pub(crate) fn execute_in_txn(
    primitives: &Arc<Primitives>,
    ctx: &mut TransactionContext,
    namespace: Arc<Namespace>,
    space: &str,
    command: crate::Command,
    effects: &mut TxnSideEffects,
) -> Result<Output> {
    match command {
        crate::Command::KvGet { key, .. } => {
            convert_result(validate_key(&key))?;
            let full_key = Key::new_kv(namespace, &key);
            let result = ctx.get(&full_key).map_err(crate::Error::from)?;
            Ok(Output::Maybe(result))
        }
        crate::Command::KvList {
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
            let prefix_key = match prefix {
                Some(ref prefix) => Key::new_kv(namespace.clone(), prefix),
                None => Key::new(namespace.clone(), TypeTag::KV, vec![]),
            };
            let entries = ctx.scan_prefix(&prefix_key).map_err(crate::Error::from)?;
            let mut keys: Vec<String> = entries
                .into_iter()
                .filter_map(|(key, _)| key.user_key_string())
                .collect();
            keys.sort();
            keys.dedup();
            Ok(page_keys(keys, cursor.as_deref(), limit))
        }
        crate::Command::KvScan { start, limit, .. } => {
            if let Some(start) = &start {
                convert_result(validate_key(start))?;
            }
            let prefix_key = Key::new(namespace, TypeTag::KV, vec![]);
            let entries = ctx.scan_prefix(&prefix_key).map_err(crate::Error::from)?;
            let mut pairs: Vec<(String, strata_core::Value)> = entries
                .into_iter()
                .filter_map(|(key, value)| key.user_key_string().map(|key| (key, value)))
                .collect();
            pairs.sort_by(|left, right| left.0.cmp(&right.0));
            if let Some(start) = start {
                pairs.retain(|(key, _)| key.as_str() >= start.as_str());
            }
            if let Some(limit) = limit {
                pairs.truncate(limit as usize);
            }
            Ok(Output::KvScanResult(pairs))
        }
        crate::Command::KvPut { key, value, .. } => {
            convert_result(validate_key(&key))?;
            convert_result(validate_value(&value, &primitives.limits))?;
            let mut txn = ScopedTransaction::new(ctx, namespace);
            let version = txn.kv_put(&key, value).map_err(crate::Error::from)?;
            effects.record_kv(space, &key);
            Ok(Output::WriteResult {
                key,
                version: extract_version(&version),
            })
        }
        crate::Command::KvDelete { key, .. } => {
            convert_result(validate_key(&key))?;
            let full_key = Key::new_kv(namespace, &key);
            let existed = ctx.exists(&full_key).map_err(crate::Error::from)?;
            ctx.delete(full_key).map_err(crate::Error::from)?;
            if existed {
                effects.record_kv(space, &key);
            }
            Ok(Output::DeleteResult {
                key,
                deleted: existed,
            })
        }
        crate::Command::KvBatchPut { entries, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                entries.len()
            ];
            let mut txn = ScopedTransaction::new(ctx, namespace);
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = validate_value(&entry.value, &primitives.limits) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = txn.kv_put(&entry.key, entry.value) {
                    results[index].error = Some(error.to_string());
                } else {
                    effects.record_kv(space, &entry.key);
                }
            }
            Ok(Output::BatchResults(results))
        }
        crate::Command::KvBatchGet { keys, .. } => {
            let mut results = vec![
                BatchGetItemResult {
                    value: None,
                    version: None,
                    timestamp: None,
                    error: None,
                };
                keys.len()
            ];
            for (index, key) in keys.into_iter().enumerate() {
                if let Err(error) = validate_key(&key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let full_key = Key::new_kv(namespace.clone(), &key);
                let value = ctx.get(&full_key).map_err(crate::Error::from)?;
                results[index].value = value;
            }
            Ok(Output::BatchGetResults(results))
        }
        crate::Command::KvBatchDelete { keys, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                keys.len()
            ];
            for (index, key) in keys.into_iter().enumerate() {
                if let Err(error) = validate_key(&key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let full_key = Key::new_kv(namespace.clone(), &key);
                let existed = ctx.exists(&full_key).map_err(crate::Error::from)?;
                if let Err(error) = ctx.delete(full_key) {
                    results[index].error = Some(error.to_string());
                } else if existed {
                    effects.record_kv(space, &key);
                }
            }
            Ok(Output::BatchResults(results))
        }
        crate::Command::KvBatchExists { keys, .. } => {
            let mut values = Vec::with_capacity(keys.len());
            for key in &keys {
                convert_result(validate_key(key))?;
            }
            for key in keys {
                let full_key = Key::new_kv(namespace.clone(), &key);
                values.push(ctx.exists(&full_key).map_err(crate::Error::from)?);
            }
            Ok(Output::BoolList(values))
        }
        other => Err(crate::Error::Internal {
            reason: format!("unexpected KV transaction command: {}", other.name()),
            hint: None,
        }),
    }
}
