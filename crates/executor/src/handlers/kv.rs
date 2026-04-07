//! KV command handlers.
//!
//! This module implements handlers for the 4 MVP KV commands by dispatching
//! directly to engine primitives via `bridge::Primitives`.

use std::sync::Arc;

use strata_core::Value;

use crate::bridge::{
    extract_version, to_core_branch_id, to_versioned_value, validate_key, validate_value,
    Primitives,
};
use crate::convert::convert_result;
use crate::types::BranchId;
use crate::{Error, Output, Result};

use super::require_branch_exists;

/// Handle KvGetv command — get full version history for a key.
pub fn kv_getv(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let result = convert_result(p.kv.getv(&branch_id, &space, &key))
        .map_err(|e| enrich_kv_error(p, &branch_id, &space, e))?;
    let mapped = result.map(|history| {
        history
            .into_versions()
            .into_iter()
            .map(to_versioned_value)
            .collect()
    });
    Ok(Output::VersionHistory(mapped))
}

// =============================================================================
// MVP Handlers (4 commands)
// =============================================================================

/// Handle KvPut command.
pub fn kv_put(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    value: Value,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_value(&value, &p.limits))?;

    // Extract text before the value is consumed by put()
    let text = super::embed_hook::extract_text(&value);

    let version = convert_result(p.kv.put(&branch_id, &space, &key, value))?;
    let v = extract_version(&version);

    // Best-effort auto-embed after successful write
    if let Some(ref text) = text {
        super::embed_hook::maybe_embed_text(
            p,
            branch_id,
            &space,
            super::embed_hook::SHADOW_KV,
            &key,
            text,
            strata_core::EntityRef::kv(branch_id, &space, &key),
        );
    }

    Ok(Output::WriteResult { key, version: v })
}

/// Handle KvGet command.
///
/// Returns `MaybeVersioned` with value, version, and timestamp metadata.
pub fn kv_get(p: &Arc<Primitives>, branch: BranchId, space: String, key: String) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let result = convert_result(p.kv.get_versioned_direct(&branch_id, &space, &key))
        .map_err(|e| enrich_kv_error(p, &branch_id, &space, e))?;
    Ok(Output::MaybeVersioned(result.map(to_versioned_value)))
}

/// Handle KvGet with as_of timestamp (time-travel read).
pub fn kv_get_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    as_of_ts: u64,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let result = convert_result(p.kv.get_at(&branch_id, &space, &key, as_of_ts))
        .map_err(|e| enrich_kv_error(p, &branch_id, &space, e))?;
    Ok(Output::Maybe(result))
}

/// Handle KvDelete command.
pub fn kv_delete(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let existed = convert_result(p.kv.delete(&branch_id, &space, &key))?;

    // Best-effort remove shadow embedding
    if existed {
        super::embed_hook::maybe_remove_embedding(
            p,
            branch_id,
            &space,
            super::embed_hook::SHADOW_KV,
            &key,
        );
    }

    Ok(Output::DeleteResult {
        key,
        deleted: existed,
    })
}

/// Handle KvList command.
pub fn kv_list(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(ref pfx) = prefix {
        if !pfx.is_empty() {
            convert_result(validate_key(pfx))?;
        }
    }

    if let Some(lim) = limit {
        // Push cursor + limit down to the engine: only materializes lim+1
        // user-key strings instead of the entire key space.
        let fetched = convert_result(p.kv.list_with_cursor_limit(
            &branch_id,
            &space,
            prefix.as_deref(),
            cursor.as_deref(),
            lim as usize,
        ))?;
        let has_more = fetched.len() > lim as usize;
        let page: Vec<String> = fetched.into_iter().take(lim as usize).collect();
        let next_cursor = if has_more { page.last().cloned() } else { None };
        Ok(Output::KeysPage {
            keys: page,
            has_more,
            cursor: next_cursor,
        })
    } else {
        let keys = convert_result(p.kv.list(&branch_id, &space, prefix.as_deref()))?;
        Ok(Output::Keys(keys))
    }
}

/// Handle KvBatchPut command.
///
/// Pre-validates all entries, passes valid ones to the engine, and merges
/// validation errors with engine results into `Vec<BatchItemResult>`.
pub fn kv_batch_put(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::types::BatchKvEntry>,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;

    if entries.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let n = entries.len();
    let mut results: Vec<crate::types::BatchItemResult> = vec![
        crate::types::BatchItemResult {
            version: None,
            error: None,
        };
        n
    ];

    // Pre-validate entries, collect valid ones with their original indices
    let mut valid_entries: Vec<(usize, String, Value)> = Vec::with_capacity(n);
    for (i, entry) in entries.into_iter().enumerate() {
        if let Err(e) = validate_key(&entry.key) {
            results[i].error = Some(e.to_string());
            continue;
        }
        if let Err(e) = validate_value(&entry.value, &p.limits) {
            results[i].error = Some(e.to_string());
            continue;
        }
        valid_entries.push((i, entry.key, entry.value));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    // Extract text for embed hooks BEFORE values are consumed
    let embed_data: Vec<(usize, String, Option<String>)> = valid_entries
        .iter()
        .map(|(idx, key, value)| {
            let text = super::embed_hook::extract_text(value);
            (*idx, key.clone(), text)
        })
        .collect();

    // Build engine entries (key, value) pairs
    let engine_entries: Vec<(String, Value)> = valid_entries
        .into_iter()
        .map(|(_, key, value)| (key, value))
        .collect();

    let engine_results = convert_result(p.kv.batch_put(&branch_id, &space, engine_entries))?;

    // Merge engine results back into the results vec
    for (j, (orig_idx, _, _)) in embed_data.iter().enumerate() {
        match &engine_results[j] {
            Ok(version) => {
                results[*orig_idx].version = Some(extract_version(version));
            }
            Err(e) => {
                results[*orig_idx].error = Some(e.clone());
            }
        }
    }

    // Post-commit: fire embed hooks for successful items
    for (_, key, text) in &embed_data {
        if let Some(ref text) = text {
            super::embed_hook::maybe_embed_text(
                p,
                branch_id,
                &space,
                super::embed_hook::SHADOW_KV,
                key,
                text,
                strata_core::EntityRef::kv(branch_id, &space, key),
            );
        }
    }

    Ok(Output::BatchResults(results))
}

/// Handle KvBatchGet command — get multiple values in a single transaction.
pub fn kv_batch_get(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    keys: Vec<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    if keys.is_empty() {
        return Ok(Output::BatchGetResults(Vec::new()));
    }

    let n = keys.len();
    let mut results: Vec<crate::types::BatchGetItemResult> = Vec::with_capacity(n);

    // Pre-validate keys, tracking which are valid
    let mut valid_keys: Vec<(usize, String)> = Vec::with_capacity(n);
    for (i, key) in keys.into_iter().enumerate() {
        if let Err(e) = validate_key(&key) {
            results.push(crate::types::BatchGetItemResult {
                value: None,
                version: None,
                timestamp: None,
                error: Some(e.to_string()),
            });
        } else {
            results.push(crate::types::BatchGetItemResult {
                value: None,
                version: None,
                timestamp: None,
                error: None,
            });
            valid_keys.push((i, key));
        }
    }

    if valid_keys.is_empty() {
        return Ok(Output::BatchGetResults(results));
    }

    let engine_keys: Vec<String> = valid_keys.iter().map(|(_, k)| k.clone()).collect();
    let engine_results = convert_result(p.kv.batch_get(&branch_id, &space, &engine_keys))?;

    for (j, (orig_idx, _)) in valid_keys.iter().enumerate() {
        if let Some(vv) = &engine_results[j] {
            let converted = to_versioned_value(vv.clone());
            results[*orig_idx].value = Some(converted.value);
            results[*orig_idx].version = Some(converted.version);
            results[*orig_idx].timestamp = Some(converted.timestamp);
        }
    }

    Ok(Output::BatchGetResults(results))
}

/// Handle KvBatchDelete command — delete multiple keys in a single transaction.
pub fn kv_batch_delete(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    keys: Vec<String>,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;

    if keys.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let n = keys.len();
    let mut results: Vec<crate::types::BatchItemResult> = vec![
        crate::types::BatchItemResult {
            version: None,
            error: None,
        };
        n
    ];

    // Pre-validate keys
    let mut valid_keys: Vec<(usize, String)> = Vec::with_capacity(n);
    for (i, key) in keys.into_iter().enumerate() {
        if let Err(e) = validate_key(&key) {
            results[i].error = Some(e.to_string());
        } else {
            valid_keys.push((i, key));
        }
    }

    if valid_keys.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    let engine_keys: Vec<String> = valid_keys.iter().map(|(_, k)| k.clone()).collect();
    let engine_results = convert_result(p.kv.batch_delete(&branch_id, &space, &engine_keys))?;

    for (j, (orig_idx, key)) in valid_keys.iter().enumerate() {
        let deleted = engine_results[j];
        // Use version 0 to indicate success (no new version created by delete)
        if deleted {
            results[*orig_idx].version = Some(0);
        }

        // Best-effort remove shadow embedding
        if deleted {
            super::embed_hook::maybe_remove_embedding(
                p,
                branch_id,
                &space,
                super::embed_hook::SHADOW_KV,
                key,
            );
        }
    }

    Ok(Output::BatchResults(results))
}

/// Handle KvBatchExists command — check existence of multiple keys.
pub fn kv_batch_exists(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    keys: Vec<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    if keys.is_empty() {
        return Ok(Output::BoolList(Vec::new()));
    }

    // Validate all keys first
    for key in &keys {
        convert_result(validate_key(key))?;
    }

    let results = convert_result(p.kv.batch_exists(&branch_id, &space, &keys))?;
    Ok(Output::BoolList(results))
}

/// Handle KvCount command — count keys matching a prefix.
///
/// Delegates to `KVStore::count()` which counts scan results without
/// materializing user-key strings, avoiding O(N) String allocations.
pub fn kv_count(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(ref pfx) = prefix {
        if !pfx.is_empty() {
            convert_result(validate_key(pfx))?;
        }
    }
    let count = convert_result(p.kv.count(&branch_id, &space, prefix.as_deref()))?;
    Ok(Output::Uint(count))
}

/// Handle KvSample command — evenly-spaced sample of KV entries.
///
/// Delegates to `KVStore::sample()` which performs a single `scan_prefix`
/// and reads values inline, avoiding separate `list()` + K `get()` round-trips.
pub fn kv_sample(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    count: usize,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(ref pfx) = prefix {
        if !pfx.is_empty() {
            convert_result(validate_key(pfx))?;
        }
    }
    let (total_count, sampled) =
        convert_result(p.kv.sample(&branch_id, &space, prefix.as_deref(), count))?;
    let items = sampled
        .into_iter()
        .map(|(key, value)| crate::types::SampleItem { key, value })
        .collect();
    Ok(Output::SampleResult { total_count, items })
}

/// Enrich a KV error with fuzzy-match suggestions for key-not-found errors.
///
/// Uses the first character of the key as a prefix filter combined with
/// `list_with_cursor_limit` to avoid materializing millions of keys just
/// for fuzzy matching (SCALE-001).
fn enrich_kv_error(
    p: &Arc<Primitives>,
    branch_id: &strata_core::types::BranchId,
    space: &str,
    err: Error,
) -> Error {
    match err {
        Error::KeyNotFound { key, hint: None } => {
            // Use first-char prefix to avoid scanning millions of keys.
            // This misses typos that change the first character, but those
            // are rare compared to mid-key typos.
            let prefix = key.chars().next().map(|c| c.to_string());
            let max_candidates = p.limits.max_fuzzy_candidates;
            let candidates = match p.kv.list_with_cursor_limit(
                branch_id,
                space,
                prefix.as_deref(),
                None,
                max_candidates,
            ) {
                Ok(keys) => keys.into_iter().take(max_candidates).collect::<Vec<_>>(),
                Err(e) => {
                    tracing::debug!("fuzzy suggestion list failed: {e}");
                    vec![]
                }
            };
            let hint = crate::suggest::format_hint("keys", &candidates, &key, 2);
            Error::KeyNotFound { key, hint }
        }
        other => other,
    }
}

/// Handle KvScan command — range query returning key-value pairs.
pub fn kv_scan(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    start: Option<String>,
    limit: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(ref s) = start {
        if !s.is_empty() {
            convert_result(validate_key(s))?;
        }
    }
    let pairs = convert_result(p.kv.scan(
        &branch_id,
        &space,
        start.as_deref(),
        limit.map(|l| l as usize),
    ))?;
    Ok(Output::KvScanResult(pairs))
}

/// Handle KvList with as_of timestamp (time-travel read).
pub fn kv_list_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    as_of_ts: u64,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    if let Some(ref pfx) = prefix {
        if !pfx.is_empty() {
            convert_result(validate_key(pfx))?;
        }
    }
    let keys = convert_result(p.kv.list_at(&branch_id, &space, prefix.as_deref(), as_of_ts))?;
    Ok(Output::Keys(keys))
}
