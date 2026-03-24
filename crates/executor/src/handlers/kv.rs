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
            strata_core::EntityRef::kv(branch_id, &key),
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
    let result = convert_result(p.kv.get_versioned(&branch_id, &space, &key))
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
    let keys = convert_result(p.kv.list(&branch_id, &space, prefix.as_deref()))?;

    // Apply cursor-based pagination if limit is present
    if let Some(lim) = limit {
        let start_idx = if let Some(ref cur) = cursor {
            keys.iter().position(|k| k > cur).unwrap_or(keys.len())
        } else {
            0
        };
        let fetch_end = std::cmp::min(start_idx + lim as usize + 1, keys.len());
        let fetched = &keys[start_idx..fetch_end];
        let has_more = fetched.len() > lim as usize;
        let page: Vec<String> = fetched.iter().take(lim as usize).cloned().collect();
        let next_cursor = if has_more { page.last().cloned() } else { None };
        Ok(Output::KeysPage {
            keys: page,
            has_more,
            cursor: next_cursor,
        })
    } else {
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
                strata_core::EntityRef::kv(branch_id, key),
            );
        }
    }

    Ok(Output::BatchResults(results))
}

/// Handle KvCount command — count keys matching a prefix.
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
    let keys = convert_result(p.kv.list(&branch_id, &space, prefix.as_deref()))?;
    Ok(Output::Uint(keys.len() as u64))
}

/// Handle KvSample command — evenly-spaced sample of KV entries.
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
    let keys = convert_result(p.kv.list(&branch_id, &space, prefix.as_deref()))?;
    let total = keys.len() as u64;
    let indices = super::sample_indices(keys.len(), count);
    let mut items = Vec::with_capacity(indices.len());
    for idx in indices {
        let key = &keys[idx];
        let value = match convert_result(p.kv.get(&branch_id, &space, key))? {
            Some(v) => v,
            None => continue,
        };
        items.push(crate::types::SampleItem {
            key: key.clone(),
            value,
        });
    }
    Ok(Output::SampleResult {
        total_count: total,
        items,
    })
}

/// Maximum number of keys to scan for fuzzy matching suggestions.
const MAX_FUZZY_CANDIDATES: usize = 100;

/// Enrich a KV error with fuzzy-match suggestions for key-not-found errors.
///
/// Uses the first character of the key as a prefix filter to avoid a full
/// key scan on spaces with many keys. The KV list primitive has no limit
/// parameter, so prefix narrowing is the main safeguard.
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
            let candidates =
                p.kv.list(branch_id, space, prefix.as_deref())
                    .unwrap_or_default()
                    .into_iter()
                    .take(MAX_FUZZY_CANDIDATES)
                    .collect::<Vec<_>>();
            let hint = crate::suggest::format_hint("keys", &candidates, &key, 2);
            Error::KeyNotFound { key, hint }
        }
        other => other,
    }
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
