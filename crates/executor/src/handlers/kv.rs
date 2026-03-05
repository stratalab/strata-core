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
use crate::{Output, Result};

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
    let result = convert_result(p.kv.getv(&branch_id, &space, &key))?;
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

    Ok(Output::Version(extract_version(&version)))
}

/// Handle KvGet command.
///
/// Returns `MaybeVersioned` with value, version, and timestamp metadata.
pub fn kv_get(p: &Arc<Primitives>, branch: BranchId, space: String, key: String) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let result = convert_result(p.kv.get_versioned(&branch_id, &space, &key))?;
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
    let result = convert_result(p.kv.get_at(&branch_id, &space, &key, as_of_ts))?;
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

    Ok(Output::Bool(existed))
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
        let end_idx = std::cmp::min(start_idx + lim as usize, keys.len());
        let page = keys[start_idx..end_idx].to_vec();
        Ok(Output::Keys(page))
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
