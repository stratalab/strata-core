//! JSON command handlers (MVP).
//!
//! This module implements handlers for the 4 MVP JSON commands.

use std::sync::Arc;

use strata_core::Value;

use crate::bridge::{
    extract_version, json_to_value, parse_path, to_core_branch_id, validate_key, validate_value,
    value_to_json, Primitives,
};
use crate::convert::convert_result;
use crate::types::{BatchGetItemResult, BatchItemResult, BranchId, VersionedValue};
use crate::{Output, Result};

use super::require_branch_exists;

/// Handle JsonGetv command — get full version history for a JSON document.
pub fn json_getv(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let result = convert_result(p.json.getv(&branch_id, &space, &key))
        .map_err(|e| enrich_json_error(p, &branch_id, &space, e))?;
    let mapped = result
        .map(|history| {
            history
                .into_versions()
                .into_iter()
                .map(|v| {
                    let value = convert_result(json_to_value(v.value))?;
                    Ok(VersionedValue {
                        value,
                        version: extract_version(&v.version),
                        timestamp: v.timestamp.into(),
                    })
                })
                .collect::<Result<Vec<VersionedValue>>>()
        })
        .transpose()?;
    Ok(Output::VersionHistory(mapped))
}

// =============================================================================
// MVP Handlers (4)
// =============================================================================

/// Handle JsonSet command.
///
/// Auto-creation logic:
/// - If doc doesn't exist and path is root: create the document.
/// - If doc doesn't exist and path is non-root: create with empty object, then set at path.
/// - If doc exists: set at path.
pub fn json_set(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    path: String,
    value: Value,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    convert_result(validate_value(&value, &p.limits))?;

    let json_path = convert_result(parse_path(&path))?;
    let json_value = convert_result(value_to_json(value))?;

    // Single atomic transaction: checks existence, creates if needed, sets at path.
    // Produces exactly 1 WAL append (fixes #973).
    let version = convert_result(
        p.json
            .set_or_create(&branch_id, &space, &key, &json_path, json_value),
    )?;
    let v = extract_version(&version);

    // Best-effort auto-embed: read back the full document so we embed the complete
    // content, not just the fragment written at this path.
    embed_full_doc(p, branch_id, &space, &key);

    Ok(Output::WriteResult { key, version: v })
}

/// Handle JsonGet command.
///
/// Returns `MaybeVersioned` with value, version, and timestamp metadata.
pub fn json_get(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    path: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let json_path = convert_result(parse_path(&path))?;

    let result = convert_result(p.json.get_versioned(&branch_id, &space, &key, &json_path))
        .map_err(|e| enrich_json_error(p, &branch_id, &space, e))?;
    match result {
        Some(versioned) => {
            let value = convert_result(json_to_value(versioned.value))?;
            Ok(Output::MaybeVersioned(Some(VersionedValue {
                value,
                version: extract_version(&versioned.version),
                timestamp: versioned.timestamp.into(),
            })))
        }
        None => Ok(Output::MaybeVersioned(None)),
    }
}

/// Handle JsonGet with as_of timestamp (time-travel read).
pub fn json_get_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    path: String,
    as_of_ts: u64,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let json_path = convert_result(parse_path(&path))?;

    let result = convert_result(
        p.json
            .get_at(&branch_id, &space, &key, &json_path, as_of_ts),
    )
    .map_err(|e| enrich_json_error(p, &branch_id, &space, e))?;
    match result {
        Some(json_val) => {
            let value = convert_result(json_to_value(json_val))?;
            Ok(Output::Maybe(Some(value)))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Handle JsonDelete command.
///
/// - Root path: destroy entire document (returns 1 if existed, 0 otherwise).
/// - Non-root path: delete at path (returns 1).
pub fn json_delete(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    key: String,
    path: String,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_key(&key))?;
    let json_path = convert_result(parse_path(&path))?;

    if json_path.is_root() {
        let deleted = convert_result(p.json.destroy(&branch_id, &space, &key))?;

        // Best-effort remove shadow embedding when entire document is destroyed
        if deleted {
            super::embed_hook::maybe_remove_embedding(
                p,
                branch_id,
                &space,
                super::embed_hook::SHADOW_JSON,
                &key,
            );
        }

        Ok(Output::DeleteResult { key, deleted })
    } else {
        match p.json.delete_at_path(&branch_id, &space, &key, &json_path) {
            Ok(_) => {
                // Re-embed the remaining document after sub-path deletion
                embed_full_doc(p, branch_id, &space, &key);
                Ok(Output::DeleteResult { key, deleted: true })
            }
            Err(e) => {
                // If path not found, return deleted=false (nothing deleted)
                let err = crate::Error::from(e);
                match err {
                    crate::Error::InvalidPath { .. } | crate::Error::InvalidInput { .. } => {
                        Ok(Output::DeleteResult {
                            key,
                            deleted: false,
                        })
                    }
                    other => Err(other),
                }
            }
        }
    }
}

/// Handle JsonBatchSet command.
///
/// Pre-validates all entries, passes valid ones to the engine, and merges
/// validation errors with engine results into `Vec<BatchItemResult>`.
pub fn json_batch_set(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::types::BatchJsonEntry>,
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

    // Pre-validate entries, convert types
    let mut valid_entries: Vec<(
        usize,
        String,
        strata_core::primitives::json::JsonPath,
        strata_core::primitives::json::JsonValue,
    )> = Vec::with_capacity(n);

    for (i, entry) in entries.into_iter().enumerate() {
        if let Err(e) = validate_key(&entry.key) {
            results[i].error = Some(e.to_string());
            continue;
        }
        if let Err(e) = validate_value(&entry.value, &p.limits) {
            results[i].error = Some(e.to_string());
            continue;
        }
        let json_path = match parse_path(&entry.path) {
            Ok(p) => p,
            Err(e) => {
                results[i].error = Some(e.to_string());
                continue;
            }
        };
        let json_value = match value_to_json(entry.value) {
            Ok(v) => v,
            Err(e) => {
                results[i].error = Some(e.to_string());
                continue;
            }
        };
        valid_entries.push((i, entry.key, json_path, json_value));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    // Collect original indices for merging
    let orig_indices: Vec<usize> = valid_entries.iter().map(|(idx, _, _, _)| *idx).collect();
    let keys: Vec<String> = valid_entries.iter().map(|(_, k, _, _)| k.clone()).collect();

    // Build engine entries
    let engine_entries: Vec<(
        String,
        strata_core::primitives::json::JsonPath,
        strata_core::primitives::json::JsonValue,
    )> = valid_entries
        .into_iter()
        .map(|(_, key, path, value)| (key, path, value))
        .collect();

    let engine_results = convert_result(p.json.batch_set_or_create(
        &branch_id,
        &space,
        engine_entries,
    ))?;

    // Merge engine results
    for (j, orig_idx) in orig_indices.iter().enumerate() {
        results[*orig_idx].version = Some(extract_version(&engine_results[j]));
    }

    // Post-commit: fire embed hooks for successful items
    for (j, orig_idx) in orig_indices.iter().enumerate() {
        if results[*orig_idx].version.is_some() {
            embed_full_doc(p, branch_id, &space, &keys[j]);
        }
    }

    Ok(Output::BatchResults(results))
}

/// Handle JsonBatchGet command.
///
/// Loops over entries, calling `get_versioned()` for each. No engine batch
/// method needed since reads are independent.
pub fn json_batch_get(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::types::BatchJsonGetEntry>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    if entries.is_empty() {
        return Ok(Output::BatchGetResults(Vec::new()));
    }

    let n = entries.len();
    let mut results: Vec<BatchGetItemResult> = Vec::with_capacity(n);

    for entry in entries {
        // Validate key
        if let Err(e) = validate_key(&entry.key) {
            results.push(BatchGetItemResult {
                value: None,
                version: None,
                timestamp: None,
                error: Some(e.to_string()),
            });
            continue;
        }
        // Parse path
        let json_path = match parse_path(&entry.path) {
            Ok(p) => p,
            Err(e) => {
                results.push(BatchGetItemResult {
                    value: None,
                    version: None,
                    timestamp: None,
                    error: Some(e.to_string()),
                });
                continue;
            }
        };
        // Get from engine
        match convert_result(
            p.json
                .get_versioned(&branch_id, &space, &entry.key, &json_path),
        ) {
            Ok(Some(versioned)) => match convert_result(json_to_value(versioned.value)) {
                Ok(value) => {
                    results.push(BatchGetItemResult {
                        value: Some(value),
                        version: Some(extract_version(&versioned.version)),
                        timestamp: Some(versioned.timestamp.into()),
                        error: None,
                    });
                }
                Err(e) => {
                    results.push(BatchGetItemResult {
                        value: None,
                        version: None,
                        timestamp: None,
                        error: Some(e.to_string()),
                    });
                }
            },
            Ok(None) => {
                results.push(BatchGetItemResult {
                    value: None,
                    version: None,
                    timestamp: None,
                    error: None,
                });
            }
            Err(e) => {
                results.push(BatchGetItemResult {
                    value: None,
                    version: None,
                    timestamp: None,
                    error: Some(e.to_string()),
                });
            }
        }
    }

    Ok(Output::BatchGetResults(results))
}

/// Handle JsonBatchDelete command.
///
/// Loops over entries. Root path deletes destroy the entire document;
/// non-root deletes remove at the specified path. Reuses `BatchItemResult`
/// with `version` as count (1=deleted, 0=not found).
pub fn json_batch_delete(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::types::BatchJsonDeleteEntry>,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;

    if entries.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let n = entries.len();
    let mut results: Vec<BatchItemResult> = vec![
        BatchItemResult {
            version: None,
            error: None,
        };
        n
    ];

    for (i, entry) in entries.into_iter().enumerate() {
        if let Err(e) = validate_key(&entry.key) {
            results[i].error = Some(e.to_string());
            continue;
        }
        let json_path = match parse_path(&entry.path) {
            Ok(p) => p,
            Err(e) => {
                results[i].error = Some(e.to_string());
                continue;
            }
        };

        if json_path.is_root() {
            // Root delete — destroy entire document
            match convert_result(p.json.destroy(&branch_id, &space, &entry.key)) {
                Ok(deleted) => {
                    results[i].version = Some(if deleted { 1 } else { 0 });
                    if deleted {
                        super::embed_hook::maybe_remove_embedding(
                            p,
                            branch_id,
                            &space,
                            super::embed_hook::SHADOW_JSON,
                            &entry.key,
                        );
                    }
                }
                Err(e) => results[i].error = Some(e.to_string()),
            }
        } else {
            // Non-root — delete at path
            // Match single json_delete behavior: call without convert_result,
            // then match on error variants directly.
            match p
                .json
                .delete_at_path(&branch_id, &space, &entry.key, &json_path)
            {
                Ok(_version) => {
                    results[i].version = Some(1);
                    embed_full_doc(p, branch_id, &space, &entry.key);
                }
                Err(e) => {
                    let err = crate::Error::from(e);
                    match err {
                        crate::Error::InvalidPath { .. } | crate::Error::InvalidInput { .. } => {
                            results[i].version = Some(0);
                        }
                        other => {
                            results[i].error = Some(other.to_string());
                        }
                    }
                }
            }
        }
    }

    Ok(Output::BatchResults(results))
}

/// Handle JsonList command.
pub fn json_list(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: u64,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    let result = convert_result(p.json.list(
        &branch_id,
        &space,
        prefix.as_deref(),
        cursor.as_deref(),
        limit as usize,
    ))?;

    let has_more = result.next_cursor.is_some();
    Ok(Output::JsonListResult {
        keys: result.doc_ids,
        has_more,
        cursor: result.next_cursor,
    })
}

/// Handle JsonCount command — count documents matching a prefix.
pub fn json_count(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    // Use paginated list with a large limit to count documents
    let mut total = 0u64;
    let mut cursor: Option<String> = None;
    loop {
        let result = convert_result(p.json.list(
            &branch_id,
            &space,
            prefix.as_deref(),
            cursor.as_deref(),
            1000,
        ))?;
        total += result.doc_ids.len() as u64;
        match result.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok(Output::Uint(total))
}

/// Handle JsonSample command — evenly-spaced sample of JSON documents.
pub fn json_sample(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    count: usize,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    // Collect all document keys first
    let mut all_keys = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let result = convert_result(p.json.list(
            &branch_id,
            &space,
            prefix.as_deref(),
            cursor.as_deref(),
            1000,
        ))?;
        all_keys.extend(result.doc_ids);
        match result.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    let total = all_keys.len() as u64;
    let indices = super::sample_indices(all_keys.len(), count);
    let mut items = Vec::with_capacity(indices.len());
    for idx in indices {
        let key = &all_keys[idx];
        let json_path = strata_core::primitives::json::JsonPath::root();
        let result = convert_result(p.json.get(&branch_id, &space, key, &json_path))?;
        if let Some(json_val) = result {
            if let Ok(value) = convert_result(json_to_value(json_val)) {
                items.push(crate::types::SampleItem {
                    key: key.clone(),
                    value,
                });
            }
        }
    }
    Ok(Output::SampleResult {
        total_count: total,
        items,
    })
}

/// Maximum number of document keys to scan for fuzzy matching suggestions.
const MAX_FUZZY_CANDIDATES: usize = 100;

/// Enrich a JSON error with fuzzy-match suggestions for document-not-found errors.
fn enrich_json_error(
    p: &Arc<Primitives>,
    branch_id: &strata_core::types::BranchId,
    space: &str,
    err: crate::Error,
) -> crate::Error {
    match err {
        crate::Error::DocumentNotFound { key, hint: None } => {
            // Use paginated list to get up to MAX_FUZZY_CANDIDATES document keys
            let candidates = p
                .json
                .list(branch_id, space, None, None, MAX_FUZZY_CANDIDATES)
                .ok()
                .map(|r| r.doc_ids)
                .unwrap_or_default();
            let hint = crate::suggest::format_hint("documents", &candidates, &key, 2);
            crate::Error::DocumentNotFound { key, hint }
        }
        other => other,
    }
}

/// Best-effort: read back the full JSON document and embed its complete text.
///
/// This ensures that partial-path writes (e.g. `$.name`) produce an embedding
/// that reflects the entire document, not just the written fragment.
fn embed_full_doc(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    key: &str,
) {
    use strata_core::primitives::json::JsonPath;

    let full_doc = p.json.get(&branch_id, space, key, &JsonPath::root());
    match full_doc {
        Ok(Some(json_val)) => {
            if let Ok(value) = json_to_value(json_val) {
                if let Some(text) = super::embed_hook::extract_text(&value) {
                    super::embed_hook::maybe_embed_text(
                        p,
                        branch_id,
                        space,
                        super::embed_hook::SHADOW_JSON,
                        key,
                        &text,
                        strata_core::EntityRef::json(branch_id, key),
                    );
                }
            }
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(
                target: "strata::embed",
                key = key,
                error = %e,
                "Failed to read back document for embedding"
            );
        }
    }
}

/// Handle JsonList with as_of timestamp (time-travel list).
///
/// Returns only document IDs that existed at or before the given timestamp.
/// Does not support cursor-based pagination (returns all matching docs).
pub fn json_list_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    prefix: Option<String>,
    as_of_ts: u64,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let keys = convert_result(
        p.json
            .list_at(&branch_id, &space, prefix.as_deref(), as_of_ts),
    )?;
    Ok(Output::JsonListResult {
        keys,
        has_more: false,
        cursor: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_core_branch_id_default() {
        let branch = BranchId::from("default");
        let core_id = to_core_branch_id(&branch).unwrap();
        assert_eq!(core_id.as_bytes(), &[0u8; 16]);
    }

    #[test]
    fn test_extract_version() {
        use strata_core::Version;
        assert_eq!(extract_version(&Version::Txn(42)), 42);
        assert_eq!(extract_version(&Version::Counter(100)), 100);
    }
}
