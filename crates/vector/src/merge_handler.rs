//! Engine bridge for vector-aware merge.
//!
//! The vector crate cannot be a direct dependency of the engine crate
//! (vector already depends on engine — adding the reverse edge would be a
//! cycle). The engine has two callback slots for vector merge:
//! `VectorMergePrecheckFn` and `VectorMergePostCommitFn`. Calling
//! `register_vector_merge` populates both slots with the functions
//! defined here. Engine's `VectorMergeHandler` then dispatches to them
//! during `merge_branches`.
//!
//! ## Why this layering
//!
//! Mirrors the `register_graph_semantic_merge` function-pointer
//! registration pattern. Avoids exposing vector-internal types
//! (`CollectionRecord`, `VectorConfig`) through the engine crate's public
//! API while still letting per-primitive merge logic live in the
//! primitive's own crate.
//!
//! ## What runs here
//!
//! - **`vector_precheck_fn`** scans every space's `__config__/{collection}`
//!   rows on both source and target branches, decodes the configs, and
//!   refuses the merge if any shared collection has a different dimension
//!   or distance metric. These mismatches are never auto-mergeable
//!   regardless of `MergeStrategy` — combining vectors of different
//!   dimensions into one HNSW would corrupt the index.
//!
//! - **`vector_post_commit_fn`** receives the set of `(space, collection)`
//!   pairs the engine recorded during `plan` and rebuilds each affected
//!   collection's HNSW backend from KV via
//!   `VectorStore::rebuild_collection_after_merge`. Untouched collections
//!   are left alone — this is the per-collection rebuild that replaces
//!   the legacy "rebuild every collection in the branch on every merge"
//!   behavior of `VectorRefreshHook::post_merge_reload`.
//!
//! ## Lifecycle
//!
//! Test fixtures and application startup must call
//! `register_vector_merge` before any `merge_branches` invocation. The
//! standard test fixtures register it alongside
//! `register_graph_semantic_merge` and the branch DAG hook. Idempotent —
//! the engine's `OnceCell`-backed slot ignores subsequent calls.

use std::collections::BTreeSet;
use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::traits::Storage;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::{StrataError, StrataResult};
use strata_engine::{register_vector_merge, Database};

use crate::{CollectionRecord, VectorStore};

/// Register the vector semantic merge implementation with the engine.
///
/// Idempotent: the engine's `OnceCell` slot only accepts the first call.
/// Test fixtures and application startup should call this exactly once,
/// before any `merge_branches` call.
pub fn register_vector_semantic_merge() {
    register_vector_merge(vector_precheck_fn, vector_post_commit_fn);
}

/// Precheck callback: refuse merges that combine collections with
/// incompatible configurations.
///
/// For every collection that exists on both source and target with the
/// same name, decode `CollectionRecord` on both sides and compare the
/// dimension and distance metric. Any mismatch returns an error,
/// aborting the merge with no writes.
///
/// Walks every space the SpaceIndex knows about for the source and
/// target branches (a collection in `tenant_a` on source vs the same
/// name in `tenant_b` on target is unrelated and not validated).
pub fn vector_precheck_fn(
    db: &Arc<Database>,
    source: BranchId,
    target: BranchId,
) -> StrataResult<()> {
    let space_index = strata_engine::SpaceIndex::new(db.clone());
    let source_spaces: Vec<String> = space_index.list(source)?;
    let target_spaces: Vec<String> = space_index.list(target)?;

    // We only care about spaces that exist on BOTH sides — a collection
    // that exists only on one side can't have a config mismatch.
    let target_set: std::collections::HashSet<&str> =
        target_spaces.iter().map(|s| s.as_str()).collect();
    let shared_spaces: Vec<&String> = source_spaces
        .iter()
        .filter(|s| target_set.contains(s.as_str()))
        .collect();

    let storage = db.storage();
    let version = CommitVersion(storage.version());

    for space in shared_spaces {
        let source_ns = Arc::new(Namespace::for_branch_space(source, space));
        let target_ns = Arc::new(Namespace::for_branch_space(target, space));

        // Pre-fetch all configs for this space on both sides.
        let source_configs =
            decode_configs_for_space(db, &Key::new_vector_config_prefix(source_ns), version);
        let target_configs =
            decode_configs_for_space(db, &Key::new_vector_config_prefix(target_ns), version);

        for (collection_name, source_record) in &source_configs {
            let Some(target_record) = target_configs.get(collection_name) else {
                continue; // Collection only on source — nothing to compare
            };

            let source_dim = source_record.config.dimension;
            let target_dim = target_record.config.dimension;
            if source_dim != target_dim {
                return Err(StrataError::invalid_input(format!(
                    "merge unsupported: vector collection '{collection_name}' in space '{space}' \
                     has incompatible dimensions (source={source_dim}, target={target_dim}). \
                     Vector dimensions cannot be merged automatically — recreate the collection \
                     with a consistent dimension before merging."
                )));
            }

            // metric and storage_dtype are stored as bytes; comparing the
            // bytes is equivalent to comparing the decoded variants.
            let source_metric_byte = source_record.config.metric;
            let target_metric_byte = target_record.config.metric;
            if source_metric_byte != target_metric_byte {
                let source_metric = crate::DistanceMetric::from_byte(source_metric_byte);
                let target_metric = crate::DistanceMetric::from_byte(target_metric_byte);
                return Err(StrataError::invalid_input(format!(
                    "merge unsupported: vector collection '{collection_name}' in space '{space}' \
                     has incompatible distance metrics \
                     (source={source_metric:?}, target={target_metric:?}). \
                     Distance metrics cannot be merged automatically — recreate the collection \
                     with a consistent metric before merging."
                )));
            }

            let source_dtype = source_record.config.storage_dtype;
            let target_dtype = target_record.config.storage_dtype;
            if source_dtype != target_dtype {
                return Err(StrataError::invalid_input(format!(
                    "merge unsupported: vector collection '{collection_name}' in space '{space}' \
                     has incompatible storage dtypes \
                     (source={source_dtype}, target={target_dtype}). \
                     Storage dtypes cannot be merged automatically — recreate the collection \
                     with a consistent storage dtype before merging."
                )));
            }
        }
    }

    Ok(())
}

/// Read every `__config__/{collection}` row under `prefix` and return a
/// map from collection name to decoded `CollectionRecord`. Bytes that
/// fail to decode are silently skipped — they cannot meaningfully
/// participate in a config-mismatch check.
fn decode_configs_for_space(
    db: &Arc<Database>,
    prefix: &Key,
    version: CommitVersion,
) -> std::collections::BTreeMap<String, CollectionRecord> {
    let mut out = std::collections::BTreeMap::new();
    let entries = match db.storage().scan_prefix(prefix, version) {
        Ok(entries) => entries,
        Err(_) => return out,
    };
    for (key, versioned) in &entries {
        let bytes = match &versioned.value {
            Value::Bytes(b) => b,
            _ => continue,
        };
        let record = match CollectionRecord::from_bytes(bytes) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let Some(user_key) = key.user_key_string() else {
            continue;
        };
        let Some(name) = user_key.strip_prefix("__config__/") else {
            continue;
        };
        out.insert(name.to_string(), record);
    }
    out
}

/// Post-commit callback: rebuild the HNSW backend for each affected
/// `(space, collection)` pair.
///
/// Failures on individual collections are logged as warnings and the
/// loop continues — at this point the merge has already committed to KV,
/// so propagating an error would leave the database in a state where the
/// KV is correct but the in-memory backend is partially stale. The next
/// branch open / full recovery will catch up via the existing recovery
/// path. The merge `MergeInfo` returned to the caller still reflects
/// success.
pub fn vector_post_commit_fn(
    db: &Arc<Database>,
    source: BranchId,
    target: BranchId,
    affected: &BTreeSet<(String, String)>,
) -> StrataResult<()> {
    if affected.is_empty() {
        return Ok(());
    }

    let store = VectorStore::new(db.clone());
    for (space, collection_name) in affected {
        if let Err(e) =
            store.rebuild_collection_after_merge(target, Some(source), space, collection_name)
        {
            tracing::warn!(
                target: "strata::vector",
                space = %space,
                collection = %collection_name,
                error = %e,
                "Failed to rebuild collection after merge — falling back to next recovery"
            );
        }
    }
    Ok(())
}
