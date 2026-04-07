//! Per-document JSON three-way merge.
//!
//! Used by `JsonMergeHandler` to merge two divergent revisions of the same
//! `JsonDoc` by walking object keys recursively. Disjoint changes from both
//! sides combine into a single merged value; same-path edits to different
//! values produce a path-level conflict that honors `MergeStrategy`.
//!
//! ## Algorithm
//!
//! For each `(ancestor, source, target)` triple of `JsonValue`s:
//!
//! 1. If all three (or both extant ones) are JSON objects, recurse per-key
//!    over the union of their key sets and merge subtrees independently.
//! 2. Otherwise (scalars, arrays, or type-mixed sides) treat the value as
//!    opaque and apply the standard three-way classification:
//!    - source == target → no conflict, use either.
//!    - source == ancestor → target made the change, use target.
//!    - target == ancestor → source made the change, use source.
//!    - all three differ → path-level conflict, LWW resolves by picking
//!      source's value.
//!
//! Arrays are intentionally treated as opaque. Element-level array merging
//! requires LCS or operational-transform algorithms that are out of scope
//! for the first JSON merge cut. Object-level merging covers the disjoint
//! key-edits case that the design calls out.
//!
//! ## Path strings
//!
//! Conflict paths are reported as dot-separated strings (e.g. `user.name`)
//! using the same convention as `JsonPath::to_path_string`. Path strings
//! are surfaced in `ConflictEntry::key` so callers can locate the
//! conflicting subtree without re-running the diff.

use serde_json::{Map, Value as JsonInner};
use strata_core::primitives::json::JsonValue;

use super::MergeStrategy;

/// One per-doc merge result for a single JSON document key.
pub(crate) struct JsonDocMergeResult {
    /// The merged value to write back to the target document, or `None` if
    /// the merge concluded the target should be unchanged.
    pub merged: Option<JsonValue>,
    /// Path-level conflicts surfaced during the recursive merge. An empty
    /// vector means the merge auto-completed without conflict. Non-empty
    /// means at least one path collided; under `LastWriterWins` the
    /// returned `merged` value already reflects the source-wins resolution,
    /// while under `Strict` the caller must surface the conflicts and
    /// abort the merge.
    pub conflict_paths: Vec<String>,
}

/// Three-way merge two non-tombstoned `JsonValue`s.
///
/// `ancestor` is the common ancestor (may be `None` if both sides added
/// the document). `source` and `target` are both non-`None` here — caller
/// handles the cases where one side deleted the entire document at the
/// `JsonDoc` envelope level (delete/modify, modify/delete, both-deleted).
///
/// Returns the merged value and any path-level conflicts. The merged
/// value is always populated source-wins at conflicting paths regardless
/// of `strategy` — the strategy is accepted on the public API for
/// symmetry with the engine's per-handler dispatch but not consumed
/// internally, since the engine caller (`JsonMergeHandler::plan`)
/// decides whether to commit (LWW) or surface the conflict and abort
/// (Strict) once it sees `conflict_paths`.
pub(crate) fn merge_json_values(
    ancestor: Option<&JsonValue>,
    source: &JsonValue,
    target: &JsonValue,
    _strategy: MergeStrategy,
) -> JsonDocMergeResult {
    let mut conflicts: Vec<String> = Vec::new();
    let merged_inner = merge_recursive(
        ancestor.map(|v| v.as_inner()),
        Some(source.as_inner()),
        Some(target.as_inner()),
        &mut Vec::new(),
        &mut conflicts,
    );
    JsonDocMergeResult {
        merged: merged_inner.map(JsonValue::from),
        conflict_paths: conflicts,
    }
}

/// Internal recursive worker. Returns `Some(merged_inner)` for a value to
/// keep at this position, `None` for a tombstone (delete). The `path`
/// stack carries the current location for conflict reporting.
fn merge_recursive(
    ancestor: Option<&JsonInner>,
    source: Option<&JsonInner>,
    target: Option<&JsonInner>,
    path: &mut Vec<String>,
    conflicts: &mut Vec<String>,
) -> Option<JsonInner> {
    // Both sides agree → trivially merged regardless of ancestor.
    match (source, target) {
        (None, None) => return None,
        (Some(s), Some(t)) if s == t => return Some(s.clone()),
        _ => {}
    }

    // Single-sided change relative to ancestor: keep the side that diverged.
    if source == ancestor {
        return target.cloned();
    }
    if target == ancestor {
        return source.cloned();
    }

    // True divergence. If both sides are objects, recurse per-key. Anything
    // else (arrays, scalars, type-mixed) is treated as an opaque conflict.
    if let (Some(JsonInner::Object(src_obj)), Some(JsonInner::Object(tgt_obj))) = (source, target) {
        let anc_obj = match ancestor {
            Some(JsonInner::Object(o)) => Some(o),
            _ => None,
        };
        return Some(merge_objects(anc_obj, src_obj, tgt_obj, path, conflicts));
    }

    // Opaque divergence at this path → conflict. Source-wins
    // resolution; engine decides whether to commit or abort.
    record_conflict(path, conflicts);
    source.cloned()
}

/// Recursive object merge. Walks the union of keys from
/// (ancestor, source, target) and applies `merge_recursive` to each value.
fn merge_objects(
    ancestor: Option<&Map<String, JsonInner>>,
    source: &Map<String, JsonInner>,
    target: &Map<String, JsonInner>,
    path: &mut Vec<String>,
    conflicts: &mut Vec<String>,
) -> JsonInner {
    use std::collections::BTreeSet;
    let mut all_keys: BTreeSet<&String> = BTreeSet::new();
    if let Some(a) = ancestor {
        all_keys.extend(a.keys());
    }
    all_keys.extend(source.keys());
    all_keys.extend(target.keys());

    let mut merged = Map::new();
    for key in all_keys {
        let anc_v = ancestor.and_then(|m| m.get(key));
        let src_v = source.get(key);
        let tgt_v = target.get(key);

        path.push(key.clone());
        let merged_v = merge_recursive(anc_v, src_v, tgt_v, path, conflicts);
        path.pop();

        if let Some(v) = merged_v {
            merged.insert(key.clone(), v);
        }
        // None at this key means "delete" — leave the key out of the
        // merged object.
    }
    JsonInner::Object(merged)
}

/// Append the current `path` stack to `conflicts` as a dot-joined string.
/// Empty path → root.
fn record_conflict(path: &[String], conflicts: &mut Vec<String>) {
    if path.is_empty() {
        conflicts.push("$".to_string());
    } else {
        conflicts.push(path.join("."));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn jv(v: serde_json::Value) -> JsonValue {
        JsonValue::from(v)
    }

    #[test]
    fn disjoint_top_level_keys_auto_merge() {
        let ancestor = jv(json!({"a": 1}));
        let source = jv(json!({"a": 1, "b": 2}));
        let target = jv(json!({"a": 1, "c": 3}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert!(result.conflict_paths.is_empty());
        let merged = result.merged.unwrap();
        assert_eq!(merged.as_inner(), &json!({"a": 1, "b": 2, "c": 3}));
    }

    #[test]
    fn disjoint_nested_paths_auto_merge() {
        let ancestor = jv(json!({"user": {"name": "Alice", "age": 30}}));
        let source = jv(json!({"user": {"name": "Bob", "age": 30}}));
        let target = jv(json!({"user": {"name": "Alice", "age": 31}}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert!(result.conflict_paths.is_empty());
        let merged = result.merged.unwrap();
        assert_eq!(
            merged.as_inner(),
            &json!({"user": {"name": "Bob", "age": 31}})
        );
    }

    #[test]
    fn same_path_same_value_no_conflict() {
        let ancestor = jv(json!({"a": 1}));
        let source = jv(json!({"a": 2}));
        let target = jv(json!({"a": 2}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert!(result.conflict_paths.is_empty());
        assert_eq!(result.merged.unwrap().as_inner(), &json!({"a": 2}));
    }

    #[test]
    fn same_path_different_values_lww_picks_source() {
        let ancestor = jv(json!({"a": 1}));
        let source = jv(json!({"a": 2}));
        let target = jv(json!({"a": 3}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert_eq!(result.conflict_paths, vec!["a".to_string()]);
        assert_eq!(result.merged.unwrap().as_inner(), &json!({"a": 2}));
    }

    #[test]
    fn key_added_on_one_side_only() {
        let ancestor = jv(json!({"a": 1}));
        let source = jv(json!({"a": 1, "b": 2}));
        let target = jv(json!({"a": 1}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert!(result.conflict_paths.is_empty());
        assert_eq!(result.merged.unwrap().as_inner(), &json!({"a": 1, "b": 2}));
    }

    #[test]
    fn key_deleted_on_source_kept_on_target_unchanged() {
        let ancestor = jv(json!({"a": 1, "b": 2}));
        let source = jv(json!({"a": 1}));
        let target = jv(json!({"a": 1, "b": 2}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert!(result.conflict_paths.is_empty());
        // Source deleted "b", target unchanged → b is removed.
        assert_eq!(result.merged.unwrap().as_inner(), &json!({"a": 1}));
    }

    #[test]
    fn key_deleted_on_one_side_modified_on_other_conflicts() {
        let ancestor = jv(json!({"a": 1, "b": 2}));
        let source = jv(json!({"a": 1}));
        let target = jv(json!({"a": 1, "b": 99}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert_eq!(result.conflict_paths, vec!["b".to_string()]);
        // LWW: source wins → "b" is removed in merged result.
        assert_eq!(result.merged.unwrap().as_inner(), &json!({"a": 1}));
    }

    #[test]
    fn array_treated_as_opaque_conflict() {
        // Array element-level merge is out of scope: differing arrays
        // generate a path-level conflict.
        let ancestor = jv(json!({"items": [1, 2, 3]}));
        let source = jv(json!({"items": [1, 2, 3, 4]}));
        let target = jv(json!({"items": [0, 1, 2, 3]}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert_eq!(result.conflict_paths, vec!["items".to_string()]);
        // LWW: source array wins.
        assert_eq!(
            result.merged.unwrap().as_inner(),
            &json!({"items": [1, 2, 3, 4]})
        );
    }

    #[test]
    fn array_unchanged_on_one_side_keeps_other_side_change() {
        // If one side left the array alone, the other side's array
        // version wins without a conflict — this is the "single-sided
        // change" branch in merge_recursive.
        let ancestor = jv(json!({"items": [1, 2, 3]}));
        let source = jv(json!({"items": [1, 2, 3, 4]}));
        let target = jv(json!({"items": [1, 2, 3]}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert!(result.conflict_paths.is_empty());
        assert_eq!(
            result.merged.unwrap().as_inner(),
            &json!({"items": [1, 2, 3, 4]})
        );
    }

    #[test]
    fn no_ancestor_both_added_same_value() {
        // Both sides added the same key with the same value → no conflict.
        let source = jv(json!({"a": 1}));
        let target = jv(json!({"a": 1}));

        let result = merge_json_values(None, &source, &target, MergeStrategy::LastWriterWins);
        assert!(result.conflict_paths.is_empty());
        assert_eq!(result.merged.unwrap().as_inner(), &json!({"a": 1}));
    }

    #[test]
    fn no_ancestor_both_added_disjoint_keys_auto_merge() {
        // Both sides added the same doc but with disjoint top-level keys.
        let source = jv(json!({"a": 1}));
        let target = jv(json!({"b": 2}));

        let result = merge_json_values(None, &source, &target, MergeStrategy::LastWriterWins);
        assert!(result.conflict_paths.is_empty());
        assert_eq!(result.merged.unwrap().as_inner(), &json!({"a": 1, "b": 2}));
    }

    #[test]
    fn deep_nested_conflict_path_reported() {
        let ancestor = jv(json!({"user": {"profile": {"name": "Alice"}}}));
        let source = jv(json!({"user": {"profile": {"name": "Bob"}}}));
        let target = jv(json!({"user": {"profile": {"name": "Carol"}}}));

        let result = merge_json_values(
            Some(&ancestor),
            &source,
            &target,
            MergeStrategy::LastWriterWins,
        );
        assert_eq!(result.conflict_paths, vec!["user.profile.name".to_string()]);
        assert_eq!(
            result.merged.unwrap().as_inner(),
            &json!({"user": {"profile": {"name": "Bob"}}})
        );
    }
}
