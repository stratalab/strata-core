//! JSON conflict detection for region-based concurrency control.
//!
//! This module provides write-write conflict detection for JSON operations
//! within transactions. Two JSON writes conflict if their paths overlap
//! (one is ancestor, descendant, or equal to the other).

use crate::transaction::JsonPatchEntry;
use crate::validation::ConflictType;

/// Check for write-write conflicts in a transaction
///
/// A write-write conflict occurs when:
/// - Two writes target the same document AND
/// - The write paths overlap
///
/// # Arguments
///
/// * `writes` - List of JSON patches to be applied
///
/// # Returns
///
/// A vector of all detected write-write conflicts as `ConflictType`
pub fn check_write_write_conflicts(writes: &[JsonPatchEntry]) -> Vec<ConflictType> {
    let mut conflicts = Vec::new();

    for (i, w1) in writes.iter().enumerate() {
        for w2 in writes.iter().skip(i + 1) {
            // Same document?
            if w1.key != w2.key {
                continue;
            }

            // Paths overlap?
            if w1.patch.path().overlaps(w2.patch.path()) {
                conflicts.push(ConflictType::JsonPathWriteWriteConflict {
                    key: w1.key.clone(),
                    path1: w1.patch.path().clone(),
                    path2: w2.patch.path().clone(),
                });
            }
        }
    }

    conflicts
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::primitives::json::JsonPatch;
    use strata_core::types::{BranchId, Key, Namespace};

    fn test_key(doc: &str) -> Key {
        Key::new_json(
            Arc::new(Namespace::for_branch(BranchId::new())),
            doc,
        )
    }

    #[test]
    fn test_empty_writes_no_conflicts() {
        let conflicts = check_write_write_conflicts(&[]);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_single_write_no_conflicts() {
        let key = test_key("doc");
        let writes = vec![JsonPatchEntry::new(
            key,
            JsonPatch::set_at("foo".parse().unwrap(), serde_json::json!(1).into()),
            1,
        )];

        let conflicts = check_write_write_conflicts(&writes);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_ancestor_descendant_path_conflict() {
        let key = test_key("doc");

        let writes = vec![
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("foo".parse().unwrap(), serde_json::json!(1).into()),
                2,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("foo.bar".parse().unwrap(), serde_json::json!(2).into()),
                3,
            ),
        ];

        let conflicts = check_write_write_conflicts(&writes);
        assert_eq!(conflicts.len(), 1);

        // Verify actual field values, not just pattern match
        match &conflicts[0] {
            ConflictType::JsonPathWriteWriteConflict { key: k, path1, path2 } => {
                assert_eq!(*k, key);
                assert_eq!(path1.to_string(), "foo");
                assert_eq!(path2.to_string(), "foo.bar");
            }
            other => panic!("Expected JsonPathWriteWriteConflict, got {:?}", other),
        }
    }

    #[test]
    fn test_exact_same_path_conflicts() {
        let key = test_key("doc");

        let writes = vec![
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("foo.bar".parse().unwrap(), serde_json::json!(1).into()),
                2,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("foo.bar".parse().unwrap(), serde_json::json!(2).into()),
                3,
            ),
        ];

        let conflicts = check_write_write_conflicts(&writes);
        assert_eq!(conflicts.len(), 1);
    }

    #[test]
    fn test_disjoint_paths_no_conflict() {
        let key = test_key("doc");

        let writes = vec![
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("foo".parse().unwrap(), serde_json::json!(1).into()),
                2,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("bar".parse().unwrap(), serde_json::json!(2).into()),
                3,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("baz".parse().unwrap(), serde_json::json!(3).into()),
                4,
            ),
        ];

        let conflicts = check_write_write_conflicts(&writes);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_different_documents_same_path_no_conflict() {
        // Two different document keys (different namespaces via BranchId::new())
        let key1 = test_key("doc-a");
        let key2 = test_key("doc-b");

        let writes = vec![
            JsonPatchEntry::new(
                key1,
                JsonPatch::set_at("foo".parse().unwrap(), serde_json::json!(1).into()),
                2,
            ),
            JsonPatchEntry::new(
                key2,
                JsonPatch::set_at("foo".parse().unwrap(), serde_json::json!(2).into()),
                3,
            ),
        ];

        let conflicts = check_write_write_conflicts(&writes);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_multiple_conflicts_from_three_overlapping_writes() {
        // 3 writes to overlapping paths on same doc → 3 conflict pairs (0-1, 0-2, 1-2)
        let key = test_key("doc");

        let writes = vec![
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("x".parse().unwrap(), serde_json::json!(1).into()),
                1,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("x.a".parse().unwrap(), serde_json::json!(2).into()),
                2,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("x.a.b".parse().unwrap(), serde_json::json!(3).into()),
                3,
            ),
        ];

        let conflicts = check_write_write_conflicts(&writes);
        // x overlaps x.a (ancestor), x overlaps x.a.b (ancestor), x.a overlaps x.a.b (ancestor)
        assert_eq!(conflicts.len(), 3);

        // All should be JsonPathWriteWriteConflict with correct key
        for c in &conflicts {
            match c {
                ConflictType::JsonPathWriteWriteConflict { key: k, .. } => {
                    assert_eq!(*k, key);
                }
                other => panic!("Expected JsonPathWriteWriteConflict, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_mixed_conflicting_and_disjoint_paths() {
        let key = test_key("doc");

        let writes = vec![
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("a".parse().unwrap(), serde_json::json!(1).into()),
                1,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("b".parse().unwrap(), serde_json::json!(2).into()),
                2,
            ),
            JsonPatchEntry::new(
                key.clone(),
                JsonPatch::set_at("a.child".parse().unwrap(), serde_json::json!(3).into()),
                3,
            ),
        ];

        let conflicts = check_write_write_conflicts(&writes);
        // Only a↔a.child conflict; b is disjoint from both
        assert_eq!(conflicts.len(), 1);
        match &conflicts[0] {
            ConflictType::JsonPathWriteWriteConflict { path1, path2, .. } => {
                assert_eq!(path1.to_string(), "a");
                assert_eq!(path2.to_string(), "a.child");
            }
            other => panic!("Expected JsonPathWriteWriteConflict, got {:?}", other),
        }
    }
}
