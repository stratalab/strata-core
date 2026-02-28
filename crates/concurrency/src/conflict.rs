//! JSON conflict detection for region-based concurrency control.
//!
//! This module provides conflict detection for JSON operations within transactions.
//! Two JSON operations conflict if their paths overlap (one is ancestor, descendant, or equal).
//!
//! # Conflict Types
//!
//! - **Read-Write Conflict**: A read at path X conflicts with a write at path Y if X.overlaps(Y)
//! - **Write-Write Conflict**: Two writes at paths X and Y conflict if X.overlaps(Y)
//! - **Version Mismatch**: The document version changed since transaction start

use strata_core::primitives::json::JsonPath;
use strata_core::types::Key;

use crate::transaction::JsonPatchEntry;

/// Result of conflict detection
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictResult {
    /// No conflict detected
    NoConflict,
    /// Read-write conflict detected
    ReadWriteConflict {
        /// The document key where the conflict occurred
        key: Key,
        /// The path that was read
        read_path: JsonPath,
        /// The path that was written (conflicts with read_path)
        write_path: JsonPath,
    },
    /// Write-write conflict detected
    WriteWriteConflict {
        /// The document key where the conflict occurred
        key: Key,
        /// The first conflicting write path
        path1: JsonPath,
        /// The second conflicting write path
        path2: JsonPath,
    },
    /// Version mismatch (stale read)
    VersionMismatch {
        /// The document key where the version mismatch occurred
        key: Key,
        /// The version expected (from snapshot)
        expected: u64,
        /// The version found (current)
        found: u64,
    },
}

/// Error type for JSON conflicts
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum JsonConflictError {
    /// A read-write conflict was detected
    #[error("read-write conflict on {key:?}: read at {read_path}, write at {write_path}")]
    ReadWriteConflict {
        /// The document key where the conflict occurred
        key: Key,
        /// The path that was read
        read_path: JsonPath,
        /// The path that was written (conflicts with read_path)
        write_path: JsonPath,
    },
    /// A write-write conflict was detected
    #[error("write-write conflict on {key:?}: writes at {path1} and {path2}")]
    WriteWriteConflict {
        /// The document key where the conflict occurred
        key: Key,
        /// The first conflicting write path
        path1: JsonPath,
        /// The second conflicting write path
        path2: JsonPath,
    },
    /// A version mismatch was detected (stale read)
    #[error("version mismatch on {key:?}: expected {expected}, found {found}")]
    VersionMismatch {
        /// The document key where the version mismatch occurred
        key: Key,
        /// The version expected (from snapshot)
        expected: u64,
        /// The version found (current)
        found: u64,
    },
}

impl From<ConflictResult> for Option<JsonConflictError> {
    fn from(result: ConflictResult) -> Self {
        match result {
            ConflictResult::NoConflict => None,
            ConflictResult::ReadWriteConflict {
                key,
                read_path,
                write_path,
            } => Some(JsonConflictError::ReadWriteConflict {
                key,
                read_path,
                write_path,
            }),
            ConflictResult::WriteWriteConflict { key, path1, path2 } => {
                Some(JsonConflictError::WriteWriteConflict { key, path1, path2 })
            }
            ConflictResult::VersionMismatch {
                key,
                expected,
                found,
            } => Some(JsonConflictError::VersionMismatch {
                key,
                expected,
                found,
            }),
        }
    }
}

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
/// A vector of all detected write-write conflicts
pub fn check_write_write_conflicts(writes: &[JsonPatchEntry]) -> Vec<ConflictResult> {
    let mut conflicts = Vec::new();

    for (i, w1) in writes.iter().enumerate() {
        for w2 in writes.iter().skip(i + 1) {
            // Same document?
            if w1.key != w2.key {
                continue;
            }

            // Paths overlap?
            if w1.patch.path().overlaps(w2.patch.path()) {
                conflicts.push(ConflictResult::WriteWriteConflict {
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
    use strata_core::types::{BranchId, Namespace};

    fn test_key() -> Key {
        Key::new_json(Arc::new(Namespace::for_branch(BranchId::new())), "test-doc")
    }

    #[test]
    fn test_write_write_conflict_detection() {
        let key = test_key();

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
        assert!(matches!(
            conflicts[0],
            ConflictResult::WriteWriteConflict { .. }
        ));
    }

    #[test]
    fn test_no_write_conflict_disjoint_paths() {
        let key = test_key();

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
    fn test_write_write_different_documents() {
        let key1 = test_key();
        let key2 = test_key();

        let writes = vec![
            JsonPatchEntry::new(
                key1.clone(),
                JsonPatch::set_at("foo".parse().unwrap(), serde_json::json!(1).into()),
                2,
            ),
            JsonPatchEntry::new(
                key2.clone(),
                JsonPatch::set_at("foo".parse().unwrap(), serde_json::json!(2).into()),
                3,
            ),
        ];

        // Same path but different documents - no conflict
        let conflicts = check_write_write_conflicts(&writes);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_json_conflict_error_display() {
        let key = test_key();

        let err = JsonConflictError::ReadWriteConflict {
            key: key.clone(),
            read_path: "foo".parse().unwrap(),
            write_path: "foo.bar".parse().unwrap(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("read-write conflict"));
        assert!(msg.contains("foo"));

        let err = JsonConflictError::WriteWriteConflict {
            key: key.clone(),
            path1: "foo".parse().unwrap(),
            path2: "foo.bar".parse().unwrap(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("write-write conflict"));

        let err = JsonConflictError::VersionMismatch {
            key,
            expected: 5,
            found: 7,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("version mismatch"));
        assert!(msg.contains("5"));
        assert!(msg.contains("7"));
    }
}
