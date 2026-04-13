//! Transaction validation for OCC (Optimistic Concurrency Control)
//!
//! This module implements conflict detection per Section 3 of
//! `docs/architecture/M2_TRANSACTION_SEMANTICS.md`.
//!
//! # Snapshot Isolation Semantics
//!
//! Strata uses **Snapshot Isolation (SI)** with first-committer-wins:
//!
//! - Each transaction reads from a consistent point-in-time snapshot.
//! - At commit, only the **read-set** is validated: if any key read by
//!   the transaction was modified (version changed) since the snapshot,
//!   the transaction aborts.
//! - **Blind writes** (writes without a preceding read) never conflict.
//!
//! # Write Skew
//!
//! SI intentionally **allows** write skew. Write skew occurs when two
//! transactions each read disjoint keys but write to the other's read
//! key, and both commit:
//!
//! ```text
//! T1: read(A), write(B)
//! T2: read(B), write(A)
//! // Both pass validation → both commit → write skew
//! ```
//!
//! Mitigation strategies:
//! - Use `cas()` or `cas_with_read()` for atomic check-and-set
//! - Combine reads and writes on the same key
//! - Use application-level locking for invariants that span keys
//!
//! # Key rules from the spec
//!
//! - First-committer-wins based on READ-SET, not write-set
//! - Blind writes (write without read) do NOT conflict
//! - CAS is validated separately from read-set
//! - Write skew is ALLOWED (do not try to prevent it)

use crate::transaction::{CASOperation, TransactionContext};
use std::collections::HashMap;
use strata_core::id::CommitVersion;
use strata_core::traits::Storage;
use strata_core::types::Key;

/// Types of conflicts that can occur during transaction validation
///
/// See spec Section 3.1 for when each conflict type occurs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictType {
    /// Read-write conflict: key was read at one version but current version differs
    ///
    /// From spec Section 3.1 Condition 1:
    /// "T1 read key K and recorded version V in its read_set.
    ///  At commit time, the current storage version of K is V' where V' != V"
    ReadWriteConflict {
        /// The key that has a conflict
        key: Key,
        /// Version recorded in read_set when read
        read_version: CommitVersion,
        /// Current version in storage at validation time
        current_version: CommitVersion,
    },

    /// CAS conflict: expected version doesn't match current version
    ///
    /// From spec Section 3.1 Condition 3:
    /// "T1 called CAS(K, expected_version=V, new_value).
    ///  At commit time, current storage version of K != V"
    CASConflict {
        /// The key that has a CAS conflict
        key: Key,
        /// Expected version specified in CAS operation
        expected_version: CommitVersion,
        /// Current version in storage at validation time
        current_version: CommitVersion,
    },

    /// JSON document conflict: document version changed since read
    ///
    /// From M5 spec: Conflict occurs when a JSON document read during
    /// the transaction has been modified by another transaction.
    /// This is conservative (document-level) conflict detection.
    JsonDocConflict {
        /// The key of the JSON document with a conflict
        key: Key,
        /// Document version when read (snapshot version)
        snapshot_version: CommitVersion,
        /// Current document version at validation time
        current_version: CommitVersion,
    },

    /// JSON path write-write conflict: two writes to overlapping paths
    ///
    /// From M5 Epic 31: Region-based conflict detection.
    /// Conflict occurs when two writes within the same transaction target
    /// overlapping paths. This is a semantic error.
    JsonPathWriteWriteConflict {
        /// The key of the JSON document
        key: Key,
        /// The first write path
        path1: strata_core::primitives::json::JsonPath,
        /// The second write path (overlaps with path1)
        path2: strata_core::primitives::json::JsonPath,
    },
}

/// Result of transaction validation
///
/// Accumulates all conflicts found during validation.
/// A transaction commits only if is_valid() returns true.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// All conflicts detected during validation
    pub conflicts: Vec<ConflictType>,
}

impl ValidationResult {
    /// Create a successful validation result (no conflicts)
    pub fn ok() -> Self {
        ValidationResult {
            conflicts: Vec::new(),
        }
    }

    /// Create a validation result with a single conflict
    pub fn conflict(conflict: ConflictType) -> Self {
        ValidationResult {
            conflicts: vec![conflict],
        }
    }

    /// Check if validation passed (no conflicts)
    pub fn is_valid(&self) -> bool {
        self.conflicts.is_empty()
    }

    /// Merge another validation result into this one
    ///
    /// Used to combine results from different validation phases.
    pub fn merge(&mut self, other: ValidationResult) {
        self.conflicts.extend(other.conflicts);
    }

    /// Get the number of conflicts
    pub fn conflict_count(&self) -> usize {
        self.conflicts.len()
    }
}

/// Validate the read-set against current storage state
///
/// Per spec Section 3.1 Condition 1:
/// - For each key in read_set, check if current version matches read version
/// - If any version changed, report ReadWriteConflict
///
/// # Arguments
/// * `read_set` - Keys read with their versions at read time
/// * `store` - Storage to check current versions against
///
/// # Returns
/// ValidationResult with any ReadWriteConflicts found
pub fn validate_read_set<S: Storage>(
    read_set: &HashMap<Key, CommitVersion>,
    store: &S,
) -> strata_core::StrataResult<ValidationResult> {
    let mut result = ValidationResult::ok();

    for (key, read_version) in read_set {
        // Get current version from storage (as u64 for comparison)
        let current_version = match store.get_version_only(key) {
            Ok(Some(v)) => v,
            Ok(None) => CommitVersion::ZERO, // Key doesn't exist = version 0
            Err(e) => {
                // Storage error - abort validation to prevent incorrect commit
                return Err(strata_core::StrataError::internal(format!(
                    "Storage error during read-set validation for key {:?}: {}",
                    key, e
                )));
            }
        };

        // Check if version changed
        if current_version != *read_version {
            result.conflicts.push(ConflictType::ReadWriteConflict {
                key: key.clone(),
                read_version: *read_version,
                current_version,
            });
        }
    }

    Ok(result)
}

/// Validate CAS operations against current storage state
///
/// Per spec Section 3.1 Condition 3:
/// - For each CAS op, check if current version matches expected_version
/// - If versions don't match, report CASConflict
///
/// Per spec Section 3.4:
/// - CAS does NOT add to read_set (validated separately)
/// - expected_version=0 means "key must not exist"
///
/// # Arguments
/// * `cas_set` - CAS operations to validate
/// * `store` - Storage to check current versions against
///
/// # Returns
/// ValidationResult with any CASConflicts found
pub fn validate_cas_set<S: Storage>(
    cas_set: &[CASOperation],
    store: &S,
) -> strata_core::StrataResult<ValidationResult> {
    let mut result = ValidationResult::ok();

    for cas_op in cas_set {
        // Get current version from storage (as u64 for comparison)
        let current_version = match store.get_version_only(&cas_op.key) {
            Ok(Some(v)) => v,
            Ok(None) => CommitVersion::ZERO, // Key doesn't exist = version 0
            Err(e) => {
                return Err(strata_core::StrataError::internal(format!(
                    "Storage error during CAS validation for key {:?}: {}",
                    cas_op.key, e
                )));
            }
        };

        // Check if expected version matches
        if current_version != cas_op.expected_version {
            result.conflicts.push(ConflictType::CASConflict {
                key: cas_op.key.clone(),
                expected_version: cas_op.expected_version,
                current_version,
            });
        }
    }

    Ok(result)
}

/// Validate JSON document versions against current storage state
///
/// Per M5 spec: JSON conflict detection is document-level (conservative).
/// If any JSON document read during the transaction has been modified,
/// the transaction must abort.
///
/// # Document-Level vs Path-Level Detection
///
/// This uses **document-level** (conservative) detection: if a JSON
/// document's version changed at all, the transaction aborts — even if
/// the modified paths don't overlap with the paths read by the
/// transaction. This is simpler and cheaper than path-level tracking,
/// but may cause false positives when concurrent transactions modify
/// disjoint paths within the same document.
///
/// Path-level detection would reduce false positives but requires
/// tracking every path read and comparing against every path written,
/// adding O(paths_read × paths_written) overhead per document.
///
/// # Arguments
/// * `json_snapshot_versions` - Document keys and their versions at read time
/// * `store` - Storage to check current versions against
///
/// # Returns
/// ValidationResult with any JsonDocConflicts found
pub fn validate_json_set<S: Storage>(
    json_snapshot_versions: Option<&HashMap<Key, CommitVersion>>,
    store: &S,
) -> strata_core::StrataResult<ValidationResult> {
    let mut result = ValidationResult::ok();

    let Some(versions) = json_snapshot_versions else {
        return Ok(result); // No JSON operations = no JSON conflicts
    };

    for (key, snapshot_version) in versions {
        // Get current version from storage
        let current_version = match store.get_version_only(key) {
            Ok(Some(v)) => v,
            Ok(None) => CommitVersion::ZERO, // Document deleted = version 0
            Err(e) => {
                return Err(strata_core::StrataError::internal(format!(
                    "Storage error during JSON validation for key {:?}: {}",
                    key, e
                )));
            }
        };

        // Check if version changed since transaction read it
        if current_version != *snapshot_version {
            result.conflicts.push(ConflictType::JsonDocConflict {
                key: key.clone(),
                snapshot_version: *snapshot_version,
                current_version,
            });
        }
    }

    Ok(result)
}

/// Validate JSON path-level conflicts (M5 Epic 31)
///
/// This provides region-based conflict detection for JSON operations.
/// It checks for:
/// - Write-write conflicts: Two writes to overlapping paths within the transaction
///
/// Note: Read-write path conflicts are intentionally NOT checked here because
/// reading a path and then writing to an overlapping path is valid behavior
/// (read-your-writes semantics). The version-based conflict detection in
/// `validate_json_set` already handles the case where concurrent transactions
/// modify the same document.
///
/// # Arguments
/// * `json_writes` - JSON patches to be applied
///
/// # Returns
/// ValidationResult with any path conflicts found
pub fn validate_json_paths(json_writes: &[crate::transaction::JsonPatchEntry]) -> ValidationResult {
    use crate::conflict::check_write_write_conflicts;

    let mut result = ValidationResult::ok();

    // Check for write-write conflicts (overlapping write paths)
    // This is a semantic error - the order of writes matters and the result is undefined
    result
        .conflicts
        .extend(check_write_write_conflicts(json_writes));

    result
}

/// Validate a complete transaction against current storage state
///
/// Per spec Section 3 (Conflict Detection):
/// 1. Validates read-set: detects read-write conflicts (first-committer-wins)
/// 2. Validates CAS-set: ensures expected versions still match
/// 3. Validates JSON-set: ensures JSON document versions haven't changed
/// 4. Validates JSON paths: ensures no overlapping writes within transaction (M5 Epic 31)
///
/// Note: Write-set validation is intentionally omitted. Per spec Section 3.2,
/// blind writes do not conflict — first-committer-wins is based on the read-set.
///
/// **Per spec Section 3.2 Scenario 3**: Read-only transactions ALWAYS succeed.
/// If a transaction has no writes (empty write_set, delete_set, cas_set, and json_writes),
/// validation is skipped entirely and the transaction succeeds.
///
/// # Arguments
/// * `txn` - Transaction to validate (should be in Validating state for correctness,
///   but this function doesn't enforce that)
/// * `store` - Current storage state to validate against
///
/// # Returns
/// ValidationResult containing any conflicts found
///
/// # Spec Reference
/// - Section 3.1: When conflicts occur
/// - Section 3.2: Conflict scenarios (including read-only transaction rule)
/// - Section 3.3: First-committer-wins rule
/// - JSON document-level conflict detection
pub fn validate_transaction<S: Storage>(
    txn: &TransactionContext,
    store: &S,
) -> strata_core::StrataResult<ValidationResult> {
    // Per spec Section 3.2 Scenario 3: Read-only transactions ALWAYS commit.
    // "Read-Only Transaction: T1 only reads keys, never writes any → ALWAYS COMMITS"
    // "Why: Read-only transactions have no writes to validate. They simply return their snapshot view."
    // Note: We also need to check for JSON writes
    if txn.is_read_only() && txn.json_writes().is_empty() {
        return Ok(ValidationResult::ok());
    }

    let mut result = ValidationResult::ok();

    // 1. Validate read-set (detects read-write conflicts)
    result.merge(validate_read_set(&txn.read_set, store)?);

    // 2. Validate CAS-set (detects version mismatches)
    result.merge(validate_cas_set(&txn.cas_set, store)?);

    // 3. Validate JSON-set (detects JSON document version changes)
    result.merge(validate_json_set(txn.json_snapshot_versions(), store)?);

    // 4. Validate JSON paths (detects overlapping writes within transaction)
    result.merge(validate_json_paths(txn.json_writes()));

    Ok(result)
}
