//! Branch operations: fork, diff, and merge
//!
//! Engine-level module providing branch operations that work directly with
//! the database and storage layers, following the pattern established by `bundle.rs`.
//!
//! ## Operations
//!
//! - `fork_branch` — Create a copy of a branch with all its data
//! - `diff_branches` — Compare two branches and return structured differences
//! - `merge_branches` — Merge data from one branch into another

use crate::database::Database;
use crate::primitives::branch::resolve_branch_name;
use crate::BranchIndex;
use crate::SpaceIndex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::value::Value;
use strata_core::PrimitiveType;
use strata_core::StrataError;
use strata_core::StrataResult;
use tracing::info;

// =============================================================================
// Data TypeTags to scan (all user data types)
// =============================================================================

const DATA_TYPE_TAGS: [TypeTag; 6] = [
    TypeTag::KV,
    TypeTag::Event,
    TypeTag::State,
    TypeTag::Json,
    TypeTag::Vector,
    TypeTag::VectorConfig,
];

// =============================================================================
// Public result types
// =============================================================================

/// Information returned after forking a branch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkInfo {
    /// Source branch name
    pub source: String,
    /// Destination branch name
    pub destination: String,
    /// Number of keys copied
    pub keys_copied: u64,
    /// Number of spaces copied
    pub spaces_copied: u64,
}

/// A single entry in a branch diff.
///
/// Named `BranchDiffEntry` to distinguish from [`crate::recovery::replay::DiffEntry`]
/// which is used for recovery replay diffs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchDiffEntry {
    /// User key (UTF-8 or hex-encoded for binary keys)
    pub key: String,
    /// Raw user key bytes (for programmatic access, preserves binary keys)
    pub raw_key: Vec<u8>,
    /// Primitive type of this entry
    pub primitive: PrimitiveType,
    /// Space this entry belongs to
    pub space: String,
    /// Debug-formatted value in branch A (None if not present)
    pub value_a: Option<String>,
    /// Debug-formatted value in branch B (None if not present)
    pub value_b: Option<String>,
}

/// Per-space diff between two branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpaceDiff {
    /// Space name
    pub space: String,
    /// Entries in B but not A
    pub added: Vec<BranchDiffEntry>,
    /// Entries in A but not B
    pub removed: Vec<BranchDiffEntry>,
    /// Entries in both with different values
    pub modified: Vec<BranchDiffEntry>,
}

/// Summary statistics for a branch diff.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiffSummary {
    /// Total entries added (in B but not A)
    pub total_added: usize,
    /// Total entries removed (in A but not B)
    pub total_removed: usize,
    /// Total entries modified (in both, different values)
    pub total_modified: usize,
    /// Spaces that exist only in branch A
    pub spaces_only_in_a: Vec<String>,
    /// Spaces that exist only in branch B
    pub spaces_only_in_b: Vec<String>,
}

/// Complete result of comparing two branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchDiffResult {
    /// Name of branch A
    pub branch_a: String,
    /// Name of branch B
    pub branch_b: String,
    /// Per-space diffs
    pub spaces: Vec<SpaceDiff>,
    /// Aggregate summary
    pub summary: DiffSummary,
}

/// Strategy for resolving conflicts during merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeStrategy {
    /// Source values overwrite target values on conflict
    LastWriterWins,
    /// Merge fails if any conflicts exist
    Strict,
}

/// A conflict detected during merge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConflictEntry {
    /// User key
    pub key: String,
    /// Primitive type
    pub primitive: PrimitiveType,
    /// Space
    pub space: String,
    /// Value in the source branch
    pub source_value: String,
    /// Value in the target branch
    pub target_value: String,
}

/// Information returned after merging branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeInfo {
    /// Source branch name
    pub source: String,
    /// Target branch name
    pub target: String,
    /// Number of keys written to target
    pub keys_applied: u64,
    /// Conflicts encountered (empty for LWW, populated for Strict failures)
    pub conflicts: Vec<ConflictEntry>,
    /// Number of spaces merged
    pub spaces_merged: u64,
}

// =============================================================================
// Helpers
// =============================================================================

/// Map a TypeTag to the corresponding PrimitiveType.
fn type_tag_to_primitive(tag: TypeTag) -> PrimitiveType {
    match tag {
        TypeTag::KV => PrimitiveType::Kv,
        TypeTag::Event => PrimitiveType::Event,
        TypeTag::State => PrimitiveType::State,
        TypeTag::Json => PrimitiveType::Json,
        TypeTag::Vector | TypeTag::VectorConfig => PrimitiveType::Vector,
        _ => PrimitiveType::Kv, // fallback for Branch/Space/Trace metadata tags
    }
}

/// Format a user key as a readable string (UTF-8 or hex for binary).
fn format_user_key(user_key: &[u8]) -> String {
    match std::str::from_utf8(user_key) {
        Ok(s) => s.to_string(),
        Err(_) => {
            // Manual hex encoding to avoid adding hex crate dependency
            user_key
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        }
    }
}

/// Format a value for display in diffs.
fn format_value(value: &Value) -> String {
    format!("{:?}", value)
}

/// Verify a branch exists and return its resolved BranchId.
fn resolve_and_verify(db: &Arc<Database>, name: &str) -> StrataResult<BranchId> {
    let branch_index = BranchIndex::new(db.clone());
    branch_index
        .get_branch(name)?
        .ok_or_else(|| StrataError::invalid_input(format!("Branch '{}' not found", name)))?;
    Ok(resolve_branch_name(name))
}

// =============================================================================
// Fork
// =============================================================================

/// Fork a branch, creating a complete copy of all its data.
///
/// Creates a new branch with `destination` name and copies all data
/// (KV, Event, State, JSON, Vector, VectorConfig) from `source` to it,
/// preserving space organization.
///
/// # Errors
///
/// - Source branch does not exist
/// - Destination branch already exists
pub fn fork_branch(db: &Arc<Database>, source: &str, destination: &str) -> StrataResult<ForkInfo> {
    let branch_index = BranchIndex::new(db.clone());
    let space_index = SpaceIndex::new(db.clone());

    // 1. Verify source exists
    branch_index.get_branch(source)?.ok_or_else(|| {
        StrataError::invalid_input(format!("Source branch '{}' not found", source))
    })?;

    // 2. Verify destination doesn't exist
    if branch_index.exists(destination)? {
        return Err(StrataError::invalid_input(format!(
            "Destination branch '{}' already exists",
            destination
        )));
    }

    // 3. Create destination branch
    branch_index.create_branch(destination)?;

    // 4. Resolve BranchIds
    let source_id = resolve_branch_name(source);
    let dest_id = resolve_branch_name(destination);

    // 5. List source spaces and register them in destination
    let source_spaces = space_index.list(source_id)?;
    let mut spaces_copied = 0u64;

    for space in &source_spaces {
        if space != "default" {
            space_index.register(dest_id, space)?;
        }
        spaces_copied += 1;
    }

    // 6. Scan all source data and copy to destination
    let storage = db.storage();
    let mut keys_copied = 0u64;

    for type_tag in DATA_TYPE_TAGS {
        let entries = storage.list_by_type(&source_id, type_tag);

        if entries.is_empty() {
            continue;
        }

        // Batch write all entries for this type tag
        let batch: Vec<(Key, Value)> = entries
            .into_iter()
            .map(|(key, vv)| {
                // Rewrite key with destination namespace (preserving space and user_key)
                let new_ns = Namespace::for_branch_space(dest_id, &key.namespace.space);
                let new_key = Key::new(new_ns, key.type_tag, key.user_key.clone());
                (new_key, vv.value)
            })
            .collect();

        let batch_len = batch.len() as u64;

        db.transaction(dest_id, |txn| {
            for (key, value) in &batch {
                txn.put(key.clone(), value.clone())?;
            }
            Ok(())
        })?;

        keys_copied += batch_len;
    }

    // Reload vector backends for the destination branch so that forked
    // vectors are immediately searchable without requiring a database restart.
    {
        use crate::primitives::vector::store::VectorStore;
        let vector_store = VectorStore::new(db.clone());
        if let Err(e) = vector_store.post_merge_reload_vectors_from(dest_id, Some(source_id)) {
            tracing::warn!(
                target: "strata::branch_ops",
                error = %e,
                "Failed to reload vector backends after fork"
            );
        }
    }

    info!(
        target: "strata::branch_ops",
        source,
        destination,
        keys_copied,
        spaces_copied,
        "Branch forked"
    );

    Ok(ForkInfo {
        source: source.to_string(),
        destination: destination.to_string(),
        keys_copied,
        spaces_copied,
    })
}

// =============================================================================
// Diff
// =============================================================================

/// Compare two branches and return a structured diff.
///
/// Scans all data in both branches across all spaces and data types,
/// producing per-space diffs showing added, removed, and modified entries.
///
/// # Errors
///
/// - Either branch does not exist
pub fn diff_branches(
    db: &Arc<Database>,
    branch_a: &str,
    branch_b: &str,
) -> StrataResult<BranchDiffResult> {
    let space_index = SpaceIndex::new(db.clone());

    // 1. Verify both branches exist and resolve IDs
    let id_a = resolve_and_verify(db, branch_a)?;
    let id_b = resolve_and_verify(db, branch_b)?;

    // 2. List spaces in both branches
    let spaces_a: HashSet<String> = space_index.list(id_a)?.into_iter().collect();
    let spaces_b: HashSet<String> = space_index.list(id_b)?.into_iter().collect();

    let spaces_only_in_a: Vec<String> = spaces_a.difference(&spaces_b).cloned().collect();
    let spaces_only_in_b: Vec<String> = spaces_b.difference(&spaces_a).cloned().collect();
    let all_spaces: HashSet<String> = spaces_a.union(&spaces_b).cloned().collect();

    let storage = db.storage();
    let mut space_diffs = Vec::new();
    let mut total_added = 0usize;
    let mut total_removed = 0usize;
    let mut total_modified = 0usize;

    // 3. Scan all data once per type tag, grouped by space
    let mut maps_a: HashMap<String, HashMap<(Vec<u8>, TypeTag), Value>> = HashMap::new();
    let mut maps_b: HashMap<String, HashMap<(Vec<u8>, TypeTag), Value>> = HashMap::new();

    for type_tag in DATA_TYPE_TAGS {
        for (key, vv) in storage.list_by_type(&id_a, type_tag) {
            maps_a
                .entry(key.namespace.space.clone())
                .or_default()
                .insert((key.user_key.clone(), type_tag), vv.value);
        }
        for (key, vv) in storage.list_by_type(&id_b, type_tag) {
            maps_b
                .entry(key.namespace.space.clone())
                .or_default()
                .insert((key.user_key.clone(), type_tag), vv.value);
        }
    }

    // 4. For each space, compare data
    for space in &all_spaces {
        let map_a = maps_a.remove(space).unwrap_or_default();
        let map_b = maps_b.remove(space).unwrap_or_default();

        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut modified = Vec::new();

        // Keys in A (check for removed and modified)
        for ((user_key, tag), val_a) in &map_a {
            let key_str = format_user_key(user_key);
            let primitive = type_tag_to_primitive(*tag);

            match map_b.get(&(user_key.clone(), *tag)) {
                None => {
                    removed.push(BranchDiffEntry {
                        key: key_str,
                        raw_key: user_key.clone(),
                        primitive,
                        space: space.clone(),
                        value_a: Some(format_value(val_a)),
                        value_b: None,
                    });
                }
                Some(val_b) => {
                    if val_a != val_b {
                        modified.push(BranchDiffEntry {
                            key: key_str,
                            raw_key: user_key.clone(),
                            primitive,
                            space: space.clone(),
                            value_a: Some(format_value(val_a)),
                            value_b: Some(format_value(val_b)),
                        });
                    }
                }
            }
        }

        // Keys only in B (added)
        for ((user_key, tag), val_b) in &map_b {
            if !map_a.contains_key(&(user_key.clone(), *tag)) {
                added.push(BranchDiffEntry {
                    key: format_user_key(user_key),
                    raw_key: user_key.clone(),
                    primitive: type_tag_to_primitive(*tag),
                    space: space.clone(),
                    value_a: None,
                    value_b: Some(format_value(val_b)),
                });
            }
        }

        total_added += added.len();
        total_removed += removed.len();
        total_modified += modified.len();

        if !added.is_empty() || !removed.is_empty() || !modified.is_empty() {
            space_diffs.push(SpaceDiff {
                space: space.clone(),
                added,
                removed,
                modified,
            });
        }
    }

    Ok(BranchDiffResult {
        branch_a: branch_a.to_string(),
        branch_b: branch_b.to_string(),
        spaces: space_diffs,
        summary: DiffSummary {
            total_added,
            total_removed,
            total_modified,
            spaces_only_in_a,
            spaces_only_in_b,
        },
    })
}

// =============================================================================
// Merge
// =============================================================================

/// Merge data from source branch into target branch.
///
/// Uses `diff_branches(target, source)` to identify changes, then applies
/// them to the target. Target is branch A (base), source is branch B (incoming).
///
/// - **Added entries** (in source but not target): written to target
/// - **Modified entries** (in both, different values):
///   - `LastWriterWins`: source value overwrites target (appends new version)
///   - `Strict`: merge fails with conflict list (no writes)
/// - **Removed entries** (in target but not source): left unchanged
///
/// Version history in the target is preserved — merged values are appended
/// as new versions via `db.transaction()`.
///
/// # Errors
///
/// - Either branch does not exist
/// - `Strict` strategy with conflicts
pub fn merge_branches(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    strategy: MergeStrategy,
) -> StrataResult<MergeInfo> {
    let space_index = SpaceIndex::new(db.clone());

    // 1. Diff: target is A (base), source is B (incoming)
    let diff = diff_branches(db, target, source)?;

    // 2. Check for conflicts in Strict mode
    if strategy == MergeStrategy::Strict && diff.summary.total_modified > 0 {
        let conflicts: Vec<ConflictEntry> = diff
            .spaces
            .iter()
            .flat_map(|sd| {
                sd.modified.iter().map(|entry| ConflictEntry {
                    key: entry.key.clone(),
                    primitive: entry.primitive,
                    space: entry.space.clone(),
                    source_value: entry.value_b.clone().unwrap_or_default(),
                    target_value: entry.value_a.clone().unwrap_or_default(),
                })
            })
            .collect();

        return Err(StrataError::invalid_input(format!(
            "Merge conflict: {} keys differ between '{}' and '{}'. Use LastWriterWins strategy or resolve conflicts manually.",
            conflicts.len(),
            source,
            target
        )));
    }

    // Collect conflicts for reporting (LWW resolves them, Strict already returned error)
    let conflicts: Vec<ConflictEntry> = if strategy == MergeStrategy::LastWriterWins {
        diff.spaces
            .iter()
            .flat_map(|sd| {
                sd.modified.iter().map(|entry| ConflictEntry {
                    key: entry.key.clone(),
                    primitive: entry.primitive,
                    space: entry.space.clone(),
                    source_value: entry.value_b.clone().unwrap_or_default(),
                    target_value: entry.value_a.clone().unwrap_or_default(),
                })
            })
            .collect()
    } else {
        vec![]
    };

    // 3. Resolve IDs
    let source_id = resolve_branch_name(source);
    let target_id = resolve_branch_name(target);
    let storage = db.storage();

    let mut keys_applied = 0u64;
    let mut spaces_merged = 0u64;

    // 4. Apply changes
    for space_diff in &diff.spaces {
        let space = &space_diff.space;

        // Ensure target has this space
        if space != "default" {
            space_index.register(target_id, space)?;
        }
        spaces_merged += 1;

        // Collect entries to write from source: added + modified (for LWW)
        let entries_to_apply: Vec<&BranchDiffEntry> = space_diff
            .added
            .iter()
            .chain(if strategy == MergeStrategy::LastWriterWins {
                space_diff.modified.iter()
            } else {
                [].iter()
            })
            .collect();

        if entries_to_apply.is_empty() {
            continue;
        }

        // Re-scan source data for this space to get actual values
        // (diff only stores string representations)
        let mut source_values: HashMap<(Vec<u8>, TypeTag), Value> = HashMap::new();
        for type_tag in DATA_TYPE_TAGS {
            let entries = storage.list_by_type(&source_id, type_tag);
            for (key, vv) in entries {
                if key.namespace.space == *space {
                    source_values.insert((key.user_key.clone(), type_tag), vv.value);
                }
            }
        }

        // Write to target
        let mut batch: Vec<(Key, Value)> = Vec::new();
        for diff_entry in &entries_to_apply {
            // Find the matching source value
            for type_tag in DATA_TYPE_TAGS {
                if type_tag_to_primitive(type_tag) == diff_entry.primitive {
                    let user_key_bytes = diff_entry.raw_key.clone();
                    if let Some(value) = source_values.get(&(user_key_bytes.clone(), type_tag)) {
                        let target_ns = Namespace::for_branch_space(target_id, space);
                        let target_key = Key::new(target_ns, type_tag, user_key_bytes);
                        batch.push((target_key, value.clone()));
                        break;
                    }
                }
            }
        }

        let batch_len = batch.len() as u64;
        if batch_len > 0 {
            db.transaction(target_id, |txn| {
                for (key, value) in &batch {
                    txn.put(key.clone(), value.clone())?;
                }
                Ok(())
            })?;
            keys_applied += batch_len;
        }
    }

    // Reload vector backends for the target branch so that vectors merged
    // at the KV level become visible to in-memory search immediately.
    {
        use crate::primitives::vector::store::VectorStore;
        let vector_store = VectorStore::new(db.clone());
        if let Err(e) = vector_store.post_merge_reload_vectors_from(target_id, Some(source_id)) {
            tracing::warn!(
                target: "strata::branch_ops",
                error = %e,
                "Failed to reload vector backends after merge"
            );
        }
    }

    info!(
        target: "strata::branch_ops",
        source,
        target,
        keys_applied,
        spaces_merged,
        strategy = ?strategy,
        "Branches merged"
    );

    Ok(MergeInfo {
        source: source.to_string(),
        target: target.to_string(),
        keys_applied,
        conflicts,
        spaces_merged,
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::Namespace;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        (temp_dir, db)
    }

    fn setup_with_branch(name: &str) -> (TempDir, Arc<Database>) {
        let (temp_dir, db) = setup();
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch(name).unwrap();
        (temp_dir, db)
    }

    fn write_kv(db: &Arc<Database>, branch: &str, space: &str, key: &str, value: Value) {
        let branch_id = resolve_branch_name(branch);
        let ns = Namespace::for_branch_space(branch_id, space);
        db.transaction(branch_id, |txn| {
            txn.put(
                Key::new(ns.clone(), TypeTag::KV, key.as_bytes().to_vec()),
                value,
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn write_state(db: &Arc<Database>, branch: &str, space: &str, key: &str, value: Value) {
        let branch_id = resolve_branch_name(branch);
        let ns = Namespace::for_branch_space(branch_id, space);
        db.transaction(branch_id, |txn| {
            txn.put(
                Key::new(ns.clone(), TypeTag::State, key.as_bytes().to_vec()),
                value,
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn write_json(db: &Arc<Database>, branch: &str, space: &str, key: &str, value: Value) {
        let branch_id = resolve_branch_name(branch);
        let ns = Namespace::for_branch_space(branch_id, space);
        db.transaction(branch_id, |txn| {
            txn.put(
                Key::new(ns.clone(), TypeTag::Json, key.as_bytes().to_vec()),
                value,
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn read_kv(db: &Arc<Database>, branch: &str, space: &str, key: &str) -> Option<Value> {
        let branch_id = resolve_branch_name(branch);
        let _ns = Namespace::for_branch_space(branch_id, space);
        let storage = db.storage();
        let entries = storage.list_by_type(&branch_id, TypeTag::KV);
        for (k, vv) in entries {
            if k.namespace.space == space && k.user_key == key.as_bytes() {
                return Some(vv.value);
            }
        }
        None
    }

    // =========================================================================
    // Fork Tests
    // =========================================================================

    #[test]
    fn test_fork_copies_all_data() {
        let (_temp, db) = setup_with_branch("source");

        // Write various data types
        write_kv(&db, "source", "default", "k1", Value::String("v1".into()));
        write_kv(&db, "source", "default", "k2", Value::Int(42));
        write_state(&db, "source", "default", "s1", Value::Bool(true));
        write_json(
            &db,
            "source",
            "default",
            "doc1",
            Value::String(r#"{"a":1}"#.into()),
        );

        let info = fork_branch(&db, "source", "dest").unwrap();
        assert_eq!(info.source, "source");
        assert_eq!(info.destination, "dest");
        assert!(info.keys_copied >= 4);

        // Verify all data is present in destination
        assert_eq!(
            read_kv(&db, "dest", "default", "k1"),
            Some(Value::String("v1".into()))
        );
        assert_eq!(read_kv(&db, "dest", "default", "k2"), Some(Value::Int(42)));

        // Verify state data
        let dest_id = resolve_branch_name("dest");
        let storage = db.storage();
        let state_entries = storage.list_by_type(&dest_id, TypeTag::State);
        assert!(
            state_entries
                .iter()
                .any(|(k, _)| k.user_key == b"s1" && k.namespace.space == "default"),
            "State data should be forked"
        );

        // Verify JSON data
        let json_entries = storage.list_by_type(&dest_id, TypeTag::Json);
        assert!(
            json_entries
                .iter()
                .any(|(k, _)| k.user_key == b"doc1" && k.namespace.space == "default"),
            "JSON data should be forked"
        );
    }

    #[test]
    fn test_fork_source_not_found() {
        let (_temp, db) = setup();
        let result = fork_branch(&db, "nonexistent", "dest");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found"),
            "Error should mention not found: {}",
            err
        );
    }

    #[test]
    fn test_fork_destination_exists() {
        let (_temp, db) = setup_with_branch("source");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("dest").unwrap();

        let result = fork_branch(&db, "source", "dest");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("already exists"),
            "Error should mention already exists: {}",
            err
        );
    }

    #[test]
    fn test_fork_multi_space() {
        let (_temp, db) = setup_with_branch("source");
        let space_index = SpaceIndex::new(db.clone());
        let source_id = resolve_branch_name("source");

        // Register additional spaces
        space_index.register(source_id, "alpha").unwrap();
        space_index.register(source_id, "beta").unwrap();

        // Write data to each space
        write_kv(&db, "source", "default", "k1", Value::Int(1));
        write_kv(&db, "source", "alpha", "k2", Value::Int(2));
        write_kv(&db, "source", "beta", "k3", Value::Int(3));

        let info = fork_branch(&db, "source", "dest").unwrap();
        assert!(info.spaces_copied >= 3);

        // Verify spaces exist in destination
        let dest_id = resolve_branch_name("dest");
        let dest_spaces = space_index.list(dest_id).unwrap();
        assert!(dest_spaces.contains(&"alpha".to_string()));
        assert!(dest_spaces.contains(&"beta".to_string()));

        // Verify data in each space
        assert_eq!(read_kv(&db, "dest", "default", "k1"), Some(Value::Int(1)));
        assert_eq!(read_kv(&db, "dest", "alpha", "k2"), Some(Value::Int(2)));
        assert_eq!(read_kv(&db, "dest", "beta", "k3"), Some(Value::Int(3)));
    }

    #[test]
    fn test_fork_source_unchanged() {
        let (_temp, db) = setup_with_branch("source");

        write_kv(
            &db,
            "source",
            "default",
            "k1",
            Value::String("original".into()),
        );

        fork_branch(&db, "source", "dest").unwrap();

        // Source data should be unchanged
        assert_eq!(
            read_kv(&db, "source", "default", "k1"),
            Some(Value::String("original".into()))
        );

        // Modify destination
        write_kv(
            &db,
            "dest",
            "default",
            "k1",
            Value::String("modified".into()),
        );

        // Source should still be unchanged
        assert_eq!(
            read_kv(&db, "source", "default", "k1"),
            Some(Value::String("original".into()))
        );
    }

    // =========================================================================
    // Diff Tests
    // =========================================================================

    #[test]
    fn test_diff_empty_branches() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        let diff = diff_branches(&db, "a", "b").unwrap();
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    #[test]
    fn test_diff_one_populated() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        write_kv(&db, "a", "default", "k1", Value::Int(1));
        write_kv(&db, "a", "default", "k2", Value::Int(2));

        let diff = diff_branches(&db, "a", "b").unwrap();
        // Keys in A but not B → removed
        assert_eq!(diff.summary.total_removed, 2);
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_modified, 0);

        // Reverse: diff(b, a) should show them as added
        let diff_rev = diff_branches(&db, "b", "a").unwrap();
        assert_eq!(diff_rev.summary.total_added, 2);
        assert_eq!(diff_rev.summary.total_removed, 0);
    }

    #[test]
    fn test_diff_modifications() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        write_kv(&db, "a", "default", "shared", Value::Int(1));
        write_kv(&db, "b", "default", "shared", Value::Int(2));

        let diff = diff_branches(&db, "a", "b").unwrap();
        assert_eq!(diff.summary.total_modified, 1);
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);

        // Verify the modified entry has correct values
        let space_diff = &diff.spaces[0];
        assert_eq!(space_diff.modified.len(), 1);
        assert_eq!(space_diff.modified[0].key, "shared");
        assert!(space_diff.modified[0]
            .value_a
            .as_ref()
            .unwrap()
            .contains("Int(1)"));
        assert!(space_diff.modified[0]
            .value_b
            .as_ref()
            .unwrap()
            .contains("Int(2)"));
    }

    #[test]
    fn test_diff_multi_space() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        let space_index = SpaceIndex::new(db.clone());
        let id_a = resolve_branch_name("a");
        let id_b = resolve_branch_name("b");
        space_index.register(id_a, "alpha").unwrap();
        space_index.register(id_b, "beta").unwrap();

        write_kv(&db, "a", "alpha", "k1", Value::Int(1));
        write_kv(&db, "b", "beta", "k2", Value::Int(2));

        let diff = diff_branches(&db, "a", "b").unwrap();

        // "alpha" only in A, "beta" only in B
        assert!(diff.summary.spaces_only_in_a.contains(&"alpha".to_string()));
        assert!(diff.summary.spaces_only_in_b.contains(&"beta".to_string()));

        // k1 in alpha (only A) → removed, k2 in beta (only B) → added
        assert_eq!(diff.summary.total_removed, 1);
        assert_eq!(diff.summary.total_added, 1);
    }

    // =========================================================================
    // Merge Tests
    // =========================================================================

    #[test]
    fn test_merge_lww() {
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        // Write conflicting data
        write_kv(&db, "target", "default", "shared", Value::Int(1));
        write_kv(&db, "source", "default", "shared", Value::Int(2));

        // Source-only key
        write_kv(
            &db,
            "source",
            "default",
            "new_key",
            Value::String("new".into()),
        );

        let info = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();
        assert!(info.keys_applied >= 2);

        // Target should now have source's value for "shared"
        assert_eq!(
            read_kv(&db, "target", "default", "shared"),
            Some(Value::Int(2))
        );
        // Target should have the new key
        assert_eq!(
            read_kv(&db, "target", "default", "new_key"),
            Some(Value::String("new".into()))
        );
    }

    #[test]
    fn test_merge_strict_no_conflicts() {
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        // Non-overlapping data (no conflicts)
        write_kv(&db, "target", "default", "target_key", Value::Int(1));
        write_kv(&db, "source", "default", "source_key", Value::Int(2));

        let info = merge_branches(&db, "source", "target", MergeStrategy::Strict).unwrap();
        assert!(info.keys_applied >= 1);

        // Target should have both keys
        assert_eq!(
            read_kv(&db, "target", "default", "target_key"),
            Some(Value::Int(1))
        );
        assert_eq!(
            read_kv(&db, "target", "default", "source_key"),
            Some(Value::Int(2))
        );
    }

    #[test]
    fn test_merge_strict_with_conflicts() {
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        // Conflicting data
        write_kv(&db, "target", "default", "shared", Value::Int(1));
        write_kv(&db, "source", "default", "shared", Value::Int(2));

        let result = merge_branches(&db, "source", "target", MergeStrategy::Strict);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("conflict") || err.contains("Merge"),
            "Error should mention conflict: {}",
            err
        );

        // Target should be unchanged (no writes happened)
        assert_eq!(
            read_kv(&db, "target", "default", "shared"),
            Some(Value::Int(1))
        );
    }

    #[test]
    fn test_merge_adds_without_deleting() {
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        // Target has keys that source doesn't
        write_kv(&db, "target", "default", "target_only", Value::Int(1));
        write_kv(&db, "source", "default", "source_only", Value::Int(2));

        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Target-only key should still exist
        assert_eq!(
            read_kv(&db, "target", "default", "target_only"),
            Some(Value::Int(1))
        );
        // Source-only key should now be in target
        assert_eq!(
            read_kv(&db, "target", "default", "source_only"),
            Some(Value::Int(2))
        );
    }

    #[test]
    fn test_merge_preserves_target_version_history() {
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        // Write multiple versions to target
        write_kv(&db, "target", "default", "key", Value::Int(1));
        write_kv(&db, "target", "default", "key", Value::Int(2));

        // Source has different value
        write_kv(&db, "source", "default", "key", Value::Int(99));

        // Check version history before merge
        let target_id = resolve_branch_name("target");
        let ns = Namespace::for_branch_space(target_id, "default");
        let history_key = Key::new(ns, TypeTag::KV, b"key".to_vec());
        let history_before = db.get_history(&history_key, None, None).unwrap();
        let versions_before = history_before.len();

        // Merge
        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Check version history after merge — should have one more version
        let ns2 = Namespace::for_branch_space(target_id, "default");
        let history_key2 = Key::new(ns2, TypeTag::KV, b"key".to_vec());
        let history_after = db.get_history(&history_key2, None, None).unwrap();
        assert!(
            history_after.len() > versions_before,
            "Merge should add a new version, not replace history. Before: {}, After: {}",
            versions_before,
            history_after.len()
        );

        // Latest value should be the merged value
        assert_eq!(
            read_kv(&db, "target", "default", "key"),
            Some(Value::Int(99))
        );
    }

    #[test]
    fn test_merge_binary_keys() {
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        let source_id = resolve_branch_name("source");
        let target_id = resolve_branch_name("target");

        // Write data with binary keys (simulating event sequence numbers)
        let binary_key: Vec<u8> = 1u64.to_be_bytes().to_vec();
        let ns_source = Namespace::for_branch_space(source_id, "default");
        db.transaction(source_id, |txn| {
            txn.put(
                Key::new(ns_source.clone(), TypeTag::Event, binary_key.clone()),
                Value::String("event-data".into()),
            )?;
            Ok(())
        })
        .unwrap();

        // Merge source into target
        let info = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();
        assert_eq!(info.keys_applied, 1, "Binary key entry should be merged");

        // Verify the binary key data is in target
        let storage = db.storage();
        let target_events = storage.list_by_type(&target_id, TypeTag::Event);
        assert!(
            target_events.iter().any(|(k, vv)| k.user_key == binary_key
                && vv.value == Value::String("event-data".into())),
            "Binary key event should be present in target after merge"
        );
    }

    #[test]
    fn test_merge_lww_reports_conflicts() {
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        // Write conflicting data
        write_kv(&db, "target", "default", "shared", Value::Int(1));
        write_kv(&db, "source", "default", "shared", Value::Int(2));

        // Source-only key (not a conflict)
        write_kv(
            &db,
            "source",
            "default",
            "new_key",
            Value::String("new".into()),
        );

        let info = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Should report the conflict even though LWW resolved it
        assert_eq!(info.conflicts.len(), 1, "Should report 1 conflict");
        assert_eq!(info.conflicts[0].key, "shared");
        assert_eq!(info.conflicts[0].primitive, PrimitiveType::Kv);
    }

    // =========================================================================
    // Post-Merge Vector Reload Tests (Phase 2)
    // =========================================================================

    #[test]
    fn test_merge_reloads_vector_backends() {
        use crate::primitives::branch::resolve_branch_name;
        use crate::primitives::vector::store::VectorStore;
        use crate::primitives::vector::{DistanceMetric, VectorConfig};

        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        let source_id = resolve_branch_name("source");
        let target_id = resolve_branch_name("target");

        let store = VectorStore::new(db.clone());
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Create collection and insert vectors on source branch
        store
            .create_collection(source_id, "default", "docs", config)
            .unwrap();
        store
            .insert(source_id, "default", "docs", "v1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(source_id, "default", "docs", "v2", &[0.0, 1.0, 0.0], None)
            .unwrap();
        store
            .insert(source_id, "default", "docs", "v3", &[0.0, 0.0, 1.0], None)
            .unwrap();

        // Verify source can search
        let source_results = store
            .search(source_id, "default", "docs", &[1.0, 0.0, 0.0], 3, None)
            .unwrap();
        assert_eq!(source_results.len(), 3, "Source should have 3 vectors");

        // Merge source into target (LWW)
        let info = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();
        assert!(info.keys_applied > 0, "Should have applied some keys");

        // Target should now be able to search the merged vectors
        let target_results = store
            .search(target_id, "default", "docs", &[1.0, 0.0, 0.0], 3, None)
            .unwrap();
        assert_eq!(
            target_results.len(),
            3,
            "Target should have 3 vectors after merge"
        );

        // Verify the nearest neighbor is correct
        assert_eq!(target_results[0].key, "v1");
        assert!(
            target_results[0].score > 0.99,
            "v1 should be the best match for [1,0,0]"
        );
    }

    #[test]
    fn test_merge_vectors_with_existing_target_collection() {
        use crate::primitives::branch::resolve_branch_name;
        use crate::primitives::vector::store::VectorStore;
        use crate::primitives::vector::{DistanceMetric, VectorConfig};

        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        let source_id = resolve_branch_name("source");
        let target_id = resolve_branch_name("target");

        let store = VectorStore::new(db.clone());
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Both branches have the same collection with different vectors
        store
            .create_collection(source_id, "default", "docs", config.clone())
            .unwrap();
        store
            .create_collection(target_id, "default", "docs", config)
            .unwrap();

        store
            .insert(
                source_id,
                "default",
                "docs",
                "from_source",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(
                target_id,
                "default",
                "docs",
                "from_target",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();

        // Merge source into target
        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Target should now have vectors from both branches
        let results = store
            .search(target_id, "default", "docs", &[1.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert_eq!(
            results.len(),
            2,
            "Target should have 2 vectors (from source + target)"
        );

        let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        assert!(
            keys.contains(&"from_source"),
            "Should contain source vector"
        );
        assert!(
            keys.contains(&"from_target"),
            "Should contain target vector"
        );
    }

    #[test]
    fn test_merge_no_vectors_is_noop() {
        // Merge with only KV data (no vectors) should not error
        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        write_kv(&db, "source", "default", "k1", Value::String("v1".into()));

        let info = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();
        assert!(info.keys_applied >= 1);

        // No vector collections means no error
        assert_eq!(
            read_kv(&db, "target", "default", "k1"),
            Some(Value::String("v1".into()))
        );
    }

    #[test]
    fn test_merge_get_works_after_vectorid_remap() {
        // This verifies the VectorId remapping is consistent between
        // KV records and in-memory backend: get() must return the
        // correct embedding after merge.
        use crate::primitives::branch::resolve_branch_name;
        use crate::primitives::vector::store::VectorStore;
        use crate::primitives::vector::{DistanceMetric, VectorConfig};

        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        let source_id = resolve_branch_name("source");
        let target_id = resolve_branch_name("target");

        let store = VectorStore::new(db.clone());
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Both branches have the same collection — VectorIds will collide
        store
            .create_collection(source_id, "default", "docs", config.clone())
            .unwrap();
        store
            .create_collection(target_id, "default", "docs", config)
            .unwrap();

        store
            .insert(
                source_id,
                "default",
                "docs",
                "src_vec",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(
                target_id,
                "default",
                "docs",
                "tgt_vec",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();

        // Merge
        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // get() for each vector must return the CORRECT embedding
        let src_entry = store
            .get(target_id, "default", "docs", "src_vec")
            .unwrap()
            .expect("src_vec should exist after merge")
            .value;
        assert_eq!(
            src_entry.embedding,
            vec![1.0, 0.0, 0.0],
            "src_vec embedding must be correct after VectorId remap"
        );

        let tgt_entry = store
            .get(target_id, "default", "docs", "tgt_vec")
            .unwrap()
            .expect("tgt_vec should exist after merge")
            .value;
        assert_eq!(
            tgt_entry.embedding,
            vec![0.0, 1.0, 0.0],
            "tgt_vec embedding must be correct after VectorId remap"
        );
    }

    #[test]
    fn test_merge_preserves_metadata() {
        use crate::primitives::branch::resolve_branch_name;
        use crate::primitives::vector::store::VectorStore;
        use crate::primitives::vector::{DistanceMetric, VectorConfig};

        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        let source_id = resolve_branch_name("source");
        let target_id = resolve_branch_name("target");

        let store = VectorStore::new(db.clone());
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        store
            .create_collection(source_id, "default", "docs", config)
            .unwrap();

        let metadata = serde_json::json!({"type": "article", "lang": "en"});
        store
            .insert(
                source_id,
                "default",
                "docs",
                "v1",
                &[1.0, 0.0, 0.0],
                Some(metadata.clone()),
            )
            .unwrap();

        // Merge
        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Verify metadata survives the reload
        let entry = store
            .get(target_id, "default", "docs", "v1")
            .unwrap()
            .expect("v1 should exist after merge")
            .value;
        assert_eq!(
            entry.metadata,
            Some(metadata),
            "Metadata must survive merge"
        );
    }

    #[test]
    fn test_merge_multiple_collections() {
        use crate::primitives::branch::resolve_branch_name;
        use crate::primitives::vector::store::VectorStore;
        use crate::primitives::vector::{DistanceMetric, VectorConfig};

        let (_temp, db) = setup_with_branch("target");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        let source_id = resolve_branch_name("source");
        let target_id = resolve_branch_name("target");

        let store = VectorStore::new(db.clone());

        // Source has two different collections
        let config3 = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let config2 = VectorConfig::new(2, DistanceMetric::Euclidean).unwrap();

        store
            .create_collection(source_id, "default", "col_a", config3)
            .unwrap();
        store
            .create_collection(source_id, "default", "col_b", config2)
            .unwrap();

        store
            .insert(
                source_id,
                "default",
                "col_a",
                "vec1",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(source_id, "default", "col_b", "vec2", &[1.0, 0.0], None)
            .unwrap();

        // Merge
        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Both collections should be searchable on target
        let results_a = store
            .search(target_id, "default", "col_a", &[1.0, 0.0, 0.0], 5, None)
            .unwrap();
        assert_eq!(results_a.len(), 1, "col_a should have 1 vector");
        assert_eq!(results_a[0].key, "vec1");

        let results_b = store
            .search(target_id, "default", "col_b", &[1.0, 0.0], 5, None)
            .unwrap();
        assert_eq!(results_b.len(), 1, "col_b should have 1 vector");
        assert_eq!(results_b[0].key, "vec2");
    }

    #[test]
    fn test_fork_vectors_searchable_immediately() {
        use crate::primitives::branch::resolve_branch_name;
        use crate::primitives::vector::store::VectorStore;
        use crate::primitives::vector::{DistanceMetric, VectorConfig};

        let (_temp, db) = setup_with_branch("source");

        let source_id = resolve_branch_name("source");

        let store = VectorStore::new(db.clone());
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        store
            .create_collection(source_id, "default", "docs", config)
            .unwrap();
        store
            .insert(source_id, "default", "docs", "v1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(source_id, "default", "docs", "v2", &[0.0, 1.0, 0.0], None)
            .unwrap();

        // Fork
        let info = fork_branch(&db, "source", "dest").unwrap();
        assert!(info.keys_copied > 0);

        let dest_id = resolve_branch_name("dest");

        // Forked branch should be immediately searchable
        let results = store
            .search(dest_id, "default", "docs", &[1.0, 0.0, 0.0], 5, None)
            .unwrap();
        assert_eq!(
            results.len(),
            2,
            "Forked branch should have 2 searchable vectors"
        );
        assert_eq!(results[0].key, "v1");

        // get() should also work
        let entry = store
            .get(dest_id, "default", "docs", "v1")
            .unwrap()
            .expect("v1 should be gettable on forked branch")
            .value;
        assert_eq!(entry.embedding, vec![1.0, 0.0, 0.0]);
    }
}
