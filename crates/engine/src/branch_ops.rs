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

const DATA_TYPE_TAGS: [TypeTag; 5] = [
    TypeTag::KV,
    TypeTag::Event,
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
    /// Number of keys copied (0 for COW fork)
    pub keys_copied: u64,
    /// Number of spaces copied
    pub spaces_copied: u64,
    /// Fork version (COW snapshot point). `Option` for serde backward compat.
    #[serde(default)]
    pub fork_version: Option<u64>,
}

/// A single entry in a branch diff.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchDiffEntry {
    /// User key (UTF-8 or hex-encoded for binary keys)
    pub key: String,
    /// Raw user key bytes (for programmatic access, preserves binary keys)
    pub raw_key: Vec<u8>,
    /// Primitive type of this entry
    pub primitive: PrimitiveType,
    /// Storage-level type tag (preserves Vector vs VectorConfig distinction)
    pub type_tag: TypeTag,
    /// Space this entry belongs to
    pub space: String,
    /// Value in branch A (None if not present)
    pub value_a: Option<Value>,
    /// Value in branch B (None if not present)
    pub value_b: Option<Value>,
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
    pub source_value: Option<Value>,
    /// Value in the target branch
    pub target_value: Option<Value>,
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

/// Options for filtering and time-scoping a branch diff.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DiffOptions {
    /// Optional filter to narrow the diff by primitive type or space.
    pub filter: Option<DiffFilter>,
    /// Optional timestamp (microseconds) for point-in-time comparison.
    /// When set, the diff compares branches as they looked at this timestamp.
    pub as_of: Option<u64>,
}

/// Filter criteria for narrowing a branch diff.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DiffFilter {
    /// Only include these primitive types. `None` means all types.
    pub primitives: Option<Vec<PrimitiveType>>,
    /// Only include these spaces. `None` means all spaces.
    pub spaces: Option<Vec<String>>,
}

// =============================================================================
// Helpers
// =============================================================================

/// Map a PrimitiveType to the TypeTags that should be scanned.
fn primitive_to_type_tags(prim: PrimitiveType) -> Vec<TypeTag> {
    match prim {
        PrimitiveType::Kv => vec![TypeTag::KV],
        PrimitiveType::Event => vec![TypeTag::Event],
        PrimitiveType::Json => vec![TypeTag::Json],
        PrimitiveType::Vector => vec![TypeTag::Vector, TypeTag::VectorConfig],
        PrimitiveType::Branch => vec![], // Branch metadata is not scanned in diffs
    }
}

/// Map a TypeTag to the corresponding PrimitiveType.
fn type_tag_to_primitive(tag: TypeTag) -> PrimitiveType {
    match tag {
        TypeTag::KV => PrimitiveType::Kv,
        TypeTag::Event => PrimitiveType::Event,
        TypeTag::Json => PrimitiveType::Json,
        TypeTag::Vector | TypeTag::VectorConfig => PrimitiveType::Vector,
        _ => PrimitiveType::Kv, // fallback for Branch/Space/State/Trace metadata tags
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

/// Fork a branch via O(1) COW (copy-on-write).
///
/// Creates a new branch with `destination` name and shares the source's
/// segments via inherited layers. No data is copied — writes to the
/// destination are isolated via the multi-layer read path.
///
/// Requires a disk-backed database. Ephemeral (in-memory) databases
/// return an error.
///
/// # Errors
///
/// - Source branch does not exist
/// - Destination branch already exists
/// - Database is ephemeral (no segments directory)
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

    // 3. Resolve BranchIds
    let source_id = resolve_branch_name(source);
    let dest_id = resolve_branch_name(destination);

    // 4. COW fork via storage layer — BEFORE creating KV metadata (#1724).
    //
    //    The storage fork writes durable manifests. By doing this before
    //    the KV metadata (create_branch), a crash can never leave a
    //    user-visible branch with no inherited data:
    //      - Crash before storage fork: no KV metadata → clean state
    //      - Crash after storage fork, before KV: orphaned storage state
    //        (harmless — refcounts rebuilt from manifests on recovery)
    //      - Crash after KV: complete fork
    let storage = db.storage();
    if !storage.has_segments_dir() {
        return Err(StrataError::invalid_input(
            "fork_branch requires a disk-backed database",
        ));
    }

    let (fork_version, _segments_shared) = storage
        .fork_branch(&source_id, &dest_id)
        .map_err(|e| StrataError::storage(format!("fork failed: {}", e)))?;

    // 5. Create destination branch in KV metadata (WAL-protected).
    //    If this fails, rollback the storage fork.
    if let Err(e) = branch_index.create_branch(destination) {
        storage.clear_branch(&dest_id);
        return Err(e);
    }

    // 6. Copy spaces from source to destination
    let source_spaces = space_index.list(source_id)?;
    let mut spaces_copied = 0u64;

    for space in &source_spaces {
        if space != "default" {
            space_index.register(dest_id, space)?;
        }
        spaces_copied += 1;
    }

    info!(
        target: "strata::branch_ops",
        source,
        destination,
        fork_version,
        "Branch forked (COW)"
    );

    Ok(ForkInfo {
        source: source.to_string(),
        destination: destination.to_string(),
        keys_copied: 0,
        spaces_copied,
        fork_version: Some(fork_version),
    })
}

// =============================================================================
// Diff
// =============================================================================

/// Compare two branches and return a structured diff.
///
/// Convenience wrapper around [`diff_branches_with_options`] with no filtering
/// or time constraints.
///
/// # Errors
///
/// - Either branch does not exist
pub fn diff_branches(
    db: &Arc<Database>,
    branch_a: &str,
    branch_b: &str,
) -> StrataResult<BranchDiffResult> {
    diff_branches_with_options(db, branch_a, branch_b, DiffOptions::default())
}

/// Compare two branches with optional filtering and point-in-time snapshot.
///
/// Scans data in both branches across all (or filtered) spaces and data types,
/// producing per-space diffs showing added, removed, and modified entries.
///
/// # Options
///
/// - `filter.primitives`: Only scan matching `TypeTag`s
/// - `filter.spaces`: Only compare listed spaces
/// - `as_of`: Use timestamp-aware storage scan for point-in-time comparison
///
/// # Errors
///
/// - Either branch does not exist
pub fn diff_branches_with_options(
    db: &Arc<Database>,
    branch_a: &str,
    branch_b: &str,
    options: DiffOptions,
) -> StrataResult<BranchDiffResult> {
    let space_index = SpaceIndex::new(db.clone());

    // 1. Verify both branches exist and resolve IDs
    let id_a = resolve_and_verify(db, branch_a)?;
    let id_b = resolve_and_verify(db, branch_b)?;

    // 2. List spaces in both branches
    let spaces_a: HashSet<String> = space_index.list(id_a)?.into_iter().collect();
    let spaces_b: HashSet<String> = space_index.list(id_b)?.into_iter().collect();

    // Apply space filter if set
    let space_filter: Option<HashSet<String>> = options
        .filter
        .as_ref()
        .and_then(|f| f.spaces.as_ref())
        .map(|s| s.iter().cloned().collect());

    let mut spaces_only_in_a: Vec<String> = spaces_a.difference(&spaces_b).cloned().collect();
    let mut spaces_only_in_b: Vec<String> = spaces_b.difference(&spaces_a).cloned().collect();
    let mut all_spaces: HashSet<String> = spaces_a.union(&spaces_b).cloned().collect();

    if let Some(ref allowed) = space_filter {
        all_spaces.retain(|s| allowed.contains(s));
        spaces_only_in_a.retain(|s| allowed.contains(s));
        spaces_only_in_b.retain(|s| allowed.contains(s));
    }

    // Determine which TypeTags to scan
    let type_tags: Vec<TypeTag> = match options.filter.as_ref().and_then(|f| f.primitives.as_ref())
    {
        Some(prims) => {
            let mut tags: Vec<TypeTag> = prims
                .iter()
                .flat_map(|p| primitive_to_type_tags(*p))
                .collect();
            tags.sort();
            tags.dedup();
            tags
        }
        None => DATA_TYPE_TAGS.to_vec(),
    };

    let storage = db.storage();
    let mut space_diffs = Vec::new();
    let mut total_added = 0usize;
    let mut total_removed = 0usize;
    let mut total_modified = 0usize;

    // 3. Scan data once per type tag, grouped by space
    let mut maps_a: HashMap<String, HashMap<(Vec<u8>, TypeTag), Value>> = HashMap::new();
    let mut maps_b: HashMap<String, HashMap<(Vec<u8>, TypeTag), Value>> = HashMap::new();

    for type_tag in &type_tags {
        let entries_a = match options.as_of {
            Some(ts) => storage.list_by_type_at_timestamp(&id_a, *type_tag, ts),
            None => storage.list_by_type(&id_a, *type_tag),
        };
        for (key, vv) in entries_a {
            if space_filter
                .as_ref()
                .map_or(true, |f| f.contains(&key.namespace.space))
            {
                maps_a
                    .entry(key.namespace.space.clone())
                    .or_default()
                    .insert((key.user_key.to_vec(), *type_tag), vv.value);
            }
        }

        let entries_b = match options.as_of {
            Some(ts) => storage.list_by_type_at_timestamp(&id_b, *type_tag, ts),
            None => storage.list_by_type(&id_b, *type_tag),
        };
        for (key, vv) in entries_b {
            if space_filter
                .as_ref()
                .map_or(true, |f| f.contains(&key.namespace.space))
            {
                maps_b
                    .entry(key.namespace.space.clone())
                    .or_default()
                    .insert((key.user_key.to_vec(), *type_tag), vv.value);
            }
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
                        type_tag: *tag,
                        space: space.clone(),
                        value_a: Some(val_a.clone()),
                        value_b: None,
                    });
                }
                Some(val_b) => {
                    if val_a != val_b {
                        modified.push(BranchDiffEntry {
                            key: key_str,
                            raw_key: user_key.clone(),
                            primitive,
                            type_tag: *tag,
                            space: space.clone(),
                            value_a: Some(val_a.clone()),
                            value_b: Some(val_b.clone()),
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
                    type_tag: *tag,
                    space: space.clone(),
                    value_a: None,
                    value_b: Some(val_b.clone()),
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
                    source_value: entry.value_b.clone(),
                    target_value: entry.value_a.clone(),
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
                    source_value: entry.value_b.clone(),
                    target_value: entry.value_a.clone(),
                })
            })
            .collect()
    } else {
        vec![]
    };

    // 3. Resolve IDs
    let source_id = resolve_branch_name(source);
    let target_id = resolve_branch_name(target);

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

        // Write to target using values already present in the diff entries.
        // Each diff entry corresponds to exactly one (user_key, type_tag) pair
        // from the diff scan, so we use the type_tag directly.
        let mut batch: Vec<(Key, Value)> = Vec::new();
        for diff_entry in &entries_to_apply {
            if let Some(value) = &diff_entry.value_b {
                let target_ns = Arc::new(Namespace::for_branch_space(target_id, space));
                let target_key =
                    Key::new(target_ns, diff_entry.type_tag, diff_entry.raw_key.clone());
                batch.push((target_key, value.clone()));
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
// Materialize
// =============================================================================

/// Information returned after materializing inherited layers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MaterializeInfo {
    /// Branch name
    pub branch: String,
    /// Total entries materialized across all layers
    pub entries_materialized: u64,
    /// Total new segments created
    pub segments_created: usize,
    /// Number of inherited layers collapsed
    pub layers_collapsed: usize,
}

/// Materialize all inherited layers of a branch.
///
/// Collapses inherited layers deepest-first so that each layer's data
/// becomes "own" data before shallower layers are processed (enabling
/// shadow detection for subsequent layers).
///
/// # Errors
///
/// - Branch does not exist
/// - Database is ephemeral
pub fn materialize_branch(db: &Arc<Database>, branch_name: &str) -> StrataResult<MaterializeInfo> {
    let branch_id = resolve_and_verify(db, branch_name)?;
    let storage = db.storage();

    if !storage.has_segments_dir() {
        return Err(StrataError::invalid_input(
            "materialize_branch requires a disk-backed database",
        ));
    }

    let mut total_entries = 0u64;
    let mut total_segments = 0usize;
    let mut layers_collapsed = 0usize;

    // Loop: materialize deepest layer first
    loop {
        let layer_count = storage.inherited_layer_count(&branch_id);
        if layer_count == 0 {
            break;
        }
        let deepest = layer_count - 1;
        match storage.materialize_layer(&branch_id, deepest) {
            Ok(result) => {
                total_entries += result.entries_materialized;
                total_segments += result.segments_created;
                // Only count as collapsed if the layer was actually removed.
                // When another thread is materializing this branch, the call
                // returns Ok(0, 0) without removing the layer (#1693).
                let new_count = storage.inherited_layer_count(&branch_id);
                if new_count < layer_count {
                    layers_collapsed += 1;
                } else if result.entries_materialized == 0 && result.segments_created == 0 {
                    // No work done and layer not removed — concurrent
                    // materialization in progress. Break instead of spinning.
                    break;
                }
            }
            Err(e) => {
                return Err(StrataError::storage(format!(
                    "materialize_layer failed: {}",
                    e
                )));
            }
        }
    }

    info!(
        target: "strata::branch_ops",
        branch = branch_name,
        entries_materialized = total_entries,
        segments_created = total_segments,
        layers_collapsed,
        "Branch materialized"
    );

    Ok(MaterializeInfo {
        branch: branch_name.to_string(),
        entries_materialized: total_entries,
        segments_created: total_segments,
        layers_collapsed,
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
        let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
        db.transaction(branch_id, |txn| {
            txn.put(
                Key::new(ns.clone(), TypeTag::KV, key.as_bytes().to_vec()),
                value,
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn write_json(db: &Arc<Database>, branch: &str, space: &str, key: &str, value: Value) {
        let branch_id = resolve_branch_name(branch);
        let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
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
            if k.namespace.space == space && *k.user_key == *key.as_bytes() {
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
        assert_eq!(info.keys_copied, 0, "fork copies zero keys");
        assert!(info.fork_version.is_some(), "fork returns fork_version");

        // Verify all data is present in destination (via inherited layers)
        assert_eq!(
            read_kv(&db, "dest", "default", "k1"),
            Some(Value::String("v1".into()))
        );
        assert_eq!(read_kv(&db, "dest", "default", "k2"), Some(Value::Int(42)));

        // Verify JSON data visible through inherited layers
        let dest_id = resolve_branch_name("dest");
        let storage = db.storage();
        let json_entries = storage.list_by_type(&dest_id, TypeTag::Json);
        assert!(
            json_entries
                .iter()
                .any(|(k, _)| *k.user_key == *b"doc1" && k.namespace.space == "default"),
            "JSON data should be visible through inherited layers"
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
        assert_eq!(space_diff.modified[0].value_a, Some(Value::Int(1)));
        assert_eq!(space_diff.modified[0].value_b, Some(Value::Int(2)));
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
        let ns = Arc::new(Namespace::for_branch_space(target_id, "default"));
        let history_key = Key::new(ns, TypeTag::KV, b"key".to_vec());
        let history_before = db.get_history(&history_key, None, None).unwrap();
        let versions_before = history_before.len();

        // Merge
        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Check version history after merge — should have one more version
        let ns2 = Arc::new(Namespace::for_branch_space(target_id, "default"));
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
        let ns_source = Arc::new(Namespace::for_branch_space(source_id, "default"));
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
            target_events
                .iter()
                .any(|(k, vv)| *k.user_key == *binary_key
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
        // Verify conflict carries actual Values (source=B=incoming, target=A=base)
        assert_eq!(info.conflicts[0].source_value, Some(Value::Int(2)));
        assert_eq!(info.conflicts[0].target_value, Some(Value::Int(1)));
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

    // =========================================================================
    // DiffOptions Tests (filtering + as_of)
    // =========================================================================

    #[test]
    fn test_diff_filter_by_primitive_kv_only() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        // Write KV and JSON data to branch A only
        write_kv(&db, "a", "default", "kv1", Value::Int(1));
        write_json(&db, "a", "default", "j1", Value::String("doc".into()));

        // Unfiltered: should see all 2 removed
        let diff_all = diff_branches(&db, "a", "b").unwrap();
        assert_eq!(diff_all.summary.total_removed, 2);

        // Filter to KV only: should see 1 removed
        let diff_kv = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: Some(vec![PrimitiveType::Kv]),
                    spaces: None,
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff_kv.summary.total_removed, 1);
        assert_eq!(diff_kv.spaces[0].removed[0].primitive, PrimitiveType::Kv);
        assert_eq!(diff_kv.spaces[0].removed[0].key, "kv1");
    }

    #[test]
    fn test_diff_filter_by_multiple_primitives() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        write_kv(&db, "a", "default", "kv1", Value::Int(1));
        write_json(&db, "a", "default", "j1", Value::String("doc".into()));

        // Filter to KV + Json: should see 2 removed
        let diff = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: Some(vec![PrimitiveType::Kv, PrimitiveType::Json]),
                    spaces: None,
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_removed, 2);
    }

    #[test]
    fn test_diff_filter_by_space() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        let space_index = SpaceIndex::new(db.clone());
        let id_a = resolve_branch_name("a");
        space_index.register(id_a, "alpha").unwrap();
        space_index.register(id_a, "beta").unwrap();

        write_kv(&db, "a", "default", "k1", Value::Int(1));
        write_kv(&db, "a", "alpha", "k2", Value::Int(2));
        write_kv(&db, "a", "beta", "k3", Value::Int(3));

        // Unfiltered: 3 removed
        let diff_all = diff_branches(&db, "a", "b").unwrap();
        assert_eq!(diff_all.summary.total_removed, 3);

        // Filter to "alpha" only: 1 removed
        let diff_alpha = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: None,
                    spaces: Some(vec!["alpha".to_string()]),
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff_alpha.summary.total_removed, 1);
        assert_eq!(diff_alpha.spaces[0].removed[0].key, "k2");

        // spaces_only_in_a should also be filtered
        // "alpha", "beta", "default" are all only in A, but filter to "alpha"
        // should exclude "beta" and "default"
        assert!(
            !diff_alpha
                .summary
                .spaces_only_in_a
                .contains(&"beta".to_string()),
            "Filtered diff should not report 'beta' in spaces_only_in_a"
        );
        assert!(
            !diff_alpha
                .summary
                .spaces_only_in_a
                .contains(&"default".to_string()),
            "Filtered diff should not report 'default' in spaces_only_in_a"
        );
    }

    #[test]
    fn test_diff_filter_combined_primitive_and_space() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        let space_index = SpaceIndex::new(db.clone());
        let id_a = resolve_branch_name("a");
        space_index.register(id_a, "alpha").unwrap();

        // KV in default, KV in alpha
        write_kv(&db, "a", "default", "kv_d", Value::Int(1));
        write_kv(&db, "a", "alpha", "kv_a", Value::Int(2));

        // Filter: KV only + alpha only → should see 1 removed (kv_a)
        let diff = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: Some(vec![PrimitiveType::Kv]),
                    spaces: Some(vec!["alpha".to_string()]),
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_removed, 1);
        assert_eq!(diff.spaces[0].removed[0].key, "kv_a");
        assert_eq!(diff.spaces[0].removed[0].primitive, PrimitiveType::Kv);
    }

    #[test]
    fn test_diff_filter_empty_primitives_returns_empty() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        write_kv(&db, "a", "default", "k1", Value::Int(1));

        // Empty primitives filter → no type tags → empty diff
        let diff = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: Some(vec![]),
                    spaces: None,
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    #[test]
    fn test_diff_filter_nonexistent_space_returns_empty() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        write_kv(&db, "a", "default", "k1", Value::Int(1));

        // Filter to a space that doesn't exist → empty diff
        let diff = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: None,
                    spaces: Some(vec!["nonexistent".to_string()]),
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_added, 0);
    }

    #[test]
    fn test_diff_as_of_sees_old_values() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        // Write initial value to A
        write_kv(&db, "a", "default", "key", Value::Int(1));

        // Record a timestamp after the first write
        std::thread::sleep(std::time::Duration::from_millis(10));
        let snapshot_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Overwrite with a new value after the snapshot
        write_kv(&db, "a", "default", "key", Value::Int(2));

        // Current diff: should show modified with value_a=Int(2) (no data in B)
        let diff_now = diff_branches(&db, "a", "b").unwrap();
        assert_eq!(diff_now.summary.total_removed, 1);
        assert_eq!(
            diff_now.spaces[0].removed[0].value_a,
            Some(Value::Int(2)),
            "Current diff should see latest value"
        );

        // as_of snapshot: should show value_a=Int(1)
        let diff_snap = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: None,
                as_of: Some(snapshot_ts),
            },
        )
        .unwrap();
        assert_eq!(diff_snap.summary.total_removed, 1);
        assert_eq!(
            diff_snap.spaces[0].removed[0].value_a,
            Some(Value::Int(1)),
            "Snapshot diff should see value at snapshot time"
        );
    }

    #[test]
    fn test_diff_as_of_before_any_writes_returns_empty() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        // Record a timestamp before any data writes
        let before_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        std::thread::sleep(std::time::Duration::from_millis(10));

        write_kv(&db, "a", "default", "k1", Value::Int(1));

        // as_of before any writes: should be empty diff
        let diff = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: None,
                as_of: Some(before_ts),
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    #[test]
    fn test_diff_as_of_with_filter() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        // Write KV data
        write_kv(&db, "a", "default", "kv1", Value::Int(1));

        std::thread::sleep(std::time::Duration::from_millis(10));
        let snapshot_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        // KV exists at snapshot_ts; filter to KV only
        let diff = diff_branches_with_options(
            &db,
            "a",
            "b",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: Some(vec![PrimitiveType::Kv]),
                    spaces: None,
                }),
                as_of: Some(snapshot_ts),
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_removed, 1);
        assert_eq!(diff.spaces[0].removed[0].primitive, PrimitiveType::Kv);
    }

    #[test]
    fn test_diff_default_options_matches_diff_branches() {
        // Verify that diff_branches_with_options with default options
        // produces the same result as diff_branches
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        write_kv(&db, "a", "default", "k1", Value::Int(1));
        write_kv(&db, "b", "default", "k1", Value::Int(2));
        write_kv(&db, "b", "default", "k2", Value::Int(3));

        let diff1 = diff_branches(&db, "a", "b").unwrap();
        let diff2 = diff_branches_with_options(&db, "a", "b", DiffOptions::default()).unwrap();

        assert_eq!(diff1, diff2);
    }

    #[test]
    fn test_diff_value_fidelity_complex_types() {
        // Verify that complex Value types round-trip through diff correctly
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("b").unwrap();

        let complex_val = Value::Array(Box::new(vec![
            Value::Int(1),
            Value::String("hello".into()),
            Value::Bool(true),
        ]));
        write_kv(&db, "a", "default", "arr", complex_val.clone());

        let diff = diff_branches(&db, "a", "b").unwrap();
        assert_eq!(diff.summary.total_removed, 1);
        assert_eq!(
            diff.spaces[0].removed[0].value_a,
            Some(complex_val),
            "Complex Value should be preserved exactly"
        );
    }

    #[test]
    fn test_fork_vectors_kv_visible() {
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

        // Fork via COW
        let info = fork_branch(&db, "source", "dest").unwrap();
        assert_eq!(info.keys_copied, 0, "fork copies zero keys");
        assert!(info.fork_version.is_some());

        let dest_id = resolve_branch_name("dest");

        // Vector KV data (VectorConfig) is visible through inherited layers.
        // Full vector get()/search() require HNSW backend materialization (Epic D).
        let storage = db.storage();
        let vec_entries = storage.list_by_type(&dest_id, TypeTag::VectorConfig);
        assert!(
            !vec_entries.is_empty(),
            "VectorConfig should be visible through inherited layers"
        );
    }

    #[test]
    fn test_force_materialize_all_layers() {
        let (_temp, db) = setup_with_branch("source");

        // Write data to source
        write_kv(&db, "source", "default", "k1", Value::Int(1));
        write_kv(&db, "source", "default", "k2", Value::Int(2));

        // Fork source → dest
        let fork_info = fork_branch(&db, "source", "dest").unwrap();
        assert!(fork_info.fork_version.is_some());

        // Verify inherited layers exist
        let dest_id = resolve_branch_name("dest");
        let storage = db.storage();
        assert!(
            storage.inherited_layer_count(&dest_id) > 0,
            "dest should have inherited layers"
        );

        // Materialize all layers
        let mat_info = materialize_branch(&db, "dest").unwrap();
        assert!(mat_info.layers_collapsed > 0);

        // No inherited layers remain
        assert_eq!(storage.inherited_layer_count(&dest_id), 0);

        // Data still accessible
        let val = read_kv(&db, "dest", "default", "k1");
        assert_eq!(val, Some(Value::Int(1)));
        let val = read_kv(&db, "dest", "default", "k2");
        assert_eq!(val, Some(Value::Int(2)));
    }

    #[test]
    fn test_materialize_then_diff_merge() {
        let (_temp, db) = setup_with_branch("source");

        // Write to source
        write_kv(&db, "source", "default", "shared", Value::Int(1));

        // Fork
        fork_branch(&db, "source", "dest").unwrap();

        // Write different data to dest
        write_kv(&db, "dest", "default", "dest_only", Value::Int(42));

        // Materialize dest
        let mat_info = materialize_branch(&db, "dest").unwrap();
        assert!(mat_info.layers_collapsed > 0);

        // Diff should still work
        let diff = diff_branches(&db, "source", "dest").unwrap();
        assert_eq!(
            diff.summary.total_added, 1,
            "dest_only should show as added in dest"
        );

        // Merge dest → source should work
        let merge_info =
            merge_branches(&db, "dest", "source", MergeStrategy::LastWriterWins).unwrap();
        assert!(merge_info.keys_applied > 0);

        // Source should now have dest_only
        let val = read_kv(&db, "source", "default", "dest_only");
        assert_eq!(val, Some(Value::Int(42)));
    }

    /// Regression test for #1693: concurrent materialize_layer() calls on the
    /// same branch must not duplicate L0 segments or corrupt refcounts.
    ///
    /// Races N threads calling materialize_branch() simultaneously. The
    /// materializing_branches guard (#1703) ensures only one proceeds; the
    /// rest return early. After all threads complete we verify:
    ///   - exactly one set of materialized segments (no duplication)
    ///   - inherited layers fully collapsed
    ///   - data remains correct
    #[test]
    fn test_issue_1693_concurrent_materialize_no_duplication() {
        use std::sync::Barrier;
        use std::thread;

        let (_temp, db) = setup_with_branch("source");

        // Write enough keys to produce meaningful segments
        for i in 0..50 {
            write_kv(
                &db,
                "source",
                "default",
                &format!("key_{:04}", i),
                Value::Int(i),
            );
        }

        // Fork source → child
        fork_branch(&db, "source", "child").unwrap();

        let child_id = resolve_branch_name("child");
        let storage = db.storage();
        let layer_count_before = storage.inherited_layer_count(&child_id);
        assert!(
            layer_count_before > 0,
            "child must have inherited layers before materialization"
        );

        // Record L0 segment count before materialization
        let l0_before = storage.l0_segment_count(&child_id);

        // Race 8 threads all calling materialize_branch on the same child
        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let db = db.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    materialize_branch(&db, "child")
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All calls must succeed
        for (i, r) in results.iter().enumerate() {
            assert!(r.is_ok(), "thread {} failed: {:?}", i, r);
        }

        // Exactly one thread should have materialized entries (entries > 0);
        // the rest were serialized out by the materializing_branches guard
        // and returned 0 entries.
        let workers: Vec<_> = results
            .iter()
            .filter(|r| r.as_ref().unwrap().entries_materialized > 0)
            .collect();
        assert_eq!(
            workers.len(),
            1,
            "expected exactly 1 thread to materialize entries, got {}",
            workers.len()
        );

        // Non-winning threads must report layers_collapsed == 0 (#1693 fix).
        let idle_count = results
            .iter()
            .filter(|r| r.as_ref().unwrap().layers_collapsed == 0)
            .count();
        assert!(
            idle_count >= (num_threads - 1),
            "at most 1 thread should report layers_collapsed > 0"
        );

        // Inherited layers must be fully collapsed
        assert_eq!(
            storage.inherited_layer_count(&child_id),
            0,
            "all inherited layers should be removed after materialization"
        );

        // L0 segments should have increased by a bounded amount (no duplication).
        // The winner materializes the layer into new L0 segments; the losers add
        // nothing. So the increase equals the winner's segments_created.
        let l0_after = storage.l0_segment_count(&child_id);
        let winner = workers[0].as_ref().unwrap();
        assert_eq!(
            l0_after - l0_before,
            winner.segments_created,
            "L0 segment increase must match the single winner's output"
        );

        // Data must still be correct
        for i in 0..50 {
            let val = read_kv(&db, "child", "default", &format!("key_{:04}", i));
            assert_eq!(
                val,
                Some(Value::Int(i)),
                "key_{:04} should be readable after materialization",
                i
            );
        }
    }

    /// Stress variant of #1693: race background-style (single deepest layer)
    /// and explicit-style (full materialize_branch) concurrently.
    #[test]
    fn test_issue_1693_concurrent_materialize_bg_vs_explicit() {
        use std::sync::Barrier;
        use std::thread;

        let (_temp, db) = setup_with_branch("parent");

        for i in 0..20 {
            write_kv(
                &db,
                "parent",
                "default",
                &format!("pk_{:03}", i),
                Value::Int(i),
            );
        }

        fork_branch(&db, "parent", "racing").unwrap();

        let racing_id = resolve_branch_name("racing");
        let storage = db.storage();
        assert!(storage.inherited_layer_count(&racing_id) > 0);

        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: simulate background scheduler — materialize deepest layer
        let db1 = db.clone();
        let barrier1 = barrier.clone();
        let bg_handle = thread::spawn(move || {
            barrier1.wait();
            let storage = db1.storage();
            let racing_id = resolve_branch_name("racing");
            let layer_count = storage.inherited_layer_count(&racing_id);
            if layer_count > 0 {
                let deepest = layer_count - 1;
                storage.materialize_layer(&racing_id, deepest)
            } else {
                Ok(strata_storage::MaterializeResult {
                    entries_materialized: 0,
                    segments_created: 0,
                })
            }
        });

        // Thread 2: explicit API call
        let db2 = db.clone();
        let barrier2 = barrier.clone();
        let api_handle = thread::spawn(move || {
            barrier2.wait();
            materialize_branch(&db2, "racing")
        });

        let bg_result = bg_handle.join().unwrap();
        let api_result = api_handle.join().unwrap();

        // Both must succeed (no panics, no corruption)
        assert!(
            bg_result.is_ok(),
            "background materialize failed: {:?}",
            bg_result
        );
        assert!(
            api_result.is_ok(),
            "API materialize failed: {:?}",
            api_result
        );

        // All layers collapsed
        assert_eq!(storage.inherited_layer_count(&racing_id), 0);

        // Data correct
        for i in 0..20 {
            let val = read_kv(&db, "racing", "default", &format!("pk_{:03}", i));
            assert_eq!(val, Some(Value::Int(i)));
        }
    }

    /// Regression test for #1704: materialization must run even when no branches
    /// need flushing.
    ///
    /// Before the fix, `schedule_flush_if_needed()` returned early when
    /// `branches_needing_flush()` was empty, skipping the materialization loop.
    /// This meant deeply-forked idle branches accumulated inherited layers
    /// indefinitely.
    ///
    /// This test creates a fork chain exceeding MAX_INHERITED_LAYERS (4), then
    /// triggers `schedule_flush_if_needed` via a write to a separate branch
    /// (which doesn't fill its memtable, so no flush is needed). The leaf
    /// branch's inherited layers must still get materialized.
    ///
    /// Note: After #1724, `create_branch()` (called after the storage fork)
    /// triggers `schedule_flush_if_needed`, which eagerly materializes deep
    /// branches during the fork chain itself. The storage-level fork is used
    /// directly to build the chain without triggering engine-level materialization,
    /// so we can still verify that writing to root triggers materialization.
    #[test]
    fn test_issue_1704_materialization_runs_without_pending_flush() {
        // Chain: root → b1 → b2 → b3 → b4 → leaf
        // leaf ends up with 5 inherited layers (> MAX_INHERITED_LAYERS=4).
        let branch_names = ["root", "b1", "b2", "b3", "b4", "leaf"];

        let (_temp, db) = setup_with_branch(branch_names[0]);

        // Write data to root so it has segments to inherit.
        write_kv(&db, "root", "default", "origin", Value::Int(0));

        // Build the fork chain via the storage layer directly, bypassing
        // the engine's fork_branch() to avoid triggering materialization
        // during chain construction.  This isolates the #1704 test from
        // the #1724 ordering change (create_branch after storage fork now
        // triggers schedule_flush_if_needed which eagerly materializes).
        for i in 1..branch_names.len() {
            let source = branch_names[i - 1];
            let dest = branch_names[i];

            // Write data to source before forking (ensures source has segments).
            write_kv(
                &db,
                source,
                "default",
                &format!("{}_key", source),
                Value::Int(i as i64),
            );

            let source_id = resolve_branch_name(source);
            let dest_id = resolve_branch_name(dest);

            // Storage-only fork (no KV metadata, no schedule_flush_if_needed).
            db.storage().fork_branch(&source_id, &dest_id).unwrap();
        }

        let leaf_id = resolve_branch_name("leaf");
        let storage = db.storage();

        let layers_before = storage.inherited_layer_count(&leaf_id);
        assert!(
            layers_before > 4,
            "leaf should exceed MAX_INHERITED_LAYERS (4), got {}",
            layers_before,
        );

        // Write to root — triggers schedule_flush_if_needed().
        // Root's memtable is NOT full, so branches_needing_flush() returns
        // empty. Pre-fix, this early-returned and skipped materialization.
        write_kv(&db, "root", "default", "trigger", Value::Int(999));

        // Materialization now runs on the background scheduler (#1736).
        // Drain to ensure it completes before checking.
        db.scheduler().drain();

        // Post-fix, materialization should have run on the leaf branch.
        let layers_after = storage.inherited_layer_count(&leaf_id);
        assert!(
            layers_after < layers_before,
            "leaf inherited layers should decrease after materialization \
             (before={}, after={})",
            layers_before,
            layers_after,
        );

        // Verify data is still accessible on the leaf branch.
        let val = read_kv(&db, "leaf", "default", "origin");
        assert_eq!(
            val,
            Some(Value::Int(0)),
            "root data must be visible on leaf"
        );
    }

    // =========================================================================
    // Issue #1724 — fork_branch crash atomicity
    // =========================================================================

    /// Simulates the crash scenario from issue #1724: if the process crashes
    /// after fork_branch() creates the branch in KV metadata but before the
    /// storage-layer COW fork completes, recovery reveals an empty orphan
    /// branch that has no inherited data.
    ///
    /// The fix reorders fork_branch() to perform the storage fork (with
    /// durable manifest writes) BEFORE creating branch KV metadata, so a
    /// crash can never leave a user-visible branch without its data.
    #[test]
    fn test_issue_1724_fork_crash_no_orphan_branch() {
        let temp_dir = TempDir::new().unwrap();

        // Phase 1: Create source branch with data, then do a proper fork.
        // After fork, drop the database to simulate "crash after completion."
        // This verifies the fork is fully durable.
        {
            let db = Database::open(temp_dir.path()).unwrap();
            let branch_index = BranchIndex::new(db.clone());
            branch_index.create_branch("main").unwrap();

            write_kv(
                &db,
                "main",
                "default",
                "key1",
                Value::String("from_parent".into()),
            );

            // Fork main -> child
            fork_branch(&db, "main", "child").unwrap();

            // Drop without explicit shutdown (simulates unclean exit)
        }

        // Phase 2: Reopen and verify fork survived recovery with data intact.
        {
            let db = Database::open(temp_dir.path()).unwrap();
            let branch_index = BranchIndex::new(db.clone());

            // Child must exist
            assert!(
                branch_index.exists("child").unwrap(),
                "forked branch must survive recovery"
            );

            // Child must have inherited data — an empty branch is the bug
            let val = read_kv(&db, "child", "default", "key1");
            assert_eq!(
                val,
                Some(Value::String("from_parent".into())),
                "forked branch must have inherited data after recovery (issue #1724)"
            );
        }
    }

    /// Verifies that fork_branch() does not create branch metadata in KV
    /// before the storage fork completes. With the pre-fix ordering
    /// (KV first, storage second), a failed storage fork leaves orphaned
    /// metadata even though the error path attempts rollback.
    ///
    /// This test verifies the post-fix ordering: storage fork happens
    /// first, so if it fails, no KV metadata was ever written.
    #[test]
    fn test_issue_1724_fork_error_no_orphan_metadata() {
        // Use a cache (in-memory) database where storage fork must fail
        let db = Database::cache().unwrap();
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("source").unwrap();

        write_kv(&db, "source", "default", "k1", Value::String("v1".into()));

        // fork_branch must fail — no disk storage
        let result = fork_branch(&db, "source", "child");
        assert!(result.is_err(), "fork on ephemeral DB must fail");

        // After the fix: "child" must NOT exist in KV because the storage
        // fork is attempted before create_branch(). Since the storage fork
        // fails (no segments dir), create_branch() is never reached.
        //
        // Before the fix: create_branch() was called first, then rollback
        // via delete_branch(). The rollback works for errors, but NOT for
        // crashes — this test verifies the ordering property that makes
        // crashes safe too.
        assert!(
            !branch_index.exists("child").unwrap(),
            "failed fork must not leave orphaned branch metadata"
        );
    }

    /// Verifies the storage manifest is durable before fork_branch()
    /// returns. After the fix (#1724), the storage fork (which writes
    /// the manifest) happens before KV metadata creation, so a crash
    /// after return can never leave a branch without its inherited data.
    #[test]
    fn test_issue_1724_fork_manifest_before_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch("main").unwrap();

        write_kv(
            &db,
            "main",
            "default",
            "key1",
            Value::String("parent_data".into()),
        );

        fork_branch(&db, "main", "child").unwrap();

        // Verify storage manifest exists and has inherited layers.
        let child_id = resolve_branch_name("child");
        let child_hex = {
            let bytes = child_id.as_bytes();
            let mut s = String::with_capacity(32);
            for &b in bytes.iter() {
                use std::fmt::Write;
                let _ = write!(s, "{:02x}", b);
            }
            s
        };
        let manifest_dir = temp_dir.path().join("segments").join(&child_hex);
        let manifest = strata_storage::manifest::read_manifest(&manifest_dir).unwrap();
        assert!(
            manifest.is_some(),
            "storage manifest must exist after fork_branch()"
        );
        assert!(
            !manifest.unwrap().inherited_layers.is_empty(),
            "forked branch must have inherited layers in manifest"
        );
    }
}
