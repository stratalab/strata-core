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
//!
//! ## Submodules
//!
//! - [`primitive_merge`] — `PrimitiveMergeHandler` trait and per-primitive
//!   handlers. See `docs/design/branching/primitive-aware-merge.md`.
//! - [`json_merge`] — Per-document JSON three-way merge helpers used by
//!   `JsonMergeHandler` to combine disjoint path edits on the same doc.

pub(crate) mod branch_control_store;
pub mod dag_hooks;
pub(crate) mod json_merge;
pub mod primitive_merge;

pub(crate) use dag_hooks::{
    dispatch_cherry_pick_hook, dispatch_fork_hook, dispatch_merge_hook, dispatch_revert_hook,
    with_branch_dag_hooks_suppressed,
};

use crate::database::Database;
use crate::primitives::branch::resolve_branch_name;
use crate::primitives::branch::BranchIndex;
use crate::SpaceIndex;
use primitive_merge::{
    build_merge_registry, MergeHandlerRegistry, MergePlanCtx, MergePostCommitCtx, MergePrecheckCtx,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use strata_core::branch::BranchLifecycleStatus;
use strata_core::id::CommitVersion;
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::value::Value;
use strata_core::StrataError;
use strata_core::StrataResult;
use strata_core::{
    BranchControlRecord, BranchGeneration, BranchRef, ForkAnchor, PrimitiveType, Version,
    VersionedValue,
};
use tracing::info;

use crate::branch_ops::branch_control_store::{active_ptr_key, BranchControlStore, MergeBasePoint};

// =============================================================================
// Constants and key-format helpers
// =============================================================================

/// All primitive type tags that participate in the per-primitive merge
/// dispatch. `pub(crate)` so the `primitive_merge` submodule's registry can
/// iterate the same tag set.
pub(crate) const DATA_TYPE_TAGS: [TypeTag; 5] = [
    TypeTag::KV,
    TypeTag::Event,
    TypeTag::Json,
    TypeTag::Vector,
    TypeTag::Graph,
];

/// The well-known branch name used for internal metadata (tags, notes).
const SYSTEM_BRANCH: &str = "_system_";

fn tag_key(branch: &str, name: &str) -> String {
    format!("tag:{branch}:{name}")
}

fn tag_prefix(branch: &str) -> String {
    format!("tag:{branch}:")
}

fn note_key(branch: &str, version: u64) -> String {
    format!("note:{branch}:{version}")
}

fn note_prefix(branch: &str) -> String {
    format!("note:{branch}:")
}

/// Delete all branch-scoped tag/note annotation rows for `branch` inside an
/// existing transaction.
///
/// Tags and notes live under the `_system_` branch, so branch deletion must
/// clear them explicitly; deleting the user branch namespace alone is not
/// sufficient. Keeping this cleanup in the same transaction as branch metadata
/// deletion prevents same-name recreate from inheriting stale annotations.
pub(crate) fn delete_annotations_for_branch_in_txn(
    txn: &mut strata_concurrency::TransactionContext,
    branch: &str,
) -> StrataResult<()> {
    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let ns = Arc::new(Namespace::for_branch_space(system_id, "default"));

    for prefix in [tag_prefix(branch), note_prefix(branch)] {
        let prefix_key = Key::new(ns.clone(), TypeTag::KV, prefix.into_bytes());
        let entries = txn.scan_prefix(&prefix_key)?;
        for (key, _) in entries {
            txn.delete(key)?;
        }
    }

    Ok(())
}

// =============================================================================
// Snapshot map builders (ENG-DEBT-001)
// =============================================================================

/// Build an ancestor-style map from versioned entries, tombstone-aware.
///
/// Entries outside `space` are skipped. Tombstones map to `None`.
pub(crate) fn build_ancestor_map(
    entries: &[strata_storage::VersionedEntry],
    space: &str,
) -> HashMap<Vec<u8>, Option<Value>> {
    let mut map = HashMap::new();
    for entry in entries {
        if entry.key.namespace.space == *space {
            if entry.is_tombstone {
                map.insert(entry.key.user_key.to_vec(), None);
            } else {
                map.insert(entry.key.user_key.to_vec(), Some(entry.value.clone()));
            }
        }
    }
    map
}

/// Convert `VersionedEntry` list to `(Key, VersionedValue)` pairs, dropping tombstones.
fn live_entries_from_versioned(
    entries: Vec<strata_storage::VersionedEntry>,
) -> Vec<(Key, VersionedValue)> {
    entries
        .into_iter()
        .filter(|e| !e.is_tombstone)
        .map(|e| {
            let vv = VersionedValue {
                value: e.value,
                version: Version::Txn(e.commit_id.as_u64()),
                timestamp: strata_core::Timestamp::from_micros(0),
            };
            (e.key, vv)
        })
        .collect()
}

/// Build a live-entry map from `(Key, VersionedValue)` pairs, filtered by space.
pub(crate) fn build_live_map(
    entries: &[(Key, VersionedValue)],
    space: &str,
) -> HashMap<Vec<u8>, Value> {
    let mut map = HashMap::new();
    for (key, vv) in entries {
        if key.namespace.space == *space {
            map.insert(key.user_key.to_vec(), vv.value.clone());
        }
    }
    map
}

/// Build a tombstone-aware map keyed by `(space, user_key)` from versioned entries.
///
/// Used by `revert_version_range` where entries span multiple spaces.
fn build_versioned_space_map(
    entries: &[strata_storage::VersionedEntry],
) -> HashMap<(String, Vec<u8>), Option<Value>> {
    let mut map = HashMap::new();
    for entry in entries {
        let key = (
            entry.key.namespace.space.clone(),
            entry.key.user_key.to_vec(),
        );
        if entry.is_tombstone {
            map.insert(key, None);
        } else {
            map.insert(key, Some(entry.value.clone()));
        }
    }
    map
}

/// Build a live-entry map keyed by `(space, user_key)` from `(Key, VersionedValue)` pairs.
fn build_live_space_map(entries: &[(Key, VersionedValue)]) -> HashMap<(String, Vec<u8>), Value> {
    let mut map = HashMap::new();
    for (key, vv) in entries {
        map.insert(
            (key.namespace.space.clone(), key.user_key.to_vec()),
            vv.value.clone(),
        );
    }
    map
}

// =============================================================================
// Event merge safety
// =============================================================================
//
// See docs/design/branching/primitive-aware-merge.md for context. The generic
// three-way merge treats EventLog records as opaque `(space, TypeTag::Event,
// user_key)` triples, which silently corrupts the hash chain and per-type
// index when both sides of a fork have appended to the same space. The
// `check_event_merge_divergence` helper detects this case and refuses the
// merge before any writes happen.
//
// The detection piggybacks on the version-scoped reads the diff already
// performs, so there is no extra storage cost. Single-sided merges and
// merges where the divergence is across different spaces continue through
// the existing generic path unchanged.

/// The fixed user_key bytes used by `EventLog` for its per-(branch, space)
/// metadata row. Keep in sync with `Key::new_event_meta` in core.
const EVENT_META_USER_KEY: &[u8] = b"__meta__";

/// Decode the stored `EventLogMeta` from a raw `Value::String` (the on-disk
/// encoding used by `EventLog`). Returns `Err` on corruption.
fn decode_event_meta(value: &Value) -> StrataResult<crate::primitives::event::EventLogMeta> {
    match value {
        Value::String(s) => serde_json::from_str(s)
            .map_err(|e| StrataError::serialization(format!("corrupt EventLog metadata: {e}"))),
        _ => Err(StrataError::serialization(
            "EventLog metadata row is not a Value::String",
        )),
    }
}

/// Extract the `EventLogMeta` row for `space` from a slice of ancestor-side
/// `VersionedEntry` records (the shape returned by `list_by_type_at_version`).
/// Returns `Ok(None)` when the space has no `__meta__` row yet (i.e. no
/// events have been appended on that side) or when the latest version is a
/// tombstone.
fn extract_event_meta_from_versioned(
    entries: &[strata_storage::VersionedEntry],
    space: &str,
) -> StrataResult<Option<crate::primitives::event::EventLogMeta>> {
    for entry in entries {
        if entry.key.namespace.space == *space
            && entry.key.type_tag == TypeTag::Event
            && entry.key.user_key.as_ref() == EVENT_META_USER_KEY
        {
            if entry.is_tombstone {
                return Ok(None);
            }
            return Ok(Some(decode_event_meta(&entry.value)?));
        }
    }
    Ok(None)
}

/// Extract the `EventLogMeta` row for `space` from a slice of live
/// `(Key, VersionedValue)` pairs (the shape produced by
/// `live_entries_from_versioned`).
fn extract_event_meta_from_live(
    entries: &[(Key, VersionedValue)],
    space: &str,
) -> StrataResult<Option<crate::primitives::event::EventLogMeta>> {
    for (key, vv) in entries {
        if key.namespace.space == *space
            && key.type_tag == TypeTag::Event
            && key.user_key.as_ref() == EVENT_META_USER_KEY
        {
            return Ok(Some(decode_event_meta(&vv.value)?));
        }
    }
    Ok(None)
}

/// Return `Err` if both source and target have appended events to `space`
/// since the merge base. This is the "refuse divergent Event merge" check
/// described in docs/design/branching/primitive-aware-merge.md.
///
/// The rule is per-space: cross-space divergence (e.g. source wrote to
/// "orders" while target wrote to "users") is allowed because each space's
/// EventLogMeta is a separate row and classifies cleanly under the generic
/// three-way merge.
fn check_event_merge_divergence(
    space: &str,
    ancestor_entries: &[strata_storage::VersionedEntry],
    source_entries: &[(Key, VersionedValue)],
    target_entries: &[(Key, VersionedValue)],
) -> StrataResult<()> {
    let ancestor_meta = extract_event_meta_from_versioned(ancestor_entries, space)?;
    let source_meta = extract_event_meta_from_live(source_entries, space)?;
    let target_meta = extract_event_meta_from_live(target_entries, space)?;

    let anc_next = ancestor_meta.as_ref().map(|m| m.next_sequence).unwrap_or(0);
    let anc_head = ancestor_meta
        .as_ref()
        .map(|m| m.head_hash)
        .unwrap_or([0u8; 32]);
    let src_next = source_meta.as_ref().map(|m| m.next_sequence).unwrap_or(0);
    let src_head = source_meta
        .as_ref()
        .map(|m| m.head_hash)
        .unwrap_or([0u8; 32]);
    let tgt_next = target_meta.as_ref().map(|m| m.next_sequence).unwrap_or(0);
    let tgt_head = target_meta
        .as_ref()
        .map(|m| m.head_hash)
        .unwrap_or([0u8; 32]);

    // Primary rule: both sides advanced next_sequence past ancestor.
    let seq_divergent = src_next > anc_next && tgt_next > anc_next;
    // Defense-in-depth: both sides' hash chain heads moved off the ancestor.
    // Catches any theoretical "same count, different content" state that
    // `next_sequence` alone would miss.
    let hash_divergent = src_head != anc_head && tgt_head != anc_head;

    if seq_divergent || hash_divergent {
        return Err(StrataError::invalid_input(format!(
            "merge unsupported: divergent event appends in space '{space}' since fork \
             (ancestor next_sequence={anc_next}, source={src_next}, target={tgt_next}). \
             Event log merge with concurrent appends on both sides of the fork is not \
             yet supported; see docs/design/branching/primitive-aware-merge.md."
        )));
    }

    Ok(())
}

// =============================================================================
// Graph merge safety: tactical divergence refusal
// =============================================================================
//
// The graph crate ships a real semantic merge that handles divergent
// branches correctly (decoded edge diffing, additive merging of disjoint
// edges, referential integrity validation). Production builds register it
// via `db.merge_registry().register_graph()` in `GraphSubsystem::initialize()`.
// The fallback in this file is the tactical "refuse any divergent graph
// merge" rule, used only by engine unit tests that don't load the graph crate.
//
// Without the semantic merge, the generic three-way merge would treat
// `(space, TypeTag::Graph)` cells as opaque KV, producing two distinct
// corruption modes when both branches modify graph state since the merge
// base:
//
//   1. **Bidirectional adjacency inconsistency under LWW.** Edges live as
//      packed binary in two physical locations: `{graph}/fwd/{src}` and
//      `{graph}/rev/{dst}`. The generic LWW path treats those as
//      independent KV keys. If both branches add an outgoing edge from the
//      same source node, LWW picks one whole `fwd/X` list and silently
//      drops the other side's edges in the forward direction — but the
//      losing side's `rev/{dst}` entries (which the winner never touched)
//      survive, leaving `incoming_neighbors` and `outgoing_neighbors`
//      disagreeing about edge existence.
//
//   2. **Concurrent node delete + edge add.** Source deletes node X
//      (drops `n/X`, `fwd/X`, and updates every `rev/*` that referenced X).
//      Target adds an edge X→Y. Generic merge resolves `n/X` and `fwd/X` to
//      the source side, but target's freshly-added `rev/Y` entry is
//      classified `TargetAdded` and survives — leaving a dangling reference
//      to the now-deleted node X.
//
// The fallback closes both with a tactical refusal: if BOTH source and
// target have any graph writes since the merge base, we abort the merge
// before any classification or write happens. Single-sided graph merges
// (only one branch touched graph state) continue working unchanged
// because each edge addition writes both `fwd/{src}` and `rev/{dst}`
// together as one transactional unit, so the generic merge transports
// them as a coherent group.

/// Returns `true` if `side` differs from `ancestor` at any key. Used by
/// `check_graph_merge_divergence` to detect "this branch made any graph
/// modification since the merge base."
///
/// `ancestor` is the tombstone-aware view (`Some(None)` = tombstone),
/// matching the shape produced by `build_ancestor_map`. `side` is the
/// live-only view (no tombstones), matching `build_live_map`.
///
/// A side is "modified" iff:
///   - it has a key whose value differs from ancestor's live value,
///   - it has a key that ancestor either lacked or had as a tombstone, OR
///   - it lacks a key that ancestor had live (i.e. the side deleted it).
fn graph_side_modified(
    ancestor: &HashMap<Vec<u8>, Option<Value>>,
    side: &HashMap<Vec<u8>, Value>,
) -> bool {
    // Side adds, replaces, or modifies relative to ancestor.
    for (k, v) in side {
        match ancestor.get(k) {
            // Ancestor never had this key, or had a tombstone here.
            None => return true,
            Some(None) => return true,
            // Ancestor had a live value; modified iff different.
            Some(Some(av)) if av != v => return true,
            _ => {}
        }
    }
    // Side deleted a key the ancestor had live.
    for (k, av) in ancestor {
        if av.is_some() && !side.contains_key(k) {
            return true;
        }
    }
    false
}

/// Return `Err` if both source and target have made any modifications to
/// graph data in `space` since the merge base. This is the
/// "refuse divergent Graph merge" check described in
/// docs/design/branching/primitive-aware-merge.md.
///
/// The rule is per-space: cross-space divergence (source touched space A,
/// target touched space B) is allowed because each space's graph keys
/// classify cleanly under the generic three-way merge.
///
/// This helper is only reachable from `GraphMergeHandler::precheck`'s
/// fallback when no semantic merge is registered (engine-only unit
/// tests). In production the graph crate's semantic merge runs instead.
///
/// Single-sided graph merges (only one of source/target has graph writes
/// since the merge base) intentionally pass through this check unchanged.
fn check_graph_merge_divergence(
    space: &str,
    ancestor_entries: &[strata_storage::VersionedEntry],
    source_entries: &[(Key, VersionedValue)],
    target_entries: &[(Key, VersionedValue)],
) -> StrataResult<()> {
    let ancestor_map = build_ancestor_map(ancestor_entries, space);
    let source_map = build_live_map(source_entries, space);
    let target_map = build_live_map(target_entries, space);

    let source_modified = graph_side_modified(&ancestor_map, &source_map);
    let target_modified = graph_side_modified(&ancestor_map, &target_map);

    if source_modified && target_modified {
        return Err(StrataError::invalid_input(format!(
            "merge unsupported: divergent graph writes in space '{space}' since fork. \
             Graph branch merge with concurrent writes on both sides of the fork is \
             not semantically safe under the fallback divergence-refusal path — \
             single-sided graph merges still work. \
             See docs/design/branching/primitive-aware-merge.md for the full design \
             and the semantic graph merge that handles this case."
        )));
    }

    Ok(())
}

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
    /// Storage-level type tag
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
    /// Number of keys deleted on target (via tombstone)
    #[serde(default)]
    pub keys_deleted: u64,
    /// Conflicts encountered (empty for LWW, populated for Strict failures)
    pub conflicts: Vec<ConflictEntry>,
    /// Number of spaces merged
    pub spaces_merged: u64,
    /// MVCC version of the merge transaction (used as merge base for subsequent merges)
    #[serde(default)]
    pub merge_version: Option<u64>,
}

/// Result of three-way diff for a single key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreeWayDiffEntry {
    /// User key (UTF-8 or hex-encoded for binary keys)
    pub key: String,
    /// Raw user key bytes (for programmatic access, preserves binary keys)
    pub raw_key: Vec<u8>,
    /// Space this entry belongs to
    pub space: String,
    /// Primitive type of this entry
    pub primitive: PrimitiveType,
    /// Storage-level type tag
    pub type_tag: TypeTag,
    /// Classification of the change
    pub change: ThreeWayChange,
}

/// Full three-way diff result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreeWayDiffResult {
    /// Source branch name
    pub source: String,
    /// Target branch name
    pub target: String,
    /// Merge base branch name
    pub merge_base_branch: String,
    /// Merge base version
    pub merge_base_version: CommitVersion,
    /// Entries that differ (excludes Unchanged)
    pub entries: Vec<ThreeWayDiffEntry>,
}

/// Merge base information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeBaseInfo {
    /// Branch name at the merge base
    pub branch: String,
    /// MVCC version at the merge base
    pub version: CommitVersion,
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
// Diff setup helpers (ENG-DEBT-004)
// =============================================================================

/// Shared space-listing and filtering for diff operations.
///
/// Returns `(spaces_only_in_a, spaces_only_in_b, all_spaces)`, all filtered.
fn compute_space_sets(
    spaces_a: &HashSet<String>,
    spaces_b: &HashSet<String>,
    space_filter: &Option<HashSet<String>>,
) -> (Vec<String>, Vec<String>, HashSet<String>) {
    let mut only_a: Vec<String> = spaces_a.difference(spaces_b).cloned().collect();
    let mut only_b: Vec<String> = spaces_b.difference(spaces_a).cloned().collect();
    let mut all: HashSet<String> = spaces_a.union(spaces_b).cloned().collect();
    if let Some(ref allowed) = space_filter {
        only_a.retain(|s| allowed.contains(s));
        only_b.retain(|s| allowed.contains(s));
        all.retain(|s| allowed.contains(s));
    }
    (only_a, only_b, all)
}

/// Extract the space filter from DiffOptions.
fn extract_space_filter(options: &DiffOptions) -> Option<HashSet<String>> {
    options
        .filter
        .as_ref()
        .and_then(|f| f.spaces.as_ref())
        .map(|s| s.iter().cloned().collect())
}

/// Determine which TypeTags to scan based on the filter.
fn resolve_type_tags(options: &DiffOptions) -> Vec<TypeTag> {
    match options.filter.as_ref().and_then(|f| f.primitives.as_ref()) {
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
    }
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
        PrimitiveType::Vector => vec![TypeTag::Vector],
        PrimitiveType::Branch => vec![], // Branch metadata is not scanned in diffs
        PrimitiveType::Graph => vec![TypeTag::Graph],
    }
}

/// Map a TypeTag to the corresponding PrimitiveType.
fn type_tag_to_primitive(tag: TypeTag) -> PrimitiveType {
    match tag {
        TypeTag::KV => PrimitiveType::Kv,
        TypeTag::Event => PrimitiveType::Event,
        TypeTag::Json => PrimitiveType::Json,
        TypeTag::Vector => PrimitiveType::Vector,
        TypeTag::Graph => PrimitiveType::Graph,
        _ => PrimitiveType::Kv, // fallback for Branch/Space metadata tags
    }
}

/// Decide whether a space name belongs in the user-visible `SpaceIndex`.
///
/// Internal namespaces used by primitive secondary indexes (e.g.
/// `_idx/{collection_space}/{index_name}` for JSON secondary index
/// entries, `_idx_meta/{collection_space}` for JSON index metadata)
/// are written through normal `Key::new(...)` plumbing during a merge,
/// so they would otherwise show up as MergeAction.space values and get
/// auto-registered with `SpaceIndex` like any user space. They are
/// implementation details that should not pollute `db.list_spaces()` /
/// `SpaceIndex::list(...)`.
///
/// Used by `merge_branches` and `cherry_pick_from_diff` to filter the
/// `spaces_touched` set before calling `space_index.register`. The
/// underlying KV writes still happen — only the SpaceIndex bookkeeping
/// is suppressed.
pub(crate) fn is_user_visible_space(space: &str) -> bool {
    !(space.starts_with("_idx/") || space.starts_with("_idx_meta/"))
}

/// Format a user key as a readable string (UTF-8 or hex for binary).
pub(crate) fn format_user_key(user_key: &[u8]) -> String {
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

/// Fork a branch via O(1) COW (copy-on-write), recording `message` and
/// `creator` on the resulting branch DAG fork event.
///
/// Creates a new branch with `destination` name and shares the source's
/// segments via inherited layers. No data is copied — writes to the
/// destination are isolated via the multi-layer read path.
///
/// Requires a disk-backed database. Ephemeral (in-memory) databases
/// return an error.
///
/// `dest_gen` is preallocated by the caller (see
/// [`resolve_fork_generation`]). The source `BranchRef` is resolved
/// inside fork's quiesce guard so the control-store fork anchor points
/// at the same lifecycle instance whose storage snapshot is being
/// forked. Storage-fork-first ordering is preserved: the storage fork
/// commits before the KV txn, so a crash between them leaves harmless
/// orphan storage (refcounts rebuild from manifests on recovery).
///
/// Canonical entry point: [`crate::database::BranchService::fork`] /
/// [`crate::database::BranchService::fork_with_options`]. This helper is
/// `pub(crate)` and must not be called from outside the engine crate.
///
/// # Errors
///
/// - Source branch does not exist
/// - Destination branch already exists
/// - Database is ephemeral (no segments directory)
pub(crate) fn fork_branch_with_metadata(
    db: &Arc<Database>,
    source: &str,
    destination: &str,
    message: Option<&str>,
    creator: Option<&str>,
    dest_gen: BranchGeneration,
) -> StrataResult<ForkInfo> {
    let branch_index = BranchIndex::new(db.clone());
    let space_index = SpaceIndex::new(db.clone());
    let control_store = BranchControlStore::new(db.clone());

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

    // 3b. Reject fork if source is being deleted (#2108).
    if db.is_branch_deleting(&source_id) {
        return Err(StrataError::invalid_input(format!(
            "Source branch '{}' is being deleted",
            source
        )));
    }

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

    // Quiesce in-flight commits before forking (#2105/#2110, #2108).
    //
    // The quiesce write guard drains ALL in-flight commits (they hold the
    // shared read side of commit_quiesce) and prevents new ones from starting.
    // This protects against two distinct bugs:
    //
    //   1. Version gap (#2105/#2110): A concurrent commit to any branch can
    //      advance the global storage version while the source branch's
    //      pending writes haven't landed. The quiesce guard ensures all
    //      pending writes are applied before we read the version.
    //
    //   2. Delete tombstone capture (#2108): delete_branch's transaction
    //      writes tombstones to the source's storage inside commit(), which
    //      holds quiesce.read(). The write guard here waits for that commit
    //      to finish, so fork either sees the branch already marked as
    //      deleting (and rejects) or the delete hasn't started yet.
    //
    // Re-check is_deleting under the quiesce guard to close the TOCTOU
    // window — if delete's transaction committed before we acquired the
    // write lock, the deleting flag is set and we bail.
    //
    // Note: we intentionally do NOT hold the branch commit lock here.
    // commit() acquires quiesce.read() → branch_commit_lock, so nesting
    // branch_commit_lock inside quiesce.write() would invert the lock order
    // and deadlock with concurrent commits on the source branch.
    let (parent_ref, fork_version, _segments_shared) = {
        let _quiesce_guard = db.quiesce_commits();
        if db.is_branch_deleting(&source_id) {
            return Err(StrataError::invalid_input(format!(
                "Source branch '{}' is being deleted",
                source
            )));
        }
        let parent_ref = match control_store.find_active_by_name(source)? {
            Some(rec) => rec.branch,
            None => BranchRef::new(source_id, 0),
        };
        let (fork_version, shared) = storage
            .fork_branch(&source_id, &dest_id)
            .map_err(|e| StrataError::storage(format!("fork failed: {}", e)))?;
        (parent_ref, fork_version, shared)
    };

    // 5. Create destination branch in KV metadata (WAL-protected) AND
    //    write the new BranchControlRecord atomically in the same
    //    transaction (B3.2). If either fails, rollback the storage fork
    //    so the visibility invariant holds (AD6).
    let child_ref = BranchRef::new(dest_id, dest_gen);
    let control_record = BranchControlRecord {
        branch: child_ref,
        name: destination.to_string(),
        lifecycle: BranchLifecycleStatus::Active,
        fork: Some(ForkAnchor {
            parent: parent_ref,
            point: fork_version,
        }),
    };
    let kv_result = branch_index.create_branch_with_hook(destination, |txn| {
        control_store.put_record(&control_record, txn)
    });
    if let Err(e) = kv_result {
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
        fork_version = fork_version.as_u64(),
        parent_generation = parent_ref.generation,
        dest_generation = dest_gen,
        "Branch forked (COW)"
    );

    let info = ForkInfo {
        source: source.to_string(),
        destination: destination.to_string(),
        keys_copied: 0,
        spaces_copied,
        fork_version: Some(fork_version.as_u64()),
    };

    dispatch_fork_hook(db, &info, message, creator);

    Ok(info)
}

/// Allocate the child's next generation for a fork.
///
/// The source `BranchRef` is resolved later, under fork's quiesce guard,
/// so the fork anchor is derived from the exact lifecycle instance whose
/// storage snapshot is being copied.
///
/// Checks the destination up-front before bumping the counter so a
/// trivially-failing fork (duplicate destination) does not leak a
/// generation from the monotonic counter.
pub(crate) fn resolve_fork_generation(
    db: &Arc<Database>,
    destination: &str,
) -> StrataResult<BranchGeneration> {
    let store = BranchControlStore::new(db.clone());
    let branch_index = BranchIndex::new(db.clone());
    if branch_index.exists(destination)? || store.find_active_by_name(destination)?.is_some() {
        return Err(StrataError::invalid_input(format!(
            "Destination branch '{}' already exists",
            destination
        )));
    }
    let dest_id = resolve_branch_name(destination);
    let dest_gen = db.transaction(BranchId::from_bytes([0u8; 16]), |txn| {
        store.next_generation(dest_id, txn)
    })?;
    Ok(dest_gen)
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
    // 1. Verify both branches exist and resolve IDs
    let id_a = resolve_and_verify(db, branch_a)?;
    let id_b = resolve_and_verify(db, branch_b)?;

    let storage = db.storage();

    // Capture snapshot version for consistent reads (#1920)
    let snapshot_version = db.current_version();

    // COW fast path: if one branch is a direct child of the other and no
    // as_of timestamp, use O(W) diff instead of O(N) full scan.
    // Checked early to avoid the O(spaces) space listing below.
    if options.as_of.is_none() {
        if let Some((parent, fv)) = storage.get_fork_info(&id_b) {
            if parent == id_a {
                return cow_diff_branches(
                    db,
                    id_a,
                    id_b,
                    id_b,
                    id_a,
                    fv,
                    branch_a,
                    branch_b,
                    &options,
                    snapshot_version,
                );
            }
        }
        if let Some((parent, fv)) = storage.get_fork_info(&id_a) {
            if parent == id_b {
                return cow_diff_branches(
                    db,
                    id_a,
                    id_b,
                    id_a,
                    id_b,
                    fv,
                    branch_a,
                    branch_b,
                    &options,
                    snapshot_version,
                );
            }
        }
    }

    // 2. List spaces in both branches, apply filters
    let space_index = SpaceIndex::new(db.clone());
    let spaces_a: HashSet<String> = space_index.list(id_a)?.into_iter().collect();
    let spaces_b: HashSet<String> = space_index.list(id_b)?.into_iter().collect();
    let space_filter = extract_space_filter(&options);
    let (spaces_only_in_a, spaces_only_in_b, all_spaces) =
        compute_space_sets(&spaces_a, &spaces_b, &space_filter);
    let type_tags = resolve_type_tags(&options);

    let mut space_diffs = Vec::new();
    let mut total_added = 0usize;
    let mut total_removed = 0usize;
    let mut total_modified = 0usize;

    // 3. Scan data once per type tag, grouped by space
    let mut maps_a: HashMap<String, HashMap<(Vec<u8>, TypeTag), Value>> = HashMap::new();
    let mut maps_b: HashMap<String, HashMap<(Vec<u8>, TypeTag), Value>> = HashMap::new();

    for type_tag in &type_tags {
        let entries_a: Vec<(Key, VersionedValue)> = match options.as_of {
            Some(ts) => storage.list_by_type_at_timestamp(&id_a, *type_tag, ts),
            None => live_entries_from_versioned(storage.list_by_type_at_version(
                &id_a,
                *type_tag,
                snapshot_version,
            )),
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

        let entries_b: Vec<(Key, VersionedValue)> = match options.as_of {
            Some(ts) => storage.list_by_type_at_timestamp(&id_b, *type_tag, ts),
            None => live_entries_from_versioned(storage.list_by_type_at_version(
                &id_b,
                *type_tag,
                snapshot_version,
            )),
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
        let diff = classify_two_way_space(space, map_a, map_b);

        total_added += diff.added.len();
        total_removed += diff.removed.len();
        total_modified += diff.modified.len();

        if !diff.added.is_empty() || !diff.removed.is_empty() || !diff.modified.is_empty() {
            space_diffs.push(diff);
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
// COW-Aware Diff (O(W) fast path for parent-child branches)
// =============================================================================

/// COW-aware diff: O(W) where W = writes since fork.
///
/// Only called when one branch is a direct child of the other (detected via
/// `get_fork_info`) and no `as_of` timestamp is specified.
///
/// Algorithm:
/// 1. Scan the child's own entries (memtable + own segments, no inherited layers).
/// 2. Scan the parent's entries written after fork_version.
/// 3. For each changed key, point-lookup both branches for the current visible value.
/// 4. Classify as added/removed/modified using the same logic as the full-scan diff.
#[allow(clippy::too_many_arguments)]
fn cow_diff_branches(
    db: &Arc<Database>,
    id_a: BranchId,
    id_b: BranchId,
    child_id: BranchId,
    parent_id: BranchId,
    fork_version: CommitVersion,
    branch_a_name: &str,
    branch_b_name: &str,
    options: &DiffOptions,
    snapshot_version: CommitVersion,
) -> StrataResult<BranchDiffResult> {
    use strata_core::Storage;
    let space_index = SpaceIndex::new(db.clone());
    let storage = db.storage();

    // 1. Space listing and filtering (shared with full-scan path)
    let spaces_a: HashSet<String> = space_index.list(id_a)?.into_iter().collect();
    let spaces_b: HashSet<String> = space_index.list(id_b)?.into_iter().collect();
    let space_filter = extract_space_filter(options);
    let (spaces_only_in_a, spaces_only_in_b, _all_spaces) =
        compute_space_sets(&spaces_a, &spaces_b, &space_filter);

    // 2. Determine which TypeTags to scan
    let type_tags: HashSet<TypeTag> = resolve_type_tags(options).into_iter().collect();

    // 3. Scan only changed entries (child's own + parent's post-fork)
    let child_own = storage.list_own_entries(&child_id, None);
    let parent_post_fork = storage.list_own_entries(&parent_id, Some(fork_version));

    // 4. Build the set of potentially changed keys, applying filters
    let mut changed_keys: HashSet<(String, Vec<u8>, TypeTag)> = HashSet::new();

    for entry in child_own.iter().chain(parent_post_fork.iter()) {
        if !type_tags.contains(&entry.key.type_tag) {
            continue;
        }
        if let Some(ref allowed) = space_filter {
            if !allowed.contains(&entry.key.namespace.space) {
                continue;
            }
        }
        changed_keys.insert((
            entry.key.namespace.space.clone(),
            entry.key.user_key.to_vec(),
            entry.key.type_tag,
        ));
    }

    // 5. Point-lookup both branches for each changed key and classify
    // (added, removed, modified) per space
    type SpaceBuckets = (
        Vec<BranchDiffEntry>,
        Vec<BranchDiffEntry>,
        Vec<BranchDiffEntry>,
    );
    let mut space_diffs_map: HashMap<String, SpaceBuckets> = HashMap::new();

    // Cache Arc<Namespace> per (branch_id, space) to avoid repeated allocation
    let mut ns_cache: HashMap<(BranchId, String), Arc<Namespace>> = HashMap::new();

    for (space, user_key, type_tag) in &changed_keys {
        let ns_a = ns_cache
            .entry((id_a, space.clone()))
            .or_insert_with(|| Arc::new(Namespace::for_branch_space(id_a, space)))
            .clone();
        let key_a = Key::new(ns_a, *type_tag, user_key.clone());
        let val_a = storage
            .get_versioned(&key_a, snapshot_version)?
            .map(|vv| vv.value);

        let ns_b = ns_cache
            .entry((id_b, space.clone()))
            .or_insert_with(|| Arc::new(Namespace::for_branch_space(id_b, space)))
            .clone();
        let key_b = Key::new(ns_b, *type_tag, user_key.clone());
        let val_b = storage
            .get_versioned(&key_b, snapshot_version)?
            .map(|vv| vv.value);

        let diff_entry = match (&val_a, &val_b) {
            (None, None) => continue,
            (Some(a), Some(b)) if a == b => continue,
            _ => BranchDiffEntry {
                key: format_user_key(user_key),
                raw_key: user_key.clone(),
                primitive: type_tag_to_primitive(*type_tag),
                type_tag: *type_tag,
                space: space.clone(),
                value_a: val_a.clone(),
                value_b: val_b.clone(),
            },
        };

        let (added, removed, modified) = space_diffs_map.entry(space.clone()).or_default();

        match (&diff_entry.value_a, &diff_entry.value_b) {
            (Some(_), None) => removed.push(diff_entry),
            (None, Some(_)) => added.push(diff_entry),
            (Some(_), Some(_)) => modified.push(diff_entry),
            _ => unreachable!(),
        }
    }

    // 6. Assemble result
    let mut total_added = 0usize;
    let mut total_removed = 0usize;
    let mut total_modified = 0usize;
    let mut space_diffs = Vec::new();

    for (space, (added, removed, modified)) in space_diffs_map {
        total_added += added.len();
        total_removed += removed.len();
        total_modified += modified.len();

        if !added.is_empty() || !removed.is_empty() || !modified.is_empty() {
            space_diffs.push(SpaceDiff {
                space,
                added,
                removed,
                modified,
            });
        }
    }

    Ok(BranchDiffResult {
        branch_a: branch_a_name.to_string(),
        branch_b: branch_b_name.to_string(),
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
// Three-Way Merge Infrastructure
// =============================================================================

/// Common ancestor state for three-way merge.
///
/// `pub` (re-exported from `strata_engine`) so primitive crates that
/// implement graph plan callbacks can borrow it through `MergePlanCtx`.
#[derive(Debug, Clone)]
pub struct MergeBase {
    /// Branch to read ancestor state from. For COW-aware merges, this
    /// is the child branch even when merging the parent into the child;
    /// reads at `version` return the inherited (pre-fork) view.
    pub branch_id: BranchId,
    /// MVCC version to read ancestor state at.
    pub version: CommitVersion,
}

/// Classification of a key in a three-way merge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ThreeWayChange {
    /// Key unchanged on both sides. No action.
    Unchanged,
    /// Key only changed on source side. Apply source value to target.
    SourceChanged {
        /// The source branch's new value.
        value: Value,
    },
    /// Key only changed on target side. No action.
    TargetChanged,
    /// Key changed on both sides to the same value. No action.
    BothChangedSame,
    /// Key changed on both sides to different values. Conflict.
    Conflict {
        /// Value on the source branch.
        source_value: Value,
        /// Value on the target branch.
        target_value: Value,
    },
    /// Key added on source only. Apply.
    SourceAdded {
        /// The source branch's value.
        value: Value,
    },
    /// Key added on target only. No action.
    TargetAdded,
    /// Key added on both sides with same value. No action.
    BothAddedSame,
    /// Key added on both sides with different values. Conflict.
    BothAddedDifferent {
        /// Value on the source branch.
        source_value: Value,
        /// Value on the target branch.
        target_value: Value,
    },
    /// Key deleted on source, unchanged on target. Delete on target.
    SourceDeleted,
    /// Key deleted on target, unchanged on source. No action.
    TargetDeleted,
    /// Key deleted on both sides. No action.
    BothDeleted,
    /// Key deleted on source, modified on target. Conflict.
    DeleteModifyConflict {
        /// Value on the target branch (source deleted it).
        target_value: Value,
    },
    /// Key modified on source, deleted on target. Conflict.
    ModifyDeleteConflict {
        /// Value on the source branch (target deleted it).
        source_value: Value,
    },
}

/// Compare two branch maps for a single space and classify entries as added/removed/modified.
fn classify_two_way_space(
    space: &str,
    map_a: HashMap<(Vec<u8>, TypeTag), Value>,
    map_b: HashMap<(Vec<u8>, TypeTag), Value>,
) -> SpaceDiff {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut modified = Vec::new();

    for ((user_key, tag), val_a) in &map_a {
        let key_str = format_user_key(user_key);
        let primitive = type_tag_to_primitive(*tag);
        match map_b.get(&(user_key.clone(), *tag)) {
            None => removed.push(BranchDiffEntry {
                key: key_str,
                raw_key: user_key.clone(),
                primitive,
                type_tag: *tag,
                space: space.to_string(),
                value_a: Some(val_a.clone()),
                value_b: None,
            }),
            Some(val_b) if val_a != val_b => modified.push(BranchDiffEntry {
                key: key_str,
                raw_key: user_key.clone(),
                primitive,
                type_tag: *tag,
                space: space.to_string(),
                value_a: Some(val_a.clone()),
                value_b: Some(val_b.clone()),
            }),
            _ => {}
        }
    }
    for ((user_key, tag), val_b) in &map_b {
        if !map_a.contains_key(&(user_key.clone(), *tag)) {
            added.push(BranchDiffEntry {
                key: format_user_key(user_key),
                raw_key: user_key.clone(),
                primitive: type_tag_to_primitive(*tag),
                type_tag: *tag,
                space: space.to_string(),
                value_a: None,
                value_b: Some(val_b.clone()),
            });
        }
    }

    SpaceDiff {
        space: space.to_string(),
        added,
        removed,
        modified,
    }
}

/// Classify a single key's change across ancestor, source, and target.
///
/// Implements the 14-case decision matrix used by git, Dolt, and lakeFS:
/// - `ancestor`: None = key absent (or tombstoned) at ancestor
/// - `source`: None = key absent in current source (live listing)
/// - `target`: None = key absent in current target (live listing)
fn classify_change(
    ancestor: Option<&Value>,
    source: Option<&Value>,
    target: Option<&Value>,
) -> ThreeWayChange {
    match (ancestor, source, target) {
        // All absent
        (None, None, None) => ThreeWayChange::Unchanged,

        // All present and equal
        (Some(a), Some(s), Some(t)) if a == s && s == t => ThreeWayChange::Unchanged,

        // Only source changed
        (Some(a), Some(s), Some(t)) if a == t && a != s => {
            ThreeWayChange::SourceChanged { value: s.clone() }
        }
        // Only target changed
        (Some(a), Some(s), Some(t)) if a == s && a != t => ThreeWayChange::TargetChanged,
        // Both changed to same value
        (Some(a), Some(s), Some(t)) if a != s && s == t => ThreeWayChange::BothChangedSame,
        // Both changed to different values → conflict
        (Some(_), Some(s), Some(t)) => ThreeWayChange::Conflict {
            source_value: s.clone(),
            target_value: t.clone(),
        },

        // Source added (not in ancestor or target)
        (None, Some(s), None) => ThreeWayChange::SourceAdded { value: s.clone() },
        // Target added (not in ancestor or source)
        (None, None, Some(_)) => ThreeWayChange::TargetAdded,
        // Both added same value
        (None, Some(s), Some(t)) if s == t => ThreeWayChange::BothAddedSame,
        // Both added different values → conflict
        (None, Some(s), Some(t)) => ThreeWayChange::BothAddedDifferent {
            source_value: s.clone(),
            target_value: t.clone(),
        },

        // Source deleted, target unchanged
        (Some(a), None, Some(t)) if a == t => ThreeWayChange::SourceDeleted,
        // Source deleted, target modified → conflict
        (Some(_), None, Some(t)) => ThreeWayChange::DeleteModifyConflict {
            target_value: t.clone(),
        },
        // Target deleted, source unchanged
        (Some(a), Some(s), None) if a == s => ThreeWayChange::TargetDeleted,
        // Source modified, target deleted → conflict
        (Some(_), Some(s), None) => ThreeWayChange::ModifyDeleteConflict {
            source_value: s.clone(),
        },
        // Both deleted
        (Some(_), None, None) => ThreeWayChange::BothDeleted,
    }
}

/// A merge action to apply to the target branch.
///
/// `pub` (re-exported from `strata_engine`) so primitive crates that
/// register graph plan callbacks can construct one directly — the graph
/// crate's semantic merge produces these from inside its plan function.
pub struct MergeAction {
    /// Space name within the branch.
    pub space: String,
    /// Raw user-key bytes (the format depends on `type_tag`).
    pub raw_key: Vec<u8>,
    /// Storage type tag identifying which primitive owns this key.
    pub type_tag: TypeTag,
    /// What to do at the key — `Put(value)` or `Delete`.
    pub action: MergeActionKind,
    /// What the diff saw in the target at snapshot time.
    /// `None` means the key did not exist in the target.
    /// Used to validate target hasn't changed between diff and apply (#1917).
    pub expected_target: Option<Value>,
}

/// Whether a `MergeAction` writes a value or deletes an existing key.
pub enum MergeActionKind {
    /// Write `value` to the action's key.
    Put(Value),
    /// Delete the action's key.
    Delete,
}

/// Compute the merge base between two branches.
///
/// B3.3 cutover: delegates to [`BranchControlStore::find_merge_base`].
/// The store's lineage edges are the single authority — no caller
/// override, no storage-fork fallback, no DAG fallback. This matches
/// the [`crate::database::BranchService::merge_base`] inspection path so
/// both `merge` apply and `merge_base` inspection read from the same
/// lineage (see B3.3 done-when in
/// `docs/design/branching/b3-phasing-plan.md`).
fn compute_merge_base_for_refs(
    db: &Arc<Database>,
    source_ref: BranchRef,
    target_ref: BranchRef,
) -> StrataResult<Option<MergeBase>> {
    let Some(point) = compute_merge_base_point_for_refs(db, source_ref, target_ref)? else {
        return Ok(None);
    };

    Ok(Some(MergeBase {
        branch_id: point.branch.id,
        version: point.commit_version,
    }))
}

fn compute_merge_base_point_for_refs(
    db: &Arc<Database>,
    source_ref: BranchRef,
    target_ref: BranchRef,
) -> StrataResult<Option<MergeBasePoint>> {
    let store = BranchControlStore::new(db.clone());
    store.find_merge_base(source_ref, target_ref)
}

fn verify_expected_active_ref(
    db: &Arc<Database>,
    branch_name: &str,
    expected: BranchRef,
) -> StrataResult<()> {
    let store = BranchControlStore::new(db.clone());
    match store.active_generation_for_id(expected.id)? {
        Some(current_generation) if current_generation == expected.generation => Ok(()),
        Some(current_generation) => Err(StrataError::conflict(format!(
            "branch '{}' lifecycle advanced from generation {} to {} during operation",
            branch_name, expected.generation, current_generation
        ))),
        None => Err(StrataError::conflict(format!(
            "branch '{}' lifecycle no longer has active generation {} during operation",
            branch_name, expected.generation
        ))),
    }
}

fn resolve_and_verify_with_expected(
    db: &Arc<Database>,
    name: &str,
    expected: Option<BranchRef>,
) -> StrataResult<BranchId> {
    if let Some(expected) = expected {
        verify_expected_active_ref(db, name, expected)?;
        return Ok(expected.id);
    }
    resolve_and_verify(db, name)
}

fn verify_expected_active_ref_in_txn(
    txn: &mut strata_concurrency::TransactionContext,
    branch_name: &str,
    expected: BranchRef,
) -> StrataResult<()> {
    let key = active_ptr_key(expected.id);
    txn.set_allow_cross_branch(true);
    let current = txn.get(&key)?;
    txn.set_allow_cross_branch(false);

    let current_generation = match current {
        Some(Value::Int(v)) if v >= 0 => v as u64,
        Some(other) => {
            return Err(StrataError::corruption(format!(
                "branch '{}' lifecycle guard stored invalid active generation value: {:?}",
                branch_name, other
            )));
        }
        None => {
            return Err(StrataError::conflict(format!(
                "branch '{}' lifecycle no longer has active generation {} during operation",
                branch_name, expected.generation
            )));
        }
    };

    if current_generation != expected.generation {
        return Err(StrataError::conflict(format!(
            "branch '{}' lifecycle advanced from generation {} to {} during operation",
            branch_name, expected.generation, current_generation
        )));
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct MergeExecutionResult {
    pub info: MergeInfo,
    pub merge_base_used: MergeBasePoint,
}

fn compute_merge_base(
    db: &Arc<Database>,
    source_id: BranchId,
    target_id: BranchId,
) -> StrataResult<Option<MergeBase>> {
    let store = BranchControlStore::new(db.clone());
    let Some(source_gen) = store.active_generation_for_id(source_id)? else {
        return Ok(None);
    };
    let Some(target_gen) = store.active_generation_for_id(target_id)? else {
        return Ok(None);
    };
    let source_ref = BranchRef::new(source_id, source_gen);
    let target_ref = BranchRef::new(target_id, target_gen);

    // Apply-path reads are keyed by `BranchId` + version via
    // `list_by_type_at_version`. The `BranchRef` generation is preserved
    // on the stored edge and surfaced by `BranchControlStore` queries;
    // here we collapse back to a plain `MergeBase` because the ancestor
    // read path does not need generation disambiguation (same-name
    // recreate yields a new id-equivalent generation, and storage reads
    // are keyed by `BranchId` regardless).
    compute_merge_base_for_refs(db, source_ref, target_ref)
}

/// Per-(space, type_tag) entry slices gathered for a three-way merge.
///
/// Produced by [`gather_typed_entries`] and consumed by
/// [`classify_typed_entries_for_tag`] (which produces the flat action vector
/// for one type tag, used by the default `PrimitiveMergeHandler::plan` impl)
/// and `PrimitiveMergeHandler::precheck` (which inspects the slices to
/// validate per-primitive invariants — e.g. Event divergence). Splitting
/// gather from classify lets handlers run their precheck on the same
/// materialized snapshots the classifier uses, with no extra storage reads.
///
/// Each `TypedEntryCell` is **pre-filtered to its own space**, so the
/// total memory held by a `TypedEntries` instance is bounded by the actual
/// merge surface (sum of per-space slices), not by `n_spaces × full_branch_data`.
/// `BTreeMap` for deterministic iteration order.
///
/// `pub` (re-exported from `strata_engine`) so primitive crates implementing
/// graph plan callbacks can iterate cells from inside their function.
pub struct TypedEntries {
    /// Per-(space, type_tag) entry slices, keyed by `(space_name, type_tag)`.
    /// Sorted via `BTreeMap` for deterministic iteration order.
    pub cells: BTreeMap<(String, TypeTag), TypedEntryCell>,
}

/// Ancestor / source / target entry slices for one (space, type_tag) cell.
///
/// Entries are pre-filtered to the cell's `space` at gather time so cells
/// don't redundantly hold cross-space data. Downstream filters (e.g. in
/// `build_ancestor_map`) become no-ops on these slices, but they're kept
/// in place to preserve the contract of those helpers for other callers.
pub struct TypedEntryCell {
    /// Ancestor state at `merge_base.version`, with tombstones,
    /// pre-filtered to the cell's space.
    pub ancestor: Vec<strata_storage::VersionedEntry>,
    /// Source state at `snapshot_version`, live entries only,
    /// pre-filtered to the cell's space.
    pub source: Vec<(Key, VersionedValue)>,
    /// Target state at `snapshot_version`, live entries only,
    /// pre-filtered to the cell's space.
    pub target: Vec<(Key, VersionedValue)>,
}

/// Read ancestor / source / target entry slices for every (space, type_tag)
/// cell that this merge will touch.
///
/// All reads are version-scoped (`list_by_type_at_version`) for consistent
/// point-in-time snapshots, matching the original `three_way_diff` reads.
/// Per-(branch, type_tag) reads are issued exactly once and the resulting
/// rows are partitioned across the per-space cells in a single pass — both
/// fewer storage calls and bounded memory regardless of how many spaces
/// the merge touches. Each cell ends up holding only the rows whose
/// `namespace.space` matches the cell key.
pub(crate) fn gather_typed_entries(
    db: &Arc<Database>,
    source_id: BranchId,
    target_id: BranchId,
    merge_base: &MergeBase,
    spaces: &[String],
    snapshot_version: CommitVersion,
) -> StrataResult<TypedEntries> {
    let storage = db.storage();
    let space_set: HashSet<&str> = spaces.iter().map(|s| s.as_str()).collect();
    let mut cells: BTreeMap<(String, TypeTag), TypedEntryCell> = BTreeMap::new();

    // Seed an empty cell for every requested (space, type_tag) so the
    // classifier and handler precheck always see an entry for every
    // expected key, even if a particular cell happens to be empty.
    for space in spaces {
        for &type_tag in &DATA_TYPE_TAGS {
            cells.insert(
                (space.clone(), type_tag),
                TypedEntryCell {
                    ancestor: Vec::new(),
                    source: Vec::new(),
                    target: Vec::new(),
                },
            );
        }
    }

    // Helper: panics with a clear message if the invariant "every space in
    // `space_set` has a seeded cell for every type_tag" is ever violated.
    // The contains check above filters to known spaces, and the seed loop
    // creates a cell for every (space, type_tag) where space ∈ spaces, so
    // by construction this lookup must succeed. Using `.expect` instead of
    // `if let Some(...)` so any future bug crashes loud rather than
    // silently dropping entries.
    fn cell_for<'c>(
        cells: &'c mut BTreeMap<(String, TypeTag), TypedEntryCell>,
        space: &str,
        type_tag: TypeTag,
    ) -> &'c mut TypedEntryCell {
        cells
            .get_mut(&(space.to_string(), type_tag))
            .expect("seed loop creates a cell for every requested (space, type_tag)")
    }

    for &type_tag in &DATA_TYPE_TAGS {
        // Single per-(branch, type_tag) read; partition into the per-space
        // cells in a single pass. Drops any rows whose space is not part
        // of this merge (defensive: list_by_type_at_version may return
        // rows in any space the branch holds).

        // 1. Ancestor state (at merge base version, WITH tombstones)
        for entry in
            storage.list_by_type_at_version(&merge_base.branch_id, type_tag, merge_base.version)
        {
            if !space_set.contains(entry.key.namespace.space.as_str()) {
                continue;
            }
            cell_for(&mut cells, &entry.key.namespace.space, type_tag)
                .ancestor
                .push(entry);
        }

        // 2. Source state at snapshot (consistent point-in-time, #1917).
        //    `live_entries_from_versioned` drops tombstones.
        for (key, vv) in live_entries_from_versioned(storage.list_by_type_at_version(
            &source_id,
            type_tag,
            snapshot_version,
        )) {
            if !space_set.contains(key.namespace.space.as_str()) {
                continue;
            }
            cell_for(&mut cells, &key.namespace.space, type_tag)
                .source
                .push((key, vv));
        }

        // 3. Target state at snapshot (consistent point-in-time, #1917).
        for (key, vv) in live_entries_from_versioned(storage.list_by_type_at_version(
            &target_id,
            type_tag,
            snapshot_version,
        )) {
            if !space_set.contains(key.namespace.space.as_str()) {
                continue;
            }
            cell_for(&mut cells, &key.namespace.space, type_tag)
                .target
                .push((key, vv));
        }
    }

    Ok(TypedEntries { cells })
}

/// Run the 14-case classification matrix on a single cell, accumulating
/// actions and conflicts into the provided buffers.
///
/// Extracted as a helper so `classify_typed_entries_for_tag` (used by the
/// per-handler `plan` default impl) has a single per-cell classification
/// implementation. The unfiltered `classify_typed_entries` wrapper that
/// once existed has been removed — both `merge_branches` and
/// `cherry_pick_from_diff` go through the per-handler `plan` dispatch
/// instead, which calls `classify_typed_entries_for_tag` per primitive.
fn classify_cell(
    space: &str,
    type_tag: TypeTag,
    cell: &TypedEntryCell,
    strategy: MergeStrategy,
    actions: &mut Vec<MergeAction>,
    conflicts: &mut Vec<ConflictEntry>,
) {
    let ancestor_map = build_ancestor_map(&cell.ancestor, space);
    let source_map = build_live_map(&cell.source, space);
    let target_map = build_live_map(&cell.target, space);

    // Union all keys and classify
    let all_keys: HashSet<Vec<u8>> = ancestor_map
        .keys()
        .chain(source_map.keys())
        .chain(target_map.keys())
        .cloned()
        .collect();

    for user_key in &all_keys {
        // Ancestor: flatten Option<Option<Value>> → Option<&Value>
        // ancestor_map entry = Some(None) means tombstone → treat as absent
        // ancestor_map entry = Some(Some(v)) means live value
        // ancestor_map entry = None means key never existed at ancestor
        let ancestor_val = ancestor_map.get(user_key).and_then(|opt| opt.as_ref());
        let source_val = source_map.get(user_key);
        let target_val = target_map.get(user_key);

        let change = classify_change(ancestor_val, source_val, target_val);

        match change {
            ThreeWayChange::Unchanged
            | ThreeWayChange::TargetChanged
            | ThreeWayChange::BothChangedSame
            | ThreeWayChange::TargetAdded
            | ThreeWayChange::BothAddedSame
            | ThreeWayChange::TargetDeleted
            | ThreeWayChange::BothDeleted => {
                // No action needed — target already has the correct state
            }

            ThreeWayChange::SourceChanged { value } | ThreeWayChange::SourceAdded { value } => {
                actions.push(MergeAction {
                    space: space.to_string(),
                    raw_key: user_key.clone(),
                    type_tag,
                    action: MergeActionKind::Put(value),
                    expected_target: target_val.cloned(),
                });
            }

            ThreeWayChange::SourceDeleted => {
                actions.push(MergeAction {
                    space: space.to_string(),
                    raw_key: user_key.clone(),
                    type_tag,
                    action: MergeActionKind::Delete,
                    expected_target: target_val.cloned(),
                });
            }

            // Conflicts — always report, resolve per strategy
            ThreeWayChange::Conflict {
                source_value,
                target_value,
            } => {
                conflicts.push(ConflictEntry {
                    key: format_user_key(user_key),
                    primitive: type_tag_to_primitive(type_tag),
                    space: space.to_string(),
                    source_value: Some(source_value.clone()),
                    target_value: Some(target_value),
                });
                if strategy == MergeStrategy::LastWriterWins {
                    actions.push(MergeAction {
                        space: space.to_string(),
                        raw_key: user_key.clone(),
                        type_tag,
                        action: MergeActionKind::Put(source_value),
                        expected_target: target_val.cloned(),
                    });
                }
            }

            ThreeWayChange::BothAddedDifferent {
                source_value,
                target_value,
            } => {
                conflicts.push(ConflictEntry {
                    key: format_user_key(user_key),
                    primitive: type_tag_to_primitive(type_tag),
                    space: space.to_string(),
                    source_value: Some(source_value.clone()),
                    target_value: Some(target_value),
                });
                if strategy == MergeStrategy::LastWriterWins {
                    actions.push(MergeAction {
                        space: space.to_string(),
                        raw_key: user_key.clone(),
                        type_tag,
                        action: MergeActionKind::Put(source_value),
                        expected_target: target_val.cloned(),
                    });
                }
            }

            ThreeWayChange::ModifyDeleteConflict { source_value } => {
                conflicts.push(ConflictEntry {
                    key: format_user_key(user_key),
                    primitive: type_tag_to_primitive(type_tag),
                    space: space.to_string(),
                    source_value: Some(source_value.clone()),
                    target_value: None,
                });
                if strategy == MergeStrategy::LastWriterWins {
                    actions.push(MergeAction {
                        space: space.to_string(),
                        raw_key: user_key.clone(),
                        type_tag,
                        action: MergeActionKind::Put(source_value),
                        expected_target: target_val.cloned(),
                    });
                }
            }

            ThreeWayChange::DeleteModifyConflict { target_value } => {
                conflicts.push(ConflictEntry {
                    key: format_user_key(user_key),
                    primitive: type_tag_to_primitive(type_tag),
                    space: space.to_string(),
                    source_value: None,
                    target_value: Some(target_value),
                });
                if strategy == MergeStrategy::LastWriterWins {
                    actions.push(MergeAction {
                        space: space.to_string(),
                        raw_key: user_key.clone(),
                        type_tag,
                        action: MergeActionKind::Delete,
                        expected_target: target_val.cloned(),
                    });
                }
            }
        }
    }
}

/// Run the 14-case classification matrix over only the cells matching
/// `tag`. Used by the `PrimitiveMergeHandler::plan` default implementation
/// — each handler asks for its own type tag's cells, so the cumulative
/// per-handler classification is equivalent to one global call but
/// preserves the per-handler abstraction.
pub(crate) fn classify_typed_entries_for_tag(
    typed: &TypedEntries,
    strategy: MergeStrategy,
    tag: TypeTag,
) -> (Vec<MergeAction>, Vec<ConflictEntry>) {
    let mut actions = Vec::new();
    let mut conflicts = Vec::new();
    for ((space, type_tag), cell) in &typed.cells {
        if *type_tag != tag {
            continue;
        }
        classify_cell(
            space,
            *type_tag,
            cell,
            strategy,
            &mut actions,
            &mut conflicts,
        );
    }
    (actions, conflicts)
}

// Note: there's no `three_way_diff` wrapper. `merge_branches` and
// `cherry_pick_from_diff` both call `gather_typed_entries` followed by
// the per-handler `precheck` + `plan` dispatch via `MergeHandlerRegistry`,
// which calls `classify_typed_entries_for_tag` per primitive.

/// Reload secondary index backends after merge.
fn reload_secondary_backends(db: &Arc<Database>, target_id: BranchId, source_id: BranchId) {
    if let Ok(hooks) = db.extension::<crate::database::refresh::RefreshHooks>() {
        for hook in hooks.hooks() {
            if let Err(e) = hook.post_merge_reload(db, target_id, Some(source_id)) {
                tracing::warn!(
                    target: "strata::branch_ops",
                    error = %e,
                    "Failed to reload secondary index backends after merge"
                );
            }
        }
    }
}

// =============================================================================
// Merge
// =============================================================================

/// Merge data from source branch into target branch using three-way merge,
/// recording `message` and `creator` on the resulting branch DAG merge event.
///
/// Computes the common ancestor (merge base) from the fork/merge relationship
/// between the branches, then classifies each key using a 14-case decision matrix.
/// Correctly handles delete propagation and preserves target-only changes.
///
/// B3.3: merge base comes from [`BranchControlStore::find_merge_base`] — no
/// caller-supplied override.
///
/// Canonical entry point: [`crate::database::BranchService::merge`] /
/// [`crate::database::BranchService::merge_with_options`]. This helper is
/// `pub(crate)` and must not be called from outside the engine crate.
///
/// # Errors
///
/// - Either branch does not exist
/// - No fork or merge relationship between branches
/// - `Strict` strategy with conflicts
pub(crate) fn merge_branches_with_metadata(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    strategy: MergeStrategy,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<MergeInfo> {
    Ok(merge_branches_with_metadata_expected_detailed(
        db, source, target, strategy, message, creator, None, None,
    )?
    .info)
}

pub(crate) fn merge_branches_with_metadata_expected(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    strategy: MergeStrategy,
    message: Option<&str>,
    creator: Option<&str>,
    expected_source_ref: Option<BranchRef>,
    expected_target_ref: Option<BranchRef>,
) -> StrataResult<MergeInfo> {
    Ok(merge_branches_with_metadata_expected_detailed(
        db,
        source,
        target,
        strategy,
        message,
        creator,
        expected_source_ref,
        expected_target_ref,
    )?
    .info)
}

pub(crate) fn merge_branches_with_metadata_expected_detailed(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    strategy: MergeStrategy,
    message: Option<&str>,
    creator: Option<&str>,
    expected_source_ref: Option<BranchRef>,
    expected_target_ref: Option<BranchRef>,
) -> StrataResult<MergeExecutionResult> {
    let source_id = resolve_and_verify_with_expected(db, source, expected_source_ref)?;
    let target_id = resolve_and_verify_with_expected(db, target, expected_target_ref)?;

    let merge_base_point = match match (expected_source_ref, expected_target_ref) {
        (Some(source_ref), Some(target_ref)) => {
            compute_merge_base_point_for_refs(db, source_ref, target_ref)?
        }
        _ => {
            let store = BranchControlStore::new(db.clone());
            let Some(source_gen) = store.active_generation_for_id(source_id)? else {
                return Err(StrataError::invalid_input(format!(
                    "Cannot merge '{}' into '{}': no fork or merge relationship found. \
                     Branches must be related by fork or a previous merge.",
                    source, target
                )));
            };
            let Some(target_gen) = store.active_generation_for_id(target_id)? else {
                return Err(StrataError::invalid_input(format!(
                    "Cannot merge '{}' into '{}': no fork or merge relationship found. \
                     Branches must be related by fork or a previous merge.",
                    source, target
                )));
            };
            compute_merge_base_point_for_refs(
                db,
                BranchRef::new(source_id, source_gen),
                BranchRef::new(target_id, target_gen),
            )?
        }
    } {
        Some(point) => point,
        None => {
            return Err(StrataError::invalid_input(format!(
                "Cannot merge '{}' into '{}': no fork or merge relationship found. \
                 Branches must be related by fork or a previous merge.",
                source, target
            )));
        }
    };
    let merge_base = MergeBase {
        branch_id: merge_base_point.branch.id,
        version: merge_base_point.commit_version,
    };

    // Collect spaces from both branches
    let space_index = SpaceIndex::new(db.clone());
    let source_spaces: HashSet<String> = space_index.list(source_id)?.into_iter().collect();
    let target_spaces: HashSet<String> = space_index.list(target_id)?.into_iter().collect();
    let all_spaces: Vec<String> = source_spaces.union(&target_spaces).cloned().collect();

    // Capture snapshot version for consistent reads during diff (#1917)
    let snapshot_version = db.current_version();

    // Gather typed entries once, then route through the
    // PrimitiveMergeHandler registry. The registry's `precheck` step is
    // where the Event divergence safety check lives — see
    // `EventMergeHandler::precheck` and the design doc at
    // docs/design/branching/primitive-aware-merge.md.
    let typed = gather_typed_entries(
        db,
        source_id,
        target_id,
        &merge_base,
        &all_spaces,
        snapshot_version,
    )?;

    let registry: MergeHandlerRegistry = build_merge_registry();
    let precheck_ctx = MergePrecheckCtx {
        db,
        source_id,
        target_id,
        merge_base: &merge_base,
        strategy,
        typed_entries: &typed,
    };
    for &tag in &DATA_TYPE_TAGS {
        registry.get(tag).precheck(&precheck_ctx)?;
    }

    // Each handler produces its own per-primitive write plan.
    // KV / Vector / Event use the trait's default `plan` impl which
    // delegates to `classify_typed_entries_for_tag` (the 14-case
    // decision matrix). `GraphMergeHandler::plan` overrides with the
    // semantic merge that decodes packed adjacency lists, validates
    // referential integrity, and re-encodes the projected state.
    // `JsonMergeHandler::plan` overrides with per-document path-level
    // merge AND emits secondary index `MergeAction`s for `_idx/...`
    // namespaces so doc + index updates commit atomically.
    let plan_ctx = MergePlanCtx {
        db,
        source_id,
        target_id,
        merge_base: &merge_base,
        strategy,
        typed_entries: &typed,
    };
    let mut actions: Vec<MergeAction> = Vec::new();
    let mut conflicts: Vec<ConflictEntry> = Vec::new();
    for &tag in &DATA_TYPE_TAGS {
        let plan = registry.get(tag).plan(&plan_ctx)?;
        actions.extend(plan.actions);
        conflicts.extend(plan.conflicts);
    }

    // If strict and conflicts exist, return error
    if strategy == MergeStrategy::Strict && !conflicts.is_empty() {
        return Err(StrataError::invalid_input(format!(
            "Merge conflict: {} keys differ between '{}' and '{}'. Use LastWriterWins strategy or resolve conflicts manually.",
            conflicts.len(),
            source,
            target
        )));
    }

    // Apply actions
    let mut keys_applied = 0u64;
    let mut keys_deleted = 0u64;
    let mut spaces_touched: HashSet<String> = HashSet::new();
    let mut merge_version: Option<u64> = None;

    // Group actions by space for space registration. Internal
    // namespaces used by secondary indexes (`_idx/...`, `_idx_meta/...`)
    // are filtered via `is_user_visible_space` so they don't pollute
    // SpaceIndex — they're implementation details, not user spaces.
    for action in &actions {
        spaces_touched.insert(action.space.clone());
    }
    for space in &spaces_touched {
        if space != "default" && is_user_visible_space(space) {
            space_index.register(target_id, space)?;
        }
    }

    if !actions.is_empty() {
        // Build puts, deletes, and expected target values for validation (#1917).
        // OCC is enforced for user-visible namespaces (the actual primitive
        // data) but skipped for internal namespaces (`_idx/...`,
        // `_idx_meta/...`) used by secondary indexes — those entries are
        // derived from the doc-level actions whose OCC IS enforced, so
        // duplicating the check would be redundant and would also fail
        // spuriously when a concurrent JSON write modifies the index
        // entries between gather and apply (the doc-level OCC catches
        // that race correctly).
        let mut puts: Vec<(Key, Value)> = Vec::new();
        let mut deletes: Vec<Key> = Vec::new();
        let mut expected_targets: Vec<(Key, Option<Value>)> = Vec::new();

        for action in &actions {
            let ns = Arc::new(Namespace::for_branch_space(target_id, &action.space));
            let key = Key::new(ns, action.type_tag, action.raw_key.clone());
            if is_user_visible_space(&action.space) {
                expected_targets.push((key.clone(), action.expected_target.clone()));
            }
            match &action.action {
                MergeActionKind::Put(value) => {
                    puts.push((key, value.clone()));
                    keys_applied += 1;
                }
                MergeActionKind::Delete => {
                    deletes.push(key);
                    keys_deleted += 1;
                }
            }
        }

        // Apply in a single transaction, capturing the merge version.
        // Read each target key first to populate read_set for OCC (#1917).
        let ((), version) = db.transaction_with_version(target_id, |txn| {
            if let Some(expected) = expected_source_ref {
                verify_expected_active_ref_in_txn(txn, source, expected)?;
            }
            if let Some(expected) = expected_target_ref {
                verify_expected_active_ref_in_txn(txn, target, expected)?;
            }
            // Validate target hasn't changed since the diff snapshot (#1917).
            // txn.get() populates read_set, enabling OCC conflict detection.
            for (key, expected) in &expected_targets {
                let current = txn.get(key)?;
                if current.as_ref() != expected.as_ref() {
                    return Err(StrataError::conflict(format!(
                        "merge target concurrently modified: key changed between \
                         diff snapshot and apply (expected present={}, found present={})",
                        expected.is_some(),
                        current.is_some(),
                    )));
                }
            }
            for (key, value) in &puts {
                txn.put(key.clone(), value.clone())?;
            }
            for key in &deletes {
                txn.delete(key.clone())?;
            }
            Ok(())
        })?;
        merge_version = Some(version);
    }

    // Per-primitive post-commit handlers run before the existing reload
    // sweep. KV / Event are no-ops; Vector rebuilds the affected
    // collections' HNSW backends; JSON refreshes secondary indexes and
    // the BM25 inverted index for affected documents; Graph is a no-op
    // because the semantic merge in `plan` already wrote the projected
    // adjacency lists. The legacy `reload_secondary_backends` sweep
    // below remains for refresh hooks that haven't migrated to
    // per-handler `post_commit` yet (currently a no-op for the
    // registered VectorRefreshHook).
    let post_ctx = MergePostCommitCtx {
        db,
        source_id,
        target_id,
        merge_version,
    };
    for &tag in &DATA_TYPE_TAGS {
        registry.get(tag).post_commit(&post_ctx)?;
    }

    // Reload secondary index backends
    reload_secondary_backends(db, target_id, source_id);

    info!(
        target: "strata::branch_ops",
        source,
        target,
        keys_applied,
        keys_deleted,
        ?merge_version,
        spaces_merged = spaces_touched.len(),
        strategy = ?strategy,
        "Branches merged (three-way)"
    );

    let info = MergeInfo {
        source: source.to_string(),
        target: target.to_string(),
        keys_applied,
        keys_deleted,
        conflicts,
        spaces_merged: spaces_touched.len() as u64,
        merge_version,
    };

    dispatch_merge_hook(db, &info, strategy, message, creator);

    Ok(MergeExecutionResult {
        info,
        merge_base_used: merge_base_point,
    })
}

// =============================================================================
// Three-Way Diff (public entry point)
// =============================================================================

/// Compute the three-way diff between source and target branches.
///
/// Returns the raw classification for each key (14-case decision matrix)
/// without applying any merge strategy. This is useful for inspection
/// before deciding to merge.
///
/// B3.3: merge base comes from [`BranchControlStore::find_merge_base`] —
/// no caller-supplied override.
///
/// # Errors
///
/// - Either branch does not exist
/// - No fork or merge relationship between branches
pub fn diff_three_way(
    db: &Arc<Database>,
    source: &str,
    target: &str,
) -> StrataResult<ThreeWayDiffResult> {
    let source_id = resolve_and_verify(db, source)?;
    let target_id = resolve_and_verify(db, target)?;

    let merge_base = match compute_merge_base(db, source_id, target_id)? {
        Some(mb) => mb,
        None => {
            return Err(StrataError::invalid_input(format!(
                "Cannot compute three-way diff between '{}' and '{}': no fork or merge relationship found. \
                 Branches must be related by fork or a previous merge.",
                source, target
            )));
        }
    };

    // Resolve merge base branch_id back to a name.
    // The merge base comes from either source_id or target_id (via compute_merge_base).
    let merge_base_branch_name = if merge_base.branch_id == source_id {
        source.to_string()
    } else if merge_base.branch_id == target_id {
        target.to_string()
    } else {
        // Fallback: try to find the branch name from the branch index
        let branch_index = BranchIndex::new(db.clone());
        let mut found_name = format!("{:?}", merge_base.branch_id);
        if let Ok(names) = branch_index.list_branches() {
            for name in names {
                if resolve_branch_name(&name) == merge_base.branch_id {
                    found_name = name;
                    break;
                }
            }
        }
        found_name
    };

    // Collect spaces from both branches
    let space_index = SpaceIndex::new(db.clone());
    let source_spaces: HashSet<String> = space_index.list(source_id)?.into_iter().collect();
    let target_spaces: HashSet<String> = space_index.list(target_id)?.into_iter().collect();
    let all_spaces: Vec<String> = source_spaces.union(&target_spaces).cloned().collect();

    let storage = db.storage();
    let mut entries = Vec::new();

    for space in &all_spaces {
        for &type_tag in &DATA_TYPE_TAGS {
            // 1. Ancestor state (at merge base version, WITH tombstones)
            let ancestor_entries = storage.list_by_type_at_version(
                &merge_base.branch_id,
                type_tag,
                merge_base.version,
            );

            // 2. Source current state (live keys only)
            let source_entries = storage.list_by_type(&source_id, type_tag);

            // 3. Target current state (live keys only)
            let target_entries = storage.list_by_type(&target_id, type_tag);

            let ancestor_map = build_ancestor_map(&ancestor_entries, space);
            let source_map = build_live_map(&source_entries, space);
            let target_map = build_live_map(&target_entries, space);

            // Union all keys and classify
            let all_keys: HashSet<Vec<u8>> = ancestor_map
                .keys()
                .chain(source_map.keys())
                .chain(target_map.keys())
                .cloned()
                .collect();

            for user_key in &all_keys {
                let ancestor_val = ancestor_map.get(user_key).and_then(|opt| opt.as_ref());
                let source_val = source_map.get(user_key);
                let target_val = target_map.get(user_key);

                let change = classify_change(ancestor_val, source_val, target_val);

                // Only include non-Unchanged entries
                if change != ThreeWayChange::Unchanged {
                    entries.push(ThreeWayDiffEntry {
                        key: format_user_key(user_key),
                        raw_key: user_key.clone(),
                        space: space.clone(),
                        primitive: type_tag_to_primitive(type_tag),
                        type_tag,
                        change,
                    });
                }
            }
        }
    }

    Ok(ThreeWayDiffResult {
        source: source.to_string(),
        target: target.to_string(),
        merge_base_branch: merge_base_branch_name,
        merge_base_version: merge_base.version,
        entries,
    })
}

/// Get the merge base for two branches.
///
/// Returns `None` if the branches have no fork or merge relationship.
///
/// B3.3: merge base comes from [`BranchControlStore::find_merge_base`] —
/// no caller-supplied override.
///
/// # Errors
///
/// - Either branch does not exist
pub fn get_merge_base(
    db: &Arc<Database>,
    source: &str,
    target: &str,
) -> StrataResult<Option<MergeBaseInfo>> {
    let source_id = resolve_and_verify(db, source)?;
    let target_id = resolve_and_verify(db, target)?;

    match compute_merge_base(db, source_id, target_id)? {
        None => Ok(None),
        Some(mb) => {
            // Resolve merge base branch_id back to a name
            let branch_name = if mb.branch_id == source_id {
                source.to_string()
            } else if mb.branch_id == target_id {
                target.to_string()
            } else {
                // Fallback: scan branch index
                let branch_index = BranchIndex::new(db.clone());
                let mut found_name = format!("{:?}", mb.branch_id);
                if let Ok(names) = branch_index.list_branches() {
                    for name in names {
                        if resolve_branch_name(&name) == mb.branch_id {
                            found_name = name;
                            break;
                        }
                    }
                }
                found_name
            };

            Ok(Some(MergeBaseInfo {
                branch: branch_name,
                version: mb.version,
            }))
        }
    }
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
// Tags
// =============================================================================

/// A named version bookmark on a branch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TagInfo {
    /// Tag name (e.g. "v1.0")
    pub name: String,
    /// Branch this tag belongs to
    pub branch: String,
    /// MVCC version this tag points to
    pub version: u64,
    /// Optional human-readable message
    pub message: Option<String>,
    /// Timestamp in microseconds since epoch
    pub timestamp: u64,
    /// Optional creator identifier
    pub creator: Option<String>,
}

/// Create a tag on a branch at a specific version.
///
/// If `version` is `None`, tags the current database version.
///
/// Canonical entry point: [`crate::database::BranchService::tag`]. This
/// helper is `pub(crate)` and must not be called from outside the engine
/// crate.
///
/// # Errors
///
/// - Branch does not exist
/// - Serialization failure (internal)
pub(crate) fn create_tag(
    db: &Arc<Database>,
    branch: &str,
    name: &str,
    version: Option<u64>,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<TagInfo> {
    create_tag_with_expected(db, branch, name, version, message, creator, None)
}

/// Create a tag on a branch while pinning the expected branch lifecycle
/// instance.
pub(crate) fn create_tag_with_expected(
    db: &Arc<Database>,
    branch: &str,
    name: &str,
    version: Option<u64>,
    message: Option<&str>,
    creator: Option<&str>,
    expected_branch_ref: Option<BranchRef>,
) -> StrataResult<TagInfo> {
    resolve_and_verify_with_expected(db, branch, expected_branch_ref)?;

    if branch.contains(':') {
        return Err(StrataError::invalid_input("Branch name cannot contain ':'"));
    }
    if name.contains(':') {
        return Err(StrataError::invalid_input("Tag name cannot contain ':'"));
    }

    if resolve_tag(db, branch, name)?.is_some() {
        return Err(StrataError::invalid_input(format!(
            "Tag '{}' already exists on branch '{}'",
            name, branch
        )));
    }

    let version = version.unwrap_or_else(|| db.current_version().as_u64());
    let timestamp = strata_core::contract::Timestamp::now().as_micros();

    let tag_info = TagInfo {
        name: name.to_string(),
        branch: branch.to_string(),
        version,
        message: message.map(|s| s.to_string()),
        timestamp,
        creator: creator.map(|s| s.to_string()),
    };

    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let tag_key = tag_key(branch, name);
    let tag_value = serde_json::to_string(&tag_info)
        .map_err(|e| StrataError::internal(format!("Failed to serialize tag: {}", e)))?;

    db.transaction(system_id, move |txn| {
        if let Some(expected) = expected_branch_ref {
            verify_expected_active_ref_in_txn(txn, branch, expected)?;
        }
        let ns = Arc::new(Namespace::for_branch_space(system_id, "default"));
        let key = Key::new(ns, TypeTag::KV, tag_key.as_bytes().to_vec());
        txn.put(key, Value::String(tag_value.clone()))?;
        Ok(())
    })?;

    Ok(tag_info)
}

/// Delete a tag.
///
/// Returns `true` if the tag existed and was deleted.
///
/// Canonical entry point: [`crate::database::BranchService::untag`]. This
/// helper is `pub(crate)` and must not be called from outside the engine
/// crate.
pub(crate) fn delete_tag(db: &Arc<Database>, branch: &str, name: &str) -> StrataResult<bool> {
    delete_tag_with_expected(db, branch, name, None)
}

/// Delete a tag while pinning the expected branch lifecycle instance.
pub(crate) fn delete_tag_with_expected(
    db: &Arc<Database>,
    branch: &str,
    name: &str,
    expected_branch_ref: Option<BranchRef>,
) -> StrataResult<bool> {
    resolve_and_verify_with_expected(db, branch, expected_branch_ref)?;

    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let tag_key = tag_key(branch, name);

    let storage = db.storage();
    let entries = storage.list_by_type(&system_id, TypeTag::KV);
    let exists = entries
        .iter()
        .any(|(k, _)| k.namespace.space == "default" && *k.user_key == *tag_key.as_bytes());

    if exists {
        db.transaction(system_id, move |txn| {
            if let Some(expected) = expected_branch_ref {
                verify_expected_active_ref_in_txn(txn, branch, expected)?;
            }
            let ns = Arc::new(Namespace::for_branch_space(system_id, "default"));
            let key = Key::new(ns, TypeTag::KV, tag_key.as_bytes().to_vec());
            txn.delete(key)?;
            Ok(())
        })?;
    }

    Ok(exists)
}

/// List all tags on a branch.
pub fn list_tags(db: &Arc<Database>, branch: &str) -> StrataResult<Vec<TagInfo>> {
    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let prefix = tag_prefix(branch);

    let storage = db.storage();
    let entries = storage.list_by_type(&system_id, TypeTag::KV);

    let mut tags = Vec::new();
    for (key, vv) in &entries {
        if key.namespace.space == "default" {
            let key_str = String::from_utf8_lossy(&key.user_key);
            if key_str.starts_with(&prefix) {
                if let Value::String(json_str) = &vv.value {
                    if let Ok(tag) = serde_json::from_str::<TagInfo>(json_str) {
                        tags.push(tag);
                    }
                }
            }
        }
    }

    tags.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    Ok(tags)
}

/// Resolve a tag name to its info.
pub fn resolve_tag(db: &Arc<Database>, branch: &str, name: &str) -> StrataResult<Option<TagInfo>> {
    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let tag_key = tag_key(branch, name);

    let storage = db.storage();
    let entries = storage.list_by_type(&system_id, TypeTag::KV);

    for (key, vv) in &entries {
        if key.namespace.space == "default" && *key.user_key == *tag_key.as_bytes() {
            if let Value::String(json_str) = &vv.value {
                if let Ok(tag) = serde_json::from_str::<TagInfo>(json_str) {
                    return Ok(Some(tag));
                }
            }
        }
    }

    Ok(None)
}

// =============================================================================
// Notes
// =============================================================================

/// A version-annotated note on a branch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoteInfo {
    /// Branch this note belongs to
    pub branch: String,
    /// MVCC version this note is attached to
    pub version: u64,
    /// Human-readable message
    pub message: String,
    /// Optional author identifier
    pub author: Option<String>,
    /// Timestamp in microseconds since epoch
    pub timestamp: u64,
    /// Optional structured metadata
    pub metadata: Option<Value>,
}

/// Add a note to a specific version of a branch.
///
/// Canonical entry point: [`crate::database::BranchService::add_note`].
/// This helper is `pub(crate)` and must not be called from outside the
/// engine crate.
///
/// # Errors
///
/// - Branch does not exist
/// - Serialization failure (internal)
pub(crate) fn add_note(
    db: &Arc<Database>,
    branch: &str,
    version: CommitVersion,
    message: &str,
    author: Option<&str>,
    metadata: Option<Value>,
) -> StrataResult<NoteInfo> {
    add_note_with_expected(db, branch, version, message, author, metadata, None)
}

/// Add a note while pinning the expected branch lifecycle instance.
pub(crate) fn add_note_with_expected(
    db: &Arc<Database>,
    branch: &str,
    version: CommitVersion,
    message: &str,
    author: Option<&str>,
    metadata: Option<Value>,
    expected_branch_ref: Option<BranchRef>,
) -> StrataResult<NoteInfo> {
    resolve_and_verify_with_expected(db, branch, expected_branch_ref)?;

    if branch.contains(':') {
        return Err(StrataError::invalid_input("Branch name cannot contain ':'"));
    }

    let timestamp = strata_core::contract::Timestamp::now().as_micros();
    let note = NoteInfo {
        branch: branch.to_string(),
        version: version.as_u64(),
        message: message.to_string(),
        author: author.map(|s| s.to_string()),
        timestamp,
        metadata,
    };

    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let note_key = note_key(branch, version.as_u64());
    let note_value = serde_json::to_string(&note)
        .map_err(|e| StrataError::internal(format!("Failed to serialize note: {}", e)))?;

    db.transaction(system_id, move |txn| {
        if let Some(expected) = expected_branch_ref {
            verify_expected_active_ref_in_txn(txn, branch, expected)?;
        }
        let ns = Arc::new(Namespace::for_branch_space(system_id, "default"));
        let key = Key::new(ns, TypeTag::KV, note_key.as_bytes().to_vec());
        txn.put(key, Value::String(note_value.clone()))?;
        Ok(())
    })?;

    Ok(note)
}

/// Get notes for a branch, optionally filtered by version.
///
/// If `version` is `None`, returns all notes for the branch.
/// If `version` is `Some(v)`, returns only the note at that version (if any).
pub fn get_notes(
    db: &Arc<Database>,
    branch: &str,
    version: Option<u64>,
) -> StrataResult<Vec<NoteInfo>> {
    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let prefix = note_prefix(branch);
    let exact_key = version.map(|v| note_key(branch, v));

    let storage = db.storage();
    let entries = storage.list_by_type(&system_id, TypeTag::KV);

    let mut notes = Vec::new();
    for (key, vv) in &entries {
        if key.namespace.space == "default" {
            let key_str = String::from_utf8_lossy(&key.user_key);
            let matches = match &exact_key {
                Some(ek) => *key_str == **ek,
                None => key_str.starts_with(&prefix),
            };
            if matches {
                if let Value::String(json_str) = &vv.value {
                    if let Ok(note) = serde_json::from_str::<NoteInfo>(json_str) {
                        notes.push(note);
                    }
                }
            }
        }
    }

    notes.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(notes)
}

/// Delete a note at a specific version.
///
/// Returns `true` if the note existed and was deleted.
///
/// Canonical entry point: [`crate::database::BranchService::delete_note`].
/// This helper is `pub(crate)` and must not be called from outside the
/// engine crate.
pub(crate) fn delete_note(
    db: &Arc<Database>,
    branch: &str,
    version: CommitVersion,
) -> StrataResult<bool> {
    delete_note_with_expected(db, branch, version, None)
}

/// Delete a note while pinning the expected branch lifecycle instance.
pub(crate) fn delete_note_with_expected(
    db: &Arc<Database>,
    branch: &str,
    version: CommitVersion,
    expected_branch_ref: Option<BranchRef>,
) -> StrataResult<bool> {
    resolve_and_verify_with_expected(db, branch, expected_branch_ref)?;

    let system_id = resolve_branch_name(SYSTEM_BRANCH);
    let note_key = note_key(branch, version.as_u64());

    let storage = db.storage();
    let entries = storage.list_by_type(&system_id, TypeTag::KV);
    let exists = entries
        .iter()
        .any(|(k, _)| k.namespace.space == "default" && *k.user_key == *note_key.as_bytes());

    if exists {
        db.transaction(system_id, move |txn| {
            if let Some(expected) = expected_branch_ref {
                verify_expected_active_ref_in_txn(txn, branch, expected)?;
            }
            let ns = Arc::new(Namespace::for_branch_space(system_id, "default"));
            let key = Key::new(ns, TypeTag::KV, note_key.as_bytes().to_vec());
            txn.delete(key)?;
            Ok(())
        })?;
    }

    Ok(exists)
}

// =============================================================================
// Revert
// =============================================================================

/// Information returned after reverting a version range.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RevertInfo {
    /// Branch name
    pub branch: String,
    /// Start of the reverted range (inclusive)
    pub from_version: CommitVersion,
    /// End of the reverted range (inclusive)
    pub to_version: CommitVersion,
    /// Number of keys reverted
    pub keys_reverted: u64,
    /// MVCC version of the revert transaction
    pub revert_version: Option<CommitVersion>,
}

/// Revert a version range on a branch, recording `message` and `creator`
/// on the resulting branch DAG revert event.
///
/// For each key modified in [from_version, to_version], restores its value
/// to what it was at (from_version - 1). Only reverts keys whose current
/// value matches the state at to_version — keys modified after to_version
/// are left untouched (preserving subsequent work).
///
/// Canonical entry point: [`crate::database::BranchService::revert`]. This
/// helper is `pub(crate)` and must not be called from outside the engine
/// crate.
pub(crate) fn revert_version_range_with_metadata(
    db: &Arc<Database>,
    branch: &str,
    from_version: CommitVersion,
    to_version: CommitVersion,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<RevertInfo> {
    revert_version_range_with_expected(db, branch, from_version, to_version, message, creator, None)
}

pub(crate) fn revert_version_range_with_expected(
    db: &Arc<Database>,
    branch: &str,
    from_version: CommitVersion,
    to_version: CommitVersion,
    message: Option<&str>,
    creator: Option<&str>,
    expected_branch_ref: Option<BranchRef>,
) -> StrataResult<RevertInfo> {
    // Validate range
    if from_version == CommitVersion::ZERO {
        return Err(StrataError::invalid_input("from_version must be > 0"));
    }
    if from_version > to_version {
        return Err(StrataError::invalid_input(format!(
            "from_version ({}) must be <= to_version ({})",
            from_version, to_version
        )));
    }
    let current_version = db.current_version();
    if to_version > current_version {
        return Err(StrataError::invalid_input(format!(
            "to_version ({}) exceeds current database version ({})",
            to_version, current_version
        )));
    }

    let branch_id = resolve_and_verify_with_expected(db, branch, expected_branch_ref)?;
    let storage = db.storage();

    let mut puts: Vec<(Key, Value)> = Vec::new();
    let mut deletes: Vec<Key> = Vec::new();

    for &type_tag in &DATA_TYPE_TAGS {
        // 1. "before" state: what the branch looked like at (from_version - 1)
        //    WITH tombstones, so we can distinguish "key existed" from "key absent"
        let before_entries = storage.list_by_type_at_version(
            &branch_id,
            type_tag,
            CommitVersion(from_version.as_u64().saturating_sub(1)),
        );

        // 2. "after" state: what the branch looked like at to_version
        //    WITH tombstones
        let after_entries = storage.list_by_type_at_version(&branch_id, type_tag, to_version);

        // 3. Current live state
        let current_entries = storage.list_by_type(&branch_id, type_tag);

        let before_map = build_versioned_space_map(&before_entries);
        let after_map = build_versioned_space_map(&after_entries);
        let current_map = build_live_space_map(&current_entries);

        // Union of all keys in before and after
        let all_keys: HashSet<(String, Vec<u8>)> =
            before_map.keys().chain(after_map.keys()).cloned().collect();

        for compound_key in &all_keys {
            let before_state = before_map.get(compound_key).cloned().flatten();
            let after_state = after_map.get(compound_key).cloned().flatten();
            let current_state = current_map.get(compound_key).cloned();

            // If before == after: key wasn't modified in range, skip
            if before_state == after_state {
                continue;
            }

            // Key was modified in [from_version, to_version].
            // Only revert if current state matches the "after" state.
            if current_state == after_state {
                let (space, user_key) = compound_key;
                let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
                let key = Key::new(ns, type_tag, user_key.clone());

                match &before_state {
                    Some(value) => {
                        // Restore to "before" value
                        puts.push((key, value.clone()));
                    }
                    None => {
                        // Key didn't exist before the range — delete it
                        deletes.push(key);
                    }
                }
            }
            // else: current != after, meaning someone modified it after the range.
            // Preserve subsequent work by skipping.
        }
    }

    let keys_reverted = (puts.len() + deletes.len()) as u64;
    let mut revert_version = None;

    if !puts.is_empty() || !deletes.is_empty() {
        let ((), version) = db.transaction_with_version(branch_id, |txn| {
            if let Some(expected) = expected_branch_ref {
                verify_expected_active_ref_in_txn(txn, branch, expected)?;
            }
            for (key, value) in &puts {
                txn.put(key.clone(), value.clone())?;
            }
            for key in &deletes {
                txn.delete(key.clone())?;
            }
            Ok(())
        })?;
        revert_version = Some(CommitVersion(version));
    }

    info!(
        target: "strata::branch_ops",
        branch,
        from_version = from_version.as_u64(),
        to_version = to_version.as_u64(),
        keys_reverted,
        revert_version = ?revert_version.map(|v| v.as_u64()),
        "Version range reverted"
    );

    let info = RevertInfo {
        branch: branch.to_string(),
        from_version,
        to_version,
        keys_reverted,
        revert_version,
    };

    dispatch_revert_hook(db, &info, message, creator);

    Ok(info)
}

// =============================================================================
// Cherry-Pick
// =============================================================================

/// Information returned after cherry-picking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CherryPickInfo {
    /// Source branch name
    pub source: String,
    /// Target branch name
    pub target: String,
    /// Number of keys applied (puts)
    pub keys_applied: u64,
    /// Number of keys deleted
    pub keys_deleted: u64,
    /// MVCC version of the cherry-pick transaction
    pub cherry_pick_version: Option<u64>,
}

/// Filter for cherry-pick operations.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CherryPickFilter {
    /// Only pick changes in these spaces.
    pub spaces: Option<Vec<String>>,
    /// Only pick changes for these keys (user_key strings).
    pub keys: Option<Vec<String>>,
    /// Only pick changes for these primitive types.
    pub primitives: Option<Vec<PrimitiveType>>,
}

/// Cherry-pick specific keys from source branch to target branch.
///
/// Reads the current value of each specified key from source and writes
/// it to target in a single transaction.
///
/// Canonical entry point:
/// [`crate::database::BranchService::cherry_pick`]. This helper is
/// `pub(crate)` and must not be called from outside the engine crate.
pub(crate) fn cherry_pick_keys(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    keys: &[(String, String)], // (space, key) pairs
) -> StrataResult<CherryPickInfo> {
    cherry_pick_keys_expected(db, source, target, keys, None, None)
}

pub(crate) fn cherry_pick_keys_expected(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    keys: &[(String, String)],
    expected_source_ref: Option<BranchRef>,
    expected_target_ref: Option<BranchRef>,
) -> StrataResult<CherryPickInfo> {
    let source_id = resolve_and_verify_with_expected(db, source, expected_source_ref)?;
    let target_id = resolve_and_verify_with_expected(db, target, expected_target_ref)?;
    let storage = db.storage();
    let space_index = SpaceIndex::new(db.clone());

    let requested: HashSet<(&str, &str)> =
        keys.iter().map(|(s, k)| (s.as_str(), k.as_str())).collect();

    let mut puts: Vec<(Key, Value)> = Vec::new();

    for &type_tag in &DATA_TYPE_TAGS {
        let entries = storage.list_by_type(&source_id, type_tag);
        for (key, vv) in &entries {
            let space = &key.namespace.space;
            let user_key_str = format_user_key(&key.user_key);
            if requested.contains(&(space.as_str(), user_key_str.as_str())) {
                // Ensure target has this space
                if space != "default" {
                    space_index.register(target_id, space)?;
                }
                let ns = Arc::new(Namespace::for_branch_space(target_id, space));
                let target_key = Key::new(ns, type_tag, key.user_key.to_vec());
                puts.push((target_key, vv.value.clone()));
            }
        }
    }

    let mut cherry_pick_version = None;
    let keys_applied = puts.len() as u64;

    if !puts.is_empty() {
        let ((), version) = db.transaction_with_version(target_id, |txn| {
            if let Some(expected) = expected_source_ref {
                verify_expected_active_ref_in_txn(txn, source, expected)?;
            }
            if let Some(expected) = expected_target_ref {
                verify_expected_active_ref_in_txn(txn, target, expected)?;
            }
            for (key, value) in &puts {
                txn.put(key.clone(), value.clone())?;
            }
            Ok(())
        })?;
        cherry_pick_version = Some(version);
    }

    info!(
        target: "strata::branch_ops",
        source,
        target,
        keys_applied,
        ?cherry_pick_version,
        "Keys cherry-picked"
    );

    let info = CherryPickInfo {
        source: source.to_string(),
        target: target.to_string(),
        keys_applied,
        keys_deleted: 0,
        cherry_pick_version,
    };

    dispatch_cherry_pick_hook(db, source, target, &info);

    Ok(info)
}

/// Graph cherry-pick atomicity guard.
///
/// Graph plan actions for the same `(space, Graph)` cell are interdependent
/// (`n/X`, `fwd/X`, `rev/Y`, `__edge_count__/T` for the same logical change
/// must apply atomically — bidirectional consistency depends on it). The
/// `CherryPickFilter` has three orthogonal dimensions:
///
/// - `spaces` and `primitives` partition cells atomically: a (space, type_tag)
///   cell is either entirely in or entirely out, regardless of which actions
///   it produced.
/// - `keys` is the dangerous one: a user-supplied set of `user_key` strings
///   could include `g/n/alice` but exclude `g/fwd/alice`, leaving target
///   with the node but no outgoing adjacency entry — the same bidirectional
///   inconsistency the `check_graph_merge_divergence` fallback exists to
///   prevent.
///
/// Rule: for each `(space, Graph)` cell that produced actions, count how many
/// of the cell's graph actions pass the full filter. If 0 pass, drop them all
/// (fine). If all pass, keep them all (fine). Otherwise the filter would split
/// the cell — return a structured error.
///
/// Returns `Ok(())` immediately when `filter.keys` is `None`, since only the
/// keys dimension can split graph actions within a cell.
fn check_graph_action_atomicity(
    actions: &[MergeAction],
    filter: &CherryPickFilter,
) -> StrataResult<()> {
    if filter.keys.is_none() {
        return Ok(());
    }
    let space_filter: Option<HashSet<&str>> = filter
        .spaces
        .as_ref()
        .map(|s| s.iter().map(|x| x.as_str()).collect());
    let key_filter: Option<HashSet<&str>> = filter
        .keys
        .as_ref()
        .map(|k| k.iter().map(|x| x.as_str()).collect());
    let prim_filter: Option<HashSet<PrimitiveType>> = filter
        .primitives
        .as_ref()
        .map(|p| p.iter().copied().collect());

    // Inline the predicate from the existing filter loop so the atomicity
    // check sees the same pass/fail decisions as the actual filter.
    let action_passes = |action: &MergeAction| -> bool {
        if let Some(ref allowed) = space_filter {
            if !allowed.contains(action.space.as_str()) {
                return false;
            }
        }
        if let Some(ref allowed) = key_filter {
            let key_str = format_user_key(&action.raw_key);
            if !allowed.contains(key_str.as_str()) {
                return false;
            }
        }
        if let Some(ref allowed) = prim_filter {
            let prim = type_tag_to_primitive(action.type_tag);
            if !allowed.contains(&prim) {
                return false;
            }
        }
        true
    };

    // Group graph actions by space and count (passed, total) per group.
    let mut graph_actions_by_space: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for action in actions {
        if action.type_tag != TypeTag::Graph {
            continue;
        }
        let entry = graph_actions_by_space
            .entry(action.space.as_str())
            .or_insert((0, 0));
        entry.1 += 1;
        if action_passes(action) {
            entry.0 += 1;
        }
    }

    for (space, (passed, total)) in &graph_actions_by_space {
        if *passed != 0 && *passed != *total {
            return Err(StrataError::invalid_input(format!(
                "cherry-pick: graph data must be applied atomically per cell. \
                 Filter would include {passed} of {total} graph plan actions \
                 for space '{space}'. Graph actions are interdependent \
                 (nodes, forward/reverse adjacency, edge counters must apply \
                 together to preserve bidirectional consistency). Use \
                 `--primitives` or `--spaces` filters instead of `--keys` \
                 when cherry-picking graph data, or include all graph keys \
                 for the cell."
            )));
        }
    }

    Ok(())
}

/// Cherry-pick from a three-way diff, applying only entries matching a filter.
///
/// This is a selective merge: computes the three-way diff between source and
/// target, then applies only the changes that match the filter criteria.
///
/// B3.3: merge base comes from [`BranchControlStore::find_merge_base`] —
/// no caller-supplied override.
///
/// Canonical entry point:
/// [`crate::database::BranchService::cherry_pick_from_diff`]. This helper
/// is `pub(crate)` and must not be called from outside the engine crate.
pub(crate) fn cherry_pick_from_diff(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    filter: CherryPickFilter,
) -> StrataResult<CherryPickInfo> {
    cherry_pick_from_diff_expected(db, source, target, filter, None, None)
}

pub(crate) fn cherry_pick_from_diff_expected(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    filter: CherryPickFilter,
    expected_source_ref: Option<BranchRef>,
    expected_target_ref: Option<BranchRef>,
) -> StrataResult<CherryPickInfo> {
    let source_id = resolve_and_verify_with_expected(db, source, expected_source_ref)?;
    let target_id = resolve_and_verify_with_expected(db, target, expected_target_ref)?;

    let merge_base = match match (expected_source_ref, expected_target_ref) {
        (Some(source_ref), Some(target_ref)) => {
            compute_merge_base_for_refs(db, source_ref, target_ref)?
        }
        _ => compute_merge_base(db, source_id, target_id)?,
    } {
        Some(mb) => mb,
        None => {
            return Err(StrataError::invalid_input(format!(
                "Cannot cherry-pick from '{}' to '{}': no fork or merge relationship found. \
                 Branches must be related by fork or a previous merge.",
                source, target
            )));
        }
    };

    // Collect spaces from both branches
    let space_index = SpaceIndex::new(db.clone());
    let source_spaces: HashSet<String> = space_index.list(source_id)?.into_iter().collect();
    let target_spaces: HashSet<String> = space_index.list(target_id)?.into_iter().collect();
    let all_spaces: Vec<String> = source_spaces.union(&target_spaces).cloned().collect();

    // Capture snapshot for consistent reads (#1917)
    let snapshot_version = db.current_version();

    // Cherry-pick uses the same per-handler `precheck` + `plan` dispatch
    // as `merge_branches`, so it inherits the semantic graph merge and
    // the additive catalog merge for free.
    //
    // Strategy is always LastWriterWins for cherry-pick — non-fatal cell
    // conflicts (NodeProperty / EdgeData / etc.) get silently resolved by
    // the graph handler under LWW, matching cherry-pick's "ignore
    // conflicts" semantics. Fatal conflicts (DanglingEdge,
    // OrphanedReference) propagate as `Err` from `plan` and abort the
    // cherry-pick before any writes happen. (CatalogDivergence is
    // reserved but unreachable from the current additive catalog merge.)
    let typed = gather_typed_entries(
        db,
        source_id,
        target_id,
        &merge_base,
        &all_spaces,
        snapshot_version,
    )?;

    let registry = primitive_merge::build_merge_registry();
    let precheck_ctx = MergePrecheckCtx {
        db,
        source_id,
        target_id,
        merge_base: &merge_base,
        strategy: MergeStrategy::LastWriterWins,
        typed_entries: &typed,
    };
    for &tag in &DATA_TYPE_TAGS {
        registry.get(tag).precheck(&precheck_ctx)?;
    }

    let plan_ctx = MergePlanCtx {
        db,
        source_id,
        target_id,
        merge_base: &merge_base,
        strategy: MergeStrategy::LastWriterWins,
        typed_entries: &typed,
    };
    let mut actions: Vec<MergeAction> = Vec::new();
    for &tag in &DATA_TYPE_TAGS {
        let plan = registry.get(tag).plan(&plan_ctx)?;
        actions.extend(plan.actions);
        // Cherry-pick discards non-fatal conflicts (LWW resolves them in
        // place). Fatal conflicts already propagated via `plan(...)?` above.
    }

    // Graph plan actions are interdependent (n/X, fwd/X, rev/Y,
    // __edge_count__/T must apply atomically for one logical change). If
    // the user's CherryPickFilter would accept some but not all graph
    // actions for the same (space, Graph) cell, refuse with a clear
    // error so the caller can either widen their filter or use
    // --primitives/--spaces instead of --keys.
    check_graph_action_atomicity(&actions, &filter)?;

    // Filter by CherryPickFilter
    let space_filter: Option<HashSet<&str>> = filter
        .spaces
        .as_ref()
        .map(|s| s.iter().map(|x| x.as_str()).collect());
    let key_filter: Option<HashSet<&str>> = filter
        .keys
        .as_ref()
        .map(|k| k.iter().map(|x| x.as_str()).collect());
    let prim_filter: Option<HashSet<PrimitiveType>> = filter
        .primitives
        .as_ref()
        .map(|p| p.iter().copied().collect());

    let filtered_actions: Vec<&MergeAction> = actions
        .iter()
        .filter(|action| {
            // Space filter
            if let Some(ref allowed) = space_filter {
                if !allowed.contains(action.space.as_str()) {
                    return false;
                }
            }
            // Key filter
            if let Some(ref allowed) = key_filter {
                let key_str = format_user_key(&action.raw_key);
                if !allowed.contains(key_str.as_str()) {
                    return false;
                }
            }
            // Primitive type filter
            if let Some(ref allowed) = prim_filter {
                let prim = type_tag_to_primitive(action.type_tag);
                if !allowed.contains(&prim) {
                    return false;
                }
            }
            true
        })
        .collect();

    // Register spaces and build puts/deletes. Internal namespaces
    // (`_idx/...`, `_idx_meta/...`) are filtered out via
    // `is_user_visible_space` so they don't pollute SpaceIndex.
    let mut puts: Vec<(Key, Value)> = Vec::new();
    let mut deletes: Vec<Key> = Vec::new();
    let mut spaces_touched: HashSet<String> = HashSet::new();

    for action in &filtered_actions {
        spaces_touched.insert(action.space.clone());
    }
    for space in &spaces_touched {
        if space != "default" && is_user_visible_space(space) {
            space_index.register(target_id, space)?;
        }
    }

    // OCC validation is enforced for user-visible namespaces only;
    // internal namespaces (`_idx/...`, `_idx_meta/...`) are derived
    // from the doc-level actions whose OCC IS validated. See the
    // matching comment in `merge_branches` for the rationale.
    let mut expected_targets: Vec<(Key, Option<Value>)> = Vec::new();
    for action in &filtered_actions {
        let ns = Arc::new(Namespace::for_branch_space(target_id, &action.space));
        let key = Key::new(ns, action.type_tag, action.raw_key.clone());
        if is_user_visible_space(&action.space) {
            expected_targets.push((key.clone(), action.expected_target.clone()));
        }
        match &action.action {
            MergeActionKind::Put(value) => {
                puts.push((key, value.clone()));
            }
            MergeActionKind::Delete => {
                deletes.push(key);
            }
        }
    }

    let keys_applied = puts.len() as u64;
    let keys_deleted = deletes.len() as u64;
    let mut cherry_pick_version = None;

    if !puts.is_empty() || !deletes.is_empty() {
        let ((), version) = db.transaction_with_version(target_id, |txn| {
            if let Some(expected) = expected_source_ref {
                verify_expected_active_ref_in_txn(txn, source, expected)?;
            }
            if let Some(expected) = expected_target_ref {
                verify_expected_active_ref_in_txn(txn, target, expected)?;
            }
            // Validate target hasn't changed since diff snapshot (#1917)
            for (key, expected) in &expected_targets {
                let current = txn.get(key)?;
                if current.as_ref() != expected.as_ref() {
                    return Err(StrataError::conflict(
                        "cherry-pick target concurrently modified",
                    ));
                }
            }
            for (key, value) in &puts {
                txn.put(key.clone(), value.clone())?;
            }
            for key in &deletes {
                txn.delete(key.clone())?;
            }
            Ok(())
        })?;
        cherry_pick_version = Some(version);
    }

    // Per-primitive post-commit handlers — symmetric with merge_branches.
    // - Vector: rebuilds HNSW backends for collections that `plan`
    //   recorded as affected.
    // - JSON: refreshes secondary index entries and BM25 inverted-index
    //   entries for documents `plan` recorded as affected.
    //
    // Without this dispatch, the cherry-picked rows would land in KV
    // correctly but derived state would be stale until the next full
    // recovery.
    //
    // Note: the affected set is populated from the UNFILTERED plan
    // actions, so a cherry-pick that filters out a doc/collection's
    // writes entirely will still trigger a redundant refresh for that
    // doc/collection. The refresh reads from KV (which is unchanged for
    // filtered rows) so it's wasted work but produces correct state.
    let post_ctx = MergePostCommitCtx {
        db,
        source_id,
        target_id,
        merge_version: cherry_pick_version,
    };
    for &tag in &DATA_TYPE_TAGS {
        registry.get(tag).post_commit(&post_ctx)?;
    }

    // Reload secondary index backends — preserved for any RefreshHook
    // that hasn't migrated to per-handler `post_commit` yet (currently
    // a no-op for the registered VectorRefreshHook).
    reload_secondary_backends(db, target_id, source_id);

    info!(
        target: "strata::branch_ops",
        source,
        target,
        keys_applied,
        keys_deleted,
        ?cherry_pick_version,
        "Cherry-picked from diff"
    );

    let info = CherryPickInfo {
        source: source.to_string(),
        target: target.to_string(),
        keys_applied,
        keys_deleted,
        cherry_pick_version,
    };

    dispatch_cherry_pick_hook(db, source, target, &info);

    Ok(info)
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
        // Go through BranchService so the canonical BranchControlRecord
        // is written alongside the legacy metadata — merge_base reads
        // from the control store after B3.3.
        db.branches().create(name).unwrap();
        (temp_dir, db)
    }

    // Test-only low-level helpers. B4.3 deleted the public no-metadata
    // `fork_branch` / `merge_branches` / `revert_version_range` wrappers
    // from the module surface; the characterization tests below exercise
    // the underlying `_with_metadata` helpers directly, so we re-introduce
    // the convenience wrappers here scoped to the test module.
    fn fork_branch(db: &Arc<Database>, source: &str, destination: &str) -> StrataResult<ForkInfo> {
        let dest_gen = resolve_fork_generation(db, destination)?;
        fork_branch_with_metadata(db, source, destination, None, None, dest_gen)
    }

    fn merge_branches(
        db: &Arc<Database>,
        source: &str,
        target: &str,
        strategy: MergeStrategy,
    ) -> StrataResult<MergeInfo> {
        merge_branches_with_metadata(db, source, target, strategy, None, None)
    }

    fn revert_version_range(
        db: &Arc<Database>,
        branch: &str,
        from_version: CommitVersion,
        to_version: CommitVersion,
    ) -> StrataResult<RevertInfo> {
        revert_version_range_with_metadata(db, branch, from_version, to_version, None, None)
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
        db.branches().create("dest").unwrap();

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
        db.branches().create("b").unwrap();

        let diff = diff_branches(&db, "a", "b").unwrap();
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    #[test]
    fn test_diff_one_populated() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        // Fork target → source, both modify "shared", source adds "new_key"
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "shared", Value::Int(1));

        fork_branch(&db, "target", "source").unwrap();

        // Both modify "shared" (conflict under three-way: both changed from ancestor=1)
        write_kv(&db, "target", "default", "shared", Value::Int(10));
        write_kv(&db, "source", "default", "shared", Value::Int(2));
        // Source adds a new key
        write_kv(
            &db,
            "source",
            "default",
            "new_key",
            Value::String("new".into()),
        );

        let info = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();
        assert!(
            info.keys_applied >= 2,
            "shared (conflict resolved) + new_key"
        );

        // LWW: source wins on conflict → shared=2
        assert_eq!(
            read_kv(&db, "target", "default", "shared"),
            Some(Value::Int(2))
        );
        assert_eq!(
            read_kv(&db, "target", "default", "new_key"),
            Some(Value::String("new".into()))
        );
    }

    #[test]
    fn test_merge_strict_no_conflicts() {
        // Fork target → source, each writes a different key (no conflict)
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "base_key", Value::Int(0)); // ensure branch in storage
        fork_branch(&db, "target", "source").unwrap();

        write_kv(&db, "target", "default", "target_key", Value::Int(1));
        write_kv(&db, "source", "default", "source_key", Value::Int(2));

        let info = merge_branches(&db, "source", "target", MergeStrategy::Strict).unwrap();
        assert!(info.keys_applied >= 1);

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
        // Fork target → source, both modify same key → Strict fails
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "shared", Value::Int(1));

        fork_branch(&db, "target", "source").unwrap();

        write_kv(&db, "target", "default", "shared", Value::Int(10));
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
            Some(Value::Int(10))
        );
    }

    #[test]
    fn test_merge_preserves_target_additions() {
        // Fork target → source, target adds a key, source adds a different key
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "base_key", Value::Int(0)); // ensure branch in storage
        fork_branch(&db, "target", "source").unwrap();

        write_kv(&db, "target", "default", "target_only", Value::Int(1));
        write_kv(&db, "source", "default", "source_only", Value::Int(2));

        merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();

        // Both keys should exist on target
        assert_eq!(
            read_kv(&db, "target", "default", "target_only"),
            Some(Value::Int(1))
        );
        assert_eq!(
            read_kv(&db, "target", "default", "source_only"),
            Some(Value::Int(2))
        );
    }

    #[test]
    fn test_merge_preserves_target_version_history() {
        // Fork target → source, target writes multiple versions, source overwrites
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "key", Value::Int(1));

        fork_branch(&db, "target", "source").unwrap();

        // Target writes more versions
        write_kv(&db, "target", "default", "key", Value::Int(2));

        // Source has different value
        write_kv(&db, "source", "default", "key", Value::Int(99));

        // Check version history before merge
        let target_id = resolve_branch_name("target");
        let ns = Arc::new(Namespace::for_branch_space(target_id, "default"));
        let history_key = Key::new(ns, TypeTag::KV, b"key".to_vec());
        let history_before = db.get_history(&history_key, None, None).unwrap();
        let versions_before = history_before.len();

        // Merge (both modified from ancestor=1 → conflict, LWW takes source=99)
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
        // Fork target → source, source writes a binary key (event)
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "base_key", Value::Int(0)); // ensure branch in storage
        fork_branch(&db, "target", "source").unwrap();

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
        // Fork target → source, both modify "shared" → LWW resolves but reports
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "shared", Value::Int(1));

        fork_branch(&db, "target", "source").unwrap();

        write_kv(&db, "target", "default", "shared", Value::Int(10));
        write_kv(&db, "source", "default", "shared", Value::Int(2));
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
        assert_eq!(info.conflicts[0].source_value, Some(Value::Int(2)));
        assert_eq!(info.conflicts[0].target_value, Some(Value::Int(10)));
    }

    #[test]
    fn test_merge_no_vectors_is_noop() {
        // Merge with only KV data (no vectors) should not error
        let (_temp, db) = setup_with_branch("target");
        write_kv(&db, "target", "default", "base_key", Value::Int(0)); // ensure branch in storage
        fork_branch(&db, "target", "source").unwrap();

        write_kv(&db, "source", "default", "k1", Value::String("v1".into()));

        let info = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins).unwrap();
        assert!(info.keys_applied >= 1);
        assert_eq!(
            read_kv(&db, "target", "default", "k1"),
            Some(Value::String("v1".into()))
        );
    }

    #[test]
    fn test_merge_unrelated_branches_errors() {
        // Two branches with no fork relationship should fail
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        db.branches().create("b").unwrap();

        write_kv(&db, "a", "default", "k1", Value::Int(1));
        write_kv(&db, "b", "default", "k2", Value::Int(2));

        let result = merge_branches(&db, "a", "b", MergeStrategy::LastWriterWins);
        assert!(result.is_err(), "Merging unrelated branches should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no fork or merge relationship"),
            "Error: {}",
            err
        );
    }

    // =========================================================================
    // DiffOptions Tests (filtering + as_of)
    // =========================================================================

    #[test]
    fn test_diff_filter_by_primitive_kv_only() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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
        db.branches().create("b").unwrap();

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

        // Fork — control-store `ForkAnchor` persists after materialization
        // so merge_base survives storage-layer flattening.
        let _fork_info = fork_branch(&db, "source", "dest").unwrap();

        // Write different data to dest
        write_kv(&db, "dest", "default", "dest_only", Value::Int(42));

        // Materialize dest — this removes inherited layers from storage,
        // but the `BranchControlStore` fork anchor survives so
        // merge_base is still computable.
        let mat_info = materialize_branch(&db, "dest").unwrap();
        assert!(mat_info.layers_collapsed > 0);

        // Diff should still work
        let diff = diff_branches(&db, "source", "dest").unwrap();
        assert_eq!(
            diff.summary.total_added, 1,
            "dest_only should show as added in dest"
        );

        // Merge dest → source: B3.3 derives merge base from the control
        // store's persisted `ForkAnchor`, so the merge succeeds without
        // a caller-supplied override even though storage's fork info
        // was flattened by materialization.
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

        // Drain any background compaction chain left over from the setup
        // writes. `schedule_background_compaction` deduplicates via the
        // `compaction_in_flight` flag, and the chain only calls
        // `run_materialization` at `idle_count == 0`. If we skip this drain,
        // under heavy parallel test contention (feature-matrix CI) the leftover
        // chain is still spinning idle rounds when our trigger write fires,
        // the trigger's `schedule_background_compaction` is a no-op, and
        // materialization never runs for the newly-built fork chain — see
        // `database/transaction.rs:compaction_round` for the idle-round state
        // machine. Draining here guarantees the flag is clear before we trigger,
        // so the next chain starts fresh at `idle_count == 0`.
        db.scheduler().drain();

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
            db.branches().create("main").unwrap();

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
        db.branches().create("source").unwrap();

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
        db.branches().create("main").unwrap();

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

    // =========================================================================
    // Three-Way Merge: classify_change unit tests
    // =========================================================================

    #[test]
    fn test_classify_all_absent() {
        assert_eq!(classify_change(None, None, None), ThreeWayChange::Unchanged);
    }

    #[test]
    fn test_classify_all_same() {
        let v = Value::Int(1);
        assert_eq!(
            classify_change(Some(&v), Some(&v), Some(&v)),
            ThreeWayChange::Unchanged
        );
    }

    #[test]
    fn test_classify_source_changed() {
        let a = Value::Int(1);
        let s = Value::Int(2);
        assert_eq!(
            classify_change(Some(&a), Some(&s), Some(&a)),
            ThreeWayChange::SourceChanged {
                value: Value::Int(2)
            }
        );
    }

    #[test]
    fn test_classify_target_changed() {
        let a = Value::Int(1);
        let t = Value::Int(2);
        assert_eq!(
            classify_change(Some(&a), Some(&a), Some(&t)),
            ThreeWayChange::TargetChanged
        );
    }

    #[test]
    fn test_classify_both_changed_same() {
        let a = Value::Int(1);
        let v = Value::Int(2);
        assert_eq!(
            classify_change(Some(&a), Some(&v), Some(&v)),
            ThreeWayChange::BothChangedSame
        );
    }

    #[test]
    fn test_classify_conflict() {
        let a = Value::Int(1);
        let s = Value::Int(2);
        let t = Value::Int(3);
        assert_eq!(
            classify_change(Some(&a), Some(&s), Some(&t)),
            ThreeWayChange::Conflict {
                source_value: Value::Int(2),
                target_value: Value::Int(3),
            }
        );
    }

    #[test]
    fn test_classify_source_added() {
        let s = Value::Int(1);
        assert_eq!(
            classify_change(None, Some(&s), None),
            ThreeWayChange::SourceAdded {
                value: Value::Int(1)
            }
        );
    }

    #[test]
    fn test_classify_target_added() {
        let t = Value::Int(1);
        assert_eq!(
            classify_change(None, None, Some(&t)),
            ThreeWayChange::TargetAdded
        );
    }

    #[test]
    fn test_classify_both_added_same() {
        let v = Value::Int(1);
        assert_eq!(
            classify_change(None, Some(&v), Some(&v)),
            ThreeWayChange::BothAddedSame
        );
    }

    #[test]
    fn test_classify_both_added_different() {
        let s = Value::Int(1);
        let t = Value::Int(2);
        assert_eq!(
            classify_change(None, Some(&s), Some(&t)),
            ThreeWayChange::BothAddedDifferent {
                source_value: Value::Int(1),
                target_value: Value::Int(2),
            }
        );
    }

    #[test]
    fn test_classify_source_deleted() {
        let a = Value::Int(1);
        assert_eq!(
            classify_change(Some(&a), None, Some(&a)),
            ThreeWayChange::SourceDeleted
        );
    }

    #[test]
    fn test_classify_target_deleted() {
        let a = Value::Int(1);
        assert_eq!(
            classify_change(Some(&a), Some(&a), None),
            ThreeWayChange::TargetDeleted
        );
    }

    #[test]
    fn test_classify_both_deleted() {
        let a = Value::Int(1);
        assert_eq!(
            classify_change(Some(&a), None, None),
            ThreeWayChange::BothDeleted
        );
    }

    #[test]
    fn test_classify_delete_modify_conflict() {
        let a = Value::Int(1);
        let t = Value::Int(2);
        assert_eq!(
            classify_change(Some(&a), None, Some(&t)),
            ThreeWayChange::DeleteModifyConflict {
                target_value: Value::Int(2),
            }
        );
    }

    #[test]
    fn test_classify_modify_delete_conflict() {
        let a = Value::Int(1);
        let s = Value::Int(2);
        assert_eq!(
            classify_change(Some(&a), Some(&s), None),
            ThreeWayChange::ModifyDeleteConflict {
                source_value: Value::Int(2),
            }
        );
    }

    // =========================================================================
    // Three-Way Merge: integration tests
    // =========================================================================

    fn delete_kv(db: &Arc<Database>, branch: &str, space: &str, key: &str) {
        let branch_id = resolve_branch_name(branch);
        let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
        db.transaction(branch_id, |txn| {
            txn.delete(Key::new(ns.clone(), TypeTag::KV, key.as_bytes().to_vec()))?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_three_way_merge_delete_propagation() {
        // #1537: Key deleted on source after fork → should be deleted on target after merge
        let (_temp, db) = setup_with_branch("parent");

        write_kv(&db, "parent", "default", "a", Value::Int(1));
        write_kv(&db, "parent", "default", "b", Value::Int(2));

        fork_branch(&db, "parent", "child").unwrap();

        // Child deletes "b"
        delete_kv(&db, "child", "default", "b");

        // Merge child → parent
        let info = merge_branches(&db, "child", "parent", MergeStrategy::LastWriterWins).unwrap();
        assert!(info.keys_deleted >= 1, "should have deleted at least 1 key");

        // Verify: "a" still exists, "b" is gone
        assert_eq!(read_kv(&db, "parent", "default", "a"), Some(Value::Int(1)));
        assert_eq!(read_kv(&db, "parent", "default", "b"), None);
    }

    #[test]
    fn test_three_way_merge_preserves_target_only_changes() {
        // #1692: Key added on target after fork → should NOT be deleted by merge
        let (_temp, db) = setup_with_branch("parent");

        write_kv(&db, "parent", "default", "a", Value::Int(1));

        fork_branch(&db, "parent", "child").unwrap();

        // Parent adds "b" after fork
        write_kv(&db, "parent", "default", "b", Value::Int(2));

        // Child does nothing. Merge child → parent.
        let info = merge_branches(&db, "child", "parent", MergeStrategy::LastWriterWins).unwrap();
        assert_eq!(info.keys_deleted, 0, "nothing should be deleted");

        // Verify: both "a" and "b" still exist on parent
        assert_eq!(read_kv(&db, "parent", "default", "a"), Some(Value::Int(1)));
        assert_eq!(read_kv(&db, "parent", "default", "b"), Some(Value::Int(2)));
    }

    #[test]
    fn test_three_way_merge_disjoint_changes() {
        // Disjoint changes on parent and child merge cleanly
        let (_temp, db) = setup_with_branch("parent");

        write_kv(&db, "parent", "default", "a", Value::Int(1));
        write_kv(&db, "parent", "default", "b", Value::Int(2));
        write_kv(&db, "parent", "default", "c", Value::Int(3));

        fork_branch(&db, "parent", "child").unwrap();

        // Parent changes a, child changes b
        write_kv(&db, "parent", "default", "a", Value::Int(10));
        write_kv(&db, "child", "default", "b", Value::Int(20));

        // Merge child → parent
        merge_branches(&db, "child", "parent", MergeStrategy::LastWriterWins).unwrap();

        // Verify: a=10 (parent's change preserved), b=20 (child's change applied), c=3 (unchanged)
        assert_eq!(read_kv(&db, "parent", "default", "a"), Some(Value::Int(10)));
        assert_eq!(read_kv(&db, "parent", "default", "b"), Some(Value::Int(20)));
        assert_eq!(read_kv(&db, "parent", "default", "c"), Some(Value::Int(3)));
    }

    #[test]
    fn test_three_way_merge_strict_conflict() {
        // Both-modified conflict under Strict mode should fail
        let (_temp, db) = setup_with_branch("parent");

        write_kv(&db, "parent", "default", "a", Value::Int(1));

        fork_branch(&db, "parent", "child").unwrap();

        // Both sides change "a" to different values
        write_kv(&db, "parent", "default", "a", Value::Int(10));
        write_kv(&db, "child", "default", "a", Value::Int(20));

        // Strict merge should fail
        let result = merge_branches(&db, "child", "parent", MergeStrategy::Strict);
        assert!(result.is_err(), "Strict merge should fail on conflict");

        // Parent's value should be unchanged
        assert_eq!(read_kv(&db, "parent", "default", "a"), Some(Value::Int(10)));

        // LWW merge should succeed with source's value
        let info = merge_branches(&db, "child", "parent", MergeStrategy::LastWriterWins).unwrap();
        assert!(info.keys_applied >= 1);
        assert_eq!(read_kv(&db, "parent", "default", "a"), Some(Value::Int(20)));
    }

    #[test]
    fn test_three_way_merge_returns_merge_version() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "a", Value::Int(1));

        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "b", Value::Int(2));

        let info = merge_branches(&db, "child", "parent", MergeStrategy::LastWriterWins).unwrap();
        assert!(
            info.merge_version.is_some(),
            "merge should return a version"
        );
    }

    // =========================================================================
    // Tag Tests
    // =========================================================================

    #[test]
    fn test_tag_create_and_resolve() {
        let (_temp, db) = setup_with_branch("main");
        write_kv(&db, "main", "default", "k", Value::Int(1));

        let tag = create_tag(&db, "main", "v1.0", None, Some("release"), None).unwrap();
        assert_eq!(tag.name, "v1.0");
        assert_eq!(tag.branch, "main");
        assert!(tag.version > 0);

        let resolved = resolve_tag(&db, "main", "v1.0").unwrap();
        assert_eq!(resolved, Some(tag));
    }

    #[test]
    fn test_tag_list_and_delete() {
        let (_temp, db) = setup_with_branch("main");
        write_kv(&db, "main", "default", "k", Value::Int(1));

        create_tag(&db, "main", "t1", None, None, None).unwrap();
        create_tag(&db, "main", "t2", None, None, None).unwrap();

        let tags = list_tags(&db, "main").unwrap();
        assert_eq!(tags.len(), 2);

        let deleted = delete_tag(&db, "main", "t1").unwrap();
        assert!(deleted);

        let tags = list_tags(&db, "main").unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].name, "t2");
    }

    #[test]
    fn test_tag_with_expected_rejects_recreated_branch_generation() {
        let (_temp, db) = setup_with_branch("main");
        let store = BranchControlStore::new(db.clone());
        let stale_ref = store.require_writable_by_name("main").unwrap().branch;

        db.branches().delete("main").unwrap();
        db.branches().create("main").unwrap();

        let err = create_tag_with_expected(&db, "main", "v1.0", None, None, None, Some(stale_ref))
            .unwrap_err();
        assert!(
            matches!(err, StrataError::Conflict { .. }),
            "recreated branch must reject stale expected ref, got {err:?}"
        );
    }

    #[test]
    fn test_delete_clears_tags_before_same_name_recreate() {
        let (_temp, db) = setup_with_branch("main");
        create_tag(&db, "main", "v1.0", None, None, None).unwrap();

        db.branches().delete("main").unwrap();
        db.branches().create("main").unwrap();

        assert!(
            list_tags(&db, "main").unwrap().is_empty(),
            "recreated branch must not inherit tags from the prior lifecycle"
        );

        create_tag(&db, "main", "v1.0", None, None, None)
            .expect("same-name recreate must allow reusing a deleted lifecycle's tag name");
    }

    // =========================================================================
    // Note Tests
    // =========================================================================

    #[test]
    fn test_note_add_and_get() {
        let (_temp, db) = setup_with_branch("main");
        write_kv(&db, "main", "default", "k", Value::Int(1));

        let note = add_note(
            &db,
            "main",
            CommitVersion(1),
            "initial state",
            Some("ai"),
            None,
        )
        .unwrap();
        assert_eq!(note.message, "initial state");
        assert_eq!(note.version, 1);

        let notes = get_notes(&db, "main", None).unwrap();
        assert_eq!(notes.len(), 1);

        let notes_at_v = get_notes(&db, "main", Some(1)).unwrap();
        assert_eq!(notes_at_v.len(), 1);
    }

    #[test]
    fn test_note_delete() {
        let (_temp, db) = setup_with_branch("main");

        add_note(&db, "main", CommitVersion(1), "note1", None, None).unwrap();
        add_note(&db, "main", CommitVersion(2), "note2", None, None).unwrap();

        assert!(delete_note(&db, "main", CommitVersion(1)).unwrap());

        let notes = get_notes(&db, "main", None).unwrap();
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].version, 2);
    }

    #[test]
    fn test_delete_note_with_expected_rejects_recreated_branch_generation() {
        let (_temp, db) = setup_with_branch("main");
        add_note(&db, "main", CommitVersion(1), "note1", None, None).unwrap();

        let store = BranchControlStore::new(db.clone());
        let stale_ref = store.require_writable_by_name("main").unwrap().branch;

        db.branches().delete("main").unwrap();
        db.branches().create("main").unwrap();

        let err =
            delete_note_with_expected(&db, "main", CommitVersion(1), Some(stale_ref)).unwrap_err();
        assert!(
            matches!(err, StrataError::Conflict { .. }),
            "recreated branch must reject stale expected ref, got {err:?}"
        );
    }

    #[test]
    fn test_delete_clears_notes_before_same_name_recreate() {
        let (_temp, db) = setup_with_branch("main");
        add_note(&db, "main", CommitVersion(1), "note1", None, None).unwrap();

        db.branches().delete("main").unwrap();
        db.branches().create("main").unwrap();

        assert!(
            get_notes(&db, "main", None).unwrap().is_empty(),
            "recreated branch must not inherit notes from the prior lifecycle"
        );

        add_note(&db, "main", CommitVersion(1), "note1", None, None)
            .expect("same-name recreate must allow reusing a deleted lifecycle's note slot");
    }

    // =========================================================================
    // Three-Way Diff API Tests
    // =========================================================================

    #[test]
    fn test_diff_three_way_disjoint_changes() {
        // Fork, make disjoint changes, verify classification
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "a", Value::Int(1));
        write_kv(&db, "parent", "default", "b", Value::Int(2));

        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "parent", "default", "a", Value::Int(10));
        write_kv(&db, "child", "default", "b", Value::Int(20));

        let result = diff_three_way(&db, "child", "parent").unwrap();

        // Should see: a=TargetChanged, b=SourceChanged
        let a_entry = result.entries.iter().find(|e| e.key == "a").unwrap();
        assert!(matches!(a_entry.change, ThreeWayChange::TargetChanged));

        let b_entry = result.entries.iter().find(|e| e.key == "b").unwrap();
        assert!(matches!(
            b_entry.change,
            ThreeWayChange::SourceChanged { .. }
        ));
    }

    #[test]
    fn test_get_merge_base() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "a", Value::Int(1));

        let fork_info = fork_branch(&db, "parent", "child").unwrap();

        let base = get_merge_base(&db, "child", "parent").unwrap();
        assert!(base.is_some());
        let base = base.unwrap();
        assert_eq!(base.version, CommitVersion(fork_info.fork_version.unwrap()));
    }

    #[test]
    fn test_get_merge_base_unrelated() {
        let (_temp, db) = setup_with_branch("a");
        let branch_index = BranchIndex::new(db.clone());
        db.branches().create("b").unwrap();

        let base = get_merge_base(&db, "a", "b").unwrap();
        assert!(base.is_none());
    }

    // =========================================================================
    // Revert Tests
    // =========================================================================

    #[test]
    fn test_revert_single_version() {
        let (_temp, db) = setup_with_branch("main");

        write_kv(&db, "main", "default", "a", Value::Int(1)); // v1
        write_kv(&db, "main", "default", "a", Value::Int(2)); // v2
        write_kv(&db, "main", "default", "b", Value::Int(10)); // v3

        // Get current version
        let current = db.current_version();

        // Revert the last write (v3 = b:10)
        let info = revert_version_range(&db, "main", current, current).unwrap();
        assert_eq!(info.keys_reverted, 1); // only "b" was changed in that version

        // "a" should still be 2 (unchanged in range)
        assert_eq!(read_kv(&db, "main", "default", "a"), Some(Value::Int(2)));
        // "b" should be gone (didn't exist before v3)
        assert_eq!(read_kv(&db, "main", "default", "b"), None);
    }

    #[test]
    fn test_revert_preserves_subsequent_work() {
        let (_temp, db) = setup_with_branch("main");

        write_kv(&db, "main", "default", "a", Value::Int(1)); // v_start
        let v_start = db.current_version().as_u64();
        write_kv(&db, "main", "default", "a", Value::Int(2)); // v_change
        let v_change = db.current_version();
        write_kv(&db, "main", "default", "a", Value::Int(3)); // v_after (after range)

        // Revert [v_start+1, v_change] — but "a" was modified after the range
        let info = revert_version_range(&db, "main", CommitVersion(v_start + 1), v_change).unwrap();
        assert_eq!(
            info.keys_reverted, 0,
            "should skip keys modified after range"
        );

        // "a" should still be 3 (subsequent work preserved)
        assert_eq!(read_kv(&db, "main", "default", "a"), Some(Value::Int(3)));
    }

    #[test]
    fn test_revert_invalid_range() {
        let (_temp, db) = setup_with_branch("main");

        // from_version > to_version
        let result = revert_version_range(&db, "main", CommitVersion(5), CommitVersion(3));
        assert!(result.is_err());

        // from_version == 0
        let result = revert_version_range(&db, "main", CommitVersion(0), CommitVersion(3));
        assert!(result.is_err());
    }

    #[test]
    fn test_revert_restores_deleted_key() {
        let (_temp, db) = setup_with_branch("main");
        write_kv(&db, "main", "default", "a", Value::Int(1));
        let v_before = db.current_version().as_u64();

        // Delete "a" in the revert range
        delete_kv(&db, "main", "default", "a");
        let v_delete = db.current_version();

        // Verify it's gone
        assert_eq!(read_kv(&db, "main", "default", "a"), None);

        // Revert the deletion
        let info =
            revert_version_range(&db, "main", CommitVersion(v_before + 1), v_delete).unwrap();
        assert_eq!(info.keys_reverted, 1);

        // Key should be restored
        assert_eq!(read_kv(&db, "main", "default", "a"), Some(Value::Int(1)));
    }

    // =========================================================================
    // Cherry-Pick Tests
    // =========================================================================

    #[test]
    fn test_cherry_pick_specific_keys() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "a", Value::Int(1));
        write_kv(&db, "parent", "default", "b", Value::Int(2));

        fork_branch(&db, "parent", "source").unwrap();
        write_kv(&db, "source", "default", "a", Value::Int(10));
        write_kv(&db, "source", "default", "b", Value::Int(20));
        write_kv(&db, "source", "default", "c", Value::Int(30));

        // Cherry-pick only "a" from source to parent
        let info = cherry_pick_keys(
            &db,
            "source",
            "parent",
            &[("default".to_string(), "a".to_string())],
        )
        .unwrap();
        assert_eq!(info.keys_applied, 1);

        // "a" should have source's value
        assert_eq!(read_kv(&db, "parent", "default", "a"), Some(Value::Int(10)));
        // "b" should be unchanged (not cherry-picked)
        assert_eq!(read_kv(&db, "parent", "default", "b"), Some(Value::Int(2)));
        // "c" should not exist on parent
        assert_eq!(read_kv(&db, "parent", "default", "c"), None);
    }

    #[test]
    fn test_cherry_pick_from_diff_with_filter() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "a", Value::Int(1));
        write_kv(&db, "parent", "default", "b", Value::Int(2));

        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "a", Value::Int(10));
        write_kv(&db, "child", "default", "b", Value::Int(20));
        write_kv(&db, "child", "default", "c", Value::Int(30));

        // Cherry-pick from diff, only key "b"
        let filter = CherryPickFilter {
            keys: Some(vec!["b".to_string()]),
            spaces: None,
            primitives: None,
        };
        let info = cherry_pick_from_diff(&db, "child", "parent", filter).unwrap();
        assert_eq!(info.keys_applied, 1);

        // Only "b" should be changed
        assert_eq!(read_kv(&db, "parent", "default", "a"), Some(Value::Int(1)));
        assert_eq!(read_kv(&db, "parent", "default", "b"), Some(Value::Int(20)));
    }

    #[test]
    fn test_cherry_pick_empty_keys() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "a", Value::Int(1));

        fork_branch(&db, "parent", "source").unwrap();
        write_kv(&db, "source", "default", "a", Value::Int(10));

        // Cherry-pick with no matching keys
        let info = cherry_pick_keys(
            &db,
            "source",
            "parent",
            &[("default".to_string(), "nonexistent".to_string())],
        )
        .unwrap();
        assert_eq!(info.keys_applied, 0);
        assert!(info.cherry_pick_version.is_none());
    }

    // =========================================================================
    // COW-Aware Diff Tests
    // =========================================================================

    #[test]
    fn test_cow_diff_child_adds_key() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "existing", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "new_key", Value::Int(42));

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_added, 1);
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_modified, 0);

        let space = &diff.spaces[0];
        assert_eq!(space.added[0].key, "new_key");
        assert!(space.added[0].value_a.is_none());
        assert_eq!(space.added[0].value_b, Some(Value::Int(42)));
    }

    #[test]
    fn test_cow_diff_child_modifies_key() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "k", Value::Int(2));

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_modified, 1);
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);

        let space = &diff.spaces[0];
        assert_eq!(space.modified[0].key, "k");
        assert_eq!(space.modified[0].value_a, Some(Value::Int(1)));
        assert_eq!(space.modified[0].value_b, Some(Value::Int(2)));
    }

    #[test]
    fn test_cow_diff_child_deletes_key() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        delete_kv(&db, "child", "default", "k");

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_removed, 1);
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_modified, 0);

        let space = &diff.spaces[0];
        assert_eq!(space.removed[0].key, "k");
        assert_eq!(space.removed[0].value_a, Some(Value::Int(1)));
        assert!(space.removed[0].value_b.is_none());
    }

    #[test]
    fn test_cow_diff_parent_modifies_after_fork() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        // Parent modifies after fork
        write_kv(&db, "parent", "default", "k", Value::Int(99));

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_modified, 1);

        let space = &diff.spaces[0];
        assert_eq!(space.modified[0].value_a, Some(Value::Int(99))); // parent's new value
        assert_eq!(space.modified[0].value_b, Some(Value::Int(1))); // child still has old
    }

    #[test]
    fn test_cow_diff_both_modify_same_key() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "parent", "default", "k", Value::Int(10));
        write_kv(&db, "child", "default", "k", Value::Int(20));

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_modified, 1);

        let space = &diff.spaces[0];
        assert_eq!(space.modified[0].value_a, Some(Value::Int(10)));
        assert_eq!(space.modified[0].value_b, Some(Value::Int(20)));
    }

    #[test]
    fn test_cow_diff_child_writes_same_value() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        // Child writes same value — should NOT appear in diff
        write_kv(&db, "child", "default", "k", Value::Int(1));

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    #[test]
    fn test_cow_diff_child_deletes_nonexistent() {
        let (_temp, db) = setup_with_branch("parent");
        // Parent must have at least one key for fork to succeed
        write_kv(&db, "parent", "default", "exists", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        // Child deletes a key that doesn't exist in parent — should NOT appear
        delete_kv(&db, "child", "default", "phantom");

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    #[test]
    fn test_cow_diff_directionality() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "new", Value::Int(2));

        // diff(parent, child): "new" is added (in child/b, not parent/a)
        let diff_pc = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff_pc.summary.total_added, 1);
        assert_eq!(diff_pc.summary.total_removed, 0);

        // diff(child, parent): "new" is removed (in child/a, not parent/b)
        let diff_cp = diff_branches(&db, "child", "parent").unwrap();
        assert_eq!(diff_cp.summary.total_added, 0);
        assert_eq!(diff_cp.summary.total_removed, 1);
    }

    #[test]
    fn test_cow_diff_with_space_filter() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k1", Value::Int(1));
        write_kv(&db, "parent", "other", "k2", Value::Int(2));
        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "k1", Value::Int(10));
        write_kv(&db, "child", "other", "k2", Value::Int(20));

        // Filter to "default" space only
        let diff = diff_branches_with_options(
            &db,
            "parent",
            "child",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: None,
                    spaces: Some(vec!["default".to_string()]),
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_modified, 1);
        assert_eq!(diff.spaces.len(), 1);
        assert_eq!(diff.spaces[0].space, "default");
    }

    #[test]
    fn test_cow_diff_with_primitive_filter() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "kv_key", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "kv_key", Value::Int(2));

        // Filter to KV only
        let diff = diff_branches_with_options(
            &db,
            "parent",
            "child",
            DiffOptions {
                filter: Some(DiffFilter {
                    primitives: Some(vec![PrimitiveType::Kv]),
                    spaces: None,
                }),
                as_of: None,
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_modified, 1);
    }

    #[test]
    fn test_cow_diff_as_of_falls_back() {
        // When as_of is set, COW fast path should not be used (falls back to full scan).
        // Verify correctness by checking the result matches expectations.
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        write_kv(&db, "child", "default", "k", Value::Int(2));

        // With as_of=0 (before any writes), diff should be empty
        let diff = diff_branches_with_options(
            &db,
            "parent",
            "child",
            DiffOptions {
                filter: None,
                as_of: Some(0),
            },
        )
        .unwrap();
        assert_eq!(diff.summary.total_modified, 0);
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);
    }

    #[test]
    fn test_cow_diff_no_changes() {
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        // No changes on child — diff should be empty
        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    /// Issue #1917: merge_branches() blind writes skip OCC validation,
    /// allowing concurrent target modifications to be silently overwritten.
    ///
    /// This test reproduces the race:
    /// - Target has key_A="old", source has key_A="new" (SourceChanged)
    /// - A concurrent writer modifies target[key_A]="concurrent_value"
    /// - merge_branches applies key_A="new" to target
    ///
    /// Before fix: merge blindly writes "new" (empty read_set → OCC skipped),
    ///   "concurrent_value" silently lost.
    /// After fix: merge reads target keys in apply transaction (populating
    ///   read_set), detects concurrent modification via OCC or validation.
    #[test]
    fn test_issue_1917_merge_concurrent_target_write() {
        use std::sync::Barrier;
        use std::thread;

        let (_temp, db) = setup_with_branch("target");
        write_kv(
            &db,
            "target",
            "default",
            "key_A",
            Value::String("old".into()),
        );

        // Add padding keys so the diff computation takes longer,
        // widening the window for the concurrent write to land between
        // the diff read and the apply.
        for i in 0..200 {
            write_kv(
                &db,
                "target",
                "default",
                &format!("pad_{:04}", i),
                Value::Int(i as i64),
            );
        }

        fork_branch(&db, "target", "source").unwrap();
        write_kv(
            &db,
            "source",
            "default",
            "key_A",
            Value::String("new".into()),
        );

        // Modify padding keys on source so diff has work to do
        for i in 0..200 {
            write_kv(
                &db,
                "source",
                "default",
                &format!("pad_{:04}", i),
                Value::Int(i as i64 + 1000),
            );
        }

        let mut silent_overwrites = 0u32;
        let iterations = 200;

        for _ in 0..iterations {
            // Reset target key_A to "old" (ancestor value)
            write_kv(
                &db,
                "target",
                "default",
                "key_A",
                Value::String("old".into()),
            );

            let db2 = db.clone();
            let barrier = Arc::new(Barrier::new(2));
            let b2 = barrier.clone();

            let writer = thread::spawn(move || {
                b2.wait();
                thread::yield_now();
                write_kv(
                    &db2,
                    "target",
                    "default",
                    "key_A",
                    Value::String("concurrent_value".into()),
                );
            });

            barrier.wait();
            let result = merge_branches(&db, "source", "target", MergeStrategy::LastWriterWins);
            writer.join().unwrap();

            // Both merge and concurrent write have committed at this point.
            let val = read_kv(&db, "target", "default", "key_A").unwrap();

            if let Ok(info) = result {
                // Merge succeeded. Check if the concurrent write was handled correctly:
                //
                // - If val == "new" AND conflicts includes key_A: the diff SAW the
                //   concurrent write (BothChanged) and resolved via LWW → correct.
                //
                // - If val == "new" AND conflicts is empty: the diff saw target as
                //   "old" (SourceChanged), but the concurrent write committed before
                //   the merge → silent overwrite (BUG #1917).
                //
                // - If val == "concurrent_value": concurrent write committed after
                //   the merge → correct (latest write wins).
                if val == Value::String("new".into()) && info.conflicts.is_empty() {
                    silent_overwrites += 1;
                }
            }
            // If result is Err: merge detected concurrent modification → correct.
        }

        // After fix: concurrent modifications are either seen during diff
        // (reported as conflicts) or detected during apply (error returned).
        // Silent overwrites should be zero.
        assert_eq!(
            silent_overwrites, 0,
            "Bug #1917: merge silently overwrote concurrent target modifications \
             {} times out of {} iterations. merge_branches() should detect \
             concurrent target writes via OCC validation, not use blind writes.",
            silent_overwrites, iterations
        );
    }

    /// Issue #1917 stress variant: two concurrent merges to the same target
    /// should not both succeed with blind writes. After fix, OCC detects
    /// the conflict when both transactions read and write the same target key.
    #[test]
    fn test_issue_1917_concurrent_merges_occ() {
        use std::sync::Barrier;
        use std::thread;

        let (_temp, db) = setup_with_branch("target");
        write_kv(
            &db,
            "target",
            "default",
            "shared",
            Value::String("ancestor".into()),
        );

        fork_branch(&db, "target", "source1").unwrap();
        fork_branch(&db, "target", "source2").unwrap();
        write_kv(
            &db,
            "source1",
            "default",
            "shared",
            Value::String("from_s1".into()),
        );
        write_kv(
            &db,
            "source2",
            "default",
            "shared",
            Value::String("from_s2".into()),
        );

        let mut both_succeeded = 0u32;
        let iterations = 200;

        for _ in 0..iterations {
            // Reset target to ancestor value
            write_kv(
                &db,
                "target",
                "default",
                "shared",
                Value::String("ancestor".into()),
            );

            let db1 = db.clone();
            let db2 = db.clone();
            let barrier = Arc::new(Barrier::new(2));
            let b1 = barrier.clone();
            let b2 = barrier.clone();

            let t1 = thread::spawn(move || {
                b1.wait();
                merge_branches(&db1, "source1", "target", MergeStrategy::LastWriterWins)
            });
            let t2 = thread::spawn(move || {
                b2.wait();
                merge_branches(&db2, "source2", "target", MergeStrategy::LastWriterWins)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            if r1.is_ok() && r2.is_ok() {
                both_succeeded += 1;
            }
        }

        // After fix: concurrent merges to the same target key should produce
        // OCC conflicts (both read and write the same key in their transactions).
        // Before fix: both always succeed (blind writes, empty read_set).
        assert!(
            both_succeeded < iterations,
            "Bug #1917: two concurrent merges to the same target key both succeeded \
             in all {} iterations. At least some should fail with OCC conflict when \
             merge transactions include target key reads.",
            iterations
        );
    }

    #[test]
    fn test_cow_diff_both_delete_same_key() {
        // Both branches delete the same key after fork → (None, None) → no diff
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "k", Value::Int(1));
        fork_branch(&db, "parent", "child").unwrap();
        delete_kv(&db, "child", "default", "k");
        delete_kv(&db, "parent", "default", "k");

        let diff = diff_branches(&db, "parent", "child").unwrap();
        assert_eq!(diff.summary.total_added, 0);
        assert_eq!(diff.summary.total_removed, 0);
        assert_eq!(diff.summary.total_modified, 0);
    }

    #[test]
    fn test_issue_1920_diff_branches_snapshot_consistency() {
        // Verify that diff_branches uses snapshot-isolated reads so that
        // concurrent writes don't produce an inconsistent diff.
        let (_temp, db) = setup();
        let branch_index = BranchIndex::new(db.clone());
        db.branches().create("snap-a").unwrap();
        db.branches().create("snap-b").unwrap();

        // Write different data to each branch
        write_kv(&db, "snap-a", "default", "shared", Value::Int(1));
        write_kv(&db, "snap-a", "default", "only-a", Value::Int(10));
        write_kv(&db, "snap-b", "default", "shared", Value::Int(2));
        write_kv(&db, "snap-b", "default", "only-b", Value::Int(20));

        // Diff should succeed with consistent snapshot reads
        let diff =
            diff_branches_with_options(&db, "snap-a", "snap-b", DiffOptions::default()).unwrap();

        // "only-a" removed (in A, not in B), "only-b" added (in B, not in A),
        // "shared" modified (different values)
        assert_eq!(diff.summary.total_removed, 1, "only-a should be removed");
        assert_eq!(diff.summary.total_added, 1, "only-b should be added");
        assert_eq!(diff.summary.total_modified, 1, "shared should be modified");
    }

    #[test]
    fn test_issue_1918_cow_diff_snapshot_consistency() {
        // Verify that the COW fast path (parent-child diff) uses snapshot-isolated
        // point lookups via get_versioned, not get_value_direct (#1918).
        //
        // We set up a parent-child fork, write to the child, then verify that
        // diff_branches (which hits the COW path) returns a consistent result
        // that matches the expected state.
        let (_temp, db) = setup_with_branch("parent");
        write_kv(&db, "parent", "default", "base", Value::Int(1));
        write_kv(&db, "parent", "default", "shared", Value::Int(10));

        fork_branch(&db, "parent", "child").unwrap();

        // Child modifies shared and adds a new key
        write_kv(&db, "child", "default", "shared", Value::Int(20));
        write_kv(&db, "child", "default", "child_only", Value::Int(99));

        // This diff should use the COW fast path (child is direct child of parent)
        let diff = diff_branches(&db, "parent", "child").unwrap();

        // shared: modified (10 → 20), child_only: added, base: unchanged
        assert_eq!(diff.summary.total_modified, 1, "shared should be modified");
        assert_eq!(diff.summary.total_added, 1, "child_only should be added");
        assert_eq!(diff.summary.total_removed, 0, "nothing removed");

        // Verify the values are snapshot-consistent
        let space = &diff.spaces[0];
        let all_entries: Vec<_> = space.added.iter().chain(space.modified.iter()).collect();
        for entry in &all_entries {
            if entry.key == "shared" {
                assert_eq!(entry.value_a, Some(Value::Int(10)));
                assert_eq!(entry.value_b, Some(Value::Int(20)));
            } else if entry.key == "child_only" {
                assert!(entry.value_a.is_none());
                assert_eq!(entry.value_b, Some(Value::Int(99)));
            }
        }
    }
}
