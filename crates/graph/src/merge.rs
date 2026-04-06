//! Semantic graph merge (Phase 3b of primitive-aware merge).
//!
//! See `docs/design/branching/primitive-aware-merge.md` §Phase 3b.
//!
//! ## Role
//!
//! Phase 3 (#2333) refused divergent graph branch merges with a tactical
//! check. That was strictly conservative — disjoint additions on both sides
//! of a fork were also rejected, even when the merge would have been
//! perfectly safe. This module replaces that refusal with a real semantic
//! merge:
//!
//! 1. **Decode** the packed adjacency lists from each side into logical
//!    edge sets, plus node records, catalog, meta, and "other" graph keys.
//! 2. **Project** the post-merge state by applying the standard 14-case
//!    decision matrix to nodes and edges separately, honoring `MergeStrategy`
//!    for cell-level conflicts.
//! 3. **Validate** referential integrity (no dangling endpoints, no orphan
//!    references after node deletion). Violations are *always* fatal.
//! 4. **Re-encode** the projected adjacency lists back into packed binary
//!    and emit puts/deletes only for affected keys.
//!
//! ## Layering
//!
//! This module is **pure**: no engine dispatch, no transaction context,
//! no `MergeAction`. The engine crate's `GraphMergeHandler::plan` calls
//! `compute_graph_merge` and converts the resulting `GraphMergeOutput`
//! into the engine's per-handler `PrimitiveMergePlan` shape.
//!
//! ## Phase 3c additions on top of Phase 3b
//!
//! - **Additive catalog merging.** `merge_catalog` decodes each side as a
//!   `BTreeSet<String>` and projects per-name presence. Concurrent
//!   independent `create_graph` calls now merge cleanly. The
//!   `CatalogDivergence` variant is reserved but no longer produced.
//! - **Cherry-pick semantic merge.** `cherry_pick_from_diff` now uses the
//!   same per-handler `plan` dispatch as `merge_branches`, so cherry-pick
//!   inherits the Phase 3b semantic merge and Phase 3c additive catalog.
//!
//! ## What Phase 3b/3c don't do
//!
//! - **Ontology semantic merge.** Ontology entries are merged as opaque KV
//!   via the 14-case matrix. A richer ontology-aware merge (additive type
//!   definitions, schema migration semantics) warrants its own design pass.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use strata_core::types::Key;
use strata_core::value::Value;
use strata_core::{StrataError, StrataResult, VersionedValue};
use strata_engine::{MergeStrategy, VersionedEntry};

use crate::keys;
use crate::packed;
use crate::types::{EdgeData, GraphMeta, NodeData};

// =============================================================================
// Public types
// =============================================================================

/// Decoded view of one side's graph state, partitioned by graph name.
///
/// Produced by [`decode_graph_cell_versioned`] (for the ancestor side, which
/// includes tombstones in the input but produces a tombstone-free decoded
/// view) and [`decode_graph_cell_live`] (for the source/target sides, which
/// receive live-only inputs).
#[derive(Debug, Default, Clone)]
pub struct GraphCellState {
    /// Per-graph state, keyed by graph name. Sorted for deterministic
    /// iteration during the merge.
    pub graphs: BTreeMap<String, PerGraphState>,
    /// Raw `__catalog__` value if present on this side. Decoded by
    /// `merge_catalog` as a JSON list of graph names and merged
    /// per-name additively (Phase 3c). `None` means "this side has not
    /// touched the catalog" (NOT "this side deleted everything").
    pub catalog: Option<Value>,
}

/// Per-graph decoded state for one side. The merge algorithm operates on
/// one of these per (graph_name, side) tuple.
#[derive(Debug, Default, Clone)]
pub struct PerGraphState {
    /// Live nodes, keyed by node_id.
    pub nodes: HashMap<String, NodeData>,
    /// Live edges, keyed by `(src, dst, edge_type)` triple. The triple is
    /// the edge identity; `EdgeData` is the edge value.
    pub edges: HashMap<EdgeKey, EdgeData>,
    /// Graph metadata if present.
    pub meta: Option<GraphMeta>,
    /// Other graph keys (ontology, ref index, type index, edge counters).
    /// Phase 3b merges these as opaque KV via the 14-case matrix; the
    /// catalog is handled separately at the `GraphCellState` level. The
    /// edge counter keys are intentionally INCLUDED here in the decoded
    /// view but are *overwritten* by the projected counters during the
    /// merge (we derive counts from the projected edge set).
    pub other: HashMap<Vec<u8>, Value>,
}

/// Identity of an edge: `(src_node_id, dst_node_id, edge_type)`. The
/// associated `EdgeData` is the edge's value, compared for equality
/// during the merge.
pub type EdgeKey = (String, String, String);

/// Result of `compute_graph_merge`. The puts/deletes are pairs of
/// `(user_key_within_graph_namespace, value)` ready to be wrapped in
/// `MergeAction` by the caller.
#[derive(Debug, Default)]
pub struct GraphMergeOutput {
    /// User-key string + value. The caller wraps each pair in a
    /// `Key::new_graph(..., user_key)` to build the storage Key.
    pub puts: Vec<(String, Value)>,
    /// User-key strings to delete. Same wrapping as puts.
    pub deletes: Vec<String>,
    /// Conflicts surfaced during the merge. Cell-level conflicts honor
    /// `MergeStrategy`; structural conflicts (referential integrity,
    /// catalog divergence) are always reported regardless of strategy.
    pub conflicts: Vec<GraphMergeConflict>,
}

/// A conflict surfaced by `compute_graph_merge`.
#[derive(Debug, Clone)]
pub enum GraphMergeConflict {
    /// Both branches modified a node's properties differently.
    NodePropertyConflict {
        graph: String,
        node_id: String,
        source: NodeData,
        target: NodeData,
    },
    /// Both branches modified an edge's data differently.
    EdgeDataConflict {
        graph: String,
        src: String,
        dst: String,
        edge_type: String,
        source: EdgeData,
        target: EdgeData,
    },
    /// One side modified a node, the other side deleted it.
    NodeModifyDeleteConflict { graph: String, node_id: String },
    /// One side modified an edge's data, the other side removed the edge.
    EdgeModifyDeleteConflict {
        graph: String,
        src: String,
        dst: String,
        edge_type: String,
    },
    /// Both branches modified the graph metadata differently.
    MetaConflict { graph: String },
    /// An edge in the projected merge result has an endpoint that doesn't
    /// exist in the projected node set. Always fatal regardless of strategy.
    DanglingEdge {
        graph: String,
        src: String,
        dst: String,
        edge_type: String,
        missing_node: String,
    },
    /// A node was deleted but the projected edge set still references it.
    /// Always fatal regardless of strategy.
    OrphanedReference {
        graph: String,
        node_id: String,
        edge_count: usize,
    },
    /// Both branches modified the graph catalog differently. Always fatal.
    ///
    /// **Reserved as of Phase 3c.** The current `merge_catalog` does
    /// per-name additive set merging and never produces this variant. Kept
    /// in the enum because it's `pub` and removing it would be a breaking
    /// API change with no benefit. `is_fatal()` continues to return `true`
    /// for it so any future caller that DID produce it (e.g. an opt-in
    /// strict-catalog merge mode) would still abort the merge.
    CatalogDivergence,
    /// A non-graph-aware key (ontology, ref index, type index) had
    /// conflicting modifications. Honors strategy.
    ///
    /// `source` and `target` carry the conflicting raw `Value`s when both
    /// sides have a value (Conflict / BothAddedDifferent). They are `None`
    /// for ModifyDelete / DeleteModify shapes where one side has no value.
    /// These are surfaced into `ConflictEntry.source_value` /
    /// `target_value` so users can inspect what's actually different on
    /// ontology / ref-index / type-index keys.
    OtherKeyConflict {
        user_key: Vec<u8>,
        source: Option<Value>,
        target: Option<Value>,
    },
}

impl GraphMergeConflict {
    /// Returns true if this conflict cannot be resolved by any merge
    /// strategy (referential integrity, catalog).
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            GraphMergeConflict::DanglingEdge { .. }
                | GraphMergeConflict::OrphanedReference { .. }
                | GraphMergeConflict::CatalogDivergence
        )
    }
}

// =============================================================================
// Key shape classification
// =============================================================================

/// What kind of graph key a `user_key` represents.
///
/// Used during decoding to route each entry to the right slot in
/// `PerGraphState`. Returns `None` for keys we don't recognize as graph
/// keys (shouldn't happen if the entries are filtered to TypeTag::Graph,
/// but we're defensive).
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeyShape {
    /// `__catalog__` (no graph name)
    Catalog,
    /// `{graph}/__meta__`
    Meta { graph: String },
    /// `{graph}/n/{node_id}`
    Node { graph: String, node_id: String },
    /// `{graph}/fwd/{src}` — forward adjacency list (packed binary)
    ForwardAdj { graph: String, src: String },
    /// `{graph}/rev/{dst}` — reverse adjacency list (packed binary)
    ReverseAdj { graph: String, dst: String },
    /// Anything else under a `{graph}/...` prefix or a different reserved
    /// key. Treated as opaque KV by the merge.
    Other { graph: Option<String> },
}

/// Extract the graph name from a user_key, or `None` if the key has no
/// graph prefix (e.g., `__catalog__` or any reserved top-level key).
fn extract_graph_name(user_key: &str) -> Option<&str> {
    if user_key.starts_with("__") {
        // Top-level reserved key like `__catalog__` or `__ref__/...`.
        return None;
    }
    user_key.split('/').next().filter(|s| !s.is_empty())
}

/// Classify a `user_key` against all known graph key shapes.
fn classify_key_shape(user_key: &str) -> KeyShape {
    // Catalog: exact match on the well-known constant.
    if user_key == keys::graph_catalog_key() {
        return KeyShape::Catalog;
    }

    // Top-level reserved keys without a graph prefix → other.
    let Some(graph) = extract_graph_name(user_key) else {
        return KeyShape::Other { graph: None };
    };

    // Meta: `{graph}/__meta__` exact match.
    if user_key == keys::meta_key(graph) {
        return KeyShape::Meta {
            graph: graph.to_string(),
        };
    }

    // Node: `{graph}/n/{node_id}`
    if let Some(node_id) = keys::parse_node_key(graph, user_key) {
        return KeyShape::Node {
            graph: graph.to_string(),
            node_id,
        };
    }

    // Forward adjacency: `{graph}/fwd/{src}`
    if let Some(src) = keys::parse_forward_adj_key(graph, user_key) {
        return KeyShape::ForwardAdj {
            graph: graph.to_string(),
            src,
        };
    }

    // Reverse adjacency: `{graph}/rev/{dst}`
    if let Some(dst) = keys::parse_reverse_adj_key(graph, user_key) {
        return KeyShape::ReverseAdj {
            graph: graph.to_string(),
            dst,
        };
    }

    // Anything else under `{graph}/...` (ontology, ref idx, type idx,
    // edge counters, etc.) → opaque other.
    KeyShape::Other {
        graph: Some(graph.to_string()),
    }
}

// =============================================================================
// Decode
// =============================================================================

/// Decode the ancestor side's entries into a `GraphCellState`. Tombstones
/// in the input are dropped (a tombstoned key is treated identically to
/// a key that never existed at the ancestor version, matching the 14-case
/// matrix's collapsing of those two states).
pub fn decode_graph_cell_versioned(entries: &[VersionedEntry]) -> StrataResult<GraphCellState> {
    let mut state = GraphCellState::default();
    for entry in entries {
        if entry.is_tombstone {
            continue;
        }
        let user_key_str = match std::str::from_utf8(&entry.key.user_key) {
            Ok(s) => s,
            Err(_) => {
                // Non-UTF8 graph key → store in catch-all "other" with no
                // graph affiliation.
                state
                    .graphs
                    .entry(String::new())
                    .or_default()
                    .other
                    .insert(entry.key.user_key.to_vec(), entry.value.clone());
                continue;
            }
        };
        ingest_entry(
            &mut state,
            user_key_str,
            entry.key.user_key.to_vec(),
            entry.value.clone(),
        )?;
    }
    Ok(state)
}

/// Decode a live (source/target) side's entries into a `GraphCellState`.
pub fn decode_graph_cell_live(entries: &[(Key, VersionedValue)]) -> StrataResult<GraphCellState> {
    let mut state = GraphCellState::default();
    for (key, vv) in entries {
        let user_key_str = match std::str::from_utf8(&key.user_key) {
            Ok(s) => s,
            Err(_) => {
                state
                    .graphs
                    .entry(String::new())
                    .or_default()
                    .other
                    .insert(key.user_key.to_vec(), vv.value.clone());
                continue;
            }
        };
        ingest_entry(
            &mut state,
            user_key_str,
            key.user_key.to_vec(),
            vv.value.clone(),
        )?;
    }
    Ok(state)
}

/// Route a single entry into the right slot of `state` based on its key shape.
fn ingest_entry(
    state: &mut GraphCellState,
    user_key_str: &str,
    user_key_bytes: Vec<u8>,
    value: Value,
) -> StrataResult<()> {
    match classify_key_shape(user_key_str) {
        KeyShape::Catalog => {
            state.catalog = Some(value);
        }
        KeyShape::Meta { graph } => {
            let meta: GraphMeta = match &value {
                Value::String(s) => serde_json::from_str(s).map_err(|e| {
                    StrataError::serialization(format!("graph meta is not valid JSON: {e}"))
                })?,
                _ => {
                    return Err(StrataError::serialization(
                        "graph meta is not stored as Value::String",
                    ));
                }
            };
            state.graphs.entry(graph).or_default().meta = Some(meta);
        }
        KeyShape::Node { graph, node_id } => {
            let node_data: NodeData = match &value {
                Value::String(s) => serde_json::from_str(s).map_err(|e| {
                    StrataError::serialization(format!("node data is not valid JSON: {e}"))
                })?,
                _ => {
                    return Err(StrataError::serialization(
                        "node data is not stored as Value::String",
                    ));
                }
            };
            state
                .graphs
                .entry(graph)
                .or_default()
                .nodes
                .insert(node_id, node_data);
        }
        KeyShape::ForwardAdj { graph, src } => {
            // Decode the packed list and add each edge to the per-graph
            // edge map keyed by (src, dst, edge_type).
            let bytes = match &value {
                Value::Bytes(b) => b,
                _ => {
                    return Err(StrataError::serialization(
                        "forward adjacency is not stored as Value::Bytes",
                    ));
                }
            };
            let decoded = packed::decode(bytes)?;
            let per_graph = state.graphs.entry(graph).or_default();
            for (dst, edge_type, edge_data) in decoded {
                per_graph
                    .edges
                    .insert((src.clone(), dst, edge_type), edge_data);
            }
        }
        KeyShape::ReverseAdj { graph, dst } => {
            // Reverse lists are derived from forward lists; we use them
            // as an internal consistency check during decode but rely on
            // the forward list as the source of truth for edges. Each
            // entry says "this src → this dst with this type/data".
            let bytes = match &value {
                Value::Bytes(b) => b,
                _ => {
                    return Err(StrataError::serialization(
                        "reverse adjacency is not stored as Value::Bytes",
                    ));
                }
            };
            let decoded = packed::decode(bytes)?;
            let per_graph = state.graphs.entry(graph).or_default();
            for (src, edge_type, edge_data) in decoded {
                // Insert into the edge map keyed by (src, dst, edge_type).
                // If the forward side already inserted this edge, we
                // overwrite with the reverse side's EdgeData (which should
                // be identical). If not, this populates from the reverse
                // side. Either way, the edge map ends up complete.
                per_graph
                    .edges
                    .insert((src, dst.clone(), edge_type), edge_data);
            }
        }
        KeyShape::Other { graph } => {
            let key = graph.clone().unwrap_or_default();
            state
                .graphs
                .entry(key)
                .or_default()
                .other
                .insert(user_key_bytes, value);
        }
    }
    Ok(())
}

// =============================================================================
// Three-way classification (generic over T: PartialEq + Clone)
// =============================================================================

/// Output of the 14-case decision matrix specialized for typed values.
/// Mirrors `branch_ops::ThreeWayChange` but with borrowed value refs and
/// generic over the value type so we can reuse it for nodes, edges, and
/// raw KV.
#[derive(Debug)]
enum ThreeWay<'a, T> {
    Unchanged,
    SourceChanged(&'a T),
    TargetChanged,
    BothChangedSame,
    Conflict { source: &'a T, target: &'a T },
    SourceAdded(&'a T),
    TargetAdded,
    BothAddedSame,
    BothAddedDifferent { source: &'a T, target: &'a T },
    SourceDeleted,
    TargetDeleted,
    BothDeleted,
    DeleteModifyConflict { target: &'a T },
    ModifyDeleteConflict { source: &'a T },
}

fn classify_three_way<'a, T: PartialEq>(
    ancestor: Option<&'a T>,
    source: Option<&'a T>,
    target: Option<&'a T>,
) -> ThreeWay<'a, T> {
    match (ancestor, source, target) {
        (None, None, None) => ThreeWay::Unchanged,
        (Some(a), Some(s), Some(t)) if a == s && s == t => ThreeWay::Unchanged,
        (Some(a), Some(s), Some(t)) if a == t && a != s => ThreeWay::SourceChanged(s),
        (Some(a), Some(s), Some(t)) if a == s && a != t => {
            let _ = t;
            ThreeWay::TargetChanged
        }
        (Some(a), Some(s), Some(t)) if a != s && s == t => {
            let _ = a;
            ThreeWay::BothChangedSame
        }
        (Some(_), Some(s), Some(t)) => ThreeWay::Conflict {
            source: s,
            target: t,
        },
        (None, Some(s), None) => ThreeWay::SourceAdded(s),
        (None, None, Some(_)) => ThreeWay::TargetAdded,
        (None, Some(s), Some(t)) if s == t => {
            let _ = s;
            ThreeWay::BothAddedSame
        }
        (None, Some(s), Some(t)) => ThreeWay::BothAddedDifferent {
            source: s,
            target: t,
        },
        (Some(a), None, Some(t)) if a == t => ThreeWay::SourceDeleted,
        (Some(_), None, Some(t)) => ThreeWay::DeleteModifyConflict { target: t },
        (Some(a), Some(s), None) if a == s => ThreeWay::TargetDeleted,
        (Some(_), Some(s), None) => ThreeWay::ModifyDeleteConflict { source: s },
        (Some(_), None, None) => ThreeWay::BothDeleted,
    }
}

// =============================================================================
// Top-level merge entry point
// =============================================================================

/// Compute the semantic merge of three decoded `GraphCellState`s.
///
/// The algorithm:
/// 1. Merge the catalog (treated as opaque KV; concurrent divergent edits
///    are reported as `CatalogDivergence`, always fatal).
/// 2. For each graph name in the union of the three sides' graphs, run
///    the per-graph merge: nodes → edges → meta → other → counters.
/// 3. Validate referential integrity (dangling edges, orphaned references)
///    on the projected per-graph state.
/// 4. Project all writes into `puts`/`deletes`.
///
/// Returns a `GraphMergeOutput` containing the writes and any conflicts.
/// The caller is responsible for converting puts/deletes into the engine's
/// `MergeAction` shape and computing `expected_target` from the target
/// state for each affected key.
pub fn compute_graph_merge(
    ancestor: &GraphCellState,
    source: &GraphCellState,
    target: &GraphCellState,
    strategy: MergeStrategy,
) -> GraphMergeOutput {
    let mut output = GraphMergeOutput::default();

    // 1. Catalog
    merge_catalog(ancestor, source, target, &mut output);

    // 2. Per-graph merge for every graph that appears on any side.
    let mut all_graphs: BTreeSet<String> = BTreeSet::new();
    all_graphs.extend(ancestor.graphs.keys().cloned());
    all_graphs.extend(source.graphs.keys().cloned());
    all_graphs.extend(target.graphs.keys().cloned());

    let empty = PerGraphState::default();
    for graph_name in &all_graphs {
        let anc = ancestor.graphs.get(graph_name).unwrap_or(&empty);
        let src = source.graphs.get(graph_name).unwrap_or(&empty);
        let tgt = target.graphs.get(graph_name).unwrap_or(&empty);
        merge_per_graph(graph_name, anc, src, tgt, strategy, &mut output);
    }

    output
}

// =============================================================================
// Catalog merge
// =============================================================================

/// Additive set-union catalog merge (Phase 3c).
///
/// The catalog (`__catalog__`) is a `Value::String` containing a JSON array of
/// graph names. Phase 3b treated it as opaque KV and reported any divergent
/// edit as a fatal `CatalogDivergence` — which meant two branches that each
/// independently called `create_graph` could never merge.
///
/// Phase 3c decodes both sides into `BTreeSet<String>` and projects per-name
/// presence: a graph name is in the merged catalog iff it isn't dropped by a
/// delete-without-readd on either side. This is the natural set semantics for
/// a catalog of names — each name's presence is independent of every other
/// name's, so concurrent independent `create_graph` / `delete_graph` calls
/// always merge cleanly.
///
/// Per-name rule (for each name in `anc ∪ src ∪ tgt`):
/// - If src deleted it (in anc, not in src) AND tgt didn't add it back → drop
/// - If tgt deleted it (in anc, not in tgt) AND src didn't add it back → drop
/// - Otherwise → keep
///
/// Equivalently: `projected = (anc - deleted_by_source - deleted_by_target)
/// ∪ added_by_source ∪ added_by_target`.
///
/// Because the projection is purely set-membership and the values being
/// compared are unit (presence/absence), no conflict variant ever fires from
/// this function. `GraphMergeConflict::CatalogDivergence` remains in the enum
/// for backwards-compat (it's `pub`) but is unreachable from the current
/// algorithm.
fn merge_catalog(
    ancestor: &GraphCellState,
    source: &GraphCellState,
    target: &GraphCellState,
    output: &mut GraphMergeOutput,
) {
    let key = keys::graph_catalog_key().to_string();

    // Decode each side as a BTreeSet of graph names. The fallback semantics
    // matter: a side with no catalog value at all (`None`) means "this side
    // has not touched the catalog", NOT "this side deleted the catalog".
    // Conflating those would silently drop entries on legacy data
    // (graphs created before the catalog key existed). Fall back to the
    // ancestor's set so an absent value contributes nothing to the additive
    // merge — equivalent to treating the side as unchanged for catalog
    // purposes while still letting the per-graph data merge run normally.
    //
    // The same fallback applies to malformed catalog values (wrong Value
    // type, JSON parse failure): we don't know what was intended, so we
    // refuse to silently drop entries. The per-graph merge will still
    // surface any data-level conflicts.
    let parse_or = |v: Option<&Value>, fallback: &BTreeSet<String>| -> BTreeSet<String> {
        match v {
            Some(Value::String(s)) => serde_json::from_str(s).unwrap_or_else(|_| fallback.clone()),
            Some(_) => fallback.clone(),
            None => fallback.clone(),
        }
    };
    let empty: BTreeSet<String> = BTreeSet::new();
    let anc = parse_or(ancestor.catalog.as_ref(), &empty);
    let src = parse_or(source.catalog.as_ref(), &anc);
    let tgt = parse_or(target.catalog.as_ref(), &anc);

    // Project per-name presence. Iterate the union of all three sides and
    // decide membership for each unique name. Skip duplicates that the chain
    // iterator may yield when a name appears on multiple sides.
    let mut projected: BTreeSet<String> = BTreeSet::new();
    for name in anc.iter().chain(src.iter()).chain(tgt.iter()) {
        if projected.contains(name) {
            continue;
        }
        let in_anc = anc.contains(name);
        let in_src = src.contains(name);
        let in_tgt = tgt.contains(name);
        // A name is in the projected set iff at least one side has it AND
        // neither side deleted it (i.e. it wasn't in ancestor and removed by
        // exactly one of source/target). The additive set merge:
        //   projected = (anc ∪ src ∪ tgt) - source_only_deletes - target_only_deletes
        let deleted_by_source = in_anc && !in_src;
        let deleted_by_target = in_anc && !in_tgt;
        if !deleted_by_source && !deleted_by_target && (in_anc || in_src || in_tgt) {
            projected.insert(name.clone());
        }
    }

    // Emit a Put only when the projected catalog actually differs from
    // target's current decoded catalog. The wire format is the same JSON
    // array shape `list_graphs` / `create_graph` use; the BTreeSet gives
    // deterministic sorted order, so equivalent merges produce byte-equal
    // output.
    if projected != tgt {
        let projected_vec: Vec<String> = projected.into_iter().collect();
        let json = serde_json::to_string(&projected_vec)
            .expect("BTreeSet<String> JSON serialization is infallible");
        output.puts.push((key, Value::String(json)));
    }
}

// =============================================================================
// Per-graph merge
// =============================================================================

fn merge_per_graph(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) {
    // Step 1: Project nodes.
    let projected_nodes = merge_nodes(graph_name, ancestor, source, target, strategy, output);

    // Step 2: Project edges.
    let projected_edges = merge_edges(graph_name, ancestor, source, target, strategy, output);

    // Step 3: Validate referential integrity.
    validate_dangling_edges(graph_name, &projected_edges, &projected_nodes, output);
    validate_orphan_references(
        graph_name,
        &ancestor.nodes,
        &projected_nodes,
        &projected_edges,
        output,
    );

    // Step 4: Project node writes.
    project_node_writes(graph_name, ancestor, target, &projected_nodes, output);

    // Step 5: Project adjacency writes (fwd + rev).
    project_adjacency_writes(graph_name, &projected_edges, target, output);

    // Step 6: Project edge counter writes (derived from projected edges).
    project_edge_counter_writes(graph_name, &projected_edges, target, output);

    // Step 7: Merge meta (raw 14-case).
    merge_meta(graph_name, ancestor, source, target, strategy, output);

    // Step 8: Merge other graph keys (raw 14-case per key, but exclude
    // edge counters — those are derived from the projected edge set).
    merge_other(graph_name, ancestor, source, target, strategy, output);
}

// =============================================================================
// Node merge
// =============================================================================

/// Project the post-merge node set. Records cell-level conflicts in
/// `output.conflicts` (honoring `strategy`). Returns a `HashMap<node_id, NodeData>`
/// representing the merged live nodes — node_ids absent from the result
/// are nodes that ended up deleted (or never existed).
fn merge_nodes(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) -> HashMap<String, NodeData> {
    let mut projected = HashMap::new();
    let mut all_ids: HashSet<&str> = HashSet::new();
    all_ids.extend(ancestor.nodes.keys().map(|s| s.as_str()));
    all_ids.extend(source.nodes.keys().map(|s| s.as_str()));
    all_ids.extend(target.nodes.keys().map(|s| s.as_str()));

    for id in all_ids {
        let a = ancestor.nodes.get(id);
        let s = source.nodes.get(id);
        let t = target.nodes.get(id);
        match classify_three_way(a, s, t) {
            ThreeWay::Unchanged
            | ThreeWay::TargetChanged
            | ThreeWay::BothChangedSame
            | ThreeWay::TargetAdded
            | ThreeWay::BothAddedSame => {
                if let Some(node) = t {
                    projected.insert(id.to_string(), node.clone());
                }
            }
            ThreeWay::SourceChanged(value) | ThreeWay::SourceAdded(value) => {
                projected.insert(id.to_string(), value.clone());
            }
            ThreeWay::Conflict { source, target }
            | ThreeWay::BothAddedDifferent { source, target } => {
                output
                    .conflicts
                    .push(GraphMergeConflict::NodePropertyConflict {
                        graph: graph_name.to_string(),
                        node_id: id.to_string(),
                        source: source.clone(),
                        target: target.clone(),
                    });
                if strategy == MergeStrategy::LastWriterWins {
                    projected.insert(id.to_string(), source.clone());
                } else {
                    // Strict — keep target's value but the conflict is reported.
                    projected.insert(id.to_string(), target.clone());
                }
            }
            ThreeWay::SourceDeleted | ThreeWay::BothDeleted | ThreeWay::TargetDeleted => {
                // Node is gone in the projected state.
            }
            ThreeWay::DeleteModifyConflict { target } => {
                output
                    .conflicts
                    .push(GraphMergeConflict::NodeModifyDeleteConflict {
                        graph: graph_name.to_string(),
                        node_id: id.to_string(),
                    });
                if strategy == MergeStrategy::LastWriterWins {
                    // Source deleted → drop the node.
                } else {
                    projected.insert(id.to_string(), target.clone());
                }
            }
            ThreeWay::ModifyDeleteConflict { source } => {
                output
                    .conflicts
                    .push(GraphMergeConflict::NodeModifyDeleteConflict {
                        graph: graph_name.to_string(),
                        node_id: id.to_string(),
                    });
                if strategy == MergeStrategy::LastWriterWins {
                    projected.insert(id.to_string(), source.clone());
                }
                // Strict — node remains deleted (target's view) and conflict reported.
            }
        }
    }

    projected
}

// =============================================================================
// Edge merge
// =============================================================================

/// Project the post-merge edge set. Returns a map keyed by `(src, dst, edge_type)`
/// containing the merged edge data.
fn merge_edges(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) -> HashMap<EdgeKey, EdgeData> {
    let mut projected = HashMap::new();
    let mut all_keys: HashSet<EdgeKey> = HashSet::new();
    all_keys.extend(ancestor.edges.keys().cloned());
    all_keys.extend(source.edges.keys().cloned());
    all_keys.extend(target.edges.keys().cloned());

    for edge_key in all_keys {
        let a = ancestor.edges.get(&edge_key);
        let s = source.edges.get(&edge_key);
        let t = target.edges.get(&edge_key);
        match classify_three_way(a, s, t) {
            ThreeWay::Unchanged
            | ThreeWay::TargetChanged
            | ThreeWay::BothChangedSame
            | ThreeWay::TargetAdded
            | ThreeWay::BothAddedSame => {
                if let Some(data) = t {
                    projected.insert(edge_key, data.clone());
                }
            }
            ThreeWay::SourceChanged(value) | ThreeWay::SourceAdded(value) => {
                projected.insert(edge_key, value.clone());
            }
            ThreeWay::Conflict { source, target }
            | ThreeWay::BothAddedDifferent { source, target } => {
                let (src, dst, edge_type) = edge_key.clone();
                output.conflicts.push(GraphMergeConflict::EdgeDataConflict {
                    graph: graph_name.to_string(),
                    src,
                    dst,
                    edge_type,
                    source: source.clone(),
                    target: target.clone(),
                });
                if strategy == MergeStrategy::LastWriterWins {
                    projected.insert(edge_key, source.clone());
                } else {
                    projected.insert(edge_key, target.clone());
                }
            }
            ThreeWay::SourceDeleted | ThreeWay::BothDeleted | ThreeWay::TargetDeleted => {
                // Edge is gone.
            }
            ThreeWay::DeleteModifyConflict { target } => {
                let (src, dst, edge_type) = edge_key.clone();
                output
                    .conflicts
                    .push(GraphMergeConflict::EdgeModifyDeleteConflict {
                        graph: graph_name.to_string(),
                        src,
                        dst,
                        edge_type,
                    });
                if strategy == MergeStrategy::LastWriterWins {
                    // Source deleted → drop edge.
                } else {
                    projected.insert(edge_key, target.clone());
                }
            }
            ThreeWay::ModifyDeleteConflict { source } => {
                let (src, dst, edge_type) = edge_key.clone();
                output
                    .conflicts
                    .push(GraphMergeConflict::EdgeModifyDeleteConflict {
                        graph: graph_name.to_string(),
                        src,
                        dst,
                        edge_type,
                    });
                if strategy == MergeStrategy::LastWriterWins {
                    projected.insert(edge_key, source.clone());
                }
            }
        }
    }

    projected
}

// =============================================================================
// Referential integrity validation
// =============================================================================

fn validate_dangling_edges(
    graph_name: &str,
    projected_edges: &HashMap<EdgeKey, EdgeData>,
    projected_nodes: &HashMap<String, NodeData>,
    output: &mut GraphMergeOutput,
) {
    for (src, dst, edge_type) in projected_edges.keys() {
        if !projected_nodes.contains_key(src) {
            output.conflicts.push(GraphMergeConflict::DanglingEdge {
                graph: graph_name.to_string(),
                src: src.clone(),
                dst: dst.clone(),
                edge_type: edge_type.clone(),
                missing_node: src.clone(),
            });
        }
        if !projected_nodes.contains_key(dst) {
            output.conflicts.push(GraphMergeConflict::DanglingEdge {
                graph: graph_name.to_string(),
                src: src.clone(),
                dst: dst.clone(),
                edge_type: edge_type.clone(),
                missing_node: dst.clone(),
            });
        }
    }
}

fn validate_orphan_references(
    graph_name: &str,
    ancestor_nodes: &HashMap<String, NodeData>,
    projected_nodes: &HashMap<String, NodeData>,
    projected_edges: &HashMap<EdgeKey, EdgeData>,
    output: &mut GraphMergeOutput,
) {
    // For each node that USED TO exist (in ancestor) but no longer does
    // (not in projected_nodes), check that no projected edge references it.
    for deleted_id in ancestor_nodes.keys() {
        if projected_nodes.contains_key(deleted_id) {
            continue;
        }
        let referencing_count = projected_edges
            .keys()
            .filter(|(src, dst, _)| src == deleted_id || dst == deleted_id)
            .count();
        if referencing_count > 0 {
            output
                .conflicts
                .push(GraphMergeConflict::OrphanedReference {
                    graph: graph_name.to_string(),
                    node_id: deleted_id.clone(),
                    edge_count: referencing_count,
                });
        }
    }
}

// =============================================================================
// Write projection
// =============================================================================

/// Emit Put/Delete actions for nodes whose projected state differs from
/// the target's current state.
fn project_node_writes(
    graph_name: &str,
    ancestor: &PerGraphState,
    target: &PerGraphState,
    projected: &HashMap<String, NodeData>,
    output: &mut GraphMergeOutput,
) {
    // Nodes that appear in projected but not in target → Put
    // Nodes that appear in projected with a different value than target → Put
    // Nodes that appear in target but not in projected → Delete
    for (id, data) in projected {
        let target_data = target.nodes.get(id);
        if target_data != Some(data) {
            let user_key = keys::node_key(graph_name, id);
            let json = match serde_json::to_string(data) {
                Ok(j) => j,
                Err(e) => {
                    // Serialization error — push as a "node modify-delete"
                    // conflict for visibility. Should be unreachable for
                    // well-formed NodeData.
                    output
                        .conflicts
                        .push(GraphMergeConflict::NodeModifyDeleteConflict {
                            graph: graph_name.to_string(),
                            node_id: format!("{id} (serialize error: {e})"),
                        });
                    continue;
                }
            };
            output.puts.push((user_key, Value::String(json)));
        }
    }
    for id in target.nodes.keys() {
        if !projected.contains_key(id) {
            // Was in target, projected says it should be gone → Delete.
            // But also: if it was in ancestor and isn't in projected, that
            // means a side deleted it (already handled). If it was only in
            // target (TargetAdded then somehow gone — can't happen via the
            // 14-case matrix), we delete it too.
            let _ = ancestor;
            output.deletes.push(keys::node_key(graph_name, id));
        }
    }
}

/// Emit Put/Delete actions for forward and reverse adjacency lists by
/// re-encoding the projected edge set per source node and per destination
/// node, then comparing against the target's current encoded lists.
fn project_adjacency_writes(
    graph_name: &str,
    projected_edges: &HashMap<EdgeKey, EdgeData>,
    target: &PerGraphState,
    output: &mut GraphMergeOutput,
) {
    // Group projected edges by source for fwd lists.
    let mut fwd_by_src: HashMap<&str, Vec<(&str, &str, &EdgeData)>> = HashMap::new();
    let mut rev_by_dst: HashMap<&str, Vec<(&str, &str, &EdgeData)>> = HashMap::new();
    for ((src, dst, edge_type), data) in projected_edges {
        fwd_by_src
            .entry(src.as_str())
            .or_default()
            .push((dst.as_str(), edge_type.as_str(), data));
        rev_by_dst
            .entry(dst.as_str())
            .or_default()
            .push((src.as_str(), edge_type.as_str(), data));
    }

    // Sort each per-node edge list for deterministic encoding (otherwise
    // two equivalent merges could produce different byte representations,
    // causing spurious OCC conflicts on retry and inconsistent post-merge
    // state across replicas).
    for edges in fwd_by_src.values_mut() {
        edges.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
    }
    for edges in rev_by_dst.values_mut() {
        edges.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
    }

    // Emit fwd writes.
    for (src, edges) in &fwd_by_src {
        let new_bytes = packed::encode(edges);
        let user_key = keys::forward_adj_key(graph_name, src);
        // Compare against target's existing encoding (if any).
        let target_bytes = encode_target_fwd(target, src);
        if target_bytes.as_deref() != Some(new_bytes.as_slice()) {
            output.puts.push((user_key, Value::Bytes(new_bytes)));
        }
    }
    // Delete fwd lists for src nodes that no longer have any outgoing edges
    // but DID have a list in target.
    for src in collect_target_fwd_srcs(target) {
        if !fwd_by_src.contains_key(src.as_str()) {
            output.deletes.push(keys::forward_adj_key(graph_name, &src));
        }
    }

    // Same for rev.
    for (dst, edges) in &rev_by_dst {
        let new_bytes = packed::encode(edges);
        let user_key = keys::reverse_adj_key(graph_name, dst);
        let target_bytes = encode_target_rev(target, dst);
        if target_bytes.as_deref() != Some(new_bytes.as_slice()) {
            output.puts.push((user_key, Value::Bytes(new_bytes)));
        }
    }
    for dst in collect_target_rev_dsts(target) {
        if !rev_by_dst.contains_key(dst.as_str()) {
            output.deletes.push(keys::reverse_adj_key(graph_name, &dst));
        }
    }
}

/// Re-encode target's forward adjacency for `src`, sorted to match the
/// canonical encoding the merge produces. Returns `None` if target has
/// no edges from `src`.
fn encode_target_fwd(target: &PerGraphState, src: &str) -> Option<Vec<u8>> {
    let mut edges: Vec<(&str, &str, &EdgeData)> = target
        .edges
        .iter()
        .filter(|((s, _, _), _)| s == src)
        .map(|((_, dst, et), data)| (dst.as_str(), et.as_str(), data))
        .collect();
    if edges.is_empty() {
        return None;
    }
    edges.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
    Some(packed::encode(&edges))
}

fn encode_target_rev(target: &PerGraphState, dst: &str) -> Option<Vec<u8>> {
    let mut edges: Vec<(&str, &str, &EdgeData)> = target
        .edges
        .iter()
        .filter(|((_, d, _), _)| d == dst)
        .map(|((src, _, et), data)| (src.as_str(), et.as_str(), data))
        .collect();
    if edges.is_empty() {
        return None;
    }
    edges.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
    Some(packed::encode(&edges))
}

fn collect_target_fwd_srcs(target: &PerGraphState) -> HashSet<String> {
    target.edges.keys().map(|(s, _, _)| s.clone()).collect()
}

fn collect_target_rev_dsts(target: &PerGraphState) -> HashSet<String> {
    target.edges.keys().map(|(_, d, _)| d.clone()).collect()
}

/// Derive edge counter writes from the projected edge set.
///
/// Compares the projected per-type counts against target's *decoded* edge
/// counts (not the stored counter), so the merge only emits counter writes
/// when the merge actually changed the edge count for that type. This
/// avoids two failure modes:
///
/// - **Stale counters get unintentionally rewritten on no-op merges.**
///   If target's stored counter disagrees with target's actual edge count
///   (e.g., from a pre-existing bug or duplicate-edge entries in the
///   packed list), comparing against the stored counter would emit a
///   write on a single-sided no-op merge — surprising the user with an
///   unexpected counter mutation that doesn't correspond to any edge change.
///
/// - **Counter writes go out of sync with `fwd/rev` writes.** The
///   adjacency-write projection compares NEW vs target re-encoded (both
///   from the decoded HashMap). A duplicate-entries-on-disk case would
///   produce no fwd/rev write (because both encodings dedupe to the same
///   bytes) but the previous counter logic would still rewrite the
///   counter — leaving the on-disk counter inconsistent with the unchanged
///   on-disk fwd/rev lists.
///
/// The current rule: a counter is touched only when the merge legitimately
/// changes the per-type count (an add or remove that actually flows
/// through the projection). Pre-existing stale counters are left alone.
fn project_edge_counter_writes(
    graph_name: &str,
    projected_edges: &HashMap<EdgeKey, EdgeData>,
    target: &PerGraphState,
    output: &mut GraphMergeOutput,
) {
    // Per-type counts in the projected merge result.
    let mut projected_counts: HashMap<&str, u64> = HashMap::new();
    for (_, _, edge_type) in projected_edges.keys() {
        *projected_counts.entry(edge_type.as_str()).or_insert(0) += 1;
    }
    // Per-type counts from target's *decoded* edges (NOT target.other's
    // stored counter, which can be stale or disagree with the actual
    // edge set).
    let mut target_counts: HashMap<&str, u64> = HashMap::new();
    for (_, _, edge_type) in target.edges.keys() {
        *target_counts.entry(edge_type.as_str()).or_insert(0) += 1;
    }

    // Emit a Put when the projected count differs from target's decoded
    // count. The merge actually changed the per-type count → the counter
    // needs updating to match the new edge set.
    for (edge_type, count) in &projected_counts {
        let target_count = target_counts.get(edge_type).copied().unwrap_or(0);
        if target_count != *count {
            let user_key = keys::edge_type_count_key(graph_name, edge_type);
            output
                .puts
                .push((user_key, Value::String(count.to_string())));
        }
    }

    // Emit a Delete for counters whose edge type used to exist in target
    // (decoded count > 0) but no longer exists in the projected state.
    // We check target.edges (decoded), not target.other (stored counter),
    // to avoid deleting stale-but-correct counter keys on no-op merges.
    for edge_type in target_counts.keys() {
        if !projected_counts.contains_key(edge_type) {
            output
                .deletes
                .push(keys::edge_type_count_key(graph_name, edge_type));
        }
    }
}

// =============================================================================
// Meta merge
// =============================================================================

fn merge_meta(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) {
    let key = keys::meta_key(graph_name);

    // Serialize-or-flag helper. `serde_json::to_string(&GraphMeta)` is
    // virtually infallible for well-formed metadata, but if it ever did
    // fail (corrupt enum, out-of-memory…) we must NOT silently drop the
    // put — that would let the merge proceed with stale meta. Push a
    // synthetic MetaConflict instead, mirroring the project_node_writes
    // serialization-error path. Returns false when the put was dropped.
    let try_push_put = |meta: &GraphMeta, output: &mut GraphMergeOutput| -> bool {
        match serde_json::to_string(meta) {
            Ok(json) => {
                output.puts.push((key.clone(), Value::String(json)));
                true
            }
            Err(_) => {
                output.conflicts.push(GraphMergeConflict::MetaConflict {
                    graph: graph_name.to_string(),
                });
                false
            }
        }
    };

    match classify_three_way(
        ancestor.meta.as_ref(),
        source.meta.as_ref(),
        target.meta.as_ref(),
    ) {
        ThreeWay::Unchanged
        | ThreeWay::TargetChanged
        | ThreeWay::BothChangedSame
        | ThreeWay::TargetAdded
        | ThreeWay::BothAddedSame
        | ThreeWay::TargetDeleted
        | ThreeWay::BothDeleted => {}
        ThreeWay::SourceChanged(value) | ThreeWay::SourceAdded(value) => {
            try_push_put(value, output);
        }
        ThreeWay::SourceDeleted => {
            output.deletes.push(key);
        }
        ThreeWay::Conflict { source, target } | ThreeWay::BothAddedDifferent { source, target } => {
            output.conflicts.push(GraphMergeConflict::MetaConflict {
                graph: graph_name.to_string(),
            });
            if strategy == MergeStrategy::LastWriterWins {
                try_push_put(source, output);
            }
            let _ = target;
        }
        ThreeWay::DeleteModifyConflict { target } => {
            output.conflicts.push(GraphMergeConflict::MetaConflict {
                graph: graph_name.to_string(),
            });
            if strategy == MergeStrategy::LastWriterWins {
                output.deletes.push(key);
            }
            let _ = target;
        }
        ThreeWay::ModifyDeleteConflict { source } => {
            output.conflicts.push(GraphMergeConflict::MetaConflict {
                graph: graph_name.to_string(),
            });
            if strategy == MergeStrategy::LastWriterWins {
                try_push_put(source, output);
            }
        }
    }
}

// =============================================================================
// Other graph keys (ontology, ref idx, type idx, raw counters etc.)
// =============================================================================

/// Merge the catch-all "other" graph keys via the 14-case matrix at the
/// raw KV level. Edge counter keys are SKIPPED here because the
/// `project_edge_counter_writes` step derives them from the projected edge
/// set instead.
fn merge_other(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) {
    let counter_prefix = format!("{graph_name}/__edge_count__/");
    let mut all_keys: HashSet<&Vec<u8>> = HashSet::new();
    all_keys.extend(ancestor.other.keys());
    all_keys.extend(source.other.keys());
    all_keys.extend(target.other.keys());

    for key_bytes in all_keys {
        // Skip edge counters; handled by project_edge_counter_writes.
        if let Ok(key_str) = std::str::from_utf8(key_bytes) {
            if key_str.starts_with(&counter_prefix) {
                continue;
            }
        }

        let a = ancestor.other.get(key_bytes);
        let s = source.other.get(key_bytes);
        let t = target.other.get(key_bytes);

        let user_key_str = match std::str::from_utf8(key_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => continue, // shouldn't happen for graph keys
        };

        match classify_three_way(a, s, t) {
            ThreeWay::Unchanged
            | ThreeWay::TargetChanged
            | ThreeWay::BothChangedSame
            | ThreeWay::TargetAdded
            | ThreeWay::BothAddedSame
            | ThreeWay::TargetDeleted
            | ThreeWay::BothDeleted => {}
            ThreeWay::SourceChanged(value) | ThreeWay::SourceAdded(value) => {
                output.puts.push((user_key_str, value.clone()));
            }
            ThreeWay::SourceDeleted => {
                output.deletes.push(user_key_str);
            }
            ThreeWay::Conflict { source, target }
            | ThreeWay::BothAddedDifferent { source, target } => {
                output.conflicts.push(GraphMergeConflict::OtherKeyConflict {
                    user_key: key_bytes.clone(),
                    source: Some(source.clone()),
                    target: Some(target.clone()),
                });
                if strategy == MergeStrategy::LastWriterWins {
                    output.puts.push((user_key_str, source.clone()));
                }
            }
            ThreeWay::DeleteModifyConflict { target } => {
                output.conflicts.push(GraphMergeConflict::OtherKeyConflict {
                    user_key: key_bytes.clone(),
                    source: None,
                    target: Some(target.clone()),
                });
                if strategy == MergeStrategy::LastWriterWins {
                    output.deletes.push(user_key_str);
                }
            }
            ThreeWay::ModifyDeleteConflict { source } => {
                output.conflicts.push(GraphMergeConflict::OtherKeyConflict {
                    user_key: key_bytes.clone(),
                    source: Some(source.clone()),
                    target: None,
                });
                if strategy == MergeStrategy::LastWriterWins {
                    output.puts.push((user_key_str, source.clone()));
                }
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn node(props: serde_json::Value) -> NodeData {
        NodeData {
            entity_ref: None,
            properties: Some(props),
            object_type: None,
        }
    }

    fn edge_data(weight: f64) -> EdgeData {
        EdgeData {
            weight,
            properties: None,
        }
    }

    fn empty_state() -> GraphCellState {
        GraphCellState::default()
    }

    fn state_with_nodes(graph: &str, nodes: &[(&str, NodeData)]) -> GraphCellState {
        let mut s = GraphCellState::default();
        let per_graph = s.graphs.entry(graph.to_string()).or_default();
        for (id, data) in nodes {
            per_graph.nodes.insert(id.to_string(), data.clone());
        }
        s
    }

    fn state_with_nodes_and_edges(
        graph: &str,
        nodes: &[(&str, NodeData)],
        edges: &[(&str, &str, &str, EdgeData)],
    ) -> GraphCellState {
        let mut s = state_with_nodes(graph, nodes);
        let per_graph = s.graphs.get_mut(graph).unwrap();
        for (src, dst, et, data) in edges {
            per_graph.edges.insert(
                (src.to_string(), dst.to_string(), et.to_string()),
                data.clone(),
            );
        }
        s
    }

    #[test]
    fn classify_key_shape_recognizes_all_shapes() {
        assert_eq!(classify_key_shape("__catalog__"), KeyShape::Catalog);
        assert_eq!(
            classify_key_shape("g/__meta__"),
            KeyShape::Meta {
                graph: "g".to_string()
            }
        );
        assert_eq!(
            classify_key_shape("g/n/alice"),
            KeyShape::Node {
                graph: "g".to_string(),
                node_id: "alice".to_string()
            }
        );
        assert_eq!(
            classify_key_shape("g/fwd/alice"),
            KeyShape::ForwardAdj {
                graph: "g".to_string(),
                src: "alice".to_string()
            }
        );
        assert_eq!(
            classify_key_shape("g/rev/bob"),
            KeyShape::ReverseAdj {
                graph: "g".to_string(),
                dst: "bob".to_string()
            }
        );
        assert!(matches!(
            classify_key_shape("g/__edge_count__/follows"),
            KeyShape::Other { .. }
        ));
    }

    #[test]
    fn merge_disjoint_node_additions_succeeds() {
        // Ancestor: alice. Source adds carol. Target adds dave.
        let ancestor = state_with_nodes("g", &[("alice", node(serde_json::json!({})))]);
        let source = state_with_nodes(
            "g",
            &[
                ("alice", node(serde_json::json!({}))),
                ("carol", node(serde_json::json!({}))),
            ],
        );
        let target = state_with_nodes(
            "g",
            &[
                ("alice", node(serde_json::json!({}))),
                ("dave", node(serde_json::json!({}))),
            ],
        );

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        assert!(
            result.conflicts.is_empty(),
            "expected no conflicts, got {:?}",
            result.conflicts
        );
        // The merge should produce a Put for `n/carol` (source added) but
        // NOT for `n/dave` or `n/alice` (target already has them).
        let put_keys: HashSet<&str> = result.puts.iter().map(|(k, _)| k.as_str()).collect();
        assert!(put_keys.contains("g/n/carol"));
        assert!(!put_keys.contains("g/n/dave"));
        assert!(!put_keys.contains("g/n/alice"));
    }

    #[test]
    fn merge_disjoint_edge_additions_succeeds() {
        // Pre-fork: alice, bob, carol, dave. Source adds alice→carol. Target adds bob→dave.
        let pre_nodes = vec![
            ("alice", node(serde_json::json!({}))),
            ("bob", node(serde_json::json!({}))),
            ("carol", node(serde_json::json!({}))),
            ("dave", node(serde_json::json!({}))),
        ];
        let ancestor = state_with_nodes("g", &pre_nodes);
        let source = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "carol", "follows", edge_data(1.0))],
        );
        let target = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("bob", "dave", "follows", edge_data(1.0))],
        );

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        assert!(
            result.conflicts.is_empty(),
            "expected no conflicts, got {:?}",
            result.conflicts
        );
        // Merge should produce: fwd/alice (source's new edge), rev/carol
        // (paired with fwd/alice). NOT fwd/bob or rev/dave (target already has them).
        let put_keys: HashSet<&str> = result.puts.iter().map(|(k, _)| k.as_str()).collect();
        assert!(put_keys.contains("g/fwd/alice"), "puts: {put_keys:?}");
        assert!(put_keys.contains("g/rev/carol"), "puts: {put_keys:?}");
        assert!(!put_keys.contains("g/fwd/bob"), "puts: {put_keys:?}");
        assert!(!put_keys.contains("g/rev/dave"), "puts: {put_keys:?}");
    }

    #[test]
    fn dangling_edge_is_rejected() {
        // Source adds edge alice→carol. Target deletes carol.
        // Projected edge set has alice→carol but projected nodes lacks carol.
        let pre_nodes = vec![
            ("alice", node(serde_json::json!({}))),
            ("carol", node(serde_json::json!({}))),
        ];
        let ancestor = state_with_nodes("g", &pre_nodes);
        let source = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "carol", "follows", edge_data(1.0))],
        );
        // Target: carol deleted
        let target = state_with_nodes("g", &[("alice", node(serde_json::json!({})))]);

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        // Should have at least one DanglingEdge or OrphanedReference conflict.
        let has_dangling = result
            .conflicts
            .iter()
            .any(|c| matches!(c, GraphMergeConflict::DanglingEdge { .. }));
        let has_orphan = result
            .conflicts
            .iter()
            .any(|c| matches!(c, GraphMergeConflict::OrphanedReference { .. }));
        assert!(
            has_dangling || has_orphan,
            "expected dangling or orphan conflict, got {:?}",
            result.conflicts
        );
    }

    #[test]
    fn orphan_reference_after_node_delete_rejected() {
        // Pre-fork: alice, bob (no edges).
        // Source: deletes alice.
        // Target: adds an edge alice→bob (alice still exists on target).
        // The merge sees: source removed alice, target ADDED a new edge from
        // alice. Edge is TargetAdded (kept). Alice node is SourceDeleted
        // (removed). Result: projected_edges has alice→bob but
        // projected_nodes lacks alice → dangling edge / orphan reference.
        let pre_nodes = vec![
            ("alice", node(serde_json::json!({}))),
            ("bob", node(serde_json::json!({}))),
        ];
        let ancestor = state_with_nodes("g", &pre_nodes);
        // Source: alice removed (no edges in ancestor to clean up)
        let source = state_with_nodes("g", &[("bob", node(serde_json::json!({})))]);
        // Target: alice still present, plus a new edge alice→bob
        let target = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "bob", "follows", edge_data(1.0))],
        );

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        // The projected state has:
        //   - alice: gone (SourceDeleted)
        //   - bob: present
        //   - edge alice→bob: present (TargetAdded)
        // → DanglingEdge (missing endpoint "alice") AND/OR OrphanedReference
        //   ("alice" deleted but edge still references it).
        let has_dangling_or_orphan = result.conflicts.iter().any(|c| {
            matches!(
                c,
                GraphMergeConflict::DanglingEdge { .. }
                    | GraphMergeConflict::OrphanedReference { .. }
            )
        });
        assert!(
            has_dangling_or_orphan,
            "expected dangling/orphan conflict, got {:?}",
            result.conflicts
        );
    }

    #[test]
    fn conflicting_node_props_lww_source_wins() {
        let original = node(serde_json::json!({"role": "user"}));
        let source_data = node(serde_json::json!({"role": "admin"}));
        let target_data = node(serde_json::json!({"role": "guest"}));

        let ancestor = state_with_nodes("g", &[("alice", original)]);
        let source = state_with_nodes("g", &[("alice", source_data.clone())]);
        let target = state_with_nodes("g", &[("alice", target_data)]);

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        // Should have a NodePropertyConflict reported AND a Put for n/alice
        // with source's value.
        assert!(result
            .conflicts
            .iter()
            .any(|c| matches!(c, GraphMergeConflict::NodePropertyConflict { .. })));
        let alice_put = result
            .puts
            .iter()
            .find(|(k, _)| k == "g/n/alice")
            .expect("expected a Put for n/alice under LWW");
        if let Value::String(s) = &alice_put.1 {
            let parsed: NodeData = serde_json::from_str(s).unwrap();
            assert_eq!(parsed, source_data);
        } else {
            panic!("expected Value::String for serialized NodeData");
        }
    }

    #[test]
    fn conflicting_node_props_strict_reports_but_no_put() {
        let original = node(serde_json::json!({"role": "user"}));
        let source_data = node(serde_json::json!({"role": "admin"}));
        let target_data = node(serde_json::json!({"role": "guest"}));

        let ancestor = state_with_nodes("g", &[("alice", original)]);
        let source = state_with_nodes("g", &[("alice", source_data)]);
        let target = state_with_nodes("g", &[("alice", target_data)]);

        let result = compute_graph_merge(&ancestor, &source, &target, MergeStrategy::Strict);

        assert!(result
            .conflicts
            .iter()
            .any(|c| matches!(c, GraphMergeConflict::NodePropertyConflict { .. })));
        // Under Strict the conflict is reported but no Put is emitted (target's
        // value is preserved by not emitting a write).
        assert!(
            !result.puts.iter().any(|(k, _)| k == "g/n/alice"),
            "Strict should not emit a Put for n/alice"
        );
    }

    #[test]
    fn conflicting_edge_data_lww_source_wins() {
        // Pre-fork: alice→bob with weight=1.0
        // Source: alice→bob with weight=2.0 (modified)
        // Target: alice→bob with weight=3.0 (modified differently)
        let pre_nodes = vec![
            ("alice", node(serde_json::json!({}))),
            ("bob", node(serde_json::json!({}))),
        ];
        let ancestor = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "bob", "follows", edge_data(1.0))],
        );
        let source = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "bob", "follows", edge_data(2.0))],
        );
        let target = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "bob", "follows", edge_data(3.0))],
        );

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        // Conflict reported with full source/target EdgeData.
        let conflict = result
            .conflicts
            .iter()
            .find_map(|c| match c {
                GraphMergeConflict::EdgeDataConflict {
                    src,
                    dst,
                    edge_type,
                    source,
                    target,
                    ..
                } => Some((src, dst, edge_type, source, target)),
                _ => None,
            })
            .expect("expected EdgeDataConflict");
        assert_eq!(conflict.0, "alice");
        assert_eq!(conflict.1, "bob");
        assert_eq!(conflict.2, "follows");
        assert_eq!(conflict.3.weight, 2.0);
        assert_eq!(conflict.4.weight, 3.0);

        // Under LWW: the projected fwd/alice should encode the source's
        // weight (2.0), since source wins the conflict. We verify by
        // decoding the emitted Put and checking the EdgeData.
        let fwd_alice = result
            .puts
            .iter()
            .find(|(k, _)| k == "g/fwd/alice")
            .expect("expected fwd/alice put under LWW conflict resolution");
        if let Value::Bytes(b) = &fwd_alice.1 {
            let edges = packed::decode(b).unwrap();
            assert_eq!(edges.len(), 1, "fwd/alice should contain one edge");
            assert_eq!(edges[0].2.weight, 2.0, "source's weight should win");
        } else {
            panic!("expected Value::Bytes for fwd/alice");
        }
    }

    #[test]
    fn conflicting_edge_data_strict_reports_no_put() {
        // Same fork as above, but Strict strategy: conflict reported, no put.
        let pre_nodes = vec![
            ("alice", node(serde_json::json!({}))),
            ("bob", node(serde_json::json!({}))),
        ];
        let ancestor = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "bob", "follows", edge_data(1.0))],
        );
        let source = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "bob", "follows", edge_data(2.0))],
        );
        let target = state_with_nodes_and_edges(
            "g",
            &pre_nodes,
            &[("alice", "bob", "follows", edge_data(3.0))],
        );

        let result = compute_graph_merge(&ancestor, &source, &target, MergeStrategy::Strict);

        assert!(result
            .conflicts
            .iter()
            .any(|c| matches!(c, GraphMergeConflict::EdgeDataConflict { .. })));
        // Under Strict the projected edge keeps target's value (3.0), so the
        // re-encoded fwd/alice equals target's existing fwd/alice → no put.
        assert!(
            !result.puts.iter().any(|(k, _)| k == "g/fwd/alice"),
            "Strict should not emit a Put for fwd/alice when target's value is preserved"
        );
    }

    #[test]
    fn other_key_conflict_carries_source_and_target_values() {
        // Both branches concurrently set a different ontology entry for the
        // same object_type. Phase 3b treats ontology as opaque KV via the
        // 14-case matrix, but the resulting OtherKeyConflict must carry both
        // sides' values so users can inspect what's actually different.
        let key = b"g/__types__/object/Patient".to_vec();
        let mut ancestor = state_with_nodes("g", &[]);
        ancestor.graphs.get_mut("g").unwrap().other.insert(
            key.clone(),
            Value::String("{\"label\":\"original\"}".to_string()),
        );
        let mut source = state_with_nodes("g", &[]);
        source.graphs.get_mut("g").unwrap().other.insert(
            key.clone(),
            Value::String("{\"label\":\"source\"}".to_string()),
        );
        let mut target = state_with_nodes("g", &[]);
        target.graphs.get_mut("g").unwrap().other.insert(
            key.clone(),
            Value::String("{\"label\":\"target\"}".to_string()),
        );

        let result = compute_graph_merge(&ancestor, &source, &target, MergeStrategy::Strict);

        let (got_user_key, got_source, got_target) = result
            .conflicts
            .iter()
            .find_map(|c| match c {
                GraphMergeConflict::OtherKeyConflict {
                    user_key,
                    source,
                    target,
                } => Some((user_key, source, target)),
                _ => None,
            })
            .expect("expected OtherKeyConflict");
        assert_eq!(got_user_key, &key);
        assert_eq!(
            got_source.as_ref().unwrap(),
            &Value::String("{\"label\":\"source\"}".to_string())
        );
        assert_eq!(
            got_target.as_ref().unwrap(),
            &Value::String("{\"label\":\"target\"}".to_string())
        );
    }

    /// Decode the catalog Value::String JSON list into a sorted Vec<String>
    /// for stable test assertions. Returns None if no catalog Put was emitted.
    fn extract_catalog_put(result: &GraphMergeOutput) -> Option<Vec<String>> {
        let (_, value) = result
            .puts
            .iter()
            .find(|(k, _)| k == keys::graph_catalog_key())?;
        match value {
            Value::String(s) => {
                let mut v: Vec<String> = serde_json::from_str(s).ok()?;
                v.sort();
                Some(v)
            }
            _ => None,
        }
    }

    #[test]
    fn catalog_additive_disjoint_creates_succeeds() {
        // Phase 3c: both branches concurrently created a different new graph.
        // The Phase 3b behavior was a fatal CatalogDivergence; Phase 3c
        // produces a set union [g1,g2,g3] with no conflicts.
        let mut ancestor = empty_state();
        ancestor.catalog = Some(Value::String("[\"g1\"]".to_string()));
        let mut source = empty_state();
        source.catalog = Some(Value::String("[\"g1\",\"g2\"]".to_string()));
        let mut target = empty_state();
        target.catalog = Some(Value::String("[\"g1\",\"g3\"]".to_string()));

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        assert!(
            !result
                .conflicts
                .iter()
                .any(|c| matches!(c, GraphMergeConflict::CatalogDivergence)),
            "Phase 3c must not produce CatalogDivergence, got {:?}",
            result.conflicts
        );
        let merged = extract_catalog_put(&result).expect("expected a catalog Put");
        assert_eq!(merged, vec!["g1", "g2", "g3"]);
    }

    #[test]
    fn catalog_additive_one_creates_one_deletes_different_graphs_succeeds() {
        // Source deletes g2; target adds g3. Pre-fork catalog had {g1,g2}.
        // Projected: g1 (kept), g2 dropped (source deleted, target didn't
        // add back), g3 (target added). Expected: [g1,g3].
        let mut ancestor = empty_state();
        ancestor.catalog = Some(Value::String("[\"g1\",\"g2\"]".to_string()));
        let mut source = empty_state();
        source.catalog = Some(Value::String("[\"g1\"]".to_string()));
        let mut target = empty_state();
        target.catalog = Some(Value::String("[\"g1\",\"g2\",\"g3\"]".to_string()));

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        assert!(
            !result
                .conflicts
                .iter()
                .any(|c| matches!(c, GraphMergeConflict::CatalogDivergence)),
            "Phase 3c must not produce CatalogDivergence, got {:?}",
            result.conflicts
        );
        let merged = extract_catalog_put(&result).expect("expected a catalog Put");
        assert_eq!(merged, vec!["g1", "g3"]);
    }

    #[test]
    fn catalog_additive_both_delete_independent_graphs_succeeds() {
        // ancestor=[g1,g2,g3]. Source deletes g2. Target deletes g3. The
        // projected catalog drops both deletes and keeps only g1.
        let mut ancestor = empty_state();
        ancestor.catalog = Some(Value::String("[\"g1\",\"g2\",\"g3\"]".to_string()));
        let mut source = empty_state();
        source.catalog = Some(Value::String("[\"g1\",\"g3\"]".to_string()));
        let mut target = empty_state();
        target.catalog = Some(Value::String("[\"g1\",\"g2\"]".to_string()));

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        assert!(
            !result
                .conflicts
                .iter()
                .any(|c| matches!(c, GraphMergeConflict::CatalogDivergence)),
            "Phase 3c must not produce CatalogDivergence, got {:?}",
            result.conflicts
        );
        let merged = extract_catalog_put(&result).expect("expected a catalog Put");
        assert_eq!(merged, vec!["g1"]);
    }

    #[test]
    fn catalog_additive_legacy_target_without_catalog_preserved() {
        // Regression for the post-implementation review fix:
        // `merge_catalog` previously collapsed `None` catalog Values to the
        // empty set, which made "target has legacy data and never wrote a
        // catalog" indistinguishable from "target deleted everything from
        // the catalog". The bug:
        //
        //   - ancestor: Some([g1])  (g1 was created pre-fork)
        //   - source:   Some([g1, g2])  (source ran create_graph("g2"))
        //   - target:   None  (legacy / pre-catalog data; g1 lives on disk
        //                       but the __catalog__ key was never written)
        //
        // The empty-set collapse interpreted target=None as "g1 was
        // deleted", projected the catalog to [g2], and emitted a Put. After
        // the merge target's __catalog__ would explicitly say only g2
        // exists, hiding g1 from `list_graphs`.
        //
        // The fallback fix treats `None` as "this side hasn't touched the
        // catalog" → falls back to ancestor's set. The projected catalog
        // becomes [g1, g2] which correctly reflects all graphs that exist
        // on either side post-merge.
        let mut ancestor = empty_state();
        ancestor.catalog = Some(Value::String("[\"g1\"]".to_string()));
        let mut source = empty_state();
        source.catalog = Some(Value::String("[\"g1\",\"g2\"]".to_string()));
        let target = empty_state(); // catalog: None — never written

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        assert!(
            !result
                .conflicts
                .iter()
                .any(|c| matches!(c, GraphMergeConflict::CatalogDivergence)),
            "no fatal conflict expected, got {:?}",
            result.conflicts
        );
        let merged = extract_catalog_put(&result)
            .expect("expected a catalog Put — projected differs from target's None");
        assert_eq!(
            merged,
            vec!["g1", "g2"],
            "legacy target's missing __catalog__ must NOT cause g1 to be \
             dropped — both names must survive the merge"
        );
    }

    #[test]
    fn catalog_additive_no_op_when_target_already_has_union() {
        // Source added g2. Target also already has [g1,g2] (its prior fork
        // history). Projected catalog == target → no Put.
        let mut ancestor = empty_state();
        ancestor.catalog = Some(Value::String("[\"g1\"]".to_string()));
        let mut source = empty_state();
        source.catalog = Some(Value::String("[\"g1\",\"g2\"]".to_string()));
        let mut target = empty_state();
        target.catalog = Some(Value::String("[\"g1\",\"g2\"]".to_string()));

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        assert!(
            extract_catalog_put(&result).is_none(),
            "no-op merges must not emit a catalog Put; got {:?}",
            result.puts
        );
    }

    #[test]
    fn no_changes_produces_no_actions() {
        let state = state_with_nodes("g", &[("alice", node(serde_json::json!({})))]);
        let result = compute_graph_merge(&state, &state, &state, MergeStrategy::LastWriterWins);
        assert!(result.puts.is_empty());
        assert!(result.deletes.is_empty());
        assert!(result.conflicts.is_empty());
    }

    #[test]
    fn three_way_classify_unchanged() {
        let v = "x";
        assert!(matches!(
            classify_three_way::<&str>(Some(&v), Some(&v), Some(&v)),
            ThreeWay::Unchanged
        ));
    }

    #[test]
    fn three_way_classify_source_added() {
        let v = "x";
        assert!(matches!(
            classify_three_way::<&str>(None, Some(&v), None),
            ThreeWay::SourceAdded(_)
        ));
    }

    #[test]
    fn no_op_merge_does_not_rewrite_stale_counters() {
        // Regression for the post-implementation review fix:
        // `project_edge_counter_writes` used to compare projected counts
        // against target's *stored* counter (target.other), not target's
        // *decoded* edge count. If the stored counter was stale (e.g.,
        // duplicate-edge entries in the packed list, or pre-existing
        // counter bug), a no-op merge would emit a counter Put that
        // didn't correspond to any edge change — leaving the counter
        // out of sync with the unchanged on-disk fwd/rev lists.
        //
        // This test pins the fix: a single-sided merge where neither
        // side touches the relevant edge type must NOT emit a counter
        // write even when target.other has a stale counter value.
        let pre = vec![
            ("alice", node(serde_json::json!({}))),
            ("bob", node(serde_json::json!({}))),
        ];
        let mut ancestor =
            state_with_nodes_and_edges("g", &pre, &[("alice", "bob", "follows", edge_data(1.0))]);
        // Stale stored counter on ancestor: claims 5 follows-edges
        // even though there's only 1 in the decoded edges map.
        ancestor.graphs.get_mut("g").unwrap().other.insert(
            b"g/__edge_count__/follows".to_vec(),
            Value::String("5".into()),
        );

        let source = ancestor.clone();
        let target = ancestor.clone();

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);

        // No conflicts, no actions — the merge is a true no-op.
        assert!(
            result.conflicts.is_empty(),
            "no-op merge should produce no conflicts"
        );
        // Specifically: no counter write was emitted, even though target's
        // stored counter (5) disagrees with the decoded edge count (1).
        let counter_puts: Vec<&(String, Value)> = result
            .puts
            .iter()
            .filter(|(k, _)| k.contains("__edge_count__"))
            .collect();
        assert!(
            counter_puts.is_empty(),
            "no-op merge must not rewrite counters; found {counter_puts:?}"
        );
        let counter_deletes: Vec<&String> = result
            .deletes
            .iter()
            .filter(|k| k.contains("__edge_count__"))
            .collect();
        assert!(
            counter_deletes.is_empty(),
            "no-op merge must not delete counters; found {counter_deletes:?}"
        );
    }

    #[test]
    fn merged_edges_are_bidirectionally_consistent() {
        // Source adds alice→carol, target adds bob→dave.
        // After projection, the algorithm should emit fwd/alice, rev/carol,
        // (target already has fwd/bob, rev/dave so no put for those).
        // The projected edge set should have BOTH edges.
        let pre = vec![
            ("alice", node(serde_json::json!({}))),
            ("bob", node(serde_json::json!({}))),
            ("carol", node(serde_json::json!({}))),
            ("dave", node(serde_json::json!({}))),
        ];
        let ancestor = state_with_nodes("g", &pre);
        let source =
            state_with_nodes_and_edges("g", &pre, &[("alice", "carol", "follows", edge_data(1.0))]);
        let target =
            state_with_nodes_and_edges("g", &pre, &[("bob", "dave", "follows", edge_data(1.0))]);

        let result =
            compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);
        assert!(result.conflicts.is_empty());

        // Verify: fwd/alice contains (carol, follows). rev/carol contains
        // (alice, follows). Both encode-decode roundtrip.
        let fwd_alice = result
            .puts
            .iter()
            .find(|(k, _)| k == "g/fwd/alice")
            .expect("expected fwd/alice put");
        if let Value::Bytes(b) = &fwd_alice.1 {
            let edges = packed::decode(b).unwrap();
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0].0, "carol"); // dst
            assert_eq!(edges[0].1, "follows");
        } else {
            panic!("expected Value::Bytes");
        }

        let rev_carol = result
            .puts
            .iter()
            .find(|(k, _)| k == "g/rev/carol")
            .expect("expected rev/carol put");
        if let Value::Bytes(b) = &rev_carol.1 {
            let edges = packed::decode(b).unwrap();
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0].0, "alice"); // src
            assert_eq!(edges[0].1, "follows");
        } else {
            panic!("expected Value::Bytes");
        }
    }
}
