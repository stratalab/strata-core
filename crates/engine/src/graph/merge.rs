//! Semantic graph merge.
//!
//! See `docs/design/branching/primitive-aware-merge.md`.
//!
//! ## Role
//!
//! The fallback divergence-refusal in `branch_ops/mod.rs` rejects every
//! merge where both branches modified graph state since fork — strictly
//! conservative, but it rejects disjoint additions too. This module
//! replaces that refusal with a real semantic merge:
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
//! `merge_catalog` (the catalog merge step) decodes each side as a
//! `BTreeSet<String>` and projects per-name presence, so concurrent
//! independent `create_graph` calls on different names merge cleanly.
//! The `CatalogDivergence` conflict variant is reserved but no longer
//! produced from the additive merge.
//!
//! Both `merge_branches` and `cherry_pick_from_diff` go through this
//! algorithm via the per-handler `plan` dispatch in
//! `crates/engine/src/branch_ops/primitive_merge.rs`.
//!
//! ## Layering
//!
//! This module is **pure**: no engine dispatch, no transaction context,
//! no `MergeAction`. The engine crate's `GraphMergeHandler::plan` calls
//! `compute_graph_merge` and converts the resulting `GraphMergeOutput`
//! into the engine's per-handler `PrimitiveMergePlan` shape.
//!
//! ## Out of scope
//!
//! - **Ontology semantic merge.** Ontology entries are merged as opaque KV
//!   via the 14-case matrix. A richer ontology-aware merge (additive type
//!   definitions, schema migration semantics) warrants its own design pass.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::{MergeStrategy, VersionedEntry};
use crate::{StrataError, StrataResult};
use strata_core::Value;
use strata_core::VersionedValue;
use strata_storage::Key;

use crate::graph::keys;
use crate::graph::packed;
use crate::graph::types::{EdgeData, GraphMeta, NodeData};

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
    /// per-name additively. `None` means "this side has not touched the
    /// catalog" (NOT "this side deleted everything").
    pub catalog: Option<Value>,
}

/// Per-graph decoded state for one side. The merge algorithm operates on
/// one of these per (graph_name, side) tuple.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct PerGraphState {
    /// Live nodes, keyed by node_id.
    pub nodes: HashMap<String, NodeData>,
    /// Live edges, keyed by `(src, dst, edge_type)` triple. The triple is
    /// the edge identity; `EdgeData` is the edge value.
    pub edges: HashMap<EdgeKey, EdgeData>,
    /// Graph metadata if present.
    pub meta: Option<GraphMeta>,
    /// Other graph keys (ontology, ref index, type index, edge counters).
    /// These are merged as opaque KV via the 14-case matrix; the catalog
    /// is handled separately at the `GraphCellState` level. The edge
    /// counter keys are intentionally INCLUDED here in the decoded view
    /// but are *overwritten* by the projected counters during the merge
    /// (we derive counts from the projected edge set).
    pub other: HashMap<Vec<u8>, Value>,
}

impl PerGraphState {
    /// True iff this side has no live data for the graph: no nodes, no
    /// edges, no metadata, and no other keys (ontology, type index,
    /// counters, etc.).
    ///
    /// Used by the wholesale-delete detector in `compute_graph_merge`:
    /// if `ancestor` is non-empty and one side is empty, that side did
    /// `delete_graph` (or equivalent). This signal is reliable because
    /// branches are COW — an *untouched* side inherits ancestor's data
    /// via the version chain and decodes back to a non-empty
    /// `PerGraphState`, while a side that explicitly wiped the graph
    /// has zero entries to decode.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
            && self.edges.is_empty()
            && self.meta.is_none()
            && self.other.is_empty()
    }
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
    /// **Reserved.** The current `merge_catalog` does per-name additive
    /// set merging and never produces this variant. Kept in the enum
    /// because it's `pub` and removing it would be a breaking API change
    /// with no benefit. `is_fatal()` continues to return `true` for it
    /// so any future caller that DID produce it (e.g. an opt-in
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
    /// One side wholesale-deleted the graph (`delete_graph` or
    /// equivalent) while the other side made post-fork modifications to
    /// the same graph. Honors strategy.
    ///
    /// Without this detection, target-side additions would silently
    /// survive a source-side wholesale delete (or vice versa), leaving
    /// the catalog and storage out of sync. Under `LastWriterWins`, the
    /// source side wins (matching the convention in
    /// [`merge_meta`] / [`merge_other`] for `ModifyDeleteConflict`):
    /// source-deleted means the whole graph is dropped from the
    /// projection; source-modified means source's per-graph state
    /// is restored on top of target's empty state.
    WholeGraphDeleteConflict { graph: String },
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
            // Reverse-ref index keys (`__ref__/{uri}/{graph}/{node_id}`)
            // are top-level (no graph prefix in the key shape) but
            // logically belong to a specific graph. Route them to the
            // per-graph slot so they participate in `merge_per_graph`'s
            // index-consistency post-pass alongside `__by_type__` keys.
            let routed_graph = match graph {
                Some(g) => g,
                None => keys::parse_ref_index_key(user_key_str)
                    .map(|(_, graph, _)| graph)
                    .unwrap_or_default(),
            };
            state
                .graphs
                .entry(routed_graph)
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

    // The set of graph names that have any live data after the per-graph
    // merge. Drives the catalog post-pass below — a graph is in the
    // projected catalog iff it has live per-graph state, ensuring the
    // catalog and storage stay in sync.
    let mut alive_graphs: BTreeSet<String> = BTreeSet::new();

    // Per-graph merge for every graph that appears on any side.
    let mut all_graphs: BTreeSet<String> = BTreeSet::new();
    all_graphs.extend(ancestor.graphs.keys().cloned());
    all_graphs.extend(source.graphs.keys().cloned());
    all_graphs.extend(target.graphs.keys().cloned());

    let empty = PerGraphState::default();
    for graph_name in &all_graphs {
        let anc = ancestor.graphs.get(graph_name).unwrap_or(&empty);
        let src = source.graphs.get(graph_name).unwrap_or(&empty);
        let tgt = target.graphs.get(graph_name).unwrap_or(&empty);

        // Wholesale-delete detection. Ancestor had data and one side has
        // none → that side did `delete_graph` (or equivalent). The
        // signal is reliable because branches are COW: an *untouched*
        // side inherits ancestor's data via the version chain and
        // decodes back to a non-empty `PerGraphState`, while a side that
        // explicitly wiped the graph has zero entries to decode.
        let src_wiped = !anc.is_empty() && src.is_empty();
        let tgt_wiped = !anc.is_empty() && tgt.is_empty();

        // If both sides wholesale-deleted the same graph, that's a
        // convergent merge — no conflict, no writes. Fall through to
        // normal per-graph merge: the per-key 14-case matrix produces
        // `BothDeleted` for every entry and the projected state is
        // empty.
        if src_wiped && !tgt_wiped && !tgt.is_empty() {
            // Source wholesale-deleted; target still has data AND that
            // data differs from the ancestor (otherwise there's nothing
            // to conflict with — the normal merge would just produce
            // SourceDeleted everywhere). Without this branch,
            // target-side additions would be silently kept by the
            // per-key merge as `TargetAdded`, leaving orphaned data
            // with the catalog dropping the graph name.
            //
            // Note we don't need an explicit `tgt != anc` check here
            // because `tgt_wiped == false && tgt non-empty` already
            // implies tgt has SOMETHING; if tgt == anc the normal
            // merge produces SourceDeleted for every entry which is
            // also correct. We fire the conflict only when target has
            // data; whether that data is post-fork modifications or
            // unchanged-from-ancestor, the LWW outcome is the same
            // (drop everything to match source's intent).
            if tgt != anc {
                output
                    .conflicts
                    .push(GraphMergeConflict::WholeGraphDeleteConflict {
                        graph: graph_name.clone(),
                    });
                if strategy == MergeStrategy::LastWriterWins {
                    // Source's delete wins (matching the LWW convention
                    // in `merge_meta` for `DeleteModifyConflict`). Drop
                    // everything for this graph from target's storage.
                    project_full_drop(graph_name, tgt, &mut output);
                }
                // Strict: don't write anything; the outer strict check
                // rejects the merge before any actions are applied.
                continue;
            }
            // tgt == anc → fall through; normal per-key merge produces
            // SourceDeleted everywhere, which is exactly right.
        }

        if tgt_wiped && !src_wiped && !src.is_empty() && src != anc {
            // Target wholesale-deleted; source has post-fork
            // modifications. Symmetric case.
            output
                .conflicts
                .push(GraphMergeConflict::WholeGraphDeleteConflict {
                    graph: graph_name.clone(),
                });
            if strategy == MergeStrategy::LastWriterWins {
                // Source's modifications win (matching the LWW
                // convention in `merge_meta` for `ModifyDeleteConflict`).
                // Restore source's per-graph state on top of target's
                // empty state and mark the graph alive so the catalog
                // post-pass adds it back.
                project_full_restore(graph_name, src, &mut output);
                alive_graphs.insert(graph_name.clone());
            }
            continue;
        }

        // Normal per-graph merge — neither side wholesale-deleted (or
        // they did and the other side is also empty/unchanged, in which
        // case the per-key merge produces the right result naturally).
        let alive = merge_per_graph(graph_name, anc, src, tgt, strategy, &mut output);
        if alive {
            alive_graphs.insert(graph_name.clone());
        }
    }

    // Catalog projection.
    project_catalog(ancestor, source, target, &alive_graphs, &mut output);

    output
}

// =============================================================================
// Whole-graph drop / restore (used by wholesale-delete LWW resolution)
// =============================================================================

/// Emit deletes for every storage key that makes up `target`'s state for
/// `graph_name`. Used when source wholesale-deleted the graph and source
/// wins under LWW: target's per-graph state must be wiped to match
/// source's intent.
///
/// Covers: nodes, packed forward/reverse adjacency lists, edge counters,
/// graph metadata, and per-graph "other" keys. The "other" set includes
/// ontology rows, `{graph}/__by_type__/...` type-index entries, and the
/// top-level `__ref__/...` reverse-ref index entries (which `ingest_entry`
/// routes to the per-graph slot via `parse_ref_index_key`).
fn project_full_drop(graph_name: &str, target: &PerGraphState, output: &mut GraphMergeOutput) {
    // Nodes.
    for node_id in target.nodes.keys() {
        output.deletes.push(keys::node_key(graph_name, node_id));
    }
    // Forward / reverse adjacency lists. One per unique src / dst.
    let fwd_srcs: HashSet<&String> = target.edges.keys().map(|(s, _, _)| s).collect();
    for src in fwd_srcs {
        output.deletes.push(keys::forward_adj_key(graph_name, src));
    }
    let rev_dsts: HashSet<&String> = target.edges.keys().map(|(_, d, _)| d).collect();
    for dst in rev_dsts {
        output.deletes.push(keys::reverse_adj_key(graph_name, dst));
    }
    // Edge counters: one per unique edge type that target had.
    let edge_types: HashSet<&String> = target.edges.keys().map(|(_, _, et)| et).collect();
    for edge_type in edge_types {
        output
            .deletes
            .push(keys::edge_type_count_key(graph_name, edge_type));
    }
    // Graph metadata (`{graph}/__meta__`).
    if target.meta.is_some() {
        output.deletes.push(keys::meta_key(graph_name));
    }
    // Other graph keys (ontology, `{graph}/__by_type__/...`, etc.). All
    // of these live under the `{graph}/...` prefix and are decoded into
    // `target.other` keyed by their raw user-key bytes.
    for key_bytes in target.other.keys() {
        if let Ok(s) = std::str::from_utf8(key_bytes) {
            output.deletes.push(s.to_string());
        }
    }
}

/// Emit puts for every storage key that makes up `source`'s state for
/// `graph_name`. Used when target wholesale-deleted the graph and source
/// wins under LWW: source's per-graph state must be restored on top of
/// target's empty state.
///
/// Re-encodes the packed adjacency lists from source's decoded edge map
/// (the same canonical encoding `project_adjacency_writes` uses) so the
/// restored state is byte-equal to what a fresh write would produce.
fn project_full_restore(graph_name: &str, source: &PerGraphState, output: &mut GraphMergeOutput) {
    // Nodes.
    for (node_id, data) in &source.nodes {
        let user_key = keys::node_key(graph_name, node_id);
        match serde_json::to_string(data) {
            Ok(json) => output.puts.push((user_key, Value::String(json))),
            Err(e) => output
                .conflicts
                .push(GraphMergeConflict::NodeModifyDeleteConflict {
                    graph: graph_name.to_string(),
                    node_id: format!("{node_id} (serialize error: {e})"),
                }),
        }
    }
    // Forward / reverse adjacency lists, grouped by src / dst.
    let mut fwd_by_src: HashMap<&str, Vec<(&str, &str, &EdgeData)>> = HashMap::new();
    let mut rev_by_dst: HashMap<&str, Vec<(&str, &str, &EdgeData)>> = HashMap::new();
    for ((src, dst, edge_type), data) in &source.edges {
        fwd_by_src
            .entry(src.as_str())
            .or_default()
            .push((dst.as_str(), edge_type.as_str(), data));
        rev_by_dst
            .entry(dst.as_str())
            .or_default()
            .push((src.as_str(), edge_type.as_str(), data));
    }
    // Sort for deterministic encoding.
    for edges in fwd_by_src.values_mut() {
        edges.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
    }
    for edges in rev_by_dst.values_mut() {
        edges.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
    }
    for (src, edges) in &fwd_by_src {
        let bytes = packed::encode(edges);
        output
            .puts
            .push((keys::forward_adj_key(graph_name, src), Value::Bytes(bytes)));
    }
    for (dst, edges) in &rev_by_dst {
        let bytes = packed::encode(edges);
        output
            .puts
            .push((keys::reverse_adj_key(graph_name, dst), Value::Bytes(bytes)));
    }
    // Edge counters: one per unique edge type, derived from source's
    // decoded edge set.
    let mut counts: HashMap<&str, u64> = HashMap::new();
    for (_, _, edge_type) in source.edges.keys() {
        *counts.entry(edge_type.as_str()).or_insert(0) += 1;
    }
    for (edge_type, count) in counts {
        output.puts.push((
            keys::edge_type_count_key(graph_name, edge_type),
            Value::String(count.to_string()),
        ));
    }
    // Graph metadata.
    if let Some(meta) = &source.meta {
        match serde_json::to_string(meta) {
            Ok(json) => output
                .puts
                .push((keys::meta_key(graph_name), Value::String(json))),
            Err(_) => output.conflicts.push(GraphMergeConflict::MetaConflict {
                graph: graph_name.to_string(),
            }),
        }
    }
    // Other graph keys.
    for (key_bytes, value) in &source.other {
        if let Ok(s) = std::str::from_utf8(key_bytes) {
            output.puts.push((s.to_string(), value.clone()));
        }
    }
}

// =============================================================================
// Catalog projection
// =============================================================================

/// Project the post-merge `__catalog__` value.
///
/// The catalog is a `Value::String` containing a JSON array of graph
/// names. Two paths produce its contents:
///
/// 1. **Per-graph alive set.** Any graph with live data after the
///    per-graph merge (nodes / edges / meta / other) is in the catalog.
///    This is the source of truth — it's derived from the actual
///    storage state, so the catalog can never disagree with what
///    `list_graphs` would see by scanning per-graph rows.
///
/// 2. **Catalog-only set semantics** for graph names that have NO
///    per-graph state on any side (artificial test scenarios where the
///    catalog is the only signal, or transient states during recipe
///    application). For each such name we apply the additive
///    set-union rule: a name survives unless one side deleted it
///    without the other side re-adding it.
///
/// Concurrent independent `create_graph` / `delete_graph` calls fall
/// into category 1 (because both wrote at least a `__meta__` row), so
/// the additive merge property is preserved automatically. The
/// catalog-only path is the fallback for edge cases.
fn project_catalog(
    ancestor: &GraphCellState,
    source: &GraphCellState,
    target: &GraphCellState,
    alive_graphs: &BTreeSet<String>,
    output: &mut GraphMergeOutput,
) {
    let key = keys::graph_catalog_key().to_string();

    // Decode each side's catalog as a BTreeSet. The fallback semantics
    // matter: a side with no catalog value at all (`None`) means "this
    // side has not touched the catalog", NOT "this side deleted the
    // catalog". Conflating those would silently drop entries on legacy
    // data (graphs created before the catalog key existed).
    let parse_or = |v: Option<&Value>, fallback: &BTreeSet<String>| -> BTreeSet<String> {
        match v {
            Some(Value::String(s)) => serde_json::from_str(s).unwrap_or_else(|_| fallback.clone()),
            Some(_) => fallback.clone(),
            None => fallback.clone(),
        }
    };
    let empty: BTreeSet<String> = BTreeSet::new();
    let anc_catalog = parse_or(ancestor.catalog.as_ref(), &empty);
    let src_catalog = parse_or(source.catalog.as_ref(), &anc_catalog);
    let tgt_catalog = parse_or(target.catalog.as_ref(), &anc_catalog);

    // Start with all alive graphs (path 1).
    let mut projected: BTreeSet<String> = alive_graphs.clone();

    // Walk the union of catalog entries; for any name that has NO
    // per-graph state on any side, fall through to the additive set
    // semantics (path 2).
    for name in anc_catalog
        .iter()
        .chain(src_catalog.iter())
        .chain(tgt_catalog.iter())
    {
        // Skip names already decided by the alive set.
        if projected.contains(name) {
            continue;
        }
        // Skip names that have per-graph state on any side — those are
        // already accounted for by the alive set (and were deliberately
        // dropped if they ended up empty).
        if ancestor.graphs.contains_key(name)
            || source.graphs.contains_key(name)
            || target.graphs.contains_key(name)
        {
            continue;
        }
        // Catalog-only fallback: additive set rule.
        let in_anc = anc_catalog.contains(name);
        let in_src = src_catalog.contains(name);
        let in_tgt = tgt_catalog.contains(name);
        let deleted_by_source = in_anc && !in_src;
        let deleted_by_target = in_anc && !in_tgt;
        if !deleted_by_source && !deleted_by_target && (in_anc || in_src || in_tgt) {
            projected.insert(name.clone());
        }
    }

    // Compute target's "effective" catalog: the explicit `__catalog__`
    // value if one was written, or the implicit lazy-fallback set
    // derived from target's per-graph data otherwise. Comparing against
    // the effective catalog (rather than just the explicit value)
    // preserves the no-op-merge property for legacy data — a no-op
    // merge against a target that has graph data but no catalog row
    // must NOT lazily write a catalog as a side effect.
    let effective_target_catalog: BTreeSet<String> = match target.catalog.as_ref() {
        Some(Value::String(_)) => tgt_catalog.clone(),
        _ => target
            .graphs
            .iter()
            .filter(|(_, state)| !state.is_empty())
            .map(|(name, _)| name.clone())
            .collect(),
    };

    // Emit a Put only when the projected catalog differs from target's
    // effective catalog. The wire format mirrors `list_graphs` /
    // `create_graph`; BTreeSet → Vec preserves deterministic sorted
    // order so equivalent merges produce byte-equal output.
    if projected != effective_target_catalog {
        let projected_vec: Vec<String> = projected.into_iter().collect();
        let json = serde_json::to_string(&projected_vec)
            .expect("BTreeSet<String> JSON serialization is infallible");
        output.puts.push((key, Value::String(json)));
    }
}

// =============================================================================
// Per-graph merge
// =============================================================================

/// Run the per-graph merge for one graph name. Returns `true` if the
/// projected per-graph state has any live data after the merge (used by
/// `compute_graph_merge` to populate the alive set that drives catalog
/// projection).
fn merge_per_graph(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) -> bool {
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

    // Step 7: Project secondary index writes (`__ref__/...` and
    // `{graph}/__by_type__/...`). Derived from `projected_nodes` so that
    // a node-level conflict resolution under LWW can never leave the
    // indexes pointing at the losing version. Skipped in `merge_other`.
    project_node_index_writes(graph_name, target, &projected_nodes, output);

    // Step 8: Merge meta (raw 14-case). Returns whether projected meta is
    // present after the merge.
    let projected_meta_alive = merge_meta(graph_name, ancestor, source, target, strategy, output);

    // Step 9: Merge other graph keys (raw 14-case per key, but exclude
    // edge counters and node-derived secondary indexes). Returns whether
    // any non-index "other" key has a non-deleted projected value.
    let projected_other_alive = merge_other(graph_name, ancestor, source, target, strategy, output);

    // The graph is alive iff anything in the projected state is non-empty.
    // (Index writes don't need their own aliveness check — they're derived
    // from projected_nodes, so they're alive iff projected_nodes is alive.)
    !projected_nodes.is_empty()
        || !projected_edges.is_empty()
        || projected_meta_alive
        || projected_other_alive
}

/// Project secondary node-index writes (`__ref__/...`, `{graph}/__by_type__/...`)
/// from the merged node set.
///
/// Both index families derive deterministically from `NodeData`:
///
///   - `__ref__/{encoded_uri}/{graph}/{node_id}` exists iff the node has
///     `entity_ref = uri`.
///   - `{graph}/__by_type__/{object_type}/{node_id}` exists iff the node
///     has `object_type = object_type`.
///
/// Without this post-pass, the merge would route those keys through the
/// raw 14-case matrix in `merge_other`, treating them as opaque KV. That
/// breaks the invariant that the indexes match the live `NodeData` after
/// any conflict where one side modified the node body and the other side
/// modified the indexes (e.g. source changed `properties` while target
/// changed `entity_ref`): LWW would pick source's NodeData but keep
/// target's index entries, so `nodes_for_entity` would point at a
/// node whose `entity_ref` says something else.
///
/// The fix derives the canonical index set from `projected_nodes`,
/// compares against target's current index entries, and emits puts /
/// deletes to make them match. Stale indexes (target had them, the
/// projected node doesn't justify them anymore) get dropped; missing
/// canonical indexes (projected node has the field but target's storage
/// lacks the row) get added.
fn project_node_index_writes(
    graph_name: &str,
    target: &PerGraphState,
    projected_nodes: &HashMap<String, NodeData>,
    output: &mut GraphMergeOutput,
) {
    // Step 1: build the canonical set of secondary-index keys from the
    // projected node data.
    let mut canonical: HashSet<String> = HashSet::new();
    for (node_id, data) in projected_nodes {
        if let Some(uri) = &data.entity_ref {
            canonical.insert(keys::ref_index_key(uri, graph_name, node_id));
        }
        if let Some(object_type) = &data.object_type {
            canonical.insert(keys::type_index_key(graph_name, object_type, node_id));
        }
    }

    // Step 2: walk target's current index entries (filtered out of
    // `merge_other` and routed to `target.other` by `ingest_entry`) and
    // emit deletes for any that aren't in the canonical set.
    let type_index_prefix_str = format!("{graph_name}/__by_type__/");
    for key_bytes in target.other.keys() {
        let Ok(key_str) = std::str::from_utf8(key_bytes) else {
            continue;
        };
        let is_index =
            key_str.starts_with("__ref__/") || key_str.starts_with(&type_index_prefix_str);
        if !is_index {
            continue;
        }
        if !canonical.contains(key_str) {
            output.deletes.push(key_str.to_string());
        }
    }

    // Step 3: emit puts for canonical entries that target doesn't already
    // have. Index values are always `Value::Null` — the key's *presence*
    // is the signal.
    for canonical_key in &canonical {
        let already_present = target.other.contains_key(canonical_key.as_bytes());
        if !already_present {
            output.puts.push((canonical_key.clone(), Value::Null));
        }
    }
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

/// Returns `true` if the projected meta after the merge is `Some` (used
/// by `merge_per_graph` to decide whether the graph is alive).
fn merge_meta(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) -> bool {
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
        // The projected value equals target's value: alive iff target had
        // meta. (`Unchanged` collapses both "all None" and "all Some(equal)";
        // the bool below distinguishes them.)
        ThreeWay::Unchanged
        | ThreeWay::TargetChanged
        | ThreeWay::BothChangedSame
        | ThreeWay::TargetAdded
        | ThreeWay::BothAddedSame => target.meta.is_some(),
        ThreeWay::TargetDeleted | ThreeWay::BothDeleted => false,
        ThreeWay::SourceChanged(value) | ThreeWay::SourceAdded(value) => {
            try_push_put(value, output)
        }
        ThreeWay::SourceDeleted => {
            output.deletes.push(key);
            false
        }
        ThreeWay::Conflict { source, target } | ThreeWay::BothAddedDifferent { source, target } => {
            output.conflicts.push(GraphMergeConflict::MetaConflict {
                graph: graph_name.to_string(),
            });
            if strategy == MergeStrategy::LastWriterWins {
                try_push_put(source, output)
            } else {
                // Strict — target's value persists in storage.
                let _ = target;
                true
            }
        }
        ThreeWay::DeleteModifyConflict { target } => {
            output.conflicts.push(GraphMergeConflict::MetaConflict {
                graph: graph_name.to_string(),
            });
            if strategy == MergeStrategy::LastWriterWins {
                output.deletes.push(key);
                false
            } else {
                // Strict — target's value persists.
                let _ = target;
                true
            }
        }
        ThreeWay::ModifyDeleteConflict { source } => {
            output.conflicts.push(GraphMergeConflict::MetaConflict {
                graph: graph_name.to_string(),
            });
            if strategy == MergeStrategy::LastWriterWins {
                try_push_put(source, output)
            } else {
                // Strict — target had no meta and we don't write one.
                false
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
///
/// Returns `true` if any "other" key has a non-deleted projected value
/// (used by `merge_per_graph` to decide whether the graph is alive).
fn merge_other(
    graph_name: &str,
    ancestor: &PerGraphState,
    source: &PerGraphState,
    target: &PerGraphState,
    strategy: MergeStrategy,
    output: &mut GraphMergeOutput,
) -> bool {
    let counter_prefix = format!("{graph_name}/__edge_count__/");
    let type_index_prefix_str = format!("{graph_name}/__by_type__/");
    let mut all_keys: HashSet<&Vec<u8>> = HashSet::new();
    all_keys.extend(ancestor.other.keys());
    all_keys.extend(source.other.keys());
    all_keys.extend(target.other.keys());

    let mut any_alive = false;
    for key_bytes in all_keys {
        let key_str_opt = std::str::from_utf8(key_bytes).ok();
        if let Some(key_str) = key_str_opt {
            // Skip edge counters; handled by `project_edge_counter_writes`.
            // (Counters don't count toward "alive" because they're derived
            // from edges, which we already check.)
            if key_str.starts_with(&counter_prefix) {
                continue;
            }
            // Skip secondary indexes derived from node data — they're
            // handled by `project_node_index_writes` after the per-graph
            // merge so they stay consistent with the projected node set
            // even when LWW resolves a node conflict in source's favor
            // while target was the only side that touched the indexes.
            if key_str.starts_with(&type_index_prefix_str) || key_str.starts_with("__ref__/") {
                // Track aliveness for these keys via the projected node
                // set: if any projected node has a corresponding index,
                // the graph is alive. Computed by the caller.
                continue;
            }
        }

        let user_key_str = match key_str_opt {
            Some(s) => s.to_string(),
            None => continue, // shouldn't happen for graph keys
        };

        let a = ancestor.other.get(key_bytes);
        let s = source.other.get(key_bytes);
        let t = target.other.get(key_bytes);

        match classify_three_way(a, s, t) {
            // Target's value (or absence) persists. Alive iff target had
            // a value.
            ThreeWay::Unchanged
            | ThreeWay::TargetChanged
            | ThreeWay::BothChangedSame
            | ThreeWay::TargetAdded
            | ThreeWay::BothAddedSame => {
                if t.is_some() {
                    any_alive = true;
                }
            }
            ThreeWay::TargetDeleted | ThreeWay::BothDeleted => {}
            ThreeWay::SourceChanged(value) | ThreeWay::SourceAdded(value) => {
                output.puts.push((user_key_str, value.clone()));
                any_alive = true;
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
                // Both sides have a value either way → alive.
                any_alive = true;
            }
            ThreeWay::DeleteModifyConflict { target } => {
                output.conflicts.push(GraphMergeConflict::OtherKeyConflict {
                    user_key: key_bytes.clone(),
                    source: None,
                    target: Some(target.clone()),
                });
                if strategy == MergeStrategy::LastWriterWins {
                    output.deletes.push(user_key_str);
                } else {
                    // Strict: target's value persists.
                    any_alive = true;
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
                    any_alive = true;
                }
                // Strict: target had no value, none written.
            }
        }
    }
    any_alive
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
        // same object_type. The merge treats ontology as opaque KV via the
        // 14-case matrix, but the resulting OtherKeyConflict must carry
        // both sides' values so users can inspect what's actually different.
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
        // Both branches concurrently created a different new graph.
        // Additive set-union catalog merge produces [g1,g2,g3] with no
        // conflicts (rather than treating it as a fatal divergence).
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
            "additive catalog merge must not produce CatalogDivergence, got {:?}",
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
            "additive catalog merge must not produce CatalogDivergence, got {:?}",
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
            "additive catalog merge must not produce CatalogDivergence, got {:?}",
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
    fn both_sides_wholesale_delete_is_convergent_no_conflict() {
        // Both branches concurrently `delete_graph("g")` after fork —
        // a convergent intent. The merge must produce zero conflicts
        // and zero writes; under Strict it must NOT reject the merge.
        // Earlier the wholesale-delete branch fired spuriously when
        // both sides were wiped because `tgt != anc` was true (tgt
        // empty, anc non-empty).
        let ancestor = state_with_nodes("g", &[("alice", node(serde_json::json!({})))]);
        let source = empty_state();
        let target = empty_state();

        // LWW must succeed cleanly with no conflicts.
        let lww = compute_graph_merge(&ancestor, &source, &target, MergeStrategy::LastWriterWins);
        assert!(
            lww.conflicts.is_empty(),
            "convergent wholesale-delete must not produce conflicts under LWW; got {:?}",
            lww.conflicts
        );
        // No writes either — target is already empty for "g".
        let g_puts: Vec<_> = lww
            .puts
            .iter()
            .filter(|(k, _)| k.starts_with("g/"))
            .collect();
        let g_deletes: Vec<_> = lww.deletes.iter().filter(|k| k.starts_with("g/")).collect();
        assert!(
            g_puts.is_empty(),
            "convergent wholesale-delete must not emit puts for graph; got {g_puts:?}"
        );
        assert!(
            g_deletes.is_empty(),
            "convergent wholesale-delete must not emit deletes for graph; got {g_deletes:?}"
        );

        // Strict must also succeed cleanly — no conflict to reject on.
        let strict = compute_graph_merge(&ancestor, &source, &target, MergeStrategy::Strict);
        assert!(
            strict.conflicts.is_empty(),
            "convergent wholesale-delete must not produce conflicts under Strict; got {:?}",
            strict.conflicts
        );
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
