//! Engine bridge for the graph semantic merge (Phase 3b).
//!
//! The graph crate's `merge` module is pure (no engine dispatch). This
//! module wires the algorithm into the engine's `PrimitiveMergeHandler`
//! dispatch via a registration callback. The engine has a slot for a
//! "graph merge plan function" (`GraphMergePlanFn`); calling
//! `register_graph_semantic_merge` populates that slot with the function
//! defined here. Engine's `GraphMergeHandler::plan` then dispatches to
//! it when called.
//!
//! ## Why this layering
//!
//! The engine crate cannot depend on `strata-graph` (graph already
//! depends on engine — adding the reverse edge would be a cycle). The
//! function-pointer registration pattern matches the existing
//! `register_recovery_participant` / `register_vector_recovery` pattern
//! that vector and search use, and avoids exposing graph-internal types
//! through the engine crate's public API.
//!
//! ## Lifecycle
//!
//! Test fixtures and application startup must call
//! `register_graph_semantic_merge` before any `merge_branches` invocation.
//! It is idempotent (the engine's `OnceCell`-backed slot ignores
//! subsequent calls). If never called, the engine falls back to Phase 3's
//! tactical refusal of divergent graph merges.

use std::collections::HashMap;

use strata_core::types::TypeTag;
use strata_core::value::Value;
use strata_core::{StrataError, StrataResult};
use strata_engine::{
    register_graph_merge_plan, ConflictEntry, MergeAction, MergeActionKind, MergePlanCtx,
    PrimitiveMergePlan,
};

use crate::merge::{self, GraphMergeConflict};
use crate::types::{EdgeData, NodeData};

/// Register the graph semantic merge implementation with the engine.
///
/// Idempotent: the engine's `OnceCell` slot only accepts the first call.
/// Test fixtures and application startup should call this exactly once,
/// before any `merge_branches` call. The standard test fixtures register
/// it alongside `register_vector_recovery` / `register_search_recovery`.
pub fn register_graph_semantic_merge() {
    register_graph_merge_plan(graph_plan_fn);
}

/// The function the engine's `GraphMergeHandler::plan` dispatches to.
///
/// Walks every `(space, TypeTag::Graph)` cell in `ctx.typed_entries`,
/// decodes the three sides into structured graph state via
/// `merge::decode_graph_cell_*`, runs the semantic merge algorithm via
/// `merge::compute_graph_merge`, and converts the resulting
/// `GraphMergeOutput` into the engine's `PrimitiveMergePlan` shape.
///
/// `expected_target` for each `MergeAction` is populated from the cell's
/// target side, matching the OCC TOCTOU semantics of the existing
/// `classify_typed_entries` path.
fn graph_plan_fn(ctx: &MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan> {
    let mut actions: Vec<MergeAction> = Vec::new();
    let mut conflicts: Vec<ConflictEntry> = Vec::new();

    for ((space, type_tag), cell) in &ctx.typed_entries.cells {
        if *type_tag != TypeTag::Graph {
            continue;
        }

        // Decode all three sides into structured graph state.
        let ancestor_state = merge::decode_graph_cell_versioned(&cell.ancestor)?;
        let source_state = merge::decode_graph_cell_live(&cell.source)?;
        let target_state = merge::decode_graph_cell_live(&cell.target)?;

        // Compute the semantic merge.
        let output =
            merge::compute_graph_merge(&ancestor_state, &source_state, &target_state, ctx.strategy);

        // Fatal conflicts (referential integrity, catalog divergence) cannot
        // be papered over by any merge strategy — they represent structural
        // impossibilities. Abort the merge with a clear error before any
        // writes happen. Cell-level conflicts (NodeProperty, EdgeData,
        // ModifyDelete, MetaConflict) flow through as ConflictEntry rows
        // and are handled by `merge_branches`'s strict-strategy check.
        let fatal: Vec<&GraphMergeConflict> =
            output.conflicts.iter().filter(|c| c.is_fatal()).collect();
        if !fatal.is_empty() {
            let summary = fatal
                .iter()
                .map(|c| format_fatal_conflict(c))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(StrataError::invalid_input(format!(
                "merge unsupported: graph referential integrity violation in space '{space}': \
                 {summary}. See docs/design/branching/primitive-aware-merge.md §Phase 3b."
            )));
        }

        // Build a (raw_user_key → Value) lookup over target's entries so
        // we can populate `expected_target` for each MergeAction. This
        // matches the OCC TOCTOU semantics that `classify_typed_entries`
        // uses for the generic merge path.
        let target_lookup: HashMap<Vec<u8>, Value> = cell
            .target
            .iter()
            .map(|(key, vv)| (key.user_key.to_vec(), vv.value.clone()))
            .collect();

        // Convert puts → MergeActionKind::Put.
        for (user_key_str, value) in output.puts {
            let raw_key = user_key_str.into_bytes();
            let expected_target = target_lookup.get(&raw_key).cloned();
            actions.push(MergeAction {
                space: space.clone(),
                raw_key,
                type_tag: TypeTag::Graph,
                action: MergeActionKind::Put(value),
                expected_target,
            });
        }

        // Convert deletes → MergeActionKind::Delete.
        for user_key_str in output.deletes {
            let raw_key = user_key_str.into_bytes();
            let expected_target = target_lookup.get(&raw_key).cloned();
            actions.push(MergeAction {
                space: space.clone(),
                raw_key,
                type_tag: TypeTag::Graph,
                action: MergeActionKind::Delete,
                expected_target,
            });
        }

        // Convert each GraphMergeConflict into a ConflictEntry. The key
        // field encodes the conflict shape (node id, edge triple, or a
        // structured keyword for non-key conflicts) so users can grep /
        // parse merge info output and understand what went wrong.
        for conflict in output.conflicts {
            conflicts.push(graph_conflict_to_entry(space, conflict));
        }
    }

    Ok(PrimitiveMergePlan { actions, conflicts })
}

/// Render a fatal `GraphMergeConflict` as a short user-facing string for
/// the merge error message. Only called for conflicts where `is_fatal()`
/// returns true.
fn format_fatal_conflict(conflict: &GraphMergeConflict) -> String {
    use GraphMergeConflict::*;
    match conflict {
        DanglingEdge {
            graph,
            src,
            dst,
            edge_type,
            missing_node,
        } => format!(
            "dangling edge {src}->{dst}/{edge_type} in graph '{graph}' (missing node: {missing_node})"
        ),
        OrphanedReference {
            graph,
            node_id,
            edge_count,
        } => format!(
            "orphaned reference: deleted node '{node_id}' in graph '{graph}' is still referenced by {edge_count} edge(s)"
        ),
        CatalogDivergence => {
            "graph catalog modified differently on both sides (concurrent create_graph/delete_graph). \
             Phase 3b.5 will add additive catalog merging."
                .to_string()
        }
        // Non-fatal conflicts shouldn't reach this function.
        _ => format!("{:?}", conflict),
    }
}

/// Convert a `GraphMergeConflict` into the engine's `ConflictEntry` shape.
fn graph_conflict_to_entry(space: &str, conflict: GraphMergeConflict) -> ConflictEntry {
    use GraphMergeConflict::*;

    let serialize_node =
        |data: &NodeData| -> Option<Value> { serde_json::to_string(data).ok().map(Value::String) };
    let serialize_edge =
        |data: &EdgeData| -> Option<Value> { serde_json::to_string(data).ok().map(Value::String) };

    match conflict {
        NodePropertyConflict {
            graph,
            node_id,
            source,
            target,
        } => ConflictEntry {
            key: format!("graph:{graph}:node:{node_id}"),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: serialize_node(&source),
            target_value: serialize_node(&target),
        },
        EdgeDataConflict {
            graph,
            src,
            dst,
            edge_type,
            source,
            target,
        } => ConflictEntry {
            key: format!("graph:{graph}:edge:{src}->{dst}/{edge_type}"),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: serialize_edge(&source),
            target_value: serialize_edge(&target),
        },
        NodeModifyDeleteConflict { graph, node_id } => ConflictEntry {
            key: format!("graph:{graph}:node-modify-delete:{node_id}"),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: None,
            target_value: None,
        },
        EdgeModifyDeleteConflict {
            graph,
            src,
            dst,
            edge_type,
        } => ConflictEntry {
            key: format!("graph:{graph}:edge-modify-delete:{src}->{dst}/{edge_type}"),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: None,
            target_value: None,
        },
        MetaConflict { graph } => ConflictEntry {
            key: format!("graph:{graph}:meta"),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: None,
            target_value: None,
        },
        DanglingEdge {
            graph,
            src,
            dst,
            edge_type,
            missing_node,
        } => ConflictEntry {
            key: format!(
                "graph:{graph}:dangling-edge:{src}->{dst}/{edge_type} (missing node: {missing_node})"
            ),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: None,
            target_value: None,
        },
        OrphanedReference {
            graph,
            node_id,
            edge_count,
        } => ConflictEntry {
            key: format!(
                "graph:{graph}:orphaned-reference:{node_id} ({edge_count} referencing edges)"
            ),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: None,
            target_value: None,
        },
        CatalogDivergence => ConflictEntry {
            key: "graph:catalog-divergence".to_string(),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: None,
            target_value: None,
        },
        OtherKeyConflict {
            user_key,
            source,
            target,
        } => ConflictEntry {
            key: format!(
                "graph:other-key-conflict:{}",
                String::from_utf8_lossy(&user_key)
            ),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: source,
            target_value: target,
        },
    }
}
