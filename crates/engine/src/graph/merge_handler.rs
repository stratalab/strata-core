//! Engine bridge for the graph semantic merge.
//!
//! The graph `merge` module is pure (no engine dispatch). This module wires
//! the algorithm into the engine's `PrimitiveMergeHandler` dispatch via
//! per-database registration. `GraphSubsystem::initialize()` calls
//! `db.merge_registry().register_graph(graph_plan_fn)` to populate the
//! per-database slot. Engine's `GraphMergeHandler::plan` then dispatches to it
//! when called.
//!
//! ## Why this layering
//!
//! The function-pointer registration pattern keeps graph merge planning
//! decoupled from the primitive merge dispatcher and avoids widening the
//! dispatcher's public API around graph-internal types.

use std::collections::HashMap;

use crate::{ConflictEntry, MergeAction, MergeActionKind, MergePlanCtx, PrimitiveMergePlan};
use crate::{StrataError, StrataResult};
use strata_core::Value;
use strata_storage::TypeTag;

use crate::graph::merge::{self, GraphMergeConflict};
use crate::graph::types::{EdgeData, NodeData};

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
pub fn graph_plan_fn(ctx: &MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan> {
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
                 {summary}. See docs/design/branching/primitive-aware-merge.md."
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
            // `merge_catalog` is additive, so this variant is no longer
            // produced by the current algorithm. Kept here for forward
            // compatibility — any future caller (e.g. an opt-in
            // strict-catalog mode) that DID push it would still abort the
            // merge with this message.
            "graph catalog modified differently on both sides — \
             unable to project an additive merge result"
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
        WholeGraphDeleteConflict { graph } => ConflictEntry {
            key: format!("graph:{graph}:whole-graph-delete-modify"),
            primitive: strata_core::PrimitiveType::Graph,
            space: space.to_string(),
            source_value: None,
            target_value: None,
        },
    }
}
