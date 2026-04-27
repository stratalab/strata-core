use std::sync::Arc;

use strata_engine::TransactionContext;
use strata_graph::ext::GraphStoreExt;
use strata_graph::types::{Direction, EdgeData, GraphMeta, NodeData};

use crate::bridge::{
    require_branch_exists, serde_json_to_value_public, to_core_branch_id,
    value_to_serde_json_public, Primitives,
};
use crate::convert::convert_result;
use crate::{BranchId, BulkGraphEdge, BulkGraphNode, Output, Result, Value};

#[path = "graph_impl.rs"]
mod graph_impl;

pub(crate) fn create(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    cascade_policy: Option<String>,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    graph_impl::graph_create(primitives, branch, space, graph, cascade_policy)
}

pub(crate) fn delete(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    graph_impl::graph_delete(primitives, branch, space, graph)
}

pub(crate) fn list(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
) -> Result<Output> {
    graph_impl::graph_list(primitives, branch, space)
}

pub(crate) fn get_meta(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    graph_impl::graph_get_meta(primitives, branch, space, graph)
}

pub(crate) fn add_node(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    node_id: String,
    entity_ref: Option<String>,
    properties: Option<Value>,
    object_type: Option<String>,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    graph_impl::graph_add_node(
        primitives,
        branch,
        space,
        graph,
        node_id,
        entity_ref,
        properties,
        object_type,
    )
}

pub(crate) fn get_node(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    node_id: String,
    as_of: Option<u64>,
) -> Result<Output> {
    match as_of {
        Some(as_of) => {
            graph_impl::graph_get_node_at(primitives, branch, space, graph, node_id, as_of)
        }
        None => graph_impl::graph_get_node(primitives, branch, space, graph, node_id),
    }
}

pub(crate) fn remove_node(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    node_id: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    graph_impl::graph_remove_node(primitives, branch, space, graph, node_id)
}

pub(crate) fn list_nodes(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    as_of: Option<u64>,
) -> Result<Output> {
    match as_of {
        Some(as_of) => graph_impl::graph_list_nodes_at(primitives, branch, space, graph, as_of),
        None => graph_impl::graph_list_nodes(primitives, branch, space, graph),
    }
}

pub(crate) fn list_nodes_paginated(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    limit: usize,
    cursor: Option<String>,
) -> Result<Output> {
    graph_impl::graph_list_nodes_paginated(primitives, branch, space, graph, limit, cursor)
}

pub(crate) fn add_edge(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    src: String,
    dst: String,
    edge_type: String,
    weight: Option<f64>,
    properties: Option<Value>,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    graph_impl::graph_add_edge(
        primitives, branch, space, graph, src, dst, edge_type, weight, properties,
    )
}

pub(crate) fn remove_edge(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    src: String,
    dst: String,
    edge_type: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    graph_impl::graph_remove_edge(primitives, branch, space, graph, src, dst, edge_type)
}

pub(crate) fn neighbors(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    node_id: String,
    direction: Option<String>,
    edge_type: Option<String>,
    as_of: Option<u64>,
) -> Result<Output> {
    match as_of {
        Some(as_of) => graph_impl::graph_neighbors_at(
            primitives, branch, space, graph, node_id, direction, edge_type, as_of,
        ),
        None => graph_impl::graph_neighbors(
            primitives, branch, space, graph, node_id, direction, edge_type,
        ),
    }
}

pub(crate) fn bulk_insert(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    nodes: Vec<BulkGraphNode>,
    edges: Vec<BulkGraphEdge>,
    chunk_size: Option<usize>,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    graph_impl::graph_bulk_insert(primitives, branch, space, graph, nodes, edges, chunk_size)
}

pub(crate) fn bfs(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    start: String,
    max_depth: usize,
    max_nodes: Option<usize>,
    edge_types: Option<Vec<String>>,
    direction: Option<String>,
) -> Result<Output> {
    graph_impl::graph_bfs(
        primitives, branch, space, graph, start, max_depth, max_nodes, edge_types, direction,
    )
}

pub(crate) fn define_object_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    definition: Value,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    graph_impl::graph_define_object_type(primitives, branch, space, graph, definition)
}

pub(crate) fn get_object_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    name: String,
) -> Result<Output> {
    graph_impl::graph_get_object_type(primitives, branch, space, graph, name)
}

pub(crate) fn list_object_types(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    graph_impl::graph_list_object_types(primitives, branch, space, graph)
}

pub(crate) fn delete_object_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    name: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    graph_impl::graph_delete_object_type(primitives, branch, space, graph, name)
}

pub(crate) fn define_link_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    definition: Value,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    graph_impl::graph_define_link_type(primitives, branch, space, graph, definition)
}

pub(crate) fn get_link_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    name: String,
) -> Result<Output> {
    graph_impl::graph_get_link_type(primitives, branch, space, graph, name)
}

pub(crate) fn list_link_types(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    graph_impl::graph_list_link_types(primitives, branch, space, graph)
}

pub(crate) fn delete_link_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    name: String,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    graph_impl::graph_delete_link_type(primitives, branch, space, graph, name)
}

pub(crate) fn freeze_ontology(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    prepare_space_write(primitives, &branch, &space)?;
    graph_impl::graph_freeze_ontology(primitives, branch, space, graph)
}

pub(crate) fn ontology_status(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    graph_impl::graph_ontology_status(primitives, branch, space, graph)
}

pub(crate) fn ontology_summary(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    graph_impl::graph_ontology_summary(primitives, branch, space, graph)
}

pub(crate) fn list_ontology_types(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
) -> Result<Output> {
    graph_impl::graph_list_ontology_types(primitives, branch, space, graph)
}

pub(crate) fn nodes_by_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    object_type: String,
) -> Result<Output> {
    graph_impl::graph_nodes_by_type(primitives, branch, space, graph, object_type)
}

pub(crate) fn wcc(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    graph_impl::graph_wcc(primitives, branch, space, graph, top_n, include_all)
}

pub(crate) fn cdlp(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    max_iterations: usize,
    direction: Option<String>,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    graph_impl::graph_cdlp(
        primitives,
        branch,
        space,
        graph,
        max_iterations,
        direction,
        top_n,
        include_all,
    )
}

pub(crate) fn pagerank(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    damping: Option<f64>,
    max_iterations: Option<usize>,
    tolerance: Option<f64>,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    graph_impl::graph_pagerank(
        primitives,
        branch,
        space,
        graph,
        damping,
        max_iterations,
        tolerance,
        top_n,
        include_all,
    )
}

pub(crate) fn lcc(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    graph_impl::graph_lcc(primitives, branch, space, graph, top_n, include_all)
}

pub(crate) fn sssp(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    graph: String,
    source: String,
    direction: Option<String>,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    graph_impl::graph_sssp(
        primitives,
        branch,
        space,
        graph,
        source,
        direction,
        top_n,
        include_all,
    )
}

fn prepare_space_write(primitives: &Arc<Primitives>, branch: &BranchId, space: &str) -> Result<()> {
    require_branch_exists(primitives, branch)?;
    let branch_id = to_core_branch_id(branch)?;
    convert_result(primitives.space.register(branch_id, space))?;
    Ok(())
}

pub(crate) fn execute_in_txn(
    primitives: &Arc<Primitives>,
    ctx: &mut TransactionContext,
    branch_id: strata_core::types::BranchId,
    space: &str,
    command: crate::Command,
) -> Result<Output> {
    match command {
        crate::Command::GraphCreate {
            graph,
            cascade_policy,
            ..
        } => {
            let policy = parse_cascade_policy(cascade_policy.as_deref())?;
            let meta = GraphMeta {
                cascade_policy: policy,
                ..Default::default()
            };
            convert_result(ctx.graph_create(branch_id, space, &graph, meta))?;
            Ok(Output::Unit)
        }
        crate::Command::GraphAddNode {
            graph,
            node_id,
            entity_ref,
            properties,
            object_type,
            ..
        } => {
            let properties = properties
                .map(value_to_serde_json_public)
                .transpose()
                .map_err(crate::Error::from)?;
            let data = NodeData {
                entity_ref,
                properties,
                object_type,
            };
            let backend_state = convert_result(primitives.graph.state())?;
            let created = convert_result(ctx.graph_add_node(
                branch_id,
                space,
                &graph,
                &node_id,
                &data,
                &backend_state,
            ))?;
            Ok(Output::GraphWriteResult { node_id, created })
        }
        crate::Command::GraphGetNode { graph, node_id, .. } => {
            let node = convert_result(ctx.graph_get_node(branch_id, space, &graph, &node_id))?;
            match node {
                Some(data) => {
                    let json = serde_json::to_value(&data).map_err(|error| {
                        crate::Error::Serialization {
                            reason: error.to_string(),
                        }
                    })?;
                    Ok(Output::Maybe(Some(convert_result(
                        serde_json_to_value_public(json),
                    )?)))
                }
                None => Ok(Output::Maybe(None)),
            }
        }
        crate::Command::GraphRemoveNode { graph, node_id, .. } => {
            let backend_state = convert_result(primitives.graph.state())?;
            convert_result(ctx.graph_remove_node(
                branch_id,
                space,
                &graph,
                &node_id,
                &backend_state,
            ))?;
            Ok(Output::Unit)
        }
        crate::Command::GraphListNodes { graph, .. } => {
            let nodes = convert_result(ctx.graph_list_nodes(branch_id, space, &graph))?;
            Ok(Output::Keys(nodes))
        }
        crate::Command::GraphGetMeta { graph, .. } => {
            let meta = convert_result(ctx.graph_get_meta(branch_id, space, &graph))?;
            match meta {
                Some(meta) => {
                    let json = serde_json::to_value(&meta).map_err(|error| {
                        crate::Error::Serialization {
                            reason: error.to_string(),
                        }
                    })?;
                    Ok(Output::Maybe(Some(convert_result(
                        serde_json_to_value_public(json),
                    )?)))
                }
                None => Ok(Output::Maybe(None)),
            }
        }
        crate::Command::GraphList { .. } => {
            let graphs = convert_result(ctx.graph_list(branch_id, space))?;
            Ok(Output::Keys(graphs))
        }
        crate::Command::GraphAddEdge {
            graph,
            src,
            dst,
            edge_type,
            weight,
            properties,
            ..
        } => {
            let properties = properties
                .map(value_to_serde_json_public)
                .transpose()
                .map_err(crate::Error::from)?;
            let data = EdgeData {
                weight: weight.unwrap_or(1.0),
                properties,
            };
            let created = convert_result(
                ctx.graph_add_edge(branch_id, space, &graph, &src, &dst, &edge_type, &data),
            )?;
            Ok(Output::GraphEdgeWriteResult {
                src,
                dst,
                edge_type,
                created,
            })
        }
        crate::Command::GraphRemoveEdge {
            graph,
            src,
            dst,
            edge_type,
            ..
        } => {
            convert_result(
                ctx.graph_remove_edge(branch_id, space, &graph, &src, &dst, &edge_type),
            )?;
            Ok(Output::Unit)
        }
        crate::Command::GraphNeighbors {
            graph,
            node_id,
            direction,
            edge_type,
            ..
        } => {
            let direction = parse_direction(direction.as_deref())?;
            let neighbors = match direction {
                Direction::Outgoing => convert_result(ctx.graph_outgoing_neighbors(
                    branch_id,
                    space,
                    &graph,
                    &node_id,
                    edge_type.as_deref(),
                ))?,
                Direction::Incoming => convert_result(ctx.graph_incoming_neighbors(
                    branch_id,
                    space,
                    &graph,
                    &node_id,
                    edge_type.as_deref(),
                ))?,
                Direction::Both => {
                    let mut outgoing = convert_result(ctx.graph_outgoing_neighbors(
                        branch_id,
                        space,
                        &graph,
                        &node_id,
                        edge_type.as_deref(),
                    ))?;
                    let incoming = convert_result(ctx.graph_incoming_neighbors(
                        branch_id,
                        space,
                        &graph,
                        &node_id,
                        edge_type.as_deref(),
                    ))?;
                    outgoing.extend(incoming);
                    outgoing
                }
            };
            Ok(Output::GraphNeighbors(
                neighbors
                    .into_iter()
                    .map(|neighbor| crate::GraphNeighborHit {
                        node_id: neighbor.node_id,
                        edge_type: neighbor.edge_type,
                        weight: neighbor.edge_data.weight,
                    })
                    .collect(),
            ))
        }
        other => Err(crate::Error::Internal {
            reason: format!("unexpected graph transaction command: {}", other.name()),
            hint: None,
        }),
    }
}

fn parse_direction(direction: Option<&str>) -> Result<Direction> {
    match direction {
        None | Some("outgoing") => Ok(Direction::Outgoing),
        Some("incoming") => Ok(Direction::Incoming),
        Some("both") => Ok(Direction::Both),
        Some(other) => Err(crate::Error::InvalidInput {
            reason: format!(
                "Invalid direction '{}'. Must be 'outgoing', 'incoming', or 'both'.",
                other
            ),
            hint: None,
        }),
    }
}

fn parse_cascade_policy(s: Option<&str>) -> Result<strata_graph::types::CascadePolicy> {
    match s {
        None | Some("ignore") => Ok(strata_graph::types::CascadePolicy::Ignore),
        Some("cascade") => Ok(strata_graph::types::CascadePolicy::Cascade),
        Some("detach") => Ok(strata_graph::types::CascadePolicy::Detach),
        Some(other) => Err(crate::Error::InvalidInput {
            reason: format!(
                "Invalid cascade_policy '{}'. Must be 'cascade', 'detach', or 'ignore'.",
                other
            ),
            hint: None,
        }),
    }
}
