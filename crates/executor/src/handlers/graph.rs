//! Graph command handlers.

use std::sync::Arc;

use strata_core::Value;
use strata_engine::graph::types::{
    BfsOptions, CascadePolicy, Direction, EdgeData, GraphMeta, NodeData,
};

use crate::bridge::{to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::types::BranchId;
use crate::{Error, Output, Result};

/// Parse a direction string to a Direction enum.
fn parse_direction(s: Option<&str>) -> Result<Direction> {
    match s {
        None | Some("outgoing") => Ok(Direction::Outgoing),
        Some("incoming") => Ok(Direction::Incoming),
        Some("both") => Ok(Direction::Both),
        Some(other) => Err(Error::InvalidInput {
            reason: format!(
                "Invalid direction '{}'. Must be 'outgoing', 'incoming', or 'both'.",
                other
            ),
        }),
    }
}

/// Parse a cascade policy string.
fn parse_cascade_policy(s: Option<&str>) -> Result<CascadePolicy> {
    match s {
        None | Some("ignore") => Ok(CascadePolicy::Ignore),
        Some("cascade") => Ok(CascadePolicy::Cascade),
        Some("detach") => Ok(CascadePolicy::Detach),
        Some(other) => Err(Error::InvalidInput {
            reason: format!(
                "Invalid cascade_policy '{}'. Must be 'cascade', 'detach', or 'ignore'.",
                other
            ),
        }),
    }
}

/// Handle GraphCreate command.
pub fn graph_create(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    cascade_policy: Option<String>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let policy = parse_cascade_policy(cascade_policy.as_deref())?;
    let meta = GraphMeta {
        cascade_policy: policy,
    };
    convert_result(p.graph.create_graph(core_branch, &graph, Some(meta)))?;
    Ok(Output::Unit)
}

/// Handle GraphDelete command.
pub fn graph_delete(p: &Arc<Primitives>, branch: BranchId, graph: String) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    convert_result(p.graph.delete_graph(core_branch, &graph))?;
    Ok(Output::Unit)
}

/// Handle GraphList command.
pub fn graph_list(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let graphs = convert_result(p.graph.list_graphs(core_branch))?;
    Ok(Output::Keys(graphs))
}

/// Handle GraphGetMeta command.
pub fn graph_get_meta(p: &Arc<Primitives>, branch: BranchId, graph: String) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let meta = convert_result(p.graph.get_graph_meta(core_branch, &graph))?;
    match meta {
        Some(m) => {
            let json = serde_json::to_value(&m).map_err(|e| Error::Serialization {
                reason: e.to_string(),
            })?;
            Ok(Output::Maybe(Some(serde_json_to_value(json)?)))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Handle GraphAddNode command.
pub fn graph_add_node(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    node_id: String,
    entity_ref: Option<String>,
    properties: Option<Value>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let props = match properties {
        Some(v) => {
            let json = crate::bridge::value_to_serde_json_public(v)?;
            Some(json)
        }
        None => None,
    };
    let data = NodeData {
        entity_ref,
        properties: props,
    };
    convert_result(p.graph.add_node(core_branch, &graph, &node_id, data))?;
    Ok(Output::Unit)
}

/// Handle GraphGetNode command.
pub fn graph_get_node(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    node_id: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let node = convert_result(p.graph.get_node(core_branch, &graph, &node_id))?;
    match node {
        Some(data) => {
            let json = serde_json::to_value(&data).map_err(|e| Error::Serialization {
                reason: e.to_string(),
            })?;
            Ok(Output::Maybe(Some(serde_json_to_value(json)?)))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Handle GraphRemoveNode command.
pub fn graph_remove_node(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    node_id: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    convert_result(p.graph.remove_node(core_branch, &graph, &node_id))?;
    Ok(Output::Unit)
}

/// Handle GraphListNodes command.
pub fn graph_list_nodes(p: &Arc<Primitives>, branch: BranchId, graph: String) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let nodes = convert_result(p.graph.list_nodes(core_branch, &graph))?;
    Ok(Output::Keys(nodes))
}

/// Handle GraphAddEdge command.
#[allow(clippy::too_many_arguments)]
pub fn graph_add_edge(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    src: String,
    dst: String,
    edge_type: String,
    weight: Option<f64>,
    properties: Option<Value>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let props = match properties {
        Some(v) => {
            let json = crate::bridge::value_to_serde_json_public(v)?;
            Some(json)
        }
        None => None,
    };
    let data = EdgeData {
        weight: weight.unwrap_or(1.0),
        properties: props,
    };
    convert_result(
        p.graph
            .add_edge(core_branch, &graph, &src, &dst, &edge_type, data),
    )?;
    Ok(Output::Unit)
}

/// Handle GraphRemoveEdge command.
pub fn graph_remove_edge(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    src: String,
    dst: String,
    edge_type: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    convert_result(
        p.graph
            .remove_edge(core_branch, &graph, &src, &dst, &edge_type),
    )?;
    Ok(Output::Unit)
}

/// Handle GraphNeighbors command.
pub fn graph_neighbors(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    node_id: String,
    direction: Option<String>,
    edge_type: Option<String>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let dir = parse_direction(direction.as_deref())?;
    let neighbors = convert_result(p.graph.neighbors(
        core_branch,
        &graph,
        &node_id,
        dir,
        edge_type.as_deref(),
    ))?;

    let hits: Vec<crate::types::GraphNeighborHit> = neighbors
        .into_iter()
        .map(|n| crate::types::GraphNeighborHit {
            node_id: n.node_id,
            edge_type: n.edge_type,
            weight: n.edge_data.weight,
        })
        .collect();

    Ok(Output::GraphNeighbors(hits))
}

/// Handle GraphBfs command.
#[allow(clippy::too_many_arguments)]
pub fn graph_bfs(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    start: String,
    max_depth: usize,
    max_nodes: Option<usize>,
    edge_types: Option<Vec<String>>,
    direction: Option<String>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let dir = parse_direction(direction.as_deref())?;
    let opts = BfsOptions {
        max_depth,
        max_nodes,
        edge_types,
        direction: dir,
    };

    let result = convert_result(p.graph.bfs(core_branch, &graph, &start, opts))?;

    Ok(Output::GraphBfs(crate::types::GraphBfsResult {
        visited: result.visited,
        depths: result.depths,
        edges: result.edges,
    }))
}

/// Handle GraphBulkInsert command.
pub fn graph_bulk_insert(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    nodes: Vec<crate::types::BulkGraphNode>,
    edges: Vec<crate::types::BulkGraphEdge>,
    chunk_size: Option<usize>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;

    // Convert BulkGraphNode → (String, NodeData)
    let node_data: Vec<(String, NodeData)> = nodes
        .into_iter()
        .map(|n| {
            let props = match n.properties {
                Some(v) => Some(crate::bridge::value_to_serde_json_public(v)?),
                None => None,
            };
            Ok((
                n.node_id,
                NodeData {
                    entity_ref: n.entity_ref,
                    properties: props,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    // Convert BulkGraphEdge → (String, String, String, EdgeData)
    let edge_data: Vec<(String, String, String, EdgeData)> = edges
        .into_iter()
        .map(|e| {
            let props = match e.properties {
                Some(v) => Some(crate::bridge::value_to_serde_json_public(v)?),
                None => None,
            };
            Ok((
                e.src,
                e.dst,
                e.edge_type,
                EdgeData {
                    weight: e.weight.unwrap_or(1.0),
                    properties: props,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let (ni, ei) = convert_result(p.graph.bulk_insert(
        core_branch,
        &graph,
        &node_data,
        &edge_data,
        chunk_size,
    ))?;

    Ok(Output::GraphBulkInsertResult {
        nodes_inserted: ni as u64,
        edges_inserted: ei as u64,
    })
}

/// Convert serde_json::Value to strata_core::Value.
fn serde_json_to_value(json: serde_json::Value) -> Result<Value> {
    crate::bridge::serde_json_to_value_public(json).map_err(|e| Error::Serialization {
        reason: e.to_string(),
    })
}
