//! Graph command handlers.

use std::sync::Arc;

use strata_core::Value;
use strata_engine::graph::types::{
    BfsOptions, CascadePolicy, CdlpOptions, Direction, EdgeData, GraphMeta, LinkTypeDef, NodeData,
    ObjectTypeDef, PageRankOptions, SsspOptions,
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
        ..Default::default()
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
#[allow(clippy::too_many_arguments)]
pub fn graph_add_node(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    node_id: String,
    entity_ref: Option<String>,
    properties: Option<Value>,
    object_type: Option<String>,
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
        object_type,
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

/// Handle GraphListNodesPaginated command.
pub fn graph_list_nodes_paginated(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    limit: usize,
    cursor: Option<String>,
) -> Result<Output> {
    use strata_engine::graph::types::PageRequest;
    let core_branch = to_core_branch_id(&branch)?;
    let page = PageRequest { limit, cursor };
    let result = convert_result(p.graph.list_nodes_paginated(core_branch, &graph, page))?;
    Ok(Output::GraphPage {
        items: result.items,
        next_cursor: result.next_cursor,
    })
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
                    object_type: n.object_type,
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

// =============================================================================
// Ontology handlers
// =============================================================================

/// Handle GraphDefineObjectType command.
pub fn graph_define_object_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    definition: Value,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let json = crate::bridge::value_to_serde_json_public(definition)?;
    let def: ObjectTypeDef = serde_json::from_value(json).map_err(|e| Error::InvalidInput {
        reason: format!("Invalid object type definition: {}", e),
    })?;
    convert_result(p.graph.define_object_type(core_branch, &graph, def))?;
    Ok(Output::Unit)
}

/// Handle GraphGetObjectType command.
pub fn graph_get_object_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    name: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let def = convert_result(p.graph.get_object_type(core_branch, &graph, &name))?;
    match def {
        Some(d) => {
            let json = serde_json::to_value(&d).map_err(|e| Error::Serialization {
                reason: e.to_string(),
            })?;
            Ok(Output::Maybe(Some(serde_json_to_value(json)?)))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Handle GraphListObjectTypes command.
pub fn graph_list_object_types(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let names = convert_result(p.graph.list_object_types(core_branch, &graph))?;
    Ok(Output::Keys(names))
}

/// Handle GraphDeleteObjectType command.
pub fn graph_delete_object_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    name: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    convert_result(p.graph.delete_object_type(core_branch, &graph, &name))?;
    Ok(Output::Unit)
}

/// Handle GraphDefineLinkType command.
pub fn graph_define_link_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    definition: Value,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let json = crate::bridge::value_to_serde_json_public(definition)?;
    let def: LinkTypeDef = serde_json::from_value(json).map_err(|e| Error::InvalidInput {
        reason: format!("Invalid link type definition: {}", e),
    })?;
    convert_result(p.graph.define_link_type(core_branch, &graph, def))?;
    Ok(Output::Unit)
}

/// Handle GraphGetLinkType command.
pub fn graph_get_link_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    name: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let def = convert_result(p.graph.get_link_type(core_branch, &graph, &name))?;
    match def {
        Some(d) => {
            let json = serde_json::to_value(&d).map_err(|e| Error::Serialization {
                reason: e.to_string(),
            })?;
            Ok(Output::Maybe(Some(serde_json_to_value(json)?)))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Handle GraphListLinkTypes command.
pub fn graph_list_link_types(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let names = convert_result(p.graph.list_link_types(core_branch, &graph))?;
    Ok(Output::Keys(names))
}

/// Handle GraphDeleteLinkType command.
pub fn graph_delete_link_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    name: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    convert_result(p.graph.delete_link_type(core_branch, &graph, &name))?;
    Ok(Output::Unit)
}

/// Handle GraphFreezeOntology command.
pub fn graph_freeze_ontology(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    convert_result(p.graph.freeze_ontology(core_branch, &graph))?;
    Ok(Output::Unit)
}

/// Handle GraphOntologyStatus command.
pub fn graph_ontology_status(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let status = convert_result(p.graph.ontology_status(core_branch, &graph))?;
    match status {
        Some(s) => {
            let json = serde_json::to_value(s).map_err(|e| Error::Serialization {
                reason: e.to_string(),
            })?;
            Ok(Output::Maybe(Some(serde_json_to_value(json)?)))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Handle GraphOntologySummary command.
pub fn graph_ontology_summary(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let summary = convert_result(p.graph.ontology_summary(core_branch, &graph))?;
    match summary {
        Some(s) => {
            let json = serde_json::to_value(&s).map_err(|e| Error::Serialization {
                reason: e.to_string(),
            })?;
            Ok(Output::Maybe(Some(serde_json_to_value(json)?)))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Handle GraphNodesByType command.
pub fn graph_nodes_by_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    object_type: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let node_ids = convert_result(p.graph.nodes_by_type(core_branch, &graph, &object_type))?;
    Ok(Output::Keys(node_ids))
}

// =============================================================================
// Analytics handlers
// =============================================================================

/// Handle GraphWcc command.
pub fn graph_wcc(p: &Arc<Primitives>, branch: BranchId, graph: String) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let result = convert_result(p.graph.wcc(core_branch, &graph))?;
    Ok(Output::GraphAnalyticsU64(
        crate::types::GraphAnalyticsU64Result {
            algorithm: "wcc".to_string(),
            result: result.components,
        },
    ))
}

/// Handle GraphCdlp command.
pub fn graph_cdlp(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    max_iterations: usize,
    direction: Option<String>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let dir = parse_direction(direction.as_deref())?;
    let opts = CdlpOptions {
        max_iterations,
        direction: dir,
    };
    let result = convert_result(p.graph.cdlp(core_branch, &graph, opts))?;
    Ok(Output::GraphAnalyticsU64(
        crate::types::GraphAnalyticsU64Result {
            algorithm: "cdlp".to_string(),
            result: result.labels,
        },
    ))
}

/// Handle GraphPagerank command.
pub fn graph_pagerank(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    damping: Option<f64>,
    max_iterations: Option<usize>,
    tolerance: Option<f64>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let defaults = PageRankOptions::default();
    let opts = PageRankOptions {
        damping: damping.unwrap_or(defaults.damping),
        max_iterations: max_iterations.unwrap_or(defaults.max_iterations),
        tolerance: tolerance.unwrap_or(defaults.tolerance),
    };
    let result = convert_result(p.graph.pagerank(core_branch, &graph, opts))?;
    Ok(Output::GraphAnalyticsF64(
        crate::types::GraphAnalyticsF64Result {
            algorithm: "pagerank".to_string(),
            result: result.ranks,
            iterations: Some(result.iterations),
        },
    ))
}

/// Handle GraphLcc command.
pub fn graph_lcc(p: &Arc<Primitives>, branch: BranchId, graph: String) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let result = convert_result(p.graph.lcc(core_branch, &graph))?;
    Ok(Output::GraphAnalyticsF64(
        crate::types::GraphAnalyticsF64Result {
            algorithm: "lcc".to_string(),
            result: result.coefficients,
            iterations: None,
        },
    ))
}

/// Handle GraphSssp command.
pub fn graph_sssp(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    source: String,
    direction: Option<String>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let dir = parse_direction(direction.as_deref())?;
    let opts = SsspOptions { direction: dir };
    let result = convert_result(p.graph.sssp(core_branch, &graph, &source, opts))?;
    Ok(Output::GraphAnalyticsF64(
        crate::types::GraphAnalyticsF64Result {
            algorithm: "sssp".to_string(),
            result: result.distances,
            iterations: None,
        },
    ))
}

/// Convert serde_json::Value to strata_core::Value.
fn serde_json_to_value(json: serde_json::Value) -> Result<Value> {
    crate::bridge::serde_json_to_value_public(json).map_err(|e| Error::Serialization {
        reason: e.to_string(),
    })
}
