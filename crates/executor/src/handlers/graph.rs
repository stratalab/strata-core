//! Graph command handlers.

use std::sync::Arc;

use strata_core::Value;
use strata_graph::types::{
    BfsOptions, CascadePolicy, CdlpOptions, Direction, EdgeData, GraphMeta, LinkTypeDef, NodeData,
    ObjectTypeDef, PageRankOptions, SsspOptions,
};

use crate::bridge::{to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::types::BranchId;
use crate::{Error, Output, Result};

/// Enrich a graph error: if the error indicates the graph doesn't exist,
/// convert it to `GraphNotFound` with fuzzy-match suggestions.
fn enrich_graph_error(
    p: &Primitives,
    branch_id: strata_core::types::BranchId,
    graph: &str,
    err: Error,
) -> Error {
    match &err {
        Error::ConstraintViolation { .. } | Error::InvalidInput { .. } | Error::Internal { .. } => {
            // Check if this might be a "graph not found" scenario
            let graphs = match p.graph.list_graphs(branch_id) {
                Ok(g) => g,
                Err(_) => return err,
            };
            if graphs.iter().all(|g| g != graph) {
                let hint = crate::suggest::format_hint("graphs", &graphs, graph, 2);
                return Error::GraphNotFound {
                    graph: graph.to_string(),
                    hint,
                };
            }
            err
        }
        _ => err,
    }
}

/// Enrich ontology validation errors with fuzzy-match suggestions.
///
/// When a node's object_type or an edge's edge_type is "not declared in frozen ontology",
/// suggest similar types from the ontology.
fn enrich_ontology_error(
    p: &Primitives,
    branch_id: strata_core::types::BranchId,
    graph: &str,
    err: Error,
) -> Error {
    let msg = match &err {
        Error::ConstraintViolation { reason, .. } => reason.clone(),
        Error::InvalidInput { reason, .. } => reason.clone(),
        _ => return err,
    };

    // Pattern: "Object type 'X' is not declared in frozen ontology for graph 'G'"
    if msg.contains("is not declared in frozen ontology") {
        if msg.starts_with("Object type") {
            if let Ok(types) = p.graph.list_object_types(branch_id, graph) {
                if let Some(type_name) = extract_quoted(&msg) {
                    let hint = crate::suggest::format_hint("object types", &types, &type_name, 2);
                    return Error::InvalidInput { reason: msg, hint };
                }
            }
        } else if msg.starts_with("Edge type") {
            if let Ok(types) = p.graph.list_link_types(branch_id, graph) {
                if let Some(type_name) = extract_quoted(&msg) {
                    let hint = crate::suggest::format_hint("link types", &types, &type_name, 2);
                    return Error::InvalidInput { reason: msg, hint };
                }
            }
        }
    }
    err
}

/// Extract the first single-quoted substring from a message (e.g. "'Foo'" → "Foo").
fn extract_quoted(msg: &str) -> Option<String> {
    let start = msg.find('\'')?;
    let rest = &msg[start + 1..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

/// Parse a direction string to a Direction enum.
pub(crate) fn parse_direction(s: Option<&str>) -> Result<Direction> {
    match s {
        None | Some("outgoing") => Ok(Direction::Outgoing),
        Some("incoming") => Ok(Direction::Incoming),
        Some("both") => Ok(Direction::Both),
        Some(other) => Err(Error::InvalidInput {
            reason: format!(
                "Invalid direction '{}'. Must be 'outgoing', 'incoming', or 'both'.",
                other
            ),
            hint: None,
        }),
    }
}

/// Parse a cascade policy string.
pub(crate) fn parse_cascade_policy(s: Option<&str>) -> Result<CascadePolicy> {
    match s {
        None | Some("ignore") => Ok(CascadePolicy::Ignore),
        Some("cascade") => Ok(CascadePolicy::Cascade),
        Some("detach") => Ok(CascadePolicy::Detach),
        Some(other) => Err(Error::InvalidInput {
            reason: format!(
                "Invalid cascade_policy '{}'. Must be 'cascade', 'detach', or 'ignore'.",
                other
            ),
            hint: None,
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
    let meta = convert_result(p.graph.get_graph_meta(core_branch, &graph))
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;
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
    let created = convert_result(p.graph.add_node(core_branch, &graph, &node_id, data))
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))
        .map_err(|e| enrich_ontology_error(p, core_branch, &graph, e))?;
    Ok(Output::GraphWriteResult { node_id, created })
}

/// Handle GraphGetNode command.
pub fn graph_get_node(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    node_id: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let node = convert_result(p.graph.get_node(core_branch, &graph, &node_id))
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;
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
    convert_result(p.graph.remove_node(core_branch, &graph, &node_id))
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;
    Ok(Output::Unit)
}

/// Handle GraphListNodes command.
pub fn graph_list_nodes(p: &Arc<Primitives>, branch: BranchId, graph: String) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let nodes = convert_result(p.graph.list_nodes(core_branch, &graph))
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;
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
    use strata_graph::types::PageRequest;
    let core_branch = to_core_branch_id(&branch)?;
    let page = PageRequest { limit, cursor };
    let result = convert_result(p.graph.list_nodes_paginated(core_branch, &graph, page))
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;
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
    let created =
        convert_result(
            p.graph
                .add_edge(core_branch, &graph, &src, &dst, &edge_type, data),
        )
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))
        .map_err(|e| enrich_ontology_error(p, core_branch, &graph, e))?;
    Ok(Output::GraphEdgeWriteResult {
        src,
        dst,
        edge_type,
        created,
    })
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
    )
    .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;
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
    let neighbors =
        convert_result(
            p.graph
                .neighbors(core_branch, &graph, &node_id, dir, edge_type.as_deref()),
        )
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;

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

    let result = convert_result(p.graph.bfs(core_branch, &graph, &start, opts))
        .map_err(|e| enrich_graph_error(p, core_branch, &graph, e))?;

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
        hint: None,
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
        hint: None,
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

/// Handle GraphListOntologyTypes command — lists both object and link types.
pub fn graph_list_ontology_types(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let mut names = convert_result(p.graph.list_object_types(core_branch, &graph))?;
    let link_names = convert_result(p.graph.list_link_types(core_branch, &graph))?;
    names.extend(link_names);
    names.sort();
    names.dedup();
    Ok(Output::Keys(names))
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

const DEFAULT_TOP_N: usize = 10;
const GROUP_SAMPLE_SIZE: usize = 5;

/// Compute distribution statistics from a sorted slice of f64 values.
fn compute_distribution(sorted: &[f64]) -> crate::types::ScoreDistribution {
    let n = sorted.len();
    if n == 0 {
        return crate::types::ScoreDistribution {
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            median: 0.0,
            p90: 0.0,
            p99: 0.0,
        };
    }
    let sum: f64 = sorted.iter().sum();
    let mean = sum / n as f64;
    let median = if n % 2 == 0 {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    } else {
        sorted[n / 2]
    };
    // Nearest-rank percentile: index = ceil(p * n) - 1
    let p90_idx = ((0.9_f64 * n as f64).ceil() as usize)
        .saturating_sub(1)
        .min(n - 1);
    let p99_idx = ((0.99_f64 * n as f64).ceil() as usize)
        .saturating_sub(1)
        .min(n - 1);
    crate::types::ScoreDistribution {
        min: sorted[0],
        max: sorted[n - 1],
        mean,
        median,
        p90: sorted[p90_idx],
        p99: sorted[p99_idx],
    }
}

/// Build a group summary from raw u64 per-node results.
fn build_group_summary(
    algorithm: &str,
    graph: &str,
    raw: std::collections::HashMap<String, u64>,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> crate::types::GraphGroupSummary {
    let top_n = top_n.unwrap_or(DEFAULT_TOP_N);
    let node_count = raw.len();

    // Invert: group nodes by label/component ID
    let mut groups: std::collections::HashMap<u64, Vec<String>> = std::collections::HashMap::new();
    for (node, label) in &raw {
        groups.entry(*label).or_default().push(node.clone());
    }

    let group_count = groups.len();

    // Sort groups by size descending
    let mut group_vec: Vec<(u64, Vec<String>)> = groups.into_iter().collect();
    group_vec.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    let largest_group_size = group_vec.first().map(|(_, v)| v.len()).unwrap_or(0);

    let top_groups: Vec<crate::types::GroupInfo> = group_vec
        .into_iter()
        .take(top_n)
        .map(|(id, mut nodes)| {
            nodes.sort();
            let size = nodes.len();
            nodes.truncate(GROUP_SAMPLE_SIZE);
            crate::types::GroupInfo {
                id,
                size,
                sample_nodes: nodes,
            }
        })
        .collect();

    let all = if include_all.unwrap_or(false) {
        Some(raw)
    } else {
        None
    };

    crate::types::GraphGroupSummary {
        algorithm: algorithm.to_string(),
        graph: graph.to_string(),
        node_count,
        group_count,
        largest_group_size,
        groups: top_groups,
        all,
    }
}

/// Build a score summary from raw f64 per-node results.
///
/// If `descending` is true, top nodes are those with the highest scores (PageRank, LCC).
/// If false, top nodes are those with the lowest scores (SSSP distances).
fn build_score_summary(
    algorithm: &str,
    graph: &str,
    raw: std::collections::HashMap<String, f64>,
    top_n: Option<usize>,
    include_all: Option<bool>,
    descending: bool,
) -> crate::types::GraphScoreSummary {
    let top_n = top_n.unwrap_or(DEFAULT_TOP_N);
    let node_count = raw.len();

    // Sort entries for top-N selection
    let mut entries: Vec<(String, f64)> = raw.iter().map(|(k, v)| (k.clone(), *v)).collect();
    if descending {
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        entries.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    }

    let top_nodes: Vec<crate::types::NodeScore> = entries
        .iter()
        .take(top_n)
        .map(|(node_id, score)| crate::types::NodeScore {
            node_id: node_id.clone(),
            score: *score,
        })
        .collect();

    // Compute distribution from all values sorted ascending
    let mut all_values: Vec<f64> = raw.values().copied().collect();
    all_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let distribution = compute_distribution(&all_values);

    let all = if include_all.unwrap_or(false) {
        Some(raw)
    } else {
        None
    };

    crate::types::GraphScoreSummary {
        algorithm: algorithm.to_string(),
        graph: graph.to_string(),
        node_count,
        top_nodes,
        distribution,
        iterations: None,
        converged: None,
        global_clustering_coefficient: None,
        zero_count: None,
        source: None,
        farthest: None,
        all,
    }
}

/// Handle GraphWcc command.
pub fn graph_wcc(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let result = convert_result(p.graph.wcc(core_branch, &graph))?;
    let summary = build_group_summary("wcc", &graph, result.components, top_n, include_all);
    Ok(Output::GraphGroupSummary(summary))
}

/// Handle GraphCdlp command.
pub fn graph_cdlp(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    max_iterations: usize,
    direction: Option<String>,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let dir = parse_direction(direction.as_deref())?;
    let opts = CdlpOptions {
        max_iterations,
        direction: dir,
    };
    let result = convert_result(p.graph.cdlp(core_branch, &graph, opts))?;
    let summary = build_group_summary("cdlp", &graph, result.labels, top_n, include_all);
    Ok(Output::GraphGroupSummary(summary))
}

/// Handle GraphPagerank command.
#[allow(clippy::too_many_arguments)]
pub fn graph_pagerank(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    damping: Option<f64>,
    max_iterations: Option<usize>,
    tolerance: Option<f64>,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let defaults = PageRankOptions::default();
    let max_iter = max_iterations.unwrap_or(defaults.max_iterations);
    let opts = PageRankOptions {
        damping: damping.unwrap_or(defaults.damping),
        max_iterations: max_iter,
        tolerance: tolerance.unwrap_or(defaults.tolerance),
    };
    let result = convert_result(p.graph.pagerank(core_branch, &graph, opts))?;
    let mut summary =
        build_score_summary("pagerank", &graph, result.ranks, top_n, include_all, true);
    summary.iterations = Some(result.iterations);
    summary.converged = Some(result.iterations < max_iter);
    Ok(Output::GraphScoreSummary(summary))
}

/// Handle GraphLcc command.
pub fn graph_lcc(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let result = convert_result(p.graph.lcc(core_branch, &graph))?;
    let zero_count = result.coefficients.values().filter(|&&v| v == 0.0).count();
    let gcc = if result.coefficients.is_empty() {
        0.0
    } else {
        let sum: f64 = result.coefficients.values().sum();
        sum / result.coefficients.len() as f64
    };
    let mut summary =
        build_score_summary("lcc", &graph, result.coefficients, top_n, include_all, true);
    summary.global_clustering_coefficient = Some(gcc);
    summary.zero_count = Some(zero_count);
    Ok(Output::GraphScoreSummary(summary))
}

/// Handle GraphSssp command.
pub fn graph_sssp(
    p: &Arc<Primitives>,
    branch: BranchId,
    graph: String,
    source: String,
    direction: Option<String>,
    top_n: Option<usize>,
    include_all: Option<bool>,
) -> Result<Output> {
    let core_branch = to_core_branch_id(&branch)?;
    let dir = parse_direction(direction.as_deref())?;
    let opts = SsspOptions { direction: dir };
    let result = convert_result(p.graph.sssp(core_branch, &graph, &source, opts))?;
    let top_n_val = top_n.unwrap_or(DEFAULT_TOP_N);

    // Filter out non-finite distances (unreachable nodes) for summary/farthest,
    // but keep them in the raw `all` map if include_all is set.
    let finite_distances: std::collections::HashMap<String, f64> = result
        .distances
        .iter()
        .filter(|(_, &d)| d.is_finite())
        .map(|(k, &v)| (k.clone(), v))
        .collect();

    // Build farthest nodes (highest distances, descending)
    let mut dist_entries: Vec<(String, f64)> = finite_distances
        .iter()
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    dist_entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let farthest: Vec<crate::types::NodeScore> = dist_entries
        .iter()
        .take(top_n_val)
        .map(|(node_id, score)| crate::types::NodeScore {
            node_id: node_id.clone(),
            score: *score,
        })
        .collect();

    // Use finite distances for summary/distribution, but pass original for `all`
    let all_raw = if include_all.unwrap_or(false) {
        Some(result.distances)
    } else {
        None
    };
    let mut summary = build_score_summary(
        "sssp",
        &graph,
        finite_distances,
        top_n,
        // Already handled `all` above to preserve non-finite values in raw output
        Some(false),
        false,
    );
    summary.all = all_raw;
    summary.source = Some(source);
    summary.farthest = Some(farthest);
    Ok(Output::GraphScoreSummary(summary))
}

/// Convert serde_json::Value to strata_core::Value.
pub(crate) fn serde_json_to_value(json: serde_json::Value) -> Result<Value> {
    crate::bridge::serde_json_to_value_public(json).map_err(|e| Error::Serialization {
        reason: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn extract_quoted_basic() {
        assert_eq!(
            extract_quoted("Object type 'Person' is not declared"),
            Some("Person".to_string())
        );
    }

    #[test]
    fn extract_quoted_no_quotes() {
        assert_eq!(extract_quoted("no quotes here"), None);
    }

    #[test]
    fn extract_quoted_single_quote_only() {
        assert_eq!(extract_quoted("just 'one"), None);
    }

    #[test]
    fn extract_quoted_empty_between_quotes() {
        assert_eq!(extract_quoted("empty '' value"), Some("".to_string()));
    }

    #[test]
    fn extract_quoted_multiple_pairs() {
        // Returns the first pair
        assert_eq!(
            extract_quoted("'first' and 'second'"),
            Some("first".to_string())
        );
    }

    // =========================================================================
    // compute_distribution tests
    // =========================================================================

    #[test]
    fn distribution_empty() {
        let d = compute_distribution(&[]);
        assert_eq!(d.min, 0.0);
        assert_eq!(d.max, 0.0);
        assert_eq!(d.mean, 0.0);
        assert_eq!(d.median, 0.0);
        assert_eq!(d.p90, 0.0);
        assert_eq!(d.p99, 0.0);
    }

    #[test]
    fn distribution_single_element() {
        let d = compute_distribution(&[42.0]);
        assert_eq!(d.min, 42.0);
        assert_eq!(d.max, 42.0);
        assert_eq!(d.mean, 42.0);
        assert_eq!(d.median, 42.0);
        assert_eq!(d.p90, 42.0);
        assert_eq!(d.p99, 42.0);
    }

    #[test]
    fn distribution_two_elements() {
        let d = compute_distribution(&[1.0, 3.0]);
        assert_eq!(d.min, 1.0);
        assert_eq!(d.max, 3.0);
        assert_eq!(d.mean, 2.0);
        // Median of 2 elements = average of both
        assert_eq!(d.median, 2.0);
        // p90 and p99 for 2 elements: ceil(0.9*2)-1 = 1, ceil(0.99*2)-1 = 1
        assert_eq!(d.p90, 3.0);
        assert_eq!(d.p99, 3.0);
    }

    #[test]
    fn distribution_ten_elements() {
        // sorted: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        let vals: Vec<f64> = (1..=10).map(|x| x as f64).collect();
        let d = compute_distribution(&vals);
        assert_eq!(d.min, 1.0);
        assert_eq!(d.max, 10.0);
        assert_eq!(d.mean, 5.5);
        // Median of 10 elements: (sorted[4] + sorted[5]) / 2 = (5+6)/2 = 5.5
        assert_eq!(d.median, 5.5);
        // p90: ceil(0.9 * 10) - 1 = 9 - 1 = 8 → sorted[8] = 9
        assert_eq!(d.p90, 9.0);
        // p99: ceil(0.99 * 10) - 1 = 10 - 1 = 9 → sorted[9] = 10
        assert_eq!(d.p99, 10.0);
    }

    #[test]
    fn distribution_hundred_elements() {
        let vals: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let d = compute_distribution(&vals);
        assert_eq!(d.min, 1.0);
        assert_eq!(d.max, 100.0);
        assert_eq!(d.mean, 50.5);
        assert_eq!(d.median, 50.5);
        // p90: ceil(0.9 * 100) - 1 = 90 - 1 = 89 → sorted[89] = 90
        assert_eq!(d.p90, 90.0);
        // p99: ceil(0.99 * 100) - 1 = 99 - 1 = 98 → sorted[98] = 99
        assert_eq!(d.p99, 99.0);
    }

    #[test]
    fn distribution_odd_count_median() {
        let d = compute_distribution(&[1.0, 2.0, 3.0]);
        assert_eq!(d.median, 2.0);
    }

    // =========================================================================
    // build_group_summary tests
    // =========================================================================

    #[test]
    fn group_summary_empty_graph() {
        let raw: HashMap<String, u64> = HashMap::new();
        let s = build_group_summary("wcc", "g", raw, None, None);
        assert_eq!(s.algorithm, "wcc");
        assert_eq!(s.graph, "g");
        assert_eq!(s.node_count, 0);
        assert_eq!(s.group_count, 0);
        assert_eq!(s.largest_group_size, 0);
        assert!(s.groups.is_empty());
        assert!(s.all.is_none());
    }

    #[test]
    fn group_summary_single_component() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 1);
        raw.insert("b".into(), 1);
        raw.insert("c".into(), 1);
        let s = build_group_summary("wcc", "g", raw, None, None);
        assert_eq!(s.node_count, 3);
        assert_eq!(s.group_count, 1);
        assert_eq!(s.largest_group_size, 3);
        assert_eq!(s.groups.len(), 1);
        assert_eq!(s.groups[0].size, 3);
        assert_eq!(s.groups[0].id, 1);
        // Sample nodes should be sorted alphabetically
        assert_eq!(s.groups[0].sample_nodes, vec!["a", "b", "c"]);
    }

    #[test]
    fn group_summary_top_n_limits_groups() {
        let mut raw = HashMap::new();
        // 5 groups of sizes 5, 4, 3, 2, 1
        for i in 0..5u64 {
            for j in 0..(5 - i) {
                raw.insert(format!("g{}_n{}", i, j), i);
            }
        }
        let s = build_group_summary("cdlp", "g", raw, Some(2), None);
        assert_eq!(s.group_count, 5);
        assert_eq!(s.groups.len(), 2); // Only top 2 groups returned
        assert_eq!(s.groups[0].size, 5); // Largest
        assert_eq!(s.groups[1].size, 4); // Second largest
    }

    #[test]
    fn group_summary_sample_nodes_capped_at_5() {
        let mut raw = HashMap::new();
        for i in 0..20 {
            raw.insert(format!("node_{:02}", i), 42);
        }
        let s = build_group_summary("wcc", "g", raw, None, None);
        assert_eq!(s.groups.len(), 1);
        assert_eq!(s.groups[0].size, 20);
        assert_eq!(s.groups[0].sample_nodes.len(), GROUP_SAMPLE_SIZE);
    }

    #[test]
    fn group_summary_include_all() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 1);
        raw.insert("b".into(), 2);

        let s_without = build_group_summary("wcc", "g", raw.clone(), None, None);
        assert!(s_without.all.is_none());

        let s_false = build_group_summary("wcc", "g", raw.clone(), None, Some(false));
        assert!(s_false.all.is_none());

        let s_with = build_group_summary("wcc", "g", raw.clone(), None, Some(true));
        assert!(s_with.all.is_some());
        assert_eq!(s_with.all.unwrap().len(), 2);
    }

    #[test]
    fn group_summary_top_n_zero() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 1);
        let s = build_group_summary("wcc", "g", raw, Some(0), None);
        assert!(s.groups.is_empty());
        assert_eq!(s.group_count, 1);
        assert_eq!(s.largest_group_size, 1);
    }

    #[test]
    fn group_summary_top_n_exceeds_group_count() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 1);
        raw.insert("b".into(), 2);
        let s = build_group_summary("wcc", "g", raw, Some(100), None);
        assert_eq!(s.groups.len(), 2); // Only 2 groups exist
    }

    // =========================================================================
    // build_score_summary tests
    // =========================================================================

    #[test]
    fn score_summary_empty() {
        let raw: HashMap<String, f64> = HashMap::new();
        let s = build_score_summary("pagerank", "g", raw, None, None, true);
        assert_eq!(s.node_count, 0);
        assert!(s.top_nodes.is_empty());
        assert_eq!(s.distribution.min, 0.0);
    }

    #[test]
    fn score_summary_descending_order() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 0.1);
        raw.insert("b".into(), 0.5);
        raw.insert("c".into(), 0.3);
        let s = build_score_summary("pagerank", "g", raw, None, None, true);
        assert_eq!(s.top_nodes.len(), 3);
        assert_eq!(s.top_nodes[0].node_id, "b"); // highest first
        assert_eq!(s.top_nodes[0].score, 0.5);
        assert_eq!(s.top_nodes[1].node_id, "c");
        assert_eq!(s.top_nodes[2].node_id, "a");
    }

    #[test]
    fn score_summary_ascending_order() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 10.0);
        raw.insert("b".into(), 1.0);
        raw.insert("c".into(), 5.0);
        let s = build_score_summary("sssp", "g", raw, None, None, false);
        assert_eq!(s.top_nodes[0].node_id, "b"); // lowest first
        assert_eq!(s.top_nodes[0].score, 1.0);
        assert_eq!(s.top_nodes[1].node_id, "c");
        assert_eq!(s.top_nodes[2].node_id, "a");
    }

    #[test]
    fn score_summary_top_n_limits() {
        let mut raw = HashMap::new();
        for i in 0..20 {
            raw.insert(format!("n{}", i), i as f64);
        }
        let s = build_score_summary("pagerank", "g", raw, Some(3), None, true);
        assert_eq!(s.top_nodes.len(), 3);
        assert_eq!(s.node_count, 20);
        // Descending: top 3 should be 19, 18, 17
        assert_eq!(s.top_nodes[0].score, 19.0);
        assert_eq!(s.top_nodes[1].score, 18.0);
        assert_eq!(s.top_nodes[2].score, 17.0);
    }

    #[test]
    fn score_summary_top_n_zero() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 1.0);
        let s = build_score_summary("pagerank", "g", raw, Some(0), None, true);
        assert!(s.top_nodes.is_empty());
        assert_eq!(s.node_count, 1);
        // Distribution is still computed from all values
        assert_eq!(s.distribution.min, 1.0);
        assert_eq!(s.distribution.max, 1.0);
    }

    #[test]
    fn score_summary_include_all() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 1.0);

        let s_without = build_score_summary("pagerank", "g", raw.clone(), None, None, true);
        assert!(s_without.all.is_none());

        let s_with = build_score_summary("pagerank", "g", raw.clone(), None, Some(true), true);
        assert!(s_with.all.is_some());
        assert_eq!(s_with.all.unwrap().len(), 1);
    }

    #[test]
    fn score_summary_distribution_accuracy() {
        let mut raw = HashMap::new();
        for i in 1..=100 {
            raw.insert(format!("n{}", i), i as f64);
        }
        let s = build_score_summary("pagerank", "g", raw, None, None, true);
        assert_eq!(s.distribution.min, 1.0);
        assert_eq!(s.distribution.max, 100.0);
        assert_eq!(s.distribution.mean, 50.5);
        assert_eq!(s.distribution.median, 50.5);
        assert_eq!(s.distribution.p90, 90.0);
        assert_eq!(s.distribution.p99, 99.0);
    }

    #[test]
    fn score_summary_optional_fields_default_none() {
        let raw = HashMap::new();
        let s = build_score_summary("pagerank", "g", raw, None, None, true);
        assert!(s.iterations.is_none());
        assert!(s.converged.is_none());
        assert!(s.global_clustering_coefficient.is_none());
        assert!(s.zero_count.is_none());
        assert!(s.source.is_none());
        assert!(s.farthest.is_none());
    }

    // =========================================================================
    // Serialization roundtrip tests
    // =========================================================================

    #[test]
    fn group_summary_serde_roundtrip() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 1);
        raw.insert("b".into(), 1);
        raw.insert("c".into(), 2);
        let s = build_group_summary("wcc", "mygraph", raw, Some(10), Some(true));
        let json = serde_json::to_string(&s).unwrap();
        let deserialized: crate::types::GraphGroupSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s, deserialized);
    }

    #[test]
    fn score_summary_serde_roundtrip() {
        let mut raw = HashMap::new();
        raw.insert("a".into(), 0.5);
        raw.insert("b".into(), 0.3);
        let mut s = build_score_summary("pagerank", "mygraph", raw, None, Some(true), true);
        s.iterations = Some(15);
        s.converged = Some(true);
        let json = serde_json::to_string(&s).unwrap();
        let deserialized: crate::types::GraphScoreSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s, deserialized);
    }

    #[test]
    fn score_summary_serde_omits_none_fields() {
        let raw = HashMap::new();
        let s = build_score_summary("lcc", "g", raw, None, None, true);
        let json = serde_json::to_string(&s).unwrap();
        // None fields should be omitted entirely
        assert!(!json.contains("iterations"));
        assert!(!json.contains("converged"));
        assert!(!json.contains("source"));
        assert!(!json.contains("farthest"));
        assert!(!json.contains("\"all\""));
    }

    #[test]
    fn group_summary_serde_omits_none_all() {
        let raw = HashMap::new();
        let s = build_group_summary("wcc", "g", raw, None, None);
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("\"all\""));
    }
}
