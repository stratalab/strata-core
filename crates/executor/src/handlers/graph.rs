use std::sync::Arc;

use crate::bridge::{require_branch_exists, to_core_branch_id, Primitives};
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
