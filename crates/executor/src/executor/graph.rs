use super::{
    engine_graph_batch_operation, engine_graph_binding_target, engine_graph_direction,
    engine_graph_edge_data, engine_graph_node_data, engine_graph_property_defs,
    graph_batch_operation_name, graph_batch_write_output, graph_binding_page_output,
    graph_create_output, graph_delete_output, graph_edge_data_output, graph_edge_type,
    graph_edge_write_output, graph_info_data, graph_name, graph_name_page_output,
    graph_neighbor_page_output, graph_node_data_output, graph_node_id, graph_node_page_output,
    graph_node_write_output, graph_ontology_delete_output, graph_ontology_freeze_output,
    graph_ontology_output, graph_ontology_summary_output, graph_ontology_write_output,
    graph_type_name, optional_graph_edge_type, optional_graph_name, optional_graph_node_id,
    optional_limit, EngineGraphBatchWrite, EngineGraphLinkTypeDef, EngineGraphObjectTypeDef,
    Executor, ExecutorError, ExecutorResult, GraphBatchOperation, GraphBindingTarget,
    GraphDirection, GraphEdgeData, GraphEntityBinding, GraphNodeData, GraphPropertyDef, Maybe,
    Output, PageInfo, DEFAULT_GRAPH_LIST_LIMIT,
};

impl Executor {
    pub(super) fn execute_graph_create(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let mut service = self.graph_service(branch, space)?;
        let (info, commit) = service.create_graph(graph)?;
        Ok(graph_create_output(&info, commit))
    }

    pub(super) fn execute_graph_delete(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_delete_output(
            &service.delete_graph(&graph)?,
            None,
            None,
            None,
            None,
        ))
    }

    pub(super) fn execute_graph_list(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        cursor: Option<String>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let cursor = optional_graph_name(cursor)?;
        let limit = optional_limit(limit)?.unwrap_or(DEFAULT_GRAPH_LIST_LIMIT);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let page = if let Some(as_of) = as_of {
            service.list_graphs_at(cursor.as_ref(), limit, as_of)?
        } else {
            service.list_graphs(cursor.as_ref(), limit)?
        };
        Ok(graph_name_page_output(&page))
    }

    pub(super) fn execute_graph_get_meta(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let info = if let Some(as_of) = as_of {
            service.graph_info_at(&graph, as_of)?
        } else {
            service.graph_info(&graph)?
        };
        // A missing graph is an error here, matching every other graph read
        // (get-node, list-nodes, neighbors, ontology). graph_info returning
        // an Option is an engine quirk the wire boundary must not leak as a
        // null "success".
        let info = info
            .ok_or_else(|| ExecutorError::new("not_found.engine.graph", "graph does not exist"))?;
        Ok(Output::GraphInfoResult(Some(graph_info_data(&info))))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_add_node(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        node_id: String,
        properties: Option<serde_json::Value>,
        binding: Option<GraphEntityBinding>,
        object_type: Option<String>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let node_id = graph_node_id(node_id)?;
        let mut data = GraphNodeData::new(properties, binding);
        if let Some(object_type) = object_type {
            data = data.with_object_type(object_type);
        }
        let data = engine_graph_node_data(data)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_node_write_output(
            &service.upsert_node(&graph, node_id, data)?,
        ))
    }

    pub(super) fn execute_graph_get_node(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        node_id: String,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let node_id = graph_node_id(node_id)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let node = if let Some(as_of) = as_of {
            service.get_node_at(&graph, &node_id, as_of)?
        } else {
            service.get_node(&graph, &node_id)?
        };
        Ok(Output::GraphNodeResult(Maybe::from_option(
            node.as_ref().map(graph_node_data_output),
        )))
    }

    pub(super) fn execute_graph_remove_node(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        node_id: String,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let node_id = graph_node_id(node_id)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_delete_output(
            &service.delete_node(&graph, &node_id)?,
            Some(node_id.as_str().to_owned()),
            None,
            None,
            None,
        ))
    }

    pub(super) fn execute_graph_sample(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        count: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let count = optional_limit(count)?.unwrap_or(10);
        let mut service = self.graph_service(branch, space)?;
        let (total_count, nodes) = service.sample_nodes(&graph, count)?;
        Ok(Output::GraphSampleResult {
            total_count,
            items: nodes.iter().map(graph_node_data_output).collect(),
            page: PageInfo::terminal(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_list_nodes(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        prefix: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let prefix = optional_graph_node_id(prefix)?;
        let cursor = optional_graph_node_id(cursor)?;
        let limit = optional_limit(limit)?.unwrap_or(DEFAULT_GRAPH_LIST_LIMIT);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let page = if let Some(as_of) = as_of {
            service.list_nodes_at(&graph, prefix.as_ref(), cursor.as_ref(), limit, as_of)?
        } else {
            service.list_nodes(&graph, prefix.as_ref(), cursor.as_ref(), limit)?
        };
        Ok(graph_node_page_output(&page))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_add_edge(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        src: String,
        edge_type: String,
        dst: String,
        weight: Option<f64>,
        properties: Option<serde_json::Value>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let src = graph_node_id(src)?;
        let edge_type = graph_edge_type(edge_type)?;
        let dst = graph_node_id(dst)?;
        let data = engine_graph_edge_data(GraphEdgeData::new(weight, properties))?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_edge_write_output(
            &service.upsert_edge(&graph, src, edge_type, dst, data)?,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_get_edge(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        src: String,
        edge_type: String,
        dst: String,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let src = graph_node_id(src)?;
        let edge_type = graph_edge_type(edge_type)?;
        let dst = graph_node_id(dst)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let edge = if let Some(as_of) = as_of {
            service.get_edge_at(&graph, &src, &edge_type, &dst, as_of)?
        } else {
            service.get_edge(&graph, &src, &edge_type, &dst)?
        };
        Ok(Output::GraphEdgeResult(Maybe::from_option(
            edge.as_ref().map(graph_edge_data_output),
        )))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_remove_edge(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        src: String,
        edge_type: String,
        dst: String,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let src = graph_node_id(src)?;
        let edge_type = graph_edge_type(edge_type)?;
        let dst = graph_node_id(dst)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_delete_output(
            &service.delete_edge(&graph, &src, &edge_type, &dst)?,
            None,
            Some(src.as_str().to_owned()),
            Some(edge_type.as_str().to_owned()),
            Some(dst.as_str().to_owned()),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_neighbors(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        node_id: String,
        direction: GraphDirection,
        edge_type: Option<String>,
        cursor: Option<&str>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let node_id = graph_node_id(node_id)?;
        let direction = engine_graph_direction(direction);
        let edge_type = optional_graph_edge_type(edge_type)?;
        let limit = optional_limit(limit)?.unwrap_or(DEFAULT_GRAPH_LIST_LIMIT);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let page = if let Some(as_of) = as_of {
            service.neighbors_at(
                &graph,
                &node_id,
                direction,
                edge_type.as_ref(),
                cursor,
                limit,
                as_of,
            )?
        } else {
            service.neighbors(
                &graph,
                &node_id,
                direction,
                edge_type.as_ref(),
                cursor,
                limit,
            )?
        };
        Ok(graph_neighbor_page_output(&page))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_bindings_for_entity(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        target: GraphBindingTarget,
        cursor: Option<&str>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let target = engine_graph_binding_target(target)?;
        let limit = optional_limit(limit)?.unwrap_or(DEFAULT_GRAPH_LIST_LIMIT);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let page = if let Some(as_of) = as_of {
            service.bindings_for_entity_at(&target, cursor, limit, as_of)?
        } else {
            service.bindings_for_entity(&target, cursor, limit)?
        };
        Ok(graph_binding_page_output(&page))
    }

    pub(super) fn execute_graph_batch_write(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        operations: Vec<GraphBatchOperation>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let operation_names = operations
            .iter()
            .map(graph_batch_operation_name)
            .collect::<Vec<_>>();
        let operations = operations
            .into_iter()
            .map(engine_graph_batch_operation)
            .collect::<ExecutorResult<Vec<_>>>()?;
        let batch = EngineGraphBatchWrite::new(operations);
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_batch_write_output(
            &service.batch_write(&graph, &batch)?,
            &operation_names,
        ))
    }

    pub(super) fn execute_graph_define_object_type(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        name: String,
        properties: std::collections::BTreeMap<String, GraphPropertyDef>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let def = EngineGraphObjectTypeDef::new(
            graph_type_name(name)?,
            engine_graph_property_defs(properties)?,
        )?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_ontology_write_output(
            &service.define_object_type(&graph, def)?,
            "object",
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_define_link_type(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        name: String,
        source: String,
        target: String,
        cardinality: Option<String>,
        properties: std::collections::BTreeMap<String, GraphPropertyDef>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let def = EngineGraphLinkTypeDef::new(
            graph_type_name(name)?,
            graph_type_name(source)?,
            graph_type_name(target)?,
            cardinality,
            engine_graph_property_defs(properties)?,
        )?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_ontology_write_output(
            &service.define_link_type(&graph, def)?,
            "link",
        ))
    }

    pub(super) fn execute_graph_delete_object_type(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        name: String,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let name = graph_type_name(name)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_ontology_delete_output(
            &service.delete_object_type(&graph, &name)?,
            "object",
            name.as_str(),
        ))
    }

    pub(super) fn execute_graph_delete_link_type(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        name: String,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let name = graph_type_name(name)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_ontology_delete_output(
            &service.delete_link_type(&graph, &name)?,
            "link",
            name.as_str(),
        ))
    }

    pub(super) fn execute_graph_freeze_ontology(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(graph_ontology_freeze_output(
            &service.freeze_ontology(&graph)?,
        ))
    }

    pub(super) fn execute_graph_get_ontology(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let ontology = if let Some(as_of) = as_of {
            service.ontology_at(&graph, as_of)?
        } else {
            service.ontology(&graph)?
        };
        Ok(Output::GraphOntologyResult(
            ontology.as_ref().map(graph_ontology_output),
        ))
    }

    pub(super) fn execute_graph_ontology_summary(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let summary = if let Some(as_of) = as_of {
            service.ontology_summary_at(&graph, as_of)?
        } else {
            service.ontology_summary(&graph)?
        };
        Ok(Output::GraphOntologySummaryResult(
            summary.as_ref().map(graph_ontology_summary_output),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_nodes_by_type(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        object_type: String,
        cursor: Option<String>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let object_type = graph_type_name(object_type)?;
        let cursor = optional_graph_node_id(cursor)?;
        let limit = optional_limit(limit)?.unwrap_or(DEFAULT_GRAPH_LIST_LIMIT);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        let page = if let Some(as_of) = as_of {
            service.nodes_by_type_at(&graph, &object_type, cursor.as_ref(), limit, as_of)?
        } else {
            service.nodes_by_type(&graph, &object_type, cursor.as_ref(), limit)?
        };
        Ok(graph_node_page_output(&page))
    }
}

use super::{
    engine_graph_bfs_options, engine_graph_budget, engine_graph_bulk_edges,
    engine_graph_bulk_nodes, engine_graph_cdlp_options, engine_graph_delete_policy,
    engine_graph_pagerank_options, engine_graph_personalization, graph_bfs_output,
    graph_bulk_insert_output, graph_cdlp_output, graph_delete_policy_output, graph_lcc_output,
    graph_pagerank_output, graph_sssp_output, graph_wcc_output, EngineGraphAdjacencyIndex,
    EngineGraphName, GraphAnalyticsBudget, GraphBulkEdge, GraphBulkNode, GraphDeletePolicy,
};

impl Executor {
    /// Builds the adjacency snapshot every analytics command runs on,
    /// honoring the optional size bounds and read timestamp.
    fn graph_analytics_index(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: &EngineGraphName,
        budget: Option<GraphAnalyticsBudget>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<EngineGraphAdjacencyIndex, ExecutorError> {
        let budget = engine_graph_budget(budget)?;
        // #3112 S3b: the analytics family shares one resolution point — every
        // `graph <algorithm>` command reaches its snapshot through here.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.graph_service(branch, space)?;
        Ok(if let Some(as_of) = as_of {
            service.adjacency_index_at(graph, &budget, as_of)?
        } else {
            service.adjacency_index(graph, &budget)?
        })
    }

    pub(super) fn execute_graph_wcc(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        budget: Option<GraphAnalyticsBudget>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let index = self.graph_analytics_index(branch, space, &graph, budget, as_of, as_of_time)?;
        Ok(graph_wcc_output(&index, &index.wcc()))
    }

    pub(super) fn execute_graph_lcc(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        budget: Option<GraphAnalyticsBudget>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let index = self.graph_analytics_index(branch, space, &graph, budget, as_of, as_of_time)?;
        Ok(graph_lcc_output(&index, &index.lcc()))
    }

    pub(super) fn execute_graph_sssp(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        source: String,
        direction: Option<GraphDirection>,
        budget: Option<GraphAnalyticsBudget>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let source = graph_node_id(source)?;
        let direction = direction.unwrap_or(GraphDirection::Outgoing);
        let index = self.graph_analytics_index(branch, space, &graph, budget, as_of, as_of_time)?;
        let result = index.sssp(&source, engine_graph_direction(direction))?;
        Ok(graph_sssp_output(&index, direction, &result))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_pagerank(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        damping: Option<f64>,
        max_iterations: Option<u64>,
        tolerance: Option<f64>,
        personalization: Option<std::collections::BTreeMap<String, f64>>,
        budget: Option<GraphAnalyticsBudget>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let options = engine_graph_pagerank_options(damping, max_iterations, tolerance)?;
        let index = self.graph_analytics_index(branch, space, &graph, budget, as_of, as_of_time)?;
        let (result, personalized) = if let Some(personalization) = personalization {
            let seeds = engine_graph_personalization(personalization)?;
            (index.personalized_pagerank(&options, &seeds)?, true)
        } else {
            (index.pagerank(&options), false)
        };
        Ok(graph_pagerank_output(&index, &result, personalized))
    }

    pub(super) fn execute_graph_cdlp(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        max_iterations: Option<u64>,
        direction: Option<GraphDirection>,
        budget: Option<GraphAnalyticsBudget>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let options = engine_graph_cdlp_options(max_iterations, direction)?;
        let index = self.graph_analytics_index(branch, space, &graph, budget, as_of, as_of_time)?;
        Ok(graph_cdlp_output(&index, &index.cdlp(&options)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_bfs(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        start: String,
        max_depth: Option<u64>,
        max_nodes: Option<u64>,
        edge_types: Option<Vec<String>>,
        direction: Option<GraphDirection>,
        budget: Option<GraphAnalyticsBudget>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let start = graph_node_id(start)?;
        let options = engine_graph_bfs_options(max_depth, max_nodes, edge_types, direction)?;
        let index = self.graph_analytics_index(branch, space, &graph, budget, as_of, as_of_time)?;
        let result = index.bfs(&start, &options)?;
        Ok(graph_bfs_output(&index, &start, &result))
    }

    pub(super) fn execute_graph_apply_delete_policy(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        target: GraphBindingTarget,
        policy: GraphDeletePolicy,
    ) -> Result<Output, ExecutorError> {
        let target = engine_graph_binding_target(target)?;
        let policy = engine_graph_delete_policy(policy);
        let mut service = self.graph_service(branch, space)?;
        let outcome = service.apply_binding_delete_policy(&target, policy)?;
        Ok(graph_delete_policy_output(&outcome))
    }

    pub(super) fn execute_graph_bulk_insert(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        graph: String,
        nodes: Vec<GraphBulkNode>,
        edges: Vec<GraphBulkEdge>,
        chunk_size: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let graph = graph_name(graph)?;
        let nodes = engine_graph_bulk_nodes(nodes)?;
        let edges = engine_graph_bulk_edges(edges)?;
        let chunk_size = optional_limit(chunk_size)?;
        let mut service = self.graph_service(branch, space)?;
        let outcome = service.bulk_insert(&graph, &nodes, &edges, chunk_size)?;
        Ok(graph_bulk_insert_output(&outcome))
    }
}
