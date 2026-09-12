use super::{
    branch_name, commit_receipt, delete_effect, graph_batch_result, product_space, upsert_effect,
    usize_to_u64, BatchItem, CommitOutcome, EngineGraphAdjacencyIndex, EngineGraphAnalyticsBudget,
    EngineGraphBatchOpOutcome, EngineGraphBatchOperation, EngineGraphBatchWriteOutcome,
    EngineGraphBfsOptions, EngineGraphBfsResult, EngineGraphBinding, EngineGraphBindingPage,
    EngineGraphBindingPrimitive, EngineGraphBindingTarget, EngineGraphBulkInsertOutcome,
    EngineGraphCdlpOptions, EngineGraphCdlpResult, EngineGraphDeleteOutcome,
    EngineGraphDeletePolicy, EngineGraphDeletePolicyOutcome, EngineGraphDirection, EngineGraphEdge,
    EngineGraphEdgeData, EngineGraphEdgeType, EngineGraphEdgeWriteOutcome,
    EngineGraphEntityBinding, EngineGraphInfo, EngineGraphLccResult, EngineGraphLinkTypeDef,
    EngineGraphName, EngineGraphNamePage, EngineGraphNeighbor, EngineGraphNeighborPage,
    EngineGraphNode, EngineGraphNodeData, EngineGraphNodeId, EngineGraphNodePage,
    EngineGraphObjectTypeDef, EngineGraphOntology, EngineGraphOntologyFreezeOutcome,
    EngineGraphOntologyStatus, EngineGraphOntologySummary, EngineGraphOntologyWriteOutcome,
    EngineGraphPageRankOptions, EngineGraphPageRankResult, EngineGraphProperties,
    EngineGraphPropertyDef, EngineGraphSsspResult, EngineGraphTargetStatus, EngineGraphTypeName,
    EngineGraphWccResult, EngineGraphWriteOutcome, ExecutorError, ExecutorResult,
    GraphAnalyticsBudget, GraphBatchItemResult, GraphBatchOperation, GraphBfsData,
    GraphBfsEdgeData, GraphBindingHit, GraphBindingPrimitive, GraphBindingTarget, GraphBulkEdge,
    GraphBulkNode, GraphCdlpData, GraphDeletePolicy, GraphDirection, GraphEdgeData,
    GraphEdgeDataOutput, GraphEntityBinding, GraphInfoData, GraphLccData, GraphLinkTypeDefData,
    GraphLinkTypeSummaryData, GraphNeighborHit, GraphNodeData, GraphNodeDataOutput,
    GraphObjectTypeDefData, GraphObjectTypeSummaryData, GraphOntologyData,
    GraphOntologySummaryData, GraphPagerankData, GraphPropertyDef, GraphSsspData, GraphWccData,
    MutationEffect, MutationEffectKind, Output, PageInfo, DEFAULT_BRANCH, DEFAULT_SPACE,
};

pub(super) fn graph_name(name: String) -> Result<EngineGraphName, ExecutorError> {
    EngineGraphName::new(name).map_err(ExecutorError::from)
}

pub(super) fn optional_graph_name(
    name: Option<String>,
) -> Result<Option<EngineGraphName>, ExecutorError> {
    name.map(graph_name).transpose()
}

pub(super) fn graph_node_id(node_id: String) -> Result<EngineGraphNodeId, ExecutorError> {
    EngineGraphNodeId::new(node_id).map_err(ExecutorError::from)
}

pub(super) fn optional_graph_node_id(
    node_id: Option<String>,
) -> Result<Option<EngineGraphNodeId>, ExecutorError> {
    node_id.map(graph_node_id).transpose()
}

pub(super) fn graph_edge_type(edge_type: String) -> Result<EngineGraphEdgeType, ExecutorError> {
    EngineGraphEdgeType::new(edge_type).map_err(ExecutorError::from)
}

pub(super) fn optional_graph_edge_type(
    edge_type: Option<String>,
) -> Result<Option<EngineGraphEdgeType>, ExecutorError> {
    edge_type.map(graph_edge_type).transpose()
}

pub(super) fn engine_graph_properties(
    properties: Option<serde_json::Value>,
) -> Result<Option<EngineGraphProperties>, ExecutorError> {
    properties
        .map(EngineGraphProperties::new)
        .transpose()
        .map_err(ExecutorError::from)
}

pub(super) fn engine_graph_node_data(
    data: GraphNodeData,
) -> Result<EngineGraphNodeData, ExecutorError> {
    let (properties, binding, object_type) = data.into_parts();
    let mut data = EngineGraphNodeData::new(
        engine_graph_properties(properties)?,
        binding.map(engine_graph_entity_binding).transpose()?,
    );
    if let Some(object_type) = object_type {
        data = data.with_object_type(graph_type_name(object_type)?);
    }
    Ok(data)
}

pub(super) fn graph_type_name(name: String) -> Result<EngineGraphTypeName, ExecutorError> {
    EngineGraphTypeName::new(name).map_err(ExecutorError::from)
}

pub(super) fn engine_graph_edge_data(
    data: GraphEdgeData,
) -> Result<EngineGraphEdgeData, ExecutorError> {
    let (weight, properties) = data.into_parts();
    let properties = engine_graph_properties(properties)?;
    if let Some(weight) = weight {
        return EngineGraphEdgeData::new(weight, properties).map_err(ExecutorError::from);
    }
    Ok(EngineGraphEdgeData::default_weight(properties))
}

pub(super) const fn engine_graph_direction(direction: GraphDirection) -> EngineGraphDirection {
    match direction {
        GraphDirection::Outgoing => EngineGraphDirection::Outgoing,
        GraphDirection::Incoming => EngineGraphDirection::Incoming,
        GraphDirection::Both => EngineGraphDirection::Both,
    }
}

pub(super) const fn output_graph_direction(direction: EngineGraphDirection) -> GraphDirection {
    match direction {
        EngineGraphDirection::Outgoing => GraphDirection::Outgoing,
        EngineGraphDirection::Incoming => GraphDirection::Incoming,
        EngineGraphDirection::Both => GraphDirection::Both,
    }
}

pub(super) const fn engine_graph_binding_primitive(
    primitive: GraphBindingPrimitive,
) -> EngineGraphBindingPrimitive {
    match primitive {
        GraphBindingPrimitive::Kv => EngineGraphBindingPrimitive::Kv,
        GraphBindingPrimitive::Json => EngineGraphBindingPrimitive::Json,
        GraphBindingPrimitive::Vector => EngineGraphBindingPrimitive::Vector,
        GraphBindingPrimitive::Event => EngineGraphBindingPrimitive::Event,
        GraphBindingPrimitive::Graph => EngineGraphBindingPrimitive::Graph,
    }
}

pub(super) const fn output_graph_binding_primitive(
    primitive: EngineGraphBindingPrimitive,
) -> GraphBindingPrimitive {
    match primitive {
        EngineGraphBindingPrimitive::Kv => GraphBindingPrimitive::Kv,
        EngineGraphBindingPrimitive::Json => GraphBindingPrimitive::Json,
        EngineGraphBindingPrimitive::Vector => GraphBindingPrimitive::Vector,
        EngineGraphBindingPrimitive::Event => GraphBindingPrimitive::Event,
        EngineGraphBindingPrimitive::Graph => GraphBindingPrimitive::Graph,
    }
}

pub(super) fn engine_graph_binding_target(
    target: GraphBindingTarget,
) -> Result<EngineGraphBindingTarget, ExecutorError> {
    let (primitive, branch, space, key) = target.into_parts();
    let branch = branch
        .as_deref()
        .map(|branch| branch_name(Some(branch), DEFAULT_BRANCH))
        .transpose()?;
    let space = product_space(Some(&space), DEFAULT_SPACE)?;
    EngineGraphBindingTarget::new(
        engine_graph_binding_primitive(primitive),
        branch,
        space,
        key,
    )
    .map_err(ExecutorError::from)
}

pub(super) fn engine_graph_entity_binding(
    binding: GraphEntityBinding,
) -> Result<EngineGraphEntityBinding, ExecutorError> {
    Ok(EngineGraphEntityBinding::new(engine_graph_binding_target(
        binding.into_target(),
    )?))
}

pub(super) fn engine_graph_batch_operation(
    operation: GraphBatchOperation,
) -> Result<EngineGraphBatchOperation, ExecutorError> {
    match operation {
        GraphBatchOperation::UpsertNode { node_id, data } => {
            Ok(EngineGraphBatchOperation::UpsertNode {
                node_id: graph_node_id(node_id)?,
                data: engine_graph_node_data(data)?,
            })
        }
        GraphBatchOperation::DeleteNode { node_id } => Ok(EngineGraphBatchOperation::DeleteNode {
            node_id: graph_node_id(node_id)?,
        }),
        GraphBatchOperation::UpsertEdge {
            src,
            edge_type,
            dst,
            data,
        } => Ok(EngineGraphBatchOperation::UpsertEdge {
            src: graph_node_id(src)?,
            edge_type: graph_edge_type(edge_type)?,
            dst: graph_node_id(dst)?,
            data: engine_graph_edge_data(data)?,
        }),
        GraphBatchOperation::DeleteEdge {
            src,
            edge_type,
            dst,
        } => Ok(EngineGraphBatchOperation::DeleteEdge {
            src: graph_node_id(src)?,
            edge_type: graph_edge_type(edge_type)?,
            dst: graph_node_id(dst)?,
        }),
    }
}

pub(super) const fn graph_batch_operation_name(operation: &GraphBatchOperation) -> &'static str {
    match operation {
        GraphBatchOperation::UpsertNode { .. } => "upsert_node",
        GraphBatchOperation::DeleteNode { .. } => "delete_node",
        GraphBatchOperation::UpsertEdge { .. } => "upsert_edge",
        GraphBatchOperation::DeleteEdge { .. } => "delete_edge",
    }
}

pub(super) fn output_graph_binding_target(target: &EngineGraphBindingTarget) -> GraphBindingTarget {
    GraphBindingTarget::new(
        output_graph_binding_primitive(target.primitive()),
        target.branch().map(|branch| branch.as_str().to_owned()),
        target.space().as_str().to_owned(),
        target.key().to_owned(),
    )
}

pub(super) fn output_graph_entity_binding(
    binding: &EngineGraphEntityBinding,
) -> GraphEntityBinding {
    GraphEntityBinding::new(output_graph_binding_target(binding.target()))
}

pub(super) fn graph_info_data(info: &EngineGraphInfo) -> GraphInfoData {
    GraphInfoData::new(
        info.name().as_str().to_owned(),
        info.node_count(),
        info.edge_count(),
        info.created_version().as_u64(),
        info.created_timestamp().as_micros(),
        info.updated_version().as_u64(),
        info.updated_timestamp().as_micros(),
    )
}

pub(super) fn graph_create_output(info: &EngineGraphInfo, commit: CommitOutcome) -> Output {
    Output::GraphCreateResult {
        info: graph_info_data(info),
        effect: MutationEffect::created(),
        commit: commit_receipt(commit),
    }
}

pub(super) fn graph_node_data_output(node: &EngineGraphNode) -> GraphNodeDataOutput {
    GraphNodeDataOutput::new(
        node.graph().as_str().to_owned(),
        node.node_id().as_str().to_owned(),
        node.data()
            .properties()
            .map(|properties| properties.as_inner().clone()),
        node.data().binding().map(output_graph_entity_binding),
        node.data()
            .object_type()
            .map(|object_type| object_type.as_str().to_owned()),
        node.version().as_u64(),
        node.timestamp().as_micros(),
    )
}

pub(super) fn graph_edge_data_output(edge: &EngineGraphEdge) -> GraphEdgeDataOutput {
    GraphEdgeDataOutput::new(
        edge.graph().as_str().to_owned(),
        edge.src().as_str().to_owned(),
        edge.edge_type().as_str().to_owned(),
        edge.dst().as_str().to_owned(),
        edge.data().weight(),
        edge.data()
            .properties()
            .map(|properties| properties.as_inner().clone()),
        edge.version().as_u64(),
        edge.timestamp().as_micros(),
    )
}

pub(super) fn graph_neighbor_hit(neighbor: &EngineGraphNeighbor) -> GraphNeighborHit {
    GraphNeighborHit::new(
        graph_node_data_output(neighbor.node()),
        graph_edge_data_output(neighbor.edge()),
        output_graph_direction(neighbor.direction()),
        neighbor
            .target_status()
            .map(|status| graph_target_status(status).to_owned()),
    )
}

pub(super) fn engine_graph_bulk_nodes(
    nodes: Vec<GraphBulkNode>,
) -> Result<Vec<(EngineGraphNodeId, EngineGraphNodeData)>, ExecutorError> {
    nodes
        .into_iter()
        .map(|node| {
            let (node_id, properties, binding, object_type) = node.into_parts();
            let mut data = GraphNodeData::new(properties, binding);
            if let Some(object_type) = object_type {
                data = data.with_object_type(object_type);
            }
            Ok((graph_node_id(node_id)?, engine_graph_node_data(data)?))
        })
        .collect()
}

pub(super) fn engine_graph_bulk_edges(
    edges: Vec<GraphBulkEdge>,
) -> Result<
    Vec<(
        EngineGraphNodeId,
        EngineGraphEdgeType,
        EngineGraphNodeId,
        EngineGraphEdgeData,
    )>,
    ExecutorError,
> {
    edges
        .into_iter()
        .map(|edge| {
            let (src, edge_type, dst, weight, properties) = edge.into_parts();
            Ok((
                graph_node_id(src)?,
                graph_edge_type(edge_type)?,
                graph_node_id(dst)?,
                engine_graph_edge_data(GraphEdgeData::new(weight, properties))?,
            ))
        })
        .collect()
}

pub(super) fn graph_bulk_insert_output(outcome: &EngineGraphBulkInsertOutcome) -> Output {
    Output::GraphBulkInsertResult {
        graph: outcome.graph().as_str().to_owned(),
        nodes_inserted: outcome.nodes_inserted(),
        edges_inserted: outcome.edges_inserted(),
        commits: outcome.commits(),
        commit: outcome.last_commit().map(|commit| commit_receipt(*commit)),
    }
}

pub(super) const fn engine_graph_delete_policy(
    policy: GraphDeletePolicy,
) -> EngineGraphDeletePolicy {
    match policy {
        GraphDeletePolicy::Cascade => EngineGraphDeletePolicy::Cascade,
        GraphDeletePolicy::Detach => EngineGraphDeletePolicy::Detach,
        GraphDeletePolicy::KeepDangling => EngineGraphDeletePolicy::KeepDangling,
    }
}

pub(super) fn graph_delete_policy_output(outcome: &EngineGraphDeletePolicyOutcome) -> Output {
    let policy = match outcome.policy() {
        EngineGraphDeletePolicy::Cascade => "cascade",
        EngineGraphDeletePolicy::Detach => "detach",
        EngineGraphDeletePolicy::KeepDangling => "keep_dangling",
    };
    let matched = outcome.nodes_affected() > 0;
    let kind = match outcome.policy() {
        _ if outcome.commit().is_none() => MutationEffectKind::Unchanged,
        EngineGraphDeletePolicy::Cascade => MutationEffectKind::Deleted,
        _ => MutationEffectKind::Updated,
    };
    Output::GraphDeletePolicyResult {
        policy: policy.to_owned(),
        effect: MutationEffect::new(
            outcome.commit().is_some(),
            kind,
            matched,
            outcome.nodes_affected(),
        ),
        commit: outcome.commit().map(|commit| commit_receipt(*commit)),
    }
}

pub(super) fn graph_target_status(status: EngineGraphTargetStatus) -> &'static str {
    match status {
        EngineGraphTargetStatus::Present => "present",
        EngineGraphTargetStatus::Deleted => "deleted",
        EngineGraphTargetStatus::Missing => "missing",
        EngineGraphTargetStatus::MalformedTarget => "malformed_target",
        // Unsupported, plus any future state the non-exhaustive contract
        // vocabulary adds before it gains a deliberate wire name.
        _ => "unsupported",
    }
}

pub(super) fn graph_binding_hit(binding: &EngineGraphBinding) -> GraphBindingHit {
    GraphBindingHit::new(
        binding.graph().as_str().to_owned(),
        binding.node_id().as_str().to_owned(),
        output_graph_entity_binding(binding.binding()),
        binding.version().as_u64(),
        binding.timestamp().as_micros(),
    )
}

pub(super) fn graph_name_page_output(page: &EngineGraphNamePage) -> Output {
    Output::GraphNamePage {
        items: page
            .graphs()
            .iter()
            .map(|graph| graph.as_str().to_owned())
            .collect(),
        page: PageInfo::new(
            page.has_more(),
            page.cursor().map(|cursor| cursor.as_str().to_owned()),
        ),
    }
}

pub(super) fn graph_node_page_output(page: &EngineGraphNodePage) -> Output {
    Output::GraphNodePage {
        items: page.nodes().iter().map(graph_node_data_output).collect(),
        page: PageInfo::new(
            page.has_more(),
            page.cursor().map(|cursor| cursor.as_str().to_owned()),
        ),
    }
}

pub(super) fn graph_neighbor_page_output(page: &EngineGraphNeighborPage) -> Output {
    Output::GraphNeighborPage {
        items: page.neighbors().iter().map(graph_neighbor_hit).collect(),
        page: PageInfo::new(page.has_more(), page.cursor().map(str::to_owned)),
    }
}

pub(super) fn graph_binding_page_output(page: &EngineGraphBindingPage) -> Output {
    Output::GraphBindingPage {
        items: page.bindings().iter().map(graph_binding_hit).collect(),
        page: PageInfo::new(page.has_more(), page.cursor().map(str::to_owned)),
    }
}

pub(super) fn graph_node_write_output(outcome: &EngineGraphWriteOutcome) -> Output {
    Output::GraphNodeWriteResult {
        graph: outcome.graph().as_str().to_owned(),
        node_id: outcome.node_id().as_str().to_owned(),
        effect: upsert_effect(!outcome.created()),
        commit: commit_receipt(outcome.commit()),
    }
}

pub(super) fn graph_edge_write_output(outcome: &EngineGraphEdgeWriteOutcome) -> Output {
    Output::GraphEdgeWriteResult {
        graph: outcome.graph().as_str().to_owned(),
        src: outcome.src().as_str().to_owned(),
        edge_type: outcome.edge_type().as_str().to_owned(),
        dst: outcome.dst().as_str().to_owned(),
        effect: upsert_effect(!outcome.created()),
        commit: commit_receipt(outcome.commit()),
    }
}

pub(super) fn graph_delete_output(
    outcome: &EngineGraphDeleteOutcome,
    node_id: Option<String>,
    src: Option<String>,
    edge_type: Option<String>,
    dst: Option<String>,
) -> Output {
    Output::GraphDeleteResult {
        graph: outcome.graph().as_str().to_owned(),
        node_id,
        src,
        edge_type,
        dst,
        effect: delete_effect(outcome.deleted()),
        commit: outcome.commit().map(commit_receipt),
    }
}

pub(super) fn graph_batch_write_output(
    outcome: &EngineGraphBatchWriteOutcome,
    operation_names: &[&'static str],
) -> Output {
    let commit = outcome.commit();
    Output::GraphBatchWriteResult {
        graph: outcome.graph().as_str().to_owned(),
        batch: graph_batch_result(
            outcome
                .results()
                .iter()
                .map(|item| graph_batch_item_result(item, operation_names, commit))
                .collect(),
        ),
    }
}

pub(super) fn graph_batch_item_result(
    item: &EngineGraphBatchOpOutcome,
    operation_names: &[&'static str],
    commit: Option<CommitOutcome>,
) -> BatchItem<GraphBatchItemResult> {
    let operation_index = usize_to_u64(item.operation_index());
    let operation = operation_names
        .get(item.operation_index())
        .copied()
        .unwrap_or("unknown");
    let applied = graph_batch_item_applied(item);
    BatchItem::ok(
        operation_index,
        applied,
        Some(graph_batch_item_effect(item)),
        applied.then(|| commit.map(commit_receipt)).flatten(),
        GraphBatchItemResult::new(
            operation_index,
            operation,
            item.created_flag(),
            item.deleted_flag(),
        ),
    )
}

pub(super) const fn graph_batch_item_applied(item: &EngineGraphBatchOpOutcome) -> bool {
    item.created_flag().is_some() || matches!(item.deleted_flag(), Some(true))
}

pub(super) fn graph_batch_item_effect(item: &EngineGraphBatchOpOutcome) -> MutationEffect {
    if let Some(created) = item.created_flag() {
        return upsert_effect(!created);
    }
    delete_effect(item.deleted_flag() == Some(true))
}

pub(super) fn engine_graph_property_defs(
    properties: std::collections::BTreeMap<String, GraphPropertyDef>,
) -> Result<Vec<(String, EngineGraphPropertyDef)>, ExecutorError> {
    properties
        .into_iter()
        .map(|(name, def)| {
            let (value_type, required) = def.into_parts();
            Ok((
                name,
                EngineGraphPropertyDef::new(value_type, required).map_err(ExecutorError::from)?,
            ))
        })
        .collect()
}

fn output_graph_property_defs(
    properties: &std::collections::BTreeMap<String, EngineGraphPropertyDef>,
) -> std::collections::BTreeMap<String, GraphPropertyDef> {
    properties
        .iter()
        .map(|(name, def)| {
            (
                name.clone(),
                GraphPropertyDef::new(def.value_type().map(str::to_owned), def.required()),
            )
        })
        .collect()
}

fn output_graph_object_type_def(def: &EngineGraphObjectTypeDef) -> GraphObjectTypeDefData {
    GraphObjectTypeDefData::new(
        def.name().as_str().to_owned(),
        output_graph_property_defs(def.properties()),
    )
}

fn output_graph_link_type_def(def: &EngineGraphLinkTypeDef) -> GraphLinkTypeDefData {
    GraphLinkTypeDefData::new(
        def.name().as_str().to_owned(),
        def.source().as_str().to_owned(),
        def.target().as_str().to_owned(),
        def.cardinality().map(str::to_owned),
        output_graph_property_defs(def.properties()),
    )
}

pub(super) const fn graph_ontology_status(status: EngineGraphOntologyStatus) -> &'static str {
    match status {
        EngineGraphOntologyStatus::Draft => "draft",
        EngineGraphOntologyStatus::Frozen => "frozen",
    }
}

pub(super) fn graph_ontology_output(ontology: &EngineGraphOntology) -> GraphOntologyData {
    GraphOntologyData::new(
        ontology.graph().as_str().to_owned(),
        graph_ontology_status(ontology.status()).to_owned(),
        ontology
            .object_types()
            .iter()
            .map(output_graph_object_type_def)
            .collect(),
        ontology
            .link_types()
            .iter()
            .map(output_graph_link_type_def)
            .collect(),
        ontology.version().as_u64(),
        ontology.timestamp().as_micros(),
    )
}

pub(super) fn graph_ontology_summary_output(
    summary: &EngineGraphOntologySummary,
) -> GraphOntologySummaryData {
    GraphOntologySummaryData::new(
        summary.graph().as_str().to_owned(),
        graph_ontology_status(summary.status()).to_owned(),
        summary
            .object_types()
            .iter()
            .map(|entry| {
                GraphObjectTypeSummaryData::new(
                    output_graph_object_type_def(entry.def()),
                    entry.node_count(),
                )
            })
            .collect(),
        summary
            .link_types()
            .iter()
            .map(|entry| {
                GraphLinkTypeSummaryData::new(
                    output_graph_link_type_def(entry.def()),
                    entry.edge_count(),
                )
            })
            .collect(),
        summary.version().as_u64(),
        summary.timestamp().as_micros(),
    )
}

pub(super) fn graph_ontology_write_output(
    outcome: &EngineGraphOntologyWriteOutcome,
    kind: &str,
) -> Output {
    Output::GraphOntologyWriteResult {
        graph: outcome.graph().as_str().to_owned(),
        kind: kind.to_owned(),
        type_name: outcome.type_name().as_str().to_owned(),
        effect: upsert_effect(!outcome.created()),
        commit: commit_receipt(*outcome.commit()),
    }
}

pub(super) fn graph_ontology_delete_output(
    outcome: &EngineGraphDeleteOutcome,
    kind: &str,
    type_name: &str,
) -> Output {
    Output::GraphOntologyDeleteResult {
        graph: outcome.graph().as_str().to_owned(),
        kind: kind.to_owned(),
        type_name: type_name.to_owned(),
        effect: delete_effect(outcome.deleted()),
        commit: outcome.commit().map(commit_receipt),
    }
}

pub(super) fn graph_ontology_freeze_output(outcome: &EngineGraphOntologyFreezeOutcome) -> Output {
    Output::GraphOntologyFreezeResult {
        graph: outcome.graph().as_str().to_owned(),
        object_types: usize_to_u64(outcome.object_types()),
        link_types: usize_to_u64(outcome.link_types()),
        commit: commit_receipt(*outcome.commit()),
    }
}

fn analytics_bound(value: u64, field: &'static str) -> Result<usize, ExecutorError> {
    usize::try_from(value)
        .map_err(|_| ExecutorError::new("invalid_argument.executor.graph_analytics_budget", field))
}

pub(super) fn engine_graph_budget(
    budget: Option<GraphAnalyticsBudget>,
) -> Result<EngineGraphAnalyticsBudget, ExecutorError> {
    let defaults = EngineGraphAnalyticsBudget::default();
    let budget = budget.unwrap_or_default();
    Ok(EngineGraphAnalyticsBudget::new(
        budget
            .max_nodes()
            .map(|value| analytics_bound(value, "budget max_nodes does not fit this platform"))
            .transpose()?
            .unwrap_or(defaults.max_nodes()),
        budget
            .max_edges()
            .map(|value| analytics_bound(value, "budget max_edges does not fit this platform"))
            .transpose()?
            .unwrap_or(defaults.max_edges()),
    ))
}

pub(super) fn engine_graph_pagerank_options(
    damping: Option<f64>,
    max_iterations: Option<u64>,
    tolerance: Option<f64>,
) -> Result<EngineGraphPageRankOptions, ExecutorError> {
    let defaults = EngineGraphPageRankOptions::default();
    let max_iterations = max_iterations
        .map(|value| analytics_bound(value, "max_iterations does not fit this platform"))
        .transpose()?
        .unwrap_or(defaults.max_iterations());
    EngineGraphPageRankOptions::new(
        damping.unwrap_or(defaults.damping()),
        max_iterations,
        tolerance.unwrap_or(defaults.tolerance()),
    )
    .map_err(ExecutorError::from)
}

pub(super) fn engine_graph_cdlp_options(
    max_iterations: Option<u64>,
    direction: Option<GraphDirection>,
) -> Result<EngineGraphCdlpOptions, ExecutorError> {
    let defaults = EngineGraphCdlpOptions::default();
    Ok(EngineGraphCdlpOptions::new(
        max_iterations
            .map(|value| analytics_bound(value, "max_iterations does not fit this platform"))
            .transpose()?
            .unwrap_or(defaults.max_iterations()),
        direction.map_or(defaults.direction(), engine_graph_direction),
    ))
}

pub(super) fn engine_graph_bfs_options(
    max_depth: Option<u64>,
    max_nodes: Option<u64>,
    edge_types: Option<Vec<String>>,
    direction: Option<GraphDirection>,
) -> Result<EngineGraphBfsOptions, ExecutorError> {
    let defaults = EngineGraphBfsOptions::default();
    let max_depth = max_depth
        .map(|value| analytics_bound(value, "max_depth does not fit this platform"))
        .transpose()?
        .unwrap_or(defaults.max_depth());
    let max_nodes = max_nodes
        .map(|value| analytics_bound(value, "max_nodes does not fit this platform"))
        .transpose()?
        .map_or(defaults.max_nodes(), Some);
    let edge_types = edge_types
        .map(|types| {
            types
                .into_iter()
                .map(graph_edge_type)
                .collect::<ExecutorResult<Vec<_>>>()
        })
        .transpose()?;
    Ok(EngineGraphBfsOptions::new(
        max_depth,
        max_nodes,
        edge_types,
        direction.map_or(EngineGraphDirection::Outgoing, engine_graph_direction),
    ))
}

pub(super) fn engine_graph_personalization(
    personalization: std::collections::BTreeMap<String, f64>,
) -> Result<std::collections::HashMap<EngineGraphNodeId, f64>, ExecutorError> {
    personalization
        .into_iter()
        .map(|(node_id, weight)| Ok((graph_node_id(node_id)?, weight)))
        .collect()
}

/// Maps every node id to its component/community representative: the
/// node id at the label index.
fn representative_map(
    index: &EngineGraphAdjacencyIndex,
    labels: &[usize],
) -> std::collections::BTreeMap<String, String> {
    index
        .node_ids()
        .iter()
        .zip(labels)
        .map(|(node_id, label)| {
            let representative = index
                .node_id(*label)
                .map_or_else(|| node_id.as_str(), EngineGraphNodeId::as_str);
            (node_id.as_str().to_owned(), representative.to_owned())
        })
        .collect()
}

pub(super) fn graph_wcc_output(
    index: &EngineGraphAdjacencyIndex,
    result: &EngineGraphWccResult,
) -> Output {
    Output::GraphWccResult(GraphWccData::new(
        index.graph().as_str().to_owned(),
        representative_map(index, result.components()),
        usize_to_u64(result.component_count()),
    ))
}

pub(super) fn graph_lcc_output(
    index: &EngineGraphAdjacencyIndex,
    result: &EngineGraphLccResult,
) -> Output {
    Output::GraphLccResult(GraphLccData::new(
        index.graph().as_str().to_owned(),
        index
            .node_ids()
            .iter()
            .zip(result.coefficients())
            .map(|(node_id, coefficient)| (node_id.as_str().to_owned(), *coefficient))
            .collect(),
    ))
}

pub(super) fn graph_sssp_output(
    index: &EngineGraphAdjacencyIndex,
    direction: GraphDirection,
    result: &EngineGraphSsspResult,
) -> Output {
    let source = index
        .node_id(result.source())
        .map_or("", EngineGraphNodeId::as_str)
        .to_owned();
    let distances = index
        .node_ids()
        .iter()
        .zip(result.distances())
        .filter_map(|(node_id, distance)| {
            distance.map(|distance| (node_id.as_str().to_owned(), distance))
        })
        .collect();
    Output::GraphSsspResult(GraphSsspData::new(
        index.graph().as_str().to_owned(),
        source,
        direction,
        distances,
    ))
}

pub(super) fn graph_pagerank_output(
    index: &EngineGraphAdjacencyIndex,
    result: &EngineGraphPageRankResult,
    personalized: bool,
) -> Output {
    Output::GraphPagerankResult(GraphPagerankData::new(
        index.graph().as_str().to_owned(),
        index
            .node_ids()
            .iter()
            .zip(result.ranks())
            .map(|(node_id, rank)| (node_id.as_str().to_owned(), *rank))
            .collect(),
        usize_to_u64(result.iterations()),
        personalized,
    ))
}

pub(super) fn graph_cdlp_output(
    index: &EngineGraphAdjacencyIndex,
    result: &EngineGraphCdlpResult,
) -> Output {
    Output::GraphCdlpResult(GraphCdlpData::new(
        index.graph().as_str().to_owned(),
        representative_map(index, result.labels()),
    ))
}

pub(super) fn graph_bfs_output(
    index: &EngineGraphAdjacencyIndex,
    start: &EngineGraphNodeId,
    result: &EngineGraphBfsResult,
) -> Output {
    let resolve = |position: usize| {
        index
            .node_id(position)
            .map_or("", EngineGraphNodeId::as_str)
            .to_owned()
    };
    let visited: Vec<String> = result.visited().iter().map(|node| resolve(*node)).collect();
    let depths = result
        .visited()
        .iter()
        .filter_map(|node| {
            result
                .depth(*node)
                .map(|depth| (resolve(*node), usize_to_u64(depth)))
        })
        .collect();
    let edges = result
        .edges()
        .iter()
        .map(|edge| {
            let edge_type = index
                .edge_type_name(edge.edge_type())
                .map_or("", EngineGraphEdgeType::as_str)
                .to_owned();
            GraphBfsEdgeData::new(
                resolve(edge.source()),
                resolve(edge.target()),
                edge_type,
                edge.weight(),
            )
        })
        .collect();
    Output::GraphBfsResult(GraphBfsData::new(
        index.graph().as_str().to_owned(),
        start.as_str().to_owned(),
        visited,
        depths,
        edges,
        result.truncated(),
    ))
}
