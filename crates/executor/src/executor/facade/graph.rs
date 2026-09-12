use super::super::{
    Command, Executor, ExecutorError, GraphBatchOperation, GraphBindingTarget, GraphBulkEdge,
    GraphBulkNode, GraphDeletePolicy, GraphDirection, GraphEntityBinding, Output,
};

impl Executor {
    /// Executes a default-branch graph-create command.
    pub fn graph_create(&mut self, graph: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: graph.into(),
        })
    }

    /// Executes a default-branch graph-delete command.
    pub fn graph_delete(&mut self, graph: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphDelete {
            branch: None,
            space: None,
            graph: graph.into(),
        })
    }

    /// Executes a default-branch graph-list command.
    pub fn graph_list(
        &mut self,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphList {
            branch: None,
            space: None,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph-metadata command.
    pub fn graph_get_meta(&mut self, graph: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphGetMeta {
            branch: None,
            space: None,
            graph: graph.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph node upsert command.
    pub fn graph_add_node(
        &mut self,
        graph: impl Into<String>,
        node_id: impl Into<String>,
        properties: Option<serde_json::Value>,
        binding: Option<GraphEntityBinding>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphAddNode {
            branch: None,
            space: None,
            graph: graph.into(),
            node_id: node_id.into(),
            properties,
            binding,
            object_type: None,
        })
    }

    /// Executes a default-branch typed graph node add command.
    pub fn graph_add_typed_node(
        &mut self,
        graph: impl Into<String>,
        node_id: impl Into<String>,
        object_type: impl Into<String>,
        properties: Option<serde_json::Value>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphAddNode {
            branch: None,
            space: None,
            graph: graph.into(),
            node_id: node_id.into(),
            properties,
            binding: None,
            object_type: Some(object_type.into()),
        })
    }

    /// Executes a default-branch object type definition command.
    pub fn graph_define_object_type(
        &mut self,
        graph: impl Into<String>,
        name: impl Into<String>,
        properties: std::collections::BTreeMap<String, crate::types::GraphPropertyDef>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphDefineObjectType {
            branch: None,
            space: None,
            graph: graph.into(),
            name: name.into(),
            properties,
        })
    }

    /// Executes a default-branch link type definition command.
    pub fn graph_define_link_type(
        &mut self,
        graph: impl Into<String>,
        name: impl Into<String>,
        source: impl Into<String>,
        target: impl Into<String>,
        cardinality: Option<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphDefineLinkType {
            branch: None,
            space: None,
            graph: graph.into(),
            name: name.into(),
            source: source.into(),
            target: target.into(),
            cardinality,
            properties: std::collections::BTreeMap::new(),
        })
    }

    /// Executes a default-branch object type deletion command.
    pub fn graph_delete_object_type(
        &mut self,
        graph: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphDeleteObjectType {
            branch: None,
            space: None,
            graph: graph.into(),
            name: name.into(),
        })
    }

    /// Executes a default-branch link type deletion command.
    pub fn graph_delete_link_type(
        &mut self,
        graph: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphDeleteLinkType {
            branch: None,
            space: None,
            graph: graph.into(),
            name: name.into(),
        })
    }

    /// Executes a default-branch ontology freeze command.
    pub fn graph_freeze_ontology(
        &mut self,
        graph: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphFreezeOntology {
            branch: None,
            space: None,
            graph: graph.into(),
        })
    }

    /// Executes a default-branch ontology read command.
    pub fn graph_get_ontology(
        &mut self,
        graph: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphGetOntology {
            branch: None,
            space: None,
            graph: graph.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch ontology summary command.
    pub fn graph_ontology_summary(
        &mut self,
        graph: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphOntologySummary {
            branch: None,
            space: None,
            graph: graph.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch nodes-by-type command.
    pub fn graph_nodes_by_type(
        &mut self,
        graph: impl Into<String>,
        object_type: impl Into<String>,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphNodesByType {
            branch: None,
            space: None,
            graph: graph.into(),
            object_type: object_type.into(),
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph node get command.
    pub fn graph_get_node(
        &mut self,
        graph: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphGetNode {
            branch: None,
            space: None,
            graph: graph.into(),
            node_id: node_id.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph node delete command.
    pub fn graph_remove_node(
        &mut self,
        graph: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphRemoveNode {
            branch: None,
            space: None,
            graph: graph.into(),
            node_id: node_id.into(),
        })
    }

    /// Executes a default-branch graph node-list command.
    pub fn graph_list_nodes(
        &mut self,
        graph: impl Into<String>,
        prefix: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphListNodes {
            branch: None,
            space: None,
            graph: graph.into(),
            prefix,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph edge upsert command.
    #[allow(clippy::too_many_arguments)]
    pub fn graph_add_edge(
        &mut self,
        graph: impl Into<String>,
        src: impl Into<String>,
        edge_type: impl Into<String>,
        dst: impl Into<String>,
        weight: Option<f64>,
        properties: Option<serde_json::Value>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: graph.into(),
            src: src.into(),
            edge_type: edge_type.into(),
            dst: dst.into(),
            weight,
            properties,
        })
    }

    /// Executes a default-branch graph edge get command.
    pub fn graph_get_edge(
        &mut self,
        graph: impl Into<String>,
        src: impl Into<String>,
        edge_type: impl Into<String>,
        dst: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphGetEdge {
            branch: None,
            space: None,
            graph: graph.into(),
            src: src.into(),
            edge_type: edge_type.into(),
            dst: dst.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph edge delete command.
    pub fn graph_remove_edge(
        &mut self,
        graph: impl Into<String>,
        src: impl Into<String>,
        edge_type: impl Into<String>,
        dst: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphRemoveEdge {
            branch: None,
            space: None,
            graph: graph.into(),
            src: src.into(),
            edge_type: edge_type.into(),
            dst: dst.into(),
        })
    }

    /// Executes a default-branch graph neighbors command.
    pub fn graph_neighbors(
        &mut self,
        graph: impl Into<String>,
        node_id: impl Into<String>,
        direction: GraphDirection,
        edge_type: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphNeighbors {
            branch: None,
            space: None,
            graph: graph.into(),
            node_id: node_id.into(),
            direction,
            edge_type,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph binding lookup command.
    pub fn graph_bindings_for_entity(
        &mut self,
        target: GraphBindingTarget,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphBindingsForEntity {
            branch: None,
            space: None,
            target,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch graph batch write command.
    pub fn graph_batch_write(
        &mut self,
        graph: impl Into<String>,
        operations: Vec<GraphBatchOperation>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphBatchWrite {
            branch: None,
            space: None,
            graph: graph.into(),
            operations,
        })
    }

    /// Executes a default-branch weakly-connected-components command.
    pub fn graph_wcc(&mut self, graph: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphWcc {
            branch: None,
            space: None,
            graph: graph.into(),
            budget: None,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch clustering-coefficient command.
    pub fn graph_lcc(&mut self, graph: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphLcc {
            branch: None,
            space: None,
            graph: graph.into(),
            budget: None,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch shortest-path command.
    pub fn graph_sssp(
        &mut self,
        graph: impl Into<String>,
        source: impl Into<String>,
        direction: Option<GraphDirection>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphSssp {
            branch: None,
            space: None,
            graph: graph.into(),
            source: source.into(),
            direction,
            budget: None,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch `PageRank` command, personalized when
    /// seed weights are provided.
    pub fn graph_pagerank(
        &mut self,
        graph: impl Into<String>,
        personalization: Option<std::collections::BTreeMap<String, f64>>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphPagerank {
            branch: None,
            space: None,
            graph: graph.into(),
            damping: None,
            max_iterations: None,
            tolerance: None,
            personalization,
            budget: None,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch community-detection command.
    pub fn graph_cdlp(&mut self, graph: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphCdlp {
            branch: None,
            space: None,
            graph: graph.into(),
            max_iterations: None,
            direction: None,
            budget: None,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch breadth-first-traversal command.
    pub fn graph_bfs(
        &mut self,
        graph: impl Into<String>,
        start: impl Into<String>,
        direction: Option<GraphDirection>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphBfs {
            branch: None,
            space: None,
            graph: graph.into(),
            start: start.into(),
            max_depth: None,
            max_nodes: None,
            edge_types: None,
            direction,
            budget: None,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch delete-policy application.
    pub fn graph_apply_delete_policy(
        &mut self,
        target: GraphBindingTarget,
        policy: GraphDeletePolicy,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphApplyDeletePolicy {
            branch: None,
            space: None,
            target,
            policy,
        })
    }

    /// Executes a default-branch bulk ingest.
    pub fn graph_bulk_insert(
        &mut self,
        graph: impl Into<String>,
        nodes: Vec<GraphBulkNode>,
        edges: Vec<GraphBulkEdge>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::GraphBulkInsert {
            branch: None,
            space: None,
            graph: graph.into(),
            nodes,
            edges,
            chunk_size: None,
        })
    }
}
