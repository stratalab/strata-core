//! Graph core outcome types.

use strata_core::{CommitVersion, Timestamp};

use crate::commit::CommitOutcome;

use super::{
    GraphDirection, GraphEdgeData, GraphEdgeType, GraphEntityBinding, GraphName, GraphNodeData,
    GraphNodeId,
};

/// Graph metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphInfo {
    name: GraphName,
    node_count: u64,
    edge_count: u64,
    created_version: CommitVersion,
    created_timestamp: Timestamp,
    updated_version: CommitVersion,
    updated_timestamp: Timestamp,
    import_pending: bool,
}

impl GraphInfo {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        name: GraphName,
        node_count: u64,
        edge_count: u64,
        created_version: CommitVersion,
        created_timestamp: Timestamp,
        updated_version: CommitVersion,
        updated_timestamp: Timestamp,
        import_pending: bool,
    ) -> Self {
        Self {
            name,
            node_count,
            edge_count,
            created_version,
            created_timestamp,
            updated_version,
            updated_timestamp,
            import_pending,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn name(&self) -> &GraphName {
        &self.name
    }

    #[must_use]
    /// Whether a `bulk_insert` spanning more than one commit began and has
    /// not finished (#3464): its first chunk commit sets the watermark and
    /// its last clears it, so a crash in between leaves it set. The graph
    /// is readable and writable meanwhile; re-running the same payload —
    /// every row an upsert — completes the import and clears it.
    pub const fn import_pending(&self) -> bool {
        self.import_pending
    }

    #[must_use]
    /// Returns visible node count.
    pub const fn node_count(&self) -> u64 {
        self.node_count
    }

    #[must_use]
    /// Returns visible edge count.
    pub const fn edge_count(&self) -> u64 {
        self.edge_count
    }

    #[must_use]
    /// Returns the commit version that created the visible graph metadata row.
    pub const fn created_version(&self) -> CommitVersion {
        self.created_version
    }

    #[must_use]
    /// Returns the commit timestamp that created the visible graph metadata row.
    pub const fn created_timestamp(&self) -> Timestamp {
        self.created_timestamp
    }

    #[must_use]
    /// Returns the latest commit version that changed visible graph state.
    pub const fn updated_version(&self) -> CommitVersion {
        self.updated_version
    }

    #[must_use]
    /// Returns the latest commit timestamp that changed visible graph state.
    pub const fn updated_timestamp(&self) -> Timestamp {
        self.updated_timestamp
    }
}

/// Paginated graph names.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphNamePage {
    graphs: Vec<GraphName>,
    has_more: bool,
    cursor: Option<GraphName>,
}

impl GraphNamePage {
    pub(crate) const fn new(
        graphs: Vec<GraphName>,
        has_more: bool,
        cursor: Option<GraphName>,
    ) -> Self {
        Self {
            graphs,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns graph names in this page.
    pub fn graphs(&self) -> &[GraphName] {
        &self.graphs
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns cursor for the next page.
    pub const fn cursor(&self) -> Option<&GraphName> {
        self.cursor.as_ref()
    }
}

/// Latest graph node with commit facts.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphNode {
    graph: GraphName,
    node_id: GraphNodeId,
    data: GraphNodeData,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl GraphNode {
    pub(crate) const fn new(
        graph: GraphName,
        node_id: GraphNodeId,
        data: GraphNodeData,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            graph,
            node_id,
            data,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the node id.
    pub const fn node_id(&self) -> &GraphNodeId {
        &self.node_id
    }

    #[must_use]
    /// Returns the node data.
    pub const fn data(&self) -> &GraphNodeData {
        &self.data
    }

    #[must_use]
    /// Returns commit version.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns commit timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Paginated graph nodes.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphNodePage {
    nodes: Vec<GraphNode>,
    has_more: bool,
    cursor: Option<GraphNodeId>,
}

impl GraphNodePage {
    pub(crate) const fn new(
        nodes: Vec<GraphNode>,
        has_more: bool,
        cursor: Option<GraphNodeId>,
    ) -> Self {
        Self {
            nodes,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns nodes in this page.
    pub fn nodes(&self) -> &[GraphNode] {
        &self.nodes
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns cursor for the next page.
    pub const fn cursor(&self) -> Option<&GraphNodeId> {
        self.cursor.as_ref()
    }
}

/// Latest graph edge with commit facts.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphEdge {
    graph: GraphName,
    src: GraphNodeId,
    edge_type: GraphEdgeType,
    dst: GraphNodeId,
    data: GraphEdgeData,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl GraphEdge {
    pub(crate) const fn new(
        graph: GraphName,
        src: GraphNodeId,
        edge_type: GraphEdgeType,
        dst: GraphNodeId,
        data: GraphEdgeData,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            graph,
            src,
            edge_type,
            dst,
            data,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the source node id.
    pub const fn src(&self) -> &GraphNodeId {
        &self.src
    }

    #[must_use]
    /// Returns the edge type.
    pub const fn edge_type(&self) -> &GraphEdgeType {
        &self.edge_type
    }

    #[must_use]
    /// Returns the destination node id.
    pub const fn dst(&self) -> &GraphNodeId {
        &self.dst
    }

    #[must_use]
    /// Returns edge data.
    pub const fn data(&self) -> &GraphEdgeData {
        &self.data
    }

    #[must_use]
    /// Returns commit version.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns commit timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Paginated graph edges with their full edge data.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphEdgePage {
    edges: Vec<GraphEdge>,
    has_more: bool,
    cursor: Option<String>,
}

impl GraphEdgePage {
    pub(crate) const fn new(edges: Vec<GraphEdge>, has_more: bool, cursor: Option<String>) -> Self {
        Self {
            edges,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns edges in this page, each with its full `GraphEdgeData`.
    pub fn edges(&self) -> &[GraphEdge] {
        &self.edges
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns the cursor for the next page.
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

/// One neighbor hit.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphNeighbor {
    node: GraphNode,
    edge: GraphEdge,
    direction: GraphDirection,
    target_status: Option<GraphTargetStatus>,
}

impl GraphNeighbor {
    pub(crate) const fn new(
        node: GraphNode,
        edge: GraphEdge,
        direction: GraphDirection,
        target_status: Option<GraphTargetStatus>,
    ) -> Self {
        Self {
            node,
            edge,
            direction,
            target_status,
        }
    }

    #[must_use]
    /// Returns the neighboring node.
    pub const fn node(&self) -> &GraphNode {
        &self.node
    }

    #[must_use]
    /// Returns the edge connecting the requested node and neighbor.
    pub const fn edge(&self) -> &GraphEdge {
        &self.edge
    }

    #[must_use]
    /// Returns which direction produced the hit.
    pub const fn direction(&self) -> GraphDirection {
        self.direction
    }

    #[must_use]
    /// Returns the neighbor's bound-entity status (`None` when the node
    /// has no binding), resolved at the read's temporal context.
    pub const fn target_status(&self) -> Option<GraphTargetStatus> {
        self.target_status
    }
}

/// Result of one chunked bulk ingest.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphBulkInsertOutcome {
    graph: GraphName,
    nodes_inserted: u64,
    edges_inserted: u64,
    commits: u64,
    last_commit: Option<CommitOutcome>,
}

impl GraphBulkInsertOutcome {
    pub(crate) const fn new(
        graph: GraphName,
        nodes_inserted: u64,
        edges_inserted: u64,
        commits: u64,
        last_commit: Option<CommitOutcome>,
    ) -> Self {
        Self {
            graph,
            nodes_inserted,
            edges_inserted,
            commits,
            last_commit,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns how many node upserts the ingest applied.
    pub const fn nodes_inserted(&self) -> u64 {
        self.nodes_inserted
    }

    #[must_use]
    /// Returns how many edge upserts the ingest applied.
    pub const fn edges_inserted(&self) -> u64 {
        self.edges_inserted
    }

    #[must_use]
    /// Returns how many chunk commits the ingest produced.
    pub const fn commits(&self) -> u64 {
        self.commits
    }

    #[must_use]
    /// Returns the final chunk's commit, when any chunk committed.
    pub const fn last_commit(&self) -> Option<&CommitOutcome> {
        self.last_commit.as_ref()
    }
}

/// Result of applying a delete policy to every graph fact bound to
/// one entity target.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphDeletePolicyOutcome {
    policy: super::GraphDeletePolicy,
    nodes_affected: u64,
    commit: Option<CommitOutcome>,
}

impl GraphDeletePolicyOutcome {
    pub(crate) const fn new(
        policy: super::GraphDeletePolicy,
        nodes_affected: u64,
        commit: Option<CommitOutcome>,
    ) -> Self {
        Self {
            policy,
            nodes_affected,
            commit,
        }
    }

    #[must_use]
    /// Returns the applied policy.
    pub const fn policy(&self) -> super::GraphDeletePolicy {
        self.policy
    }

    #[must_use]
    /// Returns how many bound nodes the policy covered.
    pub const fn nodes_affected(&self) -> u64 {
        self.nodes_affected
    }

    #[must_use]
    /// Returns the commit when the policy mutated rows.
    pub const fn commit(&self) -> Option<&CommitOutcome> {
        self.commit.as_ref()
    }
}

/// Resolution status of one binding target, per the relationship-layer
/// contract vocabulary. Dangling references are explicit: traversal
/// reports the target's state instead of silently dropping it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphTargetStatus {
    /// The target row is visible in the read's temporal context.
    Present,
    /// The target has a visible tombstone: it existed and was deleted.
    Deleted,
    /// No target row exists in the requested context.
    Missing,
    /// The binding key cannot address a row of the target primitive.
    MalformedTarget,
    /// The target primitive uses a composite address this engine does
    /// not resolve yet (vector and graph targets).
    Unsupported,
}

/// Paginated neighbor hits.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphNeighborPage {
    neighbors: Vec<GraphNeighbor>,
    has_more: bool,
    cursor: Option<String>,
}

impl GraphNeighborPage {
    pub(crate) const fn new(
        neighbors: Vec<GraphNeighbor>,
        has_more: bool,
        cursor: Option<String>,
    ) -> Self {
        Self {
            neighbors,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns neighbor hits.
    pub fn neighbors(&self) -> &[GraphNeighbor] {
        &self.neighbors
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns cursor for the next page.
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

/// One graph node binding hit.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphBinding {
    graph: GraphName,
    node_id: GraphNodeId,
    binding: GraphEntityBinding,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl GraphBinding {
    pub(crate) const fn new(
        graph: GraphName,
        node_id: GraphNodeId,
        binding: GraphEntityBinding,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            graph,
            node_id,
            binding,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the node id.
    pub const fn node_id(&self) -> &GraphNodeId {
        &self.node_id
    }

    #[must_use]
    /// Returns the binding.
    pub const fn binding(&self) -> &GraphEntityBinding {
        &self.binding
    }

    #[must_use]
    /// Returns commit version.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns commit timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Paginated entity binding hits.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphBindingPage {
    bindings: Vec<GraphBinding>,
    has_more: bool,
    cursor: Option<String>,
}

impl GraphBindingPage {
    pub(crate) const fn new(
        bindings: Vec<GraphBinding>,
        has_more: bool,
        cursor: Option<String>,
    ) -> Self {
        Self {
            bindings,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns binding hits.
    pub fn bindings(&self) -> &[GraphBinding] {
        &self.bindings
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns cursor for the next page.
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

/// Node write outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphWriteOutcome {
    graph: GraphName,
    node_id: GraphNodeId,
    created: bool,
    commit: CommitOutcome,
}

impl GraphWriteOutcome {
    pub(crate) const fn new(
        graph: GraphName,
        node_id: GraphNodeId,
        created: bool,
        commit: CommitOutcome,
    ) -> Self {
        Self {
            graph,
            node_id,
            created,
            commit,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the node id.
    pub const fn node_id(&self) -> &GraphNodeId {
        &self.node_id
    }

    #[must_use]
    /// Returns true when the node did not previously exist.
    pub const fn created(&self) -> bool {
        self.created
    }

    #[must_use]
    /// Returns commit outcome.
    pub const fn commit(&self) -> CommitOutcome {
        self.commit
    }
}

/// Edge write outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphEdgeWriteOutcome {
    graph: GraphName,
    src: GraphNodeId,
    edge_type: GraphEdgeType,
    dst: GraphNodeId,
    created: bool,
    commit: CommitOutcome,
}

impl GraphEdgeWriteOutcome {
    pub(crate) const fn new(
        graph: GraphName,
        src: GraphNodeId,
        edge_type: GraphEdgeType,
        dst: GraphNodeId,
        created: bool,
        commit: CommitOutcome,
    ) -> Self {
        Self {
            graph,
            src,
            edge_type,
            dst,
            created,
            commit,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns source node id.
    pub const fn src(&self) -> &GraphNodeId {
        &self.src
    }

    #[must_use]
    /// Returns edge type.
    pub const fn edge_type(&self) -> &GraphEdgeType {
        &self.edge_type
    }

    #[must_use]
    /// Returns destination node id.
    pub const fn dst(&self) -> &GraphNodeId {
        &self.dst
    }

    #[must_use]
    /// Returns true when the edge did not previously exist.
    pub const fn created(&self) -> bool {
        self.created
    }

    #[must_use]
    /// Returns commit outcome.
    pub const fn commit(&self) -> CommitOutcome {
        self.commit
    }
}

/// Delete outcome for graph, node, and edge deletes.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphDeleteOutcome {
    graph: GraphName,
    deleted: bool,
    commit: Option<CommitOutcome>,
}

impl GraphDeleteOutcome {
    pub(crate) const fn new(
        graph: GraphName,
        deleted: bool,
        commit: Option<CommitOutcome>,
    ) -> Self {
        Self {
            graph,
            deleted,
            commit,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns true when a visible graph fact was deleted.
    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    #[must_use]
    /// Returns commit outcome when a delete was applied.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// One positional batch operation outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphBatchOpOutcome {
    operation_index: usize,
    created: Option<bool>,
    deleted: Option<bool>,
}

impl GraphBatchOpOutcome {
    pub(crate) const fn created(operation_index: usize, created: bool) -> Self {
        Self {
            operation_index,
            created: Some(created),
            deleted: None,
        }
    }

    pub(crate) const fn deleted(operation_index: usize, deleted: bool) -> Self {
        Self {
            operation_index,
            created: None,
            deleted: Some(deleted),
        }
    }

    #[must_use]
    /// Returns the input operation index.
    pub const fn operation_index(&self) -> usize {
        self.operation_index
    }

    #[must_use]
    /// Returns create/update fact for upsert operations.
    pub const fn created_flag(&self) -> Option<bool> {
        self.created
    }

    #[must_use]
    /// Returns delete/no-op fact for delete operations.
    pub const fn deleted_flag(&self) -> Option<bool> {
        self.deleted
    }
}

/// Batch write outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphBatchWriteOutcome {
    graph: GraphName,
    results: Vec<GraphBatchOpOutcome>,
    commit: Option<CommitOutcome>,
}

impl GraphBatchWriteOutcome {
    pub(crate) const fn new(
        graph: GraphName,
        results: Vec<GraphBatchOpOutcome>,
        commit: Option<CommitOutcome>,
    ) -> Self {
        Self {
            graph,
            results,
            commit,
        }
    }

    #[must_use]
    /// Returns the graph name.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns positional operation outcomes.
    pub fn results(&self) -> &[GraphBatchOpOutcome] {
        &self.results
    }

    #[must_use]
    /// Returns commit outcome when mutations were applied.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

#[cfg(test)]
mod tests {
    use strata_core::{CommitVersion, Timestamp};

    use crate::commit::CommitDurability;

    use super::{
        CommitOutcome, GraphBatchOpOutcome, GraphBatchWriteOutcome, GraphBinding, GraphBindingPage,
        GraphDeleteOutcome, GraphDirection, GraphEdge, GraphEdgeData, GraphEdgeType,
        GraphEdgeWriteOutcome, GraphInfo, GraphName, GraphNamePage, GraphNeighbor,
        GraphNeighborPage, GraphNode, GraphNodeData, GraphNodeId, GraphNodePage, GraphWriteOutcome,
    };

    fn commit() -> CommitOutcome {
        CommitOutcome::new(
            CommitVersion::new(7),
            Timestamp::from_micros(11),
            2,
            1,
            CommitDurability::Standard,
        )
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn graph_outcomes_expose_engine_facts_without_storage_types() {
        let graph = GraphName::new("deps").expect("graph");
        let node_id = GraphNodeId::new("a").expect("node");
        let dst = GraphNodeId::new("b").expect("dst");
        let edge_type = GraphEdgeType::new("links").expect("edge type");
        let commit = commit();

        let info = GraphInfo::new(
            graph.clone(),
            3,
            4,
            commit.version(),
            commit.timestamp(),
            commit.version(),
            commit.timestamp(),
            true,
        );
        assert_eq!(info.name(), &graph);
        assert_eq!(info.node_count(), 3);
        assert_eq!(info.edge_count(), 4);
        assert!(info.import_pending());
        assert_eq!(info.created_version(), commit.version());
        assert_eq!(info.created_timestamp(), commit.timestamp());
        assert_eq!(info.updated_version(), commit.version());
        assert_eq!(info.updated_timestamp(), commit.timestamp());

        let node = GraphNode::new(
            graph.clone(),
            node_id.clone(),
            GraphNodeData::default(),
            commit.version(),
            commit.timestamp(),
        );
        assert_eq!(node.graph(), &graph);
        assert_eq!(node.node_id(), &node_id);
        assert_eq!(node.version(), commit.version());
        assert_eq!(node.timestamp(), commit.timestamp());

        let edge = GraphEdge::new(
            graph.clone(),
            node_id.clone(),
            edge_type.clone(),
            dst.clone(),
            GraphEdgeData::default(),
            commit.version(),
            commit.timestamp(),
        );
        assert_eq!(edge.graph(), &graph);
        assert_eq!(edge.src(), &node_id);
        assert_eq!(edge.edge_type(), &edge_type);
        assert_eq!(edge.dst(), &dst);
        assert_eq!(edge.version(), commit.version());

        let graph_page = GraphNamePage::new(vec![graph.clone()], true, Some(graph.clone()));
        assert_eq!(graph_page.graphs(), std::slice::from_ref(&graph));
        assert!(graph_page.has_more());
        assert_eq!(graph_page.cursor(), Some(&graph));

        let node_page = GraphNodePage::new(vec![node.clone()], true, Some(node_id.clone()));
        assert_eq!(node_page.nodes(), std::slice::from_ref(&node));
        assert!(node_page.has_more());
        assert_eq!(node_page.cursor(), Some(&node_id));

        let neighbor =
            GraphNeighbor::new(node.clone(), edge.clone(), GraphDirection::Outgoing, None);
        assert_eq!(neighbor.node(), &node);
        assert_eq!(neighbor.edge(), &edge);
        assert_eq!(neighbor.direction(), GraphDirection::Outgoing);
        let neighbor_page =
            GraphNeighborPage::new(vec![neighbor.clone()], true, Some("cursor".to_owned()));
        assert_eq!(neighbor_page.neighbors(), &[neighbor]);
        assert!(neighbor_page.has_more());
        assert_eq!(neighbor_page.cursor(), Some("cursor"));

        let binding_page = GraphBindingPage::new(Vec::<GraphBinding>::new(), false, None);
        assert!(binding_page.bindings().is_empty());
        assert!(!binding_page.has_more());
        assert_eq!(binding_page.cursor(), None);

        let write = GraphWriteOutcome::new(graph.clone(), node_id.clone(), true, commit);
        assert_eq!(write.graph(), &graph);
        assert_eq!(write.node_id(), &node_id);
        assert!(write.created());
        assert_eq!(write.commit(), commit);

        let edge_write = GraphEdgeWriteOutcome::new(
            graph.clone(),
            node_id.clone(),
            edge_type.clone(),
            dst,
            false,
            commit,
        );
        assert_eq!(edge_write.graph(), &graph);
        assert_eq!(edge_write.src(), &node_id);
        assert_eq!(edge_write.edge_type(), &edge_type);
        assert!(!edge_write.created());

        let delete = GraphDeleteOutcome::new(graph.clone(), true, Some(commit));
        assert_eq!(delete.graph(), &graph);
        assert!(delete.deleted());
        assert_eq!(delete.commit(), Some(commit));

        let created = GraphBatchOpOutcome::created(0, true);
        let deleted = GraphBatchOpOutcome::deleted(1, false);
        assert_eq!(created.operation_index(), 0);
        assert_eq!(created.created_flag(), Some(true));
        assert_eq!(deleted.operation_index(), 1);
        assert_eq!(deleted.deleted_flag(), Some(false));
        let batch =
            GraphBatchWriteOutcome::new(graph.clone(), vec![created, deleted], Some(commit));
        assert_eq!(batch.graph(), &graph);
        assert_eq!(batch.results().len(), 2);
        assert_eq!(batch.commit(), Some(commit));
    }
}
