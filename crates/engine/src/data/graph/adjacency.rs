//! In-memory adjacency snapshots (GA1).
//!
//! A [`GraphAdjacencyIndex`] is a compact, deterministic picture of one
//! graph's visible nodes and edges, built from storage at a single
//! consistent read — the substrate the later GA slices (traversal and
//! analytics stages) execute on. Because it is built at a read selector,
//! the same machinery serves historical snapshots for free.
//!
//! Memory is bounded up front: the builder refuses graphs beyond the
//! caller's [`GraphAnalyticsBudget`] with
//! `resource_exhausted.engine.graph_analytics_budget` instead of
//! exhausting memory mid-build.

use std::collections::HashMap;

use crate::diagnostics::EngineError;

use super::{GraphEdgeType, GraphName, GraphNodeId};

const DEFAULT_MAX_NODES: usize = 1_000_000;
const DEFAULT_MAX_EDGES: usize = 8_000_000;

/// Size bounds for one adjacency snapshot. The defaults suit embedded use
/// (roughly hundreds of megabytes at worst); callers working on larger
/// graphs opt in explicitly with bigger bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphAnalyticsBudget {
    max_nodes: usize,
    max_edges: usize,
}

impl GraphAnalyticsBudget {
    /// Creates an explicit budget.
    #[must_use]
    pub const fn new(max_nodes: usize, max_edges: usize) -> Self {
        Self {
            max_nodes,
            max_edges,
        }
    }

    #[must_use]
    /// Returns the maximum node count.
    pub const fn max_nodes(&self) -> usize {
        self.max_nodes
    }

    #[must_use]
    /// Returns the maximum edge count.
    pub const fn max_edges(&self) -> usize {
        self.max_edges
    }
}

impl Default for GraphAnalyticsBudget {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_NODES, DEFAULT_MAX_EDGES)
    }
}

/// One directed edge in the snapshot: the neighbor's node index, the
/// interned edge-type index, and the edge weight.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GraphAdjacencyEdge {
    neighbor: usize,
    edge_type: usize,
    weight: f64,
}

impl GraphAdjacencyEdge {
    #[must_use]
    /// Returns the neighbor's node index.
    pub const fn neighbor(&self) -> usize {
        self.neighbor
    }

    #[must_use]
    /// Returns the interned edge-type index
    /// ([`GraphAdjacencyIndex::edge_type_name`] resolves it).
    pub const fn edge_type(&self) -> usize {
        self.edge_type
    }

    #[must_use]
    /// Returns the edge weight.
    pub const fn weight(&self) -> f64 {
        self.weight
    }
}

/// A deterministic adjacency snapshot of one graph.
///
/// Nodes are indexed `0..node_count()` in ascending node-id order; edge
/// lists are sorted by (edge type, neighbor). Two builds over the same
/// visible state produce equal indexes.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphAdjacencyIndex {
    graph: GraphName,
    node_ids: Vec<GraphNodeId>,
    node_lookup: HashMap<GraphNodeId, usize>,
    edge_types: Vec<GraphEdgeType>,
    outgoing: Vec<Vec<GraphAdjacencyEdge>>,
    incoming: Vec<Vec<GraphAdjacencyEdge>>,
    edge_count: u64,
}

impl GraphAdjacencyIndex {
    #[must_use]
    /// Returns the graph this snapshot was built from.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the node count (isolated nodes included).
    pub fn node_count(&self) -> usize {
        self.node_ids.len()
    }

    #[must_use]
    /// Returns the edge count.
    pub const fn edge_count(&self) -> u64 {
        self.edge_count
    }

    #[must_use]
    /// Returns every node id, ascending; positions are node indexes.
    pub fn node_ids(&self) -> &[GraphNodeId] {
        &self.node_ids
    }

    #[must_use]
    /// Returns the node index of `node_id`, when present.
    pub fn node_index(&self, node_id: &GraphNodeId) -> Option<usize> {
        self.node_lookup.get(node_id).copied()
    }

    #[must_use]
    /// Returns the node id at `index`, when in range.
    pub fn node_id(&self, index: usize) -> Option<&GraphNodeId> {
        self.node_ids.get(index)
    }

    #[must_use]
    /// Returns the interned edge type at `index`, when in range.
    pub fn edge_type_name(&self, index: usize) -> Option<&GraphEdgeType> {
        self.edge_types.get(index)
    }

    #[must_use]
    /// Returns the interned index of `edge_type`, when this snapshot
    /// contains at least one edge of that type.
    pub fn edge_type_index(&self, edge_type: &GraphEdgeType) -> Option<usize> {
        // Interned edge types are name-sorted by the builder.
        self.edge_types.binary_search(edge_type).ok()
    }

    #[must_use]
    /// Returns the outgoing edges of the node at `index` (empty when out
    /// of range).
    pub fn outgoing(&self, index: usize) -> &[GraphAdjacencyEdge] {
        self.outgoing.get(index).map_or(&[], Vec::as_slice)
    }

    #[must_use]
    /// Returns the incoming edges of the node at `index` (empty when out
    /// of range).
    pub fn incoming(&self, index: usize) -> &[GraphAdjacencyEdge] {
        self.incoming.get(index).map_or(&[], Vec::as_slice)
    }
}

fn budget_error(kind: &str, limit: usize) -> EngineError {
    EngineError::new(
        "resource_exhausted.engine.graph_analytics_budget",
        format!("graph exceeds the analytics budget of {limit} {kind}; retry with a larger GraphAnalyticsBudget"),
    )
}

/// Two-phase builder: every visible node first (isolated nodes matter to
/// the analytics stages), then every visible edge.
pub(crate) struct GraphAdjacencyIndexBuilder {
    graph: GraphName,
    budget: GraphAnalyticsBudget,
    node_ids: Vec<GraphNodeId>,
    node_lookup: HashMap<GraphNodeId, usize>,
    edge_types: Vec<GraphEdgeType>,
    edge_type_lookup: HashMap<GraphEdgeType, usize>,
    outgoing: Vec<Vec<GraphAdjacencyEdge>>,
    incoming: Vec<Vec<GraphAdjacencyEdge>>,
    edge_count: u64,
    nodes_finished: bool,
}

impl GraphAdjacencyIndexBuilder {
    pub(crate) fn new(graph: GraphName, budget: GraphAnalyticsBudget) -> Self {
        Self {
            graph,
            budget,
            node_ids: Vec::new(),
            node_lookup: HashMap::new(),
            edge_types: Vec::new(),
            edge_type_lookup: HashMap::new(),
            outgoing: Vec::new(),
            incoming: Vec::new(),
            edge_count: 0,
            nodes_finished: false,
        }
    }

    pub(crate) fn add_node(&mut self, node_id: GraphNodeId) -> Result<(), EngineError> {
        debug_assert!(!self.nodes_finished, "add_node after finish_nodes");
        if self.node_ids.len() >= self.budget.max_nodes() {
            return Err(budget_error("nodes", self.budget.max_nodes()));
        }
        self.node_ids.push(node_id);
        Ok(())
    }

    /// Seals the node set: sorts ids (index order is id order) and
    /// prepares the adjacency lists.
    pub(crate) fn finish_nodes(&mut self) {
        debug_assert!(!self.nodes_finished, "finish_nodes twice");
        self.node_ids.sort();
        self.node_lookup = self
            .node_ids
            .iter()
            .enumerate()
            .map(|(index, node_id)| (node_id.clone(), index))
            .collect();
        self.outgoing = vec![Vec::new(); self.node_ids.len()];
        self.incoming = vec![Vec::new(); self.node_ids.len()];
        self.nodes_finished = true;
    }

    pub(crate) fn add_edge(
        &mut self,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
        weight: f64,
    ) -> Result<(), EngineError> {
        debug_assert!(self.nodes_finished, "add_edge before finish_nodes");
        if self.edge_count >= self.budget.max_edges() as u64 {
            return Err(budget_error("edges", self.budget.max_edges()));
        }
        let endpoint = |node_id: &GraphNodeId| {
            self.node_lookup.get(node_id).copied().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.graph_index",
                    "graph edge names a node with no visible row",
                )
            })
        };
        let src_index = endpoint(src)?;
        let dst_index = endpoint(dst)?;
        let edge_type_index = if let Some(index) = self.edge_type_lookup.get(edge_type) {
            *index
        } else {
            let index = self.edge_types.len();
            self.edge_types.push(edge_type.clone());
            self.edge_type_lookup.insert(edge_type.clone(), index);
            index
        };
        self.outgoing[src_index].push(GraphAdjacencyEdge {
            neighbor: dst_index,
            edge_type: edge_type_index,
            weight,
        });
        self.incoming[dst_index].push(GraphAdjacencyEdge {
            neighbor: src_index,
            edge_type: edge_type_index,
            weight,
        });
        self.edge_count += 1;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> GraphAdjacencyIndex {
        debug_assert!(self.nodes_finished, "finish before finish_nodes");
        // Edge-type indexes follow first-encounter order, which depends on
        // the storage scan; remap them to name order so equal visible
        // states build equal indexes regardless of construction history.
        let mut order: Vec<usize> = (0..self.edge_types.len()).collect();
        order.sort_by(|left, right| self.edge_types[*left].cmp(&self.edge_types[*right]));
        let mut remap = vec![0usize; self.edge_types.len()];
        for (new_index, old_index) in order.iter().enumerate() {
            remap[*old_index] = new_index;
        }
        let mut edge_types = self.edge_types.clone();
        edge_types.sort();
        for list in self.outgoing.iter_mut().chain(self.incoming.iter_mut()) {
            for edge in list.iter_mut() {
                edge.edge_type = remap[edge.edge_type];
            }
            list.sort_by(|left, right| {
                (left.edge_type, left.neighbor).cmp(&(right.edge_type, right.neighbor))
            });
        }
        GraphAdjacencyIndex {
            graph: self.graph,
            node_ids: self.node_ids,
            node_lookup: self.node_lookup,
            edge_types,
            outgoing: self.outgoing,
            incoming: self.incoming,
            edge_count: self.edge_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GraphAdjacencyIndexBuilder, GraphAnalyticsBudget};
    use crate::data::graph::{GraphEdgeType, GraphName, GraphNodeId};

    fn node(value: &str) -> GraphNodeId {
        GraphNodeId::new(value).expect("node id")
    }

    fn edge_type(value: &str) -> GraphEdgeType {
        GraphEdgeType::new(value).expect("edge type")
    }

    fn builder(budget: GraphAnalyticsBudget) -> GraphAdjacencyIndexBuilder {
        GraphAdjacencyIndexBuilder::new(GraphName::new("deps").expect("graph"), budget)
    }

    #[test]
    fn index_is_deterministic_and_id_ordered() {
        let build = |node_order: &[&str], edge_order: &[(&str, &str, &str, f64)]| {
            let mut builder = builder(GraphAnalyticsBudget::default());
            for id in node_order {
                builder.add_node(node(id)).expect("node fits");
            }
            builder.finish_nodes();
            for (src, kind, dst, weight) in edge_order {
                builder
                    .add_edge(&node(src), &edge_type(kind), &node(dst), *weight)
                    .expect("edge fits");
            }
            builder.finish()
        };

        let forward = build(
            &["c", "a", "b"],
            &[("a", "links", "b", 1.0), ("a", "cites", "c", 2.0)],
        );
        let shuffled = build(
            &["b", "c", "a"],
            &[("a", "cites", "c", 2.0), ("a", "links", "b", 1.0)],
        );
        assert_eq!(forward, shuffled, "equal state builds equal indexes");

        let ids: Vec<&str> = forward.node_ids().iter().map(GraphNodeId::as_str).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
        let a = forward.node_index(&node("a")).expect("a present");
        let out = forward.outgoing(a);
        assert_eq!(out.len(), 2);
        // Sorted by (edge type, neighbor): "cites" < "links".
        assert_eq!(
            forward
                .edge_type_name(out[0].edge_type())
                .expect("type")
                .as_str(),
            "cites"
        );
        assert!((out[1].weight() - 1.0).abs() < f64::EPSILON);
        let b = forward.node_index(&node("b")).expect("b present");
        assert_eq!(forward.incoming(b).len(), 1);
        assert_eq!(forward.incoming(b)[0].neighbor(), a);
    }

    #[test]
    fn self_loops_count_once_and_appear_both_ways() {
        let mut builder = builder(GraphAnalyticsBudget::default());
        builder.add_node(node("a")).expect("node fits");
        builder.finish_nodes();
        builder
            .add_edge(&node("a"), &edge_type("links"), &node("a"), 1.0)
            .expect("edge fits");
        let index = builder.finish();
        assert_eq!(index.edge_count(), 1);
        assert_eq!(index.outgoing(0).len(), 1);
        assert_eq!(index.incoming(0).len(), 1);
        assert_eq!(index.outgoing(0)[0].neighbor(), 0);
    }

    #[test]
    fn budget_refusals_are_typed() {
        let mut builder = builder(GraphAnalyticsBudget::new(1, 10));
        builder.add_node(node("a")).expect("first node fits");
        let error = builder.add_node(node("b")).expect_err("node budget");
        assert_eq!(
            error.code(),
            "resource_exhausted.engine.graph_analytics_budget"
        );

        let mut builder = builder_with_nodes(GraphAnalyticsBudget::new(10, 1));
        builder
            .add_edge(&node("a"), &edge_type("links"), &node("b"), 1.0)
            .expect("first edge fits");
        let error = builder
            .add_edge(&node("b"), &edge_type("links"), &node("a"), 1.0)
            .expect_err("edge budget");
        assert_eq!(
            error.code(),
            "resource_exhausted.engine.graph_analytics_budget"
        );
    }

    fn builder_with_nodes(budget: GraphAnalyticsBudget) -> GraphAdjacencyIndexBuilder {
        let mut builder = builder(budget);
        builder.add_node(node("a")).expect("node fits");
        builder.add_node(node("b")).expect("node fits");
        builder.finish_nodes();
        builder
    }

    #[test]
    fn unknown_endpoints_are_corruption() {
        let mut builder = builder_with_nodes(GraphAnalyticsBudget::default());
        let error = builder
            .add_edge(&node("a"), &edge_type("links"), &node("ghost"), 1.0)
            .expect_err("unknown endpoint");
        assert_eq!(error.code(), "data_loss.engine.graph_index");
    }
}
