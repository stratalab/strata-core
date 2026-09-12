//! Graph traversal (GA4): breadth-first search, degree, and subgraph
//! extraction over the adjacency snapshot.
//!
//! Like the analytics stages, traversal runs as pure methods on
//! [`GraphAdjacencyIndex`]: build one snapshot at any read selector,
//! then traverse it as many times as needed — historical traversal
//! needs no extra machinery. Neighbor lists are (edge type, neighbor)
//! sorted by the builder, so visit order is deterministic for a given
//! visible state.
//!
//! Semantics are ported from the v0.6 engine: bounded breadth-first
//! search with depth, node, edge-type, and direction controls; degree
//! per direction (a self-loop counts once per direction, twice for
//! both); subgraph extraction keeping only edges whose endpoints are
//! both selected.

use std::collections::{HashSet, VecDeque};

use crate::diagnostics::EngineError;

use super::{GraphAdjacencyIndex, GraphDirection, GraphEdgeType, GraphNodeId};

const DEFAULT_BFS_MAX_DEPTH: usize = 100;
const DEFAULT_BFS_MAX_NODES: usize = 10_000;

/// Options for breadth-first search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphBfsOptions {
    max_depth: usize,
    max_nodes: Option<usize>,
    edge_types: Option<Vec<GraphEdgeType>>,
    direction: GraphDirection,
}

impl GraphBfsOptions {
    /// Creates explicit breadth-first-search options. `None` bounds are
    /// unbounded; an edge-type list restricts every hop to those types.
    #[must_use]
    pub const fn new(
        max_depth: usize,
        max_nodes: Option<usize>,
        edge_types: Option<Vec<GraphEdgeType>>,
        direction: GraphDirection,
    ) -> Self {
        Self {
            max_depth,
            max_nodes,
            edge_types,
            direction,
        }
    }

    #[must_use]
    /// Returns the maximum traversal depth.
    pub const fn max_depth(&self) -> usize {
        self.max_depth
    }

    #[must_use]
    /// Returns the visited-node bound, when set.
    pub const fn max_nodes(&self) -> Option<usize> {
        self.max_nodes
    }

    #[must_use]
    /// Returns the edge-type restriction, when set.
    pub fn edge_types(&self) -> Option<&[GraphEdgeType]> {
        self.edge_types.as_deref()
    }

    #[must_use]
    /// Returns the traversal direction.
    pub const fn direction(&self) -> GraphDirection {
        self.direction
    }
}

impl Default for GraphBfsOptions {
    fn default() -> Self {
        Self::new(
            DEFAULT_BFS_MAX_DEPTH,
            Some(DEFAULT_BFS_MAX_NODES),
            None,
            GraphDirection::Outgoing,
        )
    }
}

/// One edge crossed by a traversal or kept in a subgraph.
///
/// `source` and `target` follow traversal order: for an incoming hop
/// the physical edge runs `target -> source`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GraphTraversalEdge {
    source: usize,
    target: usize,
    edge_type: usize,
    weight: f64,
}

impl GraphTraversalEdge {
    #[must_use]
    /// Returns the node index the traversal came from.
    pub const fn source(&self) -> usize {
        self.source
    }

    #[must_use]
    /// Returns the node index the traversal reached.
    pub const fn target(&self) -> usize {
        self.target
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

/// Result of a breadth-first search: nodes in visit order, per-node
/// depths, and the tree edges that discovered each node.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphBfsResult {
    visited: Vec<usize>,
    depths: Vec<Option<usize>>,
    edges: Vec<GraphTraversalEdge>,
    truncated: bool,
}

impl GraphBfsResult {
    #[must_use]
    /// Returns the visited node indexes in breadth-first order.
    pub fn visited(&self) -> &[usize] {
        &self.visited
    }

    #[must_use]
    /// Returns the depth per node index (`None` when not visited).
    pub fn depths(&self) -> &[Option<usize>] {
        &self.depths
    }

    #[must_use]
    /// Returns the depth of the node at `index` (`None` when not
    /// visited or out of range).
    pub fn depth(&self, index: usize) -> Option<usize> {
        self.depths.get(index).copied().flatten()
    }

    #[must_use]
    /// Returns the tree edges in discovery order.
    pub fn edges(&self) -> &[GraphTraversalEdge] {
        &self.edges
    }

    #[must_use]
    /// Whether a depth or node-count cap stopped the traversal before every
    /// reachable node was visited, so `visited`/`edges` are a partial result.
    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

/// A structural subgraph: the selected node indexes and every edge of
/// the snapshot whose endpoints are both selected.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphSubgraphResult {
    nodes: Vec<usize>,
    edges: Vec<GraphTraversalEdge>,
}

impl GraphSubgraphResult {
    #[must_use]
    /// Returns the selected node indexes, ascending.
    pub fn nodes(&self) -> &[usize] {
        &self.nodes
    }

    #[must_use]
    /// Returns the kept edges in (source, edge type, target) order.
    pub fn edges(&self) -> &[GraphTraversalEdge] {
        &self.edges
    }

    #[must_use]
    /// Returns the number of selected nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    /// Returns the number of kept edges.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

impl GraphAdjacencyIndex {
    /// Returns the degree of the node at `index` along `direction`
    /// (zero when out of range). A self-loop counts once per direction,
    /// so it contributes two under [`GraphDirection::Both`].
    #[must_use]
    pub fn degree(&self, index: usize, direction: GraphDirection) -> usize {
        match direction {
            GraphDirection::Outgoing => self.outgoing(index).len(),
            GraphDirection::Incoming => self.incoming(index).len(),
            GraphDirection::Both => self.outgoing(index).len() + self.incoming(index).len(),
        }
    }

    /// Runs a bounded breadth-first search from `start`.
    ///
    /// Visits nodes level by level along the requested direction,
    /// keeping the first discovering edge per node (tree edges only).
    /// `max_depth` stops expansion, `max_nodes` stops visiting, and an
    /// edge-type restriction applies at every hop. Refuses a start node
    /// missing from the snapshot with `not_found.engine.graph_node`.
    pub fn bfs(
        &self,
        start: &GraphNodeId,
        options: &GraphBfsOptions,
    ) -> Result<GraphBfsResult, EngineError> {
        let Some(start_index) = self.node_index(start) else {
            return Err(EngineError::not_found(
                "not_found.engine.graph_node",
                "graph node was not found in this snapshot",
            ));
        };
        // Resolve the type restriction to interned indexes once; a name
        // with no edges in this snapshot restricts to nothing.
        let type_filter: Option<HashSet<usize>> = options.edge_types().map(|types| {
            types
                .iter()
                .filter_map(|edge_type| self.edge_type_index(edge_type))
                .collect()
        });

        let mut visited: Vec<usize> = Vec::new();
        let mut depths: Vec<Option<usize>> = vec![None; self.node_count()];
        let mut edges: Vec<GraphTraversalEdge> = Vec::new();
        let mut seen = vec![false; self.node_count()];
        let mut queue: VecDeque<(usize, usize)> = VecDeque::new();
        let mut truncated = false;

        seen[start_index] = true;
        queue.push_back((start_index, 0));

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(max) = options.max_nodes() {
                if visited.len() >= max {
                    // A node remained to visit past the cap, so the result is a
                    // partial traversal.
                    truncated = true;
                    break;
                }
            }
            visited.push(current);
            depths[current] = Some(depth);
            if depth >= options.max_depth() {
                // Stopping at the depth cap leaves any not-yet-seen neighbor of
                // this node unvisited, so the result is partial.
                let has_unseen = |list: &[super::GraphAdjacencyEdge]| {
                    list.iter().any(|edge| {
                        type_filter
                            .as_ref()
                            .is_none_or(|filter| filter.contains(&edge.edge_type()))
                            && !seen[edge.neighbor()]
                    })
                };
                if match options.direction() {
                    GraphDirection::Outgoing => has_unseen(self.outgoing(current)),
                    GraphDirection::Incoming => has_unseen(self.incoming(current)),
                    GraphDirection::Both => {
                        has_unseen(self.outgoing(current)) || has_unseen(self.incoming(current))
                    }
                } {
                    truncated = true;
                }
                continue;
            }

            let mut expand = |list: &[super::GraphAdjacencyEdge]| {
                for edge in list {
                    if let Some(filter) = &type_filter {
                        if !filter.contains(&edge.edge_type()) {
                            continue;
                        }
                    }
                    if !seen[edge.neighbor()] {
                        seen[edge.neighbor()] = true;
                        edges.push(GraphTraversalEdge {
                            source: current,
                            target: edge.neighbor(),
                            edge_type: edge.edge_type(),
                            weight: edge.weight(),
                        });
                        queue.push_back((edge.neighbor(), depth + 1));
                    }
                }
            };
            match options.direction() {
                GraphDirection::Outgoing => expand(self.outgoing(current)),
                GraphDirection::Incoming => expand(self.incoming(current)),
                GraphDirection::Both => {
                    expand(self.outgoing(current));
                    expand(self.incoming(current));
                }
            }
        }

        // A node cap discovers a level's edges before it stops visiting, so an
        // edge can point at a node that was never visited; drop those so every
        // emitted edge connects two visited nodes.
        edges.retain(|edge| depths[edge.target].is_some());

        Ok(GraphBfsResult {
            visited,
            depths,
            edges,
            truncated,
        })
    }

    /// Extracts the subgraph induced by `nodes`: the selected nodes
    /// (unknown ids are skipped, duplicates collapse) plus every edge
    /// whose endpoints are both selected.
    #[must_use]
    pub fn subgraph(&self, nodes: &[GraphNodeId]) -> GraphSubgraphResult {
        let mut selected: Vec<usize> = nodes
            .iter()
            .filter_map(|node_id| self.node_index(node_id))
            .collect();
        selected.sort_unstable();
        selected.dedup();
        let mut member = vec![false; self.node_count()];
        for node in &selected {
            member[*node] = true;
        }

        let mut edges: Vec<GraphTraversalEdge> = Vec::new();
        for source in &selected {
            for edge in self.outgoing(*source) {
                if member[edge.neighbor()] {
                    edges.push(GraphTraversalEdge {
                        source: *source,
                        target: edge.neighbor(),
                        edge_type: edge.edge_type(),
                        weight: edge.weight(),
                    });
                }
            }
        }
        GraphSubgraphResult {
            nodes: selected,
            edges,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{GraphAdjacencyIndexBuilder, GraphAnalyticsBudget};
    use crate::data::graph::{GraphDirection, GraphEdgeType, GraphName, GraphNodeId};

    use super::{GraphAdjacencyIndex, GraphBfsOptions};

    fn node(value: &str) -> GraphNodeId {
        GraphNodeId::new(value).expect("node id")
    }

    fn edge_type(value: &str) -> GraphEdgeType {
        GraphEdgeType::new(value).expect("edge type")
    }

    fn make_index(edges: &[(&str, &str, &str)], extra_nodes: &[&str]) -> GraphAdjacencyIndex {
        let mut ids: Vec<&str> = extra_nodes.to_vec();
        for (src, _, dst) in edges {
            ids.push(src);
            ids.push(dst);
        }
        ids.sort_unstable();
        ids.dedup();
        let mut builder = GraphAdjacencyIndexBuilder::new(
            GraphName::new("g").expect("graph"),
            GraphAnalyticsBudget::default(),
        );
        for id in ids {
            builder.add_node(node(id)).expect("node fits");
        }
        builder.finish_nodes();
        for (src, kind, dst) in edges {
            builder
                .add_edge(&node(src), &edge_type(kind), &node(dst), 1.0)
                .expect("edge fits");
        }
        builder.finish()
    }

    fn at(index: &GraphAdjacencyIndex, id: &str) -> usize {
        index.node_index(&node(id)).expect("node present")
    }

    fn bfs_options(max_depth: usize) -> GraphBfsOptions {
        GraphBfsOptions::new(max_depth, None, None, GraphDirection::Outgoing)
    }

    fn visited_ids<'a>(
        index: &'a GraphAdjacencyIndex,
        result: &super::GraphBfsResult,
    ) -> Vec<&'a str> {
        result
            .visited()
            .iter()
            .map(|position| index.node_id(*position).expect("id resolves").as_str())
            .collect()
    }

    #[test]
    fn degree_counts_each_direction() {
        let index = make_index(&[("A", "e", "B"), ("A", "e", "C")], &[]);
        let a = at(&index, "A");
        assert_eq!(index.degree(a, GraphDirection::Outgoing), 2);
        assert_eq!(index.degree(a, GraphDirection::Incoming), 0);
        assert_eq!(index.degree(a, GraphDirection::Both), 2);
        let b = at(&index, "B");
        assert_eq!(index.degree(b, GraphDirection::Both), 1);
    }

    #[test]
    fn degree_zero_without_edges_and_out_of_range() {
        let index = make_index(&[], &["A"]);
        assert_eq!(index.degree(at(&index, "A"), GraphDirection::Both), 0);
        assert_eq!(index.degree(999, GraphDirection::Both), 0);
    }

    #[test]
    fn degree_self_loop_counts_once_per_direction() {
        let index = make_index(&[("A", "self", "A")], &[]);
        let a = at(&index, "A");
        assert_eq!(index.degree(a, GraphDirection::Outgoing), 1);
        assert_eq!(index.degree(a, GraphDirection::Incoming), 1);
        assert_eq!(index.degree(a, GraphDirection::Both), 2);
    }

    #[test]
    fn bfs_records_depths_along_a_chain() {
        let index = make_index(&[("A", "e", "B"), ("B", "e", "C")], &[]);
        let result = index.bfs(&node("A"), &bfs_options(10)).expect("bfs runs");
        assert_eq!(visited_ids(&index, &result), vec!["A", "B", "C"]);
        assert_eq!(result.depth(at(&index, "A")), Some(0));
        assert_eq!(result.depth(at(&index, "B")), Some(1));
        assert_eq!(result.depth(at(&index, "C")), Some(2));
    }

    #[test]
    fn bfs_visits_level_by_level() {
        let index = make_index(
            &[
                ("A", "e", "B"),
                ("A", "e", "C"),
                ("B", "e", "D"),
                ("C", "e", "E"),
            ],
            &[],
        );
        let result = index.bfs(&node("A"), &bfs_options(10)).expect("bfs runs");
        let order = visited_ids(&index, &result);
        assert_eq!(order[0], "A");
        let position = |id: &str| {
            order
                .iter()
                .position(|entry| *entry == id)
                .expect("visited")
        };
        for shallow in ["B", "C"] {
            for deep in ["D", "E"] {
                assert!(position(shallow) < position(deep));
            }
        }
    }

    #[test]
    fn bfs_diamond_visits_the_join_once() {
        let index = make_index(
            &[
                ("A", "e", "B"),
                ("A", "e", "C"),
                ("B", "e", "D"),
                ("C", "e", "D"),
            ],
            &[],
        );
        let result = index.bfs(&node("A"), &bfs_options(2)).expect("bfs runs");
        assert_eq!(result.visited().len(), 4);
        let d = at(&index, "D");
        assert_eq!(
            result
                .visited()
                .iter()
                .filter(|position| **position == d)
                .count(),
            1
        );
        // Tree edges only: 3 discoveries for 4 nodes.
        assert_eq!(result.edges().len(), 3);
    }

    #[test]
    fn bfs_terminates_on_cycles() {
        let index = make_index(&[("A", "e", "B"), ("B", "e", "C"), ("C", "e", "A")], &[]);
        let result = index.bfs(&node("A"), &bfs_options(10)).expect("bfs runs");
        assert_eq!(result.visited().len(), 3);
    }

    #[test]
    fn bfs_self_loop_does_not_double_visit() {
        let index = make_index(&[("A", "e", "A"), ("A", "e", "B")], &[]);
        let result = index.bfs(&node("A"), &bfs_options(10)).expect("bfs runs");
        assert_eq!(visited_ids(&index, &result), vec!["A", "B"]);
    }

    #[test]
    fn bfs_depth_zero_visits_only_the_start() {
        let index = make_index(&[("A", "e", "B")], &[]);
        let result = index.bfs(&node("A"), &bfs_options(0)).expect("bfs runs");
        assert_eq!(visited_ids(&index, &result), vec!["A"]);
        assert!(result.edges().is_empty());
    }

    #[test]
    fn bfs_max_nodes_truncates_the_visit() {
        let index = make_index(
            &[
                ("A", "e", "B"),
                ("A", "e", "C"),
                ("A", "e", "D"),
                ("A", "e", "E"),
            ],
            &[],
        );
        let options = GraphBfsOptions::new(10, Some(3), None, GraphDirection::Outgoing);
        let result = index.bfs(&node("A"), &options).expect("bfs runs");
        assert_eq!(result.visited().len(), 3);

        let only_start = GraphBfsOptions::new(10, Some(1), None, GraphDirection::Outgoing);
        let result = index.bfs(&node("A"), &only_start).expect("bfs runs");
        assert_eq!(visited_ids(&index, &result), vec!["A"]);
    }

    #[test]
    fn bfs_edge_type_filter_applies_at_every_hop() {
        let index = make_index(
            &[
                ("A", "knows", "B"),
                ("B", "hates", "C"),
                ("B", "knows", "D"),
            ],
            &[],
        );
        let options = GraphBfsOptions::new(
            10,
            None,
            Some(vec![edge_type("knows")]),
            GraphDirection::Outgoing,
        );
        let result = index.bfs(&node("A"), &options).expect("bfs runs");
        let order = visited_ids(&index, &result);
        assert!(order.contains(&"D"));
        assert!(!order.contains(&"C"), "filter blocks the second hop");

        // A type with no edges in the snapshot restricts to nothing.
        let unknown = GraphBfsOptions::new(
            10,
            None,
            Some(vec![edge_type("ghost")]),
            GraphDirection::Outgoing,
        );
        let result = index.bfs(&node("A"), &unknown).expect("bfs runs");
        assert_eq!(visited_ids(&index, &result), vec!["A"]);
    }

    #[test]
    fn bfs_follows_direction() {
        let index = make_index(&[("A", "e", "B"), ("B", "e", "C"), ("C", "e", "D")], &[]);
        let incoming = GraphBfsOptions::new(10, None, None, GraphDirection::Incoming);
        let result = index.bfs(&node("D"), &incoming).expect("bfs runs");
        assert_eq!(result.visited().len(), 4);
        assert_eq!(result.depth(at(&index, "A")), Some(3));

        // Both directions reach outgoing and incoming neighbors alike,
        // and the traversal edge records the hop, not the physical edge.
        let index = make_index(&[("A", "e", "B"), ("C", "e", "A")], &[]);
        let both = GraphBfsOptions::new(1, None, None, GraphDirection::Both);
        let result = index.bfs(&node("A"), &both).expect("bfs runs");
        assert_eq!(result.visited().len(), 3);
        let hop_to_c = result
            .edges()
            .iter()
            .find(|edge| edge.target() == at(&index, "C"))
            .expect("C discovered");
        assert_eq!(hop_to_c.source(), at(&index, "A"));
    }

    #[test]
    fn bfs_missing_start_refuses() {
        let index = make_index(&[("A", "e", "B")], &[]);
        let error = index
            .bfs(&node("ghost"), &bfs_options(10))
            .expect_err("missing start");
        assert_eq!(error.code(), "not_found.engine.graph_node");
    }

    #[test]
    fn subgraph_keeps_edges_within_the_selection() {
        let index = make_index(&[("A", "e", "B"), ("B", "e", "C")], &[]);
        let full = index.subgraph(&[node("A"), node("B"), node("C")]);
        assert_eq!(full.node_count(), 3);
        assert_eq!(full.edge_count(), 2);

        let partial = index.subgraph(&[node("A"), node("B")]);
        assert_eq!(partial.node_count(), 2);
        assert_eq!(partial.edge_count(), 1);
        assert_eq!(partial.edges()[0].source(), at(&index, "A"));
        assert_eq!(partial.edges()[0].target(), at(&index, "B"));

        let single = index.subgraph(&[node("A")]);
        assert_eq!(single.node_count(), 1);
        assert_eq!(single.edge_count(), 0);
    }

    #[test]
    fn subgraph_handles_empty_disconnected_and_unknown_selections() {
        let index = make_index(&[("A", "e", "B")], &["C"]);
        assert_eq!(index.subgraph(&[]).node_count(), 0);

        let disconnected = index.subgraph(&[node("A"), node("C")]);
        assert_eq!(disconnected.node_count(), 2);
        assert_eq!(disconnected.edge_count(), 0);

        let with_ghost = index.subgraph(&[node("A"), node("ghost"), node("A")]);
        assert_eq!(
            with_ghost.node_count(),
            1,
            "unknown skipped, duplicate collapsed"
        );
    }

    #[test]
    fn subgraph_keeps_self_loops_and_is_deterministic() {
        let index = make_index(&[("A", "self", "A"), ("A", "e", "B")], &[]);
        let subgraph = index.subgraph(&[node("B"), node("A")]);
        assert_eq!(subgraph.nodes(), &[at(&index, "A"), at(&index, "B")]);
        assert_eq!(subgraph.edge_count(), 2);
        let again = index.subgraph(&[node("A"), node("B")]);
        assert_eq!(again, subgraph);
    }
}
