//! Exact graph analytics (GA2): weakly connected components, local
//! clustering coefficients, and single-source shortest paths.
//!
//! Every algorithm is a pure method on [`GraphAdjacencyIndex`], so one
//! snapshot built by `GraphService::adjacency_index` (at any read
//! selector) serves any number of runs, and historical analytics need no
//! extra machinery. Results are keyed by node index — positions in
//! [`GraphAdjacencyIndex::node_ids`] — and are deterministic because the
//! index itself is.
//!
//! Semantics are ported from the v0.6 engine: components and clustering
//! treat the graph as undirected, self-loops never contribute to
//! clustering, and shortest paths follow the caller's edge direction.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use crate::diagnostics::EngineError;

use super::{GraphAdjacencyIndex, GraphDirection, GraphNodeId};

/// Weakly connected components of one snapshot.
///
/// Each node maps to a component label: the smallest node index in its
/// component. Labels are therefore canonical for a given visible state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphWccResult {
    components: Vec<usize>,
    component_count: usize,
}

impl GraphWccResult {
    #[must_use]
    /// Returns the component label per node index.
    pub fn components(&self) -> &[usize] {
        &self.components
    }

    #[must_use]
    /// Returns the component label of the node at `index`, when in range.
    pub fn component(&self, index: usize) -> Option<usize> {
        self.components.get(index).copied()
    }

    #[must_use]
    /// Returns the number of distinct components.
    pub const fn component_count(&self) -> usize {
        self.component_count
    }
}

/// Local clustering coefficients of one snapshot.
///
/// The coefficient of a node is the fraction of its undirected neighbor
/// pairs that are themselves connected; nodes with fewer than two
/// neighbors score `0.0`. Self-loops are ignored throughout.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphLccResult {
    coefficients: Vec<f64>,
}

impl GraphLccResult {
    #[must_use]
    /// Returns the clustering coefficient per node index.
    pub fn coefficients(&self) -> &[f64] {
        &self.coefficients
    }

    #[must_use]
    /// Returns the coefficient of the node at `index`, when in range.
    pub fn coefficient(&self, index: usize) -> Option<f64> {
        self.coefficients.get(index).copied()
    }
}

/// Shortest-path distances from one source node.
///
/// Distances are keyed by node index; `None` marks a node the source
/// cannot reach under the requested direction.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphSsspResult {
    source: usize,
    distances: Vec<Option<f64>>,
}

impl GraphSsspResult {
    #[must_use]
    /// Returns the source node index.
    pub const fn source(&self) -> usize {
        self.source
    }

    #[must_use]
    /// Returns the distance per node index (`None` when unreachable).
    pub fn distances(&self) -> &[Option<f64>] {
        &self.distances
    }

    #[must_use]
    /// Returns the distance of the node at `index` (`None` when
    /// unreachable or out of range).
    pub fn distance(&self, index: usize) -> Option<f64> {
        self.distances.get(index).copied().flatten()
    }

    #[must_use]
    /// Returns the number of reachable nodes, the source included.
    pub fn reachable_count(&self) -> usize {
        self.distances.iter().flatten().count()
    }
}

/// Total order on distances for the Dijkstra frontier. Weights are
/// finite by [`super::GraphEdgeData`] validation and non-negative by the
/// precondition check, so `partial_cmp` cannot see NaN.
#[derive(Clone, Copy, Debug, PartialEq)]
struct FrontierDistance(f64);

impl Eq for FrontierDistance {}

impl PartialOrd for FrontierDistance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FrontierDistance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Union-find root lookup with path halving: iterative, so budget-sized
/// graphs cannot exhaust the stack.
fn find_root(parent: &mut [usize], mut node: usize) -> usize {
    while parent[node] != node {
        parent[node] = parent[parent[node]];
        node = parent[node];
    }
    node
}

impl GraphAdjacencyIndex {
    /// Computes weakly connected components: connectivity over the
    /// undirected view of the snapshot, so every edge joins its
    /// endpoints regardless of direction. Isolated nodes form their own
    /// components.
    #[must_use]
    pub fn wcc(&self) -> GraphWccResult {
        let node_count = self.node_count();
        let mut parent: Vec<usize> = (0..node_count).collect();

        // Every edge appears exactly once in the outgoing lists.
        for source in 0..node_count {
            for edge in self.outgoing(source) {
                let left = find_root(&mut parent, source);
                let right = find_root(&mut parent, edge.neighbor());
                if left != right {
                    // Union by smaller index: the root is then the
                    // canonical (smallest) member, making labels stable.
                    let (small, large) = if left < right {
                        (left, right)
                    } else {
                        (right, left)
                    };
                    parent[large] = small;
                }
            }
        }

        let mut components = vec![0usize; node_count];
        let mut roots = HashSet::new();
        for (node, label) in components.iter_mut().enumerate() {
            let root = find_root(&mut parent, node);
            *label = root;
            roots.insert(root);
        }
        GraphWccResult {
            components,
            component_count: roots.len(),
        }
    }

    /// Computes the local clustering coefficient of every node over the
    /// undirected view of the snapshot, ignoring self-loops.
    #[must_use]
    pub fn lcc(&self) -> GraphLccResult {
        let node_count = self.node_count();
        // Undirected neighbor sets, self-loops excluded; doubles as the
        // O(1) connectivity probe for the pair loop below.
        let mut neighbors: Vec<HashSet<usize>> = vec![HashSet::new(); node_count];
        for (node, adjacent) in neighbors.iter_mut().enumerate() {
            for edge in self.outgoing(node).iter().chain(self.incoming(node)) {
                if edge.neighbor() != node {
                    adjacent.insert(edge.neighbor());
                }
            }
        }

        let coefficients = (0..node_count)
            .map(|node| {
                let degree = neighbors[node].len();
                if degree < 2 {
                    return 0.0;
                }
                let members: Vec<usize> = neighbors[node].iter().copied().collect();
                let mut connected_pairs = 0u64;
                for (position, left) in members.iter().enumerate() {
                    for right in &members[position + 1..] {
                        if neighbors[*left].contains(right) {
                            connected_pairs += 1;
                        }
                    }
                }
                let possible_pairs = (degree * (degree - 1)) / 2;
                #[allow(clippy::cast_precision_loss)]
                let coefficient = connected_pairs as f64 / possible_pairs as f64;
                coefficient
            })
            .collect();
        GraphLccResult { coefficients }
    }

    /// Computes shortest-path distances from `source` along `direction`
    /// using Dijkstra's algorithm.
    ///
    /// Refuses with `not_found.engine.graph_node` when the source is not
    /// in the snapshot, and with
    /// `failed_precondition.engine.graph_negative_weight` when any edge
    /// carries a negative weight (shortest distances are undefined
    /// there, and the search could fail to terminate).
    pub fn sssp(
        &self,
        source: &GraphNodeId,
        direction: GraphDirection,
    ) -> Result<GraphSsspResult, EngineError> {
        let Some(source_index) = self.node_index(source) else {
            return Err(EngineError::not_found(
                "not_found.engine.graph_node",
                "graph node was not found in this snapshot",
            ));
        };
        for node in 0..self.node_count() {
            if self.outgoing(node).iter().any(|edge| edge.weight() < 0.0) {
                return Err(EngineError::conflict(
                    "failed_precondition.engine.graph_negative_weight",
                    "shortest-path distances require non-negative edge weights",
                ));
            }
        }

        let mut distances: Vec<Option<f64>> = vec![None; self.node_count()];
        distances[source_index] = Some(0.0);
        let mut frontier = BinaryHeap::new();
        frontier.push(Reverse((FrontierDistance(0.0), source_index)));

        while let Some(Reverse((FrontierDistance(distance), node))) = frontier.pop() {
            match distances[node] {
                Some(known) if distance > known => continue,
                _ => {}
            }
            let mut relax = |neighbor: usize, weight: f64| {
                let candidate = distance + weight;
                if distances[neighbor].is_none_or(|current| candidate < current) {
                    distances[neighbor] = Some(candidate);
                    frontier.push(Reverse((FrontierDistance(candidate), neighbor)));
                }
            };
            match direction {
                GraphDirection::Outgoing => {
                    for edge in self.outgoing(node) {
                        relax(edge.neighbor(), edge.weight());
                    }
                }
                GraphDirection::Incoming => {
                    for edge in self.incoming(node) {
                        relax(edge.neighbor(), edge.weight());
                    }
                }
                GraphDirection::Both => {
                    for edge in self.outgoing(node).iter().chain(self.incoming(node)) {
                        relax(edge.neighbor(), edge.weight());
                    }
                }
            }
        }

        Ok(GraphSsspResult {
            source: source_index,
            distances,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::{GraphAdjacencyIndexBuilder, GraphAnalyticsBudget};
    use crate::data::graph::{GraphDirection, GraphEdgeType, GraphName, GraphNodeId};

    use super::GraphAdjacencyIndex;

    fn node(value: &str) -> GraphNodeId {
        GraphNodeId::new(value).expect("node id")
    }

    /// Builds an index from weighted edges plus explicit extra nodes.
    fn make_index(edges: &[(&str, &str, f64)], extra_nodes: &[&str]) -> GraphAdjacencyIndex {
        let mut ids: Vec<&str> = extra_nodes.to_vec();
        for (src, dst, _) in edges {
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
        let edge_type = GraphEdgeType::new("e").expect("edge type");
        for (src, dst, weight) in edges {
            builder
                .add_edge(&node(src), &edge_type, &node(dst), *weight)
                .expect("edge fits");
        }
        builder.finish()
    }

    fn make_unweighted_index(edges: &[(&str, &str)], extra_nodes: &[&str]) -> GraphAdjacencyIndex {
        let weighted: Vec<(&str, &str, f64)> =
            edges.iter().map(|(src, dst)| (*src, *dst, 1.0)).collect();
        make_index(&weighted, extra_nodes)
    }

    fn component_of(index: &GraphAdjacencyIndex, wcc: &super::GraphWccResult, id: &str) -> usize {
        wcc.component(index.node_index(&node(id)).expect("node present"))
            .expect("component present")
    }

    fn coefficient_of(index: &GraphAdjacencyIndex, lcc: &super::GraphLccResult, id: &str) -> f64 {
        lcc.coefficient(index.node_index(&node(id)).expect("node present"))
            .expect("coefficient present")
    }

    fn distance_of(
        index: &GraphAdjacencyIndex,
        sssp: &super::GraphSsspResult,
        id: &str,
    ) -> Option<f64> {
        sssp.distance(index.node_index(&node(id)).expect("node present"))
    }

    #[test]
    fn wcc_disconnected_components() {
        let index = make_unweighted_index(&[("A", "B"), ("C", "D")], &[]);
        let wcc = index.wcc();
        assert_eq!(
            component_of(&index, &wcc, "A"),
            component_of(&index, &wcc, "B")
        );
        assert_eq!(
            component_of(&index, &wcc, "C"),
            component_of(&index, &wcc, "D")
        );
        assert_ne!(
            component_of(&index, &wcc, "A"),
            component_of(&index, &wcc, "C")
        );
        assert_eq!(wcc.component_count(), 2);
    }

    #[test]
    fn wcc_single_component_ignores_direction() {
        let index = make_unweighted_index(&[("A", "B"), ("C", "B"), ("C", "D")], &[]);
        let wcc = index.wcc();
        let label = component_of(&index, &wcc, "A");
        for id in ["B", "C", "D"] {
            assert_eq!(component_of(&index, &wcc, id), label);
        }
        assert_eq!(wcc.component_count(), 1);
        // The label is the smallest member index — "A" is index 0.
        assert_eq!(label, 0);
    }

    #[test]
    fn wcc_isolated_nodes_are_own_components() {
        let index = make_unweighted_index(&[], &["A", "B", "C"]);
        let wcc = index.wcc();
        assert_eq!(wcc.component_count(), 3);
        assert_eq!(wcc.components(), &[0, 1, 2]);
    }

    #[test]
    fn wcc_self_loop_stays_isolated() {
        let index = make_unweighted_index(&[("A", "A"), ("B", "C")], &[]);
        let wcc = index.wcc();
        assert_ne!(
            component_of(&index, &wcc, "A"),
            component_of(&index, &wcc, "B")
        );
        assert_eq!(
            component_of(&index, &wcc, "B"),
            component_of(&index, &wcc, "C")
        );
    }

    #[test]
    fn wcc_empty_graph() {
        let index = make_unweighted_index(&[], &[]);
        let wcc = index.wcc();
        assert!(wcc.components().is_empty());
        assert_eq!(wcc.component_count(), 0);
    }

    #[test]
    fn lcc_triangle_scores_one() {
        let index = make_unweighted_index(&[("A", "B"), ("B", "C"), ("C", "A")], &[]);
        let lcc = index.lcc();
        for id in ["A", "B", "C"] {
            assert!((coefficient_of(&index, &lcc, id) - 1.0).abs() < 1e-10);
        }
    }

    #[test]
    fn lcc_star_scores_zero() {
        let index = make_unweighted_index(&[("A", "B"), ("A", "C"), ("A", "D")], &[]);
        let lcc = index.lcc();
        assert!((coefficient_of(&index, &lcc, "A")).abs() < 1e-10);
        assert!((coefficient_of(&index, &lcc, "B")).abs() < 1e-10);
    }

    #[test]
    fn lcc_square_has_no_triangles() {
        let index = make_unweighted_index(&[("A", "B"), ("B", "C"), ("C", "D"), ("D", "A")], &[]);
        let lcc = index.lcc();
        assert!((coefficient_of(&index, &lcc, "A")).abs() < 1e-10);
    }

    #[test]
    fn lcc_ignores_self_loops() {
        let index = make_unweighted_index(&[("A", "A"), ("A", "B"), ("A", "C"), ("B", "C")], &[]);
        let lcc = index.lcc();
        // A's neighbors excluding itself: {B, C}; B–C exists → 1.0.
        assert!((coefficient_of(&index, &lcc, "A") - 1.0).abs() < 1e-10);
    }

    #[test]
    fn lcc_isolated_nodes_score_zero() {
        let index = make_unweighted_index(&[], &["A", "B"]);
        let lcc = index.lcc();
        assert!((coefficient_of(&index, &lcc, "A")).abs() < 1e-10);
        assert!((coefficient_of(&index, &lcc, "B")).abs() < 1e-10);
    }

    #[test]
    fn sssp_prefers_cheaper_multi_hop_path() {
        let index = make_index(&[("A", "B", 1.0), ("B", "C", 2.0), ("A", "C", 10.0)], &[]);
        let sssp = index
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(distance_of(&index, &sssp, "A"), Some(0.0));
        assert_eq!(distance_of(&index, &sssp, "B"), Some(1.0));
        assert_eq!(distance_of(&index, &sssp, "C"), Some(3.0));
    }

    #[test]
    fn sssp_marks_unreachable_nodes() {
        let index = make_unweighted_index(&[("A", "B")], &["C"]);
        let sssp = index
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(distance_of(&index, &sssp, "C"), None);
        assert_eq!(sssp.reachable_count(), 2);
    }

    #[test]
    fn sssp_zero_weight_edges_propagate() {
        let index = make_index(&[("A", "B", 0.0), ("B", "C", 0.0)], &[]);
        let sssp = index
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(distance_of(&index, &sssp, "C"), Some(0.0));
    }

    #[test]
    fn sssp_incoming_and_both_follow_reverse_edges() {
        let index = make_index(&[("A", "B", 1.0), ("B", "C", 2.0)], &[]);
        for direction in [GraphDirection::Incoming, GraphDirection::Both] {
            let sssp = index.sssp(&node("C"), direction).expect("sssp runs");
            assert_eq!(distance_of(&index, &sssp, "C"), Some(0.0));
            assert_eq!(distance_of(&index, &sssp, "B"), Some(2.0));
            assert_eq!(distance_of(&index, &sssp, "A"), Some(3.0));
        }
        // Outgoing from C reaches nothing else.
        let sssp = index
            .sssp(&node("C"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(sssp.reachable_count(), 1);
    }

    #[test]
    fn sssp_missing_source_refuses() {
        let index = make_unweighted_index(&[("A", "B")], &[]);
        let error = index
            .sssp(&node("Z"), GraphDirection::Outgoing)
            .expect_err("missing source");
        assert_eq!(error.code(), "not_found.engine.graph_node");
    }

    #[test]
    fn sssp_negative_weight_refuses() {
        let index = make_index(&[("A", "B", 1.0), ("B", "C", -0.5)], &[]);
        let error = index
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect_err("negative weight");
        assert_eq!(
            error.code(),
            "failed_precondition.engine.graph_negative_weight"
        );
    }
}
