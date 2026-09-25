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

/// Shortest-path distances and predecessors from one source node.
///
/// Distances are keyed by node index; `None` marks a node the source
/// cannot reach under the requested direction. Each reachable node also
/// records the node its cheapest route arrived from, so the route itself
/// can be unpacked ([`Self::path_to`], #3456).
#[derive(Clone, Debug, PartialEq)]
pub struct GraphSsspResult {
    source: usize,
    distances: Vec<Option<f64>>,
    predecessors: Vec<Option<usize>>,
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
    /// Returns the distance of the node at `index` as an exact count. Over
    /// count weights ([`super::GraphEdgeData::from_count`]) every distance
    /// up to [`super::GraphEdgeData::MAX_COUNT`] is exact — each partial sum along
    /// the walk is smaller still — so this is `Some` exactly when the
    /// distance can be trusted as an integer: `None` when unreachable, out
    /// of range, fractional, or beyond the exact range (#3465).
    pub fn distance_count(&self, index: usize) -> Option<u64> {
        self.distance(index).and_then(super::types::exact_count)
    }

    #[must_use]
    /// Returns the number of reachable nodes, the source included.
    pub fn reachable_count(&self) -> usize {
        self.distances.iter().flatten().count()
    }

    #[must_use]
    /// Returns the predecessor per node index — the node the cheapest route
    /// arrived from; `None` for the source and for unreachable nodes.
    pub fn predecessors(&self) -> &[Option<usize>] {
        &self.predecessors
    }

    #[must_use]
    /// Returns the predecessor of the node at `index` (`None` for the
    /// source, an unreachable node, or an index out of range).
    pub fn predecessor(&self, index: usize) -> Option<usize> {
        self.predecessors.get(index).copied().flatten()
    }

    #[must_use]
    /// Returns the cheapest route to the node at `index` as node indexes,
    /// source first — `Some(vec![source])` for the source itself, `None`
    /// when the node is unreachable or out of range.
    ///
    /// Ties resolve to the route discovered first: the frontier pops by
    /// (distance, node index), edges relax in (edge type, neighbor) order,
    /// and an equal-cost alternative never replaces a recorded predecessor.
    /// Under [`GraphDirection::Both`] a step may run against an edge's
    /// stored direction; the route is a node sequence, not a drive.
    pub fn path_to(&self, index: usize) -> Option<Vec<usize>> {
        self.distance(index)?;
        let mut path = vec![index];
        let mut at = index;
        // The chain is a tree rooted at the source — a link is recorded only
        // when a distance strictly drops — so it has fewer steps than there
        // are nodes. The bound keeps the walk finite whatever the chain holds.
        for _ in 0..self.predecessors.len() {
            let Some(previous) = self.predecessor(at) else {
                break;
            };
            path.push(previous);
            at = previous;
        }
        path.reverse();
        Some(path)
    }
}

/// Options for a single-source shortest-path run (#3471).
///
/// Mirrors [`super::GraphBfsOptions`]: an edge-type list restricts every
/// relaxation to those types, and `None` walks every type. A type this
/// snapshot never saw restricts to nothing — it is not an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphSsspOptions {
    direction: GraphDirection,
    edge_types: Option<Vec<super::GraphEdgeType>>,
}

impl GraphSsspOptions {
    /// Creates explicit options: the direction to walk and, when given, the
    /// edge types the walk may use.
    #[must_use]
    pub const fn new(
        direction: GraphDirection,
        edge_types: Option<Vec<super::GraphEdgeType>>,
    ) -> Self {
        Self {
            direction,
            edge_types,
        }
    }

    #[must_use]
    /// Returns the traversal direction.
    pub const fn direction(&self) -> GraphDirection {
        self.direction
    }

    #[must_use]
    /// Returns the edge-type restriction, when set.
    pub fn edge_types(&self) -> Option<&[super::GraphEdgeType]> {
        self.edge_types.as_deref()
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
    /// over every edge type — [`Self::sssp_with`] with no type restriction.
    pub fn sssp(
        &self,
        source: &GraphNodeId,
        direction: GraphDirection,
    ) -> Result<GraphSsspResult, EngineError> {
        self.sssp_with(source, &GraphSsspOptions::new(direction, None))
    }

    /// Computes shortest-path distances and predecessors from `source`
    /// under `options`, using Dijkstra's algorithm.
    ///
    /// Refuses with `not_found.engine.graph_node` when the source is not
    /// in the snapshot, and with
    /// `failed_precondition.engine.graph_negative_weight` when an edge the
    /// walk may use carries a negative weight — one of the selected types,
    /// or any type when unrestricted (shortest distances are undefined
    /// there, and the search could fail to terminate). A negative edge of
    /// a type the options exclude does not refuse (#3471).
    pub fn sssp_with(
        &self,
        source: &GraphNodeId,
        options: &GraphSsspOptions,
    ) -> Result<GraphSsspResult, EngineError> {
        let Some(source_index) = self.node_index(source) else {
            return Err(EngineError::not_found(
                "not_found.engine.graph_node",
                "graph node was not found in this snapshot",
            ));
        };
        // Resolve the type restriction to interned indexes once; a name
        // with no edges in this snapshot restricts to nothing.
        let type_filter: Option<std::collections::HashSet<usize>> =
            options.edge_types().map(|types| {
                types
                    .iter()
                    .filter_map(|edge_type| self.edge_type_index(edge_type))
                    .collect()
            });
        // #3460: the negative-weight verdict is recorded per edge type at
        // build time, so refusing is a set probe rather than an O(E) re-scan
        // on every query — and it covers only the types this walk may use.
        // The snapshot is immutable: it cannot gain a negative edge without
        // being rebuilt.
        let negative_selected = match &type_filter {
            None => self.has_negative_weight(),
            Some(filter) => filter
                .iter()
                .any(|edge_type| self.edge_type_has_negative_weight(*edge_type)),
        };
        if negative_selected {
            return Err(EngineError::conflict(
                "failed_precondition.engine.graph_negative_weight",
                "shortest-path distances require non-negative edge weights",
            ));
        }

        let mut distances: Vec<Option<f64>> = vec![None; self.node_count()];
        distances[source_index] = Some(0.0);
        let mut predecessors: Vec<Option<usize>> = vec![None; self.node_count()];
        let mut settled = vec![false; self.node_count()];
        let mut frontier = BinaryHeap::new();
        frontier.push(Reverse((FrontierDistance(0.0), source_index)));

        while let Some(Reverse((FrontierDistance(distance), node))) = frontier.pop() {
            // A node settles on its first pop, which carries its final
            // distance under non-negative weights; every later entry for it
            // is stale. Settling also bounds the walk to one relaxation per
            // edge, so it ends whatever the weights do.
            if std::mem::replace(&mut settled[node], true) {
                continue;
            }
            let mut relax = |edge: &super::GraphAdjacencyEdge| {
                if type_filter
                    .as_ref()
                    .is_some_and(|filter| !filter.contains(&edge.edge_type()))
                {
                    return;
                }
                let candidate = distance + edge.weight();
                if distances[edge.neighbor()].is_none_or(|current| candidate < current) {
                    distances[edge.neighbor()] = Some(candidate);
                    predecessors[edge.neighbor()] = Some(node);
                    frontier.push(Reverse((FrontierDistance(candidate), edge.neighbor())));
                }
            };
            match options.direction() {
                GraphDirection::Outgoing => {
                    for edge in self.outgoing(node) {
                        relax(edge);
                    }
                }
                GraphDirection::Incoming => {
                    for edge in self.incoming(node) {
                        relax(edge);
                    }
                }
                GraphDirection::Both => {
                    for edge in self.outgoing(node).iter().chain(self.incoming(node)) {
                        relax(edge);
                    }
                }
            }
        }

        Ok(GraphSsspResult {
            source: source_index,
            distances,
            predecessors,
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

    /// Builds an index from `(src, edge type, dst, weight)` edges — the
    /// heterogeneous shape the edge-type filter exists for (#3471).
    fn make_typed_index(edges: &[(&str, &str, &str, f64)]) -> GraphAdjacencyIndex {
        let mut ids: Vec<&str> = Vec::new();
        for (src, _, dst, _) in edges {
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
        for (src, kind, dst, weight) in edges {
            builder
                .add_edge(
                    &node(src),
                    &GraphEdgeType::new(*kind).expect("edge type"),
                    &node(dst),
                    *weight,
                )
                .expect("edge fits");
        }
        builder.finish()
    }

    /// Builds the `edge_types` argument, which is `Option` at every call site.
    #[allow(clippy::unnecessary_wraps)]
    fn types(names: &[&str]) -> Option<Vec<GraphEdgeType>> {
        Some(
            names
                .iter()
                .map(|name| GraphEdgeType::new(*name).expect("edge type"))
                .collect(),
        )
    }

    /// The node ids along `path_to(id)`, source first.
    fn path_of(
        index: &GraphAdjacencyIndex,
        sssp: &super::GraphSsspResult,
        id: &str,
    ) -> Option<Vec<String>> {
        sssp.path_to(index.node_index(&node(id)).expect("node present"))
            .map(|path| {
                path.iter()
                    .map(|step| {
                        index
                            .node_id(*step)
                            .expect("index in range")
                            .as_str()
                            .to_owned()
                    })
                    .collect()
            })
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

    #[test]
    fn has_negative_weight_is_recorded_at_build_time() {
        // #3460: the bit is set iff SOME edge is negative, recorded once at
        // build time. A zero weight is non-negative, so a clean graph keeps the
        // bit clear and sssp runs.
        let clean = make_index(&[("A", "B", 1.0), ("B", "C", 0.0)], &[]);
        assert!(!clean.has_negative_weight());
        clean
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect("a non-negative snapshot runs sssp");

        // Negatives that are NOT the last edge added still set the bit, and two
        // of them must not cancel — this guards the accumulation itself (`|=`),
        // not just the final edge's sign. sssp then refuses off the bit.
        let negative = make_index(&[("A", "B", -0.5), ("A", "C", -2.0), ("B", "C", 1.0)], &[]);
        assert!(negative.has_negative_weight());
        assert_eq!(
            negative
                .sssp(&node("A"), GraphDirection::Outgoing)
                .expect_err("a negative snapshot refuses sssp")
                .code(),
            "failed_precondition.engine.graph_negative_weight"
        );
    }

    // --- predecessors and paths (#3456) ---------------------------------

    /// The cheaper two-hop route unpacks to its node list — not just the
    /// cheaper scalar. The source's path is itself; an unreachable node has
    /// no path and no predecessor.
    #[test]
    fn sssp_path_to_unpacks_the_cheaper_two_hop_route() {
        let index = make_index(
            &[("A", "B", 1.0), ("B", "C", 2.0), ("A", "C", 10.0)],
            &["Z"],
        );
        let sssp = index
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(
            path_of(&index, &sssp, "C"),
            Some(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()])
        );
        assert_eq!(path_of(&index, &sssp, "A"), Some(vec!["A".to_owned()]));
        assert_eq!(
            sssp.predecessor(index.node_index(&node("A")).expect("A")),
            None
        );
        assert_eq!(path_of(&index, &sssp, "Z"), None);
        assert_eq!(
            sssp.predecessor(index.node_index(&node("Z")).expect("Z")),
            None
        );
        assert_eq!(sssp.predecessors().len(), index.node_count());
        // An index past the node set has no distance, no predecessor, and no
        // path — the accessors bound-check rather than panic.
        let out_of_range = index.node_count() + 5;
        assert_eq!(sssp.distance(out_of_range), None);
        assert_eq!(sssp.predecessor(out_of_range), None);
        assert_eq!(sssp.path_to(out_of_range), None);
    }

    /// Two equal-cost routes: the one discovered first keeps the node, and
    /// discovery order is the documented one — the frontier pops by
    /// (distance, node index), so `B` (lower index) reaches `D` before `C`
    /// does, and `C`'s equal-cost offer never replaces it.
    #[test]
    fn sssp_tie_break_keeps_the_first_discovered_path() {
        let index = make_index(
            &[
                ("A", "B", 1.0),
                ("A", "C", 1.0),
                ("B", "D", 1.0),
                ("C", "D", 1.0),
            ],
            &[],
        );
        let sssp = index
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(distance_of(&index, &sssp, "D"), Some(2.0));
        assert_eq!(
            path_of(&index, &sssp, "D"),
            Some(vec!["A".to_owned(), "B".to_owned(), "D".to_owned()])
        );
        // Deterministic: the same snapshot yields the same predecessors.
        let again = index
            .sssp(&node("A"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(again, sssp);
    }

    /// Under `Both`, a predecessor is the neighbor the walk arrived from —
    /// the path is a node sequence regardless of each edge's direction.
    #[test]
    fn sssp_path_under_both_follows_reverse_edges() {
        let index = make_index(&[("A", "B", 1.0), ("C", "B", 1.0)], &[]);
        let sssp = index
            .sssp(&node("A"), GraphDirection::Both)
            .expect("sssp runs");
        assert_eq!(
            path_of(&index, &sssp, "C"),
            Some(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()])
        );
    }

    // --- edge-type filter (#3471) ---------------------------------------

    /// #3471's mixed graph: a street edge and a two-hop semantic shortcut.
    /// Unfiltered, the shortcut wins; street-only, the street distance is
    /// the answer and the category node is unreachable; a type this
    /// snapshot never saw restricts to nothing, as it does for bfs.
    #[test]
    fn sssp_edge_type_filter_excludes_other_types() {
        let index = make_typed_index(&[
            ("a", "street", "b", 100.0),
            ("a", "member", "category", 1.0),
            ("category", "member", "b", 1.0),
        ]);
        let unfiltered = index
            .sssp(&node("a"), GraphDirection::Outgoing)
            .expect("sssp runs");
        assert_eq!(distance_of(&index, &unfiltered, "b"), Some(2.0));

        let street = index
            .sssp_with(
                &node("a"),
                &super::GraphSsspOptions::new(GraphDirection::Outgoing, types(&["street"])),
            )
            .expect("filtered sssp runs");
        assert_eq!(distance_of(&index, &street, "b"), Some(100.0));
        assert_eq!(distance_of(&index, &street, "category"), None);
        assert_eq!(
            path_of(&index, &street, "b"),
            Some(vec!["a".to_owned(), "b".to_owned()])
        );

        let member = index
            .sssp_with(
                &node("a"),
                &super::GraphSsspOptions::new(GraphDirection::Outgoing, types(&["member"])),
            )
            .expect("filtered sssp runs");
        assert_eq!(distance_of(&index, &member, "b"), Some(2.0));

        for restricting_to_nothing in [types(&["unknown"]), types(&[])] {
            let none = index
                .sssp_with(
                    &node("a"),
                    &super::GraphSsspOptions::new(GraphDirection::Outgoing, restricting_to_nothing),
                )
                .expect("a filter that matches nothing still runs");
            assert_eq!(none.reachable_count(), 1, "only the source is reachable");
        }
        // No filter at all is the unfiltered result.
        let all = index
            .sssp_with(
                &node("a"),
                &super::GraphSsspOptions::new(GraphDirection::Outgoing, None),
            )
            .expect("sssp runs");
        assert_eq!(all, unfiltered);
    }

    /// The filter applies along incoming and both-direction walks too.
    #[test]
    fn sssp_edge_type_filter_applies_in_every_direction() {
        let index = make_typed_index(&[("a", "street", "b", 5.0), ("c", "member", "b", 1.0)]);
        for direction in [GraphDirection::Incoming, GraphDirection::Both] {
            let street = index
                .sssp_with(
                    &node("b"),
                    &super::GraphSsspOptions::new(direction, types(&["street"])),
                )
                .expect("filtered sssp runs");
            assert_eq!(distance_of(&index, &street, "a"), Some(5.0));
            assert_eq!(distance_of(&index, &street, "c"), None);
        }
    }

    /// Negative weights are refused for the edges a query can actually
    /// walk: a negative `hates` edge refuses an unfiltered or `hates`-only
    /// query, and never a `knows`-only one — while the snapshot still
    /// reports that it holds a negative weight.
    #[test]
    fn sssp_negative_weight_refusal_scopes_to_the_selected_types() {
        let index = make_typed_index(&[("A", "knows", "B", 1.0), ("A", "hates", "C", -1.0)]);
        assert!(index.has_negative_weight());
        for options in [
            super::GraphSsspOptions::new(GraphDirection::Outgoing, None),
            super::GraphSsspOptions::new(GraphDirection::Outgoing, types(&["hates"])),
            super::GraphSsspOptions::new(GraphDirection::Outgoing, types(&["knows", "hates"])),
        ] {
            assert_eq!(
                index
                    .sssp_with(&node("A"), &options)
                    .expect_err("a selected negative edge refuses")
                    .code(),
                "failed_precondition.engine.graph_negative_weight"
            );
        }
        let knows = index
            .sssp_with(
                &node("A"),
                &super::GraphSsspOptions::new(GraphDirection::Outgoing, types(&["knows"])),
            )
            .expect("the negative edge is not selected");
        assert_eq!(distance_of(&index, &knows, "B"), Some(1.0));
        assert_eq!(distance_of(&index, &knows, "C"), None);
    }

    /// The builder interns types in first-encounter order and `finish`
    /// remaps them to name order; the per-type negative verdict must follow
    /// the remap. `z` is interned first and sorts last.
    #[test]
    fn negative_weight_types_follow_the_finish_remap() {
        let index = make_typed_index(&[("A", "z", "B", 1.0), ("A", "a", "C", -1.0)]);
        assert_eq!(
            index.edge_type_index(&GraphEdgeType::new("a").expect("type")),
            Some(0)
        );
        index
            .sssp_with(
                &node("A"),
                &super::GraphSsspOptions::new(GraphDirection::Outgoing, types(&["z"])),
            )
            .expect("`z` carries no negative edge");
        assert_eq!(
            index
                .sssp_with(
                    &node("A"),
                    &super::GraphSsspOptions::new(GraphDirection::Outgoing, types(&["a"])),
                )
                .expect_err("`a` carries the negative edge")
                .code(),
            "failed_precondition.engine.graph_negative_weight"
        );
    }

    /// #3465: over count weights a distance is an exact count up to
    /// 2^53 − 1, and `distance_count` says so; the first sum that leaves the
    /// safe range reads as none rather than as a rounded integer.
    #[test]
    fn sssp_distances_over_count_weights_are_exact_counts() {
        const HALF: f64 = 4_503_599_627_370_496.0; // 2^52
                                                   // a -> b -> c reaches 2^53 - 1 exactly; c -> d lands on 2^53, which
                                                   // 2^53 + 1 also rounds to, so it is no longer a trustworthy count;
                                                   // d -> e (81 more) is beyond the range outright.
        let index = make_index(
            &[
                ("a", "b", HALF),
                ("b", "c", HALF - 1.0),
                ("c", "d", 1.0),
                ("d", "e", 81.0),
                ("a", "f", 3.0),
                ("f", "g", 0.5),
            ],
            &[],
        );
        let sssp = index
            .sssp(&node("a"), GraphDirection::Outgoing)
            .expect("sssp runs");
        let count_of =
            |id: &str| sssp.distance_count(index.node_index(&node(id)).expect("present"));
        assert_eq!(count_of("a"), Some(0));
        assert_eq!(count_of("b"), Some(4_503_599_627_370_496));
        assert_eq!(count_of("c"), Some(9_007_199_254_740_991));
        assert_eq!(count_of("d"), None, "2^53 is ambiguous, not a count");
        assert_eq!(count_of("e"), None, "beyond the safe range");
        assert_eq!(count_of("f"), Some(3));
        assert_eq!(count_of("g"), None, "a fractional weight breaks the count");
        assert_eq!(
            sssp.distance_count(index.node_count() + 1),
            None,
            "out of range is none, like distance()"
        );
        // The snapshot's edges report the same reading as the edge data did.
        let a = index.node_index(&node("a")).expect("present");
        let counts: Vec<Option<u64>> = index
            .outgoing(a)
            .iter()
            .map(super::super::GraphAdjacencyEdge::weight_count)
            .collect();
        assert_eq!(counts, [Some(4_503_599_627_370_496), Some(3)]);
    }
}
