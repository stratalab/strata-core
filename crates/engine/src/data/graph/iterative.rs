//! Iterative graph analytics (GA3): community detection via label
//! propagation, `PageRank`, and personalized `PageRank`.
//!
//! Like the exact algorithms, these are pure methods on
//! [`GraphAdjacencyIndex`] with dense, node-index-keyed results. All
//! accumulation runs in node-index order over dense vectors, so outputs
//! are bitwise deterministic for a given visible state — floating-point
//! sums are not associative, which makes the fixed iteration order
//! load-bearing.
//!
//! Semantics are ported from the v0.6 engine, including the
//! HippoRAG-style personalized variant: both the teleport term and the
//! dangling-mass redistribution follow the personalization weights, so
//! probability concentrates around the seed nodes instead of diffusing
//! uniformly (Gutierrez et al., "`HippoRAG`", `NeurIPS` 2024,
//! <https://arxiv.org/abs/2405.14831>).

use std::collections::HashMap;

use crate::diagnostics::EngineError;

use super::{GraphAdjacencyIndex, GraphDirection, GraphNodeId};

const DEFAULT_PAGERANK_DAMPING: f64 = 0.85;
const DEFAULT_PAGERANK_MAX_ITERATIONS: usize = 20;
const DEFAULT_PAGERANK_TOLERANCE: f64 = 1e-6;
const DEFAULT_CDLP_MAX_ITERATIONS: usize = 10;

/// Options for community detection via label propagation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GraphCdlpOptions {
    max_iterations: usize,
    direction: GraphDirection,
}

impl GraphCdlpOptions {
    /// Creates explicit label-propagation options.
    #[must_use]
    pub const fn new(max_iterations: usize, direction: GraphDirection) -> Self {
        Self {
            max_iterations,
            direction,
        }
    }

    #[must_use]
    /// Returns the iteration bound.
    pub const fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    #[must_use]
    /// Returns the neighbor direction labels propagate along.
    pub const fn direction(&self) -> GraphDirection {
        self.direction
    }
}

impl Default for GraphCdlpOptions {
    fn default() -> Self {
        Self::new(DEFAULT_CDLP_MAX_ITERATIONS, GraphDirection::Both)
    }
}

/// Community labels from label propagation, keyed by node index.
///
/// Labels start as each node's own index and converge toward the most
/// frequent label among neighbors, ties broken by the smaller label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphCdlpResult {
    labels: Vec<usize>,
}

impl GraphCdlpResult {
    #[must_use]
    /// Returns the community label per node index.
    pub fn labels(&self) -> &[usize] {
        &self.labels
    }

    #[must_use]
    /// Returns the label of the node at `index`, when in range.
    pub fn label(&self, index: usize) -> Option<usize> {
        self.labels.get(index).copied()
    }
}

/// Options for `PageRank` and personalized `PageRank`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GraphPageRankOptions {
    damping: f64,
    max_iterations: usize,
    tolerance: f64,
}

impl GraphPageRankOptions {
    /// Creates explicit `PageRank` options.
    ///
    /// Refuses with `invalid_argument.engine.graph_pagerank_options`
    /// when `damping` is outside `[0, 1]` or `tolerance` is negative or
    /// not finite — either would silently poison every rank downstream.
    pub fn new(damping: f64, max_iterations: usize, tolerance: f64) -> Result<Self, EngineError> {
        if !damping.is_finite() || !(0.0..=1.0).contains(&damping) {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_pagerank_options",
                "pagerank damping must be within [0, 1]",
            ));
        }
        if !tolerance.is_finite() || tolerance < 0.0 {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_pagerank_options",
                "pagerank tolerance must be non-negative and finite",
            ));
        }
        Ok(Self {
            damping,
            max_iterations,
            tolerance,
        })
    }

    #[must_use]
    /// Returns the damping factor.
    pub const fn damping(&self) -> f64 {
        self.damping
    }

    #[must_use]
    /// Returns the iteration bound.
    pub const fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    #[must_use]
    /// Returns the L1 convergence tolerance.
    pub const fn tolerance(&self) -> f64 {
        self.tolerance
    }
}

impl Default for GraphPageRankOptions {
    fn default() -> Self {
        Self {
            damping: DEFAULT_PAGERANK_DAMPING,
            max_iterations: DEFAULT_PAGERANK_MAX_ITERATIONS,
            tolerance: DEFAULT_PAGERANK_TOLERANCE,
        }
    }
}

/// Rank scores from `PageRank` or personalized `PageRank`, keyed by node
/// index. Total mass is 1.0 (dangling-node mass is redistributed, never
/// leaked).
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPageRankResult {
    ranks: Vec<f64>,
    iterations: usize,
}

impl GraphPageRankResult {
    #[must_use]
    /// Returns the rank score per node index.
    pub fn ranks(&self) -> &[f64] {
        &self.ranks
    }

    #[must_use]
    /// Returns the rank of the node at `index`, when in range.
    pub fn rank(&self, index: usize) -> Option<f64> {
        self.ranks.get(index).copied()
    }

    #[must_use]
    /// Returns the number of power iterations performed.
    pub const fn iterations(&self) -> usize {
        self.iterations
    }
}

fn personalization_error(message: &'static str) -> EngineError {
    EngineError::invalid_input("invalid_argument.engine.graph_personalization", message)
}

impl GraphAdjacencyIndex {
    /// Detects communities via asynchronous label propagation.
    ///
    /// Every node starts labeled with its own index; each iteration
    /// re-labels every node in node-index order with the most frequent
    /// label among its neighbors along `options.direction()` — reading the
    /// labels already updated earlier in the same sweep, ties broken by the
    /// smaller label — and stops early once a full sweep changes no label.
    /// Nodes without neighbors keep their label. Updating in place converges
    /// on bipartite structures (e.g. a star collapses to one community),
    /// where synchronous propagation oscillates with period two and never
    /// settles.
    #[must_use]
    pub fn cdlp(&self, options: &GraphCdlpOptions) -> GraphCdlpResult {
        let node_count = self.node_count();
        let mut labels: Vec<usize> = (0..node_count).collect();
        // #3024: propagation is SYNCHRONOUS (the LDBC Graphalytics
        // definition) — every node's update in iteration i reads the labels
        // of iteration i-1, so results are independent of sweep order. The
        // previous in-place sweep was asynchronous and diverged from the
        // standard reference outputs.
        let mut next = labels.clone();

        for _ in 0..options.max_iterations() {
            let mut changed = false;

            for node in 0..node_count {
                let best_label = {
                    let mut frequency: HashMap<usize, usize> = HashMap::new();
                    let mut tally = |edges: &[super::GraphAdjacencyEdge]| {
                        for edge in edges {
                            *frequency.entry(labels[edge.neighbor()]).or_insert(0) += 1;
                        }
                    };
                    match options.direction() {
                        GraphDirection::Outgoing => tally(self.outgoing(node)),
                        GraphDirection::Incoming => tally(self.incoming(node)),
                        GraphDirection::Both => {
                            tally(self.outgoing(node));
                            tally(self.incoming(node));
                        }
                    }
                    let top_count = frequency.values().copied().max();
                    top_count.and_then(|top_count| {
                        frequency
                            .iter()
                            .filter(|(_, count)| **count == top_count)
                            .map(|(label, _)| *label)
                            .min()
                    })
                };
                let label = best_label.unwrap_or(labels[node]);
                if label != labels[node] {
                    changed = true;
                }
                next[node] = label;
            }

            std::mem::swap(&mut labels, &mut next);
            if !changed {
                break;
            }
        }

        GraphCdlpResult { labels }
    }

    /// Computes `PageRank` by power iteration with uniform teleport.
    ///
    /// Dangling nodes (no outgoing edges) redistribute their mass
    /// uniformly, so total rank stays at 1.0. Iteration stops when the
    /// L1 delta between rounds falls below `options.tolerance()`.
    #[must_use]
    pub fn pagerank(&self, options: &GraphPageRankOptions) -> GraphPageRankResult {
        let node_count = self.node_count();
        if node_count == 0 {
            return GraphPageRankResult {
                ranks: Vec::new(),
                iterations: 0,
            };
        }
        #[allow(clippy::cast_precision_loss)]
        let uniform = 1.0 / node_count as f64;
        let teleport: Vec<f64> = vec![uniform; node_count];
        let initial = teleport.clone();
        self.power_iteration(options, &teleport, initial, DanglingRedistribution::Uniform)
    }

    /// Computes personalized `PageRank`: power iteration where both the
    /// teleport term and the dangling-mass redistribution follow the
    /// personalization weights, concentrating mass around the seeds.
    ///
    /// The weights are L1-normalized internally over the nodes present
    /// in this snapshot, so callers can pass raw seed scores; entries
    /// for unknown nodes are ignored (a stale anchor cannot drain mass).
    /// Refuses with `invalid_argument.engine.graph_personalization` when
    /// the map is empty, any weight is negative or not finite, or no
    /// positive weight lands on a node in the snapshot.
    pub fn personalized_pagerank(
        &self,
        options: &GraphPageRankOptions,
        personalization: &HashMap<GraphNodeId, f64>,
    ) -> Result<GraphPageRankResult, EngineError> {
        if personalization.is_empty() {
            return Err(personalization_error("personalization is empty"));
        }
        if personalization
            .values()
            .any(|weight| !weight.is_finite() || *weight < 0.0)
        {
            return Err(personalization_error(
                "personalization contains a negative, NaN, or infinite weight",
            ));
        }

        let node_count = self.node_count();
        if node_count == 0 {
            return Ok(GraphPageRankResult {
                ranks: Vec::new(),
                iterations: 0,
            });
        }

        // Project onto the snapshot and L1-normalize in node-index order:
        // dense accumulation keeps the sum — and with it every rank —
        // bitwise reproducible.
        let mut weights: Vec<f64> = vec![0.0; node_count];
        for (node_id, weight) in personalization {
            if let Some(index) = self.node_index(node_id) {
                weights[index] = *weight;
            }
        }
        let total: f64 = weights.iter().sum();
        if !total.is_finite() || total <= 0.0 {
            return Err(personalization_error(
                "personalization has no positive weight on any node in this graph",
            ));
        }
        for weight in &mut weights {
            *weight /= total;
        }

        let initial = weights.clone();
        Ok(self.power_iteration(
            options,
            &weights,
            initial,
            DanglingRedistribution::Personalized,
        ))
    }

    /// Shared power-iteration core. `teleport` must sum to 1.0; ranks
    /// start from `ranks` (uniform for `PageRank`, the normalized weights
    /// for the personalized variant).
    fn power_iteration(
        &self,
        options: &GraphPageRankOptions,
        teleport: &[f64],
        mut ranks: Vec<f64>,
        dangling: DanglingRedistribution,
    ) -> GraphPageRankResult {
        let node_count = self.node_count();
        let out_degree: Vec<usize> = (0..node_count)
            .map(|node| self.outgoing(node).len())
            .collect();
        let damping = options.damping();
        #[allow(clippy::cast_precision_loss)]
        let uniform = 1.0 / node_count as f64;
        let mut iterations = 0;

        for _ in 0..options.max_iterations() {
            iterations += 1;

            let dangling_sum: f64 = (0..node_count)
                .filter(|node| out_degree[*node] == 0)
                .map(|node| ranks[node])
                .sum();

            let mut new_ranks = vec![0.0f64; node_count];
            for (node, new_rank) in new_ranks.iter_mut().enumerate() {
                let teleport_share = teleport[node];
                let base = (1.0 - damping) * teleport_share;
                let dangling_contribution = match dangling {
                    DanglingRedistribution::Uniform => damping * dangling_sum * uniform,
                    DanglingRedistribution::Personalized => damping * dangling_sum * teleport_share,
                };
                let mut inbound = 0.0;
                for edge in self.incoming(node) {
                    // The source of an incoming edge has that edge in its
                    // outgoing list, so its degree is at least one.
                    #[allow(clippy::cast_precision_loss)]
                    let share = ranks[edge.neighbor()] / out_degree[edge.neighbor()] as f64;
                    inbound += share;
                }
                *new_rank = base + dangling_contribution + damping * inbound;
            }

            let delta: f64 = new_ranks
                .iter()
                .zip(&ranks)
                .map(|(new, old)| (new - old).abs())
                .sum();
            ranks = new_ranks;
            if delta < options.tolerance() {
                break;
            }
        }

        GraphPageRankResult { ranks, iterations }
    }
}

/// How dangling-node mass re-enters the walk.
#[derive(Clone, Copy)]
enum DanglingRedistribution {
    /// Spread evenly over all nodes (standard `PageRank`).
    Uniform,
    /// Spread by teleport weight (personalized `PageRank`).
    Personalized,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::{GraphAdjacencyIndexBuilder, GraphAnalyticsBudget};
    use crate::data::graph::{GraphDirection, GraphEdgeType, GraphName, GraphNodeId};

    use super::{GraphAdjacencyIndex, GraphCdlpOptions, GraphPageRankOptions};

    fn node(value: &str) -> GraphNodeId {
        GraphNodeId::new(value).expect("node id")
    }

    fn make_index(edges: &[(&str, &str)], extra_nodes: &[&str]) -> GraphAdjacencyIndex {
        let mut ids: Vec<&str> = extra_nodes.to_vec();
        for (src, dst) in edges {
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
        for (src, dst) in edges {
            builder
                .add_edge(&node(src), &edge_type, &node(dst), 1.0)
                .expect("edge fits");
        }
        builder.finish()
    }

    fn at(index: &GraphAdjacencyIndex, id: &str) -> usize {
        index.node_index(&node(id)).expect("node present")
    }

    fn personalization(entries: &[(&str, f64)]) -> HashMap<GraphNodeId, f64> {
        entries
            .iter()
            .map(|(id, weight)| (node(id), *weight))
            .collect()
    }

    #[test]
    fn cdlp_groups_two_cliques_across_a_bridge() {
        let index = make_index(
            &[
                ("A", "B"),
                ("B", "A"),
                ("A", "C"),
                ("C", "A"),
                ("B", "C"),
                ("C", "B"),
                ("D", "E"),
                ("E", "D"),
                ("D", "F"),
                ("F", "D"),
                ("E", "F"),
                ("F", "E"),
                ("C", "D"),
            ],
            &[],
        );
        let cdlp = index.cdlp(&GraphCdlpOptions::default());
        assert_eq!(cdlp.label(at(&index, "A")), cdlp.label(at(&index, "B")));
        assert_eq!(cdlp.label(at(&index, "A")), cdlp.label(at(&index, "C")));
        assert_eq!(cdlp.label(at(&index, "D")), cdlp.label(at(&index, "E")));
        assert_eq!(cdlp.label(at(&index, "D")), cdlp.label(at(&index, "F")));
    }

    #[test]
    fn cdlp_updates_synchronously_within_an_iteration() {
        // #3024 promoted contract: every node's update in iteration i reads
        // the labels of iteration i-1 (the LDBC Graphalytics definition),
        // never labels already rewritten in the same sweep. On a two-node
        // cycle with ONE iteration, each node must adopt the other's
        // INITIAL label; the old asynchronous sweep let the second node see
        // the first node's fresh label and collapse both communities.
        let index = make_index(&[("A", "B"), ("B", "A")], &[]);
        let cdlp = index.cdlp(&GraphCdlpOptions::new(1, GraphDirection::Outgoing));
        assert_eq!(cdlp.label(at(&index, "A")), Some(at(&index, "B")));
        assert_eq!(cdlp.label(at(&index, "B")), Some(at(&index, "A")));
    }

    #[test]
    fn cdlp_single_node_keeps_its_label() {
        let index = make_index(&[], &["A"]);
        let cdlp = index.cdlp(&GraphCdlpOptions::default());
        assert_eq!(cdlp.labels(), &[0]);
    }

    #[test]
    fn cdlp_converges_before_the_iteration_bound() {
        let index = make_index(
            &[
                ("A", "B"),
                ("B", "A"),
                ("A", "C"),
                ("C", "A"),
                ("B", "C"),
                ("C", "B"),
            ],
            &[],
        );
        let cdlp = index.cdlp(&GraphCdlpOptions::new(100, GraphDirection::Both));
        assert_eq!(cdlp.label(at(&index, "A")), cdlp.label(at(&index, "B")));
        assert_eq!(cdlp.label(at(&index, "B")), cdlp.label(at(&index, "C")));
    }

    #[test]
    fn cdlp_outgoing_direction_leaves_sinks_fixed() {
        let index = make_index(&[("A", "B"), ("A", "C")], &[]);
        let cdlp = index.cdlp(&GraphCdlpOptions::new(10, GraphDirection::Outgoing));
        let label_b = cdlp.label(at(&index, "B")).expect("labeled");
        let label_c = cdlp.label(at(&index, "C")).expect("labeled");
        assert_ne!(label_b, label_c, "sinks keep their own labels");
        assert_eq!(
            cdlp.label(at(&index, "A")),
            Some(label_b.min(label_c)),
            "A adopts the smaller neighbor label"
        );
    }

    #[test]
    fn pagerank_chain_ranks_downstream_higher() {
        let index = make_index(&[("A", "B"), ("B", "C")], &[]);
        let result = index.pagerank(&GraphPageRankOptions::default());
        let rank = |id: &str| result.rank(at(&index, id)).expect("ranked");
        assert!(rank("C") > rank("B"));
        assert!(rank("B") > rank("A"));
    }

    #[test]
    fn pagerank_star_leaves_receive_equal_rank() {
        let index = make_index(&[("A", "B"), ("A", "C"), ("A", "D")], &[]);
        let result = index.pagerank(&GraphPageRankOptions::default());
        let rank = |id: &str| result.rank(at(&index, id)).expect("ranked");
        assert!((rank("B") - rank("C")).abs() < 1e-10);
        assert!((rank("B") - rank("D")).abs() < 1e-10);
    }

    #[test]
    fn pagerank_mass_sums_to_one() {
        let index = make_index(&[("A", "B"), ("B", "C"), ("C", "A")], &[]);
        let result = index.pagerank(&GraphPageRankOptions::default());
        let sum: f64 = result.ranks().iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {sum}");
    }

    #[test]
    fn pagerank_lower_damping_flattens_ranks() {
        let index = make_index(&[("A", "B"), ("B", "C"), ("C", "A")], &[]);
        let spread = |damping: f64| {
            let options = GraphPageRankOptions::new(damping, 20, 1e-6).expect("options valid");
            let result = index.pagerank(&options);
            let max = result.ranks().iter().copied().fold(0.0f64, f64::max);
            let min = result.ranks().iter().copied().fold(f64::INFINITY, f64::min);
            max - min
        };
        assert!(spread(0.5) <= spread(0.99) + 1e-10);
    }

    #[test]
    fn pagerank_empty_graph_is_empty() {
        let index = make_index(&[], &[]);
        let result = index.pagerank(&GraphPageRankOptions::default());
        assert!(result.ranks().is_empty());
        assert_eq!(result.iterations(), 0);
    }

    #[test]
    fn pagerank_dangling_nodes_preserve_mass() {
        let index = make_index(&[("A", "B"), ("B", "C")], &[]);
        let result = index.pagerank(&GraphPageRankOptions::default());
        let sum: f64 = result.ranks().iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {sum} (mass leaked)");
    }

    #[test]
    fn pagerank_all_dangling_stays_uniform() {
        let index = make_index(&[], &["A", "B", "C"]);
        let result = index.pagerank(&GraphPageRankOptions::default());
        let sum: f64 = result.ranks().iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {sum}");
        for rank in result.ranks() {
            assert!((rank - 1.0 / 3.0).abs() < 1e-6, "rank {rank} != 1/3");
        }
    }

    #[test]
    fn pagerank_options_refuse_bad_damping_and_tolerance() {
        for (damping, tolerance) in [
            (-0.1, 1e-6),
            (1.5, 1e-6),
            (f64::NAN, 1e-6),
            (0.85, -1.0),
            (0.85, f64::NAN),
        ] {
            let error =
                GraphPageRankOptions::new(damping, 20, tolerance).expect_err("options refuse");
            assert_eq!(
                error.code(),
                "invalid_argument.engine.graph_pagerank_options"
            );
        }
    }

    #[test]
    fn ppr_concentrates_mass_around_the_seed() {
        // Chain A→B→C seeded at A. Closed form: rank(A) = (1-d)/(1-d³).
        let index = make_index(&[("A", "B"), ("B", "C")], &[]);
        let options = GraphPageRankOptions::new(0.85, 200, 1e-8).expect("options valid");
        let result = index
            .personalized_pagerank(&options, &personalization(&[("A", 1.0)]))
            .expect("ppr runs");
        let rank = |id: &str| result.rank(at(&index, id)).expect("ranked");
        assert!(rank("A") > rank("B"));
        assert!(rank("B") > rank("C"));
        let damping: f64 = 0.85;
        let expected_a = (1.0 - damping) / (1.0 - damping.powi(3));
        assert!(
            (rank("A") - expected_a).abs() < 1e-4,
            "rank(A)={} vs closed-form {expected_a}",
            rank("A")
        );
    }

    #[test]
    fn ppr_symmetric_seeds_produce_bitwise_equal_ranks() {
        // Two isomorphic bidirectional chains seeded at mirror endpoints:
        // deterministic Jacobi iteration must produce bitwise-equal
        // mirror ranks; a tolerance would hide subtle drift.
        let index = make_index(
            &[
                ("A", "B"),
                ("B", "A"),
                ("B", "C"),
                ("C", "B"),
                ("X", "Y"),
                ("Y", "X"),
                ("Y", "Z"),
                ("Z", "Y"),
            ],
            &[],
        );
        let result = index
            .personalized_pagerank(
                &GraphPageRankOptions::default(),
                &personalization(&[("A", 0.5), ("X", 0.5)]),
            )
            .expect("ppr runs");
        for (left, right) in [("A", "X"), ("B", "Y"), ("C", "Z")] {
            let left_rank = result.rank(at(&index, left)).expect("ranked");
            let right_rank = result.rank(at(&index, right)).expect("ranked");
            assert_eq!(
                left_rank.to_bits(),
                right_rank.to_bits(),
                "rank({left})={left_rank} vs rank({right})={right_rank}"
            );
        }
    }

    #[test]
    fn ppr_dangling_mass_returns_to_the_seed() {
        // A→B plus isolated dangling C and D, all personalization on A:
        // dangling mass must flow back to A, not spread uniformly.
        let index = make_index(&[("A", "B")], &["C", "D"]);
        let options = GraphPageRankOptions::default();
        let ppr = index
            .personalized_pagerank(&options, &personalization(&[("A", 1.0)]))
            .expect("ppr runs");
        let uniform = index.pagerank(&options);
        let a = at(&index, "A");
        assert!(
            ppr.rank(a).expect("ranked") > uniform.rank(a).expect("ranked") + 0.15,
            "PPR rank(A)={:?} should clearly exceed uniform rank(A)={:?}",
            ppr.rank(a),
            uniform.rank(a)
        );
        assert!(
            ppr.rank(a).expect("ranked") > 0.5,
            "seed keeps the majority"
        );
        // C and D have no teleport share and no incoming edges: their
        // rank stays exactly zero through every iteration.
        assert_eq!(ppr.rank(at(&index, "C")), Some(0.0));
        assert_eq!(ppr.rank(at(&index, "D")), Some(0.0));
    }

    #[test]
    fn ppr_zero_iterations_returns_the_normalized_seed_state() {
        let index = make_index(&[("A", "B"), ("B", "C")], &[]);
        let options = GraphPageRankOptions::new(0.85, 0, 1e-6).expect("options valid");
        let result = index
            .personalized_pagerank(&options, &personalization(&[("A", 0.75), ("B", 0.25)]))
            .expect("ppr runs");
        assert_eq!(result.iterations(), 0);
        assert_eq!(
            result.rank(at(&index, "A")).expect("ranked").to_bits(),
            0.75f64.to_bits()
        );
        assert_eq!(
            result.rank(at(&index, "B")).expect("ranked").to_bits(),
            0.25f64.to_bits()
        );
        assert_eq!(
            result.rank(at(&index, "C")).expect("ranked").to_bits(),
            0.0f64.to_bits()
        );
    }

    #[test]
    fn ppr_preserves_mass_across_damping_values() {
        let index = make_index(&[("A", "B"), ("B", "C")], &[]);
        for damping in [0.3f64, 0.5, 0.85] {
            let options = GraphPageRankOptions::new(damping, 20, 1e-6).expect("options valid");
            let result = index
                .personalized_pagerank(&options, &personalization(&[("A", 0.6), ("B", 0.4)]))
                .expect("ppr runs");
            let sum: f64 = result.ranks().iter().sum();
            assert!(
                (sum - 1.0).abs() < 1e-6,
                "damping={damping} sum={sum} (mass leaked)"
            );
        }
    }

    #[test]
    fn ppr_refuses_empty_all_zero_and_invalid_weights() {
        let index = make_index(&[("A", "B")], &[]);
        let options = GraphPageRankOptions::default();
        let cases: Vec<HashMap<GraphNodeId, f64>> = vec![
            personalization(&[]),
            personalization(&[("A", 0.0), ("B", 0.0)]),
            personalization(&[("A", 5.0), ("B", -2.0)]),
            personalization(&[("A", f64::NAN)]),
            personalization(&[("A", f64::INFINITY)]),
        ];
        for case in cases {
            let error = index
                .personalized_pagerank(&options, &case)
                .expect_err("personalization refuses");
            assert_eq!(
                error.code(),
                "invalid_argument.engine.graph_personalization"
            );
        }
    }

    #[test]
    fn ppr_ignores_unknown_nodes_and_renormalizes() {
        // A stale anchor that is not in the snapshot must not drain
        // probability mass: {A: 3, ghost: 2} behaves exactly like {A: 1}.
        let index = make_index(&[("A", "B"), ("B", "C")], &[]);
        let options = GraphPageRankOptions::default();
        let with_ghost = index
            .personalized_pagerank(&options, &personalization(&[("A", 3.0), ("GHOST", 2.0)]))
            .expect("ppr runs");
        let sum: f64 = with_ghost.ranks().iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "mass leaked to a ghost: {sum}");
        let seed_only = index
            .personalized_pagerank(&options, &personalization(&[("A", 1.0)]))
            .expect("ppr runs");
        for node in 0..index.node_count() {
            assert_eq!(
                with_ghost.rank(node).expect("ranked").to_bits(),
                seed_only.rank(node).expect("ranked").to_bits()
            );
        }
        // All weight on unknown nodes leaves nothing to anchor the walk.
        let error = index
            .personalized_pagerank(
                &options,
                &personalization(&[("GHOST1", 1.0), ("GHOST2", 2.0)]),
            )
            .expect_err("all-ghost personalization refuses");
        assert_eq!(
            error.code(),
            "invalid_argument.engine.graph_personalization"
        );
    }

    #[test]
    fn ppr_normalizes_raw_weights_bitwise_identically() {
        // Raw {A: 5, B: 3} must match pre-normalized {A: 0.625, B: 0.375}
        // bitwise — 5/8 and 3/8 are exact in f64, so any drift would be a
        // normalization bug.
        let index = make_index(&[("A", "B"), ("B", "C")], &[]);
        let options = GraphPageRankOptions::default();
        let raw = index
            .personalized_pagerank(&options, &personalization(&[("A", 5.0), ("B", 3.0)]))
            .expect("ppr runs");
        let normalized = index
            .personalized_pagerank(&options, &personalization(&[("A", 0.625), ("B", 0.375)]))
            .expect("ppr runs");
        for node in 0..index.node_count() {
            assert_eq!(
                raw.rank(node).expect("ranked").to_bits(),
                normalized.rank(node).expect("ranked").to_bits()
            );
        }
    }
}
