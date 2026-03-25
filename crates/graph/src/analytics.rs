//! Graph analytics algorithms: WCC, CDLP, PageRank, LCC, SSSP.
//!
//! All algorithms follow the same pattern as BFS: build an in-memory
//! `AdjacencyIndex` from KV storage, then run the algorithm in-memory.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use strata_core::types::BranchId;
use strata_core::StrataResult;

use super::adjacency::AdjacencyIndex;
use super::types::*;
use super::GraphStore;

impl GraphStore {
    /// Weakly Connected Components using union-find.
    pub fn wcc(&self, branch_id: BranchId, graph: &str) -> StrataResult<WccResult> {
        let index = self.build_adjacency_index(branch_id, graph)?;
        // Also load isolated nodes (nodes with no edges).
        let all_nodes = self.list_nodes(branch_id, graph)?;
        let mut full_index = index;
        for node_id in &all_nodes {
            full_index.nodes.insert(node_id.clone());
        }
        Ok(wcc_with_index(&full_index))
    }

    /// Community Detection via Label Propagation.
    pub fn cdlp(
        &self,
        branch_id: BranchId,
        graph: &str,
        opts: CdlpOptions,
    ) -> StrataResult<CdlpResult> {
        let index = self.build_adjacency_index(branch_id, graph)?;
        let all_nodes = self.list_nodes(branch_id, graph)?;
        let mut full_index = index;
        for node_id in &all_nodes {
            full_index.nodes.insert(node_id.clone());
        }
        Ok(cdlp_with_index(&full_index, &opts))
    }

    /// PageRank iterative importance scoring.
    pub fn pagerank(
        &self,
        branch_id: BranchId,
        graph: &str,
        opts: PageRankOptions,
    ) -> StrataResult<PageRankResult> {
        let index = self.build_adjacency_index(branch_id, graph)?;
        let all_nodes = self.list_nodes(branch_id, graph)?;
        let mut full_index = index;
        for node_id in &all_nodes {
            full_index.nodes.insert(node_id.clone());
        }
        Ok(pagerank_with_index(&full_index, &opts))
    }

    /// Local Clustering Coefficient.
    pub fn lcc(&self, branch_id: BranchId, graph: &str) -> StrataResult<LccResult> {
        let index = self.build_adjacency_index(branch_id, graph)?;
        let all_nodes = self.list_nodes(branch_id, graph)?;
        let mut full_index = index;
        for node_id in &all_nodes {
            full_index.nodes.insert(node_id.clone());
        }
        Ok(lcc_with_index(&full_index))
    }

    /// Single-Source Shortest Path (Dijkstra).
    pub fn sssp(
        &self,
        branch_id: BranchId,
        graph: &str,
        source: &str,
        opts: SsspOptions,
    ) -> StrataResult<SsspResult> {
        let index = self.build_adjacency_index(branch_id, graph)?;
        Ok(sssp_with_index(&index, source, &opts))
    }
}

// =============================================================================
// Pure algorithm implementations (testable with hand-built AdjacencyIndex)
// =============================================================================

/// Weakly Connected Components using union-find with path compression.
pub fn wcc_with_index(index: &AdjacencyIndex) -> WccResult {
    // Collect all node IDs (from edges + explicit nodes set).
    let mut all_nodes: Vec<&str> = Vec::new();
    let mut node_set: HashSet<&str> = HashSet::new();

    for node_id in &index.nodes {
        if node_set.insert(node_id.as_str()) {
            all_nodes.push(node_id.as_str());
        }
    }
    for (src, edges) in &index.outgoing {
        if node_set.insert(src.as_str()) {
            all_nodes.push(src.as_str());
        }
        for (dst, _, _) in edges {
            if node_set.insert(dst.as_str()) {
                all_nodes.push(dst.as_str());
            }
        }
    }
    for (dst, edges) in &index.incoming {
        if node_set.insert(dst.as_str()) {
            all_nodes.push(dst.as_str());
        }
        for (src, _, _) in edges {
            if node_set.insert(src.as_str()) {
                all_nodes.push(src.as_str());
            }
        }
    }

    all_nodes.sort();

    // Map node_id → index
    let id_map: HashMap<&str, usize> = all_nodes
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect();

    let n = all_nodes.len();
    let mut parent: Vec<usize> = (0..n).collect();
    let mut rank: Vec<usize> = vec![0; n];

    fn find(parent: &mut [usize], x: usize) -> usize {
        if parent[x] != x {
            parent[x] = find(parent, parent[x]);
        }
        parent[x]
    }

    fn union(parent: &mut [usize], rank: &mut [usize], x: usize, y: usize) {
        let rx = find(parent, x);
        let ry = find(parent, y);
        if rx == ry {
            return;
        }
        match rank[rx].cmp(&rank[ry]) {
            std::cmp::Ordering::Less => parent[rx] = ry,
            std::cmp::Ordering::Greater => parent[ry] = rx,
            std::cmp::Ordering::Equal => {
                parent[ry] = rx;
                rank[rx] += 1;
            }
        }
    }

    // Process all edges (undirected — only need outgoing since each edge appears once).
    for (src, edges) in &index.outgoing {
        if let Some(&src_idx) = id_map.get(src.as_str()) {
            for (dst, _, _) in edges {
                if let Some(&dst_idx) = id_map.get(dst.as_str()) {
                    union(&mut parent, &mut rank, src_idx, dst_idx);
                }
            }
        }
    }

    // Assign component IDs (use stable hash of smallest node_id in component).
    // First, find the minimum-index node in each component.
    let mut component_min: HashMap<usize, usize> = HashMap::new();
    for i in 0..n {
        let root = find(&mut parent, i);
        let entry = component_min.entry(root).or_insert(i);
        if i < *entry {
            *entry = i;
        }
    }

    // Build result: each node maps to its component ID (the index of the smallest node).
    let mut components = HashMap::new();
    for (i, &node_id) in all_nodes.iter().enumerate() {
        let root = find(&mut parent, i);
        let min_idx = component_min[&root];
        components.insert(node_id.to_string(), min_idx as u64);
    }

    WccResult { components }
}

/// Community Detection via Label Propagation.
pub fn cdlp_with_index(index: &AdjacencyIndex, opts: &CdlpOptions) -> CdlpResult {
    // Collect all nodes.
    let mut all_nodes: Vec<String> = Vec::new();
    let mut node_set: HashSet<&str> = HashSet::new();

    // Helper: collect from nodes set and edges
    for node_id in &index.nodes {
        if node_set.insert(node_id.as_str()) {
            all_nodes.push(node_id.clone());
        }
    }
    for (src, edges) in &index.outgoing {
        if node_set.insert(src.as_str()) {
            all_nodes.push(src.clone());
        }
        for (dst, _, _) in edges {
            if node_set.insert(dst.as_str()) {
                all_nodes.push(dst.clone());
            }
        }
    }
    for (dst, edges) in &index.incoming {
        if node_set.insert(dst.as_str()) {
            all_nodes.push(dst.clone());
        }
        for (src, _, _) in edges {
            if node_set.insert(src.as_str()) {
                all_nodes.push(src.clone());
            }
        }
    }

    all_nodes.sort();

    // Initialize each node with a unique label (its sorted index).
    let mut labels: HashMap<String, u64> = all_nodes
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i as u64))
        .collect();

    for _ in 0..opts.max_iterations {
        let mut changed = false;
        let mut new_labels = labels.clone();

        for node_id in &all_nodes {
            // Collect neighbor labels based on direction.
            let mut neighbor_labels: Vec<u64> = Vec::new();

            match opts.direction {
                Direction::Outgoing => {
                    for (dst, _) in index.outgoing_neighbor_ids(node_id, None) {
                        if let Some(&lbl) = labels.get(dst) {
                            neighbor_labels.push(lbl);
                        }
                    }
                }
                Direction::Incoming => {
                    for (src, _) in index.incoming_neighbor_ids(node_id, None) {
                        if let Some(&lbl) = labels.get(src) {
                            neighbor_labels.push(lbl);
                        }
                    }
                }
                Direction::Both => {
                    for (dst, _) in index.outgoing_neighbor_ids(node_id, None) {
                        if let Some(&lbl) = labels.get(dst) {
                            neighbor_labels.push(lbl);
                        }
                    }
                    for (src, _) in index.incoming_neighbor_ids(node_id, None) {
                        if let Some(&lbl) = labels.get(src) {
                            neighbor_labels.push(lbl);
                        }
                    }
                }
            }

            if neighbor_labels.is_empty() {
                continue;
            }

            // Find most frequent label, ties broken by smallest label.
            let mut freq: HashMap<u64, usize> = HashMap::new();
            for &lbl in &neighbor_labels {
                *freq.entry(lbl).or_insert(0) += 1;
            }

            let max_count = *freq.values().max().unwrap();
            let best_label = *freq
                .iter()
                .filter(|(_, &count)| count == max_count)
                .map(|(&lbl, _)| lbl)
                .collect::<Vec<_>>()
                .iter()
                .min()
                .unwrap();

            if best_label != labels[node_id.as_str()] {
                new_labels.insert(node_id.clone(), best_label);
                changed = true;
            }
        }

        labels = new_labels;
        if !changed {
            break;
        }
    }

    CdlpResult { labels }
}

/// PageRank power iteration.
pub fn pagerank_with_index(index: &AdjacencyIndex, opts: &PageRankOptions) -> PageRankResult {
    // Collect all nodes.
    let mut all_nodes: Vec<String> = Vec::new();
    let mut node_set: HashSet<&str> = HashSet::new();

    for node_id in &index.nodes {
        if node_set.insert(node_id.as_str()) {
            all_nodes.push(node_id.clone());
        }
    }
    for (src, edges) in &index.outgoing {
        if node_set.insert(src.as_str()) {
            all_nodes.push(src.clone());
        }
        for (dst, _, _) in edges {
            if node_set.insert(dst.as_str()) {
                all_nodes.push(dst.clone());
            }
        }
    }
    for (dst, edges) in &index.incoming {
        if node_set.insert(dst.as_str()) {
            all_nodes.push(dst.clone());
        }
        for (src, _, _) in edges {
            if node_set.insert(src.as_str()) {
                all_nodes.push(src.clone());
            }
        }
    }

    let n = all_nodes.len();
    if n == 0 {
        return PageRankResult {
            ranks: HashMap::new(),
            iterations: 0,
        };
    }

    let init_rank = 1.0 / n as f64;
    let mut ranks: HashMap<String, f64> =
        all_nodes.iter().map(|id| (id.clone(), init_rank)).collect();

    // Precompute out-degrees.
    let mut out_degree: HashMap<&str, usize> = HashMap::new();
    for node_id in &all_nodes {
        let deg = index
            .outgoing
            .get(node_id.as_str())
            .map(|e| e.len())
            .unwrap_or(0);
        out_degree.insert(node_id.as_str(), deg);
    }

    let d = opts.damping;
    let base = (1.0 - d) / n as f64;
    let mut iterations = 0;

    for _ in 0..opts.max_iterations {
        iterations += 1;

        // Compute dangling node mass: sum of ranks for nodes with no outgoing edges.
        let dangling_sum: f64 = all_nodes
            .iter()
            .filter(|id| *out_degree.get(id.as_str()).unwrap_or(&0) == 0)
            .map(|id| ranks[id])
            .sum();
        let dangling_contrib = d * dangling_sum / n as f64;

        let mut new_ranks: HashMap<String, f64> = HashMap::with_capacity(n);

        for node_id in &all_nodes {
            let mut sum = 0.0;
            // Sum contributions from incoming neighbors.
            if let Some(incoming) = index.incoming.get(node_id.as_str()) {
                for (src, _, _) in incoming {
                    let src_rank = ranks.get(src.as_str()).copied().unwrap_or(0.0);
                    let src_deg = *out_degree.get(src.as_str()).unwrap_or(&1);
                    if src_deg > 0 {
                        sum += src_rank / src_deg as f64;
                    }
                }
            }
            new_ranks.insert(node_id.clone(), base + dangling_contrib + d * sum);
        }

        // Check convergence (L1 norm).
        let delta: f64 = all_nodes
            .iter()
            .map(|id| (new_ranks[id] - ranks[id]).abs())
            .sum();

        ranks = new_ranks;
        if delta < opts.tolerance {
            break;
        }
    }

    PageRankResult { ranks, iterations }
}

/// Local Clustering Coefficient.
pub fn lcc_with_index(index: &AdjacencyIndex) -> LccResult {
    // Collect all nodes.
    let mut all_nodes: Vec<String> = Vec::new();
    let mut node_set: HashSet<&str> = HashSet::new();

    for node_id in &index.nodes {
        if node_set.insert(node_id.as_str()) {
            all_nodes.push(node_id.clone());
        }
    }
    for (src, edges) in &index.outgoing {
        if node_set.insert(src.as_str()) {
            all_nodes.push(src.clone());
        }
        for (dst, _, _) in edges {
            if node_set.insert(dst.as_str()) {
                all_nodes.push(dst.clone());
            }
        }
    }
    for (dst, edges) in &index.incoming {
        if node_set.insert(dst.as_str()) {
            all_nodes.push(dst.clone());
        }
        for (src, _, _) in edges {
            if node_set.insert(src.as_str()) {
                all_nodes.push(src.clone());
            }
        }
    }

    // Build an edge set for O(1) connectivity checks (undirected).
    let mut edge_set: HashSet<(&str, &str)> = HashSet::new();
    for (src, edges) in &index.outgoing {
        for (dst, _, _) in edges {
            edge_set.insert((src.as_str(), dst.as_str()));
        }
    }

    let mut coefficients = HashMap::new();

    for node_id in &all_nodes {
        // Get undirected neighbors (union of outgoing and incoming).
        // Exclude self-loops: a node is not its own neighbor for LCC purposes.
        let mut neighbors: HashSet<&str> = HashSet::new();
        for (dst, _) in index.outgoing_neighbor_ids(node_id, None) {
            if dst != node_id.as_str() {
                neighbors.insert(dst);
            }
        }
        for (src, _) in index.incoming_neighbor_ids(node_id, None) {
            if src != node_id.as_str() {
                neighbors.insert(src);
            }
        }

        let deg = neighbors.len();
        if deg < 2 {
            coefficients.insert(node_id.clone(), 0.0);
            continue;
        }

        // Count edges between neighbors (triangles).
        let neighbor_vec: Vec<&str> = neighbors.iter().copied().collect();
        let mut triangles = 0u64;

        for i in 0..neighbor_vec.len() {
            for j in (i + 1)..neighbor_vec.len() {
                let u = neighbor_vec[i];
                let w = neighbor_vec[j];
                // Check if u and w are connected (either direction).
                if edge_set.contains(&(u, w)) || edge_set.contains(&(w, u)) {
                    triangles += 1;
                }
            }
        }

        let max_edges = (deg * (deg - 1)) / 2;
        let coeff = triangles as f64 / max_edges as f64;
        coefficients.insert(node_id.clone(), coeff);
    }

    LccResult { coefficients }
}

/// Single-Source Shortest Path using Dijkstra.
pub fn sssp_with_index(index: &AdjacencyIndex, source: &str, opts: &SsspOptions) -> SsspResult {
    let mut distances: HashMap<String, f64> = HashMap::new();
    // Min-heap: (distance, node_id)
    let mut heap: BinaryHeap<Reverse<(OrdF64, String)>> = BinaryHeap::new();

    distances.insert(source.to_string(), 0.0);
    heap.push(Reverse((OrdF64(0.0), source.to_string())));

    while let Some(Reverse((OrdF64(dist), node))) = heap.pop() {
        // Skip if we already found a shorter path.
        if let Some(&known) = distances.get(&node) {
            if dist > known {
                continue;
            }
        }

        // Iterate weighted neighbors based on direction.
        match opts.direction {
            Direction::Outgoing => {
                for (dst, weight) in index.outgoing_weighted(&node) {
                    let new_dist = dist + weight;
                    let current = distances.get(dst).copied().unwrap_or(f64::INFINITY);
                    if new_dist < current {
                        distances.insert(dst.to_string(), new_dist);
                        heap.push(Reverse((OrdF64(new_dist), dst.to_string())));
                    }
                }
            }
            Direction::Incoming => {
                if let Some(incoming) = index.incoming.get(node.as_str()) {
                    for (src, _, data) in incoming {
                        let new_dist = dist + data.weight;
                        let current = distances
                            .get(src.as_str())
                            .copied()
                            .unwrap_or(f64::INFINITY);
                        if new_dist < current {
                            distances.insert(src.clone(), new_dist);
                            heap.push(Reverse((OrdF64(new_dist), src.clone())));
                        }
                    }
                }
            }
            Direction::Both => {
                for (dst, weight) in index.outgoing_weighted(&node) {
                    let new_dist = dist + weight;
                    let current = distances.get(dst).copied().unwrap_or(f64::INFINITY);
                    if new_dist < current {
                        distances.insert(dst.to_string(), new_dist);
                        heap.push(Reverse((OrdF64(new_dist), dst.to_string())));
                    }
                }
                if let Some(incoming) = index.incoming.get(node.as_str()) {
                    for (src, _, data) in incoming {
                        let new_dist = dist + data.weight;
                        let current = distances
                            .get(src.as_str())
                            .copied()
                            .unwrap_or(f64::INFINITY);
                        if new_dist < current {
                            distances.insert(src.clone(), new_dist);
                            heap.push(Reverse((OrdF64(new_dist), src.clone())));
                        }
                    }
                }
            }
        }
    }

    SsspResult { distances }
}

/// Wrapper for f64 that implements Ord (required for BinaryHeap).
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EdgeData;

    fn make_index(edges: &[(&str, &str, f64)]) -> AdjacencyIndex {
        let mut idx = AdjacencyIndex::new();
        for &(src, dst, weight) in edges {
            idx.add_node(src);
            idx.add_node(dst);
            idx.add_edge(
                src,
                dst,
                "E",
                EdgeData {
                    weight,
                    properties: None,
                },
            );
        }
        idx
    }

    fn make_unweighted_index(edges: &[(&str, &str)]) -> AdjacencyIndex {
        let mut idx = AdjacencyIndex::new();
        for &(src, dst) in edges {
            idx.add_node(src);
            idx.add_node(dst);
            idx.add_edge(src, dst, "E", EdgeData::default());
        }
        idx
    }

    // =========================================================================
    // WCC
    // =========================================================================

    #[test]
    fn wcc_disconnected_components() {
        let idx = make_unweighted_index(&[("A", "B"), ("C", "D")]);
        let result = wcc_with_index(&idx);
        // A and B should be in the same component
        assert_eq!(result.components["A"], result.components["B"]);
        // C and D should be in the same component
        assert_eq!(result.components["C"], result.components["D"]);
        // A and C should be in different components
        assert_ne!(result.components["A"], result.components["C"]);
    }

    #[test]
    fn wcc_single_component() {
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C"), ("C", "D")]);
        let result = wcc_with_index(&idx);
        let comp = result.components["A"];
        assert_eq!(result.components["B"], comp);
        assert_eq!(result.components["C"], comp);
        assert_eq!(result.components["D"], comp);
    }

    #[test]
    fn wcc_isolated_nodes() {
        let mut idx = AdjacencyIndex::new();
        idx.add_node("A");
        idx.add_node("B");
        idx.add_node("C");
        let result = wcc_with_index(&idx);
        // Each isolated node is its own component.
        assert_eq!(result.components.len(), 3);
        let values: HashSet<u64> = result.components.values().copied().collect();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn wcc_self_loop() {
        let idx = make_unweighted_index(&[("A", "A"), ("B", "C")]);
        let result = wcc_with_index(&idx);
        assert_ne!(result.components["A"], result.components["B"]);
        assert_eq!(result.components["B"], result.components["C"]);
    }

    #[test]
    fn wcc_empty_graph() {
        let idx = AdjacencyIndex::new();
        let result = wcc_with_index(&idx);
        assert!(result.components.is_empty());
    }

    // =========================================================================
    // CDLP
    // =========================================================================

    #[test]
    fn cdlp_two_communities() {
        // Two cliques connected by a single edge
        let idx = make_unweighted_index(&[
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
            ("C", "D"), // bridge
        ]);
        let result = cdlp_with_index(&idx, &CdlpOptions::default());
        // Nodes in same clique should have the same label
        assert_eq!(result.labels["A"], result.labels["B"]);
        assert_eq!(result.labels["A"], result.labels["C"]);
        assert_eq!(result.labels["D"], result.labels["E"]);
        assert_eq!(result.labels["D"], result.labels["F"]);
    }

    #[test]
    fn cdlp_single_node() {
        let mut idx = AdjacencyIndex::new();
        idx.add_node("A");
        let result = cdlp_with_index(&idx, &CdlpOptions::default());
        assert_eq!(result.labels.len(), 1);
        assert!(result.labels.contains_key("A"));
    }

    #[test]
    fn cdlp_convergence_stops_early() {
        // A fully connected triangle should converge quickly — all labels become the same.
        let idx = make_unweighted_index(&[
            ("A", "B"),
            ("B", "A"),
            ("A", "C"),
            ("C", "A"),
            ("B", "C"),
            ("C", "B"),
        ]);
        // Run with high max_iterations but convergence should happen in ~2.
        let result = cdlp_with_index(
            &idx,
            &CdlpOptions {
                max_iterations: 100,
                ..Default::default()
            },
        );
        // All nodes should have the same label after convergence.
        assert_eq!(result.labels["A"], result.labels["B"]);
        assert_eq!(result.labels["B"], result.labels["C"]);
    }

    #[test]
    fn cdlp_directed_outgoing_only() {
        // A → B, A → C: with Direction::Outgoing, B and C have no outgoing neighbors,
        // so their labels stay fixed. A sees B and C's labels.
        let idx = make_unweighted_index(&[("A", "B"), ("A", "C")]);
        let result = cdlp_with_index(
            &idx,
            &CdlpOptions {
                max_iterations: 10,
                direction: Direction::Outgoing,
            },
        );
        // B and C should keep their original labels (no outgoing neighbors).
        assert_ne!(result.labels["B"], result.labels["C"]);
        // A should adopt the smaller of B's or C's label.
        let min_label = result.labels["B"].min(result.labels["C"]);
        assert_eq!(result.labels["A"], min_label);
    }

    // =========================================================================
    // PageRank
    // =========================================================================

    #[test]
    fn pagerank_simple_chain() {
        // A → B → C
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C")]);
        let result = pagerank_with_index(&idx, &PageRankOptions::default());
        // C should have highest rank (receives from B, which receives from A)
        assert!(result.ranks["C"] > result.ranks["B"]);
        assert!(result.ranks["B"] > result.ranks["A"]);
    }

    #[test]
    fn pagerank_star_graph() {
        // A → B, A → C, A → D (hub pattern)
        let idx = make_unweighted_index(&[("A", "B"), ("A", "C"), ("A", "D")]);
        let result = pagerank_with_index(&idx, &PageRankOptions::default());
        // B, C, D should have roughly equal rank
        assert!((result.ranks["B"] - result.ranks["C"]).abs() < 1e-10);
        assert!((result.ranks["B"] - result.ranks["D"]).abs() < 1e-10);
    }

    #[test]
    fn pagerank_sum_approximately_one() {
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C"), ("C", "A")]);
        let result = pagerank_with_index(&idx, &PageRankOptions::default());
        let sum: f64 = result.ranks.values().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {}", sum);
    }

    #[test]
    fn pagerank_damping_effect() {
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C"), ("C", "A")]);
        let result_high = pagerank_with_index(
            &idx,
            &PageRankOptions {
                damping: 0.99,
                ..Default::default()
            },
        );
        let result_low = pagerank_with_index(
            &idx,
            &PageRankOptions {
                damping: 0.5,
                ..Default::default()
            },
        );
        // With low damping, ranks should be more uniform
        let high_range = result_high.ranks.values().cloned().fold(0.0f64, f64::max)
            - result_high
                .ranks
                .values()
                .cloned()
                .fold(f64::INFINITY, f64::min);
        let low_range = result_low.ranks.values().cloned().fold(0.0f64, f64::max)
            - result_low
                .ranks
                .values()
                .cloned()
                .fold(f64::INFINITY, f64::min);
        assert!(low_range <= high_range + 1e-10);
    }

    #[test]
    fn pagerank_empty_graph() {
        let idx = AdjacencyIndex::new();
        let result = pagerank_with_index(&idx, &PageRankOptions::default());
        assert!(result.ranks.is_empty());
        assert_eq!(result.iterations, 0);
    }

    #[test]
    fn pagerank_dangling_nodes_preserve_mass() {
        // A → B, B → C: C is a dangling node (no outgoing edges).
        // Without dangling node handling, rank mass leaks and sum != 1.0.
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C")]);
        let result = pagerank_with_index(&idx, &PageRankOptions::default());
        let sum: f64 = result.ranks.values().sum();
        assert!(
            (sum - 1.0).abs() < 1e-6,
            "sum was {} (dangling node mass leaked)",
            sum
        );
    }

    #[test]
    fn pagerank_all_dangling() {
        // Three isolated nodes — all are dangling. Ranks should remain uniform.
        let mut idx = AdjacencyIndex::new();
        idx.add_node("A");
        idx.add_node("B");
        idx.add_node("C");
        let result = pagerank_with_index(&idx, &PageRankOptions::default());
        let sum: f64 = result.ranks.values().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {}", sum);
        // All ranks should be equal (1/3 each).
        for &rank in result.ranks.values() {
            assert!((rank - 1.0 / 3.0).abs() < 1e-6, "rank {} != 1/3", rank);
        }
    }

    // =========================================================================
    // LCC
    // =========================================================================

    #[test]
    fn lcc_triangle() {
        // A → B, B → C, C → A (triangle)
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C"), ("C", "A")]);
        let result = lcc_with_index(&idx);
        // All nodes have degree 2 (undirected) and 1 triangle → coefficient = 1.0
        assert!((result.coefficients["A"] - 1.0).abs() < 1e-10);
        assert!((result.coefficients["B"] - 1.0).abs() < 1e-10);
        assert!((result.coefficients["C"] - 1.0).abs() < 1e-10);
    }

    #[test]
    fn lcc_star() {
        // A → B, A → C, A → D (star from A)
        let idx = make_unweighted_index(&[("A", "B"), ("A", "C"), ("A", "D")]);
        let result = lcc_with_index(&idx);
        // A has degree 3, no edges between B, C, D → coefficient = 0.0
        assert!((result.coefficients["A"] - 0.0).abs() < 1e-10);
        // B, C, D have degree 1 → coefficient = 0.0
        assert!((result.coefficients["B"] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn lcc_square() {
        // A → B, B → C, C → D, D → A (square)
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C"), ("C", "D"), ("D", "A")]);
        let result = lcc_with_index(&idx);
        // Each node has degree 2 (undirected), no triangles → coefficient = 0.0
        assert!((result.coefficients["A"] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn lcc_self_loop_ignored() {
        // A → A (self-loop), A → B, A → C, B → C
        // Self-loop should NOT inflate A's degree or create false triangles.
        let idx = make_unweighted_index(&[("A", "A"), ("A", "B"), ("A", "C"), ("B", "C")]);
        let result = lcc_with_index(&idx);
        // A's neighbors (excluding self): {B, C}. B-C edge exists → 1 triangle / 1 possible = 1.0
        assert!(
            (result.coefficients["A"] - 1.0).abs() < 1e-10,
            "A's LCC = {} (expected 1.0, self-loop should be excluded)",
            result.coefficients["A"]
        );
    }

    #[test]
    fn lcc_isolated_nodes() {
        let mut idx = AdjacencyIndex::new();
        idx.add_node("A");
        idx.add_node("B");
        let result = lcc_with_index(&idx);
        // Isolated nodes have degree 0 → coefficient = 0.0
        assert!((result.coefficients["A"] - 0.0).abs() < 1e-10);
        assert!((result.coefficients["B"] - 0.0).abs() < 1e-10);
    }

    // =========================================================================
    // SSSP
    // =========================================================================

    #[test]
    fn sssp_weighted_path() {
        // A --1--> B --2--> C, A --10--> C
        let idx = make_index(&[("A", "B", 1.0), ("B", "C", 2.0), ("A", "C", 10.0)]);
        let result = sssp_with_index(&idx, "A", &SsspOptions::default());
        assert!((result.distances["A"] - 0.0).abs() < 1e-10);
        assert!((result.distances["B"] - 1.0).abs() < 1e-10);
        assert!((result.distances["C"] - 3.0).abs() < 1e-10); // via A→B→C
    }

    #[test]
    fn sssp_unreachable_node() {
        let mut idx = make_unweighted_index(&[("A", "B")]);
        idx.add_node("C"); // C is isolated
        let result = sssp_with_index(&idx, "A", &SsspOptions::default());
        assert!(result.distances.contains_key("A"));
        assert!(result.distances.contains_key("B"));
        assert!(!result.distances.contains_key("C"));
    }

    #[test]
    fn sssp_source_to_itself() {
        let idx = make_unweighted_index(&[("A", "B")]);
        let result = sssp_with_index(&idx, "A", &SsspOptions::default());
        assert!((result.distances["A"] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn sssp_zero_weight_edges() {
        let idx = make_index(&[("A", "B", 0.0), ("B", "C", 0.0)]);
        let result = sssp_with_index(&idx, "A", &SsspOptions::default());
        assert!((result.distances["A"] - 0.0).abs() < 1e-10);
        assert!((result.distances["B"] - 0.0).abs() < 1e-10);
        assert!((result.distances["C"] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn sssp_nonexistent_source() {
        let idx = make_unweighted_index(&[("A", "B")]);
        let result = sssp_with_index(&idx, "Z", &SsspOptions::default());
        // Only the source should be in the result with distance 0.
        assert_eq!(result.distances.len(), 1);
        assert!((result.distances["Z"] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn sssp_direction_both() {
        // A → B → C: with Direction::Both, C can also reach A via B.
        let idx = make_index(&[("A", "B", 1.0), ("B", "C", 2.0)]);
        let result = sssp_with_index(
            &idx,
            "C",
            &SsspOptions {
                direction: Direction::Both,
            },
        );
        // C→B (reverse edge, weight 2.0), C→B→A (reverse edges, weight 2.0 + 1.0)
        assert!((result.distances["C"] - 0.0).abs() < 1e-10);
        assert!((result.distances["B"] - 2.0).abs() < 1e-10);
        assert!((result.distances["A"] - 3.0).abs() < 1e-10);
    }

    #[test]
    fn sssp_direction_incoming() {
        // A → B → C: with Direction::Incoming from C, traverse reverse edges.
        let idx = make_index(&[("A", "B", 1.0), ("B", "C", 2.0)]);
        let result = sssp_with_index(
            &idx,
            "C",
            &SsspOptions {
                direction: Direction::Incoming,
            },
        );
        assert!((result.distances["C"] - 0.0).abs() < 1e-10);
        assert!((result.distances["B"] - 2.0).abs() < 1e-10);
        assert!((result.distances["A"] - 3.0).abs() < 1e-10);
    }

    // =========================================================================
    // Integration: GraphStore-level tests
    // =========================================================================

    use std::sync::Arc;
    use strata_engine::Database;

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::cache().unwrap();
        let gs = GraphStore::new(db.clone());
        (db, gs)
    }

    fn default_branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    #[test]
    fn graph_store_wcc() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        let result = gs.wcc(b, "g").unwrap();
        assert_eq!(result.components["A"], result.components["B"]);
        assert_ne!(result.components["A"], result.components["C"]);
    }

    #[test]
    fn graph_store_pagerank() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        let result = gs.pagerank(b, "g", PageRankOptions::default()).unwrap();
        assert!(result.ranks["B"] > result.ranks["A"]);
    }

    #[test]
    fn graph_store_sssp() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();
        gs.add_edge(
            b,
            "g",
            "A",
            "B",
            "E",
            EdgeData {
                weight: 2.0,
                properties: None,
            },
        )
        .unwrap();
        gs.add_edge(
            b,
            "g",
            "B",
            "C",
            "E",
            EdgeData {
                weight: 3.0,
                properties: None,
            },
        )
        .unwrap();
        let result = gs.sssp(b, "g", "A", SsspOptions::default()).unwrap();
        assert!((result.distances["A"] - 0.0).abs() < 1e-10);
        assert!((result.distances["B"] - 2.0).abs() < 1e-10);
        assert!((result.distances["C"] - 5.0).abs() < 1e-10);
    }
}
