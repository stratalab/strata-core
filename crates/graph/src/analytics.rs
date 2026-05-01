//! Graph analytics algorithms: WCC, CDLP, PageRank, LCC, SSSP.
//!
//! All algorithms follow the same pattern as BFS: build an in-memory
//! `AdjacencyIndex` from KV storage, then run the algorithm in-memory.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use strata_core::BranchId;
use strata_engine::{StrataError, StrataResult};

use super::adjacency::AdjacencyIndex;
use super::types::*;
use super::GraphStore;

impl GraphStore {
    /// Weakly Connected Components using union-find.
    pub fn wcc(&self, branch_id: BranchId, space: &str, graph: &str) -> StrataResult<WccResult> {
        let index = self.build_full_adjacency_index_atomic(branch_id, space, graph)?;
        Ok(wcc_with_index(&index))
    }

    /// Community Detection via Label Propagation.
    pub fn cdlp(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        opts: CdlpOptions,
    ) -> StrataResult<CdlpResult> {
        let index = self.build_full_adjacency_index_atomic(branch_id, space, graph)?;
        Ok(cdlp_with_index(&index, &opts))
    }

    /// PageRank iterative importance scoring.
    pub fn pagerank(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        opts: PageRankOptions,
    ) -> StrataResult<PageRankResult> {
        let index = self.build_full_adjacency_index_atomic(branch_id, space, graph)?;
        Ok(pagerank_with_index(&index, &opts))
    }

    /// Local Clustering Coefficient.
    pub fn lcc(&self, branch_id: BranchId, space: &str, graph: &str) -> StrataResult<LccResult> {
        let index = self.build_full_adjacency_index_atomic(branch_id, space, graph)?;
        Ok(lcc_with_index(&index))
    }

    /// Single-Source Shortest Path (Dijkstra).
    pub fn sssp(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        source: &str,
        opts: SsspOptions,
    ) -> StrataResult<SsspResult> {
        let index = self.build_adjacency_index(branch_id, space, graph)?;
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

    // Sort for deterministic iteration: floating-point sums in the rank update
    // and L1 convergence check below are not associative, so the iteration order
    // affects bitwise output. Matches `wcc_with_index` and `cdlp_with_index`.
    all_nodes.sort();

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

/// Personalized PageRank (HippoRAG-style).
///
/// Runs power iteration with a per-node teleport vector. Unlike the standard
/// PageRank, both the teleport term and the dangling-mass redistribution are
/// proportional to the personalization weights — so probability mass
/// concentrates around the seed nodes rather than diffusing uniformly.
///
/// The `personalization` map is L1-normalized internally, so callers can pass
/// raw seed weights (e.g., BM25 scores) without pre-normalizing. Entries
/// whose keys are not in the graph are silently ignored (they cannot anchor
/// any walk), so stale anchors from a prior snapshot do not leak probability
/// mass. Nodes absent from the map receive zero teleport mass.
///
/// Returns `Err(StrataError::InvalidInput)` if:
///   - `personalization` is empty,
///   - any individual weight is negative, NaN, or non-finite (Infinity),
///   - no positive weight lands on a graph node.
///
/// All three cases are programmer bugs, not runtime conditions.
///
/// Reference: Gutierrez et al., "HippoRAG", NeurIPS 2024
/// (https://arxiv.org/abs/2405.14831).
pub fn pagerank_personalized_with_index(
    index: &AdjacencyIndex,
    opts: &PageRankOptions,
    personalization: &HashMap<String, f64>,
) -> StrataResult<PageRankResult> {
    if personalization.is_empty() {
        return Err(StrataError::invalid_input(
            "personalization vector is empty",
        ));
    }

    // Validate each individual weight up front: negative, NaN, or infinite
    // entries would silently produce negative or NaN ranks downstream.
    for v in personalization.values() {
        if !v.is_finite() || *v < 0.0 {
            return Err(StrataError::invalid_input(
                "personalization contains a negative, NaN, or infinite weight",
            ));
        }
    }

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

    // Sort for deterministic iteration: per-node personalization makes the
    // floating-point sums even more order-sensitive than uniform PageRank, so
    // the sort is load-bearing for bitwise-reproducible output.
    all_nodes.sort();

    let n = all_nodes.len();
    if n == 0 {
        return Ok(PageRankResult {
            ranks: HashMap::new(),
            iterations: 0,
        });
    }

    // L1-normalize the personalization vector over the intersection with the
    // graph. Filtering to graph nodes before normalizing preserves the mass
    // invariant (sum of ranks = 1) — a personalization entry whose key is not
    // in the graph would otherwise drain probability mass into a void.
    //
    // Single-pass: filter once into a Vec, then derive `total` and `normalized`
    // from it. Avoids drift between two separately-maintained filter predicates.
    let filtered: Vec<(&str, f64)> = personalization
        .iter()
        .filter_map(|(k, v)| {
            if node_set.contains(k.as_str()) {
                Some((k.as_str(), *v))
            } else {
                None
            }
        })
        .collect();
    let total: f64 = filtered.iter().map(|(_, v)| *v).sum();
    // `total` may be non-finite if the sum overflows even though each summand
    // is finite. Reject that case alongside the trivial zero case.
    if !total.is_finite() || total <= 0.0 {
        return Err(StrataError::invalid_input(
            "personalization has no positive weight on any graph node",
        ));
    }
    let normalized: HashMap<&str, f64> =
        filtered.into_iter().map(|(k, v)| (k, v / total)).collect();

    // Initialize ranks from the normalized personalization vector. Nodes
    // outside the personalization map start at 0.0. Total initial mass = 1.0.
    let mut ranks: HashMap<String, f64> = all_nodes
        .iter()
        .map(|id| {
            let r = normalized.get(id.as_str()).copied().unwrap_or(0.0);
            (id.clone(), r)
        })
        .collect();

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
    let mut iterations = 0;

    for _ in 0..opts.max_iterations {
        iterations += 1;

        // Compute dangling node mass: sum of ranks for nodes with no outgoing edges.
        let dangling_sum: f64 = all_nodes
            .iter()
            .filter(|id| *out_degree.get(id.as_str()).unwrap_or(&0) == 0)
            .map(|id| ranks[id])
            .sum();

        let mut new_ranks: HashMap<String, f64> = HashMap::with_capacity(n);

        for node_id in &all_nodes {
            // Per-node teleport weight (HippoRAG).
            let p = normalized.get(node_id.as_str()).copied().unwrap_or(0.0);
            let base = (1.0 - d) * p;
            // Per-node dangling redistribution (HippoRAG).
            let dangling_contrib = d * dangling_sum * p;

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

    Ok(PageRankResult { ranks, iterations })
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
    // Personalized PageRank (HippoRAG)
    // =========================================================================

    #[test]
    fn ppr_concentrates_around_seeds() {
        // Chain A→B→C with seed on A. Closed-form steady state:
        //   rank(A) = (1-d) / (1 - d^3)
        //   rank(B) = d * rank(A)
        //   rank(C) = d^2 * rank(A)
        // With default damping 0.85: A≈0.389, B≈0.330, C≈0.281 — strictly
        // monotonically decreasing. Note we bump max_iterations above the
        // default because the "wave" of seed mass walking the chain oscillates
        // for many iterations before the dangling feedback loop settles.
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C")]);
        let mut pers: HashMap<String, f64> = HashMap::new();
        pers.insert("A".to_string(), 1.0);

        let opts = PageRankOptions {
            max_iterations: 200,
            tolerance: 1e-8,
            ..PageRankOptions::default()
        };
        let result = pagerank_personalized_with_index(&idx, &opts, &pers).unwrap();

        // Monotonic decay along the chain: the seed dominates and rank falls
        // off with each hop — the core PPR guarantee.
        assert!(
            result.ranks["A"] > result.ranks["B"],
            "rank(A)={} should exceed rank(B)={}",
            result.ranks["A"],
            result.ranks["B"]
        );
        assert!(
            result.ranks["B"] > result.ranks["C"],
            "rank(B)={} should exceed rank(C)={}",
            result.ranks["B"],
            result.ranks["C"]
        );
        // Closed-form sanity check — within 1e-4 of the analytical value.
        let d: f64 = 0.85;
        let expected_a = (1.0 - d) / (1.0 - d.powi(3));
        assert!(
            (result.ranks["A"] - expected_a).abs() < 1e-4,
            "rank(A)={} vs closed-form {}",
            result.ranks["A"],
            expected_a
        );
    }

    #[test]
    fn ppr_two_seeds_balance() {
        // Two disconnected, structurally identical bidirectional chains:
        //   Left:  A↔B↔C
        //   Right: X↔Y↔Z
        // Seed each endpoint equally (A=0.5, X=0.5). Because the components
        // are isomorphic, the seeds are mirror images, and the algorithm is
        // deterministic Jacobi iteration (reads only previous-iteration values),
        // symmetric inputs must produce *bitwise-equal* ranks. Asserting
        // `to_bits()` equality catches subtle drift that a numeric tolerance
        // would hide.
        let idx = make_unweighted_index(&[
            ("A", "B"),
            ("B", "A"),
            ("B", "C"),
            ("C", "B"),
            ("X", "Y"),
            ("Y", "X"),
            ("Y", "Z"),
            ("Z", "Y"),
        ]);
        let mut pers: HashMap<String, f64> = HashMap::new();
        pers.insert("A".to_string(), 0.5);
        pers.insert("X".to_string(), 0.5);

        let result =
            pagerank_personalized_with_index(&idx, &PageRankOptions::default(), &pers).unwrap();

        for (left, right) in [("A", "X"), ("B", "Y"), ("C", "Z")] {
            assert_eq!(
                result.ranks[left].to_bits(),
                result.ranks[right].to_bits(),
                "rank({})={} vs rank({})={} (expected bitwise equal)",
                left,
                result.ranks[left],
                right,
                result.ranks[right]
            );
        }
    }

    #[test]
    fn ppr_dangling_mass_redistributes_via_personalization() {
        // Core HippoRAG correctness test.
        //
        // Graph: A→B, plus two isolated dangling nodes C and D.
        // With all personalization on A, dangling mass from C and D must
        // flow back to A (via per-node redistribution), NOT spread uniformly
        // across {A, B, C, D} the way standard PageRank would.
        let mut idx = make_unweighted_index(&[("A", "B")]);
        idx.add_node("C");
        idx.add_node("D");

        let mut pers: HashMap<String, f64> = HashMap::new();
        pers.insert("A".to_string(), 1.0);

        let opts = PageRankOptions::default();
        let ppr = pagerank_personalized_with_index(&idx, &opts, &pers).unwrap();

        // Baseline: standard (uniform-redistribution) PageRank on the same graph.
        // HippoRAG's per-node redistribution should concentrate significantly
        // more mass on A than the uniform variant does.
        let uniform = pagerank_with_index(&idx, &opts);

        assert!(
            ppr.ranks["A"] > uniform.ranks["A"] + 0.15,
            "PPR rank(A)={} should exceed uniform rank(A)={} by >0.15",
            ppr.ranks["A"],
            uniform.ranks["A"]
        );
        // And PPR rank(A) should be a clear majority of mass.
        assert!(
            ppr.ranks["A"] > 0.5,
            "PPR rank(A)={} should exceed 0.5 (seed dominates)",
            ppr.ranks["A"]
        );
        // C and D have no teleport and no incoming edges — they never receive
        // any mass (neither from the personalization vector nor from the
        // dangling redistribution, since their own `p` is zero). Assert they
        // stay at exactly zero throughout iteration.
        assert_eq!(ppr.ranks["C"], 0.0, "rank(C) should be exactly 0");
        assert_eq!(ppr.ranks["D"], 0.0, "rank(D) should be exactly 0");
    }

    #[test]
    fn ppr_zero_iterations_returns_initial_state() {
        // With max_iterations=0 the iteration loop never runs, so the result
        // must be the initial personalization-weighted state: ranks equal
        // the (normalized) personalization weights, and iterations == 0.
        // This guards against regressions in the counter initialization or
        // the zero-iteration early exit path.
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C")]);
        let mut pers: HashMap<String, f64> = HashMap::new();
        pers.insert("A".to_string(), 0.75);
        pers.insert("B".to_string(), 0.25);

        let opts = PageRankOptions {
            max_iterations: 0,
            ..PageRankOptions::default()
        };
        let result = pagerank_personalized_with_index(&idx, &opts, &pers).unwrap();

        assert_eq!(result.iterations, 0);
        // Initial ranks come straight from the normalized personalization
        // vector (already L1 — total is 1.0) and nodes not in the map get 0.
        assert_eq!(result.ranks["A"].to_bits(), 0.75_f64.to_bits());
        assert_eq!(result.ranks["B"].to_bits(), 0.25_f64.to_bits());
        assert_eq!(result.ranks["C"].to_bits(), 0.0_f64.to_bits());
        // Mass invariant holds even at iteration 0.
        let sum: f64 = result.ranks.values().sum();
        assert!((sum - 1.0).abs() < 1e-12, "sum was {}", sum);
    }

    #[test]
    fn ppr_personalization_preserves_mass() {
        // Chain A→B→C (C is dangling). With any personalization the total
        // rank mass must stay at 1.0 regardless of damping.
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C")]);
        let mut pers: HashMap<String, f64> = HashMap::new();
        pers.insert("A".to_string(), 0.6);
        pers.insert("B".to_string(), 0.4);

        for damping in [0.3_f64, 0.5, 0.85] {
            let opts = PageRankOptions {
                damping,
                ..PageRankOptions::default()
            };
            let result = pagerank_personalized_with_index(&idx, &opts, &pers).unwrap();
            let sum: f64 = result.ranks.values().sum();
            assert!(
                (sum - 1.0).abs() < 1e-6,
                "damping={} sum={} (mass leaked)",
                damping,
                sum
            );
        }
    }

    #[test]
    fn ppr_empty_personalization_errors() {
        let idx = make_unweighted_index(&[("A", "B")]);
        let opts = PageRankOptions::default();

        // Empty map → error.
        let empty: HashMap<String, f64> = HashMap::new();
        let result = pagerank_personalized_with_index(&idx, &opts, &empty);
        assert!(
            matches!(result, Err(StrataError::InvalidInput { .. })),
            "expected InvalidInput for empty personalization, got {:?}",
            result
        );

        // Non-empty but all-zero weights → error (no positive weight on any node).
        let mut all_zero: HashMap<String, f64> = HashMap::new();
        all_zero.insert("A".to_string(), 0.0);
        all_zero.insert("B".to_string(), 0.0);
        let result = pagerank_personalized_with_index(&idx, &opts, &all_zero);
        assert!(
            matches!(result, Err(StrataError::InvalidInput { .. })),
            "expected InvalidInput for all-zero personalization, got {:?}",
            result
        );
    }

    #[test]
    fn ppr_rejects_invalid_weights() {
        // Negative, NaN, and infinite individual weights must be rejected
        // up front rather than silently producing bad ranks downstream.
        let idx = make_unweighted_index(&[("A", "B")]);
        let opts = PageRankOptions::default();

        // Negative weight.
        let mut negative: HashMap<String, f64> = HashMap::new();
        negative.insert("A".to_string(), 5.0);
        negative.insert("B".to_string(), -2.0);
        let result = pagerank_personalized_with_index(&idx, &opts, &negative);
        assert!(
            matches!(result, Err(StrataError::InvalidInput { .. })),
            "expected InvalidInput for negative weight, got {:?}",
            result
        );

        // NaN weight.
        let mut nan: HashMap<String, f64> = HashMap::new();
        nan.insert("A".to_string(), f64::NAN);
        let result = pagerank_personalized_with_index(&idx, &opts, &nan);
        assert!(
            matches!(result, Err(StrataError::InvalidInput { .. })),
            "expected InvalidInput for NaN weight, got {:?}",
            result
        );

        // Infinite weight.
        let mut inf: HashMap<String, f64> = HashMap::new();
        inf.insert("A".to_string(), f64::INFINITY);
        let result = pagerank_personalized_with_index(&idx, &opts, &inf);
        assert!(
            matches!(result, Err(StrataError::InvalidInput { .. })),
            "expected InvalidInput for infinite weight, got {:?}",
            result
        );
    }

    #[test]
    fn ppr_ignores_non_graph_keys_and_preserves_mass() {
        // Personalization containing a key that isn't a graph node (e.g., a
        // stale BM25 anchor from a prior snapshot) must be filtered out
        // before normalization. Otherwise the mass invariant (sum=1) breaks
        // because probability is assigned to a non-existent node and lost.
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C")]);
        let opts = PageRankOptions::default();

        let mut pers: HashMap<String, f64> = HashMap::new();
        pers.insert("A".to_string(), 3.0);
        pers.insert("GHOST".to_string(), 2.0); // not in the graph
        let result = pagerank_personalized_with_index(&idx, &opts, &pers).unwrap();

        // Mass invariant must hold: total rank = 1.0 despite the GHOST entry.
        let sum: f64 = result.ranks.values().sum();
        assert!(
            (sum - 1.0).abs() < 1e-6,
            "mass leak from non-graph personalization key: sum={}",
            sum
        );

        // The result should be bitwise-identical to passing only {A: 1.0}
        // (the only surviving entry after filtering, renormalized to 1.0).
        let mut a_only: HashMap<String, f64> = HashMap::new();
        a_only.insert("A".to_string(), 1.0);
        let a_only_result = pagerank_personalized_with_index(&idx, &opts, &a_only).unwrap();
        for node in ["A", "B", "C"] {
            assert_eq!(
                result.ranks[node].to_bits(),
                a_only_result.ranks[node].to_bits(),
                "bitwise mismatch at {}: with-ghost={} vs a-only={}",
                node,
                result.ranks[node],
                a_only_result.ranks[node]
            );
        }

        // If ALL personalization keys are non-graph nodes, that's an error.
        let mut all_ghosts: HashMap<String, f64> = HashMap::new();
        all_ghosts.insert("GHOST1".to_string(), 1.0);
        all_ghosts.insert("GHOST2".to_string(), 2.0);
        let result = pagerank_personalized_with_index(&idx, &opts, &all_ghosts);
        assert!(
            matches!(result, Err(StrataError::InvalidInput { .. })),
            "expected InvalidInput when all personalization keys are non-graph, got {:?}",
            result
        );
    }

    #[test]
    fn ppr_normalizes_personalization() {
        // Unnormalized raw weights (total=8.0) should produce bitwise-identical
        // output to pre-normalized {A: 0.625, B: 0.375}. This is the core
        // guarantee: callers can pass raw BM25-style scores without pre-
        // normalizing, and the function handles it internally.
        let idx = make_unweighted_index(&[("A", "B"), ("B", "C")]);

        let mut raw: HashMap<String, f64> = HashMap::new();
        raw.insert("A".to_string(), 5.0);
        raw.insert("B".to_string(), 3.0);

        let mut normalized: HashMap<String, f64> = HashMap::new();
        normalized.insert("A".to_string(), 0.625);
        normalized.insert("B".to_string(), 0.375);

        let opts = PageRankOptions::default();
        let raw_result = pagerank_personalized_with_index(&idx, &opts, &raw).unwrap();
        let norm_result = pagerank_personalized_with_index(&idx, &opts, &normalized).unwrap();

        // Mass invariant holds for the raw-weight call.
        let sum: f64 = raw_result.ranks.values().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {}", sum);

        // Raw and pre-normalized inputs must produce bitwise-identical
        // results: normalization is deterministic, iteration order is sorted,
        // and 5.0/8.0 == 0.625, 3.0/8.0 == 0.375 exactly in f64. Any drift
        // here would indicate a correctness bug in the normalization path.
        for node in ["A", "B", "C"] {
            assert_eq!(
                raw_result.ranks[node].to_bits(),
                norm_result.ranks[node].to_bits(),
                "bitwise mismatch at {}: raw={} norm={}",
                node,
                raw_result.ranks[node],
                norm_result.ranks[node]
            );
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
    use strata_engine::database::OpenSpec;
    use strata_engine::{Database, SearchSubsystem};

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
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
        gs.create_graph(b, "default", "g", None).unwrap();
        gs.add_node(b, "default", "g", "A", NodeData::default())
            .unwrap();
        gs.add_node(b, "default", "g", "B", NodeData::default())
            .unwrap();
        gs.add_node(b, "default", "g", "C", NodeData::default())
            .unwrap();
        gs.add_edge(b, "default", "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        let result = gs.wcc(b, "default", "g").unwrap();
        assert_eq!(result.components["A"], result.components["B"]);
        assert_ne!(result.components["A"], result.components["C"]);
    }

    #[test]
    fn graph_store_pagerank() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();
        gs.add_node(b, "default", "g", "A", NodeData::default())
            .unwrap();
        gs.add_node(b, "default", "g", "B", NodeData::default())
            .unwrap();
        gs.add_edge(b, "default", "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        let result = gs
            .pagerank(b, "default", "g", PageRankOptions::default())
            .unwrap();
        assert!(result.ranks["B"] > result.ranks["A"]);
    }

    #[test]
    fn graph_store_sssp() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();
        gs.add_node(b, "default", "g", "A", NodeData::default())
            .unwrap();
        gs.add_node(b, "default", "g", "B", NodeData::default())
            .unwrap();
        gs.add_node(b, "default", "g", "C", NodeData::default())
            .unwrap();
        gs.add_edge(
            b,
            "default",
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
            "default",
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
        let result = gs
            .sssp(b, "default", "g", "A", SsspOptions::default())
            .unwrap();
        assert!((result.distances["A"] - 0.0).abs() < 1e-10);
        assert!((result.distances["B"] - 2.0).abs() < 1e-10);
        assert!((result.distances["C"] - 5.0).abs() < 1e-10);
    }

    // =========================================================================
    // Determinism + snapshot consistency (regression guards for v0.4 PageRank)
    // =========================================================================

    /// PageRank must return bitwise-identical scores across repeated runs over
    /// the same graph. Without `all_nodes.sort()` in `pagerank_with_index`, the
    /// `HashSet`-driven iteration order can shift between runs, and the
    /// non-associative floating-point sums in the rank update + L1 convergence
    /// check produce different bits. This is a v0.4 prerequisite because
    /// personalization vectors will skew the sums even further.
    ///
    /// Uses a 64-node graph with long, varied string keys and 20 runs to make
    /// the test reliably catch the bug — small graphs with short ASCII keys
    /// can incidentally produce the same `HashSet` iteration order across
    /// different hash seeds because of low collision counts in tiny tables.
    #[test]
    fn pagerank_bitwise_deterministic_across_runs() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // 64 nodes with long, varied keys to defeat any "small set looks
        // stable under different seeds" coincidence in HashSet iteration.
        let node_ids: Vec<String> = (0..64)
            .map(|i| format!("node_with_a_long_label_to_diversify_hashing_{:04}", i))
            .collect();
        for id in &node_ids {
            gs.add_node(b, "default", "g", id, NodeData::default())
                .unwrap();
        }

        // Dense edges with deliberate dangling nodes. The dangling-mass
        // sum in `pagerank_with_index` iterates `all_nodes` in order, and
        // `dangling_sum: f64 = ... .sum()` is the sum that becomes
        // order-sensitive when the values come from differently-rounded
        // ranks. Without enough dangling nodes (or with all-uniform ranks)
        // the sum is the same regardless of order, hiding the bug.
        //
        // First half: each non-dangling node points to ~6 others.
        // Second half (last 16 nodes): no outgoing edges → dangling.
        let dangling_start = node_ids.len() - 16;
        for i in 0..dangling_start {
            let dsts = [
                (i + 1) % node_ids.len(),
                (i + 3) % node_ids.len(),
                (i + 7) % node_ids.len(),
                (i + 13) % node_ids.len(),
                (i + 23) % node_ids.len(),
                (i + 31) % node_ids.len(),
            ];
            for &j in &dsts {
                if i != j {
                    gs.add_edge(
                        b,
                        "default",
                        "g",
                        &node_ids[i],
                        &node_ids[j],
                        "E",
                        EdgeData::default(),
                    )
                    .unwrap();
                }
            }
        }

        let opts = PageRankOptions::default();
        let baseline = gs
            .pagerank(b, "default", "g", opts.clone())
            .expect("baseline pagerank run");

        // 19 more runs — across all 20 the bitwise output must be identical.
        // With the sort, this is mathematically guaranteed; without the sort,
        // at least one of these runs is overwhelmingly likely to land on a
        // different `HashSet` iteration order and produce different f64 bits.
        for run_idx in 1..20 {
            let result = gs
                .pagerank(b, "default", "g", opts.clone())
                .expect("repeat pagerank run");
            assert_eq!(
                result.ranks.len(),
                baseline.ranks.len(),
                "rank set size differs on run {}",
                run_idx
            );
            assert_eq!(
                result.iterations, baseline.iterations,
                "iteration count differs on run {}",
                run_idx
            );
            for (k, v_base) in &baseline.ranks {
                let v_now = result.ranks.get(k).expect("missing key in repeat run");
                // Use `to_bits()` for bitwise equality (canonical f64 idiom
                // that also satisfies `clippy::float_cmp`).
                assert_eq!(
                    v_base.to_bits(),
                    v_now.to_bits(),
                    "rank for {} not bitwise-equal on run {}: baseline={} now={}",
                    k,
                    run_idx,
                    v_base,
                    v_now
                );
            }
        }
    }

    /// Concurrent-safety smoke test for `pagerank()` under a busy writer.
    /// Asserts the mass invariant (sum ≈ 1.0, all ranks finite) holds across
    /// many concurrent runs. This is a regression guard for "pagerank does not
    /// crash, panic, deadlock, or produce invalid distributions when reads
    /// race against writes" — NOT a strict snapshot-isolation regression test.
    ///
    /// The atomic single-transaction read in `build_full_adjacency_index_atomic`
    /// is a structural correctness improvement (one snapshot per analytic
    /// invocation), but its absence does not break the mass invariant because
    /// `pagerank_with_index` picks up edge endpoints via its own iteration and
    /// renormalises by `n = all_nodes.len()`. Snapshot consistency is enforced
    /// structurally by the helper opening one txn; this test is the
    /// safety-net that proves "structurally consistent does not regress
    /// numerical correctness under contention."
    ///
    /// Not flaky: the assertion is a numerical invariant any correct PageRank
    /// must satisfy regardless of which valid graph state it observed.
    /// Iteration count is fixed; no wall-clock waits.
    #[test]
    fn pagerank_consistent_under_concurrent_writer() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;

        let (_db, gs) = setup();
        let gs = Arc::new(gs);
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // Seed with 20 nodes in a ring.
        for i in 0..20 {
            gs.add_node(b, "default", "g", &format!("n{}", i), NodeData::default())
                .unwrap();
        }
        for i in 0..20 {
            let s = format!("n{}", i);
            let d = format!("n{}", (i + 1) % 20);
            gs.add_edge(b, "default", "g", &s, &d, "E", EdgeData::default())
                .unwrap();
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Writer: continuously add+delete a temporary node with one outbound
        // edge. Generates contention against the reader's analytic transactions.
        // Note: this race does NOT directly drift the rank sum — pagerank
        // picks up edge endpoints via its own iteration of `outgoing`/
        // `incoming` and renormalises by `n = all_nodes.len()`, so a
        // slightly-blended graph view still produces a valid distribution.
        // The point of this writer is to stress the read path itself
        // (locking, OCC, scan-prefix iteration) under load.
        let gs_w = Arc::clone(&gs);
        let stop_w = Arc::clone(&stop);
        let writer = thread::spawn(move || {
            let b = default_branch();
            let mut counter = 0u64;
            while !stop_w.load(Ordering::Relaxed) {
                let node_id = format!("tmp{}", counter);
                // Best-effort: ignore OCC retries — we just want load.
                let _ = gs_w.add_node(b, "default", "g", &node_id, NodeData::default());
                let _ = gs_w.add_edge(b, "default", "g", "n0", &node_id, "E", EdgeData::default());
                let _ = gs_w.remove_node(b, "default", "g", &node_id);
                counter = counter.wrapping_add(1);
            }
        });

        // Reader: 500 pagerank invocations, asserting the sum invariant on
        // every run.
        let opts = PageRankOptions::default();
        for i in 0..500 {
            let result = gs
                .pagerank(b, "default", "g", opts.clone())
                .expect("pagerank should not error under concurrent writer");
            let sum: f64 = result.ranks.values().sum();
            assert!(
                (sum - 1.0).abs() < 1e-6,
                "pagerank sum drifted to {} on iteration {} (concurrent writer)",
                sum,
                i
            );
            for (k, v) in &result.ranks {
                assert!(v.is_finite(), "non-finite rank {} for {}", v, k);
            }
        }

        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }
}
