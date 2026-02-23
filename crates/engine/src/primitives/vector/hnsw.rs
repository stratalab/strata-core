//! HNSW (Hierarchical Navigable Small World) Index Backend
//!
//! O(log n) approximate nearest neighbor search built from scratch.
//!
//! ## Design Goals
//! - Incremental inserts (no rebuild required)
//! - Incremental deletes (mark-and-skip)
//! - Deterministic results (fixed RNG seed, sorted neighbor lists)
//! - Compatible with VectorIndexBackend trait
//!
//! ## Algorithm
//!
//! HNSW builds a multi-layer graph where:
//! - Layer 0 contains all nodes with up to 2*M connections each
//! - Higher layers contain a subset of nodes with up to M connections each
//! - Search starts from the top layer and greedily descends to layer 0
//! - At each layer, a beam search finds the ef closest neighbors
//!
//! ## Determinism
//!
//! - Fixed RNG seed + monotonic counter for level assignment
//! - BTreeMap for node storage (deterministic iteration)
//! - BTreeSet for neighbor lists (sorted)
//! - Tie-breaking: (score desc, VectorId asc)

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use crate::primitives::vector::backend::VectorIndexBackend;
use crate::primitives::vector::distance::compute_similarity;
use crate::primitives::vector::heap::VectorHeap;
use crate::primitives::vector::types::InlineMeta;
use crate::primitives::vector::{DistanceMetric, VectorConfig, VectorError, VectorId};

/// HNSW configuration parameters
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Max connections per layer (default: 16)
    pub m: usize,
    /// Build-time beam width (default: 200)
    pub ef_construction: usize,
    /// Search-time beam width (default: 50)
    pub ef_search: usize,
    /// Level multiplier: 1/ln(m)
    pub ml: f64,
}

impl Default for HnswConfig {
    fn default() -> Self {
        let m = 16;
        Self {
            m,
            ef_construction: 200,
            ef_search: 50,
            ml: 1.0 / (m as f64).ln(),
        }
    }
}

impl HnswConfig {
    /// Max connections for layer 0 (2*M)
    fn max_connections_layer0(&self) -> usize {
        self.m * 2
    }

    /// Max connections for layers > 0
    fn max_connections(&self) -> usize {
        self.m
    }
}

/// A node in the HNSW graph
#[derive(Debug, Clone)]
struct HnswNode {
    /// Neighbors per layer: neighbors[layer] = set of neighbor VectorIds
    /// BTreeSet for deterministic iteration order
    neighbors: Vec<BTreeSet<VectorId>>,
    /// Max layer this node appears in
    max_layer: usize,
    /// Creation timestamp (microseconds since epoch; 0 = legacy/unknown)
    created_at: u64,
    /// Soft-delete timestamp: Some(ts) = deleted at ts; None = alive
    deleted_at: Option<u64>,
}

impl HnswNode {
    fn new(max_layer: usize, created_at: u64) -> Self {
        let neighbors = (0..=max_layer).map(|_| BTreeSet::new()).collect();
        Self {
            neighbors,
            max_layer,
            created_at,
            deleted_at: None,
        }
    }

    /// Returns true if this node has been soft-deleted
    fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Check if this node was alive at the given timestamp.
    /// Nodes with created_at == 0 are treated as always-existing (legacy nodes).
    fn is_alive_at(&self, as_of_ts: u64) -> bool {
        (self.created_at == 0 || self.created_at <= as_of_ts)
            && self.deleted_at.map_or(true, |d| d > as_of_ts)
    }
}

/// Scored candidate for search (max-heap by score, tie-break by VectorId asc)
#[derive(Debug, Clone, PartialEq)]
struct ScoredId {
    score: f32,
    id: VectorId,
}

impl Eq for ScoredId {}

impl PartialOrd for ScoredId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredId {
    fn cmp(&self, other: &Self) -> Ordering {
        // Natural ordering: higher score = Greater
        // BinaryHeap<ScoredId> = max-heap (pops highest score first → nearest candidate)
        // BinaryHeap<Reverse<ScoredId>> = min-heap (pops lowest score first → worst result)
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
            // Tie-break: lower VectorId = Greater (deterministic, lower ID preferred)
            .then_with(|| other.id.cmp(&self.id))
    }
}

// ============================================================================
// HnswGraph: Graph-only HNSW structure (no embedding ownership)
// ============================================================================

/// Graph-only HNSW structure. Does NOT own embeddings.
///
/// Used by `SealedSegment` to avoid duplicating the global VectorHeap.
/// All search and graph-building methods accept an external `&VectorHeap`
/// for distance computation.
pub(crate) struct HnswGraph {
    config: HnswConfig,
    vector_config: VectorConfig,
    /// Graph structure: VectorId -> HnswNode
    /// BTreeMap for deterministic iteration (Invariant R3)
    nodes: BTreeMap<VectorId, HnswNode>,
    /// Entry point (top-level node)
    entry_point: Option<VectorId>,
    /// Current max level in graph
    max_level: usize,
    /// Fixed seed for deterministic level assignment
    rng_seed: u64,
    /// Monotonic counter for deterministic RNG
    rng_counter: u64,
}

impl HnswGraph {
    /// Create a new empty HNSW graph
    pub(crate) fn new(vector_config: &VectorConfig, hnsw_config: HnswConfig) -> Self {
        Self {
            config: hnsw_config,
            vector_config: vector_config.clone(),
            nodes: BTreeMap::new(),
            entry_point: None,
            max_level: 0,
            rng_seed: 42,
            rng_counter: 0,
        }
    }

    // ========================================================================
    // Internal: Level Assignment
    // ========================================================================

    /// Assign a random level for a new node using deterministic RNG
    ///
    /// Uses a simple hash-based PRNG seeded with a fixed seed and monotonic counter.
    /// This ensures identical level assignment across identical insert sequences.
    fn assign_level(&mut self) -> usize {
        self.rng_counter += 1;
        let hash = self.splitmix64(self.rng_seed.wrapping_add(self.rng_counter));

        // Convert to uniform [0, 1) and apply exponential distribution
        let uniform = (hash as f64) / (u64::MAX as f64);
        // Clamp to avoid log(0)
        let uniform = uniform.max(1e-15);
        (-uniform.ln() * self.config.ml) as usize
    }

    /// SplitMix64 hash function for deterministic PRNG
    fn splitmix64(&self, mut x: u64) -> u64 {
        x = x.wrapping_add(0x9e3779b97f4a7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }

    // ========================================================================
    // Internal: Graph Operations
    // ========================================================================

    /// Beam search at a single layer (Paper Algorithm 2: SEARCH-LAYER)
    ///
    /// Returns up to `ef` closest non-deleted nodes, sorted by (score desc, VectorId asc).
    ///
    /// Key differences from naive implementation:
    /// - Candidates use max-heap: extract NEAREST (highest score) first for expansion
    /// - Results use min-heap: the worst result (lowest score) sits on top for O(1) eviction
    /// - Deleted nodes are traversed as graph waypoints but excluded from results
    fn search_layer(
        &self,
        query: &[f32],
        entry_id: VectorId,
        ef: usize,
        layer: usize,
        heap: &VectorHeap,
    ) -> Vec<ScoredId> {
        let metric = self.vector_config.metric;

        let entry_embedding = match heap.get(entry_id) {
            Some(e) => e,
            None => return Vec::new(),
        };
        let entry_score = compute_similarity(query, entry_embedding, metric);

        let mut visited = BTreeSet::new();
        visited.insert(entry_id);

        // C: candidates — max-heap (highest score popped first = nearest to query)
        let mut candidates = BinaryHeap::new();
        candidates.push(ScoredId {
            score: entry_score,
            id: entry_id,
        });

        // W: dynamic result list — min-heap via Reverse (lowest score on top for eviction)
        let mut results: BinaryHeap<Reverse<ScoredId>> = BinaryHeap::new();
        let entry_deleted = self
            .nodes
            .get(&entry_id)
            .map(|n| n.is_deleted())
            .unwrap_or(false);
        if !entry_deleted {
            results.push(Reverse(ScoredId {
                score: entry_score,
                id: entry_id,
            }));
        }

        while let Some(nearest) = candidates.pop() {
            // Paper line 7: if nearest candidate is worse than worst result, stop
            let worst_result_score = results
                .peek()
                .map(|r| r.0.score)
                .unwrap_or(f32::NEG_INFINITY);
            if nearest.score < worst_result_score && results.len() >= ef {
                break;
            }

            // Paper line 8: expand neighbors (traverse through deleted nodes too — Bug 4 fix)
            if let Some(node) = self.nodes.get(&nearest.id) {
                if layer < node.neighbors.len() {
                    for &neighbor_id in &node.neighbors[layer] {
                        if visited.contains(&neighbor_id) {
                            continue;
                        }
                        visited.insert(neighbor_id);

                        if let Some(neighbor_embedding) = heap.get(neighbor_id) {
                            let score = compute_similarity(query, neighbor_embedding, metric);

                            let worst_result_score = results
                                .peek()
                                .map(|r| r.0.score)
                                .unwrap_or(f32::NEG_INFINITY);

                            // Paper line 12: if better than worst result or results not full
                            if results.len() < ef || score > worst_result_score {
                                // Always add to candidates for continued traversal
                                candidates.push(ScoredId {
                                    score,
                                    id: neighbor_id,
                                });

                                // Only add non-deleted nodes to results
                                let is_deleted = self
                                    .nodes
                                    .get(&neighbor_id)
                                    .map(|n| n.is_deleted())
                                    .unwrap_or(false);
                                if !is_deleted {
                                    results.push(Reverse(ScoredId {
                                        score,
                                        id: neighbor_id,
                                    }));
                                    // Paper line 15: evict worst if over capacity
                                    if results.len() > ef {
                                        results.pop();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Extract results and sort: score desc, VectorId asc (Invariant R4)
        let mut result_vec: Vec<ScoredId> = results.into_iter().map(|r| r.0).collect();
        result_vec.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });

        result_vec
    }

    /// Greedy search from top layer to target layer (Paper Algorithm 5, lines 3-5)
    ///
    /// At each layer, evaluates ALL neighbors and moves to the globally best one
    /// (not just the first improving neighbor). Equivalent to SEARCH-LAYER with ef=1.
    fn greedy_search_to_layer(
        &self,
        query: &[f32],
        entry_id: VectorId,
        from_layer: usize,
        to_layer: usize,
        heap: &VectorHeap,
    ) -> VectorId {
        let metric = self.vector_config.metric;
        let mut current = entry_id;

        for layer in (to_layer..=from_layer).rev() {
            let mut improved = true;
            while improved {
                improved = false;
                let current_embedding = match heap.get(current) {
                    Some(e) => e,
                    None => break,
                };
                let current_score = compute_similarity(query, current_embedding, metric);

                // Find the globally best neighbor (not just the first improvement)
                let mut best_score = current_score;
                let mut best_id = current;

                if let Some(node) = self.nodes.get(&current) {
                    if layer < node.neighbors.len() {
                        for &neighbor_id in &node.neighbors[layer] {
                            if let Some(neighbor_embedding) = heap.get(neighbor_id) {
                                let score = compute_similarity(query, neighbor_embedding, metric);
                                if score > best_score
                                    || (score == best_score && neighbor_id < best_id)
                                {
                                    best_score = score;
                                    best_id = neighbor_id;
                                }
                            }
                        }
                    }
                }

                if best_id != current {
                    current = best_id;
                    improved = true;
                }
            }
        }

        current
    }

    /// Select neighbors using the simple heuristic (select closest)
    fn select_neighbors(&self, candidates: &[ScoredId], max_connections: usize) -> Vec<VectorId> {
        // Already sorted by score desc, just take top max_connections
        candidates
            .iter()
            .take(max_connections)
            .map(|s| s.id)
            .collect()
    }

    /// Prune a node's neighbors at a given layer to max_connections
    fn prune_neighbors_for(
        &mut self,
        id: VectorId,
        layer: usize,
        max_connections: usize,
        heap: &VectorHeap,
    ) {
        let metric = self.vector_config.metric;

        let embedding = match heap.get(id) {
            Some(e) => e.to_vec(),
            None => return,
        };

        let neighbors: Vec<VectorId> = if let Some(node) = self.nodes.get(&id) {
            if layer < node.neighbors.len() {
                node.neighbors[layer].iter().copied().collect()
            } else {
                return;
            }
        } else {
            return;
        };

        // Score all neighbors
        let mut scored: Vec<ScoredId> = neighbors
            .iter()
            .filter_map(|&nid| {
                heap.get(nid).map(|n_emb| ScoredId {
                    score: compute_similarity(&embedding, n_emb, metric),
                    id: nid,
                })
            })
            .collect();

        // Sort: score desc, id asc
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });

        // Keep top max_connections
        let keep: BTreeSet<VectorId> = scored.iter().take(max_connections).map(|s| s.id).collect();

        if let Some(node) = self.nodes.get_mut(&id) {
            if layer < node.neighbors.len() {
                node.neighbors[layer] = keep;
            }
        }
    }

    // ========================================================================
    // Graph Building
    // ========================================================================

    /// Rebuild the graph from scratch using heap contents
    ///
    /// Called after recovery to reconstruct the HNSW graph structure.
    pub(crate) fn rebuild_graph(
        &mut self,
        heap: &VectorHeap,
        pending_timestamps: &mut BTreeMap<VectorId, u64>,
    ) {
        // Collect all IDs
        let ids: Vec<VectorId> = heap.ids().collect();
        if ids.is_empty() {
            return;
        }

        // Reset graph state
        self.nodes.clear();
        self.entry_point = None;
        self.max_level = 0;
        // Reset RNG counter to reproduce deterministic levels
        self.rng_counter = 0;

        // Re-insert each vector in ID order (deterministic)
        for id in ids {
            let embedding = match heap.get(id) {
                Some(e) => e.to_vec(),
                None => continue,
            };
            let created_at = pending_timestamps.remove(&id).unwrap_or(0);
            self.insert_into_graph(id, &embedding, created_at, heap);
        }
        pending_timestamps.clear();
    }

    /// Insert a vector into the graph structure (Paper Algorithm 1: INSERT)
    ///
    /// Key paper details:
    /// - Line 9: SELECT-NEIGHBORS uses M (not Mmax) for the new node's connections
    /// - Lines 11-15: Existing neighbors are pruned to Mmax only if they exceed capacity
    /// - Mmax = M for layers > 0, 2*M for layer 0
    pub(crate) fn insert_into_graph(
        &mut self,
        id: VectorId,
        embedding: &[f32],
        created_at: u64,
        heap: &VectorHeap,
    ) {
        let level = self.assign_level();

        // Create node
        let node = HnswNode::new(level, created_at);
        self.nodes.insert(id, node);

        // First node
        if self.entry_point.is_none() {
            self.entry_point = Some(id);
            self.max_level = level;
            return;
        }

        let entry_id = self.entry_point.unwrap();

        // Paper lines 4-6: greedy search from top to level+1
        let mut current_entry = entry_id;
        if self.max_level > level {
            current_entry =
                self.greedy_search_to_layer(embedding, entry_id, self.max_level, level + 1, heap);
        }

        // Paper lines 7-16: at each layer, find neighbors and create connections
        let start_layer = level.min(self.max_level);
        for layer in (0..=start_layer).rev() {
            let candidates = self.search_layer(
                embedding,
                current_entry,
                self.config.ef_construction,
                layer,
                heap,
            );

            // Paper line 9: SELECT-NEIGHBORS(q, W, M) — use M, not Mmax
            let selected = self.select_neighbors(&candidates, self.config.m);

            // Paper line 10: add bidirectional connections
            // First, set the new node's neighbors at this layer
            if let Some(new_node) = self.nodes.get_mut(&id) {
                if layer < new_node.neighbors.len() {
                    for &neighbor_id in &selected {
                        new_node.neighbors[layer].insert(neighbor_id);
                    }
                }
            }

            // Paper lines 11-15: add reverse connections from each neighbor to the new node
            // Prune existing neighbors only if they exceed Mmax
            let max_conn = if layer == 0 {
                self.config.max_connections_layer0()
            } else {
                self.config.max_connections()
            };

            for &neighbor_id in &selected {
                let needs_prune = if let Some(neighbor_node) = self.nodes.get_mut(&neighbor_id) {
                    if layer < neighbor_node.neighbors.len() {
                        neighbor_node.neighbors[layer].insert(id);
                        neighbor_node.neighbors[layer].len() > max_conn
                    } else {
                        false
                    }
                } else {
                    false
                };

                if needs_prune {
                    self.prune_neighbors_for(neighbor_id, layer, max_conn, heap);
                }
            }

            // Paper line 16: use closest candidate as entry for next layer
            if let Some(closest) = candidates.first() {
                current_entry = closest.id;
            }
        }

        // Paper line 17: update entry point if new node has higher level
        if level > self.max_level {
            self.entry_point = Some(id);
            self.max_level = level;
        }
    }

    // ========================================================================
    // Search (with external heap)
    // ========================================================================

    /// Search for nearest neighbors using an external heap for embeddings
    pub(crate) fn search_with_heap(
        &self,
        query: &[f32],
        k: usize,
        heap: &VectorHeap,
    ) -> Vec<(VectorId, f32)> {
        if k == 0 || heap.is_empty() {
            return Vec::new();
        }

        if query.len() != heap.dimension() {
            return Vec::new();
        }

        let entry_id = match self.entry_point {
            Some(id) => id,
            None => return Vec::new(),
        };

        // Skip if entry point is deleted and no valid nodes exist
        if self.nodes.values().all(|n| n.is_deleted()) {
            return Vec::new();
        }

        // Greedy search from top layer to layer 1
        let mut current_entry = entry_id;
        if self.max_level > 0 {
            current_entry = self.greedy_search_to_layer(query, entry_id, self.max_level, 1, heap);
        }

        // ef-search at layer 0
        let ef = self.config.ef_search.max(k);
        let candidates = self.search_layer(query, current_entry, ef, 0, heap);

        // Filter out deleted nodes and take top-k
        candidates
            .into_iter()
            .filter(|s| {
                self.nodes
                    .get(&s.id)
                    .map(|n| !n.is_deleted())
                    .unwrap_or(false)
            })
            .take(k)
            .map(|s| (s.id, s.score))
            .collect()
    }

    /// Temporal search using an external heap
    pub(crate) fn search_at_with_heap(
        &self,
        query: &[f32],
        k: usize,
        as_of_ts: u64,
        heap: &VectorHeap,
    ) -> Vec<(VectorId, f32)> {
        if self.nodes.is_empty() || k == 0 {
            return Vec::new();
        }

        if query.len() != heap.dimension() {
            return Vec::new();
        }

        // Early exit: if no nodes were alive at as_of_ts, no results possible.
        let has_alive = self.nodes.values().any(|n| n.is_alive_at(as_of_ts));
        if !has_alive {
            return Vec::new();
        }

        // Strategy: traverse the *full* current graph and filter results temporally.
        let mut results = self.search_with_heap(query, k * 2, heap);

        results.retain(|(id, _)| self.nodes.get(id).is_some_and(|n| n.is_alive_at(as_of_ts)));

        results.truncate(k);
        results
    }

    /// Range search using an external heap
    pub(crate) fn search_in_range_with_heap(
        &self,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
        heap: &VectorHeap,
    ) -> Vec<(VectorId, f32)> {
        if self.nodes.is_empty() || k == 0 {
            return Vec::new();
        }

        if query.len() != heap.dimension() {
            return Vec::new();
        }

        // Over-fetch and filter by created_at range
        let mut results = self.search_with_heap(query, k * 2, heap);
        results.retain(|(id, _)| {
            self.nodes.get(id).is_some_and(|n| {
                n.created_at >= start_ts && n.created_at <= end_ts && !n.is_deleted()
            })
        });
        results.truncate(k);
        results
    }

    // ========================================================================
    // Graph-only Operations (no heap needed)
    // ========================================================================

    /// Check if a vector exists and is alive (not soft-deleted) in this graph
    #[allow(dead_code)]
    pub(crate) fn contains(&self, id: VectorId) -> bool {
        self.nodes.get(&id).is_some_and(|n| !n.is_deleted())
    }

    /// Soft-delete a vector in the graph. Returns true if the vector was alive.
    pub(crate) fn delete_with_timestamp(&mut self, id: VectorId, deleted_at: u64) -> bool {
        let was_alive = self.nodes.get(&id).is_some_and(|n| !n.is_deleted());
        if let Some(node) = self.nodes.get_mut(&id) {
            node.deleted_at = Some(deleted_at);
        }
        if was_alive && self.entry_point == Some(id) {
            self.entry_point = self
                .nodes
                .iter()
                .find(|(_, n)| !n.is_deleted())
                .map(|(id, _)| *id);
            if let Some(ep) = self.entry_point {
                self.max_level = self.nodes[&ep].max_layer;
            } else {
                self.max_level = 0;
            }
        }
        was_alive
    }

    /// Remove a node and all its bidirectional connections from the graph (for updates)
    pub(crate) fn remove_node(&mut self, id: VectorId) {
        if let Some(node) = self.nodes.remove(&id) {
            // Remove references to this node from all neighbors
            for (layer, neighbors) in node.neighbors.iter().enumerate() {
                for &neighbor_id in neighbors {
                    if let Some(n) = self.nodes.get_mut(&neighbor_id) {
                        if layer < n.neighbors.len() {
                            n.neighbors[layer].remove(&id);
                        }
                    }
                }
            }
            // Update entry point if needed
            if self.entry_point == Some(id) {
                self.entry_point = self.nodes.keys().next().copied();
                self.max_level = self.nodes.values().map(|n| n.max_layer).max().unwrap_or(0);
            }
        }
    }

    /// Count of non-deleted nodes in the graph
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.nodes.values().filter(|n| !n.is_deleted()).count()
    }

    /// Memory usage of the graph structure (excludes embedding data)
    pub(crate) fn memory_usage(&self) -> usize {
        self.nodes
            .values()
            .map(|node| {
                // BTreeSet overhead per neighbor
                node.neighbors
                    .iter()
                    .map(|ns| ns.len() * 16 + 64)
                    .sum::<usize>()
                    + 64 // node overhead
            })
            .sum()
    }

    // ========================================================================
    // Snapshot State
    // ========================================================================

    /// Serialize HNSW graph state to bytes
    pub(crate) fn serialize_graph_state(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // Entry point
        match self.entry_point {
            Some(id) => {
                data.push(1u8);
                data.extend_from_slice(&id.as_u64().to_le_bytes());
            }
            None => {
                data.push(0u8);
            }
        }

        // Max level
        data.extend_from_slice(&(self.max_level as u64).to_le_bytes());

        // RNG state
        data.extend_from_slice(&self.rng_seed.to_le_bytes());
        data.extend_from_slice(&self.rng_counter.to_le_bytes());

        // Node count
        data.extend_from_slice(&(self.nodes.len() as u64).to_le_bytes());

        // Nodes (in BTreeMap order for determinism)
        for (&id, node) in &self.nodes {
            // VectorId
            data.extend_from_slice(&id.as_u64().to_le_bytes());
            // max_layer
            data.extend_from_slice(&(node.max_layer as u64).to_le_bytes());
            // created_at timestamp
            data.extend_from_slice(&node.created_at.to_le_bytes());
            // deleted_at: [has_deleted_at: u8] [deleted_at: u64 if flag=1]
            match node.deleted_at {
                Some(ts) => {
                    data.push(1u8);
                    data.extend_from_slice(&ts.to_le_bytes());
                }
                None => {
                    data.push(0u8);
                }
            }
            // Layer count
            data.extend_from_slice(&(node.neighbors.len() as u64).to_le_bytes());
            // Neighbors per layer
            for layer_neighbors in &node.neighbors {
                data.extend_from_slice(&(layer_neighbors.len() as u64).to_le_bytes());
                for &neighbor_id in layer_neighbors {
                    data.extend_from_slice(&neighbor_id.as_u64().to_le_bytes());
                }
            }
        }

        data
    }

    /// Deserialize HNSW graph state from bytes
    pub(crate) fn deserialize_graph_state(&mut self, data: &[u8]) -> Result<(), VectorError> {
        let mut pos = 0;

        let read_u8 = |pos: &mut usize, data: &[u8]| -> Result<u8, VectorError> {
            if *pos >= data.len() {
                return Err(VectorError::Serialization("unexpected end of data".into()));
            }
            let v = data[*pos];
            *pos += 1;
            Ok(v)
        };

        let read_u64 = |pos: &mut usize, data: &[u8]| -> Result<u64, VectorError> {
            if *pos + 8 > data.len() {
                return Err(VectorError::Serialization("unexpected end of data".into()));
            }
            let bytes: [u8; 8] = data[*pos..*pos + 8]
                .try_into()
                .map_err(|_| VectorError::Serialization("failed to read u64".into()))?;
            *pos += 8;
            Ok(u64::from_le_bytes(bytes))
        };

        // Entry point
        let has_entry = read_u8(&mut pos, data)?;
        self.entry_point = if has_entry == 1 {
            Some(VectorId::new(read_u64(&mut pos, data)?))
        } else {
            None
        };

        // Max level
        self.max_level = read_u64(&mut pos, data)? as usize;

        // RNG state
        self.rng_seed = read_u64(&mut pos, data)?;
        self.rng_counter = read_u64(&mut pos, data)?;

        // Node count
        let node_count = read_u64(&mut pos, data)? as usize;

        self.nodes.clear();

        for _ in 0..node_count {
            let id = VectorId::new(read_u64(&mut pos, data)?);
            let max_layer = read_u64(&mut pos, data)? as usize;
            let created_at = read_u64(&mut pos, data)?;
            let has_deleted_at = read_u8(&mut pos, data)?;
            let deleted_at = if has_deleted_at == 1 {
                Some(read_u64(&mut pos, data)?)
            } else {
                None
            };
            let layer_count = read_u64(&mut pos, data)? as usize;

            let mut neighbors = Vec::with_capacity(layer_count);
            for _ in 0..layer_count {
                let neighbor_count = read_u64(&mut pos, data)? as usize;
                let mut layer_neighbors = BTreeSet::new();
                for _ in 0..neighbor_count {
                    let neighbor_id = VectorId::new(read_u64(&mut pos, data)?);
                    layer_neighbors.insert(neighbor_id);
                }
                neighbors.push(layer_neighbors);
            }

            self.nodes.insert(
                id,
                HnswNode {
                    neighbors,
                    max_layer,
                    created_at,
                    deleted_at,
                },
            );
        }

        Ok(())
    }
}

// ============================================================================
// CompactHnswGraph: Immutable, memory-efficient sealed representation
// ============================================================================

/// Compact, immutable node for sealed HNSW segments.
///
/// Per-layer neighbors are stored as `(start, count)` ranges into a shared
/// `neighbor_data` flat array.  `u32` start and `u16` count give up to 4 G
/// entries and 65 K neighbors per layer — well beyond any practical config.
pub(crate) struct CompactHnswNode {
    /// Per-layer: (start_offset in neighbor_data, count)
    pub(crate) layer_ranges: Vec<(u32, u16)>,
    pub(crate) created_at: u64,
    pub(crate) deleted_at: Option<u64>,
}

/// Storage for neighbor IDs in a compact HNSW graph.
///
/// `Owned` keeps data in a heap-allocated `Vec` (used during normal operation).
/// `Mmap` reads data directly from a memory-mapped file (used after recovery
/// to avoid loading the full neighbor array into RSS).
pub(crate) enum NeighborData {
    Owned(Vec<u64>),
    Mmap {
        mmap: memmap2::Mmap,
        /// Byte offset where u64 neighbor data begins in the mmap
        byte_offset: usize,
        /// Number of u64 elements
        len: usize,
    },
}

// NeighborData is Send+Sync because all variants contain Send+Sync types:
// - Vec<u64> is Send+Sync
// - memmap2::Mmap is Send+Sync
// - usize is Send+Sync
// The compiler auto-derives these traits; no explicit unsafe impl needed.

impl NeighborData {
    /// View the neighbor data as a contiguous `&[u64]` slice.
    pub(crate) fn as_slice(&self) -> &[u64] {
        match self {
            NeighborData::Owned(v) => v,
            NeighborData::Mmap {
                mmap,
                byte_offset,
                len,
            } => {
                if *len == 0 {
                    return &[];
                }
                debug_assert!(
                    *byte_offset % 8 == 0,
                    "NeighborData mmap byte_offset must be 8-byte aligned, got {}",
                    byte_offset
                );
                let bytes = &mmap[*byte_offset..*byte_offset + *len * 8];
                // SAFETY: file format guarantees 8-byte alignment at byte_offset
                // (via align8 in write_graph_file), and the data is native-endian
                // u64 (LE on LE platforms).
                unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const u64, *len) }
            }
        }
    }

    /// Number of u64 elements.
    pub(crate) fn len(&self) -> usize {
        match self {
            NeighborData::Owned(v) => v.len(),
            NeighborData::Mmap { len, .. } => *len,
        }
    }

    /// Whether the data is backed by a memory-mapped file.
    pub(crate) fn is_mmap(&self) -> bool {
        matches!(self, NeighborData::Mmap { .. })
    }
}

/// Immutable, compact representation of a sealed HNSW graph.
///
/// Neighbors are stored in a contiguous sorted `Vec<u64>` (or mmap-backed
/// slice) instead of one `BTreeSet<VectorId>` per layer per node.  This
/// reduces per-element overhead from ~48 bytes (BTreeSet) to 8 bytes (u64).
pub(crate) struct CompactHnswGraph {
    pub(crate) config: HnswConfig,
    pub(crate) vector_config: VectorConfig,
    /// Flat array of all neighbor IDs (u64 for VectorId)
    pub(crate) neighbor_data: NeighborData,
    /// Per-node metadata indexed by VectorId
    pub(crate) nodes: BTreeMap<VectorId, CompactHnswNode>,
    pub(crate) entry_point: Option<VectorId>,
    pub(crate) max_level: usize,
}

impl CompactHnswGraph {
    /// Build from an existing `HnswGraph` (consumes graph state).
    ///
    /// `BTreeSet` iterates in sorted order so the resulting flat array
    /// preserves deterministic neighbor ordering.
    pub(crate) fn from_graph(graph: &HnswGraph) -> Self {
        let mut neighbor_data: Vec<u64> = Vec::new();
        let mut compact_nodes: BTreeMap<VectorId, CompactHnswNode> = BTreeMap::new();

        for (&id, node) in &graph.nodes {
            let mut layer_ranges = Vec::with_capacity(node.neighbors.len());
            for layer_set in &node.neighbors {
                let start = neighbor_data.len() as u32;
                let count = layer_set.len() as u16;
                for &neighbor_id in layer_set {
                    neighbor_data.push(neighbor_id.as_u64());
                }
                layer_ranges.push((start, count));
            }
            compact_nodes.insert(
                id,
                CompactHnswNode {
                    layer_ranges,
                    created_at: node.created_at,
                    deleted_at: node.deleted_at,
                },
            );
        }

        CompactHnswGraph {
            config: graph.config.clone(),
            vector_config: graph.vector_config.clone(),
            neighbor_data: NeighborData::Owned(neighbor_data),
            nodes: compact_nodes,
            entry_point: graph.entry_point,
            max_level: graph.max_level,
        }
    }

    /// Get neighbor IDs for a node at a given layer.
    fn neighbors_at(&self, id: VectorId, layer: usize) -> &[u64] {
        match self.nodes.get(&id) {
            Some(node) if layer < node.layer_ranges.len() => {
                let (start, count) = node.layer_ranges[layer];
                let data = self.neighbor_data.as_slice();
                &data[start as usize..(start as usize + count as usize)]
            }
            _ => &[],
        }
    }

    /// Check if node is deleted
    fn is_deleted(&self, id: VectorId) -> bool {
        self.nodes.get(&id).is_some_and(|n| n.deleted_at.is_some())
    }

    /// Check if node was alive at `as_of_ts`
    fn is_alive_at(&self, id: VectorId, as_of_ts: u64) -> bool {
        self.nodes.get(&id).is_some_and(|n| {
            (n.created_at == 0 || n.created_at <= as_of_ts)
                && n.deleted_at.map_or(true, |d| d > as_of_ts)
        })
    }

    // ====================================================================
    // Search (mirrors HnswGraph methods but uses flat neighbor array)
    // ====================================================================

    /// Beam search at a single layer (same algorithm as HnswGraph::search_layer)
    fn search_layer(
        &self,
        query: &[f32],
        entry_id: VectorId,
        ef: usize,
        layer: usize,
        heap: &VectorHeap,
    ) -> Vec<ScoredId> {
        let metric = self.vector_config.metric;

        let entry_embedding = match heap.get(entry_id) {
            Some(e) => e,
            None => return Vec::new(),
        };
        let entry_score = compute_similarity(query, entry_embedding, metric);

        let mut visited = BTreeSet::new();
        visited.insert(entry_id);

        let mut candidates = BinaryHeap::new();
        candidates.push(ScoredId {
            score: entry_score,
            id: entry_id,
        });

        let mut results: BinaryHeap<Reverse<ScoredId>> = BinaryHeap::new();
        if !self.is_deleted(entry_id) {
            results.push(Reverse(ScoredId {
                score: entry_score,
                id: entry_id,
            }));
        }

        while let Some(nearest) = candidates.pop() {
            let worst_result_score = results
                .peek()
                .map(|r| r.0.score)
                .unwrap_or(f32::NEG_INFINITY);
            if nearest.score < worst_result_score && results.len() >= ef {
                break;
            }

            for &neighbor_u64 in self.neighbors_at(nearest.id, layer) {
                let neighbor_id = VectorId::new(neighbor_u64);
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);

                if let Some(neighbor_embedding) = heap.get(neighbor_id) {
                    let score = compute_similarity(query, neighbor_embedding, metric);

                    let worst_result_score = results
                        .peek()
                        .map(|r| r.0.score)
                        .unwrap_or(f32::NEG_INFINITY);
                    if score > worst_result_score || results.len() < ef {
                        candidates.push(ScoredId {
                            score,
                            id: neighbor_id,
                        });
                        if !self.is_deleted(neighbor_id) {
                            results.push(Reverse(ScoredId {
                                score,
                                id: neighbor_id,
                            }));
                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut result_vec: Vec<ScoredId> = results.into_iter().map(|r| r.0).collect();
        result_vec.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        result_vec
    }

    /// Greedy search from top layer to target layer
    fn greedy_search_to_layer(
        &self,
        query: &[f32],
        entry_id: VectorId,
        from_layer: usize,
        to_layer: usize,
        heap: &VectorHeap,
    ) -> VectorId {
        let metric = self.vector_config.metric;
        let mut current = entry_id;

        for layer in (to_layer..=from_layer).rev() {
            let mut improved = true;
            while improved {
                improved = false;
                let current_embedding = match heap.get(current) {
                    Some(e) => e,
                    None => break,
                };
                let current_score = compute_similarity(query, current_embedding, metric);

                let mut best_score = current_score;
                let mut best_id = current;

                for &neighbor_u64 in self.neighbors_at(current, layer) {
                    let neighbor_id = VectorId::new(neighbor_u64);
                    if let Some(neighbor_embedding) = heap.get(neighbor_id) {
                        let score = compute_similarity(query, neighbor_embedding, metric);
                        if score > best_score || (score == best_score && neighbor_id < best_id) {
                            best_score = score;
                            best_id = neighbor_id;
                        }
                    }
                }

                if best_id != current {
                    current = best_id;
                    improved = true;
                }
            }
        }

        current
    }

    /// Search for nearest neighbors using an external heap for embeddings
    pub(crate) fn search_with_heap(
        &self,
        query: &[f32],
        k: usize,
        heap: &VectorHeap,
    ) -> Vec<(VectorId, f32)> {
        if k == 0 || self.nodes.is_empty() {
            return Vec::new();
        }

        if query.len() != heap.dimension() {
            return Vec::new();
        }

        let entry_id = match self.entry_point {
            Some(id) => id,
            None => return Vec::new(),
        };

        if self.nodes.values().all(|n| n.deleted_at.is_some()) {
            return Vec::new();
        }

        let mut current_entry = entry_id;
        if self.max_level > 0 {
            current_entry = self.greedy_search_to_layer(query, entry_id, self.max_level, 1, heap);
        }

        let ef = self.config.ef_search.max(k);
        let candidates = self.search_layer(query, current_entry, ef, 0, heap);

        candidates
            .into_iter()
            .filter(|s| !self.is_deleted(s.id))
            .take(k)
            .map(|s| (s.id, s.score))
            .collect()
    }

    /// Temporal search using an external heap
    pub(crate) fn search_at_with_heap(
        &self,
        query: &[f32],
        k: usize,
        as_of_ts: u64,
        heap: &VectorHeap,
    ) -> Vec<(VectorId, f32)> {
        if self.nodes.is_empty() || k == 0 {
            return Vec::new();
        }
        if query.len() != heap.dimension() {
            return Vec::new();
        }

        let has_alive = self.nodes.keys().any(|&id| self.is_alive_at(id, as_of_ts));
        if !has_alive {
            return Vec::new();
        }

        let mut results = self.search_with_heap(query, k * 2, heap);
        results.retain(|(id, _)| self.is_alive_at(*id, as_of_ts));
        results.truncate(k);
        results
    }

    /// Range search using an external heap
    pub(crate) fn search_in_range_with_heap(
        &self,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
        heap: &VectorHeap,
    ) -> Vec<(VectorId, f32)> {
        if self.nodes.is_empty() || k == 0 {
            return Vec::new();
        }
        if query.len() != heap.dimension() {
            return Vec::new();
        }

        let mut results = self.search_with_heap(query, k * 2, heap);
        results.retain(|(id, _)| {
            self.nodes.get(id).is_some_and(|n| {
                n.created_at >= start_ts && n.created_at <= end_ts && n.deleted_at.is_none()
            })
        });
        results.truncate(k);
        results
    }

    // ====================================================================
    // Graph-only operations
    // ====================================================================

    /// Check if a vector exists and is alive
    pub(crate) fn contains(&self, id: VectorId) -> bool {
        self.nodes.get(&id).is_some_and(|n| n.deleted_at.is_none())
    }

    /// Soft-delete a vector. Returns true if it was alive.
    pub(crate) fn delete_with_timestamp(&mut self, id: VectorId, deleted_at: u64) -> bool {
        let was_alive = self.nodes.get(&id).is_some_and(|n| n.deleted_at.is_none());
        if let Some(node) = self.nodes.get_mut(&id) {
            node.deleted_at = Some(deleted_at);
        }
        if was_alive && self.entry_point == Some(id) {
            self.entry_point = self
                .nodes
                .iter()
                .find(|(_, n)| n.deleted_at.is_none())
                .map(|(id, _)| *id);
            if let Some(ep) = self.entry_point {
                self.max_level = self.nodes[&ep].layer_ranges.len().saturating_sub(1);
            } else {
                self.max_level = 0;
            }
        }
        was_alive
    }

    /// Number of nodes in the graph (including soft-deleted)
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the neighbor data is backed by a memory-mapped file.
    #[allow(dead_code)]
    pub(crate) fn is_neighbor_data_mmap(&self) -> bool {
        self.neighbor_data.is_mmap()
    }

    /// Memory usage of the compact graph (excludes embedding data)
    pub(crate) fn memory_usage(&self) -> usize {
        // Neighbor data: 0 for mmap (OS manages those pages)
        let neighbor_bytes = if self.neighbor_data.is_mmap() {
            0
        } else {
            self.neighbor_data.len() * std::mem::size_of::<u64>()
        };
        let node_bytes: usize = self
            .nodes
            .values()
            .map(|n| {
                std::mem::size_of::<CompactHnswNode>()
                    + n.layer_ranges.capacity() * std::mem::size_of::<(u32, u16)>()
                    + 64 // BTreeMap node overhead
            })
            .sum();
        neighbor_bytes + node_bytes
    }
}

// ============================================================================
// HnswBackend: Thin wrapper around HnswGraph + VectorHeap
// ============================================================================

/// HNSW index backend
///
/// Wraps an `HnswGraph` (graph structure) with a `VectorHeap` (embedding storage)
/// to provide a self-contained HNSW implementation for the `VectorIndexBackend` trait.
pub struct HnswBackend {
    graph: HnswGraph,
    /// Embedding storage (reuses VectorHeap for contiguous f32 storage)
    heap: VectorHeap,
    /// Timestamps stored during recovery, consumed by rebuild_graph()
    pending_timestamps: BTreeMap<VectorId, u64>,
}

impl HnswBackend {
    /// Create a new HNSW backend
    pub fn new(vector_config: &VectorConfig, hnsw_config: HnswConfig) -> Self {
        Self {
            graph: HnswGraph::new(vector_config, hnsw_config),
            heap: VectorHeap::new(vector_config.clone()),
            pending_timestamps: BTreeMap::new(),
        }
    }

    /// Create from existing heap (for recovery)
    pub fn from_heap(heap: VectorHeap, hnsw_config: HnswConfig) -> Self {
        Self {
            graph: HnswGraph::new(heap.config(), hnsw_config),
            heap,
            pending_timestamps: BTreeMap::new(),
        }
    }

    /// Get read access to heap (for snapshot)
    pub fn heap(&self) -> &VectorHeap {
        &self.heap
    }

    /// Get mutable access to heap (for recovery)
    pub fn heap_mut(&mut self) -> &mut VectorHeap {
        &mut self.heap
    }

    /// Rebuild the graph from scratch
    pub fn rebuild_graph(&mut self) {
        self.graph
            .rebuild_graph(&self.heap, &mut self.pending_timestamps);
    }

    /// Serialize HNSW graph state to bytes
    pub fn serialize_graph_state(&self) -> Vec<u8> {
        self.graph.serialize_graph_state()
    }

    /// Deserialize HNSW graph state from bytes
    pub fn deserialize_graph_state(&mut self, data: &[u8]) -> Result<(), VectorError> {
        self.graph.deserialize_graph_state(data)
    }
}

impl VectorIndexBackend for HnswBackend {
    fn allocate_id(&mut self) -> VectorId {
        self.heap.allocate_id()
    }

    fn insert(&mut self, id: VectorId, embedding: &[f32]) -> Result<(), VectorError> {
        self.insert_with_timestamp(id, embedding, 0)
    }

    fn insert_with_timestamp(
        &mut self,
        id: VectorId,
        embedding: &[f32],
        created_at: u64,
    ) -> Result<(), VectorError> {
        // Check if this is an update
        let is_update = self.heap.contains(id);

        // Update/insert into heap
        self.heap.upsert(id, embedding)?;

        if is_update {
            // For updates, remove old graph connections and re-insert
            self.graph.remove_node(id);
        }

        // Insert into graph with timestamp
        self.graph
            .insert_into_graph(id, embedding, created_at, &self.heap);

        Ok(())
    }

    fn insert_with_id(&mut self, id: VectorId, embedding: &[f32]) -> Result<(), VectorError> {
        self.heap.insert_with_id(id, embedding)?;
        // Don't build graph during recovery - rebuild_graph() will be called after
        Ok(())
    }

    fn insert_with_id_and_timestamp(
        &mut self,
        id: VectorId,
        embedding: &[f32],
        created_at: u64,
    ) -> Result<(), VectorError> {
        self.heap.insert_with_id(id, embedding)?;
        // Store timestamp for rebuild_graph() to use later
        self.pending_timestamps.insert(id, created_at);
        Ok(())
    }

    fn delete(&mut self, id: VectorId) -> Result<bool, VectorError> {
        self.delete_with_timestamp(id, 0)
    }

    fn delete_with_timestamp(
        &mut self,
        id: VectorId,
        deleted_at: u64,
    ) -> Result<bool, VectorError> {
        let existed = self.heap.delete(id);
        if existed {
            // Mark as deleted in graph (lazy deletion) with timestamp
            self.graph.delete_with_timestamp(id, deleted_at);
        }
        Ok(existed)
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorId, f32)> {
        self.graph.search_with_heap(query, k, &self.heap)
    }

    fn search_at(&self, query: &[f32], k: usize, as_of_ts: u64) -> Vec<(VectorId, f32)> {
        self.graph
            .search_at_with_heap(query, k, as_of_ts, &self.heap)
    }

    fn search_in_range(
        &self,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
    ) -> Vec<(VectorId, f32)> {
        self.graph
            .search_in_range_with_heap(query, k, start_ts, end_ts, &self.heap)
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn dimension(&self) -> usize {
        self.heap.dimension()
    }

    fn metric(&self) -> DistanceMetric {
        self.heap.metric()
    }

    fn config(&self) -> VectorConfig {
        self.heap.config().clone()
    }

    fn get(&self, id: VectorId) -> Option<&[f32]> {
        self.heap.get(id)
    }

    fn contains(&self, id: VectorId) -> bool {
        self.heap.contains(id)
    }

    fn rebuild_index(&mut self) {
        self.rebuild_graph();
    }

    fn index_type_name(&self) -> &'static str {
        "hnsw"
    }

    fn memory_usage(&self) -> usize {
        // Embedding storage (0 for mmap — OS manages those pages)
        let embedding_bytes = self.heap.anon_data_bytes();
        // Graph structure
        let graph_bytes = self.graph.memory_usage();
        let heap_overhead =
            self.heap.len() * (std::mem::size_of::<VectorId>() + std::mem::size_of::<usize>() + 64);
        let free_slots_bytes = std::mem::size_of_val(self.heap.free_slots());

        embedding_bytes + graph_bytes + heap_overhead + free_slots_bytes
    }

    fn vector_ids(&self) -> Vec<VectorId> {
        self.heap.ids().collect()
    }

    fn snapshot_state(&self) -> (u64, Vec<usize>) {
        (self.heap.next_id_value(), self.heap.free_slots().to_vec())
    }

    fn restore_snapshot_state(&mut self, next_id: u64, free_slots: Vec<usize>) {
        self.heap.restore_snapshot_state(next_id, free_slots);
    }

    fn freeze_heap_to_disk(&self, path: &std::path::Path) -> Result<(), VectorError> {
        self.heap.freeze_to_disk(path)
    }

    fn replace_heap(&mut self, heap: crate::primitives::vector::VectorHeap) {
        self.heap = heap;
    }

    fn register_mmap_vector(&mut self, id: VectorId, created_at: u64) {
        self.pending_timestamps.insert(id, created_at);
    }

    fn is_heap_mmap(&self) -> bool {
        self.heap.is_mmap()
    }

    fn set_inline_meta(&mut self, id: VectorId, meta: InlineMeta) {
        self.heap.set_inline_meta(id, meta);
    }

    fn get_inline_meta(&self, id: VectorId) -> Option<&InlineMeta> {
        self.heap.get_inline_meta(id)
    }

    fn remove_inline_meta(&mut self, id: VectorId) {
        self.heap.remove_inline_meta(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backend(dim: usize, metric: DistanceMetric) -> HnswBackend {
        let config = VectorConfig::new(dim, metric).unwrap();
        HnswBackend::new(&config, HnswConfig::default())
    }

    #[test]
    fn test_hnsw_basic_insert_search() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.9, 0.1, 0.0]).unwrap();

        assert_eq!(backend.len(), 3);

        let results = backend.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, VectorId::new(1)); // Most similar
        assert_eq!(results[1].0, VectorId::new(3)); // Second
    }

    #[test]
    fn test_hnsw_delete() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();

        // Delete vector 1
        let existed = backend.delete(VectorId::new(1)).unwrap();
        assert!(existed);
        assert_eq!(backend.len(), 2);

        // Deleted vector should not appear in search results
        let results = backend.search(&[1.0, 0.0, 0.0], 10);
        for (id, _) in &results {
            assert_ne!(*id, VectorId::new(1));
        }
    }

    #[test]
    fn test_hnsw_determinism() {
        // Same inserts should produce same search results
        for _ in 0..10 {
            let mut backend = make_backend(3, DistanceMetric::Cosine);

            backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
            backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
            backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
            backend.insert(VectorId::new(4), &[0.7, 0.7, 0.0]).unwrap();
            backend.insert(VectorId::new(5), &[0.5, 0.5, 0.5]).unwrap();

            let results = backend.search(&[1.0, 0.0, 0.0], 3);
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].0, VectorId::new(1));
        }
    }

    #[test]
    fn test_hnsw_empty_search() {
        let backend = make_backend(3, DistanceMetric::Cosine);
        let results = backend.search(&[1.0, 0.0, 0.0], 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_k_zero() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);
        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();

        let results = backend.search(&[1.0, 0.0, 0.0], 0);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_dimension_mismatch() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);
        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();

        let results = backend.search(&[1.0, 0.0], 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_score_ordering() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();

        let results = backend.search(&[1.0, 0.0, 0.0], 2);

        // Higher score should come first
        assert!(results[0].1 >= results[1].1);
    }

    #[test]
    fn test_hnsw_tie_breaking() {
        let mut backend = make_backend(3, DistanceMetric::DotProduct);

        let embedding = [1.0, 0.0, 0.0];
        backend.insert(VectorId::new(5), &embedding).unwrap();
        backend.insert(VectorId::new(2), &embedding).unwrap();
        backend.insert(VectorId::new(8), &embedding).unwrap();
        backend.insert(VectorId::new(1), &embedding).unwrap();

        let results = backend.search(&[1.0, 0.0, 0.0], 10);

        // All same score -> sorted by VectorId ascending
        let ids: Vec<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids, vec![1, 2, 5, 8]);
    }

    #[test]
    fn test_hnsw_upsert() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();

        // Update vector 1 to point in a different direction
        backend.insert(VectorId::new(1), &[0.0, 0.0, 1.0]).unwrap();

        assert_eq!(backend.len(), 2);

        // Query for [0,0,1] should find vector 1 first now
        let results = backend.search(&[0.0, 0.0, 1.0], 1);
        assert_eq!(results[0].0, VectorId::new(1));
    }

    #[test]
    fn test_hnsw_vs_brute_force_recall() {
        use crate::primitives::vector::brute_force::BruteForceBackend;

        let dim = 32;
        let n = 200;
        let k = 10;

        let config = VectorConfig::new(dim, DistanceMetric::Cosine).unwrap();
        let mut hnsw = HnswBackend::new(&config, HnswConfig::default());
        let mut brute = BruteForceBackend::new(&config);

        // Insert same vectors into both
        for i in 1..=n {
            let embedding: Vec<f32> = (0..dim)
                .map(|j| ((i * dim + j) as f32 / 1000.0).sin())
                .collect();
            let id = VectorId::new(i as u64);
            hnsw.insert(id, &embedding).unwrap();
            brute.insert(id, &embedding).unwrap();
        }

        // Compare search results
        let query: Vec<f32> = (0..dim).map(|i| (i as f32 / 100.0).cos()).collect();
        let hnsw_results = hnsw.search(&query, k);
        let brute_results = brute.search(&query, k);

        // Check recall (proportion of true top-k that HNSW found)
        let brute_ids: BTreeSet<VectorId> = brute_results.iter().map(|(id, _)| *id).collect();
        let hnsw_ids: BTreeSet<VectorId> = hnsw_results.iter().map(|(id, _)| *id).collect();

        let overlap = brute_ids.intersection(&hnsw_ids).count();
        let recall = overlap as f64 / k as f64;

        assert!(
            recall >= 0.9,
            "HNSW recall {:.2} is below threshold 0.90 (found {} of {} true top-k)",
            recall,
            overlap,
            k
        );
    }

    #[test]
    fn test_hnsw_graph_serialization_roundtrip() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();

        // Serialize
        let graph_data = backend.serialize_graph_state();

        // Search before
        let results_before = backend.search(&[1.0, 0.0, 0.0], 3);

        // Deserialize into fresh graph
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut restored = HnswBackend::new(&config, HnswConfig::default());

        // Re-insert embeddings into heap (simulating snapshot restore)
        restored
            .heap_mut()
            .insert_with_id(VectorId::new(1), &[1.0, 0.0, 0.0])
            .unwrap();
        restored
            .heap_mut()
            .insert_with_id(VectorId::new(2), &[0.0, 1.0, 0.0])
            .unwrap();
        restored
            .heap_mut()
            .insert_with_id(VectorId::new(3), &[0.0, 0.0, 1.0])
            .unwrap();

        restored.deserialize_graph_state(&graph_data).unwrap();

        let results_after = restored.search(&[1.0, 0.0, 0.0], 3);

        // Results should be identical
        assert_eq!(results_before.len(), results_after.len());
        for (before, after) in results_before.iter().zip(results_after.iter()) {
            assert_eq!(before.0, after.0);
            assert!((before.1 - after.1).abs() < 1e-6);
        }
    }

    #[test]
    fn test_hnsw_rebuild_graph() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut backend = HnswBackend::new(&config, HnswConfig::default());

        // Insert using insert_with_id (simulating recovery)
        backend
            .heap_mut()
            .insert_with_id(VectorId::new(1), &[1.0, 0.0, 0.0])
            .unwrap();
        backend
            .heap_mut()
            .insert_with_id(VectorId::new(2), &[0.0, 1.0, 0.0])
            .unwrap();
        backend
            .heap_mut()
            .insert_with_id(VectorId::new(3), &[0.0, 0.0, 1.0])
            .unwrap();

        // Graph is empty at this point
        assert!(backend.graph.entry_point.is_none());

        // Rebuild
        backend.rebuild_graph();

        // Now search should work
        let results = backend.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, VectorId::new(1));
    }

    #[test]
    fn test_hnsw_large_scale() {
        let dim = 128;
        let n = 1000;

        let config = VectorConfig::new(dim, DistanceMetric::Cosine).unwrap();
        let mut backend = HnswBackend::new(&config, HnswConfig::default());

        for i in 1..=n {
            let embedding: Vec<f32> = (0..dim)
                .map(|j| ((i * dim + j) as f32 / 1000.0).sin())
                .collect();
            backend.insert(VectorId::new(i as u64), &embedding).unwrap();
        }

        let query: Vec<f32> = (0..dim).map(|i| (i as f32 / 100.0).cos()).collect();
        let start = std::time::Instant::now();
        let results = backend.search(&query, 10);
        let elapsed = start.elapsed();

        assert_eq!(results.len(), 10);
        // HNSW should be fast
        assert!(
            elapsed.as_millis() < 100,
            "HNSW search took too long: {:?}",
            elapsed
        );

        // Verify ordering
        for i in 1..results.len() {
            assert!(
                results[i - 1].1 >= results[i].1,
                "Results not sorted by score"
            );
        }
    }

    #[test]
    fn test_hnsw_metrics() {
        // Test with different distance metrics
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
        ] {
            let mut backend = make_backend(3, metric);
            backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
            backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();

            let results = backend.search(&[1.0, 0.0, 0.0], 2);
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].0, VectorId::new(1));
        }
    }

    #[test]
    fn test_hnsw_snapshot_state() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();

        let (next_id, free_slots) = backend.snapshot_state();
        assert!(next_id > 0);
        assert!(free_slots.is_empty());

        // Delete and check free slots
        backend.delete(VectorId::new(1)).unwrap();
        let (_, free_slots2) = backend.snapshot_state();
        assert_eq!(free_slots2.len(), 1);
    }

    #[test]
    fn test_hnsw_accessors() {
        let backend = make_backend(3, DistanceMetric::Cosine);
        assert_eq!(backend.dimension(), 3);
        assert_eq!(backend.metric(), DistanceMetric::Cosine);
        assert_eq!(backend.index_type_name(), "hnsw");
        assert!(backend.is_empty());
    }

    #[test]
    fn test_hnsw_multi_query_recall() {
        // Rigorous recall test: 500 vectors, 20 random queries, average recall >= 0.95
        use crate::primitives::vector::brute_force::BruteForceBackend;

        let dim = 64;
        let n = 500;
        let k = 10;
        let num_queries = 20;

        let config = VectorConfig::new(dim, DistanceMetric::Cosine).unwrap();
        let mut hnsw = HnswBackend::new(&config, HnswConfig::default());
        let mut brute = BruteForceBackend::new(&config);

        // Deterministic embeddings
        for i in 1..=n {
            let embedding: Vec<f32> = (0..dim)
                .map(|j| ((i * dim + j) as f32 / 1000.0).sin())
                .collect();
            let id = VectorId::new(i as u64);
            hnsw.insert(id, &embedding).unwrap();
            brute.insert(id, &embedding).unwrap();
        }

        // Run multiple queries and average recall
        let mut total_recall = 0.0;
        for q in 0..num_queries {
            let query: Vec<f32> = (0..dim)
                .map(|i| ((q * dim + i) as f32 / 200.0).cos())
                .collect();

            let hnsw_results = hnsw.search(&query, k);
            let brute_results = brute.search(&query, k);

            let brute_ids: BTreeSet<VectorId> = brute_results.iter().map(|(id, _)| *id).collect();
            let hnsw_ids: BTreeSet<VectorId> = hnsw_results.iter().map(|(id, _)| *id).collect();

            let overlap = brute_ids.intersection(&hnsw_ids).count();
            total_recall += overlap as f64 / k as f64;
        }

        let avg_recall = total_recall / num_queries as f64;
        assert!(
            avg_recall >= 0.95,
            "Average HNSW recall {:.3} is below 0.95 across {} queries",
            avg_recall,
            num_queries
        );
    }
}
