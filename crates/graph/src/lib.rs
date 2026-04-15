//! Graph module for strata-graph.
//!
//! Provides a property graph overlay on top of Strata's KV storage.
//! Nodes and edges are stored as KV entries under the `_graph_` space,
//! providing branch isolation, time-travel, and transactional guarantees.

pub mod adjacency;
pub mod analytics;
pub mod boost;
pub mod branch_dag;
pub mod branch_status_cache;
mod bulk;
pub mod dag_hook_impl;
mod edges;
pub mod ext;
pub mod integrity;
pub mod keys;
mod lifecycle;
pub mod merge;
pub mod merge_handler;
mod nodes;
pub mod ontology;
pub mod packed;
mod snapshot;
pub mod store;
pub mod traversal;
pub mod types;

pub use branch_dag::GraphSubsystem;
pub use store::{GraphBackendState, StagedGraphOp};
pub use strata_core::branch_dag::{
    is_system_branch, DagBranchInfo, DagBranchStatus, DagEventId, ForkRecord, MergeRecord,
    BRANCH_DAG_GRAPH, SYSTEM_BRANCH,
};

use std::sync::Arc;

use strata_core::types::{BranchId, Key};
use strata_core::{StrataError, StrataResult, Value};

use adjacency::AdjacencyIndex;
use strata_engine::Database;
use types::*;

/// Graph store providing CRUD operations on nodes and edges.
///
/// All data is stored in the underlying KV engine under the `_graph_` space.
/// Operations are transactional and branch-isolated.
#[derive(Clone)]
pub struct GraphStore {
    db: Arc<Database>,
}

impl GraphStore {
    /// Create a new GraphStore backed by the given database.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Get access to the shared backend state.
    ///
    /// This returns the shared `GraphBackendState` stored in the Database.
    /// All GraphStore instances for the same Database share this state.
    ///
    /// Also ensures runtime wiring (commit/replay observers) is registered.
    pub fn state(&self) -> StrataResult<Arc<GraphBackendState>> {
        let state = self
            .db
            .extension::<GraphBackendState>()
            .map_err(|e| StrataError::internal(e.to_string()))?;
        store::ensure_runtime_wiring(&self.db, &state);
        Ok(state)
    }

    /// Build a snapshot of the entire graph.
    pub fn snapshot(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<GraphSnapshot> {
        let nodes = self.all_nodes(branch_id, space, graph)?;
        let edges = self.all_edges(branch_id, space, graph)?;
        Ok(GraphSnapshot { nodes, edges })
    }

    /// Get graph statistics (node/edge counts) without loading all data.
    ///
    /// Uses `packed::edge_count()` on each forward adjacency entry (header-only,
    /// no full decode) and counts node keys via prefix scan.
    /// Both counts are read in a single transaction for consistency.
    pub fn snapshot_stats(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<GraphStats> {
        let node_prefix = keys::all_nodes_prefix(graph);
        let node_prefix_key = keys::storage_key(branch_id, space, &node_prefix);
        let fwd_prefix = keys::all_forward_adj_prefix(graph);
        let fwd_prefix_key = keys::storage_key(branch_id, space, &fwd_prefix);

        self.db.transaction(branch_id, |txn| {
            // Count nodes via prefix scan (no decode — just count keys)
            let node_count = txn.scan_prefix(&node_prefix_key)?.len();

            // Count edges via forward adjacency prefix scan + packed header read
            let mut edge_count = 0usize;
            for (_key, val) in txn.scan_prefix(&fwd_prefix_key)? {
                if let Value::Bytes(bytes) = val {
                    edge_count += packed::edge_count(&bytes) as usize;
                }
            }

            Ok(GraphStats {
                node_count,
                edge_count,
            })
        })
    }

    /// Build an in-memory adjacency index for a graph.
    ///
    /// Loads all edges in a single prefix scan and populates an AdjacencyIndex
    /// for O(1) neighbor lookups during traversal. This replaces N per-node
    /// scans with 1 bulk scan.
    pub fn build_adjacency_index(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<AdjacencyIndex> {
        let edges = self.all_edges(branch_id, space, graph)?;
        let mut index = AdjacencyIndex::new();
        for edge in edges {
            index.add_edge(&edge.src, &edge.dst, &edge.edge_type, edge.data);
        }
        Ok(index)
    }

    /// Build a fully-populated adjacency index atomically in a single transaction.
    ///
    /// Unlike `build_adjacency_index`, this helper also loads every node ID
    /// (including isolated nodes) and opens exactly ONE read transaction for both
    /// the edge scan and the node list. This guarantees the returned index reflects
    /// a single snapshot version of the graph, fixing the torn-read window analytics
    /// would otherwise observe between two sequential reads.
    ///
    /// Used by `pagerank`, `wcc`, `cdlp`, and `lcc`. `sssp` does not need this
    /// because it does not load isolated nodes.
    pub(crate) fn build_full_adjacency_index_atomic(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<AdjacencyIndex> {
        use crate::ext::GraphStoreExt;

        let fwd_prefix = keys::all_forward_adj_prefix(graph);
        let fwd_prefix_key = keys::storage_key(branch_id, space, &fwd_prefix);

        self.db.transaction(branch_id, |txn| {
            let mut index = AdjacencyIndex::new();

            // 1. Scan forward adjacency lists (mirrors all_edges() body).
            for (key, val) in txn.scan_prefix(&fwd_prefix_key)? {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(src) = keys::parse_forward_adj_key(graph, &user_key) {
                        if let Value::Bytes(bytes) = val {
                            let adj = packed::decode(&bytes)?;
                            for (dst, edge_type, data) in adj {
                                index.add_edge(&src, &dst, &edge_type, data);
                            }
                        }
                    }
                }
            }

            // 2. Load all node IDs in the SAME txn (picks up isolated nodes).
            let node_ids = txn.graph_list_nodes(branch_id, space, graph)?;
            for node_id in node_ids {
                index.nodes.insert(node_id);
            }

            Ok(index)
        })
    }
}

// =============================================================================
// Searchable implementation
// =============================================================================

impl strata_engine::search::Searchable for GraphStore {
    fn search(
        &self,
        req: &strata_engine::SearchRequest,
    ) -> StrataResult<strata_engine::SearchResponse> {
        use std::time::Instant;
        use strata_engine::search::{EntityRef, InvertedIndex, SearchHit, SearchStats};

        let start = Instant::now();
        let index = self.db.extension::<InvertedIndex>()?;

        if !index.is_enabled() || index.total_docs() == 0 {
            return Ok(strata_engine::SearchResponse::empty());
        }

        let parsed = strata_engine::search::tokenizer::parse_query(&req.query);
        let phrase_cfg = strata_engine::search::PhraseConfig {
            phrases: &parsed.phrases,
            boost: req.phrase_boost,
            slop: req.phrase_slop,
            filter: req.phrase_filter,
        };
        let prox_cfg = strata_engine::search::ProximityConfig {
            enabled: req.proximity,
            window: req.proximity_window,
            weight: req.proximity_weight,
        };

        // Score all matching docs in the shared index, then filter to Graph refs.
        // Request more than k to account for non-graph results being filtered out.
        // Pass `Some(&req.space)` so cross-space hits are dropped at the
        // index level — graph nodes from other tenants must never leak in.
        let top_k = index.score_top_k(
            &parsed.terms,
            &req.branch_id,
            req.k.saturating_mul(4),
            req.bm25_k1,
            req.bm25_b,
            &phrase_cfg,
            &prox_cfg,
            Some(&req.space),
        );

        let hits: Vec<SearchHit> = top_k
            .into_iter()
            .filter_map(|scored| {
                let entity_ref = index.resolve_doc_id(scored.doc_id)?;

                // Only include graph entity refs
                if !entity_ref.is_graph() {
                    return None;
                }

                // Extract snippet from graph node data. The key format is
                // "{graph}/n/{node_id}" (from keys::node_key). The
                // EntityRef carries the space, so we read the snippet
                // from exactly the space the node was indexed under
                // rather than guessing from the request's current space.
                let snippet = if let EntityRef::Graph {
                    ref space, ref key, ..
                } = entity_ref
                {
                    self.extract_graph_snippet(&req.branch_id, space, key)
                } else {
                    None
                };

                Some(SearchHit {
                    doc_ref: entity_ref,
                    score: scored.score,
                    rank: 0,
                    snippet,
                })
            })
            .take(req.k)
            .enumerate()
            .map(|(i, mut hit)| {
                hit.rank = (i + 1) as u32;
                hit
            })
            .collect();

        let elapsed = start.elapsed().as_micros() as u64;
        let mut stats = SearchStats::new(elapsed, hits.len());
        stats = stats.with_index_used(true);

        Ok(strata_engine::SearchResponse {
            hits,
            truncated: false,
            stats,
        })
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Graph
    }
}

impl GraphStore {
    // =========================================================================
    // Search index helpers
    // =========================================================================

    /// Index a graph node's text into the inverted index for BM25 search.
    ///
    /// Called after successful add_node/bulk_insert commits. The
    /// `(branch_id, space, graph_user_key)` triple uniquely identifies
    /// the node in the search index, so two tenants with the same
    /// `(graph, node_id)` in different spaces don't collide.
    pub fn index_node_for_search(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        data: &NodeData,
    ) {
        let Ok(index) = self.db.extension::<strata_engine::search::InvertedIndex>() else {
            return;
        };
        if !index.is_enabled() {
            return;
        }

        let text = Self::build_node_search_text(node_id, data);
        let user_key = keys::node_key(graph, node_id);
        let entity_ref = strata_engine::search::EntityRef::Graph {
            branch_id,
            space: space.to_string(),
            key: user_key,
        };
        index.index_document(&entity_ref, &text, None);
    }

    /// Remove a graph node from the inverted index.
    ///
    /// Called after successful remove_node/delete_graph commits.
    pub fn deindex_node_for_search(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
    ) {
        let Ok(index) = self.db.extension::<strata_engine::search::InvertedIndex>() else {
            return;
        };
        if !index.is_enabled() {
            return;
        }

        let user_key = keys::node_key(graph, node_id);
        let entity_ref = strata_engine::search::EntityRef::Graph {
            branch_id,
            space: space.to_string(),
            key: user_key,
        };
        index.remove_document(&entity_ref);
    }

    /// Build searchable text from a node's data.
    fn build_node_search_text(node_id: &str, data: &NodeData) -> String {
        let mut text = String::new();
        text.push_str(node_id);
        if let Some(ref ot) = data.object_type {
            text.push(' ');
            text.push_str(ot);
        }
        if let Some(ref props) = data.properties {
            text.push(' ');
            text.push_str(&serde_json::to_string(props).unwrap_or_default());
        }
        if let Some(ref uri) = data.entity_ref {
            text.push(' ');
            text.push_str(uri);
        }
        text
    }

    /// Extract a search snippet from a graph node's data.
    ///
    /// Parses the storage key to find the graph name and node ID,
    /// fetches the node from the given `space`, and builds a snippet from
    /// object_type + properties.
    fn extract_graph_snippet(
        &self,
        branch_id: &BranchId,
        space: &str,
        storage_key: &str,
    ) -> Option<String> {
        // Storage key format: "{graph}/n/{node_id}"
        let parts: Vec<&str> = storage_key.splitn(3, '/').collect();
        if parts.len() < 3 || parts[1] != "n" {
            return None;
        }
        let graph = parts[0];
        let node_id = parts[2];

        let data = self.get_node(*branch_id, space, graph, node_id).ok()??;

        let mut text = String::new();
        if let Some(ref ot) = data.object_type {
            text.push_str(ot);
            text.push_str(": ");
        }
        if let Some(ref props) = data.properties {
            text.push_str(&serde_json::to_string(props).unwrap_or_default());
        } else if text.is_empty() {
            // Fallback: use node_id if no type or properties
            text.push_str(node_id);
        }

        Some(strata_engine::search::truncate_text(&text, 100))
    }
}
