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
mod edges;
pub mod integrity;
pub mod keys;
mod lifecycle;
mod nodes;
pub mod ontology;
pub mod packed;
mod snapshot;
pub mod traversal;
pub mod types;

pub use branch_dag::GraphSubsystem;
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

    /// Build a snapshot of the entire graph.
    pub fn snapshot(&self, branch_id: BranchId, graph: &str) -> StrataResult<GraphSnapshot> {
        let nodes = self.all_nodes(branch_id, graph)?;
        let edges = self.all_edges(branch_id, graph)?;
        Ok(GraphSnapshot { nodes, edges })
    }

    /// Get graph statistics (node/edge counts) without loading all data.
    ///
    /// Uses `packed::edge_count()` on each forward adjacency entry (header-only,
    /// no full decode) and counts node keys via prefix scan.
    /// Both counts are read in a single transaction for consistency.
    pub fn snapshot_stats(&self, branch_id: BranchId, graph: &str) -> StrataResult<GraphStats> {
        let node_prefix = keys::all_nodes_prefix(graph);
        let node_prefix_key = keys::storage_key(branch_id, &node_prefix);
        let fwd_prefix = keys::all_forward_adj_prefix(graph);
        let fwd_prefix_key = keys::storage_key(branch_id, &fwd_prefix);

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
        graph: &str,
    ) -> StrataResult<AdjacencyIndex> {
        let edges = self.all_edges(branch_id, graph)?;
        let mut index = AdjacencyIndex::new();
        for edge in edges {
            index.add_edge(&edge.src, &edge.dst, &edge.edge_type, edge.data);
        }
        Ok(index)
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

        // Score all matching docs in the shared index, then filter to Graph refs.
        // Request more than k to account for non-graph results being filtered out.
        let top_k = index.score_top_k(
            &parsed.terms,
            &req.branch_id,
            req.k.saturating_mul(4),
            req.bm25_k1,
            req.bm25_b,
            &phrase_cfg,
        );

        let hits: Vec<SearchHit> = top_k
            .into_iter()
            .filter_map(|scored| {
                let entity_ref = index.resolve_doc_id(scored.doc_id)?;

                // Only include graph entity refs
                if !entity_ref.is_graph() {
                    return None;
                }

                // Extract snippet from graph node data.
                // The key format is "{graph}/n/{node_id}" (from keys::node_key).
                let snippet = if let EntityRef::Graph { ref key, .. } = entity_ref {
                    self.extract_graph_snippet(&req.branch_id, key)
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
    /// Called after successful add_node/bulk_insert commits.
    pub(crate) fn index_node_for_search(
        &self,
        branch_id: BranchId,
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
            key: user_key,
        };
        index.index_document(&entity_ref, &text, None);
    }

    /// Remove a graph node from the inverted index.
    ///
    /// Called after successful remove_node/delete_graph commits.
    pub(crate) fn deindex_node_for_search(&self, branch_id: BranchId, graph: &str, node_id: &str) {
        let Ok(index) = self.db.extension::<strata_engine::search::InvertedIndex>() else {
            return;
        };
        if !index.is_enabled() {
            return;
        }

        let user_key = keys::node_key(graph, node_id);
        let entity_ref = strata_engine::search::EntityRef::Graph {
            branch_id,
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
    /// fetches the node, and builds a snippet from object_type + properties.
    fn extract_graph_snippet(&self, branch_id: &BranchId, storage_key: &str) -> Option<String> {
        // Storage key format: "{graph}/n/{node_id}"
        let parts: Vec<&str> = storage_key.splitn(3, '/').collect();
        if parts.len() < 3 || parts[1] != "n" {
            return None;
        }
        let graph = parts[0];
        let node_id = parts[2];

        let data = self.get_node(*branch_id, graph, node_id).ok()??;

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
