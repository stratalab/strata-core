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
