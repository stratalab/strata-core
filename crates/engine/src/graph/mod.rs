//! Graph module for strata-graph.
//!
//! Provides a property graph overlay on top of Strata's KV storage.
//! Nodes and edges are stored as KV entries under the `_graph_` space,
//! providing branch isolation, time-travel, and transactional guarantees.

pub mod adjacency;
pub mod analytics;
pub mod boost;
pub mod integrity;
pub mod keys;
pub mod ontology;
pub mod packed;
mod snapshot;
pub mod traversal;
pub mod types;

use std::sync::Arc;

use strata_core::types::{BranchId, Key};
use strata_core::{StrataError, StrataResult, Value};

use crate::database::Database;
use adjacency::AdjacencyIndex;
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

    // =========================================================================
    // Graph lifecycle
    // =========================================================================

    /// Create a new graph with the given name and optional metadata.
    pub fn create_graph(
        &self,
        branch_id: BranchId,
        graph: &str,
        meta: Option<GraphMeta>,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        let meta = meta.unwrap_or_default();
        let meta_json =
            serde_json::to_string(&meta).map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::meta_key(graph);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            txn.put(storage_key.clone(), Value::String(meta_json.clone()))
        })
    }

    /// Get graph metadata, or None if graph doesn't exist.
    pub fn get_graph_meta(
        &self,
        branch_id: BranchId,
        graph: &str,
    ) -> StrataResult<Option<GraphMeta>> {
        let user_key = keys::meta_key(graph);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            let val = txn.get(&storage_key)?;
            match val {
                Some(Value::String(s)) => {
                    let meta: GraphMeta = serde_json::from_str(&s)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    Ok(Some(meta))
                }
                Some(_) => Err(StrataError::serialization(
                    "Graph meta is not a string".to_string(),
                )),
                None => Ok(None),
            }
        })
    }

    /// List all graph names on a branch.
    pub fn list_graphs(&self, branch_id: BranchId) -> StrataResult<Vec<String>> {
        // Scan all keys and filter for meta keys: `{graph}/__meta__`
        // We scan with empty prefix to get all graph keys, then filter.
        let ns = keys::graph_namespace(branch_id);
        let prefix_key = strata_core::types::Key::new_kv(ns, "");

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut graphs = Vec::new();
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if user_key.ends_with("/__meta__") {
                        if let Some(name) = user_key.strip_suffix("/__meta__") {
                            graphs.push(name.to_string());
                        }
                    }
                }
            }
            Ok(graphs)
        })
    }

    /// Delete a graph and all its data (nodes, edges, meta, ref index entries).
    pub fn delete_graph(&self, branch_id: BranchId, graph: &str) -> StrataResult<()> {
        let prefix = keys::graph_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);
        let node_prefix = keys::all_nodes_prefix(graph);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;

            // First pass: collect ref index keys to delete from nodes
            let mut ref_keys_to_delete = Vec::new();
            for (key, val) in &results {
                if let Some(user_key) = key.user_key_string() {
                    if user_key.starts_with(&node_prefix) {
                        if let Value::String(json) = val {
                            if let Ok(data) = serde_json::from_str::<NodeData>(json) {
                                if let Some(uri) = data.entity_ref {
                                    if let Some(node_id) = keys::parse_node_key(graph, &user_key) {
                                        let rk = keys::ref_index_key(&uri, graph, &node_id);
                                        ref_keys_to_delete.push(keys::storage_key(branch_id, &rk));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Delete ref index entries
            for rk in ref_keys_to_delete {
                txn.delete(rk)?;
            }

            // Delete all graph keys (nodes, edges, meta)
            for (key, _) in results {
                txn.delete(key)?;
            }
            Ok(())
        })
    }

    // =========================================================================
    // Node CRUD
    // =========================================================================

    /// Add or update a node in the graph.
    pub fn add_node(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
        data: NodeData,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        keys::validate_node_id(node_id)?;

        // Validate against frozen ontology if applicable
        if data.object_type.is_some() {
            if let Some(meta) = self.get_graph_meta(branch_id, graph)? {
                if meta.ontology_status == Some(types::OntologyStatus::Frozen) {
                    self.validate_node(branch_id, graph, node_id, &data)?;
                }
            }
        }

        let node_json =
            serde_json::to_string(&data).map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::node_key(graph, node_id);
        let storage_key = keys::storage_key(branch_id, &user_key);

        // Build ref index key if entity_ref is present
        let ref_key = data.entity_ref.as_ref().map(|uri| {
            let rk = keys::ref_index_key(uri, graph, node_id);
            keys::storage_key(branch_id, &rk)
        });

        // Build type index key if object_type is present
        let type_key = data.object_type.as_ref().map(|ot| {
            let tk = keys::type_index_key(graph, ot, node_id);
            keys::storage_key(branch_id, &tk)
        });

        self.db.transaction(branch_id, |txn| {
            // If updating, clean up old ref index and type index entries
            let old_val = txn.get(&storage_key)?;
            if let Some(Value::String(old_json)) = old_val {
                if let Ok(old_data) = serde_json::from_str::<NodeData>(&old_json) {
                    if let Some(old_uri) = old_data.entity_ref {
                        let old_rk = keys::ref_index_key(&old_uri, graph, node_id);
                        let old_sk = keys::storage_key(branch_id, &old_rk);
                        txn.delete(old_sk)?;
                    }
                    if let Some(old_ot) = old_data.object_type {
                        let old_tk = keys::type_index_key(graph, &old_ot, node_id);
                        let old_sk = keys::storage_key(branch_id, &old_tk);
                        txn.delete(old_sk)?;
                    }
                }
            }

            txn.put(storage_key.clone(), Value::String(node_json.clone()))?;

            // Write ref index
            if let Some(rk) = ref_key.clone() {
                txn.put(rk, Value::Null)?;
            }

            // Write type index
            if let Some(tk) = type_key.clone() {
                txn.put(tk, Value::Null)?;
            }
            Ok(())
        })
    }

    /// Get node data, or None if node doesn't exist.
    pub fn get_node(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
    ) -> StrataResult<Option<NodeData>> {
        let user_key = keys::node_key(graph, node_id);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            let val = txn.get(&storage_key)?;
            match val {
                Some(Value::String(s)) => {
                    let data: NodeData = serde_json::from_str(&s)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    Ok(Some(data))
                }
                Some(_) => Err(StrataError::serialization(
                    "Node data is not a string".to_string(),
                )),
                None => Ok(None),
            }
        })
    }

    /// List all node IDs in a graph.
    pub fn list_nodes(&self, branch_id: BranchId, graph: &str) -> StrataResult<Vec<String>> {
        let prefix = keys::all_nodes_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut nodes = Vec::new();
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(id) = keys::parse_node_key(graph, &user_key) {
                        nodes.push(id);
                    }
                }
            }
            Ok(nodes)
        })
    }

    /// Remove a node and all its incident edges.
    pub fn remove_node(&self, branch_id: BranchId, graph: &str, node_id: &str) -> StrataResult<()> {
        let node_user_key = keys::node_key(graph, node_id);
        let node_storage_key = keys::storage_key(branch_id, &node_user_key);
        let fwd_adj_uk = keys::forward_adj_key(graph, node_id);
        let fwd_adj_sk = keys::storage_key(branch_id, &fwd_adj_uk);
        let rev_adj_uk = keys::reverse_adj_key(graph, node_id);
        let rev_adj_sk = keys::storage_key(branch_id, &rev_adj_uk);

        self.db.transaction(branch_id, |txn| {
            // Read node to get entity_ref for ref index cleanup
            let node_val = txn.get(&node_storage_key)?;
            if node_val.is_none() {
                return Ok(());
            }

            // Clean up ref index and type index
            if let Some(Value::String(json)) = &node_val {
                if let Ok(data) = serde_json::from_str::<NodeData>(json) {
                    if let Some(uri) = data.entity_ref {
                        let rk = keys::ref_index_key(&uri, graph, node_id);
                        let sk = keys::storage_key(branch_id, &rk);
                        txn.delete(sk)?;
                    }
                    if let Some(ot) = data.object_type {
                        let tk = keys::type_index_key(graph, &ot, node_id);
                        let sk = keys::storage_key(branch_id, &tk);
                        txn.delete(sk)?;
                    }
                }
            }

            // Remove this node from each neighbor's reverse adjacency list
            if let Some(Value::Bytes(fwd_bytes)) = txn.get(&fwd_adj_sk)? {
                let outgoing = packed::decode(&fwd_bytes)?;
                for (dst, edge_type, _) in &outgoing {
                    let dst_rev_uk = keys::reverse_adj_key(graph, dst);
                    let dst_rev_sk = keys::storage_key(branch_id, &dst_rev_uk);
                    if let Some(Value::Bytes(dst_rev_bytes)) = txn.get(&dst_rev_sk)? {
                        if let Some(new_bytes) =
                            packed::remove_edge(&dst_rev_bytes, node_id, edge_type)
                        {
                            if packed::edge_count(&new_bytes) == 0 {
                                txn.delete(dst_rev_sk)?;
                            } else {
                                txn.put_replace(dst_rev_sk, Value::Bytes(new_bytes))?;
                            }
                        }
                    }
                }
            }

            // Remove this node from each neighbor's forward adjacency list
            if let Some(Value::Bytes(rev_bytes)) = txn.get(&rev_adj_sk)? {
                let incoming = packed::decode(&rev_bytes)?;
                for (src, edge_type, _) in &incoming {
                    let src_fwd_uk = keys::forward_adj_key(graph, src);
                    let src_fwd_sk = keys::storage_key(branch_id, &src_fwd_uk);
                    if let Some(Value::Bytes(src_fwd_bytes)) = txn.get(&src_fwd_sk)? {
                        if let Some(new_bytes) =
                            packed::remove_edge(&src_fwd_bytes, node_id, edge_type)
                        {
                            if packed::edge_count(&new_bytes) == 0 {
                                txn.delete(src_fwd_sk)?;
                            } else {
                                txn.put_replace(src_fwd_sk, Value::Bytes(new_bytes))?;
                            }
                        }
                    }
                }
            }

            // Delete the node's own adjacency lists and the node itself
            txn.delete(fwd_adj_sk)?;
            txn.delete(rev_adj_sk)?;
            txn.delete(node_storage_key.clone())?;
            Ok(())
        })
    }

    // =========================================================================
    // Edge CRUD
    // =========================================================================

    /// Add or update an edge in the graph.
    /// Appends to packed forward and reverse adjacency lists atomically.
    pub fn add_edge(
        &self,
        branch_id: BranchId,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
        data: EdgeData,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        keys::validate_node_id(src)?;
        keys::validate_node_id(dst)?;
        keys::validate_edge_type(edge_type)?;

        // Validate against frozen ontology
        if let Some(meta) = self.get_graph_meta(branch_id, graph)? {
            if meta.ontology_status == Some(types::OntologyStatus::Frozen) {
                self.validate_edge(branch_id, graph, src, dst, edge_type, &data)?;
            }
        }

        let src_node_uk = keys::node_key(graph, src);
        let dst_node_uk = keys::node_key(graph, dst);
        let src_node_key = keys::storage_key(branch_id, &src_node_uk);
        let dst_node_key = keys::storage_key(branch_id, &dst_node_uk);

        let fwd_uk = keys::forward_adj_key(graph, src);
        let fwd_sk = keys::storage_key(branch_id, &fwd_uk);
        let rev_uk = keys::reverse_adj_key(graph, dst);
        let rev_sk = keys::storage_key(branch_id, &rev_uk);

        self.db.transaction(branch_id, |txn| {
            // Validate both nodes exist within the transaction (prevents TOCTOU)
            if txn.get(&src_node_key)?.is_none() {
                return Err(StrataError::invalid_input(format!(
                    "Source node '{}' does not exist in graph '{}'",
                    src, graph
                )));
            }
            if txn.get(&dst_node_key)?.is_none() {
                return Err(StrataError::invalid_input(format!(
                    "Target node '{}' does not exist in graph '{}'",
                    dst, graph
                )));
            }

            // Read-modify-write forward adjacency list (src → dst)
            let mut fwd_buf = match txn.get(&fwd_sk)? {
                Some(Value::Bytes(b)) => b,
                _ => packed::empty(),
            };
            // Remove existing edge if present (upsert semantics)
            if let Some(new_buf) = packed::remove_edge(&fwd_buf, dst, edge_type) {
                fwd_buf = new_buf;
            }
            packed::append_edge(&mut fwd_buf, dst, edge_type, &data)?;
            txn.put_replace(fwd_sk.clone(), Value::Bytes(fwd_buf))?;

            // Read-modify-write reverse adjacency list (dst ← src)
            let mut rev_buf = match txn.get(&rev_sk)? {
                Some(Value::Bytes(b)) => b,
                _ => packed::empty(),
            };
            if let Some(new_buf) = packed::remove_edge(&rev_buf, src, edge_type) {
                rev_buf = new_buf;
            }
            packed::append_edge(&mut rev_buf, src, edge_type, &data)?;
            txn.put_replace(rev_sk.clone(), Value::Bytes(rev_buf))?;

            Ok(())
        })
    }

    /// Get edge data, or None if edge doesn't exist.
    pub fn get_edge(
        &self,
        branch_id: BranchId,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<Option<EdgeData>> {
        let fwd_uk = keys::forward_adj_key(graph, src);
        let fwd_sk = keys::storage_key(branch_id, &fwd_uk);

        self.db
            .transaction(branch_id, |txn| match txn.get(&fwd_sk)? {
                Some(Value::Bytes(bytes)) => Ok(packed::find_edge(&bytes, dst, edge_type)),
                _ => Ok(None),
            })
    }

    /// Remove an edge (from both forward and reverse adjacency lists).
    pub fn remove_edge(
        &self,
        branch_id: BranchId,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<()> {
        let fwd_uk = keys::forward_adj_key(graph, src);
        let fwd_sk = keys::storage_key(branch_id, &fwd_uk);
        let rev_uk = keys::reverse_adj_key(graph, dst);
        let rev_sk = keys::storage_key(branch_id, &rev_uk);

        self.db.transaction(branch_id, |txn| {
            // Remove from forward adjacency list
            if let Some(Value::Bytes(fwd_bytes)) = txn.get(&fwd_sk)? {
                if let Some(new_bytes) = packed::remove_edge(&fwd_bytes, dst, edge_type) {
                    if packed::edge_count(&new_bytes) == 0 {
                        txn.delete(fwd_sk.clone())?;
                    } else {
                        txn.put_replace(fwd_sk.clone(), Value::Bytes(new_bytes))?;
                    }
                }
            }

            // Remove from reverse adjacency list
            if let Some(Value::Bytes(rev_bytes)) = txn.get(&rev_sk)? {
                if let Some(new_bytes) = packed::remove_edge(&rev_bytes, src, edge_type) {
                    if packed::edge_count(&new_bytes) == 0 {
                        txn.delete(rev_sk.clone())?;
                    } else {
                        txn.put_replace(rev_sk.clone(), Value::Bytes(new_bytes))?;
                    }
                }
            }

            Ok(())
        })
    }

    // =========================================================================
    // Traversal helpers (used by traversal.rs)
    // =========================================================================

    /// Get outgoing neighbors of a node (optionally filtered by edge type).
    pub fn outgoing_neighbors(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> StrataResult<Vec<Neighbor>> {
        let fwd_uk = keys::forward_adj_key(graph, node_id);
        let fwd_sk = keys::storage_key(branch_id, &fwd_uk);

        self.db
            .transaction(branch_id, |txn| match txn.get(&fwd_sk)? {
                Some(Value::Bytes(bytes)) => {
                    let edges = packed::decode(&bytes)?;
                    let mut neighbors = Vec::new();
                    for (dst, edge_type, edge_data) in edges {
                        if let Some(filter) = edge_type_filter {
                            if edge_type != filter {
                                continue;
                            }
                        }
                        neighbors.push(Neighbor {
                            node_id: dst,
                            edge_type,
                            edge_data,
                        });
                    }
                    Ok(neighbors)
                }
                _ => Ok(Vec::new()),
            })
    }

    /// Get incoming neighbors of a node (optionally filtered by edge type).
    pub fn incoming_neighbors(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> StrataResult<Vec<Neighbor>> {
        let rev_uk = keys::reverse_adj_key(graph, node_id);
        let rev_sk = keys::storage_key(branch_id, &rev_uk);

        self.db
            .transaction(branch_id, |txn| match txn.get(&rev_sk)? {
                Some(Value::Bytes(bytes)) => {
                    let edges = packed::decode(&bytes)?;
                    let mut neighbors = Vec::new();
                    for (src, edge_type, edge_data) in edges {
                        if let Some(filter) = edge_type_filter {
                            if edge_type != filter {
                                continue;
                            }
                        }
                        neighbors.push(Neighbor {
                            node_id: src,
                            edge_type,
                            edge_data,
                        });
                    }
                    Ok(neighbors)
                }
                _ => Ok(Vec::new()),
            })
    }

    /// Get all edges in a graph (for snapshot).
    pub fn all_edges(&self, branch_id: BranchId, graph: &str) -> StrataResult<Vec<Edge>> {
        let prefix = keys::all_forward_adj_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut edges = Vec::new();
            for (key, val) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(src) = keys::parse_forward_adj_key(graph, &user_key) {
                        if let Value::Bytes(bytes) = val {
                            let adj = packed::decode(&bytes)?;
                            for (dst, edge_type, data) in adj {
                                edges.push(Edge {
                                    src: src.clone(),
                                    dst,
                                    edge_type,
                                    data,
                                });
                            }
                        }
                    }
                }
            }
            Ok(edges)
        })
    }

    /// Get all nodes with their data in a graph (for snapshot).
    pub fn all_nodes(
        &self,
        branch_id: BranchId,
        graph: &str,
    ) -> StrataResult<std::collections::HashMap<String, NodeData>> {
        let prefix = keys::all_nodes_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut nodes = std::collections::HashMap::new();
            for (key, val) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(node_id) = keys::parse_node_key(graph, &user_key) {
                        let data = if let Value::String(s) = val {
                            serde_json::from_str(&s).map_err(|e| {
                                StrataError::serialization(format!(
                                    "Corrupt node data in graph '{}': {}",
                                    graph, e
                                ))
                            })?
                        } else {
                            NodeData::default()
                        };
                        nodes.insert(node_id, data);
                    }
                }
            }
            Ok(nodes)
        })
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

    /// Stream edges one packed adjacency list at a time without accumulating.
    ///
    /// For each forward adjacency entry, decodes the packed list and calls `callback`
    /// with each edge. This avoids materializing all edges in memory.
    pub fn for_each_edge<F>(
        &self,
        branch_id: BranchId,
        graph: &str,
        mut callback: F,
    ) -> StrataResult<()>
    where
        F: FnMut(Edge),
    {
        let prefix = keys::all_forward_adj_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            for (key, val) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(src) = keys::parse_forward_adj_key(graph, &user_key) {
                        if let Value::Bytes(bytes) = val {
                            let adj = packed::decode(&bytes)?;
                            for (dst, edge_type, data) in adj {
                                callback(Edge {
                                    src: src.clone(),
                                    dst,
                                    edge_type,
                                    data,
                                });
                            }
                        }
                    }
                }
            }
            Ok(())
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

    // =========================================================================
    // Bulk Insert
    // =========================================================================

    /// Default chunk size for bulk insert operations.
    pub const DEFAULT_BULK_CHUNK_SIZE: usize = 250_000;

    /// Bulk insert nodes and edges into a graph using chunked transactions.
    ///
    /// Edges are grouped by source/destination and packed into adjacency lists,
    /// reducing KV entry count by ~27x compared to per-edge storage.
    ///
    /// Returns `(nodes_inserted, edges_inserted)`.
    pub fn bulk_insert(
        &self,
        branch_id: BranchId,
        graph: &str,
        nodes: &[(String, NodeData)],
        edges: &[(String, String, String, EdgeData)], // (src, dst, edge_type, data)
        chunk_size: Option<usize>,
    ) -> StrataResult<(usize, usize)> {
        keys::validate_graph_name(graph)?;

        // Read ontology status once before the loop
        let is_frozen = self
            .get_graph_meta(branch_id, graph)?
            .and_then(|m| m.ontology_status)
            == Some(types::OntologyStatus::Frozen);

        // Cache type definitions once for bulk validation (G-13).
        let obj_type_cache = if is_frozen {
            Some(self.load_object_type_cache(branch_id, graph)?)
        } else {
            None
        };
        let link_type_cache = if is_frozen {
            Some(self.load_link_type_cache(branch_id, graph)?)
        } else {
            None
        };

        let chunk_size = std::cmp::max(1, chunk_size.unwrap_or(Self::DEFAULT_BULK_CHUNK_SIZE));
        let empty_json = "{}";

        // Hoist namespace once — avoids DashMap lookup per key in inner loops.
        let ns = keys::graph_namespace(branch_id);

        // Insert nodes in chunks
        let mut nodes_inserted = 0usize;
        for chunk in nodes.chunks(chunk_size) {
            let mut entries: Vec<(Key, String)> = Vec::with_capacity(chunk.len());
            let mut ref_entries: Vec<Key> = Vec::new();
            let mut type_entries: Vec<Key> = Vec::new();

            for (node_id, data) in chunk {
                keys::validate_node_id(node_id)?;

                if is_frozen && data.object_type.is_some() {
                    if let Some(ref cache) = obj_type_cache {
                        self.validate_node_cached(graph, node_id, data, cache)?;
                    }
                }

                let user_key = keys::node_key(graph, node_id);
                let sk = Key::new_kv(ns.clone(), &user_key);

                let json = if data.entity_ref.is_none()
                    && data.properties.is_none()
                    && data.object_type.is_none()
                {
                    empty_json.to_string()
                } else {
                    serde_json::to_string(data)
                        .map_err(|e| StrataError::serialization(e.to_string()))?
                };

                entries.push((sk, json));

                if let Some(uri) = &data.entity_ref {
                    let rk = keys::ref_index_key(uri, graph, node_id);
                    ref_entries.push(Key::new_kv(ns.clone(), &rk));
                }

                if let Some(ot) = &data.object_type {
                    let tk = keys::type_index_key(graph, ot, node_id);
                    type_entries.push(Key::new_kv(ns.clone(), &tk));
                }
            }

            self.db.transaction(branch_id, |txn| {
                // Clean up old index entries for nodes being re-inserted (upsert)
                for (node_id, _data) in chunk {
                    let user_key = keys::node_key(graph, node_id);
                    let sk = Key::new_kv(ns.clone(), &user_key);
                    if let Some(Value::String(old_json)) = txn.get(&sk)? {
                        if let Ok(old_data) = serde_json::from_str::<NodeData>(&old_json) {
                            if let Some(old_uri) = old_data.entity_ref {
                                let old_rk = keys::ref_index_key(&old_uri, graph, node_id);
                                txn.delete(Key::new_kv(ns.clone(), &old_rk))?;
                            }
                            if let Some(old_ot) = old_data.object_type {
                                let old_tk = keys::type_index_key(graph, &old_ot, node_id);
                                txn.delete(Key::new_kv(ns.clone(), &old_tk))?;
                            }
                        }
                    }
                }

                for (sk, json) in &entries {
                    txn.put(sk.clone(), Value::String(json.clone()))?;
                }
                for rk in &ref_entries {
                    txn.put(rk.clone(), Value::Null)?;
                }
                for tk in &type_entries {
                    txn.put(tk.clone(), Value::Null)?;
                }
                Ok(())
            })?;

            nodes_inserted += chunk.len();
        }

        // Group edges by source (forward) and destination (reverse)
        let mut fwd_map: std::collections::HashMap<&str, Vec<(&str, &str, &EdgeData)>> =
            std::collections::HashMap::new();
        let mut rev_map: std::collections::HashMap<&str, Vec<(&str, &str, &EdgeData)>> =
            std::collections::HashMap::new();

        for (src, dst, edge_type, data) in edges {
            keys::validate_node_id(src)?;
            keys::validate_node_id(dst)?;
            keys::validate_edge_type(edge_type)?;

            if is_frozen {
                if let Some(ref cache) = link_type_cache {
                    self.validate_edge_cached(branch_id, graph, src, dst, edge_type, data, cache)?;
                }
            }

            fwd_map
                .entry(src.as_str())
                .or_default()
                .push((dst.as_str(), edge_type.as_str(), data));
            rev_map
                .entry(dst.as_str())
                .or_default()
                .push((src.as_str(), edge_type.as_str(), data));
        }

        // Write packed forward adjacency lists in chunks, merging with existing lists.
        // Reads use get_value_direct() (store-direct, no read-set tracking) so that
        // commit skips OCC validation — bulk_insert is single-writer, no conflicts.
        let fwd_entries: Vec<_> = fwd_map.into_iter().collect();
        for chunk in fwd_entries.chunks(chunk_size) {
            let mut bufs: Vec<(Key, Vec<u8>)> = Vec::with_capacity(chunk.len());
            for (node_id, new_edges) in chunk {
                let uk = keys::forward_adj_key(graph, node_id);
                let sk = Key::new_kv(ns.clone(), &uk);
                let mut buf = match self.db.get_value_direct(&sk) {
                    Some(Value::Bytes(existing)) => existing,
                    _ => packed::empty(),
                };
                for &(target_id, edge_type, data) in new_edges {
                    packed::append_edge(&mut buf, target_id, edge_type, data)?;
                }
                buf.shrink_to_fit();
                bufs.push((sk, buf));
            }
            self.db.transaction(branch_id, |txn| {
                for (sk, buf) in bufs {
                    txn.put_replace(sk, Value::Bytes(buf))?;
                }
                Ok(())
            })?;
        }

        // Write packed reverse adjacency lists in chunks, merging with existing lists.
        let rev_entries: Vec<_> = rev_map.into_iter().collect();
        for chunk in rev_entries.chunks(chunk_size) {
            let mut bufs: Vec<(Key, Vec<u8>)> = Vec::with_capacity(chunk.len());
            for (node_id, new_edges) in chunk {
                let uk = keys::reverse_adj_key(graph, node_id);
                let sk = Key::new_kv(ns.clone(), &uk);
                let mut buf = match self.db.get_value_direct(&sk) {
                    Some(Value::Bytes(existing)) => existing,
                    _ => packed::empty(),
                };
                for &(target_id, edge_type, data) in new_edges {
                    packed::append_edge(&mut buf, target_id, edge_type, data)?;
                }
                buf.shrink_to_fit();
                bufs.push((sk, buf));
            }
            self.db.transaction(branch_id, |txn| {
                for (sk, buf) in bufs {
                    txn.put_replace(sk, Value::Bytes(buf))?;
                }
                Ok(())
            })?;
        }

        Ok((nodes_inserted, edges.len()))
    }

    /// Look up all (graph, node_id) pairs bound to a given entity ref URI.
    pub fn nodes_for_entity(
        &self,
        branch_id: BranchId,
        entity_ref_uri: &str,
    ) -> StrataResult<Vec<(String, String)>> {
        let prefix = keys::ref_index_prefix(entity_ref_uri);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut entries = Vec::new();
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((_uri, graph, node_id)) = keys::parse_ref_index_key(&user_key) {
                        entries.push((graph, node_id));
                    }
                }
            }
            Ok(entries)
        })
    }

    /// Get all node IDs of a given object type via the `__by_type__` index.
    pub fn nodes_by_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        object_type: &str,
    ) -> StrataResult<Vec<String>> {
        let prefix = keys::type_index_prefix(graph, object_type);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut node_ids = Vec::new();
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((_ot, node_id)) = keys::parse_type_index_key(graph, &user_key) {
                        node_ids.push(node_id);
                    }
                }
            }
            Ok(node_ids)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::cache().unwrap();
        let graph = GraphStore::new(db.clone());
        (db, graph)
    }

    fn default_branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    // =========================================================================
    // Graph lifecycle
    // =========================================================================

    #[test]
    fn create_graph_then_get_meta() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "test_graph", None).unwrap();
        let meta = gs.get_graph_meta(branch, "test_graph").unwrap();
        assert!(meta.is_some());
        assert_eq!(meta.unwrap().cascade_policy, CascadePolicy::Ignore);
    }

    #[test]
    fn list_graphs_after_creating_3() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "g1", None).unwrap();
        gs.create_graph(branch, "g2", None).unwrap();
        gs.create_graph(branch, "g3", None).unwrap();

        let mut graphs = gs.list_graphs(branch).unwrap();
        graphs.sort();
        assert_eq!(graphs, vec!["g1", "g2", "g3"]);
    }

    #[test]
    fn delete_graph_removes_meta() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "to_delete", None).unwrap();
        gs.delete_graph(branch, "to_delete").unwrap();
        assert!(gs.get_graph_meta(branch, "to_delete").unwrap().is_none());
    }

    #[test]
    fn delete_graph_removes_nodes_and_edges() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "dg", None).unwrap();
        gs.add_node(branch, "dg", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "dg", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "dg", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        gs.delete_graph(branch, "dg").unwrap();
        assert!(gs.get_node(branch, "dg", "A").unwrap().is_none());
        assert!(gs.get_node(branch, "dg", "B").unwrap().is_none());
        assert!(gs
            .get_edge(branch, "dg", "A", "B", "KNOWS")
            .unwrap()
            .is_none());
    }

    #[test]
    fn create_graph_invalid_name_errors() {
        let (_db, gs) = setup();
        let branch = default_branch();
        assert!(gs.create_graph(branch, "", None).is_err());
        assert!(gs.create_graph(branch, "has/slash", None).is_err());
        assert!(gs.create_graph(branch, "__reserved", None).is_err());
    }

    // =========================================================================
    // Node CRUD
    // =========================================================================

    #[test]
    fn add_node_then_get() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        gs.add_node(
            branch,
            "ng",
            "n1",
            NodeData {
                entity_ref: None,
                properties: Some(serde_json::json!({"name": "Alice"})),
                ..Default::default()
            },
        )
        .unwrap();

        let node = gs.get_node(branch, "ng", "n1").unwrap().unwrap();
        assert_eq!(node.properties, Some(serde_json::json!({"name": "Alice"})));
    }

    #[test]
    fn add_node_with_entity_ref() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        gs.add_node(
            branch,
            "ng",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/patient-4821".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        let node = gs.get_node(branch, "ng", "n1").unwrap().unwrap();
        assert_eq!(node.entity_ref, Some("kv://main/patient-4821".to_string()));
    }

    #[test]
    fn add_node_without_entity_ref() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        gs.add_node(branch, "ng", "n1", NodeData::default())
            .unwrap();

        let node = gs.get_node(branch, "ng", "n1").unwrap().unwrap();
        assert!(node.entity_ref.is_none());
    }

    #[test]
    fn list_nodes_returns_all() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        for id in &["a", "b", "c"] {
            gs.add_node(branch, "ng", id, NodeData::default()).unwrap();
        }

        let mut nodes = gs.list_nodes(branch, "ng").unwrap();
        nodes.sort();
        assert_eq!(nodes, vec!["a", "b", "c"]);
    }

    #[test]
    fn remove_node_then_get_returns_none() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        gs.add_node(branch, "ng", "n1", NodeData::default())
            .unwrap();
        gs.remove_node(branch, "ng", "n1").unwrap();
        assert!(gs.get_node(branch, "ng", "n1").unwrap().is_none());
    }

    #[test]
    fn remove_node_removes_incident_edges() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        for id in &["A", "B", "C"] {
            gs.add_node(branch, "ng", id, NodeData::default()).unwrap();
        }
        gs.add_edge(branch, "ng", "A", "B", "E1", EdgeData::default())
            .unwrap();
        gs.add_edge(branch, "ng", "C", "A", "E2", EdgeData::default())
            .unwrap();

        gs.remove_node(branch, "ng", "A").unwrap();

        // Both edges involving A should be gone
        assert!(gs.get_edge(branch, "ng", "A", "B", "E1").unwrap().is_none());
        assert!(gs.get_edge(branch, "ng", "C", "A", "E2").unwrap().is_none());
        // B and C still exist
        assert!(gs.get_node(branch, "ng", "B").unwrap().is_some());
        assert!(gs.get_node(branch, "ng", "C").unwrap().is_some());
    }

    #[test]
    fn add_node_invalid_id_errors() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        assert!(gs.add_node(branch, "ng", "", NodeData::default(),).is_err());
    }

    // =========================================================================
    // Edge CRUD
    // =========================================================================

    #[test]
    fn add_edge_then_get() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_node(branch, "eg", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "eg", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "eg", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        let edge = gs.get_edge(branch, "eg", "A", "B", "KNOWS").unwrap();
        assert!(edge.is_some());
        assert_eq!(edge.unwrap().weight, 1.0);
    }

    #[test]
    fn add_edge_with_custom_weight_and_properties() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_node(branch, "eg", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "eg", "B", NodeData::default()).unwrap();
        gs.add_edge(
            branch,
            "eg",
            "A",
            "B",
            "SCORED",
            EdgeData {
                weight: 0.95,
                properties: Some(serde_json::json!({"source": "manual"})),
            },
        )
        .unwrap();

        let edge = gs
            .get_edge(branch, "eg", "A", "B", "SCORED")
            .unwrap()
            .unwrap();
        assert_eq!(edge.weight, 0.95);
        assert_eq!(
            edge.properties,
            Some(serde_json::json!({"source": "manual"}))
        );
    }

    #[test]
    fn add_edge_creates_both_forward_and_reverse() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_node(branch, "eg", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "eg", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "eg", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        // Verify via raw key reads — packed adjacency lists
        let fwd_uk = keys::forward_adj_key("eg", "A");
        let rev_uk = keys::reverse_adj_key("eg", "B");
        let fwd_sk = keys::storage_key(branch, &fwd_uk);
        let rev_sk = keys::storage_key(branch, &rev_uk);

        let fwd_exists = gs.db.transaction(branch, |txn| txn.get(&fwd_sk)).unwrap();
        let rev_exists = gs.db.transaction(branch, |txn| txn.get(&rev_sk)).unwrap();
        assert!(fwd_exists.is_some());
        assert!(rev_exists.is_some());

        // Verify the packed list contains the expected edge
        if let Some(Value::Bytes(bytes)) = fwd_exists {
            assert!(packed::find_edge(&bytes, "B", "KNOWS").is_some());
        } else {
            panic!("Forward adjacency list should be Value::Bytes");
        }
    }

    #[test]
    fn remove_edge_removes_both_entries() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_node(branch, "eg", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "eg", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "eg", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.remove_edge(branch, "eg", "A", "B", "KNOWS").unwrap();

        assert!(gs
            .get_edge(branch, "eg", "A", "B", "KNOWS")
            .unwrap()
            .is_none());

        // Verify reverse adjacency list no longer contains the edge
        let rev_uk = keys::reverse_adj_key("eg", "B");
        let rev_sk = keys::storage_key(branch, &rev_uk);
        let rev_val = gs.db.transaction(branch, |txn| txn.get(&rev_sk)).unwrap();
        // Should be deleted entirely (last edge was removed)
        assert!(rev_val.is_none());
    }

    #[test]
    fn add_edge_default_weight() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_node(branch, "eg", "X", NodeData::default()).unwrap();
        gs.add_node(branch, "eg", "Y", NodeData::default()).unwrap();
        gs.add_edge(branch, "eg", "X", "Y", "LINKS", EdgeData::default())
            .unwrap();

        let edge = gs
            .get_edge(branch, "eg", "X", "Y", "LINKS")
            .unwrap()
            .unwrap();
        assert_eq!(edge.weight, 1.0);
    }

    // =========================================================================
    // Invariant tests
    // =========================================================================

    #[test]
    fn delete_graph_does_not_affect_other_graphs() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "gA", None).unwrap();
        gs.create_graph(branch, "gB", None).unwrap();
        gs.add_node(branch, "gA", "n1", NodeData::default())
            .unwrap();
        gs.add_node(branch, "gB", "n1", NodeData::default())
            .unwrap();

        gs.delete_graph(branch, "gA").unwrap();

        // gB should be intact
        assert!(gs.get_graph_meta(branch, "gB").unwrap().is_some());
        assert!(gs.get_node(branch, "gB", "n1").unwrap().is_some());
    }

    #[test]
    fn delete_nonexistent_graph_is_ok() {
        let (_db, gs) = setup();
        let branch = default_branch();
        // Should not error — idempotent
        gs.delete_graph(branch, "nonexistent").unwrap();
    }

    #[test]
    fn add_edge_existing_overwrites() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_node(branch, "eg", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "eg", "B", NodeData::default()).unwrap();
        gs.add_edge(
            branch,
            "eg",
            "A",
            "B",
            "E",
            EdgeData {
                weight: 1.0,
                properties: None,
            },
        )
        .unwrap();
        gs.add_edge(
            branch,
            "eg",
            "A",
            "B",
            "E",
            EdgeData {
                weight: 2.0,
                properties: None,
            },
        )
        .unwrap();

        let edge = gs.get_edge(branch, "eg", "A", "B", "E").unwrap().unwrap();
        assert_eq!(edge.weight, 2.0);
    }

    #[test]
    fn add_node_existing_upserts_properties() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        gs.add_node(
            branch,
            "ng",
            "n1",
            NodeData {
                entity_ref: None,
                properties: Some(serde_json::json!({"v": 1})),
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(
            branch,
            "ng",
            "n1",
            NodeData {
                entity_ref: None,
                properties: Some(serde_json::json!({"v": 2})),
                ..Default::default()
            },
        )
        .unwrap();

        let node = gs.get_node(branch, "ng", "n1").unwrap().unwrap();
        assert_eq!(node.properties, Some(serde_json::json!({"v": 2})));
    }

    #[test]
    fn ref_index_add_node_with_entity_ref() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "rg", None).unwrap();
        gs.add_node(
            branch,
            "rg",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/key1".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        let refs = gs.nodes_for_entity(branch, "kv://main/key1").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], ("rg".to_string(), "n1".to_string()));
    }

    #[test]
    fn ref_index_not_set_without_entity_ref() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "rg", None).unwrap();
        gs.add_node(branch, "rg", "n1", NodeData::default())
            .unwrap();

        let refs = gs.nodes_for_entity(branch, "kv://main/key1").unwrap();
        assert!(refs.is_empty());
    }

    #[test]
    fn ref_index_multiple_graphs_same_entity() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "g1", None).unwrap();
        gs.create_graph(branch, "g2", None).unwrap();

        let uri = "kv://main/shared";
        gs.add_node(
            branch,
            "g1",
            "n1",
            NodeData {
                entity_ref: Some(uri.to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(
            branch,
            "g2",
            "n2",
            NodeData {
                entity_ref: Some(uri.to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        let mut refs = gs.nodes_for_entity(branch, uri).unwrap();
        refs.sort();
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn ref_index_removed_on_node_delete() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "rg", None).unwrap();
        gs.add_node(
            branch,
            "rg",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/key1".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.remove_node(branch, "rg", "n1").unwrap();

        let refs = gs.nodes_for_entity(branch, "kv://main/key1").unwrap();
        assert!(refs.is_empty());
    }

    #[test]
    fn ref_index_updated_on_entity_ref_change() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "rg", None).unwrap();
        gs.add_node(
            branch,
            "rg",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/old".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        // Update with new entity_ref
        gs.add_node(
            branch,
            "rg",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/new".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        let old_refs = gs.nodes_for_entity(branch, "kv://main/old").unwrap();
        assert!(old_refs.is_empty());

        let new_refs = gs.nodes_for_entity(branch, "kv://main/new").unwrap();
        assert_eq!(new_refs.len(), 1);
    }

    #[test]
    fn delete_graph_cleans_ref_index_entries() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "rg", None).unwrap();
        gs.add_node(
            branch,
            "rg",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/key1".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(
            branch,
            "rg",
            "n2",
            NodeData {
                entity_ref: Some("kv://main/key2".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        // Verify ref index entries exist before deletion
        assert_eq!(
            gs.nodes_for_entity(branch, "kv://main/key1").unwrap().len(),
            1
        );
        assert_eq!(
            gs.nodes_for_entity(branch, "kv://main/key2").unwrap().len(),
            1
        );

        gs.delete_graph(branch, "rg").unwrap();

        // Ref index entries should be cleaned up
        assert!(gs
            .nodes_for_entity(branch, "kv://main/key1")
            .unwrap()
            .is_empty());
        assert!(gs
            .nodes_for_entity(branch, "kv://main/key2")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn delete_graph_with_ref_does_not_affect_other_graph_refs() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "gA", None).unwrap();
        gs.create_graph(branch, "gB", None).unwrap();

        let uri = "kv://main/shared";
        gs.add_node(
            branch,
            "gA",
            "n1",
            NodeData {
                entity_ref: Some(uri.to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(
            branch,
            "gB",
            "n1",
            NodeData {
                entity_ref: Some(uri.to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        gs.delete_graph(branch, "gA").unwrap();

        // gB's ref should still exist
        let refs = gs.nodes_for_entity(branch, uri).unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], ("gB".to_string(), "n1".to_string()));
    }

    #[test]
    fn add_node_then_get_verifies_all_fields() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        gs.add_node(
            branch,
            "ng",
            "patient-1",
            NodeData {
                entity_ref: Some("kv://main/p1".to_string()),
                properties: Some(serde_json::json!({"department": "cardiology", "age": 45})),
                ..Default::default()
            },
        )
        .unwrap();

        let node = gs.get_node(branch, "ng", "patient-1").unwrap().unwrap();
        assert_eq!(node.entity_ref, Some("kv://main/p1".to_string()));
        let props = node.properties.unwrap();
        assert_eq!(props["department"], "cardiology");
        assert_eq!(props["age"], 45);
    }

    #[test]
    fn add_edge_then_get_verifies_all_fields() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_node(branch, "eg", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "eg", "B", NodeData::default()).unwrap();
        gs.add_edge(
            branch,
            "eg",
            "A",
            "B",
            "SCORED",
            EdgeData {
                weight: 0.75,
                properties: Some(serde_json::json!({"source": "model", "confidence": 0.9})),
            },
        )
        .unwrap();

        let edge = gs
            .get_edge(branch, "eg", "A", "B", "SCORED")
            .unwrap()
            .unwrap();
        assert_eq!(edge.weight, 0.75);
        let props = edge.properties.unwrap();
        assert_eq!(props["source"], "model");
        assert!((props["confidence"].as_f64().unwrap() - 0.9).abs() < 1e-10);
    }

    #[test]
    fn ref_index_cleared_when_entity_ref_set_to_none() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "rg", None).unwrap();
        gs.add_node(
            branch,
            "rg",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/key1".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();

        // Update with entity_ref=None
        gs.add_node(branch, "rg", "n1", NodeData::default())
            .unwrap();

        let refs = gs.nodes_for_entity(branch, "kv://main/key1").unwrap();
        assert!(refs.is_empty());
    }

    // =========================================================================
    // Bulk Insert
    // =========================================================================

    #[test]
    fn bulk_insert_nodes_and_edges() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![
            ("A".into(), NodeData::default()),
            ("B".into(), NodeData::default()),
            ("C".into(), NodeData::default()),
        ];
        let edges: Vec<(String, String, String, EdgeData)> = vec![
            ("A".into(), "B".into(), "KNOWS".into(), EdgeData::default()),
            ("B".into(), "C".into(), "KNOWS".into(), EdgeData::default()),
        ];

        let (ni, ei) = gs.bulk_insert(branch, "bg", &nodes, &edges, None).unwrap();
        assert_eq!(ni, 3);
        assert_eq!(ei, 2);

        // Verify nodes exist and deserialize correctly
        for id in &["A", "B", "C"] {
            let node = gs.get_node(branch, "bg", id).unwrap().unwrap();
            assert!(node.entity_ref.is_none());
            assert!(node.properties.is_none());
        }

        // Verify forward edges
        let edge_ab = gs
            .get_edge(branch, "bg", "A", "B", "KNOWS")
            .unwrap()
            .unwrap();
        assert_eq!(edge_ab.weight, 1.0);
        assert!(edge_ab.properties.is_none());

        let edge_bc = gs
            .get_edge(branch, "bg", "B", "C", "KNOWS")
            .unwrap()
            .unwrap();
        assert_eq!(edge_bc.weight, 1.0);

        // Verify outgoing neighbors (forward keys)
        let out_a = gs.outgoing_neighbors(branch, "bg", "A", None).unwrap();
        assert_eq!(out_a.len(), 1);
        assert_eq!(out_a[0].node_id, "B");

        // Verify incoming neighbors (reverse keys)
        let in_b = gs.incoming_neighbors(branch, "bg", "B", None).unwrap();
        assert_eq!(in_b.len(), 1);
        assert_eq!(in_b[0].node_id, "A");

        let in_c = gs.incoming_neighbors(branch, "bg", "C", None).unwrap();
        assert_eq!(in_c.len(), 1);
        assert_eq!(in_c[0].node_id, "B");
    }

    #[test]
    fn bulk_insert_with_entity_ref_multiple() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![
            (
                "n1".into(),
                NodeData {
                    entity_ref: Some("kv://main/key1".into()),
                    properties: None,
                    ..Default::default()
                },
            ),
            (
                "n2".into(),
                NodeData {
                    entity_ref: Some("kv://main/key2".into()),
                    properties: None,
                    ..Default::default()
                },
            ),
            ("n3".into(), NodeData::default()),
            (
                "n4".into(),
                NodeData {
                    entity_ref: Some("kv://main/key1".into()),
                    properties: None,
                    ..Default::default()
                },
            ),
        ];

        gs.bulk_insert(branch, "bg", &nodes, &[], None).unwrap();

        // Verify both ref index entries for key1
        let mut refs1 = gs.nodes_for_entity(branch, "kv://main/key1").unwrap();
        refs1.sort();
        assert_eq!(refs1.len(), 2);
        assert_eq!(refs1[0], ("bg".to_string(), "n1".to_string()));
        assert_eq!(refs1[1], ("bg".to_string(), "n4".to_string()));

        // Verify ref index for key2
        let refs2 = gs.nodes_for_entity(branch, "kv://main/key2").unwrap();
        assert_eq!(refs2.len(), 1);
        assert_eq!(refs2[0], ("bg".to_string(), "n2".to_string()));

        // n3 has no entity_ref — should not appear
        let refs_none = gs.nodes_for_entity(branch, "kv://main/key3").unwrap();
        assert!(refs_none.is_empty());
    }

    #[test]
    fn bulk_insert_with_properties_full_roundtrip() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![(
            "n1".into(),
            NodeData {
                entity_ref: Some("kv://main/p1".into()),
                properties: Some(serde_json::json!({"name": "Alice", "age": 30})),
                ..Default::default()
            },
        )];
        let edges: Vec<(String, String, String, EdgeData)> = vec![(
            "n1".into(),
            "n1".into(),
            "SELF".into(),
            EdgeData {
                weight: 0.5,
                properties: Some(serde_json::json!({"reason": "test", "score": 0.9})),
            },
        )];

        gs.bulk_insert(branch, "bg", &nodes, &edges, None).unwrap();

        // Verify node data round-trips completely
        let node = gs.get_node(branch, "bg", "n1").unwrap().unwrap();
        assert_eq!(node.entity_ref, Some("kv://main/p1".to_string()));
        let props = node.properties.unwrap();
        assert_eq!(props["name"], "Alice");
        assert_eq!(props["age"], 30);

        // Verify edge data round-trips completely
        let edge = gs
            .get_edge(branch, "bg", "n1", "n1", "SELF")
            .unwrap()
            .unwrap();
        assert_eq!(edge.weight, 0.5);
        let eprops = edge.properties.unwrap();
        assert_eq!(eprops["reason"], "test");
        assert!((eprops["score"].as_f64().unwrap() - 0.9).abs() < 1e-10);
    }

    #[test]
    fn bulk_insert_default_edge_data_roundtrips() {
        // Specifically tests the "{\"weight\":1.0}" optimization path
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![
            ("A".into(), NodeData::default()),
            ("B".into(), NodeData::default()),
        ];
        let edges: Vec<(String, String, String, EdgeData)> =
            vec![("A".into(), "B".into(), "E".into(), EdgeData::default())];

        gs.bulk_insert(branch, "bg", &nodes, &edges, None).unwrap();

        // The optimized path must produce data identical to the normal path
        let edge = gs.get_edge(branch, "bg", "A", "B", "E").unwrap().unwrap();
        assert_eq!(edge.weight, 1.0);
        assert!(edge.properties.is_none());

        // Also check that outgoing/incoming see the correct weight
        let out = gs.outgoing_neighbors(branch, "bg", "A", None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].edge_data.weight, 1.0);
        assert!(out[0].edge_data.properties.is_none());

        let inc = gs.incoming_neighbors(branch, "bg", "B", None).unwrap();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].edge_data.weight, 1.0);
    }

    #[test]
    fn bulk_insert_chunk_boundaries_nodes_and_edges() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        // 7 nodes, chunk_size=3 → chunks of [3, 3, 1]
        let nodes: Vec<(String, NodeData)> = (0..7)
            .map(|i| {
                (
                    format!("n{}", i),
                    NodeData {
                        entity_ref: None,
                        properties: None,
                        ..Default::default()
                    },
                )
            })
            .collect();

        // 5 edges, edge_chunk_size = max(1, 3/2) = 1 → chunks of [1, 1, 1, 1, 1]
        let edges: Vec<(String, String, String, EdgeData)> = (0..5)
            .map(|i| {
                (
                    format!("n{}", i),
                    format!("n{}", i + 1),
                    "NEXT".into(),
                    EdgeData::default(),
                )
            })
            .collect();

        let (ni, ei) = gs
            .bulk_insert(branch, "bg", &nodes, &edges, Some(3))
            .unwrap();
        assert_eq!(ni, 7);
        assert_eq!(ei, 5);

        // Verify all nodes
        let mut listed = gs.list_nodes(branch, "bg").unwrap();
        listed.sort();
        assert_eq!(listed.len(), 7);

        // Verify all edges (including cross-chunk-boundary edges)
        for i in 0..5 {
            let edge = gs
                .get_edge(
                    branch,
                    "bg",
                    &format!("n{}", i),
                    &format!("n{}", i + 1),
                    "NEXT",
                )
                .unwrap();
            assert!(edge.is_some(), "edge n{} -> n{} missing", i, i + 1);
        }

        // Verify reverse edges work for middle node
        let in_n3 = gs.incoming_neighbors(branch, "bg", "n3", None).unwrap();
        assert_eq!(in_n3.len(), 1);
        assert_eq!(in_n3[0].node_id, "n2");
    }

    #[test]
    fn bulk_insert_chunk_size_zero_does_not_panic() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![("A".into(), NodeData::default())];

        // chunk_size=0 should be clamped to 1, not panic
        let (ni, _) = gs.bulk_insert(branch, "bg", &nodes, &[], Some(0)).unwrap();
        assert_eq!(ni, 1);
        assert!(gs.get_node(branch, "bg", "A").unwrap().is_some());
    }

    #[test]
    fn bulk_insert_chunk_size_one() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![
            ("A".into(), NodeData::default()),
            ("B".into(), NodeData::default()),
        ];
        let edges: Vec<(String, String, String, EdgeData)> =
            vec![("A".into(), "B".into(), "E".into(), EdgeData::default())];

        // chunk_size=1 → each node/edge in its own transaction
        let (ni, ei) = gs
            .bulk_insert(branch, "bg", &nodes, &edges, Some(1))
            .unwrap();
        assert_eq!(ni, 2);
        assert_eq!(ei, 1);

        assert!(gs.get_edge(branch, "bg", "A", "B", "E").unwrap().is_some());
    }

    #[test]
    fn bulk_insert_invalid_node_id_in_middle_of_batch() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![
            ("good1".into(), NodeData::default()),
            ("".into(), NodeData::default()), // invalid
            ("good2".into(), NodeData::default()),
        ];

        let result = gs.bulk_insert(branch, "bg", &nodes, &[], None);
        assert!(result.is_err());
    }

    #[test]
    fn bulk_insert_invalid_edge_fields() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        // Invalid src (empty)
        let edges_bad_src: Vec<(String, String, String, EdgeData)> =
            vec![("".into(), "B".into(), "E".into(), EdgeData::default())];
        assert!(gs
            .bulk_insert(branch, "bg", &[], &edges_bad_src, None)
            .is_err());

        // Invalid dst (empty)
        let edges_bad_dst: Vec<(String, String, String, EdgeData)> =
            vec![("A".into(), "".into(), "E".into(), EdgeData::default())];
        assert!(gs
            .bulk_insert(branch, "bg", &[], &edges_bad_dst, None)
            .is_err());

        // Invalid edge_type (empty)
        let edges_bad_type: Vec<(String, String, String, EdgeData)> =
            vec![("A".into(), "B".into(), "".into(), EdgeData::default())];
        assert!(gs
            .bulk_insert(branch, "bg", &[], &edges_bad_type, None)
            .is_err());

        // Invalid edge_type (contains /)
        let edges_slash: Vec<(String, String, String, EdgeData)> = vec![(
            "A".into(),
            "B".into(),
            "has/slash".into(),
            EdgeData::default(),
        )];
        assert!(gs
            .bulk_insert(branch, "bg", &[], &edges_slash, None)
            .is_err());
    }

    #[test]
    fn bulk_insert_empty_is_ok() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let (ni, ei) = gs.bulk_insert(branch, "bg", &[], &[], None).unwrap();
        assert_eq!(ni, 0);
        assert_eq!(ei, 0);
    }

    #[test]
    fn bulk_insert_invalid_graph_name_errors() {
        let (_db, gs) = setup();
        let branch = default_branch();

        assert!(gs.bulk_insert(branch, "", &[], &[], None).is_err());
        assert!(gs.bulk_insert(branch, "has/slash", &[], &[], None).is_err());
        assert!(gs
            .bulk_insert(branch, "__reserved", &[], &[], None)
            .is_err());
    }

    #[test]
    fn bulk_insert_bfs_traversal_works() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        // Build a small graph: A -> B -> C -> D
        let nodes: Vec<(String, NodeData)> = vec![
            ("A".into(), NodeData::default()),
            ("B".into(), NodeData::default()),
            ("C".into(), NodeData::default()),
            ("D".into(), NodeData::default()),
        ];
        let edges: Vec<(String, String, String, EdgeData)> = vec![
            ("A".into(), "B".into(), "NEXT".into(), EdgeData::default()),
            ("B".into(), "C".into(), "NEXT".into(), EdgeData::default()),
            ("C".into(), "D".into(), "NEXT".into(), EdgeData::default()),
        ];

        gs.bulk_insert(branch, "bg", &nodes, &edges, None).unwrap();

        // BFS from A with unlimited depth should find all nodes
        let result = gs
            .bfs(
                branch,
                "bg",
                "A",
                BfsOptions {
                    max_depth: 10,
                    max_nodes: None,
                    edge_types: None,
                    direction: Direction::Outgoing,
                },
            )
            .unwrap();

        assert_eq!(result.visited.len(), 4);
        assert_eq!(*result.depths.get("A").unwrap(), 0);
        assert_eq!(*result.depths.get("B").unwrap(), 1);
        assert_eq!(*result.depths.get("C").unwrap(), 2);
        assert_eq!(*result.depths.get("D").unwrap(), 3);

        // BFS from A with max_depth=1 should find A and B only
        let result_shallow = gs
            .bfs(
                branch,
                "bg",
                "A",
                BfsOptions {
                    max_depth: 1,
                    max_nodes: None,
                    edge_types: None,
                    direction: Direction::Outgoing,
                },
            )
            .unwrap();
        assert_eq!(result_shallow.visited.len(), 2);
    }

    #[test]
    fn bulk_insert_only_nodes_no_edges() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = (0..100)
            .map(|i| {
                (
                    format!("v{}", i),
                    NodeData {
                        entity_ref: None,
                        properties: None,
                        ..Default::default()
                    },
                )
            })
            .collect();

        let (ni, ei) = gs.bulk_insert(branch, "bg", &nodes, &[], Some(10)).unwrap();
        assert_eq!(ni, 100);
        assert_eq!(ei, 0);

        let listed = gs.list_nodes(branch, "bg").unwrap();
        assert_eq!(listed.len(), 100);
    }

    #[test]
    fn bulk_insert_only_edges_no_nodes() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        // bulk_insert bypasses add_edge() node existence checks for performance
        let edges: Vec<(String, String, String, EdgeData)> =
            vec![("X".into(), "Y".into(), "E".into(), EdgeData::default())];

        let (ni, ei) = gs.bulk_insert(branch, "bg", &[], &edges, None).unwrap();
        assert_eq!(ni, 0);
        assert_eq!(ei, 1);

        assert!(gs.get_edge(branch, "bg", "X", "Y", "E").unwrap().is_some());
    }

    #[test]
    fn bulk_insert_snapshot_matches() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "bg", None).unwrap();

        let nodes: Vec<(String, NodeData)> = vec![
            ("A".into(), NodeData::default()),
            ("B".into(), NodeData::default()),
        ];
        let edges: Vec<(String, String, String, EdgeData)> = vec![(
            "A".into(),
            "B".into(),
            "E1".into(),
            EdgeData {
                weight: 2.5,
                properties: None,
            },
        )];

        gs.bulk_insert(branch, "bg", &nodes, &edges, None).unwrap();

        let snap = gs.snapshot(branch, "bg").unwrap();
        assert_eq!(snap.nodes.len(), 2);
        assert!(snap.nodes.contains_key("A"));
        assert!(snap.nodes.contains_key("B"));
        assert_eq!(snap.edges.len(), 1);
        assert_eq!(snap.edges[0].src, "A");
        assert_eq!(snap.edges[0].dst, "B");
        assert_eq!(snap.edges[0].edge_type, "E1");
        assert_eq!(snap.edges[0].data.weight, 2.5);
    }

    // =========================================================================
    // G-1: Deserialization error propagation
    // =========================================================================

    #[test]
    fn test_corrupt_edge_data_returns_error() {
        let (db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "g", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        // Corrupt the packed forward adjacency list with truncated bytes
        let fwd_uk = keys::forward_adj_key("g", "A");
        let fwd_sk = keys::storage_key(branch, &fwd_uk);
        db.transaction(branch, |txn| {
            // Header says 1 edge, but no actual edge data follows
            txn.put(fwd_sk.clone(), Value::Bytes(vec![1, 0, 0, 0]))
        })
        .unwrap();

        let result = gs.outgoing_neighbors(branch, "g", "A", None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("truncated or corrupt"),
            "Error should mention corrupt data: {}",
            err
        );
    }

    #[test]
    fn test_corrupt_edge_data_incoming_returns_error() {
        let (db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "g", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        // Corrupt the packed reverse adjacency list
        let rev_uk = keys::reverse_adj_key("g", "B");
        let rev_sk = keys::storage_key(branch, &rev_uk);
        db.transaction(branch, |txn| {
            txn.put(rev_sk.clone(), Value::Bytes(vec![1, 0, 0, 0]))
        })
        .unwrap();

        let result = gs.incoming_neighbors(branch, "g", "B", None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("truncated or corrupt"),
            "Error should mention corrupt data: {}",
            err
        );
    }

    #[test]
    fn test_corrupt_edge_data_all_edges_returns_error() {
        let (db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "g", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        // Corrupt the packed forward adjacency list
        let fwd_uk = keys::forward_adj_key("g", "A");
        let fwd_sk = keys::storage_key(branch, &fwd_uk);
        db.transaction(branch, |txn| {
            txn.put(fwd_sk.clone(), Value::Bytes(vec![1, 0, 0, 0]))
        })
        .unwrap();

        let result = gs.all_edges(branch, "g");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("truncated or corrupt"),
            "Error should mention corrupt data: {}",
            err
        );
    }

    #[test]
    fn test_corrupt_node_data_returns_error() {
        let (db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();

        // Corrupt the node value directly via the underlying KV store
        let node_uk = keys::node_key("g", "A");
        let node_sk = keys::storage_key(branch, &node_uk);
        db.transaction(branch, |txn| {
            txn.put(node_sk.clone(), Value::String("NOT VALID JSON{{".into()))
        })
        .unwrap();

        let result = gs.all_nodes(branch, "g");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Corrupt node data"),
            "Error should mention corrupt node data: {}",
            err
        );
    }

    // =========================================================================
    // G-7: Node existence validation on edge creation
    // =========================================================================

    #[test]
    fn test_add_edge_rejects_nonexistent_source() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "B", NodeData::default()).unwrap();

        let result = gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Source node") && err.contains("does not exist"),
            "Error should mention source node: {}",
            err
        );

        // Verify no partial writes — neither forward nor reverse edge should exist
        assert!(gs
            .get_edge(branch, "g", "A", "B", "KNOWS")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_add_edge_rejects_nonexistent_target() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();

        let result = gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Target node") && err.contains("does not exist"),
            "Error should mention target node: {}",
            err
        );

        // Verify no partial writes
        assert!(gs
            .get_edge(branch, "g", "A", "B", "KNOWS")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_add_edge_rejects_both_nonexistent() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();

        let result = gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("does not exist"),
            "Error should mention non-existence: {}",
            err
        );
    }

    #[test]
    fn test_add_edge_succeeds_when_both_nodes_exist() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "g", "B", NodeData::default()).unwrap();

        let result = gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default());
        assert!(result.is_ok());

        // Verify edge was actually created
        let edge = gs.get_edge(branch, "g", "A", "B", "KNOWS").unwrap();
        assert!(edge.is_some());
    }

    #[test]
    fn test_add_edge_self_loop_succeeds() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();

        // Self-edge: src == dst, node exists — should succeed
        let result = gs.add_edge(branch, "g", "A", "A", "SELF", EdgeData::default());
        assert!(result.is_ok());

        let edge = gs.get_edge(branch, "g", "A", "A", "SELF").unwrap();
        assert!(edge.is_some());
    }

    #[test]
    fn test_add_edge_self_loop_nonexistent_node() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();

        // Self-edge to nonexistent node
        let result = gs.add_edge(branch, "g", "X", "X", "SELF", EdgeData::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("does not exist"),
            "Error should mention non-existence: {}",
            err
        );
    }

    // =========================================================================
    // Edge-case tests: remove_node with multiple edges to same neighbor
    // =========================================================================

    #[test]
    fn remove_node_with_multiple_edges_to_same_neighbor() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        for id in &["A", "B"] {
            gs.add_node(branch, "g", id, NodeData::default()).unwrap();
        }
        // Two different edge types from A to B
        gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(branch, "g", "A", "B", "LIKES", EdgeData::default())
            .unwrap();

        gs.remove_node(branch, "g", "A").unwrap();

        assert!(gs
            .get_edge(branch, "g", "A", "B", "KNOWS")
            .unwrap()
            .is_none());
        assert!(gs
            .get_edge(branch, "g", "A", "B", "LIKES")
            .unwrap()
            .is_none());
        // B's reverse adj list should be empty (both edges removed)
        let incoming = gs.incoming_neighbors(branch, "g", "B", None).unwrap();
        assert!(incoming.is_empty());
    }

    #[test]
    fn remove_node_with_self_loop() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "g", "B", NodeData::default()).unwrap();
        gs.add_edge(branch, "g", "A", "A", "SELF", EdgeData::default())
            .unwrap();
        gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        gs.remove_node(branch, "g", "A").unwrap();

        assert!(gs.get_node(branch, "g", "A").unwrap().is_none());
        assert!(gs
            .get_edge(branch, "g", "A", "A", "SELF")
            .unwrap()
            .is_none());
        assert!(gs
            .get_edge(branch, "g", "A", "B", "KNOWS")
            .unwrap()
            .is_none());
        // B's reverse list should be empty
        let incoming = gs.incoming_neighbors(branch, "g", "B", None).unwrap();
        assert!(incoming.is_empty());
    }

    #[test]
    fn remove_node_hub_with_many_neighbors() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();
        // Create a hub node A with edges to 10 neighbors
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();
        for i in 0..10 {
            let id = format!("N{}", i);
            gs.add_node(branch, "g", &id, NodeData::default()).unwrap();
            gs.add_edge(branch, "g", "A", &id, "E", EdgeData::default())
                .unwrap();
        }

        gs.remove_node(branch, "g", "A").unwrap();

        // All edges removed
        for i in 0..10 {
            let id = format!("N{}", i);
            assert!(gs.get_edge(branch, "g", "A", &id, "E").unwrap().is_none());
            let incoming = gs.incoming_neighbors(branch, "g", &id, None).unwrap();
            assert!(incoming.is_empty(), "N{} should have no incoming edges", i);
        }
    }

    // =========================================================================
    // Edge-case tests: bulk_insert merging with existing edges
    // =========================================================================

    #[test]
    fn bulk_insert_merges_with_existing_edges() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();

        // Insert nodes and an initial edge via add_edge
        gs.add_node(branch, "g", "A", NodeData::default()).unwrap();
        gs.add_node(branch, "g", "B", NodeData::default()).unwrap();
        gs.add_node(branch, "g", "C", NodeData::default()).unwrap();
        gs.add_edge(branch, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        // Now bulk_insert a new edge from the same source A
        let edges = vec![("A".into(), "C".into(), "LIKES".into(), EdgeData::default())];
        gs.bulk_insert(branch, "g", &[], &edges, None).unwrap();

        // Both edges should exist
        assert!(gs
            .get_edge(branch, "g", "A", "B", "KNOWS")
            .unwrap()
            .is_some());
        assert!(gs
            .get_edge(branch, "g", "A", "C", "LIKES")
            .unwrap()
            .is_some());

        // A should have 2 outgoing neighbors
        let outgoing = gs.outgoing_neighbors(branch, "g", "A", None).unwrap();
        assert_eq!(outgoing.len(), 2);
    }

    #[test]
    fn bulk_insert_two_batches_merge() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();

        let nodes = vec![
            ("A".into(), NodeData::default()),
            ("B".into(), NodeData::default()),
            ("C".into(), NodeData::default()),
            ("D".into(), NodeData::default()),
        ];
        let edges1 = vec![("A".into(), "B".into(), "E1".into(), EdgeData::default())];
        gs.bulk_insert(branch, "g", &nodes, &edges1, None).unwrap();

        // Second batch adds another edge from A
        let edges2 = vec![("A".into(), "C".into(), "E2".into(), EdgeData::default())];
        gs.bulk_insert(branch, "g", &[], &edges2, None).unwrap();

        // Third batch adds edge from A to D
        let edges3 = vec![("A".into(), "D".into(), "E3".into(), EdgeData::default())];
        gs.bulk_insert(branch, "g", &[], &edges3, None).unwrap();

        // All edges should coexist
        assert!(gs.get_edge(branch, "g", "A", "B", "E1").unwrap().is_some());
        assert!(gs.get_edge(branch, "g", "A", "C", "E2").unwrap().is_some());
        assert!(gs.get_edge(branch, "g", "A", "D", "E3").unwrap().is_some());
        let outgoing = gs.outgoing_neighbors(branch, "g", "A", None).unwrap();
        assert_eq!(outgoing.len(), 3);
    }

    #[test]
    fn bulk_insert_reverse_adj_lists_merge_correctly() {
        let (_db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "g", None).unwrap();

        let nodes = vec![
            ("A".into(), NodeData::default()),
            ("B".into(), NodeData::default()),
            ("C".into(), NodeData::default()),
        ];
        // Batch 1: A→C
        let edges1 = vec![("A".into(), "C".into(), "E1".into(), EdgeData::default())];
        gs.bulk_insert(branch, "g", &nodes, &edges1, None).unwrap();

        // Batch 2: B→C (same destination, different source)
        let edges2 = vec![("B".into(), "C".into(), "E2".into(), EdgeData::default())];
        gs.bulk_insert(branch, "g", &[], &edges2, None).unwrap();

        // C should have 2 incoming neighbors
        let incoming = gs.incoming_neighbors(branch, "g", "C", None).unwrap();
        assert_eq!(incoming.len(), 2);
        let src_ids: Vec<&str> = incoming.iter().map(|n| n.node_id.as_str()).collect();
        assert!(src_ids.contains(&"A"));
        assert!(src_ids.contains(&"B"));
    }

    #[test]
    fn bulk_insert_replace_mode_prevents_version_accumulation() {
        let (db, gs) = setup();
        let branch = default_branch();
        gs.create_graph(branch, "bg", None).unwrap();

        // Insert nodes
        let nodes: Vec<(String, NodeData)> = (0..10)
            .map(|i| (format!("n{}", i), NodeData::default()))
            .collect();
        gs.bulk_insert(branch, "bg", &nodes, &[], None).unwrap();

        // Insert edges in 5 separate batches to the same nodes
        for batch in 0..5 {
            let edges: Vec<(String, String, String, EdgeData)> = (0..5)
                .map(|i| {
                    (
                        format!("n{}", i),
                        format!("n{}", i + 5),
                        format!("E{}", batch * 5 + i),
                        EdgeData::default(),
                    )
                })
                .collect();
            gs.bulk_insert(branch, "bg", &[], &edges, Some(3)).unwrap();
        }

        // Check version counts — adj lists should have 1 version each (replaced, not accumulated)
        let (entries, versions, btree) = db
            .storage()
            .shard_stats_detailed(&branch)
            .expect("shard should exist");

        // Version ratio should be ~1.0 (each entry has exactly 1 version)
        let ratio = versions as f64 / entries as f64;
        assert!(
            ratio <= 1.01,
            "Version ratio {:.2} (entries={}, versions={}, btree={}) — \
             adj lists are accumulating versions instead of replacing!",
            ratio,
            entries,
            versions,
            btree,
        );

        // Verify edges are correct (all 25 edges should be present)
        for batch in 0..5 {
            for i in 0..5 {
                let edge_type = format!("E{}", batch * 5 + i);
                let edge = gs
                    .get_edge(
                        branch,
                        "bg",
                        &format!("n{}", i),
                        &format!("n{}", i + 5),
                        &edge_type,
                    )
                    .unwrap();
                assert!(
                    edge.is_some(),
                    "edge n{} -> n{} type {} missing",
                    i,
                    i + 5,
                    edge_type
                );
            }
        }
    }
}
