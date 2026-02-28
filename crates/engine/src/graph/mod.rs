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
mod snapshot;
pub mod traversal;
pub mod types;

use std::sync::Arc;

use strata_core::types::BranchId;
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
                txn.put(rk, Value::String(String::new()))?;
            }

            // Write type index
            if let Some(tk) = type_key.clone() {
                txn.put(tk, Value::String(String::new()))?;
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

        // Prefixes for scanning incident edges
        let fwd_prefix = keys::forward_edges_prefix(graph, node_id);
        let fwd_prefix_key = keys::storage_key(branch_id, &fwd_prefix);
        let rev_prefix = keys::reverse_edges_prefix(graph, node_id);
        let rev_prefix_key = keys::storage_key(branch_id, &rev_prefix);

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

            // Delete outgoing edges (forward + their reverse counterparts)
            let fwd_edges = txn.scan_prefix(&fwd_prefix_key)?;
            for (key, _) in fwd_edges {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((src, edge_type, dst)) =
                        keys::parse_forward_edge_key(graph, &user_key)
                    {
                        // Delete the reverse counterpart
                        let rev_key = keys::reverse_edge_key(graph, &dst, &edge_type, &src);
                        let rev_sk = keys::storage_key(branch_id, &rev_key);
                        txn.delete(rev_sk)?;
                    }
                }
                txn.delete(key)?;
            }

            // Delete incoming edges (reverse + their forward counterparts)
            let rev_edges = txn.scan_prefix(&rev_prefix_key)?;
            for (key, _) in rev_edges {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((dst, edge_type, src)) =
                        keys::parse_reverse_edge_key(graph, &user_key)
                    {
                        // Delete the forward counterpart
                        let fwd_key = keys::forward_edge_key(graph, &src, &edge_type, &dst);
                        let fwd_sk = keys::storage_key(branch_id, &fwd_key);
                        txn.delete(fwd_sk)?;
                    }
                }
                txn.delete(key)?;
            }

            // Delete the node itself
            txn.delete(node_storage_key.clone())?;
            Ok(())
        })
    }

    // =========================================================================
    // Edge CRUD
    // =========================================================================

    /// Add or update an edge in the graph.
    /// Creates both forward and reverse entries atomically.
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
                self.validate_edge(branch_id, graph, src, dst, edge_type)?;
            }
        }

        let edge_json =
            serde_json::to_string(&data).map_err(|e| StrataError::serialization(e.to_string()))?;

        let fwd = keys::forward_edge_key(graph, src, edge_type, dst);
        let rev = keys::reverse_edge_key(graph, dst, edge_type, src);
        let fwd_sk = keys::storage_key(branch_id, &fwd);
        let rev_sk = keys::storage_key(branch_id, &rev);

        self.db.transaction(branch_id, |txn| {
            txn.put(fwd_sk.clone(), Value::String(edge_json.clone()))?;
            txn.put(rev_sk.clone(), Value::String(edge_json.clone()))?;
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
        let fwd = keys::forward_edge_key(graph, src, edge_type, dst);
        let fwd_sk = keys::storage_key(branch_id, &fwd);

        self.db.transaction(branch_id, |txn| {
            let val = txn.get(&fwd_sk)?;
            match val {
                Some(Value::String(s)) => {
                    let data: EdgeData = serde_json::from_str(&s)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    Ok(Some(data))
                }
                Some(_) => Err(StrataError::serialization(
                    "Edge data is not a string".to_string(),
                )),
                None => Ok(None),
            }
        })
    }

    /// Remove an edge (both forward and reverse entries).
    pub fn remove_edge(
        &self,
        branch_id: BranchId,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<()> {
        let fwd = keys::forward_edge_key(graph, src, edge_type, dst);
        let rev = keys::reverse_edge_key(graph, dst, edge_type, src);
        let fwd_sk = keys::storage_key(branch_id, &fwd);
        let rev_sk = keys::storage_key(branch_id, &rev);

        self.db.transaction(branch_id, |txn| {
            txn.delete(fwd_sk.clone())?;
            txn.delete(rev_sk.clone())?;
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
        let prefix = match edge_type_filter {
            Some(et) => keys::forward_edges_typed_prefix(graph, node_id, et),
            None => keys::forward_edges_prefix(graph, node_id),
        };
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut neighbors = Vec::new();
            for (key, val) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((_src, edge_type, dst)) =
                        keys::parse_forward_edge_key(graph, &user_key)
                    {
                        let edge_data = if let Value::String(s) = val {
                            serde_json::from_str(&s).unwrap_or_default()
                        } else {
                            EdgeData::default()
                        };
                        neighbors.push(Neighbor {
                            node_id: dst,
                            edge_type,
                            edge_data,
                        });
                    }
                }
            }
            Ok(neighbors)
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
        let prefix = keys::reverse_edges_prefix(graph, node_id);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut neighbors = Vec::new();
            for (key, val) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((_dst, edge_type, src)) =
                        keys::parse_reverse_edge_key(graph, &user_key)
                    {
                        if let Some(filter) = edge_type_filter {
                            if edge_type != filter {
                                continue;
                            }
                        }
                        let edge_data = if let Value::String(s) = val {
                            serde_json::from_str(&s).unwrap_or_default()
                        } else {
                            EdgeData::default()
                        };
                        neighbors.push(Neighbor {
                            node_id: src,
                            edge_type,
                            edge_data,
                        });
                    }
                }
            }
            Ok(neighbors)
        })
    }

    /// Get all edges in a graph (for snapshot).
    pub fn all_edges(&self, branch_id: BranchId, graph: &str) -> StrataResult<Vec<Edge>> {
        let prefix = keys::all_edges_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut edges = Vec::new();
            for (key, val) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((src, edge_type, dst)) =
                        keys::parse_forward_edge_key(graph, &user_key)
                    {
                        let data = if let Value::String(s) = val {
                            serde_json::from_str(&s).unwrap_or_default()
                        } else {
                            EdgeData::default()
                        };
                        edges.push(Edge {
                            src,
                            dst,
                            edge_type,
                            data,
                        });
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
                            serde_json::from_str(&s).unwrap_or_default()
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
    pub const DEFAULT_BULK_CHUNK_SIZE: usize = 10_000;

    /// Bulk insert nodes and edges into a graph using chunked transactions.
    ///
    /// This is much faster than individual `add_node`/`add_edge` calls because
    /// it batches many operations into fewer transactions. Each chunk of nodes
    /// or edges is committed atomically.
    ///
    /// For best performance, use this for fresh insertion. It also handles
    /// re-insertion (upsert) correctly by cleaning up old index entries.
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

        let chunk_size = std::cmp::max(1, chunk_size.unwrap_or(Self::DEFAULT_BULK_CHUNK_SIZE));
        let empty_json = "{}";
        let default_edge_json = "{\"weight\":1.0}";

        // Insert nodes in chunks
        let mut nodes_inserted = 0usize;
        for chunk in nodes.chunks(chunk_size) {
            // Pre-compute keys and serialized values outside the transaction
            let mut entries: Vec<(strata_core::types::Key, String)> =
                Vec::with_capacity(chunk.len());
            let mut ref_entries: Vec<strata_core::types::Key> = Vec::new();
            let mut type_entries: Vec<strata_core::types::Key> = Vec::new();

            for (node_id, data) in chunk {
                keys::validate_node_id(node_id)?;

                // Validate against frozen ontology
                if is_frozen && data.object_type.is_some() {
                    self.validate_node(branch_id, graph, node_id, data)?;
                }

                let user_key = keys::node_key(graph, node_id);
                let sk = keys::storage_key(branch_id, &user_key);

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
                    ref_entries.push(keys::storage_key(branch_id, &rk));
                }

                if let Some(ot) = &data.object_type {
                    let tk = keys::type_index_key(graph, ot, node_id);
                    type_entries.push(keys::storage_key(branch_id, &tk));
                }
            }

            self.db.transaction(branch_id, |txn| {
                // Clean up old index entries for nodes being re-inserted (upsert)
                for (node_id, _data) in chunk {
                    let user_key = keys::node_key(graph, node_id);
                    let sk = keys::storage_key(branch_id, &user_key);
                    if let Some(Value::String(old_json)) = txn.get(&sk)? {
                        if let Ok(old_data) = serde_json::from_str::<NodeData>(&old_json) {
                            if let Some(old_uri) = old_data.entity_ref {
                                let old_rk = keys::ref_index_key(&old_uri, graph, node_id);
                                txn.delete(keys::storage_key(branch_id, &old_rk))?;
                            }
                            if let Some(old_ot) = old_data.object_type {
                                let old_tk = keys::type_index_key(graph, &old_ot, node_id);
                                txn.delete(keys::storage_key(branch_id, &old_tk))?;
                            }
                        }
                    }
                }

                for (sk, json) in &entries {
                    txn.put(sk.clone(), Value::String(json.clone()))?;
                }
                for rk in &ref_entries {
                    txn.put(rk.clone(), Value::String(String::new()))?;
                }
                for tk in &type_entries {
                    txn.put(tk.clone(), Value::String(String::new()))?;
                }
                Ok(())
            })?;

            nodes_inserted += chunk.len();
        }

        // Insert edges in chunks (each edge = 2 puts, so use chunk_size/2)
        let edge_chunk_size = std::cmp::max(1, chunk_size / 2);
        let mut edges_inserted = 0usize;
        for chunk in edges.chunks(edge_chunk_size) {
            let mut entries: Vec<(strata_core::types::Key, strata_core::types::Key, String)> =
                Vec::with_capacity(chunk.len());

            for (src, dst, edge_type, data) in chunk {
                keys::validate_node_id(src)?;
                keys::validate_node_id(dst)?;
                keys::validate_edge_type(edge_type)?;

                // Validate edge against frozen ontology
                if is_frozen {
                    self.validate_edge(branch_id, graph, src, dst, edge_type)?;
                }

                let fwd = keys::forward_edge_key(graph, src, edge_type, dst);
                let rev = keys::reverse_edge_key(graph, dst, edge_type, src);
                let fwd_sk = keys::storage_key(branch_id, &fwd);
                let rev_sk = keys::storage_key(branch_id, &rev);

                let json = if data.properties.is_none() && (data.weight - 1.0).abs() < f64::EPSILON
                {
                    default_edge_json.to_string()
                } else {
                    serde_json::to_string(data)
                        .map_err(|e| StrataError::serialization(e.to_string()))?
                };

                entries.push((fwd_sk, rev_sk, json));
            }

            self.db.transaction(branch_id, |txn| {
                for (fwd_sk, rev_sk, json) in &entries {
                    txn.put(fwd_sk.clone(), Value::String(json.clone()))?;
                    txn.put(rev_sk.clone(), Value::String(json.clone()))?;
                }
                Ok(())
            })?;

            edges_inserted += chunk.len();
        }

        Ok((nodes_inserted, edges_inserted))
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
        gs.add_edge(branch, "eg", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        // Verify via raw key reads
        let fwd = keys::forward_edge_key("eg", "A", "KNOWS", "B");
        let rev = keys::reverse_edge_key("eg", "B", "KNOWS", "A");
        let fwd_sk = keys::storage_key(branch, &fwd);
        let rev_sk = keys::storage_key(branch, &rev);

        let fwd_exists = gs.db.transaction(branch, |txn| txn.get(&fwd_sk)).unwrap();
        let rev_exists = gs.db.transaction(branch, |txn| txn.get(&rev_sk)).unwrap();
        assert!(fwd_exists.is_some());
        assert!(rev_exists.is_some());
    }

    #[test]
    fn remove_edge_removes_both_entries() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
        gs.add_edge(branch, "eg", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.remove_edge(branch, "eg", "A", "B", "KNOWS").unwrap();

        assert!(gs
            .get_edge(branch, "eg", "A", "B", "KNOWS")
            .unwrap()
            .is_none());

        // Verify reverse is also gone
        let rev = keys::reverse_edge_key("eg", "B", "KNOWS", "A");
        let rev_sk = keys::storage_key(branch, &rev);
        let rev_exists = gs.db.transaction(branch, |txn| txn.get(&rev_sk)).unwrap();
        assert!(rev_exists.is_none());
    }

    #[test]
    fn add_edge_default_weight() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "eg", None).unwrap();
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

        // Edges without corresponding node entries (graph allows this)
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
}
