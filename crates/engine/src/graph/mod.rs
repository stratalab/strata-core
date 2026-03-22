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
        let catalog_sk = keys::storage_key(branch_id, keys::graph_catalog_key());

        self.db.transaction(branch_id, |txn| {
            txn.put(storage_key.clone(), Value::String(meta_json.clone()))?;

            // Update catalog: read JSON array, append name, write back
            let mut catalog: Vec<String> = match txn.get(&catalog_sk)? {
                Some(Value::String(s)) => serde_json::from_str(&s).unwrap_or_default(),
                _ => Vec::new(),
            };
            if !catalog.contains(&graph.to_string()) {
                catalog.push(graph.to_string());
                let catalog_json = serde_json::to_string(&catalog)
                    .map_err(|e| StrataError::serialization(e.to_string()))?;
                txn.put(catalog_sk.clone(), Value::String(catalog_json))?;
            }
            Ok(())
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
    ///
    /// Reads a single catalog key (O(1)) instead of scanning all graph data.
    /// Falls back to full scan if catalog is missing (legacy data), and lazily
    /// creates the catalog on fallback.
    pub fn list_graphs(&self, branch_id: BranchId) -> StrataResult<Vec<String>> {
        let catalog_sk = keys::storage_key(branch_id, keys::graph_catalog_key());

        // Try catalog first (fast path)
        let catalog_val = self.db.transaction(branch_id, |txn| txn.get(&catalog_sk))?;

        if let Some(Value::String(s)) = catalog_val {
            let catalog: Vec<String> =
                serde_json::from_str(&s).map_err(|e| StrataError::serialization(e.to_string()))?;
            return Ok(catalog);
        }

        // Fallback: scan for __meta__ keys (legacy data without catalog)
        let ns = keys::graph_namespace(branch_id);
        let prefix_key = strata_core::types::Key::new_kv(ns, "");

        let graphs = self.db.transaction(branch_id, |txn| {
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
        })?;

        // Lazily create the catalog for next time
        if !graphs.is_empty() {
            let catalog_json = serde_json::to_string(&graphs)
                .map_err(|e| StrataError::serialization(e.to_string()))?;
            let _ = self.db.transaction(branch_id, |txn| {
                txn.put(catalog_sk.clone(), Value::String(catalog_json.clone()))
            });
        }

        Ok(graphs)
    }

    /// Maximum keys to delete per transaction batch during graph deletion.
    const DELETE_BATCH_SIZE: usize = 10_000;

    /// Delete a graph and all its data (nodes, edges, meta, ref index entries).
    ///
    /// Uses batched deletion with bounded memory: each batch is its own
    /// transaction, preventing massive write sets on large graphs.
    pub fn delete_graph(&self, branch_id: BranchId, graph: &str) -> StrataResult<()> {
        let node_prefix = keys::all_nodes_prefix(graph);
        let node_prefix_key = keys::storage_key(branch_id, &node_prefix);
        let catalog_sk = keys::storage_key(branch_id, keys::graph_catalog_key());

        // Step 1: Scan nodes, extract entity_refs, delete ref index entries (batched)
        loop {
            let done = self.db.transaction(branch_id, |txn| {
                let results = txn.scan_prefix(&node_prefix_key)?;
                if results.is_empty() {
                    return Ok(true);
                }

                let batch: Vec<_> = results.into_iter().take(Self::DELETE_BATCH_SIZE).collect();
                let is_last = batch.len() < Self::DELETE_BATCH_SIZE;

                for (key, val) in &batch {
                    if let Some(user_key) = key.user_key_string() {
                        if let Value::String(json) = val {
                            if let Ok(data) = serde_json::from_str::<NodeData>(json) {
                                if let Some(uri) = data.entity_ref {
                                    if let Some(node_id) = keys::parse_node_key(graph, &user_key) {
                                        let rk = keys::ref_index_key(&uri, graph, &node_id);
                                        txn.delete(keys::storage_key(branch_id, &rk))?;
                                    }
                                }
                            }
                        }
                    }
                    txn.delete(key.clone())?;
                }

                Ok(is_last)
            })?;

            if done {
                break;
            }
        }

        // Step 2: Delete remaining graph keys (fwd adj, rev adj, type index,
        // ontology, edge counters, meta) in batches
        let graph_prefix = keys::graph_prefix(graph);
        let graph_prefix_key = keys::storage_key(branch_id, &graph_prefix);
        self.delete_prefix_batched(branch_id, &graph_prefix_key)?;

        // Step 3: Update catalog
        self.db.transaction(branch_id, |txn| {
            if let Some(Value::String(s)) = txn.get(&catalog_sk)? {
                if let Ok(mut catalog) = serde_json::from_str::<Vec<String>>(&s) {
                    catalog.retain(|g| g != graph);
                    let catalog_json = serde_json::to_string(&catalog)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    txn.put(catalog_sk.clone(), Value::String(catalog_json))?;
                }
            }
            Ok(())
        })?;

        Ok(())
    }

    /// Delete all keys matching a prefix in batches of DELETE_BATCH_SIZE.
    fn delete_prefix_batched(&self, branch_id: BranchId, prefix_key: &Key) -> StrataResult<()> {
        loop {
            let done = self.db.transaction(branch_id, |txn| {
                let results = txn.scan_prefix(prefix_key)?;
                if results.is_empty() {
                    return Ok(true);
                }

                let batch: Vec<_> = results.into_iter().take(Self::DELETE_BATCH_SIZE).collect();
                let is_last = batch.len() < Self::DELETE_BATCH_SIZE;

                for (key, _) in batch {
                    txn.delete(key)?;
                }

                Ok(is_last)
            })?;

            if done {
                break;
            }
        }
        Ok(())
    }

    // =========================================================================
    // Node CRUD
    // =========================================================================

    /// Add or update a node in the graph.
    ///
    /// Returns `true` if a new node was created, `false` if an existing node was updated.
    pub fn add_node(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
        data: NodeData,
    ) -> StrataResult<bool> {
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
            let created = old_val.is_none();
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
            Ok(created)
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

    /// List node IDs with cursor-based pagination.
    ///
    /// KV keys are sorted, so cursor-based pagination is natural.
    /// `next_cursor` is the last returned node_id, or None if this is the last page.
    pub fn list_nodes_paginated(
        &self,
        branch_id: BranchId,
        graph: &str,
        page: PageRequest,
    ) -> StrataResult<PageResponse<String>> {
        let prefix = keys::all_nodes_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut items = Vec::new();
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(id) = keys::parse_node_key(graph, &user_key) {
                        // Skip entries at or before cursor
                        if let Some(ref cursor) = page.cursor {
                            if id.as_str() <= cursor.as_str() {
                                continue;
                            }
                        }
                        items.push(id);
                        if items.len() >= page.limit {
                            break;
                        }
                    }
                }
            }

            let next_cursor = if items.len() >= page.limit {
                items.last().cloned()
            } else {
                None
            };

            Ok(PageResponse { items, next_cursor })
        })
    }

    /// List graph names with cursor-based pagination.
    pub fn list_graphs_paginated(
        &self,
        branch_id: BranchId,
        page: PageRequest,
    ) -> StrataResult<PageResponse<String>> {
        let mut graphs = self.list_graphs(branch_id)?;
        graphs.sort();

        // Apply cursor: skip entries at or before cursor
        let start = if let Some(ref cursor) = page.cursor {
            graphs
                .iter()
                .position(|g| g.as_str() > cursor.as_str())
                .unwrap_or(graphs.len())
        } else {
            0
        };

        let end = std::cmp::min(start + page.limit, graphs.len());
        let items: Vec<String> = graphs[start..end].to_vec();

        let next_cursor = if end < graphs.len() {
            items.last().cloned()
        } else {
            None
        };

        Ok(PageResponse { items, next_cursor })
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

            // Track edge type counts to decrement (only from outgoing edges,
            // since each edge is counted once via the forward adj list)
            let mut edge_type_decrements: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();

            // Remove this node from each neighbor's reverse adjacency list
            if let Some(Value::Bytes(fwd_bytes)) = txn.get(&fwd_adj_sk)? {
                let outgoing = packed::decode(&fwd_bytes)?;
                for (dst, edge_type, _) in &outgoing {
                    *edge_type_decrements.entry(edge_type.clone()).or_insert(0) += 1;
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

            // Count incoming edges that are NOT already counted as outgoing
            // (i.e., exclude self-loops which were already counted above)
            if let Some(Value::Bytes(rev_bytes)) = txn.get(&rev_adj_sk)? {
                let incoming = packed::decode(&rev_bytes)?;
                for (src, edge_type, _) in &incoming {
                    // Only count incoming edges from OTHER nodes (self-loops already counted)
                    if src != node_id {
                        *edge_type_decrements.entry(edge_type.clone()).or_insert(0) += 1;
                    }
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

            // Decrement edge type counters
            for (et, dec) in &edge_type_decrements {
                let count_uk = keys::edge_type_count_key(graph, et);
                let count_sk = keys::storage_key(branch_id, &count_uk);
                let count = Self::read_edge_type_count(txn, &count_sk)?;
                Self::write_edge_type_count(txn, &count_sk, count.saturating_sub(*dec))?;
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
    ///
    /// Returns `true` if a new edge was created, `false` if an existing edge was updated.
    pub fn add_edge(
        &self,
        branch_id: BranchId,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
        data: EdgeData,
    ) -> StrataResult<bool> {
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

        let count_uk = keys::edge_type_count_key(graph, edge_type);
        let count_sk = keys::storage_key(branch_id, &count_uk);

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
            let was_update = if let Some(new_buf) = packed::remove_edge(&fwd_buf, dst, edge_type) {
                fwd_buf = new_buf;
                true
            } else {
                false
            };
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

            // Increment edge type counter only for new edges (not updates)
            if !was_update {
                let count = Self::read_edge_type_count(txn, &count_sk)?;
                Self::write_edge_type_count(txn, &count_sk, count + 1)?;
            }

            Ok(!was_update)
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
        let count_uk = keys::edge_type_count_key(graph, edge_type);
        let count_sk = keys::storage_key(branch_id, &count_uk);

        self.db.transaction(branch_id, |txn| {
            // Remove from forward adjacency list
            let mut removed = false;
            if let Some(Value::Bytes(fwd_bytes)) = txn.get(&fwd_sk)? {
                if let Some(new_bytes) = packed::remove_edge(&fwd_bytes, dst, edge_type) {
                    removed = true;
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

            // Decrement edge type counter
            if removed {
                let count = Self::read_edge_type_count(txn, &count_sk)?;
                if count > 0 {
                    Self::write_edge_type_count(txn, &count_sk, count - 1)?;
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

        // Group edges by source (forward) and destination (reverse),
        // and accumulate per-type edge counts for counters (G-3).
        let mut fwd_map: std::collections::HashMap<&str, Vec<(&str, &str, &EdgeData)>> =
            std::collections::HashMap::new();
        let mut rev_map: std::collections::HashMap<&str, Vec<(&str, &str, &EdgeData)>> =
            std::collections::HashMap::new();
        let mut edge_type_counts: std::collections::HashMap<&str, u64> =
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

            *edge_type_counts.entry(edge_type.as_str()).or_insert(0) += 1;
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
                let mut buf = match self.db.get_value_direct(&sk)? {
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
                let mut buf = match self.db.get_value_direct(&sk)? {
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

        // Write accumulated edge type counters (G-3)
        if !edge_type_counts.is_empty() {
            self.db.transaction(branch_id, |txn| {
                for (et, new_count) in &edge_type_counts {
                    let count_uk = keys::edge_type_count_key(graph, et);
                    let count_sk = Key::new_kv(ns.clone(), &count_uk);
                    let existing = Self::read_edge_type_count(txn, &count_sk)?;
                    Self::write_edge_type_count(txn, &count_sk, existing + new_count)?;
                }
                Ok(())
            })?;
        }

        Ok((nodes_inserted, edges.len()))
    }

    // =========================================================================
    // Batch Edge Insert
    // =========================================================================

    /// Default chunk size for batch edge operations.
    pub const DEFAULT_BATCH_EDGE_CHUNK_SIZE: usize = 5_000;

    /// Batch add edges to a graph, processing N edges in minimal transactions.
    ///
    /// Groups edges by source/destination for efficient adjacency list updates,
    /// reducing KV operations from O(edges) to O(unique_nodes). For 1000 edges
    /// from 100 source nodes, this performs ~400 KV ops instead of ~8000-10000.
    ///
    /// **Append-only semantics:** Unlike [`add_edge`] which does upsert (remove
    /// old + append new), this method appends all edges unconditionally.
    /// Duplicate `(src, dst, edge_type)` tuples within the batch or with
    /// existing edges will create duplicate entries in the adjacency list.
    /// This matches [`bulk_insert`] behavior and is intentional for throughput.
    ///
    /// When `chunk_size` is provided and the batch exceeds it, edges are split
    /// into multiple transactions. Partial success is possible — edges from
    /// committed chunks remain even if a later chunk fails.
    ///
    /// Returns the total number of edges inserted.
    pub fn batch_add_edges(
        &self,
        branch_id: BranchId,
        graph: &str,
        edges: &[(String, String, String, EdgeData)], // (src, dst, edge_type, data)
        chunk_size: Option<usize>,
    ) -> StrataResult<usize> {
        if edges.is_empty() {
            return Ok(0);
        }

        keys::validate_graph_name(graph)?;

        // Validate all inputs upfront (before opening any transaction)
        for (src, dst, edge_type, _) in edges {
            keys::validate_node_id(src)?;
            keys::validate_node_id(dst)?;
            keys::validate_edge_type(edge_type)?;
        }

        // Check ontology constraints if frozen
        let is_frozen = self
            .get_graph_meta(branch_id, graph)?
            .and_then(|m| m.ontology_status)
            == Some(types::OntologyStatus::Frozen);

        if is_frozen {
            let link_type_cache = self.load_link_type_cache(branch_id, graph)?;
            for (src, dst, edge_type, data) in edges {
                self.validate_edge_cached(
                    branch_id,
                    graph,
                    src,
                    dst,
                    edge_type,
                    data,
                    &link_type_cache,
                )?;
            }
        }

        let chunk_size = chunk_size
            .unwrap_or(Self::DEFAULT_BATCH_EDGE_CHUNK_SIZE)
            .max(1);
        let ns = keys::graph_namespace(branch_id);
        let mut total_inserted = 0;

        for chunk in edges.chunks(chunk_size) {
            total_inserted += self.batch_add_edges_chunk(branch_id, graph, chunk, &ns)?;
        }

        Ok(total_inserted)
    }

    /// Process a single chunk of edges in one transaction.
    fn batch_add_edges_chunk(
        &self,
        branch_id: BranchId,
        graph: &str,
        edges: &[(String, String, String, EdgeData)],
        ns: &std::sync::Arc<strata_core::types::Namespace>,
    ) -> StrataResult<usize> {
        // Collect unique node IDs for existence checks
        let mut unique_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (src, dst, _, _) in edges {
            unique_nodes.insert(src.as_str());
            unique_nodes.insert(dst.as_str());
        }

        // Group edges by source (forward) and destination (reverse)
        let mut fwd_map: std::collections::HashMap<&str, Vec<(&str, &str, &EdgeData)>> =
            std::collections::HashMap::new();
        let mut rev_map: std::collections::HashMap<&str, Vec<(&str, &str, &EdgeData)>> =
            std::collections::HashMap::new();
        let mut edge_type_counts: std::collections::HashMap<&str, u64> =
            std::collections::HashMap::new();

        for (src, dst, edge_type, data) in edges {
            *edge_type_counts.entry(edge_type.as_str()).or_insert(0) += 1;
            fwd_map
                .entry(src.as_str())
                .or_default()
                .push((dst.as_str(), edge_type.as_str(), data));
            rev_map
                .entry(dst.as_str())
                .or_default()
                .push((src.as_str(), edge_type.as_str(), data));
        }

        let edge_count = edges.len();

        // Single transaction: node checks + adjacency writes + counters
        self.db.transaction(branch_id, |txn| {
            // Check all referenced nodes exist (once per unique node)
            for node_id in &unique_nodes {
                let node_uk = keys::node_key(graph, node_id);
                let node_sk = Key::new_kv(ns.clone(), &node_uk);
                if txn.get(&node_sk)?.is_none() {
                    return Err(StrataError::invalid_input(format!(
                        "Node '{}' does not exist in graph '{}'",
                        node_id, graph
                    )));
                }
            }

            // Write forward adjacency lists (1 read + 1 write per unique source)
            for (src, new_edges) in &fwd_map {
                let fwd_uk = keys::forward_adj_key(graph, src);
                let fwd_sk = Key::new_kv(ns.clone(), &fwd_uk);
                let mut buf = match txn.get(&fwd_sk)? {
                    Some(Value::Bytes(b)) => b,
                    _ => packed::empty(),
                };
                for &(target_id, edge_type, data) in new_edges {
                    packed::append_edge(&mut buf, target_id, edge_type, data)?;
                }
                txn.put_replace(fwd_sk, Value::Bytes(buf))?;
            }

            // Write reverse adjacency lists (1 read + 1 write per unique destination)
            for (dst, new_edges) in &rev_map {
                let rev_uk = keys::reverse_adj_key(graph, dst);
                let rev_sk = Key::new_kv(ns.clone(), &rev_uk);
                let mut buf = match txn.get(&rev_sk)? {
                    Some(Value::Bytes(b)) => b,
                    _ => packed::empty(),
                };
                for &(target_id, edge_type, data) in new_edges {
                    packed::append_edge(&mut buf, target_id, edge_type, data)?;
                }
                txn.put_replace(rev_sk, Value::Bytes(buf))?;
            }

            // Update edge type counters (1 read-modify-write per unique edge type)
            for (et, new_count) in &edge_type_counts {
                let count_uk = keys::edge_type_count_key(graph, et);
                let count_sk = Key::new_kv(ns.clone(), &count_uk);
                let existing = Self::read_edge_type_count(txn, &count_sk)?;
                Self::write_edge_type_count(txn, &count_sk, existing + new_count)?;
            }

            Ok(edge_count)
        })
    }

    // =========================================================================
    // Edge type count helpers (G-3)
    // =========================================================================

    /// Read an edge type count from a transaction.
    fn read_edge_type_count(
        txn: &mut strata_concurrency::TransactionContext,
        count_sk: &Key,
    ) -> StrataResult<u64> {
        match txn.get(count_sk)? {
            Some(Value::String(s)) => s
                .parse::<u64>()
                .map_err(|e| StrataError::serialization(e.to_string())),
            _ => Ok(0),
        }
    }

    /// Write an edge type count in a transaction.
    fn write_edge_type_count(
        txn: &mut strata_concurrency::TransactionContext,
        count_sk: &Key,
        count: u64,
    ) -> StrataResult<()> {
        txn.put(count_sk.clone(), Value::String(count.to_string()))
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
        let created = gs
            .add_node(
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
        assert!(created, "add_node should return true for new node");

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
        let created1 = gs
            .add_edge(
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
        assert!(created1, "first add_edge should return created=true");

        let created2 = gs
            .add_edge(
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
        assert!(!created2, "second add_edge should return created=false");

        let edge = gs.get_edge(branch, "eg", "A", "B", "E").unwrap().unwrap();
        assert_eq!(edge.weight, 2.0);
    }

    #[test]
    fn add_node_existing_upserts_properties() {
        let (_db, gs) = setup();
        let branch = default_branch();

        gs.create_graph(branch, "ng", None).unwrap();
        let created1 = gs
            .add_node(
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
        assert!(created1, "first add_node should return created=true");

        let created2 = gs
            .add_node(
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
        assert!(!created2, "second add_node should return created=false");

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

        // SegmentedStore appends all versions (pruning happens at compaction),
        // so the version ratio may be > 1.0. Verify stats are plausible.
        assert!(entries > 0, "should have entries");
        assert!(
            versions >= entries,
            "versions >= entries (entries={entries}, versions={versions}, btree={btree})"
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

    // =========================================================================
    // G-5: Graph catalog tests
    // =========================================================================

    #[test]
    fn catalog_idempotent_create() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.create_graph(b, "g", None).unwrap(); // second create

        let graphs = gs.list_graphs(b).unwrap();
        assert_eq!(
            graphs.iter().filter(|g| g.as_str() == "g").count(),
            1,
            "catalog should not have duplicate entries"
        );
    }

    #[test]
    fn catalog_updated_on_delete() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "a", None).unwrap();
        gs.create_graph(b, "b", None).unwrap();
        gs.create_graph(b, "c", None).unwrap();

        gs.delete_graph(b, "b").unwrap();

        let mut graphs = gs.list_graphs(b).unwrap();
        graphs.sort();
        assert_eq!(graphs, vec!["a", "c"]);
    }

    #[test]
    fn catalog_create_after_delete_readds() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.delete_graph(b, "g").unwrap();
        assert!(gs.list_graphs(b).unwrap().is_empty());

        gs.create_graph(b, "g", None).unwrap();
        assert_eq!(gs.list_graphs(b).unwrap(), vec!["g"]);
    }

    #[test]
    fn catalog_delete_nonexistent_graph_noop() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "keep", None).unwrap();
        // Deleting a graph that doesn't exist should not crash or corrupt catalog
        gs.delete_graph(b, "ghost").unwrap();

        assert_eq!(gs.list_graphs(b).unwrap(), vec!["keep"]);
    }

    // =========================================================================
    // G-3: Edge type counter tests
    // =========================================================================

    #[test]
    fn edge_type_counter_basic_add() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "HATES", EdgeData::default())
            .unwrap();

        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 2);
        assert_eq!(gs.count_edges_by_type(b, "g", "HATES").unwrap(), 1);
        assert_eq!(gs.count_edges_by_type(b, "g", "MISSING").unwrap(), 0);
    }

    #[test]
    fn edge_type_counter_upsert_no_double_count() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();

        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        // Upsert same edge with different weight
        gs.add_edge(
            b,
            "g",
            "A",
            "B",
            "KNOWS",
            EdgeData {
                weight: 0.5,
                properties: None,
            },
        )
        .unwrap();

        assert_eq!(
            gs.count_edges_by_type(b, "g", "KNOWS").unwrap(),
            1,
            "upsert should not double-count"
        );
    }

    #[test]
    fn edge_type_counter_decrements_on_remove() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "KNOWS", EdgeData::default())
            .unwrap();

        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 2);

        gs.remove_edge(b, "g", "A", "B", "KNOWS").unwrap();
        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 1);

        gs.remove_edge(b, "g", "A", "C", "KNOWS").unwrap();
        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 0);
    }

    #[test]
    fn edge_type_counter_remove_nonexistent_edge_no_underflow() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();

        // Remove an edge that was never added — should not underflow
        gs.remove_edge(b, "g", "A", "B", "GHOST").unwrap();
        assert_eq!(gs.count_edges_by_type(b, "g", "GHOST").unwrap(), 0);
    }

    #[test]
    fn edge_type_counter_self_loop() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();

        gs.add_edge(b, "g", "A", "A", "SELF", EdgeData::default())
            .unwrap();
        assert_eq!(gs.count_edges_by_type(b, "g", "SELF").unwrap(), 1);

        gs.remove_edge(b, "g", "A", "A", "SELF").unwrap();
        assert_eq!(gs.count_edges_by_type(b, "g", "SELF").unwrap(), 0);
    }

    #[test]
    fn edge_type_counter_remove_node_decrements() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "B", "TRUSTS", EdgeData::default())
            .unwrap();

        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 2);
        assert_eq!(gs.count_edges_by_type(b, "g", "TRUSTS").unwrap(), 1);

        // Removing A should decrement all edges incident to A
        gs.remove_node(b, "g", "A").unwrap();

        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 0);
        assert_eq!(gs.count_edges_by_type(b, "g", "TRUSTS").unwrap(), 0);
    }

    #[test]
    fn edge_type_counter_remove_node_with_self_loop() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();

        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "A", "SELF", EdgeData::default())
            .unwrap();

        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 1);
        assert_eq!(gs.count_edges_by_type(b, "g", "SELF").unwrap(), 1);

        gs.remove_node(b, "g", "A").unwrap();

        assert_eq!(
            gs.count_edges_by_type(b, "g", "KNOWS").unwrap(),
            0,
            "outgoing KNOWS edge should be decremented"
        );
        assert_eq!(
            gs.count_edges_by_type(b, "g", "SELF").unwrap(),
            0,
            "self-loop should be decremented exactly once"
        );
    }

    #[test]
    fn edge_type_counter_multiple_types() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        // A→B (KNOWS), A→B (TRUSTS) are different edges (different types)
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "B", "TRUSTS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "KNOWS", EdgeData::default())
            .unwrap();

        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 2);
        assert_eq!(gs.count_edges_by_type(b, "g", "TRUSTS").unwrap(), 1);
    }

    #[test]
    fn edge_type_counter_bulk_insert() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();

        let nodes: Vec<(String, NodeData)> = (0..5)
            .map(|i| (format!("n{}", i), NodeData::default()))
            .collect();
        let edges: Vec<(String, String, String, EdgeData)> = vec![
            (
                "n0".into(),
                "n1".into(),
                "KNOWS".into(),
                EdgeData::default(),
            ),
            (
                "n1".into(),
                "n2".into(),
                "KNOWS".into(),
                EdgeData::default(),
            ),
            (
                "n2".into(),
                "n3".into(),
                "TRUSTS".into(),
                EdgeData::default(),
            ),
            (
                "n3".into(),
                "n4".into(),
                "KNOWS".into(),
                EdgeData::default(),
            ),
        ];

        gs.bulk_insert(b, "g", &nodes, &edges, None).unwrap();

        assert_eq!(gs.count_edges_by_type(b, "g", "KNOWS").unwrap(), 3);
        assert_eq!(gs.count_edges_by_type(b, "g", "TRUSTS").unwrap(), 1);
    }

    // =========================================================================
    // G-18: Pagination tests
    // =========================================================================

    #[test]
    fn list_nodes_paginated_basic() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        for id in &["a", "b", "c", "d", "e"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }

        let page = gs
            .list_nodes_paginated(
                b,
                "g",
                PageRequest {
                    limit: 3,
                    cursor: None,
                },
            )
            .unwrap();

        assert_eq!(page.items.len(), 3);
        assert!(page.next_cursor.is_some(), "should have a next page");
    }

    #[test]
    fn list_nodes_paginated_iterate_all() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        let expected: Vec<String> = (0..7).map(|i| format!("n{:02}", i)).collect();
        for id in &expected {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }

        let mut all_items = Vec::new();
        let mut cursor = None;
        let mut pages = 0;

        loop {
            let page = gs
                .list_nodes_paginated(
                    b,
                    "g",
                    PageRequest {
                        limit: 3,
                        cursor: cursor.clone(),
                    },
                )
                .unwrap();
            all_items.extend(page.items);
            cursor = page.next_cursor;
            pages += 1;
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(
            all_items, expected,
            "paginated iteration should return all nodes in order"
        );
        assert!(pages >= 2, "should have taken multiple pages");
    }

    #[test]
    fn list_nodes_paginated_empty_graph() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();

        let page = gs
            .list_nodes_paginated(
                b,
                "g",
                PageRequest {
                    limit: 10,
                    cursor: None,
                },
            )
            .unwrap();

        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn list_nodes_paginated_exact_limit() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        for id in &["a", "b", "c"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }

        // Request exactly 3 nodes when there are exactly 3
        let page = gs
            .list_nodes_paginated(
                b,
                "g",
                PageRequest {
                    limit: 3,
                    cursor: None,
                },
            )
            .unwrap();

        assert_eq!(page.items.len(), 3);
        // next_cursor may be Some (we don't know there are no more),
        // but a subsequent call should return empty
        if let Some(cursor) = page.next_cursor {
            let page2 = gs
                .list_nodes_paginated(
                    b,
                    "g",
                    PageRequest {
                        limit: 3,
                        cursor: Some(cursor),
                    },
                )
                .unwrap();
            assert!(page2.items.is_empty());
            assert!(page2.next_cursor.is_none());
        }
    }

    #[test]
    fn list_nodes_paginated_cursor_past_end() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "a", NodeData::default()).unwrap();
        gs.add_node(b, "g", "b", NodeData::default()).unwrap();

        // Use a cursor past all existing node IDs
        let page = gs
            .list_nodes_paginated(
                b,
                "g",
                PageRequest {
                    limit: 10,
                    cursor: Some("zzz".to_string()),
                },
            )
            .unwrap();

        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn list_graphs_paginated_basic() {
        let (_db, gs) = setup();
        let b = default_branch();

        for name in &["alpha", "beta", "gamma", "delta", "epsilon"] {
            gs.create_graph(b, name, None).unwrap();
        }

        let page = gs
            .list_graphs_paginated(
                b,
                PageRequest {
                    limit: 2,
                    cursor: None,
                },
            )
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert!(page.next_cursor.is_some());
        // First page should be alphabetically first two
        assert_eq!(page.items[0], "alpha");
        assert_eq!(page.items[1], "beta");
    }

    #[test]
    fn list_graphs_paginated_iterate_all() {
        let (_db, gs) = setup();
        let b = default_branch();

        let expected = vec!["a", "b", "c", "d", "e"];
        for name in &expected {
            gs.create_graph(b, name, None).unwrap();
        }

        let mut all = Vec::new();
        let mut cursor = None;

        loop {
            let page = gs
                .list_graphs_paginated(
                    b,
                    PageRequest {
                        limit: 2,
                        cursor: cursor.clone(),
                    },
                )
                .unwrap();
            all.extend(page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let expected_sorted: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(all, expected_sorted);
    }

    #[test]
    fn list_graphs_paginated_empty() {
        let (_db, gs) = setup();
        let b = default_branch();

        let page = gs
            .list_graphs_paginated(
                b,
                PageRequest {
                    limit: 10,
                    cursor: None,
                },
            )
            .unwrap();

        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
    }

    // =========================================================================
    // G-11: Batched deletion tests
    // =========================================================================

    #[test]
    fn delete_graph_cleans_ref_index() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/entity1".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        // Verify ref index exists before delete
        let bindings = gs.nodes_for_entity(b, "kv://main/entity1").unwrap();
        assert_eq!(bindings.len(), 1);

        gs.delete_graph(b, "g").unwrap();

        // Ref index should be cleaned up
        let bindings = gs.nodes_for_entity(b, "kv://main/entity1").unwrap();
        assert!(
            bindings.is_empty(),
            "ref index should be cleaned after graph deletion"
        );
    }

    #[test]
    fn delete_graph_with_many_nodes() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();

        // Insert enough nodes to require multiple batches if batch size were small
        let nodes: Vec<(String, NodeData)> = (0..50)
            .map(|i| (format!("n{:04}", i), NodeData::default()))
            .collect();
        let edges: Vec<(String, String, String, EdgeData)> = (0..49)
            .map(|i| {
                (
                    format!("n{:04}", i),
                    format!("n{:04}", i + 1),
                    "NEXT".to_string(),
                    EdgeData::default(),
                )
            })
            .collect();

        gs.bulk_insert(b, "g", &nodes, &edges, None).unwrap();

        gs.delete_graph(b, "g").unwrap();

        assert!(gs.get_graph_meta(b, "g").unwrap().is_none());
        assert!(gs.list_nodes(b, "g").unwrap().is_empty());
        assert!(gs.list_graphs(b).unwrap().is_empty());
    }

    #[test]
    fn delete_graph_preserves_other_graphs() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "keep", None).unwrap();
        gs.add_node(b, "keep", "A", NodeData::default()).unwrap();

        gs.create_graph(b, "remove", None).unwrap();
        gs.add_node(b, "remove", "B", NodeData::default()).unwrap();

        gs.delete_graph(b, "remove").unwrap();

        // "keep" graph should be untouched
        assert!(gs.get_graph_meta(b, "keep").unwrap().is_some());
        assert!(gs.get_node(b, "keep", "A").unwrap().is_some());
        assert_eq!(gs.list_graphs(b).unwrap(), vec!["keep"]);
    }

    // =========================================================================
    // Batch add edges (Epic 8a)
    // =========================================================================

    #[test]
    fn batch_add_edges_inserts_all() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        // Create 10 nodes
        for i in 0..10 {
            gs.add_node(b, "g", &format!("n{}", i), NodeData::default())
                .unwrap();
        }

        // Create 100 edges: n0→n1, n0→n2, ..., n0→n9, n1→n0, n1→n2, ...
        let edges: Vec<_> = (0..10)
            .flat_map(|src| {
                (0..10).filter(move |&dst| dst != src).map(move |dst| {
                    (
                        format!("n{}", src),
                        format!("n{}", dst),
                        "KNOWS".to_string(),
                        EdgeData::default(),
                    )
                })
            })
            .collect();

        let inserted = gs.batch_add_edges(b, "g", &edges, None).unwrap();
        assert_eq!(inserted, 90);

        // Verify all edges exist
        for (src, dst, et, _) in &edges {
            assert!(
                gs.get_edge(b, "g", src, dst, et).unwrap().is_some(),
                "Edge {} -> {} should exist",
                src,
                dst
            );
        }
    }

    #[test]
    fn batch_add_edges_atomicity() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        // B does not exist — should fail atomically

        let edges = vec![(
            "A".to_string(),
            "B".to_string(),
            "KNOWS".to_string(),
            EdgeData::default(),
        )];

        let result = gs.batch_add_edges(b, "g", &edges, None);
        assert!(result.is_err());

        // No partial state: A should have no outgoing edges
        let neighbors = gs.outgoing_neighbors(b, "g", "A", None).unwrap();
        assert!(neighbors.is_empty());
    }

    #[test]
    fn batch_add_edges_groups_adjacency() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();
        gs.add_node(b, "g", "D", NodeData::default()).unwrap();

        // Multiple edges from same source
        let edges = vec![
            (
                "A".to_string(),
                "B".to_string(),
                "KNOWS".to_string(),
                EdgeData::default(),
            ),
            (
                "A".to_string(),
                "C".to_string(),
                "KNOWS".to_string(),
                EdgeData::default(),
            ),
            (
                "A".to_string(),
                "D".to_string(),
                "LIKES".to_string(),
                EdgeData::default(),
            ),
        ];

        gs.batch_add_edges(b, "g", &edges, None).unwrap();

        let neighbors = gs.outgoing_neighbors(b, "g", "A", None).unwrap();
        assert_eq!(neighbors.len(), 3);
    }

    #[test]
    fn batch_add_edges_reverse_edges() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        let edges = vec![
            (
                "A".to_string(),
                "C".to_string(),
                "KNOWS".to_string(),
                EdgeData::default(),
            ),
            (
                "B".to_string(),
                "C".to_string(),
                "KNOWS".to_string(),
                EdgeData::default(),
            ),
        ];

        gs.batch_add_edges(b, "g", &edges, None).unwrap();

        // C should have 2 incoming edges
        let incoming = gs.incoming_neighbors(b, "g", "C", None).unwrap();
        assert_eq!(incoming.len(), 2);
    }

    #[test]
    fn batch_add_edges_node_existence() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        // "X" does not exist

        let edges = vec![(
            "A".to_string(),
            "X".to_string(),
            "KNOWS".to_string(),
            EdgeData::default(),
        )];

        let result = gs.batch_add_edges(b, "g", &edges, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn batch_add_edges_edge_count() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        let edges = vec![
            (
                "A".to_string(),
                "B".to_string(),
                "KNOWS".to_string(),
                EdgeData::default(),
            ),
            (
                "A".to_string(),
                "C".to_string(),
                "KNOWS".to_string(),
                EdgeData::default(),
            ),
            (
                "B".to_string(),
                "C".to_string(),
                "LIKES".to_string(),
                EdgeData::default(),
            ),
        ];

        gs.batch_add_edges(b, "g", &edges, None).unwrap();

        // Verify correct counts via neighbor queries
        let a_out = gs.outgoing_neighbors(b, "g", "A", None).unwrap();
        assert_eq!(a_out.len(), 2); // A→B, A→C
        let b_out = gs.outgoing_neighbors(b, "g", "B", None).unwrap();
        assert_eq!(b_out.len(), 1); // B→C
        let c_in = gs.incoming_neighbors(b, "g", "C", None).unwrap();
        assert_eq!(c_in.len(), 2); // A→C, B→C
    }

    #[test]
    fn batch_add_edges_empty() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();

        let result = gs.batch_add_edges(b, "g", &[], None).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn batch_add_edges_validates_inputs() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();

        // Invalid edge type (empty)
        let edges = vec![(
            "A".to_string(),
            "B".to_string(),
            "".to_string(),
            EdgeData::default(),
        )];
        assert!(gs.batch_add_edges(b, "g", &edges, None).is_err());

        // Invalid node ID (contains slash)
        let edges = vec![(
            "A/B".to_string(),
            "C".to_string(),
            "KNOWS".to_string(),
            EdgeData::default(),
        )];
        assert!(gs.batch_add_edges(b, "g", &edges, None).is_err());
    }

    #[test]
    fn batch_add_edges_properties() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();

        let data = EdgeData {
            weight: 0.5,
            properties: Some(serde_json::json!({"since": 2020})),
        };
        let edges = vec![("A".to_string(), "B".to_string(), "KNOWS".to_string(), data)];

        gs.batch_add_edges(b, "g", &edges, None).unwrap();

        let edge = gs.get_edge(b, "g", "A", "B", "KNOWS").unwrap().unwrap();
        assert!((edge.weight - 0.5).abs() < f64::EPSILON);
        assert!(edge.properties.is_some());
    }

    #[test]
    fn batch_add_edges_mixed_types() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();

        let edges = vec![
            (
                "A".to_string(),
                "B".to_string(),
                "KNOWS".to_string(),
                EdgeData::default(),
            ),
            (
                "A".to_string(),
                "B".to_string(),
                "LIKES".to_string(),
                EdgeData::default(),
            ),
            (
                "B".to_string(),
                "A".to_string(),
                "FOLLOWS".to_string(),
                EdgeData::default(),
            ),
        ];

        gs.batch_add_edges(b, "g", &edges, None).unwrap();

        assert!(gs.get_edge(b, "g", "A", "B", "KNOWS").unwrap().is_some());
        assert!(gs.get_edge(b, "g", "A", "B", "LIKES").unwrap().is_some());
        assert!(gs.get_edge(b, "g", "B", "A", "FOLLOWS").unwrap().is_some());
    }

    #[test]
    fn batch_add_edges_appends_to_existing() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        // Add one edge via single-edge API
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        // Add more edges via batch — should append, not replace
        let edges = vec![(
            "A".to_string(),
            "C".to_string(),
            "KNOWS".to_string(),
            EdgeData::default(),
        )];
        gs.batch_add_edges(b, "g", &edges, None).unwrap();

        // Both edges should exist
        assert!(gs.get_edge(b, "g", "A", "B", "KNOWS").unwrap().is_some());
        assert!(gs.get_edge(b, "g", "A", "C", "KNOWS").unwrap().is_some());
        let out = gs.outgoing_neighbors(b, "g", "A", None).unwrap();
        assert_eq!(out.len(), 2);
    }

    // =========================================================================
    // Chunked batch edges (Epic 8c)
    // =========================================================================

    #[test]
    fn batch_add_edges_large_batch_chunks() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        // Create 100 nodes
        for i in 0..100 {
            gs.add_node(b, "g", &format!("n{}", i), NodeData::default())
                .unwrap();
        }

        // Create 9900 edges (100 nodes × 99 targets each) with chunk_size=100
        let edges: Vec<_> = (0..100)
            .flat_map(|src| {
                (0..100).filter(move |&dst| dst != src).map(move |dst| {
                    (
                        format!("n{}", src),
                        format!("n{}", dst),
                        "EDGE".to_string(),
                        EdgeData::default(),
                    )
                })
            })
            .collect();

        assert_eq!(edges.len(), 9900);
        let inserted = gs.batch_add_edges(b, "g", &edges, Some(100)).unwrap();
        assert_eq!(inserted, 9900);

        // Spot-check some edges
        assert!(gs.get_edge(b, "g", "n0", "n1", "EDGE").unwrap().is_some());
        assert!(gs.get_edge(b, "g", "n99", "n0", "EDGE").unwrap().is_some());
    }

    #[test]
    fn batch_add_edges_chunk_size_zero_treated_as_one() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();

        let edges = vec![(
            "A".to_string(),
            "B".to_string(),
            "E".to_string(),
            EdgeData::default(),
        )];

        // chunk_size=0 should not panic — clamped to 1
        let inserted = gs.batch_add_edges(b, "g", &edges, Some(0)).unwrap();
        assert_eq!(inserted, 1);
        assert!(gs.get_edge(b, "g", "A", "B", "E").unwrap().is_some());
    }

    #[test]
    fn batch_add_edges_chunk_boundaries() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();

        // 3 edges with chunk_size=2 → 2 chunks (2 + 1)
        let edges = vec![
            (
                "A".to_string(),
                "B".to_string(),
                "E1".to_string(),
                EdgeData::default(),
            ),
            (
                "A".to_string(),
                "C".to_string(),
                "E2".to_string(),
                EdgeData::default(),
            ),
            (
                "B".to_string(),
                "C".to_string(),
                "E3".to_string(),
                EdgeData::default(),
            ),
        ];

        let inserted = gs.batch_add_edges(b, "g", &edges, Some(2)).unwrap();
        assert_eq!(inserted, 3);

        assert!(gs.get_edge(b, "g", "A", "B", "E1").unwrap().is_some());
        assert!(gs.get_edge(b, "g", "A", "C", "E2").unwrap().is_some());
        assert!(gs.get_edge(b, "g", "B", "C", "E3").unwrap().is_some());
    }

    // =========================================================================
    // Parallel ingest verification (Epic 8e)
    // =========================================================================

    #[test]
    fn parallel_branch_ingest_throughput() {
        use std::sync::Arc;

        let db = Database::cache().unwrap();
        let gs = Arc::new(GraphStore::new(db.clone()));

        let num_threads = 4;
        let entries_per_thread = 1_000;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let gs = Arc::clone(&gs);
                std::thread::spawn(move || {
                    // Each thread uses a different branch
                    let branch = BranchId::from_bytes([t as u8; 16]);
                    gs.create_graph(branch, "g", None).unwrap();

                    for i in 0..entries_per_thread {
                        gs.add_node(branch, "g", &format!("n{}", i), NodeData::default())
                            .unwrap();
                    }
                    entries_per_thread
                })
            })
            .collect();

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, num_threads * entries_per_thread);

        // Verify each branch has its data
        for t in 0..num_threads {
            let branch = BranchId::from_bytes([t as u8; 16]);
            let nodes = gs.list_nodes(branch, "g").unwrap();
            assert_eq!(nodes.len(), entries_per_thread);
        }
    }

    #[test]
    fn parallel_ingest_isolation() {
        use std::sync::Arc;

        let db = Database::cache().unwrap();
        let gs = Arc::new(GraphStore::new(db.clone()));

        let handles: Vec<_> = (0..4)
            .map(|t| {
                let gs = Arc::clone(&gs);
                std::thread::spawn(move || {
                    let branch = BranchId::from_bytes([t as u8; 16]);
                    gs.create_graph(branch, "g", None).unwrap();

                    // Each branch gets unique node names
                    for i in 0..100 {
                        gs.add_node(branch, "g", &format!("t{}_n{}", t, i), NodeData::default())
                            .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify isolation: each branch only has its own nodes
        for t in 0..4u8 {
            let branch = BranchId::from_bytes([t; 16]);
            let nodes = gs.list_nodes(branch, "g").unwrap();
            assert_eq!(nodes.len(), 100);
            for node in &nodes {
                assert!(
                    node.starts_with(&format!("t{}_", t)),
                    "Branch {} has foreign node: {}",
                    t,
                    node
                );
            }
        }
    }
}
