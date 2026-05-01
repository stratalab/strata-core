//! Extension trait for graph operations on TransactionContext.
//!
//! Follows the same pattern as `KVStoreExt`, `EventLogExt`, `JsonStoreExt`,
//! and `VectorStoreExt`. By implementing graph operations directly on
//! `TransactionContext`, both the standalone `GraphStore` methods and the
//! Session's `dispatch_in_txn` can share one implementation.
//!
//! Graph index updates (BM25 search) cannot participate in OCC (they're
//! in-memory and not rollback-safe), so write methods queue `StagedGraphOp`s
//! in the `GraphBackendState` for the `GraphCommitObserver` to apply after
//! successful commit.

use std::collections::HashMap;

use strata_core::BranchId;
use strata_core::Value;
use strata_engine::{StrataError, StrataResult};
use strata_storage::{Key, TransactionContext};

use crate::keys;
use crate::packed;
use crate::store::{GraphBackendState, StagedGraphOp};
use crate::types::*;

// ============================================================================
// Helpers (module-level, not associated with a struct)
// ============================================================================

/// Read an edge type count from a transaction.
pub(crate) fn read_edge_type_count(
    txn: &mut TransactionContext,
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
pub(crate) fn write_edge_type_count(
    txn: &mut TransactionContext,
    count_sk: &Key,
    count: u64,
) -> StrataResult<()> {
    Ok(txn.put(count_sk.clone(), Value::String(count.to_string()))?)
}

// ============================================================================
// GraphStoreExt trait
// ============================================================================

/// Extension trait providing graph operations on `TransactionContext`.
///
/// This enables graph mutations and reads to participate in user transactions
/// (OCC). The session's `dispatch_in_txn` calls these methods on the active
/// `TransactionContext`, and the standalone `GraphStore` methods wrap them
/// in `db.transaction()`.
pub trait GraphStoreExt {
    // --- Lifecycle ---

    /// Create a graph with the given name and metadata.
    fn graph_create(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        meta: GraphMeta,
    ) -> StrataResult<()>;

    /// Get graph metadata, or None if graph doesn't exist.
    fn graph_get_meta(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<Option<GraphMeta>>;

    /// List all graph names on a branch within the given space (from catalog).
    fn graph_list(&mut self, branch_id: BranchId, space: &str) -> StrataResult<Vec<String>>;

    // --- Nodes ---

    /// Add or update a node. Returns true if created, false if updated.
    ///
    /// Queues a `StagedGraphOp::IndexNode` in the backend state for the
    /// `GraphCommitObserver` to apply after successful commit.
    fn graph_add_node(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        data: &NodeData,
        backend_state: &GraphBackendState,
    ) -> StrataResult<bool>;

    /// Get node data, or None if not found.
    fn graph_get_node(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
    ) -> StrataResult<Option<NodeData>>;

    /// Remove a node and all its incident edges.
    ///
    /// Queues a `StagedGraphOp::DeindexNode` in the backend state for the
    /// `GraphCommitObserver` to apply after successful commit.
    fn graph_remove_node(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        backend_state: &GraphBackendState,
    ) -> StrataResult<()>;

    /// List all node IDs in a graph.
    fn graph_list_nodes(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<Vec<String>>;

    // --- Edges ---

    /// Add or update an edge. Returns true if created, false if updated.
    #[allow(clippy::too_many_arguments)]
    fn graph_add_edge(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
        data: &EdgeData,
    ) -> StrataResult<bool>;

    /// Remove an edge from both adjacency lists.
    fn graph_remove_edge(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<()>;

    /// Get edge data, or None if not found.
    fn graph_get_edge(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<Option<EdgeData>>;

    /// Get outgoing neighbors, optionally filtered by edge type.
    fn graph_outgoing_neighbors(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> StrataResult<Vec<Neighbor>>;

    /// Get incoming neighbors, optionally filtered by edge type.
    fn graph_incoming_neighbors(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> StrataResult<Vec<Neighbor>>;
}

// ============================================================================
// Implementation for TransactionContext
// ============================================================================

impl GraphStoreExt for TransactionContext {
    // --- Lifecycle ---

    fn graph_create(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        meta: GraphMeta,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        strata_engine::primitives::space::ensure_space_registered_in_txn(self, &branch_id, space)?;
        let meta_json =
            serde_json::to_string(&meta).map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::meta_key(graph);
        let storage_key = keys::storage_key(branch_id, space, &user_key);
        let catalog_sk = keys::storage_key(branch_id, space, keys::graph_catalog_key());

        self.put(storage_key, Value::String(meta_json))?;

        // Update catalog: read JSON array, append name, write back
        let mut catalog: Vec<String> = match self.get(&catalog_sk)? {
            Some(Value::String(s)) => serde_json::from_str(&s).unwrap_or_default(),
            _ => Vec::new(),
        };
        if !catalog.contains(&graph.to_string()) {
            catalog.push(graph.to_string());
            let catalog_json = serde_json::to_string(&catalog)
                .map_err(|e| StrataError::serialization(e.to_string()))?;
            self.put(catalog_sk, Value::String(catalog_json))?;
        }
        Ok(())
    }

    fn graph_get_meta(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<Option<GraphMeta>> {
        let user_key = keys::meta_key(graph);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

        match self.get(&storage_key)? {
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
    }

    fn graph_list(&mut self, branch_id: BranchId, space: &str) -> StrataResult<Vec<String>> {
        let catalog_sk = keys::storage_key(branch_id, space, keys::graph_catalog_key());

        // Try catalog first (fast path)
        if let Some(Value::String(s)) = self.get(&catalog_sk)? {
            let catalog: Vec<String> =
                serde_json::from_str(&s).map_err(|e| StrataError::serialization(e.to_string()))?;
            return Ok(catalog);
        }

        // Fallback: scan for __meta__ keys (legacy data without catalog)
        let ns = keys::graph_namespace(branch_id, space);
        let prefix_key = Key::new_graph(ns, "");

        let results = self.scan_prefix(&prefix_key)?;
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

        // Lazily create the catalog for next time
        if !graphs.is_empty() {
            let catalog_json = serde_json::to_string(&graphs)
                .map_err(|e| StrataError::serialization(e.to_string()))?;
            self.put(catalog_sk, Value::String(catalog_json))?;
        }

        Ok(graphs)
    }

    // --- Nodes ---

    fn graph_add_node(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        data: &NodeData,
        backend_state: &GraphBackendState,
    ) -> StrataResult<bool> {
        keys::validate_graph_name(graph)?;
        keys::validate_node_id(node_id)?;
        strata_engine::primitives::space::ensure_space_registered_in_txn(self, &branch_id, space)?;
        let node_json =
            serde_json::to_string(data).map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::node_key(graph, node_id);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

        // Build ref index key if entity_ref is present
        let ref_key = data.entity_ref.as_ref().map(|uri| {
            let rk = keys::ref_index_key(uri, graph, node_id);
            keys::storage_key(branch_id, space, &rk)
        });

        // Build type index key if object_type is present
        let type_key = data.object_type.as_ref().map(|ot| {
            let tk = keys::type_index_key(graph, ot, node_id);
            keys::storage_key(branch_id, space, &tk)
        });

        // If updating, clean up old ref index and type index entries
        let old_val = self.get(&storage_key)?;
        let created = old_val.is_none();
        if let Some(Value::String(old_json)) = old_val {
            if let Ok(old_data) = serde_json::from_str::<NodeData>(&old_json) {
                if let Some(old_uri) = old_data.entity_ref {
                    let old_rk = keys::ref_index_key(&old_uri, graph, node_id);
                    let old_sk = keys::storage_key(branch_id, space, &old_rk);
                    self.delete(old_sk)?;
                }
                if let Some(old_ot) = old_data.object_type {
                    let old_tk = keys::type_index_key(graph, &old_ot, node_id);
                    let old_sk = keys::storage_key(branch_id, space, &old_tk);
                    self.delete(old_sk)?;
                }
            }
        }

        self.put(storage_key, Value::String(node_json))?;

        // Write ref index
        if let Some(rk) = ref_key {
            self.put(rk, Value::Null)?;
        }

        // Write type index
        if let Some(tk) = type_key {
            self.put(tk, Value::Null)?;
        }

        // Queue for GraphCommitObserver (subsystem-owned maintenance).
        // T2-E5: derived index work is staged in subsystem state, not Session.
        backend_state.queue_pending_op(
            self.txn_id,
            StagedGraphOp::IndexNode {
                branch_id,
                space: space.to_string(),
                graph: graph.to_string(),
                node_id: node_id.to_string(),
                data: data.clone(),
            },
        );

        Ok(created)
    }

    fn graph_get_node(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
    ) -> StrataResult<Option<NodeData>> {
        let user_key = keys::node_key(graph, node_id);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

        match self.get(&storage_key)? {
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
    }

    fn graph_remove_node(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        backend_state: &GraphBackendState,
    ) -> StrataResult<()> {
        let node_user_key = keys::node_key(graph, node_id);
        let node_storage_key = keys::storage_key(branch_id, space, &node_user_key);
        let fwd_adj_uk = keys::forward_adj_key(graph, node_id);
        let fwd_adj_sk = keys::storage_key(branch_id, space, &fwd_adj_uk);
        let rev_adj_uk = keys::reverse_adj_key(graph, node_id);
        let rev_adj_sk = keys::storage_key(branch_id, space, &rev_adj_uk);

        // Read node to get entity_ref for ref index cleanup
        let node_val = self.get(&node_storage_key)?;
        if node_val.is_none() {
            return Ok(());
        }

        // Clean up ref index and type index
        if let Some(Value::String(json)) = &node_val {
            if let Ok(data) = serde_json::from_str::<NodeData>(json) {
                if let Some(uri) = data.entity_ref {
                    let rk = keys::ref_index_key(&uri, graph, node_id);
                    let sk = keys::storage_key(branch_id, space, &rk);
                    self.delete(sk)?;
                }
                if let Some(ot) = data.object_type {
                    let tk = keys::type_index_key(graph, &ot, node_id);
                    let sk = keys::storage_key(branch_id, space, &tk);
                    self.delete(sk)?;
                }
            }
        }

        // Track edge type counts to decrement
        let mut edge_type_decrements: HashMap<String, u64> = HashMap::new();

        // Remove this node from each neighbor's reverse adjacency list
        if let Some(Value::Bytes(fwd_bytes)) = self.get(&fwd_adj_sk)? {
            let outgoing = packed::decode(&fwd_bytes)?;
            for (dst, edge_type, _) in &outgoing {
                *edge_type_decrements.entry(edge_type.clone()).or_insert(0) += 1;
                let dst_rev_uk = keys::reverse_adj_key(graph, dst);
                let dst_rev_sk = keys::storage_key(branch_id, space, &dst_rev_uk);
                if let Some(Value::Bytes(dst_rev_bytes)) = self.get(&dst_rev_sk)? {
                    if let Some(new_bytes) = packed::remove_edge(&dst_rev_bytes, node_id, edge_type)
                    {
                        if packed::edge_count(&new_bytes) == 0 {
                            self.delete(dst_rev_sk)?;
                        } else {
                            self.put_replace(dst_rev_sk, Value::Bytes(new_bytes))?;
                        }
                    }
                }
            }
        }

        // Remove this node from each neighbor's forward adjacency list
        // Only count edges from OTHER nodes (self-loops already counted above)
        if let Some(Value::Bytes(rev_bytes)) = self.get(&rev_adj_sk)? {
            let incoming = packed::decode(&rev_bytes)?;
            for (src, edge_type, _) in &incoming {
                if src != node_id {
                    *edge_type_decrements.entry(edge_type.clone()).or_insert(0) += 1;
                }
                let src_fwd_uk = keys::forward_adj_key(graph, src);
                let src_fwd_sk = keys::storage_key(branch_id, space, &src_fwd_uk);
                if let Some(Value::Bytes(src_fwd_bytes)) = self.get(&src_fwd_sk)? {
                    if let Some(new_bytes) = packed::remove_edge(&src_fwd_bytes, node_id, edge_type)
                    {
                        if packed::edge_count(&new_bytes) == 0 {
                            self.delete(src_fwd_sk)?;
                        } else {
                            self.put_replace(src_fwd_sk, Value::Bytes(new_bytes))?;
                        }
                    }
                }
            }
        }

        // Decrement edge type counters
        for (et, dec) in &edge_type_decrements {
            let count_uk = keys::edge_type_count_key(graph, et);
            let count_sk = keys::storage_key(branch_id, space, &count_uk);
            let count = read_edge_type_count(self, &count_sk)?;
            write_edge_type_count(self, &count_sk, count.saturating_sub(*dec))?;
        }

        // Delete the node's own adjacency lists and the node itself
        self.delete(fwd_adj_sk)?;
        self.delete(rev_adj_sk)?;
        self.delete(node_storage_key)?;

        // Queue for GraphCommitObserver (subsystem-owned maintenance).
        // T2-E5: derived index work is staged in subsystem state, not Session.
        backend_state.queue_pending_op(
            self.txn_id,
            StagedGraphOp::DeindexNode {
                branch_id,
                space: space.to_string(),
                graph: graph.to_string(),
                node_id: node_id.to_string(),
            },
        );

        Ok(())
    }

    fn graph_list_nodes(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<Vec<String>> {
        let prefix = keys::all_nodes_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, space, &prefix);

        let results = self.scan_prefix(&prefix_key)?;
        let mut nodes = Vec::new();
        for (key, _) in results {
            if let Some(user_key) = key.user_key_string() {
                if let Some(id) = keys::parse_node_key(graph, &user_key) {
                    nodes.push(id);
                }
            }
        }
        Ok(nodes)
    }

    // --- Edges ---

    fn graph_add_edge(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
        data: &EdgeData,
    ) -> StrataResult<bool> {
        keys::validate_graph_name(graph)?;
        keys::validate_node_id(src)?;
        keys::validate_node_id(dst)?;
        keys::validate_edge_type(edge_type)?;
        strata_engine::primitives::space::ensure_space_registered_in_txn(self, &branch_id, space)?;
        let src_node_uk = keys::node_key(graph, src);
        let dst_node_uk = keys::node_key(graph, dst);
        let src_node_key = keys::storage_key(branch_id, space, &src_node_uk);
        let dst_node_key = keys::storage_key(branch_id, space, &dst_node_uk);

        let fwd_uk = keys::forward_adj_key(graph, src);
        let fwd_sk = keys::storage_key(branch_id, space, &fwd_uk);
        let rev_uk = keys::reverse_adj_key(graph, dst);
        let rev_sk = keys::storage_key(branch_id, space, &rev_uk);

        let count_uk = keys::edge_type_count_key(graph, edge_type);
        let count_sk = keys::storage_key(branch_id, space, &count_uk);

        // Validate both nodes exist (prevents TOCTOU race)
        if self.get(&src_node_key)?.is_none() {
            return Err(StrataError::invalid_input(format!(
                "Source node '{}' does not exist in graph '{}'",
                src, graph
            )));
        }
        if self.get(&dst_node_key)?.is_none() {
            return Err(StrataError::invalid_input(format!(
                "Target node '{}' does not exist in graph '{}'",
                dst, graph
            )));
        }

        // Read-modify-write forward adjacency list (src → dst)
        let mut fwd_buf = match self.get(&fwd_sk)? {
            Some(Value::Bytes(b)) => b,
            _ => packed::empty(),
        };
        let was_update = if let Some(new_buf) = packed::remove_edge(&fwd_buf, dst, edge_type) {
            fwd_buf = new_buf;
            true
        } else {
            false
        };
        packed::append_edge(&mut fwd_buf, dst, edge_type, data)?;
        self.put_replace(fwd_sk, Value::Bytes(fwd_buf))?;

        // Read-modify-write reverse adjacency list (dst ← src)
        let mut rev_buf = match self.get(&rev_sk)? {
            Some(Value::Bytes(b)) => b,
            _ => packed::empty(),
        };
        if let Some(new_buf) = packed::remove_edge(&rev_buf, src, edge_type) {
            rev_buf = new_buf;
        }
        packed::append_edge(&mut rev_buf, src, edge_type, data)?;
        self.put_replace(rev_sk, Value::Bytes(rev_buf))?;

        // Increment edge type counter only for new edges (not updates)
        if !was_update {
            let count = read_edge_type_count(self, &count_sk)?;
            write_edge_type_count(self, &count_sk, count + 1)?;
        }

        Ok(!was_update)
    }

    fn graph_remove_edge(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<()> {
        let fwd_uk = keys::forward_adj_key(graph, src);
        let fwd_sk = keys::storage_key(branch_id, space, &fwd_uk);
        let rev_uk = keys::reverse_adj_key(graph, dst);
        let rev_sk = keys::storage_key(branch_id, space, &rev_uk);
        let count_uk = keys::edge_type_count_key(graph, edge_type);
        let count_sk = keys::storage_key(branch_id, space, &count_uk);

        // Remove from forward adjacency list
        let mut removed = false;
        if let Some(Value::Bytes(fwd_bytes)) = self.get(&fwd_sk)? {
            if let Some(new_bytes) = packed::remove_edge(&fwd_bytes, dst, edge_type) {
                removed = true;
                if packed::edge_count(&new_bytes) == 0 {
                    self.delete(fwd_sk)?;
                } else {
                    self.put_replace(fwd_sk, Value::Bytes(new_bytes))?;
                }
            }
        }

        // Remove from reverse adjacency list
        if let Some(Value::Bytes(rev_bytes)) = self.get(&rev_sk)? {
            if let Some(new_bytes) = packed::remove_edge(&rev_bytes, src, edge_type) {
                if packed::edge_count(&new_bytes) == 0 {
                    self.delete(rev_sk)?;
                } else {
                    self.put_replace(rev_sk, Value::Bytes(new_bytes))?;
                }
            }
        }

        // Decrement edge type counter
        if removed {
            let count = read_edge_type_count(self, &count_sk)?;
            if count > 0 {
                write_edge_type_count(self, &count_sk, count - 1)?;
            }
        }

        Ok(())
    }

    fn graph_get_edge(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<Option<EdgeData>> {
        let fwd_uk = keys::forward_adj_key(graph, src);
        let fwd_sk = keys::storage_key(branch_id, space, &fwd_uk);

        match self.get(&fwd_sk)? {
            Some(Value::Bytes(bytes)) => Ok(packed::find_edge(&bytes, dst, edge_type)),
            _ => Ok(None),
        }
    }

    fn graph_outgoing_neighbors(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> StrataResult<Vec<Neighbor>> {
        let fwd_uk = keys::forward_adj_key(graph, node_id);
        let fwd_sk = keys::storage_key(branch_id, space, &fwd_uk);

        match self.get(&fwd_sk)? {
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
        }
    }

    fn graph_incoming_neighbors(
        &mut self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> StrataResult<Vec<Neighbor>> {
        let rev_uk = keys::reverse_adj_key(graph, node_id);
        let rev_sk = keys::storage_key(branch_id, space, &rev_uk);

        match self.get(&rev_sk)? {
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
        }
    }
}
