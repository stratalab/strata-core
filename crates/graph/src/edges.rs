//! Edge CRUD operations: add, get, remove, neighbors, traversal helpers, edge type counters.

use super::*;
use crate::ext::GraphStoreExt;

impl GraphStore {
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

        self.db.transaction(branch_id, |txn| {
            txn.graph_add_edge(branch_id, graph, src, dst, edge_type, &data)
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
        self.db.transaction(branch_id, |txn| {
            txn.graph_get_edge(branch_id, graph, src, dst, edge_type)
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
        self.db.transaction(branch_id, |txn| {
            txn.graph_remove_edge(branch_id, graph, src, dst, edge_type)
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
        self.db.transaction(branch_id, |txn| {
            txn.graph_outgoing_neighbors(branch_id, graph, node_id, edge_type_filter)
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
        self.db.transaction(branch_id, |txn| {
            txn.graph_incoming_neighbors(branch_id, graph, node_id, edge_type_filter)
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

    // =========================================================================
    // Edge type count helpers (G-3)
    // =========================================================================

    /// Read an edge type count from a transaction.
    pub(crate) fn read_edge_type_count(
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
    pub(crate) fn write_edge_type_count(
        txn: &mut strata_concurrency::TransactionContext,
        count_sk: &Key,
        count: u64,
    ) -> StrataResult<()> {
        txn.put(count_sk.clone(), Value::String(count.to_string()))
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

    // =========================================================================
    // G-1: Deserialization error propagation (edges)
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

    // =========================================================================
    // Concurrency tests (Issue #1983)
    // =========================================================================

    /// Concurrent add_edge to the same source node from multiple threads.
    ///
    /// Each thread adds edges from the same hub node to different targets.
    /// The packed adjacency list is RMW (read-modify-write), so concurrent
    /// writers will hit OCC conflicts. All edges should eventually succeed
    /// (via OCC retry at the GraphStore level or serialized execution).
    #[test]
    fn concurrent_add_edge_same_source() {
        use std::sync::Arc;

        let db = Database::cache().unwrap();
        let gs = Arc::new(GraphStore::new(db));
        let b = BranchId::from_bytes([0u8; 16]);

        gs.create_graph(b, "g", None).unwrap();

        // Create hub + 40 target nodes
        gs.add_node(b, "g", "hub", NodeData::default()).unwrap();
        for i in 0..40 {
            gs.add_node(b, "g", &format!("t{}", i), NodeData::default())
                .unwrap();
        }

        // 4 threads, each adding 10 edges from hub to different targets.
        // Since all threads write to hub's adjacency list, OCC conflicts are expected.
        // Retry with exponential backoff to handle contention.
        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let gs = Arc::clone(&gs);
                std::thread::spawn(move || {
                    let start = thread_id * 10;
                    for i in start..start + 10 {
                        let target = format!("t{}", i);
                        let edge_type = format!("E{}", i);
                        let mut attempts = 0;
                        loop {
                            match gs.add_edge(
                                b,
                                "g",
                                "hub",
                                &target,
                                &edge_type,
                                EdgeData::default(),
                            ) {
                                Ok(_) => break,
                                Err(_) if attempts < 20 => {
                                    attempts += 1;
                                    // Exponential backoff: 10µs, 20µs, 40µs, ...
                                    std::thread::sleep(std::time::Duration::from_micros(
                                        10 << attempts.min(10),
                                    ));
                                }
                                Err(e) => panic!(
                                    "Thread {} failed to add edge to {} after {} retries: {}",
                                    thread_id, target, attempts, e
                                ),
                            }
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // All 40 edges should exist
        let out = gs.outgoing_neighbors(b, "g", "hub", None).unwrap();
        assert_eq!(
            out.len(),
            40,
            "All 40 concurrent edges should be present, got {}",
            out.len()
        );
    }
}
