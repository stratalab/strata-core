//! Graph lifecycle operations: create, delete, list, metadata.

use super::*;

impl GraphStore {
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
        let prefix_key = strata_core::types::Key::new_graph(ns, "");

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
            let mut deleted_node_ids: Vec<String> = Vec::new();
            let done = self.db.transaction(branch_id, |txn| {
                let results = txn.scan_prefix(&node_prefix_key)?;
                if results.is_empty() {
                    return Ok(true);
                }

                let batch: Vec<_> = results.into_iter().take(Self::DELETE_BATCH_SIZE).collect();
                let is_last = batch.len() < Self::DELETE_BATCH_SIZE;

                for (key, val) in &batch {
                    if let Some(user_key) = key.user_key_string() {
                        if let Some(node_id) = keys::parse_node_key(graph, &user_key) {
                            deleted_node_ids.push(node_id.clone());
                            if let Value::String(json) = val {
                                if let Ok(data) = serde_json::from_str::<NodeData>(json) {
                                    if let Some(uri) = data.entity_ref {
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

            // Post-commit: remove deleted nodes from search index
            for node_id in &deleted_node_ids {
                self.deindex_node_for_search(branch_id, graph, node_id);
            }

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
    // G-18: Graph pagination tests
    // =========================================================================

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
    // Scale tests (Issue #1983)
    // =========================================================================

    /// Delete a graph with 10K nodes and edges — exercises batched deletion.
    #[test]
    fn scale_delete_graph_10k_nodes() {
        let (_db, gs) = setup();
        let b = default_branch();

        gs.create_graph(b, "big", None).unwrap();

        // Bulk insert 10K nodes with entity_refs (exercises ref index cleanup)
        let nodes: Vec<(String, NodeData)> = (0..10_000)
            .map(|i| {
                (
                    format!("n{:05}", i),
                    NodeData {
                        entity_ref: Some(format!("kv://main/entity{}", i)),
                        properties: Some(serde_json::json!({"idx": i})),
                        ..Default::default()
                    },
                )
            })
            .collect();
        let edges: Vec<(String, String, String, EdgeData)> = (0..9_999)
            .map(|i| {
                (
                    format!("n{:05}", i),
                    format!("n{:05}", i + 1),
                    "NEXT".to_string(),
                    EdgeData::default(),
                )
            })
            .collect();

        gs.bulk_insert(b, "big", &nodes, &edges, Some(5_000))
            .unwrap();

        // Verify graph exists
        assert_eq!(gs.snapshot_stats(b, "big").unwrap().node_count, 10_000);

        // Delete — this triggers batched node deletion + ref index cleanup + prefix batched delete
        gs.delete_graph(b, "big").unwrap();

        // Verify everything is gone
        assert!(gs.get_graph_meta(b, "big").unwrap().is_none());
        assert!(gs.list_nodes(b, "big").unwrap().is_empty());
        assert!(gs.list_graphs(b).unwrap().is_empty());

        // Verify ref index entries are cleaned up (spot check)
        assert!(gs
            .nodes_for_entity(b, "kv://main/entity0")
            .unwrap()
            .is_empty());
        assert!(gs
            .nodes_for_entity(b, "kv://main/entity5000")
            .unwrap()
            .is_empty());
        assert!(gs
            .nodes_for_entity(b, "kv://main/entity9999")
            .unwrap()
            .is_empty());
    }
}
