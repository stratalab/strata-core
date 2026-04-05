//! Node CRUD operations: add, get, list, remove, index queries.

use super::*;
use crate::ext::GraphStoreExt;

impl GraphStore {
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

        let result = self.db.transaction(branch_id, |txn| {
            txn.graph_add_node(branch_id, graph, node_id, &data)
        })?;

        // Post-commit: update search index
        self.index_node_for_search(branch_id, graph, node_id, &data);

        Ok(result)
    }

    /// Get node data, or None if node doesn't exist.
    pub fn get_node(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
    ) -> StrataResult<Option<NodeData>> {
        self.db.transaction(branch_id, |txn| {
            txn.graph_get_node(branch_id, graph, node_id)
        })
    }

    /// List all node IDs in a graph.
    pub fn list_nodes(&self, branch_id: BranchId, graph: &str) -> StrataResult<Vec<String>> {
        self.db
            .transaction(branch_id, |txn| txn.graph_list_nodes(branch_id, graph))
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

    /// Remove a node and all its incident edges.
    pub fn remove_node(&self, branch_id: BranchId, graph: &str, node_id: &str) -> StrataResult<()> {
        self.db.transaction(branch_id, |txn| {
            txn.graph_remove_node(branch_id, graph, node_id)
        })?;

        // Post-commit: remove from search index
        self.deindex_node_for_search(branch_id, graph, node_id);

        Ok(())
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

    // =========================================================================
    // Ref index tests
    // =========================================================================

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
    // G-1: Deserialization error propagation (node)
    // =========================================================================

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
    // Edge type counter tests (remove_node)
    // =========================================================================

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

    // =========================================================================
    // G-18: Node pagination tests
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
}
