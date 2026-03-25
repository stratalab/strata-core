//! Graph snapshot tests and algorithm execution.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use strata_core::types::BranchId;

    use crate::types::*;
    use crate::GraphStore;
    use strata_engine::Database;

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::cache().unwrap();
        let gs = GraphStore::new(db.clone());
        (db, gs)
    }

    fn branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    // =========================================================================
    // Snapshot
    // =========================================================================

    #[test]
    fn empty_graph_empty_snapshot() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        let snap = gs.snapshot(b, "g").unwrap();
        assert_eq!(snap.node_count(), 0);
        assert_eq!(snap.edge_count(), 0);
    }

    #[test]
    fn snapshot_captures_all_nodes_and_edges() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B", "C"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        gs.add_edge(b, "g", "A", "B", "E1", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E2", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E3", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "E4", EdgeData::default())
            .unwrap();

        let snap = gs.snapshot(b, "g").unwrap();
        assert_eq!(snap.node_count(), 3);
        assert_eq!(snap.edge_count(), 4);
    }

    #[test]
    fn snapshot_after_remove_node() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.remove_node(b, "g", "A").unwrap();

        let snap = gs.snapshot(b, "g").unwrap();
        assert_eq!(snap.node_count(), 1);
        assert_eq!(snap.edge_count(), 0);
        assert!(!snap.nodes.contains_key("A"));
    }

    #[test]
    fn snapshot_nodes_without_edges() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B", "C", "D", "E"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        let snap = gs.snapshot(b, "g").unwrap();
        assert_eq!(snap.node_count(), 5);
        assert_eq!(snap.edge_count(), 0);
    }

    // =========================================================================
    // Export formats
    // =========================================================================

    #[test]
    fn to_edge_list() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();
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
        gs.add_edge(b, "g", "B", "C", "LINKS", EdgeData::default())
            .unwrap();

        let snap = gs.snapshot(b, "g").unwrap();
        let list = snap.to_edge_list();
        assert_eq!(list.len(), 2);
        // Check that entries contain expected data
        let has_knows = list
            .iter()
            .any(|(s, d, t, w)| s == "A" && d == "B" && t == "KNOWS" && (*w - 0.5).abs() < 1e-10);
        assert!(has_knows);
    }

    #[test]
    fn to_adjacency_list() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_node(b, "g", "C", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();

        let snap = gs.snapshot(b, "g").unwrap();
        let adj = snap.to_adjacency_list();
        assert_eq!(adj.get("A").unwrap().len(), 2);
        assert_eq!(adj.get("B").unwrap().len(), 1);
        assert!(!adj.contains_key("C")); // C has no outgoing
    }

    #[test]
    fn to_csv_parseable() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();

        let snap = gs.snapshot(b, "g").unwrap();
        let csv = snap.to_csv();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[0], "src,dst,edge_type,weight");
        assert_eq!(lines.len(), 2); // header + 1 edge
        assert!(lines[1].contains("A"));
        assert!(lines[1].contains("B"));
        assert!(lines[1].contains("KNOWS"));
    }

    #[test]
    fn to_csv_default_weight_is_1() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "X", NodeData::default()).unwrap();
        gs.add_node(b, "g", "Y", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "X", "Y", "E", EdgeData::default())
            .unwrap();

        let snap = gs.snapshot(b, "g").unwrap();
        let csv = snap.to_csv();
        assert!(csv.contains("1")); // weight=1.0
    }

    // =========================================================================
    // GraphAlgorithm trait
    // =========================================================================

    /// A trivial algorithm that counts the degree of each node.
    struct DegreeCount;

    impl GraphAlgorithm for DegreeCount {
        type Output = HashMap<String, usize>;

        fn execute(&self, snapshot: &GraphSnapshot) -> Self::Output {
            let mut degrees: HashMap<String, usize> = HashMap::new();
            for node_id in snapshot.nodes.keys() {
                degrees.insert(node_id.clone(), 0);
            }
            for edge in &snapshot.edges {
                *degrees.entry(edge.src.clone()).or_insert(0) += 1;
                *degrees.entry(edge.dst.clone()).or_insert(0) += 1;
            }
            degrees
        }
    }

    #[test]
    fn graph_algorithm_degree_count() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B", "C"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();

        let snap = gs.snapshot(b, "g").unwrap();
        let degrees = DegreeCount.execute(&snap);

        assert_eq!(degrees["A"], 1); // A→B
        assert_eq!(degrees["B"], 2); // A→B + B→C
        assert_eq!(degrees["C"], 1); // B→C
    }

    // =========================================================================
    // snapshot_stats (G-12)
    // =========================================================================

    #[test]
    fn snapshot_stats_empty_graph() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        let stats = gs.snapshot_stats(b, "g").unwrap();
        assert_eq!(stats.node_count, 0);
        assert_eq!(stats.edge_count, 0);
    }

    #[test]
    fn snapshot_stats_matches_snapshot() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B", "C"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        gs.add_edge(b, "g", "A", "B", "E1", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E2", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E3", EdgeData::default())
            .unwrap();

        let stats = gs.snapshot_stats(b, "g").unwrap();
        let snap = gs.snapshot(b, "g").unwrap();

        assert_eq!(stats.node_count, snap.node_count());
        assert_eq!(stats.edge_count, snap.edge_count());
    }

    #[test]
    fn snapshot_stats_after_remove() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.remove_node(b, "g", "A").unwrap();

        let stats = gs.snapshot_stats(b, "g").unwrap();
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.edge_count, 0);
    }

    // =========================================================================
    // for_each_edge (G-12)
    // =========================================================================

    #[test]
    fn for_each_edge_empty_graph() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        let mut count = 0;
        gs.for_each_edge(b, "g", |_| count += 1).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn for_each_edge_nodes_only_no_edges() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B", "C"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        let mut count = 0;
        gs.for_each_edge(b, "g", |_| count += 1).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn for_each_edge_matches_all_edges() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        for id in &["A", "B", "C"] {
            gs.add_node(b, "g", id, NodeData::default()).unwrap();
        }
        gs.add_edge(b, "g", "A", "B", "E1", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E2", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E3", EdgeData::default())
            .unwrap();

        let mut edges = Vec::new();
        gs.for_each_edge(b, "g", |e| edges.push(e)).unwrap();
        assert_eq!(edges.len(), 3);

        let all_edges = gs.all_edges(b, "g").unwrap();
        assert_eq!(edges.len(), all_edges.len());
    }

    #[test]
    fn for_each_edge_preserves_edge_data() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        gs.add_node(b, "g", "A", NodeData::default()).unwrap();
        gs.add_node(b, "g", "B", NodeData::default()).unwrap();
        gs.add_edge(
            b,
            "g",
            "A",
            "B",
            "SCORED",
            EdgeData {
                weight: 0.42,
                properties: Some(serde_json::json!({"source": "test"})),
            },
        )
        .unwrap();

        let mut edges = Vec::new();
        gs.for_each_edge(b, "g", |e| edges.push(e)).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].src, "A");
        assert_eq!(edges[0].dst, "B");
        assert!((edges[0].data.weight - 0.42).abs() < f64::EPSILON);
    }
}
