//! Graph traversal operations: neighbors, degree, BFS, subgraph extraction.

use std::collections::{HashMap, HashSet, VecDeque};

use strata_core::types::BranchId;
use strata_core::StrataResult;

use super::adjacency::AdjacencyIndex;
use super::types::*;
use super::GraphStore;

impl GraphStore {
    /// Get neighbors of a node in a given direction, optionally filtered by edge type.
    pub fn neighbors(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
        direction: Direction,
        edge_type_filter: Option<&str>,
    ) -> StrataResult<Vec<Neighbor>> {
        match direction {
            Direction::Outgoing => {
                self.outgoing_neighbors(branch_id, graph, node_id, edge_type_filter)
            }
            Direction::Incoming => {
                self.incoming_neighbors(branch_id, graph, node_id, edge_type_filter)
            }
            Direction::Both => {
                let mut out =
                    self.outgoing_neighbors(branch_id, graph, node_id, edge_type_filter)?;
                let incoming =
                    self.incoming_neighbors(branch_id, graph, node_id, edge_type_filter)?;
                // Include all edges from both directions.
                // Distinct mutual edges (A→B and B→A) both appear.
                // Self-loops (A→A) will appear twice (once from each direction).
                out.extend(incoming);
                Ok(out)
            }
        }
    }

    /// Get the degree of a node (number of edges in a given direction).
    ///
    /// Uses `packed::edge_count()` to read the 4-byte header directly,
    /// avoiding full adjacency list decode.
    pub fn degree(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
        direction: Direction,
    ) -> StrataResult<usize> {
        match direction {
            Direction::Outgoing => self.degree_packed(branch_id, graph, node_id, true),
            Direction::Incoming => self.degree_packed(branch_id, graph, node_id, false),
            Direction::Both => {
                let out = self.degree_packed(branch_id, graph, node_id, true)?;
                let inc = self.degree_packed(branch_id, graph, node_id, false)?;
                Ok(out + inc)
            }
        }
    }

    /// Read degree from packed adjacency list header (zero decode).
    fn degree_packed(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_id: &str,
        forward: bool,
    ) -> StrataResult<usize> {
        let user_key = if forward {
            super::keys::forward_adj_key(graph, node_id)
        } else {
            super::keys::reverse_adj_key(graph, node_id)
        };
        let storage_key = super::keys::storage_key(branch_id, &user_key);
        match self.db.get_value_direct(&storage_key) {
            Some(strata_core::Value::Bytes(bytes)) => {
                Ok(super::packed::edge_count(&bytes) as usize)
            }
            _ => Ok(0),
        }
    }

    /// Breadth-first search from a start node.
    ///
    /// Builds an in-memory adjacency index from all edges in a single bulk scan,
    /// then runs BFS against it. This avoids per-node KV transactions during traversal.
    pub fn bfs(
        &self,
        branch_id: BranchId,
        graph: &str,
        start: &str,
        opts: BfsOptions,
    ) -> StrataResult<BfsResult> {
        let index = self.build_adjacency_index(branch_id, graph)?;
        Ok(self.bfs_with_index(start, &opts, &index))
    }

    /// BFS using a pre-built adjacency index (no per-node KV lookups).
    ///
    /// Use this when you already have an `AdjacencyIndex` (e.g. for running
    /// multiple traversals on the same graph without rebuilding the index).
    pub fn bfs_with_index(
        &self,
        start: &str,
        opts: &BfsOptions,
        index: &AdjacencyIndex,
    ) -> BfsResult {
        let mut visited: Vec<String> = Vec::new();
        let mut depths: HashMap<String, usize> = HashMap::new();
        let mut edges: Vec<(String, String, String)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();

        queue.push_back((start.to_string(), 0));
        seen.insert(start.to_string());

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(max) = opts.max_nodes {
                if visited.len() >= max {
                    break;
                }
            }

            visited.push(current.clone());
            depths.insert(current.clone(), depth);

            if depth >= opts.max_depth {
                continue;
            }

            // Macro-like inline to process each (dst, edge_type) pair from the
            // zero-alloc iterators, avoiding closure borrow conflicts.
            macro_rules! process_neighbors {
                ($iter:expr) => {
                    for (neighbor_id, et) in $iter {
                        if let Some(ref filter) = opts.edge_types {
                            if !filter.iter().any(|f| f == et) {
                                continue;
                            }
                        }
                        if !seen.contains(neighbor_id) {
                            seen.insert(neighbor_id.to_string());
                            edges.push((current.clone(), neighbor_id.to_string(), et.to_string()));
                            queue.push_back((neighbor_id.to_string(), depth + 1));
                        }
                    }
                };
            }

            match opts.direction {
                Direction::Outgoing => {
                    process_neighbors!(index.outgoing_neighbor_ids(&current, None));
                }
                Direction::Incoming => {
                    process_neighbors!(index.incoming_neighbor_ids(&current, None));
                }
                Direction::Both => {
                    process_neighbors!(index.outgoing_neighbor_ids(&current, None));
                    process_neighbors!(index.incoming_neighbor_ids(&current, None));
                }
            }
        }

        BfsResult {
            visited,
            depths,
            edges,
        }
    }

    /// Extract a subgraph containing only the specified node IDs.
    ///
    /// Uses per-node adjacency lookups instead of scanning all edges,
    /// so cost is O(Σ degree(n) for n in node_ids) instead of O(E_total).
    pub fn subgraph(
        &self,
        branch_id: BranchId,
        graph: &str,
        node_ids: &[String],
    ) -> StrataResult<GraphSnapshot> {
        let node_set: HashSet<&str> = node_ids.iter().map(|s| s.as_str()).collect();
        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        // Collect nodes
        for id in node_ids {
            if let Some(data) = self.get_node(branch_id, graph, id)? {
                nodes.insert(id.clone(), data);
            }
        }

        // Collect edges between nodes in the set via per-node outgoing lookups
        for src_id in node_ids {
            if !nodes.contains_key(src_id) {
                continue; // skip nonexistent nodes
            }
            let neighbors = self.outgoing_neighbors(branch_id, graph, src_id, None)?;
            for n in neighbors {
                if node_set.contains(n.node_id.as_str()) {
                    edges.push(Edge {
                        src: src_id.clone(),
                        dst: n.node_id,
                        edge_type: n.edge_type,
                        data: n.edge_data,
                    });
                }
            }
        }

        Ok(GraphSnapshot { nodes, edges })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use std::sync::Arc;

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::cache().unwrap();
        let gs = GraphStore::new(db.clone());
        (db, gs)
    }

    fn branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    fn add_nodes(gs: &GraphStore, b: BranchId, g: &str, ids: &[&str]) {
        gs.create_graph(b, g, None).unwrap();
        for id in ids {
            gs.add_node(b, g, id, NodeData::default()).unwrap();
        }
    }

    // =========================================================================
    // Neighbors
    // =========================================================================

    #[test]
    fn node_no_outgoing_edges() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A"]);
        let n = gs
            .neighbors(b, "g", "A", Direction::Outgoing, None)
            .unwrap();
        assert!(n.is_empty());
    }

    #[test]
    fn node_one_outgoing_edge() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        let n = gs
            .neighbors(b, "g", "A", Direction::Outgoing, None)
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, "B");
    }

    #[test]
    fn node_five_mixed_outgoing_edges() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D", "E", "F"]);
        for (dst, et) in &[
            ("B", "T1"),
            ("C", "T2"),
            ("D", "T1"),
            ("E", "T3"),
            ("F", "T2"),
        ] {
            gs.add_edge(b, "g", "A", dst, et, EdgeData::default())
                .unwrap();
        }
        let n = gs
            .neighbors(b, "g", "A", Direction::Outgoing, None)
            .unwrap();
        assert_eq!(n.len(), 5);
    }

    #[test]
    fn direction_outgoing_only() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "E", EdgeData::default())
            .unwrap();
        let n = gs
            .neighbors(b, "g", "A", Direction::Outgoing, None)
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, "B");
    }

    #[test]
    fn direction_incoming_only() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "E", EdgeData::default())
            .unwrap();
        let n = gs
            .neighbors(b, "g", "A", Direction::Incoming, None)
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, "C");
    }

    #[test]
    fn direction_both() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "E", EdgeData::default())
            .unwrap();
        let n = gs.neighbors(b, "g", "A", Direction::Both, None).unwrap();
        assert_eq!(n.len(), 2);
    }

    #[test]
    fn edge_type_filter() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "HATES", EdgeData::default())
            .unwrap();
        let n = gs
            .neighbors(b, "g", "A", Direction::Outgoing, Some("KNOWS"))
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, "B");
    }

    #[test]
    fn edge_type_filter_no_matches() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        let n = gs
            .neighbors(b, "g", "A", Direction::Outgoing, Some("HATES"))
            .unwrap();
        assert!(n.is_empty());
    }

    #[test]
    fn nonexistent_node_returns_empty() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();
        let n = gs
            .neighbors(b, "g", "nonexistent", Direction::Outgoing, None)
            .unwrap();
        assert!(n.is_empty());
    }

    #[test]
    fn direction_both_with_mutual_edges() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        // A→B and B→A are distinct edges
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "A", "KNOWS", EdgeData::default())
            .unwrap();

        let n = gs.neighbors(b, "g", "A", Direction::Both, None).unwrap();
        // Should see both edges: outgoing A→B and incoming B→A
        assert_eq!(n.len(), 2);
        assert!(n.iter().all(|nb| nb.node_id == "B"));
    }

    #[test]
    fn direction_both_mutual_edges_different_weights() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(
            b,
            "g",
            "A",
            "B",
            "TRUST",
            EdgeData {
                weight: 0.9,
                properties: None,
            },
        )
        .unwrap();
        gs.add_edge(
            b,
            "g",
            "B",
            "A",
            "TRUST",
            EdgeData {
                weight: 0.3,
                properties: None,
            },
        )
        .unwrap();

        let n = gs.neighbors(b, "g", "A", Direction::Both, None).unwrap();
        assert_eq!(n.len(), 2);
        let mut weights: Vec<f64> = n.iter().map(|nb| nb.edge_data.weight).collect();
        weights.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!((weights[0] - 0.3).abs() < 1e-10);
        assert!((weights[1] - 0.9).abs() < 1e-10);
    }

    #[test]
    fn neighbors_returns_edge_data() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(
            b,
            "g",
            "A",
            "B",
            "SCORED",
            EdgeData {
                weight: 0.95,
                properties: Some(serde_json::json!({"source": "manual"})),
            },
        )
        .unwrap();

        let n = gs
            .neighbors(b, "g", "A", Direction::Outgoing, None)
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].edge_data.weight, 0.95);
        assert_eq!(
            n[0].edge_data.properties,
            Some(serde_json::json!({"source": "manual"}))
        );
    }

    // =========================================================================
    // Degree
    // =========================================================================

    #[test]
    fn degree_matches_neighbors_count() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E", EdgeData::default())
            .unwrap();
        assert_eq!(gs.degree(b, "g", "A", Direction::Outgoing).unwrap(), 2);
        assert_eq!(gs.degree(b, "g", "A", Direction::Incoming).unwrap(), 0);
    }

    #[test]
    fn degree_zero_no_edges() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A"]);
        assert_eq!(gs.degree(b, "g", "A", Direction::Both).unwrap(), 0);
    }

    #[test]
    fn self_loop_degree() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A"]);
        gs.add_edge(b, "g", "A", "A", "SELF", EdgeData::default())
            .unwrap();
        // Self-loop: outgoing=1, incoming=1, both=2 (same edge from each direction).
        assert_eq!(gs.degree(b, "g", "A", Direction::Outgoing).unwrap(), 1);
        assert_eq!(gs.degree(b, "g", "A", Direction::Incoming).unwrap(), 1);
        assert_eq!(gs.degree(b, "g", "A", Direction::Both).unwrap(), 2);
    }

    // =========================================================================
    // BFS
    // =========================================================================

    #[test]
    fn bfs_linear_chain_depth_2() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "D", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 2,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 3);
        assert!(result.visited.contains(&"A".to_string()));
        assert!(result.visited.contains(&"B".to_string()));
        assert!(result.visited.contains(&"C".to_string()));
        assert!(!result.visited.contains(&"D".to_string()));
    }

    #[test]
    fn bfs_linear_chain_unlimited() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "D", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 4);
    }

    #[test]
    fn bfs_fan_out_depth_1() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "D", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 4);
    }

    #[test]
    fn bfs_diamond() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "D", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "D", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 2,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 4);
        // D should appear exactly once in visited
        assert_eq!(result.visited.iter().filter(|&v| v == "D").count(), 1);
    }

    #[test]
    fn bfs_max_nodes() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D", "E"]);
        for dst in &["B", "C", "D", "E"] {
            gs.add_edge(b, "g", "A", dst, "E", EdgeData::default())
                .unwrap();
        }

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    max_nodes: Some(3),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 3);
    }

    #[test]
    fn bfs_cycle_terminates() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 3);
    }

    #[test]
    fn bfs_edge_type_filter() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "DEPENDS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "KNOWS", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    edge_types: Some(vec!["DEPENDS".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 2);
        assert!(result.visited.contains(&"B".to_string()));
        assert!(!result.visited.contains(&"C".to_string()));
    }

    #[test]
    fn bfs_incoming_direction() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "D", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "D",
                BfsOptions {
                    max_depth: 10,
                    direction: Direction::Incoming,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 4);
    }

    #[test]
    fn bfs_start_not_in_graph() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();

        let result = gs.bfs(b, "g", "ghost", BfsOptions::default()).unwrap();
        assert_eq!(result.visited.len(), 1);
        assert_eq!(result.visited[0], "ghost");
        assert_eq!(*result.depths.get("ghost").unwrap(), 0);
    }

    #[test]
    fn bfs_depth_0() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 0,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 1);
        assert_eq!(result.visited[0], "A");
    }

    #[test]
    fn bfs_depths_correct() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();

        let result = gs.bfs(b, "g", "A", BfsOptions::default()).unwrap();
        assert_eq!(*result.depths.get("A").unwrap(), 0);
        assert_eq!(*result.depths.get("B").unwrap(), 1);
        assert_eq!(*result.depths.get("C").unwrap(), 2);
    }

    #[test]
    fn bfs_edges_contain_traversed() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();

        let result = gs.bfs(b, "g", "A", BfsOptions::default()).unwrap();
        assert_eq!(result.edges.len(), 1);
        assert_eq!(
            result.edges[0],
            ("A".to_string(), "B".to_string(), "E".to_string())
        );
    }

    #[test]
    fn bfs_traversal_order_is_breadth_first() {
        let (_db, gs) = setup();
        let b = branch();
        // Build: A → B → D, A → C → E
        add_nodes(&gs, b, "g", &["A", "B", "C", "D", "E"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "D", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "E", "E", EdgeData::default())
            .unwrap();

        let result = gs.bfs(b, "g", "A", BfsOptions::default()).unwrap();

        // A should be first (depth 0)
        assert_eq!(result.visited[0], "A");
        // B and C at depth 1, before D and E at depth 2
        let b_pos = result.visited.iter().position(|v| v == "B").unwrap();
        let c_pos = result.visited.iter().position(|v| v == "C").unwrap();
        let d_pos = result.visited.iter().position(|v| v == "D").unwrap();
        let e_pos = result.visited.iter().position(|v| v == "E").unwrap();
        assert!(b_pos < d_pos, "B (depth 1) should come before D (depth 2)");
        assert!(b_pos < e_pos, "B (depth 1) should come before E (depth 2)");
        assert!(c_pos < d_pos, "C (depth 1) should come before D (depth 2)");
        assert!(c_pos < e_pos, "C (depth 1) should come before E (depth 2)");
    }

    #[test]
    fn bfs_max_nodes_1_returns_only_start() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    max_nodes: Some(1),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.visited.len(), 1);
        assert_eq!(result.visited[0], "A");
    }

    #[test]
    fn bfs_direction_both_traverses_bidirectionally() {
        let (_db, gs) = setup();
        let b = branch();
        // A → B, C → A (A has outgoing to B and incoming from C)
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "E", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 1,
                    direction: Direction::Both,
                    ..Default::default()
                },
            )
            .unwrap();
        // Should visit A, B (outgoing), and C (incoming)
        assert_eq!(result.visited.len(), 3);
        assert!(result.visited.contains(&"B".to_string()));
        assert!(result.visited.contains(&"C".to_string()));
    }

    // =========================================================================
    // build_adjacency_index
    // =========================================================================

    #[test]
    fn build_adjacency_index_captures_all_edges() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E1", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E2", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "A", "E3", EdgeData::default())
            .unwrap();

        let index = gs.build_adjacency_index(b, "g").unwrap();

        // Forward edges
        let a_out: Vec<_> = index.outgoing_neighbor_ids("A", None).collect();
        assert_eq!(a_out.len(), 1);
        assert_eq!(a_out[0], ("B", "E1"));

        let b_out: Vec<_> = index.outgoing_neighbor_ids("B", None).collect();
        assert_eq!(b_out.len(), 1);
        assert_eq!(b_out[0], ("C", "E2"));

        // Reverse edges
        let a_in: Vec<_> = index.incoming_neighbor_ids("A", None).collect();
        assert_eq!(a_in.len(), 1);
        assert_eq!(a_in[0], ("C", "E3"));

        let c_in: Vec<_> = index.incoming_neighbor_ids("C", None).collect();
        assert_eq!(c_in.len(), 1);
        assert_eq!(c_in[0], ("B", "E2"));
    }

    #[test]
    fn build_adjacency_index_empty_graph() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();

        let index = gs.build_adjacency_index(b, "g").unwrap();
        assert!(index.outgoing_neighbor_ids("A", None).next().is_none());
    }

    // =========================================================================
    // bfs_with_index (direct tests)
    // =========================================================================

    #[test]
    fn bfs_with_index_matches_bfs() {
        // Verify bfs_with_index produces identical results to bfs() on the
        // same graph — this is the primary correctness contract.
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "D", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "D", "E", EdgeData::default())
            .unwrap();

        let opts = BfsOptions {
            max_depth: 10,
            ..Default::default()
        };

        let via_bfs = gs.bfs(b, "g", "A", opts.clone()).unwrap();

        let index = gs.build_adjacency_index(b, "g").unwrap();
        let via_index = gs.bfs_with_index("A", &opts, &index);

        assert_eq!(via_bfs.visited, via_index.visited);
        assert_eq!(via_bfs.depths, via_index.depths);
        assert_eq!(via_bfs.edges, via_index.edges);
    }

    #[test]
    fn bfs_with_index_reuse_different_starts() {
        // The whole point of bfs_with_index: build once, traverse many times.
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "C", "D", "E", EdgeData::default())
            .unwrap();

        let index = gs.build_adjacency_index(b, "g").unwrap();
        let opts = BfsOptions::default();

        let from_a = gs.bfs_with_index("A", &opts, &index);
        assert_eq!(from_a.visited.len(), 4); // A → B → C → D

        let from_b = gs.bfs_with_index("B", &opts, &index);
        assert_eq!(from_b.visited.len(), 3); // B → C → D
        assert_eq!(*from_b.depths.get("B").unwrap(), 0);
        assert_eq!(*from_b.depths.get("C").unwrap(), 1);
        assert_eq!(*from_b.depths.get("D").unwrap(), 2);

        let from_d = gs.bfs_with_index("D", &opts, &index);
        assert_eq!(from_d.visited.len(), 1); // D has no outgoing edges
    }

    // =========================================================================
    // BFS edge cases
    // =========================================================================

    #[test]
    fn bfs_self_loop_does_not_double_visit() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "B", "SELF", EdgeData::default())
            .unwrap();

        let result = gs.bfs(b, "g", "A", BfsOptions::default()).unwrap();
        assert_eq!(result.visited.len(), 2);
        assert_eq!(
            result.visited.iter().filter(|v| v.as_str() == "B").count(),
            1
        );
        // Self-loop edge should not appear in traversal edges (B already seen
        // when we try to follow B→B).
        assert!(!result
            .edges
            .iter()
            .any(|(src, dst, _)| src == "B" && dst == "B"));
    }

    #[test]
    fn bfs_self_loop_direction_both() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A"]);
        gs.add_edge(b, "g", "A", "A", "SELF", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    direction: Direction::Both,
                    ..Default::default()
                },
            )
            .unwrap();
        // A should appear exactly once even though the self-loop is visible
        // from both outgoing and incoming directions.
        assert_eq!(result.visited.len(), 1);
        assert_eq!(result.visited[0], "A");
        assert!(result.edges.is_empty());
    }

    #[test]
    fn bfs_multi_type_edge_filter() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "C", "TRUSTS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "A", "D", "HATES", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    edge_types: Some(vec!["KNOWS".to_string(), "TRUSTS".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        // Should follow KNOWS and TRUSTS but not HATES
        assert_eq!(result.visited.len(), 3);
        assert!(result.visited.contains(&"B".to_string()));
        assert!(result.visited.contains(&"C".to_string()));
        assert!(!result.visited.contains(&"D".to_string()));
    }

    #[test]
    fn bfs_edge_type_filter_multi_hop() {
        // Verify edge_types filter applies at every hop, not just the first.
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C", "D"]);
        gs.add_edge(b, "g", "A", "B", "KNOWS", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "HATES", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "D", "KNOWS", EdgeData::default())
            .unwrap();

        let result = gs
            .bfs(
                b,
                "g",
                "A",
                BfsOptions {
                    max_depth: 10,
                    edge_types: Some(vec!["KNOWS".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        // A → B (KNOWS, OK), B → C (HATES, blocked), B → D (KNOWS, OK)
        assert_eq!(result.visited.len(), 3);
        assert!(result.visited.contains(&"A".to_string()));
        assert!(result.visited.contains(&"B".to_string()));
        assert!(result.visited.contains(&"D".to_string()));
        assert!(!result.visited.contains(&"C".to_string()));
    }

    #[test]
    fn bfs_on_graph_with_only_nodes() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);

        let result = gs.bfs(b, "g", "A", BfsOptions::default()).unwrap();
        assert_eq!(result.visited.len(), 1);
        assert_eq!(result.visited[0], "A");
        assert!(result.edges.is_empty());
    }

    // =========================================================================
    // Subgraph extraction
    // =========================================================================

    #[test]
    fn subgraph_full_set() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();

        let sub = gs
            .subgraph(b, "g", &["A".into(), "B".into(), "C".into()])
            .unwrap();
        assert_eq!(sub.node_count(), 3);
        assert_eq!(sub.edge_count(), 2);
    }

    #[test]
    fn subgraph_partial_set() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();
        gs.add_edge(b, "g", "B", "C", "E", EdgeData::default())
            .unwrap();

        let sub = gs.subgraph(b, "g", &["A".into(), "B".into()]).unwrap();
        assert_eq!(sub.node_count(), 2);
        assert_eq!(sub.edge_count(), 1); // Only A→B
    }

    #[test]
    fn subgraph_empty_set() {
        let (_db, gs) = setup();
        let b = branch();
        gs.create_graph(b, "g", None).unwrap();

        let sub = gs.subgraph(b, "g", &[]).unwrap();
        assert_eq!(sub.node_count(), 0);
        assert_eq!(sub.edge_count(), 0);
    }

    #[test]
    fn subgraph_single_node() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();

        let sub = gs.subgraph(b, "g", &["A".into()]).unwrap();
        assert_eq!(sub.node_count(), 1);
        assert_eq!(sub.edge_count(), 0); // No edge because B is not in set
    }

    #[test]
    fn subgraph_disconnected_nodes() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A", "B", "C"]);
        gs.add_edge(b, "g", "A", "B", "E", EdgeData::default())
            .unwrap();

        let sub = gs.subgraph(b, "g", &["A".into(), "C".into()]).unwrap();
        assert_eq!(sub.node_count(), 2);
        assert_eq!(sub.edge_count(), 0);
    }

    #[test]
    fn subgraph_nonexistent_nodes_skipped() {
        let (_db, gs) = setup();
        let b = branch();
        add_nodes(&gs, b, "g", &["A"]);

        let sub = gs.subgraph(b, "g", &["A".into(), "ghost".into()]).unwrap();
        assert_eq!(sub.node_count(), 1);
    }
}
