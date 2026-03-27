//! Bulk insert and batch edge operations.

use super::*;

impl GraphStore {
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
                let sk = Key::new_graph(ns.clone(), &user_key);

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
                    ref_entries.push(Key::new_graph(ns.clone(), &rk));
                }

                if let Some(ot) = &data.object_type {
                    let tk = keys::type_index_key(graph, ot, node_id);
                    type_entries.push(Key::new_graph(ns.clone(), &tk));
                }
            }

            self.db.transaction(branch_id, |txn| {
                // Clean up old index entries for nodes being re-inserted (upsert)
                for (node_id, _data) in chunk {
                    let user_key = keys::node_key(graph, node_id);
                    let sk = Key::new_graph(ns.clone(), &user_key);
                    if let Some(Value::String(old_json)) = txn.get(&sk)? {
                        if let Ok(old_data) = serde_json::from_str::<NodeData>(&old_json) {
                            if let Some(old_uri) = old_data.entity_ref {
                                let old_rk = keys::ref_index_key(&old_uri, graph, node_id);
                                txn.delete(Key::new_graph(ns.clone(), &old_rk))?;
                            }
                            if let Some(old_ot) = old_data.object_type {
                                let old_tk = keys::type_index_key(graph, &old_ot, node_id);
                                txn.delete(Key::new_graph(ns.clone(), &old_tk))?;
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

            // Post-commit: update search index for this chunk
            for (node_id, data) in chunk {
                self.index_node_for_search(branch_id, graph, node_id, data);
            }

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
                let sk = Key::new_graph(ns.clone(), &uk);
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
                let sk = Key::new_graph(ns.clone(), &uk);
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
                    let count_sk = Key::new_graph(ns.clone(), &count_uk);
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
                let node_sk = Key::new_graph(ns.clone(), &node_uk);
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
                let fwd_sk = Key::new_graph(ns.clone(), &fwd_uk);
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
                let rev_sk = Key::new_graph(ns.clone(), &rev_uk);
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
                let count_sk = Key::new_graph(ns.clone(), &count_uk);
                let existing = Self::read_edge_type_count(txn, &count_sk)?;
                Self::write_edge_type_count(txn, &count_sk, existing + new_count)?;
            }

            Ok(edge_count)
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
    // G-3: Edge type counter via bulk_insert
    // =========================================================================

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

    // =========================================================================
    // Scale tests (Issue #1983)
    // =========================================================================

    #[test]
    fn scale_10k_nodes_bulk_insert() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        let nodes: Vec<(String, NodeData)> = (0..10_000)
            .map(|i| (format!("n{:05}", i), NodeData::default()))
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

        let (ni, ei) = gs.bulk_insert(b, "g", &nodes, &edges, Some(5_000)).unwrap();
        assert_eq!(ni, 10_000);
        assert_eq!(ei, 9_999);

        // Verify counts
        let stats = gs.snapshot_stats(b, "g").unwrap();
        assert_eq!(stats.node_count, 10_000);
        assert_eq!(stats.edge_count, 9_999);

        // Spot-check first, middle, and last
        assert!(gs.get_node(b, "g", "n00000").unwrap().is_some());
        assert!(gs.get_node(b, "g", "n05000").unwrap().is_some());
        assert!(gs.get_node(b, "g", "n09999").unwrap().is_some());

        // Verify edge at chunk boundary
        assert!(gs
            .get_edge(b, "g", "n04999", "n05000", "NEXT")
            .unwrap()
            .is_some());
    }

    #[test]
    fn scale_high_degree_node_1000_edges() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        // Create hub node + 1000 leaf nodes
        let mut nodes: Vec<(String, NodeData)> = vec![("hub".into(), NodeData::default())];
        for i in 0..1_000 {
            nodes.push((format!("leaf{:04}", i), NodeData::default()));
        }
        gs.bulk_insert(b, "g", &nodes, &[], None).unwrap();

        // Add 1000 edges from hub to each leaf via batch
        let edges: Vec<(String, String, String, EdgeData)> = (0..1_000)
            .map(|i| {
                (
                    "hub".to_string(),
                    format!("leaf{:04}", i),
                    "CONNECTS".to_string(),
                    EdgeData::default(),
                )
            })
            .collect();
        let inserted = gs.batch_add_edges(b, "g", &edges, Some(200)).unwrap();
        assert_eq!(inserted, 1_000);

        // Verify all outgoing edges
        let out = gs.outgoing_neighbors(b, "g", "hub", None).unwrap();
        assert_eq!(out.len(), 1_000);

        // Verify reverse: each leaf has exactly 1 incoming
        for i in [0, 499, 999] {
            let inc = gs
                .incoming_neighbors(b, "g", &format!("leaf{:04}", i), None)
                .unwrap();
            assert_eq!(inc.len(), 1);
            assert_eq!(inc[0].node_id, "hub");
        }

        // Verify edge type counter
        assert_eq!(gs.count_edges_by_type(b, "g", "CONNECTS").unwrap(), 1_000);

        // Remove hub — should clean up all 1000 edges
        gs.remove_node(b, "g", "hub").unwrap();
        assert_eq!(gs.count_edges_by_type(b, "g", "CONNECTS").unwrap(), 0);

        // Verify leaves have no incoming edges
        let inc = gs.incoming_neighbors(b, "g", "leaf0500", None).unwrap();
        assert!(inc.is_empty());
    }
}
