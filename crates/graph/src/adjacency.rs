//! In-memory adjacency index for fast graph traversals.
//!
//! The adjacency index is a materialized view of the graph's edge structure,
//! loaded from KV storage. It provides O(1) neighbor lookups instead of
//! O(log n) prefix scans.

use std::collections::HashMap;

use super::types::{EdgeData, Neighbor};

/// In-memory adjacency index for a single graph.
#[derive(Debug, Clone, Default)]
pub struct AdjacencyIndex {
    /// Forward adjacency: src → [(dst, edge_type, EdgeData)]
    pub outgoing: HashMap<String, Vec<(String, String, EdgeData)>>,
    /// Reverse adjacency: dst → [(src, edge_type, EdgeData)]
    pub incoming: HashMap<String, Vec<(String, String, EdgeData)>>,
    /// Set of all node IDs in the graph.
    pub nodes: std::collections::HashSet<String>,
}

impl AdjacencyIndex {
    /// Create a new empty adjacency index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a node to the index.
    pub fn add_node(&mut self, node_id: &str) {
        self.nodes.insert(node_id.to_string());
    }

    /// Remove a node and all its incident edges.
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.remove(node_id);

        // Remove outgoing edges
        if let Some(edges) = self.outgoing.remove(node_id) {
            for (dst, edge_type, _) in edges {
                if let Some(incoming) = self.incoming.get_mut(&dst) {
                    incoming.retain(|(src, et, _)| !(src == node_id && et == &edge_type));
                }
            }
        }

        // Remove incoming edges
        if let Some(edges) = self.incoming.remove(node_id) {
            for (src, edge_type, _) in edges {
                if let Some(outgoing) = self.outgoing.get_mut(&src) {
                    outgoing.retain(|(dst, et, _)| !(dst == node_id && et == &edge_type));
                }
            }
        }
    }

    /// Add an edge to the index (dedup: last-write-wins for same dst+edge_type).
    pub fn add_edge(&mut self, src: &str, dst: &str, edge_type: &str, data: EdgeData) {
        // Forward: dedup on (dst, edge_type)
        let fwd = self.outgoing.entry(src.to_string()).or_default();
        if let Some(existing) = fwd
            .iter_mut()
            .find(|(d, et, _)| d == dst && et == edge_type)
        {
            existing.2 = data.clone();
        } else {
            fwd.push((dst.to_string(), edge_type.to_string(), data.clone()));
        }

        // Reverse: dedup on (src, edge_type)
        let rev = self.incoming.entry(dst.to_string()).or_default();
        if let Some(existing) = rev
            .iter_mut()
            .find(|(s, et, _)| s == src && et == edge_type)
        {
            existing.2 = data;
        } else {
            rev.push((src.to_string(), edge_type.to_string(), data));
        }
    }

    /// Remove an edge from the index.
    pub fn remove_edge(&mut self, src: &str, dst: &str, edge_type: &str) {
        if let Some(edges) = self.outgoing.get_mut(src) {
            edges.retain(|(d, et, _)| !(d == dst && et == edge_type));
        }
        if let Some(edges) = self.incoming.get_mut(dst) {
            edges.retain(|(s, et, _)| !(s == src && et == edge_type));
        }
    }

    /// Get outgoing neighbors, optionally filtered by edge type.
    pub fn outgoing_neighbors(
        &self,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> Vec<Neighbor> {
        self.outgoing
            .get(node_id)
            .map(|edges| {
                edges
                    .iter()
                    .filter(|(_, et, _)| edge_type_filter.is_none_or(|f| et == f))
                    .map(|(dst, et, data)| Neighbor {
                        node_id: dst.clone(),
                        edge_type: et.clone(),
                        edge_data: data.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get incoming neighbors, optionally filtered by edge type.
    pub fn incoming_neighbors(
        &self,
        node_id: &str,
        edge_type_filter: Option<&str>,
    ) -> Vec<Neighbor> {
        self.incoming
            .get(node_id)
            .map(|edges| {
                edges
                    .iter()
                    .filter(|(_, et, _)| edge_type_filter.is_none_or(|f| et == f))
                    .map(|(src, et, data)| Neighbor {
                        node_id: src.clone(),
                        edge_type: et.clone(),
                        edge_data: data.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Iterate outgoing neighbors without allocating Neighbor structs.
    ///
    /// Returns `(dst, edge_type)` pairs as string references, avoiding
    /// the Vec<Neighbor> + EdgeData cloning overhead of `outgoing_neighbors`.
    pub fn outgoing_neighbor_ids<'a>(
        &'a self,
        node_id: &str,
        edge_type_filter: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a str)> {
        self.outgoing
            .get(node_id)
            .into_iter()
            .flat_map(move |edges| {
                edges
                    .iter()
                    .filter(move |(_, et, _)| edge_type_filter.is_none_or(|f| et == f))
                    .map(|(dst, et, _)| (dst.as_str(), et.as_str()))
            })
    }

    /// Iterate outgoing neighbors with weights, without allocating Neighbor structs.
    ///
    /// Returns `(dst, weight)` pairs as string references + f64, avoiding
    /// the Vec<Neighbor> + EdgeData cloning overhead.
    pub fn outgoing_weighted<'a>(&'a self, node_id: &str) -> impl Iterator<Item = (&'a str, f64)> {
        self.outgoing.get(node_id).into_iter().flat_map(|edges| {
            edges
                .iter()
                .map(|(dst, _, data)| (dst.as_str(), data.weight))
        })
    }

    /// Iterate incoming neighbors without allocating Neighbor structs.
    ///
    /// Returns `(src, edge_type)` pairs as string references, avoiding
    /// the Vec<Neighbor> + EdgeData cloning overhead of `incoming_neighbors`.
    pub fn incoming_neighbor_ids<'a>(
        &'a self,
        node_id: &str,
        edge_type_filter: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a str)> {
        self.incoming
            .get(node_id)
            .into_iter()
            .flat_map(move |edges| {
                edges
                    .iter()
                    .filter(move |(_, et, _)| edge_type_filter.is_none_or(|f| et == f))
                    .map(|(src, et, _)| (src.as_str(), et.as_str()))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_adjacency_index() {
        let idx = AdjacencyIndex::new();
        assert!(idx.outgoing_neighbors("A", None).is_empty());
        assert!(idx.incoming_neighbors("A", None).is_empty());
    }

    #[test]
    fn add_node_and_edges() {
        let mut idx = AdjacencyIndex::new();
        idx.add_node("A");
        idx.add_node("B");
        idx.add_edge("A", "B", "KNOWS", EdgeData::default());

        let out = idx.outgoing_neighbors("A", None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, "B");

        let inc = idx.incoming_neighbors("B", None);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].node_id, "A");
    }

    #[test]
    fn remove_edge() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "KNOWS", EdgeData::default());
        idx.remove_edge("A", "B", "KNOWS");
        assert!(idx.outgoing_neighbors("A", None).is_empty());
        assert!(idx.incoming_neighbors("B", None).is_empty());
    }

    #[test]
    fn remove_node_cleans_edges() {
        let mut idx = AdjacencyIndex::new();
        idx.add_node("A");
        idx.add_node("B");
        idx.add_node("C");
        idx.add_edge("A", "B", "E", EdgeData::default());
        idx.add_edge("C", "A", "E", EdgeData::default());

        idx.remove_node("A");

        assert!(idx.outgoing_neighbors("A", None).is_empty());
        assert!(idx.incoming_neighbors("B", None).is_empty());
        assert!(idx.outgoing_neighbors("C", None).is_empty());
    }

    #[test]
    fn edge_type_filter() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "KNOWS", EdgeData::default());
        idx.add_edge("A", "C", "HATES", EdgeData::default());

        let knows = idx.outgoing_neighbors("A", Some("KNOWS"));
        assert_eq!(knows.len(), 1);
        assert_eq!(knows[0].node_id, "B");

        let hates = idx.outgoing_neighbors("A", Some("HATES"));
        assert_eq!(hates.len(), 1);
        assert_eq!(hates[0].node_id, "C");
    }

    #[test]
    fn multiple_edges() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "E1", EdgeData::default());
        idx.add_edge("A", "C", "E2", EdgeData::default());
        idx.add_edge("A", "D", "E3", EdgeData::default());

        assert_eq!(idx.outgoing_neighbors("A", None).len(), 3);
    }

    #[test]
    fn add_edge_duplicate_replaces() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "KNOWS", EdgeData::default());
        idx.add_edge(
            "A",
            "B",
            "KNOWS",
            EdgeData {
                weight: 2.0,
                properties: None,
            },
        );

        // Dedup: same (dst, edge_type) → last-write-wins
        let out = idx.outgoing_neighbors("A", None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].edge_data.weight, 2.0);

        let inc = idx.incoming_neighbors("B", None);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].edge_data.weight, 2.0);
    }

    #[test]
    fn different_edge_types_same_dst_not_deduped() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "KNOWS", EdgeData::default());
        idx.add_edge("A", "B", "LIKES", EdgeData::default());

        // Same (src, dst) but different edge types → kept as separate edges
        let out = idx.outgoing_neighbors("A", None);
        assert_eq!(out.len(), 2);

        let inc = idx.incoming_neighbors("B", None);
        assert_eq!(inc.len(), 2);

        // Each type should be separately queryable
        assert_eq!(idx.outgoing_neighbors("A", Some("KNOWS")).len(), 1);
        assert_eq!(idx.outgoing_neighbors("A", Some("LIKES")).len(), 1);
    }

    #[test]
    fn mutual_edges_tracked_separately() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge(
            "A",
            "B",
            "TRUST",
            EdgeData {
                weight: 0.9,
                properties: None,
            },
        );
        idx.add_edge(
            "B",
            "A",
            "TRUST",
            EdgeData {
                weight: 0.3,
                properties: None,
            },
        );

        let a_out = idx.outgoing_neighbors("A", None);
        assert_eq!(a_out.len(), 1);
        assert_eq!(a_out[0].edge_data.weight, 0.9);

        let a_in = idx.incoming_neighbors("A", None);
        assert_eq!(a_in.len(), 1);
        assert_eq!(a_in[0].edge_data.weight, 0.3);
    }

    #[test]
    fn remove_node_with_no_edges() {
        let mut idx = AdjacencyIndex::new();
        idx.add_node("A");
        idx.remove_node("A");
        assert!(!idx.nodes.contains("A"));
    }

    #[test]
    fn remove_nonexistent_node_is_noop() {
        let mut idx = AdjacencyIndex::new();
        idx.remove_node("ghost"); // Should not panic
    }

    #[test]
    fn remove_nonexistent_edge_is_noop() {
        let mut idx = AdjacencyIndex::new();
        idx.remove_edge("A", "B", "E"); // Should not panic
    }

    // =========================================================================
    // Zero-alloc iterator methods
    // =========================================================================

    #[test]
    fn outgoing_neighbor_ids_basic() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "KNOWS", EdgeData::default());
        idx.add_edge("A", "C", "HATES", EdgeData::default());

        let mut out: Vec<(&str, &str)> = idx.outgoing_neighbor_ids("A", None).collect();
        out.sort();
        assert_eq!(out, vec![("B", "KNOWS"), ("C", "HATES")]);
    }

    #[test]
    fn outgoing_neighbor_ids_with_filter() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "KNOWS", EdgeData::default());
        idx.add_edge("A", "C", "HATES", EdgeData::default());
        idx.add_edge("A", "D", "KNOWS", EdgeData::default());

        let out: Vec<(&str, &str)> = idx.outgoing_neighbor_ids("A", Some("KNOWS")).collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|(_, et)| *et == "KNOWS"));
    }

    #[test]
    fn outgoing_neighbor_ids_empty_node() {
        let idx = AdjacencyIndex::new();
        let out: Vec<(&str, &str)> = idx.outgoing_neighbor_ids("ghost", None).collect();
        assert!(out.is_empty());
    }

    #[test]
    fn incoming_neighbor_ids_basic() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("X", "A", "E1", EdgeData::default());
        idx.add_edge("Y", "A", "E2", EdgeData::default());

        let mut inc: Vec<(&str, &str)> = idx.incoming_neighbor_ids("A", None).collect();
        inc.sort();
        assert_eq!(inc, vec![("X", "E1"), ("Y", "E2")]);
    }

    #[test]
    fn incoming_neighbor_ids_with_filter() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("X", "A", "KNOWS", EdgeData::default());
        idx.add_edge("Y", "A", "HATES", EdgeData::default());

        let inc: Vec<(&str, &str)> = idx.incoming_neighbor_ids("A", Some("KNOWS")).collect();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0], ("X", "KNOWS"));
    }

    #[test]
    fn neighbor_ids_self_loop() {
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "A", "SELF", EdgeData::default());

        let out: Vec<_> = idx.outgoing_neighbor_ids("A", None).collect();
        assert_eq!(out, vec![("A", "SELF")]);

        let inc: Vec<_> = idx.incoming_neighbor_ids("A", None).collect();
        assert_eq!(inc, vec![("A", "SELF")]);
    }

    #[test]
    fn neighbor_ids_matches_neighbor_structs() {
        // Verify that the zero-alloc iterators return the same (node, type) pairs
        // as the allocating Vec<Neighbor> methods.
        let mut idx = AdjacencyIndex::new();
        idx.add_edge("A", "B", "E1", EdgeData::default());
        idx.add_edge("A", "C", "E2", EdgeData::default());
        idx.add_edge("D", "A", "E3", EdgeData::default());

        let alloc_out: Vec<(String, String)> = idx
            .outgoing_neighbors("A", None)
            .into_iter()
            .map(|n| (n.node_id, n.edge_type))
            .collect();
        let iter_out: Vec<(String, String)> = idx
            .outgoing_neighbor_ids("A", None)
            .map(|(id, et)| (id.to_string(), et.to_string()))
            .collect();
        assert_eq!(alloc_out, iter_out);

        let alloc_in: Vec<(String, String)> = idx
            .incoming_neighbors("A", None)
            .into_iter()
            .map(|n| (n.node_id, n.edge_type))
            .collect();
        let iter_in: Vec<(String, String)> = idx
            .incoming_neighbor_ids("A", None)
            .map(|(id, et)| (id.to_string(), et.to_string()))
            .collect();
        assert_eq!(alloc_in, iter_in);
    }
}
