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

    /// Add an edge to the index.
    pub fn add_edge(&mut self, src: &str, dst: &str, edge_type: &str, data: EdgeData) {
        self.outgoing.entry(src.to_string()).or_default().push((
            dst.to_string(),
            edge_type.to_string(),
            data.clone(),
        ));
        self.incoming.entry(dst.to_string()).or_default().push((
            src.to_string(),
            edge_type.to_string(),
            data,
        ));
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
                    .filter(|(_, et, _)| edge_type_filter.map_or(true, |f| et == f))
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
                    .filter(|(_, et, _)| edge_type_filter.map_or(true, |f| et == f))
                    .map(|(src, et, data)| Neighbor {
                        node_id: src.clone(),
                        edge_type: et.clone(),
                        edge_data: data.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
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
    fn add_edge_duplicate_appends() {
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

        // AdjacencyIndex doesn't deduplicate — caller is responsible
        let out = idx.outgoing_neighbors("A", None);
        assert_eq!(out.len(), 2);
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
}
