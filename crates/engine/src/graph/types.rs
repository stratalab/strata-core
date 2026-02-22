//! Core graph types for the strata-graph module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Data stored on a graph node.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct NodeData {
    /// Optional reference to an entity in another primitive (e.g. "kv://main/patient-4821").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_ref: Option<String>,
    /// Arbitrary properties attached to this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

/// Data stored on a graph edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EdgeData {
    /// Edge weight (default 1.0).
    #[serde(default = "default_weight")]
    pub weight: f64,
    /// Arbitrary properties attached to this edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

fn default_weight() -> f64 {
    1.0
}

impl Default for EdgeData {
    fn default() -> Self {
        Self {
            weight: 1.0,
            properties: None,
        }
    }
}

/// Full edge representation including endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Edge {
    /// Source node ID.
    pub src: String,
    /// Destination node ID.
    pub dst: String,
    /// Edge type label.
    pub edge_type: String,
    /// Edge data (weight, properties).
    pub data: EdgeData,
}

/// Cascade policy for referential integrity when a bound entity is deleted.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CascadePolicy {
    /// Delete the graph node and all its edges.
    Cascade,
    /// Keep the node but remove the entity_ref binding.
    Detach,
    /// Do nothing (default).
    #[default]
    Ignore,
}

/// Metadata about a graph instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GraphMeta {
    /// Cascade policy when bound entities are deleted.
    #[serde(default)]
    pub cascade_policy: CascadePolicy,
}

/// Direction for traversal operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// Follow outgoing edges (src → dst).
    Outgoing,
    /// Follow incoming edges (dst → src).
    Incoming,
    /// Follow edges in both directions.
    Both,
}

/// Options for BFS traversal.
#[derive(Debug, Clone)]
pub struct BfsOptions {
    /// Maximum traversal depth.
    pub max_depth: usize,
    /// Maximum number of nodes to visit.
    pub max_nodes: Option<usize>,
    /// Only traverse edges of these types (None = all).
    pub edge_types: Option<Vec<String>>,
    /// Traversal direction.
    pub direction: Direction,
}

impl Default for BfsOptions {
    fn default() -> Self {
        Self {
            max_depth: usize::MAX,
            max_nodes: None,
            edge_types: None,
            direction: Direction::Outgoing,
        }
    }
}

/// Result of a BFS traversal.
#[derive(Debug, Clone, PartialEq)]
pub struct BfsResult {
    /// Set of visited node IDs.
    pub visited: Vec<String>,
    /// Depth at which each node was first discovered.
    pub depths: HashMap<String, usize>,
    /// Edges traversed during BFS.
    pub edges: Vec<(String, String, String)>,
}

/// A neighbor entry returned by neighbor queries.
#[derive(Debug, Clone, PartialEq)]
pub struct Neighbor {
    /// The neighbor node ID.
    pub node_id: String,
    /// The edge type connecting to this neighbor.
    pub edge_type: String,
    /// The edge data.
    pub edge_data: EdgeData,
}

/// A snapshot of a graph at a point in time.
#[derive(Debug, Clone)]
pub struct GraphSnapshot {
    /// All nodes: node_id → NodeData.
    pub nodes: HashMap<String, NodeData>,
    /// All edges.
    pub edges: Vec<Edge>,
}

impl GraphSnapshot {
    /// Number of nodes in the snapshot.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of edges in the snapshot.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Convert to edge list: (src, dst, edge_type, weight).
    pub fn to_edge_list(&self) -> Vec<(String, String, String, f64)> {
        self.edges
            .iter()
            .map(|e| {
                (
                    e.src.clone(),
                    e.dst.clone(),
                    e.edge_type.clone(),
                    e.data.weight,
                )
            })
            .collect()
    }

    /// Convert to adjacency list: src → [(dst, edge_type, weight)].
    pub fn to_adjacency_list(&self) -> HashMap<String, Vec<(String, String, f64)>> {
        let mut adj: HashMap<String, Vec<(String, String, f64)>> = HashMap::new();
        for e in &self.edges {
            adj.entry(e.src.clone()).or_default().push((
                e.dst.clone(),
                e.edge_type.clone(),
                e.data.weight,
            ));
        }
        adj
    }

    /// Export edges to CSV format.
    pub fn to_csv(&self) -> String {
        let mut out = String::from("src,dst,edge_type,weight\n");
        for e in &self.edges {
            // Escape fields that might contain commas
            let src = csv_escape(&e.src);
            let dst = csv_escape(&e.dst);
            let edge_type = csv_escape(&e.edge_type);
            out.push_str(&format!(
                "{},{},{},{}\n",
                src, dst, edge_type, e.data.weight
            ));
        }
        out
    }
}

/// Escape a CSV field value.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Trait for graph algorithms that operate on a snapshot.
pub trait GraphAlgorithm {
    /// The result type of this algorithm.
    type Output;
    /// Execute the algorithm on the given snapshot.
    fn execute(&self, snapshot: &GraphSnapshot) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_data_default_weight_is_1() {
        let data = EdgeData::default();
        assert_eq!(data.weight, 1.0);
    }

    #[test]
    fn cascade_policy_default_is_ignore() {
        let policy = CascadePolicy::default();
        assert_eq!(policy, CascadePolicy::Ignore);
    }

    #[test]
    fn serde_roundtrip_node_data_with_entity_ref() {
        let node = NodeData {
            entity_ref: Some("kv://main/patient-4821".to_string()),
            properties: Some(serde_json::json!({"department": "endocrinology"})),
        };
        let json = serde_json::to_string(&node).unwrap();
        let restored: NodeData = serde_json::from_str(&json).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn serde_roundtrip_node_data_without_entity_ref() {
        let node = NodeData {
            entity_ref: None,
            properties: None,
        };
        let json = serde_json::to_string(&node).unwrap();
        let restored: NodeData = serde_json::from_str(&json).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn serde_roundtrip_edge_data_with_custom_weight() {
        let data = EdgeData {
            weight: 0.95,
            properties: Some(serde_json::json!({"confidence": "high"})),
        };
        let json = serde_json::to_string(&data).unwrap();
        let restored: EdgeData = serde_json::from_str(&json).unwrap();
        assert_eq!(data, restored);
    }

    #[test]
    fn serde_roundtrip_edge_data_default() {
        let data = EdgeData::default();
        let json = serde_json::to_string(&data).unwrap();
        let restored: EdgeData = serde_json::from_str(&json).unwrap();
        assert_eq!(data, restored);
    }

    #[test]
    fn serde_roundtrip_edge_full() {
        let edge = Edge {
            src: "A".to_string(),
            dst: "B".to_string(),
            edge_type: "KNOWS".to_string(),
            data: EdgeData {
                weight: 0.5,
                properties: None,
            },
        };
        let json = serde_json::to_string(&edge).unwrap();
        let restored: Edge = serde_json::from_str(&json).unwrap();
        assert_eq!(edge, restored);
    }

    #[test]
    fn serde_roundtrip_graph_meta_each_cascade_policy() {
        for policy in [
            CascadePolicy::Cascade,
            CascadePolicy::Detach,
            CascadePolicy::Ignore,
        ] {
            let meta = GraphMeta {
                cascade_policy: policy.clone(),
            };
            let json = serde_json::to_string(&meta).unwrap();
            let restored: GraphMeta = serde_json::from_str(&json).unwrap();
            assert_eq!(meta, restored);
        }
    }

    #[test]
    fn cascade_policy_serde_lowercase() {
        // Verify serde serializes as lowercase (matching API input format)
        let json = serde_json::to_string(&CascadePolicy::Cascade).unwrap();
        assert_eq!(json, "\"cascade\"");
        let json = serde_json::to_string(&CascadePolicy::Detach).unwrap();
        assert_eq!(json, "\"detach\"");
        let json = serde_json::to_string(&CascadePolicy::Ignore).unwrap();
        assert_eq!(json, "\"ignore\"");
    }

    #[test]
    fn cascade_policy_deserialize_lowercase() {
        let c: CascadePolicy = serde_json::from_str("\"cascade\"").unwrap();
        assert_eq!(c, CascadePolicy::Cascade);
        let d: CascadePolicy = serde_json::from_str("\"detach\"").unwrap();
        assert_eq!(d, CascadePolicy::Detach);
        let i: CascadePolicy = serde_json::from_str("\"ignore\"").unwrap();
        assert_eq!(i, CascadePolicy::Ignore);
    }

    #[test]
    fn direction_variants_construct_and_compare() {
        assert_ne!(Direction::Outgoing, Direction::Incoming);
        assert_ne!(Direction::Outgoing, Direction::Both);
        assert_ne!(Direction::Incoming, Direction::Both);
        assert_eq!(Direction::Outgoing, Direction::Outgoing);
    }

    #[test]
    fn bfs_options_defaults() {
        let opts = BfsOptions::default();
        assert!(opts.max_nodes.is_none());
        assert!(opts.edge_types.is_none());
        assert_eq!(opts.direction, Direction::Outgoing);
    }
}
