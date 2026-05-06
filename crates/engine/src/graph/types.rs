//! Core graph types for the engine-owned graph module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// Ontology Types
// =============================================================================

/// Status of a graph's ontology lifecycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OntologyStatus {
    /// Ontology is being defined; types can be added/removed.
    #[default]
    Draft,
    /// Ontology is finalized; schema validation is active.
    Frozen,
}

/// Definition of a property on an object or link type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyDef {
    /// Property type hint (e.g. "string", "integer"). Recorded for AI orientation, NOT enforced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Whether this property is required on nodes of this type.
    #[serde(default)]
    pub required: bool,
}

/// Definition of an object (node) type in the ontology.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectTypeDef {
    /// Type name (e.g. "Patient", "LabResult").
    pub name: String,
    /// Property definitions for this type.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, PropertyDef>,
}

/// Definition of a link (edge) type in the ontology.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkTypeDef {
    /// Type name (e.g. "HAS_RESULT").
    pub name: String,
    /// Source object type name.
    pub source: String,
    /// Target object type name.
    pub target: String,
    /// Cardinality hint (e.g. "one-to-many"). Recorded for AI orientation, NOT enforced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<String>,
    /// Property definitions for this link type.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, PropertyDef>,
}

/// Summary of an object type for introspection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectTypeSummary {
    /// Property definitions.
    pub properties: HashMap<String, PropertyDef>,
    /// Number of nodes of this type.
    pub node_count: u64,
}

/// Summary of a link type for introspection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkTypeSummary {
    /// Source object type.
    pub source: String,
    /// Target object type.
    pub target: String,
    /// Cardinality hint.
    pub cardinality: Option<String>,
    /// Property definitions.
    pub properties: HashMap<String, PropertyDef>,
    /// Number of edges of this type.
    pub edge_count: u64,
}

/// Complete ontology summary for AI orientation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OntologySummary {
    /// Current ontology lifecycle status.
    pub status: OntologyStatus,
    /// Object type summaries keyed by type name.
    pub object_types: HashMap<String, ObjectTypeSummary>,
    /// Link type summaries keyed by type name.
    pub link_types: HashMap<String, LinkTypeSummary>,
}

/// Data stored on a graph node.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct NodeData {
    /// Optional reference to an entity in another primitive (e.g. "kv://main/patient-4821").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_ref: Option<String>,
    /// Arbitrary properties attached to this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
    /// Optional ontology object type (e.g. "Patient").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_type: Option<String>,
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
    /// Ontology lifecycle status (None = untyped graph).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ontology_status: Option<OntologyStatus>,
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
            max_depth: 100,
            max_nodes: Some(10_000),
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

// =============================================================================
// Analytics Result & Options Types
// =============================================================================

/// Result of Weakly Connected Components (WCC).
#[derive(Debug, Clone, PartialEq)]
pub struct WccResult {
    /// Node → component ID (smallest node hash in each component).
    pub components: HashMap<String, u64>,
}

/// Result of Community Detection via Label Propagation (CDLP).
#[derive(Debug, Clone, PartialEq)]
pub struct CdlpResult {
    /// Node → community label.
    pub labels: HashMap<String, u64>,
}

/// Options for CDLP.
#[derive(Debug, Clone)]
pub struct CdlpOptions {
    /// Maximum iterations before stopping.
    pub max_iterations: usize,
    /// Traversal direction for neighbor lookups.
    pub direction: Direction,
}

impl Default for CdlpOptions {
    fn default() -> Self {
        Self {
            max_iterations: 10,
            direction: Direction::Both,
        }
    }
}

/// Result of PageRank.
#[derive(Debug, Clone, PartialEq)]
pub struct PageRankResult {
    /// Node → rank score.
    pub ranks: HashMap<String, f64>,
    /// Number of iterations performed.
    pub iterations: usize,
}

/// Options for PageRank.
#[derive(Debug, Clone)]
pub struct PageRankOptions {
    /// Damping factor (default 0.85).
    pub damping: f64,
    /// Maximum iterations (default 20).
    pub max_iterations: usize,
    /// Convergence tolerance (default 1e-6).
    pub tolerance: f64,
}

impl Default for PageRankOptions {
    fn default() -> Self {
        Self {
            damping: 0.85,
            max_iterations: 20,
            tolerance: 1e-6,
        }
    }
}

/// Result of Local Clustering Coefficient (LCC).
#[derive(Debug, Clone, PartialEq)]
pub struct LccResult {
    /// Node → clustering coefficient (0.0 for degree < 2).
    pub coefficients: HashMap<String, f64>,
}

/// Result of Single-Source Shortest Path (SSSP).
#[derive(Debug, Clone, PartialEq)]
pub struct SsspResult {
    /// Node → shortest distance from source (unreachable nodes omitted).
    pub distances: HashMap<String, f64>,
}

/// Options for SSSP.
#[derive(Debug, Clone)]
pub struct SsspOptions {
    /// Direction for edge traversal.
    pub direction: Direction,
}

impl Default for SsspOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
        }
    }
}

/// Result of a cascade operation triggered by entity deletion.
#[derive(Debug, Clone)]
pub struct CascadeResult {
    /// Number of bindings that were processed successfully.
    pub succeeded: usize,
    /// Per-binding failures (graph, node_id, error message).
    pub failed: Vec<CascadeError>,
}

/// A single cascade failure for one (graph, node_id) binding.
#[derive(Debug, Clone)]
pub struct CascadeError {
    /// Graph name where the cascade failed.
    pub graph: String,
    /// Node ID that failed to cascade.
    pub node_id: String,
    /// Error description.
    pub error: String,
}

impl CascadeResult {
    /// Returns true if all bindings were processed successfully.
    pub fn is_ok(&self) -> bool {
        self.failed.is_empty()
    }
}

/// Statistics about a graph (node and edge counts without full materialization).
#[derive(Debug, Clone, PartialEq)]
pub struct GraphStats {
    /// Number of nodes in the graph.
    pub node_count: usize,
    /// Number of edges in the graph.
    pub edge_count: usize,
}

/// Cursor-based pagination request.
#[derive(Debug, Clone)]
pub struct PageRequest {
    /// Maximum number of items to return.
    pub limit: usize,
    /// Exclusive start key for continuation (None = start from beginning).
    pub cursor: Option<String>,
}

/// Cursor-based pagination response.
#[derive(Debug, Clone, PartialEq)]
pub struct PageResponse<T> {
    /// Items in this page.
    pub items: Vec<T>,
    /// Cursor for the next page (None = last page).
    pub next_cursor: Option<String>,
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
            ..Default::default()
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
            ..Default::default()
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
                ..Default::default()
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
        assert_eq!(opts.max_depth, 100);
        assert_eq!(opts.max_nodes, Some(10_000));
        assert!(opts.edge_types.is_none());
        assert_eq!(opts.direction, Direction::Outgoing);
    }

    // =========================================================================
    // Ontology type tests
    // =========================================================================

    #[test]
    fn ontology_status_default_is_draft() {
        let status = OntologyStatus::default();
        assert_eq!(status, OntologyStatus::Draft);
    }

    #[test]
    fn ontology_status_serde_lowercase() {
        let json = serde_json::to_string(&OntologyStatus::Draft).unwrap();
        assert_eq!(json, "\"draft\"");
        let json = serde_json::to_string(&OntologyStatus::Frozen).unwrap();
        assert_eq!(json, "\"frozen\"");
    }

    #[test]
    fn ontology_status_deserialize_lowercase() {
        let d: OntologyStatus = serde_json::from_str("\"draft\"").unwrap();
        assert_eq!(d, OntologyStatus::Draft);
        let f: OntologyStatus = serde_json::from_str("\"frozen\"").unwrap();
        assert_eq!(f, OntologyStatus::Frozen);
    }

    #[test]
    fn serde_roundtrip_property_def() {
        let prop = PropertyDef {
            r#type: Some("string".to_string()),
            required: true,
        };
        let json = serde_json::to_string(&prop).unwrap();
        let restored: PropertyDef = serde_json::from_str(&json).unwrap();
        assert_eq!(prop, restored);
    }

    #[test]
    fn serde_roundtrip_property_def_minimal() {
        let prop = PropertyDef {
            r#type: None,
            required: false,
        };
        let json = serde_json::to_string(&prop).unwrap();
        let restored: PropertyDef = serde_json::from_str(&json).unwrap();
        assert_eq!(prop, restored);
    }

    #[test]
    fn serde_roundtrip_object_type_def() {
        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            PropertyDef {
                r#type: Some("string".to_string()),
                required: true,
            },
        );
        let def = ObjectTypeDef {
            name: "Patient".to_string(),
            properties,
        };
        let json = serde_json::to_string(&def).unwrap();
        let restored: ObjectTypeDef = serde_json::from_str(&json).unwrap();
        assert_eq!(def, restored);
    }

    #[test]
    fn serde_roundtrip_object_type_def_no_properties() {
        let def = ObjectTypeDef {
            name: "Tag".to_string(),
            properties: HashMap::new(),
        };
        let json = serde_json::to_string(&def).unwrap();
        let restored: ObjectTypeDef = serde_json::from_str(&json).unwrap();
        assert_eq!(def, restored);
    }

    #[test]
    fn serde_roundtrip_link_type_def() {
        let mut properties = HashMap::new();
        properties.insert(
            "date".to_string(),
            PropertyDef {
                r#type: Some("string".to_string()),
                required: false,
            },
        );
        let def = LinkTypeDef {
            name: "HAS_RESULT".to_string(),
            source: "Patient".to_string(),
            target: "LabResult".to_string(),
            cardinality: Some("one-to-many".to_string()),
            properties,
        };
        let json = serde_json::to_string(&def).unwrap();
        let restored: LinkTypeDef = serde_json::from_str(&json).unwrap();
        assert_eq!(def, restored);
    }

    #[test]
    fn serde_roundtrip_link_type_def_minimal() {
        let def = LinkTypeDef {
            name: "KNOWS".to_string(),
            source: "Person".to_string(),
            target: "Person".to_string(),
            cardinality: None,
            properties: HashMap::new(),
        };
        let json = serde_json::to_string(&def).unwrap();
        let restored: LinkTypeDef = serde_json::from_str(&json).unwrap();
        assert_eq!(def, restored);
    }

    #[test]
    fn backward_compat_old_graph_meta_json() {
        // Old GraphMeta without ontology_status should deserialize correctly
        let old_json = r#"{"cascade_policy":"ignore"}"#;
        let meta: GraphMeta = serde_json::from_str(old_json).unwrap();
        assert_eq!(meta.cascade_policy, CascadePolicy::Ignore);
        assert!(meta.ontology_status.is_none());
    }

    #[test]
    fn backward_compat_old_node_data_json() {
        // Old NodeData without object_type should deserialize correctly
        let old_json = r#"{"entity_ref":"kv://main/key1"}"#;
        let data: NodeData = serde_json::from_str(old_json).unwrap();
        assert_eq!(data.entity_ref, Some("kv://main/key1".to_string()));
        assert!(data.object_type.is_none());
    }

    #[test]
    fn serde_roundtrip_node_data_with_object_type() {
        let node = NodeData {
            entity_ref: None,
            properties: Some(serde_json::json!({"name": "Alice"})),
            object_type: Some("Patient".to_string()),
        };
        let json = serde_json::to_string(&node).unwrap();
        let restored: NodeData = serde_json::from_str(&json).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn serde_roundtrip_graph_meta_with_ontology() {
        let meta = GraphMeta {
            cascade_policy: CascadePolicy::Ignore,
            ontology_status: Some(OntologyStatus::Frozen),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let restored: GraphMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, restored);
    }
}
