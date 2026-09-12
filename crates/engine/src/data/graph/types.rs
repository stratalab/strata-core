//! Graph core input types.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::branch::BranchName;
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;

const MAX_GRAPH_NAME_BYTES: usize = 256;
const MAX_NODE_ID_BYTES: usize = 1024;
const MAX_EDGE_TYPE_BYTES: usize = 256;
const MAX_BINDING_KEY_BYTES: usize = 1024;
const MAX_GRAPH_PROPERTIES_BYTES: usize = 16 * 1024 * 1024;

/// Graph name within one branch and product space.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct GraphName(String);

impl GraphName {
    /// Creates a validated graph name.
    pub fn new(name: impl Into<String>) -> Result<Self, EngineError> {
        let name = name.into();
        validate_text_component(
            &name,
            MAX_GRAPH_NAME_BYTES,
            "invalid_argument.engine.graph_name",
            "graph name",
        )?;
        if name.starts_with('_') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_name_reserved",
                "graph name is reserved for engine internals",
            ));
        }
        if name.contains('/') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_name",
                "graph name must not contain `/`",
            ));
        }
        Ok(Self(name))
    }

    #[must_use]
    /// Returns the graph name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for GraphName {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for GraphName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GraphName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Node id within one graph.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct GraphNodeId(String);

impl GraphNodeId {
    /// Creates a validated node id.
    pub fn new(node_id: impl Into<String>) -> Result<Self, EngineError> {
        let node_id = node_id.into();
        validate_text_component(
            &node_id,
            MAX_NODE_ID_BYTES,
            "invalid_argument.engine.graph_node_id",
            "graph node id",
        )?;
        Ok(Self(node_id))
    }

    #[must_use]
    /// Returns the node id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for GraphNodeId {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for GraphNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GraphNodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Edge type within one graph.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct GraphEdgeType(String);

impl GraphEdgeType {
    /// Creates a validated edge type.
    pub fn new(edge_type: impl Into<String>) -> Result<Self, EngineError> {
        let edge_type = edge_type.into();
        validate_text_component(
            &edge_type,
            MAX_EDGE_TYPE_BYTES,
            "invalid_argument.engine.graph_edge_type",
            "graph edge type",
        )?;
        if edge_type.starts_with('_') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_edge_type_reserved",
                "graph edge type is reserved for engine internals",
            ));
        }
        Ok(Self(edge_type))
    }

    #[must_use]
    /// Returns the edge type.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for GraphEdgeType {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for GraphEdgeType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GraphEdgeType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Neighbor traversal direction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphDirection {
    /// Outgoing edges from the selected node.
    #[default]
    Outgoing,
    /// Incoming edges into the selected node.
    Incoming,
    /// Incoming and outgoing edges.
    Both,
}

/// Explicit policy for graph facts bound to a deleted entity, per the
/// relationship-layer contract vocabulary. Reject-delete is not a V1
/// policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphDeletePolicy {
    /// Delete the bound graph nodes and their incident edges.
    Cascade,
    /// Preserve the graph nodes and remove their entity bindings.
    Detach,
    /// Preserve the bindings; traversal reports the target's status.
    KeepDangling,
}

/// Typed primitive kind for graph entity bindings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphBindingPrimitive {
    /// KV primitive.
    Kv,
    /// JSON primitive.
    Json,
    /// Vector primitive.
    Vector,
    /// Event primitive.
    Event,
    /// Graph primitive.
    Graph,
}

impl GraphBindingPrimitive {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Kv => "kv",
            Self::Json => "json",
            Self::Vector => "vector",
            Self::Event => "event",
            Self::Graph => "graph",
        }
    }
}

/// Typed product identity that can be attached to a graph node.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub struct GraphBindingTarget {
    primitive: GraphBindingPrimitive,
    #[serde(skip_serializing_if = "Option::is_none")]
    branch: Option<BranchName>,
    space: ProductSpace,
    key: String,
}

impl GraphBindingTarget {
    /// Creates a validated graph binding target.
    pub fn new(
        primitive: GraphBindingPrimitive,
        branch: Option<BranchName>,
        space: ProductSpace,
        key: impl Into<String>,
    ) -> Result<Self, EngineError> {
        let key = key.into();
        validate_text_component(
            &key,
            MAX_BINDING_KEY_BYTES,
            "invalid_argument.engine.graph_binding",
            "graph binding key",
        )?;
        Ok(Self {
            primitive,
            branch,
            space,
            key,
        })
    }

    #[must_use]
    /// Returns the primitive kind.
    pub const fn primitive(&self) -> GraphBindingPrimitive {
        self.primitive
    }

    #[must_use]
    /// Returns the optional bound branch.
    pub const fn branch(&self) -> Option<&BranchName> {
        self.branch.as_ref()
    }

    #[must_use]
    /// Returns the bound product space.
    pub const fn space(&self) -> &ProductSpace {
        &self.space
    }

    #[must_use]
    /// Returns the bound product key.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl<'de> Deserialize<'de> for GraphBindingTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StoredTarget {
            primitive: GraphBindingPrimitive,
            #[serde(default)]
            branch: Option<BranchName>,
            space: ProductSpace,
            key: String,
        }

        let stored = StoredTarget::deserialize(deserializer)?;
        Self::new(stored.primitive, stored.branch, stored.space, stored.key)
            .map_err(serde::de::Error::custom)
    }
}

/// Node-to-entity binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GraphEntityBinding {
    target: GraphBindingTarget,
}

impl GraphEntityBinding {
    /// Creates a node binding for the target.
    #[must_use]
    pub const fn new(target: GraphBindingTarget) -> Self {
        Self { target }
    }

    #[must_use]
    /// Returns the bound target.
    pub const fn target(&self) -> &GraphBindingTarget {
        &self.target
    }
}

/// JSON object properties stored on graph nodes and edges.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct GraphProperties(Value);

impl GraphProperties {
    /// Creates validated graph properties.
    pub fn new(value: Value) -> Result<Self, EngineError> {
        if !value.is_object() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_properties",
                "graph properties must be a JSON object",
            ));
        }
        let size = serde_json::to_vec(&value)
            .map_err(|error| {
                EngineError::invalid_input(
                    "invalid_argument.engine.graph_properties",
                    format!("graph properties cannot be encoded: {error}"),
                )
            })?
            .len();
        if size > MAX_GRAPH_PROPERTIES_BYTES {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_properties_too_large",
                "graph properties exceed the maximum encoded size",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    /// Returns the wrapped JSON object.
    pub const fn as_inner(&self) -> &Value {
        &self.0
    }

    #[must_use]
    /// Consumes the wrapper and returns the JSON value.
    pub fn into_inner(self) -> Value {
        self.0
    }

    pub(crate) fn clone_inner(&self) -> Value {
        self.0.clone()
    }

    #[must_use]
    /// Returns true when the property key is present (any value counts,
    /// including an explicit null).
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.as_object().is_some_and(|map| map.contains_key(key))
    }
}

impl TryFrom<Value> for GraphProperties {
    type Error = EngineError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for GraphProperties {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Graph node payload.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GraphNodeData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    properties: Option<GraphProperties>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binding: Option<GraphEntityBinding>,
    /// Declared object type (GO2). Untyped nodes are always accepted; a
    /// typed node is validated against the graph's ontology once frozen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_type: Option<super::ontology::GraphTypeName>,
}

impl GraphNodeData {
    /// Creates graph node data (untyped; see [`Self::with_object_type`]).
    #[must_use]
    pub const fn new(
        properties: Option<GraphProperties>,
        binding: Option<GraphEntityBinding>,
    ) -> Self {
        Self {
            properties,
            binding,
            object_type: None,
        }
    }

    /// Declares the node's object type.
    #[must_use]
    pub fn with_object_type(mut self, object_type: super::ontology::GraphTypeName) -> Self {
        self.object_type = Some(object_type);
        self
    }

    #[must_use]
    /// Returns optional node properties.
    pub const fn properties(&self) -> Option<&GraphProperties> {
        self.properties.as_ref()
    }

    #[must_use]
    /// Returns optional entity binding.
    pub const fn binding(&self) -> Option<&GraphEntityBinding> {
        self.binding.as_ref()
    }

    #[must_use]
    /// Returns the declared object type.
    pub const fn object_type(&self) -> Option<&super::ontology::GraphTypeName> {
        self.object_type.as_ref()
    }
}

/// Graph edge payload.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GraphEdgeData {
    weight: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<GraphProperties>,
}

impl GraphEdgeData {
    /// Creates graph edge data.
    pub fn new(weight: f64, properties: Option<GraphProperties>) -> Result<Self, EngineError> {
        if !weight.is_finite() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_edge_weight",
                "graph edge weight must be finite",
            ));
        }
        Ok(Self { weight, properties })
    }

    /// Creates graph edge data with the default weight.
    #[must_use]
    pub const fn default_weight(properties: Option<GraphProperties>) -> Self {
        Self {
            weight: 1.0,
            properties,
        }
    }

    #[must_use]
    /// Returns the edge weight.
    pub const fn weight(&self) -> f64 {
        self.weight
    }

    #[must_use]
    /// Returns optional edge properties.
    pub const fn properties(&self) -> Option<&GraphProperties> {
        self.properties.as_ref()
    }
}

impl Default for GraphEdgeData {
    fn default() -> Self {
        Self::default_weight(None)
    }
}

impl<'de> Deserialize<'de> for GraphEdgeData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StoredEdgeData {
            #[serde(default)]
            weight: Option<f64>,
            #[serde(default)]
            properties: Option<GraphProperties>,
        }

        let stored = StoredEdgeData::deserialize(deserializer)?;
        Self::new(stored.weight.unwrap_or(1.0), stored.properties).map_err(serde::de::Error::custom)
    }
}

/// One graph batch operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum GraphBatchOperation {
    /// Upserts one node.
    UpsertNode {
        /// Node id.
        node_id: GraphNodeId,
        /// Node payload.
        data: GraphNodeData,
    },
    /// Deletes one node and its incident edges.
    DeleteNode {
        /// Node id.
        node_id: GraphNodeId,
    },
    /// Upserts one edge.
    UpsertEdge {
        /// Source node id.
        src: GraphNodeId,
        /// Edge type.
        edge_type: GraphEdgeType,
        /// Destination node id.
        dst: GraphNodeId,
        /// Edge payload.
        data: GraphEdgeData,
    },
    /// Deletes one edge.
    DeleteEdge {
        /// Source node id.
        src: GraphNodeId,
        /// Edge type.
        edge_type: GraphEdgeType,
        /// Destination node id.
        dst: GraphNodeId,
    },
}

/// All-or-nothing graph batch write request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GraphBatchWrite(Vec<GraphBatchOperation>);

impl GraphBatchWrite {
    /// Creates a graph batch from operations.
    #[must_use]
    pub fn new(operations: Vec<GraphBatchOperation>) -> Self {
        Self(operations)
    }

    #[must_use]
    /// Returns batch operations.
    pub fn operations(&self) -> &[GraphBatchOperation] {
        &self.0
    }

    #[must_use]
    /// Returns true when the batch has no operations.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub(super) fn validate_text_component(
    value: &str,
    max_bytes: usize,
    code: &'static str,
    label: &'static str,
) -> Result<(), EngineError> {
    if value.is_empty() || value.trim().is_empty() {
        return Err(EngineError::invalid_input(
            code,
            format!("{label} must not be empty"),
        ));
    }
    if value.len() > max_bytes {
        return Err(EngineError::invalid_input(
            code,
            format!("{label} exceeds the maximum length"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(EngineError::invalid_input(
            code,
            format!("{label} contains an unsupported control byte"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        GraphBindingPrimitive, GraphBindingTarget, GraphDirection, GraphEdgeData, GraphEdgeType,
        GraphName, GraphNodeId, GraphProperties,
    };
    use crate::branch::BranchName;
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn graph_identifier_validation_rejects_empty_reserved_and_control_values() {
        for rejected in [
            "",
            " \t",
            "_internal",
            "bad/name",
            "bad\0name",
            "bad\u{1f}name",
        ] {
            let error = GraphName::new(rejected).expect_err("graph name rejected");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        }
        for rejected in ["", " \t", "bad\0node", "bad\u{1f}node"] {
            let error = GraphNodeId::new(rejected).expect_err("node id rejected");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        }
        for rejected in ["", " \t", "_internal", "bad\0edge", "bad\u{1f}edge"] {
            let error = GraphEdgeType::new(rejected).expect_err("edge type rejected");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        }
    }

    #[test]
    fn graph_identifier_validation_accepts_documented_boundaries() {
        GraphName::new("g".repeat(256)).expect("max graph name accepted");
        GraphNodeId::new("n".repeat(1024)).expect("max node id accepted");
        GraphEdgeType::new("e".repeat(256)).expect("max edge type accepted");

        assert_eq!(
            GraphName::new("g".repeat(257))
                .expect_err("oversize graph name rejected")
                .class(),
            EngineErrorClass::InvalidInput
        );
        assert_eq!(
            GraphNodeId::new("n".repeat(1025))
                .expect_err("oversize node id rejected")
                .class(),
            EngineErrorClass::InvalidInput
        );
        assert_eq!(
            GraphEdgeType::new("e".repeat(257))
                .expect_err("oversize edge type rejected")
                .class(),
            EngineErrorClass::InvalidInput
        );
    }

    #[test]
    fn graph_properties_require_object_values() {
        for rejected in [json!(null), json!(1), json!("x"), json!([1])] {
            let error = GraphProperties::new(rejected).expect_err("properties rejected");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
            assert_eq!(error.code(), "invalid_argument.engine.graph_properties");
        }
        GraphProperties::new(json!({"nested": {"ok": true}})).expect("object accepted");
    }

    #[test]
    fn graph_edge_weight_must_be_finite() {
        GraphEdgeData::new(1.25, None).expect("finite weight accepted");
        assert!((GraphEdgeData::default().weight() - 1.0).abs() < f64::EPSILON);
        for rejected in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = GraphEdgeData::new(rejected, None).expect_err("weight rejected");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
            assert_eq!(error.code(), "invalid_argument.engine.graph_edge_weight");
        }
    }

    #[test]
    fn graph_direction_serde_is_explicit() {
        assert_eq!(GraphDirection::default(), GraphDirection::Outgoing);
        for (value, direction) in [
            (json!("outgoing"), GraphDirection::Outgoing),
            (json!("incoming"), GraphDirection::Incoming),
            (json!("both"), GraphDirection::Both),
        ] {
            assert_eq!(
                serde_json::from_value::<GraphDirection>(value.clone()).expect("direction decodes"),
                direction
            );
            assert_eq!(
                serde_json::to_value(direction).expect("direction encodes"),
                value
            );
        }

        serde_json::from_value::<GraphDirection>(json!("sideways"))
            .expect_err("unknown direction rejected");
    }

    #[test]
    fn graph_binding_target_is_typed() {
        let target = GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            Some(BranchName::new("default").expect("branch")),
            ProductSpace::new("docs").expect("space"),
            "doc-1",
        )
        .expect("target accepted");
        assert_eq!(target.primitive(), GraphBindingPrimitive::Json);
        assert_eq!(target.branch().expect("branch").as_str(), "default");
        assert_eq!(target.space().as_str(), "docs");
        assert_eq!(target.key(), "doc-1");

        let error = GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            None,
            ProductSpace::new("docs").expect("space"),
            "bad\u{1f}key",
        )
        .expect_err("control binding key rejected");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    }
}
