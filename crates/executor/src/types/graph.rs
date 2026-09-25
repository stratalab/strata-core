use super::{Deserialize, Serialize, Value};

/// Graph neighbor traversal direction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
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

/// Explicit policy for graph facts bound to a deleted entity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GraphDeletePolicy {
    /// Delete the bound graph nodes and their incident edges.
    Cascade,
    /// Preserve the graph nodes and remove their entity bindings.
    Detach,
    /// Preserve the bindings; traversal reports the target's status.
    KeepDangling,
}

/// Product primitive kind used by graph entity bindings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
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

/// Typed product identity attached to a graph node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphBindingTarget {
    primitive: GraphBindingPrimitive,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    branch: Option<String>,
    space: String,
    key: String,
}

impl GraphBindingTarget {
    /// Creates a graph binding target.
    pub fn new(
        primitive: GraphBindingPrimitive,
        branch: Option<String>,
        space: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            primitive,
            branch,
            space: space.into(),
            key: key.into(),
        }
    }

    /// Returns the primitive kind.
    pub const fn primitive(&self) -> GraphBindingPrimitive {
        self.primitive
    }

    /// Returns the optional target branch.
    pub fn branch(&self) -> Option<&str> {
        self.branch.as_deref()
    }

    /// Returns the target product space.
    pub fn space(&self) -> &str {
        &self.space
    }

    /// Returns the target key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Consumes the target.
    pub fn into_parts(self) -> (GraphBindingPrimitive, Option<String>, String, String) {
        (self.primitive, self.branch, self.space, self.key)
    }
}

/// Node-to-entity binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphEntityBinding {
    target: GraphBindingTarget,
}

impl GraphEntityBinding {
    /// Creates a graph entity binding.
    pub const fn new(target: GraphBindingTarget) -> Self {
        Self { target }
    }

    /// Returns the bound target.
    pub const fn target(&self) -> &GraphBindingTarget {
        &self.target
    }

    /// Consumes the binding.
    pub fn into_target(self) -> GraphBindingTarget {
        self.target
    }
}

/// Graph node input payload.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphNodeData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    properties: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binding: Option<GraphEntityBinding>,
    /// Optional declared object type (validated against a frozen ontology).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_type: Option<String>,
}

impl GraphNodeData {
    /// Creates graph node data (untyped; see [`Self::with_object_type`]).
    pub const fn new(properties: Option<Value>, binding: Option<GraphEntityBinding>) -> Self {
        Self {
            properties,
            binding,
            object_type: None,
        }
    }

    /// Declares the node's object type.
    #[must_use]
    pub fn with_object_type(mut self, object_type: impl Into<String>) -> Self {
        self.object_type = Some(object_type.into());
        self
    }

    /// Returns optional node properties.
    pub const fn properties(&self) -> Option<&Value> {
        self.properties.as_ref()
    }

    /// Returns optional entity binding.
    pub const fn binding(&self) -> Option<&GraphEntityBinding> {
        self.binding.as_ref()
    }

    /// Returns the declared object type.
    pub fn object_type(&self) -> Option<&str> {
        self.object_type.as_deref()
    }

    /// Consumes the payload.
    pub fn into_parts(self) -> (Option<Value>, Option<GraphEntityBinding>, Option<String>) {
        (self.properties, self.binding, self.object_type)
    }
}

/// Graph edge input payload.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphEdgeData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    weight: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    properties: Option<Value>,
}

impl GraphEdgeData {
    /// Creates graph edge data.
    pub const fn new(weight: Option<f64>, properties: Option<Value>) -> Self {
        Self { weight, properties }
    }

    /// Returns optional edge weight.
    pub const fn weight(&self) -> Option<f64> {
        self.weight
    }

    /// Returns optional edge properties.
    pub const fn properties(&self) -> Option<&Value> {
        self.properties.as_ref()
    }

    /// Consumes the payload.
    pub fn into_parts(self) -> (Option<f64>, Option<Value>) {
        (self.weight, self.properties)
    }
}

/// One graph batch write operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case", tag = "type", deny_unknown_fields)]
pub enum GraphBatchOperation {
    /// Upserts one node.
    UpsertNode {
        /// Node id.
        node_id: String,
        /// Node payload.
        data: GraphNodeData,
    },
    /// Deletes one node and incident edges.
    DeleteNode {
        /// Node id.
        node_id: String,
    },
    /// Upserts one edge.
    UpsertEdge {
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
        /// Edge payload.
        data: GraphEdgeData,
    },
    /// Deletes one edge.
    DeleteEdge {
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
    },
}

/// Serializable graph metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphInfoData {
    graph: String,
    node_count: u64,
    edge_count: u64,
    created_version: u64,
    created_timestamp: u64,
    updated_version: u64,
    updated_timestamp: u64,
    /// Whether a `graph_bulk_insert` spanning more than one commit began
    /// and has not finished: its first commit sets this, its last clears
    /// it, so an interrupted import leaves it set until the same payload
    /// is run again (#3464).
    import_pending: bool,
}

impl GraphInfoData {
    /// Creates graph metadata output.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        graph: String,
        node_count: u64,
        edge_count: u64,
        created_version: u64,
        created_timestamp: u64,
        updated_version: u64,
        updated_timestamp: u64,
        import_pending: bool,
    ) -> Self {
        Self {
            graph,
            node_count,
            edge_count,
            created_version,
            created_timestamp,
            updated_version,
            updated_timestamp,
            import_pending,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Whether a multi-commit bulk import began and has not finished.
    pub const fn import_pending(&self) -> bool {
        self.import_pending
    }

    /// Returns visible node count.
    pub const fn node_count(&self) -> u64 {
        self.node_count
    }

    /// Returns visible edge count.
    pub const fn edge_count(&self) -> u64 {
        self.edge_count
    }

    /// Returns metadata creation version.
    pub const fn created_version(&self) -> u64 {
        self.created_version
    }

    /// Returns metadata creation timestamp.
    pub const fn created_timestamp(&self) -> u64 {
        self.created_timestamp
    }

    /// Returns latest graph-state update version.
    pub const fn updated_version(&self) -> u64 {
        self.updated_version
    }

    /// Returns latest graph-state update timestamp.
    pub const fn updated_timestamp(&self) -> u64 {
        self.updated_timestamp
    }
}

/// Serializable graph node output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphNodeDataOutput {
    graph: String,
    node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    properties: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binding: Option<GraphEntityBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_type: Option<String>,
    version: u64,
    timestamp: u64,
}

impl GraphNodeDataOutput {
    /// Creates graph node output.
    pub const fn new(
        graph: String,
        node_id: String,
        properties: Option<Value>,
        binding: Option<GraphEntityBinding>,
        object_type: Option<String>,
        version: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            graph,
            node_id,
            properties,
            binding,
            object_type,
            version,
            timestamp,
        }
    }

    /// Returns the declared object type.
    pub fn object_type(&self) -> Option<&str> {
        self.object_type.as_deref()
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the node id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Returns optional properties.
    pub const fn properties(&self) -> Option<&Value> {
        self.properties.as_ref()
    }

    /// Returns optional entity binding.
    pub const fn binding(&self) -> Option<&GraphEntityBinding> {
        self.binding.as_ref()
    }

    /// Returns commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Serializable graph edge output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphEdgeDataOutput {
    graph: String,
    src: String,
    edge_type: String,
    dst: String,
    weight: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    properties: Option<Value>,
    version: u64,
    timestamp: u64,
}

impl GraphEdgeDataOutput {
    /// Creates graph edge output.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        graph: String,
        src: String,
        edge_type: String,
        dst: String,
        weight: f64,
        properties: Option<Value>,
        version: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            graph,
            src,
            edge_type,
            dst,
            weight,
            properties,
            version,
            timestamp,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the source node id.
    pub fn src(&self) -> &str {
        &self.src
    }

    /// Returns the edge type.
    pub fn edge_type(&self) -> &str {
        &self.edge_type
    }

    /// Returns the destination node id.
    pub fn dst(&self) -> &str {
        &self.dst
    }

    /// Returns the edge weight.
    pub const fn weight(&self) -> f64 {
        self.weight
    }

    /// Returns optional properties.
    pub const fn properties(&self) -> Option<&Value> {
        self.properties.as_ref()
    }

    /// Returns commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Serializable graph neighbor hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphNeighborHit {
    graph: String,
    node_id: String,
    src: String,
    edge_type: String,
    dst: String,
    direction: GraphDirection,
    node: GraphNodeDataOutput,
    edge: GraphEdgeDataOutput,
    /// Bound-entity resolution status; absent when the node is unbound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_status: Option<String>,
}

impl GraphNeighborHit {
    /// Creates a graph neighbor hit.
    pub fn new(
        node: GraphNodeDataOutput,
        edge: GraphEdgeDataOutput,
        direction: GraphDirection,
        target_status: Option<String>,
    ) -> Self {
        debug_assert_eq!(node.graph(), edge.graph());
        Self {
            graph: node.graph().to_owned(),
            node_id: node.node_id().to_owned(),
            src: edge.src().to_owned(),
            edge_type: edge.edge_type().to_owned(),
            dst: edge.dst().to_owned(),
            direction,
            node,
            edge,
            target_status,
        }
    }

    /// Returns the bound-entity resolution status, when the node is bound.
    pub fn target_status(&self) -> Option<&str> {
        self.target_status.as_deref()
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the neighboring node id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Returns the connecting edge source node id.
    pub fn src(&self) -> &str {
        &self.src
    }

    /// Returns the connecting edge type.
    pub fn edge_type(&self) -> &str {
        &self.edge_type
    }

    /// Returns the connecting edge destination node id.
    pub fn dst(&self) -> &str {
        &self.dst
    }

    /// Returns the neighboring node.
    pub const fn node(&self) -> &GraphNodeDataOutput {
        &self.node
    }

    /// Returns the connecting edge.
    pub const fn edge(&self) -> &GraphEdgeDataOutput {
        &self.edge
    }

    /// Returns which direction produced the hit.
    pub const fn direction(&self) -> GraphDirection {
        self.direction
    }
}

/// Serializable graph entity binding hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphBindingHit {
    graph: String,
    node_id: String,
    binding: GraphEntityBinding,
    version: u64,
    timestamp: u64,
}

impl GraphBindingHit {
    /// Creates a graph binding hit.
    pub const fn new(
        graph: String,
        node_id: String,
        binding: GraphEntityBinding,
        version: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            graph,
            node_id,
            binding,
            version,
            timestamp,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the node id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Returns the entity binding.
    pub const fn binding(&self) -> &GraphEntityBinding {
        &self.binding
    }

    /// Returns commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Positional graph batch write result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status, mutation
/// effect, commit receipt, and error; this payload carries the graph-specific
/// operation identity and the create/delete facts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphBatchItemResult {
    operation_index: u64,
    operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deleted: Option<bool>,
}

impl GraphBatchItemResult {
    /// Creates a graph batch item result payload.
    pub fn new(
        operation_index: u64,
        operation: impl Into<String>,
        created: Option<bool>,
        deleted: Option<bool>,
    ) -> Self {
        Self {
            operation_index,
            operation: operation.into(),
            created,
            deleted,
        }
    }

    /// Returns the input operation index.
    pub const fn operation_index(&self) -> u64 {
        self.operation_index
    }

    /// Returns the operation kind.
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Returns create/update fact.
    pub const fn created(&self) -> Option<bool> {
        self.created
    }

    /// Returns delete/no-op fact.
    pub const fn deleted(&self) -> Option<bool> {
        self.deleted
    }
}

/// One declared property on a graph object or link type (wire form).
/// `value_type` is a recorded hint, not an enforced constraint.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphPropertyDef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value_type: Option<String>,
    #[serde(default)]
    required: bool,
}

impl GraphPropertyDef {
    /// Creates a property definition.
    pub const fn new(value_type: Option<String>, required: bool) -> Self {
        Self {
            value_type,
            required,
        }
    }

    /// Returns the recorded value-type hint.
    pub fn value_type(&self) -> Option<&str> {
        self.value_type.as_deref()
    }

    /// Returns whether the property is required once the ontology freezes.
    pub const fn required(&self) -> bool {
        self.required
    }

    /// Consumes the definition.
    pub fn into_parts(self) -> (Option<String>, bool) {
        (self.value_type, self.required)
    }
}

/// A declared object type (wire form).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphObjectTypeDefData {
    name: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    properties: std::collections::BTreeMap<String, GraphPropertyDef>,
}

impl GraphObjectTypeDefData {
    /// Creates an object type definition.
    pub const fn new(
        name: String,
        properties: std::collections::BTreeMap<String, GraphPropertyDef>,
    ) -> Self {
        Self { name, properties }
    }

    /// Returns the type name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the declared properties, ordered by name.
    pub const fn properties(&self) -> &std::collections::BTreeMap<String, GraphPropertyDef> {
        &self.properties
    }
}

/// A declared link type (wire form). `cardinality` is a recorded hint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphLinkTypeDefData {
    name: String,
    source: String,
    target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cardinality: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    properties: std::collections::BTreeMap<String, GraphPropertyDef>,
}

impl GraphLinkTypeDefData {
    /// Creates a link type definition.
    pub const fn new(
        name: String,
        source: String,
        target: String,
        cardinality: Option<String>,
        properties: std::collections::BTreeMap<String, GraphPropertyDef>,
    ) -> Self {
        Self {
            name,
            source,
            target,
            cardinality,
            properties,
        }
    }

    /// Returns the type name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the declared source object type.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the declared target object type.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the recorded cardinality hint.
    pub fn cardinality(&self) -> Option<&str> {
        self.cardinality.as_deref()
    }

    /// Returns the declared properties, ordered by name.
    pub const fn properties(&self) -> &std::collections::BTreeMap<String, GraphPropertyDef> {
        &self.properties
    }
}

/// A graph's ontology: status plus every declared type (wire form).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphOntologyData {
    graph: String,
    status: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    object_types: Vec<GraphObjectTypeDefData>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    link_types: Vec<GraphLinkTypeDefData>,
    version: u64,
    timestamp: u64,
}

impl GraphOntologyData {
    /// Creates ontology output data.
    pub const fn new(
        graph: String,
        status: String,
        object_types: Vec<GraphObjectTypeDefData>,
        link_types: Vec<GraphLinkTypeDefData>,
        version: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            graph,
            status,
            object_types,
            link_types,
            version,
            timestamp,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the lifecycle status (`draft` or `frozen`).
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Returns declared object types, ordered by name.
    pub fn object_types(&self) -> &[GraphObjectTypeDefData] {
        &self.object_types
    }

    /// Returns declared link types, ordered by name.
    pub fn link_types(&self) -> &[GraphLinkTypeDefData] {
        &self.link_types
    }

    /// Returns the commit version of the visible ontology row.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp of the visible ontology row.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// One object type with its visible node count (wire form).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphObjectTypeSummaryData {
    #[serde(flatten)]
    def: GraphObjectTypeDefData,
    node_count: u64,
}

impl GraphObjectTypeSummaryData {
    /// Creates an object type summary.
    pub const fn new(def: GraphObjectTypeDefData, node_count: u64) -> Self {
        Self { def, node_count }
    }

    /// Returns the type definition.
    pub const fn def(&self) -> &GraphObjectTypeDefData {
        &self.def
    }

    /// Returns the number of visible nodes declaring this type.
    pub const fn node_count(&self) -> u64 {
        self.node_count
    }
}

/// One link type with its visible edge count (wire form).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphLinkTypeSummaryData {
    #[serde(flatten)]
    def: GraphLinkTypeDefData,
    edge_count: u64,
}

impl GraphLinkTypeSummaryData {
    /// Creates a link type summary.
    pub const fn new(def: GraphLinkTypeDefData, edge_count: u64) -> Self {
        Self { def, edge_count }
    }

    /// Returns the type definition.
    pub const fn def(&self) -> &GraphLinkTypeDefData {
        &self.def
    }

    /// Returns the number of visible edges carrying this type.
    pub const fn edge_count(&self) -> u64 {
        self.edge_count
    }
}

/// The ontology with per-type usage counts (wire form).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphOntologySummaryData {
    graph: String,
    status: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    object_types: Vec<GraphObjectTypeSummaryData>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    link_types: Vec<GraphLinkTypeSummaryData>,
    version: u64,
    timestamp: u64,
}

impl GraphOntologySummaryData {
    /// Creates ontology summary output data.
    pub const fn new(
        graph: String,
        status: String,
        object_types: Vec<GraphObjectTypeSummaryData>,
        link_types: Vec<GraphLinkTypeSummaryData>,
        version: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            graph,
            status,
            object_types,
            link_types,
            version,
            timestamp,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the lifecycle status (`draft` or `frozen`).
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Returns object type summaries, ordered by name.
    pub fn object_types(&self) -> &[GraphObjectTypeSummaryData] {
        &self.object_types
    }

    /// Returns link type summaries, ordered by name.
    pub fn link_types(&self) -> &[GraphLinkTypeSummaryData] {
        &self.link_types
    }

    /// Returns the commit version of the visible ontology row.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp of the visible ontology row.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Optional size bounds for one graph analytics snapshot (input form).
/// Unset fields use the engine defaults.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GraphAnalyticsBudget {
    /// Maximum node count admitted into the snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_nodes: Option<u64>,
    /// Maximum edge count admitted into the snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_edges: Option<u64>,
}

impl GraphAnalyticsBudget {
    /// Creates explicit snapshot bounds.
    #[must_use]
    pub const fn new(max_nodes: Option<u64>, max_edges: Option<u64>) -> Self {
        Self {
            max_nodes,
            max_edges,
        }
    }

    /// Returns the node bound, when set.
    pub const fn max_nodes(&self) -> Option<u64> {
        self.max_nodes
    }

    /// Returns the edge bound, when set.
    pub const fn max_edges(&self) -> Option<u64> {
        self.max_edges
    }
}

/// Weakly-connected-components result (wire form). Every node maps to
/// its component representative: the smallest node id in the component.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphWccData {
    graph: String,
    components: std::collections::BTreeMap<String, String>,
    component_count: u64,
}

impl GraphWccData {
    /// Creates component output data.
    pub const fn new(
        graph: String,
        components: std::collections::BTreeMap<String, String>,
        component_count: u64,
    ) -> Self {
        Self {
            graph,
            components,
            component_count,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns each node's component representative, keyed by node id.
    pub const fn components(&self) -> &std::collections::BTreeMap<String, String> {
        &self.components
    }

    /// Returns the number of distinct components.
    pub const fn component_count(&self) -> u64 {
        self.component_count
    }
}

/// Local-clustering-coefficient result (wire form).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphLccData {
    graph: String,
    coefficients: std::collections::BTreeMap<String, f64>,
}

impl GraphLccData {
    /// Creates coefficient output data.
    pub const fn new(graph: String, coefficients: std::collections::BTreeMap<String, f64>) -> Self {
        Self {
            graph,
            coefficients,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the clustering coefficient per node id.
    pub const fn coefficients(&self) -> &std::collections::BTreeMap<String, f64> {
        &self.coefficients
    }
}

/// Shortest-path result (wire form). Unreachable nodes are omitted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphSsspData {
    graph: String,
    source: String,
    direction: GraphDirection,
    distances: std::collections::BTreeMap<String, f64>,
    /// The node each reachable node's cheapest walk arrived from; the source
    /// has no entry. Following it back to the source unpacks the path.
    predecessors: std::collections::BTreeMap<String, String>,
}

impl GraphSsspData {
    /// Creates shortest-path output data.
    pub const fn new(
        graph: String,
        source: String,
        direction: GraphDirection,
        distances: std::collections::BTreeMap<String, f64>,
        predecessors: std::collections::BTreeMap<String, String>,
    ) -> Self {
        Self {
            graph,
            source,
            direction,
            distances,
            predecessors,
        }
    }

    /// Returns the predecessor per reachable node id (the source has none).
    pub const fn predecessors(&self) -> &std::collections::BTreeMap<String, String> {
        &self.predecessors
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the source node id.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the traversal direction.
    pub const fn direction(&self) -> GraphDirection {
        self.direction
    }

    /// Returns the distance per reachable node id.
    pub const fn distances(&self) -> &std::collections::BTreeMap<String, f64> {
        &self.distances
    }
}

/// `PageRank` result (wire form).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphPagerankData {
    graph: String,
    ranks: std::collections::BTreeMap<String, f64>,
    iterations: u64,
    personalized: bool,
}

impl GraphPagerankData {
    /// Creates rank output data.
    pub const fn new(
        graph: String,
        ranks: std::collections::BTreeMap<String, f64>,
        iterations: u64,
        personalized: bool,
    ) -> Self {
        Self {
            graph,
            ranks,
            iterations,
            personalized,
        }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the rank per node id.
    pub const fn ranks(&self) -> &std::collections::BTreeMap<String, f64> {
        &self.ranks
    }

    /// Returns the number of power iterations performed.
    pub const fn iterations(&self) -> u64 {
        self.iterations
    }

    /// Returns true when seed weights steered the walk.
    pub const fn personalized(&self) -> bool {
        self.personalized
    }
}

/// Community-detection result (wire form). Every node maps to its
/// community representative node id.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphCdlpData {
    graph: String,
    labels: std::collections::BTreeMap<String, String>,
}

impl GraphCdlpData {
    /// Creates community output data.
    pub const fn new(graph: String, labels: std::collections::BTreeMap<String, String>) -> Self {
        Self { graph, labels }
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns each node's community representative, keyed by node id.
    pub const fn labels(&self) -> &std::collections::BTreeMap<String, String> {
        &self.labels
    }
}

/// One traversal step recorded by a breadth-first search (wire form).
/// `src`/`dst` follow traversal order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphBfsEdgeData {
    src: String,
    dst: String,
    edge_type: String,
    weight: f64,
}

impl GraphBfsEdgeData {
    /// Creates traversal edge output data.
    pub const fn new(src: String, dst: String, edge_type: String, weight: f64) -> Self {
        Self {
            src,
            dst,
            edge_type,
            weight,
        }
    }

    /// Returns the node the traversal came from.
    pub fn src(&self) -> &str {
        &self.src
    }

    /// Returns the node the traversal reached.
    pub fn dst(&self) -> &str {
        &self.dst
    }

    /// Returns the edge type.
    pub fn edge_type(&self) -> &str {
        &self.edge_type
    }

    /// Returns the edge weight.
    pub const fn weight(&self) -> f64 {
        self.weight
    }
}

/// Breadth-first-search result (wire form).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct GraphBfsData {
    graph: String,
    start: String,
    visited: Vec<String>,
    depths: std::collections::BTreeMap<String, u64>,
    edges: Vec<GraphBfsEdgeData>,
    /// Whether a depth or node-count cap stopped the traversal before every
    /// reachable node was visited, so `visited`/`edges` are a partial result.
    truncated: bool,
}

impl GraphBfsData {
    /// Creates traversal output data.
    pub const fn new(
        graph: String,
        start: String,
        visited: Vec<String>,
        depths: std::collections::BTreeMap<String, u64>,
        edges: Vec<GraphBfsEdgeData>,
        truncated: bool,
    ) -> Self {
        Self {
            graph,
            start,
            visited,
            depths,
            edges,
            truncated,
        }
    }

    /// Whether a depth or node-count cap made this a partial traversal.
    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    /// Returns the graph name.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Returns the start node id.
    pub fn start(&self) -> &str {
        &self.start
    }

    /// Returns the visited node ids in breadth-first order.
    pub fn visited(&self) -> &[String] {
        &self.visited
    }

    /// Returns the depth per visited node id.
    pub const fn depths(&self) -> &std::collections::BTreeMap<String, u64> {
        &self.depths
    }

    /// Returns the tree edges in discovery order.
    pub fn edges(&self) -> &[GraphBfsEdgeData] {
        &self.edges
    }
}

/// One node in a bulk ingest (input form).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GraphBulkNode {
    /// Node id.
    node_id: String,
    /// Optional JSON properties.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    properties: Option<Value>,
    /// Optional entity binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binding: Option<GraphEntityBinding>,
    /// Optional declared object type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_type: Option<String>,
}

impl GraphBulkNode {
    /// Creates a bulk node entry.
    #[must_use]
    pub const fn new(
        node_id: String,
        properties: Option<Value>,
        binding: Option<GraphEntityBinding>,
        object_type: Option<String>,
    ) -> Self {
        Self {
            node_id,
            properties,
            binding,
            object_type,
        }
    }

    /// Splits the entry into its parts.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<Value>,
        Option<GraphEntityBinding>,
        Option<String>,
    ) {
        (
            self.node_id,
            self.properties,
            self.binding,
            self.object_type,
        )
    }
}

/// One edge in a bulk ingest (input form).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GraphBulkEdge {
    /// Source node id.
    src: String,
    /// Edge type.
    edge_type: String,
    /// Destination node id.
    dst: String,
    /// Optional weight. Defaults to 1.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    weight: Option<f64>,
    /// Optional JSON properties.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    properties: Option<Value>,
}

impl GraphBulkEdge {
    /// Creates a bulk edge entry.
    #[must_use]
    pub const fn new(
        src: String,
        edge_type: String,
        dst: String,
        weight: Option<f64>,
        properties: Option<Value>,
    ) -> Self {
        Self {
            src,
            edge_type,
            dst,
            weight,
            properties,
        }
    }

    /// Splits the entry into its parts.
    #[must_use]
    pub fn into_parts(self) -> (String, String, String, Option<f64>, Option<Value>) {
        (
            self.src,
            self.edge_type,
            self.dst,
            self.weight,
            self.properties,
        )
    }
}
