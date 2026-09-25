//! Graph core storage envelopes.

use serde::{Deserialize, Serialize};
use strata_core::{CommitVersion, Timestamp};

use crate::diagnostics::EngineError;

use super::{
    GraphBindingPrimitive, GraphBindingTarget, GraphEdgeData, GraphEdgeType, GraphEntityBinding,
    GraphName, GraphNodeData, GraphNodeId, GraphProperties, GraphTypeName,
};

const GRAPH_METADATA_FORMAT_VERSION: u8 = 1;
const GRAPH_NODE_FORMAT_VERSION: u8 = 1;
const GRAPH_EDGE_FORMAT_VERSION: u8 = 1;
const GRAPH_BINDING_FORMAT_VERSION: u8 = 1;
const GRAPH_TYPE_INDEX_FORMAT_VERSION: u8 = 1;

/// Stored graph metadata.
///
/// #3474: the row carries the graph's live node and edge counts and its
/// create commit, and every commit that changes a node or edge rewrites it
/// in the same batch — so the row's own commit is the graph's last change
/// and `graph_info` is one point read. A row written before counts were
/// kept decodes with `counts: None` (and `created: None`, meaning the row's
/// own commit); the graph's next write backfills it from a scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphMetadataRecord {
    graph: GraphName,
    created: Option<GraphCommitPoint>,
    counts: Option<GraphCounts>,
    /// #3477: set by the first commit of a deletion too large for one
    /// commit. From that commit on the graph is absent to every read and
    /// write while its rows are swept in further commits; the row itself
    /// is tombstoned last. A row still carrying the mark after a crash is
    /// a deletion to resume, which `delete_graph` and `create_graph` do.
    deleting: bool,
    /// #3464: set by the first chunk commit of a `bulk_insert` that spans
    /// more than one commit and cleared by its last, so a crash between
    /// them leaves a durable watermark — `graph_info` reports the import
    /// pending — and a re-run of the same payload (idempotent upserts)
    /// clears it. The graph stays readable and writable throughout.
    importing: bool,
}

impl GraphMetadataRecord {
    /// A freshly created graph: no nodes, no edges, created by the commit
    /// that writes this row.
    pub(crate) const fn new(graph: GraphName) -> Self {
        Self {
            graph,
            created: None,
            counts: Some(GraphCounts::new(0, 0)),
            deleting: false,
            importing: false,
        }
    }

    /// A rewritten row: the create commit it must keep, the counts after
    /// the commit that writes it, and whether a bulk import is pending
    /// after it.
    pub(crate) const fn with_state(
        graph: GraphName,
        created: GraphCommitPoint,
        counts: GraphCounts,
        importing: bool,
    ) -> Self {
        Self {
            graph,
            created: Some(created),
            counts: Some(counts),
            deleting: false,
            importing,
        }
    }

    /// Whether a multi-commit bulk import has begun and not yet finished.
    pub(crate) const fn importing(&self) -> bool {
        self.importing
    }

    /// The same row, marked as a deletion in progress.
    pub(crate) const fn marked_deleting(mut self) -> Self {
        self.deleting = true;
        self
    }

    pub(crate) const fn graph(&self) -> &GraphName {
        &self.graph
    }

    /// Whether a deletion of this graph has begun and not yet swept its
    /// rows; such a graph is absent to readers and writers.
    pub(crate) const fn deleting(&self) -> bool {
        self.deleting
    }

    /// The create commit, when the row has been rewritten since; `None`
    /// means the row's own commit created the graph.
    pub(crate) const fn created(&self) -> Option<GraphCommitPoint> {
        self.created
    }

    /// The maintained counts; `None` on a row written before #3474.
    pub(crate) const fn counts(&self) -> Option<GraphCounts> {
        self.counts
    }
}

/// A commit's coordinates, kept on a rewritten metadata row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GraphCommitPoint {
    version: CommitVersion,
    timestamp: Timestamp,
}

impl GraphCommitPoint {
    pub(crate) const fn new(version: CommitVersion, timestamp: Timestamp) -> Self {
        Self { version, timestamp }
    }

    pub(crate) const fn version(self) -> CommitVersion {
        self.version
    }

    pub(crate) const fn timestamp(self) -> Timestamp {
        self.timestamp
    }
}

/// Live node and edge counts of a graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GraphCounts {
    nodes: u64,
    edges: u64,
}

impl GraphCounts {
    pub(crate) const fn new(nodes: u64, edges: u64) -> Self {
        Self { nodes, edges }
    }

    pub(crate) const fn nodes(self) -> u64 {
        self.nodes
    }

    pub(crate) const fn edges(self) -> u64 {
        self.edges
    }

    /// The counts after a commit that adds (or, negative, removes) live
    /// rows. A count that would go below zero means the row and the rows
    /// it describes disagree, which is corruption, not a clamp.
    pub(crate) fn adjusted(self, node_delta: i64, edge_delta: i64) -> Result<Self, EngineError> {
        let underflow = |what: &str| {
            EngineError::corruption(
                "data_loss.engine.graph_metadata",
                format!("stored graph metadata counts fewer {what} than this commit removes"),
            )
        };
        Ok(Self {
            nodes: self
                .nodes
                .checked_add_signed(node_delta)
                .ok_or_else(|| underflow("nodes"))?,
            edges: self
                .edges
                .checked_add_signed(edge_delta)
                .ok_or_else(|| underflow("edges"))?,
        })
    }
}

/// Stored node record.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphNodeRecord {
    graph: GraphName,
    node_id: GraphNodeId,
    data: GraphNodeData,
}

impl GraphNodeRecord {
    pub(crate) const fn new(graph: GraphName, node_id: GraphNodeId, data: GraphNodeData) -> Self {
        Self {
            graph,
            node_id,
            data,
        }
    }

    pub(crate) const fn graph(&self) -> &GraphName {
        &self.graph
    }

    pub(crate) const fn node_id(&self) -> &GraphNodeId {
        &self.node_id
    }

    pub(crate) const fn data(&self) -> &GraphNodeData {
        &self.data
    }
}

/// Stored forward or reverse edge record.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphEdgeRecord {
    graph: GraphName,
    src: GraphNodeId,
    edge_type: GraphEdgeType,
    dst: GraphNodeId,
    data: GraphEdgeData,
}

impl GraphEdgeRecord {
    pub(crate) const fn new(
        graph: GraphName,
        src: GraphNodeId,
        edge_type: GraphEdgeType,
        dst: GraphNodeId,
        data: GraphEdgeData,
    ) -> Self {
        Self {
            graph,
            src,
            edge_type,
            dst,
            data,
        }
    }

    pub(crate) const fn graph(&self) -> &GraphName {
        &self.graph
    }

    pub(crate) const fn src(&self) -> &GraphNodeId {
        &self.src
    }

    pub(crate) const fn edge_type(&self) -> &GraphEdgeType {
        &self.edge_type
    }

    pub(crate) const fn dst(&self) -> &GraphNodeId {
        &self.dst
    }

    pub(crate) const fn data(&self) -> &GraphEdgeData {
        &self.data
    }
}

/// Stored binding index record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphBindingRecord {
    graph: GraphName,
    node_id: GraphNodeId,
    binding: GraphEntityBinding,
}

impl GraphBindingRecord {
    pub(crate) const fn new(
        graph: GraphName,
        node_id: GraphNodeId,
        binding: GraphEntityBinding,
    ) -> Self {
        Self {
            graph,
            node_id,
            binding,
        }
    }

    pub(crate) const fn graph(&self) -> &GraphName {
        &self.graph
    }

    pub(crate) const fn node_id(&self) -> &GraphNodeId {
        &self.node_id
    }

    pub(crate) const fn binding(&self) -> &GraphEntityBinding {
        &self.binding
    }
}

/// Stored node-type index record (derived row, GO3): the key carries the
/// full identity; the value revalidates it on decode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphTypeIndexRecord {
    graph: GraphName,
    object_type: GraphTypeName,
    node_id: GraphNodeId,
}

impl GraphTypeIndexRecord {
    pub(crate) const fn new(
        graph: GraphName,
        object_type: GraphTypeName,
        node_id: GraphNodeId,
    ) -> Self {
        Self {
            graph,
            object_type,
            node_id,
        }
    }

    pub(crate) const fn graph(&self) -> &GraphName {
        &self.graph
    }

    pub(crate) const fn object_type(&self) -> &GraphTypeName {
        &self.object_type
    }

    pub(crate) const fn node_id(&self) -> &GraphNodeId {
        &self.node_id
    }
}

#[derive(Serialize, Deserialize)]
struct StoredGraphTypeIndex {
    graph: String,
    object_type: String,
    node_id: String,
}

pub(crate) fn encode_graph_type_index_record(record: &GraphTypeIndexRecord) -> Vec<u8> {
    let stored = StoredGraphTypeIndex {
        graph: record.graph().as_str().to_owned(),
        object_type: record.object_type().as_str().to_owned(),
        node_id: record.node_id().as_str().to_owned(),
    };
    encode_json_record(
        GRAPH_TYPE_INDEX_FORMAT_VERSION,
        &stored,
        "graph type index record cannot be encoded",
    )
}

pub(crate) fn decode_graph_type_index_record(
    expected_graph: &GraphName,
    expected_object_type: &GraphTypeName,
    expected_node_id: &GraphNodeId,
    bytes: &[u8],
) -> Result<GraphTypeIndexRecord, EngineError> {
    let corruption = |detail: &str| {
        EngineError::corruption(
            "data_loss.engine.graph_type_index_record",
            format!("stored graph type index record {detail}"),
        )
    };
    if bytes.first().copied() != Some(GRAPH_TYPE_INDEX_FORMAT_VERSION) {
        return Err(corruption("has an unknown format version"));
    }
    let stored = serde_json::from_slice::<StoredGraphTypeIndex>(&bytes[1..])
        .map_err(|error| corruption(&format!("cannot be decoded: {error}")))?;
    let graph =
        GraphName::new(stored.graph).map_err(|_| corruption("contains an invalid graph name"))?;
    let object_type = GraphTypeName::new(stored.object_type)
        .map_err(|_| corruption("contains an invalid object type"))?;
    let node_id =
        GraphNodeId::new(stored.node_id).map_err(|_| corruption("contains an invalid node id"))?;
    if &graph != expected_graph
        || &object_type != expected_object_type
        || &node_id != expected_node_id
    {
        return Err(corruption("identity does not match its row key"));
    }
    Ok(GraphTypeIndexRecord::new(graph, object_type, node_id))
}

#[derive(Serialize, Deserialize)]
struct StoredGraphMetadata {
    graph: String,
    /// Absent on a row that is still the create commit's own (and on every
    /// row written before #3474).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created: Option<StoredCommitPoint>,
    /// Absent on a row written before #3474; such a graph is counted by
    /// scan until its next write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    counts: Option<StoredGraphCounts>,
    /// Present only while a chunked deletion is sweeping the graph's rows
    /// (#3477); absent on every other row, so older readers see nothing new.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    deleting: bool,
    /// Present only between the first and last commit of a multi-commit
    /// bulk import (#3464); absent everywhere else.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    importing: bool,
}

#[derive(Serialize, Deserialize)]
struct StoredCommitPoint {
    version: u64,
    timestamp: u64,
}

#[derive(Serialize, Deserialize)]
struct StoredGraphCounts {
    nodes: u64,
    edges: u64,
}

#[derive(Serialize, Deserialize)]
struct StoredGraphNode {
    graph: String,
    node_id: String,
    properties: Option<serde_json::Value>,
    binding: Option<StoredGraphBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_type: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct StoredGraphEdge {
    graph: String,
    src: String,
    edge_type: String,
    dst: String,
    weight: f64,
    properties: Option<serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredGraphBinding {
    target: StoredGraphBindingTarget,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredGraphBindingTarget {
    primitive: GraphBindingPrimitive,
    branch: Option<String>,
    space: String,
    key: String,
}

pub(crate) fn encode_graph_metadata_record(record: &GraphMetadataRecord) -> Vec<u8> {
    let stored = StoredGraphMetadata {
        graph: record.graph().as_str().to_owned(),
        created: record.created().map(|point| StoredCommitPoint {
            version: point.version().as_u64(),
            timestamp: point.timestamp().as_micros(),
        }),
        counts: record.counts().map(|counts| StoredGraphCounts {
            nodes: counts.nodes(),
            edges: counts.edges(),
        }),
        deleting: record.deleting(),
        importing: record.importing(),
    };
    encode_json_record(
        GRAPH_METADATA_FORMAT_VERSION,
        &stored,
        "graph metadata cannot be encoded",
    )
}

/// The pre-#3474 row body — the graph name alone — so a test can hold the
/// scan fallback and the backfill on the next write to the maintained row.
#[cfg(any(test, feature = "testkit"))]
pub(crate) fn encode_legacy_graph_metadata_record_for_test(graph: &GraphName) -> Vec<u8> {
    encode_json_record(
        GRAPH_METADATA_FORMAT_VERSION,
        &StoredGraphMetadata {
            graph: graph.as_str().to_owned(),
            created: None,
            counts: None,
            deleting: false,
            importing: false,
        },
        "graph metadata cannot be encoded",
    )
}

pub(crate) fn decode_graph_metadata_record(
    expected_graph: &GraphName,
    bytes: &[u8],
) -> Result<GraphMetadataRecord, EngineError> {
    if bytes.first().copied() != Some(GRAPH_METADATA_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_metadata",
            "stored graph metadata has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredGraphMetadata>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.graph_metadata",
            format!("stored graph metadata cannot be decoded: {error}"),
        )
    })?;
    let graph = GraphName::new(stored.graph).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_metadata",
            "stored graph name is invalid",
        )
    })?;
    if &graph != expected_graph {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_metadata",
            "stored graph metadata identity does not match its row key",
        ));
    }
    Ok(GraphMetadataRecord {
        graph,
        created: stored.created.map(|point| {
            GraphCommitPoint::new(
                CommitVersion::new(point.version),
                Timestamp::from_micros(point.timestamp),
            )
        }),
        counts: stored
            .counts
            .map(|counts| GraphCounts::new(counts.nodes, counts.edges)),
        deleting: stored.deleting,
        importing: stored.importing,
    })
}

pub(crate) fn encode_graph_node_record(record: &GraphNodeRecord) -> Vec<u8> {
    let stored = StoredGraphNode {
        graph: record.graph().as_str().to_owned(),
        node_id: record.node_id().as_str().to_owned(),
        properties: record.data().properties().map(GraphProperties::clone_inner),
        binding: record.data().binding().map(binding_to_stored),
        object_type: record
            .data()
            .object_type()
            .map(|object_type| object_type.as_str().to_owned()),
    };
    encode_json_record(
        GRAPH_NODE_FORMAT_VERSION,
        &stored,
        "graph node record cannot be encoded",
    )
}

pub(crate) fn decode_graph_node_record(
    expected_graph: &GraphName,
    expected_node_id: &GraphNodeId,
    bytes: &[u8],
) -> Result<GraphNodeRecord, EngineError> {
    if bytes.first().copied() != Some(GRAPH_NODE_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_node_record",
            "stored graph node record has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredGraphNode>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.graph_node_record",
            format!("stored graph node record cannot be decoded: {error}"),
        )
    })?;
    let graph = GraphName::new(stored.graph).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_node_record",
            "stored graph node record graph name is invalid",
        )
    })?;
    let node_id = GraphNodeId::new(stored.node_id).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_node_record",
            "stored graph node record node id is invalid",
        )
    })?;
    if &graph != expected_graph || &node_id != expected_node_id {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_node_record",
            "stored graph node identity does not match its row key",
        ));
    }
    let properties = stored
        .properties
        .map(GraphProperties::new)
        .transpose()
        .map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_node_record",
                "stored graph node properties violate engine limits",
            )
        })?;
    let binding = stored
        .binding
        .map(binding_from_stored)
        .transpose()
        .map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_node_record",
                "stored graph node binding is invalid",
            )
        })?;
    let mut data = GraphNodeData::new(properties, binding);
    if let Some(object_type) = stored.object_type {
        data = data.with_object_type(crate::data::graph::GraphTypeName::new(object_type).map_err(
            |_| {
                EngineError::corruption(
                    "data_loss.engine.graph_node_record",
                    "stored graph node object type is invalid",
                )
            },
        )?);
    }
    Ok(GraphNodeRecord::new(graph, node_id, data))
}

pub(crate) fn encode_graph_edge_record(record: &GraphEdgeRecord) -> Vec<u8> {
    let stored = StoredGraphEdge {
        graph: record.graph().as_str().to_owned(),
        src: record.src().as_str().to_owned(),
        edge_type: record.edge_type().as_str().to_owned(),
        dst: record.dst().as_str().to_owned(),
        weight: record.data().weight(),
        properties: record.data().properties().map(GraphProperties::clone_inner),
    };
    encode_json_record(
        GRAPH_EDGE_FORMAT_VERSION,
        &stored,
        "graph edge record cannot be encoded",
    )
}

pub(crate) fn decode_graph_edge_record(
    expected_graph: &GraphName,
    expected_src: &GraphNodeId,
    expected_edge_type: &GraphEdgeType,
    expected_dst: &GraphNodeId,
    bytes: &[u8],
) -> Result<GraphEdgeRecord, EngineError> {
    if bytes.first().copied() != Some(GRAPH_EDGE_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            "stored graph edge record has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredGraphEdge>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            format!("stored graph edge record cannot be decoded: {error}"),
        )
    })?;
    let graph = GraphName::new(stored.graph).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            "stored graph edge record graph name is invalid",
        )
    })?;
    let src = GraphNodeId::new(stored.src).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            "stored graph edge record source id is invalid",
        )
    })?;
    let edge_type = GraphEdgeType::new(stored.edge_type).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            "stored graph edge record type is invalid",
        )
    })?;
    let dst = GraphNodeId::new(stored.dst).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            "stored graph edge record destination id is invalid",
        )
    })?;
    if &graph != expected_graph
        || &src != expected_src
        || &edge_type != expected_edge_type
        || &dst != expected_dst
    {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            "stored graph edge identity does not match its row key",
        ));
    }
    let properties = stored
        .properties
        .map(GraphProperties::new)
        .transpose()
        .map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_edge_record",
                "stored graph edge properties violate engine limits",
            )
        })?;
    let data = GraphEdgeData::new(stored.weight, properties).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_edge_record",
            "stored graph edge weight violates engine limits",
        )
    })?;
    Ok(GraphEdgeRecord::new(graph, src, edge_type, dst, data))
}

pub(crate) fn encode_graph_binding_record(record: &GraphBindingRecord) -> Vec<u8> {
    let stored = StoredGraphNode {
        graph: record.graph().as_str().to_owned(),
        node_id: record.node_id().as_str().to_owned(),
        properties: None,
        binding: Some(binding_to_stored(record.binding())),
        object_type: None,
    };
    encode_json_record(
        GRAPH_BINDING_FORMAT_VERSION,
        &stored,
        "graph binding record cannot be encoded",
    )
}

pub(crate) fn decode_graph_binding_record(
    expected_graph: &GraphName,
    expected_node_id: &GraphNodeId,
    bytes: &[u8],
) -> Result<GraphBindingRecord, EngineError> {
    if bytes.first().copied() != Some(GRAPH_BINDING_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding record has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredGraphNode>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            format!("stored graph binding record cannot be decoded: {error}"),
        )
    })?;
    let graph = GraphName::new(stored.graph).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding graph name is invalid",
        )
    })?;
    let node_id = GraphNodeId::new(stored.node_id).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding node id is invalid",
        )
    })?;
    if &graph != expected_graph || &node_id != expected_node_id {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding identity does not match its row key",
        ));
    }
    let binding = stored.binding.ok_or_else(|| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding record is missing binding data",
        )
    })?;
    Ok(GraphBindingRecord::new(
        graph,
        node_id,
        binding_from_stored(binding)?,
    ))
}

/// Encodes a validated record. #2651: `serde_json::to_vec` of a plain
/// `#[derive(Serialize)]` value built from already-validated fields cannot fail,
/// so this `.expect()`s rather than surfacing an unreachable
/// `invalid_argument.engine.graph_*_record` code. The fallible mirror is the
/// decode side (`data_loss.engine.graph_*`).
fn encode_json_record<T: Serialize>(version: u8, value: &T, message: &'static str) -> Vec<u8> {
    let mut bytes = vec![version];
    bytes.extend(serde_json::to_vec(value).expect(message));
    bytes
}

fn binding_to_stored(binding: &GraphEntityBinding) -> StoredGraphBinding {
    let target = binding.target();
    StoredGraphBinding {
        target: StoredGraphBindingTarget {
            primitive: target.primitive(),
            branch: target.branch().map(|branch| branch.as_str().to_owned()),
            space: target.space().as_str().to_owned(),
            key: target.key().to_owned(),
        },
    }
}

fn binding_from_stored(stored: StoredGraphBinding) -> Result<GraphEntityBinding, EngineError> {
    let branch = stored
        .target
        .branch
        .map(BranchName::new)
        .transpose()
        .map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_binding_record",
                "stored graph binding branch is invalid",
            )
        })?;
    let space = crate::data::kv::ProductSpace::new(stored.target.space).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding space is invalid",
        )
    })?;
    let target = GraphBindingTarget::new(stored.target.primitive, branch, space, stored.target.key)
        .map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_binding_record",
                "stored graph binding target is invalid",
            )
        })?;
    Ok(GraphEntityBinding::new(target))
}

use crate::branch::BranchName;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        decode_graph_binding_record, decode_graph_edge_record, decode_graph_metadata_record,
        decode_graph_node_record, encode_graph_binding_record, encode_graph_edge_record,
        encode_graph_metadata_record, encode_graph_node_record, GraphBindingRecord,
        GraphEdgeRecord, GraphMetadataRecord, GraphNodeRecord,
    };
    use crate::data::graph::{
        GraphBindingPrimitive, GraphBindingTarget, GraphEdgeData, GraphEdgeType,
        GraphEntityBinding, GraphName, GraphNodeData, GraphNodeId, GraphProperties,
    };
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn graph_records_round_trip_and_validate_identity() {
        let graph = GraphName::new("deps").expect("graph");
        let node = GraphNodeId::new("a").expect("node");
        let target = GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            None,
            ProductSpace::new("docs").expect("space"),
            "doc-1",
        )
        .expect("target");
        let data = GraphNodeData::new(
            Some(GraphProperties::new(json!({"kind": "doc"})).expect("properties")),
            Some(GraphEntityBinding::new(target)),
        );
        let metadata = GraphMetadataRecord::new(graph.clone());
        let node_record = GraphNodeRecord::new(graph.clone(), node.clone(), data);
        let edge_record = GraphEdgeRecord::new(
            graph.clone(),
            node.clone(),
            GraphEdgeType::new("links").expect("edge type"),
            GraphNodeId::new("b").expect("dst"),
            GraphEdgeData::new(2.0, None).expect("edge data"),
        );
        let binding_record = GraphBindingRecord::new(
            graph.clone(),
            node.clone(),
            node_record.data().binding().expect("binding").clone(),
        );

        assert_eq!(
            decode_graph_metadata_record(&graph, &encode_graph_metadata_record(&metadata))
                .expect("decoded"),
            metadata
        );
        assert_eq!(
            decode_graph_node_record(&graph, &node, &encode_graph_node_record(&node_record))
                .expect("decoded"),
            node_record
        );
        assert_eq!(
            decode_graph_edge_record(
                &graph,
                edge_record.src(),
                edge_record.edge_type(),
                edge_record.dst(),
                &encode_graph_edge_record(&edge_record)
            )
            .expect("decoded"),
            edge_record
        );
        assert_eq!(
            decode_graph_binding_record(
                &graph,
                &node,
                &encode_graph_binding_record(&binding_record)
            )
            .expect("decoded"),
            binding_record
        );
    }

    #[test]
    fn graph_records_reject_wrong_identity() {
        let graph = GraphName::new("deps").expect("graph");
        let other = GraphName::new("other").expect("graph");
        let metadata = GraphMetadataRecord::new(graph);
        let error = decode_graph_metadata_record(&other, &encode_graph_metadata_record(&metadata))
            .expect_err("wrong graph rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_metadata");
    }

    #[test]
    fn graph_records_reject_unknown_versions_and_malformed_payloads() {
        let graph = GraphName::new("deps").expect("graph");
        let node = GraphNodeId::new("a").expect("node");
        let edge_type = GraphEdgeType::new("links").expect("edge type");
        let dst = GraphNodeId::new("b").expect("dst");

        for (case, error) in [
            (
                "metadata-version",
                decode_graph_metadata_record(&graph, b"\x02{}").expect_err("metadata rejected"),
            ),
            (
                "node-version",
                decode_graph_node_record(&graph, &node, b"\x02{}").expect_err("node rejected"),
            ),
            (
                "edge-version",
                decode_graph_edge_record(&graph, &node, &edge_type, &dst, b"\x02{}")
                    .expect_err("edge rejected"),
            ),
            (
                "binding-version",
                decode_graph_binding_record(&graph, &node, b"\x02{}")
                    .expect_err("binding rejected"),
            ),
            (
                "metadata-json",
                decode_graph_metadata_record(&graph, b"\x01{").expect_err("metadata JSON rejected"),
            ),
            (
                "node-json",
                decode_graph_node_record(&graph, &node, b"\x01{").expect_err("node JSON rejected"),
            ),
            (
                "edge-json",
                decode_graph_edge_record(&graph, &node, &edge_type, &dst, b"\x01{")
                    .expect_err("edge JSON rejected"),
            ),
            (
                "edge-weight-overflow",
                decode_graph_edge_record(
                    &graph,
                    &node,
                    &edge_type,
                    &dst,
                    b"\x01{\"graph\":\"deps\",\"src\":\"a\",\"edge_type\":\"links\",\"dst\":\"b\",\"weight\":1e999}",
                )
                .expect_err("edge weight rejected"),
            ),
            (
                "binding-missing",
                decode_graph_binding_record(
                    &graph,
                    &node,
                    b"\x01{\"graph\":\"deps\",\"node_id\":\"a\"}",
                )
                .expect_err("missing binding rejected"),
            ),
        ] {
            assert_eq!(error.class(), EngineErrorClass::Corruption, "{case}");
        }
    }

    /// #3474: a row written before counts were kept still decodes — with no
    /// counts and no create point — and a maintained row round-trips both.
    #[test]
    fn metadata_counts_and_create_point_round_trip_and_legacy_rows_decode() {
        use super::{GraphCommitPoint, GraphCounts, GraphMetadataRecord};
        use strata_core::{CommitVersion, Timestamp};

        let graph = GraphName::new("deps").expect("graph");
        let legacy = decode_graph_metadata_record(&graph, b"\x01{\"graph\":\"deps\"}")
            .expect("a pre-#3474 row decodes");
        assert_eq!(legacy.graph(), &graph);
        assert_eq!(legacy.counts(), None, "counted by scan until rewritten");
        assert_eq!(legacy.created(), None, "created by its own commit");

        // A new graph's row starts maintained: no rows, created by itself.
        let fresh = GraphMetadataRecord::new(graph.clone());
        assert_eq!(fresh.counts(), Some(GraphCounts::new(0, 0)));
        assert_eq!(fresh.created(), None);
        assert_eq!(
            encode_graph_metadata_record(&fresh),
            b"\x01{\"graph\":\"deps\",\"counts\":{\"nodes\":0,\"edges\":0}}",
            "absent fields are omitted, so the shape stays readable by older code"
        );

        let created = GraphCommitPoint::new(CommitVersion::new(7), Timestamp::from_micros(1_000));
        let maintained =
            GraphMetadataRecord::with_state(graph.clone(), created, GraphCounts::new(3, 4), false);
        let decoded =
            decode_graph_metadata_record(&graph, &encode_graph_metadata_record(&maintained))
                .expect("decoded");
        assert_eq!(decoded, maintained);
        assert_eq!(decoded.created(), Some(created));
        assert_eq!(decoded.counts(), Some(GraphCounts::new(3, 4)));
        assert_eq!(created.version(), CommitVersion::new(7));
        assert_eq!(created.timestamp(), Timestamp::from_micros(1_000));
    }

    /// #3474: counts move by what a commit adds or removes; going below zero
    /// is corruption, never a clamp.
    #[test]
    fn metadata_counts_adjust_by_delta_and_refuse_underflow() {
        use super::GraphCounts;

        let counts = GraphCounts::new(2, 3);
        assert_eq!(
            counts.adjusted(1, -3).expect("in range"),
            GraphCounts::new(3, 0)
        );
        assert_eq!(
            counts.adjusted(0, 0).expect("a replace changes nothing"),
            counts
        );
        assert_eq!(
            counts.adjusted(-2, 0).expect("down to zero"),
            GraphCounts::new(0, 3)
        );
        for (nodes, edges) in [(-3, 0), (0, -4)] {
            let error = counts
                .adjusted(nodes, edges)
                .expect_err("fewer rows than the commit removes");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.graph_metadata");
        }
    }

    /// #3477: the deleting mark round-trips, is written only when set, and
    /// is absent on every row written before it existed.
    #[test]
    fn metadata_deleting_mark_round_trips_and_is_omitted_when_clear() {
        use super::GraphMetadataRecord;

        let graph = GraphName::new("deps").expect("graph");
        let clear = GraphMetadataRecord::new(graph.clone());
        assert!(!clear.deleting());
        assert!(
            !String::from_utf8_lossy(&encode_graph_metadata_record(&clear)).contains("deleting"),
            "a clear mark leaves the row as older readers know it"
        );
        let marked = clear.clone().marked_deleting();
        assert!(marked.deleting());
        assert_eq!(marked.counts(), clear.counts(), "the mark keeps the rest");
        let decoded = decode_graph_metadata_record(&graph, &encode_graph_metadata_record(&marked))
            .expect("decoded");
        assert_eq!(decoded, marked);
        assert!(decoded.deleting());
        let legacy = decode_graph_metadata_record(&graph, b"\x01{\"graph\":\"deps\"}")
            .expect("legacy row decodes");
        assert!(!legacy.deleting());
    }

    /// #3464: the import watermark round-trips, is written only when set,
    /// and is absent on every row written before it existed.
    #[test]
    fn metadata_import_watermark_round_trips_and_is_omitted_when_clear() {
        use super::{GraphCommitPoint, GraphCounts, GraphMetadataRecord};
        use strata_core::{CommitVersion, Timestamp};

        let graph = GraphName::new("deps").expect("graph");
        let created = GraphCommitPoint::new(CommitVersion::new(2), Timestamp::from_micros(20));
        let clear =
            GraphMetadataRecord::with_state(graph.clone(), created, GraphCounts::new(1, 0), false);
        assert!(!clear.importing());
        assert!(
            !String::from_utf8_lossy(&encode_graph_metadata_record(&clear)).contains("importing"),
            "a clear watermark leaves the row as older readers know it"
        );
        let pending =
            GraphMetadataRecord::with_state(graph.clone(), created, GraphCounts::new(1, 0), true);
        assert!(pending.importing());
        let decoded = decode_graph_metadata_record(&graph, &encode_graph_metadata_record(&pending))
            .expect("decoded");
        assert_eq!(decoded, pending);
        assert!(decoded.importing());
        assert!(!GraphMetadataRecord::new(graph.clone()).importing());
        assert!(
            !decode_graph_metadata_record(&graph, b"\x01{\"graph\":\"deps\"}")
                .expect("legacy row decodes")
                .importing()
        );
    }
}
