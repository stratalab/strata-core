//! Graph core storage envelopes.

use serde::{Deserialize, Serialize};

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphMetadataRecord {
    graph: GraphName,
}

impl GraphMetadataRecord {
    pub(crate) const fn new(graph: GraphName) -> Self {
        Self { graph }
    }

    pub(crate) const fn graph(&self) -> &GraphName {
        &self.graph
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

pub(crate) fn encode_graph_type_index_record(
    record: &GraphTypeIndexRecord,
) -> Result<Vec<u8>, EngineError> {
    let stored = StoredGraphTypeIndex {
        graph: record.graph().as_str().to_owned(),
        object_type: record.object_type().as_str().to_owned(),
        node_id: record.node_id().as_str().to_owned(),
    };
    encode_json_record(
        GRAPH_TYPE_INDEX_FORMAT_VERSION,
        &stored,
        "invalid_argument.engine.graph_type_index_record",
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

pub(crate) fn encode_graph_metadata_record(
    record: &GraphMetadataRecord,
) -> Result<Vec<u8>, EngineError> {
    let stored = StoredGraphMetadata {
        graph: record.graph().as_str().to_owned(),
    };
    encode_json_record(
        GRAPH_METADATA_FORMAT_VERSION,
        &stored,
        "invalid_argument.engine.graph_metadata",
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
    Ok(GraphMetadataRecord::new(graph))
}

pub(crate) fn encode_graph_node_record(record: &GraphNodeRecord) -> Result<Vec<u8>, EngineError> {
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
        "invalid_argument.engine.graph_node_record",
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

pub(crate) fn encode_graph_edge_record(record: &GraphEdgeRecord) -> Result<Vec<u8>, EngineError> {
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
        "invalid_argument.engine.graph_edge_record",
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

pub(crate) fn encode_graph_binding_record(
    record: &GraphBindingRecord,
) -> Result<Vec<u8>, EngineError> {
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
        "invalid_argument.engine.graph_binding_record",
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

fn encode_json_record<T: Serialize>(
    version: u8,
    value: &T,
    code: &'static str,
    message: &'static str,
) -> Result<Vec<u8>, EngineError> {
    let mut bytes = vec![version];
    bytes.extend(
        serde_json::to_vec(value)
            .map_err(|error| EngineError::invalid_input(code, format!("{message}: {error}")))?,
    );
    Ok(bytes)
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
            decode_graph_metadata_record(
                &graph,
                &encode_graph_metadata_record(&metadata).expect("encoded")
            )
            .expect("decoded"),
            metadata
        );
        assert_eq!(
            decode_graph_node_record(
                &graph,
                &node,
                &encode_graph_node_record(&node_record).expect("encoded")
            )
            .expect("decoded"),
            node_record
        );
        assert_eq!(
            decode_graph_edge_record(
                &graph,
                edge_record.src(),
                edge_record.edge_type(),
                edge_record.dst(),
                &encode_graph_edge_record(&edge_record).expect("encoded")
            )
            .expect("decoded"),
            edge_record
        );
        assert_eq!(
            decode_graph_binding_record(
                &graph,
                &node,
                &encode_graph_binding_record(&binding_record).expect("encoded")
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
        let error = decode_graph_metadata_record(
            &other,
            &encode_graph_metadata_record(&metadata).expect("encoded"),
        )
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
}
