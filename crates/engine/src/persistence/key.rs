//! Stable engine row-key encoding.

use crate::data::event::{EventSequence, EventType};
use crate::data::graph::{
    GraphBindingPrimitive, GraphBindingTarget, GraphEdgeType, GraphName, GraphNodeId, GraphTypeName,
};
use crate::data::json::{JsonDocumentId, JsonIndexName};
use crate::data::kv::{KvKey, ProductSpace};
use crate::data::vector::{VectorCollectionName, VectorKey};
use crate::diagnostics::EngineError;

const KEY_VERSION: u8 = 1;
const KV_DISCRIMINATOR: u8 = b'k';
const JSON_DISCRIMINATOR: u8 = b'j';
const JSON_INDEX_META_DISCRIMINATOR: u8 = b'm';
const JSON_INDEX_ENTRY_DISCRIMINATOR: u8 = b'i';
const VECTOR_COLLECTION_DISCRIMINATOR: u8 = b'c';
const VECTOR_ENTRY_DISCRIMINATOR: u8 = b'v';
const EVENT_RECORD_DISCRIMINATOR: u8 = b'e';
const EVENT_META_DISCRIMINATOR: u8 = b'E';
const EVENT_TYPE_INDEX_DISCRIMINATOR: u8 = b't';
const GRAPH_METADATA_DISCRIMINATOR: u8 = b'g';
const GRAPH_NODE_DISCRIMINATOR: u8 = b'n';
const GRAPH_EDGE_DISCRIMINATOR: u8 = b'o';
const GRAPH_REVERSE_EDGE_DISCRIMINATOR: u8 = b'r';
const GRAPH_BINDING_INDEX_DISCRIMINATOR: u8 = b'b';
const GRAPH_ONTOLOGY_DISCRIMINATOR: u8 = b'y';
const GRAPH_TYPE_INDEX_DISCRIMINATOR: u8 = b'T';

pub(crate) fn encode_kv_key(space: &ProductSpace, key: &KvKey) -> Vec<u8> {
    encode_kv_key_bytes(space, key.as_bytes())
}

pub(crate) fn encode_kv_key_bytes(space: &ProductSpace, key_bytes: &[u8]) -> Vec<u8> {
    encode_user_key(KV_DISCRIMINATOR, space, key_bytes)
}

pub(crate) fn encode_json_key(space: &ProductSpace, id: &JsonDocumentId) -> Vec<u8> {
    encode_user_key(JSON_DISCRIMINATOR, space, id.as_str().as_bytes())
}

pub(crate) fn encode_json_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(JSON_DISCRIMINATOR, space, &[])
}

pub(crate) fn decode_json_document_id(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<JsonDocumentId, EngineError> {
    let id_bytes = decode_user_key(
        space,
        encoded,
        JSON_DISCRIMINATOR,
        "data_loss.engine.json_key",
        "stored JSON row key is not valid for the selected product space",
    )?;
    let id = std::str::from_utf8(id_bytes).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_key",
            "stored JSON row key is not valid UTF-8",
        )
    })?;
    JsonDocumentId::new(id).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_key",
            "stored JSON row key is not a valid document id",
        )
    })
}

pub(crate) fn encode_json_index_meta_key(space: &ProductSpace, name: &JsonIndexName) -> Vec<u8> {
    encode_json_index_key(JSON_INDEX_META_DISCRIMINATOR, space, name, &[])
}

pub(crate) fn encode_json_index_meta_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(JSON_INDEX_META_DISCRIMINATOR, space, &[])
}

pub(crate) fn decode_json_index_name(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<JsonIndexName, EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        JSON_INDEX_META_DISCRIMINATOR,
        "data_loss.engine.json_index_key",
        "stored JSON index metadata key is not valid for the selected product space",
    )?;
    if bytes.len() < 2 {
        return Err(EngineError::corruption(
            "data_loss.engine.json_index_key",
            "stored JSON index metadata key is truncated",
        ));
    }
    let name_len = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    let name_end = 2usize.checked_add(name_len).ok_or_else(|| {
        EngineError::corruption(
            "data_loss.engine.json_index_key",
            "stored JSON index metadata key length overflowed",
        )
    })?;
    if bytes.len() != name_end {
        return Err(EngineError::corruption(
            "data_loss.engine.json_index_key",
            "stored JSON index metadata key has trailing bytes",
        ));
    }
    let name = std::str::from_utf8(&bytes[2..name_end]).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_index_key",
            "stored JSON index metadata key is not valid UTF-8",
        )
    })?;
    JsonIndexName::new(name).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_index_key",
            "stored JSON index metadata key is not a valid index name",
        )
    })
}

pub(crate) fn encode_json_index_entry_key(
    space: &ProductSpace,
    name: &JsonIndexName,
    encoded_value: &[u8],
    id: &JsonDocumentId,
) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(encoded_value.len() + 1 + id.as_str().len());
    suffix.extend_from_slice(encoded_value);
    suffix.push(0xff);
    suffix.extend_from_slice(id.as_str().as_bytes());
    encode_json_index_key(JSON_INDEX_ENTRY_DISCRIMINATOR, space, name, &suffix)
}

pub(crate) fn encode_json_index_entry_prefix(
    space: &ProductSpace,
    name: &JsonIndexName,
) -> Vec<u8> {
    encode_json_index_key(JSON_INDEX_ENTRY_DISCRIMINATOR, space, name, &[])
}

pub(crate) fn encode_json_index_entry_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(JSON_INDEX_ENTRY_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_vector_collection_key(
    space: &ProductSpace,
    collection: &VectorCollectionName,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, collection.as_str());
    encode_user_key(VECTOR_COLLECTION_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_vector_collection_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(VECTOR_COLLECTION_DISCRIMINATOR, space, &[])
}

pub(crate) fn decode_vector_collection_name(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<VectorCollectionName, EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        VECTOR_COLLECTION_DISCRIMINATOR,
        "data_loss.engine.vector_collection_key",
        "stored vector collection row key is not valid for the selected product space",
    )?;
    let (collection, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.vector_collection_key",
        "stored vector collection row key is truncated",
    )?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_collection_key",
            "stored vector collection row key has trailing bytes",
        ));
    }
    VectorCollectionName::new(collection).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_collection_key",
            "stored vector collection row key contains an invalid collection name",
        )
    })
}

pub(crate) fn encode_vector_key(
    space: &ProductSpace,
    collection: &VectorCollectionName,
    key: &VectorKey,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, collection.as_str());
    suffix.extend_from_slice(key.as_str().as_bytes());
    encode_user_key(VECTOR_ENTRY_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_vector_collection_entry_prefix(
    space: &ProductSpace,
    collection: &VectorCollectionName,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, collection.as_str());
    encode_user_key(VECTOR_ENTRY_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn decode_vector_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<(VectorCollectionName, VectorKey), EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        VECTOR_ENTRY_DISCRIMINATOR,
        "data_loss.engine.vector_key",
        "stored vector row key is not valid for the selected product space",
    )?;
    let (collection, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.vector_key",
        "stored vector row key is missing a collection name",
    )?;
    let key = std::str::from_utf8(rest).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_key",
            "stored vector row key contains a non-UTF-8 vector key",
        )
    })?;
    let collection = VectorCollectionName::new(collection).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_key",
            "stored vector row key contains an invalid collection name",
        )
    })?;
    let key = VectorKey::new(key).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_key",
            "stored vector row key contains an invalid vector key",
        )
    })?;
    Ok((collection, key))
}

pub(crate) fn encode_event_key(space: &ProductSpace, sequence: EventSequence) -> Vec<u8> {
    encode_user_key(
        EVENT_RECORD_DISCRIMINATOR,
        space,
        &sequence.as_u64().to_be_bytes(),
    )
}

pub(crate) fn decode_event_key_sequence(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<EventSequence, EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        EVENT_RECORD_DISCRIMINATOR,
        "data_loss.engine.event_key",
        "stored event row key is not valid for the selected product space",
    )?;
    decode_event_sequence_suffix(bytes, "data_loss.engine.event_key")
}

pub(crate) fn encode_event_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(EVENT_RECORD_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_event_meta_key(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(EVENT_META_DISCRIMINATOR, space, b"meta")
}

pub(crate) fn encode_event_meta_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(EVENT_META_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_event_type_index_key(
    space: &ProductSpace,
    event_type: &EventType,
    sequence: EventSequence,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, event_type.as_str());
    suffix.extend_from_slice(&sequence.as_u64().to_be_bytes());
    encode_user_key(EVENT_TYPE_INDEX_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_event_type_index_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(EVENT_TYPE_INDEX_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_graph_metadata_key(space: &ProductSpace, graph: &GraphName) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_user_key(GRAPH_METADATA_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_ontology_key(space: &ProductSpace, graph: &GraphName) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_user_key(GRAPH_ONTOLOGY_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_ontology_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(GRAPH_ONTOLOGY_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_graph_type_index_key(
    space: &ProductSpace,
    graph: &GraphName,
    object_type: &GraphTypeName,
    node_id: &GraphNodeId,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_length_prefixed_text(&mut suffix, object_type.as_str());
    encode_length_prefixed_text(&mut suffix, node_id.as_str());
    encode_user_key(GRAPH_TYPE_INDEX_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_type_index_type_prefix(
    space: &ProductSpace,
    graph: &GraphName,
    object_type: &GraphTypeName,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_length_prefixed_text(&mut suffix, object_type.as_str());
    encode_user_key(GRAPH_TYPE_INDEX_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_type_index_graph_prefix(
    space: &ProductSpace,
    graph: &GraphName,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_user_key(GRAPH_TYPE_INDEX_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_type_index_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(GRAPH_TYPE_INDEX_DISCRIMINATOR, space, &[])
}

pub(crate) fn decode_graph_type_index_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<(GraphName, GraphTypeName, GraphNodeId), EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        GRAPH_TYPE_INDEX_DISCRIMINATOR,
        "data_loss.engine.graph_type_index_key",
        "stored graph type index row key is not valid for the selected product space",
    )?;
    let (graph, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.graph_type_index_key",
        "stored graph type index row key is missing a graph name",
    )?;
    let (object_type, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_type_index_key",
        "stored graph type index row key is missing an object type",
    )?;
    let (node_id, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_type_index_key",
        "stored graph type index row key is missing a node id",
    )?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_type_index_key",
            "stored graph type index row key has trailing bytes",
        ));
    }
    Ok((
        GraphName::new(graph).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_type_index_key",
                "stored graph type index row key contains an invalid graph name",
            )
        })?,
        GraphTypeName::new(object_type).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_type_index_key",
                "stored graph type index row key contains an invalid object type",
            )
        })?,
        GraphNodeId::new(node_id).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_type_index_key",
                "stored graph type index row key contains an invalid node id",
            )
        })?,
    ))
}

pub(crate) fn encode_graph_metadata_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(GRAPH_METADATA_DISCRIMINATOR, space, &[])
}

pub(crate) fn decode_graph_metadata_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<GraphName, EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        GRAPH_METADATA_DISCRIMINATOR,
        "data_loss.engine.graph_key",
        "stored graph metadata row key is not valid for the selected product space",
    )?;
    let (graph, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.graph_key",
        "stored graph metadata row key is missing a graph name",
    )?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_key",
            "stored graph metadata row key has trailing bytes",
        ));
    }
    GraphName::new(graph).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_key",
            "stored graph metadata row key contains an invalid graph name",
        )
    })
}

pub(crate) fn decode_graph_ontology_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<GraphName, EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        GRAPH_ONTOLOGY_DISCRIMINATOR,
        "data_loss.engine.graph_key",
        "stored graph ontology row key is not valid for the selected product space",
    )?;
    let (graph, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.graph_key",
        "stored graph ontology row key is missing a graph name",
    )?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_key",
            "stored graph ontology row key has trailing bytes",
        ));
    }
    GraphName::new(graph).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_key",
            "stored graph ontology row key contains an invalid graph name",
        )
    })
}

pub(crate) fn encode_graph_node_key(
    space: &ProductSpace,
    graph: &GraphName,
    node_id: &GraphNodeId,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_length_prefixed_text(&mut suffix, node_id.as_str());
    encode_user_key(GRAPH_NODE_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_node_prefix(space: &ProductSpace, graph: &GraphName) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_user_key(GRAPH_NODE_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_node_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(GRAPH_NODE_DISCRIMINATOR, space, &[])
}

pub(crate) fn decode_graph_node_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<(GraphName, GraphNodeId), EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        GRAPH_NODE_DISCRIMINATOR,
        "data_loss.engine.graph_node_key",
        "stored graph node row key is not valid for the selected product space",
    )?;
    let (graph, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.graph_node_key",
        "stored graph node row key is missing a graph name",
    )?;
    let (node_id, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_node_key",
        "stored graph node row key is missing a node id",
    )?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_node_key",
            "stored graph node row key has trailing bytes",
        ));
    }
    Ok((
        GraphName::new(graph).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_node_key",
                "stored graph node row key contains an invalid graph name",
            )
        })?,
        GraphNodeId::new(node_id).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_node_key",
                "stored graph node row key contains an invalid node id",
            )
        })?,
    ))
}

pub(crate) fn encode_graph_edge_key(
    space: &ProductSpace,
    graph: &GraphName,
    src: &GraphNodeId,
    edge_type: &GraphEdgeType,
    dst: &GraphNodeId,
) -> Vec<u8> {
    encode_graph_edge_like_key(GRAPH_EDGE_DISCRIMINATOR, space, graph, src, edge_type, dst)
}

pub(crate) fn encode_graph_edge_prefix(space: &ProductSpace, graph: &GraphName) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_user_key(GRAPH_EDGE_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_edge_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(GRAPH_EDGE_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_graph_outgoing_edge_prefix(
    space: &ProductSpace,
    graph: &GraphName,
    src: &GraphNodeId,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_length_prefixed_text(&mut suffix, src.as_str());
    encode_user_key(GRAPH_EDGE_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn decode_graph_edge_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<(GraphName, GraphNodeId, GraphEdgeType, GraphNodeId), EngineError> {
    decode_graph_edge_like_key(
        space,
        encoded,
        GRAPH_EDGE_DISCRIMINATOR,
        "data_loss.engine.graph_edge_key",
        "stored graph edge row key is not valid for the selected product space",
    )
}

pub(crate) fn encode_graph_reverse_edge_key(
    space: &ProductSpace,
    graph: &GraphName,
    dst: &GraphNodeId,
    edge_type: &GraphEdgeType,
    src: &GraphNodeId,
) -> Vec<u8> {
    encode_graph_edge_like_key(
        GRAPH_REVERSE_EDGE_DISCRIMINATOR,
        space,
        graph,
        dst,
        edge_type,
        src,
    )
}

pub(crate) fn encode_graph_reverse_edge_prefix(space: &ProductSpace, graph: &GraphName) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_user_key(GRAPH_REVERSE_EDGE_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_reverse_edge_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(GRAPH_REVERSE_EDGE_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_graph_incoming_edge_prefix(
    space: &ProductSpace,
    graph: &GraphName,
    dst: &GraphNodeId,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_length_prefixed_text(&mut suffix, dst.as_str());
    encode_user_key(GRAPH_REVERSE_EDGE_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn decode_graph_reverse_edge_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<(GraphName, GraphNodeId, GraphEdgeType, GraphNodeId), EngineError> {
    decode_graph_edge_like_key(
        space,
        encoded,
        GRAPH_REVERSE_EDGE_DISCRIMINATOR,
        "data_loss.engine.graph_reverse_edge_key",
        "stored graph reverse edge row key is not valid for the selected product space",
    )
}

pub(crate) fn encode_graph_binding_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(GRAPH_BINDING_INDEX_DISCRIMINATOR, space, &[])
}

pub(crate) fn encode_graph_binding_target_prefix(
    space: &ProductSpace,
    target: &GraphBindingTarget,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_graph_binding_target_suffix(&mut suffix, target);
    encode_user_key(GRAPH_BINDING_INDEX_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn encode_graph_binding_key(
    space: &ProductSpace,
    target: &GraphBindingTarget,
    graph: &GraphName,
    node_id: &GraphNodeId,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_graph_binding_target_suffix(&mut suffix, target);
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_length_prefixed_text(&mut suffix, node_id.as_str());
    encode_user_key(GRAPH_BINDING_INDEX_DISCRIMINATOR, space, &suffix)
}

pub(crate) fn decode_graph_binding_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<(GraphBindingTarget, GraphName, GraphNodeId), EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        GRAPH_BINDING_INDEX_DISCRIMINATOR,
        "data_loss.engine.graph_binding_key",
        "stored graph binding row key is not valid for the selected product space",
    )?;
    let (target, rest) = decode_graph_binding_target_suffix(bytes)?;
    let (graph, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_binding_key",
        "stored graph binding row key is missing a graph name",
    )?;
    let (node_id, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_binding_key",
        "stored graph binding row key is missing a node id",
    )?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_binding_key",
            "stored graph binding row key has trailing bytes",
        ));
    }
    Ok((
        target,
        GraphName::new(graph).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_binding_key",
                "stored graph binding row key contains an invalid graph name",
            )
        })?,
        GraphNodeId::new(node_id).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_binding_key",
                "stored graph binding row key contains an invalid node id",
            )
        })?,
    ))
}

#[cfg(test)]
fn encode_event_type_index_prefix(space: &ProductSpace, event_type: &EventType) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, event_type.as_str());
    encode_user_key(EVENT_TYPE_INDEX_DISCRIMINATOR, space, &suffix)
}

#[cfg(test)]
fn decode_event_sequence(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<EventSequence, EngineError> {
    decode_event_type_index_key(space, encoded).map(|(_, sequence)| sequence)
}

#[cfg(test)]
fn decode_event_type_index_key(
    space: &ProductSpace,
    encoded: &[u8],
) -> Result<(EventType, EventSequence), EngineError> {
    let bytes = decode_user_key(
        space,
        encoded,
        EVENT_TYPE_INDEX_DISCRIMINATOR,
        "data_loss.engine.event_index_key",
        "stored event type index row key is not valid for the selected product space",
    )?;
    let (event_type, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.event_index_key",
        "stored event type index row key is missing an event type",
    )?;
    let event_type = EventType::new(event_type).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.event_index_key",
            "stored event type index row key contains an invalid event type",
        )
    })?;
    if rest.len() != 8 {
        return Err(EngineError::corruption(
            "data_loss.engine.event_index_key",
            "stored event type index row key has an invalid sequence length",
        ));
    }
    let sequence = decode_event_sequence_suffix(rest, "data_loss.engine.event_index_key")?;
    Ok((event_type, sequence))
}

fn decode_event_sequence_suffix(
    bytes: &[u8],
    code: &'static str,
) -> Result<EventSequence, EngineError> {
    if bytes.len() != 8 {
        return Err(EngineError::corruption(
            code,
            "stored event row key has an invalid sequence length",
        ));
    }
    let sequence = u64::from_be_bytes(bytes.try_into().map_err(|_| {
        EngineError::corruption(code, "stored event row key sequence is malformed")
    })?);
    Ok(EventSequence::new(sequence))
}

fn encode_json_index_key(
    discriminator: u8,
    space: &ProductSpace,
    name: &JsonIndexName,
    suffix: &[u8],
) -> Vec<u8> {
    let name_bytes = name.as_str().as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("validated JSON index name length");
    let mut key = encode_user_key(discriminator, space, &[]);
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(suffix);
    key
}

fn encode_graph_edge_like_key(
    discriminator: u8,
    space: &ProductSpace,
    graph: &GraphName,
    first: &GraphNodeId,
    edge_type: &GraphEdgeType,
    second: &GraphNodeId,
) -> Vec<u8> {
    let mut suffix = Vec::new();
    encode_length_prefixed_text(&mut suffix, graph.as_str());
    encode_length_prefixed_text(&mut suffix, first.as_str());
    encode_length_prefixed_text(&mut suffix, edge_type.as_str());
    encode_length_prefixed_text(&mut suffix, second.as_str());
    encode_user_key(discriminator, space, &suffix)
}

fn decode_graph_edge_like_key(
    space: &ProductSpace,
    encoded: &[u8],
    discriminator: u8,
    code: &'static str,
    message: &'static str,
) -> Result<(GraphName, GraphNodeId, GraphEdgeType, GraphNodeId), EngineError> {
    let bytes = decode_user_key(space, encoded, discriminator, code, message)?;
    let (graph, rest) =
        decode_length_prefixed_text(bytes, code, "stored graph edge row key is missing a graph")?;
    let (first, rest) =
        decode_length_prefixed_text(rest, code, "stored graph edge row key is missing a node id")?;
    let (edge_type, rest) = decode_length_prefixed_text(
        rest,
        code,
        "stored graph edge row key is missing an edge type",
    )?;
    let (second, rest) =
        decode_length_prefixed_text(rest, code, "stored graph edge row key is missing a node id")?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            code,
            "stored graph edge row key has trailing bytes",
        ));
    }
    Ok((
        GraphName::new(graph).map_err(|_| {
            EngineError::corruption(code, "stored graph edge key contains an invalid graph name")
        })?,
        GraphNodeId::new(first).map_err(|_| {
            EngineError::corruption(code, "stored graph edge key contains an invalid node id")
        })?,
        GraphEdgeType::new(edge_type).map_err(|_| {
            EngineError::corruption(code, "stored graph edge key contains an invalid edge type")
        })?,
        GraphNodeId::new(second).map_err(|_| {
            EngineError::corruption(code, "stored graph edge key contains an invalid node id")
        })?,
    ))
}

fn encode_graph_binding_target_suffix(output: &mut Vec<u8>, target: &GraphBindingTarget) {
    encode_length_prefixed_text(output, target.primitive().as_str());
    encode_length_prefixed_text(output, target.branch().map_or("", |branch| branch.as_str()));
    encode_length_prefixed_text(output, target.space().as_str());
    encode_length_prefixed_text(output, target.key());
}

fn decode_graph_binding_target_suffix(
    bytes: &[u8],
) -> Result<(GraphBindingTarget, &[u8]), EngineError> {
    let (primitive, rest) = decode_length_prefixed_text(
        bytes,
        "data_loss.engine.graph_binding_key",
        "stored graph binding row key is missing primitive kind",
    )?;
    let primitive = match primitive {
        "kv" => GraphBindingPrimitive::Kv,
        "json" => GraphBindingPrimitive::Json,
        "vector" => GraphBindingPrimitive::Vector,
        "event" => GraphBindingPrimitive::Event,
        "graph" => GraphBindingPrimitive::Graph,
        _ => {
            return Err(EngineError::corruption(
                "data_loss.engine.graph_binding_key",
                "stored graph binding row key contains an unknown primitive kind",
            ));
        }
    };
    let (branch, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_binding_key",
        "stored graph binding row key is missing branch",
    )?;
    let branch = if branch.is_empty() {
        None
    } else {
        Some(crate::branch::BranchName::new(branch).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.graph_binding_key",
                "stored graph binding row key contains an invalid branch",
            )
        })?)
    };
    let (space, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_binding_key",
        "stored graph binding row key is missing bound space",
    )?;
    let space = ProductSpace::new(space).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_key",
            "stored graph binding row key contains an invalid bound space",
        )
    })?;
    let (key, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.graph_binding_key",
        "stored graph binding row key is missing bound key",
    )?;
    let target = GraphBindingTarget::new(primitive, branch, space, key).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_key",
            "stored graph binding row key contains an invalid target",
        )
    })?;
    Ok((target, rest))
}

fn encode_length_prefixed_text(output: &mut Vec<u8>, value: &str) {
    let value_bytes = value.as_bytes();
    let value_len = u16::try_from(value_bytes.len()).expect("validated vector key field length");
    output.extend_from_slice(&value_len.to_be_bytes());
    output.extend_from_slice(value_bytes);
}

fn decode_length_prefixed_text<'a>(
    bytes: &'a [u8],
    code: &'static str,
    message: &'static str,
) -> Result<(&'a str, &'a [u8]), EngineError> {
    if bytes.len() < 2 {
        return Err(EngineError::corruption(code, message));
    }
    let value_len = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    let value_start = 2usize;
    let value_end = value_start
        .checked_add(value_len)
        .ok_or_else(|| EngineError::corruption(code, "stored vector row key length overflowed"))?;
    if bytes.len() < value_end {
        return Err(EngineError::corruption(code, message));
    }
    let value = std::str::from_utf8(&bytes[value_start..value_end]).map_err(|_| {
        EngineError::corruption(code, "stored vector row key field is not valid UTF-8")
    })?;
    Ok((value, &bytes[value_end..]))
}

fn encode_user_key(discriminator: u8, space: &ProductSpace, key_bytes: &[u8]) -> Vec<u8> {
    let space_bytes = space.as_str().as_bytes();
    let space_len = u16::try_from(space_bytes.len()).expect("validated product space length");
    let mut encoded = Vec::with_capacity(4 + space_bytes.len() + key_bytes.len());
    encoded.push(KEY_VERSION);
    encoded.push(discriminator);
    encoded.extend_from_slice(&space_len.to_be_bytes());
    encoded.extend_from_slice(space_bytes);
    encoded.extend_from_slice(key_bytes);
    encoded
}

pub(crate) fn encode_kv_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_kv_key_bytes(space, &[])
}

pub(crate) fn encode_vector_space_prefix(space: &ProductSpace) -> Vec<u8> {
    encode_user_key(VECTOR_ENTRY_DISCRIMINATOR, space, &[])
}

pub(crate) fn decode_kv_key(space: &ProductSpace, encoded: &[u8]) -> Result<KvKey, EngineError> {
    let key_bytes = decode_user_key(
        space,
        encoded,
        KV_DISCRIMINATOR,
        "data_loss.engine.kv_key",
        "stored KV row key is not valid for the selected product space",
    )?;
    KvKey::new(key_bytes)
}

fn decode_user_key<'a>(
    space: &ProductSpace,
    encoded: &'a [u8],
    discriminator: u8,
    code: &'static str,
    message: &'static str,
) -> Result<&'a [u8], EngineError> {
    let corruption = || EngineError::corruption(code, message);
    if encoded.len() < 4 {
        return Err(corruption());
    }
    if encoded[0] != KEY_VERSION || encoded[1] != discriminator {
        return Err(corruption());
    }
    let space_len = usize::from(u16::from_be_bytes([encoded[2], encoded[3]]));
    let key_start = 4usize.checked_add(space_len).ok_or_else(corruption)?;
    if encoded.len() < key_start {
        return Err(corruption());
    }
    if &encoded[4..key_start] != space.as_str().as_bytes() {
        return Err(corruption());
    }
    let key_bytes = &encoded[key_start..];
    if key_bytes.is_empty() {
        return Err(corruption());
    }
    Ok(key_bytes)
}

pub(crate) fn remote_origin_key() -> Vec<u8> {
    b"\x01provenance:remote-origin".to_vec()
}

pub(crate) fn database_identity_key() -> Vec<u8> {
    b"\x01identity".to_vec()
}

pub(crate) fn local_instance_identity_key() -> Vec<u8> {
    b"\x01identity:local-instance".to_vec()
}

pub(crate) fn storage_registry_key() -> Vec<u8> {
    b"\x01registry:storage-spaces".to_vec()
}

pub(crate) fn capability_registry_key() -> Vec<u8> {
    b"\x01registry:capabilities".to_vec()
}

pub(crate) fn migration_registry_key() -> Vec<u8> {
    b"\x01registry:migrations".to_vec()
}

pub(crate) fn branch_index_key() -> Vec<u8> {
    b"\x01branch:index".to_vec()
}

pub(crate) fn branch_default_key() -> Vec<u8> {
    b"\x01branch:default".to_vec()
}

pub(crate) fn branch_pending_index_key() -> Vec<u8> {
    b"\x01branch:pending-index".to_vec()
}

pub(crate) fn branch_catalog_key(name: &str) -> Vec<u8> {
    let name_len = u16::try_from(name.len()).expect("validated branch name length");
    let mut key = Vec::with_capacity(10 + name.len());
    key.extend_from_slice(b"\x01branch:");
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

pub(crate) fn branch_pending_key(name: &str) -> Vec<u8> {
    let name_len = u16::try_from(name.len()).expect("validated branch name length");
    let mut key = Vec::with_capacity(18 + name.len());
    key.extend_from_slice(b"\x01branch:pending:");
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

pub(crate) fn space_index_key() -> Vec<u8> {
    b"\x01space:index".to_vec()
}

pub(crate) fn space_catalog_key(name: &str) -> Vec<u8> {
    let name_len = u16::try_from(name.len()).expect("validated product space length");
    let mut key = Vec::with_capacity(9 + name.len());
    key.extend_from_slice(b"\x01space:");
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

pub(crate) fn reserved_space_key(name: &str) -> Vec<u8> {
    let name_len = u16::try_from(name.len()).expect("validated reserved space length");
    let mut key = Vec::with_capacity(18 + name.len());
    key.extend_from_slice(b"\x01space:reserved:");
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

pub(crate) fn vector_index_manifest_key(
    space: &ProductSpace,
    collection: &VectorCollectionName,
) -> Vec<u8> {
    let space_len = u16::try_from(space.as_str().len()).expect("validated product space length");
    let collection_len =
        u16::try_from(collection.as_str().len()).expect("validated collection name length");
    let mut key = Vec::with_capacity(27 + space.as_str().len() + collection.as_str().len());
    key.extend_from_slice(b"\x01vector:index-manifest:");
    key.extend_from_slice(&space_len.to_be_bytes());
    key.extend_from_slice(space.as_str().as_bytes());
    key.extend_from_slice(&collection_len.to_be_bytes());
    key.extend_from_slice(collection.as_str().as_bytes());
    key
}

pub(crate) fn vector_index_manifest_prefix() -> Vec<u8> {
    b"\x01vector:index-manifest:".to_vec()
}

pub(crate) fn decode_vector_index_manifest_key(
    encoded: &[u8],
) -> Result<(ProductSpace, VectorCollectionName), EngineError> {
    let prefix = vector_index_manifest_prefix();
    if !encoded.starts_with(&prefix) {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_index_manifest_key",
            "stored vector index manifest key has an invalid prefix",
        ));
    }
    let mut rest = &encoded[prefix.len()..];
    let (space, next) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.vector_index_manifest_key",
        "stored vector index manifest key is missing a product space",
    )?;
    rest = next;
    let (collection, rest) = decode_length_prefixed_text(
        rest,
        "data_loss.engine.vector_index_manifest_key",
        "stored vector index manifest key is missing a collection name",
    )?;
    if !rest.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_index_manifest_key",
            "stored vector index manifest key has trailing bytes",
        ));
    }
    Ok((
        ProductSpace::new(space).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_index_manifest_key",
                "stored vector index manifest key contains an invalid product space",
            )
        })?,
        VectorCollectionName::new(collection).map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_index_manifest_key",
                "stored vector index manifest key contains an invalid collection name",
            )
        })?,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        branch_catalog_key, branch_default_key, branch_index_key, branch_pending_key,
        capability_registry_key, database_identity_key, decode_event_key_sequence,
        decode_event_sequence, decode_event_type_index_key, decode_graph_binding_key,
        decode_graph_edge_key, decode_graph_metadata_key, decode_graph_node_key,
        decode_graph_reverse_edge_key, decode_json_document_id, decode_json_index_name,
        decode_kv_key, decode_vector_collection_name, decode_vector_index_manifest_key,
        decode_vector_key, encode_event_key, encode_event_meta_key, encode_event_space_prefix,
        encode_event_type_index_key, encode_event_type_index_prefix, encode_graph_binding_key,
        encode_graph_binding_target_prefix, encode_graph_edge_key, encode_graph_edge_prefix,
        encode_graph_metadata_key, encode_graph_metadata_prefix, encode_graph_node_key,
        encode_graph_node_prefix, encode_graph_reverse_edge_key, encode_graph_reverse_edge_prefix,
        encode_json_index_entry_key, encode_json_index_entry_prefix, encode_json_index_meta_key,
        encode_json_key, encode_json_space_prefix, encode_kv_key, encode_kv_space_prefix,
        encode_vector_collection_entry_prefix, encode_vector_collection_key,
        encode_vector_collection_prefix, encode_vector_key, local_instance_identity_key,
        migration_registry_key, reserved_space_key, space_catalog_key, space_index_key,
        storage_registry_key, vector_index_manifest_key, vector_index_manifest_prefix,
    };
    use crate::data::event::{EventSequence, EventType};
    use crate::data::graph::{
        GraphBindingPrimitive, GraphBindingTarget, GraphEdgeType, GraphName, GraphNodeId,
    };
    use crate::data::json::{JsonDocumentId, JsonIndexName};
    use crate::data::kv::{KvKey, ProductSpace};
    use crate::data::vector::{VectorCollectionName, VectorKey};
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn kv_key_encoding_is_deterministic_for_ascii_keys() {
        let space = ProductSpace::new("users").expect("valid space");
        let key = KvKey::new(b"alice".as_slice()).expect("valid key");
        assert_eq!(
            encode_kv_key(&space, &key),
            b"\x01k\0\x05usersalice".to_vec()
        );
    }

    #[test]
    fn kv_key_encoding_is_deterministic_for_binary_keys() {
        let space = ProductSpace::new("default").expect("valid space");
        let key = KvKey::new([0, 1, 255]).expect("valid key");
        assert_eq!(
            encode_kv_key(&space, &key),
            vec![1, b'k', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 1, 255]
        );
    }

    #[test]
    fn kv_key_encoding_preserves_order_inside_one_space() {
        let space = ProductSpace::new("default").expect("valid space");
        let a = encode_kv_key(&space, &KvKey::new(b"a".as_slice()).expect("valid key"));
        let b = encode_kv_key(&space, &KvKey::new(b"b".as_slice()).expect("valid key"));
        assert!(a < b);
    }

    #[test]
    fn kv_key_decoding_preserves_binary_user_key() {
        let space = ProductSpace::new("default").expect("valid space");
        let encoded = encode_kv_key(&space, &KvKey::new([0, 1, 255]).expect("valid key"));
        let decoded = decode_kv_key(&space, &encoded).expect("valid encoded key");
        assert_eq!(decoded.as_bytes(), &[0, 1, 255]);
    }

    #[test]
    fn kv_key_decoding_preserves_ascii_user_key() {
        let space = ProductSpace::new("users").expect("valid space");
        let encoded = encode_kv_key(&space, &KvKey::new(b"alice".as_slice()).expect("valid key"));
        let decoded = decode_kv_key(&space, &encoded).expect("valid encoded key");
        assert_eq!(decoded.as_bytes(), b"alice");
    }

    #[test]
    fn kv_key_space_prefix_is_not_a_valid_user_key() {
        let space = ProductSpace::new("default").expect("valid space");
        let error =
            decode_kv_key(&space, &encode_kv_space_prefix(&space)).expect_err("no user key");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.kv_key");
    }

    #[test]
    fn kv_key_decoding_rejects_malformed_rows() {
        let space = ProductSpace::new("default").expect("valid space");
        for (case, encoded) in [
            ("truncated-header", vec![1, b'k', 0]),
            ("truncated-space-length", vec![1, b'k']),
            (
                "unknown-version",
                vec![
                    2, b'k', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', b'a',
                ],
            ),
            (
                "unknown-discriminator",
                vec![
                    1, b'x', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', b'a',
                ],
            ),
            (
                "truncated-space",
                vec![1, b'k', 0, 8, b'd', b'e', b'f', b'a', b'u', b'l', b't'],
            ),
            (
                "mismatched-space",
                vec![1, b'k', 0, 5, b'o', b't', b'h', b'e', b'r', b'a'],
            ),
        ] {
            let error = decode_kv_key(&space, &encoded).expect_err(case);
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.kv_key");
        }
    }

    #[test]
    fn kv_key_decoding_rejects_control_plane_rows() {
        let space = ProductSpace::new("default").expect("valid space");
        for encoded in [
            database_identity_key(),
            local_instance_identity_key(),
            storage_registry_key(),
            capability_registry_key(),
            migration_registry_key(),
            branch_index_key(),
            branch_default_key(),
            branch_catalog_key("default"),
            branch_pending_key("feature"),
            space_index_key(),
            space_catalog_key("default"),
            reserved_space_key("_system_"),
        ] {
            let error = decode_kv_key(&space, &encoded).expect_err("control row rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.kv_key");
        }
    }

    #[test]
    fn json_key_encoding_is_deterministic_and_ordered() {
        let space = ProductSpace::new("users").expect("valid space");
        let alice = JsonDocumentId::new("alice").expect("valid document id");
        let bob = JsonDocumentId::new("bob").expect("valid document id");

        assert_eq!(
            encode_json_key(&space, &alice),
            b"\x01j\0\x05usersalice".to_vec()
        );
        assert!(encode_json_key(&space, &alice) < encode_json_key(&space, &bob));
    }

    #[test]
    fn json_key_decoding_preserves_utf8_document_ids() {
        let space = ProductSpace::new("default").expect("valid space");
        let id = JsonDocumentId::new("café").expect("valid document id");
        let decoded =
            decode_json_document_id(&space, &encode_json_key(&space, &id)).expect("decode");
        assert_eq!(decoded, id);
    }

    #[test]
    fn json_key_space_prefix_is_not_a_valid_document_key() {
        let space = ProductSpace::new("default").expect("valid space");
        let error = decode_json_document_id(&space, &encode_json_space_prefix(&space))
            .expect_err("space prefix rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.json_key");
    }

    #[test]
    fn json_key_decoding_rejects_malformed_rows() {
        let space = ProductSpace::new("default").expect("valid space");
        for (case, encoded) in [
            ("truncated-header", vec![1, b'j', 0]),
            (
                "unknown-version",
                vec![
                    2, b'j', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', b'a',
                ],
            ),
            (
                "unknown-discriminator",
                vec![
                    1, b'x', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', b'a',
                ],
            ),
            (
                "truncated-space",
                vec![1, b'j', 0, 8, b'd', b'e', b'f', b'a', b'u', b'l', b't'],
            ),
            (
                "mismatched-space",
                vec![1, b'j', 0, 5, b'o', b't', b'h', b'e', b'r', b'a'],
            ),
            (
                "invalid-utf8-id",
                vec![
                    1, b'j', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0xff,
                ],
            ),
        ] {
            let error = decode_json_document_id(&space, &encoded).expect_err(case);
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.json_key");
        }
    }

    #[test]
    fn json_key_decoding_rejects_kv_and_control_plane_rows() {
        let space = ProductSpace::new("default").expect("valid space");
        let kv = encode_kv_key(&space, &KvKey::new(b"alice".as_slice()).expect("valid key"));
        for encoded in [
            kv,
            database_identity_key(),
            local_instance_identity_key(),
            storage_registry_key(),
            capability_registry_key(),
            migration_registry_key(),
            branch_index_key(),
            branch_default_key(),
            branch_catalog_key("default"),
            branch_pending_key("feature"),
            space_index_key(),
            space_catalog_key("default"),
            reserved_space_key("_system_"),
        ] {
            let error =
                decode_json_document_id(&space, &encoded).expect_err("non-JSON row rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.json_key");
        }
    }

    #[test]
    fn json_index_keys_are_deterministic_and_decodable() {
        let space = ProductSpace::new("users").expect("valid space");
        let name = JsonIndexName::new("by_name").expect("valid index name");
        let id = JsonDocumentId::new("alice").expect("valid document id");

        assert_eq!(
            encode_json_index_meta_key(&space, &name),
            b"\x01m\0\x05users\0\x07by_name".to_vec()
        );
        assert_eq!(
            decode_json_index_name(&space, &encode_json_index_meta_key(&space, &name))
                .expect("decode"),
            name
        );
        assert_eq!(
            encode_json_index_entry_prefix(&space, &name),
            b"\x01i\0\x05users\0\x07by_name".to_vec()
        );
        assert_eq!(
            encode_json_index_entry_key(&space, &name, b"alice", &id),
            b"\x01i\0\x05users\0\x07by_namealice\xffalice".to_vec()
        );
    }

    #[test]
    fn json_index_name_decoding_rejects_malformed_metadata_keys() {
        let space = ProductSpace::new("default").expect("valid space");
        for (case, encoded) in [
            (
                "truncated-name-len",
                vec![1, b'm', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0],
            ),
            (
                "truncated-name",
                vec![
                    1, b'm', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'a',
                ],
            ),
            (
                "trailing-bytes",
                vec![
                    1, b'm', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 1, b'a', b'x',
                ],
            ),
        ] {
            let error = decode_json_index_name(&space, &encoded).expect_err(case);
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.json_index_key");
        }
    }

    #[test]
    fn event_keys_are_deterministic_and_ordered() {
        let space = ProductSpace::new("default").expect("valid space");
        let event_type = EventType::new("order.created").expect("valid event type");
        assert_eq!(
            encode_event_key(&space, EventSequence::new(7)),
            b"\x01e\0\x07default\0\0\0\0\0\0\0\x07".to_vec()
        );
        assert_eq!(
            encode_event_meta_key(&space),
            b"\x01E\0\x07defaultmeta".to_vec()
        );
        assert_eq!(
            encode_event_type_index_prefix(&space, &event_type),
            b"\x01t\0\x07default\0\rorder.created".to_vec()
        );
        let index = encode_event_type_index_key(&space, &event_type, EventSequence::new(7));
        assert_eq!(
            decode_event_type_index_key(&space, &index).expect("decode index key"),
            (event_type, EventSequence::new(7))
        );
        assert_eq!(
            decode_event_key_sequence(&space, &encode_event_key(&space, EventSequence::new(7)))
                .expect("decode event key"),
            EventSequence::new(7)
        );
        assert!(
            encode_event_key(&space, EventSequence::new(7))
                < encode_event_key(&space, EventSequence::new(8))
        );
        assert!(
            encode_event_space_prefix(&space) < encode_event_key(&space, EventSequence::new(0))
        );
    }

    #[test]
    fn event_key_decoding_preserves_separators_and_boundary_type_names() {
        let space = ProductSpace::new("default").expect("valid space");
        for event_type in [
            EventType::new("order/created:tenant.one").expect("valid event type"),
            EventType::new("e".repeat(256)).expect("valid event type"),
        ] {
            let encoded = encode_event_type_index_key(&space, &event_type, EventSequence::new(42));
            assert_eq!(
                decode_event_type_index_key(&space, &encoded).expect("decode index key"),
                (event_type, EventSequence::new(42))
            );
        }
    }

    #[test]
    fn event_key_decoding_rejects_malformed_rows() {
        let space = ProductSpace::new("default").expect("valid space");
        for (case, encoded, code, decode_type_index) in [
            (
                "event-truncated-header",
                vec![1, b'e', 0],
                "data_loss.engine.event_key",
                false,
            ),
            (
                "event-unknown-version",
                vec![
                    2, b'e', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 0, 0, 0, 0, 0, 0, 0,
                ],
                "data_loss.engine.event_key",
                false,
            ),
            (
                "event-unknown-discriminator",
                vec![
                    1, b'x', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 0, 0, 0, 0, 0, 0, 0,
                ],
                "data_loss.engine.event_key",
                false,
            ),
            (
                "event-truncated-sequence",
                vec![1, b'e', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0],
                "data_loss.engine.event_key",
                false,
            ),
            (
                "index-truncated-type-length",
                vec![1, b't', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0],
                "data_loss.engine.event_index_key",
                true,
            ),
            (
                "index-truncated-type",
                vec![
                    1, b't', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'a',
                ],
                "data_loss.engine.event_index_key",
                true,
            ),
            (
                "index-invalid-utf8",
                vec![
                    1, b't', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 1, 0xff, 0, 0, 0,
                    0, 0, 0, 0, 1,
                ],
                "data_loss.engine.event_index_key",
                true,
            ),
            (
                "index-invalid-event-type",
                vec![
                    1, b't', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 1, b' ', 0, 0, 0,
                    0, 0, 0, 0, 1,
                ],
                "data_loss.engine.event_index_key",
                true,
            ),
            (
                "index-truncated-sequence",
                vec![
                    1, b't', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 1, b'a', 0,
                ],
                "data_loss.engine.event_index_key",
                true,
            ),
        ] {
            let error = if decode_type_index {
                decode_event_sequence(&space, &encoded).expect_err(case)
            } else {
                decode_event_key_sequence(&space, &encoded).expect_err(case)
            };
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), code);
        }
    }

    #[test]
    fn event_key_decoding_rejects_other_row_families() {
        let space = ProductSpace::new("default").expect("valid space");
        let kv = encode_kv_key(&space, &KvKey::new(b"event".as_slice()).expect("valid key"));
        let json = encode_json_key(
            &space,
            &JsonDocumentId::new("event").expect("valid document id"),
        );
        let vector = encode_vector_key(
            &space,
            &VectorCollectionName::new("docs").expect("valid collection"),
            &VectorKey::new("event").expect("valid key"),
        );

        for encoded in [kv, json, vector] {
            let error =
                decode_event_key_sequence(&space, &encoded).expect_err("non-event row rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.event_key");
        }
    }

    #[test]
    fn vector_collection_keys_are_deterministic_and_decodable() {
        let space = ProductSpace::new("users").expect("valid space");
        let collection = VectorCollectionName::new("docs").expect("valid collection");

        assert_eq!(
            encode_vector_collection_key(&space, &collection),
            b"\x01c\0\x05users\0\x04docs".to_vec()
        );
        assert_eq!(
            decode_vector_collection_name(
                &space,
                &encode_vector_collection_key(&space, &collection)
            )
            .expect("decode"),
            collection
        );
        assert_eq!(
            encode_vector_collection_prefix(&space),
            b"\x01c\0\x05users".to_vec()
        );
    }

    #[test]
    fn vector_entry_keys_preserve_public_key_order() {
        let space = ProductSpace::new("users").expect("valid space");
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        let keys = ["a", "aa", "ab", "b", "ba"];
        let mut encoded = keys
            .iter()
            .map(|key| {
                encode_vector_key(
                    &space,
                    &collection,
                    &VectorKey::new(*key).expect("valid key"),
                )
            })
            .collect::<Vec<_>>();
        encoded.sort();
        let decoded = encoded
            .iter()
            .map(|key| {
                decode_vector_key(&space, key)
                    .expect("decode")
                    .1
                    .as_str()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, keys);
        assert_eq!(
            encode_vector_collection_entry_prefix(&space, &collection),
            b"\x01v\0\x05users\0\x04docs".to_vec()
        );
    }

    #[test]
    fn vector_key_decoding_preserves_separators_and_empty_keys() {
        let space = ProductSpace::new("default").expect("valid space");
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        for key in ["", "doc/1", "nested/path/key"] {
            let key = VectorKey::new(key).expect("valid key");
            let (decoded_collection, decoded_key) =
                decode_vector_key(&space, &encode_vector_key(&space, &collection, &key))
                    .expect("decode");
            assert_eq!(decoded_collection, collection);
            assert_eq!(decoded_key, key);
        }
    }

    #[test]
    fn vector_key_decoding_rejects_malformed_rows() {
        let space = ProductSpace::new("default").expect("valid space");
        for (case, encoded, code) in [
            (
                "collection-truncated-header",
                vec![1, b'c', 0],
                "data_loss.engine.vector_collection_key",
            ),
            (
                "collection-truncated-name-length",
                vec![1, b'c', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0],
                "data_loss.engine.vector_collection_key",
            ),
            (
                "collection-truncated-name",
                vec![
                    1, b'c', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd',
                ],
                "data_loss.engine.vector_collection_key",
            ),
            (
                "collection-trailing-bytes",
                vec![
                    1, b'c', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 1, b'd', b'x',
                ],
                "data_loss.engine.vector_collection_key",
            ),
            (
                "entry-truncated-collection",
                vec![
                    1, b'v', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd',
                ],
                "data_loss.engine.vector_key",
            ),
            (
                "entry-invalid-key-utf8",
                vec![
                    1, b'v', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd', b'o',
                    b'c', b's', 0xff,
                ],
                "data_loss.engine.vector_key",
            ),
            (
                "unknown-version",
                vec![
                    2, b'v', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd', b'o',
                    b'c', b's', b'a',
                ],
                "data_loss.engine.vector_key",
            ),
        ] {
            let error = if code == "data_loss.engine.vector_collection_key" {
                decode_vector_collection_name(&space, &encoded).expect_err(case)
            } else {
                decode_vector_key(&space, &encoded).expect_err(case)
            };
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), code);
        }
    }

    #[test]
    fn vector_decoding_rejects_other_row_families() {
        let space = ProductSpace::new("default").expect("valid space");
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        let vector = encode_vector_key(
            &space,
            &collection,
            &VectorKey::new("doc").expect("valid key"),
        );
        let kv = encode_kv_key(&space, &KvKey::new(b"doc".as_slice()).expect("valid key"));
        let json = encode_json_key(
            &space,
            &JsonDocumentId::new("doc").expect("valid document id"),
        );

        assert_eq!(
            decode_vector_key(&space, &kv)
                .expect_err("KV row rejected")
                .code(),
            "data_loss.engine.vector_key"
        );
        assert_eq!(
            decode_vector_key(&space, &json)
                .expect_err("JSON row rejected")
                .code(),
            "data_loss.engine.vector_key"
        );
        assert_eq!(
            decode_kv_key(&space, &vector)
                .expect_err("vector row rejected")
                .code(),
            "data_loss.engine.kv_key"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn graph_keys_are_deterministic_and_decodable() {
        let space = ProductSpace::new("users").expect("valid space");
        let graph = GraphName::new("deps").expect("valid graph");
        let src = GraphNodeId::new("doc").expect("valid source");
        let dst = GraphNodeId::new("chunk").expect("valid destination");
        let edge_type = GraphEdgeType::new("contains").expect("valid edge type");
        let target = GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            None,
            ProductSpace::new("docs").expect("valid bound space"),
            "doc-1",
        )
        .expect("valid target");

        assert_eq!(
            encode_graph_metadata_key(&space, &graph),
            b"\x01g\0\x05users\0\x04deps".to_vec()
        );
        assert_eq!(
            encode_graph_metadata_prefix(&space),
            b"\x01g\0\x05users".to_vec()
        );
        assert_eq!(
            decode_graph_metadata_key(&space, &encode_graph_metadata_key(&space, &graph))
                .expect("decode graph"),
            graph
        );

        assert_eq!(
            encode_graph_node_key(&space, &graph, &src),
            b"\x01n\0\x05users\0\x04deps\0\x03doc".to_vec()
        );
        assert_eq!(
            encode_graph_node_prefix(&space, &graph),
            b"\x01n\0\x05users\0\x04deps".to_vec()
        );
        assert_eq!(
            decode_graph_node_key(&space, &encode_graph_node_key(&space, &graph, &src))
                .expect("decode node"),
            (graph.clone(), src.clone())
        );

        let edge_key = encode_graph_edge_key(&space, &graph, &src, &edge_type, &dst);
        assert_eq!(
            decode_graph_edge_key(&space, &edge_key).expect("decode edge"),
            (graph.clone(), src.clone(), edge_type.clone(), dst.clone())
        );
        assert_eq!(
            encode_graph_edge_prefix(&space, &graph),
            b"\x01o\0\x05users\0\x04deps".to_vec()
        );

        let reverse_key = encode_graph_reverse_edge_key(&space, &graph, &dst, &edge_type, &src);
        assert_eq!(
            decode_graph_reverse_edge_key(&space, &reverse_key).expect("decode reverse edge"),
            (graph.clone(), dst, edge_type, src.clone())
        );
        assert_eq!(
            encode_graph_reverse_edge_prefix(&space, &graph),
            b"\x01r\0\x05users\0\x04deps".to_vec()
        );

        let binding_key = encode_graph_binding_key(&space, &target, &graph, &src);
        assert_eq!(
            decode_graph_binding_key(&space, &binding_key).expect("decode binding"),
            (target.clone(), graph.clone(), src.clone())
        );
        assert!(binding_key.starts_with(&encode_graph_binding_target_prefix(&space, &target)));

        let max_graph = GraphName::new("g".repeat(256)).expect("max graph");
        let max_src = GraphNodeId::new("s".repeat(1024)).expect("max source");
        let max_dst = GraphNodeId::new("d".repeat(1024)).expect("max destination");
        let max_edge_type = GraphEdgeType::new("e".repeat(256)).expect("max edge type");
        let max_target = GraphBindingTarget::new(
            GraphBindingPrimitive::Graph,
            None,
            ProductSpace::new("bound").expect("valid bound space"),
            "b".repeat(1024),
        )
        .expect("max binding target");

        assert_eq!(
            decode_graph_metadata_key(&space, &encode_graph_metadata_key(&space, &max_graph))
                .expect("decode max graph"),
            max_graph
        );
        assert_eq!(
            decode_graph_node_key(&space, &encode_graph_node_key(&space, &max_graph, &max_src))
                .expect("decode max node"),
            (max_graph.clone(), max_src.clone())
        );
        assert_eq!(
            decode_graph_edge_key(
                &space,
                &encode_graph_edge_key(&space, &max_graph, &max_src, &max_edge_type, &max_dst,)
            )
            .expect("decode max edge"),
            (
                max_graph.clone(),
                max_src.clone(),
                max_edge_type.clone(),
                max_dst.clone()
            )
        );
        assert_eq!(
            decode_graph_reverse_edge_key(
                &space,
                &encode_graph_reverse_edge_key(
                    &space,
                    &max_graph,
                    &max_dst,
                    &max_edge_type,
                    &max_src,
                )
            )
            .expect("decode max reverse edge"),
            (max_graph.clone(), max_dst, max_edge_type, max_src.clone())
        );
        assert_eq!(
            decode_graph_binding_key(
                &space,
                &encode_graph_binding_key(&space, &max_target, &max_graph, &max_src)
            )
            .expect("decode max binding"),
            (max_target, max_graph, max_src)
        );
    }

    #[test]
    fn graph_key_decoding_rejects_malformed_rows() {
        let space = ProductSpace::new("default").expect("valid space");
        for (case, encoded, code) in [
            (
                "metadata-truncated-graph",
                vec![
                    1, b'g', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd',
                ],
                "data_loss.engine.graph_key",
            ),
            (
                "node-truncated-node",
                vec![
                    1, b'n', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd', b'e',
                    b'p', b's', 0,
                ],
                "data_loss.engine.graph_node_key",
            ),
            (
                "edge-truncated-type",
                vec![
                    1, b'o', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd', b'e',
                    b'p', b's', 0, 3, b'd', b'o', b'c', 0,
                ],
                "data_loss.engine.graph_edge_key",
            ),
            (
                "reverse-truncated-type",
                vec![
                    1, b'r', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 4, b'd', b'e',
                    b'p', b's', 0, 5, b'c', b'h', b'u', b'n', b'k', 0,
                ],
                "data_loss.engine.graph_reverse_edge_key",
            ),
            (
                "binding-unknown-primitive",
                vec![
                    1, b'b', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 7, b'u', b'n',
                    b'k', b'n', b'o', b'w', b'n',
                ],
                "data_loss.engine.graph_binding_key",
            ),
        ] {
            let error = match code {
                "data_loss.engine.graph_key" => {
                    decode_graph_metadata_key(&space, &encoded).expect_err(case)
                }
                "data_loss.engine.graph_node_key" => {
                    decode_graph_node_key(&space, &encoded).expect_err(case)
                }
                "data_loss.engine.graph_edge_key" => {
                    decode_graph_edge_key(&space, &encoded).expect_err(case)
                }
                "data_loss.engine.graph_reverse_edge_key" => {
                    decode_graph_reverse_edge_key(&space, &encoded).expect_err(case)
                }
                "data_loss.engine.graph_binding_key" => {
                    decode_graph_binding_key(&space, &encoded).expect_err(case)
                }
                _ => unreachable!(),
            };
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), code);
        }
    }

    #[test]
    fn graph_key_decoding_rejects_wrong_family_version_and_utf8() {
        let space = ProductSpace::new("default").expect("valid space");
        let graph = GraphName::new("deps").expect("valid graph");
        let kv_key = KvKey::new(b"deps".as_slice()).expect("valid KV key");
        let json_id = JsonDocumentId::new("doc").expect("valid JSON id");
        let vector_collection = VectorCollectionName::new("vectors").expect("valid collection");

        let mut wrong_version = encode_graph_metadata_key(&space, &graph);
        wrong_version[0] = 2;
        let error =
            decode_graph_metadata_key(&space, &wrong_version).expect_err("version rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_key");

        for (case, wrong_family) in [
            ("kv", encode_kv_key(&space, &kv_key)),
            ("json", encode_json_key(&space, &json_id)),
            ("event", encode_event_key(&space, EventSequence::new(1))),
            (
                "vector",
                encode_vector_collection_key(&space, &vector_collection),
            ),
            ("control", storage_registry_key()),
        ] {
            let error = decode_graph_metadata_key(&space, &wrong_family).expect_err(case);
            assert_eq!(error.class(), EngineErrorClass::Corruption, "{case}");
            assert_eq!(error.code(), "data_loss.engine.graph_key", "{case}");
        }

        let invalid_utf8 = vec![
            1, b'g', 0, 7, b'd', b'e', b'f', b'a', b'u', b'l', b't', 0, 1, 0xff,
        ];
        let error = decode_graph_metadata_key(&space, &invalid_utf8).expect_err("UTF-8 rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_key");
    }

    #[test]
    fn control_row_keys_are_deterministic() {
        assert_eq!(database_identity_key(), b"\x01identity".to_vec());
        assert_eq!(
            local_instance_identity_key(),
            b"\x01identity:local-instance".to_vec()
        );
        assert_eq!(
            storage_registry_key(),
            b"\x01registry:storage-spaces".to_vec()
        );
        assert_eq!(
            capability_registry_key(),
            b"\x01registry:capabilities".to_vec()
        );
        assert_eq!(
            migration_registry_key(),
            b"\x01registry:migrations".to_vec()
        );
        assert_eq!(branch_index_key(), b"\x01branch:index".to_vec());
        assert_eq!(branch_default_key(), b"\x01branch:default".to_vec());
        assert_eq!(
            branch_catalog_key("default"),
            b"\x01branch:\0\x07default".to_vec()
        );
        assert_eq!(
            branch_pending_key("feature"),
            b"\x01branch:pending:\0\x07feature".to_vec()
        );
        assert_eq!(space_index_key(), b"\x01space:index".to_vec());
        assert_eq!(
            space_catalog_key("default"),
            b"\x01space:\0\x07default".to_vec()
        );
        assert_eq!(
            reserved_space_key("_system_"),
            b"\x01space:reserved:\0\x08_system_".to_vec()
        );
        assert_eq!(
            vector_index_manifest_key(
                &ProductSpace::new("default").expect("valid space"),
                &VectorCollectionName::new("docs").expect("valid collection")
            ),
            b"\x01vector:index-manifest:\0\x07default\0\x04docs".to_vec()
        );
        assert_eq!(
            vector_index_manifest_prefix(),
            b"\x01vector:index-manifest:".to_vec()
        );
        assert_eq!(
            decode_vector_index_manifest_key(&vector_index_manifest_key(
                &ProductSpace::new("default").expect("valid space"),
                &VectorCollectionName::new("docs").expect("valid collection")
            ))
            .expect("manifest key decodes"),
            (
                ProductSpace::new("default").expect("valid space"),
                VectorCollectionName::new("docs").expect("valid collection")
            )
        );
    }
}
