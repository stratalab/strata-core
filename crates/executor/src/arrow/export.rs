//! Arrow file export through executor commands.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    FixedSizeListBuilder, Float32Builder, Float64Builder, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use serde_json::Value;

use crate::error::ExecutorError;
use crate::output::Output;
use crate::types::{
    ArrowExportPrimitive, ArrowExportResult, ArrowFileFormat, Bytes, EventRangeDirection,
    GraphDirection, GraphEdgeDataOutput, GraphNodeDataOutput, VectorCollectionInfo,
};
use crate::{Command, Executor};

use super::writer::write_file;
use super::{internal_error, invalid_input, required_option, unexpected_output};

const PAGE_LIMIT: u64 = 1_000;

#[allow(clippy::too_many_arguments)]
pub(crate) fn export_file(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    primitive: ArrowExportPrimitive,
    format: ArrowFileFormat,
    path: String,
    prefix: Option<&str>,
    limit: Option<u64>,
    collection: Option<String>,
    graph: Option<String>,
    event_type: Option<String>,
) -> Result<Output, ExecutorError> {
    let branch = branch.map(str::to_owned);
    let space = space.map(str::to_owned);
    let path = PathBuf::from(path);
    let mut paths = Vec::new();
    let mut size_bytes = 0;
    let row_count;

    // CSV cannot serialize a vector embedding column (a nested FixedSizeList); the
    // write only failed after truncating the output file and as a *retryable* IO
    // error, so a retrying caller looped forever (#3082). Reject the combination
    // up front, before any file is opened.
    if matches!(
        (primitive, format),
        (ArrowExportPrimitive::Vector, ArrowFileFormat::Csv)
    ) {
        return Err(invalid_input(
            "invalid_argument.executor.arrow_format",
            "vector export to CSV is not supported: embeddings are nested list columns CSV cannot represent; export vectors as jsonl or parquet",
        ));
    }

    match primitive {
        ArrowExportPrimitive::Kv => {
            let batch = export_kv(executor, branch.as_deref(), space.as_deref(), prefix, limit)?;
            row_count = batch.num_rows() as u64;
            size_bytes += write_file(&path, format, &[batch])?;
            paths.push(path_to_string(&path));
        }
        ArrowExportPrimitive::Json => {
            let batch = export_json(executor, branch.as_deref(), space.as_deref(), prefix, limit)?;
            row_count = batch.num_rows() as u64;
            size_bytes += write_file(&path, format, &[batch])?;
            paths.push(path_to_string(&path));
        }
        ArrowExportPrimitive::Event => {
            let batch = export_events(executor, branch, space, event_type, limit)?;
            row_count = batch.num_rows() as u64;
            size_bytes += write_file(&path, format, &[batch])?;
            paths.push(path_to_string(&path));
        }
        ArrowExportPrimitive::Vector => {
            let collection = required_option(
                collection,
                "invalid_argument.executor.arrow_collection",
                "vector Arrow export requires a collection",
            )?;
            let batch = export_vector(
                executor,
                branch.as_deref(),
                space.as_deref(),
                &collection,
                prefix,
                limit,
            )?;
            row_count = batch.num_rows() as u64;
            size_bytes += write_file(&path, format, &[batch])?;
            paths.push(path_to_string(&path));
        }
        ArrowExportPrimitive::Graph => {
            let graph = required_option(
                graph,
                "invalid_argument.executor.arrow_graph",
                "graph Arrow export requires a graph",
            )?;
            let (node_path, edge_path) = graph_paths(&path, format);
            let node_batch = export_graph_nodes(
                executor,
                branch.as_deref(),
                space.as_deref(),
                &graph,
                prefix,
                limit,
            )?;
            let edge_batch =
                export_graph_edges(executor, branch.as_deref(), space.as_deref(), &graph, limit)?;
            row_count = (node_batch.num_rows() + edge_batch.num_rows()) as u64;
            size_bytes += write_file(&node_path, format, &[node_batch])?;
            size_bytes += write_file(&edge_path, format, &[edge_batch])?;
            paths.push(path_to_string(&node_path));
            paths.push(path_to_string(&edge_path));
        }
    }

    Ok(Output::ArrowExportResult(ArrowExportResult::new(
        primitive, format, paths, row_count, size_bytes,
    )))
}

fn export_kv(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<RecordBatch, ExecutorError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("key_encoding", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("value_encoding", DataType::Utf8, false),
        Field::new("version", DataType::UInt64, true),
        Field::new("timestamp", DataType::UInt64, true),
    ]));
    let mut key_builder = StringBuilder::new();
    let mut key_encoding_builder = StringBuilder::new();
    let mut value_builder = StringBuilder::new();
    let mut value_encoding_builder = StringBuilder::new();
    let mut version_builder = UInt64Builder::new();
    let mut timestamp_builder = UInt64Builder::new();
    let max = limit.unwrap_or(u64::MAX);
    let mut cursor = None;
    let mut written = 0_u64;

    while written < max {
        let page_limit = PAGE_LIMIT.min(max - written);
        let output = executor.execute(Command::KvList {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            prefix: prefix.map(Bytes::from),
            cursor,
            limit: Some(page_limit),
            as_of: None,
            as_of_time: None,
        })?;
        let (keys, has_more, next_cursor) = match output {
            Output::KeysPage { items: keys, page } => {
                (keys, page.has_more(), page.cursor().cloned())
            }
            _ => return Err(unexpected_output("kv_list")),
        };
        if keys.is_empty() {
            break;
        }
        let output = executor.execute(Command::KvBatchGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            keys,
        })?;
        let Output::BatchGetResults(results) = output else {
            return Err(unexpected_output("kv_batch_get"));
        };
        for item in results.items() {
            if written >= max {
                break;
            }
            let Some(result) = item.result() else {
                continue;
            };
            if let Some(value) = result.value() {
                let (key_text, key_encoding) = encode_bytes(result.key().as_slice());
                let (value_text, value_encoding) = encode_bytes(value.as_slice());
                key_builder.append_value(key_text);
                key_encoding_builder.append_value(key_encoding);
                value_builder.append_value(value_text);
                value_encoding_builder.append_value(value_encoding);
                append_u64_option(&mut version_builder, result.version());
                append_u64_option(&mut timestamp_builder, result.timestamp());
                written += 1;
            }
        }
        if !has_more {
            break;
        }
        cursor = next_cursor;
    }

    record_batch(
        schema,
        vec![
            Arc::new(key_builder.finish()),
            Arc::new(key_encoding_builder.finish()),
            Arc::new(value_builder.finish()),
            Arc::new(value_encoding_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(timestamp_builder.finish()),
        ],
        "KV",
    )
}

fn export_json(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<RecordBatch, ExecutorError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("document", DataType::Utf8, false),
        Field::new("version", DataType::UInt64, true),
        Field::new("timestamp", DataType::UInt64, true),
        Field::new("document_version", DataType::UInt64, true),
    ]));
    let mut key_builder = StringBuilder::new();
    let mut document_builder = StringBuilder::new();
    let mut version_builder = UInt64Builder::new();
    let mut timestamp_builder = UInt64Builder::new();
    let mut document_version_builder = UInt64Builder::new();
    let max = limit.unwrap_or(u64::MAX);
    let mut cursor = None;
    let mut written = 0_u64;

    while written < max {
        let page_limit = PAGE_LIMIT.min(max - written);
        let output = executor.execute(Command::JsonList {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            prefix: prefix.map(str::to_owned),
            cursor,
            limit: Some(page_limit),
            as_of: None,
            as_of_time: None,
        })?;
        let Output::JsonListResult { items: keys, page } = output else {
            return Err(unexpected_output("json_list"));
        };
        let has_more = page.has_more();
        let next_cursor = page.cursor().cloned();
        if keys.is_empty() {
            break;
        }
        for key in keys {
            if written >= max {
                break;
            }
            let output = executor.execute(Command::JsonGet {
                branch: branch.map(str::to_owned),
                space: space.map(str::to_owned),
                key: key.clone(),
                path: "$".to_owned(),
                as_of: None,
                as_of_time: None,
            })?;
            let Output::JsonVersionedValue(value) = output else {
                return Err(unexpected_output("json_get"));
            };
            if let Some(value) = value.value() {
                key_builder.append_value(key);
                document_builder.append_value(json_to_string(value.value())?);
                version_builder.append_value(value.version());
                timestamp_builder.append_value(value.timestamp());
                document_version_builder.append_value(value.document_version());
                written += 1;
            }
        }
        if !has_more {
            break;
        }
        cursor = next_cursor;
    }

    record_batch(
        schema,
        vec![
            Arc::new(key_builder.finish()),
            Arc::new(document_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(timestamp_builder.finish()),
            Arc::new(document_version_builder.finish()),
        ],
        "JSON",
    )
}

fn export_events(
    executor: &mut Executor,
    branch: Option<String>,
    space: Option<String>,
    event_type: Option<String>,
    limit: Option<u64>,
) -> Result<RecordBatch, ExecutorError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("event_timestamp", DataType::UInt64, false),
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("previous_hash", DataType::Utf8, false),
        Field::new("hash", DataType::Utf8, false),
    ]));
    let output = executor.execute(Command::EventRange {
        branch,
        space,
        start_seq: 0,
        end_seq: None,
        limit,
        direction: EventRangeDirection::Forward,
        event_type,
    })?;
    let Output::EventRangeResult { items: events, .. } = output else {
        return Err(unexpected_output("event_range"));
    };
    let mut sequence_builder = UInt64Builder::new();
    let mut event_type_builder = StringBuilder::new();
    let mut payload_builder = StringBuilder::new();
    let mut event_timestamp_builder = UInt64Builder::new();
    let mut version_builder = UInt64Builder::new();
    let mut timestamp_builder = UInt64Builder::new();
    let mut previous_hash_builder = StringBuilder::new();
    let mut hash_builder = StringBuilder::new();
    for event in events {
        sequence_builder.append_value(event.event().sequence());
        event_type_builder.append_value(event.event().event_type());
        payload_builder.append_value(json_to_string(event.event().payload())?);
        event_timestamp_builder.append_value(event.event().timestamp());
        version_builder.append_value(event.version());
        timestamp_builder.append_value(event.timestamp());
        previous_hash_builder.append_value(event.event().previous_hash());
        hash_builder.append_value(event.event().hash());
    }
    record_batch(
        schema,
        vec![
            Arc::new(sequence_builder.finish()),
            Arc::new(event_type_builder.finish()),
            Arc::new(payload_builder.finish()),
            Arc::new(event_timestamp_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(timestamp_builder.finish()),
            Arc::new(previous_hash_builder.finish()),
            Arc::new(hash_builder.finish()),
        ],
        "event",
    )
}

fn export_vector(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<RecordBatch, ExecutorError> {
    let info = vector_collection_info(executor, branch, space, collection)?;
    let dimension = i32::try_from(info.dimension()).map_err(|_| {
        invalid_input(
            "invalid_argument.executor.arrow_vector_dimension",
            "vector dimension does not fit Arrow FixedSizeList",
        )
    })?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        ),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("vector_revision", DataType::UInt64, false),
    ]));
    let mut key_builder = StringBuilder::new();
    let mut embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), dimension);
    let mut metadata_builder = StringBuilder::new();
    let mut version_builder = UInt64Builder::new();
    let mut timestamp_builder = UInt64Builder::new();
    let mut vector_revision_builder = UInt64Builder::new();
    let mut cursor = None;
    let max = limit.unwrap_or(u64::MAX);
    let mut written = 0_u64;

    while written < max {
        let page_limit = PAGE_LIMIT.min(max - written);
        let output = executor.execute(Command::VectorListKeys {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            prefix: prefix.map(str::to_owned),
            cursor,
            limit: Some(page_limit),
            as_of: None,
            as_of_time: None,
        })?;
        let Output::VectorKeyPage { items: keys, page } = output else {
            return Err(unexpected_output("vector_list_keys"));
        };
        let has_more = page.has_more();
        let next_cursor = page.cursor().cloned();
        if keys.is_empty() {
            break;
        }
        let output = executor.execute(Command::VectorBatchGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            keys,
        })?;
        let Output::VectorBatchGetResults(results) = output else {
            return Err(unexpected_output("vector_batch_get"));
        };
        for item in results.items() {
            if written >= max {
                break;
            }
            let Some(result) = item.result() else {
                continue;
            };
            let Some(value) = result.value() else {
                continue;
            };
            key_builder.append_value(value.key());
            let values = embedding_builder.values();
            for embedding_value in value.data().embedding() {
                values.append_value(*embedding_value);
            }
            embedding_builder.append(true);
            if let Some(metadata) = value.data().metadata() {
                metadata_builder.append_value(json_to_string(metadata)?);
            } else {
                metadata_builder.append_null();
            }
            version_builder.append_value(value.version());
            timestamp_builder.append_value(value.timestamp());
            vector_revision_builder.append_value(value.vector_revision());
            written += 1;
        }
        if !has_more {
            break;
        }
        cursor = next_cursor;
    }

    record_batch(
        schema,
        vec![
            Arc::new(key_builder.finish()),
            Arc::new(embedding_builder.finish()),
            Arc::new(metadata_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(timestamp_builder.finish()),
            Arc::new(vector_revision_builder.finish()),
        ],
        "vector",
    )
}

fn export_graph_nodes(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<RecordBatch, ExecutorError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("graph", DataType::Utf8, false),
        Field::new("node_id", DataType::Utf8, false),
        Field::new("properties", DataType::Utf8, true),
        Field::new("binding", DataType::Utf8, true),
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
    ]));
    let mut graph_builder = StringBuilder::new();
    let mut node_builder = StringBuilder::new();
    let mut properties_builder = StringBuilder::new();
    let mut binding_builder = StringBuilder::new();
    let mut version_builder = UInt64Builder::new();
    let mut timestamp_builder = UInt64Builder::new();
    let mut cursor = None;
    let max = limit.unwrap_or(u64::MAX);
    let mut written = 0_u64;

    while written < max {
        let page_limit = PAGE_LIMIT.min(max - written);
        let output = executor.execute(Command::GraphListNodes {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            prefix: prefix.map(str::to_owned),
            cursor,
            limit: Some(page_limit),
            as_of: None,
            as_of_time: None,
        })?;
        let Output::GraphNodePage { items: nodes, page } = output else {
            return Err(unexpected_output("graph_list_nodes"));
        };
        let has_more = page.has_more();
        let next_cursor = page.cursor().cloned();
        if nodes.is_empty() {
            break;
        }
        for node in nodes {
            append_graph_node(
                &node,
                &mut graph_builder,
                &mut node_builder,
                &mut properties_builder,
                &mut binding_builder,
                &mut version_builder,
                &mut timestamp_builder,
            )?;
            written += 1;
        }
        if !has_more {
            break;
        }
        cursor = next_cursor;
    }
    record_batch(
        schema,
        vec![
            Arc::new(graph_builder.finish()),
            Arc::new(node_builder.finish()),
            Arc::new(properties_builder.finish()),
            Arc::new(binding_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(timestamp_builder.finish()),
        ],
        "graph_nodes",
    )
}

fn export_graph_edges(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    limit: Option<u64>,
) -> Result<RecordBatch, ExecutorError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("graph", DataType::Utf8, false),
        Field::new("src", DataType::Utf8, false),
        Field::new("edge_type", DataType::Utf8, false),
        Field::new("dst", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("properties", DataType::Utf8, true),
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
    ]));
    let mut graph_builder = StringBuilder::new();
    let mut src_builder = StringBuilder::new();
    let mut edge_type_builder = StringBuilder::new();
    let mut dst_builder = StringBuilder::new();
    let mut weight_builder = Float64Builder::new();
    let mut properties_builder = StringBuilder::new();
    let mut version_builder = UInt64Builder::new();
    let mut timestamp_builder = UInt64Builder::new();
    let mut edge_keys = BTreeSet::new();
    let node_ids = graph_node_ids(executor, branch, space, graph)?;
    let max = limit.unwrap_or(u64::MAX);
    let mut written = 0_u64;

    for node_id in node_ids {
        if written >= max {
            break;
        }
        let mut cursor = None;
        loop {
            let page_limit = PAGE_LIMIT.min(max - written);
            let output = executor.execute(Command::GraphNeighbors {
                branch: branch.map(str::to_owned),
                space: space.map(str::to_owned),
                graph: graph.to_owned(),
                node_id: node_id.clone(),
                direction: GraphDirection::Outgoing,
                edge_type: None,
                cursor,
                limit: Some(page_limit),
                as_of: None,
                as_of_time: None,
            })?;
            let Output::GraphNeighborPage {
                items: neighbors,
                page,
            } = output
            else {
                return Err(unexpected_output("graph_neighbors"));
            };
            let has_more = page.has_more();
            let next_cursor = page.cursor().cloned();
            if neighbors.is_empty() {
                break;
            }
            for neighbor in neighbors {
                if written >= max {
                    break;
                }
                let edge = neighbor.edge();
                let edge_key = (
                    edge.graph().to_owned(),
                    edge.src().to_owned(),
                    edge.edge_type().to_owned(),
                    edge.dst().to_owned(),
                );
                if edge_keys.insert(edge_key) {
                    append_graph_edge(
                        edge,
                        &mut graph_builder,
                        &mut src_builder,
                        &mut edge_type_builder,
                        &mut dst_builder,
                        &mut weight_builder,
                        &mut properties_builder,
                        &mut version_builder,
                        &mut timestamp_builder,
                    )?;
                    written += 1;
                }
            }
            if !has_more || written >= max {
                break;
            }
            cursor = next_cursor;
        }
    }
    record_batch(
        schema,
        vec![
            Arc::new(graph_builder.finish()),
            Arc::new(src_builder.finish()),
            Arc::new(edge_type_builder.finish()),
            Arc::new(dst_builder.finish()),
            Arc::new(weight_builder.finish()),
            Arc::new(properties_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(timestamp_builder.finish()),
        ],
        "graph_edges",
    )
}

fn vector_collection_info(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
) -> Result<VectorCollectionInfo, ExecutorError> {
    let output = executor.execute(Command::VectorCollectionStats {
        branch: branch.map(str::to_owned),
        space: space.map(str::to_owned),
        collection: collection.to_owned(),
    })?;
    let Output::VectorCollectionList {
        items: mut collections,
        ..
    } = output
    else {
        return Err(unexpected_output("vector_collection_stats"));
    };
    collections.pop().ok_or_else(|| {
        invalid_input(
            "invalid_argument.executor.arrow_collection",
            "vector collection stats returned no collection",
        )
    })
}

fn graph_node_ids(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
) -> Result<Vec<String>, ExecutorError> {
    let mut node_ids = Vec::new();
    let mut cursor = None;
    loop {
        let output = executor.execute(Command::GraphListNodes {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            prefix: None,
            cursor,
            limit: Some(PAGE_LIMIT),
            as_of: None,
            as_of_time: None,
        })?;
        let Output::GraphNodePage { items: nodes, page } = output else {
            return Err(unexpected_output("graph_list_nodes"));
        };
        let has_more = page.has_more();
        let next_cursor = page.cursor().cloned();
        if nodes.is_empty() {
            break;
        }
        node_ids.extend(nodes.into_iter().map(|node| node.node_id().to_owned()));
        if !has_more {
            break;
        }
        cursor = next_cursor;
    }
    Ok(node_ids)
}

#[allow(clippy::too_many_arguments)]
fn append_graph_node(
    node: &GraphNodeDataOutput,
    graph_builder: &mut StringBuilder,
    node_builder: &mut StringBuilder,
    properties_builder: &mut StringBuilder,
    binding_builder: &mut StringBuilder,
    version_builder: &mut UInt64Builder,
    timestamp_builder: &mut UInt64Builder,
) -> Result<(), ExecutorError> {
    graph_builder.append_value(node.graph());
    node_builder.append_value(node.node_id());
    if let Some(properties) = node.properties() {
        properties_builder.append_value(json_to_string(properties)?);
    } else {
        properties_builder.append_null();
    }
    if let Some(binding) = node.binding() {
        binding_builder.append_value(json_to_string(&serde_json::to_value(binding).map_err(
            |error| internal_error(format!("failed to serialize graph binding: {error}")),
        )?)?);
    } else {
        binding_builder.append_null();
    }
    version_builder.append_value(node.version());
    timestamp_builder.append_value(node.timestamp());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_graph_edge(
    edge: &GraphEdgeDataOutput,
    graph_builder: &mut StringBuilder,
    src_builder: &mut StringBuilder,
    edge_type_builder: &mut StringBuilder,
    dst_builder: &mut StringBuilder,
    weight_builder: &mut Float64Builder,
    properties_builder: &mut StringBuilder,
    version_builder: &mut UInt64Builder,
    timestamp_builder: &mut UInt64Builder,
) -> Result<(), ExecutorError> {
    graph_builder.append_value(edge.graph());
    src_builder.append_value(edge.src());
    edge_type_builder.append_value(edge.edge_type());
    dst_builder.append_value(edge.dst());
    weight_builder.append_value(edge.weight());
    if let Some(properties) = edge.properties() {
        properties_builder.append_value(json_to_string(properties)?);
    } else {
        properties_builder.append_null();
    }
    version_builder.append_value(edge.version());
    timestamp_builder.append_value(edge.timestamp());
    Ok(())
}

fn record_batch(
    schema: Arc<Schema>,
    columns: Vec<Arc<dyn arrow::array::Array>>,
    label: &str,
) -> Result<RecordBatch, ExecutorError> {
    RecordBatch::try_new(schema, columns)
        .map_err(|error| internal_error(format!("failed to build {label} RecordBatch: {error}")))
}

fn append_u64_option(builder: &mut UInt64Builder, value: Option<u64>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

fn encode_bytes(bytes: &[u8]) -> (String, &'static str) {
    match std::str::from_utf8(bytes) {
        Ok(value) => (value.to_owned(), "utf8"),
        Err(_) => (
            base64::engine::general_purpose::STANDARD.encode(bytes),
            "base64",
        ),
    }
}

fn json_to_string(value: &Value) -> Result<String, ExecutorError> {
    serde_json::to_string(value)
        .map_err(|error| internal_error(format!("failed to serialize JSON value: {error}")))
}

pub(super) fn graph_paths(path: &Path, format: ArrowFileFormat) -> (PathBuf, PathBuf) {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map_or_else(|| format_extension(format).to_owned(), str::to_owned);
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("graph");
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    (
        parent.join(format!("{stem}_nodes.{extension}")),
        parent.join(format!("{stem}_edges.{extension}")),
    )
}

const fn format_extension(format: ArrowFileFormat) -> &'static str {
    match format {
        ArrowFileFormat::Parquet => "parquet",
        ArrowFileFormat::Csv => "csv",
        ArrowFileFormat::Jsonl => "jsonl",
    }
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
