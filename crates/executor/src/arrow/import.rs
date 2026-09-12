//! Arrow file import through executor commands.

use std::path::{Path, PathBuf};

use arrow::array::{Array, Float64Array, StringArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use crate::error::ExecutorError;
use crate::output::Output;
use crate::types::{
    ArrowFileFormat, ArrowImportResult, ArrowImportTarget, BatchEventEntry, BatchJsonEntry,
    BatchKvEntry, BatchVectorEntry, Bytes, GraphBatchOperation, GraphEdgeData, GraphEntityBinding,
    GraphNodeData,
};
use crate::{Command, Executor};

use super::format::detect_format;
use super::reader::read_file;
use super::schema::{
    json_document, key_bytes, resolve_mapping, value_bytes, vector_embedding, vector_metadata,
};
use super::{internal_error, invalid_input, not_found, required_option, unexpected_output};

#[allow(clippy::too_many_arguments)]
pub(crate) fn import_file(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    file_path: String,
    format: Option<ArrowFileFormat>,
    target: ArrowImportTarget,
    key_column: Option<&str>,
    value_column: Option<&str>,
    collection: Option<&str>,
    graph: Option<&str>,
) -> Result<Output, ExecutorError> {
    let path = PathBuf::from(&file_path);
    let format = match format {
        Some(format) => format,
        None => detect_format(&path)?,
    };

    // #3083: a flag that does not apply to the target must be rejected, not
    // silently ignored.
    validate_import_flags(target, collection.is_some(), graph.is_some())?;

    // Graph import reads two files (nodes + edges) with dedicated schemas, so it
    // bypasses the generic single-file `read_file`/`resolve_mapping` path that
    // kv/json/vector share.
    if let ArrowImportTarget::Graph = target {
        let result = import_graph(executor, branch, space, graph, &path, format)?;
        return Ok(Output::ArrowImportResult(ArrowImportResult::new(
            target,
            file_path,
            result.rows_imported,
            result.rows_skipped,
            result.batches_processed,
        )));
    }

    let (schema, batches) = read_file(&path, format)?;

    // #3083: a 0-row file (e.g. an empty export) infers a schema with no columns;
    // importing it is a no-op, not a "no key column found" error. Short-circuit
    // before any target-specific column mapping so a 0-row export round-trips to
    // zero imported rows.
    if batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(Output::ArrowImportResult(ArrowImportResult::new(
            target,
            file_path,
            0,
            0,
            batches.len() as u64,
        )));
    }

    // Event import reads a single file with a fixed event schema (not the
    // key/value column mapping kv/json/vector share), and replays each row as an
    // ordinary append — Arrow is an analytics interchange, so the log is
    // re-derived (fresh sequence/timestamp/hash); clone artifacts are the
    // lossless backup path.
    if let ArrowImportTarget::Event = target {
        let result = import_event(executor, branch, space, &schema, &batches)?;
        return Ok(Output::ArrowImportResult(ArrowImportResult::new(
            target,
            file_path,
            result.rows_imported,
            result.rows_skipped,
            result.batches_processed,
        )));
    }

    let mapping = resolve_mapping(&schema, target, key_column, value_column)?;

    let result = match target {
        ArrowImportTarget::Kv => import_kv(executor, branch, space, &batches, &mapping)?,
        ArrowImportTarget::Json => import_json(executor, branch, space, &batches, &mapping)?,
        ArrowImportTarget::Vector => {
            let collection = required_option(
                collection,
                "invalid_argument.executor.arrow_collection",
                "vector Arrow import requires a collection",
            )?;
            import_vector(executor, branch, space, collection, &batches, &mapping)?
        }
        // Graph and Event return above, before the column-mapping path; these
        // arms keep the match exhaustive without a panicking `unreachable!()`.
        ArrowImportTarget::Graph => {
            return Err(internal_error(
                "graph Arrow import is handled before column mapping",
            ));
        }
        ArrowImportTarget::Event => {
            return Err(internal_error(
                "event Arrow import is handled before column mapping",
            ));
        }
    };

    Ok(Output::ArrowImportResult(ArrowImportResult::new(
        target,
        file_path,
        result.rows_imported,
        result.rows_skipped,
        result.batches_processed,
    )))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ImportCounts {
    rows_imported: u64,
    rows_skipped: u64,
    batches_processed: u64,
}

/// Rejects import flags that do not apply to the target (#3083): `--collection`
/// is only meaningful for a vector import, `--graph` only for a graph import.
/// Accepting them silently hid a caller mistake.
fn validate_import_flags(
    target: ArrowImportTarget,
    has_collection: bool,
    has_graph: bool,
) -> Result<(), ExecutorError> {
    if has_collection && !matches!(target, ArrowImportTarget::Vector) {
        return Err(invalid_input(
            "invalid_argument.executor.arrow_collection",
            "a collection is only valid for a vector import",
        ));
    }
    if has_graph && !matches!(target, ArrowImportTarget::Graph) {
        return Err(invalid_input(
            "invalid_argument.executor.arrow_graph",
            "a graph is only valid for a graph import",
        ));
    }
    Ok(())
}

fn import_kv(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    batches: &[arrow::record_batch::RecordBatch],
    mapping: &super::schema::ImportMapping,
) -> Result<ImportCounts, ExecutorError> {
    let mut counts = ImportCounts::default();
    for batch in batches {
        let mut entries = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let Some(key) = key_bytes(batch, mapping, row)? else {
                counts.rows_skipped += 1;
                continue;
            };
            // #3083: a null value cell is skipped, not stored as an empty-byte
            // value (which is indistinguishable from a real empty value).
            if let Some(value_idx) = mapping.value_idx {
                if batch.column(value_idx).is_null(row) {
                    counts.rows_skipped += 1;
                    continue;
                }
            }
            let value = value_bytes(batch, mapping, row)?;
            entries.push(BatchKvEntry::new(Bytes::from(key), Bytes::from(value)));
        }
        if !entries.is_empty() {
            let output = executor.execute(Command::KvBatchPut {
                branch: branch.map(str::to_owned),
                space: space.map(str::to_owned),
                entries,
            })?;
            let Output::BatchResults(results) = output else {
                return Err(unexpected_output("kv_batch_put"));
            };
            for item in results.items() {
                if item.error_status().is_some() || !item.applied() {
                    counts.rows_skipped += 1;
                } else {
                    counts.rows_imported += 1;
                }
            }
        }
        counts.batches_processed += 1;
    }
    Ok(counts)
}

fn import_json(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    batches: &[arrow::record_batch::RecordBatch],
    mapping: &super::schema::ImportMapping,
) -> Result<ImportCounts, ExecutorError> {
    let mut counts = ImportCounts::default();
    for batch in batches {
        let mut entries = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let Some(key) = key_bytes(batch, mapping, row)? else {
                counts.rows_skipped += 1;
                continue;
            };
            let key = String::from_utf8(key).map_err(|error| {
                invalid_input(
                    "invalid_argument.executor.arrow_json_key",
                    format!("JSON import key is not valid UTF-8: {error}"),
                )
            })?;
            let value = json_document(batch, mapping, row)?;
            entries.push(BatchJsonEntry::new(key, "$", value));
        }
        if !entries.is_empty() {
            let output = executor.execute(Command::JsonBatchSet {
                branch: branch.map(str::to_owned),
                space: space.map(str::to_owned),
                entries,
            })?;
            let Output::JsonBatchResults(results) = output else {
                return Err(unexpected_output("json_batch_set"));
            };
            for item in results.items() {
                if item.error_status().is_some() {
                    counts.rows_skipped += 1;
                } else {
                    counts.rows_imported += 1;
                }
            }
        }
        counts.batches_processed += 1;
    }
    Ok(counts)
}

fn import_vector(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    batches: &[arrow::record_batch::RecordBatch],
    mapping: &super::schema::ImportMapping,
) -> Result<ImportCounts, ExecutorError> {
    let mut counts = ImportCounts::default();
    let collection_ready = collection_exists(executor, branch, space, collection)?;
    for batch in batches {
        let mut entries = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let Some(key) = key_bytes(batch, mapping, row)? else {
                counts.rows_skipped += 1;
                continue;
            };
            let key = String::from_utf8(key).map_err(|error| {
                invalid_input(
                    "invalid_argument.executor.arrow_vector_key",
                    format!("vector import key is not valid UTF-8: {error}"),
                )
            })?;
            let Some(vector) = vector_embedding(batch, mapping, row) else {
                counts.rows_skipped += 1;
                continue;
            };
            if !collection_ready {
                // The executor must not invent a distance metric (a semantic the
                // metric cannot be inferred from the data); require the target
                // collection to pre-exist so the user chooses it explicitly.
                return Err(not_found(
                    "not_found.engine.vector_collection",
                    format!(
                        "vector collection `{collection}` does not exist; create it with the desired distance metric before importing (Arrow import does not create vector collections)"
                    ),
                ));
            }
            let metadata = vector_metadata(batch, mapping, row)?;
            // Arrow embeddings are already f32; widen losslessly to the wire type.
            entries.push(BatchVectorEntry::new(
                key,
                vector.into_iter().map(f64::from).collect(),
                metadata,
            ));
        }
        if !entries.is_empty() {
            let output = executor.execute(Command::VectorBatchUpsert {
                branch: branch.map(str::to_owned),
                space: space.map(str::to_owned),
                collection: collection.to_owned(),
                entries,
            })?;
            let Output::VectorBatchUpsertResults(results) = output else {
                return Err(unexpected_output("vector_batch_upsert"));
            };
            for item in results.items() {
                if item.error_status().is_some() || !item.applied() {
                    counts.rows_skipped += 1;
                } else {
                    counts.rows_imported += 1;
                }
            }
        }
        counts.batches_processed += 1;
    }
    Ok(counts)
}

fn collection_exists(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
) -> Result<bool, ExecutorError> {
    let output = executor.execute(Command::VectorListCollections {
        branch: branch.map(str::to_owned),
        space: space.map(str::to_owned),
    })?;
    let Output::VectorCollectionList {
        items: collections, ..
    } = output
    else {
        return Err(unexpected_output("vector_list_collections"));
    };
    Ok(collections.iter().any(|entry| entry.name() == collection))
}

/// Imports a graph from the `_nodes`/`_edges` files that `arrow export graph`
/// writes, replaying every node and edge through one `GraphBatchWrite`. Nodes
/// are applied before edges so an edge always resolves its endpoints.
fn import_graph(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: Option<&str>,
    base_path: &Path,
    format: ArrowFileFormat,
) -> Result<ImportCounts, ExecutorError> {
    let graph_name = required_option(
        graph,
        "invalid_argument.executor.arrow_graph",
        "graph Arrow import requires a graph",
    )?;
    // Derive the two concrete files exactly as export does, so an exported graph
    // round-trips without the caller restating the node/edge paths.
    let (node_path, edge_path) = super::export::graph_paths(base_path, format);
    let (node_schema, node_batches) = read_file(&node_path, format)?;
    let (edge_schema, edge_batches) = read_file(&edge_path, format)?;

    let mut operations = Vec::new();

    let node_id_idx = graph_column_index(&node_schema, "node_id")?;
    let node_properties_idx = node_schema.index_of("properties").ok();
    let binding_idx = node_schema.index_of("binding").ok();
    let mut node_rows = 0_usize;
    for batch in &node_batches {
        for row in 0..batch.num_rows() {
            let node_id = graph_string_cell(batch, node_id_idx, row)?;
            let properties = graph_optional_json(batch, node_properties_idx, row)?;
            let binding = graph_optional_binding(batch, binding_idx, row)?;
            operations.push(GraphBatchOperation::UpsertNode {
                node_id,
                data: GraphNodeData::new(properties, binding),
            });
            node_rows += 1;
        }
    }

    let src_idx = graph_column_index(&edge_schema, "src")?;
    let edge_type_idx = graph_column_index(&edge_schema, "edge_type")?;
    let dst_idx = graph_column_index(&edge_schema, "dst")?;
    let weight_idx = graph_column_index(&edge_schema, "weight")?;
    let edge_properties_idx = edge_schema.index_of("properties").ok();
    let mut edge_rows = 0_usize;
    for batch in &edge_batches {
        for row in 0..batch.num_rows() {
            let src = graph_string_cell(batch, src_idx, row)?;
            let edge_type = graph_string_cell(batch, edge_type_idx, row)?;
            let dst = graph_string_cell(batch, dst_idx, row)?;
            let weight = graph_f64_cell(batch, weight_idx, row)?;
            let properties = graph_optional_json(batch, edge_properties_idx, row)?;
            operations.push(GraphBatchOperation::UpsertEdge {
                src,
                edge_type,
                dst,
                data: GraphEdgeData::new(Some(weight), properties),
            });
            edge_rows += 1;
        }
    }

    if !operations.is_empty() {
        let output = executor.execute(Command::GraphBatchWrite {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph_name.to_owned(),
            operations,
        })?;
        // Confirm the engine acknowledged the batch; every submitted operation is
        // an upsert, so all node/edge rows count as imported.
        let Output::GraphBatchWriteResult { .. } = output else {
            return Err(unexpected_output("graph_batch_write"));
        };
    }

    Ok(ImportCounts {
        rows_imported: (node_rows + edge_rows) as u64,
        rows_skipped: 0,
        batches_processed: (node_batches.len() + edge_batches.len()) as u64,
    })
}

/// Resolves a required graph-file column by name, failing with a stable code when
/// the exported schema is missing it.
/// Replays exported events into the target branch as ordinary appends. Only the
/// event `type` and `payload` are restored; the append reassigns
/// sequence/timestamp/hash (Arrow is analytics interchange, not a backup).
fn import_event(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<ImportCounts, ExecutorError> {
    let event_type_idx = event_column_index(schema, "event_type")?;
    let payload_idx = event_column_index(schema, "payload")?;

    let mut counts = ImportCounts::default();
    let mut entries = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let event_type = event_string_cell(batch, event_type_idx, row)?;
            let payload_text = event_string_cell(batch, payload_idx, row)?;
            let payload = serde_json::from_str(&payload_text).map_err(|error| {
                invalid_input(
                    "invalid_argument.executor.arrow_event",
                    format!("event import payload cell is not valid JSON: {error}"),
                )
            })?;
            entries.push(BatchEventEntry::new(event_type, payload));
        }
    }

    if !entries.is_empty() {
        let output = executor.execute(Command::EventBatchAppend {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            entries,
        })?;
        let Output::EventBatchAppendResults(results) = output else {
            return Err(unexpected_output("event_batch_append"));
        };
        // The engine validates each entry (empty/oversized type, bad payload) and
        // returns a per-item result; a rejected row must count as skipped, not
        // silently inflate rows_imported (#3081).
        for item in results.items() {
            if item.error_status().is_some() {
                counts.rows_skipped += 1;
            } else {
                counts.rows_imported += 1;
            }
        }
    }
    counts.batches_processed = batches.len() as u64;
    Ok(counts)
}

fn event_column_index(schema: &Schema, name: &str) -> Result<usize, ExecutorError> {
    schema.index_of(name).map_err(|_| {
        invalid_input(
            "invalid_argument.executor.arrow_event",
            format!("event import file is missing the `{name}` column"),
        )
    })
}

fn event_string_cell(
    batch: &RecordBatch,
    index: usize,
    row: usize,
) -> Result<String, ExecutorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| array.value(row).to_owned())
        .ok_or_else(|| {
            invalid_input(
                "invalid_argument.executor.arrow_event",
                format!("event import expects a string column at index {index}"),
            )
        })
}

fn graph_column_index(schema: &Schema, name: &str) -> Result<usize, ExecutorError> {
    schema.index_of(name).map_err(|_| {
        invalid_input(
            "invalid_argument.executor.arrow_graph",
            format!("graph import file is missing the `{name}` column"),
        )
    })
}

/// Reads a required Utf8 cell (node id, edge endpoint, or edge type).
fn graph_string_cell(
    batch: &RecordBatch,
    index: usize,
    row: usize,
) -> Result<String, ExecutorError> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            invalid_input(
                "invalid_argument.executor.arrow_graph",
                format!("graph import expects a string column at index {index}"),
            )
        })?;
    if array.is_null(row) {
        // #3083: node_id/src/dst/edge_type are required identity columns; a null
        // must be rejected, not read as "" (which would anchor a node/edge to an
        // empty-id phantom node).
        return Err(invalid_input(
            "invalid_argument.executor.arrow_graph",
            format!("graph import requires a non-null id at column index {index}"),
        ));
    }
    Ok(array.value(row).to_owned())
}

/// Reads a required Float64 edge weight cell.
fn graph_f64_cell(batch: &RecordBatch, index: usize, row: usize) -> Result<f64, ExecutorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .map(|array| array.value(row))
        .ok_or_else(|| {
            invalid_input(
                "invalid_argument.executor.arrow_graph",
                format!("graph import expects a float64 weight column at index {index}"),
            )
        })
}

/// Reads the raw text of an optional Utf8 cell; a missing column or null cell is
/// `None`.
fn graph_optional_string(
    batch: &RecordBatch,
    index: Option<usize>,
    row: usize,
) -> Result<Option<String>, ExecutorError> {
    let Some(index) = index else {
        return Ok(None);
    };
    let column = batch.column(index);
    if column.is_null(row) {
        return Ok(None);
    }
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| Some(array.value(row).to_owned()))
        .ok_or_else(|| {
            invalid_input(
                "invalid_argument.executor.arrow_graph",
                format!("graph import expects a JSON string column at index {index}"),
            )
        })
}

/// Parses an optional `properties` cell (a JSON string) back into a value.
fn graph_optional_json(
    batch: &RecordBatch,
    index: Option<usize>,
    row: usize,
) -> Result<Option<Value>, ExecutorError> {
    let Some(text) = graph_optional_string(batch, index, row)? else {
        return Ok(None);
    };
    serde_json::from_str(&text).map(Some).map_err(|error| {
        invalid_input(
            "invalid_argument.executor.arrow_graph",
            format!("graph import properties cell is not valid JSON: {error}"),
        )
    })
}

/// Parses an optional `binding` cell (a JSON string) back into a binding.
fn graph_optional_binding(
    batch: &RecordBatch,
    index: Option<usize>,
    row: usize,
) -> Result<Option<GraphEntityBinding>, ExecutorError> {
    let Some(text) = graph_optional_string(batch, index, row)? else {
        return Ok(None);
    };
    serde_json::from_str(&text).map(Some).map_err(|error| {
        invalid_input(
            "invalid_argument.executor.arrow_graph",
            format!("graph import binding cell is not valid JSON: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::ExecutorErrorClass;

    use super::{graph_string_cell, validate_import_flags, ArrowImportTarget};

    #[test]
    fn validate_import_flags_rejects_flags_that_do_not_apply_to_the_target() {
        use ArrowImportTarget::{Event, Graph, Json, Kv, Vector};
        // --collection is valid only for vector; --graph only for graph.
        for target in [Kv, Json, Event, Graph] {
            let error = validate_import_flags(target, true, false)
                .expect_err("collection is irrelevant here");
            assert_eq!(error.code(), "invalid_argument.executor.arrow_collection");
        }
        for target in [Kv, Json, Event, Vector] {
            let error =
                validate_import_flags(target, false, true).expect_err("graph is irrelevant here");
            assert_eq!(error.code(), "invalid_argument.executor.arrow_graph");
        }
        // The applicable combinations, and the no-flags case, are accepted.
        validate_import_flags(Vector, true, false).expect("collection ok for vector");
        validate_import_flags(Graph, false, true).expect("graph ok for graph");
        validate_import_flags(Kv, false, false).expect("no flags ok");
    }

    #[test]
    fn graph_string_cell_rejects_a_null_id_instead_of_fabricating_an_empty_string() {
        // #3083: a null node_id/src/dst/edge_type must be rejected, not read as ""
        // (which would anchor a node/edge to an empty-id phantom node).
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("src", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .expect("batch");

        // A present id reads through unchanged.
        assert_eq!(graph_string_cell(&batch, 0, 0).expect("non-null id"), "a");
        // A null id is rejected, not fabricated into "".
        let error = graph_string_cell(&batch, 0, 1).expect_err("null id rejected");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.executor.arrow_graph");
    }
}
