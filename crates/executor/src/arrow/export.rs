//! Export Strata primitives to Arrow RecordBatches.

use std::sync::Arc;

use arrow::array::{
    FixedSizeListBuilder, Float32Builder, Float64Builder, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use strata_core::primitives::json::JsonPath;

use crate::bridge::{extract_version, Primitives};
use crate::convert::convert_result;
use crate::{Error, Result};

/// Convert a VectorResult to an executor Result.
fn convert_vector_result<T>(
    r: std::result::Result<T, strata_vector::VectorError>,
    branch_id: strata_core::types::BranchId,
) -> Result<T> {
    convert_result(r.map_err(|e| e.into_strata_error(branch_id)))
}

/// Which primitive to export and any filtering options.
pub enum ExportSource {
    /// Key-value store, with optional key prefix filter.
    Kv {
        /// Optional key prefix to filter by.
        prefix: Option<String>,
    },
    /// JSON document store, with optional key prefix filter.
    Json {
        /// Optional key prefix to filter by.
        prefix: Option<String>,
    },
    /// Event log, with optional event type filter.
    Event {
        /// Optional event type to filter by.
        event_type: Option<String>,
    },
    /// Vector collection export.
    Vector {
        /// Name of the vector collection.
        collection: String,
    },
    /// Graph nodes export.
    GraphNodes {
        /// Name of the graph.
        graph: String,
    },
    /// Graph edges export.
    GraphEdges {
        /// Name of the graph.
        graph: String,
    },
}

/// Export primitive data as Arrow RecordBatches.
///
/// Returns the schema and one RecordBatch containing all exported rows
/// (or an empty vec if no data exists).
pub fn export_to_batches(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    source: ExportSource,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    match source {
        ExportSource::Kv { prefix } => export_kv(primitives, branch_id, space, prefix, limit),
        ExportSource::Json { prefix } => export_json(primitives, branch_id, space, prefix, limit),
        ExportSource::Event { event_type } => {
            export_event(primitives, branch_id, space, event_type, limit)
        }
        ExportSource::Vector { collection } => {
            export_vector(primitives, branch_id, space, &collection, limit)
        }
        ExportSource::GraphNodes { graph } => {
            export_graph_nodes(primitives, branch_id, space, &graph, limit)
        }
        ExportSource::GraphEdges { graph } => {
            export_graph_edges(primitives, branch_id, space, &graph, limit)
        }
    }
}

/// Serialize any Value to a string for Arrow utf8 columns.
///
/// Matches the existing export handler's rendering: scalars use to_string(),
/// Bytes use base64, Array/Object use JSON.
pub fn value_to_string(v: &strata_core::Value) -> String {
    use base64::Engine;
    match v {
        strata_core::Value::Null => String::new(),
        strata_core::Value::Bool(b) => b.to_string(),
        strata_core::Value::Int(i) => i.to_string(),
        strata_core::Value::Float(f) => f.to_string(),
        strata_core::Value::String(s) => s.clone(),
        strata_core::Value::Bytes(b) => {
            base64::engine::general_purpose::STANDARD.encode(b.as_slice())
        }
        strata_core::Value::Array(_) | strata_core::Value::Object(_) => {
            serde_json::to_string(&value_to_json(v)).unwrap_or_default()
        }
    }
}

/// Convert a strata Value to a serde_json::Value for proper JSON serialization.
///
/// Unlike `serde_json::to_value(v)` which would produce tagged enum variants,
/// this produces clean JSON (objects as `{}`, arrays as `[]`, etc.).
fn value_to_json(v: &strata_core::Value) -> serde_json::Value {
    use base64::Engine;
    match v {
        strata_core::Value::Null => serde_json::Value::Null,
        strata_core::Value::Bool(b) => serde_json::Value::Bool(*b),
        strata_core::Value::Int(i) => serde_json::json!(*i),
        strata_core::Value::Float(f) => serde_json::json!(*f),
        strata_core::Value::String(s) => serde_json::Value::String(s.clone()),
        strata_core::Value::Bytes(b) => serde_json::Value::String(
            base64::engine::general_purpose::STANDARD.encode(b.as_slice()),
        ),
        strata_core::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json).collect())
        }
        strata_core::Value::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

// =============================================================================
// KV export
// =============================================================================

fn kv_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
    ])
}

fn export_kv(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    prefix: Option<String>,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let schema = kv_schema();

    let keys = convert_result(p.kv.list(&branch_id, space, prefix.as_deref()))?;
    let max = limit.unwrap_or(usize::MAX);

    let mut key_builder = StringBuilder::new();
    let mut value_builder = StringBuilder::new();
    let mut version_builder = UInt64Builder::new();
    let mut timestamp_builder = UInt64Builder::new();

    for key in keys.into_iter().take(max) {
        if let Some(versioned) = convert_result(p.kv.get_versioned(&branch_id, space, &key))? {
            key_builder.append_value(&key);
            value_builder.append_value(value_to_string(&versioned.value));
            version_builder.append_value(extract_version(&versioned.version));
            timestamp_builder.append_value(versioned.timestamp.as_micros());
        }
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(timestamp_builder.finish()),
        ],
    )
    .map_err(|e| Error::Internal {
        reason: format!("failed to build KV RecordBatch: {e}"),
        hint: None,
    })?;

    if batch.num_rows() == 0 {
        return Ok((schema, vec![]));
    }
    Ok((schema, vec![batch]))
}

// =============================================================================
// JSON export
// =============================================================================

fn json_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("document", DataType::Utf8, false),
    ])
}

fn export_json(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    prefix: Option<String>,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let schema = json_schema();
    let max = limit.unwrap_or(usize::MAX);
    let root = JsonPath::root();

    let mut key_builder = StringBuilder::new();
    let mut doc_builder = StringBuilder::new();
    let mut count = 0usize;
    let mut cursor: Option<String> = None;

    loop {
        let result = convert_result(p.json.list(
            &branch_id,
            space,
            prefix.as_deref(),
            cursor.as_deref(),
            1000,
        ))?;
        let page_empty = result.doc_ids.is_empty();

        for doc_id in result.doc_ids {
            if count >= max {
                break;
            }
            if let Some(json_val) = convert_result(p.json.get(&branch_id, space, &doc_id, &root))? {
                let doc_str =
                    serde_json::to_string(&json_val).unwrap_or_else(|_| "null".to_string());
                key_builder.append_value(&doc_id);
                doc_builder.append_value(&doc_str);
                count += 1;
            }
        }

        if count >= max {
            break;
        }
        match result.next_cursor {
            Some(_) if page_empty => break,
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(key_builder.finish()),
            Arc::new(doc_builder.finish()),
        ],
    )
    .map_err(|e| Error::Internal {
        reason: format!("failed to build JSON RecordBatch: {e}"),
        hint: None,
    })?;

    if batch.num_rows() == 0 {
        return Ok((schema, vec![]));
    }
    Ok((schema, vec![batch]))
}

// =============================================================================
// Event export
// =============================================================================

fn event_schema() -> Schema {
    Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("timestamp", DataType::UInt64, false),
    ])
}

fn export_event(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    event_type: Option<String>,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let schema = event_schema();

    let events = convert_result(p.event.range(
        &branch_id,
        space,
        0,
        None,
        limit,
        false,
        event_type.as_deref(),
    ))?;

    let mut seq_builder = UInt64Builder::new();
    let mut type_builder = StringBuilder::new();
    let mut payload_builder = StringBuilder::new();
    let mut ts_builder = UInt64Builder::new();

    for versioned in &events {
        let event = &versioned.value;
        seq_builder.append_value(event.sequence);
        type_builder.append_value(&event.event_type);
        let payload_str = serde_json::to_string(&value_to_json(&event.payload))
            .unwrap_or_else(|_| "null".to_string());
        payload_builder.append_value(&payload_str);
        ts_builder.append_value(event.timestamp.as_micros());
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(seq_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(payload_builder.finish()),
            Arc::new(ts_builder.finish()),
        ],
    )
    .map_err(|e| Error::Internal {
        reason: format!("failed to build Event RecordBatch: {e}"),
        hint: None,
    })?;

    if batch.num_rows() == 0 {
        return Ok((schema, vec![]));
    }
    Ok((schema, vec![batch]))
}

// =============================================================================
// Vector export
// =============================================================================

fn export_vector(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    collection: &str,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    // Get collection info for the dimension
    let collections =
        convert_vector_result(p.vector.list_collections(branch_id, space), branch_id)?;
    let info = collections
        .iter()
        .find(|c| c.name == collection)
        .ok_or_else(|| Error::CollectionNotFound {
            collection: collection.to_string(),
            hint: None,
        })?;
    let dim = info.config.dimension as i32;

    let schema = Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        ),
        Field::new("metadata", DataType::Utf8, true),
    ]);

    let keys = convert_vector_result(p.vector.list_keys(branch_id, space, collection), branch_id)?;
    let max = limit.unwrap_or(usize::MAX);

    let mut key_builder = StringBuilder::new();
    let mut embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), dim);
    let mut metadata_builder = StringBuilder::new();

    for key in keys.into_iter().take(max) {
        if let Some(versioned) =
            convert_vector_result(p.vector.get(branch_id, space, collection, &key), branch_id)?
        {
            let entry = &versioned.value;
            key_builder.append_value(&entry.key);

            let values = embedding_builder.values();
            for &f in &entry.embedding {
                values.append_value(f);
            }
            embedding_builder.append(true);

            match &entry.metadata {
                Some(meta) => {
                    metadata_builder.append_value(
                        serde_json::to_string(meta).unwrap_or_else(|_| "null".to_string()),
                    );
                }
                None => metadata_builder.append_null(),
            }
        }
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(key_builder.finish()),
            Arc::new(embedding_builder.finish()),
            Arc::new(metadata_builder.finish()),
        ],
    )
    .map_err(|e| Error::Internal {
        reason: format!("failed to build Vector RecordBatch: {e}"),
        hint: None,
    })?;

    if batch.num_rows() == 0 {
        return Ok((schema, vec![]));
    }
    Ok((schema, vec![batch]))
}

// =============================================================================
// Graph nodes export
// =============================================================================

fn export_graph_nodes(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    graph: &str,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let schema = Schema::new(vec![
        Field::new("node_id", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, true),
        Field::new("properties", DataType::Utf8, true),
    ]);

    let nodes = convert_result(p.graph.all_nodes(branch_id, space, graph))?;
    let max = limit.unwrap_or(usize::MAX);

    let mut id_builder = StringBuilder::new();
    let mut type_builder = StringBuilder::new();
    let mut props_builder = StringBuilder::new();

    for (node_id, data) in nodes.iter().take(max) {
        id_builder.append_value(node_id);
        match &data.object_type {
            Some(t) => type_builder.append_value(t),
            None => type_builder.append_null(),
        }
        match &data.properties {
            Some(props) => {
                props_builder.append_value(
                    serde_json::to_string(props).unwrap_or_else(|_| "null".to_string()),
                );
            }
            None => props_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(props_builder.finish()),
        ],
    )
    .map_err(|e| Error::Internal {
        reason: format!("failed to build GraphNodes RecordBatch: {e}"),
        hint: None,
    })?;

    if batch.num_rows() == 0 {
        return Ok((schema, vec![]));
    }
    Ok((schema, vec![batch]))
}

// =============================================================================
// Graph edges export
// =============================================================================

fn export_graph_edges(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    graph: &str,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let schema = Schema::new(vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("target", DataType::Utf8, false),
        Field::new("edge_type", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("properties", DataType::Utf8, true),
    ]);

    let edges = convert_result(p.graph.all_edges(branch_id, space, graph))?;
    let max = limit.unwrap_or(usize::MAX);

    let mut src_builder = StringBuilder::new();
    let mut dst_builder = StringBuilder::new();
    let mut type_builder = StringBuilder::new();
    let mut weight_builder = Float64Builder::new();
    let mut props_builder = StringBuilder::new();

    for edge in edges.iter().take(max) {
        src_builder.append_value(&edge.src);
        dst_builder.append_value(&edge.dst);
        type_builder.append_value(&edge.edge_type);
        weight_builder.append_value(edge.data.weight);
        match &edge.data.properties {
            Some(props) => {
                props_builder.append_value(
                    serde_json::to_string(props).unwrap_or_else(|_| "null".to_string()),
                );
            }
            None => props_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(src_builder.finish()),
            Arc::new(dst_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(weight_builder.finish()),
            Arc::new(props_builder.finish()),
        ],
    )
    .map_err(|e| Error::Internal {
        reason: format!("failed to build GraphEdges RecordBatch: {e}"),
        hint: None,
    })?;

    if batch.num_rows() == 0 {
        return Ok((schema, vec![]));
    }
    Ok((schema, vec![batch]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, UInt64Array};
    use strata_core::types::BranchId;
    use strata_core::Value;
    /// The "default" branch maps to all-zero UUID in core.
    fn default_branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    /// Create a Strata cache DB, return the executor's primitives for export testing.
    fn setup_with<F>(populate: F) -> (Arc<Primitives>, BranchId)
    where
        F: FnOnce(&crate::Strata),
    {
        let strata = crate::Strata::cache().expect("open cache db");
        populate(&strata);
        let p = Arc::new(Primitives::new(strata.database()));
        // Keep strata alive so the database isn't dropped
        std::mem::forget(strata);
        (p, default_branch())
    }

    /// Setup variant that gives the populate closure a transport-neutral
    /// executor so commands without a thin typed wrapper (vector, graph)
    /// can drive setup directly through the public `Command` surface.
    fn setup_with_executor<F>(populate: F) -> (Arc<Primitives>, BranchId)
    where
        F: FnOnce(&crate::Executor),
    {
        let strata = crate::Strata::cache().expect("open cache db");
        let executor = crate::Executor::new(strata.database());
        populate(&executor);
        let p = Arc::new(Primitives::new(strata.database()));
        std::mem::forget(strata);
        (p, default_branch())
    }

    #[test]
    fn test_kv_export_schema() {
        let (p, branch_id) = setup_with(|db| {
            db.kv_put("k1", Value::String("v1".into())).unwrap();
            db.kv_put("k2", Value::Int(42)).unwrap();
            db.kv_put("k3", Value::Bool(true)).unwrap();
        });

        let (schema, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::Kv { prefix: None },
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "key");
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(2).name(), "version");
        assert_eq!(schema.field(3).name(), "timestamp");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(2).data_type(), DataType::UInt64);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_kv_export_values() {
        let (p, branch_id) = setup_with(|db| {
            db.kv_put("user:1", Value::String("Alice".into())).unwrap();
            db.kv_put("user:2", Value::Int(99)).unwrap();
        });

        let (_, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::Kv { prefix: None },
            None,
        )
        .unwrap();

        let batch = &batches[0];
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let versions = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let timestamps = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        // Keys should be present (order may vary due to key sorting)
        let key_set: Vec<&str> = (0..keys.len()).map(|i| keys.value(i)).collect();
        assert!(key_set.contains(&"user:1"));
        assert!(key_set.contains(&"user:2"));

        // Find "user:1" row and verify its value
        let idx = key_set.iter().position(|k| *k == "user:1").unwrap();
        assert_eq!(values.value(idx), "Alice");
        assert!(versions.value(idx) > 0);
        assert!(timestamps.value(idx) > 0);

        // Find "user:2" and verify Int renders as string
        let idx2 = key_set.iter().position(|k| *k == "user:2").unwrap();
        assert_eq!(values.value(idx2), "99");
    }

    #[test]
    fn test_json_export() {
        let (p, branch_id) = setup_with(|db| {
            db.json_set(
                "u1",
                "$",
                Value::String(r#"{"name":"Alice","age":30}"#.into()),
            )
            .unwrap();
            db.json_set(
                "u2",
                "$",
                Value::String(r#"{"name":"Bob","age":25}"#.into()),
            )
            .unwrap();
        });

        let (schema, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::Json { prefix: None },
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "key");
        assert_eq!(schema.field(1).name(), "document");

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let docs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let key_set: Vec<&str> = (0..keys.len()).map(|i| keys.value(i)).collect();
        assert!(key_set.contains(&"u1"));
        assert!(key_set.contains(&"u2"));

        // Verify document content parses as valid JSON with expected fields
        let idx = key_set.iter().position(|k| *k == "u1").unwrap();
        let parsed: serde_json::Value = serde_json::from_str(docs.value(idx)).unwrap();
        // json_set with Value::String stores the string as the document value
        assert!(parsed.is_string() || parsed.is_object());
    }

    #[test]
    fn test_event_export() {
        let (p, branch_id) = setup_with(|db| {
            db.event_append(
                "click",
                Value::object({
                    let mut m = std::collections::HashMap::new();
                    m.insert("page".to_string(), Value::String("home".into()));
                    m
                }),
            )
            .unwrap();
            db.event_append(
                "view",
                Value::object({
                    let mut m = std::collections::HashMap::new();
                    m.insert("page".to_string(), Value::String("about".into()));
                    m
                }),
            )
            .unwrap();
            db.event_append(
                "click",
                Value::object({
                    let mut m = std::collections::HashMap::new();
                    m.insert("page".to_string(), Value::String("contact".into()));
                    m
                }),
            )
            .unwrap();
        });

        let (schema, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::Event { event_type: None },
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "sequence");
        assert_eq!(schema.field(1).name(), "event_type");
        assert_eq!(schema.field(2).name(), "payload");
        assert_eq!(schema.field(3).name(), "timestamp");

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let seqs = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let payloads = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let timestamps = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        // Events are ordered by sequence
        assert_eq!(seqs.value(0), 0);
        assert_eq!(seqs.value(1), 1);
        assert_eq!(seqs.value(2), 2);
        assert_eq!(types.value(0), "click");
        assert_eq!(types.value(1), "view");
        assert_eq!(types.value(2), "click");

        let p0: serde_json::Value = serde_json::from_str(payloads.value(0)).unwrap();
        assert_eq!(p0["page"], "home");

        assert!(timestamps.value(0) > 0);
    }

    #[test]
    fn test_export_with_limit() {
        let (p, branch_id) = setup_with(|db| {
            for i in 0..10 {
                db.kv_put(&format!("k{i:02}"), Value::Int(i)).unwrap();
            }
        });

        let (_, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::Kv { prefix: None },
            Some(3),
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_vector_export() {
        use crate::types::DistanceMetric;
        use crate::Command;
        use arrow::array::{FixedSizeListArray, Float32Array};

        let (p, branch_id) = setup_with_executor(|exec| {
            exec.execute(Command::VectorCreateCollection {
                branch: None,
                space: None,
                collection: "docs".into(),
                dimension: 3,
                metric: DistanceMetric::Cosine,
            })
            .unwrap();
            exec.execute(Command::VectorUpsert {
                branch: None,
                space: None,
                collection: "docs".into(),
                key: "v1".into(),
                vector: vec![1.0, 2.0, 3.0],
                metadata: Some(Value::String(r#"{"tag":"a"}"#.into())),
            })
            .unwrap();
            exec.execute(Command::VectorUpsert {
                branch: None,
                space: None,
                collection: "docs".into(),
                key: "v2".into(),
                vector: vec![4.0, 5.0, 6.0],
                metadata: None,
            })
            .unwrap();
            exec.execute(Command::VectorUpsert {
                branch: None,
                space: None,
                collection: "docs".into(),
                key: "v3".into(),
                vector: vec![7.0, 8.0, 9.0],
                metadata: Some(Value::String(r#"{"tag":"c"}"#.into())),
            })
            .unwrap();
        });

        let (schema, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::Vector {
                collection: "docs".into(),
            },
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "key");
        assert_eq!(schema.field(1).name(), "embedding");
        assert_eq!(schema.field(2).name(), "metadata");

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let embeddings = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let metadata = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Verify embedding dimension
        assert_eq!(embeddings.value_length(), 3);

        // Find v1 and check its embedding values
        let key_set: Vec<&str> = (0..keys.len()).map(|i| keys.value(i)).collect();
        let idx = key_set.iter().position(|k| *k == "v1").unwrap();
        let emb = embeddings.value(idx);
        let floats = emb.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((floats.value(0) - 1.0).abs() < f32::EPSILON);
        assert!((floats.value(1) - 2.0).abs() < f32::EPSILON);
        assert!((floats.value(2) - 3.0).abs() < f32::EPSILON);

        // v1 has metadata, v2 does not
        assert!(!metadata.is_null(idx));
        let idx2 = key_set.iter().position(|k| *k == "v2").unwrap();
        assert!(metadata.is_null(idx2));
    }

    #[test]
    fn test_graph_nodes_export() {
        use crate::Command;

        let (p, branch_id) = setup_with_executor(|exec| {
            exec.execute(Command::GraphCreate {
                branch: None,
                space: None,
                graph: "social".into(),
                cascade_policy: None,
            })
            .unwrap();
            exec.execute(Command::GraphAddNode {
                branch: None,
                space: None,
                graph: "social".into(),
                node_id: "alice".into(),
                entity_ref: None,
                properties: Some(Value::String(r#"{"age":30}"#.into())),
                object_type: Some("Person".into()),
            })
            .unwrap();
            exec.execute(Command::GraphAddNode {
                branch: None,
                space: None,
                graph: "social".into(),
                node_id: "bob".into(),
                entity_ref: None,
                properties: Some(Value::String(r#"{"age":25}"#.into())),
                object_type: Some("Person".into()),
            })
            .unwrap();
            exec.execute(Command::GraphAddNode {
                branch: None,
                space: None,
                graph: "social".into(),
                node_id: "acme".into(),
                entity_ref: None,
                properties: None,
                object_type: None,
            })
            .unwrap();
        });

        let (schema, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::GraphNodes {
                graph: "social".into(),
            },
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "node_id");
        assert_eq!(schema.field(1).name(), "object_type");
        assert_eq!(schema.field(2).name(), "properties");

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let props = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let id_set: Vec<&str> = (0..ids.len()).map(|i| ids.value(i)).collect();
        assert!(id_set.contains(&"alice"));
        assert!(id_set.contains(&"bob"));
        assert!(id_set.contains(&"acme"));

        // alice has type "Person" and properties, acme has neither
        let alice_idx = id_set.iter().position(|k| *k == "alice").unwrap();
        assert_eq!(types.value(alice_idx), "Person");
        assert!(!props.is_null(alice_idx));

        let acme_idx = id_set.iter().position(|k| *k == "acme").unwrap();
        assert!(types.is_null(acme_idx));
        assert!(props.is_null(acme_idx));
    }

    #[test]
    fn test_graph_edges_export() {
        use crate::Command;
        use arrow::array::Float64Array;

        let (p, branch_id) = setup_with_executor(|exec| {
            exec.execute(Command::GraphCreate {
                branch: None,
                space: None,
                graph: "social".into(),
                cascade_policy: None,
            })
            .unwrap();
            for node in ["alice", "bob", "carol"] {
                exec.execute(Command::GraphAddNode {
                    branch: None,
                    space: None,
                    graph: "social".into(),
                    node_id: node.into(),
                    entity_ref: None,
                    properties: None,
                    object_type: None,
                })
                .unwrap();
            }
            exec.execute(Command::GraphAddEdge {
                branch: None,
                space: None,
                graph: "social".into(),
                src: "alice".into(),
                dst: "bob".into(),
                edge_type: "knows".into(),
                weight: Some(0.9),
                properties: None,
            })
            .unwrap();
            exec.execute(Command::GraphAddEdge {
                branch: None,
                space: None,
                graph: "social".into(),
                src: "bob".into(),
                dst: "carol".into(),
                edge_type: "knows".into(),
                weight: Some(0.5),
                properties: None,
            })
            .unwrap();
        });

        let (schema, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::GraphEdges {
                graph: "social".into(),
            },
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "source");
        assert_eq!(schema.field(1).name(), "target");
        assert_eq!(schema.field(2).name(), "edge_type");
        assert_eq!(schema.field(3).name(), "weight");
        assert_eq!(schema.field(4).name(), "properties");

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let targets = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let edge_types = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let weights = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Verify both edges (order may vary)
        let src_set: Vec<&str> = (0..sources.len()).map(|i| sources.value(i)).collect();
        assert!(src_set.contains(&"alice"));
        assert!(src_set.contains(&"bob"));

        let alice_idx = src_set.iter().position(|k| *k == "alice").unwrap();
        assert_eq!(targets.value(alice_idx), "bob");
        assert_eq!(edge_types.value(alice_idx), "knows");
        assert!((weights.value(alice_idx) - 0.9).abs() < f64::EPSILON);

        let bob_idx = src_set.iter().position(|k| *k == "bob").unwrap();
        assert_eq!(targets.value(bob_idx), "carol");
        assert_eq!(edge_types.value(bob_idx), "knows");
        assert!((weights.value(bob_idx) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_export_empty_database() {
        let (p, branch_id) = setup_with(|_db| {
            // No data written
        });

        let (schema, batches) = export_to_batches(
            &p,
            branch_id,
            "default",
            ExportSource::Kv { prefix: None },
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert!(batches.is_empty());
    }
}
