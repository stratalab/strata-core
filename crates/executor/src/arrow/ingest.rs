//! Ingest pipeline: RecordBatch rows -> Strata KV, JSON, Vector primitives.

use std::sync::Arc;

use arrow::array::{self, Array};
use arrow::record_batch::RecordBatch;

use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::{DistanceMetric, JsonPath, JsonValue, StorageDtype, VectorConfig};

use super::schema::{arrow_to_value, row_to_json, ImportMapping};
use crate::bridge::Primitives;
use crate::convert::convert_result;
use crate::{Error, Result};

/// Result of an import operation.
#[derive(Debug)]
pub struct ImportResult {
    /// Number of rows successfully imported.
    pub rows_imported: u64,
    /// Number of rows skipped (null key, type error, etc.).
    pub rows_skipped: u64,
    /// Number of RecordBatches processed.
    pub batches_processed: u64,
}

/// Ingest RecordBatches into the KV primitive.
pub fn ingest_kv(
    p: &Arc<Primitives>,
    branch_id: BranchId,
    space: &str,
    batches: &[RecordBatch],
    mapping: &ImportMapping,
) -> Result<ImportResult> {
    let mut result = ImportResult {
        rows_imported: 0,
        rows_skipped: 0,
        batches_processed: 0,
    };

    let extra_cols: Vec<(usize, String)> = mapping
        .extra_indices
        .iter()
        .zip(mapping.extra_names.iter())
        .map(|(&i, n)| (i, n.clone()))
        .collect();

    for batch in batches {
        let key_col = batch.column(mapping.key_idx);
        let mut entries = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            // Skip null keys.
            if key_col.is_null(row) {
                result.rows_skipped += 1;
                continue;
            }

            let key = extract_key_string(key_col.as_ref(), row);
            let key = match key {
                Some(k) => k,
                None => {
                    result.rows_skipped += 1;
                    continue;
                }
            };

            let value = if let Some(val_idx) = mapping.value_idx {
                arrow_to_value(batch.column(val_idx).as_ref(), row)?
            } else {
                // Serialize extra columns as JSON object.
                let cols_ref: Vec<(usize, &str)> =
                    extra_cols.iter().map(|(i, n)| (*i, n.as_str())).collect();
                let json_str = row_to_json(batch, row, &cols_ref)?;
                Value::String(json_str)
            };

            entries.push((key, value));
        }

        if !entries.is_empty() {
            register_space(p, branch_id, space)?;
            let count = entries.len() as u64;
            convert_result(p.kv.batch_put(&branch_id, space, entries))?;
            result.rows_imported += count;
        }

        result.batches_processed += 1;
    }

    Ok(result)
}

/// Ingest RecordBatches into the JSON primitive.
pub fn ingest_json(
    p: &Arc<Primitives>,
    branch_id: BranchId,
    space: &str,
    batches: &[RecordBatch],
    mapping: &ImportMapping,
) -> Result<ImportResult> {
    let mut result = ImportResult {
        rows_imported: 0,
        rows_skipped: 0,
        batches_processed: 0,
    };

    let extra_cols: Vec<(usize, String)> = mapping
        .extra_indices
        .iter()
        .zip(mapping.extra_names.iter())
        .map(|(&i, n)| (i, n.clone()))
        .collect();

    for batch in batches {
        let key_col = batch.column(mapping.key_idx);
        let mut entries: Vec<(String, JsonPath, JsonValue)> = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            if key_col.is_null(row) {
                result.rows_skipped += 1;
                continue;
            }

            let key = match extract_key_string(key_col.as_ref(), row) {
                Some(k) => k,
                None => {
                    result.rows_skipped += 1;
                    continue;
                }
            };

            let doc_str = if let Some(val_idx) = mapping.value_idx {
                let val = arrow_to_value(batch.column(val_idx).as_ref(), row)?;
                match val {
                    Value::String(s) => s,
                    other => crate::arrow::export::value_to_string(&other),
                }
            } else {
                let cols_ref: Vec<(usize, &str)> =
                    extra_cols.iter().map(|(i, n)| (*i, n.as_str())).collect();
                row_to_json(batch, row, &cols_ref)?
            };

            // Parse JSON string into JsonValue, upsert document at root.
            let json_val: serde_json::Value = serde_json::from_str(&doc_str)
                .unwrap_or(serde_json::Value::String(doc_str.clone()));
            let json_value = JsonValue::from(json_val);

            entries.push((key, JsonPath::root(), json_value));
        }

        if !entries.is_empty() {
            register_space(p, branch_id, space)?;
            let count = entries.len() as u64;
            convert_result(p.json.batch_set_or_create(&branch_id, space, entries))?;
            result.rows_imported += count;
        }

        result.batches_processed += 1;
    }

    Ok(result)
}

/// Ingest RecordBatches into the Vector primitive.
pub fn ingest_vector(
    p: &Arc<Primitives>,
    branch_id: BranchId,
    space: &str,
    collection: &str,
    batches: &[RecordBatch],
    mapping: &ImportMapping,
) -> Result<ImportResult> {
    let mut result = ImportResult {
        rows_imported: 0,
        rows_skipped: 0,
        batches_processed: 0,
    };

    let extra_cols: Vec<(usize, String)> = mapping
        .extra_indices
        .iter()
        .zip(mapping.extra_names.iter())
        .map(|(&i, n)| (i, n.clone()))
        .collect();

    let val_idx = mapping.value_idx.ok_or_else(|| Error::InvalidInput {
        reason: "no embedding column resolved".into(),
        hint: None,
    })?;

    let mut collection_created = false;

    for batch in batches {
        let key_col = batch.column(mapping.key_idx);
        let emb_col = batch.column(val_idx);
        let mut entries: Vec<(String, Vec<f32>, Option<serde_json::Value>)> =
            Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            if key_col.is_null(row) {
                result.rows_skipped += 1;
                continue;
            }

            let key = match extract_key_string(key_col.as_ref(), row) {
                Some(k) => k,
                None => {
                    result.rows_skipped += 1;
                    continue;
                }
            };

            let embedding = match extract_embedding(emb_col.as_ref(), row) {
                Some(e) => e,
                None => {
                    result.rows_skipped += 1;
                    continue;
                }
            };

            // Auto-create collection on first valid embedding (need dimension).
            if !collection_created {
                register_space(p, branch_id, space)?;
                auto_create_collection(p, branch_id, space, collection, embedding.len())?;
                collection_created = true;
            }

            // Extract optional metadata from extra columns.
            let metadata: Option<serde_json::Value> = if extra_cols.is_empty() {
                None
            } else {
                let cols_ref: Vec<(usize, &str)> =
                    extra_cols.iter().map(|(i, n)| (*i, n.as_str())).collect();
                let json_str = row_to_json(batch, row, &cols_ref)?;
                let val: serde_json::Value =
                    serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null);
                Some(val)
            };

            entries.push((key, embedding, metadata));
        }

        if !entries.is_empty() {
            let count = entries.len() as u64;
            convert_vector_result(
                p.vector.batch_insert(branch_id, space, collection, entries),
                branch_id,
            )?;
            result.rows_imported += count;
        }

        result.batches_processed += 1;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_key_string(col: &dyn Array, row: usize) -> Option<String> {
    match col.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<array::StringArray>()?;
            Some(arr.value(row).to_string())
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<array::LargeStringArray>()?;
            Some(arr.value(row).to_string())
        }
        // For non-string key columns, convert to string representation.
        _ => {
            let formatter =
                arrow::util::display::ArrayFormatter::try_new(col, &Default::default()).ok()?;
            Some(formatter.value(row).to_string())
        }
    }
}

fn extract_embedding(col: &dyn Array, row: usize) -> Option<Vec<f32>> {
    if col.is_null(row) {
        return None;
    }

    let inner = match col.data_type() {
        arrow::datatypes::DataType::FixedSizeList(_, _) => {
            let list = col.as_any().downcast_ref::<array::FixedSizeListArray>()?;
            list.value(row)
        }
        arrow::datatypes::DataType::List(_) => {
            let list = col.as_any().downcast_ref::<array::ListArray>()?;
            list.value(row)
        }
        _ => return None,
    };

    // Try f32 first, then f64.
    if let Some(f32_arr) = inner.as_any().downcast_ref::<array::Float32Array>() {
        Some(f32_arr.values().to_vec())
    } else {
        inner
            .as_any()
            .downcast_ref::<array::Float64Array>()
            .map(|f64_arr| f64_arr.values().iter().map(|&v| v as f32).collect())
    }
}

fn auto_create_collection(
    p: &Arc<Primitives>,
    branch_id: BranchId,
    space: &str,
    collection: &str,
    dimension: usize,
) -> Result<()> {
    // Check if collection already exists.
    let collections =
        convert_vector_result(p.vector.list_collections(branch_id, space), branch_id)?;
    if collections.iter().any(|c| c.name == collection) {
        return Ok(());
    }

    let config = VectorConfig {
        dimension,
        metric: DistanceMetric::Cosine,
        storage_dtype: StorageDtype::F32,
    };

    convert_vector_result(
        p.vector
            .create_collection(branch_id, space, collection, config),
        branch_id,
    )?;

    Ok(())
}

fn register_space(p: &Arc<Primitives>, branch_id: BranchId, space: &str) -> Result<()> {
    // The implicit "default" and "_system_" spaces are always discoverable via
    // `SpaceIndex::exists` without metadata, so skip the empty registration
    // transaction that would otherwise burn a storage version per ingest call.
    if space == "default" || space == strata_engine::system_space::SYSTEM_SPACE {
        return Ok(());
    }
    convert_result(p.space.register(branch_id, space))?;
    Ok(())
}

fn convert_vector_result<T>(
    r: std::result::Result<T, strata_vector::VectorError>,
    branch_id: BranchId,
) -> Result<T> {
    convert_result(r.map_err(|e| e.into_strata_error(branch_id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn default_branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    fn setup_with<F>(populate: F) -> (Arc<Primitives>, BranchId)
    where
        F: FnOnce(&crate::Strata),
    {
        let strata = crate::Strata::cache().expect("open cache db");
        populate(&strata);
        let p = Arc::new(Primitives::new(strata.database()));
        std::mem::forget(strata);
        (p, default_branch())
    }

    fn make_kv_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2", "k3"])),
                Arc::new(StringArray::from(vec!["v1", "v2", "v3"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_ingest_kv() {
        let (p, branch_id) = setup_with(|_| {});
        let batch = make_kv_batch();

        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: Some(1),
            extra_indices: vec![],
            extra_names: vec![],
        };

        let result = ingest_kv(&p, branch_id, "default", &[batch], &mapping).unwrap();
        assert_eq!(result.rows_imported, 3);
        assert_eq!(result.rows_skipped, 0);
        assert_eq!(result.batches_processed, 1);

        // Verify data via KV get.
        let v1 = convert_result(p.kv.get(&branch_id, "default", "k1")).unwrap();
        assert_eq!(v1, Some(Value::String("v1".into())));
        let v2 = convert_result(p.kv.get(&branch_id, "default", "k2")).unwrap();
        assert_eq!(v2, Some(Value::String("v2".into())));
    }

    #[test]
    fn test_ingest_kv_extra_columns_as_json() {
        let (p, branch_id) = setup_with(|_| {});

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["u1", "u2"])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int64Array::from(vec![30, 25])),
            ],
        )
        .unwrap();

        // No value_idx: extra columns (name, age) become JSON.
        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: None,
            extra_indices: vec![1, 2],
            extra_names: vec!["name".into(), "age".into()],
        };

        let result = ingest_kv(&p, branch_id, "default", &[batch], &mapping).unwrap();
        assert_eq!(result.rows_imported, 2);

        let v = convert_result(p.kv.get(&branch_id, "default", "u1"))
            .unwrap()
            .unwrap();
        if let Value::String(s) = &v {
            let parsed: serde_json::Value = serde_json::from_str(s).unwrap();
            assert_eq!(parsed["name"], "Alice");
            assert_eq!(parsed["age"], 30);
        } else {
            panic!("expected Value::String, got: {:?}", v);
        }
    }

    #[test]
    fn test_ingest_json() {
        let (p, branch_id) = setup_with(|_| {});

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("document", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["doc1", "doc2", "doc3", "doc4"])),
                Arc::new(StringArray::from(vec![
                    r#"{"title":"Hello"}"#,
                    r#"{"title":"World"}"#,
                    r#"{"title":"Foo"}"#,
                    r#"{"title":"Bar"}"#,
                ])),
            ],
        )
        .unwrap();

        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: Some(1),
            extra_indices: vec![],
            extra_names: vec![],
        };

        // Capture storage version before/after to assert a single commit per batch.
        let v_before = p.db.storage().version();
        let result = ingest_json(&p, branch_id, "default", &[batch], &mapping).unwrap();
        let v_after = p.db.storage().version();
        assert_eq!(result.rows_imported, 4);
        assert_eq!(
            v_after - v_before,
            1,
            "ingest_json must commit exactly once per batch (got {} commits for 4 rows)",
            v_after - v_before
        );

        let doc = convert_result(p.json.get(&branch_id, "default", "doc1", &JsonPath::root()))
            .unwrap()
            .unwrap();
        let s = doc.to_string();
        assert!(s.contains("Hello"), "got: {s}");
        let doc4 = convert_result(p.json.get(&branch_id, "default", "doc4", &JsonPath::root()))
            .unwrap()
            .unwrap();
        assert!(doc4.to_string().contains("Bar"));
    }

    #[test]
    fn test_ingest_json_from_columns() {
        let (p, branch_id) = setup_with(|_| {});

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["u1"])),
                Arc::new(StringArray::from(vec!["Alice"])),
                Arc::new(StringArray::from(vec!["alice@example.com"])),
            ],
        )
        .unwrap();

        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: None,
            extra_indices: vec![1, 2],
            extra_names: vec!["name".into(), "email".into()],
        };

        let result = ingest_json(&p, branch_id, "default", &[batch], &mapping).unwrap();
        assert_eq!(result.rows_imported, 1);

        let doc = convert_result(p.json.get(&branch_id, "default", "u1", &JsonPath::root()))
            .unwrap()
            .unwrap();
        let s = doc.to_string();
        assert!(s.contains("Alice"), "got: {s}");
        assert!(s.contains("alice@example.com"), "got: {s}");
    }

    #[test]
    fn test_ingest_vector() {
        let (p, branch_id) = setup_with(|_| {});

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                false,
            ),
        ]));

        let values = Float32Array::from(vec![
            1.0, 0.0, 0.0, // v1
            0.0, 1.0, 0.0, // v2
            0.0, 0.0, 1.0, // v3
            0.5, 0.5, 0.0, // v4
        ]);
        let list = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            3,
            Arc::new(values),
            None,
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["v1", "v2", "v3", "v4"])),
                Arc::new(list),
            ],
        )
        .unwrap();

        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: Some(1),
            extra_indices: vec![],
            extra_names: vec![],
        };

        // Capture storage version before/after to assert a single ingest commit per batch.
        // auto_create_collection performs its own commit, so the delta is 2 (create + insert).
        let v_before = p.db.storage().version();
        let result =
            ingest_vector(&p, branch_id, "default", "test_col", &[batch], &mapping).unwrap();
        let v_after = p.db.storage().version();
        assert_eq!(result.rows_imported, 4);
        assert_eq!(result.rows_skipped, 0);
        assert_eq!(
            v_after - v_before,
            2,
            "ingest_vector must do one create_collection + one batch_insert commit \
             (got {} commits for 4 rows)",
            v_after - v_before
        );

        // Verify via vector get.
        let entry = convert_vector_result(
            p.vector.get(branch_id, "default", "test_col", "v1"),
            branch_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(entry.value.embedding.len(), 3);
        assert_eq!(entry.value.embedding[0], 1.0);

        let entry4 = convert_vector_result(
            p.vector.get(branch_id, "default", "test_col", "v4"),
            branch_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(entry4.value.embedding, vec![0.5, 0.5, 0.0]);
    }

    #[test]
    fn test_ingest_vector_pre_created_collection_one_commit_per_batch() {
        // When the collection already exists, ingest_vector should produce
        // exactly one commit per RecordBatch (no per-row commits).
        let (p, branch_id) = setup_with(|_| {});

        // Pre-create the collection so auto_create_collection is a no-op.
        let config = VectorConfig {
            dimension: 3,
            metric: DistanceMetric::Cosine,
            storage_dtype: StorageDtype::F32,
        };
        convert_vector_result(
            p.vector
                .create_collection(branch_id, "default", "preexisting", config),
            branch_id,
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                false,
            ),
        ]));

        let values = Float32Array::from(vec![
            1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.5, 0.5, 0.0, 0.25, 0.25, 0.5,
        ]);
        let list = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            3,
            Arc::new(values),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(list),
            ],
        )
        .unwrap();

        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: Some(1),
            extra_indices: vec![],
            extra_names: vec![],
        };

        let v_before = p.db.storage().version();
        let result =
            ingest_vector(&p, branch_id, "default", "preexisting", &[batch], &mapping).unwrap();
        let v_after = p.db.storage().version();

        assert_eq!(result.rows_imported, 5);
        assert_eq!(
            v_after - v_before,
            1,
            "ingest_vector into a pre-existing collection must commit exactly once per batch \
             (got {} commits for 5 rows)",
            v_after - v_before
        );
    }

    #[test]
    fn test_ingest_vector_auto_creates_collection() {
        let (p, branch_id) = setup_with(|_| {});

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]));

        let values = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let list = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            4,
            Arc::new(values),
            None,
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["vec1"])), Arc::new(list)],
        )
        .unwrap();

        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: Some(1),
            extra_indices: vec![],
            extra_names: vec![],
        };

        // Collection "auto_col" doesn't exist yet.
        let result =
            ingest_vector(&p, branch_id, "default", "auto_col", &[batch], &mapping).unwrap();
        assert_eq!(result.rows_imported, 1);

        // Verify collection was created with dimension 4.
        let collections =
            convert_vector_result(p.vector.list_collections(branch_id, "default"), branch_id)
                .unwrap();
        assert!(
            collections.iter().any(|c| c.name == "auto_col"),
            "collection should have been auto-created"
        );
    }

    #[test]
    fn test_ingest_kv_null_keys_skipped() {
        let (p, branch_id) = setup_with(|_| {});

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("k1"), None, Some("k3")])),
                Arc::new(StringArray::from(vec!["v1", "v2", "v3"])),
            ],
        )
        .unwrap();

        let mapping = ImportMapping {
            key_idx: 0,
            value_idx: Some(1),
            extra_indices: vec![],
            extra_names: vec![],
        };

        let result = ingest_kv(&p, branch_id, "default", &[batch], &mapping).unwrap();
        assert_eq!(result.rows_imported, 2);
        assert_eq!(result.rows_skipped, 1);
    }
}
