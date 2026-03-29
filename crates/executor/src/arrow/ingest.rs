//! Ingest pipeline: RecordBatch rows -> Strata KV, JSON, Vector primitives.

use std::sync::Arc;

use arrow::array::{self, Array};
use arrow::record_batch::RecordBatch;

use strata_core::primitives::json::{JsonPath, JsonValue};
use strata_core::primitives::{DistanceMetric, StorageDtype, VectorConfig};
use strata_core::types::BranchId;
use strata_core::value::Value;

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

            // Parse JSON string into JsonValue, upsert document.
            let json_val: serde_json::Value = serde_json::from_str(&doc_str)
                .unwrap_or(serde_json::Value::String(doc_str.clone()));
            let json_value = JsonValue::from(json_val);

            // Try create first; if doc already exists, overwrite at root.
            if convert_result(p.json.create(&branch_id, space, &key, json_value.clone())).is_err() {
                convert_result(
                    p.json
                        .set(&branch_id, space, &key, &JsonPath::root(), json_value),
                )?;
            }
            result.rows_imported += 1;
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

            // Auto-create collection on first embedding.
            if !collection_created {
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

            convert_vector_result(
                p.vector
                    .insert(branch_id, space, collection, &key, &embedding, metadata),
                branch_id,
            )?;
            result.rows_imported += 1;
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
                Arc::new(StringArray::from(vec!["doc1", "doc2"])),
                Arc::new(StringArray::from(vec![
                    r#"{"title":"Hello"}"#,
                    r#"{"title":"World"}"#,
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

        let result = ingest_json(&p, branch_id, "default", &[batch], &mapping).unwrap();
        assert_eq!(result.rows_imported, 2);

        let doc = convert_result(p.json.get(&branch_id, "default", "doc1", &JsonPath::root()))
            .unwrap()
            .unwrap();
        let s = doc.to_string();
        assert!(s.contains("Hello"), "got: {s}");
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

        let values = Float32Array::from(vec![1.0, 0.0, 0.0, 0.0, 1.0, 0.0]);
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
                Arc::new(StringArray::from(vec!["v1", "v2"])),
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

        let result =
            ingest_vector(&p, branch_id, "default", "test_col", &[batch], &mapping).unwrap();
        assert_eq!(result.rows_imported, 2);
        assert_eq!(result.rows_skipped, 0);

        // Verify via vector get.
        let entry = convert_vector_result(
            p.vector.get(branch_id, "default", "test_col", "v1"),
            branch_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(entry.value.embedding.len(), 3);
        assert_eq!(entry.value.embedding[0], 1.0);
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
