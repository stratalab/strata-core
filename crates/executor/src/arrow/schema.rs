//! Column mapping and type coercion for Arrow import.
//!
//! Resolves which columns in an input file map to key/value/embedding/metadata,
//! and converts Arrow array values to Strata `Value` types.

use arrow::array::{self, Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use strata_core::Value;

use crate::{Error, Result};

/// Target primitive for import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportPrimitive {
    /// Key-value pairs.
    Kv,
    /// JSON documents.
    Json,
    /// Vector embeddings.
    Vector,
}

/// Resolved column mapping from an Arrow schema.
#[derive(Debug)]
pub struct ImportMapping {
    /// Column index for the key field.
    pub key_idx: usize,
    /// Column index for the explicit value/document/embedding field (if any).
    pub value_idx: Option<usize>,
    /// Column indices for remaining fields (become JSON metadata).
    pub extra_indices: Vec<usize>,
    /// Column names for extra fields (for JSON serialization).
    pub extra_names: Vec<String>,
}

/// Resolve column mapping from CLI flags + file schema.
pub fn resolve_mapping(
    schema: &Schema,
    primitive: ImportPrimitive,
    key_column: Option<&str>,
    value_column: Option<&str>,
) -> Result<ImportMapping> {
    let key_idx = resolve_key_column(schema, key_column)?;

    let (value_idx, extra_indices, extra_names) = match primitive {
        ImportPrimitive::Kv => resolve_kv_value(schema, key_idx, value_column)?,
        ImportPrimitive::Json => resolve_json_document(schema, key_idx, value_column)?,
        ImportPrimitive::Vector => resolve_vector_embedding(schema, key_idx, value_column)?,
    };

    Ok(ImportMapping {
        key_idx,
        value_idx,
        extra_indices,
        extra_names,
    })
}

/// Extract a single value from an Arrow array at the given row index.
pub fn arrow_to_value(col: &dyn Array, row: usize) -> Result<Value> {
    if col.is_null(row) {
        return Ok(Value::Null);
    }

    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<array::StringArray>().unwrap();
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::LargeStringArray>()
                .unwrap();
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<array::Int8Array>().unwrap();
            Ok(Value::Int(arr.value(row) as i64))
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<array::Int16Array>().unwrap();
            Ok(Value::Int(arr.value(row) as i64))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<array::Int32Array>().unwrap();
            Ok(Value::Int(arr.value(row) as i64))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<array::Int64Array>().unwrap();
            Ok(Value::Int(arr.value(row)))
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<array::UInt8Array>().unwrap();
            Ok(Value::Int(arr.value(row) as i64))
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<array::UInt16Array>().unwrap();
            Ok(Value::Int(arr.value(row) as i64))
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<array::UInt32Array>().unwrap();
            Ok(Value::Int(arr.value(row) as i64))
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<array::UInt64Array>().unwrap();
            Ok(Value::Int(arr.value(row) as i64))
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<array::Float32Array>().unwrap();
            Ok(Value::Float(arr.value(row) as f64))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<array::Float64Array>().unwrap();
            Ok(Value::Float(arr.value(row)))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<array::BooleanArray>().unwrap();
            Ok(Value::Bool(arr.value(row)))
        }
        DataType::Binary => {
            let arr = col.as_any().downcast_ref::<array::BinaryArray>().unwrap();
            Ok(Value::Bytes(arr.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let arr = col
                .as_any()
                .downcast_ref::<array::LargeBinaryArray>()
                .unwrap();
            Ok(Value::Bytes(arr.value(row).to_vec()))
        }
        DataType::Null => Ok(Value::Null),
        // List, Struct, and other complex types → JSON-serialized string.
        other => {
            let json_val = array_value_to_json(col, row)?;
            let s = serde_json::to_string(&json_val).map_err(|e| Error::Io {
                reason: format!("failed to serialize {other} value to JSON: {e}"),
                hint: None,
            })?;
            Ok(Value::String(s))
        }
    }
}

/// Serialize specified columns of a row as a JSON object string.
pub fn row_to_json(batch: &RecordBatch, row: usize, columns: &[(usize, &str)]) -> Result<String> {
    let mut map = serde_json::Map::new();
    for &(idx, name) in columns {
        let col = batch.column(idx);
        if col.is_null(row) {
            map.insert(name.to_string(), serde_json::Value::Null);
        } else {
            let json_val = array_value_to_json(col.as_ref(), row)?;
            map.insert(name.to_string(), json_val);
        }
    }
    serde_json::to_string(&serde_json::Value::Object(map)).map_err(|e| Error::Io {
        reason: format!("failed to serialize row to JSON: {e}"),
        hint: None,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn resolve_key_column(schema: &Schema, key_column: Option<&str>) -> Result<usize> {
    if let Some(name) = key_column {
        return schema.index_of(name).map_err(|_| Error::InvalidInput {
            reason: format!(
                "key column '{name}' not found. Available columns: {}",
                format_columns(schema)
            ),
            hint: None,
        });
    }

    for candidate in &["key", "_id", "id"] {
        if let Ok(idx) = schema.index_of(candidate) {
            return Ok(idx);
        }
    }

    Err(Error::InvalidInput {
        reason: format!(
            "no key column found. Available columns: {}",
            format_columns(schema)
        ),
        hint: Some("Specify --key-column <COL>".into()),
    })
}

fn resolve_kv_value(
    schema: &Schema,
    key_idx: usize,
    value_column: Option<&str>,
) -> Result<(Option<usize>, Vec<usize>, Vec<String>)> {
    if let Some(name) = value_column {
        let idx = schema.index_of(name).map_err(|_| Error::InvalidInput {
            reason: format!(
                "value column '{name}' not found. Available columns: {}",
                format_columns(schema)
            ),
            hint: None,
        })?;
        let (extra_i, extra_n) = collect_extras(schema, &[key_idx, idx]);
        return Ok((Some(idx), extra_i, extra_n));
    }

    // Auto-detect: try "value".
    if let Ok(idx) = schema.index_of("value") {
        let (extra_i, extra_n) = collect_extras(schema, &[key_idx, idx]);
        return Ok((Some(idx), extra_i, extra_n));
    }

    // 2-column shortcut: the non-key column is the value.
    if schema.fields().len() == 2 {
        let idx = if key_idx == 0 { 1 } else { 0 };
        return Ok((Some(idx), vec![], vec![]));
    }

    // Remaining columns become a JSON object.
    let (extra_i, extra_n) = collect_extras(schema, &[key_idx]);
    Ok((None, extra_i, extra_n))
}

fn resolve_json_document(
    schema: &Schema,
    key_idx: usize,
    value_column: Option<&str>,
) -> Result<(Option<usize>, Vec<usize>, Vec<String>)> {
    if let Some(name) = value_column {
        let idx = schema.index_of(name).map_err(|_| Error::InvalidInput {
            reason: format!(
                "document column '{name}' not found. Available columns: {}",
                format_columns(schema)
            ),
            hint: None,
        })?;
        let (extra_i, extra_n) = collect_extras(schema, &[key_idx, idx]);
        return Ok((Some(idx), extra_i, extra_n));
    }

    for candidate in &["document", "value", "doc", "body"] {
        if let Ok(idx) = schema.index_of(candidate) {
            let (extra_i, extra_n) = collect_extras(schema, &[key_idx, idx]);
            return Ok((Some(idx), extra_i, extra_n));
        }
    }

    // No explicit document column: all non-key columns become JSON.
    let (extra_i, extra_n) = collect_extras(schema, &[key_idx]);
    Ok((None, extra_i, extra_n))
}

fn resolve_vector_embedding(
    schema: &Schema,
    key_idx: usize,
    value_column: Option<&str>,
) -> Result<(Option<usize>, Vec<usize>, Vec<String>)> {
    let idx = if let Some(name) = value_column {
        schema.index_of(name).map_err(|_| Error::InvalidInput {
            reason: format!(
                "embedding column '{name}' not found. Available columns: {}",
                format_columns(schema)
            ),
            hint: None,
        })?
    } else {
        let mut found = None;
        for candidate in &["embedding", "vector", "embeddings", "emb"] {
            if let Ok(idx) = schema.index_of(candidate) {
                found = Some(idx);
                break;
            }
        }
        found.ok_or_else(|| Error::InvalidInput {
            reason: format!(
                "no embedding column found. Available columns: {}",
                format_columns(schema)
            ),
            hint: Some("Specify --value-column <COL> pointing to a float list column".into()),
        })?
    };

    // Validate that the column is a list of floats.
    let field = schema.field(idx);
    match field.data_type() {
        DataType::FixedSizeList(inner, _) | DataType::List(inner) => match inner.data_type() {
            DataType::Float32 | DataType::Float64 => {}
            dt => {
                return Err(Error::InvalidInput {
                    reason: format!(
                        "embedding column '{}' has inner type {dt}, expected Float32 or Float64",
                        field.name()
                    ),
                    hint: None,
                });
            }
        },
        dt => {
            return Err(Error::InvalidInput {
                reason: format!(
                    "embedding column '{}' has type {dt}, expected FixedSizeList<f32> or List<f32>",
                    field.name()
                ),
                hint: None,
            });
        }
    }

    let (extra_i, extra_n) = collect_extras(schema, &[key_idx, idx]);
    Ok((Some(idx), extra_i, extra_n))
}

fn collect_extras(schema: &Schema, exclude: &[usize]) -> (Vec<usize>, Vec<String>) {
    let mut indices = Vec::new();
    let mut names = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        if !exclude.contains(&i) {
            indices.push(i);
            names.push(field.name().clone());
        }
    }
    (indices, names)
}

fn format_columns(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("{} ({})", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Convert a single array element to a serde_json::Value.
fn array_value_to_json(col: &dyn Array, row: usize) -> Result<serde_json::Value> {
    if col.is_null(row) {
        return Ok(serde_json::Value::Null);
    }

    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<array::StringArray>().unwrap();
            Ok(serde_json::Value::String(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::LargeStringArray>()
                .unwrap();
            Ok(serde_json::Value::String(arr.value(row).to_string()))
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<array::Int8Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<array::Int16Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<array::Int32Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<array::Int64Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<array::UInt8Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<array::UInt16Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<array::UInt32Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<array::UInt64Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<array::Float32Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<array::Float64Array>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<array::BooleanArray>().unwrap();
            Ok(serde_json::json!(arr.value(row)))
        }
        _ => {
            // Fallback: use Arrow's JSON formatter via display.
            let formatter = arrow::util::display::ArrayFormatter::try_new(col, &Default::default())
                .map_err(|e| Error::Io {
                    reason: format!("failed to format array value: {e}"),
                    hint: None,
                })?;
            let s = formatter.value(row).to_string();
            Ok(serde_json::Value::String(s))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn schema(fields: Vec<(&str, DataType)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        )
    }

    // -----------------------------------------------------------------------
    // Column resolution tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_key_column_auto() {
        let s = schema(vec![("key", DataType::Utf8), ("value", DataType::Utf8)]);
        let m = resolve_mapping(&s, ImportPrimitive::Kv, None, None).unwrap();
        assert_eq!(m.key_idx, 0);
    }

    #[test]
    fn test_resolve_key_column_id_fallback() {
        let s = schema(vec![
            ("name", DataType::Utf8),
            ("id", DataType::Utf8),
            ("value", DataType::Utf8),
        ]);
        let m = resolve_mapping(&s, ImportPrimitive::Kv, None, None).unwrap();
        assert_eq!(m.key_idx, 1);
    }

    #[test]
    fn test_resolve_key_column_explicit() {
        let s = schema(vec![("user_id", DataType::Utf8), ("value", DataType::Utf8)]);
        let m = resolve_mapping(&s, ImportPrimitive::Kv, Some("user_id"), None).unwrap();
        assert_eq!(m.key_idx, 0);
    }

    #[test]
    fn test_resolve_key_column_missing() {
        let s = schema(vec![("name", DataType::Utf8), ("email", DataType::Utf8)]);
        let err = resolve_mapping(&s, ImportPrimitive::Kv, None, None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no key column found"), "got: {msg}");
        assert!(
            msg.contains("name"),
            "should list available columns, got: {msg}"
        );
    }

    #[test]
    fn test_resolve_value_column_kv() {
        let s = schema(vec![("key", DataType::Utf8), ("value", DataType::Utf8)]);
        let m = resolve_mapping(&s, ImportPrimitive::Kv, None, None).unwrap();
        assert_eq!(m.value_idx, Some(1));
        assert!(m.extra_indices.is_empty());
    }

    #[test]
    fn test_resolve_embedding_column() {
        let s = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                false,
            ),
        ]);
        let m = resolve_mapping(&s, ImportPrimitive::Vector, None, None).unwrap();
        assert_eq!(m.value_idx, Some(1));
    }

    #[test]
    fn test_resolve_embedding_wrong_type() {
        let s = schema(vec![("key", DataType::Utf8), ("embedding", DataType::Utf8)]);
        let err = resolve_mapping(&s, ImportPrimitive::Vector, None, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("expected FixedSizeList") || msg.contains("expected List"),
            "got: {msg}"
        );
    }

    #[test]
    fn test_resolve_extra_columns_as_json() {
        let s = schema(vec![
            ("id", DataType::Utf8),
            ("name", DataType::Utf8),
            ("email", DataType::Utf8),
            ("age", DataType::Int64),
        ]);
        let m = resolve_mapping(&s, ImportPrimitive::Kv, Some("id"), None).unwrap();
        // No "value" column → all non-key columns become extras.
        assert_eq!(m.value_idx, None);
        assert_eq!(m.extra_indices, vec![1, 2, 3]);
        assert_eq!(m.extra_names, vec!["name", "email", "age"]);
    }

    // -----------------------------------------------------------------------
    // Type coercion tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_arrow_to_value_string() {
        let arr = StringArray::from(vec!["hello"]);
        let val = arrow_to_value(&arr, 0).unwrap();
        assert_eq!(val, Value::String("hello".into()));
    }

    #[test]
    fn test_arrow_to_value_int() {
        let arr = Int64Array::from(vec![42]);
        let val = arrow_to_value(&arr, 0).unwrap();
        assert_eq!(val, Value::Int(42));
    }

    #[test]
    fn test_arrow_to_value_float() {
        let arr = Float64Array::from(vec![2.5]);
        let val = arrow_to_value(&arr, 0).unwrap();
        assert_eq!(val, Value::Float(2.5));
    }

    #[test]
    fn test_arrow_to_value_bool() {
        let arr = BooleanArray::from(vec![true]);
        let val = arrow_to_value(&arr, 0).unwrap();
        assert_eq!(val, Value::Bool(true));
    }

    #[test]
    fn test_arrow_to_value_null() {
        let arr = Int64Array::from(vec![None::<i64>]);
        let val = arrow_to_value(&arr, 0).unwrap();
        assert_eq!(val, Value::Null);
    }

    // -----------------------------------------------------------------------
    // Additional coverage
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_kv_two_column_shortcut() {
        // Schema has exactly 2 columns, neither named "value" — the non-key column
        // should be auto-selected as the value.
        let s = schema(vec![("id", DataType::Utf8), ("payload", DataType::Utf8)]);
        let m = resolve_mapping(&s, ImportPrimitive::Kv, Some("id"), None).unwrap();
        assert_eq!(m.key_idx, 0);
        assert_eq!(m.value_idx, Some(1));
        assert!(m.extra_indices.is_empty());
    }

    #[test]
    fn test_row_to_json() {
        let s = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["k1"])) as _,
                Arc::new(StringArray::from(vec!["Alice"])) as _,
                Arc::new(Int64Array::from(vec![30])) as _,
            ],
        )
        .unwrap();

        // Serialize columns 1 ("name") and 2 ("age") as JSON.
        let json_str = row_to_json(&batch, 0, &[(1, "name"), (2, "age")]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["name"], "Alice");
        assert_eq!(parsed["age"], 30);
    }
}
