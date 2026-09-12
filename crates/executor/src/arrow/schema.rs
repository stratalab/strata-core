//! Arrow schema mapping and row coercion.

use arrow::array::{self, Array};
use arrow::datatypes::{ArrowNativeType, DataType, Schema};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use serde_json::Value;

use crate::error::ExecutorError;
use crate::types::ArrowImportTarget;

use super::{internal_error, invalid_input};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ImportMapping {
    pub(crate) key_idx: usize,
    pub(crate) value_idx: Option<usize>,
    pub(crate) metadata_idx: Option<usize>,
    pub(crate) extra_indices: Vec<usize>,
    pub(crate) extra_names: Vec<String>,
    pub(crate) key_encoding_idx: Option<usize>,
    pub(crate) value_encoding_idx: Option<usize>,
}

pub(crate) fn resolve_mapping(
    schema: &Schema,
    target: ArrowImportTarget,
    key_column: Option<&str>,
    value_column: Option<&str>,
) -> Result<ImportMapping, ExecutorError> {
    let key_idx = resolve_key_column(schema, key_column)?;
    let (value_idx, mut extra_indices, mut extra_names) = match target {
        ArrowImportTarget::Kv => resolve_kv_value(schema, key_idx, value_column)?,
        ArrowImportTarget::Json => resolve_json_document(schema, key_idx, value_column)?,
        ArrowImportTarget::Vector => resolve_vector_embedding(schema, key_idx, value_column)?,
        // Graph and Event import use dedicated schemas and never call
        // `resolve_mapping`; these arms keep the match exhaustive defensively.
        ArrowImportTarget::Graph => {
            return Err(internal_error(
                "graph Arrow import does not use single-column mapping",
            ));
        }
        ArrowImportTarget::Event => {
            return Err(internal_error(
                "event Arrow import does not use single-column mapping",
            ));
        }
    };
    // A vector export writes a designated JSON `metadata` column plus an internal
    // `vector_revision`. Treat metadata as a document (parsed by `vector_metadata`,
    // not re-wrapped as a string) and drop both it and the revision from the
    // generic extras bundle so neither leaks into reconstructed metadata.
    let metadata_idx = if matches!(target, ArrowImportTarget::Vector) {
        let metadata_idx = schema.index_of("metadata").ok();
        remove_extra(&mut extra_indices, &mut extra_names, metadata_idx);
        remove_extra(
            &mut extra_indices,
            &mut extra_names,
            schema.index_of("vector_revision").ok(),
        );
        metadata_idx
    } else {
        None
    };
    // `key_encoding` / `value_encoding` are optional metadata columns; a
    // missing column (`index_of` returns Err) means "no encoding override",
    // not a schema error, so the lookup collapses to `None`.
    let key_encoding_idx = schema.index_of("key_encoding").ok();
    let value_encoding_idx = schema.index_of("value_encoding").ok();
    Ok(ImportMapping {
        key_idx,
        value_idx,
        metadata_idx,
        extra_indices,
        extra_names,
        key_encoding_idx,
        value_encoding_idx,
    })
}

pub(crate) fn key_bytes(
    batch: &RecordBatch,
    mapping: &ImportMapping,
    row: usize,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    let key_col = batch.column(mapping.key_idx);
    if key_col.is_null(row) {
        return Ok(None);
    }
    let encoding = encoding_at(batch, mapping.key_encoding_idx, row);
    cell_to_bytes(key_col.as_ref(), row, encoding.as_deref()).map(Some)
}

pub(crate) fn value_bytes(
    batch: &RecordBatch,
    mapping: &ImportMapping,
    row: usize,
) -> Result<Vec<u8>, ExecutorError> {
    if let Some(value_idx) = mapping.value_idx {
        let value_col = batch.column(value_idx);
        let encoding = encoding_at(batch, mapping.value_encoding_idx, row);
        return cell_to_bytes(value_col.as_ref(), row, encoding.as_deref());
    }
    let value = row_to_json_object(batch, row, &mapping.extra_indices, &mapping.extra_names)?;
    serde_json::to_vec(&value).map_err(|error| {
        internal_error(format!(
            "failed to serialize Arrow row as JSON bytes: {error}"
        ))
    })
}

pub(crate) fn json_document(
    batch: &RecordBatch,
    mapping: &ImportMapping,
    row: usize,
) -> Result<Value, ExecutorError> {
    if let Some(value_idx) = mapping.value_idx {
        return cell_to_json_document(batch.column(value_idx).as_ref(), row);
    }
    row_to_json_object(batch, row, &mapping.extra_indices, &mapping.extra_names)
}

pub(crate) fn vector_embedding(
    batch: &RecordBatch,
    mapping: &ImportMapping,
    row: usize,
) -> Option<Vec<f32>> {
    let value_idx = mapping.value_idx?;
    let column = batch.column(value_idx);
    extract_embedding(column.as_ref(), row)
}

pub(crate) fn vector_metadata(
    batch: &RecordBatch,
    mapping: &ImportMapping,
    row: usize,
) -> Result<Option<Value>, ExecutorError> {
    // A designated `metadata` column (as written by `arrow export vector`) is a
    // JSON document: parse it and return it unwrapped, matching what was stored.
    if let Some(metadata_idx) = mapping.metadata_idx {
        let column = batch.column(metadata_idx);
        if column.is_null(row) {
            return Ok(None);
        }
        return Ok(Some(cell_to_json_document(column.as_ref(), row)?));
    }
    // Hand-authored files without a `metadata` column: bundle any stray columns.
    if mapping.extra_indices.is_empty() {
        return Ok(None);
    }
    Ok(Some(row_to_json_object(
        batch,
        row,
        &mapping.extra_indices,
        &mapping.extra_names,
    )?))
}

pub(crate) fn row_to_json_object(
    batch: &RecordBatch,
    row: usize,
    indices: &[usize],
    names: &[String],
) -> Result<Value, ExecutorError> {
    let mut map = serde_json::Map::new();
    for (index, name) in indices.iter().zip(names) {
        let column = batch.column(*index);
        map.insert(name.clone(), cell_to_json(column.as_ref(), row)?);
    }
    Ok(Value::Object(map))
}

fn resolve_key_column(schema: &Schema, key_column: Option<&str>) -> Result<usize, ExecutorError> {
    if let Some(column) = key_column {
        return schema.index_of(column).map_err(|_| {
            invalid_input(
                "invalid_argument.executor.arrow_key_column",
                format!(
                    "key column '{column}' not found; available columns: {}",
                    format_columns(schema)
                ),
            )
        });
    }
    for candidate in ["key", "_id", "id"] {
        if let Ok(index) = schema.index_of(candidate) {
            return Ok(index);
        }
    }
    Err(invalid_input(
        "invalid_argument.executor.arrow_key_column",
        format!(
            "no key column found; available columns: {}",
            format_columns(schema)
        ),
    ))
}

/// Where a primitive's value lives in an Arrow schema, and what else came with
/// it: the value column's index when the schema names one, then the indices and
/// names of every other column, kept parallel.
type ValueColumns = (Option<usize>, Vec<usize>, Vec<String>);

fn resolve_kv_value(
    schema: &Schema,
    key_idx: usize,
    value_column: Option<&str>,
) -> Result<ValueColumns, ExecutorError> {
    if let Some(column) = value_column {
        let value_idx = schema.index_of(column).map_err(|_| {
            invalid_input(
                "invalid_argument.executor.arrow_value_column",
                format!(
                    "value column '{column}' not found; available columns: {}",
                    format_columns(schema)
                ),
            )
        })?;
        return Ok(resolve_extras(schema, &[key_idx, value_idx]));
    }
    if let Ok(value_idx) = schema.index_of("value") {
        return Ok(resolve_extras(schema, &[key_idx, value_idx]));
    }
    // 2-column shortcut: the single non-key column is the value — but only if it
    // is an actual value, not an encoding metadata column (#3083: a `key` +
    // `value_encoding` file must not treat the encoding as the value).
    if schema.fields().len() == 2 {
        let value_idx = usize::from(key_idx == 0);
        if !is_kv_encoding_column(schema.field(value_idx).name()) {
            return Ok((Some(value_idx), Vec::new(), Vec::new()));
        }
    }
    // Otherwise the value is a JSON object of the remaining columns — excluding
    // the encoding metadata columns. If none remain, there is no value to import;
    // fail instead of fabricating `b"{}"` for every key (#3083).
    let mut exclude = vec![key_idx];
    for name in ["key_encoding", "value_encoding"] {
        if let Ok(index) = schema.index_of(name) {
            exclude.push(index);
        }
    }
    let (extra_indices, extra_names) = collect_extras(schema, &exclude);
    if extra_indices.is_empty() {
        return Err(invalid_input(
            "invalid_argument.executor.arrow_value_column",
            format!(
                "no value column found; provide a `value` column, pass --value-column, or include value columns. available columns: {}",
                format_columns(schema)
            ),
        ));
    }
    Ok((None, extra_indices, extra_names))
}

/// Whether a column name is a KV encoding metadata column (never a value).
fn is_kv_encoding_column(name: &str) -> bool {
    matches!(name, "key_encoding" | "value_encoding")
}

fn resolve_json_document(
    schema: &Schema,
    key_idx: usize,
    value_column: Option<&str>,
) -> Result<ValueColumns, ExecutorError> {
    if let Some(column) = value_column {
        let value_idx = schema.index_of(column).map_err(|_| {
            invalid_input(
                "invalid_argument.executor.arrow_value_column",
                format!(
                    "document column '{column}' not found; available columns: {}",
                    format_columns(schema)
                ),
            )
        })?;
        return Ok(resolve_extras(schema, &[key_idx, value_idx]));
    }
    for candidate in ["document", "value", "doc", "body"] {
        if let Ok(value_idx) = schema.index_of(candidate) {
            return Ok(resolve_extras(schema, &[key_idx, value_idx]));
        }
    }
    let (extra_indices, extra_names) = collect_extras(schema, &[key_idx]);
    Ok((None, extra_indices, extra_names))
}

fn resolve_vector_embedding(
    schema: &Schema,
    key_idx: usize,
    value_column: Option<&str>,
) -> Result<ValueColumns, ExecutorError> {
    let value_idx = if let Some(column) = value_column {
        schema.index_of(column).map_err(|_| {
            invalid_input(
                "invalid_argument.executor.arrow_value_column",
                format!(
                    "embedding column '{column}' not found; available columns: {}",
                    format_columns(schema)
                ),
            )
        })?
    } else {
        ["embedding", "vector", "embeddings", "emb"]
            .iter()
            // Probe known embedding column aliases; `.ok()` skips names that
            // are absent so the first present alias wins.
            .find_map(|candidate| schema.index_of(candidate).ok())
            .ok_or_else(|| {
                invalid_input(
                    "invalid_argument.executor.arrow_value_column",
                    format!(
                        "no embedding column found; available columns: {}",
                        format_columns(schema)
                    ),
                )
            })?
    };
    let field = schema.field(value_idx);
    match field.data_type() {
        DataType::FixedSizeList(inner, _) | DataType::List(inner) => match inner.data_type() {
            DataType::Float32 | DataType::Float64 => {}
            data_type => {
                return Err(invalid_input(
                    "invalid_argument.executor.arrow_embedding_type",
                    format!(
                        "embedding column '{}' has inner type {data_type}; expected Float32 or Float64",
                        field.name()
                    ),
                ));
            }
        },
        data_type => {
            return Err(invalid_input(
                "invalid_argument.executor.arrow_embedding_type",
                format!(
                    "embedding column '{}' has type {data_type}; expected a float list",
                    field.name()
                ),
            ));
        }
    }
    Ok(resolve_extras(schema, &[key_idx, value_idx]))
}

fn resolve_extras(schema: &Schema, exclude: &[usize]) -> ValueColumns {
    let value_idx = exclude.last().copied();
    let (extra_indices, extra_names) = collect_extras(schema, exclude);
    (value_idx, extra_indices, extra_names)
}

/// Removes a column from the parallel `(indices, names)` extras vectors, keeping
/// them in sync. Used to pull designated/internal vector columns out of the
/// generic extras bundle.
fn remove_extra(indices: &mut Vec<usize>, names: &mut Vec<String>, target: Option<usize>) {
    let Some(target) = target else {
        return;
    };
    if let Some(position) = indices.iter().position(|&index| index == target) {
        indices.remove(position);
        names.remove(position);
    }
}

fn collect_extras(schema: &Schema, exclude: &[usize]) -> (Vec<usize>, Vec<String>) {
    let mut indices = Vec::new();
    let mut names = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        let ignored = exclude.contains(&index)
            || matches!(
                field.name().as_str(),
                "key_encoding" | "value_encoding" | "version" | "timestamp"
            );
        if !ignored {
            indices.push(index);
            names.push(field.name().clone());
        }
    }
    (indices, names)
}

fn format_columns(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|field| format!("{} ({})", field.name(), field.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn encoding_at(batch: &RecordBatch, index: Option<usize>, row: usize) -> Option<String> {
    index
        // A cell that cannot be read as a string means "no per-row encoding".
        .and_then(|index| cell_to_string(batch.column(index).as_ref(), row).ok())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

fn cell_to_bytes(
    column: &dyn Array,
    row: usize,
    encoding: Option<&str>,
) -> Result<Vec<u8>, ExecutorError> {
    if column.is_null(row) {
        return Ok(Vec::new());
    }
    match encoding {
        Some("base64") => {
            let encoded = cell_to_string(column, row)?;
            return base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .map_err(|error| {
                    invalid_input(
                        "invalid_argument.executor.arrow_base64",
                        format!("failed to decode base64 Arrow cell: {error}"),
                    )
                });
        }
        // `utf8` (what the exporter emits for text) and an absent encoding column
        // fall through to the raw column bytes below.
        None | Some("utf8") => {}
        // Any other declared encoding was silently mis-decoded as raw ASCII on the
        // very column meant to prevent that (#3079); reject it instead.
        Some(other) => {
            return Err(invalid_input(
                "invalid_argument.executor.arrow_encoding",
                format!("unsupported cell encoding '{other}'; expected 'utf8' or 'base64'"),
            ));
        }
    }
    match column.data_type() {
        DataType::Binary => {
            let array = column
                .as_any()
                .downcast_ref::<array::BinaryArray>()
                .unwrap();
            Ok(array.value(row).to_vec())
        }
        DataType::LargeBinary => {
            let array = column
                .as_any()
                .downcast_ref::<array::LargeBinaryArray>()
                .unwrap();
            Ok(array.value(row).to_vec())
        }
        _ => Ok(cell_to_string(column, row)?.into_bytes()),
    }
}

fn cell_to_json_document(column: &dyn Array, row: usize) -> Result<Value, ExecutorError> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    match column.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let text = cell_to_string(column, row)?;
            Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
        }
        _ => cell_to_json(column, row),
    }
}

/// JSON has no representation for non-finite floats: `serde_json::json!(NaN|Inf)`
/// silently yields `Value::Null` (`Number::from_f64` returns `None`), dropping
/// the number under a successful import (#3078). Reject them with a typed error
/// instead of corrupting the document. Finite floats produce the exact JSON
/// `serde_json` already produced (an f32 widens to f64 identically).
fn finite_float_to_json(value: f64) -> Result<Value, ExecutorError> {
    if value.is_finite() {
        Ok(serde_json::json!(value))
    } else {
        Err(invalid_input(
            "invalid_argument.executor.arrow_non_finite_float",
            format!("a float column holds a non-finite value ({value}) that JSON cannot represent"),
        ))
    }
}

fn cell_to_json(column: &dyn Array, row: usize) -> Result<Value, ExecutorError> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    match column.data_type() {
        DataType::Null => Ok(Value::Null),
        DataType::Int8 => {
            let array = column.as_any().downcast_ref::<array::Int8Array>().unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::Int16 => {
            let array = column.as_any().downcast_ref::<array::Int16Array>().unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<array::Int32Array>().unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<array::Int64Array>().unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::UInt8 => {
            let array = column.as_any().downcast_ref::<array::UInt8Array>().unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::UInt16 => {
            let array = column
                .as_any()
                .downcast_ref::<array::UInt16Array>()
                .unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::UInt32 => {
            let array = column
                .as_any()
                .downcast_ref::<array::UInt32Array>()
                .unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::UInt64 => {
            let array = column
                .as_any()
                .downcast_ref::<array::UInt64Array>()
                .unwrap();
            Ok(serde_json::json!(array.value(row)))
        }
        DataType::Float32 => {
            let array = column
                .as_any()
                .downcast_ref::<array::Float32Array>()
                .unwrap();
            finite_float_to_json(f64::from(array.value(row)))
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<array::Float64Array>()
                .unwrap();
            finite_float_to_json(array.value(row))
        }
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<array::BooleanArray>()
                .unwrap();
            Ok(Value::Bool(array.value(row)))
        }
        DataType::Binary => {
            let array = column
                .as_any()
                .downcast_ref::<array::BinaryArray>()
                .unwrap();
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(array.value(row)),
            ))
        }
        DataType::LargeBinary => {
            let array = column
                .as_any()
                .downcast_ref::<array::LargeBinaryArray>()
                .unwrap();
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(array.value(row)),
            ))
        }
        DataType::Struct(fields) => {
            // #3063: reconstruct a real JSON object from the struct's fields
            // instead of falling through to the Arrow `Display` string (which
            // stored a lossy, unqueryable rendering and dropped nulls).
            let array = column
                .as_any()
                .downcast_ref::<array::StructArray>()
                .unwrap();
            let mut object = serde_json::Map::with_capacity(fields.len());
            for (field, child) in fields.iter().zip(array.columns()) {
                object.insert(field.name().clone(), cell_to_json(child.as_ref(), row)?);
            }
            Ok(Value::Object(object))
        }
        DataType::List(_) => {
            // #3063: a JSON array field becomes an Arrow list; reconstruct the
            // array element-by-element rather than the Arrow `Display` string.
            let array = column.as_any().downcast_ref::<array::ListArray>().unwrap();
            let values = array.value(row);
            let mut items = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                items.push(cell_to_json(values.as_ref(), index)?);
            }
            Ok(Value::Array(items))
        }
        DataType::LargeList(_) => {
            // #3075: a Parquet LargeList value column must reconstruct a JSON
            // array, not fall to the lossy Arrow `Display` string.
            let array = column
                .as_any()
                .downcast_ref::<array::LargeListArray>()
                .unwrap();
            let values = array.value(row);
            let mut items = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                items.push(cell_to_json(values.as_ref(), index)?);
            }
            Ok(Value::Array(items))
        }
        DataType::FixedSizeList(_, _) => {
            // #3075: a Parquet FixedSizeList value column must reconstruct a JSON
            // array, not fall to the lossy Arrow `Display` string.
            let array = column
                .as_any()
                .downcast_ref::<array::FixedSizeListArray>()
                .unwrap();
            let values = array.value(row);
            let mut items = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                items.push(cell_to_json(values.as_ref(), index)?);
            }
            Ok(Value::Array(items))
        }
        DataType::Map(_, _) => {
            // #3091: a Map value column reconstructs a JSON object (string keys),
            // not the Arrow `Display` string.
            let array = column.as_any().downcast_ref::<array::MapArray>().unwrap();
            let entries = array.value(row);
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut object = serde_json::Map::with_capacity(entries.len());
            for index in 0..entries.len() {
                let key = cell_to_string(keys.as_ref(), index)?;
                object.insert(key, cell_to_json(values.as_ref(), index)?);
            }
            Ok(Value::Object(object))
        }
        DataType::Dictionary(_, _) => {
            // #3091: a dictionary is an encoding, not a semantic type — decode to
            // the underlying value's JSON (a dict-encoded int is the number 42,
            // not the string "42"). Read the single key at `row` (O(1)); the
            // whole-column `normalized_keys()` would be O(n) per cell, i.e. O(n^2)
            // over the row loop. Null cells are handled by the is_null guard
            // above, so the key here always references a present value.
            arrow::array::downcast_dictionary_array!(
                column => {
                    let value_index = column.keys().value(row).as_usize();
                    cell_to_json(column.values().as_ref(), value_index)
                }
                _ => unreachable!("outer match guarantees a dictionary array"),
            )
        }
        // Timestamp/Date/Time/Duration/Decimal remain the Arrow `Display` string:
        // JSON has no native temporal or decimal type, so ISO-8601 / decimal text
        // is a lossless representation (a JSON number would lose Decimal
        // precision). Tracked as `value.temporal_decimal` (accepted) in the
        // conformance ledger.
        _ => Ok(Value::String(cell_to_string(column, row)?)),
    }
}

fn cell_to_string(column: &dyn Array, row: usize) -> Result<String, ExecutorError> {
    if column.is_null(row) {
        return Ok(String::new());
    }
    match column.data_type() {
        DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<array::StringArray>()
                .unwrap();
            Ok(array.value(row).to_owned())
        }
        DataType::LargeUtf8 => {
            let array = column
                .as_any()
                .downcast_ref::<array::LargeStringArray>()
                .unwrap();
            Ok(array.value(row).to_owned())
        }
        _ => {
            let formatter = arrow::util::display::ArrayFormatter::try_new(
                column,
                &arrow::util::display::FormatOptions::default(),
            )
            .map_err(|error| {
                internal_error(format!("failed to format Arrow cell as string: {error}"))
            })?;
            Ok(formatter.value(row).to_string())
        }
    }
}

fn extract_embedding(column: &dyn Array, row: usize) -> Option<Vec<f32>> {
    if column.is_null(row) {
        return None;
    }
    let values = match column.data_type() {
        DataType::FixedSizeList(_, _) => {
            let array = column
                .as_any()
                .downcast_ref::<array::FixedSizeListArray>()?;
            array.value(row)
        }
        DataType::List(_) => {
            let array = column.as_any().downcast_ref::<array::ListArray>()?;
            array.value(row)
        }
        _ => return None,
    };
    if let Some(array) = values.as_any().downcast_ref::<array::Float32Array>() {
        if (0..array.len()).any(|index| array.is_null(index)) {
            return None;
        }
        let vector = array.values().to_vec();
        // #3083: reject a non-finite (NaN/±Inf) component, consistent with the
        // Float64 path — importing it would poison every downstream distance.
        if vector.iter().any(|value| !value.is_finite()) {
            return None;
        }
        return Some(vector);
    }
    values
        .as_any()
        .downcast_ref::<array::Float64Array>()
        .and_then(|array| {
            if (0..array.len()).any(|index| array.is_null(index)) {
                return None;
            }
            array
                .values()
                .iter()
                .copied()
                .map(f64_embedding_value_to_f32)
                .collect()
        })
}

#[allow(clippy::cast_possible_truncation)]
fn f64_embedding_value_to_f32(value: f64) -> Option<f32> {
    if value.is_finite() && value >= f64::from(f32::MIN) && value <= f64::from(f32::MAX) {
        Some(value as f32)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BinaryArray, BooleanArray, DictionaryArray, FixedSizeListBuilder, Float32Array,
        Float32Builder, Float64Array, Float64Builder, Int16Array, Int32Array, Int64Array,
        Int64Builder, Int8Array, LargeBinaryArray, LargeStringArray, ListBuilder, MapBuilder,
        NullArray, StringArray, StringBuilder, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::datatypes::{Field, Int32Type, Int64Type};
    use base64::Engine;
    use serde_json::json;

    use crate::ExecutorErrorClass;

    use super::*;

    #[test]
    fn key_value_and_document_columns_follow_old_detection_order() {
        let schema = Schema::new(vec![
            utf8_field("_id"),
            utf8_field("id"),
            utf8_field("key"),
            utf8_field("value"),
        ]);
        let mapping =
            resolve_mapping(&schema, ArrowImportTarget::Kv, None, None).expect("mapping resolves");
        assert_eq!(mapping.key_idx, 2);
        assert_eq!(mapping.value_idx, Some(3));

        let schema = Schema::new(vec![
            utf8_field("_id"),
            utf8_field("id"),
            utf8_field("payload"),
        ]);
        let mapping = resolve_mapping(&schema, ArrowImportTarget::Kv, Some("id"), Some("payload"))
            .expect("explicit mapping resolves");
        assert_eq!(mapping.key_idx, 1);
        assert_eq!(mapping.value_idx, Some(2));

        for candidate in ["document", "value", "doc", "body"] {
            let schema = Schema::new(vec![utf8_field("key"), utf8_field(candidate)]);
            let mapping = resolve_mapping(&schema, ArrowImportTarget::Json, None, None)
                .expect("json mapping resolves");
            assert_eq!(mapping.value_idx, Some(1), "{candidate}");
        }
    }

    #[test]
    fn mapping_errors_include_stable_codes_and_available_columns() {
        let schema = Schema::new(vec![utf8_field("name"), utf8_field("payload")]);
        let error = resolve_mapping(&schema, ArrowImportTarget::Kv, None, None)
            .expect_err("missing key fails");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.executor.arrow_key_column");
        assert!(error.message().contains("name (Utf8)"));

        let error = resolve_mapping(&schema, ArrowImportTarget::Json, Some("name"), Some("doc"))
            .expect_err("missing document fails");
        assert_eq!(error.code(), "invalid_argument.executor.arrow_value_column");
        assert!(error.message().contains("payload (Utf8)"));
    }

    #[test]
    fn resolve_kv_value_errors_instead_of_fabricating_a_value() {
        // #3083: a key-only file has no value — must error, not resolve to an
        // empty extras bundle that stores b"{}" for every key.
        let one_col = Schema::new(vec![utf8_field("key")]);
        let error = resolve_kv_value(&one_col, 0, None).expect_err("no value column");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.executor.arrow_value_column");

        // #3083: the 2-column shortcut must not pick an encoding metadata column
        // as the value.
        let key_enc = Schema::new(vec![utf8_field("key"), utf8_field("value_encoding")]);
        let error = resolve_kv_value(&key_enc, 0, None).expect_err("encoding is not a value");
        assert_eq!(error.code(), "invalid_argument.executor.arrow_value_column");

        // Direction control: a real 2-column value column is still picked...
        let key_data = Schema::new(vec![utf8_field("key"), utf8_field("data")]);
        let (value_idx, _, _) = resolve_kv_value(&key_data, 0, None).expect("shortcut value");
        assert_eq!(value_idx, Some(1));
        // ...as is an explicit `value` column, and a multi-column row object.
        let kv = Schema::new(vec![utf8_field("key"), utf8_field("value")]);
        assert_eq!(
            resolve_kv_value(&kv, 0, None).expect("value col").0,
            Some(1)
        );
        let row_obj = Schema::new(vec![utf8_field("key"), utf8_field("a"), utf8_field("b")]);
        let (value_idx, extras, _) = resolve_kv_value(&row_obj, 0, None).expect("row object");
        assert_eq!(value_idx, None);
        assert_eq!(extras, vec![1, 2]);
    }

    #[test]
    fn kv_mapping_uses_two_column_shortcut_and_extra_object_fallback() {
        let schema = Schema::new(vec![utf8_field("id"), utf8_field("payload")]);
        let mapping =
            resolve_mapping(&schema, ArrowImportTarget::Kv, None, None).expect("mapping resolves");
        assert_eq!(mapping.key_idx, 0);
        assert_eq!(mapping.value_idx, Some(1));
        assert!(mapping.extra_indices.is_empty());

        let schema = Schema::new(vec![
            utf8_field("id"),
            utf8_field("name"),
            Field::new("active", DataType::Boolean, false),
        ]);
        let mapping =
            resolve_mapping(&schema, ArrowImportTarget::Kv, None, None).expect("mapping resolves");
        assert_eq!(mapping.value_idx, None);
        assert_eq!(mapping.extra_names, vec!["name", "active"]);
    }

    #[test]
    fn bytes_respect_binary_columns_and_exported_base64_encodings() {
        let encoded_key = base64::engine::general_purpose::STANDARD.encode([0, 255]);
        let encoded_value = base64::engine::general_purpose::STANDARD.encode([255, 254]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                utf8_field("key"),
                utf8_field("key_encoding"),
                utf8_field("value"),
                utf8_field("value_encoding"),
            ])),
            vec![
                Arc::new(StringArray::from(vec![encoded_key])),
                Arc::new(StringArray::from(vec!["base64"])),
                Arc::new(StringArray::from(vec![encoded_value])),
                Arc::new(StringArray::from(vec!["base64"])),
            ],
        )
        .expect("batch");
        let mapping = resolve_mapping(batch.schema().as_ref(), ArrowImportTarget::Kv, None, None)
            .expect("mapping resolves");
        assert_eq!(
            key_bytes(&batch, &mapping, 0).expect("key"),
            Some(vec![0, 255])
        );
        assert_eq!(
            value_bytes(&batch, &mapping, 0).expect("value"),
            vec![255, 254]
        );

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Binary, false),
                Field::new("value", DataType::LargeBinary, false),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![b"k".as_slice()])),
                Arc::new(LargeBinaryArray::from(vec![b"raw".as_slice()])),
            ],
        )
        .expect("binary batch");
        let mapping = resolve_mapping(batch.schema().as_ref(), ArrowImportTarget::Kv, None, None)
            .expect("mapping resolves");
        assert_eq!(
            key_bytes(&batch, &mapping, 0).expect("key"),
            Some(b"k".to_vec())
        );
        assert_eq!(
            value_bytes(&batch, &mapping, 0).expect("value"),
            b"raw".to_vec()
        );
    }

    #[test]
    fn json_document_and_object_conversion_cover_arrow_scalars() {
        let batch = scalar_batch();
        let names = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let indices = (0..batch.num_columns()).collect::<Vec<_>>();
        let value = row_to_json_object(&batch, 0, &indices, &names).expect("json object");
        assert_eq!(
            value,
            json!({
                "utf8": "text",
                "large_utf8": "large",
                "binary": "Ymlu",
                "large_binary": "bGFyZ2U=",
                "int8": -8,
                "int16": -16,
                "int32": -32,
                "int64": -64,
                "uint8": 8,
                "uint16": 16,
                "uint32": 32,
                "uint64": 64,
                "float32": 1.5,
                "float64": 2.5,
                "bool": true,
                "null": null,
            })
        );

        let document_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![utf8_field("key"), utf8_field("document")])),
            vec![
                Arc::new(StringArray::from(vec!["doc-a", "doc-b"])),
                Arc::new(StringArray::from(vec!["{\"ok\":true}", "not-json"])),
            ],
        )
        .expect("document batch");
        let mapping = resolve_mapping(
            document_batch.schema().as_ref(),
            ArrowImportTarget::Json,
            None,
            None,
        )
        .expect("mapping resolves");
        assert_eq!(
            json_document(&document_batch, &mapping, 0).expect("json document"),
            json!({"ok": true})
        );
        assert_eq!(
            json_document(&document_batch, &mapping, 1).expect("json document"),
            json!("not-json")
        );
    }

    #[test]
    fn vector_mapping_accepts_float_lists_and_rejects_other_embedding_shapes() {
        for candidate in ["embedding", "vector", "embeddings", "emb"] {
            let schema = Schema::new(vec![
                utf8_field("key"),
                Field::new(
                    candidate,
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        2,
                    ),
                    false,
                ),
            ]);
            let mapping = resolve_mapping(&schema, ArrowImportTarget::Vector, None, None)
                .expect("vector mapping resolves");
            assert_eq!(mapping.value_idx, Some(1), "{candidate}");
        }

        let non_list = Schema::new(vec![utf8_field("key"), utf8_field("embedding")]);
        let error = resolve_mapping(&non_list, ArrowImportTarget::Vector, None, None)
            .expect_err("non-list embedding fails");
        assert_eq!(
            error.code(),
            "invalid_argument.executor.arrow_embedding_type"
        );

        let non_float_list = Schema::new(vec![
            utf8_field("key"),
            Field::new(
                "embedding",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ]);
        let error = resolve_mapping(&non_float_list, ArrowImportTarget::Vector, None, None)
            .expect_err("non-float embedding fails");
        assert_eq!(
            error.code(),
            "invalid_argument.executor.arrow_embedding_type"
        );
    }

    #[test]
    fn vector_embeddings_accept_supported_lists_and_skip_invalid_rows() {
        let fixed_float64 = fixed_float64_embedding_batch(&[1.0, 2.0, f64::INFINITY, 4.0]);
        let mapping = resolve_mapping(
            fixed_float64.schema().as_ref(),
            ArrowImportTarget::Vector,
            None,
            None,
        )
        .expect("mapping resolves");
        assert_eq!(
            vector_embedding(&fixed_float64, &mapping, 0).expect("embedding"),
            vec![1.0, 2.0]
        );
        assert!(vector_embedding(&fixed_float64, &mapping, 1).is_none());

        let list_float32 = list_float32_embedding_batch();
        let mapping = resolve_mapping(
            list_float32.schema().as_ref(),
            ArrowImportTarget::Vector,
            None,
            None,
        )
        .expect("mapping resolves");
        assert_eq!(
            vector_embedding(&list_float32, &mapping, 0).expect("embedding"),
            vec![3.0, 4.0]
        );
    }

    #[test]
    fn vector_metadata_parses_the_designated_column_without_leaking_internal_fields() {
        // Mirrors the schema `arrow export vector` writes: a JSON-string
        // `metadata` column plus internal version/timestamp/vector_revision
        // columns. Import must parse `metadata` back into its object and never
        // surface `vector_revision`.
        // Row 0 carries metadata; row 1 has none (export writes a null cell),
        // which must round-trip to `None`, not spurious metadata.
        let mut embedding = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        for value in [1.0, 0.0, 0.0, 1.0] {
            embedding.values().append_value(value);
        }
        embedding.append(true);
        embedding.append(true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                utf8_field("key"),
                Field::new(
                    "embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        2,
                    ),
                    false,
                ),
                Field::new("metadata", DataType::Utf8, true),
                Field::new("version", DataType::UInt64, false),
                Field::new("timestamp", DataType::UInt64, false),
                Field::new("vector_revision", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["doc-a", "doc-b"])),
                Arc::new(embedding.finish()),
                Arc::new(StringArray::from(vec![
                    Some("{\"kind\":\"note\",\"rank\":1}"),
                    None,
                ])),
                Arc::new(UInt64Array::from(vec![7_u64, 8])),
                Arc::new(UInt64Array::from(vec![9_u64, 10])),
                Arc::new(UInt64Array::from(vec![1_u64, 2])),
            ],
        )
        .expect("vector batch");
        let mapping = resolve_mapping(
            batch.schema().as_ref(),
            ArrowImportTarget::Vector,
            None,
            None,
        )
        .expect("mapping resolves");
        let metadata = vector_metadata(&batch, &mapping, 0)
            .expect("metadata")
            .expect("metadata present");
        assert_eq!(metadata, json!({"kind": "note", "rank": 1}));
        assert_eq!(
            vector_metadata(&batch, &mapping, 1).expect("metadata"),
            None,
            "a null metadata cell must round-trip to no metadata"
        );
    }

    #[test]
    fn vector_metadata_bundles_user_columns_but_drops_the_internal_revision() {
        // A hand-authored vector file with no designated `metadata` column: the
        // genuine user column is bundled, but the internal `vector_revision` is
        // stripped rather than leaked.
        let mut embedding = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        embedding.values().append_value(1.0);
        embedding.values().append_value(0.0);
        embedding.append(true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                utf8_field("key"),
                Field::new(
                    "embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        2,
                    ),
                    false,
                ),
                utf8_field("note"),
                Field::new("vector_revision", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["doc-a"])),
                Arc::new(embedding.finish()),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(UInt64Array::from(vec![1_u64])),
            ],
        )
        .expect("vector batch");
        let mapping = resolve_mapping(
            batch.schema().as_ref(),
            ArrowImportTarget::Vector,
            None,
            None,
        )
        .expect("mapping resolves");
        assert_eq!(
            vector_metadata(&batch, &mapping, 0)
                .expect("metadata")
                .expect("metadata present"),
            json!({"note": "hello"})
        );
    }

    fn utf8_field(name: &str) -> Field {
        Field::new(name, DataType::Utf8, false)
    }

    fn scalar_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                utf8_field("utf8"),
                Field::new("large_utf8", DataType::LargeUtf8, false),
                Field::new("binary", DataType::Binary, false),
                Field::new("large_binary", DataType::LargeBinary, false),
                Field::new("int8", DataType::Int8, false),
                Field::new("int16", DataType::Int16, false),
                Field::new("int32", DataType::Int32, false),
                Field::new("int64", DataType::Int64, false),
                Field::new("uint8", DataType::UInt8, false),
                Field::new("uint16", DataType::UInt16, false),
                Field::new("uint32", DataType::UInt32, false),
                Field::new("uint64", DataType::UInt64, false),
                Field::new("float32", DataType::Float32, false),
                Field::new("float64", DataType::Float64, false),
                Field::new("bool", DataType::Boolean, false),
                Field::new("null", DataType::Null, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["text"])),
                Arc::new(LargeStringArray::from(vec!["large"])),
                Arc::new(BinaryArray::from(vec![b"bin".as_slice()])),
                Arc::new(LargeBinaryArray::from(vec![b"large".as_slice()])),
                Arc::new(Int8Array::from(vec![-8])),
                Arc::new(Int16Array::from(vec![-16])),
                Arc::new(Int32Array::from(vec![-32])),
                Arc::new(Int64Array::from(vec![-64])),
                Arc::new(UInt8Array::from(vec![8])),
                Arc::new(UInt16Array::from(vec![16])),
                Arc::new(UInt32Array::from(vec![32])),
                Arc::new(UInt64Array::from(vec![64])),
                Arc::new(Float32Array::from(vec![1.5])),
                Arc::new(Float64Array::from(vec![2.5])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(NullArray::new(1)),
            ],
        )
        .expect("scalar batch")
    }

    fn fixed_float64_embedding_batch(values: &[f64; 4]) -> RecordBatch {
        let mut embedding_builder = FixedSizeListBuilder::new(Float64Builder::new(), 2);
        for value in values {
            embedding_builder.values().append_value(*value);
        }
        embedding_builder.append(true);
        embedding_builder.append(true);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                utf8_field("key"),
                Field::new(
                    "embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float64, true)),
                        2,
                    ),
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(embedding_builder.finish()),
            ],
        )
        .expect("embedding batch")
    }

    fn list_float32_embedding_batch() -> RecordBatch {
        let mut embedding_builder = ListBuilder::new(Float32Builder::new());
        embedding_builder.values().append_value(3.0);
        embedding_builder.values().append_value(4.0);
        embedding_builder.append(true);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                utf8_field("key"),
                Field::new(
                    "embedding",
                    DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(embedding_builder.finish()),
            ],
        )
        .expect("embedding batch")
    }

    #[test]
    fn cell_to_json_reconstructs_nested_struct_and_preserves_null() {
        // #3063: a value column that is an Arrow struct must reconstruct a real
        // JSON object — with nested objects and nulls intact — not the Arrow
        // `Display` string (which dropped the null and quoted nothing), which
        // was stored under a success report and made every path query nil.
        let nest: array::ArrayRef = Arc::new(array::StructArray::from(vec![(
            Arc::new(Field::new("k", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["v"])) as array::ArrayRef,
        )]));
        let nest_dt = nest.data_type().clone();
        let doc = array::StructArray::from(vec![
            (
                Arc::new(Field::new("n", DataType::Int64, false)),
                Arc::new(Int64Array::from(vec![1_i64])) as array::ArrayRef,
            ),
            (
                Arc::new(Field::new("nul", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![None::<i64>])) as array::ArrayRef,
            ),
            (Arc::new(Field::new("nest", nest_dt, false)), nest),
        ]);
        let value = cell_to_json(&doc, 0).expect("nested struct converts");
        assert_eq!(value, json!({"n": 1, "nul": null, "nest": {"k": "v"}}));
        assert!(
            value.is_object(),
            "reconstructed value must be a JSON object"
        );
    }

    #[test]
    fn cell_to_json_reconstructs_list() {
        // #3063: a JSON array field lands as an Arrow list; it must reconstruct
        // a real JSON array (with nulls preserved), not the Display string.
        let list = array::ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
            Some(1_i64),
            None,
            Some(3_i64),
        ])]);
        let value = cell_to_json(&list, 0).expect("list converts");
        assert_eq!(value, json!([1, null, 3]));
        assert!(value.is_array(), "reconstructed value must be a JSON array");
    }

    #[test]
    fn cell_to_json_reconstructs_large_list() {
        // #3075: a LargeList value column (reachable via Parquet) must reconstruct
        // a real JSON array with nulls preserved, not the Arrow Display string.
        let list = array::LargeListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
            Some(1_i64),
            None,
            Some(3_i64),
        ])]);
        let value = cell_to_json(&list, 0).expect("large list converts");
        assert_eq!(value, json!([1, null, 3]));
        assert!(value.is_array(), "reconstructed value must be a JSON array");
    }

    #[test]
    fn cell_to_json_reconstructs_fixed_size_list() {
        // #3075: a FixedSizeList value column (reachable via Parquet) must
        // reconstruct a real JSON array, not the Arrow Display string.
        let mut builder = FixedSizeListBuilder::new(Int64Builder::new(), 3);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true);
        let list = builder.finish();
        let value = cell_to_json(&list, 0).expect("fixed-size list converts");
        assert_eq!(value, json!([1, 2, 3]));
        assert!(value.is_array(), "reconstructed value must be a JSON array");
    }

    #[test]
    fn cell_to_json_reconstructs_map_as_object() {
        // #3091: a Map value column reconstructs a JSON object, not the Display
        // string.
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        builder.keys().append_value("a");
        builder.values().append_value(1);
        builder.keys().append_value("b");
        builder.values().append_value(2);
        builder.append(true).expect("append map row");
        let map = builder.finish();
        let value = cell_to_json(&map, 0).expect("map converts");
        assert_eq!(value, json!({"a": 1, "b": 2}));
        assert!(
            value.is_object(),
            "reconstructed value must be a JSON object"
        );
    }

    #[test]
    fn cell_to_json_decodes_dictionary_to_the_underlying_value() {
        // #3091: a dictionary is an encoding; a dict-encoded integer must decode
        // to the JSON number 42, not the Display string "42".
        let values = Int64Array::from(vec![42_i64, 7]);
        let keys = Int32Array::from(vec![0_i32, 1]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))
            .expect("dictionary array");
        // Row 0 -> key 0 -> 42; row 1 -> key 1 -> 7. Both rows are checked so the
        // per-row key read (`value(row)`) is exercised, not just index 0.
        assert_eq!(cell_to_json(&dict, 0).expect("row 0 converts"), json!(42));
        let row1 = cell_to_json(&dict, 1).expect("row 1 converts");
        assert_eq!(row1, json!(7));
        assert!(row1.is_number(), "decoded value must be a JSON number");
    }

    #[test]
    fn extract_embedding_rejects_a_non_finite_float32_component() {
        // #3083: a NaN/Inf embedding component must be rejected (the row is
        // skipped) — consistent with the Float64 path — not silently imported
        // as a NaN vector that poisons every downstream distance.
        let mut nan = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        nan.values().append_value(1.0);
        nan.values().append_value(f32::NAN);
        nan.append(true);
        assert_eq!(extract_embedding(&nan.finish(), 0), None);

        let mut inf = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        inf.values().append_value(f32::INFINITY);
        inf.values().append_value(2.0);
        inf.append(true);
        assert_eq!(extract_embedding(&inf.finish(), 0), None);

        // A finite Float32 embedding is still accepted (direction control).
        let mut ok = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        ok.values().append_value(1.0);
        ok.values().append_value(2.0);
        ok.append(true);
        assert_eq!(extract_embedding(&ok.finish(), 0), Some(vec![1.0_f32, 2.0]));
    }

    #[test]
    fn cell_to_bytes_rejects_unknown_encoding_and_honors_utf8_and_base64() {
        // #3079: a declared encoding cell_to_bytes doesn't understand must fail
        // loudly, not silently fall through to the raw ASCII bytes.
        let cell = StringArray::from(vec!["deadbeef"]);
        for unknown in ["hex", "base64url", "utf-16"] {
            let err = cell_to_bytes(&cell, 0, Some(unknown))
                .expect_err("unknown encoding must be rejected");
            assert_eq!(err.class(), ExecutorErrorClass::InvalidInput);
            assert_eq!(err.code(), "invalid_argument.executor.arrow_encoding");
        }
        // The encodings Strata's exporter emits (plus no-encoding) still decode.
        assert_eq!(
            cell_to_bytes(&cell, 0, Some("utf8")).expect("utf8 ok"),
            b"deadbeef".to_vec()
        );
        assert_eq!(
            cell_to_bytes(&cell, 0, None).expect("no encoding ok"),
            b"deadbeef".to_vec()
        );
        let b64 = StringArray::from(vec!["aGk="]);
        assert_eq!(
            cell_to_bytes(&b64, 0, Some("base64")).expect("base64 ok"),
            b"hi".to_vec()
        );
    }

    #[test]
    fn cell_to_json_rejects_non_finite_floats_instead_of_nulling_them() {
        // #3078: serde_json maps NaN/±Inf to null, silently dropping the number.
        // A non-finite float cell must fail loudly, never corrupt to JSON null.
        let f64s = Float64Array::from(vec![1.5, f64::NAN]);
        assert_eq!(cell_to_json(&f64s, 0).expect("finite converts"), json!(1.5));
        let err = cell_to_json(&f64s, 1).expect_err("NaN must be rejected, not nulled");
        assert_eq!(err.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(
            err.code(),
            "invalid_argument.executor.arrow_non_finite_float"
        );

        // Both f32 infinities are rejected too; a finite f32 still converts.
        let f32s = Float32Array::from(vec![2.5, f32::INFINITY, f32::NEG_INFINITY]);
        assert_eq!(
            cell_to_json(&f32s, 0).expect("finite f32 converts"),
            json!(2.5)
        );
        assert_eq!(
            cell_to_json(&f32s, 1).expect_err("+Inf rejected").code(),
            "invalid_argument.executor.arrow_non_finite_float"
        );
        assert_eq!(
            cell_to_json(&f32s, 2).expect_err("-Inf rejected").code(),
            "invalid_argument.executor.arrow_non_finite_float"
        );
    }

    #[test]
    fn finite_float_to_json_keeps_finite_and_rejects_non_finite() {
        // Finite values pass through as JSON numbers...
        assert_eq!(finite_float_to_json(2.5).expect("finite"), json!(2.5));
        assert_eq!(finite_float_to_json(0.0).expect("zero"), json!(0.0));
        assert_eq!(finite_float_to_json(-1.0).expect("negative"), json!(-1.0));
        // ...while every non-finite value is rejected with the stable code.
        for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = finite_float_to_json(non_finite).expect_err("non-finite rejected");
            assert_eq!(err.class(), ExecutorErrorClass::InvalidInput);
            assert_eq!(
                err.code(),
                "invalid_argument.executor.arrow_non_finite_float"
            );
        }
    }
}
