//! db_export handler: export primitive data to CSV, JSON, or JSONL.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::bridge::{to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::types::{BranchId, ExportFormat, ExportPrimitive, ExportResult};
use crate::{Error, Output, Result};
use strata_core::Value;

/// Inline threshold: return data inline if row count is at most this.
const INLINE_ROW_LIMIT: usize = 1000;
/// Inline threshold: return data inline if rendered size is at most this (1 MB).
const INLINE_BYTE_LIMIT: usize = 1_048_576;

/// A single export row: key + value.
struct ExportRow {
    key: String,
    value: Value,
}

// =============================================================================
// Data collection
// =============================================================================

fn collect_kv(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let keys = convert_result(p.kv.list(&branch_id, space, prefix))?;
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let mut rows = Vec::new();
    for key in keys.into_iter().take(max) {
        if let Some(vv) = convert_result(p.kv.get(&branch_id, space, &key))? {
            rows.push(ExportRow {
                key,
                value: vv.value,
            });
        }
    }
    Ok(rows)
}

fn collect_json(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let keys = convert_result(p.json.list(&branch_id, space, prefix, None, None))?;
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let mut rows = Vec::new();
    for key in keys.into_iter().take(max) {
        if let Some(vv) = convert_result(p.json.get(&branch_id, space, &key, "$"))? {
            rows.push(ExportRow {
                key,
                value: vv.value,
            });
        }
    }
    Ok(rows)
}

fn collect_state(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let cells = convert_result(p.state.list(&branch_id, space, prefix))?;
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let mut rows = Vec::new();
    for cell in cells.into_iter().take(max) {
        if let Some(vv) = convert_result(p.state.get(&branch_id, space, &cell))? {
            rows.push(ExportRow {
                key: cell,
                value: vv.value,
            });
        }
    }
    Ok(rows)
}

fn collect_events(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let len = convert_result(p.event.len(&branch_id, space))?;
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let count = std::cmp::min(len as usize, max);
    let mut rows = Vec::new();
    for seq in 0..count {
        if let Some(event) = convert_result(p.event.get(&branch_id, space, seq as u64))? {
            // Build an object from the event fields
            let mut obj = std::collections::BTreeMap::new();
            obj.insert("sequence".to_string(), Value::Int(seq as i64));
            obj.insert(
                "event_type".to_string(),
                Value::String(event.event_type.clone()),
            );
            obj.insert("payload".to_string(), event.payload.clone());
            obj.insert(
                "timestamp".to_string(),
                Value::Int(event.timestamp.as_micros() as i64),
            );
            rows.push(ExportRow {
                key: seq.to_string(),
                value: Value::Object(Box::new(obj)),
            });
        }
    }
    Ok(rows)
}

// =============================================================================
// CSV helpers
// =============================================================================

/// Discover all unique leaf-path columns across all rows.
fn detect_columns(rows: &[ExportRow]) -> Vec<String> {
    let mut cols = BTreeSet::new();
    for row in rows {
        collect_paths("", &row.value, &mut cols);
    }
    // "key" is always first, then sorted field names
    let mut result = vec!["key".to_string()];
    for col in cols {
        if col == "key" {
            continue;
        }
        result.push(col);
    }
    result
}

/// Recursively collect leaf paths from a Value.
fn collect_paths(prefix: &str, value: &Value, out: &mut BTreeSet<String>) {
    match value {
        Value::Object(obj) => {
            for (k, v) in obj.as_ref() {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                collect_paths(&path, v, out);
            }
        }
        _ => {
            let col = if prefix.is_empty() {
                "value".to_string()
            } else {
                prefix.to_string()
            };
            out.insert(col);
        }
    }
}

/// Flatten a Value into a map of column_path -> cell_string.
fn flatten_value(
    prefix: &str,
    value: &Value,
    out: &mut std::collections::BTreeMap<String, String>,
) {
    match value {
        Value::Object(obj) => {
            for (k, v) in obj.as_ref() {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                flatten_value(&path, v, out);
            }
        }
        Value::Null => {
            let col = if prefix.is_empty() { "value" } else { prefix };
            out.insert(col.to_string(), String::new());
        }
        Value::Bool(b) => {
            let col = if prefix.is_empty() { "value" } else { prefix };
            out.insert(col.to_string(), b.to_string());
        }
        Value::Int(i) => {
            let col = if prefix.is_empty() { "value" } else { prefix };
            out.insert(col.to_string(), i.to_string());
        }
        Value::Float(f) => {
            let col = if prefix.is_empty() { "value" } else { prefix };
            out.insert(col.to_string(), f.to_string());
        }
        Value::String(s) => {
            let col = if prefix.is_empty() { "value" } else { prefix };
            out.insert(col.to_string(), s.clone());
        }
        Value::Bytes(b) => {
            use base64::Engine;
            let col = if prefix.is_empty() { "value" } else { prefix };
            out.insert(
                col.to_string(),
                base64::engine::general_purpose::STANDARD.encode(b.as_ref()),
            );
        }
        Value::Array(arr) => {
            let col = if prefix.is_empty() { "value" } else { prefix };
            // JSON-serialize arrays into a single cell
            let json = value_to_json_value(&Value::Array(arr.clone()));
            out.insert(col.to_string(), json.to_string());
        }
    }
}

/// CSV-escape a field per RFC 4180.
fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

// =============================================================================
// Renderers
// =============================================================================

fn render_csv(rows: &[ExportRow]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let columns = detect_columns(rows);
    let mut lines = Vec::with_capacity(rows.len() + 1);

    // Header
    lines.push(columns.iter().map(|c| csv_escape(c)).collect::<Vec<_>>().join(","));

    // Data rows
    for row in rows {
        let mut flat = std::collections::BTreeMap::new();
        flatten_value("", &row.value, &mut flat);

        let cells: Vec<String> = columns
            .iter()
            .map(|col| {
                if col == "key" {
                    csv_escape(&row.key)
                } else {
                    csv_escape(flat.get(col).map(|s| s.as_str()).unwrap_or(""))
                }
            })
            .collect();
        lines.push(cells.join(","));
    }
    lines.join("\n")
}

fn value_to_json_value(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            use base64::Engine;
            serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(b.as_ref()),
            )
        }
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json_value).collect())
        }
        Value::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json_value(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

fn row_to_json(row: &ExportRow) -> serde_json::Value {
    match &row.value {
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            map.insert("key".to_string(), serde_json::Value::String(row.key.clone()));
            for (k, v) in obj.as_ref() {
                map.insert(k.clone(), value_to_json_value(v));
            }
            serde_json::Value::Object(map)
        }
        other => {
            serde_json::json!({
                "key": row.key,
                "value": value_to_json_value(other),
            })
        }
    }
}

fn render_json(rows: &[ExportRow]) -> String {
    let arr: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
    serde_json::to_string_pretty(&arr).unwrap_or_else(|_| "[]".to_string())
}

fn render_jsonl(rows: &[ExportRow]) -> String {
    rows.iter()
        .map(|r| serde_json::to_string(&row_to_json(r)).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n")
}

// =============================================================================
// Main handler
// =============================================================================

/// Handle DbExport command.
pub fn db_export(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    primitive: ExportPrimitive,
    format: ExportFormat,
    prefix: Option<String>,
    limit: Option<u64>,
    path: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    // 1. Collect rows
    let rows = match primitive {
        ExportPrimitive::Kv => collect_kv(p, branch_id, &space, prefix.as_deref(), limit)?,
        ExportPrimitive::Json => collect_json(p, branch_id, &space, prefix.as_deref(), limit)?,
        ExportPrimitive::State => collect_state(p, branch_id, &space, prefix.as_deref(), limit)?,
        ExportPrimitive::Events => collect_events(p, branch_id, &space, limit)?,
    };

    // 2. Render
    let rendered = match format {
        ExportFormat::Csv => render_csv(&rows),
        ExportFormat::Json => render_json(&rows),
        ExportFormat::Jsonl => render_jsonl(&rows),
    };

    let row_count = rows.len() as u64;

    // 3. Decide inline vs file
    let should_write = path.is_some()
        || rows.len() > INLINE_ROW_LIMIT
        || rendered.len() > INLINE_BYTE_LIMIT;

    if should_write {
        let file_path = match path {
            Some(p) => p,
            None => {
                // Generate a default path under data_dir/exports/
                let data_dir = p.db.data_dir();
                let exports_dir = data_dir.join("exports");
                std::fs::create_dir_all(&exports_dir).map_err(|e| Error::Io {
                    reason: format!("Failed to create exports directory: {}", e),
                })?;
                let ext = match format {
                    ExportFormat::Csv => "csv",
                    ExportFormat::Json => "json",
                    ExportFormat::Jsonl => "jsonl",
                };
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                exports_dir
                    .join(format!("{}_{}.{}", primitive.as_str(), ts, ext))
                    .to_string_lossy()
                    .to_string()
            }
        };

        // Validate parent directory exists for explicit paths
        if let Some(parent) = std::path::Path::new(&file_path).parent() {
            if !parent.exists() {
                return Err(Error::Io {
                    reason: format!(
                        "Parent directory '{}' does not exist",
                        parent.display()
                    ),
                });
            }
        }

        std::fs::write(&file_path, &rendered).map_err(|e| Error::Io {
            reason: format!("Failed to write export file: {}", e),
        })?;

        Ok(Output::Exported(ExportResult {
            row_count,
            format,
            primitive,
            data: None,
            path: Some(file_path),
            size_bytes: Some(rendered.len() as u64),
        }))
    } else {
        Ok(Output::Exported(ExportResult {
            row_count,
            format,
            primitive,
            data: Some(rendered),
            path: None,
            size_bytes: None,
        }))
    }
}
