//! db_export handler: export primitive data to CSV, JSON, or JSONL.

use std::collections::BTreeSet;
use std::sync::Arc;

use strata_engine::JsonPath;

use crate::bridge::{json_to_value, to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::types::{BranchId, ExportFormat, ExportPrimitive, ExportResult};
use crate::{Error, Output, Result};
use strata_core::Value;

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
    branch_id: strata_core::BranchId,
    space: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let keys = convert_result(p.kv.list(&branch_id, space, prefix))?;
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let mut rows = Vec::new();
    for key in keys.into_iter().take(max) {
        if let Some(value) = convert_result(p.kv.get(&branch_id, space, &key))? {
            rows.push(ExportRow { key, value });
        }
    }
    Ok(rows)
}

fn collect_json(
    p: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let mut rows = Vec::new();
    let mut cursor: Option<String> = None;
    let root = JsonPath::root();

    loop {
        let result =
            convert_result(
                p.json
                    .list(&branch_id, space, prefix, cursor.as_deref(), 1000),
            )?;

        let page_empty = result.doc_ids.is_empty();

        for doc_id in result.doc_ids {
            if rows.len() >= max {
                return Ok(rows);
            }
            if let Some(json_val) = convert_result(p.json.get(&branch_id, space, &doc_id, &root))? {
                let value = convert_result(json_to_value(json_val))?;
                rows.push(ExportRow { key: doc_id, value });
            }
        }

        match result.next_cursor {
            // Guard against empty pages with a cursor (would spin forever)
            Some(_) if page_empty => break,
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok(rows)
}

fn collect_events(
    p: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let len = convert_result(p.event.len(&branch_id, space))?;
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let count = std::cmp::min(len as usize, max);
    let mut rows = Vec::new();
    for seq in 0..count {
        if let Some(versioned) = convert_result(p.event.get(&branch_id, space, seq as u64))? {
            let event = &versioned.value;
            let mut obj = std::collections::HashMap::new();
            obj.insert("sequence".to_string(), Value::Int(event.sequence as i64));
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

fn collect_graph(
    p: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    graph: &str,
    prefix: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<ExportRow>> {
    let max = limit.map(|l| l as usize).unwrap_or(usize::MAX);
    let mut rows = Vec::new();

    let nodes = convert_result(p.graph.list_nodes(branch_id, space, graph))?;
    for node_id in nodes {
        if let Some(pfx) = prefix {
            if !node_id.starts_with(pfx) {
                continue;
            }
        }
        if rows.len() >= max {
            return Ok(rows);
        }
        if let Some(data) = convert_result(p.graph.get_node(branch_id, space, graph, &node_id))? {
            let key = format!("{}/{}", graph, node_id);
            let value = serde_json::to_string(&data)
                .map(Value::String)
                .unwrap_or(Value::Null);
            rows.push(ExportRow { key, value });
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
                base64::engine::general_purpose::STANDARD.encode(b.as_slice()),
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
    lines.push(
        columns
            .iter()
            .map(|c| csv_escape(c))
            .collect::<Vec<_>>()
            .join(","),
    );

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
                base64::engine::general_purpose::STANDARD.encode(b.as_slice()),
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
            // Insert object fields first, then "key" last so the export key
            // always wins over any "key" field inside the value.
            for (k, v) in obj.as_ref() {
                map.insert(k.clone(), value_to_json_value(v));
            }
            map.insert(
                "key".to_string(),
                serde_json::Value::String(row.key.clone()),
            );
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn db_export(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    primitive: ExportPrimitive,
    format: ExportFormat,
    prefix: Option<String>,
    limit: Option<u64>,
    path: Option<String>,
    collection: Option<String>,
    graph: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    // Validate required fields for specific primitives
    if primitive == ExportPrimitive::Vector && collection.is_none() {
        return Err(Error::InvalidInput {
            reason: "--collection is required for vector export".into(),
            hint: None,
        });
    }
    if primitive == ExportPrimitive::Graph && graph.is_none() {
        return Err(Error::InvalidInput {
            reason: "--graph is required for graph export".into(),
            hint: None,
        });
    }

    // Arrow file export path: Parquet always, or CSV/JSONL when --output is provided
    // and the arrow feature is enabled.
    #[cfg(feature = "arrow")]
    {
        let use_arrow = match format {
            ExportFormat::Parquet => true,
            ExportFormat::Csv | ExportFormat::Jsonl => path.is_some(),
            ExportFormat::Json => false, // JSON array format uses existing renderer
        };

        if use_arrow {
            let file_path = match &path {
                Some(p) => p.clone(),
                None if format == ExportFormat::Parquet => {
                    return Err(Error::InvalidInput {
                        reason: "Parquet export requires --output <FILE>".into(),
                        hint: None,
                    });
                }
                None => unreachable!(), // CSV/JSONL use_arrow requires path.is_some()
            };

            return arrow_export(
                p, branch_id, &space, primitive, format, prefix, limit, &file_path, collection,
                graph,
            );
        }
    }

    #[cfg(not(feature = "arrow"))]
    if format == ExportFormat::Parquet {
        return Err(Error::Internal {
            reason: "Parquet export requires the 'arrow' feature".into(),
            hint: Some("Rebuild with: cargo build --features arrow".into()),
        });
    }

    // Existing string-based rendering path (inline output or file without arrow)
    let rows = match primitive {
        ExportPrimitive::Kv => collect_kv(p, branch_id, &space, prefix.as_deref(), limit)?,
        ExportPrimitive::Json => collect_json(p, branch_id, &space, prefix.as_deref(), limit)?,
        ExportPrimitive::Events => collect_events(p, branch_id, &space, limit)?,
        ExportPrimitive::Graph => collect_graph(
            p,
            branch_id,
            &space,
            graph
                .as_deref()
                .expect("graph export validates --graph before data collection"),
            prefix.as_deref(),
            limit,
        )?,
        ExportPrimitive::Vector => {
            return Err(Error::InvalidInput {
                reason: "Vector export requires the 'arrow' feature and --output <FILE>".into(),
                hint: Some("Rebuild with: cargo build --features arrow".into()),
            });
        }
    };

    let rendered = match format {
        ExportFormat::Csv => render_csv(&rows),
        ExportFormat::Json => render_json(&rows),
        ExportFormat::Jsonl => render_jsonl(&rows),
        ExportFormat::Parquet => unreachable!(), // handled above
    };

    let row_count = rows.len() as u64;

    if let Some(file_path) = path {
        if let Some(parent) = std::path::Path::new(&file_path).parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(Error::Io {
                    reason: format!("Parent directory '{}' does not exist", parent.display()),
                    hint: None,
                });
            }
        }

        std::fs::write(&file_path, &rendered).map_err(|e| Error::Io {
            reason: format!("Failed to write export file: {}", e),
            hint: None,
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

/// Arrow-based file export (Parquet, CSV, JSONL).
#[cfg(feature = "arrow")]
#[allow(clippy::too_many_arguments)]
fn arrow_export(
    p: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    primitive: ExportPrimitive,
    format: ExportFormat,
    prefix: Option<String>,
    limit: Option<u64>,
    file_path: &str,
    collection: Option<String>,
    graph: Option<String>,
) -> Result<Output> {
    use crate::arrow::{export_to_batches, write_file, ExportSource};

    // Validate parent directory exists
    if let Some(parent) = std::path::Path::new(file_path).parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(Error::Io {
                reason: format!("Parent directory '{}' does not exist", parent.display()),
                hint: None,
            });
        }
    }

    let limit = limit.map(|l| l as usize);

    // Build ExportSource from primitive + options
    let source = match primitive {
        ExportPrimitive::Kv => ExportSource::Kv { prefix },
        ExportPrimitive::Json => ExportSource::Json { prefix },
        ExportPrimitive::Events => ExportSource::Event { event_type: None },
        ExportPrimitive::Vector => ExportSource::Vector {
            collection: collection.unwrap(), // validated above
        },
        ExportPrimitive::Graph => {
            let graph_name = graph.unwrap(); // validated above

            // Graph exports produce two files: nodes + edges
            let path = std::path::Path::new(file_path);
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("graph");
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("parquet");
            let parent = path.parent().unwrap_or(std::path::Path::new("."));

            let nodes_path = parent.join(format!("{stem}_nodes.{ext}"));
            let edges_path = parent.join(format!("{stem}_edges.{ext}"));

            let arrow_format = to_arrow_format(format)?;

            // Export nodes
            let (_, node_batches) = export_to_batches(
                p,
                branch_id,
                space,
                ExportSource::GraphNodes {
                    graph: graph_name.clone(),
                },
                limit,
            )?;
            let node_bytes = if node_batches.is_empty() {
                0
            } else {
                write_file(&nodes_path, arrow_format, &node_batches)?
            };

            // Export edges
            let (_, edge_batches) = export_to_batches(
                p,
                branch_id,
                space,
                ExportSource::GraphEdges { graph: graph_name },
                limit,
            )?;
            let edge_bytes = if edge_batches.is_empty() {
                0
            } else {
                write_file(&edges_path, arrow_format, &edge_batches)?
            };

            let node_rows = node_batches
                .iter()
                .map(|b| b.num_rows() as u64)
                .sum::<u64>();
            let edge_rows = edge_batches
                .iter()
                .map(|b| b.num_rows() as u64)
                .sum::<u64>();

            return Ok(Output::Exported(ExportResult {
                row_count: node_rows + edge_rows,
                format,
                primitive,
                data: None,
                path: Some(file_path.to_string()),
                size_bytes: Some(node_bytes + edge_bytes),
            }));
        }
    };

    let (_, batches) = export_to_batches(p, branch_id, space, source, limit)?;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    let arrow_format = to_arrow_format(format)?;
    let size_bytes = if batches.is_empty() {
        0
    } else {
        write_file(std::path::Path::new(file_path), arrow_format, &batches)?
    };

    Ok(Output::Exported(ExportResult {
        row_count,
        format,
        primitive,
        data: None,
        path: Some(file_path.to_string()),
        size_bytes: Some(size_bytes),
    }))
}

/// Map ExportFormat to Arrow FileFormat.
#[cfg(feature = "arrow")]
fn to_arrow_format(format: ExportFormat) -> Result<crate::arrow::FileFormat> {
    use crate::arrow::FileFormat;
    match format {
        ExportFormat::Parquet => Ok(FileFormat::Parquet),
        ExportFormat::Csv => Ok(FileFormat::Csv),
        ExportFormat::Jsonl => Ok(FileFormat::Jsonl),
        ExportFormat::Json => Err(Error::InvalidInput {
            reason: "JSON array format is not supported for Arrow file export".into(),
            hint: Some("Use --format parquet, csv, or jsonl".into()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, value: Value) -> ExportRow {
        ExportRow {
            key: key.to_string(),
            value,
        }
    }

    // =========================================================================
    // csv_escape
    // =========================================================================

    #[test]
    fn csv_escape_plain() {
        assert_eq!(csv_escape("hello"), "hello");
    }

    #[test]
    fn csv_escape_with_comma() {
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
    }

    #[test]
    fn csv_escape_with_quotes() {
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn csv_escape_with_newline() {
        assert_eq!(csv_escape("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn csv_escape_with_cr() {
        assert_eq!(csv_escape("a\rb"), "\"a\rb\"");
    }

    #[test]
    fn csv_escape_empty() {
        assert_eq!(csv_escape(""), "");
    }

    // =========================================================================
    // detect_columns
    // =========================================================================

    #[test]
    fn detect_columns_scalar_values() {
        let rows = vec![row("a", Value::Int(1)), row("b", Value::String("x".into()))];
        assert_eq!(detect_columns(&rows), vec!["key", "value"]);
    }

    #[test]
    fn detect_columns_object_values() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("name".to_string(), Value::String("Alice".into()));
        obj.insert("age".to_string(), Value::Int(30));
        let rows = vec![row("u1", Value::object(obj))];
        let cols = detect_columns(&rows);
        assert_eq!(cols[0], "key");
        assert!(cols.contains(&"age".to_string()));
        assert!(cols.contains(&"name".to_string()));
    }

    #[test]
    fn detect_columns_mixed_shapes() {
        // Row 1 has {name}, Row 2 has {name, email} — union of columns
        let mut obj1 = std::collections::HashMap::new();
        obj1.insert("name".to_string(), Value::String("Alice".into()));
        let mut obj2 = std::collections::HashMap::new();
        obj2.insert("name".to_string(), Value::String("Bob".into()));
        obj2.insert("email".to_string(), Value::String("b@b.com".into()));
        let rows = vec![
            row("u1", Value::object(obj1)),
            row("u2", Value::object(obj2)),
        ];
        let cols = detect_columns(&rows);
        assert_eq!(cols[0], "key");
        assert!(cols.contains(&"email".to_string()));
        assert!(cols.contains(&"name".to_string()));
        assert_eq!(cols.len(), 3); // key, email, name
    }

    #[test]
    fn detect_columns_nested_object() {
        let mut inner = std::collections::HashMap::new();
        inner.insert("city".to_string(), Value::String("NYC".into()));
        let mut obj = std::collections::HashMap::new();
        obj.insert("addr".to_string(), Value::object(inner));
        let rows = vec![row("u1", Value::object(obj))];
        let cols = detect_columns(&rows);
        assert!(cols.contains(&"addr.city".to_string()));
    }

    #[test]
    fn detect_columns_empty_rows() {
        let rows: Vec<ExportRow> = vec![];
        // No rows = no columns beyond "key"
        assert_eq!(detect_columns(&rows), vec!["key"]);
    }

    // =========================================================================
    // render_csv
    // =========================================================================

    #[test]
    fn render_csv_empty() {
        assert_eq!(render_csv(&[]), "");
    }

    #[test]
    fn render_csv_scalar_values() {
        let rows = vec![row("a", Value::Int(1)), row("b", Value::Int(2))];
        let csv = render_csv(&rows);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[0], "key,value");
        assert_eq!(lines[1], "a,1");
        assert_eq!(lines[2], "b,2");
    }

    #[test]
    fn render_csv_object_values() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("x".to_string(), Value::Int(10));
        obj.insert("y".to_string(), Value::Int(20));
        let rows = vec![row("p1", Value::object(obj))];
        let csv = render_csv(&rows);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[0], "key,x,y");
        assert_eq!(lines[1], "p1,10,20");
    }

    #[test]
    fn render_csv_missing_columns_are_empty() {
        // Row 1 has {a, b}, Row 2 only has {a} — b should be empty for row 2
        let mut obj1 = std::collections::HashMap::new();
        obj1.insert("a".to_string(), Value::Int(1));
        obj1.insert("b".to_string(), Value::Int(2));
        let mut obj2 = std::collections::HashMap::new();
        obj2.insert("a".to_string(), Value::Int(3));
        let rows = vec![
            row("r1", Value::object(obj1)),
            row("r2", Value::object(obj2)),
        ];
        let csv = render_csv(&rows);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[0], "key,a,b");
        assert_eq!(lines[1], "r1,1,2");
        assert_eq!(lines[2], "r2,3,"); // b is empty
    }

    #[test]
    fn render_csv_value_with_comma_is_quoted() {
        let rows = vec![row("k", Value::String("hello, world".into()))];
        let csv = render_csv(&rows);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[1], "k,\"hello, world\"");
    }

    #[test]
    fn render_csv_key_with_special_chars() {
        let rows = vec![row("a,b", Value::Int(1))];
        let csv = render_csv(&rows);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[1], "\"a,b\",1");
    }

    #[test]
    fn render_csv_null_value() {
        let rows = vec![row("k", Value::Null)];
        let csv = render_csv(&rows);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[1], "k,"); // Null renders as empty
    }

    #[test]
    fn render_csv_bool_value() {
        let rows = vec![row("k", Value::Bool(true))];
        let csv = render_csv(&rows);
        assert!(csv.contains("k,true"));
    }

    #[test]
    fn render_csv_float_value() {
        let rows = vec![row("k", Value::Float(2.78))];
        let csv = render_csv(&rows);
        assert!(csv.contains("k,2.78"));
    }

    #[test]
    fn render_csv_bytes_value() {
        let rows = vec![row("k", Value::Bytes(vec![0xDE, 0xAD]))];
        let csv = render_csv(&rows);
        // base64 of [0xDE, 0xAD] = "3q0="
        assert!(csv.contains("3q0="));
    }

    #[test]
    fn render_csv_array_value() {
        let rows = vec![row(
            "k",
            Value::Array(Box::new(vec![Value::Int(1), Value::Int(2)])),
        )];
        let csv = render_csv(&rows);
        // Arrays become JSON in a single cell — contains brackets and comma
        assert!(csv.contains("[1"));
        assert!(csv.contains("2]"));
    }

    // =========================================================================
    // render_json / render_jsonl
    // =========================================================================

    #[test]
    fn render_json_empty() {
        assert_eq!(render_json(&[]), "[]");
    }

    #[test]
    fn render_jsonl_empty() {
        assert_eq!(render_jsonl(&[]), "");
    }

    #[test]
    fn render_json_scalar() {
        let rows = vec![row("k1", Value::Int(42))];
        let json = render_json(&rows);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["key"], "k1");
        assert_eq!(parsed[0]["value"], 42);
    }

    #[test]
    fn render_json_object_merges_fields() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("name".to_string(), Value::String("Alice".into()));
        let rows = vec![row("u1", Value::object(obj))];
        let json = render_json(&rows);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[0]["key"], "u1");
        assert_eq!(parsed[0]["name"], "Alice");
        // "value" key should NOT exist — fields are merged
        assert!(parsed[0].get("value").is_none());
    }

    #[test]
    fn render_json_object_key_field_does_not_shadow_row_key() {
        // Object has a "key" field — export key must win
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("WRONG".into()));
        obj.insert("x".to_string(), Value::Int(1));
        let rows = vec![row("correct-key", Value::object(obj))];
        let json = render_json(&rows);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[0]["key"], "correct-key");
    }

    #[test]
    fn render_jsonl_multiple_rows() {
        let rows = vec![row("a", Value::Int(1)), row("b", Value::Int(2))];
        let jsonl = render_jsonl(&rows);
        let lines: Vec<&str> = jsonl.lines().collect();
        assert_eq!(lines.len(), 2);
        let r0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let r1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(r0["key"], "a");
        assert_eq!(r1["key"], "b");
    }

    #[test]
    fn render_jsonl_each_line_is_valid_json() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("n".to_string(), Value::String("line\nbreak".into()));
        let rows = vec![row("k", Value::object(obj))];
        let jsonl = render_jsonl(&rows);
        // JSONL lines must not contain raw newlines from data
        // serde_json escapes \n inside strings, so this should be one line
        assert_eq!(jsonl.lines().count(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&jsonl).unwrap();
        assert_eq!(parsed["n"], "line\nbreak");
    }

    #[test]
    fn render_json_bytes_as_base64() {
        let rows = vec![row("k", Value::Bytes(vec![1, 2, 3]))];
        let json = render_json(&rows);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        // base64 of [1, 2, 3] = "AQID"
        assert_eq!(parsed[0]["value"], "AQID");
    }

    #[test]
    fn render_json_nested_object() {
        let mut inner = std::collections::HashMap::new();
        inner.insert("z".to_string(), Value::Int(99));
        let mut obj = std::collections::HashMap::new();
        obj.insert("nested".to_string(), Value::object(inner));
        let rows = vec![row("k", Value::object(obj))];
        let json = render_json(&rows);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[0]["nested"]["z"], 99);
    }

    #[test]
    fn render_json_null_value() {
        let rows = vec![row("k", Value::Null)];
        let json = render_json(&rows);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert!(parsed[0]["value"].is_null());
    }

    // =========================================================================
    // value_to_json_value
    // =========================================================================

    #[test]
    fn value_to_json_all_types() {
        assert_eq!(value_to_json_value(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            value_to_json_value(&Value::Bool(true)),
            serde_json::json!(true)
        );
        assert_eq!(value_to_json_value(&Value::Int(42)), serde_json::json!(42));
        assert_eq!(
            value_to_json_value(&Value::Float(1.5)),
            serde_json::json!(1.5)
        );
        assert_eq!(
            value_to_json_value(&Value::String("hi".into())),
            serde_json::json!("hi")
        );
        // Bytes → base64 string
        let b = value_to_json_value(&Value::Bytes(vec![0xFF]));
        assert!(b.is_string());
        // Array
        let a = value_to_json_value(&Value::Array(Box::new(vec![Value::Int(1)])));
        assert!(a.is_array());
        assert_eq!(a[0], 1);
        // Object
        let mut m = std::collections::HashMap::new();
        m.insert("k".to_string(), Value::Int(1));
        let o = value_to_json_value(&Value::object(m));
        assert_eq!(o["k"], 1);
    }

    // =========================================================================
    // flatten_value
    // =========================================================================

    #[test]
    fn flatten_value_scalar_at_root() {
        let mut out = std::collections::BTreeMap::new();
        flatten_value("", &Value::Int(42), &mut out);
        assert_eq!(out.get("value").unwrap(), "42");
    }

    #[test]
    fn flatten_value_object_at_root() {
        let mut m = std::collections::HashMap::new();
        m.insert("a".to_string(), Value::Int(1));
        m.insert("b".to_string(), Value::String("hi".into()));
        let mut out = std::collections::BTreeMap::new();
        flatten_value("", &Value::object(m), &mut out);
        assert_eq!(out.get("a").unwrap(), "1");
        assert_eq!(out.get("b").unwrap(), "hi");
    }

    #[test]
    fn flatten_value_nested_object() {
        let mut inner = std::collections::HashMap::new();
        inner.insert("c".to_string(), Value::Int(9));
        let mut outer = std::collections::HashMap::new();
        outer.insert("sub".to_string(), Value::object(inner));
        let mut out = std::collections::BTreeMap::new();
        flatten_value("", &Value::object(outer), &mut out);
        assert_eq!(out.get("sub.c").unwrap(), "9");
    }

    #[test]
    fn flatten_value_null() {
        let mut out = std::collections::BTreeMap::new();
        flatten_value("", &Value::Null, &mut out);
        assert_eq!(out.get("value").unwrap(), "");
    }
}
