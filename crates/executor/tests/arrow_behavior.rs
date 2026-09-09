//! Executor Arrow import/export behavior tests.

#![cfg(feature = "arrow")]

use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{FixedSizeListBuilder, Float32Builder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use strata_executor::{
    public_error_code_entry, ArrowExportPrimitive, ArrowExportResult, ArrowFileFormat,
    ArrowImportResult, ArrowImportTarget, BatchEventEntry, Bytes, Command, EventRangeDirection,
    Executor, ExecutorErrorClass, GraphBindingPrimitive, GraphBindingTarget, GraphDirection,
    GraphEntityBinding, Output, VectorDistanceMetric, DEFAULT_BRANCH,
};
use tempfile::TempDir;

#[test]
fn arrow_commands_round_trip_through_json() {
    let import = Command::ArrowImport {
        branch: Some("feature".to_owned()),
        space: Some("space-a".to_owned()),
        file_path: "input.csv".to_owned(),
        format: Some(ArrowFileFormat::Csv),
        target: ArrowImportTarget::Json,
        key_column: Some("id".to_owned()),
        value_column: Some("document".to_owned()),
        collection: None,
        graph: None,
    };
    let encoded = serde_json::to_value(&import).expect("command serializes");
    assert_eq!(encoded["type"], "arrow_import");
    assert_eq!(encoded["format"], "csv");
    assert_eq!(encoded["target"], "json");
    assert_eq!(
        serde_json::from_value::<Command>(encoded).expect("command deserializes"),
        import
    );

    let output = Output::ArrowExportResult(ArrowExportResult::new(
        ArrowExportPrimitive::Kv,
        ArrowFileFormat::Jsonl,
        vec!["out.jsonl".to_owned()],
        2,
        100,
    ));
    let encoded = serde_json::to_value(&output).expect("output serializes");
    assert_eq!(encoded["type"], "arrow_export_result");
    assert_eq!(encoded["data"]["primitive"], "kv");
    assert_eq!(encoded["data"]["format"], "jsonl");
}

#[test]
fn csv_kv_import_and_jsonl_export_round_trip_bytes() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("kv.csv");
    let output_path = dir.path().join("kv.jsonl");
    fs::write(
        &input_path,
        "key,key_encoding,value,value_encoding\nplain,utf8,value,utf8\n//4=,base64,AP8=,base64\n",
    )
    .expect("write csv");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("kv import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_import_result(&result, ArrowImportTarget::Kv, 2, 0, 1);

    assert_eq!(
        kv_get(&mut executor, Bytes::from("plain")),
        Some(b"value".to_vec())
    );
    assert_eq!(
        kv_get(&mut executor, Bytes::from(vec![0xff, 0xfe])),
        Some(vec![0x00, 0xff])
    );

    let output = executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Kv,
            format: ArrowFileFormat::Jsonl,
            path: output_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect("kv export succeeds");
    let Output::ArrowExportResult(result) = output else {
        panic!("unexpected export output");
    };
    assert_eq!(result.primitive(), ArrowExportPrimitive::Kv);
    assert_eq!(result.row_count(), 2);
    assert_eq!(
        result.paths(),
        &[output_path.to_string_lossy().into_owned()]
    );
    assert!(result.size_bytes() > 0);

    let lines = fs::read_to_string(&output_path).expect("read jsonl");
    assert!(lines.contains("\"key_encoding\":\"base64\""));
    assert!(lines.contains("\"value_encoding\":\"base64\""));
}

/// #3077: CSV carries no schema, so `read_csv` used to re-infer column types and
/// retype a numeric-looking text column — a `"00501"` key came back `501`,
/// silently corrupting the stored bytes on round-trip. The reader must honor the
/// text columns Strata's own exporters declare (`key`/`value`/...).
#[test]
fn csv_import_preserves_numeric_looking_string_keys_and_values() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("kv.csv");
    // Every key/value looks numeric, so blind inference would pick Int64 and
    // drop the leading zeros.
    fs::write(
        &input_path,
        "key,key_encoding,value,value_encoding\n00501,utf8,00042,utf8\n00777,utf8,00088,utf8\n",
    )
    .expect("write csv");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("kv import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_import_result(&result, ArrowImportTarget::Kv, 2, 0, 1);

    // Leading zeros survive: the key is the exact bytes "00501", not "501".
    assert_eq!(
        kv_get(&mut executor, Bytes::from("00501")),
        Some(b"00042".to_vec())
    );
    assert_eq!(
        kv_get(&mut executor, Bytes::from("00777")),
        Some(b"00088".to_vec())
    );
    // The re-inferred (corrupted) key must not be what got stored.
    assert_eq!(kv_get(&mut executor, Bytes::from("501")), None);
}

/// #3079: a `value_encoding` the reader does not understand (here `hex`) must
/// fail the import with a typed error, not silently store the raw ASCII bytes
/// on the very column meant to prevent mis-decoding.
#[test]
fn kv_import_rejects_an_unsupported_value_encoding() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("kv.csv");
    fs::write(
        &input_path,
        "key,key_encoding,value,value_encoding\nk1,utf8,deadbeef,hex\n",
    )
    .expect("write csv");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect_err("an unsupported value_encoding must fail the import");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.executor.arrow_encoding");
    // The error fires while building entries, before any storage write.
    assert_eq!(kv_get(&mut executor, Bytes::from("k1")), None);
}

/// #3083: a null value cell must be skipped, not stored as an empty-byte value
/// (which is indistinguishable from a real empty value).
#[test]
fn kv_import_skips_a_null_value_cell_instead_of_storing_empty_bytes() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("kv.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    // Row "a" has a value; row "b"'s value is null.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec![Some("x"), None])),
        ],
    )
    .expect("record batch");
    let file = std::fs::File::create(&path).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("kv import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    // "a" imported; "b" skipped for its null value — not stored as empty bytes.
    assert_eq!(result.rows_imported(), 1);
    assert_eq!(result.rows_skipped(), 1);
    assert_eq!(kv_get(&mut executor, Bytes::from("a")), Some(b"x".to_vec()));
    assert_eq!(kv_get(&mut executor, Bytes::from("b")), None);
}

/// #3077 (graph/event arm): numeric-looking node/edge ids exported as Utf8 were
/// re-inferred as `Int64` on CSV import, so the strict `StringArray` downcast
/// failed the whole import. Honoring the text columns lets a graph with
/// leading-zero ids round-trip through CSV intact.
#[test]
fn csv_graph_round_trip_preserves_numeric_looking_node_and_edge_ids() {
    let dir = TempDir::new().expect("temp dir");
    let export_path = dir.path().join("g.csv");
    let mut executor = Executor::open_cache().expect("cache executor opens");

    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "g".to_owned(),
        })
        .expect("graph create succeeds");
    for node_id in ["00501", "00777"] {
        executor
            .execute(Command::GraphAddNode {
                object_type: None,
                branch: None,
                space: None,
                graph: "g".to_owned(),
                node_id: node_id.to_owned(),
                properties: Some(json!({"kind": "person"})),
                binding: None,
            })
            .expect("node add succeeds");
    }
    executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: "g".to_owned(),
            src: "00501".to_owned(),
            edge_type: "knows".to_owned(),
            dst: "00777".to_owned(),
            weight: Some(1.5),
            properties: None,
        })
        .expect("edge add succeeds");

    executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Graph,
            format: ArrowFileFormat::Csv,
            path: export_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: Some("g".to_owned()),
            event_type: None,
        })
        .expect("graph csv export succeeds");

    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "g2".to_owned(),
        })
        .expect("second graph create succeeds");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: export_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Graph,
            key_column: None,
            value_column: None,
            collection: None,
            graph: Some("g2".to_owned()),
        })
        .expect("graph csv import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    // Two nodes + one edge, nothing skipped — the strict downcast no longer fails.
    assert_import_result(&result, ArrowImportTarget::Graph, 3, 0, 2);

    // The leading-zero node ids survive exactly (not "501"/"777").
    let nodes = graph_node_properties(&mut executor, "g2");
    assert_eq!(nodes.len(), 2);
    assert_eq!(
        nodes.get("00501").cloned().flatten(),
        Some(json!({"kind": "person"}))
    );
    assert!(nodes.contains_key("00777"));
    assert!(!nodes.contains_key("501"));

    // The edge keeps its numeric-looking endpoints and weight.
    let edges = graph_outgoing_edges(&mut executor, "g2", "00501");
    assert_eq!(
        edges,
        vec![(
            "00501".to_owned(),
            "knows".to_owned(),
            "00777".to_owned(),
            1.5
        )]
    );
}

/// #3077 (event arm): a numeric-looking `event_type` exported as Utf8 was
/// re-inferred as `Int64` on CSV import, failing the strict `StringArray`
/// downcast. Honoring the text columns lets an event log round-trip through CSV.
#[test]
fn csv_event_round_trip_preserves_numeric_looking_event_types() {
    let dir = TempDir::new().expect("temp dir");
    let export_path = dir.path().join("events.csv");
    let mut executor = Executor::open_cache().expect("cache executor opens");

    executor
        .execute(Command::EventBatchAppend {
            branch: None,
            space: None,
            entries: vec![
                BatchEventEntry::new("40001", json!({"id": 1})),
                BatchEventEntry::new("40002", json!({"id": 2})),
            ],
        })
        .expect("event batch append succeeds");

    executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Event,
            format: ArrowFileFormat::Csv,
            path: export_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect("event csv export succeeds");

    executor
        .execute(Command::BranchCreate {
            branch: "restore".to_owned(),
        })
        .expect("branch create succeeds");
    let output = executor
        .execute(Command::ArrowImport {
            branch: Some("restore".to_owned()),
            space: None,
            file_path: export_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Event,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("event csv import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_eq!(result.rows_imported(), 2);

    let range = executor
        .execute(Command::EventRange {
            branch: Some("restore".to_owned()),
            space: None,
            start_seq: 0,
            end_seq: None,
            limit: None,
            direction: EventRangeDirection::Forward,
            event_type: None,
        })
        .expect("event range read succeeds");
    let Output::EventRangeResult { items, .. } = range else {
        panic!("unexpected range output");
    };
    assert_eq!(items.len(), 2);
    // The event types survive as the exact strings "40001"/"40002", not integers.
    assert_eq!(items[0].event().event_type(), "40001");
    assert_eq!(items[0].event().payload(), &json!({"id": 1}));
    assert_eq!(items[1].event().event_type(), "40002");
    assert_eq!(items[1].event().payload(), &json!({"id": 2}));
}

#[test]
fn csv_json_import_and_jsonl_export_preserve_documents() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("docs.csv");
    let output_path = dir.path().join("docs.jsonl");
    fs::write(
        &input_path,
        "id,document\nuser-a,\"{\"\"name\"\":\"\"Ada\"\",\"\"rank\"\":1}\"\nuser-b,\"{\"\"name\"\":\"\"Bob\"\"}\"\n",
    )
    .expect("write csv");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: None,
            target: ArrowImportTarget::Json,
            key_column: Some("id".to_owned()),
            value_column: Some("document".to_owned()),
            collection: None,
            graph: None,
        })
        .expect("json import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_import_result(&result, ArrowImportTarget::Json, 2, 0, 1);
    assert_eq!(
        json_get(&mut executor, "user-a"),
        Some(json!({"name": "Ada", "rank": 1}))
    );

    executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Json,
            format: ArrowFileFormat::Jsonl,
            path: output_path.to_string_lossy().into_owned(),
            prefix: Some("user-".to_owned()),
            limit: Some(1),
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect("json export succeeds");
    let exported = fs::read_to_string(&output_path).expect("read jsonl");
    assert!(exported.contains("\"key\":\"user-a\""));
    assert!(!exported.contains("\"key\":\"user-b\""));
    assert!(exported.contains("\\\"name\\\":\\\"Ada\\\""));
}

/// #3080: JSONL schema inference was bounded to the first 100 rows, so a field
/// first appearing at row 101 was dropped from the schema and silently omitted
/// from every stored document under a success report. Inference must span the
/// whole file. Uses a row-object import (no `value`/`document` column) so the
/// late field lands in the reconstructed document.
#[test]
fn jsonl_import_keeps_a_field_that_first_appears_after_row_100() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("late.jsonl");
    // 101 rows: the first 100 carry only key + `a`; the last adds `late`.
    let mut jsonl = String::new();
    for i in 0..100 {
        writeln!(jsonl, "{{\"key\":\"k{i}\",\"a\":\"a{i}\"}}").unwrap();
    }
    jsonl.push_str("{\"key\":\"k100\",\"a\":\"a100\",\"late\":\"important\"}\n");
    fs::write(&input_path, jsonl).expect("write jsonl");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Jsonl),
            target: ArrowImportTarget::Json,
            key_column: Some("key".to_owned()),
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("json import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_eq!(result.rows_imported(), 101);

    // The late field must survive on the row that introduced it.
    let doc = json_get(&mut executor, "k100").expect("k100 present");
    assert_eq!(doc.get("late"), Some(&json!("important")));
    assert_eq!(doc.get("a"), Some(&json!("a100")));
    // A first-window row keeps its own field; under the now-unified schema a
    // row-object import represents the absent field as an explicit null (a
    // consistent shape), never dropping the column outright.
    let doc0 = json_get(&mut executor, "k0").expect("k0 present");
    assert_eq!(doc0.get("a"), Some(&json!("a0")));
    assert_eq!(doc0.get("late"), Some(&json!(null)));
}

/// #3080 (CSV arm): a column that looks integer for the first 100 rows but holds
/// a decimal at row 101 was typed `Int64` from the prefix, so the later value
/// could not be read. Whole-file inference must see the widening value.
#[test]
fn csv_import_infers_column_type_from_a_value_after_row_100() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("late.csv");
    // Header + 101 data rows: `n` is integer-looking for 100 rows, decimal at 101.
    let mut csv = String::from("key,n\n");
    for i in 0..100 {
        writeln!(csv, "k{i},{i}").unwrap();
    }
    csv.push_str("k100,2.5\n");
    fs::write(&input_path, csv).expect("write csv");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Json,
            key_column: Some("key".to_owned()),
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("csv import succeeds (the decimal row is not rejected)");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_eq!(result.rows_imported(), 101);

    // The widening value at row 101 is preserved, not lost to a prefix-only type.
    let doc = json_get(&mut executor, "k100").expect("k100 present");
    assert_eq!(doc.get("n"), Some(&json!(2.5)));
}

#[test]
fn jsonl_struct_import_stores_a_real_document_not_a_display_string() {
    // #3063: a jsonl value column that is a nested object arrives as an Arrow
    // struct. It must store a queryable JSON document, not the struct's Display
    // string — before the fix the stored value was a string, nulls were
    // dropped, and every path query returned nil.
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("docs.jsonl");
    fs::write(
        &input_path,
        "{\"key\":\"a\",\"doc\":{\"n\":1,\"nul\":null,\"nest\":{\"k\":\"v\"}}}\n",
    )
    .expect("write jsonl");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: None,
            target: ArrowImportTarget::Json,
            key_column: Some("key".to_owned()),
            value_column: Some("doc".to_owned()),
            collection: None,
            graph: None,
        })
        .expect("json import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_import_result(&result, ArrowImportTarget::Json, 1, 0, 1);

    let stored = json_get(&mut executor, "a").expect("document present after import");
    assert_eq!(stored, json!({"n": 1, "nul": null, "nest": {"k": "v"}}));
    assert!(
        stored.is_object(),
        "the imported value is a real JSON object, not a Display string: {stored}"
    );
}

#[test]
fn parquet_vector_import_and_export_uses_batch_commands() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("vectors.parquet");
    let output_path = dir.path().join("vectors.parquet");
    write_vector_parquet(&input_path);

    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_docs_collection(&mut executor);
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Vector,
            key_column: None,
            value_column: None,
            collection: Some("docs".to_owned()),
            graph: None,
        })
        .expect("vector import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_import_result(&result, ArrowImportTarget::Vector, 2, 0, 1);

    assert_eq!(vector_count(&mut executor, "docs"), 2);
    assert_eq!(
        vector_get_metadata(&mut executor, "docs", "doc-a"),
        json!({"kind": "a"})
    );

    let output = executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Vector,
            format: ArrowFileFormat::Parquet,
            path: output_path.to_string_lossy().into_owned(),
            prefix: Some("doc-".to_owned()),
            limit: None,
            collection: Some("docs".to_owned()),
            graph: None,
            event_type: None,
        })
        .expect("vector export succeeds");
    let Output::ArrowExportResult(result) = output else {
        panic!("unexpected export output");
    };
    assert_eq!(result.row_count(), 2);
    assert!(output_path.exists());
}

#[test]
fn vector_export_import_round_trip_preserves_metadata_without_leaking_internals() {
    let dir = TempDir::new().expect("temp dir");
    let source_path = dir.path().join("source.parquet");
    let exported_path = dir.path().join("exported.parquet");
    write_vector_parquet(&source_path);

    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_docs_collection(&mut executor);
    // Seed `docs` with metadata {"kind":"a"} / {"kind":"b"}.
    executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: source_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Vector,
            key_column: None,
            value_column: None,
            collection: Some("docs".to_owned()),
            graph: None,
        })
        .expect("seed import succeeds");

    // Export the real vector schema (a JSON `metadata` column + internal fields).
    executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Vector,
            format: ArrowFileFormat::Parquet,
            path: exported_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: Some("docs".to_owned()),
            graph: None,
            event_type: None,
        })
        .expect("export succeeds");

    // Re-import into a fresh collection and confirm the metadata round-trips.
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs_roundtrip".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("second collection create succeeds");
    executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: exported_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Vector,
            key_column: None,
            value_column: None,
            collection: Some("docs_roundtrip".to_owned()),
            graph: None,
        })
        .expect("round-trip import succeeds");

    assert_eq!(vector_count(&mut executor, "docs_roundtrip"), 2);
    // Metadata must survive identically: no `metadata`-string wrapper, no
    // leaked `vector_revision` field.
    assert_eq!(
        vector_get_metadata(&mut executor, "docs_roundtrip", "doc-a"),
        json!({"kind": "a"})
    );
    assert_eq!(
        vector_get_metadata(&mut executor, "docs_roundtrip", "doc-b"),
        json!({"kind": "b"})
    );
}

#[test]
fn vector_import_into_missing_collection_fails_instead_of_defaulting_a_metric() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("vectors.parquet");
    write_vector_parquet(&input_path);

    // THIN-2: the executor must not invent a distance metric by auto-creating a
    // Cosine collection; import into a non-existent vector collection fails.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Vector,
            key_column: None,
            value_column: None,
            collection: Some("missing".to_owned()),
            graph: None,
        })
        .expect_err("import into a missing vector collection is rejected");
    assert_eq!(error.code(), "not_found.engine.vector_collection");
}

#[test]
fn arrow_import_export_respects_branch_and_space_isolation() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("scoped.csv");
    let output_path = dir.path().join("scoped.jsonl");
    fs::write(&input_path, "key,value\nscoped,visible\n").expect("write csv");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::BranchForkCurrent {
            source: DEFAULT_BRANCH.to_owned(),
            branch: "feature".to_owned(),
        })
        .expect("branch fork succeeds");

    let output = executor
        .execute(Command::ArrowImport {
            branch: Some("feature".to_owned()),
            space: Some("tenant-a".to_owned()),
            file_path: input_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Kv,
            key_column: Some("key".to_owned()),
            value_column: Some("value".to_owned()),
            collection: None,
            graph: None,
        })
        .expect("scoped import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_import_result(&result, ArrowImportTarget::Kv, 1, 0, 1);

    assert_eq!(kv_get(&mut executor, Bytes::from("scoped")), None);
    assert_eq!(
        kv_get_in(
            &mut executor,
            Some("feature"),
            Some("tenant-a"),
            Bytes::from("scoped"),
        ),
        Some(b"visible".to_vec())
    );
    assert_eq!(
        kv_get_in(&mut executor, Some("feature"), None, Bytes::from("scoped"),),
        None
    );

    let output = executor
        .execute(Command::ArrowExport {
            branch: Some("feature".to_owned()),
            space: Some("tenant-a".to_owned()),
            primitive: ArrowExportPrimitive::Kv,
            format: ArrowFileFormat::Jsonl,
            path: output_path.to_string_lossy().into_owned(),
            prefix: Some("sc".to_owned()),
            limit: Some(10),
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect("scoped export succeeds");
    let Output::ArrowExportResult(result) = output else {
        panic!("unexpected export output");
    };
    assert_eq!(result.row_count(), 1);
    let exported = fs::read_to_string(output_path).expect("read jsonl");
    assert!(exported.contains("\"key\":\"scoped\""));
    assert!(exported.contains("\"value\":\"visible\""));
}

#[test]
fn durable_arrow_import_survives_reopen_and_exports() {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().join("db");
    let kv_path = dir.path().join("kv.csv");
    let json_path = dir.path().join("docs.csv");
    let vector_path = dir.path().join("vectors.parquet");
    let export_path = dir.path().join("exported.jsonl");
    fs::write(&kv_path, "key,value\npersisted,value\n").expect("write kv csv");
    fs::write(
        &json_path,
        "id,document\nuser-a,\"{\"\"name\"\":\"\"Ada\"\"}\"\n",
    )
    .expect("write json csv");
    write_vector_parquet(&vector_path);

    {
        let mut executor = Executor::open_durable_local(&db_path).expect("durable executor opens");
        executor
            .execute(Command::ArrowImport {
                branch: None,
                space: None,
                file_path: kv_path.to_string_lossy().into_owned(),
                format: Some(ArrowFileFormat::Csv),
                target: ArrowImportTarget::Kv,
                key_column: Some("key".to_owned()),
                value_column: Some("value".to_owned()),
                collection: None,
                graph: None,
            })
            .expect("kv import succeeds");
        executor
            .execute(Command::ArrowImport {
                branch: None,
                space: None,
                file_path: json_path.to_string_lossy().into_owned(),
                format: Some(ArrowFileFormat::Csv),
                target: ArrowImportTarget::Json,
                key_column: Some("id".to_owned()),
                value_column: Some("document".to_owned()),
                collection: None,
                graph: None,
            })
            .expect("json import succeeds");
        create_docs_collection(&mut executor);
        executor
            .execute(Command::ArrowImport {
                branch: None,
                space: None,
                file_path: vector_path.to_string_lossy().into_owned(),
                format: Some(ArrowFileFormat::Parquet),
                target: ArrowImportTarget::Vector,
                key_column: None,
                value_column: None,
                collection: Some("docs".to_owned()),
                graph: None,
            })
            .expect("vector import succeeds");
        executor.close().expect("close succeeds");
    }

    let mut reopened = Executor::open_durable_local(&db_path).expect("durable executor reopens");
    assert_eq!(
        kv_get(&mut reopened, Bytes::from("persisted")),
        Some(b"value".to_vec())
    );
    assert_eq!(
        json_get(&mut reopened, "user-a"),
        Some(json!({"name": "Ada"}))
    );
    assert_eq!(vector_count(&mut reopened, "docs"), 2);
    assert_eq!(
        vector_get_metadata(&mut reopened, "docs", "doc-b"),
        json!({"kind": "b"})
    );

    let output = reopened
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Kv,
            format: ArrowFileFormat::Jsonl,
            path: export_path.to_string_lossy().into_owned(),
            prefix: Some("persist".to_owned()),
            limit: None,
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect("durable export succeeds");
    let Output::ArrowExportResult(result) = output else {
        panic!("unexpected export output");
    };
    assert_eq!(result.row_count(), 1);
    assert!(fs::read_to_string(export_path)
        .expect("read export")
        .contains("\"key\":\"persisted\""));
}

#[test]
fn event_export_writes_filtered_jsonl_without_mutating_log() {
    let dir = TempDir::new().expect("temp dir");
    let output_path = dir.path().join("events.jsonl");
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::EventBatchAppend {
            branch: None,
            space: None,
            entries: vec![
                BatchEventEntry::new("audit.created", json!({"id": 1})),
                BatchEventEntry::new("audit.updated", json!({"id": 1})),
            ],
        })
        .expect("event batch append succeeds");

    let output = executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Event,
            format: ArrowFileFormat::Jsonl,
            path: output_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: None,
            event_type: Some("audit.created".to_owned()),
        })
        .expect("event export succeeds");
    let Output::ArrowExportResult(result) = output else {
        panic!("unexpected export output");
    };
    assert_eq!(result.row_count(), 1);
    let exported = fs::read_to_string(output_path).expect("read jsonl");
    assert!(exported.contains("\"event_type\":\"audit.created\""));
    assert!(!exported.contains("\"event_type\":\"audit.updated\""));
    assert_eq!(event_count(&mut executor), 2);
}

#[test]
fn event_export_import_round_trip_restores_payloads_and_types() {
    let dir = TempDir::new().expect("temp dir");
    let export_path = dir.path().join("events.parquet");
    let mut executor = Executor::open_cache().expect("cache executor opens");

    // Source events on the default branch.
    executor
        .execute(Command::EventBatchAppend {
            branch: None,
            space: None,
            entries: vec![
                BatchEventEntry::new("audit.created", json!({"id": 1})),
                BatchEventEntry::new("audit.updated", json!({"id": 2})),
            ],
        })
        .expect("event batch append succeeds");

    executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Event,
            format: ArrowFileFormat::Parquet,
            path: export_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect("event export succeeds");

    // Import into a fresh, empty branch. Arrow is an analytics interchange
    // (clone artifacts are the lossless backup path), so the log is re-derived:
    // event type and payload round-trip, while sequence/timestamp/hash are
    // reassigned by the ordinary append.
    executor
        .execute(Command::BranchCreate {
            branch: "restore".to_owned(),
        })
        .expect("branch create succeeds");
    let output = executor
        .execute(Command::ArrowImport {
            branch: Some("restore".to_owned()),
            space: None,
            file_path: export_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Event,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("event import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_eq!(result.rows_imported(), 2);

    // The restored branch carries both events with their original type + payload.
    let range = executor
        .execute(Command::EventRange {
            branch: Some("restore".to_owned()),
            space: None,
            start_seq: 0,
            end_seq: None,
            limit: None,
            direction: EventRangeDirection::Forward,
            event_type: None,
        })
        .expect("event range read succeeds");
    let Output::EventRangeResult { items, .. } = range else {
        panic!("unexpected range output");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].event().event_type(), "audit.created");
    assert_eq!(items[0].event().payload(), &json!({"id": 1}));
    assert_eq!(items[1].event().event_type(), "audit.updated");
    assert_eq!(items[1].event().payload(), &json!({"id": 2}));

    // The source branch is untouched by the import.
    assert_eq!(event_count(&mut executor), 2);
}

#[test]
fn event_import_rejects_a_file_missing_event_columns() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("vectors.parquet");
    write_vector_parquet(&path);

    // A file without the `event_type`/`payload` columns is rejected with the
    // event-import code rather than panicking on a missing column.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Event,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect_err("a non-event file is rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.executor.arrow_event");
}

#[test]
fn graph_export_writes_node_and_edge_tables() {
    let dir = TempDir::new().expect("temp dir");
    let output_path = dir.path().join("graph.jsonl");
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_graph_fixture(&mut executor);

    let output = executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Graph,
            format: ArrowFileFormat::Jsonl,
            path: output_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: Some("deps".to_owned()),
            event_type: None,
        })
        .expect("graph export succeeds");
    let Output::ArrowExportResult(result) = output else {
        panic!("unexpected export output");
    };
    assert_eq!(result.primitive(), ArrowExportPrimitive::Graph);
    assert_eq!(result.row_count(), 3);
    let node_path = dir.path().join("graph_nodes.jsonl");
    let edge_path = dir.path().join("graph_edges.jsonl");
    assert_eq!(
        result.paths(),
        &[
            node_path.to_string_lossy().into_owned(),
            edge_path.to_string_lossy().into_owned(),
        ]
    );
    assert!(
        !output_path.exists(),
        "graph export path is a stem; use returned paths for written files"
    );
    assert!(node_path.exists());
    assert!(edge_path.exists());
    let nodes = fs::read_to_string(&node_path).expect("nodes");
    assert!(nodes.contains("\"node_id\":\"node-a\""));
    assert!(nodes.contains("\\\"key\\\":\\\"doc-a\\\""));
    let edges = fs::read_to_string(&edge_path).expect("edges");
    assert!(edges.contains("\"edge_type\":\"depends_on\""));
}

#[test]
fn graph_export_import_round_trip_restores_nodes_and_edges() {
    let dir = TempDir::new().expect("temp dir");
    let export_path = dir.path().join("g.parquet");
    let mut executor = Executor::open_cache().expect("cache executor opens");

    // Source graph `g`: three propertied nodes and two weighted edges.
    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "g".to_owned(),
        })
        .expect("graph create succeeds");
    for (node_id, kind) in [("n1", "person"), ("n2", "person"), ("n3", "org")] {
        executor
            .execute(Command::GraphAddNode {
                object_type: None,
                branch: None,
                space: None,
                graph: "g".to_owned(),
                node_id: node_id.to_owned(),
                properties: Some(json!({"kind": kind})),
                binding: None,
            })
            .expect("node add succeeds");
    }
    for (src, edge_type, dst, weight) in [
        ("n1", "knows", "n2", 1.5_f64),
        ("n1", "works_at", "n3", 2.0),
    ] {
        executor
            .execute(Command::GraphAddEdge {
                branch: None,
                space: None,
                graph: "g".to_owned(),
                src: src.to_owned(),
                edge_type: edge_type.to_owned(),
                dst: dst.to_owned(),
                weight: Some(weight),
                properties: None,
            })
            .expect("edge add succeeds");
    }

    // Export `g`: writes g_nodes.parquet + g_edges.parquet under the stem.
    executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Graph,
            format: ArrowFileFormat::Parquet,
            path: export_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: Some("g".to_owned()),
            event_type: None,
        })
        .expect("graph export succeeds");

    // Import the exported stem into a fresh graph `g2`; import re-derives the
    // node/edge paths from the stem exactly as export wrote them.
    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "g2".to_owned(),
        })
        .expect("second graph create succeeds");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: export_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Graph,
            key_column: None,
            value_column: None,
            collection: None,
            graph: Some("g2".to_owned()),
        })
        .expect("graph import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    // Three nodes + two edges imported across one node batch and one edge batch.
    assert_import_result(&result, ArrowImportTarget::Graph, 5, 0, 2);

    // Nodes round-trip with identical properties.
    let nodes = graph_node_properties(&mut executor, "g2");
    assert_eq!(nodes.len(), 3);
    assert_eq!(
        nodes.get("n1").cloned().flatten(),
        Some(json!({"kind": "person"}))
    );
    assert_eq!(
        nodes.get("n3").cloned().flatten(),
        Some(json!({"kind": "org"}))
    );

    // Edges round-trip with preserved type, endpoints, and weight.
    let mut edges = graph_outgoing_edges(&mut executor, "g2", "n1");
    edges.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(edges.len(), 2);
    assert_eq!(
        edges[0],
        ("n1".to_owned(), "knows".to_owned(), "n2".to_owned(), 1.5)
    );
    assert_eq!(
        edges[1],
        ("n1".to_owned(), "works_at".to_owned(), "n3".to_owned(), 2.0)
    );
}

#[test]
fn arrow_export_rejects_missing_primitive_options() {
    let dir = TempDir::new().expect("temp dir");
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let vector_error = executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Vector,
            format: ArrowFileFormat::Jsonl,
            path: dir
                .path()
                .join("vectors.jsonl")
                .to_string_lossy()
                .into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect_err("missing vector collection fails");
    assert_eq!(vector_error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        vector_error.code(),
        "invalid_argument.executor.arrow_collection"
    );

    let graph_error = executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Graph,
            format: ArrowFileFormat::Jsonl,
            path: dir
                .path()
                .join("graph.jsonl")
                .to_string_lossy()
                .into_owned(),
            prefix: None,
            limit: None,
            collection: None,
            graph: None,
            event_type: None,
        })
        .expect_err("missing graph fails");
    assert_eq!(graph_error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(graph_error.code(), "invalid_argument.executor.arrow_graph");
}

#[test]
fn arrow_import_rejects_unknown_format_before_storage_mutation() {
    let dir = TempDir::new().expect("temp dir");
    let input_path = dir.path().join("records.arrow");
    fs::write(&input_path, b"not read").expect("write unknown extension");
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: input_path.to_string_lossy().into_owned(),
            format: None,
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect_err("unknown format fails");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.executor.arrow_format");
    assert!(kv_keys(&mut executor).is_empty());
}

#[test]
fn missing_input_is_reported_before_arrow_feature_work() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: "definitely-missing.csv".to_owned(),
            format: Some(ArrowFileFormat::Csv),
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect_err("missing input fails");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.executor.arrow_input_missing"
    );
}

/// #3244: the `arrow_io` construction site restated the row's retry flag and
/// had drifted from it (`same_request` at the site, `after_state_change` in the
/// registry). Driven end to end through `ArrowImport` so the assertion observes
/// what the boundary hands the caller, not the site.
#[test]
fn malformed_input_carries_the_arrow_io_registry_row() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("bad.parquet");
    fs::write(&path, b"not parquet").expect("write malformed");
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: path.display().to_string(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect_err("malformed input fails");
    let entry =
        public_error_code_entry("unavailable.executor.arrow_io").expect("arrow_io is registered");
    assert_eq!(error.code(), entry.code);
    assert_eq!(error.public_class(), entry.class);
    assert_eq!(error.retry_policy(), entry.retry_policy);
    assert_eq!(error.commit_outcome(), entry.commit_outcome);
    assert_eq!(error.suggested_fix(), entry.suggested_fix);
}

/// #3078: a Parquet Float64 column holding NaN must fail the import with a typed
/// error, not silently store `{"score": null}`. (JSONL cannot carry a literal
/// NaN — the JSON parser rejects it — so a float column is the reachable path.)
#[test]
fn parquet_import_rejects_non_finite_float_instead_of_storing_null() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("scores.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.5, f64::NAN])),
        ],
    )
    .expect("record batch");
    let file = std::fs::File::create(&path).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Json,
            key_column: Some("key".to_owned()),
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect_err("a non-finite float must fail the import, not store null");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.executor.arrow_non_finite_float"
    );
}

/// #3081: `import_event` discarded the per-item batch result and reported every
/// row imported with zero skipped. When the engine rejects some rows (here an
/// empty `event_type`, which `EventType::new` refuses), the import must report
/// the real counts, not a success that overstates what landed.
#[test]
fn event_import_reports_rows_the_engine_rejected_as_skipped() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("events.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    // Row 1 is valid; row 2 has an empty event_type the engine rejects per-item.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["order.created", ""])),
            Arc::new(StringArray::from(vec![r#"{"id":1}"#, r#"{"id":2}"#])),
        ],
    )
    .expect("record batch");
    let file = std::fs::File::create(&path).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Event,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("event import succeeds (as a partial)");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    // One row landed, one was rejected — not the old "2 imported, 0 skipped".
    assert_eq!(result.rows_imported(), 1);
    assert_eq!(result.rows_skipped(), 1);

    // And the valid event really is the only one present on the branch.
    assert_eq!(event_count(&mut executor), 1);
}

/// #3082: vector export to CSV cannot work — the embedding is a nested
/// `FixedSizeList` the CSV writer can't serialize. It must be rejected up front
/// with a non-retryable error and must not truncate/create the output file
/// (the old path left a stale header-only file and reported a retryable IO
/// error, so a retrying caller looped forever).
#[test]
fn vector_export_to_csv_is_rejected_up_front_without_touching_the_output_file() {
    let dir = TempDir::new().expect("temp dir");
    let source_path = dir.path().join("source.parquet");
    let out_path = dir.path().join("vectors.csv");
    write_vector_parquet(&source_path);

    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_docs_collection(&mut executor);
    executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: source_path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Parquet),
            target: ArrowImportTarget::Vector,
            key_column: None,
            value_column: None,
            collection: Some("docs".to_owned()),
            graph: None,
        })
        .expect("seed import succeeds");

    let error = executor
        .execute(Command::ArrowExport {
            branch: None,
            space: None,
            primitive: ArrowExportPrimitive::Vector,
            format: ArrowFileFormat::Csv,
            path: out_path.to_string_lossy().into_owned(),
            prefix: None,
            limit: None,
            collection: Some("docs".to_owned()),
            graph: None,
            event_type: None,
        })
        .expect_err("vector export to CSV must be rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.executor.arrow_format");
    // Permanent incompatibility — a retrying caller must not loop.
    assert!(!error.retryable(), "the rejection must be non-retryable");
    // The output file was never opened, so no stale/truncated file is left.
    assert!(!out_path.exists(), "no output file should be created");
}

/// #3083: a 0-row export writes an empty file whose inferred schema has no
/// columns; re-importing it must be a no-op (zero rows), not fail with
/// "no key column found".
#[test]
fn empty_jsonl_import_is_a_no_op_not_a_missing_key_error() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("empty.jsonl");
    std::fs::write(&path, "").expect("write empty jsonl");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: path.to_string_lossy().into_owned(),
            format: Some(ArrowFileFormat::Jsonl),
            target: ArrowImportTarget::Kv,
            key_column: None,
            value_column: None,
            collection: None,
            graph: None,
        })
        .expect("empty import succeeds as a no-op");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_eq!(result.rows_imported(), 0);
    assert_eq!(result.rows_skipped(), 0);
}

/// #3083: a flag that doesn't apply to the import target must be rejected, not
/// silently ignored — `--collection` is only for vector, `--graph` only for graph.
#[test]
fn kv_import_rejects_irrelevant_collection_and_graph_flags() {
    let dir = TempDir::new().expect("temp dir");
    let input = dir.path().join("kv.csv");
    fs::write(&input, "key,value\na,x\n").expect("write csv");

    let mut executor = Executor::open_cache().expect("cache executor opens");
    let kv_import = |collection: Option<String>, graph: Option<String>| Command::ArrowImport {
        branch: None,
        space: None,
        file_path: input.to_string_lossy().into_owned(),
        format: Some(ArrowFileFormat::Csv),
        target: ArrowImportTarget::Kv,
        key_column: None,
        value_column: None,
        collection,
        graph,
    };

    // --collection on a kv import is rejected (only valid for vector).
    let error = executor
        .execute(kv_import(Some("docs".to_owned()), None))
        .expect_err("collection is irrelevant for kv");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.executor.arrow_collection");

    // --graph on a kv import is rejected (only valid for graph).
    let error = executor
        .execute(kv_import(None, Some("g".to_owned())))
        .expect_err("graph is irrelevant for kv");
    assert_eq!(error.code(), "invalid_argument.executor.arrow_graph");

    // Direction control: a plain kv import (no irrelevant flags) still works.
    let output = executor
        .execute(kv_import(None, None))
        .expect("plain kv import succeeds");
    let Output::ArrowImportResult(result) = output else {
        panic!("unexpected import output");
    };
    assert_eq!(result.rows_imported(), 1);
    assert_eq!(kv_get(&mut executor, Bytes::from("a")), Some(b"x".to_vec()));
}

fn assert_import_result(
    result: &ArrowImportResult,
    target: ArrowImportTarget,
    rows_imported: u64,
    rows_skipped: u64,
    batches_processed: u64,
) {
    assert_eq!(result.target(), target);
    assert_eq!(result.rows_imported(), rows_imported);
    assert_eq!(result.rows_skipped(), rows_skipped);
    assert_eq!(result.batches_processed(), batches_processed);
}

fn kv_get(executor: &mut Executor, key: Bytes) -> Option<Vec<u8>> {
    kv_get_in(executor, None, None, key)
}

fn kv_keys(executor: &mut Executor) -> Vec<Bytes> {
    let output = executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: None,
            cursor: None,
            limit: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("kv list succeeds");
    match output {
        Output::KeysPage { items: keys, .. } => keys,
        output => panic!("unexpected kv list output: {output:?}"),
    }
}

fn kv_get_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    key: Bytes,
) -> Option<Vec<u8>> {
    let output = executor
        .execute(Command::KvGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            key,
            as_of: None,
            as_of_time: None,
        })
        .expect("kv get succeeds");
    let Output::KvVersionedValue(value) = output else {
        panic!("unexpected kv get output");
    };
    value
        .into_option()
        .map(|value| value.value().as_slice().to_vec())
}

fn json_get(executor: &mut Executor, key: &str) -> Option<Value> {
    json_get_in(executor, None, None, key)
}

fn json_get_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    key: &str,
) -> Option<Value> {
    let output = executor
        .execute(Command::JsonGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            key: key.to_owned(),
            path: "$".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("json get succeeds");
    let Output::JsonVersionedValue(value) = output else {
        panic!("unexpected json get output");
    };
    value.value().map(|value| value.value().clone())
}

fn vector_count(executor: &mut Executor, collection: &str) -> u64 {
    let output = executor
        .execute(Command::VectorCount {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("vector count succeeds");
    let Output::Uint(count) = output else {
        panic!("unexpected vector count output");
    };
    count
}

fn event_count(executor: &mut Executor) -> u64 {
    let output = executor
        .execute(Command::EventCount {
            branch: None,
            space: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("event len succeeds");
    let Output::EventCount { count } = output else {
        panic!("unexpected event len output");
    };
    count
}

fn vector_get_metadata(executor: &mut Executor, collection: &str, key: &str) -> Value {
    let output = executor
        .execute(Command::VectorGet {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("vector get succeeds");
    let Output::VectorData(value) = output else {
        panic!("unexpected vector get output");
    };
    value
        .into_option()
        .expect("vector value present")
        .data()
        .metadata()
        .cloned()
        .expect("metadata")
}

fn graph_node_properties(
    executor: &mut Executor,
    graph: &str,
) -> std::collections::BTreeMap<String, Option<Value>> {
    let mut properties = std::collections::BTreeMap::new();
    let mut cursor = None;
    loop {
        let output = executor
            .execute(Command::GraphListNodes {
                branch: None,
                space: None,
                graph: graph.to_owned(),
                prefix: None,
                cursor,
                limit: Some(100),
                as_of: None,
                as_of_time: None,
            })
            .expect("graph list nodes succeeds");
        let Output::GraphNodePage { items, page } = output else {
            panic!("unexpected graph list nodes output");
        };
        let has_more = page.has_more();
        let next_cursor = page.cursor().cloned();
        for node in &items {
            properties.insert(node.node_id().to_owned(), node.properties().cloned());
        }
        if !has_more {
            break;
        }
        cursor = next_cursor;
    }
    properties
}

fn graph_outgoing_edges(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
) -> Vec<(String, String, String, f64)> {
    let mut edges = Vec::new();
    let mut cursor = None;
    loop {
        let output = executor
            .execute(Command::GraphNeighbors {
                branch: None,
                space: None,
                graph: graph.to_owned(),
                node_id: node_id.to_owned(),
                direction: GraphDirection::Outgoing,
                edge_type: None,
                cursor,
                limit: Some(100),
                as_of: None,
                as_of_time: None,
            })
            .expect("graph neighbors succeeds");
        let Output::GraphNeighborPage { items, page } = output else {
            panic!("unexpected graph neighbors output");
        };
        let has_more = page.has_more();
        let next_cursor = page.cursor().cloned();
        for hit in &items {
            let edge = hit.edge();
            edges.push((
                edge.src().to_owned(),
                edge.edge_type().to_owned(),
                edge.dst().to_owned(),
                edge.weight(),
            ));
        }
        if !has_more {
            break;
        }
        cursor = next_cursor;
    }
    edges
}

fn create_docs_collection(executor: &mut Executor) {
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("collection create succeeds");
}

fn write_vector_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            false,
        ),
        Field::new("kind", DataType::Utf8, false),
    ]));
    let mut embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    for value in [1.0, 0.0, 0.0, 1.0] {
        embedding_builder.values().append_value(value);
    }
    embedding_builder.append(true);
    embedding_builder.append(true);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["doc-a", "doc-b"])),
            Arc::new(embedding_builder.finish()),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .expect("record batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn create_graph_fixture(executor: &mut Executor) {
    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "deps".to_owned(),
        })
        .expect("graph create succeeds");
    for (node_id, kind, binding) in [
        ("node-a", "root", Some(graph_binding("doc-a"))),
        ("node-b", "leaf", None),
    ] {
        executor
            .execute(Command::GraphAddNode {
                object_type: None,
                branch: None,
                space: None,
                graph: "deps".to_owned(),
                node_id: node_id.to_owned(),
                properties: Some(json!({"kind": kind})),
                binding,
            })
            .expect("node add succeeds");
    }
    executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: "deps".to_owned(),
            src: "node-a".to_owned(),
            edge_type: "depends_on".to_owned(),
            dst: "node-b".to_owned(),
            weight: Some(2.5),
            properties: Some(json!({"why": "test"})),
        })
        .expect("edge add succeeds");
}

fn graph_binding(key: &str) -> GraphEntityBinding {
    GraphEntityBinding::new(GraphBindingTarget::new(
        GraphBindingPrimitive::Json,
        None,
        "docs",
        key,
    ))
}
