//! Integration tests for the ArrowImport command.

#![cfg(feature = "arrow")]

use crate::types::*;
use crate::Value;
use crate::{Command, Executor, Output, Strata};

fn import_cmd(
    file_path: &str,
    target: &str,
    key_column: Option<&str>,
    value_column: Option<&str>,
    collection: Option<&str>,
    format: Option<&str>,
) -> Command {
    Command::ArrowImport {
        branch: None,
        space: None,
        file_path: file_path.to_string(),
        target: target.to_string(),
        key_column: key_column.map(String::from),
        value_column: value_column.map(String::from),
        collection: collection.map(String::from),
        format: format.map(String::from),
    }
}

// ---------------------------------------------------------------------------
// KV imports
// ---------------------------------------------------------------------------

#[test]
fn test_import_kv_from_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kv.parquet");

    // Write a Parquet file with key/value columns.
    write_kv_parquet(&path, &[("k1", "v1"), ("k2", "v2"), ("k3", "v3")]);

    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(import_cmd(
            path.to_str().unwrap(),
            "kv",
            Some("key"),
            Some("value"),
            None,
            None,
        ))
        .unwrap();

    match result {
        Output::ArrowImported {
            rows_imported,
            rows_skipped,
            target,
            ..
        } => {
            assert_eq!(rows_imported, 3);
            assert_eq!(rows_skipped, 0);
            assert_eq!(target, "kv");
        }
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    // Verify data.
    assert_eq!(
        strata.kv_get("k1").unwrap(),
        Some(Value::String("v1".into()))
    );
    assert_eq!(
        strata.kv_get("k2").unwrap(),
        Some(Value::String("v2".into()))
    );
}

#[test]
fn test_import_kv_from_csv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kv.csv");
    std::fs::write(&path, "key,value\nk1,v1\nk2,v2\n").unwrap();

    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(import_cmd(
            path.to_str().unwrap(),
            "kv",
            None,
            None,
            None,
            None,
        ))
        .unwrap();

    match result {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 2),
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    assert_eq!(
        strata.kv_get("k1").unwrap(),
        Some(Value::String("v1".into()))
    );
}

// ---------------------------------------------------------------------------
// JSON imports
// ---------------------------------------------------------------------------

#[test]
fn test_import_json_from_jsonl() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("docs.jsonl");
    std::fs::write(
        &path,
        r#"{"key":"d1","document":"{\"title\":\"Hello\"}"}
{"key":"d2","document":"{\"title\":\"World\"}"}
"#,
    )
    .unwrap();

    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(import_cmd(
            path.to_str().unwrap(),
            "json",
            Some("key"),
            Some("document"),
            None,
            None,
        ))
        .unwrap();

    match result {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 2),
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    let doc = strata.json_get("d1", "$").unwrap().unwrap();
    let s = format!("{doc:?}");
    assert!(s.contains("Hello"), "doc1 should contain 'Hello', got: {s}");
}

#[test]
fn test_import_json_remaining_columns_as_document() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("users.csv");
    std::fs::write(
        &path,
        "id,name,email\nu1,Alice,alice@example.com\nu2,Bob,bob@example.com\n",
    )
    .unwrap();

    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(import_cmd(
            path.to_str().unwrap(),
            "json",
            Some("id"),
            None,
            None,
            None,
        ))
        .unwrap();

    match result {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 2),
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    // Verify: extra columns (name, email) became the JSON document.
    let doc = strata.json_get("u1", "$").unwrap().unwrap();
    let s = format!("{doc:?}");
    assert!(s.contains("Alice"), "got: {s}");
    assert!(s.contains("alice@example.com"), "got: {s}");
}

// ---------------------------------------------------------------------------
// Vector imports
// ---------------------------------------------------------------------------

#[test]
fn test_import_vector_from_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("emb.parquet");

    write_vector_parquet(
        &path,
        &[("v1", vec![1.0, 0.0, 0.0]), ("v2", vec![0.0, 1.0, 0.0])],
    );

    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(import_cmd(
            path.to_str().unwrap(),
            "vector",
            Some("key"),
            Some("embedding"),
            Some("test_coll"),
            None,
        ))
        .unwrap();

    match result {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 2),
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    // Verify embedding data was actually stored.
    let entry = strata
        .vector_get("test_coll", "v1")
        .expect("vector get should succeed");
    assert!(entry.is_some(), "vector v1 should exist");
}

#[test]
fn test_import_vector_auto_creates_collection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("emb2.parquet");

    write_vector_parquet(&path, &[("v1", vec![1.0, 2.0, 3.0, 4.0])]);

    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(import_cmd(
            path.to_str().unwrap(),
            "vector",
            Some("key"),
            Some("embedding"),
            Some("auto_coll"),
            None,
        ))
        .unwrap();

    match result {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 1),
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    // Collection was auto-created — verify via export listing.
    let collections = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
        .unwrap();
    let s = format!("{collections:?}");
    assert!(s.contains("auto_coll"), "collection not found in: {s}");
}

// ---------------------------------------------------------------------------
// Round-trip tests
// ---------------------------------------------------------------------------

#[test]
fn test_roundtrip_kv_parquet() {
    // Populate KV.
    let strata = Strata::cache().unwrap();
    strata.kv_put("rt1", Value::String("hello".into())).unwrap();
    strata.kv_put("rt2", Value::String("world".into())).unwrap();
    let executor = Executor::new(strata.database());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roundtrip.parquet");
    let path_str = path.to_str().unwrap().to_string();

    // Export.
    executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Parquet,
            prefix: None,
            limit: None,
            path: Some(path_str.clone()),
            collection: None,
            graph: None,
        })
        .unwrap();

    // Create a new DB and import.
    let strata2 = Strata::cache().unwrap();
    let executor2 = Executor::new(strata2.database());

    let result = executor2
        .execute(import_cmd(&path_str, "kv", None, None, None, None))
        .unwrap();

    match result {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 2),
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    // Compare.
    assert_eq!(
        strata2.kv_get("rt1").unwrap(),
        Some(Value::String("hello".into()))
    );
    assert_eq!(
        strata2.kv_get("rt2").unwrap(),
        Some(Value::String("world".into()))
    );
}

#[test]
fn test_roundtrip_json_csv() {
    // Populate JSON.
    let strata = Strata::cache().unwrap();
    strata
        .json_set("rd1", "$", Value::String(r#"{"name":"Alice"}"#.into()))
        .unwrap();
    strata
        .json_set("rd2", "$", Value::String(r#"{"name":"Bob"}"#.into()))
        .unwrap();
    let executor = Executor::new(strata.database());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roundtrip.csv");
    let path_str = path.to_str().unwrap().to_string();

    // Export.
    executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Json,
            format: ExportFormat::Csv,
            prefix: None,
            limit: None,
            path: Some(path_str.clone()),
            collection: None,
            graph: None,
        })
        .unwrap();

    // Create a new DB and import.
    let strata2 = Strata::cache().unwrap();
    let executor2 = Executor::new(strata2.database());

    let result = executor2
        .execute(import_cmd(&path_str, "json", None, None, None, None))
        .unwrap();

    match result {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 2),
        other => panic!("expected ArrowImported, got {:?}", other),
    }

    // Verify docs exist with correct content.
    let doc1 = strata2.json_get("rd1", "$").unwrap().unwrap();
    let s1 = format!("{doc1:?}");
    assert!(
        s1.contains("Alice"),
        "rd1 should contain 'Alice', got: {s1}"
    );

    let doc2 = strata2.json_get("rd2", "$").unwrap().unwrap();
    let s2 = format!("{doc2:?}");
    assert!(s2.contains("Bob"), "rd2 should contain 'Bob', got: {s2}");
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

#[test]
fn test_import_vector_requires_collection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("emb.parquet");
    write_vector_parquet(&path, &[("v1", vec![1.0, 0.0, 0.0])]);

    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let err = executor
        .execute(import_cmd(
            path.to_str().unwrap(),
            "vector",
            Some("key"),
            Some("embedding"),
            None, // no --collection
            None,
        ))
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("--collection"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_kv_parquet(path: &std::path::Path, entries: &[(&str, &str)]) {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
    let values: Vec<&str> = entries.iter().map(|(_, v)| *v).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .unwrap();

    crate::arrow::write_file(path, crate::arrow::FileFormat::Parquet, &[batch]).unwrap();
}

fn write_vector_parquet(path: &std::path::Path, entries: &[(&str, Vec<f32>)]) {
    use arrow::array::{FixedSizeListArray, Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let dim = entries[0].1.len() as i32;

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        ),
    ]));

    let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
    let all_values: Vec<f32> = entries
        .iter()
        .flat_map(|(_, v)| v.iter().copied())
        .collect();

    let list = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim,
        Arc::new(Float32Array::from(all_values)),
        None,
    )
    .unwrap();

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(keys)), Arc::new(list)],
    )
    .unwrap();

    crate::arrow::write_file(path, crate::arrow::FileFormat::Parquet, &[batch]).unwrap();
}
