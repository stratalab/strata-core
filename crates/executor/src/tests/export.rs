//! Integration tests for the DbExport command.
//!
//! These tests write real data via the executor and verify that DbExport
//! produces correct CSV, JSON, and JSONL output.

use crate::types::*;
use crate::Value;
use crate::{Command, Executor, Output, Strata};

/// Helper: create a cache executor with some KV data.
fn setup_kv_db() -> Executor {
    let strata = Strata::cache().unwrap();
    strata.kv_put("user:1", Value::Int(10)).unwrap();
    strata.kv_put("user:2", Value::Int(20)).unwrap();
    strata.kv_put("other:1", Value::Int(99)).unwrap();
    let db = strata.database();
    Executor::new(db)
}

// =============================================================================
// KV exports
// =============================================================================

#[test]
fn export_kv_csv_inline() {
    let executor = setup_kv_db();
    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Csv,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 3);
            assert_eq!(r.format, ExportFormat::Csv);
            assert_eq!(r.primitive, ExportPrimitive::Kv);
            assert!(r.path.is_none());
            assert!(r.size_bytes.is_none());
            let data = r.data.unwrap();
            let lines: Vec<&str> = data.lines().collect();
            assert_eq!(lines[0], "key,value");
            assert_eq!(lines.len(), 4); // header + 3 rows
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_kv_json_inline() {
    let executor = setup_kv_db();
    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Json,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 3);
            let data = r.data.unwrap();
            let parsed: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap();
            assert_eq!(parsed.len(), 3);
            // Each should have "key" and "value"
            for item in &parsed {
                assert!(item.get("key").is_some());
                assert!(item.get("value").is_some());
            }
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_kv_jsonl_inline() {
    let executor = setup_kv_db();
    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Jsonl,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 3);
            let data = r.data.unwrap();
            let lines: Vec<&str> = data.lines().collect();
            assert_eq!(lines.len(), 3);
            // Each line is valid JSON
            for line in &lines {
                let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
                assert!(parsed.get("key").is_some());
            }
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_kv_with_prefix() {
    let executor = setup_kv_db();
    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Json,
            prefix: Some("user:".to_string()),
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 2); // only user:1 and user:2
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_kv_with_limit() {
    let executor = setup_kv_db();
    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Json,
            prefix: None,
            limit: Some(1),
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 1);
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_kv_to_file() {
    let executor = setup_kv_db();
    let tmp = std::env::temp_dir().join(format!("strata_export_test_{}.csv", std::process::id()));
    let path = tmp.to_str().unwrap().to_string();

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Csv,
            prefix: None,
            limit: None,
            path: Some(path.clone()),
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 3);
            assert!(r.data.is_none()); // not inline
            assert_eq!(r.path.as_deref(), Some(path.as_str()));
            assert!(r.size_bytes.unwrap() > 0);
            // Verify file exists and has content
            let content = std::fs::read_to_string(&path).unwrap();
            assert!(content.starts_with("key,value"));
            // Clean up
            let _ = std::fs::remove_file(&path);
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

// =============================================================================
// Empty exports
// =============================================================================

#[test]
fn export_empty_kv() {
    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Csv,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 0);
            assert_eq!(r.data.as_deref(), Some(""));
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_empty_json_format() {
    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Json,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 0);
            assert_eq!(r.data.as_deref(), Some("[]"));
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_empty_jsonl_format() {
    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Jsonl,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 0);
            assert_eq!(r.data.as_deref(), Some(""));
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

// =============================================================================
// State exports
// =============================================================================

#[test]
fn export_state_csv() {
    let strata = Strata::cache().unwrap();
    strata.state_set("counter", Value::Int(42)).unwrap();
    strata.state_set("flag", Value::Bool(true)).unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::State,
            format: ExportFormat::Csv,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 2);
            let data = r.data.unwrap();
            assert!(data.starts_with("key,value"));
            assert!(data.contains("counter,42") || data.contains("counter,42\n"));
            assert!(data.contains("flag,true"));
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

// =============================================================================
// Event exports
// =============================================================================

#[test]
fn export_events_json() {
    let strata = Strata::cache().unwrap();
    strata
        .event_append("click", Value::object(Default::default()))
        .unwrap();
    strata
        .event_append("purchase", Value::object(Default::default()))
        .unwrap();
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Events,
            format: ExportFormat::Json,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 2);
            let data = r.data.unwrap();
            let parsed: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap();
            assert_eq!(parsed.len(), 2);
            // Events are objects with sequence, event_type, payload, timestamp
            assert_eq!(parsed[0]["key"], "0");
            assert!(parsed[0]["sequence"].is_number());
            assert!(parsed[0]["event_type"].is_string());
            assert!(parsed[0]["timestamp"].is_number());
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

#[test]
fn export_events_with_limit() {
    let strata = Strata::cache().unwrap();
    for i in 0..5 {
        strata
            .event_append(&format!("e{}", i), Value::object(Default::default()))
            .unwrap();
    }
    let executor = Executor::new(strata.database());

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Events,
            format: ExportFormat::Json,
            prefix: None,
            limit: Some(2),
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 2);
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

// =============================================================================
// JSON doc exports
// =============================================================================

#[test]
fn export_json_docs() {
    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    // Insert JSON docs via executor
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "doc1".into(),
            path: "$".into(),
            value: Value::Int(100),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "doc2".into(),
            path: "$".into(),
            value: Value::String("hello".into()),
        })
        .unwrap();

    let result = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Json,
            format: ExportFormat::Json,
            prefix: None,
            limit: None,
            path: None,
        })
        .unwrap();

    match result {
        Output::Exported(r) => {
            assert_eq!(r.row_count, 2);
            let data = r.data.unwrap();
            let parsed: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap();
            assert_eq!(parsed.len(), 2);
            // Verify keys exist
            let keys: Vec<&str> = parsed.iter().map(|p| p["key"].as_str().unwrap()).collect();
            assert!(keys.contains(&"doc1"));
            assert!(keys.contains(&"doc2"));
        }
        other => panic!("expected Exported, got {:?}", other),
    }
}

// =============================================================================
// DbExport is not a write
// =============================================================================

#[test]
fn export_is_not_a_write() {
    let cmd = Command::DbExport {
        branch: None,
        space: None,
        primitive: ExportPrimitive::Kv,
        format: ExportFormat::Csv,
        prefix: None,
        limit: None,
        path: None,
    };
    assert!(!cmd.is_write());
}

// =============================================================================
// File path error handling
// =============================================================================

#[test]
fn export_to_nonexistent_directory_fails() {
    let strata = Strata::cache().unwrap();
    let executor = Executor::new(strata.database());

    let result = executor.execute(Command::DbExport {
        branch: None,
        space: None,
        primitive: ExportPrimitive::Kv,
        format: ExportFormat::Csv,
        prefix: None,
        limit: None,
        path: Some("/nonexistent/dir/file.csv".to_string()),
    });

    assert!(result.is_err());
}
