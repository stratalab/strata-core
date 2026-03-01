//! Serialization Tests
//!
//! Tests for JSON serialization/deserialization of Commands and Outputs.
//! This is critical for cross-language SDKs (Python, CLI, MCP).

use strata_core::Value;
use strata_executor::{BranchId, BranchStatus, Command, DistanceMetric, Output, VersionedValue};

// ============================================================================
// Command Serialization Roundtrip
// ============================================================================

#[test]
fn kv_put_roundtrip() {
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "test_key".into(),
        value: Value::String("test_value".into()),
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

#[test]
fn kv_put_with_branch_roundtrip() {
    let cmd = Command::KvPut {
        branch: Some(BranchId::from("550e8400-e29b-41d4-a716-446655440401")),
        space: None,
        key: "key".into(),
        value: Value::Int(42),
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

#[test]
fn kv_get_roundtrip() {
    let cmd = Command::KvGet {
        branch: None,
        space: None,
        key: "key".into(),
        as_of: None,
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

#[test]
fn event_append_roundtrip() {
    let cmd = Command::EventAppend {
        branch: None,
        space: None,
        event_type: "events".into(),
        payload: Value::object(
            [("data".to_string(), Value::Int(123))]
                .into_iter()
                .collect(),
        ),
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

#[test]
fn vector_search_roundtrip() {
    let cmd = Command::VectorSearch {
        branch: None,
        space: None,
        collection: "embeddings".into(),
        query: vec![1.0, 0.0, 0.0, 0.0],
        k: 10,
        filter: None,
        metric: Some(DistanceMetric::Cosine),
        as_of: None,
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

#[test]
fn branch_create_roundtrip() {
    let cmd = Command::BranchCreate {
        branch_id: Some("550e8400-e29b-41d4-a716-446655440401-id".into()),
        metadata: Some(Value::object(
            [("key".to_string(), Value::String("value".into()))]
                .into_iter()
                .collect(),
        )),
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

#[test]
fn txn_begin_roundtrip() {
    let cmd = Command::TxnBegin {
        branch: None,
        options: None,
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

// ============================================================================
// Command JSON Format Stability
// ============================================================================

#[test]
fn kv_put_json_format() {
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::Int(42),
    };

    let json = serde_json::to_value(&cmd).unwrap();

    assert_eq!(json["KvPut"]["key"], "k");
    // Value uses tagged enum serialization: {"Int": 42}
    assert_eq!(json["KvPut"]["value"]["Int"], 42);
}

#[test]
fn branch_field_omitted_when_none() {
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::Int(1),
    };

    let json = serde_json::to_string(&cmd).unwrap();

    // branch should not appear in JSON when None
    assert!(!json.contains("\"branch\""));
}

#[test]
fn branch_field_present_when_some() {
    let cmd = Command::KvPut {
        branch: Some(BranchId::from("550e8400-e29b-41d4-a716-446655440401")),
        space: None,
        key: "k".into(),
        value: Value::Int(1),
    };

    let json = serde_json::to_string(&cmd).unwrap();

    // branch should appear in JSON when Some
    assert!(json.contains("\"branch\""));
    assert!(json.contains("550e8400-e29b-41d4-a716-446655440401"));
}

// ============================================================================
// Deserialize from External JSON
// ============================================================================

#[test]
fn deserialize_kv_put_minimal() {
    // Value uses tagged enum serialization: {"Int": 42}
    let json = r#"{"KvPut":{"key":"k","value":{"Int":42}}}"#;

    let cmd: Command = serde_json::from_str(json).unwrap();

    match cmd {
        Command::KvPut {
            branch,
            space: _,
            key,
            value,
        } => {
            assert!(branch.is_none());
            assert_eq!(key, "k");
            assert_eq!(value, Value::Int(42));
        }
        _ => panic!("Expected KvPut"),
    }
}

#[test]
fn deserialize_kv_put_with_branch() {
    // Value uses tagged enum serialization: {"String": "v"}
    let json = r#"{"KvPut":{"branch":"550e8400-e29b-41d4-a716-446655440401","key":"k","value":{"String":"v"}}}"#;

    let cmd: Command = serde_json::from_str(json).unwrap();

    match cmd {
        Command::KvPut {
            branch,
            space: _,
            key,
            value,
        } => {
            assert_eq!(
                branch.unwrap().as_str(),
                "550e8400-e29b-41d4-a716-446655440401"
            );
            assert_eq!(key, "k");
            assert_eq!(value, Value::String("v".into()));
        }
        _ => panic!("Expected KvPut"),
    }
}

#[test]
fn deserialize_event_append() {
    // Value::Object uses tagged enum: {"Object": {...}}
    let json =
        r#"{"EventAppend":{"event_type":"logs","payload":{"Object":{"msg":{"String":"hello"}}}}}"#;

    let cmd: Command = serde_json::from_str(json).unwrap();

    match cmd {
        Command::EventAppend {
            branch,
            space: _,
            event_type,
            payload,
        } => {
            assert!(branch.is_none());
            assert_eq!(event_type, "logs");
            match payload {
                Value::Object(map) => {
                    assert_eq!(map.get("msg"), Some(&Value::String("hello".into())));
                }
                _ => panic!("Expected Object payload"),
            }
        }
        _ => panic!("Expected EventAppend"),
    }
}

#[test]
fn deserialize_vector_search() {
    let json = r#"{"VectorSearch":{"collection":"emb","query":[1.0,0.0,0.0],"k":5}}"#;

    let cmd: Command = serde_json::from_str(json).unwrap();

    match cmd {
        Command::VectorSearch {
            collection,
            query,
            k,
            ..
        } => {
            assert_eq!(collection, "emb");
            assert_eq!(query, vec![1.0, 0.0, 0.0]);
            assert_eq!(k, 5);
        }
        _ => panic!("Expected VectorSearch"),
    }
}

#[test]
fn deserialize_ping() {
    let json = r#""Ping""#;

    let cmd: Command = serde_json::from_str(json).unwrap();
    assert!(matches!(cmd, Command::Ping));
}

#[test]
fn deserialize_txn_commit() {
    let json = r#""TxnCommit""#;

    let cmd: Command = serde_json::from_str(json).unwrap();
    assert!(matches!(cmd, Command::TxnCommit));
}

// ============================================================================
// Output Serialization
// ============================================================================

#[test]
fn output_version_roundtrip() {
    let output = Output::Version(123);

    let json = serde_json::to_string(&output).unwrap();
    let parsed: Output = serde_json::from_str(&json).unwrap();

    assert_eq!(output, parsed);
}

#[test]
fn output_bool_roundtrip() {
    let output = Output::Bool(true);

    let json = serde_json::to_string(&output).unwrap();
    let parsed: Output = serde_json::from_str(&json).unwrap();

    assert_eq!(output, parsed);
}

#[test]
fn output_keys_roundtrip() {
    let output = Output::Keys(vec!["a".into(), "b".into(), "c".into()]);

    let json = serde_json::to_string(&output).unwrap();
    let parsed: Output = serde_json::from_str(&json).unwrap();

    assert_eq!(output, parsed);
}

#[test]
fn output_maybe_versioned_some_roundtrip() {
    let output = Output::MaybeVersioned(Some(VersionedValue {
        value: Value::Int(42),
        version: 1,
        timestamp: 12345,
    }));

    let json = serde_json::to_string(&output).unwrap();
    let parsed: Output = serde_json::from_str(&json).unwrap();

    assert_eq!(output, parsed);
}

#[test]
fn output_maybe_versioned_none_roundtrip() {
    let output = Output::MaybeVersioned(None);

    let json = serde_json::to_string(&output).unwrap();
    let parsed: Output = serde_json::from_str(&json).unwrap();

    assert_eq!(output, parsed);
}

// ============================================================================
// Value Type Serialization
// ============================================================================

#[test]
fn value_types_roundtrip() {
    // String
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::String("hello".into()),
    };
    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, parsed);

    // Int
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::Int(-42),
    };
    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, parsed);

    // Float
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::Float(3.125),
    };
    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, parsed);

    // Bool
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::Bool(true),
    };
    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, parsed);

    // Null
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::Null,
    };
    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, parsed);

    // Array
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::array(vec![Value::Int(1), Value::Int(2)]),
    };
    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, parsed);

    // Object
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::object(
            [("nested".to_string(), Value::Int(1))]
                .into_iter()
                .collect(),
        ),
    };
    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, parsed);
}

// ============================================================================
// Distance Metric Serialization
// ============================================================================

#[test]
fn distance_metric_roundtrip() {
    let metrics = [
        DistanceMetric::Cosine,
        DistanceMetric::Euclidean,
        DistanceMetric::DotProduct,
    ];

    for metric in metrics {
        let cmd = Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "test".into(),
            dimension: 4,
            metric,
        };

        let json = serde_json::to_string(&cmd).unwrap();
        let parsed: Command = serde_json::from_str(&json).unwrap();

        assert_eq!(cmd, parsed);
    }
}

// ============================================================================
// BranchStatus Serialization
// ============================================================================

#[test]
fn branch_status_roundtrip() {
    let cmd = Command::BranchList {
        state: Some(BranchStatus::Active),
        limit: None,
        offset: None,
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let parsed: Command = serde_json::from_str(&json).unwrap();

    assert_eq!(cmd, parsed);
}

// ============================================================================
// Unknown Fields Rejected
// ============================================================================

#[test]
fn unknown_fields_rejected() {
    // Command has deny_unknown_fields
    let json = r#"{"KvPut":{"key":"k","value":1,"unknown_field":"oops"}}"#;

    let result: Result<Command, _> = serde_json::from_str(json);
    assert!(result.is_err());
}
