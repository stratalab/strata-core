//! Serialization round-trip tests for Command and Output enums.
//!
//! These tests verify that all enum variants can be serialized to JSON
//! and deserialized back without loss of information.

use crate::types::*;
use crate::Value;
use crate::{Command, Output};
use strata_engine::MergeStrategy;

/// Helper to test round-trip serialization of a Command.
fn test_command_round_trip(cmd: Command) {
    let json = serde_json::to_string(&cmd).expect("Failed to serialize command");
    let restored: Command = serde_json::from_str(&json).expect("Failed to deserialize command");
    assert_eq!(
        serde_json::to_value(&cmd).unwrap(),
        serde_json::to_value(&restored).unwrap(),
        "Command round-trip failed for: {:?}",
        cmd
    );
}

/// Helper to test round-trip serialization of an Output.
fn test_output_round_trip(output: Output) {
    let json = serde_json::to_string(&output).expect("Failed to serialize output");
    let restored: Output = serde_json::from_str(&json).expect("Failed to deserialize output");

    // For outputs containing floats that might be NaN, compare JSON values
    let original_json = serde_json::to_value(&output).unwrap();
    let restored_json = serde_json::to_value(&restored).unwrap();

    // NaN != NaN, so we compare JSON representations
    assert_eq!(
        original_json.to_string(),
        restored_json.to_string(),
        "Output round-trip failed for: {:?}",
        output
    );
}

// =============================================================================
// Database Command Tests
// =============================================================================

#[test]
fn test_command_ping() {
    test_command_round_trip(Command::Ping);
}

#[test]
fn test_command_info() {
    test_command_round_trip(Command::Info);
}

#[test]
fn test_command_flush() {
    test_command_round_trip(Command::Flush);
}

#[test]
fn test_command_compact() {
    test_command_round_trip(Command::Compact);
}

// =============================================================================
// KV Command Tests (4 MVP)
// =============================================================================

#[test]
fn test_command_kv_put() {
    test_command_round_trip(Command::KvPut {
        branch: Some(BranchId::from("default")),
        space: None,
        key: "test-key".to_string(),
        value: Value::String("test-value".to_string()),
    });
}

#[test]
fn test_command_kv_get() {
    test_command_round_trip(Command::KvGet {
        branch: Some(BranchId::from("default")),
        space: None,
        key: "test-key".to_string(),
        as_of: None,
    });
}

#[test]
fn test_command_kv_delete() {
    test_command_round_trip(Command::KvDelete {
        branch: Some(BranchId::from("default")),
        space: None,
        key: "test-key".to_string(),
    });
}

#[test]
fn test_command_kv_list() {
    test_command_round_trip(Command::KvList {
        branch: Some(BranchId::from("default")),
        space: None,
        prefix: Some("user:".to_string()),
        cursor: None,
        limit: None,
        as_of: None,
    });
}

// =============================================================================
// JSON Command Tests
// =============================================================================

#[test]
fn test_command_json_set() {
    test_command_round_trip(Command::JsonSet {
        branch: Some(BranchId::from("default")),
        space: None,
        key: "doc1".to_string(),
        path: "$.name".to_string(),
        value: Value::String("Alice".to_string()),
    });
}

#[test]
fn test_command_json_get() {
    test_command_round_trip(Command::JsonGet {
        branch: Some(BranchId::from("default")),
        space: None,
        key: "doc1".to_string(),
        path: "$.name".to_string(),
        as_of: None,
    });
}

// =============================================================================
// Event Command Tests (4 MVP)
// =============================================================================

#[test]
fn test_command_event_append() {
    test_command_round_trip(Command::EventAppend {
        branch: Some(BranchId::from("default")),
        space: None,
        event_type: "events".to_string(),
        payload: Value::Object(
            [("type".to_string(), Value::String("click".to_string()))]
                .into_iter()
                .collect(),
        ),
    });
}

#[test]
fn test_command_event_get() {
    test_command_round_trip(Command::EventGet {
        branch: Some(BranchId::from("default")),
        space: None,
        sequence: 42,
        as_of: None,
    });
}

#[test]
fn test_command_event_get_by_type() {
    test_command_round_trip(Command::EventGetByType {
        branch: Some(BranchId::from("default")),
        space: None,
        event_type: "events".to_string(),
        limit: None,
        after_sequence: None,
        as_of: None,
    });
}

#[test]
fn test_command_event_len() {
    test_command_round_trip(Command::EventLen {
        branch: Some(BranchId::from("default")),
        space: None,
    });
}

// =============================================================================
// State Command Tests
// =============================================================================

#[test]
fn test_command_state_set() {
    test_command_round_trip(Command::StateSet {
        branch: Some(BranchId::from("default")),
        space: None,
        cell: "counter".to_string(),
        value: Value::Int(42),
    });
}

#[test]
fn test_command_state_cas() {
    test_command_round_trip(Command::StateCas {
        branch: Some(BranchId::from("default")),
        space: None,
        cell: "counter".to_string(),
        expected_counter: Some(5),
        value: Value::Int(6),
    });
}

// =============================================================================
// Vector Command Tests
// =============================================================================

#[test]
fn test_command_vector_upsert() {
    test_command_round_trip(Command::VectorUpsert {
        branch: Some(BranchId::from("default")),
        space: None,
        collection: "embeddings".to_string(),
        key: "vec1".to_string(),
        vector: vec![0.1, 0.2, 0.3, 0.4],
        metadata: Some(Value::Object(
            [("label".to_string(), Value::String("test".to_string()))]
                .into_iter()
                .collect(),
        )),
    });
}

#[test]
fn test_command_vector_search() {
    test_command_round_trip(Command::VectorSearch {
        branch: Some(BranchId::from("default")),
        space: None,
        collection: "embeddings".to_string(),
        query: vec![0.1, 0.2, 0.3, 0.4],
        k: 10,
        filter: None,
        metric: Some(DistanceMetric::Cosine),
        as_of: None,
    });
}

#[test]
fn test_command_vector_create_collection() {
    test_command_round_trip(Command::VectorCreateCollection {
        branch: Some(BranchId::from("default")),
        space: None,
        collection: "embeddings".to_string(),
        dimension: 384,
        metric: DistanceMetric::Cosine,
    });
}

// =============================================================================
// Branch Command Tests
// =============================================================================

#[test]
fn test_command_branch_create() {
    test_command_round_trip(Command::BranchCreate {
        branch_id: Some("my-branch".to_string()),
        metadata: Some(Value::Object(
            [("name".to_string(), Value::String("Test Branch".to_string()))]
                .into_iter()
                .collect(),
        )),
    });
}

#[test]
fn test_command_branch_list() {
    test_command_round_trip(Command::BranchList {
        state: Some(BranchStatus::Active),
        limit: Some(10),
        offset: Some(0),
    });
}

// =============================================================================
// Transaction Command Tests
// =============================================================================

#[test]
fn test_command_txn_begin() {
    test_command_round_trip(Command::TxnBegin {
        branch: None,
        options: Some(TxnOptions { read_only: true }),
    });
}

#[test]
fn test_command_txn_commit() {
    test_command_round_trip(Command::TxnCommit);
}

#[test]
fn test_command_txn_rollback() {
    test_command_round_trip(Command::TxnRollback);
}

// =============================================================================
// Output Tests
// =============================================================================

#[test]
fn test_output_unit() {
    test_output_round_trip(Output::Unit);
}

#[test]
fn test_output_bool() {
    test_output_round_trip(Output::Bool(true));
    test_output_round_trip(Output::Bool(false));
}

#[test]
fn test_output_uint() {
    test_output_round_trip(Output::Uint(12345));
}

#[test]
fn test_output_version() {
    test_output_round_trip(Output::Version(42));
}

#[test]
fn test_output_maybe_versioned() {
    test_output_round_trip(Output::MaybeVersioned(Some(VersionedValue {
        value: Value::Int(42),
        version: 5,
        timestamp: 2000000,
    })));
    test_output_round_trip(Output::MaybeVersioned(None));
}

#[test]
fn test_output_keys() {
    test_output_round_trip(Output::Keys(vec![
        "key1".to_string(),
        "key2".to_string(),
        "key3".to_string(),
    ]));
}

#[test]
fn test_output_versioned_values() {
    test_output_round_trip(Output::VersionedValues(vec![
        VersionedValue {
            value: Value::Int(1),
            version: 1,
            timestamp: 1000,
        },
        VersionedValue {
            value: Value::Int(2),
            version: 2,
            timestamp: 2000,
        },
    ]));
}

#[test]
fn test_output_vector_matches() {
    test_output_round_trip(Output::VectorMatches(vec![VectorMatch {
        key: "vec1".to_string(),
        score: 0.95,
        metadata: Some(Value::String("test".to_string())),
    }]));
}

#[test]
fn test_output_branch_info() {
    test_output_round_trip(Output::BranchWithVersion {
        info: BranchInfo {
            id: BranchId::from("test-branch"),
            status: BranchStatus::Active,
            created_at: 1000000,
            updated_at: 1000000,
            parent_id: None,
        },
        version: 1,
    });
}

#[test]
fn test_output_pong() {
    test_output_round_trip(Output::Pong {
        version: "0.1.0".to_string(),
    });
}

#[test]
fn test_output_database_info() {
    test_output_round_trip(Output::DatabaseInfo(DatabaseInfo {
        version: "0.1.0".to_string(),
        uptime_secs: 3600,
        branch_count: 10,
        total_keys: 1000,
    }));
}

// =============================================================================
// Search Command Tests
// =============================================================================

#[test]
fn test_command_search_minimal() {
    test_command_round_trip(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "user auth errors".to_string(),
            k: None,
            primitives: None,
            time_range: None,
            mode: None,
            expand: None,
            rerank: None,
            precomputed_embedding: None,
        },
    });
}

#[test]
fn test_command_search_full() {
    test_command_round_trip(Command::Search {
        branch: Some(BranchId::from("default")),
        space: Some("production".to_string()),
        search: SearchQuery {
            query: "user auth errors".to_string(),
            k: Some(10),
            primitives: Some(vec!["kv".to_string(), "json".to_string()]),
            time_range: Some(TimeRangeInput {
                start: "2026-02-07T00:00:00Z".to_string(),
                end: "2026-02-09T23:59:59Z".to_string(),
            }),
            mode: Some("hybrid".to_string()),
            expand: Some(true),
            rerank: Some(false),
            precomputed_embedding: None,
        },
    });
}

// =============================================================================
// Complex Value Serialization Tests
// =============================================================================

#[test]
fn test_command_with_complex_value() {
    let complex_value = Value::Object(
        [
            ("string".to_string(), Value::String("hello".to_string())),
            ("int".to_string(), Value::Int(42)),
            ("float".to_string(), Value::Float(3.14)),
            ("bool".to_string(), Value::Bool(true)),
            ("null".to_string(), Value::Null),
            (
                "array".to_string(),
                Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            ),
            (
                "nested".to_string(),
                Value::Object(
                    [("deep".to_string(), Value::String("value".to_string()))]
                        .into_iter()
                        .collect(),
                ),
            ),
        ]
        .into_iter()
        .collect(),
    );

    test_command_round_trip(Command::KvPut {
        branch: Some(BranchId::from("default")),
        space: None,
        key: "complex".to_string(),
        value: complex_value,
    });
}

#[test]
fn test_command_with_bytes_value() {
    test_command_round_trip(Command::KvPut {
        branch: Some(BranchId::from("default")),
        space: None,
        key: "binary".to_string(),
        value: Value::Bytes(vec![0, 1, 2, 255, 254, 253]),
    });
}

// =============================================================================
// Optional Branch Serialization Tests
// =============================================================================

#[test]
fn test_command_with_branch_none_round_trip() {
    // Commands with branch: None should serialize without a branch field
    // and deserialize back to branch: None
    let cmd = Command::KvPut {
        branch: None,
        space: None,
        key: "test".to_string(),
        value: Value::Int(42),
    };
    let json = serde_json::to_string(&cmd).unwrap();
    assert!(
        !json.contains("branch"),
        "branch: None should be skipped in serialization"
    );
    let restored: Command = serde_json::from_str(&json).unwrap();
    match restored {
        Command::KvPut {
            branch, key, value, ..
        } => {
            assert!(
                branch.is_none(),
                "branch should deserialize as None when omitted"
            );
            assert_eq!(key, "test");
            assert_eq!(value, Value::Int(42));
        }
        _ => panic!("Wrong command variant"),
    }
}

#[test]
fn test_command_with_branch_some_round_trip() {
    // Commands with branch: Some(...) should include the branch field
    let cmd = Command::KvGet {
        branch: Some(BranchId::from("my-branch")),
        space: None,
        key: "test".to_string(),
        as_of: None,
    };
    let json = serde_json::to_string(&cmd).unwrap();
    assert!(
        json.contains("branch"),
        "branch: Some should be included in serialization"
    );
    let restored: Command = serde_json::from_str(&json).unwrap();
    match restored {
        Command::KvGet { branch, key, .. } => {
            assert_eq!(branch, Some(BranchId::from("my-branch")));
            assert_eq!(key, "test");
        }
        _ => panic!("Wrong command variant"),
    }
}

#[test]
fn test_command_json_omitted_branch_deserializes() {
    // Verify that JSON without a "branch" field deserializes to branch: None
    let json = r#"{"KvPut":{"key":"foo","value":{"Int":42}}}"#;
    let cmd: Command = serde_json::from_str(json).unwrap();
    match cmd {
        Command::KvPut {
            branch, key, value, ..
        } => {
            assert!(branch.is_none());
            assert_eq!(key, "foo");
            assert_eq!(value, Value::Int(42));
        }
        _ => panic!("Wrong command variant"),
    }
}

#[test]
fn test_command_json_explicit_branch_deserializes() {
    // Verify that JSON with "branch": "default" still works
    let json = r#"{"KvPut":{"branch":"default","key":"foo","value":{"Int":42}}}"#;
    let cmd: Command = serde_json::from_str(json).unwrap();
    match cmd {
        Command::KvPut {
            branch, key, value, ..
        } => {
            assert_eq!(branch, Some(BranchId::from("default")));
            assert_eq!(key, "foo");
            assert_eq!(value, Value::Int(42));
        }
        _ => panic!("Wrong command variant"),
    }
}

// =============================================================================
// New Command Variant Round-Trip Tests
// =============================================================================

#[test]
fn test_command_branch_fork() {
    test_command_round_trip(Command::BranchFork {
        source: "main".to_string(),
        destination: "experiment".to_string(),
    });
}

#[test]
fn test_command_branch_diff() {
    test_command_round_trip(Command::BranchDiff {
        branch_a: "main".to_string(),
        branch_b: "experiment".to_string(),
    });
}

#[test]
fn test_command_branch_merge_lww() {
    test_command_round_trip(Command::BranchMerge {
        source: "experiment".to_string(),
        target: "main".to_string(),
        strategy: MergeStrategy::LastWriterWins,
    });
}

#[test]
fn test_command_branch_merge_strict() {
    test_command_round_trip(Command::BranchMerge {
        source: "experiment".to_string(),
        target: "main".to_string(),
        strategy: MergeStrategy::Strict,
    });
}

#[test]
fn test_command_config_get() {
    test_command_round_trip(Command::ConfigGet);
}

#[test]
fn test_command_config_set_auto_embed() {
    test_command_round_trip(Command::ConfigSetAutoEmbed { enabled: true });
    test_command_round_trip(Command::ConfigSetAutoEmbed { enabled: false });
}

#[test]
fn test_command_auto_embed_status() {
    test_command_round_trip(Command::AutoEmbedStatus);
}

#[test]
fn test_command_durability_counters() {
    test_command_round_trip(Command::DurabilityCounters);
}

// =============================================================================
// New Output Variant Serialization Tests (#1200 + #1205)
// =============================================================================

#[test]
fn test_output_branch_forked() {
    test_output_round_trip(Output::BranchForked(strata_engine::branch_ops::ForkInfo {
        source: "main".to_string(),
        destination: "experiment".to_string(),
        keys_copied: 42,
        spaces_copied: 2,
    }));
}

#[test]
fn test_output_branch_diff() {
    use strata_engine::branch_ops::*;
    test_output_round_trip(Output::BranchDiff(BranchDiffResult {
        branch_a: "main".to_string(),
        branch_b: "experiment".to_string(),
        spaces: vec![SpaceDiff {
            space: "default".to_string(),
            added: vec![BranchDiffEntry {
                key: "new-key".to_string(),
                raw_key: b"new-key".to_vec(),
                primitive: strata_core::PrimitiveType::Kv,
                space: "default".to_string(),
                value_a: None,
                value_b: Some("hello".to_string()),
            }],
            removed: vec![],
            modified: vec![],
        }],
        summary: DiffSummary {
            total_added: 1,
            total_removed: 0,
            total_modified: 0,
            spaces_only_in_a: vec![],
            spaces_only_in_b: vec![],
        },
    }));
}

#[test]
fn test_output_branch_merged() {
    test_output_round_trip(Output::BranchMerged(strata_engine::branch_ops::MergeInfo {
        source: "experiment".to_string(),
        target: "main".to_string(),
        keys_applied: 5,
        conflicts: vec![],
        spaces_merged: 1,
    }));
}

#[test]
fn test_output_config() {
    test_output_round_trip(Output::Config(strata_engine::StrataConfig {
        durability: "standard".to_string(),
        auto_embed: false,
        model: None,
        embed_batch_size: None,
        bm25_k1: None,
        bm25_b: None,
    }));
}

#[test]
fn test_output_durability_counters() {
    test_output_round_trip(Output::DurabilityCounters(strata_engine::WalCounters {
        wal_appends: 100,
        sync_calls: 10,
        bytes_written: 4096,
        sync_nanos: 500_000,
    }));
    // Also test default (cache databases)
    test_output_round_trip(Output::DurabilityCounters(
        strata_engine::WalCounters::default(),
    ));
}
