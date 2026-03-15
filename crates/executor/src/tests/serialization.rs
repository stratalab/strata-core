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

#[test]
fn test_command_describe() {
    test_command_round_trip(Command::Describe {
        branch: Some(BranchId::from("default")),
    });
    test_command_round_trip(Command::Describe { branch: None });
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
        payload: Value::object(
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
        metadata: Some(Value::object(
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
        metadata: Some(Value::object(
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
fn test_output_described() {
    test_output_round_trip(Output::Described(DescribeResult {
        version: "0.6.0".to_string(),
        path: "/tmp/test".to_string(),
        branch: "default".to_string(),
        branches: vec!["default".to_string(), "experiment".to_string()],
        spaces: vec!["default".to_string()],
        follower: false,
        primitives: PrimitiveSummary {
            kv: CountSummary { count: 10 },
            json: CountSummary { count: 5 },
            events: CountSummary { count: 100 },
            state: StateSummary {
                count: 2,
                cells: vec!["status".to_string(), "counter".to_string()],
            },
            vector: VectorSummary {
                collections: vec![VectorCollectionSummary {
                    name: "embeddings".to_string(),
                    dimension: 384,
                    metric: DistanceMetric::Cosine,
                    count: 5000,
                }],
            },
            graph: GraphSummary {
                graphs: vec![GraphSummaryEntry {
                    name: "social".to_string(),
                    nodes: 100,
                    edges: 500,
                    object_types: vec!["Person".to_string()],
                    link_types: vec!["follows".to_string()],
                }],
            },
        },
        config: ConfigSummary {
            provider: "local".to_string(),
            default_model: None,
            auto_embed: false,
            embed_model: "miniLM".to_string(),
            durability: "standard".to_string(),
        },
        capabilities: CapabilitySummary {
            search: true,
            vector_search: true,
            generation: false,
            auto_embed: false,
        },
    }));
}

#[test]
fn test_output_described_empty_graph_skips_ontology() {
    // GraphSummaryEntry with empty object_types/link_types should skip those in JSON
    let entry = GraphSummaryEntry {
        name: "g".to_string(),
        nodes: 0,
        edges: 0,
        object_types: vec![],
        link_types: vec![],
    };
    let json = serde_json::to_string(&entry).unwrap();
    assert!(
        !json.contains("object_types"),
        "Empty vec should be skipped: {}",
        json
    );
    assert!(
        !json.contains("link_types"),
        "Empty vec should be skipped: {}",
        json
    );

    // With values, they should appear
    let entry2 = GraphSummaryEntry {
        name: "g2".to_string(),
        nodes: 1,
        edges: 0,
        object_types: vec!["Person".to_string()],
        link_types: vec![],
    };
    let json2 = serde_json::to_string(&entry2).unwrap();
    assert!(
        json2.contains("object_types"),
        "Non-empty vec should appear: {}",
        json2
    );
    assert!(
        !json2.contains("link_types"),
        "Empty vec should be skipped: {}",
        json2
    );
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
// Data Introspection Command Tests (#1445)
// =============================================================================

#[test]
fn test_command_kv_count() {
    test_command_round_trip(Command::KvCount {
        branch: None,
        space: None,
        prefix: Some("user:".to_string()),
    });
    test_command_round_trip(Command::KvCount {
        branch: None,
        space: None,
        prefix: None,
    });
}

#[test]
fn test_command_json_count() {
    test_command_round_trip(Command::JsonCount {
        branch: None,
        space: None,
        prefix: Some("doc:".to_string()),
    });
}

#[test]
fn test_command_kv_sample() {
    test_command_round_trip(Command::KvSample {
        branch: None,
        space: None,
        prefix: None,
        count: Some(10),
    });
}

#[test]
fn test_command_json_sample() {
    test_command_round_trip(Command::JsonSample {
        branch: None,
        space: None,
        prefix: None,
        count: Some(5),
    });
}

#[test]
fn test_command_vector_sample() {
    test_command_round_trip(Command::VectorSample {
        branch: None,
        space: None,
        collection: "embeddings".to_string(),
        count: Some(3),
    });
}

// =============================================================================
// New Output Variant Tests (#1446, #1445, #1450)
// =============================================================================

#[test]
fn test_output_search_results_with_stats() {
    test_output_round_trip(Output::SearchResults {
        hits: vec![SearchResultHit {
            entity: "key1".to_string(),
            primitive: "kv".to_string(),
            score: 0.95,
            rank: 1,
            snippet: Some("matched text".to_string()),
        }],
        stats: SearchStatsOutput {
            elapsed_ms: 12.5,
            candidates_considered: 150,
            candidates_by_primitive: [("kv".to_string(), 100), ("json".to_string(), 50)]
                .into_iter()
                .collect(),
            index_used: true,
            truncated: false,
            mode: "hybrid".to_string(),
            expansion_used: false,
            rerank_used: false,
            expansion_model: None,
            rerank_model: None,
        },
    });
}

#[test]
fn test_output_search_results_with_model_names() {
    test_output_round_trip(Output::SearchResults {
        hits: vec![],
        stats: SearchStatsOutput {
            elapsed_ms: 5.0,
            candidates_considered: 50,
            candidates_by_primitive: std::collections::HashMap::new(),
            index_used: false,
            truncated: false,
            mode: "hybrid".to_string(),
            expansion_used: true,
            rerank_used: true,
            expansion_model: Some("qwen3:1.7b".to_string()),
            rerank_model: Some("qwen3:1.7b".to_string()),
        },
    });
}

#[test]
fn test_output_config_set_result() {
    test_output_round_trip(Output::ConfigSetResult {
        key: "provider".to_string(),
        new_value: "anthropic".to_string(),
    });
}

#[test]
fn test_output_sample_result() {
    test_output_round_trip(Output::SampleResult {
        total_count: 100,
        items: vec![
            SampleItem {
                key: "key1".to_string(),
                value: Value::String("hello".to_string()),
            },
            SampleItem {
                key: "key2".to_string(),
                value: Value::Int(42),
            },
        ],
    });
    test_output_round_trip(Output::SampleResult {
        total_count: 0,
        items: vec![],
    });
}

#[test]
fn test_output_graph_write_result() {
    test_output_round_trip(Output::GraphWriteResult {
        node_id: "node-1".to_string(),
        created: true,
    });
    test_output_round_trip(Output::GraphWriteResult {
        node_id: "node-1".to_string(),
        created: false,
    });
}

#[test]
fn test_output_graph_edge_write_result() {
    test_output_round_trip(Output::GraphEdgeWriteResult {
        src: "A".to_string(),
        dst: "B".to_string(),
        edge_type: "KNOWS".to_string(),
        created: true,
    });
    test_output_round_trip(Output::GraphEdgeWriteResult {
        src: "A".to_string(),
        dst: "B".to_string(),
        edge_type: "KNOWS".to_string(),
        created: false,
    });
}

// =============================================================================
// Complex Value Serialization Tests
// =============================================================================

#[test]
fn test_command_with_complex_value() {
    let complex_value = Value::object(
        [
            ("string".to_string(), Value::String("hello".to_string())),
            ("int".to_string(), Value::Int(42)),
            ("float".to_string(), Value::Float(3.125)),
            ("bool".to_string(), Value::Bool(true)),
            ("null".to_string(), Value::Null),
            (
                "array".to_string(),
                Value::array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            ),
            (
                "nested".to_string(),
                Value::object(
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
        filter_primitives: None,
        filter_spaces: None,
        as_of: None,
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
                value_b: Some(Value::String("hello".into())),
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
        ..strata_engine::StrataConfig::default()
    }));
}

#[test]
fn test_output_config_value_some() {
    test_output_round_trip(Output::ConfigValue(Some("anthropic".to_string())));
}

#[test]
fn test_output_config_value_none() {
    test_output_round_trip(Output::ConfigValue(None));
}

#[test]
fn test_command_configure_set() {
    test_command_round_trip(Command::ConfigureSet {
        key: "provider".to_string(),
        value: "anthropic".to_string(),
    });
}

#[test]
fn test_command_configure_get_key() {
    test_command_round_trip(Command::ConfigureGetKey {
        key: "provider".to_string(),
    });
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

// =============================================================================
// JSON Batch Get/Delete Serialization Tests
// =============================================================================

#[test]
fn test_command_json_batch_get() {
    test_command_round_trip(Command::JsonBatchGet {
        branch: Some(BranchId::from("default")),
        space: None,
        entries: vec![
            BatchJsonGetEntry {
                key: "doc1".to_string(),
                path: ".name".to_string(),
            },
            BatchJsonGetEntry {
                key: "doc2".to_string(),
                path: "".to_string(),
            },
        ],
    });
}

#[test]
fn test_command_json_batch_get_empty() {
    test_command_round_trip(Command::JsonBatchGet {
        branch: None,
        space: None,
        entries: vec![],
    });
}

#[test]
fn test_command_json_batch_delete() {
    test_command_round_trip(Command::JsonBatchDelete {
        branch: Some(BranchId::from("default")),
        space: Some("custom".to_string()),
        entries: vec![
            BatchJsonDeleteEntry {
                key: "doc1".to_string(),
                path: "".to_string(),
            },
            BatchJsonDeleteEntry {
                key: "doc2".to_string(),
                path: ".field".to_string(),
            },
        ],
    });
}

#[test]
fn test_output_batch_get_results() {
    // Found item
    test_output_round_trip(Output::BatchGetResults(vec![BatchGetItemResult {
        value: Some(Value::String("hello".to_string())),
        version: Some(3),
        timestamp: Some(1000),
        error: None,
    }]));

    // Not found item (all None, no error)
    test_output_round_trip(Output::BatchGetResults(vec![BatchGetItemResult {
        value: None,
        version: None,
        timestamp: None,
        error: None,
    }]));

    // Error item
    test_output_round_trip(Output::BatchGetResults(vec![BatchGetItemResult {
        value: None,
        version: None,
        timestamp: None,
        error: Some("invalid key".to_string()),
    }]));

    // Mixed results
    test_output_round_trip(Output::BatchGetResults(vec![
        BatchGetItemResult {
            value: Some(Value::Int(42)),
            version: Some(1),
            timestamp: Some(500),
            error: None,
        },
        BatchGetItemResult {
            value: None,
            version: None,
            timestamp: None,
            error: None,
        },
        BatchGetItemResult {
            value: None,
            version: None,
            timestamp: None,
            error: Some("bad key".to_string()),
        },
    ]));

    // Empty
    test_output_round_trip(Output::BatchGetResults(vec![]));
}

#[test]
fn test_output_batch_get_results_skip_serializing_none() {
    // Verify skip_serializing_if = "Option::is_none" works: None fields
    // should be absent in JSON (not "value": null)
    let result = BatchGetItemResult {
        value: None,
        version: None,
        timestamp: None,
        error: None,
    };
    let json = serde_json::to_string(&result).unwrap();
    assert_eq!(json, "{}");

    // Found item should only have non-None fields
    let result = BatchGetItemResult {
        value: Some(Value::Int(1)),
        version: Some(5),
        timestamp: None,
        error: None,
    };
    let json = serde_json::to_string(&result).unwrap();
    assert!(!json.contains("timestamp"));
    assert!(!json.contains("error"));
    assert!(json.contains("value"));
    assert!(json.contains("version"));
}

// =============================================================================
// Write Metadata Output Tests (#1443)
// =============================================================================

#[test]
fn test_output_write_result() {
    test_output_round_trip(Output::WriteResult {
        key: "my-key".to_string(),
        version: 42,
    });
}

#[test]
fn test_output_delete_result() {
    test_output_round_trip(Output::DeleteResult {
        key: "my-key".to_string(),
        deleted: true,
    });
    test_output_round_trip(Output::DeleteResult {
        key: "missing-key".to_string(),
        deleted: false,
    });
}

#[test]
fn test_output_event_append_result() {
    test_output_round_trip(Output::EventAppendResult {
        sequence: 100,
        event_type: "user.login".to_string(),
    });
}

#[test]
fn test_output_vector_write_result() {
    test_output_round_trip(Output::VectorWriteResult {
        collection: "embeddings".to_string(),
        key: "vec-1".to_string(),
        version: 7,
    });
}

#[test]
fn test_output_vector_delete_result() {
    test_output_round_trip(Output::VectorDeleteResult {
        collection: "embeddings".to_string(),
        key: "vec-1".to_string(),
        deleted: true,
    });
    test_output_round_trip(Output::VectorDeleteResult {
        collection: "embeddings".to_string(),
        key: "missing".to_string(),
        deleted: false,
    });
}

#[test]
fn test_output_state_cas_result_success() {
    test_output_round_trip(Output::StateCasResult {
        cell: "counter".to_string(),
        success: true,
        version: Some(6),
        current_value: None,
        current_version: None,
    });
}

#[test]
fn test_output_state_cas_result_conflict() {
    test_output_round_trip(Output::StateCasResult {
        cell: "counter".to_string(),
        success: false,
        version: None,
        current_value: Some(Value::Int(5)),
        current_version: Some(3),
    });
}

#[test]
fn test_output_state_cas_result_skip_none_fields() {
    // On success, current_value and current_version should be absent from JSON
    let output = Output::StateCasResult {
        cell: "c".to_string(),
        success: true,
        version: Some(1),
        current_value: None,
        current_version: None,
    };
    let json = serde_json::to_string(&output).unwrap();
    assert!(
        !json.contains("current_value"),
        "None fields should be skipped: {}",
        json
    );
    assert!(
        !json.contains("current_version"),
        "None fields should be skipped: {}",
        json
    );

    // On conflict, version should be absent
    let output = Output::StateCasResult {
        cell: "c".to_string(),
        success: false,
        version: None,
        current_value: Some(Value::Int(10)),
        current_version: Some(2),
    };
    let json = serde_json::to_string(&output).unwrap();
    assert!(
        json.contains("current_value"),
        "Conflict fields should be present: {}",
        json
    );
    assert!(
        json.contains("current_version"),
        "Conflict fields should be present: {}",
        json
    );
}

// =============================================================================
// Pagination Output Tests (#1444)
// =============================================================================

#[test]
fn test_output_keys_page_with_more() {
    test_output_round_trip(Output::KeysPage {
        keys: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        has_more: true,
        cursor: Some("c".to_string()),
    });
}

#[test]
fn test_output_keys_page_last_page() {
    test_output_round_trip(Output::KeysPage {
        keys: vec!["x".to_string(), "y".to_string()],
        has_more: false,
        cursor: None,
    });
}

#[test]
fn test_output_keys_page_empty() {
    test_output_round_trip(Output::KeysPage {
        keys: vec![],
        has_more: false,
        cursor: None,
    });
}

#[test]
fn test_output_keys_page_cursor_skipped_when_none() {
    let output = Output::KeysPage {
        keys: vec!["z".to_string()],
        has_more: false,
        cursor: None,
    };
    let json = serde_json::to_string(&output).unwrap();
    assert!(
        !json.contains("cursor"),
        "None cursor should be skipped: {}",
        json
    );
}

#[test]
fn test_output_json_list_result_with_has_more() {
    test_output_round_trip(Output::JsonListResult {
        keys: vec!["doc1".to_string(), "doc2".to_string()],
        has_more: true,
        cursor: Some("doc2".to_string()),
    });
    test_output_round_trip(Output::JsonListResult {
        keys: vec!["doc3".to_string()],
        has_more: false,
        cursor: None,
    });
}

#[test]
fn test_output_json_list_result_cursor_skipped_when_none() {
    let output = Output::JsonListResult {
        keys: vec!["doc1".to_string()],
        has_more: false,
        cursor: None,
    };
    let json = serde_json::to_string(&output).unwrap();
    assert!(
        !json.contains("cursor"),
        "None cursor should be skipped in JsonListResult: {}",
        json
    );
}

#[test]
fn test_output_write_result_edge_cases() {
    // Empty key
    test_output_round_trip(Output::WriteResult {
        key: "".to_string(),
        version: 0,
    });
    // Large version
    test_output_round_trip(Output::WriteResult {
        key: "k".to_string(),
        version: 999_999_999,
    });
}

#[test]
fn test_output_delete_result_edge_cases() {
    // Empty key with deleted=false
    test_output_round_trip(Output::DeleteResult {
        key: "".to_string(),
        deleted: false,
    });
}

#[test]
fn test_output_keys_page_large_page() {
    let keys: Vec<String> = (0..1000).map(|i| format!("key:{:04}", i)).collect();
    test_output_round_trip(Output::KeysPage {
        keys,
        has_more: true,
        cursor: Some("key:0999".to_string()),
    });
}

// =============================================================================
// Export Command/Output Tests
// =============================================================================

#[test]
fn test_command_db_export() {
    test_command_round_trip(Command::DbExport {
        branch: Some(BranchId::from("default")),
        space: None,
        primitive: ExportPrimitive::Kv,
        format: ExportFormat::Csv,
        prefix: Some("user:".to_string()),
        limit: Some(100),
        path: Some("/tmp/export.csv".to_string()),
    });
    test_command_round_trip(Command::DbExport {
        branch: None,
        space: None,
        primitive: ExportPrimitive::Json,
        format: ExportFormat::Json,
        prefix: None,
        limit: None,
        path: None,
    });
    test_command_round_trip(Command::DbExport {
        branch: None,
        space: None,
        primitive: ExportPrimitive::Events,
        format: ExportFormat::Jsonl,
        prefix: None,
        limit: Some(50),
        path: None,
    });
    test_command_round_trip(Command::DbExport {
        branch: None,
        space: None,
        primitive: ExportPrimitive::State,
        format: ExportFormat::Json,
        prefix: None,
        limit: None,
        path: None,
    });
}

#[test]
fn test_output_exported_inline() {
    test_output_round_trip(Output::Exported(ExportResult {
        row_count: 3,
        format: ExportFormat::Csv,
        primitive: ExportPrimitive::Kv,
        data: Some("key,value\na,1\nb,2\nc,3".to_string()),
        path: None,
        size_bytes: None,
    }));
}

#[test]
fn test_output_exported_to_file() {
    test_output_round_trip(Output::Exported(ExportResult {
        row_count: 100,
        format: ExportFormat::Json,
        primitive: ExportPrimitive::Json,
        data: None,
        path: Some("/tmp/export.json".to_string()),
        size_bytes: Some(4096),
    }));
}

#[test]
fn test_output_exported_optional_fields_skipped() {
    let output = Output::Exported(ExportResult {
        row_count: 0,
        format: ExportFormat::Jsonl,
        primitive: ExportPrimitive::Events,
        data: None,
        path: None,
        size_bytes: None,
    });
    let json = serde_json::to_string(&output).unwrap();
    assert!(
        !json.contains("\"data\""),
        "None data should be skipped: {}",
        json
    );
    assert!(
        !json.contains("\"path\""),
        "None path should be skipped: {}",
        json
    );
    assert!(
        !json.contains("\"size_bytes\""),
        "None size_bytes should be skipped: {}",
        json
    );
}
