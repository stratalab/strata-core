//! Parity tests: verify executor produces same results as direct engine calls.
//!
//! These tests ensure the Executor layer is a faithful proxy to the underlying
//! engine primitives, with no unexpected transformations or data loss.

use crate::bridge::Primitives;
use crate::types::*;
use crate::Value;
use crate::{Command, Executor, Output};
use std::sync::Arc;
use strata_engine::database::OpenSpec;
use strata_engine::{Database, SearchSubsystem};

/// Create a test executor with shared primitives for parity comparisons.
fn create_test_environment() -> (Executor, Arc<Primitives>) {
    let spec = OpenSpec::cache().with_subsystem(SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();
    let executor = Executor::new(db.clone());
    let primitives = Arc::new(Primitives::new(db));
    (executor, primitives)
}

// =============================================================================
// KV Parity Tests (MVP: 4 commands)
// =============================================================================

#[test]
fn test_kv_put_get_parity() {
    let (executor, p) = create_test_environment();
    let branch_id = strata_core::types::BranchId::from_bytes([0u8; 16]);

    // Direct primitive call to write key1
    let _direct_version =
        p.kv.put(
            &branch_id,
            "default",
            "key1",
            Value::String("direct".into()),
        )
        .unwrap();

    // Executor call to write key2
    let exec_result = executor.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "key2".to_string(),
        value: Value::String("executor".into()),
    });

    // Both should succeed with a WriteResult output
    match exec_result {
        Ok(Output::WriteResult { key, version }) => {
            assert_eq!(key, "key2");
            assert!(version > 0, "Write should return a positive version");
        }
        _ => panic!("Expected WriteResult output"),
    }

    // Now verify we can read back what was written via both methods
    let direct_value = p.kv.get(&branch_id, "default", "key1").unwrap();
    let exec_get = executor.execute(Command::KvGet {
        branch: None,
        space: None,
        key: "key2".to_string(),
        as_of: None,
    });

    assert_eq!(direct_value.unwrap(), Value::String("direct".into()));
    match exec_get {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            let v = vv.value;
            assert_eq!(v, Value::String("executor".into()));
        }
        _ => panic!("Expected Maybe output"),
    }

    // Cross-check: executor can read primitive write and vice versa
    let cross_read_exec = executor.execute(Command::KvGet {
        branch: None,
        space: None,
        key: "key1".to_string(),
        as_of: None,
    });
    match cross_read_exec {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            let v = vv.value;
            assert_eq!(v, Value::String("direct".into()));
        }
        _ => panic!("Cross-read failed"),
    }

    let cross_read_prim = p.kv.get(&branch_id, "default", "key2").unwrap();
    assert_eq!(cross_read_prim.unwrap(), Value::String("executor".into()));
}

#[test]
fn test_kv_delete_parity() {
    let (executor, p) = create_test_environment();
    let branch_id = strata_core::types::BranchId::from_bytes([0u8; 16]);

    // Set up data via primitive
    p.kv.put(&branch_id, "default", "to-delete", Value::Int(42))
        .unwrap();

    // Delete via executor
    let result = executor.execute(Command::KvDelete {
        branch: None,
        space: None,
        key: "to-delete".to_string(),
    });

    // Should succeed and return DeleteResult with deleted=true (existed)
    match result {
        Ok(Output::DeleteResult { key, deleted }) => {
            assert_eq!(key, "to-delete");
            assert!(deleted);
        }
        _ => panic!("Expected DeleteResult output"),
    }

    // Verify deleted via direct primitive call
    let check = p.kv.get(&branch_id, "default", "to-delete").unwrap();
    assert!(check.is_none(), "Key should be deleted");
}

#[test]
fn test_kv_list_parity() {
    let (executor, p) = create_test_environment();
    let branch_id = strata_core::types::BranchId::from_bytes([0u8; 16]);

    // Create keys via primitive
    p.kv.put(&branch_id, "default", "user:1", Value::Int(1))
        .unwrap();
    p.kv.put(&branch_id, "default", "user:2", Value::Int(2))
        .unwrap();
    p.kv.put(&branch_id, "default", "task:1", Value::Int(3))
        .unwrap();

    // List via executor with prefix filter
    let result = executor.execute(Command::KvList {
        branch: None,
        space: None,
        prefix: Some("user:".to_string()),
        cursor: None,
        limit: None,
        as_of: None,
    });

    match result {
        Ok(Output::Keys(keys)) => {
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&"user:1".to_string()));
            assert!(keys.contains(&"user:2".to_string()));
        }
        _ => panic!("Expected Keys output"),
    }

    // List all via executor
    let result_all = executor.execute(Command::KvList {
        branch: None,
        space: None,
        prefix: None,
        cursor: None,
        limit: None,
        as_of: None,
    });

    match result_all {
        Ok(Output::Keys(keys)) => {
            assert_eq!(keys.len(), 3);
        }
        _ => panic!("Expected Keys output"),
    }
}

// =============================================================================
// JSON Parity Tests
// =============================================================================

#[test]
fn test_json_set_get_parity() {
    let (executor, _p) = create_test_environment();

    // Set via executor - use root path (empty string means root)
    let result = executor.execute(Command::JsonSet {
        branch: None,
        space: None,
        key: "doc1".to_string(),
        path: "".to_string(), // Root path
        value: Value::object(
            [("name".to_string(), Value::String("Alice".into()))]
                .into_iter()
                .collect(),
        ),
    });

    // JsonSet returns WriteResult
    match result {
        Ok(Output::WriteResult { key, version }) => {
            assert_eq!(key, "doc1");
            assert!(version > 0);
        }
        other => panic!("Expected WriteResult output, got {:?}", other),
    }

    // Get via executor - JsonGet returns MaybeVersioned
    let exec_get = executor.execute(Command::JsonGet {
        branch: None,
        space: None,
        key: "doc1".to_string(),
        path: ".name".to_string(),
        as_of: None,
    });

    match exec_get {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            let v = vv.value;
            assert_eq!(v, Value::String("Alice".into()));
        }
        other => panic!("Expected Maybe output, got {:?}", other),
    }
}

// =============================================================================
// Event Parity Tests
// =============================================================================

#[test]
fn test_event_append_get_by_type_parity() {
    let (executor, p) = create_test_environment();
    let branch_id = strata_core::types::BranchId::from_bytes([0u8; 16]);

    // Append via executor - EventAppend returns Version
    let result1 = executor.execute(Command::EventAppend {
        branch: None,
        space: None,
        event_type: "events".to_string(),
        payload: Value::object(
            [("type".to_string(), Value::String("click".into()))]
                .into_iter()
                .collect(),
        ),
    });

    // Just verify it returns an EventAppendResult
    match result1 {
        Ok(Output::EventAppendResult { event_type, .. }) => {
            assert_eq!(event_type, "events");
        }
        other => panic!("Expected EventAppendResult output, got {:?}", other),
    }

    // Append via direct primitive
    let _seq2 = p
        .event
        .append(
            &branch_id,
            "default",
            "events",
            Value::object(
                [("type".to_string(), Value::String("scroll".into()))]
                    .into_iter()
                    .collect(),
            ),
        )
        .unwrap();

    // ReadByType query via executor
    let read_result = executor.execute(Command::EventGetByType {
        branch: None,
        space: None,
        event_type: "events".to_string(),
        after_sequence: None,
        limit: None,
        as_of: None,
    });

    match read_result {
        Ok(Output::VersionedValues(events)) => {
            assert_eq!(events.len(), 2);
        }
        other => panic!("Expected VersionedValues output, got {:?}", other),
    }
}

// =============================================================================
// Vector Parity Tests
// =============================================================================

#[test]
fn test_vector_create_collection_parity() {
    let (executor, p) = create_test_environment();
    let branch_id = strata_core::types::BranchId::from_bytes([0u8; 16]);

    // Create collection via executor
    let result = executor.execute(Command::VectorCreateCollection {
        branch: None,
        space: None,
        collection: "embeddings".to_string(),
        dimension: 4,
        metric: DistanceMetric::Cosine,
    });

    assert!(result.is_ok());

    // Verify via direct primitive using list_collections (get_collection is internal)
    let collections = p.vector.list_collections(branch_id, "default").unwrap();
    let info = collections.iter().find(|c| c.name == "embeddings");
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.config.dimension, 4);
}

#[test]
fn test_vector_upsert_search_parity() {
    let (executor, p) = create_test_environment();
    let branch_id = strata_core::types::BranchId::from_bytes([0u8; 16]);

    // Create collection first via primitive
    let config = strata_core::primitives::vector::VectorConfig::new(
        4,
        strata_vector::DistanceMetric::Cosine,
    )
    .unwrap();
    p.vector
        .create_collection(branch_id, "default", "vecs", config)
        .unwrap();

    // Upsert via executor
    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "vecs".to_string(),
            key: "v1".to_string(),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            metadata: None,
        })
        .unwrap();

    // Upsert via direct primitive
    p.vector
        .insert(
            branch_id,
            "default",
            "vecs",
            "v2",
            &[0.0, 1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    // Search via executor
    let search_result = executor.execute(Command::VectorQuery {
        branch: None,
        space: None,
        collection: "vecs".to_string(),
        query: vec![1.0, 0.0, 0.0, 0.0],
        k: 10,
        filter: None,
        metric: None,
        as_of: None,
    });

    match search_result {
        Ok(Output::VectorMatches(matches)) => {
            assert_eq!(matches.len(), 2);
            // v1 should be the closest match (exact match)
            assert_eq!(matches[0].key, "v1");
        }
        _ => panic!("Expected VectorMatches output"),
    }
}

// =============================================================================
// Branch Parity Tests
// =============================================================================

#[test]
fn test_branch_create_get_parity() {
    let (executor, _p) = create_test_environment();

    // Create branch via executor with a UUID
    let result = executor.execute(Command::BranchCreate {
        branch_id: Some("550e8400-e29b-41d4-a716-446655440001".to_string()),
        metadata: Some(Value::object(
            [("name".to_string(), Value::String("Test".into()))]
                .into_iter()
                .collect(),
        )),
    });

    match result {
        Ok(Output::BranchWithVersion { info, .. }) => {
            assert_eq!(info.id.as_str(), "550e8400-e29b-41d4-a716-446655440001");
        }
        other => panic!("Expected BranchWithVersion output, got {:?}", other),
    }

    // List branches via executor
    let list_result = executor.execute(Command::BranchList {
        state: None,
        limit: None,
        offset: None,
    });

    match list_result {
        Ok(Output::BranchInfoList(branches)) => {
            assert!(!branches.is_empty(), "Expected at least 1 branch");
        }
        other => panic!("Expected BranchInfoList output, got {:?}", other),
    }
}

// =============================================================================
// Database Parity Tests
// =============================================================================

#[test]
fn test_ping_parity() {
    let (executor, _p) = create_test_environment();

    let result = executor.execute(Command::Ping);

    match result {
        Ok(Output::Pong { version }) => {
            assert!(!version.is_empty());
        }
        _ => panic!("Expected Pong output"),
    }
}

#[test]
fn test_info_parity() {
    let (executor, _p) = create_test_environment();

    let result = executor.execute(Command::Info);

    match result {
        Ok(Output::DatabaseInfo(info)) => {
            assert!(!info.version.is_empty());
        }
        _ => panic!("Expected DatabaseInfo output"),
    }
}

#[test]
fn test_flush_compact_parity() {
    let (executor, p) = create_test_environment();

    // Flush should delegate to db.flush() and succeed (no-op on ephemeral)
    let flush_result = executor.execute(Command::Flush);
    assert!(flush_result.is_ok());

    // On ephemeral databases, both checkpoint and compact are no-ops (Ok)
    let engine_result = p.db.compact();
    let executor_result = executor.execute(Command::Compact);
    assert_eq!(
        engine_result.is_ok(),
        executor_result.is_ok(),
        "Executor Compact result must match engine compact() result"
    );
}

// =============================================================================
// Cross-category Integration Tests
// =============================================================================

#[test]
fn test_branch_isolation_parity() {
    let (executor, _p) = create_test_environment();

    // Create two branches with valid UUIDs
    executor
        .execute(Command::BranchCreate {
            branch_id: Some("550e8400-e29b-41d4-a716-446655440003".to_string()),
            metadata: None,
        })
        .unwrap();

    executor
        .execute(Command::BranchCreate {
            branch_id: Some("550e8400-e29b-41d4-a716-446655440004".to_string()),
            metadata: None,
        })
        .unwrap();

    // Write to branch-a
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::from("550e8400-e29b-41d4-a716-446655440003")),
            space: None,
            key: "shared-key".to_string(),
            value: Value::String("from-a".into()),
        })
        .unwrap();

    // Write to branch-b
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::from("550e8400-e29b-41d4-a716-446655440004")),
            space: None,
            key: "shared-key".to_string(),
            value: Value::String("from-b".into()),
        })
        .unwrap();

    // Read from branch-a
    let result_a = executor.execute(Command::KvGet {
        branch: Some(BranchId::from("550e8400-e29b-41d4-a716-446655440003")),
        space: None,
        key: "shared-key".to_string(),
        as_of: None,
    });

    match result_a {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            let v = vv.value;
            assert_eq!(v, Value::String("from-a".into()));
        }
        _ => panic!("Expected value from branch-a"),
    }

    // Read from branch-b
    let result_b = executor.execute(Command::KvGet {
        branch: Some(BranchId::from("550e8400-e29b-41d4-a716-446655440004")),
        space: None,
        key: "shared-key".to_string(),
        as_of: None,
    });

    match result_b {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            let v = vv.value;
            assert_eq!(v, Value::String("from-b".into()));
        }
        _ => panic!("Expected value from branch-b"),
    }
}

// =============================================================================
// JSON Batch Get Tests
// =============================================================================

#[test]
fn test_json_batch_get_basic() {
    let (executor, _p) = create_test_environment();

    // Set 3 docs
    for (key, name) in &[("d1", "Alice"), ("d2", "Bob"), ("d3", "Carol")] {
        let r = executor.execute(Command::JsonSet {
            branch: None,
            space: None,
            key: key.to_string(),
            path: "".to_string(),
            value: Value::object(
                [("name".to_string(), Value::String((*name).into()))]
                    .into_iter()
                    .collect(),
            ),
        });
        assert!(r.is_ok(), "JsonSet failed for {}", key);
    }

    // Batch get all 3
    let result = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry {
                    key: "d1".to_string(),
                    path: ".name".to_string(),
                },
                BatchJsonGetEntry {
                    key: "d2".to_string(),
                    path: ".name".to_string(),
                },
                BatchJsonGetEntry {
                    key: "d3".to_string(),
                    path: ".name".to_string(),
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchGetResults(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].value, Some(Value::String("Alice".into())));
            assert_eq!(results[1].value, Some(Value::String("Bob".into())));
            assert_eq!(results[2].value, Some(Value::String("Carol".into())));
            // All should have versions and timestamps
            for r in &results {
                assert!(r.version.is_some());
                assert!(r.timestamp.is_some());
                assert!(r.error.is_none());
            }
        }
        other => panic!("Expected BatchGetResults, got {:?}", other),
    }
}

#[test]
fn test_json_batch_get_mixed() {
    let (executor, _p) = create_test_environment();

    // Set 2 docs, leave 1 missing
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "exists1".to_string(),
            path: "".to_string(),
            value: Value::Int(10),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "exists2".to_string(),
            path: "".to_string(),
            value: Value::Int(20),
        })
        .unwrap();

    let result = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry {
                    key: "exists1".to_string(),
                    path: "".to_string(),
                },
                BatchJsonGetEntry {
                    key: "missing".to_string(),
                    path: "".to_string(),
                },
                BatchJsonGetEntry {
                    key: "exists2".to_string(),
                    path: "".to_string(),
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchGetResults(results) => {
            assert_eq!(results.len(), 3);
            // First: found
            assert_eq!(results[0].value, Some(Value::Int(10)));
            assert!(results[0].error.is_none());
            // Second: not found
            assert!(results[1].value.is_none());
            assert!(results[1].version.is_none());
            assert!(results[1].error.is_none());
            // Third: found
            assert_eq!(results[2].value, Some(Value::Int(20)));
            assert!(results[2].error.is_none());
        }
        other => panic!("Expected BatchGetResults, got {:?}", other),
    }
}

#[test]
fn test_json_batch_get_invalid_key() {
    let (executor, _p) = create_test_environment();

    // Set a valid doc so we can verify errors don't abort the batch
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "valid-key".to_string(),
            path: "".to_string(),
            value: Value::Int(42),
        })
        .unwrap();

    let result = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry {
                    key: "".to_string(), // invalid empty key
                    path: "".to_string(),
                },
                BatchJsonGetEntry {
                    key: "valid-key".to_string(),
                    path: "".to_string(),
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchGetResults(results) => {
            assert_eq!(results.len(), 2);
            // First: error for invalid key
            assert!(results[0].error.is_some());
            assert!(results[0].value.is_none());
            // Second: found despite first entry having an error
            assert!(results[1].error.is_none());
            assert_eq!(results[1].value, Some(Value::Int(42)));
        }
        other => panic!("Expected BatchGetResults, got {:?}", other),
    }
}

#[test]
fn test_json_batch_get_invalid_path() {
    let (executor, _p) = create_test_environment();

    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "doc".to_string(),
            path: "".to_string(),
            value: Value::Int(1),
        })
        .unwrap();

    let result = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry {
                    key: "doc".to_string(),
                    path: "[invalid".to_string(), // bad path
                },
                BatchJsonGetEntry {
                    key: "doc".to_string(),
                    path: "".to_string(), // valid
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchGetResults(results) => {
            assert_eq!(results.len(), 2);
            // First: error for invalid path, doesn't abort batch
            assert!(results[0].error.is_some());
            assert!(results[0].value.is_none());
            // Second: succeeds
            assert!(results[1].error.is_none());
            assert_eq!(results[1].value, Some(Value::Int(1)));
        }
        other => panic!("Expected BatchGetResults, got {:?}", other),
    }
}

#[test]
fn test_json_batch_get_subpath() {
    let (executor, _p) = create_test_environment();

    // Set a nested doc
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "deep".to_string(),
            path: "".to_string(),
            value: Value::object(
                [(
                    "outer".to_string(),
                    Value::object(
                        [("inner".to_string(), Value::String("found".into()))]
                            .into_iter()
                            .collect(),
                    ),
                )]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();

    let result = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry {
                    key: "deep".to_string(),
                    path: ".outer.inner".to_string(),
                },
                BatchJsonGetEntry {
                    key: "deep".to_string(),
                    path: ".outer".to_string(),
                },
                BatchJsonGetEntry {
                    key: "deep".to_string(),
                    path: ".nonexistent".to_string(),
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchGetResults(results) => {
            assert_eq!(results.len(), 3);
            // Deep path
            assert_eq!(results[0].value, Some(Value::String("found".into())));
            // Intermediate path returns sub-object
            assert!(results[1].value.is_some());
            // Non-existent path — not found (no error, just None)
            assert!(results[2].value.is_none());
            assert!(results[2].error.is_none());
        }
        other => panic!("Expected BatchGetResults, got {:?}", other),
    }
}

#[test]
fn test_json_batch_get_empty() {
    let (executor, _p) = create_test_environment();

    let result = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![],
        })
        .unwrap();

    match result {
        Output::BatchGetResults(results) => {
            assert!(results.is_empty());
        }
        other => panic!("Expected empty BatchGetResults, got {:?}", other),
    }
}

// =============================================================================
// JSON Batch Delete Tests
// =============================================================================

#[test]
fn test_json_batch_delete_basic() {
    let (executor, _p) = create_test_environment();

    // Set 3 docs
    for key in &["del1", "del2", "del3"] {
        executor
            .execute(Command::JsonSet {
                branch: None,
                space: None,
                key: key.to_string(),
                path: "".to_string(),
                value: Value::Int(42),
            })
            .unwrap();
    }

    // Batch delete all 3
    let result = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry {
                    key: "del1".to_string(),
                    path: "".to_string(),
                },
                BatchJsonDeleteEntry {
                    key: "del2".to_string(),
                    path: "".to_string(),
                },
                BatchJsonDeleteEntry {
                    key: "del3".to_string(),
                    path: "".to_string(),
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 3);
            for r in &results {
                assert_eq!(r.version, Some(1)); // 1 = deleted
                assert!(r.error.is_none());
            }
        }
        other => panic!("Expected BatchResults, got {:?}", other),
    }

    // Verify docs are gone
    let get_result = executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "del1".to_string(),
            path: "".to_string(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(get_result, Output::MaybeVersioned(None)));
}

#[test]
fn test_json_batch_delete_mixed_paths() {
    let (executor, _p) = create_test_environment();

    // Set a doc with nested structure
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "nested".to_string(),
            path: "".to_string(),
            value: Value::object(
                [
                    ("a".to_string(), Value::Int(1)),
                    ("b".to_string(), Value::Int(2)),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();

    // Set another doc for root delete
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "whole".to_string(),
            path: "".to_string(),
            value: Value::Int(99),
        })
        .unwrap();

    // Mix root and non-root deletes
    let result = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry {
                    key: "nested".to_string(),
                    path: ".a".to_string(), // non-root
                },
                BatchJsonDeleteEntry {
                    key: "whole".to_string(),
                    path: "".to_string(), // root
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].version, Some(1)); // path deleted
            assert_eq!(results[1].version, Some(1)); // doc destroyed
        }
        other => panic!("Expected BatchResults, got {:?}", other),
    }

    // Verify "nested" still exists but without .a
    let get = executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "nested".to_string(),
            path: ".a".to_string(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(get, Output::MaybeVersioned(None)));

    // .b should still exist
    let get_b = executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "nested".to_string(),
            path: ".b".to_string(),
            as_of: None,
        })
        .unwrap();
    match get_b {
        Output::MaybeVersioned(Some(vv)) => assert_eq!(vv.value, Value::Int(2)),
        other => panic!("Expected .b=2, got {:?}", other),
    }

    // "whole" should be gone
    let get_w = executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "whole".to_string(),
            path: "".to_string(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(get_w, Output::MaybeVersioned(None)));
}

#[test]
fn test_json_batch_delete_nonexistent() {
    let (executor, _p) = create_test_environment();

    let result = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry {
                    key: "nope1".to_string(),
                    path: "".to_string(),
                },
                BatchJsonDeleteEntry {
                    key: "nope2".to_string(),
                    path: "".to_string(),
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 2);
            for r in &results {
                assert_eq!(r.version, Some(0)); // 0 = not found
                assert!(r.error.is_none());
            }
        }
        other => panic!("Expected BatchResults, got {:?}", other),
    }
}

#[test]
fn test_json_batch_delete_empty() {
    let (executor, _p) = create_test_environment();

    let result = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![],
        })
        .unwrap();

    match result {
        Output::BatchResults(results) => {
            assert!(results.is_empty());
        }
        other => panic!("Expected empty BatchResults, got {:?}", other),
    }
}

#[test]
fn test_json_batch_delete_invalid_key() {
    let (executor, _p) = create_test_environment();

    // Set a valid doc so we can verify errors don't abort the batch
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "valid".to_string(),
            path: "".to_string(),
            value: Value::Int(1),
        })
        .unwrap();

    let result = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry {
                    key: "".to_string(), // invalid empty key
                    path: "".to_string(),
                },
                BatchJsonDeleteEntry {
                    key: "valid".to_string(),
                    path: "".to_string(),
                },
            ],
        })
        .unwrap();

    match result {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 2);
            // First: error for invalid key
            assert!(results[0].error.is_some());
            assert!(results[0].version.is_none());
            // Second: successfully deleted despite first entry error
            assert_eq!(results[1].version, Some(1));
            assert!(results[1].error.is_none());
        }
        other => panic!("Expected BatchResults, got {:?}", other),
    }

    // Verify the valid doc was actually deleted
    let get = executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "valid".to_string(),
            path: "".to_string(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(get, Output::MaybeVersioned(None)));
}

#[test]
fn test_json_batch_delete_nonroot_nonexistent_doc() {
    let (executor, _p) = create_test_environment();

    // Delete at non-root path for a document that doesn't exist
    let result = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![BatchJsonDeleteEntry {
                key: "ghost".to_string(),
                path: ".field".to_string(),
            }],
        })
        .unwrap();

    match result {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 1);
            // Non-root delete on nonexistent doc should return 0 (not found),
            // matching single json_delete behavior
            assert_eq!(results[0].version, Some(0));
            assert!(results[0].error.is_none());
        }
        other => panic!("Expected BatchResults, got {:?}", other),
    }
}
