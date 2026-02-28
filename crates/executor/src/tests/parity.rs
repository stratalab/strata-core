//! Parity tests: verify executor produces same results as direct engine calls.
//!
//! These tests ensure the Executor layer is a faithful proxy to the underlying
//! engine primitives, with no unexpected transformations or data loss.

use crate::bridge::{self, Primitives};
use crate::types::*;
use crate::Value;
use crate::{Command, Executor, Output};
use std::sync::Arc;
use strata_engine::Database;

/// Create a test executor with shared primitives for parity comparisons.
fn create_test_environment() -> (Executor, Arc<Primitives>) {
    let db = Database::cache().unwrap();
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

    // Both should succeed with a Version output
    match exec_result {
        Ok(Output::Version(v)) => {
            assert!(v > 0, "Write should return a positive version");
        }
        _ => panic!("Expected Version output"),
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

    // Should succeed and return true (existed)
    match result {
        Ok(Output::Bool(existed)) => assert!(existed),
        _ => panic!("Expected Bool output"),
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
        value: Value::Object(Box::new(
            [("name".to_string(), Value::String("Alice".into()))]
                .into_iter()
                .collect(),
        )),
    });

    // JsonSet returns Version
    match result {
        Ok(Output::Version(v)) => assert!(v > 0),
        other => panic!("Expected Version output, got {:?}", other),
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
        payload: Value::Object(Box::new(
            [("type".to_string(), Value::String("click".into()))]
                .into_iter()
                .collect(),
        )),
    });

    // Just verify it returns a Version
    match result1 {
        Ok(Output::Version(_seq)) => {}
        other => panic!("Expected Version output, got {:?}", other),
    }

    // Append via direct primitive
    let _seq2 = p
        .event
        .append(
            &branch_id,
            "default",
            "events",
            Value::Object(Box::new(
                [("type".to_string(), Value::String("scroll".into()))]
                    .into_iter()
                    .collect(),
            )),
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
// State Parity Tests
// =============================================================================

#[test]
fn test_state_set_get_parity() {
    let (executor, p) = create_test_environment();
    let branch_id = strata_core::types::BranchId::from_bytes([0u8; 16]);

    // Set via executor
    let result = executor.execute(Command::StateSet {
        branch: None,
        space: None,
        cell: "cell1".to_string(),
        value: Value::Int(100),
    });

    let counter1 = match result {
        Ok(Output::Version(c)) => c,
        _ => panic!("Expected Version output"),
    };

    // Get via direct primitive
    let direct_get = p.state.get(&branch_id, "default", "cell1").unwrap();
    assert!(direct_get.is_some());
    assert_eq!(direct_get.unwrap(), Value::Int(100));

    // Set via direct primitive
    let versioned2 = p
        .state
        .set(&branch_id, "default", "cell2", Value::Int(200))
        .unwrap();

    // Both should have counter 1 (first write to each cell)
    assert_eq!(counter1, 1);
    assert_eq!(bridge::extract_version(&versioned2), 1);

    // Get cell2 via executor
    let exec_get = executor.execute(Command::StateGet {
        branch: None,
        space: None,
        cell: "cell2".to_string(),
        as_of: None,
    });

    match exec_get {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            let v = vv.value;
            assert_eq!(v, Value::Int(200));
        }
        _ => panic!("Expected Maybe output"),
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
        strata_engine::DistanceMetric::Cosine,
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
    let search_result = executor.execute(Command::VectorSearch {
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
        metadata: Some(Value::Object(Box::new(
            [("name".to_string(), Value::String("Test".into()))]
                .into_iter()
                .collect(),
        ))),
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
