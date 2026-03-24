//! Determinism tests: verify same input produces same output.
//!
//! These tests ensure the Executor layer is deterministic - the same
//! Command executed on the same database state produces the same Output.

use crate::types::*;
use crate::Value;
use crate::{Command, Executor, Output};
use strata_engine::Database;

/// Create a test executor.
fn create_test_executor() -> Executor {
    let db = Database::cache().unwrap();
    Executor::new(db)
}

// =============================================================================
// Output Determinism Tests
// =============================================================================

#[test]
fn test_ping_determinism() {
    let executor = create_test_executor();

    // Execute same command multiple times
    let results: Vec<_> = (0..5).map(|_| executor.execute(Command::Ping)).collect();

    // All should produce Pong with same version
    let first = &results[0];
    for result in &results {
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", first),
            "Ping should produce deterministic output"
        );
    }
}

#[test]
fn test_info_determinism() {
    let executor = create_test_executor();

    // Execute same command multiple times
    let results: Vec<_> = (0..5).map(|_| executor.execute(Command::Info)).collect();

    // All should produce DatabaseInfo with same basic structure
    for result in &results {
        match result {
            Ok(Output::DatabaseInfo(info)) => {
                assert!(!info.version.is_empty());
            }
            _ => panic!("Expected DatabaseInfo output"),
        }
    }
}

#[test]
fn test_kv_get_nonexistent_determinism() {
    let executor = create_test_executor();

    // Getting a non-existent key should always return None
    let results: Vec<_> = (0..5)
        .map(|_| {
            executor.execute(Command::KvGet {
                branch: Some(BranchId::from("default")),
                space: None,
                key: "nonexistent-key".to_string(),
                as_of: None,
            })
        })
        .collect();

    for result in &results {
        match result {
            Ok(Output::MaybeVersioned(None)) | Ok(Output::Maybe(None)) => {}
            _ => panic!("Expected Maybe(None) for nonexistent key"),
        }
    }
}

// =============================================================================
// Read After Write Determinism Tests
// =============================================================================

#[test]
fn test_kv_write_read_determinism() {
    let executor = create_test_executor();

    // Write a value
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "test-key".to_string(),
            value: Value::String("test-value".into()),
        })
        .unwrap();

    // Read it multiple times - should always get same result
    let results: Vec<_> = (0..5)
        .map(|_| {
            executor.execute(Command::KvGet {
                branch: Some(BranchId::from("default")),
                space: None,
                key: "test-key".to_string(),
                as_of: None,
            })
        })
        .collect();

    for result in &results {
        match result {
            Ok(Output::MaybeVersioned(Some(vv))) => {
                assert_eq!(vv.value, Value::String("test-value".into()));
            }
            _ => panic!("Expected Maybe(Some) after write"),
        }
    }
}

// =============================================================================
// Error Determinism Tests
// =============================================================================

#[test]
fn test_kv_delete_nonexistent_determinism() {
    let executor = create_test_executor();

    // Deleting a non-existent key should always return false deterministically
    let results: Vec<_> = (0..5)
        .map(|_| {
            executor.execute(Command::KvDelete {
                branch: Some(BranchId::from("default")),
                space: None,
                key: "nonexistent-key".to_string(),
            })
        })
        .collect();

    // All should return DeleteResult with deleted=false
    for result in &results {
        match result {
            Ok(Output::DeleteResult { deleted: false, .. }) => {}
            _ => panic!("Expected DeleteResult {{ deleted: false }} for deleting nonexistent key"),
        }
    }
}

// =============================================================================
// Sequence Determinism Tests
// =============================================================================

#[test]
fn test_sequential_writes_determinism() {
    let executor = create_test_executor();

    // Write multiple values sequentially
    for i in 0..10 {
        let result = executor.execute(Command::KvPut {
            branch: Some(BranchId::from("default")),
            space: None,
            key: format!("key-{}", i),
            value: Value::Int(i),
        });

        match result {
            Ok(Output::WriteResult { key, version }) => {
                assert_eq!(key, format!("key-{}", i));
                assert!(version > 0, "Version should be positive");
            }
            _ => panic!("Expected WriteResult output for put"),
        }
    }

    // Read them back - each should have the value we wrote
    for i in 0..10 {
        let result = executor.execute(Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: format!("key-{}", i),
            as_of: None,
        });

        match result {
            Ok(Output::MaybeVersioned(Some(vv))) => {
                let v = vv.value;
                assert_eq!(v, Value::Int(i));
            }
            _ => panic!("Expected value at key-{}", i),
        }
    }
}

#[test]
fn test_kv_list_determinism() {
    let executor = create_test_executor();

    // Write some keys with a common prefix
    for i in 0..5 {
        executor
            .execute(Command::KvPut {
                branch: Some(BranchId::from("default")),
                space: None,
                key: format!("user:{}", i),
                value: Value::Int(i),
            })
            .unwrap();
    }

    // List keys multiple times - should get same results
    let results: Vec<_> = (0..5)
        .map(|_| {
            executor.execute(Command::KvList {
                branch: Some(BranchId::from("default")),
                space: None,
                prefix: Some("user:".to_string()),
                cursor: None,
                limit: None,
                as_of: None,
            })
        })
        .collect();

    // All results should have same keys
    let first = match &results[0] {
        Ok(Output::Keys(keys)) => keys,
        _ => panic!("Expected Keys output"),
    };

    assert_eq!(first.len(), 5, "Should have 5 keys");

    for result in &results {
        match result {
            Ok(Output::Keys(keys)) => {
                assert_eq!(keys.len(), first.len());
                for key in keys {
                    assert!(first.contains(key), "Key should be in first result");
                }
            }
            _ => panic!("Expected Keys output"),
        }
    }
}

// =============================================================================
// Vector Determinism Tests
// =============================================================================

#[test]
fn test_vector_search_determinism() {
    let executor = create_test_executor();

    // Create collection
    executor
        .execute(Command::VectorCreateCollection {
            branch: Some(BranchId::from("default")),
            space: None,
            collection: "embeddings".to_string(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    // Add some vectors
    for i in 0..5 {
        let mut vec = vec![0.0; 4];
        vec[i % 4] = 1.0;
        executor
            .execute(Command::VectorUpsert {
                branch: Some(BranchId::from("default")),
                space: None,
                collection: "embeddings".to_string(),
                key: format!("v{}", i),
                vector: vec,
                metadata: None,
            })
            .unwrap();
    }

    // Search multiple times with same query - should get same results
    let query = vec![1.0, 0.0, 0.0, 0.0];
    let results: Vec<_> = (0..5)
        .map(|_| {
            executor.execute(Command::VectorSearch {
                branch: Some(BranchId::from("default")),
                space: None,
                collection: "embeddings".to_string(),
                query: query.clone(),
                k: 3,
                filter: None,
                metric: None,
                as_of: None,
            })
        })
        .collect();

    // All results should have same matches in same order
    let first = match &results[0] {
        Ok(Output::VectorMatches(matches)) => matches,
        _ => panic!("Expected VectorMatches"),
    };

    for result in &results {
        match result {
            Ok(Output::VectorMatches(matches)) => {
                assert_eq!(matches.len(), first.len());
                for (a, b) in matches.iter().zip(first.iter()) {
                    assert_eq!(a.key, b.key, "Search results should be deterministic");
                }
            }
            _ => panic!("Expected VectorMatches"),
        }
    }
}

// =============================================================================
// Event Determinism Tests
// =============================================================================

#[test]
fn test_event_get_by_type_determinism() {
    let executor = create_test_executor();

    // Append some events
    for i in 0..5 {
        executor
            .execute(Command::EventAppend {
                branch: Some(BranchId::from("default")),
                space: None,
                event_type: "events".to_string(),
                payload: Value::object([("seq".to_string(), Value::Int(i))].into_iter().collect()),
            })
            .unwrap();
    }

    // ReadByType query multiple times - should get same results
    let results: Vec<_> = (0..5)
        .map(|_| {
            executor.execute(Command::EventGetByType {
                branch: Some(BranchId::from("default")),
                space: None,
                event_type: "events".to_string(),
                after_sequence: None,
                limit: None,
                as_of: None,
            })
        })
        .collect();

    // All should have same events
    for result in &results {
        match result {
            Ok(Output::VersionedValues(events)) => {
                assert_eq!(events.len(), 5);
            }
            _ => panic!("Expected VersionedValues"),
        }
    }
}

// =============================================================================
// JSON Determinism Tests
// =============================================================================

#[test]
fn test_json_get_determinism() {
    let executor = create_test_executor();

    // Set a JSON document
    executor
        .execute(Command::JsonSet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "doc".to_string(),
            path: "".to_string(),
            value: Value::object(
                [
                    ("name".to_string(), Value::String("Alice".into())),
                    ("age".to_string(), Value::Int(30)),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();

    // Get multiple times - should be deterministic
    let results: Vec<_> = (0..5)
        .map(|_| {
            executor.execute(Command::JsonGet {
                branch: Some(BranchId::from("default")),
                space: None,
                key: "doc".to_string(),
                path: ".name".to_string(),
                as_of: None,
            })
        })
        .collect();

    for result in &results {
        match result {
            Ok(Output::MaybeVersioned(Some(vv))) => {
                assert_eq!(vv.value, Value::String("Alice".into()));
            }
            _ => panic!("Expected Maybe(Some)"),
        }
    }
}
