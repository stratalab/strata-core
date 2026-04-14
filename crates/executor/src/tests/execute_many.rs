//! Tests for the execute_many batch execution method.
//!
//! These tests verify that batch command execution works correctly,
//! including error handling and result ordering.

use crate::types::*;
use crate::Value;
use crate::{Command, Executor, Output};

/// Create a test executor with a cache in-memory database.
fn create_test_executor() -> Executor {
    use strata_engine::database::OpenSpec;
    use strata_engine::{Database, SearchSubsystem};

    let spec = OpenSpec::cache().with_subsystem(SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();
    Executor::new(db)
}

#[test]
fn test_execute_many_empty() {
    let executor = create_test_executor();
    let results = executor.execute_many(vec![]);
    assert!(results.is_empty());
}

#[test]
fn test_execute_many_single_command() {
    let executor = create_test_executor();
    let results = executor.execute_many(vec![Command::Ping]);

    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
    match &results[0] {
        Ok(Output::Pong { version }) => {
            assert!(!version.is_empty());
        }
        _ => panic!("Expected Pong output"),
    }
}

#[test]
fn test_execute_many_multiple_commands() {
    let executor = create_test_executor();
    let results = executor.execute_many(vec![Command::Ping, Command::Info, Command::Flush]);

    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert!(results[2].is_ok());
}

#[test]
fn test_execute_many_preserves_order() {
    let executor = create_test_executor();

    // Put three different values
    let put_results = executor.execute_many(vec![
        Command::KvPut {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "key1".to_string(),
            value: Value::Int(1),
        },
        Command::KvPut {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "key2".to_string(),
            value: Value::Int(2),
        },
        Command::KvPut {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "key3".to_string(),
            value: Value::Int(3),
        },
    ]);

    assert!(put_results.iter().all(|r| r.is_ok()));

    // Get them back in a specific order
    let get_results = executor.execute_many(vec![
        Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "key3".to_string(),
            as_of: None,
        },
        Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "key1".to_string(),
            as_of: None,
        },
        Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "key2".to_string(),
            as_of: None,
        },
    ]);

    assert_eq!(get_results.len(), 3);

    // Verify order is preserved
    match &get_results[0] {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            assert_eq!(vv.value, Value::Int(3));
        }
        _ => panic!("Expected Int(3) for key3"),
    }

    match &get_results[1] {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            assert_eq!(vv.value, Value::Int(1));
        }
        _ => panic!("Expected Int(1) for key1"),
    }

    match &get_results[2] {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            assert_eq!(vv.value, Value::Int(2));
        }
        _ => panic!("Expected Int(2) for key2"),
    }
}

#[test]
fn test_execute_many_continues_after_error() {
    let executor = create_test_executor();

    // Mix of valid and invalid commands - use an invalid key format to trigger error
    let results = executor.execute_many(vec![
        Command::Ping, // Should succeed
        Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "".to_string(), // Empty key should fail validation
            as_of: None,
        },
        Command::Ping, // Should succeed even after previous failure
    ]);

    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok(), "First Ping should succeed");
    assert!(results[1].is_err(), "Empty key should fail");
    assert!(
        results[2].is_ok(),
        "Second Ping should succeed despite previous error"
    );
}

#[test]
fn test_execute_many_mixed_operations() {
    let executor = create_test_executor();

    let results = executor.execute_many(vec![
        // Store a value
        Command::KvPut {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "counter".to_string(),
            value: Value::Int(10),
        },
        // Get it
        Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "counter".to_string(),
            as_of: None,
        },
        // Delete it
        Command::KvDelete {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "counter".to_string(),
        },
        // Get again (should be None)
        Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "counter".to_string(),
            as_of: None,
        },
    ]);

    assert_eq!(results.len(), 4);
    assert!(results[0].is_ok()); // Put

    // Get should return the value
    match &results[1] {
        Ok(Output::MaybeVersioned(Some(vv))) => assert_eq!(&vv.value, &Value::Int(10)),
        _ => panic!("Expected Maybe(Some(Int(10)))"),
    }

    // Delete should return DeleteResult with deleted=true (existed)
    match &results[2] {
        Ok(Output::DeleteResult { key, deleted }) => {
            assert_eq!(key, "counter");
            assert!(*deleted);
        }
        _ => panic!("Expected DeleteResult {{ deleted: true }}"),
    }

    // Get after delete should return None
    match &results[3] {
        Ok(Output::MaybeVersioned(None)) | Ok(Output::Maybe(None)) => {}
        _ => panic!("Expected Maybe(None) after delete"),
    }
}

#[test]
fn test_execute_many_all_database_commands() {
    let executor = create_test_executor();

    let results = executor.execute_many(vec![
        Command::Ping,
        Command::Info,
        Command::Flush,
        Command::Compact,
    ]);

    assert_eq!(results.len(), 4);

    // Verify specific outputs
    matches!(&results[0], Ok(Output::Pong { .. }));
    matches!(&results[1], Ok(Output::DatabaseInfo(_)));
    matches!(&results[2], Ok(Output::Unit));
    // Compact on ephemeral database is a no-op
    assert!(results[3].is_ok(), "Compact on ephemeral DB should succeed");
}

#[test]
fn test_execute_many_large_batch() {
    let executor = create_test_executor();

    // Create a batch of 100 commands
    let commands: Vec<Command> = (0..100)
        .map(|i| Command::KvPut {
            branch: Some(BranchId::from("default")),
            space: None,
            key: format!("key_{}", i),
            value: Value::Int(i),
        })
        .collect();

    let results = executor.execute_many(commands);

    assert_eq!(results.len(), 100);
    assert!(results.iter().all(|r| r.is_ok()));
}
