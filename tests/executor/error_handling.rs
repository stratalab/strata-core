//! Error Handling Tests
//!
//! Tests for error conditions in the executor layer.

use crate::common::*;
use strata_core::Value;
use strata_executor::{BranchId, Command, DistanceMetric, Error, Output};

// ============================================================================
// Vector Errors
// ============================================================================

#[test]
fn vector_upsert_to_nonexistent_collection_behavior() {
    let executor = create_executor();

    let result = executor.execute(Command::VectorUpsert {
        branch: None,
        space: None,
        collection: "nonexistent".into(),
        key: "v1".into(),
        vector: vec![1.0, 0.0, 0.0, 0.0],
        metadata: None,
    });

    // Vector auto-create was removed (#923) - upsert to nonexistent collection
    // should now fail with CollectionNotFound
    match result {
        Err(Error::CollectionNotFound { collection, .. }) => {
            assert!(
                collection.contains("nonexistent"),
                "Error should reference 'nonexistent', got: {}",
                collection
            );
        }
        Err(_) => {} // Other errors also acceptable
        Ok(_) => panic!("Expected error for upsert to nonexistent collection"),
    }
}

#[test]
fn vector_query_in_nonexistent_collection_fails() {
    let executor = create_executor();

    let result = executor.execute(Command::VectorQuery {
        branch: None,
        space: None,
        collection: "nonexistent".into(),
        query: vec![1.0, 0.0, 0.0, 0.0],
        k: 10,
        filter: None,
        metric: None,
        as_of: None,
    });

    match result {
        Err(Error::CollectionNotFound { collection, .. }) => {
            assert!(
                collection.contains("nonexistent"),
                "Collection error should reference 'nonexistent', got: {}",
                collection
            );
        }
        other => panic!("Expected CollectionNotFound, got {:?}", other),
    }
}

#[test]
fn vector_wrong_dimension_fails() {
    let executor = create_executor();

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "dim4".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    // Try to insert wrong dimension
    let result = executor.execute(Command::VectorUpsert {
        branch: None,
        space: None,
        collection: "dim4".into(),
        key: "v1".into(),
        vector: vec![1.0, 0.0], // Only 2 dimensions
        metadata: None,
    });

    match result {
        Err(Error::DimensionMismatch {
            expected, actual, ..
        }) => {
            assert_eq!(expected, 4);
            assert_eq!(actual, 2);
        }
        other => panic!("Expected DimensionMismatch, got {:?}", other),
    }
}

#[test]
fn vector_delete_nonexistent_collection_behavior() {
    let executor = create_executor();

    let result = executor.execute(Command::VectorDeleteCollection {
        branch: None,
        space: None,
        collection: "nonexistent".into(),
    });

    // Check that deleting nonexistent collection returns false (not error)
    match result {
        Ok(strata_executor::Output::Bool(deleted)) => {
            assert!(!deleted, "Deleting nonexistent should return false");
        }
        Err(_) => {} // Also acceptable if it errors
        other => panic!("Unexpected output: {:?}", other),
    }
}

// ============================================================================
// Branch Errors
// ============================================================================

#[test]
fn branch_get_nonexistent_returns_none() {
    let executor = create_executor();

    let result = executor.execute(Command::BranchGet {
        branch: BranchId::from("nonexistent-branch"),
    });

    // BranchGet on nonexistent branch should return MaybeBranchInfo(None) or error
    match result {
        Ok(strata_executor::Output::MaybeBranchInfo(None)) => {}
        Err(_) => {} // Also acceptable
        _ => panic!("Unexpected output: {:?}", result),
    }
}

#[test]
fn branch_duplicate_id_fails() {
    let executor = create_executor();

    executor
        .execute(Command::BranchCreate {
            branch_id: Some("unique-branch".into()),
            metadata: None,
        })
        .unwrap();

    // Try to create another with same name
    let result = executor.execute(Command::BranchCreate {
        branch_id: Some("unique-branch".into()),
        metadata: None,
    });

    match result {
        Err(Error::BranchExists { branch }) => {
            assert!(
                branch.contains("unique-branch"),
                "BranchExists error should reference 'unique-branch', got: {}",
                branch
            );
        }
        Err(Error::InvalidInput { reason, .. }) => {
            assert!(
                reason.contains("unique-branch"),
                "InvalidInput error should reference 'unique-branch', got: {}",
                reason
            );
        }
        other => panic!("Expected BranchExists or InvalidInput, got {:?}", other),
    }
}

// ============================================================================
// Transaction Errors
// ============================================================================

#[test]
fn transaction_already_active_error() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });

    match result {
        Err(Error::TransactionAlreadyActive { .. }) => {}
        Err(e) => panic!("Expected TransactionAlreadyActive, got {:?}", e),
        Ok(_) => panic!("Expected error"),
    }
}

#[test]
fn transaction_not_active_commit_error() {
    let mut session = create_session();

    let result = session.execute(Command::TxnCommit);

    match result {
        Err(Error::TransactionNotActive { .. }) => {}
        Err(e) => panic!("Expected TransactionNotActive, got {:?}", e),
        Ok(_) => panic!("Expected error"),
    }
}

#[test]
fn transaction_not_active_rollback_error() {
    let mut session = create_session();

    let result = session.execute(Command::TxnRollback);

    match result {
        Err(Error::TransactionNotActive { .. }) => {}
        Err(e) => panic!("Expected TransactionNotActive, got {:?}", e),
        Ok(_) => panic!("Expected error"),
    }
}

// ============================================================================
// Event Errors
// ============================================================================

#[test]
fn event_append_non_object_fails() {
    let executor = create_executor();

    // Event payloads must be Objects
    let result = executor.execute(Command::EventAppend {
        branch: None,
        space: None,
        event_type: "stream".into(),
        payload: Value::Int(42), // Not an object
    });

    match result {
        Err(Error::InvalidInput { .. }) => {}
        other => panic!("Expected InvalidInput, got {:?}", other),
    }
}

// ============================================================================
// JSON Errors
// ============================================================================

#[test]
fn json_get_nonexistent_returns_none() {
    let executor = create_executor();

    let result = executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "nonexistent".into(),
            path: "$".into(),
            as_of: None,
        })
        .unwrap();

    match result {
        Output::MaybeVersioned(None) | Output::Maybe(None) => {}
        _ => panic!("Expected None for nonexistent document"),
    }
}

// ============================================================================
// Error Type Inspection
// ============================================================================

#[test]
fn error_is_serializable() {
    let error = Error::TransactionAlreadyActive { hint: None };
    let json = serde_json::to_string(&error).unwrap();
    assert!(!json.is_empty());
}

#[test]
fn error_display() {
    let error = Error::TransactionAlreadyActive { hint: None };
    let msg = error.to_string();
    assert!(!msg.is_empty());
}

// ============================================================================
// Hint Enrichment (Issue #1544)
// ============================================================================

#[test]
fn kv_not_found_enrichment_tested_in_unit_tests() {
    // KV get/getv return None for missing keys rather than KeyNotFound.
    // The KeyNotFound enrichment handles edge cases (e.g. storage-layer errors).
    // The fuzzy matching logic is verified in unit tests (suggest.rs).
    // Here we verify the basic contract: missing key returns None, not error.
    let executor = create_executor();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "greeting".into(),
            value: Value::String("hello".into()),
        })
        .unwrap();

    let result = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "greting".into(),
            as_of: None,
        })
        .unwrap();

    // Missing keys return None (not KeyNotFound error)
    match result {
        Output::MaybeVersioned(None) => {}
        other => panic!("Expected MaybeVersioned(None), got {:?}", other),
    }
}

#[test]
fn dimension_mismatch_includes_collection_context() {
    let executor = create_executor();

    // Create collection with 4 dimensions
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "embeddings".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    // Insert with wrong dimension
    let result = executor.execute(Command::VectorUpsert {
        branch: None,
        space: None,
        collection: "embeddings".into(),
        key: "v1".into(),
        vector: vec![1.0, 0.0],
        metadata: None,
    });

    match result {
        Err(Error::DimensionMismatch { hint, .. }) => {
            let hint = hint.expect("should have a hint");
            assert!(
                hint.contains("embeddings"),
                "hint should name the collection, got: {}",
                hint
            );
        }
        other => panic!("Expected DimensionMismatch with hint, got {:?}", other),
    }
}

#[test]
fn transaction_already_active_has_hint() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });

    match result {
        Err(Error::TransactionAlreadyActive { hint }) => {
            let hint = hint.expect("should have a hint");
            assert!(
                hint.contains("Commit or rollback"),
                "hint should guide user, got: {}",
                hint
            );
        }
        other => panic!(
            "Expected TransactionAlreadyActive with hint, got {:?}",
            other
        ),
    }
}

#[test]
fn transaction_not_active_has_hint() {
    let mut session = create_session();

    let result = session.execute(Command::TxnCommit);

    match result {
        Err(Error::TransactionNotActive { hint }) => {
            let hint = hint.expect("should have a hint");
            assert!(
                hint.contains("begin"),
                "hint should suggest 'begin', got: {}",
                hint
            );
        }
        other => panic!("Expected TransactionNotActive with hint, got {:?}", other),
    }
}

// ============================================================================
// Concurrent Error Scenarios
// ============================================================================

#[test]
fn concurrent_sessions_independent_transactions() {
    let db = create_db();

    let mut session1 = strata_executor::Session::new(db.clone());
    let mut session2 = strata_executor::Session::new(db.clone());

    // Both can start transactions
    session1
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    session2
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Both are in transaction
    assert!(session1.in_transaction());
    assert!(session2.in_transaction());

    // Both can commit
    session1.execute(Command::TxnCommit).unwrap();
    session2.execute(Command::TxnCommit).unwrap();

    assert!(!session1.in_transaction());
    assert!(!session2.in_transaction());
}
