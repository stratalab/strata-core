//! Session Transaction Tests
//!
//! Tests for the Session's stateful transaction support, including:
//! - Transaction lifecycle (begin, commit, rollback)
//! - Read-your-writes semantics within a transaction
//! - Transaction isolation between sessions
//! - Error handling for invalid transaction states

use crate::common::*;
use strata_core::Value;
use strata_executor::{BranchId, Command, DistanceMetric, Output, Session, TxnStatus};

// ============================================================================
// Transaction Lifecycle
// ============================================================================

#[test]
fn session_starts_without_transaction() {
    let mut session = create_session();

    assert!(!session.in_transaction());

    let output = session.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(output, Output::Bool(false)));
}

#[test]
fn begin_starts_transaction() {
    let mut session = create_session();

    let output = session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    assert!(matches!(output, Output::TxnBegun));
    assert!(session.in_transaction());

    let output = session.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(output, Output::Bool(true)));
}

#[test]
fn commit_ends_transaction() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    assert!(session.in_transaction());

    let output = session.execute(Command::TxnCommit).unwrap();

    match output {
        Output::TxnCommitted { version: _ } => {}
        _ => panic!("Expected TxnCommitted output"),
    }

    assert!(!session.in_transaction());
}

#[test]
fn rollback_ends_transaction() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    assert!(session.in_transaction());

    let output = session.execute(Command::TxnRollback).unwrap();

    assert!(matches!(output, Output::TxnAborted));
    assert!(!session.in_transaction());
}

#[test]
fn txn_info_returns_info_when_active() {
    let mut session = create_session();

    // No transaction - should return None
    let output = session.execute(Command::TxnInfo).unwrap();
    assert!(matches!(output, Output::TxnInfo(None)));

    // Start transaction
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Should return Some with info
    let output = session.execute(Command::TxnInfo).unwrap();
    match output {
        Output::TxnInfo(Some(info)) => {
            assert!(!info.id.is_empty());
            assert!(matches!(info.status, TxnStatus::Active));
        }
        _ => panic!("Expected TxnInfo(Some) when transaction active"),
    }
}

// ============================================================================
// Read-Your-Writes Within Transaction
// ============================================================================

#[test]
fn read_your_writes_kv() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Write within transaction
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn_key".into(),
            value: Value::Int(42),
        })
        .unwrap();

    // Read within same transaction should see the write
    let output = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "txn_key".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            assert_eq!(vv.value, Value::Int(42));
        }
        Output::Maybe(Some(val)) => {
            assert_eq!(val, Value::Int(42));
        }
        _ => panic!("Expected to read our own write"),
    }

    session.execute(Command::TxnCommit).unwrap();
}

#[test]
fn read_your_writes_event() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Note: EventAppend in transaction uses event_type
    session
        .execute(Command::EventAppend {
            branch: None,
            space: None,
            event_type: "default".into(),
            payload: event_payload("data", Value::Int(1)),
        })
        .unwrap();

    // EventLen counts events in the log
    let output = session
        .execute(Command::EventLen {
            branch: None,
            space: None,
        })
        .unwrap();

    match output {
        Output::Uint(len) => assert_eq!(len, 1, "Expected exactly 1 event, got {}", len),
        _ => panic!("Expected Uint output"),
    }

    session.execute(Command::TxnCommit).unwrap();
}

// ============================================================================
// Rollback Discards Writes
// ============================================================================

#[test]
fn rollback_discards_kv_writes() {
    let db = create_db();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "rollback_test".into(),
            value: Value::Int(100),
        })
        .unwrap();

    // Rollback instead of commit
    session.execute(Command::TxnRollback).unwrap();

    // Read with a new session - should not find the value
    let executor = strata_executor::Executor::new(db);
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "rollback_test".into(),
            as_of: None,
        })
        .unwrap();

    assert!(matches!(
        output,
        Output::MaybeVersioned(None) | Output::Maybe(None)
    ));
}

// ============================================================================
// Commit Makes Writes Visible
// ============================================================================

#[test]
fn commit_makes_kv_writes_visible() {
    let db = create_db();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "commit_test".into(),
            value: Value::Int(999),
        })
        .unwrap();

    session.execute(Command::TxnCommit).unwrap();

    // Read with a new executor - should find the value
    let executor = strata_executor::Executor::new(db);
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "commit_test".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::Int(999));
        }
        _ => panic!("Expected committed value to be visible"),
    }
}

// ============================================================================
// Transaction Error States
// ============================================================================

#[test]
fn begin_while_active_fails() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Second begin should fail
    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });

    assert!(result.is_err());
}

#[test]
fn commit_without_transaction_fails() {
    let mut session = create_session();

    let result = session.execute(Command::TxnCommit);

    assert!(result.is_err());
}

#[test]
fn rollback_without_transaction_fails() {
    let mut session = create_session();

    let result = session.execute(Command::TxnRollback);

    assert!(result.is_err());
}

// ============================================================================
// Non-Transactional Commands Still Work
// ============================================================================

#[test]
fn branch_create_blocked_inside_transaction() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Branch create/delete operations are now blocked inside transactions
    let result = session.execute(Command::BranchCreate {
        branch_id: Some("txn-bypass-test".into()),
        metadata: None,
    });

    assert!(
        result.is_err(),
        "Branch create should be blocked inside a transaction"
    );

    session.execute(Command::TxnRollback).unwrap();

    // Branch read commands should still work outside transaction
    let output = session
        .execute(Command::BranchGet {
            branch: BranchId::from("txn-bypass-test"),
        })
        .unwrap();

    // Branch was never created (blocked), so should not exist
    assert!(matches!(output, Output::MaybeBranchInfo(None)));
}

#[test]
fn vector_write_blocked_inside_transaction() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Vector write operations are now blocked inside transactions
    let result = session.execute(Command::VectorCreateCollection {
        branch: None,
        space: None,
        collection: "txn_coll".into(),
        dimension: 4,
        metric: DistanceMetric::Cosine,
    });

    assert!(
        result.is_err(),
        "Vector create collection should be blocked inside a transaction"
    );

    session.execute(Command::TxnRollback).unwrap();

    // After rollback, collection should not exist (was blocked)
    let output = session
        .execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
        .unwrap();

    match output {
        Output::VectorCollectionList(infos) => {
            assert!(
                !infos.iter().any(|c| c.name == "txn_coll"),
                "Collection should not exist since creation was blocked"
            );
        }
        _ => panic!("Expected VectorCollectionList output"),
    }
}

#[test]
fn db_commands_work_in_transaction() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // These should all work
    session.execute(Command::Ping).unwrap();
    session.execute(Command::Info).unwrap();

    session.execute(Command::TxnCommit).unwrap();
}

// ============================================================================
// Multiple Operations in Transaction
// ============================================================================

#[test]
fn multiple_kv_operations_in_transaction() {
    let db = create_db();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Multiple puts
    for i in 0..10 {
        session
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: format!("key_{}", i),
                value: Value::Int(i),
            })
            .unwrap();
    }

    // All should be readable within transaction
    for i in 0..10 {
        let output = session
            .execute(Command::KvGet {
                branch: None,
                space: None,
                key: format!("key_{}", i),
                as_of: None,
            })
            .unwrap();

        match output {
            Output::MaybeVersioned(Some(vv)) => {
                assert_eq!(vv.value, Value::Int(i));
            }
            Output::Maybe(Some(val)) => {
                // In-transaction reads may return Maybe instead of MaybeVersioned
                assert_eq!(val, Value::Int(i));
            }
            _ => panic!("Expected to read key_{}", i),
        }
    }

    session.execute(Command::TxnCommit).unwrap();

    // Verify all visible after commit
    let executor = strata_executor::Executor::new(db);
    for i in 0..10 {
        let output = executor
            .execute(Command::KvGet {
                branch: None,
                space: None,
                key: format!("key_{}", i),
                as_of: None,
            })
            .unwrap();

        assert!(matches!(
            output,
            Output::MaybeVersioned(Some(_)) | Output::Maybe(Some(_))
        ));
    }
}

#[test]
fn cross_primitive_transaction() {
    let db = create_db();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // KV - fully supported in transactions
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "kv_key".into(),
            value: Value::Int(1),
        })
        .unwrap();

    // Event - append supported in transactions
    session
        .execute(Command::EventAppend {
            branch: None,
            space: None,
            event_type: "default".into(),
            payload: event_payload("n", Value::Int(3)),
        })
        .unwrap();

    session.execute(Command::TxnCommit).unwrap();

    // All should be visible
    let executor = strata_executor::Executor::new(db);

    let kv_out = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "kv_key".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        kv_out,
        Output::MaybeVersioned(Some(_)) | Output::Maybe(Some(_))
    ));

    let event_out = executor
        .execute(Command::EventLen {
            branch: None,
            space: None,
        })
        .unwrap();
    match event_out {
        Output::Uint(len) => assert_eq!(len, 1, "Expected exactly 1 event, got {}", len),
        _ => panic!("Expected Uint"),
    }
}

// ============================================================================
// Session Drop Cleanup
// ============================================================================

#[test]
fn session_drop_cleans_up_transaction() {
    let db = create_db();

    {
        let mut session = Session::new(db.clone());

        session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .unwrap();

        session
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: "drop_test".into(),
                value: Value::Int(1),
            })
            .unwrap();

        // Session dropped here without commit or rollback
    }

    // Transaction should be rolled back, data not visible
    let executor = strata_executor::Executor::new(db);
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "drop_test".into(),
            as_of: None,
        })
        .unwrap();

    assert!(matches!(
        output,
        Output::MaybeVersioned(None) | Output::Maybe(None)
    ));
}
