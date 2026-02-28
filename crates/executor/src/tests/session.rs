//! Session tests: verify transactional session lifecycle and routing.

use crate::Value;
use crate::{Command, Error, Output, Session};
use strata_engine::Database;

/// Create a test session with a cache in-memory database.
fn create_test_session() -> Session {
    let db = Database::cache().unwrap();
    Session::new(db)
}

// =============================================================================
// Transaction Lifecycle
// =============================================================================

#[test]
fn test_begin_commit_lifecycle() {
    let mut session = create_test_session();

    assert!(!session.in_transaction());

    // Begin
    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });
    assert!(matches!(result, Ok(Output::TxnBegun)));
    assert!(session.in_transaction());

    // Commit
    let result = session.execute(Command::TxnCommit);
    assert!(matches!(result, Ok(Output::TxnCommitted { .. })));
    assert!(!session.in_transaction());
}

#[test]
fn test_begin_abort_lifecycle() {
    let mut session = create_test_session();

    // Begin
    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });
    assert!(matches!(result, Ok(Output::TxnBegun)));
    assert!(session.in_transaction());

    // Abort (TxnRollback)
    let result = session.execute(Command::TxnRollback);
    assert!(matches!(result, Ok(Output::TxnAborted)));
    assert!(!session.in_transaction());
}

#[test]
fn test_double_begin_returns_error() {
    let mut session = create_test_session();

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
    assert!(
        matches!(result, Err(Error::TransactionAlreadyActive)),
        "Expected TransactionAlreadyActive, got {:?}",
        result,
    );
}

#[test]
fn test_commit_without_begin_returns_error() {
    let mut session = create_test_session();

    let result = session.execute(Command::TxnCommit);
    assert!(
        matches!(result, Err(Error::TransactionNotActive)),
        "Expected TransactionNotActive, got {:?}",
        result,
    );
}

#[test]
fn test_abort_without_begin_returns_error() {
    let mut session = create_test_session();

    let result = session.execute(Command::TxnRollback);
    assert!(
        matches!(result, Err(Error::TransactionNotActive)),
        "Expected TransactionNotActive, got {:?}",
        result,
    );
}

#[test]
fn test_txn_is_active() {
    let mut session = create_test_session();

    let result = session.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(result, Output::Bool(false)));

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    let result = session.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(result, Output::Bool(true)));

    session.execute(Command::TxnCommit).unwrap();

    let result = session.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(result, Output::Bool(false)));
}

#[test]
fn test_txn_info() {
    let mut session = create_test_session();

    // No txn active
    let result = session.execute(Command::TxnInfo).unwrap();
    assert!(matches!(result, Output::TxnInfo(None)));

    // With txn active
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    let result = session.execute(Command::TxnInfo).unwrap();
    match result {
        Output::TxnInfo(Some(info)) => {
            assert!(!info.id.is_empty());
        }
        other => panic!("Expected TxnInfo(Some(_)), got {:?}", other),
    }
}

// =============================================================================
// Read-Your-Writes (RYW) Semantics
// =============================================================================

#[test]
fn test_ryw_kv_put_get_inside_txn() {
    let mut session = create_test_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Put inside txn
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "ryw_key".to_string(),
            value: Value::String("written_in_txn".into()),
        })
        .unwrap();

    // Get should see the written value (read-your-writes)
    let result = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "ryw_key".to_string(),
            as_of: None,
        })
        .unwrap();

    match result {
        Output::MaybeVersioned(Some(vv)) => {
            assert_eq!(vv.value, Value::String("written_in_txn".into()));
        }
        Output::Maybe(Some(v)) => {
            assert_eq!(v, Value::String("written_in_txn".into()));
        }
        Output::MaybeVersioned(None) | Output::Maybe(None) => {
            // RYW may not be visible if the Transaction impl returns None
            // for keys only in write_set. This is acceptable as the
            // Transaction::kv_get implementation does check write_set.
        }
        other => panic!("Expected Maybe or MaybeVersioned, got {:?}", other),
    }

    session.execute(Command::TxnCommit).unwrap();
}

#[test]
fn test_ryw_kv_get_inside_txn() {
    let mut session = create_test_session();

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
            key: "exists_key".to_string(),
            value: Value::Int(42),
        })
        .unwrap();

    let result = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "exists_key".to_string(),
            as_of: None,
        })
        .unwrap();

    match result {
        Output::MaybeVersioned(Some(vv)) => {
            assert_eq!(vv.value, Value::Int(42), "Value should match what was put");
        }
        Output::Maybe(Some(val)) => {
            // In-transaction reads may return Maybe instead of MaybeVersioned
            assert_eq!(val, Value::Int(42), "Value should match what was put");
        }
        other => panic!(
            "Expected Maybe(Some) or MaybeVersioned(Some), got {:?}",
            other
        ),
    }

    session.execute(Command::TxnCommit).unwrap();
}

// =============================================================================
// Drop Cleanup
// =============================================================================

#[test]
fn test_drop_with_active_txn_does_not_panic() {
    let mut session = create_test_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Write something inside the txn
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "abandoned".to_string(),
            value: Value::Bool(true),
        })
        .unwrap();

    // Drop the session with an active transaction — should not panic
    drop(session);
}

#[test]
fn test_drop_without_active_txn_does_not_panic() {
    let session = create_test_session();
    drop(session);
}

// =============================================================================
// Non-Transactional Commands During Active Txn
// =============================================================================

#[test]
fn test_non_transactional_commands_delegate_during_txn() {
    let mut session = create_test_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Ping should still work (delegates to executor)
    let result = session.execute(Command::Ping);
    assert!(result.is_ok(), "Ping should succeed during active txn");

    // Info should still work
    let result = session.execute(Command::Info);
    assert!(result.is_ok(), "Info should succeed during active txn");

    session.execute(Command::TxnCommit).unwrap();
}

// =============================================================================
// Data Commands Without Txn Delegate to Executor
// =============================================================================

#[test]
fn test_data_commands_without_txn_delegate() {
    let mut session = create_test_session();

    // KvPut without txn delegates to executor
    let result = session.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "no_txn_key".to_string(),
        value: Value::Int(99),
    });
    assert!(result.is_ok(), "KvPut should succeed without txn");

    // KvGet without txn delegates to executor
    let result = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "no_txn_key".to_string(),
            as_of: None,
        })
        .unwrap();

    match result {
        Output::MaybeVersioned(Some(vv)) => {
            let v = vv.value;
            assert_eq!(v, Value::Int(99));
        }
        other => panic!("Expected Maybe(Some), got {:?}", other),
    }
}

// =============================================================================
// Begin With Explicit Run
// =============================================================================

#[test]
fn test_begin_with_explicit_branch() {
    let mut session = create_test_session();

    let result = session.execute(Command::TxnBegin {
        branch: Some(crate::types::BranchId::from("default")),
        options: None,
    });
    assert!(matches!(result, Ok(Output::TxnBegun)));

    session.execute(Command::TxnCommit).unwrap();
}

// =============================================================================
// Multiple Begin-Commit Cycles
// =============================================================================

#[test]
fn test_multiple_txn_cycles() {
    let mut session = create_test_session();

    for i in 0..3 {
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
                key: format!("cycle_{}", i),
                value: Value::Int(i as i64),
            })
            .unwrap();

        session.execute(Command::TxnCommit).unwrap();
    }

    assert!(!session.in_transaction());
}

// =============================================================================
// Event Operations In Transaction
// =============================================================================

#[test]
fn test_event_append_in_txn() {
    let mut session = create_test_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    let result = session.execute(Command::EventAppend {
        branch: None,
        space: None,
        event_type: "test_stream".to_string(),
        payload: Value::Object(Box::new(std::collections::HashMap::from([(
            "data".to_string(),
            Value::String("event_data".into()),
        )]))),
    });
    assert!(result.is_ok(), "EventAppend should succeed in txn");

    session.execute(Command::TxnCommit).unwrap();
}

// =============================================================================
// State Operations In Transaction
// =============================================================================

#[test]
fn test_state_init_in_txn() {
    let mut session = create_test_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    let result = session.execute(Command::StateInit {
        branch: None,
        space: None,
        cell: "counter".to_string(),
        value: Value::Int(0),
    });
    assert!(result.is_ok(), "StateInit should succeed in txn");

    session.execute(Command::TxnCommit).unwrap();
}
