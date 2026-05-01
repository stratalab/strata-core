//! Transaction State Machine Tests
//!
//! Tests the transaction state transitions:
//! - Active → Validating → Committed
//! - Active → Validating → Aborted
//! - Active → Aborted (explicit)

use strata_core::id::{CommitVersion, TxnId};
use strata_core::BranchId;
use strata_storage::{CASOperation, TransactionContext, TransactionStatus};

// ============================================================================
// State Inspection
// ============================================================================

#[test]
fn new_transaction_is_active() {
    let branch_id = BranchId::new();
    let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    assert!(txn.is_active());
    assert!(!txn.is_committed());
    assert!(!txn.is_aborted());
    assert!(matches!(txn.status.clone(), TransactionStatus::Active));
}

#[test]
fn transaction_status_active_variant() {
    let branch_id = BranchId::new();
    let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    match txn.status.clone() {
        TransactionStatus::Active => {}
        _ => panic!("Expected Active status"),
    }
}

// ============================================================================
// Valid State Transitions
// ============================================================================

#[test]
fn active_to_validating_succeeds() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    assert!(txn.is_active());
    txn.mark_validating().unwrap();
    assert!(matches!(txn.status.clone(), TransactionStatus::Validating));
}

#[test]
fn validating_to_committed_succeeds() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_validating().unwrap();
    txn.mark_committed().unwrap();

    assert!(txn.is_committed());
    assert!(!txn.is_active());
    assert!(!txn.is_aborted());
    assert!(matches!(txn.status.clone(), TransactionStatus::Committed));
}

#[test]
fn validating_to_aborted_succeeds() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_validating().unwrap();
    txn.mark_aborted("validation failed".to_string()).unwrap();

    assert!(txn.is_aborted());
    assert!(!txn.is_active());
    assert!(!txn.is_committed());
}

#[test]
fn active_to_aborted_succeeds() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_aborted("explicit abort".to_string()).unwrap();

    assert!(txn.is_aborted());
    assert!(!txn.is_active());
}

#[test]
fn aborted_status_contains_reason() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_aborted("conflict detected".to_string()).unwrap();

    match txn.status.clone() {
        TransactionStatus::Aborted { reason } => {
            assert!(reason.contains("conflict"));
        }
        _ => panic!("Expected Aborted status"),
    }
}

#[test]
fn committed_status_is_committed() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_validating().unwrap();
    txn.mark_committed().unwrap();

    assert!(matches!(txn.status.clone(), TransactionStatus::Committed));
}

// ============================================================================
// Invalid State Transitions
// ============================================================================

#[test]
fn double_mark_validating_fails() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_validating().unwrap();
    let result = txn.mark_validating();
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("active") || msg.contains("invalid") || msg.contains("state"),
        "Expected invalid state transition error, got: {}",
        err
    );
}

#[test]
fn commit_while_active_fails() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    // Skip validating state
    let result = txn.mark_committed();
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("commit") || msg.contains("state") || msg.contains("invalid"),
        "Expected invalid state transition error, got: {}",
        err
    );
}

#[test]
fn commit_while_aborted_fails() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_aborted("aborted".to_string()).unwrap();
    let result = txn.mark_committed();
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("commit") || msg.contains("state") || msg.contains("abort"),
        "Expected invalid state transition error, got: {}",
        err
    );
}

#[test]
fn commit_while_already_committed_fails() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    txn.mark_validating().unwrap();
    txn.mark_committed().unwrap();
    let result = txn.mark_committed();
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("commit") || msg.contains("state") || msg.contains("invalid"),
        "Expected invalid state transition error, got: {}",
        err
    );
}

// ============================================================================
// Transaction Properties
// ============================================================================

#[test]
fn transaction_preserves_ids() {
    let branch_id = BranchId::new();
    let txn = TransactionContext::new(TxnId(42), branch_id, CommitVersion(100));

    assert_eq!(txn.txn_id, TxnId(42));
    assert_eq!(txn.branch_id, branch_id);
    assert_eq!(txn.start_version, CommitVersion(100));
}

#[test]
fn transaction_tracks_elapsed_time() {
    let branch_id = BranchId::new();
    let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let elapsed = txn.elapsed();
    assert!(elapsed.as_secs() < 1, "Elapsed should be very small");
}

#[test]
fn transaction_expiration_check() {
    use std::time::Duration;

    let branch_id = BranchId::new();
    let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    // Should not be expired with 1 hour timeout
    assert!(!txn.is_expired(Duration::from_secs(3600)));

    // Should be "expired" with 0 timeout (always expired)
    assert!(txn.is_expired(Duration::from_secs(0)));
}

// ============================================================================
// Read-Only Detection
// ============================================================================

#[test]
fn empty_transaction_is_read_only() {
    let branch_id = BranchId::new();
    let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    assert!(txn.is_read_only());
}

#[test]
fn transaction_with_write_is_not_read_only() {
    use std::sync::Arc;
    use strata_core::value::Value;
    use strata_storage::{Key, Namespace};

    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let key = Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), "test");
    txn.write_set.insert(key, Value::Int(42));

    assert!(!txn.is_read_only());
}

#[test]
fn transaction_with_delete_is_not_read_only() {
    use std::sync::Arc;
    use strata_storage::{Key, Namespace};

    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let key = Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), "test");
    txn.delete_set.insert(key);

    assert!(!txn.is_read_only());
}

#[test]
fn transaction_with_cas_is_not_read_only() {
    use std::sync::Arc;
    use strata_core::value::Value;
    use strata_storage::{Key, Namespace};

    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let key = Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), "test");
    txn.cas_set.push(CASOperation {
        key,
        expected_version: CommitVersion(1),
        new_value: Value::Int(42),
    });

    assert!(!txn.is_read_only());
}

#[test]
fn transaction_with_only_reads_is_read_only() {
    use std::sync::Arc;
    use strata_storage::{Key, Namespace};

    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let key = Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), "test");
    txn.read_set.insert(key, CommitVersion(1));

    assert!(txn.is_read_only());
}

// ============================================================================
// Operation Counts
// ============================================================================

#[test]
fn pending_operations_count() {
    use std::sync::Arc;
    use strata_core::value::Value;
    use strata_storage::{Key, Namespace};

    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Add some operations
    txn.write_set
        .insert(Key::new_kv(ns.clone(), "w1"), Value::Int(1));
    txn.write_set
        .insert(Key::new_kv(ns.clone(), "w2"), Value::Int(2));
    txn.delete_set.insert(Key::new_kv(ns.clone(), "d1"));

    let pending = txn.pending_operations();
    assert_eq!(pending.puts, 2);
    assert_eq!(pending.deletes, 1);
    assert_eq!(pending.cas, 0);
}

#[test]
fn read_count_tracks_reads() {
    use std::sync::Arc;
    use strata_storage::{Key, Namespace};

    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let ns = Arc::new(Namespace::for_branch(branch_id));

    txn.read_set
        .insert(Key::new_kv(ns.clone(), "r1"), CommitVersion(1));
    txn.read_set
        .insert(Key::new_kv(ns.clone(), "r2"), CommitVersion(2));
    txn.read_set
        .insert(Key::new_kv(ns.clone(), "r3"), CommitVersion(3));

    assert_eq!(txn.read_count(), 3);
}

#[test]
fn write_count_tracks_only_writes() {
    use std::sync::Arc;
    use strata_core::value::Value;
    use strata_storage::{Key, Namespace};

    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

    let ns = Arc::new(Namespace::for_branch(branch_id));

    txn.write_set
        .insert(Key::new_kv(ns.clone(), "w1"), Value::Int(1));
    txn.write_set
        .insert(Key::new_kv(ns.clone(), "w2"), Value::Int(2));
    txn.delete_set.insert(Key::new_kv(ns.clone(), "d1"));
    txn.delete_set.insert(Key::new_kv(ns.clone(), "d2"));

    assert_eq!(txn.write_count(), 2); // Only writes, not deletes
    assert_eq!(txn.delete_set.len(), 2); // Deletes tracked separately
}
