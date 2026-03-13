//! Transaction Lifecycle Tests
//!
//! Tests for complete transaction workflows:
//! - Begin-commit cycle
//! - Begin-abort cycle
//! - Transaction reset/reuse

use std::sync::Arc;
use strata_concurrency::transaction::TransactionContext;
use strata_concurrency::validation::validate_transaction;
use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::sharded::ShardedStore;

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// Begin-Commit Cycle
// ============================================================================

#[test]
fn begin_commit_makes_writes_permanent() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "committed");

    // Begin transaction
    let mut txn = TransactionContext::new(1, branch_id, 1);
    assert!(txn.is_active());

    // Write
    txn.write_set.insert(key.clone(), Value::Int(42));

    // Validate
    txn.mark_validating().unwrap();
    let result = validate_transaction(&txn, &*store);
    assert!(result.unwrap().is_valid());

    // Commit
    txn.mark_committed().unwrap();
    assert!(txn.is_committed());

    // Apply write (simulating what manager does)
    store
        .put_with_version_mode(key.clone(), Value::Int(42), 1, None, WriteMode::Append)
        .unwrap();

    // Value should be visible
    let stored = store.get_versioned(&key, u64::MAX).unwrap();
    assert!(stored.is_some());
    assert_eq!(stored.unwrap().value, Value::Int(42));
}

#[test]
fn committed_status_is_committed() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(1, branch_id, 1);

    txn.mark_validating().unwrap();
    txn.mark_committed().unwrap();

    assert!(txn.is_committed());
    assert!(matches!(
        txn.status,
        strata_concurrency::transaction::TransactionStatus::Committed
    ));
}

// ============================================================================
// Begin-Abort Cycle
// ============================================================================

#[test]
fn begin_abort_discards_writes() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "aborted");

    // Begin transaction
    let mut txn = TransactionContext::new(1, branch_id, 1);

    // Write (not yet applied to store)
    txn.write_set.insert(key.clone(), Value::Int(42));

    // Abort
    txn.mark_aborted("user requested".to_string()).unwrap();
    assert!(txn.is_aborted());

    // Write should NOT be in store
    let stored = store.get_versioned(&key, u64::MAX).unwrap();
    assert!(
        stored.is_none(),
        "Aborted transaction writes should not be visible"
    );
}

#[test]
fn abort_reason_recorded_in_status() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(1, branch_id, 1);

    txn.mark_aborted("validation failed: conflict".to_string())
        .unwrap();

    match &txn.status {
        strata_concurrency::transaction::TransactionStatus::Aborted { reason } => {
            assert!(reason.contains("conflict"));
        }
        _ => panic!("Expected Aborted status"),
    }
}

#[test]
fn validation_failure_leads_to_abort() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "conflict");

    // Initial value
    store
        .put_with_version_mode(key.clone(), Value::Int(1), 1, None, WriteMode::Append)
        .unwrap();
    let version = store
        .get_versioned(&key, u64::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // Transaction reads key
    let mut txn = TransactionContext::new(1, branch_id, 1);
    txn.read_set.insert(key.clone(), version);
    txn.write_set.insert(key.clone(), Value::Int(10));

    // Concurrent modification
    store
        .put_with_version_mode(key.clone(), Value::Int(2), 2, None, WriteMode::Append)
        .unwrap();

    // Validate - should fail
    txn.mark_validating().unwrap();
    let result = validate_transaction(&txn, &*store).unwrap();
    assert!(!result.is_valid());

    // Abort
    txn.mark_aborted(format!("conflict count: {}", result.conflict_count()))
        .unwrap();
    assert!(txn.is_aborted());
}

// ============================================================================
// Transaction Reset/Reuse
// ============================================================================

#[test]
fn reset_clears_all_sets() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "reset");

    let mut txn = TransactionContext::new(1, branch_id, 1);

    // Add some data
    txn.read_set.insert(key.clone(), 1);
    txn.write_set.insert(key.clone(), Value::Int(42));
    txn.delete_set.insert(create_test_key(branch_id, "deleted"));

    assert!(!txn.read_set.is_empty());
    assert!(!txn.write_set.is_empty());
    assert!(!txn.delete_set.is_empty());

    // Reset
    txn.reset(2, branch_id, None);

    // All sets should be empty
    assert!(txn.read_set.is_empty());
    assert!(txn.write_set.is_empty());
    assert!(txn.delete_set.is_empty());
    assert!(txn.cas_set.is_empty());

    // New values
    assert_eq!(txn.txn_id, 2);
    assert_eq!(txn.start_version, 0); // 0 when no snapshot provided
    assert!(txn.is_active());
}

#[test]
fn reset_preserves_capacity() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(1, branch_id, 1);

    // Add many items to force allocation
    for i in 0..100 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
        txn.read_set.insert(key.clone(), i as u64);
        txn.write_set.insert(key, Value::Int(i));
    }

    let read_capacity_before = txn.read_set.capacity();
    let write_capacity_before = txn.write_set.capacity();

    // Reset
    txn.reset(2, branch_id, None);

    // Capacity should be preserved (no reallocation needed for next use)
    assert!(txn.read_set.capacity() >= read_capacity_before);
    assert!(txn.write_set.capacity() >= write_capacity_before);
}

#[test]
fn reset_after_abort_allows_reuse() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(1, branch_id, 1);

    // Abort
    txn.mark_aborted("test".to_string()).unwrap();
    assert!(txn.is_aborted());

    // Reset
    txn.reset(2, branch_id, None);

    // Should be active again
    assert!(txn.is_active());
}

#[test]
fn reset_after_commit_allows_reuse() {
    let branch_id = BranchId::new();
    let mut txn = TransactionContext::new(1, branch_id, 1);

    // Commit
    txn.mark_validating().unwrap();
    txn.mark_committed().unwrap();
    assert!(txn.is_committed());

    // Reset
    txn.reset(2, branch_id, None);

    // Should be active again
    assert!(txn.is_active());
}

// ============================================================================
// Full Workflow Tests
// ============================================================================

#[test]
fn read_modify_write_workflow() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "rmw");

    // Initial value
    store
        .put_with_version_mode(key.clone(), Value::Int(100), 1, None, WriteMode::Append)
        .unwrap();
    let version = store
        .get_versioned(&key, u64::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // Read-modify-write
    let mut txn = TransactionContext::new(1, branch_id, version);

    // Read (track in read_set)
    let current = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
    txn.read_set.insert(key.clone(), current.version.as_u64());

    // Modify
    if let Value::Int(v) = current.value {
        txn.write_set.insert(key.clone(), Value::Int(v + 10));
    }

    // Validate and commit
    txn.mark_validating().unwrap();
    let result = validate_transaction(&txn, &*store);
    assert!(result.unwrap().is_valid());

    txn.mark_committed().unwrap();

    // Apply
    store
        .put_with_version_mode(key.clone(), Value::Int(110), 2, None, WriteMode::Append)
        .unwrap();

    // Verify
    let final_value = store.get_versioned(&key, u64::MAX).unwrap().unwrap().value;
    assert_eq!(final_value, Value::Int(110));
}

#[test]
fn multi_key_transaction_workflow() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();

    let key1 = create_test_key(branch_id, "k1");
    let key2 = create_test_key(branch_id, "k2");
    let key3 = create_test_key(branch_id, "k3");

    // Initial values
    store
        .put_with_version_mode(key1.clone(), Value::Int(1), 1, None, WriteMode::Append)
        .unwrap();
    store
        .put_with_version_mode(key2.clone(), Value::Int(2), 2, None, WriteMode::Append)
        .unwrap();
    // key3 doesn't exist

    let v1 = store
        .get_versioned(&key1, u64::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();
    let v2 = store
        .get_versioned(&key2, u64::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // Transaction: read k1, write k2, create k3
    let mut txn = TransactionContext::new(1, branch_id, 1);
    txn.read_set.insert(key1.clone(), v1);
    txn.read_set.insert(key2.clone(), v2);
    txn.write_set.insert(key2.clone(), Value::Int(20));
    txn.write_set.insert(key3.clone(), Value::Int(3));

    // Validate
    txn.mark_validating().unwrap();
    let result = validate_transaction(&txn, &*store);
    assert!(result.unwrap().is_valid());

    txn.mark_committed().unwrap();

    // Apply all writes
    store
        .put_with_version_mode(key2.clone(), Value::Int(20), 3, None, WriteMode::Append)
        .unwrap();
    store
        .put_with_version_mode(key3.clone(), Value::Int(3), 4, None, WriteMode::Append)
        .unwrap();

    // Verify
    assert_eq!(
        store.get_versioned(&key2, u64::MAX).unwrap().unwrap().value,
        Value::Int(20)
    );
    assert_eq!(
        store.get_versioned(&key3, u64::MAX).unwrap().unwrap().value,
        Value::Int(3)
    );
}

#[test]
fn delete_workflow() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "to_delete");

    // Initial value
    store
        .put_with_version_mode(key.clone(), Value::Int(42), 1, None, WriteMode::Append)
        .unwrap();
    let version = store
        .get_versioned(&key, u64::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // Transaction: read then delete
    let mut txn = TransactionContext::new(1, branch_id, 1);
    txn.read_set.insert(key.clone(), version);
    txn.delete_set.insert(key.clone());

    // Validate
    txn.mark_validating().unwrap();
    let result = validate_transaction(&txn, &*store);
    assert!(result.unwrap().is_valid());

    txn.mark_committed().unwrap();

    // Apply delete
    store.delete_with_version(&key, 2).unwrap();

    // Verify deleted
    assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_transaction_commits() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();

    let mut txn = TransactionContext::new(1, branch_id, 1);

    txn.mark_validating().unwrap();
    let result = validate_transaction(&txn, &*store);
    assert!(result.unwrap().is_valid());

    txn.mark_committed().unwrap();
    assert!(txn.is_committed());
}

#[test]
fn many_sequential_transactions() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "sequential");

    store
        .put_with_version_mode(key.clone(), Value::Int(0), 1, None, WriteMode::Append)
        .unwrap();

    for i in 1..=10 {
        let version = store
            .get_versioned(&key, u64::MAX)
            .unwrap()
            .unwrap()
            .version
            .as_u64();

        let mut txn = TransactionContext::new(i as u64, branch_id, version);
        txn.read_set.insert(key.clone(), version);
        txn.write_set.insert(key.clone(), Value::Int(i));

        txn.mark_validating().unwrap();
        let result = validate_transaction(&txn, &*store);
        assert!(
            result.unwrap().is_valid(),
            "Transaction {} should validate",
            i
        );

        txn.mark_committed().unwrap();
        store
            .put_with_version_mode(
                key.clone(),
                Value::Int(i),
                (i + 1) as u64,
                None,
                WriteMode::Append,
            )
            .unwrap();
    }

    // Final value should be 10
    let final_value = store.get_versioned(&key, u64::MAX).unwrap().unwrap().value;
    assert_eq!(final_value, Value::Int(10));
}
