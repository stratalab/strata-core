//! OCC Invariant Tests
//!
//! Tests the core Optimistic Concurrency Control guarantees:
//! - First-committer-wins based on read-set
//! - Blind writes don't conflict
//! - Read-only transactions always commit
//! - Write skew is allowed (per spec)

use std::sync::Arc;
use strata_concurrency::transaction::TransactionContext;
use strata_concurrency::validation::{validate_transaction, ConflictType, ValidationResult};
use strata_core::traits::Storage;
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::sharded::ShardedStore;

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// First-Committer-Wins Rule
// ============================================================================

#[test]
fn first_committer_wins_read_write_conflict() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "contested");

    // Initial value
    Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();

    // T1 reads the key
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    let value = Storage::get(&*store, &key).unwrap();
    t1.read_set
        .insert(key.clone(), value.unwrap().version.as_u64());

    // T2 reads and commits first
    let mut t2 = TransactionContext::new(2, branch_id, 1);
    let value = Storage::get(&*store, &key).unwrap();
    t2.read_set
        .insert(key.clone(), value.unwrap().version.as_u64());
    t2.write_set.insert(key.clone(), Value::Int(200));

    // T2 commits - should succeed
    let result = validate_transaction(&t2, &*store);
    assert!(result.unwrap().is_valid(), "T2 should commit successfully");

    // Apply T2's write
    Storage::put(&*store, key.clone(), Value::Int(200), None).unwrap();

    // T1 tries to commit - should fail with read-write conflict
    t1.write_set.insert(key.clone(), Value::Int(300));
    let result = validate_transaction(&t1, &*store).unwrap();

    assert!(!result.is_valid(), "T1 should fail validation");
    assert_eq!(result.conflict_count(), 1);

    match &result.conflicts[0] {
        ConflictType::ReadWriteConflict {
            key: k,
            read_version,
            current_version,
        } => {
            assert_eq!(k, &key);
            assert_eq!(*read_version, 1);
            assert!(*current_version > 1);
        }
        _ => panic!("Expected ReadWriteConflict"),
    }
}

#[test]
fn blind_writes_dont_conflict() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "blind");

    // Initial value
    Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();

    // T1 does a blind write (no read)
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    t1.write_set.insert(key.clone(), Value::Int(200));

    // T2 modifies the key
    Storage::put(&*store, key.clone(), Value::Int(300), None).unwrap();

    // T1 should still commit - blind writes don't conflict
    let result = validate_transaction(&t1, &*store);
    assert!(
        result.unwrap().is_valid(),
        "Blind write should not conflict"
    );
}

#[test]
fn read_only_transaction_always_commits() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "readonly");

    // Initial value
    Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();

    // T1 only reads
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    let value = Storage::get(&*store, &key).unwrap();
    t1.read_set
        .insert(key.clone(), value.unwrap().version.as_u64());

    // Another transaction modifies the key
    Storage::put(&*store, key.clone(), Value::Int(200), None).unwrap();

    // T1 should still commit - read-only transactions always succeed
    // (per spec Section 3.2 Scenario 3)
    assert!(t1.is_read_only());
    let result = validate_transaction(&t1, &*store);
    assert!(
        result.unwrap().is_valid(),
        "Read-only transaction should always commit"
    );
}

#[test]
fn write_skew_is_allowed() {
    // Classic write skew: two accounts A and B, constraint A + B >= 0
    // T1 reads A=50, B=50, writes A=-10 (check: -10 + 50 = 40 >= 0)
    // T2 reads A=50, B=50, writes B=-10 (check: 50 + -10 = 40 >= 0)
    // Both commit, final: A=-10, B=-10, constraint violated!
    // Per spec: write skew is ALLOWED (we don't try to prevent it)

    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key_a = create_test_key(branch_id, "account_a");
    let key_b = create_test_key(branch_id, "account_b");

    // Initial balances
    Storage::put(&*store, key_a.clone(), Value::Int(50), None).unwrap();
    Storage::put(&*store, key_b.clone(), Value::Int(50), None).unwrap();

    // T1 reads A and B, writes A
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    let val_a = Storage::get(&*store, &key_a).unwrap().unwrap();
    let val_b = Storage::get(&*store, &key_b).unwrap().unwrap();
    t1.read_set.insert(key_a.clone(), val_a.version.as_u64());
    t1.read_set.insert(key_b.clone(), val_b.version.as_u64());
    t1.write_set.insert(key_a.clone(), Value::Int(-10));

    // T2 reads A and B, writes B
    let mut t2 = TransactionContext::new(2, branch_id, 1);
    t2.read_set.insert(key_a.clone(), val_a.version.as_u64());
    t2.read_set.insert(key_b.clone(), val_b.version.as_u64());
    t2.write_set.insert(key_b.clone(), Value::Int(-10));

    // Both should validate successfully (write skew allowed)
    let result1 = validate_transaction(&t1, &*store);
    let result2 = validate_transaction(&t2, &*store);

    assert!(
        result1.unwrap().is_valid(),
        "T1 should commit (write skew allowed)"
    );
    assert!(
        result2.unwrap().is_valid(),
        "T2 should commit (write skew allowed)"
    );
}

// ============================================================================
// Conflict Detection Accuracy
// ============================================================================

#[test]
fn conflict_reports_correct_versions() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "versioned");

    // Initial value at version 1
    Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();
    let v1 = Storage::get(&*store, &key)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // T1 reads at version 1
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    t1.read_set.insert(key.clone(), v1);

    // Update to version 2
    Storage::put(&*store, key.clone(), Value::Int(200), None).unwrap();
    let v2 = Storage::get(&*store, &key)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // T1 writes
    t1.write_set.insert(key.clone(), Value::Int(300));

    let result = validate_transaction(&t1, &*store).unwrap();
    assert!(!result.is_valid());

    match &result.conflicts[0] {
        ConflictType::ReadWriteConflict {
            read_version,
            current_version,
            ..
        } => {
            assert_eq!(*read_version, v1);
            assert_eq!(*current_version, v2);
        }
        _ => panic!("Expected ReadWriteConflict"),
    }
}

#[test]
fn multiple_conflicts_all_reported() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key1 = create_test_key(branch_id, "key1");
    let key2 = create_test_key(branch_id, "key2");

    // Initial values
    Storage::put(&*store, key1.clone(), Value::Int(1), None).unwrap();
    Storage::put(&*store, key2.clone(), Value::Int(2), None).unwrap();

    let v1 = Storage::get(&*store, &key1)
        .unwrap()
        .unwrap()
        .version
        .as_u64();
    let v2 = Storage::get(&*store, &key2)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // T1 reads both
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    t1.read_set.insert(key1.clone(), v1);
    t1.read_set.insert(key2.clone(), v2);

    // Both keys modified
    Storage::put(&*store, key1.clone(), Value::Int(10), None).unwrap();
    Storage::put(&*store, key2.clone(), Value::Int(20), None).unwrap();

    // T1 writes
    t1.write_set.insert(key1.clone(), Value::Int(100));

    let result = validate_transaction(&t1, &*store).unwrap();
    assert!(!result.is_valid());
    assert_eq!(result.conflict_count(), 2, "Should report both conflicts");
}

#[test]
fn no_conflict_when_versions_match() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "stable");

    // Initial value
    Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();
    let version = Storage::get(&*store, &key)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // T1 reads and writes
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    t1.read_set.insert(key.clone(), version);
    t1.write_set.insert(key.clone(), Value::Int(200));

    // No concurrent modification - version still matches
    let result = validate_transaction(&t1, &*store);
    assert!(
        result.unwrap().is_valid(),
        "Should commit when version unchanged"
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_transaction_validates() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();

    let t1 = TransactionContext::new(1, branch_id, 1);
    assert!(t1.is_read_only());

    let result = validate_transaction(&t1, &*store);
    assert!(
        result.unwrap().is_valid(),
        "Empty transaction should validate"
    );
}

#[test]
fn read_nonexistent_key_tracks_version_zero() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "ghost");

    // T1 reads nonexistent key (version 0)
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    let result = Storage::get(&*store, &key).unwrap();
    assert!(result.is_none());
    t1.read_set.insert(key.clone(), 0); // Version 0 = doesn't exist

    // Key is created
    Storage::put(&*store, key.clone(), Value::Int(42), None).unwrap();

    // T1 writes - should conflict (version changed from 0 to non-zero)
    t1.write_set.insert(key.clone(), Value::Int(100));

    let result = validate_transaction(&t1, &*store);
    assert!(
        !result.unwrap().is_valid(),
        "Should conflict when key created after read"
    );
}

#[test]
fn delete_after_read_causes_conflict() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "deleted");

    // Initial value
    Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();
    let version = Storage::get(&*store, &key)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // T1 reads
    let mut t1 = TransactionContext::new(1, branch_id, 1);
    t1.read_set.insert(key.clone(), version);

    // Key is deleted
    Storage::delete(&*store, &key).unwrap();

    // T1 writes
    t1.write_set.insert(key.clone(), Value::Int(200));

    let result = validate_transaction(&t1, &*store);
    assert!(
        !result.unwrap().is_valid(),
        "Should conflict when key deleted after read"
    );
}

#[test]
fn validation_result_merge_combines_conflicts() {
    let mut result1 = ValidationResult::ok();
    let result2 = ValidationResult::conflict(ConflictType::ReadWriteConflict {
        key: create_test_key(BranchId::new(), "k1"),
        read_version: 1,
        current_version: 2,
    });

    assert!(result1.is_valid());
    result1.merge(result2);
    assert!(!result1.is_valid());
    assert_eq!(result1.conflict_count(), 1);
}
