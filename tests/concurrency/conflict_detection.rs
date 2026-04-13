//! Conflict Detection Tests
//!
//! Tests for all conflict types:
//! - Read-write conflicts
//! - CAS conflicts
//! - JSON document conflicts
//! - JSON path conflicts

use std::collections::HashMap;
use std::sync::Arc;
use strata_concurrency::transaction::{CASOperation, TransactionContext};
use strata_concurrency::validation::{
    validate_cas_set, validate_read_set, validate_transaction, ConflictType,
};
use strata_core::id::{CommitVersion, TxnId};
use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::SegmentedStore;

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// Read-Write Conflicts
// ============================================================================

#[test]
fn read_write_conflict_version_increased() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "rw");

    // Initial value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v1 = CommitVersion(store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64());

    // Read set at v1
    let mut read_set = HashMap::new();
    read_set.insert(key.clone(), v1);

    // Update key
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Validate
    let result = validate_read_set(&read_set, &*store).unwrap();
    assert!(!result.is_valid());
    assert!(matches!(
        &result.conflicts[0],
        ConflictType::ReadWriteConflict { .. }
    ));
}

#[test]
fn read_write_conflict_key_deleted() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "deleted");

    // Initial value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v1 = CommitVersion(store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64());

    // Read set at v1
    let mut read_set = HashMap::new();
    read_set.insert(key.clone(), v1);

    // Delete key
    store.delete_with_version(&key, CommitVersion(2)).unwrap();

    // Validate - should conflict (version changed to 0)
    let result = validate_read_set(&read_set, &*store).unwrap();
    assert!(!result.is_valid());
}

#[test]
fn read_write_conflict_key_created() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "created");

    // Read set at v0 (nonexistent)
    let mut read_set = HashMap::new();
    read_set.insert(key.clone(), CommitVersion::ZERO);

    // Create key
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Validate - should conflict (version changed from 0)
    let result = validate_read_set(&read_set, &*store).unwrap();
    assert!(!result.is_valid());
}

#[test]
fn no_read_write_conflict_version_same() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "stable");

    // Initial value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v1 = CommitVersion(store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64());

    // Read set at v1
    let mut read_set = HashMap::new();
    read_set.insert(key.clone(), v1);

    // No changes

    // Validate - should pass
    let result = validate_read_set(&read_set, &*store).unwrap();
    assert!(result.is_valid());
}

// ============================================================================
// CAS Conflicts
// ============================================================================

#[test]
fn cas_conflict_version_mismatch() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas");

    // Initial value at version 1
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Update to version 2
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(200),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v2 = CommitVersion(store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64());

    // CAS with stale expected_version (1, but current is 2)
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(1), // Stale!
        new_value: Value::Int(300),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(!result.is_valid());
    match &result.conflicts[0] {
        ConflictType::CASConflict {
            expected_version,
            current_version,
            ..
        } => {
            assert_eq!(*expected_version, CommitVersion(1));
            assert_eq!(*current_version, v2);
        }
        _ => panic!("Expected CASConflict"),
    }
}

#[test]
fn cas_create_conflict_key_exists() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_create");

    // Key already exists
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // CAS with expected_version=0 (key must not exist)
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion::ZERO, // Expects key doesn't exist
        new_value: Value::Int(200),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(!result.is_valid());
    match &result.conflicts[0] {
        ConflictType::CASConflict {
            expected_version, ..
        } => {
            assert_eq!(*expected_version, CommitVersion::ZERO);
        }
        _ => panic!("Expected CASConflict"),
    }
}

#[test]
fn cas_success_version_matches() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_ok");

    // Initial value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v1 = CommitVersion(store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64());

    // CAS with correct expected_version
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: v1,
        new_value: Value::Int(200),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(result.is_valid());
}

#[test]
fn cas_create_success_key_not_exists() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_new");

    // Key doesn't exist

    // CAS with expected_version=0 (create)
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion::ZERO,
        new_value: Value::Int(100),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(result.is_valid());
}

#[test]
fn multiple_cas_operations() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key1 = create_test_key(branch_id, "cas1");
    let key2 = create_test_key(branch_id, "cas2");

    // Setup
    store
        .put_with_version_mode(
            key1.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v1 = store
        .get_versioned(&key1, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();
    store
        .put_with_version_mode(
            key2.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Update key2
    store
        .put_with_version_mode(
            key2.clone(),
            Value::Int(20),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // CAS on both - key1 should succeed, key2 should fail
    let cas_set = vec![
        CASOperation {
            key: key1.clone(),
            expected_version: CommitVersion(v1),
            new_value: Value::Int(10),
        },
        CASOperation {
            key: key2.clone(),
            expected_version: CommitVersion(1), // Stale
            new_value: Value::Int(200),
        },
    ];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(!result.is_valid());
    assert_eq!(result.conflict_count(), 1); // Only key2 conflicts
}

// ============================================================================
// Combined Validation
// ============================================================================

#[test]
fn transaction_validation_combines_all_checks() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key1 = create_test_key(branch_id, "read_key");
    let key2 = create_test_key(branch_id, "cas_key");

    // Setup
    store
        .put_with_version_mode(
            key1.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v1 = store
        .get_versioned(&key1, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();
    store
        .put_with_version_mode(
            key2.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Transaction with read and CAS
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));
    txn.read_set.insert(key1.clone(), CommitVersion(v1));
    txn.cas_set.push(CASOperation {
        key: key2.clone(),
        expected_version: CommitVersion(1), // Stale
        new_value: Value::Int(20),
    });

    // Modify both keys
    store
        .put_with_version_mode(
            key1.clone(),
            Value::Int(10),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            key2.clone(),
            Value::Int(20),
            CommitVersion(4),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Validate - should have both conflicts
    let result = validate_transaction(&txn, &*store).unwrap();
    assert!(!result.is_valid());
    // Read-write conflict on key1, CAS conflict on key2
    assert!(result.conflict_count() >= 1);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_read_set_validates() {
    let store = Arc::new(SegmentedStore::new());
    let read_set = HashMap::new();

    let result = validate_read_set(&read_set, &*store).unwrap();
    assert!(result.is_valid());
}

#[test]
fn empty_cas_set_validates() {
    let store = Arc::new(SegmentedStore::new());
    let cas_set: Vec<CASOperation> = Vec::new();

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(result.is_valid());
}

#[test]
fn conflict_type_debug_formatting() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "debug");

    let conflict = ConflictType::ReadWriteConflict {
        key: key.clone(),
        read_version: CommitVersion(1),
        current_version: CommitVersion(2),
    };

    let debug_str = format!("{:?}", conflict);
    assert!(debug_str.contains("ReadWriteConflict"));
    assert!(debug_str.contains("read_version"));
}

#[test]
fn large_read_set_validation() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Create 100 keys
    let mut read_set = HashMap::new();
    for i in 0..100 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
        store
            .put_with_version_mode(
                key.clone(),
                Value::Int(i),
                CommitVersion((i + 1) as u64),
                None,
                WriteMode::Append,
            )
            .unwrap();
        let v = store
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .version
            .as_u64();
        read_set.insert(key, CommitVersion(v));
    }

    // All versions match - should validate
    let result = validate_read_set(&read_set, &*store).unwrap();
    assert!(result.is_valid());
}

#[test]
fn large_read_set_with_one_conflict() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Create 100 keys
    let mut read_set = HashMap::new();
    for i in 0..100 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
        store
            .put_with_version_mode(
                key.clone(),
                Value::Int(i),
                CommitVersion((i + 1) as u64),
                None,
                WriteMode::Append,
            )
            .unwrap();
        let v = store
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .version
            .as_u64();
        read_set.insert(key, CommitVersion(v));
    }

    // Modify one key
    let modified_key = create_test_key(branch_id, "key_50");
    store
        .put_with_version_mode(
            modified_key,
            Value::Int(500),
            CommitVersion(101),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Should have exactly one conflict
    let result = validate_read_set(&read_set, &*store).unwrap();
    assert!(!result.is_valid());
    assert_eq!(result.conflict_count(), 1);
}
