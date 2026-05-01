//! CAS (Compare-And-Swap) Operation Tests
//!
//! Tests for CAS semantics:
//! - Successful CAS when version matches
//! - Failed CAS on version mismatch
//! - CAS create (expected_version=0)
//! - CAS not in read set

use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::{
    validate_cas_set, validate_transaction, CASOperation, ConflictType, Key, Namespace,
    SegmentedStore, Storage, TransactionContext, WriteMode,
};

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// Successful CAS
// ============================================================================

#[test]
fn cas_succeeds_when_version_matches() {
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
    let version = store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // CAS with correct version
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(version),
        new_value: Value::Int(200),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(result.is_valid(), "CAS should succeed when version matches");
}

#[test]
fn cas_create_succeeds_when_key_absent() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_create");

    // Key doesn't exist

    // CAS with expected_version=0 (create)
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion::ZERO,
        new_value: Value::Int(42),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(
        result.is_valid(),
        "CAS create should succeed when key absent"
    );
}

// ============================================================================
// Failed CAS
// ============================================================================

#[test]
fn cas_fails_when_version_stale() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_stale");

    // Create at version 1
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Update to version 2
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let current_version = store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // CAS with stale version (1)
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(1), // Stale!
        new_value: Value::Int(3),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(!result.is_valid(), "CAS should fail when version is stale");

    match &result.conflicts[0] {
        ConflictType::CASConflict {
            expected_version,
            current_version: cv,
            ..
        } => {
            assert_eq!(*expected_version, CommitVersion(1));
            assert_eq!(*cv, CommitVersion(current_version));
        }
        _ => panic!("Expected CASConflict"),
    }
}

#[test]
fn cas_create_fails_when_key_exists() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_exists");

    // Key exists
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // CAS with expected_version=0 (expects key doesn't exist)
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion::ZERO,
        new_value: Value::Int(200),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(!result.is_valid(), "CAS create should fail when key exists");
}

#[test]
fn cas_fails_when_key_deleted() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_deleted");

    // Create and delete
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let version = store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();
    store.delete_with_version(&key, CommitVersion(2)).unwrap();

    // CAS with old version (before delete)
    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(version),
        new_value: Value::Int(200),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(!result.is_valid(), "CAS should fail when key was deleted");
}

// ============================================================================
// CAS Not In Read Set
// ============================================================================

#[test]
fn cas_not_added_to_read_set() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_no_read");

    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));

    // Add CAS operation
    txn.cas_set.push(CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(1),
        new_value: Value::Int(42),
    });

    // Read set should be empty
    assert!(txn.read_set.is_empty(), "CAS should not add to read_set");
    assert!(!txn.is_read_only(), "Transaction with CAS is not read-only");
}

#[test]
fn cas_validated_separately_from_reads() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let read_key = create_test_key(branch_id, "read_key");
    let cas_key = create_test_key(branch_id, "cas_key");

    // Setup
    store
        .put_with_version_mode(
            read_key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let read_version = store
        .get_versioned(&read_key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();
    store
        .put_with_version_mode(
            cas_key.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let cas_version = store
        .get_versioned(&cas_key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // Transaction reads one key, CAS on another
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));
    txn.read_set
        .insert(read_key.clone(), CommitVersion(read_version));
    txn.cas_set.push(CASOperation {
        key: cas_key.clone(),
        expected_version: CommitVersion(cas_version),
        new_value: Value::Int(20),
    });

    // Modify read_key only
    store
        .put_with_version_mode(
            read_key.clone(),
            Value::Int(10),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Validation should fail on read_key (ReadWriteConflict), not on CAS
    let result = validate_transaction(&txn, &*store).unwrap();
    assert!(!result.is_valid());

    // Should have ReadWriteConflict, not CASConflict
    assert!(result
        .conflicts
        .iter()
        .any(|c| matches!(c, ConflictType::ReadWriteConflict { .. })));
    assert!(!result
        .conflicts
        .iter()
        .any(|c| matches!(c, ConflictType::CASConflict { .. })));
}

// ============================================================================
// Multiple CAS Operations
// ============================================================================

#[test]
fn multiple_cas_all_succeed() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Setup 3 keys
    let keys: Vec<_> = (0..3)
        .map(|i| {
            let key = create_test_key(branch_id, &format!("cas_{}", i));
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
            (key, v)
        })
        .collect();

    // CAS all with correct versions
    let cas_set: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, (key, version))| CASOperation {
            key: key.clone(),
            expected_version: CommitVersion(*version),
            new_value: Value::Int((i * 10) as i64),
        })
        .collect();

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(result.is_valid(), "All CAS should succeed");
}

#[test]
fn multiple_cas_one_fails() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Setup 3 keys
    let key1 = create_test_key(branch_id, "cas_1");
    let key2 = create_test_key(branch_id, "cas_2");
    let key3 = create_test_key(branch_id, "cas_3");

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
    // Update key2 to make its version stale
    store
        .put_with_version_mode(
            key2.clone(),
            Value::Int(20),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();

    store
        .put_with_version_mode(
            key3.clone(),
            Value::Int(3),
            CommitVersion(4),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v3 = store
        .get_versioned(&key3, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // CAS with key2's version being stale
    let cas_set = vec![
        CASOperation {
            key: key1.clone(),
            expected_version: CommitVersion(v1),
            new_value: Value::Int(10),
        },
        CASOperation {
            key: key2.clone(),
            expected_version: CommitVersion(1), // Stale - was updated
            new_value: Value::Int(200),
        },
        CASOperation {
            key: key3.clone(),
            expected_version: CommitVersion(v3),
            new_value: Value::Int(30),
        },
    ];

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(!result.is_valid(), "Should fail due to key2");
    assert_eq!(result.conflict_count(), 1, "Only key2 should conflict");
}

// ============================================================================
// CAS with Transaction Workflow
// ============================================================================

#[test]
fn cas_in_full_transaction() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_txn");

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
    let version = store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // Transaction with CAS
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));
    txn.cas_set.push(CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(version),
        new_value: Value::Int(200),
    });

    // Validate
    let result = validate_transaction(&txn, &*store);
    assert!(result.unwrap().is_valid());
}

#[test]
fn cas_with_read_of_same_key() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cas_read_same");

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
    let version = store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64();

    // Transaction reads and CAS same key
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));
    txn.read_set.insert(key.clone(), CommitVersion(version));
    txn.cas_set.push(CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(version),
        new_value: Value::Int(200),
    });

    // Both should pass (version matches)
    let result = validate_transaction(&txn, &*store);
    assert!(result.unwrap().is_valid());
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn cas_empty_set_validates() {
    let store = Arc::new(SegmentedStore::new());
    let cas_set: Vec<CASOperation> = Vec::new();

    let result = validate_cas_set(&cas_set, &*store).unwrap();
    assert!(result.is_valid());
}

#[test]
fn cas_operation_fields_accessible() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "fields");

    let cas = CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(42),
        new_value: Value::Int(100),
    };

    assert_eq!(cas.key, key);
    assert_eq!(cas.expected_version, CommitVersion(42));
    assert_eq!(cas.new_value, Value::Int(100));
}

#[test]
fn cas_conflict_reports_correct_key() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "conflict_key");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let cas_set = vec![CASOperation {
        key: key.clone(),
        expected_version: CommitVersion(1),
        new_value: Value::Int(100),
    }];

    let result = validate_cas_set(&cas_set, &*store).unwrap();

    match &result.conflicts[0] {
        ConflictType::CASConflict { key: k, .. } => {
            assert_eq!(k, &key);
        }
        _ => panic!("Expected CASConflict"),
    }
}
