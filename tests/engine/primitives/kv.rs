//! KVStore Primitive Tests
//!
//! Tests for key-value storage operations.

use crate::common::*;

// ============================================================================
// Basic CRUD
// ============================================================================

#[test]
fn get_nonexistent_returns_none() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let result = kv
        .get(&test_db.branch_id, "default", "nonexistent")
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn put_and_get() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "key", Value::Int(42))
        .unwrap();

    let result = kv.get(&test_db.branch_id, "default", "key").unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), Value::Int(42));
}

#[test]
fn put_returns_version() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let version = kv
        .put(&test_db.branch_id, "default", "key", Value::Int(1))
        .unwrap();
    assert!(version.as_u64() > 0);
}

#[test]
fn put_overwrites_value() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "key", Value::Int(1))
        .unwrap();
    kv.put(&test_db.branch_id, "default", "key", Value::Int(2))
        .unwrap();

    let result = kv.get(&test_db.branch_id, "default", "key").unwrap();
    assert_eq!(result.unwrap(), Value::Int(2));
}

#[test]
fn put_increments_version() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let v1 = kv
        .put(&test_db.branch_id, "default", "key", Value::Int(1))
        .unwrap();
    let v2 = kv
        .put(&test_db.branch_id, "default", "key", Value::Int(2))
        .unwrap();

    assert!(v2.as_u64() > v1.as_u64());
}

#[test]
fn delete_existing_returns_true() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "key", Value::Int(42))
        .unwrap();

    let deleted = kv.delete(&test_db.branch_id, "default", "key").unwrap();
    assert!(deleted);

    let result = kv.get(&test_db.branch_id, "default", "key").unwrap();
    assert!(result.is_none());
}

#[test]
fn delete_nonexistent_returns_false() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let deleted = kv
        .delete(&test_db.branch_id, "default", "nonexistent")
        .unwrap();
    assert!(!deleted);
}

#[test]
fn exists_returns_correct_status() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    // exists via get().is_some()
    assert!(kv
        .get(&test_db.branch_id, "default", "key")
        .unwrap()
        .is_none());

    kv.put(&test_db.branch_id, "default", "key", Value::Int(1))
        .unwrap();
    assert!(kv
        .get(&test_db.branch_id, "default", "key")
        .unwrap()
        .is_some());

    kv.delete(&test_db.branch_id, "default", "key").unwrap();
    assert!(kv
        .get(&test_db.branch_id, "default", "key")
        .unwrap()
        .is_none());
}

// ============================================================================
// List Operations
// ============================================================================

#[test]
fn list_empty_returns_empty() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let keys = kv.list(&test_db.branch_id, "default", None).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn list_returns_all_keys() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "a", Value::Int(1))
        .unwrap();
    kv.put(&test_db.branch_id, "default", "b", Value::Int(2))
        .unwrap();
    kv.put(&test_db.branch_id, "default", "c", Value::Int(3))
        .unwrap();

    let keys = kv.list(&test_db.branch_id, "default", None).unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"a".to_string()));
    assert!(keys.contains(&"b".to_string()));
    assert!(keys.contains(&"c".to_string()));
}

#[test]
fn list_with_prefix_filters() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "user:1", Value::Int(1))
        .unwrap();
    kv.put(&test_db.branch_id, "default", "user:2", Value::Int(2))
        .unwrap();
    kv.put(&test_db.branch_id, "default", "item:1", Value::Int(3))
        .unwrap();

    let user_keys = kv
        .list(&test_db.branch_id, "default", Some("user:"))
        .unwrap();
    assert_eq!(user_keys.len(), 2);

    let item_keys = kv
        .list(&test_db.branch_id, "default", Some("item:"))
        .unwrap();
    assert_eq!(item_keys.len(), 1);

    let no_match = kv
        .list(&test_db.branch_id, "default", Some("other:"))
        .unwrap();
    assert!(no_match.is_empty());
}

// ============================================================================
// Value Types
// ============================================================================

#[test]
fn supports_int_values() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "int", Value::Int(42))
        .unwrap();
    let result = kv.get(&test_db.branch_id, "default", "int").unwrap();
    assert_eq!(result.unwrap(), Value::Int(42));
}

#[test]
fn supports_string_values() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(
        &test_db.branch_id,
        "default",
        "str",
        Value::String("hello".into()),
    )
    .unwrap();
    let result = kv.get(&test_db.branch_id, "default", "str").unwrap();
    assert_eq!(result.unwrap(), Value::String("hello".into()));
}

#[test]
fn supports_bool_values() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "bool", Value::Bool(true))
        .unwrap();
    let result = kv.get(&test_db.branch_id, "default", "bool").unwrap();
    assert_eq!(result.unwrap(), Value::Bool(true));
}

#[test]
fn supports_float_values() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "float", Value::Float(3.125))
        .unwrap();
    let result = kv.get(&test_db.branch_id, "default", "float").unwrap();

    match result.unwrap() {
        Value::Float(f) => assert!((f - 3.125).abs() < 0.001),
        _ => panic!("Expected float"),
    }
}

#[test]
fn supports_bytes_values() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(
        &test_db.branch_id,
        "default",
        "bytes",
        Value::Bytes(vec![1, 2, 3]),
    )
    .unwrap();
    let result = kv.get(&test_db.branch_id, "default", "bytes").unwrap();
    assert_eq!(result.unwrap(), Value::Bytes(vec![1, 2, 3]));
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_string_key_works() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "", Value::Int(1))
        .unwrap();
    let result = kv.get(&test_db.branch_id, "default", "").unwrap();
    assert_eq!(result, Some(Value::Int(1)));
}

#[test]
fn long_key_works() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let long_key = "k".repeat(1000);
    kv.put(&test_db.branch_id, "default", &long_key, Value::Int(1))
        .unwrap();
    let result = kv.get(&test_db.branch_id, "default", &long_key).unwrap();
    assert_eq!(result, Some(Value::Int(1)));
}

#[test]
fn special_characters_in_key() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let special_key = "key/with:special@chars#and$symbols";
    kv.put(&test_db.branch_id, "default", special_key, Value::Int(1))
        .unwrap();
    let result = kv.get(&test_db.branch_id, "default", special_key).unwrap();
    assert_eq!(result, Some(Value::Int(1)));
}

// ============================================================================
// Batch Get
// ============================================================================

#[test]
fn batch_get_returns_values_in_order() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "a", Value::Int(1)).unwrap();
    kv.put(&test_db.branch_id, "default", "b", Value::Int(2)).unwrap();
    kv.put(&test_db.branch_id, "default", "c", Value::Int(3)).unwrap();

    let keys = vec!["c".to_string(), "a".to_string(), "b".to_string()];
    let results = kv.batch_get(&test_db.branch_id, "default", &keys).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap().value, Value::Int(3));
    assert_eq!(results[1].as_ref().unwrap().value, Value::Int(1));
    assert_eq!(results[2].as_ref().unwrap().value, Value::Int(2));
}

#[test]
fn batch_get_missing_keys_return_none() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "exists", Value::Int(1)).unwrap();

    let keys = vec!["exists".to_string(), "missing".to_string()];
    let results = kv.batch_get(&test_db.branch_id, "default", &keys).unwrap();

    assert_eq!(results.len(), 2);
    assert!(results[0].is_some());
    assert!(results[1].is_none());
}

#[test]
fn batch_get_empty_keys_returns_empty() {
    let test_db = TestDb::new();
    let kv = test_db.kv();
    let results = kv.batch_get(&test_db.branch_id, "default", &[]).unwrap();
    assert!(results.is_empty());
}

// ============================================================================
// Batch Delete
// ============================================================================

#[test]
fn batch_delete_returns_existed_flags() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "a", Value::Int(1)).unwrap();
    kv.put(&test_db.branch_id, "default", "b", Value::Int(2)).unwrap();

    let keys = vec!["a".to_string(), "missing".to_string(), "b".to_string()];
    let results = kv.batch_delete(&test_db.branch_id, "default", &keys).unwrap();

    assert_eq!(results, vec![true, false, true]);
}

#[test]
fn batch_delete_actually_removes_keys() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "x", Value::Int(10)).unwrap();
    kv.put(&test_db.branch_id, "default", "y", Value::Int(20)).unwrap();

    let keys = vec!["x".to_string(), "y".to_string()];
    kv.batch_delete(&test_db.branch_id, "default", &keys).unwrap();

    assert!(kv.get(&test_db.branch_id, "default", "x").unwrap().is_none());
    assert!(kv.get(&test_db.branch_id, "default", "y").unwrap().is_none());
}

#[test]
fn batch_delete_empty_keys_returns_empty() {
    let test_db = TestDb::new();
    let kv = test_db.kv();
    let results = kv.batch_delete(&test_db.branch_id, "default", &[]).unwrap();
    assert!(results.is_empty());
}

// ============================================================================
// Batch Exists
// ============================================================================

#[test]
fn batch_exists_returns_correct_flags() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "a", Value::Int(1)).unwrap();
    kv.put(&test_db.branch_id, "default", "c", Value::Int(3)).unwrap();

    let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let results = kv.batch_exists(&test_db.branch_id, "default", &keys).unwrap();

    assert_eq!(results, vec![true, false, true]);
}

#[test]
fn batch_exists_empty_keys_returns_empty() {
    let test_db = TestDb::new();
    let kv = test_db.kv();
    let results = kv.batch_exists(&test_db.branch_id, "default", &[]).unwrap();
    assert!(results.is_empty());
}

#[test]
fn batch_get_returns_version_and_timestamp() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "key1", Value::Int(1)).unwrap();

    let keys = vec!["key1".to_string()];
    let results = kv.batch_get(&test_db.branch_id, "default", &keys).unwrap();

    let vv = results[0].as_ref().unwrap();
    assert_eq!(vv.value, Value::Int(1));
    assert!(vv.version.as_u64() > 0, "version should be non-zero");
    assert!(u64::from(vv.timestamp) > 0, "timestamp should be non-zero");
}

#[test]
fn batch_delete_then_batch_exists_returns_false() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "a", Value::Int(1)).unwrap();
    kv.put(&test_db.branch_id, "default", "b", Value::Int(2)).unwrap();

    let keys = vec!["a".to_string(), "b".to_string()];
    kv.batch_delete(&test_db.branch_id, "default", &keys).unwrap();

    let exists = kv.batch_exists(&test_db.branch_id, "default", &keys).unwrap();
    assert_eq!(exists, vec![false, false]);
}

#[test]
fn batch_get_duplicate_keys_returns_same_value() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "dup", Value::Int(42)).unwrap();

    let keys = vec!["dup".to_string(), "dup".to_string()];
    let results = kv.batch_get(&test_db.branch_id, "default", &keys).unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().value, Value::Int(42));
    assert_eq!(results[1].as_ref().unwrap().value, Value::Int(42));
}

#[test]
fn batch_delete_duplicate_key_first_true_second_false() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    kv.put(&test_db.branch_id, "default", "dup", Value::Int(1)).unwrap();

    let keys = vec!["dup".to_string(), "dup".to_string()];
    let results = kv.batch_delete(&test_db.branch_id, "default", &keys).unwrap();

    // First delete finds the key, second sees it already deleted within the same txn
    assert_eq!(results, vec![true, false]);
}
