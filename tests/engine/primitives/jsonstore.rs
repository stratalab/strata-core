//! JsonStore Primitive Tests
//!
//! Tests for JSON document storage with path-based operations.

use crate::common::*;
use std::str::FromStr;

// Helper function to parse path or return root
fn jpath(s: &str) -> JsonPath {
    if s.is_empty() {
        JsonPath::root()
    } else {
        JsonPath::from_str(s).expect("valid path")
    }
}

// ============================================================================
// Basic CRUD
// ============================================================================

#[test]
fn create_and_get() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc = serde_json::json!({"name": "test", "value": 42});
    json.create(&test_db.branch_id, "default", "doc1", doc.clone().into())
        .unwrap();

    let result = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap();
    assert!(result.is_some());
    // Compare the inner value
    let result_json: serde_json::Value = result.unwrap().into();
    assert_eq!(result_json, doc);
}

#[test]
fn create_fails_if_exists() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc: JsonValue = serde_json::json!({"x": 1}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc.clone())
        .unwrap();

    let result = json.create(&test_db.branch_id, "default", "doc1", doc);
    assert!(result.is_err());
}

#[test]
fn get_nonexistent_returns_none() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let result = json
        .get(
            &test_db.branch_id,
            "default",
            "nonexistent",
            &JsonPath::root(),
        )
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn exists_returns_correct_status() {
    let test_db = TestDb::new();
    let json = test_db.json();

    assert!(!json.exists(&test_db.branch_id, "default", "doc1").unwrap());

    json.create(
        &test_db.branch_id,
        "default",
        "doc1",
        serde_json::json!({}).into(),
    )
    .unwrap();
    assert!(json.exists(&test_db.branch_id, "default", "doc1").unwrap());
}

#[test]
fn destroy_removes_document() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create(
        &test_db.branch_id,
        "default",
        "doc1",
        serde_json::json!({}).into(),
    )
    .unwrap();
    assert!(json.exists(&test_db.branch_id, "default", "doc1").unwrap());

    let destroyed = json.destroy(&test_db.branch_id, "default", "doc1").unwrap();
    assert!(destroyed);

    assert!(!json.exists(&test_db.branch_id, "default", "doc1").unwrap());
}

#[test]
fn destroy_nonexistent_returns_false() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let destroyed = json
        .destroy(&test_db.branch_id, "default", "nonexistent")
        .unwrap();
    assert!(!destroyed);
}

// ============================================================================
// Path Operations
// ============================================================================

#[test]
fn get_at_path() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc = serde_json::json!({
        "user": {
            "name": "Alice",
            "age": 30
        }
    });
    json.create(&test_db.branch_id, "default", "doc1", doc.into())
        .unwrap();

    let name = json
        .get(&test_db.branch_id, "default", "doc1", &jpath("user.name"))
        .unwrap();
    assert!(name.is_some());
    let name_val: serde_json::Value = name.unwrap().into();
    assert_eq!(name_val, serde_json::json!("Alice"));

    let age = json
        .get(&test_db.branch_id, "default", "doc1", &jpath("user.age"))
        .unwrap();
    assert!(age.is_some());
    let age_val: serde_json::Value = age.unwrap().into();
    assert_eq!(age_val, serde_json::json!(30));
}

#[test]
fn set_at_path() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc = serde_json::json!({"x": 1});
    json.create(&test_db.branch_id, "default", "doc1", doc.into())
        .unwrap();

    json.set(
        &test_db.branch_id,
        "default",
        "doc1",
        &jpath("y"),
        serde_json::json!(2).into(),
    )
    .unwrap();

    let result = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap()
        .unwrap();
    let result_json: serde_json::Value = result.into();
    assert_eq!(result_json["x"], serde_json::json!(1));
    assert_eq!(result_json["y"], serde_json::json!(2));
}

#[test]
fn set_nested_path() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc = serde_json::json!({"a": {"b": 1}});
    json.create(&test_db.branch_id, "default", "doc1", doc.into())
        .unwrap();

    json.set(
        &test_db.branch_id,
        "default",
        "doc1",
        &jpath("a.c"),
        serde_json::json!(2).into(),
    )
    .unwrap();

    let result = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap()
        .unwrap();
    let result_json: serde_json::Value = result.into();
    assert_eq!(result_json["a"]["b"], serde_json::json!(1));
    assert_eq!(result_json["a"]["c"], serde_json::json!(2));
}

#[test]
fn delete_at_path() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc = serde_json::json!({"x": 1, "y": 2});
    json.create(&test_db.branch_id, "default", "doc1", doc.into())
        .unwrap();

    json.delete_at_path(&test_db.branch_id, "default", "doc1", &jpath("y"))
        .unwrap();

    let result = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap()
        .unwrap();
    let result_json: serde_json::Value = result.into();
    assert_eq!(result_json, serde_json::json!({"x": 1}));
}

// ============================================================================
// Merge Operations
// ============================================================================

#[test]
#[ignore = "requires: JsonStore::merge"]
fn merge_adds_new_fields() {
    let _test_db = TestDb::new();
    // JSON merge is an architectural principle
    // but merge() is not yet in the MVP API
}

#[test]
#[ignore = "requires: JsonStore::merge"]
fn merge_overwrites_existing_fields() {
    let _test_db = TestDb::new();
    // JSON merge is an architectural principle
    // but merge() is not yet in the MVP API
}

#[test]
#[ignore = "requires: JsonStore::merge"]
fn merge_null_removes_field() {
    let _test_db = TestDb::new();
    // JSON merge is an architectural principle
    // but merge() is not yet in the MVP API
}

// ============================================================================
// CAS Operations
// ============================================================================

#[test]
#[ignore = "requires: JsonStore::cas"]
fn cas_succeeds_with_correct_version() {
    let _test_db = TestDb::new();
    // OCC on JSON is an architectural principle
    // but cas() is not yet in the MVP API
}

#[test]
#[ignore = "requires: JsonStore::cas"]
fn cas_fails_with_wrong_version() {
    let _test_db = TestDb::new();
    // OCC on JSON is an architectural principle
    // but cas() is not yet in the MVP API
}

// ============================================================================
// List & Count
// ============================================================================

#[test]
fn list_returns_all_documents() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create(
        &test_db.branch_id,
        "default",
        "doc1",
        serde_json::json!({}).into(),
    )
    .unwrap();
    json.create(
        &test_db.branch_id,
        "default",
        "doc2",
        serde_json::json!({}).into(),
    )
    .unwrap();
    json.create(
        &test_db.branch_id,
        "default",
        "doc3",
        serde_json::json!({}).into(),
    )
    .unwrap();

    let docs = json
        .list(&test_db.branch_id, "default", None, None, 100)
        .unwrap();
    assert_eq!(docs.doc_ids.len(), 3);
}

#[test]
fn count_returns_document_count() {
    let test_db = TestDb::new();
    let json = test_db.json();

    // count rewritten as list().doc_ids.len()
    assert_eq!(
        json.list(&test_db.branch_id, "default", None, None, 1000)
            .unwrap()
            .doc_ids
            .len(),
        0
    );

    json.create(
        &test_db.branch_id,
        "default",
        "doc1",
        serde_json::json!({}).into(),
    )
    .unwrap();
    json.create(
        &test_db.branch_id,
        "default",
        "doc2",
        serde_json::json!({}).into(),
    )
    .unwrap();

    assert_eq!(
        json.list(&test_db.branch_id, "default", None, None, 1000)
            .unwrap()
            .doc_ids
            .len(),
        2
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_document() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create(
        &test_db.branch_id,
        "default",
        "doc1",
        serde_json::json!({}).into(),
    )
    .unwrap();

    let result = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap()
        .unwrap();
    let result_json: serde_json::Value = result.into();
    assert_eq!(result_json, serde_json::json!({}));
}

#[test]
fn deeply_nested_document() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc = serde_json::json!({
        "a": {"b": {"c": {"d": {"e": 42}}}}
    });
    json.create(&test_db.branch_id, "default", "doc1", doc.into())
        .unwrap();

    let result = json
        .get(&test_db.branch_id, "default", "doc1", &jpath("a.b.c.d.e"))
        .unwrap();
    assert!(result.is_some());
    let result_json: serde_json::Value = result.unwrap().into();
    assert_eq!(result_json, serde_json::json!(42));
}

#[test]
fn various_json_types() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let doc = serde_json::json!({
        "string": "hello",
        "number": 42,
        "float": 3.125,
        "bool": true,
        "null": null,
        "array": [1, 2, 3],
        "object": {"nested": true}
    });
    json.create(&test_db.branch_id, "default", "doc1", doc.clone().into())
        .unwrap();

    let result = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap()
        .unwrap();
    let result_json: serde_json::Value = result.into();
    assert_eq!(result_json, doc);
}
