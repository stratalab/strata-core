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

// ============================================================================
// Secondary Index Tests
// ============================================================================

use strata_engine::primitives::json::index::IndexType;

#[test]
fn create_index_and_list() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let def = json
        .create_index(
            &test_db.branch_id,
            "default",
            "price_idx",
            "$.price",
            IndexType::Numeric,
        )
        .unwrap();
    assert_eq!(def.name, "price_idx");
    assert_eq!(def.field_path, "$.price");
    assert_eq!(def.index_type, IndexType::Numeric);

    let indexes = json.list_indexes(&test_db.branch_id, "default").unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "price_idx");
}

#[test]
fn create_index_duplicate_fails() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "idx1",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();
    let result = json.create_index(
        &test_db.branch_id,
        "default",
        "idx1",
        "$.name",
        IndexType::Text,
    );
    assert!(result.is_err());
}

#[test]
fn drop_index_removes_metadata_and_entries() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    // Write a document so index entries exist
    let doc: JsonValue = serde_json::json!({"price": 29.99}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    // Drop the index
    let existed = json
        .drop_index(&test_db.branch_id, "default", "price_idx")
        .unwrap();
    assert!(existed);

    // Verify metadata removed
    let indexes = json.list_indexes(&test_db.branch_id, "default").unwrap();
    assert!(indexes.is_empty());

    // Drop again returns false
    let existed = json
        .drop_index(&test_db.branch_id, "default", "price_idx")
        .unwrap();
    assert!(!existed);
}

#[test]
fn index_entries_created_on_document_write() {
    let test_db = TestDb::new();
    let json = test_db.json();

    // Create index before writing documents
    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    // Write documents
    let doc1: JsonValue = serde_json::json!({"price": 10.0, "name": "widget"}).into();
    let doc2: JsonValue = serde_json::json!({"price": 50.0, "name": "gadget"}).into();
    let doc3: JsonValue = serde_json::json!({"price": 30.0, "name": "thing"}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc1)
        .unwrap();
    json.create(&test_db.branch_id, "default", "doc2", doc2)
        .unwrap();
    json.create(&test_db.branch_id, "default", "doc3", doc3)
        .unwrap();

    // Verify index entries by scanning the index space
    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();

    // Should have 3 index entries
    assert_eq!(entries.len(), 3);

    // Entries should be in numeric order (10.0 < 30.0 < 50.0)
    let doc_ids: Vec<String> = entries
        .iter()
        .filter_map(|(k, _)| {
            strata_engine::primitives::json::index::extract_doc_id_from_index_key(&k.user_key)
        })
        .collect();
    assert_eq!(doc_ids, vec!["doc1", "doc3", "doc2"]); // sorted by price
}

#[test]
fn index_entries_updated_on_document_update() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    // Create doc with price 10.0
    let doc: JsonValue = serde_json::json!({"price": 10.0}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    // Update price to 50.0
    let new_val: JsonValue = serde_json::json!(50.0).into();
    json.set(
        &test_db.branch_id,
        "default",
        "doc1",
        &jpath("price"),
        new_val,
    )
    .unwrap();

    // Scan index — should have exactly 1 entry (old removed, new added)
    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 1);

    // The entry should point to doc1 with the new price
    let doc_id = strata_engine::primitives::json::index::extract_doc_id_from_index_key(
        &entries[0].0.user_key,
    );
    assert_eq!(doc_id, Some("doc1".to_string()));
}

#[test]
fn index_entries_removed_on_document_destroy() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    let doc: JsonValue = serde_json::json!({"price": 42.0}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    // Verify index entry exists
    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 1);

    // Destroy the document
    json.destroy(&test_db.branch_id, "default", "doc1").unwrap();

    // Index entry should be gone
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn index_missing_field_no_entry() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    // Write a document WITHOUT a price field
    let doc: JsonValue = serde_json::json!({"name": "no-price"}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    // No index entry should be created
    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn index_type_mismatch_no_entry() {
    let test_db = TestDb::new();
    let json = test_db.json();

    // Numeric index on price field
    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    // Write a document with a STRING price (type mismatch for numeric index)
    let doc: JsonValue = serde_json::json!({"price": "expensive"}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    // No index entry — string can't be encoded as numeric
    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn index_tag_type_exact_match() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "status_idx",
        "$.status",
        IndexType::Tag,
    )
    .unwrap();

    let doc1: JsonValue = serde_json::json!({"status": "Active", "name": "a"}).into();
    let doc2: JsonValue = serde_json::json!({"status": "PENDING", "name": "b"}).into();
    let doc3: JsonValue = serde_json::json!({"status": "active", "name": "c"}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc1)
        .unwrap();
    json.create(&test_db.branch_id, "default", "doc2", doc2)
        .unwrap();
    json.create(&test_db.branch_id, "default", "doc3", doc3)
        .unwrap();

    // Tags are lowercased, so "Active" and "active" produce same index value
    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "status_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 3);

    // Should sort: "active" (doc1), "active" (doc3), "pending" (doc2)
    let doc_ids: Vec<String> = entries
        .iter()
        .filter_map(|(k, _)| {
            strata_engine::primitives::json::index::extract_doc_id_from_index_key(&k.user_key)
        })
        .collect();
    assert_eq!(doc_ids, vec!["doc1", "doc3", "doc2"]);
}

#[test]
fn index_branch_isolation() {
    let mut test_db = TestDb::new();
    let json = test_db.json();
    let branch_a = test_db.branch_id;

    // Create index on branch A
    json.create_index(
        &branch_a,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    // Create a new branch B
    let branch_b = test_db.new_branch();

    // Index should NOT be visible on branch B
    let indexes_b = json.list_indexes(&branch_b, "default").unwrap();
    assert!(indexes_b.is_empty());

    // Index should still be on branch A
    let indexes_a = json.list_indexes(&branch_a, "default").unwrap();
    assert_eq!(indexes_a.len(), 1);
}

#[test]
fn index_multiple_indexes_on_same_space() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();
    json.create_index(
        &test_db.branch_id,
        "default",
        "status_idx",
        "$.status",
        IndexType::Tag,
    )
    .unwrap();

    let indexes = json.list_indexes(&test_db.branch_id, "default").unwrap();
    assert_eq!(indexes.len(), 2);

    // Write a document — both indexes should be updated
    let doc: JsonValue = serde_json::json!({"price": 25.0, "status": "active"}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    // Check price index
    let price_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let price_entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&price_prefix))
        .unwrap();
    assert_eq!(price_entries.len(), 1);

    // Check status index
    let status_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "status_idx",
    );
    let status_entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&status_prefix))
        .unwrap();
    assert_eq!(status_entries.len(), 1);
}

#[test]
fn index_batch_set_updates_indexes() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    let entries = vec![
        (
            "doc1".to_string(),
            jpath(""),
            serde_json::json!({"price": 10.0}).into(),
        ),
        (
            "doc2".to_string(),
            jpath(""),
            serde_json::json!({"price": 20.0}).into(),
        ),
        (
            "doc3".to_string(),
            jpath(""),
            serde_json::json!({"price": 30.0}).into(),
        ),
    ];
    json.batch_set_or_create(&test_db.branch_id, "default", entries)
        .unwrap();

    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let index_entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(index_entries.len(), 3);
}

#[test]
fn index_batch_destroy_removes_entries() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    let doc1: JsonValue = serde_json::json!({"price": 10.0}).into();
    let doc2: JsonValue = serde_json::json!({"price": 20.0}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc1)
        .unwrap();
    json.create(&test_db.branch_id, "default", "doc2", doc2)
        .unwrap();

    // Batch destroy both
    json.batch_destroy(
        &test_db.branch_id,
        "default",
        &["doc1".to_string(), "doc2".to_string()],
    )
    .unwrap();

    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn index_delete_at_path_updates_entries() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();

    let doc: JsonValue = serde_json::json!({"price": 42.0, "name": "widget"}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    // Verify index entry exists
    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "price_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 1);

    // Delete the price field from the document
    json.delete_at_path(&test_db.branch_id, "default", "doc1", &jpath("price"))
        .unwrap();

    // Index entry for price should be gone (field no longer exists)
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn no_indexes_no_overhead() {
    // Verify that without indexes, documents still work normally
    let test_db = TestDb::new();
    let json = test_db.json();

    // No indexes created — just normal CRUD
    let doc: JsonValue = serde_json::json!({"price": 42.0}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    let val = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap();
    assert!(val.is_some());

    json.destroy(&test_db.branch_id, "default", "doc1").unwrap();
    let val = json
        .get(&test_db.branch_id, "default", "doc1", &JsonPath::root())
        .unwrap();
    assert!(val.is_none());
}

#[test]
fn index_numeric_handles_integers() {
    // JSON integers (42) should be indexed correctly via as_f64()
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "qty_idx",
        "$.quantity",
        IndexType::Numeric,
    )
    .unwrap();

    let doc: JsonValue = serde_json::json!({"quantity": 42}).into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "qty_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 1);
}

#[test]
fn index_nested_field_path() {
    let test_db = TestDb::new();
    let json = test_db.json();

    json.create_index(
        &test_db.branch_id,
        "default",
        "city_idx",
        "$.address.city",
        IndexType::Tag,
    )
    .unwrap();

    let doc: JsonValue =
        serde_json::json!({"name": "Alice", "address": {"city": "Portland", "zip": "97201"}})
            .into();
    json.create(&test_db.branch_id, "default", "doc1", doc)
        .unwrap();

    let scan_prefix = strata_engine::primitives::json::index::index_scan_prefix(
        &test_db.branch_id,
        "default",
        "city_idx",
    );
    let entries: Vec<_> = test_db
        .db
        .transaction(test_db.branch_id, |txn| txn.scan_prefix(&scan_prefix))
        .unwrap();
    assert_eq!(entries.len(), 1);

    let doc_id = strata_engine::primitives::json::index::extract_doc_id_from_index_key(
        &entries[0].0.user_key,
    );
    assert_eq!(doc_id, Some("doc1".to_string()));
}

#[test]
fn create_index_empty_name_rejected() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let result = json.create_index(
        &test_db.branch_id,
        "default",
        "",
        "$.price",
        IndexType::Numeric,
    );
    assert!(result.is_err());
}

#[test]
fn create_index_invalid_name_rejected() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let result = json.create_index(
        &test_db.branch_id,
        "default",
        "bad/name",
        "$.price",
        IndexType::Numeric,
    );
    assert!(result.is_err());
}
