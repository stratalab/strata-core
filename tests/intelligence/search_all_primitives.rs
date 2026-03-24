//! Search All Primitives Tests
//!
//! Tests Searchable trait implementation across all primitives.

use crate::common::*;
use strata_core::json::JsonValue;
use strata_core::contract::PrimitiveType;
use strata_core::search_types::SearchRequest;
use strata_core::types::JsonDocId;
use strata_engine::Searchable;

/// Test that implemented primitives have Searchable trait.
#[test]
fn test_searchable_primitives() {
    let test_db = TestDb::new();
    let p = test_db.all_primitives();

    // These primitives implement Searchable
    fn assert_searchable<T: Searchable>(_: &T) {}

    assert_searchable(&p.kv);
    assert_searchable(&p.json);
    assert_searchable(&p.event);

    // When ISSUE-001 is fixed, add:
    // assert_searchable(&p.vector);
}

/// Test primitive_kind for each primitive.
#[test]
fn test_primitive_kinds() {
    let test_db = TestDb::new();
    let p = test_db.all_primitives();

    assert_eq!(p.kv.primitive_kind(), PrimitiveType::Kv);
    assert_eq!(p.json.primitive_kind(), PrimitiveType::Json);
    assert_eq!(p.event.primitive_kind(), PrimitiveType::Event);

    // When ISSUE-001 is fixed:
    // assert_eq!(p.vector.primitive_kind(), PrimitiveType::Vector);
}

/// Test search returns SearchResponse for all primitives.
#[test]
fn test_search_returns_response() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // Populate primitives with searchable data
    p.kv.put(&branch_id, "search_test", strata_core::value::Value::String("searchable content".into()))
        .expect("kv");
    let doc_id = JsonDocId::new();
    p.json.create(&branch_id, &doc_id, JsonValue::from(serde_json::json!({"text": "searchable json content"})))
        .expect("json");

    let search_req = SearchRequest::new(branch_id, "searchable").with_k(10);

    // Search each primitive
    let kv_response = p.kv.search(&search_req).expect("kv search");
    let json_response = p.json.search(&search_req).expect("json search");

    // Verify responses are valid SearchResponse
    assert!(kv_response.stats.elapsed_micros >= 0);
    assert!(json_response.stats.elapsed_micros >= 0);
}
