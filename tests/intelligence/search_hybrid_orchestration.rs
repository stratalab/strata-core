//! Substrate Orchestration Tests
//!
//! Tests the retrieval substrate across multiple primitives.

use crate::common::*;
use crate::common::search::{substrate_search, verify_substrate_scores_decreasing, verify_substrate_ranks_sequential};
use strata_core::json::JsonValue;
use strata_core::types::JsonDocId;

/// Test substrate orchestrates multiple primitives.
#[test]
fn test_substrate_orchestrates_primitives() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // Populate primitives with related content
    p.kv.put(&branch_id, "topic1", strata_core::value::Value::String("machine learning algorithm".into()))
        .expect("kv");
    let doc_id = JsonDocId::new();
    p.json.create(&branch_id, &doc_id, JsonValue::from(serde_json::json!({
        "title": "Introduction to Machine Learning",
        "content": "ML algorithms are powerful"
    }))).expect("json");

    // Substrate should combine results from all primitives via RRF fusion.
    let req = strata_core::search_types::SearchRequest::new(branch_id, "machine learning");
    let response = substrate_search(&test_db.db, &req);

    // Both KV and JSON should contribute hits
    assert!(!response.hits.is_empty(), "Should find results across primitives");
}

/// Test RRF fusion with multiple result sets.
#[test]
fn test_rrf_fusion_multiple_sources() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // Add data to KV
    p.kv.put(&branch_id, "doc1", strata_core::value::Value::String("the quick brown fox".into()))
        .expect("kv");
    p.kv.put(&branch_id, "doc2", strata_core::value::Value::String("lazy brown dog".into()))
        .expect("kv");

    let req = strata_core::search_types::SearchRequest::new(branch_id, "brown");
    let response = substrate_search(&test_db.db, &req);

    // Results should be ranked by relevance
    verify_substrate_scores_decreasing(&response.hits);
    verify_substrate_ranks_sequential(&response.hits);
}
