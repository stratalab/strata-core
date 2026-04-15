//! Tier 2: Search Correctness
//!
//! Validates search determinism, exhaustiveness, and filter behavior.

use crate::common::*;
use strata_core::search_types::{PrimitiveType, SearchRequest};
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::KVStore;
use crate::common::search::{substrate_search, verify_substrate_scores_decreasing};

// ============================================================================
// Determinism Tests
// ============================================================================

/// Same request produces identical results
#[test]
fn test_tier2_search_deterministic() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    verify_deterministic(&db, &req);
}

/// Primitive search is deterministic
#[test]
fn test_tier2_primitive_search_deterministic() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");

    // Execute same search multiple times
    let results: Vec<_> = (0..5).map(|_| kv.search(&req).unwrap()).collect();

    // All results should be identical
    for (i, result) in results.iter().enumerate().skip(1) {
        assert_eq!(
            result.hits.len(),
            results[0].hits.len(),
            "Iteration {} should have same hit count",
            i
        );

        for (h1, h2) in result.hits.iter().zip(results[0].hits.iter()) {
            assert_eq!(h1.doc_ref, h2.doc_ref, "DocRefs should match");
            assert_eq!(h1.rank, h2.rank, "Ranks should match");
        }
    }
}

/// Substrate search is deterministic
#[test]
fn test_tier2_substrate_search_deterministic() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");

    let r1 = substrate_search(&db, &req);
    let r2 = substrate_search(&db, &req);

    assert_eq!(r1.hits.len(), r2.hits.len());
    for (h1, h2) in r1.hits.iter().zip(r2.hits.iter()) {
        assert_eq!(h1.doc_ref, h2.doc_ref);
    }
}

// ============================================================================
// Branch Isolation Tests
// ============================================================================

/// Search respects branch_id filter
#[test]
fn test_tier2_search_respects_branch_id() {
    let db = create_test_db();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    let kv = KVStore::new(db.clone());

    db.branches().create(&branch1.to_string()).unwrap();
    db.branches().create(&branch2.to_string()).unwrap();

    // Add shared term to both branches
    kv.put(&branch1, "key1", Value::String("shared test term".into()))
        .unwrap();
    kv.put(&branch2, "key2", Value::String("shared test term".into()))
        .unwrap();

    // Search branch1 only
    let req = SearchRequest::new(branch1, "shared");
    let response = kv.search(&req).unwrap();

    // All results should belong to branch1
    assert_all_from_branch(&response, branch1);
}

/// Branch isolation between different branches
#[test]
fn test_tier2_branch_isolation() {
    let db = create_test_db();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    let kv = KVStore::new(db.clone());

    db.branches().create(&branch1.to_string()).unwrap();
    db.branches().create(&branch2.to_string()).unwrap();

    // Add same key with different values to different branches
    kv.put(&branch1, "key", Value::String("branch1 test value".into()))
        .unwrap();
    kv.put(&branch2, "key", Value::String("branch2 test value".into()))
        .unwrap();

    // Search branch1
    let req1 = SearchRequest::new(branch1, "test");
    let r1 = kv.search(&req1).unwrap();

    // Search branch2
    let req2 = SearchRequest::new(branch2, "test");
    let r2 = kv.search(&req2).unwrap();

    // Results should be isolated
    assert_all_from_branch(&r1, branch1);
    assert_all_from_branch(&r2, branch2);
}

/// Non-existent branch returns empty results
#[test]
fn test_tier2_nonexistent_branch_empty() {
    let db = create_test_db();
    let branch_id = BranchId::new();

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    assert!(
        response.hits.is_empty(),
        "Non-existent branch should return empty results"
    );
}

// ============================================================================
// Primitive Filter Tests
// ============================================================================

/// Primitive filter limits search scope
#[test]
fn test_tier2_primitive_filter_works() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    // Search only KV primitive
    let req = SearchRequest::new(branch_id, "test").with_primitive_filter(vec![PrimitiveType::Kv]);
    let response = substrate_search(&db, &req);

    // All results should be from KV only
    for hit in &response.hits {
        assert_eq!(hit.doc_ref.primitive_type(), PrimitiveType::Kv);
    }
}

/// Empty primitive filter means no results
#[test]
fn test_tier2_empty_filter_no_results() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    // Search with empty filter
    let req = SearchRequest::new(branch_id, "test").with_primitive_filter(vec![]);
    let response = substrate_search(&db, &req);

    assert!(
        response.hits.is_empty(),
        "Empty filter should produce no results"
    );
}

// ============================================================================
// Result Ordering Tests
// ============================================================================

/// Scores are monotonically decreasing
#[test]
fn test_tier2_scores_monotonically_decreasing() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test").with_k(20);
    let response = substrate_search(&db, &req);

    verify_substrate_scores_decreasing(&response.hits);
}

/// Ranks are sequential starting from 1
#[test]
fn test_tier2_ranks_sequential() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test").with_k(10);
    let response = kv.search(&req).unwrap();

    verify_ranks_sequential(&response);
}

/// Top-k respects k parameter
#[test]
fn test_tier2_respects_k_parameter() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_large_dataset(&db, &branch_id, 50);

    let kv = KVStore::new(db.clone());

    // Search with k=5
    let req = SearchRequest::new(branch_id, "searchable").with_k(5);
    let response = kv.search(&req).unwrap();

    assert!(response.hits.len() <= 5, "Should respect k parameter");
}

/// Smaller k is prefix of larger k
#[test]
fn test_tier2_consistent_across_k_values() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req_k3 = SearchRequest::new(branch_id, "test").with_k(3);
    let req_k10 = SearchRequest::new(branch_id, "test").with_k(10);

    let r3 = substrate_search(&db, &req_k3);
    let r10 = substrate_search(&db, &req_k10);

    // Smaller k results should be prefix of larger k results
    for (i, hit) in r3.hits.iter().enumerate() {
        if i < r10.hits.len() {
            assert_eq!(
                hit.doc_ref, r10.hits[i].doc_ref,
                "Top-3 should be prefix of top-10"
            );
        }
    }
}
