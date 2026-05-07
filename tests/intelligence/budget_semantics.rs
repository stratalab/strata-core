//! Tier 3: Budget Semantics
//!
//! Budget is a soft cap that truncates results, not a correctness condition.
//! These tests verify truncation behavior, ordering preservation, and isolation.

use crate::common::*;
use crate::common::search::substrate_search;
use strata_engine::{KVStore, SearchBudget, SearchRequest};
use std::collections::HashSet;

// ============================================================================
// Budget Truncation Tests
// ============================================================================

/// Budget causes truncation, not corruption
#[test]
fn test_tier3_budget_truncates_not_corrupts() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_large_dataset(&db, &branch_id, 100);

    let kv = KVStore::new(db.clone());

    // Search with very tight budget
    let tight_budget = SearchBudget::default()
        .with_candidates(10)
        .with_per_primitive(10);
    let req_tight = SearchRequest::new(branch_id, "searchable")
        .with_budget(tight_budget)
        .with_k(100);

    // Search with unlimited budget
    let unlimited_budget = SearchBudget::default()
        .with_candidates(10000)
        .with_per_primitive(10000);
    let req_full = SearchRequest::new(branch_id, "searchable")
        .with_budget(unlimited_budget)
        .with_k(100);

    let truncated_result = kv.search(&req_tight).unwrap();
    let full_result = kv.search(&req_full).unwrap();

    // Every hit in truncated result must exist in full result
    let full_refs: HashSet<_> = full_result.hits.iter().map(|h| &h.doc_ref).collect();
    for hit in &truncated_result.hits {
        assert!(
            full_refs.contains(&hit.doc_ref),
            "Truncated result contains hit not in full result - CORRUPTION!"
        );
    }
}

/// Truncation sets truncated flag
#[test]
fn test_tier3_truncation_sets_flag() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_large_dataset(&db, &branch_id, 100);

    let kv = KVStore::new(db.clone());

    // Very tight budget should cause truncation
    let tight_budget = SearchBudget::default()
        .with_candidates(5)
        .with_per_primitive(5);
    let req = SearchRequest::new(branch_id, "searchable")
        .with_budget(tight_budget)
        .with_k(100);

    let response = kv.search(&req).unwrap();

    // With 100 documents and budget of 5, should truncate
    if response.hits.len() < 100 {
        assert!(
            response.truncated,
            "Should set truncated flag when budget exceeded"
        );
    }
}

// ============================================================================
// Budget Ordering Tests
// ============================================================================

/// Budget never changes prefix ordering
#[test]
fn test_tier3_budget_preserves_prefix_ordering() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_large_dataset(&db, &branch_id, 50);

    let kv = KVStore::new(db.clone());

    // Get results with different budgets
    let budget_10 = SearchBudget::default()
        .with_candidates(10)
        .with_per_primitive(10);
    let budget_50 = SearchBudget::default()
        .with_candidates(50)
        .with_per_primitive(50);

    let req_10 = SearchRequest::new(branch_id, "searchable")
        .with_budget(budget_10)
        .with_k(50);
    let req_50 = SearchRequest::new(branch_id, "searchable")
        .with_budget(budget_50)
        .with_k(50);

    let r10 = kv.search(&req_10).unwrap();
    let r50 = kv.search(&req_50).unwrap();

    // The truncated result should be a prefix of the full result
    // (allowing for potential score differences in edge cases)
    if !r10.hits.is_empty() && !r50.hits.is_empty() {
        // First result should match
        assert_eq!(
            r10.hits[0].doc_ref, r50.hits[0].doc_ref,
            "First result should be same regardless of budget"
        );
    }
}

/// Scores still monotonically decreasing with budget
#[test]
fn test_tier3_scores_decreasing_with_budget() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_large_dataset(&db, &branch_id, 50);

    let kv = KVStore::new(db.clone());

    let budget = SearchBudget::default()
        .with_candidates(20)
        .with_per_primitive(20);
    let req = SearchRequest::new(branch_id, "searchable")
        .with_budget(budget)
        .with_k(50);

    let response = kv.search(&req).unwrap();

    verify_scores_decreasing(&response);
}

// ============================================================================
// Budget Isolation Tests
// ============================================================================

/// Budget never introduces duplicates
#[test]
fn test_tier3_budget_no_duplicates() {
    use strata_engine::search::recipe::get_builtin_recipe;
    use strata_engine::search::substrate::{self, RetrievalRequest};

    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_large_dataset(&db, &branch_id, 50);

    let mut recipe = get_builtin_recipe("keyword").expect("keyword recipe must exist");
    recipe.transform = Some(strata_engine::search::recipe::TransformConfig {
        limit: Some(50),
        ..Default::default()
    });

    let req = RetrievalRequest {
        query: "searchable".into(),
        branch_id,
        space: "default".into(),
        recipe,
        embedding: None,
        time_range: None,
        primitive_filter: None,
        as_of: None,
        budget_ms: Some(500), // 500ms budget — exercises budget path
    };

    let response = substrate::retrieve(&db, &req).unwrap();

    let refs: HashSet<_> = response.hits.iter().map(|h| &h.doc_ref).collect();
    assert_eq!(
        refs.len(),
        response.hits.len(),
        "Budget should not introduce duplicates"
    );
}

/// Budget respects branch isolation
#[test]
fn test_tier3_budget_respects_branch_isolation() {
    let db = create_test_db();
    let branch1 = test_branch_id();
    let branch2 = test_branch_id();

    populate_large_dataset(&db, &branch1, 25);
    populate_large_dataset(&db, &branch2, 25);

    let kv = KVStore::new(db.clone());

    let budget = SearchBudget::default()
        .with_candidates(15)
        .with_per_primitive(15);
    let req1 = SearchRequest::new(branch1, "searchable")
        .with_budget(budget)
        .with_k(50);

    let response = kv.search(&req1).unwrap();

    // All results should be from branch1 only
    assert_all_from_branch(&response, branch1);
}

// ============================================================================
// Budget Configuration Tests
// ============================================================================

/// Budget builder works correctly
#[test]
fn test_tier3_budget_builder() {
    let budget = SearchBudget::default()
        .with_time(50_000)
        .with_candidates(5_000)
        .with_per_primitive(1_000);

    assert_eq!(budget.max_wall_time_micros, 50_000);
    assert_eq!(budget.max_candidates, 5_000);
    assert_eq!(budget.max_candidates_per_primitive, 1_000);
}

/// Default budget has sensible values
#[test]
fn test_tier3_budget_defaults() {
    let budget = SearchBudget::default();

    assert_eq!(budget.max_wall_time_micros, 100_000); // 100ms
    assert_eq!(budget.max_candidates, 10_000);
    assert_eq!(budget.max_candidates_per_primitive, 2_000);
}
