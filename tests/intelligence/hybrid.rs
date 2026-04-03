//! Tier 8: Cross-Primitive Search (Substrate)
//!
//! Tests for the unified retrieval substrate orchestration.

use crate::common::*;
use crate::common::search::{substrate_search, verify_substrate_ranks_sequential, verify_substrate_scores_decreasing};
use strata_core::search_types::{PrimitiveType, SearchRequest};
use std::collections::HashSet;

// ============================================================================
// Substrate Search Basic Tests
// ============================================================================

/// Substrate search works on empty database
#[test]
fn test_tier8_substrate_empty_db() {
    let db = create_test_db();
    let branch_id = test_branch_id();

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    assert!(response.hits.is_empty());
}

/// Substrate search finds results across primitives
#[test]
fn test_tier8_substrate_finds_results() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    assert!(!response.hits.is_empty());
}

/// Substrate search includes KV results
#[test]
fn test_tier8_substrate_includes_kv() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    let primitives: HashSet<_> = response
        .hits
        .iter()
        .map(|h| h.doc_ref.primitive_type())
        .collect();

    assert!(primitives.contains(&PrimitiveType::Kv));
}

// ============================================================================
// Substrate Search Filter Tests
// ============================================================================

/// Substrate search respects primitive filter
#[test]
fn test_tier8_substrate_respects_filter() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test").with_primitive_filter(vec![PrimitiveType::Kv]);
    let response = substrate_search(&db, &req);

    for hit in &response.hits {
        assert_eq!(hit.doc_ref.primitive_type(), PrimitiveType::Kv);
    }
}

/// Empty filter returns no results
#[test]
fn test_tier8_substrate_empty_filter() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test").with_primitive_filter(vec![]);
    let response = substrate_search(&db, &req);

    assert!(response.hits.is_empty());
}

// ============================================================================
// Substrate Search Consistency Tests
// ============================================================================

/// Substrate search is deterministic
#[test]
fn test_tier8_substrate_deterministic() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    verify_deterministic(&db, &req);
}

/// Substrate search results have valid ranks
#[test]
fn test_tier8_substrate_valid_ranks() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    verify_substrate_ranks_sequential(&response.hits);
}

/// Substrate search results have valid scores
#[test]
fn test_tier8_substrate_valid_scores() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    verify_substrate_scores_decreasing(&response.hits);
}

// ============================================================================
// Substrate Search Stats Tests
// ============================================================================

/// Substrate search populates stats
#[test]
fn test_tier8_substrate_populates_stats() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    // Stats should be populated
    assert!(response.stats.elapsed_ms >= 0.0);
    assert!(response.stats.snapshot_version > 0);
}

// ============================================================================
// Budget Exhaustion Tests
// ============================================================================

/// Budget exhaustion flag is reported correctly
#[test]
fn test_tier8_substrate_budget_exhaustion() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    // Search with no budget — should not be exhausted
    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);
    assert!(!response.stats.budget_exhausted);
}
