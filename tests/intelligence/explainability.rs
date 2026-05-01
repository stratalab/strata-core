//! Tier 9: Result Explainability
//!
//! Tests for result provenance and debugging capabilities.

use crate::common::*;
use crate::common::search::substrate_search;
use strata_core::PrimitiveType;
use strata_engine::{KVStore, SearchRequest, SearchStats};

// ============================================================================
// Result Provenance Tests
// ============================================================================

/// Each hit has primitive kind
#[test]
fn test_tier9_hit_has_primitive_kind() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let req = SearchRequest::new(branch_id, "test");
    let response = substrate_search(&db, &req);

    for hit in &response.hits {
        let kind = hit.doc_ref.primitive_type();
        assert!(PrimitiveType::all().contains(&kind));
    }
}

/// Each hit has branch_id
#[test]
fn test_tier9_hit_has_branch_id() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    for hit in &response.hits {
        assert_eq!(hit.doc_ref.branch_id(), branch_id);
    }
}

/// Each hit has score
#[test]
fn test_tier9_hit_has_score() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    for hit in &response.hits {
        // Score should be a valid float
        assert!(!hit.score.is_nan());
        // Matching hits should have positive score
        assert!(hit.score >= 0.0);
    }
}

/// Each hit has rank
#[test]
fn test_tier9_hit_has_rank() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    for (i, hit) in response.hits.iter().enumerate() {
        assert_eq!(hit.rank, (i + 1) as u32);
    }
}

// ============================================================================
// SearchStats Tests
// ============================================================================

/// Response has stats
#[test]
fn test_tier9_response_has_stats() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    let _: &SearchStats = &response.stats;
}

/// Stats has elapsed_micros
#[test]
fn test_tier9_stats_has_elapsed() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    // Should be a non-negative value
    let _: u64 = response.stats.elapsed_micros;
}

/// Stats has candidates_considered
#[test]
fn test_tier9_stats_has_candidates() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    // Should be a non-negative value
    let _: usize = response.stats.candidates_considered;
}

/// Stats tracks per-primitive candidates
#[test]
fn test_tier9_stats_per_primitive() {
    let stats = SearchStats::default();

    let mut stats = stats;
    stats.add_primitive_candidates(PrimitiveType::Kv, 100);
    stats.add_primitive_candidates(PrimitiveType::Json, 50);

    assert_eq!(
        stats.candidates_by_primitive.get(&PrimitiveType::Kv),
        Some(&100)
    );
    assert_eq!(
        stats.candidates_by_primitive.get(&PrimitiveType::Json),
        Some(&50)
    );
    assert_eq!(stats.candidates_considered, 150);
}

/// Stats tracks index_used
#[test]
fn test_tier9_stats_index_used() {
    let stats = SearchStats::new(1000, 500).with_index_used(true);
    assert!(stats.index_used);

    let stats = SearchStats::new(1000, 500).with_index_used(false);
    assert!(!stats.index_used);
}

// ============================================================================
// Truncation Explainability Tests
// ============================================================================

/// Response has truncated flag
#[test]
fn test_tier9_response_has_truncated() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    let _: bool = response.truncated;
}

/// Truncated flag is false when not truncated
#[test]
fn test_tier9_truncated_false_when_not_truncated() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    // Request more than available
    let req = SearchRequest::new(branch_id, "test").with_k(100);
    let response = kv.search(&req).unwrap();

    // With only 3 test documents, shouldn't be truncated
    if response.hits.len() < 100 {
        assert!(!response.truncated);
    }
}

// ============================================================================
// DocRef Debugging Tests
// ============================================================================

/// DocRef is Debug-printable
#[test]
fn test_tier9_docref_debug() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    for hit in &response.hits {
        // Should be able to debug-print
        let _debug = format!("{:?}", hit.doc_ref);
    }
}

/// SearchHit is Debug-printable
#[test]
fn test_tier9_searchhit_debug() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    for hit in &response.hits {
        let _debug = format!("{:?}", hit);
    }
}

/// SearchResponse is Debug-printable
#[test]
fn test_tier9_searchresponse_debug() {
    let db = create_test_db();
    let branch_id = test_branch_id();
    populate_test_data(&db, &branch_id);

    let kv = KVStore::new(db.clone());
    let req = SearchRequest::new(branch_id, "test");
    let response = kv.search(&req).unwrap();

    let _debug = format!("{:?}", response);
}
