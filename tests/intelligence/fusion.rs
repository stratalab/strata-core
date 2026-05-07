//! Tier 5: Fusion Correctness
//!
//! Tests for RRF fusion correctness.

use strata_core::{BranchId, EntityRef as DocRef, PrimitiveType};
use strata_engine::{SearchHit, SearchResponse, SearchStats};
use strata_engine::search::{Fuser, RRFFuser};

const DEFAULT_SPACE: &str = "default";

// ============================================================================
// Test Helpers
// ============================================================================

fn make_hit(doc_ref: DocRef, score: f32, rank: u32) -> SearchHit {
    SearchHit {
        doc_ref,
        score,
        rank,
        snippet: None,
    }
}

fn make_response(hits: Vec<SearchHit>) -> SearchResponse {
    SearchResponse {
        hits,
        truncated: false,
        stats: SearchStats::new(0, 0),
    }
}

/// Returns DocRef for a KV entity with a given branch_id
fn make_kv_doc_ref_with_branch(branch_id: &BranchId, name: &str) -> DocRef {
    DocRef::kv(branch_id.clone(), DEFAULT_SPACE, name)
}

// ============================================================================
// RRFFuser Tests
// ============================================================================

/// RRFFuser handles empty input
#[test]
fn test_tier5_rrf_fuser_empty() {
    let fuser = RRFFuser::default();
    let result = fuser.fuse(vec![], 10);
    assert!(result.hits.is_empty());
    assert!(!result.truncated);
}

/// RRFFuser handles single list
#[test]
fn test_tier5_rrf_fuser_single() {
    let fuser = RRFFuser::default();

    let branch_id = BranchId::new();
    let doc_ref_a = make_kv_doc_ref_with_branch(&branch_id, "a");
    let doc_ref_b = make_kv_doc_ref_with_branch(&branch_id, "b");

    let hits = vec![
        make_hit(doc_ref_a, 0.9, 1),
        make_hit(doc_ref_b, 0.8, 2),
    ];
    let results = vec![(PrimitiveType::Kv, make_response(hits))];

    let result = fuser.fuse(results, 10);
    assert_eq!(result.hits.len(), 2);
    // RRF scores: 1/(60+1)=0.0164, 1/(60+2)=0.0161
    assert!(result.hits[0].score > result.hits[1].score);
}

/// RRFFuser deduplicates across lists
#[test]
fn test_tier5_rrf_fuser_deduplication() {
    let fuser = RRFFuser::default();

    let branch_id = BranchId::new();
    let doc_ref_shared = make_kv_doc_ref_with_branch(&branch_id, "shared");

    // Same DocRef in both lists
    let list1_hits = vec![make_hit(doc_ref_shared.clone(), 0.9, 1)];
    let list2_hits = vec![make_hit(doc_ref_shared.clone(), 0.8, 1)];

    let results = vec![
        (PrimitiveType::Kv, make_response(list1_hits)),
        (PrimitiveType::Json, make_response(list2_hits)),
    ];

    let result = fuser.fuse(results, 10);

    // Should only have one hit (deduplicated)
    assert_eq!(result.hits.len(), 1);

    // RRF score should be sum: 1/(60+1) + 1/(60+1) = 2 * 0.0164 = 0.0328
    let expected_rrf = 2.0 / 61.0;
    assert!((result.hits[0].score - expected_rrf).abs() < 0.0001);
}

/// Documents in multiple lists rank higher
#[test]
fn test_tier5_rrf_multi_list_boost() {
    let fuser = RRFFuser::default();

    let branch_id = BranchId::new();
    let doc_ref_shared = make_kv_doc_ref_with_branch(&branch_id, "shared");
    let doc_ref_only1 = make_kv_doc_ref_with_branch(&branch_id, "only1");
    let doc_ref_only2 = make_kv_doc_ref_with_branch(&branch_id, "only2");

    let list1 = vec![
        make_hit(doc_ref_shared.clone(), 0.9, 1),
        make_hit(doc_ref_only1, 0.8, 2),
    ];
    let list2 = vec![
        make_hit(doc_ref_only2, 0.9, 1),
        make_hit(doc_ref_shared.clone(), 0.7, 2),
    ];

    let results = vec![
        (PrimitiveType::Kv, make_response(list1)),
        (PrimitiveType::Json, make_response(list2)),
    ];

    let result = fuser.fuse(results, 10);

    // Shared doc should be first (appears in both lists)
    assert_eq!(result.hits[0].doc_ref, doc_ref_shared);
}

/// RRFFuser respects k limit
#[test]
fn test_tier5_rrf_fuser_respects_k() {
    let fuser = RRFFuser::default();

    let branch_id = BranchId::new();
    let hits: Vec<_> = (0..10)
        .map(|i| {
            let doc_ref = make_kv_doc_ref_with_branch(&branch_id, &format!("key{}", i));
            make_hit(doc_ref, 1.0 - i as f32 * 0.1, (i + 1) as u32)
        })
        .collect();

    let results = vec![(PrimitiveType::Kv, make_response(hits))];

    let result = fuser.fuse(results, 3);
    assert_eq!(result.hits.len(), 3);
    assert!(result.truncated);
}

/// RRFFuser is deterministic
#[test]
fn test_tier5_rrf_fuser_deterministic() {
    let fuser = RRFFuser::default();

    let branch_id = BranchId::new();
    let doc_ref_a = make_kv_doc_ref_with_branch(&branch_id, "det_a");
    let doc_ref_b = make_kv_doc_ref_with_branch(&branch_id, "det_b");
    let doc_ref_c = make_kv_doc_ref_with_branch(&branch_id, "det_c");

    let make_results = || {
        vec![
            (
                PrimitiveType::Kv,
                make_response(vec![
                    make_hit(doc_ref_a.clone(), 0.9, 1),
                    make_hit(doc_ref_b.clone(), 0.8, 2),
                ]),
            ),
            (
                PrimitiveType::Json,
                make_response(vec![
                    make_hit(doc_ref_c.clone(), 0.9, 1),
                    make_hit(doc_ref_b.clone(), 0.7, 2),
                ]),
            ),
        ]
    };

    let result1 = fuser.fuse(make_results(), 10);
    let result2 = fuser.fuse(make_results(), 10);

    assert_eq!(result1.hits.len(), result2.hits.len());
    for (h1, h2) in result1.hits.iter().zip(result2.hits.iter()) {
        assert_eq!(h1.doc_ref, h2.doc_ref);
        assert_eq!(h1.rank, h2.rank);
        assert!((h1.score - h2.score).abs() < 0.0001);
    }
}

/// RRFFuser custom k parameter works
#[test]
fn test_tier5_rrf_custom_k() {
    let fuser = RRFFuser::new(10);
    assert_eq!(fuser.k_rrf(), 10);

    let branch_id = BranchId::new();
    let doc_ref = make_kv_doc_ref_with_branch(&branch_id, "custom");
    let hits = vec![make_hit(doc_ref, 0.9, 1)];
    let results = vec![(PrimitiveType::Kv, make_response(hits))];

    let result = fuser.fuse(results, 10);

    // With k=10, score should be 1/(10+1) = 0.0909
    let expected = 1.0 / 11.0;
    assert!((result.hits[0].score - expected).abs() < 0.0001);
}

/// RRFFuser has correct name
#[test]
fn test_tier5_rrf_fuser_name() {
    let fuser = RRFFuser::default();
    assert_eq!(fuser.name(), "rrf");
}

// ============================================================================
// Fuser Trait Tests
// ============================================================================

/// Fusers are Send + Sync
#[test]
fn test_tier5_fusers_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RRFFuser>();
}
