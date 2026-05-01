//! Fusion infrastructure for combining search results
//!
//! This module provides:
//! - Fuser trait for pluggable fusion algorithms
//! - RRFFuser: Reciprocal Rank Fusion (default)
//! - weighted_rrf_fuse: multi-query fusion with per-list weights
//!
//! See `docs/architecture/M6_ARCHITECTURE.md` for authoritative specification.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use strata_core::PrimitiveType;
use strata_engine::search::{EntityRef, SearchHit, SearchResponse};

// ============================================================================
// FusedResult
// ============================================================================

/// Result of fusing multiple primitive search results
#[derive(Debug, Clone)]
pub struct FusedResult {
    /// Final ranked list of hits
    pub hits: Vec<SearchHit>,
    /// Whether results were truncated
    pub truncated: bool,
}

impl FusedResult {
    /// Create a new FusedResult
    pub fn new(hits: Vec<SearchHit>, truncated: bool) -> Self {
        FusedResult { hits, truncated }
    }
}

// ============================================================================
// Fuser Trait
// ============================================================================

/// Pluggable fusion interface
///
/// Fusers combine search results from multiple primitives into a single
/// ranked list. Different algorithms can prioritize different factors.
///
/// # Thread Safety
///
/// Fusers must be Send + Sync for concurrent search operations.
///
/// # Implementation Notes
///
/// RRFFuser (Reciprocal Rank Fusion) is the default and only built-in fuser.
pub trait Fuser: Send + Sync {
    /// Fuse results from multiple primitives
    ///
    /// Takes a list of (primitive, results) pairs and returns a combined
    /// ranked list truncated to k items.
    fn fuse(&self, results: Vec<(PrimitiveType, SearchResponse)>, k: usize) -> FusedResult;

    /// Name for debugging and logging
    fn name(&self) -> &str;
}

// ============================================================================
// Shared RRF helpers
// ============================================================================

/// Sort scored entries by RRF score with deterministic tie-breaking.
///
/// Tie-breaking order:
/// 1. RRF score descending
/// 2. Original hit score descending (from first occurrence)
/// 3. EntityRef hash for stable ordering
fn sort_rrf_scored(scored: &mut [(EntityRef, f32)], hit_data: &HashMap<EntityRef, SearchHit>) {
    scored.sort_by(|a, b| match b.1.partial_cmp(&a.1) {
        Some(std::cmp::Ordering::Equal) | None => {
            let orig_a = hit_data.get(&a.0).map(|h| h.score).unwrap_or(0.0);
            let orig_b = hit_data.get(&b.0).map(|h| h.score).unwrap_or(0.0);
            match orig_b.partial_cmp(&orig_a) {
                Some(std::cmp::Ordering::Equal) | None => {
                    let hash_a = {
                        let mut hasher = DefaultHasher::new();
                        a.0.hash(&mut hasher);
                        hasher.finish()
                    };
                    let hash_b = {
                        let mut hasher = DefaultHasher::new();
                        b.0.hash(&mut hasher);
                        hasher.finish()
                    };
                    hash_a.cmp(&hash_b)
                }
                Some(ord) => ord,
            }
        }
        Some(ord) => ord,
    });
}

/// Build a ranked FusedResult from sorted RRF scores.
fn build_ranked_result(
    scored: Vec<(EntityRef, f32)>,
    mut hit_data: HashMap<EntityRef, SearchHit>,
    k: usize,
) -> FusedResult {
    let truncated = scored.len() > k;
    let hits: Vec<SearchHit> = scored
        .into_iter()
        .take(k)
        .enumerate()
        .map(|(i, (doc_ref, rrf_score))| {
            let mut hit = hit_data
                .remove(&doc_ref)
                .expect("invariant violation: scored doc_ref must exist in hit_data");
            hit.score = rrf_score;
            hit.rank = (i + 1) as u32;
            hit
        })
        .collect();
    FusedResult::new(hits, truncated)
}

// ============================================================================
// RRFFuser
// ============================================================================

/// Reciprocal Rank Fusion (RRF)
///
/// RRF Score = sum(1 / (k + rank)) across all lists
/// Where k is a smoothing constant (default 60).
///
/// This fuser excels at combining results from different ranking algorithms
/// (e.g., keyword + vector search).
///
/// # Algorithm
///
/// For each document appearing in any list:
/// - Calculate RRF contribution: 1 / (k_rrf + rank)
/// - Sum contributions across all lists
/// - Higher RRF score = higher final rank
///
/// # Example
///
/// ```text
/// Given:
///   - List A: [doc1@rank1, doc2@rank2, doc3@rank3]
///   - List B: [doc2@rank1, doc4@rank2, doc1@rank3]
///   - k_rrf = 60
///
/// RRF scores:
///   doc1: 1/(60+1) + 1/(60+3) = 0.0164 + 0.0159 = 0.0323
///   doc2: 1/(60+2) + 1/(60+1) = 0.0161 + 0.0164 = 0.0325  <- highest
///   doc3: 1/(60+3) = 0.0159
///   doc4: 1/(60+2) = 0.0161
///
/// Final ranking: [doc2, doc1, doc4, doc3]
/// ```
#[derive(Debug, Clone)]
pub struct RRFFuser {
    /// Smoothing constant (default 60)
    k_rrf: u32,
}

impl Default for RRFFuser {
    fn default() -> Self {
        RRFFuser { k_rrf: 60 }
    }
}

impl RRFFuser {
    /// Create a new RRFFuser with custom k value
    pub fn new(k_rrf: u32) -> Self {
        RRFFuser { k_rrf }
    }

    /// Get the k parameter
    pub fn k_rrf(&self) -> u32 {
        self.k_rrf
    }
}

impl Fuser for RRFFuser {
    fn fuse(&self, results: Vec<(PrimitiveType, SearchResponse)>, k: usize) -> FusedResult {
        let mut rrf_scores: HashMap<EntityRef, f32> = HashMap::new();
        let mut hit_data: HashMap<EntityRef, SearchHit> = HashMap::new();

        for (_primitive, response) in results {
            for hit in response.hits {
                let rrf_contribution = 1.0 / (self.k_rrf as f32 + hit.rank as f32);
                *rrf_scores.entry(hit.doc_ref.clone()).or_insert(0.0) += rrf_contribution;
                hit_data.entry(hit.doc_ref.clone()).or_insert(hit);
            }
        }

        let mut scored: Vec<_> = rrf_scores.into_iter().collect();
        sort_rrf_scored(&mut scored, &hit_data);
        build_ranked_result(scored, hit_data, k)
    }

    fn name(&self) -> &str {
        "rrf"
    }
}

// ============================================================================
// Score-based merge (for single-signal keyword search)
// ============================================================================

/// Merge multiple primitive result lists by raw score (no fusion).
///
/// Used for keyword-only search where all primitives use the same BM25 scorer
/// and scores are directly comparable. Simply concatenates, deduplicates,
/// sorts by score descending, and truncates to top_k.
pub fn merge_by_score(results: Vec<(PrimitiveType, SearchResponse)>, top_k: usize) -> FusedResult {
    let mut hit_map: HashMap<EntityRef, SearchHit> = HashMap::new();

    for (_primitive, response) in results {
        for hit in response.hits {
            hit_map
                .entry(hit.doc_ref.clone())
                .and_modify(|existing| {
                    // Keep the higher score if doc appears in multiple primitives
                    if hit.score > existing.score {
                        *existing = hit.clone();
                    }
                })
                .or_insert(hit);
        }
    }

    let mut hits: Vec<SearchHit> = hit_map.into_values().collect();
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let truncated = hits.len() > top_k;
    hits.truncate(top_k);

    for (i, hit) in hits.iter_mut().enumerate() {
        hit.rank = (i + 1) as u32;
    }

    FusedResult::new(hits, truncated)
}

// ============================================================================
// Weighted RRF (for multi-query fusion)
// ============================================================================

/// Top-rank bonus: #1 in any list gets +0.05, #2-3 gets +0.02.
/// Prevents dilution of exact matches during multi-list fusion.
const RANK1_BONUS: f32 = 0.05;
const TOP3_BONUS: f32 = 0.02;

/// Fuse multiple ranked result lists with per-list weights using RRF.
///
/// Used by the multi-query expansion pipeline to combine results from
/// the original query (weighted 2x) with expansion variants (weighted 1x).
///
/// Each `(SearchResponse, f32)` pair is a result list and its weight multiplier.
/// The weighted RRF score for a document is:
///   `sum(weight * 1 / (k + rank))` across all lists where it appears,
/// plus a top-rank bonus (+0.05 for #1, +0.02 for #2-3 in any list).
pub fn weighted_rrf_fuse(
    results: Vec<(SearchResponse, f32)>,
    k_rrf: u32,
    top_k: usize,
) -> FusedResult {
    let mut rrf_scores: HashMap<EntityRef, f32> = HashMap::new();
    let mut best_rank: HashMap<EntityRef, u32> = HashMap::new();
    let mut hit_data: HashMap<EntityRef, SearchHit> = HashMap::new();

    for (response, weight) in results {
        for hit in response.hits {
            let rrf_contribution = weight / (k_rrf as f32 + hit.rank as f32);
            *rrf_scores.entry(hit.doc_ref.clone()).or_insert(0.0) += rrf_contribution;

            // Track best (lowest) rank across all lists
            let entry = best_rank.entry(hit.doc_ref.clone()).or_insert(u32::MAX);
            if hit.rank < *entry {
                *entry = hit.rank;
            }

            hit_data.entry(hit.doc_ref.clone()).or_insert(hit);
        }
    }

    // Apply top-rank bonus
    for (doc_ref, score) in rrf_scores.iter_mut() {
        if let Some(&rank) = best_rank.get(doc_ref) {
            if rank == 1 {
                *score += RANK1_BONUS;
            } else if rank <= 3 {
                *score += TOP3_BONUS;
            }
        }
    }

    let mut scored: Vec<_> = rrf_scores.into_iter().collect();
    sort_rrf_scored(&mut scored, &hit_data);
    build_ranked_result(scored, hit_data, top_k)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::BranchId;
    use strata_engine::search::{EntityRef, SearchStats};

    fn make_hit(doc_ref: EntityRef, score: f32, rank: u32) -> SearchHit {
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

    /// Helper to create a KV EntityRef
    fn make_kv_doc_ref(branch_id: &BranchId, key: &str) -> EntityRef {
        EntityRef::Kv {
            branch_id: *branch_id,
            space: "default".to_string(),
            key: key.to_string(),
        }
    }

    #[test]
    fn test_fuser_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RRFFuser>();
    }

    #[test]
    fn test_rrf_fuser_empty() {
        let fuser = RRFFuser::default();
        let result = fuser.fuse(vec![], 10);
        assert!(result.hits.is_empty());
        assert!(!result.truncated);
    }

    #[test]
    fn test_rrf_fuser_single_list() {
        let fuser = RRFFuser::default();

        let branch_id = BranchId::new();
        let doc_ref_a = make_kv_doc_ref(&branch_id, "a");
        let doc_ref_b = make_kv_doc_ref(&branch_id, "b");

        let hits = vec![make_hit(doc_ref_a, 0.9, 1), make_hit(doc_ref_b, 0.8, 2)];
        let results = vec![(PrimitiveType::Kv, make_response(hits))];

        let result = fuser.fuse(results, 10);
        assert_eq!(result.hits.len(), 2);
        // RRF scores: 1/(60+1)=0.0164, 1/(60+2)=0.0161
        assert!(result.hits[0].score > result.hits[1].score);
    }

    #[test]
    fn test_rrf_fuser_deduplication() {
        let fuser = RRFFuser::default();

        let branch_id = BranchId::new();
        let doc_ref_shared = make_kv_doc_ref(&branch_id, "shared");

        // Same EntityRef in both lists
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

    #[test]
    fn test_rrf_fuser_documents_in_both_lists_rank_higher() {
        let fuser = RRFFuser::default();

        let branch_id = BranchId::new();
        let doc_ref_a = make_kv_doc_ref(&branch_id, "in_both");
        let doc_ref_b = make_kv_doc_ref(&branch_id, "only_list1");
        let doc_ref_c = make_kv_doc_ref(&branch_id, "only_list2");

        let list1_hits = vec![
            make_hit(doc_ref_a.clone(), 0.9, 1),
            make_hit(doc_ref_b, 0.8, 2),
        ];
        let list2_hits = vec![
            make_hit(doc_ref_c, 0.9, 1),
            make_hit(doc_ref_a.clone(), 0.7, 2),
        ];

        let results = vec![
            (PrimitiveType::Kv, make_response(list1_hits)),
            (PrimitiveType::Json, make_response(list2_hits)),
        ];

        let result = fuser.fuse(results, 10);

        // key_a appears in both lists, so it should have highest RRF score
        // key_a: 1/(60+1) + 1/(60+2) = 0.0164 + 0.0161 = 0.0325
        // key_b: 1/(60+2) = 0.0161
        // key_c: 1/(60+1) = 0.0164
        assert_eq!(result.hits.len(), 3);
        assert_eq!(result.hits[0].doc_ref, doc_ref_a);
    }

    #[test]
    fn test_rrf_fuser_respects_k() {
        let fuser = RRFFuser::default();

        let branch_id = BranchId::new();
        let hits: Vec<_> = (0..10)
            .map(|i| {
                let doc_ref = make_kv_doc_ref(&branch_id, &format!("key{}", i));
                make_hit(doc_ref, 1.0 - i as f32 * 0.1, (i + 1) as u32)
            })
            .collect();

        let results = vec![(PrimitiveType::Kv, make_response(hits))];

        let result = fuser.fuse(results, 3);
        assert_eq!(result.hits.len(), 3);
        assert!(result.truncated);
    }

    #[test]
    fn test_rrf_fuser_determinism() {
        let fuser = RRFFuser::default();

        let branch_id = BranchId::new();
        let doc_ref_a = make_kv_doc_ref(&branch_id, "det_a");
        let doc_ref_b = make_kv_doc_ref(&branch_id, "det_b");
        let doc_ref_c = make_kv_doc_ref(&branch_id, "det_c");

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

        // Same inputs should produce same output order
        assert_eq!(result1.hits.len(), result2.hits.len());
        for (h1, h2) in result1.hits.iter().zip(result2.hits.iter()) {
            assert_eq!(h1.doc_ref, h2.doc_ref);
            assert_eq!(h1.rank, h2.rank);
            assert!((h1.score - h2.score).abs() < 0.0001);
        }
    }

    #[test]
    fn test_rrf_fuser_custom_k() {
        let fuser = RRFFuser::new(10);
        assert_eq!(fuser.k_rrf(), 10);

        let branch_id = BranchId::new();
        let doc_ref = make_kv_doc_ref(&branch_id, "custom_k");
        let hits = vec![make_hit(doc_ref, 0.9, 1)];
        let results = vec![(PrimitiveType::Kv, make_response(hits))];

        let result = fuser.fuse(results, 10);

        // With k=10, score should be 1/(10+1) = 0.0909
        let expected = 1.0 / 11.0;
        assert!((result.hits[0].score - expected).abs() < 0.0001);
    }

    #[test]
    fn test_rrf_fuser_name() {
        let fuser = RRFFuser::default();
        assert_eq!(fuser.name(), "rrf");
    }

    // ========================================
    // Weighted RRF Tests
    // ========================================

    #[test]
    fn test_weighted_rrf_top_rank_bonus() {
        let branch_id = BranchId::new();
        let doc_a = make_kv_doc_ref(&branch_id, "rank1_doc");
        let doc_b = make_kv_doc_ref(&branch_id, "rank2_doc");
        let doc_c = make_kv_doc_ref(&branch_id, "rank4_doc");

        // doc_a is #1 in list 1, doc_b is #2, doc_c is #4
        let list1 = make_response(vec![
            make_hit(doc_a.clone(), 0.9, 1),
            make_hit(doc_b.clone(), 0.8, 2),
            make_hit(doc_c.clone(), 0.6, 4),
        ]);

        let results = vec![(list1, 1.0)];
        let fused = weighted_rrf_fuse(results, 60, 10);

        // doc_a (rank 1): 1/(60+1) + 0.05 bonus = 0.01639 + 0.05
        let expected_a = 1.0 / 61.0 + 0.05;
        // doc_b (rank 2): 1/(60+2) + 0.02 bonus = 0.01613 + 0.02
        let expected_b = 1.0 / 62.0 + 0.02;
        // doc_c (rank 4): 1/(60+4) = 0.01563, no bonus
        let expected_c = 1.0 / 64.0;

        assert!((fused.hits[0].score - expected_a).abs() < 0.001);
        assert!((fused.hits[1].score - expected_b).abs() < 0.001);
        assert!((fused.hits[2].score - expected_c).abs() < 0.001);
    }

    #[test]
    fn test_weighted_rrf_original_gets_higher_weight() {
        let branch_id = BranchId::new();
        let doc_a = make_kv_doc_ref(&branch_id, "original_result");
        let doc_b = make_kv_doc_ref(&branch_id, "expansion_result");

        // doc_a is #1 in original (weight 2.0), doc_b is #1 in expansion (weight 1.0)
        let original = make_response(vec![make_hit(doc_a.clone(), 0.9, 1)]);
        let expansion = make_response(vec![make_hit(doc_b.clone(), 0.9, 1)]);

        let results = vec![(original, 2.0), (expansion, 1.0)];
        let fused = weighted_rrf_fuse(results, 60, 10);

        // Both are rank 1, both get rank1 bonus (+0.05)
        // doc_a: 2.0/(60+1) + 0.05 = 0.0328 + 0.05
        // doc_b: 1.0/(60+1) + 0.05 = 0.0164 + 0.05
        assert_eq!(fused.hits[0].doc_ref, doc_a);
        assert!(fused.hits[0].score > fused.hits[1].score);
    }

    #[test]
    fn test_weighted_rrf_empty() {
        let fused = weighted_rrf_fuse(vec![], 60, 10);
        assert!(fused.hits.is_empty());
    }

    // ========================================
    // merge_by_score Tests
    // ========================================

    #[test]
    fn test_merge_by_score_preserves_raw_scores() {
        let branch_id = BranchId::new();
        let doc_a = make_kv_doc_ref(&branch_id, "a");
        let doc_b = make_kv_doc_ref(&branch_id, "b");

        let hits = vec![make_hit(doc_a, 2.5, 1), make_hit(doc_b, 1.8, 2)];
        let results = vec![(PrimitiveType::Kv, make_response(hits))];

        let fused = merge_by_score(results, 10);
        assert_eq!(fused.hits.len(), 2);
        assert!((fused.hits[0].score - 2.5).abs() < 0.0001);
        assert!((fused.hits[1].score - 1.8).abs() < 0.0001);
        assert_eq!(fused.hits[0].rank, 1);
        assert_eq!(fused.hits[1].rank, 2);
    }

    #[test]
    fn test_merge_by_score_across_primitives() {
        let branch_id = BranchId::new();
        let doc_a = make_kv_doc_ref(&branch_id, "a");
        let doc_b = make_kv_doc_ref(&branch_id, "b");
        let doc_c = make_kv_doc_ref(&branch_id, "c");

        // doc_b appears in both lists with different scores — keep the higher one
        let list1 = vec![
            make_hit(doc_a.clone(), 3.0, 1),
            make_hit(doc_b.clone(), 1.5, 2),
        ];
        let list2 = vec![
            make_hit(doc_b.clone(), 2.0, 1),
            make_hit(doc_c.clone(), 1.0, 2),
        ];

        let results = vec![
            (PrimitiveType::Kv, make_response(list1)),
            (PrimitiveType::Json, make_response(list2)),
        ];

        let fused = merge_by_score(results, 10);
        assert_eq!(fused.hits.len(), 3);

        // Sorted by raw score: doc_a(3.0), doc_b(2.0), doc_c(1.0)
        assert_eq!(fused.hits[0].doc_ref, doc_a);
        assert!((fused.hits[0].score - 3.0).abs() < 0.0001);
        assert_eq!(fused.hits[1].doc_ref, doc_b);
        assert!((fused.hits[1].score - 2.0).abs() < 0.0001);
        assert_eq!(fused.hits[2].doc_ref, doc_c);
        assert!((fused.hits[2].score - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_merge_by_score_respects_top_k() {
        let branch_id = BranchId::new();
        let hits: Vec<_> = (0..10)
            .map(|i| {
                let doc_ref = make_kv_doc_ref(&branch_id, &format!("key{}", i));
                make_hit(doc_ref, 1.0 - i as f32 * 0.1, (i + 1) as u32)
            })
            .collect();

        let results = vec![(PrimitiveType::Kv, make_response(hits))];

        let fused = merge_by_score(results, 3);
        assert_eq!(fused.hits.len(), 3);
        assert!(fused.truncated);
    }

    #[test]
    fn test_merge_by_score_empty() {
        let fused = merge_by_score(vec![], 10);
        assert!(fused.hits.is_empty());
        assert!(!fused.truncated);
    }
}
