//! Composite search orchestrator for M6
//!
//! This module provides:
//! - HybridSearch struct that orchestrates searches across primitives
//! - Primitive selection based on filters
//! - Budget allocation across primitives
//! - Search orchestration with consistent snapshot
//!
//! See `docs/architecture/M6_ARCHITECTURE.md` for authoritative specification.
//!
//! # Architectural Rules
//!
//! - Rule 3: Composite Orchestrates, Doesn't Replace
//! - Rule 4: Snapshot-Consistent Search
//!
//! HybridSearch is STATELESS. It holds only references to Database and primitives.

use crate::fuser::{Fuser, RRFFuser};
use rayon::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use strata_core::PrimitiveType;
use strata_core::StrataResult;
use strata_engine::database::{SHADOW_EVENT, SHADOW_JSON, SHADOW_KV, SHADOW_STATE};
use strata_engine::search::{
    SearchBudget, SearchHit, SearchMode, SearchRequest, SearchResponse, SearchStats,
};
use strata_engine::Database;
use strata_engine::{BranchIndex, EventLog, JsonStore, KVStore, StateCell, VectorStore};

// ============================================================================
// HybridSearch
// ============================================================================

/// Composite search orchestrator
///
/// HybridSearch coordinates searches across multiple primitives
/// and fuses results into a single ranked list.
///
/// # Architecture
///
/// ```text
/// SearchRequest
///      │
///      ▼
/// ┌─────────────────────────────────────────┐
/// │            HybridSearch                  │
/// │  ┌────────────┐  ┌────────────────────┐ │
/// │  │select_prims│──│allocate_budgets    │ │
/// │  └────────────┘  └────────────────────┘ │
/// │                                          │
/// │  ┌────────────────────────────────────┐ │
/// │  │     Search Each Primitive          │ │
/// │  │  ┌───┐ ┌────┐ ┌─────┐ ┌─────┐     │ │
/// │  │  │KV │ │JSON│ │Event│ │State│ ... │ │
/// │  │  └─┬─┘ └──┬─┘ └──┬──┘ └──┬──┘     │ │
/// │  └────┼──────┼──────┼───────┼────────┘ │
/// │       └──────┴──────┴───────┘          │
/// │                │                        │
/// │         ┌──────┴──────┐                 │
/// │         │   Fuser     │                 │
/// │         └──────┬──────┘                 │
/// └────────────────┼────────────────────────┘
///                  │
///                  ▼
///           SearchResponse
/// ```
///
/// # Stateless Design
///
/// CRITICAL: HybridSearch is STATELESS. It holds only Arc references.
/// All search state is ephemeral per-request.
#[derive(Clone)]
pub struct HybridSearch {
    /// Database reference — kept alive for primitive stores; read indirectly via embedder
    #[allow(dead_code)]
    db: Arc<Database>,
    /// Optional query embedder for hybrid (BM25+vector) search
    embedder: Option<Arc<dyn crate::QueryEmbedder>>,
    /// Fuser for combining results
    fuser: Arc<dyn Fuser>,
    /// All primitive facades
    kv: KVStore,
    json: JsonStore,
    event: EventLog,
    state: StateCell,
    branch_index: BranchIndex,
    vector: VectorStore,
}

impl HybridSearch {
    /// Create a new HybridSearch orchestrator
    ///
    /// Creates all primitive facades internally.
    /// Uses RRFFuser by default (appropriate for cross-primitive fusion).
    pub fn new(db: Arc<Database>) -> Self {
        HybridSearch {
            kv: KVStore::new(db.clone()),
            json: JsonStore::new(db.clone()),
            event: EventLog::new(db.clone()),
            state: StateCell::new(db.clone()),
            branch_index: BranchIndex::new(db.clone()),
            vector: VectorStore::new(db.clone()),
            db,
            embedder: None,
            fuser: Arc::new(RRFFuser::default()),
        }
    }

    /// Create a new HybridSearch with a query embedder for vector search.
    pub fn with_embedder(db: Arc<Database>, embedder: Arc<dyn crate::QueryEmbedder>) -> Self {
        HybridSearch {
            kv: KVStore::new(db.clone()),
            json: JsonStore::new(db.clone()),
            event: EventLog::new(db.clone()),
            state: StateCell::new(db.clone()),
            branch_index: BranchIndex::new(db.clone()),
            vector: VectorStore::new(db.clone()),
            db,
            embedder: Some(embedder),
            fuser: Arc::new(RRFFuser::default()),
        }
    }

    // ========================================================================
    // Search Orchestration
    // ========================================================================

    /// Search across all (or filtered) primitives
    ///
    /// # Flow
    ///
    /// 1. Select primitives based on filter
    /// 2. Allocate budget across primitives
    /// 3. Execute searches (respecting budget)
    /// 4. Fuse results
    /// 5. Return combined response
    ///
    /// # Snapshot Consistency
    ///
    /// Per Rule 4: Each primitive's search() uses its own snapshot.
    /// For true cross-primitive consistency, primitives would need
    /// search_with_snapshot() methods. This is acceptable for M6.
    pub fn search(&self, req: &SearchRequest) -> StrataResult<SearchResponse> {
        let start = Instant::now();

        // 1. Select primitives
        let primitives = self.select_primitives(req);

        if primitives.is_empty() {
            return Ok(SearchResponse {
                hits: vec![],
                truncated: false,
                stats: SearchStats::new(start.elapsed().as_micros() as u64, 0),
            });
        }

        // 2. Allocate budgets
        let budgets = self.allocate_budgets(req, primitives.len());

        // 3 & 4. In Hybrid mode, overlap BM25 primitive searches with query
        // embedding (rayon::join), then parallelize shadow collection vector
        // searches (par_iter). In Keyword mode, run BM25 sequentially as before.
        let is_hybrid = req.mode == SearchMode::Hybrid;

        // Compute the query embedding in parallel with BM25 searches (Hybrid only).
        let (bm25_result, query_embedding) = if is_hybrid {
            rayon::join(
                || -> StrataResult<(Vec<(PrimitiveType, SearchResponse)>, usize, bool)> {
                    let mut primitive_results = Vec::new();
                    let mut total_candidates = 0;
                    let mut any_truncated = false;

                    for (primitive, budget) in primitives.iter().zip(budgets.iter()) {
                        if *primitive == PrimitiveType::Vector {
                            continue;
                        }
                        if start.elapsed().as_micros() as u64 >= req.budget.max_wall_time_micros {
                            any_truncated = true;
                            break;
                        }
                        let sub_req = req.clone().with_budget(*budget);
                        let result = self.search_primitive(*primitive, &sub_req)?;
                        total_candidates += result.stats.candidates_considered;
                        if result.truncated {
                            any_truncated = true;
                        }
                        primitive_results.push((*primitive, result));
                    }
                    Ok((primitive_results, total_candidates, any_truncated))
                },
                || -> Option<Vec<f32>> {
                    // Use precomputed embedding if available, otherwise compute it
                    req.precomputed_embedding
                        .clone()
                        .or_else(|| self.embedder.as_ref().and_then(|e| e.embed(&req.query)))
                },
            )
        } else {
            // Keyword mode: no embedding needed
            let bm25 = (|| -> StrataResult<(Vec<(PrimitiveType, SearchResponse)>, usize, bool)> {
                let mut primitive_results = Vec::new();
                let mut total_candidates = 0;
                let mut any_truncated = false;

                for (primitive, budget) in primitives.iter().zip(budgets.iter()) {
                    if start.elapsed().as_micros() as u64 >= req.budget.max_wall_time_micros {
                        any_truncated = true;
                        break;
                    }
                    let sub_req = req.clone().with_budget(*budget);
                    let result = self.search_primitive(*primitive, &sub_req)?;
                    total_candidates += result.stats.candidates_considered;
                    if result.truncated {
                        any_truncated = true;
                    }
                    primitive_results.push((*primitive, result));
                }
                Ok((primitive_results, total_candidates, any_truncated))
            })();
            (bm25, None)
        };

        let (mut primitive_results, mut total_candidates, any_truncated) = bm25_result?;

        // Vector search for Hybrid mode: search shadow collections in parallel
        if is_hybrid {
            if let Some(query_embedding) = query_embedding {
                let shadow_collections = [SHADOW_KV, SHADOW_JSON, SHADOW_EVENT, SHADOW_STATE];

                let vector_hits: Vec<SearchHit> = shadow_collections
                    .par_iter()
                    .filter(|coll| {
                        self.vector
                            .system_collection_len(req.branch_id, coll)
                            .unwrap_or(0)
                            > 0
                    })
                    .flat_map(|collection| {
                        let matches = if let Some((start, end)) = req.time_range {
                            self.vector.system_search_with_sources_in_range(
                                req.branch_id,
                                collection,
                                &query_embedding,
                                req.k,
                                start,
                                end,
                            )
                        } else {
                            self.vector.system_search_with_sources(
                                req.branch_id,
                                collection,
                                &query_embedding,
                                req.k,
                            )
                        };

                        matches
                            .into_iter()
                            .flat_map(|results| results.into_iter())
                            .filter_map(|m| {
                                m.source_ref.map(|source_ref| {
                                    SearchHit::new(
                                        source_ref, m.score,
                                        0, // placeholder — re-assigned after global sort
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect();

                if !vector_hits.is_empty() {
                    // Sort by score descending so RRF ranks reflect global relevance,
                    // not the arbitrary shadow-collection iteration order.
                    let mut vector_hits = vector_hits;
                    vector_hits.sort_by(|a, b| {
                        b.score
                            .partial_cmp(&a.score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    for (i, hit) in vector_hits.iter_mut().enumerate() {
                        hit.rank = (i + 1) as u32;
                    }

                    total_candidates += vector_hits.len();
                    let vector_response =
                        SearchResponse::new(vector_hits, false, SearchStats::new(0, 0));
                    primitive_results.push((PrimitiveType::Vector, vector_response));
                }
            }
        }

        // 5. Combine results: keyword mode merges by raw score,
        //    hybrid mode fuses with RRF across BM25 + vector lists.
        let fused = if req.mode == SearchMode::Keyword {
            crate::fuser::merge_by_score(primitive_results, req.k)
        } else {
            self.fuser.fuse(primitive_results, req.k)
        };

        // 6. Build stats
        let stats = SearchStats::new(start.elapsed().as_micros() as u64, total_candidates);

        Ok(SearchResponse {
            hits: fused.hits,
            truncated: any_truncated || fused.truncated,
            stats,
        })
    }

    // ========================================================================
    // Primitive Selection
    // ========================================================================

    /// Select which primitives to search based on request filters
    fn select_primitives(&self, req: &SearchRequest) -> Vec<PrimitiveType> {
        match &req.primitive_filter {
            Some(filter) => filter.clone(),
            None => PrimitiveType::all().to_vec(),
        }
    }

    // ========================================================================
    // Budget Allocation
    // ========================================================================

    /// Allocate budget across primitives
    ///
    /// Simple proportional allocation: divide time evenly.
    /// Future: could weight by primitive "importance" or size.
    fn allocate_budgets(&self, req: &SearchRequest, num_primitives: usize) -> Vec<SearchBudget> {
        if num_primitives == 0 {
            return vec![];
        }

        let per_primitive_time = req.budget.max_wall_time_micros / num_primitives as u64;
        let per_primitive_candidates = req.budget.max_candidates_per_primitive;

        vec![
            SearchBudget {
                max_wall_time_micros: per_primitive_time,
                max_candidates: per_primitive_candidates,
                max_candidates_per_primitive: per_primitive_candidates,
            };
            num_primitives
        ]
    }

    // ========================================================================
    // Per-Primitive Search
    // ========================================================================

    /// Execute search on a single primitive
    fn search_primitive(
        &self,
        primitive: PrimitiveType,
        req: &SearchRequest,
    ) -> StrataResult<SearchResponse> {
        use strata_engine::Searchable;

        match primitive {
            PrimitiveType::Kv => self.kv.search(req),
            PrimitiveType::Json => self.json.search(req),
            PrimitiveType::Event => self.event.search(req),
            PrimitiveType::State => self.state.search(req),
            PrimitiveType::Branch => self.branch_index.search(req),
            // Vector primitive now implements Searchable.
            // Per M8_ARCHITECTURE.md Section 12.3:
            // - Keyword search returns empty (by design)
            // - For vector/hybrid search with embeddings, the orchestrator
            //   should call vector.search_response() directly with the embedding
            PrimitiveType::Vector => Searchable::search(&self.vector, req),
        }
    }

    /// Search with query expansion: run multiple queries and fuse with weighted RRF.
    ///
    /// Takes the original query plus expanded variants. Each expanded query runs
    /// through the full HybridSearch pipeline. Results are fused with weighted RRF
    /// where the original query gets `original_weight` and expansions get 1.0.
    ///
    /// Batch-embeds all hybrid expansion texts upfront (single batched GPU call)
    /// to avoid N serial embedding calls.
    ///
    /// # Search mode by query type
    ///
    /// - `Lex` expansions: Keyword mode (BM25 only)
    /// - `Vec` expansions: Hybrid mode (BM25 + vector)
    /// - `Hyde` expansions: Hybrid mode (embed the hypothetical text)
    pub fn search_expanded(
        &self,
        req: &SearchRequest,
        expansions: &[crate::expand::ExpandedQuery],
        original_weight: f32,
    ) -> StrataResult<SearchResponse> {
        use crate::expand::QueryType;
        use crate::fuser::weighted_rrf_fuse;

        let start = Instant::now();
        let mut result_lists: Vec<(SearchResponse, f32)> = Vec::new();

        // Batch-embed all texts that need embedding (original query + Vec/Hyde expansions).
        // This turns N serial GPU calls into 1 batched call.
        let mut texts_to_embed: Vec<&str> = Vec::new();
        // Index 0 = original query
        texts_to_embed.push(&req.query);
        // Indices 1..N = expansion texts that need embedding (Vec/Hyde only)
        let mut expansion_embed_indices: Vec<Option<usize>> = Vec::new();
        for expansion in expansions {
            match expansion.query_type {
                QueryType::Vec | QueryType::Hyde => {
                    expansion_embed_indices.push(Some(texts_to_embed.len()));
                    texts_to_embed.push(&expansion.text);
                }
                QueryType::Lex => {
                    expansion_embed_indices.push(None);
                }
            }
        }

        let embeddings = if let Some(embedder) = &self.embedder {
            embedder.embed_batch(&texts_to_embed)
        } else {
            vec![None; texts_to_embed.len()]
        };

        // Pass 0: original query with Hybrid mode and original_weight
        let mut original_req = req.clone().with_mode(SearchMode::Hybrid);
        if let Some(emb) = embeddings[0].clone() {
            original_req = original_req.with_precomputed_embedding(emb);
        }
        let original_response = self.search(&original_req)?;
        result_lists.push((original_response, original_weight));

        // Expansion passes
        for (i, expansion) in expansions.iter().enumerate() {
            let mode = match expansion.query_type {
                QueryType::Lex => SearchMode::Keyword,
                QueryType::Vec | QueryType::Hyde => SearchMode::Hybrid,
            };

            let mut exp_req = SearchRequest::new(req.branch_id, &expansion.text)
                .with_k(req.k)
                .with_mode(mode)
                .with_budget(req.budget);

            if let Some(ref filter) = req.primitive_filter {
                exp_req = exp_req.with_primitive_filter(filter.clone());
            }
            if let Some((start, end)) = req.time_range {
                exp_req = exp_req.with_time_range(start, end);
            }

            // Attach precomputed embedding for Vec/Hyde expansions
            if let Some(embed_idx) = expansion_embed_indices[i] {
                if let Some(emb) = embeddings[embed_idx].clone() {
                    exp_req = exp_req.with_precomputed_embedding(emb);
                }
            }

            match self.search(&exp_req) {
                Ok(response) => result_lists.push((response, 1.0)),
                Err(e) => {
                    tracing::warn!(
                        target: "strata::search",
                        error = %e,
                        expansion = %expansion.text,
                        "Expansion search failed, skipping"
                    );
                }
            }
        }

        // Fuse all results with weighted RRF
        let fused = weighted_rrf_fuse(result_lists, 60, req.k);

        let stats = strata_engine::search::SearchStats::new(
            start.elapsed().as_micros() as u64,
            0, // total candidates not tracked across passes
        );

        Ok(SearchResponse {
            hits: fused.hits,
            truncated: fused.truncated,
            stats,
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::BranchId;
    use strata_core::value::Value;
    use strata_engine::search::{EntityRef, SearchHit};

    fn test_db() -> Arc<Database> {
        Database::cache().expect("Failed to create test database")
    }

    #[test]
    fn test_hybrid_search_new() {
        let db = test_db();
        let _hybrid = HybridSearch::new(db);
        // Should compile and not panic - primitives share the same database
    }

    #[test]
    fn test_hybrid_search_empty_db_returns_no_hits() {
        let db = test_db();
        let hybrid = HybridSearch::new(db);
        let branch_id = BranchId::new();

        let req = SearchRequest::new(branch_id, "test");
        let response = hybrid.search(&req).unwrap();

        assert!(response.hits.is_empty());
        assert!(!response.truncated);
    }

    #[test]
    fn test_orchestration_does_not_crash_with_data() {
        // Primitives' Searchable impls return empty (search is done at executor layer
        // via InvertedIndex). This test verifies the orchestration pipeline completes
        // without errors when data exists in primitives.
        let db = test_db();
        let kv = KVStore::new(db.clone());
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "hello",
            Value::String("world test data".into()),
        )
        .unwrap();
        kv.put(
            &branch_id,
            "default",
            "test",
            Value::String("this is a test".into()),
        )
        .unwrap();

        let hybrid = HybridSearch::new(db);
        let req =
            SearchRequest::new(branch_id, "test").with_primitive_filter(vec![PrimitiveType::Kv]);
        let response = hybrid.search(&req).unwrap();

        // Primitives return empty SearchResponses — actual text search is done at
        // the executor layer via build_search_response + InvertedIndex. See
        // tests/intelligence/search_correctness.rs for end-to-end verification.
        assert!(!response.truncated);
    }

    #[test]
    fn test_primitive_filter_selects_correct_primitives() {
        let db = test_db();
        let hybrid = HybridSearch::new(db);
        let branch_id = BranchId::new();

        let req_filtered = SearchRequest::new(branch_id, "test")
            .with_primitive_filter(vec![PrimitiveType::Kv, PrimitiveType::Json]);

        let primitives = hybrid.select_primitives(&req_filtered);
        assert_eq!(primitives.len(), 2);
        assert!(primitives.contains(&PrimitiveType::Kv));
        assert!(primitives.contains(&PrimitiveType::Json));

        // Without filter selects all 6 primitives
        let req_all = SearchRequest::new(branch_id, "test");
        let all_primitives = hybrid.select_primitives(&req_all);
        assert_eq!(all_primitives.len(), 6);
    }

    #[test]
    fn test_budget_allocation_divides_evenly() {
        let db = test_db();
        let hybrid = HybridSearch::new(db);
        let branch_id = BranchId::new();

        let req = SearchRequest::new(branch_id, "test");

        let budgets = hybrid.allocate_budgets(&req, 3);
        assert_eq!(budgets.len(), 3);

        let expected_time = req.budget.max_wall_time_micros / 3;
        for budget in &budgets {
            assert_eq!(budget.max_wall_time_micros, expected_time);
        }

        // Edge case: 0 primitives
        let empty_budgets = hybrid.allocate_budgets(&req, 0);
        assert!(empty_budgets.is_empty());
    }

    #[test]
    fn test_hybrid_search_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HybridSearch>();
    }

    /// Verify that fuser produces ranked results when primitives return hits.
    ///
    /// This directly tests the fuse → response path that's opaque in orchestration
    /// tests (since engine-level Searchable impls return empty).
    #[test]
    fn test_fuser_produces_ranked_output() {
        let branch_id = BranchId::new();

        // Simulate two primitives returning overlapping hits.
        // doc_b appears in both — RRF should merge it and boost its score.
        let kv_hits = vec![
            SearchHit::new(
                EntityRef::Kv {
                    branch_id,
                    key: "doc_a".into(),
                },
                0.9,
                1,
            ),
            SearchHit::new(
                EntityRef::Kv {
                    branch_id,
                    key: "doc_b".into(),
                },
                0.7,
                2,
            ),
        ];
        let event_hits = vec![
            SearchHit::new(
                EntityRef::Kv {
                    branch_id,
                    key: "doc_b".into(),
                },
                0.8,
                1,
            ),
            SearchHit::new(
                EntityRef::Kv {
                    branch_id,
                    key: "doc_c".into(),
                },
                0.6,
                2,
            ),
        ];

        let primitive_results = vec![
            (
                PrimitiveType::Kv,
                SearchResponse::new(kv_hits, false, SearchStats::new(100, 2)),
            ),
            (
                PrimitiveType::Event,
                SearchResponse::new(event_hits, false, SearchStats::new(100, 2)),
            ),
        ];

        let fuser = RRFFuser::default();
        let fused = fuser.fuse(primitive_results, 10);

        // 3 unique entities: doc_a, doc_b (merged from both), doc_c
        assert_eq!(fused.hits.len(), 3, "Expected 3 fused hits");

        // Ranks should be 1, 2, 3
        for (i, hit) in fused.hits.iter().enumerate() {
            assert_eq!(hit.rank, (i + 1) as u32);
            assert!(hit.score > 0.0, "RRF scores should be positive");
        }

        // Scores should be in descending order
        for window in fused.hits.windows(2) {
            assert!(
                window[0].score >= window[1].score,
                "Hits should be sorted by score descending"
            );
        }

        // doc_b appeared in both result lists → its RRF score should be highest
        // (boosted by appearing at rank 2 + rank 1 across two lists)
        let top_hit = &fused.hits[0];
        assert_eq!(
            top_hit.doc_ref,
            EntityRef::Kv {
                branch_id,
                key: "doc_b".into()
            },
            "doc_b should be top-ranked (appears in both primitive results)"
        );
    }

    // ========================================================================
    // with_embedder and QueryEmbedder tests
    // ========================================================================

    /// Mock embedder that always returns a fixed vector.
    struct FixedEmbedder(Vec<f32>);

    impl crate::QueryEmbedder for FixedEmbedder {
        fn embed(&self, _text: &str) -> Option<Vec<f32>> {
            Some(self.0.clone())
        }
    }

    /// Mock embedder that always fails (returns None).
    struct FailingEmbedder;

    impl crate::QueryEmbedder for FailingEmbedder {
        fn embed(&self, _text: &str) -> Option<Vec<f32>> {
            None
        }
    }

    #[test]
    fn test_with_embedder_constructs_successfully() {
        let db = test_db();
        let embedder = Arc::new(FixedEmbedder(vec![0.1; 384]));
        let hybrid = HybridSearch::with_embedder(db, embedder);
        // Verify it has an embedder
        assert!(hybrid.embedder.is_some());
    }

    #[test]
    fn test_new_has_no_embedder() {
        let db = test_db();
        let hybrid = HybridSearch::new(db);
        assert!(hybrid.embedder.is_none());
    }

    #[test]
    fn test_with_embedder_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HybridSearch>();
        // Also verify Arc<dyn QueryEmbedder> is Send + Sync
        fn assert_embedder_trait<T: crate::QueryEmbedder>() {}
        assert_embedder_trait::<FixedEmbedder>();
    }

    #[test]
    fn test_hybrid_mode_without_embedder_skips_vector_search() {
        // With no embedder, hybrid mode should still work — just no vector hits.
        let db = test_db();
        let hybrid = HybridSearch::new(db);
        let branch_id = BranchId::new();

        let req = SearchRequest::new(branch_id, "test").with_mode(SearchMode::Hybrid);
        let response = hybrid.search(&req).unwrap();
        // Should succeed without panicking — vector search block is skipped
        assert!(response.hits.is_empty());
    }

    #[test]
    fn test_hybrid_mode_with_failing_embedder_skips_vector_search() {
        // Embedder returns None → should gracefully skip vector search.
        let db = test_db();
        let embedder = Arc::new(FailingEmbedder);
        let hybrid = HybridSearch::with_embedder(db, embedder);
        let branch_id = BranchId::new();

        let req = SearchRequest::new(branch_id, "test").with_mode(SearchMode::Hybrid);
        let response = hybrid.search(&req).unwrap();
        assert!(response.hits.is_empty());
    }

    #[test]
    fn test_hybrid_mode_with_embedder_attempts_vector_search() {
        // Embedder returns a vector, but no shadow collections exist → no vector hits,
        // but the code path through the embedder should be exercised without error.
        let db = test_db();
        let embedder = Arc::new(FixedEmbedder(vec![0.1; 384]));
        let hybrid = HybridSearch::with_embedder(db, embedder);
        let branch_id = BranchId::new();

        let req = SearchRequest::new(branch_id, "test").with_mode(SearchMode::Hybrid);
        let response = hybrid.search(&req).unwrap();
        // No shadow collections populated → no vector hits, but no panic either
        assert!(response.hits.is_empty());
    }

    #[test]
    fn test_keyword_mode_does_not_invoke_embedder() {
        // In keyword mode, the embedder should never be called.
        use std::sync::atomic::{AtomicBool, Ordering};

        struct TrackingEmbedder(AtomicBool);
        impl crate::QueryEmbedder for TrackingEmbedder {
            fn embed(&self, _text: &str) -> Option<Vec<f32>> {
                self.0.store(true, Ordering::SeqCst);
                Some(vec![0.1; 384])
            }
        }

        let db = test_db();
        let embedder = Arc::new(TrackingEmbedder(AtomicBool::new(false)));
        let embedder_ref = embedder.clone();
        let hybrid = HybridSearch::with_embedder(db, embedder);
        let branch_id = BranchId::new();

        let req = SearchRequest::new(branch_id, "test").with_mode(SearchMode::Keyword);
        let _response = hybrid.search(&req).unwrap();
        assert!(
            !embedder_ref.0.load(Ordering::SeqCst),
            "Embedder should NOT be called in keyword mode"
        );
    }

    #[test]
    fn test_with_embedder_clone_preserves_embedder() {
        let db = test_db();
        let embedder = Arc::new(FixedEmbedder(vec![0.1; 384]));
        let hybrid = HybridSearch::with_embedder(db, embedder);
        let cloned = hybrid.clone();
        assert!(cloned.embedder.is_some());
    }
}
