//! Retrieval substrate — the model-free, deterministic core of Strata search.
//!
//! The substrate executes a resolved [`Recipe`] to produce ranked results.
//! It is the single entry point for all retrieval. The intelligence layer
//! (embedding, expansion, reranking, RAG) wraps the substrate — model-dependent
//! operations happen outside, not inside.
//!
//! # Invariants
//!
//! - **INV-1 Deterministic.** Same recipe + same snapshot = identical results.
//! - **INV-2 Declarative.** The recipe is the complete spec. No hidden state.
//! - **INV-3 Snapshot-isolated.** Retrieval runs against a consistent MVCC snapshot.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use strata_core::types::BranchId;
use strata_core::StrataResult;
use strata_engine::search::recipe::FusionConfig;
use strata_engine::search::{
    EntityRef, PrimitiveType, Recipe, SearchHit, SearchMode, SearchRequest, Searchable,
};
use strata_engine::Database;

// Primitive facades
use strata_engine::primitives::{EventLog, JsonStore, KVStore};
use strata_graph::GraphStore;
use strata_vector::VectorStore;

// ============================================================================
// Types
// ============================================================================

/// Input to the retrieval substrate.
pub struct RetrievalRequest {
    /// Query text (used by BM25 for scoring).
    pub query: String,
    /// Branch to search within.
    pub branch_id: BranchId,
    /// Space to search within (defaults to "default").
    pub space: String,
    /// Resolved recipe (three-level merge already applied by caller).
    pub recipe: Recipe,
    /// Precomputed query embedding (provided by intelligence layer).
    pub embedding: Option<Vec<f32>>,
    /// Optional time range filter (start_micros, end_micros).
    pub time_range: Option<(u64, u64)>,
    /// Optional primitive filter (restrict which primitives are searched).
    pub primitive_filter: Option<Vec<PrimitiveType>>,
}

/// Fixed response format — all fields always present.
pub struct RetrievalResponse {
    /// Ranked search hits.
    pub hits: Vec<SearchHit>,
    /// Generated answer (None in v0.1).
    pub answer: Option<String>,
    /// Temporal diff (None in v0.1).
    pub diff: Option<serde_json::Value>,
    /// Aggregations (None in v0.1).
    pub aggregations: Option<serde_json::Value>,
    /// Grouped results (None in v0.1).
    pub groups: Option<serde_json::Value>,
    /// Execution statistics.
    pub stats: RetrievalStats,
}

/// Per-retrieval execution statistics.
pub struct RetrievalStats {
    /// MVCC snapshot version used for this retrieval.
    pub snapshot_version: u64,
    /// Which recipe was used (e.g., "builtin", "branch:default", "inline").
    pub recipe_used: String,
    /// Total wall-clock time in milliseconds.
    pub elapsed_ms: f64,
    /// Per-stage timing and candidate counts.
    pub stages: HashMap<String, StageStats>,
    /// Whether the budget was exhausted.
    pub budget_exhausted: bool,
}

/// Statistics for a single pipeline stage.
pub struct StageStats {
    /// Wall-clock time in milliseconds.
    pub elapsed_ms: f64,
    /// Number of candidates produced by this stage.
    pub candidates: usize,
}

// ============================================================================
// Retrieve
// ============================================================================

/// Execute a recipe against the database, returning ranked results.
///
/// This is the single entry point for all retrieval. The caller is responsible
/// for resolving the recipe (three-level merge) and embedding the query
/// (intelligence layer) before calling this function.
pub fn retrieve(db: &Arc<Database>, request: &RetrievalRequest) -> StrataResult<RetrievalResponse> {
    let start = Instant::now();
    let recipe = &request.recipe;
    let mut candidate_lists: Vec<(String, Vec<SearchHit>)> = Vec::new();
    let mut stages = HashMap::new();

    // INV-3: Snapshot isolation — all primitives see the same version.
    let snapshot = db.current_version();

    // ---- Step 1: BM25 retrieval across primitives ----
    if let Some(bm25_cfg) = recipe.retrieve.as_ref().and_then(|r| r.bm25.as_ref()) {
        let bm25_start = Instant::now();
        let k = bm25_cfg.k.unwrap_or(50);

        let mut search_req = SearchRequest::new(request.branch_id, &request.query)
            .with_k(k)
            .with_mode(SearchMode::Keyword)
            .with_space(&request.space)
            .with_snapshot_version(snapshot);
        if let Some((start, end)) = request.time_range {
            search_req = search_req.with_time_range(start, end);
        }
        if let Some(ref filter) = request.primitive_filter {
            search_req = search_req.with_primitive_filter(filter.clone());
        }

        let mut bm25_hits = Vec::new();

        // Fan out to BM25-capable primitives, respecting primitive filter.
        let all_primitives: Vec<(PrimitiveType, Box<dyn Searchable>)> = vec![
            (PrimitiveType::Kv, Box::new(KVStore::new(db.clone()))),
            (PrimitiveType::Json, Box::new(JsonStore::new(db.clone()))),
            (PrimitiveType::Event, Box::new(EventLog::new(db.clone()))),
            (PrimitiveType::Graph, Box::new(GraphStore::new(db.clone()))),
        ];

        for (kind, prim) in &all_primitives {
            if let Some(ref filter) = request.primitive_filter {
                if !filter.contains(kind) {
                    continue;
                }
            }
            match prim.search(&search_req) {
                Ok(resp) => bm25_hits.extend(resp.hits),
                Err(e) => tracing::warn!(
                    primitive = %prim.primitive_kind(),
                    error = %e,
                    "BM25 retrieval error, skipping primitive"
                ),
            }
        }

        // Sort by score descending, deterministic tie-breaking by entity hash.
        bm25_hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| entity_ref_order(&a.doc_ref, &b.doc_ref))
        });
        bm25_hits.truncate(k);

        stages.insert(
            "bm25".into(),
            StageStats {
                elapsed_ms: bm25_start.elapsed().as_secs_f64() * 1000.0,
                candidates: bm25_hits.len(),
            },
        );
        candidate_lists.push(("bm25".into(), bm25_hits));
    }

    // ---- Step 2: Vector retrieval ----
    if let Some(vec_cfg) = recipe.retrieve.as_ref().and_then(|r| r.vector.as_ref()) {
        if let Some(embedding) = &request.embedding {
            let vec_start = Instant::now();
            let k = vec_cfg.k.unwrap_or(50);

            let mut search_req = SearchRequest::new(request.branch_id, &request.query)
                .with_k(k)
                .with_mode(SearchMode::Vector)
                .with_space(&request.space)
                .with_precomputed_embedding(embedding.clone())
                .with_snapshot_version(snapshot);
            if let Some((start, end)) = request.time_range {
                search_req = search_req.with_time_range(start, end);
            }

            let vector = VectorStore::new(db.clone());
            match Searchable::search(&vector, &search_req) {
                Ok(resp) => {
                    stages.insert(
                        "vector".into(),
                        StageStats {
                            elapsed_ms: vec_start.elapsed().as_secs_f64() * 1000.0,
                            candidates: resp.hits.len(),
                        },
                    );
                    candidate_lists.push(("vector".into(), resp.hits));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Vector retrieval error, skipping");
                }
            }
        }
    }

    // ---- Step 3: RRF Fusion ----
    let fusion_start = Instant::now();
    let fused = rrf_fuse(&candidate_lists, recipe.fusion.as_ref());
    stages.insert(
        "fusion".into(),
        StageStats {
            elapsed_ms: fusion_start.elapsed().as_secs_f64() * 1000.0,
            candidates: fused.len(),
        },
    );

    // ---- Step 4: Transform (limit) ----
    let limit = recipe
        .transform
        .as_ref()
        .and_then(|t| t.limit)
        .unwrap_or(10);
    let mut hits: Vec<SearchHit> = fused.into_iter().take(limit).collect();

    // Re-assign ranks after limiting (1-indexed).
    for (i, hit) in hits.iter_mut().enumerate() {
        hit.rank = (i + 1) as u32;
    }

    // ---- Step 5: Return fixed-format response ----
    Ok(RetrievalResponse {
        hits,
        answer: None,
        diff: None,
        aggregations: None,
        groups: None,
        stats: RetrievalStats {
            snapshot_version: snapshot,
            recipe_used: "resolved".into(),
            elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
            stages,
            budget_exhausted: false,
        },
    })
}

// ============================================================================
// RRF Fusion
// ============================================================================

/// Reciprocal Rank Fusion across named candidate lists.
///
/// Score = sum(weight / (k + rank + 1)) per source.
/// Deterministic tie-breaking: score desc, then entity hash (INV-1).
pub fn rrf_fuse(
    sources: &[(String, Vec<SearchHit>)],
    fusion_cfg: Option<&FusionConfig>,
) -> Vec<SearchHit> {
    if sources.is_empty() {
        return Vec::new();
    }

    // Single source: pass through (no fusion needed).
    if sources.len() == 1 {
        return sources[0].1.clone();
    }

    let k = fusion_cfg.and_then(|c| c.k).unwrap_or(60) as f32;
    let weights = fusion_cfg.and_then(|c| c.weights.as_ref());

    let mut scores: HashMap<EntityRef, f32> = HashMap::new();
    let mut best_hit: HashMap<EntityRef, SearchHit> = HashMap::new();

    for (source_name, hits) in sources {
        let weight = weights
            .and_then(|w| w.get(source_name))
            .copied()
            .unwrap_or(1.0);

        for (rank, hit) in hits.iter().enumerate() {
            let rrf_score = weight / (k + rank as f32 + 1.0);
            *scores.entry(hit.doc_ref.clone()).or_default() += rrf_score;
            best_hit
                .entry(hit.doc_ref.clone())
                .or_insert_with(|| hit.clone());
        }
    }

    // Build result list with fused scores.
    let mut results: Vec<SearchHit> = scores
        .into_iter()
        .map(|(entity, score)| {
            let mut hit = best_hit.remove(&entity).unwrap();
            hit.score = score;
            hit
        })
        .collect();

    // INV-1: Deterministic ordering — score desc, then entity hash for tie-breaking.
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| entity_ref_order(&a.doc_ref, &b.doc_ref))
    });

    results
}

/// Deterministic ordering for EntityRef (Hash-based since EntityRef doesn't impl Ord).
fn entity_ref_order(a: &EntityRef, b: &EntityRef) -> Ordering {
    let ha = hash_entity(a);
    let hb = hash_entity(b);
    ha.cmp(&hb)
}

fn hash_entity(e: &EntityRef) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    e.hash(&mut hasher);
    hasher.finish()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::Value;
    use strata_engine::search::recipe::{TransformConfig, VectorRetrieveConfig};
    use strata_engine::search::{builtin_defaults, Recipe};

    /// Helper: create a test database and insert KV data.
    /// Returns (db, branch_id) so tests can use the same branch for search.
    fn setup_db_with_kv(entries: &[(&str, &str)]) -> (Arc<Database>, BranchId) {
        let db = Database::cache().expect("Failed to create test database");
        let branch_id = BranchId::new();
        let kv = KVStore::new(db.clone());
        for (key, value) in entries {
            kv.put(&branch_id, "default", key, Value::String((*value).into()))
                .expect("Failed to put KV");
        }
        // Flush to ensure InvertedIndex has processed documents.
        db.flush().expect("Failed to flush");
        (db, branch_id)
    }

    #[test]
    fn test_retrieve_bm25_only() {
        let (db, branch_id) = setup_db_with_kv(&[
            ("doc1", "the quick brown fox"),
            ("doc2", "lazy brown dog"),
            ("doc3", "something completely different"),
        ]);
        let recipe = builtin_defaults(); // BM25 only, no vector
        let request = RetrievalRequest {
            query: "brown fox".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
        };

        let response = retrieve(&db, &request).unwrap();

        assert!(!response.hits.is_empty(), "Should have BM25 hits");
        assert!(response.answer.is_none());
        assert!(response.diff.is_none());
        assert!(response.aggregations.is_none());
        assert!(response.groups.is_none());
        assert!(response.stats.elapsed_ms > 0.0);
        assert!(response.stats.stages.contains_key("bm25"));
        assert!(response.stats.stages.contains_key("fusion"));

        // Ranks should be 1-indexed and sequential.
        for (i, hit) in response.hits.iter().enumerate() {
            assert_eq!(hit.rank, (i + 1) as u32);
        }
    }

    #[test]
    fn test_retrieve_limit() {
        let (db, branch_id) = setup_db_with_kv(&[
            ("a", "test data one"),
            ("b", "test data two"),
            ("c", "test data three"),
            ("d", "test data four"),
            ("e", "test data five"),
        ]);

        let mut recipe = builtin_defaults();
        recipe.transform = Some(TransformConfig {
            limit: Some(2),
            ..Default::default()
        });

        let request = RetrievalRequest {
            query: "test data".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
        };

        let response = retrieve(&db, &request).unwrap();
        assert!(response.hits.len() <= 2, "Limit should cap at 2");
    }

    #[test]
    fn test_retrieve_empty_db() {
        let db = Database::cache().expect("Failed to create test database");
        let recipe = builtin_defaults();
        let request = RetrievalRequest {
            query: "anything".into(),
            branch_id: BranchId::default(),
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
        };

        let response = retrieve(&db, &request).unwrap();
        assert!(response.hits.is_empty());
        assert!(!response.stats.budget_exhausted);
    }

    #[test]
    fn test_retrieve_deterministic() {
        let (db, branch_id) = setup_db_with_kv(&[
            ("x", "alpha beta gamma"),
            ("y", "beta gamma delta"),
            ("z", "gamma delta epsilon"),
        ]);
        let recipe = builtin_defaults();

        let req = RetrievalRequest {
            query: "gamma".into(),
            branch_id,
            space: "default".into(),
            recipe: recipe.clone(),
            embedding: None,
            time_range: None,
            primitive_filter: None,
        };

        let r1 = retrieve(&db, &req).unwrap();
        let r2 = retrieve(&db, &req).unwrap();

        assert_eq!(r1.hits.len(), r2.hits.len());
        for (a, b) in r1.hits.iter().zip(r2.hits.iter()) {
            assert_eq!(a.doc_ref, b.doc_ref);
            assert_eq!(a.score, b.score);
            assert_eq!(a.rank, b.rank);
        }
    }

    #[test]
    fn test_retrieve_no_bm25_section() {
        let (db, branch_id) = setup_db_with_kv(&[("doc", "some text")]);
        // Recipe with only vector config but no embedding — should return empty.
        let recipe = Recipe {
            retrieve: Some(strata_engine::search::recipe::RetrieveConfig {
                vector: Some(VectorRetrieveConfig::default()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let request = RetrievalRequest {
            query: "some text".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
        };

        let response = retrieve(&db, &request).unwrap();
        assert!(response.hits.is_empty());
    }

    // ---- RRF unit tests ----

    /// Fixed branch_id for RRF tests (EntityRef includes branch_id, so it must match).
    fn test_branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    fn make_hit(key: &str, score: f32, rank: u32) -> SearchHit {
        SearchHit {
            doc_ref: EntityRef::Kv {
                branch_id: test_branch(),
                key: key.into(),
            },
            score,
            rank,
            snippet: None,
        }
    }

    #[test]
    fn test_rrf_single_source() {
        let sources = vec![(
            "bm25".into(),
            vec![make_hit("a", 5.0, 1), make_hit("b", 3.0, 2)],
        )];

        let result = rrf_fuse(&sources, None);
        assert_eq!(result.len(), 2);
        // Single source passes through with original scores.
        assert_eq!(result[0].score, 5.0);
        assert_eq!(result[1].score, 3.0);
    }

    #[test]
    fn test_rrf_two_sources_overlap() {
        // Doc "a" appears in both lists — should get boosted.
        let sources = vec![
            (
                "bm25".into(),
                vec![make_hit("a", 5.0, 1), make_hit("b", 3.0, 2)],
            ),
            (
                "vector".into(),
                vec![make_hit("a", 0.9, 1), make_hit("c", 0.8, 2)],
            ),
        ];

        let result = rrf_fuse(&sources, None);
        assert_eq!(result.len(), 3); // a, b, c

        // "a" should be first (appears in both lists → highest RRF score).
        assert_eq!(
            result[0].doc_ref,
            EntityRef::Kv {
                branch_id: test_branch(),
                key: "a".into()
            }
        );

        // "a" score should be higher than "b" or "c" (boosted by dual presence).
        assert!(result[0].score > result[1].score);
    }

    #[test]
    fn test_rrf_weighted() {
        let sources = vec![
            ("bm25".into(), vec![make_hit("a", 5.0, 1)]),
            ("vector".into(), vec![make_hit("b", 0.9, 1)]),
        ];

        let mut weights = HashMap::new();
        weights.insert("bm25".into(), 2.0);
        weights.insert("vector".into(), 1.0);

        let cfg = FusionConfig {
            method: Some("rrf".into()),
            k: Some(60),
            weights: Some(weights),
        };

        let result = rrf_fuse(&sources, Some(&cfg));
        assert_eq!(result.len(), 2);

        // "a" from bm25 (weight 2.0) should outrank "b" from vector (weight 1.0).
        assert_eq!(
            result[0].doc_ref,
            EntityRef::Kv {
                branch_id: test_branch(),
                key: "a".into()
            }
        );
    }

    #[test]
    fn test_rrf_empty_sources() {
        let sources: Vec<(String, Vec<SearchHit>)> = vec![];
        let result = rrf_fuse(&sources, None);
        assert!(result.is_empty());
    }
}
