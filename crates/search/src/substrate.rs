//! Retrieval substrate — the model-free, deterministic core of Strata search.
//!
//! The substrate executes a resolved [`Recipe`] to produce ranked results.
//! It is the **single entry point** for all retrieval. The intelligence layer
//! (embedding, expansion, reranking, RAG) wraps the substrate — model-dependent
//! operations happen outside, not inside.
//!
//! # Invariants
//!
//! - **INV-1 Deterministic.** Same recipe + same snapshot = identical results.
//! - **INV-2 Declarative.** The recipe is the complete spec. No hidden state.
//! - **INV-3 Snapshot-isolated.** Retrieval runs against a consistent MVCC snapshot.
//!
//! # Parallelism
//!
//! BM25 fan-out and vector shadow-collection search run concurrently via
//! `rayon::join()`. Multiple shadow collections are searched in parallel via
//! `par_iter()`. This mirrors the performance characteristics of the former
//! `HybridSearch` orchestrator while keeping all retrieval in one code path.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use strata_core::id::CommitVersion;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::StrataResult;
use strata_engine::search::recipe::FusionConfig;
use strata_engine::search::{
    EntityRef, PrimitiveType, Recipe, SearchHit, SearchMode, SearchRequest, Searchable,
};
use strata_engine::system_space::SYSTEM_SPACE;
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
    /// Point-in-time search: only return results visible at this timestamp (microseconds).
    pub as_of: Option<u64>,
    /// Optional wall-clock budget in milliseconds. When exceeded, remaining stages
    /// are skipped and `budget_exhausted` is set in stats. `None` means no limit.
    pub budget_ms: Option<u64>,
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
    pub snapshot_version: CommitVersion,
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
/// This is the **single entry point** for all retrieval. The caller is responsible
/// for resolving the recipe (three-level merge) and embedding the query
/// (intelligence layer) before calling this function.
///
/// BM25 and vector retrieval run in parallel via `rayon::join()`. Shadow
/// collections are discovered dynamically and searched with `par_iter()`.
pub fn retrieve(db: &Arc<Database>, request: &RetrievalRequest) -> StrataResult<RetrievalResponse> {
    let start = Instant::now();
    let recipe = &request.recipe;
    let mut stages = HashMap::new();
    let mut budget_exhausted = false;

    // Compute deadline from optional budget.
    let deadline: Option<Instant> = request
        .budget_ms
        .map(|ms| start + std::time::Duration::from_millis(ms));

    // INV-3: Snapshot isolation — all primitives see the same version.
    let snapshot = db.current_version();

    let has_bm25 = recipe
        .retrieve
        .as_ref()
        .and_then(|r| r.bm25.as_ref())
        .is_some();
    let has_vector = recipe
        .retrieve
        .as_ref()
        .and_then(|r| r.vector.as_ref())
        .is_some()
        && request.embedding.is_some();

    // ---- Parallel BM25 + Vector retrieval via rayon::join() ----
    let (bm25_result, vector_result) = rayon::join(
        || -> (Vec<SearchHit>, Option<StageStats>, bool) {
            if !has_bm25 {
                return (Vec::new(), None, false);
            }
            let bm25_cfg = recipe.retrieve.as_ref().unwrap().bm25.as_ref().unwrap();
            let bm25_start = Instant::now();
            let k = bm25_cfg.k.unwrap_or(50);

            let bm25_k1 = bm25_cfg.k1.unwrap_or(0.9);
            let bm25_b = bm25_cfg.b.unwrap_or(0.4);
            let phrase_boost = bm25_cfg.phrase_boost.unwrap_or(2.0);
            let phrase_slop = bm25_cfg.phrase_slop.unwrap_or(0);
            let phrase_filter = bm25_cfg
                .phrase_mode
                .as_deref()
                .map(|m| m == "filter")
                .unwrap_or(false);
            let prox_enabled = bm25_cfg.proximity.unwrap_or(true);
            let prox_window = bm25_cfg.proximity_window.unwrap_or(10);
            let prox_weight = bm25_cfg.proximity_weight.unwrap_or(0.5);
            let mut search_req = SearchRequest::new(request.branch_id, &request.query)
                .with_k(k)
                .with_mode(SearchMode::Keyword)
                .with_space(&request.space)
                .with_snapshot_version(snapshot)
                .with_bm25_params(bm25_k1, bm25_b)
                .with_phrase_params(phrase_boost, phrase_slop, phrase_filter)
                .with_proximity_params(prox_enabled, prox_window, prox_weight);
            if let Some((s, e)) = request.time_range {
                search_req = search_req.with_time_range(s, e);
            }
            if let Some(ref filter) = request.primitive_filter {
                search_req = search_req.with_primitive_filter(filter.clone());
            }

            let mut bm25_hits = Vec::new();
            let mut over_budget = false;

            // Fan out to BM25-capable primitives, respecting primitive filter.
            let all_primitives: Vec<(PrimitiveType, Box<dyn Searchable>)> = vec![
                (PrimitiveType::Kv, Box::new(KVStore::new(db.clone()))),
                (PrimitiveType::Json, Box::new(JsonStore::new(db.clone()))),
                (PrimitiveType::Event, Box::new(EventLog::new(db.clone()))),
                (PrimitiveType::Graph, Box::new(GraphStore::new(db.clone()))),
            ];

            for (kind, prim) in &all_primitives {
                // Budget check between primitives.
                if let Some(dl) = deadline {
                    if Instant::now() >= dl {
                        over_budget = true;
                        break;
                    }
                }
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

            // Temporal post-filter: enforce as_of and time_range against BM25 hits.
            //
            // Vector hits get their own temporal filtering inside the vector
            // primitive (search_at for as_of, system_search_with_sources_in_range
            // for time_range — though the as_of path is currently a no-op
            // pending search_at_with_sources, see Phase 1 Part 2 below). BM25
            // primitives have no version-aware index, so the substrate filters
            // here by looking up each hit's commit timestamp via
            // `db.get_at_timestamp` and dropping hits that fall outside the
            // window.
            //
            // The namespace must come from **the hit's own space**, not the
            // request's. After Phase 0 the hit carries its real space; using
            // `request.space` here would silently miss hits in other tenants
            // (and potentially fetch the wrong entity if a key happens to
            // exist in `request.space` too).
            //
            // Coverage:
            //   - KV    : uses hit.doc_ref.space (Phase 0)
            //   - JSON  : uses hit.doc_ref.space (Phase 0)
            //   - Graph : uses hit.doc_ref.space (NEW — fixes Gap A leak)
            //   - Event : passes through (intrinsic event_time semantic
            //             deferred to v0.5)
            //   - Branch/Vector : pass through (don't appear in bm25_hits)
            if request.as_of.is_some() || request.time_range.is_some() {
                // Effective upper bound: the tighter of as_of and time_range.end.
                let upper = match (request.as_of, request.time_range) {
                    (Some(a), Some((_, e))) => a.min(e),
                    (Some(a), None) => a,
                    (None, Some((_, e))) => e,
                    (None, None) => unreachable!("outer guard ensures one is Some"),
                };

                // Closure: visibility + lower-bound check for any storage key.
                // Returns false on miss, tombstone, storage error, or commit
                // timestamp before time_range.start. Dropping on error is
                // safer than leaking unfiltered hits — `get_at_timestamp` is
                // unlikely to fail since the hit's underlying key was just
                // resolved by the inverted index a moment earlier.
                let time_range = request.time_range;
                let visible_in_window = |key: &Key| -> bool {
                    let versioned = match db.get_at_timestamp(key, upper) {
                        Ok(Some(v)) => v,
                        _ => return false,
                    };
                    if let Some((start, _)) = time_range {
                        if versioned.timestamp.as_micros() < start {
                            return false;
                        }
                    }
                    true
                };

                bm25_hits.retain(|hit| match &hit.doc_ref {
                    EntityRef::Kv { key, space, .. } => {
                        let ns = Arc::new(Namespace::new(request.branch_id, space.clone()));
                        visible_in_window(&Key::new_kv(ns, key.as_bytes()))
                    }
                    EntityRef::Json { doc_id, space, .. } => {
                        let ns = Arc::new(Namespace::new(request.branch_id, space.clone()));
                        visible_in_window(&Key::new_json(ns, doc_id))
                    }
                    EntityRef::Graph { key, space, .. } => {
                        let ns = Arc::new(Namespace::new(request.branch_id, space.clone()));
                        visible_in_window(&Key::new_graph(ns, key.as_bytes()))
                    }
                    // Events: append-only with intrinsic event_time, deferred to v0.5.
                    // Branch/Vector: don't appear in bm25_hits.
                    _ => true,
                });
            }

            let stats = StageStats {
                elapsed_ms: bm25_start.elapsed().as_secs_f64() * 1000.0,
                candidates: bm25_hits.len(),
            };
            (bm25_hits, Some(stats), over_budget)
        },
        || -> (Vec<SearchHit>, Option<StageStats>, bool) {
            if !has_vector {
                return (Vec::new(), None, false);
            }
            let vec_cfg = recipe.retrieve.as_ref().unwrap().vector.as_ref().unwrap();
            let embedding = request.embedding.as_ref().unwrap();
            let vec_start = Instant::now();
            let k = vec_cfg.k.unwrap_or(50);

            let vector = VectorStore::new(db.clone());

            // Discover shadow collections dynamically, or use explicit list from recipe.
            let explicit_collections = vec_cfg
                .collections
                .as_ref()
                .filter(|c| !c.is_empty())
                .cloned();

            let collection_names: Vec<String> = if let Some(explicit) = explicit_collections {
                explicit
            } else {
                // Dynamic discovery: list all _system_embed_* collections.
                vector
                    .list_collections(request.branch_id, SYSTEM_SPACE)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|c| c.name.starts_with("_system_embed_") && c.count > 0)
                    .map(|c| c.name)
                    .collect()
            };

            if collection_names.is_empty() {
                return (Vec::new(), None, false);
            }

            // Hits-from-results closure: shared by all three temporal modes.
            // Drops orphans and cross-space matches; builds SearchHit with
            // snippet. Centralised here so the as_of, time_range, and current
            // branches all enforce the same Phase 1 isolation guarantees.
            let to_hits = |result: strata_vector::VectorResult<
                Vec<strata_vector::VectorMatchWithSource>,
            >|
             -> Vec<SearchHit> {
                result
                    .into_iter()
                    .flat_map(|matches| matches.into_iter())
                    .filter_map(|m| {
                        // Phase 1 Part 2: hybrid retrieval must not leak
                        // across spaces. The shadow collection lives in
                        // SYSTEM_SPACE but each match's `source_ref`
                        // carries the real user space. Drop matches whose
                        // source isn't in the caller's space, and drop
                        // matches with no `source_ref` entirely — we
                        // cannot honestly attribute them to any user space.
                        let source = m.source_ref?;
                        if source.space() != Some(request.space.as_str()) {
                            return None;
                        }
                        let snippet = m
                            .metadata
                            .as_ref()
                            .map(|meta| truncate_text(&meta.to_string(), 100));
                        Some(SearchHit {
                            doc_ref: source,
                            score: m.score,
                            rank: 0,
                            snippet,
                        })
                    })
                    .collect::<Vec<_>>()
            };

            // Mode selection: 4-way match on (as_of, time_range). All four
            // arms feed `to_hits`, which centralises orphan-drop and
            // cross-space filtering. Symmetric with the BM25 branch — when
            // both knobs are set, the vector path now applies them as an
            // intersection (visible at `as_of` AND `created_at` inside
            // `[start, end]`), matching the contract PR #2365 made on the
            // BM25 side.
            let vec_hits: Vec<SearchHit> = collection_names
                .par_iter()
                .flat_map(|coll| {
                    let result = match (request.as_of, request.time_range) {
                        (Some(as_of_ts), Some((s, e))) => vector
                            .system_search_at_in_range_with_sources(
                                request.branch_id,
                                coll,
                                embedding,
                                k,
                                as_of_ts,
                                s,
                                e,
                            ),
                        (Some(as_of_ts), None) => vector.system_search_at_with_sources(
                            request.branch_id,
                            coll,
                            embedding,
                            k,
                            as_of_ts,
                        ),
                        (None, Some((s, e))) => vector.system_search_with_sources_in_range(
                            request.branch_id,
                            coll,
                            embedding,
                            k,
                            s,
                            e,
                        ),
                        (None, None) => {
                            vector.system_search_with_sources(request.branch_id, coll, embedding, k)
                        }
                    };
                    to_hits(result)
                })
                .collect();

            // Sort and truncate.
            let mut vec_hits = vec_hits;
            vec_hits.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| entity_ref_order(&a.doc_ref, &b.doc_ref))
            });
            vec_hits.truncate(k);
            for (i, hit) in vec_hits.iter_mut().enumerate() {
                hit.rank = (i + 1) as u32;
            }

            let stats = StageStats {
                elapsed_ms: vec_start.elapsed().as_secs_f64() * 1000.0,
                candidates: vec_hits.len(),
            };
            (vec_hits, Some(stats), false)
        },
    );

    // Collect results from parallel branches.
    let (bm25_hits, bm25_stats, bm25_over_budget) = bm25_result;
    let (vec_hits, vec_stats, _vec_over_budget) = vector_result;

    if bm25_over_budget {
        budget_exhausted = true;
    }

    let mut candidate_lists: Vec<(String, Vec<SearchHit>)> = Vec::new();

    if let Some(stats) = bm25_stats {
        stages.insert("bm25".into(), stats);
        if !bm25_hits.is_empty() {
            candidate_lists.push(("bm25".into(), bm25_hits));
        }
    }
    if let Some(stats) = vec_stats {
        stages.insert("vector".into(), stats);
        if !vec_hits.is_empty() {
            candidate_lists.push(("vector".into(), vec_hits));
        }
    }

    // ---- Budget check before fusion ----
    if let Some(dl) = deadline {
        if Instant::now() >= dl {
            budget_exhausted = true;
        }
    }

    // ---- RRF Fusion ----
    let fusion_start = Instant::now();
    let fused = rrf_fuse(&candidate_lists, recipe.fusion.as_ref());
    stages.insert(
        "fusion".into(),
        StageStats {
            elapsed_ms: fusion_start.elapsed().as_secs_f64() * 1000.0,
            candidates: fused.len(),
        },
    );

    // ---- Transform (limit) ----
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

    // ---- Return fixed-format response ----
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
            budget_exhausted,
        },
    })
}

/// Truncate text to approximately `max_chars` characters (Unicode-safe).
fn truncate_text(text: &str, max_chars: usize) -> String {
    let truncated: String = text.chars().take(max_chars).collect();
    if truncated.len() < text.len() {
        format!("{}...", truncated)
    } else {
        truncated
    }
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
    use strata_engine::database::OpenSpec;
    use strata_engine::search::recipe::{
        BM25Config, FusionConfig, RetrieveConfig, TransformConfig, VectorRetrieveConfig,
    };
    use strata_engine::search::Recipe;
    use strata_engine::SearchSubsystem;

    /// Simple keyword recipe for tests (BM25 + RRF + limit 10).
    fn test_recipe() -> Recipe {
        Recipe {
            version: Some(1),
            retrieve: Some(RetrieveConfig {
                bm25: Some(BM25Config {
                    k: Some(50),
                    k1: Some(0.9),
                    b: Some(0.4),
                    stemmer: Some("porter".into()),
                    stopwords: Some("lucene33".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            fusion: Some(FusionConfig {
                method: Some("rrf".into()),
                k: Some(60),
                ..Default::default()
            }),
            transform: Some(TransformConfig {
                limit: Some(10),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Helper: create a test database and insert KV data.
    /// Returns (db, branch_id) so tests can use the same branch for search.
    fn setup_db_with_kv(entries: &[(&str, &str)]) -> (Arc<Database>, BranchId) {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("Failed to create test database");
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
        let recipe = test_recipe(); // BM25 only, no vector
        let request = RetrievalRequest {
            query: "brown fox".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
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

        let mut recipe = test_recipe();
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
            as_of: None,
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        assert!(response.hits.len() <= 2, "Limit should cap at 2");
    }

    #[test]
    fn test_retrieve_empty_db() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("Failed to create test database");
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "anything".into(),
            branch_id: BranchId::default(),
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
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
        let recipe = test_recipe();

        let req = RetrievalRequest {
            query: "gamma".into(),
            branch_id,
            space: "default".into(),
            recipe: recipe.clone(),
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
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
            as_of: None,
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        assert!(response.hits.is_empty());
    }

    /// Phase 1 Part 2: vector stage in the substrate must drop matches whose
    /// `source_ref.space()` doesn't match the request, and must drop matches
    /// with no `source_ref` entirely.
    #[test]
    fn test_substrate_vector_filters_cross_space_sources() {
        use strata_engine::search::recipe::VectorRetrieveConfig;

        // Create a fresh DB and a shadow vector collection with three records:
        // one in tenant_a, one in tenant_b, one with no source_ref.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let vector_store = strata_vector::VectorStore::new(db.clone());
        let config =
            strata_vector::VectorConfig::new(3, strata_vector::DistanceMetric::Cosine).unwrap();
        vector_store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        vector_store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-a",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "tenant_a", "k"),
            )
            .unwrap();
        vector_store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-b",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "tenant_b", "k"),
            )
            .unwrap();
        vector_store
            .system_insert(
                branch_id,
                "_system_embed_kv",
                "shadow-orphan",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        db.flush().unwrap();

        // Hybrid recipe: BM25 + vector. BM25 finds nothing because there's
        // no KV data — only the vector stage produces hits.
        let recipe = Recipe {
            retrieve: Some(strata_engine::search::recipe::RetrieveConfig {
                bm25: Some(BM25Config {
                    k: Some(50),
                    ..Default::default()
                }),
                vector: Some(VectorRetrieveConfig {
                    k: Some(50),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            fusion: Some(FusionConfig {
                method: Some("rrf".into()),
                k: Some(60),
                ..Default::default()
            }),
            ..Default::default()
        };

        // Request from tenant_a → exactly the tenant_a hit
        let req_a = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "tenant_a".into(),
            recipe: recipe.clone(),
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };
        let resp_a = retrieve(&db, &req_a).unwrap();
        assert_eq!(
            resp_a.hits.len(),
            1,
            "tenant_a should see exactly its own hit, got {}",
            resp_a.hits.len()
        );
        match &resp_a.hits[0].doc_ref {
            EntityRef::Kv { space, .. } => assert_eq!(space, "tenant_a"),
            other => panic!("expected EntityRef::Kv, got {:?}", other),
        }

        // Request from a third space → zero hits (no source matches and
        // the orphan must be dropped).
        let req_c = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "tenant_c".into(),
            recipe,
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };
        let resp_c = retrieve(&db, &req_c).unwrap();
        assert_eq!(
            resp_c.hits.len(),
            0,
            "tenant_c has no matching sources and orphan must be dropped"
        );
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
                space: "default".to_string(),
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
                space: "default".to_string(),
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
                space: "default".to_string(),
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

    // ---- Temporal search tests ----

    #[test]
    fn test_retrieve_with_as_of() {
        // Insert doc at T1, sleep, insert another at T2.
        // Search with as_of=T1 should only return the first doc.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("Failed to create test database");
        let branch_id = BranchId::new();
        let kv = KVStore::new(db.clone());

        // Insert first doc
        kv.put(
            &branch_id,
            "default",
            "doc_early",
            Value::String("early document about rust".into()),
        )
        .expect("put");
        db.flush().expect("flush");

        // Capture timestamp after first insert
        let t1 = strata_core::Timestamp::now().as_micros();

        // Small delay to ensure distinct timestamps
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Insert second doc
        kv.put(
            &branch_id,
            "default",
            "doc_late",
            Value::String("late document about rust".into()),
        )
        .expect("put");
        db.flush().expect("flush");

        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: Some(t1),
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();

        // Only the early doc should be visible at T1
        let keys: Vec<&str> = response
            .hits
            .iter()
            .filter_map(|h| match &h.doc_ref {
                EntityRef::Kv { key, space: _, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            keys.contains(&"doc_early"),
            "Early doc should be visible at T1"
        );
        assert!(
            !keys.contains(&"doc_late"),
            "Late doc should NOT be visible at T1"
        );
    }

    #[test]
    fn test_retrieve_as_of_before_data() {
        // as_of before any data → empty results
        let (db, branch_id) = setup_db_with_kv(&[("doc", "some text about testing")]);
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "testing".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: Some(1), // microsecond 1 — before any data
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        assert!(
            response.hits.is_empty(),
            "No hits should exist before data was inserted"
        );
    }

    #[test]
    fn test_retrieve_as_of_none_returns_all() {
        // as_of=None should return all docs (regression test)
        let (db, branch_id) =
            setup_db_with_kv(&[("a", "alpha test data"), ("b", "beta test data")]);
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "test data".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        assert_eq!(response.hits.len(), 2, "Both docs should be returned");
    }

    // ---- Gap A + Gap C: temporal post-filter for graph + time_range ----

    /// Insert a KV value and return its observed commit timestamp.
    /// Reads the value back via `get_at_timestamp(u64::MAX)` so the test
    /// uses the actual storage commit time, robust against clock variance.
    fn put_kv_and_observe_ts(
        db: &Arc<Database>,
        branch_id: &BranchId,
        space: &str,
        key: &str,
        value: &str,
    ) -> u64 {
        let kv = KVStore::new(db.clone());
        kv.put(branch_id, space, key, Value::String(value.into()))
            .expect("kv put");
        db.flush().expect("flush");
        let storage_key = Key::new_kv(
            Arc::new(Namespace::new(*branch_id, space.to_string())),
            key.as_bytes(),
        );
        db.get_at_timestamp(&storage_key, u64::MAX)
            .expect("get_at_timestamp")
            .expect("just-written value should be visible")
            .timestamp
            .as_micros()
    }

    /// Insert a JSON document and return its observed commit timestamp.
    fn put_json_and_observe_ts(
        db: &Arc<Database>,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        json_str: &str,
    ) -> u64 {
        use strata_core::primitives::json::JsonValue;
        let json_store = JsonStore::new(db.clone());
        let value: JsonValue = json_str.parse().expect("parse json");
        json_store
            .create(branch_id, space, doc_id, value)
            .expect("json create");
        db.flush().expect("flush");
        let storage_key = Key::new_json(
            Arc::new(Namespace::new(*branch_id, space.to_string())),
            doc_id,
        );
        db.get_at_timestamp(&storage_key, u64::MAX)
            .expect("get_at_timestamp")
            .expect("just-written value should be visible")
            .timestamp
            .as_micros()
    }

    /// Insert a graph node and return its observed commit timestamp.
    /// `text` becomes the node's properties JSON so BM25 can index it.
    fn put_graph_node_and_observe_ts(
        db: &Arc<Database>,
        branch_id: &BranchId,
        space: &str,
        graph: &str,
        node_id: &str,
        text: &str,
    ) -> u64 {
        use strata_graph::types::NodeData;
        let graph_store = GraphStore::new(db.clone());
        let data = NodeData {
            entity_ref: None,
            properties: Some(serde_json::json!({ "body": text })),
            object_type: None,
        };
        graph_store
            .add_node(*branch_id, space, graph, node_id, data)
            .expect("add_node");
        db.flush().expect("flush");
        // Use the graph crate's own key helper rather than hardcoding the
        // "{graph}/n/{node_id}" format — protects against future format
        // changes in the graph crate.
        let user_key = strata_graph::keys::node_key(graph, node_id);
        let storage_key = Key::new_graph(
            Arc::new(Namespace::new(*branch_id, space.to_string())),
            user_key.as_bytes(),
        );
        db.get_at_timestamp(&storage_key, u64::MAX)
            .expect("get_at_timestamp")
            .expect("just-written graph node should be visible")
            .timestamp
            .as_micros()
    }

    /// Gap A regression: a graph node created after `as_of` must not appear
    /// in the result set. Without the new Graph arm in the post-filter, the
    /// `_ => true` fallthrough would let it leak through. Uses a non-default
    /// space ("tenant_a") to also exercise the multi-space namespace path.
    #[test]
    fn test_as_of_filters_graph_node_created_after_target() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        // Insert "early" graph node, then "late" 10ms later.
        let _t1 = put_graph_node_and_observe_ts(
            &db,
            &branch_id,
            "tenant_a",
            "g",
            "early",
            "rust language",
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mid = strata_core::Timestamp::now().as_micros();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _t2 = put_graph_node_and_observe_ts(
            &db,
            &branch_id,
            "tenant_a",
            "g",
            "late",
            "rust language",
        );

        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "tenant_a".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: Some(mid),
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        let graph_node_ids: Vec<&str> = response
            .hits
            .iter()
            .filter_map(|h| match &h.doc_ref {
                EntityRef::Graph { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            graph_node_ids.iter().any(|k| k.contains("early")),
            "early graph node should be visible at as_of=mid, got: {:?}",
            graph_node_ids
        );
        assert!(
            !graph_node_ids.iter().any(|k| k.contains("late")),
            "late graph node should be filtered out at as_of=mid, got: {:?}",
            graph_node_ids
        );
    }

    /// Gap C regression for KV: a hybrid query with `time_range` must drop
    /// BM25 hits whose latest visible commit timestamp falls outside the
    /// range. Without the new outer guard `... || request.time_range.is_some()`,
    /// the post-filter never fires for time_range and BM25 hits leak from
    /// any time, producing inconsistent result sets relative to vector hits.
    #[test]
    fn test_time_range_filters_kv_outside_range() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let _t1 = put_kv_and_observe_ts(&db, &branch_id, "default", "before", "rust early");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = put_kv_and_observe_ts(&db, &branch_id, "default", "inside", "rust middle");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _t3 = put_kv_and_observe_ts(&db, &branch_id, "default", "after", "rust late");

        // time_range = (t2, t2) — point at the middle doc only.
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: Some((t2, t2)),
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        let kv_keys: Vec<&str> = response
            .hits
            .iter()
            .filter_map(|h| match &h.doc_ref {
                EntityRef::Kv { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            kv_keys,
            vec!["inside"],
            "only the middle doc should survive the time_range filter, got: {:?}",
            kv_keys
        );
    }

    /// Same shape as above but for JSON documents — exercises the JSON arm
    /// of the post-filter through the time_range path.
    #[test]
    fn test_time_range_filters_json_outside_range() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let _t1 = put_json_and_observe_ts(
            &db,
            &branch_id,
            "default",
            "before",
            r#"{"text":"rust early doc"}"#,
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = put_json_and_observe_ts(
            &db,
            &branch_id,
            "default",
            "inside",
            r#"{"text":"rust middle doc"}"#,
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _t3 = put_json_and_observe_ts(
            &db,
            &branch_id,
            "default",
            "after",
            r#"{"text":"rust late doc"}"#,
        );

        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: Some((t2, t2)),
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        let json_doc_ids: Vec<&str> = response
            .hits
            .iter()
            .filter_map(|h| match &h.doc_ref {
                EntityRef::Json { doc_id, .. } => Some(doc_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            json_doc_ids,
            vec!["inside"],
            "only the middle JSON doc should survive the time_range filter, got: {:?}",
            json_doc_ids
        );
    }

    /// Same shape for graph nodes — exercises the new Graph arm through
    /// the time_range path.
    #[test]
    fn test_time_range_filters_graph_outside_range() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let _t1 = put_graph_node_and_observe_ts(
            &db,
            &branch_id,
            "default",
            "g",
            "before",
            "rust early node",
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = put_graph_node_and_observe_ts(
            &db,
            &branch_id,
            "default",
            "g",
            "inside",
            "rust middle node",
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _t3 = put_graph_node_and_observe_ts(
            &db,
            &branch_id,
            "default",
            "g",
            "after",
            "rust late node",
        );

        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: Some((t2, t2)),
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        let graph_keys: Vec<&str> = response
            .hits
            .iter()
            .filter_map(|h| match &h.doc_ref {
                EntityRef::Graph { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            graph_keys.len(),
            1,
            "exactly one graph hit, got: {:?}",
            graph_keys
        );
        assert!(
            graph_keys[0].contains("inside"),
            "only the middle graph node should survive, got: {:?}",
            graph_keys
        );
    }

    /// Both filters together: as_of upper bound + time_range lower+upper bound
    /// must compose by intersection. With docs at T1, T2, T3, T4 and
    /// `as_of=T3, time_range=(T2, T4)`, the effective upper is `min(T3, T4) = T3`
    /// and the lower is T2 — so docs at T2 and T3 should be returned, T1 and
    /// T4 dropped.
    #[test]
    fn test_time_range_and_as_of_combine() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let _t1 = put_kv_and_observe_ts(&db, &branch_id, "default", "d1", "rust one");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = put_kv_and_observe_ts(&db, &branch_id, "default", "d2", "rust two");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t3 = put_kv_and_observe_ts(&db, &branch_id, "default", "d3", "rust three");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t4 = put_kv_and_observe_ts(&db, &branch_id, "default", "d4", "rust four");

        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: Some((t2, t4)),
            primitive_filter: None,
            as_of: Some(t3),
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();
        let mut kv_keys: Vec<&str> = response
            .hits
            .iter()
            .filter_map(|h| match &h.doc_ref {
                EntityRef::Kv { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        kv_keys.sort();
        assert_eq!(
            kv_keys,
            vec!["d2", "d3"],
            "expected docs in [t2, min(t3,t4)=t3], got: {:?}",
            kv_keys
        );
    }

    /// Drive-by regression for the `KVStore::search` cross-primitive filter
    /// (kv.rs:651): when a query matches both a KV doc and a JSON doc, the
    /// substrate must return one hit per primitive owner — never duplicates.
    ///
    /// Before the kv.rs fix, `KVStore::search` returned every entity_ref that
    /// `index.score_top_k` resolved (regardless of variant), only checking
    /// `is_kv` for the snippet path. Combined with `rrf_fuse`'s single-source
    /// pass-through optimization (substrate.rs:530-532), this caused the JSON
    /// doc to appear twice in `bm25_hits` — once from `KVStore::search` and
    /// once from `JsonStore::search` — and the duplicates leaked all the way
    /// to the response. Adding the `if let EntityRef::Kv { .. } else None`
    /// restructure in kv.rs fixes it.
    ///
    /// This test failed with `["inside", "inside"]` before the kv.rs fix
    /// landed. It is the canonical regression guard for that filter.
    #[test]
    fn test_bm25_fan_out_no_cross_primitive_duplicates() {
        use strata_core::primitives::json::JsonValue;

        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        // One KV doc and one JSON doc, both indexed under the same branch+space
        // and both containing the query term "rust".
        let kv = KVStore::new(db.clone());
        kv.put(
            &branch_id,
            "default",
            "kv_doc",
            Value::String("rust kv content".into()),
        )
        .expect("kv put");

        let json_store = JsonStore::new(db.clone());
        let value: JsonValue = r#"{"text":"rust json content"}"#.parse().expect("parse json");
        json_store
            .create(&branch_id, "default", "json_doc", value)
            .expect("json create");
        db.flush().expect("flush");

        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };

        let response = retrieve(&db, &request).unwrap();

        let kv_count = response
            .hits
            .iter()
            .filter(|h| matches!(h.doc_ref, EntityRef::Kv { .. }))
            .count();
        let json_count = response
            .hits
            .iter()
            .filter(|h| matches!(h.doc_ref, EntityRef::Json { .. }))
            .count();
        let other_count = response.hits.len() - kv_count - json_count;

        assert_eq!(
            kv_count, 1,
            "expected exactly 1 KV hit, got hits: {:?}",
            response.hits
        );
        assert_eq!(
            json_count, 1,
            "expected exactly 1 JSON hit, got hits: {:?}",
            response.hits
        );
        assert_eq!(
            other_count, 0,
            "expected no Event/Graph hits in this scenario, got hits: {:?}",
            response.hits
        );
        assert_eq!(
            response.hits.len(),
            2,
            "expected exactly 2 total hits with no duplicates, got: {:?}",
            response.hits
        );
    }

    // ---- Budget enforcement tests ----

    #[test]
    fn test_retrieve_budget_no_limit() {
        let (db, branch_id) = setup_db_with_kv(&[("a", "test budget enforcement")]);
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "test".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None, // No limit
        };
        let response = retrieve(&db, &request).unwrap();
        assert!(!response.stats.budget_exhausted);
    }

    #[test]
    fn test_retrieve_budget_generous() {
        let (db, branch_id) = setup_db_with_kv(&[("a", "test budget generous")]);
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "test".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: Some(10_000), // 10 seconds — should complete
        };
        let response = retrieve(&db, &request).unwrap();
        assert!(!response.stats.budget_exhausted);
        assert!(!response.hits.is_empty());
    }

    #[test]
    fn test_retrieve_budget_zero_exhausts() {
        let (db, branch_id) = setup_db_with_kv(&[("a", "test budget zero")]);
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "test".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: Some(0), // Zero budget — exhausted immediately
        };
        let response = retrieve(&db, &request).unwrap();
        assert!(
            response.stats.budget_exhausted,
            "Zero budget should be exhausted"
        );
    }

    // ---- Rayon parallelism determinism test ----

    #[test]
    fn test_retrieve_parallel_deterministic() {
        // Run the same query many times to catch any non-determinism from rayon.
        let (db, branch_id) = setup_db_with_kv(&[
            ("p1", "alpha beta gamma delta"),
            ("p2", "beta gamma delta epsilon"),
            ("p3", "gamma delta epsilon zeta"),
            ("p4", "delta epsilon zeta eta"),
        ]);
        let recipe = test_recipe();
        let request = RetrievalRequest {
            query: "gamma delta".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: None,
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };

        let baseline = retrieve(&db, &request).unwrap();
        for _ in 0..10 {
            let run = retrieve(&db, &request).unwrap();
            assert_eq!(baseline.hits.len(), run.hits.len());
            for (a, b) in baseline.hits.iter().zip(run.hits.iter()) {
                assert_eq!(a.doc_ref, b.doc_ref, "Non-deterministic hit ordering");
                assert_eq!(a.score, b.score, "Non-deterministic scores");
                assert_eq!(a.rank, b.rank, "Non-deterministic ranks");
            }
        }
    }

    // ---- Temporal hybrid retrieval tests (Audit Finding 3) ----
    //
    // Pin the vector stage of the substrate when `as_of` is set. The path
    // goes through `system_search_at_with_sources`, which resolves source_ref
    // from the version chain at the requested timestamp. BM25 finds nothing
    // in these tests (no KV data), so any returned hit must have come from
    // the temporal vector branch.

    /// Build a hybrid recipe (BM25 + vector + RRF) for the temporal tests.
    fn temporal_hybrid_recipe() -> Recipe {
        Recipe {
            retrieve: Some(RetrieveConfig {
                bm25: Some(BM25Config {
                    k: Some(50),
                    ..Default::default()
                }),
                vector: Some(VectorRetrieveConfig {
                    k: Some(50),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            fusion: Some(FusionConfig {
                method: Some("rrf".into()),
                k: Some(60),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_substrate_temporal_hybrid_returns_vector_hits() {
        // Insert shadow-early, capture t1, insert shadow-late.
        // as_of=t1 → exactly the early hit. as_of=None → both.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let vector_store = strata_vector::VectorStore::new(db.clone());
        let config =
            strata_vector::VectorConfig::new(3, strata_vector::DistanceMetric::Cosine).unwrap();
        vector_store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        vector_store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-early",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "default", "k_early"),
            )
            .unwrap();
        db.flush().unwrap();

        // Capture timestamp after the first insert is visible.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t1 = strata_core::Timestamp::now().as_micros();
        std::thread::sleep(std::time::Duration::from_millis(10));

        vector_store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-late",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "default", "k_late"),
            )
            .unwrap();
        db.flush().unwrap();

        let recipe = temporal_hybrid_recipe();

        // as_of=t1 → only the early shadow.
        let req_at_t1 = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "default".into(),
            recipe: recipe.clone(),
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: Some(t1),
            budget_ms: None,
        };
        let resp_t1 = retrieve(&db, &req_at_t1).unwrap();
        // The test relies on BM25 contributing nothing — there is no KV/Json/
        // Event/Graph data, so any hit MUST come from the temporal vector
        // branch. Pin this so the test can't pass for the wrong reason if a
        // future change makes BM25 see shadow collections directly.
        assert_eq!(
            resp_t1
                .stats
                .stages
                .get("bm25")
                .map(|s| s.candidates)
                .unwrap_or(0),
            0,
            "BM25 must produce zero hits — vector branch is the only source"
        );
        assert_eq!(
            resp_t1.hits.len(),
            1,
            "as_of=t1 should yield exactly the early shadow's source"
        );
        match &resp_t1.hits[0].doc_ref {
            EntityRef::Kv { space, key, .. } => {
                assert_eq!(space, "default");
                assert_eq!(key, "k_early");
            }
            other => panic!("expected EntityRef::Kv, got {:?}", other),
        }

        // as_of=None → both shadows visible.
        let req_now = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: None,
            budget_ms: None,
        };
        let resp_now = retrieve(&db, &req_now).unwrap();
        assert_eq!(resp_now.hits.len(), 2, "as_of=None should see both shadows");
    }

    #[test]
    fn test_substrate_temporal_hybrid_filters_cross_space() {
        // Three shadow inserts (tenant_a, tenant_b, orphan with no source_ref).
        // Retrieve with as_of and tenant_a → exactly the tenant_a hit.
        // Retrieve with as_of and tenant_c → zero hits.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let vector_store = strata_vector::VectorStore::new(db.clone());
        let config =
            strata_vector::VectorConfig::new(3, strata_vector::DistanceMetric::Cosine).unwrap();
        vector_store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        vector_store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-a",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "tenant_a", "k"),
            )
            .unwrap();
        vector_store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-b",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "tenant_b", "k"),
            )
            .unwrap();
        vector_store
            .system_insert(
                branch_id,
                "_system_embed_kv",
                "shadow-orphan",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        db.flush().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        let snapshot = strata_core::Timestamp::now().as_micros();

        let recipe = temporal_hybrid_recipe();

        let req_a = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "tenant_a".into(),
            recipe: recipe.clone(),
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: Some(snapshot),
            budget_ms: None,
        };
        let resp_a = retrieve(&db, &req_a).unwrap();
        assert_eq!(
            resp_a.hits.len(),
            1,
            "tenant_a should see exactly its own hit at as_of"
        );
        match &resp_a.hits[0].doc_ref {
            EntityRef::Kv { space, .. } => assert_eq!(space, "tenant_a"),
            other => panic!("expected EntityRef::Kv, got {:?}", other),
        }

        let req_c = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "tenant_c".into(),
            recipe,
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: Some(snapshot),
            budget_ms: None,
        };
        let resp_c = retrieve(&db, &req_c).unwrap();
        assert_eq!(
            resp_c.hits.len(),
            0,
            "tenant_c has no matching source and orphan must be dropped"
        );
    }

    #[test]
    fn test_substrate_temporal_hybrid_orphan_dropped() {
        // Only an orphan (no source_ref). as_of query → 0 hits.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let vector_store = strata_vector::VectorStore::new(db.clone());
        let config =
            strata_vector::VectorConfig::new(3, strata_vector::DistanceMetric::Cosine).unwrap();
        vector_store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        vector_store
            .system_insert(
                branch_id,
                "_system_embed_kv",
                "shadow-orphan",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        db.flush().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        let as_of_ts = strata_core::Timestamp::now().as_micros();

        let request = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "default".into(),
            recipe: temporal_hybrid_recipe(),
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: Some(as_of_ts),
            budget_ms: None,
        };
        let response = retrieve(&db, &request).unwrap();
        assert_eq!(
            response.hits.len(),
            0,
            "orphan vector with no source_ref must be dropped under as_of"
        );
    }

    #[test]
    fn test_substrate_temporal_hybrid_before_data() {
        // as_of captured BEFORE any vector insert → 0 vector hits.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let t0 = strata_core::Timestamp::now().as_micros();
        std::thread::sleep(std::time::Duration::from_millis(10));

        let vector_store = strata_vector::VectorStore::new(db.clone());
        let config =
            strata_vector::VectorConfig::new(3, strata_vector::DistanceMetric::Cosine).unwrap();
        vector_store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();
        vector_store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "default", "k"),
            )
            .unwrap();
        db.flush().unwrap();

        let request = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "default".into(),
            recipe: temporal_hybrid_recipe(),
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: Some(t0),
            budget_ms: None,
        };
        let response = retrieve(&db, &request).unwrap();
        assert_eq!(
            response.hits.len(),
            0,
            "as_of before any insert must yield zero vector hits"
        );
    }

    // ---- Gap G' (#2370): vector intersection of as_of + time_range ----
    //
    // Pin that the vector branch applies BOTH knobs as an intersection,
    // matching the BM25 contract from PR #2365. Pre-fix the vector branch
    // ignored `time_range` whenever `as_of` was set, leaving a documented
    // asymmetry where a fused hybrid response could mix BM25 hits filtered
    // by both bounds with vector hits filtered by only one.

    #[test]
    fn test_substrate_temporal_hybrid_intersect_as_of_and_time_range() {
        // Insert 4 shadow vectors sequentially, capturing a wall-clock
        // checkpoint after each one (with 10ms sleeps so each shadow_N is
        // reliably committed *before* tN). Request as_of=t3 + time_range=(t2, t4):
        //   - shadow_1 (created < t1 < t2)        excluded by time_range
        //   - shadow_2 (created in (t1, t2))      excluded by time_range
        //   - shadow_3 (created in (t2, t3))      INCLUDED (in range, visible at t3)
        //   - shadow_4 (created in (t3, t4))      excluded by as_of (created > t3)
        //
        // The single hit must be k_3. Pre-fix the vector branch would
        // return shadow_1, shadow_2, shadow_3 (everything visible at t3),
        // breaking the assertion.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let vector_store = strata_vector::VectorStore::new(db.clone());
        let config =
            strata_vector::VectorConfig::new(3, strata_vector::DistanceMetric::Cosine).unwrap();
        vector_store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        let insert_shadow = |idx: u32| {
            let shadow = format!("shadow_{}", idx);
            let key = format!("k_{}", idx);
            vector_store
                .system_insert_with_source(
                    branch_id,
                    "_system_embed_kv",
                    &shadow,
                    &[1.0, 0.0, 0.0],
                    None,
                    EntityRef::kv(branch_id, "default", &key),
                )
                .unwrap();
            db.flush().unwrap();
        };
        let checkpoint = || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            let ts = strata_core::Timestamp::now().as_micros();
            std::thread::sleep(std::time::Duration::from_millis(10));
            ts
        };

        insert_shadow(1);
        // _t1 isn't asserted on directly — it documents the timeline and
        // also enforces the 20ms gap between shadow_1's commit and t2 (via
        // checkpoint()'s sleep-before-and-after pattern), so shadow_1's
        // created_at is reliably strictly less than t2.
        let _t1 = checkpoint();
        insert_shadow(2);
        let t2 = checkpoint();
        insert_shadow(3);
        let t3 = checkpoint();
        insert_shadow(4);
        let t4 = checkpoint();

        let recipe = temporal_hybrid_recipe();

        // Query 1: intersection — exactly shadow_3.
        let req_intersect = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "default".into(),
            recipe: recipe.clone(),
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: Some((t2, t4)),
            primitive_filter: None,
            as_of: Some(t3),
            budget_ms: None,
        };
        let resp_intersect = retrieve(&db, &req_intersect).unwrap();

        // BM25 contributes nothing (no KV/Json/Event/Graph data) — any hit
        // came from the vector branch. Pin this so a future BM25 change
        // can't make this test pass for the wrong reason.
        assert_eq!(
            resp_intersect
                .stats
                .stages
                .get("bm25")
                .map(|s| s.candidates)
                .unwrap_or(0),
            0,
            "BM25 must produce zero hits — vector branch is the only source"
        );
        assert_eq!(
            resp_intersect.hits.len(),
            1,
            "intersection of as_of=t3 and time_range=(t2, t4) should yield \
             exactly k_3; got {} hits: {:?}",
            resp_intersect.hits.len(),
            resp_intersect
                .hits
                .iter()
                .map(|h| &h.doc_ref)
                .collect::<Vec<_>>()
        );
        match &resp_intersect.hits[0].doc_ref {
            EntityRef::Kv { space, key, .. } => {
                assert_eq!(space, "default");
                assert_eq!(key, "k_3");
            }
            other => panic!("expected EntityRef::Kv k_3, got {:?}", other),
        }

        // Query 2: as_of-only — proves the intersection actually narrows.
        // Without time_range, k_1, k_2, k_3 are all visible at t3, k_4 is
        // not. So 3 hits, not 1.
        let req_as_of_only = RetrievalRequest {
            query: "anything".into(),
            branch_id,
            space: "default".into(),
            recipe,
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: None,
            primitive_filter: None,
            as_of: Some(t3),
            budget_ms: None,
        };
        let resp_as_of_only = retrieve(&db, &req_as_of_only).unwrap();
        assert_eq!(
            resp_as_of_only.hits.len(),
            3,
            "as_of=t3 alone should yield k_1, k_2, k_3 (k_4 not yet visible)"
        );
        let keys: std::collections::BTreeSet<String> = resp_as_of_only
            .hits
            .iter()
            .filter_map(|h| match &h.doc_ref {
                EntityRef::Kv { key, .. } => Some(key.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            keys,
            ["k_1", "k_2", "k_3"]
                .iter()
                .map(|s| s.to_string())
                .collect::<std::collections::BTreeSet<_>>(),
            "as_of-only result must contain exactly k_1, k_2, k_3"
        );
    }

    #[test]
    fn test_substrate_temporal_hybrid_intersect_matches_bm25_pattern() {
        // For each tN, insert one shadow vector pointing at k_N AND a KV
        // doc at k_N with text "rust N". Query "rust" with as_of=t3 and
        // time_range=(t2, t4) exercises BM25 + vector in the same call.
        //
        // BM25 (already intersection-correct since #2365) → 1 candidate
        // Vector (post-fix intersection) → 1 candidate
        //
        // Pre-fix the vector stage would report 3 candidates (k_1, k_2,
        // k_3 — everything visible at t3) while BM25 reports 1, breaking
        // the symmetry. Post-fix both branches converge on the same
        // {k_3} set.
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
            .expect("create db");
        let branch_id = BranchId::new();

        let vector_store = strata_vector::VectorStore::new(db.clone());
        let config =
            strata_vector::VectorConfig::new(3, strata_vector::DistanceMetric::Cosine).unwrap();
        vector_store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        let insert_pair = |idx: u32| {
            let shadow = format!("shadow_{}", idx);
            let key = format!("k_{}", idx);
            let body = format!("rust {}", idx);
            vector_store
                .system_insert_with_source(
                    branch_id,
                    "_system_embed_kv",
                    &shadow,
                    &[1.0, 0.0, 0.0],
                    None,
                    EntityRef::kv(branch_id, "default", &key),
                )
                .unwrap();
            // put_kv_and_observe_ts flushes internally; we ignore the
            // returned ts and rely on wall-clock checkpoints below for
            // bounds, since the BM25 and vector stages each see slightly
            // different created_at values for the (kv, vector) pair.
            let _ = put_kv_and_observe_ts(&db, &branch_id, "default", &key, &body);
        };
        let checkpoint = || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            let ts = strata_core::Timestamp::now().as_micros();
            std::thread::sleep(std::time::Duration::from_millis(10));
            ts
        };

        insert_pair(1);
        let _t1 = checkpoint();
        insert_pair(2);
        let t2 = checkpoint();
        insert_pair(3);
        let t3 = checkpoint();
        insert_pair(4);
        let t4 = checkpoint();

        let request = RetrievalRequest {
            query: "rust".into(),
            branch_id,
            space: "default".into(),
            recipe: temporal_hybrid_recipe(),
            embedding: Some(vec![1.0, 0.0, 0.0]),
            time_range: Some((t2, t4)),
            primitive_filter: None,
            as_of: Some(t3),
            budget_ms: None,
        };
        let response = retrieve(&db, &request).unwrap();

        let bm25_count = response
            .stats
            .stages
            .get("bm25")
            .map(|s| s.candidates)
            .unwrap_or(0);
        let vec_count = response
            .stats
            .stages
            .get("vector")
            .map(|s| s.candidates)
            .unwrap_or(0);

        assert_eq!(bm25_count, 1, "BM25 intersection should yield exactly k_3");
        // The asymmetry pin: vector must agree with BM25 on the cardinality
        // of the intersection window. Pre-fix this would be 3 (everything
        // visible at t3 with time_range silently dropped).
        assert_eq!(
            vec_count, bm25_count,
            "vector intersection should match BM25 intersection cardinality \
             (asymmetry would give vector={}, bm25={})",
            vec_count, bm25_count
        );

        // The fused result: BM25 and vector both point at the same
        // EntityRef::Kv k_3, so RRF dedupes them into a single hit.
        assert_eq!(response.hits.len(), 1, "fused result should be exactly k_3");
        match &response.hits[0].doc_ref {
            EntityRef::Kv { space, key, .. } => {
                assert_eq!(space, "default");
                assert_eq!(key, "k_3");
            }
            other => panic!("expected EntityRef::Kv k_3, got {:?}", other),
        }
    }
}
