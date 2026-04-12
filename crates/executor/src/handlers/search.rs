//! Search command handler.
//!
//! Routes all search through the retrieval substrate. The intelligence layer
//! (query embedding) wraps the substrate transparently.

use std::sync::Arc;

use strata_engine::search::recipe::TransformConfig;
use strata_engine::search::Recipe;
use strata_search::substrate::{self, RetrievalRequest};

use crate::bridge::{to_core_branch_id, Primitives};
use crate::types::{
    AnswerResponse, BranchId, ChangedHit, DiffOutput, EntityRefOutput, SearchQuery,
    SearchResultHit, SearchStatsOutput, VersionInfo,
};
use crate::{Error, Output, Result};

/// Handle Search command via the retrieval substrate.
///
/// The recipe is the single source of truth for search behavior.
/// Resolved by name (string) or used directly (inline JSON object).
pub fn search(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    sq: SearchQuery,
) -> Result<Output> {
    let core_branch_id = to_core_branch_id(&branch)?;

    // ---- Resolve recipe (name or inline) ----
    let mut resolved: Recipe = match sq.recipe {
        Some(ref v) if v.is_string() => {
            // String → look up named recipe (user branch → _system_ fallback)
            let name = v.as_str().unwrap();
            strata_engine::recipe_store::get_recipe(&p.db, core_branch_id, name)
                .map_err(|e| Error::Internal {
                    reason: format!("Failed to look up recipe '{name}': {e}"),
                    hint: None,
                })?
                .ok_or_else(|| Error::InvalidInput {
                    reason: format!("Unknown recipe: '{name}'"),
                    hint: Some(format!(
                        "Available recipes: {:?}",
                        strata_engine::recipe_store::list_recipes(&p.db, core_branch_id)
                            .unwrap_or_default()
                    )),
                })?
        }
        Some(ref v) if v.is_object() => {
            // JSON object → use directly as complete recipe
            serde_json::from_value(v.clone()).map_err(|e| Error::InvalidInput {
                reason: format!("Invalid recipe JSON: {e}"),
                hint: None,
            })?
        }
        Some(ref v) => {
            return Err(Error::InvalidInput {
                reason: format!(
                    "recipe must be a string (name) or object (inline recipe), got: {}",
                    v
                ),
                hint: None,
            });
        }
        None => {
            // Absent → use "default" recipe
            strata_engine::recipe_store::get_default_recipe(&p.db, core_branch_id).map_err(|e| {
                Error::Internal {
                    reason: format!("Failed to get default recipe: {e}"),
                    hint: None,
                }
            })?
        }
    };

    // Apply k shorthand (the one convenience patch)
    if let Some(k) = sq.k {
        let t = resolved
            .transform
            .get_or_insert_with(TransformConfig::default);
        t.limit = Some(k as usize);
    }

    // ---- Embed query (intelligence layer) ----
    let has_vector = resolved
        .retrieve
        .as_ref()
        .and_then(|r| r.vector.as_ref())
        .is_some();

    let embed_model = resolved
        .models
        .as_ref()
        .and_then(|m| m.embed.clone())
        .unwrap_or_else(|| p.db.embed_model());

    let embedding = if let Some(emb) = sq.precomputed_embedding {
        Some(emb)
    } else if has_vector {
        embed_query_with_model(&p.db, &sq.query, &embed_model)
    } else {
        None
    };

    let mode = if has_vector && embedding.is_some() {
        "hybrid"
    } else {
        "keyword"
    };

    // ---- Expand (intelligence layer, before substrate) ----
    let (expanded_queries, expansion_used) = try_expand_query(
        p,
        &sq.query,
        &resolved,
        &embed_model,
        core_branch_id,
        &space,
    );

    // ---- Resolve temporal context ----
    // If diff is set, the "after" snapshot uses t2 as the as_of timestamp.
    let as_of = sq.as_of.or(sq.diff.map(|(_, t2)| t2));
    let time_range = resolved.filter.as_ref().and_then(|f| f.time_range);

    // ---- Multi-pass substrate retrieval ----
    let all_hits = multi_pass_retrieve(
        p,
        &sq.query,
        &expanded_queries,
        &resolved,
        embedding.as_deref(),
        &embed_model,
        core_branch_id,
        &space,
        time_range,
        None, // primitive_filter: TODO wire from recipe.retrieve.bm25.sources
        as_of,
    );

    // ---- Fuse expanded results ----
    let original_weight = resolved
        .expansion
        .as_ref()
        .and_then(|e| e.original_weight)
        .unwrap_or(2.0);
    let fused = fuse_multi_pass(&all_hits, original_weight, &resolved);

    // ---- Rerank (intelligence layer, after substrate) ----
    let rerank_model = resolved
        .models
        .as_ref()
        .and_then(|m| m.rerank.clone())
        .unwrap_or_else(|| "jina-reranker-v1-tiny".to_string());
    let (final_hits, rerank_used) = if let Some(ref rerank_cfg) = resolved.rerank {
        rerank_hits(&sq.query, fused, rerank_cfg, &rerank_model)
    } else {
        (fused, false)
    };

    // ---- RAG generation (intelligence layer, after rerank) ----
    //
    // Gated on `recipe.prompt.is_some()` inside `generate_rag_answer`.
    // Returns `None` when prompt is unset, model load fails, or generation
    // errors — in all those cases hits are still returned, just without
    // an answer. This is the trigger pattern that mirrors how
    // `recipe.expansion.is_some()` and `recipe.rerank.is_some()` gate
    // their respective layers.
    let rag_answer = generate_rag_answer(p, &sq.query, &final_hits, &resolved);

    // ---- Convert to Output ----
    let mut hits: Vec<SearchResultHit> = final_hits
        .iter()
        .map(|hit| SearchResultHit {
            entity_ref: (&hit.doc_ref).into(),
            score: hit.score,
            rank: hit.rank,
            snippet: hit.snippet.clone(),
            versions: None,
        })
        .collect();

    // ---- Version history enrichment ----
    let include_history = resolved
        .version_output
        .as_ref()
        .and_then(|v| v.include_history)
        .unwrap_or(false);
    if include_history {
        let depth = resolved
            .version_output
            .as_ref()
            .and_then(|v| v.depth)
            .unwrap_or(3);
        // Pass the engine `EntityRef` from `final_hits` directly so
        // enrichment can dispatch on the strongly-typed variant and
        // use the hit's space (not the request's).
        enrich_versions(p, core_branch_id, &space, &final_hits, &mut hits, depth);
    }

    // ---- Temporal diff ----
    let diff_output = if let Some((t1, t2)) = sq.diff {
        // Current results are the "after" snapshot (computed with as_of = t2 or current).
        // Re-run the pipeline at T1 to get the "before" snapshot.
        let before_hits = multi_pass_retrieve(
            p,
            &sq.query,
            &expanded_queries,
            &resolved,
            embedding.as_deref(),
            &embed_model,
            core_branch_id,
            &space,
            time_range,
            None,
            Some(t1),
        );
        let before_fused = fuse_multi_pass(&before_hits, original_weight, &resolved);
        let before_results: Vec<SearchResultHit> = before_fused
            .iter()
            .map(|hit| SearchResultHit {
                entity_ref: (&hit.doc_ref).into(),
                score: hit.score,
                rank: hit.rank,
                snippet: hit.snippet.clone(),
                versions: None,
            })
            .collect();
        Some(compute_diff(&before_results, &hits, t1, t2))
    } else {
        None
    };

    let embed_status = crate::handlers::embed_hook::embed_status(p);
    let (embedding_pending, embedding_total) =
        if embed_status.auto_embed && embed_status.pending > 0 {
            (
                Some(embed_status.pending as u64),
                Some(embed_status.total_queued),
            )
        } else {
            (None, None)
        };

    // Split the rag_answer into the executor-level AnswerResponse + stats
    // fields. The intelligence-layer RagAnswer carries both the user-visible
    // answer and the bookkeeping (model, elapsed, tokens) — we project them
    // into the wire types here.
    let (answer_output, rag_used_flag, rag_model_field, rag_elapsed, rag_in, rag_out) =
        match rag_answer {
            Some(ans) => (
                Some(AnswerResponse {
                    text: ans.text,
                    sources: ans.sources,
                }),
                Some(true),
                Some(ans.model),
                Some(ans.elapsed_ms),
                Some(ans.tokens_in),
                Some(ans.tokens_out),
            ),
            None => (None, None, None, None, None, None),
        };

    let stats = SearchStatsOutput {
        elapsed_ms: 0.0, // TODO: track total elapsed
        candidates_considered: 0,
        candidates_by_primitive: std::collections::HashMap::new(),
        index_used: true,
        truncated: false,
        mode: mode.to_string(),
        expansion_used,
        rerank_used,
        expansion_model: if expansion_used {
            resolved.models.as_ref().and_then(|m| m.expand.clone())
        } else {
            None
        },
        rerank_model: if rerank_used {
            Some(rerank_model)
        } else {
            None
        },
        embedding_pending,
        embedding_total,
        snapshot_version: Some(p.db.current_version().as_u64()),
        rag_used: rag_used_flag,
        rag_model: rag_model_field,
        rag_elapsed_ms: rag_elapsed,
        rag_tokens_in: rag_in,
        rag_tokens_out: rag_out,
    };

    Ok(Output::SearchResults {
        hits,
        stats,
        diff: diff_output,
        answer: answer_output,
    })
}

// ============================================================================
// Expansion orchestration
// ============================================================================

/// Try to expand the query. Returns (queries, was_expanded).
/// If expansion is disabled or fails, returns the original query only.
#[cfg(feature = "embed")]
fn try_expand_query(
    p: &Arc<Primitives>,
    query: &str,
    recipe: &Recipe,
    _embed_model: &str,
    branch_id: strata_core::types::BranchId,
    space: &str,
) -> (Vec<strata_search::expand::ExpandedQuery>, bool) {
    use strata_search::expand::{ExpandedQuery, QueryType};

    let expansion_cfg = match recipe.expansion.as_ref() {
        Some(c) => c,
        None => {
            return (
                vec![ExpandedQuery {
                    query_type: QueryType::Lex,
                    text: query.to_string(),
                }],
                false,
            );
        }
    };

    // BM25 probe for strong signal detection
    let mut probe_recipe = recipe.clone();
    if let Some(ref mut r) = probe_recipe.retrieve {
        r.vector = None;
    }
    probe_recipe.expansion = None;

    let probe_req = RetrievalRequest {
        query: query.to_string(),
        branch_id,
        space: space.to_string(),
        recipe: probe_recipe,
        embedding: None,
        time_range: None,
        primitive_filter: None,
        as_of: None,
        budget_ms: None,
    };

    if let Ok(probe) = substrate::retrieve(&p.db, &probe_req) {
        if strata_intelligence::expand::has_strong_signal(&probe.hits, expansion_cfg) {
            return (
                vec![ExpandedQuery {
                    query_type: QueryType::Lex,
                    text: query.to_string(),
                }],
                false,
            );
        }
    } else {
        tracing::warn!(target: "strata::search", "BM25 probe for expansion failed");
    }

    // Generate expansions
    let expand_model = recipe
        .models
        .as_ref()
        .and_then(|m| m.expand.clone())
        .unwrap_or_else(|| "qwen3:1.7b".to_string());

    let expansions = strata_intelligence::expand::expand_query(
        &p.db,
        query,
        expansion_cfg,
        &expand_model,
        branch_id,
    );

    if expansions.is_empty() {
        return (
            vec![ExpandedQuery {
                query_type: QueryType::Lex,
                text: query.to_string(),
            }],
            false,
        );
    }

    // Prepend original query
    let mut all = vec![ExpandedQuery {
        query_type: QueryType::Lex,
        text: query.to_string(),
    }];
    all.extend(expansions);
    (all, true)
}

#[cfg(not(feature = "embed"))]
fn try_expand_query(
    _p: &Arc<Primitives>,
    query: &str,
    _recipe: &Recipe,
    _embed_model: &str,
    _branch_id: strata_core::types::BranchId,
    _space: &str,
) -> (Vec<strata_search::expand::ExpandedQuery>, bool) {
    use strata_search::expand::{ExpandedQuery, QueryType};
    (
        vec![ExpandedQuery {
            query_type: QueryType::Lex,
            text: query.to_string(),
        }],
        false,
    )
}

// ============================================================================
// Multi-pass retrieval
// ============================================================================

/// Run one substrate call per expanded query, routing by query type:
/// - Lex → keyword mode (BM25 only)
/// - Vec/Hyde → embed + hybrid mode
///
/// Returns labeled candidate lists for fusion.
#[allow(clippy::too_many_arguments)]
fn multi_pass_retrieve(
    p: &Arc<Primitives>,
    _original_query: &str,
    expanded_queries: &[strata_search::expand::ExpandedQuery],
    recipe: &Recipe,
    original_embedding: Option<&[f32]>,
    embed_model: &str,
    branch_id: strata_core::types::BranchId,
    space: &str,
    time_range: Option<(u64, u64)>,
    primitive_filter: Option<&[strata_engine::search::PrimitiveType]>,
    as_of: Option<u64>,
) -> Vec<(String, Vec<strata_engine::search::SearchHit>)> {
    use strata_search::expand::QueryType;

    let mut candidate_lists = Vec::new();

    for (i, eq) in expanded_queries.iter().enumerate() {
        let is_original = i == 0;

        // Build per-pass recipe
        let mut pass_recipe = recipe.clone();
        pass_recipe.expansion = None; // no recursive expansion

        // Original query: use full recipe + original embedding (BM25 + vector).
        // Expansion variants: route by type (lex→keyword, vec→hybrid, hyde→vector).
        let pass_embedding = if is_original {
            original_embedding.map(|e| e.to_vec())
        } else {
            match eq.query_type {
                QueryType::Lex => {
                    if let Some(ref mut r) = pass_recipe.retrieve {
                        r.vector = None;
                    }
                    None
                }
                QueryType::Vec => embed_query_with_model(&p.db, &eq.text, embed_model),
                QueryType::Hyde => {
                    if let Some(ref mut r) = pass_recipe.retrieve {
                        r.bm25 = None;
                    }
                    embed_query_with_model(&p.db, &eq.text, embed_model)
                }
            }
        };

        let request = RetrievalRequest {
            query: eq.text.clone(),
            branch_id,
            space: space.to_string(),
            recipe: pass_recipe,
            embedding: pass_embedding,
            time_range,
            primitive_filter: primitive_filter.map(|f| f.to_vec()),
            as_of,
            budget_ms: None,
        };

        if let Ok(response) = substrate::retrieve(&p.db, &request) {
            let label = if is_original {
                "original".to_string()
            } else {
                format!("exp_{:?}_{}", eq.query_type, i)
            };
            candidate_lists.push((label, response.hits));
        }
    }

    candidate_lists
}

/// Fuse multi-pass results via weighted RRF (original gets higher weight).
fn fuse_multi_pass(
    candidate_lists: &[(String, Vec<strata_engine::search::SearchHit>)],
    original_weight: f32,
    recipe: &Recipe,
) -> Vec<strata_engine::search::SearchHit> {
    use strata_engine::search::recipe::FusionConfig;

    if candidate_lists.len() <= 1 {
        return candidate_lists
            .first()
            .map(|(_, hits)| hits.clone())
            .unwrap_or_default();
    }

    // Build fusion config with per-source weights
    let mut weights = std::collections::HashMap::new();
    for (name, _) in candidate_lists {
        let w = if name == "original" {
            original_weight
        } else {
            1.0
        };
        weights.insert(name.clone(), w);
    }

    let fusion_cfg = FusionConfig {
        method: recipe.fusion.as_ref().and_then(|f| f.method.clone()),
        k: recipe.fusion.as_ref().and_then(|f| f.k),
        weights: Some(weights),
    };

    let mut fused = substrate::rrf_fuse(candidate_lists, Some(&fusion_cfg));

    // Apply transform.limit
    let limit = recipe
        .transform
        .as_ref()
        .and_then(|t| t.limit)
        .unwrap_or(10);
    fused.truncate(limit);
    for (i, hit) in fused.iter_mut().enumerate() {
        hit.rank = (i + 1) as u32;
    }

    fused
}

// ============================================================================
// Reranking orchestration
// ============================================================================

#[cfg(feature = "embed")]
fn rerank_hits(
    query: &str,
    hits: Vec<strata_engine::search::SearchHit>,
    config: &strata_engine::search::recipe::RerankConfig,
    model_spec: &str,
) -> (Vec<strata_engine::search::SearchHit>, bool) {
    strata_intelligence::rerank::rerank_hits(query, hits, config, model_spec)
}

#[cfg(not(feature = "embed"))]
fn rerank_hits(
    _query: &str,
    hits: Vec<strata_engine::search::SearchHit>,
    _config: &strata_engine::search::recipe::RerankConfig,
    _model_spec: &str,
) -> (Vec<strata_engine::search::SearchHit>, bool) {
    (hits, false)
}

// ============================================================================
// RAG generation orchestration
// ============================================================================
//
// Thin shim around the intelligence-layer RAG module. Mirrors the cfg-gated
// pattern used by `rerank_hits` and `try_expand_query`. The intelligence
// layer owns the prompt construction, model loading, and citation
// extraction; this handler is just the call site that decides whether to
// run RAG and folds the result back into the executor wire types.

#[cfg(feature = "embed")]
fn generate_rag_answer(
    p: &Arc<Primitives>,
    query: &str,
    hits: &[strata_engine::search::SearchHit],
    recipe: &Recipe,
) -> Option<strata_intelligence::rag::RagAnswer> {
    strata_intelligence::rag::generate_answer(&p.db, query, hits, recipe)
}

#[cfg(not(feature = "embed"))]
fn generate_rag_answer(
    _p: &Arc<Primitives>,
    _query: &str,
    _hits: &[strata_engine::search::SearchHit],
    _recipe: &Recipe,
) -> Option<RagAnswerStub> {
    None
}

// Stub type for non-embed builds. The compiler still needs `rag_answer`
// to have a concrete type with `text`/`sources`/`model`/`elapsed_ms`/
// `tokens_in`/`tokens_out` fields, even though None is the only value
// produced. This avoids leaking `strata_intelligence::rag::RagAnswer`
// into the non-embed code path.
#[cfg(not(feature = "embed"))]
#[allow(dead_code)]
struct RagAnswerStub {
    text: String,
    sources: Vec<usize>,
    model: String,
    elapsed_ms: f64,
    tokens_in: u32,
    tokens_out: u32,
}

// ============================================================================
// Intelligence layer: query embedding
// ============================================================================

#[cfg(feature = "embed")]
fn embed_query_with_model(
    db: &Arc<strata_engine::Database>,
    text: &str,
    model_spec: &str,
) -> Option<Vec<f32>> {
    strata_intelligence::embed::embed_query_with_model(db, text, model_spec)
}

#[cfg(not(feature = "embed"))]
fn embed_query_with_model(
    _db: &Arc<strata_engine::Database>,
    _text: &str,
    _model_spec: &str,
) -> Option<Vec<f32>> {
    None
}

// ============================================================================
// Temporal: diff computation
// ============================================================================

/// Compute the diff between "before" and "after" search results.
///
/// Keys hits by their structured `entity_ref` (which now includes
/// `space`), so two hits with the same `(primitive, key)` in different
/// spaces are correctly tracked as distinct.
fn compute_diff(
    before: &[SearchResultHit],
    after: &[SearchResultHit],
    t1: u64,
    t2: u64,
) -> DiffOutput {
    use std::collections::HashMap;

    type HitKey = EntityRefOutput;
    fn key_of(h: &SearchResultHit) -> HitKey {
        h.entity_ref.clone()
    }

    let before_map: HashMap<HitKey, &SearchResultHit> =
        before.iter().map(|h| (key_of(h), h)).collect();
    let after_map: HashMap<HitKey, &SearchResultHit> =
        after.iter().map(|h| (key_of(h), h)).collect();

    let added: Vec<SearchResultHit> = after
        .iter()
        .filter(|h| !before_map.contains_key(&key_of(h)))
        .cloned()
        .collect();

    let removed: Vec<SearchResultHit> = before
        .iter()
        .filter(|h| !after_map.contains_key(&key_of(h)))
        .cloned()
        .collect();

    let changed: Vec<ChangedHit> = after
        .iter()
        .filter_map(|h| {
            before_map.get(&key_of(h)).and_then(|prev| {
                let score_changed = (h.score - prev.score).abs() > 0.05;
                let rank_changed = h.rank != prev.rank;
                if score_changed || rank_changed {
                    Some(ChangedHit {
                        hit: h.clone(),
                        previous_score: prev.score,
                        previous_rank: prev.rank,
                    })
                } else {
                    None
                }
            })
        })
        .collect();

    DiffOutput {
        before_ts: t1,
        after_ts: t2,
        added,
        removed,
        changed,
    }
}

// ============================================================================
// Temporal: version history enrichment
// ============================================================================

/// Enrich search result hits with version history from `getv()`.
///
/// Dispatches on the strongly-typed `EntityRef` from the engine
/// `SearchHit` rather than a flat primitive string. Uses **the hit's
/// own space** (`hit_ref.space()`) to fetch versions, not the request
/// space — this is the Phase 0 realization of bug 3.3 from
/// `docs/design/space-correctness-fix-plan.md`: before Phase 0, hits
/// with the same key in different spaces collapsed onto a single
/// version history from the caller's scope.
///
/// `_request_space` is kept in the signature as a pure fallback for
/// the (unreachable) case of a `Branch` variant slipping through,
/// which has no per-hit space. `Kv` and `Json` — the only variants
/// that dispatch — always carry a real space.
fn enrich_versions(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    _request_space: &str,
    hit_refs: &[strata_engine::search::SearchHit],
    hits: &mut [SearchResultHit],
    depth: usize,
) {
    debug_assert_eq!(
        hits.len(),
        hit_refs.len(),
        "hit / hit_ref slices must be aligned by the caller"
    );
    for (hit, engine_hit) in hits.iter_mut().zip(hit_refs.iter()) {
        let hit_ref = &engine_hit.doc_ref;
        hit.versions = match hit_ref {
            strata_core::EntityRef::Kv { space, key, .. } => {
                get_kv_versions(p, branch_id, space, key, depth)
            }
            strata_core::EntityRef::Json { space, doc_id, .. } => {
                get_json_versions(p, branch_id, space, doc_id, depth)
            }
            _ => None,
        };
    }
}

fn get_kv_versions(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    key: &str,
    depth: usize,
) -> Option<Vec<VersionInfo>> {
    let history = p.kv.getv(&branch_id, space, key).ok()??;
    Some(
        history
            .versions()
            .iter()
            .take(depth)
            .map(|v| {
                let snippet = match &v.value {
                    strata_core::Value::String(s) => Some(s.chars().take(200).collect::<String>()),
                    other => Some(format!("{:?}", other).chars().take(200).collect()),
                };
                VersionInfo {
                    version: v.version.as_u64(),
                    timestamp: v.timestamp.as_micros(),
                    snippet,
                }
            })
            .collect(),
    )
}

fn get_json_versions(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    doc_id: &str,
    depth: usize,
) -> Option<Vec<VersionInfo>> {
    let history = p.json.getv(&branch_id, space, doc_id).ok()??;
    Some(
        history
            .versions()
            .iter()
            .take(depth)
            .map(|v| {
                let snippet = Some(
                    serde_json::to_string(&v.value)
                        .unwrap_or_default()
                        .chars()
                        .take(200)
                        .collect(),
                );
                VersionInfo {
                    version: v.version.as_u64(),
                    timestamp: v.timestamp.as_micros(),
                    snippet,
                }
            })
            .collect(),
    )
}

#[cfg(test)]
mod compute_diff_tests {
    use super::{compute_diff, SearchResultHit};
    use crate::types::EntityRefOutput;

    fn kv_hit(space: &str, key: &str, score: f32, rank: u32) -> SearchResultHit {
        SearchResultHit {
            entity_ref: EntityRefOutput {
                kind: "kv".into(),
                branch_id: "00000000-0000-0000-0000-000000000000".into(),
                space: Some(space.into()),
                key: Some(key.into()),
                doc_id: None,
                sequence: None,
                collection: None,
            },
            score,
            rank,
            snippet: None,
            versions: None,
        }
    }

    /// Two hits with the same `key` in different spaces are distinct
    /// — neither appears in `added` nor `removed` of the diff between
    /// snapshots that contain both. Without space-aware identity, the
    /// before/after maps would alias them and `compute_diff` would
    /// drop one.
    #[test]
    fn cross_space_same_key_treated_as_distinct() {
        let before = vec![
            kv_hit("tenant_a", "k", 0.9, 1),
            kv_hit("tenant_b", "k", 0.5, 2),
        ];
        let after = vec![
            kv_hit("tenant_a", "k", 0.9, 1),
            kv_hit("tenant_b", "k", 0.5, 2),
        ];
        let diff = compute_diff(&before, &after, 100, 200);
        assert!(
            diff.added.is_empty(),
            "no hits should be added: {:?}",
            diff.added
        );
        assert!(
            diff.removed.is_empty(),
            "no hits should be removed: {:?}",
            diff.removed
        );
        assert!(
            diff.changed.is_empty(),
            "no hits should change: {:?}",
            diff.changed
        );
    }

    /// Removing the `tenant_b` copy of `"k"` must produce a removal
    /// for `tenant_b` only — not for `tenant_a`. This is the precise
    /// regression that bug 3.3 in the design doc described.
    #[test]
    fn removal_only_targets_the_right_space() {
        let before = vec![
            kv_hit("tenant_a", "k", 0.9, 1),
            kv_hit("tenant_b", "k", 0.5, 2),
        ];
        let after = vec![kv_hit("tenant_a", "k", 0.9, 1)];
        let diff = compute_diff(&before, &after, 100, 200);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(
            diff.removed[0].entity_ref.space.as_deref(),
            Some("tenant_b")
        );
        assert!(diff.added.is_empty());
    }

    /// A score change on the `tenant_a` copy must surface in
    /// `changed`, not aliased by the unchanged `tenant_b` hit.
    #[test]
    fn score_change_attributed_to_correct_space() {
        let before = vec![
            kv_hit("tenant_a", "k", 0.9, 1),
            kv_hit("tenant_b", "k", 0.5, 2),
        ];
        let after = vec![
            kv_hit("tenant_a", "k", 0.4, 1),
            kv_hit("tenant_b", "k", 0.5, 2),
        ];
        let diff = compute_diff(&before, &after, 100, 200);
        assert_eq!(diff.changed.len(), 1);
        assert_eq!(
            diff.changed[0].hit.entity_ref.space.as_deref(),
            Some("tenant_a")
        );
        assert!((diff.changed[0].previous_score - 0.9).abs() < 0.01);
    }
}
