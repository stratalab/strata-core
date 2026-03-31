//! Search command handler.
//!
//! Routes all search through the retrieval substrate. The intelligence layer
//! (query embedding) wraps the substrate transparently.

use std::sync::Arc;

use strata_engine::search::recipe::TransformConfig;
use strata_engine::search::{builtin_defaults, Recipe};
use strata_search::substrate::{self, RetrievalRequest};

use crate::bridge::{to_core_branch_id, Primitives};
use crate::types::{BranchId, SearchQuery, SearchResultHit, SearchStatsOutput};
use crate::{Error, Output, Result};

/// Handle Search command via the retrieval substrate.
///
/// The recipe is the single source of truth for search behavior.
/// Three-level merge: builtin defaults → branch recipe → per-call override.
pub fn search(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    sq: SearchQuery,
) -> Result<Output> {
    let core_branch_id = to_core_branch_id(&branch)?;

    // ---- Resolve recipe (three-level merge) ----
    let builtin = builtin_defaults();
    let branch_recipe = strata_engine::recipe_store::get_default_recipe(&p.db, core_branch_id)
        .map_err(|e| Error::Internal {
            reason: format!("Failed to get branch recipe: {}", e),
            hint: None,
        })?;

    // Per-call override: inline recipe JSON + k shorthand
    let mut per_call: Option<Recipe> = match sq.recipe {
        Some(v) => Some(serde_json::from_value(v).map_err(|e| Error::InvalidInput {
            reason: format!("Invalid recipe JSON: {}", e),
            hint: None,
        })?),
        None => None,
    };
    if let Some(k) = sq.k {
        let r = per_call.get_or_insert_with(Recipe::default);
        let t = r.transform.get_or_insert_with(TransformConfig::default);
        t.limit = Some(k as usize);
    }

    let resolved = Recipe::resolve(&builtin, &branch_recipe, per_call.as_ref());

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
        None, // time_range: TODO wire from recipe.filter
        None, // primitive_filter: TODO wire from recipe.retrieve.bm25.sources
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

    // ---- Convert to Output ----
    let hits: Vec<SearchResultHit> = final_hits
        .iter()
        .map(|hit| {
            let (entity, primitive) = format_entity_ref(&hit.doc_ref);
            SearchResultHit {
                entity,
                primitive,
                score: hit.score,
                rank: hit.rank,
                snippet: hit.snippet.clone(),
            }
        })
        .collect();

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
    };

    Ok(Output::SearchResults { hits, stats })
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

    let expansions =
        strata_intelligence::expand::expand_query(&p.db, query, expansion_cfg, &expand_model);

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

/// Format an EntityRef into (entity_string, primitive_string) for display.
pub(crate) fn format_entity_ref(doc_ref: &strata_engine::search::EntityRef) -> (String, String) {
    match doc_ref {
        strata_engine::search::EntityRef::Kv { key, .. } => (key.clone(), "kv".to_string()),
        strata_engine::search::EntityRef::Json { doc_id, .. } => {
            (doc_id.clone(), "json".to_string())
        }
        strata_engine::search::EntityRef::Event { sequence, .. } => {
            (format!("seq:{}", sequence), "event".to_string())
        }
        strata_engine::search::EntityRef::Branch { branch_id } => {
            let uuid = uuid::Uuid::from_bytes(*branch_id.as_bytes());
            (uuid.to_string(), "branch".to_string())
        }
        strata_engine::search::EntityRef::Vector { key, .. } => (key.clone(), "vector".to_string()),
        strata_engine::search::EntityRef::Graph { key, .. } => (key.clone(), "graph".to_string()),
    }
}
