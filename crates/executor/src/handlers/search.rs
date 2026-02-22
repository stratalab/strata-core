//! Search command handler.
//!
//! Handles cross-primitive search via the intelligence layer's HybridSearch.
//! When a model is configured, transparently expands queries for better recall
//! and re-ranks results for better precision.

use std::sync::Arc;

use chrono::DateTime;
use strata_engine::search::{PrimitiveType, SearchResponse};
use strata_engine::{SearchBudget, SearchMode, SearchRequest};
use strata_search::HybridSearch;
use tracing::debug;

use crate::bridge::{to_core_branch_id, Primitives};
use crate::types::{BranchId, SearchQuery, SearchResultHit, TimeRangeInput};
use crate::{Error, Output, Result};

/// Strong signal threshold: if top BM25 score >= this, skip expansion.
const STRONG_SIGNAL_SCORE: f32 = 0.85;
/// Strong signal gap: top score must exceed #2 by at least this much.
const STRONG_SIGNAL_GAP: f32 = 0.15;
/// Maximum number of candidates to send for re-ranking.
const MAX_RERANK_CANDIDATES: usize = 20;
/// Minimum number of snippets required to attempt re-ranking.
const MIN_RERANK_CANDIDATES: usize = 3;

/// Parse an ISO 8601 datetime string to microseconds since epoch.
fn parse_iso8601_to_micros(s: &str) -> Result<u64> {
    let dt = DateTime::parse_from_rfc3339(s).map_err(|e| Error::InvalidInput {
        reason: format!("Invalid ISO 8601 datetime '{}': {}", s, e),
    })?;
    let micros = dt.timestamp_micros();
    if micros < 0 {
        return Err(Error::InvalidInput {
            reason: format!("Datetime '{}' is before Unix epoch", s),
        });
    }
    Ok(micros as u64)
}

/// Parse a TimeRangeInput into (start_micros, end_micros).
fn parse_time_range(input: &TimeRangeInput) -> Result<(u64, u64)> {
    let start = parse_iso8601_to_micros(&input.start)?;
    let end = parse_iso8601_to_micros(&input.end)?;
    if start > end {
        return Err(Error::InvalidInput {
            reason: "time_range.start must be <= time_range.end".into(),
        });
    }
    Ok((start, end))
}

/// Handle Search command: cross-primitive search
pub fn search(
    p: &Arc<Primitives>,
    branch: BranchId,
    _space: String,
    sq: SearchQuery,
) -> Result<Output> {
    let core_branch_id = to_core_branch_id(&branch)?;

    // Build primitive filter from string names
    let primitive_filter = sq.primitives.as_ref().map(|names| {
        names
            .iter()
            .filter_map(|name| match name.to_lowercase().as_str() {
                "kv" => Some(PrimitiveType::Kv),
                "json" => Some(PrimitiveType::Json),
                "event" => Some(PrimitiveType::Event),
                "state" => Some(PrimitiveType::State),
                "branch" => Some(PrimitiveType::Branch),
                "vector" => Some(PrimitiveType::Vector),
                _ => None,
            })
            .collect::<Vec<_>>()
    });

    // Parse time_range
    let parsed_time_range = sq.time_range.as_ref().map(parse_time_range).transpose()?;

    let mut req = SearchRequest::new(core_branch_id, &sq.query);
    if let Some(top_k) = sq.k {
        req = req.with_k(top_k as usize);
    }
    req.budget = SearchBudget::default();
    if let Some(filter) = primitive_filter {
        if !filter.is_empty() {
            req = req.with_primitive_filter(filter);
        }
    }

    // Apply time range
    if let Some((start, end)) = parsed_time_range {
        req = req.with_time_range(start, end);
    }

    // Set search mode (default: hybrid for cross-primitive search)
    let mode = match sq.mode.as_deref() {
        Some("keyword") => SearchMode::Keyword,
        Some("hybrid") | None => SearchMode::Hybrid,
        Some(_) => SearchMode::Hybrid, // unrecognized mode, use default
    };
    req = req.with_mode(mode);

    // Pass precomputed embedding if provided (skips GPU inference during search)
    if let Some(emb) = sq.precomputed_embedding {
        req = req.with_precomputed_embedding(emb);
    }

    let hybrid = build_hybrid_search(&p.db);

    // Check if a model is configured for query expansion
    let has_model = has_model_configured(&p.db);

    // Honor expand/rerank toggles
    let should_expand = match sq.expand {
        Some(true) => has_model,
        Some(false) => false,
        None => has_model,
    };
    let should_rerank = match sq.rerank {
        Some(true) => has_model,
        Some(false) => false,
        None => has_model,
    };

    let response = if should_expand {
        // Strong signal detection: cheap BM25 probe BEFORE calling LLM
        let probe_req = req.clone().with_mode(SearchMode::Keyword);
        let probe = hybrid.search(&probe_req).map_err(crate::Error::from)?;

        if has_strong_signal(&probe) {
            debug!(
                target: "strata::search",
                query = %sq.query,
                top_score = probe.hits.first().map(|h| h.score).unwrap_or(0.0),
                "Strong BM25 signal, skipping expansion and reranking"
            );
            // Strong signal: return full hybrid search (skip LLM entirely)
            hybrid.search(&req).map_err(crate::Error::from)?
        } else if let Some(expansions) = try_expand(&p.db, &sq.query) {
            debug!(
                target: "strata::search",
                query = %sq.query,
                expansion_count = expansions.len(),
                "Using query expansion"
            );
            let expanded_response = hybrid
                .search_expanded(&req, &expansions, 2.0)
                .map_err(crate::Error::from)?;
            if should_rerank {
                try_rerank(&p.db, &sq.query, expanded_response)
            } else {
                expanded_response
            }
        } else {
            // Expansion failed — fall back to normal search + optional reranking
            let base_response = hybrid.search(&req).map_err(crate::Error::from)?;
            if should_rerank {
                try_rerank(&p.db, &sq.query, base_response)
            } else {
                base_response
            }
        }
    } else if should_rerank {
        // No expansion but reranking enabled
        let base_response = hybrid.search(&req).map_err(crate::Error::from)?;
        try_rerank(&p.db, &sq.query, base_response)
    } else {
        // No model configured or both disabled — plain search
        hybrid.search(&req).map_err(crate::Error::from)?
    };

    // Convert SearchResponse hits to SearchResultHit
    let results: Vec<SearchResultHit> = response
        .hits
        .into_iter()
        .map(|hit| {
            let (entity, primitive) = format_entity_ref(&hit.doc_ref);
            SearchResultHit {
                entity,
                primitive,
                score: hit.score,
                rank: hit.rank,
                snippet: hit.snippet,
            }
        })
        .collect();

    Ok(Output::SearchResults(results))
}

/// Check if a model is configured (cheap — no LLM call).
fn has_model_configured(db: &Arc<strata_engine::Database>) -> bool {
    db.config().model.is_some()
}

/// Try to expand a query using the configured model.
///
/// Returns `Some(expansions)` if a model is configured and expansion succeeds.
/// Returns `None` if no model is configured or expansion fails (graceful degradation).
fn try_expand(
    db: &Arc<strata_engine::Database>,
    query: &str,
) -> Option<Vec<strata_search::expand::ExpandedQuery>> {
    let config = db.config().model?;

    let expander = strata_search::expand::ApiExpander::new(
        &config.endpoint,
        &config.model,
        config.api_key.as_deref(),
        config.timeout_ms,
    );

    match strata_search::expand::QueryExpander::expand(&expander, query) {
        Ok(expanded) if !expanded.queries.is_empty() => Some(expanded.queries),
        Ok(_) => {
            debug!(target: "strata::search", "Expansion returned empty, falling back");
            None
        }
        Err(e) => {
            debug!(target: "strata::search", error = %e, "Expansion failed, falling back");
            None
        }
    }
}

/// Try to re-rank search results using the configured model.
///
/// Extracts snippets from the top-N hits, sends them to the model for relevance
/// scoring, and blends the reranker scores with RRF scores using position-aware
/// weights. Returns results unchanged if:
/// - No model is configured
/// - Fewer than MIN_RERANK_CANDIDATES snippets available
/// - Reranking fails (graceful degradation)
fn try_rerank(
    db: &Arc<strata_engine::Database>,
    query: &str,
    mut response: SearchResponse,
) -> SearchResponse {
    let config = match db.config().model {
        Some(c) => c,
        None => return response,
    };

    // Extract (index, snippet) pairs from top-N hits
    let snippets: Vec<(usize, String)> = response
        .hits
        .iter()
        .take(MAX_RERANK_CANDIDATES)
        .enumerate()
        .filter_map(|(i, hit)| hit.snippet.as_ref().map(|s| (i, s.clone())))
        .collect();

    if snippets.len() < MIN_RERANK_CANDIDATES {
        debug!(
            target: "strata::search",
            snippet_count = snippets.len(),
            "Too few snippets for reranking, skipping"
        );
        return response;
    }

    let reranker = strata_search::rerank::ApiReranker::new(
        &config.endpoint,
        &config.model,
        config.api_key.as_deref(),
        config.timeout_ms,
    );

    let snippet_refs: Vec<(usize, &str)> = snippets.iter().map(|(i, s)| (*i, s.as_str())).collect();

    match strata_search::rerank::Reranker::rerank(&reranker, query, &snippet_refs) {
        Ok(scores) if !scores.is_empty() => {
            debug!(
                target: "strata::search",
                query = %query,
                score_count = scores.len(),
                "Re-ranking applied"
            );
            response.hits = strata_search::rerank::blend_scores(response.hits, &scores);
            response
        }
        Ok(_) => {
            debug!(target: "strata::search", "Reranking returned no scores, using RRF results");
            response
        }
        Err(e) => {
            debug!(target: "strata::search", error = %e, "Reranking failed, using RRF results");
            response
        }
    }
}

/// Check if BM25 probe results have a strong enough signal to skip expansion.
fn has_strong_signal(response: &strata_engine::search::SearchResponse) -> bool {
    if response.hits.is_empty() {
        return false;
    }
    let top_score = response.hits[0].score;
    let second_score = response.hits.get(1).map(|h| h.score).unwrap_or(0.0);
    top_score >= STRONG_SIGNAL_SCORE && (top_score - second_score) >= STRONG_SIGNAL_GAP
}

// ============================================================================
// Embedder bridge: wires strata-intelligence embed into strata-search trait
// ============================================================================

#[cfg(feature = "embed")]
struct IntelligenceEmbedder {
    db: Arc<strata_engine::Database>,
}

#[cfg(feature = "embed")]
impl strata_search::QueryEmbedder for IntelligenceEmbedder {
    fn embed(&self, text: &str) -> Option<Vec<f32>> {
        strata_intelligence::embed::embed_query(&self.db, text)
    }

    fn embed_batch(&self, texts: &[&str]) -> Vec<Option<Vec<f32>>> {
        strata_intelligence::embed::embed_batch_queries(&self.db, texts)
    }
}

/// Build a HybridSearch, injecting the embedder when the embed feature is active.
fn build_hybrid_search(db: &Arc<strata_engine::Database>) -> HybridSearch {
    #[cfg(feature = "embed")]
    {
        let embedder = Arc::new(IntelligenceEmbedder { db: db.clone() });
        HybridSearch::with_embedder(db.clone(), embedder)
    }
    #[cfg(not(feature = "embed"))]
    {
        HybridSearch::new(db.clone())
    }
}

/// Format an EntityRef into (entity_string, primitive_string) for display
fn format_entity_ref(doc_ref: &strata_engine::search::EntityRef) -> (String, String) {
    match doc_ref {
        strata_engine::search::EntityRef::Kv { key, .. } => (key.clone(), "kv".to_string()),
        strata_engine::search::EntityRef::Json { doc_id, .. } => {
            (doc_id.clone(), "json".to_string())
        }
        strata_engine::search::EntityRef::Event { sequence, .. } => {
            (format!("seq:{}", sequence), "event".to_string())
        }
        strata_engine::search::EntityRef::State { name, .. } => (name.clone(), "state".to_string()),
        strata_engine::search::EntityRef::Branch { branch_id } => {
            let uuid = uuid::Uuid::from_bytes(*branch_id.as_bytes());
            (uuid.to_string(), "branch".to_string())
        }
        strata_engine::search::EntityRef::Vector { key, .. } => (key.clone(), "vector".to_string()),
    }
}
