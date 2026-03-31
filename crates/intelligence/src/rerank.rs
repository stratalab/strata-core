//! Reranking via the intelligence layer.
//!
//! Re-scores top-N results using a cross-encoder model, then blends
//! the reranker scores with RRF scores using position-aware weights
//! from the recipe.

use strata_engine::search::recipe::RerankConfig;
use strata_engine::search::SearchHit;
use strata_search::rerank::{BlendWeights, RerankScore};

/// Minimum number of snippets required to attempt reranking.
const MIN_RERANK_CANDIDATES: usize = 3;

/// Rerank hits using a cross-encoder model and blend with fusion scores.
///
/// Returns `(hits, was_reranked)`. On failure or insufficient candidates,
/// returns original hits unchanged (graceful degradation).
pub fn rerank_hits(
    query: &str,
    hits: Vec<SearchHit>,
    config: &RerankConfig,
    model_spec: &str,
) -> (Vec<SearchHit>, bool) {
    let top_n = config.top_n.unwrap_or(20);

    if hits.len() < MIN_RERANK_CANDIDATES {
        return (hits, false);
    }

    // Extract snippets from top-N candidates
    let snippets: Vec<(usize, String)> = hits
        .iter()
        .take(top_n)
        .enumerate()
        .filter_map(|(i, hit)| hit.snippet.as_ref().map(|s| (i, s.clone())))
        .collect();

    if snippets.len() < MIN_RERANK_CANDIDATES {
        return (hits, false);
    }

    // Score via cross-encoder
    let scores = match score_passages(query, &snippets, model_spec) {
        Some(s) => s,
        None => return (hits, false),
    };

    // Build BlendWeights from recipe config
    let weights = config.blending.as_ref().map(|b| BlendWeights {
        rank_1_3: b.rank_1_3.unwrap_or(0.75),
        rank_4_10: b.rank_4_10.unwrap_or(0.60),
        rank_11_plus: b.rank_11_plus.unwrap_or(0.40),
    });

    let blended = strata_search::rerank::blend_scores(hits, &scores, weights.as_ref());
    (blended, true)
}

/// Score passages using a cross-encoder ranking model.
/// Returns None on failure.
fn score_passages(
    query: &str,
    snippets: &[(usize, String)],
    model_spec: &str,
) -> Option<Vec<RerankScore>> {
    let engine = match crate::load_ranker(model_spec) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(
                target: "strata::rerank",
                error = %e,
                model = %model_spec,
                "Failed to load ranking model"
            );
            return None;
        }
    };

    let passages: Vec<&str> = snippets.iter().map(|(_, s)| s.as_str()).collect();
    match engine.rank(query, &passages) {
        Ok(scores) => {
            tracing::debug!(
                target: "strata::rerank",
                query = %query,
                score_count = scores.len(),
                "Reranking applied"
            );
            Some(
                scores
                    .into_iter()
                    .enumerate()
                    .map(|(i, score)| RerankScore {
                        index: snippets[i].0,
                        relevance_score: score,
                    })
                    .collect(),
            )
        }
        Err(e) => {
            tracing::warn!(target: "strata::rerank", error = %e, "Ranking failed");
            None
        }
    }
}
