//! Query expansion via the intelligence layer.
//!
//! Generates typed query variants (lex/vec/hyde) using a generation model
//! with grammar constraints, then filters via a hallucination guard.
//! The caller (search handler) routes each variant to the appropriate
//! substrate retrieval pass.

use strata_engine::search::recipe::ExpansionConfig;
use strata_engine::search::tokenize_unique;
use strata_search::expand::{ExpandedQuery, QueryType};

// Generation parameters for expansion (v0.8 will move these to recipe.models config).
const EXPAND_MAX_TOKENS: usize = 600;
const EXPAND_TEMPERATURE: f32 = 0.7;
const EXPAND_TOP_K: usize = 20;
const EXPAND_TOP_P: f32 = 0.8;

/// GBNF grammar that constrains expansion output to parseable `type: content` lines.
const EXPAND_GRAMMAR: &str = r#"root ::= line+
line ::= type ": " content "\n"
type ::= "lex" | "vec" | "hyde"
content ::= [^\n]+"#;

/// Expand a query using a generation model with grammar constraints.
///
/// Returns the filtered expansion variants. Returns empty vec on any failure
/// (graceful degradation — search continues with the original query only).
pub fn expand_query(
    db: &strata_engine::Database,
    query: &str,
    config: &ExpansionConfig,
    model_spec: &str,
) -> Vec<ExpandedQuery> {
    let raw = generate_expansions(db, query, model_spec);
    if raw.is_empty() {
        return vec![];
    }
    filter_expansions(query, raw, config)
}

/// Check if BM25 results have a strong enough signal to skip expansion.
pub fn has_strong_signal(
    hits: &[strata_engine::search::SearchHit],
    config: &ExpansionConfig,
) -> bool {
    let threshold = config.strong_signal_threshold.unwrap_or(0.85);
    let gap = config.strong_signal_gap.unwrap_or(0.15);

    if hits.is_empty() {
        return false;
    }
    let top = hits[0].score;
    let second = hits.get(1).map(|h| h.score).unwrap_or(0.0);
    top >= threshold && (top - second) >= gap
}

/// Generate expansions via the cached generation model.
fn generate_expansions(
    db: &strata_engine::Database,
    query: &str,
    model_spec: &str,
) -> Vec<ExpandedQuery> {
    use super::generate::GenerateModelState;

    let state = match db.extension::<GenerateModelState>() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to get generate model state for expansion");
            return vec![];
        }
    };

    let entry = match state.get_or_load(model_spec) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, model = %model_spec, "Failed to load expansion model");
            return vec![];
        }
    };

    let prompt = format!(
        "<|im_start|>user\n/no_think Expand this search query: {}<|im_end|>\n<|im_start|>assistant\n",
        query
    );

    let request = crate::GenerateRequest {
        prompt,
        max_tokens: EXPAND_MAX_TOKENS,
        temperature: EXPAND_TEMPERATURE,
        top_k: EXPAND_TOP_K,
        top_p: EXPAND_TOP_P,
        seed: None,
        stop_sequences: vec![],
        stop_tokens: vec![],
        grammar: Some(EXPAND_GRAMMAR.to_string()),
    };

    let result = super::generate::with_engine(&entry, |engine| engine.generate(&request));

    match result {
        Ok(Ok(response)) => {
            let parsed = strata_search::expand::parser::parse_expansion_with_filter(
                &response.text,
                Some(query),
            );
            tracing::debug!(
                target: "strata::expand",
                query = %query,
                count = parsed.queries.len(),
                "Generated expansions"
            );
            parsed.queries
        }
        Ok(Err(e)) => {
            tracing::warn!(target: "strata::expand", error = %e, "Expansion generation failed");
            vec![]
        }
        Err(e) => {
            tracing::warn!(target: "strata::expand", error = %e, "Expansion engine error");
            vec![]
        }
    }
}

/// Filter expansions via hallucination guard.
///
/// Discards expansions sharing fewer than `min_shared_stems` stemmed terms
/// with the original query. Hyde variants are exempt (they use different vocabulary).
fn filter_expansions(
    original_query: &str,
    expansions: Vec<ExpandedQuery>,
    config: &ExpansionConfig,
) -> Vec<ExpandedQuery> {
    let min_shared = config.min_shared_stems.unwrap_or(2);
    let original_stems = tokenize_unique(original_query);

    expansions
        .into_iter()
        .filter(|eq| {
            if eq.query_type == QueryType::Hyde {
                return true; // Hyde is exempt
            }
            let exp_stems = tokenize_unique(&eq.text);
            let shared = original_stems
                .iter()
                .filter(|t| exp_stems.contains(t))
                .count();
            shared >= min_shared
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_strong_signal_empty() {
        let config = ExpansionConfig::default();
        assert!(!has_strong_signal(&[], &config));
    }

    #[test]
    fn test_has_strong_signal_high() {
        use strata_core::types::BranchId;
        use strata_engine::search::{EntityRef, SearchHit};

        let hits = vec![
            SearchHit {
                doc_ref: EntityRef::Kv {
                    branch_id: BranchId::from_bytes([0u8; 16]),
                    key: "a".into(),
                },
                score: 0.90,
                rank: 1,
                snippet: None,
            },
            SearchHit {
                doc_ref: EntityRef::Kv {
                    branch_id: BranchId::from_bytes([0u8; 16]),
                    key: "b".into(),
                },
                score: 0.50,
                rank: 2,
                snippet: None,
            },
        ];
        let config = ExpansionConfig {
            strong_signal_threshold: Some(0.85),
            strong_signal_gap: Some(0.15),
            ..Default::default()
        };
        assert!(has_strong_signal(&hits, &config));
    }

    #[test]
    fn test_filter_expansions_keeps_relevant() {
        let config = ExpansionConfig {
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let expansions = vec![
            ExpandedQuery {
                query_type: QueryType::Lex,
                text: "ssh key authentication".into(),
            },
            ExpandedQuery {
                query_type: QueryType::Lex,
                text: "completely unrelated terms".into(),
            },
            ExpandedQuery {
                query_type: QueryType::Hyde,
                text: "This is a hypothetical document about something".into(),
            },
        ];
        let filtered = filter_expansions("ssh setup", expansions, &config);
        // "ssh key authentication" shares "ssh" → kept
        // "completely unrelated" shares nothing → dropped
        // Hyde is exempt → kept
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].query_type, QueryType::Lex);
        assert_eq!(filtered[1].query_type, QueryType::Hyde);
    }
}
