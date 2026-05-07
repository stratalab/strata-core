//! Query expansion via the intelligence layer.
//!
//! Generates typed query variants (lex/vec/hyde) using a generation model
//! with grammar constraints, then filters via a hallucination guard.
//! The caller (search handler) routes each variant to the appropriate
//! substrate retrieval pass.

use strata_core::BranchId;
use strata_engine::search::expand::parser::parse_expansion_with_filter;
use strata_engine::search::expand::{ExpandedQuery, QueryType};
use strata_engine::search::recipe::ExpansionConfig;
use strata_engine::search::tokenize_unique;

use crate::expand_cache;

/// GBNF grammar that constrains expansion output to parseable `type: content` lines.
const EXPAND_GRAMMAR: &str = r#"root ::= line+
line ::= type ": " content "\n"
type ::= "lex" | "vec" | "hyde"
content ::= [^\n]+"#;

/// Expand a query using a generation model with grammar constraints.
///
/// Reads from the per-branch persistent cache first; on a miss, calls the
/// model, filters the result, and writes the filtered variants back to the
/// cache. All failures are non-fatal — search continues with whatever
/// variants are available (or with the original query alone on total
/// failure).
pub fn expand_query(
    db: &strata_engine::Database,
    query: &str,
    config: &ExpansionConfig,
    model_spec: &str,
    branch_id: BranchId,
) -> Vec<ExpandedQuery> {
    let cache_enabled = config.cache_enabled.unwrap_or(true);
    let key_hex = expand_cache::cache_key(query, model_spec);

    // Cache read: graceful miss on any failure.
    if cache_enabled {
        if let Some(cached) = expand_cache::get(db, branch_id, &key_hex) {
            tracing::debug!(
                target: "strata::expand",
                query = %query,
                "expansion cache hit"
            );
            return cached;
        }
    }

    let raw = generate_expansions(db, query, config, model_spec);
    if raw.is_empty() {
        return vec![];
    }
    let filtered = filter_expansions(query, raw, config);

    // Cache write: non-fatal on failure.
    if cache_enabled && !filtered.is_empty() {
        let capacity = config.cache_capacity.unwrap_or(1000);
        if let Err(e) = expand_cache::put(
            db, branch_id, &key_hex, query, model_spec, &filtered, capacity,
        ) {
            tracing::warn!(
                target: "strata::expand",
                error = %e,
                "expansion cache write failed"
            );
        }
    }

    filtered
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
    config: &ExpansionConfig,
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
        max_tokens: config.max_tokens.unwrap_or(600),
        temperature: config.temperature.unwrap_or(0.7),
        top_k: config.top_k.unwrap_or(20),
        top_p: config.top_p.unwrap_or(0.8),
        seed: None,
        stop_sequences: vec![],
        stop_tokens: vec![],
        grammar: Some(EXPAND_GRAMMAR.to_string()),
    };

    let result = super::generate::with_engine(&entry, |engine| engine.generate(&request));

    match result {
        Ok(Ok(response)) => {
            let parsed = parse_expansion_with_filter(&response.text, Some(query));
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

/// Resolve the `expansion.strategy` recipe field to the set of allowed query types.
///
/// Valid values: `"lex"`, `"vec"`, `"hyde"`, `"full"`. `None` and any unknown
/// value default to `"full"` (all three types). Unknown values log a warning
/// but do not fail — graceful degradation matches the codebase pattern for
/// other Option<String> recipe fields (e.g. `fusion.method`).
fn parse_strategy(strategy: &Option<String>) -> Vec<QueryType> {
    match strategy.as_deref() {
        Some("lex") => vec![QueryType::Lex],
        Some("vec") => vec![QueryType::Vec],
        Some("hyde") => vec![QueryType::Hyde],
        Some("full") | None => vec![QueryType::Lex, QueryType::Vec, QueryType::Hyde],
        Some(other) => {
            tracing::warn!(
                target: "strata::expand",
                strategy = %other,
                "Unknown expansion strategy, defaulting to 'full'"
            );
            vec![QueryType::Lex, QueryType::Vec, QueryType::Hyde]
        }
    }
}

/// Filter expansions via the strategy filter and the hallucination guard.
///
/// Two passes:
/// 1. **Strategy filter**: drop variants whose `query_type` is not in the
///    recipe's allowed strategy (`"lex"`, `"vec"`, `"hyde"`, or `"full"`).
/// 2. **Hallucination guard**: drop Lex/Vec variants sharing fewer than
///    `min_shared_stems` stemmed terms with the original query. Hyde variants
///    use different vocabulary by design and are exempt.
fn filter_expansions(
    original_query: &str,
    expansions: Vec<ExpandedQuery>,
    config: &ExpansionConfig,
) -> Vec<ExpandedQuery> {
    let allowed = parse_strategy(&config.strategy);
    let min_shared = config.min_shared_stems.unwrap_or(2);
    let original_stems = tokenize_unique(original_query);

    expansions
        .into_iter()
        .filter(|eq| allowed.contains(&eq.query_type))
        .filter(|eq| {
            if eq.query_type == QueryType::Hyde {
                return true; // Hyde is exempt from the hallucination guard
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
        use strata_core::BranchId;
        use strata_engine::search::{EntityRef, SearchHit};

        let hits = vec![
            SearchHit {
                doc_ref: EntityRef::Kv {
                    branch_id: BranchId::from_bytes([0u8; 16]),
                    space: "default".into(),
                    key: "a".into(),
                },
                score: 0.90,
                rank: 1,
                snippet: None,
            },
            SearchHit {
                doc_ref: EntityRef::Kv {
                    branch_id: BranchId::from_bytes([0u8; 16]),
                    space: "default".into(),
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

    /// Helper: build a 3-variant expansion list (Lex/Vec/Hyde) that all share
    /// at least one stem with "ssh setup", so the hallucination guard never
    /// drops anything — strategy is the only filter under test.
    fn three_variant_expansions() -> Vec<ExpandedQuery> {
        vec![
            ExpandedQuery {
                query_type: QueryType::Lex,
                text: "ssh keygen".into(),
            },
            ExpandedQuery {
                query_type: QueryType::Vec,
                text: "ssh key authentication".into(),
            },
            ExpandedQuery {
                query_type: QueryType::Hyde,
                text: "Run ssh-keygen -t ed25519 to create a new key pair.".into(),
            },
        ]
    }

    #[test]
    fn test_parse_strategy_variants() {
        assert_eq!(parse_strategy(&Some("lex".into())), vec![QueryType::Lex]);
        assert_eq!(parse_strategy(&Some("vec".into())), vec![QueryType::Vec]);
        assert_eq!(parse_strategy(&Some("hyde".into())), vec![QueryType::Hyde]);
        assert_eq!(
            parse_strategy(&Some("full".into())),
            vec![QueryType::Lex, QueryType::Vec, QueryType::Hyde]
        );
        assert_eq!(
            parse_strategy(&None),
            vec![QueryType::Lex, QueryType::Vec, QueryType::Hyde]
        );
        assert_eq!(
            parse_strategy(&Some("garbage".into())),
            vec![QueryType::Lex, QueryType::Vec, QueryType::Hyde]
        );
    }

    #[test]
    fn test_filter_strategy_lex_only() {
        let config = ExpansionConfig {
            strategy: Some("lex".into()),
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let filtered = filter_expansions("ssh setup", three_variant_expansions(), &config);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].query_type, QueryType::Lex);
    }

    #[test]
    fn test_filter_strategy_vec_only() {
        let config = ExpansionConfig {
            strategy: Some("vec".into()),
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let filtered = filter_expansions("ssh setup", three_variant_expansions(), &config);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].query_type, QueryType::Vec);
    }

    #[test]
    fn test_filter_strategy_hyde_only() {
        let config = ExpansionConfig {
            strategy: Some("hyde".into()),
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let filtered = filter_expansions("ssh setup", three_variant_expansions(), &config);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].query_type, QueryType::Hyde);
    }

    /// Assert the filtered list contains exactly one variant of each query type.
    /// Stronger than `len == 3` — would catch a bug returning `[Lex, Lex, Lex]`.
    fn assert_one_of_each_type(filtered: &[ExpandedQuery]) {
        assert_eq!(filtered.len(), 3, "expected three variants");
        assert!(
            filtered.iter().any(|eq| eq.query_type == QueryType::Lex),
            "missing Lex variant"
        );
        assert!(
            filtered.iter().any(|eq| eq.query_type == QueryType::Vec),
            "missing Vec variant"
        );
        assert!(
            filtered.iter().any(|eq| eq.query_type == QueryType::Hyde),
            "missing Hyde variant"
        );
    }

    #[test]
    fn test_filter_strategy_full_keeps_all() {
        let config = ExpansionConfig {
            strategy: Some("full".into()),
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let filtered = filter_expansions("ssh setup", three_variant_expansions(), &config);
        assert_one_of_each_type(&filtered);
    }

    #[test]
    fn test_filter_strategy_none_defaults_to_full() {
        let config = ExpansionConfig {
            strategy: None,
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let filtered = filter_expansions("ssh setup", three_variant_expansions(), &config);
        assert_one_of_each_type(&filtered);
    }

    #[test]
    fn test_filter_strategy_invalid_defaults_to_full() {
        let config = ExpansionConfig {
            strategy: Some("not-a-strategy".into()),
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let filtered = filter_expansions("ssh setup", three_variant_expansions(), &config);
        assert_one_of_each_type(&filtered);
    }

    #[test]
    fn test_filter_expansions_empty_input() {
        // Empty input → empty output, regardless of strategy or guard config.
        let config = ExpansionConfig {
            strategy: Some("full".into()),
            min_shared_stems: Some(2),
            ..Default::default()
        };
        let filtered = filter_expansions("ssh setup", vec![], &config);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_strategy_drops_to_empty() {
        // strategy="lex" with no Lex variants in the input → empty output.
        // Proves the strategy filter can fully drain the candidate list,
        // not just shrink it.
        let config = ExpansionConfig {
            strategy: Some("lex".into()),
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let expansions = vec![
            ExpandedQuery {
                query_type: QueryType::Vec,
                text: "ssh key authentication".into(),
            },
            ExpandedQuery {
                query_type: QueryType::Hyde,
                text: "Run ssh-keygen -t ed25519 to create a new key pair.".into(),
            },
        ];
        let filtered = filter_expansions("ssh setup", expansions, &config);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_strategy_lex_drops_vec_even_when_relevant() {
        // Strategy filter runs BEFORE the hallucination guard, so a relevant Vec
        // variant is dropped purely on type, not on stem overlap.
        let config = ExpansionConfig {
            strategy: Some("lex".into()),
            min_shared_stems: Some(1),
            ..Default::default()
        };
        let expansions = vec![
            ExpandedQuery {
                query_type: QueryType::Vec,
                text: "ssh setup guide".into(), // shares "ssh", "setup" — would pass guard
            },
            ExpandedQuery {
                query_type: QueryType::Lex,
                text: "ssh keygen".into(),
            },
        ];
        let filtered = filter_expansions("ssh setup", expansions, &config);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].query_type, QueryType::Lex);
    }
}
