//! Recipe schema for search retrieval configuration.
//!
//! Recipes control all aspects of search behavior: BM25 tuning, vector search,
//! fusion strategy, expansion, reranking, and more. Each recipe is fully
//! self-contained — what you see is what runs. No inheritance, no merge.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Top-level Recipe
// ============================================================================

/// Search recipe — controls all retrieval behavior.
///
/// Each recipe is fully self-contained. Presence of a section = enabled,
/// absence = disabled. Named recipes are stored on the `_system_` branch.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Recipe {
    /// Schema version (currently 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,

    /// Retrieval stage configuration (BM25, vector, graph).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retrieve: Option<RetrieveConfig>,

    /// Query expansion configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expansion: Option<ExpansionConfig>,

    /// Pre-retrieval filter configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterConfig>,

    /// Result fusion configuration (e.g., RRF).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fusion: Option<FusionConfig>,

    /// Re-ranking configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank: Option<RerankConfig>,

    /// Post-retrieval transform (limit, dedup).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<TransformConfig>,

    /// RAG prompt template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,

    /// Number of search hits to include in RAG context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rag_context_hits: Option<usize>,

    /// Maximum token budget for RAG context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rag_max_tokens: Option<usize>,

    /// Model routing configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models: Option<ModelsConfig>,

    /// Version output configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_output: Option<VersionOutputConfig>,

    /// Execution control (budget, timeouts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ControlConfig>,
}

// ============================================================================
// Sub-configs
// ============================================================================

/// Retrieval stage: which retrievers to use and how.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct RetrieveConfig {
    /// BM25 keyword retrieval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bm25: Option<BM25Config>,
    /// Vector (embedding) retrieval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorRetrieveConfig>,
    /// Graph traversal retrieval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<GraphConfig>,
}

/// BM25 keyword retrieval parameters.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct BM25Config {
    /// Which primitives to search (e.g., ["kv", "json", "event"]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<String>>,
    /// Which spaces to search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spaces: Option<Vec<String>>,
    /// Top-k candidates from BM25.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<usize>,
    /// Term frequency saturation (default 0.9).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k1: Option<f32>,
    /// Document length normalization (default 0.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub b: Option<f32>,
    /// Per-field boost weights.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_weights: Option<HashMap<String, f32>>,
    /// Stemmer algorithm (e.g., "porter").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stemmer: Option<String>,
    /// Stopword list (e.g., "lucene33").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<String>,
    /// Score multiplier for documents containing an exact phrase match.
    /// Default: 2.0. Only used when phrase_mode = "boost".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phrase_boost: Option<f32>,
    /// Term proximity boost factor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proximity_boost: Option<f32>,
    /// Phrase matching mode: "boost" (default) or "filter".
    /// "boost": phrase matches get score multiplied by phrase_boost.
    /// "filter": only documents containing exact phrases are returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phrase_mode: Option<String>,
    /// Maximum word gap allowed between phrase terms.
    /// Default: 0 (exact adjacency).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phrase_slop: Option<u32>,
    /// Store term positions in the inverted index.
    /// Enables phrase queries and proximity scoring.
    /// Default: true. Set to false for minimal index size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positions: Option<bool>,
}

/// Vector (embedding) retrieval parameters.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct VectorRetrieveConfig {
    /// Which collections to search (default: all `_system_embed_*`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collections: Option<Vec<String>>,
    /// Top-k candidates from vector search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<usize>,
    /// Distance metric override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
}

/// Graph traversal retrieval parameters.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GraphConfig {
    /// Named graph to traverse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    /// Maximum hops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hops: Option<usize>,
    /// Top-k graph results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<usize>,
}

/// Query expansion configuration.
///
/// Presence = enabled, absence = disabled. The intelligence layer generates
/// typed query variants (lex/vec/hyde) that fan out to BM25 and vector search.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ExpansionConfig {
    /// Strategy: "lex", "vec", "hyde", or "full" (default: "full").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    /// Skip expansion if top BM25 score >= this (default: 0.85).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strong_signal_threshold: Option<f32>,
    /// Top BM25 score must exceed #2 by at least this (default: 0.15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strong_signal_gap: Option<f32>,
    /// Discard expansions sharing fewer than this many stemmed terms
    /// with the original query (default: 2). Hyde is exempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_shared_stems: Option<usize>,
    /// Weight for original query in RRF fusion (default: 2.0).
    /// Expansion results use weight 1.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_weight: Option<f32>,
}

/// Pre-retrieval filter configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct FilterConfig {
    /// Time range filter (start_us, end_us).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<(u64, u64)>,
    /// Tag filter (match any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags_any: Option<Vec<String>>,
}

/// Result fusion configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct FusionConfig {
    /// Fusion method (e.g., "rrf", "linear").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// RRF k parameter (default 60).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<usize>,
    /// Per-source weights for linear fusion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weights: Option<HashMap<String, f32>>,
}

/// Re-ranking configuration.
///
/// Presence = enabled, absence = disabled. Re-scores top-N results after
/// fusion using a cross-encoder model with position-aware score blending.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct RerankConfig {
    /// Number of top candidates to re-rank (default: 20).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_n: Option<usize>,
    /// Position-aware blending weights.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blending: Option<BlendingConfig>,
}

/// Position-aware blending weights for reranking.
///
/// Each value is the RRF weight for that rank tier.
/// Reranker weight = 1.0 - rrf_weight.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct BlendingConfig {
    /// RRF weight for ranks 1-3 (default: 0.75).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank_1_3: Option<f32>,
    /// RRF weight for ranks 4-10 (default: 0.60).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank_4_10: Option<f32>,
    /// RRF weight for ranks 11+ (default: 0.40).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank_11_plus: Option<f32>,
}

/// Post-retrieval transform.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TransformConfig {
    /// Final result limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Deduplication strategy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup: Option<String>,
}

/// Model routing configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ModelsConfig {
    /// Embedding model (e.g., "local:miniLM").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed: Option<String>,
    /// Reranking model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank: Option<String>,
    /// Generation model (for RAG).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate: Option<String>,
    /// Expansion model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expand: Option<String>,
}

/// Version output configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct VersionOutputConfig {
    /// Include version metadata in results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_version: Option<bool>,
    /// Include timestamps in results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_timestamp: Option<bool>,
    /// Include full version history per hit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_history: Option<bool>,
    /// Maximum number of history versions to include (default 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<usize>,
}

/// Execution control.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ControlConfig {
    /// Total budget in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_ms: Option<u64>,
    /// Maximum candidates per primitive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_candidates: Option<usize>,
    /// Pin all reads to this MVCC snapshot version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<u64>,
}

// ============================================================================
// Merge
// ============================================================================
// Built-in Defaults
// ============================================================================

/// The full-featured default recipe.
///
/// BM25 tuned to Anserini/Pyserini BEIR values (k1=0.9, b=0.4).
/// Expansion and reranking enabled — gracefully degrade when models
/// are not available (search still works, just without the quality knobs).
///
/// Used internally by `builtin_recipes()` to define the "default" named recipe
/// and as a fallback in `get_default_recipe()` for legacy/unseeded databases.
pub(crate) fn builtin_defaults() -> Recipe {
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
            vector: Some(VectorRetrieveConfig::default()),
            ..Default::default()
        }),
        expansion: Some(ExpansionConfig {
            strategy: Some("full".into()),
            strong_signal_threshold: Some(0.85),
            strong_signal_gap: Some(0.15),
            min_shared_stems: Some(2),
            original_weight: Some(2.0),
        }),
        fusion: Some(FusionConfig {
            method: Some("rrf".into()),
            k: Some(60),
            ..Default::default()
        }),
        rerank: Some(RerankConfig {
            top_n: Some(20),
            blending: Some(BlendingConfig {
                rank_1_3: Some(0.75),
                rank_4_10: Some(0.60),
                rank_11_plus: Some(0.40),
            }),
        }),
        transform: Some(TransformConfig {
            limit: Some(10),
            ..Default::default()
        }),
        control: Some(ControlConfig {
            budget_ms: Some(5000),
            ..Default::default()
        }),
        ..Default::default()
    }
}

// ============================================================================
// Named Built-in Recipes
// ============================================================================

/// Names of all built-in recipes shipped with Strata.
pub const BUILTIN_RECIPE_NAMES: &[&str] =
    &["keyword", "semantic", "hybrid", "default", "graph", "rag"];

/// BM25 config shared across recipes that use keyword search.
fn bm25_defaults() -> BM25Config {
    BM25Config {
        k: Some(50),
        k1: Some(0.9),
        b: Some(0.4),
        stemmer: Some("porter".into()),
        stopwords: Some("lucene33".into()),
        phrase_boost: Some(2.0),
        ..Default::default()
    }
}

/// Returns all built-in recipes as (name, recipe) pairs.
///
/// These are written to the `_system_` branch at database creation time.
/// Each recipe is fully self-contained — no inheritance, no merge.
pub fn builtin_recipes() -> Vec<(&'static str, Recipe)> {
    vec![
        (
            "keyword",
            Recipe {
                version: Some(1),
                retrieve: Some(RetrieveConfig {
                    bm25: Some(bm25_defaults()),
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
                control: Some(ControlConfig {
                    budget_ms: Some(5000),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        (
            "semantic",
            Recipe {
                version: Some(1),
                retrieve: Some(RetrieveConfig {
                    vector: Some(VectorRetrieveConfig::default()),
                    ..Default::default()
                }),
                transform: Some(TransformConfig {
                    limit: Some(10),
                    ..Default::default()
                }),
                models: Some(ModelsConfig {
                    embed: Some("local:miniLM".into()),
                    ..Default::default()
                }),
                control: Some(ControlConfig {
                    budget_ms: Some(5000),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        (
            "hybrid",
            Recipe {
                version: Some(1),
                retrieve: Some(RetrieveConfig {
                    bm25: Some(bm25_defaults()),
                    vector: Some(VectorRetrieveConfig::default()),
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
                models: Some(ModelsConfig {
                    embed: Some("local:miniLM".into()),
                    ..Default::default()
                }),
                control: Some(ControlConfig {
                    budget_ms: Some(5000),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        ("default", builtin_defaults()),
        (
            "graph",
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
                    graph: Some(GraphConfig {
                        graph: None, // user must set via db.set_recipe
                        max_hops: Some(2),
                        k: Some(50),
                    }),
                    ..Default::default()
                }),
                fusion: Some(FusionConfig {
                    method: Some("rrf".into()),
                    k: Some(60),
                    weights: Some(
                        [("bm25".into(), 1.0), ("graph".into(), 0.3)]
                            .into_iter()
                            .collect(),
                    ),
                }),
                transform: Some(TransformConfig {
                    limit: Some(10),
                    ..Default::default()
                }),
                control: Some(ControlConfig {
                    budget_ms: Some(5000),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        (
            "rag",
            Recipe {
                version: Some(1),
                retrieve: Some(RetrieveConfig {
                    bm25: Some(bm25_defaults()),
                    vector: Some(VectorRetrieveConfig::default()),
                    ..Default::default()
                }),
                expansion: Some(ExpansionConfig {
                    strategy: Some("full".into()),
                    strong_signal_threshold: Some(0.85),
                    strong_signal_gap: Some(0.15),
                    min_shared_stems: Some(2),
                    original_weight: Some(2.0),
                }),
                fusion: Some(FusionConfig {
                    method: Some("rrf".into()),
                    k: Some(60),
                    ..Default::default()
                }),
                rerank: Some(RerankConfig {
                    top_n: Some(20),
                    blending: Some(BlendingConfig {
                        rank_1_3: Some(0.75),
                        rank_4_10: Some(0.60),
                        rank_11_plus: Some(0.40),
                    }),
                }),
                transform: Some(TransformConfig {
                    limit: Some(10),
                    ..Default::default()
                }),
                prompt: Some(
                    "Answer using only the provided context. Cite sources with [N].".into(),
                ),
                rag_context_hits: Some(5),
                rag_max_tokens: Some(500),
                models: Some(ModelsConfig {
                    embed: Some("local:miniLM".into()),
                    expand: Some("local:qwen3:1.7b".into()),
                    rerank: Some("local:jina-reranker-v1-tiny".into()),
                    generate: Some("anthropic:claude-sonnet-4-6".into()),
                }),
                control: Some(ControlConfig {
                    budget_ms: Some(10000),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
    ]
}

/// Look up a built-in recipe by name (in-memory, no DB access).
pub fn get_builtin_recipe(name: &str) -> Option<Recipe> {
    builtin_recipes()
        .into_iter()
        .find(|(n, _)| *n == name)
        .map(|(_, r)| r)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recipe_deserialize_empty() {
        let recipe: Recipe = serde_json::from_str("{}").unwrap();
        assert_eq!(recipe, Recipe::default());
        assert!(recipe.version.is_none());
        assert!(recipe.retrieve.is_none());
    }

    #[test]
    fn test_recipe_deserialize_nested() {
        let json = r#"{"retrieve":{"bm25":{}}}"#;
        let recipe: Recipe = serde_json::from_str(json).unwrap();
        assert!(recipe.retrieve.is_some());
        let bm25 = recipe.retrieve.unwrap().bm25.unwrap();
        assert!(bm25.k1.is_none());
        assert!(bm25.b.is_none());
        assert!(bm25.k.is_none());
    }

    #[test]
    fn test_builtin_recipes_count() {
        assert_eq!(BUILTIN_RECIPE_NAMES.len(), 6);
        assert_eq!(builtin_recipes().len(), 6);
    }

    #[test]
    fn test_builtin_keyword_no_expansion() {
        let r = get_builtin_recipe("keyword").unwrap();
        assert!(r.retrieve.as_ref().unwrap().bm25.is_some());
        assert!(r.retrieve.as_ref().unwrap().vector.is_none());
        assert!(r.expansion.is_none());
        assert!(r.rerank.is_none());
    }

    #[test]
    fn test_builtin_default_has_expansion() {
        let r = get_builtin_recipe("default").unwrap();
        assert!(r.expansion.is_some());
        assert!(r.rerank.is_some());
        assert!(r.retrieve.as_ref().unwrap().bm25.is_some());
        assert!(r.retrieve.as_ref().unwrap().vector.is_some());
    }

    #[test]
    fn test_builtin_semantic_vector_only() {
        let r = get_builtin_recipe("semantic").unwrap();
        assert!(r.retrieve.as_ref().unwrap().bm25.is_none());
        assert!(r.retrieve.as_ref().unwrap().vector.is_some());
    }

    #[test]
    fn test_builtin_unknown_returns_none() {
        assert!(get_builtin_recipe("nonexistent").is_none());
    }

    #[test]
    fn test_recipe_roundtrip() {
        let original = builtin_defaults();
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Recipe = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }
}
