//! Recipe schema for search retrieval configuration.
//!
//! Recipes control all aspects of search behavior: BM25 tuning, vector search,
//! fusion strategy, expansion, reranking, and more. Every field is `Option<T>`
//! to support three-level merge: built-in defaults → branch recipe → per-call override.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Top-level Recipe
// ============================================================================

/// Search recipe — controls all retrieval behavior.
///
/// All fields are `Option<T>` to support partial overrides via [`Recipe::merge`].
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
    /// Phrase proximity boost factor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phrase_boost: Option<f32>,
    /// Term proximity boost factor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proximity_boost: Option<f32>,
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
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ExpansionConfig {
    /// Expansion method (e.g., "lex", "vec", "hyde").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Number of expansion terms/queries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
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
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct RerankConfig {
    /// Whether reranking is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Model to use for reranking (e.g., "local:qwen3-reranker").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Top-k after reranking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<usize>,
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
}

// ============================================================================
// Merge
// ============================================================================

/// Merge two optional sub-configs using a merge function.
fn merge_option<T, F>(base: &Option<T>, overlay: &Option<T>, merge_fn: F) -> Option<T>
where
    T: Clone,
    F: FnOnce(&T, &T) -> T,
{
    match (base, overlay) {
        (Some(b), Some(o)) => Some(merge_fn(b, o)),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), None) => Some(b.clone()),
        (None, None) => None,
    }
}

impl Recipe {
    /// Merge two recipes. Overlay fields win when `Some`; base fills gaps.
    pub fn merge(base: &Recipe, overlay: &Recipe) -> Recipe {
        Recipe {
            version: overlay.version.or(base.version),
            retrieve: merge_option(&base.retrieve, &overlay.retrieve, RetrieveConfig::merge),
            expansion: overlay
                .expansion
                .as_ref()
                .or(base.expansion.as_ref())
                .cloned(),
            filter: overlay.filter.as_ref().or(base.filter.as_ref()).cloned(),
            fusion: merge_option(&base.fusion, &overlay.fusion, FusionConfig::merge),
            rerank: overlay.rerank.as_ref().or(base.rerank.as_ref()).cloned(),
            transform: merge_option(&base.transform, &overlay.transform, TransformConfig::merge),
            prompt: overlay.prompt.as_ref().or(base.prompt.as_ref()).cloned(),
            rag_context_hits: overlay.rag_context_hits.or(base.rag_context_hits),
            rag_max_tokens: overlay.rag_max_tokens.or(base.rag_max_tokens),
            models: merge_option(&base.models, &overlay.models, ModelsConfig::merge),
            version_output: overlay
                .version_output
                .as_ref()
                .or(base.version_output.as_ref())
                .cloned(),
            control: merge_option(&base.control, &overlay.control, ControlConfig::merge),
        }
    }

    /// Three-level resolution: built-in defaults → branch recipe → per-call override.
    pub fn resolve(builtin: &Recipe, branch: &Recipe, per_call: Option<&Recipe>) -> Recipe {
        let merged = Recipe::merge(builtin, branch);
        match per_call {
            Some(o) => Recipe::merge(&merged, o),
            None => merged,
        }
    }
}

impl RetrieveConfig {
    fn merge(base: &Self, overlay: &Self) -> Self {
        RetrieveConfig {
            bm25: merge_option(&base.bm25, &overlay.bm25, BM25Config::merge),
            vector: merge_option(&base.vector, &overlay.vector, VectorRetrieveConfig::merge),
            graph: overlay.graph.as_ref().or(base.graph.as_ref()).cloned(),
        }
    }
}

impl BM25Config {
    fn merge(base: &Self, overlay: &Self) -> Self {
        BM25Config {
            sources: overlay.sources.as_ref().or(base.sources.as_ref()).cloned(),
            spaces: overlay.spaces.as_ref().or(base.spaces.as_ref()).cloned(),
            k: overlay.k.or(base.k),
            k1: overlay.k1.or(base.k1),
            b: overlay.b.or(base.b),
            field_weights: overlay
                .field_weights
                .as_ref()
                .or(base.field_weights.as_ref())
                .cloned(),
            stemmer: overlay.stemmer.as_ref().or(base.stemmer.as_ref()).cloned(),
            stopwords: overlay
                .stopwords
                .as_ref()
                .or(base.stopwords.as_ref())
                .cloned(),
            phrase_boost: overlay.phrase_boost.or(base.phrase_boost),
            proximity_boost: overlay.proximity_boost.or(base.proximity_boost),
        }
    }
}

impl VectorRetrieveConfig {
    fn merge(base: &Self, overlay: &Self) -> Self {
        VectorRetrieveConfig {
            collections: overlay
                .collections
                .as_ref()
                .or(base.collections.as_ref())
                .cloned(),
            k: overlay.k.or(base.k),
            metric: overlay.metric.as_ref().or(base.metric.as_ref()).cloned(),
        }
    }
}

impl FusionConfig {
    fn merge(base: &Self, overlay: &Self) -> Self {
        FusionConfig {
            method: overlay.method.as_ref().or(base.method.as_ref()).cloned(),
            k: overlay.k.or(base.k),
            weights: overlay.weights.as_ref().or(base.weights.as_ref()).cloned(),
        }
    }
}

impl TransformConfig {
    fn merge(base: &Self, overlay: &Self) -> Self {
        TransformConfig {
            limit: overlay.limit.or(base.limit),
            dedup: overlay.dedup.as_ref().or(base.dedup.as_ref()).cloned(),
        }
    }
}

impl ModelsConfig {
    fn merge(base: &Self, overlay: &Self) -> Self {
        ModelsConfig {
            embed: overlay.embed.as_ref().or(base.embed.as_ref()).cloned(),
            rerank: overlay.rerank.as_ref().or(base.rerank.as_ref()).cloned(),
            generate: overlay
                .generate
                .as_ref()
                .or(base.generate.as_ref())
                .cloned(),
            expand: overlay.expand.as_ref().or(base.expand.as_ref()).cloned(),
        }
    }
}

impl ControlConfig {
    fn merge(base: &Self, overlay: &Self) -> Self {
        ControlConfig {
            budget_ms: overlay.budget_ms.or(base.budget_ms),
            max_candidates: overlay.max_candidates.or(base.max_candidates),
        }
    }
}

// ============================================================================
// Built-in Defaults
// ============================================================================

/// The built-in default recipe (level 0 of three-level merge).
///
/// These values match Anserini/Pyserini BEIR-tuned defaults.
pub fn builtin_defaults() -> Recipe {
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
        control: Some(ControlConfig {
            budget_ms: Some(5000),
            ..Default::default()
        }),
        ..Default::default()
    }
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
    fn test_recipe_merge_empty_overlay() {
        let base = builtin_defaults();
        let overlay = Recipe::default();
        let merged = Recipe::merge(&base, &overlay);
        assert_eq!(merged, base);
    }

    #[test]
    fn test_recipe_merge_single_field_override() {
        let base = builtin_defaults();
        let overlay = Recipe {
            retrieve: Some(RetrieveConfig {
                bm25: Some(BM25Config {
                    k1: Some(1.5),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let merged = Recipe::merge(&base, &overlay);

        // k1 overridden
        let bm25 = merged.retrieve.as_ref().unwrap().bm25.as_ref().unwrap();
        assert_eq!(bm25.k1, Some(1.5));
        // b still from base
        assert_eq!(bm25.b, Some(0.4));
        // k still from base
        assert_eq!(bm25.k, Some(50));
        // fusion still from base
        assert_eq!(merged.fusion.as_ref().unwrap().method, Some("rrf".into()));
    }

    #[test]
    fn test_recipe_resolve_three_levels() {
        let builtin = builtin_defaults();

        let branch = Recipe {
            retrieve: Some(RetrieveConfig {
                bm25: Some(BM25Config {
                    k1: Some(1.2),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let per_call = Recipe {
            transform: Some(TransformConfig {
                limit: Some(5),
                ..Default::default()
            }),
            ..Default::default()
        };

        let resolved = Recipe::resolve(&builtin, &branch, Some(&per_call));

        // k1 from branch level
        assert_eq!(
            resolved
                .retrieve
                .as_ref()
                .unwrap()
                .bm25
                .as_ref()
                .unwrap()
                .k1,
            Some(1.2)
        );
        // b from builtin (branch didn't override)
        assert_eq!(
            resolved.retrieve.as_ref().unwrap().bm25.as_ref().unwrap().b,
            Some(0.4)
        );
        // limit from per_call
        assert_eq!(resolved.transform.as_ref().unwrap().limit, Some(5));
        // budget_ms from builtin (neither branch nor per_call overrode)
        assert_eq!(resolved.control.as_ref().unwrap().budget_ms, Some(5000));
    }

    #[test]
    fn test_builtin_defaults() {
        let d = builtin_defaults();
        assert_eq!(d.version, Some(1));
        let bm25 = d.retrieve.as_ref().unwrap().bm25.as_ref().unwrap();
        assert_eq!(bm25.k1, Some(0.9));
        assert_eq!(bm25.b, Some(0.4));
        assert_eq!(bm25.k, Some(50));
        assert_eq!(bm25.stemmer, Some("porter".into()));
        assert_eq!(bm25.stopwords, Some("lucene33".into()));
        assert_eq!(d.fusion.as_ref().unwrap().method, Some("rrf".into()));
        assert_eq!(d.fusion.as_ref().unwrap().k, Some(60));
        assert_eq!(d.transform.as_ref().unwrap().limit, Some(10));
        assert_eq!(d.control.as_ref().unwrap().budget_ms, Some(5000));
    }

    #[test]
    fn test_recipe_roundtrip() {
        let original = builtin_defaults();
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Recipe = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }
}
