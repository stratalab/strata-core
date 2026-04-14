//! RAG (retrieval-augmented generation) for Strata search.
//!
//! Takes the top-N hits from the substrate, builds a prompt
//! (sandwich-ordered, type-tagged — see `prompt.rs` and the v0.6 plan),
//! calls the generation model specified by `recipe.models.generate`, and
//! parses citations from the result. Returns `None` on any failure
//! (model load error, generation error, no hits) — the caller treats a
//! `None` answer as "RAG didn't run, return hits without an answer".
//!
//! # Trigger
//!
//! `recipe.prompt.is_some()` is the gate. Recipes without a `prompt` field
//! get standard search; recipes with one (e.g. the `"rag"` builtin) get
//! RAG. Mirrors how `expansion`/`rerank` are gated.
//!
//! # Multi-model
//!
//! Reads `recipe.models.generate` (default `"local:qwen3:1.7b"`) and
//! passes the spec to the existing strata-inference loader. Local AND
//! cloud providers work uniformly when the corresponding features are
//! compiled in. Multi-model **routing** (rules, fallbacks, cost-aware
//! selection) stays in v0.8; v0.6 just respects the field.

use strata_engine::search::recipe::Recipe;
use strata_engine::search::SearchHit;
use strata_engine::Database;

pub use citation::extract_citations;
pub use prompt::build_rag_prompt;

mod citation;
mod prompt;

/// Default model spec when `recipe.models.generate` is unset.
const DEFAULT_RAG_MODEL: &str = "local:qwen3:1.7b";

/// Default top-N hits to feed into the prompt.
const DEFAULT_CONTEXT_HITS: usize = 5;

/// Default answer length budget in tokens.
const DEFAULT_MAX_TOKENS: usize = 500;

/// Sampling temperature for RAG generation. Lower than expansion (which
/// uses 0.7) because RAG wants factual, grounded answers, not creative
/// reformulations.
const RAG_TEMPERATURE: f32 = 0.3;

/// Top-k sampling for RAG generation.
const RAG_TOP_K: usize = 40;

/// Top-p (nucleus) sampling for RAG generation.
const RAG_TOP_P: f32 = 0.9;

/// Result of a successful RAG generation pass.
///
/// The caller (executor handler) builds the executor-level
/// `AnswerResponse` from `text` and `sources`, and populates the
/// `rag_*` fields on `SearchStatsOutput` from the metadata fields.
#[derive(Debug, Clone)]
pub struct RagAnswer {
    /// The generated answer text.
    pub text: String,
    /// 1-indexed hit numbers cited in `text`.
    pub sources: Vec<usize>,
    /// Model spec actually used (echoes `recipe.models.generate` or the default).
    pub model: String,
    /// Wall-clock time spent in the model call, in milliseconds.
    pub elapsed_ms: f64,
    /// Prompt token count reported by the model.
    pub tokens_in: u32,
    /// Completion token count reported by the model.
    pub tokens_out: u32,
}

/// Generate a grounded answer from search hits.
///
/// Returns `Some(RagAnswer)` on success, `None` on any failure
/// (graceful degradation — never propagates an error to the caller).
///
/// The trigger is `recipe.prompt.is_some()`. If the prompt is unset,
/// returns `None` without doing any work.
pub fn generate_answer(
    db: &Database,
    query: &str,
    hits: &[SearchHit],
    recipe: &Recipe,
) -> Option<RagAnswer> {
    // Trigger gate: only run RAG when the recipe has a prompt set.
    let prompt_template = recipe.prompt.as_deref()?;

    let model_spec = recipe
        .models
        .as_ref()
        .and_then(|m| m.generate.as_deref())
        .unwrap_or(DEFAULT_RAG_MODEL);

    // Zero-hits canned response — never call the model.
    if hits.is_empty() {
        return Some(RagAnswer {
            text: "I don't have enough information to answer this question.".into(),
            sources: Vec::new(),
            model: model_spec.to_string(),
            elapsed_ms: 0.0,
            tokens_in: 0,
            tokens_out: 0,
        });
    }

    let context_hits = recipe.rag_context_hits.unwrap_or(DEFAULT_CONTEXT_HITS);
    let max_tokens = recipe.rag_max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
    let top_n = &hits[..hits.len().min(context_hits)];

    // Build the prompt (sandwich-ordered, type-tagged, token-budgeted).
    let full_prompt = match prompt::build_rag_prompt(prompt_template, top_n, query, max_tokens) {
        Some(p) => p,
        None => {
            tracing::warn!(
                target: "strata::rag",
                "RAG prompt construction returned None; skipping generation"
            );
            return None;
        }
    };

    // Load the generation model. The recipe carries a `provider:model` spec
    // (e.g. `"local:qwen3:1.7b"`, `"anthropic:claude-sonnet-4-6"`). Dispatch
    // on the provider prefix:
    //   - `local:` (or no prefix)  → cached local registry load
    //   - `anthropic:`/`openai:`/`google:` → uncached cloud HTTP wrapper,
    //                                        API key sourced from db config
    //                                        (which is auto-populated from
    //                                        ANTHROPIC_API_KEY etc. env vars
    //                                        by `apply_env_overrides`)
    //
    // Cloud engines are deliberately NOT cached: they're stateless HTTP
    // wrappers and not caching avoids stale-API-key bugs when the user
    // updates their key via `CONFIGURE SET`.
    let state = match db.extension::<crate::generate::GenerateModelState>() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                target: "strata::rag",
                error = %e,
                "Failed to get generate model state for RAG"
            );
            return None;
        }
    };

    let entry = match load_generation_engine(db, &state, model_spec) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(
                target: "strata::rag",
                error = %e,
                model = %model_spec,
                "RAG model load failed; returning hits without answer"
            );
            return None;
        }
    };

    let request = crate::GenerateRequest {
        prompt: full_prompt,
        max_tokens,
        temperature: RAG_TEMPERATURE,
        top_k: RAG_TOP_K,
        top_p: RAG_TOP_P,
        seed: None,
        stop_sequences: vec![],
        stop_tokens: vec![],
        grammar: None, // RAG output is free-form text, no GBNF
    };

    let start = std::time::Instant::now();
    let result = crate::generate::with_engine(&entry, |engine| engine.generate(&request));
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let response = match result {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            tracing::warn!(
                target: "strata::rag",
                error = %e,
                model = %model_spec,
                "RAG generation failed"
            );
            return None;
        }
        Err(e) => {
            tracing::warn!(
                target: "strata::rag",
                error = %e,
                "RAG engine error"
            );
            return None;
        }
    };

    let sources = citation::extract_citations(&response.text, top_n.len());

    let tokens_in = response.prompt_tokens as u32;
    let tokens_out = response.completion_tokens as u32;

    tracing::debug!(
        target: "strata::rag",
        model = %model_spec,
        elapsed_ms = elapsed_ms,
        tokens_in = tokens_in,
        tokens_out = tokens_out,
        sources = sources.len(),
        "RAG generation succeeded"
    );

    Some(RagAnswer {
        text: response.text,
        sources,
        model: model_spec.to_string(),
        elapsed_ms,
        tokens_in,
        tokens_out,
    })
}

/// Load a generation engine for a `provider:model` spec, dispatching local
/// (cached) vs cloud (uncached, key from db config) routing.
///
/// Cloud engines are deliberately not cached — see `create_cloud_engine`'s
/// docstring for why. The cache is local-only.
fn load_generation_engine(
    db: &Database,
    state: &crate::generate::GenerateModelState,
    model_spec: &str,
) -> Result<std::sync::Arc<std::sync::Mutex<crate::generate::CachedEngine>>, String> {
    use strata_inference::ProviderKind;

    // Local path: strip optional `local:` prefix and use the cache.
    if !model_spec.starts_with("anthropic:")
        && !model_spec.starts_with("openai:")
        && !model_spec.starts_with("google:")
    {
        let local_name = model_spec.strip_prefix("local:").unwrap_or(model_spec);
        return state.get_or_load(local_name);
    }

    // Cloud path: parse provider, fetch API key from db config, build a
    // fresh (uncached) engine.
    let (provider_str, model_name) = model_spec.split_once(':').ok_or_else(|| {
        format!(
            "Malformed cloud spec '{}': expected 'provider:model'",
            model_spec
        )
    })?;
    let provider: ProviderKind = provider_str
        .parse()
        .map_err(|_| format!("Unknown provider '{}'", provider_str))?;

    let cfg = db.config();
    let api_key = match provider {
        ProviderKind::Anthropic => cfg.anthropic_api_key.clone(),
        ProviderKind::OpenAI => cfg.openai_api_key.clone(),
        ProviderKind::Google => cfg.google_api_key.clone(),
        ProviderKind::Local => unreachable!("local handled above"),
    };
    let api_key = api_key.filter(|k| !k.as_str().is_empty()).ok_or_else(|| {
        format!(
            "No API key configured for {} (set {}_API_KEY env var or `CONFIGURE SET {}_api_key ...`)",
            provider,
            provider.to_string().to_uppercase(),
            provider,
        )
    })?;

    crate::generate::GenerateModelState::create_cloud_engine(
        provider,
        api_key.into_inner(),
        model_name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::BranchId;
    use strata_core::EntityRef;
    use strata_engine::search::recipe::{ModelsConfig, Recipe};

    fn kv_hit(key: &str, snippet: &str) -> SearchHit {
        SearchHit {
            doc_ref: EntityRef::Kv {
                branch_id: BranchId::from_bytes([0u8; 16]),
                space: "default".into(),
                key: key.into(),
            },
            score: 0.9,
            rank: 1,
            snippet: Some(snippet.into()),
        }
    }

    fn standard_recipe() -> Recipe {
        // No prompt → RAG disabled.
        Recipe::default()
    }

    fn rag_recipe(model_spec: Option<&str>) -> Recipe {
        Recipe {
            prompt: Some("Answer using only the provided context. Cite [N].".into()),
            rag_context_hits: Some(3),
            rag_max_tokens: Some(200),
            models: Some(ModelsConfig {
                generate: model_spec.map(|s| s.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_generate_answer_skipped_when_prompt_unset() {
        use strata_engine::database::search_only_cache_spec;
        let db = Database::open_runtime(search_only_cache_spec()).expect("create db");
        let hits = vec![kv_hit("doc1", "rust is a systems language")];
        let result = generate_answer(&db, "What is Rust?", &hits, &standard_recipe());
        assert!(
            result.is_none(),
            "RAG must be skipped when recipe.prompt is None"
        );
    }

    #[test]
    fn test_generate_answer_zero_hits_canned_response() {
        use strata_engine::database::search_only_cache_spec;
        let db = Database::open_runtime(search_only_cache_spec()).expect("create db");
        let recipe = rag_recipe(Some("local:qwen3:1.7b"));
        let result = generate_answer(&db, "What is Rust?", &[], &recipe);
        let answer = result.expect("zero-hits should return a canned response, not None");
        assert_eq!(
            answer.text,
            "I don't have enough information to answer this question."
        );
        assert!(answer.sources.is_empty());
        assert_eq!(answer.elapsed_ms, 0.0, "zero-hits path skips the model");
        assert_eq!(answer.tokens_in, 0);
        assert_eq!(answer.tokens_out, 0);
        assert_eq!(answer.model, "local:qwen3:1.7b");
    }

    #[test]
    fn test_generate_answer_zero_hits_uses_default_model_when_unset() {
        use strata_engine::database::search_only_cache_spec;
        let db = Database::open_runtime(search_only_cache_spec()).expect("create db");
        let recipe = rag_recipe(None); // No models.generate
        let result = generate_answer(&db, "Q?", &[], &recipe);
        let answer = result.expect("canned response should still return");
        assert_eq!(answer.model, DEFAULT_RAG_MODEL);
    }

    #[test]
    fn test_generate_answer_model_load_failure_returns_none() {
        // An invalid model spec should fail to load → graceful None.
        use strata_engine::database::search_only_cache_spec;
        let db = Database::open_runtime(search_only_cache_spec()).expect("create db");
        let recipe = rag_recipe(Some("nonexistent:model"));
        let hits = vec![kv_hit("doc1", "some content about rust")];
        let result = generate_answer(&db, "What is Rust?", &hits, &recipe);
        assert!(
            result.is_none(),
            "invalid model spec should produce None, not an error"
        );
    }
}
