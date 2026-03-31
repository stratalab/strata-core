mod error;
pub mod registry;

#[cfg(feature = "local")]
pub mod llama;

#[cfg(feature = "local")]
mod embed;

#[cfg(any(feature = "openai", feature = "google"))]
mod cloud_embed;

#[cfg(feature = "local")]
mod rank;

#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
mod provider;

#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
mod generate;

pub use error::InferenceError;
pub use registry::{ModelInfo, ModelRegistry, ModelTask};

#[cfg(feature = "local")]
pub use embed::EmbeddingEngine;

#[cfg(any(feature = "openai", feature = "google"))]
pub use cloud_embed::CloudEmbeddingEngine;

#[cfg(feature = "local")]
pub use rank::RankingEngine;

#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
pub use generate::GenerationEngine;

use std::fmt;
use std::str::FromStr;

/// A request for text generation, provider-agnostic.
#[derive(Debug, Clone)]
pub struct GenerateRequest {
    pub prompt: String,
    pub max_tokens: usize,
    pub temperature: f32,
    pub top_k: usize,
    pub top_p: f32,
    pub seed: Option<u64>,
    pub stop_sequences: Vec<String>,
    pub stop_tokens: Vec<u32>,
    /// Optional GBNF grammar string for constrained generation.
    ///
    /// For local models, this is passed to llama.cpp's grammar sampler.
    /// For OpenAI, this enables JSON mode (`response_format: json_object`).
    /// For Anthropic/Google, this field is silently ignored.
    pub grammar: Option<String>,
}

impl Default for GenerateRequest {
    fn default() -> Self {
        Self {
            prompt: String::new(),
            max_tokens: 256,
            temperature: 0.0,
            top_k: 0,
            top_p: 1.0,
            seed: None,
            stop_sequences: Vec::new(),
            stop_tokens: Vec::new(),
            grammar: None,
        }
    }
}

/// The result of a generation request.
#[derive(Debug, Clone)]
pub struct GenerateResponse {
    pub text: String,
    pub stop_reason: StopReason,
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
}

/// Why generation stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    StopToken,
    MaxTokens,
    ContextLength,
    Cancelled,
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopReason::StopToken => write!(f, "stop_token"),
            StopReason::MaxTokens => write!(f, "max_tokens"),
            StopReason::ContextLength => write!(f, "context_length"),
            StopReason::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Which inference provider to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    Local,
    Anthropic,
    OpenAI,
    Google,
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProviderKind::Local => write!(f, "local"),
            ProviderKind::Anthropic => write!(f, "anthropic"),
            ProviderKind::OpenAI => write!(f, "openai"),
            ProviderKind::Google => write!(f, "google"),
        }
    }
}

impl FromStr for ProviderKind {
    type Err = InferenceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(ProviderKind::Local),
            "anthropic" => Ok(ProviderKind::Anthropic),
            "openai" => Ok(ProviderKind::OpenAI),
            "google" => Ok(ProviderKind::Google),
            _ => Err(InferenceError::Provider(format!(
                "unknown provider: {:?} (expected: local, anthropic, openai, google)",
                s.trim()
            ))),
        }
    }
}

/// Unified inference trait for generation and embedding.
///
/// Implementors override the methods they support. Default implementations
/// return `NotSupported`, so a generation-only engine can be held as
/// `Box<dyn InferenceEngine>` without implementing `embed()`.
pub trait InferenceEngine: Send + std::fmt::Debug {
    /// Generate text from a prompt.
    fn generate(&mut self, request: &GenerateRequest) -> Result<GenerateResponse, InferenceError> {
        let _ = request;
        Err(InferenceError::NotSupported(
            "this engine does not support generation".to_string(),
        ))
    }

    /// Embed a single text into a dense vector.
    fn embed(&self, text: &str) -> Result<Vec<f32>, InferenceError> {
        let _ = text;
        Err(InferenceError::NotSupported(
            "this engine does not support embedding".to_string(),
        ))
    }

    /// Embed a batch of texts into dense vectors.
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError> {
        let _ = texts;
        Err(InferenceError::NotSupported(
            "this engine does not support embedding".to_string(),
        ))
    }

    /// Whether this engine supports generation.
    fn supports_generate(&self) -> bool {
        false
    }

    /// Whether this engine supports embedding.
    fn supports_embed(&self) -> bool {
        false
    }

    /// Score passages against a query using cross-encoder reranking.
    ///
    /// Returns one relevance score per passage. Higher scores indicate
    /// greater relevance. `scores[i]` corresponds to `passages[i]`.
    fn rank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, InferenceError> {
        let _ = (query, passages);
        Err(InferenceError::NotSupported(
            "this engine does not support ranking".to_string(),
        ))
    }

    /// Whether this engine supports ranking/reranking.
    fn supports_rank(&self) -> bool {
        false
    }

    /// Dimensionality of embedding vectors produced by this engine.
    /// Returns 0 if unknown or engine doesn't support embedding.
    fn embedding_dim(&self) -> usize {
        0
    }

    /// Whether the engine is healthy and operational.
    /// Local engines return false if their internal state is poisoned.
    /// Cloud engines always return true (no internal state).
    fn is_healthy(&self) -> bool {
        true
    }
}

/// Parse a `"provider:model_name"` spec into its components.
///
/// Format: `"provider:model_name"` where provider is one of: local, anthropic, openai, google.
/// A bare name without a colon (e.g., `"miniLM"`) defaults to `local`.
/// For local models with colons in the name (e.g., `"local:qwen3:1.7b:q8_0"`), only the
/// first colon separates provider from model — the rest is part of the model name.
pub fn parse_model_spec(spec: &str) -> Result<(ProviderKind, String), InferenceError> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(InferenceError::Provider("model spec is empty".to_string()));
    }

    // Split on first colon only
    let (provider_str, model) = match spec.find(':') {
        Some(idx) => (&spec[..idx], &spec[idx + 1..]),
        None => return Ok((ProviderKind::Local, spec.to_string())),
    };

    let provider: ProviderKind = provider_str.parse()?;

    let model = model.trim();
    if model.is_empty() {
        return Err(InferenceError::Provider(format!(
            "model name is empty in spec {:?}",
            spec
        )));
    }

    Ok((provider, model.to_string()))
}

/// Environment variable name for a provider's API key.
fn api_key_env_var(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Anthropic => "ANTHROPIC_API_KEY",
        ProviderKind::OpenAI => "OPENAI_API_KEY",
        ProviderKind::Google => "GOOGLE_API_KEY",
        ProviderKind::Local => "STRATA_LOCAL_API_KEY", // unused, but complete
    }
}

/// Load an inference engine from a `"provider:model_name"` spec.
///
/// Returns a trait object supporting generation. For cloud providers, reads
/// the API key from environment variables (`ANTHROPIC_API_KEY`, etc.).
///
/// # Examples
///
/// ```ignore
/// let mut engine = strata_inference::load("local:qwen3:1.7b")?;
/// let mut engine = strata_inference::load("anthropic:claude-sonnet-4-6")?;
/// ```
#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
pub fn load(model_spec: &str) -> Result<Box<dyn InferenceEngine>, InferenceError> {
    let (provider, model) = parse_model_spec(model_spec)?;

    match provider {
        #[cfg(feature = "local")]
        ProviderKind::Local => Ok(Box::new(GenerationEngine::from_registry(&model)?)),
        #[cfg(not(feature = "local"))]
        ProviderKind::Local => Err(InferenceError::NotSupported(
            "local provider requires the 'local' feature".to_string(),
        )),
        cloud_provider => {
            let env_var = api_key_env_var(cloud_provider);
            let api_key = std::env::var(env_var).map_err(|_| {
                InferenceError::Provider(format!(
                    "{} not set (required for {}:{})",
                    env_var, cloud_provider, model
                ))
            })?;
            Ok(Box::new(GenerationEngine::cloud(
                cloud_provider,
                api_key,
                model,
            )?))
        }
    }
}

/// Load an embedding engine from a `"provider:model_name"` spec.
///
/// Supports local models (via llama.cpp), OpenAI, and Google embedding APIs.
/// Anthropic does not offer an embedding API and returns `NotSupported`.
///
/// # Examples
///
/// ```ignore
/// let engine = strata_inference::load_embedder("local:miniLM")?;
/// let embedding = engine.embed("hello world")?;
///
/// let engine = strata_inference::load_embedder("openai:text-embedding-3-small")?;
/// let embedding = engine.embed("hello world")?;
/// ```
#[cfg(any(
    feature = "local",
    feature = "openai",
    feature = "google",
    feature = "anthropic"
))]
pub fn load_embedder(model_spec: &str) -> Result<Box<dyn InferenceEngine>, InferenceError> {
    let (provider, model) = parse_model_spec(model_spec)?;

    match provider {
        #[cfg(feature = "local")]
        ProviderKind::Local => Ok(Box::new(EmbeddingEngine::from_registry(&model)?)),
        #[cfg(not(feature = "local"))]
        ProviderKind::Local => Err(InferenceError::NotSupported(
            "local provider requires the 'local' feature".to_string(),
        )),

        ProviderKind::Anthropic => Err(InferenceError::NotSupported(
            "Anthropic does not offer an embedding API".to_string(),
        )),

        #[cfg(any(feature = "openai", feature = "google"))]
        cloud_provider => {
            let env_var = api_key_env_var(cloud_provider);
            let api_key = std::env::var(env_var).map_err(|_| {
                InferenceError::Provider(format!(
                    "{} not set (required for {}:{})",
                    env_var, cloud_provider, model
                ))
            })?;
            Ok(Box::new(CloudEmbeddingEngine::new(
                cloud_provider,
                api_key,
                model,
            )?))
        }

        #[cfg(not(any(feature = "openai", feature = "google")))]
        other => {
            let _ = model;
            Err(InferenceError::NotSupported(format!(
                "cloud embedding requires 'openai' or 'google' feature (provider: {other})"
            )))
        }
    }
}

/// Load a ranking engine from a `"provider:model_name"` spec.
///
/// Currently only local cross-encoder models are supported.
///
/// # Examples
///
/// ```ignore
/// let engine = strata_inference::load_ranker("local:jina-reranker-v1-tiny")?;
/// let scores = engine.rank("query", &["passage 1", "passage 2"])?;
/// ```
#[cfg(feature = "local")]
pub fn load_ranker(model_spec: &str) -> Result<Box<dyn InferenceEngine>, InferenceError> {
    let (provider, model) = parse_model_spec(model_spec)?;

    match provider {
        ProviderKind::Local => Ok(Box::new(RankingEngine::from_registry(&model)?)),
        other => Err(InferenceError::NotSupported(format!(
            "cloud ranking not yet supported (provider: {})",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- GenerateRequest ---

    #[test]
    fn generate_request_default_values() {
        let req = GenerateRequest::default();
        assert_eq!(req.max_tokens, 256);
        assert_eq!(req.temperature, 0.0);
        assert_eq!(req.top_k, 0);
        assert_eq!(req.top_p, 1.0);
        assert!(req.seed.is_none());
        assert!(req.stop_sequences.is_empty());
        assert!(req.stop_tokens.is_empty());
        assert!(req.prompt.is_empty());
        assert!(req.grammar.is_none());
    }

    #[test]
    fn grammar_field_default_is_none() {
        let req = GenerateRequest::default();
        assert!(req.grammar.is_none());
    }

    #[test]
    fn grammar_field_can_be_set() {
        let req = GenerateRequest {
            prompt: "List colors as JSON".into(),
            grammar: Some(
                r#"root ::= "{" ws "\"colors\"" ws ":" ws "[" ws string ("," ws string)* "]" ws "}"
ws ::= [ \t\n]*
string ::= "\"" [a-zA-Z]+ "\""
"#
                .into(),
            ),
            ..Default::default()
        };
        assert!(req.grammar.is_some());
        assert!(req.grammar.as_ref().unwrap().contains("root"));
    }

    #[test]
    fn grammar_field_survives_clone() {
        let req = GenerateRequest {
            grammar: Some("root ::= \"hello\"".into()),
            ..Default::default()
        };
        let cloned = req.clone();
        assert_eq!(cloned.grammar, Some("root ::= \"hello\"".into()));
    }

    #[test]
    fn generate_request_clone_all_fields() {
        let req = GenerateRequest {
            prompt: "test prompt".into(),
            max_tokens: 512,
            temperature: 0.7,
            top_k: 40,
            top_p: 0.9,
            seed: Some(42),
            stop_sequences: vec!["STOP".into(), "\n\n".into()],
            stop_tokens: vec![1, 2, 50256],
            grammar: Some("root ::= \"test\"".into()),
        };
        let cloned = req.clone();
        assert_eq!(cloned.prompt, "test prompt");
        assert_eq!(cloned.max_tokens, 512);
        assert_eq!(cloned.temperature, 0.7);
        assert_eq!(cloned.top_k, 40);
        assert_eq!(cloned.top_p, 0.9);
        assert_eq!(cloned.seed, Some(42));
        assert_eq!(cloned.stop_sequences, vec!["STOP", "\n\n"]);
        assert_eq!(cloned.stop_tokens, vec![1, 2, 50256]);
    }

    #[test]
    fn generate_request_clone_is_independent() {
        let mut original = GenerateRequest {
            prompt: "original".into(),
            stop_sequences: vec!["a".into()],
            ..Default::default()
        };
        let cloned = original.clone();
        // Mutating original shouldn't affect clone
        original.prompt = "mutated".into();
        original.stop_sequences.push("b".into());
        assert_eq!(cloned.prompt, "original");
        assert_eq!(cloned.stop_sequences.len(), 1);
    }

    #[test]
    fn generate_request_struct_update_syntax() {
        // Verify ..Default::default() works correctly for partial construction
        let req = GenerateRequest {
            prompt: "hello".into(),
            max_tokens: 10,
            ..Default::default()
        };
        assert_eq!(req.temperature, 0.0);
        assert_eq!(req.top_p, 1.0);
        assert!(req.seed.is_none());
    }

    // --- GenerateResponse ---

    #[test]
    fn generate_response_construction() {
        let resp = GenerateResponse {
            text: "Hello world".into(),
            stop_reason: StopReason::StopToken,
            prompt_tokens: 5,
            completion_tokens: 2,
        };
        assert_eq!(resp.text, "Hello world");
        assert_eq!(resp.stop_reason, StopReason::StopToken);
        assert_eq!(resp.prompt_tokens, 5);
        assert_eq!(resp.completion_tokens, 2);
    }

    #[test]
    fn generate_response_clone() {
        let resp = GenerateResponse {
            text: "cloned text".into(),
            stop_reason: StopReason::MaxTokens,
            prompt_tokens: 10,
            completion_tokens: 256,
        };
        let cloned = resp.clone();
        assert_eq!(cloned.text, "cloned text");
        assert_eq!(cloned.stop_reason, StopReason::MaxTokens);
        assert_eq!(cloned.prompt_tokens, 10);
        assert_eq!(cloned.completion_tokens, 256);
    }

    #[test]
    fn generate_response_with_empty_text() {
        let resp = GenerateResponse {
            text: String::new(),
            stop_reason: StopReason::ContextLength,
            prompt_tokens: 1024,
            completion_tokens: 0,
        };
        assert!(resp.text.is_empty());
        assert_eq!(resp.completion_tokens, 0);
    }

    // --- StopReason ---

    #[test]
    fn stop_reason_display() {
        assert_eq!(StopReason::StopToken.to_string(), "stop_token");
        assert_eq!(StopReason::MaxTokens.to_string(), "max_tokens");
        assert_eq!(StopReason::ContextLength.to_string(), "context_length");
        assert_eq!(StopReason::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn stop_reason_equality_and_inequality() {
        assert_eq!(StopReason::StopToken, StopReason::StopToken);
        assert_ne!(StopReason::StopToken, StopReason::MaxTokens);
        assert_ne!(StopReason::MaxTokens, StopReason::ContextLength);
        assert_ne!(StopReason::ContextLength, StopReason::Cancelled);
        // Exhaustive: every pair of distinct variants is unequal
        let variants = [
            StopReason::StopToken,
            StopReason::MaxTokens,
            StopReason::ContextLength,
            StopReason::Cancelled,
        ];
        for (i, a) in variants.iter().enumerate() {
            for (j, b) in variants.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn stop_reason_copy() {
        let a = StopReason::Cancelled;
        let b = a; // Copy, not move
        let c = a; // Can still use `a` after copy
        assert_eq!(b, c);
    }

    #[test]
    fn stop_reason_debug() {
        // Verify Debug output includes variant name (not just Display)
        let dbg = format!("{:?}", StopReason::StopToken);
        assert_eq!(dbg, "StopToken");
    }

    // --- ProviderKind ---

    #[test]
    fn provider_kind_display() {
        assert_eq!(ProviderKind::Local.to_string(), "local");
        assert_eq!(ProviderKind::Anthropic.to_string(), "anthropic");
        assert_eq!(ProviderKind::OpenAI.to_string(), "openai");
        assert_eq!(ProviderKind::Google.to_string(), "google");
    }

    #[test]
    fn provider_kind_from_str_exact() {
        assert_eq!(
            "local".parse::<ProviderKind>().unwrap(),
            ProviderKind::Local
        );
        assert_eq!(
            "anthropic".parse::<ProviderKind>().unwrap(),
            ProviderKind::Anthropic
        );
        assert_eq!(
            "openai".parse::<ProviderKind>().unwrap(),
            ProviderKind::OpenAI
        );
        assert_eq!(
            "google".parse::<ProviderKind>().unwrap(),
            ProviderKind::Google
        );
    }

    #[test]
    fn provider_kind_from_str_case_insensitive() {
        assert_eq!(
            "Anthropic".parse::<ProviderKind>().unwrap(),
            ProviderKind::Anthropic
        );
        assert_eq!(
            "OPENAI".parse::<ProviderKind>().unwrap(),
            ProviderKind::OpenAI
        );
        assert_eq!(
            "Google".parse::<ProviderKind>().unwrap(),
            ProviderKind::Google
        );
        assert_eq!(
            "LOCAL".parse::<ProviderKind>().unwrap(),
            ProviderKind::Local
        );
    }

    #[test]
    fn provider_kind_from_str_trims_whitespace() {
        assert_eq!(
            " anthropic ".parse::<ProviderKind>().unwrap(),
            ProviderKind::Anthropic
        );
        assert_eq!(
            "\topenai\n".parse::<ProviderKind>().unwrap(),
            ProviderKind::OpenAI
        );
    }

    #[test]
    fn provider_kind_from_str_invalid_has_useful_error() {
        let err = "unknown".parse::<ProviderKind>().unwrap_err();
        let msg = err.to_string();
        // Error should contain the bad input
        assert!(
            msg.contains("unknown"),
            "error should contain bad input: {msg}"
        );
        // Error should list valid options
        assert!(
            msg.contains("local"),
            "error should list valid options: {msg}"
        );
        assert!(
            msg.contains("anthropic"),
            "error should list valid options: {msg}"
        );
    }

    #[test]
    fn provider_kind_from_str_empty_is_error() {
        assert!("".parse::<ProviderKind>().is_err());
        assert!("  ".parse::<ProviderKind>().is_err());
    }

    #[test]
    fn provider_kind_display_from_str_roundtrip() {
        let variants = [
            ProviderKind::Local,
            ProviderKind::Anthropic,
            ProviderKind::OpenAI,
            ProviderKind::Google,
        ];
        for p in &variants {
            let s = p.to_string();
            let parsed: ProviderKind = s.parse().unwrap();
            assert_eq!(*p, parsed, "round-trip failed for {p}");
        }
    }

    #[test]
    fn provider_kind_equality_debug_clone_copy() {
        let p = ProviderKind::Anthropic;
        let p2 = p; // Copy
        assert_eq!(p, p2);
        let p3 = p;
        assert_eq!(p, p3);
        let dbg = format!("{:?}", p);
        assert_eq!(dbg, "Anthropic");
    }

    #[test]
    fn provider_kind_all_variants_distinct() {
        let variants = [
            ProviderKind::Local,
            ProviderKind::Anthropic,
            ProviderKind::OpenAI,
            ProviderKind::Google,
        ];
        for (i, a) in variants.iter().enumerate() {
            for (j, b) in variants.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    // --- parse_model_spec ---

    #[test]
    fn model_spec_parse_local_model() {
        let (provider, model) = parse_model_spec("local:qwen3:1.7b").unwrap();
        assert_eq!(provider, ProviderKind::Local);
        assert_eq!(model, "qwen3:1.7b");
    }

    #[test]
    fn model_spec_parse_anthropic_model() {
        let (provider, model) = parse_model_spec("anthropic:claude-sonnet-4-6").unwrap();
        assert_eq!(provider, ProviderKind::Anthropic);
        assert_eq!(model, "claude-sonnet-4-6");
    }

    #[test]
    fn model_spec_parse_openai_model() {
        let (provider, model) = parse_model_spec("openai:gpt-4o-mini").unwrap();
        assert_eq!(provider, ProviderKind::OpenAI);
        assert_eq!(model, "gpt-4o-mini");
    }

    #[test]
    fn model_spec_parse_google_model() {
        let (provider, model) = parse_model_spec("google:gemini-2.5-flash").unwrap();
        assert_eq!(provider, ProviderKind::Google);
        assert_eq!(model, "gemini-2.5-flash");
    }

    #[test]
    fn model_spec_parse_bare_name_defaults_to_local() {
        let (provider, model) = parse_model_spec("miniLM").unwrap();
        assert_eq!(provider, ProviderKind::Local);
        assert_eq!(model, "miniLM");
    }

    #[test]
    fn model_spec_parse_empty_string_error() {
        assert!(parse_model_spec("").is_err());
    }

    #[test]
    fn model_spec_parse_whitespace_only_error() {
        assert!(parse_model_spec("  ").is_err());
    }

    #[test]
    fn model_spec_parse_unknown_provider_error() {
        let err = parse_model_spec("azure:gpt-4").unwrap_err();
        assert!(
            err.to_string().contains("azure"),
            "error should mention bad provider: {err}"
        );
    }

    #[test]
    fn model_spec_parse_provider_colon_empty_model_error() {
        assert!(parse_model_spec("anthropic:").is_err());
    }

    #[test]
    fn model_spec_parse_local_with_quant_variant() {
        // "local:qwen3:1.7b:q8_0" — provider is "local", model is "qwen3:1.7b:q8_0"
        let (provider, model) = parse_model_spec("local:qwen3:1.7b:q8_0").unwrap();
        assert_eq!(provider, ProviderKind::Local);
        assert_eq!(model, "qwen3:1.7b:q8_0");
    }

    // --- load ---

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn model_spec_load_cloud_missing_api_key() {
        // Ensure the env var is unset for this test
        std::env::remove_var("ANTHROPIC_API_KEY");
        let err = load("anthropic:claude-sonnet-4-6").unwrap_err();
        assert!(
            err.to_string().contains("ANTHROPIC_API_KEY"),
            "error should mention env var: {err}"
        );
    }

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn model_spec_load_cloud_with_api_key_constructs() {
        // Set a fake key — construction should succeed (actual API call isn't made)
        std::env::set_var("OPENAI_API_KEY", "sk-test-fake-key-12345");
        let result = load("openai:gpt-4o-mini");
        std::env::remove_var("OPENAI_API_KEY");
        assert!(
            result.is_ok(),
            "load should succeed with API key set: {:?}",
            result.err()
        );
    }

    // --- InferenceEngine trait ---

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn trait_load_returns_box_dyn() {
        // load() should return Box<dyn InferenceEngine>
        std::env::set_var("OPENAI_API_KEY", "sk-test-fake-key");
        let engine: Box<dyn InferenceEngine> = load("openai:gpt-4o-mini").unwrap();
        std::env::remove_var("OPENAI_API_KEY");
        // Cloud engine should not support embed
        let err = engine.embed("test").unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "embed on cloud should be NotSupported: {err}"
        );
    }

    #[cfg(feature = "openai")]
    #[test]
    fn load_embedder_openai_constructs_with_api_key() {
        std::env::set_var("OPENAI_API_KEY", "sk-test-embed-key");
        let result = load_embedder("openai:text-embedding-3-small");
        std::env::remove_var("OPENAI_API_KEY");
        assert!(
            result.is_ok(),
            "load_embedder should succeed for OpenAI: {:?}",
            result.err()
        );
        let engine = result.unwrap();
        assert!(engine.supports_embed());
        assert!(!engine.supports_generate());
    }

    #[cfg(feature = "openai")]
    #[test]
    fn load_embedder_openai_missing_api_key() {
        std::env::remove_var("OPENAI_API_KEY");
        let err = load_embedder("openai:text-embedding-3-small").unwrap_err();
        assert!(
            err.to_string().contains("OPENAI_API_KEY"),
            "error should mention env var: {err}"
        );
    }

    #[cfg(feature = "google")]
    #[test]
    fn load_embedder_google_constructs_with_api_key() {
        std::env::set_var("GOOGLE_API_KEY", "AIza-test-embed-key");
        let result = load_embedder("google:text-embedding-004");
        std::env::remove_var("GOOGLE_API_KEY");
        assert!(
            result.is_ok(),
            "load_embedder should succeed for Google: {:?}",
            result.err()
        );
        let engine = result.unwrap();
        assert!(engine.supports_embed());
    }

    #[cfg(feature = "google")]
    #[test]
    fn load_embedder_google_missing_api_key() {
        std::env::remove_var("GOOGLE_API_KEY");
        let err = load_embedder("google:text-embedding-004").unwrap_err();
        assert!(
            err.to_string().contains("GOOGLE_API_KEY"),
            "error should mention env var: {err}"
        );
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn load_embedder_anthropic_returns_not_supported() {
        std::env::set_var("ANTHROPIC_API_KEY", "sk-ant-test");
        let err = load_embedder("anthropic:some-model").unwrap_err();
        std::env::remove_var("ANTHROPIC_API_KEY");
        assert!(
            err.to_string().contains("Anthropic"),
            "error should mention Anthropic: {err}"
        );
        assert!(
            matches!(err, InferenceError::NotSupported(_)),
            "should be NotSupported: {err}"
        );
    }

    // --- load_ranker ---

    #[cfg(feature = "local")]
    #[test]
    fn load_ranker_cloud_not_supported() {
        let err = load_ranker("openai:some-reranker").unwrap_err();
        assert!(
            err.to_string().contains("not yet supported"),
            "cloud ranker should fail: {err}"
        );
    }

    #[cfg(feature = "local")]
    #[test]
    fn load_ranker_unknown_model_returns_error() {
        let err = load_ranker("local:nonexistent-reranker").unwrap_err();
        assert!(
            matches!(err, InferenceError::Registry(_)),
            "should be Registry error, got: {err}"
        );
    }

    // --- InferenceEngine rank defaults ---

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn trait_rank_default_returns_not_supported() {
        std::env::set_var("OPENAI_API_KEY", "sk-test-key");
        let engine: Box<dyn InferenceEngine> = load("openai:gpt-4o-mini").unwrap();
        std::env::remove_var("OPENAI_API_KEY");
        let err = engine.rank("query", &["passage"]).unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "rank on generation engine should be NotSupported: {err}"
        );
        assert!(!engine.supports_rank());
    }

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn trait_generation_engine_generate_not_supported_default() {
        // Cloud GenerationEngine via trait should support generate (not return NotSupported)
        // We can't actually call generate without a real API key hitting the server,
        // but we can verify the trait object is constructable
        std::env::set_var("OPENAI_API_KEY", "sk-test-key");
        let engine: Box<dyn InferenceEngine> = load("openai:gpt-4o-mini").unwrap();
        std::env::remove_var("OPENAI_API_KEY");
        // Verify it's a trait object we can hold
        assert!(!engine.supports_embed());
        assert!(engine.supports_generate());
    }
}
