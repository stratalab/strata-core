//! Public inference runtime and provider API.

#![cfg_attr(all(not(feature = "local"), not(test)), deny(unsafe_code))]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::case_sensitive_file_extension_comparisons,
    clippy::doc_link_with_quotes,
    clippy::doc_markdown,
    clippy::float_cmp,
    clippy::if_same_then_else,
    clippy::manual_let_else,
    clippy::manual_string_new,
    clippy::match_same_arms,
    clippy::needless_raw_string_hashes,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::single_match_else,
    clippy::struct_excessive_bools,
    clippy::uninlined_format_args,
    clippy::unused_self,
    clippy::used_underscore_binding
)]

pub mod api;
mod error;
// Compiled unconditionally so its unit tests run without the `local` feature;
// only `provider/local.rs` consumes it, so it is dead code when `local` is off.
#[cfg_attr(not(feature = "local"), allow(dead_code))]
mod grammar;
pub mod registry;
mod resolve;
pub mod runtime;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
// Compiled unconditionally alongside `grammar` (same rationale); only
// `provider/local.rs` consumes it, so it is dead code when `local` is off.
#[cfg_attr(not(feature = "local"), allow(dead_code))]
mod tool_grammar;
pub mod wire;

#[cfg(feature = "local")]
mod llama;

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

pub use error::{InferenceError, ProviderFailure, RegistryFailure, UnsupportedKind};
pub use registry::{ModelInfo, ModelRegistry, ModelTask};
pub use resolve::{
    Availability, AvailabilityDetails, AvailabilityKind, ModelSource, ModelUse, ResolvedModel,
};
pub use runtime::{
    EmbedRequest, EmbedResponse, EmbedRuntimeOutcome, InferenceCapability, InferenceRuntime,
    InferenceRuntimeConfig, InferenceStatus, ModelCacheStatus, ProviderStatus, PullModelOutput,
    RankRequest, RankResponse, RankRuntimeOutcome, LOCAL_UNAVAILABLE_REMEDY,
};
pub use wire::{
    ChatChoice, ChatMessage, ChatRequest, ChatResponse, EmbedInput, EmbeddingItem,
    EmbeddingsRequest, EmbeddingsResponse, FinishReason, FunctionDef, InputType, JsonSchemaSpec,
    LogProbs, Mirostat, ModelConfig, NamedToolChoice, Pooling, RerankRequest, RerankResponse,
    RerankResult, ResponseFormat, Role, TokenLogProb, Tool, ToolCall, ToolCallFunction, ToolChoice,
    ToolChoiceFunction, ToolChoiceMode, TopLogProb, Usage,
};

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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct GenerateRequest {
    /// Input prompt text.
    pub prompt: String,
    /// Maximum number of completion tokens to produce.
    pub max_tokens: usize,
    /// Sampling temperature.
    pub temperature: f32,
    /// Top-k sampling cutoff for providers that support it.
    pub top_k: usize,
    /// Nucleus sampling cutoff for providers that support it.
    pub top_p: f32,
    /// Optional deterministic sampling seed for providers that support it.
    pub seed: Option<u64>,
    /// String stop sequences.
    pub stop_sequences: Vec<String>,
    /// Token-id stop sequences for local providers.
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct GenerateResponse {
    /// Generated output text.
    pub text: String,
    /// Why generation stopped.
    pub stop_reason: StopReason,
    /// Provider-reported prompt token count.
    pub prompt_tokens: usize,
    /// Provider-reported completion token count.
    pub completion_tokens: usize,
}

/// Why generation stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// Generation reached a stop token or stop sequence.
    StopToken,
    /// Generation reached the configured maximum token count.
    MaxTokens,
    /// Generation reached the model context limit.
    ContextLength,
    /// Generation was cancelled by provider policy or caller control.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Local GGUF model provider.
    Local,
    /// Anthropic cloud provider.
    Anthropic,
    /// OpenAI cloud provider.
    #[serde(rename = "openai")]
    OpenAI,
    /// Google Gemini cloud provider.
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
            _ => Err(InferenceError::InvalidSpec(format!(
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

/// The runtime-level inference surface the executor depends on.
///
/// Unlike [`InferenceEngine`] (a single model's compute: generate/embed/rank),
/// this mirrors [`InferenceRuntime`]'s public API — model management plus the
/// wire-typed compute paths — so the executor can hold `Box<dyn InferenceService>`
/// and be driven by either the real runtime or a deterministic testkit fake.
pub trait InferenceService: Send {
    /// Lists every catalog model.
    fn list_models(&self) -> Vec<ModelInfo>;

    /// Lists the models present locally.
    fn list_local_models(&self) -> Vec<ModelInfo>;

    /// Resolves (downloading if needed) a model artifact.
    fn pull_model(&self, model: &str) -> Result<PullModelOutput, InferenceError>;

    /// Reports the capabilities of a model spec.
    fn capability(&self, model_spec: &str) -> Result<InferenceCapability, InferenceError>;

    /// Runs a chat generation request.
    fn chat(&self, model_spec: &str, request: &ChatRequest)
        -> Result<ChatResponse, InferenceError>;

    /// Tokenizes text with a local model's tokenizer.
    fn tokenize(
        &self,
        model_spec: &str,
        text: &str,
        add_special: bool,
    ) -> Result<Vec<u32>, InferenceError>;

    /// Detokenizes local token ids.
    fn detokenize(&self, model_spec: &str, ids: &[u32]) -> Result<String, InferenceError>;

    /// Runs an embeddings request.
    fn embeddings(
        &self,
        model_spec: &str,
        request: &EmbeddingsRequest,
    ) -> Result<EmbeddingsResponse, InferenceError>;

    /// Scores passages against a query.
    fn rank(&self, model_spec: &str, request: &RankRequest)
        -> Result<RankResponse, InferenceError>;

    /// Unloads a cached model (or all when `None`).
    fn unload(&self, model_spec: Option<&str>) -> Result<bool, InferenceError>;

    /// Reports the model cache status.
    fn cache_status(&self) -> Result<ModelCacheStatus, InferenceError>;

    /// Reports what this binary can do before anything is attempted.
    fn status(&self) -> InferenceStatus;
}

/// Parse a `"provider:model_name"` spec into its components.
///
/// Format: `"provider:model_name"` where provider is one of: local, anthropic, openai, google.
/// Only the first colon separates provider from model — the rest is part of the model
/// name (e.g., `"local:qwen3:1.7b:q8_0"`).
///
/// A spec whose first segment is not a provider name is a local model name in full:
/// a bare name (`"miniLM"`), a catalog `family:size` or `name:quant` form
/// (`"qwen3:1.7b"`, `"tinyllama:q8_0"`), or a GGUF path. The provider set is a closed
/// enum, so nothing else the segment could be is a provider; whether the name exists
/// is the registry's decision (#3222).
pub fn parse_model_spec(spec: &str) -> Result<(ProviderKind, String), InferenceError> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(InferenceError::InvalidSpec(
            "model spec is empty".to_string(),
        ));
    }

    // Split on first colon only
    let (provider, model) = match spec.split_once(':') {
        Some((provider_str, model)) => match provider_str.parse::<ProviderKind>() {
            Ok(provider) => (provider, model.trim()),
            // Not a provider name, so the colon is part of a local model name.
            Err(_) => (ProviderKind::Local, spec),
        },
        None => (ProviderKind::Local, spec),
    };

    if model.is_empty() {
        return Err(InferenceError::InvalidSpec(format!(
            "model name is empty in spec {:?}",
            spec
        )));
    }

    Ok((provider, model.to_string()))
}

/// Environment variable name for a provider's API key.
///
/// Not gated on the cloud features: `status` reports every provider, including
/// the ones this build cannot call, and naming the variable is exactly how it
/// says what a caller would need to set. A pure name mapping with no
/// dependencies, so there is nothing to gate.
pub(crate) fn api_key_env_var(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Anthropic => "ANTHROPIC_API_KEY",
        ProviderKind::OpenAI => "OPENAI_API_KEY",
        ProviderKind::Google => "GOOGLE_API_KEY",
        ProviderKind::Local => "STRATA_LOCAL_API_KEY", // unused, but complete
    }
}

/// Public metadata for a cloud provider's API key: the canonical provider name,
/// the environment variable Strata reads, and where a user acquires a key.
///
/// Strata is embedded and ships no keys — callers bring their own. This is the
/// one source of truth the CLI's `config` surface and the missing-key error
/// both draw on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderKeyInfo {
    /// Canonical provider name (`openai` / `anthropic` / `google`).
    pub provider: &'static str,
    /// Environment variable Strata reads for this provider's key.
    pub env_var: &'static str,
    /// URL where a user acquires an API key for this provider.
    pub acquisition_url: &'static str,
}

/// Key metadata for every cloud provider, in a stable order.
pub const CLOUD_PROVIDER_KEYS: &[ProviderKeyInfo] = &[
    ProviderKeyInfo {
        provider: "openai",
        env_var: "OPENAI_API_KEY",
        acquisition_url: "https://platform.openai.com/api-keys",
    },
    ProviderKeyInfo {
        provider: "anthropic",
        env_var: "ANTHROPIC_API_KEY",
        acquisition_url: "https://console.anthropic.com/settings/keys",
    },
    ProviderKeyInfo {
        provider: "google",
        env_var: "GOOGLE_API_KEY",
        acquisition_url: "https://aistudio.google.com/apikey",
    },
];

/// Look up a cloud provider's key metadata by canonical name (case-insensitive).
/// Returns `None` for unknown or non-cloud providers.
#[must_use]
pub fn provider_key_info(provider: &str) -> Option<&'static ProviderKeyInfo> {
    let provider = provider.trim();
    CLOUD_PROVIDER_KEYS
        .iter()
        .find(|info| info.provider.eq_ignore_ascii_case(provider))
}

pub(crate) fn generation_provider_feature_enabled(provider: ProviderKind) -> bool {
    match provider {
        ProviderKind::Local => cfg!(feature = "local"),
        ProviderKind::Anthropic => cfg!(feature = "anthropic"),
        ProviderKind::OpenAI => cfg!(feature = "openai"),
        ProviderKind::Google => cfg!(feature = "google"),
    }
}

#[cfg(any(feature = "openai", feature = "google"))]
pub(crate) fn embedding_provider_feature_enabled(provider: ProviderKind) -> bool {
    match provider {
        ProviderKind::Local => cfg!(feature = "local"),
        ProviderKind::OpenAI => cfg!(feature = "openai"),
        ProviderKind::Google => cfg!(feature = "google"),
        ProviderKind::Anthropic => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    use std::ffi::OsString;
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    use std::sync::Mutex;

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Shared with the other test modules in this binary: every test that
    /// touches a key variable must serialize on the same `ENV_LOCK`, or one
    /// module's unset races another's read. Setting a key is deliberately
    /// not offered: an engine that needs one takes it as an argument.
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    pub(crate) fn with_env_unset<T>(env_var: &str, test: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let previous = std::env::var_os(env_var);
        std::env::remove_var(env_var);
        let result = test();
        restore_env(env_var, previous);
        result
    }

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn restore_env(env_var: &str, previous: Option<OsString>) {
        if let Some(previous) = previous {
            std::env::set_var(env_var, previous);
        } else {
            std::env::remove_var(env_var);
        }
    }

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

    /// The provider set is a closed enum, so a first segment that is not one
    /// of its four names cannot be a provider — it is the start of a local
    /// model name. Catalog names are themselves colon-shaped (`family:size`,
    /// `name:quant`) and `models list` prints them; the parser used to refuse
    /// exactly those as "unknown provider" (#3222). Whether the name exists is
    /// the registry's decision, not the parser's: a local spec may also be a
    /// file path, which no catalog lookup could vouch for.
    #[test]
    fn test_model_spec_parse_unknown_prefix_is_a_local_model_name() {
        for spec in [
            "qwen3:1.7b",
            "tinyllama:q8_0",
            "qwen3:1.7b:q8_0",
            // A trailing empty part is the registry's to ignore, as it does.
            "qwen3:",
            // A typo'd provider is an unknown local name; the registry says so.
            "azure:gpt-4",
            // A drive letter is not a provider either.
            r"C:\models\tiny.gguf",
            ":model",
        ] {
            let (provider, model) = parse_model_spec(spec).expect(spec);
            assert_eq!(provider, ProviderKind::Local, "{spec}");
            assert_eq!(model, spec, "{spec}: the whole spec is the model name");
        }
    }

    /// Direction control for the rule above: a first segment that IS a
    /// provider name still selects that provider (in any casing) and still
    /// needs a model after the colon — `local:` is malformed where `qwen3:`
    /// is a name.
    #[test]
    fn test_model_spec_parse_provider_prefix_still_selects_the_provider() {
        for (spec, expected_provider, expected_model) in [
            ("local:qwen3:1.7b", ProviderKind::Local, "qwen3:1.7b"),
            (
                "LOCAL:tinyllama:q8_0",
                ProviderKind::Local,
                "tinyllama:q8_0",
            ),
            ("openai:gpt-4o-mini", ProviderKind::OpenAI, "gpt-4o-mini"),
            // Whitespace after the provider's colon is not part of the model name.
            ("openai: gpt-4o-mini", ProviderKind::OpenAI, "gpt-4o-mini"),
            (
                "Anthropic:claude-sonnet-4-6",
                ProviderKind::Anthropic,
                "claude-sonnet-4-6",
            ),
            (
                "google:models/gemini-2.5-flash",
                ProviderKind::Google,
                "models/gemini-2.5-flash",
            ),
        ] {
            let (provider, model) = parse_model_spec(spec).expect(spec);
            assert_eq!(provider, expected_provider, "{spec}");
            assert_eq!(model, expected_model, "{spec}");
        }
        for spec in ["local:", "anthropic:", "openai:   "] {
            let error = parse_model_spec(spec).expect_err(spec);
            assert_eq!(error.code(), "inference.invalid_request", "{spec}");
        }
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

    // --- InferenceEngine trait defaults, over a cloud generation engine ---

    #[cfg(feature = "openai")]
    const ENABLED_PROVIDER: (ProviderKind, &str) = (ProviderKind::OpenAI, "gpt-4o-mini");

    #[cfg(all(not(feature = "openai"), feature = "anthropic"))]
    const ENABLED_PROVIDER: (ProviderKind, &str) = (ProviderKind::Anthropic, "claude-sonnet-4-6");

    #[cfg(all(
        not(feature = "openai"),
        not(feature = "anthropic"),
        feature = "google"
    ))]
    const ENABLED_PROVIDER: (ProviderKind, &str) = (ProviderKind::Google, "gemini-2.5-flash");

    /// A cloud engine built with a fake key. Nothing here sends a request.
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn cloud_generation_engine() -> Box<dyn InferenceEngine> {
        let (provider, model) = ENABLED_PROVIDER;
        Box::new(
            GenerationEngine::cloud(provider, "test-fake-key-12345".to_owned(), model.to_owned())
                .expect("a cloud engine constructs without contacting the provider"),
        )
    }

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn trait_embed_default_returns_not_supported() {
        let engine = cloud_generation_engine();
        let err = engine.embed("test").unwrap_err();
        assert_eq!(err.code(), "inference.unsupported_operation", "{err}");
        assert!(!engine.supports_embed());
    }

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn trait_rank_default_returns_not_supported() {
        let engine = cloud_generation_engine();
        let err = engine.rank("query", &["passage"]).unwrap_err();
        assert_eq!(err.code(), "inference.unsupported_operation", "{err}");
        assert!(!engine.supports_rank());
    }

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    #[test]
    fn a_cloud_generation_engine_is_a_generator() {
        let engine = cloud_generation_engine();
        assert!(engine.supports_generate());
        assert!(!engine.supports_embed());
    }

    #[cfg(feature = "openai")]
    #[test]
    fn a_cloud_embedding_engine_embeds_and_does_not_generate() {
        let engine: Box<dyn InferenceEngine> = Box::new(
            CloudEmbeddingEngine::new(
                ProviderKind::OpenAI,
                "test-fake-key".to_owned(),
                "text-embedding-3-small".to_owned(),
            )
            .expect("constructs without contacting the provider"),
        );
        assert!(engine.supports_embed());
        assert!(!engine.supports_generate());
    }
}
