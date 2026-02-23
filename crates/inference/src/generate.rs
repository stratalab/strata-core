//! Generation engine: prompt → text via local llama.cpp or cloud providers.
//!
//! [`GenerationEngine`] provides a unified API for text generation across
//! different backends. Currently only the local llama.cpp backend is implemented;
//! cloud providers (Anthropic, OpenAI, Google) will be added in Epic 7.
//!
//! The engine takes `&mut self` — callers needing thread-safety should wrap
//! in a `Mutex`.

use std::path::Path;

use tracing::info;

use crate::provider::local::LocalProvider;
use crate::{GenerateRequest, GenerateResponse, InferenceError, ProviderKind};

#[cfg(test)]
use crate::StopReason;

/// High-level generation engine with provider dispatch.
///
/// Currently wraps a local llama.cpp provider. In the future, cloud providers
/// will be added as enum variants for provider dispatch.
///
/// # Example
///
/// ```no_run
/// use strata_inference::{GenerationEngine, GenerateRequest};
///
/// let mut engine = GenerationEngine::from_gguf("model.gguf")?;
/// let response = engine.generate(&GenerateRequest {
///     prompt: "Once upon a time".into(),
///     max_tokens: 50,
///     ..Default::default()
/// })?;
/// println!("{}", response.text);
/// # Ok::<(), strata_inference::InferenceError>(())
/// ```
pub struct GenerationEngine {
    provider: Provider,
}

/// Internal provider dispatch.
///
/// Cloud variants will be added in Epic 7.
enum Provider {
    Local(LocalProvider),
}

impl std::fmt::Debug for GenerationEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.provider {
            Provider::Local(p) => f
                .debug_struct("GenerationEngine")
                .field("provider", &"local")
                .field("vocab_size", &p.vocab_size())
                .field("context_size", &p.context_size())
                .finish(),
        }
    }
}

impl GenerationEngine {
    /// Load a generation engine from a GGUF file with default context size.
    ///
    /// The model is loaded with GPU offloading enabled and the context size
    /// capped at min(training_ctx, 4096).
    pub fn from_gguf(path: impl AsRef<Path>) -> Result<Self, InferenceError> {
        Self::from_gguf_with_ctx(path, None)
    }

    /// Load a generation engine from a GGUF file with a custom context size.
    ///
    /// The context size is capped at the model's training context length.
    pub fn from_gguf_with_ctx(
        path: impl AsRef<Path>,
        ctx_size: Option<usize>,
    ) -> Result<Self, InferenceError> {
        let path = path.as_ref();
        info!(path = %path.display(), "Loading generation engine from GGUF");
        let provider = LocalProvider::from_gguf(path, ctx_size)?;
        info!(
            vocab_size = provider.vocab_size(),
            context_size = provider.context_size(),
            "Generation engine ready"
        );
        Ok(Self {
            provider: Provider::Local(provider),
        })
    }

    /// Load a generation engine by model name from the registry.
    ///
    /// Blocked until Epic 5 (registry); returns `NotSupported` for now.
    pub fn from_registry(_name: &str) -> Result<Self, InferenceError> {
        Err(InferenceError::NotSupported(
            "model registry not yet implemented (Epic 5)".to_string(),
        ))
    }

    /// Create a generation engine backed by a cloud provider.
    ///
    /// Blocked until Epic 7 (cloud providers); returns `NotSupported` for now.
    pub fn cloud(
        _provider: ProviderKind,
        _api_key: String,
        _model: String,
    ) -> Result<Self, InferenceError> {
        Err(InferenceError::NotSupported(
            "cloud providers not yet implemented (Epic 7)".to_string(),
        ))
    }

    /// Generate text from a prompt.
    ///
    /// Dispatches to the underlying provider (local llama.cpp or cloud API).
    pub fn generate(
        &mut self,
        request: &GenerateRequest,
    ) -> Result<GenerateResponse, InferenceError> {
        match &mut self.provider {
            Provider::Local(p) => p.generate(request),
        }
    }

    /// Encode text to token IDs using the model's tokenizer.
    ///
    /// Only available for local providers. Cloud providers return `NotSupported`.
    pub fn encode(&self, text: &str) -> Result<Vec<u32>, InferenceError> {
        match &self.provider {
            Provider::Local(p) => Ok(p.encode(text)),
        }
    }

    /// Decode token IDs back to text using the model's tokenizer.
    ///
    /// Only available for local providers. Cloud providers return `NotSupported`.
    pub fn decode(&self, ids: &[u32]) -> Result<String, InferenceError> {
        match &self.provider {
            Provider::Local(p) => Ok(p.decode(ids)),
        }
    }

    /// Returns `true` if this engine uses a local llama.cpp backend.
    pub fn is_local(&self) -> bool {
        matches!(self.provider, Provider::Local(_))
    }

    /// Returns which provider this engine uses.
    pub fn provider(&self) -> ProviderKind {
        match &self.provider {
            Provider::Local(_) => ProviderKind::Local,
        }
    }
}

// Compile-time verify Send (not Sync — &mut self API).
const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_both() {
        assert_send::<GenerationEngine>();
    }
    let _ = assert_both;
};

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Constructor tests (no libllama needed for error paths)
    // -----------------------------------------------------------------------

    #[test]
    fn from_registry_returns_not_supported() {
        let result = GenerationEngine::from_registry("gpt2");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, InferenceError::NotSupported(_)),
            "should be NotSupported, got: {err}"
        );
        assert!(
            err.to_string().contains("registry"),
            "error should mention registry: {err}"
        );
    }

    #[test]
    fn from_registry_with_any_name_returns_not_supported() {
        for name in &["gpt2", "", "   ", "nonexistent-model"] {
            let result = GenerationEngine::from_registry(name);
            assert!(
                matches!(result.unwrap_err(), InferenceError::NotSupported(_)),
                "from_registry({name:?}) should be NotSupported"
            );
        }
    }

    #[test]
    fn cloud_returns_not_supported() {
        let result = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "sk-test".to_string(),
            "claude-3".to_string(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, InferenceError::NotSupported(_)),
            "should be NotSupported, got: {err}"
        );
        assert!(
            err.to_string().contains("cloud"),
            "error should mention cloud: {err}"
        );
    }

    #[test]
    fn cloud_with_all_providers_returns_not_supported() {
        for provider in &[
            ProviderKind::Anthropic,
            ProviderKind::OpenAI,
            ProviderKind::Google,
        ] {
            let result = GenerationEngine::cloud(
                *provider,
                "key".to_string(),
                "model".to_string(),
            );
            assert!(
                matches!(result.unwrap_err(), InferenceError::NotSupported(_)),
                "cloud({provider}) should be NotSupported"
            );
        }
    }

    #[test]
    fn from_gguf_nonexistent_returns_descriptive_error() {
        let result = GenerationEngine::from_gguf("/nonexistent/path/model.gguf");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, InferenceError::LlamaCpp(_)),
            "should be LlamaCpp error, got: {msg}"
        );
        assert!(!msg.is_empty(), "error should not be empty");
    }

    #[test]
    fn from_gguf_with_ctx_nonexistent_returns_descriptive_error() {
        let result =
            GenerationEngine::from_gguf_with_ctx("/nonexistent/model.gguf", Some(2048));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, InferenceError::LlamaCpp(_)),
            "should be LlamaCpp error, got: {msg}"
        );
        assert!(!msg.is_empty(), "error should not be empty");
    }

    #[test]
    fn from_gguf_accepts_path_types() {
        // All fail without libllama, but verify AsRef<Path> works
        let r1 = GenerationEngine::from_gguf("/tmp/model.gguf");
        let r2 = GenerationEngine::from_gguf(String::from("/tmp/model.gguf"));
        let r3 = GenerationEngine::from_gguf(std::path::PathBuf::from("/tmp/model.gguf"));
        assert!(r1.is_err());
        assert!(r2.is_err());
        assert!(r3.is_err());
    }

    // -----------------------------------------------------------------------
    // Compile-time trait assertions
    // -----------------------------------------------------------------------

    #[test]
    fn generation_engine_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<GenerationEngine>();
    }

    #[test]
    fn generation_engine_has_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<GenerationEngine>();
    }

    // -----------------------------------------------------------------------
    // GenerateRequest default sanity
    // -----------------------------------------------------------------------

    #[test]
    fn default_request_has_greedy_temperature() {
        let req = GenerateRequest::default();
        assert_eq!(req.temperature, 0.0, "default should be greedy (temp=0)");
        assert_eq!(req.max_tokens, 256);
        assert!(req.stop_sequences.is_empty());
        assert!(req.stop_tokens.is_empty());
    }

    #[test]
    fn stop_reason_display_exhaustive() {
        // Verify all StopReason variants that generate() can return
        assert_eq!(StopReason::StopToken.to_string(), "stop_token");
        assert_eq!(StopReason::MaxTokens.to_string(), "max_tokens");
        assert_eq!(StopReason::ContextLength.to_string(), "context_length");
        assert_eq!(StopReason::Cancelled.to_string(), "cancelled");
    }

    // -----------------------------------------------------------------------
    // cloud() edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn cloud_with_local_provider_returns_not_supported() {
        // ProviderKind::Local is semantically wrong for cloud(), but the stub
        // rejects all providers uniformly.
        let result = GenerationEngine::cloud(
            ProviderKind::Local,
            "key".to_string(),
            "model".to_string(),
        );
        assert!(
            matches!(result.unwrap_err(), InferenceError::NotSupported(_)),
            "cloud(Local) should be NotSupported"
        );
    }

    #[test]
    fn cloud_with_empty_key_and_model_returns_not_supported() {
        let result = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            String::new(),
            String::new(),
        );
        assert!(
            matches!(result.unwrap_err(), InferenceError::NotSupported(_)),
            "cloud with empty strings should still be NotSupported"
        );
    }

    #[test]
    fn from_gguf_with_ctx_zero_returns_error() {
        // ctx_size=0 should fail (or be handled gracefully)
        let result =
            GenerationEngine::from_gguf_with_ctx("/nonexistent/model.gguf", Some(0));
        assert!(result.is_err());
    }

    #[test]
    fn from_gguf_with_ctx_none_delegates_to_from_gguf() {
        // from_gguf(path) calls from_gguf_with_ctx(path, None); verify both
        // fail the same way for a nonexistent file
        let r1 = GenerationEngine::from_gguf("/nonexistent/model.gguf");
        let r2 = GenerationEngine::from_gguf_with_ctx("/nonexistent/model.gguf", None);
        assert!(r1.is_err());
        assert!(r2.is_err());
        // Both should produce the same error type
        assert!(matches!(r1.unwrap_err(), InferenceError::LlamaCpp(_)));
        assert!(matches!(r2.unwrap_err(), InferenceError::LlamaCpp(_)));
    }

    #[test]
    fn from_registry_error_mentions_epic() {
        let err = GenerationEngine::from_registry("any").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Epic 5") || msg.contains("registry"),
            "error should mention Epic 5 or registry: {msg}"
        );
    }

    #[test]
    fn cloud_error_mentions_epic() {
        let err = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "k".into(),
            "m".into(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Epic 7") || msg.contains("cloud"),
            "error should mention Epic 7 or cloud: {msg}"
        );
    }
}
