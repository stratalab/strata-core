//! Generation engine: prompt → text via local llama.cpp or cloud providers.
//!
//! [`GenerationEngine`] provides a unified API for text generation across
//! different backends: local llama.cpp, Anthropic (Claude), OpenAI (GPT),
//! and Google (Gemini).
//!
//! The engine takes `&mut self` — callers needing thread-safety should wrap
//! in a `Mutex`.

#[cfg(feature = "local")]
use std::path::Path;

use tracing::info;

#[cfg(feature = "local")]
use crate::provider::local::LocalProvider;
#[cfg(feature = "local")]
use crate::wire::ModelConfig;
use crate::wire::{ChatRequest, ChatResponse};
use crate::{GenerateRequest, GenerateResponse, InferenceError, ProviderKind};

#[cfg(test)]
use crate::StopReason;

/// High-level generation engine with provider dispatch.
///
/// Wraps a local llama.cpp provider or a cloud provider (Anthropic, OpenAI,
/// Google) and dispatches `generate()` calls to the underlying backend.
///
/// # Example (local)
///
/// ```no_run
/// # #[cfg(feature = "local")]
/// # fn example() -> Result<(), strata_inference::InferenceError> {
/// use strata_inference::{GenerationEngine, GenerateRequest};
///
/// let mut engine = GenerationEngine::from_gguf("model.gguf")?;
/// let response = engine.generate(&GenerateRequest {
///     prompt: "Once upon a time".into(),
///     max_tokens: 50,
///     ..Default::default()
/// })?;
/// println!("{}", response.text);
/// # Ok(())
/// # }
/// ```
pub struct GenerationEngine {
    provider: Provider,
}

/// Internal provider dispatch enum.
///
/// Each variant is gated behind its feature flag. At least one provider
/// feature must be enabled for `GenerationEngine` to be constructible.
enum Provider {
    #[cfg(feature = "local")]
    Local(LocalProvider),

    #[cfg(feature = "anthropic")]
    Anthropic(crate::provider::anthropic::AnthropicProvider),

    #[cfg(feature = "openai")]
    OpenAI(crate::provider::openai::OpenAIProvider),

    #[cfg(feature = "google")]
    Google(crate::provider::google::GoogleProvider),
}

impl std::fmt::Debug for GenerationEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.provider {
            #[cfg(feature = "local")]
            Provider::Local(p) => f
                .debug_struct("GenerationEngine")
                .field("provider", &"local")
                .field("vocab_size", &p.vocab_size())
                .field("context_size", &p.context_size())
                .finish(),

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(p) => f
                .debug_struct("GenerationEngine")
                .field("provider", &"anthropic")
                .field("model", &p.model())
                .finish(),

            #[cfg(feature = "openai")]
            Provider::OpenAI(p) => f
                .debug_struct("GenerationEngine")
                .field("provider", &"openai")
                .field("model", &p.model())
                .finish(),

            #[cfg(feature = "google")]
            Provider::Google(p) => f
                .debug_struct("GenerationEngine")
                .field("provider", &"google")
                .field("model", &p.model())
                .finish(),
        }
    }
}

impl GenerationEngine {
    /// Load a generation engine from a GGUF file with default context size.
    ///
    /// The model is loaded with GPU offloading enabled and the context size
    /// capped at min(training_ctx, 4096).
    #[cfg(feature = "local")]
    pub fn from_gguf(path: impl AsRef<Path>) -> Result<Self, InferenceError> {
        Self::from_gguf_with_ctx(path, None)
    }

    /// Load a generation engine from a GGUF file with a custom context size.
    ///
    /// The context size is capped at the model's training context length.
    #[cfg(feature = "local")]
    pub fn from_gguf_with_ctx(
        path: impl AsRef<Path>,
        ctx_size: Option<usize>,
    ) -> Result<Self, InferenceError> {
        let config = ctx_size.map(|ctx| ModelConfig {
            n_ctx: Some(u32::try_from(ctx).unwrap_or(u32::MAX)),
            ..ModelConfig::default()
        });
        Self::from_gguf_with_config(path, config.as_ref())
    }

    /// Load a generation engine from a GGUF file with a model/context config
    /// (`n_ctx`, `n_gpu_layers`, `n_batch`, `n_threads`).
    #[cfg(feature = "local")]
    pub fn from_gguf_with_config(
        path: impl AsRef<Path>,
        config: Option<&ModelConfig>,
    ) -> Result<Self, InferenceError> {
        let path = path.as_ref();
        info!(path = %path.display(), "Loading generation engine from GGUF");
        let provider = LocalProvider::from_gguf(path, config)?;
        info!(
            vocab_size = provider.vocab_size(),
            context_size = provider.context_size(),
            "Generation engine ready"
        );
        Ok(Self {
            provider: Provider::Local(provider),
        })
    }

    /// Create a generation engine backed by a cloud provider.
    ///
    /// The `provider` selects which cloud API to use. `api_key` and `model`
    /// are validated (must not be empty). `ProviderKind::Local` is rejected —
    /// use `from_gguf` for local models. Model names are resolved by
    /// [`InferenceRuntime::resolve`](crate::InferenceRuntime::resolve), not
    /// here.
    ///
    /// Returns `NotSupported` if the requested provider's feature flag is not
    /// enabled at compile time.
    pub fn cloud(
        provider: ProviderKind,
        api_key: String,
        model: String,
    ) -> Result<Self, InferenceError> {
        if provider == ProviderKind::Local {
            return Err(InferenceError::Provider(
                "use from_gguf() for local models, not cloud()".to_string(),
            ));
        }

        match provider {
            ProviderKind::Local => unreachable!(),

            ProviderKind::Anthropic => {
                #[cfg(feature = "anthropic")]
                {
                    let p = crate::provider::anthropic::AnthropicProvider::new(api_key, model)?;
                    info!(
                        provider = "anthropic",
                        model = p.model(),
                        "Cloud generation engine ready"
                    );
                    Ok(Self {
                        provider: Provider::Anthropic(p),
                    })
                }
                #[cfg(not(feature = "anthropic"))]
                {
                    let _ = (api_key, model);
                    Err(InferenceError::NotSupported(
                        "Anthropic provider not enabled (compile with --features anthropic)"
                            .to_string(),
                    ))
                }
            }

            ProviderKind::OpenAI => {
                #[cfg(feature = "openai")]
                {
                    let p = crate::provider::openai::OpenAIProvider::new(api_key, model)?;
                    info!(
                        provider = "openai",
                        model = p.model(),
                        "Cloud generation engine ready"
                    );
                    Ok(Self {
                        provider: Provider::OpenAI(p),
                    })
                }
                #[cfg(not(feature = "openai"))]
                {
                    let _ = (api_key, model);
                    Err(InferenceError::NotSupported(
                        "OpenAI provider not enabled (compile with --features openai)".to_string(),
                    ))
                }
            }

            ProviderKind::Google => {
                #[cfg(feature = "google")]
                {
                    let p = crate::provider::google::GoogleProvider::new(api_key, model)?;
                    info!(
                        provider = "google",
                        model = p.model(),
                        "Cloud generation engine ready"
                    );
                    Ok(Self {
                        provider: Provider::Google(p),
                    })
                }
                #[cfg(not(feature = "google"))]
                {
                    let _ = (api_key, model);
                    Err(InferenceError::NotSupported(
                        "Google provider not enabled (compile with --features google)".to_string(),
                    ))
                }
            }
        }
    }

    /// Send a cloud engine's requests to `base_url` instead of the provider's
    /// public endpoint (#3270). The runtime applies the effective base URL
    /// from its [`ProviderSettings`](crate::ProviderSettings) here; a local
    /// engine has nowhere to send a request and is unchanged.
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    pub(crate) fn with_base_url(self, base_url: &str) -> Self {
        let provider = match self.provider {
            #[cfg(feature = "local")]
            Provider::Local(p) => Provider::Local(p),

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(p) => Provider::Anthropic(p.with_base_url(base_url)),

            #[cfg(feature = "openai")]
            Provider::OpenAI(p) => Provider::OpenAI(p.with_base_url(base_url)),

            #[cfg(feature = "google")]
            Provider::Google(p) => Provider::Google(p.with_base_url(base_url)),
        };
        Self { provider }
    }

    /// Generate text from a prompt.
    ///
    /// Dispatches to the underlying provider (local llama.cpp or cloud API).
    pub fn generate(
        &mut self,
        request: &GenerateRequest,
    ) -> Result<GenerateResponse, InferenceError> {
        match &mut self.provider {
            #[cfg(feature = "local")]
            Provider::Local(p) => p.generate(request),

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(p) => p.generate(request),

            #[cfg(feature = "openai")]
            Provider::OpenAI(p) => p.generate(request),

            #[cfg(feature = "google")]
            Provider::Google(p) => p.generate(request),
        }
    }

    /// Generate from an OpenAI-shaped chat request.
    ///
    /// Local applies the model's chat template and the full sampler chain;
    /// cloud providers map messages natively (system prompt, multi-turn history,
    /// assistant prefill) and forward every knob the provider supports.
    pub fn generate_chat(&mut self, request: &ChatRequest) -> Result<ChatResponse, InferenceError> {
        match &mut self.provider {
            #[cfg(feature = "local")]
            Provider::Local(p) => p.generate_chat(request),

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(p) => p.generate_chat(request),

            #[cfg(feature = "openai")]
            Provider::OpenAI(p) => p.generate_chat(request),

            #[cfg(feature = "google")]
            Provider::Google(p) => p.generate_chat(request),
        }
    }

    /// Encode text to token IDs using the model's tokenizer.
    ///
    /// When `add_special` is true, BOS/EOS tokens are added per the model's
    /// tokenizer configuration. Only available for local providers; cloud
    /// providers return `NotSupported`.
    pub fn encode(&self, text: &str, add_special: bool) -> Result<Vec<u32>, InferenceError> {
        match &self.provider {
            #[cfg(feature = "local")]
            Provider::Local(p) => Ok(p.encode(text, add_special)),

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(_) => {
                let _ = (text, add_special);
                Err(InferenceError::NotSupported(
                    "tokenization not available for Anthropic cloud provider".to_string(),
                ))
            }

            #[cfg(feature = "openai")]
            Provider::OpenAI(_) => {
                let _ = (text, add_special);
                Err(InferenceError::NotSupported(
                    "tokenization not available for OpenAI cloud provider".to_string(),
                ))
            }

            #[cfg(feature = "google")]
            Provider::Google(_) => {
                let _ = (text, add_special);
                Err(InferenceError::NotSupported(
                    "tokenization not available for Google cloud provider".to_string(),
                ))
            }
        }
    }

    /// Decode token IDs back to text using the model's tokenizer.
    ///
    /// Only available for local providers. Cloud providers return `NotSupported`.
    pub fn decode(&self, ids: &[u32]) -> Result<String, InferenceError> {
        match &self.provider {
            #[cfg(feature = "local")]
            Provider::Local(p) => Ok(p.decode(ids)),

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(_) => {
                let _ = ids;
                Err(InferenceError::NotSupported(
                    "detokenization not available for Anthropic cloud provider".to_string(),
                ))
            }

            #[cfg(feature = "openai")]
            Provider::OpenAI(_) => {
                let _ = ids;
                Err(InferenceError::NotSupported(
                    "detokenization not available for OpenAI cloud provider".to_string(),
                ))
            }

            #[cfg(feature = "google")]
            Provider::Google(_) => {
                let _ = ids;
                Err(InferenceError::NotSupported(
                    "detokenization not available for Google cloud provider".to_string(),
                ))
            }
        }
    }

    /// Returns `true` if this engine uses a local llama.cpp backend.
    pub fn is_local(&self) -> bool {
        match &self.provider {
            #[cfg(feature = "local")]
            Provider::Local(_) => true,

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(_) => false,

            #[cfg(feature = "openai")]
            Provider::OpenAI(_) => false,

            #[cfg(feature = "google")]
            Provider::Google(_) => false,
        }
    }

    /// Returns which provider this engine uses.
    pub fn provider(&self) -> ProviderKind {
        match &self.provider {
            #[cfg(feature = "local")]
            Provider::Local(_) => ProviderKind::Local,

            #[cfg(feature = "anthropic")]
            Provider::Anthropic(_) => ProviderKind::Anthropic,

            #[cfg(feature = "openai")]
            Provider::OpenAI(_) => ProviderKind::OpenAI,

            #[cfg(feature = "google")]
            Provider::Google(_) => ProviderKind::Google,
        }
    }
}

impl crate::InferenceEngine for GenerationEngine {
    fn generate(&mut self, request: &GenerateRequest) -> Result<GenerateResponse, InferenceError> {
        GenerationEngine::generate(self, request)
    }

    fn supports_generate(&self) -> bool {
        true
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

    #[cfg(feature = "local")]
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

    #[cfg(feature = "local")]
    #[test]
    fn from_gguf_with_ctx_nonexistent_returns_descriptive_error() {
        let result = GenerationEngine::from_gguf_with_ctx("/nonexistent/model.gguf", Some(2048));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, InferenceError::LlamaCpp(_)),
            "should be LlamaCpp error, got: {msg}"
        );
        assert!(!msg.is_empty(), "error should not be empty");
    }

    #[cfg(feature = "local")]
    #[test]
    fn from_gguf_accepts_path_types() {
        let r1 = GenerationEngine::from_gguf("/tmp/model.gguf");
        let r2 = GenerationEngine::from_gguf(String::from("/tmp/model.gguf"));
        let r3 = GenerationEngine::from_gguf(std::path::PathBuf::from("/tmp/model.gguf"));
        assert!(r1.is_err());
        assert!(r2.is_err());
        assert!(r3.is_err());
    }

    #[cfg(feature = "local")]
    #[test]
    fn from_gguf_with_ctx_zero_returns_error() {
        let result = GenerationEngine::from_gguf_with_ctx("/nonexistent/model.gguf", Some(0));
        assert!(result.is_err());
    }

    #[cfg(feature = "local")]
    #[test]
    fn from_gguf_with_ctx_none_delegates_to_from_gguf() {
        let r1 = GenerationEngine::from_gguf("/nonexistent/model.gguf");
        let r2 = GenerationEngine::from_gguf_with_ctx("/nonexistent/model.gguf", None);
        assert!(r1.is_err());
        assert!(r2.is_err());
        assert!(matches!(r1.unwrap_err(), InferenceError::LlamaCpp(_)));
        assert!(matches!(r2.unwrap_err(), InferenceError::LlamaCpp(_)));
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
        assert_eq!(StopReason::StopToken.to_string(), "stop_token");
        assert_eq!(StopReason::MaxTokens.to_string(), "max_tokens");
        assert_eq!(StopReason::ContextLength.to_string(), "context_length");
        assert_eq!(StopReason::Cancelled.to_string(), "cancelled");
    }

    // -----------------------------------------------------------------------
    // cloud() constructor tests
    // -----------------------------------------------------------------------

    #[test]
    fn cloud_local_provider_returns_provider_error() {
        let result =
            GenerationEngine::cloud(ProviderKind::Local, "key".to_string(), "model".to_string());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, InferenceError::Provider(_)),
            "cloud(Local) should be Provider error, got: {err}"
        );
        assert!(
            err.to_string().contains("from_gguf"),
            "error should suggest from_gguf: {err}"
        );
    }

    #[test]
    fn cloud_local_provider_with_empty_key_still_rejects_local() {
        // The Local check happens before key validation
        let result = GenerationEngine::cloud(ProviderKind::Local, String::new(), String::new());
        assert!(matches!(result.unwrap_err(), InferenceError::Provider(_)));
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn cloud_anthropic_constructs_successfully() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "sk-ant-test-key".to_string(),
            "claude-sonnet-4-20250514".to_string(),
        );
        assert!(engine.is_ok());
        let engine = engine.unwrap();
        assert!(!engine.is_local());
        assert_eq!(engine.provider(), ProviderKind::Anthropic);
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn cloud_anthropic_empty_key_returns_error() {
        let result = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            String::new(),
            "claude-sonnet-4-20250514".to_string(),
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Provider(_)));
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn cloud_anthropic_empty_model_returns_error() {
        let result = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "sk-test".to_string(),
            String::new(),
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Provider(_)));
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn cloud_anthropic_encode_returns_not_supported() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "sk-test".to_string(),
            "model".to_string(),
        )
        .unwrap();
        let result = engine.encode("hello", false);
        assert!(matches!(
            result.unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn cloud_anthropic_decode_returns_not_supported() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "sk-test".to_string(),
            "model".to_string(),
        )
        .unwrap();
        let result = engine.decode(&[1, 2, 3]);
        assert!(matches!(
            result.unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn cloud_anthropic_debug_contains_model() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "sk-test".to_string(),
            "claude-sonnet-4-20250514".to_string(),
        )
        .unwrap();
        let dbg = format!("{:?}", engine);
        assert!(dbg.contains("anthropic"), "debug: {dbg}");
        assert!(dbg.contains("claude-sonnet-4-20250514"), "debug: {dbg}");
    }

    #[cfg(feature = "openai")]
    #[test]
    fn cloud_openai_constructs_successfully() {
        let engine = GenerationEngine::cloud(
            ProviderKind::OpenAI,
            "sk-test-key".to_string(),
            "gpt-4".to_string(),
        );
        assert!(engine.is_ok());
        let engine = engine.unwrap();
        assert!(!engine.is_local());
        assert_eq!(engine.provider(), ProviderKind::OpenAI);
    }

    #[cfg(feature = "openai")]
    #[test]
    fn cloud_openai_empty_key_returns_error() {
        let result =
            GenerationEngine::cloud(ProviderKind::OpenAI, String::new(), "gpt-4".to_string());
        assert!(result.is_err());
    }

    #[cfg(feature = "openai")]
    #[test]
    fn cloud_openai_encode_returns_not_supported() {
        let engine = GenerationEngine::cloud(
            ProviderKind::OpenAI,
            "sk-test".to_string(),
            "gpt-4".to_string(),
        )
        .unwrap();
        assert!(matches!(
            engine.encode("hi", true).unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(feature = "openai")]
    #[test]
    fn cloud_openai_decode_returns_not_supported() {
        let engine = GenerationEngine::cloud(
            ProviderKind::OpenAI,
            "sk-test".to_string(),
            "gpt-4".to_string(),
        )
        .unwrap();
        assert!(matches!(
            engine.decode(&[1]).unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(feature = "google")]
    #[test]
    fn cloud_google_constructs_successfully() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Google,
            "AIza-test".to_string(),
            "gemini-pro".to_string(),
        );
        assert!(engine.is_ok());
        let engine = engine.unwrap();
        assert!(!engine.is_local());
        assert_eq!(engine.provider(), ProviderKind::Google);
    }

    #[cfg(feature = "google")]
    #[test]
    fn cloud_google_empty_key_returns_error() {
        let result = GenerationEngine::cloud(
            ProviderKind::Google,
            String::new(),
            "gemini-pro".to_string(),
        );
        assert!(result.is_err());
    }

    #[cfg(feature = "google")]
    #[test]
    fn cloud_google_encode_returns_not_supported() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Google,
            "key".to_string(),
            "gemini-pro".to_string(),
        )
        .unwrap();
        assert!(matches!(
            engine.encode("hi", false).unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(feature = "google")]
    #[test]
    fn cloud_google_decode_returns_not_supported() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Google,
            "key".to_string(),
            "gemini-pro".to_string(),
        )
        .unwrap();
        assert!(matches!(
            engine.decode(&[1]).unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    // -----------------------------------------------------------------------
    // cloud() with disabled features returns NotSupported
    // -----------------------------------------------------------------------

    #[cfg(not(feature = "anthropic"))]
    #[test]
    fn cloud_anthropic_disabled_returns_not_supported() {
        let result = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "key".to_string(),
            "model".to_string(),
        );
        assert!(matches!(
            result.unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(not(feature = "openai"))]
    #[test]
    fn cloud_openai_disabled_returns_not_supported() {
        let result =
            GenerationEngine::cloud(ProviderKind::OpenAI, "key".to_string(), "model".to_string());
        assert!(matches!(
            result.unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(not(feature = "google"))]
    #[test]
    fn cloud_google_disabled_returns_not_supported() {
        let result =
            GenerationEngine::cloud(ProviderKind::Google, "key".to_string(), "model".to_string());
        assert!(matches!(
            result.unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    // -----------------------------------------------------------------------
    // Debug output: model visible, API key never leaked
    // -----------------------------------------------------------------------

    #[cfg(feature = "openai")]
    #[test]
    fn cloud_openai_debug_contains_model_not_key() {
        let engine = GenerationEngine::cloud(
            ProviderKind::OpenAI,
            "sk-secret-key-do-not-leak".to_string(),
            "gpt-4-turbo".to_string(),
        )
        .unwrap();
        let dbg = format!("{:?}", engine);
        assert!(dbg.contains("openai"), "debug: {dbg}");
        assert!(dbg.contains("gpt-4-turbo"), "debug: {dbg}");
        assert!(
            !dbg.contains("sk-secret-key-do-not-leak"),
            "API key leaked in Debug: {dbg}"
        );
    }

    #[cfg(feature = "google")]
    #[test]
    fn cloud_google_debug_contains_model_not_key() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Google,
            "AIza-secret-key-do-not-leak".to_string(),
            "gemini-1.5-pro".to_string(),
        )
        .unwrap();
        let dbg = format!("{:?}", engine);
        assert!(dbg.contains("google"), "debug: {dbg}");
        assert!(dbg.contains("gemini-1.5-pro"), "debug: {dbg}");
        assert!(
            !dbg.contains("AIza-secret-key-do-not-leak"),
            "API key leaked in Debug: {dbg}"
        );
    }

    #[cfg(feature = "anthropic")]
    #[test]
    fn cloud_anthropic_debug_does_not_leak_key() {
        let engine = GenerationEngine::cloud(
            ProviderKind::Anthropic,
            "sk-ant-secret-key-do-not-leak".to_string(),
            "claude-sonnet-4-20250514".to_string(),
        )
        .unwrap();
        let dbg = format!("{:?}", engine);
        assert!(
            !dbg.contains("sk-ant-secret-key-do-not-leak"),
            "API key leaked in Debug: {dbg}"
        );
    }
}
