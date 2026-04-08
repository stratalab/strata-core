//! Embedding and inference infrastructure for Strata.
//!
//! This crate is a thin adapter bridging `strata-inference` (the GGUF inference
//! engine) with `strata-core`/`strata-engine`. All inference features — embedding,
//! model management — are exposed through the executor and CLI, not through
//! `strata-inference`'s own CLI binaries.

#[cfg(feature = "embed")]
pub mod embed;
#[cfg(feature = "embed")]
pub mod expand;
#[cfg(feature = "embed")]
pub mod expand_cache;
#[cfg(feature = "embed")]
pub mod generate;
#[cfg(feature = "embed")]
pub mod rag;
#[cfg(feature = "embed")]
pub mod rerank;

// Re-export key strata-inference types so that the executor depends only on
// strata-intelligence, not directly on strata-inference.
#[cfg(feature = "embed")]
pub use strata_inference::load_ranker;
#[cfg(feature = "embed")]
pub use strata_inference::EmbeddingEngine;
#[cfg(feature = "embed")]
pub use strata_inference::GenerationEngine;
#[cfg(feature = "embed")]
pub use strata_inference::InferenceEngine;
#[cfg(feature = "embed")]
pub use strata_inference::InferenceError;
#[cfg(feature = "embed")]
pub use strata_inference::ModelRegistry;
#[cfg(feature = "embed")]
pub use strata_inference::ModelTask;
#[cfg(feature = "embed")]
pub use strata_inference::ProviderKind;
#[cfg(feature = "embed")]
pub use strata_inference::{GenerateRequest, GenerateResponse, StopReason};

// ---------------------------------------------------------------------------
// Re-export validation tests — ensure the wiring from strata-intelligence
// through to strata-inference works correctly. These catch Cargo.toml
// feature/path misconfigurations that would otherwise only surface at the
// executor layer.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "embed")]
mod reexport_tests {
    use super::*;

    #[test]
    fn generate_request_constructible_via_reexport() {
        let req = GenerateRequest {
            prompt: "hello".into(),
            max_tokens: 10,
            temperature: 0.5,
            top_k: 40,
            top_p: 0.9,
            seed: Some(42),
            stop_sequences: vec!["STOP".into()],
            stop_tokens: vec![1, 2],
            grammar: None,
        };
        assert_eq!(req.prompt, "hello");
        assert_eq!(req.max_tokens, 10);
    }

    #[test]
    fn generate_request_default_via_reexport() {
        let req = GenerateRequest::default();
        assert_eq!(req.max_tokens, 256);
        assert_eq!(req.temperature, 0.0);
        assert_eq!(req.top_k, 0);
        assert_eq!(req.top_p, 1.0);
        assert!(req.seed.is_none());
        assert!(req.prompt.is_empty());
        assert!(req.stop_sequences.is_empty());
        assert!(req.stop_tokens.is_empty());
    }

    #[test]
    fn generate_request_struct_update_syntax() {
        // This is the pattern the executor uses
        let req = GenerateRequest {
            prompt: "test".into(),
            max_tokens: 10,
            ..Default::default()
        };
        assert_eq!(req.temperature, 0.0);
        assert!(req.stop_tokens.is_empty());
    }

    #[test]
    fn generate_response_constructible_via_reexport() {
        let resp = GenerateResponse {
            text: "output".into(),
            stop_reason: StopReason::MaxTokens,
            prompt_tokens: 5,
            completion_tokens: 10,
        };
        assert_eq!(resp.text, "output");
        assert_eq!(resp.stop_reason, StopReason::MaxTokens);
        assert_eq!(resp.prompt_tokens, 5);
        assert_eq!(resp.completion_tokens, 10);
    }

    #[test]
    fn stop_reason_display_via_reexport() {
        assert_eq!(StopReason::StopToken.to_string(), "stop_token");
        assert_eq!(StopReason::MaxTokens.to_string(), "max_tokens");
        assert_eq!(StopReason::ContextLength.to_string(), "context_length");
        assert_eq!(StopReason::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn stop_reason_equality_via_reexport() {
        assert_eq!(StopReason::StopToken, StopReason::StopToken);
        assert_ne!(StopReason::StopToken, StopReason::MaxTokens);
    }

    #[test]
    fn inference_error_variants_accessible() {
        let e = InferenceError::NotSupported("test".into());
        assert!(e.to_string().contains("test"));
        assert!(matches!(e, InferenceError::NotSupported(_)));

        let e2 = InferenceError::Registry("bad model".into());
        assert!(e2.to_string().contains("bad model"));
    }

    #[test]
    fn model_registry_constructible() {
        let reg = ModelRegistry::new();
        // Should not panic; models_dir should be a valid path
        let dir = reg.models_dir();
        assert!(!dir.as_os_str().is_empty());
    }

    #[test]
    fn generation_engine_from_registry_error_accessible() {
        // Verify GenerationEngine is re-exported and error types work
        let result = GenerationEngine::from_registry("nonexistent-reexport-test");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::Registry(_) | InferenceError::LlamaCpp(_)
        ));
    }

    #[test]
    fn embedding_engine_from_registry_error_accessible() {
        let result = EmbeddingEngine::from_registry("nonexistent-reexport-test");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::Registry(_) | InferenceError::LlamaCpp(_)
        ));
    }

    #[test]
    fn provider_kind_reexported() {
        assert_eq!(ProviderKind::Local.to_string(), "local");
        assert_eq!(ProviderKind::Anthropic.to_string(), "anthropic");
        assert_eq!(ProviderKind::OpenAI.to_string(), "openai");
        assert_eq!(ProviderKind::Google.to_string(), "google");
    }

    #[test]
    fn provider_kind_from_str_via_reexport() {
        assert_eq!(
            "local".parse::<ProviderKind>().unwrap(),
            ProviderKind::Local
        );
        assert_eq!(
            "anthropic".parse::<ProviderKind>().unwrap(),
            ProviderKind::Anthropic
        );
        assert!("bad".parse::<ProviderKind>().is_err());
    }
}
