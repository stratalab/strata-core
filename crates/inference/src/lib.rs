mod error;
pub mod registry;

#[cfg(feature = "local")]
pub mod llama;

#[cfg(feature = "local")]
mod embed;

#[cfg(feature = "local")]
mod provider;

#[cfg(feature = "local")]
mod generate;

pub use error::InferenceError;
pub use registry::{ModelRegistry, ModelInfo, ModelTask};

#[cfg(feature = "local")]
pub use embed::EmbeddingEngine;

#[cfg(feature = "local")]
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
        assert!(msg.contains("unknown"), "error should contain bad input: {msg}");
        // Error should list valid options
        assert!(msg.contains("local"), "error should list valid options: {msg}");
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
        let p3 = p.clone();
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
}
