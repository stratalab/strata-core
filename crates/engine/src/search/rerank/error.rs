//! Error types for rerank contracts.

use std::fmt;

/// Errors returned by rerank implementations.
///
/// Engine owns the model-free rerank contract and deterministic blending.
/// Provider execution lives above engine and should map provider failures into
/// this error type.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerankError {
    /// Rerank output could not be parsed into valid scores.
    #[non_exhaustive]
    Parse {
        /// Human-readable parse failure detail.
        message: String,
    },
    /// The provider implementation failed outside engine.
    #[non_exhaustive]
    Provider {
        /// Human-readable provider failure detail.
        message: String,
    },
    /// Reranking is unavailable in the current runtime.
    #[non_exhaustive]
    Unavailable {
        /// Runtime feature or capability that is unavailable.
        feature: &'static str,
    },
}

impl RerankError {
    /// Construct a parse error.
    pub fn parse(message: impl Into<String>) -> Self {
        Self::Parse {
            message: message.into(),
        }
    }

    /// Construct a provider error.
    pub fn provider(message: impl Into<String>) -> Self {
        Self::Provider {
            message: message.into(),
        }
    }

    /// Construct an unavailable-capability error.
    pub fn unavailable(feature: &'static str) -> Self {
        Self::Unavailable { feature }
    }
}

impl fmt::Display for RerankError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RerankError::Parse { message } => write!(f, "parse error: {}", message),
            RerankError::Provider { message } => write!(f, "provider error: {}", message),
            RerankError::Unavailable { feature } => write!(f, "rerank unavailable: {}", feature),
        }
    }
}

impl std::error::Error for RerankError {}
