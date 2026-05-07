//! Error types for query expansion contracts.

use std::fmt;

/// Errors returned by query expansion implementations.
///
/// Engine owns the model-free expansion contract, but provider execution lives
/// above engine. Provider-backed implementations should map their provider
/// errors into this contract type before returning to engine consumers.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandError {
    /// Expansion output could not be parsed into valid query variants.
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
    /// Expansion is unavailable in the current runtime.
    #[non_exhaustive]
    Unavailable {
        /// Runtime feature or capability that is unavailable.
        feature: &'static str,
    },
}

impl ExpandError {
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

impl fmt::Display for ExpandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpandError::Parse { message } => write!(f, "parse error: {}", message),
            ExpandError::Provider { message } => write!(f, "provider error: {}", message),
            ExpandError::Unavailable { feature } => {
                write!(f, "query expansion unavailable: {}", feature)
            }
        }
    }
}

impl std::error::Error for ExpandError {}
