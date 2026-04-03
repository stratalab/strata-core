//! Query expansion for intelligent search
//!
//! This module provides the `QueryExpander` trait and types for expanding
//! a natural language query into multiple typed search variants (lex/vec/hyde).
//! The expanded queries are run through the retrieval substrate in multiple
//! passes and fused with weighted RRF.
//!
//! # Expansion Types
//!
//! | Type | Purpose | Search mode |
//! |------|---------|-------------|
//! | `Lex` | Keyword reformulations | BM25 only |
//! | `Vec` | Semantic rephrasings | BM25 + vector (hybrid) |
//! | `Hyde` | Hypothetical document text | Vector only |

pub mod api;
pub mod error;
pub mod parser;
pub mod prompt;

#[cfg(test)]
pub(crate) mod mock;

pub use api::ApiExpander;
pub use error::ExpandError;

/// Type of expanded query — determines how it is searched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// Keyword reformulation — searched via BM25 only
    Lex,
    /// Semantic rephrasing — searched via hybrid (BM25 + vector)
    Vec,
    /// Hypothetical document text (HyDE) — embedded and searched via vector only
    Hyde,
}

/// A single expanded query with its type.
#[derive(Debug, Clone)]
pub struct ExpandedQuery {
    /// How this query should be searched
    pub query_type: QueryType,
    /// The query text
    pub text: String,
}

/// Result of query expansion — multiple typed queries.
#[derive(Debug, Clone)]
pub struct ExpandedQueries {
    /// The expanded query variants
    pub queries: Vec<ExpandedQuery>,
}

/// Trait for query expansion implementations.
///
/// Implementations take a natural language query and generate typed search
/// variants. The trait is object-safe for use as `Arc<dyn QueryExpander>`.
///
/// # Implementations
///
/// - `ApiExpander` — calls an OpenAI-compatible endpoint
pub trait QueryExpander: Send + Sync {
    /// Expand a query into multiple typed search variants.
    fn expand(&self, query: &str) -> Result<ExpandedQueries, ExpandError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expand::mock::MockExpander;
    use std::sync::Arc;

    #[test]
    fn test_query_expander_is_object_safe() {
        let expander: Arc<dyn QueryExpander> = Arc::new(MockExpander);
        let result = expander.expand("test").unwrap();
        assert!(!result.queries.is_empty());
    }

    #[test]
    fn test_expanded_query_types() {
        assert_ne!(QueryType::Lex, QueryType::Vec);
        assert_ne!(QueryType::Vec, QueryType::Hyde);
        assert_ne!(QueryType::Lex, QueryType::Hyde);
    }
}
