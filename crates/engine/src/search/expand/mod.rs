//! Query expansion contracts for intelligent search.
//!
//! This module provides model-free traits and types for expanding a natural
//! language query into typed search variants. Provider execution belongs above
//! engine; engine owns only the contract, parser, and prompt shape.

pub mod error;
pub mod parser;
pub mod prompt;

pub use error::ExpandError;

/// Type of expanded query and the retrieval mode it should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// Keyword reformulation searched via BM25 only.
    Lex,
    /// Semantic rephrasing searched via hybrid BM25 plus vector retrieval.
    Vec,
    /// Hypothetical document text searched via vector retrieval.
    Hyde,
}

/// A single expanded query with its retrieval type.
#[derive(Debug, Clone)]
pub struct ExpandedQuery {
    /// How this query should be searched.
    pub query_type: QueryType,
    /// The query text.
    pub text: String,
}

/// Result of query expansion: multiple typed query variants.
#[derive(Debug, Clone)]
pub struct ExpandedQueries {
    /// The expanded query variants.
    pub queries: Vec<ExpandedQuery>,
}

/// Trait for query expansion implementations.
///
/// Implementations take a natural language query and generate typed search
/// variants. The trait is object-safe for use as `Arc<dyn QueryExpander>`.
pub trait QueryExpander: Send + Sync {
    /// Expand a query into multiple typed search variants.
    fn expand(&self, query: &str) -> Result<ExpandedQueries, ExpandError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct StaticExpander;

    impl QueryExpander for StaticExpander {
        fn expand(&self, query: &str) -> Result<ExpandedQueries, ExpandError> {
            Ok(ExpandedQueries {
                queries: vec![
                    ExpandedQuery {
                        query_type: QueryType::Lex,
                        text: query.to_string(),
                    },
                    ExpandedQuery {
                        query_type: QueryType::Vec,
                        text: format!("information about {}", query),
                    },
                    ExpandedQuery {
                        query_type: QueryType::Hyde,
                        text: format!("A relevant document about {}", query),
                    },
                ],
            })
        }
    }

    #[test]
    fn query_expander_is_object_safe() {
        let expander: Arc<dyn QueryExpander> = Arc::new(StaticExpander);
        let result = expander.expand("test").unwrap();
        assert!(!result.queries.is_empty());
    }

    #[test]
    fn expanded_query_types_are_distinct() {
        assert_ne!(QueryType::Lex, QueryType::Vec);
        assert_ne!(QueryType::Vec, QueryType::Hyde);
        assert_ne!(QueryType::Lex, QueryType::Hyde);
    }

    #[test]
    fn expand_error_display_is_provider_neutral() {
        assert_eq!(
            ExpandError::parse("bad output").to_string(),
            "parse error: bad output"
        );
        assert_eq!(
            ExpandError::unavailable("model provider").to_string(),
            "query expansion unavailable: model provider"
        );
    }
}
