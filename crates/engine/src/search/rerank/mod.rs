//! Rerank contracts for search results.
//!
//! Engine owns the model-free rerank trait, response parser, prompt shape, and
//! deterministic blending. Provider execution belongs above engine.

pub mod blend;
pub mod error;
pub mod parser;
pub mod prompt;

pub use blend::{blend_scores, BlendWeights};
pub use error::RerankError;
pub use parser::parse_rerank_response;

/// A relevance score assigned by a reranker to a search hit.
#[derive(Debug, Clone)]
pub struct RerankScore {
    /// Index into the original hit list.
    pub index: usize,
    /// Normalized relevance score in `[0.0, 1.0]`.
    pub relevance_score: f32,
}

/// Trait for rerank implementations.
///
/// Implementations take a query and document snippets, then return relevance
/// scores. The trait is object-safe for use as `Arc<dyn Reranker>`.
pub trait Reranker: Send + Sync {
    /// Score the relevance of each snippet to the query.
    ///
    /// `snippets` contains `(original_index, snippet_text)` pairs. Returned
    /// scores map back to those original indices.
    fn rerank(
        &self,
        query: &str,
        snippets: &[(usize, &str)],
    ) -> Result<Vec<RerankScore>, RerankError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct StaticReranker;

    impl Reranker for StaticReranker {
        fn rerank(
            &self,
            _query: &str,
            snippets: &[(usize, &str)],
        ) -> Result<Vec<RerankScore>, RerankError> {
            Ok(snippets
                .iter()
                .map(|(index, _)| RerankScore {
                    index: *index,
                    relevance_score: 0.5,
                })
                .collect())
        }
    }

    #[test]
    fn rerank_score_fields() {
        let score = RerankScore {
            index: 3,
            relevance_score: 0.85,
        };
        assert_eq!(score.index, 3);
        assert!((score.relevance_score - 0.85).abs() < f32::EPSILON);
    }

    #[test]
    fn reranker_is_object_safe() {
        let reranker: Arc<dyn Reranker> = Arc::new(StaticReranker);
        let snippets = [(4, "doc")];
        let scores = reranker.rerank("query", &snippets).unwrap();
        assert_eq!(scores[0].index, 4);
    }

    #[test]
    fn rerank_error_display_is_provider_neutral() {
        assert_eq!(
            RerankError::provider("quota").to_string(),
            "provider error: quota"
        );
        assert_eq!(
            RerankError::unavailable("model provider").to_string(),
            "rerank unavailable: model provider"
        );
    }
}
