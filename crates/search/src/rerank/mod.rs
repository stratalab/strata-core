//! Cross-encoder re-ranking for search results
//!
//! This module provides post-fusion re-ranking: after RRF produces top candidates,
//! a configured model rescores (query, document) pairs for fine-grained relevance
//! ordering via `/chat/completions` with a relevance-scoring prompt.
//!
//! # Architecture
//!
//! Re-ranking sits between RRF fusion and final result return:
//!
//! ```text
//! multi-pass HybridSearch → RRF fusion → top-N candidates
//!     → LLM rerank via chat completions → position-aware blending
//!     → return top-k results
//! ```
//!
//! A single API call scores all candidates (not one-per-document),
//! keeping latency bounded to one round-trip.

pub mod api;
pub mod blend;
pub mod error;
pub mod prompt;

pub use api::ApiReranker;
pub use blend::{blend_scores, BlendWeights};
pub use error::RerankError;

/// A relevance score assigned by the reranker to a search hit.
#[derive(Debug, Clone)]
pub struct RerankScore {
    /// Index into the original hit list (0-based position)
    pub index: usize,
    /// Normalized relevance score in [0.0, 1.0]
    pub relevance_score: f32,
}

/// Trait for re-ranking implementations.
///
/// Implementations take a query and document snippets, and return relevance
/// scores. The trait is object-safe for use as `Arc<dyn Reranker>`.
///
/// # Implementations
///
/// - `ApiReranker` — calls an OpenAI-compatible endpoint
pub trait Reranker: Send + Sync {
    /// Score the relevance of each snippet to the query.
    ///
    /// `snippets` contains `(original_index, snippet_text)` pairs.
    /// Returns scores mapped back to original indices.
    fn rerank(
        &self,
        query: &str,
        snippets: &[(usize, &str)],
    ) -> Result<Vec<RerankScore>, RerankError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rerank_score_fields() {
        let score = RerankScore {
            index: 3,
            relevance_score: 0.85,
        };
        assert_eq!(score.index, 3);
        assert!((score.relevance_score - 0.85).abs() < f32::EPSILON);
    }
}
