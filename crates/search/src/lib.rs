//! Pluggable search orchestration for Strata.

pub mod expand;
pub mod fuser;
pub mod hybrid;
pub mod llm_client;
pub mod rerank;

use std::sync::Arc;
use strata_engine::Database;

pub use fuser::{weighted_rrf_fuse, FusedResult, Fuser, RRFFuser};
pub use hybrid::HybridSearch;

/// Trait for embedding query text into a vector.
/// Injected by the executor from strata-intelligence when the embed feature is active.
pub trait QueryEmbedder: Send + Sync {
    /// Embed the given text, returning None on failure.
    fn embed(&self, text: &str) -> Option<Vec<f32>>;

    /// Embed multiple texts in a single batched call.
    ///
    /// Default implementation falls back to serial `embed()` calls.
    /// Implementations backed by a batch-capable model should override this
    /// to use tensor-batched inference for better throughput.
    fn embed_batch(&self, texts: &[&str]) -> Vec<Option<Vec<f32>>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

/// Extension trait for Database to provide search functionality.
pub trait DatabaseSearchExt {
    /// Get the hybrid search interface
    fn hybrid(&self) -> HybridSearch;
}

impl DatabaseSearchExt for Arc<Database> {
    fn hybrid(&self) -> HybridSearch {
        HybridSearch::new(Arc::clone(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::BranchId;
    use strata_engine::search::SearchRequest;

    #[test]
    fn test_database_search_ext() {
        let db = Database::cache().expect("Failed to create test database");

        let hybrid = db.hybrid();
        let branch_id = BranchId::new();
        let req = SearchRequest::new(branch_id, "test");

        let response = hybrid.search(&req).unwrap();
        assert!(response.hits.is_empty());
    }

    #[test]
    fn test_query_embedder_is_object_safe() {
        // Verify QueryEmbedder can be used as a trait object behind Arc.
        struct MockEmbedder;
        impl QueryEmbedder for MockEmbedder {
            fn embed(&self, _text: &str) -> Option<Vec<f32>> {
                Some(vec![0.1, 0.2, 0.3])
            }
        }

        let embedder: Arc<dyn QueryEmbedder> = Arc::new(MockEmbedder);
        let result = embedder.embed("hello");
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);
    }

    #[test]
    fn test_query_embedder_none_propagates() {
        struct FailingEmbedder;
        impl QueryEmbedder for FailingEmbedder {
            fn embed(&self, _text: &str) -> Option<Vec<f32>> {
                None
            }
        }

        let embedder: Arc<dyn QueryEmbedder> = Arc::new(FailingEmbedder);
        assert!(embedder.embed("hello").is_none());
    }
}
