//! Pluggable search orchestration for Strata.
//!
//! All retrieval flows through [`substrate::retrieve()`] — the single,
//! deterministic, model-free entry point for search.

pub mod expand;
pub mod fuser;
pub mod llm_client;
pub mod rerank;
pub mod substrate;

pub use fuser::{weighted_rrf_fuse, FusedResult, Fuser, RRFFuser};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
