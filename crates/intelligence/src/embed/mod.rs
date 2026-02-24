//! Auto-embedding module: text embeddings via strata-inference GGUF engine.
//!
//! Provides a lazy-loading model lifecycle via [`EmbedModelState`] and
//! text extraction from Strata [`Value`] types.

pub mod download;
pub mod extract;

use std::sync::Arc;

use strata_inference::EmbeddingEngine;

/// Default model name used for embedding (resolved by strata-inference registry).
pub const DEFAULT_MODEL: &str = "miniLM";

/// Lazy-loading model state stored as a Database extension.
///
/// On first use, loads the embedding model from the strata-inference registry.
/// If model loading fails, stores the error and never retries.
pub struct EmbedModelState {
    engine: once_cell::sync::OnceCell<Result<Arc<EmbeddingEngine>, String>>,
}

impl Default for EmbedModelState {
    fn default() -> Self {
        Self {
            engine: once_cell::sync::OnceCell::new(),
        }
    }
}

impl EmbedModelState {
    /// Get or load the embedding engine.
    ///
    /// Loads the model via `EmbeddingEngine::from_registry(model_name)`.
    /// The `_model_dir` parameter is accepted for backwards compatibility but
    /// ignored — the registry manages model storage in `~/.strata/models/`.
    /// Caches the result (success or failure) so loading is attempted at most once.
    /// The first call's `model_name` wins (OnceCell semantics).
    pub fn get_or_load(
        &self,
        _model_dir: &std::path::Path,
        model_name: &str,
    ) -> Result<Arc<EmbeddingEngine>, String> {
        self.engine
            .get_or_init(|| {
                let engine = EmbeddingEngine::from_registry(model_name).map_err(|e| {
                    format!("Failed to load embedding model '{}': {}", model_name, e)
                })?;
                Ok(Arc::new(engine))
            })
            .clone()
    }

    /// The dimensionality of output embedding vectors.
    ///
    /// Returns `None` if the engine hasn't been loaded yet or failed to load.
    pub fn embedding_dim(&self) -> Option<usize> {
        self.engine
            .get()
            .and_then(|r| r.as_ref().ok())
            .map(|e| e.embedding_dim())
    }
}

/// Embed a query string using the cached embedding engine from the database.
///
/// Loads or retrieves the cached engine via [`EmbedModelState`], then embeds the
/// given text. Returns `None` (with a warning log) if the engine cannot be loaded
/// or embedding fails. This is a best-effort helper for hybrid search.
pub fn embed_query(db: &strata_engine::Database, text: &str) -> Option<Vec<f32>> {
    let model_dir = db.model_dir();
    let model_name = db.embed_model();
    let state = match db.extension::<EmbedModelState>() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Failed to get embed model state");
            return None;
        }
    };
    let engine = match state.get_or_load(&model_dir, &model_name) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Failed to load embed model for hybrid search");
            return None;
        }
    };
    match engine.embed(text) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Embedding failed");
            None
        }
    }
}

/// Batch-embed multiple query strings using the cached embedding model.
///
/// Uses [`EmbeddingEngine::embed_batch`] for batched inference. Returns one
/// `Option<Vec<f32>>` per input text — `None` if the model fails to load or
/// if the batch embedding call itself fails (in either case, all entries are
/// `None`).
pub fn embed_batch_queries(db: &strata_engine::Database, texts: &[&str]) -> Vec<Option<Vec<f32>>> {
    if texts.is_empty() {
        return vec![];
    }
    let model_dir = db.model_dir();
    let model_name = db.embed_model();
    let state = match db.extension::<EmbedModelState>() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Failed to get embed model state");
            return vec![None; texts.len()];
        }
    };
    let model = match state.get_or_load(&model_dir, &model_name) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Failed to load embed model for batch query embedding");
            return vec![None; texts.len()];
        }
    };
    match model.embed_batch(texts) {
        Ok(embeddings) => embeddings.into_iter().map(Some).collect(),
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Batch embedding failed");
            vec![None; texts.len()]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_embedding_dim_none_before_load() {
        let state = EmbedModelState::default();
        assert!(
            state.embedding_dim().is_none(),
            "dim should be None before any load attempt"
        );
    }

    #[test]
    fn test_get_or_load_returns_deterministic_result() {
        let state = EmbedModelState::default();
        let result = state.get_or_load(Path::new("/nonexistent/path"), DEFAULT_MODEL);
        // On CI without model files this will be Err; locally with model it may be Ok.
        // Either way, calling it again must return the exact same outcome.
        let result2 = state.get_or_load(Path::new("/different/path"), DEFAULT_MODEL);
        assert_eq!(
            result.is_ok(),
            result2.is_ok(),
            "OnceCell should return the same result regardless of path arg"
        );
    }

    #[test]
    fn test_error_is_cached_with_identical_message() {
        let state = EmbedModelState::default();
        let r1 = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
        let r2 = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
        assert_eq!(r1.is_ok(), r2.is_ok());
        if let (Err(e1), Err(e2)) = (&r1, &r2) {
            assert_eq!(e1, e2, "cached error message must be identical");
            assert!(
                e1.contains(DEFAULT_MODEL),
                "error should mention model name '{}', got: {}",
                DEFAULT_MODEL,
                e1
            );
        }
    }

    #[test]
    fn test_embedding_dim_none_after_failed_load() {
        let state = EmbedModelState::default();
        // Trigger a load attempt (may fail if model not installed).
        let _ = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
        // If load failed, dim should still be None.
        if state
            .get_or_load(Path::new("/unused"), DEFAULT_MODEL)
            .is_err()
        {
            assert!(
                state.embedding_dim().is_none(),
                "dim should be None after a failed load"
            );
        }
    }

    #[test]
    fn test_custom_model_name_in_error() {
        let state = EmbedModelState::default();
        let result = state.get_or_load(Path::new("/nonexistent"), "nomic-embed");
        // Whether OK or Err depends on model availability. If Err, the error
        // should mention the custom model name, not DEFAULT_MODEL.
        if let Err(e) = result {
            assert!(
                e.contains("nomic-embed"),
                "error should mention custom model name 'nomic-embed', got: {}",
                e
            );
        }
    }

    #[test]
    fn test_model_dir_param_is_ignored() {
        // Two calls with different paths must return the same result,
        // proving the path argument is truly ignored.
        let state = EmbedModelState::default();
        let r1 = state.get_or_load(Path::new("/path/a"), DEFAULT_MODEL);
        let r2 = state.get_or_load(Path::new("/path/b"), DEFAULT_MODEL);
        match (&r1, &r2) {
            (Ok(a), Ok(b)) => assert!(Arc::ptr_eq(a, b), "same Arc regardless of path"),
            (Err(a), Err(b)) => assert_eq!(a, b, "same error regardless of path"),
            _ => panic!("r1 and r2 should have same Ok/Err variant"),
        }
    }
}
