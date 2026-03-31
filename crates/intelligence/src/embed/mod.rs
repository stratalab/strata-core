//! Auto-embedding module: text embeddings via strata-inference.
//!
//! Provides a lazy-loading model lifecycle via [`EmbedModelState`] and
//! text extraction from Strata [`Value`] types.
//!
//! Supports both local GGUF models (e.g., `"miniLM"`) and cloud APIs
//! (e.g., `"openai:text-embedding-3-small"`) via `strata_inference::load_embedder()`.

pub mod download;
pub mod extract;

use std::sync::{Arc, Mutex};

use strata_inference::InferenceEngine;

/// Default model name used for embedding (resolved by strata-inference registry).
pub const DEFAULT_MODEL: &str = "miniLM";

/// Maximum number of load attempts before giving up.
const MAX_RETRIES: u32 = 3;

/// Retry-capable model state stored as a Database extension.
///
/// On first use, loads the embedding engine via `strata_inference::load_embedder()`,
/// which handles both local GGUF models and cloud API providers.
/// If model loading fails, retries up to [`MAX_RETRIES`] times.
/// Thread-safe wrapper around a `dyn InferenceEngine` (which is `Send` but not necessarily `Sync`).
type SharedEngine = Arc<Mutex<Box<dyn InferenceEngine>>>;

pub struct EmbedModelState {
    engine: Mutex<Option<SharedEngine>>,
    /// The model spec that was used to load the current engine.
    loaded_model: Mutex<Option<String>>,
    load_error: Mutex<Option<(String, u32)>>,
}

impl Default for EmbedModelState {
    fn default() -> Self {
        Self {
            engine: Mutex::new(None),
            loaded_model: Mutex::new(None),
            load_error: Mutex::new(None),
        }
    }
}

impl EmbedModelState {
    /// Get or load the embedding engine.
    ///
    /// Accepts model specs in `"provider:model"` format (e.g.,
    /// `"openai:text-embedding-3-small"`) or bare local names (e.g., `"miniLM"`).
    /// The `_model_dir` parameter is accepted for backwards compatibility but
    /// ignored — the registry manages model storage in `~/.strata/models/`.
    ///
    /// On failure, retries up to [`MAX_RETRIES`] times. Once all retries are
    /// exhausted, subsequent calls return the cached error without retrying.
    pub fn get_or_load(
        &self,
        _model_dir: &std::path::Path,
        model_name: &str,
    ) -> Result<SharedEngine, String> {
        // Fast path: engine already loaded, healthy, and matches requested model.
        {
            let mut guard = self.engine.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ref eng) = *guard {
                let model_matches = {
                    let m = self.loaded_model.lock().unwrap_or_else(|e| e.into_inner());
                    m.as_deref() == Some(model_name)
                };

                let healthy = {
                    let inner = eng.lock().unwrap_or_else(|e| e.into_inner());
                    inner.is_healthy()
                };

                if model_matches && healthy {
                    return Ok(Arc::clone(eng));
                }
                if !healthy {
                    tracing::warn!(
                        target: "strata::embed",
                        "Embedding engine context poisoned (likely a prior panic) \u{2014} reloading model"
                    );
                }
                *guard = None;
                drop(guard);
                let mut err_guard = self.load_error.lock().unwrap_or_else(|e| e.into_inner());
                *err_guard = None;
            }
        }

        // Check if retries are exhausted.
        {
            let err_guard = self.load_error.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((ref msg, attempts)) = *err_guard {
                if attempts >= MAX_RETRIES {
                    return Err(msg.clone());
                }
            }
        }

        // Attempt to load the model (handles both local and cloud providers).
        match strata_inference::load_embedder(model_name) {
            Ok(engine) => {
                let shared: SharedEngine = Arc::new(Mutex::new(engine));
                {
                    let mut guard = self.engine.lock().unwrap_or_else(|e| e.into_inner());
                    *guard = Some(Arc::clone(&shared));
                }
                {
                    let mut m = self.loaded_model.lock().unwrap_or_else(|e| e.into_inner());
                    *m = Some(model_name.to_string());
                }
                {
                    let mut err_guard = self.load_error.lock().unwrap_or_else(|e| e.into_inner());
                    *err_guard = None;
                }
                Ok(shared)
            }
            Err(e) => {
                let msg = format!("Failed to load embedding model '{}': {}", model_name, e);
                let mut err_guard = self.load_error.lock().unwrap_or_else(|e| e.into_inner());
                let attempts = err_guard.as_ref().map_or(0, |(_, a)| *a) + 1;
                *err_guard = Some((msg.clone(), attempts));
                Err(msg)
            }
        }
    }

    /// The dimensionality of output embedding vectors.
    ///
    /// Returns `None` if the engine hasn't been loaded yet or failed to load.
    pub fn embedding_dim(&self) -> Option<usize> {
        let guard = self.engine.lock().unwrap_or_else(|e| e.into_inner());
        guard.as_ref().map(|shared| {
            let inner = shared.lock().unwrap_or_else(|e| e.into_inner());
            inner.embedding_dim()
        })
    }
}

/// Embed a query string using the cached embedding engine from the database.
///
/// Uses the database's configured `embed_model` to choose the engine.
/// Returns `None` (with a warning log) if the engine cannot be loaded
/// or embedding fails. This is a best-effort helper for hybrid search.
pub fn embed_query(db: &strata_engine::Database, text: &str) -> Option<Vec<f32>> {
    let model_name = db.embed_model();
    embed_query_with_model(db, text, &model_name)
}

/// Embed a query string using a specific model spec (e.g., `"openai:text-embedding-3-small"`).
///
/// The model spec is passed to `strata_inference::load_embedder()` which handles
/// both local GGUF models and cloud API providers.
pub fn embed_query_with_model(
    db: &strata_engine::Database,
    text: &str,
    model_spec: &str,
) -> Option<Vec<f32>> {
    let model_dir = db.model_dir();
    let state = match db.extension::<EmbedModelState>() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Failed to get embed model state");
            return None;
        }
    };
    let shared = match state.get_or_load(&model_dir, model_spec) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Failed to load embed model for hybrid search");
            return None;
        }
    };
    let engine = shared.lock().unwrap_or_else(|e| e.into_inner());
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
/// `None`). After a successful batch, validates that all returned vectors have
/// the same dimensionality; if any differ, logs a warning and returns all `None`.
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
    let shared = match state.get_or_load(&model_dir, &model_name) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(target: "strata::hybrid", error = %e, "Failed to load embed model for batch query embedding");
            return vec![None; texts.len()];
        }
    };
    let model = shared.lock().unwrap_or_else(|e| e.into_inner());
    match model.embed_batch(texts) {
        Ok(embeddings) => {
            // Dimension validation: all vectors must have the same length.
            if !embeddings.is_empty() {
                let expected_dim = embeddings[0].len();
                let all_same = embeddings.iter().all(|v| v.len() == expected_dim);
                if !all_same {
                    tracing::warn!(
                        target: "strata::hybrid",
                        expected_dim = expected_dim,
                        count = embeddings.len(),
                        "Batch embedding dimension mismatch: not all vectors have the same length"
                    );
                    return vec![None; texts.len()];
                }
            }
            embeddings.into_iter().map(Some).collect()
        }
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
        // With retry semantics, the result may change between calls if one fails
        // and a later one retries. But for a consistent environment (same model
        // availability), calling with the same model name should yield the same
        // Ok/Err variant once the state stabilizes.
        let result2 = state.get_or_load(Path::new("/different/path"), DEFAULT_MODEL);
        assert_eq!(
            result.is_ok(),
            result2.is_ok(),
            "same environment should return the same result variant"
        );
    }

    #[test]
    fn test_error_is_cached_with_identical_message() {
        let state = EmbedModelState::default();
        // Exhaust all retries so the error is cached.
        let mut last_err = None;
        for _ in 0..MAX_RETRIES {
            let r = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
            if let Err(e) = r {
                last_err = Some(e);
            }
        }
        // After MAX_RETRIES, additional calls should return the cached error.
        let r_after = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
        if let (Some(cached), Err(returned)) = (&last_err, &r_after) {
            assert_eq!(cached, returned, "cached error message must be identical");
            assert!(
                returned.contains(DEFAULT_MODEL),
                "error should mention model name '{}', got: {}",
                DEFAULT_MODEL,
                returned
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
        // proving the path argument is truly ignored. With retry semantics,
        // both calls attempt loading with the same model name and should
        // converge to the same outcome in a consistent environment.
        let state = EmbedModelState::default();
        let r1 = state.get_or_load(Path::new("/path/a"), DEFAULT_MODEL);
        let r2 = state.get_or_load(Path::new("/path/b"), DEFAULT_MODEL);
        match (&r1, &r2) {
            (Ok(a), Ok(b)) => assert!(Arc::ptr_eq(a, b), "same Arc regardless of path"),
            (Err(_), Err(_)) => {
                // Both failed — this is expected when the model is not installed.
                // The error messages may differ slightly between retries (e.g., if
                // the underlying error includes timing info), but both should
                // mention the model name.
                assert!(
                    r1.as_ref().unwrap_err().contains(DEFAULT_MODEL),
                    "error should mention model name"
                );
            }
            _ => panic!("r1 and r2 should have same Ok/Err variant"),
        }
    }

    #[test]
    fn test_retry_resets_on_success() {
        // Verify the state machine: after a successful load, the error tracking
        // is cleared. We test this by checking that a successful load results in
        // the engine being cached and error state being None.
        let state = EmbedModelState::default();
        // First, attempt a load (will fail on CI without model).
        let r1 = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
        if r1.is_ok() {
            // Model is available — verify engine is cached and error is clear.
            let err_guard = state.load_error.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                err_guard.is_none(),
                "error tracking should be None after successful load"
            );
            drop(err_guard);
            assert!(
                state.embedding_dim().is_some(),
                "dim should be Some after successful load"
            );
        } else {
            // Model not available — verify error tracking has attempt count 1.
            let err_guard = state.load_error.lock().unwrap_or_else(|e| e.into_inner());
            let (_, attempts) = err_guard.as_ref().expect("error should be tracked");
            assert_eq!(*attempts, 1, "first failure should record 1 attempt");
            drop(err_guard);
            // A second call should retry (attempt count < MAX_RETRIES).
            let r2 = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
            if r2.is_err() {
                let err_guard = state.load_error.lock().unwrap_or_else(|e| e.into_inner());
                let (_, attempts) = err_guard.as_ref().expect("error should be tracked");
                assert_eq!(*attempts, 2, "second failure should record 2 attempts");
            }
        }
    }

    #[test]
    fn test_max_retries_exhausted() {
        let state = EmbedModelState::default();
        // Exhaust all retries.
        let mut errors = Vec::new();
        for i in 0..MAX_RETRIES {
            let r = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
            if let Err(e) = r {
                errors.push(e);
            } else {
                // Model is available, skip this test.
                return;
            }
            // Verify attempt count increments.
            let err_guard = state.load_error.lock().unwrap_or_else(|e| e.into_inner());
            let (_, attempts) = err_guard.as_ref().unwrap();
            assert_eq!(*attempts, i + 1, "attempt count should increment");
        }
        // After MAX_RETRIES, further calls should return the cached error
        // without incrementing the attempt count.
        let cached_error = errors.last().unwrap().clone();
        for _ in 0..5 {
            let r = state.get_or_load(Path::new("/nonexistent"), DEFAULT_MODEL);
            assert!(r.is_err(), "should still be Err after retries exhausted");
            assert_eq!(
                r.unwrap_err(),
                cached_error,
                "should return the same cached error"
            );
            // Verify attempt count hasn't increased beyond MAX_RETRIES.
            let err_guard = state.load_error.lock().unwrap_or_else(|e| e.into_inner());
            let (_, attempts) = err_guard.as_ref().unwrap();
            assert_eq!(
                *attempts, MAX_RETRIES,
                "attempt count should not increase beyond MAX_RETRIES"
            );
        }
    }

    #[test]
    fn test_dimension_check_logic() {
        // Directly test the dimension validation logic used in embed_batch_queries.
        // We can't call embed_batch_queries without a Database, but we can verify
        // the check pattern works on raw vectors.
        let uniform: Vec<Vec<f32>> = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let expected_dim = uniform[0].len();
        assert!(
            uniform.iter().all(|v| v.len() == expected_dim),
            "uniform dimensions should pass the check"
        );

        let mixed: Vec<Vec<f32>> = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0]];
        let expected_dim = mixed[0].len();
        assert!(
            !mixed.iter().all(|v| v.len() == expected_dim),
            "mixed dimensions should fail the check"
        );

        // Empty batch passes trivially (no dimension to check).
        let empty: Vec<Vec<f32>> = vec![];
        assert!(empty.is_empty(), "empty batch needs no dimension check");

        // Single vector batch always passes.
        let single: Vec<Vec<f32>> = vec![vec![1.0]];
        let expected_dim = single[0].len();
        assert!(single.iter().all(|v| v.len() == expected_dim));
    }

    #[test]
    fn test_poison_recovery_clears_error_state() {
        // Verify that clearing engine + load_error (as the poison recovery
        // path does) allows fresh retries even after MAX_RETRIES was exhausted.
        // Uses a bogus model name that will never resolve, so this test
        // works regardless of which models are installed locally.
        let state = EmbedModelState::default();
        let bogus_model = "nonexistent-test-model-for-poison-recovery";

        // Exhaust retries with a model name that definitely doesn't exist.
        for _ in 0..MAX_RETRIES {
            let _ = state.get_or_load(Path::new("/nonexistent"), bogus_model);
        }

        // Confirm retries are exhausted — returns cached error.
        let r = state.get_or_load(Path::new("/nonexistent"), bogus_model);
        assert!(r.is_err(), "retries should be exhausted");
        let cached_err = r.unwrap_err();

        // Simulate what happens after poison recovery:
        // engine is set to None and load_error is cleared.
        {
            let mut guard = state.engine.lock().unwrap_or_else(|e| e.into_inner());
            *guard = None;
        }
        {
            let mut err_guard = state.load_error.lock().unwrap_or_else(|e| e.into_inner());
            *err_guard = None;
        }

        // Should be able to retry now (not blocked by stale error state).
        let r = state.get_or_load(Path::new("/nonexistent"), bogus_model);
        assert!(r.is_err(), "bogus model should still fail");
        let fresh_err = r.unwrap_err();

        // Verify the retry counter was reset (attempt count should be 1, not MAX_RETRIES).
        let err_guard = state.load_error.lock().unwrap_or_else(|e| e.into_inner());
        let (_, attempts) = err_guard
            .as_ref()
            .expect("should have error after fresh attempt");
        assert_eq!(
            *attempts, 1,
            "attempt count should be 1 after poison recovery cleared state, got {}",
            attempts
        );
        assert!(
            fresh_err.contains(bogus_model),
            "error should mention model name: {}",
            fresh_err
        );
        // Error messages should be the same (same failure), confirming a fresh attempt.
        assert_eq!(cached_err, fresh_err, "same model → same error message");
    }
}
