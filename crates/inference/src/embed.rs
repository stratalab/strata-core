//! Embedding engine: text → dense vector embedding via llama.cpp.
//!
//! [`EmbeddingEngine`] provides a high-level API for producing L2-normalized
//! text embeddings from any supported GGUF embedding model.
//!
//! The pipeline: tokenize → truncate → encode/decode → pool → L2 normalize.
//!
//! Thread-safe via internal `Mutex` (llama.cpp contexts are not thread-safe).

use std::path::Path;
use std::sync::Mutex;

use tracing::info;

use crate::llama::context::LlamaCppContext;
use crate::InferenceError;

/// High-level embedding engine backed by llama.cpp.
///
/// Wraps a GGUF model loaded via the llama.cpp C API, exposing a simple
/// `embed(text) -> Vec<f32>` interface. Thread-safe via internal `Mutex`.
///
/// # Example
///
/// ```no_run
/// use strata_inference::EmbeddingEngine;
///
/// let engine = EmbeddingEngine::from_gguf("model.gguf")?;
/// let embedding = engine.embed("Hello world")?;
/// assert_eq!(embedding.len(), engine.embedding_dim());
/// # Ok::<(), strata_inference::InferenceError>(())
/// ```
pub struct EmbeddingEngine {
    ctx: Mutex<LlamaCppContext>,
}

impl std::fmt::Debug for EmbeddingEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (n_embd, vocab_size) = match self.ctx.lock() {
            Ok(ctx) => (ctx.n_embd, ctx.vocab_size),
            Err(_) => (0, 0),
        };
        f.debug_struct("EmbeddingEngine")
            .field("n_embd", &n_embd)
            .field("vocab_size", &vocab_size)
            .finish()
    }
}

impl EmbeddingEngine {
    /// Load an embedding engine from a GGUF file.
    ///
    /// The model is loaded with `embeddings=true` and `pooling_type=MEAN`.
    /// All layers remain on CPU (suitable for small embedding models).
    pub fn from_gguf(path: impl AsRef<Path>) -> Result<Self, InferenceError> {
        let path = path.as_ref();
        info!(path = %path.display(), "Loading embedding engine from GGUF");
        let ctx = LlamaCppContext::load_for_embedding(path)?;
        info!(
            n_embd = ctx.n_embd,
            vocab_size = ctx.vocab_size,
            n_ctx = ctx.n_ctx,
            "Embedding engine ready"
        );
        Ok(Self {
            ctx: Mutex::new(ctx),
        })
    }

    /// Load an embedding engine by model name from the registry.
    ///
    /// Resolves the name (e.g., `"miniLM"`) to a local GGUF file path.
    /// Blocked until Epic 5 (registry); returns `NotSupported` for now.
    pub fn from_registry(_name: &str) -> Result<Self, InferenceError> {
        Err(InferenceError::NotSupported(
            "model registry not yet implemented (Epic 5)".to_string(),
        ))
    }

    /// Produce an L2-normalized embedding vector for the given text.
    ///
    /// Steps:
    /// 1. Tokenize with special tokens (BOS/EOS or CLS/SEP)
    /// 2. Truncate to context size if needed
    /// 3. Run encode (encoder models) or decode (decoder models)
    /// 4. Extract pooled embeddings
    /// 5. L2-normalize the result
    /// 6. Clear KV cache for next call
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, InferenceError> {
        let ctx = self.ctx.lock().map_err(|e| {
            InferenceError::LlamaCpp(format!("mutex poisoned: {}", e))
        })?;

        // 1. Tokenize
        let mut tokens = ctx.tokenize(text, true);
        if tokens.is_empty() {
            return Ok(vec![0.0f32; ctx.n_embd]);
        }

        // 2. Truncate to context size
        if tokens.len() > ctx.n_ctx {
            tokens.truncate(ctx.n_ctx);
        }

        // 3. Create batch and run inference
        let batch = ctx.api.batch_get_one(&mut tokens);

        if ctx.has_encoder {
            ctx.api
                .encode(ctx.ctx, batch)
                .map_err(InferenceError::LlamaCpp)?;
        } else {
            ctx.api
                .decode(ctx.ctx, batch)
                .map_err(InferenceError::LlamaCpp)?;
        }

        // 4. Extract embeddings
        // Try sequence-level pooled embeddings first (for models with pooling)
        let emb_ptr = ctx.api.get_embeddings_seq(ctx.ctx, 0);
        let emb_ptr = if emb_ptr.is_null() {
            // Fall back to token-level embeddings
            ctx.api.get_embeddings(ctx.ctx)
        } else {
            emb_ptr
        };

        if emb_ptr.is_null() {
            ctx.clear_memory();
            return Err(InferenceError::LlamaCpp(
                "llama_get_embeddings returned null — model may not support embeddings"
                    .to_string(),
            ));
        }

        // SAFETY: emb_ptr is non-null and points to n_embd floats owned by
        // llama.cpp's context, which we hold via the Mutex lock.
        let embedding =
            unsafe { std::slice::from_raw_parts(emb_ptr, ctx.n_embd) }.to_vec();

        // 5. L2 normalize
        let normalized = l2_normalize(&embedding);

        // 6. Clear memory for next call
        ctx.clear_memory();

        Ok(normalized)
    }

    /// Produce embeddings for a batch of texts.
    ///
    /// Each text is processed independently (sequential). Batched GPU execution
    /// can be optimized in a future iteration.
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError> {
        texts.iter().map(|text| self.embed(text)).collect()
    }

    /// The dimensionality of output embedding vectors.
    pub fn embedding_dim(&self) -> usize {
        self.ctx.lock().unwrap().n_embd
    }

    /// The vocabulary size of the loaded model.
    pub fn vocab_size(&self) -> usize {
        self.ctx.lock().unwrap().vocab_size
    }
}

/// L2-normalize a vector in-place, returning the normalized result.
///
/// If the vector has zero norm (all zeros), it is returned unchanged.
fn l2_normalize(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter().map(|x| x / norm).collect()
    } else {
        v.to_vec()
    }
}

// Compile-time verify Send + Sync.
const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_both() {
        assert_send::<EmbeddingEngine>();
        assert_sync::<EmbeddingEngine>();
    }
    let _ = assert_both;
};

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // l2_normalize unit tests (no libllama needed)
    // -----------------------------------------------------------------------

    #[test]
    fn l2_normalize_unit_vector_unchanged() {
        // Already unit: [1, 0, 0]
        let v = vec![1.0, 0.0, 0.0];
        let n = l2_normalize(&v);
        assert!((n[0] - 1.0).abs() < 1e-6);
        assert!((n[1]).abs() < 1e-6);
        assert!((n[2]).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_scales_to_unit_length() {
        let v = vec![3.0, 4.0];
        let n = l2_normalize(&v);
        // norm = 5.0, so [0.6, 0.8]
        assert!((n[0] - 0.6).abs() < 1e-6);
        assert!((n[1] - 0.8).abs() < 1e-6);
        // Verify L2 norm is 1.0
        let norm: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "norm should be 1.0, got {norm}");
    }

    #[test]
    fn l2_normalize_zero_vector_unchanged() {
        let v = vec![0.0, 0.0, 0.0];
        let n = l2_normalize(&v);
        assert_eq!(n, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn l2_normalize_negative_values() {
        let v = vec![-3.0, 4.0];
        let n = l2_normalize(&v);
        let norm: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "norm should be 1.0, got {norm}");
        // Direction preserved: n[0] should be negative, n[1] positive
        assert!(n[0] < 0.0);
        assert!(n[1] > 0.0);
    }

    #[test]
    fn l2_normalize_single_element() {
        let v = vec![5.0];
        let n = l2_normalize(&v);
        assert!((n[0] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_single_negative_element() {
        let v = vec![-5.0];
        let n = l2_normalize(&v);
        assert!((n[0] - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_empty_vector() {
        let v: Vec<f32> = vec![];
        let n = l2_normalize(&v);
        assert!(n.is_empty());
    }

    #[test]
    fn l2_normalize_large_vector() {
        // 384-dim vector (MiniLM embedding size)
        let v: Vec<f32> = (0..384).map(|i| (i as f32) * 0.01).collect();
        let n = l2_normalize(&v);
        assert_eq!(n.len(), 384);
        let norm: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-5,
            "384-dim norm should be 1.0, got {norm}"
        );
    }

    #[test]
    fn l2_normalize_very_small_values() {
        // Very small but non-zero — should still normalize
        let v = vec![1e-20, 1e-20, 1e-20];
        let n = l2_normalize(&v);
        let norm: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-4,
            "very small values should normalize, got norm={norm}"
        );
    }

    #[test]
    fn l2_normalize_preserves_relative_magnitudes() {
        let v = vec![1.0, 2.0, 3.0];
        let n = l2_normalize(&v);
        // n[1]/n[0] should be 2.0, n[2]/n[0] should be 3.0
        assert!((n[1] / n[0] - 2.0).abs() < 1e-6);
        assert!((n[2] / n[0] - 3.0).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_idempotent() {
        // Normalizing twice should give the same result
        let v = vec![3.0, 4.0, 5.0];
        let n1 = l2_normalize(&v);
        let n2 = l2_normalize(&n1);
        for (a, b) in n1.iter().zip(n2.iter()) {
            assert!((a - b).abs() < 1e-6, "normalize should be idempotent");
        }
    }

    // -----------------------------------------------------------------------
    // EmbeddingEngine constructor tests (no libllama needed for error paths)
    // -----------------------------------------------------------------------

    #[test]
    fn from_registry_returns_not_supported() {
        let result = EmbeddingEngine::from_registry("miniLM");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, InferenceError::NotSupported(_)),
            "should be NotSupported, got: {err}"
        );
        assert!(
            err.to_string().contains("registry"),
            "error should mention registry: {err}"
        );
    }

    #[test]
    fn from_gguf_nonexistent_file_returns_error() {
        // This will fail at LlamaCppApi::load() since libllama isn't installed in CI
        let result = EmbeddingEngine::from_gguf("/nonexistent/path/model.gguf");
        assert!(result.is_err());
    }

    #[test]
    fn from_gguf_accepts_path_types() {
        // Verify the `impl AsRef<Path>` works with various types
        // All will fail (no libllama), but should compile and return errors
        let _ = EmbeddingEngine::from_gguf("/tmp/model.gguf");
        let _ = EmbeddingEngine::from_gguf(String::from("/tmp/model.gguf"));
        let _ = EmbeddingEngine::from_gguf(std::path::PathBuf::from("/tmp/model.gguf"));
    }

    // -----------------------------------------------------------------------
    // Compile-time trait assertions
    // -----------------------------------------------------------------------

    #[test]
    fn embedding_engine_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EmbeddingEngine>();
    }
}
