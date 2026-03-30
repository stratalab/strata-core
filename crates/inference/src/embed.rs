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
    /// Resolves the name (e.g., `"miniLM"`) to a local GGUF file path,
    /// automatically downloading from HuggingFace if the model is not
    /// present locally. On load failure, checks for corrupted files
    /// (size mismatch vs catalog) and deletes them so the next attempt
    /// can re-download a fresh copy.
    pub fn from_registry(name: &str) -> Result<Self, InferenceError> {
        let registry = crate::registry::ModelRegistry::new();

        #[cfg(feature = "download")]
        let path = registry.resolve_or_pull(name)?;

        #[cfg(not(feature = "download"))]
        let path = registry.resolve(name)?;

        match Self::from_gguf(&path) {
            Ok(engine) => Ok(engine),
            Err(e) => {
                // If loading failed, the file may be corrupted/truncated.
                // Delete it if size doesn't match catalog so next retry
                // can re-download a fresh copy.
                registry.check_and_clean_corrupt(name, &path);
                Err(e)
            }
        }
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
        let ctx = self
            .ctx
            .lock()
            .map_err(|e| InferenceError::LlamaCpp(format!("mutex poisoned: {}", e)))?;

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

        let inference_result = if ctx.has_encoder {
            ctx.api.encode(ctx.ctx, batch)
        } else {
            ctx.api.decode(ctx.ctx, batch)
        };
        if let Err(e) = inference_result {
            ctx.clear_memory();
            return Err(InferenceError::LlamaCpp(e));
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
                "llama_get_embeddings returned null — model may not support embeddings".to_string(),
            ));
        }

        // SAFETY: emb_ptr is non-null and points to n_embd floats owned by
        // llama.cpp's context, which we hold via the Mutex lock.
        let embedding = unsafe { std::slice::from_raw_parts(emb_ptr, ctx.n_embd) }.to_vec();

        // 5. L2 normalize
        let normalized = l2_normalize(&embedding);

        // 6. Clear memory for next call
        ctx.clear_memory();

        Ok(normalized)
    }

    /// Produce embeddings for a batch of texts using native multi-sequence batching.
    ///
    /// Multiple texts are packed into a single llama.cpp batch with distinct
    /// sequence IDs, enabling the encoder to process them in one pass. When the
    /// total token count exceeds the context window (`n_ctx`), texts are split
    /// into sub-batches automatically.
    ///
    /// Note: unlike [`embed`], this method does **not** fall back to
    /// `get_embeddings` when `get_embeddings_seq` returns null. Per-sequence
    /// pooled embeddings are required, which is guaranteed when the context is
    /// created with `pooling_type = MEAN` (our default in `load_for_embedding`).
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        if texts.len() == 1 {
            return Ok(vec![self.embed(texts[0])?]);
        }

        let ctx = self
            .ctx
            .lock()
            .map_err(|e| InferenceError::LlamaCpp(format!("mutex poisoned: {}", e)))?;

        let n_embd = ctx.n_embd;
        let n_ctx = ctx.n_ctx;
        let n_seq_max = ctx.n_seq_max;

        // 1. Tokenize all texts, truncating to n_ctx
        let tokenized: Vec<Vec<i32>> = texts
            .iter()
            .map(|text| {
                let mut tokens = ctx.tokenize(text, true);
                if tokens.len() > n_ctx {
                    tokens.truncate(n_ctx);
                }
                tokens
            })
            .collect();

        let mut results: Vec<Vec<f32>> = vec![Vec::new(); texts.len()];

        // 2. Process in sub-batches that fit within n_ctx tokens and n_seq_max sequences
        let mut i = 0;
        while i < texts.len() {
            let mut total_tokens = 0usize;
            let mut batch_end = i;
            let mut non_empty = 0usize;

            while batch_end < texts.len() {
                let tok_len = tokenized[batch_end].len();
                if tok_len == 0 {
                    batch_end += 1;
                    continue;
                }
                if non_empty >= n_seq_max {
                    break;
                }
                if total_tokens + tok_len > n_ctx && non_empty > 0 {
                    break;
                }
                total_tokens += tok_len;
                non_empty += 1;
                batch_end += 1;
            }

            if batch_end == i {
                batch_end = i + 1;
                total_tokens = tokenized[i].len();
            }

            if non_empty == 0 {
                // All empty texts in this sub-batch
                for result in &mut results[i..batch_end] {
                    *result = vec![0.0f32; n_embd];
                }
            } else {
                // Allocate batch: total_tokens slots, 0 = token input, 1 seq_id per token
                let mut batch = ctx.api.batch_init(total_tokens as i32, 0, 1);

                let mut seq_id: i32 = 0;
                let mut token_offset = 0usize;
                let mut seq_map: Vec<(usize, i32)> = Vec::with_capacity(non_empty);

                for idx in i..batch_end {
                    if tokenized[idx].is_empty() {
                        results[idx] = vec![0.0f32; n_embd];
                        continue;
                    }

                    for (pos, &token) in tokenized[idx].iter().enumerate() {
                        // SAFETY: batch was allocated with total_tokens capacity via
                        // batch_init. token_offset < total_tokens is guaranteed by the
                        // sub-batch grouping logic above.
                        unsafe {
                            *batch.token.add(token_offset) = token;
                            *batch.pos.add(token_offset) = pos as i32;
                            *batch.n_seq_id.add(token_offset) = 1;
                            *(*batch.seq_id.add(token_offset)) = seq_id;
                            *batch.logits.add(token_offset) = 0;
                        }
                        token_offset += 1;
                    }

                    seq_map.push((idx, seq_id));
                    seq_id += 1;
                }

                batch.n_tokens = token_offset as i32;

                // 3. Run inference (single encode/decode for the entire sub-batch)
                let inference_result = if ctx.has_encoder {
                    ctx.api.encode(ctx.ctx, batch)
                } else {
                    ctx.api.decode(ctx.ctx, batch)
                };

                if let Err(e) = inference_result {
                    ctx.api.batch_free(batch);
                    ctx.clear_memory();
                    return Err(InferenceError::LlamaCpp(e));
                }

                // 4. Extract per-sequence embeddings
                for &(idx, sid) in &seq_map {
                    let emb_ptr = ctx.api.get_embeddings_seq(ctx.ctx, sid);
                    if emb_ptr.is_null() {
                        ctx.api.batch_free(batch);
                        ctx.clear_memory();
                        return Err(InferenceError::LlamaCpp(format!(
                            "get_embeddings_seq returned null for seq_id {sid}"
                        )));
                    }

                    // SAFETY: emb_ptr is non-null and points to n_embd floats owned
                    // by llama.cpp's context, which we hold via the Mutex lock.
                    let embedding = unsafe { std::slice::from_raw_parts(emb_ptr, n_embd) }.to_vec();
                    results[idx] = l2_normalize(&embedding);
                }

                ctx.api.batch_free(batch);
                ctx.clear_memory();
            }

            i = batch_end;
        }

        Ok(results)
    }

    /// The dimensionality of output embedding vectors.
    ///
    /// Returns 0 if the internal Mutex is poisoned (a thread panicked while
    /// holding the lock). In normal operation this cannot happen.
    pub fn embedding_dim(&self) -> usize {
        self.ctx.lock().map(|ctx| ctx.n_embd).unwrap_or(0)
    }

    /// The vocabulary size of the loaded model.
    ///
    /// Returns 0 if the internal Mutex is poisoned.
    pub fn vocab_size(&self) -> usize {
        self.ctx.lock().map(|ctx| ctx.vocab_size).unwrap_or(0)
    }

    /// Check whether the internal llama.cpp context is healthy.
    ///
    /// Returns `false` if the internal `Mutex` is poisoned (a thread panicked
    /// while holding the lock). A poisoned context should be discarded and
    /// the model reloaded fresh, as the llama.cpp state may be inconsistent.
    pub fn is_healthy(&self) -> bool {
        !self.ctx.is_poisoned()
    }
}

/// L2-normalize a vector, returning a new unit-length vector.
///
/// If the vector has zero norm (all zeros), a copy is returned unchanged.
/// NaN and infinity values are passed through without special handling.
fn l2_normalize(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter().map(|x| x / norm).collect()
    } else {
        v.to_vec()
    }
}

impl crate::InferenceEngine for EmbeddingEngine {
    fn embed(&self, text: &str) -> Result<Vec<f32>, crate::InferenceError> {
        EmbeddingEngine::embed(self, text)
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, crate::InferenceError> {
        EmbeddingEngine::embed_batch(self, texts)
    }

    fn supports_embed(&self) -> bool {
        true
    }

    fn embedding_dim(&self) -> usize {
        EmbeddingEngine::embedding_dim(self)
    }

    fn is_healthy(&self) -> bool {
        EmbeddingEngine::is_healthy(self)
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

    #[test]
    fn l2_normalize_with_nan_passes_through() {
        let v = vec![1.0, f32::NAN, 3.0];
        let n = l2_normalize(&v);
        assert_eq!(n.len(), 3);
        // NaN propagates: norm is NaN, so norm > 0.0 is false,
        // and the original vector is returned as-is
        assert!(n[1].is_nan(), "NaN should be preserved");
        assert_eq!(n[0], 1.0);
        assert_eq!(n[2], 3.0);
    }

    #[test]
    fn l2_normalize_with_infinity() {
        let v = vec![1.0, f32::INFINITY];
        let n = l2_normalize(&v);
        assert_eq!(n.len(), 2);
        // infinity norm: inf > 0.0 is true, so division happens
        // 1.0/inf = 0.0, inf/inf = NaN
        // This is acceptable "garbage in" behavior
    }

    #[test]
    fn l2_normalize_output_length_always_matches_input() {
        for len in [0, 1, 2, 3, 10, 100, 384, 768] {
            let v: Vec<f32> = (0..len).map(|i| i as f32).collect();
            let n = l2_normalize(&v);
            assert_eq!(
                n.len(),
                len,
                "output length should match input for len={len}"
            );
        }
    }

    #[test]
    fn l2_normalize_mixed_positive_negative_zero() {
        let v = vec![-2.0, 0.0, 3.0, 0.0, -1.0];
        let n = l2_normalize(&v);
        let norm: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "norm should be 1.0, got {norm}");
        // Zero elements stay zero
        assert_eq!(n[1], 0.0);
        assert_eq!(n[3], 0.0);
        // Signs preserved
        assert!(n[0] < 0.0);
        assert!(n[2] > 0.0);
        assert!(n[4] < 0.0);
    }

    // -----------------------------------------------------------------------
    // EmbeddingEngine constructor tests (no libllama needed for error paths)
    // -----------------------------------------------------------------------

    #[test]
    fn from_registry_known_model_not_local_returns_error_or_succeeds() {
        // Known model but not downloaded → Registry error with helpful message.
        // If the model is already on disk, this succeeds — that's fine.
        let result = EmbeddingEngine::from_registry("miniLM");
        if let Err(err) = result {
            let msg = err.to_string();
            assert!(
                matches!(
                    err,
                    InferenceError::Registry(_) | InferenceError::LlamaCpp(_)
                ),
                "should be Registry or LlamaCpp error, got: {msg}"
            );
        }
    }

    #[test]
    fn from_registry_unknown_model_returns_registry_error() {
        let result = EmbeddingEngine::from_registry("nonexistent-model");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, InferenceError::Registry(_)),
            "should be Registry error, got: {err}"
        );
        assert!(
            err.to_string().contains("Unknown model"),
            "error should mention unknown model: {err}"
        );
    }

    #[test]
    fn from_registry_empty_name_returns_error() {
        let result = EmbeddingEngine::from_registry("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Registry(_)));
    }

    #[test]
    fn from_gguf_nonexistent_file_returns_descriptive_error() {
        let result = EmbeddingEngine::from_gguf("/nonexistent/path/model.gguf");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        // Should be a LlamaCpp error (libllama not found or file not found)
        assert!(
            matches!(err, InferenceError::LlamaCpp(_)),
            "should be LlamaCpp error, got: {msg}"
        );
        assert!(!msg.is_empty(), "error message should not be empty");
    }

    #[test]
    fn from_gguf_accepts_path_types_and_all_return_errors() {
        // Verify the `impl AsRef<Path>` works with various types
        // All will fail (no libllama), but should compile and return errors
        let r1 = EmbeddingEngine::from_gguf("/tmp/model.gguf");
        let r2 = EmbeddingEngine::from_gguf(String::from("/tmp/model.gguf"));
        let r3 = EmbeddingEngine::from_gguf(std::path::PathBuf::from("/tmp/model.gguf"));
        assert!(r1.is_err(), "&str path should fail without libllama");
        assert!(r2.is_err(), "String path should fail without libllama");
        assert!(r3.is_err(), "PathBuf path should fail without libllama");
    }

    // -----------------------------------------------------------------------
    // Debug impl tests
    // -----------------------------------------------------------------------

    #[test]
    fn debug_format_contains_struct_name() {
        // We can't construct an EmbeddingEngine without libllama,
        // but we can verify the Debug impl compiles and is callable
        // by testing it indirectly through the error path.
        // The compile-time Send+Sync check already exercises the type.
        // Here we just verify the trait bound is satisfied.
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<EmbeddingEngine>();
    }

    // -----------------------------------------------------------------------
    // is_healthy() compile-time check
    // -----------------------------------------------------------------------

    #[test]
    fn is_healthy_method_exists() {
        // Compile-time check: is_healthy exists and returns bool.
        // Can't construct an engine without libllama, so just verify
        // the method signature compiles.
        fn _check(e: &EmbeddingEngine) -> bool {
            e.is_healthy()
        }
    }

    // -----------------------------------------------------------------------
    // Compile-time trait assertions
    // -----------------------------------------------------------------------

    #[test]
    fn embedding_engine_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EmbeddingEngine>();
    }

    // --- embed_batch compile-time checks (no libllama needed) ---

    #[test]
    fn embed_batch_signature_returns_correct_type() {
        // Compile-time check: embed_batch accepts &[&str] and returns
        // Result<Vec<Vec<f32>>, InferenceError>. Runtime behavior is
        // tested in the smoke tests below (requires libllama + model).
        fn _check(_e: &EmbeddingEngine) -> Result<Vec<Vec<f32>>, crate::InferenceError> {
            _e.embed_batch(&[])
        }
    }

    // --- Smoke tests: load miniLM and produce embeddings ---

    #[test]
    #[ignore]
    fn smoke_embed_minilm() {
        let engine = match EmbeddingEngine::from_registry("miniLM") {
            Ok(e) => e,
            Err(e) => {
                eprintln!("skipping smoke_embed_minilm: {e}");
                return;
            }
        };

        // Verify engine metadata
        assert_eq!(engine.embedding_dim(), 384, "MiniLM should have 384 dims");
        assert!(engine.vocab_size() > 0, "vocab_size should be > 0");

        // Embed a simple string
        let embedding = engine
            .embed("test")
            .expect("embed should succeed for a simple string");

        // MiniLM produces 384-dimensional embeddings
        assert_eq!(
            embedding.len(),
            384,
            "expected 384 dimensions, got {}",
            embedding.len()
        );

        // Verify L2 norm is approximately 1.0 (we normalize in embed())
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-4,
            "L2 norm should be ~1.0, got {norm}"
        );

        // Verify embedding is not all zeros (actual content was encoded)
        assert!(
            embedding.iter().any(|&x| x.abs() > 1e-6),
            "embedding should not be all zeros"
        );

        // Embed a second string and verify it differs from the first
        let embedding2 = engine
            .embed("completely different sentence about cats")
            .expect("second embed should succeed");
        assert_eq!(embedding2.len(), 384);
        let norm2: f32 = embedding2.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm2 - 1.0).abs() < 1e-4,
            "second embedding L2 norm should be ~1.0, got {norm2}"
        );

        // Cosine similarity between different texts should be < 1.0
        let cosine_sim: f32 = embedding
            .iter()
            .zip(embedding2.iter())
            .map(|(a, b)| a * b)
            .sum();
        assert!(
            cosine_sim < 0.99,
            "different texts should produce different embeddings, cosine_sim={cosine_sim}"
        );

        // Embed the same string again — should produce identical results (deterministic)
        let embedding_repeat = engine.embed("test").expect("repeat embed should succeed");
        for (i, (a, b)) in embedding.iter().zip(embedding_repeat.iter()).enumerate() {
            assert!(
                (a - b).abs() < 1e-6,
                "embedding should be deterministic, dim {i}: {a} vs {b}"
            );
        }
    }

    #[test]
    #[ignore]
    fn smoke_embed_batch_minilm() {
        let engine = match EmbeddingEngine::from_registry("miniLM") {
            Ok(e) => e,
            Err(e) => {
                eprintln!("skipping smoke_embed_batch_minilm: {e}");
                return;
            }
        };

        // --- Empty batch ---
        let empty = engine.embed_batch(&[]).expect("empty batch should succeed");
        assert!(empty.is_empty());

        // --- Single-element batch should match single embed ---
        let single_batch = engine
            .embed_batch(&["hello world"])
            .expect("single batch should succeed");
        let single = engine
            .embed("hello world")
            .expect("single embed should succeed");
        assert_eq!(single_batch.len(), 1);
        for (a, b) in single_batch[0].iter().zip(single.iter()) {
            assert!(
                (a - b).abs() < 1e-6,
                "single-batch should match single embed"
            );
        }

        // --- Multi-element batch with empties and duplicates ---
        let texts = vec![
            "The cat sat on the mat",
            "A dog ran through the park",
            "Machine learning is fascinating",
            "",                       // empty text
            "The cat sat on the mat", // duplicate of first
        ];
        let embeddings = engine
            .embed_batch(&texts)
            .expect("multi batch should succeed");
        assert_eq!(embeddings.len(), 5);

        // All embeddings (including empty) should have correct dimension
        for (i, emb) in embeddings.iter().enumerate() {
            assert_eq!(
                emb.len(),
                384,
                "text {i} should have 384 dims, got {}",
                emb.len()
            );
        }

        // Empty text should produce zero vector
        let zero_norm: f32 = embeddings[3].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            zero_norm < 1e-6,
            "empty text should produce zero vector, got norm={zero_norm}"
        );

        // Non-empty embeddings should be unit vectors
        for i in [0, 1, 2, 4] {
            let norm: f32 = embeddings[i].iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(
                (norm - 1.0).abs() < 1e-4,
                "text {i} should be unit vector, got norm={norm}"
            );
        }

        // Duplicate texts should produce identical embeddings
        for (a, b) in embeddings[0].iter().zip(embeddings[4].iter()) {
            assert!(
                (a - b).abs() < 1e-6,
                "duplicate texts should produce identical embeddings"
            );
        }

        // Different texts should produce different embeddings
        let cosine_01: f32 = embeddings[0]
            .iter()
            .zip(embeddings[1].iter())
            .map(|(a, b)| a * b)
            .sum();
        assert!(
            cosine_01 < 0.99,
            "different texts should differ, cosine={cosine_01}"
        );

        // --- Batch results must match individual embed() for every text ---
        // This is the critical correctness property: multi-sequence batching
        // must produce identical results to single-sequence processing.
        for (i, text) in texts.iter().enumerate() {
            if text.is_empty() {
                continue; // empty produces zero vector in both paths
            }
            let individual = engine.embed(text).expect("individual embed should succeed");
            for (j, (a, b)) in embeddings[i].iter().zip(individual.iter()).enumerate() {
                assert!(
                    (a - b).abs() < 1e-5,
                    "text[{i}] dim[{j}] batch={a} vs individual={b}"
                );
            }
        }

        // --- All-empty batch ---
        let all_empty = engine
            .embed_batch(&["", "", ""])
            .expect("all-empty batch should succeed");
        assert_eq!(all_empty.len(), 3);
        for (i, emb) in all_empty.iter().enumerate() {
            assert_eq!(emb.len(), 384, "empty text {i} should have 384 dims");
            let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(norm < 1e-6, "empty text {i} should be zero vector");
        }

        // --- Larger batch to exercise sub-batch splitting ---
        // MiniLM has n_ctx=512. With ~50-token texts, sub-batching triggers
        // at ~10 texts. Generate 20 distinct texts to force multiple sub-batches.
        let long_texts: Vec<String> = (0..20)
            .map(|i| {
                format!(
                    "This is test sentence number {} which contains enough words to \
                     generate a reasonable number of tokens for the embedding model \
                     to process during the sub-batch splitting test",
                    i
                )
            })
            .collect();
        let long_refs: Vec<&str> = long_texts.iter().map(|s| s.as_str()).collect();
        let batch_results = engine
            .embed_batch(&long_refs)
            .expect("large batch should succeed");
        assert_eq!(batch_results.len(), 20);

        // Verify each text in the large batch matches its individual embedding
        for (i, text) in long_refs.iter().enumerate() {
            let individual = engine.embed(text).expect("individual embed should succeed");
            assert_eq!(batch_results[i].len(), individual.len());
            for (j, (a, b)) in batch_results[i].iter().zip(individual.iter()).enumerate() {
                assert!(
                    (a - b).abs() < 1e-5,
                    "large batch text[{i}] dim[{j}] batch={a} vs individual={b}"
                );
            }
        }

        // --- Two texts that are both exactly at truncation boundary ---
        // Create a very long text that will be truncated to n_ctx
        let long_text = "word ".repeat(2000); // ~2000 tokens, well over n_ctx=512
        let batch_truncated = engine
            .embed_batch(&[&long_text, &long_text])
            .expect("truncated batch should succeed");
        assert_eq!(batch_truncated.len(), 2);
        // Both should produce identical embeddings after truncation
        for (a, b) in batch_truncated[0].iter().zip(batch_truncated[1].iter()) {
            assert!(
                (a - b).abs() < 1e-6,
                "identical truncated texts should produce identical embeddings"
            );
        }
        // And should match individual embed
        let individual_truncated = engine
            .embed(&long_text)
            .expect("individual truncated embed should succeed");
        for (j, (a, b)) in batch_truncated[0]
            .iter()
            .zip(individual_truncated.iter())
            .enumerate()
        {
            assert!(
                (a - b).abs() < 1e-5,
                "truncated batch dim[{j}] batch={a} vs individual={b}"
            );
        }
    }
}
