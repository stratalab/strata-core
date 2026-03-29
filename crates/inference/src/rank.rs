//! Ranking engine: cross-encoder reranking via llama.cpp.
//!
//! [`RankingEngine`] provides a high-level API for scoring (query, passage) pairs
//! using a cross-encoder model. Used by the reranking stage to re-score candidates
//! after initial retrieval.
//!
//! The pipeline: for each passage, tokenize (query, passage) → truncate → encode →
//! extract relevance score.
//!
//! Thread-safe via internal `Mutex` (llama.cpp contexts are not thread-safe).

use std::path::Path;
use std::sync::Mutex;

use tracing::info;

use crate::llama::context::LlamaCppContext;
use crate::InferenceError;

/// High-level ranking engine backed by llama.cpp.
///
/// Wraps a GGUF cross-encoder model, exposing a simple
/// `rank(query, passages) -> Vec<f32>` interface. Thread-safe via internal `Mutex`.
///
/// # Example
///
/// ```no_run
/// use strata_inference::RankingEngine;
///
/// let engine = RankingEngine::from_gguf("reranker.gguf")?;
/// let scores = engine.rank("what is rust?", &["Rust is a language", "Python is great"])?;
/// assert_eq!(scores.len(), 2);
/// # Ok::<(), strata_inference::InferenceError>(())
/// ```
pub struct RankingEngine {
    ctx: Mutex<LlamaCppContext>,
}

impl std::fmt::Debug for RankingEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vocab_size = match self.ctx.lock() {
            Ok(ctx) => ctx.vocab_size,
            Err(_) => 0,
        };
        f.debug_struct("RankingEngine")
            .field("vocab_size", &vocab_size)
            .finish()
    }
}

impl RankingEngine {
    /// Load a ranking engine from a GGUF file.
    ///
    /// The model is loaded with `embeddings=true` and `pooling_type=RANK`.
    pub fn from_gguf(path: impl AsRef<Path>) -> Result<Self, InferenceError> {
        let path = path.as_ref();
        info!(path = %path.display(), "Loading ranking engine from GGUF");
        let ctx = LlamaCppContext::load_for_ranking(path)?;
        info!(
            vocab_size = ctx.vocab_size,
            n_ctx = ctx.n_ctx,
            "Ranking engine ready"
        );
        Ok(Self {
            ctx: Mutex::new(ctx),
        })
    }

    /// Load a ranking engine by model name from the registry.
    ///
    /// Resolves the name (e.g., `"jina-reranker-v1-tiny"`) to a local GGUF file path,
    /// automatically downloading from HuggingFace if the model is not
    /// present locally.
    pub fn from_registry(name: &str) -> Result<Self, InferenceError> {
        let registry = crate::registry::ModelRegistry::new();

        #[cfg(feature = "download")]
        let path = registry.resolve_or_pull(name)?;

        #[cfg(not(feature = "download"))]
        let path = registry.resolve(name)?;

        match Self::from_gguf(&path) {
            Ok(engine) => Ok(engine),
            Err(e) => {
                registry.check_and_clean_corrupt(name, &path);
                Err(e)
            }
        }
    }

    /// Score each passage against the query using cross-encoder inference.
    ///
    /// Returns one relevance score per passage. Higher scores indicate
    /// greater relevance. Passages are scored independently — each
    /// (query, passage) pair is a separate inference call.
    ///
    /// # Arguments
    ///
    /// * `query` — the search query
    /// * `passages` — candidate passages to score
    ///
    /// # Returns
    ///
    /// A `Vec<f32>` of length `passages.len()`, where `scores[i]` is the
    /// relevance score for `passages[i]`.
    pub fn rank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, InferenceError> {
        if passages.is_empty() {
            return Ok(Vec::new());
        }

        let ctx = self
            .ctx
            .lock()
            .map_err(|e| InferenceError::LlamaCpp(format!("mutex poisoned: {}", e)))?;

        let mut scores = Vec::with_capacity(passages.len());

        for passage in passages {
            let score = Self::rank_single(&ctx, query, passage)?;
            scores.push(score);
        }

        Ok(scores)
    }

    /// Score a single (query, passage) pair.
    fn rank_single(
        ctx: &LlamaCppContext,
        query: &str,
        passage: &str,
    ) -> Result<f32, InferenceError> {
        // Cross-encoder models expect: [CLS] query [SEP] passage [SEP]
        // We tokenize query and passage separately, then join them.
        let query_tokens = ctx.tokenize(query, true); // adds [CLS] ... [SEP]
        let passage_tokens = ctx.tokenize(passage, true); // adds [CLS] ... [SEP]

        if query_tokens.is_empty() && passage_tokens.is_empty() {
            return Ok(0.0);
        }

        // Build combined token sequence: [CLS] query [SEP] passage [SEP]
        // Strip the leading [CLS] from the passage tokens only when the
        // query already provides one (non-empty query_tokens starts with BOS).
        let mut tokens = query_tokens;
        let has_query_bos = !tokens.is_empty() && tokens[0] == ctx.bos_id;
        let passage_start =
            if has_query_bos && !passage_tokens.is_empty() && passage_tokens[0] == ctx.bos_id {
                1
            } else {
                0
            };
        tokens.extend_from_slice(&passage_tokens[passage_start..]);

        // Truncate to context size, preserving the trailing [SEP] when
        // possible so the model sees a proper segment boundary.
        if tokens.len() > ctx.n_ctx {
            let last_token = tokens.last().copied();
            tokens.truncate(ctx.n_ctx);
            // If truncation removed the trailing SEP (eos), replace the
            // last token with it so the model sees a complete segment.
            if let Some(sep) = last_token {
                if sep == ctx.eos_id && tokens.last().copied() != Some(sep) {
                    if let Some(last) = tokens.last_mut() {
                        *last = sep;
                    }
                }
            }
        }

        // Run inference
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

        // Extract relevance score.
        // With LLAMA_POOLING_TYPE_RANK, get_embeddings_seq returns a pointer
        // to a single float: the relevance score for this sequence.
        let score_ptr = ctx.api.get_embeddings_seq(ctx.ctx, 0);
        if score_ptr.is_null() {
            ctx.clear_memory();
            return Err(InferenceError::LlamaCpp(
                "get_embeddings_seq returned null — model may not support ranking".to_string(),
            ));
        }

        // SAFETY: score_ptr is non-null, points to at least 1 float owned by
        // llama.cpp's context, which we hold via the Mutex lock.
        let score = unsafe { *score_ptr };

        // Clear memory for next call
        ctx.clear_memory();

        Ok(score)
    }

    /// Check whether the internal llama.cpp context is healthy.
    pub fn is_healthy(&self) -> bool {
        !self.ctx.is_poisoned()
    }
}

impl crate::InferenceEngine for RankingEngine {
    fn rank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, crate::InferenceError> {
        RankingEngine::rank(self, query, passages)
    }

    fn supports_rank(&self) -> bool {
        true
    }
}

// Compile-time verify Send + Sync.
const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_both() {
        assert_send::<RankingEngine>();
        assert_sync::<RankingEngine>();
    }
    let _ = assert_both;
};

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // RankingEngine constructor tests (no libllama needed for error paths)
    // -----------------------------------------------------------------------

    #[test]
    fn from_registry_unknown_model_returns_registry_error() {
        let result = RankingEngine::from_registry("nonexistent-reranker");
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
        let result = RankingEngine::from_registry("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Registry(_)));
    }

    #[test]
    fn from_gguf_nonexistent_file_returns_descriptive_error() {
        let result = RankingEngine::from_gguf("/nonexistent/path/reranker.gguf");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, InferenceError::LlamaCpp(_)),
            "should be LlamaCpp error, got: {msg}"
        );
        assert!(!msg.is_empty(), "error message should not be empty");
    }

    #[test]
    fn from_gguf_accepts_path_types() {
        let r1 = RankingEngine::from_gguf("/tmp/reranker.gguf");
        let r2 = RankingEngine::from_gguf(String::from("/tmp/reranker.gguf"));
        let r3 = RankingEngine::from_gguf(std::path::PathBuf::from("/tmp/reranker.gguf"));
        assert!(r1.is_err(), "&str path should fail without libllama");
        assert!(r2.is_err(), "String path should fail without libllama");
        assert!(r3.is_err(), "PathBuf path should fail without libllama");
    }

    // -----------------------------------------------------------------------
    // Compile-time trait assertions
    // -----------------------------------------------------------------------

    #[test]
    fn ranking_engine_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RankingEngine>();
    }

    #[test]
    fn debug_format_compiles() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<RankingEngine>();
    }

    #[test]
    fn is_healthy_method_exists() {
        fn _check(e: &RankingEngine) -> bool {
            e.is_healthy()
        }
    }

    #[test]
    fn rank_signature_returns_correct_type() {
        fn _check(e: &RankingEngine) -> Result<Vec<f32>, crate::InferenceError> {
            e.rank("query", &[])
        }
    }

    /// Verify RankingEngine does NOT support embed or generate via the trait.
    #[test]
    fn trait_does_not_support_embed_or_generate() {
        // Compile-time proof: these trait methods exist and return the
        // expected defaults. Can't construct an engine without libllama,
        // so we verify via function signatures.
        fn _check_embed(e: &dyn crate::InferenceEngine) -> bool {
            !e.supports_embed() && !e.supports_generate() && e.supports_rank()
        }
        // Also verify the return type signatures compile
        fn _check_embed_err(
            e: &dyn crate::InferenceEngine,
        ) -> Result<Vec<f32>, crate::InferenceError> {
            e.embed("text")?;
            unreachable!()
        }
        let _ = _check_embed;
        let _ = _check_embed_err;
    }

    // -----------------------------------------------------------------------
    // Smoke tests (require libllama + downloaded model)
    // -----------------------------------------------------------------------

    #[test]
    #[ignore]
    fn smoke_rank_jina_reranker() {
        let engine = match RankingEngine::from_registry("jina-reranker-v1-tiny") {
            Ok(e) => e,
            Err(e) => {
                eprintln!("skipping smoke_rank_jina_reranker: {e}");
                return;
            }
        };

        // Empty passages
        let empty = engine.rank("test", &[]).expect("empty should succeed");
        assert!(empty.is_empty());

        // Single passage
        let scores = engine
            .rank("what is rust?", &["Rust is a systems programming language"])
            .expect("single passage should succeed");
        assert_eq!(scores.len(), 1);
        assert!(scores[0].is_finite(), "score should be finite");

        // Multiple passages — relevant one should score higher
        let passages = &[
            "Rust is a systems programming language focused on safety",
            "The weather today is sunny with a high of 75",
            "Rust provides memory safety without garbage collection",
        ];
        let scores = engine
            .rank("what is rust?", passages)
            .expect("multi passage should succeed");
        assert_eq!(scores.len(), 3);
        for (i, s) in scores.iter().enumerate() {
            assert!(s.is_finite(), "score[{i}] should be finite: {s}");
        }
        // Relevant passages (0, 2) should score higher than irrelevant (1)
        assert!(
            scores[0] > scores[1],
            "relevant passage should score higher: {} vs {}",
            scores[0],
            scores[1]
        );
        assert!(
            scores[2] > scores[1],
            "relevant passage should score higher: {} vs {}",
            scores[2],
            scores[1]
        );

        // Same query scored twice should produce deterministic results
        let scores2 = engine
            .rank("what is rust?", passages)
            .expect("repeat should succeed");
        for (i, (a, b)) in scores.iter().zip(scores2.iter()).enumerate() {
            assert!(
                (a - b).abs() < 1e-6,
                "scores should be deterministic, passage {i}: {a} vs {b}"
            );
        }
    }
}
