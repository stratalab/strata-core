//! Local generation provider backed by llama.cpp.
//!
//! [`LocalProvider`] wraps a [`LlamaCppContext`] loaded for generation and
//! implements the autoregressive decode loop with sampler chain support.

use std::path::Path;

use crate::llama::context::LlamaCppContext;
use crate::llama::ffi::*;
use crate::{GenerateRequest, GenerateResponse, InferenceError, StopReason};

/// Local generation provider using llama.cpp.
///
/// Not thread-safe on its own — the caller (`GenerationEngine`) is responsible
/// for synchronization if needed.
pub(crate) struct LocalProvider {
    ctx: LlamaCppContext,
}

impl LocalProvider {
    /// Load from a GGUF file with an optional context size override.
    pub fn from_gguf(
        path: &Path,
        ctx_size: Option<usize>,
    ) -> Result<Self, InferenceError> {
        let ctx = LlamaCppContext::load_for_generation(path, ctx_size)?;
        Ok(Self { ctx })
    }

    /// Generate text from a prompt using the loaded model.
    ///
    /// Flow:
    /// 1. Tokenize prompt
    /// 2. Validate (non-empty, fits in context)
    /// 3. Clear memory state
    /// 4. Build sampler chain
    /// 5. Prefill prompt tokens
    /// 6. Autoregressive decode loop with stop condition checks
    /// 7. Cleanup (free sampler, clear memory)
    /// 8. Detokenize and return response
    pub fn generate(
        &mut self,
        request: &GenerateRequest,
    ) -> Result<GenerateResponse, InferenceError> {
        // 1. Tokenize prompt
        let mut prompt_tokens = self.ctx.tokenize(&request.prompt, true);
        let prompt_token_count = prompt_tokens.len();

        // 2. Validate
        if prompt_tokens.is_empty() {
            return Err(InferenceError::LlamaCpp(
                "prompt tokenized to empty sequence".to_string(),
            ));
        }

        if prompt_tokens.len() >= self.ctx.n_ctx {
            return Err(InferenceError::LlamaCpp(format!(
                "prompt length ({}) exceeds context size ({})",
                prompt_tokens.len(),
                self.ctx.n_ctx,
            )));
        }

        // Handle max_tokens=0 early
        if request.max_tokens == 0 {
            return Ok(GenerateResponse {
                text: String::new(),
                stop_reason: StopReason::MaxTokens,
                prompt_tokens: prompt_token_count,
                completion_tokens: 0,
            });
        }

        // 3. Clear any previous state
        self.ctx.clear_memory();

        // 4. Build sampler chain
        let sampler = self.build_sampler(request);

        // 5. Prefill: decode all prompt tokens at once
        let batch = self.ctx.api.batch_get_one(&mut prompt_tokens);
        if let Err(e) = self.ctx.api.decode(self.ctx.ctx, batch) {
            self.ctx.api.sampler_free(sampler);
            self.ctx.clear_memory();
            return Err(InferenceError::LlamaCpp(e));
        }

        // Sample first token
        let mut next_token =
            self.ctx.api.sampler_sample(sampler, self.ctx.ctx, -1);

        // 6. Decode loop
        let mut generated_ids: Vec<u32> = Vec::new();
        let mut stop_reason = StopReason::MaxTokens;
        let mut pos = prompt_tokens.len();

        for _step in 0..request.max_tokens {
            // Check EOG (end-of-generation) tokens
            if self.ctx.api.vocab_is_eog(self.ctx.vocab, next_token) {
                stop_reason = StopReason::StopToken;
                break;
            }

            // Check user-provided stop tokens
            if request.stop_tokens.contains(&(next_token as u32)) {
                stop_reason = StopReason::StopToken;
                break;
            }

            generated_ids.push(next_token as u32);

            pos += 1;
            if pos >= self.ctx.n_ctx {
                stop_reason = StopReason::ContextLength;
                break;
            }

            // Decode single token
            let mut single = [next_token];
            let batch = self.ctx.api.batch_get_one(&mut single);
            if let Err(e) = self.ctx.api.decode(self.ctx.ctx, batch) {
                self.ctx.api.sampler_free(sampler);
                self.ctx.clear_memory();
                return Err(InferenceError::LlamaCpp(e));
            }

            // Sample next token
            next_token = self.ctx.api.sampler_sample(sampler, self.ctx.ctx, -1);
        }

        // 7. Cleanup
        self.ctx.api.sampler_free(sampler);
        self.ctx.clear_memory();

        // 8. Detokenize
        let i32_ids: Vec<i32> = generated_ids.iter().map(|&id| id as i32).collect();
        let text = self.ctx.detokenize(&i32_ids);

        // Check stop_sequences against generated text
        let (final_text, final_reason) =
            check_stop_sequences(&text, &request.stop_sequences, stop_reason);

        Ok(GenerateResponse {
            text: final_text,
            stop_reason: final_reason,
            prompt_tokens: prompt_token_count,
            completion_tokens: generated_ids.len(),
        })
    }

    /// Build a llama.cpp sampler chain from request parameters.
    ///
    /// - `temperature <= 0.0` → greedy (argmax)
    /// - `temperature > 0.0` → optional top_k + optional top_p + temp + dist(seed)
    fn build_sampler(&self, request: &GenerateRequest) -> LlamaSampler {
        let chain_params = self.ctx.api.sampler_chain_default_params();
        let chain = self.ctx.api.sampler_chain_init(chain_params);

        if request.temperature <= 0.0 {
            // Greedy sampling
            self.ctx
                .api
                .sampler_chain_add(chain, self.ctx.api.sampler_init_greedy());
        } else {
            // Stochastic sampling chain
            if request.top_k > 0 {
                self.ctx.api.sampler_chain_add(
                    chain,
                    self.ctx.api.sampler_init_top_k(request.top_k as i32),
                );
            }
            if request.top_p < 1.0 {
                self.ctx.api.sampler_chain_add(
                    chain,
                    self.ctx.api.sampler_init_top_p(request.top_p, 1),
                );
            }
            self.ctx.api.sampler_chain_add(
                chain,
                self.ctx.api.sampler_init_temp(request.temperature),
            );
            let seed = request.seed.unwrap_or(0xFFFFFFFF) as u32;
            self.ctx.api.sampler_chain_add(
                chain,
                self.ctx.api.sampler_init_dist(seed),
            );
        }

        chain
    }

    /// Encode text to token IDs using the model's tokenizer.
    pub fn encode(&self, text: &str) -> Vec<u32> {
        self.ctx
            .tokenize(text, true)
            .into_iter()
            .map(|id| id as u32)
            .collect()
    }

    /// Decode token IDs back to text using the model's tokenizer.
    pub fn decode(&self, ids: &[u32]) -> String {
        let i32_ids: Vec<i32> = ids.iter().map(|&id| id as i32).collect();
        self.ctx.detokenize(&i32_ids)
    }

    /// The vocabulary size of the loaded model.
    pub fn vocab_size(&self) -> usize {
        self.ctx.vocab_size
    }

    /// The context size of the loaded model.
    pub fn context_size(&self) -> usize {
        self.ctx.n_ctx
    }
}

/// Check generated text against stop sequences.
///
/// If a stop sequence is found, the text is truncated before it and the
/// stop reason is changed to `StopToken`.
fn check_stop_sequences(
    text: &str,
    stop_sequences: &[String],
    original_reason: StopReason,
) -> (String, StopReason) {
    for seq in stop_sequences {
        if seq.is_empty() {
            continue;
        }
        if let Some(pos) = text.find(seq.as_str()) {
            return (text[..pos].to_string(), StopReason::StopToken);
        }
    }
    (text.to_string(), original_reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // check_stop_sequences unit tests (no libllama needed)
    // -----------------------------------------------------------------------

    #[test]
    fn stop_sequences_no_match_returns_original() {
        let (text, reason) =
            check_stop_sequences("Hello world", &["STOP".into()], StopReason::MaxTokens);
        assert_eq!(text, "Hello world");
        assert_eq!(reason, StopReason::MaxTokens);
    }

    #[test]
    fn stop_sequences_match_truncates_text() {
        let (text, reason) = check_stop_sequences(
            "Hello STOP world",
            &["STOP".into()],
            StopReason::MaxTokens,
        );
        assert_eq!(text, "Hello ");
        assert_eq!(reason, StopReason::StopToken);
    }

    #[test]
    fn stop_sequences_match_at_start_returns_empty() {
        let (text, reason) = check_stop_sequences(
            "STOP world",
            &["STOP".into()],
            StopReason::MaxTokens,
        );
        assert_eq!(text, "");
        assert_eq!(reason, StopReason::StopToken);
    }

    #[test]
    fn stop_sequences_match_at_end() {
        let (text, reason) = check_stop_sequences(
            "Hello world\n\n",
            &["\n\n".into()],
            StopReason::MaxTokens,
        );
        assert_eq!(text, "Hello world");
        assert_eq!(reason, StopReason::StopToken);
    }

    #[test]
    fn stop_sequences_first_match_wins() {
        let (text, reason) = check_stop_sequences(
            "Hello STOP1 and STOP2 end",
            &["STOP2".into(), "STOP1".into()],
            StopReason::MaxTokens,
        );
        // STOP2 is checked first in iteration order, but STOP1 appears earlier in text.
        // Actually, we iterate stop_sequences in order. STOP2 appears at position 16,
        // STOP1 appears at position 6. STOP2 is found first in the iteration.
        assert_eq!(text, "Hello STOP1 and ");
        assert_eq!(reason, StopReason::StopToken);
    }

    #[test]
    fn stop_sequences_empty_list_returns_original() {
        let (text, reason) =
            check_stop_sequences("Hello world", &[], StopReason::ContextLength);
        assert_eq!(text, "Hello world");
        assert_eq!(reason, StopReason::ContextLength);
    }

    #[test]
    fn stop_sequences_empty_string_in_list_skipped() {
        let (text, reason) = check_stop_sequences(
            "Hello world",
            &[String::new(), "STOP".into()],
            StopReason::MaxTokens,
        );
        assert_eq!(text, "Hello world");
        assert_eq!(reason, StopReason::MaxTokens);
    }

    #[test]
    fn stop_sequences_on_empty_text() {
        let (text, reason) =
            check_stop_sequences("", &["STOP".into()], StopReason::MaxTokens);
        assert_eq!(text, "");
        assert_eq!(reason, StopReason::MaxTokens);
    }

    #[test]
    fn stop_sequences_preserves_cancelled_reason_when_no_match() {
        let (text, reason) =
            check_stop_sequences("Hello world", &["STOP".into()], StopReason::Cancelled);
        assert_eq!(text, "Hello world");
        assert_eq!(reason, StopReason::Cancelled);
    }

    #[test]
    fn stop_sequences_overrides_reason_on_match() {
        // Even if original reason was ContextLength, finding a stop sequence
        // changes it to StopToken
        let (text, reason) = check_stop_sequences(
            "Hello STOP",
            &["STOP".into()],
            StopReason::ContextLength,
        );
        assert_eq!(text, "Hello ");
        assert_eq!(reason, StopReason::StopToken);
    }

    #[test]
    fn stop_sequences_newline_sequence() {
        let (text, reason) = check_stop_sequences(
            "Line 1\nLine 2\n\nLine 3",
            &["\n\n".into()],
            StopReason::MaxTokens,
        );
        assert_eq!(text, "Line 1\nLine 2");
        assert_eq!(reason, StopReason::StopToken);
    }

    #[test]
    fn stop_sequences_unicode() {
        let (text, reason) = check_stop_sequences(
            "Hello 世界 end",
            &["世界".into()],
            StopReason::MaxTokens,
        );
        assert_eq!(text, "Hello ");
        assert_eq!(reason, StopReason::StopToken);
    }
}
