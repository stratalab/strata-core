//! Static extern declarations for bundled llama.cpp.
//!
//! These symbols are resolved at link time (not dlsym) when llama.cpp
//! is compiled from vendored source via `build.rs`.

use std::os::raw::c_char;

use super::ffi::*;

// b5440 uses llama_kv_self_clear(ctx) instead of the newer
// llama_get_memory(ctx) + llama_memory_clear(mem, data) API.
// These shims bridge the two: get_memory returns the ctx pointer itself,
// and memory_clear passes it back to llama_kv_self_clear.

/// Returns the ctx pointer as the "memory" handle.
pub unsafe extern "C" fn shim_get_memory(ctx: LlamaContext) -> LlamaMemory {
    ctx
}

/// Calls llama_kv_self_clear with the ctx pointer (passed as mem).
pub unsafe extern "C" fn shim_memory_clear(mem: LlamaMemory, _data: bool) {
    llama_kv_self_clear(mem as LlamaContext);
}

extern "C" {
    // Backend
    pub fn llama_backend_init();
    pub fn llama_backend_free();

    // Model
    pub fn llama_model_default_params() -> LlamaModelParams;
    pub fn llama_model_load_from_file(path: *const c_char, params: LlamaModelParams) -> LlamaModel;
    pub fn llama_model_free(model: LlamaModel);
    pub fn llama_model_get_vocab(model: LlamaModel) -> LlamaVocab;
    pub fn llama_model_n_embd(model: LlamaModel) -> i32;
    pub fn llama_model_n_ctx_train(model: LlamaModel) -> i32;
    pub fn llama_model_has_encoder(model: LlamaModel) -> bool;

    // Context
    pub fn llama_context_default_params() -> LlamaContextParams;
    pub fn llama_init_from_model(model: LlamaModel, params: LlamaContextParams) -> LlamaContext;
    pub fn llama_free(ctx: LlamaContext);

    // Memory (b5440 uses the older KV cache API — called via shims above)
    #[allow(dead_code)]
    pub fn llama_get_kv_self(ctx: LlamaContext) -> LlamaMemory;
    pub fn llama_kv_self_clear(ctx: LlamaContext);

    // Tokenize
    pub fn llama_tokenize(
        vocab: LlamaVocab,
        text: *const c_char,
        text_len: i32,
        tokens: *mut LlamaToken,
        n_tokens_max: i32,
        add_special: bool,
        parse_special: bool,
    ) -> i32;
    pub fn llama_token_to_piece(
        vocab: LlamaVocab,
        token: LlamaToken,
        buf: *mut c_char,
        length: i32,
        lstrip: i32,
        special: bool,
    ) -> i32;
    pub fn llama_detokenize(
        vocab: LlamaVocab,
        tokens: *const LlamaToken,
        n_tokens: i32,
        text: *mut c_char,
        text_len_max: i32,
        remove_special: bool,
        unparse_special: bool,
    ) -> i32;

    // Vocab
    pub fn llama_vocab_n_tokens(vocab: LlamaVocab) -> i32;
    pub fn llama_vocab_bos(vocab: LlamaVocab) -> LlamaToken;
    pub fn llama_vocab_eos(vocab: LlamaVocab) -> LlamaToken;
    pub fn llama_vocab_is_eog(vocab: LlamaVocab, token: LlamaToken) -> bool;

    // Batch
    pub fn llama_batch_get_one(tokens: *mut LlamaToken, n_tokens: i32) -> LlamaBatch;
    pub fn llama_batch_init(n_tokens: i32, embd: i32, n_seq_max: i32) -> LlamaBatch;
    pub fn llama_batch_free(batch: LlamaBatch);

    // Inference
    pub fn llama_encode(ctx: LlamaContext, batch: LlamaBatch) -> i32;
    pub fn llama_decode(ctx: LlamaContext, batch: LlamaBatch) -> i32;

    // Output
    pub fn llama_get_logits_ith(ctx: LlamaContext, i: i32) -> *mut f32;
    pub fn llama_get_embeddings(ctx: LlamaContext) -> *mut f32;
    pub fn llama_get_embeddings_ith(ctx: LlamaContext, i: i32) -> *mut f32;
    pub fn llama_get_embeddings_seq(ctx: LlamaContext, seq_id: LlamaSeqId) -> *mut f32;

    // Sampling
    pub fn llama_sampler_chain_init(params: LlamaSamplerChainParams) -> LlamaSampler;
    pub fn llama_sampler_chain_default_params() -> LlamaSamplerChainParams;
    pub fn llama_sampler_chain_add(chain: LlamaSampler, smpl: LlamaSampler);
    pub fn llama_sampler_sample(smpl: LlamaSampler, ctx: LlamaContext, idx: i32) -> LlamaToken;
    pub fn llama_sampler_free(smpl: LlamaSampler);
    pub fn llama_sampler_init_greedy() -> LlamaSampler;
    pub fn llama_sampler_init_dist(seed: u32) -> LlamaSampler;
    pub fn llama_sampler_init_top_k(k: i32) -> LlamaSampler;
    pub fn llama_sampler_init_top_p(p: f32, min_keep: usize) -> LlamaSampler;
    pub fn llama_sampler_init_temp(t: f32) -> LlamaSampler;
    pub fn llama_sampler_init_min_p(p: f32, min_keep: usize) -> LlamaSampler;
}
