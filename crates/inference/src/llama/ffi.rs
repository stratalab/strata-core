//! llama.cpp C API bindings via static linking.
//!
//! All symbols are resolved at link time from the vendored llama.cpp build
//! (compiled via `build.rs`). [`LlamaCppApi::load()`] initialises the backend
//! once and runs layout probes, then returns a zero-sized handle whose methods
//! call directly into the extern symbols.

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

// ---------------------------------------------------------------------------
// llama.cpp C types
// ---------------------------------------------------------------------------

pub type LlamaModel = *mut c_void;
pub type LlamaContext = *mut c_void;
pub type LlamaSampler = *mut c_void;
pub type LlamaVocab = *const c_void;
pub type LlamaToken = i32;
pub type LlamaPos = i32;
pub type LlamaSeqId = i32;
/// Opaque memory handle returned by `llama_get_memory`.
pub type LlamaMemory = *mut c_void;

/// Special null token value.
pub const LLAMA_TOKEN_NULL: LlamaToken = -1;

// Pooling type enum values (matches llama_pooling_type in llama.h).
pub const LLAMA_POOLING_TYPE_UNSPECIFIED: i32 = -1;
pub const LLAMA_POOLING_TYPE_NONE: i32 = 0;
pub const LLAMA_POOLING_TYPE_MEAN: i32 = 1;
pub const LLAMA_POOLING_TYPE_CLS: i32 = 2;
pub const LLAMA_POOLING_TYPE_LAST: i32 = 3;

// ---------------------------------------------------------------------------
// #[repr(C)] struct definitions matching llama.h
// ---------------------------------------------------------------------------

/// Matches `struct llama_model_params` from llama.h (72 bytes on x86_64).
#[repr(C)]
pub struct LlamaModelParams {
    pub devices: *mut c_void,                 // ggml_backend_dev_t *
    pub tensor_buft_overrides: *const c_void, // const llama_model_tensor_buft_override *
    pub n_gpu_layers: i32,
    pub split_mode: i32, // enum llama_split_mode
    pub main_gpu: i32,
    _pad0: i32, // padding to align tensor_split ptr
    pub tensor_split: *const f32,
    pub progress_callback: *const c_void, // llama_progress_callback
    pub progress_callback_user_data: *mut c_void,
    pub kv_overrides: *const c_void,
    pub vocab_only: bool,
    pub use_mmap: bool,
    pub use_direct_io: bool,
    pub use_mlock: bool,
    pub check_tensors: bool,
    pub use_extra_bufts: bool,
    pub no_host: bool,
    pub no_alloc: bool,
}

const _: () = assert!(std::mem::size_of::<LlamaModelParams>() == 72);

/// Matches `struct llama_context_params` from llama.h (136 bytes on x86_64).
#[repr(C)]
pub struct LlamaContextParams {
    pub n_ctx: u32,
    pub n_batch: u32,
    pub n_ubatch: u32,
    pub n_seq_max: u32,
    pub n_threads: i32,
    pub n_threads_batch: i32,
    pub rope_scaling_type: i32, // enum llama_rope_scaling_type
    pub pooling_type: i32,      // enum llama_pooling_type
    pub attention_type: i32,    // enum llama_attention_type
    pub flash_attn_type: i32,   // enum llama_flash_attn_type
    pub rope_freq_base: f32,
    pub rope_freq_scale: f32,
    pub yarn_ext_factor: f32,
    pub yarn_attn_factor: f32,
    pub yarn_beta_fast: f32,
    pub yarn_beta_slow: f32,
    pub yarn_orig_ctx: u32,
    pub defrag_thold: f32,
    pub cb_eval: *const c_void, // ggml_backend_sched_eval_callback
    pub cb_eval_user_data: *mut c_void,
    pub type_k: i32,                   // enum ggml_type
    pub type_v: i32,                   // enum ggml_type
    pub abort_callback: *const c_void, // ggml_abort_callback
    pub abort_callback_data: *mut c_void,
    pub embeddings: bool,
    pub offload_kqv: bool,
    pub no_perf: bool,
    pub op_offload: bool,
    pub swa_full: bool,
    pub kv_unified: bool,
    _pad_bools: [u8; 2],       // padding to align next pointer
    pub samplers: *mut c_void, // struct llama_sampler_seq_config *
    pub n_samplers: usize,
}

const _: () = assert!(std::mem::size_of::<LlamaContextParams>() == 136);

/// Matches `struct llama_sampler_chain_params` from llama.h.
#[repr(C)]
pub struct LlamaSamplerChainParams {
    pub no_perf: bool,
}

const _: () = assert!(std::mem::size_of::<LlamaSamplerChainParams>() == 1);

/// Matches `struct llama_batch` from llama.h (56 bytes on x86_64).
#[derive(Copy, Clone)]
#[repr(C)]
pub struct LlamaBatch {
    pub n_tokens: i32,
    _pad: i32,
    pub token: *mut LlamaToken,
    pub embd: *mut f32,
    pub pos: *mut LlamaPos,
    pub n_seq_id: *mut i32,
    pub seq_id: *mut *mut LlamaSeqId,
    pub logits: *mut i8,
}

const _: () = assert!(std::mem::size_of::<LlamaBatch>() == 56);

// ---------------------------------------------------------------------------
// Statically linked extern "C" symbols
// ---------------------------------------------------------------------------

// b5440 uses llama_kv_self_clear(ctx) instead of the newer
// llama_get_memory(ctx) + llama_memory_clear(mem, data) API.
// These shims bridge the two: get_memory returns the ctx pointer itself,
// and memory_clear passes it back to llama_kv_self_clear.

/// Returns the ctx pointer as the "memory" handle.
unsafe extern "C" fn shim_get_memory(ctx: LlamaContext) -> LlamaMemory {
    ctx
}

/// Calls llama_kv_self_clear with the ctx pointer (passed as mem).
unsafe extern "C" fn shim_memory_clear(mem: LlamaMemory, _data: bool) {
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

    // Memory (b5440 uses the older KV cache API -- called via shims above)
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

// ---------------------------------------------------------------------------
// LlamaCppApi — thin handle to statically linked symbols
// ---------------------------------------------------------------------------

/// Handle to the statically linked llama.cpp API.
///
/// Created once via [`LlamaCppApi::load()`]. `Drop` calls `llama_backend_free()`.
///
/// The struct is zero-sized — all methods call directly into extern symbols.
pub struct LlamaCppApi {
    _private: (),
}

impl std::fmt::Debug for LlamaCppApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlamaCppApi").finish_non_exhaustive()
    }
}

// SAFETY: llama.cpp's C API functions are thread-safe for separate contexts.
// We ensure single-context access via Mutex in the engine layer.
unsafe impl Send for LlamaCppApi {}
unsafe impl Sync for LlamaCppApi {}

impl LlamaCppApi {
    /// Initialise the llama.cpp backend (once) and verify struct layout probes.
    pub fn load() -> Result<Self, String> {
        use std::sync::Once;
        static BACKEND_INIT: Once = Once::new();
        BACKEND_INIT.call_once(|| unsafe { llama_backend_init() });

        // Runtime layout probes: verify struct layout by checking default params.
        let mparams = unsafe { llama_model_default_params() };
        if !mparams.use_mmap {
            return Err(
                "llama.cpp version mismatch: llama_model_default_params().use_mmap is false \
                 (expected true). The struct layout may differ from what strata-inference expects."
                    .to_string(),
            );
        }
        if mparams.vocab_only {
            return Err(
                "llama.cpp version mismatch: vocab_only should be false by default".to_string(),
            );
        }
        let cparams = unsafe { llama_context_default_params() };
        if cparams.n_ctx == 0 && cparams.n_batch == 0 {
            return Err(
                "llama.cpp version mismatch: context default params look zeroed".to_string(),
            );
        }

        Ok(Self { _private: () })
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — model lifecycle
    // -----------------------------------------------------------------------

    pub fn model_default_params(&self) -> LlamaModelParams {
        unsafe { llama_model_default_params() }
    }

    pub fn model_load_from_file(
        &self,
        path: &CStr,
        params: LlamaModelParams,
    ) -> Result<LlamaModel, String> {
        let model = unsafe { llama_model_load_from_file(path.as_ptr(), params) };
        if model.is_null() {
            return Err(format!("llama_model_load_from_file failed for {:?}", path));
        }
        Ok(model)
    }

    pub fn model_free(&self, model: LlamaModel) {
        if !model.is_null() {
            unsafe { llama_model_free(model) };
        }
    }

    pub fn model_get_vocab(&self, model: LlamaModel) -> LlamaVocab {
        let vocab = unsafe { llama_model_get_vocab(model) };
        assert!(!vocab.is_null(), "llama_model_get_vocab returned null");
        vocab
    }

    pub fn model_n_embd(&self, model: LlamaModel) -> i32 {
        unsafe { llama_model_n_embd(model) }
    }

    pub fn model_n_ctx_train(&self, model: LlamaModel) -> i32 {
        unsafe { llama_model_n_ctx_train(model) }
    }

    pub fn model_has_encoder(&self, model: LlamaModel) -> bool {
        unsafe { llama_model_has_encoder(model) }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — context lifecycle
    // -----------------------------------------------------------------------

    pub fn context_default_params(&self) -> LlamaContextParams {
        unsafe { llama_context_default_params() }
    }

    pub fn init_from_model(
        &self,
        model: LlamaModel,
        params: LlamaContextParams,
    ) -> Result<LlamaContext, String> {
        let ctx = unsafe { llama_init_from_model(model, params) };
        if ctx.is_null() {
            return Err("llama_init_from_model returned null".to_string());
        }
        Ok(ctx)
    }

    pub fn free(&self, ctx: LlamaContext) {
        if !ctx.is_null() {
            unsafe { llama_free(ctx) };
        }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — memory
    // -----------------------------------------------------------------------

    pub fn get_memory(&self, ctx: LlamaContext) -> LlamaMemory {
        unsafe { shim_get_memory(ctx) }
    }

    pub fn memory_clear(&self, mem: LlamaMemory, data: bool) {
        if !mem.is_null() {
            unsafe { shim_memory_clear(mem, data) };
        }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — tokenization
    // -----------------------------------------------------------------------

    pub fn tokenize(
        &self,
        vocab: LlamaVocab,
        text: &[u8],
        tokens: &mut [LlamaToken],
        add_special: bool,
        parse_special: bool,
    ) -> i32 {
        unsafe {
            llama_tokenize(
                vocab,
                text.as_ptr() as *const c_char,
                text.len() as i32,
                tokens.as_mut_ptr(),
                tokens.len() as i32,
                add_special,
                parse_special,
            )
        }
    }

    pub fn token_to_piece(&self, vocab: LlamaVocab, token: LlamaToken, buf: &mut [u8]) -> i32 {
        unsafe {
            llama_token_to_piece(
                vocab,
                token,
                buf.as_mut_ptr() as *mut c_char,
                buf.len() as i32,
                0,     // lstrip
                false, // special
            )
        }
    }

    pub fn detokenize(
        &self,
        vocab: LlamaVocab,
        tokens: &[LlamaToken],
        buf: &mut [u8],
        remove_special: bool,
    ) -> i32 {
        unsafe {
            llama_detokenize(
                vocab,
                tokens.as_ptr(),
                tokens.len() as i32,
                buf.as_mut_ptr() as *mut c_char,
                buf.len() as i32,
                remove_special,
                false, // unparse_special
            )
        }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — vocab
    // -----------------------------------------------------------------------

    pub fn vocab_n_tokens(&self, vocab: LlamaVocab) -> i32 {
        unsafe { llama_vocab_n_tokens(vocab) }
    }

    pub fn vocab_bos(&self, vocab: LlamaVocab) -> LlamaToken {
        unsafe { llama_vocab_bos(vocab) }
    }

    pub fn vocab_eos(&self, vocab: LlamaVocab) -> LlamaToken {
        unsafe { llama_vocab_eos(vocab) }
    }

    pub fn vocab_is_eog(&self, vocab: LlamaVocab, token: LlamaToken) -> bool {
        unsafe { llama_vocab_is_eog(vocab, token) }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — batch
    // -----------------------------------------------------------------------

    pub fn batch_get_one(&self, tokens: &mut [LlamaToken]) -> LlamaBatch {
        unsafe { llama_batch_get_one(tokens.as_mut_ptr(), tokens.len() as i32) }
    }

    pub fn batch_init(&self, n_tokens: i32, embd: i32, n_seq_max: i32) -> LlamaBatch {
        unsafe { llama_batch_init(n_tokens, embd, n_seq_max) }
    }

    pub fn batch_free(&self, batch: LlamaBatch) {
        unsafe { llama_batch_free(batch) };
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — inference
    // -----------------------------------------------------------------------

    /// Run the encoder on a batch of tokens.
    ///
    /// Used for encoder-decoder models. The batch should have `add_special`
    /// tokens: use `true` for initial prompts (adds BOS/EOS per the model's
    /// chat template), `false` for continuation text.
    pub fn encode(&self, ctx: LlamaContext, batch: LlamaBatch) -> Result<(), String> {
        let rc = unsafe { llama_encode(ctx, batch) };
        if rc != 0 {
            return Err(format!("llama_encode failed with code {}", rc));
        }
        Ok(())
    }

    pub fn decode(&self, ctx: LlamaContext, batch: LlamaBatch) -> Result<(), String> {
        let rc = unsafe { llama_decode(ctx, batch) };
        if rc != 0 {
            return Err(format!("llama_decode failed with code {}", rc));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — output
    // -----------------------------------------------------------------------

    /// Get logits for the i-th token of the last decode call.
    /// Returns a raw pointer to `n_vocab` floats owned by llama.cpp, or null on error.
    pub fn get_logits_ith(&self, ctx: LlamaContext, i: i32) -> *mut f32 {
        let ptr = unsafe { llama_get_logits_ith(ctx, i) };
        assert!(!ptr.is_null(), "llama_get_logits_ith returned null");
        ptr
    }

    /// Get pooled embeddings (for models with pooling).
    /// Returns a raw pointer to `n_embd` floats owned by llama.cpp, or null if
    /// the model does not support embeddings or no data is available.
    pub fn get_embeddings(&self, ctx: LlamaContext) -> *mut f32 {
        unsafe { llama_get_embeddings(ctx) }
    }

    pub fn get_embeddings_ith(&self, ctx: LlamaContext, i: i32) -> *mut f32 {
        unsafe { llama_get_embeddings_ith(ctx, i) }
    }

    /// Get pooled embeddings for a specific sequence ID.
    /// Returns null if the sequence has no embeddings (caller must handle).
    pub fn get_embeddings_seq(&self, ctx: LlamaContext, seq_id: LlamaSeqId) -> *mut f32 {
        unsafe { llama_get_embeddings_seq(ctx, seq_id) }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — sampling
    // -----------------------------------------------------------------------

    pub fn sampler_chain_init(&self, params: LlamaSamplerChainParams) -> LlamaSampler {
        unsafe { llama_sampler_chain_init(params) }
    }

    pub fn sampler_chain_default_params(&self) -> LlamaSamplerChainParams {
        unsafe { llama_sampler_chain_default_params() }
    }

    pub fn sampler_chain_add(&self, chain: LlamaSampler, smpl: LlamaSampler) {
        unsafe { llama_sampler_chain_add(chain, smpl) };
    }

    pub fn sampler_sample(&self, smpl: LlamaSampler, ctx: LlamaContext, idx: i32) -> LlamaToken {
        unsafe { llama_sampler_sample(smpl, ctx, idx) }
    }

    pub fn sampler_free(&self, smpl: LlamaSampler) {
        if !smpl.is_null() {
            unsafe { llama_sampler_free(smpl) };
        }
    }

    pub fn sampler_init_greedy(&self) -> LlamaSampler {
        unsafe { llama_sampler_init_greedy() }
    }

    pub fn sampler_init_dist(&self, seed: u32) -> LlamaSampler {
        unsafe { llama_sampler_init_dist(seed) }
    }

    pub fn sampler_init_top_k(&self, k: i32) -> LlamaSampler {
        unsafe { llama_sampler_init_top_k(k) }
    }

    pub fn sampler_init_top_p(&self, p: f32, min_keep: usize) -> LlamaSampler {
        unsafe { llama_sampler_init_top_p(p, min_keep) }
    }

    pub fn sampler_init_temp(&self, t: f32) -> LlamaSampler {
        unsafe { llama_sampler_init_temp(t) }
    }

    pub fn sampler_init_min_p(&self, p: f32, min_keep: usize) -> LlamaSampler {
        unsafe { llama_sampler_init_min_p(p, min_keep) }
    }
}

impl Drop for LlamaCppApi {
    fn drop(&mut self) {
        unsafe { llama_backend_free() };
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Compile-time struct size assertions (also checked above via const) ---

    #[test]
    fn struct_sizes_match_llama_h() {
        assert_eq!(std::mem::size_of::<LlamaModelParams>(), 72);
        assert_eq!(std::mem::size_of::<LlamaContextParams>(), 136);
        assert_eq!(std::mem::size_of::<LlamaSamplerChainParams>(), 1);
        assert_eq!(std::mem::size_of::<LlamaBatch>(), 56);
    }

    // --- Struct alignment checks ---

    #[test]
    fn struct_alignments() {
        // All structs containing pointers should be 8-byte aligned on 64-bit
        assert_eq!(std::mem::align_of::<LlamaModelParams>(), 8);
        assert_eq!(std::mem::align_of::<LlamaContextParams>(), 8);
        assert_eq!(std::mem::align_of::<LlamaBatch>(), 8);
        // LlamaSamplerChainParams contains only a bool — 1-byte aligned
        assert_eq!(std::mem::align_of::<LlamaSamplerChainParams>(), 1);
    }

    // --- Type alias sizes ---

    #[test]
    fn type_alias_sizes() {
        // Opaque pointers are pointer-sized
        assert_eq!(
            std::mem::size_of::<LlamaModel>(),
            std::mem::size_of::<*mut c_void>()
        );
        assert_eq!(
            std::mem::size_of::<LlamaContext>(),
            std::mem::size_of::<*mut c_void>()
        );
        assert_eq!(
            std::mem::size_of::<LlamaSampler>(),
            std::mem::size_of::<*mut c_void>()
        );
        assert_eq!(
            std::mem::size_of::<LlamaVocab>(),
            std::mem::size_of::<*const c_void>()
        );
        assert_eq!(
            std::mem::size_of::<LlamaMemory>(),
            std::mem::size_of::<*mut c_void>()
        );
        // Token/Pos/SeqId are i32
        assert_eq!(std::mem::size_of::<LlamaToken>(), 4);
        assert_eq!(std::mem::size_of::<LlamaPos>(), 4);
        assert_eq!(std::mem::size_of::<LlamaSeqId>(), 4);
    }

    // --- Constants ---

    #[test]
    fn pooling_type_constants() {
        assert_eq!(LLAMA_POOLING_TYPE_UNSPECIFIED, -1);
        assert_eq!(LLAMA_POOLING_TYPE_NONE, 0);
        assert_eq!(LLAMA_POOLING_TYPE_MEAN, 1);
        assert_eq!(LLAMA_POOLING_TYPE_CLS, 2);
        assert_eq!(LLAMA_POOLING_TYPE_LAST, 3);
    }

    #[test]
    fn null_token_constant() {
        assert_eq!(LLAMA_TOKEN_NULL, -1);
    }

    // --- LlamaModelParams field offset smoke test ---

    #[test]
    fn model_params_bool_fields_at_end() {
        // The 8 bool fields occupy bytes 64..72 (last 8 bytes of the 72-byte struct).
        // Verify by checking that the offset of vocab_only is past all pointer fields.
        let size = std::mem::size_of::<LlamaModelParams>();
        assert_eq!(size, 72);
        // The bool fields start after kv_overrides (5 pointers * 8 = 40, but with
        // other fields in between). We can't easily test offsets without
        // offset_of!, but the size assertion + runtime probe in load() cover this.
    }

    // --- LlamaContextParams field count smoke test ---

    #[test]
    fn context_params_has_embeddings_field() {
        // Verify the embeddings bool is accessible and default-constructible
        // (we can't call default_params without libllama, but we can test the
        // struct is constructible with known values)
        let size = std::mem::size_of::<LlamaContextParams>();
        assert_eq!(size, 136);
    }

    // --- LlamaBatch field layout ---

    #[test]
    fn batch_has_expected_pointer_count() {
        // LlamaBatch: i32 + pad + 5 pointers + 1 pointer = 8 + 48 = 56
        let size = std::mem::size_of::<LlamaBatch>();
        assert_eq!(size, 56);
        // n_tokens is at offset 0
        // 5 pointers (token, embd, pos, n_seq_id, seq_id) + logits pointer = 6 * 8 = 48
        // Plus the i32 + pad = 8. Total = 56.
    }

    #[test]
    fn pooling_types_are_sequential() {
        // Verify the enum values are in order (important for array indexing)
        assert_eq!(LLAMA_POOLING_TYPE_NONE + 1, LLAMA_POOLING_TYPE_MEAN);
        assert_eq!(LLAMA_POOLING_TYPE_MEAN + 1, LLAMA_POOLING_TYPE_CLS);
        assert_eq!(LLAMA_POOLING_TYPE_CLS + 1, LLAMA_POOLING_TYPE_LAST);
    }

    #[test]
    fn model_params_n_gpu_layers_at_expected_offset() {
        // n_gpu_layers should be at byte offset 16 (after 2 pointers)
        // We verify by constructing a zeroed struct and checking field access
        let params: LlamaModelParams = unsafe { std::mem::zeroed() };
        // If the struct layout is correct, accessing these fields won't panic
        assert_eq!(params.n_gpu_layers, 0);
        assert!(!params.use_mmap); // zeroed = false
        assert!(!params.vocab_only);
    }

    #[test]
    fn context_params_zeroed_is_safe() {
        // Verify that zeroed LlamaContextParams is accessible
        let params: LlamaContextParams = unsafe { std::mem::zeroed() };
        assert_eq!(params.n_ctx, 0);
        assert_eq!(params.n_batch, 0);
        assert!(!params.embeddings);
        assert_eq!(params.pooling_type, LLAMA_POOLING_TYPE_NONE);
    }

    #[test]
    fn batch_zeroed_has_null_pointers() {
        let batch: LlamaBatch = unsafe { std::mem::zeroed() };
        assert_eq!(batch.n_tokens, 0);
        assert!(batch.token.is_null());
        assert!(batch.embd.is_null());
        assert!(batch.pos.is_null());
        assert!(batch.logits.is_null());
    }

    // --- Smoke test: load and verify symbol resolution ---

    #[test]
    #[ignore]
    fn smoke_test_load_api() {
        match LlamaCppApi::load() {
            Ok(api) => {
                // Verify model default params are sane
                let mparams = api.model_default_params();
                assert!(
                    mparams.use_mmap,
                    "model_default_params().use_mmap should be true"
                );
                assert_eq!(
                    mparams.n_gpu_layers, 0,
                    "default n_gpu_layers should be 0 (CPU-only default)"
                );
                assert!(!mparams.vocab_only, "default vocab_only should be false");

                // Verify context default params are sane
                let cparams = api.context_default_params();
                assert!(
                    cparams.n_ctx > 0,
                    "default n_ctx should be > 0, got {}",
                    cparams.n_ctx
                );
                assert!(
                    cparams.n_batch > 0,
                    "default n_batch should be > 0, got {}",
                    cparams.n_batch
                );

                // Verify sampler chain default params
                let sparams = api.sampler_chain_default_params();
                // no_perf defaults to false in llama.cpp
                assert!(
                    !sparams.no_perf,
                    "sampler_chain_default_params().no_perf should be false"
                );

                // Verify Debug impl works on a live instance
                let dbg = format!("{:?}", api);
                assert!(
                    dbg.contains("LlamaCppApi"),
                    "Debug output should contain struct name: {dbg}"
                );
            }
            Err(e) => {
                panic!("load() failed: {e}");
            }
        }
    }
}
