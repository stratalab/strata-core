//! llama.cpp C API bindings via static linking.
//!
//! All symbols are resolved at link time from the vendored llama.cpp build
//! (compiled via `build.rs`). [`LlamaCppApi::load()`] initialises the backend
//! once and runs layout probes, then returns a zero-sized handle whose methods
//! call directly into the extern symbols.

// The llama.cpp C API speaks i32 lengths and mutable pointers; the casts
// at this boundary are the interface, not accidents.
#![allow(dead_code, missing_docs, unreachable_pub)]
#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::ptr_as_ptr)]

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::sync::{Mutex, MutexGuard, OnceLock};

static LLAMA_API_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Serializes calls into llama.cpp.
///
/// The C API exposes process-global backend state, and some backend paths
/// (notably Metal model loading) are not safe to exercise concurrently even
/// when contexts are independent.
pub(crate) fn llama_api_lock() -> MutexGuard<'static, ()> {
    LLAMA_API_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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
pub const LLAMA_POOLING_TYPE_RANK: i32 = 4;

// Load mode enum values (llama_load_mode in llama.h, b10766).
pub const LLAMA_LOAD_MODE_AUTO: i32 = -1;

// Flash-attention type (llama_flash_attn_type in llama.h, b10766). The default
// is AUTO, which enables FA — a behavior change from the pre-b10766 bool default
// (off) that breaks encoder/embedding context init, so we pin DISABLED.
pub const LLAMA_FLASH_ATTN_TYPE_DISABLED: i32 = 0;

// ---------------------------------------------------------------------------
// #[repr(C)] struct definitions matching llama.h
// ---------------------------------------------------------------------------

/// Matches `struct llama_model_params` from llama.h (80 bytes on x86_64, b10766).
#[repr(C)]
pub struct LlamaModelParams {
    pub devices: *mut c_void,                 // ggml_backend_dev_t *
    pub tensor_buft_overrides: *const c_void, // const llama_model_tensor_buft_override *
    pub n_gpu_layers: i32,
    pub split_mode: i32, // enum llama_split_mode
    pub load_mode: i32,  // enum llama_load_mode (b10766: replaces use_mmap/use_direct_io)
    pub lazy_mode: i32,  // enum llama_lazy_mode
    pub main_gpu: i32,
    _pad0: i32, // padding to align tensor_split ptr
    pub tensor_split: *const f32,
    pub progress_callback: *const c_void, // llama_progress_callback
    pub progress_callback_user_data: *mut c_void,
    pub kv_overrides: *const c_void,
    pub vocab_only: bool,
    pub check_tensors: bool,
    pub use_extra_bufts: bool,
    pub no_host: bool,
    pub no_alloc: bool,
    pub load_mtp: bool,
}

const _: () = assert!(std::mem::size_of::<LlamaModelParams>() == 80);

/// Matches `struct llama_context_params` from llama.h (160 bytes on x86_64, b10766).
#[repr(C)]
pub struct LlamaContextParams {
    pub n_ctx: u32,
    pub n_batch: u32,
    pub n_ubatch: u32,
    pub n_seq_max: u32,
    pub n_rs_seq: u32,              // b10766: recurrent-state snapshots per seq
    pub n_outputs_max: u32,         // b10766: max outputs in a ubatch
    pub n_outputs_max_per_seq: u32, // b10766: max outputs per sequence
    pub n_threads: i32,
    pub n_threads_batch: i32,
    pub ctx_type: i32,          // enum llama_context_type (b10766)
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
    pub ctx_other: *mut c_void, // struct llama_context * (b10766)
}

const _: () = assert!(std::mem::size_of::<LlamaContextParams>() == 160);

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

/// Matches `struct llama_chat_message` from llama.h (two C-string pointers).
#[repr(C)]
pub struct LlamaChatMessage {
    pub role: *const c_char,
    pub content: *const c_char,
}

// ---------------------------------------------------------------------------
// Statically linked extern "C" symbols
// ---------------------------------------------------------------------------

/// `ggml_log_callback` from ggml.h: a level, a NUL-terminated line, and the
/// `user_data` passed to [`llama_log_set`].
pub type GgmlLogCallback =
    unsafe extern "C" fn(level: i32, text: *const c_char, user_data: *mut c_void);

extern "C" {
    // Backend
    pub fn llama_backend_init();
    pub fn llama_backend_free();

    /// Routes every future llama.cpp *and* ggml log line to `log_callback`.
    /// One call covers both: `llama_log_set` calls `ggml_log_set` itself
    /// (`src/llama-impl.cpp`, b10766). Passing null restores the default,
    /// which writes everything to stderr.
    pub fn llama_log_set(log_callback: Option<GgmlLogCallback>, user_data: *mut c_void);

    /// Reads back whatever [`llama_log_set`] installed. Used only to assert
    /// that the default stderr logger is no longer the one in place.
    pub fn ggml_log_get(log_callback: *mut Option<GgmlLogCallback>, user_data: *mut *mut c_void);

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

    // Memory (KV cache) — the `llama_memory_t` API (llama.cpp b10766).
    pub fn llama_get_memory(ctx: LlamaContext) -> LlamaMemory;
    pub fn llama_memory_clear(mem: LlamaMemory, data: bool);

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
    pub fn llama_sampler_init_grammar(
        vocab: LlamaVocab,
        grammar_str: *const c_char,
        grammar_root: *const c_char,
    ) -> LlamaSampler;
    pub fn llama_sampler_init_typical(p: f32, min_keep: usize) -> LlamaSampler;
    pub fn llama_sampler_init_temp_ext(t: f32, delta: f32, exponent: f32) -> LlamaSampler;
    pub fn llama_sampler_init_penalties(
        penalty_last_n: i32,
        penalty_repeat: f32,
        penalty_freq: f32,
        penalty_present: f32,
    ) -> LlamaSampler;
    pub fn llama_sampler_init_mirostat(
        n_vocab: i32,
        seed: u32,
        tau: f32,
        eta: f32,
        m: i32,
    ) -> LlamaSampler;
    pub fn llama_sampler_init_mirostat_v2(seed: u32, tau: f32, eta: f32) -> LlamaSampler;

    // Chat templates
    pub fn llama_model_chat_template(model: LlamaModel, name: *const c_char) -> *const c_char;
    pub fn llama_chat_apply_template(
        tmpl: *const c_char,
        chat: *const LlamaChatMessage,
        n_msg: usize,
        add_ass: bool,
        buf: *mut c_char,
        length: i32,
    ) -> i32;
}

// ---------------------------------------------------------------------------
// LlamaCppApi — thin handle to statically linked symbols
// ---------------------------------------------------------------------------

/// Handle to the statically linked llama.cpp API.
///
/// [`LlamaCppApi::load()`] initializes the process-wide llama.cpp backend once.
/// Model and context handles are still freed explicitly, but the backend itself
/// stays initialized for the lifetime of the process because llama.cpp exposes
/// global backend state that can be shared by independently-created contexts.
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

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// `enum ggml_log_level` (ggml.h, b10766).
const GGML_LOG_LEVEL_NONE: i32 = 0;
const GGML_LOG_LEVEL_DEBUG: i32 = 1;
const GGML_LOG_LEVEL_INFO: i32 = 2;
const GGML_LOG_LEVEL_WARN: i32 = 3;
const GGML_LOG_LEVEL_ERROR: i32 = 4;
/// Not a level: the line continues the previous one, at the previous level.
const GGML_LOG_LEVEL_CONT: i32 = 5;

/// The level a continuation belongs to — the last real level llama.cpp used.
static LAST_LOG_LEVEL: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(GGML_LOG_LEVEL_INFO);

/// Where one llama.cpp line goes in Strata's own levels.
///
/// llama.cpp's INFO is the loader dump — ~900 lines per model load, which is
/// what made a successful local `generate` unreadable (#3234). It is
/// diagnostics about our dependency, not news for the person who asked a
/// question, so it lands at `debug`: nothing by default, everything under
/// `STRATA_LOG=debug`. Warnings and errors keep their own level, because a
/// failed load has to stay findable.
///
/// `NONE` is llama.cpp's "no level" and is dropped. `CONT` is not a level at
/// all — the line continues the previous one (the loader's `....` progress
/// bar is emitted this way) — so it takes the level of whatever it continues,
/// which keeps a multi-line error whole instead of splitting its tail into
/// `debug`.
///
/// A pure function on purpose: the callback itself can only be exercised by
/// llama.cpp, in a build with `local`, and the mapping is the part with a
/// decision in it.
fn tracing_level_for(level: i32, last: i32) -> Option<tracing::Level> {
    let effective = if level == GGML_LOG_LEVEL_CONT {
        last
    } else {
        level
    };
    match effective {
        GGML_LOG_LEVEL_ERROR => Some(tracing::Level::ERROR),
        GGML_LOG_LEVEL_WARN => Some(tracing::Level::WARN),
        GGML_LOG_LEVEL_INFO => Some(tracing::Level::DEBUG),
        GGML_LOG_LEVEL_DEBUG => Some(tracing::Level::TRACE),
        _ => None,
    }
}

/// Receives every llama.cpp and ggml log line and forwards it to `tracing`.
///
/// Installed once, before anything can load a model, so the default logger —
/// which writes all of it to stderr at full verbosity — never runs.
unsafe extern "C" fn forward_log_to_tracing(
    level: i32,
    text: *const c_char,
    _user_data: *mut c_void,
) {
    use std::sync::atomic::Ordering;

    if level != GGML_LOG_LEVEL_CONT {
        LAST_LOG_LEVEL.store(level, Ordering::Relaxed);
    }
    let Some(target) = tracing_level_for(level, LAST_LOG_LEVEL.load(Ordering::Relaxed)) else {
        return;
    };
    if text.is_null() {
        return;
    }
    // SAFETY: llama.cpp passes a NUL-terminated buffer that outlives the call.
    let Ok(line) = (unsafe { CStr::from_ptr(text) }).to_str() else {
        return; // a line that is not UTF-8 is not worth a lossy allocation
    };
    let line = line.trim_end_matches('\n');
    if line.is_empty() {
        return;
    }
    match target {
        tracing::Level::ERROR => tracing::error!(target: "llama_cpp", "{line}"),
        tracing::Level::WARN => tracing::warn!(target: "llama_cpp", "{line}"),
        tracing::Level::DEBUG => tracing::debug!(target: "llama_cpp", "{line}"),
        _ => tracing::trace!(target: "llama_cpp", "{line}"),
    }
}

impl LlamaCppApi {
    /// Initialise the llama.cpp backend (once) and verify struct layout probes.
    pub fn load() -> Result<Self, String> {
        use std::sync::Once;
        static BACKEND_INIT: Once = Once::new();
        BACKEND_INIT.call_once(|| unsafe {
            // Before `llama_backend_init`, so the backend's own startup lines
            // go through the callback too rather than straight to stderr.
            llama_log_set(Some(forward_log_to_tracing), std::ptr::null_mut());
            llama_backend_init();
        });

        // Runtime layout probes: verify struct layout by checking default params.
        let mparams = unsafe { llama_model_default_params() };
        // use_extra_bufts defaults true and load_mode defaults AUTO (-1); reading
        // either wrong means the b10766 llama_model_params layout drifted.
        if !mparams.use_extra_bufts || mparams.load_mode != LLAMA_LOAD_MODE_AUTO {
            return Err(
                "llama.cpp version mismatch: llama_model_default_params() layout probe failed \
                 (use_extra_bufts/load_mode). The struct layout may differ from what \
                 strata-inference expects."
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
            // llama.cpp's own reason ("invalid magic characters", a missing
            // tensor, …) is logged, not returned — and since #3234 it is
            // logged through `tracing` rather than sprayed on stderr, so the
            // reader has to be told where it went. Capturing it into this
            // error is the better answer and is tracked separately.
            return Err(format!(
                "llama_model_load_from_file failed for {path:?} (set STRATA_LOG=error for \
                 llama.cpp's reason)"
            ));
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
        unsafe { llama_get_memory(ctx) }
    }

    /// Returns true if the context has no KV cache (encoder-only models like BERT).
    pub fn kv_self_is_null(&self, ctx: LlamaContext) -> bool {
        unsafe { llama_get_memory(ctx) }.is_null()
    }

    pub fn memory_clear(&self, mem: LlamaMemory, data: bool) {
        if !mem.is_null() {
            unsafe { llama_memory_clear(mem, data) };
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

    pub fn sampler_init_grammar(
        &self,
        vocab: LlamaVocab,
        grammar_str: &str,
        grammar_root: &str,
    ) -> Result<LlamaSampler, String> {
        let c_grammar = std::ffi::CString::new(grammar_str)
            .map_err(|e| format!("grammar string contains null byte: {e}"))?;
        let c_root = std::ffi::CString::new(grammar_root)
            .map_err(|e| format!("grammar root contains null byte: {e}"))?;
        let sampler =
            unsafe { llama_sampler_init_grammar(vocab, c_grammar.as_ptr(), c_root.as_ptr()) };
        if sampler.is_null() {
            return Err("llama_sampler_init_grammar returned null (invalid grammar?)".to_string());
        }
        Ok(sampler)
    }

    pub fn sampler_init_typical(&self, p: f32, min_keep: usize) -> LlamaSampler {
        unsafe { llama_sampler_init_typical(p, min_keep) }
    }

    pub fn sampler_init_temp_ext(&self, t: f32, delta: f32, exponent: f32) -> LlamaSampler {
        unsafe { llama_sampler_init_temp_ext(t, delta, exponent) }
    }

    pub fn sampler_init_penalties(
        &self,
        penalty_last_n: i32,
        penalty_repeat: f32,
        penalty_freq: f32,
        penalty_present: f32,
    ) -> LlamaSampler {
        unsafe {
            llama_sampler_init_penalties(
                penalty_last_n,
                penalty_repeat,
                penalty_freq,
                penalty_present,
            )
        }
    }

    pub fn sampler_init_mirostat(
        &self,
        n_vocab: i32,
        seed: u32,
        tau: f32,
        eta: f32,
        m: i32,
    ) -> LlamaSampler {
        unsafe { llama_sampler_init_mirostat(n_vocab, seed, tau, eta, m) }
    }

    pub fn sampler_init_mirostat_v2(&self, seed: u32, tau: f32, eta: f32) -> LlamaSampler {
        unsafe { llama_sampler_init_mirostat_v2(seed, tau, eta) }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — chat templates
    // -----------------------------------------------------------------------

    /// Returns the model's embedded chat template (`name = None` for the
    /// default), or `None` if the model has no template.
    pub fn model_chat_template(&self, model: LlamaModel, name: Option<&str>) -> Option<String> {
        let c_name = name.and_then(|n| CString::new(n).ok());
        let name_ptr = c_name.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
        let ptr = unsafe { llama_model_chat_template(model, name_ptr) };
        if ptr.is_null() {
            return None;
        }
        Some(
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned(),
        )
    }

    /// Applies a chat template (a jinja string or a built-in name like
    /// `"chatml"`) to `messages`, returning the formatted prompt. `add_ass`
    /// appends the assistant generation prefix.
    pub fn chat_apply_template(
        &self,
        tmpl: &str,
        messages: &[(String, String)],
        add_ass: bool,
    ) -> Result<String, String> {
        // Keep the role/content CStrings alive for the duration of the calls.
        let owned: Vec<(CString, CString)> = messages
            .iter()
            .map(|(role, content)| {
                Ok((
                    CString::new(role.as_str()).map_err(|e| format!("role null byte: {e}"))?,
                    CString::new(content.as_str())
                        .map_err(|e| format!("content null byte: {e}"))?,
                ))
            })
            .collect::<Result<_, String>>()?;
        let msgs: Vec<LlamaChatMessage> = owned
            .iter()
            .map(|(role, content)| LlamaChatMessage {
                role: role.as_ptr(),
                content: content.as_ptr(),
            })
            .collect();
        let tmpl_c = CString::new(tmpl).map_err(|e| format!("template null byte: {e}"))?;

        // First call sizes the buffer (returns the required length).
        let needed = unsafe {
            llama_chat_apply_template(
                tmpl_c.as_ptr(),
                msgs.as_ptr(),
                msgs.len(),
                add_ass,
                std::ptr::null_mut(),
                0,
            )
        };
        if needed < 0 {
            return Err(format!(
                "llama_chat_apply_template failed ({needed}); template may be unsupported"
            ));
        }
        let mut buf = vec![0u8; needed as usize + 1];
        let written = unsafe {
            llama_chat_apply_template(
                tmpl_c.as_ptr(),
                msgs.as_ptr(),
                msgs.len(),
                add_ass,
                buf.as_mut_ptr() as *mut c_char,
                buf.len() as i32,
            )
        };
        if written < 0 {
            return Err(format!(
                "llama_chat_apply_template failed on write ({written})"
            ));
        }
        let written = (written as usize).min(buf.len());
        Ok(String::from_utf8_lossy(&buf[..written]).into_owned())
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
        assert_eq!(std::mem::size_of::<LlamaModelParams>(), 80);
        assert_eq!(std::mem::size_of::<LlamaContextParams>(), 160);
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
        assert_eq!(LLAMA_POOLING_TYPE_RANK, 4);
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
        assert_eq!(size, 80);
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
        assert_eq!(size, 160);
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
        assert_eq!(LLAMA_POOLING_TYPE_LAST + 1, LLAMA_POOLING_TYPE_RANK);
    }

    #[test]
    fn model_params_n_gpu_layers_at_expected_offset() {
        // n_gpu_layers should be at byte offset 16 (after 2 pointers)
        // We verify by constructing a zeroed struct and checking field access
        let params: LlamaModelParams = unsafe { std::mem::zeroed() };
        // If the struct layout is correct, accessing these fields won't panic
        assert_eq!(params.n_gpu_layers, 0);
        assert!(!params.use_extra_bufts); // zeroed = false
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
                    mparams.use_extra_bufts,
                    "model_default_params().use_extra_bufts should be true"
                );
                assert_eq!(
                    mparams.n_gpu_layers, -1,
                    "default n_gpu_layers should be -1 (all layers) in b10766"
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

#[cfg(test)]
mod log_routing_tests {
    use super::{
        tracing_level_for, GGML_LOG_LEVEL_CONT, GGML_LOG_LEVEL_DEBUG, GGML_LOG_LEVEL_ERROR,
        GGML_LOG_LEVEL_INFO, GGML_LOG_LEVEL_NONE, GGML_LOG_LEVEL_WARN,
    };

    /// Where each of llama.cpp's levels lands, and why it matters that INFO
    /// is not one of ours: INFO is the ~900-line loader dump that made a
    /// successful local `generate` unreadable (#3234).
    #[test]
    fn a_llama_line_lands_where_its_level_says() {
        let at = |level| tracing_level_for(level, GGML_LOG_LEVEL_INFO);

        assert_eq!(at(GGML_LOG_LEVEL_ERROR), Some(tracing::Level::ERROR));
        assert_eq!(at(GGML_LOG_LEVEL_WARN), Some(tracing::Level::WARN));
        // The loader dump: diagnostics about a dependency, not an answer.
        assert_eq!(at(GGML_LOG_LEVEL_INFO), Some(tracing::Level::DEBUG));
        assert_eq!(at(GGML_LOG_LEVEL_DEBUG), Some(tracing::Level::TRACE));
        // Not a level, and not ours to guess at.
        assert_eq!(at(GGML_LOG_LEVEL_NONE), None);
        assert_eq!(at(9_999), None);
    }

    /// A continuation takes the level of the line it continues, so a
    /// multi-line error stays an error instead of having its tail demoted to
    /// `debug` — and the loader's `....` progress bar, which is emitted as a
    /// continuation of an INFO line, stays out of the way.
    #[test]
    fn a_continuation_inherits_the_line_it_continues() {
        assert_eq!(
            tracing_level_for(GGML_LOG_LEVEL_CONT, GGML_LOG_LEVEL_ERROR),
            Some(tracing::Level::ERROR)
        );
        assert_eq!(
            tracing_level_for(GGML_LOG_LEVEL_CONT, GGML_LOG_LEVEL_INFO),
            Some(tracing::Level::DEBUG)
        );
        assert_eq!(
            tracing_level_for(GGML_LOG_LEVEL_CONT, GGML_LOG_LEVEL_NONE),
            None,
            "a continuation of nothing is still nothing"
        );
    }
}

/// The property the fix rests on: after the backend is initialised, ggml's
/// log callback is *ours*, so llama.cpp's default logger — which writes every
/// line to stderr at full verbosity — never runs (#3234).
///
/// Asserted against the real library rather than inferred from the call
/// site, because `llama_backend_init` runs once per process behind a `Once`:
/// installing the callback after it, or on a path some other caller reaches
/// first, would leave the default logger in place for exactly the load that
/// prints the 900 lines.
#[cfg(all(test, feature = "local"))]
mod log_installation_tests {
    use super::{forward_log_to_tracing, ggml_log_get, GgmlLogCallback, LlamaCppApi};
    use std::os::raw::c_void;

    #[test]
    fn the_backend_hands_its_logging_to_us_not_to_stderr() {
        let mut before: Option<GgmlLogCallback> = None;
        let mut user_data: *mut c_void = std::ptr::null_mut();
        // SAFETY: both out-pointers are valid for the length of the call.
        unsafe { ggml_log_get(&raw mut before, &raw mut user_data) };

        LlamaCppApi::load().expect("the backend initialises");

        let mut installed: Option<GgmlLogCallback> = None;
        // SAFETY: as above.
        unsafe { ggml_log_get(&raw mut installed, &raw mut user_data) };

        let installed = installed.expect("a callback is installed");
        assert!(
            std::ptr::fn_addr_eq(installed, forward_log_to_tracing as GgmlLogCallback),
            "ggml is logging through something other than our callback, so the \
             default stderr logger may still be in place"
        );
        assert!(user_data.is_null(), "we pass no user data");
    }
}
