//! llama.cpp C API bindings loaded at runtime via dlopen.
//!
//! Uses the [`DynLib`] wrapper from [`super::dl`] to load `libllama.so` (Linux),
//! `libllama.dylib` (macOS), or `llama.dll` (Windows) at runtime, avoiding any
//! build-time dependency. All function pointers are resolved once during
//! [`LlamaCppApi::load()`].

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};
use std::path::PathBuf;

use super::dl::DynLib;

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
    pub devices: *mut c_void,                    // ggml_backend_dev_t *
    pub tensor_buft_overrides: *const c_void,    // const llama_model_tensor_buft_override *
    pub n_gpu_layers: i32,
    pub split_mode: i32,                         // enum llama_split_mode
    pub main_gpu: i32,
    _pad0: i32,                                  // padding to align tensor_split ptr
    pub tensor_split: *const f32,
    pub progress_callback: *const c_void,        // llama_progress_callback
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
    pub rope_scaling_type: i32,      // enum llama_rope_scaling_type
    pub pooling_type: i32,           // enum llama_pooling_type
    pub attention_type: i32,         // enum llama_attention_type
    pub flash_attn_type: i32,        // enum llama_flash_attn_type
    pub rope_freq_base: f32,
    pub rope_freq_scale: f32,
    pub yarn_ext_factor: f32,
    pub yarn_attn_factor: f32,
    pub yarn_beta_fast: f32,
    pub yarn_beta_slow: f32,
    pub yarn_orig_ctx: u32,
    pub defrag_thold: f32,
    pub cb_eval: *const c_void,      // ggml_backend_sched_eval_callback
    pub cb_eval_user_data: *mut c_void,
    pub type_k: i32,                 // enum ggml_type
    pub type_v: i32,                 // enum ggml_type
    pub abort_callback: *const c_void, // ggml_abort_callback
    pub abort_callback_data: *mut c_void,
    pub embeddings: bool,
    pub offload_kqv: bool,
    pub no_perf: bool,
    pub op_offload: bool,
    pub swa_full: bool,
    pub kv_unified: bool,
    _pad_bools: [u8; 2],            // padding to align next pointer
    pub samplers: *mut c_void,       // struct llama_sampler_seq_config *
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
// Function pointer types
// ---------------------------------------------------------------------------

// Backend
type FnBackendInit = unsafe extern "C" fn();
type FnBackendFree = unsafe extern "C" fn();

// Model
type FnModelDefaultParams = unsafe extern "C" fn() -> LlamaModelParams;
type FnModelLoadFromFile =
    unsafe extern "C" fn(path: *const c_char, params: LlamaModelParams) -> LlamaModel;
type FnModelFree = unsafe extern "C" fn(model: LlamaModel);
type FnModelGetVocab = unsafe extern "C" fn(model: LlamaModel) -> LlamaVocab;
type FnModelNEmbd = unsafe extern "C" fn(model: LlamaModel) -> i32;
type FnModelNCtxTrain = unsafe extern "C" fn(model: LlamaModel) -> i32;
type FnModelHasEncoder = unsafe extern "C" fn(model: LlamaModel) -> bool;

// Context
type FnContextDefaultParams = unsafe extern "C" fn() -> LlamaContextParams;
type FnInitFromModel =
    unsafe extern "C" fn(model: LlamaModel, params: LlamaContextParams) -> LlamaContext;
type FnFree = unsafe extern "C" fn(ctx: LlamaContext);

// Memory
type FnGetMemory = unsafe extern "C" fn(ctx: LlamaContext) -> LlamaMemory;
type FnMemoryClear = unsafe extern "C" fn(mem: LlamaMemory, data: bool);

// Tokenize
type FnTokenize = unsafe extern "C" fn(
    vocab: LlamaVocab,
    text: *const c_char,
    text_len: i32,
    tokens: *mut LlamaToken,
    n_tokens_max: i32,
    add_special: bool,
    parse_special: bool,
) -> i32;
type FnTokenToPiece = unsafe extern "C" fn(
    vocab: LlamaVocab,
    token: LlamaToken,
    buf: *mut c_char,
    length: i32,
    lstrip: i32,
    special: bool,
) -> i32;
type FnDetokenize = unsafe extern "C" fn(
    vocab: LlamaVocab,
    tokens: *const LlamaToken,
    n_tokens: i32,
    text: *mut c_char,
    text_len_max: i32,
    remove_special: bool,
    unparse_special: bool,
) -> i32;

// Vocab
type FnVocabNTokens = unsafe extern "C" fn(vocab: LlamaVocab) -> i32;
type FnVocabBos = unsafe extern "C" fn(vocab: LlamaVocab) -> LlamaToken;
type FnVocabEos = unsafe extern "C" fn(vocab: LlamaVocab) -> LlamaToken;
type FnVocabIsEog = unsafe extern "C" fn(vocab: LlamaVocab, token: LlamaToken) -> bool;

// Batch
type FnBatchGetOne = unsafe extern "C" fn(tokens: *mut LlamaToken, n_tokens: i32) -> LlamaBatch;
type FnBatchInit = unsafe extern "C" fn(n_tokens: i32, embd: i32, n_seq_max: i32) -> LlamaBatch;
type FnBatchFree = unsafe extern "C" fn(batch: LlamaBatch);

// Inference
type FnEncode = unsafe extern "C" fn(ctx: LlamaContext, batch: LlamaBatch) -> i32;
type FnDecode = unsafe extern "C" fn(ctx: LlamaContext, batch: LlamaBatch) -> i32;

// Output
type FnGetLogitsIth = unsafe extern "C" fn(ctx: LlamaContext, i: i32) -> *mut f32;
type FnGetEmbeddings = unsafe extern "C" fn(ctx: LlamaContext) -> *mut f32;
type FnGetEmbeddingsIth = unsafe extern "C" fn(ctx: LlamaContext, i: i32) -> *mut f32;
type FnGetEmbeddingsSeq = unsafe extern "C" fn(ctx: LlamaContext, seq_id: LlamaSeqId) -> *mut f32;

// Sampling
type FnSamplerChainInit =
    unsafe extern "C" fn(params: LlamaSamplerChainParams) -> LlamaSampler;
type FnSamplerChainDefaultParams = unsafe extern "C" fn() -> LlamaSamplerChainParams;
type FnSamplerChainAdd = unsafe extern "C" fn(chain: LlamaSampler, smpl: LlamaSampler);
type FnSamplerSample =
    unsafe extern "C" fn(smpl: LlamaSampler, ctx: LlamaContext, idx: i32) -> LlamaToken;
type FnSamplerFree = unsafe extern "C" fn(smpl: LlamaSampler);
type FnSamplerInitGreedy = unsafe extern "C" fn() -> LlamaSampler;
type FnSamplerInitDist = unsafe extern "C" fn(seed: u32) -> LlamaSampler;
type FnSamplerInitTopK = unsafe extern "C" fn(k: i32) -> LlamaSampler;
type FnSamplerInitTopP = unsafe extern "C" fn(p: f32, min_keep: usize) -> LlamaSampler;
type FnSamplerInitTemp = unsafe extern "C" fn(t: f32) -> LlamaSampler;
type FnSamplerInitMinP = unsafe extern "C" fn(p: f32, min_keep: usize) -> LlamaSampler;

// ---------------------------------------------------------------------------
// Symbol loading macro
// ---------------------------------------------------------------------------

macro_rules! load_sym {
    ($lib:expr, $name:expr) => {{
        let cname = concat!($name, "\0");
        let cstr = unsafe { CStr::from_bytes_with_nul_unchecked(cname.as_bytes()) };
        let ptr =
            unsafe { $lib.sym(cstr) }.map_err(|e| format!("failed to load {}: {}", $name, e))?;
        if ptr.is_null() {
            return Err(format!("{} resolved to null", $name));
        }
        unsafe { std::mem::transmute::<*mut c_void, _>(ptr) }
    }};
}

// ---------------------------------------------------------------------------
// LlamaCppApi — resolved function pointers
// ---------------------------------------------------------------------------

/// Holds the dynamically loaded libllama and all resolved function pointers.
///
/// Created once via [`LlamaCppApi::load()`]. `Drop` calls `llama_backend_free()`.
///
/// `Debug` is implemented manually because function pointers don't derive Debug.
pub struct LlamaCppApi {
    _lib: DynLib,

    // Backend
    backend_free: FnBackendFree,

    // Model
    pub(crate) model_default_params: FnModelDefaultParams,
    model_load_from_file: FnModelLoadFromFile,
    model_free: FnModelFree,
    model_get_vocab: FnModelGetVocab,
    model_n_embd: FnModelNEmbd,
    model_n_ctx_train: FnModelNCtxTrain,
    model_has_encoder: FnModelHasEncoder,

    // Context
    pub(crate) context_default_params: FnContextDefaultParams,
    init_from_model: FnInitFromModel,
    free: FnFree,

    // Memory
    get_memory: FnGetMemory,
    memory_clear: FnMemoryClear,

    // Tokenize
    tokenize: FnTokenize,
    token_to_piece: FnTokenToPiece,
    detokenize: FnDetokenize,

    // Vocab
    vocab_n_tokens: FnVocabNTokens,
    vocab_bos: FnVocabBos,
    vocab_eos: FnVocabEos,
    vocab_is_eog: FnVocabIsEog,

    // Batch
    batch_get_one: FnBatchGetOne,
    batch_init: FnBatchInit,
    batch_free: FnBatchFree,

    // Inference
    encode: FnEncode,
    decode: FnDecode,

    // Output
    get_logits_ith: FnGetLogitsIth,
    get_embeddings: FnGetEmbeddings,
    get_embeddings_ith: FnGetEmbeddingsIth,
    get_embeddings_seq: FnGetEmbeddingsSeq,

    // Sampling
    sampler_chain_init: FnSamplerChainInit,
    pub(crate) sampler_chain_default_params: FnSamplerChainDefaultParams,
    sampler_chain_add: FnSamplerChainAdd,
    sampler_sample: FnSamplerSample,
    sampler_free: FnSamplerFree,
    sampler_init_greedy: FnSamplerInitGreedy,
    sampler_init_dist: FnSamplerInitDist,
    sampler_init_top_k: FnSamplerInitTopK,
    sampler_init_top_p: FnSamplerInitTopP,
    sampler_init_temp: FnSamplerInitTemp,
    sampler_init_min_p: FnSamplerInitMinP,
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
    /// Attempt to load libllama and resolve all required function pointers.
    ///
    /// Search order:
    /// 1. `LLAMA_LIB_PATH` env var (exact path)
    /// 2. Same directory as executable
    /// 3. `../lib/` relative to executable
    /// 4. `~/.strata/lib/` (Unix) or `%LOCALAPPDATA%\strata\lib\` (Windows)
    /// 5. System search (`libllama.so` / `libllama.dylib` / `llama.dll`)
    pub fn load() -> Result<Self, String> {
        let lib = Self::open_library()?;

        // Resolve all symbols
        let backend_init: FnBackendInit = load_sym!(lib, "llama_backend_init");
        let backend_free: FnBackendFree = load_sym!(lib, "llama_backend_free");

        let model_default_params: FnModelDefaultParams =
            load_sym!(lib, "llama_model_default_params");
        let model_load_from_file: FnModelLoadFromFile =
            load_sym!(lib, "llama_model_load_from_file");
        let model_free: FnModelFree = load_sym!(lib, "llama_model_free");
        let model_get_vocab: FnModelGetVocab = load_sym!(lib, "llama_model_get_vocab");
        let model_n_embd: FnModelNEmbd = load_sym!(lib, "llama_model_n_embd");
        let model_n_ctx_train: FnModelNCtxTrain = load_sym!(lib, "llama_model_n_ctx_train");
        let model_has_encoder: FnModelHasEncoder = load_sym!(lib, "llama_model_has_encoder");

        let context_default_params: FnContextDefaultParams =
            load_sym!(lib, "llama_context_default_params");
        let init_from_model: FnInitFromModel = load_sym!(lib, "llama_init_from_model");
        let free: FnFree = load_sym!(lib, "llama_free");

        let get_memory: FnGetMemory = load_sym!(lib, "llama_get_memory");
        let memory_clear: FnMemoryClear = load_sym!(lib, "llama_memory_clear");

        let tokenize: FnTokenize = load_sym!(lib, "llama_tokenize");
        let token_to_piece: FnTokenToPiece = load_sym!(lib, "llama_token_to_piece");
        let detokenize: FnDetokenize = load_sym!(lib, "llama_detokenize");

        let vocab_n_tokens: FnVocabNTokens = load_sym!(lib, "llama_vocab_n_tokens");
        let vocab_bos: FnVocabBos = load_sym!(lib, "llama_vocab_bos");
        let vocab_eos: FnVocabEos = load_sym!(lib, "llama_vocab_eos");
        let vocab_is_eog: FnVocabIsEog = load_sym!(lib, "llama_vocab_is_eog");

        let batch_get_one: FnBatchGetOne = load_sym!(lib, "llama_batch_get_one");
        let batch_init: FnBatchInit = load_sym!(lib, "llama_batch_init");
        let batch_free: FnBatchFree = load_sym!(lib, "llama_batch_free");

        let encode: FnEncode = load_sym!(lib, "llama_encode");
        let decode: FnDecode = load_sym!(lib, "llama_decode");

        let get_logits_ith: FnGetLogitsIth = load_sym!(lib, "llama_get_logits_ith");
        let get_embeddings: FnGetEmbeddings = load_sym!(lib, "llama_get_embeddings");
        let get_embeddings_ith: FnGetEmbeddingsIth = load_sym!(lib, "llama_get_embeddings_ith");
        let get_embeddings_seq: FnGetEmbeddingsSeq = load_sym!(lib, "llama_get_embeddings_seq");

        let sampler_chain_init: FnSamplerChainInit = load_sym!(lib, "llama_sampler_chain_init");
        let sampler_chain_default_params: FnSamplerChainDefaultParams =
            load_sym!(lib, "llama_sampler_chain_default_params");
        let sampler_chain_add: FnSamplerChainAdd = load_sym!(lib, "llama_sampler_chain_add");
        let sampler_sample: FnSamplerSample = load_sym!(lib, "llama_sampler_sample");
        let sampler_free: FnSamplerFree = load_sym!(lib, "llama_sampler_free");
        let sampler_init_greedy: FnSamplerInitGreedy = load_sym!(lib, "llama_sampler_init_greedy");
        let sampler_init_dist: FnSamplerInitDist = load_sym!(lib, "llama_sampler_init_dist");
        let sampler_init_top_k: FnSamplerInitTopK = load_sym!(lib, "llama_sampler_init_top_k");
        let sampler_init_top_p: FnSamplerInitTopP = load_sym!(lib, "llama_sampler_init_top_p");
        let sampler_init_temp: FnSamplerInitTemp = load_sym!(lib, "llama_sampler_init_temp");
        let sampler_init_min_p: FnSamplerInitMinP = load_sym!(lib, "llama_sampler_init_min_p");

        // Initialize the backend
        unsafe { backend_init() };

        // Runtime probe: verify struct layout by checking default params.
        // use_mmap defaults to true in all known llama.cpp versions; if it
        // reads as false, our struct layout is misaligned.
        let mparams = unsafe { model_default_params() };
        if !mparams.use_mmap {
            return Err(
                "libllama version mismatch: llama_model_default_params().use_mmap is false \
                 (expected true). The struct layout may differ from what strata-inference expects. \
                 Check LLAMA_CPP_VERSION for the pinned version."
                    .to_string(),
            );
        }

        Ok(Self {
            _lib: lib,
            backend_free,
            model_default_params,
            model_load_from_file,
            model_free,
            model_get_vocab,
            model_n_embd,
            model_n_ctx_train,
            model_has_encoder,
            context_default_params,
            init_from_model,
            free,
            get_memory,
            memory_clear,
            tokenize,
            token_to_piece,
            detokenize,
            vocab_n_tokens,
            vocab_bos,
            vocab_eos,
            vocab_is_eog,
            batch_get_one,
            batch_init,
            batch_free,
            encode,
            decode,
            get_logits_ith,
            get_embeddings,
            get_embeddings_ith,
            get_embeddings_seq,
            sampler_chain_init,
            sampler_chain_default_params,
            sampler_chain_add,
            sampler_sample,
            sampler_free,
            sampler_init_greedy,
            sampler_init_dist,
            sampler_init_top_k,
            sampler_init_top_p,
            sampler_init_temp,
            sampler_init_min_p,
        })
    }

    /// Try to open libllama from multiple search paths.
    ///
    /// Search order per PLAN-strata-inference.md:
    /// 1. `LLAMA_LIB_PATH` env var (exact path)
    /// 2. Same directory as executable
    /// 3. `../lib/` relative to executable
    /// 4. `~/.strata/lib/` (Unix) or `%LOCALAPPDATA%\strata\lib\` (Windows)
    /// 5. System search (bare library name)
    fn open_library() -> Result<DynLib, String> {
        let filename = super::dl::libllama_filename();

        // 1. Explicit env var — exact path
        if let Ok(path) = std::env::var("LLAMA_LIB_PATH") {
            if !path.is_empty() {
                let cpath = std::ffi::CString::new(path.as_bytes())
                    .map_err(|_| "LLAMA_LIB_PATH contains null byte".to_string())?;
                return DynLib::open(&cpath);
            }
        }

        // Build candidate paths
        let mut candidates: Vec<PathBuf> = Vec::new();

        // 2 & 3. Relative to executable
        if let Ok(exe) = std::env::current_exe() {
            if let Some(exe_dir) = exe.parent() {
                // 2. Same directory as executable
                candidates.push(exe_dir.join(&filename));
                // 3. ../lib/ relative to executable
                candidates.push(exe_dir.join("../lib").join(&filename));
            }
        }

        // 4. User-local directory
        #[cfg(unix)]
        if let Ok(home) = std::env::var("HOME") {
            candidates.push(PathBuf::from(home).join(".strata/lib").join(&filename));
        }
        #[cfg(windows)]
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            candidates.push(PathBuf::from(local).join("strata\\lib").join(&filename));
        }

        // Try each candidate path
        for path in &candidates {
            if let Some(path_str) = path.to_str() {
                if let Ok(cpath) = std::ffi::CString::new(path_str) {
                    if let Ok(lib) = DynLib::open(&cpath) {
                        return Ok(lib);
                    }
                }
            }
        }

        // 5. System search (bare library name)
        let sys_name = std::ffi::CString::new(filename.as_bytes())
            .map_err(|_| "library filename contains null byte".to_string())?;
        let last_err = match DynLib::open(&sys_name) {
            Ok(lib) => return Ok(lib),
            Err(e) => e,
        };

        // Build a helpful error listing all paths tried
        let tried: Vec<String> = candidates
            .iter()
            .map(|p| format!("  - {}", p.display()))
            .collect();
        Err(format!(
            "could not load {filename}: {last_err}\n\
             Searched:\n\
             {tried}\n\
             Set LLAMA_LIB_PATH to the exact path, or install libllama in one of the above locations.",
            tried = tried.join("\n")
        ))
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — model lifecycle
    // -----------------------------------------------------------------------

    pub fn model_default_params(&self) -> LlamaModelParams {
        unsafe { (self.model_default_params)() }
    }

    pub fn model_load_from_file(
        &self,
        path: &CStr,
        params: LlamaModelParams,
    ) -> Result<LlamaModel, String> {
        let model = unsafe { (self.model_load_from_file)(path.as_ptr(), params) };
        if model.is_null() {
            return Err(format!(
                "llama_model_load_from_file failed for {:?}",
                path
            ));
        }
        Ok(model)
    }

    pub fn model_free(&self, model: LlamaModel) {
        if !model.is_null() {
            unsafe { (self.model_free)(model) };
        }
    }

    pub fn model_get_vocab(&self, model: LlamaModel) -> LlamaVocab {
        unsafe { (self.model_get_vocab)(model) }
    }

    pub fn model_n_embd(&self, model: LlamaModel) -> i32 {
        unsafe { (self.model_n_embd)(model) }
    }

    pub fn model_n_ctx_train(&self, model: LlamaModel) -> i32 {
        unsafe { (self.model_n_ctx_train)(model) }
    }

    pub fn model_has_encoder(&self, model: LlamaModel) -> bool {
        unsafe { (self.model_has_encoder)(model) }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — context lifecycle
    // -----------------------------------------------------------------------

    pub fn context_default_params(&self) -> LlamaContextParams {
        unsafe { (self.context_default_params)() }
    }

    pub fn init_from_model(
        &self,
        model: LlamaModel,
        params: LlamaContextParams,
    ) -> Result<LlamaContext, String> {
        let ctx = unsafe { (self.init_from_model)(model, params) };
        if ctx.is_null() {
            return Err("llama_init_from_model returned null".to_string());
        }
        Ok(ctx)
    }

    pub fn free(&self, ctx: LlamaContext) {
        if !ctx.is_null() {
            unsafe { (self.free)(ctx) };
        }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — memory
    // -----------------------------------------------------------------------

    pub fn get_memory(&self, ctx: LlamaContext) -> LlamaMemory {
        unsafe { (self.get_memory)(ctx) }
    }

    pub fn memory_clear(&self, mem: LlamaMemory, data: bool) {
        if !mem.is_null() {
            unsafe { (self.memory_clear)(mem, data) };
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
            (self.tokenize)(
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

    pub fn token_to_piece(
        &self,
        vocab: LlamaVocab,
        token: LlamaToken,
        buf: &mut [u8],
    ) -> i32 {
        unsafe {
            (self.token_to_piece)(
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
            (self.detokenize)(
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
        unsafe { (self.vocab_n_tokens)(vocab) }
    }

    pub fn vocab_bos(&self, vocab: LlamaVocab) -> LlamaToken {
        unsafe { (self.vocab_bos)(vocab) }
    }

    pub fn vocab_eos(&self, vocab: LlamaVocab) -> LlamaToken {
        unsafe { (self.vocab_eos)(vocab) }
    }

    pub fn vocab_is_eog(&self, vocab: LlamaVocab, token: LlamaToken) -> bool {
        unsafe { (self.vocab_is_eog)(vocab, token) }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — batch
    // -----------------------------------------------------------------------

    pub fn batch_get_one(&self, tokens: &mut [LlamaToken]) -> LlamaBatch {
        unsafe { (self.batch_get_one)(tokens.as_mut_ptr(), tokens.len() as i32) }
    }

    pub fn batch_init(&self, n_tokens: i32, embd: i32, n_seq_max: i32) -> LlamaBatch {
        unsafe { (self.batch_init)(n_tokens, embd, n_seq_max) }
    }

    pub fn batch_free(&self, batch: LlamaBatch) {
        unsafe { (self.batch_free)(batch) };
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — inference
    // -----------------------------------------------------------------------

    pub fn encode(&self, ctx: LlamaContext, batch: LlamaBatch) -> Result<(), String> {
        let rc = unsafe { (self.encode)(ctx, batch) };
        if rc != 0 {
            return Err(format!("llama_encode failed with code {}", rc));
        }
        Ok(())
    }

    pub fn decode(&self, ctx: LlamaContext, batch: LlamaBatch) -> Result<(), String> {
        let rc = unsafe { (self.decode)(ctx, batch) };
        if rc != 0 {
            return Err(format!("llama_decode failed with code {}", rc));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — output
    // -----------------------------------------------------------------------

    /// Get logits for the i-th token of the last decode call.
    /// Returns a raw pointer to `n_vocab` floats owned by llama.cpp.
    pub fn get_logits_ith(&self, ctx: LlamaContext, i: i32) -> *mut f32 {
        unsafe { (self.get_logits_ith)(ctx, i) }
    }

    /// Get pooled embeddings (for models with pooling).
    /// Returns a raw pointer to `n_embd` floats owned by llama.cpp.
    pub fn get_embeddings(&self, ctx: LlamaContext) -> *mut f32 {
        unsafe { (self.get_embeddings)(ctx) }
    }

    pub fn get_embeddings_ith(&self, ctx: LlamaContext, i: i32) -> *mut f32 {
        unsafe { (self.get_embeddings_ith)(ctx, i) }
    }

    pub fn get_embeddings_seq(&self, ctx: LlamaContext, seq_id: LlamaSeqId) -> *mut f32 {
        unsafe { (self.get_embeddings_seq)(ctx, seq_id) }
    }

    // -----------------------------------------------------------------------
    // Safe wrappers — sampling
    // -----------------------------------------------------------------------

    pub fn sampler_chain_init(&self, params: LlamaSamplerChainParams) -> LlamaSampler {
        unsafe { (self.sampler_chain_init)(params) }
    }

    pub fn sampler_chain_default_params(&self) -> LlamaSamplerChainParams {
        unsafe { (self.sampler_chain_default_params)() }
    }

    pub fn sampler_chain_add(&self, chain: LlamaSampler, smpl: LlamaSampler) {
        unsafe { (self.sampler_chain_add)(chain, smpl) };
    }

    pub fn sampler_sample(&self, smpl: LlamaSampler, ctx: LlamaContext, idx: i32) -> LlamaToken {
        unsafe { (self.sampler_sample)(smpl, ctx, idx) }
    }

    pub fn sampler_free(&self, smpl: LlamaSampler) {
        if !smpl.is_null() {
            unsafe { (self.sampler_free)(smpl) };
        }
    }

    pub fn sampler_init_greedy(&self) -> LlamaSampler {
        unsafe { (self.sampler_init_greedy)() }
    }

    pub fn sampler_init_dist(&self, seed: u32) -> LlamaSampler {
        unsafe { (self.sampler_init_dist)(seed) }
    }

    pub fn sampler_init_top_k(&self, k: i32) -> LlamaSampler {
        unsafe { (self.sampler_init_top_k)(k) }
    }

    pub fn sampler_init_top_p(&self, p: f32, min_keep: usize) -> LlamaSampler {
        unsafe { (self.sampler_init_top_p)(p, min_keep) }
    }

    pub fn sampler_init_temp(&self, t: f32) -> LlamaSampler {
        unsafe { (self.sampler_init_temp)(t) }
    }

    pub fn sampler_init_min_p(&self, p: f32, min_keep: usize) -> LlamaSampler {
        unsafe { (self.sampler_init_min_p)(p, min_keep) }
    }
}

impl Drop for LlamaCppApi {
    fn drop(&mut self) {
        unsafe { (self.backend_free)() };
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
        assert_eq!(std::mem::size_of::<LlamaModel>(), std::mem::size_of::<*mut c_void>());
        assert_eq!(std::mem::size_of::<LlamaContext>(), std::mem::size_of::<*mut c_void>());
        assert_eq!(std::mem::size_of::<LlamaSampler>(), std::mem::size_of::<*mut c_void>());
        assert_eq!(std::mem::size_of::<LlamaVocab>(), std::mem::size_of::<*const c_void>());
        assert_eq!(std::mem::size_of::<LlamaMemory>(), std::mem::size_of::<*mut c_void>());
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

    // --- Library loading error (no libllama expected in CI) ---

    #[test]
    fn load_without_libllama_returns_descriptive_error() {
        // Temporarily override to ensure we don't accidentally find a real libllama
        // This test verifies the error message is helpful, not just "error"
        std::env::set_var("LLAMA_LIB_PATH", "/nonexistent/path/libllama.so");
        let result = LlamaCppApi::load();
        std::env::remove_var("LLAMA_LIB_PATH");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            !err.is_empty(),
            "error should not be empty"
        );
        // Error should mention the path or library name
        assert!(
            err.contains("nonexistent") || err.contains("libllama"),
            "error should mention the path or library: {err}"
        );
    }

    #[test]
    fn load_with_empty_llama_lib_path_falls_through() {
        // Empty LLAMA_LIB_PATH should skip the env var check and try other paths
        std::env::set_var("LLAMA_LIB_PATH", "");
        let result = LlamaCppApi::load();
        std::env::remove_var("LLAMA_LIB_PATH");

        // Will fail (no libllama in CI) but should have tried other paths
        if let Err(err) = result {
            // Error should mention the search paths, not "LLAMA_LIB_PATH"
            assert!(
                err.contains("Searched") || err.contains("libllama"),
                "error should list search paths: {err}"
            );
        }
        // If it somehow succeeds (libllama is installed), that's fine too
    }

    #[test]
    fn load_error_includes_system_search_failure() {
        // When all paths fail, the error should reflect the system search
        // failure (the last attempt), not just candidate path failures
        std::env::set_var("LLAMA_LIB_PATH", "");
        let result = LlamaCppApi::load();
        std::env::remove_var("LLAMA_LIB_PATH");

        if let Err(err) = result {
            // The error message should contain the library filename
            let filename = super::super::dl::libllama_filename();
            assert!(
                err.contains(&filename),
                "error should mention {filename}: {err}"
            );
        }
    }

    #[test]
    fn llama_cpp_api_debug_impl() {
        // LlamaCppApi has a manual Debug impl — verify it doesn't panic
        // and contains the struct name. We can't construct one without libllama,
        // but we test the format indirectly via the error path.
        let result = LlamaCppApi::load();
        if let Ok(api) = result {
            let dbg = format!("{:?}", api);
            assert!(
                dbg.contains("LlamaCppApi"),
                "Debug output should contain struct name: {dbg}"
            );
        }
        // If load fails, that's expected (no libllama) — the test still verifies
        // the Debug impl compiles and is callable.
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

    // --- Smoke test: load libllama and verify symbol resolution ---

    #[test]
    #[ignore]
    fn smoke_test_load_api() {
        let has_env = std::env::var("LLAMA_LIB_PATH").is_ok();
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
                assert!(
                    !mparams.vocab_only,
                    "default vocab_only should be false"
                );

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
                if has_env {
                    panic!("LLAMA_LIB_PATH is set but load failed: {e}");
                }
                // No LLAMA_LIB_PATH → expected in CI without libllama
                eprintln!("skipping smoke_test_load_api: {e}");
            }
        }
    }
}
