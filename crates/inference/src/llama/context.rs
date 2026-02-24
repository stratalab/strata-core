//! Shared llama.cpp model/context lifecycle and tokenization helpers.

use std::path::Path;
use std::sync::Arc;

use tracing::info;

use super::ffi::*;
use crate::InferenceError;

/// Shared llama.cpp model and context state.
///
/// Owns the model and context handles. `Drop` frees context then model
/// (order matters — context references model internals).
pub(crate) struct LlamaCppContext {
    pub api: Arc<LlamaCppApi>,
    pub model: LlamaModel,
    pub ctx: LlamaContext,
    pub vocab: LlamaVocab,
    pub n_embd: usize,
    pub n_ctx: usize,
    pub vocab_size: usize,
    #[allow(dead_code)]
    pub bos_id: LlamaToken,
    #[allow(dead_code)]
    pub eos_id: LlamaToken,
    pub has_encoder: bool,
}

// SAFETY: LlamaCppContext is only accessed through Mutex in the engine layer.
unsafe impl Send for LlamaCppContext {}

impl LlamaCppContext {
    /// Load a model configured for embedding (pooling enabled).
    pub fn load_for_embedding(path: &Path) -> Result<Self, InferenceError> {
        let api = Arc::new(LlamaCppApi::load().map_err(InferenceError::LlamaCpp)?);

        let c_path = path_to_cstring(path)?;

        // Model params: default (use mmap, no GPU layers needed for small models)
        let mparams = api.model_default_params();

        info!(path = %path.display(), "Loading model via llama.cpp");
        let model = api
            .model_load_from_file(&c_path, mparams)
            .map_err(InferenceError::LlamaCpp)?;

        let vocab = api.model_get_vocab(model);
        let n_embd_raw = api.model_n_embd(model);
        let has_encoder = api.model_has_encoder(model);
        let vocab_size_raw = api.vocab_n_tokens(vocab);
        let bos_id = api.vocab_bos(vocab);
        let eos_id = api.vocab_eos(vocab);

        if n_embd_raw <= 0 || vocab_size_raw <= 0 {
            api.model_free(model);
            return Err(InferenceError::LlamaCpp(format!(
                "invalid model metadata: n_embd={n_embd_raw}, vocab_size={vocab_size_raw}"
            )));
        }
        let n_embd = n_embd_raw as usize;
        let vocab_size = vocab_size_raw as usize;

        // Context params: enable embeddings, set pooling to MEAN
        let mut cparams = api.context_default_params();
        cparams.embeddings = true;
        cparams.pooling_type = LLAMA_POOLING_TYPE_MEAN;
        // Use model's training context size
        cparams.n_ctx = 0; // 0 = from model

        let ctx = api.init_from_model(model, cparams).map_err(|e| {
            // Free model on context creation failure to avoid leak
            api.model_free(model);
            InferenceError::LlamaCpp(e)
        })?;

        let n_ctx_raw = api.model_n_ctx_train(model);
        let n_ctx = if n_ctx_raw > 0 {
            n_ctx_raw as usize
        } else {
            512
        };

        info!(
            n_embd = n_embd,
            vocab_size = vocab_size,
            n_ctx = n_ctx,
            has_encoder = has_encoder,
            "llama.cpp embedding context created"
        );

        Ok(Self {
            api,
            model,
            ctx,
            vocab,
            n_embd,
            n_ctx,
            vocab_size,
            bos_id,
            eos_id,
            has_encoder,
        })
    }

    /// Load a model configured for text generation.
    pub fn load_for_generation(
        path: &Path,
        ctx_override: Option<usize>,
    ) -> Result<Self, InferenceError> {
        let api = Arc::new(LlamaCppApi::load().map_err(InferenceError::LlamaCpp)?);

        let c_path = path_to_cstring(path)?;

        // Model params: default with GPU offloading
        let mut mparams = api.model_default_params();
        mparams.n_gpu_layers = 999; // offload all layers

        info!(path = %path.display(), "Loading model via llama.cpp");
        let model = api
            .model_load_from_file(&c_path, mparams)
            .map_err(InferenceError::LlamaCpp)?;

        let vocab = api.model_get_vocab(model);
        let n_embd_raw = api.model_n_embd(model);
        let has_encoder = api.model_has_encoder(model);
        let vocab_size_raw = api.vocab_n_tokens(vocab);
        let bos_id = api.vocab_bos(vocab);
        let eos_id = api.vocab_eos(vocab);
        let train_ctx_raw = api.model_n_ctx_train(model);

        if n_embd_raw <= 0 || vocab_size_raw <= 0 {
            api.model_free(model);
            return Err(InferenceError::LlamaCpp(format!(
                "invalid model metadata: n_embd={n_embd_raw}, vocab_size={vocab_size_raw}"
            )));
        }
        let n_embd = n_embd_raw as usize;
        let vocab_size = vocab_size_raw as usize;
        let train_ctx = if train_ctx_raw > 0 {
            train_ctx_raw as usize
        } else {
            2048
        };

        // Context params
        let mut cparams = api.context_default_params();
        cparams.embeddings = false;

        let n_ctx = match ctx_override {
            Some(ctx) => ctx.min(train_ctx),
            None => train_ctx.min(4096), // default cap
        };
        cparams.n_ctx = n_ctx as u32;

        let ctx = api.init_from_model(model, cparams).map_err(|e| {
            // Free model on context creation failure to avoid leak
            api.model_free(model);
            InferenceError::LlamaCpp(e)
        })?;

        info!(
            n_embd = n_embd,
            vocab_size = vocab_size,
            n_ctx = n_ctx,
            has_encoder = has_encoder,
            "llama.cpp generation context created"
        );

        Ok(Self {
            api,
            model,
            ctx,
            vocab,
            n_embd,
            n_ctx,
            vocab_size,
            bos_id,
            eos_id,
            has_encoder,
        })
    }

    /// Tokenize text into token IDs.
    pub fn tokenize(&self, text: &str, add_special: bool) -> Vec<LlamaToken> {
        let text_bytes = text.as_bytes();

        // First call: get required buffer size
        let n_needed = self
            .api
            .tokenize(self.vocab, text_bytes, &mut [], add_special, false);

        // n_needed is negative (= -required_size) when buffer is too small
        let buf_size = if n_needed < 0 {
            (-n_needed) as usize
        } else {
            // Empty text or very short — use n_needed directly
            n_needed as usize
        };

        if buf_size == 0 {
            return Vec::new();
        }

        let mut tokens = vec![0i32; buf_size + 1]; // +1 for safety
        let n = self
            .api
            .tokenize(self.vocab, text_bytes, &mut tokens, add_special, false);

        if n < 0 {
            // Should not happen since we allocated enough
            tokens.truncate(0);
        } else {
            tokens.truncate(n as usize);
        }

        tokens
    }

    /// Detokenize token IDs back to text.
    pub fn detokenize(&self, tokens: &[LlamaToken]) -> String {
        if tokens.is_empty() {
            return String::new();
        }

        // First call to get required size
        let n_needed = self.api.detokenize(
            self.vocab,
            tokens,
            &mut [],
            true, // remove_special
        );

        let buf_size = if n_needed < 0 {
            (-n_needed) as usize
        } else {
            n_needed as usize
        };

        if buf_size == 0 {
            return String::new();
        }

        let mut buf = vec![0u8; buf_size + 1];
        let n = self.api.detokenize(self.vocab, tokens, &mut buf, true);

        if n > 0 {
            buf.truncate(n as usize);
            String::from_utf8_lossy(&buf).into_owned()
        } else {
            // Fallback: piece-by-piece
            let mut result = String::new();
            let mut piece_buf = [0u8; 256];
            for &token in tokens {
                let len = self.api.token_to_piece(self.vocab, token, &mut piece_buf);
                if len > 0 && (len as usize) <= piece_buf.len() {
                    result.push_str(&String::from_utf8_lossy(&piece_buf[..len as usize]));
                }
                // If len > 256 or negative, skip this token (shouldn't happen with well-formed vocab)
            }
            result
        }
    }

    /// Clear the KV cache / memory state for a fresh inference.
    pub fn clear_memory(&self) {
        let mem = self.api.get_memory(self.ctx);
        self.api.memory_clear(mem, true);
    }
}

impl Drop for LlamaCppContext {
    fn drop(&mut self) {
        // Free context first (references model internals), then model
        self.api.free(self.ctx);
        self.api.model_free(self.model);
    }
}

/// Convert a Path to a CString for C API calls.
fn path_to_cstring(path: &Path) -> Result<std::ffi::CString, InferenceError> {
    let path_str = path.to_str().ok_or_else(|| {
        InferenceError::LlamaCpp(format!("path contains invalid UTF-8: {:?}", path))
    })?;
    std::ffi::CString::new(path_str)
        .map_err(|_| InferenceError::LlamaCpp(format!("path contains null byte: {:?}", path)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_to_cstring_valid_path() {
        let result = path_to_cstring(Path::new("/tmp/model.gguf"));
        assert!(result.is_ok());
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), "/tmp/model.gguf");
    }

    #[test]
    fn path_to_cstring_with_spaces() {
        let result = path_to_cstring(Path::new("/tmp/my models/model file.gguf"));
        assert!(result.is_ok());
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), "/tmp/my models/model file.gguf");
    }

    #[test]
    fn path_to_cstring_with_unicode() {
        let result = path_to_cstring(Path::new("/tmp/модель.gguf"));
        assert!(result.is_ok());
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), "/tmp/модель.gguf");
    }

    #[test]
    fn path_to_cstring_with_null_byte() {
        let result = path_to_cstring(Path::new("/tmp/model\0.gguf"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, InferenceError::LlamaCpp(_)));
        assert!(err.to_string().contains("null byte"), "error: {err}");
    }

    #[test]
    fn path_to_cstring_empty_path() {
        let result = path_to_cstring(Path::new(""));
        // Empty string is a valid CString (just a null terminator)
        assert!(result.is_ok());
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), "");
    }

    #[test]
    fn path_to_cstring_relative_path() {
        let result = path_to_cstring(Path::new("models/model.gguf"));
        assert!(result.is_ok());
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), "models/model.gguf");
    }

    #[test]
    fn path_to_cstring_deeply_nested() {
        let result = path_to_cstring(Path::new("/a/b/c/d/e/f/g/h/model.gguf"));
        assert!(result.is_ok());
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), "/a/b/c/d/e/f/g/h/model.gguf");
    }

    #[test]
    fn path_to_cstring_with_special_chars() {
        let result = path_to_cstring(Path::new("/tmp/model (1) [copy].gguf"));
        assert!(result.is_ok());
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), "/tmp/model (1) [copy].gguf");
    }

    #[test]
    fn path_to_cstring_preserves_dot_segments() {
        // CString conversion should not normalize the path
        let result = path_to_cstring(Path::new("/tmp/../tmp/./model.gguf"));
        assert!(result.is_ok());
        // Path::new normalizes some things, but the CString should match
        // what Path gives us
        let expected = Path::new("/tmp/../tmp/./model.gguf").to_str().unwrap();
        let cstr = result.unwrap();
        assert_eq!(cstr.to_str().unwrap(), expected);
    }
}
