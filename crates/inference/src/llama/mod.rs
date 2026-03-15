//! llama.cpp FFI layer for local embedding and generation.
//!
//! Statically links `libllama` compiled from vendored source via `build.rs`.
//! - [`ffi::LlamaCppApi`] — safe wrappers around the llama.cpp C API
//! - [`context::LlamaCppContext`] — model/context lifecycle and tokenization
//!
//! Requires the `local` feature flag (enabled by default).

pub(crate) mod context;
// Safe wrappers intentionally accept raw pointers (opaque FFI handles).
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub mod ffi;
