//! llama.cpp FFI layer for local embedding and generation.
//!
//! Dynamically loads `libllama` at runtime via [`dl::DynLib`] and provides:
//! - [`ffi::LlamaCppApi`] — resolved function pointers with safe wrappers
//! - [`context::LlamaCppContext`] — model/context lifecycle and tokenization
//!
//! Requires the `local` feature flag. No build-time dependency on llama.cpp.

pub mod dl;
pub mod ffi;
pub(crate) mod context;
