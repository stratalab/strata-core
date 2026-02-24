//! Generation providers — pluggable backends for text generation.
//!
//! The local provider uses llama.cpp; cloud providers send HTTP requests to
//! Anthropic, OpenAI, or Google APIs.

#[cfg(feature = "local")]
pub(crate) mod local;

#[cfg(feature = "anthropic")]
pub(crate) mod anthropic;

#[cfg(feature = "openai")]
pub(crate) mod openai;

#[cfg(feature = "google")]
pub(crate) mod google;
