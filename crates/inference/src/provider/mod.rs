//! Generation providers — pluggable backends for text generation.
//!
//! Currently only the local llama.cpp provider is implemented.
//! Cloud providers (Anthropic, OpenAI, Google) will be added in Epic 7.

pub(crate) mod local;
