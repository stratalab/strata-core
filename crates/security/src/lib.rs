//! Access control and configuration for Strata database.
//!
//! This crate provides the [`AccessMode`] and [`OpenOptions`] types used to
//! control how a database is opened and what operations are permitted.

#![warn(missing_docs)]

use serde::{Deserialize, Serialize};

/// Controls whether the database allows writes or is read-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AccessMode {
    /// Allow both reads and writes (default).
    #[default]
    ReadWrite,
    /// Read-only mode — all write operations return an error.
    ReadOnly,
}

/// Options for opening a database.
///
/// Use the builder pattern to configure options. Any field set to `Some`
/// overrides the corresponding value in `strata.toml`.
///
/// ```ignore
/// use strata_security::{OpenOptions, AccessMode};
///
/// let opts = OpenOptions::new()
///     .access_mode(AccessMode::ReadOnly)
///     .durability("always");
/// ```
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// The access mode for the database.
    pub access_mode: AccessMode,
    /// Enable automatic text embedding for semantic search.
    /// `None` means "use the config file default".
    pub auto_embed: Option<bool>,
    /// Override durability mode: `"standard"` or `"always"`.
    /// `None` means "use the config file default".
    pub durability: Option<String>,
    /// Override model endpoint (OpenAI-compatible URL).
    pub model_endpoint: Option<String>,
    /// Override model name.
    pub model_name: Option<String>,
    /// Override model API key.
    pub model_api_key: Option<String>,
    /// Override model request timeout in milliseconds.
    pub model_timeout_ms: Option<u64>,
    /// Override embedding batch size for auto-embed.
    /// `None` means "use the config file value, or 512 if unset".
    pub embed_batch_size: Option<usize>,
    /// Enable multi-process coordination mode.
    ///
    /// When `true`, multiple processes can open the same database directory
    /// concurrently. A shared file lock is used instead of an exclusive one,
    /// and commits are coordinated through the WAL.
    ///
    /// Default: `false` (exclusive single-process access).
    pub multi_process: bool,
}

impl OpenOptions {
    /// Create a new `OpenOptions` with default settings (read-write mode).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the access mode for the database.
    pub fn access_mode(mut self, mode: AccessMode) -> Self {
        self.access_mode = mode;
        self
    }

    /// Enable or disable automatic text embedding.
    pub fn auto_embed(mut self, enabled: bool) -> Self {
        self.auto_embed = Some(enabled);
        self
    }

    /// Set the durability mode (`"standard"` or `"always"`).
    pub fn durability(mut self, mode: &str) -> Self {
        self.durability = Some(mode.to_string());
        self
    }

    /// Set the model endpoint and name for query expansion / re-ranking.
    pub fn model(mut self, endpoint: &str, name: &str) -> Self {
        self.model_endpoint = Some(endpoint.to_string());
        self.model_name = Some(name.to_string());
        self
    }

    /// Set the model API key.
    pub fn model_api_key(mut self, key: &str) -> Self {
        self.model_api_key = Some(key.to_string());
        self
    }

    /// Set the model request timeout in milliseconds.
    pub fn model_timeout_ms(mut self, ms: u64) -> Self {
        self.model_timeout_ms = Some(ms);
        self
    }

    /// Set the embedding batch size for auto-embed.
    pub fn embed_batch_size(mut self, size: usize) -> Self {
        self.embed_batch_size = Some(size);
        self
    }

    /// Enable multi-process coordination mode.
    ///
    /// When enabled, multiple processes can open the same database directory
    /// concurrently. Commits are coordinated through the WAL file lock.
    pub fn multi_process(mut self, enabled: bool) -> Self {
        self.multi_process = enabled;
        self
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            access_mode: AccessMode::ReadWrite,
            auto_embed: None,
            durability: None,
            model_endpoint: None,
            model_name: None,
            model_api_key: None,
            model_timeout_ms: None,
            embed_batch_size: None,
            multi_process: false,
        }
    }
}
