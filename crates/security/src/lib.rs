//! Access control and configuration for Strata database.
//!
//! This crate provides the [`AccessMode`] and [`OpenOptions`] types used to
//! control how a database is opened and what operations are permitted.

#![warn(missing_docs)]

mod sensitive;

pub use sensitive::SensitiveString;

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
/// Configuration settings (durability, auto_embed, model, etc.) are managed
/// via `CONFIG SET`/`CONFIG GET` and persisted in `strata.toml`. `OpenOptions`
/// only controls access mode and multi-process coordination.
///
/// ```ignore
/// use strata_security::{OpenOptions, AccessMode};
///
/// let opts = OpenOptions::new()
///     .access_mode(AccessMode::ReadOnly);
/// ```
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// The access mode for the database.
    pub access_mode: AccessMode,
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
            multi_process: false,
        }
    }
}
