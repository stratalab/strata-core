//! Access control and configuration for Strata database.
//!
//! This crate provides the [`AccessMode`] and [`OpenOptions`] types used to
//! control how a database is opened and what operations are permitted.

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
/// only controls access mode and follower mode.
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
    /// Open as a read-only follower of an existing primary instance.
    ///
    /// Followers do not acquire any file lock and can open a database
    /// that is already exclusively locked by another process. All write
    /// operations are rejected. Call `refresh()` to see new commits
    /// from the primary.
    ///
    /// Default: `false`.
    pub follower: bool,
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

    /// Open as a read-only follower of an existing primary instance.
    ///
    /// Followers do not acquire any file lock and can open a database
    /// that is already exclusively locked by another process. All write
    /// operations are rejected. Call `refresh()` to see new commits
    /// from the primary.
    pub fn follower(mut self, enabled: bool) -> Self {
        self.follower = enabled;
        self
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            access_mode: AccessMode::ReadWrite,
            follower: false,
        }
    }
}
