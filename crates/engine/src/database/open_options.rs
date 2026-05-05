//! Product open access policy.

use serde::{Deserialize, Serialize};

/// Controls whether the product handle allows writes or is read-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AccessMode {
    /// Allow both reads and writes.
    #[default]
    ReadWrite,
    /// Reject write operations at the executor/session boundary.
    ReadOnly,
}

/// Options for opening a database through the product API.
///
/// Configuration settings such as durability, embedding, model provider, and
/// storage resources are managed by [`StrataConfig`](crate::StrataConfig).
/// `OpenOptions` only controls product access policy and follower mode.
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// The access mode for the returned product handle.
    pub access_mode: AccessMode,
    /// Open as a read-only follower of an existing primary instance.
    ///
    /// Followers do not acquire the primary file lock and can open a database
    /// that is already exclusively locked by another process. All write
    /// operations are rejected. Call `refresh()` to see new commits from the
    /// primary.
    pub follower: bool,
}

impl OpenOptions {
    /// Create `OpenOptions` with default read-write, non-follower behavior.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the product access mode.
    pub fn access_mode(mut self, mode: AccessMode) -> Self {
        self.access_mode = mode;
        self
    }

    /// Set whether the database should be opened as a follower.
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
