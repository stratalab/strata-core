//! DatabaseBuilder for subsystem-aware database initialization.
//!
//! The builder is the canonical entry point for opening a `Database` with
//! an explicit `Subsystem` list. The same ordered list drives both
//! recovery (called in registration order) and freeze-on-drop (called in
//! reverse order), so consumers cannot accidentally recover one set of
//! subsystems while freezing a different set.
//!
//! ## Example
//!
//! ```text
//! use strata_engine::{DatabaseBuilder, SearchSubsystem};
//! use strata_vector::VectorSubsystem;
//!
//! let db = DatabaseBuilder::new()
//!     .with_subsystem(VectorSubsystem)
//!     .with_subsystem(SearchSubsystem)
//!     .open("/path/to/data")?;
//! ```

use std::path::Path;
use std::sync::Arc;

use strata_core::StrataResult;

use super::config::StrataConfig;
use super::spec::OpenSpec;
use super::Database;
use crate::recovery::Subsystem;

/// Builder for `Database` that accepts an explicit `Subsystem` list.
///
/// Subsystems are recovered in registration order on open and frozen in
/// reverse order on shutdown / drop. The builder assembles an `OpenSpec`
/// and delegates to `Database::open_runtime`, so the supplied list is the
/// sole driver of recovery for builder-opened databases — there is no
/// implicit hardcoded subsystem.
pub(crate) struct DatabaseBuilder {
    subsystems: Vec<Box<dyn Subsystem>>,
    config: Option<StrataConfig>,
    default_branch: Option<String>,
}

impl DatabaseBuilder {
    /// Create a new builder with no subsystems.
    pub fn new() -> Self {
        Self {
            subsystems: Vec::new(),
            config: None,
            default_branch: None,
        }
    }

    /// Register a subsystem for recovery and freeze hooks.
    ///
    /// Subsystems are recovered in registration order and frozen in
    /// reverse order on shutdown / drop.
    pub fn with_subsystem(mut self, subsystem: impl Subsystem) -> Self {
        self.subsystems.push(Box::new(subsystem));
        self
    }

    /// Set the database configuration.
    ///
    /// If not set, configuration is read from `strata.toml` in the data
    /// directory (or defaults are used if the file doesn't exist).
    pub fn with_config(mut self, config: StrataConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the default branch name.
    ///
    /// The branch is created during bootstrap if it doesn't exist.
    pub fn with_default_branch(mut self, name: impl Into<String>) -> Self {
        self.default_branch = Some(name.into());
        self
    }

    /// Open a disk-backed primary database with the registered subsystems.
    ///
    /// Reads `strata.toml` from the data directory (creating a default
    /// if one does not exist) unless a config was set via `with_config`.
    /// The same subsystem list drives recovery on open and freeze-on-drop.
    pub fn open<P: AsRef<Path>>(self, path: P) -> StrataResult<Arc<Database>> {
        let mut spec = OpenSpec::primary(path).with_subsystems(self.subsystems);
        if let Some(cfg) = self.config {
            spec = spec.with_config(cfg);
        }
        if let Some(branch) = self.default_branch {
            spec = spec.with_default_branch(branch);
        }
        Database::open_runtime(spec)
    }

    /// Open a read-only follower of an existing database with the
    /// registered subsystems.
    ///
    /// Followers do not freeze on drop (`Drop for Database` short-circuits
    /// on `is_follower()`), but the supplied subsystems still drive
    /// recovery.
    pub fn open_follower<P: AsRef<Path>>(self, path: P) -> StrataResult<Arc<Database>> {
        let mut spec = OpenSpec::follower(path).with_subsystems(self.subsystems);
        if let Some(cfg) = self.config {
            spec = spec.with_config(cfg);
        }
        // Note: default_branch is ignored for followers (they don't bootstrap)
        Database::open_runtime(spec)
    }

    /// Create an ephemeral in-memory database with the registered
    /// subsystems.
    ///
    /// Cache databases have no disk files and no WAL, so each subsystem's
    /// disk-recovery and freeze helpers early-return on the empty
    /// `data_dir`. Recovery still runs so any non-disk side effects (e.g.
    /// vector refresh-hook registration) fire, and the subsystems are
    /// installed for freeze symmetry — `Drop for Database` will invoke
    /// the freeze chain on cache databases too, but each freeze step
    /// no-ops on the empty path.
    pub fn cache(self) -> StrataResult<Arc<Database>> {
        let mut spec = OpenSpec::cache().with_subsystems(self.subsystems);
        if let Some(branch) = self.default_branch {
            spec = spec.with_default_branch(branch);
        }
        // Note: config is ignored for cache (ephemeral, no persistence)
        Database::open_runtime(spec)
    }
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}
