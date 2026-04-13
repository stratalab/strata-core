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
use tracing::info;

use super::config::StrataConfig;
use super::Database;
use crate::recovery::Subsystem;

/// Builder for `Database` that accepts an explicit `Subsystem` list.
///
/// Subsystems are recovered in registration order on open and frozen in
/// reverse order on shutdown / drop. The builder routes through the
/// subsystem-aware open paths on `Database`, so the supplied list is the
/// sole driver of recovery for builder-opened databases — there is no
/// implicit hardcoded subsystem.
pub struct DatabaseBuilder {
    subsystems: Vec<Box<dyn Subsystem>>,
}

impl DatabaseBuilder {
    /// Create a new builder with no subsystems.
    pub fn new() -> Self {
        Self {
            subsystems: Vec::new(),
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

    /// Open a disk-backed primary database with the registered subsystems.
    ///
    /// Reads `strata.toml` from the data directory, creating a default
    /// config file if one does not exist. The same subsystem list drives
    /// recovery on open and freeze-on-drop.
    pub fn open<P: AsRef<Path>>(self, path: P) -> StrataResult<Arc<Database>> {
        Database::open_with_subsystems(path, self.subsystems)
    }

    /// Open a disk-backed primary database with an explicit `StrataConfig`
    /// and the registered subsystems.
    ///
    /// The supplied config is written to `strata.toml` so that subsequent
    /// opens pick up the same settings.
    pub fn open_with_config<P: AsRef<Path>>(
        self,
        path: P,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Database>> {
        Database::open_with_config_and_subsystems(path, cfg, self.subsystems)
    }

    /// Open a read-only follower of an existing database with the
    /// registered subsystems.
    ///
    /// Followers do not freeze on drop (`Drop for Database` short-circuits
    /// on `is_follower()`), but the supplied subsystems still drive
    /// recovery.
    pub fn open_follower<P: AsRef<Path>>(self, path: P) -> StrataResult<Arc<Database>> {
        Database::open_follower_with_subsystems(path, self.subsystems)
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
        let db = Database::cache()?;

        // Phase 1: Recovery
        for subsystem in &self.subsystems {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running cache subsystem recovery"
            );
            subsystem.recover(&db)?;
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Cache subsystem recovery complete"
            );
        }

        db.set_subsystems(self.subsystems);

        // Phase 2: Initialize (write-free wiring of hooks/handlers)
        for subsystem in db.installed_subsystems().iter() {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running cache subsystem initialize"
            );
            subsystem.initialize(&db)?;
        }

        // Phase 3: Bootstrap (idempotent first-time writes; cache = not follower)
        for subsystem in db.installed_subsystems().iter() {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running cache subsystem bootstrap"
            );
            subsystem.bootstrap(&db)?;
        }

        db.set_lifecycle_complete();

        Ok(db)
    }
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}
