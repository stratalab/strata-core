//! DatabaseBuilder for subsystem-aware database initialization.
//!
//! The builder pattern replaces global static recovery registration with
//! explicit subsystem wiring. Subsystems implement the `Subsystem` trait
//! for recovery on open and freeze on shutdown.
//!
//! ## Example
//!
//! ```text
//! use strata_engine::{DatabaseBuilder, VectorSubsystem, SearchSubsystem};
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

/// Builder for Database that accepts subsystem registrations.
///
/// Subsystems are called in registration order during recovery (open)
/// and in reverse order during freeze (shutdown/drop).
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

    /// Register a subsystem for recovery and shutdown hooks.
    ///
    /// Subsystems are recovered in registration order and frozen in reverse order.
    pub fn with_subsystem(mut self, subsystem: impl Subsystem) -> Self {
        self.subsystems.push(Box::new(subsystem));
        self
    }

    /// Open a disk-backed database with registered subsystems.
    ///
    /// Reads `strata.toml` from the data directory. Creates the directory and
    /// default config if they don't exist.
    ///
    /// Note: `Database::open()` runs its own recovery internally. The builder's
    /// subsystems replace the default freeze hooks so that shutdown/drop calls
    /// the correct subsystem implementations.
    pub fn open<P: AsRef<Path>>(self, path: P) -> StrataResult<Arc<Database>> {
        let db = Database::open(path)?;
        db.set_subsystems(self.subsystems);
        Ok(db)
    }

    /// Open a disk-backed database with an explicit configuration.
    pub fn open_with_config<P: AsRef<Path>>(
        self,
        path: P,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Database>> {
        let db = Database::open_with_config(path, cfg)?;
        db.set_subsystems(self.subsystems);
        Ok(db)
    }

    /// Create an ephemeral in-memory database with registered subsystems.
    ///
    /// No disk files, no WAL, no recovery. Subsystems are still registered
    /// for freeze-on-drop behavior.
    pub fn cache(self, enable_search: bool) -> StrataResult<Arc<Database>> {
        let db = Database::cache()?;
        if enable_search {
            self.run_subsystem_recovery(&db)?;
        }
        db.set_subsystems(self.subsystems);
        Ok(db)
    }

    /// Run recovery for all registered subsystems.
    fn run_subsystem_recovery(&self, db: &Database) -> StrataResult<()> {
        for subsystem in &self.subsystems {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running subsystem recovery"
            );
            subsystem.recover(db)?;
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Subsystem recovery complete"
            );
        }
        Ok(())
    }
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}
