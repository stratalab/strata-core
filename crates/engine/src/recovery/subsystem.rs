//! Subsystem trait for pluggable recovery and shutdown hooks.
//!
//! Subsystems are independent components (vector index, search index, etc.)
//! that need to rebuild state on database open and persist state on shutdown.
//!
//! ## Usage
//!
//! ```text
//! use strata_engine::{DatabaseBuilder, Subsystem};
//!
//! struct MySubsystem;
//! impl Subsystem for MySubsystem {
//!     fn name(&self) -> &'static str { "my-subsystem" }
//!     fn recover(&self, db: &Arc<Database>) -> StrataResult<()> { Ok(()) }
//! }
//!
//! let db = DatabaseBuilder::new()
//!     .with_subsystem(MySubsystem)
//!     .open("/path/to/data")?;
//! ```

use crate::database::Database;
use std::sync::Arc;
use strata_core::StrataResult;

/// Trait for subsystems that need recovery on open and cleanup on shutdown.
///
/// Each subsystem registers itself with the `DatabaseBuilder` before the
/// database is opened. During open, `recover()` is called after WAL replay
/// completes. During shutdown (or drop), `freeze()` is called to persist
/// in-memory state for fast recovery on next open.
pub trait Subsystem: Send + Sync + 'static {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Called after WAL recovery completes during database open.
    ///
    /// Rebuild in-memory state from persistent storage (KV scan, mmap files, etc.).
    fn recover(&self, db: &Arc<Database>) -> StrataResult<()>;

    /// Called during shutdown/drop for cleanup and persistence.
    ///
    /// Freeze in-memory state to disk for fast recovery on next open.
    /// Default implementation does nothing.
    fn freeze(&self, db: &Database) -> StrataResult<()> {
        let _ = db;
        Ok(())
    }
}
