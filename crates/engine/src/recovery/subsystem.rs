//! Subsystem trait for pluggable recovery and shutdown hooks.
//!
//! Subsystems are independent components (vector index, search index, etc.)
//! that need to rebuild state on database open and persist state on shutdown.
//!
//! ## Lifecycle
//!
//! During database open, subsystems go through these phases in order:
//!
//! 1. **recover** — Rebuild in-memory state from persistent storage
//! 2. **initialize** — Write-free wiring (install hooks, observers, handlers)
//! 3. **bootstrap** — Idempotent open-time writes (create system state)
//!
//! During shutdown, `freeze()` is called in reverse order.
//!
//! ## Mode-Gating
//!
//! - `recover()` and `initialize()` run for all modes
//! - `bootstrap()` runs only for Primary and Cache modes (not Follower)
//! - Followers read state from shared storage; they don't create it
//!
//! ## Usage
//!
//! ```text
//! use strata_engine::{Database, OpenSpec, Subsystem};
//!
//! struct MySubsystem;
//! impl Subsystem for MySubsystem {
//!     fn name(&self) -> &'static str { "my-subsystem" }
//!     fn recover(&self, db: &Arc<Database>) -> StrataResult<()> { Ok(()) }
//! }
//!
//! let db = Database::open_runtime(
//!     OpenSpec::primary("/path/to/data").with_subsystem(MySubsystem)
//! )?;
//! ```

use crate::database::Database;
use crate::StrataResult;
use std::sync::Arc;
use strata_core::BranchId;

/// Trait for subsystems that need recovery on open and cleanup on shutdown.
///
/// Each subsystem is added to the `OpenSpec` via `with_subsystem()` before
/// the database is opened. The lifecycle methods are called in this order:
///
/// 1. `recover()` — after WAL replay, rebuild in-memory state
/// 2. `initialize()` — install hooks, observers, handlers (no writes)
/// 3. `bootstrap()` — create initial state (primary/cache only)
///
/// During shutdown (or drop), `freeze()` is called to persist in-memory state.
pub trait Subsystem: Send + Sync + 'static {
    /// Human-readable name for logging and `CompatibilitySignature`.
    fn name(&self) -> &'static str;

    /// Called after WAL recovery completes during database open.
    ///
    /// Rebuild in-memory state from persistent storage (KV scan, mmap files, etc.).
    /// This is the first lifecycle method called on a subsystem.
    fn recover(&self, db: &Arc<Database>) -> StrataResult<()>;

    /// Called after `recover()` to wire up hooks, observers, and handlers.
    ///
    /// This method must be **write-free**: it only installs callbacks and
    /// registers state, but does not perform any storage writes. This ensures
    /// followers can safely call `initialize()` without creating state.
    ///
    /// ## What to do here
    ///
    /// - Install merge handlers into the per-Database `MergeHandlerRegistry`
    /// - Install `BranchDagHook` via `db.install_dag_hook()`
    /// - Register `CommitObserver`, `ReplayObserver`, `BranchOpObserver`
    /// - Wire up any other per-Database callbacks
    ///
    /// ## What NOT to do here
    ///
    /// - Create branches or write to storage
    /// - Initialize system state (that belongs in `bootstrap`)
    ///
    /// Default implementation does nothing.
    fn initialize(&self, db: &Arc<Database>) -> StrataResult<()> {
        let _ = db;
        Ok(())
    }

    /// Called after `initialize()` to create initial state.
    ///
    /// This method performs **idempotent writes**: it creates system state
    /// (branches, collections, indexes) if they don't already exist.
    ///
    /// ## Mode-Gating
    ///
    /// - **Primary/Cache:** `bootstrap()` is called
    /// - **Follower:** `bootstrap()` is skipped (reads state from primary)
    ///
    /// ## What to do here
    ///
    /// - Create `_system_` branch and branch DAG structures
    /// - Create default collections or indexes
    /// - Any other idempotent initialization writes
    ///
    /// ## Idempotence
    ///
    /// `bootstrap()` must be safe to call multiple times. Check for existence
    /// before creating, or use idempotent operations (upsert, create-if-missing).
    ///
    /// Default implementation does nothing.
    fn bootstrap(&self, db: &Arc<Database>) -> StrataResult<()> {
        let _ = db;
        Ok(())
    }

    /// Called after a branch has been deleted and its storage has been cleared.
    ///
    /// Subsystems use this hook to purge any branch-owned in-memory state or
    /// sidecar disk caches that are not part of the core storage namespace.
    /// Implementations must be idempotent because the branch deletion is already
    /// committed when this hook runs.
    fn cleanup_deleted_branch(
        &self,
        db: &Arc<Database>,
        branch_id: &BranchId,
        branch_name: &str,
    ) -> StrataResult<()> {
        let _ = (db, branch_id, branch_name);
        Ok(())
    }

    /// Called during shutdown/drop for cleanup and persistence.
    ///
    /// Freeze in-memory state to disk for fast recovery on next open.
    /// Default implementation does nothing.
    fn freeze(&self, db: &Database) -> StrataResult<()> {
        let _ = db;
        Ok(())
    }
}
