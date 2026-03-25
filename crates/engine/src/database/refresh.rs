//! Refresh hook trait for follower refresh.
//!
//! Secondary index subsystems (vector, search) implement this trait
//! to participate in incremental follower refresh without the engine
//! needing to know the concrete types.
//!
//! ## Usage
//!
//! 1. Implement `RefreshHook` for your subsystem's state type
//! 2. Register the state as a Database extension (`db.extension::<MyState>()`)
//! 3. Register the hook with `db.register_refresh_hook()`
//! 4. `Database::refresh()` will call your hook automatically

use std::sync::Arc;
use strata_core::types::Key;
use strata_core::value::Value;
use strata_core::StrataResult;

/// Trait for secondary index backends that participate in follower refresh.
///
/// Implementations handle incremental updates from WAL replay during
/// `Database::refresh()`. The engine calls hooks in two phases:
///
/// 1. `pre_delete_read`: Before storage mutations, read state needed for deletes
/// 2. `apply_refresh`: After storage mutations, apply puts and deletes
pub trait RefreshHook: Send + Sync + 'static {
    /// Pre-read any state needed for processing deletes.
    ///
    /// Called BEFORE storage mutations are applied. Returns opaque pre-read
    /// data that will be passed to `apply_refresh`. The storage still has
    /// the old values at this point.
    fn pre_delete_read(&self, db: &super::Database, deletes: &[Key]) -> Vec<(Key, Vec<u8>)>;

    /// Apply puts and deletes from a single WAL record.
    ///
    /// Called AFTER storage mutations are applied.
    fn apply_refresh(&self, puts: &[(Key, Value)], pre_read_deletes: &[(Key, Vec<u8>)]);

    /// Freeze in-memory state to disk for fast recovery on next open.
    fn freeze_to_disk(&self, db: &super::Database) -> StrataResult<()>;

    /// Reload in-memory state after a branch merge.
    ///
    /// Called after branch merge completes to reload vector backends
    /// for the target branch from KV storage.
    fn post_merge_reload(
        &self,
        _db: &super::Database,
        _target_branch: strata_core::types::BranchId,
        _source_branch: Option<strata_core::types::BranchId>,
    ) -> StrataResult<()> {
        Ok(())
    }
}

/// Container for registered refresh hooks.
///
/// Stored as a Database extension and accessed during `refresh()`.
pub struct RefreshHooks {
    hooks: parking_lot::RwLock<Vec<Arc<dyn RefreshHook>>>,
}

impl Default for RefreshHooks {
    fn default() -> Self {
        Self {
            hooks: parking_lot::RwLock::new(Vec::new()),
        }
    }
}

impl RefreshHooks {
    /// Register a new refresh hook.
    pub fn register(&self, hook: Arc<dyn RefreshHook>) {
        self.hooks.write().push(hook);
    }

    /// Get all registered hooks.
    pub fn hooks(&self) -> Vec<Arc<dyn RefreshHook>> {
        self.hooks.read().clone()
    }
}
