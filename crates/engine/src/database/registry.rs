//! Global database registry for singleton management
//!
//! Ensures only one Database instance exists per filesystem path.
//! Uses weak references to allow cleanup when all references are dropped.

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Weak;

use super::Database;

// =============================================================================
// Global Database Registry
// =============================================================================
//
// This registry ensures singleton behavior: opening the same database path
// twice returns the same Database instance. This is essential for:
//   1. Avoiding WAL conflicts (two databases writing to same WAL)
//   2. Sharing in-memory state (transactions, caches)
//   3. Consistent behavior across the process
//
// The registry uses weak references so databases are cleaned up when dropped.
//
// Uses parking_lot::Mutex instead of std::sync::Mutex to avoid cascading
// panics from mutex poisoning (issue #1047).

/// Global registry of open databases (path -> weak reference)
pub static OPEN_DATABASES: Lazy<Mutex<HashMap<PathBuf, Weak<Database>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Deterministic removal of a path entry from the registry.
///
/// Used by `Database::shutdown`, which is called explicitly by the owner and
/// is never re-entered from inside `acquire_primary_db`'s guard, so a blocking
/// `lock()` is safe here and guarantees the entry is gone on return.
pub(crate) fn unregister(path: &Path) {
    if path.as_os_str().is_empty() {
        return;
    }
    OPEN_DATABASES.lock().remove(path);
}
