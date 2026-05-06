//! System space: hidden, internal-only namespace on every branch.
//!
//! Follows the `_graph_` pattern in `crates/engine/src/graph/keys.rs`. Used to store
//! recipes, semantic profiles, and other internal metadata per-branch.
//!
//! - Users cannot write to `_system_` (validation rejects the name)
//! - `SpaceIndex::list()` filters it out
//! - `SpaceIndex::exists("_system_")` returns true
//! - Internal code uses `system_kv_key()` to write directly via `Database`

use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;
use strata_core::BranchId;
use strata_storage::{Key, Namespace};

/// The reserved space name for internal system data.
pub const SYSTEM_SPACE: &str = "_system_";

/// Global cache of `Arc<Namespace>` per branch — one heap allocation per branch.
static NS_CACHE: Lazy<DashMap<BranchId, Arc<Namespace>>> = Lazy::new(DashMap::new);

/// Build a namespace for system operations on a given branch.
///
/// Returns a cached `Arc<Namespace>` — one heap allocation per branch, ever.
/// Subsequent calls return `Arc::clone()` (atomic refcount bump only).
pub fn system_namespace(branch_id: BranchId) -> Arc<Namespace> {
    NS_CACHE
        .entry(branch_id)
        .or_insert_with(|| Arc::new(Namespace::for_branch_space(branch_id, SYSTEM_SPACE)))
        .clone()
}

/// Build a KV key in the `_system_` space for a given branch.
pub fn system_kv_key(branch_id: BranchId, key: &str) -> Key {
    Key::new_kv(system_namespace(branch_id), key)
}

/// Remove cached namespace entry for a branch (call on branch deletion).
pub fn invalidate_cache(branch_id: &BranchId) {
    NS_CACHE.remove(branch_id);
}

// ========== Tests ==========

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_namespace_cached() {
        let branch = BranchId::new();
        let ns1 = system_namespace(branch);
        let ns2 = system_namespace(branch);
        // Same Arc (pointer equality)
        assert!(Arc::ptr_eq(&ns1, &ns2));
        assert_eq!(ns1.space, SYSTEM_SPACE);
    }

    #[test]
    fn test_system_namespace_different_branches() {
        let b1 = BranchId::new();
        let b2 = BranchId::new();
        let ns1 = system_namespace(b1);
        let ns2 = system_namespace(b2);
        assert!(!Arc::ptr_eq(&ns1, &ns2));
        assert_eq!(ns1.space, SYSTEM_SPACE);
        assert_eq!(ns2.space, SYSTEM_SPACE);
    }

    #[test]
    fn test_system_kv_key() {
        let branch = BranchId::new();
        let key = system_kv_key(branch, "recipe:default");
        assert_eq!(key.user_key_string(), Some("recipe:default".to_string()));
    }

    #[test]
    fn test_invalidate_cache() {
        let branch = BranchId::new();
        let ns_before = system_namespace(branch);
        invalidate_cache(&branch);
        let ns_after = system_namespace(branch);
        // Same value, different allocation
        assert_eq!(*ns_before, *ns_after);
        assert!(!Arc::ptr_eq(&ns_before, &ns_after));
    }
}
