//! SpaceIndex: Space lifecycle management within branches
//!
//! SpaceIndex tracks which spaces exist within each branch. Spaces are
//! organizational namespaces that provide data isolation within a branch.
//!
//! ## Methods
//!
//! - `register(branch_id, space)` - Idempotent registration of a space
//! - `exists(branch_id, space)` - Check if a space exists ("default" always true)
//! - `list(branch_id)` - List all spaces (always includes "default")
//! - `delete(branch_id, space)` - Delete space metadata key only
//! - `is_empty(branch_id, space)` - Check if a space has any data
//!
//! ## Key Design
//!
//! - Space metadata is stored in the branch-level namespace (not space-scoped)
//!   to avoid circular dependency.
//! - Key format: `<branch_namespace>:<TypeTag::Space>:<space_name>`

use crate::database::Database;
use std::sync::Arc;
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::value::Value;
use strata_core::StrataResult;
use tracing::info;

/// Space lifecycle management primitive.
///
/// SpaceIndex is a stateless facade over the Database engine, holding only
/// an `Arc<Database>` reference. It tracks which spaces exist within each
/// branch and can check whether a space contains any data.
#[derive(Clone)]
pub struct SpaceIndex {
    db: Arc<Database>,
}

impl SpaceIndex {
    /// Create a new SpaceIndex instance.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Idempotent registration of a space.
    ///
    /// Writes a metadata key for the space if it doesn't already exist.
    /// Safe to call repeatedly — only the first call writes.
    pub fn register(&self, branch_id: BranchId, space: &str) -> StrataResult<()> {
        self.db.transaction(branch_id, |txn| {
            let key = Key::new_space(branch_id, space);
            if txn.get(&key)?.is_none() {
                txn.put(key, Value::String("{}".to_string()))?;
                info!(target: "strata::space", space, branch_id = %branch_id, "Space registered");
            }
            Ok(())
        })
    }

    /// Check if a space exists.
    ///
    /// Returns `true` for "default" without hitting storage.
    pub fn exists(&self, branch_id: BranchId, space: &str) -> StrataResult<bool> {
        if space == "default" {
            return Ok(true);
        }
        self.db.transaction(branch_id, |txn| {
            let key = Key::new_space(branch_id, space);
            Ok(txn.get(&key)?.is_some())
        })
    }

    /// List all spaces in a branch.
    ///
    /// Always includes "default" in the result, even if not explicitly registered.
    pub fn list(&self, branch_id: BranchId) -> StrataResult<Vec<String>> {
        self.db.transaction(branch_id, |txn| {
            let prefix = Key::new_space_prefix(branch_id);
            let results = txn.scan_prefix(&prefix)?;

            let mut spaces: Vec<String> = results
                .into_iter()
                .filter_map(|(k, _)| String::from_utf8(k.user_key.to_vec()).ok())
                .collect();

            // Always include "default"
            if !spaces.contains(&"default".to_string()) {
                spaces.insert(0, "default".to_string());
            }

            Ok(spaces)
        })
    }

    /// Delete the space metadata key only.
    ///
    /// The caller is responsible for cleaning up data in the space before
    /// calling this method.
    pub fn delete(&self, branch_id: BranchId, space: &str) -> StrataResult<()> {
        self.db.transaction(branch_id, |txn| {
            let key = Key::new_space(branch_id, space);
            txn.delete(key)?;
            info!(target: "strata::space", space, branch_id = %branch_id, "Space deleted");
            Ok(())
        })
    }

    /// Check if a space has any data.
    ///
    /// **Note:** This is O(N) — scans all data TypeTags (KV, Event,
    /// Json, Vector, Graph) in the space's namespace. Not a cheap check.
    pub fn is_empty(&self, branch_id: BranchId, space: &str) -> StrataResult<bool> {
        self.db.transaction(branch_id, |txn| {
            let ns = Arc::new(Namespace::for_branch_space(branch_id, space));

            for type_tag in [
                TypeTag::KV,
                TypeTag::Event,
                TypeTag::Json,
                TypeTag::Vector,
                TypeTag::Graph,
            ] {
                let prefix = Key::new(ns.clone(), type_tag, vec![]);
                let entries = txn.scan_prefix(&prefix)?;
                if !entries.is_empty() {
                    return Ok(false);
                }
            }

            Ok(true)
        })
    }
}

// ========== Tests ==========

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::KVStore;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, SpaceIndex) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let si = SpaceIndex::new(db.clone());
        (temp_dir, db, si)
    }

    fn default_branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    #[test]
    fn test_register_and_exists() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        assert!(!si.exists(bid, "alpha").unwrap());
        si.register(bid, "alpha").unwrap();
        assert!(si.exists(bid, "alpha").unwrap());
    }

    #[test]
    fn test_list_includes_default() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        let spaces = si.list(bid).unwrap();
        assert!(spaces.contains(&"default".to_string()));
    }

    #[test]
    fn test_register_idempotent() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        si.register(bid, "alpha").unwrap();
        si.register(bid, "alpha").unwrap();

        let spaces = si.list(bid).unwrap();
        let alpha_count = spaces.iter().filter(|s| *s == "alpha").count();
        assert_eq!(alpha_count, 1);
    }

    #[test]
    fn test_is_empty_true_for_new_space() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        si.register(bid, "alpha").unwrap();
        assert!(si.is_empty(bid, "alpha").unwrap());
    }

    #[test]
    fn test_is_empty_false_after_write() {
        let (_temp, db, si) = setup();
        let bid = default_branch();

        si.register(bid, "alpha").unwrap();

        // Write KV data into the "alpha" space
        let kv = KVStore::new(db.clone());
        kv.put(&bid, "alpha", "test-key", Value::Int(42)).unwrap();

        assert!(!si.is_empty(bid, "alpha").unwrap());
    }

    #[test]
    fn test_delete_metadata() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        si.register(bid, "alpha").unwrap();
        assert!(si.exists(bid, "alpha").unwrap());

        si.delete(bid, "alpha").unwrap();
        assert!(!si.exists(bid, "alpha").unwrap());
    }

    #[test]
    fn test_default_always_exists() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        // "default" should always return true without explicit registration
        assert!(si.exists(bid, "default").unwrap());
    }

    #[test]
    fn test_list_with_multiple_spaces() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        si.register(bid, "alpha").unwrap();
        si.register(bid, "beta").unwrap();
        si.register(bid, "gamma").unwrap();

        let spaces = si.list(bid).unwrap();
        assert!(spaces.contains(&"default".to_string()));
        assert!(spaces.contains(&"alpha".to_string()));
        assert!(spaces.contains(&"beta".to_string()));
        assert!(spaces.contains(&"gamma".to_string()));
    }
}
