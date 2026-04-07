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

    /// Check if a space exists in the branch.
    ///
    /// Returns `true` for the implicit `default` and `_system_` spaces
    /// without hitting storage. Otherwise checks the metadata index first
    /// (cheap) and falls back to a data scan so that legacy or
    /// bypass-written spaces are still discoverable.
    pub fn exists(&self, branch_id: BranchId, space: &str) -> StrataResult<bool> {
        if space == "default" || space == crate::system_space::SYSTEM_SPACE {
            return Ok(true);
        }
        if self.exists_metadata(branch_id, space)? {
            return Ok(true);
        }
        self.has_any_data(branch_id, space)
    }

    /// List all spaces in a branch.
    ///
    /// Returns the union of metadata-registered spaces and spaces
    /// discovered by scanning the data layer. Always includes `default`.
    /// `_system_` is excluded.
    ///
    /// This is administrative — not a hot path. Cost is dominated by
    /// `discover_used_spaces`, which walks every user-data type tag once.
    pub fn list(&self, branch_id: BranchId) -> StrataResult<Vec<String>> {
        use std::collections::BTreeSet;

        let metadata_spaces: BTreeSet<String> =
            self.list_metadata(branch_id)?.into_iter().collect();
        let discovered_spaces: BTreeSet<String> =
            self.discover_used_spaces(branch_id)?.into_iter().collect();

        Ok(metadata_spaces.union(&discovered_spaces).cloned().collect())
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

    /// Discover spaces that contain real user data, regardless of whether
    /// they were ever registered through `register()`.
    ///
    /// Iterates the storage layer for each user-data type tag (KV, Event,
    /// Json, Vector, Graph) at the current MVCC version, collecting the
    /// distinct `space` field of every live (non-tombstone) entry. The
    /// `_system_` shadow space is excluded; `default` is always included.
    ///
    /// Cost: O(branch size) over five iterations. This is an administrative
    /// operation — it powers `SPACE LIST`, branch ops, and startup repair.
    /// No hot path uses it.
    pub fn discover_used_spaces(&self, branch_id: BranchId) -> StrataResult<Vec<String>> {
        use std::collections::BTreeSet;

        let mut spaces: BTreeSet<String> = BTreeSet::new();
        spaces.insert("default".to_string());

        let snapshot = self.db.storage().version();
        for type_tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::Json,
            TypeTag::Vector,
            TypeTag::Graph,
        ] {
            for entry in self
                .db
                .storage()
                .list_by_type_at_version(&branch_id, type_tag, snapshot)
            {
                if entry.is_tombstone {
                    continue;
                }
                let space = entry.key.namespace.space.as_str();
                if space == crate::system_space::SYSTEM_SPACE {
                    continue;
                }
                spaces.insert(space.to_string());
            }
        }
        Ok(spaces.into_iter().collect())
    }

    /// Reconcile metadata with real data: register every space that has
    /// data but no metadata entry. Returns the count of spaces newly
    /// registered. Idempotent — a second call after the first returns 0.
    ///
    /// Called during database startup so that downstream recovery
    /// participants and administrative consumers see a complete set of
    /// spaces, even on legacy databases or after bypass writes.
    pub fn repair_space_metadata(&self, branch_id: BranchId) -> StrataResult<usize> {
        let discovered = self.discover_used_spaces(branch_id)?;
        let mut repaired = 0usize;
        for space in &discovered {
            if space == "default" || space == crate::system_space::SYSTEM_SPACE {
                continue;
            }
            if !self.exists_metadata(branch_id, space)? {
                self.register(branch_id, space)?;
                repaired += 1;
            }
        }
        Ok(repaired)
    }

    /// Internal: metadata-only existence check.
    ///
    /// Used by `exists()` as a fast path before falling back to a data scan,
    /// and by `repair_space_metadata()` to avoid double-writes.
    fn exists_metadata(&self, branch_id: BranchId, space: &str) -> StrataResult<bool> {
        self.db.transaction(branch_id, |txn| {
            let key = Key::new_space(branch_id, space);
            Ok(txn.get(&key)?.is_some())
        })
    }

    /// Internal: metadata-only list of registered spaces (always includes
    /// "default"). Used by `list()` as one half of the union.
    fn list_metadata(&self, branch_id: BranchId) -> StrataResult<Vec<String>> {
        self.db.transaction(branch_id, |txn| {
            let prefix = Key::new_space_prefix(branch_id);
            let results = txn.scan_prefix(&prefix)?;

            let mut spaces: Vec<String> = results
                .into_iter()
                .filter_map(|(k, _)| String::from_utf8(k.user_key.to_vec()).ok())
                .filter(|s| s != crate::system_space::SYSTEM_SPACE)
                .collect();

            if !spaces.contains(&"default".to_string()) {
                spaces.insert(0, "default".to_string());
            }

            Ok(spaces)
        })
    }

    /// Internal: returns true if any user-data entry exists for this
    /// `(branch, space)`. Used by `exists()` as the fallback when metadata
    /// doesn't have the space registered.
    ///
    /// Equivalent to `!is_empty(branch_id, space)?`, but expressed
    /// directly so the call site is self-documenting.
    fn has_any_data(&self, branch_id: BranchId, space: &str) -> StrataResult<bool> {
        Ok(!self.is_empty(branch_id, space)?)
    }
}

/// Atomically ensure a space's metadata key exists within an open
/// transaction.
///
/// Idempotent: writes the metadata key only if it is missing. Skips the
/// implicit `default` and `_system_` spaces. Callers invoke this from
/// inside the same `db.transaction(...)` that performs the data write so
/// that registration is atomic with the user write — no partial state.
///
/// This is the canonical helper used by `EventLog::append`,
/// `KVStore::put`, `JsonStore::create/set`,
/// `VectorStore::create_collection/insert_inner`, and graph mutate paths.
/// Centralising it here ensures every primitive uses the same skip rules
/// and the same key encoding.
pub fn ensure_space_registered_in_txn(
    txn: &mut strata_concurrency::TransactionContext,
    branch_id: &BranchId,
    space: &str,
) -> StrataResult<()> {
    if space == "default" || space == crate::system_space::SYSTEM_SPACE {
        return Ok(());
    }
    let key = Key::new_space(*branch_id, space);
    if txn.get(&key)?.is_none() {
        txn.put(key, Value::String("{}".to_string()))?;
    }
    Ok(())
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

    #[test]
    fn test_system_space_exists() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        // _system_ should always exist without explicit registration
        assert!(si.exists(bid, crate::system_space::SYSTEM_SPACE).unwrap());
    }

    #[test]
    fn test_system_space_hidden_from_list() {
        let (_temp, _db, si) = setup();
        let bid = default_branch();

        let spaces = si.list(bid).unwrap();
        assert!(!spaces.contains(&crate::system_space::SYSTEM_SPACE.to_string()));
    }

    #[test]
    fn test_internal_write_to_system_space() {
        let (_temp, db, _si) = setup();
        let bid = default_branch();

        // Internal code can write to _system_ via system_kv_key
        let key = crate::system_space::system_kv_key(bid, "recipe:default");
        db.transaction(bid, |txn| {
            txn.put(key.clone(), Value::String("{\"version\":1}".into()))?;
            Ok(())
        })
        .unwrap();

        // Read it back and verify content
        let val = db
            .transaction(bid, |txn| txn.get(&key))
            .unwrap()
            .expect("system space value should be readable");
        assert_eq!(val.as_str().unwrap(), "{\"version\":1}");
    }
}
