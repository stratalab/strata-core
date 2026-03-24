//! Secondary indices for efficient query patterns
//!
//! This module provides secondary indices that enable efficient queries
//! without scanning the entire BTreeMap:
//! - BranchIndex: Maps BranchId → Set<Key> for fast branch-scoped queries (critical for replay)
//! - TypeIndex: Maps TypeTag → Set<Key> for primitive-specific queries

use std::collections::{HashMap, HashSet};
use strata_core::{BranchId, Key, TypeTag};

/// Secondary index: BranchId → Keys
///
/// Enables efficient scan_by_branch queries for replay by maintaining
/// a mapping from each BranchId to all keys belonging to that branch.
/// This changes scan_by_branch from O(total data) to O(branch size).
#[derive(Debug, Default)]
pub struct BranchIndex {
    index: HashMap<BranchId, HashSet<Key>>,
}

impl BranchIndex {
    /// Create a new empty BranchIndex
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Add key to branch's index
    ///
    /// Inserts the key into the set of keys for the given branch_id.
    /// If the branch_id doesn't exist yet, creates a new entry.
    pub fn insert(&mut self, branch_id: BranchId, key: Key) {
        self.index.entry(branch_id).or_default().insert(key);
    }

    /// Remove key from branch's index
    ///
    /// Removes the key from the set for the given branch_id.
    /// If the set becomes empty, removes the branch_id entry entirely
    /// to avoid accumulating empty sets.
    pub fn remove(&mut self, branch_id: BranchId, key: &Key) {
        if let Some(keys) = self.index.get_mut(&branch_id) {
            keys.remove(key);
            if keys.is_empty() {
                self.index.remove(&branch_id);
            }
        }
    }

    /// Get all keys for a branch
    ///
    /// Returns a reference to the set of keys for the given branch_id,
    /// or None if no keys exist for that branch.
    pub fn get(&self, branch_id: &BranchId) -> Option<&HashSet<Key>> {
        self.index.get(branch_id)
    }

    /// Remove all keys for a branch (for cleanup)
    ///
    /// Removes the entire entry for a branch_id, useful for
    /// cleaning up after a branch is complete.
    pub fn remove_branch(&mut self, branch_id: &BranchId) {
        self.index.remove(branch_id);
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Get the number of branches in the index
    pub fn len(&self) -> usize {
        self.index.len()
    }
}

/// Secondary index: TypeTag → Keys
///
/// Enables efficient queries by primitive type by maintaining
/// a mapping from each TypeTag to all keys of that type.
/// Useful for queries like "all events" or "all KV entries".
#[derive(Debug, Default)]
pub struct TypeIndex {
    index: HashMap<TypeTag, HashSet<Key>>,
}

impl TypeIndex {
    /// Create a new empty TypeIndex
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Add key to type's index
    ///
    /// Inserts the key into the set of keys for the given type_tag.
    /// If the type_tag doesn't exist yet, creates a new entry.
    pub fn insert(&mut self, type_tag: TypeTag, key: Key) {
        self.index.entry(type_tag).or_default().insert(key);
    }

    /// Remove key from type's index
    ///
    /// Removes the key from the set for the given type_tag.
    /// If the set becomes empty, removes the type_tag entry entirely.
    pub fn remove(&mut self, type_tag: TypeTag, key: &Key) {
        if let Some(keys) = self.index.get_mut(&type_tag) {
            keys.remove(key);
            if keys.is_empty() {
                self.index.remove(&type_tag);
            }
        }
    }

    /// Get all keys for a type
    ///
    /// Returns a reference to the set of keys for the given type_tag,
    /// or None if no keys exist for that type.
    pub fn get(&self, type_tag: &TypeTag) -> Option<&HashSet<Key>> {
        self.index.get(type_tag)
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Get the number of types in the index
    pub fn len(&self) -> usize {
        self.index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::Namespace;

    /// Helper to create a test namespace
    fn test_namespace(branch_id: BranchId) -> Arc<Namespace> {
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    // ========================================
    // BranchIndex Tests
    // ========================================

    #[test]
    fn test_branch_index_insert_and_get() {
        let mut index = BranchIndex::new();
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key1 = Key::new_kv(ns.clone(), "key1");
        let key2 = Key::new_kv(ns.clone(), "key2");

        // Insert two keys for the same branch
        index.insert(branch_id, key1.clone());
        index.insert(branch_id, key2.clone());

        // Verify both keys are in the index
        let keys = index.get(&branch_id).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
    }

    #[test]
    fn test_branch_index_remove() {
        let mut index = BranchIndex::new();
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key1 = Key::new_kv(ns.clone(), "key1");
        let key2 = Key::new_kv(ns.clone(), "key2");

        // Insert two keys
        index.insert(branch_id, key1.clone());
        index.insert(branch_id, key2.clone());

        // Remove one key
        index.remove(branch_id, &key1);

        // Verify only key2 remains
        let keys = index.get(&branch_id).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(!keys.contains(&key1));
        assert!(keys.contains(&key2));

        // Remove the last key - set should be cleaned up
        index.remove(branch_id, &key2);
        assert!(index.get(&branch_id).is_none());
        assert!(index.is_empty());
    }

    #[test]
    fn test_branch_index_multiple_branches() {
        let mut index = BranchIndex::new();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let ns1 = test_namespace(branch1);
        let ns2 = test_namespace(branch2);

        let key1 = Key::new_kv(ns1.clone(), "key1");
        let key2 = Key::new_kv(ns2.clone(), "key2");

        index.insert(branch1, key1.clone());
        index.insert(branch2, key2.clone());

        // Verify each branch has its own key
        assert_eq!(index.get(&branch1).unwrap().len(), 1);
        assert_eq!(index.get(&branch2).unwrap().len(), 1);
        assert!(index.get(&branch1).unwrap().contains(&key1));
        assert!(index.get(&branch2).unwrap().contains(&key2));
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_branch_index_remove_branch() {
        let mut index = BranchIndex::new();
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);

        index.insert(branch_id, Key::new_kv(ns.clone(), "key1"));
        index.insert(branch_id, Key::new_kv(ns.clone(), "key2"));

        // Remove entire branch
        index.remove_branch(&branch_id);

        assert!(index.get(&branch_id).is_none());
        assert!(index.is_empty());
    }

    #[test]
    fn test_branch_index_default() {
        let index = BranchIndex::default();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    // ========================================
    // TypeIndex Tests
    // ========================================

    #[test]
    fn test_type_index_insert_and_get() {
        let mut index = TypeIndex::new();
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key1 = Key::new_kv(ns.clone(), "key1");
        let key2 = Key::new_kv(ns.clone(), "key2");

        // Insert two KV keys
        index.insert(TypeTag::KV, key1.clone());
        index.insert(TypeTag::KV, key2.clone());

        // Verify both keys are in the index
        let keys = index.get(&TypeTag::KV).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
    }

    #[test]
    fn test_type_index_remove() {
        let mut index = TypeIndex::new();
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);
        let key1 = Key::new_kv(ns.clone(), "key1");
        let key2 = Key::new_kv(ns.clone(), "key2");

        // Insert two keys
        index.insert(TypeTag::KV, key1.clone());
        index.insert(TypeTag::KV, key2.clone());

        // Remove one key
        index.remove(TypeTag::KV, &key1);

        // Verify only key2 remains
        let keys = index.get(&TypeTag::KV).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(!keys.contains(&key1));
        assert!(keys.contains(&key2));

        // Remove the last key - set should be cleaned up
        index.remove(TypeTag::KV, &key2);
        assert!(index.get(&TypeTag::KV).is_none());
        assert!(index.is_empty());
    }

    #[test]
    fn test_type_index_multiple_types() {
        let mut index = TypeIndex::new();
        let branch_id = BranchId::new();
        let ns = test_namespace(branch_id);

        let kv_key = Key::new_kv(ns.clone(), "data");
        let event_key = Key::new_event(ns.clone(), 1);
        index.insert(TypeTag::KV, kv_key.clone());
        index.insert(TypeTag::Event, event_key.clone());

        // Verify each type has its own key
        assert_eq!(index.get(&TypeTag::KV).unwrap().len(), 1);
        assert_eq!(index.get(&TypeTag::Event).unwrap().len(), 1);
        assert!(index.get(&TypeTag::KV).unwrap().contains(&kv_key));
        assert!(index.get(&TypeTag::Event).unwrap().contains(&event_key));
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_type_index_default() {
        let index = TypeIndex::default();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }
}
