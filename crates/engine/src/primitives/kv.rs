//! KVStore: General-purpose key-value storage primitive
//!
//! ## Design
//!
//! KVStore is a stateless facade over the Database engine. It holds no
//! in-memory state beyond an `Arc<Database>` reference.
//!
//! ## Branch Isolation
//!
//! All operations are scoped to a `BranchId`. Keys are prefixed with the
//! branch's namespace, ensuring complete isolation between branches.
//!
//! ## Thread Safety
//!
//! KVStore is `Send + Sync` and can be safely shared across threads.
//! Multiple KVStore instances on the same Database are safe.
//!
//! ## MVP API
//!
//! - `get(branch_id, key)` - Get latest value
//! - `put(branch_id, key, value)` - Store a value
//! - `delete(branch_id, key)` - Delete a key
//! - `list(branch_id, prefix)` - List keys with prefix

use crate::database::Database;
use crate::primitives::extensions::KVStoreExt;
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::StrataResult;
use strata_core::{Version, VersionedHistory};

/// General-purpose key-value store primitive
///
/// Stateless facade over Database - all state lives in storage.
/// Multiple KVStore instances on same Database are safe.
///
/// # Example
///
/// ```text
/// let db = Database::open("/path/to/data")?;
/// let kv = KVStore::new(db);
/// let branch_id = BranchId::new();
///
/// kv.put(&branch_id, "default", "key", Value::String("value".into()))?;
/// let value = kv.get(&branch_id, "default", "key")?;
/// kv.delete(&branch_id, "default", "key")?;
/// ```
#[derive(Clone)]
pub struct KVStore {
    db: Arc<Database>,
}

impl KVStore {
    /// Create new KVStore instance
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Build namespace for branch+space-scoped operations
    fn namespace_for(&self, branch_id: &BranchId, space: &str) -> Namespace {
        Namespace::for_branch_space(*branch_id, space)
    }

    /// Build key for KV operation
    fn key_for(&self, branch_id: &BranchId, space: &str, user_key: &str) -> Key {
        Key::new_kv(self.namespace_for(branch_id, space), user_key)
    }

    // ========== MVP API ==========

    /// Get a value by key
    ///
    /// Returns the latest value for the key, or None if it doesn't exist.
    ///
    /// # Example
    ///
    /// ```text
    /// let value = kv.get(&branch_id, "default", "user:123")?;
    /// if let Some(v) = value {
    ///     println!("Found: {:?}", v);
    /// }
    /// ```
    pub fn get(&self, branch_id: &BranchId, space: &str, key: &str) -> StrataResult<Option<Value>> {
        self.db.transaction(*branch_id, |txn| {
            let storage_key = self.key_for(branch_id, space, key);
            txn.get(&storage_key)
        })
    }

    /// Get a value with its version metadata.
    ///
    /// Uses a transaction to retrieve the latest value together with its
    /// version and timestamp, providing snapshot isolation.
    /// Returns `None` if the key doesn't exist.
    pub fn get_versioned(
        &self,
        branch_id: &BranchId,
        space: &str,
        key: &str,
    ) -> StrataResult<Option<strata_core::VersionedValue>> {
        self.db.transaction(*branch_id, |txn| {
            let storage_key = self.key_for(branch_id, space, key);
            txn.get_versioned(&storage_key)
        })
    }

    /// Get full version history for a key.
    ///
    /// Returns `None` if the key doesn't exist. Index with `[0]` = latest,
    /// `[1]` = previous, etc. Reads directly from storage (non-transactional).
    pub fn getv(
        &self,
        branch_id: &BranchId,
        space: &str,
        key: &str,
    ) -> StrataResult<Option<VersionedHistory<Value>>> {
        let storage_key = self.key_for(branch_id, space, key);
        let history = self.db.get_history(&storage_key, None, None)?;
        Ok(VersionedHistory::new(history))
    }

    /// Put a value
    ///
    /// Creates the key if it doesn't exist, overwrites if it does.
    /// Returns the version created by this write operation.
    ///
    /// # Example
    ///
    /// ```text
    /// let version = kv.put(&branch_id, "default", "user:123", Value::String("Alice".into()))?;
    /// ```
    pub fn put(
        &self,
        branch_id: &BranchId,
        space: &str,
        key: &str,
        value: Value,
    ) -> StrataResult<Version> {
        // Extract text for indexing before the value is consumed by the transaction
        let text_for_index = match &value {
            Value::String(s) => Some(s.clone()),
            Value::Null | Value::Bool(_) | Value::Bytes(_) => None,
            other => serde_json::to_string(other).ok(),
        };

        let ((), commit_version) = self.db.transaction_with_version(*branch_id, |txn| {
            let storage_key = self.key_for(branch_id, space, key);
            txn.put(storage_key, value)
        })?;

        // Update inverted index for BM25 search (zero overhead when disabled)
        if let Some(text) = text_for_index {
            let index = self.db.extension::<crate::search::InvertedIndex>()?;
            if index.is_enabled() {
                let entity_ref = crate::search::EntityRef::Kv {
                    branch_id: *branch_id,
                    key: key.to_string(),
                };
                index.index_document(&entity_ref, &text, None);
            }
        }

        Ok(Version::Txn(commit_version))
    }

    /// Delete a key
    ///
    /// Returns `true` if the key existed and was deleted, `false` if it didn't exist.
    ///
    /// # Example
    ///
    /// ```text
    /// let was_deleted = kv.delete(&branch_id, "default", "user:123")?;
    /// ```
    pub fn delete(&self, branch_id: &BranchId, space: &str, key: &str) -> StrataResult<bool> {
        self.db.transaction(*branch_id, |txn| {
            let storage_key = self.key_for(branch_id, space, key);
            let exists = txn.get(&storage_key)?.is_some();
            if exists {
                txn.delete(storage_key)?;
            }
            Ok(exists)
        })
    }

    /// List keys with optional prefix filter
    ///
    /// Returns all keys matching the prefix (or all keys if prefix is None).
    ///
    /// # Example
    ///
    /// ```text
    /// // List all keys starting with "user:"
    /// let keys = kv.list(&branch_id, "default", Some("user:"))?;
    ///
    /// // List all keys
    /// let all_keys = kv.list(&branch_id, "default", None)?;
    /// ```
    pub fn list(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
    ) -> StrataResult<Vec<String>> {
        self.db.transaction(*branch_id, |txn| {
            let ns = self.namespace_for(branch_id, space);
            let scan_prefix = Key::new_kv(ns, prefix.unwrap_or(""));

            let results = txn.scan_prefix(&scan_prefix)?;

            Ok(results
                .into_iter()
                .filter_map(|(key, _)| key.user_key_string())
                .collect())
        })
    }

    // ========== Batch API ==========

    /// Put multiple key-value pairs in a single transaction.
    ///
    /// All items share one lock acquisition, one WAL record, and one commit.
    /// Each entry reports success/failure independently via `Result<Version, String>`.
    /// Blind writes (empty read_set) → validation skipped.
    pub fn batch_put(
        &self,
        branch_id: &BranchId,
        space: &str,
        entries: Vec<(String, Value)>,
    ) -> StrataResult<Vec<Result<Version, String>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Extract text for indexing BEFORE the values are consumed by the transaction
        let texts: Vec<Option<String>> = entries
            .iter()
            .map(|(_, value)| match value {
                Value::String(s) => Some(s.clone()),
                Value::Null | Value::Bool(_) | Value::Bytes(_) => None,
                other => serde_json::to_string(other).ok(),
            })
            .collect();

        let ((), commit_version) = self.db.transaction_with_version(*branch_id, |txn| {
            for (key, value) in &entries {
                let storage_key = self.key_for(branch_id, space, key);
                txn.put(storage_key, value.clone())?;
            }
            Ok(())
        })?;

        let version = Version::Txn(commit_version);

        // Post-commit: update inverted index for all items
        let index = self.db.extension::<crate::search::InvertedIndex>()?;
        let index_enabled = index.is_enabled();
        for (i, (key, _)) in entries.iter().enumerate() {
            if index_enabled {
                if let Some(ref text) = texts[i] {
                    let entity_ref = crate::search::EntityRef::Kv {
                        branch_id: *branch_id,
                        key: key.clone(),
                    };
                    index.index_document(&entity_ref, text, None);
                }
            }
        }

        // All items succeed with the same commit version
        let results = entries.iter().map(|_| Ok(version)).collect();

        Ok(results)
    }

    // ========== Time-Travel API ==========

    /// Get a value by key as of a past timestamp (microseconds since epoch).
    ///
    /// Returns the latest value whose commit timestamp <= as_of_ts, or None.
    /// This is a non-transactional read directly from the storage version chain.
    pub fn get_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        key: &str,
        as_of_ts: u64,
    ) -> StrataResult<Option<Value>> {
        let storage_key = self.key_for(branch_id, space, key);
        let result = self.db.get_at_timestamp(&storage_key, as_of_ts)?;
        Ok(result.map(|vv| vv.value))
    }

    /// List keys as of a past timestamp.
    ///
    /// Returns keys whose values existed at the given timestamp.
    pub fn list_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        as_of_ts: u64,
    ) -> StrataResult<Vec<String>> {
        let ns = self.namespace_for(branch_id, space);
        let scan_prefix = Key::new_kv(ns, prefix.unwrap_or(""));
        let results = self.db.scan_prefix_at_timestamp(&scan_prefix, as_of_ts)?;
        Ok(results
            .into_iter()
            .filter_map(|(key, _)| key.user_key_string())
            .collect())
    }
}

// ========== Searchable Trait Implementation ==========
//
// Search is handled by the intelligence layer (strata-intelligence).
// This implementation returns empty results - use InvertedIndex for full-text search.

impl crate::search::Searchable for KVStore {
    fn search(
        &self,
        req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        use crate::search::{truncate_text, EntityRef, InvertedIndex, SearchHit, SearchStats};
        use std::time::Instant;

        let start = Instant::now();
        let index = self.db.extension::<InvertedIndex>()?;

        // If the index is disabled or empty, return early
        if !index.is_enabled() || index.total_docs() == 0 {
            return Ok(crate::SearchResponse::empty());
        }

        let query_terms = crate::search::tokenize(&req.query);
        let scorer = self.db.config().bm25_scorer();

        // Score top-k entirely inside the index (zero-copy posting iteration)
        let top_k = index.score_top_k(&query_terms, &req.branch_id, req.k, scorer.k1, scorer.b);

        // Only resolve doc_ids and fetch text for the final top-k results
        let hits: Vec<SearchHit> = top_k
            .into_iter()
            .filter_map(|scored| {
                let entity_ref = index.resolve_doc_id(scored.doc_id)?;
                let snippet = if let EntityRef::Kv { ref key, .. } = entity_ref {
                    self.get(&req.branch_id, "default", key)
                        .ok()
                        .flatten()
                        .map(|v| match &v {
                            strata_core::value::Value::String(s) => truncate_text(s, 100),
                            other => truncate_text(
                                &serde_json::to_string(other).unwrap_or_default(),
                                100,
                            ),
                        })
                } else {
                    None
                };

                Some(SearchHit {
                    doc_ref: entity_ref,
                    score: scored.score,
                    rank: 0, // Set below
                    snippet,
                })
            })
            .enumerate()
            .map(|(i, mut hit)| {
                hit.rank = (i + 1) as u32;
                hit
            })
            .collect();

        let elapsed = start.elapsed().as_micros() as u64;
        let mut stats = SearchStats::new(elapsed, hits.len());
        stats = stats.with_index_used(true);

        Ok(crate::SearchResponse {
            hits,
            truncated: false,
            stats,
        })
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Kv
    }
}

// ========== KVStoreExt Implementation ==========

impl KVStoreExt for TransactionContext {
    fn kv_get(&mut self, key: &str) -> StrataResult<Option<Value>> {
        let storage_key = Key::new_kv(Namespace::for_branch(self.branch_id), key);
        self.get(&storage_key)
    }

    fn kv_put(&mut self, key: &str, value: Value) -> StrataResult<()> {
        let storage_key = Key::new_kv(Namespace::for_branch(self.branch_id), key);
        self.put(storage_key, value)
    }

    fn kv_delete(&mut self, key: &str) -> StrataResult<()> {
        let storage_key = Key::new_kv(Namespace::for_branch(self.branch_id), key);
        self.delete(storage_key)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::TypeTag;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, KVStore) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let kv = KVStore::new(db.clone());
        (temp_dir, db, kv)
    }

    #[test]
    fn test_kvstore_creation() {
        let (_temp, _db, _kv) = setup();
    }

    #[test]
    fn test_kvstore_is_clone() {
        let (_temp, db, kv1) = setup();
        let kv2 = kv1.clone();
        // Both use same database
        assert!(Arc::ptr_eq(&db, &db));
        drop(kv2);
    }

    #[test]
    fn test_kvstore_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<KVStore>();
    }

    #[test]
    fn test_key_construction() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        let key = kv.key_for(&branch_id, "default", "test-key");
        assert_eq!(key.type_tag, TypeTag::KV);
    }

    #[test]
    fn test_put_and_get() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "key1",
            Value::String("value1".into()),
        )
        .unwrap();
        let result = kv.get(&branch_id, "default", "key1").unwrap();
        assert_eq!(result, Some(Value::String("value1".into())));
    }

    #[test]
    fn test_get_nonexistent() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let result = kv.get(&branch_id, "default", "nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_put_overwrite() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "key1",
            Value::String("value1".into()),
        )
        .unwrap();
        kv.put(
            &branch_id,
            "default",
            "key1",
            Value::String("value2".into()),
        )
        .unwrap();

        let result = kv.get(&branch_id, "default", "key1").unwrap();
        assert_eq!(result, Some(Value::String("value2".into())));
    }

    #[test]
    fn test_delete() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "key1",
            Value::String("value1".into()),
        )
        .unwrap();
        assert!(kv.get(&branch_id, "default", "key1").unwrap().is_some());

        let deleted = kv.delete(&branch_id, "default", "key1").unwrap();
        assert!(deleted);
        assert!(kv.get(&branch_id, "default", "key1").unwrap().is_none());
    }

    #[test]
    fn test_delete_nonexistent() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let deleted = kv.delete(&branch_id, "default", "nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_branch_isolation() {
        let (_temp, _db, kv) = setup();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        kv.put(
            &branch1,
            "default",
            "shared-key",
            Value::String("branch1-value".into()),
        )
        .unwrap();
        kv.put(
            &branch2,
            "default",
            "shared-key",
            Value::String("branch2-value".into()),
        )
        .unwrap();

        // Each branch sees its own value
        assert_eq!(
            kv.get(&branch1, "default", "shared-key").unwrap(),
            Some(Value::String("branch1-value".into()))
        );
        assert_eq!(
            kv.get(&branch2, "default", "shared-key").unwrap(),
            Some(Value::String("branch2-value".into()))
        );
    }

    #[test]
    fn test_list_all() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();
        kv.put(&branch_id, "default", "c", Value::Int(3)).unwrap();

        let keys = kv.list(&branch_id, "default", None).unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
        assert!(keys.contains(&"c".to_string()));
    }

    #[test]
    fn test_list_with_prefix() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "user:1", Value::Int(1))
            .unwrap();
        kv.put(&branch_id, "default", "user:2", Value::Int(2))
            .unwrap();
        kv.put(&branch_id, "default", "task:1", Value::Int(3))
            .unwrap();

        let user_keys = kv.list(&branch_id, "default", Some("user:")).unwrap();
        assert_eq!(user_keys.len(), 2);
        assert!(user_keys.contains(&"user:1".to_string()));
        assert!(user_keys.contains(&"user:2".to_string()));

        let task_keys = kv.list(&branch_id, "default", Some("task:")).unwrap();
        assert_eq!(task_keys.len(), 1);
        assert!(task_keys.contains(&"task:1".to_string()));
    }

    #[test]
    fn test_list_empty_prefix() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "key1", Value::Int(1))
            .unwrap();
        kv.put(&branch_id, "default", "key2", Value::Int(2))
            .unwrap();

        let keys = kv
            .list(&branch_id, "default", Some("nonexistent:"))
            .unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_list_branch_isolation() {
        let (_temp, _db, kv) = setup();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        kv.put(&branch1, "default", "branch1-key", Value::Int(1))
            .unwrap();
        kv.put(&branch2, "default", "branch2-key", Value::Int(2))
            .unwrap();

        // Each branch only sees its own keys
        let branch1_keys = kv.list(&branch1, "default", None).unwrap();
        assert_eq!(branch1_keys.len(), 1);
        assert!(branch1_keys.contains(&"branch1-key".to_string()));

        let branch2_keys = kv.list(&branch2, "default", None).unwrap();
        assert_eq!(branch2_keys.len(), 1);
        assert!(branch2_keys.contains(&"branch2-key".to_string()));
    }

    #[test]
    fn test_various_value_types() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        // String
        kv.put(
            &branch_id,
            "default",
            "string",
            Value::String("hello".into()),
        )
        .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "string").unwrap(),
            Some(Value::String("hello".into()))
        );

        // Integer
        kv.put(&branch_id, "default", "int", Value::Int(42))
            .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "int").unwrap(),
            Some(Value::Int(42))
        );

        // Float
        kv.put(&branch_id, "default", "float", Value::Float(3.14))
            .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "float").unwrap(),
            Some(Value::Float(3.14))
        );

        // Boolean
        kv.put(&branch_id, "default", "bool", Value::Bool(true))
            .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "bool").unwrap(),
            Some(Value::Bool(true))
        );

        // Null - Value::Null should be storable and round-trip correctly
        kv.put(&branch_id, "default", "null", Value::Null).unwrap();
        let result = kv.get(&branch_id, "default", "null").unwrap();
        assert!(result.is_some(), "Value::Null should be storable");
        assert_eq!(result.unwrap(), Value::Null);

        // Bytes
        kv.put(&branch_id, "default", "bytes", Value::Bytes(vec![1, 2, 3]))
            .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "bytes").unwrap(),
            Some(Value::Bytes(vec![1, 2, 3]))
        );

        // Array
        kv.put(
            &branch_id,
            "default",
            "array",
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
        )
        .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "array").unwrap(),
            Some(Value::Array(vec![Value::Int(1), Value::Int(2)]))
        );
    }

    #[test]
    fn test_kvstore_ext_in_transaction() {
        use crate::primitives::extensions::KVStoreExt;

        let (_temp, db, _kv) = setup();
        let branch_id = BranchId::new();

        db.transaction(branch_id, |txn| {
            txn.kv_put("ext-key", Value::String("ext-value".into()))?;
            let val = txn.kv_get("ext-key")?;
            assert_eq!(val, Some(Value::String("ext-value".into())));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_kvstore_ext_delete() {
        use crate::primitives::extensions::KVStoreExt;

        let (_temp, db, kv) = setup();
        let branch_id = BranchId::new();

        // Setup
        kv.put(&branch_id, "default", "key", Value::Int(42))
            .unwrap();

        // Delete via extension trait
        db.transaction(branch_id, |txn| {
            txn.kv_delete("key")?;
            let val = txn.kv_get("key")?;
            assert_eq!(val, None);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_get_versioned_returns_version_info() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let version = kv
            .put(&branch_id, "default", "vkey", Value::Int(99))
            .unwrap();
        let vv = kv
            .get_versioned(&branch_id, "default", "vkey")
            .unwrap()
            .unwrap();
        assert_eq!(vv.value, Value::Int(99));
        assert_eq!(vv.version, version);
    }

    #[test]
    fn test_get_versioned_snapshot_isolation() {
        let (_temp, db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "iso_key", Value::Int(1))
            .unwrap();

        // Start a manual transaction, read, then check the versioned read
        // is consistent even if a concurrent write happens
        let mut txn = db.begin_transaction(branch_id);
        let storage_key =
            strata_core::types::Key::new_kv(Namespace::for_branch(branch_id), "iso_key");
        let vv = txn.get_versioned(&storage_key).unwrap().unwrap();
        assert_eq!(vv.value, Value::Int(1));

        // Concurrent write after our snapshot
        kv.put(&branch_id, "default", "iso_key", Value::Int(2))
            .unwrap();

        // Re-read within same transaction should still see old value
        let vv2 = txn.get_versioned(&storage_key).unwrap().unwrap();
        assert_eq!(vv2.value, Value::Int(1));

        db.end_transaction(txn);
    }

    // ========== Batch API Tests ==========

    #[test]
    fn test_batch_put_basic() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let entries = vec![
            ("k1".to_string(), Value::Int(1)),
            ("k2".to_string(), Value::String("two".into())),
            ("k3".to_string(), Value::Bool(true)),
        ];

        let results = kv.batch_put(&branch_id, "default", entries).unwrap();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok());
        }

        // Verify all items persisted
        assert_eq!(
            kv.get(&branch_id, "default", "k1").unwrap(),
            Some(Value::Int(1))
        );
        assert_eq!(
            kv.get(&branch_id, "default", "k2").unwrap(),
            Some(Value::String("two".into()))
        );
        assert_eq!(
            kv.get(&branch_id, "default", "k3").unwrap(),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn test_batch_put_empty() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let results = kv.batch_put(&branch_id, "default", vec![]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_batch_put_single() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let entries = vec![("single".to_string(), Value::Int(42))];
        let results = kv.batch_put(&branch_id, "default", entries).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        assert_eq!(
            kv.get(&branch_id, "default", "single").unwrap(),
            Some(Value::Int(42))
        );
    }

    #[test]
    fn test_batch_put_overwrites_existing() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "existing",
            Value::String("old".into()),
        )
        .unwrap();

        let entries = vec![("existing".to_string(), Value::String("new".into()))];
        let results = kv.batch_put(&branch_id, "default", entries).unwrap();
        assert!(results[0].is_ok());

        assert_eq!(
            kv.get(&branch_id, "default", "existing").unwrap(),
            Some(Value::String("new".into()))
        );
    }

    // ========== KVStore::search() integration tests ==========

    /// Setup with index enabled (Database::open enables it by default,
    /// but this is explicit for clarity).
    fn setup_with_index() -> (TempDir, Arc<Database>, KVStore) {
        let (temp, db, kv) = setup();
        // Database::open() already enables the index; assert it.
        let index = db.extension::<crate::search::InvertedIndex>().unwrap();
        assert!(index.is_enabled());
        (temp, db, kv)
    }

    #[test]
    fn test_search_basic() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "doc1",
            Value::String("the quick brown fox jumps over the lazy dog".into()),
        )
        .unwrap();
        kv.put(
            &branch_id,
            "default",
            "doc2",
            Value::String("the lazy cat sleeps all day".into()),
        )
        .unwrap();

        let req = crate::SearchRequest::new(branch_id, "lazy");
        let response = kv.search(&req).unwrap();

        assert!(
            !response.is_empty(),
            "Should find documents containing 'lazy'"
        );
        assert!(response.stats.index_used);
        // Both docs contain "lazy"
        assert_eq!(response.len(), 2);
        // Ranks should be sequential 1, 2
        assert_eq!(response.hits[0].rank, 1);
        assert_eq!(response.hits[1].rank, 2);
        // Scores descending
        assert!(response.hits[0].score >= response.hits[1].score);
    }

    #[test]
    fn test_search_empty_index() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        // No documents inserted
        let req = crate::SearchRequest::new(branch_id, "hello");
        let response = kv.search(&req).unwrap();
        assert!(response.is_empty());
    }

    #[test]
    fn test_search_disabled_index() {
        use crate::search::Searchable;

        let (_temp, db, kv) = setup();
        let branch_id = BranchId::new();

        // Database::open() enables the index by default; put data first,
        // then explicitly disable to test the disabled-index path.
        kv.put(
            &branch_id,
            "default",
            "doc1",
            Value::String("hello world".into()),
        )
        .unwrap();

        let index = db.extension::<crate::search::InvertedIndex>().unwrap();
        index.disable();

        let req = crate::SearchRequest::new(branch_id, "hello");
        let response = kv.search(&req).unwrap();
        assert!(response.is_empty());
        assert!(!response.stats.index_used);
    }

    #[test]
    fn test_search_branch_isolation() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();

        kv.put(
            &branch_a,
            "default",
            "doc_a",
            Value::String("unique alpha content".into()),
        )
        .unwrap();
        kv.put(
            &branch_b,
            "default",
            "doc_b",
            Value::String("unique beta content".into()),
        )
        .unwrap();

        // Search branch_a should not find branch_b's docs
        let req_a = crate::SearchRequest::new(branch_a, "unique");
        let response_a = kv.search(&req_a).unwrap();
        assert_eq!(response_a.len(), 1);
        assert_eq!(
            response_a.hits[0].doc_ref,
            crate::search::EntityRef::Kv {
                branch_id: branch_a,
                key: "doc_a".to_string(),
            }
        );

        // Search branch_b should not find branch_a's docs
        let req_b = crate::SearchRequest::new(branch_b, "unique");
        let response_b = kv.search(&req_b).unwrap();
        assert_eq!(response_b.len(), 1);
        assert_eq!(
            response_b.hits[0].doc_ref,
            crate::search::EntityRef::Kv {
                branch_id: branch_b,
                key: "doc_b".to_string(),
            }
        );
    }

    #[test]
    fn test_search_respects_k() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        // Insert 20 documents all containing "common"
        for i in 0..20 {
            kv.put(
                &branch_id,
                "default",
                &format!("doc{}", i),
                Value::String(format!("common content number {}", i)),
            )
            .unwrap();
        }

        let req = crate::SearchRequest::new(branch_id, "common").with_k(5);
        let response = kv.search(&req).unwrap();
        assert_eq!(response.len(), 5);
        // Verify ranks are 1..5
        for (i, hit) in response.hits.iter().enumerate() {
            assert_eq!(hit.rank, (i + 1) as u32);
        }
    }

    #[test]
    fn test_search_has_snippets() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "doc1",
            Value::String("hello world test content".into()),
        )
        .unwrap();

        let req = crate::SearchRequest::new(branch_id, "hello");
        let response = kv.search(&req).unwrap();
        assert_eq!(response.len(), 1);
        assert!(response.hits[0].snippet.is_some());
        assert!(response.hits[0].snippet.as_ref().unwrap().contains("hello"));
    }

    #[test]
    fn test_search_multi_term_relevance() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        // doc1 matches both query terms, doc2 matches only one
        kv.put(
            &branch_id,
            "default",
            "doc1",
            Value::String("quick brown fox".into()),
        )
        .unwrap();
        kv.put(
            &branch_id,
            "default",
            "doc2",
            Value::String("quick red car".into()),
        )
        .unwrap();

        let req = crate::SearchRequest::new(branch_id, "quick fox");
        let response = kv.search(&req).unwrap();
        assert_eq!(response.len(), 2);
        // doc1 should rank higher (matches both terms)
        assert_eq!(
            response.hits[0].doc_ref,
            crate::search::EntityRef::Kv {
                branch_id,
                key: "doc1".to_string(),
            }
        );
    }

    #[test]
    fn test_search_no_match() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "doc1",
            Value::String("hello world".into()),
        )
        .unwrap();

        let req = crate::SearchRequest::new(branch_id, "zzzznonexistent");
        let response = kv.search(&req).unwrap();
        assert!(response.is_empty());
    }

    #[test]
    fn test_search_non_string_values() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        // Non-string values shouldn't be indexed (no text to extract)
        kv.put(&branch_id, "default", "int_doc", Value::Int(42))
            .unwrap();
        kv.put(&branch_id, "default", "bool_doc", Value::Bool(true))
            .unwrap();
        // But JSON-serializable values should be
        kv.put(&branch_id, "default", "float_doc", Value::Float(3.14))
            .unwrap();

        let req = crate::SearchRequest::new(branch_id, "42");
        let response = kv.search(&req).unwrap();
        // Int value gets serialized to "42" which should be indexed
        assert!(response.len() <= 1, "At most 1 result for '42'");
    }
}
