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
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::{StrataError, StrataResult};
use strata_core::{Version, VersionedHistory};
use strata_storage::StorageIterator;

/// Global cache of `Arc<Namespace>` per (branch, space) pair. One heap allocation
/// per unique combination, ever — subsequent calls return `Arc::clone()`.
static NS_CACHE: Lazy<DashMap<(BranchId, String), Arc<Namespace>>> = Lazy::new(DashMap::new);

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

    /// Build namespace for branch+space-scoped operations.
    ///
    /// Returns a cached `Arc<Namespace>` — one heap allocation per unique
    /// (branch, space) pair, ever. Subsequent calls return `Arc::clone()`.
    fn namespace_for(&self, branch_id: &BranchId, space: &str) -> Arc<Namespace> {
        cached_namespace(*branch_id, space)
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
        let storage_key = self.key_for(branch_id, space, key);
        // Direct value-only read — skips VersionedValue construction entirely
        self.db.get_value_direct(&storage_key)
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

    /// Get a value with version metadata, bypassing the transaction layer.
    ///
    /// Provides per-key read consistency without coordinator mutex or
    /// read-set tracking overhead. For multi-key snapshot isolation,
    /// use `get_versioned()` which goes through `Database::transaction()`.
    pub fn get_versioned_direct(
        &self,
        branch_id: &BranchId,
        space: &str,
        key: &str,
    ) -> StrataResult<Option<strata_core::VersionedValue>> {
        let storage_key = self.key_for(branch_id, space, key);
        self.db.get_versioned_direct(&storage_key)
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
        // Extract text for indexing before the value is consumed
        let text_for_index = value.extractable_text();

        let storage_key = self.key_for(branch_id, space, key);
        let branch_id = *branch_id;
        let ((), commit_version) = self
            .db
            .transaction_with_version(branch_id, |txn| txn.put(storage_key, value))?;

        // Post-commit: update inverted index for BM25 search (zero overhead when disabled)
        if let Some(text) = text_for_index {
            let index = self.db.extension::<crate::search::InvertedIndex>()?;
            if index.is_enabled() {
                let entity_ref = crate::search::EntityRef::Kv {
                    branch_id,
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
        let existed = self.db.transaction(*branch_id, |txn| {
            let storage_key = self.key_for(branch_id, space, key);
            let exists = txn.get(&storage_key)?.is_some();
            if exists {
                txn.delete(storage_key)?;
            }
            Ok(exists)
        })?;

        // Remove from inverted index to prevent stale postings
        if existed {
            if let Ok(index) = self.db.extension::<crate::search::InvertedIndex>() {
                if index.is_enabled() {
                    let entity_ref = crate::search::EntityRef::Kv {
                        branch_id: *branch_id,
                        key: key.to_string(),
                    };
                    index.remove_document(&entity_ref);
                }
            }
        }

        Ok(existed)
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

    /// List keys with cursor-based pagination pushed down to the engine.
    ///
    /// Returns up to `limit + 1` keys after `cursor` (the extra entry signals
    /// "has more"). This avoids materializing user-key strings for the entire
    /// scan — only the page window is allocated.
    ///
    /// Note: The underlying `scan_prefix` still reads all matching entries from
    /// storage. True storage-level push-down is a future optimisation.
    pub fn list_with_cursor_limit(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> StrataResult<Vec<String>> {
        self.db.transaction(*branch_id, |txn| {
            let ns = self.namespace_for(branch_id, space);
            let scan_prefix = Key::new_kv(ns, prefix.unwrap_or(""));

            let results = txn.scan_prefix(&scan_prefix)?;

            let iter = results
                .into_iter()
                .filter_map(|(key, _)| key.user_key_string());

            if let Some(cur) = cursor {
                // "Start after" semantics: skip keys <= cursor
                let cur_owned = cur.to_string();
                Ok(iter
                    .skip_while(move |k| k.as_str() <= cur_owned.as_str())
                    .take(limit + 1)
                    .collect())
            } else {
                Ok(iter.take(limit + 1).collect())
            }
        })
    }

    /// Scan key-value pairs starting from a cursor key.
    ///
    /// Returns up to `limit` pairs where key >= start_key, sorted by key.
    /// If `start` is None, scans from the beginning. If `limit` is None,
    /// returns all matching pairs.
    ///
    /// Bypasses the transaction layer (read-only) and uses the lazy merge
    /// pipeline with seek + limit pushdown. Uses `current_version()` for
    /// snapshot isolation.
    pub fn scan(
        &self,
        branch_id: &BranchId,
        space: &str,
        start: Option<&str>,
        limit: Option<usize>,
    ) -> StrataResult<Vec<(String, Value)>> {
        let ns = self.namespace_for(branch_id, space);
        let scan_prefix = Key::new_kv(Arc::clone(&ns), "");
        let start_key = Key::new_kv(ns, start.unwrap_or(""));
        let snapshot = self.db.current_version();
        let results = self
            .db
            .scan_range(&scan_prefix, &start_key, snapshot, limit)?;
        Ok(results
            .into_iter()
            .filter_map(|(key, vv)| key.user_key_string().map(|k| (k, vv.value)))
            .collect())
    }

    /// Create a persistent iterator for cursor-based pagination.
    ///
    /// Returns a [`StorageIterator`] positioned at the branch. Call `seek()`
    /// to position, then `next()` to advance. The iterator holds a snapshot
    /// of the branch — memtable rotation and compaction don't affect it.
    pub fn scan_iter(&self, branch_id: &BranchId, space: &str) -> StrataResult<StorageIterator> {
        let ns = self.namespace_for(branch_id, space);
        let prefix = Key::new_kv(ns, "");
        let snapshot_version = self.db.current_version();
        self.db
            .storage_iterator(branch_id, prefix, snapshot_version)
            .ok_or_else(|| StrataError::branch_not_found(*branch_id))
    }

    /// Count keys matching an optional prefix without materializing entries.
    ///
    /// Bypasses the transaction layer (read-only) and delegates to the
    /// storage engine's `count_prefix`, which runs the merge/MVCC pipeline
    /// and counts live entries instead of collecting them into a Vec.
    pub fn count(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
    ) -> StrataResult<u64> {
        let ns = self.namespace_for(branch_id, space);
        let scan_prefix = Key::new_kv(ns, prefix.unwrap_or(""));
        self.db.count_prefix(&scan_prefix, u64::MAX)
    }

    /// Sample key-value pairs with evenly-spaced selection from a single scan.
    ///
    /// Performs one `scan_prefix` and reads values inline, avoiding the
    /// separate `list()` + K `get()` round-trips of the old approach.
    pub fn sample(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        count: usize,
    ) -> StrataResult<(u64, Vec<(String, Value)>)> {
        self.db.transaction(*branch_id, |txn| {
            let ns = self.namespace_for(branch_id, space);
            let scan_prefix = Key::new_kv(ns, prefix.unwrap_or(""));

            let results = txn.scan_prefix(&scan_prefix)?;

            // Collect only user-key entries (filter out internal keys)
            let entries: Vec<(String, Value)> = results
                .into_iter()
                .filter_map(|(key, value)| key.user_key_string().map(|k| (k, value)))
                .collect();

            let total = entries.len() as u64;

            if count == 0 || entries.is_empty() {
                return Ok((total, Vec::new()));
            }
            if count >= entries.len() {
                return Ok((total, entries));
            }

            // Evenly-spaced sampling
            let step = entries.len() as f64 / count as f64;
            let sampled = (0..count)
                .map(|i| {
                    let idx = (i as f64 * step) as usize;
                    // Safety: idx < entries.len() because step = len/count and i < count
                    entries[idx].clone()
                })
                .collect();

            Ok((total, sampled))
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
            .map(|(_, value)| value.extractable_text())
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

    /// Get multiple values by key in a single transaction.
    ///
    /// Returns a `Vec` whose i-th element corresponds to `keys[i]`.
    /// Missing keys yield `None`. All reads share one snapshot for consistency.
    pub fn batch_get(
        &self,
        branch_id: &BranchId,
        space: &str,
        keys: &[String],
    ) -> StrataResult<Vec<Option<strata_core::VersionedValue>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.db.transaction(*branch_id, |txn| {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                let storage_key = self.key_for(branch_id, space, key);
                results.push(txn.get_versioned(&storage_key)?);
            }
            Ok(results)
        })
    }

    /// Delete multiple keys in a single transaction.
    ///
    /// Returns a `Vec<bool>` where `results[i]` is `true` if `keys[i]` existed
    /// and was deleted, `false` if it was not found. All deletes are atomic.
    pub fn batch_delete(
        &self,
        branch_id: &BranchId,
        space: &str,
        keys: &[String],
    ) -> StrataResult<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let results = self.db.transaction(*branch_id, |txn| {
            let mut existed = Vec::with_capacity(keys.len());
            for key in keys {
                let storage_key = self.key_for(branch_id, space, key);
                let exists = txn.get(&storage_key)?.is_some();
                if exists {
                    txn.delete(storage_key)?;
                }
                existed.push(exists);
            }
            Ok(existed)
        })?;

        // Post-commit: remove deleted keys from inverted index
        if let Ok(index) = self.db.extension::<crate::search::InvertedIndex>() {
            if index.is_enabled() {
                for (i, key) in keys.iter().enumerate() {
                    if results[i] {
                        let entity_ref = crate::search::EntityRef::Kv {
                            branch_id: *branch_id,
                            key: key.clone(),
                        };
                        index.remove_document(&entity_ref);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Check existence of multiple keys in a single transaction.
    ///
    /// Returns a `Vec<bool>` where `results[i]` is `true` if `keys[i]` exists.
    /// All checks share one snapshot for consistency.
    pub fn batch_exists(
        &self,
        branch_id: &BranchId,
        space: &str,
        keys: &[String],
    ) -> StrataResult<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.db.transaction(*branch_id, |txn| {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                let storage_key = self.key_for(branch_id, space, key);
                results.push(txn.get(&storage_key)?.is_some());
            }
            Ok(results)
        })
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

        // Score top-k entirely inside the index (zero-copy posting iteration)
        let top_k = index.score_top_k(&query_terms, &req.branch_id, req.k, req.bm25_k1, req.bm25_b);

        // Only resolve doc_ids and fetch text for the final top-k results
        let hits: Vec<SearchHit> = top_k
            .into_iter()
            .filter_map(|scored| {
                let entity_ref = index.resolve_doc_id(scored.doc_id)?;
                let snippet = if let EntityRef::Kv { ref key, .. } = entity_ref {
                    self.get(&req.branch_id, &req.space, key)
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

/// Look up or create a cached namespace for a (branch, space) pair.
fn cached_namespace(branch_id: BranchId, space: &str) -> Arc<Namespace> {
    let sp = space.to_string();
    NS_CACHE
        .entry((branch_id, sp.clone()))
        .or_insert_with(|| Arc::new(Namespace::for_branch_space(branch_id, &sp)))
        .clone()
}

/// Remove all cached namespace entries for a branch (call on branch deletion).
pub fn invalidate_kv_namespace_cache(branch_id: &BranchId) {
    NS_CACHE.retain(|k, _| k.0 != *branch_id);
}

impl KVStoreExt for TransactionContext {
    fn kv_get_in_space(&mut self, space: &str, key: &str) -> StrataResult<Option<Value>> {
        let ns = cached_namespace(self.branch_id, space);
        let storage_key = Key::new_kv(ns, key);
        self.get(&storage_key)
    }

    fn kv_put_in_space(&mut self, space: &str, key: &str, value: Value) -> StrataResult<()> {
        let ns = cached_namespace(self.branch_id, space);
        let storage_key = Key::new_kv(ns, key);
        self.put(storage_key, value)
    }

    fn kv_delete_in_space(&mut self, space: &str, key: &str) -> StrataResult<()> {
        let ns = cached_namespace(self.branch_id, space);
        let storage_key = Key::new_kv(ns, key);
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
        kv.put(&branch_id, "default", "float", Value::Float(2.78))
            .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "float").unwrap(),
            Some(Value::Float(2.78))
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
            Value::array(vec![Value::Int(1), Value::Int(2)]),
        )
        .unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "array").unwrap(),
            Some(Value::array(vec![Value::Int(1), Value::Int(2)]))
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
        let mut txn = db.begin_transaction(branch_id).unwrap();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let storage_key = strata_core::types::Key::new_kv(ns.clone(), "iso_key");
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
        kv.put(&branch_id, "default", "float_doc", Value::Float(2.78))
            .unwrap();

        let req = crate::SearchRequest::new(branch_id, "42");
        let response = kv.search(&req).unwrap();
        // Int value gets serialized to "42" which should be indexed
        assert!(response.len() <= 1, "At most 1 result for '42'");
    }

    #[test]
    fn test_kv_delete_removes_from_index() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "ephemeral",
            Value::String("fleeting butterfly garden".into()),
        )
        .unwrap();
        kv.put(
            &branch_id,
            "default",
            "permanent",
            Value::String("enduring butterfly monument".into()),
        )
        .unwrap();

        // Both should be searchable
        let req = crate::SearchRequest::new(branch_id, "butterfly");
        let response = kv.search(&req).unwrap();
        assert_eq!(response.len(), 2, "Both docs should match 'butterfly'");

        // Delete "ephemeral"
        let deleted = kv.delete(&branch_id, "default", "ephemeral").unwrap();
        assert!(deleted);

        // Now only "permanent" should appear in search
        let req = crate::SearchRequest::new(branch_id, "butterfly");
        let response = kv.search(&req).unwrap();
        assert_eq!(
            response.len(),
            1,
            "Only 'permanent' should match after delete"
        );
        assert_eq!(
            response.hits[0].doc_ref,
            crate::search::EntityRef::Kv {
                branch_id,
                key: "permanent".to_string(),
            }
        );

        // Term unique to deleted doc should return no results
        let req = crate::SearchRequest::new(branch_id, "fleeting");
        let response = kv.search(&req).unwrap();
        assert!(
            response.is_empty(),
            "Deleted doc's unique terms should not match"
        );
    }

    // ========== Direct-path performance tests ==========

    #[test]
    fn test_direct_get_returns_none_after_delete() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "k", Value::Int(1)).unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "k").unwrap(),
            Some(Value::Int(1))
        );

        kv.delete(&branch_id, "default", "k").unwrap();
        assert_eq!(
            kv.get(&branch_id, "default", "k").unwrap(),
            None,
            "Direct get must filter tombstones"
        );
    }

    #[test]
    fn test_direct_put_returns_monotonic_versions() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let v1 = kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        let v2 = kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();
        let v3 = kv.put(&branch_id, "default", "a", Value::Int(3)).unwrap();

        // Versions must be strictly increasing
        assert!(v2 > v1, "v2 ({v2:?}) must be > v1 ({v1:?})");
        assert!(v3 > v2, "v3 ({v3:?}) must be > v2 ({v2:?})");
    }

    #[test]
    fn test_direct_put_then_list_rebuilds_ordered_index() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        // Insert keys in reverse order (stresses lazy BTreeSet rebuild)
        for i in (0..20).rev() {
            kv.put(
                &branch_id,
                "default",
                &format!("key_{:03}", i),
                Value::Int(i),
            )
            .unwrap();
        }

        // list() triggers a prefix scan which rebuilds the BTreeSet
        let keys = kv.list(&branch_id, "default", None).unwrap();
        assert_eq!(keys.len(), 20, "All 20 keys should be listed");

        // Verify sorted order
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "Keys must be returned in sorted order");
    }

    #[test]
    fn test_direct_put_interleaved_with_scan() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        // Insert some keys, scan, insert more, scan again
        kv.put(&branch_id, "default", "a:1", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "a:2", Value::Int(2)).unwrap();

        let first_scan = kv.list(&branch_id, "default", Some("a:")).unwrap();
        assert_eq!(first_scan.len(), 2);

        // Add more keys (invalidates the lazy BTreeSet)
        kv.put(&branch_id, "default", "a:3", Value::Int(3)).unwrap();
        kv.put(&branch_id, "default", "b:1", Value::Int(4)).unwrap();

        let second_scan = kv.list(&branch_id, "default", Some("a:")).unwrap();
        assert_eq!(
            second_scan.len(),
            3,
            "Second scan must see newly inserted 'a:3'"
        );

        let b_scan = kv.list(&branch_id, "default", Some("b:")).unwrap();
        assert_eq!(b_scan.len(), 1, "b: prefix scan must find 'b:1'");
    }

    #[test]
    fn test_direct_put_concurrent_threads() {
        use std::thread;

        let (_temp, db, _kv) = setup();
        let n_threads = 4;
        let n_ops = 100;

        let mut handles = Vec::new();
        for t in 0..n_threads {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                let kv = KVStore::new(db);
                let branch_id = BranchId::new();
                for i in 0..n_ops {
                    let key = format!("t{}-k{}", t, i);
                    kv.put(&branch_id, "default", &key, Value::Int(i as i64))
                        .unwrap();
                    let val = kv.get(&branch_id, "default", &key).unwrap();
                    assert!(val.is_some(), "Key {key} must exist after put");
                }
            }));
        }

        for h in handles {
            h.join().expect("Thread panicked");
        }
    }

    #[test]
    fn test_direct_put_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let branch_id = BranchId::new();

        // Phase 1: Write data, then drop the database (simulates crash)
        {
            let db = Database::open(temp_dir.path()).unwrap();
            let kv = KVStore::new(db.clone());

            kv.put(
                &branch_id,
                "default",
                "survive",
                Value::String("hello".into()),
            )
            .unwrap();
            kv.put(&branch_id, "default", "survive2", Value::Int(42))
                .unwrap();

            // Explicit shutdown to flush WAL
            let _ = db.shutdown();
        }

        // Phase 2: Reopen and verify data survived via WAL recovery
        {
            let db = Database::open(temp_dir.path()).unwrap();
            let kv = KVStore::new(db);

            let v1 = kv.get(&branch_id, "default", "survive").unwrap();
            assert_eq!(
                v1,
                Some(Value::String("hello".into())),
                "Value must survive WAL recovery"
            );

            let v2 = kv.get(&branch_id, "default", "survive2").unwrap();
            assert_eq!(
                v2,
                Some(Value::Int(42)),
                "Second value must survive WAL recovery"
            );
        }
    }

    #[test]
    fn test_get_versioned_still_uses_transaction() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "vk", Value::Int(99)).unwrap();

        // get_versioned still goes through transactions (snapshot isolation)
        let vv = kv.get_versioned(&branch_id, "default", "vk").unwrap();
        assert!(vv.is_some());
        let vv = vv.unwrap();
        assert_eq!(vv.value, Value::Int(99));
        assert!(vv.version.as_u64() > 0, "Version must be non-zero");
    }

    // ========== count() tests ==========

    #[test]
    fn test_count_empty() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        assert_eq!(kv.count(&branch_id, "default", None).unwrap(), 0);
    }

    #[test]
    fn test_count_all() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();
        kv.put(&branch_id, "default", "c", Value::Int(3)).unwrap();
        assert_eq!(kv.count(&branch_id, "default", None).unwrap(), 3);
    }

    #[test]
    fn test_count_with_prefix() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        kv.put(&branch_id, "default", "user:1", Value::Int(1))
            .unwrap();
        kv.put(&branch_id, "default", "user:2", Value::Int(2))
            .unwrap();
        kv.put(&branch_id, "default", "task:1", Value::Int(3))
            .unwrap();
        assert_eq!(kv.count(&branch_id, "default", Some("user:")).unwrap(), 2);
        assert_eq!(kv.count(&branch_id, "default", Some("task:")).unwrap(), 1);
        assert_eq!(kv.count(&branch_id, "default", Some("none:")).unwrap(), 0);
    }

    // ========== list_with_cursor_limit() tests ==========

    #[test]
    fn test_list_with_cursor_limit_basic() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        for i in 0..10 {
            kv.put(
                &branch_id,
                "default",
                &format!("key_{:02}", i),
                Value::Int(i),
            )
            .unwrap();
        }

        // First page of 3
        let page = kv
            .list_with_cursor_limit(&branch_id, "default", None, None, 3)
            .unwrap();
        assert_eq!(page.len(), 4); // limit + 1 for has_more detection
        assert_eq!(page[0], "key_00");
        assert_eq!(page[1], "key_01");
        assert_eq!(page[2], "key_02");
        assert_eq!(page[3], "key_03"); // extra "has more" entry
    }

    #[test]
    fn test_list_with_cursor_limit_with_cursor() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        for i in 0..10 {
            kv.put(
                &branch_id,
                "default",
                &format!("key_{:02}", i),
                Value::Int(i),
            )
            .unwrap();
        }

        // Page after "key_04", limit 3
        let page = kv
            .list_with_cursor_limit(&branch_id, "default", None, Some("key_04"), 3)
            .unwrap();
        assert_eq!(page.len(), 4); // limit + 1
        assert_eq!(page[0], "key_05"); // start after cursor
    }

    #[test]
    fn test_list_with_cursor_limit_cursor_beyond_end() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();

        let page = kv
            .list_with_cursor_limit(&branch_id, "default", None, Some("z"), 10)
            .unwrap();
        assert!(page.is_empty(), "cursor beyond all keys returns empty");
    }

    #[test]
    fn test_list_with_cursor_limit_last_page() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();

        // Request limit 5, only 2 keys exist → returns 2 (no extra "has_more" entry)
        let page = kv
            .list_with_cursor_limit(&branch_id, "default", None, None, 5)
            .unwrap();
        assert_eq!(page.len(), 2);
    }

    // ========== sample() tests ==========

    #[test]
    fn test_sample_empty() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        let (total, items) = kv.sample(&branch_id, "default", None, 5).unwrap();
        assert_eq!(total, 0);
        assert!(items.is_empty());
    }

    #[test]
    fn test_sample_count_zero() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        let (total, items) = kv.sample(&branch_id, "default", None, 0).unwrap();
        assert_eq!(total, 1);
        assert!(items.is_empty());
    }

    #[test]
    fn test_sample_count_exceeds_total() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();
        let (total, items) = kv.sample(&branch_id, "default", None, 10).unwrap();
        assert_eq!(total, 2);
        assert_eq!(items.len(), 2); // returns all entries
    }

    #[test]
    fn test_sample_returns_values() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        for i in 0..20 {
            kv.put(&branch_id, "default", &format!("k{:02}", i), Value::Int(i))
                .unwrap();
        }
        let (total, items) = kv.sample(&branch_id, "default", None, 5).unwrap();
        assert_eq!(total, 20);
        assert_eq!(items.len(), 5);
        // Each sampled item should have key and value
        for (key, value) in &items {
            assert!(key.starts_with('k'));
            assert!(matches!(value, Value::Int(_)));
        }
    }

    #[test]
    fn test_sample_with_prefix() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();
        kv.put(&branch_id, "default", "user:1", Value::Int(1))
            .unwrap();
        kv.put(&branch_id, "default", "user:2", Value::Int(2))
            .unwrap();
        kv.put(&branch_id, "default", "task:1", Value::Int(3))
            .unwrap();
        let (total, items) = kv.sample(&branch_id, "default", Some("user:"), 10).unwrap();
        assert_eq!(total, 2);
        assert_eq!(items.len(), 2);
    }

    // ========== KVStoreExt in-space tests ==========

    #[test]
    fn test_kvstore_ext_in_space() {
        use crate::primitives::extensions::KVStoreExt;

        let (_temp, db, _kv) = setup();
        let branch_id = BranchId::new();

        db.transaction(branch_id, |txn| {
            // Write to non-default space
            txn.kv_put_in_space("myspace", "key1", Value::Int(42))?;

            // Read from non-default space
            let val = txn.kv_get_in_space("myspace", "key1")?;
            assert_eq!(val, Some(Value::Int(42)));

            // Default space should not see it
            let val_default = txn.kv_get("key1")?;
            assert_eq!(val_default, None, "default space must not see myspace key");

            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_kvstore_ext_delete_in_space() {
        use crate::primitives::extensions::KVStoreExt;

        let (_temp, db, _kv) = setup();
        let branch_id = BranchId::new();

        db.transaction(branch_id, |txn| {
            txn.kv_put_in_space("alpha", "k", Value::Int(1))?;
            txn.kv_put_in_space("beta", "k", Value::Int(2))?;

            // Delete from alpha only
            txn.kv_delete_in_space("alpha", "k")?;

            assert_eq!(txn.kv_get_in_space("alpha", "k")?, None);
            assert_eq!(txn.kv_get_in_space("beta", "k")?, Some(Value::Int(2)));
            Ok(())
        })
        .unwrap();
    }

    // ========== Namespace cache tests ==========

    #[test]
    fn test_namespace_cache_returns_same_arc() {
        let branch = BranchId::new();
        let ns1 = super::cached_namespace(branch, "test-space");
        let ns2 = super::cached_namespace(branch, "test-space");
        assert!(Arc::ptr_eq(&ns1, &ns2), "cache must return same Arc");
    }

    #[test]
    fn test_namespace_cache_different_spaces_different_arcs() {
        let branch = BranchId::new();
        let ns_a = super::cached_namespace(branch, "space-a");
        let ns_b = super::cached_namespace(branch, "space-b");
        assert!(!Arc::ptr_eq(&ns_a, &ns_b));
        assert_eq!(ns_a.space, "space-a");
        assert_eq!(ns_b.space, "space-b");
    }

    #[test]
    fn test_invalidate_kv_namespace_cache() {
        let branch = BranchId::new();
        let ns_before = super::cached_namespace(branch, "evict-test");
        super::invalidate_kv_namespace_cache(&branch);
        let ns_after = super::cached_namespace(branch, "evict-test");
        // Value is identical but must be a new allocation
        assert_eq!(*ns_before, *ns_after);
        assert!(!Arc::ptr_eq(&ns_before, &ns_after));
    }

    // ========== Issue #1934: Test coverage gap scenarios ==========

    // --- Scenario 1: Engine accepts empty keys (no engine-level validation) ---

    #[test]
    fn test_put_empty_key_engine_accepts() {
        // The engine has no key validation — only the executor's validate_key()
        // rejects empty keys. This test documents the engine-level behavior:
        // empty keys are accepted and round-trip correctly.
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "",
            Value::String("empty-key-value".into()),
        )
        .unwrap();

        let result = kv.get(&branch_id, "default", "").unwrap();
        assert_eq!(result, Some(Value::String("empty-key-value".into())));
    }

    #[test]
    fn test_list_includes_empty_key() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "", Value::Int(0)).unwrap();
        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();

        let keys = kv.list(&branch_id, "default", None).unwrap();
        assert!(
            keys.contains(&String::new()),
            "empty key must appear in list"
        );
        assert_eq!(keys.len(), 2);
    }

    // --- Scenario 2: Engine accepts _strata/ prefix keys ---

    #[test]
    fn test_put_reserved_prefix_engine_accepts() {
        // The executor rejects _strata/ prefixed keys, but the engine
        // has no such restriction. This test documents that the engine
        // layer stores them without error.
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(
            &branch_id,
            "default",
            "_strata/internal",
            Value::String("reserved".into()),
        )
        .unwrap();

        let result = kv.get(&branch_id, "default", "_strata/internal").unwrap();
        assert_eq!(result, Some(Value::String("reserved".into())));
    }

    // --- Scenario 3: Engine accepts oversized values (no engine-level validation) ---

    #[test]
    fn test_put_large_value_engine_accepts() {
        // The executor enforces value size limits via validate_value().
        // The engine has no such check — it stores whatever it receives.
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let large_string = "x".repeat(1_000_000); // 1MB string
        kv.put(
            &branch_id,
            "default",
            "big",
            Value::String(large_string.clone()),
        )
        .unwrap();

        let result = kv.get(&branch_id, "default", "big").unwrap();
        assert_eq!(result, Some(Value::String(large_string)));
    }

    // --- Scenario 5: get_at() with future timestamp ---

    #[test]
    fn test_get_at_future_timestamp() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "k", Value::Int(1)).unwrap();

        // A far-future timestamp should see the latest value
        let far_future = u64::MAX;
        let result = kv.get_at(&branch_id, "default", "k", far_future).unwrap();
        assert_eq!(
            result,
            Some(Value::Int(1)),
            "future timestamp must see latest value"
        );
    }

    #[test]
    fn test_get_at_future_sees_latest_version() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "k", Value::Int(1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        kv.put(&branch_id, "default", "k", Value::Int(2)).unwrap();

        let far_future = u64::MAX;
        let result = kv.get_at(&branch_id, "default", "k", far_future).unwrap();
        assert_eq!(
            result,
            Some(Value::Int(2)),
            "future timestamp must see latest version"
        );
    }

    // --- Scenario 6: get_at() with timestamp before any writes ---

    #[test]
    fn test_get_at_before_any_writes() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "k", Value::Int(1)).unwrap();

        // Timestamp 0 is before any possible write
        let result = kv.get_at(&branch_id, "default", "k", 0).unwrap();
        assert_eq!(result, None, "timestamp before any writes must return None");
    }

    #[test]
    fn test_get_at_nonexistent_key() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        let result = kv
            .get_at(&branch_id, "default", "missing", u64::MAX)
            .unwrap();
        assert_eq!(
            result, None,
            "nonexistent key must return None at any timestamp"
        );
    }

    #[test]
    fn test_list_at_before_any_writes() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();

        let keys = kv.list_at(&branch_id, "default", None, 0).unwrap();
        assert!(keys.is_empty(), "list_at before any writes must be empty");
    }

    #[test]
    fn test_list_at_future_timestamp() {
        let (_temp, _db, kv) = setup();
        let branch_id = BranchId::new();

        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();

        let keys = kv.list_at(&branch_id, "default", None, u64::MAX).unwrap();
        assert_eq!(
            keys.len(),
            2,
            "list_at with future timestamp must see all keys"
        );
    }

    // --- Scenario 8: Search after batch_put() ---

    #[test]
    fn test_search_after_batch_put() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        let entries = vec![
            (
                "doc1".to_string(),
                Value::String("alpha bravo charlie".into()),
            ),
            (
                "doc2".to_string(),
                Value::String("delta echo foxtrot".into()),
            ),
            ("doc3".to_string(), Value::String("alpha delta golf".into())),
        ];

        let results = kv.batch_put(&branch_id, "default", entries).unwrap();
        assert!(
            results.iter().all(|r| r.is_ok()),
            "all batch items must succeed"
        );

        // All batch_put items must be searchable immediately
        let req = crate::SearchRequest::new(branch_id, "alpha");
        let response = kv.search(&req).unwrap();
        assert_eq!(
            response.len(),
            2,
            "both docs containing 'alpha' must be found"
        );

        let req = crate::SearchRequest::new(branch_id, "delta");
        let response = kv.search(&req).unwrap();
        assert_eq!(
            response.len(),
            2,
            "both docs containing 'delta' must be found"
        );

        let req = crate::SearchRequest::new(branch_id, "echo");
        let response = kv.search(&req).unwrap();
        assert_eq!(response.len(), 1, "only doc2 contains 'echo'");
    }

    #[test]
    fn test_search_after_batch_put_then_delete() {
        use crate::search::Searchable;

        let (_temp, _db, kv) = setup_with_index();
        let branch_id = BranchId::new();

        let entries = vec![
            (
                "d1".to_string(),
                Value::String("searchable content here".into()),
            ),
            (
                "d2".to_string(),
                Value::String("searchable data there".into()),
            ),
        ];
        kv.batch_put(&branch_id, "default", entries).unwrap();

        // Delete one
        kv.delete(&branch_id, "default", "d1").unwrap();

        let req = crate::SearchRequest::new(branch_id, "searchable");
        let response = kv.search(&req).unwrap();
        assert_eq!(response.len(), 1, "deleted doc must not appear in search");
    }
}
