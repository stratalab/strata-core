//! Core traits for storage and snapshot abstraction
//!
//! This module defines the Storage and SnapshotView traits that enable
//! swapping implementations without breaking upper layers.

use std::time::Duration;

use crate::contract::VersionedValue;
use crate::error::StrataResult;
use crate::types::{BranchId, Key};
use crate::value::Value;

/// Controls how a write interacts with existing versions of a key.
///
/// - `Append` (default): Standard MVCC — adds a new version, preserving history.
/// - `Replace`: Overwrites the latest version, dropping all old versions.
///   Use for internal data (e.g., graph adjacency lists) where multi-version
///   history wastes memory without providing value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Standard MVCC: push a new version onto the chain (keeps history)
    #[default]
    Append,
    /// Single-version: replace the latest value, drop all old versions
    Replace,
}

/// Storage abstraction for unified backend
///
/// This trait enables replacing the MVP BTreeMap+RwLock implementation
/// with sharded, lock-free, or distributed storage without breaking
/// upper layers (concurrency, primitives, engine).
///
/// ## Thread Safety
///
/// All implementations **must** be `Send + Sync`. Methods may be called
/// concurrently from multiple threads without external synchronization.
///
/// ## Visibility Semantics
///
/// A successful `put()` **happens-before** any subsequent `get()` on the same
/// key within the same thread. Cross-thread visibility is guaranteed by the
/// `Send + Sync` bounds — implementations must use internal synchronization
/// (e.g., `RwLock`, atomics, or lock-free structures) to ensure that a `put()`
/// on thread A is visible to a `get()` on thread B that starts after the `put()`
/// returns.
///
/// ## Snapshot Consistency
///
/// `get_versioned()` and `scan_prefix()` accept a `max_version` parameter.
/// They must return a view consistent with that version — i.e., only values
/// written at or before `max_version` are visible. This is the foundation
/// for snapshot isolation in the concurrency layer.
///
/// ## Error Conditions
///
/// All fallible methods return `StrataResult`. Implementations should return:
/// - `StrataError::storage(...)` for I/O failures (disk, network)
/// - `StrataError::internal(...)` for invariant violations (bugs)
///
/// Callers should **not** assume that errors are transient — a `StorageError`
/// may indicate permanent media failure.
///
/// ## Implementors
///
/// Implementors **should** validate size limits in `put()` and `put_with_version()`
/// using [`crate::limits::Limits`]. This enforcement is planned for the storage
/// epic (#1306) but is not yet required by this trait.
pub trait Storage: Send + Sync {
    /// Get current value for key (latest version)
    ///
    /// Returns None if key doesn't exist or is expired.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>>;

    /// Get value at or before specified version (for snapshot isolation)
    ///
    /// This enables creating snapshots without cloning the entire store.
    /// Returns the latest version <= max_version.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn get_versioned(&self, key: &Key, max_version: u64) -> StrataResult<Option<VersionedValue>>;

    /// Get version history for a key
    ///
    /// Returns historical versions of the value, newest first.
    /// Used by all primitives that support history queries (KV, JSON, State, etc.).
    ///
    /// # Arguments
    /// * `key` - The key to get history for
    /// * `limit` - Maximum versions to return (None = all)
    /// * `before_version` - Only return versions older than this (for pagination)
    ///
    /// # Returns
    /// Vector of VersionedValue in descending version order (newest first).
    /// Empty if key doesn't exist or has no history.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn get_history(
        &self,
        key: &Key,
        limit: Option<usize>,
        before_version: Option<u64>,
    ) -> StrataResult<Vec<VersionedValue>>;

    /// Put key-value pair with optional TTL
    ///
    /// Returns the version assigned to this write.
    /// Version is monotonically increasing and assigned by the storage layer.
    ///
    /// After a successful return, the written value is immediately visible
    /// to `get()` calls on the same or any other thread.
    ///
    /// # Errors
    ///
    /// - `StorageError` — I/O or backend failure
    /// - Future: `ConstraintViolation` when limit enforcement is added (#1306)
    fn put(&self, key: Key, value: Value, ttl: Option<Duration>) -> StrataResult<u64>;

    /// Delete key
    ///
    /// Returns the deleted value if it existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn delete(&self, key: &Key) -> StrataResult<Option<VersionedValue>>;

    /// Scan keys with given prefix at or before max_version
    ///
    /// Results are sorted by key order (namespace → type_tag → user_key).
    /// Used for range queries and namespace scans.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn scan_prefix(
        &self,
        prefix: &Key,
        max_version: u64,
    ) -> StrataResult<Vec<(Key, VersionedValue)>>;

    /// Scan all keys for a given branch_id at or before max_version
    ///
    /// Critical for replay: fetch all writes for a specific branch.
    /// Results are sorted by key order.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn scan_by_branch(
        &self,
        branch_id: BranchId,
        max_version: u64,
    ) -> StrataResult<Vec<(Key, VersionedValue)>>;

    /// Get current global version
    ///
    /// Returns the highest version assigned so far.
    /// Used for creating snapshots at current version.
    fn current_version(&self) -> u64;

    /// Put a value with a specific version
    ///
    /// Used by transaction commit to apply writes with the commit version.
    /// Unlike `put()`, this does NOT allocate a new version - it uses the
    /// provided version directly.
    ///
    /// Per spec Section 6.1: All keys in a transaction get the same commit version.
    ///
    /// # Arguments
    /// * `key` - The key to write
    /// * `value` - The value to write
    /// * `version` - The exact version to assign to this write
    /// * `ttl` - Optional time-to-live for the value
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn put_with_version(
        &self,
        key: Key,
        value: Value,
        version: u64,
        ttl: Option<Duration>,
    ) -> StrataResult<()>;

    /// Put a value with a specific version and write mode.
    ///
    /// Same as `put_with_version`, but respects `WriteMode`:
    /// - `Append`: standard MVCC (adds version to chain)
    /// - `Replace`: overwrites all previous versions (single-version)
    ///
    /// Default implementation ignores the mode and delegates to `put_with_version`.
    /// Backends that support single-version writes (e.g., ShardedStore) should
    /// override for memory efficiency.
    fn put_with_version_mode(
        &self,
        key: Key,
        value: Value,
        version: u64,
        ttl: Option<Duration>,
        mode: WriteMode,
    ) -> StrataResult<()> {
        let _ = mode; // default ignores mode
        self.put_with_version(key, value, version, ttl)
    }

    /// Delete a key with a specific version (creates tombstone)
    ///
    /// Used by transaction commit to apply deletes with the commit version.
    /// Per spec Section 6.5: Deleted keys get versioned tombstones.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    /// * `version` - The version for this delete operation
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn delete_with_version(&self, key: &Key, version: u64) -> StrataResult<()>;

    /// Get only the version number for a key (no Value clone)
    ///
    /// Returns the raw version as `u64` without constructing a `VersionedValue`.
    /// Used by validation paths that only need version comparison.
    ///
    /// Default implementation delegates to `get()` and extracts the version.
    /// Implementations can override for efficiency (e.g., skipping Value clone).
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn get_version_only(&self, key: &Key) -> StrataResult<Option<u64>> {
        Ok(self.get(key)?.map(|vv| vv.version.as_u64()))
    }
}

/// Snapshot view abstraction for snapshot isolation
///
/// Provides a **frozen, read-only** view of storage at a specific version.
/// All reads through a snapshot return data as it existed at `version()`,
/// regardless of concurrent writes to the underlying storage.
///
/// ## Thread Safety
///
/// All implementations **must** be `Send + Sync`. A snapshot may be created
/// on one thread and read from another.
///
/// ## Consistency Guarantees
///
/// - `get()` returns the latest value written at or before `version()`.
/// - `scan_prefix()` returns a consistent set of key-value pairs — all
///   visible at `version()`, none written after.
/// - The snapshot is **immutable**: repeated reads of the same key always
///   return the same result (or `None` if the key didn't exist at that version).
///
/// ## Lifetime
///
/// Snapshots may hold references to shared state (e.g., an `Arc` to the
/// underlying storage). Dropping a snapshot releases these references.
/// Long-lived snapshots may prevent garbage collection of old versions
/// in compacting storage implementations.
pub trait SnapshotView: Send + Sync {
    /// Get value from snapshot
    ///
    /// Returns value as it existed at snapshot version.
    /// Returns None if key didn't exist at that version.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>>;

    /// Scan keys with prefix from snapshot
    ///
    /// Returns all matching keys as they existed at snapshot version.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn scan_prefix(&self, prefix: &Key) -> StrataResult<Vec<(Key, VersionedValue)>>;

    /// Get just the value and raw version for a key.
    ///
    /// Returns `(Value, u64)` without constructing a full `VersionedValue`.
    /// Used by the transaction read path where only the value and version
    /// number are needed (for conflict detection).
    ///
    /// Default implementation delegates to `get()` and destructures.
    /// Implementations can override for efficiency (e.g., skipping
    /// `Version::Txn` enum construction).
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn get_value_and_version(&self, key: &Key) -> StrataResult<Option<(Value, u64)>> {
        Ok(self.get(key)?.map(|vv| (vv.value, vv.version.as_u64())))
    }

    /// Get snapshot version
    ///
    /// Returns the version this snapshot was created at.
    fn version(&self) -> u64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{Timestamp, Version, Versioned};
    use crate::error::StrataError;
    use crate::types::Namespace;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    };

    // ====================================================================
    // Minimal mock implementations for behavioral testing
    // ====================================================================

    /// A minimal in-memory Storage implementation for testing the trait contract.
    struct MockStorage {
        data: RwLock<BTreeMap<Key, Vec<VersionedValue>>>,
        version: AtomicU64,
    }

    impl MockStorage {
        fn new() -> Self {
            MockStorage {
                data: RwLock::new(BTreeMap::new()),
                version: AtomicU64::new(0),
            }
        }
    }

    impl Storage for MockStorage {
        fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
            let data = self.data.read().unwrap();
            Ok(data.get(key).and_then(|versions| versions.last().cloned()))
        }

        fn get_versioned(
            &self,
            key: &Key,
            max_version: u64,
        ) -> StrataResult<Option<VersionedValue>> {
            let data = self.data.read().unwrap();
            Ok(data.get(key).and_then(|versions| {
                versions
                    .iter()
                    .rev()
                    .find(|v| v.version().as_u64() <= max_version)
                    .cloned()
            }))
        }

        fn get_history(
            &self,
            key: &Key,
            limit: Option<usize>,
            before_version: Option<u64>,
        ) -> StrataResult<Vec<VersionedValue>> {
            let data = self.data.read().unwrap();
            let Some(versions) = data.get(key) else {
                return Ok(vec![]);
            };
            let mut result: Vec<_> = versions
                .iter()
                .rev()
                .filter(|v| before_version.map_or(true, |bv| v.version().as_u64() < bv))
                .cloned()
                .collect();
            if let Some(limit) = limit {
                result.truncate(limit);
            }
            Ok(result)
        }

        fn put(&self, key: Key, value: Value, _ttl: Option<Duration>) -> StrataResult<u64> {
            let ver = self.version.fetch_add(1, Ordering::SeqCst) + 1;
            let versioned = Versioned::with_timestamp(value, Version::txn(ver), Timestamp::now());
            let mut data = self.data.write().unwrap();
            data.entry(key).or_default().push(versioned);
            Ok(ver)
        }

        fn delete(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
            let mut data = self.data.write().unwrap();
            Ok(data.remove(key).and_then(|v| v.last().cloned()))
        }

        fn scan_prefix(
            &self,
            prefix: &Key,
            max_version: u64,
        ) -> StrataResult<Vec<(Key, VersionedValue)>> {
            let data = self.data.read().unwrap();
            let mut result = vec![];
            for (k, versions) in data.iter() {
                if k.starts_with(prefix) {
                    if let Some(v) = versions
                        .iter()
                        .rev()
                        .find(|v| v.version().as_u64() <= max_version)
                    {
                        result.push((k.clone(), v.clone()));
                    }
                }
            }
            Ok(result)
        }

        fn scan_by_branch(
            &self,
            branch_id: BranchId,
            max_version: u64,
        ) -> StrataResult<Vec<(Key, VersionedValue)>> {
            let data = self.data.read().unwrap();
            let mut result = vec![];
            for (k, versions) in data.iter() {
                if k.namespace.branch_id == branch_id {
                    if let Some(v) = versions
                        .iter()
                        .rev()
                        .find(|v| v.version().as_u64() <= max_version)
                    {
                        result.push((k.clone(), v.clone()));
                    }
                }
            }
            Ok(result)
        }

        fn current_version(&self) -> u64 {
            self.version.load(Ordering::SeqCst)
        }

        fn put_with_version(
            &self,
            key: Key,
            value: Value,
            version: u64,
            _ttl: Option<Duration>,
        ) -> StrataResult<()> {
            let versioned =
                Versioned::with_timestamp(value, Version::txn(version), Timestamp::now());
            let mut data = self.data.write().unwrap();
            data.entry(key).or_default().push(versioned);
            Ok(())
        }

        fn delete_with_version(&self, key: &Key, _version: u64) -> StrataResult<()> {
            self.delete(key)?;
            Ok(())
        }
    }

    /// A minimal SnapshotView for testing.
    struct MockSnapshot {
        data: BTreeMap<Key, VersionedValue>,
        snap_version: u64,
    }

    impl MockSnapshot {
        fn from_storage(storage: &MockStorage, version: u64) -> Self {
            let data = storage.data.read().unwrap();
            let mut snap = BTreeMap::new();
            for (k, versions) in data.iter() {
                if let Some(v) = versions
                    .iter()
                    .rev()
                    .find(|v| v.version().as_u64() <= version)
                {
                    snap.insert(k.clone(), v.clone());
                }
            }
            MockSnapshot {
                data: snap,
                snap_version: version,
            }
        }
    }

    impl SnapshotView for MockSnapshot {
        fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
            Ok(self.data.get(key).cloned())
        }

        fn scan_prefix(&self, prefix: &Key) -> StrataResult<Vec<(Key, VersionedValue)>> {
            Ok(self
                .data
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        }

        fn version(&self) -> u64 {
            self.snap_version
        }
    }

    fn test_ns() -> Arc<Namespace> {
        Arc::new(Namespace::new(
            "test".into(),
            "app".into(),
            "agent".into(),
            BranchId::new(),
            "default".into(),
        ))
    }

    fn test_key(ns: &Arc<Namespace>, name: &str) -> Key {
        Key::new_kv(ns.clone(), name)
    }

    // ====================================================================
    // Compile-time contract tests (object safety, Send+Sync)
    // ====================================================================

    #[test]
    fn storage_is_object_safe_and_send_sync() {
        fn accepts_storage(_: &dyn Storage) {}
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        let _ = accepts_storage as fn(&dyn Storage);
        assert_send::<Box<dyn Storage>>();
        assert_sync::<Box<dyn Storage>>();
    }

    #[test]
    fn snapshot_view_is_object_safe_and_send_sync() {
        fn accepts_snapshot(_: &dyn SnapshotView) {}
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        let _ = accepts_snapshot as fn(&dyn SnapshotView);
        assert_send::<Box<dyn SnapshotView>>();
        assert_sync::<Box<dyn SnapshotView>>();
    }

    // ====================================================================
    // Storage behavioral tests
    // ====================================================================

    #[test]
    fn storage_get_nonexistent_returns_none() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "missing");
        assert!(store.get(&key).unwrap().is_none());
    }

    #[test]
    fn storage_put_then_get_returns_value() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "hello");
        let ver = store.put(key.clone(), Value::Int(42), None).unwrap();
        assert!(ver > 0);

        let result = store.get(&key).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(42));
    }

    #[test]
    fn storage_put_increments_version_monotonically() {
        let store = MockStorage::new();
        let ns = test_ns();
        let v1 = store.put(test_key(&ns, "a"), Value::Int(1), None).unwrap();
        let v2 = store.put(test_key(&ns, "b"), Value::Int(2), None).unwrap();
        let v3 = store.put(test_key(&ns, "c"), Value::Int(3), None).unwrap();
        assert!(v1 < v2);
        assert!(v2 < v3);
    }

    #[test]
    fn storage_delete_removes_key() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "deleteme");
        store.put(key.clone(), Value::Int(1), None).unwrap();

        let deleted = store.delete(&key).unwrap();
        assert!(deleted.is_some());
        assert_eq!(deleted.unwrap().value, Value::Int(1));

        assert!(store.get(&key).unwrap().is_none());
    }

    #[test]
    fn storage_delete_nonexistent_returns_none() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "never_existed");
        assert!(store.delete(&key).unwrap().is_none());
    }

    #[test]
    fn storage_get_versioned_respects_max_version() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "versioned");

        let v1 = store.put(key.clone(), Value::Int(1), None).unwrap();
        let _v2 = store.put(key.clone(), Value::Int(2), None).unwrap();

        // Reading at v1 should see the first write
        let result = store.get_versioned(&key, v1).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(1));
    }

    #[test]
    fn storage_get_versioned_at_zero_returns_none() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "v");
        store.put(key.clone(), Value::Int(1), None).unwrap();

        // Version 0 is before any write
        assert!(store.get_versioned(&key, 0).unwrap().is_none());
    }

    #[test]
    fn storage_get_history_returns_newest_first() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "history");

        store.put(key.clone(), Value::Int(1), None).unwrap();
        store.put(key.clone(), Value::Int(2), None).unwrap();
        store.put(key.clone(), Value::Int(3), None).unwrap();

        let history = store.get_history(&key, None, None).unwrap();
        assert_eq!(history.len(), 3);
        // Newest first
        assert_eq!(history[0].value, Value::Int(3));
        assert_eq!(history[1].value, Value::Int(2));
        assert_eq!(history[2].value, Value::Int(1));
    }

    #[test]
    fn storage_get_history_with_limit() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "limited");

        for i in 0..10 {
            store.put(key.clone(), Value::Int(i), None).unwrap();
        }

        let history = store.get_history(&key, Some(3), None).unwrap();
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn storage_get_history_nonexistent_returns_empty() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "nope");
        assert!(store.get_history(&key, None, None).unwrap().is_empty());
    }

    #[test]
    fn storage_get_history_before_version_paginates() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "paginate");

        let v1 = store.put(key.clone(), Value::Int(1), None).unwrap();
        let v2 = store.put(key.clone(), Value::Int(2), None).unwrap();
        let _v3 = store.put(key.clone(), Value::Int(3), None).unwrap();

        // Get history before v3 (should return v2 and v1)
        let history = store.get_history(&key, None, Some(v2 + 1)).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].value, Value::Int(2));

        // Get history before v2 (should return only v1)
        let history = store.get_history(&key, None, Some(v1 + 1)).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].value, Value::Int(1));
    }

    #[test]
    fn storage_current_version_starts_at_zero() {
        let store = MockStorage::new();
        assert_eq!(store.current_version(), 0);
    }

    #[test]
    fn storage_current_version_advances_with_puts() {
        let store = MockStorage::new();
        let ns = test_ns();
        store.put(test_key(&ns, "a"), Value::Int(1), None).unwrap();
        assert!(store.current_version() > 0);
    }

    #[test]
    fn storage_put_with_version_uses_explicit_version() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "explicit");
        store
            .put_with_version(key.clone(), Value::Int(99), 42, None)
            .unwrap();

        let result = store.get(&key).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(99));
        assert_eq!(result.version().as_u64(), 42);
    }

    #[test]
    fn storage_scan_prefix_returns_matching_keys() {
        let store = MockStorage::new();
        let ns = test_ns();
        let prefix = Key::new_kv(ns.clone(), "user/");
        store
            .put(Key::new_kv(ns.clone(), "user/alice"), Value::Int(1), None)
            .unwrap();
        store
            .put(Key::new_kv(ns.clone(), "user/bob"), Value::Int(2), None)
            .unwrap();
        store
            .put(Key::new_kv(ns.clone(), "config/x"), Value::Int(3), None)
            .unwrap();

        let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn storage_scan_by_branch_isolates_branches() {
        let store = MockStorage::new();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let ns1 = Arc::new(Namespace::new(
            "t".into(),
            "a".into(),
            "g".into(),
            branch1,
            "default".into(),
        ));
        let ns2 = Arc::new(Namespace::new(
            "t".into(),
            "a".into(),
            "g".into(),
            branch2,
            "default".into(),
        ));

        store
            .put(Key::new_kv(ns1.clone(), "k1"), Value::Int(1), None)
            .unwrap();
        store
            .put(Key::new_kv(ns2.clone(), "k2"), Value::Int(2), None)
            .unwrap();

        let results = store.scan_by_branch(branch1, u64::MAX).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.value, Value::Int(1));
    }

    // ====================================================================
    // SnapshotView behavioral tests
    // ====================================================================

    #[test]
    fn snapshot_captures_point_in_time_state() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "snap");

        let v1 = store.put(key.clone(), Value::Int(1), None).unwrap();
        let snap = MockSnapshot::from_storage(&store, v1);

        // Write after snapshot
        store.put(key.clone(), Value::Int(2), None).unwrap();

        // Snapshot should still see old value
        let result = snap.get(&key).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(1));

        // Live storage should see new value
        let result = store.get(&key).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(2));
    }

    #[test]
    fn snapshot_version_returns_creation_version() {
        let store = MockStorage::new();
        let ns = test_ns();
        let v = store.put(test_key(&ns, "x"), Value::Int(1), None).unwrap();
        let snap = MockSnapshot::from_storage(&store, v);
        assert_eq!(snap.version(), v);
    }

    #[test]
    fn snapshot_get_nonexistent_returns_none() {
        let store = MockStorage::new();
        let snap = MockSnapshot::from_storage(&store, 0);
        let ns = test_ns();
        assert!(snap.get(&test_key(&ns, "missing")).unwrap().is_none());
    }

    #[test]
    fn snapshot_does_not_see_writes_after_version() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "invisible");

        let snap = MockSnapshot::from_storage(&store, 0);
        store.put(key.clone(), Value::Int(1), None).unwrap();

        assert!(snap.get(&key).unwrap().is_none());
    }

    // ====================================================================
    // Error propagation through trait
    // ====================================================================

    /// A storage that always returns errors.
    struct FailingStorage;

    impl Storage for FailingStorage {
        fn get(&self, _: &Key) -> StrataResult<Option<VersionedValue>> {
            Err(StrataError::storage("disk read failed"))
        }
        fn get_versioned(&self, _: &Key, _: u64) -> StrataResult<Option<VersionedValue>> {
            Err(StrataError::storage("disk read failed"))
        }
        fn get_history(
            &self,
            _: &Key,
            _: Option<usize>,
            _: Option<u64>,
        ) -> StrataResult<Vec<VersionedValue>> {
            Err(StrataError::storage("disk read failed"))
        }
        fn put(&self, _: Key, _: Value, _: Option<Duration>) -> StrataResult<u64> {
            Err(StrataError::storage("disk write failed"))
        }
        fn delete(&self, _: &Key) -> StrataResult<Option<VersionedValue>> {
            Err(StrataError::storage("disk write failed"))
        }
        fn scan_prefix(&self, _: &Key, _: u64) -> StrataResult<Vec<(Key, VersionedValue)>> {
            Err(StrataError::storage("disk read failed"))
        }
        fn scan_by_branch(&self, _: BranchId, _: u64) -> StrataResult<Vec<(Key, VersionedValue)>> {
            Err(StrataError::storage("disk read failed"))
        }
        fn current_version(&self) -> u64 {
            0
        }
        fn put_with_version(
            &self,
            _: Key,
            _: Value,
            _: u64,
            _: Option<Duration>,
        ) -> StrataResult<()> {
            Err(StrataError::storage("disk write failed"))
        }
        fn delete_with_version(&self, _: &Key, _: u64) -> StrataResult<()> {
            Err(StrataError::storage("disk write failed"))
        }
    }

    #[test]
    fn storage_errors_propagate_through_trait_object() {
        let store: Box<dyn Storage> = Box::new(FailingStorage);
        let ns = test_ns();
        let key = test_key(&ns, "k");

        assert!(store.get(&key).is_err());
        assert!(store.put(key.clone(), Value::Null, None).is_err());
        assert!(store.delete(&key).is_err());
        assert!(store.get_versioned(&key, 0).is_err());
        assert!(store.get_history(&key, None, None).is_err());
        assert!(store.scan_prefix(&key, 0).is_err());
        assert!(store.scan_by_branch(BranchId::new(), 0).is_err());
        assert!(store
            .put_with_version(key.clone(), Value::Null, 1, None)
            .is_err());
        assert!(store.delete_with_version(&key, 1).is_err());
    }

    #[test]
    fn storage_error_types_are_correct() {
        let store = FailingStorage;
        let ns = test_ns();
        let key = test_key(&ns, "k");

        let err = store.get(&key).unwrap_err();
        assert!(err.is_storage_error());

        let err = store.put(key, Value::Null, None).unwrap_err();
        assert!(err.is_storage_error());
    }
}
