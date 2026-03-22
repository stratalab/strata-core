//! Core traits for storage abstraction
//!
//! This module defines the Storage trait that enables swapping
//! implementations without breaking upper layers.

use std::time::Duration;

use crate::contract::VersionedValue;
use crate::error::StrataResult;
use crate::types::Key;
use crate::value::Value;

/// Controls how a write interacts with existing versions of a key.
///
/// - `Append` (default): Standard MVCC — adds a new version, preserving history.
/// - `KeepLast(n)`: Keep at most `n` versions, pruning oldest after write.
///   `KeepLast(1)` replaces the old `Replace` behavior (single-version).
///   Use for internal data (e.g., graph adjacency lists) where unbounded
///   version history wastes memory without providing value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Standard MVCC: push a new version onto the chain (keeps history)
    #[default]
    Append,
    /// Keep at most N versions, pruning oldest after write.
    /// `KeepLast(1)` replaces the old `Replace` behavior.
    /// `KeepLast(n)` for n > 1 keeps bounded history.
    KeepLast(usize),
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
/// A successful `put_with_version_mode()` **happens-before** any subsequent
/// `get_versioned()` on the same key within the same thread. Cross-thread
/// visibility is guaranteed by the `Send + Sync` bounds — implementations
/// must use internal synchronization (e.g., `RwLock`, atomics, or lock-free
/// structures) to ensure that a write on thread A is visible to a read on
/// thread B that starts after the write returns.
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
/// Implementors **should** validate size limits in `put_with_version_mode()`
/// using [`crate::limits::Limits`]. This enforcement is planned for the storage
/// epic (#1306) but is not yet required by this trait.
pub trait Storage: Send + Sync {
    /// Get value at or before specified version (for snapshot isolation)
    ///
    /// This enables creating snapshots without cloning the entire store.
    /// Returns the latest version <= max_version. Pass `u64::MAX` to get the
    /// latest version unconditionally.
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

    /// Get current global version
    ///
    /// Returns the highest version assigned so far.
    /// Used for creating snapshots at current version.
    fn current_version(&self) -> u64;

    /// Put a value with a specific version and write mode.
    ///
    /// Used by transaction commit to apply writes with the commit version.
    /// Does NOT allocate a new version — uses the provided version directly.
    ///
    /// Per spec Section 6.1: All keys in a transaction get the same commit version.
    ///
    /// Write modes:
    /// - `Append`: standard MVCC (adds version to chain, preserves history)
    /// - `KeepLast(n)`: keep at most n versions, pruning oldest after write
    ///
    /// # Arguments
    /// * `key` - The key to write
    /// * `value` - The value to write
    /// * `version` - The exact version to assign to this write
    /// * `ttl` - Optional time-to-live for the value
    /// * `mode` - How this write interacts with existing versions
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn put_with_version_mode(
        &self,
        key: Key,
        value: Value,
        version: u64,
        ttl: Option<Duration>,
        mode: WriteMode,
    ) -> StrataResult<()>;

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

    /// Apply a batch of writes in a single operation.
    ///
    /// Default implementation iterates and calls `put_with_version_mode()` per entry.
    /// Storage implementations can override to batch operations (e.g., acquire
    /// branch guards once per branch instead of per entry).
    ///
    /// All writes share the same `version` (same transaction commit version).
    fn apply_batch(&self, writes: Vec<(Key, Value, WriteMode)>, version: u64) -> StrataResult<()> {
        for (key, value, mode) in writes {
            self.put_with_version_mode(key, value, version, None, mode)?;
        }
        Ok(())
    }

    /// Apply a batch of deletes in a single operation.
    ///
    /// Default implementation iterates and calls `delete_with_version()` per entry.
    fn delete_batch(&self, deletes: Vec<Key>, version: u64) -> StrataResult<()> {
        for key in &deletes {
            self.delete_with_version(key, version)?;
        }
        Ok(())
    }

    /// Apply puts and deletes atomically: all entries are installed before
    /// the global version is advanced.
    ///
    /// Default implementation delegates to `apply_batch` then `delete_batch`.
    /// `SegmentedStore` overrides this to defer the version bump until all
    /// entries are in the memtable, preventing partial-state visibility (#1706).
    fn apply_writes_atomic(
        &self,
        writes: Vec<(Key, Value, WriteMode)>,
        deletes: Vec<Key>,
        version: u64,
    ) -> StrataResult<()> {
        self.apply_batch(writes, version)?;
        if !deletes.is_empty() {
            self.delete_batch(deletes, version)?;
        }
        Ok(())
    }

    /// Get only the version number for a key (no Value clone)
    ///
    /// Returns the raw version as `u64` without constructing a `VersionedValue`.
    /// Used by validation paths that only need version comparison.
    ///
    /// Default implementation delegates to `get_versioned(key, u64::MAX)` and
    /// extracts the version. Implementations can override for efficiency
    /// (e.g., skipping Value clone).
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    fn get_version_only(&self, key: &Key) -> StrataResult<Option<u64>> {
        Ok(self
            .get_versioned(key, u64::MAX)?
            .map(|vv| vv.version.as_u64()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{Timestamp, Version, Versioned};
    use crate::error::StrataError;
    use crate::types::{BranchId, Namespace};
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

        fn current_version(&self) -> u64 {
            self.version.load(Ordering::SeqCst)
        }

        fn put_with_version_mode(
            &self,
            key: Key,
            value: Value,
            version: u64,
            _ttl: Option<Duration>,
            _mode: WriteMode,
        ) -> StrataResult<()> {
            let versioned =
                Versioned::with_timestamp(value, Version::txn(version), Timestamp::now());
            let mut data = self.data.write().unwrap();
            data.entry(key).or_default().push(versioned);
            self.version.fetch_max(version, Ordering::SeqCst);
            Ok(())
        }

        fn delete_with_version(&self, key: &Key, _version: u64) -> StrataResult<()> {
            let mut data = self.data.write().unwrap();
            data.remove(key);
            Ok(())
        }
    }

    fn test_ns() -> Arc<Namespace> {
        Arc::new(Namespace::new(BranchId::new(), "default".into()))
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

    // ====================================================================
    // Storage behavioral tests
    // ====================================================================

    /// Helper: seed a value via put_with_version_mode (the only write method)
    fn seed(store: &MockStorage, key: Key, value: Value, version: u64) {
        store
            .put_with_version_mode(key, value, version, None, WriteMode::Append)
            .unwrap();
    }

    #[test]
    fn storage_get_versioned_nonexistent_returns_none() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "missing");
        assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
    }

    #[test]
    fn storage_put_then_get_returns_value() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "hello");
        seed(&store, key.clone(), Value::Int(42), 1);

        let result = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(42));
    }

    #[test]
    fn storage_get_versioned_respects_max_version() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "versioned");

        seed(&store, key.clone(), Value::Int(1), 1);
        seed(&store, key.clone(), Value::Int(2), 2);

        // Reading at v1 should see the first write
        let result = store.get_versioned(&key, 1).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(1));

        // Reading at u64::MAX should see the latest write
        let result = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(2));
    }

    #[test]
    fn storage_get_versioned_at_zero_returns_none() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "v");
        seed(&store, key.clone(), Value::Int(1), 1);

        // Version 0 is before any write
        assert!(store.get_versioned(&key, 0).unwrap().is_none());
    }

    #[test]
    fn storage_get_history_returns_newest_first() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "history");

        seed(&store, key.clone(), Value::Int(1), 1);
        seed(&store, key.clone(), Value::Int(2), 2);
        seed(&store, key.clone(), Value::Int(3), 3);

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

        for i in 1..=10 {
            seed(&store, key.clone(), Value::Int(i), i as u64);
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

        seed(&store, key.clone(), Value::Int(1), 1);
        seed(&store, key.clone(), Value::Int(2), 2);
        seed(&store, key.clone(), Value::Int(3), 3);

        // Get history before v3 (should return v2 and v1)
        let history = store.get_history(&key, None, Some(3)).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].value, Value::Int(2));

        // Get history before v2 (should return only v1)
        let history = store.get_history(&key, None, Some(2)).unwrap();
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
        seed(&store, test_key(&ns, "a"), Value::Int(1), 5);
        assert!(store.current_version() >= 5);
    }

    #[test]
    fn storage_put_with_version_mode_uses_explicit_version() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "explicit");
        seed(&store, key.clone(), Value::Int(99), 42);

        let result = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(99));
        assert_eq!(result.version().as_u64(), 42);
    }

    #[test]
    fn storage_scan_prefix_returns_matching_keys() {
        let store = MockStorage::new();
        let ns = test_ns();
        let prefix = Key::new_kv(ns.clone(), "user/");
        seed(
            &store,
            Key::new_kv(ns.clone(), "user/alice"),
            Value::Int(1),
            1,
        );
        seed(
            &store,
            Key::new_kv(ns.clone(), "user/bob"),
            Value::Int(2),
            2,
        );
        seed(
            &store,
            Key::new_kv(ns.clone(), "config/x"),
            Value::Int(3),
            3,
        );

        let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn storage_delete_with_version_removes_key() {
        let store = MockStorage::new();
        let ns = test_ns();
        let key = test_key(&ns, "deleteme");
        seed(&store, key.clone(), Value::Int(1), 1);

        store.delete_with_version(&key, 2).unwrap();
        assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
    }

    // ====================================================================
    // Error propagation through trait
    // ====================================================================

    /// A storage that always returns errors.
    struct FailingStorage;

    impl Storage for FailingStorage {
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
        fn scan_prefix(&self, _: &Key, _: u64) -> StrataResult<Vec<(Key, VersionedValue)>> {
            Err(StrataError::storage("disk read failed"))
        }
        fn current_version(&self) -> u64 {
            0
        }
        fn put_with_version_mode(
            &self,
            _: Key,
            _: Value,
            _: u64,
            _: Option<Duration>,
            _: WriteMode,
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

        assert!(store.get_versioned(&key, u64::MAX).is_err());
        assert!(store.get_history(&key, None, None).is_err());
        assert!(store.scan_prefix(&key, 0).is_err());
        assert!(store
            .put_with_version_mode(key.clone(), Value::Null, 1, None, WriteMode::Append)
            .is_err());
        assert!(store.delete_with_version(&key, 1).is_err());
    }

    #[test]
    fn storage_error_types_are_correct() {
        let store = FailingStorage;
        let ns = test_ns();
        let key = test_key(&ns, "k");

        let err = store.get_versioned(&key, u64::MAX).unwrap_err();
        assert!(err.is_storage_error());

        let err = store
            .put_with_version_mode(key, Value::Null, 1, None, WriteMode::Append)
            .unwrap_err();
        assert!(err.is_storage_error());
    }
}
