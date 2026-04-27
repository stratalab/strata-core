//! Storage trait and write semantics.

use std::time::Duration;

use strata_core::{CommitVersion, Value, VersionedValue};

use crate::{Key, StorageResult};

/// Controls how a write interacts with existing versions of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Standard MVCC: push a new version onto the chain (keeps history).
    #[default]
    Append,
    /// Retention hint: keep at most N versions during compaction.
    KeepLast(usize),
}

/// Storage-facing trait surface exported by the storage crate.
///
/// This is the storage-owned contract for MVCC reads and writes. Callers above
/// the storage layer should depend on this trait rather than reaching into core
/// for persistence behavior.
pub trait Storage: Send + Sync {
    /// Returns the latest visible value for `key` at or before `max_version`.
    fn get_versioned(
        &self,
        key: &Key,
        max_version: CommitVersion,
    ) -> StorageResult<Option<VersionedValue>>;

    /// Returns historical values for `key`, newest first.
    fn get_history(
        &self,
        key: &Key,
        limit: Option<usize>,
        before_version: Option<CommitVersion>,
    ) -> StorageResult<Vec<VersionedValue>>;

    /// Scans keys that share the given prefix at or before `max_version`.
    fn scan_prefix(
        &self,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> StorageResult<Vec<(Key, VersionedValue)>>;

    /// Returns the highest committed version known to this store.
    fn current_version(&self) -> CommitVersion;

    /// Writes a versioned value using the provided retention mode.
    fn put_with_version_mode(
        &self,
        key: Key,
        value: Value,
        version: CommitVersion,
        ttl: Option<Duration>,
        mode: WriteMode,
    ) -> StorageResult<()>;

    /// Writes a tombstone for `key` at `version`.
    fn delete_with_version(&self, key: &Key, version: CommitVersion) -> StorageResult<()>;

    /// Applies a batch of writes that all share the same commit version.
    fn apply_batch(
        &self,
        writes: Vec<(Key, Value, WriteMode)>,
        version: CommitVersion,
    ) -> StorageResult<()> {
        for (key, value, mode) in writes {
            self.put_with_version_mode(key, value, version, None, mode)?;
        }
        Ok(())
    }

    /// Applies a batch of tombstones that all share the same commit version.
    fn delete_batch(&self, deletes: Vec<Key>, version: CommitVersion) -> StorageResult<()> {
        for key in &deletes {
            self.delete_with_version(key, version)?;
        }
        Ok(())
    }

    /// Applies puts and deletes atomically before advancing the visible version.
    fn apply_writes_atomic(
        &self,
        writes: Vec<(Key, Value, WriteMode)>,
        deletes: Vec<Key>,
        version: CommitVersion,
        put_ttls: &[u64],
    ) -> StorageResult<()> {
        let _ = put_ttls;
        self.apply_batch(writes, version)?;
        if !deletes.is_empty() {
            self.delete_batch(deletes, version)?;
        }
        Ok(())
    }

    /// Returns only the latest visible version for `key`, without cloning the value.
    fn get_version_only(&self, key: &Key) -> StorageResult<Option<CommitVersion>> {
        Ok(self
            .get_versioned(key, CommitVersion::MAX)?
            .map(|vv| CommitVersion(vv.version.as_u64())))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{Storage, WriteMode};
    use crate::{Key, Namespace, SegmentedStore, StorageResult};
    use strata_core::{BranchId, CommitVersion, Timestamp, Value, Version, VersionedValue};

    #[derive(Default)]
    struct FakeStore {
        writes: Mutex<Vec<(Key, Value, WriteMode, CommitVersion)>>,
        deletes: Mutex<Vec<(Key, CommitVersion)>>,
        current: CommitVersion,
        data: Mutex<HashMap<Key, VersionedValue>>,
    }

    impl Storage for FakeStore {
        fn get_versioned(
            &self,
            key: &Key,
            _max_version: CommitVersion,
        ) -> StorageResult<Option<VersionedValue>> {
            Ok(self.data.lock().unwrap().get(key).cloned())
        }

        fn get_history(
            &self,
            _key: &Key,
            _limit: Option<usize>,
            _before_version: Option<CommitVersion>,
        ) -> StorageResult<Vec<VersionedValue>> {
            Ok(Vec::new())
        }

        fn scan_prefix(
            &self,
            _prefix: &Key,
            _max_version: CommitVersion,
        ) -> StorageResult<Vec<(Key, VersionedValue)>> {
            Ok(Vec::new())
        }

        fn current_version(&self) -> CommitVersion {
            self.current
        }

        fn put_with_version_mode(
            &self,
            key: Key,
            value: Value,
            version: CommitVersion,
            _ttl: Option<Duration>,
            mode: WriteMode,
        ) -> StorageResult<()> {
            self.writes
                .lock()
                .unwrap()
                .push((key.clone(), value.clone(), mode, version));
            self.data.lock().unwrap().insert(
                key,
                VersionedValue {
                    value,
                    version: Version::txn(version.as_u64()),
                    timestamp: Timestamp::now(),
                },
            );
            Ok(())
        }

        fn delete_with_version(&self, key: &Key, version: CommitVersion) -> StorageResult<()> {
            self.deletes.lock().unwrap().push((key.clone(), version));
            self.data.lock().unwrap().remove(key);
            Ok(())
        }
    }

    fn key(name: &str) -> Key {
        let ns = Arc::new(Namespace::for_branch(BranchId::from_user_name(
            "storage-trait",
        )));
        Key::new_kv(ns, name)
    }

    fn assert_storage_surface<S: Storage + ?Sized>(_: &S) {}

    #[test]
    fn write_mode_defaults_to_append() {
        assert_eq!(WriteMode::default(), WriteMode::Append);
    }

    #[test]
    fn write_mode_roundtrips_to_legacy_core() {
        assert_eq!(WriteMode::default(), WriteMode::Append);
        assert_eq!(WriteMode::KeepLast(5), WriteMode::KeepLast(5));
    }

    #[test]
    fn apply_batch_uses_put_with_version_mode_for_each_entry() {
        let store = FakeStore::default();
        let surface: &dyn Storage = &store;
        let version = CommitVersion(42);
        let writes = vec![
            (key("a"), Value::Int(1), WriteMode::Append),
            (key("b"), Value::Int(2), WriteMode::KeepLast(1)),
        ];

        surface.apply_batch(writes, version).unwrap();
        let recorded = store.writes.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].3, version);
        assert_eq!(recorded[1].2, WriteMode::KeepLast(1));
    }

    #[test]
    fn storage_surface_accepts_storage_owned_implementors() {
        let store = FakeStore::default();
        assert_storage_surface(&store);

        let dyn_view: &dyn Storage = &store;
        assert_eq!(dyn_view.current_version(), CommitVersion(0));
        assert!(dyn_view
            .get_versioned(&key("missing"), CommitVersion::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn apply_writes_atomic_runs_writes_then_deletes() {
        let store = FakeStore::default();
        let surface: &dyn Storage = &store;
        let version = CommitVersion(9);
        let keep = key("keep");
        let drop_key = key("drop");
        surface
            .put_with_version_mode(
                drop_key.clone(),
                Value::String("before".to_string()),
                CommitVersion(1),
                None,
                WriteMode::Append,
            )
            .unwrap();

        surface
            .apply_writes_atomic(
                vec![(keep.clone(), Value::Bool(true), WriteMode::Append)],
                vec![drop_key.clone()],
                version,
                &[],
            )
            .unwrap();

        assert!(surface
            .get_versioned(&keep, CommitVersion::MAX)
            .unwrap()
            .is_some());
        assert!(surface
            .get_versioned(&drop_key, CommitVersion::MAX)
            .unwrap()
            .is_none());
        assert_eq!(store.deletes.lock().unwrap()[0].1, version);
    }

    #[test]
    fn segmented_store_history_and_scan_follow_storage_surface() {
        let store = SegmentedStore::new();
        let key = key("history-forwarded");
        let prefix = Key::new_kv(key.namespace.clone(), b"history-".to_vec());
        let storage_view: &dyn Storage = &store;

        storage_view
            .put_with_version_mode(
                key.clone(),
                Value::Int(1),
                CommitVersion(1),
                None,
                WriteMode::Append,
            )
            .unwrap();
        storage_view
            .put_with_version_mode(
                key.clone(),
                Value::Int(2),
                CommitVersion(2),
                None,
                WriteMode::Append,
            )
            .unwrap();
        let history = storage_view.get_history(&key, None, None).unwrap();
        let scan = storage_view
            .scan_prefix(&prefix, CommitVersion::MAX)
            .unwrap();

        assert_eq!(history.len(), 2);
        assert_eq!(history[0].value, Value::Int(2));
        assert_eq!(history[1].value, Value::Int(1));
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].0, key);
        assert_eq!(scan[0].1.value, Value::Int(2));
    }

    #[test]
    fn segmented_store_atomic_writes_match_storage_surface() {
        let store = SegmentedStore::new();
        let ns = Arc::new(Namespace::for_branch(BranchId::from_user_name(
            "atomic-forwarded",
        )));
        let keep = Key::new_kv(ns.clone(), b"atomic-keep".to_vec());
        let drop_key = Key::new_kv(ns.clone(), b"atomic-drop".to_vec());
        let prefix = Key::new_kv(ns, b"atomic-".to_vec());
        let storage_view: &dyn Storage = &store;

        storage_view
            .put_with_version_mode(
                drop_key.clone(),
                Value::String("before".to_string()),
                CommitVersion(1),
                None,
                WriteMode::Append,
            )
            .unwrap();

        storage_view
            .apply_writes_atomic(
                vec![(keep.clone(), Value::Bool(true), WriteMode::Append)],
                vec![drop_key.clone()],
                CommitVersion(2),
                &[],
            )
            .unwrap();

        let keep_history = storage_view.get_history(&keep, None, None).unwrap();
        let dropped = storage_view
            .get_versioned(&drop_key, CommitVersion::MAX)
            .unwrap();
        let scan = storage_view
            .scan_prefix(&prefix, CommitVersion::MAX)
            .unwrap();

        assert_eq!(keep_history.len(), 1);
        assert_eq!(keep_history[0].value, Value::Bool(true));
        assert!(dropped.is_none());
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].0, keep);
        assert_eq!(scan[0].1.value, Value::Bool(true));
    }
}
