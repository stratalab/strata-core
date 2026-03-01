//! Snapshot isolation for OCC transactions
//!
//! This module provides snapshot implementations for transactions.
//! M2 uses `ClonedSnapshotView` (deep copy of data).
//!
//! # Snapshot Isolation Guarantees
//!
//! From `docs/architecture/M2_TRANSACTION_SEMANTICS.md`:
//!
//! **What a Snapshot ALWAYS Provides** (Section 2.1):
//! - Committed data as of `start_version`
//! - Consistent point-in-time view
//! - Repeatable reads (same key returns same value)
//!
//! **What a Snapshot NEVER Shows** (Section 2.2):
//! - Writes committed AFTER `start_version`
//! - Uncommitted writes from other transactions
//! - Partial writes
//!
//! # Future Optimization
//!
//! The `SnapshotView` trait (defined in `strata_core::traits`) enables
//! future implementations like `LazySnapshotView` that read from live
//! storage with version bounds, avoiding the clone overhead.

use std::collections::BTreeMap;
use std::sync::Arc;
use strata_core::traits::SnapshotView;
use strata_core::types::Key;
use strata_core::StrataResult;
use strata_core::VersionedValue;

/// Clone-based snapshot (O(n) deep copy).
///
/// Creates an immutable, isolated view of storage by cloning the data
/// at snapshot creation time. This is primarily used in **tests** and
/// **recovery** where simplicity matters more than performance.
///
/// For production transaction workloads, prefer `ShardedSnapshot` from
/// `strata_storage`, which provides O(1) snapshot creation via
/// lock-free DashMap iteration.
///
/// # Known Limitations
///
/// - **Memory**: O(data_size) per active transaction
/// - **Time**: O(data_size) per snapshot creation
///
/// # Why This Exists
///
/// - Agent workloads have small working sets (<100MB typical)
/// - Transactions are short-lived (<1 second)
/// - Simplicity enables correctness validation
/// - The `SnapshotView` trait abstraction enables future optimization
///
/// # Future Optimization: LazySnapshotView
///
/// M3+ may implement `LazySnapshotView` that:
/// - Reads from live storage with version bounds
/// - Has O(1) creation time and memory
/// - Trades read latency for snapshot creation cost
///
/// # Example
///
/// ```
/// use strata_concurrency::snapshot::ClonedSnapshotView;
/// use strata_core::traits::SnapshotView;
/// use std::collections::BTreeMap;
///
/// // Create empty snapshot
/// let snapshot = ClonedSnapshotView::empty(100);
/// assert_eq!(snapshot.version(), 100);
/// assert!(snapshot.is_empty());
/// ```
pub struct ClonedSnapshotView {
    /// The snapshot version (global version at creation time)
    version: u64,
    /// Immutable clone of storage data
    data: Arc<BTreeMap<Key, VersionedValue>>,
}

impl ClonedSnapshotView {
    /// Create a new snapshot by taking ownership of the provided data
    ///
    /// # Arguments
    /// * `version` - The snapshot version (global version at creation time)
    /// * `data` - The data for this snapshot
    ///
    /// # Performance
    /// This wraps the data in an Arc, which is O(1).
    /// The actual cloning cost is paid by the caller when preparing the data.
    pub fn new(version: u64, data: BTreeMap<Key, VersionedValue>) -> Self {
        ClonedSnapshotView {
            version,
            data: Arc::new(data),
        }
    }

    /// Create an empty snapshot at a given version
    ///
    /// Useful for testing or for transactions that start with no data.
    pub fn empty(version: u64) -> Self {
        ClonedSnapshotView {
            version,
            data: Arc::new(BTreeMap::new()),
        }
    }

    /// Get the number of keys in the snapshot
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the snapshot is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a reference to the underlying data (for testing/debugging)
    #[cfg(test)]
    pub fn data(&self) -> &BTreeMap<Key, VersionedValue> {
        &self.data
    }
}

impl SnapshotView for ClonedSnapshotView {
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        // Simple lookup - data is already filtered to snapshot version
        Ok(self.data.get(key).cloned())
    }

    fn scan_prefix(&self, prefix: &Key) -> StrataResult<Vec<(Key, VersionedValue)>> {
        // Use Key::starts_with for prefix matching
        // BTreeMap iteration is ordered, so results are sorted
        let results: Vec<(Key, VersionedValue)> = self
            .data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(results)
    }

    fn version(&self) -> u64 {
        self.version
    }
}

#[cfg(test)]
mod tests {
    // Verify ClonedSnapshotView is Send + Sync (required by SnapshotView trait)
    static_assertions::assert_impl_all!(super::ClonedSnapshotView: Send, Sync);
    use super::*;
    use strata_core::types::{BranchId, Namespace, TypeTag};
    use strata_core::value::Value;
    use strata_core::Version;

    // === Test Helpers ===

    fn create_test_namespace() -> Arc<Namespace> {
        Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            BranchId::new(),
            "default".to_string(),
        ))
    }

    fn create_test_key(ns: &Arc<Namespace>, user_key: &[u8]) -> Key {
        Key::new(ns.clone(), TypeTag::KV, user_key.to_vec())
    }

    fn create_versioned_value(data: &[u8], version: u64) -> VersionedValue {
        VersionedValue::new(Value::Bytes(data.to_vec()), Version::txn(version))
    }

    fn create_populated_snapshot() -> (ClonedSnapshotView, Arc<Namespace>) {
        let ns = create_test_namespace();
        let mut data = BTreeMap::new();

        // Add some test data
        data.insert(
            create_test_key(&ns, b"key1"),
            create_versioned_value(b"value1", 10),
        );
        data.insert(
            create_test_key(&ns, b"key2"),
            create_versioned_value(b"value2", 20),
        );
        data.insert(
            create_test_key(&ns, b"key3"),
            create_versioned_value(b"value3", 30),
        );
        data.insert(
            create_test_key(&ns, b"other"),
            create_versioned_value(b"other_value", 40),
        );

        (ClonedSnapshotView::new(100, data), ns)
    }

    // === Empty Snapshot Tests ===

    #[test]
    fn test_empty_snapshot_has_correct_version() {
        let snapshot = ClonedSnapshotView::empty(50);
        assert_eq!(snapshot.version(), 50);
    }

    #[test]
    fn test_empty_snapshot_is_empty() {
        let snapshot = ClonedSnapshotView::empty(50);
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn test_empty_snapshot_get_returns_none() {
        let snapshot = ClonedSnapshotView::empty(50);
        let ns = create_test_namespace();
        let key = create_test_key(&ns, b"any_key");

        let result = snapshot.get(&key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_snapshot_scan_returns_empty() {
        let snapshot = ClonedSnapshotView::empty(50);
        let ns = create_test_namespace();
        let prefix = create_test_key(&ns, b"prefix");

        let results = snapshot.scan_prefix(&prefix).unwrap();
        assert!(results.is_empty());
    }

    // === Populated Snapshot Tests ===

    #[test]
    fn test_snapshot_version() {
        let (snapshot, _) = create_populated_snapshot();
        assert_eq!(snapshot.version(), 100);
    }

    #[test]
    fn test_snapshot_len() {
        let (snapshot, _) = create_populated_snapshot();
        assert_eq!(snapshot.len(), 4);
        assert!(!snapshot.is_empty());
    }

    #[test]
    fn test_get_existing_key() {
        let (snapshot, ns) = create_populated_snapshot();
        let key = create_test_key(&ns, b"key1");

        let result = snapshot.get(&key).unwrap();
        assert!(result.is_some());

        let vv = result.unwrap();
        assert_eq!(vv.version.as_u64(), 10);
        match &vv.value {
            Value::Bytes(data) => assert_eq!(data, b"value1"),
            _ => panic!("Expected Bytes value"),
        }
    }

    #[test]
    fn test_get_nonexistent_key() {
        let (snapshot, ns) = create_populated_snapshot();
        let key = create_test_key(&ns, b"nonexistent");

        let result = snapshot.get(&key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_key_different_namespace() {
        let (snapshot, _) = create_populated_snapshot();

        // Create key with different namespace
        let other_ns = Arc::new(Namespace::new(
            "other_tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            BranchId::new(),
            "default".to_string(),
        ));
        let key = create_test_key(&other_ns, b"key1");

        let result = snapshot.get(&key).unwrap();
        assert!(result.is_none());
    }

    // === Prefix Scan Tests ===

    #[test]
    fn test_scan_prefix_matches() {
        let (snapshot, ns) = create_populated_snapshot();
        let prefix = create_test_key(&ns, b"key");

        let results = snapshot.scan_prefix(&prefix).unwrap();
        // Should find key1, key2, key3 but not "other"
        assert_eq!(results.len(), 3);

        // Verify all results start with "key"
        for (key, _) in &results {
            assert!(key.starts_with(&prefix));
        }
    }

    #[test]
    fn test_scan_prefix_no_matches() {
        let (snapshot, ns) = create_populated_snapshot();
        let prefix = create_test_key(&ns, b"nonexistent");

        let results = snapshot.scan_prefix(&prefix).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_prefix_exact_match() {
        let (snapshot, ns) = create_populated_snapshot();
        // Prefix that exactly matches one key
        let prefix = create_test_key(&ns, b"other");

        let results = snapshot.scan_prefix(&prefix).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_scan_prefix_empty_prefix() {
        let (snapshot, ns) = create_populated_snapshot();
        // Empty user_key prefix should match all keys in namespace
        let prefix = create_test_key(&ns, b"");

        let results = snapshot.scan_prefix(&prefix).unwrap();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_scan_prefix_different_namespace() {
        let (snapshot, _) = create_populated_snapshot();

        let other_ns = Arc::new(Namespace::new(
            "other_tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            BranchId::new(),
            "default".to_string(),
        ));
        let prefix = create_test_key(&other_ns, b"key");

        let results = snapshot.scan_prefix(&prefix).unwrap();
        assert!(results.is_empty());
    }

    // === Snapshot Independence Tests ===

    #[test]
    fn test_snapshot_is_independent_of_source() {
        let ns = create_test_namespace();
        let key = create_test_key(&ns, b"key");

        // Create original data
        let mut data = BTreeMap::new();
        data.insert(key.clone(), create_versioned_value(b"original", 1));

        // Create snapshot
        let snapshot = ClonedSnapshotView::new(100, data.clone());

        // "Modify" original data (simulating another transaction)
        // In real usage, this would be a separate storage instance
        data.insert(key.clone(), create_versioned_value(b"modified", 2));

        // Snapshot should still see original value
        let result = snapshot.get(&key).unwrap().unwrap();
        match &result.value {
            Value::Bytes(d) => assert_eq!(d, b"original"),
            _ => panic!("Expected Bytes value"),
        }
    }

    #[test]
    fn test_multiple_snapshots_are_independent() {
        let ns = create_test_namespace();
        let key = create_test_key(&ns, b"key");

        // Create first version of data
        let mut data1 = BTreeMap::new();
        data1.insert(key.clone(), create_versioned_value(b"v1", 1));
        let snapshot1 = ClonedSnapshotView::new(100, data1);

        // Create second version of data
        let mut data2 = BTreeMap::new();
        data2.insert(key.clone(), create_versioned_value(b"v2", 2));
        let snapshot2 = ClonedSnapshotView::new(200, data2);

        // Each snapshot sees its own version
        let v1 = snapshot1.get(&key).unwrap().unwrap();
        let v2 = snapshot2.get(&key).unwrap().unwrap();

        match (&v1.value, &v2.value) {
            (Value::Bytes(d1), Value::Bytes(d2)) => {
                assert_eq!(d1, b"v1");
                assert_eq!(d2, b"v2");
            }
            _ => panic!("Expected Bytes values"),
        }

        assert_eq!(snapshot1.version(), 100);
        assert_eq!(snapshot2.version(), 200);
    }

    // === Thread Safety Tests ===

    #[test]
    fn test_snapshot_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ClonedSnapshotView>();
    }

    #[test]
    fn test_snapshot_is_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<ClonedSnapshotView>();
    }

    #[test]
    fn test_snapshot_can_be_used_across_threads() {
        use std::sync::Arc;
        use std::thread;

        let (snapshot, ns) = create_populated_snapshot();
        let snapshot = Arc::new(snapshot);
        let key = create_test_key(&ns, b"key1");

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let snapshot = Arc::clone(&snapshot);
                let key = key.clone();
                thread::spawn(move || {
                    let result = snapshot.get(&key).unwrap();
                    assert!(result.is_some());
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    // === Trait Object Tests ===

    #[test]
    fn test_snapshot_as_trait_object() {
        let (snapshot, ns) = create_populated_snapshot();
        let key = create_test_key(&ns, b"key1");

        // Use as trait object
        let dyn_snapshot: &dyn SnapshotView = &snapshot;

        assert_eq!(dyn_snapshot.version(), 100);
        assert!(dyn_snapshot.get(&key).unwrap().is_some());
    }

    #[test]
    fn test_boxed_snapshot() {
        let (snapshot, ns) = create_populated_snapshot();
        let key = create_test_key(&ns, b"key1");

        // Box the snapshot
        let boxed: Box<dyn SnapshotView> = Box::new(snapshot);

        assert_eq!(boxed.version(), 100);
        assert!(boxed.get(&key).unwrap().is_some());
    }
}
