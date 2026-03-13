//! TTL (Time-To-Live) index for efficient expiration cleanup
//!
//! This module provides TTLIndex that enables efficient queries for expired keys
//! without scanning the entire data store:
//! - Maps expiry_timestamp → Set<Key> using BTreeMap for sorted order
//! - find_expired() returns all keys expired before a given timestamp
//! - O(expired count) instead of O(total data)

use std::collections::{BTreeMap, HashSet};
use strata_core::{Key, Timestamp};

/// TTL index: expiry_timestamp → Keys
///
/// Enables efficient cleanup of expired keys by maintaining a mapping from
/// expiry timestamps to sets of keys that expire at that time.
///
/// Uses BTreeMap for sorted ordering, allowing efficient range queries
/// for all keys expired before a given timestamp.
#[derive(Debug, Default)]
pub struct TTLIndex {
    /// Index mapping expiry timestamp to keys expiring at that time
    index: BTreeMap<Timestamp, HashSet<Key>>,
}

impl TTLIndex {
    /// Create a new empty TTLIndex
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
        }
    }

    /// Add key to TTL index with given expiry timestamp
    ///
    /// The key will be included in `find_expired()` results when
    /// the current time exceeds `expiry_timestamp`.
    pub fn insert(&mut self, expiry_timestamp: Timestamp, key: Key) {
        self.index.entry(expiry_timestamp).or_default().insert(key);
    }

    /// Remove key from TTL index at given expiry timestamp
    ///
    /// Used when a key is deleted or overwritten.
    /// If the set becomes empty, removes the timestamp entry entirely.
    pub fn remove(&mut self, expiry_timestamp: Timestamp, key: &Key) {
        if let Some(keys) = self.index.get_mut(&expiry_timestamp) {
            keys.remove(key);
            if keys.is_empty() {
                self.index.remove(&expiry_timestamp);
            }
        }
    }

    /// Find all keys that have expired before the given timestamp
    ///
    /// Returns a Vec of all keys where expiry_timestamp < now.
    /// This is O(expired count) not O(total data) because we use
    /// BTreeMap range query to only scan expired entries.
    pub fn find_expired(&self, now: Timestamp) -> Vec<Key> {
        self.index
            .range(..=now)
            .flat_map(|(_, keys)| keys.iter().cloned())
            .collect()
    }

    /// Remove all entries for keys that have expired before the given timestamp
    ///
    /// This is used after cleanup to remove stale index entries.
    /// Returns the number of entries removed.
    pub fn remove_expired(&mut self, now: Timestamp) -> usize {
        let expired_timestamps: Vec<Timestamp> =
            self.index.range(..=now).map(|(ts, _)| *ts).collect();

        let mut count = 0;
        for ts in expired_timestamps {
            if let Some(keys) = self.index.remove(&ts) {
                count += keys.len();
            }
        }
        count
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Get the total number of keys in the index
    pub fn len(&self) -> usize {
        self.index.values().map(|keys| keys.len()).sum()
    }

    /// Get the number of unique expiry timestamps
    pub fn timestamp_count(&self) -> usize {
        self.index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::{BranchId, Namespace};

    /// Helper to create a test key
    fn test_key(suffix: &str) -> Key {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));
        Key::new_kv(ns, suffix)
    }

    #[test]
    fn test_ttl_index_insert_and_find_expired() {
        let mut index = TTLIndex::new();
        let now = Timestamp::from_micros(1000); // Use a fixed timestamp for testing

        // Insert keys with different expiry times
        let key1 = test_key("expires_at_500");
        let key2 = test_key("expires_at_800");
        let key3 = test_key("expires_at_1200");
        let key4 = test_key("expires_at_500_also");

        index.insert(Timestamp::from_micros(500), key1.clone());
        index.insert(Timestamp::from_micros(800), key2.clone());
        index.insert(Timestamp::from_micros(1200), key3.clone());
        index.insert(Timestamp::from_micros(500), key4.clone()); // Same expiry as key1

        // Find expired at time 1000 - should include key1, key2, key4
        let expired = index.find_expired(now);
        assert_eq!(expired.len(), 3);
        assert!(expired.contains(&key1));
        assert!(expired.contains(&key2));
        assert!(expired.contains(&key4));
        assert!(!expired.contains(&key3)); // Not expired yet
    }

    #[test]
    fn test_ttl_index_remove() {
        let mut index = TTLIndex::new();

        let key1 = test_key("key1");
        let key2 = test_key("key2");

        index.insert(Timestamp::from_micros(500), key1.clone());
        index.insert(Timestamp::from_micros(500), key2.clone());

        assert_eq!(index.len(), 2);

        // Remove one key
        index.remove(Timestamp::from_micros(500), &key1);
        assert_eq!(index.len(), 1);

        // Verify only key2 remains
        let expired = index.find_expired(Timestamp::from_micros(600));
        assert_eq!(expired.len(), 1);
        assert!(expired.contains(&key2));

        // Remove the last key - timestamp entry should be cleaned up
        index.remove(Timestamp::from_micros(500), &key2);
        assert!(index.is_empty());
        assert_eq!(index.timestamp_count(), 0);
    }

    #[test]
    fn test_ttl_index_remove_expired() {
        let mut index = TTLIndex::new();

        let key1 = test_key("key1");
        let key2 = test_key("key2");
        let key3 = test_key("key3");

        index.insert(Timestamp::from_micros(500), key1);
        index.insert(Timestamp::from_micros(800), key2);
        index.insert(Timestamp::from_micros(1200), key3);

        // Remove expired before 1000
        let removed = index.remove_expired(Timestamp::from_micros(1000));
        assert_eq!(removed, 2); // key1 and key2

        // Only key3 should remain
        assert_eq!(index.len(), 1);
        assert_eq!(index.timestamp_count(), 1);
    }

    #[test]
    fn test_ttl_index_find_expired_empty() {
        let index = TTLIndex::new();
        let expired = index.find_expired(Timestamp::from_micros(1000));
        assert!(expired.is_empty());
    }

    #[test]
    fn test_ttl_index_find_expired_none_expired() {
        let mut index = TTLIndex::new();

        let key = test_key("future_key");
        index.insert(Timestamp::from_micros(2000), key);

        // At time 1000, nothing is expired
        let expired = index.find_expired(Timestamp::from_micros(1000));
        assert!(expired.is_empty());
    }

    #[test]
    fn test_ttl_index_default() {
        let index = TTLIndex::default();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.timestamp_count(), 0);
    }

    #[test]
    fn test_ttl_index_multiple_timestamps() {
        let mut index = TTLIndex::new();

        // Insert keys at 5 different timestamps
        for i in 0..5u64 {
            let key = test_key(&format!("key_{}", i));
            index.insert(Timestamp::from_micros((i + 1) * 100), key);
        }

        assert_eq!(index.len(), 5);
        assert_eq!(index.timestamp_count(), 5);

        // Find expired at 350 - should get keys at 100, 200, 300
        let expired = index.find_expired(Timestamp::from_micros(350));
        assert_eq!(expired.len(), 3);
    }
}
