//! Per-branch shard — single branch's in-memory data.
//!
//! Each `BranchId` gets its own `Shard` with an `FxHashMap` for O(1) lookups.
//! Branches never contend with each other.

use std::collections::BTreeSet;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use strata_core::types::Key;

use super::version_chain::VersionChain;
use crate::ttl::TTLIndex;

/// Each BranchId gets its own shard with an FxHashMap for O(1) lookups.
/// This ensures different branches never contend with each other.
///
/// Uses VersionChain for MVCC - multiple versions per key for snapshot isolation.
///
/// # Memory Optimization
///
/// Both `data` and `ordered_keys` store `Arc<Key>` so the BTreeSet shares
/// Key allocations with the HashMap instead of deep-cloning every Key.
#[derive(Debug)]
pub struct Shard {
    /// HashMap with FxHash for O(1) lookups, storing version chains
    pub(crate) data: FxHashMap<Arc<Key>, VersionChain>,
    /// Sorted index of all keys for O(log n + k) prefix scans.
    /// Built lazily on first scan request, then maintained incrementally.
    pub(crate) ordered_keys: Option<BTreeSet<Arc<Key>>>,
    /// TTL index for efficient expiration cleanup.
    /// Maps expiry timestamps to keys for O(expired) cleanup.
    pub(crate) ttl_index: TTLIndex,
}

impl Shard {
    /// Create a new empty shard
    pub fn new() -> Self {
        Self {
            data: FxHashMap::default(),
            ordered_keys: None,
            ttl_index: TTLIndex::new(),
        }
    }

    /// Create a shard with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            ordered_keys: None,
            ttl_index: TTLIndex::new(),
        }
    }

    /// Rebuild `ordered_keys` from `data.keys()` if not yet built (None).
    /// Once built, the index is maintained incrementally by `put()` and
    /// `commit_batch()`, so this only triggers on the very first scan.
    pub(crate) fn ensure_ordered_keys(&mut self) {
        if self.ordered_keys.is_none() {
            self.ordered_keys = Some(
                self.data
                    .iter()
                    .filter(|(_, chain)| !chain.is_dead())
                    .map(|(k, _)| Arc::clone(k))
                    .collect(),
            );
        }
    }

    /// Get number of keys in this shard
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if shard is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}
