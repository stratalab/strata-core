//! Shared block cache for decompressed KV segment data blocks.
//!
//! Caches decompressed data blocks to eliminate repeated zstd decompression
//! on the read hot path. A global LRU cache is shared across all segments.
//!
//! ## Architecture
//!
//! 16-shard LRU cache with O(1) insert, lookup, and eviction. Each shard is
//! independently locked (`parking_lot::Mutex`) so concurrent readers/writers
//! on different shards never contend. Within each shard, a `HashMap` provides
//! O(1) key lookup and a doubly-linked list provides O(1) LRU eviction.
//!
//! Two priority tiers (HIGH for index/bloom blocks, LOW for data blocks)
//! ensure metadata stays cached under memory pressure.
//!
//! Cache key: `(file_id, block_offset)` where file_id is derived from the
//! file path hash. This ensures distinct segments never collide even if they
//! happen to have the same block offsets.

use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Number of independent cache shards. Must be a power of two.
const NUM_SHARDS: usize = 16;

/// Default block cache capacity: 64 MiB.
const DEFAULT_CAPACITY_BYTES: usize = 64 * 1024 * 1024;

/// A cache key identifying a specific data block in a specific segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CacheKey {
    /// Hash of the segment file path (distinguishes segments).
    file_id: u64,
    /// Byte offset of the block within the segment file.
    block_offset: u64,
}

/// Priority tier for cached blocks.
///
/// HIGH priority blocks (index, bloom filters) are evicted only when no
/// LOW priority blocks remain. This keeps hot metadata resident even
/// under data-block churn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// Data blocks — evicted first under memory pressure.
    Low,
    /// Index and bloom filter blocks — evicted last.
    High,
}

// ---------------------------------------------------------------------------
// Doubly-linked LRU list (intrusive, sentinel-based)
// ---------------------------------------------------------------------------

/// Node in the per-shard doubly-linked LRU list.
///
/// Heap-allocated via `Box::into_raw`. Ownership is tracked by `LruShard.map`;
/// the linked list provides ordering only.
struct LruNode {
    key: CacheKey,
    data: Arc<Vec<u8>>,
    size: usize,
    priority: Priority,
    prev: *mut LruNode,
    next: *mut LruNode,
}

/// Doubly-linked list with head/tail sentinels for O(1) operations.
///
/// Invariants:
/// - `head.next` points to the most-recently-used node (or `tail` if empty).
/// - `tail.prev` points to the least-recently-used node (or `head` if empty).
/// - All data nodes are between `head` and `tail`.
struct LruList {
    head: *mut LruNode,
    tail: *mut LruNode,
}

// SAFETY: LruList nodes are only accessed while the owning shard mutex is held.
unsafe impl Send for LruList {}

impl LruList {
    /// Create a new empty list with sentinel nodes.
    fn new() -> Self {
        // Sentinel nodes are never exposed to callers.
        let head = Box::into_raw(Box::new(LruNode {
            key: CacheKey {
                file_id: 0,
                block_offset: 0,
            },
            data: Arc::new(Vec::new()),
            size: 0,
            priority: Priority::Low,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }));
        let tail = Box::into_raw(Box::new(LruNode {
            key: CacheKey {
                file_id: 0,
                block_offset: 0,
            },
            data: Arc::new(Vec::new()),
            size: 0,
            priority: Priority::Low,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }));
        // SAFETY: head and tail are valid, non-null, uniquely owned pointers.
        unsafe {
            (*head).next = tail;
            (*tail).prev = head;
        }
        LruList { head, tail }
    }

    /// Insert `node` at the MRU position (right after the head sentinel).
    ///
    /// SAFETY: `node` must be a valid, non-null pointer not currently in any list.
    unsafe fn push_front(&self, node: *mut LruNode) {
        let next = (*self.head).next;
        (*node).prev = self.head;
        (*node).next = next;
        (*self.head).next = node;
        (*next).prev = node;
    }

    /// Remove `node` from this list.
    ///
    /// SAFETY: `node` must be a valid pointer currently linked in this list
    /// (not a sentinel).
    unsafe fn remove(node: *mut LruNode) {
        let prev = (*node).prev;
        let next = (*node).next;
        (*prev).next = next;
        (*next).prev = prev;
        (*node).prev = ptr::null_mut();
        (*node).next = ptr::null_mut();
    }

    /// Pop the LRU node (right before the tail sentinel). Returns `None` if empty.
    ///
    /// SAFETY: the list must only contain valid data-node pointers between sentinels.
    unsafe fn pop_back(&self) -> Option<*mut LruNode> {
        let node = (*self.tail).prev;
        if node == self.head {
            return None;
        }
        Self::remove(node);
        Some(node)
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        // SAFETY: head is always valid.
        unsafe { (*self.head).next == self.tail }
    }
}

impl Drop for LruList {
    fn drop(&mut self) {
        // Only free sentinel nodes. Data nodes are freed by `LruShard::drop`
        // via the HashMap before this runs.
        unsafe {
            // Detach sentinels from any remaining nodes (defensive).
            (*self.head).next = self.tail;
            (*self.tail).prev = self.head;
            drop(Box::from_raw(self.head));
            drop(Box::from_raw(self.tail));
        }
    }
}

// ---------------------------------------------------------------------------
// Per-shard state
// ---------------------------------------------------------------------------

/// One of 16 independently-locked cache shards.
///
/// Each shard maintains two LRU lists (LOW and HIGH priority) and a HashMap
/// for O(1) key-to-node lookup.
struct LruShard {
    /// Maps cache keys to their LRU nodes. Provides O(1) lookup.
    /// The HashMap owns the node pointers (freed in `Drop`).
    map: HashMap<CacheKey, *mut LruNode>,
    /// LRU list for LOW priority (data) blocks. Evicted first.
    low: LruList,
    /// LRU list for HIGH priority (index/bloom) blocks. Evicted last.
    high: LruList,
    /// Current total size of cached data in this shard.
    current_bytes: usize,
    /// Maximum capacity for this shard.
    capacity_bytes: usize,
}

// SAFETY: node pointers in `map` are only accessed while the shard mutex is held.
unsafe impl Send for LruShard {}

impl LruShard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            map: HashMap::new(),
            low: LruList::new(),
            high: LruList::new(),
            current_bytes: 0,
            capacity_bytes,
        }
    }

    /// Evict LRU entries until `needed` bytes can be accommodated.
    /// LOW priority entries are evicted first; HIGH only when LOW is exhausted.
    fn evict_for(&mut self, needed: usize) {
        while self.current_bytes + needed > self.capacity_bytes {
            // Try LOW priority first
            let evicted = unsafe { self.low.pop_back() };
            let node = if let Some(n) = evicted {
                n
            } else {
                // Fall back to HIGH priority
                match unsafe { self.high.pop_back() } {
                    Some(n) => n,
                    None => break,
                }
            };
            unsafe {
                self.current_bytes = self.current_bytes.saturating_sub((*node).size);
                self.map.remove(&(*node).key);
                drop(Box::from_raw(node));
            }
        }
    }
}

impl Drop for LruShard {
    fn drop(&mut self) {
        // Free all data nodes. The LruList::drop will free sentinels only.
        for (_, node_ptr) in self.map.drain() {
            unsafe {
                drop(Box::from_raw(node_ptr));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BlockCache — the public sharded cache
// ---------------------------------------------------------------------------

/// Thread-safe sharded LRU block cache for decompressed segment data blocks.
///
/// 16 shards, each independently locked. Lookups, inserts, and evictions are
/// all O(1). Two priority tiers keep index/bloom metadata resident under
/// data-block churn.
pub struct BlockCache {
    shards: Vec<parking_lot::Mutex<LruShard>>,
    total_capacity: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Cache statistics snapshot.
#[derive(Debug, Clone)]
pub struct BlockCacheStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Current number of cached blocks.
    pub entries: usize,
    /// Current total size of cached data in bytes.
    pub size_bytes: usize,
    /// Maximum capacity in bytes.
    pub capacity_bytes: usize,
}

impl BlockCache {
    /// Create a new block cache with the given total capacity in bytes.
    ///
    /// Capacity is divided equally across 16 shards.
    pub fn new(capacity_bytes: usize) -> Self {
        let per_shard = capacity_bytes / NUM_SHARDS;
        let shards = (0..NUM_SHARDS)
            .map(|_| parking_lot::Mutex::new(LruShard::new(per_shard)))
            .collect();
        Self {
            shards,
            total_capacity: capacity_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a block cache with the default capacity (64 MiB).
    pub fn default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY_BYTES)
    }

    /// Determine which shard a key maps to.
    #[inline]
    fn shard_index(key: &CacheKey) -> usize {
        // Mix file_id and block_offset for even distribution across shards.
        let h = key.file_id.wrapping_mul(0x517cc1b727220a95) ^ key.block_offset;
        (h as usize) & (NUM_SHARDS - 1)
    }

    /// Look up a cached block. Returns the decompressed data if present.
    ///
    /// On hit, moves the block to the MRU position (O(1)).
    pub fn get(&self, file_id: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        let key = CacheKey {
            file_id,
            block_offset,
        };
        let shard = self.shards[Self::shard_index(&key)].lock();
        if let Some(&node) = shard.map.get(&key) {
            unsafe {
                // Move to head of the node's priority list (mark as MRU).
                LruList::remove(node);
                match (*node).priority {
                    Priority::Low => shard.low.push_front(node),
                    Priority::High => shard.high.push_front(node),
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(Arc::clone(&(*node).data))
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a decompressed block with LOW priority (data blocks).
    ///
    /// If the cache is over capacity after insertion, evicts the
    /// least-recently-used LOW priority entries first, then HIGH.
    /// If the block is larger than the shard capacity, it is not cached.
    pub fn insert(&self, file_id: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        self.insert_with_priority(file_id, block_offset, data, Priority::Low)
    }

    /// Insert a decompressed block with an explicit priority tier.
    ///
    /// Use `Priority::High` for index and bloom filter blocks that should
    /// survive eviction pressure from data blocks.
    pub fn insert_with_priority(
        &self,
        file_id: u64,
        block_offset: u64,
        data: Vec<u8>,
        priority: Priority,
    ) -> Arc<Vec<u8>> {
        let size = data.len();
        let data = Arc::new(data);
        let key = CacheKey {
            file_id,
            block_offset,
        };
        let mut shard = self.shards[Self::shard_index(&key)].lock();

        // Don't cache blocks larger than this shard's capacity.
        // Also skip if the shard has zero capacity (degenerate config).
        if size > shard.capacity_bytes || shard.capacity_bytes == 0 {
            return data;
        }

        // Already present (another thread beat us) — return existing
        if let Some(&node) = shard.map.get(&key) {
            unsafe {
                return Arc::clone(&(*node).data);
            }
        }

        // Evict until there's room
        shard.evict_for(size);

        // Allocate and link the new node
        let node = Box::into_raw(Box::new(LruNode {
            key,
            data: Arc::clone(&data),
            size,
            priority,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }));

        unsafe {
            match priority {
                Priority::Low => shard.low.push_front(node),
                Priority::High => shard.high.push_front(node),
            }
        }
        shard.map.insert(key, node);
        shard.current_bytes += size;

        data
    }

    /// Remove all cached blocks for a given file.
    ///
    /// Called after compaction deletes a segment file, to free cache
    /// space occupied by blocks from the deleted segment.
    pub fn invalidate_file(&self, file_id: u64) {
        for mutex in &self.shards {
            let mut shard = mutex.lock();
            let keys: Vec<CacheKey> = shard
                .map
                .keys()
                .filter(|k| k.file_id == file_id)
                .copied()
                .collect();
            for key in keys {
                if let Some(node) = shard.map.remove(&key) {
                    unsafe {
                        shard.current_bytes = shard.current_bytes.saturating_sub((*node).size);
                        LruList::remove(node);
                        drop(Box::from_raw(node));
                    }
                }
            }
        }
    }

    /// Get cache statistics (aggregated across all shards).
    pub fn stats(&self) -> BlockCacheStats {
        let mut total_entries = 0;
        let mut total_bytes = 0;
        for mutex in &self.shards {
            let shard = mutex.lock();
            total_entries += shard.map.len();
            total_bytes += shard.current_bytes;
        }
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: total_entries,
            size_bytes: total_bytes,
            capacity_bytes: self.total_capacity,
        }
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Compute a file identity hash from a file path.
///
/// Uses FxHash-style mixing for speed. The hash doesn't need to be
/// cryptographic — it just needs to distinguish different segment files.
pub fn file_path_hash(path: &std::path::Path) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    path.hash(&mut hasher);
    hasher.finish()
}

/// Auto-detect a reasonable cache capacity based on available system memory.
///
/// On Linux, reads `/proc/meminfo` and returns `max(256 MiB, available_ram / 4)`.
/// On other platforms (or if detection fails), returns 256 MiB.
pub fn auto_detect_capacity() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if let Some(rest) = line.strip_prefix("MemAvailable:") {
                    if let Some(kb_str) = rest.split_whitespace().next() {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            let quarter = (kb * 1024) / 4;
                            return quarter.max(256 * 1024 * 1024);
                        }
                    }
                }
            }
        }
    }
    256 * 1024 * 1024
}

// ---------------------------------------------------------------------------
// Global singleton
// ---------------------------------------------------------------------------

/// Global block cache singleton.
///
/// Lazily initialized on first access. Shared across all segments in
/// the process. Capacity can be configured via `set_global_capacity()`
/// before the first access.
static GLOBAL_CACHE: std::sync::OnceLock<BlockCache> = std::sync::OnceLock::new();
static GLOBAL_CAPACITY: AtomicUsize = AtomicUsize::new(DEFAULT_CAPACITY_BYTES);

/// Set the global block cache capacity. Must be called before any reads.
pub fn set_global_capacity(bytes: usize) {
    GLOBAL_CAPACITY.store(bytes, Ordering::Relaxed);
}

/// Get or create the global block cache.
pub fn global_cache() -> &'static BlockCache {
    GLOBAL_CACHE.get_or_init(|| BlockCache::new(GLOBAL_CAPACITY.load(Ordering::Relaxed)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit_and_miss() {
        let cache = BlockCache::new(1024 * 1024);

        // Miss
        assert!(cache.get(1, 0).is_none());

        // Insert
        let data = vec![1, 2, 3, 4, 5];
        let cached = cache.insert(1, 0, data.clone());
        assert_eq!(&*cached, &data);

        // Hit
        let hit = cache.get(1, 0).unwrap();
        assert_eq!(&*hit, &data);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
    }

    #[test]
    fn cache_eviction_on_capacity() {
        // 100 bytes per shard (16 * 100 = 1600 total)
        let cache = BlockCache::new(16 * 100);

        // Find 3 keys that map to the same shard so eviction is testable
        let mut keys = Vec::new();
        for fid in 0u64..1000 {
            let key = CacheKey {
                file_id: fid,
                block_offset: 0,
            };
            if BlockCache::shard_index(&key) == 0 {
                keys.push(fid);
                if keys.len() >= 3 {
                    break;
                }
            }
        }

        // Insert 2 blocks of 40 bytes each — fits in 100-byte shard
        cache.insert(keys[0], 0, vec![0; 40]);
        cache.insert(keys[1], 0, vec![1; 40]);
        assert_eq!(cache.stats().entries, 2);

        // 3rd should evict the 1st (LRU) — shard only has 100 bytes
        cache.insert(keys[2], 0, vec![2; 40]);
        assert_eq!(
            cache.stats().entries,
            2,
            "3rd insert should have evicted one entry"
        );
        // Verify the evicted entry is gone and the newest is present
        assert!(
            cache.get(keys[0], 0).is_none(),
            "LRU entry should be evicted"
        );
        assert!(cache.get(keys[2], 0).is_some(), "newest entry should exist");
    }

    #[test]
    fn cache_different_files_same_offset() {
        let cache = BlockCache::new(1024 * 1024);

        cache.insert(1, 0, vec![1, 1, 1]);
        cache.insert(2, 0, vec![2, 2, 2]);

        let a = cache.get(1, 0).unwrap();
        let b = cache.get(2, 0).unwrap();
        assert_eq!(&*a, &[1, 1, 1]);
        assert_eq!(&*b, &[2, 2, 2]);
    }

    #[test]
    fn block_larger_than_cache_not_stored() {
        let cache = BlockCache::new(10);
        let data = vec![0; 100]; // Larger than cache
        let result = cache.insert(1, 0, data.clone());
        assert_eq!(&*result, &data);
        assert_eq!(cache.stats().entries, 0); // Not cached
    }

    #[test]
    fn invalidate_file_removes_all_blocks() {
        let cache = BlockCache::new(1024 * 1024);

        // Mix of LOW and HIGH priority entries for file 1
        cache.insert(1, 0, vec![0; 100]);
        cache.insert_with_priority(1, 100, vec![0; 100], Priority::High);
        cache.insert(2, 0, vec![0; 100]);
        assert_eq!(cache.stats().entries, 3);

        cache.invalidate_file(1);
        assert_eq!(cache.stats().entries, 1);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 100).is_none());
        assert!(cache.get(2, 0).is_some());

        // Verify size accounting is correct after invalidation
        assert_eq!(cache.stats().size_bytes, 100);
    }

    #[test]
    fn high_priority_survives_low_eviction() {
        // Small cache: only room for ~2 entries of 30 bytes each
        // (capacity / 16 shards means per-shard capacity is small)
        // Use a cache big enough that at least 1 shard can hold 2 entries.
        let cache = BlockCache::new(16 * 80); // 80 bytes per shard

        // Insert a HIGH priority block — use file_id/offset that lands in a
        // predictable shard.
        let high_fid = 100u64;
        let high_off = 0u64;
        cache.insert_with_priority(high_fid, high_off, vec![0xAA; 30], Priority::High);

        // Fill the same shard with LOW priority blocks to trigger eviction.
        // To hit the same shard, we need keys with the same shard_index.
        let target_shard = BlockCache::shard_index(&CacheKey {
            file_id: high_fid,
            block_offset: high_off,
        });

        // Find file_ids that map to the same shard
        let mut low_keys = Vec::new();
        for fid in 200u64..500 {
            let key = CacheKey {
                file_id: fid,
                block_offset: 0,
            };
            if BlockCache::shard_index(&key) == target_shard {
                low_keys.push(fid);
                if low_keys.len() >= 5 {
                    break;
                }
            }
        }

        // Insert enough LOW blocks to force eviction
        for &fid in &low_keys {
            cache.insert(fid, 0, vec![0xBB; 30]);
        }

        // The HIGH priority block should still be present
        let high = cache.get(high_fid, high_off);
        assert!(
            high.is_some(),
            "HIGH priority block should survive LOW eviction"
        );
        assert_eq!(&*high.unwrap(), &vec![0xAA; 30]);
    }

    #[test]
    fn concurrent_access_no_deadlock() {
        use std::sync::Arc;
        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let mut handles = Vec::new();

        for t in 0..16u64 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..1000u64 {
                    let fid = t * 1000 + i;
                    c.insert(fid, 0, vec![t as u8; 64]);
                    c.get(fid, 0);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Just verify no panic/deadlock — stats should be consistent
        let stats = cache.stats();
        assert!(stats.entries > 0);
        assert!(stats.hits > 0);
    }

    #[test]
    fn eviction_is_lru_order() {
        // 70 bytes per shard: fits 2 x 30-byte entries but not 3.
        let cache = BlockCache::new(16 * 70); // 70 bytes per shard

        // Find 4 keys that map to the same shard
        let mut keys = Vec::new();
        for fid in 0u64..1000 {
            let key = CacheKey {
                file_id: fid,
                block_offset: 0,
            };
            if BlockCache::shard_index(&key) == 0 {
                keys.push(fid);
                if keys.len() >= 4 {
                    break;
                }
            }
        }
        assert!(keys.len() >= 4, "need 4 keys in shard 0");

        // Insert A, B (fills 60 of 70 bytes)
        cache.insert(keys[0], 0, vec![0xA; 30]);
        cache.insert(keys[1], 0, vec![0xB; 30]);

        // Access A to make it MRU (B is now LRU)
        cache.get(keys[0], 0);

        // Insert C (30 bytes) — evicts B (LRU), not A
        cache.insert(keys[2], 0, vec![0xC; 30]);

        // A and C should be present, B should be gone
        assert!(
            cache.get(keys[0], 0).is_some(),
            "A should survive (was MRU)"
        );
        assert!(
            cache.get(keys[1], 0).is_none(),
            "B should be evicted (was LRU)"
        );
        assert!(
            cache.get(keys[2], 0).is_some(),
            "C should be present (just inserted)"
        );
    }

    #[test]
    fn duplicate_insert_returns_existing() {
        let cache = BlockCache::new(1024 * 1024);

        let first = cache.insert(1, 0, vec![0xAA; 10]);
        let second = cache.insert(1, 0, vec![0xBB; 10]); // same key, different data

        // Second insert should return the FIRST data (dedup)
        assert_eq!(&*second, &vec![0xAA; 10]);
        assert_eq!(Arc::as_ptr(&first), Arc::as_ptr(&second));
        assert_eq!(cache.stats().entries, 1);
    }

    #[test]
    fn zero_capacity_cache_does_not_store() {
        let cache = BlockCache::new(0);

        let data = cache.insert(1, 0, vec![1, 2, 3]);
        assert_eq!(&*data, &[1, 2, 3]); // data is returned
        assert_eq!(cache.stats().entries, 0); // but not cached
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn auto_detect_returns_positive() {
        let cap = auto_detect_capacity();
        assert!(
            cap >= 256 * 1024 * 1024,
            "auto-detect should return at least 256 MiB, got {}",
            cap
        );
    }
}
