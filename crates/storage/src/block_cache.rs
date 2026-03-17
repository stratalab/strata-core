//! Shared block cache for decompressed KV segment data blocks.
//!
//! Caches decompressed data blocks to eliminate repeated zstd decompression
//! on the read hot path. A global LRU cache is shared across all segments.
//!
//! Cache key: `(block_offset_in_file, file_identity)` where file_identity
//! is derived from the file path hash. This ensures distinct segments never
//! collide even if they happen to have the same block offsets.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

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

/// Entry in the LRU list.
struct CacheEntry {
    /// Decompressed block data (shared via Arc for zero-copy reads).
    data: Arc<Vec<u8>>,
    /// Size of the decompressed data (for capacity tracking).
    size: usize,
    /// Access counter for LRU approximation (higher = more recent).
    last_access: u64,
}

/// Thread-safe LRU block cache for decompressed segment data blocks.
///
/// Uses a `parking_lot::Mutex` over a `HashMap` with approximate LRU eviction.
/// The mutex is held only for cache lookup/insert (microsecond-scale), not
/// during block decompression.
pub struct BlockCache {
    entries: parking_lot::Mutex<HashMap<CacheKey, CacheEntry>>,
    capacity_bytes: usize,
    current_bytes: AtomicUsize,
    access_counter: AtomicU64,
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
    /// Create a new block cache with the given capacity in bytes.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            entries: parking_lot::Mutex::new(HashMap::new()),
            capacity_bytes,
            current_bytes: AtomicUsize::new(0),
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a block cache with the default capacity (64 MiB).
    pub fn default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY_BYTES)
    }

    /// Look up a cached block. Returns the decompressed data if present.
    pub fn get(&self, file_id: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        let key = CacheKey {
            file_id,
            block_offset,
        };
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.get_mut(&key) {
            entry.last_access = self.access_counter.fetch_add(1, Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(&entry.data))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a decompressed block into the cache.
    ///
    /// If the cache is over capacity, evicts the least-recently-used entries
    /// until there's room. If the block is larger than the cache capacity,
    /// it is not cached.
    pub fn insert(&self, file_id: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        let size = data.len();
        let data = Arc::new(data);

        // Don't cache blocks larger than the entire cache
        if size > self.capacity_bytes {
            return data;
        }

        let key = CacheKey {
            file_id,
            block_offset,
        };
        let access = self.access_counter.fetch_add(1, Ordering::Relaxed);

        let mut entries = self.entries.lock();

        // Evict until we have room
        while self.current_bytes.load(Ordering::Relaxed) + size > self.capacity_bytes {
            if entries.is_empty() {
                break;
            }
            // Find the entry with the lowest last_access (approximate LRU)
            let evict_key = entries
                .iter()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| *k);
            if let Some(ek) = evict_key {
                if let Some(removed) = entries.remove(&ek) {
                    self.current_bytes
                        .fetch_sub(removed.size, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }

        // Insert (or update if already present)
        if let Some(existing) = entries.get(&key) {
            // Already inserted by another thread — just return it
            return Arc::clone(&existing.data);
        }

        self.current_bytes.fetch_add(size, Ordering::Relaxed);
        entries.insert(
            key,
            CacheEntry {
                data: Arc::clone(&data),
                size,
                last_access: access,
            },
        );

        data
    }

    /// Remove all cached blocks for a given file.
    ///
    /// Called after compaction deletes a segment file, to free cache
    /// space occupied by blocks from the deleted segment.
    pub fn invalidate_file(&self, file_id: u64) {
        let mut entries = self.entries.lock();
        entries.retain(|k, v| {
            if k.file_id == file_id {
                self.current_bytes.fetch_sub(v.size, Ordering::Relaxed);
                false
            } else {
                true
            }
        });
    }

    /// Get cache statistics.
    pub fn stats(&self) -> BlockCacheStats {
        let entries = self.entries.lock();
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: entries.len(),
            size_bytes: self.current_bytes.load(Ordering::Relaxed),
            capacity_bytes: self.capacity_bytes,
        }
    }
}

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
        // 100 bytes capacity
        let cache = BlockCache::new(100);

        // Insert 3 blocks of 40 bytes each — 3rd should evict the 1st
        cache.insert(1, 0, vec![0; 40]);
        cache.insert(1, 100, vec![1; 40]);
        assert_eq!(cache.stats().entries, 2);

        cache.insert(1, 200, vec![2; 40]);
        // Should have evicted one to make room
        assert!(cache.stats().size_bytes <= 100);
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
}
