//! Storage runtime configuration mechanics.
//!
//! This module owns storage-local runtime knobs and their application to
//! `SegmentedStore`. Higher layers may adapt public/product configuration into
//! this type, but storage remains the owner of the setter list and storage
//! defaults represented here.

use crate::segmented::SegmentedStore;

/// Storage-owned block-cache runtime configuration.
///
/// Public/product config may still use `0` as its compatibility spelling for
/// automatic sizing. That sentinel is converted at the storage runtime boundary
/// so storage no longer carries an ambiguous raw integer for global cache
/// application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageBlockCacheConfig {
    /// Resolve capacity from storage's host-memory auto-detection.
    Auto,
    /// Set the global block cache to an explicit byte capacity.
    ///
    /// `Bytes(0)` is allowed and means an explicit zero-capacity cache. This is
    /// distinct from [`Self::Auto`] and prevents small derived memory budgets
    /// from accidentally expanding into host-level auto-detection.
    Bytes(usize),
}

impl StorageBlockCacheConfig {
    /// Interpret a raw public block-cache byte value.
    ///
    /// The public compatibility convention is `0 == auto`; callers that derive
    /// an explicit byte capacity should use [`Self::bytes`] instead.
    pub const fn from_raw_size(bytes: usize) -> Self {
        if bytes == 0 {
            Self::Auto
        } else {
            Self::Bytes(bytes)
        }
    }

    /// Build an explicit byte-capacity config.
    pub const fn bytes(bytes: usize) -> Self {
        Self::Bytes(bytes)
    }

    /// Return the compatibility byte value; `0` represents auto-detect.
    pub const fn configured_bytes(self) -> usize {
        match self {
            Self::Auto => 0,
            Self::Bytes(bytes) => bytes,
        }
    }

    /// Resolve the capacity to apply to the global block cache.
    pub fn resolved_capacity(self) -> usize {
        match self {
            Self::Auto => crate::block_cache::auto_detect_capacity(),
            Self::Bytes(bytes) => bytes,
        }
    }

    /// Whether this config requests storage auto-detection.
    pub const fn is_auto(self) -> bool {
        matches!(self, Self::Auto)
    }
}

impl Default for StorageBlockCacheConfig {
    fn default() -> Self {
        Self::Auto
    }
}

/// Runtime storage knobs applied to a `SegmentedStore`.
///
/// This mirrors storage setter types rather than higher-layer product defaults.
/// Higher layers remain responsible for building this from their public
/// configuration until the public configuration split is complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageRuntimeConfig {
    /// Global block-cache runtime sizing.
    pub block_cache: StorageBlockCacheConfig,
    /// Effective memtable write buffer size in bytes.
    ///
    /// `apply_to_store` applies this value only when the builder received an
    /// explicit write-buffer input or derived one from `memory_budget`. Default
    /// runtime configs preserve the constructor-provided write-buffer size.
    pub write_buffer_size: usize,
    /// Advisory branch-limit value stored on the store; `0` means unlimited.
    ///
    /// Current storage mechanics record and expose this value, but runtime
    /// config application does not introduce new branch-creation enforcement.
    pub max_branches: usize,
    /// Maximum retained versions per key; `0` means unlimited.
    pub max_versions_per_key: usize,
    /// Maximum frozen memtables per branch before rotation is skipped.
    pub max_immutable_memtables: usize,
    /// Target size for a single segment file in bytes.
    pub target_file_size: u64,
    /// Target total size for L1 in bytes.
    pub level_base_bytes: u64,
    /// Data block size for newly built segment files.
    pub data_block_size: usize,
    /// Bloom filter bits per key for newly built segment files.
    pub bloom_bits_per_key: usize,
    /// Compaction I/O rate limit in bytes per second; `0` means unlimited.
    pub compaction_rate_limit: u64,
    apply_write_buffer_size: bool,
}

impl StorageRuntimeConfig {
    /// Default block cache runtime configuration.
    pub const DEFAULT_BLOCK_CACHE: StorageBlockCacheConfig = StorageBlockCacheConfig::Auto;
    /// Default write buffer size for store constructors that do not receive a
    /// public runtime configuration. `SegmentedStore::with_dir` still uses its
    /// explicit constructor argument for write-buffer sizing.
    pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 0;
    /// Default target size for a single segment file in bytes.
    ///
    /// `SegmentedStore` constructors use this value for their initial runtime
    /// configuration.
    pub const DEFAULT_TARGET_FILE_SIZE: u64 = 64 << 20;
    /// Default target total size for L1 in bytes.
    ///
    /// `SegmentedStore` constructors use this value for their initial runtime
    /// configuration.
    pub const DEFAULT_LEVEL_BASE_BYTES: u64 = 256 << 20;
    /// Default segment data block size in bytes.
    ///
    /// `SegmentedStore` constructors use this value for their initial runtime
    /// configuration.
    pub const DEFAULT_DATA_BLOCK_SIZE: usize = 4096;
    /// Default bloom filter bits per key.
    ///
    /// `SegmentedStore` constructors use this value for their initial runtime
    /// configuration.
    pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;

    /// Start a builder for storage runtime config from raw storage-layer inputs.
    pub fn builder() -> StorageRuntimeConfigBuilder {
        StorageRuntimeConfigBuilder::default()
    }

    /// Apply these storage runtime knobs to `storage`.
    ///
    /// This is the storage-owned application point for static
    /// `SegmentedStore` runtime knobs. Higher layers should adapt their public
    /// config into `StorageRuntimeConfig` and call this helper rather than
    /// carrying their own `SegmentedStore::set_*` list. Default runtime configs
    /// preserve a store's constructor-provided write-buffer size; the
    /// write-buffer setter is applied only when the builder receives
    /// `write_buffer_size` explicitly or derives it from `memory_budget`.
    pub fn apply_to_store(&self, storage: &SegmentedStore) {
        if self.apply_write_buffer_size {
            storage.set_write_buffer_size(self.write_buffer_size);
        }
        storage.set_max_branches(self.max_branches);
        storage.set_max_versions_per_key(self.max_versions_per_key);
        storage.set_max_immutable_memtables(self.max_immutable_memtables);
        storage.set_target_file_size(self.target_file_size);
        storage.set_level_base_bytes(self.level_base_bytes);
        storage.set_data_block_size(self.data_block_size);
        storage.set_bloom_bits_per_key(self.bloom_bits_per_key);
        storage.set_compaction_rate_limit(self.compaction_rate_limit);
    }

    /// Apply process-global storage runtime knobs.
    ///
    /// Store-local knobs belong in [`Self::apply_to_store`]. This helper owns
    /// storage-wide global mechanics such as block-cache capacity. Block-cache
    /// capacity is process-global by design; repeated calls overwrite the
    /// previous capacity when applied sequentially. Concurrent opens race and
    /// have no deterministic winner.
    pub fn apply_global_runtime(&self) {
        crate::block_cache::set_global_capacity(self.resolved_block_cache_capacity());
    }

    /// Resolve the block-cache capacity, applying storage auto-detection when
    /// the runtime config requests automatic sizing.
    pub fn resolved_block_cache_capacity(&self) -> usize {
        self.block_cache.resolved_capacity()
    }

    /// Return the compatibility byte value for the block-cache config.
    ///
    /// This is primarily for legacy public config helpers and logging surfaces
    /// that still report `0` as auto-detect.
    pub const fn block_cache_configured_bytes(&self) -> usize {
        self.block_cache.configured_bytes()
    }
}

impl Default for StorageRuntimeConfig {
    fn default() -> Self {
        Self {
            block_cache: Self::DEFAULT_BLOCK_CACHE,
            write_buffer_size: Self::DEFAULT_WRITE_BUFFER_SIZE,
            max_branches: 0,
            max_versions_per_key: 0,
            max_immutable_memtables: 0,
            target_file_size: Self::DEFAULT_TARGET_FILE_SIZE,
            level_base_bytes: Self::DEFAULT_LEVEL_BASE_BYTES,
            data_block_size: Self::DEFAULT_DATA_BLOCK_SIZE,
            bloom_bits_per_key: Self::DEFAULT_BLOOM_BITS_PER_KEY,
            compaction_rate_limit: 0,
            apply_write_buffer_size: false,
        }
    }
}

/// Builder for storage runtime config from raw storage-facing inputs.
///
/// Higher layers pass public field values through this builder without
/// reimplementing storage-local effective-value derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageRuntimeConfigBuilder {
    memory_budget: usize,
    block_cache_size: usize,
    write_buffer_size: usize,
    // Tracks whether `write_buffer_size` is an explicit apply request instead
    // of the default constructor-preserving placeholder.
    apply_write_buffer_size: bool,
    max_branches: usize,
    max_versions_per_key: usize,
    max_immutable_memtables: usize,
    target_file_size: u64,
    level_base_bytes: u64,
    data_block_size: usize,
    bloom_bits_per_key: usize,
    compaction_rate_limit: u64,
}

impl StorageRuntimeConfigBuilder {
    /// Set a unified storage memory budget in bytes.
    ///
    /// When non-zero, storage derives the effective runtime resource split:
    /// half of the budget becomes block cache, one quarter becomes the active
    /// write buffer, and one immutable memtable is retained so the active plus
    /// frozen write path accounts for the other half.
    pub fn memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget = bytes;
        self
    }

    /// Set the raw block cache size in bytes; `0` means auto-detect.
    pub fn block_cache_size(mut self, bytes: usize) -> Self {
        self.block_cache_size = bytes;
        self
    }

    /// Set the raw write buffer size in bytes.
    ///
    /// Calling this marks the write-buffer size as explicitly configured, so
    /// `apply_to_store` will apply it even when the value is zero.
    pub fn write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size = bytes;
        self.apply_write_buffer_size = true;
        self
    }

    /// Store an advisory branch-limit value; `0` means unlimited.
    ///
    /// Storage runtime config centralizes where the value is stored on
    /// `SegmentedStore`; it does not add branch-creation enforcement.
    pub fn max_branches(mut self, max: usize) -> Self {
        self.max_branches = max;
        self
    }

    /// Set the retained-versions-per-key limit; `0` means unlimited.
    pub fn max_versions_per_key(mut self, max: usize) -> Self {
        self.max_versions_per_key = max;
        self
    }

    /// Set the raw immutable-memtable limit.
    pub fn max_immutable_memtables(mut self, max: usize) -> Self {
        self.max_immutable_memtables = max;
        self
    }

    /// Set the target segment file size in bytes.
    pub fn target_file_size(mut self, bytes: u64) -> Self {
        self.target_file_size = bytes;
        self
    }

    /// Set the target total L1 size in bytes.
    pub fn level_base_bytes(mut self, bytes: u64) -> Self {
        self.level_base_bytes = bytes;
        self
    }

    /// Set the segment data block size in bytes.
    pub fn data_block_size(mut self, bytes: usize) -> Self {
        self.data_block_size = bytes;
        self
    }

    /// Set the bloom filter bits per key.
    pub fn bloom_bits_per_key(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }

    /// Set the compaction I/O rate limit in bytes per second; `0` means unlimited.
    pub fn compaction_rate_limit(mut self, bytes_per_sec: u64) -> Self {
        self.compaction_rate_limit = bytes_per_sec;
        self
    }

    /// Build the effective storage runtime config.
    pub fn build(self) -> StorageRuntimeConfig {
        let (block_cache, write_buffer_size, max_immutable_memtables) = if self.memory_budget > 0 {
            (
                StorageBlockCacheConfig::bytes(self.memory_budget / 2),
                self.memory_budget / 4,
                1,
            )
        } else {
            (
                StorageBlockCacheConfig::from_raw_size(self.block_cache_size),
                self.write_buffer_size,
                self.max_immutable_memtables,
            )
        };
        let apply_write_buffer_size = self.apply_write_buffer_size || self.memory_budget > 0;

        StorageRuntimeConfig {
            block_cache,
            write_buffer_size,
            max_branches: self.max_branches,
            max_versions_per_key: self.max_versions_per_key,
            max_immutable_memtables,
            target_file_size: self.target_file_size,
            level_base_bytes: self.level_base_bytes,
            data_block_size: self.data_block_size,
            bloom_bits_per_key: self.bloom_bits_per_key,
            compaction_rate_limit: self.compaction_rate_limit,
            apply_write_buffer_size,
        }
    }
}

impl Default for StorageRuntimeConfigBuilder {
    fn default() -> Self {
        let defaults = StorageRuntimeConfig::default();
        Self {
            memory_budget: 0,
            block_cache_size: defaults.block_cache_configured_bytes(),
            write_buffer_size: defaults.write_buffer_size,
            apply_write_buffer_size: false,
            max_branches: defaults.max_branches,
            max_versions_per_key: defaults.max_versions_per_key,
            max_immutable_memtables: defaults.max_immutable_memtables,
            target_file_size: defaults.target_file_size,
            level_base_bytes: defaults.level_base_bytes,
            data_block_size: defaults.data_block_size,
            bloom_bits_per_key: defaults.bloom_bits_per_key,
            compaction_rate_limit: defaults.compaction_rate_limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static GLOBAL_BLOCK_CACHE_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct GlobalBlockCacheCapacityGuard {
        previous_capacity: usize,
        _lock: MutexGuard<'static, ()>,
    }

    impl GlobalBlockCacheCapacityGuard {
        fn capture() -> Self {
            let lock = GLOBAL_BLOCK_CACHE_TEST_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Self {
                previous_capacity: crate::block_cache::global_capacity(),
                _lock: lock,
            }
        }
    }

    impl Drop for GlobalBlockCacheCapacityGuard {
        fn drop(&mut self) {
            crate::block_cache::set_global_capacity(self.previous_capacity);
        }
    }

    #[test]
    fn default_matches_segmented_store_defaults() {
        let cfg = StorageRuntimeConfig::default();
        let store = SegmentedStore::new();

        assert_eq!(cfg.block_cache, StorageBlockCacheConfig::Auto);
        assert_eq!(cfg.block_cache_configured_bytes(), 0);
        assert_eq!(cfg.write_buffer_size, 0);
        assert_eq!(cfg.max_branches, 0);
        assert_eq!(cfg.max_versions_per_key, 0);
        assert_eq!(cfg.max_immutable_memtables, 0);
        assert_eq!(cfg.target_file_size, store.target_file_size());
        assert_eq!(cfg.level_base_bytes, store.level_base_bytes());
        assert_eq!(cfg.data_block_size, store.data_block_size());
        assert_eq!(cfg.bloom_bits_per_key, store.bloom_bits_per_key());
        assert_eq!(cfg.compaction_rate_limit, 0);
    }

    #[test]
    fn default_apply_preserves_constructor_write_buffer_size() {
        let cfg = StorageRuntimeConfig::default();
        let store = SegmentedStore::new();
        store.set_write_buffer_size(4 << 20);

        cfg.apply_to_store(&store);

        assert_eq!(store.write_buffer_size_for_test(), 4 << 20);
    }

    #[test]
    fn explicit_zero_write_buffer_disables_rotation() {
        let cfg = StorageRuntimeConfig::builder().write_buffer_size(0).build();
        let store = SegmentedStore::new();
        store.set_write_buffer_size(4 << 20);

        cfg.apply_to_store(&store);

        assert_eq!(store.write_buffer_size_for_test(), 0);
    }

    #[test]
    fn applies_runtime_knobs_to_store() {
        let cfg = StorageRuntimeConfig::builder()
            .write_buffer_size(4 << 20)
            .max_branches(128)
            .max_versions_per_key(16)
            .max_immutable_memtables(2)
            .target_file_size(2 << 20)
            .level_base_bytes(5 << 20)
            .data_block_size(8 << 10)
            .bloom_bits_per_key(12)
            .compaction_rate_limit(1_234_567)
            .build();
        let store = SegmentedStore::new();

        cfg.apply_to_store(&store);

        assert_eq!(store.write_buffer_size_for_test(), 4 << 20);
        assert_eq!(store.max_branches_for_test(), 128);
        assert_eq!(store.max_versions_per_key_for_test(), 16);
        assert_eq!(store.max_immutable_memtables_for_test(), 2);
        assert_eq!(store.target_file_size(), 2 << 20);
        assert_eq!(store.level_base_bytes(), 5 << 20);
        assert_eq!(store.data_block_size(), 8 << 10);
        assert_eq!(store.bloom_bits_per_key(), 12);
        assert_eq!(store.compaction_rate_limit_for_test(), 1_234_567);
    }

    #[test]
    fn builder_uses_individual_values_without_memory_budget() {
        let cfg = StorageRuntimeConfig::builder()
            .block_cache_size(64 << 20)
            .write_buffer_size(32 << 20)
            .max_immutable_memtables(3)
            .build();

        assert_eq!(cfg.block_cache, StorageBlockCacheConfig::Bytes(64 << 20));
        assert_eq!(cfg.block_cache_configured_bytes(), 64 << 20);
        assert_eq!(cfg.write_buffer_size, 32 << 20);
        assert_eq!(cfg.max_immutable_memtables, 3);
    }

    #[test]
    fn builder_maps_raw_zero_block_cache_to_auto_without_memory_budget() {
        let cfg = StorageRuntimeConfig::builder().block_cache_size(0).build();

        assert_eq!(cfg.block_cache, StorageBlockCacheConfig::Auto);
        assert_eq!(cfg.block_cache_configured_bytes(), 0);
    }

    #[test]
    fn apply_global_runtime_resolves_auto_block_cache_capacity() {
        let _capacity_guard = GlobalBlockCacheCapacityGuard::capture();
        let cfg = StorageRuntimeConfig::builder().block_cache_size(0).build();
        crate::block_cache::set_global_capacity(1);

        assert_eq!(cfg.block_cache, StorageBlockCacheConfig::Auto);
        cfg.apply_global_runtime();

        assert_ne!(crate::block_cache::global_capacity(), 1);
        assert!(crate::block_cache::global_capacity() > 0);
    }

    #[test]
    fn builder_derives_effective_values_from_memory_budget() {
        let cfg = StorageRuntimeConfig::builder()
            .memory_budget(32 << 20)
            .block_cache_size(64 << 20)
            .write_buffer_size(32 << 20)
            .max_immutable_memtables(3)
            .build();

        assert_eq!(cfg.block_cache, StorageBlockCacheConfig::Bytes(16 << 20));
        assert_eq!(cfg.block_cache_configured_bytes(), 16 << 20);
        assert_eq!(cfg.write_buffer_size, 8 << 20);
        assert_eq!(cfg.max_immutable_memtables, 1);
        let total = cfg.block_cache_configured_bytes()
            + cfg.write_buffer_size * (1 + cfg.max_immutable_memtables);
        assert_eq!(total, 32 << 20);
    }

    #[test]
    fn memory_budget_zero_byte_derivation_is_explicit_not_auto() {
        let _capacity_guard = GlobalBlockCacheCapacityGuard::capture();
        let cfg = StorageRuntimeConfig::builder().memory_budget(1).build();

        assert_eq!(cfg.block_cache, StorageBlockCacheConfig::Bytes(0));
        cfg.apply_global_runtime();

        assert_eq!(crate::block_cache::global_capacity(), 0);
    }

    #[test]
    fn apply_global_runtime_sets_resolved_block_cache_capacity() {
        let _capacity_guard = GlobalBlockCacheCapacityGuard::capture();
        let cfg = StorageRuntimeConfig::builder()
            .block_cache_size(32 << 20)
            .build();

        cfg.apply_global_runtime();

        assert_eq!(crate::block_cache::global_capacity(), 32 << 20);
    }

    #[test]
    fn apply_global_runtime_sequential_calls_overwrite_process_global_capacity() {
        let _capacity_guard = GlobalBlockCacheCapacityGuard::capture();
        let first = StorageRuntimeConfig::builder()
            .block_cache_size(11 << 20)
            .build();
        let second = StorageRuntimeConfig::builder()
            .block_cache_size(13 << 20)
            .build();

        first.apply_global_runtime();
        assert_eq!(crate::block_cache::global_capacity(), 11 << 20);

        second.apply_global_runtime();
        assert_eq!(crate::block_cache::global_capacity(), 13 << 20);
    }
}
