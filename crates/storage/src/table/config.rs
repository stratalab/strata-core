//! Table-runtime configuration shells.

use super::{TableRuntimeError, TableRuntimeResult};
use crate::format::TableCompression;

// B2 (2026-07-08): 16 KiB replaced 64 KiB after the block-size sweep at 10M —
// C run +22% (median of 3, interleaved), read p99 434 -> 296us (and stable),
// B +20-34%, A parity, miss IO 6.5GB -> 2.1GB per 500K reads. 8 KiB read even
// less IO but showed erratic tails; per-database override via the open
// options' data-block byte target.
const DEFAULT_TARGET_DATA_BLOCK_SIZE: u32 = 16 * 1024;
const DEFAULT_ROWS_PER_BLOCK: usize = 256;
const DEFAULT_CACHE_CAPACITY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_COMPACTION_TARGET_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_COMPACTION_MAX_OUTPUT_TABLES: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableRuntimeConfig {
    builder: TableBuilderConfig,
    reader: TableReaderConfig,
    cache: TableCacheConfig,
    compaction: TableCompactionConfig,
}

impl TableRuntimeConfig {
    pub(crate) fn new(
        builder: TableBuilderConfig,
        reader: TableReaderConfig,
        cache: TableCacheConfig,
        compaction: TableCompactionConfig,
    ) -> TableRuntimeResult<Self> {
        builder.validate()?;
        cache.validate()?;
        compaction.validate()?;
        Ok(Self {
            builder,
            reader,
            cache,
            compaction,
        })
    }

    pub(crate) const fn builder(&self) -> &TableBuilderConfig {
        &self.builder
    }

    pub(crate) const fn reader(&self) -> &TableReaderConfig {
        &self.reader
    }

    pub(crate) const fn cache(&self) -> &TableCacheConfig {
        &self.cache
    }

    pub(crate) const fn compaction(&self) -> &TableCompactionConfig {
        &self.compaction
    }
}

impl Default for TableRuntimeConfig {
    fn default() -> Self {
        Self::new(
            TableBuilderConfig::default(),
            TableReaderConfig::default(),
            TableCacheConfig::default(),
            TableCompactionConfig::default(),
        )
        .expect("default table runtime configuration is valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableBuilderConfig {
    target_data_block_size: u32,
    rows_per_block: usize,
    compression: TableCompression,
    // BS4.3: `Some(bits_per_key)` makes the builder compute + persist a bloom filter frame; `None`
    // (the default) writes a zero-slot, unfiltered table. Config-gated and off by default until a
    // later slice flips it on for production.
    filter_bits_per_key: Option<usize>,
}

impl TableBuilderConfig {
    pub(crate) fn new(
        target_data_block_size: u32,
        rows_per_block: usize,
        compression: TableCompression,
    ) -> TableRuntimeResult<Self> {
        let config = Self {
            target_data_block_size,
            rows_per_block,
            compression,
            filter_bits_per_key: None,
        };
        config.validate()?;
        Ok(config)
    }

    /// BS4.3: enable (or disable) writing a persisted bloom filter frame. `Some(bits_per_key)` sizes
    /// the bloom; `None` writes an unfiltered table.
    pub(crate) const fn with_filter_bits_per_key(
        mut self,
        filter_bits_per_key: Option<usize>,
    ) -> Self {
        self.filter_bits_per_key = filter_bits_per_key;
        self
    }

    pub(crate) const fn target_data_block_size(self) -> u32 {
        self.target_data_block_size
    }

    pub(crate) const fn rows_per_block(self) -> usize {
        self.rows_per_block
    }

    pub(crate) const fn compression(self) -> TableCompression {
        self.compression
    }

    /// BS4.3: bits-per-key for the persisted bloom filter, or `None` when filter writing is disabled.
    pub(crate) const fn filter_bits_per_key(self) -> Option<usize> {
        self.filter_bits_per_key
    }

    pub(crate) fn validate(self) -> TableRuntimeResult<()> {
        if self.target_data_block_size == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "target_data_block_size",
                reason: "must be nonzero",
            });
        }
        if self.rows_per_block == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "rows_per_block",
                reason: "must be nonzero",
            });
        }
        if self.filter_bits_per_key == Some(0) {
            return Err(TableRuntimeError::InvalidConfig {
                field: "filter_bits_per_key",
                reason: "must be nonzero",
            });
        }
        Ok(())
    }

    /// #3469: every field, as output-identity fingerprint words. The builder
    /// config decides a compaction output's bytes (block cuts, codec, filter
    /// frame), so it is part of the output's content-complete identity. The
    /// exhaustive destructure makes a new field a compile error here rather
    /// than a silent identity hole.
    pub(super) fn output_content_words(self) -> [u64; 5] {
        let Self {
            target_data_block_size,
            rows_per_block,
            compression,
            filter_bits_per_key,
        } = self;
        let compression_word = match compression {
            TableCompression::Uncompressed => 0,
            TableCompression::Zstd => 1,
        };
        [
            u64::from(target_data_block_size),
            rows_per_block as u64,
            compression_word,
            u64::from(filter_bits_per_key.is_some()),
            filter_bits_per_key.map_or(0, |bits| bits as u64),
        ]
    }
}

impl Default for TableBuilderConfig {
    fn default() -> Self {
        Self {
            target_data_block_size: DEFAULT_TARGET_DATA_BLOCK_SIZE,
            rows_per_block: DEFAULT_ROWS_PER_BLOCK,
            compression: TableCompression::Uncompressed,
            filter_bits_per_key: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableReaderConfig {
    validation: TableReaderValidationMode,
    eager_filter: TableReaderEagerFilterMode,
    materialization_policy: TableMaterializationPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableReaderValidationMode {
    ValidateOnOpen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableReaderEagerFilterMode {
    BuildOnOpen,
    Unavailable,
}

/// BS4.4d: whether a lazy (disk-backed) reader may materialize all of its rows at runtime. `DenyRuntime`
/// makes `try_rows`/`into_materialized` refuse (a typed error, debug-asserted first) so an accidental
/// full-table read on the durable path is caught once the constructor flips to lazy (BS4.4f). Eager
/// readers never reach the lazy materialization path, so the policy is irrelevant to them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableMaterializationPolicy {
    Allow,
    DenyRuntime,
}

impl TableReaderConfig {
    pub(crate) const fn new() -> Self {
        Self {
            validation: TableReaderValidationMode::ValidateOnOpen,
            eager_filter: TableReaderEagerFilterMode::BuildOnOpen,
            materialization_policy: TableMaterializationPolicy::Allow,
        }
    }

    pub(crate) const fn validation_mode(self) -> TableReaderValidationMode {
        self.validation
    }

    pub(crate) const fn with_eager_filter_unavailable(mut self) -> Self {
        self.eager_filter = TableReaderEagerFilterMode::Unavailable;
        self
    }

    pub(crate) const fn eager_filter_mode(self) -> TableReaderEagerFilterMode {
        self.eager_filter
    }

    pub(crate) const fn deny_runtime_materialization(mut self) -> Self {
        self.materialization_policy = TableMaterializationPolicy::DenyRuntime;
        self
    }

    pub(crate) const fn materialization_policy(self) -> TableMaterializationPolicy {
        self.materialization_policy
    }
}

impl Default for TableReaderConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableCacheConfig {
    enabled: bool,
    capacity_bytes: usize,
}

impl TableCacheConfig {
    pub(crate) fn new(enabled: bool, capacity_bytes: usize) -> TableRuntimeResult<Self> {
        let config = Self {
            enabled,
            capacity_bytes,
        };
        config.validate()?;
        Ok(config)
    }

    pub(crate) const fn enabled(self) -> bool {
        self.enabled
    }

    pub(crate) const fn capacity_bytes(self) -> usize {
        self.capacity_bytes
    }

    pub(crate) fn validate(self) -> TableRuntimeResult<()> {
        if self.enabled && self.capacity_bytes == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "cache_capacity_bytes",
                reason: "enabled cache must have positive capacity",
            });
        }
        Ok(())
    }
}

impl Default for TableCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity_bytes: DEFAULT_CACHE_CAPACITY_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactionConfig {
    target_output_bytes: u64,
    max_output_tables: usize,
}

impl TableCompactionConfig {
    pub(crate) fn new(
        target_output_bytes: u64,
        max_output_tables: usize,
    ) -> TableRuntimeResult<Self> {
        let config = Self {
            target_output_bytes,
            max_output_tables,
        };
        config.validate()?;
        Ok(config)
    }

    pub(crate) const fn target_output_bytes(self) -> u64 {
        self.target_output_bytes
    }

    pub(crate) const fn max_output_tables(self) -> usize {
        self.max_output_tables
    }

    pub(crate) fn validate(self) -> TableRuntimeResult<()> {
        if self.target_output_bytes == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "target_output_bytes",
                reason: "must be nonzero",
            });
        }
        if self.max_output_tables == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "max_output_tables",
                reason: "must be nonzero",
            });
        }
        Ok(())
    }

    /// #3469: every field, as output-identity fingerprint words (the output
    /// size target decides where outputs split; see
    /// [`TableBuilderConfig::output_content_words`]).
    pub(super) fn output_content_words(self) -> [u64; 2] {
        let Self {
            target_output_bytes,
            max_output_tables,
        } = self;
        [target_output_bytes, max_output_tables as u64]
    }
}

impl Default for TableCompactionConfig {
    fn default() -> Self {
        Self {
            target_output_bytes: DEFAULT_COMPACTION_TARGET_OUTPUT_BYTES,
            max_output_tables: DEFAULT_COMPACTION_MAX_OUTPUT_TABLES,
        }
    }
}
