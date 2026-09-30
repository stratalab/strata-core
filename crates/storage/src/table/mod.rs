//! Table runtime, builders, readers, and cursors.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "table runtime scaffolding is consumed by later table-runtime work"
    )
)]

mod builder;
mod cache;
mod compaction;
mod config;
mod cursor;
mod error;
mod facts;
mod key;
mod mutable;
mod reader;

#[cfg_attr(
    all(not(test), not(feature = "testkit")),
    allow(
        unused_imports,
        reason = "immutable table builder surfaces are consumed by later table-runtime work"
    )
)]
pub(crate) use builder::{
    BuiltTableArtifact, ImmutableTableBuilder, ImmutableTableStreamingBuilder,
};
#[cfg_attr(
    all(not(test), not(feature = "testkit")),
    expect(
        unused_imports,
        reason = "table cache and accelerator surfaces are consumed by later table-runtime work"
    )
)]
pub(crate) use cache::{
    CacheInsert, TableBlockAddress, TableBlockCache, TableBlockCacheKey, TableBlockCacheKind,
    TableBlockCacheStats, TableBloomFilter, TableBloomProbe, TableCacheTableId,
};
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "generic table compaction surfaces are consumed by later table-runtime work"
    )
)]
pub(crate) use compaction::{
    CompactionCutBoundary, CompactionOutputCutHints, OutputContentHasher, TableCompactionDecision,
    TableCompactionDropReason, TableCompactionDropSummary, TableCompactionInput,
    TableCompactionMergeCursor, TableCompactionOutput, TableCompactionPolicy,
    TableCompactionReport, TableCompactionRowContext, TableCompactionSource,
    TableCompactionSourceId, TableCompactor, TableStreamingArtifactBuilder,
    TableStreamingArtifactReport,
};
#[cfg_attr(
    all(not(test), not(feature = "testkit")),
    allow(
        unused_imports,
        reason = "table scaffold exports define the local surface for later slices"
    )
)]
pub(crate) use config::{
    TableBuilderConfig, TableCacheConfig, TableCompactionConfig, TableMaterializationPolicy,
    TableReaderConfig, TableReaderEagerFilterMode, TableReaderValidationMode, TableRuntimeConfig,
};
#[cfg_attr(
    all(not(test), not(feature = "testkit")),
    expect(
        unused_imports,
        reason = "raw cursor surfaces are consumed by later table-runtime work"
    )
)]
pub(crate) use cursor::{
    BoundedTableCursor, CursorMergePath, MemoryTableCursor, MergeTableCursor, TableCursor,
    MERGE_HEAP_THRESHOLD,
};
pub(crate) use error::{TableRuntimeError, TableRuntimeResult};
pub(crate) use facts::{
    TableCommitRange, TableIdentity, TableKeyRange, TableRuntimeFacts, TableSummaryExtras,
};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "full row-key helper surface is consumed by later table-runtime work"
    )
)]
pub(crate) use key::{
    first_table_key, last_table_key, validate_strictly_sorted_unique_keys, TableKeyBound,
};
#[cfg_attr(
    all(not(test), not(feature = "testkit")),
    expect(
        unused_imports,
        reason = "table row-key adapters define the local surface for later slices"
    )
)]
pub(crate) use key::{
    sort_table_rows_by_key, validate_strictly_sorted_unique_rows, TableInternalKeyBytes,
    TableKeyBounds, TablePhysicalKeyBound, TablePhysicalKeyBytes, TablePreparedPointLookup,
    TableRow,
};
#[cfg_attr(
    all(not(test), not(feature = "testkit")),
    expect(
        unused_imports,
        reason = "mutable and frozen table surfaces are consumed by later table-runtime work"
    )
)]
pub(crate) use mutable::{FrozenTable, MutableTable, MutableTableAppendBaseline, TableMemoryFacts};
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "immutable table reader surfaces are consumed by later table-runtime work"
    )
)]
pub(crate) use reader::{
    BytesTableSource, ImmutableTableReader, TableByteSource, TablePointLookupRow,
    TableReaderFilter, TableReaderOpenMode,
};

#[cfg(test)]
mod tests;
