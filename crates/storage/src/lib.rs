//! Storage layer for Strata
//!
//! This crate provides in-memory data structures and on-disk segment files.
//!
//! - SegmentedStore: memtable + immutable segments with MVCC
//! - Memtable: concurrent skiplist write buffer (ordered by InternalKey)
//! - KVSegment: immutable sorted segment files with pread I/O + block cache
//! - Lock-free reads via SkipMap
//! - Per-BranchId sharding (no cross-branch contention)
//!
//! Persistence and durability are owned by the storage durability subtree.

pub mod block_cache;
pub mod bloom;
pub mod compaction;
pub mod durability;
/// Storage-local error types used by publication barriers and manifest I/O.
pub mod error;
pub mod index;
pub mod key_encoding;
pub mod layout;
pub mod manifest;
pub mod memory_stats;
pub mod memtable;
pub mod merge_iter;
pub mod pressure;
pub mod quarantine;
pub mod rate_limiter;
pub mod runtime_config;
pub mod seekable;
pub mod segment;
pub mod segment_builder;
pub mod segmented;
pub mod stored_value;
mod test_hooks;
pub mod traits;
pub mod ttl;
pub mod txn;

pub use bloom::BloomFilter;
pub use compaction::{CompactionIterator, CompactionScheduler, TierMergeCandidate};
pub use error::{BranchOp, StorageError, StorageResult};
pub use index::{BranchIndex, TypeIndex};
pub use layout::{validate_space_name, Key, Namespace, TypeTag};
pub use memory_stats::{BranchMemoryStats, StorageMemoryStats};
pub use pressure::{MemoryPressure, PressureLevel};
pub use quarantine::{
    read_quarantine_manifest, write_quarantine_manifest, QuarantineEntry, QuarantineManifest,
    QUARANTINE_DIR, QUARANTINE_FILENAME,
};
pub use rate_limiter::RateLimiter;
pub use runtime_config::{StorageBlockCacheConfig, StorageRuntimeConfig};
pub use segment::{KVSegment, OwnedSegmentIter};
pub use segment_builder::{CompressionCodec, SegmentBuilder, SegmentMeta, SplittingSegmentBuilder};
pub use segmented::{
    CompactionResult, DecodedSnapshotEntry, DecodedSnapshotValue, DegradationClass,
    MaterializeResult, PickAndCompactResult, PublishHealth, PurgeReport, RecoveredState,
    RecoveryFault, RecoveryHealth, SegmentedStore, StorageBranchRetention,
    StorageInheritedLayerInfo, StorageIterator, VersionedEntry,
};
pub use traits::{Storage, WriteMode};
pub use ttl::TTLIndex;
pub use txn::{
    validate_cas_set, validate_read_set, validate_transaction, ApplyResult, CASOperation,
    CommitError, ConflictType, PendingOperations, TransactionContext, TransactionManager,
    TransactionStatus, ValidationResult,
};
