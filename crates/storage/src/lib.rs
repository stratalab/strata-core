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
//! Persistence and durability are handled by the `strata-durability` crate.

pub mod block_cache;
pub mod bloom;
pub mod compaction;
/// Storage-local error types used by publication barriers and manifest I/O.
pub mod error;
pub mod index;
pub mod key_encoding;
pub mod manifest;
pub mod memory_stats;
pub mod memtable;
pub mod merge_iter;
pub mod pressure;
pub mod rate_limiter;
pub mod seekable;
pub mod segment;
pub mod segment_builder;
pub mod segmented;
pub mod stored_value;
mod test_hooks;
pub mod ttl;

pub use bloom::BloomFilter;
pub use compaction::{CompactionIterator, CompactionScheduler, TierMergeCandidate};
pub use error::{StorageError, StorageResult};
pub use index::{BranchIndex, TypeIndex};
pub use memory_stats::{BranchMemoryStats, StorageMemoryStats};
pub use pressure::{MemoryPressure, PressureLevel};
pub use rate_limiter::RateLimiter;
pub use segment::{KVSegment, OwnedSegmentIter};
pub use segment_builder::{CompressionCodec, SegmentBuilder, SegmentMeta, SplittingSegmentBuilder};
pub use segmented::{
    CompactionResult, DecodedSnapshotEntry, DecodedSnapshotValue, DegradationClass,
    MaterializeResult, PickAndCompactResult, PublishHealth, RecoveredState, RecoveryFault,
    RecoveryHealth, SegmentedStore, StorageIterator, VersionedEntry,
};
pub use ttl::TTLIndex;
