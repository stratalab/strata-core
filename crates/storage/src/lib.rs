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

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod block_cache;
pub mod bloom;
pub mod compaction;
pub mod index;
pub mod key_encoding;
pub mod manifest;
pub mod memory_stats;
pub mod memtable;
pub mod merge_iter;
pub mod pressure;
pub mod segment;
pub mod segment_builder;
pub mod segmented;
pub mod stored_value;
pub mod ttl;

pub use bloom::BloomFilter;
pub use compaction::{CompactionIterator, CompactionScheduler, TierMergeCandidate};
pub use index::{BranchIndex, TypeIndex};
pub use memory_stats::{BranchMemoryStats, StorageMemoryStats};
pub use pressure::{MemoryPressure, PressureLevel};
pub use segment::KVSegment;
pub use segment_builder::{SegmentBuilder, SegmentMeta};
pub use segmented::{CompactionResult, SegmentedStore};
pub use ttl::TTLIndex;
