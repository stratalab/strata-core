//! Storage layer for Strata
//!
//! This crate provides in-memory data structures and on-disk segment files.
//!
//! - ShardedStore: DashMap + HashMap with MVCC version chains
//! - Memtable: concurrent skiplist write buffer (ordered by InternalKey)
//! - KVSegment: immutable mmap'd sorted segment files with bloom filters
//! - Lock-free reads via DashMap and SkipMap
//! - Per-BranchId sharding (no cross-branch contention)
//!
//! Persistence and durability are handled by the `strata-durability` crate.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod bloom;
pub mod index;
pub mod key_encoding;
pub mod memtable;
pub mod merge_iter;
pub mod segment;
pub mod segment_builder;
pub mod segmented;
pub mod sharded;
pub mod stored_value;
pub mod ttl;

pub use bloom::BloomFilter;
pub use index::{BranchIndex, TypeIndex};
pub use segment::KVSegment;
pub use segment_builder::{SegmentBuilder, SegmentMeta};
pub use segmented::SegmentedStore;
pub use sharded::{BranchMemoryStats, Shard, ShardedStore, StorageMemoryStats};
pub use ttl::TTLIndex;
