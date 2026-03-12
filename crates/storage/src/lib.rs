//! Storage layer for Strata
//!
//! This crate provides pure in-memory data structures. No file I/O.
//!
//! - ShardedStore: DashMap + HashMap with MVCC version chains
//! - Lock-free reads via DashMap
//! - Per-BranchId sharding (no cross-branch contention)
//! - FxHashMap for O(1) lookups
//!
//! Persistence and durability are handled by the `strata-durability` crate.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod index;
pub mod sharded;
pub mod stored_value;
pub mod ttl;

pub use index::{BranchIndex, TypeIndex};
pub use sharded::{BranchMemoryStats, Shard, ShardedStore, StorageMemoryStats};
pub use ttl::TTLIndex;
