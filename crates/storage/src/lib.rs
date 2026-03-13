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
pub mod primitive_ext;
pub mod registry;
pub mod sharded;
pub mod stored_value;
pub mod ttl;

pub use index::{BranchIndex, TypeIndex};
pub use primitive_ext::{
    is_future_wal_type, is_vector_wal_type, primitive_for_wal_type, primitive_type_ids, wal_ranges,
    PrimitiveExtError, PrimitiveStorageExt,
};
pub use registry::PrimitiveRegistry;
pub use sharded::{BranchMemoryStats, Shard, ShardedStore, StorageMemoryStats};
pub use ttl::TTLIndex;
