//! Vector primitive types and operations
//!
//! This module provides vector storage and similarity search capabilities.
//! It includes:
//!
//! - **VectorStore**: Main facade for vector storage and search
//! - **VectorConfig**: Collection configuration (dimension, metric, storage type)
//! - **DistanceMetric**: Similarity metrics (Cosine, Euclidean, DotProduct)
//! - **VectorEntry/Match**: Vector storage and search result types
//! - **VectorHeap**: Contiguous embedding storage with slot reuse
//! - **VectorIndexBackend**: Trait for swappable index implementations
//! - **BruteForceBackend**: O(n) brute-force search
//! - **MetadataFilter**: Equality-based metadata filtering
//! - **VectorError**: Error types for vector operations
//!
//! ## Recovery
//!
//! VectorStore participates in Database recovery via `VectorSubsystem`.
//! Product database opens compose the vector subsystem internally through
//! `open_product_database()` and `open_product_cache()`. Direct
//! `OpenSpec::with_subsystem(VectorSubsystem)` registration remains available
//! for engine internals, tests, and specialized low-level runtimes.

#![allow(missing_docs)]

pub mod backend;
pub mod brute_force;
pub mod collection;
pub mod distance;
pub mod error;
pub(crate) mod ext;
pub mod filter;
pub mod heap;
pub mod hnsw;
pub mod merge_handler;
pub(crate) mod mmap;
pub(crate) mod mmap_graph;
pub mod quantize;
pub mod recovery;
pub mod segmented;
pub mod store;
pub mod types;

pub use backend::{
    IndexBackendFactory, InlineMetaCapable, MmapCapable, SegmentCapable, VectorIndexBackend,
};
pub use brute_force::BruteForceBackend;
pub use collection::{
    validate_collection_name, validate_system_collection_name, validate_vector_key,
};
pub use error::{VectorError, VectorResult};
pub use filter::{FilterCondition, FilterOp, JsonScalar, MetadataFilter};
pub use heap::VectorHeap;
pub use hnsw::{HnswBackend, HnswConfig};
pub use quantize::{QuantizationParams, RaBitQParams};
pub use recovery::VectorSubsystem;
pub use segmented::{SegmentedHnswBackend, SegmentedHnswConfig};
pub use store::{RecoveryStats, VectorBackendState, VectorStore};
pub use types::{
    CollectionId, CollectionInfo, CollectionRecord, DistanceMetric, IndexBackendType,
    SearchOptions, StorageDtype, VectorConfig, VectorConfigSerde, VectorEntry, VectorId,
    VectorMatch, VectorMatchWithSource, VectorRecord,
};

/// Compute the directory for sealed-segment graph mmap files.
///
/// Layout: `{data_dir}/vectors/{branch_hex}/{space}/{collection_name}_graphs/`.
/// Including `space` as a subdirectory ensures two collections with the
/// same `(branch_id, name)` in different spaces never share graph caches.
/// Space names are validated to `[a-z0-9_-]` so they are filesystem-safe.
pub(crate) fn graph_dir(
    data_dir: &std::path::Path,
    branch_id: strata_core::BranchId,
    space: &str,
    collection_name: &str,
) -> std::path::PathBuf {
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    data_dir
        .join("vectors")
        .join(branch_hex)
        .join(space)
        .join(format!("{}_graphs", collection_name))
}
