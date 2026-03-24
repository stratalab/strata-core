//! Primitive types for Strata
//!
//! This module defines the canonical data structures for all primitives.
//! These types are shared between the `engine` and `primitives` crates.
//!
//! ## Design Principle
//!
//! - **strata-core** defines canonical semantic types (this module)
//! - **strata-primitives** provides stateless facades and implementation logic
//! - **strata-engine** orchestrates transactions and recovery
//!
//! All crates share the same type definitions from core.

pub mod event;
pub mod json;
pub mod vector;

// Re-export all types at module level
pub use event::{ChainVerification, Event};
pub use json::{
    apply_patches, delete_at_path, get_at_path, get_at_path_mut, merge_patch, set_at_path,
    JsonLimitError, JsonPatch, JsonPath, JsonPathError, JsonValue, PathParseError, PathSegment,
    MAX_ARRAY_SIZE, MAX_DOCUMENT_SIZE, MAX_NESTING_DEPTH, MAX_PATH_LENGTH,
};
pub use vector::{
    CollectionId, CollectionInfo, DistanceMetric, FilterCondition, FilterOp, JsonScalar,
    MetadataFilter, StorageDtype, VectorConfig, VectorEntry, VectorId, VectorMatch,
};
