//! Core types and traits for Strata
//!
//! This crate defines the foundational types used throughout the system:
//! - BranchId: Unique identifier for agent branches
//! - Namespace: Hierarchical namespace (tenant/app/agent/branch)
//! - Key: Composite key with type tagging
//! - TypeTag: Discriminates between primitive types
//! - Value: Unified value enum for all data types
//! - Error: Error type hierarchy
//! - Traits: Core trait definitions (Storage, SnapshotView)
//! - Primitive types: Event, State, JSON, Vector types (in `primitives` module)
//! - Contract types: EntityRef, Versioned<T>, Version, Timestamp, PrimitiveType, BranchName

#![warn(missing_docs)]
#![warn(clippy::all)]

// Module declarations
pub mod branch_types; // Branch lifecycle types
pub mod contract; // contract types
pub mod error;
pub mod limits; // Size limits for keys, values, and vectors
pub mod primitive_ext; // extension trait for primitives to integrate with storage/durability
pub mod primitives; // primitive types (Event, State, Vector, JSON types)
pub mod search_types; // search types (EntityRef/PrimitiveType re-exports only; types moved to engine)
pub mod traits;
pub mod types;
pub mod value;

// Re-export commonly used types and traits
pub use branch_types::{BranchEventOffsets, BranchMetadata, BranchStatus};
pub use error::{
    ConstraintReason, DetailValue, ErrorCode, ErrorDetails, StrataError, StrataResult,
};
pub use limits::{LimitError, Limits};
pub use traits::{SnapshotView, Storage, WriteMode};
pub use types::{validate_space_name, BranchId, Key, Namespace, TypeTag};
pub use value::Value;

// Re-export contract types at crate root for convenience
pub use contract::{
    BranchName, BranchNameError, EntityRef, PrimitiveType, Timestamp, Version, Versioned,
    VersionedHistory, VersionedValue, MAX_BRANCH_NAME_LENGTH,
};

// Re-export primitive extension trait and helpers
pub use primitive_ext::{
    is_future_wal_type, is_vector_wal_type, primitive_for_wal_type, primitive_type_ids, wal_ranges,
    PrimitiveExtError, PrimitiveStorageExt,
};

// Re-export primitive types at crate root for convenience
pub use primitives::{
    // JSON types
    apply_patches,
    delete_at_path,
    get_at_path,
    get_at_path_mut,
    merge_patch,
    set_at_path,
    // Event types
    ChainVerification,
    // Vector types
    CollectionId,
    CollectionInfo,
    DistanceMetric,
    Event,
    JsonLimitError,
    JsonPatch,
    JsonPath,
    JsonPathError,
    JsonScalar,
    JsonValue,
    MetadataFilter,
    PathParseError,
    PathSegment,
    // State types
    State,
    StorageDtype,
    VectorConfig,
    VectorEntry,
    VectorId,
    VectorMatch,
    MAX_ARRAY_SIZE,
    MAX_DOCUMENT_SIZE,
    MAX_NESTING_DEPTH,
    MAX_PATH_LENGTH,
};
