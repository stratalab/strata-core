//! Core types and traits for Strata
//!
//! This crate defines the foundational types used throughout the system:
//! - BranchId: Unique identifier for agent branches
//! - Namespace: Branch and space isolation (branch_id/space)
//! - Key: Composite key with type tagging
//! - TypeTag: Discriminates between primitive types
//! - Value: Unified value enum for all data types
//! - Error: Error type hierarchy
//! - Traits: Core trait definitions (Storage)
//! - Primitive types: Event, JSON, Vector types (in `primitives` module)
//! - Contract types: EntityRef, Versioned<T>, Version, Timestamp, PrimitiveType, BranchName

// Module declarations
pub mod branch_dag; // Branch DAG types and constants
pub mod branch_types; // Branch lifecycle types
pub mod contract; // contract types
pub mod error;
pub mod instrumentation; // Performance tracing (feature-gated)
pub mod limits; // Size limits for keys, values, and vectors
pub mod primitives; // primitive types (Event, Vector, JSON types)
pub mod traits;
pub mod types;
pub mod value;

// Re-export commonly used types and traits
pub use branch_types::{BranchEventOffsets, BranchMetadata, BranchStatus};
pub use error::{
    ConstraintReason, DetailValue, ErrorCode, ErrorDetails, StrataError, StrataResult,
};
pub use limits::{LimitError, Limits};
pub use traits::{Storage, WriteMode};
pub use types::{validate_space_name, BranchId, Key, Namespace, TypeTag};
pub use value::Value;

// Re-export contract types at crate root for convenience
pub use contract::{
    BranchName, BranchNameError, EntityRef, PrimitiveType, Timestamp, Version, Versioned,
    VersionedHistory, VersionedValue, MAX_BRANCH_NAME_LENGTH,
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
