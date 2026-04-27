//! Legacy core compatibility surface for Strata.
//!
//! This crate preserves older import paths while lower-layer ownership is
//! being moved to their long-term homes. New code should prefer the narrower
//! owning crates instead of treating this crate as the canonical boundary.
//!
//! It currently exposes:
//! - BranchId: Unique identifier for agent branches
//! - Compatibility storage layout types (`Namespace`, `Key`, `TypeTag`)
//! - Compatibility storage traits (`Storage`, `WriteMode`)
//! - Value: Unified value enum for all data types
//! - Error: Error type hierarchy
//! - Primitive types: Event, JSON, Vector types (in `primitives` module)
//! - Contract types: EntityRef, Versioned<T>, Version, Timestamp, PrimitiveType, BranchName

// Module declarations
pub mod branch; // Canonical branch-truth types (B2)
pub mod branch_dag; // Branch DAG types and constants
pub mod branch_types; // Branch lifecycle types
pub mod contract; // contract types
pub mod error;
pub mod id;
pub mod instrumentation; // Performance tracing (feature-gated)
pub mod limits; // Size limits for keys, values, and vectors
pub mod primitives; // primitive types (Event, Vector, JSON types)
pub mod traits;
pub mod types;
pub mod value;

// Re-export canonical branch-truth types at crate root.
pub use branch::{
    BranchControlRecord, BranchGeneration, BranchLifecycleStatus, BranchRef, ForkAnchor,
};

// Re-export commonly used types and traits
pub use branch_types::{BranchEventOffsets, BranchMetadata, BranchStatus};
pub use error::{
    ConstraintReason, DetailValue, ErrorCode, ErrorDetails, PrimitiveDegradedReason, StrataError,
    StrataResult,
};
pub use id::{CommitVersion, TxnId};
pub use limits::{LimitError, Limits};
pub use traits::{Storage, WriteMode};
pub use types::{validate_space_name, BranchId, Key, Namespace, TypeTag};
pub use value::{extractable_text, LegacyValueExt, Value};

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

#[cfg(test)]
mod forwarding_tests {
    use std::any::TypeId;

    #[test]
    fn foundational_ids_and_contract_dtos_are_forwarded_from_new_core() {
        assert_eq!(
            TypeId::of::<crate::BranchId>(),
            TypeId::of::<strata_core_foundation::BranchId>()
        );
        assert_eq!(
            TypeId::of::<crate::CommitVersion>(),
            TypeId::of::<strata_core_foundation::CommitVersion>()
        );
        assert_eq!(
            TypeId::of::<crate::TxnId>(),
            TypeId::of::<strata_core_foundation::TxnId>()
        );
        assert_eq!(
            TypeId::of::<crate::EntityRef>(),
            TypeId::of::<strata_core_foundation::EntityRef>()
        );
        assert_eq!(
            TypeId::of::<crate::Value>(),
            TypeId::of::<strata_core_foundation::Value>()
        );
    }

    #[test]
    fn legacy_value_surface_retains_extractable_text() {
        use crate::LegacyValueExt;

        let value = crate::Value::String("hello".to_string());
        assert_eq!(value.extractable_text(), Some("hello".to_string()));
        assert_eq!(crate::extractable_text(&value), Some("hello".to_string()));
    }
}
