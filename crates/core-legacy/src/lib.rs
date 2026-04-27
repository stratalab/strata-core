//! Shared Strata surface for frequently used data types and traits.
//!
//! This crate exposes a broad convenience import path for IDs, contracts,
//! values, errors, storage layout types, and primitive data types used across
//! the workspace.

// Module declarations
pub mod branch; // Branch lineage and lifecycle types
pub mod branch_dag; // Branch DAG types and constants
pub mod branch_types; // Branch lifecycle types
pub mod contract; // contract types
pub mod error;
pub mod id;
pub mod instrumentation; // Performance tracing (feature-gated)
pub mod limits; // Size limits for keys, values, and vectors
pub mod primitives; // Primitive data types (Event, Vector, JSON)
pub mod traits;
pub mod types;
pub mod value;

// Re-export branch lineage and lifecycle types at crate root.
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
pub use value::{extractable_text, LegacyValueExt, Value, ValueTextExt};

// Re-export contract types at crate root for convenience
pub use contract::{
    BranchName, BranchNameError, EntityRef, PrimitiveType, Timestamp, Version, Versioned,
    VersionedHistory, VersionedValue, MAX_BRANCH_NAME_LENGTH,
};

// Re-export primitive data types at crate root for convenience.
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
mod surface_tests {
    #[test]
    fn foundational_root_exports_behave_consistently() {
        let branch_id = crate::BranchId::from_user_name("surface-check");
        let entity = crate::EntityRef::json(branch_id, "profiles", "user-1");
        let versioned = crate::Versioned::new(
            crate::Value::array(vec![crate::Value::Int(1), crate::Value::String("a".into())]),
            crate::Version::txn(7),
        );

        assert_eq!(entity.branch_id(), branch_id);
        assert_eq!(entity.space(), Some("profiles"));
        assert_eq!(entity.json_doc_id(), Some("user-1"));
        assert_eq!(versioned.version(), crate::Version::txn(7));
        assert_eq!(
            versioned.value().as_array().unwrap(),
            &[crate::Value::Int(1), crate::Value::String("a".into())]
        );
    }

    #[test]
    fn value_surface_retains_extractable_text() {
        use crate::ValueTextExt;

        let value = crate::Value::String("hello".to_string());
        assert_eq!(value.extractable_text(), Some("hello".to_string()));
        assert_eq!(crate::extractable_text(&value), Some("hello".to_string()));
    }
}
