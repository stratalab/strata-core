//! Error types for the Vector primitive

use strata_core::{BranchId, EntityRef, StrataError};
use thiserror::Error;

/// Errors specific to the Vector primitive
#[derive(Debug, Error)]
pub enum VectorError {
    /// Collection with given name was not found
    #[error("Collection not found: {name}")]
    CollectionNotFound {
        /// Collection name
        name: String,
    },

    /// Collection with given name already exists
    #[error("Collection already exists: {name}")]
    CollectionAlreadyExists {
        /// Collection name
        name: String,
    },

    /// Vector dimension doesn't match collection configuration
    #[error("Dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension from collection config
        expected: usize,
        /// Actual dimension of provided vector
        got: usize,
    },

    /// Invalid dimension specified (must be > 0)
    #[error("Invalid dimension: {dimension} (must be > 0)")]
    InvalidDimension {
        /// The invalid dimension value
        dimension: usize,
    },

    /// Vector with given key was not found
    #[error("Vector not found: {key}")]
    VectorNotFound {
        /// Vector key
        key: String,
    },

    /// Embedding vector is empty
    #[error("Empty embedding")]
    EmptyEmbedding,

    /// Embedding contains invalid float values (NaN or Infinity)
    #[error("Invalid embedding: {reason}")]
    InvalidEmbedding {
        /// Reason the embedding is invalid
        reason: String,
    },

    /// Collection name is invalid
    #[error("Invalid collection name: {name} ({reason})")]
    InvalidCollectionName {
        /// The invalid name
        name: String,
        /// Reason why it's invalid
        reason: String,
    },

    /// Vector key is invalid
    #[error("Invalid key: {key} ({reason})")]
    InvalidKey {
        /// The invalid key
        key: String,
        /// Reason why it's invalid
        reason: String,
    },

    /// Collection configuration cannot be changed
    #[error("Collection '{collection}' config mismatch: {field} cannot be changed")]
    ConfigMismatch {
        /// Collection name
        collection: String,
        /// The field that cannot be changed
        field: String,
    },

    /// Search limit exceeded
    #[error("Search limit exceeded: requested {requested}, max {max}")]
    SearchLimitExceeded {
        /// Requested limit
        requested: usize,
        /// Maximum allowed limit
        max: usize,
    },

    /// Storage layer error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Transaction error
    #[error("Transaction error: {0}")]
    Transaction(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// IO error (for snapshot operations)
    #[error("IO error: {0}")]
    Io(String),

    /// Database error
    #[error("Database error: {0}")]
    Database(String),
}

impl VectorError {
    /// Check if this error indicates the vector/collection was not found
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            VectorError::CollectionNotFound { .. } | VectorError::VectorNotFound { .. }
        )
    }

    /// Check if this error is a validation error
    pub fn is_validation_error(&self) -> bool {
        matches!(
            self,
            VectorError::DimensionMismatch { .. }
                | VectorError::InvalidDimension { .. }
                | VectorError::EmptyEmbedding
                | VectorError::InvalidEmbedding { .. }
                | VectorError::InvalidCollectionName { .. }
                | VectorError::InvalidKey { .. }
                | VectorError::ConfigMismatch { .. }
        )
    }
}

/// Result type alias for Vector operations
pub type VectorResult<T> = Result<T, VectorError>;

// =============================================================================
// Conversion to StrataError
// =============================================================================

impl VectorError {
    /// Convert to `StrataError` with the actual branch context.
    ///
    /// Prefer this over the `From` impl when `branch_id` is available,
    /// so that `EntityRef` fields contain the real branch instead of a placeholder.
    pub fn into_strata_error(self, branch_id: BranchId) -> StrataError {
        match self {
            VectorError::CollectionNotFound { name } => StrataError::NotFound {
                entity_ref: EntityRef::vector(branch_id, name, ""),
            },
            VectorError::CollectionAlreadyExists { name } => StrataError::InvalidOperation {
                entity_ref: EntityRef::vector(branch_id, name, ""),
                reason: "Collection already exists".to_string(),
            },
            VectorError::VectorNotFound { key } => StrataError::NotFound {
                entity_ref: EntityRef::vector(branch_id, "unknown", key),
            },
            VectorError::ConfigMismatch { collection, field } => StrataError::InvalidOperation {
                entity_ref: EntityRef::vector(branch_id, collection, ""),
                reason: format!("Config field '{}' cannot be changed", field),
            },
            // Remaining variants don't use branch context — delegate to From impl
            other => StrataError::from(other),
        }
    }
}

impl From<VectorError> for StrataError {
    fn from(e: VectorError) -> Self {
        // Fallback conversion without branch context.
        // Prefer `VectorError::into_strata_error(branch_id)` when the branch is known.
        let placeholder_branch_id = BranchId::new();

        match e {
            VectorError::CollectionNotFound { name } => StrataError::NotFound {
                entity_ref: EntityRef::vector(placeholder_branch_id, name, ""),
            },
            VectorError::CollectionAlreadyExists { name } => StrataError::InvalidOperation {
                entity_ref: EntityRef::vector(placeholder_branch_id, name, ""),
                reason: "Collection already exists".to_string(),
            },
            VectorError::DimensionMismatch { expected, got } => {
                StrataError::DimensionMismatch { expected, got }
            }
            VectorError::InvalidDimension { dimension } => StrataError::InvalidInput {
                message: format!("Invalid dimension: {} (must be > 0)", dimension),
            },
            VectorError::VectorNotFound { key } => StrataError::NotFound {
                entity_ref: EntityRef::vector(placeholder_branch_id, "unknown", key),
            },
            VectorError::EmptyEmbedding => StrataError::InvalidInput {
                message: "Empty embedding".to_string(),
            },
            VectorError::InvalidEmbedding { reason } => StrataError::InvalidInput {
                message: format!("Invalid embedding: {}", reason),
            },
            VectorError::InvalidCollectionName { name, reason } => StrataError::InvalidInput {
                message: format!("Invalid collection name '{}': {}", name, reason),
            },
            VectorError::InvalidKey { key, reason } => StrataError::InvalidInput {
                message: format!("Invalid key '{}': {}", key, reason),
            },
            VectorError::ConfigMismatch { collection, field } => StrataError::InvalidOperation {
                entity_ref: EntityRef::vector(placeholder_branch_id, collection, ""),
                reason: format!("Config field '{}' cannot be changed", field),
            },
            VectorError::SearchLimitExceeded { requested, max } => StrataError::CapacityExceeded {
                resource: "search results".to_string(),
                limit: max,
                requested,
            },
            VectorError::Storage(msg) => StrataError::Storage {
                message: msg,
                source: None,
            },
            VectorError::Transaction(msg) => StrataError::TransactionAborted { reason: msg },
            VectorError::Serialization(msg) => StrataError::Serialization { message: msg },
            VectorError::Internal(msg) => StrataError::Internal { message: msg },
            VectorError::Io(msg) => StrataError::Storage {
                message: format!("IO error: {}", msg),
                source: None,
            },
            VectorError::Database(msg) => StrataError::Storage {
                message: format!("Database error: {}", msg),
                source: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_not_found() {
        assert!(VectorError::CollectionNotFound {
            name: "test".into()
        }
        .is_not_found());
        assert!(VectorError::VectorNotFound { key: "key".into() }.is_not_found());
        assert!(!VectorError::EmptyEmbedding.is_not_found());
    }

    #[test]
    fn test_is_validation_error() {
        assert!(VectorError::DimensionMismatch {
            expected: 768,
            got: 384
        }
        .is_validation_error());
        assert!(VectorError::InvalidDimension { dimension: 0 }.is_validation_error());
        assert!(VectorError::EmptyEmbedding.is_validation_error());
        assert!(!VectorError::Internal("oops".into()).is_validation_error());
    }

    #[test]
    fn test_error_display() {
        let err = VectorError::DimensionMismatch {
            expected: 768,
            got: 384,
        };
        assert_eq!(err.to_string(), "Dimension mismatch: expected 768, got 384");
    }

    #[test]
    fn test_error_display_collection_not_found() {
        let err = VectorError::CollectionNotFound {
            name: "my_collection".into(),
        };
        assert_eq!(err.to_string(), "Collection not found: my_collection");
    }

    #[test]
    fn test_error_display_invalid_dimension() {
        let err = VectorError::InvalidDimension { dimension: 0 };
        assert_eq!(err.to_string(), "Invalid dimension: 0 (must be > 0)");
    }

    #[test]
    fn test_error_display_search_limit_exceeded() {
        let err = VectorError::SearchLimitExceeded {
            requested: 1000,
            max: 100,
        };
        assert_eq!(
            err.to_string(),
            "Search limit exceeded: requested 1000, max 100"
        );
    }

    #[test]
    fn test_vector_result_type() {
        fn returns_ok() -> VectorResult<i32> {
            Ok(42)
        }

        fn returns_err() -> VectorResult<i32> {
            Err(VectorError::EmptyEmbedding)
        }

        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }
}
