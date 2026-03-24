//! Error conversion from internal error types.
//!
//! This module provides conversions from internal Strata errors to
//! the executor's [`Error`] type.

use crate::Error;
use strata_core::{EntityRef, StrataError};

/// Convert a StrataError to an executor Error.
///
/// This preserves all error details while mapping to the appropriate
/// executor error variant.
impl From<StrataError> for Error {
    fn from(err: StrataError) -> Self {
        match err {
            // Not Found errors — use typed EntityRef matching
            StrataError::NotFound { entity_ref } => {
                let entity_str = entity_ref.to_string();
                match &entity_ref {
                    EntityRef::Kv { .. } | EntityRef::Json { .. } => Error::KeyNotFound {
                        key: entity_str,
                        hint: None,
                    },
                    EntityRef::Branch { .. } => Error::BranchNotFound {
                        branch: entity_str,
                        hint: None,
                    },
                    EntityRef::Vector { .. } => Error::CollectionNotFound {
                        collection: entity_str,
                        hint: None,
                    },
                    EntityRef::Event { .. } => Error::StreamNotFound {
                        stream: entity_str,
                        hint: None,
                    },
                    EntityRef::State { .. } => Error::CellNotFound {
                        cell: entity_str,
                        hint: None,
                    },
                }
            }

            StrataError::BranchNotFound { branch_id } => Error::BranchNotFound {
                branch: branch_id.to_string(),
                hint: None,
            },

            // Type errors
            StrataError::WrongType { expected, actual } => Error::WrongType {
                expected,
                actual,
                hint: None,
            },

            // Conflict errors (temporal failures)
            StrataError::Conflict { reason, .. } => Error::Conflict { reason },

            StrataError::VersionConflict {
                expected, actual, ..
            } => {
                let expected_num = version_to_u64(&expected);
                let actual_num = version_to_u64(&actual);
                Error::VersionConflict {
                    expected: expected_num,
                    actual: actual_num,
                    expected_type: version_type_name(&expected).to_string(),
                    actual_type: version_type_name(&actual).to_string(),
                    hint: Some("Re-read the current value and retry your operation.".to_string()),
                }
            }

            StrataError::WriteConflict { entity_ref, .. } => Error::Conflict {
                reason: format!("Write conflict on {}", entity_ref),
            },

            StrataError::TransactionAborted { reason } => Error::Conflict {
                reason: format!("Transaction aborted: {}", reason),
            },

            StrataError::TransactionTimeout { duration_ms } => Error::Conflict {
                reason: format!("Transaction timeout after {}ms", duration_ms),
            },

            StrataError::TransactionNotActive { .. } => Error::TransactionNotActive { hint: None },

            // Validation errors
            StrataError::InvalidOperation { entity_ref, reason } => Error::ConstraintViolation {
                reason: format!("Invalid operation on {}: {}", entity_ref, reason),
            },

            StrataError::InvalidInput { message } => Error::InvalidInput {
                reason: message,
                hint: None,
            },

            // Constraint errors
            StrataError::DimensionMismatch { expected, got } => Error::DimensionMismatch {
                expected,
                actual: got,
                hint: None,
            },

            StrataError::CapacityExceeded {
                resource,
                limit,
                requested,
            } => Error::ConstraintViolation {
                reason: format!(
                    "Capacity exceeded for {}: limit {}, requested {}",
                    resource, limit, requested
                ),
            },

            StrataError::BudgetExceeded { operation } => Error::ConstraintViolation {
                reason: format!("Budget exceeded for operation: {}", operation),
            },

            StrataError::PathNotFound { entity_ref, path } => Error::InvalidPath {
                reason: format!("Path '{}' not found in {}", path, entity_ref),
            },

            // History errors
            StrataError::HistoryTrimmed {
                requested,
                earliest_retained,
                ..
            } => Error::HistoryTrimmed {
                requested: version_to_u64(&requested),
                earliest: version_to_u64(&earliest_retained),
            },

            // System errors
            StrataError::Storage { message, source } => {
                let reason = if let Some(ref src) = source {
                    format!("{}: {}", message, src)
                } else {
                    message.clone()
                };
                let hint = io_hint(&reason);
                Error::Io { reason, hint }
            }

            StrataError::Serialization { message } => Error::Serialization { reason: message },

            StrataError::Corruption { message } => Error::Io {
                reason: format!("Data corruption: {}", message),
                hint: Some(
                    "The database may need recovery. Re-open to attempt automatic repair."
                        .to_string(),
                ),
            },

            StrataError::Internal { message } => Error::Internal {
                reason: message,
                hint: Some(
                    "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                        .to_string(),
                ),
            },
        }
    }
}

/// Convert a strata_core::StrataResult to an executor Result.
pub fn convert_result<T>(result: strata_core::StrataResult<T>) -> crate::Result<T> {
    result.map_err(Error::from)
}

/// Generate an actionable hint for I/O errors based on the error message.
fn io_hint(reason: &str) -> Option<String> {
    let lower = reason.to_lowercase();
    if lower.contains("permission denied") {
        Some("Check file permissions on the database directory.".to_string())
    } else if lower.contains("no space") || lower.contains("disk full") {
        Some("Free disk space or move the database to a larger volume.".to_string())
    } else {
        Some(
            "Check that the database directory is accessible and has sufficient disk space."
                .to_string(),
        )
    }
}

/// Extract a u64 from a Version enum.
fn version_to_u64(version: &strata_core::Version) -> u64 {
    match version {
        strata_core::Version::Txn(n) => *n,
        strata_core::Version::Sequence(n) => *n,
        strata_core::Version::Counter(n) => *n,
    }
}

/// Get a human-readable name for the Version variant type.
fn version_type_name(version: &strata_core::Version) -> &'static str {
    match version {
        strata_core::Version::Txn(_) => "Txn",
        strata_core::Version::Sequence(_) => "Sequence",
        strata_core::Version::Counter(_) => "Counter",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::{EntityRef, Version};

    #[test]
    fn test_not_found_kv() {
        let err = StrataError::not_found(EntityRef::kv(
            strata_core::types::BranchId::from_bytes([0; 16]),
            "mykey",
        ));
        let converted: Error = err.into();
        match converted {
            Error::KeyNotFound { key, .. } => assert!(key.contains("mykey")),
            _ => panic!("Expected KeyNotFound"),
        }
    }

    #[test]
    fn test_version_conflict() {
        let err = StrataError::version_conflict(
            EntityRef::kv(strata_core::types::BranchId::from_bytes([0; 16]), "key"),
            Version::Txn(5),
            Version::Txn(6),
        );
        let converted: Error = err.into();
        match converted {
            Error::VersionConflict {
                expected,
                actual,
                expected_type,
                actual_type,
                ..
            } => {
                assert_eq!(expected, 5);
                assert_eq!(actual, 6);
                assert_eq!(expected_type, "Txn");
                assert_eq!(actual_type, "Txn");
            }
            _ => panic!("Expected VersionConflict"),
        }
    }

    #[test]
    fn test_wrong_type() {
        let err = StrataError::wrong_type("Int", "String");
        let converted: Error = err.into();
        match converted {
            Error::WrongType { expected, actual, .. } => {
                assert_eq!(expected, "Int");
                assert_eq!(actual, "String");
            }
            _ => panic!("Expected WrongType"),
        }
    }

    #[test]
    fn test_internal_error() {
        let err = StrataError::internal("something went wrong");
        let converted: Error = err.into();
        match converted {
            Error::Internal { reason, .. } => assert!(reason.contains("something went wrong")),
            _ => panic!("Expected Internal"),
        }
    }

    #[test]
    fn test_dimension_mismatch() {
        let err = StrataError::dimension_mismatch(384, 768);
        let converted: Error = err.into();
        match converted {
            Error::DimensionMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, 384);
                assert_eq!(actual, 768);
            }
            _ => panic!("Expected DimensionMismatch"),
        }
    }
}
