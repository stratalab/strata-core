//! Conversion from internal errors to executor-facing errors.

use crate::Error;
use strata_core::EntityRef;
use strata_engine::{StrataError, StrataResult};

pub(crate) fn convert_result<T>(result: StrataResult<T>) -> crate::Result<T> {
    result.map_err(Error::from)
}

impl From<StrataError> for Error {
    fn from(err: StrataError) -> Self {
        match err {
            StrataError::NotFound { entity_ref } => {
                let entity_str = entity_ref.to_string();
                match entity_ref.as_ref() {
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
                    EntityRef::Graph { .. } => Error::KeyNotFound {
                        key: entity_str,
                        hint: None,
                    },
                }
            }
            StrataError::BranchNotFound { branch_id } => Error::BranchNotFound {
                branch: branch_id.to_string(),
                hint: None,
            },
            StrataError::BranchNotFoundByName { name, .. } => Error::BranchNotFound {
                branch: name,
                hint: None,
            },
            StrataError::BranchArchived { name } => Error::BranchArchived {
                branch: name,
                hint: None,
            },
            StrataError::WrongType { expected, actual } => Error::WrongType {
                expected,
                actual,
                hint: None,
            },
            StrataError::Conflict { reason, .. } => Error::Conflict { reason },
            StrataError::VersionConflict {
                expected, actual, ..
            } => Error::VersionConflict {
                expected: version_to_u64(&expected),
                actual: version_to_u64(&actual),
                expected_type: version_type_name(&expected).to_string(),
                actual_type: version_type_name(&actual).to_string(),
                hint: Some("Re-read the current value and retry your operation.".to_string()),
            },
            StrataError::WriteConflict { entity_ref, .. } => Error::Conflict {
                reason: format!("Write conflict on {}", entity_ref),
            },
            StrataError::TransactionAborted { reason } => Error::Conflict {
                reason: format!("Transaction aborted: {}", reason),
            },
            StrataError::TransactionTimeout { duration_ms } => Error::Conflict {
                reason: format!("Transaction timeout after {}ms", duration_ms),
            },
            StrataError::WriteStallTimeout {
                stall_duration_ms,
                l0_count,
            } => Error::Conflict {
                reason: format!(
                    "Write stall timeout after {}ms (L0 count: {})",
                    stall_duration_ms, l0_count
                ),
            },
            StrataError::TransactionNotActive { .. } => Error::TransactionNotActive { hint: None },
            StrataError::InvalidOperation { entity_ref, reason } => Error::ConstraintViolation {
                reason: format!("Invalid operation on {}: {}", entity_ref, reason),
            },
            StrataError::InvalidInput { message } => Error::InvalidInput {
                reason: message,
                hint: None,
            },
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
            StrataError::HistoryTrimmed {
                requested,
                earliest_retained,
                ..
            } => Error::HistoryTrimmed {
                requested: version_to_u64(&requested),
                earliest: version_to_u64(&earliest_retained),
            },
            StrataError::Storage { message, source } => {
                let reason = if let Some(ref src) = source {
                    format!("{}: {}", message, src)
                } else {
                    message
                };
                Error::Io {
                    hint: io_hint(&reason),
                    reason,
                }
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
            _ => Error::Internal {
                reason: format!("Unmapped error: {err}"),
                hint: Some(
                    "A new error variant was added that the executor doesn't handle yet."
                        .to_string(),
                ),
            },
        }
    }
}

fn io_hint(reason: &str) -> Option<String> {
    let lower = reason.to_lowercase();
    if lower.contains("permission denied") {
        Some("Check file permissions on the database directory.".to_string())
    } else if lower.contains("no space") || lower.contains("disk full") {
        Some("Free disk space or move the database to a larger volume.".to_string())
    } else if lower.contains("not found") || lower.contains("no such file") {
        Some("Check that the database path exists and is accessible.".to_string())
    } else {
        None
    }
}

fn version_to_u64(v: &strata_core::Version) -> u64 {
    match v {
        strata_core::Version::Txn(v)
        | strata_core::Version::Sequence(v)
        | strata_core::Version::Counter(v) => *v,
    }
}

fn version_type_name(v: &strata_core::Version) -> &'static str {
    match v {
        strata_core::Version::Txn(_) => "txn",
        strata_core::Version::Sequence(_) => "seq",
        strata_core::Version::Counter(_) => "cnt",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_engine::StrataError;

    #[test]
    fn branch_name_lookup_maps_to_branch_not_found() {
        let err = Error::from(StrataError::BranchNotFoundByName {
            name: "missing".to_string(),
            branch_id: strata_core::BranchId::new(),
        });

        assert_eq!(
            err,
            Error::BranchNotFound {
                branch: "missing".to_string(),
                hint: None,
            }
        );
    }

    #[test]
    fn storage_error_preserves_io_hint() {
        let err = Error::from(StrataError::Storage {
            message: "permission denied".to_string(),
            source: None,
        });

        match err {
            Error::Io { reason, hint } => {
                assert!(reason.contains("permission denied"));
                assert_eq!(
                    hint.as_deref(),
                    Some("Check file permissions on the database directory.")
                );
            }
            other => panic!("expected Error::Io, got {:?}", other),
        }
    }
}
