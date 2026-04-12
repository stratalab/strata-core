//! Error types for command execution.
//!
//! All errors from command execution are represented by the [`Error`] enum.
//! These errors are:
//! - **Structured**: Each variant has typed fields for error details
//! - **Serializable**: Can be converted to/from JSON
//! - **Lossless**: No error information is lost in conversion from internal errors

use serde::{Deserialize, Serialize};

/// Command execution errors.
///
/// All errors that can occur during command execution are represented here.
/// Errors are structured to preserve details for client-side handling.
///
/// # Categories
///
/// | Category | Variants | Description |
/// |----------|----------|-------------|
/// | Not Found | `KeyNotFound`, `BranchNotFound`, etc. | Entity doesn't exist |
/// | Type | `WrongType` | Type mismatch |
/// | Validation | `InvalidKey`, `InvalidPath`, `InvalidInput` | Bad input |
/// | Concurrency | `VersionConflict`, `TransitionFailed`, `Conflict` | Race conditions |
/// | State | `BranchClosed`, `BranchExists`, `CollectionExists` | Invalid state transition |
/// | Constraint | `DimensionMismatch`, `ConstraintViolation`, etc. | Limits exceeded |
/// | Transaction | `TransactionNotActive`, `TransactionAlreadyActive` | Transaction state |
/// | System | `Io`, `Serialization`, `Internal` | Infrastructure errors |
///
/// # Example
///
/// ```text
/// use strata_executor::{Command, Error, Executor};
///
/// match executor.execute(cmd) {
///     Ok(output) => { /* handle success */ }
///     Err(Error::KeyNotFound { key, .. }) => {
///         println!("Key '{}' not found", key);
///     }
///     Err(e) => {
///         println!("Error: {}", e);
///     }
/// }
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub enum Error {
    // ==================== Not Found ====================
    /// Key not found in KV store
    #[error("key not found: {key}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    KeyNotFound {
        /// The missing key.
        key: String,
        /// Optional actionable hint (e.g. "Did you mean ...?").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Branch not found
    #[error("branch not found: {branch}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    BranchNotFound {
        /// The missing branch identifier.
        branch: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Vector collection not found
    #[error("collection not found: {collection}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    CollectionNotFound {
        /// The missing collection name.
        collection: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Event stream not found
    #[error("stream not found: {stream}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    StreamNotFound {
        /// The missing stream name.
        stream: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// JSON document not found
    #[error("document not found: {key}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    DocumentNotFound {
        /// The missing document key.
        key: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Graph not found
    #[error("graph not found: {graph}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    GraphNotFound {
        /// The missing graph name.
        graph: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    // ==================== Type Errors ====================
    /// Wrong type for operation
    #[error("wrong type: expected {expected}, got {actual}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    WrongType {
        /// Expected type name.
        expected: String,
        /// Actual type name.
        actual: String,
        /// Optional actionable hint (e.g. "Key 'x' is a Counter. Use INCR/DECR.").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    // ==================== Validation Errors ====================
    /// Invalid key format
    #[error("invalid key: {reason}")]
    InvalidKey {
        /// Reason the key is invalid.
        reason: String,
    },

    /// Invalid JSON path
    #[error("invalid path: {reason}")]
    InvalidPath {
        /// Reason the path is invalid.
        reason: String,
    },

    /// Invalid input
    #[error("invalid input: {reason}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    InvalidInput {
        /// Description of the validation failure.
        reason: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    // ==================== Concurrency Errors ====================
    /// Version conflict (CAS failure)
    #[error("version conflict: expected {expected_type}:{expected}, got {actual_type}:{actual}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    VersionConflict {
        /// Expected version number.
        expected: u64,
        /// Actual version number found.
        actual: u64,
        /// Expected version type label.
        expected_type: String,
        /// Actual version type label.
        actual_type: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// State transition failed (expected value mismatch)
    #[error("transition failed: expected {expected}, got {actual}")]
    TransitionFailed {
        /// Expected state value.
        expected: String,
        /// Actual state value.
        actual: String,
    },

    /// Generic conflict
    #[error("conflict: {reason}")]
    Conflict {
        /// Description of the conflict.
        reason: String,
    },

    // ==================== State Errors ====================
    /// Branch is closed
    #[error("branch closed: {branch}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    BranchClosed {
        /// The closed branch identifier.
        branch: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Branch already exists
    #[error("branch already exists: {branch}")]
    BranchExists {
        /// The duplicate branch identifier.
        branch: String,
    },

    /// Collection already exists
    #[error("collection already exists: {collection}")]
    CollectionExists {
        /// The duplicate collection name.
        collection: String,
    },

    // ==================== Constraint Errors ====================
    /// Vector dimension mismatch
    #[error("dimension mismatch: expected {expected}, got {actual}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    DimensionMismatch {
        /// Expected dimensionality.
        expected: usize,
        /// Actual dimensionality provided.
        actual: usize,
        /// Optional actionable hint (e.g. model identification).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Constraint violation
    #[error("constraint violation: {reason}")]
    ConstraintViolation {
        /// Description of the violated constraint.
        reason: String,
    },

    /// Requested version was trimmed by retention policy
    #[error("history trimmed: requested version {requested}, earliest is {earliest}")]
    HistoryTrimmed {
        /// Version that was requested.
        requested: u64,
        /// Earliest available version.
        earliest: u64,
    },

    /// Numeric overflow
    #[error("overflow: {reason}")]
    Overflow {
        /// Description of the overflow.
        reason: String,
    },

    // ==================== Access Control ====================
    /// Write command rejected because the database is read-only
    #[error("access denied: {command} rejected — database is read-only{}", hint.as_ref().map(|h| format!(". {}", h)).unwrap_or_default())]
    AccessDenied {
        /// Name of the rejected command.
        command: String,
        /// Optional actionable hint (e.g. follower guidance).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    // ==================== Transaction Errors ====================
    /// No active transaction
    #[error("no active transaction{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    TransactionNotActive {
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Transaction already active
    #[error("transaction already active{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    TransactionAlreadyActive {
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Transaction conflict (commit-time validation failure)
    #[error("transaction conflict: {reason}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    TransactionConflict {
        /// Description of the transaction conflict.
        reason: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    // ==================== System Errors ====================
    /// I/O error
    #[error("I/O error: {reason}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    Io {
        /// I/O error details.
        reason: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Serialization error
    #[error("serialization error: {reason}")]
    Serialization {
        /// Serialization error details.
        reason: String,
    },

    /// Internal error (bug or invariant violation)
    #[error("internal error: {reason}{}", hint.as_deref().map(|h| format!(". {}", h)).unwrap_or_default())]
    Internal {
        /// Internal error details.
        reason: String,
        /// Optional actionable hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },

    /// Feature not yet implemented
    #[error("not implemented: {feature} - {reason}")]
    NotImplemented {
        /// Name of the unimplemented feature.
        feature: String,
        /// Details about what is missing.
        reason: String,
    },

    /// The requested timestamp is before the oldest available data
    #[error("history unavailable: requested timestamp {requested_ts} is before oldest available {oldest_available_ts}")]
    HistoryUnavailable {
        /// The timestamp that was requested.
        requested_ts: u64,
        /// The oldest available timestamp.
        oldest_available_ts: u64,
    },
}

/// Error severity classification for CLI output formatting.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// User made a mistake (wrong input, missing entity, wrong type).
    UserError,
    /// System failure (I/O, storage, transient conflict).
    SystemFailure,
    /// Internal bug (invariant violation).
    InternalBug,
}

impl Error {
    /// Classify this error by severity.
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Error::Io { .. } | Error::Serialization { .. } => ErrorSeverity::SystemFailure,
            Error::Internal { .. } => ErrorSeverity::InternalBug,
            _ => ErrorSeverity::UserError,
        }
    }
}
