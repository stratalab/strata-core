//! Error types for Strata database
//!
//! This module defines all error types used throughout the system.
//! We use `thiserror` for automatic `Display` and `Error` trait implementations.
//!
//! ## Strata Error Model
//!
//! The `StrataError` type is the unified error type for all Strata APIs.
//! It provides consistent error handling across all primitives.
//!
//! ### Canonical Error Codes (Frozen)
//!
//! The following 10 error codes are the canonical wire representation:
//!
//! | Code | Description |
//! |------|-------------|
//! | NotFound | Entity or key not found |
//! | WrongType | Wrong primitive or value type |
//! | InvalidKey | Key syntax invalid |
//! | InvalidPath | JSON path invalid |
//! | HistoryTrimmed | Requested version is unavailable |
//! | ConstraintViolation | API-level invariant violation (structural failure) |
//! | Conflict | Temporal failure (version mismatch, write conflict) |
//! | SerializationError | Invalid Value encoding |
//! | StorageError | Disk or WAL failure |
//! | InternalError | Bug or invariant violation |
//!
//! ### Error Classification
//!
//! - **Temporal failures (Conflict)**: Version conflicts, write conflicts, transaction aborts
//!   - These are retryable - the operation may succeed with fresh data
//! - **Structural failures (ConstraintViolation)**: Invalid input, dimension mismatch, capacity exceeded
//!   - These require input changes to resolve
//!
//! ### Wire Encoding
//!
//! All errors encode to JSON as:
//! ```json
//! {
//!   "code": "NotFound",
//!   "message": "Entity not found: kv:default/config",
//!   "details": { "entity": "kv:default/config" }
//! }
//! ```
//!
//! ### Usage
//!
//! ```no_run
//! # use strata_core::{StrataError, StrataResult};
//! # let result: StrataResult<String> = Ok("ok".to_string());
//! match result {
//!     Err(StrataError::NotFound { entity_ref }) => {
//!         println!("Entity not found: {}", entity_ref);
//!     }
//!     Err(StrataError::Conflict { reason, .. }) => {
//!         println!("Conflict: {}", reason);
//!     }
//!     Err(e) if e.is_retryable() => {
//!         // Retry the operation
//!     }
//!     Err(e) => {
//!         println!("Other error: {}", e);
//!     }
//!     Ok(value) => { /* success */ }
//! }
//! ```

use crate::contract::{EntityRef, Version};
use crate::types::BranchId;
use std::collections::HashMap;
use std::io;
use thiserror::Error;

// =============================================================================
// ErrorCode - Canonical Wire Error Codes (Frozen)
// =============================================================================

/// Canonical error codes for wire encoding
///
/// These 10 codes are the stable wire representation of all Strata errors.
/// They are frozen and will not change without a major version bump.
///
/// ## Error Categories
///
/// - **NotFound**: Entity or key not found
/// - **WrongType**: Wrong primitive or value type
/// - **InvalidKey**: Key syntax invalid
/// - **InvalidPath**: JSON path invalid
/// - **HistoryTrimmed**: Requested version is unavailable
/// - **ConstraintViolation**: Structural failure (invalid input, limits exceeded)
/// - **Conflict**: Temporal failure (version mismatch, write conflict, transaction abort)
/// - **SerializationError**: Invalid Value encoding
/// - **StorageError**: Disk or WAL failure
/// - **InternalError**: Bug or invariant violation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// Entity or key not found
    NotFound,
    /// Wrong primitive or value type
    WrongType,
    /// Key syntax invalid
    InvalidKey,
    /// JSON path invalid
    InvalidPath,
    /// Requested version is unavailable (history trimmed)
    HistoryTrimmed,
    /// Structural failure: invalid input, dimension mismatch, capacity exceeded
    ConstraintViolation,
    /// Temporal failure: version mismatch, write conflict, transaction abort
    Conflict,
    /// Invalid Value encoding
    SerializationError,
    /// Disk or WAL failure
    StorageError,
    /// Bug or invariant violation
    InternalError,
}

impl ErrorCode {
    /// Get the canonical string representation for wire encoding
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::NotFound => "NotFound",
            ErrorCode::WrongType => "WrongType",
            ErrorCode::InvalidKey => "InvalidKey",
            ErrorCode::InvalidPath => "InvalidPath",
            ErrorCode::HistoryTrimmed => "HistoryTrimmed",
            ErrorCode::ConstraintViolation => "ConstraintViolation",
            ErrorCode::Conflict => "Conflict",
            ErrorCode::SerializationError => "SerializationError",
            ErrorCode::StorageError => "StorageError",
            ErrorCode::InternalError => "InternalError",
        }
    }

    /// Parse an error code from its string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "NotFound" => Some(ErrorCode::NotFound),
            "WrongType" => Some(ErrorCode::WrongType),
            "InvalidKey" => Some(ErrorCode::InvalidKey),
            "InvalidPath" => Some(ErrorCode::InvalidPath),
            "HistoryTrimmed" => Some(ErrorCode::HistoryTrimmed),
            "ConstraintViolation" => Some(ErrorCode::ConstraintViolation),
            "Conflict" => Some(ErrorCode::Conflict),
            "SerializationError" => Some(ErrorCode::SerializationError),
            "StorageError" => Some(ErrorCode::StorageError),
            "InternalError" => Some(ErrorCode::InternalError),
            _ => None,
        }
    }

    /// Check if this error code represents a retryable error
    pub fn is_retryable(&self) -> bool {
        matches!(self, ErrorCode::Conflict)
    }

    /// Check if this error code represents a serious/unrecoverable error
    pub fn is_serious(&self) -> bool {
        matches!(self, ErrorCode::InternalError | ErrorCode::StorageError)
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// ErrorDetails - Structured Error Details for Wire Encoding
// =============================================================================

/// Structured error details for wire encoding
///
/// This provides type-safe access to error details that will be serialized
/// as JSON in the wire format.
#[derive(Debug, Clone, Default)]
pub struct ErrorDetails {
    /// Key-value pairs for error details
    fields: HashMap<String, DetailValue>,
}

/// A value in error details
#[derive(Debug, Clone)]
pub enum DetailValue {
    /// String value
    String(String),
    /// Integer value
    Int(i64),
    /// Boolean value
    Bool(bool),
}

impl ErrorDetails {
    /// Create empty error details
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Add a string field
    pub fn with_string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields
            .insert(key.into(), DetailValue::String(value.into()));
        self
    }

    /// Add an integer field
    pub fn with_int(mut self, key: impl Into<String>, value: i64) -> Self {
        self.fields.insert(key.into(), DetailValue::Int(value));
        self
    }

    /// Add a boolean field
    pub fn with_bool(mut self, key: impl Into<String>, value: bool) -> Self {
        self.fields.insert(key.into(), DetailValue::Bool(value));
        self
    }

    /// Check if details are empty
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Get the underlying fields
    pub fn fields(&self) -> &HashMap<String, DetailValue> {
        &self.fields
    }

    /// Convert to a HashMap<String, String> for simple serialization
    pub fn to_string_map(&self) -> HashMap<String, String> {
        self.fields
            .iter()
            .map(|(k, v)| {
                let s = match v {
                    DetailValue::String(s) => s.clone(),
                    DetailValue::Int(i) => i.to_string(),
                    DetailValue::Bool(b) => b.to_string(),
                };
                (k.clone(), s)
            })
            .collect()
    }
}

// =============================================================================
// ConstraintReason - Structured Constraint Violation Reasons
// =============================================================================

/// Structured constraint violation reasons
///
/// These reasons indicate why a constraint was violated.
/// Per ERR-4: ConstraintViolation = structural failures (not temporal).
/// These require input changes to resolve - they won't succeed on retry alone.
///
/// ## Categories
///
/// - **Value Constraints**: Value too large, dimension mismatch
/// - **Key Constraints**: Invalid key format
/// - **Capacity Constraints**: Resource limits exceeded
/// - **Operation Constraints**: Invalid operation for current state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConstraintReason {
    // Value constraints
    /// Value exceeds maximum allowed size
    ValueTooLarge,
    /// String exceeds maximum length
    StringTooLong,
    /// Bytes exceeds maximum length
    BytesTooLong,
    /// Array exceeds maximum length
    ArrayTooLong,
    /// Object exceeds maximum entries
    ObjectTooLarge,
    /// Nesting depth exceeds maximum
    NestingTooDeep,

    // Key constraints
    /// Key exceeds maximum length
    KeyTooLong,
    /// Key contains invalid characters
    KeyInvalid,
    /// Key is empty
    KeyEmpty,

    // Vector constraints
    /// Vector dimension doesn't match collection
    DimensionMismatch,
    /// Vector dimension exceeds maximum
    DimensionTooLarge,

    // Capacity constraints
    /// Resource limit reached
    CapacityExceeded,
    /// Operation budget exceeded
    BudgetExceeded,

    // Operation constraints
    /// Operation not allowed in current state
    InvalidOperation,
    /// Branch is not active
    BranchNotActive,
    /// Transaction not active
    TransactionNotActive,

    // Type constraints
    /// Wrong value type for operation
    WrongType,
    /// Numeric overflow
    Overflow,
}

impl ConstraintReason {
    /// Get the canonical string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ConstraintReason::ValueTooLarge => "value_too_large",
            ConstraintReason::StringTooLong => "string_too_long",
            ConstraintReason::BytesTooLong => "bytes_too_long",
            ConstraintReason::ArrayTooLong => "array_too_long",
            ConstraintReason::ObjectTooLarge => "object_too_large",
            ConstraintReason::NestingTooDeep => "nesting_too_deep",
            ConstraintReason::KeyTooLong => "key_too_long",
            ConstraintReason::KeyInvalid => "key_invalid",
            ConstraintReason::KeyEmpty => "key_empty",
            ConstraintReason::DimensionMismatch => "dimension_mismatch",
            ConstraintReason::DimensionTooLarge => "dimension_too_large",
            ConstraintReason::CapacityExceeded => "capacity_exceeded",
            ConstraintReason::BudgetExceeded => "budget_exceeded",
            ConstraintReason::InvalidOperation => "invalid_operation",
            ConstraintReason::BranchNotActive => "branch_not_active",
            ConstraintReason::TransactionNotActive => "transaction_not_active",
            ConstraintReason::WrongType => "wrong_type",
            ConstraintReason::Overflow => "overflow",
        }
    }

    /// Parse a constraint reason from its string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "value_too_large" => Some(ConstraintReason::ValueTooLarge),
            "string_too_long" => Some(ConstraintReason::StringTooLong),
            "bytes_too_long" => Some(ConstraintReason::BytesTooLong),
            "array_too_long" => Some(ConstraintReason::ArrayTooLong),
            "object_too_large" => Some(ConstraintReason::ObjectTooLarge),
            "nesting_too_deep" => Some(ConstraintReason::NestingTooDeep),
            "key_too_long" => Some(ConstraintReason::KeyTooLong),
            "key_invalid" => Some(ConstraintReason::KeyInvalid),
            "key_empty" => Some(ConstraintReason::KeyEmpty),
            "dimension_mismatch" => Some(ConstraintReason::DimensionMismatch),
            "dimension_too_large" => Some(ConstraintReason::DimensionTooLarge),
            "capacity_exceeded" => Some(ConstraintReason::CapacityExceeded),
            "budget_exceeded" => Some(ConstraintReason::BudgetExceeded),
            "invalid_operation" => Some(ConstraintReason::InvalidOperation),
            "branch_not_active" => Some(ConstraintReason::BranchNotActive),
            "transaction_not_active" => Some(ConstraintReason::TransactionNotActive),
            "wrong_type" => Some(ConstraintReason::WrongType),
            "overflow" => Some(ConstraintReason::Overflow),
            _ => None,
        }
    }

    /// Get a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            ConstraintReason::ValueTooLarge => "Value exceeds maximum allowed size",
            ConstraintReason::StringTooLong => "String exceeds maximum length",
            ConstraintReason::BytesTooLong => "Bytes exceeds maximum length",
            ConstraintReason::ArrayTooLong => "Array exceeds maximum length",
            ConstraintReason::ObjectTooLarge => "Object exceeds maximum entries",
            ConstraintReason::NestingTooDeep => "Nesting depth exceeds maximum",
            ConstraintReason::KeyTooLong => "Key exceeds maximum length",
            ConstraintReason::KeyInvalid => "Key contains invalid characters",
            ConstraintReason::KeyEmpty => "Key cannot be empty",
            ConstraintReason::DimensionMismatch => "Vector dimension doesn't match collection",
            ConstraintReason::DimensionTooLarge => "Vector dimension exceeds maximum",
            ConstraintReason::CapacityExceeded => "Resource limit reached",
            ConstraintReason::BudgetExceeded => "Operation budget exceeded",
            ConstraintReason::InvalidOperation => "Operation not allowed in current state",
            ConstraintReason::BranchNotActive => "Branch is not active",
            ConstraintReason::TransactionNotActive => "Transaction is not active",
            ConstraintReason::WrongType => "Wrong value type for operation",
            ConstraintReason::Overflow => "Numeric overflow",
        }
    }
}

impl std::fmt::Display for ConstraintReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// StrataError - Unified Error Type
// =============================================================================

/// Unified error type for all Strata operations
///
/// This is the canonical error type returned by all Strata APIs.
/// It provides consistent error handling across all primitives.
///
/// ## Wire Encoding
///
/// All errors encode to the canonical wire format with three fields:
/// - `code`: One of the 10 canonical error codes (see `ErrorCode`)
/// - `message`: Human-readable error message
/// - `details`: Optional structured details
///
/// ## Error Categories
///
/// - **Not Found**: Entity doesn't exist (`NotFound`, `BranchNotFound`, `PathNotFound`)
/// - **Wrong Type**: Type mismatch (`WrongType`)
/// - **Temporal Failures (Conflict)**: Version mismatch, write conflict, transaction abort
///   - These are **retryable** with fresh data
/// - **Structural Failures (ConstraintViolation)**: Invalid input, dimension mismatch, capacity exceeded
///   - These require input changes to resolve
/// - **Storage**: Low-level storage failures (`Storage`, `Serialization`, `Corruption`)
/// - **Internal**: Unexpected internal errors (`Internal`)
///
/// ## Usage
///
/// ```no_run
/// # fn some_db_operation() -> strata_core::StrataResult<String> { Ok("value".to_string()) }
/// use strata_core::{StrataError, StrataResult, EntityRef, Version};
///
/// fn example_operation() -> StrataResult<String> {
///     let value = some_db_operation()?;
///     Ok(value)
/// }
///
/// # let result: StrataResult<String> = some_db_operation();
/// match result {
///     Err(StrataError::NotFound { entity_ref }) => {
///         println!("Entity not found: {}", entity_ref);
///     }
///     Err(StrataError::Conflict { reason, .. }) => {
///         println!("Conflict: {}", reason);
///     }
///     Err(e) if e.is_retryable() => {
///         // Retry the operation
///     }
///     Err(e) if e.is_serious() => {
///         // Log and alert
///     }
///     Err(e) => { /* handle other errors */ }
///     Ok(value) => { /* success */ }
/// }
/// ```
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StrataError {
    // =========================================================================
    // Not Found Errors
    // =========================================================================
    /// Entity not found
    ///
    /// The referenced entity does not exist. This could be a key, document,
    /// event, state cell, or any other entity type.
    ///
    /// Wire code: `NotFound`
    #[error("not found: {entity_ref}")]
    NotFound {
        /// Reference to the entity that was not found.
        ///
        /// Boxed to keep `StrataError` under the `result_large_err`
        /// clippy threshold (see `Conflict` variant for the same
        /// rationale). `EntityRef` can be up to ~96 bytes after
        /// Phase 0 of the space-correctness fix.
        entity_ref: Box<EntityRef>,
    },

    /// Branch not found
    ///
    /// The specified branch does not exist.
    ///
    /// Wire code: `NotFound`
    #[error("branch not found: {branch_id}")]
    BranchNotFound {
        /// ID of the branch that was not found
        branch_id: BranchId,
    },

    /// Branch not found (user-facing name preserved)
    ///
    /// The specified branch does not exist, and the caller supplied a
    /// user-facing branch name that should survive error conversion.
    ///
    /// This is used by branch lifecycle gates that operate on names but still
    /// want the canonical `BranchId` attached for diagnostics and internal
    /// classification.
    ///
    /// Wire code: `NotFound`
    #[error("branch not found: {name}")]
    BranchNotFoundByName {
        /// User-facing branch name that was not found.
        name: String,
        /// Canonical branch id derived from the provided name.
        branch_id: BranchId,
    },

    /// Branch is archived
    ///
    /// The specified branch exists but is in the `Archived` lifecycle state and
    /// does not accept writes. Read operations against the branch still
    /// succeed; this error is raised at the `BranchService` write gate when a
    /// mutation targets an archived lifecycle instance.
    ///
    /// Surfaced by `BranchControlStore::require_writable_by_name` (B4).
    ///
    /// Wire code: `ConstraintViolation` — the caller attempted a mutation that
    /// violates the archived-branch read-only contract.
    #[error("branch is archived: {name}")]
    BranchArchived {
        /// User-facing name of the archived branch.
        ///
        /// Carries the name rather than a `BranchId` because the typed error
        /// is intended to be operator-usable end to end (CLI / executor
        /// error responses); callers already have the name in hand when the
        /// gate rejects them.
        name: String,
    },

    // =========================================================================
    // Type Errors
    // =========================================================================
    /// Wrong type
    ///
    /// The operation expected a different value type or primitive type.
    ///
    /// Wire code: `WrongType`
    #[error("wrong type: expected {expected}, got {actual}")]
    WrongType {
        /// Expected type
        expected: String,
        /// Actual type found
        actual: String,
    },

    // =========================================================================
    // Conflict Errors (Temporal Failures - Retryable)
    // =========================================================================
    /// Generic conflict (temporal failure)
    ///
    /// The operation failed due to a temporal conflict that may be resolved
    /// by retrying with fresh data. This is the canonical wire representation
    /// for all conflict types.
    ///
    /// Wire code: `Conflict`
    #[error("conflict: {reason}")]
    Conflict {
        /// Reason for the conflict
        reason: String,
        /// Optional entity reference.
        ///
        /// Boxed because `EntityRef` grew to ~96 bytes after Phase 0
        /// of the space-correctness fix added `space: String` to its
        /// space-bearing variants. Without boxing, `StrataError` would
        /// exceed the 128-byte `result_large_err` clippy threshold and
        /// turn every `StrataResult<T>` function into a CI failure.
        entity_ref: Option<Box<EntityRef>>,
        /// Optional transaction ID that caused the conflict
        transaction_id: Option<u64>,
    },

    /// Version conflict
    ///
    /// The operation failed because the entity's version doesn't match
    /// the expected version. This typically happens with:
    /// - Compare-and-swap (CAS) operations
    /// - Optimistic concurrency control conflicts
    ///
    /// This error is **retryable** - the operation can be retried after
    /// re-reading the current version.
    ///
    /// Wire code: `Conflict`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId, Version};
    /// # let branch_id = BranchId::new();
    /// StrataError::version_conflict(
    ///     EntityRef::kv(branch_id,"default", "counter"),
    ///     Version::Counter(5),  // expected
    ///     Version::Counter(6),  // actual
    /// );
    /// ```
    #[error("version conflict on {entity_ref}: expected {expected}, got {actual}")]
    VersionConflict {
        /// Reference to the conflicted entity (boxed — see `Conflict`).
        entity_ref: Box<EntityRef>,
        /// The version that was expected
        expected: Version,
        /// The actual version found
        actual: Version,
    },

    /// Write conflict
    ///
    /// Two transactions attempted to modify the same entity concurrently.
    /// This error is **retryable** - the transaction can be retried.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// StrataError::write_conflict(EntityRef::kv(branch_id,"default", "shared-key"));
    /// ```
    #[error("write conflict on {entity_ref}")]
    WriteConflict {
        /// Reference to the conflicted entity (boxed — see `Conflict`).
        entity_ref: Box<EntityRef>,
    },

    // =========================================================================
    // Transaction Errors
    // =========================================================================
    /// Transaction aborted
    ///
    /// The transaction was aborted due to a conflict, timeout, or other
    /// transactional failure. This error is **retryable**.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::TransactionAborted {
    ///     reason: "Conflict on key 'counter'".to_string(),
    /// };
    /// ```
    #[error("transaction aborted: {reason}")]
    TransactionAborted {
        /// Reason for the abort
        reason: String,
    },

    /// Transaction timeout
    ///
    /// The transaction exceeded the maximum allowed duration.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::TransactionTimeout { duration_ms: 5000 };
    /// ```
    #[error("transaction timeout after {duration_ms}ms")]
    TransactionTimeout {
        /// How long the transaction ran before timing out
        duration_ms: u64,
    },

    /// Write stall timeout
    ///
    /// The write was stalled waiting for L0 compaction to reduce the segment
    /// count below the stop-writes trigger, but the wait exceeded the
    /// configured timeout.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::WriteStallTimeout { stall_duration_ms: 30000, l0_count: 40 };
    /// ```
    #[error("write stall timeout after {stall_duration_ms}ms (L0 count: {l0_count})")]
    WriteStallTimeout {
        /// How long the write was stalled before giving up
        stall_duration_ms: u64,
        /// L0 segment count at the time of timeout
        l0_count: usize,
    },

    /// Transaction not active
    ///
    /// An operation was attempted on a transaction that has already
    /// been committed or rolled back.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::TransactionNotActive {
    ///     state: "committed".to_string(),
    /// };
    /// ```
    #[error("transaction not active (already {state})")]
    TransactionNotActive {
        /// Current state of the transaction
        state: String,
    },

    // =========================================================================
    // Validation Errors
    // =========================================================================
    /// Invalid operation
    ///
    /// The operation is not valid for the current state of the entity.
    /// Examples: creating a document that exists, deleting a required entity,
    /// invalid state transition.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// # let doc_id = "doc";
    /// StrataError::invalid_operation(
    ///     EntityRef::json(branch_id,"default", doc_id),
    ///     "Document already exists",
    /// );
    /// ```
    #[error("invalid operation on {entity_ref}: {reason}")]
    InvalidOperation {
        /// Reference to the entity (boxed — see `Conflict`).
        entity_ref: Box<EntityRef>,
        /// Why the operation is invalid
        reason: String,
    },

    /// Invalid input
    ///
    /// The input parameters are invalid. This is a validation error that
    /// cannot be fixed by retrying - the input must be corrected.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::invalid_input("Key cannot be empty");
    /// ```
    #[error("invalid input: {message}")]
    InvalidInput {
        /// Description of what's wrong with the input
        message: String,
    },

    /// Incompatible instance reuse
    ///
    /// The caller attempted to reuse an already-open database instance with
    /// a runtime signature that does not match the existing instance.
    #[error("incompatible reuse: {message}")]
    IncompatibleReuse {
        /// Description of the incompatibility.
        message: String,
    },

    /// Dimension mismatch (Vector-specific)
    ///
    /// The vector dimension doesn't match the collection's configured dimension.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::dimension_mismatch(384, 768);
    /// ```
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension
        expected: usize,
        /// Actual dimension provided
        got: usize,
    },

    /// Path not found (JSON-specific)
    ///
    /// The specified path doesn't exist in the JSON document.
    ///
    /// Wire code: `InvalidPath`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// # let doc_id = "doc";
    /// StrataError::PathNotFound {
    ///     entity_ref: Box::new(EntityRef::json(branch_id, "default", doc_id)),
    ///     path: "/data/items/0/name".to_string(),
    /// };
    /// ```
    #[error("path not found in {entity_ref}: {path}")]
    PathNotFound {
        /// Reference to the JSON document (boxed — see `Conflict`).
        entity_ref: Box<EntityRef>,
        /// The path that wasn't found
        path: String,
    },

    // =========================================================================
    // History Errors
    // =========================================================================
    /// History trimmed
    ///
    /// The requested version is no longer available because retention policy
    /// has removed it.
    ///
    /// Wire code: `HistoryTrimmed`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId, Version};
    /// # let branch_id = BranchId::new();
    /// StrataError::history_trimmed(
    ///     EntityRef::kv(branch_id,"default", "key"),
    ///     Version::Txn(100),
    ///     Version::Txn(150),
    /// );
    /// ```
    #[error(
        "history trimmed for {entity_ref}: requested {requested}, earliest is {earliest_retained}"
    )]
    HistoryTrimmed {
        /// Reference to the entity (boxed — see `Conflict`).
        entity_ref: Box<EntityRef>,
        /// The requested version
        requested: Version,
        /// The earliest version still retained
        earliest_retained: Version,
    },

    // =========================================================================
    // Storage Errors
    // =========================================================================
    /// Storage error
    ///
    /// Low-level storage operation failed.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::storage("Disk write failed");
    /// ```
    #[error("storage error: {message}")]
    Storage {
        /// Error message
        message: String,
        /// Optional underlying error
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Serialization error
    ///
    /// Failed to serialize or deserialize data.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::serialization("Invalid UTF-8 in key");
    /// ```
    #[error("serialization error: {message}")]
    Serialization {
        /// What went wrong
        message: String,
    },

    /// Corruption detected
    ///
    /// Data integrity check failed. This is a **serious** error that may
    /// require recovery from backup.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::Corruption {
    ///     message: "CRC mismatch in event log".to_string(),
    /// };
    /// ```
    #[error("corruption detected: {message}")]
    Corruption {
        /// Description of the corruption
        message: String,
    },

    /// Codec decode failure
    ///
    /// A codec (`aes-gcm-256`, ...) failed to decode a stored artifact.
    /// Typical triggers: wrong encryption key, AES-GCM auth-tag
    /// mismatch, truncated codec payload.
    ///
    /// **Staging note (T3-E12 Phase 1):** this variant is declared
    /// here for Phase 1's cross-crate typed-error scaffolding. No
    /// production code path constructs it yet. Phase 2 will produce
    /// it from the WAL read path and the recovery coordinator will
    /// map it to [`LossyErrorKind::CodecDecode`] when
    /// `allow_lossy_recovery` is active.
    ///
    /// Wire code: `StorageError`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::CodecDecode {
    ///     message: "AES-GCM authentication tag mismatch".to_string(),
    ///     source: None,
    /// };
    /// ```
    #[error("codec decode failure: {message}")]
    CodecDecode {
        /// Description of the decode failure.
        message: String,
        /// Optional underlying error. Phase 2 wires the installed
        /// `StorageCodec`'s `CodecError` through this field so
        /// callers can downcast to the codec-specific cause without
        /// string-matching `message`.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Legacy on-disk format
    ///
    /// A durability artifact on disk — WAL segment, MANIFEST, or snapshot
    /// — has a `format_version` older than this build supports. Hard fail
    /// — not a lossy-recoverable error. The operator must delete the
    /// offending artifact (`wal/` subdirectory, `MANIFEST` file, or
    /// `snap-*.chk` file) under the database path and reopen.
    ///
    /// The `hint` string carries the full operator-facing message,
    /// including the artifact kind and the required version (typical
    /// content: *"this build requires segment format version 3. Delete
    /// the `wal/` subdirectory and reopen."*). Keeping the artifact kind
    /// inside the hint means the `Display` prefix stays stable across
    /// future format bumps without needing a separate `artifact` struct
    /// field. D5 extended the producer set from WAL only to WAL +
    /// MANIFEST + snapshot.
    ///
    /// Wire code: `StorageError`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::LegacyFormat {
    ///     found_version: 2,
    ///     hint: "this build requires segment format version 3. \
    ///            Delete the `wal/` subdirectory and reopen.".to_string(),
    /// };
    /// ```
    #[error("legacy on-disk format: found version {found_version}. {hint}")]
    LegacyFormat {
        /// Format version read from disk.
        found_version: u32,
        /// Operator remediation hint — filesystem action only (no CLI
        /// tool is promised by this error variant). The hint is expected
        /// to name the affected artifact kind (WAL, MANIFEST, snapshot)
        /// and the required version number so the rendered diagnostic
        /// is self-contained.
        hint: String,
    },

    // =========================================================================
    // Resource Errors
    // =========================================================================
    /// Capacity exceeded
    ///
    /// A resource limit was exceeded.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::CapacityExceeded {
    ///     resource: "event log".to_string(),
    ///     limit: 1_000_000,
    ///     requested: 1_000_001,
    /// };
    /// ```
    #[error("capacity exceeded: {resource} (limit: {limit}, requested: {requested})")]
    CapacityExceeded {
        /// What resource was exceeded
        resource: String,
        /// The limit
        limit: usize,
        /// What was requested
        requested: usize,
    },

    /// Budget exceeded
    ///
    /// The operation exceeded its computational budget.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::BudgetExceeded {
    ///     operation: "vector search".to_string(),
    /// };
    /// ```
    #[error("budget exceeded: {operation}")]
    BudgetExceeded {
        /// What operation exceeded its budget
        operation: String,
    },

    // =========================================================================
    // Durability Errors
    // =========================================================================
    /// WAL writer is halted
    ///
    /// The WAL writer has halted due to a background sync failure (fsync error).
    /// The database cannot accept new commits until the underlying issue is
    /// resolved and `resume_wal_writer()` is called.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # use std::time::SystemTime;
    /// StrataError::WriterHalted {
    ///     reason: "disk full".to_string(),
    ///     first_observed_at: SystemTime::now(),
    /// };
    /// ```
    #[error("WAL writer halted: {reason} (first observed: {first_observed_at:?})")]
    WriterHalted {
        /// Human-readable reason for the halt
        reason: String,
        /// When the first sync failure was observed
        first_observed_at: std::time::SystemTime,
    },

    /// Durable but not visible
    ///
    /// The transaction was successfully written to the WAL (durable) but failed
    /// to apply to in-memory storage (not visible). The data **will be recovered**
    /// on restart.
    ///
    /// Callers should:
    /// 1. **Not retry** - the data is already durable
    /// 2. **Not assume visibility** - reads may not see the committed data
    /// 3. **Consider graceful shutdown** - to trigger recovery
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::DurableButNotVisible {
    ///     txn_id: 12345,
    ///     commit_version: 67890,
    /// };
    /// ```
    #[error("durable but not visible: txn {txn_id} at version {commit_version}")]
    DurableButNotVisible {
        /// Transaction ID that committed durably
        txn_id: u64,
        /// Commit version assigned to the transaction
        commit_version: u64,
    },

    /// Shutdown timeout
    ///
    /// The database shutdown timed out waiting for active transactions to complete.
    /// Freeze hooks were **not** run. The database is still usable; callers can
    /// either complete their transactions and retry shutdown, or abandon them.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::ShutdownTimeout { active_txn_count: 3 };
    /// ```
    #[error("shutdown timeout: {active_txn_count} transaction(s) still active")]
    ShutdownTimeout {
        /// Number of transactions that were still active when timeout occurred
        active_txn_count: u64,
    },

    // =========================================================================
    // Internal Errors
    // =========================================================================
    /// Internal error
    ///
    /// An unexpected internal error occurred. This is a **serious** error
    /// that indicates a bug in the system.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::internal("Unexpected state in transaction manager");
    /// ```
    #[error("internal error: {message}")]
    Internal {
        /// Error message
        message: String,
    },
}

const BRANCH_LINEAGE_UNAVAILABLE_PREFIX: &str = "branch_lineage_unavailable:";

impl StrataError {
    // =========================================================================
    // Constructors
    // =========================================================================

    /// Create a NotFound error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// StrataError::not_found(EntityRef::kv(branch_id,"default", "missing-key"));
    /// ```
    pub fn not_found(entity_ref: EntityRef) -> Self {
        StrataError::NotFound {
            entity_ref: Box::new(entity_ref),
        }
    }

    /// Create a BranchNotFound error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, BranchId};
    /// # let branch_id = BranchId::new();
    /// StrataError::branch_not_found(branch_id);
    /// ```
    pub fn branch_not_found(branch_id: BranchId) -> Self {
        StrataError::BranchNotFound { branch_id }
    }

    /// Create a BranchNotFound error that preserves the user-facing branch
    /// name while still carrying the canonical `BranchId`.
    pub fn branch_not_found_by_name(name: impl Into<String>) -> Self {
        let name = name.into();
        let branch_id = BranchId::from_user_name(&name);
        StrataError::BranchNotFoundByName { name, branch_id }
    }

    /// Create a `BranchArchived` error for the given user-facing name.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::branch_archived("release/2026-03");
    /// ```
    pub fn branch_archived(name: impl Into<String>) -> Self {
        StrataError::BranchArchived { name: name.into() }
    }

    /// Create a VersionConflict error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId, Version};
    /// # let branch_id = BranchId::new();
    /// StrataError::version_conflict(
    ///     EntityRef::kv(branch_id,"default", "counter"),
    ///     Version::Counter(5),
    ///     Version::Counter(6),
    /// );
    /// ```
    pub fn version_conflict(entity_ref: EntityRef, expected: Version, actual: Version) -> Self {
        StrataError::VersionConflict {
            entity_ref: Box::new(entity_ref),
            expected,
            actual,
        }
    }

    /// Create a WriteConflict error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// StrataError::write_conflict(EntityRef::kv(branch_id,"default", "shared-key"));
    /// ```
    pub fn write_conflict(entity_ref: EntityRef) -> Self {
        StrataError::WriteConflict {
            entity_ref: Box::new(entity_ref),
        }
    }

    /// Create a TransactionAborted error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::transaction_aborted("Conflict on key 'counter'");
    /// ```
    pub fn transaction_aborted(reason: impl Into<String>) -> Self {
        StrataError::TransactionAborted {
            reason: reason.into(),
        }
    }

    /// Create a TransactionTimeout error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::transaction_timeout(5000);
    /// ```
    pub fn transaction_timeout(duration_ms: u64) -> Self {
        StrataError::TransactionTimeout { duration_ms }
    }

    /// Create a WriteStallTimeout error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::write_stall_timeout(30000, 40);
    /// ```
    pub fn write_stall_timeout(stall_duration_ms: u64, l0_count: usize) -> Self {
        StrataError::WriteStallTimeout {
            stall_duration_ms,
            l0_count,
        }
    }

    /// Create a TransactionNotActive error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::transaction_not_active("committed");
    /// ```
    pub fn transaction_not_active(state: impl Into<String>) -> Self {
        StrataError::TransactionNotActive {
            state: state.into(),
        }
    }

    /// Create an InvalidOperation error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// # let doc_id = "doc";
    /// StrataError::invalid_operation(
    ///     EntityRef::json(branch_id,"default", doc_id),
    ///     "Document already exists",
    /// );
    /// ```
    pub fn invalid_operation(entity_ref: EntityRef, reason: impl Into<String>) -> Self {
        StrataError::InvalidOperation {
            entity_ref: Box::new(entity_ref),
            reason: reason.into(),
        }
    }

    /// Create an InvalidOperation that specifically marks branch-lineage
    /// reads as unavailable, while preserving the existing wire/error-code
    /// shape used for invalid operations.
    ///
    /// This gives callers a reliable predicate via
    /// [`StrataError::is_branch_lineage_unavailable`] without introducing a
    /// new public enum variant mid-tranche.
    pub fn branch_lineage_unavailable(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        StrataError::invalid_operation(
            EntityRef::branch(BranchId::from_bytes([0u8; 16])),
            format!("{BRANCH_LINEAGE_UNAVAILABLE_PREFIX}{reason}"),
        )
    }

    /// Create an InvalidInput error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::invalid_input("Key cannot be empty");
    /// ```
    pub fn invalid_input(message: impl Into<String>) -> Self {
        StrataError::InvalidInput {
            message: message.into(),
        }
    }

    /// Returns true when this error represents the dedicated
    /// branch-lineage-unavailable condition used during B3 follower legacy
    /// synthesis.
    pub fn is_branch_lineage_unavailable(&self) -> bool {
        matches!(
            self,
            StrataError::InvalidOperation { reason, .. }
                if reason.starts_with(BRANCH_LINEAGE_UNAVAILABLE_PREFIX)
        )
    }

    /// Create an IncompatibleReuse error
    pub fn incompatible_reuse(message: impl Into<String>) -> Self {
        StrataError::IncompatibleReuse {
            message: message.into(),
        }
    }

    /// Create a DimensionMismatch error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::dimension_mismatch(384, 768);
    /// ```
    pub fn dimension_mismatch(expected: usize, got: usize) -> Self {
        StrataError::DimensionMismatch { expected, got }
    }

    /// Create a PathNotFound error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// # let doc_id = "doc";
    /// StrataError::path_not_found(
    ///     EntityRef::json(branch_id,"default", doc_id),
    ///     "/data/items/0",
    /// );
    /// ```
    pub fn path_not_found(entity_ref: EntityRef, path: impl Into<String>) -> Self {
        StrataError::PathNotFound {
            entity_ref: Box::new(entity_ref),
            path: path.into(),
        }
    }

    /// Create a HistoryTrimmed error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId, Version};
    /// # let branch_id = BranchId::new();
    /// StrataError::history_trimmed(
    ///     EntityRef::kv(branch_id,"default", "key"),
    ///     Version::Txn(100),
    ///     Version::Txn(150),
    /// );
    /// ```
    pub fn history_trimmed(
        entity_ref: EntityRef,
        requested: Version,
        earliest_retained: Version,
    ) -> Self {
        StrataError::HistoryTrimmed {
            entity_ref: Box::new(entity_ref),
            requested,
            earliest_retained,
        }
    }

    /// Create a Storage error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::storage("Disk write failed");
    /// ```
    pub fn storage(message: impl Into<String>) -> Self {
        StrataError::Storage {
            message: message.into(),
            source: None,
        }
    }

    /// Create a Storage error with source
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let io_error = std::io::Error::new(std::io::ErrorKind::Other, "test");
    /// StrataError::storage_with_source("Failed to write", io_error);
    /// ```
    pub fn storage_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        StrataError::Storage {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create a Serialization error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::serialization("Invalid UTF-8 in key");
    /// ```
    pub fn serialization(message: impl Into<String>) -> Self {
        StrataError::Serialization {
            message: message.into(),
        }
    }

    /// Create a Corruption error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::corruption("CRC mismatch");
    /// ```
    pub fn corruption(message: impl Into<String>) -> Self {
        StrataError::Corruption {
            message: message.into(),
        }
    }

    /// Create a CodecDecode error without a typed source.
    ///
    /// Use this when an installed `StorageCodec` returned an error
    /// decoding a stored artifact — typically a wrong encryption key
    /// or AES-GCM authentication-tag failure on the WAL read path.
    /// Callers that have access to the underlying `CodecError` should
    /// prefer [`StrataError::codec_decode_with_source`] so
    /// downstream consumers can downcast on the typed source.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::codec_decode("AES-GCM auth tag mismatch");
    /// ```
    pub fn codec_decode(message: impl Into<String>) -> Self {
        StrataError::CodecDecode {
            message: message.into(),
            source: None,
        }
    }

    /// Create a CodecDecode error with a typed source.
    ///
    /// Phase 2 of T3-E12 uses this to wire the installed codec's
    /// `CodecError` through to the engine boundary for typed
    /// downcasting on `StrataError::source()`.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let io_error = std::io::Error::new(std::io::ErrorKind::Other, "auth tag");
    /// StrataError::codec_decode_with_source("AES-GCM auth tag mismatch", io_error);
    /// ```
    pub fn codec_decode_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        StrataError::CodecDecode {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create a `LegacyFormat` error.
    ///
    /// `hint` should name the operator remediation **and** the
    /// required segment format version, since the constructor does
    /// not carry `required_version` as a separate field (the hint is
    /// the stable diagnostic surface across version bumps). A typical
    /// hint: *"this build requires version 3. Delete the `wal/`
    /// subdirectory and reopen."*
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::legacy_format(
    ///     2,
    ///     "this build requires version 3. Delete the `wal/` subdirectory and reopen.",
    /// );
    /// ```
    pub fn legacy_format(found_version: u32, hint: impl Into<String>) -> Self {
        StrataError::LegacyFormat {
            found_version,
            hint: hint.into(),
        }
    }

    /// Create a CapacityExceeded error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::capacity_exceeded("event log", 1_000_000, 1_000_001);
    /// ```
    pub fn capacity_exceeded(resource: impl Into<String>, limit: usize, requested: usize) -> Self {
        StrataError::CapacityExceeded {
            resource: resource.into(),
            limit,
            requested,
        }
    }

    /// Create a BudgetExceeded error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::budget_exceeded("vector search");
    /// ```
    pub fn budget_exceeded(operation: impl Into<String>) -> Self {
        StrataError::BudgetExceeded {
            operation: operation.into(),
        }
    }

    /// Create an Internal error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::internal("Unexpected state");
    /// ```
    pub fn internal(message: impl Into<String>) -> Self {
        StrataError::Internal {
            message: message.into(),
        }
    }

    /// Create a WrongType error
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::wrong_type("Int", "String");
    /// ```
    pub fn wrong_type(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        StrataError::WrongType {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Create a Conflict error (generic temporal failure)
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// StrataError::conflict("Version mismatch on key 'counter'");
    /// ```
    pub fn conflict(reason: impl Into<String>) -> Self {
        StrataError::Conflict {
            reason: reason.into(),
            entity_ref: None,
            transaction_id: None,
        }
    }

    /// Create a Conflict error with entity reference
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, EntityRef, BranchId};
    /// # let branch_id = BranchId::new();
    /// StrataError::conflict_on(EntityRef::kv(branch_id,"default", "key"), "Version mismatch");
    /// ```
    pub fn conflict_on(entity_ref: EntityRef, reason: impl Into<String>) -> Self {
        StrataError::Conflict {
            reason: reason.into(),
            entity_ref: Some(Box::new(entity_ref)),
            transaction_id: None,
        }
    }

    /// Create a Conflict error with transaction ID
    ///
    /// Use this when a conflict can be attributed to a specific transaction,
    /// enabling clients to correlate conflicts with their transaction IDs.
    pub fn conflict_with_txn(reason: impl Into<String>, transaction_id: u64) -> Self {
        StrataError::Conflict {
            reason: reason.into(),
            entity_ref: None,
            transaction_id: Some(transaction_id),
        }
    }

    // =========================================================================
    // Wire Encoding Methods
    // =========================================================================

    /// Get the canonical error code for wire encoding
    ///
    /// This maps the error variant to one of the 10 canonical error codes.
    pub fn code(&self) -> ErrorCode {
        match self {
            // NotFound errors
            StrataError::NotFound { .. } => ErrorCode::NotFound,
            StrataError::BranchNotFound { .. } | StrataError::BranchNotFoundByName { .. } => {
                ErrorCode::NotFound
            }

            // Lifecycle-state errors (B4): branch exists but is not writable.
            StrataError::BranchArchived { .. } => ErrorCode::ConstraintViolation,

            // WrongType errors
            StrataError::WrongType { .. } => ErrorCode::WrongType,

            // Conflict errors (temporal failures - retryable)
            StrataError::Conflict { .. } => ErrorCode::Conflict,
            StrataError::VersionConflict { .. } => ErrorCode::Conflict,
            StrataError::WriteConflict { .. } => ErrorCode::Conflict,
            StrataError::TransactionAborted { .. } => ErrorCode::Conflict,
            StrataError::TransactionTimeout { .. } => ErrorCode::Conflict,
            StrataError::WriteStallTimeout { .. } => ErrorCode::Conflict,
            StrataError::TransactionNotActive { .. } => ErrorCode::Conflict,

            // ConstraintViolation errors (structural failures)
            StrataError::InvalidOperation { .. } => ErrorCode::ConstraintViolation,
            StrataError::InvalidInput { .. } => ErrorCode::ConstraintViolation,
            StrataError::IncompatibleReuse { .. } => ErrorCode::ConstraintViolation,
            StrataError::DimensionMismatch { .. } => ErrorCode::ConstraintViolation,
            StrataError::CapacityExceeded { .. } => ErrorCode::ConstraintViolation,
            StrataError::BudgetExceeded { .. } => ErrorCode::ConstraintViolation,

            // Path errors
            StrataError::PathNotFound { .. } => ErrorCode::InvalidPath,

            // History errors
            StrataError::HistoryTrimmed { .. } => ErrorCode::HistoryTrimmed,

            // Storage errors
            StrataError::Storage { .. } => ErrorCode::StorageError,
            StrataError::Serialization { .. } => ErrorCode::SerializationError,
            StrataError::Corruption { .. } => ErrorCode::StorageError,
            StrataError::CodecDecode { .. } => ErrorCode::StorageError,
            StrataError::LegacyFormat { .. } => ErrorCode::StorageError,

            // Internal errors
            StrataError::Internal { .. } => ErrorCode::InternalError,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(target: "strata::error::exhaustive", "Unknown error variant in code()");
                ErrorCode::InternalError
            }
        }
    }

    /// Get the error message for wire encoding
    ///
    /// This returns the human-readable error message.
    pub fn message(&self) -> String {
        self.to_string()
    }

    /// Get the structured error details for wire encoding
    ///
    /// This returns key-value details about the error that can be
    /// serialized to JSON.
    pub fn details(&self) -> ErrorDetails {
        match self {
            StrataError::NotFound { entity_ref } => {
                ErrorDetails::new().with_string("entity", entity_ref.to_string())
            }
            StrataError::BranchNotFound { branch_id } => {
                ErrorDetails::new().with_string("branch_id", branch_id.to_string())
            }
            StrataError::BranchNotFoundByName { name, branch_id } => ErrorDetails::new()
                .with_string("branch", name)
                .with_string("branch_id", branch_id.to_string()),
            StrataError::BranchArchived { name } => ErrorDetails::new().with_string("branch", name),
            StrataError::WrongType { expected, actual } => ErrorDetails::new()
                .with_string("expected", expected)
                .with_string("actual", actual),
            StrataError::Conflict {
                reason,
                entity_ref,
                transaction_id,
            } => {
                let mut details = ErrorDetails::new().with_string("reason", reason);
                if let Some(ref e) = entity_ref {
                    details = details.with_string("entity", e.to_string());
                }
                if let Some(txn_id) = transaction_id {
                    details = details.with_int("transaction_id", *txn_id as i64);
                }
                details
            }
            StrataError::VersionConflict {
                entity_ref,
                expected,
                actual,
            } => ErrorDetails::new()
                .with_string("entity", entity_ref.to_string())
                .with_string("expected", expected.to_string())
                .with_string("actual", actual.to_string()),
            StrataError::WriteConflict { entity_ref } => {
                ErrorDetails::new().with_string("entity", entity_ref.to_string())
            }
            StrataError::TransactionAborted { reason } => {
                ErrorDetails::new().with_string("reason", reason)
            }
            StrataError::TransactionTimeout { duration_ms } => {
                ErrorDetails::new().with_int("duration_ms", *duration_ms as i64)
            }
            StrataError::WriteStallTimeout {
                stall_duration_ms,
                l0_count,
            } => ErrorDetails::new()
                .with_int("stall_duration_ms", *stall_duration_ms as i64)
                .with_int("l0_count", *l0_count as i64),
            StrataError::TransactionNotActive { state } => {
                ErrorDetails::new().with_string("state", state)
            }
            StrataError::InvalidOperation { entity_ref, reason } => ErrorDetails::new()
                .with_string("entity", entity_ref.to_string())
                .with_string("reason", reason),
            StrataError::InvalidInput { message } => {
                ErrorDetails::new().with_string("message", message)
            }
            StrataError::IncompatibleReuse { message } => {
                ErrorDetails::new().with_string("message", message)
            }
            StrataError::DimensionMismatch { expected, got } => ErrorDetails::new()
                .with_int("expected", *expected as i64)
                .with_int("got", *got as i64),
            StrataError::PathNotFound { entity_ref, path } => ErrorDetails::new()
                .with_string("entity", entity_ref.to_string())
                .with_string("path", path),
            StrataError::HistoryTrimmed {
                entity_ref,
                requested,
                earliest_retained,
            } => ErrorDetails::new()
                .with_string("entity", entity_ref.to_string())
                .with_string("requested", requested.to_string())
                .with_string("earliest_retained", earliest_retained.to_string()),
            StrataError::Storage { message, .. } => {
                ErrorDetails::new().with_string("message", message)
            }
            StrataError::Serialization { message } => {
                ErrorDetails::new().with_string("message", message)
            }
            StrataError::Corruption { message } => {
                ErrorDetails::new().with_string("message", message)
            }
            StrataError::CodecDecode { message, .. } => {
                ErrorDetails::new().with_string("message", message)
            }
            StrataError::LegacyFormat {
                found_version,
                hint,
            } => ErrorDetails::new()
                .with_int("found_version", i64::from(*found_version))
                .with_string("hint", hint),
            StrataError::CapacityExceeded {
                resource,
                limit,
                requested,
            } => ErrorDetails::new()
                .with_string("resource", resource)
                .with_int("limit", *limit as i64)
                .with_int("requested", *requested as i64),
            StrataError::BudgetExceeded { operation } => {
                ErrorDetails::new().with_string("operation", operation)
            }
            StrataError::Internal { message } => {
                ErrorDetails::new().with_string("message", message)
            }

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(target: "strata::error::exhaustive", "Unknown error variant in details()");
                ErrorDetails::new()
            }
        }
    }

    // =========================================================================
    // Classification Methods
    // =========================================================================

    /// Check if this is a "not found" type error
    ///
    /// Returns true for: `NotFound`, `BranchNotFound`, `PathNotFound`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if error.is_not_found() {
    ///     // Handle missing entity
    /// }
    /// ```
    pub fn is_not_found(&self) -> bool {
        match self {
            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::PathNotFound { .. } => true,

            StrataError::BranchArchived { .. }
            | StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::WrongType { .. }
            | StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_not_found()"
                );
                false
            }
        }
    }

    /// Check if this is a conflict error (temporal failure)
    ///
    /// Returns true for: `Conflict`, `VersionConflict`, `WriteConflict`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if error.is_conflict() {
    ///     // Retry with fresh data
    /// }
    /// ```
    pub fn is_conflict(&self) -> bool {
        match self {
            StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::WrongType { .. }
            | StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_conflict()"
                );
                false
            }
        }
    }

    /// Check if this is a wrong type error
    ///
    /// Returns true for: `WrongType`
    pub fn is_wrong_type(&self) -> bool {
        match self {
            StrataError::WrongType { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_wrong_type()"
                );
                false
            }
        }
    }

    /// Check if this is a transaction error
    ///
    /// Returns true for: `TransactionAborted`, `TransactionTimeout`, `TransactionNotActive`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if error.is_transaction_error() {
    ///     // Handle transaction failure
    /// }
    /// ```
    pub fn is_transaction_error(&self) -> bool {
        match self {
            StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::WrongType { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_transaction_error()"
                );
                false
            }
        }
    }

    /// Check if this is a validation error
    ///
    /// Returns true for: `InvalidOperation`, `InvalidInput`, `DimensionMismatch`
    ///
    /// Validation errors indicate bad input - don't retry, fix the input.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if error.is_validation_error() {
    ///     // Report to user, don't retry
    /// }
    /// ```
    pub fn is_validation_error(&self) -> bool {
        match self {
            StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::WrongType { .. }
            | StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_validation_error()"
                );
                false
            }
        }
    }

    /// Check if this is a storage error
    ///
    /// Returns true for: `Storage`, `Serialization`, `Corruption`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if error.is_storage_error() {
    ///     // Check disk/IO
    /// }
    /// ```
    pub fn is_storage_error(&self) -> bool {
        match self {
            StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::WrongType { .. }
            | StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_storage_error()"
                );
                false
            }
        }
    }

    /// Check if this error is retryable
    ///
    /// Retryable errors may succeed on retry:
    /// - `Conflict`: Generic conflict, retry with fresh data
    /// - `VersionConflict`: Re-read current version and retry
    /// - `WriteConflict`: Retry the transaction
    /// - `TransactionAborted`: Retry the transaction
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::{StrataError, StrataResult};
    /// # fn operation() -> StrataResult<String> { Ok("ok".to_string()) }
    /// # fn example() -> StrataResult<String> {
    /// loop {
    ///     match operation() {
    ///         Ok(result) => return Ok(result),
    ///         Err(e) if e.is_retryable() => continue,
    ///         Err(e) => return Err(e),
    ///     }
    /// }
    /// # }
    /// ```
    pub fn is_retryable(&self) -> bool {
        match self {
            StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::TransactionAborted { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::WrongType { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_retryable()"
                );
                false
            }
        }
    }

    /// Check if this is a serious/unrecoverable error
    ///
    /// Serious errors indicate potential data corruption or bugs:
    /// - `Corruption`: Data integrity failure
    /// - `Internal`: Unexpected system state (bug)
    ///
    /// These should be logged, alerted, and investigated.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if error.is_serious() {
    ///     eprintln!("SERIOUS ERROR: {}", error);
    ///     // alert_oncall();
    /// }
    /// ```
    pub fn is_serious(&self) -> bool {
        match self {
            StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::Internal { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::WrongType { .. }
            | StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::CapacityExceeded { .. }
            | StrataError::BudgetExceeded { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_serious()"
                );
                false
            }
        }
    }

    /// Check if this is a resource error
    ///
    /// Returns true for: `CapacityExceeded`, `BudgetExceeded`
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if error.is_resource_error() {
    ///     // Reduce batch size or implement backpressure
    /// }
    /// ```
    pub fn is_resource_error(&self) -> bool {
        match self {
            StrataError::CapacityExceeded { .. } | StrataError::BudgetExceeded { .. } => true,

            StrataError::NotFound { .. }
            | StrataError::BranchNotFound { .. }
            | StrataError::BranchNotFoundByName { .. }
            | StrataError::BranchArchived { .. }
            | StrataError::PathNotFound { .. }
            | StrataError::Conflict { .. }
            | StrataError::VersionConflict { .. }
            | StrataError::WriteConflict { .. }
            | StrataError::WrongType { .. }
            | StrataError::TransactionAborted { .. }
            | StrataError::TransactionTimeout { .. }
            | StrataError::TransactionNotActive { .. }
            | StrataError::InvalidOperation { .. }
            | StrataError::InvalidInput { .. }
            | StrataError::IncompatibleReuse { .. }
            | StrataError::DimensionMismatch { .. }
            | StrataError::Storage { .. }
            | StrataError::Serialization { .. }
            | StrataError::Corruption { .. }
            | StrataError::CodecDecode { .. }
            | StrataError::LegacyFormat { .. }
            | StrataError::Internal { .. } => false,

            // Catch-all for future variants (due to #[non_exhaustive])
            #[allow(unreachable_patterns)]
            _ => {
                tracing::warn!(
                    target: "strata::error::exhaustive",
                    "unmapped StrataError variant in is_resource_error()"
                );
                false
            }
        }
    }

    /// Get the entity reference if this error is about a specific entity
    ///
    /// Returns `Some(&EntityRef)` for errors that reference an entity:
    /// - `NotFound`
    /// - `VersionConflict`
    /// - `WriteConflict`
    /// - `InvalidOperation`
    /// - `PathNotFound`
    ///
    /// Returns `None` for errors without entity context.
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if let Some(entity) = error.entity_ref() {
    ///     println!("Error on entity: {}", entity);
    /// }
    /// ```
    pub fn entity_ref(&self) -> Option<&EntityRef> {
        match self {
            StrataError::NotFound { entity_ref } => Some(entity_ref.as_ref()),
            StrataError::Conflict {
                entity_ref: Some(e),
                ..
            } => Some(e.as_ref()),
            StrataError::VersionConflict { entity_ref, .. } => Some(entity_ref.as_ref()),
            StrataError::WriteConflict { entity_ref } => Some(entity_ref.as_ref()),
            StrataError::InvalidOperation { entity_ref, .. } => Some(entity_ref.as_ref()),
            StrataError::PathNotFound { entity_ref, .. } => Some(entity_ref.as_ref()),
            _ => None,
        }
    }

    /// Get the branch ID if this error is about a specific branch
    ///
    /// Returns the BranchId from:
    /// - `BranchNotFound`: The missing branch
    /// - Entity-related errors: The branch from the EntityRef
    ///
    /// ## Example
    /// ```no_run
    /// # use strata_core::StrataError;
    /// # let error = StrataError::internal("test");
    /// if let Some(branch_id) = error.branch_id() {
    ///     println!("Error in branch: {}", branch_id);
    /// }
    /// ```
    pub fn branch_id(&self) -> Option<BranchId> {
        match self {
            StrataError::BranchNotFound { branch_id }
            | StrataError::BranchNotFoundByName { branch_id, .. } => Some(*branch_id),
            _ => self.entity_ref().map(|e| e.branch_id()),
        }
    }
}

// =============================================================================
// StrataResult Type Alias
// =============================================================================

/// Result type alias for Strata operations
///
/// All Strata API methods return `StrataResult<T>`.
///
/// ## Example
/// ```no_run
/// # use strata_core::{StrataError, StrataResult, EntityRef, BranchId};
/// fn get_value(branch_id: BranchId, key: &str) -> StrataResult<String> {
///     // Look up the key; return error if not found
///     # let value: Option<String> = None;
///     match value {
///         Some(v) => Ok(v),
///         None => Err(StrataError::not_found(EntityRef::kv(branch_id,"default", key))),
///     }
/// }
/// ```
pub type StrataResult<T> = std::result::Result<T, StrataError>;

// =============================================================================
// Conversions from Standard Library Types
// =============================================================================

impl From<io::Error> for StrataError {
    fn from(e: io::Error) -> Self {
        StrataError::Storage {
            message: format!("IO error: {}", e),
            source: Some(Box::new(e)),
        }
    }
}

impl From<bincode::Error> for StrataError {
    fn from(e: bincode::Error) -> Self {
        StrataError::Serialization {
            message: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for StrataError {
    fn from(e: serde_json::Error) -> Self {
        StrataError::Serialization {
            message: format!("JSON error: {}", e),
        }
    }
}

// =============================================================================
// StrataError Tests
// =============================================================================

#[cfg(test)]
mod strata_error_tests {
    use super::*;

    // === Constructor Tests ===

    #[test]
    fn test_not_found_constructor() {
        let branch_id = BranchId::new();
        let e = StrataError::not_found(EntityRef::kv(branch_id, "default", "key"));

        assert!(e.is_not_found());
        assert!(!e.is_conflict());
        assert!(!e.is_retryable());
        assert!(!e.is_serious());
        assert!(e.entity_ref().is_some());
    }

    #[test]
    fn test_branch_not_found_constructor() {
        let branch_id = BranchId::new();
        let e = StrataError::branch_not_found(branch_id);

        assert!(e.is_not_found());
        assert!(!e.is_conflict());
        assert_eq!(e.branch_id(), Some(branch_id));
        assert!(e.entity_ref().is_none());
    }

    #[test]
    fn test_branch_not_found_by_name_constructor() {
        let e = StrataError::branch_not_found_by_name("release/2026-03");

        assert!(e.is_not_found());
        assert!(!e.is_conflict());
        assert_eq!(
            e.branch_id(),
            Some(BranchId::from_user_name("release/2026-03"))
        );
        assert!(e.entity_ref().is_none());
        assert_eq!(e.to_string(), "branch not found: release/2026-03");
    }

    #[test]
    fn test_branch_archived_classifier() {
        // B4: archived is a distinct lifecycle-state error, not a
        // not-found, not a conflict, not retryable, not serious. Wire
        // code is `ConstraintViolation` — the caller violated the
        // archived-branch read-only contract.
        let e = StrataError::branch_archived("release/2026-03");

        assert!(!e.is_not_found());
        assert!(!e.is_conflict());
        assert!(!e.is_wrong_type());
        assert!(!e.is_transaction_error());
        assert!(!e.is_validation_error());
        assert!(!e.is_storage_error());
        assert!(!e.is_retryable());
        assert!(!e.is_serious());
        assert!(!e.is_resource_error());
        assert_eq!(e.code(), ErrorCode::ConstraintViolation);
        assert!(format!("{e}").contains("release/2026-03"));
    }

    #[test]
    fn test_version_conflict_constructor() {
        let branch_id = BranchId::new();
        let e = StrataError::version_conflict(
            EntityRef::kv(branch_id, "default", "counter"),
            Version::Counter(5),
            Version::Counter(6),
        );

        assert!(e.is_conflict());
        assert!(e.is_retryable());
        assert!(!e.is_not_found());
        assert!(!e.is_serious());
        assert!(e.entity_ref().is_some());
    }

    #[test]
    fn test_write_conflict_constructor() {
        let branch_id = BranchId::new();
        let e = StrataError::write_conflict(EntityRef::kv(branch_id, "default", "shared-key"));

        assert!(e.is_conflict());
        assert!(e.is_retryable());
    }

    #[test]
    fn test_transaction_aborted_constructor() {
        let e = StrataError::transaction_aborted("Conflict on key");

        assert!(e.is_transaction_error());
        assert!(e.is_retryable());
        assert!(!e.is_conflict());
    }

    #[test]
    fn test_transaction_timeout_constructor() {
        let e = StrataError::transaction_timeout(5000);

        assert!(e.is_transaction_error());
        assert!(!e.is_retryable());
        match e {
            StrataError::TransactionTimeout { duration_ms } => assert_eq!(duration_ms, 5000),
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_write_stall_timeout_constructor() {
        let e = StrataError::write_stall_timeout(30000, 40);

        assert!(
            !e.is_retryable(),
            "stall timeout is not retryable — write already committed"
        );
        assert!(!e.is_transaction_error());
        assert_eq!(e.code(), ErrorCode::Conflict);
        match e {
            StrataError::WriteStallTimeout {
                stall_duration_ms,
                l0_count,
            } => {
                assert_eq!(stall_duration_ms, 30000);
                assert_eq!(l0_count, 40);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_transaction_not_active_constructor() {
        let e = StrataError::transaction_not_active("committed");

        assert!(e.is_transaction_error());
        assert!(!e.is_retryable());
    }

    #[test]
    fn test_invalid_operation_constructor() {
        let branch_id = BranchId::new();
        let e = StrataError::invalid_operation(
            EntityRef::json(branch_id, "default", "test-doc"),
            "Document already exists",
        );

        assert!(e.is_validation_error());
        assert!(!e.is_retryable());
        assert!(e.entity_ref().is_some());
    }

    #[test]
    fn test_invalid_input_constructor() {
        let e = StrataError::invalid_input("Key cannot be empty");

        assert!(e.is_validation_error());
        assert!(!e.is_retryable());
        assert!(e.entity_ref().is_none());
    }

    #[test]
    fn test_dimension_mismatch_constructor() {
        let e = StrataError::dimension_mismatch(384, 768);

        assert!(e.is_validation_error());
        match e {
            StrataError::DimensionMismatch { expected, got } => {
                assert_eq!(expected, 384);
                assert_eq!(got, 768);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_path_not_found_constructor() {
        let branch_id = BranchId::new();
        let e = StrataError::path_not_found(
            EntityRef::json(branch_id, "default", "test-doc"),
            "/data/items/0",
        );

        assert!(e.is_not_found());
        assert!(e.entity_ref().is_some());
    }

    #[test]
    fn test_storage_constructor() {
        let e = StrataError::storage("Disk write failed");

        assert!(e.is_storage_error());
        assert!(!e.is_serious());
    }

    #[test]
    fn test_storage_with_source_constructor() {
        let io_err = io::Error::new(io::ErrorKind::Other, "disk full");
        let e = StrataError::storage_with_source("Write failed", io_err);

        assert!(e.is_storage_error());
        match e {
            StrataError::Storage { source, .. } => assert!(source.is_some()),
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_storage_with_source_verifies_content() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file missing");
        let e = StrataError::storage_with_source("Read failed", io_err);

        match &e {
            StrataError::Storage { message, source } => {
                // Verify the message is preserved
                assert_eq!(message, "Read failed");

                // Verify the source error content
                let source_err = source.as_ref().expect("Should have source");
                let source_msg = format!("{}", source_err);
                assert!(
                    source_msg.contains("file missing"),
                    "Source error should contain original message, got: {}",
                    source_msg
                );
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_storage_with_source_preserves_error_message() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let e = StrataError::storage_with_source("Cannot write", io_err);

        match &e {
            StrataError::Storage { source, .. } => {
                let source_err = source.as_ref().expect("Should have source");
                // Verify the source error message is preserved
                let source_display = format!("{}", source_err);
                assert!(
                    source_display.contains("access denied"),
                    "Source error should contain original message, got: {}",
                    source_display
                );
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_error_display_includes_source() {
        let io_err = io::Error::new(io::ErrorKind::Other, "underlying cause");
        let e = StrataError::storage_with_source("Operation failed", io_err);

        let display = format!("{}", e);
        // The display should mention the operation
        assert!(
            display.contains("Operation failed"),
            "Display should include main message, got: {}",
            display
        );
    }

    #[test]
    fn test_from_io_error_source_chain() {
        use std::error::Error;

        let io_err = io::Error::new(io::ErrorKind::BrokenPipe, "connection lost");
        let strata_err: StrataError = io_err.into();

        // Verify conversion works
        assert!(strata_err.is_storage_error());

        // Verify we can get the source back via Error trait
        let source = strata_err.source();
        assert!(source.is_some(), "Should have source error");

        let source_display = format!("{}", source.unwrap());
        assert!(
            source_display.contains("connection lost"),
            "Source should contain original message"
        );
    }

    #[test]
    fn test_serialization_constructor() {
        let e = StrataError::serialization("Invalid UTF-8");

        assert!(e.is_storage_error());
    }

    #[test]
    fn test_corruption_constructor() {
        let e = StrataError::corruption("CRC mismatch");

        assert!(e.is_storage_error());
        assert!(e.is_serious());
    }

    #[test]
    fn test_codec_decode_constructor() {
        let e = StrataError::codec_decode("AES-GCM auth tag mismatch");

        assert!(matches!(
            &e,
            StrataError::CodecDecode { message, source: None } if message == "AES-GCM auth tag mismatch"
        ));
        assert_eq!(e.code(), ErrorCode::StorageError);
        assert!(e.is_storage_error());
        assert!(e.is_serious());
        assert!(!e.is_retryable());
        assert!(!e.is_resource_error());
        // No typed source on the plain constructor — callers wire
        // sources via `codec_decode_with_source` (below).
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn test_codec_decode_with_source_constructor() {
        // Phase 2 of T3-E12 will pass the installed codec's
        // `CodecError` through the `source` field so downstream
        // callers can downcast without string-matching `message`.
        // Use `io::Error` as a stand-in source here so this test
        // does not take a dependency on `strata-durability`.
        let io_err = std::io::Error::new(std::io::ErrorKind::InvalidData, "auth tag mismatch");
        let e = StrataError::codec_decode_with_source("codec rejected payload", io_err);

        assert!(matches!(
            &e,
            StrataError::CodecDecode {
                source: Some(_),
                ..
            }
        ));
        assert_eq!(e.code(), ErrorCode::StorageError);
        assert!(e.is_storage_error());
        assert!(e.is_serious());
        // Source is wired through `thiserror`'s `#[source]` attribute
        // and reachable via `std::error::Error::source`.
        let wired_source =
            std::error::Error::source(&e).expect("codec_decode_with_source must install a source");
        assert!(
            wired_source.to_string().contains("auth tag mismatch"),
            "source rendering should carry the original io::Error message, got: {wired_source}"
        );
    }

    #[test]
    fn test_legacy_format_constructor() {
        let e = StrataError::legacy_format(
            2,
            "this build requires version 3. Delete the `wal/` subdirectory and reopen.",
        );

        assert!(matches!(
            &e,
            StrataError::LegacyFormat {
                found_version: 2,
                hint,
            } if hint.contains("requires version 3") && hint.contains("wal/")
        ));
        assert_eq!(e.code(), ErrorCode::StorageError);
        assert!(e.is_storage_error());
        assert!(e.is_serious());
        assert!(!e.is_retryable());
        assert!(!e.is_resource_error());
        // `found_version` renders in the Display impl so operators see
        // the actual on-disk version in logs without structured access.
        // The required version is expected to be in the hint, not a
        // separate struct field — see the variant rustdoc.
        let rendered = e.to_string();
        assert!(
            rendered.contains("found version 2"),
            "Display should include found_version, got: {rendered}"
        );
        assert!(
            rendered.contains("requires version 3"),
            "hint (which carries the required version) should render, got: {rendered}"
        );
    }

    #[test]
    fn test_capacity_exceeded_constructor() {
        let e = StrataError::capacity_exceeded("event log", 1_000_000, 1_000_001);

        assert!(e.is_resource_error());
        match e {
            StrataError::CapacityExceeded {
                resource,
                limit,
                requested,
            } => {
                assert_eq!(resource, "event log");
                assert_eq!(limit, 1_000_000);
                assert_eq!(requested, 1_000_001);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_budget_exceeded_constructor() {
        let e = StrataError::budget_exceeded("vector search");

        assert!(e.is_resource_error());
    }

    #[test]
    fn test_internal_constructor() {
        let e = StrataError::internal("Unexpected state");

        assert!(e.is_serious());
        assert!(!e.is_retryable());
    }

    // === Classification Tests ===

    #[test]
    fn test_is_retryable() {
        let branch_id = BranchId::new();

        // Retryable
        assert!(StrataError::version_conflict(
            EntityRef::kv(branch_id, "default", "k"),
            Version::Txn(1),
            Version::Txn(2),
        )
        .is_retryable());
        assert!(
            StrataError::write_conflict(EntityRef::kv(branch_id, "default", "k")).is_retryable()
        );
        assert!(StrataError::transaction_aborted("conflict").is_retryable());

        // Not retryable
        assert!(!StrataError::not_found(EntityRef::kv(branch_id, "default", "k")).is_retryable());
        assert!(!StrataError::branch_not_found(branch_id).is_retryable());
        assert!(!StrataError::invalid_input("bad").is_retryable());
        assert!(!StrataError::transaction_timeout(1000).is_retryable());
        assert!(!StrataError::internal("bug").is_retryable());
        assert!(!StrataError::corruption("bad").is_retryable());
    }

    #[test]
    fn test_is_serious() {
        assert!(StrataError::corruption("CRC mismatch").is_serious());
        assert!(StrataError::internal("unexpected state").is_serious());

        let branch_id = BranchId::new();
        assert!(!StrataError::not_found(EntityRef::kv(branch_id, "default", "k")).is_serious());
        assert!(!StrataError::storage("disk full").is_serious());
    }

    // === Display Tests ===

    #[test]
    fn test_error_display_not_found() {
        let branch_id = BranchId::new();
        let e = StrataError::not_found(EntityRef::kv(branch_id, "default", "config"));
        let msg = e.to_string();

        assert!(msg.contains("not found"));
        assert!(msg.contains("config"));
    }

    #[test]
    fn test_error_display_version_conflict() {
        let branch_id = BranchId::new();
        let e = StrataError::version_conflict(
            EntityRef::kv(branch_id, "default", "counter"),
            Version::Counter(5),
            Version::Counter(6),
        );
        let msg = e.to_string();

        assert!(msg.contains("version conflict"));
        assert!(msg.contains("cnt:5"));
        assert!(msg.contains("cnt:6"));
    }

    #[test]
    fn test_error_display_transaction_timeout() {
        let e = StrataError::transaction_timeout(5000);
        let msg = e.to_string();

        assert!(msg.contains("timeout"));
        assert!(msg.contains("5000"));
    }

    // === Entity Ref Accessor Tests ===

    #[test]
    fn test_entity_ref_accessor() {
        let branch_id = BranchId::new();
        let entity_ref = EntityRef::kv(branch_id, "default", "key");

        let e = StrataError::not_found(entity_ref.clone());
        assert_eq!(e.entity_ref(), Some(&entity_ref));

        let e = StrataError::storage("disk full");
        assert_eq!(e.entity_ref(), None);

        let e = StrataError::branch_not_found(branch_id);
        assert_eq!(e.entity_ref(), None);
    }

    #[test]
    fn test_branch_id_accessor() {
        let branch_id = BranchId::new();

        // From BranchNotFound
        let e = StrataError::branch_not_found(branch_id);
        assert_eq!(e.branch_id(), Some(branch_id));

        // From entity ref
        let e = StrataError::not_found(EntityRef::kv(branch_id, "default", "key"));
        assert_eq!(e.branch_id(), Some(branch_id));

        // No branch_id
        let e = StrataError::storage("error");
        assert_eq!(e.branch_id(), None);
    }

    // === Conversion Tests ===

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let e: StrataError = io_err.into();

        assert!(e.is_storage_error());
        assert!(e.to_string().contains("IO error"));
    }

    // === Wire Encoding Tests ===

    #[test]
    fn test_error_code_mapping_not_found() {
        let branch_id = BranchId::new();
        let e = StrataError::not_found(EntityRef::kv(branch_id, "default", "key"));
        assert_eq!(e.code(), ErrorCode::NotFound);
    }

    #[test]
    fn test_error_code_mapping_branch_not_found() {
        let e = StrataError::branch_not_found(BranchId::new());
        assert_eq!(e.code(), ErrorCode::NotFound);
    }

    #[test]
    fn test_error_code_mapping_branch_not_found_by_name() {
        let e = StrataError::branch_not_found_by_name("release/2026-03");
        assert_eq!(e.code(), ErrorCode::NotFound);
    }

    #[test]
    fn test_error_code_mapping_wrong_type() {
        let e = StrataError::wrong_type("Int", "String");
        assert_eq!(e.code(), ErrorCode::WrongType);
    }

    #[test]
    fn test_error_code_mapping_conflict() {
        let e = StrataError::conflict("version mismatch");
        assert_eq!(e.code(), ErrorCode::Conflict);
    }

    #[test]
    fn test_error_code_mapping_version_conflict() {
        let branch_id = BranchId::new();
        let e = StrataError::version_conflict(
            EntityRef::kv(branch_id, "default", "k"),
            Version::Txn(1),
            Version::Txn(2),
        );
        assert_eq!(e.code(), ErrorCode::Conflict);
    }

    #[test]
    fn test_error_code_mapping_constraint_violation() {
        let e = StrataError::invalid_input("value too large");
        assert_eq!(e.code(), ErrorCode::ConstraintViolation);
    }

    #[test]
    fn test_error_code_mapping_storage() {
        let e = StrataError::storage("disk full");
        assert_eq!(e.code(), ErrorCode::StorageError);
    }

    #[test]
    fn test_error_code_mapping_internal() {
        let e = StrataError::internal("bug");
        assert_eq!(e.code(), ErrorCode::InternalError);
    }

    #[test]
    fn test_error_message() {
        let e = StrataError::invalid_input("key too long");
        let msg = e.message();
        assert!(msg.contains("key too long"));
    }

    #[test]
    fn test_error_details() {
        let branch_id = BranchId::new();
        let e = StrataError::not_found(EntityRef::kv(branch_id, "default", "mykey"));
        let details = e.details();
        assert!(!details.is_empty());
        assert!(details.fields().contains_key("entity"));
    }

    #[test]
    fn test_error_details_version_conflict() {
        let branch_id = BranchId::new();
        let e = StrataError::version_conflict(
            EntityRef::kv(branch_id, "default", "k"),
            Version::Txn(1),
            Version::Txn(2),
        );
        let details = e.details();
        assert!(details.fields().contains_key("entity"));
        assert!(details.fields().contains_key("expected"));
        assert!(details.fields().contains_key("actual"));
    }
}

// =============================================================================
// ErrorCode Tests
// =============================================================================

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn test_all_error_codes() {
        let codes = [
            ErrorCode::NotFound,
            ErrorCode::WrongType,
            ErrorCode::InvalidKey,
            ErrorCode::InvalidPath,
            ErrorCode::HistoryTrimmed,
            ErrorCode::ConstraintViolation,
            ErrorCode::Conflict,
            ErrorCode::SerializationError,
            ErrorCode::StorageError,
            ErrorCode::InternalError,
        ];

        // All codes have string representations
        for code in &codes {
            let s = code.as_str();
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn test_error_code_parse() {
        assert_eq!(ErrorCode::parse("NotFound"), Some(ErrorCode::NotFound));
        assert_eq!(ErrorCode::parse("WrongType"), Some(ErrorCode::WrongType));
        assert_eq!(ErrorCode::parse("Conflict"), Some(ErrorCode::Conflict));
        assert_eq!(ErrorCode::parse("Invalid"), None);
    }

    #[test]
    fn test_error_code_roundtrip() {
        let codes = [
            ErrorCode::NotFound,
            ErrorCode::WrongType,
            ErrorCode::InvalidKey,
            ErrorCode::InvalidPath,
            ErrorCode::HistoryTrimmed,
            ErrorCode::ConstraintViolation,
            ErrorCode::Conflict,
            ErrorCode::SerializationError,
            ErrorCode::StorageError,
            ErrorCode::InternalError,
        ];

        for code in &codes {
            let s = code.as_str();
            let parsed = ErrorCode::parse(s);
            assert_eq!(parsed, Some(*code), "Roundtrip failed for {:?}", code);
        }
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(format!("{}", ErrorCode::NotFound), "NotFound");
        assert_eq!(format!("{}", ErrorCode::Conflict), "Conflict");
    }

    #[test]
    fn test_error_code_retryable() {
        assert!(ErrorCode::Conflict.is_retryable());
        assert!(!ErrorCode::NotFound.is_retryable());
        assert!(!ErrorCode::ConstraintViolation.is_retryable());
    }

    #[test]
    fn test_error_code_serious() {
        assert!(ErrorCode::InternalError.is_serious());
        assert!(ErrorCode::StorageError.is_serious());
        assert!(!ErrorCode::NotFound.is_serious());
        assert!(!ErrorCode::Conflict.is_serious());
    }
}

// =============================================================================
// ConstraintReason Tests
// =============================================================================

#[cfg(test)]
mod constraint_reason_tests {
    use super::*;

    #[test]
    fn test_all_constraint_reasons() {
        let reasons = [
            ConstraintReason::ValueTooLarge,
            ConstraintReason::StringTooLong,
            ConstraintReason::BytesTooLong,
            ConstraintReason::ArrayTooLong,
            ConstraintReason::ObjectTooLarge,
            ConstraintReason::NestingTooDeep,
            ConstraintReason::KeyTooLong,
            ConstraintReason::KeyInvalid,
            ConstraintReason::KeyEmpty,
            ConstraintReason::DimensionMismatch,
            ConstraintReason::DimensionTooLarge,
            ConstraintReason::CapacityExceeded,
            ConstraintReason::BudgetExceeded,
            ConstraintReason::InvalidOperation,
            ConstraintReason::BranchNotActive,
            ConstraintReason::TransactionNotActive,
            ConstraintReason::WrongType,
            ConstraintReason::Overflow,
        ];

        // All reasons have string representations
        for reason in &reasons {
            let s = reason.as_str();
            assert!(!s.is_empty());
            assert!(!reason.description().is_empty());
        }
    }

    #[test]
    fn test_constraint_reason_parse() {
        assert_eq!(
            ConstraintReason::parse("value_too_large"),
            Some(ConstraintReason::ValueTooLarge)
        );
        assert_eq!(
            ConstraintReason::parse("key_empty"),
            Some(ConstraintReason::KeyEmpty)
        );
        assert_eq!(
            ConstraintReason::parse("dimension_mismatch"),
            Some(ConstraintReason::DimensionMismatch)
        );
        assert_eq!(ConstraintReason::parse("invalid"), None);
    }

    #[test]
    fn test_constraint_reason_roundtrip() {
        let reasons = [
            ConstraintReason::ValueTooLarge,
            ConstraintReason::StringTooLong,
            ConstraintReason::BytesTooLong,
            ConstraintReason::ArrayTooLong,
            ConstraintReason::ObjectTooLarge,
            ConstraintReason::NestingTooDeep,
            ConstraintReason::KeyTooLong,
            ConstraintReason::KeyInvalid,
            ConstraintReason::KeyEmpty,
            ConstraintReason::DimensionMismatch,
            ConstraintReason::DimensionTooLarge,
            ConstraintReason::CapacityExceeded,
            ConstraintReason::BudgetExceeded,
            ConstraintReason::InvalidOperation,
            ConstraintReason::BranchNotActive,
            ConstraintReason::TransactionNotActive,
            ConstraintReason::WrongType,
            ConstraintReason::Overflow,
        ];

        for reason in &reasons {
            let s = reason.as_str();
            let parsed = ConstraintReason::parse(s);
            assert_eq!(parsed, Some(*reason), "Roundtrip failed for {:?}", reason);
        }
    }

    #[test]
    fn test_constraint_reason_display() {
        assert_eq!(
            format!("{}", ConstraintReason::ValueTooLarge),
            "value_too_large"
        );
        assert_eq!(format!("{}", ConstraintReason::KeyEmpty), "key_empty");
    }
}

// =============================================================================
// ErrorDetails Tests
// =============================================================================

#[cfg(test)]
mod error_details_tests {
    use super::*;

    #[test]
    fn test_error_details_empty() {
        let details = ErrorDetails::new();
        assert!(details.is_empty());
    }

    #[test]
    fn test_error_details_with_string() {
        let details = ErrorDetails::new().with_string("key", "value");
        assert!(!details.is_empty());
        assert!(details.fields().contains_key("key"));
    }

    #[test]
    fn test_error_details_with_int() {
        let details = ErrorDetails::new().with_int("count", 42);
        assert!(!details.is_empty());
        assert!(details.fields().contains_key("count"));
    }

    #[test]
    fn test_error_details_with_bool() {
        let details = ErrorDetails::new().with_bool("active", true);
        assert!(!details.is_empty());
        assert!(details.fields().contains_key("active"));
    }

    #[test]
    fn test_error_details_chained() {
        let details = ErrorDetails::new()
            .with_string("entity", "kv:default/key")
            .with_int("expected", 1)
            .with_int("actual", 2)
            .with_bool("retryable", true);

        assert!(!details.is_empty());
        assert_eq!(details.fields().len(), 4);
    }

    #[test]
    fn test_error_details_to_string_map() {
        let details = ErrorDetails::new()
            .with_string("name", "test")
            .with_int("count", 42)
            .with_bool("flag", true);

        let map = details.to_string_map();
        assert_eq!(map.get("name"), Some(&"test".to_string()));
        assert_eq!(map.get("count"), Some(&"42".to_string()));
        assert_eq!(map.get("flag"), Some(&"true".to_string()));
    }

    #[test]
    fn test_error_details_overwrite_key() {
        let details = ErrorDetails::new()
            .with_string("key", "first")
            .with_string("key", "second");
        let map = details.to_string_map();
        assert_eq!(map.get("key"), Some(&"second".to_string()));
        assert_eq!(details.fields().len(), 1);
    }

    #[test]
    fn test_error_details_default_is_empty() {
        let details = ErrorDetails::default();
        assert!(details.is_empty());
    }
}

#[cfg(test)]
mod conversion_tests {
    use super::*;

    #[test]
    fn test_from_bincode_error() {
        // Create a bincode error via a failed deserialization
        let bad_bytes: Vec<u8> = vec![0xFF, 0xFF];
        let err: std::result::Result<String, _> = bincode::deserialize(&bad_bytes);
        let bincode_err = err.unwrap_err();
        let strata_err: StrataError = bincode_err.into();

        assert!(strata_err.is_storage_error());
        assert_eq!(strata_err.code(), ErrorCode::SerializationError);
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_err = serde_json::from_str::<String>("not valid json").unwrap_err();
        let strata_err: StrataError = json_err.into();

        assert!(strata_err.is_storage_error());
        assert_eq!(strata_err.code(), ErrorCode::SerializationError);
        assert!(strata_err.to_string().contains("JSON error"));
    }

    #[test]
    fn test_from_io_error_preserves_source() {
        use std::error::Error;

        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let strata_err: StrataError = io_err.into();

        // Should have a source error
        assert!(strata_err.source().is_some());
    }
}

#[cfg(test)]
mod adversarial_error_tests {
    use super::*;

    #[test]
    fn test_conflict_on_constructor() {
        let branch_id = BranchId::new();
        let entity = EntityRef::kv(branch_id, "default", "shared-counter");
        let e = StrataError::conflict_on(entity.clone(), "concurrent modification");

        assert_eq!(e.code(), ErrorCode::Conflict);
        assert!(e.is_retryable());
        // conflict_on exposes entity via entity_ref() accessor
        assert_eq!(e.entity_ref(), Some(&entity));
        assert!(e.to_string().contains("concurrent modification"));
        // details() contains the "entity" key
        let details = e.details();
        assert!(
            details.fields().contains_key("entity"),
            "conflict_on details should contain 'entity' key"
        );
    }

    #[test]
    fn test_conflict_with_txn_constructor() {
        let e = StrataError::conflict_with_txn("txn conflict", 42);

        assert_eq!(e.code(), ErrorCode::Conflict);
        assert!(e.is_retryable());
        assert!(e.to_string().contains("txn conflict"));

        let details = e.details();
        assert!(
            details.fields().contains_key("transaction_id"),
            "conflict_with_txn details should contain 'transaction_id' key"
        );
    }

    #[test]
    fn test_error_code_parse_empty_string() {
        assert_eq!(ErrorCode::parse(""), None);
    }

    #[test]
    fn test_error_code_parse_case_sensitive() {
        assert_eq!(ErrorCode::parse("notfound"), None);
        assert_eq!(ErrorCode::parse("NOTFOUND"), None);
        assert_eq!(ErrorCode::parse("NotFound"), Some(ErrorCode::NotFound));
    }

    #[test]
    fn test_constraint_reason_parse_empty_string() {
        assert_eq!(ConstraintReason::parse(""), None);
    }

    #[test]
    fn test_constraint_reason_parse_case_sensitive() {
        assert_eq!(ConstraintReason::parse("Value_Too_Large"), None);
        assert_eq!(ConstraintReason::parse("VALUE_TOO_LARGE"), None);
        assert_eq!(
            ConstraintReason::parse("value_too_large"),
            Some(ConstraintReason::ValueTooLarge)
        );
    }

    #[test]
    fn test_every_error_code_has_unique_as_str() {
        let codes = [
            ErrorCode::NotFound,
            ErrorCode::WrongType,
            ErrorCode::InvalidKey,
            ErrorCode::InvalidPath,
            ErrorCode::HistoryTrimmed,
            ErrorCode::ConstraintViolation,
            ErrorCode::Conflict,
            ErrorCode::SerializationError,
            ErrorCode::StorageError,
            ErrorCode::InternalError,
        ];
        let strs: std::collections::HashSet<&str> = codes.iter().map(|c| c.as_str()).collect();
        assert_eq!(
            strs.len(),
            codes.len(),
            "All error code strings must be unique"
        );
    }

    #[test]
    fn test_every_constraint_reason_has_unique_as_str() {
        let reasons = [
            ConstraintReason::ValueTooLarge,
            ConstraintReason::StringTooLong,
            ConstraintReason::BytesTooLong,
            ConstraintReason::ArrayTooLong,
            ConstraintReason::ObjectTooLarge,
            ConstraintReason::NestingTooDeep,
            ConstraintReason::KeyTooLong,
            ConstraintReason::KeyInvalid,
            ConstraintReason::KeyEmpty,
            ConstraintReason::DimensionMismatch,
            ConstraintReason::DimensionTooLarge,
            ConstraintReason::CapacityExceeded,
            ConstraintReason::BudgetExceeded,
            ConstraintReason::InvalidOperation,
            ConstraintReason::BranchNotActive,
            ConstraintReason::TransactionNotActive,
            ConstraintReason::WrongType,
            ConstraintReason::Overflow,
        ];
        let strs: std::collections::HashSet<&str> = reasons.iter().map(|r| r.as_str()).collect();
        assert_eq!(
            strs.len(),
            reasons.len(),
            "All constraint reason strings must be unique"
        );
    }

    #[test]
    fn test_all_error_variants_have_wire_code() {
        let branch_id = BranchId::new();
        let entity = EntityRef::kv(branch_id, "default", "k");
        // Construct every error variant and verify code() doesn't panic
        let errors: Vec<StrataError> = vec![
            StrataError::not_found(entity.clone()),
            StrataError::branch_not_found(branch_id),
            StrataError::wrong_type("A", "B"),
            StrataError::conflict("reason"),
            StrataError::conflict_on(entity.clone(), "reason"),
            StrataError::conflict_with_txn("txn conflict", 99),
            StrataError::version_conflict(entity.clone(), Version::Txn(1), Version::Txn(2)),
            StrataError::write_conflict(entity.clone()),
            StrataError::transaction_aborted("reason"),
            StrataError::transaction_timeout(100),
            StrataError::write_stall_timeout(30000, 40),
            StrataError::transaction_not_active("committed"),
            StrataError::invalid_operation(entity.clone(), "reason"),
            StrataError::invalid_input("bad"),
            StrataError::dimension_mismatch(3, 4),
            StrataError::path_not_found(entity.clone(), "/x"),
            StrataError::history_trimmed(entity.clone(), Version::Txn(1), Version::Txn(5)),
            StrataError::storage("disk"),
            StrataError::serialization("bad"),
            StrataError::corruption("crc"),
            StrataError::capacity_exceeded("log", 100, 101),
            StrataError::budget_exceeded("search"),
            StrataError::internal("bug"),
        ];
        for e in &errors {
            let _ = e.code(); // Should not panic
            let _ = e.message(); // Should not panic
            let _ = e.details(); // Should not panic
        }
        assert_eq!(errors.len(), 23, "Should test all 23 error constructors");
    }

    #[test]
    fn test_history_trimmed_code_and_details() {
        let branch_id = BranchId::new();
        let e = StrataError::history_trimmed(
            EntityRef::kv(branch_id, "default", "k"),
            Version::Txn(10),
            Version::Txn(50),
        );
        assert_eq!(e.code(), ErrorCode::HistoryTrimmed);
        let details = e.details();
        assert!(details.fields().contains_key("requested"));
        assert!(details.fields().contains_key("earliest_retained"));
    }

    #[test]
    fn test_path_not_found_is_not_found_but_has_invalid_path_code() {
        let branch_id = BranchId::new();
        let e =
            StrataError::path_not_found(EntityRef::json(branch_id, "default", "doc"), "/missing");
        // is_not_found returns true (classification)
        assert!(e.is_not_found());
        // but wire code is InvalidPath
        assert_eq!(e.code(), ErrorCode::InvalidPath);
    }
}
