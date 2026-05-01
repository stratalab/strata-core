//! Generic transaction runtime for the storage substrate.
//!
//! This subtree owns the primitive-agnostic OCC state and validation surface
//! that higher layers use to build transactional behavior.

pub mod context;
pub mod lock_ordering;
pub mod manager;
pub mod validation;

pub use context::{
    ApplyResult, CASOperation, CommitError, PendingOperations, TransactionContext,
    TransactionStatus,
};
pub use manager::TransactionManager;
pub use validation::{
    validate_cas_set, validate_read_set, validate_transaction, ConflictType, ValidationResult,
};
