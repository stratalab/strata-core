//! Minimal foundational core for Strata.
//!
//! This crate defines the foundational shared language of the system.
//! It is the home for stable identifiers, values, contract DTOs, and other
//! low-level types that every higher layer must agree on.

pub mod branch;
pub mod contract;
pub mod error;
pub mod id;
pub mod value;

pub use contract::{
    BranchName, BranchNameError, EntityRef, PrimitiveType, Timestamp, Version, Versioned,
    VersionedHistory, VersionedValue, MAX_BRANCH_NAME_LENGTH,
};
pub use id::{BranchId, CommitVersion, TxnId};
pub use value::Value;
