//! Concurrency layer for Strata
//!
//! This crate implements optimistic concurrency control (OCC) with:
//! - TransactionContext: Read/write set tracking
//! - TransactionManager: Atomic commit coordination
//! - RecoveryCoordinator: Database recovery from WAL
//! - Snapshot isolation via SegmentedStore + start_version bound
//! - Conflict detection at commit time
//! - Compare-and-swap (CAS) operations
//! - WAL integration for durability
//! - JSON region-based conflict detection

pub(crate) mod conflict;
pub mod lock_ordering;
pub mod manager;
pub mod payload;
pub mod recovery;
pub mod transaction;
pub mod validation;

pub use manager::TransactionManager;
pub use payload::TransactionPayload;
pub use recovery::{RecoveryCoordinator, RecoveryResult, RecoveryStats};
pub use transaction::{CommitError, JsonStoreExt, TransactionContext, TransactionStatus};
