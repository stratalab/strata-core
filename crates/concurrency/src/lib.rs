//! Durability-coupled transaction shell for Strata.
//!
//! The canonical home for the generic transaction runtime now lives in
//! `strata-storage`. This crate is the remaining shell that still owns:
//! - the WAL-aware commit adapter
//! - transaction payload serialization for WAL records
//! - recovery / replay coordination over the durability runtime
//!
//! It exists only until the durability runtime is absorbed by storage.

mod manager;
pub mod payload;
pub mod recovery;

#[cfg(any(test, feature = "fault-injection"))]
#[doc(hidden)]
pub mod __internal {
    /// Inject a single storage-apply failure after WAL commit.
    pub fn inject_apply_failure_once(reason: impl Into<String>) {
        crate::manager::inject_apply_failure_once(reason);
    }

    /// Clear any pending injected storage-apply failure.
    pub fn clear_apply_failure_injection() {
        crate::manager::clear_apply_failure_injection();
    }
}

pub use manager::{
    commit_with_version as commit_durable_with_version, commit_with_wal as commit_durable_with_wal,
    commit_with_wal_arc as commit_durable_with_wal_arc,
};
pub use payload::TransactionPayload;
pub use recovery::{
    apply_wal_record_to_memory_storage, manifest_error_to_strata_error, CoordinatorRecoveryError,
    RecoveryCoordinator, RecoveryPlan, RecoveryResult, RecoveryStats,
};
