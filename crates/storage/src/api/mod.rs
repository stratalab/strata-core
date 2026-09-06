//! Engine-facing storage API boundary.
//!
//! Native database opens should use durable local storage first:
//!
//! ```no_run
//! # use strata_storage::api::{StorageApiResult, StorageRuntime};
//! # fn open(root: std::path::PathBuf) -> StorageApiResult<()> {
//! let outcome = StorageRuntime::open_local(root)?;
//! let runtime = outcome.into_runtime();
//! # let _ = runtime;
//! # Ok(())
//! # }
//! ```
//!
//! Volatile storage must be requested explicitly. Use this for tests, demos, or
//! sessions where losing all state after the runtime is dropped is intended:
//!
//! ```
//! # use strata_storage::api::{StorageApiResult, StorageRuntime};
//! # fn open() -> StorageApiResult<()> {
//! let outcome = StorageRuntime::open_ephemeral()?;
//! let runtime = outcome.into_runtime();
//! # let _ = runtime;
//! # Ok(())
//! # }
//! ```

#![allow(
    missing_docs,
    reason = "the storage API scaffold exposes boundary vocabulary before behavior slices add full rustdoc"
)]

mod atoms;
mod backend;
mod branch;
mod commit;
mod diagnostics;
mod error;
mod maintenance;
mod options;
mod outcome;
mod read;
mod result;
mod runtime;

pub use atoms::{BranchGeneration, ReadLimit, ScanRange, StorageKey, StorageSpaceId, StorageValue};
pub use backend::StorageBackend;
pub use branch::{
    BranchAction, BranchCleanupSummary, BranchOperation, BranchOutcome, BranchParentSummary,
    BranchRequest, BranchStatus, BranchSummary,
};
pub use commit::{
    CommitBatch, CommitCondition, CommitDurability, CommitDurabilitySummary, CommitExpectedVersion,
    CommitMutation, CommitOptions,
};
pub use diagnostics::{
    DiagnosticsBranchCatalogReport, DiagnosticsBudgetAccuracy, DiagnosticsBudgetPool,
    DiagnosticsBudgetPressure, DiagnosticsBudgetReport, DiagnosticsBudgetUsage,
    DiagnosticsCheckpointReport, DiagnosticsFactState, DiagnosticsOutcome,
    DiagnosticsQuarantineReport, DiagnosticsReadActivityReport, DiagnosticsRecoveryClass,
    DiagnosticsRecoveryFault, DiagnosticsRecoveryFaultKind, DiagnosticsRecoveryReport,
    DiagnosticsRequest, DiagnosticsRetentionReport, DiagnosticsScope,
    DiagnosticsSourceLayoutReport, DiagnosticsSourceLevelTableCount,
    DiagnosticsStoragePressureReason, DiagnosticsStoragePressureReport,
    DiagnosticsStoragePressureSeverity, DiagnosticsTableReachabilityReport,
    DiagnosticsTimelineReport, DiagnosticsWalGrowthReport,
};
pub use error::{StorageApiError, StorageApiErrorClass, StorageApiLowerLayer};
pub use maintenance::{
    MaintenanceDrainSummary, MaintenanceFailureSummary, MaintenanceQueueSummary,
    MaintenanceReasonClass, MaintenanceRequest, MaintenanceScope, MaintenanceSummary,
    MaintenanceSummaryStatus, MaintenanceTask, MaintenanceWalGrowthStatus,
    MaintenanceWalGrowthSummary, MaintenanceWalGrowthTrigger,
};
pub use options::{
    StorageBackgroundMaintenanceOptions, StorageBudgetPolicy, StorageCachePreheatPolicy,
    StorageDurabilityPolicy, StorageMaintenanceSchedulingPolicy, StorageMemoryBudget, StorageMode,
    StorageOpenOptions, StorageWalGrowthPolicy,
};
pub use outcome::{
    CommitAdmissionPressureReason, CommitAdmissionPressureSeverity, CommitAdmissionStatus,
    CommitAdmissionSummary, CommitSummary, RecoveryHealthSummary, StorageBudgetSource,
    StorageCloseSummary, StorageOpenDisposition, StorageOpenOutcome, StorageOpenSummary,
    StorageRuntimeState,
};
pub use read::{
    CommitInstantsRequest, HistoryReadOutcome, HistoryReadRequest, ImmutableSourceScanReadOutcome,
    ImmutableSourceScanReadRequest, PointReadOutcome, PointReadRequest, PrefixScanReadRequest,
    ReadBound, ScanReadOutcome, ScanReadRequest, StorageImmutableSource, StorageReadRow,
    TimelineBoundsOutcome, TimelineBoundsRequest, TimestampLookupMiss, TimestampLookupOutcome,
    TimestampLookupRequest, VersionLookupOutcome, VersionLookupRequest, WallClockLookupOutcome,
    WallClockLookupRequest,
};
pub use result::StorageApiResult;
#[cfg(any(test, feature = "testkit"))]
pub(crate) use runtime::{
    map_commit_error_for_test, map_lifecycle_error_for_test, map_maintenance_outcome_for_test,
};
pub use runtime::{StorageCloseOptions, StorageRuntime};
pub use strata_core::{BranchId, CommitVersion, Timestamp};

#[cfg(test)]
mod tests;
