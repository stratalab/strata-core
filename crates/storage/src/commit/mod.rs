//! Internal commit pipeline and timeline mechanics.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "commit runtime scaffolding is consumed by later commit-layer work"
    )
)]

mod allocator;
mod batch;
mod branch_registry;
mod cache;
mod config;
mod conflict;
mod durable;
mod durable_gate;
mod error;
mod facts;
mod guard;
mod lock_order;
mod outcome;
mod replay;
mod result;
mod timeline;
mod visibility;

#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use allocator::{
    CommitFactAllocation, CommitFactAllocator, CommitManualTimestampSource,
    CommitTimestampAllocationSource, CommitTimestampGuard, CommitTimestampSource,
    CommitVersionAllocator,
};
pub(crate) use batch::require_wal_record_fits;
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use batch::{
    CommitBatch, CommitBatchKind, CommitBatchOptions, CommitCasFact, CommitConflictValidationMode,
    CommitDuplicateKeyPolicy, CommitDurabilityMode, CommitExpiry, CommitMutation,
    CommitObservedVersion, CommitOrigin, CommitReadFact, CommitRetentionHint, CommitStamp,
    CommitTimestampPolicy, CommitValidationFacts, ValidatedCommitBatch,
};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use branch_registry::{
    admit_mutating_commit, CommitBranchDescriptor, CommitBranchGeneration,
    CommitBranchGenerationGuard, CommitBranchRegistry, CommitBranchState,
};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use cache::CommitCacheRuntime;
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "commit scaffold exports define the local surface for later slices"
    )
)]
pub(crate) use config::{CommitReadOnlyDiagnostics, CommitRuntimeConfig};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use conflict::{
    commit_conflict_validation_needs_source, validate_commit_conflicts,
    validate_commit_conflicts_without_source, CommitBranchReadViewConflictSource, CommitConflict,
    CommitConflictKind, CommitConflictReadSource, CommitConflictReport,
};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use durable::{
    publish_commit_group, CommitBranchApplyTarget, CommitDurableRuntime, CommitGroupState,
    CommitVisiblePublisher, CommitWalAppendError, CommitWalAppendFacts, CommitWalAppender,
};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use durable_gate::{
    CommitUnresolvedDurable, CommitUnresolvedDurableGate, CommitUnresolvedDurableKind,
};
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "commit scaffold exports define the local surface for later slices"
    )
)]
pub(crate) use error::{CommitLowerLayer, CommitRuntimeError};
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "commit scaffold exports define the local surface for later slices"
    )
)]
pub(crate) use facts::{
    CommitAdmissionPressureFacts, CommitAdmissionPressureThresholds, CommitDurabilityClass,
    CommitPhase, CommitVisibilityFacts,
};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use guard::{CommitBranchGuard, CommitBranchGuardSet, CommitQuiesceGuard};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use outcome::{
    execute_read_only_diagnostic, CommitMutationCounts, CommitOutcome, CommitOutcomeKind,
    CommitReadSnapshot,
};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use replay::{
    CommitReplayAction, CommitReplayReport, CommitReplayRequest, CommitReplayRuntime,
};
#[cfg_attr(
    not(test),
    allow(
        unused_imports,
        reason = "commit scaffold exports define the local surface for later slices"
    )
)]
pub(crate) use result::CommitRuntimeResult;
#[cfg(any(test, feature = "testkit"))]
pub(crate) use timeline::CommitTimelineRows;
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use timeline::{
    CommitTimelineEntry, CommitTimelineFact, CommitTimelineLookup, CommitTimelineMiss,
    CommitTimelineRowKind, CommitTimelineView, COMMIT_TIMELINE_SPACE,
};
#[allow(
    unused_imports,
    reason = "commit scaffold exports define the local surface for later slices"
)]
pub(crate) use visibility::{VisibleVersionPublish, VisibleVersionTracker};

#[cfg(test)]
mod tests;
