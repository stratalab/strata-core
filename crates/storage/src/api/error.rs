//! API error vocabulary.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use strata_core::BranchId;

use super::{CommitAdmissionPressureReason, CommitAdmissionPressureSeverity};

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageApiErrorClass {
    InvalidArgument,
    FailedPrecondition,
    NotFound,
    AlreadyExists,
    Conflict,
    Unsupported,
    HistoryUnavailable,
    AmbiguousCommit,
    ResourceExhausted,
    Internal,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageApiLowerLayer {
    Backend,
    Layout,
    Format,
    Service,
    Table,
    Branch,
    Commit,
    Lifecycle,
}

impl StorageApiLowerLayer {
    /// The layer's own stable code, used when a lower-layer error reaches
    /// the boundary without a discriminant of its own. Coarser than the
    /// inner error's `code()` — it names the failing layer, not the
    /// failure — but it beats collapsing every layer into one constant.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Backend => "internal.storage_api.backend",
            Self::Layout => "internal.storage_api.layout",
            Self::Format => "internal.storage_api.format",
            Self::Service => "internal.storage_api.service",
            Self::Table => "internal.storage_api.table",
            Self::Branch => "internal.storage_api.branch",
            Self::Commit => "internal.storage_api.commit",
            Self::Lifecycle => "internal.storage_api.lifecycle",
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum StorageApiError {
    InvalidArgument {
        field: &'static str,
        reason: &'static str,
    },
    UnsupportedCapability {
        capability: &'static str,
        reason: &'static str,
    },
    InvalidRuntimeState {
        reason: &'static str,
    },
    /// Another opener holds the database's writer lock.
    ///
    /// Distinct from `InvalidRuntimeState` on purpose: the IPC broker decides
    /// whether to hand a command to a running host by recognising exactly this
    /// condition, and `failed_precondition.storage_api.state` is the whole
    /// class's generic code — branch-not-writable, guard-unavailable and
    /// quiesce-unavailable all produce it, so brokering on it would fire on
    /// unrelated preconditions (#3005, #3167).
    WriterLockHeld,
    BranchNotFound {
        branch_id: BranchId,
    },
    BranchAlreadyExists {
        branch_id: BranchId,
    },
    BranchGenerationMismatch {
        branch_id: BranchId,
        expected: u64,
        actual: u64,
    },
    Conflict {
        branch_id: BranchId,
        storage_space: Option<u8>,
        key_fingerprint: Option<u64>,
        user_key_len: Option<usize>,
        reason: &'static str,
    },
    RetainedHistoryUnavailable {
        branch_id: BranchId,
        reason: &'static str,
    },
    TimestampHistoryUnavailable {
        branch_id: BranchId,
        reason: &'static str,
    },
    DurableUncertain {
        reason: &'static str,
        source: Option<Arc<dyn Error + Send + Sync + 'static>>,
    },
    RecoveryDegraded {
        reason: &'static str,
    },
    MaintenanceRejected {
        reason: &'static str,
    },
    StoragePressure {
        branch_id: BranchId,
        severity: CommitAdmissionPressureSeverity,
        pressure_reason: CommitAdmissionPressureReason,
        reason: &'static str,
        retryable: bool,
    },
    ResourceExhausted {
        resource: &'static str,
        requested_bytes: u64,
        used_bytes: u64,
        limit_bytes: u64,
        reason: &'static str,
    },
    IncompatibleLayout {
        reason: &'static str,
    },
    LowerLayer {
        layer: StorageApiLowerLayer,
        /// The failing inner layer's stable code (e.g.
        /// `not_found.branch.branch_id`), carried across the boundary so
        /// callers — and tests, per Hard Rule 29 — can tell one
        /// lower-layer failure from another without reading `reason`.
        ///
        /// Deliberately NOT this error's `code()`: from the API's
        /// perspective an unmapped lower-layer failure IS internal, and
        /// `code()` must keep agreeing with `class()`. This is diagnostic
        /// detail, not a reclassification. `None` while a layer still has
        /// no discriminant (TCP3.2b/c wire Commit and Table).
        inner_code: Option<&'static str>,
        reason: &'static str,
        source: Option<Arc<dyn Error + Send + Sync + 'static>>,
    },
}

impl StorageApiError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidArgument { .. } => "invalid_argument.storage_api.argument",
            Self::UnsupportedCapability { .. } => "unsupported.storage_api.capability",
            Self::InvalidRuntimeState { .. } => "failed_precondition.storage_api.state",
            Self::WriterLockHeld => "failed_precondition.storage_api.writer_lock",
            Self::BranchNotFound { .. } => "not_found.storage_api.branch",
            Self::BranchAlreadyExists { .. } => "already_exists.storage_api.branch",
            Self::BranchGenerationMismatch { .. } => {
                "failed_precondition.storage_api.branch_generation"
            }
            Self::Conflict { .. } => "conflict.storage_api.conflict",
            Self::RetainedHistoryUnavailable { .. } => "history_unavailable.storage_api.retained",
            Self::TimestampHistoryUnavailable { .. } => "history_unavailable.storage_api.timestamp",
            Self::DurableUncertain { .. } => "ambiguous_commit.storage_api.durable_uncertain",
            Self::RecoveryDegraded { .. } => "failed_precondition.storage_api.recovery_degraded",
            Self::MaintenanceRejected { .. } => "failed_precondition.storage_api.maintenance",
            Self::StoragePressure { .. } => "failed_precondition.storage_api.storage_pressure",
            Self::ResourceExhausted { .. } => "resource_exhausted.storage_api.memory_budget",
            Self::IncompatibleLayout { .. } => {
                "failed_precondition.storage_api.incompatible_layout"
            }
            Self::LowerLayer { layer, .. } => layer.code(),
        }
    }

    /// Mechanical, storage-level remediation hint.
    ///
    /// This is the storage-owned input to a Stripe-grade "suggested fix". It
    /// must stay mechanical (a storage operator/engine instruction), never
    /// product- or end-user phrasing (Hard Rule 30; see
    /// `docs/architecture/v1-error-and-diagnostics-contract.md`). Engine
    /// and the SDK translate this into user-facing guidance. Adding a new
    /// variant is a compile error until a remediation arm is supplied.
    pub const fn remediation(&self) -> &'static str {
        match self {
            Self::InvalidArgument { .. } => {
                "Correct the named argument to satisfy its documented constraint and retry the call."
            }
            Self::UnsupportedCapability { .. } => {
                "Open the database in a storage mode or with a backend that supports the requested capability."
            }
            Self::WriterLockHeld => {
                "Close the other handle to this database, or attach to the running host instead of opening it again."
            }
            Self::InvalidRuntimeState { .. } => {
                "Ensure the runtime is open and in a valid state before issuing this operation."
            }
            Self::BranchNotFound { .. } => {
                "Create the branch or target an existing branch id."
            }
            Self::BranchAlreadyExists { .. } => {
                "Target the existing branch or choose a new branch id."
            }
            Self::BranchGenerationMismatch { .. } => {
                "Reload the current branch generation and retry with the expected generation."
            }
            Self::Conflict { .. } => {
                "Re-read the conflicting key and retry the commit against the current version."
            }
            Self::RetainedHistoryUnavailable { .. } => {
                "Request a version within the retained history window."
            }
            Self::TimestampHistoryUnavailable { .. } => {
                "Request a timestamp within the covered timestamp-history window."
            }
            Self::DurableUncertain { .. } => {
                "Re-open the database to recover durable state before assuming the commit outcome."
            }
            Self::RecoveryDegraded { .. } => {
                "Inspect recovery diagnostics and resolve the degradation before resuming writes."
            }
            Self::MaintenanceRejected { .. } => {
                "Retry the maintenance request after the conflicting maintenance completes."
            }
            Self::StoragePressure { .. } => {
                "Allow background maintenance to drain storage pressure, then retry the write."
            }
            Self::ResourceExhausted { .. } => {
                "Reduce resident memory pressure or raise the configured storage memory budget, then retry the operation."
            }
            Self::IncompatibleLayout { .. } => {
                "Choose an empty directory or an existing V1 database directory; pre-V1 layouts are not readable by this version."
            }
            Self::LowerLayer { .. } => {
                "Inspect the source error and storage diagnostics for the underlying failure."
            }
        }
    }

    pub const fn class(&self) -> StorageApiErrorClass {
        match self {
            Self::InvalidArgument { .. } => StorageApiErrorClass::InvalidArgument,
            Self::UnsupportedCapability { .. } => StorageApiErrorClass::Unsupported,
            Self::InvalidRuntimeState { .. }
            | Self::WriterLockHeld
            | Self::BranchGenerationMismatch { .. }
            | Self::MaintenanceRejected { .. }
            | Self::StoragePressure { .. }
            | Self::RecoveryDegraded { .. }
            | Self::IncompatibleLayout { .. } => StorageApiErrorClass::FailedPrecondition,
            Self::BranchNotFound { .. } => StorageApiErrorClass::NotFound,
            Self::BranchAlreadyExists { .. } => StorageApiErrorClass::AlreadyExists,
            Self::Conflict { .. } => StorageApiErrorClass::Conflict,
            Self::RetainedHistoryUnavailable { .. } | Self::TimestampHistoryUnavailable { .. } => {
                StorageApiErrorClass::HistoryUnavailable
            }
            Self::DurableUncertain { .. } => StorageApiErrorClass::AmbiguousCommit,
            Self::ResourceExhausted { .. } => StorageApiErrorClass::ResourceExhausted,
            Self::LowerLayer { .. } => StorageApiErrorClass::Internal,
        }
    }

    /// Wrap a lower-layer error that has no stable discriminant yet. The
    /// boundary code degrades to the layer's own code; prefer
    /// [`Self::lower_layer_coded`] wherever the inner error implements
    /// `code()` (TCP3.2 is converting these layer by layer).
    pub fn lower_layer_with(
        layer: StorageApiLowerLayer,
        reason: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::LowerLayer {
            layer,
            inner_code: None,
            reason,
            source: Some(Arc::new(source)),
        }
    }

    /// Wrap a lower-layer error, carrying its stable code to the boundary
    /// so the specific failure survives the crossing.
    pub fn lower_layer_coded(
        layer: StorageApiLowerLayer,
        inner_code: &'static str,
        reason: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::LowerLayer {
            layer,
            inner_code: Some(inner_code),
            reason,
            source: Some(Arc::new(source)),
        }
    }

    /// The failing inner layer's stable code, when the layer has a
    /// discriminant. `None` for every other variant (their `code()` is
    /// already specific) and for layers TCP3.2 has not wired yet.
    ///
    /// This is what a test asserts to distinguish two lower-layer failures
    /// without reading display text (Hard Rule 29).
    pub const fn inner_code(&self) -> Option<&'static str> {
        match self {
            Self::LowerLayer { inner_code, .. } => *inner_code,
            _ => None,
        }
    }

    pub const fn durable_uncertain(reason: &'static str) -> Self {
        Self::DurableUncertain {
            reason,
            source: None,
        }
    }

    pub fn durable_uncertain_with(
        reason: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::DurableUncertain {
            reason,
            source: Some(Arc::new(source)),
        }
    }
}

impl fmt::Display for StorageApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { field, reason } => {
                write!(formatter, "invalid storage API argument {field}: {reason}")
            }
            Self::UnsupportedCapability { capability, reason } => {
                write!(
                    formatter,
                    "unsupported storage capability {capability}: {reason}"
                )
            }
            Self::InvalidRuntimeState { reason } => {
                write!(formatter, "invalid storage runtime state: {reason}")
            }
            Self::WriterLockHeld => {
                write!(formatter, "the database writer lock is held by another opener")
            }
            Self::IncompatibleLayout { reason } => {
                write!(formatter, "incompatible storage layout: {reason}")
            }
            Self::BranchNotFound { branch_id } => write!(formatter, "branch {branch_id} not found"),
            Self::BranchAlreadyExists { branch_id } => {
                write!(formatter, "branch {branch_id} already exists")
            }
            Self::BranchGenerationMismatch {
                branch_id,
                expected,
                actual,
            } => write!(
                formatter,
                "branch {branch_id} generation mismatch: expected {expected}, actual {actual}"
            ),
            Self::Conflict {
                branch_id, reason, ..
            } => {
                write!(formatter, "branch {branch_id} commit conflict: {reason}")
            }
            Self::RetainedHistoryUnavailable { branch_id, reason } => {
                write!(
                    formatter,
                    "branch {branch_id} retained history unavailable: {reason}"
                )
            }
            Self::TimestampHistoryUnavailable { branch_id, reason } => {
                write!(
                    formatter,
                    "branch {branch_id} timestamp history unavailable: {reason}"
                )
            }
            Self::DurableUncertain { reason, .. } => {
                write!(formatter, "storage durability is uncertain: {reason}")
            }
            Self::RecoveryDegraded { reason } => {
                write!(formatter, "storage recovery is degraded: {reason}")
            }
            Self::MaintenanceRejected { reason } => {
                write!(formatter, "storage maintenance rejected: {reason}")
            }
            Self::StoragePressure {
                branch_id,
                severity,
                pressure_reason,
                reason,
                retryable,
            } => {
                write!(
                    formatter,
                    "branch {branch_id} commit rejected by {severity:?} storage pressure from {pressure_reason:?}: {reason}"
                )?;
                if *retryable {
                    formatter.write_str(" (retryable after maintenance)")?;
                }
                Ok(())
            }
            Self::ResourceExhausted {
                resource,
                requested_bytes,
                used_bytes,
                limit_bytes,
                reason,
            } => write!(
                formatter,
                "storage resource {resource} exhausted: {reason} (requested {requested_bytes} bytes, used {used_bytes}, limit {limit_bytes})"
            ),
            Self::LowerLayer { layer, reason, .. } => {
                write!(formatter, "storage lower layer {layer:?} failed: {reason}")
            }
        }
    }
}

impl Error for StorageApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LowerLayer {
                source: Some(source),
                ..
            }
            | Self::DurableUncertain {
                source: Some(source),
                ..
            } => Some(source.as_ref()),
            _ => None,
        }
    }
}
