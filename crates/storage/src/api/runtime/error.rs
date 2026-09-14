use super::data::{map_commit_admission_pressure_reason, map_commit_admission_pressure_severity};
#[cfg(any(test, feature = "testkit"))]
use super::maintenance::map_maintenance_summary;
use crate::format::FormatError;
use crate::service::{WalOperation, WalServiceError};

use super::{
    CommitBranchGeneration, LifecycleError, RecoveryHealth, RecoveryHealthSummary, StorageApiError,
    StorageApiLowerLayer, StorageApiResult, DEFAULT_BRANCH_GENERATION,
};
#[cfg(any(test, feature = "testkit"))]
use super::{LifecycleMaintenanceOutcome, MaintenanceRequest, MaintenanceSummary};

/// Surface a storage memory-budget breach as a typed `resource_exhausted` API error.
///
/// Both budget admission paths converge here: the global pre-commit check returns
/// `LifecycleError::StorageBudgetExceeded` directly, while the per-pool commit check
/// wraps the same error under `CommitLowerLayer::StorageBudget`. Routing both through
/// one mapping keeps a budget refusal a caller-actionable resource error instead of an
/// internal lower-layer failure.
fn budget_exceeded_to_api(error: &LifecycleError) -> Option<StorageApiError> {
    match error {
        LifecycleError::StorageBudgetExceeded {
            pool,
            requested_bytes,
            used_bytes,
            limit_bytes,
            reason,
            ..
        } => Some(StorageApiError::ResourceExhausted {
            resource: pool.name(),
            requested_bytes: *requested_bytes,
            used_bytes: *used_bytes,
            limit_bytes: *limit_bytes,
            reason,
        }),
        _ => None,
    }
}

/// Surface a row too large to encode as a typed `invalid_argument` API error.
///
/// The WAL commit payload caps one row at `MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES`.
/// Exceeding it on the write path is deterministic and permanent: the same
/// request can never succeed, and no backend, branch or budget change makes it
/// succeed. Left unmapped it arrives as `LowerLayer` — class `Unavailable`,
/// `RetryPolicy::SameRequest` — which tells the caller to retry forever (#3383).
///
/// `WalOperation::Append` is what separates this from the identically-shaped
/// decode-side check: on a read, an oversized `row_len` means the stored bytes
/// are corrupt, which must keep its corruption classification. Only the encode
/// direction is a caller error.
fn row_too_large_to_api(error: &WalServiceError) -> Option<StorageApiError> {
    match error {
        WalServiceError::Format {
            operation: WalOperation::Append,
            source: FormatError::InvalidLength { field: "row_len" },
            ..
        } => Some(StorageApiError::InvalidArgument {
            field: "row",
            reason: ROW_TOO_LARGE_REASON,
        }),
        _ => None,
    }
}

/// Shared by both refusals of an oversized row — commit admission, which
/// catches it in every durability mode, and the WAL encoder, which still
/// catches the paths that do not pass through admission. One wording so the
/// caller cannot tell, and need not care, which layer refused.
const ROW_TOO_LARGE_REASON: &str = "a single committed row exceeds the maximum encodable size";

/// The table format's separate cap, on the encoded internal key rather than
/// the whole row. Named distinctly because trimming the value does nothing
/// for it — the caller has to shorten the key.
const KEY_TOO_LARGE_REASON: &str = "a committed key exceeds the maximum encodable size";

pub(super) fn branch_error(error: crate::branch::error::BranchRuntimeError) -> StorageApiError {
    match error {
        crate::branch::error::BranchRuntimeError::InsufficientTimestampHistory {
            branch_id,
            ..
        } => StorageApiError::TimestampHistoryUnavailable {
            branch_id,
            reason: "timestamp is outside retained branch history",
        },
        other => {
            // TCP3.2a: `BranchRuntimeError::code()` already existed and was
            // dead — every branch failure reached the engine as one
            // indistinguishable code. Carry it across the boundary instead.
            let code = other.code();
            StorageApiError::lower_layer_coded(
                StorageApiLowerLayer::Branch,
                code,
                "branch read failed",
                other,
            )
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "storage API keeps commit error mapping in one exhaustive registry"
)]
pub(super) fn commit_error(error: crate::commit::CommitRuntimeError) -> StorageApiError {
    match error {
        crate::commit::CommitRuntimeError::InvalidBatch { reason }
        | crate::commit::CommitRuntimeError::InvalidMutation { reason }
        | crate::commit::CommitRuntimeError::InvalidValidationFacts { reason }
        | crate::commit::CommitRuntimeError::InvalidTimestampPolicy { reason } => {
            StorageApiError::InvalidArgument {
                field: "commit",
                reason,
            }
        }
        // Word-for-word what `row_too_large_to_api` returns for the WAL
        // encoder's own refusal, so the caller sees one contract regardless of
        // which layer caught the oversized row (#3383, #3391).
        crate::commit::CommitRuntimeError::MutationTooLarge { .. } => {
            StorageApiError::InvalidArgument {
                field: "row",
                reason: ROW_TOO_LARGE_REASON,
            }
        }
        crate::commit::CommitRuntimeError::MutationKeyTooLarge { .. } => {
            StorageApiError::InvalidArgument {
                field: "key",
                reason: KEY_TOO_LARGE_REASON,
            }
        }
        crate::commit::CommitRuntimeError::DuplicateMutationKey { .. } => {
            StorageApiError::InvalidArgument {
                field: "mutations",
                reason: "commit batch must not contain duplicate keys",
            }
        }
        crate::commit::CommitRuntimeError::StorageOwnedMutationSpace { .. } => {
            StorageApiError::InvalidArgument {
                field: "storage_space",
                reason: "storage-owned commit spaces are not accepted by the API",
            }
        }
        crate::commit::CommitRuntimeError::BranchNotFound { branch_id } => {
            StorageApiError::BranchNotFound { branch_id }
        }
        crate::commit::CommitRuntimeError::BranchAlreadyExists { branch_id } => {
            StorageApiError::BranchAlreadyExists { branch_id }
        }
        crate::commit::CommitRuntimeError::BranchGenerationMismatch {
            branch_id,
            expected,
            actual,
        } => StorageApiError::BranchGenerationMismatch {
            branch_id,
            expected,
            actual,
        },
        crate::commit::CommitRuntimeError::BranchNotWritable { reason, .. }
        | crate::commit::CommitRuntimeError::BranchGuardUnavailable { reason, .. }
        | crate::commit::CommitRuntimeError::CommitQuiesceUnavailable { reason }
        | crate::commit::CommitRuntimeError::BranchUnavailable { reason } => {
            StorageApiError::InvalidRuntimeState { reason }
        }
        crate::commit::CommitRuntimeError::InvalidCommitPhase {
            reason: "read-only diagnostics are disabled",
        } => StorageApiError::UnsupportedCapability {
            capability: "read_only_diagnostics",
            reason: "read-only diagnostics are disabled",
        },
        crate::commit::CommitRuntimeError::CommitConflict { conflict } => {
            StorageApiError::Conflict {
                branch_id: conflict.branch_id(),
                storage_space: Some(conflict.storage_space_id().raw()),
                key_fingerprint: Some(conflict.key_fingerprint()),
                user_key_len: Some(conflict.user_key_len()),
                reason: "commit condition was not satisfied",
            }
        }
        crate::commit::CommitRuntimeError::DurabilityUnavailable { reason } => {
            StorageApiError::UnsupportedCapability {
                capability: "commit_durability",
                reason,
            }
        }
        crate::commit::CommitRuntimeError::DurabilityUncertain { reason, source, .. }
        | crate::commit::CommitRuntimeError::DurableButNotVisible { reason, source, .. } => {
            StorageApiError::DurableUncertain { reason, source }
        }
        crate::commit::CommitRuntimeError::UnresolvedDurableCommit { reason, .. }
        | crate::commit::CommitRuntimeError::AppliedButNotVisible { reason, .. } => {
            StorageApiError::durable_uncertain(reason)
        }
        crate::commit::CommitRuntimeError::InvalidTimelineFact { .. }
        | crate::commit::CommitRuntimeError::TimelineConflict { .. } => {
            let code = error.code();
            StorageApiError::lower_layer_coded(
                StorageApiLowerLayer::Commit,
                code,
                "commit timeline facts are invalid",
                error,
            )
        }
        crate::commit::CommitRuntimeError::LowerLayer {
            layer: crate::commit::CommitLowerLayer::StorageBudget,
            reason,
            source,
        } => {
            let mapped = source
                .as_deref()
                .and_then(|inner| inner.downcast_ref::<LifecycleError>())
                .and_then(budget_exceeded_to_api);
            mapped.unwrap_or(StorageApiError::LowerLayer {
                layer: StorageApiLowerLayer::Commit,
                inner_code: Some(crate::commit::CommitLowerLayer::StorageBudget.code()),
                reason,
                source,
            })
        }
        crate::commit::CommitRuntimeError::LowerLayer {
            layer: crate::commit::CommitLowerLayer::WalService,
            reason,
            source,
        } => {
            let mapped = source
                .as_deref()
                .and_then(|inner| inner.downcast_ref::<WalServiceError>())
                .and_then(row_too_large_to_api);
            mapped.unwrap_or(StorageApiError::LowerLayer {
                layer: StorageApiLowerLayer::Commit,
                inner_code: Some(crate::commit::CommitLowerLayer::WalService.code()),
                reason,
                source,
            })
        }
        other => {
            let code = other.code();
            StorageApiError::lower_layer_coded(
                StorageApiLowerLayer::Commit,
                code,
                "commit runtime failed",
                other,
            )
        }
    }
}

pub(super) fn map_recovery_health(health: &RecoveryHealth) -> RecoveryHealthSummary {
    match health {
        RecoveryHealth::Healthy => RecoveryHealthSummary::Healthy,
        RecoveryHealth::Degraded { .. } => RecoveryHealthSummary::Degraded,
        RecoveryHealth::Failed { .. } => RecoveryHealthSummary::Failed,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "storage API keeps lifecycle error mapping in one exhaustive registry"
)]
pub(super) fn map_lifecycle_error(error: LifecycleError) -> StorageApiError {
    match error {
        LifecycleError::InvalidConfig { field, reason } => {
            StorageApiError::InvalidArgument { field, reason }
        }
        LifecycleError::InvalidOpenPlan { reason } => StorageApiError::InvalidArgument {
            field: "open_options",
            reason,
        },
        LifecycleError::WriterLockHeld => StorageApiError::WriterLockHeld,
        LifecycleError::InvalidLifecycleState { reason }
        | LifecycleError::PinnedViewReleaseBlocked { reason, .. } => {
            StorageApiError::InvalidRuntimeState { reason }
        }
        // Recovery-integrity failures are permanent: the durable state is
        // corrupt or inconsistent, so recovery is refused with a non-retryable
        // recovery failure rather than a transient lower-layer outage a caller
        // would retry forever. `RecoveryCorruption` is a malformed byte stream;
        // `RecoveryFailed` and `TimelineRecoveryMismatch` are already coded
        // `corruption.lifecycle.*` and must surface with a matching class.
        LifecycleError::RecoveryCorruption { reason, .. }
        | LifecycleError::RecoveryFailed { reason }
        | LifecycleError::TimelineRecoveryMismatch { reason } => {
            StorageApiError::RecoveryDegraded { reason }
        }
        LifecycleError::BranchNotFound { branch_id } => {
            StorageApiError::BranchNotFound { branch_id }
        }
        LifecycleError::BranchAlreadyExists { branch_id } => {
            StorageApiError::BranchAlreadyExists { branch_id }
        }
        LifecycleError::BranchGenerationMismatch {
            branch_id,
            expected,
            actual,
        } => StorageApiError::BranchGenerationMismatch {
            branch_id,
            expected,
            actual,
        },
        LifecycleError::BranchNotWritable { state, .. } => {
            StorageApiError::InvalidRuntimeState { reason: state }
        }
        LifecycleError::BranchHasRecoveryDependentChildren { branch_id } => {
            StorageApiError::BranchHasDependentChildren { branch_id }
        }
        LifecycleError::BranchGenerationExhausted { .. } => StorageApiError::InvalidRuntimeState {
            reason: "branch generation is exhausted",
        },
        LifecycleError::BranchHistoryUnavailable { branch_id, reason } => {
            StorageApiError::RetainedHistoryUnavailable { branch_id, reason }
        }
        LifecycleError::InsufficientTimestampHistory { branch_id, reason } => {
            StorageApiError::TimestampHistoryUnavailable { branch_id, reason }
        }
        LifecycleError::SourceHasUnflushedRows { .. } => StorageApiError::InvalidRuntimeState {
            reason: "source branch has unflushed rows",
        },
        LifecycleError::CapabilityMismatch { .. } => StorageApiError::UnsupportedCapability {
            capability: "backend",
            reason: "backend capabilities do not satisfy storage mode",
        },
        LifecycleError::MaintenanceFailed { reason }
        | LifecycleError::MaintenanceQueueFull { reason }
        | LifecycleError::MaintenanceTaskFailed { reason }
        | LifecycleError::RetentionBlocked { reason }
        | LifecycleError::QuarantineProofBlocked { reason }
        | LifecycleError::PurgeProofBlocked { reason }
        | LifecycleError::WalRetentionProofIncomplete { reason }
        | LifecycleError::FlushPublicationFailed { reason }
        | LifecycleError::CheckpointPublicationFailed { reason }
        | LifecycleError::CheckpointSnapshotOrphaned { reason, .. } => {
            StorageApiError::MaintenanceRejected { reason }
        }
        LifecycleError::StoragePressureRejected {
            branch_id,
            severity,
            pressure_reason,
            retryable,
            reason,
            ..
        } => StorageApiError::StoragePressure {
            branch_id,
            severity: map_commit_admission_pressure_severity(severity),
            pressure_reason: map_commit_admission_pressure_reason(pressure_reason),
            reason,
            retryable,
        },
        LifecycleError::FlushPublicationUncertain { reason, source }
        | LifecycleError::FlushPublicationOrphaned { reason, source, .. }
        | LifecycleError::RewritePublicationUncertain { reason, source, .. }
        | LifecycleError::RewritePublicationOrphaned { reason, source, .. }
        | LifecycleError::TableManifestPublicationUncertain { reason, source } => {
            StorageApiError::DurableUncertain { reason, source }
        }
        ref lifecycle @ (LifecycleError::RewritePublicationFailed { .. }
        | LifecycleError::TableManifestPublicationFailed { .. }) => {
            let inner_code = Some(lifecycle.code());
            let (LifecycleError::RewritePublicationFailed { reason, source }
            | LifecycleError::TableManifestPublicationFailed { reason, source }) = error
            else {
                unreachable!("matched a publication-failed variant above")
            };
            StorageApiError::LowerLayer {
                layer: StorageApiLowerLayer::Lifecycle,
                inner_code,
                reason,
                source,
            }
        }
        LifecycleError::LowerLayer {
            layer: crate::lifecycle::LifecycleLowerLayer::CommitRuntime,
            source: Some(source),
            ..
        } => source
            .as_ref()
            .downcast_ref::<crate::commit::CommitRuntimeError>()
            .cloned()
            .map_or_else(
                || {
                    let wrapper = LifecycleError::LowerLayer {
                        layer: crate::lifecycle::LifecycleLowerLayer::CommitRuntime,
                        reason: "commit runtime failed",
                        source: Some(source),
                    };
                    let code = wrapper.code();
                    StorageApiError::lower_layer_coded(
                        StorageApiLowerLayer::Lifecycle,
                        code,
                        "lifecycle commit runtime failed",
                        wrapper,
                    )
                },
                commit_error,
            ),
        LifecycleError::StorageBudgetExceeded {
            pool,
            requested_bytes,
            used_bytes,
            limit_bytes,
            reason,
            ..
        } => StorageApiError::ResourceExhausted {
            resource: pool.name(),
            requested_bytes,
            used_bytes,
            limit_bytes,
            reason,
        },
        other => {
            // #2766: a WAL failure that means the durable log itself is lost
            // or corrupt (e.g. the active object vanished under a live
            // writer) is permanent no matter which lifecycle route wrapped it
            // — close, maintenance, growth. Surfacing it as a generic
            // lower-layer failure invites retries that can never succeed.
            if wal_durable_corruption_in_chain(&other) {
                return StorageApiError::RecoveryDegraded {
                    reason: "WAL durable state is lost or corrupt",
                };
            }
            let code = other.code();
            StorageApiError::lower_layer_coded(
                StorageApiLowerLayer::Lifecycle,
                code,
                "lifecycle runtime failed",
                other,
            )
        }
    }
}

/// #2766: walks the lifecycle error's source chain for a WAL service error
/// classified as durable corruption (vanished active object, undecodable
/// segment, inventory gap) — permanent loss regardless of the wrapping route.
fn wal_durable_corruption_in_chain(error: &LifecycleError) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(current) = source {
        if let Some(wal) = current.downcast_ref::<crate::service::WalServiceError>() {
            return wal.is_durable_corruption();
        }
        source = std::error::Error::source(current);
    }
    false
}

pub(super) fn default_branch_generation() -> StorageApiResult<CommitBranchGeneration> {
    CommitBranchGeneration::new(DEFAULT_BRANCH_GENERATION).map_err(|error| {
        StorageApiError::lower_layer_with(
            StorageApiLowerLayer::Commit,
            "commit branch generation failed",
            error,
        )
    })
}

#[cfg(any(test, feature = "testkit"))]
pub(crate) fn map_commit_error_for_test(
    error: crate::commit::CommitRuntimeError,
) -> StorageApiError {
    commit_error(error)
}

#[cfg(any(test, feature = "testkit"))]
pub(crate) fn map_lifecycle_error_for_test(error: LifecycleError) -> StorageApiError {
    map_lifecycle_error(error)
}

#[cfg(any(test, feature = "testkit"))]
pub(crate) fn map_maintenance_outcome_for_test(
    request: MaintenanceRequest,
    outcome: &LifecycleMaintenanceOutcome,
) -> MaintenanceSummary {
    map_maintenance_summary(request, outcome)
}

#[cfg(test)]
mod tests {
    use super::{
        branch_error, commit_error, map_lifecycle_error, KEY_TOO_LARGE_REASON, ROW_TOO_LARGE_REASON,
    };
    use crate::api::{StorageApiError, StorageApiErrorClass, StorageApiLowerLayer};
    use crate::branch::error::BranchRuntimeError;
    use crate::lifecycle::LifecycleError;

    /// The real mapping — not a hand-built error — must carry the branch
    /// error's own code across the boundary. Before TCP3.2a this arm threw
    /// the discriminant away and every branch failure arrived as one
    /// indistinguishable code, which is why `BranchRuntimeError::code()`
    /// sat behind a `dead_code` allow.
    #[test]
    fn unmapped_branch_errors_carry_their_code_across_the_boundary() {
        let mapped = branch_error(BranchRuntimeError::InvalidReadBound {
            reason: "read bound is not valid",
        });
        assert_eq!(
            mapped.inner_code(),
            Some("failed_precondition.branch.read_bound")
        );
        assert_eq!(mapped.code(), "internal.storage_api.branch");
        assert_eq!(mapped.class(), StorageApiErrorClass::Internal);
        assert!(matches!(
            mapped,
            crate::api::StorageApiError::LowerLayer {
                layer: StorageApiLowerLayer::Branch,
                ..
            }
        ));
    }

    /// Distinct branch failures stay distinct after mapping.
    #[test]
    fn distinct_branch_errors_map_to_distinct_inner_codes() {
        let bound = branch_error(BranchRuntimeError::InvalidReadBound {
            reason: "bad bound",
        });
        let state = branch_error(BranchRuntimeError::InvalidBranchState {
            reason: "bad state",
        });
        assert_ne!(bound.inner_code(), state.inner_code());
        assert_eq!(bound.code(), state.code(), "same layer, same API code");
    }

    /// The explicitly-mapped arm keeps its dedicated API variant: carrying
    /// inner codes must not swallow errors the API already models.
    #[test]
    fn explicitly_mapped_branch_errors_keep_their_api_variant() {
        let mapped = branch_error(BranchRuntimeError::InsufficientTimestampHistory {
            branch_id: strata_core::BranchId::from_bytes([1; 16]),
            requested_timestamp: strata_core::Timestamp::from_micros(1),
            earliest_available_timestamp: Some(strata_core::Timestamp::from_micros(2)),
            source: crate::branch::error::BranchTimestampHistorySource::OwnState,
        });
        assert_eq!(mapped.class(), StorageApiErrorClass::HistoryUnavailable);
        assert_eq!(mapped.code(), "history_unavailable.storage_api.timestamp");
        assert_eq!(mapped.inner_code(), None, "not a LowerLayer error");
    }

    /// TCP3.2b: commit failures that the API does not model reach the engine
    /// through the `other` catch-all. Before this slice they all arrived as
    /// one code; each now carries its own.
    #[test]
    fn unmapped_commit_errors_carry_their_code_across_the_boundary() {
        use crate::commit::CommitRuntimeError;

        let cases = [
            (
                CommitRuntimeError::InvalidCommitState {
                    reason: "bad state",
                },
                "failed_precondition.commit.state",
            ),
            (
                CommitRuntimeError::InvalidVisibilityFacts {
                    reason: "bad facts",
                },
                "failed_precondition.commit.visibility_facts",
            ),
            (
                CommitRuntimeError::VersionAllocatorOverflow {
                    last_allocated: strata_core::CommitVersion::MAX,
                },
                "resource_exhausted.commit.version_allocator",
            ),
            (
                CommitRuntimeError::BranchMismatch {
                    expected: strata_core::BranchId::from_bytes([1; 16]),
                    actual: strata_core::BranchId::from_bytes([2; 16]),
                },
                "invalid_argument.commit.branch_mismatch",
            ),
        ];
        for (error, expected) in cases {
            let mapped = commit_error(error);
            assert_eq!(mapped.inner_code(), Some(expected));
            assert_eq!(mapped.code(), "internal.storage_api.commit");
        }
    }

    /// An oversized row on the WRITE path is a caller error, not an internal
    /// lower-layer failure — and the read path's identically-shaped check must
    /// keep its corruption classification (#3383).
    #[test]
    fn an_oversized_row_maps_by_direction_not_by_layer() {
        use crate::commit::{CommitLowerLayer, CommitRuntimeError};
        use crate::format::FormatError;
        use crate::object::ObjectName;
        use crate::service::{WalOperation, WalServiceError};
        use std::sync::Arc;

        let wal_error = |operation| WalServiceError::Format {
            operation,
            object: ObjectName::new("wal/0000000000000001").expect("object name"),
            source: FormatError::InvalidLength { field: "row_len" },
        };
        let commit_error_for = |operation| {
            commit_error(CommitRuntimeError::LowerLayer {
                layer: CommitLowerLayer::WalService,
                reason: "commit runtime failed",
                source: Some(Arc::new(wal_error(operation))),
            })
        };

        // Encode direction: the caller sent a row too large to encode.
        let write = commit_error_for(WalOperation::Append);
        assert_eq!(write.code(), "invalid_argument.storage_api.argument");
        assert_eq!(
            write.class(),
            super::super::StorageApiErrorClass::InvalidArgument
        );

        // Decode direction: the same inner error means the stored bytes are
        // corrupt. It must NOT be reclassified as a caller error.
        let read = commit_error_for(WalOperation::Read);
        assert_ne!(read.code(), "invalid_argument.storage_api.argument");
        assert_eq!(read.inner_code(), Some("internal.commit.wal_service"));
    }

    /// Commit admission now refuses an oversized row before the WAL encoder
    /// ever sees it, in every durability mode (#3391). That refusal has to
    /// cross the API boundary as the same caller error the encoder's own
    /// refusal produces — unmapped it falls through to `LowerLayer`, which is
    /// class `Unavailable`, and the retry-forever bug of #3383 comes back by
    /// the new route.
    #[test]
    fn an_admission_size_refusal_maps_to_the_same_caller_error_as_the_encoder() {
        use crate::commit::CommitRuntimeError;

        let admission = commit_error(CommitRuntimeError::MutationTooLarge {
            row_len: 16 * 1024 * 1024 + 1,
            max_row_len: 16 * 1024 * 1024,
        });

        assert_eq!(admission.code(), "invalid_argument.storage_api.argument");
        assert_eq!(
            admission.class(),
            super::super::StorageApiErrorClass::InvalidArgument
        );
        // Same field and wording as `row_too_large_to_api`, so a caller cannot
        // tell which layer refused.
        assert!(matches!(
            admission,
            StorageApiError::InvalidArgument {
                field: "row",
                reason,
            } if reason == ROW_TOO_LARGE_REASON
        ));
    }

    /// An oversized key is a caller error like an oversized row, but a
    /// DIFFERENT one: the row refusal tells you to send less data, and trimming
    /// the value does nothing for a key that is too long. Unmapped it falls
    /// through to `LowerLayer` — class `Unavailable` — and becomes a retry
    /// suggestion for a write that can never succeed (#3396).
    #[test]
    fn an_admission_key_size_refusal_names_the_key_not_the_row() {
        use crate::commit::CommitRuntimeError;

        let refusal = commit_error(CommitRuntimeError::MutationKeyTooLarge {
            key_len: 64 * 1024 + 1,
            max_key_len: 64 * 1024,
        });

        assert_eq!(refusal.code(), "invalid_argument.storage_api.argument");
        assert_eq!(
            refusal.class(),
            super::super::StorageApiErrorClass::InvalidArgument
        );
        assert!(matches!(
            refusal,
            StorageApiError::InvalidArgument {
                field: "key",
                reason,
            } if reason == KEY_TOO_LARGE_REASON
        ));
        assert_ne!(
            KEY_TOO_LARGE_REASON, ROW_TOO_LARGE_REASON,
            "the two refusals must not be indistinguishable: their remedies differ"
        );
    }

    /// A fork source with live recovery-dependent children crosses the
    /// boundary as its own code.
    ///
    /// It used to arrive as the class-generic
    /// `failed_precondition.storage_api.state`, which the engine renders as
    /// "persistence is temporarily unable to accept the request" with a
    /// wait-and-retry remedy — so callers retried a permanent DAG constraint.
    /// The whole-database simulation had to string-match the reason prefix to
    /// tell this refusal from a real error (#3196).
    #[test]
    fn a_fork_source_with_dependent_children_crosses_the_boundary_as_its_own_code() {
        use strata_core::BranchId;

        let branch_id = BranchId::from_bytes([0x33; BranchId::BYTE_LEN]);
        let lifecycle = LifecycleError::BranchHasRecoveryDependentChildren { branch_id };
        assert_eq!(
            lifecycle.code(),
            "failed_precondition.lifecycle.branch_dependent_children"
        );
        assert_ne!(
            lifecycle.code(),
            LifecycleError::BranchNotWritable {
                branch_id,
                state: "something else",
            }
            .code(),
            "the dependency refusal must not share the generic not-writable code"
        );

        let refusal = map_lifecycle_error(lifecycle);

        assert_eq!(
            refusal.code(),
            "failed_precondition.storage_api.branch_dependent_children"
        );
        assert_eq!(refusal.class(), StorageApiErrorClass::FailedPrecondition);
        assert!(matches!(
            refusal,
            StorageApiError::BranchHasDependentChildren { branch_id: reported }
                if reported == branch_id
        ));
        // It must NOT collapse into the generic state code, which is shared
        // with branch-not-writable, guard-unavailable and quiesce-unavailable.
        assert_ne!(refusal.code(), "failed_precondition.storage_api.state");
    }

    /// Writer-lock contention crosses the API boundary as its own code, not as
    /// the `FailedPrecondition` class default. The IPC broker recognises this
    /// exact code to decide whether to hand a command to a running host, and
    /// `failed_precondition.storage_api.state` is shared with unrelated
    /// preconditions (#3005, #3167).
    #[test]
    fn writer_lock_contention_crosses_the_boundary_as_its_own_code() {
        let mapped = map_lifecycle_error(LifecycleError::WriterLockHeld);
        assert_eq!(mapped.code(), "failed_precondition.storage_api.writer_lock");
        assert_eq!(mapped.class(), StorageApiErrorClass::FailedPrecondition);
        assert!(matches!(mapped, StorageApiError::WriterLockHeld));

        // Direction control: an ordinary runtime-state failure keeps the shared
        // class code, so the broker cannot mistake it for contention.
        let other = map_lifecycle_error(LifecycleError::InvalidLifecycleState {
            reason: "some other precondition",
        });
        assert_eq!(other.code(), "failed_precondition.storage_api.state");
    }

    /// The timeline arm has its own reason string but must still carry the
    /// variant's code — two failures sharing a reason stay distinguishable.
    #[test]
    fn timeline_commit_errors_are_distinguishable_despite_a_shared_reason() {
        use crate::commit::CommitRuntimeError;

        let fact = commit_error(CommitRuntimeError::InvalidTimelineFact { reason: "bad" });
        let conflict = commit_error(CommitRuntimeError::TimelineConflict { reason: "bad" });
        assert_eq!(
            fact.inner_code(),
            Some("failed_precondition.commit.timeline_fact")
        );
        assert_eq!(conflict.inner_code(), Some("conflict.commit.timeline"));
        assert_ne!(fact.inner_code(), conflict.inner_code());
    }

    /// A commit error wrapping a lower sub-layer reports that sub-layer.
    #[test]
    fn commit_lower_layer_errors_report_their_sub_layer() {
        use crate::commit::{CommitLowerLayer, CommitRuntimeError};

        let mapped = commit_error(CommitRuntimeError::lower_layer(
            CommitLowerLayer::WalService,
            "wal service failed",
        ));
        assert_eq!(mapped.inner_code(), Some("internal.commit.wal_service"));
    }

    /// Every commit code must be a well-formed 3-part code (rule 27) whose
    /// class is one the contract defines, and no two variants may share a
    /// code — a copy-paste duplicate would silently collapse two failures
    /// back into one, which is the defect this slice removes.
    #[expect(
        clippy::too_many_lines,
        reason = "the exhaustive variant/code table is the point: every commit variant is pinned in one place"
    )]
    #[test]
    fn commit_codes_are_well_formed_and_unique() {
        use crate::commit::{CommitConflict, CommitLowerLayer, CommitRuntimeError};
        const CLASSES: &[&str] = &[
            "not_found",
            "already_exists",
            "invalid_argument",
            "failed_precondition",
            "conflict",
            "ambiguous_commit",
            "unsupported",
            "resource_exhausted",
            "unavailable",
            "internal",
        ];
        let branch = strata_core::BranchId::from_bytes([1; 16]);
        let version = strata_core::CommitVersion::new(1);
        let space = crate::row::StorageSpaceId::engine(0x20).expect("engine-owned space");
        let every_variant: [(CommitRuntimeError, &str); 30] = [
            (
                CommitRuntimeError::InvalidConfig {
                    field: "f",
                    reason: "r",
                },
                "invalid_argument.commit.config",
            ),
            (
                CommitRuntimeError::InvalidCommitState { reason: "r" },
                "failed_precondition.commit.state",
            ),
            (
                CommitRuntimeError::InvalidCommitPhase { reason: "r" },
                "failed_precondition.commit.phase",
            ),
            (
                CommitRuntimeError::InvalidVisibilityFacts { reason: "r" },
                "failed_precondition.commit.visibility_facts",
            ),
            (
                CommitRuntimeError::InvalidBatch { reason: "r" },
                "invalid_argument.commit.batch",
            ),
            (
                CommitRuntimeError::InvalidMutation { reason: "r" },
                "invalid_argument.commit.mutation",
            ),
            (
                CommitRuntimeError::InvalidValidationFacts { reason: "r" },
                "invalid_argument.commit.validation_facts",
            ),
            (
                CommitRuntimeError::InvalidTimelineFact { reason: "r" },
                "failed_precondition.commit.timeline_fact",
            ),
            (
                CommitRuntimeError::TimelineConflict { reason: "r" },
                "conflict.commit.timeline",
            ),
            (
                CommitRuntimeError::DuplicateMutationKey { space_id: space },
                "invalid_argument.commit.duplicate_mutation_key",
            ),
            (
                CommitRuntimeError::BranchMismatch {
                    expected: branch,
                    actual: branch,
                },
                "invalid_argument.commit.branch_mismatch",
            ),
            (
                CommitRuntimeError::BranchAlreadyExists { branch_id: branch },
                "already_exists.commit.branch",
            ),
            (
                CommitRuntimeError::BranchNotFound { branch_id: branch },
                "not_found.commit.branch",
            ),
            (
                CommitRuntimeError::BranchNotWritable {
                    branch_id: branch,
                    reason: "r",
                },
                "failed_precondition.commit.branch_not_writable",
            ),
            (
                CommitRuntimeError::BranchGenerationMismatch {
                    branch_id: branch,
                    expected: 1,
                    actual: 2,
                },
                "failed_precondition.commit.branch_generation",
            ),
            (
                CommitRuntimeError::BranchGenerationExhausted {
                    branch_id: branch,
                    generation: 1,
                },
                "resource_exhausted.commit.branch_generation",
            ),
            (
                CommitRuntimeError::BranchGuardUnavailable {
                    branch_id: branch,
                    reason: "r",
                },
                "failed_precondition.commit.branch_guard",
            ),
            (
                CommitRuntimeError::CommitQuiesceUnavailable { reason: "r" },
                "failed_precondition.commit.quiesce",
            ),
            (
                CommitRuntimeError::CommitConflict {
                    conflict: CommitConflict::new(
                        crate::commit::CommitConflictKind::ReadSet,
                        &crate::row::PhysicalKey::new(branch, "default", space, b"k".to_vec())
                            .expect("physical key"),
                        crate::commit::CommitObservedVersion::Present(version),
                        crate::commit::CommitObservedVersion::Missing,
                    ),
                },
                "conflict.commit.condition",
            ),
            (
                CommitRuntimeError::DurabilityUncertain {
                    branch_id: branch,
                    commit_version: version,
                    reason: "r",
                    source: None,
                },
                "ambiguous_commit.commit.durability_uncertain",
            ),
            (
                CommitRuntimeError::DurableButNotVisible {
                    branch_id: branch,
                    commit_version: version,
                    reason: "r",
                    source: None,
                },
                "ambiguous_commit.commit.durable_not_visible",
            ),
            (
                CommitRuntimeError::UnresolvedDurableCommit {
                    branch_id: branch,
                    commit_version: version,
                    reason: "r",
                },
                "ambiguous_commit.commit.unresolved_durable",
            ),
            (
                CommitRuntimeError::AppliedButNotVisible {
                    branch_id: branch,
                    commit_version: version,
                    reason: "r",
                },
                "ambiguous_commit.commit.applied_not_visible",
            ),
            (
                CommitRuntimeError::StorageOwnedMutationSpace { space_id: space },
                "invalid_argument.commit.storage_owned_space",
            ),
            (
                CommitRuntimeError::BranchUnavailable { reason: "r" },
                "failed_precondition.commit.branch_unavailable",
            ),
            (
                CommitRuntimeError::DurabilityUnavailable { reason: "r" },
                "unsupported.commit.durability",
            ),
            (
                CommitRuntimeError::VersionAllocatorOverflow {
                    last_allocated: version,
                },
                "resource_exhausted.commit.version_allocator",
            ),
            (
                CommitRuntimeError::TimestampUnavailable {
                    reason: "r",
                    source: None,
                },
                "unavailable.commit.timestamp",
            ),
            (
                CommitRuntimeError::InvalidTimestampPolicy { reason: "r" },
                "invalid_argument.commit.timestamp_policy",
            ),
            (
                CommitRuntimeError::lower_layer(CommitLowerLayer::BranchRuntime, "r"),
                "internal.commit.branch_runtime",
            ),
        ];

        let mut seen = std::collections::BTreeMap::new();
        for (error, expected) in &every_variant {
            let code = error.code();
            assert_eq!(&code, expected, "code drifted for {error:?}");
            let parts: Vec<&str> = code.split('.').collect();
            assert_eq!(
                parts.len(),
                3,
                "code must be <class>.<area>.<detail>: {code}"
            );
            assert!(CLASSES.contains(&parts[0]), "unknown class in {code}");
            assert_eq!(parts[1], "commit", "area must be `commit`: {code}");
            if let Some(previous) = seen.insert(code, format!("{error:?}")) {
                panic!("two commit variants share the code {code}: {previous} and {error:?}");
            }
        }
        // Every CommitLowerLayer sub-layer is pinned and distinct too.
        let sub_layers = [
            (
                CommitLowerLayer::BranchRuntime,
                "internal.commit.branch_runtime",
            ),
            (
                CommitLowerLayer::StorageBudget,
                "internal.commit.storage_budget",
            ),
            (CommitLowerLayer::WalFormat, "internal.commit.wal_format"),
            (CommitLowerLayer::WalService, "internal.commit.wal_service"),
        ];
        let mut distinct = std::collections::BTreeSet::new();
        for (layer, expected) in sub_layers {
            assert_eq!(layer.code(), expected, "sub-layer code drifted");
            assert!(distinct.insert(layer.code()), "sub-layers share a code");
        }
        assert_eq!(distinct.len(), sub_layers.len());
    }

    /// TCP3.2c: lifecycle failures the API does not model now carry the
    /// lifecycle layer's own code across the boundary instead of one
    /// constant.
    #[test]
    fn unmapped_lifecycle_errors_carry_their_code_across_the_boundary() {
        use crate::lifecycle::LifecycleError;

        let mapped = map_lifecycle_error(LifecycleError::CloseFailed { reason: "boom" });
        assert_eq!(
            mapped.inner_code(),
            Some("failed_precondition.lifecycle.close")
        );
        assert_eq!(mapped.code(), "internal.storage_api.lifecycle");
    }

    /// The two publication-failed variants keep distinct codes despite
    /// sharing the same boundary arm.
    #[test]
    fn lifecycle_publication_failures_stay_distinguishable() {
        use crate::lifecycle::LifecycleError;

        let rewrite = map_lifecycle_error(LifecycleError::RewritePublicationFailed {
            reason: "r",
            source: None,
        });
        let manifest = map_lifecycle_error(LifecycleError::TableManifestPublicationFailed {
            reason: "r",
            source: None,
        });
        assert!(rewrite.inner_code().is_some());
        assert!(manifest.inner_code().is_some());
        assert_ne!(rewrite.inner_code(), manifest.inner_code());
    }

    /// Recovery-integrity failures are coded corruption; they must surface as
    /// the permanent, non-retryable recovery-degraded class rather than falling
    /// to the catch-all `internal.storage_api.lifecycle` (which the engine maps
    /// to a retryable outage a caller would loop on forever).
    #[test]
    fn recovery_integrity_failures_map_to_non_retryable_corruption() {
        use crate::api::StorageApiError;
        use crate::lifecycle::LifecycleError;

        for error in [
            LifecycleError::RecoveryFailed {
                reason: "replay failed",
            },
            LifecycleError::TimelineRecoveryMismatch {
                reason: "timeline gap",
            },
        ] {
            let mapped = map_lifecycle_error(error);
            assert!(
                matches!(mapped, StorageApiError::RecoveryDegraded { .. }),
                "recovery-integrity failure must map to RecoveryDegraded, got {}",
                mapped.code()
            );
            assert_eq!(
                mapped.code(),
                "failed_precondition.storage_api.recovery_degraded"
            );
        }
    }
}
