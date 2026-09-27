use super::*;
use crate::backend::BackendCapability;
use std::error::Error;
use std::fmt;
use strata_core::CommitVersion;

mod branch_lifecycle;
mod budget;
mod budget_runtime;
mod cache;
mod cache_mode_lifecycle_policy;
mod capability;
mod checkpoint;
mod close;
mod commit_group;
mod commit_hardening;
mod compaction;
mod durable;
mod flush;
mod flush_watermark;
mod footprint;
mod maintenance;
mod quarantine;
mod recovery;
mod retention;
mod state;
mod table_manifest_recovery;
mod table_object_retention;

const FORBIDDEN_DISPLAY_TERMS: [&str; 11] = [
    "Database::open",
    "OpenOptions",
    "ProductRecovery",
    "public maintenance",
    "manual maintenance command",
    "Follower",
    "StrataHub",
    "VersionedValue",
    "EntityRef",
    "JsonValue",
    "IPC",
];

#[test]
fn lifecycle_default_config_is_valid_and_lossless() {
    let config = LifecycleConfig::default();

    config.validate().expect("default config");
    assert!(config.max_maintenance_queue_depth() > 0);
    assert!(config.max_recovery_faults() > 0);
    assert_eq!(
        config.close_timeout_policy(),
        LifecycleCloseTimeoutPolicy::ReturnTypedTimeout
    );
    assert_eq!(
        config.lossy_recovery(),
        LifecycleLossyRecoveryPolicy::Disabled
    );
    assert!(config.wal_growth_policy().enabled());
    assert!(config.wal_growth_policy().max_retained_wal_bytes() > 0);
    assert!(config.wal_growth_policy().max_retained_wal_segments() > 0);
    // Commit-count trigger is off by default: cadence is size-driven.
    assert!(config
        .wal_growth_policy()
        .max_commits_since_checkpoint()
        .is_none());
    assert!(config.maintenance_scheduling_policy().enabled());
    assert_eq!(
        config.maintenance_scheduling_policy(),
        LifecycleMaintenanceSchedulingPolicy::EvaluateAndEnqueue
    );
    assert!(LifecycleMaintenanceSchedulingPolicy::DeterministicInline.enabled());
    assert!(!LifecycleMaintenanceSchedulingPolicy::Disabled.enabled());
}

#[test]
fn lifecycle_explicit_config_preserves_fields() {
    let config = LifecycleConfig::new(
        7,
        3,
        LifecycleCloseTimeoutPolicy::WaitForStorageDrain,
        LifecycleLossyRecoveryPolicy::ExplicitlyAllowed,
    )
    .expect("valid config");

    assert_eq!(config.max_maintenance_queue_depth(), 7);
    assert_eq!(config.max_recovery_faults(), 3);
    assert_eq!(
        config.close_timeout_policy(),
        LifecycleCloseTimeoutPolicy::WaitForStorageDrain
    );
    assert_eq!(
        config.lossy_recovery(),
        LifecycleLossyRecoveryPolicy::ExplicitlyAllowed
    );
}

#[test]
fn lifecycle_config_rejects_zero_limits() {
    assert_eq!(
        LifecycleConfig::new(
            0,
            1,
            LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
            LifecycleLossyRecoveryPolicy::Disabled,
        ),
        Err(LifecycleError::InvalidConfig {
            field: "max_maintenance_queue_depth",
            reason: "must be nonzero",
        })
    );
    assert_eq!(
        LifecycleConfig::new(
            1,
            0,
            LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
            LifecycleLossyRecoveryPolicy::Disabled,
        ),
        Err(LifecycleError::InvalidConfig {
            field: "max_recovery_faults",
            reason: "must be nonzero",
        })
    );
    for (field, policy) in [
        (
            "max_retained_wal_bytes",
            LifecycleWalGrowthPolicy::new(0, 1, Some(1)),
        ),
        (
            "max_retained_wal_segments",
            LifecycleWalGrowthPolicy::new(1, 0, Some(1)),
        ),
        (
            "max_commits_since_checkpoint",
            LifecycleWalGrowthPolicy::new(1, 1, Some(0)),
        ),
        (
            "max_total_wal_bytes",
            LifecycleWalGrowthPolicy::new(1, 1, Some(1)).with_max_total_wal_bytes(0),
        ),
    ] {
        assert!(matches!(
            LifecycleConfig::default().with_wal_growth_policy(policy),
            Err(LifecycleError::InvalidConfig {
                field: actual,
                reason: "must be nonzero when WAL growth policy is enabled",
            }) if actual == field
        ));
    }
}

#[test]
fn lifecycle_config_rejects_invalid_write_throttle_policy() {
    // soft ratio must be strictly inside (0, 1000) so the `1000 - soft` ramp denominator is
    // never 0; max delay must be nonzero. Reasons differ per field, so match on `field` only.
    for (field, policy) in [
        (
            "write_throttle_soft_ratio_permille",
            LifecycleWriteThrottlePolicy::new(0, 20),
        ),
        (
            "write_throttle_soft_ratio_permille",
            LifecycleWriteThrottlePolicy::new(1000, 20),
        ),
        (
            "write_throttle_max_delay_millis",
            LifecycleWriteThrottlePolicy::new(700, 0),
        ),
    ] {
        assert!(matches!(
            LifecycleConfig::default().with_write_throttle_policy(policy),
            Err(LifecycleError::InvalidConfig { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn lifecycle_fact_variants_are_constructible() {
    assert_eq!(LifecycleState::New, LifecycleState::New);
    assert_eq!(LifecycleState::Opening, LifecycleState::Opening);
    assert_eq!(LifecycleState::Recovering, LifecycleState::Recovering);
    assert_eq!(LifecycleState::Open, LifecycleState::Open);
    assert_eq!(LifecycleState::Closing, LifecycleState::Closing);
    assert_eq!(LifecycleState::Closed, LifecycleState::Closed);
    assert_eq!(LifecycleState::Failed, LifecycleState::Failed);

    assert_eq!(StorageMode::Cache, StorageMode::Cache);
    assert_eq!(
        StorageMode::DurableLocalStandard,
        StorageMode::DurableLocalStandard
    );
    assert_eq!(
        StorageMode::DurableLocalAlways,
        StorageMode::DurableLocalAlways
    );
    assert_eq!(
        StorageMode::ObjectDurableCandidate,
        StorageMode::ObjectDurableCandidate
    );

    assert_eq!(LifecycleLowerLayer::Backend, LifecycleLowerLayer::Backend);
    assert_eq!(LifecycleLowerLayer::Layout, LifecycleLowerLayer::Layout);
    assert_eq!(LifecycleLowerLayer::Format, LifecycleLowerLayer::Format);
    assert_eq!(LifecycleLowerLayer::Service, LifecycleLowerLayer::Service);
    assert_eq!(
        LifecycleLowerLayer::TableRuntime,
        LifecycleLowerLayer::TableRuntime
    );
    assert_eq!(
        LifecycleLowerLayer::BranchRuntime,
        LifecycleLowerLayer::BranchRuntime
    );
    assert_eq!(
        LifecycleLowerLayer::CommitRuntime,
        LifecycleLowerLayer::CommitRuntime
    );

    assert_eq!(ClosePhase::QuiesceCommits, ClosePhase::QuiesceCommits);
    assert_eq!(ClosePhase::StopMaintenance, ClosePhase::StopMaintenance);
    assert_eq!(ClosePhase::DrainMaintenance, ClosePhase::DrainMaintenance);
    assert_eq!(ClosePhase::SyncDurableState, ClosePhase::SyncDurableState);
    assert_eq!(ClosePhase::ReleaseGuards, ClosePhase::ReleaseGuards);
    assert_eq!(ClosePhase::Closed, ClosePhase::Closed);
}

#[test]
fn storage_open_plan_validates_storage_mode_and_lossy_policy() {
    let cache = StorageOpenPlan::new(
        StorageMode::Cache,
        LifecycleCodecId::identity(),
        RecoveryStrictness::Strict,
        LifecycleConfig::default(),
    )
    .expect("cache plan");
    assert_eq!(cache.storage_mode(), StorageMode::Cache);
    assert_eq!(cache.codec_id().as_str(), "identity");
    assert_eq!(cache.recovery_policy(), RecoveryStrictness::Strict);
    assert_eq!(
        cache.lifecycle_config().lossy_recovery(),
        LifecycleLossyRecoveryPolicy::Disabled
    );

    let lossy_config = LifecycleConfig::new(
        8,
        8,
        LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
        LifecycleLossyRecoveryPolicy::ExplicitlyAllowed,
    )
    .expect("lossy config");
    let durable = StorageOpenPlan::new(
        StorageMode::DurableLocalStandard,
        LifecycleCodecId::new("identity-v2").expect("codec"),
        RecoveryStrictness::AllowExplicitLossyFallback,
        lossy_config,
    )
    .expect("durable lossy plan");
    assert_eq!(durable.storage_mode(), StorageMode::DurableLocalStandard);

    let durable_always = StorageOpenPlan::new(
        StorageMode::DurableLocalAlways,
        LifecycleCodecId::identity(),
        RecoveryStrictness::Strict,
        LifecycleConfig::default(),
    )
    .expect("durable always strict plan");
    assert_eq!(
        durable_always.storage_mode(),
        StorageMode::DurableLocalAlways
    );
    let durable_always_lossy = StorageOpenPlan::new(
        StorageMode::DurableLocalAlways,
        LifecycleCodecId::identity(),
        RecoveryStrictness::AllowExplicitLossyFallback,
        lossy_config,
    )
    .expect("durable always lossy plan");
    assert_eq!(
        durable_always_lossy.recovery_policy(),
        RecoveryStrictness::AllowExplicitLossyFallback
    );

    let object_candidate = StorageOpenPlan::new(
        StorageMode::ObjectDurableCandidate,
        LifecycleCodecId::identity(),
        RecoveryStrictness::Strict,
        LifecycleConfig::default(),
    )
    .expect("object durable candidate plan");
    assert_eq!(
        object_candidate.storage_mode(),
        StorageMode::ObjectDurableCandidate
    );
    let object_candidate_lossy = StorageOpenPlan::new(
        StorageMode::ObjectDurableCandidate,
        LifecycleCodecId::identity(),
        RecoveryStrictness::AllowExplicitLossyFallback,
        lossy_config,
    )
    .expect("object durable candidate lossy plan");
    assert_eq!(
        object_candidate_lossy.recovery_policy(),
        RecoveryStrictness::AllowExplicitLossyFallback
    );

    assert_eq!(
        StorageOpenPlan::new(
            StorageMode::Cache,
            LifecycleCodecId::identity(),
            RecoveryStrictness::AllowExplicitLossyFallback,
            lossy_config,
        ),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "cache mode cannot request durable recovery fallback",
        })
    );
    assert_eq!(
        StorageOpenPlan::new(
            StorageMode::DurableLocalAlways,
            LifecycleCodecId::identity(),
            RecoveryStrictness::AllowExplicitLossyFallback,
            LifecycleConfig::default(),
        ),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "lossy recovery must be enabled explicitly",
        })
    );
}

#[test]
fn lifecycle_codec_id_rejects_invalid_values() {
    assert_eq!(
        LifecycleCodecId::new(""),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "codec id must not be empty",
        })
    );
    assert_eq!(
        LifecycleCodecId::new("identity\0bad"),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "codec id must not contain null bytes",
        })
    );
    assert_eq!(
        LifecycleCodecId::new("x".repeat(129)),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "codec id is too long",
        })
    );
}

#[test]
fn storage_open_outcome_reports_durable_storage_facts() {
    let durable_always = StorageOpenOutcome::new(
        StorageMode::DurableLocalAlways,
        StorageOpenDisposition::OpenedExisting,
        Some(CommitVersion::new(42)),
        RecoveryHealth::Healthy,
        true,
    )
    .expect("open outcome");

    assert_eq!(durable_always.mode(), StorageMode::DurableLocalAlways);
    assert_eq!(
        durable_always.disposition(),
        StorageOpenDisposition::OpenedExisting
    );
    assert!(durable_always.opened_existing());
    assert_eq!(
        durable_always.recovered_visible_version(),
        Some(CommitVersion::new(42))
    );
    assert!(durable_always.recovery_health().is_healthy());
    assert!(durable_always.maintenance_ready());
    assert_eq!(
        durable_always.recovered_max_commit_version(),
        Some(CommitVersion::new(42))
    );
    assert_eq!(durable_always.backend_capabilities(), None);
    assert_eq!(durable_always.database_id(), None);
    assert_eq!(durable_always.codec_id(), None);
    assert_eq!(durable_always.checkpoint(), None);
    assert_eq!(durable_always.wal(), None);
    assert_eq!(durable_always.tables(), None);
    assert_eq!(durable_always.quarantine(), None);
    assert_eq!(durable_always.bootstrap(), None);
    assert_eq!(durable_always.stats(), LifecycleStats::default());

    let durable_standard = StorageOpenOutcome::new(
        StorageMode::DurableLocalStandard,
        StorageOpenDisposition::Created,
        Some(CommitVersion::new(9)),
        RecoveryHealth::Healthy,
        false,
    )
    .expect("durable standard open outcome");
    assert_eq!(durable_standard.mode(), StorageMode::DurableLocalStandard);
    assert_eq!(
        durable_standard.recovered_visible_version(),
        Some(CommitVersion::new(9))
    );
    assert_eq!(
        durable_standard.disposition(),
        StorageOpenDisposition::Created
    );
    assert!(!durable_standard.maintenance_ready());
}

#[test]
fn storage_open_outcome_reports_cache_and_object_candidate_facts() {
    let cache_outcome = StorageOpenOutcome::new(
        StorageMode::Cache,
        StorageOpenDisposition::Created,
        None,
        RecoveryHealth::Healthy,
        false,
    )
    .expect("cache open outcome");
    assert_eq!(cache_outcome.mode(), StorageMode::Cache);
    assert_eq!(cache_outcome.disposition(), StorageOpenDisposition::Created);
    assert!(!cache_outcome.opened_existing());
    assert_eq!(cache_outcome.recovered_visible_version(), None);
    assert_eq!(cache_outcome.recovered_max_commit_version(), None);
    assert!(!cache_outcome.maintenance_ready());

    let object_candidate = StorageOpenOutcome::new(
        StorageMode::ObjectDurableCandidate,
        StorageOpenDisposition::Created,
        None,
        RecoveryHealth::Healthy,
        false,
    )
    .expect("object candidate open outcome");
    assert_eq!(object_candidate.mode(), StorageMode::ObjectDurableCandidate);
    assert_eq!(object_candidate.recovered_visible_version(), None);
    assert!(!object_candidate.maintenance_ready());
}

#[test]
fn storage_open_outcome_rejects_cache_durable_recovery_claims() {
    assert_eq!(
        StorageOpenOutcome::new(
            StorageMode::Cache,
            StorageOpenDisposition::Created,
            Some(CommitVersion::new(1)),
            RecoveryHealth::Healthy,
            true,
        ),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "cache mode cannot report recovered durable visibility",
        })
    );
    assert_eq!(
        StorageOpenOutcome::new(
            StorageMode::Cache,
            StorageOpenDisposition::OpenedExisting,
            Some(CommitVersion::new(2)),
            RecoveryHealth::Healthy,
            false,
        ),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "cache mode cannot report recovered durable visibility",
        })
    );

    let degraded = RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![RecoveryFault::new(RecoveryFaultKind::IoFailure, "durable recovery").expect("fault")],
    )
    .expect("degraded health");
    assert_eq!(
        StorageOpenOutcome::new(
            StorageMode::Cache,
            StorageOpenDisposition::OpenedExisting,
            None,
            degraded,
            true,
        ),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "cache mode cannot report durable recovery degradation",
        })
    );
}

#[test]
fn recovery_health_requires_degraded_faults() {
    let fault_kinds = [
        RecoveryFaultKind::CorruptManifest,
        RecoveryFaultKind::CorruptSnapshot,
        RecoveryFaultKind::CorruptWal,
        RecoveryFaultKind::MissingManifestObject,
        RecoveryFaultKind::MissingSnapshotObject,
        RecoveryFaultKind::MissingTableObject,
        RecoveryFaultKind::InheritedLayerLoss,
        RecoveryFaultKind::NoManifestFallback,
        RecoveryFaultKind::IoFailure,
        RecoveryFaultKind::QuarantineInventoryMismatch,
        RecoveryFaultKind::TimelineMismatch,
        RecoveryFaultKind::WalTailRepairFailed,
        RecoveryFaultKind::WalCommittedSuffixMissing,
    ];
    assert_eq!(fault_kinds.len(), 13);

    let fault =
        RecoveryFault::new(RecoveryFaultKind::CorruptWal, "crc mismatch").expect("recovery fault");
    let degraded =
        RecoveryHealth::degraded(RecoveryDegradationClass::DataLoss, vec![fault.clone()])
            .expect("degraded health");

    assert_eq!(
        degraded,
        RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::DataLoss,
            faults: vec![fault.clone()],
        }
    );
    assert_eq!(fault.kind(), RecoveryFaultKind::CorruptWal);
    assert_eq!(fault.reason(), "crc mismatch");
    assert_eq!(
        RecoveryHealth::failed(fault.clone()),
        RecoveryHealth::Failed { fault }
    );
    assert_eq!(
        RecoveryHealth::degraded(RecoveryDegradationClass::Telemetry, Vec::new()),
        Err(LifecycleError::RecoveryFailed {
            reason: "degraded recovery health requires at least one fault",
        })
    );
    for class in [
        RecoveryDegradationClass::DataLoss,
        RecoveryDegradationClass::PolicyDowngrade,
        RecoveryDegradationClass::Telemetry,
    ] {
        let health = RecoveryHealth::degraded(
            class,
            vec![RecoveryFault::new(RecoveryFaultKind::IoFailure, "io failed").expect("fault")],
        )
        .expect("degraded health class");
        assert!(matches!(health, RecoveryHealth::Degraded { .. }));
    }
    assert_eq!(
        RecoveryFault::new(RecoveryFaultKind::IoFailure, ""),
        Err(LifecycleError::RecoveryFailed {
            reason: "recovery fault reason must not be empty",
        })
    );
}

#[test]
fn maintenance_facts_are_constructible() {
    let tasks = [
        MaintenanceTaskKind::Flush,
        MaintenanceTaskKind::Checkpoint,
        MaintenanceTaskKind::WalTruncation,
        MaintenanceTaskKind::Compaction,
        MaintenanceTaskKind::Materialization,
        MaintenanceTaskKind::SnapshotPruning,
        MaintenanceTaskKind::Retention,
        MaintenanceTaskKind::Quarantine,
        MaintenanceTaskKind::Purge,
        MaintenanceTaskKind::Repair,
        MaintenanceTaskKind::HealthCollection,
    ];
    for task in tasks {
        assert_eq!(format!("{task:?}"), format!("{task:?}"));
    }

    let maintenance = MaintenanceOutcome::new(
        MaintenanceTaskKind::Checkpoint,
        MaintenanceOutcomeStatus::Deferred,
    )
    .with_recovery_health(RecoveryHealth::Healthy)
    .with_effects(2, 512, true)
    .with_affected_object_names(vec!["snapshot/object".to_owned()])
    .with_reason("checkpoint has no visible rows to publish")
    .with_state_changes(1)
    .with_checkpoint_required(true)
    .with_stats(LifecycleStats::new(0, 0, 1, 0, 0));
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::Checkpoint);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        maintenance.recovery_health(),
        Some(&RecoveryHealth::Healthy)
    );
    assert_eq!(maintenance.affected_objects(), 1);
    assert_eq!(maintenance.affected_object_names(), ["snapshot/object"]);
    assert_eq!(maintenance.bytes_reclaimed(), 512);
    assert!(maintenance.retryable());
    assert_eq!(
        maintenance.reason(),
        Some("checkpoint has no visible rows to publish")
    );
    assert_eq!(
        maintenance.reason_class(),
        Some(MaintenanceOutcomeReasonClass::Deferred)
    );
    assert!(maintenance.source_error().is_none());
    assert_eq!(maintenance.state_changes(), 1);
    assert!(maintenance.checkpoint_required());
    assert_eq!(maintenance.stats().maintenance_tasks(), 1);
    assert_eq!(
        MaintenanceOutcome::new(
            MaintenanceTaskKind::Repair,
            MaintenanceOutcomeStatus::Failed
        )
        .status(),
        MaintenanceOutcomeStatus::Failed
    );
    assert_eq!(
        MaintenanceOutcome::new(
            MaintenanceTaskKind::Flush,
            MaintenanceOutcomeStatus::Completed
        )
        .status(),
        MaintenanceOutcomeStatus::Completed
    );
}

#[test]
fn retention_and_quarantine_facts_are_constructible() {
    assert_eq!(RetentionDecision::Retain, RetentionDecision::Retain);
    assert_eq!(
        RetentionDecision::PruneCandidate,
        RetentionDecision::PruneCandidate
    );
    assert_eq!(
        RetentionDecision::QuarantineCandidate,
        RetentionDecision::QuarantineCandidate
    );
    assert_eq!(
        RetentionDecision::PurgeCandidate,
        RetentionDecision::PurgeCandidate
    );
    assert_eq!(
        RetentionDecision::RepairCandidate,
        RetentionDecision::RepairCandidate
    );
    assert_eq!(
        RetentionDecision::SkipUntilProof,
        RetentionDecision::SkipUntilProof
    );
    assert_eq!(QuarantineStage::Candidate, QuarantineStage::Candidate);
    assert_eq!(
        QuarantineStage::InventoryPublished,
        QuarantineStage::InventoryPublished
    );
    assert_eq!(QuarantineStage::Quarantined, QuarantineStage::Quarantined);
    assert_eq!(
        QuarantineStage::PurgeEligible,
        QuarantineStage::PurgeEligible
    );
}

#[test]
fn close_facts_are_constructible() {
    let close = CloseOutcome::new(ClosePhase::DrainMaintenance, CloseOutcomeStatus::Timeout);
    assert_eq!(close.phase(), ClosePhase::DrainMaintenance);
    assert_eq!(close.status(), CloseOutcomeStatus::Timeout);
    assert_eq!(close.close_fact(), None);
    assert!(!close.commits_quiesced());
    assert!(!close.maintenance_drained());
    assert!(!close.durable_synced());
    assert!(!close.guards_released());
    assert!(!close.prior_final());
    assert_eq!(close.stats(), LifecycleStats::default());
    assert_eq!(
        CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
            .with_close_fact(LifecycleCloseFact::Complete)
            .with_close_effects(CloseOutcomeEffects::durable_complete(false))
            .with_stats(LifecycleStats::new(1, 0, 0, 0, 1))
            .status(),
        CloseOutcomeStatus::Complete
    );
    assert_eq!(
        CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent).status(),
        CloseOutcomeStatus::Idempotent
    );
    assert_eq!(
        CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Failed).status(),
        CloseOutcomeStatus::Failed
    );
}

#[test]
fn lifecycle_stats_default_to_zero_and_preserve_explicit_counts() {
    let empty = LifecycleStats::default();
    assert_eq!(empty.open_attempts(), 0);
    assert_eq!(empty.recovery_faults(), 0);
    assert_eq!(empty.maintenance_tasks(), 0);
    assert_eq!(empty.retention_blocks(), 0);
    assert_eq!(empty.close_attempts(), 0);

    let stats = LifecycleStats::new(1, 2, 3, 4, 5);
    assert_eq!(stats.open_attempts(), 1);
    assert_eq!(stats.recovery_faults(), 2);
    assert_eq!(stats.maintenance_tasks(), 3);
    assert_eq!(stats.retention_blocks(), 4);
    assert_eq!(stats.close_attempts(), 5);
}

#[test]
fn lifecycle_error_core_codes_are_typed() {
    for (error, expected_code) in [
        (
            LifecycleError::InvalidConfig {
                field: "max_recovery_faults",
                reason: "must be nonzero",
            },
            "invalid_argument.lifecycle.config",
        ),
        (
            LifecycleError::InvalidLifecycleState {
                reason: "not ready",
            },
            "failed_precondition.lifecycle.state",
        ),
        (
            LifecycleError::InvalidOpenPlan {
                reason: "mode rejected",
            },
            "invalid_argument.lifecycle.open_plan",
        ),
        (
            LifecycleError::CapabilityMismatch {
                storage_mode: StorageMode::DurableLocalStandard,
                required: vec![BackendCapability::DurablePublish],
                missing: vec![BackendCapability::DurablePublish],
            },
            "failed_precondition.lifecycle.capability",
        ),
        (
            LifecycleError::RecoveryFailed {
                reason: "manifest missing",
            },
            "corruption.lifecycle.recovery",
        ),
        (
            LifecycleError::CloseFailed {
                reason: "durable sync failed",
            },
            "failed_precondition.lifecycle.close",
        ),
        (
            LifecycleError::TimelineRecoveryMismatch {
                reason: "timeline pair mismatch",
            },
            "corruption.lifecycle.timeline",
        ),
        (
            LifecycleError::WalTailRepairRejected {
                reason: "strict recovery cannot repair partial WAL tail",
            },
            "failed_precondition.lifecycle.wal_tail_repair",
        ),
        (
            LifecycleError::RecoveryVisibilityFailed {
                recovered_visible_version: CommitVersion::new(9),
                reason: "visible catch-up rejected",
                source: None,
            },
            "failed_precondition.lifecycle.recovery_visibility",
        ),
    ] {
        assert_lifecycle_error_code(&error, expected_code);
    }
}

#[test]
fn lifecycle_error_maintenance_codes_are_typed() {
    for (error, expected_code) in [
        (
            LifecycleError::MaintenanceFailed {
                reason: "task rejected",
            },
            "failed_precondition.lifecycle.maintenance",
        ),
        (
            LifecycleError::MaintenanceQueueFull {
                reason: "queue full",
            },
            "resource_exhausted.lifecycle.maintenance_queue",
        ),
        (
            LifecycleError::MaintenanceTaskFailed {
                reason: "task rejected",
            },
            "failed_precondition.lifecycle.maintenance_task",
        ),
        (
            LifecycleError::FlushPublicationFailed {
                reason: "table object published but not installed",
            },
            "failed_precondition.lifecycle.flush_publication",
        ),
        (
            LifecycleError::FlushPublicationOrphaned {
                object: Some("tables/orphan".to_owned()),
                reason: "table object published but not installed",
                source: None,
            },
            "ambiguous_commit.lifecycle.flush_publication_orphan",
        ),
        (
            LifecycleError::CheckpointPublicationFailed {
                reason: "snapshot id did not advance",
            },
            "failed_precondition.lifecycle.checkpoint_publication",
        ),
        (
            LifecycleError::RetentionBlocked {
                reason: "proof unavailable",
            },
            "failed_precondition.lifecycle.retention",
        ),
        (
            LifecycleError::WalRetentionProofIncomplete {
                reason: "proof unavailable",
            },
            "failed_precondition.lifecycle.wal_retention",
        ),
    ] {
        assert_lifecycle_error_code(&error, expected_code);
    }
}

#[test]
fn lifecycle_error_source_chain_is_typed() {
    let source = DummySource("backend unavailable");
    let error =
        LifecycleError::lower_layer_with(LifecycleLowerLayer::Backend, "read failed", source);
    assert_eq!(error.code(), "io.lifecycle.backend");
    assert_eq!(
        error.source().expect("source").to_string(),
        "backend unavailable"
    );

    assert_eq!(
        LifecycleError::lower_layer(LifecycleLowerLayer::Service, "publish rejected").to_string(),
        "lifecycle lower layer service failed: publish rejected"
    );
    assert_bounded_storage_display(
        &LifecycleError::lower_layer(LifecycleLowerLayer::Service, "publish rejected").to_string(),
    );
}

fn assert_lifecycle_error_code(error: &LifecycleError, expected_code: &str) {
    assert_eq!(error.code(), expected_code);
    assert_bounded_storage_display(&error.to_string());
}

#[test]
fn lifecycle_debug_output_stays_storage_shaped() {
    let facts = [
        format!("{:?}", StorageMode::DurableLocalStandard),
        format!(
            "{:?}",
            StorageOpenPlan::new(
                StorageMode::Cache,
                LifecycleCodecId::identity(),
                RecoveryStrictness::Strict,
                LifecycleConfig::default(),
            )
            .expect("open plan")
        ),
        format!(
            "{:?}",
            StorageOpenOutcome::new(
                StorageMode::DurableLocalStandard,
                StorageOpenDisposition::OpenedExisting,
                Some(CommitVersion::new(7)),
                RecoveryHealth::Healthy,
                true,
            )
            .expect("open outcome")
        ),
        format!("{:?}", RecoveryHealth::Healthy),
        format!("{:?}", MaintenanceTaskKind::Checkpoint),
        format!("{:?}", RetentionDecision::SkipUntilProof),
        format!("{:?}", QuarantineStage::PurgeEligible),
        format!(
            "{:?}",
            CloseOutcome::new(ClosePhase::ReleaseGuards, CloseOutcomeStatus::Complete)
        ),
    ];

    for fact in facts {
        assert_storage_vocabulary(&fact);
    }
}

#[test]
fn lifecycle_result_alias_uses_lifecycle_error() {
    fn returns_lifecycle_result() -> LifecycleResult<()> {
        Err(LifecycleError::RecoveryFailed {
            reason: "missing checkpoint",
        })
    }

    assert_eq!(
        returns_lifecycle_result(),
        Err(LifecycleError::RecoveryFailed {
            reason: "missing checkpoint",
        })
    );
}

#[derive(Debug)]
struct DummySource(&'static str);

impl fmt::Display for DummySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for DummySource {}

fn assert_bounded_storage_display(text: &str) {
    assert!(text.len() <= 256, "display/debug text is too long: {text}");
    assert_storage_vocabulary(text);
}

fn assert_storage_vocabulary(text: &str) {
    for forbidden in FORBIDDEN_DISPLAY_TERMS {
        assert!(
            !text.contains(forbidden),
            "display/debug text contains forbidden term {forbidden:?}: {text}"
        );
    }
}
