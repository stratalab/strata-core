use super::background::{BackgroundExecutorMode, BackgroundShutdownStats};
use super::error::{map_lifecycle_error, map_recovery_health};
use super::{
    perf_trace, CloseOutcome, CloseOutcomeStatus, LifecycleCachePreheatPolicy, LifecycleCodecId,
    LifecycleConfig, LifecycleError, LifecycleMaintenanceSchedulingPolicy,
    LifecycleMaintenanceStats, LifecycleStorageMode, LifecycleStorageOpenOutcome,
    LifecycleWalGrowthPolicy, MaintenanceExecutorStats, RecoveryStrictness, StorageApiError,
    StorageApiResult, StorageBackend, StorageBudgetPolicy, StorageBudgetSource,
    StorageCachePreheatPolicy, StorageCloseEffects, StorageCloseSummary, StorageDurabilityPolicy,
    StorageMaintenanceSchedulingPolicy, StorageMode, StorageOpenDisposition, StorageOpenOptions,
    StorageOpenPlan, StorageOpenSummary, StorageRuntimeBudget, StorageRuntimeState,
    StorageWalGrowthPolicy,
};
use crate::backend::BackendHandle;

/// The storage budget a fresh open uses when no test override is supplied.
///
/// An explicit `memory_budget`, when set, takes precedence (storage derives its per-pool split from
/// the total). Otherwise both cache and durable opens use the configured budget policy: cache obeys
/// the same budget as durable, failing with typed resource errors rather than growing until host
/// memory is exhausted.
pub(super) fn default_open_storage_budget(
    options: &StorageOpenOptions,
) -> StorageApiResult<(StorageRuntimeBudget, StorageBudgetSource)> {
    if let Some(memory_budget) = options.memory_budget() {
        let budget = shape_budget_for_mode(options.mode(), memory_budget.bytes())?;
        return Ok((
            budget,
            StorageBudgetSource::Explicit {
                total_bytes: memory_budget.bytes(),
            },
        ));
    }
    resolve_budget_policy(
        options.budget_policy(),
        options.mode(),
        crate::host_memory::probe(),
    )
}

/// Cache mode holds the whole working set in the mutable pools (no durable
/// tables), so it derives a cache-shaped split — the durable profile capped a
/// cache database's effective capacity at total/8.
fn shape_budget_for_mode(
    mode: StorageMode,
    total_bytes: u64,
) -> StorageApiResult<StorageRuntimeBudget> {
    let budget = if mode == StorageMode::Cache {
        StorageRuntimeBudget::from_total_bytes_for_cache(total_bytes)
    } else {
        StorageRuntimeBudget::from_total_bytes(total_bytes)
    };
    budget.map_err(map_lifecycle_error)
}

/// Resolve a budget policy against host facts (pure: the probe is injected).
/// `DerivedFromHost` shapes the derived total by mode exactly like an explicit
/// budget; when the host reports nothing it falls back to the fixed default
/// rather than failing the open.
pub(super) fn resolve_budget_policy(
    policy: StorageBudgetPolicy,
    mode: StorageMode,
    facts: crate::host_memory::HostMemoryFacts,
) -> StorageApiResult<(StorageRuntimeBudget, StorageBudgetSource)> {
    let fixed = StorageRuntimeBudget::default();
    match policy {
        StorageBudgetPolicy::Default => Ok((
            fixed,
            StorageBudgetSource::FixedDefault {
                total_bytes: fixed.total_bytes(),
            },
        )),
        StorageBudgetPolicy::DerivedFromHost => {
            match crate::host_memory::derive_default_budget(facts) {
                Some(crate::host_memory::DerivedBudget {
                    total_bytes,
                    usable_host_bytes,
                }) => {
                    tracing::info!(
                        total_bytes,
                        usable_host_bytes,
                        "storage memory budget derived from host memory"
                    );
                    Ok((
                        shape_budget_for_mode(mode, total_bytes)?,
                        StorageBudgetSource::DerivedFromHost {
                            total_bytes,
                            usable_host_bytes,
                        },
                    ))
                }
                None => Ok((
                    fixed,
                    StorageBudgetSource::FixedDefault {
                        total_bytes: fixed.total_bytes(),
                    },
                )),
            }
        }
    }
}

pub(super) fn lifecycle_plan(
    options: StorageOpenOptions,
) -> StorageApiResult<(StorageOpenPlan, StorageBudgetSource)> {
    let mode = match options.mode() {
        StorageMode::Cache => LifecycleStorageMode::Cache,
        StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Standard,
        } => LifecycleStorageMode::DurableLocalStandard,
        StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Always,
        } => LifecycleStorageMode::DurableLocalAlways,
        StorageMode::ObjectDurableCandidate | StorageMode::DistributedCandidate => {
            unreachable!("unsupported modes are rejected during validation")
        }
    };
    let recovery = if options.strict_recovery() {
        RecoveryStrictness::Strict
    } else {
        RecoveryStrictness::AllowExplicitLossyFallback
    };
    let mut config = LifecycleConfig::default();
    if !options.strict_recovery() {
        config = LifecycleConfig::new(
            config.max_maintenance_queue_depth(),
            config.max_recovery_faults(),
            config.close_timeout_policy(),
            crate::lifecycle::LifecycleLossyRecoveryPolicy::ExplicitlyAllowed,
        )
        .map_err(map_lifecycle_error)?;
    }
    #[cfg(any(test, feature = "testkit"))]
    let (storage_budget, budget_source) = match options.storage_budget_for_test() {
        Some(budget) => (
            budget,
            StorageBudgetSource::Explicit {
                total_bytes: budget.total_bytes(),
            },
        ),
        None => default_open_storage_budget(&options)?,
    };
    #[cfg(not(any(test, feature = "testkit")))]
    let (storage_budget, budget_source) = default_open_storage_budget(&options)?;
    config = config
        .with_storage_budget(storage_budget)
        .map_err(map_lifecycle_error)?;
    config = config
        .with_wal_growth_policy(map_wal_growth_policy(options.wal_growth_policy()))
        .map_err(map_lifecycle_error)?;
    if let Some(bytes) = options.data_block_bytes() {
        // B2: per-database data-block byte target (validated in options).
        config = config
            .with_data_block_bytes(Some(bytes))
            .map_err(map_lifecycle_error)?;
    }
    // #3499: carry the durable table compression codec (Zstd by default) from
    // the open options into the lifecycle config, whence flush and compaction
    // inherit it.
    config = config.with_table_compression(options.table_compression());
    // #3502 Slice D: carry the MVCC version-retention policy (KeepAll by
    // default) into the lifecycle config, whence the compaction dispatch
    // builds the pruning proof.
    config = config.with_version_retention(options.version_retention());
    config = config
        .with_maintenance_scheduling_policy(map_maintenance_scheduling_policy(
            options.maintenance_scheduling_policy(),
        ))
        .map_err(map_lifecycle_error)?;
    config = config
        .with_cache_preheat_policy(map_cache_preheat_policy(options.cache_preheat_policy()))
        .map_err(map_lifecycle_error)?;
    let plan = StorageOpenPlan::new(mode, LifecycleCodecId::identity(), recovery, config)
        .map_err(map_lifecycle_error)?;
    Ok((plan, budget_source))
}

pub(super) const fn map_cache_preheat_policy(
    policy: StorageCachePreheatPolicy,
) -> LifecycleCachePreheatPolicy {
    match policy {
        StorageCachePreheatPolicy::WhenIdle => LifecycleCachePreheatPolicy::WhenIdle,
        StorageCachePreheatPolicy::Disabled => LifecycleCachePreheatPolicy::Disabled,
    }
}

pub(super) fn durable_backend_handle_for_open(
    options: StorageOpenOptions,
    backend: &StorageBackend,
) -> StorageApiResult<BackendHandle<'static>> {
    // BS4.4i: durable runtimes are uniformly owned/`'static` — the borrowed `Durable` variant is gone.
    // Every real durable backend produces an owned handle (localfs directly; the fault/reordering test
    // backends via their `Arc`-shared state). A backend with no owned handle is in-memory, which cannot
    // back durable-local storage; surface the same policy-appropriate errors the borrowed path used to
    // (Background/DeterministicInline reject the policy — a background worker cannot hold a borrowed
    // backend; EvaluateAndEnqueue/Disabled reject the capability — matching the durable assembler's
    // in-memory rejection), without ever constructing a borrowed runtime.
    #[cfg(feature = "localfs")]
    if let Some(handle) = backend.to_owned_backend_handle() {
        return Ok(handle);
    }
    #[cfg(not(feature = "localfs"))]
    let _ = backend;

    match options.maintenance_scheduling_policy() {
        StorageMaintenanceSchedulingPolicy::Background
        | StorageMaintenanceSchedulingPolicy::DeterministicInline => {
            Err(StorageApiError::InvalidArgument {
                field: "maintenance_scheduling_policy",
                reason: "background and deterministic-inline durable opens require an owned backend handle",
            })
        }
        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue
        | StorageMaintenanceSchedulingPolicy::Disabled => {
            Err(StorageApiError::UnsupportedCapability {
                capability: "durable_local",
                reason: "an in-memory backend cannot satisfy durable-local mode",
            })
        }
    }
}

pub(super) fn map_wal_growth_policy(policy: StorageWalGrowthPolicy) -> LifecycleWalGrowthPolicy {
    match policy {
        StorageWalGrowthPolicy::Default => LifecycleWalGrowthPolicy::default(),
        StorageWalGrowthPolicy::Disabled => LifecycleWalGrowthPolicy::disabled(),
        StorageWalGrowthPolicy::Thresholds {
            max_retained_wal_bytes,
            max_retained_wal_segments,
            max_commits_since_checkpoint,
        } => LifecycleWalGrowthPolicy::new(
            max_retained_wal_bytes,
            max_retained_wal_segments,
            Some(max_commits_since_checkpoint),
        ),
    }
}

pub(super) const fn map_maintenance_scheduling_policy(
    policy: StorageMaintenanceSchedulingPolicy,
) -> LifecycleMaintenanceSchedulingPolicy {
    match policy {
        StorageMaintenanceSchedulingPolicy::Background
        | StorageMaintenanceSchedulingPolicy::DeterministicInline => {
            LifecycleMaintenanceSchedulingPolicy::Background
        }
        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue => {
            LifecycleMaintenanceSchedulingPolicy::EvaluateAndEnqueue
        }
        StorageMaintenanceSchedulingPolicy::Disabled => {
            LifecycleMaintenanceSchedulingPolicy::Disabled
        }
    }
}

pub(super) const fn background_executor_mode(
    policy: StorageMaintenanceSchedulingPolicy,
) -> BackgroundExecutorMode {
    match policy {
        StorageMaintenanceSchedulingPolicy::DeterministicInline => BackgroundExecutorMode::Inline,
        StorageMaintenanceSchedulingPolicy::Background
        | StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue
        | StorageMaintenanceSchedulingPolicy::Disabled => BackgroundExecutorMode::Threaded,
    }
}

pub(super) fn map_open_summary(
    outcome: &LifecycleStorageOpenOutcome,
    mode: StorageMode,
    options: StorageOpenOptions,
    budget_source: StorageBudgetSource,
) -> StorageOpenSummary {
    StorageOpenSummary::with_open_facts(
        mode,
        match outcome.disposition() {
            crate::lifecycle::StorageOpenDisposition::Created => StorageOpenDisposition::Created,
            crate::lifecycle::StorageOpenDisposition::OpenedExisting => {
                StorageOpenDisposition::OpenedExisting
            }
        },
        map_recovery_health(outcome.recovery_health()),
        outcome.recovered_visible_version(),
        outcome.maintenance_ready(),
        options.maintenance_scheduling_policy(),
        outcome.bootstrap().is_some()
            || outcome.checkpoint().is_some()
            || outcome.wal().is_some()
            || outcome.tables().is_some()
            || outcome.quarantine().is_some(),
        outcome.backend_capabilities().is_some(),
        budget_source,
    )
}

pub(super) fn map_close_summary(outcome: CloseOutcome, idempotent: bool) -> StorageCloseSummary {
    StorageCloseSummary::with_close_facts(
        match outcome.status() {
            CloseOutcomeStatus::Complete | CloseOutcomeStatus::Idempotent => {
                StorageRuntimeState::Closed
            }
            CloseOutcomeStatus::Timeout | CloseOutcomeStatus::Failed => StorageRuntimeState::Failed,
        },
        idempotent || matches!(outcome.status(), CloseOutcomeStatus::Idempotent),
        map_close_effects(outcome),
    )
}

pub(super) fn with_background_close_facts(
    summary: StorageCloseSummary,
    background_stats: Option<&MaintenanceExecutorStats>,
) -> StorageCloseSummary {
    background_stats.map_or(summary, |stats| {
        summary.with_background_facts(
            stats.worker_count,
            stats.queue_depth,
            stats.active_tasks,
            stats.tasks_completed,
        )
    })
}

pub(super) fn background_shutdown_panic_error(
    background_shutdown: Option<&BackgroundShutdownStats>,
) -> Option<StorageApiError> {
    let shutdown = background_shutdown?;
    if !shutdown.first_shutdown || shutdown.stats.worker_panics_after_shutdown == 0 {
        return None;
    }
    Some(map_lifecycle_error(LifecycleError::MaintenanceTaskFailed {
        reason: "background maintenance task panicked during close shutdown",
    }))
}

pub(super) fn record_background_close_maintenance_facts(
    before: LifecycleMaintenanceStats,
    after: LifecycleMaintenanceStats,
) {
    perf_trace::record_lifecycle_background_shutdown_canceled_tasks(
        after.canceled().saturating_sub(before.canceled()),
    );
    perf_trace::record_lifecycle_background_shutdown_drained_tasks(
        u64::try_from(after.drained().saturating_sub(before.drained())).unwrap_or(u64::MAX),
    );
}

pub(super) fn map_close_effects(outcome: CloseOutcome) -> StorageCloseEffects {
    let mut effects = StorageCloseEffects::empty();
    if outcome.commits_quiesced() {
        effects = effects.with_commits_quiesced();
    }
    if outcome.maintenance_drained() {
        effects = effects.with_maintenance_drained();
    }
    if outcome.durable_synced() {
        effects = effects.with_durable_synced();
    }
    if outcome.guards_released() {
        effects = effects.with_guards_released();
    }
    effects
}

#[cfg(test)]
mod budget_policy_tests {
    use super::*;
    use crate::host_memory::HostMemoryFacts;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn facts(available: u64) -> HostMemoryFacts {
        HostMemoryFacts {
            available_bytes: Some(available),
            cgroup_limit_bytes: None,
        }
    }

    #[test]
    fn budget_source_total_bytes_returns_the_stored_total() {
        // Kills the total_bytes accessor mutants: each variant's stored total
        // is returned verbatim, never a 0/1 constant.
        assert_eq!(
            StorageBudgetSource::Explicit { total_bytes: 777 }.total_bytes(),
            777
        );
        assert_eq!(
            StorageBudgetSource::DerivedFromHost {
                total_bytes: 888,
                usable_host_bytes: 4 * GIB,
            }
            .total_bytes(),
            888
        );
        assert_eq!(
            StorageBudgetSource::FixedDefault { total_bytes: 999 }.total_bytes(),
            999
        );
    }

    #[test]
    fn default_policy_is_the_fixed_budget_regardless_of_host() {
        let fixed = StorageRuntimeBudget::default();
        let (budget, source) = resolve_budget_policy(
            StorageBudgetPolicy::Default,
            StorageMode::Cache,
            facts(64 * GIB),
        )
        .expect("resolves");
        assert_eq!(budget, fixed);
        assert_eq!(
            source,
            StorageBudgetSource::FixedDefault {
                total_bytes: fixed.total_bytes()
            }
        );
    }

    #[test]
    fn derived_policy_shapes_the_quarter_by_mode_and_reports_provenance() {
        let durable_mode = StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Standard,
        };
        let (durable, source) = resolve_budget_policy(
            StorageBudgetPolicy::DerivedFromHost,
            durable_mode,
            facts(16 * GIB),
        )
        .expect("resolves");
        assert_eq!(durable.total_bytes(), 4 * GIB);
        assert_eq!(
            source,
            StorageBudgetSource::DerivedFromHost {
                total_bytes: 4 * GIB,
                usable_host_bytes: 16 * GIB,
            }
        );
        // Cache mode takes the cache-shaped split of the same derived total.
        let (cache, _) = resolve_budget_policy(
            StorageBudgetPolicy::DerivedFromHost,
            StorageMode::Cache,
            facts(16 * GIB),
        )
        .expect("resolves");
        assert_eq!(cache.total_bytes(), 4 * GIB);
        assert_ne!(cache, durable, "cache and durable splits differ");
        assert_eq!(
            cache,
            StorageRuntimeBudget::from_total_bytes_for_cache(4 * GIB).expect("cache split")
        );
    }

    #[test]
    fn derived_policy_falls_back_to_the_fixed_default_without_host_facts() {
        let fixed = StorageRuntimeBudget::default();
        let (budget, source) = resolve_budget_policy(
            StorageBudgetPolicy::DerivedFromHost,
            StorageMode::Cache,
            HostMemoryFacts::default(),
        )
        .expect("resolves");
        assert_eq!(budget, fixed);
        assert_eq!(
            source,
            StorageBudgetSource::FixedDefault {
                total_bytes: fixed.total_bytes()
            }
        );
    }

    #[test]
    fn derived_policy_honors_the_ceiling_through_the_open_path() {
        let (budget, source) = resolve_budget_policy(
            StorageBudgetPolicy::DerivedFromHost,
            StorageMode::Cache,
            facts(384 * GIB),
        )
        .expect("resolves");
        assert_eq!(budget.total_bytes(), 8 * GIB);
        assert!(matches!(
            source,
            StorageBudgetSource::DerivedFromHost {
                total_bytes,
                ..
            } if total_bytes == 8 * GIB
        ));
    }
}
