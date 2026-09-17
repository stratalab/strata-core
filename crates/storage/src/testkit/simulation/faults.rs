//! The fault-combination dimension of the simulation: interleaving × fault/crash.
//!
//! STH-2 sweeps a *fixed* workload against every backend-op fault; STH-3 crashes a
//! *fixed* workload under every filesystem model. This crosses those fault and crash
//! substrates with a *seeded interleaving* — client commits interspersed with
//! maintenance drained at a sim-chosen cadence — so the fault or power-loss lands at
//! a seed-determined point of a *varied* schedule, not one fixed shape. Recovery is
//! verified against the STH-1 oracle: under a backend-op fault every acknowledged
//! commit survives (the faulted commit is in-doubt); under a power-loss crash the
//! durability contract holds (`Always` loses nothing, `Standard` a clean prefix).
//!
//! TCP4.11a: this lane runs under `DeterministicInline` exactly like the clean
//! driver — the fault/reordering decorators are `Clone` with `Arc`-shared state
//! (BS4.4h), so durable opens take the same owned-handle path, with the manual
//! maintenance clock wired and seeded clock advancement in the step grammar.
//! Every backend is additionally wrapped in the write-ordering watchdog: the
//! ordering oracle observes each faulted trajectory continuously, and CONFIRMED
//! violations (raced bytes later discarded — no innocent explanation, #2785)
//! fail the case even though a faulted run never quiesces cleanly. Each case's
//! deterministic facts replay bit-exact from the seed — the same guarantee the
//! clean lane pins.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use strata_core::BranchId;

use crate::api::{
    CommitBatch, CommitOptions, MaintenanceRequest, MaintenanceScope, MaintenanceTask,
    StorageBackend, StorageDurabilityPolicy, StorageMaintenanceSchedulingPolicy,
    StorageOpenOptions, StorageRuntime,
};
use crate::testkit::recovery_oracle::model::{ExpectedState, OracleDurability};
use crate::testkit::recovery_oracle::verify::{
    classify_recovered, scan_recovered, CrashFamily, RecoveryOracleViolation,
};
use crate::testkit::recovery_oracle::workload::{
    default_branch, generate_workload, oracle_prefix_key, oracle_space, to_commit_mutation,
    SCAN_LIMIT,
};
use crate::testkit::rng::SplitMix64;
use crate::testkit::{
    BackendOperation, FaultKind, FaultMode, FaultRule, FaultScript, FsModel, TestkitError,
};

/// Default seed count with no case budget (each seed runs one fault + one crash case).
const DEFAULT_FAULT_SIM_SEEDS: u64 = 4;
/// Interleaved action steps per case — small so the bounded sweep stays cheap.
const FAULT_SIM_STEPS: usize = 24;
/// Decorrelate the interleaving's action stream from its workload (both seeded from `seed`).
const STEP_SALT: u64 = 0x4661_756c_7453_696d;
/// The V1-reachable write-side backend operations a fault can land on.
const FAULT_OPS: [BackendOperation; 4] = [
    BackendOperation::AppendObject,
    BackendOperation::SyncObject,
    BackendOperation::PublishObject,
    BackendOperation::DeleteObject,
];
/// Every filesystem persistence model.
const FS_MODELS: [FsModel; 4] = [
    FsModel::OrderedAtomic,
    FsModel::ReorderedAppends,
    FsModel::GarbageUnsyncedTail,
    FsModel::SplitRename,
];

/// Counters describing a fault-simulation sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SimulationFaultOutcome {
    seeds_executed: usize,
    fault_cases: usize,
    crash_cases: usize,
    fault_fired_cases: usize,
    crash_perturbed_cases: usize,
    fail_loud_cases: usize,
}

impl SimulationFaultOutcome {
    #[must_use]
    pub const fn seeds_executed(&self) -> usize {
        self.seeds_executed
    }
    #[must_use]
    pub const fn fault_cases(&self) -> usize {
        self.fault_cases
    }
    #[must_use]
    pub const fn crash_cases(&self) -> usize {
        self.crash_cases
    }
    /// Backend-op fault cases where the scheduled fault actually fired (non-vacuousness).
    #[must_use]
    pub const fn fault_fired_cases(&self) -> usize {
        self.fault_fired_cases
    }
    /// Power-loss cases where the crash actually perturbed the on-disk state.
    #[must_use]
    pub const fn crash_perturbed_cases(&self) -> usize {
        self.crash_perturbed_cases
    }
    /// Power-loss cases where a corrupt tail was rejected fail-loud (not silently wrong).
    #[must_use]
    pub const fn fail_loud_cases(&self) -> usize {
        self.fail_loud_cases
    }
}

/// Deterministic-inline options: the production `Background` scheduling logic on
/// the inline executor with the manual clock — the same path as the clean driver.
pub(super) fn deterministic_options(durability: StorageDurabilityPolicy) -> StorageOpenOptions {
    StorageOpenOptions::durable_local(durability)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::DeterministicInline)
}

/// Probe that the just-opened runtime exposes the manual maintenance clock; a
/// `false` means the deterministic seam is not wired and the case is invalid.
pub(super) fn require_manual_clock(
    runtime: &StorageRuntime<'_>,
    seed: u64,
) -> Result<(), TestkitError> {
    if runtime.advance_maintenance_clock_for_test(std::time::Duration::ZERO) {
        Ok(())
    } else {
        Err(TestkitError::new(format!(
            "[seed={seed}] deterministic-inline runtime exposes no manual maintenance clock"
        )))
    }
}

/// Confirmed-only ordering check: a faulted/crashed run never quiesces, so
/// pending entries are excluded (#2785) — but a CONFIRMED violation is a real
/// ordering bug regardless of the injected fault.
pub(super) fn require_no_confirmed_ordering_violations(
    backend: &StorageBackend,
    seed: u64,
    label: &str,
) -> Result<(usize, usize), TestkitError> {
    let report = backend
        .write_ordering_confirmed_report()
        .ok_or_else(|| TestkitError::new(format!("[seed={seed}] {label}: watchdog missing")))?;
    if !report.violations().is_empty() {
        return Err(TestkitError::new(format!(
            "[seed={seed}] {label}: confirmed write-ordering violations under fault: {:?}",
            report.violations()
        )));
    }
    Ok((report.violations().len(), report.publishes_checked()))
}

fn case_dir(root: &Path, label: &str) -> Result<PathBuf, TestkitError> {
    let dir = root.join(label);
    std::fs::create_dir_all(&dir)
        .map_err(|err| TestkitError::new(format!("create case dir: {err}")))?;
    Ok(dir)
}

fn swept_op_count(backend: &StorageBackend, op: BackendOperation) -> usize {
    backend
        .fault_calls()
        .iter()
        .filter(|call| call.operation() == op)
        .count()
}

/// One interleaved action against the runtime.
#[derive(Clone, Copy, Debug)]
enum SimStep {
    Commit,
    DrainMaintenance,
    EnqueueCheckpoint,
    EnqueueFlush,
    EnqueueSnapshotPruning,
    /// Advance the manual maintenance clock by a seeded jitter (ms).
    AdvanceClock(u64),
}

impl SimStep {
    const fn label(self) -> &'static str {
        match self {
            SimStep::Commit => "commit",
            SimStep::DrainMaintenance => "drain_maintenance",
            SimStep::EnqueueCheckpoint => "enqueue_checkpoint",
            SimStep::EnqueueFlush => "enqueue_flush",
            SimStep::EnqueueSnapshotPruning => "enqueue_snapshot_pruning",
            SimStep::AdvanceClock(_) => "advance_clock",
        }
    }
}

fn draw_step(rng: &mut SplitMix64) -> SimStep {
    match rng.gen_u8_below(8) {
        // Bias toward commits so there is real write load to maintain against.
        0..=2 => SimStep::Commit,
        3 => SimStep::DrainMaintenance,
        4 => SimStep::EnqueueCheckpoint,
        5 => SimStep::EnqueueFlush,
        // Pruning issues the backend deletes that exercise the DeleteObject fault.
        6 => SimStep::EnqueueSnapshotPruning,
        _ => SimStep::AdvanceClock(u64::from(rng.gen_u8_below(50))),
    }
}

/// Deterministic facts of one interleaved drive — state and sequencing only,
/// never wall-clock numbers, so a seed replays them bit-exact.
#[derive(Clone, Debug, Default, PartialEq)]
struct InterleaveFacts {
    step_trace: Vec<&'static str>,
    acked_versions: Vec<u64>,
    in_doubt_commits: usize,
    clock_advances: usize,
}

/// Drive a seeded interleaving of commits + maintenance for `steps` actions, recording
/// every acknowledgement. The first commit that a backend fault rejects is recorded
/// in-doubt and stops the workload (later commits would diverge from the model);
/// maintenance faults are non-destructive and tolerated (errors discarded).
fn drive_interleaved(
    runtime: &mut StorageRuntime<'_>,
    branch: BranchId,
    seed: u64,
    steps: usize,
    model: &mut ExpectedState,
) -> Result<InterleaveFacts, TestkitError> {
    let mut rng = SplitMix64::new(seed ^ STEP_SALT);
    let workload = generate_workload(seed, steps.max(1));
    let mut commit_index = 0usize;
    let mut facts = InterleaveFacts::default();
    for _ in 0..steps {
        let step = draw_step(&mut rng);
        facts.step_trace.push(step.label());
        match step {
            SimStep::Commit => {
                if commit_index >= workload.len() {
                    continue;
                }
                let mutations = workload[commit_index].clone();
                commit_index += 1;
                let batch = CommitBatch::new(
                    branch,
                    mutations.iter().map(to_commit_mutation).collect(),
                    CommitOptions::default(),
                )
                .map_err(|err| TestkitError::new(format!("build batch: {err:?}")))?;
                if let Ok(summary) = runtime.commit(&batch) {
                    facts.acked_versions.push(summary.commit_version().as_u64());
                    model.record_ack(branch, summary.commit_version(), mutations);
                } else {
                    facts.in_doubt_commits += 1;
                    model.record_in_doubt(branch, mutations);
                    return Ok(facts);
                }
            }
            SimStep::DrainMaintenance => {
                let _ = runtime.drain_maintenance();
            }
            SimStep::EnqueueCheckpoint => {
                let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Branch(branch),
                ));
            }
            SimStep::EnqueueFlush => {
                let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Flush,
                    MaintenanceScope::Branch(branch),
                ));
            }
            SimStep::EnqueueSnapshotPruning => {
                let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::SnapshotPruning,
                    MaintenanceScope::Global,
                ));
            }
            SimStep::AdvanceClock(ms) => {
                if runtime.advance_maintenance_clock_for_test(std::time::Duration::from_millis(ms))
                {
                    facts.clock_advances += 1;
                }
            }
        }
    }
    Ok(facts)
}

/// Result of one backend-op fault case: the verdict plus deterministic facts
/// that must replay bit-exact from the seed.
#[derive(Debug)]
struct FaultCaseRun {
    fired: bool,
    violation: Option<RecoveryOracleViolation>,
    /// Read by the derived `Debug`, which every assertion in this file prints
    /// on failure (`"scheduled fault did not fire: {run:?}"`). Dead-code
    /// analysis ignores derived impls, and says so in its own note.
    #[allow(dead_code)]
    facts: FaultCaseFacts,
}

#[derive(Clone, Debug, PartialEq)]
struct FaultCaseFacts {
    interleave: InterleaveFacts,
    ordering_publishes_checked: usize,
    recovered_rows: usize,
}

/// Inject a seed-chosen backend-op fault during a seeded interleaving (STH-2 substrate),
/// then reopen clean and check recovery against the oracle. Faults fire *before* the
/// inner op, so the recovered state is a clean prefix of acknowledged history.
fn run_one_fault_case(root: &Path, seed: u64) -> Result<FaultCaseRun, TestkitError> {
    let branch = default_branch();
    let mut model = ExpectedState::new(OracleDurability::Always);

    let op = FAULT_OPS[usize::try_from(seed % 4).unwrap_or(0)];
    let call = 1 + (seed >> 3) % 6;
    let mode = if seed & 1 == 0 {
        FaultMode::Once
    } else {
        FaultMode::Continuously
    };
    let kind = if seed & 2 == 0 {
        FaultKind::Unavailable
    } else {
        FaultKind::NoSpace
    };

    let (fired, interleave, ordering_publishes_checked) = {
        let nz = NonZeroU64::new(call).ok_or_else(|| TestkitError::new("call must be non-zero"))?;
        let backend = StorageBackend::write_ordering_faulting_local_fs(
            root.to_path_buf(),
            FaultScript::new([FaultRule::with_mode(op, nz, kind, mode)]),
        );
        // The fault may land during open or the workload; either surfaces as a typed
        // error, never a panic.
        let interleave = if let Ok(outcome) = StorageRuntime::open_with_backend(
            deterministic_options(StorageDurabilityPolicy::Always),
            &backend,
        ) {
            let mut runtime = outcome.into_runtime();
            require_manual_clock(&runtime, seed)?;
            drive_interleaved(&mut runtime, branch, seed, FAULT_SIM_STEPS, &mut model)?
        } else {
            InterleaveFacts::default()
        };
        let (_confirmed, publishes_checked) =
            require_no_confirmed_ordering_violations(&backend, seed, "fault case")?;
        let fired = swept_op_count(&backend, op) >= usize::try_from(call).unwrap_or(usize::MAX);
        (fired, interleave, publishes_checked)
    };

    let (violation, recovered_rows) = {
        let backend = StorageBackend::local_fs(root.to_path_buf());
        let runtime = StorageRuntime::open_with_backend(
            deterministic_options(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .map_err(|err| TestkitError::new(format!("[seed={seed}] fault verify reopen: {err:?}")))?
        .into_runtime();
        let recovered = scan_recovered(
            &runtime,
            branch,
            &oracle_space(),
            &oracle_prefix_key(),
            SCAN_LIMIT,
        )?;
        let rows = recovered.len();
        (
            classify_recovered(&model, branch, &recovered, CrashFamily::ZeroLoss).err(),
            rows,
        )
    };

    Ok(FaultCaseRun {
        fired,
        violation,
        facts: FaultCaseFacts {
            interleave,
            ordering_publishes_checked,
            recovered_rows,
        },
    })
}

/// Result of one power-loss crash case: verdict plus deterministic facts.
#[derive(Debug)]
struct CrashCaseRun {
    perturbed: bool,
    fail_loud: bool,
    violation: Option<RecoveryOracleViolation>,
    /// Read by the derived `Debug`, which every assertion in this file prints
    /// on failure (`"scheduled fault did not fire: {run:?}"`). Dead-code
    /// analysis ignores derived impls, and says so in its own note.
    #[allow(dead_code)]
    facts: CrashCaseFacts,
}

#[derive(Clone, Debug, PartialEq)]
struct CrashCaseFacts {
    interleave: InterleaveFacts,
    ordering_publishes_checked: usize,
    recovered_rows: Option<usize>,
}

/// Materialize a seed-chosen power-loss crash at a seed-chosen point of a seeded
/// interleaving (STH-3 substrate), then reopen (lossy) and verify the durability
/// contract: `Always` loses nothing under any model; `Standard` recovers a clean prefix.
fn run_one_crash_case(root: &Path, seed: u64) -> Result<CrashCaseRun, TestkitError> {
    run_one_crash_case_with_damage(root, seed, None)
}

/// #2827: `extra_damage` plants beyond-crash artifact damage AFTER the
/// power-loss materialization (the artifact-fault shape) — the regression
/// contracts that once relied on `SplitRename` fabricating illegal drops now
/// plant their states explicitly. Returns whether it changed the disk.
type ExtraDamage<'a> = &'a dyn Fn(&Path) -> Result<bool, TestkitError>;

fn run_one_crash_case_with_damage(
    root: &Path,
    seed: u64,
    extra_damage: Option<ExtraDamage<'_>>,
) -> Result<CrashCaseRun, TestkitError> {
    let branch = default_branch();
    let durability = if seed & 1 == 0 {
        StorageDurabilityPolicy::Always
    } else {
        StorageDurabilityPolicy::Standard
    };
    let oracle_durability = if matches!(durability, StorageDurabilityPolicy::Always) {
        OracleDurability::Always
    } else {
        OracleDurability::Standard
    };
    let mut model = ExpectedState::new(oracle_durability);
    let fs_model = FS_MODELS[usize::try_from(seed % 4).unwrap_or(0)];
    let crash_index = 1 + usize::try_from((seed >> 2) % (FAULT_SIM_STEPS as u64)).unwrap_or(0);

    let (perturbed, interleave, ordering_publishes_checked) = {
        let backend = StorageBackend::write_ordering_reordering_local_fs(root.to_path_buf());
        let interleave = {
            let mut runtime =
                StorageRuntime::open_with_backend(deterministic_options(durability), &backend)
                    .map_err(|err| {
                        TestkitError::new(format!("[seed={seed}] reordering open: {err:?}"))
                    })?
                    .into_runtime();
            require_manual_clock(&runtime, seed)?;
            let interleave =
                drive_interleaved(&mut runtime, branch, seed, crash_index, &mut model)?;
            if matches!(fs_model, FsModel::SplitRename) {
                let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Branch(branch),
                ));
                let _ = runtime.drain_maintenance();
            }
            interleave
        };
        // Ordering is judged BEFORE the crash is materialized: the crash mutates
        // files around the Backend trait, so the watchdog's stream ends at the
        // power cut — exactly the window it must have been clean in.
        let (_confirmed, publishes_checked) =
            require_no_confirmed_ordering_violations(&backend, seed, "crash case")?;
        let mut perturbed = backend.reordering_crash(fs_model, seed)?;
        if let Some(damage) = extra_damage {
            perturbed |= damage(root)?;
        }
        (perturbed, interleave, publishes_checked)
    };

    let family = if matches!(durability, StorageDurabilityPolicy::Always) {
        CrashFamily::ZeroLoss
    } else {
        CrashFamily::OnDiskDamage
    };
    let tolerate_fail_loud = matches!(fs_model, FsModel::GarbageUnsyncedTail);

    let (fail_loud, violation, recovered_rows) = {
        let backend = StorageBackend::local_fs(root.to_path_buf());
        let opened = StorageRuntime::open_with_backend(
            deterministic_options(StorageDurabilityPolicy::Standard).with_strict_recovery(false),
            &backend,
        );
        match opened {
            Ok(outcome) => {
                let runtime = outcome.into_runtime();
                let recovered = scan_recovered(
                    &runtime,
                    branch,
                    &oracle_space(),
                    &oracle_prefix_key(),
                    SCAN_LIMIT,
                )?;
                let rows = recovered.len();
                (
                    false,
                    classify_recovered(&model, branch, &recovered, family).err(),
                    Some(rows),
                )
            }
            Err(error) if tolerate_fail_loud => {
                let _ = error;
                (true, None, None)
            }
            Err(error) => {
                return Err(TestkitError::new(format!(
                    "{fs_model:?} reopen failed (expected lossy recovery) \
                     [seed={seed} durability={durability:?} crash_index={crash_index}]: {error:?}"
                )));
            }
        }
    };

    Ok(CrashCaseRun {
        perturbed,
        fail_loud,
        violation,
        facts: CrashCaseFacts {
            interleave,
            ordering_publishes_checked,
            recovered_rows,
        },
    })
}

fn fault_violation(seed: u64, violation: &RecoveryOracleViolation) -> TestkitError {
    TestkitError::new(format!(
        "fault-simulation backend-fault violation [seed={seed}]: {violation:?}"
    ))
}

fn crash_violation(seed: u64, violation: &RecoveryOracleViolation) -> TestkitError {
    TestkitError::new(format!(
        "fault-simulation power-loss violation [seed={seed}]: {violation:?}"
    ))
}

/// Sweep seeds, running one backend-op fault case and one power-loss crash case per
/// seed over a seeded interleaving, each verified against the recovery oracle.
/// `case_limit` caps the number of cases (for CI budgets); `None` runs the default set.
pub fn run_fault_simulation_harness(
    root: &Path,
    case_limit: Option<usize>,
) -> Result<SimulationFaultOutcome, TestkitError> {
    super::assert_deterministic_environment()?;
    let mut outcome = SimulationFaultOutcome::default();
    let mut cases = 0usize;
    let seed_budget = if case_limit.is_some() {
        u64::MAX
    } else {
        DEFAULT_FAULT_SIM_SEEDS
    };
    for seed in 0..seed_budget {
        if case_limit.is_some_and(|limit| cases >= limit) {
            break;
        }
        outcome.seeds_executed += 1;

        let fault = run_one_fault_case(&case_dir(root, &format!("fault-{seed}"))?, seed).map_err(
            |error| TestkitError::new(format!("fault-simulation failed [seed={seed}]: {error}")),
        )?;
        if let Some(violation) = fault.violation {
            return Err(fault_violation(seed, &violation));
        }
        if fault.fired {
            outcome.fault_fired_cases += 1;
        }
        outcome.fault_cases += 1;
        cases += 1;

        if case_limit.is_some_and(|limit| cases >= limit) {
            break;
        }
        let crash = run_one_crash_case(&case_dir(root, &format!("crash-{seed}"))?, seed).map_err(
            |error| TestkitError::new(format!("crash-simulation failed [seed={seed}]: {error}")),
        )?;
        if let Some(violation) = crash.violation {
            return Err(crash_violation(seed, &violation));
        }
        if crash.perturbed {
            outcome.crash_perturbed_cases += 1;
        }
        if crash.fail_loud {
            outcome.fail_loud_cases += 1;
        }
        outcome.crash_cases += 1;
        cases += 1;
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::{run_fault_simulation_harness, run_one_crash_case, run_one_fault_case};
    use crate::testkit::TestkitError;

    /// Regression for a silent durability bug the STH-4 DST surfaced (see
    /// `docs/architecture/archive/implementation-plans/storage-testing/sth-4-finding-checkpoint-flush-publish-fault.md`):
    /// a `PublishObject` `NoSpace` fault during a batched `[Checkpoint, Flush]` drain left
    /// the flush's L0 table installed in-memory with its manifest unpublished, while the
    /// checkpoint still advanced the WAL-replay floor past those rows — so a clean strict
    /// reopen recovered **nothing**, yet every commit and every `drain_maintenance` returned
    /// `Ok`. The fix defers a checkpoint while a table-manifest publish is outstanding, so the
    /// floor never moves past rows no durable manifest covers and the WAL still replays them.
    /// Minimal deterministic repro: 4 puts, checkpoint, 4 puts, then a batched
    /// `[Checkpoint, Flush]` with the fault swept across publish positions; every position
    /// that opens must recover all acknowledged commits.
    #[allow(
        clippy::too_many_lines,
        reason = "multi-phase durability scenario: fault sweep, reopen, verify, rewrite"
    )]
    #[test]
    fn regression_publish_fault_during_checkpoint_flush_loses_no_data() {
        use crate::api::{
            CommitBatch, CommitMutation, CommitOptions, MaintenanceRequest, MaintenanceScope,
            MaintenanceTask, StorageBackend, StorageDurabilityPolicy,
            StorageMaintenanceSchedulingPolicy, StorageOpenOptions, StorageRuntime, StorageValue,
        };
        use crate::testkit::recovery_oracle::verify::scan_recovered;
        use crate::testkit::recovery_oracle::workload::{
            default_branch, oracle_key, oracle_prefix_key, oracle_space, SCAN_LIMIT,
        };
        use crate::testkit::{BackendOperation, FaultKind, FaultMode, FaultRule, FaultScript};
        use std::num::NonZeroU64;

        let branch = default_branch();
        let put = |rt: &mut StorageRuntime<'_>, index: u8| {
            let batch = CommitBatch::new(
                branch,
                vec![CommitMutation::Put {
                    storage_space: oracle_space(),
                    key: oracle_key(index),
                    value: StorageValue::new(vec![index]),
                    ttl: None,
                }],
                CommitOptions::default(),
            )
            .expect("batch");
            rt.commit(&batch).is_ok()
        };

        // Sweep the faulted publish position; every position that opens must recover
        // all acknowledged commits (no silent loss).
        for fault_publish in 3u64..=11 {
            let dir = tempfile::tempdir().expect("tmp");
            let backend = StorageBackend::faulting_local_fs(
                dir.path().to_path_buf(),
                FaultScript::new([FaultRule::with_mode(
                    BackendOperation::PublishObject,
                    NonZeroU64::new(fault_publish).expect("non-zero"),
                    FaultKind::NoSpace,
                    FaultMode::Once,
                )]),
            );
            let mut acked = 0u8;
            {
                let Ok(opened) = StorageRuntime::open_with_backend(
                    StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                        .with_maintenance_scheduling_policy(
                            StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                        ),
                    &backend,
                ) else {
                    continue; // fault landed during open; nothing acknowledged to lose
                };
                let mut rt = opened.into_runtime();
                for index in 0u8..4 {
                    acked += u8::from(put(&mut rt, index));
                }
                let _ = rt.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Branch(branch),
                ));
                let _ = rt.drain_maintenance();
                for index in 4u8..8 {
                    acked += u8::from(put(&mut rt, index));
                }
                // Batched drain of checkpoint + flush — the destructive combination.
                let _ = rt.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Branch(branch),
                ));
                let _ = rt.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Flush,
                    MaintenanceScope::Branch(branch),
                ));
                let _ = rt.drain_maintenance();
            }
            let reopen = StorageBackend::local_fs(dir.path().to_path_buf());
            let mut runtime = StorageRuntime::open_with_backend(
                StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                    .with_maintenance_scheduling_policy(
                        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                    ),
                &reopen,
            )
            .expect("clean reopen")
            .into_runtime();
            let recovered = scan_recovered(
                &runtime,
                branch,
                &oracle_space(),
                &oracle_prefix_key(),
                SCAN_LIMIT,
            )
            .expect("scan")
            .len();
            assert_eq!(
                recovered,
                usize::from(acked),
                "publish fault #{fault_publish} silently lost committed data \
                 (recovered {recovered} of {acked} acknowledged commits)"
            );
            // Regression guard: the recovered visible frontier must cover rows restored from
            // flushed tables, not just the checkpoint/WAL — otherwise the branch has applied
            // rows above `visible` and rejects every mutating commit (fails closed).
            assert!(
                put(&mut runtime, 200),
                "publish fault #{fault_publish} left the branch unwritable after recovery \
                 (visible frontier below restored table rows)"
            );
        }
    }

    /// Non-regression companion to the checkpoint-defer fix: a *single transient*
    /// table-manifest publish failure must defer the checkpoint (so no rows are stranded),
    /// and once a later flush republishes the manifest the debt clears and the checkpoint
    /// resumes — the defer is self-healing, not a permanent halt — with no acknowledged
    /// commit ever lost. Guards against the fix over-correcting into a stuck checkpoint.
    #[allow(
        clippy::too_many_lines,
        reason = "multi-phase durability scenario: fault, defer, heal, reopen-and-verify"
    )]
    #[test]
    fn transient_manifest_publish_failure_defers_then_resumes_checkpoint() {
        use crate::api::{
            CommitBatch, CommitMutation, CommitOptions, MaintenanceRequest, MaintenanceScope,
            MaintenanceSummaryStatus, MaintenanceTask, StorageBackend, StorageDurabilityPolicy,
            StorageMaintenanceSchedulingPolicy, StorageOpenOptions, StorageRuntime, StorageValue,
        };
        use crate::testkit::recovery_oracle::verify::scan_recovered;
        use crate::testkit::recovery_oracle::workload::{
            default_branch, oracle_key, oracle_prefix_key, oracle_space, SCAN_LIMIT,
        };
        use crate::testkit::{BackendOperation, FaultKind, FaultMode, FaultRule, FaultScript};
        use std::num::NonZeroU64;

        let branch = default_branch();
        let put = |rt: &mut StorageRuntime<'_>, index: u8| {
            let batch = CommitBatch::new(
                branch,
                vec![CommitMutation::Put {
                    storage_space: oracle_space(),
                    key: oracle_key(index),
                    value: StorageValue::new(vec![index]),
                    ttl: None,
                }],
                CommitOptions::default(),
            )
            .expect("batch");
            rt.commit(&batch).is_ok()
        };
        let enqueue = |rt: &mut StorageRuntime<'_>, task| {
            let _ = rt.enqueue_maintenance(&MaintenanceRequest::new(
                task,
                MaintenanceScope::Branch(branch),
            ));
        };
        let checkpoint_with_status = |summary: &crate::api::MaintenanceDrainSummary, status| {
            summary.outcomes().iter().any(|outcome| {
                outcome.task() == MaintenanceTask::Checkpoint && outcome.status() == status
            })
        };

        let dir = tempfile::tempdir().expect("tmp");
        // Publish #8 is the batched drain's flush manifest publish (the same destructive
        // position the regression pins, shifted by the one #2690 segment-loss watermark
        // publish the initial segment creation now performs); `Once` makes it transient so
        // a later flush heals it.
        // The 7th publish is the flush's manifest publish (creation manifest,
        // segment 1, checkpoint snapshot, checkpoint manifest, the #2690
        // commit watermark at the post-checkpoint roll, segment 2, then the
        // flush's manifest update). Verified by sweep: only this index makes
        // the flush fault into recoverable debt so the checkpoint defers.
        let backend = StorageBackend::faulting_local_fs(
            dir.path().to_path_buf(),
            FaultScript::new([FaultRule::with_mode(
                BackendOperation::PublishObject,
                NonZeroU64::new(7).expect("non-zero"),
                FaultKind::NoSpace,
                FaultMode::Once,
            )]),
        );
        let mut acked = 0u8;
        {
            let mut rt = StorageRuntime::open_with_backend(
                StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                    .with_maintenance_scheduling_policy(
                        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                    ),
                &backend,
            )
            .expect("open faulting")
            .into_runtime();
            for index in 0u8..4 {
                acked += u8::from(put(&mut rt, index));
            }
            enqueue(&mut rt, MaintenanceTask::Checkpoint);
            let _ = rt.drain_maintenance();
            for index in 4u8..8 {
                acked += u8::from(put(&mut rt, index));
            }
            // Batched drain: the flush manifest publish faults (debt), so the checkpoint defers.
            enqueue(&mut rt, MaintenanceTask::Checkpoint);
            enqueue(&mut rt, MaintenanceTask::Flush);
            let deferred = rt.drain_maintenance().expect("drain");
            assert!(
                checkpoint_with_status(&deferred, MaintenanceSummaryStatus::Deferred),
                "checkpoint did not defer while a table-manifest publish was outstanding: {deferred:?}"
            );
            // A later flush republishes the manifest (the `Once` fault is spent) → debt clears.
            for index in 8u8..12 {
                acked += u8::from(put(&mut rt, index));
            }
            enqueue(&mut rt, MaintenanceTask::Flush);
            let _ = rt.drain_maintenance();
            // The checkpoint now resumes rather than staying stuck.
            enqueue(&mut rt, MaintenanceTask::Checkpoint);
            let resumed = rt.drain_maintenance().expect("drain");
            assert!(
                checkpoint_with_status(&resumed, MaintenanceSummaryStatus::Completed),
                "checkpoint did not resume after the manifest debt cleared: {resumed:?}"
            );
        }
        let reopen = StorageBackend::local_fs(dir.path().to_path_buf());
        let runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &reopen,
        )
        .expect("clean reopen")
        .into_runtime();
        let recovered = scan_recovered(
            &runtime,
            branch,
            &oracle_space(),
            &oracle_prefix_key(),
            SCAN_LIMIT,
        )
        .expect("scan")
        .len();
        assert_eq!(
            recovered,
            usize::from(acked),
            "transient manifest publish failure lost acknowledged data \
             (recovered {recovered} of {acked})"
        );
    }

    #[test]
    fn backend_fault_during_interleaving_loses_nothing() {
        let dir = tempfile::tempdir().expect("tmp");
        // Seed 8 schedules an AppendObject fault (every commit appends), so it fires
        // reliably after the first commit succeeds — exercising loss-free recovery of
        // the acknowledged prefix around an in-doubt commit.
        let run = run_one_fault_case(dir.path(), 8).expect("fault case");
        assert!(run.fired, "scheduled fault did not fire: {run:?}");
        assert!(
            run.violation.is_none(),
            "backend fault lost acknowledged data: {run:?}"
        );
    }

    /// Regression for a power-loss recovery gap the STH-4 DST surfaced (see
    /// `docs/architecture/archive/implementation-plans/storage-testing/sth-4-finding-splitrename-power-loss-gap.md`):
    /// under Standard durability, a `SplitRename` crash that drops the table-manifest
    /// base of a *delta* checkpoint leaves recovery installing the orphaned delta (only
    /// the post-flush commit) — a non-prefix `Gap` — instead of falling back to a clean
    /// prefix as the `split_rename_falls_back_to_the_log_without_loss` contract requires.
    /// Deterministic repro: crash seed 155 (Standard / `SplitRename` / `crash_index` 15). The fix
    /// records the snapshot's delta base floor durably (`flushed_through_commit_id`) so recovery
    /// can require the table-manifest base and, when it is lost, discards the orphaned delta and
    /// recovers a clean prefix (empty here) recorded as `DataLoss` rather than a gap.
    #[test]
    fn regression_split_rename_power_loss_recovers_clean_prefix() {
        // #2827: `SplitRename` no longer fabricates the missing-manifest state
        // (a completed publish cannot vanish under the production publish
        // discipline), so the contract this seed found — a delta checkpoint
        // whose table-manifest base is missing must recover a clean prefix
        // recorded as `DataLoss`, never a `Gap` — now plants the damage
        // directly (the artifact-fault shape) on the same seed-155 trajectory.
        let dir = tempfile::tempdir().expect("tmp");
        let damage: super::ExtraDamage<'_> = &|root| {
            let manifest = crate::layout::ObjectLayout::branch_table_manifest(
                &crate::testkit::recovery_oracle::workload::default_branch().to_string(),
            )
            .map_err(|err| TestkitError::new(format!("manifest layout: {err:?}")))?;
            let path = root.join(format!("{}.object@", manifest.as_str()));
            if path.exists() {
                std::fs::remove_file(&path)
                    .map_err(|err| TestkitError::new(format!("remove manifest: {err}")))?;
                Ok(true)
            } else {
                Ok(false)
            }
        };
        let run =
            super::run_one_crash_case_with_damage(dir.path(), 155, Some(damage)).expect("case");
        // The perturbation can ONLY come from the planted damage: seed 155's
        // crash model is `SplitRename`, which is damage-free after #2827 —
        // so this assert also kills a deleted damage-application arm.
        assert!(
            run.perturbed,
            "the planted manifest-base damage must change the disk: {run:?}"
        );
        assert!(
            run.violation.is_none(),
            "a missing table-manifest base under a delta checkpoint must recover a \
             clean prefix, not a gap: {run:?}"
        );
    }

    #[test]
    fn power_loss_during_interleaving_is_oracle_valid() {
        let dir = tempfile::tempdir().expect("tmp");
        // Seed 5 (Standard / ReorderedAppends) perturbs under the #2827
        // model correction (seed 3 was SplitRename, now damage-free) — the
        // perturbation assert keeps this pin non-vacuous.
        let run = run_one_crash_case(dir.path(), 5).expect("crash case");
        assert!(run.perturbed, "crash did not perturb the disk: {run:?}");
        assert!(
            run.violation.is_none(),
            "power-loss recovery was not oracle-valid: {run:?}"
        );
    }

    /// TCP4.11a determinism guard: a fault case replays bit-exact from its
    /// seed — same step trace, same acked versions, same ordering facts, same
    /// recovered state.
    #[test]
    fn fault_case_replays_bit_exact() {
        let dir_a = tempfile::tempdir().expect("tmp");
        let dir_b = tempfile::tempdir().expect("tmp");
        let first = run_one_fault_case(dir_a.path(), 8).expect("first run");
        let second = run_one_fault_case(dir_b.path(), 8).expect("second run");
        assert!(first.fired && second.fired, "fault must fire in both runs");
        assert_eq!(
            first.facts, second.facts,
            "same seed produced divergent fault-case trajectories"
        );
    }

    /// TCP4.11a determinism guard: a power-loss crash case replays bit-exact
    /// from its seed, including the post-crash recovered state.
    #[test]
    fn crash_case_replays_bit_exact() {
        let dir_a = tempfile::tempdir().expect("tmp");
        let dir_b = tempfile::tempdir().expect("tmp");
        let first = run_one_crash_case(dir_a.path(), 5).expect("first run");
        let second = run_one_crash_case(dir_b.path(), 5).expect("second run");
        assert!(
            first.perturbed && second.perturbed,
            "crash must perturb in both runs"
        );
        assert_eq!(
            first.facts, second.facts,
            "same seed produced divergent crash-case trajectories"
        );
    }

    #[test]
    fn fault_simulation_sweep_holds_and_is_non_vacuous() {
        let dir = tempfile::tempdir().expect("tmp");
        let outcome = run_fault_simulation_harness(dir.path(), Some(16)).expect("fault sim sweep");
        assert!(
            outcome.fault_cases() > 0 && outcome.crash_cases() > 0,
            "no cases run"
        );
        assert!(
            outcome.fault_fired_cases() > 0,
            "no scheduled backend fault fired: {outcome:?}"
        );
        assert!(
            outcome.crash_perturbed_cases() > 0,
            "no crash perturbed the disk: {outcome:?}"
        );
    }
}
