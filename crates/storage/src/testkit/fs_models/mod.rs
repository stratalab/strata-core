//! Filesystem-persistence-model enumeration.
//!
//! STH-1 tore only the active WAL tail; this generalizes to every durable object
//! by routing the [`ReorderingBackend`](crate::testkit::ReorderingBackend) — which
//! knows each object's exact unsynced boundary — under a durable runtime, then
//! materializing each [`FsModel`] crash state and checking recovery against the
//! recovery oracle. The bar is the real durability contract: under `Always` no
//! acknowledged commit is lost under *any* model; under `Standard` recovery is
//! still a clean prefix (only the unsynced suffix may be lost), never torn,
//! phantom, or holed.

use std::path::{Path, PathBuf};

use strata_core::BranchId;

use super::recovery_oracle::model::{ExpectedState, OracleDurability};
use super::recovery_oracle::verify::{
    classify_recovered, scan_recovered, CrashFamily, RecoveryOracleViolation,
};
use super::recovery_oracle::workload::{
    default_branch, generate_workload, oracle_prefix_key, oracle_space, to_commit_mutation,
    SCAN_LIMIT,
};
use super::reordering_backend::FsModel;
use crate::api::{
    CommitBatch, CommitOptions, MaintenanceRequest, MaintenanceScope, MaintenanceTask,
    StorageBackend, StorageDurabilityPolicy, StorageMaintenanceSchedulingPolicy,
    StorageOpenOptions, StorageRuntime,
};
use crate::testkit::TestkitError;

/// Default seed count with no case budget. A case budget lets seeds scale freely,
/// so a large soak explores many seeds, not just these.
const DEFAULT_FS_SEEDS: u64 = 4;
/// Commits per workload — small so the bounded sweep stays cheap while still
/// producing an unsynced WAL tail to lose.
const FS_OP_COUNT: usize = 5;
/// Every filesystem persistence model, swept in order.
const FS_MODELS: [FsModel; 4] = [
    FsModel::OrderedAtomic,
    FsModel::ReorderedAppends,
    FsModel::GarbageUnsyncedTail,
    FsModel::SplitRename,
];
/// Both durability policies — `Always` (fsync per commit) must lose nothing under
/// any model; `Standard` (no per-commit fsync) may lose the unsynced suffix.
const FS_DURABILITY: [StorageDurabilityPolicy; 2] = [
    StorageDurabilityPolicy::Standard,
    StorageDurabilityPolicy::Always,
];

/// Counters describing a filesystem-model sweep run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FsModelSweepOutcome {
    seeds_executed: usize,
    cases: usize,
    perturbed_cases: usize,
    fail_loud_cases: usize,
}

impl FsModelSweepOutcome {
    #[must_use]
    pub const fn seeds_executed(&self) -> usize {
        self.seeds_executed
    }
    #[must_use]
    pub const fn cases(&self) -> usize {
        self.cases
    }
    /// Cases where the crash actually perturbed the on-disk state (non-vacuousness).
    #[must_use]
    pub const fn perturbed_cases(&self) -> usize {
        self.perturbed_cases
    }
    /// Cases where the reopen rejected a corrupt tail (fail-loud, not silently wrong).
    #[must_use]
    pub const fn fail_loud_cases(&self) -> usize {
        self.fail_loud_cases
    }
}

fn oracle_durability(durability: StorageDurabilityPolicy) -> OracleDurability {
    match durability {
        StorageDurabilityPolicy::Always => OracleDurability::Always,
        _ => OracleDurability::Standard,
    }
}

/// Deterministic-inline maintenance: the inline executor owns the queue and spawns
/// no worker threads, so the #2662 detached-worker window is closed at its main
/// source. This does NOT make the same-dir verify reopen race-free — two CI hits
/// (#2796) showed an in-process holder can still keep the writer lock briefly
/// across the staging drop — which is why `open_verify_runtime` additionally
/// carries the bounded reopen retry. Same pattern as `testkit::simulation`.
/// (This helper's old name/comment claimed a "borrowed" open path; post-BS4.4i
/// durable runtimes are uniformly owned, reordering backends included.)
fn inline_options(durability: StorageDurabilityPolicy) -> StorageOpenOptions {
    StorageOpenOptions::durable_local(durability)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::DeterministicInline)
}

/// The lossy verify reopen after a crash-state materialization: a
/// same-process reopen of the case directory, routed through the sanctioned
/// bounded retry (#2727 policy, as `testkit::simulation` does) — a briefly
/// held writer lock is a transient, not a verdict on the store (#2796: two
/// CI hits proved an in-process holder can outlive the staging drop even
/// under `DeterministicInline` — a child forked concurrently by another test
/// sharing the lock's open file description, which the writer guard's
/// explicit unlock now closes, #3609). Exhaustion returns the original error,
/// so a genuine lock leak still fails loud.
fn open_verify_runtime(
    backend: &StorageBackend,
) -> crate::api::StorageApiResult<crate::api::StorageOpenOutcome<'_>> {
    crate::testkit::reopen_retry::open_with_retry_on_unavailable(|| {
        StorageRuntime::open_with_backend(
            inline_options(StorageDurabilityPolicy::Standard).with_strict_recovery(false),
            backend,
        )
    })
}

fn case_dir(root: &Path, label: &str) -> Result<PathBuf, TestkitError> {
    let dir = root.join(label);
    std::fs::create_dir_all(&dir)
        .map_err(|err| TestkitError::new(format!("create case dir: {err}")))?;
    Ok(dir)
}

/// Drive `crash_index` commits, recording each acknowledgement. For the split-rename
/// model, force a checkpoint afterward so there is a published object to drop — and
/// the checkpoint syncs the WAL through these commits, so a vanished publish must
/// fall back to an intact log without losing a commit.
fn drive(
    runtime: &mut StorageRuntime<'_>,
    branch: BranchId,
    seed: u64,
    crash_index: usize,
    model: FsModel,
    state: &mut ExpectedState,
) -> Result<(), TestkitError> {
    for mutations in generate_workload(seed, FS_OP_COUNT)
        .iter()
        .take(crash_index)
    {
        let batch = CommitBatch::new(
            branch,
            mutations.iter().map(to_commit_mutation).collect(),
            CommitOptions::default(),
        )
        .map_err(|err| TestkitError::new(format!("build batch: {err:?}")))?;
        let Ok(summary) = runtime.commit(&batch) else {
            // A clean reordering backend never faults a commit; if one fails anyway,
            // record it in-doubt and stop (later commits would diverge from the model).
            state.record_in_doubt(branch, mutations.clone());
            return Ok(());
        };
        state.record_ack(branch, summary.commit_version(), mutations.clone());
    }
    if matches!(model, FsModel::SplitRename) {
        let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Branch(branch),
        ));
        let _ = runtime.drain_maintenance();
    }
    Ok(())
}

/// Result of materializing one filesystem-model crash state.
#[derive(Debug)]
struct FsModelRun {
    perturbed: bool,
    fail_loud: bool,
    violation: Option<RecoveryOracleViolation>,
}

/// Open durable on a reordering backend, drive `crash_index` commits, materialize the
/// `model` crash state (seeded), then reopen on a plain backend (lossy) and verify the
/// recovered state against the oracle with the family the durability + model demand.
fn run_one_fs_model(
    root: &Path,
    seed: u64,
    crash_index: usize,
    model: FsModel,
    durability: StorageDurabilityPolicy,
) -> Result<FsModelRun, TestkitError> {
    let branch = default_branch();
    let mut state = ExpectedState::new(oracle_durability(durability));

    // Drive the workload on the reordering backend, drop the runtime, then crash.
    // Scoped so the backend is freed before the verify reopen (no leak under soak).
    let perturbed = {
        let backend = StorageBackend::reordering_local_fs(root.to_path_buf());
        {
            let mut runtime =
                StorageRuntime::open_with_backend(inline_options(durability), &backend)
                    .map_err(|err| TestkitError::new(format!("reordering open: {err:?}")))?
                    .into_runtime();
            drive(&mut runtime, branch, seed, crash_index, model, &mut state)?;
        }
        backend.reordering_crash(model, seed)?
    };

    // Under `Always` every commit was fsync'd, so no model may lose one (ZeroLoss).
    // Under `Standard` the unsynced suffix may be gone, so a clean prefix is allowed.
    let family = if matches!(durability, StorageDurabilityPolicy::Always) {
        CrashFamily::ZeroLoss
    } else {
        CrashFamily::OnDiskDamage
    };
    // A garbage (torn) tail may be CRC-rejected even by a lossy reopen; that fail-loud
    // is a safe outcome. Truncation / dropped-publish models must reopen and recover.
    let tolerate_fail_loud = matches!(model, FsModel::GarbageUnsyncedTail);

    let (fail_loud, violation) = {
        let backend = StorageBackend::local_fs(root.to_path_buf());
        // Bind the open result so it drops before `backend` (reverse declaration
        // order), rather than as a tail-expression temporary that outlives it.
        let opened = open_verify_runtime(&backend);
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
                (
                    false,
                    classify_recovered(&state, branch, &recovered, family).err(),
                )
            }
            Err(error) if tolerate_fail_loud => {
                // Corruption was detected and rejected — recovery did not proceed onto
                // a silently wrong state, which is the safety property we require.
                let _ = error;
                (true, None)
            }
            Err(error) => {
                // The op trace makes the failure self-describing without
                // re-deriving the workload from the seed (#2662 diagnostics).
                return Err(TestkitError::new(format!(
                    "{model:?} reopen failed (expected lossy recovery) \
                     [seed={seed} durability={durability:?} crash_index={crash_index}]: {error:?} \
                     workload={:?}",
                    generate_workload(seed, FS_OP_COUNT)
                )));
            }
        }
    };

    Ok(FsModelRun {
        perturbed,
        fail_loud,
        violation,
    })
}

fn fs_violation(
    seed: u64,
    model: FsModel,
    durability: StorageDurabilityPolicy,
    crash_index: usize,
    violation: &RecoveryOracleViolation,
) -> TestkitError {
    // The op trace makes the failure self-describing without re-deriving the
    // workload from the seed (#2662 diagnostics).
    TestkitError::new(format!(
        "fs-model violation [seed={seed} model={model:?} durability={durability:?} \
         crash_index={crash_index}]: {violation:?} workload={:?}",
        generate_workload(seed, FS_OP_COUNT)
    ))
}

/// Sweep seeds × every [`FsModel`] × crash point × both durability policies,
/// verifying recovery against the oracle at each. `case_limit` caps the number of
/// cases (for CI budgets); `None` runs the default seed grid.
pub fn run_fs_model_harness(
    root: &Path,
    case_limit: Option<usize>,
) -> Result<FsModelSweepOutcome, TestkitError> {
    let mut outcome = FsModelSweepOutcome::default();
    let mut cases = 0usize;
    // Seeds scale with the case budget: an uncapped run covers a few seeds, a large
    // soak explores as many as the budget allows. Seeds stay `0..N` so any failure
    // replays from its printed seed.
    let seed_budget = if case_limit.is_some() {
        u64::MAX
    } else {
        DEFAULT_FS_SEEDS
    };
    'outer: for seed in 0..seed_budget {
        if case_limit.is_some_and(|limit| cases >= limit) {
            break;
        }
        outcome.seeds_executed += 1;
        for durability in FS_DURABILITY {
            for model in FS_MODELS {
                for crash_index in 1..=FS_OP_COUNT {
                    if case_limit.is_some_and(|limit| cases >= limit) {
                        break 'outer;
                    }
                    let label = format!("{model:?}-{durability:?}-{seed}-{crash_index}");
                    let run = run_one_fs_model(
                        &case_dir(root, &label)?,
                        seed,
                        crash_index,
                        model,
                        durability,
                    )?;
                    if let Some(violation) = run.violation {
                        return Err(fs_violation(
                            seed,
                            model,
                            durability,
                            crash_index,
                            &violation,
                        ));
                    }
                    if run.perturbed {
                        outcome.perturbed_cases += 1;
                    }
                    if run.fail_loud {
                        outcome.fail_loud_cases += 1;
                    }
                    outcome.cases += 1;
                    cases += 1;
                }
            }
        }
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_reopen_outlasts_a_briefly_held_writer_lock() {
        use fs2::FileExt as _;

        let dir = tempfile::tempdir().expect("tmp");
        // Stage a real store so the writer-lock object and durable state exist.
        {
            let backend = StorageBackend::local_fs(dir.path().to_path_buf());
            let _runtime = StorageRuntime::open_with_backend(
                inline_options(StorageDurabilityPolicy::Standard),
                &backend,
            )
            .expect("staging open")
            .into_runtime();
        }
        // Interloper: hold the writer flock briefly, exactly as an in-process
        // holder from the previous instance would, releasing well inside the
        // reopen retry budget. (The on-disk object suffix is ".object@".)
        let lock_path = dir.path().join("locks/writer.object@");
        std::fs::create_dir_all(lock_path.parent().expect("locks dir")).expect("locks dir");
        let interloper = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("open lock object");
        // The staging drop released the lock explicitly (#3609), so no child
        // forked concurrently by another test can still be holding it here.
        interloper
            .try_lock_exclusive()
            .expect("interloper takes the writer lock");
        let release = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            // Unlock explicitly, as the product writer guard does (#3609): a
            // bare drop leaves the lock held while any child this test binary
            // forks concurrently still shares the descriptor.
            fs2::FileExt::unlock(&interloper).expect("interloper releases the writer lock");
            drop(interloper);
        });

        let backend = StorageBackend::local_fs(dir.path().to_path_buf());
        let opened = open_verify_runtime(&backend);
        release.join().expect("release thread");
        // A transiently held writer lock is not a verdict on the store: the
        // verify reopen must absorb it instead of failing the whole sweep.
        let _runtime = opened
            .expect("verify reopen must outlast a briefly held writer lock")
            .into_runtime();
    }

    #[test]
    fn every_model_holds_the_oracle_across_seeds() {
        let dir = tempfile::tempdir().expect("tmp");
        let outcome = run_fs_model_harness(dir.path(), Some(80)).expect("sweep");
        assert!(outcome.cases() > 0, "no cases swept");
        // Non-vacuous: at least some crash actually perturbed the on-disk state.
        assert!(
            outcome.perturbed_cases() > 0,
            "no crash perturbed the disk: {outcome:?}"
        );
    }

    #[test]
    fn standard_ordered_atomic_drops_the_unsynced_suffix() {
        // Under Standard the unsynced WAL tail is genuinely lost, yet recovery is
        // still a clean prefix (oracle-valid) — not torn, phantom, or holed.
        let dir = tempfile::tempdir().expect("tmp");
        let run = run_one_fs_model(
            dir.path(),
            11,
            FS_OP_COUNT,
            FsModel::OrderedAtomic,
            StorageDurabilityPolicy::Standard,
        )
        .expect("run");
        assert!(run.perturbed, "Standard left no unsynced tail to truncate");
        assert!(
            run.violation.is_none(),
            "ordered-atomic recovery was not a clean prefix: {run:?}"
        );
    }

    #[test]
    fn always_loses_nothing_under_ordered_atomic_power_loss() {
        // The headline durability guarantee: a synced history survives power loss.
        let dir = tempfile::tempdir().expect("tmp");
        let run = run_one_fs_model(
            dir.path(),
            11,
            FS_OP_COUNT,
            FsModel::OrderedAtomic,
            StorageDurabilityPolicy::Always,
        )
        .expect("run");
        assert!(
            run.violation.is_none(),
            "Always lost an acknowledged commit under power loss: {run:?}"
        );
    }

    #[test]
    fn split_rename_falls_back_to_the_log_without_loss() {
        // A vanished publish under Always must fall back to the synced WAL losslessly.
        let dir = tempfile::tempdir().expect("tmp");
        let run = run_one_fs_model(
            dir.path(),
            7,
            FS_OP_COUNT,
            FsModel::SplitRename,
            StorageDurabilityPolicy::Always,
        )
        .expect("run");
        assert!(
            run.violation.is_none(),
            "split-rename lost a commit despite an intact log: {run:?}"
        );
    }

    #[test]
    fn garbage_unsynced_tail_is_fail_loud_or_clean_prefix() {
        // Corruption in the unsynced tail under Standard: recovery either rejects it
        // (fail-loud) or recovers a clean prefix — never a silently wrong state.
        let dir = tempfile::tempdir().expect("tmp");
        let run = run_one_fs_model(
            dir.path(),
            5,
            FS_OP_COUNT,
            FsModel::GarbageUnsyncedTail,
            StorageDurabilityPolicy::Standard,
        )
        .expect("run");
        assert!(
            run.violation.is_none(),
            "garbage tail produced a silently wrong recovery: {run:?}"
        );
    }
}
