//! Deterministic simulation (DST) harness.
//!
//! The seeded explorer over the production lifecycle path: under one seed it sweeps
//! client-op × maintenance-cadence × clock-advancement interleavings against the
//! faithful production scheduler (the inline executor on the real `Background`
//! logic), asserting the recovery oracle (safety) and queue progress (liveness)
//! every step. Every trajectory is a pure function of its seed, so any failure
//! replays bit-exact — the determinism guard lives in [`driver`].
//!
//! Interleaving comes from cadence (how much maintenance drains between client ops),
//! seeded clock advancement, and where each op lands — never from reordering the
//! production maintenance priority, so every schedule explored is one production can
//! actually emit. The fault/crash dimension ([`faults`]) runs on the SAME
//! deterministic substrate (TCP4.11a): owned decorator backends under
//! `DeterministicInline` with the write-ordering watchdog stacked as a
//! continuous oracle, bit-exact from the seed like the clean lane.

mod driver;
mod faults;
mod history;
pub(crate) mod whole_db;

use std::path::{Path, PathBuf};

use crate::testkit::TestkitError;
use driver::run_one_sim;

pub use faults::{run_fault_simulation_harness, SimulationFaultOutcome};
pub use history::{run_lineage_history_harness, LineageHistoryOutcome};

/// Counters describing a whole-DB simulation sweep (TCP4.11b).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WholeDbOutcome {
    seeds_executed: usize,
    epochs_executed: usize,
    crashed_epochs: usize,
    forks: usize,
    deletes: usize,
    temporal_probes_ok: usize,
    bytes_reclaimed: u64,
    clean_closes: usize,
    orphaned_snapshots_planted: usize,
}

impl WholeDbOutcome {
    /// Bytes the reclaim ledgers reported released across the sweep
    /// (non-vacuity for the footprint oracle, space-reclamation contract §3.5).
    #[must_use]
    pub const fn bytes_reclaimed(&self) -> u64 {
        self.bytes_reclaimed
    }
    /// Epochs that ended in a clean close (the close-time reclaim drive ran).
    #[must_use]
    pub const fn clean_closes(&self) -> usize {
        self.clean_closes
    }
    /// Orphaned snapshots planted for the open reconcile to reclaim.
    #[must_use]
    pub const fn orphaned_snapshots_planted(&self) -> usize {
        self.orphaned_snapshots_planted
    }
    #[must_use]
    pub const fn seeds_executed(&self) -> usize {
        self.seeds_executed
    }
    #[must_use]
    pub const fn epochs_executed(&self) -> usize {
        self.epochs_executed
    }
    /// Epochs that ended in a materialized power-loss crash (non-vacuity).
    #[must_use]
    pub const fn crashed_epochs(&self) -> usize {
        self.crashed_epochs
    }
    #[must_use]
    pub const fn forks(&self) -> usize {
        self.forks
    }
    #[must_use]
    pub const fn deletes(&self) -> usize {
        self.deletes
    }
    /// Successful at-version probes (non-vacuity for the temporal oracle).
    #[must_use]
    pub const fn temporal_probes_ok(&self) -> usize {
        self.temporal_probes_ok
    }
}

/// Sweep seeded whole-DB trajectories (multi-branch, multi-epoch, seeded
/// crashes), each continuously oracle-checked. `case_limit` caps seeds for
/// CI budgets; `None` runs the default set. The trajectory shape is the
/// canonical 3×24 unless `STRATA_SIM_EPOCHS` / `STRATA_SIM_STEPS_PER_EPOCH`
/// deepen it (TCP4.11c, the nightly soak).
pub fn run_whole_db_harness(
    root: &Path,
    case_limit: Option<usize>,
) -> Result<WholeDbOutcome, TestkitError> {
    const DEFAULT_WHOLE_DB_SEEDS: u64 = 6;
    assert_deterministic_environment()?;
    let (epochs, steps_per_epoch) = whole_db_shape()?;
    let mut outcome = WholeDbOutcome::default();
    let seed_budget = if case_limit.is_some() {
        u64::MAX
    } else {
        DEFAULT_WHOLE_DB_SEEDS
    };
    for (cases, seed) in (0..seed_budget).enumerate() {
        if case_limit.is_some_and(|limit| cases >= limit) {
            break;
        }
        match whole_db::run_whole_db_sim(
            &case_dir(root, &format!("whole-db-{seed}"))?,
            seed,
            epochs,
            steps_per_epoch,
        ) {
            Ok(facts) => {
                outcome.epochs_executed += facts.epochs();
                outcome.crashed_epochs += facts.crashed_epochs();
                outcome.forks += facts.forks();
                outcome.deletes += facts.deletes();
                outcome.temporal_probes_ok += facts.temporal_probes_ok();
                outcome.bytes_reclaimed = outcome
                    .bytes_reclaimed
                    .saturating_add(facts.bytes_reclaimed());
                outcome.clean_closes += facts.clean_closes();
                outcome.orphaned_snapshots_planted += facts.orphaned_snapshots_planted();
            }
            Err(error) => return Err(whole_db_replay_error(seed, epochs, steps_per_epoch, &error)),
        }
        outcome.seeds_executed += 1;
    }
    Ok(outcome)
}

/// Canonical whole-DB trajectory shape. The seed corpus (`README.md` in this
/// directory) records trajectories at exactly this shape; replay is only
/// bit-exact against the shape the failure ran at.
const WHOLE_DB_EPOCHS: usize = 3;
const WHOLE_DB_STEPS_PER_EPOCH: usize = 24;

/// TCP4.11c: the trajectory shape for this process — canonical unless the
/// soak deepens it. Both the sweep and `replay_whole_db_seed` read the same
/// knobs, so a repro line that carries the shape replays bit-exact.
fn whole_db_shape() -> Result<(usize, usize), TestkitError> {
    let epochs = parse_shape_knob(
        "STRATA_SIM_EPOCHS",
        std::env::var("STRATA_SIM_EPOCHS").ok().as_deref(),
        WHOLE_DB_EPOCHS,
    )?;
    let steps_per_epoch = parse_shape_knob(
        "STRATA_SIM_STEPS_PER_EPOCH",
        std::env::var("STRATA_SIM_STEPS_PER_EPOCH").ok().as_deref(),
        WHOLE_DB_STEPS_PER_EPOCH,
    )?;
    Ok((epochs, steps_per_epoch))
}

/// One shape knob: absent → the canonical default; present → a positive
/// integer, anything else fails loudly (a silently-ignored typo would run
/// the wrong shape and break the replay contract).
fn parse_shape_knob(name: &str, raw: Option<&str>, default: usize) -> Result<usize, TestkitError> {
    match raw {
        None => Ok(default),
        Some(value) => match value.trim().parse::<usize>() {
            Ok(parsed) if parsed > 0 => Ok(parsed),
            _ => Err(TestkitError::new(format!(
                "{name} must be a positive integer trajectory-shape override, got {value:?}"
            ))),
        },
    }
}

/// The replay contract: every sweep failure names its seed and prints the
/// one-line local repro, so a nightly finding becomes a deterministic local
/// run instead of a flake hunt. Replay is bit-exact only at the shape the
/// failure ran at, so a non-canonical shape rides the repro line.
fn whole_db_replay_error(
    seed: u64,
    epochs: usize,
    steps_per_epoch: usize,
    error: &TestkitError,
) -> TestkitError {
    TestkitError::new(format!(
        "whole-db simulation failed [seed={seed} shape={epochs}x{steps_per_epoch}]: {error}\n  \
         replay: {shape}STRATA_SIM_SEED={seed} cargo test -p strata-storage \
         --features fault-injection --test simulation_whole_db -- replay_single_seed \
         --ignored --nocapture",
        shape = whole_db_shape_prefix(epochs, steps_per_epoch),
    ))
}

/// The env prefix a repro line needs to re-run at a non-canonical shape;
/// empty at the canonical shape (the corpus documents that default).
fn whole_db_shape_prefix(epochs: usize, steps_per_epoch: usize) -> String {
    if (epochs, steps_per_epoch) == (WHOLE_DB_EPOCHS, WHOLE_DB_STEPS_PER_EPOCH) {
        String::new()
    } else {
        format!("STRATA_SIM_EPOCHS={epochs} STRATA_SIM_STEPS_PER_EPOCH={steps_per_epoch} ")
    }
}

/// Replay exactly one whole-DB seed — the deterministic local repro for a
/// sweep failure (`STRATA_SIM_SEED=n`, plus the shape knobs when the failure
/// named a non-canonical shape).
pub fn replay_whole_db_seed(root: &Path, seed: u64) -> Result<WholeDbOutcome, TestkitError> {
    assert_deterministic_environment()?;
    let (epochs, steps_per_epoch) = whole_db_shape()?;
    let facts = whole_db::run_whole_db_sim(
        &case_dir(root, &format!("whole-db-{seed}"))?,
        seed,
        epochs,
        steps_per_epoch,
    )
    .map_err(|error| whole_db_replay_error(seed, epochs, steps_per_epoch, &error))?;
    Ok(WholeDbOutcome {
        seeds_executed: 1,
        epochs_executed: facts.epochs(),
        crashed_epochs: facts.crashed_epochs(),
        forks: facts.forks(),
        deletes: facts.deletes(),
        temporal_probes_ok: facts.temporal_probes_ok(),
        bytes_reclaimed: facts.bytes_reclaimed(),
        clean_closes: facts.clean_closes(),
        orphaned_snapshots_planted: facts.orphaned_snapshots_planted(),
    })
}

/// Default seed count with no case budget. A case budget lets seeds scale freely,
/// so a large soak explores many seeds, not just these.
const DEFAULT_SIM_SEEDS: u64 = 8;
/// Actions driven per seeded trajectory.
const STEP_BUDGET: usize = 48;

/// Counters describing a simulation sweep run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SimulationOutcome {
    seeds_executed: usize,
    steps_executed: usize,
    maintenance_completed: usize,
    pressure_retries_drained: usize,
    clock_advances: usize,
}

impl SimulationOutcome {
    #[must_use]
    pub const fn seeds_executed(&self) -> usize {
        self.seeds_executed
    }
    #[must_use]
    pub const fn steps_executed(&self) -> usize {
        self.steps_executed
    }
    /// Maintenance tasks the inline executor completed across the sweep
    /// (non-vacuousness: the interleaving actually ran production maintenance).
    #[must_use]
    pub const fn maintenance_completed(&self) -> usize {
        self.maintenance_completed
    }
    /// Commits that hit retryable back-pressure, drained, and resumed (the admission
    /// liveness path was exercised).
    #[must_use]
    pub const fn pressure_retries_drained(&self) -> usize {
        self.pressure_retries_drained
    }
    /// Manual-clock advances that reached the injected clock.
    #[must_use]
    pub const fn clock_advances(&self) -> usize {
        self.clock_advances
    }
}

/// Environment knobs that change storage behavior and would silently break the
/// seed-replay guarantee if set to non-defaults. The harness refuses to run in
/// a polluted environment instead of diverging quietly.
fn assert_deterministic_environment() -> Result<(), TestkitError> {
    let checks: [(&str, &[&str]); 3] = [
        ("STRATA_SUBCOMPACTIONS", &["1"]),
        ("STRATA_COMPACTION_LANES", &["4"]),
        ("STRATA_ADMISSION", &[]),
    ];
    for (name, allowed) in checks {
        if let Ok(value) = std::env::var(name) {
            if !allowed.contains(&value.as_str()) {
                return Err(TestkitError::new(format!(
                    "simulation requires a deterministic environment: {name}={value} \
                     changes storage behavior (unset it, or set the default)"
                )));
            }
        }
    }
    Ok(())
}

fn case_dir(root: &Path, label: &str) -> Result<PathBuf, TestkitError> {
    let dir = root.join(label);
    std::fs::create_dir_all(&dir)
        .map_err(|err| TestkitError::new(format!("create case dir: {err}")))?;
    Ok(dir)
}

/// Sweep seeded simulation trajectories, each safety- and liveness-checked every
/// step against the recovery oracle. `case_limit` caps the number of trajectories
/// (for CI budgets); `None` runs the default seed set.
pub fn run_simulation_harness(
    root: &Path,
    case_limit: Option<usize>,
) -> Result<SimulationOutcome, TestkitError> {
    assert_deterministic_environment()?;
    let mut outcome = SimulationOutcome::default();
    // Seeds scale with the case budget: an uncapped run covers a few seeds, a large
    // soak explores as many as the budget allows. Seeds stay `0..N` so any failure
    // replays from its printed seed.
    let seed_budget = if case_limit.is_some() {
        u64::MAX
    } else {
        DEFAULT_SIM_SEEDS
    };
    for (cases, seed) in (0..seed_budget).enumerate() {
        if case_limit.is_some_and(|limit| cases >= limit) {
            break;
        }
        let facts = run_one_sim(&case_dir(root, &format!("sim-{seed}"))?, seed, STEP_BUDGET)
            .map_err(|error| {
                TestkitError::new(format!("simulation failed [seed={seed}]: {error}"))
            })?;
        outcome.seeds_executed += 1;
        outcome.steps_executed += facts.steps();
        outcome.maintenance_completed += facts.maintenance_completed();
        outcome.pressure_retries_drained += facts.pressure_retries();
        outcome.clock_advances += facts.clock_advances();
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::run_simulation_harness;

    /// Truth table for the reopen version-domain bound: only a lossy crash
    /// family adopts the runtime's recovered visible version as the model's
    /// ack ceiling; zero-loss reopens keep the full acked history so real
    /// losses stay detectable, and an absent recovered version bounds nothing.
    #[test]
    fn reopen_version_domain_bound_applies_only_to_lossy_reopens() {
        use super::whole_db::reopen_version_domain_bound;
        use crate::testkit::recovery_oracle::verify::CrashFamily;
        use strata_core::CommitVersion;

        let visible = Some(CommitVersion::new(19));
        assert_eq!(
            reopen_version_domain_bound(CrashFamily::OnDiskDamage, visible),
            Some(CommitVersion::new(19)),
        );
        assert_eq!(
            reopen_version_domain_bound(CrashFamily::OnDiskDamage, None),
            None,
        );
        assert_eq!(
            reopen_version_domain_bound(CrashFamily::ZeroLoss, visible),
            None
        );
        assert_eq!(
            reopen_version_domain_bound(CrashFamily::ZeroLoss, None),
            None
        );
    }

    #[test]
    fn whole_db_sweep_entry_holds_and_is_non_vacuous() {
        // EXACT values, not floors: every trajectory is bit-exact from its
        // seed, so the sweep's counters are constants of (seed set, budget)
        // — and asserting them exactly kills accessor/grammar mutants that
        // floor checks structurally cannot (a mutated grammar arm shifts
        // every counter). A divergence here on another machine would itself
        // be a determinism bug worth failing on. Re-pin the constants when
        // the grammar deliberately changes.
        let dir = tempfile::tempdir().expect("tmp");
        let outcome = super::run_whole_db_harness(dir.path(), Some(4)).expect("whole-db sweep");
        // Re-pinned for #2853: fork-at-version calls inside surviving timeline
        // coverage now succeed, shifting the trajectories' live sets and rng
        // draws (a deliberate semantic change).
        assert_eq!(outcome.seeds_executed(), 4, "{outcome:?}");
        // Re-pinned for slice 13: the reclaim-cadence action and the new
        // epoch endings (clean close, orphaned snapshot) shift every draw.
        assert_eq!(outcome.epochs_executed(), 12, "{outcome:?}");
        assert_eq!(outcome.crashed_epochs(), 9, "{outcome:?}");
        assert_eq!(outcome.forks(), 12, "{outcome:?}");
        assert_eq!(outcome.deletes(), 3, "{outcome:?}");
        assert_eq!(outcome.temporal_probes_ok(), 22, "{outcome:?}");
        // The footprint oracle's own counters (slice 13).
        // Re-pinned for slice 12: checkpoints that used to defer on a flushed
        // non-seeded branch now complete, so more bytes are reclaimed and an
        // orphaned-snapshot ending finds a snapshot to copy.
        // Re-pinned for #3622: snapshot prunes now report the bytes they free.
        // Re-pinned for #3619: a sweep's staged bytes no longer count twice.
        // Re-pinned for #3643: the timeline moved out of each snapshot into
        // sealed segments, so a pruned snapshot frees fewer bytes and its
        // superseded tail segment frees the rest (every other fact unchanged).
        assert_eq!(outcome.bytes_reclaimed(), 24024, "{outcome:?}");
        assert_eq!(outcome.clean_closes(), 1, "{outcome:?}");
        assert_eq!(outcome.orphaned_snapshots_planted(), 1, "{outcome:?}");
    }

    /// #2859 family B regression pin: seed 289 at the 6x48 deep shape crosses
    /// a lossy reopen whose recovered clock legally re-issues version numbers.
    /// Without the model's version-domain truncation the stale acked facts
    /// collide with the re-issue and the zero-loss step probe reports a
    /// phantom `LostAck`; the trajectory must replay clean end to end.
    #[test]
    fn whole_db_version_domain_reconciliation_survives_reissue_seed() {
        let dir = tempfile::tempdir().expect("tmp");
        let facts = super::whole_db::run_whole_db_sim(dir.path(), 289, 6, 48)
            .expect("seed 289 must survive the re-issue reopen");
        assert!(facts.epochs() > 1, "{facts:?}");
        assert!(facts.crashed_epochs() > 0, "{facts:?}");
    }

    /// A second exact-constants config (seeds 0-1): distinct pinned values
    /// at a different budget keep constant-mutants from coinciding with any
    /// single configuration.
    #[test]
    fn whole_db_mini_sweep_constants_are_stable() {
        let dir = tempfile::tempdir().expect("tmp");
        let outcome = super::run_whole_db_harness(dir.path(), Some(2)).expect("mini sweep");
        assert_eq!(outcome.seeds_executed(), 2, "{outcome:?}");
    }

    #[test]
    fn sweep_holds_and_is_non_vacuous() {
        let dir = tempfile::tempdir().expect("tmp");
        let outcome = run_simulation_harness(dir.path(), Some(12)).expect("simulation sweep");
        assert!(outcome.seeds_executed() > 0, "no seeds executed");
        assert!(outcome.steps_executed() > 0, "no steps executed");
        // Non-vacuousness: the interleaving actually completed production
        // maintenance, and the manual clock advanced.
        assert!(
            outcome.maintenance_completed() > 0,
            "no maintenance completed across the sweep: {outcome:?}"
        );
        assert!(
            outcome.clock_advances() > 0,
            "manual clock never advanced: {outcome:?}"
        );
    }

    /// TCP4.11c shape-knob truth table: absent → canonical default; a
    /// positive integer overrides; zero, negatives, junk, and empty fail
    /// loudly (a silently-ignored typo would run the wrong shape and break
    /// the replay contract).
    #[test]
    fn shape_knob_parses_or_fails_loudly() {
        use super::parse_shape_knob;
        assert_eq!(parse_shape_knob("K", None, 3).expect("default"), 3);
        assert_eq!(parse_shape_knob("K", Some("6"), 3).expect("override"), 6);
        assert_eq!(parse_shape_knob("K", Some(" 48 "), 24).expect("trims"), 48);
        assert!(parse_shape_knob("K", Some("0"), 3).is_err());
        assert!(parse_shape_knob("K", Some("-1"), 3).is_err());
        assert!(parse_shape_knob("K", Some("deep"), 3).is_err());
        assert!(parse_shape_knob("K", Some(""), 3).is_err());
    }

    /// The repro line carries the shape exactly when it is non-canonical:
    /// canonical failures keep the corpus-documented one-liner, deepened
    /// failures replay bit-exact only with the shape env prefix.
    #[test]
    fn shape_prefix_rides_only_non_canonical_repro_lines() {
        use super::whole_db_shape_prefix;
        assert_eq!(whole_db_shape_prefix(3, 24), "");
        assert_eq!(
            whole_db_shape_prefix(6, 48),
            "STRATA_SIM_EPOCHS=6 STRATA_SIM_STEPS_PER_EPOCH=48 "
        );
        assert_eq!(
            whole_db_shape_prefix(3, 48),
            "STRATA_SIM_EPOCHS=3 STRATA_SIM_STEPS_PER_EPOCH=48 "
        );
        assert_eq!(
            whole_db_shape_prefix(6, 24),
            "STRATA_SIM_EPOCHS=6 STRATA_SIM_STEPS_PER_EPOCH=24 "
        );
    }
}
