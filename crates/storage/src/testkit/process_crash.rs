//! Process-level kill/reopen crash harness (the T5 bar).
//!
//! Every other crash suite in this tree interrupts *in-process* and reopens
//! in the same process. This harness stages the real thing: a child process
//! drives a durable Always workload and journals `intent`/`ack` lines for
//! every commit; the parent SIGKILLs it at a chosen acknowledgement
//! threshold — no destructors, no flushes, no cooperation — then reopens the
//! store and asserts the STH-1 recovery oracle: recovered state is exactly a
//! prefix of acknowledged history (no `LostAck`, `Phantom`, `TornBatch`, or
//! `Gap`), the at-most-one in-flight commit is present-or-absent atomically,
//! and the store accepts new writes.
//!
//! The intent/ack journal is ordered by program order, which the page cache
//! preserves across a process kill, so the parent's post-kill read of the
//! journal is ground truth for what the child was told. Process kills prove
//! kill-safety (state above the OS survives); power-loss realism beyond the
//! page cache is owned by the reordering/FS-model suites.

use std::io::Write;
use std::path::{Path, PathBuf};

use super::recovery_oracle::model::{ExpectedState, OracleDurability};
use super::recovery_oracle::verify::{classify_recovered, scan_recovered, CrashFamily};
use super::recovery_oracle::workload::{
    default_branch, generate_workload, oracle_prefix_key, oracle_space, to_commit_mutation,
    SCAN_LIMIT,
};
use crate::api::{
    CommitBatch, CommitOptions, StorageBackend, StorageDurabilityPolicy,
    StorageMaintenanceSchedulingPolicy, StorageOpenOptions, StorageRuntime,
};
use crate::testkit::TestkitError;

/// Environment protocol between the parent driver and the child workload.
pub(crate) const CHILD_DIR_ENV: &str = "STRATA_PROCESS_CRASH_DIR";
pub(crate) const CHILD_SEED_ENV: &str = "STRATA_PROCESS_CRASH_SEED";

/// Ops per deterministic workload chunk; the parent regenerates chunks from
/// the same seed to rebuild mutations for every journaled index.
const CHUNK_OPS: usize = 256;
/// Upper bound on child commits — far beyond any kill threshold, so the
/// child terminates even if the parent dies first.
const CHILD_COMMIT_CAP: usize = 100_000;
/// Drain cadence: exercising flush/checkpoint publication under the kill.
const CHILD_DRAIN_EVERY: usize = 32;

fn mutations_for_index(
    seed: u64,
    index: usize,
) -> Vec<super::recovery_oracle::model::RecordedMutation> {
    let chunk = index / CHUNK_OPS;
    let offset = index % CHUNK_OPS;
    generate_workload(seed.wrapping_add(chunk as u64), CHUNK_OPS)[offset].clone()
}

fn store_dir(root: &Path) -> PathBuf {
    root.join("store")
}

fn journal_path(root: &Path) -> PathBuf {
    root.join("commit-journal.log")
}

/// The child body: journal `intent <i>`, commit, journal `ack <i> <version>`,
/// forever (bounded), with periodic maintenance drains — until `SIGKILL`ed.
/// Returns immediately when the environment protocol is absent so a stray
/// `--ignored` sweep of the suite cannot hang.
pub(crate) fn run_child_workload_from_env() -> Result<(), TestkitError> {
    let Some(root) = std::env::var_os(CHILD_DIR_ENV) else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let seed: u64 = std::env::var(CHILD_SEED_ENV)
        .map_err(|_| TestkitError::new("child seed env missing"))?
        .parse()
        .map_err(|err| TestkitError::new(format!("child seed env invalid: {err}")))?;

    let branch = default_branch();
    std::fs::create_dir_all(store_dir(&root))
        .map_err(|err| TestkitError::new(format!("child store dir: {err}")))?;
    let backend = StorageBackend::local_fs(store_dir(&root));
    // Retry-on-Unavailable absorbs transient EAGAIN under loaded parallel
    // runs (#2727); any other failure stays loud.
    let mut runtime = crate::testkit::reopen_retry::open_with_retry_on_unavailable(|| {
        StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
    })
    .map_err(|err| TestkitError::new(format!("child open: {err:?}")))?
    .into_runtime();

    let mut journal = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path(&root))
        .map_err(|err| TestkitError::new(format!("child journal open: {err}")))?;

    for index in 0..CHILD_COMMIT_CAP {
        let mutations = mutations_for_index(seed, index);
        let batch = CommitBatch::new(
            branch,
            mutations.iter().map(to_commit_mutation).collect(),
            CommitOptions::default(),
        )
        .map_err(|err| TestkitError::new(format!("child batch: {err:?}")))?;
        // Program order: the intent line reaches the page cache before the
        // commit starts, the ack line only after the commit acknowledged.
        journal
            .write_all(format!("intent {index}\n").as_bytes())
            .map_err(|err| TestkitError::new(format!("child journal intent: {err}")))?;
        let summary = runtime
            .commit(&batch)
            .map_err(|err| TestkitError::new(format!("child commit {index}: {err:?}")))?;
        journal
            .write_all(format!("ack {index} {}\n", summary.commit_version().as_u64()).as_bytes())
            .map_err(|err| TestkitError::new(format!("child journal ack: {err}")))?;
        if index % CHILD_DRAIN_EVERY == CHILD_DRAIN_EVERY - 1 {
            runtime
                .drain_maintenance()
                .map_err(|err| TestkitError::new(format!("child drain: {err:?}")))?;
        }
    }
    Ok(())
}

/// Counters describing one kill/reopen round.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProcessCrashOutcome {
    acked_commits: usize,
    in_doubt_commits: usize,
}

impl ProcessCrashOutcome {
    #[must_use]
    pub(crate) const fn acked_commits(&self) -> usize {
        self.acked_commits
    }
    #[must_use]
    pub(crate) const fn in_doubt_commits(&self) -> usize {
        self.in_doubt_commits
    }
}

/// Spawn the child (this test binary re-invoked on the child test name),
/// SIGKILL it once the journal shows `kill_after_acks` acknowledgements,
/// reopen the store, and verify the recovery oracle plus resumability.
pub(crate) fn run_kill_reopen_round(
    root: &Path,
    child_test_name: &str,
    seed: u64,
    kill_after_acks: usize,
) -> Result<ProcessCrashOutcome, TestkitError> {
    std::fs::create_dir_all(root).map_err(|err| TestkitError::new(format!("round dir: {err}")))?;
    let exe =
        std::env::current_exe().map_err(|err| TestkitError::new(format!("current_exe: {err}")))?;
    let mut child = std::process::Command::new(exe)
        .args(["--exact", child_test_name, "--ignored", "--nocapture"])
        .env(CHILD_DIR_ENV, root)
        .env(CHILD_SEED_ENV, seed.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|err| TestkitError::new(format!("spawn child: {err}")))?;

    // Wait for the journal to reach the kill threshold, then SIGKILL: no
    // destructors, no flushes, no cooperation from the child.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let acks = std::fs::read_to_string(journal_path(root))
            .map(|journal| {
                journal
                    .lines()
                    .filter(|line| line.starts_with("ack "))
                    .count()
            })
            .unwrap_or(0);
        if acks >= kill_after_acks {
            break;
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|err| TestkitError::new(format!("child try_wait: {err}")))?
        {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                use std::io::Read;
                let _ = pipe.read_to_string(&mut stderr);
            }
            return Err(TestkitError::new(format!(
                "child exited ({status}) before reaching {kill_after_acks} acks:\n{stderr}"
            )));
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(TestkitError::new(format!(
                "child never reached {kill_after_acks} acks within the deadline"
            )));
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    child
        .kill()
        .map_err(|err| TestkitError::new(format!("kill child: {err}")))?;
    let _ = child
        .wait()
        .map_err(|err| TestkitError::new(format!("wait child: {err}")))?;

    verify_recovered_against_journal(root, seed, kill_after_acks)
}

/// Rebuild the acknowledged-history model from the journal and hold the
/// reopened store to it: every acked index is owed (Always durability,
/// `ZeroLoss`); the at-most-trailing intent without an ack is in-doubt
/// (present-or-absent atomically). Split out so the sabotage test can prove
/// the verifier detects loss, not merely blesses recovery.
fn verify_recovered_against_journal(
    root: &Path,
    seed: u64,
    kill_after_acks: usize,
) -> Result<ProcessCrashOutcome, TestkitError> {
    let journal = std::fs::read_to_string(journal_path(root))
        .map_err(|err| TestkitError::new(format!("read journal: {err}")))?;
    let branch = default_branch();
    let mut model = ExpectedState::new(OracleDurability::Always);
    let mut outcome = ProcessCrashOutcome::default();
    let mut pending_intent: Option<usize> = None;
    for line in journal.lines() {
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next(), parts.next()) {
            (Some("intent"), Some(index), None) => {
                let index: usize = index
                    .parse()
                    .map_err(|err| TestkitError::new(format!("journal intent index: {err}")))?;
                pending_intent = Some(index);
            }
            (Some("ack"), Some(index), Some(version)) => {
                let index: usize = index
                    .parse()
                    .map_err(|err| TestkitError::new(format!("journal ack index: {err}")))?;
                let version: u64 = version
                    .parse()
                    .map_err(|err| TestkitError::new(format!("journal ack version: {err}")))?;
                if pending_intent != Some(index) {
                    return Err(TestkitError::new(format!(
                        "journal ack {index} without matching intent (pending {pending_intent:?})"
                    )));
                }
                pending_intent = None;
                model.record_ack(
                    branch,
                    strata_core::CommitVersion::new(version),
                    mutations_for_index(seed, index),
                );
                outcome.acked_commits += 1;
            }
            _ => {
                return Err(TestkitError::new(format!(
                    "journal line malformed: {line:?}"
                )));
            }
        }
    }
    if let Some(index) = pending_intent {
        model.record_in_doubt(branch, mutations_for_index(seed, index));
        outcome.in_doubt_commits += 1;
    }

    // The reopen after the kill IS the system under test.
    let backend = StorageBackend::local_fs(store_dir(root));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            ),
        &backend,
    )
    .map_err(|err| TestkitError::new(format!("reopen after kill must succeed: {err:?}")))?
    .into_runtime();
    let recovered = scan_recovered(
        &runtime,
        branch,
        &oracle_space(),
        &oracle_prefix_key(),
        SCAN_LIMIT,
    )?;
    if let Err(violation) = classify_recovered(&model, branch, &recovered, CrashFamily::ZeroLoss) {
        return Err(TestkitError::new(format!(
            "recovery oracle violation after SIGKILL [seed={seed} kill_after={kill_after_acks} \
             acked={} in_doubt={}]: {violation:?}",
            outcome.acked_commits, outcome.in_doubt_commits
        )));
    }
    // Kill-safety includes resumability: the reopened store takes writes.
    let resume = mutations_for_index(seed.wrapping_add(0x5157), 0);
    let batch = CommitBatch::new(
        branch,
        resume.iter().map(to_commit_mutation).collect(),
        CommitOptions::default(),
    )
    .map_err(|err| TestkitError::new(format!("resume batch: {err:?}")))?;
    runtime
        .commit(&batch)
        .map_err(|err| TestkitError::new(format!("resume after kill/reopen: {err:?}")))?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The child body. `#[ignore]` so it never runs in a normal sweep; the
    /// parent invokes it by exact name in a separate process and SIGKILLs
    /// it. Without the environment protocol it returns immediately.
    #[test]
    #[ignore = "process-crash child workload; spawned and SIGKILLed by the parent harness"]
    fn process_crash_child_workload() {
        run_child_workload_from_env().expect("child workload");
    }

    const CHILD_TEST_NAME: &str = "testkit::process_crash::tests::process_crash_child_workload";

    #[test]
    fn sigkilled_child_recovers_a_prefix_of_acknowledged_history() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut total_acked = 0usize;
        let mut saw_in_doubt = false;
        for (round, kill_after) in [3usize, 9, 21].into_iter().enumerate() {
            let outcome = run_kill_reopen_round(
                &dir.path().join(format!("round-{round}")),
                CHILD_TEST_NAME,
                41 + round as u64,
                kill_after,
            )
            .expect("kill/reopen round");
            assert!(
                outcome.acked_commits() >= kill_after,
                "round {round}: child was killed before the threshold it was killed at"
            );
            total_acked += outcome.acked_commits();
            saw_in_doubt |= outcome.in_doubt_commits() > 0;
        }
        assert!(total_acked > 0, "no acknowledged commits across rounds");
        // in-doubt tails are timing-dependent per round; across rounds their
        // absence is fine — the oracle already verified each journal shape.
        let _ = saw_in_doubt;
    }

    /// The verifier must detect loss, not merely bless whatever recovered:
    /// fabricating an acknowledgement the store never made must surface a
    /// typed oracle violation on re-verification.
    #[test]
    fn fabricated_acknowledgement_is_detected_as_loss() {
        let dir = tempfile::tempdir().expect("tmp");
        let root = dir.path().join("sabotage");
        run_kill_reopen_round(&root, CHILD_TEST_NAME, 97, 5).expect("clean round");
        let journal = journal_path(&root);
        let acks = std::fs::read_to_string(&journal).expect("journal");
        let last_index: usize = acks
            .lines()
            .filter_map(|line| line.strip_prefix("intent "))
            .filter_map(|index| index.parse().ok())
            .max()
            .expect("at least one intent");
        let fake = last_index + 1;
        let mut handle = std::fs::OpenOptions::new()
            .append(true)
            .open(&journal)
            .expect("append journal");
        std::io::Write::write_all(
            &mut handle,
            format!("intent {fake}\nack {fake} 999999\n").as_bytes(),
        )
        .expect("write fake ack");
        let err = verify_recovered_against_journal(&root, 97, 5)
            .expect_err("a fabricated acknowledgement must fail the oracle");
        // The exact class depends on how the fabricated mutations pollute
        // the expected state (a fake covering real keys diagnoses as
        // Phantom; a fake on fresh keys as LostAck) — the property is that
        // the verifier surfaces a typed violation, never a silent pass.
        let rendered = format!("{err}");
        assert!(
            rendered.contains("recovery oracle violation")
                && (rendered.contains("LostAck") || rendered.contains("Phantom")),
            "expected a typed oracle violation, got: {err}"
        );
    }

    /// Soak: many kill points across many seeds. `#[ignore]` by default;
    /// `STRATA_STORAGE_PROCESS_CRASH_ROUNDS` sets the depth (nightly).
    #[test]
    #[ignore = "soak: repeated SIGKILL/reopen rounds; run with --ignored (set STRATA_STORAGE_PROCESS_CRASH_ROUNDS)"]
    fn process_crash_soak_across_seeds_and_kill_points() {
        let rounds: usize = std::env::var("STRATA_STORAGE_PROCESS_CRASH_ROUNDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(25);
        let dir = tempfile::tempdir().expect("tmp");
        for round in 0..rounds {
            // Deterministic spread of kill thresholds without wall-clock
            // randomness: a small LCG over the round index.
            let kill_after = 2 + ((round.wrapping_mul(2_654_435_761)) % 61);
            run_kill_reopen_round(
                &dir.path().join(format!("round-{round}")),
                CHILD_TEST_NAME,
                1_000 + round as u64,
                kill_after,
            )
            .unwrap_or_else(|err| panic!("soak round {round} (kill_after={kill_after}): {err}"));
        }
    }
}
