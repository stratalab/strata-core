//! Whole-DB deterministic simulation (TCP4.11b/c): multi-branch, multi-epoch
//! seeded trajectories — commits across a live branch set, fork/delete/recreate
//! cycles, seeded crash → recover → continue epochs — continuously checked by
//! the oracle spine (per-branch prefix-of-history, temporal probes, deletion
//! semantics, branch-catalog health-vs-truth, the stacked write-ordering
//! watchdog, and the maintenance failure ring).
//!
//! Every trajectory is a pure function of its seed at the canonical shape
//! (3 epochs × 24 steps), so any failure replays bit-exact: a sweep failure
//! prints its one-line repro, executed here by `replay_single_seed`. The seed
//! corpus and replay contract are documented in
//! `src/testkit/simulation/README.md`.

#![deny(unsafe_code)]

mod common;

#[cfg(all(
    feature = "fault-injection",
    feature = "localfs",
    not(target_arch = "wasm32")
))]
#[test]
fn whole_db_simulation_holds_oracles_across_seeds() {
    let case_limit = common::fault_case_limit()
        .unwrap_or_else(|error| panic!("invalid whole-db simulation environment: {error}"));
    let tempdir = tempfile::tempdir().expect("temp whole-db simulation root");

    // Cap the seed set for the CI-fast lane; STRATA_STORAGE_FAULT_CASES overrides.
    let outcome =
        strata_storage::testkit::run_whole_db_harness(tempdir.path(), case_limit.or(Some(6)))
            .expect("whole-db simulation harness");

    // A budget of 0 is an explicit "run nothing"; otherwise the sweep must run
    // real multi-epoch trajectories that crash, fork, and probe history
    // (never a vacuous green). Exact-constant pins live with the harness's
    // unit tests; this lane asserts non-vacuity at any budget.
    assert!(
        outcome.seeds_executed() > 0 || case_limit == Some(0),
        "no whole-db seeds executed"
    );
    assert!(
        outcome.crashed_epochs() > 0 || case_limit == Some(0),
        "no epoch ended in a materialized crash: {outcome:?}"
    );
    assert!(
        outcome.forks() > 0 || case_limit == Some(0),
        "no fork was created across the sweep: {outcome:?}"
    );
    assert!(
        outcome.temporal_probes_ok() > 0 || case_limit == Some(0),
        "no temporal probe succeeded across the sweep: {outcome:?}"
    );
    // Space-reclamation contract §3.5 (slice 13): the footprint oracle is
    // non-vacuous — reclaim released bytes, epochs closed cleanly, and
    // orphaned snapshots were planted for the open reconcile to prune.
    assert!(
        outcome.bytes_reclaimed() > 0 || case_limit == Some(0),
        "no reclaim pass released bytes across the sweep: {outcome:?}"
    );
    assert!(
        outcome.clean_closes() > 0 || case_limit == Some(0),
        "no epoch closed cleanly across the sweep: {outcome:?}"
    );
    assert!(
        outcome.orphaned_snapshots_planted() > 0 || case_limit == Some(0),
        "no orphaned snapshot was planted across the sweep: {outcome:?}"
    );
}

/// Soak: a deep sweep over many seeds — the genuine bug-hunt run (this is the
/// sweep that found #2820 and #2823). `#[ignore]` by default;
/// `STRATA_STORAGE_FAULT_CASES` sets the depth.
#[cfg(all(
    feature = "fault-injection",
    feature = "localfs",
    not(target_arch = "wasm32")
))]
#[ignore = "soak: deep multi-seed whole-DB simulation; run with --ignored (raise STRATA_STORAGE_FAULT_CASES)"]
#[test]
fn whole_db_simulation_soak_deepens_across_many_seeds() {
    let case_limit = common::fault_case_limit()
        .unwrap_or_else(|error| panic!("invalid whole-db simulation environment: {error}"));
    let tempdir = tempfile::tempdir().expect("temp whole-db simulation soak root");

    let outcome =
        strata_storage::testkit::run_whole_db_harness(tempdir.path(), case_limit.or(Some(400)))
            .expect("whole-db simulation soak");

    assert!(
        outcome.crashed_epochs() > 0 && outcome.forks() > 0 && outcome.temporal_probes_ok() > 0,
        "soak was vacuous: {outcome:?}"
    );
    // The point of seed scaling: the soak must explore beyond the default seed budget.
    assert!(
        outcome.seeds_executed() > 6,
        "soak did not deepen beyond the default seed budget (got {} seeds)",
        outcome.seeds_executed()
    );
}

/// The replay contract's execution end: run exactly one seed at the canonical
/// trajectory shape. A sweep failure prints the invocation:
///
/// ```text
/// STRATA_SIM_SEED=<n> cargo test -p strata-storage --features fault-injection \
///     --test simulation_whole_db -- replay_single_seed --ignored --nocapture
/// ```
#[cfg(all(
    feature = "fault-injection",
    feature = "localfs",
    not(target_arch = "wasm32")
))]
#[ignore = "manual replay: set STRATA_SIM_SEED to the seed named by a sweep failure"]
#[test]
fn replay_single_seed() {
    let seed: u64 = std::env::var("STRATA_SIM_SEED")
        .expect("replay_single_seed requires STRATA_SIM_SEED=<seed from the failure message>")
        .parse()
        .expect("STRATA_SIM_SEED must be a u64 seed");
    let tempdir = tempfile::tempdir().expect("temp replay root");

    let outcome = strata_storage::testkit::replay_whole_db_seed(tempdir.path(), seed)
        .expect("seed replay must reproduce the sweep verdict deterministically");
    eprintln!("seed {seed} replayed clean: {outcome:?}");
}
