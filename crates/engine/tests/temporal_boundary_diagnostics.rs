//! A time-travel request that lands outside a branch's commit history names
//! the end it fell off — before the first commit, or after the latest — rather
//! than blaming a retention window when nothing was pruned (temporal contract
//! invariant 7: before-history, after-latest and pruned cases surface distinct
//! diagnostics).
//!
//! Both clocks are covered: the logical `as_of` (a commit timestamp) and the
//! wall-clock instant that `as_of_time` resolves through, plus
//! fork-at-timestamp, which shares the logical lookup. The pruned case keeps
//! its retained-window code; `version_retention.rs` drives a real pruning
//! compaction to prove that side.

mod common;

use strata_core::Timestamp;
use strata_engine::api::CommitOutcome;
use strata_engine::Database;

use common::{branch, key, open_cache_database, open_durable_database, space, value};

const BEFORE_FIRST: &str = "history_unavailable.engine.before_first_commit";
const AFTER_LATEST: &str = "history_unavailable.engine.after_latest_commit";

/// One minute in microseconds: far enough outside a commit's instant that no
/// clock jitter can land it back inside.
const MINUTE_MICROS: u64 = 60_000_000;

fn run_database_modes(exercise: fn(&mut Database)) {
    let mut cache = open_cache_database().expect("cache open succeeds");
    exercise(&mut cache);

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open succeeds");
    exercise(&mut durable);
}

fn put(database: &mut Database, branch_name: &str, bytes: &[u8]) -> CommitOutcome {
    database
        .kv(branch(branch_name), space("default"))
        .expect("KV service opens")
        .put(key(b"k"), value(bytes))
        .expect("put succeeds")
        .commit()
}

fn committed_at(commit: &CommitOutcome) -> u64 {
    commit
        .committed_at()
        .expect("a write ack carries its wall-clock instant")
        .as_micros()
}

fn read_at_code(database: &mut Database, branch_name: &str, timestamp: Timestamp) -> String {
    database
        .kv(branch(branch_name), space("default"))
        .expect("KV service opens")
        .get_at(&key(b"k"), timestamp)
        .expect_err("a read outside the commit history is refused")
        .code()
        .to_owned()
}

fn wall_clock_code(database: &mut Database, branch_name: &str, instant: u64) -> String {
    database
        .resolve_wall_clock(&branch(branch_name), Timestamp::from_micros(instant))
        .expect_err("an instant outside the dated history is refused")
        .code()
        .to_owned()
}

#[test]
fn a_logical_as_of_outside_the_history_names_the_end_it_missed() {
    run_database_modes(exercise_logical_clock);
}

fn exercise_logical_clock(database: &mut Database) {
    let first = put(database, "default", b"one");
    let latest = put(database, "default", b"two");

    // The branch may hold commits from before `first` (space setup), so the
    // exact first-commit edge is pinned in storage, where the timeline is
    // fully controlled; the epoch is before any branch's history.
    assert_eq!(
        read_at_code(database, "default", Timestamp::EPOCH),
        BEFORE_FIRST
    );
    assert_eq!(
        read_at_code(database, "default", Timestamp::MAX),
        AFTER_LATEST
    );
    assert_eq!(
        read_at_code(
            database,
            "default",
            Timestamp::from_micros(latest.timestamp().as_micros() + 1)
        ),
        AFTER_LATEST,
        "one tick past the latest commit does not clamp to it"
    );

    // Both ends of the history itself still answer.
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    for (at, expected) in [(first.timestamp(), b"one"), (latest.timestamp(), b"two")] {
        let read = kv
            .get_at(&key(b"k"), at)
            .expect("a read inside the history succeeds")
            .expect("the key exists there");
        assert_eq!(read.as_bytes(), expected);
    }
}

#[test]
fn a_wall_clock_instant_outside_the_history_names_the_end_it_missed() {
    run_database_modes(exercise_wall_clock);
}

fn exercise_wall_clock(database: &mut Database) {
    let first = put(database, "default", b"one");
    let latest = put(database, "default", b"two");

    assert_eq!(
        wall_clock_code(database, "default", committed_at(&first) - MINUTE_MICROS),
        BEFORE_FIRST
    );
    assert_eq!(
        wall_clock_code(database, "default", committed_at(&latest) + MINUTE_MICROS),
        AFTER_LATEST
    );

    // The commit's own instant resolves to that commit's logical timestamp.
    assert_eq!(
        database
            .resolve_wall_clock(
                &branch("default"),
                Timestamp::from_micros(committed_at(&latest))
            )
            .expect("an instant inside the dated history resolves"),
        latest.timestamp()
    );
}

#[test]
fn a_fork_at_a_timestamp_outside_the_history_names_the_end_it_missed() {
    run_database_modes(exercise_fork_at_timestamp);
}

fn exercise_fork_at_timestamp(database: &mut Database) {
    let first = put(database, "default", b"one");

    for (timestamp, expected) in [
        (Timestamp::EPOCH, BEFORE_FIRST),
        (Timestamp::MAX, AFTER_LATEST),
    ] {
        let error = database
            .branches()
            .expect("branch service opens")
            .fork_at_timestamp(&branch("default"), branch("refused"), timestamp)
            .expect_err("a fork outside the source's history is refused");
        assert_eq!(error.code(), expected);
    }

    // The refusals created nothing, and the first commit's own timestamp forks.
    database
        .branches()
        .expect("branch service opens")
        .fork_at_timestamp(&branch("default"), branch("refused"), first.timestamp())
        .expect("a fork inside the history succeeds under the refused name");
}

#[test]
fn a_new_branch_s_history_starts_when_it_is_created() {
    run_database_modes(exercise_new_branch);
}

/// A created branch is not a fork: it inherits none of the source's history,
/// and creating it commits its space setup. A time from before it existed is
/// before its first commit on both clocks, permanently, and a later one is
/// after its latest.
fn exercise_new_branch(database: &mut Database) {
    let anchor = put(database, "default", b"one");
    database
        .branches()
        .expect("branch service opens")
        .create(branch("fresh"))
        .expect("branch is created");

    assert_eq!(
        read_at_code(database, "fresh", anchor.timestamp()),
        BEFORE_FIRST
    );
    assert_eq!(
        wall_clock_code(database, "fresh", committed_at(&anchor)),
        BEFORE_FIRST
    );
    assert_eq!(
        read_at_code(database, "fresh", Timestamp::MAX),
        AFTER_LATEST
    );
}
