//! Two appends to one log never land at one sequence (#3776, #3782).
//!
//! An append reads the log's metadata (the next sequence and the chain's
//! head hash) and commits its event at that sequence together with the
//! advanced metadata. Services are `&self`, so two threads can append to the
//! same log at once. An append holds its branch's writer from that read
//! through its commit, so a second write of the branch waits for it and then
//! reads the metadata the first one left. These tests hold an append at its
//! commit while a second write of the branch arrives, and hold the log to its
//! contract: every acknowledged append keeps its own sequence, its payload
//! reads back, and the chain verifies.
//!
//! They live in the crate, not under `tests/`, so the test-only commit hook
//! they pause on is built under default features: the mutation lane
//! compiles and runs them.

use std::sync::mpsc;
use std::time::Duration;

use serde_json::json;
use strata_core::Timestamp;

use crate::{
    BranchName, CacheOpenOptions, Database, DurableLocalOpenOptions, EventPayload, EventSequence,
    EventType, ProductSpace,
};

fn branch() -> BranchName {
    BranchName::new("default").expect("valid branch name")
}

fn space() -> ProductSpace {
    ProductSpace::new("default").expect("valid product space")
}

fn open_cache_database() -> Database {
    Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database()
}

/// Appends one event labelled `label` to `branch` and returns its sequence.
fn append_to(database: &Database, branch: BranchName, label: &str) -> EventSequence {
    database
        .event(branch, space())
        .expect("event service opens")
        .append(
            EventType::new("note").expect("valid event type"),
            EventPayload::new(json!({ "label": label })).expect("valid payload"),
        )
        .expect("the append is acknowledged")
        .sequence()
}

/// Appends one event labelled `label` to the default branch.
fn append(database: &Database, label: &str) -> EventSequence {
    append_to(database, branch(), label)
}

/// The label of the event stored at `sequence`.
fn label_at(database: &Database, sequence: EventSequence) -> String {
    let record = database
        .event(branch(), space())
        .expect("event service opens")
        .get(sequence)
        .expect("the read succeeds")
        .expect("an acknowledged event is stored at its sequence");
    record.record().payload().as_inner()["label"]
        .as_str()
        .expect("the label is a string")
        .to_owned()
}

/// Arms a pause at the next commit: the committing thread signals and then
/// waits to be resumed. Returns the pause signal and the resume handle.
fn pause_at_next_commit(database: &Database) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    database.on_commit_for_test(0, move || {
        paused_tx.send(()).expect("the test waits for the pause");
        resume_rx.recv().expect("the test resumes the append");
    });
    (paused_rx, resume_tx)
}

/// A timestamp an hour from now, later than any append of this test.
fn an_hour_from_now() -> Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch");
    Timestamp::from_micros(u64::try_from(now.as_micros()).expect("fits") + 3_600_000_000)
}

/// The second write of the branch in [`a_second_write_waits_and_lands_after_the_held_append`].
#[derive(Clone, Copy)]
enum SecondWrite {
    /// Another append to the same log.
    Append,
    /// Artifact replay into the same log, which writes the branch like any
    /// other write.
    Replay,
}

impl SecondWrite {
    fn run(self, database: &Database) -> EventSequence {
        match self {
            Self::Append => append(database, "second"),
            Self::Replay => {
                database
                    .event(branch(), space())
                    .expect("event service opens")
                    .replay_append(vec![(
                        EventType::new("note").expect("valid event type"),
                        EventPayload::new(json!({ "label": "second" })).expect("valid payload"),
                        an_hour_from_now(),
                    )])
                    .expect("the replay commits");
                // Replay reports no sequence; the replayed event is the log's
                // last, and nothing else writes the log by now.
                let len = database
                    .event(branch(), space())
                    .expect("event service opens")
                    .len()
                    .expect("the length reads")
                    .count();
                EventSequence::new(len - 1)
            }
        }
    }
}

/// Holds an append at its commit, after `seeded` earlier appends, while a
/// second write of the branch arrives. The second write must wait for the
/// branch's writer (the turn-wait counter shows it queued, not timed), and
/// once the append lands it must read the metadata the append left and land
/// after it, never over it.
fn a_second_write_waits_and_lands_after_the_held_append(
    database: &Database,
    seeded: u64,
    second: SecondWrite,
) {
    for index in 0..seeded {
        append(database, &format!("seed-{index}"));
    }
    let pause = pause_at_next_commit(database);
    let (held, later) = std::thread::scope(|scope| {
        // Owned by the scope, so a failed step drops the resume handle and
        // the paused append fails instead of waiting forever.
        let (paused, resume) = pause;
        let held = scope.spawn(|| append(database, "held"));
        paused
            .recv_timeout(Duration::from_secs(30))
            .expect("the append pauses before its commit");
        let waits_before = database.writer_waits_for_test();
        let later = scope.spawn(move || second.run(database));
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !later.is_finished()
            && database.writer_waits_for_test() == waits_before
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(
            database.writer_waits_for_test() > waits_before,
            "the second write queued on the branch writer"
        );
        assert!(
            !later.is_finished(),
            "the second write waits for the held append's branch writer"
        );
        resume.send(()).expect("the append is waiting");
        (
            held.join().expect("the held append"),
            later.join().expect("the second write"),
        )
    });

    assert_eq!(held.as_u64(), seeded, "the held append keeps its sequence");
    assert_eq!(label_at(database, held), "held");
    assert_eq!(
        later.as_u64(),
        seeded + 1,
        "the second write lands after the held append"
    );
    assert_eq!(label_at(database, later), "second");
    let mut events = database
        .event(branch(), space())
        .expect("event service opens");
    assert_eq!(
        events.len().expect("the length reads").count(),
        seeded + 2,
        "the log holds every acknowledged event"
    );
    assert!(
        events
            .verify_chain()
            .expect("the chain verifies")
            .is_valid(),
        "the hash chain links every acknowledged event"
    );
}

#[test]
fn test_a_second_append_to_a_new_log_waits_for_the_held_one_and_lands_after_it() {
    a_second_write_waits_and_lands_after_the_held_append(
        &open_cache_database(),
        0,
        SecondWrite::Append,
    );
}

#[test]
fn test_a_second_append_to_a_written_log_waits_for_the_held_one_and_lands_after_it() {
    a_second_write_waits_and_lands_after_the_held_append(
        &open_cache_database(),
        2,
        SecondWrite::Append,
    );
}

#[test]
fn test_a_second_append_waits_for_the_held_one_and_lands_after_it_in_durable_mode() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let database = Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
        .expect("durable open succeeds")
        .into_database();
    a_second_write_waits_and_lands_after_the_held_append(&database, 1, SecondWrite::Append);
}

#[test]
fn test_a_replay_into_a_log_waits_for_a_held_append_and_lands_after_it() {
    a_second_write_waits_and_lands_after_the_held_append(
        &open_cache_database(),
        1,
        SecondWrite::Replay,
    );
}

/// An append holds its branch's writer through its commit and lets it go
/// once it lands.
#[test]
fn test_an_append_holds_its_branch_writer_through_its_commit() {
    let database = open_cache_database();
    append(&database, "seed");
    let branch_id = database
        .event(branch(), space())
        .expect("event service opens")
        .storage_branch_id_for_test();
    assert!(
        !database.branch_writer_is_held_for_test(branch_id),
        "no write is in flight"
    );
    let pause = pause_at_next_commit(&database);
    std::thread::scope(|scope| {
        // Owned by the scope, so a failed check drops the resume handle and
        // the paused append fails instead of waiting forever.
        let (paused, resume) = pause;
        let writer = scope.spawn(|| append(&database, "held"));
        paused
            .recv_timeout(Duration::from_secs(30))
            .expect("the append pauses before its commit");
        assert!(
            database.branch_writer_is_held_for_test(branch_id),
            "the append holds its branch's writer at its commit"
        );
        resume.send(()).expect("the append is waiting");
        writer.join().expect("the writer thread");
    });
    assert!(
        !database.branch_writer_is_held_for_test(branch_id),
        "the writer is released once the append lands"
    );
}

/// The writer is per branch, not one for the database: while an append
/// holds its branch, an append to another branch runs to completion.
#[test]
fn test_an_append_does_not_hold_other_branches_writers() {
    let mut database = open_cache_database();
    let other = BranchName::new("other").expect("valid branch name");
    database
        .branches()
        .expect("branch service opens")
        .create(other.clone())
        .expect("the branch is created");
    let pause = pause_at_next_commit(&database);
    std::thread::scope(|scope| {
        // Owned by the scope, so a failed check drops the resume handle and
        // the paused append fails instead of waiting forever.
        let (paused, resume) = pause;
        let held = scope.spawn(|| append(&database, "held"));
        paused
            .recv_timeout(Duration::from_secs(30))
            .expect("the append pauses before its commit");
        let (done_tx, done_rx) = mpsc::channel();
        let database = &database;
        let elsewhere = scope.spawn(move || {
            let sequence = append_to(database, other, "elsewhere");
            done_tx.send(()).expect("the test is listening");
            sequence
        });
        done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("an append to another branch lands while this one is held");
        resume.send(()).expect("the append is waiting");
        assert_eq!(held.join().expect("the held append").as_u64(), 0);
        assert_eq!(elsewhere.join().expect("the other append").as_u64(), 0);
    });
}
