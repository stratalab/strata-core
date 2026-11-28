//! Two appends to one log never land at one sequence (#3776).
//!
//! An append reads the log's metadata (the next sequence and the chain's
//! head hash) and commits its event at that sequence together with the
//! advanced metadata. Services are `&self`, so two threads can append to the
//! same log at once. Appends to one log take turns from that read through
//! their commit, and each commit is conditioned on the metadata it read, so
//! a writer that takes no turn still cannot be overwritten. These tests hold
//! an append between its read and its commit while such a writer (artifact
//! replay) commits, and hold the log to its contract: every acknowledged
//! append keeps its own sequence, its payload reads back, and the chain
//! verifies.
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

/// Appends one event labelled `label` and returns its sequence.
fn append(database: &Database, label: &str) -> EventSequence {
    database
        .event(branch(), space())
        .expect("event service opens")
        .append(
            EventType::new("note").expect("valid event type"),
            EventPayload::new(json!({ "label": label })).expect("valid payload"),
        )
        .expect("the append is acknowledged")
        .sequence()
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

/// Holds an append between its metadata read and its commit, after
/// `seeded` earlier appends, while artifact replay commits one event.
/// Replay stands in for a writer that takes no turn: in production it runs
/// only under an exclusive handle, so this interleaving is the one way to
/// make the commit condition, not the turn, decide the race. The append's commit then finds the metadata moved: it
/// must read the log again and land after the replayed event, never over it.
fn an_append_that_loses_the_race_lands_after_the_winner(database: &Database, seeded: u64) {
    for index in 0..seeded {
        append(database, &format!("seed-{index}"));
    }
    let pause = pause_at_next_commit(database);
    let late = std::thread::scope(|scope| {
        // Owned by the scope, so a failed step drops the resume handle and
        // the paused append fails instead of waiting forever.
        let (paused, resume) = pause;
        let writer = scope.spawn(|| append(database, "late"));
        paused
            .recv_timeout(Duration::from_secs(30))
            .expect("the append pauses before its commit");
        // The hook fired once, on the append, so the replay commits while the
        // append still holds the metadata it read before it.
        database
            .event(branch(), space())
            .expect("event service opens")
            .replay_append(vec![(
                EventType::new("note").expect("valid event type"),
                EventPayload::new(json!({ "label": "replayed" })).expect("valid payload"),
                an_hour_from_now(),
            )])
            .expect("the replay commits");
        resume.send(()).expect("the append is waiting");
        writer.join().expect("the writer thread")
    });

    let replayed = EventSequence::new(seeded);
    assert_eq!(label_at(database, replayed), "replayed");
    assert_eq!(
        late.as_u64(),
        seeded + 1,
        "the append lands after the winner"
    );
    assert_eq!(label_at(database, late), "late");
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
fn test_an_append_on_a_new_log_that_loses_the_race_lands_after_the_winner() {
    an_append_that_loses_the_race_lands_after_the_winner(&open_cache_database(), 0);
}

#[test]
fn test_an_append_on_a_written_log_that_loses_the_race_lands_after_the_winner() {
    an_append_that_loses_the_race_lands_after_the_winner(&open_cache_database(), 2);
}

#[test]
fn test_an_append_that_loses_the_race_lands_after_the_winner_in_durable_mode() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let database = Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
        .expect("durable open succeeds")
        .into_database();
    an_append_that_loses_the_race_lands_after_the_winner(&database, 1);
}

/// Appends to one log take turns: an append holds its log's turn from its
/// metadata read through its commit, so no other append to that log can
/// read the metadata it is about to change.
#[test]
fn test_an_append_holds_its_logs_turn_from_its_read_through_its_commit() {
    let database = open_cache_database();
    append(&database, "seed");
    let row = database
        .event(branch(), space())
        .expect("event service opens")
        .metadata_row_for_test();
    assert!(
        !database.write_turn_is_held_for_test(&row),
        "no append is in flight"
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
            database.write_turn_is_held_for_test(&row),
            "the append holds its log's turn at its commit"
        );
        resume.send(()).expect("the append is waiting");
        writer.join().expect("the writer thread");
    });
    assert!(
        !database.write_turn_is_held_for_test(&row),
        "the turn is released once the append lands"
    );
}

/// Turns are per log, not one for the database: while an append holds its
/// log's turn, appends to other logs are not made to wait. Two logs may
/// share a stripe, so of several other logs at least one must be free.
#[test]
fn test_an_append_holds_only_its_own_logs_turn() {
    let database = open_cache_database();
    let other_logs = (0..8)
        .map(|index| {
            database
                .event(
                    branch(),
                    ProductSpace::new(format!("other-{index}")).expect("valid product space"),
                )
                .expect("event service opens")
                .metadata_row_for_test()
        })
        .collect::<Vec<_>>();
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
            other_logs
                .iter()
                .any(|row| !database.write_turn_is_held_for_test(row)),
            "an append to one log leaves other logs' turns free"
        );
        resume.send(()).expect("the append is waiting");
        writer.join().expect("the writer thread");
    });
}

/// The retry itself, without a second thread: a commit refused as a lost
/// race applies nothing, so the retried append lands exactly once, at the
/// next sequence, and the log holds no ghost of the refused attempt.
#[test]
fn test_an_append_refused_as_a_lost_race_lands_once_on_retry() {
    let mut database = open_cache_database();
    let first = append(&database, "first");
    database.inject_commit_fault_for_test(crate::persistence::StorageFaultKind::Conflict);
    let second = append(&database, "second");

    assert_eq!(second.as_u64(), first.as_u64() + 1);
    assert_eq!(label_at(&database, second), "second");
    let mut events = database
        .event(branch(), space())
        .expect("event service opens");
    assert_eq!(events.len().expect("the length reads").count(), 2);
    assert!(events
        .verify_chain()
        .expect("the chain verifies")
        .is_valid());
}
