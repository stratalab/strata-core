//! An unpinned JSON batch read answers every entry from one version (#3582).
//!
//! `json.batch_get` promises "one snapshot": a batch cannot straddle a
//! concurrent write. These tests commit while a batch is in progress and hold
//! the batch to that promise: two documents written in one commit come back
//! from the same commit, never one from before it and one from after.
//!
//! They live in the crate, not under `tests/`, so the test-only point-read
//! hook they pause on is built under default features: the mutation lane
//! compiles and runs them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::json;

use crate::persistence::RowCorruption;
use crate::{
    BranchName, CacheOpenOptions, Database, DurableLocalOpenOptions, JsonDocumentId, JsonGetEntry,
    JsonPath, JsonSetEntry, JsonValue, JsonVersionedValue, ProductSpace,
};

/// The code a JSON document row that does not decode reports.
const CORRUPT_DOCUMENT_CODE: &str = "data_loss.engine.json_document";

fn branch(name: &str) -> BranchName {
    BranchName::new(name).expect("valid branch name")
}

fn space(name: &str) -> ProductSpace {
    ProductSpace::new(name).expect("valid product space")
}

fn open_cache_database() -> Database {
    Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database()
}

fn doc(id: &str) -> JsonDocumentId {
    JsonDocumentId::new(id).expect("valid document id")
}

/// Writes documents `a` and `b` with the same counter in one commit.
fn write_pair(database: &Database, counter: u64) {
    let mut service = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    let entry = |id: &str| {
        JsonSetEntry::new(
            doc(id),
            JsonPath::root(),
            JsonValue::new(json!({ "counter": counter })).expect("valid JSON value"),
        )
    };
    service
        .batch_set_or_create([entry("a"), entry("b")])
        .expect("the pair commits");
}

/// Reads `a` then `b` in one unpinned batch.
fn read_pair(database: &Database) -> Vec<Option<JsonVersionedValue>> {
    database
        .json(branch("default"), space("default"))
        .expect("JSON service opens")
        .batch_get(&[
            JsonGetEntry::new(doc("a"), JsonPath::root()),
            JsonGetEntry::new(doc("b"), JsonPath::root()),
        ])
        .expect("the batch read succeeds")
}

/// Arms a pause at the next point read: the reading thread signals and then
/// waits to be resumed. Returns the pause signal and the resume handle.
fn pause_at_next_point_read(database: &Database) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    database.on_point_read_for_test(0, move || {
        paused_tx.send(()).expect("the test waits for the pause");
        resume_rx.recv().expect("the test resumes the batch");
    });
    (paused_rx, resume_tx)
}

/// Holds the batch at each of its two point reads and commits a new pair at
/// each pause, so one commit lands between the batch's two reads whichever
/// side of a storage read the pause sits on. Both answers must still come
/// from the pair committed before the batch began.
fn batch_answers_one_version_across_concurrent_commits(database: &Database) {
    write_pair(database, 0);
    let first_pause = pause_at_next_point_read(database);
    let answers = std::thread::scope(|scope| {
        let reader = scope.spawn(|| read_pair(database));
        let (paused, resume) = first_pause;
        paused
            .recv_timeout(Duration::from_secs(30))
            .expect("the batch pauses at its first point read");
        write_pair(database, 1);
        // Armed only now: a hook fires on whichever thread reads next, and
        // the commit above reads too.
        let (paused_again, resume_again) = pause_at_next_point_read(database);
        resume.send(()).expect("the batch is waiting");
        paused_again
            .recv_timeout(Duration::from_secs(30))
            .expect("the batch pauses at its second point read");
        write_pair(database, 2);
        resume_again.send(()).expect("the batch is waiting");
        reader.join().expect("the reader thread")
    });

    let [Some(a), Some(b)] = answers.as_slice() else {
        panic!("both documents are found: {answers:?}");
    };
    assert_eq!(
        a.version(),
        b.version(),
        "both entries are read from one commit, not across a concurrent one"
    );
    let first_pair = JsonValue::new(json!({ "counter": 0 })).expect("valid JSON value");
    assert_eq!(
        a.value(),
        &first_pair,
        "the batch reads the pair it began at"
    );
    assert_eq!(
        b.value(),
        &first_pair,
        "the batch reads the pair it began at"
    );
}

/// Each answer is decoded before the next row is read, so the batch holds
/// one raw row at a time: a decode failure on the first entry ends the batch
/// before the second entry is ever read. A batch that read every row before
/// decoding any would reach the second read first.
#[test]
fn test_json_batch_decodes_each_answer_before_reading_the_next() {
    let mut database = open_cache_database();
    write_pair(&database, 0);
    database.inject_read_corruption_for_test(RowCorruption::SetValue(b"not a document".to_vec()));
    let (second_read_tx, second_read_rx) = mpsc::channel();
    database.on_point_read_for_test(1, move || {
        second_read_tx.send(()).expect("the test is listening");
    });

    let error = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens")
        .batch_get(&[
            JsonGetEntry::new(doc("a"), JsonPath::root()),
            JsonGetEntry::new(doc("b"), JsonPath::root()),
        ])
        .expect_err("the corrupted first document fails the batch");

    assert_eq!(error.code(), CORRUPT_DOCUMENT_CODE);
    assert!(
        second_read_rx.try_recv().is_err(),
        "the second entry was never read"
    );
}

/// Pairs the stress writer commits; enough that a batch read split across
/// storage reads lands between two of them many times over.
const STRESS_COMMITS: u64 = 400;

/// The stress variant: no pause point, just a writer committing pairs while
/// a reader batch-reads them. It does not depend on where the batch meets the
/// test hook, so it holds whatever shape the batch read takes inside. The
/// writer starts only once the reader has read, so the two overlap however
/// the scheduler runs them.
#[test]
fn test_unpinned_json_batch_never_straddles_a_racing_writer() {
    let database = open_cache_database();
    write_pair(&database, 0);
    let reader_started = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            while !reader_started.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            for counter in 1..=STRESS_COMMITS {
                write_pair(&database, counter);
            }
        });
        loop {
            let answers = read_pair(&database);
            let [Some(a), Some(b)] = answers.as_slice() else {
                panic!("both documents are always present: {answers:?}");
            };
            assert_eq!(a.version(), b.version(), "one batch, one commit");
            reader_started.store(true, Ordering::Release);
            if writer.is_finished() {
                break;
            }
        }
        writer.join().expect("the writer thread");
    });
}

#[test]
fn test_unpinned_json_batch_reads_every_entry_from_one_version_in_cache_mode() {
    let database = open_cache_database();
    batch_answers_one_version_across_concurrent_commits(&database);
}

#[test]
fn test_unpinned_json_batch_reads_every_entry_from_one_version_in_durable_mode() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let database = Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
        .expect("durable open succeeds")
        .into_database();
    batch_answers_one_version_across_concurrent_commits(&database);
}
