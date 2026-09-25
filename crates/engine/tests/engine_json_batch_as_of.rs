//! #3485: a JSON batch read can be pinned to one commit version or one
//! timestamp, so a set of documents hydrated for a version-pinned graph
//! result comes from that version — never a mix of it and the latest state.
//! `batch_get_at_version` and `batch_get_at` answer exactly what the per-
//! document `get_at_version` / `get_versioned_at` answer for each entry, in
//! request order, with duplicates repeated and absent documents `None`, and
//! they surface the temporal-window diagnostics instead of falling back to
//! the latest state. These tests hold the batch to the per-document oracle
//! across a history of overwrites and deletes, in cache and durable modes,
//! across a fork and after a durable reopen.

mod common;

use serde_json::json;
use strata_core::{CommitVersion, Timestamp};
use strata_engine::{
    BranchName, Database, JsonDocumentId, JsonGetEntry, JsonPath, JsonService, JsonValue,
};

use common::{branch, open_cache_database, open_durable_database, space};

fn run_database_modes(exercise: fn(&mut Database)) {
    let mut cache = open_cache_database().expect("cache open succeeds");
    exercise(&mut cache);

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open succeeds");
    exercise(&mut durable);
}

fn json_service<'a>(database: &'a mut Database, branch_name: &str) -> JsonService<'a> {
    database
        .json(branch(branch_name), space("default"))
        .expect("JSON service opens")
}

fn doc(value: &str) -> JsonDocumentId {
    JsonDocumentId::new(value).expect("valid document id")
}

fn root() -> JsonPath {
    JsonPath::root()
}

fn path(value: &str) -> JsonPath {
    value.parse().expect("valid JSON path")
}

fn value(value: serde_json::Value) -> JsonValue {
    JsonValue::new(value).expect("valid JSON value")
}

fn entry(id: &str, at: JsonPath) -> JsonGetEntry {
    JsonGetEntry::new(doc(id), at)
}

/// The commit points of the two-version history the tests read:
/// at `v1` address A spells its street one way and address B exists; at
/// `v2` A was respelled and B removed.
struct History {
    v1: CommitVersion,
    t1: Timestamp,
    v2: CommitVersion,
    t2: Timestamp,
}

/// Writes the history: A and B created (v1 is B's commit), then A respelled
/// and B deleted (v2 is the delete's commit).
fn seed(json: &mut JsonService<'_>) -> History {
    json.create(doc("addr:a"), value(json!({"street": "W 4th St", "n": 1})))
        .expect("A created");
    let b = json
        .create(doc("addr:b"), value(json!({"street": "Bleecker St"})))
        .expect("B created")
        .commit();
    json.set(
        doc("addr:a"),
        &path("street"),
        value(json!("West 4th Street")),
    )
    .expect("A respelled");
    let removed = json
        .delete(doc("addr:b"), &root())
        .expect("B removed")
        .commit()
        .expect("B's delete applied and committed");
    History {
        v1: b.version(),
        t1: b.timestamp(),
        v2: removed.version(),
        t2: removed.timestamp(),
    }
}

/// The batch every test reads, as `(document, path)` with `""` for the
/// root: A's street, B's root, a duplicate of A's street, a document that
/// never existed, A's root, and a field A never had. The last entry pins
/// that a path absent in a *present* document is a miss (`None`), exactly as
/// an absent document is — the same answer the per-document read gives.
const ENTRY_SPECS: [(&str, &str); 6] = [
    ("addr:a", "street"),
    ("addr:b", ""),
    ("addr:a", "street"),
    ("addr:missing", ""),
    ("addr:a", ""),
    ("addr:a", "zip"),
];

fn spec_path(spec: &str) -> JsonPath {
    if spec.is_empty() {
        root()
    } else {
        path(spec)
    }
}

fn entries() -> Vec<JsonGetEntry> {
    ENTRY_SPECS
        .iter()
        .map(|(id, at)| entry(id, spec_path(at)))
        .collect()
}

/// What the batch must say at `v1`: the old spelling twice, B present, the
/// unknown document absent, and A's root with the old spelling.
fn assert_v1_answers(
    answers: &[Option<strata_engine::JsonVersionedValue>],
    history: &History,
    context: &str,
) {
    assert_eq!(answers.len(), 6, "{context}: one answer per entry");
    let a = answers[0]
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: A street at v1"));
    assert_eq!(a.value().as_inner(), &json!("W 4th St"), "{context}");
    assert!(
        a.version() <= history.v1,
        "{context}: A's own commit is at or before v1"
    );
    let b = answers[1]
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: B at v1"));
    assert_eq!(
        b.value().as_inner(),
        &json!({"street": "Bleecker St"}),
        "{context}"
    );
    assert_eq!(
        b.version(),
        history.v1,
        "{context}: B answers with its own commit"
    );
    assert_eq!(
        b.timestamp(),
        history.t1,
        "{context}: B answers with its own timestamp"
    );
    assert_eq!(
        answers[2].as_ref().map(|v| v.value().as_inner().clone()),
        Some(json!("W 4th St")),
        "{context}: a duplicate entry is answered again"
    );
    assert!(
        answers[3].is_none(),
        "{context}: an unknown document is absent"
    );
    let a_root = answers[4]
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: A root at v1"));
    assert_eq!(
        a_root.value().as_inner(),
        &json!({"street": "W 4th St", "n": 1}),
        "{context}"
    );
    assert!(
        answers[5].is_none(),
        "{context}: a field A never had is a miss, not null"
    );
}

/// What the batch must say at `v2` and at the latest state: the new spelling,
/// B gone, the unknown document absent.
fn assert_v2_answers(
    answers: &[Option<strata_engine::JsonVersionedValue>],
    history: &History,
    context: &str,
) {
    assert_eq!(answers.len(), 6, "{context}: one answer per entry");
    let a = answers[0]
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: A street at v2"));
    assert_eq!(a.value().as_inner(), &json!("West 4th Street"), "{context}");
    assert!(
        a.version() > history.v1 && a.version() < history.v2,
        "{context}: A's respelling commit"
    );
    assert!(answers[1].is_none(), "{context}: B is gone at v2");
    assert_eq!(
        answers[2].as_ref().map(|v| v.value().as_inner().clone()),
        Some(json!("West 4th Street")),
        "{context}"
    );
    assert!(answers[3].is_none(), "{context}");
    assert_eq!(
        answers[4].as_ref().map(|v| v.value().as_inner().clone()),
        Some(json!({"street": "West 4th Street", "n": 1})),
        "{context}"
    );
    assert!(
        answers[5].is_none(),
        "{context}: a field A never had is a miss, not null"
    );
}

#[test]
fn batch_get_at_version_reads_every_entry_from_the_pinned_version() {
    run_database_modes(exercise_pinned_version);
}

/// The issue's case: at v1 A has one spelling and B exists; at v2 A changed
/// and B is gone. Hydrating from v1 returns v1's A and B even though the
/// latest state differs — and the latest batch still says the latest.
fn exercise_pinned_version(database: &mut Database) {
    let mut json = json_service(database, "default");
    let history = seed(&mut json);

    let at_v1 = json
        .batch_get_at_version(&entries(), history.v1)
        .expect("batch at v1 succeeds");
    assert_v1_answers(&at_v1, &history, "at v1");

    let by_instant_1 = json
        .batch_get_at(&entries(), history.t1)
        .expect("batch at t1 succeeds");
    assert_v1_answers(&by_instant_1, &history, "at t1");

    let at_v2 = json
        .batch_get_at_version(&entries(), history.v2)
        .expect("batch at v2 succeeds");
    assert_v2_answers(&at_v2, &history, "at v2");

    let by_instant_2 = json
        .batch_get_at(&entries(), history.t2)
        .expect("batch at t2 succeeds");
    assert_v2_answers(&by_instant_2, &history, "at t2");

    let latest = json.batch_get(&entries()).expect("latest batch succeeds");
    assert_v2_answers(&latest, &history, "latest");
}

#[test]
fn batch_get_at_version_matches_the_per_document_reads_at_every_commit() {
    run_database_modes(exercise_oracle);
}

/// The batch is the per-document read, plural: at every commit of the
/// history, and at every commit's timestamp, each answer equals what
/// `get_at_version` / `get_versioned_at` say for that entry on its own.
fn exercise_oracle(database: &mut Database) {
    let mut json = json_service(database, "default");
    let history = seed(&mut json);
    let first = json
        .get_versions(&doc("addr:a"))
        .expect("history reads")
        .expect("A has history");
    let versions: Vec<(CommitVersion, Timestamp)> = first
        .rows()
        .iter()
        .map(|row| (row.version(), row.timestamp()))
        .chain([(history.v1, history.t1), (history.v2, history.t2)])
        .collect();
    assert!(versions.len() >= 4, "the history spans several commits");

    for (version, timestamp) in versions {
        let by_version = json
            .batch_get_at_version(&entries(), version)
            .expect("batch at version succeeds");
        let by_timestamp = json
            .batch_get_at(&entries(), timestamp)
            .expect("batch at timestamp succeeds");
        for (index, (id, at)) in ENTRY_SPECS.iter().enumerate() {
            let (id, at) = (doc(id), spec_path(at));
            let single_value = json
                .get_at_version(&id, &at, version)
                .expect("single read at version succeeds");
            assert_eq!(
                by_version[index].as_ref().map(|v| v.value().clone()),
                single_value,
                "entry {index} at version {version:?}"
            );
            let single_versioned = json
                .get_versioned_at(&id, &at, timestamp)
                .expect("single read at timestamp succeeds");
            assert_eq!(
                by_timestamp[index].as_ref().map(|v| (
                    v.value().clone(),
                    v.version(),
                    v.timestamp()
                )),
                single_versioned
                    .as_ref()
                    .map(|v| (v.value().clone(), v.version(), v.timestamp())),
                "entry {index} at timestamp {timestamp:?}"
            );
        }
    }
}

#[test]
fn batch_get_at_outside_the_retained_window_is_a_diagnostic_not_the_latest_state() {
    run_database_modes(exercise_window_diagnostics);
}

/// A timestamp before the retained history or after the latest commit fails
/// the whole batch with the temporal-window diagnostic. A batch that fell
/// back to the latest state would answer instead — the exact mix of versions
/// the pinned read exists to rule out.
fn exercise_window_diagnostics(database: &mut Database) {
    let mut json = json_service(database, "default");
    seed(&mut json);

    for (instant, what) in [
        (Timestamp::EPOCH, "before the retained history"),
        (Timestamp::MAX, "after the latest commit"),
    ] {
        let error = json
            .batch_get_at(&entries(), instant)
            .expect_err("an out-of-window batch is refused");
        assert_eq!(
            error.code(),
            "history_unavailable.engine.persistence_history",
            "{what}"
        );
        // The same class and code the single read raises for the instant:
        // the code is what tells a trimmed or future instant from a document
        // that simply is not there.
        let single = json
            .get_versioned_at(&doc("addr:a"), &root(), instant)
            .expect_err("the single read is refused the same way");
        assert_eq!(error.class(), single.class(), "{what}");
        assert_eq!(error.code(), single.code(), "{what}");
    }
}

#[test]
fn batch_get_at_version_on_an_empty_batch_reads_nothing() {
    run_database_modes(exercise_empty_batch);
}

fn exercise_empty_batch(database: &mut Database) {
    let mut json = json_service(database, "default");
    let history = seed(&mut json);
    assert!(json
        .batch_get_at_version(&[], history.v1)
        .expect("empty batch succeeds")
        .is_empty());
    assert!(json
        .batch_get_at(&[], history.t1)
        .expect("empty batch succeeds")
        .is_empty());
}

#[test]
fn batch_get_at_version_on_a_fork_reads_the_inherited_history() {
    run_database_modes(exercise_fork);
}

/// A fork inherits the source's history: the child's batch at v1 answers
/// v1, and a write on the child moves neither the source's answers nor the
/// child's pinned ones.
fn exercise_fork(database: &mut Database) {
    let history = {
        let mut json = json_service(database, "default");
        seed(&mut json)
    };
    database
        .branches()
        .expect("branch service opens")
        .fork_current(
            &branch("default"),
            BranchName::new("child").expect("branch name"),
        )
        .expect("fork succeeds");

    {
        let mut child = json_service(database, "child");
        let at_v1 = child
            .batch_get_at_version(&entries(), history.v1)
            .expect("child batch at v1 succeeds");
        assert_v1_answers(&at_v1, &history, "child at v1");
        child
            .set(doc("addr:a"), &path("street"), value(json!("W Fourth St")))
            .expect("child write succeeds");
        let at_v1_after = child
            .batch_get_at_version(&entries(), history.v1)
            .expect("child batch at v1 succeeds after its write");
        assert_v1_answers(&at_v1_after, &history, "child at v1 after its write");
        let latest = child.batch_get(&entries()).expect("child latest succeeds");
        assert_eq!(
            latest[0].as_ref().map(|v| v.value().as_inner().clone()),
            Some(json!("W Fourth St")),
            "the child's latest is its own write"
        );
    }

    let mut source = json_service(database, "default");
    let at_v1 = source
        .batch_get_at_version(&entries(), history.v1)
        .expect("source batch at v1 succeeds");
    assert_v1_answers(&at_v1, &history, "source at v1");
    let latest = source
        .batch_get(&entries())
        .expect("source latest succeeds");
    assert_v2_answers(&latest, &history, "source latest after the child's write");
}

/// The pinned answers survive a durable reopen: the history is on disk, and
/// the batch at v1 says the same thing before and after.
#[test]
fn batch_get_at_version_survives_a_durable_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let history = {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        let history = {
            let mut json = json_service(&mut database, "default");
            let history = seed(&mut json);
            let at_v1 = json
                .batch_get_at_version(&entries(), history.v1)
                .expect("batch at v1 succeeds");
            assert_v1_answers(&at_v1, &history, "before reopen");
            history
        };
        database.close().expect("close succeeds");
        history
    };

    let mut reopened = open_durable_database(tempdir.path()).expect("reopen succeeds");
    let mut json = json_service(&mut reopened, "default");
    let at_v1 = json
        .batch_get_at_version(&entries(), history.v1)
        .expect("batch at v1 succeeds after reopen");
    assert_v1_answers(&at_v1, &history, "after reopen");
    let by_instant_1 = json
        .batch_get_at(&entries(), history.t1)
        .expect("batch at t1 succeeds after reopen");
    assert_v1_answers(&by_instant_1, &history, "after reopen, by timestamp");
    let latest = json
        .batch_get(&entries())
        .expect("latest succeeds after reopen");
    assert_v2_answers(&latest, &history, "latest after reopen");
}
