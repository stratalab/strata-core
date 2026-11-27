//! #3581: a KV batch read can be pinned to one commit version or one
//! timestamp, so a set of keys hydrated for a version-pinned graph result
//! comes from that version — never a mix of it and the latest state.
//! `batch_get_at_version` and `batch_get_at` answer exactly what the per-key
//! `get_at_version` / `get_versioned_at` answer for each key, in request
//! order, with duplicates repeated and absent keys `None`, and they surface
//! the temporal-window diagnostics instead of falling back to the latest
//! state. These tests hold the batch to the per-key oracle across a history
//! of overwrites and deletes, in cache and durable modes, across a fork and
//! after a durable reopen.

mod common;

use strata_core::{CommitVersion, Timestamp};
use strata_engine::{BranchName, Database, KvKey, KvService, KvValue, KvVersionedValue};

use common::{branch, open_cache_database, open_durable_database, space};

fn run_database_modes(exercise: fn(&mut Database)) {
    let mut cache = open_cache_database().expect("cache open succeeds");
    exercise(&mut cache);

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open succeeds");
    exercise(&mut durable);
}

fn kv_service<'a>(database: &'a mut Database, branch_name: &str) -> KvService<'a> {
    database
        .kv(branch(branch_name), space("default"))
        .expect("KV service opens")
}

fn key(name: &str) -> KvKey {
    KvKey::new(name).expect("valid KV key")
}

fn value(bytes: &str) -> KvValue {
    KvValue::new(bytes)
}

/// The commit points of the two-version history the tests read: at `v1`
/// key A holds its first value and key B exists; at `v2` A was rewritten
/// and B removed.
struct History {
    v1: CommitVersion,
    t1: Timestamp,
    v2: CommitVersion,
    t2: Timestamp,
}

/// Writes the history: A, an empty-valued key, and B (v1 is B's commit),
/// then A rewritten and B deleted (v2 is the delete's commit).
fn seed(kv: &mut KvService<'_>) -> History {
    kv.put(key("a"), value("one")).expect("A written");
    kv.put(key("empty"), value(""))
        .expect("empty value written");
    let b = kv.put(key("b"), value("bee")).expect("B written").commit();
    kv.put(key("a"), value("two")).expect("A rewritten");
    let removed = kv
        .delete(key("b"))
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

/// The batch every test reads: A, B, a duplicate of A, a key that never
/// existed, and a key holding zero bytes. The last pins that an empty value
/// is a present entry, not a miss — the same answer the per-key read gives.
const KEYS: [&str; 5] = ["a", "b", "a", "missing", "empty"];

fn keys() -> Vec<KvKey> {
    KEYS.iter().map(|name| key(name)).collect()
}

fn bytes_of(answer: Option<&KvVersionedValue>) -> Option<&[u8]> {
    answer.map(|value| value.value().as_bytes())
}

/// What the batch must say at `v1`: A's first value twice, B present with
/// its own commit, the unknown key absent, and the empty value present.
fn assert_v1_answers(answers: &[Option<KvVersionedValue>], history: &History, context: &str) {
    assert_eq!(answers.len(), 5, "{context}: one answer per key");
    let a = answers[0]
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: A at v1"));
    assert_eq!(a.value().as_bytes(), b"one", "{context}");
    assert!(
        a.version() < history.v1,
        "{context}: A's own commit is before v1"
    );
    let b = answers[1]
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: B at v1"));
    assert_eq!(b.value().as_bytes(), b"bee", "{context}");
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
        bytes_of(answers[2].as_ref()),
        Some(&b"one"[..]),
        "{context}: a duplicate key is answered again"
    );
    assert!(answers[3].is_none(), "{context}: an unknown key is absent");
    assert_eq!(
        bytes_of(answers[4].as_ref()),
        Some(&b""[..]),
        "{context}: an empty value is present, not a miss"
    );
}

/// What the batch must say at `v2` and at the latest state: A's second
/// value, B gone, the unknown key absent, the empty value present.
fn assert_v2_answers(answers: &[Option<KvVersionedValue>], history: &History, context: &str) {
    assert_eq!(answers.len(), 5, "{context}: one answer per key");
    let a = answers[0]
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: A at v2"));
    assert_eq!(a.value().as_bytes(), b"two", "{context}");
    assert!(
        a.version() > history.v1 && a.version() < history.v2,
        "{context}: A's rewrite commit"
    );
    assert!(answers[1].is_none(), "{context}: B is gone at v2");
    assert_eq!(
        bytes_of(answers[2].as_ref()),
        Some(&b"two"[..]),
        "{context}"
    );
    assert!(answers[3].is_none(), "{context}");
    assert_eq!(bytes_of(answers[4].as_ref()), Some(&b""[..]), "{context}");
}

#[test]
fn batch_get_at_version_reads_every_key_from_the_pinned_version() {
    run_database_modes(exercise_pinned_version);
}

/// The issue's case: at v1 A has one value and B exists; at v2 A changed
/// and B is gone. Hydrating from v1 returns v1's A and B even though the
/// latest state differs — and the latest batch still says the latest.
fn exercise_pinned_version(database: &mut Database) {
    let mut kv = kv_service(database, "default");
    let history = seed(&mut kv);

    let at_v1 = kv
        .batch_get_at_version(&keys(), history.v1)
        .expect("batch at v1 succeeds");
    assert_v1_answers(&at_v1, &history, "at v1");

    let by_instant_1 = kv
        .batch_get_at(&keys(), history.t1)
        .expect("batch at t1 succeeds");
    assert_v1_answers(&by_instant_1, &history, "at t1");

    let at_v2 = kv
        .batch_get_at_version(&keys(), history.v2)
        .expect("batch at v2 succeeds");
    assert_v2_answers(&at_v2, &history, "at v2");

    let by_instant_2 = kv
        .batch_get_at(&keys(), history.t2)
        .expect("batch at t2 succeeds");
    assert_v2_answers(&by_instant_2, &history, "at t2");

    let latest = kv.batch_get(&keys()).expect("latest batch succeeds");
    assert_v2_answers(&latest, &history, "latest");
}

#[test]
fn batch_get_at_version_matches_the_per_key_reads_at_every_commit() {
    run_database_modes(exercise_oracle);
}

/// The batch is the per-key read, plural: at every commit of the history,
/// and at every commit's timestamp, each answer equals what
/// `get_at_version` / `get_versioned_at` say for that key on its own.
fn exercise_oracle(database: &mut Database) {
    let mut kv = kv_service(database, "default");
    let history = seed(&mut kv);
    let a_history = kv
        .get_versions(&key("a"))
        .expect("history reads")
        .expect("A has history");
    let commits: Vec<(CommitVersion, Timestamp)> = a_history
        .rows()
        .iter()
        .map(|row| (row.version(), row.timestamp()))
        .chain([(history.v1, history.t1), (history.v2, history.t2)])
        .collect();
    assert!(commits.len() >= 4, "the history spans several commits");

    for (version, timestamp) in commits {
        let by_version = kv
            .batch_get_at_version(&keys(), version)
            .expect("batch at version succeeds");
        let by_timestamp = kv
            .batch_get_at(&keys(), timestamp)
            .expect("batch at timestamp succeeds");
        for (index, name) in KEYS.iter().enumerate() {
            let single_value = kv
                .get_at_version(&key(name), version)
                .expect("single read at version succeeds");
            assert_eq!(
                by_version[index].as_ref().map(|v| v.value().clone()),
                single_value,
                "key {index} at version {version:?}"
            );
            let single_versioned = kv
                .get_versioned_at(&key(name), timestamp)
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
                "key {index} at timestamp {timestamp:?}"
            );
        }
    }
}

#[test]
fn batch_get_at_outside_the_retained_window_is_a_diagnostic_not_the_latest_state() {
    run_database_modes(exercise_window_diagnostics);
}

/// A timestamp before the retained history or after the latest commit, and
/// a version after the latest commit, fail the whole batch with the
/// temporal-window diagnostic. A batch that fell back to the latest state
/// would answer instead — the exact mix of versions the pinned read exists
/// to rule out.
fn exercise_window_diagnostics(database: &mut Database) {
    let mut kv = kv_service(database, "default");
    seed(&mut kv);

    for (instant, what) in [
        (Timestamp::EPOCH, "before the retained history"),
        (Timestamp::MAX, "after the latest commit"),
    ] {
        let error = kv
            .batch_get_at(&keys(), instant)
            .expect_err("an out-of-window batch is refused");
        assert_eq!(
            error.code(),
            "history_unavailable.engine.persistence_history",
            "{what}"
        );
        // The same class and code the single read raises for the instant:
        // the code is what tells a trimmed or future instant from a key
        // that simply is not there.
        let single = kv
            .get_versioned_at(&key("a"), instant)
            .expect_err("the single read is refused the same way");
        assert_eq!(error.class(), single.class(), "{what}");
        assert_eq!(error.code(), single.code(), "{what}");
    }

    let error = kv
        .batch_get_at_version(&keys(), CommitVersion::MAX)
        .expect_err("a version after the latest commit is refused");
    let single = kv
        .get_at_version(&key("a"), CommitVersion::MAX)
        .expect_err("the single read is refused the same way");
    assert_eq!(error.class(), single.class());
    assert_eq!(error.code(), single.code());
}

#[test]
fn batch_get_at_version_on_an_empty_batch_reads_nothing() {
    run_database_modes(exercise_empty_batch);
}

fn exercise_empty_batch(database: &mut Database) {
    let mut kv = kv_service(database, "default");
    let history = seed(&mut kv);
    assert!(kv
        .batch_get_at_version(&[], history.v1)
        .expect("empty batch succeeds")
        .is_empty());
    assert!(kv
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
        let mut kv = kv_service(database, "default");
        seed(&mut kv)
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
        let mut child = kv_service(database, "child");
        let at_v1 = child
            .batch_get_at_version(&keys(), history.v1)
            .expect("child batch at v1 succeeds");
        assert_v1_answers(&at_v1, &history, "child at v1");
        child
            .put(key("a"), value("three"))
            .expect("child write succeeds");
        let at_v1_after = child
            .batch_get_at_version(&keys(), history.v1)
            .expect("child batch at v1 succeeds after its write");
        assert_v1_answers(&at_v1_after, &history, "child at v1 after its write");
        let latest = child.batch_get(&keys()).expect("child latest succeeds");
        assert_eq!(
            bytes_of(latest[0].as_ref()),
            Some(&b"three"[..]),
            "the child's latest is its own write"
        );
    }

    let mut source = kv_service(database, "default");
    let at_v1 = source
        .batch_get_at_version(&keys(), history.v1)
        .expect("source batch at v1 succeeds");
    assert_v1_answers(&at_v1, &history, "source at v1");
    let latest = source.batch_get(&keys()).expect("source latest succeeds");
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
            let mut kv = kv_service(&mut database, "default");
            let history = seed(&mut kv);
            let at_v1 = kv
                .batch_get_at_version(&keys(), history.v1)
                .expect("batch at v1 succeeds");
            assert_v1_answers(&at_v1, &history, "before reopen");
            history
        };
        database.close().expect("close succeeds");
        history
    };

    let mut reopened = open_durable_database(tempdir.path()).expect("reopen succeeds");
    let mut kv = kv_service(&mut reopened, "default");
    let at_v1 = kv
        .batch_get_at_version(&keys(), history.v1)
        .expect("batch at v1 succeeds after reopen");
    assert_v1_answers(&at_v1, &history, "after reopen");
    let by_instant_1 = kv
        .batch_get_at(&keys(), history.t1)
        .expect("batch at t1 succeeds after reopen");
    assert_v1_answers(&by_instant_1, &history, "after reopen, by timestamp");
    let latest = kv.batch_get(&keys()).expect("latest succeeds after reopen");
    assert_v2_answers(&latest, &history, "latest after reopen");
}
