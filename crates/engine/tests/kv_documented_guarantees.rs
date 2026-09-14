//! Every guarantee the KV commands publish, pinned (#3115).
//!
//! The `Command` rustdoc on each KV variant becomes the schema `description`,
//! and stratadb.org renders it. A guarantee published there is a promise a
//! caller writes code against — "no need to check for a partial batch", "this
//! listing is stable" — so an unpinned one is worse than none: it rots exactly
//! the way the docs in #3134 and #3147 rotted, and nothing fails when it does.
//!
//! Each test below names the sentence it pins. Changing engine behavior should
//! break one of these, and the fix is to change the published sentence with it.

mod common;

use strata_engine::{EngineErrorClass, KvKey, KvValue};

use common::{branch, key, open_cache_database, space, value};

/// `kv.batch_put`: "**Atomic across the batch.** Every entry lands in one engine
/// commit and shares its version."
#[test]
fn batch_put_is_one_commit_and_every_entry_shares_its_version() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    let outcome = kv
        .put_batch([
            (key(b"a"), value(b"1")),
            (key(b"b"), value(b"2")),
            (key(b"c"), value(b"3")),
        ])
        .expect("batch put succeeds");
    let batch_version = outcome.commit().version();

    // One commit for the batch, and every key reads back at exactly it.
    for name in [b"a", b"b", b"c"] {
        let versioned = kv
            .get_versioned(&key(name))
            .expect("read succeeds")
            .expect("entry is present");
        assert_eq!(
            versioned.version(),
            batch_version,
            "every batch entry shares the batch's commit version"
        );
    }
}

/// `kv.batch_put` / `kv.batch_delete`: "A duplicate key in one batch is refused
/// (`invalid_argument.engine.kv_batch_duplicate_key`); nothing is written."
#[test]
fn a_duplicate_key_refuses_the_batch_and_writes_nothing() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    let error = kv
        .put_batch([
            (key(b"dup"), value(b"first")),
            (key(b"other"), value(b"ok")),
            (key(b"dup"), value(b"second")),
        ])
        .expect_err("a duplicate key refuses the batch");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.kv_batch_duplicate_key"
    );

    // The refusal is total: the non-duplicate entry did not land either.
    assert!(
        kv.get(&key(b"other")).expect("read succeeds").is_none(),
        "a refused batch writes nothing at all"
    );
}

/// `kv.batch_put` / `kv.batch_delete`: "An empty batch is refused
/// (`invalid_argument.engine.kv_batch`)."
#[test]
fn an_empty_batch_is_refused() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    let put = kv
        .put_batch(Vec::<(KvKey, KvValue)>::new())
        .expect_err("an empty put batch is refused");
    assert_eq!(put.code(), "invalid_argument.engine.kv_batch");

    let delete = kv
        .delete_batch(Vec::<KvKey>::new())
        .expect_err("an empty delete batch is refused");
    assert_eq!(delete.code(), "invalid_argument.engine.kv_batch");
}

/// `kv.delete`: "Deleting a key that is not there succeeds with `deleted: false`
/// and makes **no commit** — the database is untouched and the version does
/// not move."
#[test]
fn deleting_a_missing_key_succeeds_without_committing() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    let seeded = kv
        .put(key(b"present"), value(b"v"))
        .expect("seed put succeeds")
        .commit()
        .version();

    let outcome = kv
        .delete(key(b"absent"))
        .expect("deleting a missing key succeeds");
    assert!(!outcome.deleted(), "nothing was there to delete");
    assert!(
        outcome.commit().is_none(),
        "a no-op delete makes no commit, so callers get no version back"
    );

    // The version really did not move: the seeded key still reads at its own.
    let versioned = kv
        .get_versioned(&key(b"present"))
        .expect("read succeeds")
        .expect("seeded key is present");
    assert_eq!(
        versioned.version(),
        seeded,
        "the no-op delete moved nothing"
    );
}

/// `kv.batch_delete`: "Keys that are not there are reported `false` and cost
/// nothing; the commit covers only the keys that existed."
#[test]
fn batch_delete_commits_only_the_keys_that_existed() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    kv.put_batch([(key(b"a"), value(b"1")), (key(b"c"), value(b"3"))])
        .expect("seed succeeds");

    let outcome = kv
        .delete_batch([key(b"a"), key(b"b"), key(b"c")])
        .expect("mixed delete batch succeeds");
    assert_eq!(
        outcome.deleted(),
        &[true, false, true],
        "per-key results follow the requested order"
    );
    assert!(
        outcome.commit().is_some(),
        "some key existed, so the batch committed"
    );
    assert!(kv.get(&key(b"a")).expect("read succeeds").is_none());
    assert!(kv.get(&key(b"c")).expect("read succeeds").is_none());

    // Every key missing => nothing to commit.
    let none = kv
        .delete_batch([key(b"x"), key(b"y")])
        .expect("all-missing delete batch succeeds");
    assert_eq!(none.deleted(), &[false, false]);
    assert!(
        none.commit().is_none(),
        "with nothing to delete there is no commit"
    );
}

/// `kv.list`: "**Latest per page, not a snapshot.** Each page reads the branch as
/// it is at the moment that page is fetched, so a write landing between pages
/// can appear in a later one. For a listing that cannot shift under you, page
/// `kv list --as-of` at a fixed timestamp instead."
#[test]
fn list_pages_follow_the_latest_branch_while_as_of_pages_stay_fixed() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    // Keys sort as k1 < k2 < k4; k3 is inserted mid-pagination, so it belongs
    // to the second page's range and is exactly the row a snapshot would hide.
    let seeded = kv
        .put_batch([
            (key(b"k1"), value(b"1")),
            (key(b"k2"), value(b"2")),
            (key(b"k4"), value(b"4")),
        ])
        .expect("seed succeeds")
        .commit()
        .timestamp();

    let first = kv.list_page(None, None, 2).expect("first page");
    assert!(first.has_more(), "three keys do not fit in one page of two");
    let cursor = first
        .cursor()
        .expect("a further page means a cursor")
        .clone();

    kv.put(key(b"k3"), value(b"3"))
        .expect("a write lands between pages");

    let second = kv.list_page(None, Some(&cursor), 2).expect("second page");
    let names: Vec<&[u8]> = second.keys().iter().map(KvKey::as_bytes).collect();
    assert!(
        names.contains(&b"k3".as_slice()),
        "list pages read the latest branch, so the mid-pagination write appears: {names:?}"
    );

    // The same walk pinned to the seed timestamp cannot see it.
    let first_at = kv
        .list_at_page(None, None, 2, seeded)
        .expect("first as-of page");
    let cursor_at = first_at
        .cursor()
        .expect("a further page means a cursor")
        .clone();
    let second_at = kv
        .list_at_page(None, Some(&cursor_at), 2, seeded)
        .expect("second as-of page");
    let names_at: Vec<&[u8]> = second_at.keys().iter().map(KvKey::as_bytes).collect();
    assert!(
        !names_at.contains(&b"k3".as_slice()),
        "an as-of walk is fixed at its timestamp, so a later write cannot appear: {names_at:?}"
    );
}

/// `kv.put`: "A key is any non-empty byte string; a value is any byte string,
/// including empty. Neither has a length limit of its own — the durable row
/// format caps each at 4 GiB."
#[test]
fn keys_reject_only_emptiness_and_values_accept_it() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    let empty_key = KvKey::new(Vec::new()).expect_err("an empty key is refused");
    assert_eq!(empty_key.class(), EngineErrorClass::InvalidInput);
    assert_eq!(empty_key.code(), "invalid_argument.engine.kv_key");

    // An empty value is a real value, distinct from an absent key.
    kv.put(key(b"empty"), KvValue::new(Vec::new()))
        .expect("an empty value is accepted");
    let stored = kv
        .get(&key(b"empty"))
        .expect("read succeeds")
        .expect("an empty value is present, not absent");
    assert!(stored.as_bytes().is_empty());

    // Keys carry arbitrary bytes, including NUL and newline, and stay distinct.
    let awkward = KvKey::new(b"a\0b\nc\xff".to_vec()).expect("arbitrary bytes make a key");
    kv.put(awkward.clone(), value(b"v"))
        .expect("awkward key writes");
    assert_eq!(
        kv.get(&awkward)
            .expect("read succeeds")
            .expect("awkward key is present")
            .as_bytes(),
        b"v"
    );

    // A large key and value round-trip: the limit is the format's, not a
    // small engine-imposed one.
    //
    // It was `64 * 1024` here, which is the table format's cap on the ENCODED
    // internal key — and the encoding adds the branch id, the space name and
    // the version suffix on top of the user key, so a 64 KiB user key is over
    // it. The write was accepted anyway, and took the branch down at its next
    // rotation (#3396). 60 KiB leaves room for the framing and still makes the
    // point the comment is making.
    let big_key = KvKey::new(vec![b'k'; 60 * 1024]).expect("a 60 KiB key is accepted");
    kv.put(big_key.clone(), KvValue::new(vec![7u8; 1024 * 1024]))
        .expect("a 1 MiB value is accepted");
    assert_eq!(
        kv.get(&big_key)
            .expect("read succeeds")
            .expect("big entry is present")
            .as_bytes()
            .len(),
        1024 * 1024
    );

    // And the cap is a refusal, not a silent acceptance: a key that cannot be
    // encoded into a table entry is rejected at write time.
    let unbuildable = KvKey::new(vec![b'k'; 64 * 1024]).expect("the key type itself has no cap");
    let error = kv
        .put(unbuildable, value(b"v"))
        .expect_err("a key over the table cap is refused");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
}

/// `kv.batch_get` / `kv.batch_exists`: "**Positional.** Results come back one per
/// requested key, in the order asked, with a missing key reported as absent
/// rather than skipped. Unlike the write batches, repeats are allowed."
#[test]
fn read_batches_are_positional_and_allow_repeats() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    kv.put(key(b"here"), value(b"v")).expect("seed succeeds");

    let requested = [key(b"here"), key(b"gone"), key(b"here")];
    let got = kv.batch_get(&requested).expect("read batch succeeds");
    assert_eq!(got.len(), requested.len(), "one slot per requested key");
    assert!(got[0].is_some(), "present key reports its value");
    assert!(got[1].is_none(), "missing key is absent, not skipped");
    assert!(got[2].is_some(), "a repeated key is allowed on reads");

    let exists = kv.batch_exists(&requested).expect("exists batch succeeds");
    assert_eq!(exists, vec![true, false, true]);
}

/// `kv.count`: "**A walk, not a counter.** `count` visits every live key under
/// the prefix; there is no maintained total, so its cost grows with the number
/// of keys counted."  Pinned here for exactness, not for cost.
#[test]
fn count_is_exact_over_live_keys_only() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    kv.put_batch([
        (key(b"user:1"), value(b"a")),
        (key(b"user:2"), value(b"b")),
        (key(b"other"), value(b"c")),
    ])
    .expect("seed succeeds");

    assert_eq!(kv.count(None).expect("count succeeds"), 3);
    assert_eq!(
        kv.count(Some(&key(b"user:")))
            .expect("prefix count succeeds"),
        2,
        "a prefix counts only its own keys"
    );

    // A deleted key stops counting, even though its history remains.
    kv.delete(key(b"user:1")).expect("delete succeeds");
    assert_eq!(
        kv.count(Some(&key(b"user:"))).expect("count succeeds"),
        1,
        "tombstoned keys are not live keys"
    );
}

/// `kv.sample`: "**Deterministic, not random.** The same request over unchanged
/// data returns the same rows: they are taken at even intervals through the
/// keys in order. `total` is the exact live count, not an estimate."
#[test]
fn sample_is_deterministic_and_reports_an_exact_total() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    kv.put_batch(
        (0..10u8)
            .map(|index| (key(&[b'k', b'0' + index]), value(b"v")))
            .collect::<Vec<_>>(),
    )
    .expect("seed succeeds");

    let first = kv.sample(None, 3).expect("sample succeeds");
    let again = kv.sample(None, 3).expect("second sample succeeds");
    assert_eq!(first.total_count(), 10, "total is the exact live count");
    assert_eq!(
        first
            .rows()
            .iter()
            .map(|row| row.key().as_bytes().to_vec())
            .collect::<Vec<_>>(),
        again
            .rows()
            .iter()
            .map(|row| row.key().as_bytes().to_vec())
            .collect::<Vec<_>>(),
        "the same request over unchanged data samples the same rows"
    );

    // Asking for more than exists returns everything, not padding.
    let all = kv.sample(None, 50).expect("oversized sample succeeds");
    assert_eq!(all.rows().len(), 10);
}

/// `kv.history`: "Cost follows the key's own version count, not the size of the
/// database, and it reports every version including the deletes."
#[test]
fn history_covers_one_key_and_includes_its_delete() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    kv.put(key(b"tracked"), value(b"one")).expect("first write");
    kv.put(key(b"tracked"), value(b"two"))
        .expect("second write");
    kv.delete(key(b"tracked")).expect("delete succeeds");
    // A second key with its own history must not appear in the first's.
    kv.put(key(b"unrelated"), value(b"x")).expect("other write");

    let history = kv
        .get_versions(&key(b"tracked"))
        .expect("history succeeds")
        .expect("the key has history");
    assert_eq!(
        history.rows().len(),
        3,
        "two writes and the delete, and nothing from the other key"
    );

    // The key reads as absent now, but its history is intact.
    assert!(kv.get(&key(b"tracked")).expect("read succeeds").is_none());
}
