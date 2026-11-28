//! A batch write drains the caller's iterator before it takes its branch's
//! writer (#3782, ACID-008).
//!
//! The iterator is the caller's code, and it may write the same branch: an
//! iterator that appends an event per entry is ordinary use of a `&self`
//! service. Drained under the writer, that append would take the branch's
//! writer a second time on the same thread. Each test here hands a batch
//! write such an iterator and holds both writes to landing.

use serde_json::json;

use crate::{
    BranchName, CacheOpenOptions, Database, EventPayload, EventType, JsonDocumentId, JsonPath,
    JsonSetEntry, JsonValue, KvKey, KvValue, ProductSpace,
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

fn key(name: &str) -> KvKey {
    KvKey::new(name).expect("valid key")
}

fn doc(name: &str) -> JsonDocumentId {
    JsonDocumentId::new(name).expect("valid document id")
}

/// Appends an event to the same branch, as a caller's iterator might.
fn note(database: &Database, label: &str) {
    database
        .event(branch(), space())
        .expect("event service opens")
        .append(
            EventType::new("note").expect("valid event type"),
            EventPayload::new(json!({ "label": label })).expect("valid payload"),
        )
        .expect("the iterator's append is acknowledged");
}

fn events(database: &Database) -> u64 {
    database
        .event(branch(), space())
        .expect("event service opens")
        .len()
        .expect("the length reads")
        .count()
}

#[test]
fn test_a_kv_batch_put_whose_iterator_writes_the_branch_lands_both_writes() {
    let database = open_cache_database();
    let outcome = database
        .kv(branch(), space())
        .expect("KV service opens")
        .put_batch(["a", "b"].into_iter().map(|name| {
            note(&database, name);
            (key(name), KvValue::new(name))
        }))
        .expect("the batch is acknowledged");
    assert_eq!(outcome.created(), [true, true]);
    assert_eq!(events(&database), 2, "the iterator's appends landed");
}

#[test]
fn test_a_kv_batch_delete_whose_iterator_writes_the_branch_lands_both_writes() {
    let database = open_cache_database();
    let mut kv = database.kv(branch(), space()).expect("KV service opens");
    kv.put(key("a"), KvValue::new("a")).expect("seed a");
    let outcome = kv
        .delete_batch(["a", "b"].into_iter().map(|name| {
            note(&database, name);
            key(name)
        }))
        .expect("the batch is acknowledged");
    assert_eq!(outcome.deleted(), [true, false]);
    assert_eq!(events(&database), 2, "the iterator's appends landed");
}

#[test]
fn test_a_json_batch_set_whose_iterator_writes_the_branch_lands_both_writes() {
    let database = open_cache_database();
    let outcome = database
        .json(branch(), space())
        .expect("JSON service opens")
        .batch_set_or_create(["d", "e"].into_iter().map(|name| {
            note(&database, name);
            JsonSetEntry::new(
                doc(name),
                JsonPath::root(),
                JsonValue::new(json!({ "name": name })).expect("valid JSON value"),
            )
        }))
        .expect("the batch is acknowledged");
    assert_eq!(outcome.results().len(), 2);
    assert!(outcome.results().iter().all(|item| item.created()));
    assert_eq!(events(&database), 2, "the iterator's appends landed");
}

#[test]
fn test_a_json_batch_delete_whose_iterator_writes_the_branch_lands_both_writes() {
    let database = open_cache_database();
    let mut json_service = database
        .json(branch(), space())
        .expect("JSON service opens");
    json_service
        .create(
            doc("d"),
            JsonValue::new(json!({ "name": "d" })).expect("valid JSON value"),
        )
        .expect("seed d");
    let outcome = json_service
        .batch_delete(["d", "e"].into_iter().map(|name| {
            note(&database, name);
            doc(name)
        }))
        .expect("the batch is acknowledged");
    assert_eq!(outcome.deleted(), [true, false]);
    assert_eq!(events(&database), 2, "the iterator's appends landed");
}
