//! JSON operations on one space: a document `d` with an indexed field, a
//! document `e` that does not exist yet, and one secondary index.

use serde_json::{json, Value};

use super::harness::{answer, Answer, Catalog, Op};
use crate::{
    BranchName, Database, JsonDocumentId, JsonIndexName, JsonIndexType, JsonPath, JsonService,
    JsonSetEntry, JsonValue, ProductSpace,
};

fn branch() -> BranchName {
    BranchName::new("default").expect("valid branch")
}

fn space() -> ProductSpace {
    ProductSpace::new("default").expect("valid space")
}

fn json_service(database: &Database) -> JsonService<'_> {
    database
        .json(branch(), space())
        .expect("JSON service opens")
}

fn doc(name: &str) -> JsonDocumentId {
    JsonDocumentId::new(name).expect("valid document id")
}

fn path(text: &str) -> JsonPath {
    text.parse().expect("valid JSON path")
}

fn value(value: Value) -> JsonValue {
    JsonValue::new(value).expect("valid JSON value")
}

fn index(name: &str) -> JsonIndexName {
    JsonIndexName::new(name).expect("valid index name")
}

fn seed(database: &mut Database) {
    let mut service = json_service(database);
    service
        .create_index(index("by_a"), path("$.a"), JsonIndexType::Numeric)
        .expect("seed index");
    service
        .create(doc("d"), value(json!({ "a": 1, "b": { "c": 2 } })))
        .expect("seed d");
}

fn write(result: Result<crate::JsonWriteOutcome, crate::EngineError>) -> Answer {
    answer(
        result,
        |outcome| json!({ "created": outcome.created(), "document_version": outcome.document_version() }),
    )
}

fn set_d_x(database: &Database) -> Answer {
    write(json_service(database).set(doc("d"), &path("$.x"), value(json!(1))))
}

fn set_d_y(database: &Database) -> Answer {
    write(json_service(database).set(doc("d"), &path("$.y"), value(json!(2))))
}

fn set_d_a(database: &Database) -> Answer {
    write(json_service(database).set(doc("d"), &path("$.a"), value(json!(5))))
}

fn create_e(database: &Database) -> Answer {
    write(json_service(database).create(doc("e"), value(json!({ "a": 7 }))))
}

fn set_or_create_e(database: &Database) -> Answer {
    write(json_service(database).set_or_create(
        doc("e"),
        &JsonPath::root(),
        value(json!({ "a": 8 })),
    ))
}

fn delete_d_b(database: &Database) -> Answer {
    answer(
        json_service(database).delete(doc("d"), &path("$.b")),
        |outcome| json!({ "deleted": outcome.deleted() }),
    )
}

fn delete_d(database: &Database) -> Answer {
    answer(
        json_service(database).delete(doc("d"), &JsonPath::root()),
        |outcome| json!({ "deleted": outcome.deleted() }),
    )
}

fn batch_set_d_e(database: &Database) -> Answer {
    answer(
        json_service(database).batch_set_or_create([
            JsonSetEntry::new(doc("d"), path("$.z"), value(json!(3))),
            JsonSetEntry::new(doc("e"), JsonPath::root(), value(json!({ "a": 9 }))),
        ]),
        |outcome| {
            json!(outcome
                .results()
                .iter()
                .map(|item| json!({ "created": item.created(), "document_version": item.document_version() }))
                .collect::<Vec<_>>())
        },
    )
}

fn batch_delete_d_e(database: &Database) -> Answer {
    answer(
        json_service(database).batch_delete([doc("d"), doc("e")]),
        |outcome| json!({ "deleted": outcome.deleted() }),
    )
}

fn create_index_by_x(database: &Database) -> Answer {
    answer(
        json_service(database).create_index(index("by_x"), path("$.x"), JsonIndexType::Numeric),
        |_| json!("created"),
    )
}

fn drop_index_by_a(database: &Database) -> Answer {
    answer(
        json_service(database).drop_index(&index("by_a")),
        |dropped| json!({ "dropped": dropped }),
    )
}

fn digest(database: &mut Database) -> Answer {
    let mut service = json_service(database);
    let documents = ["d", "e"]
        .iter()
        .map(|name| {
            let stored = service
                .get_versioned(&doc(name), &JsonPath::root())
                .expect("document reads")
                .map(|stored| {
                    json!({ "value": stored.value().as_inner(), "document_version": stored.document_version() })
                });
            ((*name).to_owned(), json!(stored))
        })
        .collect::<serde_json::Map<String, Value>>();
    let indexes = service
        .list_indexes()
        .expect("indexes list")
        .iter()
        .map(|definition| definition.name().as_str().to_owned())
        .collect::<Vec<_>>();
    drop(service);
    let entries = ["by_a", "by_x"]
        .iter()
        .map(|name| {
            let count = answer(
                database.json_index_entry_count_for_test(&branch(), &space(), &index(name)),
                |count| json!(count),
            );
            ((*name).to_owned(), count)
        })
        .collect::<serde_json::Map<String, Value>>();
    json!({ "documents": documents, "indexes": indexes, "index_entries": entries })
}

fn batch_exists_d_e(database: &Database) -> Answer {
    answer(
        json_service(database).batch_exists(&[doc("d"), doc("e")]),
        |exists| json!(exists),
    )
}

pub(super) const CATALOG: Catalog = Catalog {
    family: "json",
    seed,
    ops: &[
        Op {
            label: "json.set(d, $.x)",
            command: "json.set",
            run: set_d_x,
        },
        Op {
            label: "json.set(d, $.y)",
            command: "json.set",
            run: set_d_y,
        },
        Op {
            label: "json.set(d, $.a)",
            command: "json.set",
            run: set_d_a,
        },
        Op {
            label: "json.create(e)",
            command: "json.set",
            run: create_e,
        },
        Op {
            label: "json.set_or_create(e)",
            command: "json.set",
            run: set_or_create_e,
        },
        Op {
            label: "json.delete(d, $.b)",
            command: "json.delete",
            run: delete_d_b,
        },
        Op {
            label: "json.delete(d)",
            command: "json.delete",
            run: delete_d,
        },
        Op {
            label: "json.batch_set(d, e)",
            command: "json.batch_set",
            run: batch_set_d_e,
        },
        Op {
            label: "json.batch_delete(d, e)",
            command: "json.batch_delete",
            run: batch_delete_d_e,
        },
        Op {
            label: "json.index.create(by_x)",
            command: "json.index.create",
            run: create_index_by_x,
        },
        Op {
            label: "json.index.drop(by_a)",
            command: "json.index.drop",
            run: drop_index_by_a,
        },
        Op {
            label: "read json.batch_exists(d, e)",
            command: "json.batch_exists",
            run: batch_exists_d_e,
        },
    ],
    digest,
};
