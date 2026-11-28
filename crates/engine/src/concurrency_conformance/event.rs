//! Event operations on one log that already holds one event.

use serde_json::{json, Value};

use super::harness::{answer, Answer, Catalog, Op};
use crate::{
    BranchName, Database, EventBatchAppendEntry, EventPayload, EventService, EventType,
    ProductSpace,
};

fn events(database: &Database) -> EventService<'_> {
    database
        .event(
            BranchName::new("default").expect("valid branch"),
            ProductSpace::new("default").expect("valid space"),
        )
        .expect("event service opens")
}

fn append_event(database: &Database, event_type: &str, label: &str) -> Answer {
    answer(
        events(database).append(
            EventType::new(event_type).expect("valid event type"),
            EventPayload::new(json!({ "label": label })).expect("valid payload"),
        ),
        |outcome| json!({ "sequence": outcome.sequence().as_u64() }),
    )
}

fn seed(database: &mut Database) {
    append_event(database, "seed", "seed");
}

fn append_x(database: &Database) -> Answer {
    append_event(database, "x", "x")
}

fn append_y(database: &Database) -> Answer {
    append_event(database, "y", "y")
}

fn batch_append(database: &Database) -> Answer {
    answer(
        events(database).batch_append([
            EventBatchAppendEntry::new(
                EventType::new("x").expect("valid event type"),
                EventPayload::new(json!({ "label": "bx" })).expect("valid payload"),
            ),
            EventBatchAppendEntry::new(
                EventType::new("z").expect("valid event type"),
                EventPayload::new(json!({ "label": "bz" })).expect("valid payload"),
            ),
        ]),
        |outcome| {
            json!(outcome
                .items()
                .iter()
                .map(|item| json!(item.sequence().map(crate::EventSequence::as_u64)))
                .collect::<Vec<_>>())
        },
    )
}

fn digest(database: &mut Database) -> Answer {
    let mut service = events(database);
    let log = service
        .list(None, None, None)
        .expect("the log lists")
        .iter()
        .map(|record| {
            json!([
                record.sequence().as_u64(),
                record.event_type().as_str(),
                record.payload().as_inner()["label"],
            ])
        })
        .collect::<Vec<Value>>();
    let types = service
        .list_types()
        .expect("types list")
        .event_types()
        .iter()
        .map(|event_type| event_type.as_str().to_owned())
        .collect::<Vec<_>>();
    json!({
        "log": log,
        "len": service.len().expect("length reads").count(),
        "chain_valid": service.verify_chain().expect("chain verifies").is_valid(),
        "types": types,
    })
}

fn verify_chain(database: &Database) -> Answer {
    answer(
        events(database).verify_chain(),
        |verification| json!({ "valid": verification.is_valid() }),
    )
}

fn list_types(database: &Database) -> Answer {
    answer(events(database).list_types(), |types| {
        json!(types
            .event_types()
            .iter()
            .map(|event_type| event_type.as_str().to_owned())
            .collect::<Vec<_>>())
    })
}

pub(super) const CATALOG: Catalog = Catalog {
    family: "event",
    seed,
    ops: &[
        Op {
            label: "event.append(x)",
            command: "event.append",
            run: append_x,
        },
        Op {
            label: "event.append(y)",
            command: "event.append",
            run: append_y,
        },
        Op {
            label: "event.batch_append(x, z)",
            command: "event.batch_append",
            run: batch_append,
        },
        Op {
            label: "read event.verify_chain",
            command: "event.verify_chain",
            run: verify_chain,
        },
        Op {
            label: "read event.types",
            command: "event.types",
            run: list_types,
        },
    ],
    digest,
};
