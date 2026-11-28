//! First writes to new product spaces, across capabilities: each registers
//! its space in the branch's one space index.

use serde_json::json;

use super::harness::{answer, Answer, Catalog, Op};
use crate::{
    BranchName, Database, EventPayload, EventType, GraphName, JsonDocumentId, JsonPath, JsonValue,
    KvKey, KvValue, ProductSpace,
};

fn branch() -> BranchName {
    BranchName::new("default").expect("valid branch")
}

fn space(name: &str) -> ProductSpace {
    ProductSpace::new(name).expect("valid space")
}

fn seed(_database: &mut Database) {}

fn kv_first_write(database: &Database) -> Answer {
    answer(
        database
            .kv(branch(), space("s-kv"))
            .and_then(|mut kv| kv.put(KvKey::new("k").expect("valid key"), KvValue::new("v"))),
        |_| json!("written"),
    )
}

fn json_first_write(database: &Database) -> Answer {
    answer(
        database
            .json(branch(), space("s-json"))
            .and_then(|mut json| {
                json.set_or_create(
                    JsonDocumentId::new("d").expect("valid id"),
                    &JsonPath::root(),
                    JsonValue::new(json!({ "a": 1 })).expect("valid value"),
                )
            }),
        |_| json!("written"),
    )
}

fn event_first_write(database: &Database) -> Answer {
    answer(
        database
            .event(branch(), space("s-event"))
            .and_then(|mut events| {
                events.append(
                    EventType::new("e").expect("valid event type"),
                    EventPayload::new(json!({})).expect("valid payload"),
                )
            }),
        |_| json!("written"),
    )
}

fn graph_first_write(database: &Database) -> Answer {
    answer(
        database
            .graph(branch(), space("s-graph"))
            .and_then(|mut graph| graph.create_graph(GraphName::new("g").expect("valid graph"))),
        |_| json!("written"),
    )
}

fn kv_second_space(database: &Database) -> Answer {
    answer(
        database
            .kv(branch(), space("s-kv2"))
            .and_then(|mut kv| kv.put(KvKey::new("k").expect("valid key"), KvValue::new("v"))),
        |_| json!("written"),
    )
}

fn digest(database: &mut Database) -> Answer {
    let mut spaces = database
        .spaces(branch())
        .expect("space service opens")
        .list()
        .expect("spaces list")
        .iter()
        .map(|space| space.as_str().to_owned())
        .collect::<Vec<_>>();
    spaces.sort();
    json!({ "listed": spaces })
}

pub(super) const CATALOG: Catalog = Catalog {
    family: "space registration",
    seed,
    ops: &[
        Op {
            label: "kv.put(new space s-kv)",
            command: "kv.put",
            run: kv_first_write,
        },
        Op {
            label: "kv.put(new space s-kv2)",
            command: "kv.put",
            run: kv_second_space,
        },
        Op {
            label: "json.set(new space s-json)",
            command: "json.set",
            run: json_first_write,
        },
        Op {
            label: "event.append(new space s-event)",
            command: "event.append",
            run: event_first_write,
        },
        Op {
            label: "graph.create(new space s-graph)",
            command: "graph.create",
            run: graph_first_write,
        },
    ],
    digest,
};
