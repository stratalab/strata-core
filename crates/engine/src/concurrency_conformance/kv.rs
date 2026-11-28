//! KV operations on one space: a key that exists, a key that does not.

use serde_json::{json, Value};

use super::harness::{answer, Answer, Catalog, Op};
use crate::{BranchName, Database, KvKey, KvService, KvValue, ProductSpace};

fn kv(database: &Database) -> KvService<'_> {
    database
        .kv(
            BranchName::new("default").expect("valid branch"),
            ProductSpace::new("default").expect("valid space"),
        )
        .expect("KV service opens")
}

fn key(name: &str) -> KvKey {
    KvKey::new(name).expect("valid key")
}

fn seed(database: &mut Database) {
    kv(database)
        .put_batch([
            (key("k"), KvValue::new("v0")),
            (key("j"), KvValue::new("j0")),
        ])
        .expect("seed k and j");
}

fn put(database: &Database, name: &str, value: &str) -> Answer {
    answer(
        kv(database).put(key(name), KvValue::new(value)),
        |outcome| json!({ "created": outcome.created() }),
    )
}

fn put_k1(database: &Database) -> Answer {
    put(database, "k", "v1")
}

fn put_k2(database: &Database) -> Answer {
    put(database, "k", "v2")
}

fn put_n(database: &Database) -> Answer {
    put(database, "n", "n1")
}

fn delete_k(database: &Database) -> Answer {
    answer(
        kv(database).delete(key("k")),
        |outcome| json!({ "deleted": outcome.deleted() }),
    )
}

fn batch_put_k_n(database: &Database) -> Answer {
    answer(
        kv(database).put_batch([
            (key("k"), KvValue::new("v3")),
            (key("n"), KvValue::new("n3")),
        ]),
        |outcome| json!({ "created": outcome.created() }),
    )
}

fn batch_delete_k_n(database: &Database) -> Answer {
    answer(
        kv(database).delete_batch([key("k"), key("n")]),
        |outcome| json!({ "deleted": outcome.deleted() }),
    )
}

fn digest(database: &mut Database) -> Answer {
    let mut service = kv(database);
    let keys = service.list(None).expect("keys list");
    let entries = keys
        .iter()
        .map(|key| {
            let value = service
                .get(key)
                .expect("key reads")
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
            (
                String::from_utf8_lossy(key.as_bytes()).into_owned(),
                json!(value),
            )
        })
        .collect::<serde_json::Map<String, Value>>();
    Value::Object(entries)
}

fn batch_exists_k_j(database: &Database) -> Answer {
    answer(kv(database).batch_exists(&[key("k"), key("j")]), |exists| {
        json!(exists)
    })
}

fn batch_delete_k_j(database: &Database) -> Answer {
    answer(
        kv(database).delete_batch([key("k"), key("j")]),
        |outcome| json!({ "deleted": outcome.deleted() }),
    )
}

pub(super) const CATALOG: Catalog = Catalog {
    family: "kv",
    seed,
    ops: &[
        Op {
            label: "kv.put(k=v1)",
            command: "kv.put",
            run: put_k1,
        },
        Op {
            label: "kv.put(k=v2)",
            command: "kv.put",
            run: put_k2,
        },
        Op {
            label: "kv.put(n=n1)",
            command: "kv.put",
            run: put_n,
        },
        Op {
            label: "kv.delete(k)",
            command: "kv.delete",
            run: delete_k,
        },
        Op {
            label: "kv.batch_put(k, n)",
            command: "kv.batch_put",
            run: batch_put_k_n,
        },
        Op {
            label: "kv.batch_delete(k, n)",
            command: "kv.batch_delete",
            run: batch_delete_k_n,
        },
        Op {
            label: "read kv.batch_exists(k, j)",
            command: "kv.batch_exists",
            run: batch_exists_k_j,
        },
        Op {
            label: "kv.batch_delete(k, j)",
            command: "kv.batch_delete",
            run: batch_delete_k_j,
        },
    ],
    digest,
};
