use crate::common::{create_db, create_executor, create_session, event_payload};
use strata_core::Value;
#[cfg(feature = "embed")]
use strata_engine::database::{SHADOW_JSON, SHADOW_KV};
#[cfg(feature = "embed")]
use strata_engine::system_space::SYSTEM_SPACE;
#[cfg(feature = "embed")]
use strata_executor::Error;
use strata_executor::{Command, Executor, Output, SearchQuery, Session};
#[cfg(feature = "embed")]
use strata_vector::VectorStore;

#[test]
fn primitive_writes_register_non_default_spaces() {
    let executor = create_executor();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: Some("kv-space".into()),
            key: "key".into(),
            value: Value::String("value".into()),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: Some("json-space".into()),
            key: "doc".into(),
            path: "$".into(),
            value: Value::object(
                [("text".to_string(), Value::String("hello".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    executor
        .execute(Command::EventAppend {
            branch: None,
            space: Some("event-space".into()),
            event_type: "message".into(),
            payload: event_payload("text", Value::String("hello".into())),
        })
        .unwrap();

    for space in ["kv-space", "json-space", "event-space"] {
        assert_eq!(
            executor
                .execute(Command::SpaceExists {
                    branch: None,
                    space: space.into(),
                })
                .unwrap(),
            Output::Bool(true)
        );
    }
}

#[test]
fn transactional_primitive_writes_register_non_default_spaces() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    session
        .execute(Command::KvPut {
            branch: None,
            space: Some("kv-txn-space".into()),
            key: "key".into(),
            value: Value::String("value".into()),
        })
        .unwrap();
    session
        .execute(Command::JsonSet {
            branch: None,
            space: Some("json-txn-space".into()),
            key: "doc".into(),
            path: "$".into(),
            value: Value::object(
                [("text".to_string(), Value::String("hello".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    session
        .execute(Command::EventAppend {
            branch: None,
            space: Some("event-txn-space".into()),
            event_type: "message".into(),
            payload: event_payload("text", Value::String("hello".into())),
        })
        .unwrap();

    session.execute(Command::TxnCommit).unwrap();

    for space in ["kv-txn-space", "json-txn-space", "event-txn-space"] {
        assert_eq!(
            session
                .execute(Command::SpaceExists {
                    branch: None,
                    space: space.into(),
                })
                .unwrap(),
            Output::Bool(true)
        );
    }
}

#[test]
fn failed_and_noop_writes_do_not_create_spaces() {
    let executor = create_executor();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: Some("ghost-kv-invalid".into()),
            key: "".into(),
            value: Value::String("value".into()),
        })
        .expect_err("invalid KV write should fail");
    assert_eq!(
        executor
            .execute(Command::SpaceExists {
                branch: None,
                space: "ghost-kv-invalid".into(),
            })
            .unwrap(),
        Output::Bool(false)
    );

    assert_eq!(
        executor
            .execute(Command::KvDelete {
                branch: None,
                space: Some("ghost-kv-delete".into()),
                key: "missing".into(),
            })
            .unwrap(),
        Output::DeleteResult {
            key: "missing".into(),
            deleted: false,
        }
    );
    assert_eq!(
        executor
            .execute(Command::SpaceExists {
                branch: None,
                space: "ghost-kv-delete".into(),
            })
            .unwrap(),
        Output::Bool(false)
    );

    assert_eq!(
        executor
            .execute(Command::KvBatchPut {
                branch: None,
                space: Some("ghost-kv-batch".into()),
                entries: vec![],
            })
            .unwrap(),
        Output::BatchResults(Vec::new())
    );
    assert_eq!(
        executor
            .execute(Command::SpaceExists {
                branch: None,
                space: "ghost-kv-batch".into(),
            })
            .unwrap(),
        Output::Bool(false)
    );

    executor
        .execute(Command::JsonSet {
            branch: None,
            space: Some("ghost-json-invalid".into()),
            key: "".into(),
            path: "$".into(),
            value: Value::String("value".into()),
        })
        .expect_err("invalid JSON write should fail");
    assert_eq!(
        executor
            .execute(Command::SpaceExists {
                branch: None,
                space: "ghost-json-invalid".into(),
            })
            .unwrap(),
        Output::Bool(false)
    );

    assert_eq!(
        executor
            .execute(Command::EventBatchAppend {
                branch: None,
                space: Some("ghost-event-batch".into()),
                entries: vec![],
            })
            .unwrap(),
        Output::BatchResults(Vec::new())
    );
    assert_eq!(
        executor
            .execute(Command::SpaceExists {
                branch: None,
                space: "ghost-event-batch".into(),
            })
            .unwrap(),
        Output::Bool(false)
    );
}

#[test]
fn transactional_failed_and_noop_writes_do_not_create_spaces() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    session
        .execute(Command::KvPut {
            branch: None,
            space: Some("txn-ghost-kv-invalid".into()),
            key: "".into(),
            value: Value::String("value".into()),
        })
        .expect_err("invalid transactional KV write should fail");
    assert_eq!(
        session
            .execute(Command::KvDelete {
                branch: None,
                space: Some("txn-ghost-kv-delete".into()),
                key: "missing".into(),
            })
            .unwrap(),
        Output::DeleteResult {
            key: "missing".into(),
            deleted: false,
        }
    );
    assert_eq!(
        session
            .execute(Command::KvBatchPut {
                branch: None,
                space: Some("txn-ghost-kv-batch".into()),
                entries: vec![],
            })
            .unwrap(),
        Output::BatchResults(Vec::new())
    );
    session
        .execute(Command::JsonSet {
            branch: None,
            space: Some("txn-ghost-json-invalid".into()),
            key: "".into(),
            path: "$".into(),
            value: Value::String("value".into()),
        })
        .expect_err("invalid transactional JSON write should fail");
    assert_eq!(
        session
            .execute(Command::EventBatchAppend {
                branch: None,
                space: Some("txn-ghost-event-batch".into()),
                entries: vec![],
            })
            .unwrap(),
        Output::BatchResults(Vec::new())
    );

    session.execute(Command::TxnCommit).unwrap();

    for space in [
        "txn-ghost-kv-invalid",
        "txn-ghost-kv-delete",
        "txn-ghost-kv-batch",
        "txn-ghost-json-invalid",
        "txn-ghost-event-batch",
    ] {
        assert_eq!(
            session
                .execute(Command::SpaceExists {
                    branch: None,
                    space: space.into(),
                })
                .unwrap(),
            Output::Bool(false),
            "space {space} should not be created by a failed or no-op transactional write",
        );
    }
}

fn keyword_search(executor: &Executor, query: &str) -> Vec<strata_executor::SearchResultHit> {
    match executor
        .execute(Command::Search {
            branch: None,
            space: None,
            search: SearchQuery {
                query: query.into(),
                recipe: Some(serde_json::Value::String("keyword".into())),
                precomputed_embedding: None,
                k: Some(10),
                as_of: None,
                diff: None,
            },
        })
        .unwrap()
    {
        Output::SearchResults { hits, .. } => hits,
        other => panic!("unexpected search output: {other:?}"),
    }
}

#[cfg(feature = "embed")]
fn shadow_keys(vector: &VectorStore, collection: &str) -> Vec<String> {
    let branch = strata_core::BranchId::from_bytes([0u8; 16]);
    vector
        .list_keys(branch, SYSTEM_SPACE, collection)
        .unwrap_or_default()
}

#[test]
fn committed_transaction_writes_update_search_index() {
    let db = create_db();
    let executor = Executor::new(db.clone());
    let mut session = Session::new(db);

    executor.execute(Command::RecipeSeed).unwrap();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn-kv".into(),
            value: Value::String("appletxn".into()),
        })
        .unwrap();
    session
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "txn-json".into(),
            path: "$".into(),
            value: Value::object(
                [("text".to_string(), Value::String("bananatxn".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    session
        .execute(Command::EventAppend {
            branch: None,
            space: None,
            event_type: "txn.event".into(),
            payload: event_payload("text", Value::String("peartxn".into())),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    let kv_hits = keyword_search(&executor, "appletxn");
    assert!(kv_hits
        .iter()
        .any(|hit| hit.entity_ref.kind == "kv" && hit.entity_ref.key.as_deref() == Some("txn-kv")));

    let json_hits = keyword_search(&executor, "bananatxn");
    assert!(json_hits.iter().any(|hit| {
        hit.entity_ref.kind == "json" && hit.entity_ref.doc_id.as_deref() == Some("txn-json")
    }));

    let event_hits = keyword_search(&executor, "peartxn");
    assert!(event_hits
        .iter()
        .any(|hit| hit.entity_ref.kind == "event" && hit.entity_ref.sequence.is_some()));

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: "txn-kv".into(),
        })
        .unwrap();
    session
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: "txn-json".into(),
            path: "$".into(),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    let kv_hits = keyword_search(&executor, "appletxn");
    assert!(!kv_hits
        .iter()
        .any(|hit| hit.entity_ref.kind == "kv" && hit.entity_ref.key.as_deref() == Some("txn-kv")));

    let json_hits = keyword_search(&executor, "bananatxn");
    assert!(!json_hits.iter().any(|hit| {
        hit.entity_ref.kind == "json" && hit.entity_ref.doc_id.as_deref() == Some("txn-json")
    }));
}

#[test]
fn kv_text_to_non_text_update_removes_search_hit() {
    let executor = create_executor();
    executor.execute(Command::RecipeSeed).unwrap();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "kv-text".into(),
            value: Value::String("phase10_kv_text".into()),
        })
        .unwrap();
    assert!(
        keyword_search(&executor, "phase10_kv_text")
            .iter()
            .any(|hit| hit.entity_ref.kind == "kv"
                && hit.entity_ref.key.as_deref() == Some("kv-text"))
    );

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "kv-text".into(),
            value: Value::Null,
        })
        .unwrap();
    assert!(
        !keyword_search(&executor, "phase10_kv_text")
            .iter()
            .any(|hit| hit.entity_ref.kind == "kv"
                && hit.entity_ref.key.as_deref() == Some("kv-text"))
    );
}

#[cfg(feature = "embed")]
#[test]
fn auto_embed_writes_update_local_status_and_flush_locally() {
    let executor = create_executor();

    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: true })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "kv-auto".into(),
            value: Value::String("hello from kv".into()),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "json-auto".into(),
            path: "$".into(),
            value: Value::object(
                [("text".to_string(), Value::String("hello from json".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    executor
        .execute(Command::EventAppend {
            branch: None,
            space: None,
            event_type: "message".into(),
            payload: event_payload("text", Value::String("hello from event".into())),
        })
        .unwrap();

    let status = executor.execute(Command::EmbedStatus).unwrap();
    match status {
        Output::EmbedStatus(info) => {
            assert!(info.auto_embed);
            assert!(info.total_queued >= 3);
            assert!(info.pending + (info.total_embedded + info.total_failed) as usize >= 3);
        }
        other => panic!("unexpected embed status: {other:?}"),
    }

    executor.execute(Command::Flush).unwrap();

    let status = executor.execute(Command::EmbedStatus).unwrap();
    match status {
        Output::EmbedStatus(info) => {
            assert!(info.auto_embed);
            assert_eq!(info.pending, 0);
            assert!(info.total_queued >= 3);
            assert!(info.total_embedded + info.total_failed >= 3);
        }
        other => panic!("unexpected embed status after flush: {other:?}"),
    }
}

#[cfg(feature = "embed")]
#[test]
fn kv_text_to_non_text_update_removes_shadow_embedding() {
    let db = create_db();
    let executor = Executor::new(db.clone());
    let vector = VectorStore::new(db);

    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: true })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "shadow-kv".into(),
            value: Value::String("shadow me".into()),
        })
        .unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(shadow_keys(&vector, SHADOW_KV)
        .iter()
        .any(|key| key == "default\x1fshadow-kv"));

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "shadow-kv".into(),
            value: Value::Null,
        })
        .unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(!shadow_keys(&vector, SHADOW_KV)
        .iter()
        .any(|key| key == "default\x1fshadow-kv"));
}

#[cfg(feature = "embed")]
#[test]
fn json_text_to_non_text_update_removes_shadow_embedding() {
    let db = create_db();
    let executor = Executor::new(db.clone());
    let vector = VectorStore::new(db);

    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: true })
        .unwrap();

    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "shadow-json".into(),
            path: "$".into(),
            value: Value::object(
                [("text".to_string(), Value::String("shadow json".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(shadow_keys(&vector, SHADOW_JSON)
        .iter()
        .any(|key| key == "default\x1fshadow-json"));

    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "shadow-json".into(),
            path: "$".into(),
            value: Value::Null,
        })
        .unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(!shadow_keys(&vector, SHADOW_JSON)
        .iter()
        .any(|key| key == "default\x1fshadow-json"));
}

#[cfg(feature = "embed")]
#[test]
fn auto_embed_delete_drains_pending_entries_without_legacy_compat() {
    let executor = create_executor();

    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: true })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "delete-me".into(),
            value: Value::String("queued for embedding".into()),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "delete-doc".into(),
            path: "$".into(),
            value: Value::object(
                [("text".to_string(), Value::String("queued json".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();

    executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: "delete-me".into(),
        })
        .unwrap();
    executor
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: "delete-doc".into(),
            path: "$".into(),
        })
        .unwrap();

    match executor.execute(Command::EmbedStatus).unwrap() {
        Output::EmbedStatus(info) => {
            assert!(info.auto_embed);
            assert_eq!(info.pending, 0);
            assert!(info.total_queued >= 2);
        }
        other => panic!("unexpected embed status: {other:?}"),
    }
}

#[cfg(feature = "embed")]
#[test]
fn disabling_auto_embed_still_cleans_up_existing_shadow_vectors() {
    let db = create_db();
    let executor = Executor::new(db.clone());
    let vector = VectorStore::new(db);

    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: true })
        .unwrap();
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "disable-cleanup".into(),
            value: Value::String("shadow cleanup".into()),
        })
        .unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(shadow_keys(&vector, SHADOW_KV)
        .iter()
        .any(|key| key == "default\x1fdisable-cleanup"));

    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: false })
        .unwrap();
    executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: "disable-cleanup".into(),
        })
        .unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(!shadow_keys(&vector, SHADOW_KV)
        .iter()
        .any(|key| key == "default\x1fdisable-cleanup"));
}

#[cfg(feature = "embed")]
#[test]
fn committed_transaction_kv_text_to_non_text_update_removes_search_hit_and_shadow_embedding() {
    let db = create_db();
    let executor = Executor::new(db.clone());
    let mut session = Session::new(db.clone());
    let vector = VectorStore::new(db);

    executor.execute(Command::RecipeSeed).unwrap();
    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: true })
        .unwrap();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn-shadow-kv".into(),
            value: Value::String("phase10_txn_shadow".into()),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(keyword_search(&executor, "phase10_txn_shadow")
        .iter()
        .any(|hit| hit.entity_ref.kind == "kv"
            && hit.entity_ref.key.as_deref() == Some("txn-shadow-kv")));
    assert!(shadow_keys(&vector, SHADOW_KV)
        .iter()
        .any(|key| key == "default\x1ftxn-shadow-kv"));

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn-shadow-kv".into(),
            value: Value::Null,
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();
    executor.execute(Command::Flush).unwrap();

    assert!(!keyword_search(&executor, "phase10_txn_shadow")
        .iter()
        .any(|hit| hit.entity_ref.kind == "kv"
            && hit.entity_ref.key.as_deref() == Some("txn-shadow-kv")));
    assert!(!shadow_keys(&vector, SHADOW_KV)
        .iter()
        .any(|key| key == "default\x1ftxn-shadow-kv"));
}

#[cfg(feature = "embed")]
#[test]
fn committed_transaction_writes_queue_and_remove_local_auto_embed_work() {
    let db = create_db();
    let executor = Executor::new(db.clone());
    let mut session = Session::new(db.clone());
    let vector = VectorStore::new(db);
    let branch = strata_core::BranchId::from_bytes([0u8; 16]);

    executor
        .execute(Command::ConfigSetAutoEmbed { enabled: true })
        .unwrap();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn-embed".into(),
            value: Value::String("embed after commit".into()),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    match executor.execute(Command::EmbedStatus).unwrap() {
        Output::EmbedStatus(info) => {
            assert!(info.auto_embed);
            assert_eq!(info.pending, 1);
            assert!(info.total_queued >= 1);
        }
        other => panic!("unexpected embed status: {other:?}"),
    }

    let shadow_keys = vector
        .list_keys(branch, SYSTEM_SPACE, SHADOW_KV)
        .unwrap_or_default();
    assert!(
        !shadow_keys.iter().any(|key| key == "default\x1ftxn-embed"),
        "embedding should be queued before flush, not inserted eagerly"
    );

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: "txn-embed".into(),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    match executor.execute(Command::EmbedStatus).unwrap() {
        Output::EmbedStatus(info) => {
            assert!(info.auto_embed);
            assert_eq!(info.pending, 0);
        }
        other => panic!("unexpected embed status after delete: {other:?}"),
    }
}

#[cfg(feature = "embed")]
#[test]
fn reindex_embeddings_reports_model_state_locally() {
    let executor = create_executor();

    let error = executor
        .execute(Command::ReindexEmbeddings { branch: None })
        .expect_err("reindex should require a loaded embedding model");

    match error {
        Error::Internal { reason, .. } => {
            assert!(
                reason.contains("Embedding model not loaded")
                    || reason.contains("Failed to get embed model state")
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
