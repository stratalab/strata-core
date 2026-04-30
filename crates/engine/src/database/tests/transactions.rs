use super::*;
use crate::primitives::json::{JsonDoc, JsonStore};
use crate::transaction_ops::TransactionOps;
use crate::{JsonPath, JsonValue};
use std::sync::Barrier;
use std::thread;

#[test]
fn test_transaction_closure_api() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "test_key");

    // Execute transaction
    let result = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    });

    assert!(result.is_ok());

    // Verify data was committed
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(42));
}

#[test]
fn test_transaction_returns_closure_value() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "test_key");

    // Pre-populate using transaction
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(100))?;
        Ok(())
    })
    .unwrap();

    // Transaction returns a value
    let result: StrataResult<i64> = db.transaction(branch_id, |txn| {
        let val = txn.get(&key)?.unwrap();
        if let Value::Int(n) = val {
            Ok(n)
        } else {
            Err(StrataError::invalid_input("wrong type".to_string()))
        }
    });

    assert_eq!(result.unwrap(), 100);
}

#[test]
fn test_transaction_read_your_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "ryw_key");

    // Per spec Section 2.1: "Its own uncommitted writes - always visible"
    let result: StrataResult<Value> = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("written".to_string()))?;

        // Should see our own write
        let val = txn.get(&key)?.unwrap();
        Ok(val)
    });

    assert_eq!(result.unwrap(), Value::String("written".to_string()));
}

#[test]
fn test_transaction_aborts_on_closure_error() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "abort_key");

    // Transaction that errors
    let result: StrataResult<()> = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(999))?;
        Err(StrataError::invalid_input("intentional error".to_string()))
    });

    assert!(result.is_err());

    // Data should NOT be committed
    assert!(db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn test_begin_and_commit_manual() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "manual_key");

    // Manual transaction control
    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.put(key.clone(), Value::Int(123)).unwrap();

    // Commit manually
    db.commit_transaction(&mut txn).unwrap();

    // Verify committed
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(123));
}

#[test]
fn test_owned_transaction_commit_emits_one_commit_observer_event() {
    struct CountingCommitObserver {
        count: AtomicUsize,
    }

    impl super::CommitObserver for CountingCommitObserver {
        fn name(&self) -> &'static str {
            "counting-commit"
        }

        fn on_commit(&self, _info: &super::CommitInfo) -> Result<(), super::ObserverError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let observer = Arc::new(CountingCommitObserver {
        count: AtomicUsize::new(0),
    });
    db.commit_observers().register(observer.clone());

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "observer_key");

    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.put(key.clone(), Value::Int(1)).unwrap();
    let version = txn.commit().unwrap();

    assert!(version > 0);
    assert_eq!(
        observer.count.load(Ordering::SeqCst),
        1,
        "successful manual commits should emit exactly one observer event"
    );
}

#[test]
fn test_storage_publication_health_blocks_new_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    db.storage()
        .latch_publish_health("manifest rename may not be durable");

    let err = match db.begin_transaction(BranchId::new()) {
        Ok(_) => panic!("begin_transaction should fail when storage publish health is latched"),
        Err(err) => err,
    };
    assert!(matches!(err, StrataError::Storage { .. }));

    let report = db.health();
    let storage = report
        .subsystems
        .iter()
        .find(|subsystem| subsystem.name == "storage")
        .expect("storage subsystem present");
    assert_eq!(storage.status, SubsystemStatus::Unhealthy);
    assert!(storage
        .message
        .as_ref()
        .is_some_and(|message| message.contains("publication durability degraded")));
}

#[test]
fn test_manual_transaction_json_state_is_live_across_scoped_wrappers() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let mut txn = db.begin_transaction(branch_id).unwrap();
    {
        let mut json = txn.scoped(ns.clone());
        json.json_set(
            "doc1",
            &"profile.name".parse().unwrap(),
            JsonValue::from("Ada"),
        )
        .unwrap();
    }
    {
        let mut json = txn.scoped(ns.clone());
        let doc = json
            .json_get("doc1")
            .unwrap()
            .expect("document should exist");
        assert_eq!(
            doc.value,
            JsonValue::from_value(serde_json::json!({
                "profile": { "name": "Ada" }
            }))
        );
    }
    txn.commit().unwrap();

    let key = Key::new_json(ns.clone(), "doc1");
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .expect("json document should be committed");
    let doc = JsonStore::deserialize_doc_with_fallback_id(&stored.value, "doc1").unwrap();
    assert_eq!(
        doc.value,
        JsonValue::from_value(serde_json::json!({
            "profile": { "name": "Ada" }
        }))
    );
}

#[test]
fn test_manual_transaction_json_read_only_commit_ignores_later_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_json(ns.clone(), "doc1");

    let existing = JsonStore::serialize_doc(&JsonDoc::new(
        "doc1",
        JsonValue::from_value(serde_json::json!({ "value": "before" })),
    ))
    .unwrap();
    blind_write(&db, key.clone(), existing);

    let mut txn = db.begin_transaction(branch_id).unwrap();
    {
        let mut json = txn.scoped(ns.clone());
        let doc = json
            .json_get("doc1")
            .unwrap()
            .expect("document should exist");
        assert_eq!(
            doc.value,
            JsonValue::from_value(serde_json::json!({ "value": "before" }))
        );
    }

    let updated = JsonStore::serialize_doc(&JsonDoc::new(
        "doc1",
        JsonValue::from_value(serde_json::json!({ "value": "after" })),
    ))
    .unwrap();
    blind_write(&db, key, updated);

    txn.commit()
        .expect("read-only JSON transactions should not validate like writes");
}

#[test]
fn test_read_only_transaction_rejects_json_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_json(ns.clone(), "doc1");

    let mut txn = db.begin_read_only_transaction(branch_id).unwrap();
    let err = {
        let mut json = txn.scoped(ns.clone());
        json.json_set(
            "doc1",
            &JsonPath::root(),
            JsonValue::from_value(serde_json::json!({ "value": 1 })),
        )
        .expect_err("read-only transactions must reject JSON writes")
    };
    assert!(matches!(err, StrataError::InvalidInput { .. }));

    txn.commit()
        .expect("read-only transaction should still commit after rejected JSON write");
    assert!(db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn test_manual_transaction_json_get_sees_generic_raw_write_for_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_json(ns.clone(), "doc1");

    let mut txn = db.begin_transaction(branch_id).unwrap();
    let raw = JsonStore::serialize_doc(&JsonDoc::new(
        "doc1",
        JsonValue::from_value(serde_json::json!({
            "profile": { "name": "Ada" }
        })),
    ))
    .unwrap();
    txn.put(key, raw).unwrap();

    let mut json = txn.scoped(ns.clone());
    let doc = json
        .json_get("doc1")
        .unwrap()
        .expect("raw write should be visible through JSON reads");
    assert_eq!(
        doc.value,
        JsonValue::from_value(serde_json::json!({
            "profile": { "name": "Ada" }
        }))
    );
}

#[test]
fn test_manual_transaction_json_patch_uses_generic_raw_write_as_base() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let key = Key::new_json(ns.clone(), "doc1");
    let raw = JsonStore::serialize_doc(&JsonDoc::new(
        "doc1",
        JsonValue::from_value(serde_json::json!({
            "profile": { "name": "Ada" }
        })),
    ))
    .unwrap();

    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.put(key.clone(), raw).unwrap();
    {
        let mut json = txn.scoped(ns.clone());
        json.json_set(
            "doc1",
            &"profile.age".parse().unwrap(),
            JsonValue::from(30i64),
        )
        .unwrap();

        let doc = json
            .json_get("doc1")
            .unwrap()
            .expect("patched document should exist");
        assert_eq!(
            doc.value,
            JsonValue::from_value(serde_json::json!({
                "profile": { "name": "Ada", "age": 30 }
            }))
        );
    }
    txn.commit().unwrap();

    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .expect("patched document should commit");
    let doc = JsonStore::deserialize_doc_with_fallback_id(&stored.value, "doc1").unwrap();
    assert_eq!(
        doc.value,
        JsonValue::from_value(serde_json::json!({
            "profile": { "name": "Ada", "age": 30 }
        }))
    );
}

#[test]
fn test_manual_transaction_json_exists_missing_doc_conflicts_with_concurrent_create() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let mut txn1 = db.begin_transaction(branch_id).unwrap();
    {
        let mut json = txn1.scoped(ns.clone());
        assert!(!json.json_exists("unique_doc").unwrap());
        json.json_set(
            "unique_doc",
            &JsonPath::root(),
            JsonValue::from_value(serde_json::json!({ "from": "txn1" })),
        )
        .unwrap();
    }

    let mut txn2 = db.begin_transaction(branch_id).unwrap();
    {
        let mut json = txn2.scoped(ns.clone());
        assert!(!json.json_exists("unique_doc").unwrap());
        json.json_set(
            "unique_doc",
            &JsonPath::root(),
            JsonValue::from_value(serde_json::json!({ "from": "txn2" })),
        )
        .unwrap();
    }

    txn1.commit().unwrap();
    let err = txn2.commit().expect_err(
        "second create-if-absent transaction should conflict after the document is created",
    );
    assert!(
        matches!(err, StrataError::VersionConflict { .. }),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_manual_transaction_json_set_missing_doc_conflicts_with_concurrent_create() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let mut txn1 = db.begin_transaction(branch_id).unwrap();
    {
        let mut json = txn1.scoped(ns.clone());
        json.json_set(
            "set_race_doc",
            &JsonPath::root(),
            JsonValue::from_value(serde_json::json!({ "from": "txn1" })),
        )
        .unwrap();
    }

    let mut txn2 = db.begin_transaction(branch_id).unwrap();
    {
        let mut json = txn2.scoped(ns.clone());
        json.json_set(
            "set_race_doc",
            &JsonPath::root(),
            JsonValue::from_value(serde_json::json!({ "from": "txn2" })),
        )
        .unwrap();
    }

    txn1.commit().unwrap();
    let err = txn2
        .commit()
        .expect_err("blind root-set create should conflict when another transaction wins first");
    assert!(
        matches!(err, StrataError::VersionConflict { .. }),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_concurrent_json_set_to_same_missing_doc_allows_only_one_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let barrier = Arc::new(Barrier::new(2));

    let db1 = db.clone();
    let ns1 = ns.clone();
    let barrier1 = barrier.clone();
    let t1 = thread::spawn(move || -> StrataResult<()> {
        let mut txn = db1.begin_transaction(branch_id)?;
        {
            let mut json = txn.scoped(ns1);
            json.json_set(
                "racy_doc",
                &JsonPath::root(),
                JsonValue::from_value(serde_json::json!({ "from": "txn1" })),
            )?;
        }
        barrier1.wait();
        txn.commit().map(|_| ())
    });

    let db2 = db.clone();
    let ns2 = ns.clone();
    let barrier2 = barrier.clone();
    let t2 = thread::spawn(move || -> StrataResult<()> {
        let mut txn = db2.begin_transaction(branch_id)?;
        {
            let mut json = txn.scoped(ns2);
            json.json_set(
                "racy_doc",
                &JsonPath::root(),
                JsonValue::from_value(serde_json::json!({ "from": "txn2" })),
            )?;
        }
        barrier2.wait();
        txn.commit().map(|_| ())
    });

    let r1 = t1.join().expect("txn1 thread panicked");
    let r2 = t2.join().expect("txn2 thread panicked");

    let success_count = usize::from(r1.is_ok()) + usize::from(r2.is_ok());
    assert_eq!(
        success_count, 1,
        "exactly one competing JSON commit should win"
    );

    let loser = if r1.is_err() { r1.err() } else { r2.err() }.expect("one loser expected");
    assert!(
        matches!(loser, StrataError::VersionConflict { .. })
            || matches!(
                &loser,
                StrataError::TransactionAborted { reason }
                    if reason.contains("Validation failed")
            ),
        "losing concurrent JSON commit should fail with an OCC conflict, got {loser:?}"
    );

    let key = Key::new_json(ns, "racy_doc");
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .expect("winning JSON commit should persist the document");
    let doc = JsonStore::deserialize_doc_with_fallback_id(&stored.value, "racy_doc").unwrap();
    assert!(
        doc.value == JsonValue::from_value(serde_json::json!({ "from": "txn1" }))
            || doc.value == JsonValue::from_value(serde_json::json!({ "from": "txn2" })),
        "unexpected winning document payload: {:?}",
        doc.value
    );
}

#[test]
fn test_json_root_delete_wins_over_existing_generic_write_for_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_json(ns.clone(), "doc1");

    let existing = JsonStore::serialize_doc(&JsonDoc::new("doc1", JsonValue::from(0i64))).unwrap();
    blind_write(&db, key.clone(), existing);

    let mut txn = db.begin_transaction(branch_id).unwrap();
    let raw = JsonStore::serialize_doc(&JsonDoc::new("doc1", JsonValue::from(1i64))).unwrap();
    txn.put(key.clone(), raw).unwrap();
    {
        let mut json = txn.scoped(ns.clone());
        assert!(json.json_delete("doc1").unwrap());
    }
    txn.commit().unwrap();

    assert!(db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn test_json_root_set_wins_over_existing_generic_delete_for_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_json(ns.clone(), "doc1");

    let existing = JsonStore::serialize_doc(&JsonDoc::new("doc1", JsonValue::from(1i64))).unwrap();
    blind_write(&db, key.clone(), existing);

    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.delete(key.clone()).unwrap();
    {
        let mut json = txn.scoped(ns.clone());
        json.json_set(
            "doc1",
            &JsonPath::root(),
            JsonValue::from_value(serde_json::json!({
                "profile": { "name": "Ada" }
            })),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .expect("json set should restore the document");
    let doc = JsonStore::deserialize_doc_with_fallback_id(&stored.value, "doc1").unwrap();
    assert_eq!(
        doc.value,
        JsonValue::from_value(serde_json::json!({
            "profile": { "name": "Ada" }
        }))
    );
}
