//! Reopen and drop regressions for public database APIs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use strata_core::{BranchId, Value};
use strata_engine::database::OpenSpec;
use strata_engine::{Database, EventLog, KVStore, SearchSubsystem};
use strata_storage::{Key, Namespace};
use tempfile::TempDir;

fn open_primary(path: &Path) -> Arc<Database> {
    Database::open_runtime(OpenSpec::primary(path).with_subsystem(SearchSubsystem)).unwrap()
}

fn kv_key(branch_id: BranchId, key: &str) -> Key {
    Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), key)
}

#[test]
fn drop_and_reopen_preserves_committed_writes() {
    let temp_dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();

    {
        let db = open_primary(temp_dir.path());
        let kv = KVStore::new(db.clone());
        let event = EventLog::new(db.clone());

        kv.put(
            &branch_id,
            "default",
            "persisted",
            Value::String("ok".into()),
        )
        .unwrap();
        event
            .append(
                &branch_id,
                "default",
                "persisted",
                Value::object(HashMap::from([(
                    "status".to_string(),
                    Value::String("ok".into()),
                )])),
            )
            .unwrap();
        db.flush().unwrap();
    }

    let reopened = open_primary(temp_dir.path());
    let kv = KVStore::new(reopened.clone());
    let event = EventLog::new(reopened);

    assert_eq!(
        kv.get(&branch_id, "default", "persisted").unwrap(),
        Some(Value::String("ok".into()))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);
    let verification = event.verify_chain(&branch_id, "default").unwrap();
    assert_eq!(verification, strata_engine::ChainVerification::valid(1));
    let persisted = event.get(&branch_id, "default", 0).unwrap().unwrap();
    assert_eq!(
        persisted.value.payload,
        Value::object(HashMap::from([(
            "status".to_string(),
            Value::String("ok".into()),
        )]))
    );
}

#[test]
fn dropped_manual_transaction_does_not_publish_partial_writes() {
    let temp_dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();

    {
        let db = open_primary(temp_dir.path());
        let mut txn = db.begin_transaction(branch_id).unwrap();
        txn.put(kv_key(branch_id, "pending"), Value::Int(5))
            .unwrap();
        drop(txn);
        db.flush().unwrap();
    }

    let reopened = open_primary(temp_dir.path());
    let kv = KVStore::new(reopened);
    assert_eq!(kv.get(&branch_id, "default", "pending").unwrap(), None);
}
