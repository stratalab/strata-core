//! Integration tests for opening and reopening databases.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use strata_core::{BranchId, Value};
use strata_engine::database::OpenSpec;
use strata_engine::{Database, EventLog, KVStore, SearchSubsystem};
use tempfile::TempDir;

fn open_primary(path: &Path) -> Arc<Database> {
    Database::open_runtime(OpenSpec::primary(path).with_subsystem(SearchSubsystem)).unwrap()
}

#[test]
fn fresh_database_starts_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_primary(temp_dir.path());
    let branch_id = BranchId::new();

    let kv = KVStore::new(db.clone());
    let event = EventLog::new(db);

    assert_eq!(kv.get(&branch_id, "default", "missing").unwrap(), None);
    assert_eq!(event.len(&branch_id, "default").unwrap(), 0);
}

#[test]
fn reopen_restores_committed_kv_and_event_state() {
    let temp_dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();

    {
        let db = open_primary(temp_dir.path());
        let kv = KVStore::new(db.clone());
        let event = EventLog::new(db.clone());

        kv.put(
            &branch_id,
            "default",
            "greeting",
            Value::String("hello".into()),
        )
        .unwrap();
        event
            .append(
                &branch_id,
                "default",
                "greeting.logged",
                Value::object(HashMap::from([(
                    "message".to_string(),
                    Value::String("hello".into()),
                )])),
            )
            .unwrap();
        db.flush().unwrap();
    }

    let reopened = open_primary(temp_dir.path());
    let kv = KVStore::new(reopened.clone());
    let event = EventLog::new(reopened);

    assert_eq!(
        kv.get(&branch_id, "default", "greeting").unwrap(),
        Some(Value::String("hello".into()))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);
    let persisted = event.get(&branch_id, "default", 0).unwrap().unwrap();
    assert_eq!(
        persisted.value.payload,
        Value::object(HashMap::from([(
            "message".to_string(),
            Value::String("hello".into()),
        )]))
    );
}
