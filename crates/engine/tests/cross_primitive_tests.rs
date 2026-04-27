//! Cross-primitive regressions using only public APIs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use strata_core::{BranchId, Value};
use strata_engine::database::OpenSpec;
use strata_engine::{Database, EventLog, JsonPath, JsonStore, KVStore, SearchSubsystem};
use tempfile::TempDir;

fn open_primary(path: &Path) -> Arc<Database> {
    Database::open_runtime(OpenSpec::primary(path).with_subsystem(SearchSubsystem)).unwrap()
}

#[test]
fn kv_json_and_event_state_share_the_same_branch_view() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_primary(temp_dir.path());
    let branch_id = BranchId::new();

    let kv = KVStore::new(db.clone());
    let json = JsonStore::new(db.clone());
    let event = EventLog::new(db);

    kv.put(
        &branch_id,
        "default",
        "profile:name",
        Value::String("Ada".into()),
    )
    .unwrap();
    json.create(
        &branch_id,
        "default",
        "profile",
        serde_json::json!({"name": "Ada", "role": "research"}).into(),
    )
    .unwrap();
    event
        .append(
            &branch_id,
            "default",
            "profile.updated",
            Value::object(HashMap::from([(
                "name".to_string(),
                Value::String("Ada".into()),
            )])),
        )
        .unwrap();

    assert_eq!(
        kv.get(&branch_id, "default", "profile:name").unwrap(),
        Some(Value::String("Ada".into()))
    );
    assert_eq!(
        json.get(&branch_id, "default", "profile", &JsonPath::root())
            .unwrap()
            .map(|value| serde_json::to_value(value).unwrap()),
        Some(serde_json::json!({"name": "Ada", "role": "research"}))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);
}

#[test]
fn kv_json_and_event_data_survive_reopen_together() {
    let temp_dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();

    {
        let db = open_primary(temp_dir.path());
        let kv = KVStore::new(db.clone());
        let json = JsonStore::new(db.clone());
        let event = EventLog::new(db.clone());

        kv.put(&branch_id, "default", "state", Value::Int(7))
            .unwrap();
        json.create(
            &branch_id,
            "default",
            "doc",
            serde_json::json!({"v": 7}).into(),
        )
        .unwrap();
        event
            .append(
                &branch_id,
                "default",
                "updated",
                Value::object(HashMap::from([("v".to_string(), Value::Int(7))])),
            )
            .unwrap();
        db.flush().unwrap();
    }

    let reopened = open_primary(temp_dir.path());
    let kv = KVStore::new(reopened.clone());
    let json = JsonStore::new(reopened.clone());
    let event = EventLog::new(reopened);

    assert_eq!(
        kv.get(&branch_id, "default", "state").unwrap(),
        Some(Value::Int(7))
    );
    assert_eq!(
        json.get(&branch_id, "default", "doc", &JsonPath::root())
            .unwrap()
            .map(|value| serde_json::to_value(value).unwrap()),
        Some(serde_json::json!({"v": 7}))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);
    let persisted = event.get(&branch_id, "default", 0).unwrap().unwrap();
    assert_eq!(
        persisted.value.payload,
        Value::object(HashMap::from([("v".to_string(), Value::Int(7))]))
    );
}
