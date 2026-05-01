//! Integration tests for the public transaction APIs.

use std::path::Path;
use std::sync::Arc;

use strata_core::{BranchId, Value};
use strata_engine::database::OpenSpec;
use strata_engine::StrataError;
use strata_engine::{Database, KVStore, SearchSubsystem};
use strata_storage::{Key, Namespace};
use tempfile::TempDir;

fn open_primary(path: &Path) -> Arc<Database> {
    Database::open_runtime(OpenSpec::primary(path).with_subsystem(SearchSubsystem)).unwrap()
}

fn kv_key(branch_id: BranchId, key: &str) -> Key {
    Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), key)
}

#[test]
fn closure_transaction_commits_multiple_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_primary(temp_dir.path());
    let branch_id = BranchId::new();
    let key_a = kv_key(branch_id, "a");
    let key_b = kv_key(branch_id, "b");

    db.transaction(branch_id, |txn| {
        txn.put(key_a.clone(), Value::Int(1))?;
        txn.put(key_b.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    let kv = KVStore::new(db);
    assert_eq!(
        kv.get(&branch_id, "default", "a").unwrap(),
        Some(Value::Int(1))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "b").unwrap(),
        Some(Value::Int(2))
    );
}

#[test]
fn closure_transaction_error_rolls_back_changes() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_primary(temp_dir.path());
    let branch_id = BranchId::new();
    let kv = KVStore::new(db.clone());

    kv.put(&branch_id, "default", "counter", Value::Int(10))
        .unwrap();

    let key = kv_key(branch_id, "counter");
    let result: Result<(), StrataError> = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(99))?;
        Err(StrataError::invalid_input("rollback"))
    });
    assert!(result.is_err());

    assert_eq!(
        kv.get(&branch_id, "default", "counter").unwrap(),
        Some(Value::Int(10))
    );
}

#[test]
fn manual_transaction_commit_and_abort_follow_public_contract() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_primary(temp_dir.path());
    let branch_id = BranchId::new();
    let committed = kv_key(branch_id, "committed");
    let aborted = kv_key(branch_id, "aborted");

    {
        let mut txn = db.begin_transaction(branch_id).unwrap();
        txn.put(committed.clone(), Value::Int(7)).unwrap();
        txn.commit().unwrap();
    }

    {
        let mut txn = db.begin_transaction(branch_id).unwrap();
        txn.put(aborted.clone(), Value::Int(9)).unwrap();
        txn.abort();
    }

    let kv = KVStore::new(db);
    assert_eq!(
        kv.get(&branch_id, "default", "committed").unwrap(),
        Some(Value::Int(7))
    );
    assert_eq!(kv.get(&branch_id, "default", "aborted").unwrap(), None);
}
