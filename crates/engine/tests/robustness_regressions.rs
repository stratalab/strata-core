//! Robustness regressions for public database APIs.

use std::path::Path;
use std::sync::Arc;

use strata_core::{BranchId, Value};
use strata_engine::database::OpenSpec;
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
fn read_only_transactions_reject_writes_and_keep_state_intact() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_primary(temp_dir.path());
    let branch_id = BranchId::new();
    let key = kv_key(branch_id, "read-only");

    let mut txn = db.begin_read_only_transaction(branch_id).unwrap();
    let err = txn.put(key.clone(), Value::Int(1)).unwrap_err();
    assert!(err.to_string().contains("read-only"));

    let kv = KVStore::new(db);
    assert_eq!(kv.get(&branch_id, "default", "read-only").unwrap(), None);
}

#[test]
fn dropped_transaction_releases_state_for_followup_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_primary(temp_dir.path());
    let branch_id = BranchId::new();
    let key = kv_key(branch_id, "followup");

    {
        let mut txn = db.begin_transaction(branch_id).unwrap();
        txn.put(key.clone(), Value::Int(1)).unwrap();
        drop(txn);
    }

    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    let kv = KVStore::new(db);
    assert_eq!(
        kv.get(&branch_id, "default", "followup").unwrap(),
        Some(Value::Int(2))
    );
}
