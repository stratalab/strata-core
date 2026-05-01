use std::sync::Arc;

use strata_core::id::{CommitVersion, TxnId};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::{
    CommitError, Key, Namespace, SegmentedStore, Storage, StorageError, TransactionContext,
    TransactionManager, WriteMode,
};

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

struct FailingApplyStore {
    inner: Arc<SegmentedStore>,
}

impl Storage for FailingApplyStore {
    fn get_versioned(
        &self,
        key: &Key,
        max_version: CommitVersion,
    ) -> strata_storage::StorageResult<Option<strata_core::VersionedValue>> {
        self.inner.get_versioned(key, max_version)
    }

    fn get_history(
        &self,
        key: &Key,
        limit: Option<usize>,
        before_version: Option<CommitVersion>,
    ) -> strata_storage::StorageResult<Vec<strata_core::VersionedValue>> {
        self.inner.get_history(key, limit, before_version)
    }

    fn scan_prefix(
        &self,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> strata_storage::StorageResult<Vec<(Key, strata_core::VersionedValue)>> {
        self.inner.scan_prefix(prefix, max_version)
    }

    fn current_version(&self) -> CommitVersion {
        self.inner.current_version()
    }

    fn put_with_version_mode(
        &self,
        _key: Key,
        _value: Value,
        _version: CommitVersion,
        _ttl: Option<std::time::Duration>,
        _mode: WriteMode,
    ) -> strata_storage::StorageResult<()> {
        Err(StorageError::InvalidInput {
            message: "injected apply failure".to_string(),
        })
    }

    fn delete_with_version(
        &self,
        _key: &Key,
        _version: CommitVersion,
    ) -> strata_storage::StorageResult<()> {
        Err(StorageError::InvalidInput {
            message: "injected apply failure".to_string(),
        })
    }
}

#[test]
fn manager_commit_detects_conflict_on_canonical_surface() {
    let store = Arc::new(SegmentedStore::new());
    let manager = TransactionManager::new(CommitVersion(1));
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "shared");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(0),
            CommitVersion(1),
            None,
            strata_storage::WriteMode::Append,
        )
        .unwrap();

    let mut txn1 = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
    assert_eq!(txn1.get(&key).unwrap(), Some(Value::Int(0)));
    txn1.put(key.clone(), Value::Int(1)).unwrap();

    let mut txn2 = TransactionContext::with_store(TxnId(2), branch_id, Arc::clone(&store));
    assert_eq!(txn2.get(&key).unwrap(), Some(Value::Int(0)));
    txn2.put(key.clone(), Value::Int(2)).unwrap();

    let version = manager.commit(&mut txn1, store.as_ref()).unwrap();
    assert!(version > CommitVersion(1));

    let err = manager.commit(&mut txn2, store.as_ref()).unwrap_err();
    assert!(matches!(err, CommitError::ValidationFailed(_)));
}

#[test]
fn manager_commit_rejects_branch_deleting_on_canonical_surface() {
    let store = Arc::new(SegmentedStore::new());
    let manager = TransactionManager::new(CommitVersion::ZERO);
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "blocked");

    manager.mark_branch_deleting(&branch_id);

    let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
    txn.put(key, Value::Int(1)).unwrap();

    let err = manager.commit(&mut txn, store.as_ref()).unwrap_err();
    assert!(matches!(err, CommitError::BranchDeleting(id) if id == branch_id));
}

#[test]
fn manager_commit_updates_visible_version_on_canonical_surface() {
    let store = Arc::new(SegmentedStore::new());
    let manager = TransactionManager::new(CommitVersion::ZERO);
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "visible");

    let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
    txn.put(key, Value::Int(7)).unwrap();

    let version = manager.commit(&mut txn, store.as_ref()).unwrap();
    assert_eq!(manager.current_version(), version);
    assert_eq!(manager.visible_version(), version);
}

#[test]
fn manager_commit_apply_failure_surfaces_storage_error_on_canonical_surface() {
    let inner = Arc::new(SegmentedStore::new());
    let store = FailingApplyStore {
        inner: Arc::clone(&inner),
    };
    let manager = TransactionManager::new(CommitVersion::ZERO);
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "apply_fail");

    let mut txn = TransactionContext::with_store(TxnId(1), branch_id, inner);
    txn.put(key, Value::Int(9)).unwrap();

    let err = manager.commit(&mut txn, &store).unwrap_err();
    assert!(matches!(err, CommitError::WALError(_)));
    assert_eq!(manager.current_version(), CommitVersion(1));
    assert_eq!(manager.visible_version(), CommitVersion(1));
}
