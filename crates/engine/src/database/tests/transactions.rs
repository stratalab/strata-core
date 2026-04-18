use super::*;

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
