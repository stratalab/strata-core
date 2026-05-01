use super::*;

/// Issue #1697: background compaction with max_versions_per_key must not
/// prune versions that active snapshots need.
#[test]
fn test_issue_1697_compaction_preserves_snapshot_versions() {
    let temp_dir = TempDir::new().unwrap();
    let mut cfg = StrataConfig::default();
    cfg.storage.max_versions_per_key = 1;
    let db = Database::open_with_config(temp_dir.path().join("db"), cfg).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "snap_key");

    // Write version 1
    blind_write(&db, key.clone(), Value::Int(1));

    // Start a reader that pins the current snapshot
    let mut reader = db.begin_read_only_transaction(branch_id).unwrap();
    let pinned_version = reader.start_version;

    // Write version 2 — now key has 2 versions, max_versions_per_key=1
    blind_write(&db, key.clone(), Value::Int(2));

    // Force data from memtable → segments so compaction can process it.
    // Rotate twice to create 2 frozen memtables → flush both → compact.
    db.storage().rotate_memtable(&branch_id);
    db.storage().flush_oldest_frozen(&branch_id).unwrap();
    // Need a second segment to trigger compaction (min 2 segments).
    // Write a dummy key so the second memtable isn't empty.
    blind_write(&db, Key::new_kv(ns.clone(), "dummy"), Value::Int(999));
    db.storage().rotate_memtable(&branch_id);
    db.storage().flush_oldest_frozen(&branch_id).unwrap();

    // Now compact — this is where max_versions would drop the old version.
    let compacted = db
        .storage()
        .compact_branch(&branch_id, CommitVersion::ZERO)
        .unwrap();
    assert!(compacted.is_some(), "compaction should have run");

    // The reader's snapshot must still be able to find version 1
    // via the storage layer at the pinned version.
    let result = db.storage().get_versioned(&key, pinned_version).unwrap();
    assert!(
        result.is_some(),
        "version at snapshot {} was pruned by compaction despite active reader",
        pinned_version
    );
    assert_eq!(
        result.unwrap().value,
        Value::Int(1),
        "snapshot read returned wrong value after compaction"
    );

    // Latest version must also be intact
    let latest = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(latest.is_some(), "latest version missing after compaction");
    assert_eq!(latest.unwrap().value, Value::Int(2));

    // Clean up reader
    reader.abort();
}

#[test]
#[serial(open_databases)]
fn test_issue_1730_checkpoint_compact_recovery_data_loss() {
    // Inverted in the T3-E5 follow-up.
    //
    // Pre-retention, recovery was either WAL-only or lacked tombstone/TTL
    // fidelity, so `compact()` had to refuse WAL deletion on snapshot-only
    // coverage (`#1730`). Now that snapshot install is retention-complete
    // (tombstones, TTL, branch metadata round-trip), compact() succeeds,
    // WAL is reclaimed, and the committed data survives reopen through
    // snapshot install alone.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "critical_data");

        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::String("must_survive".to_string()))?;
            Ok(())
        })
        .unwrap();

        db.checkpoint().unwrap();

        // compact() now succeeds on snapshot-only coverage.
        db.compact()
            .expect("compact() must succeed once snapshot coverage is trusted");
    }

    OPEN_DATABASES.lock().clear();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "critical_data");

        let result = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("data must survive checkpoint+compact+recovery via snapshot install");
        assert_eq!(result.value, Value::String("must_survive".to_string()));
    }
}

#[test]
#[serial(open_databases)]
fn test_issue_1730_standard_durability() {
    // Inverted twin for Standard durability mode. Same contract: compact
    // succeeds on snapshot-only coverage, data round-trips via snapshot
    // install.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let durability = DurabilityMode::Standard {
        interval_ms: 100,
        batch_size: 1,
    };

    {
        let db = Database::open_with_durability(&db_path, durability).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "standard_data");

        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::String("durable".to_string()))?;
            Ok(())
        })
        .unwrap();

        // Flush ensures the record is on disk before checkpoint so the
        // snapshot + MANIFEST watermark reflect the transaction.
        db.flush().unwrap();
        db.checkpoint().unwrap();

        db.compact()
            .expect("compact() must succeed on snapshot-only watermark under Standard durability");
    }

    OPEN_DATABASES.lock().clear();

    // Reopen and verify data survived
    {
        let db = Database::open_with_durability(&db_path, durability).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "standard_data");

        let result = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("data must survive checkpoint+compact+recovery under Standard durability");
        assert_eq!(result.value, Value::String("durable".to_string()));
    }
}

/// Issue #1699: Follower refresh must use the WAL record's commit timestamp,
/// not Timestamp::now(), so that all entries from the same transaction share
/// a single timestamp for correct time-travel queries.
///
/// Without the fix, each put_with_version_mode / delete_with_version call
/// creates its own Timestamp::now(), splitting a transaction across multiple
/// timestamps and causing time-travel queries to see partial state.
#[test]
fn test_issue_1699_refresh_preserves_wal_timestamp() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::default();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // 1. Open primary with Always durability (syncs every commit to WAL)
    let primary = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    // 2. Pre-populate key B
    let key_b = Key::new_kv(ns.clone(), "b");
    primary
        .transaction(branch_id, |txn| {
            txn.put(key_b.clone(), Value::String("old".into()))?;
            Ok(())
        })
        .unwrap();

    // 3. Open follower — initial recovery replays B via RecoveryCoordinator (correct path)
    // Followers don't use the registry, so they can coexist with a primary in the same process.
    let follower = Database::open_follower(&db_path).unwrap();

    // 4. Primary commits: put A + delete B in one transaction
    let key_a = Key::new_kv(ns.clone(), "a");
    primary
        .transaction(branch_id, |txn| {
            txn.put(key_a.clone(), Value::String("new".into()))?;
            txn.delete(key_b.clone())?;
            Ok(())
        })
        .unwrap();

    // Record upper bound on the WAL record's timestamp
    let commit_ts_upper = Timestamp::now().as_micros();

    // 5. Sleep to create a clear gap between commit time and refresh time.
    //    100ms = 100_000 microseconds — far larger than any clock jitter.
    std::thread::sleep(Duration::from_millis(100));

    // 6. Follower refresh — applies the new WAL records via Database::refresh
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up(), "follower should catch up");
    assert!(
        outcome.applied_count() > 0,
        "follower should apply the new transaction"
    );

    // 7. Check: A's entry timestamp should come from the WAL record (near
    //    commit time), NOT from Timestamp::now() during refresh.
    let a_entry = follower
        .storage()
        .get_versioned(&key_a, CommitVersion::MAX)
        .unwrap()
        .expect("key A should exist after refresh");
    let a_ts = a_entry.timestamp.as_micros();

    assert!(
        a_ts <= commit_ts_upper,
        "Entry timestamp ({}) should be from WAL record (≤ {}), \
         not from Timestamp::now() during refresh",
        a_ts,
        commit_ts_upper,
    );

    // 8. Time-travel at A's timestamp: B should be deleted (same transaction).
    //    With split timestamps, B's tombstone has a later timestamp than A's put,
    //    so querying at A's timestamp would incorrectly show B as still alive.
    let b_at_a_ts = follower.storage().get_at_timestamp(&key_b, a_ts).unwrap();
    assert!(
        b_at_a_ts.is_none(),
        "Key B should be deleted at A's timestamp (same transaction). \
         Got {:?}, indicating split timestamps between put and delete.",
        b_at_a_ts.map(|v| v.value),
    );
}

/// Issue #1736: Compaction must run on the background scheduler, not inline
/// on the writer thread. We verify by checking that the background scheduler
/// completed compaction tasks — before the fix, compaction ran synchronously
/// and the scheduler had zero completed tasks.
#[test]
fn test_issue_1736_compaction_runs_in_background() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    // Tiny write buffer: forces a memtable rotation (and flush) after ~1-2 writes.
    let cfg = StrataConfig {
        durability: "always".to_string(),
        storage: StorageConfig {
            write_buffer_size: 256,
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let db = Database::open_with_config(&db_path, cfg).unwrap();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    let tasks_before = db.scheduler.stats().tasks_completed;

    // Write enough entries to create 6+ L0 segments, triggering compaction.
    // Each write ~100 bytes; write_buffer_size=256 → rotation every ~2-3 writes.
    for i in 0..20 {
        let key = Key::new_kv(ns.clone(), format!("key_{:04}", i));
        db.transaction(branch_id, |txn: &mut strata_storage::TransactionContext| {
            txn.put(key.clone(), Value::String(format!("value_{}", i)))?;
            Ok(())
        })
        .unwrap();
    }

    // Drain the scheduler to ensure all background tasks complete.
    db.scheduler.drain();

    let tasks_after = db.scheduler.stats().tasks_completed;
    let bg_tasks = tasks_after - tasks_before;

    // With background compaction (fix): scheduler ran compaction tasks → bg_tasks > 0.
    // With synchronous compaction (bug): scheduler was never used → bg_tasks == 0.
    assert!(
        bg_tasks > 0,
        "Background scheduler should have completed compaction tasks. \
         tasks_completed before={}, after={}, delta={}. \
         If delta is 0, compaction ran synchronously on the writer thread.",
        tasks_before,
        tasks_after,
        bg_tasks,
    );

    // Verify data integrity: all entries should be readable after compaction.
    for i in 0..20 {
        let key = Key::new_kv(ns.clone(), format!("key_{:04}", i));
        let entry = db
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("key_{:04} should exist after compaction", i));
        assert_eq!(entry.value, Value::String(format!("value_{}", i)),);
    }
}

#[test]
fn test_issue_1732_checkpoint_data_preserves_timestamps() {
    // Issue #1732: collect_checkpoint_data() rewrites all timestamps to `now`,
    // losing the original commit timestamps.
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "ts_test");

    // Write data
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("hello".to_string()))?;
        Ok(())
    })
    .unwrap();

    // Get the timestamp from list_by_type (same source as collect_checkpoint_data)
    let listed = db.storage().list_by_type(&branch_id, TypeTag::KV);
    let entry_for_key = listed
        .iter()
        .find(|(k, _)| k.user_key_string().unwrap_or_default() == "ts_test")
        .expect("should find ts_test entry");
    let storage_ts = entry_for_key.1.timestamp.as_micros();
    assert!(storage_ts > 0, "Storage timestamp should be non-zero");

    // Sleep so `now` would differ from the storage timestamp
    std::thread::sleep(Duration::from_millis(50));
    let now_after_sleep = strata_storage::durability::now_micros();

    let data = db.collect_checkpoint_data();
    let kv_entries = data.kv.expect("should have KV entries");

    let entry = kv_entries
        .iter()
        .find(|e| e.user_key == b"ts_test".to_vec())
        .expect("should find ts_test in checkpoint");

    // The checkpoint timestamp must match the storage timestamp, not `now`
    assert_eq!(
        entry.timestamp, storage_ts,
        "Checkpoint timestamp should match storage timestamp, not now",
    );
    // Sanity: now is significantly later than storage_ts
    assert!(
        now_after_sleep > storage_ts + 40_000,
        "now ({}) should be at least 40ms after storage_ts ({})",
        now_after_sleep,
        storage_ts,
    );
}

#[test]
fn test_issue_1732_checkpoint_data_preserves_branch_names() {
    // Issue #1732: collect_checkpoint_data() sets branch name to String::new()
    // instead of extracting it from the serialized BranchMetadata.
    use crate::primitives::branch::{BranchIndex, BranchMetadata};

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_index = BranchIndex::new(db.clone());
    branch_index.create_branch("my-test-branch").unwrap();

    let data = db.collect_checkpoint_data();
    let branch_entries = data.branches.expect("should have branch entries");

    let found = branch_entries.iter().find(|b| b.key == "my-test-branch");
    assert!(
        found.is_some(),
        "Branch key 'my-test-branch' should be preserved in checkpoint. \
         Got keys: {:?}",
        branch_entries.iter().map(|b| &b.key).collect::<Vec<_>>(),
    );
    let branch_entry = found.unwrap();

    let stored_value: Value =
        serde_json::from_slice(&branch_entry.value).expect("branch snapshot value should decode");
    let stored_json = match stored_value {
        Value::String(json) => json,
        other => panic!("branch snapshot should store Value::String, got {other:?}"),
    };
    let metadata: BranchMetadata =
        serde_json::from_str(&stored_json).expect("branch metadata should decode");

    assert_eq!(metadata.name, "my-test-branch");
    assert!(
        metadata.created_at.as_micros() > 0,
        "Branch created_at should be non-zero in serialized metadata",
    );
    assert!(
        branch_entry.version > 0,
        "Branch snapshot must preserve MVCC version"
    );
    assert!(
        branch_entry.timestamp > 0,
        "Branch snapshot must preserve MVCC timestamp",
    );
}

#[test]
fn test_checkpoint_data_preserves_branch_space_and_type_for_kv_entries() {
    let db = Database::cache().unwrap();
    let branch_a = BranchId::from_bytes([1; 16]);
    let branch_b = BranchId::from_bytes([2; 16]);
    let timestamp_micros = 1_234_567u64;
    let version = CommitVersion(7);
    let value = Value::Int(42);

    let ns_a = Arc::new(Namespace::new(branch_a, "default".to_string()));
    let ns_b = Arc::new(Namespace::new(branch_b, "tenant_a".to_string()));

    db.storage
        .put_recovery_entry(
            Key::new_kv(ns_a, "ambiguous"),
            value.clone(),
            version,
            timestamp_micros,
            0,
        )
        .unwrap();
    db.storage
        .put_recovery_entry(
            Key::new_kv(ns_b, "ambiguous"),
            value.clone(),
            version,
            timestamp_micros,
            0,
        )
        .unwrap();

    let data = db.collect_checkpoint_data();
    let kv_entries = data.kv.expect("should have KV entries");
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_a.as_bytes()
            && entry.space == "default"
            && entry.type_tag == TypeTag::KV.as_byte()
            && entry.user_key == b"ambiguous".to_vec()
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_b.as_bytes()
            && entry.space == "tenant_a"
            && entry.type_tag == TypeTag::KV.as_byte()
            && entry.user_key == b"ambiguous".to_vec()
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_binary_kv_keys() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([9; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "binary".to_string()));
    let user_key = vec![0x00, 0xFF, 0x80, 0x01];
    let key = Key::new(namespace, TypeTag::KV, user_key.clone());
    let value = Value::Int(7);
    let version = CommitVersion(13);
    let timestamp_micros = 9_999_999u64;

    db.storage
        .put_recovery_entry(key, value.clone(), version, timestamp_micros, 0)
        .unwrap();

    let kv_entries = db
        .collect_checkpoint_data()
        .kv
        .expect("should have KV entries");
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "binary"
            && entry.type_tag == TypeTag::KV.as_byte()
            && entry.user_key == user_key
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_graph_type_and_space_metadata() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([3; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "tenant_graph".to_string()));
    let timestamp_micros = 2_345_678u64;
    let version = CommitVersion(9);
    let value = Value::String("graph-payload".to_string());

    db.storage
        .put_recovery_entry(
            Key::new_graph(namespace, "shared"),
            value.clone(),
            version,
            timestamp_micros,
            0,
        )
        .unwrap();

    let data = db.collect_checkpoint_data();
    let kv_entries = data.kv.expect("should have KV entries");
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "tenant_graph"
            && entry.type_tag == TypeTag::Graph.as_byte()
            && entry.user_key == b"shared".to_vec()
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_event_space_metadata() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([6; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "tenant_events".to_string()));
    let key = Key::new_event(namespace, 7);
    let value = Value::String("event-payload".to_string());
    let timestamp_micros = 7_654_321u64;
    let version = CommitVersion(3);

    db.storage
        .put_recovery_entry(key, value.clone(), version, timestamp_micros, 0)
        .unwrap();

    let events = db
        .collect_checkpoint_data()
        .events
        .expect("should have event entries");
    assert!(events.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "tenant_events"
            && entry.sequence == 7
            && entry.payload == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_json_space_metadata() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([7; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "products".to_string()));
    let key = Key::new_json(namespace, "doc-1");
    let value = Value::String("{\"sku\":\"123\"}".to_string());
    let timestamp_micros = 8_765_432u64;
    let version = CommitVersion(4);

    db.storage
        .put_recovery_entry(key, value.clone(), version, timestamp_micros, 0)
        .unwrap();

    let json_entries = db
        .collect_checkpoint_data()
        .json
        .expect("should have json entries");
    assert!(json_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "products"
            && entry.doc_id == "doc-1"
            && entry.content == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_vector_space_metadata() {
    #[derive(serde::Serialize)]
    struct TestVectorRecord {
        vector_id: u64,
        #[serde(default)]
        embedding: Vec<f32>,
        metadata: Option<serde_json::Value>,
        version: u64,
        created_at: u64,
        updated_at: u64,
        #[serde(default)]
        source_ref: Option<strata_core::EntityRef>,
    }

    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([8; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "semantic".to_string()));
    let config_key = Key::new_vector_config(namespace.clone(), "embeddings");
    let vector_key = Key::new_vector(namespace, "embeddings", "vec-1");
    let config_version = CommitVersion(1);
    let vector_version = CommitVersion(2);
    let config_timestamp = 1_000u64;
    let vector_timestamp = 2_000u64;
    let record = TestVectorRecord {
        vector_id: 99,
        embedding: vec![0.1, 0.2, 0.3],
        metadata: Some(serde_json::json!({ "label": "test" })),
        version: 1,
        created_at: 1_000,
        updated_at: 1_000,
        source_ref: None,
    };
    let raw_record = rmp_serde::to_vec(&record).unwrap();

    db.storage
        .put_recovery_entry(
            config_key,
            Value::Bytes(b"{\"dimensions\":3}".to_vec()),
            config_version,
            config_timestamp,
            0,
        )
        .unwrap();
    db.storage
        .put_recovery_entry(
            vector_key,
            Value::Bytes(raw_record.clone()),
            vector_version,
            vector_timestamp,
            0,
        )
        .unwrap();

    let vector_entries = db
        .collect_checkpoint_data()
        .vectors
        .expect("should have vector entries");
    assert!(vector_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "semantic"
            && entry.name == "embeddings"
            && entry.config == b"{\"dimensions\":3}".to_vec()
            && entry.config_version == config_version.as_u64()
            && entry.config_timestamp == config_timestamp
            && entry.vectors.iter().any(|vector| {
                vector.key == "vec-1"
                    && vector.raw_value == raw_record
                    && vector.version == vector_version.as_u64()
                    && vector.timestamp == vector_timestamp
            })
    }));
}

#[test]
fn test_checkpoint_data_includes_tombstones() {
    // Inverted from the pre-retention `test_checkpoint_data_omits_tombstones_*`.
    // The collector now uses `list_by_type_at_version(…, MAX)` which preserves
    // tombstones, so a deleted key must appear in the checkpoint payload as
    // an `is_tombstone = true` entry.
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([4; 16]);
    let key = Key::new_kv(create_test_namespace(branch_id), "deleted");

    db.storage
        .put_recovery_entry(key.clone(), Value::Int(1), CommitVersion(1), 1_000, 0)
        .unwrap();
    db.storage
        .delete_recovery_entry(&key, CommitVersion(2), 2_000)
        .unwrap();

    let kv_entries = db.collect_checkpoint_data().kv.unwrap_or_default();
    let entry = kv_entries
        .iter()
        .find(|e| e.user_key == b"deleted".to_vec())
        .expect("deleted key must appear in checkpoint as a tombstone entry");
    assert!(
        entry.is_tombstone,
        "deleted key must surface with is_tombstone=true, got: {:?}",
        entry
    );
    assert_eq!(
        entry.version, 2,
        "tombstone must carry the delete's commit version, not the prior put's"
    );
}

#[test]
fn test_checkpoint_data_includes_ttl_metadata() {
    // Inverted from the pre-retention `test_checkpoint_data_omits_ttl_metadata`.
    // The KV snapshot DTO now carries `ttl_ms`, so the collected entry
    // reflects the per-key TTL exactly.
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([5; 16]);
    let key = Key::new_kv(create_test_namespace(branch_id), "ttl_key");
    let version = CommitVersion(11);
    let ttl_ms = 60_000u64;
    let value = Value::String("ttl".to_string());

    db.storage
        .put_with_version_mode(
            key.clone(),
            value.clone(),
            version,
            Some(Duration::from_millis(ttl_ms)),
            WriteMode::Append,
        )
        .unwrap();

    let kv_entries = db
        .collect_checkpoint_data()
        .kv
        .expect("should have KV entries");
    let snapshot_entry = kv_entries
        .iter()
        .find(|entry| entry.user_key == b"ttl_key".to_vec())
        .expect("should find ttl_key in checkpoint");

    assert_eq!(
        snapshot_entry.ttl_ms, ttl_ms,
        "snapshot entry must carry the original TTL value"
    );
    assert!(
        !snapshot_entry.is_tombstone,
        "live entry must not be tombstoned"
    );
    assert_eq!(snapshot_entry.version, version.as_u64());
}

/// Issue #1738 / 9.2.A: begin_transaction() does not check accepting_transactions.
/// After shutdown(), manual callers can still start transactions.
#[test]
fn test_issue_1738_begin_transaction_bypasses_shutdown_gate() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::new();
    db.shutdown().unwrap();

    // transaction() correctly rejects after shutdown
    let closure_result = db.transaction(branch_id, |_txn| Ok(()));
    assert!(
        closure_result.is_err(),
        "transaction() should reject after shutdown"
    );

    // begin_transaction() SHOULD also reject after shutdown
    let manual_result = db.begin_transaction(branch_id);
    assert!(
        manual_result.is_err(),
        "begin_transaction() must reject after shutdown, but it succeeded"
    );
}

/// Issue #1738 / 9.2.B: shutdown() polls active_count() with Relaxed ordering
/// instead of using wait_for_idle() with SeqCst. This test verifies that the
/// shutdown path uses the correct synchronization by checking that it calls
/// wait_for_idle (which properly synchronizes with record_start/record_commit).
///
/// We verify this indirectly: start a transaction, then shutdown. If shutdown
/// uses proper ordering, it will always see the active transaction and wait.
#[test]
fn test_issue_1738_shutdown_waits_for_active_transactions() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::new();

    // Start a transaction manually (increments active_count)
    let txn = db.begin_transaction(branch_id).unwrap();

    // Spawn shutdown in background — it should wait for the active transaction
    let db2 = Arc::clone(&db);
    let shutdown_handle = std::thread::spawn(move || {
        db2.shutdown().unwrap();
    });

    // Give shutdown a moment to start waiting
    std::thread::sleep(Duration::from_millis(50));

    // End the transaction (decrements active_count)
    db.end_transaction(txn);

    // Shutdown should complete within a reasonable time
    shutdown_handle.join().unwrap();
}

#[test]
#[serial(open_databases)]
fn test_issue_1733_history_no_duplicates_after_recovery() {
    // Issue #1733: WAL replay replays ALL records (including already-flushed
    // ones) into memtables. After segment recovery loads the same data from
    // disk, get_history() returns duplicate versions.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();

    // Phase 1: Write data and flush to segments
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "history_key");

        // Write 3 versions
        blind_write(&db, key.clone(), Value::Int(1));
        blind_write(&db, key.clone(), Value::Int(2));
        blind_write(&db, key.clone(), Value::Int(3));

        // Flush memtable to segment — data is now in BOTH WAL and segment
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();

        // Verify history has 3 versions before close
        let history = db.get_history(&key, None, None).unwrap();
        assert_eq!(history.len(), 3, "pre-close history should have 3 versions");
    }

    // Clear registry so reopen creates a fresh instance
    OPEN_DATABASES.lock().clear();

    // Phase 2: Reopen — WAL replays all records, then segments are loaded
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "history_key");

        let history = db.get_history(&key, None, None).unwrap();

        // BUG: Without dedup, this returns 6 versions (3 from memtable + 3 from segment)
        assert_eq!(
            history.len(),
            3,
            "get_history() returned {} versions after recovery (expected 3 — duplicates detected)",
            history.len()
        );

        // Verify correct values (newest first)
        assert_eq!(history[0].value, Value::Int(3));
        assert_eq!(history[1].value, Value::Int(2));
        assert_eq!(history[2].value, Value::Int(1));

        // Also verify point read returns the correct latest value (no regression)
        let latest = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap();
        assert!(latest.is_some(), "point read should find the key");
        assert_eq!(latest.unwrap().value, Value::Int(3));
    }
}

#[test]
#[serial(open_databases)]
fn test_issue_1733_partial_flush_no_duplicates() {
    // Variant: only some versions are flushed. After recovery, flushed
    // versions appear in both memtable and segment; unflushed versions
    // appear only in memtable. History must still have no duplicates.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "partial_key");

        // Write v1, v2 then flush
        blind_write(&db, key.clone(), Value::Int(1));
        blind_write(&db, key.clone(), Value::Int(2));
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();

        // Write v3 (not flushed — only in WAL)
        blind_write(&db, key.clone(), Value::Int(3));
    }

    OPEN_DATABASES.lock().clear();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "partial_key");

        let history = db.get_history(&key, None, None).unwrap();
        assert_eq!(
            history.len(),
            3,
            "partial flush: expected 3 versions, got {}",
            history.len()
        );
        assert_eq!(history[0].value, Value::Int(3));
        assert_eq!(history[1].value, Value::Int(2));
        assert_eq!(history[2].value, Value::Int(1));
    }
}

#[test]
#[serial(open_databases)]
fn test_issue_1733_tombstone_no_duplicate_after_recovery() {
    // Variant: a delete (tombstone) was flushed. After recovery, history
    // must show the tombstone exactly once, not twice.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns.clone(), "tomb_key");

        blind_write(&db, key.clone(), Value::Int(1));

        // Delete the key (creates tombstone)
        db.transaction(branch_id, |txn| {
            txn.delete(key.clone())?;
            Ok(())
        })
        .unwrap();

        // Flush both versions to segment
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();
    }

    OPEN_DATABASES.lock().clear();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "tomb_key");

        let history = db.get_history(&key, None, None).unwrap();
        // History includes tombstones: should be [tombstone@v2, put@v1]
        assert_eq!(
            history.len(),
            2,
            "tombstone dedup: expected 2 versions (tombstone + put), got {}",
            history.len()
        );

        // Point read should return None (key is deleted)
        let latest = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap();
        assert!(
            latest.is_none(),
            "deleted key should not be found via point read"
        );
    }
}
// ========================================================================
// Issue #1914: Executor event append bypasses OCC — hash chain corruption
// ========================================================================

/// Two concurrent transactions both appending events to the same branch
/// must conflict at commit time. Before the fix, the meta_key was never
/// read, so OCC validation had no read-set entry to detect the conflict.
#[test]
fn test_issue_1914_concurrent_event_append_occ_conflict() {
    use crate::transaction_ops::TransactionOps;

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();

    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Begin two transactions on the same branch at the same snapshot
    let mut txn1 = db.begin_transaction(branch_id).unwrap();
    let mut txn2 = db.begin_transaction(branch_id).unwrap();

    // Both append an event using the Transaction wrapper (executor path)
    {
        let mut t1 = crate::transaction::context::Transaction::new(&mut txn1, ns.clone());
        t1.event_append("test_event", Value::String("from_t1".into()))
            .unwrap();
    }
    {
        let mut t2 = crate::transaction::context::Transaction::new(&mut txn2, ns.clone());
        t2.event_append("test_event", Value::String("from_t2".into()))
            .unwrap();
    }

    // Commit T1 — should succeed
    db.commit_transaction(&mut txn1).unwrap();
    db.end_transaction(txn1);

    // Commit T2 — MUST fail with a conflict.
    // Both wrote to meta_key (event log metadata). If meta_key is in both
    // read_sets, OCC detects that T1 modified it after T2's snapshot.
    // Before the fix: both commit succeeds → duplicate sequence 0,
    // corrupted hash chain.
    let result = db.commit_transaction(&mut txn2);
    db.end_transaction(txn2);

    let err = result.expect_err(
        "T2 must conflict: both transactions appended to the same event log but OCC did not detect the conflict because meta_key was never read",
    );
    assert!(
        matches!(err, StrataError::TransactionAborted { .. }),
        "expected the legacy transaction-aborted OCC error, got {err:?}"
    );
}

/// Verify that after the fix, sequential event appends produce correct
/// sequence numbers sourced from persisted metadata, not ephemeral defaults.
#[test]
fn test_issue_1914_sequence_from_persisted_meta() {
    use crate::transaction_ops::TransactionOps;

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();

    let ns = Arc::new(Namespace::for_branch(branch_id));

    // First transaction: append event at sequence 0
    let mut txn1 = db.begin_transaction(branch_id).unwrap();
    {
        let mut t = crate::transaction::context::Transaction::new(&mut txn1, ns.clone());
        let v = t
            .event_append("evt", Value::String("first".into()))
            .unwrap();
        assert_eq!(v, strata_core::Version::seq(0));
    }
    db.commit_transaction(&mut txn1).unwrap();
    db.end_transaction(txn1);

    // Second transaction: should continue at sequence 1 (from persisted meta)
    let mut txn2 = db.begin_transaction(branch_id).unwrap();
    {
        let mut t = crate::transaction::context::Transaction::new(&mut txn2, ns.clone());
        let v = t
            .event_append("evt", Value::String("second".into()))
            .unwrap();
        // The second scoped wrapper must continue from persisted metadata
        // rather than resetting back to sequence 0.
        assert_eq!(
            v,
            strata_core::Version::seq(1),
            "Second append must use sequence 1 from persisted meta, not 0"
        );
    }
    db.commit_transaction(&mut txn2).unwrap();
    db.end_transaction(txn2);

    // Verify the hash chain: event 1's prev_hash must equal event 0's hash
    let mut txn3 = db.begin_transaction(branch_id).unwrap();
    {
        let mut t = crate::transaction::context::Transaction::new(&mut txn3, ns.clone());
        let e0 = t.event_get(0).unwrap().expect("event 0 must exist");
        let e1 = t.event_get(1).unwrap().expect("event 1 must exist");
        assert_eq!(
            e1.value.prev_hash, e0.value.hash,
            "event 1's prev_hash must chain from event 0's hash"
        );
    }
    db.end_transaction(txn3);
}

#[test]
fn test_issue_1551_database_uuid_is_nonzero() {
    // Verify that a disk-backed database gets a real UUID, not all-zeros.
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    let uuid = db.database_uuid();
    assert_ne!(
        uuid, [0u8; 16],
        "disk-backed database must have a non-zero UUID"
    );
}

#[test]
fn test_issue_1551_database_uuid_persists_across_reopen() {
    // Verify that the UUID is stable across close/reopen.
    let temp_dir = TempDir::new().unwrap();

    let uuid_first = {
        let db = Database::open(temp_dir.path()).unwrap();
        db.database_uuid()
    };
    assert_ne!(uuid_first, [0u8; 16]);

    // Reopen — should get the same UUID from MANIFEST
    let uuid_second = {
        let db = Database::open(temp_dir.path()).unwrap();
        db.database_uuid()
    };
    assert_eq!(uuid_first, uuid_second, "UUID must persist across reopen");
}

#[test]
fn test_issue_1551_cache_database_uuid_is_zero() {
    // Ephemeral databases have no persistence, so UUID is all-zeros.
    let db = Database::cache().unwrap();
    assert_eq!(db.database_uuid(), [0u8; 16]);
}

/// Issue #1924: Write stall timeout must be surfaced to the caller.
///
/// When L0 segment count exceeds `l0_stop_writes_trigger` and compaction
/// cannot drain L0 within `write_stall_timeout_ms`, write transactions
/// must return `Err(WriteStallTimeout)` instead of silently succeeding
/// with the timeout error discarded.
#[test]
fn test_issue_1924_write_stall_timeout_surfaced_to_caller() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Configure: tiny write buffer to force frequent L0 creation,
    // stop trigger of 1 (any L0 segment triggers stall), and 1ms timeout
    // so the stall expires before compaction can drain L0.
    let cfg = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            write_buffer_size: 128,        // tiny: forces memtable rotation
            l0_stop_writes_trigger: 1,     // stall when ANY L0 segment exists
            l0_slowdown_writes_trigger: 0, // disabled
            write_stall_timeout_ms: 1,     // 1ms: too short for compaction to drain
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let spec = super::spec::OpenSpec::primary(&db_path)
        .with_config(cfg)
        .with_subsystem(crate::search::SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();

    // Write large values to force memtable rotation and L0 creation.
    // With write_buffer_size=128, a 512-byte value exceeds the threshold.
    // After each commit, schedule_flush_if_needed flushes frozen memtables
    // into L0 segments.
    let big_value = Value::Bytes(vec![0u8; 512]);

    // Rapidly write transactions. Some may succeed (if compaction drains L0
    // between writes), some may timeout. We care that at least one fails.
    let mut saw_timeout = false;
    for i in 0..20 {
        let key_name = format!("key_{}", i);
        let ns_clone = ns.clone();
        let val = big_value.clone();
        let result = db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns_clone, &key_name), val)?;
            Ok(())
        });
        if matches!(&result, Err(StrataError::WriteStallTimeout { .. })) {
            saw_timeout = true;
            break;
        }
    }

    assert!(
        saw_timeout,
        "Expected at least one WriteStallTimeout error among 20 writes, \
         but all succeeded. L0 count: {}",
        db.storage().max_l0_segment_count()
    );
}

/// Issue #1924 (manual API): Same backpressure fix for begin/commit_transaction path.
#[test]
fn test_issue_1924_write_stall_timeout_manual_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    let cfg = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            write_buffer_size: 128,
            l0_stop_writes_trigger: 1,
            l0_slowdown_writes_trigger: 0,
            write_stall_timeout_ms: 1,
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let spec = super::spec::OpenSpec::primary(&db_path)
        .with_config(cfg)
        .with_subsystem(crate::search::SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();
    let big_value = Value::Bytes(vec![0u8; 512]);

    let mut saw_timeout = false;
    for i in 0..20 {
        let mut txn = db.begin_transaction(branch_id).unwrap();
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        txn.put(key, big_value.clone()).unwrap();
        let result = db.commit_transaction(&mut txn);
        db.end_transaction(txn);
        if matches!(&result, Err(StrataError::WriteStallTimeout { .. })) {
            saw_timeout = true;
            break;
        }
    }

    assert!(
        saw_timeout,
        "Expected WriteStallTimeout via manual commit_transaction path, \
         but all succeeded. L0 count: {}",
        db.storage().max_l0_segment_count()
    );
}

#[test]
fn test_issue_1380_default_codec_is_identity() {
    // Verify that the default codec is "identity" and the MANIFEST records it.
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    drop(db);

    // Verify MANIFEST stores "identity" codec
    let manifest_path = temp_dir.path().join("MANIFEST");
    let m = strata_storage::durability::ManifestManager::load(manifest_path).unwrap();
    assert_eq!(m.manifest().codec_id, "identity");
}

/// Serializes tests that mutate `STRATA_ENCRYPTION_KEY`, which is a
/// process-global env var. Without this mutex, parallel tests
/// (e.g. `test_issue_1380_codec_mismatch_rejected` and
/// `test_issue_1380_encryption_rejected_with_wal`) can interleave their
/// set/remove calls — one test's `remove_var` lands between another
/// test's `set_var` and the internal `open_runtime` read, producing a
/// "STRATA_ENCRYPTION_KEY environment variable not set" error instead
/// of the codec-mismatch / WAL-unsupported error the test expects.
///
/// Using `std::sync::Mutex` + `Lazy` rather than pulling in
/// `serial_test` keeps the fix local and dependency-free.
static ENV_VAR_TEST_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));

#[test]
#[serial(open_databases)]
fn test_issue_1380_codec_mismatch_rejected() {
    // A database created with "identity" must reject reopen with a different codec.
    // Use Cache mode to bypass the WAL-not-supported guard (encryption works in
    // Cache mode, WAL codec support is pending).
    let _env_guard = ENV_VAR_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let temp_dir = TempDir::new().unwrap();

    // First open with Cache mode: creates MANIFEST with "identity"
    {
        let cfg = StrataConfig {
            durability: "cache".to_string(),
            ..StrataConfig::default()
        };
        let spec = super::spec::OpenSpec::primary(temp_dir.path())
            .with_config(cfg)
            .with_subsystem(crate::search::SearchSubsystem);
        let db = Database::open_runtime(spec).unwrap();
        drop(db);
    }

    // Second open with mismatched codec in Cache mode: must fail with codec mismatch
    // (We need the env var set so get_codec validation passes before hitting the
    // MANIFEST mismatch check)
    std::env::set_var(
        "STRATA_ENCRYPTION_KEY",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    );
    let cfg = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let spec = super::spec::OpenSpec::primary(temp_dir.path())
        .with_config(cfg)
        .with_subsystem(crate::search::SearchSubsystem);
    let result = Database::open_runtime(spec);
    std::env::remove_var("STRATA_ENCRYPTION_KEY");

    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(
                err.contains("codec mismatch"),
                "error should mention codec mismatch: {}",
                err
            );
        }
        Ok(_) => panic!("should reject codec mismatch"),
    }
}

#[test]
#[serial(open_databases)]
fn test_issue_1380_encryption_with_wal_succeeds_as_of_t3_e12() {
    // Pre-T3-E12 (issue #1380) this combination was rejected at open
    // time because the WAL reader did not decode codec-encoded
    // payloads. T3-E12 Phase 2 added the per-record outer envelope
    // and codec-threaded reader, so `aes-gcm-256 + durability =
    // "standard"` now round-trips through WAL recovery. This test is
    // flipped to a positive assertion: the open must succeed and a
    // basic round-trip must work through the codec-aware WAL.
    let _env_guard = ENV_VAR_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let temp_dir = TempDir::new().unwrap();
    std::env::set_var(
        "STRATA_ENCRYPTION_KEY",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    );
    let cfg = StrataConfig {
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let mut cfg_with_durability = cfg;
    cfg_with_durability.durability = "standard".to_string();
    let spec = super::spec::OpenSpec::primary(temp_dir.path())
        .with_config(cfg_with_durability)
        .with_subsystem(crate::search::SearchSubsystem);
    let result = Database::open_runtime(spec);
    std::env::remove_var("STRATA_ENCRYPTION_KEY");

    match result {
        Ok(db) => {
            // Shape assertion: a successful open is the contract;
            // detailed round-trip coverage lives in the new AES-GCM
            // WAL tests added in Phase 2 part 5.
            assert!(!db.is_cache(), "standard-durability DB is not a cache DB");
        }
        Err(e) => panic!(
            "aes-gcm-256 + WAL durability must open successfully post-T3-E12, got: {}",
            e
        ),
    }
}
