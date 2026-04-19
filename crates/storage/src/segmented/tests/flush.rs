use super::*;

// ===== Flush pipeline tests =====

#[test]
fn flush_moves_data_to_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for i in 1..=100u64 {
        seed(
            &store,
            kv_key(&format!("k{:04}", i)),
            Value::Int(i as i64),
            i,
        );
    }

    assert!(store.rotate_memtable(&branch()));
    assert!(store.has_frozen(&branch()));
    assert_eq!(store.branch_frozen_count(&branch()), 1);

    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(flushed);
    assert_eq!(store.branch_frozen_count(&branch()), 0);
    assert_eq!(store.branch_segment_count(&branch()), 1);

    for i in 1..=100u64 {
        let result = store
            .get_versioned(&kv_key(&format!("k{:04}", i)), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn flush_produces_valid_segment_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Segments are now in a branch subdirectory
    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);
    let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .collect();
    assert!(!sst_files.is_empty());

    let seg = crate::segment::KVSegment::open(&sst_files[0].path()).unwrap();
    assert_eq!(seg.entry_count(), 1);
}

#[test]
fn flush_empty_frozen_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("a"), Value::Int(1), 1);
    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(!flushed);
}

#[test]
fn flush_without_dir_is_noop() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    assert!(store.has_frozen(&branch()));

    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(!flushed);
}

#[test]
fn flush_manifest_publish_failure_rolls_back_and_keeps_frozen_memtable() {
    crate::test_hooks::clear_manifest_publish_failure();
    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&branch());

    let err = store.flush_oldest_frozen(&branch()).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    assert_eq!(store.branch_frozen_count(&branch()), 1);
    assert_eq!(store.branch_segment_count(&branch()), 0);
    assert!(store.publish_health().is_none());
}

#[test]
fn flush_dir_fsync_failure_keeps_installed_segment_and_latches_health() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();
    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&branch());

    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(flushed);
    assert_eq!(store.branch_frozen_count(&branch()), 0);
    assert_eq!(store.branch_segment_count(&branch()), 1);
    assert!(store.publish_health().is_some());
}

#[test]
fn rotation_triggers_at_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1);

    seed(&store, kv_key("k"), Value::Int(42), 1);

    assert!(
        store.branch_frozen_count(&branch()) >= 1,
        "rotation should have triggered at 1-byte threshold"
    );
}

#[test]
fn rotation_creates_fresh_active() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1);

    seed(&store, kv_key("old"), Value::Int(1), 1);
    seed(&store, kv_key("new"), Value::Int(2), 2);

    assert_eq!(
        store
            .get_versioned(&kv_key("old"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1),
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("new"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2),
    );
}

#[test]
fn no_rotation_when_threshold_zero() {
    let store = SegmentedStore::new();

    for i in 1..=100u64 {
        seed(&store, kv_key(&format!("k{}", i)), Value::Int(i as i64), i);
    }

    assert_eq!(store.branch_frozen_count(&branch()), 0);
}

#[test]
fn mvcc_correct_across_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("k"), Value::Int(2), 2);

    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1),
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(2))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2),
    );
}

#[test]
fn prefix_scan_spans_memtable_and_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("item/a"), Value::Int(1), 1);
    seed(&store, kv_key("item/b"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("item/c"), Value::Int(3), 3);
    seed(&store, kv_key("item/d"), Value::Int(4), 4);

    let prefix = Key::new(ns(), TypeTag::KV, "item/".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(results.len(), 4);
}

#[test]
fn get_history_spans_memtable_and_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    seed(&store, kv_key("k"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("k"), Value::Int(3), 3);

    let history = store.get_history(&kv_key("k"), None, None).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, Value::Int(3));
    assert_eq!(history[1].value, Value::Int(2));
    assert_eq!(history[2].value, Value::Int(1));
}

#[test]
fn multiple_flushes_produce_multiple_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for cycle in 0..3u64 {
        let base = cycle * 10 + 1;
        for i in 0..10u64 {
            seed(
                &store,
                kv_key(&format!("c{}k{}", cycle, i)),
                Value::Int((base + i) as i64),
                base + i,
            );
        }
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();
    }

    assert_eq!(store.branch_segment_count(&branch()), 3);

    for cycle in 0..3u64 {
        let base = cycle * 10 + 1;
        for i in 0..10u64 {
            let result = store
                .get_versioned(&kv_key(&format!("c{}k{}", cycle, i)), CommitVersion::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(result.value, Value::Int((base + i) as i64));
        }
    }
}

#[test]
fn newest_segment_wins_for_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("k"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    assert_eq!(store.branch_segment_count(&branch()), 2);

    let result = store
        .get_versioned(&kv_key("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(2));
}

#[test]
fn reads_dont_block_during_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    for i in 1..=50u64 {
        store
            .put_with_version_mode(
                kv_key(&format!("k{:04}", i)),
                Value::Int(i as i64),
                CommitVersion(i),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&branch());

    let store_reader = Arc::clone(&store);
    let reader = std::thread::spawn(move || {
        for i in 1..=50u64 {
            let result = store_reader
                .get_versioned(&kv_key(&format!("k{:04}", i)), CommitVersion::MAX)
                .unwrap();
            assert!(result.is_some(), "key k{:04} should be readable", i);
        }
    });

    store.flush_oldest_frozen(&branch()).unwrap();
    reader.join().unwrap();
}

#[test]
fn rotate_nonexistent_branch_returns_false() {
    let store = SegmentedStore::new();
    assert!(!store.rotate_memtable(&branch()));
}

#[test]
fn flush_nonexistent_branch_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(!flushed);
}

#[test]
fn delete_across_flush_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();

    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion(2))
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1),
    );
}

#[test]
fn tombstone_survives_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write value then delete, so the frozen memtable contains both
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Tombstone must survive the flush — key is deleted at snapshot 2
    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion(2))
        .unwrap()
        .is_none());
    // Value is still visible at snapshot 1
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1),
    );
    // History shows both versions from the segment
    let history = store.get_history(&kv_key("k"), None, None).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].value, Value::Null); // tombstone at v2
    assert_eq!(history[1].value, Value::Int(1)); // value at v1
}

// ===== Recovery tests =====

#[test]
fn recover_segments_loads_flushed_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for i in 1..=50u64 {
        seed(
            &store,
            kv_key(&format!("k{:04}", i)),
            Value::Int(i as i64),
            i,
        );
    }
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 1);
    assert_eq!(info.segments_loaded, 1);
    assert!(matches!(info.health, super::RecoveryHealth::Healthy));

    for i in 1..=50u64 {
        let result = store2
            .get_versioned(&kv_key(&format!("k{:04}", i)), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn recover_segments_multiple_branches() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    let b1 = BranchId::from_bytes([1; 16]);
    let b2 = BranchId::from_bytes([2; 16]);
    let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
    let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

    for i in 1..=10u64 {
        let key = Key::new(ns1.clone(), TypeTag::KV, format!("k{}", i).into_bytes());
        store
            .put_with_version_mode(
                key,
                Value::Int(i as i64),
                CommitVersion(i),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&b1);
    store.flush_oldest_frozen(&b1).unwrap();

    for i in 11..=20u64 {
        let key = Key::new(ns2.clone(), TypeTag::KV, format!("k{}", i).into_bytes());
        store
            .put_with_version_mode(
                key,
                Value::Int(i as i64),
                CommitVersion(i),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&b2);
    store.flush_oldest_frozen(&b2).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 2);
    assert_eq!(info.segments_loaded, 2);

    let k1 = Key::new(ns1, TypeTag::KV, "k1".as_bytes().to_vec());
    let k11 = Key::new(ns2, TypeTag::KV, "k11".as_bytes().to_vec());
    assert!(store2
        .get_versioned(&k1, CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store2
        .get_versioned(&k11, CommitVersion::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn recover_segments_skips_corrupt_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let branch_hex = super::hex_encode_branch(&branch());
    let corrupt_path = dir.path().join(&branch_hex).join("corrupt.sst");
    std::fs::write(&corrupt_path, b"not a valid segment").unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 1);
    let corrupt_count = match &info.health {
        super::RecoveryHealth::Degraded { faults, .. } => faults
            .iter()
            .filter(|f| matches!(f, super::RecoveryFault::CorruptSegment { .. }))
            .count(),
        super::RecoveryHealth::Healthy => 0,
    };
    assert_eq!(corrupt_count, 1);
    assert!(store2
        .get_versioned(&kv_key("k"), CommitVersion::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn recover_segments_empty_dir_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 0);
    assert_eq!(info.segments_loaded, 0);
    assert!(matches!(info.health, super::RecoveryHealth::Healthy));
}

#[test]
fn recover_segments_no_dir_is_noop() {
    let store = SegmentedStore::new();
    let info = store.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 0);
}

#[test]
fn recover_segments_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for cycle in 0..3u64 {
        let base = cycle * 10 + 1;
        for i in 0..5u64 {
            seed(
                &store,
                kv_key(&format!("c{}k{}", cycle, i)),
                Value::Int((base + i) as i64),
                base + i,
            );
        }
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();
    }

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 3);

    let branch = store2.branches.get(&branch()).unwrap();
    let ver = branch.version.load();
    assert!(ver.l0_segments()[0].commit_range().1 >= ver.l0_segments()[1].commit_range().1);
    assert!(ver.l0_segments()[1].commit_range().1 >= ver.l0_segments()[2].commit_range().1);
}

#[test]
fn max_flushed_commit_returns_correct_value() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    assert_eq!(store.max_flushed_commit(&branch()), None);

    seed(&store, kv_key("k1"), Value::Int(1), 5);
    seed(&store, kv_key("k2"), Value::Int(2), 10);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    assert_eq!(store.max_flushed_commit(&branch()), Some(CommitVersion(10)));

    seed(&store, kv_key("k3"), Value::Int(3), 20);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    assert_eq!(store.max_flushed_commit(&branch()), Some(CommitVersion(20)));
}

#[test]
fn flush_writes_to_branch_subdirectory() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);
    assert!(branch_dir.exists());

    let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|e| e.to_str()) == Some("sst"))
        .collect();
    assert_eq!(sst_files.len(), 1);
}

#[test]
fn frozen_memtable_reads_correct_order() {
    let store = SegmentedStore::new();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    seed(&store, kv_key("k"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    seed(&store, kv_key("k"), Value::Int(3), 3);

    assert_eq!(store.branch_frozen_count(&branch()), 2);
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3),
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(2))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2),
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1),
    );
}

// ===== SE2 recovery-fault regression tests =====

#[test]
fn recover_corrupt_segment_produces_fault() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Seed one good segment
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Drop a garbage "segment" file next to the real one
    let branch_hex = super::hex_encode_branch(&branch());
    let corrupt_path = dir.path().join(&branch_hex).join("99999.sst");
    std::fs::write(&corrupt_path, b"not a valid segment").unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();

    match &outcome.health {
        super::RecoveryHealth::Degraded { faults, class } => {
            assert_eq!(*class, super::DegradationClass::DataLoss);
            let corrupt = faults.iter().find(|f| {
                matches!(
                    f,
                    super::RecoveryFault::CorruptSegment { file, .. } if file == &corrupt_path
                )
            });
            assert!(
                corrupt.is_some(),
                "expected CorruptSegment for {corrupt_path:?}"
            );
        }
        other => panic!("expected Degraded, got {other:?}"),
    }

    // Good segment still loads.
    assert!(store2
        .get_versioned(&kv_key("k"), CommitVersion::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn recover_missing_manifest_listed_produces_fault() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Capture the on-disk segment filename before dropping the store.
    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);
    let sst_name = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .expect("expected exactly one .sst file")
        .file_name()
        .to_str()
        .unwrap()
        .to_string();

    drop(store);

    // Delete the real .sst but leave the manifest (which still references it).
    std::fs::remove_file(branch_dir.join(&sst_name)).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();

    match &outcome.health {
        super::RecoveryHealth::Degraded { faults, class } => {
            assert_eq!(*class, super::DegradationClass::DataLoss);
            let missing = faults.iter().find(|f| {
                matches!(
                    f,
                    super::RecoveryFault::MissingManifestListed { file, .. } if file == &sst_name
                )
            });
            assert!(
                missing.is_some(),
                "expected MissingManifestListed fault for {sst_name}"
            );
        }
        other => panic!("expected Degraded, got {other:?}"),
    }
}

#[test]
fn recover_corrupt_manifest_listed_segment_is_not_reported_missing() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    drop(store);

    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);
    let listed_sst = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .expect("expected one manifest-listed .sst")
        .path();
    std::fs::write(&listed_sst, b"not a valid segment anymore").unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();

    match &outcome.health {
        super::RecoveryHealth::Degraded { faults, class } => {
            assert_eq!(*class, super::DegradationClass::DataLoss);
            let corrupt_count = faults
                .iter()
                .filter(|f| matches!(f, super::RecoveryFault::CorruptSegment { .. }))
                .count();
            let missing_count = faults
                .iter()
                .filter(|f| matches!(f, super::RecoveryFault::MissingManifestListed { .. }))
                .count();
            assert_eq!(
                corrupt_count, 1,
                "listed corrupt segment should surface exactly one CorruptSegment fault"
            );
            assert_eq!(
                missing_count, 0,
                "listed corrupt segment exists on disk and must not be misreported as missing"
            );
        }
        other => panic!("expected Degraded, got {other:?}"),
    }
    assert_eq!(
        outcome.branches_recovered, 0,
        "branch with no loadable own or inherited segments must not be counted as recovered"
    );
}

#[test]
fn recover_rebuilds_branch_max_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Seed segments whose commit versions span [1, 50].
    for i in 1..=50u64 {
        seed(
            &store,
            kv_key(&format!("k{:04}", i)),
            Value::Int(i as i64),
            i,
        );
    }
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();

    // Version floor must reflect the highest commit id in the segments.
    assert_eq!(outcome.version_floor, CommitVersion(50));
    // Per-branch version map must carry the same for this branch.
    assert_eq!(
        outcome.branch_versions.get(&branch()).copied(),
        Some(CommitVersion(50))
    );
    // Critically: the `BranchState::max_version` atomic must have been
    // rebuilt. Pre-SE2 this stayed at the constructor default (0),
    // causing post-restart `fork_branch` to under-claim the fork source
    // version (SG-008). A weaker "segments have the right commit range"
    // check would not catch a regression where the atomic is left at 0.
    let b = store2.branches.get(&branch()).unwrap();
    assert_eq!(
        b.max_version.load(std::sync::atomic::Ordering::Acquire),
        50,
        "branch.max_version must be rebuilt from loaded segment commit ids"
    );
    assert!(matches!(outcome.health, super::RecoveryHealth::Healthy));
}

#[test]
fn last_recovery_health_tracks_classification_across_calls() {
    // Degraded recovery first: last_recovery_health() must classify as
    // DataLoss (corrupt segment). Then a clean recovery on an empty dir
    // must reset last_recovery_health() to Healthy.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Drop a garbage .sst to trigger a CorruptSegment fault on next recovery.
    let branch_hex = super::hex_encode_branch(&branch());
    let corrupt_path = dir.path().join(&branch_hex).join("77777.sst");
    std::fs::write(&corrupt_path, b"garbage").unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    // Pre-recovery: accessor must return Healthy (nothing has run yet).
    assert!(matches!(
        &*store2.last_recovery_health(),
        super::RecoveryHealth::Healthy
    ));
    let outcome = store2.recover_segments().unwrap();
    // Post-recovery: accessor reflects the classified outcome.
    match (&outcome.health, &*store2.last_recovery_health()) {
        (
            super::RecoveryHealth::Degraded {
                faults: outcome_faults,
                class: outcome_class,
            },
            super::RecoveryHealth::Degraded {
                faults: stored_faults,
                class: stored_class,
            },
        ) => {
            assert_eq!(*outcome_class, super::DegradationClass::DataLoss);
            assert_eq!(*stored_class, super::DegradationClass::DataLoss);
            assert_eq!(
                outcome_faults.len(),
                stored_faults.len(),
                "stored recovery health must preserve the same fault count as the returned outcome"
            );
            assert_eq!(
                *outcome_class, *stored_class,
                "stored recovery health must preserve the same degradation class"
            );
            assert!(
                stored_faults.iter().any(|f| matches!(
                    f,
                    super::RecoveryFault::CorruptSegment { file, .. } if file == &corrupt_path
                )),
                "stored recovery health must preserve the original CorruptSegment fault"
            );
        }
        other => panic!("expected matching Degraded outcomes, got {other:?}"),
    }

    // A fresh store pointed at a clean directory must reset the accessor to
    // Healthy after a successful recovery — the field is rewritten per call.
    let clean = tempfile::tempdir().unwrap();
    let store3 = SegmentedStore::with_dir(clean.path().to_path_buf(), 0);
    store3.recover_segments().unwrap();
    assert!(matches!(
        &*store3.last_recovery_health(),
        super::RecoveryHealth::Healthy
    ));
}

#[test]
fn last_recovery_health_updates_on_prewalk_hard_error() {
    let dir = tempfile::tempdir().unwrap();
    let not_a_dir = dir.path().join("segments-file");
    std::fs::write(&not_a_dir, b"not a directory").unwrap();

    let store = SegmentedStore::with_dir(not_a_dir.clone(), 0);
    assert!(matches!(
        &*store.last_recovery_health(),
        super::RecoveryHealth::Healthy
    ));

    let err = store
        .recover_segments()
        .expect_err("top-level read_dir failure should still return Err");
    let returned_kind = err.kind();

    match &*store.last_recovery_health() {
        super::RecoveryHealth::Degraded { faults, class } => {
            assert_eq!(
                *class,
                super::DegradationClass::DataLoss,
                "pre-walk I/O failures should publish degraded recovery health"
            );
            let io_fault = faults
                .iter()
                .find_map(|fault| match fault {
                    super::RecoveryFault::Io(inner) => Some(inner),
                    _ => None,
                })
                .expect("stored recovery health should preserve the pre-walk I/O failure");
            assert_eq!(
                io_fault.kind(),
                returned_kind,
                "stored recovery health should preserve the returned error kind"
            );
            assert!(
                io_fault
                    .to_string()
                    .contains(not_a_dir.to_string_lossy().as_ref()),
                "stored recovery health should identify the segments path that failed"
            );
        }
        other => panic!("expected degraded health after pre-walk error, got {other:?}"),
    }
}
