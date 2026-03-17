//! Integration test: BackgroundScheduler + SegmentedStore flush pipeline.
//!
//! Verifies that the scheduler can drive memtable flushes as background tasks,
//! and that the full write → flush → recover lifecycle produces correct results.

use std::sync::Arc;

use strata_engine::background::{BackgroundScheduler, TaskPriority};
use strata_storage::segmented::SegmentedStore;

use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::value::Value;

fn branch() -> BranchId {
    BranchId::from_bytes([1; 16])
}

fn ns() -> Arc<Namespace> {
    Arc::new(Namespace::new(branch(), "default".to_string()))
}

fn kv_key(name: &str) -> Key {
    Key::new(ns(), TypeTag::KV, name.as_bytes().to_vec())
}

fn seed(store: &SegmentedStore, key: Key, value: Value, version: u64) {
    store
        .put_with_version_mode(key, value, version, None, WriteMode::Append)
        .unwrap();
}

#[test]
fn scheduler_flushes_after_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    for i in 1..=50u64 {
        store
            .put_with_version_mode(
                kv_key(&format!("k{:04}", i)),
                Value::Int(i as i64),
                i,
                None,
                WriteMode::Append,
            )
            .unwrap();
    }

    store.rotate_memtable(&branch());
    assert!(store.has_frozen(&branch()));
    assert_eq!(store.branch_segment_count(&branch()), 0);

    let scheduler = BackgroundScheduler::new(2, 4096);
    let store_clone = Arc::clone(&store);
    let bid = branch();
    scheduler
        .submit(TaskPriority::High, move || {
            store_clone.flush_oldest_frozen(&bid).unwrap();
        })
        .unwrap();

    scheduler.drain();
    scheduler.shutdown();

    assert_eq!(store.branch_frozen_count(&branch()), 0);
    assert_eq!(store.branch_segment_count(&branch()), 1);

    for i in 1..=50u64 {
        let result = store
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

// ============================================================================
// Epic 5: Durability lifecycle tests
// ============================================================================

use parking_lot::Mutex;
use strata_durability::codec::IdentityCodec;
use strata_durability::format::{ManifestManager, WalRecord};
use strata_durability::wal::{WalConfig, WalReader, WalWriter};
use strata_durability::DurabilityMode;

fn test_uuid() -> [u8; 16] {
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
}

/// Helper: write entries to both the SegmentedStore and WAL.
fn write_entries(store: &SegmentedStore, writer: &mut WalWriter, start: u64, end: u64) {
    for i in start..=end {
        let key = kv_key(&format!("k{:04}", i));
        seed(store, key, Value::Int(i as i64), i);

        let record = WalRecord::new(
            i,
            *branch().as_bytes(),
            i * 1000,
            format!("k{:04}={}", i, i).into_bytes(),
        );
        writer.append(&record).unwrap();
    }
}

#[test]
fn lifecycle_write_flush_recover_all_data_correct() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("WAL");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let manifest_path = dir.path().join("MANIFEST");
    let segments_dir = dir.path().join("segments");

    let mut manifest_mgr =
        ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
            .unwrap();
    let mut writer = WalWriter::new(
        wal_dir.clone(),
        test_uuid(),
        DurabilityMode::Always,
        WalConfig::default(),
        Box::new(IdentityCodec),
    )
    .unwrap();

    let store = SegmentedStore::with_dir(segments_dir.clone(), 0);
    write_entries(&store, &mut writer, 1, 100);

    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let max_commit = store.max_flushed_commit(&branch()).unwrap();
    manifest_mgr.set_flush_watermark(max_commit).unwrap();

    drop(store);
    drop(writer);

    // Recovery
    let store2 = SegmentedStore::with_dir(segments_dir, 0);
    let manifest_mgr2 = ManifestManager::load(manifest_path).unwrap();
    let flush_watermark = manifest_mgr2.manifest().flushed_through_commit_id.unwrap();
    assert_eq!(flush_watermark, max_commit);

    let info = store2.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 1);
    assert_eq!(info.segments_loaded, 1);

    let reader = WalReader::new();
    let delta_records = reader
        .read_all_after_watermark(&wal_dir, flush_watermark)
        .unwrap();
    assert!(delta_records.is_empty());

    for i in 1..=100u64 {
        let result = store2
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn lifecycle_crash_before_flush() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("WAL");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let manifest_path = dir.path().join("MANIFEST");
    let segments_dir = dir.path().join("segments");

    let _manifest_mgr =
        ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
            .unwrap();
    let mut writer = WalWriter::new(
        wal_dir.clone(),
        test_uuid(),
        DurabilityMode::Always,
        WalConfig::default(),
        Box::new(IdentityCodec),
    )
    .unwrap();

    let store = SegmentedStore::with_dir(segments_dir.clone(), 0);
    write_entries(&store, &mut writer, 1, 50);

    drop(store);
    drop(writer);

    let store2 = SegmentedStore::with_dir(segments_dir, 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 0);

    let manifest_mgr2 = ManifestManager::load(manifest_path).unwrap();
    assert_eq!(manifest_mgr2.manifest().flushed_through_commit_id, None);

    let reader = WalReader::new();
    let records = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
    assert_eq!(records.len(), 50);

    for record in &records {
        let key_val = String::from_utf8_lossy(&record.writeset);
        if let Some((k, v)) = key_val.split_once('=') {
            let key = kv_key(k);
            let val: i64 = v.parse().unwrap();
            seed(&store2, key, Value::Int(val), record.txn_id);
        }
    }

    for i in 1..=50u64 {
        let result = store2
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn lifecycle_crash_after_segment_before_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("WAL");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let manifest_path = dir.path().join("MANIFEST");
    let segments_dir = dir.path().join("segments");

    let _manifest_mgr =
        ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
            .unwrap();
    let mut writer = WalWriter::new(
        wal_dir.clone(),
        test_uuid(),
        DurabilityMode::Always,
        WalConfig::default(),
        Box::new(IdentityCodec),
    )
    .unwrap();

    let store = SegmentedStore::with_dir(segments_dir.clone(), 0);
    write_entries(&store, &mut writer, 1, 50);

    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    // DON'T update manifest — simulates crash between segment write and manifest update

    drop(store);
    drop(writer);

    let store2 = SegmentedStore::with_dir(segments_dir, 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 1);

    let manifest_mgr2 = ManifestManager::load(manifest_path).unwrap();
    assert_eq!(manifest_mgr2.manifest().flushed_through_commit_id, None);

    // Full WAL replay (watermark 0 = replay everything)
    let reader = WalReader::new();
    let records = reader.read_all_after_watermark(&wal_dir, 0).unwrap();

    for record in &records {
        let key_val = String::from_utf8_lossy(&record.writeset);
        if let Some((k, v)) = key_val.split_once('=') {
            let key = kv_key(k);
            let val: i64 = v.parse().unwrap();
            seed(&store2, key, Value::Int(val), record.txn_id);
        }
    }

    // Data readable via segment + memtable (MVCC dedup handles overlap)
    for i in 1..=50u64 {
        let result = store2
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn lifecycle_multiple_flush_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("WAL");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let manifest_path = dir.path().join("MANIFEST");
    let segments_dir = dir.path().join("segments");

    let mut manifest_mgr =
        ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
            .unwrap();
    let mut writer = WalWriter::new(
        wal_dir.clone(),
        test_uuid(),
        DurabilityMode::Always,
        WalConfig::default(),
        Box::new(IdentityCodec),
    )
    .unwrap();

    let store = SegmentedStore::with_dir(segments_dir.clone(), 0);

    for cycle in 0..3u64 {
        let start = cycle * 30 + 1;
        let end = start + 29;
        write_entries(&store, &mut writer, start, end);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        let max_commit = store.max_flushed_commit(&branch()).unwrap();
        manifest_mgr.set_flush_watermark(max_commit).unwrap();
    }

    drop(store);
    drop(writer);

    let store2 = SegmentedStore::with_dir(segments_dir, 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 3);

    let manifest_mgr2 = ManifestManager::load(manifest_path).unwrap();
    let flush_wm = manifest_mgr2.manifest().flushed_through_commit_id.unwrap();

    let reader = WalReader::new();
    let delta = reader.read_all_after_watermark(&wal_dir, flush_wm).unwrap();
    assert!(delta.is_empty());

    for i in 1..=90u64 {
        let result = store2
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn lifecycle_wal_truncation_after_flush() {
    use strata_durability::compaction::WalOnlyCompactor;

    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("WAL");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let manifest_path = dir.path().join("MANIFEST");
    let segments_dir = dir.path().join("segments");

    let manifest_mgr =
        ManifestManager::create(manifest_path.clone(), test_uuid(), "identity".to_string())
            .unwrap();
    let manifest = Arc::new(Mutex::new(manifest_mgr));

    let mut writer = WalWriter::new(
        wal_dir.clone(),
        test_uuid(),
        DurabilityMode::Always,
        WalConfig::default(),
        Box::new(IdentityCodec),
    )
    .unwrap();

    let store = SegmentedStore::with_dir(segments_dir.clone(), 0);
    write_entries(&store, &mut writer, 1, 50);

    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let max_commit = store.max_flushed_commit(&branch()).unwrap();

    {
        let mut m = manifest.lock();
        m.set_flush_watermark(max_commit).unwrap();
        m.set_active_segment(999).unwrap();
    }

    let compactor = WalOnlyCompactor::new(wal_dir.clone(), Arc::clone(&manifest));
    let compact_info = compactor.compact().unwrap();

    drop(store);
    drop(writer);

    let store2 = SegmentedStore::with_dir(segments_dir, 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 1);

    let manifest_mgr2 = ManifestManager::load(manifest_path).unwrap();
    let flush_wm = manifest_mgr2.manifest().flushed_through_commit_id.unwrap();
    let reader = WalReader::new();
    let delta = reader.read_all_after_watermark(&wal_dir, flush_wm).unwrap();

    for record in &delta {
        let key_val = String::from_utf8_lossy(&record.writeset);
        if let Some((k, v)) = key_val.split_once('=') {
            let key = kv_key(k);
            let val: i64 = v.parse().unwrap();
            seed(&store2, key, Value::Int(val), record.txn_id);
        }
    }

    for i in 1..=50u64 {
        let result = store2
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }

    assert_eq!(compact_info.snapshot_watermark, Some(max_commit));
}

#[test]
fn multi_level_compaction_cascades() {
    let dir = tempfile::tempdir().unwrap();
    let segments_dir = dir.path().join("segments");
    let store = SegmentedStore::with_dir(segments_dir, 0);
    let b = branch();

    // Flush several L0 segments, then compact L0 → L1
    for commit in 1..=5u64 {
        let key = kv_key(&format!("k{:04}", commit));
        seed(&store, key, Value::Int(commit as i64), commit);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }
    assert_eq!(store.l0_segment_count(&b), 5);

    // L0 → L1
    let r = store.compact_level(&b, 0, 0).unwrap().unwrap();
    assert_eq!(r.segments_merged, 5);
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.level_segment_count(&b, 1), 1);

    // L1 → L2 (trivial move — single file, no overlap in L2)
    let r = store.compact_level(&b, 1, 0).unwrap().unwrap();
    assert_eq!(r.segments_merged, 1);
    assert_eq!(r.output_entries, 5);
    assert_eq!(r.entries_pruned, 0);
    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert_eq!(store.level_segment_count(&b, 2), 1);

    // L2 → L3 (trivial move)
    let r = store.compact_level(&b, 2, 0).unwrap().unwrap();
    assert_eq!(r.segments_merged, 1);
    assert_eq!(r.output_entries, 5);
    assert_eq!(r.entries_pruned, 0);
    assert_eq!(store.level_segment_count(&b, 2), 0);
    assert_eq!(store.level_segment_count(&b, 3), 1);

    // All data still readable after cascading through levels
    for i in 1..=5u64 {
        let result = store
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("key k{:04} missing after cascade", i));
        assert_eq!(result.value, Value::Int(i as i64));
    }
}
