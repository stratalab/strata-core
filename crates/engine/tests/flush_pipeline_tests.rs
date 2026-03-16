//! Integration test: BackgroundScheduler + SegmentedStore flush pipeline.
//!
//! Verifies that the scheduler can drive memtable flushes as background tasks.

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

#[test]
fn scheduler_flushes_after_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Write data
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

    // Rotate memtable
    store.rotate_memtable(&branch());
    assert!(store.has_frozen(&branch()));
    assert_eq!(store.branch_segment_count(&branch()), 0);

    // Submit flush job to the scheduler
    let scheduler = BackgroundScheduler::new(2, 4096);
    let store_clone = Arc::clone(&store);
    let bid = branch();
    scheduler
        .submit(TaskPriority::High, move || {
            store_clone.flush_oldest_frozen(&bid).unwrap();
        })
        .unwrap();

    // Wait for flush to complete
    scheduler.drain();
    scheduler.shutdown();

    // Verify: frozen cleared, segment created, data readable
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
