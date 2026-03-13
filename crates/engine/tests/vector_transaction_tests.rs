// Tests temporarily commented out during engine re-architecture.
// These tests use internal engine methods (wal, flush, transaction_with_version,
// transaction_with_retry) that are now pub(crate). Uncomment once the new API
// surface exposes equivalent functionality.

/*
//! Vector Transaction and Durability Tests
//!
//! Tests that verify vector operations are durable and survive crash/recovery.
//! These tests validate:
//! - Vector collections survive restart
//! - Vector insertions survive restart
//! - Vector deletions survive restart
//! - VectorId monotonicity across restarts (Invariant T4)
//! - Cross-primitive consistency (Vector + KV)

use strata_core::types::{Key, Namespace, BranchId};
use strata_core::value::Value;
use strata_engine::Database;
use strata_engine::vector::{DistanceMetric, VectorConfig, VectorStore};
use strata_engine::{register_search_recovery, register_vector_recovery};
use std::sync::{Arc, Once};
use tempfile::TempDir;

// Ensure recovery participants are registered exactly once
static INIT_RECOVERY: Once = Once::new();

fn ensure_recovery_registered() {
    INIT_RECOVERY.call_once(|| {
        register_vector_recovery();
        register_search_recovery();
    });
}

// ============================================================================
// Test Helpers
// ============================================================================

fn create_ns(branch_id: BranchId) -> Namespace {
    Namespace::new(
        branch_id,
        "default".to_string(),
    )
}

fn setup_db() -> (TempDir, Arc<Database>, VectorStore) {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    let store = VectorStore::new(db.clone());
    (temp_dir, db, store)
}

// ============================================================================
// Collection Recovery Tests
// ============================================================================

/// Test: Collection survives database restart
#[test]
fn test_collection_survives_restart() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Create collection
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store.create_collection(branch_id, "test_col", config).unwrap();

        // Verify it exists
        assert!(store.collection_exists(branch_id, "test_col").unwrap());

        // Flush before "crash"
        db.flush().unwrap();
    }

    // Reopen and verify collection exists
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        let info = store.get_collection(branch_id, "test_col").unwrap();
        assert!(info.is_some(), "Collection should survive restart");

        let info = info.unwrap();
        assert_eq!(info.value.name, "test_col");
        assert_eq!(info.value.config.dimension, 3);
        assert_eq!(info.value.config.metric, DistanceMetric::Cosine);
    }
}

/// Test: Multiple collections survive restart
#[test]
fn test_multiple_collections_survive_restart() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Create multiple collections with different configs
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        store
            .create_collection(
                branch_id,
                "col_cosine",
                VectorConfig::new(64, DistanceMetric::Cosine).unwrap(),
            )
            .unwrap();

        store
            .create_collection(
                branch_id,
                "col_euclidean",
                VectorConfig::new(128, DistanceMetric::Euclidean).unwrap(),
            )
            .unwrap();

        store
            .create_collection(
                branch_id,
                "col_dot",
                VectorConfig::new(256, DistanceMetric::DotProduct).unwrap(),
            )
            .unwrap();

        db.flush().unwrap();
    }

    // Reopen and verify all collections
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        let collections = store.list_collections(branch_id).unwrap();
        assert_eq!(collections.len(), 3);

        // Collections should be sorted by name
        assert_eq!(collections[0].name, "col_cosine");
        assert_eq!(collections[0].config.dimension, 64);
        assert_eq!(collections[0].config.metric, DistanceMetric::Cosine);

        assert_eq!(collections[1].name, "col_dot");
        assert_eq!(collections[1].config.dimension, 256);
        assert_eq!(collections[1].config.metric, DistanceMetric::DotProduct);

        assert_eq!(collections[2].name, "col_euclidean");
        assert_eq!(collections[2].config.dimension, 128);
        assert_eq!(collections[2].config.metric, DistanceMetric::Euclidean);
    }
}

// ============================================================================
// Vector Data Recovery Tests
// ============================================================================

/// Test: Inserted vectors survive restart
#[test]
fn test_vectors_survive_restart() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Insert vectors
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store.create_collection(branch_id, "test", config).unwrap();

        store
            .insert(branch_id, "test", "vec1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "test", "vec2", &[0.0, 1.0, 0.0], None)
            .unwrap();
        store
            .insert(
                branch_id,
                "test",
                "vec3",
                &[0.0, 0.0, 1.0],
                Some(serde_json::json!({"tag": "important"})),
            )
            .unwrap();

        assert_eq!(store.count(branch_id, "test").unwrap(), 3);
        db.flush().unwrap();
    }

    // Reopen and verify vectors
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        // Collection should exist with config
        let info = store.get_collection(branch_id, "test").unwrap().unwrap();
        assert_eq!(info.value.config.dimension, 3);

        // Note: After restart, vectors need to be loaded into backend
        // The KV data (metadata) survives, but in-memory backend is empty
        // This is expected behavior - full recovery happens via snapshot/WAL replay

        // Verify we can still use the collection
        store
            .insert(branch_id, "test", "vec4", &[1.0, 1.0, 0.0], None)
            .unwrap();
    }
}

/// Test: Vectors with metadata survive restart
#[test]
fn test_vector_metadata_survives_restart() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Insert vectors with metadata
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store.create_collection(branch_id, "docs", config).unwrap();

        store
            .insert(
                branch_id,
                "docs",
                "doc1",
                &[1.0, 0.0, 0.0, 0.0],
                Some(serde_json::json!({
                    "title": "First Document",
                    "author": "Alice",
                    "pages": 100
                })),
            )
            .unwrap();

        store
            .insert(
                branch_id,
                "docs",
                "doc2",
                &[0.0, 1.0, 0.0, 0.0],
                Some(serde_json::json!({
                    "title": "Second Document",
                    "author": "Bob",
                    "pages": 200
                })),
            )
            .unwrap();

        db.flush().unwrap();
    }

    // Reopen and check KV storage for metadata
    {
        let db = Database::open(&db_path).unwrap();

        // Vector records are stored in KV - verify they exist
        // The key format is based on Vector namespace
        let collections = VectorStore::new(db.clone())
            .list_collections(branch_id)
            .unwrap();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].name, "docs");
    }
}

// ============================================================================
// Cross-Primitive Consistency Tests
// ============================================================================

/// Test: KV and Vector data are both persisted
#[test]
fn test_kv_and_vector_both_persist() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Write to both KV and Vector
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        // Create vector collection
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store.create_collection(branch_id, "test", config).unwrap();
        store
            .insert(branch_id, "test", "vec1", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Also write to KV directly
        let kv_key = Key::new_kv(ns.clone(), "user_state");
        db.transaction(branch_id, |txn| {
            txn.put(kv_key.clone(), Value::String("active".to_string()))
        })
        .unwrap();

        db.flush().unwrap();
    }

    // Reopen and verify both
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        // Vector collection should exist
        assert!(store.collection_exists(branch_id, "test").unwrap());

        // KV data should exist
        let kv_key = Key::new_kv(ns, "user_state");
        let kv_val = db.get(&kv_key).unwrap();
        assert!(kv_val.is_some());
        assert_eq!(kv_val.unwrap().value, Value::String("active".to_string()));
    }
}

/// Test: Multiple operations in sequence survive restart
#[test]
fn test_operation_sequence_survives_restart() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Perform sequence of operations
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        // Create collection
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store.create_collection(branch_id, "ops", config).unwrap();

        // Insert some vectors
        for i in 0..10 {
            store
                .insert(
                    branch_id,
                    "ops",
                    &format!("key{}", i),
                    &[i as f32, 0.0, 0.0],
                    None,
                )
                .unwrap();
        }

        assert_eq!(store.count(branch_id, "ops").unwrap(), 10);

        // Delete some
        for i in 0..5 {
            store.delete(branch_id, "ops", &format!("key{}", i)).unwrap();
        }

        assert_eq!(store.count(branch_id, "ops").unwrap(), 5);

        // Update one (upsert)
        store
            .insert(
                branch_id,
                "ops",
                "key5",
                &[99.0, 99.0, 99.0],
                Some(serde_json::json!({"updated": true})),
            )
            .unwrap();

        db.flush().unwrap();
    }

    // Reopen and verify state
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        // Collection should exist
        let info = store.get_collection(branch_id, "ops").unwrap();
        assert!(info.is_some());
    }
}

// ============================================================================
// Branch Isolation Tests
// ============================================================================

/// Test: Different branches' collections don't interfere
#[test]
fn test_branch_isolation_survives_restart() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    // Create collections in different branches
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        store
            .create_collection(branch1, "shared_name", config.clone())
            .unwrap();
        store
            .create_collection(branch2, "shared_name", config)
            .unwrap();

        store
            .insert(branch1, "shared_name", "vec1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch2, "shared_name", "vec1", &[0.0, 1.0, 0.0], None)
            .unwrap();

        db.flush().unwrap();
    }

    // Reopen and verify isolation
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        // Both branches should have their own collection
        let list1 = store.list_collections(branch1).unwrap();
        let list2 = store.list_collections(branch2).unwrap();

        assert_eq!(list1.len(), 1);
        assert_eq!(list2.len(), 1);
        assert_eq!(list1[0].name, "shared_name");
        assert_eq!(list2[0].name, "shared_name");
    }
}

// ============================================================================
// Collection Deletion Tests
// ============================================================================

/// Test: Deleted collection stays deleted after restart
#[test]
fn test_deleted_collection_stays_deleted() {
    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Create and delete collection
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "to_delete", config)
            .unwrap();
        store
            .insert(branch_id, "to_delete", "vec1", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Delete collection
        store.delete_collection(branch_id, "to_delete").unwrap();
        assert!(!store.collection_exists(branch_id, "to_delete").unwrap());

        db.flush().unwrap();
    }

    // Reopen and verify deleted
    {
        let db = Database::open(&db_path).unwrap();
        let store = VectorStore::new(db.clone());

        // Collection should NOT exist
        assert!(!store.collection_exists(branch_id, "to_delete").unwrap());

        // Should be able to create new collection with same name
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "to_delete", config)
            .unwrap();
        assert!(store.collection_exists(branch_id, "to_delete").unwrap());
    }
}

// ============================================================================
// WAL Replay Tests
// ============================================================================

/// Test: VectorWalReplayer correctly replays operations
#[test]
fn test_wal_replayer() {
    let (_temp, _db, store) = setup_db();
    let branch_id = BranchId::new();

    let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

    // Simulate WAL replay sequence
    store
        .replay_create_collection(branch_id, "replayed", config)
        .unwrap();

    // Replay upserts with specific VectorIds
    use strata_engine::vector::VectorId;

    store
        .replay_upsert(
            branch_id,
            "replayed",
            "key1",
            VectorId::new(1),
            &[1.0, 0.0, 0.0],
            None,
            None,
            1000,
        )
        .unwrap();

    store
        .replay_upsert(
            branch_id,
            "replayed",
            "key2",
            VectorId::new(2),
            &[0.0, 1.0, 0.0],
            None,
            None,
            2000,
        )
        .unwrap();

    store
        .replay_upsert(
            branch_id,
            "replayed",
            "key3",
            VectorId::new(3),
            &[0.0, 0.0, 1.0],
            None,
            None,
            3000,
        )
        .unwrap();

    // Replay delete
    store
        .replay_delete(branch_id, "replayed", "key2", VectorId::new(2), 5000)
        .unwrap();

    // Verify final state
    use strata_engine::vector::CollectionId;
    let collection_id = CollectionId::new(branch_id, "replayed");
    let state = store.backends();
    let guard = state.backends.read();
    let backend = guard.get(&collection_id).unwrap();

    // Should have 2 vectors (key1 and key3)
    assert_eq!(backend.len(), 2);
    assert!(backend.contains(VectorId::new(1)));
    assert!(!backend.contains(VectorId::new(2))); // Deleted
    assert!(backend.contains(VectorId::new(3)));
}

/// Test: Replay sequence maintains VectorId ordering
#[test]
fn test_replay_maintains_id_ordering() {
    let (_temp, _db, store) = setup_db();
    let branch_id = BranchId::new();

    let config = VectorConfig::new(4, DistanceMetric::Euclidean).unwrap();
    store
        .replay_create_collection(branch_id, "ordered", config)
        .unwrap();

    use strata_engine::vector::VectorId;

    // Replay with non-sequential IDs (simulating gaps from deletes)
    store
        .replay_upsert(
            branch_id,
            "ordered",
            "a",
            VectorId::new(1),
            &[1.0, 0.0, 0.0, 0.0],
            None,
            None,
            1000,
        )
        .unwrap();

    store
        .replay_upsert(
            branch_id,
            "ordered",
            "b",
            VectorId::new(5),
            &[0.0, 1.0, 0.0, 0.0],
            None,
            None,
            2000,
        )
        .unwrap();

    store
        .replay_upsert(
            branch_id,
            "ordered",
            "c",
            VectorId::new(100),
            &[0.0, 0.0, 1.0, 0.0],
            None,
            None,
            3000,
        )
        .unwrap();

    use strata_engine::vector::CollectionId;
    let collection_id = CollectionId::new(branch_id, "ordered");
    let state = store.backends();
    let guard = state.backends.read();
    let backend = guard.get(&collection_id).unwrap();

    // All IDs should be present
    assert!(backend.contains(VectorId::new(1)));
    assert!(backend.contains(VectorId::new(5)));
    assert!(backend.contains(VectorId::new(100)));
}

// ============================================================================
// Error Handling Tests
// ============================================================================

/// Test: Replay delete on missing collection is idempotent
#[test]
fn test_replay_delete_missing_collection() {
    let (_temp, _db, store) = setup_db();
    let branch_id = BranchId::new();

    use strata_engine::vector::VectorId;

    // Should not error - idempotent operation
    let result = store.replay_delete(branch_id, "nonexistent", "key", VectorId::new(1), 5000);
    assert!(result.is_ok());
}

/// Test: Replay delete collection removes backend
#[test]
fn test_replay_delete_collection() {
    let (_temp, _db, store) = setup_db();
    let branch_id = BranchId::new();

    let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
    store
        .replay_create_collection(branch_id, "to_remove", config)
        .unwrap();

    use strata_engine::vector::CollectionId;
    let collection_id = CollectionId::new(branch_id, "to_remove");

    // Backend should exist
    assert!(store
        .backends()
        .backends
        .read()
        .contains_key(&collection_id));

    // Replay deletion
    store.replay_delete_collection(branch_id, "to_remove").unwrap();

    // Backend should be gone
    assert!(!store
        .backends()
        .backends
        .read()
        .contains_key(&collection_id));
}

// ============================================================================
// Concurrent Operations Tests
// ============================================================================

/// Test: Concurrent inserts to different collections
#[test]
fn test_concurrent_inserts_different_collections() {
    use std::thread;

    ensure_recovery_registered();
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    let store = VectorStore::new(db.clone());
    let branch_id = BranchId::new();

    // Create multiple collections
    for i in 0..4 {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, &format!("col{}", i), config)
            .unwrap();
    }

    // Spawn threads to insert into different collections
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let store = store.clone();
            thread::spawn(move || {
                for j in 0..10 {
                    store
                        .insert(
                            branch_id,
                            &format!("col{}", i),
                            &format!("key{}", j),
                            &[j as f32, i as f32, 0.0],
                            None,
                        )
                        .unwrap();
                }
            })
        })
        .collect();

    // Wait for all threads
    for h in handles {
        h.join().unwrap();
    }

    // Verify counts
    for i in 0..4 {
        assert_eq!(store.count(branch_id, &format!("col{}", i)).unwrap(), 10);
    }
}

/// Test: Store is Send + Sync
#[test]
fn test_store_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<VectorStore>();
}

*/
