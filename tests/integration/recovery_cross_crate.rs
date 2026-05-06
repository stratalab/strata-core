//! Engine Recovery Tests
//!
//! Tests that span engine-owned graph and vector subsystems.
//! These tests were moved from crates/engine/tests/recovery_tests.rs to
//! eliminate engine's dev-dependency on vector and graph shell crates
//! per T1-E4 boundary requirements.

use crate::common::*;
use std::sync::Arc;
use tempfile::TempDir;

// ============================================================================
// Helper functions
// ============================================================================

fn setup() -> (Arc<Database>, TempDir, BranchId) {
    let temp_dir = TempDir::new().unwrap();
    let db = test_db_open(temp_dir.path());
    let branch_id = BranchId::new();
    (db, temp_dir, branch_id)
}

/// Metadata-only check: does the space have an entry in `SpaceIndex`'s
/// metadata key namespace? This bypasses the public `list/exists` union and
/// is the only way to verify that the lower-layer registration helper
/// actually fired.
fn space_metadata_exists(db: &Arc<Database>, branch_id: BranchId, space: &str) -> bool {
    use strata_storage::Key;

    db.transaction(branch_id, |txn| {
        let key = Key::new_space(branch_id, space);
        Ok(txn.get(&key)?.is_some())
    })
    .unwrap()
}

// ============================================================================
// Vector Space Isolation Tests
// ============================================================================

/// Regression test: vector collections must be isolated by space after restart.
///
/// Background: Prior to the space-aware freeze/recovery refactor, vector
/// collections in different spaces could bleed into each other because the
/// mmap file paths and recovery enumeration did not incorporate the space
/// dimension.
///
/// Test dimensions exercised:
/// - Step 1-5 write two same-name collections in different spaces
/// - Step 6 proves Part 1 of the fix (space-aware freeze path) by verifying
///   the on-disk layout has the space subdirectory.
/// - Step 7 proves Part 2 of the fix (recovery enumerates all spaces) by
///   checking backends are loaded immediately after open, not deferred.
/// - Step 7 also catches cross-space data corruption by verifying stored
///   vectors return the correct score.
/// - Step 8 deletes one collection and verifies the other survives.
#[test]
fn test_vector_collections_isolated_across_spaces_after_restart() {
    use strata_engine::SpaceIndex;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // -----------------------------------------------------------------
    // Session 1 - create two same-name collections in different spaces
    // -----------------------------------------------------------------
    {
        let db = test_db_open(&path);

        // Register the user spaces in metadata
        let space_index = SpaceIndex::new(db.clone());
        space_index.register(branch_id, "tenant_a").unwrap();
        space_index.register(branch_id, "tenant_b").unwrap();

        let store = VectorStore::new(db.clone());

        let cfg_a = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let cfg_b = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        store
            .create_collection(branch_id, "tenant_a", "embeddings", cfg_a)
            .unwrap();
        store
            .create_collection(branch_id, "tenant_b", "embeddings", cfg_b)
            .unwrap();

        // tenant_a: two distinct vectors
        store
            .insert(
                branch_id,
                "tenant_a",
                "embeddings",
                "k1",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "tenant_a",
                "embeddings",
                "k2",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();

        // tenant_b: same key as tenant_a but a different vector
        store
            .insert(
                branch_id,
                "tenant_b",
                "embeddings",
                "k1",
                &[0.0, 0.0, 1.0],
                None,
            )
            .unwrap();

        // Sanity check pre-restart
        let collections_a = store.list_collections(branch_id, "tenant_a").unwrap();
        assert_eq!(collections_a.len(), 1);
        assert_eq!(collections_a[0].count, 2);

        let collections_b = store.list_collections(branch_id, "tenant_b").unwrap();
        assert_eq!(collections_b.len(), 1);
        assert_eq!(collections_b[0].count, 1);

        drop(space_index);
        drop(store);
        drop(db);
    }

    // -----------------------------------------------------------------
    // Disk-layout check (proves freeze path fix)
    // -----------------------------------------------------------------
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    let vectors_root = path.join("vectors").join(&branch_hex);
    let tenant_a_vec = vectors_root.join("tenant_a").join("embeddings.vec");
    let tenant_b_vec = vectors_root.join("tenant_b").join("embeddings.vec");

    assert!(
        tenant_a_vec.exists(),
        "tenant_a embeddings.vec must exist at {:?} after freeze",
        tenant_a_vec
    );
    assert!(
        tenant_b_vec.exists(),
        "tenant_b embeddings.vec must exist at {:?} after freeze",
        tenant_b_vec
    );

    // Legacy path check
    let legacy_path = vectors_root.join("embeddings.vec");
    assert!(
        !legacy_path.exists(),
        "legacy space-less path {:?} must not exist",
        legacy_path
    );

    // -----------------------------------------------------------------
    // Session 2 - reopen and verify isolation after recovery
    // -----------------------------------------------------------------
    {
        use strata_engine::{CollectionId, VectorBackendState};

        let db = test_db_open(&path);

        // Check backends are loaded immediately after open
        let state = db.extension::<VectorBackendState>().unwrap();
        let cid_a = CollectionId::new(branch_id, "tenant_a", "embeddings");
        let cid_b = CollectionId::new(branch_id, "tenant_b", "embeddings");
        assert!(
            state.backends.contains_key(&cid_a),
            "tenant_a/embeddings backend must be loaded by recovery",
        );
        assert!(
            state.backends.contains_key(&cid_b),
            "tenant_b/embeddings backend must be loaded by recovery",
        );

        let store = VectorStore::new(db.clone());

        // Both collections recovered with original counts
        let collections_a = store.list_collections(branch_id, "tenant_a").unwrap();
        assert_eq!(collections_a.len(), 1);
        assert_eq!(collections_a[0].count, 2);

        let collections_b = store.list_collections(branch_id, "tenant_b").unwrap();
        assert_eq!(collections_b.len(), 1);
        assert_eq!(collections_b[0].count, 1);

        // Search tenant_a for its own k1 vector
        let hits_a = store
            .search(
                branch_id,
                "tenant_a",
                "embeddings",
                &[1.0, 0.0, 0.0],
                10,
                None,
            )
            .unwrap();
        assert!(!hits_a.is_empty());
        assert_eq!(hits_a[0].key, "k1");
        assert!((hits_a[0].score - 1.0).abs() < 1e-5);

        // Search tenant_b for its own k1 vector
        let hits_b = store
            .search(
                branch_id,
                "tenant_b",
                "embeddings",
                &[0.0, 0.0, 1.0],
                10,
                None,
            )
            .unwrap();
        assert!(!hits_b.is_empty());
        assert_eq!(hits_b[0].key, "k1");
        assert!((hits_b[0].score - 1.0).abs() < 1e-5);

        // Cross-space check: searching tenant_a for tenant_b's vector
        let cross_hits = store
            .search(
                branch_id,
                "tenant_a",
                "embeddings",
                &[0.0, 0.0, 1.0],
                10,
                None,
            )
            .unwrap();
        assert_eq!(cross_hits.len(), 2);
        for hit in &cross_hits {
            assert!(
                (hit.score - 1.0).abs() > 0.5,
                "tenant_a hit {:?} unexpectedly has perfect-match score {}",
                hit.key,
                hit.score,
            );
        }

        drop(store);
        drop(db);
    }

    // -----------------------------------------------------------------
    // Cross-space deletion isolation
    // -----------------------------------------------------------------
    {
        let db = test_db_open(&path);
        let store = VectorStore::new(db.clone());

        store
            .delete_collection(branch_id, "tenant_a", "embeddings")
            .unwrap();

        // tenant_b must still be intact
        let collections_b = store.list_collections(branch_id, "tenant_b").unwrap();
        assert_eq!(collections_b.len(), 1);
        assert_eq!(collections_b[0].count, 1);

        let collections_a = store.list_collections(branch_id, "tenant_a").unwrap();
        assert_eq!(collections_a.len(), 0);

        drop(store);
        drop(db);
    }

    // Reopen to confirm deletion + tenant_b survival persist
    {
        let db = test_db_open(&path);
        let store = VectorStore::new(db.clone());

        let collections_a = store.list_collections(branch_id, "tenant_a").unwrap();
        assert_eq!(collections_a.len(), 0);

        let collections_b = store.list_collections(branch_id, "tenant_b").unwrap();
        assert_eq!(collections_b.len(), 1);
        assert_eq!(collections_b[0].count, 1);
    }
}

// ============================================================================
// Engine-Layer Registration Tests (Vector)
// ============================================================================

#[test]
fn test_vector_create_collection_registers_space_at_engine_layer() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    let cfg = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
    store
        .create_collection(branch_id, "tenant_vec", "embeddings", cfg)
        .unwrap();

    assert!(
        space_metadata_exists(&db, branch_id, "tenant_vec"),
        "VectorStore::create_collection must persist a SpaceIndex metadata key"
    );
}

#[test]
fn test_vector_insert_registers_space_at_engine_layer() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    let cfg = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
    store
        .create_collection(branch_id, "tenant_vec_insert", "embeddings", cfg)
        .unwrap();
    store
        .insert(
            branch_id,
            "tenant_vec_insert",
            "embeddings",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    assert!(
        space_metadata_exists(&db, branch_id, "tenant_vec_insert"),
        "VectorStore::insert must persist a SpaceIndex metadata key"
    );
}

// ============================================================================
// Engine-Layer Registration Tests (Graph)
// ============================================================================

#[test]
fn test_graph_create_registers_space_at_engine_layer() {
    use strata_engine::graph::types::NodeData;

    let (db, _temp, branch_id) = setup();
    let graph = GraphStore::new(db.clone());

    graph
        .add_node(
            branch_id,
            "tenant_graph",
            "social",
            "alice",
            NodeData::default(),
        )
        .unwrap();

    assert!(
        space_metadata_exists(&db, branch_id, "tenant_graph"),
        "GraphStore::add_node must persist a SpaceIndex metadata key for its space"
    );
}

#[test]
fn test_graph_bulk_insert_empty_input_does_not_register_space() {
    let (db, _temp, branch_id) = setup();
    let graph = GraphStore::new(db.clone());

    // Empty bulk insert must NOT create a phantom space metadata entry
    let (nodes_inserted, edges_inserted) = graph
        .bulk_insert(branch_id, "phantom_bulk", "g", &[], &[], None)
        .unwrap();
    assert_eq!(nodes_inserted, 0);
    assert_eq!(edges_inserted, 0);

    assert!(
        !space_metadata_exists(&db, branch_id, "phantom_bulk"),
        "bulk_insert with empty inputs must not register a phantom space"
    );
}
