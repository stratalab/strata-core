//! EG5 vector absorption characterization.
//!
//! These tests pin vector behavior through the engine-owned vector runtime and
//! product-open composition.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use strata_core::{BranchId, EntityRef};
use strata_engine::database::OpenSpec;
use strata_engine::{
    open_product_cache, open_product_database, Database, DistanceMetric, GraphSubsystem,
    OpenOptions, ProductOpenOutcome, SearchSubsystem, VectorBackendState, VectorConfig,
    VectorStore, VectorSubsystem,
};
use strata_executor::{
    Command as ExecutorCommand, DistanceMetric as ExecutorDistanceMetric, Executor,
    Output as ExecutorOutput,
};
use tempfile::tempdir;

fn vector_product_spec(path: &Path) -> OpenSpec {
    OpenSpec::primary(path)
        .with_subsystem(GraphSubsystem)
        .with_subsystem(VectorSubsystem)
        .with_subsystem(SearchSubsystem)
}

fn open_vector_product_db(path: &Path) -> Arc<Database> {
    Database::open_runtime(vector_product_spec(path)).expect("vector product db should open")
}

fn open_local_product_db(path: &Path, options: OpenOptions) -> Arc<Database> {
    let outcome =
        open_product_database(path, options).expect("disk product open should return an outcome");
    let ProductOpenOutcome::Local { db, .. } = outcome else {
        panic!("disk product open should return a local database");
    };
    db
}

fn vector_mmap_path(
    data_dir: &Path,
    branch_id: BranchId,
    space: &str,
    collection: &str,
) -> PathBuf {
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    data_dir
        .join("vectors")
        .join(branch_hex)
        .join(space)
        .join(format!("{collection}.vec"))
}

fn vector_graph_dir(
    data_dir: &Path,
    branch_id: BranchId,
    space: &str,
    collection: &str,
) -> PathBuf {
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    data_dir
        .join("vectors")
        .join(branch_hex)
        .join(space)
        .join(format!("{collection}_graphs"))
}

#[test]
fn eg5c_cache_product_open_installs_engine_owned_vector_runtime() {
    let outcome = open_product_cache().expect("cache product open should succeed");

    let ProductOpenOutcome::Local { db, .. } = outcome else {
        panic!("cache product open should return a local database");
    };

    assert_eq!(
        db.installed_subsystem_names(),
        ["graph", "vector", "search"],
        "EG5C product open must compose the engine-owned vector runtime"
    );

    let branch_id = BranchId::default();
    let store = VectorStore::new(db.clone());
    store
        .create_collection(
            branch_id,
            "default",
            "eg5c_cache_runtime",
            VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
        )
        .expect("vector collection should be creatable through product open");
    store
        .insert(
            branch_id,
            "default",
            "eg5c_cache_runtime",
            "v1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .expect("vector insert should succeed through product open");

    let hits = store
        .search(
            branch_id,
            "default",
            "eg5c_cache_runtime",
            &[1.0, 0.0, 0.0],
            1,
            None,
        )
        .expect("vector search should use the installed vector runtime");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].key, "v1");

    db.shutdown()
        .expect("product-open database should shut down");
}

#[test]
fn eg5c_disk_product_open_installs_engine_owned_vector_runtime() {
    let temp = tempdir().expect("tempdir should succeed");
    let db = open_local_product_db(temp.path(), OpenOptions::default());

    assert_eq!(
        db.installed_subsystem_names(),
        ["graph", "vector", "search"],
        "disk product open must compose the engine-owned vector runtime"
    );

    let branch_id = BranchId::default();
    let store = VectorStore::new(db.clone());
    store
        .create_collection(
            branch_id,
            "default",
            "eg5c_disk_runtime",
            VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
        )
        .expect("vector collection should be creatable through disk product open");
    store
        .insert(
            branch_id,
            "default",
            "eg5c_disk_runtime",
            "v1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .expect("vector insert should succeed through disk product open");

    let hits = store
        .search(
            branch_id,
            "default",
            "eg5c_disk_runtime",
            &[1.0, 0.0, 0.0],
            1,
            None,
        )
        .expect("vector search should use the disk product-open vector runtime");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].key, "v1");

    db.shutdown()
        .expect("disk product-open database should shut down");
}

#[test]
fn eg5c_disk_product_open_recovers_vector_state_after_shutdown_reopen() {
    let temp = tempdir().expect("tempdir should succeed");
    let branch_id = BranchId::default();

    {
        let db = open_local_product_db(temp.path(), OpenOptions::default());
        let store = VectorStore::new(db.clone());
        store
            .create_collection(
                branch_id,
                "default",
                "eg5c_product_reopen",
                VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .expect("vector collection should be creatable through product open");
        store
            .insert(
                branch_id,
                "default",
                "eg5c_product_reopen",
                "v1",
                &[0.0, 1.0, 0.0],
                None,
            )
            .expect("vector insert should succeed through product open");
        db.flush().expect("flush should persist vector rows");
        db.shutdown()
            .expect("product-open database should shut down before reopen");
    }

    {
        let db = open_local_product_db(temp.path(), OpenOptions::default());
        assert_eq!(
            db.installed_subsystem_names(),
            ["graph", "vector", "search"],
            "product reopen should install the engine-owned vector runtime"
        );
        let store = VectorStore::new(db.clone());
        let hits = store
            .search(
                branch_id,
                "default",
                "eg5c_product_reopen",
                &[0.0, 1.0, 0.0],
                1,
                None,
            )
            .expect("product reopen should recover vector state");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "v1");
        db.shutdown()
            .expect("reopened product database should shut down");
    }
}

#[test]
fn eg5c_follower_product_open_recovers_primary_vector_state() {
    let temp = tempdir().expect("tempdir should succeed");
    let branch_id = BranchId::default();

    let primary = open_local_product_db(temp.path(), OpenOptions::default());
    let store = VectorStore::new(primary.clone());
    store
        .create_collection(
            branch_id,
            "default",
            "eg5c_follower_runtime",
            VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
        )
        .expect("primary should create vector collection");
    store
        .insert(
            branch_id,
            "default",
            "eg5c_follower_runtime",
            "v1",
            &[0.0, 0.0, 1.0],
            None,
        )
        .expect("primary should insert vector");
    primary
        .flush()
        .expect("primary flush should persist vector rows");

    let follower = open_local_product_db(temp.path(), OpenOptions::default().follower(true));
    assert_eq!(
        follower.installed_subsystem_names(),
        ["graph", "vector", "search"],
        "follower product open should install vector recovery"
    );
    assert!(
        follower.is_follower(),
        "product open should return a follower database"
    );

    let follower_store = VectorStore::new(follower.clone());
    let hits = follower_store
        .search(
            branch_id,
            "default",
            "eg5c_follower_runtime",
            &[0.0, 0.0, 1.0],
            1,
            None,
        )
        .expect("follower product open should recover primary vector rows");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].key, "v1");

    follower
        .shutdown()
        .expect("follower product database should shut down");
    primary
        .shutdown()
        .expect("primary product database should shut down");
}

#[test]
fn eg5a_recovery_rebuilds_non_default_and_system_vector_state_from_kv() {
    let temp = tempdir().expect("tempdir should succeed");
    let branch_id = BranchId::default();
    let source_ref = EntityRef::json(branch_id, "tenant_a", "doc-1");

    {
        let db = open_vector_product_db(temp.path());
        let store = VectorStore::new(db.clone());

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "tenant_a", "docs", config.clone())
            .expect("tenant vector collection should be created");
        store
            .insert(
                branch_id,
                "tenant_a",
                "docs",
                "doc-1-vector",
                &[1.0, 0.0, 0.0],
                None,
            )
            .expect("tenant vector should be inserted");

        store
            .create_system_collection(branch_id, "_system_embed_docs", config)
            .expect("system shadow collection should be created");
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_docs",
                "shadow-doc-1",
                &[1.0, 0.0, 0.0],
                None,
                source_ref.clone(),
            )
            .expect("system shadow vector should preserve source_ref");

        db.flush().expect("flush should persist KV vector rows");
        db.shutdown()
            .expect("shutdown should freeze sidecars and release locks");
    }

    let vectors_dir = temp.path().join("vectors");
    if vectors_dir.exists() {
        std::fs::remove_dir_all(&vectors_dir).expect("test should remove vector sidecars");
    }
    assert!(
        !vectors_dir.exists(),
        "reopen setup must force KV-only vector recovery"
    );

    {
        let db = open_vector_product_db(temp.path());
        let store = VectorStore::new(db.clone());

        assert_eq!(
            db.installed_subsystem_names(),
            ["graph", "vector", "search"],
            "reopen should install the same runtime order before recovery assertions"
        );

        let tenant_hits = store
            .search(branch_id, "tenant_a", "docs", &[1.0, 0.0, 0.0], 1, None)
            .expect("non-default space vectors should recover from KV");
        assert_eq!(tenant_hits.len(), 1);
        assert_eq!(tenant_hits[0].key, "doc-1-vector");

        let system_hits = store
            .system_search_with_sources(branch_id, "_system_embed_docs", &[1.0, 0.0, 0.0], 1)
            .expect("system shadow vectors should recover from KV");
        assert_eq!(system_hits.len(), 1);
        assert_eq!(system_hits[0].key, "shadow-doc-1");
        assert_eq!(system_hits[0].source_ref.as_ref(), Some(&source_ref));

        db.shutdown().expect("reopened database should shut down");
    }
}

#[test]
fn eg5e_primary_reopen_uses_vector_sidecar_cache_as_accelerator() {
    let temp = tempdir().expect("tempdir should succeed");
    let branch_id = BranchId::default();
    let vec_path = vector_mmap_path(temp.path(), branch_id, "default", "eg5e_primary_sidecar");

    {
        let db = open_vector_product_db(temp.path());
        let store = VectorStore::new(db.clone());
        store
            .create_collection(
                branch_id,
                "default",
                "eg5e_primary_sidecar",
                VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .expect("collection should be created");
        store
            .insert(
                branch_id,
                "default",
                "eg5e_primary_sidecar",
                "v1",
                &[1.0, 0.0, 0.0],
                None,
            )
            .expect("vector should be inserted");
        db.flush().expect("flush should persist vector rows");
        db.freeze_vector_heaps()
            .expect("freeze should write vector mmap cache");
        assert!(
            vec_path.exists(),
            "test setup should create the vector mmap sidecar"
        );
        db.shutdown().expect("database should shut down cleanly");
    }

    {
        let db = open_vector_product_db(temp.path());
        let state = db
            .extension::<VectorBackendState>()
            .expect("vector backend state should be installed");
        let cid = strata_engine::CollectionId::new(branch_id, "default", "eg5e_primary_sidecar");
        let backend = state
            .backends
            .get(&cid)
            .expect("primary reopen should eagerly recover the collection backend");
        assert!(
            backend.value().is_heap_mmap(),
            "primary reopen should load the valid mmap sidecar as an accelerator"
        );

        let store = VectorStore::new(db.clone());
        let hits = store
            .search(
                branch_id,
                "default",
                "eg5e_primary_sidecar",
                &[1.0, 0.0, 0.0],
                1,
                None,
            )
            .expect("search should work after sidecar-accelerated recovery");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "v1");
        db.shutdown().expect("reopened database should shut down");
    }
}

#[test]
fn eg5e_corrupt_vector_sidecar_falls_back_to_kv_without_data_loss() {
    let temp = tempdir().expect("tempdir should succeed");
    let branch_id = BranchId::default();
    let vec_path = vector_mmap_path(temp.path(), branch_id, "default", "eg5e_corrupt_sidecar");

    {
        let db = open_vector_product_db(temp.path());
        let store = VectorStore::new(db.clone());
        store
            .create_collection(
                branch_id,
                "default",
                "eg5e_corrupt_sidecar",
                VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .expect("collection should be created");
        store
            .insert(
                branch_id,
                "default",
                "eg5e_corrupt_sidecar",
                "v1",
                &[0.0, 1.0, 0.0],
                None,
            )
            .expect("vector should be inserted");
        db.flush().expect("flush should persist vector rows");
        db.freeze_vector_heaps()
            .expect("freeze should write vector mmap cache");
        assert!(
            vec_path.exists(),
            "test setup should create the vector mmap sidecar"
        );
        db.shutdown().expect("database should shut down cleanly");
    }

    std::fs::write(&vec_path, b"not a valid vector mmap cache")
        .expect("test should corrupt the sidecar cache");

    {
        let db = open_vector_product_db(temp.path());
        let state = db
            .extension::<VectorBackendState>()
            .expect("vector backend state should be installed");
        let cid = strata_engine::CollectionId::new(branch_id, "default", "eg5e_corrupt_sidecar");
        let backend = state
            .backends
            .get(&cid)
            .expect("corrupt sidecar should not prevent KV recovery");
        assert!(
            !backend.value().is_heap_mmap(),
            "corrupt sidecar should fall back to a KV-rebuilt heap"
        );

        let store = VectorStore::new(db.clone());
        let hits = store
            .search(
                branch_id,
                "default",
                "eg5e_corrupt_sidecar",
                &[0.0, 1.0, 0.0],
                1,
                None,
            )
            .expect("KV fallback should preserve vector data");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "v1");
        db.shutdown().expect("reopened database should shut down");
    }
}

#[test]
fn eg5e_space_delete_force_purges_orphan_vector_sidecars_before_lazy_reload() {
    let temp = tempdir().expect("tempdir should succeed");
    let branch_id = BranchId::from_user_name("default");
    let space = "tenant_gone";
    let collection = "eg5e_space_sidecar";
    let vec_path = vector_mmap_path(temp.path(), branch_id, space, collection);
    let graph_dir = vector_graph_dir(temp.path(), branch_id, space, collection);
    let graph_path = graph_dir.join("seg_0.hgr");

    {
        let db = open_local_product_db(temp.path(), OpenOptions::default());
        let executor = Executor::new(db.clone());
        let store = VectorStore::new(db.clone());
        executor
            .execute(ExecutorCommand::SpaceCreate {
                branch: None,
                space: space.to_string(),
            })
            .expect("space should be created");
        executor
            .execute(ExecutorCommand::VectorCreateCollection {
                branch: None,
                space: Some(space.to_string()),
                collection: collection.to_string(),
                dimension: 3,
                metric: ExecutorDistanceMetric::Cosine,
            })
            .expect("collection should be created in the deleted space");
        executor
            .execute(ExecutorCommand::VectorUpsert {
                branch: None,
                space: Some(space.to_string()),
                collection: collection.to_string(),
                key: "stale-if-recovered".to_string(),
                vector: vec![0.0, 0.0, 1.0],
                metadata: None,
            })
            .expect("vector should be inserted");
        db.flush().expect("flush should persist vector rows");
        db.freeze_vector_heaps()
            .expect("freeze should write vector sidecar cache");
        assert!(
            vec_path.exists(),
            "test setup should create the vector mmap sidecar cache"
        );
        std::fs::create_dir_all(&graph_dir).expect("test should create vector graph sidecar dir");
        std::fs::write(&graph_path, b"synthetic graph sidecar")
            .expect("test should create vector graph sidecar file");

        let cid = strata_engine::CollectionId::new(branch_id, space, collection);
        let state = db
            .extension::<VectorBackendState>()
            .expect("vector backend state should be installed");
        assert!(
            state.backends.remove(&cid).is_some(),
            "test setup should leave an orphan sidecar with no loaded backend"
        );

        let output = executor
            .execute(ExecutorCommand::SpaceDelete {
                branch: None,
                space: space.to_string(),
                force: true,
            })
            .expect("production space delete should succeed");
        assert!(matches!(output, ExecutorOutput::Unit));
        assert!(
            !vec_path.exists(),
            "space delete must remove orphan vector mmap sidecar cache"
        );
        assert!(
            !graph_dir.exists(),
            "space delete must remove orphan vector graph sidecar cache"
        );

        executor
            .execute(ExecutorCommand::VectorCreateCollection {
                branch: None,
                space: Some(space.to_string()),
                collection: collection.to_string(),
                dimension: 3,
                metric: ExecutorDistanceMetric::Cosine,
            })
            .expect("same-name collection should be recreatable after space purge");
        executor
            .execute(ExecutorCommand::VectorUpsert {
                branch: None,
                space: Some(space.to_string()),
                collection: collection.to_string(),
                key: "fresh-only".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: None,
            })
            .expect("fresh vector should be inserted after recreation");
        assert!(
            state.backends.remove(&cid).is_some(),
            "test setup should force lazy reload through the sidecar path"
        );
        let entry = store
            .get(branch_id, space, collection, "fresh-only")
            .expect("fresh vector should survive lazy reload")
            .expect("fresh vector should exist");
        assert_eq!(
            entry.value.embedding,
            vec![1.0, 0.0, 0.0],
            "lazy reload must use the recreated collection's KV embedding"
        );
        db.shutdown()
            .expect("shutdown should complete after sidecar purge");
    }
}

#[test]
fn eg5a_collection_delete_purges_vector_sidecar_cache_before_reopen() {
    let temp = tempdir().expect("tempdir should succeed");
    let branch_id = BranchId::default();
    let vec_path = vector_mmap_path(temp.path(), branch_id, "default", "eg5a_sidecar");
    let graph_dir = vector_graph_dir(temp.path(), branch_id, "default", "eg5a_sidecar");
    let graph_path = graph_dir.join("seg_0.hgr");

    {
        let db = open_vector_product_db(temp.path());
        let store = VectorStore::new(db.clone());

        store
            .create_collection(
                branch_id,
                "default",
                "eg5a_sidecar",
                VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .expect("collection should be created");
        store
            .insert(
                branch_id,
                "default",
                "eg5a_sidecar",
                "stale-if-recovered",
                &[1.0, 0.0, 0.0],
                None,
            )
            .expect("vector should be inserted");

        db.flush().expect("flush should persist vector rows");
        db.freeze_vector_heaps()
            .expect("freeze should write vector sidecar cache");
        assert!(
            vec_path.exists(),
            "test setup should create the vector mmap sidecar cache"
        );
        std::fs::create_dir_all(&graph_dir).expect("test should create vector graph sidecar dir");
        std::fs::write(&graph_path, b"synthetic graph sidecar")
            .expect("test should create vector graph sidecar file");
        assert!(
            graph_path.exists(),
            "test setup should create a vector graph sidecar"
        );

        store
            .delete_collection(branch_id, "default", "eg5a_sidecar")
            .expect("collection delete should succeed");
        assert!(
            !vec_path.exists(),
            "collection delete must purge stale vector sidecar cache"
        );
        assert!(
            !graph_dir.exists(),
            "collection delete must purge stale vector graph sidecar cache"
        );

        store
            .create_collection(
                branch_id,
                "default",
                "eg5a_sidecar",
                VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .expect("same-name collection should be recreatable after delete");
        db.shutdown()
            .expect("shutdown should complete after sidecar purge");
    }

    {
        let db = open_vector_product_db(temp.path());
        let store = VectorStore::new(db.clone());
        let hits = store
            .search(
                branch_id,
                "default",
                "eg5a_sidecar",
                &[1.0, 0.0, 0.0],
                10,
                None,
            )
            .expect("recreated empty collection should search successfully");
        assert!(
            hits.is_empty(),
            "reopen must not resurrect vectors from the deleted collection sidecar"
        );
        db.shutdown().expect("reopened database should shut down");
    }
}
