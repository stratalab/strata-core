//! Fail-closed degraded primitive paths.
//!
//! The surfaces under test are:
//!
//! - **vector in-memory / HNSW state** — config mismatch or rebuild
//!   failure must fail closed, not silently fall back.
//! - **JSON `_idx` secondary index rows** — a corrupt metadata or
//!   entry row must fail closed rather than serve stale index answers.
//!
//! These tests exercise both surfaces plus the cross-branch isolation,
//! delete-recreate clearance, and push-observer contract. All checks
//! use the per-`Database` [`PrimitiveDegradationRegistry`] as the
//! engine-owned record of fail-closed events and assert that
//! [`Database::retention_report`] surfaces the degraded entries
//! attributed to the correct `BranchRef` lifecycle instance.

#![cfg(not(miri))]

use crate::common::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use strata_core::contract::PrimitiveType;
use strata_core::value::Value;
use strata_core::BranchId;
use strata_core::{PrimitiveDegradedReason, StrataError};
use strata_engine::database::observers::ObserverError;
use strata_engine::{
    Database, PrimitiveDegradationRegistry, PrimitiveDegradedEvent, PrimitiveDegradedObserver,
};
use strata_engine::{SearchRequest, Searchable};
use strata_storage::{Key, Namespace, TypeTag};

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

/// Namespace matching what `VectorStore::namespace_for` produces at
/// runtime. Used by test poisoning helpers below.
fn vector_namespace(branch_id: BranchId, space: &str) -> Arc<Namespace> {
    Arc::new(Namespace::for_branch_space(branch_id, space))
}

/// Poison the collection's `__config__/{name}` KV entry so
/// `CollectionRecord::from_bytes` fails during the next recovery.
fn poison_vector_config(db: &Arc<Database>, branch: &str, space: &str, collection: &str) {
    let branch_id = resolve(branch);
    let ns = vector_namespace(branch_id, space);
    let key = Key::new_vector_config(ns, collection);
    db.transaction(branch_id, |txn| {
        Ok(txn.put(
            key,
            Value::Bytes(b"\x00\xFF\x00\xFFnot a collection record".to_vec()),
        )?)
    })
    .expect("poison vector config write");
}

/// Poison one persisted vector row so lazy reload from KV hits a corrupt
/// `VectorRecord`.
fn poison_vector_record(
    db: &Arc<Database>,
    branch: &str,
    space: &str,
    collection: &str,
    key: &str,
) {
    let branch_id = resolve(branch);
    let ns = vector_namespace(branch_id, space);
    let kv_key = Key::new_vector(ns, collection, key);
    db.transaction(branch_id, |txn| {
        Ok(txn.put(
            kv_key,
            Value::Bytes(b"\x00\xFF\x00\xFFnot a vector record".to_vec()),
        )?)
    })
    .expect("poison vector row");
}

fn vector_mmap_path(
    db: &Arc<Database>,
    branch: &str,
    space: &str,
    collection: &str,
) -> std::path::PathBuf {
    let branch_id = resolve(branch);
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    db.data_dir()
        .join("vectors")
        .join(branch_hex)
        .join(space)
        .join(format!("{collection}.vec"))
}

/// Corrupt the mmap header dimension so reopen must reject the cache and rebuild
/// from KV.
fn poison_vector_mmap_dimension(
    db: &Arc<Database>,
    branch: &str,
    space: &str,
    collection: &str,
    dimension: u32,
) {
    let path = vector_mmap_path(db, branch, space, collection);
    let mut bytes = std::fs::read(&path).expect("read vector mmap");
    assert!(
        bytes.len() >= 12,
        "mmap header must contain dimension field"
    );
    bytes[8..12].copy_from_slice(&dimension.to_le_bytes());
    std::fs::write(&path, bytes).expect("rewrite vector mmap");
}

/// Poison a JSON `_idx_meta/{space}` metadata row so `load_indexes`
/// fails closed on next search.
fn poison_json_idx_meta(
    db: &Arc<Database>,
    branch: &str,
    collection_space: &str,
    index_name: &str,
) {
    let branch_id = resolve(branch);
    let meta_space = format!("_idx_meta/{}", collection_space);
    let ns = Arc::new(Namespace::for_branch_space(branch_id, &meta_space));
    let key = Key::new_json(ns, index_name);
    db.transaction(branch_id, |txn| {
        Ok(txn.put(key, Value::Bytes(b"not a valid IndexDef".to_vec()))?)
    })
    .expect("poison _idx_meta write");
}

/// Poison a JSON `_idx/{space}/{name}` row so prefix/range scans hit an entry
/// whose `user_key` has no value/doc_id separator.
fn poison_json_idx_entry_missing_separator(
    db: &Arc<Database>,
    branch: &str,
    collection_space: &str,
    index_name: &str,
    raw_user_key: &[u8],
) {
    let branch_id = resolve(branch);
    let idx_space =
        strata_engine::primitives::json::index::index_space_name(collection_space, index_name);
    let ns = Arc::new(Namespace::for_branch_space(branch_id, &idx_space));
    let key = Key::new(ns, TypeTag::Json, raw_user_key.to_vec());
    db.transaction(branch_id, |txn| Ok(txn.put(key, Value::Bytes(vec![]))?))
        .expect("poison _idx entry");
}

fn small_vector_config() -> VectorConfig {
    VectorConfig {
        dimension: 3,
        metric: DistanceMetric::Cosine,
        storage_dtype: StorageDtype::F32,
    }
}

fn vector_search_request(branch: &str) -> SearchRequest {
    SearchRequest::new(resolve(branch), "")
        .with_mode(strata_engine::search::SearchMode::Vector)
        .with_precomputed_embedding(vec![1.0, 0.0, 0.0])
        .with_space("default")
        .with_k(5)
}

// =============================================================================
// §1 Vector config-mismatch closure
// =============================================================================

#[test]
fn vector_corrupt_collection_record_fails_closed_on_reopen() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .expect("create collection");
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .expect("seed vector");

    // Corrupt the persisted CollectionRecord while the branch is live.
    poison_vector_config(&test_db.db, "main", "default", "v1");

    // Reopen — vector recovery should detect the corrupt config and
    // register a PrimitiveDegradationEntry instead of silently dropping
    // the collection.
    test_db.reopen();

    let registry = test_db
        .db
        .extension::<PrimitiveDegradationRegistry>()
        .unwrap();
    let entry = registry
        .lookup(resolve("main"), PrimitiveType::Vector, "v1")
        .expect("expected degradation entry after reopen");
    assert_eq!(entry.reason, PrimitiveDegradedReason::ConfigDecodeFailure);

    // Subsequent reads must fail closed with the typed error, not
    // CollectionNotFound or silent empty results.
    let err = test_db
        .vector()
        .search(resolve("main"), "default", "v1", &[1.0, 0.0, 0.0], 5, None)
        .expect_err("search must fail closed on degraded collection");
    let err = StrataError::from(err);
    match &err {
        StrataError::PrimitiveDegraded {
            primitive,
            name,
            reason,
            ..
        } => {
            assert_eq!(*primitive, PrimitiveType::Vector);
            assert_eq!(name, "v1");
            assert_eq!(*reason, PrimitiveDegradedReason::ConfigDecodeFailure);
        }
        other => panic!("expected PrimitiveDegraded, got {other:?}"),
    }

    // retention_report() must surface the degraded entry attributed to
    // the live `main` BranchRef.
    let report = test_db.db.retention_report().expect("retention_report");
    let degraded = report
        .degraded_primitives
        .iter()
        .find(|e| e.primitive == PrimitiveType::Vector && e.primitive_name == "v1")
        .expect("retention_report must list degraded primitive");
    assert_eq!(degraded.name, "main");
    assert_eq!(
        degraded.reason,
        PrimitiveDegradedReason::ConfigDecodeFailure
    );
}

#[test]
fn vector_mmap_dim_mismatch_rebuilds_from_kv_without_degradation() {
    // Negative test: mmap cache is invalidatable per contract matrix
    // row "vector on-disk caches". Dimension mismatch in the mmap file
    // alone must NOT register a degradation — recovery falls back to
    // KV-based rebuild, which is contract-compliant.
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    // Reopen once so heap mmap is written (best-effort freeze on shutdown).
    test_db.reopen();

    // Corrupt only the mmap header dimension. This is a cache-only failure and
    // must trigger KV rebuild, not primitive degradation.
    poison_vector_mmap_dimension(&test_db.db, "main", "default", "v1", 4);

    // Reopen again — the collection must be healthy and no degradation entry
    // registered.
    test_db.reopen();
    let registry = test_db
        .db
        .extension::<PrimitiveDegradationRegistry>()
        .unwrap();
    assert!(
        registry
            .lookup(resolve("main"), PrimitiveType::Vector, "v1")
            .is_none(),
        "healthy mmap reopen must not mark collection degraded"
    );
    // Search works.
    let hits = test_db
        .vector()
        .search(resolve("main"), "default", "v1", &[1.0, 0.0, 0.0], 5, None)
        .expect("healthy search");
    assert_eq!(hits.len(), 1);
}

#[test]
fn vector_lazy_reload_corruption_fails_closed_on_triggering_read() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k2",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Force the next read through ensure_collection_loaded() instead of using
    // the already-loaded backend.
    {
        let state = test_db.vector().state().unwrap();
        let cid = strata_vector::CollectionId::new(resolve("main"), "default", "v1");
        state.backends.remove(&cid);
    }

    // Corrupt one persisted vector row after eviction. The triggering reload
    // must fail closed immediately rather than returning a partially rebuilt
    // backend for the first read.
    poison_vector_record(&test_db.db, "main", "default", "v1", "k2");

    let err = test_db
        .vector()
        .search(resolve("main"), "default", "v1", &[1.0, 0.0, 0.0], 5, None)
        .expect_err("lazy reload must fail closed on the triggering read");
    let err = StrataError::from(err);
    match &err {
        StrataError::PrimitiveDegraded {
            primitive,
            name,
            reason,
            ..
        } => {
            assert_eq!(*primitive, PrimitiveType::Vector);
            assert_eq!(name, "v1");
            assert_eq!(*reason, PrimitiveDegradedReason::ConfigMismatch);
        }
        other => panic!("expected PrimitiveDegraded, got {other:?}"),
    }

    let report = test_db.db.retention_report().expect("retention_report");
    assert!(
        report
            .degraded_primitives
            .iter()
            .any(|e| e.primitive == PrimitiveType::Vector
                && e.primitive_name == "v1"
                && e.reason == PrimitiveDegradedReason::ConfigMismatch),
        "triggering lazy reload must record degradation for reporting too"
    );
}

#[test]
fn vector_searchable_fails_closed_on_corrupt_system_collection_config() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    test_db
        .vector()
        .create_system_collection(resolve("main"), "_system_embed_kv", small_vector_config())
        .expect("create system collection");
    test_db
        .vector()
        .system_insert_with_source(
            resolve("main"),
            "_system_embed_kv",
            "shadow-k1",
            &[1.0, 0.0, 0.0],
            None,
            strata_core::EntityRef::kv(resolve("main"), "default", "real-k1"),
        )
        .expect("insert system vector");

    poison_vector_config(
        &test_db.db,
        "main",
        strata_engine::system_space::SYSTEM_SPACE,
        "_system_embed_kv",
    );

    let err = Searchable::search(&test_db.vector(), &vector_search_request("main"))
        .expect_err("branch-visible vector search must fail closed on corrupt system collection");
    match err {
        StrataError::PrimitiveDegraded {
            primitive,
            name,
            reason,
            ..
        } => {
            assert_eq!(primitive, PrimitiveType::Vector);
            assert_eq!(name, "_system_embed_kv");
            assert_eq!(reason, PrimitiveDegradedReason::ConfigDecodeFailure);
        }
        other => panic!("expected PrimitiveDegraded, got {other:?}"),
    }
}

#[test]
fn vector_searchable_fails_closed_on_lazy_reloaded_corrupt_system_vector_row() {
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    test_db
        .vector()
        .create_system_collection(resolve("main"), "_system_embed_kv", small_vector_config())
        .expect("create system collection");
    test_db
        .vector()
        .system_insert_with_source(
            resolve("main"),
            "_system_embed_kv",
            "shadow-k1",
            &[1.0, 0.0, 0.0],
            None,
            strata_core::EntityRef::kv(resolve("main"), "default", "real-k1"),
        )
        .expect("insert system vector");

    poison_vector_record(
        &test_db.db,
        "main",
        strata_engine::system_space::SYSTEM_SPACE,
        "_system_embed_kv",
        "shadow-k1",
    );
    test_db
        .vector()
        .purge_collections_in_branch(resolve("main"))
        .expect("evict in-memory backends");

    let err = Searchable::search(&test_db.vector(), &vector_search_request("main"))
        .expect_err("branch-visible vector search must fail closed on lazy reload corruption");
    match err {
        StrataError::PrimitiveDegraded {
            primitive,
            name,
            reason,
            ..
        } => {
            assert_eq!(primitive, PrimitiveType::Vector);
            assert_eq!(name, "_system_embed_kv");
            assert_eq!(reason, PrimitiveDegradedReason::ConfigMismatch);
        }
        other => panic!("expected PrimitiveDegraded, got {other:?}"),
    }
}

// =============================================================================
// §2 JSON _idx load-failure closure
// =============================================================================

#[test]
fn json_idx_meta_corrupt_fails_closed() {
    use strata_engine::search::{FieldFilter, FieldPredicate};
    use strata_engine::Searchable;

    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    let json = test_db.json();
    json.create(
        &resolve("main"),
        "default",
        "doc1",
        json_value(serde_json::json!({ "status": "active" })),
    )
    .unwrap();
    json.create_index(
        &resolve("main"),
        "default",
        "by_status",
        "$.status",
        strata_engine::primitives::json::index::IndexType::Tag,
    )
    .unwrap();

    // Corrupt the _idx_meta row.
    poison_json_idx_meta(&test_db.db, "main", "default", "by_status");

    let req = strata_engine::SearchRequest::new(resolve("main"), "").with_field_filter(
        FieldFilter::Predicate(FieldPredicate::Eq {
            field: "$.status".to_string(),
            value: serde_json::json!("active").into(),
        }),
    );
    let err = json.search(&req).expect_err("search must fail closed");
    match &err {
        StrataError::PrimitiveDegraded {
            primitive, reason, ..
        } => {
            assert_eq!(*primitive, PrimitiveType::Json);
            assert_eq!(*reason, PrimitiveDegradedReason::IndexMetadataCorrupt);
        }
        other => panic!("expected PrimitiveDegraded on JSON; got {other:?}"),
    }

    // Registry must contain the entry.
    let registry = test_db
        .db
        .extension::<PrimitiveDegradationRegistry>()
        .unwrap();
    let entry = registry
        .lookup(resolve("main"), PrimitiveType::Json, "default")
        .expect("JSON _idx degradation entry");
    assert_eq!(entry.reason, PrimitiveDegradedReason::IndexMetadataCorrupt);

    // Subsequent search short-circuits — still fails closed without
    // re-scanning.
    let err2 = json
        .search(&req)
        .expect_err("subsequent search still fails");
    assert!(matches!(err2, StrataError::PrimitiveDegraded { .. }));

    // retention_report() lists it.
    let report = test_db.db.retention_report().expect("retention_report");
    assert!(
        report
            .degraded_primitives
            .iter()
            .any(|e| e.primitive == PrimitiveType::Json && e.primitive_name == "default"),
        "retention_report must surface JSON _idx degradation"
    );
}

#[test]
fn json_idx_entry_corrupt_fails_closed() {
    use strata_engine::search::{FieldFilter, FieldPredicate};
    use strata_engine::Searchable;

    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    let json = test_db.json();
    json.create(
        &resolve("main"),
        "default",
        "doc1",
        json_value(serde_json::json!({ "status": "active" })),
    )
    .unwrap();
    json.create_index(
        &resolve("main"),
        "default",
        "by_status",
        "$.status",
        strata_engine::primitives::json::index::IndexType::Tag,
    )
    .unwrap();

    // A prefix scan over "active" will see this poisoned row, then fail while
    // extracting the doc_id because the separator is missing.
    poison_json_idx_entry_missing_separator(&test_db.db, "main", "default", "by_status", b"active");

    let req = strata_engine::SearchRequest::new(resolve("main"), "").with_field_filter(
        FieldFilter::Predicate(FieldPredicate::Prefix {
            field: "$.status".to_string(),
            prefix: "active".to_string(),
        }),
    );
    let err = json.search(&req).expect_err("search must fail closed");
    match &err {
        StrataError::PrimitiveDegraded {
            primitive, reason, ..
        } => {
            assert_eq!(*primitive, PrimitiveType::Json);
            assert_eq!(*reason, PrimitiveDegradedReason::IndexEntryCorrupt);
        }
        other => panic!("expected PrimitiveDegraded on JSON; got {other:?}"),
    }

    let registry = test_db
        .db
        .extension::<PrimitiveDegradationRegistry>()
        .unwrap();
    let entry = registry
        .lookup(resolve("main"), PrimitiveType::Json, "default")
        .expect("JSON _idx degradation entry");
    assert_eq!(entry.reason, PrimitiveDegradedReason::IndexEntryCorrupt);
}

// =============================================================================
// §3 Cross-branch isolation under degraded primitive
// =============================================================================

#[test]
fn cross_branch_isolation_under_vector_degradation() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    // Use parallel branches rather than fork: we want fully independent
    // storage namespaces (forks inherit state until materialize, which
    // is orthogonal to this test).
    test_db.db.branches().create("sibling").unwrap();

    // Do NOT bind `test_db.vector()` to a variable — holding an
    // `Arc<Database>` past `reopen()` would keep the old Database
    // alive and cause `acquire_primary_db` to weak-upgrade the existing
    // instance without re-running recovery.
    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .create_collection(resolve("sibling"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("sibling"),
            "default",
            "v1",
            "k1",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Poison only `main`'s collection.
    poison_vector_config(&test_db.db, "main", "default", "v1");
    test_db.reopen();

    // `main` fails closed.
    let err = test_db
        .vector()
        .search(resolve("main"), "default", "v1", &[1.0, 0.0, 0.0], 5, None)
        .expect_err("main must fail closed");
    let err = StrataError::from(err);
    assert!(
        matches!(err, StrataError::PrimitiveDegraded { .. }),
        "expected PrimitiveDegraded on main, got {err:?}"
    );

    // `sibling` is unaffected.
    let hits = test_db
        .vector()
        .search(
            resolve("sibling"),
            "default",
            "v1",
            &[0.0, 1.0, 0.0],
            5,
            None,
        )
        .expect("sibling search unaffected by main's degradation");
    assert_eq!(hits.len(), 1);

    // retention_report attributes only to `main`.
    let report = test_db.db.retention_report().expect("retention_report");
    let degraded_branches: Vec<&str> = report
        .degraded_primitives
        .iter()
        .filter(|e| e.primitive == PrimitiveType::Vector)
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(degraded_branches, vec!["main"]);
}

// =============================================================================
// §4 Delete / recreate clears registry
// =============================================================================

#[test]
fn delete_recreate_clears_registry_so_same_name_starts_clean() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    let gen0 = test_db
        .db
        .active_branch_ref(resolve("main"))
        .expect("gen-0 main ref");
    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    poison_vector_config(&test_db.db, "main", "default", "v1");
    test_db.reopen();

    let registry = test_db
        .db
        .extension::<PrimitiveDegradationRegistry>()
        .unwrap();
    let marked = registry
        .lookup(resolve("main"), PrimitiveType::Vector, "v1")
        .expect("recovery must register degradation");
    assert_eq!(
        marked.branch_ref, gen0,
        "registered entry must be attributed to gen-0 BranchRef"
    );

    // Delete must clear the registry for this BranchId.
    test_db.db.branches().delete("main").unwrap();
    assert!(
        registry
            .lookup(resolve("main"), PrimitiveType::Vector, "v1")
            .is_none(),
        "delete must clear primitive-degradation registry entries"
    );

    // Recreate same name — new lifecycle instance must have an advanced
    // generation so any straggler gen-0 registry entry would be filtered
    // out of `retention_report` by the generation-equality join.
    test_db.db.branches().create("main").unwrap();
    let gen1 = test_db
        .db
        .active_branch_ref(resolve("main"))
        .expect("gen-1 main ref");
    assert!(
        gen1.generation > gen0.generation,
        "recreated branch must advance the generation (got {} -> {})",
        gen0.generation,
        gen1.generation
    );

    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    // Search succeeds — no inherited degradation.
    let hits = test_db
        .vector()
        .search(resolve("main"), "default", "v1", &[1.0, 0.0, 0.0], 5, None)
        .expect("recreated main must not inherit degraded state");
    assert_eq!(hits.len(), 1);

    // retention_report for the fresh lifecycle must not carry any
    // degraded_primitives.
    let report = test_db.db.retention_report().expect("retention_report");
    assert!(
        report
            .degraded_primitives
            .iter()
            .all(|e| e.primitive_name != "v1"),
        "fresh gen-1 lifecycle must not inherit gen-0 degradation"
    );
}

// =============================================================================
// §5 Push observer receives degradation events
// =============================================================================

#[derive(Default)]
struct CountingObserver {
    count: AtomicUsize,
    last_primitive: parking_lot::Mutex<Option<PrimitiveType>>,
}

impl PrimitiveDegradedObserver for CountingObserver {
    fn name(&self) -> &'static str {
        "counting-degraded"
    }

    fn on_primitive_degraded(&self, event: &PrimitiveDegradedEvent) -> Result<(), ObserverError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        *self.last_primitive.lock() = Some(event.primitive);
        Ok(())
    }
}

#[test]
fn push_observer_fires_exactly_once_on_degradation() {
    // Registering an observer and driving a fresh degradation through
    // the registry must deliver exactly one event carrying the correct
    // `BranchRef` + primitive + reason + name. Repeat marks on the same
    // key must NOT re-fire the observer (contract: exactly-once push
    // per `(BranchId, PrimitiveType, name)` key).
    let test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    let main_ref = test_db
        .db
        .active_branch_ref(resolve("main"))
        .expect("live main branch ref");

    let observer = Arc::new(CountingObserver::default());
    test_db
        .db
        .primitive_degraded_observers()
        .register(observer.clone() as Arc<dyn PrimitiveDegradedObserver>);

    // First mark fires the observer.
    let registry = test_db
        .db
        .extension::<PrimitiveDegradationRegistry>()
        .unwrap();
    registry.mark(
        main_ref,
        PrimitiveType::Vector,
        "v1",
        PrimitiveDegradedReason::ConfigDecodeFailure,
        "seeded",
        Some(test_db.db.primitive_degraded_observers()),
    );
    assert_eq!(
        observer.count.load(Ordering::SeqCst),
        1,
        "first mark must fire observer exactly once"
    );
    assert_eq!(
        *observer.last_primitive.lock(),
        Some(PrimitiveType::Vector),
        "event must carry the correct primitive kind"
    );

    // Repeat mark of same key must NOT re-fire observer.
    registry.mark(
        main_ref,
        PrimitiveType::Vector,
        "v1",
        PrimitiveDegradedReason::ConfigDecodeFailure,
        "second attempt — ignored",
        Some(test_db.db.primitive_degraded_observers()),
    );
    assert_eq!(
        observer.count.load(Ordering::SeqCst),
        1,
        "repeat mark on same key must not re-fire observer"
    );

    // A distinct key on the same branch fires a new event.
    registry.mark(
        main_ref,
        PrimitiveType::Vector,
        "v2",
        PrimitiveDegradedReason::ConfigDecodeFailure,
        "distinct collection",
        Some(test_db.db.primitive_degraded_observers()),
    );
    assert_eq!(
        observer.count.load(Ordering::SeqCst),
        2,
        "distinct key must fire a new observer event"
    );
}

#[test]
fn register_primitive_degraded_observer_replays_recovery_discovered_entry() {
    // Recovery-time degradations happen during open, before callers can attach
    // observers. The Database-level registration helper must replay the current
    // registry so startup degradations are observable on subscribe.
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    poison_vector_config(&test_db.db, "main", "default", "v1");
    test_db.reopen();

    let observer = Arc::new(CountingObserver::default());
    test_db.db.register_primitive_degraded_observer(
        observer.clone() as Arc<dyn PrimitiveDegradedObserver>
    );
    assert_eq!(
        observer.count.load(Ordering::SeqCst),
        1,
        "registration must replay the existing recovery-time degradation"
    );
    assert_eq!(
        *observer.last_primitive.lock(),
        Some(PrimitiveType::Vector),
        "replayed event must preserve the primitive kind"
    );

    let registry = test_db
        .db
        .extension::<PrimitiveDegradationRegistry>()
        .unwrap();
    let entry = registry
        .lookup(resolve("main"), PrimitiveType::Vector, "v1")
        .expect("recovery must have registered the degradation");
    assert_eq!(entry.reason, PrimitiveDegradedReason::ConfigDecodeFailure);
    assert!(!entry.detail.is_empty(), "detail must carry decode error");
}

// =============================================================================
// §6 retention_report separates degraded primitives from orphan storage
// =============================================================================

#[test]
fn retention_report_separates_degraded_from_orphan_storage() {
    // A live branch with a degraded primitive must appear in
    // `degraded_primitives`, not `orphan_storage`. Degraded primitives
    // are logical fail-closed facts on a live lifecycle; orphans are
    // byte-based storage directories with no live control record.
    //
    // This test verifies attribution correctness: the degraded entry
    // is attributed to the specific live `BranchRef` (name + generation)
    // that owns the primitive, and the live branch does not leak into
    // `orphan_storage`.
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    let main_ref = test_db
        .db
        .active_branch_ref(resolve("main"))
        .expect("live main branch ref");
    test_db
        .vector()
        .create_collection(resolve("main"), "default", "v1", small_vector_config())
        .unwrap();
    test_db
        .vector()
        .insert(
            resolve("main"),
            "default",
            "v1",
            "k1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    poison_vector_config(&test_db.db, "main", "default", "v1");
    test_db.reopen();

    let report = test_db.db.retention_report().expect("retention_report");

    // Degraded primitive is listed with correct attribution.
    let degraded = report
        .degraded_primitives
        .iter()
        .find(|e| e.primitive == PrimitiveType::Vector && e.primitive_name == "v1")
        .expect("degraded_primitives must include the poisoned collection");
    assert_eq!(degraded.name, "main", "attribution names the live branch");
    assert_eq!(
        degraded.branch, main_ref,
        "attribution uses the live BranchRef (id + generation)"
    );
    assert_eq!(
        degraded.reason,
        PrimitiveDegradedReason::ConfigDecodeFailure
    );
    assert!(
        !degraded.detail.is_empty(),
        "detail must carry decode error text for operator triage"
    );

    // `main` is live, so it should not appear in orphan_storage.
    assert!(
        !report
            .orphan_storage
            .iter()
            .any(|o| o.branch_id == resolve("main")),
        "live branch must not appear in orphan_storage"
    );
}
