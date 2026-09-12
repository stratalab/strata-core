//! Source and dependency boundary guards.

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn storage_crate_imports_stay_inside_persistence_adapter() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let offenders: Vec<_> = rust_files(&root)
        .into_iter()
        .filter(|path| {
            !path
                .components()
                .any(|part| part.as_os_str() == "persistence")
        })
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source file");
            let forbidden = [
                "strata_storage",
                "strata-storage",
                "StorageRuntime",
                "StorageOpenOptions",
                "CommitBatch",
                "CommitMutation",
                "StorageSpaceId",
                "StorageKey",
                "StorageValue",
                "BranchRequest",
            ];
            forbidden
                .iter()
                .any(|token| text.contains(token))
                .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "storage crate names escaped persistence: {offenders:?}"
    );
}

#[test]
fn planning_vocabulary_stays_out_of_sources_and_tests() {
    let mut roots = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
    roots.push(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"));
    let exact_token = ["Ne", "xt"].concat();
    let forbidden = [
        ["M", "5"].concat(),
        ["M", "5", "G"].concat(),
        ["M", "5", "T"].concat(),
        ["vertical", " ", "spine"].concat(),
        ["next", " ", "slice"].concat(),
    ];
    let offenders: Vec<_> = roots
        .into_iter()
        .flat_map(|root| rust_files(&root))
        .filter(|path| !path.ends_with("dependency_guards.rs"))
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source file");
            (forbidden.iter().any(|token| text.contains(token.as_str()))
                || text.contains(&exact_token))
            .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "planning vocabulary appeared outside docs: {offenders:?}"
    );
}

#[test]
fn executor_facing_api_does_not_expose_storage_types() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let roots = [
        root.join("api"),
        root.join("branch"),
        root.join("data").join("kv"),
        root.join("data").join("json"),
        root.join("data").join("vector"),
        root.join("data").join("event"),
        root.join("data").join("graph"),
    ];
    let forbidden = [
        "strata_storage",
        "StorageRuntime",
        "StorageOpenOptions",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "BranchRequest",
        "WalService",
        "WalFormat",
        "ManifestService",
        "ManifestSnapshot",
        "TableManifest",
        "TableRuntime",
        "Lifecycle",
        "StorageBackend",
        "TransactionContext",
        "TransactionSession",
        "TxnContext",
    ];
    let offenders: Vec<_> = roots
        .into_iter()
        .flat_map(|root| rust_files(&root))
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source file");
            forbidden
                .iter()
                .any(|token| text.contains(token))
                .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "executor-facing API exposed lower-layer names: {offenders:?}"
    );
}

#[test]
fn kv_service_keeps_storage_requests_in_persistence_modules() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("kv")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read KV service source");
    for forbidden in [
        "strata_storage",
        "StorageRuntime",
        "PointReadRequest",
        "PrefixScanReadRequest",
        "ScanReadRequest",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "ReadBound",
        "ReadLimit",
        "BranchRequest",
    ] {
        assert!(
            !text.contains(forbidden),
            "KV service constructed storage-specific request details"
        );
    }
}

#[test]
fn json_service_keeps_storage_requests_in_persistence_modules() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("json")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read JSON service source");
    for forbidden in [
        "strata_storage",
        "StorageRuntime",
        "PointReadRequest",
        "PrefixScanReadRequest",
        "ScanReadRequest",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "ReadBound",
        "ReadLimit",
        "BranchRequest",
    ] {
        assert!(
            !text.contains(forbidden),
            "JSON service constructed storage-specific request details"
        );
    }
}

#[test]
fn vector_service_keeps_storage_requests_in_persistence_modules() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("vector")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read vector service source");
    for forbidden in [
        "strata_storage",
        "StorageRuntime",
        "PointReadRequest",
        "PrefixScanReadRequest",
        "ScanReadRequest",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "ReadBound",
        "ReadLimit",
        "BranchRequest",
    ] {
        assert!(
            !text.contains(forbidden),
            "vector service constructed storage-specific request details"
        );
    }
}

#[test]
fn event_service_keeps_storage_requests_in_persistence_modules() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("event")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read event service source");
    for forbidden in [
        "strata_storage",
        "StorageRuntime",
        "PointReadRequest",
        "PrefixScanReadRequest",
        "ScanReadRequest",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "ReadBound",
        "ReadLimit",
        "BranchRequest",
    ] {
        assert!(
            !text.contains(forbidden),
            "event service constructed storage-specific request details"
        );
    }
}

#[test]
fn graph_service_keeps_storage_requests_in_persistence_modules() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("graph")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read graph service source");
    for forbidden in [
        "strata_storage",
        "StorageRuntime",
        "PointReadRequest",
        "PrefixScanReadRequest",
        "ScanReadRequest",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "ReadBound",
        "ReadLimit",
        "BranchRequest",
    ] {
        assert!(
            !text.contains(forbidden),
            "graph service constructed storage-specific request details"
        );
    }
}

#[test]
fn graph_core_does_not_expose_deferred_graph_surface() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let roots = [root.join("api"), root.join("data").join("graph")];
    // GO1 lifted the ontology deferral and GA2-GA4 the analytics and
    // traversal deferrals (docs/design/graph-v1-port-gap-analysis.md);
    // semantic merge, branch-DAG surfaces, and search boosting stay
    // deferred until their slices land and remove their tokens here.
    let forbidden = ["SemanticMerge", "BranchDag", "Boost"];
    let offenders: Vec<_> = roots
        .into_iter()
        .flat_map(|root| rust_files(&root))
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source file");
            forbidden
                .iter()
                .any(|token| text.contains(token))
                .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "graph core exposed deferred graph surface: {offenders:?}"
    );
}

#[test]
fn branch_service_keeps_storage_branch_requests_in_persistence_modules() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("branch")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read branch service source");
    for forbidden in [
        "strata_storage",
        "BranchRequest",
        "BranchAction",
        "StorageRuntime",
        "StorageBranch",
    ] {
        assert!(
            !text.contains(forbidden),
            "branch service constructed storage-specific request details"
        );
    }
}

#[test]
fn raw_control_key_helpers_stay_inside_control_and_persistence() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let roots = [
        root.join("api"),
        root.join("branch"),
        root.join("data").join("kv"),
        root.join("data").join("json"),
        root.join("data").join("vector"),
        root.join("data").join("event"),
        root.join("data").join("graph"),
    ];
    let forbidden = [
        "database_identity_key",
        "local_instance_identity_key",
        "storage_registry_key",
        "capability_registry_key",
        "migration_registry_key",
        "branch_index_key",
        "branch_default_key",
        "branch_catalog_key",
        "branch_pending_index_key",
        "branch_pending_key",
        "space_index_key",
        "space_catalog_key",
        "reserved_space_key",
    ];
    let offenders: Vec<_> = roots
        .into_iter()
        .flat_map(|root| rust_files(&root))
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source file");
            forbidden
                .iter()
                .any(|token| text.contains(token))
                .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "raw control key helpers escaped control/persistence modules: {offenders:?}"
    );
}

#[test]
fn core_control_does_not_define_deferred_row_families() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = [
        manifest.join("src").join("persistence").join("space.rs"),
        manifest.join("src").join("control").join("records.rs"),
        manifest.join("src").join("control").join("bootstrap.rs"),
        manifest.join("src").join("control").join("space.rs"),
    ];
    let forbidden = [
        "Recipe",
        "Search",
        "Retrieval",
        "Shadow",
        "Derived",
        "ManifestCache",
        "QueryCache",
        "PromptCache",
        "RowClass::Recipe",
        "RowClass::Search",
        "RowClass::Retrieval",
        "RowClass::Shadow",
        "RowClass::Derived",
        "RowClass::Cache",
    ];
    let offenders: Vec<_> = files
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read core control source");
            forbidden
                .iter()
                .any(|token| text.contains(token))
                .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "core control introduced deferred row families: {offenders:?}"
    );
}

#[test]
fn branch_service_does_not_expose_deferred_workflows() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("branch")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read branch service source");
    for forbidden in [
        "pub fn merge",
        "pub fn publish",
        "pub fn review",
        "pub fn cherry",
        "pub fn revert",
        "pub fn restore",
    ] {
        assert!(
            !text.contains(forbidden),
            "branch service exposed deferred workflow `{forbidden}`"
        );
    }
}

#[test]
fn branch_catalog_preserves_product_default_id_separate_from_storage_id() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("branch")
        .join("catalog.rs");
    let text = fs::read_to_string(path).expect("read branch catalog source");
    assert!(text.contains("DEFAULT_BRANCH_ID: BranchId = BranchId::from_bytes([0;"));
    assert!(text.contains("DEFAULT_STORAGE_BRANCH_ID"));
    assert!(text.contains("storage_branch_id_for_product_branch_id"));
}

#[test]
fn persistence_adapter_owns_storage_request_construction_and_error_mapping() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("persistence")
        .join("adapter.rs");
    let text = fs::read_to_string(path).expect("read persistence adapter source");
    for required in [
        "PointReadRequest",
        "PrefixScanReadRequest",
        "ScanReadRequest",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "ReadBound",
        "ReadLimit",
        "BranchRequest",
        "map_storage_error",
    ] {
        assert!(
            text.contains(required),
            "persistence adapter stopped owning `{required}`"
        );
    }
}

#[test]
fn vector_manifest_code_never_imports_artifact_payloads() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("vector")
        .join("manifest.rs");
    let text = fs::read_to_string(path).expect("read vector manifest source");
    for forbidden in [
        "FlatVectorArtifact",
        "HnswVectorArtifact",
        "VectorEmbedding",
        "FlatVectorArtifactRow",
        "HnswRuntimeIndex",
        "encode_flat_vector_artifact",
        "encode_hnsw_vector_artifact",
        "decode_flat_vector_artifact",
        "decode_hnsw_vector_artifact",
        "FLAT_ARTIFACT_MAGIC",
        "HNSW_ARTIFACT_MAGIC",
        "fast_hnsw",
    ] {
        assert!(
            !text.contains(forbidden),
            "vector manifest imported artifact payload detail `{forbidden}`"
        );
    }
}

#[test]
fn vector_system_manifest_writes_stay_ref_only() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("vector")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read vector service source");
    let writer = function_body(&text, "fn put_index_manifest_bytes");
    for required in [
        "self.index_manifest_address",
        "RowMutation::put",
        "CommitPlan::new",
    ] {
        assert!(
            writer.contains(required),
            "index manifest writer stopped using expected manifest row path `{required}`"
        );
    }
    for forbidden in [
        "FlatVectorArtifact",
        "HnswVectorArtifact",
        "VectorEmbedding",
        "encode_flat_vector_artifact",
        "encode_hnsw_vector_artifact",
        "store_flat",
        "store_hnsw",
        "persist_raw_flat_payload",
        "persist_raw_hnsw_payload",
        "fast_hnsw",
    ] {
        assert!(
            !writer.contains(forbidden),
            "index manifest writer can store artifact payload detail `{forbidden}`"
        );
    }
}

#[test]
fn vector_artifact_payload_code_stays_private_to_vector_artifact_module() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("vector");
    let offenders: Vec<_> = rust_files(&root)
        .into_iter()
        .filter(|path| !path.ends_with("artifact.rs"))
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read vector source");
            [
                "FLAT_ARTIFACT_MAGIC",
                "HNSW_ARTIFACT_MAGIC",
                "persist_raw_flat_payload",
                "persist_raw_hnsw_payload",
                "decode_flat_vector_artifact",
                "decode_hnsw_vector_artifact",
                "encode_flat_vector_artifact",
                "encode_hnsw_vector_artifact",
            ]
            .iter()
            .any(|token| text.contains(token))
            .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "vector artifact payload codec escaped artifact module: {offenders:?}"
    );
}

#[test]
fn vector_indexing_does_not_reintroduce_process_global_backend_state() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("vector");
    let offenders: Vec<_> = rust_files(&root)
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read vector source");
            [
                "static mut",
                "lazy_static!",
                "OnceLock<",
                "OnceCell<",
                "static ARTIFACT",
                "static HNSW",
                "static INDEX",
                "static VECTOR",
                "static BACKEND",
            ]
            .iter()
            .any(|token| text.contains(token))
            .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "vector indexing reintroduced process-global mutable backend state: {offenders:?}"
    );
}

#[test]
fn kv_read_shapes_do_not_devolve_to_point_or_write_loops() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("data")
        .join("kv")
        .join("service.rs");
    let text = fs::read_to_string(path).expect("read KV service source");

    let batch_get = function_body(&text, "pub fn batch_get");
    assert!(batch_get.contains("let record = self.branch_record()?;"));
    assert!(batch_get.contains(".read_row("));
    assert!(!batch_get.contains("commit_batch"));

    // `count` / `count_at` delegate to `count_with_selector`, which owns the scan.
    let count = function_body(&text, "fn count_with_selector");
    assert!(count.contains(".scan_prefix("));
    assert!(!count.contains(".read_row("));

    let sample = function_body(&text, "pub fn sample");
    assert!(sample.contains(".scan_prefix("));
    assert!(!sample.contains(".read_row("));

    let page = function_body(&text, "fn scan_keys_after_cursor");
    assert!(page.contains(".scan_range("));
    assert!(page.contains("saturating_add(1)"));
    assert!(!page.contains(".persistence.scan_prefix("));
}

#[test]
fn executor_crate_does_not_import_storage_crates() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let executor = manifest
        .parent()
        .expect("engine crate has crates parent")
        .join("executor");
    if !executor.exists() {
        return;
    }
    let mut files = rust_files(&executor.join("src"));
    files.push(executor.join("Cargo.toml"));
    let offenders: Vec<_> = files
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read executor source");
            [
                "strata_storage",
                "strata-storage",
                "StorageRuntime",
                "StorageOpenOptions",
                "CommitBatch",
                "CommitMutation",
                "StorageSpaceId",
                "StorageKey",
                "StorageValue",
                "BranchRequest",
            ]
            .iter()
            .any(|token| text.contains(token))
            .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "executor crate imported storage behavior details: {offenders:?}"
    );
}

#[test]
fn storage_next_source_stays_free_of_vector_indexing_semantics() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let storage = manifest
        .parent()
        .expect("engine crate has crates parent")
        .join("storage");
    if !storage.exists() {
        return;
    }
    let offenders: Vec<_> = rust_files(&storage.join("src"))
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read storage source");
            [
                "fast_hnsw",
                "HNSW",
                "Hnsw",
                "hnsw",
                "Cosine",
                "cosine",
                "Euclidean",
                "euclidean",
                "DotProduct",
                "dot_product",
                "VectorDistanceMetric",
                "VectorEmbedding",
                "VectorCollection",
                "VectorCollectionName",
                "VectorMetadata",
                "VectorIndex",
                "FlatVectorArtifact",
                "HnswVectorArtifact",
                "vector index",
                "vector artifact",
            ]
            .iter()
            .any(|token| text.contains(token))
            .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "storage gained vector-indexing semantic dependencies: {offenders:?}"
    );
}

#[test]
fn executor_crate_does_not_compute_vector_distances_or_inspect_index_artifacts() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let executor = manifest
        .parent()
        .expect("engine crate has crates parent")
        .join("executor");
    if !executor.exists() {
        return;
    }
    let offenders: Vec<_> = rust_files(&executor.join("src"))
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read executor source");
            [
                "fast_hnsw",
                "VectorIndexPolicy",
                "VectorCandidateSource",
                "FlatVectorArtifact",
                "HnswVectorArtifact",
                "query_exact_for_test",
                "query_at_exact_for_test",
                "flat_artifact",
                "hnsw_artifact",
                "seed_flat_index",
                "seal_index",
                "cosine_distance",
                "cosine_similarity",
                "euclidean_distance",
                "dot_product_score",
                "compute_vector_score",
                "score_embedding",
            ]
            .iter()
            .any(|token| text.contains(token))
            .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "executor gained vector index internals or distance computation: {offenders:?}"
    );
}

#[test]
fn benchmark_sources_do_not_use_engine_persistence_internals() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let benchmark_root = manifest
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join("benchmarks")
        .join("src");
    if !benchmark_root.exists() {
        return;
    }
    let offenders: Vec<_> = rust_files(&benchmark_root)
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read benchmark source");
            [
                "strata_engine::persistence",
                "persistence::adapter",
                "StoragePersistence",
                "PersistenceReadRow",
                "CommitPlan",
                "RowAddress",
            ]
            .iter()
            .any(|token| text.contains(token))
            .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "benchmark source used engine persistence internals: {offenders:?}"
    );
}

#[test]
fn product_scope_stays_limited_to_current_primitives() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    // "Export" left this list when dataset clone artifacts landed: branch
    // artifact export is sanctioned engine scope per
    // docs/architecture/engine/dataset-clone-artifact-contract.md (the
    // roadmap's export/import substrate), implemented in src/artifact/.
    //
    // "Merge" left this list in M12D1 when branch promote landed: promotion is
    // sanctioned engine scope per
    // docs/architecture/engine/branch-operation-and-capability-adapter-contract.md,
    // implemented in src/branch/promote.rs (the promotion lineage record is
    // `BranchMergeRecord`). Cherry-pick and revert stay forbidden here and in
    // the branch_merge_absence guard until their M12E/M12F slices.
    let forbidden = [
        "Retrieval",
        "Ipc",
        "Diff",
        "Restore",
        "Revert",
        "CherryPick",
        "TransactionSession",
        "begin_transaction",
        "transaction_session",
    ];
    let offenders: Vec<_> = rust_files(&root)
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source file");
            forbidden
                .iter()
                .any(|token| text.contains(token))
                .then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "out-of-scope product behavior appeared in engine crate: {offenders:?}"
    );
}

#[test]
fn open_options_remain_explicit() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("api")
        .join("options.rs");
    let text = fs::read_to_string(path).expect("read options source");
    for forbidden in [
        "derive(Default)",
        "impl Default for CacheOpenOptions",
        "impl Default for DurableLocalOpenOptions",
    ] {
        assert!(
            !text.contains(forbidden),
            "open options gained implicit default mode"
        );
    }
}

/// Lists every `.rs` file under `root` with a self-contained directory walk.
///
/// This deliberately avoids shelling out to an external tool (e.g. ripgrep) so
/// the crate-shape guard suite is deterministic and runs on a stock rustup +
/// cargo toolchain with no undeclared prerequisites, per the crate-shape /
/// test-harness contract ("harnesses should be reusable, deterministic, and
/// explicitly invoked").
/// #3337: a return type written with the crate's `Result` alias is invisible to
/// the mutation gate.
///
/// cargo-mutants reads signatures as text and resolves no type aliases, so
/// `-> EngineResult<T>` looks like an unknown one-parameter container: every
/// body-replacement mutant it writes (`EngineResult::new()`,
/// `EngineResult::from(None)`) fails to compile, is scored *unviable*, and the
/// lane passes having tested nothing. Spelled out, the same function yields
/// `Ok(None)` / `Ok(true)` — the mutants that catch a result nothing observes.
///
/// The alias stays: it is public API, and every other position keeps using it.
/// Only the return position has to say `Result<T, EngineError>`.
#[test]
fn return_types_spell_the_result_out_for_the_mutation_gate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let offenders: Vec<_> = rust_files(&root)
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source file");
            // The qualified spelling hides just as well.
            (text.contains("-> EngineResult<") || text.contains("::EngineResult<")).then_some(path)
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "a return type hides behind the alias, so the mutation gate cannot \
         replace it: write `-> Result<T, EngineError>` in {offenders:?}"
    );
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files
}

fn collect_rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    // A missing root (e.g. an absent sibling crate) yields no files, matching
    // the previous behavior where such roots contributed nothing to scan.
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("rs"))
        {
            files.push(path);
        }
    }
}

fn function_body<'a>(source: &'a str, signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("missing function signature `{signature}`"));
    let open = source[start..].find('{').map_or_else(
        || panic!("missing function body `{signature}`"),
        |offset| start + offset,
    );
    let mut depth = 0usize;
    for (offset, byte) in source[open..].bytes().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return &source[open..=open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("unterminated function body `{signature}`");
}

/// #3134: the pre-M9B crate names must not come back into the live docs.
///
/// The crates shed their `-next` suffix in M9B (hard rule 43); the docs did
/// not, and thousands of stale references survived into 1.2.x — an agent
/// reading `strata-storage-next` will try to depend on a crate that has not
/// existed for a year.
///
/// The first version of this guard matched four lowercase names in
/// `docs/architecture` and `docs/spec` only, so 961 references in other cases
/// (`Storage-next`, `Storage-Next`), under names it did not list
/// (`intelligence-next`, `cli-next`, `executor-next`), and in doc trees it did
/// not scan survived it — along with eight file *names*. It now matches
/// case-insensitively, covers every renamed crate, and checks paths as well as
/// content.
///
/// Every `archive/` directory and the audit trees are deliberately exempt: both
/// are records of the rewrite, where the old names are the correct ones.
#[test]
fn live_docs_do_not_name_pre_rename_crates() {
    /// Every crate that shed a `-next` suffix, longest first so that
    /// `intelligence-next` is reported rather than the `.*-next` inside it.
    const RENAMED: &[&str] = &[
        "intelligence-next",
        "inference-next",
        "executor-next",
        "storage-next",
        "engine-next",
        "core-next",
        "cli-next",
    ];

    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crate sits under <repo>/crates/engine")
        .to_path_buf();

    let mut documents: Vec<PathBuf> = markdown_files(&repo.join("docs"));
    documents.extend(markdown_files(&repo.join("crates")));
    documents.push(repo.join("CLAUDE.md"));
    documents.push(repo.join("README.md"));

    let mut offenders = Vec::new();
    for path in documents {
        let relative = path.strip_prefix(&repo).unwrap_or(&path);
        // History keeps its own names: every `archive/` directory, and the
        // audit trees, which record what the code said when they were written.
        let historical = relative.components().any(|part| {
            matches!(
                part.as_os_str().to_string_lossy().as_ref(),
                "archive" | "audit" | "audits"
            )
        });
        if historical {
            continue;
        }
        let haystacks = [
            relative.to_string_lossy().to_lowercase(),
            fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
                .to_lowercase(),
        ];
        for name in RENAMED {
            if haystacks.iter().any(|hay| hay.contains(name)) {
                offenders.push(format!("{}: {name}", relative.display()));
                break;
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "live docs name crates that were renamed in M9B; use the shipped names \
         (strata-storage, strata-engine, strata-core, strata-inference, \
         strata-executor, strata-cli) or move the document under \
         docs/architecture/archive/:\n  {}",
        offenders.join("\n  ")
    );
}

fn markdown_files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(markdown_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "md") {
            found.push(path);
        }
    }
    found
}
