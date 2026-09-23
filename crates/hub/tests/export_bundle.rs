//! Bundle export behavior (HB3): the coordination doc §3.3 invariants,
//! source read-only guarantee, reproducibility, and the golden manifest
//! anchor that freezes the SAP1 + object-layout byte contract.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::json;
use strata_engine::{
    BranchName, Database, DurableLocalOpenOptions, GraphEdgeData, GraphEdgeType, GraphName,
    GraphNodeData, GraphNodeId, JsonDocumentId, JsonPath, JsonValue, KvKey, KvValue, ProductSpace,
    VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorKey,
    VectorMetadata,
};
use strata_hub::{BundleExportError, EngineExportOptions, StrataCoreEngine};
use stratahub_protocol::wire::{DatasetSchema, SamplePreview};

/// Builds the fixture database: kv, json, vectors, graph across the
/// default branch (events excluded — their wall-clock timestamps are
/// content and would break cross-build reproducibility; see HB2).
fn build_fixture_db(path: &Path) {
    let mut db = Database::open_local(path, DurableLocalOpenOptions::new())
        .expect("fixture db opens")
        .into_database();
    let branch = || BranchName::new("default").expect("branch");
    let space = || ProductSpace::new("default").expect("space");

    let mut kv = db.kv(branch(), space()).expect("kv");
    kv.put(
        KvKey::new("user:ada").expect("key"),
        KvValue::new(b"engineer".to_vec()),
    )
    .expect("put");
    kv.put(
        KvKey::new("user:lin").expect("key"),
        KvValue::new(b"designer".to_vec()),
    )
    .expect("put");

    let mut json_service = db.json(branch(), space()).expect("json");
    json_service
        .set_or_create(
            JsonDocumentId::new("config").expect("doc id"),
            &JsonPath::root(),
            JsonValue::new(json!({"model": "claude", "k": 5})).expect("value"),
        )
        .expect("json set");

    let mut vectors = db.vector(branch(), space()).expect("vector");
    let collection = VectorCollectionName::new("embeddings").expect("collection");
    vectors
        .create_collection(
            collection.clone(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("config"),
        )
        .expect("create collection");
    vectors
        .upsert(
            collection,
            VectorKey::new("doc1").expect("key"),
            VectorEmbedding::new(vec![0.1, 0.2, 0.3, 0.4]).expect("embedding"),
            Some(VectorMetadata::new(json!({"title": "intro"})).expect("metadata")),
        )
        .expect("upsert");

    let mut graph = db.graph(branch(), space()).expect("graph");
    let name = GraphName::new("social").expect("graph name");
    graph.create_graph(name.clone()).expect("create graph");
    for node in ["ada", "lin"] {
        graph
            .upsert_node(
                &name,
                GraphNodeId::new(node).expect("node id"),
                GraphNodeData::new(None, None),
            )
            .expect("node");
    }
    graph
        .upsert_edge(
            &name,
            GraphNodeId::new("ada").expect("src"),
            GraphEdgeType::new("knows").expect("type"),
            GraphNodeId::new("lin").expect("dst"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect("edge");
}

fn fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture_db(dir.path());
    dir
}

/// Portable file inventory (path → size), lock state excluded since the
/// engine legitimately touches it while the fixture is being *built*.
fn inventory(root: &Path) -> BTreeMap<PathBuf, u64> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.file_name().and_then(|name| name.to_str()) == Some("locks") {
                continue;
            }
            if entry.file_type().expect("file type").is_dir() {
                pending.push(path);
            } else {
                let size = entry.metadata().expect("metadata").len();
                files.insert(path.strip_prefix(root).expect("relative").to_owned(), size);
            }
        }
    }
    files
}

#[test]
fn export_satisfies_the_output_invariants() {
    let source = fixture_dir();
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");

    // Invariant 1: hash/body consistency for every object.
    for object in &output.objects {
        assert_eq!(
            stratahub_protocol::hash_bytes(&object.bytes),
            object.hash,
            "object hash matches body"
        );
        assert_eq!(object.bytes.len() as u64, object.size_bytes);
    }

    // Invariant 2: manifest objects ↔ emitted objects, exactly once each.
    assert_eq!(output.manifest.objects.len(), output.objects.len());
    for (descriptor, object) in output.manifest.objects.iter().zip(&output.objects) {
        assert_eq!(descriptor.hash, object.hash);
        assert_eq!(descriptor.path, object.path);
        assert_eq!(descriptor.size_bytes, object.size_bytes);
    }

    // Invariant 3: canonical-bytes roundtrip through both canonicalizers.
    let canonical = output.manifest.canonical_bytes().expect("canonical");
    assert_eq!(canonical, output.manifest_canonical_bytes);
    assert_eq!(
        serde_jcs::to_vec(&output.manifest).expect("serde_jcs"),
        output.manifest_canonical_bytes
    );

    // Invariant 4: compatibility facts.
    assert_eq!(
        output.manifest.engine_compatibility.required_engine_version,
        ">=1.0.0, <2.0.0"
    );
    assert_eq!(
        output
            .manifest
            .engine_compatibility
            .capability_registry_version,
        strata_hub::CAPABILITY_REGISTRY_VERSION
    );
    assert_eq!(
        output.manifest.engine_compatibility.required_capabilities,
        vec!["graph", "json", "kv", "vectors"]
    );
    // #3198: a dataset clone is point-in-time, so it never claims the
    // `time_travel` bundle capability. The exact list above already pins this;
    // the explicit negative keeps the contract legible next to it.
    assert!(
        !output
            .manifest
            .engine_compatibility
            .required_capabilities
            .contains(&"time_travel".to_owned()),
        "a point-in-time clone must not declare time_travel (#3198)"
    );

    // Structure: control document first, then section chunks.
    assert_eq!(output.objects[0].path.as_str(), "control/bundle.json");
    assert_eq!(
        output.objects[0].content_type.as_deref(),
        Some("application/json")
    );
    let paths: Vec<&str> = output
        .objects
        .iter()
        .map(|object| object.path.as_str())
        .collect();
    assert_eq!(
        paths,
        vec![
            "control/bundle.json",
            "branches/default/default/kv/0000.rows",
            "branches/default/default/json/0000.rows",
            "branches/default/default/vector/embeddings/0000.rows",
            "branches/default/default/graph/social/0000.rows",
        ]
    );

    // Manifest-level facts.
    output.manifest.validate().expect("manifest validates");
    assert_eq!(output.manifest.default_branch.as_str(), "default");
    assert_eq!(output.manifest.branches.len(), 1);
    assert!(output.manifest.branches[0]
        .head_commit
        .starts_with("blake3:"));
    assert_eq!(
        output.manifest.total_size_bytes,
        output.objects.iter().map(|o| o.size_bytes).sum::<u64>()
    );
}

#[test]
fn export_never_mutates_the_source() {
    let source = fixture_dir();
    let before = inventory(source.path());
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let _ = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    drop(engine);
    assert_eq!(before, inventory(source.path()), "source untouched");
}

#[test]
fn rebuilt_fixture_reproduces_the_same_manifest_hash() {
    let export_hash = |dir: &tempfile::TempDir| {
        let mut engine = StrataCoreEngine::open(dir.path()).expect("open");
        let output = engine
            .export_bundle(&EngineExportOptions::default())
            .expect("export");
        stratahub_protocol::hash_bytes(&output.manifest_canonical_bytes)
    };

    let first = fixture_dir();
    let second = fixture_dir();
    let hash_a = export_hash(&first);
    let hash_b = export_hash(&second);
    assert_eq!(hash_a, hash_b, "build-twice reproducibility");

    // Re-export of the same source is equally stable.
    assert_eq!(hash_a, export_hash(&first));
}

/// Golden anchor: freezes the SAP1 framing, object layout, control
/// document, and manifest field derivations in one hash. Investigate WHY
/// before updating — any drift is a wire-format change for every bundle.
#[test]
fn fixture_manifest_hash_is_pinned() {
    let source = fixture_dir();
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    let hash = stratahub_protocol::hash_bytes(&output.manifest_canonical_bytes);
    assert_eq!(
        hash.as_str(),
        "blake3:91020425367e6b9dc6aa33229605583e9893496730ee6b45084f15fdd36983fe",
        "bundle format anchor drift"
    );
}

#[test]
fn empty_database_exports_a_control_only_bundle() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let _db = Database::open_local(dir.path(), DurableLocalOpenOptions::new())
            .expect("empty db opens")
            .into_database();
    }
    let mut engine = StrataCoreEngine::open(dir.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    output.manifest.validate().expect("validates");
    assert_eq!(output.objects.len(), 1, "control document only");
    assert!(output
        .manifest
        .engine_compatibility
        .required_capabilities
        .is_empty());
    assert_eq!(
        output.manifest.created.unix_timestamp(),
        0,
        "empty bundle pins created to the epoch"
    );
}

#[test]
fn unknown_branch_and_bad_source_report_typed_errors() {
    let source = fixture_dir();
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let mut options = EngineExportOptions::default();
    options.branches = vec![stratahub_protocol::BranchName::parse("nope").expect("name")];
    let error = engine.export_bundle(&options).expect_err("unknown branch");
    assert!(matches!(error, BundleExportError::BranchNotFound(name) if name == "nope"));

    let empty = tempfile::tempdir().expect("tempdir");
    let missing = empty.path().join("nothing-here");
    let Err(error) = StrataCoreEngine::open(&missing) else {
        panic!("missing dir must refuse");
    };
    assert!(matches!(error, BundleExportError::NotAStrataDb(_)));

    // A directory that exists but holds no database.
    let Err(error) = StrataCoreEngine::open(empty.path()) else {
        panic!("non-database dir must refuse");
    };
    assert!(matches!(
        error,
        BundleExportError::NotAStrataDb(_) | BundleExportError::Internal { .. }
    ));
}

#[test]
fn emit_schema_preview_false_omits_blobs_and_hashes() {
    let source = fixture_dir();
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let mut options = EngineExportOptions::default();
    options.emit_schema_preview = false;
    let output = engine.export_bundle(&options).expect("export");
    assert!(output.schema_blob.is_none());
    assert!(output.preview_blob.is_none());
    assert!(output.manifest.schema_hash.is_none());
    assert!(output.manifest.preview_hash.is_none());
    assert_eq!(
        output.auxiliary_hashes,
        strata_hub::AuxiliaryHashes::default()
    );
}

#[test]
fn preview_truncates_long_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let db = Database::open_local(dir.path(), DurableLocalOpenOptions::new())
            .expect("db opens")
            .into_database();
        let mut kv = db
            .kv(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("kv");
        kv.put(
            KvKey::new("big").expect("key"),
            KvValue::new(vec![b'x'; 5_000]),
        )
        .expect("put");
    }
    let mut engine = StrataCoreEngine::open(dir.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    let preview: SamplePreview =
        serde_json::from_slice(output.preview_blob.as_deref().expect("preview"))
            .expect("typed preview");
    let summary = &preview.kv.expect("kv preview")[0].value_summary;
    assert_eq!(summary.chars().count(), 201, "200 chars + ellipsis");
    assert!(summary.ends_with('…'));
}

#[test]
fn auxiliary_blobs_are_typed_hashed_and_manifest_linked() {
    let source = fixture_dir();
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");

    let schema_blob = output.schema_blob.as_deref().expect("schema blob");

    let preview_blob = output.preview_blob.as_deref().expect("preview blob");
    let schema: DatasetSchema = serde_json::from_slice(schema_blob).expect("typed schema");
    let preview: SamplePreview = serde_json::from_slice(preview_blob).expect("typed preview");
    assert_eq!(
        output
            .auxiliary_hashes
            .schema
            .as_ref()
            .expect("schema hash"),
        &stratahub_protocol::hash_bytes(schema_blob)
    );
    assert_eq!(
        output
            .auxiliary_hashes
            .preview
            .as_ref()
            .expect("preview hash"),
        &stratahub_protocol::hash_bytes(preview_blob)
    );
    assert_eq!(output.manifest.schema_hash, output.auxiliary_hashes.schema);
    assert_eq!(
        output.manifest.preview_hash,
        output.auxiliary_hashes.preview
    );

    // Schema content: per-primitive sub-objects only for used primitives.
    let kv_schema = schema.kv.expect("kv schema");
    assert_eq!(kv_schema.namespaces.len(), 1);
    assert_eq!(kv_schema.namespaces[0].prefix, "user:");
    assert_eq!(kv_schema.namespaces[0].value_type, "raw");
    assert_eq!(kv_schema.namespaces[0].entry_count, 2);
    let json_schema = schema.json.expect("json schema");
    assert_eq!(
        json_schema.fields.get("model").map(String::as_str),
        Some("string")
    );
    assert_eq!(
        json_schema.fields.get("k").map(String::as_str),
        Some("number")
    );
    let vectors_schema = schema.vectors.expect("vectors schema");
    assert_eq!(vectors_schema.collections.len(), 1);
    assert_eq!(vectors_schema.collections[0].name, "embeddings");
    assert_eq!(vectors_schema.collections[0].dimension, 4);
    assert_eq!(vectors_schema.collections[0].count, 1);
    assert!(schema.events.is_none(), "no events in the fixture");

    // Preview content.
    let kv_preview = preview.kv.expect("kv preview");
    assert_eq!(kv_preview[0].key, "user:ada");
    assert_eq!(kv_preview[0].value_summary, "engineer");
    let branch_preview = preview.branches.expect("branches preview");
    assert_eq!(branch_preview.len(), 1);
    assert!(branch_preview[0].is_default);
    let vector_preview = preview.vectors.expect("vector preview");
    assert_eq!(vector_preview[0].dimension, 4);
    assert_eq!(vector_preview[0].vector_preview.len(), 4);
}
