//! Branch artifact import behavior (slice `HB6b`).
//!
//! The load-bearing proof: export → import into a fresh database →
//! re-export is byte-identical, events and all — the property StrataHub's
//! round-trip conformance (Ask 4) is built on.

use std::path::Path;

use serde_json::json;
use strata_engine::artifact::BranchArtifact;
use strata_engine::{
    BranchName, CacheOpenOptions, Database, DurableLocalOpenOptions, EventPayload, EventType,
    GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId, GraphObjectTypeDef,
    GraphPropertyDef, GraphTypeName, JsonDocumentId, JsonPath, JsonValue, KvKey, KvValue,
    ProductSpace, VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding,
    VectorKey, VectorMetadata,
};

fn branch() -> BranchName {
    BranchName::new("default").expect("branch")
}

fn space(name: &str) -> ProductSpace {
    ProductSpace::new(name).expect("space")
}

fn fresh_db() -> Database {
    Database::open_cache(CacheOpenOptions::new())
        .expect("cache open")
        .into_database()
}

/// Every data model, two spaces, ontology, and events — the full surface.
#[allow(clippy::too_many_lines)]
fn populate(db: &mut Database) {
    let mut kv = db.kv(branch(), space("default")).expect("kv");
    kv.put_batch([
        (
            KvKey::new("user:ada").expect("key"),
            KvValue::new(b"engineer".to_vec()),
        ),
        (
            KvKey::new("user:lin").expect("key"),
            KvValue::new(b"designer".to_vec()),
        ),
    ])
    .expect("kv batch");

    let mut json = db.json(branch(), space("default")).expect("json");
    json.set_or_create(
        JsonDocumentId::new("config").expect("id"),
        &JsonPath::root(),
        JsonValue::new(json!({"model": "claude", "k": 5})).expect("value"),
    )
    .expect("json");

    let mut events = db.event(branch(), space("default")).expect("events");
    for tool in ["search", "fetch"] {
        events
            .append(
                EventType::new("tool_call").expect("type"),
                EventPayload::new(json!({ "tool": tool })).expect("payload"),
            )
            .expect("append");
    }

    let mut vectors = db.vector(branch(), space("default")).expect("vectors");
    let collection = VectorCollectionName::new("embeddings").expect("name");
    vectors
        .create_collection(
            collection.clone(),
            VectorConfig::new(4, VectorDistanceMetric::Cosine).expect("config"),
        )
        .expect("collection");
    vectors
        .upsert(
            collection,
            VectorKey::new("doc1").expect("key"),
            VectorEmbedding::new(vec![0.1, 0.2, 0.3, 0.4]).expect("embedding"),
            Some(VectorMetadata::new(json!({"title": "intro"})).expect("metadata")),
        )
        .expect("upsert");

    // Second space with its own kv row.
    db.spaces(branch())
        .expect("spaces")
        .create(space("tenant_a"))
        .expect("create space");
    let mut kv_b = db.kv(branch(), space("tenant_a")).expect("kv b");
    kv_b.put(
        KvKey::new("plan").expect("key"),
        KvValue::new(b"step-1".to_vec()),
    )
    .expect("put");

    // Graph with a frozen ontology.
    let mut graph = db.graph(branch(), space("default")).expect("graph");
    let name = GraphName::new("social").expect("name");
    graph.create_graph(name.clone()).expect("create");
    graph
        .define_object_type(
            &name,
            GraphObjectTypeDef::new(
                GraphTypeName::new("person").expect("type name"),
                [(
                    "role".to_owned(),
                    GraphPropertyDef::new(Some("string".to_owned()), false).expect("prop"),
                )],
            )
            .expect("def"),
        )
        .expect("define");
    graph
        .define_link_type(
            &name,
            strata_engine::GraphLinkTypeDef::new(
                GraphTypeName::new("knows").expect("link name"),
                GraphTypeName::new("person").expect("source"),
                GraphTypeName::new("person").expect("target"),
                None,
                [],
            )
            .expect("link def"),
        )
        .expect("define link");
    graph.freeze_ontology(&name).expect("freeze");
    for node in ["ada", "lin"] {
        graph
            .upsert_node(
                &name,
                GraphNodeId::new(node).expect("id"),
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
            GraphEdgeData::new(2.5, None).expect("edge"),
        )
        .expect("edge");
}

fn export(db: &mut Database) -> BranchArtifact {
    db.export_branch_artifact(&branch()).expect("export")
}

#[test]
fn round_trip_reproduces_the_artifact_byte_for_byte() {
    let mut source = fresh_db();
    populate(&mut source);
    let original = export(&mut source);

    let mut target = fresh_db();
    let summary = target
        .import_branch_artifact(&original)
        .expect("import succeeds");
    assert_eq!(summary.sections(), original.sections().len());

    let reexported = export(&mut target);
    assert_eq!(
        original, reexported,
        "export → import → export must be byte-identical"
    );
}

#[test]
fn imported_content_is_queryable_through_normal_reads() {
    let mut source = fresh_db();
    populate(&mut source);
    let artifact = export(&mut source);

    let mut target = fresh_db();
    target.import_branch_artifact(&artifact).expect("import");

    let mut kv = target.kv(branch(), space("default")).expect("kv");
    let value = kv
        .get(&KvKey::new("user:ada").expect("key"))
        .expect("get")
        .expect("present");
    assert_eq!(value.as_bytes(), b"engineer");

    let mut events = target.event(branch(), space("default")).expect("events");
    let verification = events.verify_chain().expect("verify");
    assert!(verification.is_valid(), "hash chain re-derives cleanly");

    let graph = target.graph(branch(), space("default")).expect("graph");
    let ontology = graph
        .ontology(&GraphName::new("social").expect("name"))
        .expect("read")
        .expect("defined");
    assert_eq!(ontology.object_types().len(), 1);
}

#[test]
fn import_into_populated_branch_refuses() {
    let mut source = fresh_db();
    populate(&mut source);
    let artifact = export(&mut source);

    let mut target = fresh_db();
    let mut kv = target.kv(branch(), space("default")).expect("kv");
    kv.put(
        KvKey::new("existing").expect("key"),
        KvValue::new(b"content".to_vec()),
    )
    .expect("put");

    let Err(error) = target.import_branch_artifact(&artifact) else {
        panic!("import into populated branch must refuse");
    };
    assert_eq!(error.code(), "conflict.engine.artifact_import");
}

#[test]
fn import_is_deterministic_across_targets() {
    let mut source = fresh_db();
    populate(&mut source);
    let artifact = export(&mut source);

    let mut target_a = fresh_db();
    target_a
        .import_branch_artifact(&artifact)
        .expect("import a");
    let mut target_b = fresh_db();
    target_b
        .import_branch_artifact(&artifact)
        .expect("import b");

    assert_eq!(export(&mut target_a), export(&mut target_b));
}

#[test]
fn empty_artifact_imports_cleanly() {
    let mut source = fresh_db();
    let artifact = export(&mut source);

    let mut target = fresh_db();
    let summary = target.import_branch_artifact(&artifact).expect("import");
    assert_eq!(summary.records(), 0);
    assert_eq!(export(&mut target), artifact);
}

// --- #3070: multi-branch import (durable path enforces the monotonic floor) ---

fn open_durable(path: &Path) -> Database {
    Database::open_local(path, DurableLocalOpenOptions::new())
        .expect("durable open")
        .into_database()
}

fn put_kv(db: &mut Database, branch: &BranchName, key: &str, value: &[u8]) {
    db.kv(branch.clone(), space("default"))
        .expect("kv")
        .put(KvKey::new(key).expect("key"), KvValue::new(value.to_vec()))
        .expect("put");
}

/// Two branches whose commit timestamps interleave: `second` is forked below
/// `default`'s latest write, so importing `default` first raises the global
/// monotonic floor above `second`'s history. Importing them one-at-a-time
/// (the pre-#3070 loop) rejects the second branch's replay; a global-order
/// import must reconstruct the true interleaved stream and succeed.
fn build_interleaved_branches(path: &Path) -> (BranchName, BranchName) {
    let default = branch();
    let second = BranchName::new("second").expect("branch");
    let mut db = open_durable(path);

    put_kv(&mut db, &default, "k1", b"v1");
    db.branches()
        .expect("branches")
        .create(second.clone())
        .expect("create second");
    put_kv(&mut db, &second, "k2", b"v2");
    // A structural graph create on `second` exercises the CreateGraph replay
    // step inside the merged global order (below `default`'s later write).
    {
        let mut graph = db.graph(second.clone(), space("default")).expect("graph");
        let name = GraphName::new("g").expect("name");
        graph.create_graph(name.clone()).expect("create graph");
        graph
            .upsert_node(
                &name,
                GraphNodeId::new("n1").expect("id"),
                GraphNodeData::new(None, None),
            )
            .expect("node");
    }
    // Advance `default` past everything on `second`, inverting the floor.
    put_kv(&mut db, &default, "k3", b"v3");
    (default, second)
}

#[test]
fn multi_branch_import_reconstructs_interleaved_commit_order() {
    let source_dir = tempfile::tempdir().expect("tmp");
    let (default, second) = build_interleaved_branches(source_dir.path());

    let mut source = open_durable(source_dir.path());
    let default_artifact = source.export_branch_artifact(&default).expect("export a");
    let second_artifact = source.export_branch_artifact(&second).expect("export b");
    drop(source);

    let target_dir = tempfile::tempdir().expect("tmp");
    let mut target = open_durable(target_dir.path());
    let summaries = target
        .import_branch_artifacts(&[default_artifact.clone(), second_artifact.clone()])
        .expect("multi-branch import succeeds");
    assert_eq!(summaries.len(), 2);

    // Each branch re-exports byte-identical to its source artifact (HB6b,
    // generalized across branches) — timestamps and order preserved.
    assert_eq!(
        target
            .export_branch_artifact(&default)
            .expect("re-export a"),
        default_artifact,
        "default re-export must be byte-identical"
    );
    assert_eq!(
        target.export_branch_artifact(&second).expect("re-export b"),
        second_artifact,
        "second re-export must be byte-identical"
    );

    // Both branches serve their content through normal reads.
    assert_eq!(
        target
            .kv(default.clone(), space("default"))
            .expect("kv")
            .get(&KvKey::new("k3").expect("key"))
            .expect("get")
            .expect("present")
            .as_bytes(),
        b"v3"
    );
    assert_eq!(
        target
            .kv(second, space("default"))
            .expect("kv")
            .get(&KvKey::new("k2").expect("key"))
            .expect("get")
            .expect("present")
            .as_bytes(),
        b"v2"
    );

    // The import must not leave the commit clock pinned to the replay floor:
    // a normal write afterward allocates a fresh (generated) timestamp and
    // succeeds — proving the structural replay hold was released, not leaked.
    target
        .kv(default, space("default"))
        .expect("kv")
        .put(
            KvKey::new("post_import").expect("key"),
            KvValue::new(b"live".to_vec()),
        )
        .expect("a normal write after import must succeed");
}

/// A forked child shares the parent's rows at the parent's timestamps, then
/// diverges. Importing the parent first would raise the floor above the shared
/// rows; the global order replays the shared timestamps together.
fn build_forked_branches(path: &Path) -> (BranchName, BranchName) {
    let default = branch();
    let child = BranchName::new("child").expect("branch");
    let mut db = open_durable(path);

    put_kv(&mut db, &default, "shared", b"base");
    db.branches()
        .expect("branches")
        .fork_current(&default, child.clone())
        .expect("fork child");
    put_kv(&mut db, &child, "child_only", b"c");
    put_kv(&mut db, &default, "default_only", b"d");
    (default, child)
}

#[test]
fn multi_branch_import_handles_a_fork_sharing_timestamps() {
    let source_dir = tempfile::tempdir().expect("tmp");
    let (default, child) = build_forked_branches(source_dir.path());
    let mut source = open_durable(source_dir.path());
    let artifacts = [
        source.export_branch_artifact(&default).expect("export a"),
        source.export_branch_artifact(&child).expect("export b"),
    ];
    drop(source);

    let target_dir = tempfile::tempdir().expect("tmp");
    let mut target = open_durable(target_dir.path());
    target
        .import_branch_artifacts(&artifacts)
        .expect("forked multi-branch import succeeds");

    assert_eq!(
        target
            .export_branch_artifact(&default)
            .expect("re-export a"),
        artifacts[0],
        "default re-export must be byte-identical"
    );
    assert_eq!(
        target.export_branch_artifact(&child).expect("re-export b"),
        artifacts[1],
        "child re-export must be byte-identical"
    );
    // The shared row and each branch's divergent row are both present.
    assert_eq!(
        target
            .kv(child.clone(), space("default"))
            .expect("kv")
            .get(&KvKey::new("shared").expect("key"))
            .expect("get")
            .expect("present")
            .as_bytes(),
        b"base"
    );
    assert_eq!(
        target
            .kv(child, space("default"))
            .expect("kv")
            .get(&KvKey::new("child_only").expect("key"))
            .expect("get")
            .expect("present")
            .as_bytes(),
        b"c"
    );
}

#[test]
fn multi_branch_import_is_deterministic_across_targets() {
    let source_dir = tempfile::tempdir().expect("tmp");
    let (default, second) = build_interleaved_branches(source_dir.path());
    let mut source = open_durable(source_dir.path());
    let artifacts = [
        source.export_branch_artifact(&default).expect("export a"),
        source.export_branch_artifact(&second).expect("export b"),
    ];
    drop(source);

    let dir_a = tempfile::tempdir().expect("tmp");
    let mut target_a = open_durable(dir_a.path());
    target_a
        .import_branch_artifacts(&artifacts)
        .expect("import a");

    let dir_b = tempfile::tempdir().expect("tmp");
    let mut target_b = open_durable(dir_b.path());
    target_b
        .import_branch_artifacts(&artifacts)
        .expect("import b");

    for branch in [&default, &second] {
        assert_eq!(
            target_a.export_branch_artifact(branch).expect("a"),
            target_b.export_branch_artifact(branch).expect("b"),
            "multi-branch import must be deterministic across targets"
        );
    }
}

#[test]
fn multi_branch_import_refuses_a_populated_target_branch() {
    let source_dir = tempfile::tempdir().expect("tmp");
    let (default, second) = build_interleaved_branches(source_dir.path());
    let mut source = open_durable(source_dir.path());
    let artifacts = [
        source.export_branch_artifact(&default).expect("export a"),
        source.export_branch_artifact(&second).expect("export b"),
    ];
    drop(source);

    let target_dir = tempfile::tempdir().expect("tmp");
    let mut target = open_durable(target_dir.path());
    // Pre-populate the second target branch so the emptiness oracle trips.
    target
        .branches()
        .expect("branches")
        .create(second.clone())
        .expect("create");
    put_kv(&mut target, &second, "squatter", b"x");

    let Err(error) = target.import_branch_artifacts(&artifacts) else {
        panic!("import into a populated branch must refuse");
    };
    assert_eq!(error.code(), "conflict.engine.artifact_import");
}
