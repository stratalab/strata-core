//! Branch artifact export behavior (HB2).
//!
//! Pins the determinism contract: identical logical databases produce
//! byte-identical artifacts; any logical difference changes the bytes.

use serde_json::json;
use strata_engine::artifact::{ArtifactModel, BranchArtifact};
use strata_engine::{
    BranchName, CacheOpenOptions, Database, EventPayload, EventType, GraphEdgeData, GraphEdgeType,
    GraphName, GraphNodeData, GraphNodeId, JsonDocumentId, JsonPath, JsonValue, KvKey, KvValue,
    ProductSpace, VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding,
    VectorKey, VectorMetadata,
};

fn branch() -> BranchName {
    BranchName::new("default").expect("branch")
}

fn space() -> ProductSpace {
    ProductSpace::new("default").expect("space")
}

/// Populates every data model with fixed logical content.
fn populate(db: &mut Database) {
    populate_without_events(db);
    let mut events = db.event(branch(), space()).expect("event service");
    events
        .append(
            EventType::new("tool_call").expect("event type"),
            EventPayload::new(json!({"tool": "search"})).expect("payload"),
        )
        .expect("event append");
}

/// Fixed logical content for the cross-database determinism proof.
///
/// Events are excluded: the engine stamps event records with wall-clock
/// time at append, so two databases populated at different instants hold
/// *different* logical content in their event logs. Event export
/// stability is pinned separately via same-database re-export.
fn populate_without_events(db: &mut Database) {
    let mut kv = db.kv(branch(), space()).expect("kv service");
    kv.put(
        KvKey::new("user:ada").expect("key"),
        KvValue::new(b"engineer".to_vec()),
    )
    .expect("kv put");
    kv.put(
        KvKey::new("user:lin").expect("key"),
        KvValue::new(b"designer".to_vec()),
    )
    .expect("kv put");

    let mut json = db.json(branch(), space()).expect("json service");
    json.set_or_create(
        JsonDocumentId::new("config").expect("doc id"),
        &JsonPath::root(),
        JsonValue::new(json!({"model": "claude", "k": 5})).expect("json value"),
    )
    .expect("json set");

    let mut vectors = db.vector(branch(), space()).expect("vector service");
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
            VectorKey::new("doc1").expect("vector key"),
            VectorEmbedding::new(vec![0.1, 0.2, 0.3, 0.4]).expect("embedding"),
            Some(VectorMetadata::new(json!({"title": "intro"})).expect("metadata")),
        )
        .expect("vector upsert");

    let mut graph = db.graph(branch(), space()).expect("graph service");
    let name = GraphName::new("social").expect("graph name");
    graph.create_graph(name.clone()).expect("create graph");
    for node in ["ada", "lin"] {
        graph
            .upsert_node(
                &name,
                GraphNodeId::new(node).expect("node id"),
                GraphNodeData::new(None, None),
            )
            .expect("upsert node");
    }
    graph
        .upsert_edge(
            &name,
            GraphNodeId::new("ada").expect("src"),
            GraphEdgeType::new("knows").expect("edge type"),
            GraphNodeId::new("lin").expect("dst"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect("upsert edge");
}

fn export(db: &mut Database) -> BranchArtifact {
    db.export_branch_artifact(&branch()).expect("export")
}

fn fresh_populated() -> Database {
    let mut db = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open")
        .into_database();
    populate(&mut db);
    db
}

#[test]
fn identical_logical_databases_export_identical_bytes() {
    let mut db_a = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open")
        .into_database();
    populate_without_events(&mut db_a);
    let mut db_b = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open")
        .into_database();
    populate_without_events(&mut db_b);
    assert_eq!(
        export(&mut db_a),
        export(&mut db_b),
        "byte-determinism across logically-identical databases"
    );

    // Re-exporting the same database is stable for ALL models, events
    // included (their wall-clock timestamps are fixed once appended).
    let mut db = fresh_populated();
    assert_eq!(export(&mut db), export(&mut db));
}

#[test]
fn any_logical_difference_changes_the_bytes() {
    let baseline = export(&mut fresh_populated());

    let mut db = fresh_populated();
    let mut kv = db.kv(branch(), space()).expect("kv service");
    kv.put(
        KvKey::new("user:ada").expect("key"),
        KvValue::new(b"cto".to_vec()),
    )
    .expect("kv put");
    let diverged = export(&mut db);

    let kv_bytes = |artifact: &BranchArtifact| {
        artifact
            .sections()
            .iter()
            .find(|section| section.model() == ArtifactModel::Kv)
            .expect("kv section")
            .bytes()
            .to_vec()
    };
    assert_ne!(kv_bytes(&baseline), kv_bytes(&diverged));
}

#[test]
fn export_covers_every_data_model_and_reports_facts() {
    let mut db = fresh_populated();
    let artifact = export(&mut db);

    assert_eq!(artifact.branch().as_str(), "default");
    assert_eq!(
        artifact
            .spaces()
            .iter()
            .map(|space| space.as_str().to_owned())
            .collect::<Vec<_>>(),
        vec!["default"]
    );

    let facts: Vec<(ArtifactModel, Option<&str>, u64)> = artifact
        .sections()
        .iter()
        .map(|section| (section.model(), section.qualifier(), section.record_count()))
        .collect();
    assert_eq!(
        facts,
        vec![
            (ArtifactModel::Kv, None, 2),
            (ArtifactModel::Json, None, 1),
            (ArtifactModel::Event, None, 1),
            (ArtifactModel::Vector, Some("embeddings"), 2), // config + 1 entry
            (ArtifactModel::Graph, Some("social"), 4),      // meta + 2 nodes + 1 edge
        ]
    );

    let max = artifact.max_row_timestamp().expect("rows were exported");
    assert!(max.as_micros() > 0);
}

#[test]
fn empty_branch_exports_no_sections() {
    let mut db = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open")
        .into_database();
    let artifact = export(&mut db);
    assert!(artifact.sections().is_empty());
    assert!(artifact.max_row_timestamp().is_none());
    assert_eq!(artifact.spaces().len(), 1, "default space exists");
}

#[test]
fn kv_records_decode_under_sap1_framing() {
    let mut db = fresh_populated();
    let artifact = export(&mut db);
    let section = artifact
        .sections()
        .iter()
        .find(|section| section.model() == ArtifactModel::Kv)
        .expect("kv section");

    let mut bytes = section.bytes();
    let mut records = Vec::new();
    while !bytes.is_empty() {
        let record_len = read_u32(&mut bytes) as usize;
        let (mut record, rest) = bytes.split_at(record_len);
        bytes = rest;

        let key = read_field(&mut record);
        let value = read_field(&mut record);
        let mut timestamp = [0_u8; 8];
        timestamp.copy_from_slice(&record[..8]);
        records.push((
            String::from_utf8(key).expect("utf8 key"),
            String::from_utf8(value).expect("utf8 value"),
            u64::from_le_bytes(timestamp),
        ));
    }

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].0, "user:ada");
    assert_eq!(records[0].1, "engineer");
    assert_eq!(records[1].0, "user:lin");
    assert!(records[0].2 > 0, "timestamps ride along");
}

/// #3198: a dataset clone artifact is POINT-IN-TIME — it carries the current
/// committed value of each document, not its version history. A document with
/// two versions at the source exports as a single record holding the latest
/// value. This pins the deliberate V1 contract (see
/// docs/architecture/engine/dataset-clone-artifact-contract.md); a future
/// history-carrying export would change this and must do so consciously, and
/// declare a `time_travel` bundle capability.
#[test]
fn json_export_is_point_in_time_not_history() {
    use strata_engine::artifact::{decode_section, ArtifactRecord};

    let mut db = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open")
        .into_database();
    {
        let mut json = db.json(branch(), space()).expect("json service");
        json.set_or_create(
            JsonDocumentId::new("iris:35").expect("id"),
            &JsonPath::root(),
            JsonValue::new(json!({ "petal_width": 0.1 })).expect("v1"),
        )
        .expect("v1 set");
        json.set_or_create(
            JsonDocumentId::new("iris:35").expect("id"),
            &JsonPath::root(),
            JsonValue::new(json!({ "petal_width": 0.2 })).expect("v2"),
        )
        .expect("v2 set");

        // The source genuinely retains both versions, so the pin is non-vacuous:
        // export dropping history is the point-in-time contract, not an empty DB.
        let history = json
            .get_versions(&JsonDocumentId::new("iris:35").expect("id"))
            .expect("history read")
            .expect("document exists");
        assert_eq!(history.rows().len(), 2, "source retains two versions");
    }

    let artifact = export(&mut db);
    let section = artifact
        .sections()
        .iter()
        .find(|section| section.model() == ArtifactModel::Json)
        .expect("json section");

    let mut docs = Vec::new();
    for record in decode_section(section.model(), section.bytes()) {
        if let ArtifactRecord::Json { id, doc, .. } = record.expect("record decodes") {
            let value: serde_json::Value =
                serde_json::from_slice(&doc).expect("exported doc parses");
            docs.push((id, value));
        }
    }
    assert_eq!(
        docs.len(),
        1,
        "export carries one record per document, not one per version"
    );
    assert_eq!(docs[0].0, "iris:35");
    assert_eq!(
        docs[0].1,
        json!({ "petal_width": 0.2 }),
        "export carries the current value, not the retained history"
    );
}

fn read_u32(bytes: &mut &[u8]) -> u32 {
    let mut buffer = [0_u8; 4];
    buffer.copy_from_slice(&bytes[..4]);
    *bytes = &bytes[4..];
    u32::from_le_bytes(buffer)
}

fn read_field(bytes: &mut &[u8]) -> Vec<u8> {
    let field_len = read_u32(bytes) as usize;
    let field = bytes[..field_len].to_vec();
    *bytes = &bytes[field_len..];
    field
}

#[test]
fn decoded_records_mirror_the_populated_content() {
    use strata_engine::artifact::{decode_section, ArtifactRecord};

    let mut db = fresh_populated();
    let artifact = export(&mut db);

    let mut kinds = Vec::new();
    for section in artifact.sections() {
        for record in decode_section(section.model(), section.bytes()) {
            kinds.push(match record.expect("record decodes") {
                ArtifactRecord::Kv { key, value, .. } => {
                    assert!(!key.is_empty() && !value.is_empty());
                    "kv"
                }
                ArtifactRecord::Json { id, doc, .. } => {
                    assert_eq!(id, "config");
                    serde_json::from_slice::<serde_json::Value>(&doc).expect("doc parses");
                    "json"
                }
                ArtifactRecord::Event {
                    event_type,
                    payload,
                    ..
                } => {
                    assert_eq!(event_type, "tool_call");
                    serde_json::from_slice::<serde_json::Value>(&payload).expect("payload");
                    "event"
                }
                ArtifactRecord::VectorConfig { config, .. } => {
                    let config: serde_json::Value =
                        serde_json::from_slice(&config).expect("config");
                    assert_eq!(config["dimension"], 4);
                    "vconfig"
                }
                ArtifactRecord::VectorEntry { key, embedding, .. } => {
                    assert_eq!(key, "doc1");
                    assert_eq!(embedding.len(), 4);
                    "ventry"
                }
                ArtifactRecord::GraphMeta { .. } => "gmeta",
                ArtifactRecord::GraphNode { id, .. } => {
                    assert!(["ada", "lin"].contains(&id.as_str()));
                    "gnode"
                }
                ArtifactRecord::GraphEdge {
                    src,
                    edge_type,
                    dst,
                    ..
                } => {
                    assert_eq!(
                        (src.as_str(), edge_type.as_str(), dst.as_str()),
                        ("ada", "knows", "lin")
                    );
                    "gedge"
                }
                other => panic!("unexpected record variant: {other:?}"),
            });
        }
    }
    assert_eq!(
        kinds,
        vec!["kv", "kv", "json", "event", "vconfig", "ventry", "gmeta", "gnode", "gedge", "gnode"]
    );
}

#[test]
fn truncated_section_bytes_surface_payload_corruption() {
    use strata_engine::artifact::{decode_section, ArtifactModel};

    let mut db = fresh_populated();
    let artifact = export(&mut db);
    let section = artifact
        .sections()
        .iter()
        .find(|section| section.model() == ArtifactModel::Kv)
        .expect("kv section");

    let truncated = &section.bytes()[..section.bytes().len() - 3];
    let error = decode_section(section.model(), truncated)
        .last()
        .expect("at least one item")
        .expect_err("truncation must fail");
    assert_eq!(error.code(), "corruption.engine.artifact_payload");
}
