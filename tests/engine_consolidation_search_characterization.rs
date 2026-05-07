//! Search characterization for the engine consolidation.
//!
//! These tests preserve the search behavior baseline now that search ownership
//! lives in engine.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use strata_core::{BranchId, Value};
use strata_engine::graph::types::NodeData;
use strata_engine::search::recipe::{
    BM25Config, FusionConfig, RetrieveConfig, TransformConfig, VectorRetrieveConfig,
};
use strata_engine::search::substrate::{self, RetrievalRequest};
use strata_engine::search::{EntityRef, Recipe};
use strata_engine::{
    open_product_cache, open_product_database, Database, DistanceMetric, EventLog, GraphStore,
    JsonStore, JsonValue, KVStore, OpenOptions, ProductOpenOutcome, VectorConfig, VectorStore,
};
use tempfile::tempdir;

#[derive(Debug, serde::Deserialize)]
struct SearchManifestSnapshot {
    version: u32,
    total_docs: u64,
    total_doc_len: u64,
    next_segment_id: u64,
    segments: Vec<SearchSegmentSnapshot>,
    doc_id_map: Vec<EntityRef>,
    #[serde(default)]
    doc_lengths: Vec<Option<u32>>,
    #[serde(default)]
    doc_terms: Vec<Option<Vec<String>>>,
    #[serde(default)]
    doc_content_hashes: Vec<Option<[u8; 32]>>,
}

#[derive(Debug, serde::Deserialize)]
struct SearchSegmentSnapshot {
    segment_id: u64,
    doc_count: u32,
    total_doc_len: u64,
    tombstones: HashSet<u32>,
}

fn local_db(outcome: ProductOpenOutcome) -> Arc<Database> {
    match outcome {
        ProductOpenOutcome::Local { db, .. } => db,
        other => panic!("expected local product open outcome, got {other:?}"),
    }
}

fn default_branch_id() -> BranchId {
    BranchId::from_user_name("default")
}

fn bm25_recipe(limit: usize) -> Recipe {
    Recipe {
        retrieve: Some(RetrieveConfig {
            bm25: Some(BM25Config {
                k: Some(50),
                k1: Some(0.9),
                b: Some(0.4),
                ..Default::default()
            }),
            ..Default::default()
        }),
        fusion: Some(FusionConfig {
            method: Some("rrf".into()),
            k: Some(60),
            ..Default::default()
        }),
        transform: Some(TransformConfig {
            limit: Some(limit),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn hybrid_recipe(limit: usize) -> Recipe {
    Recipe {
        retrieve: Some(RetrieveConfig {
            bm25: Some(BM25Config {
                k: Some(50),
                ..Default::default()
            }),
            vector: Some(VectorRetrieveConfig {
                k: Some(50),
                ..Default::default()
            }),
            ..Default::default()
        }),
        fusion: Some(FusionConfig {
            method: Some("rrf".into()),
            k: Some(60),
            ..Default::default()
        }),
        transform: Some(TransformConfig {
            limit: Some(limit),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn retrieve_query(
    db: &Arc<Database>,
    branch_id: BranchId,
    recipe: Recipe,
    query: &str,
    embedding: Option<Vec<f32>>,
) -> strata_engine::search::substrate::RetrievalResponse {
    let request = RetrievalRequest {
        query: query.to_string(),
        branch_id,
        space: "default".to_string(),
        recipe,
        embedding,
        time_range: None,
        primitive_filter: None,
        as_of: None,
        budget_ms: None,
    };
    substrate::retrieve(db, &request).expect("substrate retrieval should succeed")
}

fn insert_all_bm25_primitives(db: &Arc<Database>, branch_id: BranchId) {
    KVStore::new(db.clone())
        .put(
            &branch_id,
            "default",
            "kv-doc",
            Value::String("eg6a sentinel kv document".into()),
        )
        .expect("kv put should succeed");

    let json: JsonValue = serde_json::json!({
        "body": "eg6a sentinel json document"
    })
    .into();
    JsonStore::new(db.clone())
        .create(&branch_id, "default", "json-doc", json)
        .expect("json create should succeed");

    EventLog::new(db.clone())
        .append(
            &branch_id,
            "default",
            "eg6a_event",
            serde_json::json!({
                "body": "eg6a sentinel event document"
            })
            .into(),
        )
        .expect("event append should succeed");

    GraphStore::new(db.clone())
        .add_node(
            branch_id,
            "default",
            "eg6a-graph",
            "graph-doc",
            NodeData {
                entity_ref: None,
                properties: Some(serde_json::json!({
                    "body": "eg6a sentinel graph document"
                })),
                object_type: None,
            },
        )
        .expect("graph node insert should succeed");

    db.flush().expect("flush should succeed");
}

fn hit_kinds(hits: &[strata_engine::search::SearchHit]) -> BTreeSet<&'static str> {
    hits.iter()
        .map(|hit| match &hit.doc_ref {
            EntityRef::Kv { .. } => "kv",
            EntityRef::Json { .. } => "json",
            EntityRef::Event { .. } => "event",
            EntityRef::Graph { .. } => "graph",
            EntityRef::Vector { .. } => "vector",
            EntityRef::Branch { .. } => "branch",
        })
        .collect()
}

fn search_cache_files(search_dir: &Path) -> (bool, usize) {
    let manifest_exists = search_dir.join("search.manifest").exists();
    let segment_count = fs::read_dir(search_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sidx"))
                .count()
        })
        .unwrap_or(0);
    (manifest_exists, segment_count)
}

fn load_search_manifest(search_dir: &Path) -> SearchManifestSnapshot {
    let buf =
        fs::read(search_dir.join("search.manifest")).expect("search manifest should be readable");
    assert!(buf.len() >= 8, "search manifest should include header");
    assert_eq!(&buf[0..4], b"SMNF", "search manifest magic should match");
    assert_eq!(
        u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        3,
        "search manifest format version should remain unchanged"
    );
    rmp_serde::from_slice(&buf[8..]).expect("search manifest payload should decode")
}

fn assert_manifest_segments_are_readable(search_dir: &Path, manifest: &SearchManifestSnapshot) {
    assert_eq!(
        manifest.version, 1,
        "manifest payload schema marker should remain stable"
    );
    assert!(
        !manifest.segments.is_empty(),
        "search manifest should reference at least one sealed segment"
    );
    assert_eq!(
        manifest.total_docs as usize,
        manifest.doc_id_map.len(),
        "manifest total_docs should match doc_id_map length"
    );
    assert!(
        manifest.total_doc_len > 0,
        "manifest total_doc_len should record indexed content"
    );
    assert_eq!(
        manifest.doc_lengths.len(),
        manifest.doc_id_map.len(),
        "manifest doc_lengths should align with doc_id_map"
    );
    assert_eq!(
        manifest.doc_terms.len(),
        manifest.doc_id_map.len(),
        "manifest doc_terms should align with doc_id_map"
    );
    assert_eq!(
        manifest.doc_content_hashes.len(),
        manifest.doc_id_map.len(),
        "manifest content hashes should align with doc_id_map"
    );
    assert!(
        manifest.next_segment_id
            > manifest
                .segments
                .iter()
                .map(|segment| segment.segment_id)
                .max()
                .unwrap_or(0),
        "next_segment_id should be greater than all referenced segment ids"
    );

    for segment in &manifest.segments {
        assert!(
            segment.tombstones.len() <= segment.doc_count as usize,
            "segment tombstones cannot exceed segment doc_count"
        );
        let path = search_dir.join(format!("seg_{}.sidx", segment.segment_id));
        let bytes = fs::read(&path).expect("manifest-referenced segment should be readable");
        assert!(
            bytes.len() >= 56,
            "manifest-referenced segment should include the v2 SIDX header"
        );
        assert_eq!(&bytes[0..4], b"SIDX", "segment magic should match");
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            2,
            "sealed segment format version should remain unchanged"
        );
        assert_eq!(
            u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            segment.segment_id,
            "segment header id should match manifest entry"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            segment.doc_count,
            "segment header doc_count should match manifest entry"
        );
        assert_eq!(
            u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            segment.total_doc_len,
            "segment header total_doc_len should match manifest entry"
        );
    }
}

fn segment_bytes_by_id(
    search_dir: &Path,
    manifest: &SearchManifestSnapshot,
) -> Vec<(u64, Vec<u8>)> {
    manifest
        .segments
        .iter()
        .map(|segment| {
            let bytes = fs::read(search_dir.join(format!("seg_{}.sidx", segment.segment_id)))
                .expect("manifest-referenced segment should be readable");
            (segment.segment_id, bytes)
        })
        .collect()
}

#[test]
fn eg6a_product_open_installs_graph_vector_search_order() {
    let db = local_db(open_product_cache().expect("product cache should open"));

    assert_eq!(
        db.installed_subsystem_names(),
        vec!["graph", "vector", "search"],
        "product open must preserve runtime order for EG6 movement"
    );

    db.shutdown().expect("database should shut down");
}

#[test]
fn eg6a_substrate_bm25_fans_out_across_kv_json_event_and_graph() {
    let db = local_db(open_product_cache().expect("product cache should open"));
    let branch_id = default_branch_id();
    insert_all_bm25_primitives(&db, branch_id);

    let response = retrieve_query(&db, branch_id, bm25_recipe(20), "eg6a sentinel", None);
    let kinds = hit_kinds(&response.hits);

    assert_eq!(
        kinds,
        ["event", "graph", "json", "kv"]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        "BM25 substrate should fan out over all currently searchable primitives; hits: {:?}",
        response.hits
    );
    assert_eq!(
        response
            .stats
            .stages
            .get("bm25")
            .map(|stage| stage.candidates),
        Some(4),
        "BM25 stage should report one candidate per inserted primitive"
    );

    db.shutdown().expect("database should shut down");
}

#[test]
fn eg6a_substrate_hybrid_reads_vector_shadow_sources() {
    let db = local_db(open_product_cache().expect("product cache should open"));
    let branch_id = default_branch_id();
    KVStore::new(db.clone())
        .put(
            &branch_id,
            "default",
            "kv-doc",
            Value::String("backing source row for vector shadow".into()),
        )
        .expect("kv source row should be inserted");
    let vector = VectorStore::new(db.clone());
    vector
        .create_system_collection(
            branch_id,
            "_system_embed_kv",
            VectorConfig::new(3, DistanceMetric::Cosine).expect("valid vector config"),
        )
        .expect("system collection should be created");
    vector
        .system_insert_with_source(
            branch_id,
            "_system_embed_kv",
            "shadow-kv-doc",
            &[1.0, 0.0, 0.0],
            None,
            EntityRef::kv(branch_id, "default", "kv-doc"),
        )
        .expect("shadow vector should be inserted");
    db.flush().expect("flush should succeed");

    let response = retrieve_query(
        &db,
        branch_id,
        hybrid_recipe(10),
        "anything",
        Some(vec![1.0, 0.0, 0.0]),
    );

    assert_eq!(
        response
            .stats
            .stages
            .get("vector")
            .map(|stage| stage.candidates),
        Some(1),
        "hybrid substrate should read the system vector shadow collection"
    );
    assert_eq!(response.hits.len(), 1);
    assert!(matches!(
        response.hits[0].doc_ref,
        EntityRef::Kv { ref key, .. } if key == "kv-doc"
    ));

    db.shutdown().expect("database should shut down");
}

#[test]
fn eg6a_product_reopen_preserves_search_cache_files_and_results() {
    let dir = tempdir().expect("tempdir should succeed");
    let branch_id = default_branch_id();

    let db = local_db(
        open_product_database(dir.path(), OpenOptions::default())
            .expect("product primary should open"),
    );
    insert_all_bm25_primitives(&db, branch_id);

    let before = retrieve_query(&db, branch_id, bm25_recipe(20), "eg6a sentinel", None);
    let before_refs: Vec<_> = before.hits.iter().map(|hit| hit.doc_ref.clone()).collect();
    assert_eq!(
        hit_kinds(&before.hits),
        ["event", "graph", "json", "kv"]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );

    db.shutdown()
        .expect("shutdown should freeze search cache to disk");
    let search_dir = dir.path().join("search");
    let (manifest_before_reopen, segments_before_reopen) = search_cache_files(&search_dir);
    assert!(
        manifest_before_reopen,
        "shutdown should write the current search manifest"
    );
    assert!(
        segments_before_reopen > 0,
        "shutdown should write at least one .sidx segment"
    );
    let manifest_before = load_search_manifest(&search_dir);
    assert_manifest_segments_are_readable(&search_dir, &manifest_before);
    let segment_bytes_before = segment_bytes_by_id(&search_dir, &manifest_before);

    let reopened = local_db(
        open_product_database(dir.path(), OpenOptions::default())
            .expect("product primary should reopen"),
    );
    assert_eq!(
        reopened.installed_subsystem_names(),
        vec!["graph", "vector", "search"],
        "reopen should preserve product runtime composition"
    );

    let after = retrieve_query(&reopened, branch_id, bm25_recipe(20), "eg6a sentinel", None);
    let after_refs: Vec<_> = after.hits.iter().map(|hit| hit.doc_ref.clone()).collect();
    assert_eq!(
        after_refs, before_refs,
        "reopen should preserve search results after cache recovery"
    );

    let (manifest_after_reopen, segments_after_reopen) = search_cache_files(&search_dir);
    assert!(manifest_after_reopen);
    assert!(
        segments_after_reopen >= segments_before_reopen,
        "reopen should preserve usable .sidx cache files; before={segments_before_reopen}, \
         after={segments_after_reopen}"
    );
    let manifest_after = load_search_manifest(&search_dir);
    assert_manifest_segments_are_readable(&search_dir, &manifest_after);
    let segment_ids_after: BTreeSet<_> = manifest_after
        .segments
        .iter()
        .map(|segment| segment.segment_id)
        .collect();
    for (segment_id, bytes_before) in segment_bytes_before {
        assert!(
            segment_ids_after.contains(&segment_id),
            "reopen should keep pre-existing segment {segment_id} referenced"
        );
        let bytes_after = fs::read(search_dir.join(format!("seg_{segment_id}.sidx")))
            .expect("pre-existing segment should remain readable after reopen");
        assert_eq!(
            bytes_after, bytes_before,
            "reopen should not rewrite pre-existing sealed segment {segment_id}"
        );
    }

    reopened.shutdown().expect("database should shut down");
}
