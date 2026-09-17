//! `HB5`: the M8E2 `Engine` trait impl delegates faithfully — the async
//! trait surface produces byte-identical results to the sync export.

use futures_util::StreamExt;
use serde_json::json;
use strata_engine::{
    BranchName, Database, DurableLocalOpenOptions, JsonDocumentId, JsonPath, JsonValue, KvKey,
    KvValue, ProductSpace,
};
use strata_hub::{EngineExportOptions, IngestEngine, StrataCoreEngine};
use stratahub_ingest::engine::{Engine, EngineError};
use tokio::io::AsyncReadExt;

fn build_fixture(path: &std::path::Path) {
    let db = Database::open_local(path, DurableLocalOpenOptions::new())
        .expect("fixture opens")
        .into_database();
    let branch = || BranchName::new("default").expect("branch");
    let space = || ProductSpace::new("default").expect("space");
    let mut kv = db.kv(branch(), space()).expect("kv");
    kv.put(
        KvKey::new("user:ada").expect("key"),
        KvValue::new(b"engineer".to_vec()),
    )
    .expect("put");
    let mut json = db.json(branch(), space()).expect("json");
    json.set_or_create(
        JsonDocumentId::new("config").expect("id"),
        &JsonPath::root(),
        JsonValue::new(json!({"model": "claude"})).expect("value"),
    )
    .expect("set");
}

#[test]
fn trait_export_matches_the_sync_export_byte_for_byte() {
    let source = tempfile::tempdir().expect("tempdir");
    build_fixture(source.path());

    // Sync reference output.
    let mut sync_engine = StrataCoreEngine::open(source.path()).expect("open");
    let sync_output = sync_engine
        .export_bundle(&EngineExportOptions::default())
        .expect("sync export");

    // Async trait output.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let engine = IngestEngine::new();
    let output = runtime
        .block_on(engine.export_bundle(
            source.path(),
            &stratahub_ingest::engine::EngineExportOptions::default(),
        ))
        .expect("trait export");

    assert_eq!(
        output.manifest_canonical_bytes.as_ref(),
        sync_output.manifest_canonical_bytes.as_slice(),
        "canonical manifest bytes agree across the trait boundary"
    );
    stratahub_ingest::engine::verify_roundtrip(&output.manifest, &output.manifest_canonical_bytes)
        .expect("their round-trip invariant holds");

    // Drain the object stream; every body hashes to its declared hash.
    let objects = runtime.block_on(async {
        let mut stream = output.objects;
        let mut collected = Vec::new();
        while let Some(object) = stream.next().await {
            let mut object = object.expect("object yields");
            let mut body = Vec::new();
            object
                .body
                .read_to_end(&mut body)
                .await
                .expect("body reads");
            assert_eq!(stratahub_protocol::hash_bytes(&body), object.hash);
            assert_eq!(body.len() as u64, object.size_bytes);
            collected.push((object.hash, body));
        }
        collected
    });
    assert_eq!(objects.len(), sync_output.objects.len());
    for (theirs, ours) in objects.iter().zip(&sync_output.objects) {
        assert_eq!(theirs.0, ours.hash);
        assert_eq!(theirs.1, ours.bytes);
    }

    // Auxiliary blobs and their hashes cross the boundary intact.
    assert_eq!(
        output.schema_blob.as_deref(),
        sync_output.schema_blob.as_deref()
    );
    assert_eq!(
        output.auxiliary_hashes.schema,
        sync_output.auxiliary_hashes.schema
    );

    // engine_info is stable and matches the sync surface.
    let info = engine.engine_info();
    assert_eq!(info.version, strata_hub::engine_info().version);
    assert_eq!(
        info.capability_registry_version,
        strata_hub::CAPABILITY_REGISTRY_VERSION
    );
}

#[test]
fn trait_errors_map_one_to_one() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let engine = IngestEngine::new();

    let empty = tempfile::tempdir().expect("tempdir");
    let missing = empty.path().join("nothing-here");
    let error = runtime
        .block_on(engine.export_bundle(
            &missing,
            &stratahub_ingest::engine::EngineExportOptions::default(),
        ))
        .expect_err("missing source refuses");
    assert!(matches!(error, EngineError::NotAStrataDb(_)));

    let source = tempfile::tempdir().expect("tempdir");
    build_fixture(source.path());
    let mut options = stratahub_ingest::engine::EngineExportOptions::default();
    options.branches = vec![stratahub_protocol::BranchName::parse("nope").expect("branch name")];
    let error = runtime
        .block_on(engine.export_bundle(source.path(), &options))
        .expect_err("unknown branch refuses");
    assert!(matches!(error, EngineError::BranchNotFound(_)));
}
