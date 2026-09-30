//! `HB7b` end-to-end: `strata clone` mechanics over real HTTP —
//! `stratahub-client` → `ClientTransport` → clone orchestration → local
//! database, served by an in-process hub speaking the V1 wire protocol.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use strata_engine::{
    BranchName as EngineBranchName, Database, DurableLocalOpenOptions, KvKey, KvValue, ProductSpace,
};
use strata_hub::{
    clone_dataset, read_remote_tracking_ref, ClientTransport, CloneRequest, EngineExportOptions,
    StrataCoreEngine,
};
use stratahub_protocol::{BranchName, DatasetName, Hash};

fn build_source(path: &Path) {
    let db = Database::open_local(path, DurableLocalOpenOptions::new())
        .expect("source opens")
        .into_database();
    let mut kv = db
        .kv(
            EngineBranchName::new("default").expect("branch"),
            ProductSpace::new("default").expect("space"),
        )
        .expect("kv");
    kv.put(
        KvKey::new("user:ada").expect("key"),
        KvValue::new(b"engineer".to_vec()),
    )
    .expect("put");
}

/// Serves the V1 read endpoints the clone flow hits, from a real
/// exported bundle, over plain HTTP on an ephemeral port.
fn serve_bundle(
    manifest_bytes: Vec<u8>,
    manifest_hash: Hash,
    objects: HashMap<Hash, Vec<u8>>,
) -> (String, std::thread::JoinHandle<()>) {
    let server = tiny_http::Server::http("127.0.0.1:0").expect("server binds");
    let port = server.server_addr().to_ip().expect("ip addr").port();
    let base = format!("http://127.0.0.1:{port}");

    let reference = serde_json::json!({
        "dataset": "titanic",
        "branch": "default",
        "manifest_hash": manifest_hash.as_str(),
        "last_updated": "2026-07-10T00:00:00Z",
    })
    .to_string();

    let handle = std::thread::spawn(move || {
        let objects = Arc::new(objects);
        // Serve until the test drops the client side; each clone makes a
        // bounded number of requests, so cap generously and exit.
        for _ in 0..64 {
            let Ok(Some(request)) = server.recv_timeout(std::time::Duration::from_secs(5)) else {
                return;
            };
            let url = request.url().to_owned();
            let respond = |request: tiny_http::Request, body: Vec<u8>, content_type: &str| {
                let header =
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], content_type.as_bytes())
                        .expect("header");
                let response = tiny_http::Response::from_data(body).with_header(header);
                let _ = request.respond(response);
            };

            if url == "/v1/refs/titanic/default" {
                respond(request, reference.clone().into_bytes(), "application/json");
            } else if let Some(hash) = url.strip_prefix("/v1/manifests/") {
                assert_eq!(hash, manifest_hash.as_str());
                respond(request, manifest_bytes.clone(), "application/json");
            } else if let Some(hash) = url.strip_prefix("/v1/objects/") {
                let hash = Hash::parse(hash).expect("hash parses");
                let body = objects.get(&hash).expect("object known").clone();
                respond(request, body, "application/octet-stream");
            } else {
                let _ = request.respond(tiny_http::Response::empty(404));
            }
        }
    });
    (base, handle)
}

#[test]
fn clone_over_real_http_reconstitutes_and_records_origin() {
    let source = tempfile::tempdir().expect("tempdir");
    build_source(source.path());
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    let manifest_hash = stratahub_protocol::hash_bytes(&output.manifest_canonical_bytes);
    let objects: HashMap<Hash, Vec<u8>> = output
        .objects
        .into_iter()
        .map(|object| (object.hash, object.bytes))
        .collect();

    let (base, server) = serve_bundle(
        output.manifest_canonical_bytes.clone(),
        manifest_hash.clone(),
        objects,
    );

    let transport =
        ClientTransport::new(url::Url::parse(&base).expect("url")).expect("transport builds");
    let workdir = tempfile::tempdir().expect("workdir");
    let dest = workdir.path().join("titanic.strata");
    let outcome = clone_dataset(
        &transport,
        &CloneRequest {
            dataset: DatasetName::parse("titanic").expect("dataset"),
            branch: Some(BranchName::parse("default").expect("branch")),
            dest: dest.clone(),
        },
        &mut |_| {},
    )
    .expect("clone over HTTP succeeds");
    assert_eq!(outcome.manifest_hash, manifest_hash);

    // The clone serves reads and carries its origin, hub URL included.
    let mut db = Database::open_local(&dest, DurableLocalOpenOptions::new())
        .expect("clone opens")
        .into_database();
    let mut kv = db
        .kv(
            EngineBranchName::new("default").expect("branch"),
            ProductSpace::new("default").expect("space"),
        )
        .expect("kv");
    let value = kv
        .get(&KvKey::new("user:ada").expect("key"))
        .expect("get")
        .expect("present");
    assert_eq!(value.as_bytes(), b"engineer");
    drop(kv);
    // Close, not drop: a dropped handle may leave a detached worker holding
    // the writer lock past the reopen below (#2837, #3694).
    db.close().expect("clean close");

    let tracking_ref = read_remote_tracking_ref(&dest)
        .expect("read ref")
        .expect("recorded");
    assert_eq!(tracking_ref.hub_url, base);
    assert_eq!(tracking_ref.manifest_hash, manifest_hash);

    drop(transport);
    let _ = server;
}
