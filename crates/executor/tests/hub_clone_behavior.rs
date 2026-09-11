//! `HubClone` behavior: the executor verb end-to-end against a live
//! in-process hub, plus its typed error paths.

#![cfg(feature = "hub")]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use strata_engine::{
    BranchName as EngineBranchName, Database, DurableLocalOpenOptions, KvKey, KvValue, ProductSpace,
};
use strata_executor::{
    Command, Executor, ExecutorErrorClass, HubCloneProgressStage, HubCloneResult, Output,
};
use strata_hub::stratahub_protocol::{ErrorCode, Hash, ProblemDetails};
use strata_hub::{EngineExportOptions, StrataCoreEngine};

const FIXTURE_HASH: &str =
    "blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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

fn serve_bundle(
    manifest_bytes: Vec<u8>,
    manifest_hash: &Hash,
    objects: HashMap<Hash, Vec<u8>>,
) -> String {
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

    std::thread::spawn(move || {
        let objects = Arc::new(objects);
        for _ in 0..64 {
            // #2803: the idle window must outlast sanitizer-slowed client
            // startup (tokio runtime + reqwest build take >5s under TSan;
            // a dead listener then eats the whole client retry budget). The
            // thread still self-reaps a minute after the test ends.
            let Ok(Some(request)) = server.recv_timeout(std::time::Duration::from_secs(60)) else {
                return;
            };
            let url = request.url().to_owned();
            let body = if url == "/v1/refs/titanic/default" {
                Some((reference.clone().into_bytes(), "application/json"))
            } else if url.starts_with("/v1/manifests/") {
                Some((manifest_bytes.clone(), "application/json"))
            } else if let Some(hash) = url.strip_prefix("/v1/objects/") {
                Hash::parse(hash)
                    .ok()
                    .and_then(|hash| objects.get(&hash).cloned())
                    .map(|body| (body, "application/octet-stream"))
            } else {
                None
            };
            match body {
                Some((bytes, content_type)) => {
                    let header = tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        content_type.as_bytes(),
                    )
                    .expect("header");
                    let _ =
                        request.respond(tiny_http::Response::from_data(bytes).with_header(header));
                }
                None => {
                    let _ = request.respond(tiny_http::Response::empty(404));
                }
            }
        }
    });
    base
}

fn serve_browse_hub() -> String {
    let server = tiny_http::Server::http("127.0.0.1:0").expect("server binds");
    let port = server.server_addr().to_ip().expect("ip addr").port();
    let base = format!("http://127.0.0.1:{port}");
    std::thread::spawn(move || {
        for _ in 0..16 {
            let Ok(Some(request)) = server.recv_timeout(std::time::Duration::from_secs(60)) else {
                return;
            };
            let url = request.url().to_owned();
            let path = url.split('?').next().unwrap_or(url.as_str());
            let body = match path {
                "/v1/info" => Some(serde_json::json!({
                    "protocol_version": "v1",
                    "server_implementation": "stratahub",
                    "server_version": "0.1.0",
                    "hash_algorithm": "blake3",
                    "max_object_size_bytes": 536_870_912_u64,
                    "max_manifest_size_bytes": 1_048_576_u64,
                    "max_dataset_size_bytes": 5_368_709_120_u64,
                    "supported_object_content_types": ["application/octet-stream"],
                    "telemetry_endpoint_enabled": false
                })),
                "/v1/datasets" => Some(serde_json::json!({
                    "total": 1,
                    "offset": 0,
                    "limit": 20,
                    "items": [hub_dataset_summary_json()]
                })),
                "/v1/datasets/titanic" => {
                    let mut card = hub_dataset_summary_json();
                    let object = card.as_object_mut().expect("summary is object");
                    object.extend(
                        serde_json::json!({
                            "owner": "stratahub",
                            "summary_excerpt": "Classic passenger-survival dataset.",
                            "created": "2026-09-01T00:00:00Z",
                            "manifest_hash": FIXTURE_HASH,
                            "engine_version_required": ">=1.1.0",
                            "format_version": "v1",
                            "capability_registry_version": 1,
                            "clone_command": "strata clone titanic",
                            "readme": "# Titanic\n",
                            "quick_start_snippets": {},
                            "frontmatter_extras": {}
                        })
                        .as_object()
                        .expect("card extras are object")
                        .clone(),
                    );
                    Some(card)
                }
                "/v1/datasets/titanic/refs" => Some(serde_json::json!({
                    "dataset": "titanic",
                    "default_branch": "main",
                    "refs": [{
                        "branch": "main",
                        "manifest_hash": FIXTURE_HASH,
                        "last_updated": "2026-09-02T00:00:00Z"
                    }]
                })),
                "/v1/yanked" => Some(serde_json::json!({
                    "generated_at": "2026-09-02T00:00:00Z",
                    "total": 0,
                    "items": []
                })),
                _ => None,
            };
            match body {
                Some(body) => {
                    let header =
                        tiny_http::Header::from_bytes(&b"Content-Type"[..], b"application/json")
                            .expect("header");
                    let _ = request.respond(
                        tiny_http::Response::from_string(body.to_string()).with_header(header),
                    );
                }
                None => {
                    let _ = request.respond(tiny_http::Response::empty(404));
                }
            }
        }
    });
    base
}

fn serve_not_found_hub(request_budget: usize) -> String {
    let server = tiny_http::Server::http("127.0.0.1:0").expect("server binds");
    let port = server.server_addr().to_ip().expect("ip addr").port();
    let base = format!("http://127.0.0.1:{port}");
    std::thread::spawn(move || {
        for _ in 0..request_budget {
            let Ok(Some(request)) = server.recv_timeout(std::time::Duration::from_secs(60)) else {
                return;
            };
            let code = if request.url().starts_with("/v1/datasets/") {
                ErrorCode::ResourceDatasetNotFound
            } else {
                ErrorCode::ResourceRefNotFound
            };
            let problem = ProblemDetails::from_code(code).with_detail("resource is absent");
            let header =
                tiny_http::Header::from_bytes(&b"Content-Type"[..], b"application/problem+json")
                    .expect("header");
            let _ = request.respond(
                tiny_http::Response::from_string(
                    serde_json::to_string(&problem).expect("problem serializes"),
                )
                .with_status_code(404)
                .with_header(header),
            );
        }
    });
    base
}

fn serve_problem_hub(
    status_code: u16,
    code: ErrorCode,
    detail: &'static str,
    request_budget: usize,
) -> String {
    let server = tiny_http::Server::http("127.0.0.1:0").expect("server binds");
    let port = server.server_addr().to_ip().expect("ip addr").port();
    let base = format!("http://127.0.0.1:{port}");
    std::thread::spawn(move || {
        for _ in 0..request_budget {
            let Ok(Some(request)) = server.recv_timeout(std::time::Duration::from_secs(60)) else {
                return;
            };
            let problem = ProblemDetails::from_code(code.clone()).with_detail(detail);
            let header =
                tiny_http::Header::from_bytes(&b"Content-Type"[..], b"application/problem+json")
                    .expect("header");
            let _ = request.respond(
                tiny_http::Response::from_string(
                    serde_json::to_string(&problem).expect("problem serializes"),
                )
                .with_status_code(status_code)
                .with_header(header),
            );
        }
    });
    base
}

fn expected_hub_problem_message(code: &ErrorCode, detail: &str) -> String {
    format!("hub returned {}: {detail}", code.as_str())
}

fn hub_dataset_summary_json() -> serde_json::Value {
    serde_json::json!({
        "name": "titanic",
        "description": "Classic passenger-survival dataset.",
        "size_bytes": 1024,
        "downloads": 7,
        "primitives": ["kv"],
        "tasks": ["classification"],
        "tags": ["tabular"],
        "license": "CC0",
        "default_branch": "main",
        "last_updated": "2026-09-02T00:00:00Z",
        "badge": "official"
    })
}

#[test]
fn hub_clone_reconstitutes_a_queryable_database_with_origin() {
    let source = tempfile::tempdir().expect("tempdir");
    build_source(source.path());
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    let manifest_hash =
        strata_hub::stratahub_protocol::hash_bytes(&output.manifest_canonical_bytes);
    let objects: HashMap<Hash, Vec<u8>> = output
        .objects
        .into_iter()
        .map(|object| (object.hash, object.bytes))
        .collect();
    let base = serve_bundle(output.manifest_canonical_bytes, &manifest_hash, objects);

    let workdir = tempfile::tempdir().expect("workdir");
    let dest = workdir.path().join("titanic.strata");

    let mut executor = Executor::open_cache().expect("cache executor");
    let outcome = executor
        .execute(Command::HubClone {
            dataset: "titanic".to_owned(),
            branch: Some("default".to_owned()),
            dest: dest.display().to_string(),
            hub_url: Some(base.clone()),
        })
        .expect("clone succeeds");
    let Output::HubCloneResult(HubCloneResult {
        dataset,
        branch,
        manifest_hash: reported_hash,
        ..
    }) = outcome
    else {
        panic!("unexpected output: {outcome:?}");
    };
    assert_eq!(dataset, "titanic");
    assert_eq!(branch, "default");
    assert_eq!(reported_hash, manifest_hash.as_str());

    // The clone answers reads and `remote_get` reports its origin.
    let mut clone_executor = Executor::open_durable_local(&dest).expect("clone opens");
    let origin = clone_executor
        .execute(Command::RemoteGet {})
        .expect("remote get");
    let Output::RemoteOriginResult { origin } = origin else {
        panic!("unexpected output");
    };
    let origin = origin.expect("origin recorded");
    assert_eq!(origin.hub_url_or_dataset(), ("titanic", base.as_str()));
}

#[test]
fn hub_browse_commands_return_executor_owned_envelopes() {
    let base = serve_browse_hub();
    let mut executor = Executor::open_cache().expect("cache executor");

    let info = executor
        .execute(Command::HubInfo {
            hub_url: Some(base.clone()),
        })
        .expect("info succeeds");
    assert!(matches!(info, Output::HubInfo(info) if info.protocol_version == "v1"));

    let page = executor
        .execute(Command::HubListDatasets {
            hub_url: Some(base.clone()),
            tasks: vec!["classification".to_owned()],
            tags: Vec::new(),
            primitives: vec!["kv".to_owned()],
            license: None,
            size_min_bytes: None,
            size_max_bytes: None,
            sort: None,
            limit: Some(20),
            offset: None,
        })
        .expect("list succeeds");
    assert!(matches!(page, Output::HubDatasets(page) if page.items[0].name == "titanic"));

    let card = executor
        .execute(Command::HubGetDataset {
            name: "titanic".to_owned(),
            hub_url: Some(base.clone()),
        })
        .expect("get succeeds");
    assert!(matches!(card, Output::HubDataset(card) if card.summary.name == "titanic"));

    let refs = executor
        .execute(Command::HubListRefs {
            dataset: "titanic".to_owned(),
            hub_url: Some(base.clone()),
        })
        .expect("refs succeeds");
    assert!(matches!(refs, Output::HubRefs(refs) if refs.refs[0].branch == "main"));

    let yanked = executor
        .execute(Command::HubListYanked {
            since: Some("2026-09-01T00:00:00Z".to_owned()),
            hub_url: Some(base),
        })
        .expect("yanked succeeds");
    assert!(matches!(yanked, Output::HubYanked(yanked) if yanked.total == 0));
}

#[test]
fn hub_browse_validates_local_arguments_before_network() {
    let mut executor = Executor::open_cache().expect("cache executor");
    let error = executor
        .execute(Command::HubListDatasets {
            hub_url: Some("http://127.0.0.1:9".to_owned()),
            tasks: Vec::new(),
            tags: Vec::new(),
            primitives: Vec::new(),
            license: None,
            size_min_bytes: Some(10),
            size_max_bytes: Some(1),
            sort: None,
            limit: Some(0),
            offset: None,
        })
        .expect_err("bad page refuses");
    assert_eq!(
        error.status().code(),
        "invalid_argument.executor.hub_filter"
    );

    let error = executor
        .execute(Command::HubListYanked {
            since: Some("not-rfc3339".to_owned()),
            hub_url: Some("http://127.0.0.1:9".to_owned()),
        })
        .expect_err("bad since refuses");
    assert_eq!(error.status().code(), "invalid_argument.executor.hub_since");
}

#[test]
fn hub_browse_validates_size_range_independently_of_limit() {
    let base = serve_browse_hub();
    let mut executor = Executor::open_cache().expect("cache executor");

    let error = executor
        .execute(Command::HubListDatasets {
            hub_url: Some(base.clone()),
            tasks: Vec::new(),
            tags: Vec::new(),
            primitives: Vec::new(),
            license: None,
            size_min_bytes: Some(10),
            size_max_bytes: Some(1),
            sort: None,
            limit: Some(20),
            offset: None,
        })
        .expect_err("inverted size range refuses");
    assert_eq!(
        error.status().code(),
        "invalid_argument.executor.hub_filter"
    );
    assert!(
        error.message().contains("size_min_bytes"),
        "unexpected message: {}",
        error.message()
    );

    for (size_min_bytes, size_max_bytes) in [(10, 10), (1, 10)] {
        let output = executor
            .execute(Command::HubListDatasets {
                hub_url: Some(base.clone()),
                tasks: Vec::new(),
                tags: Vec::new(),
                primitives: Vec::new(),
                license: None,
                size_min_bytes: Some(size_min_bytes),
                size_max_bytes: Some(size_max_bytes),
                sort: None,
                limit: Some(20),
                offset: None,
            })
            .expect("inclusive and ascending size ranges reach the hub");
        assert!(matches!(output, Output::HubDatasets(page) if page.items[0].name == "titanic"));
    }
}

#[test]
fn hub_browse_bad_request_errors_are_typed_with_problem_message() {
    let hub_code = ErrorCode::InputInvalidParam;
    let expected_message = expected_hub_problem_message(&hub_code, "limit must be at most 200");
    let base = serve_problem_hub(400, hub_code, "limit must be at most 200", 1);
    let mut executor = Executor::open_cache().expect("cache executor");

    let error = executor
        .execute(Command::HubListDatasets {
            hub_url: Some(base),
            tasks: Vec::new(),
            tags: Vec::new(),
            primitives: Vec::new(),
            license: None,
            size_min_bytes: None,
            size_max_bytes: None,
            sort: None,
            limit: Some(20),
            offset: None,
        })
        .expect_err("hub bad request refuses");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        error.status().code(),
        "invalid_argument.executor.hub_filter"
    );
    assert_eq!(error.message(), expected_message);
}

#[test]
fn hub_browse_not_found_errors_are_typed() {
    let base = serve_not_found_hub(2);
    let mut executor = Executor::open_cache().expect("cache executor");

    let dataset_code = ErrorCode::ResourceDatasetNotFound;
    let error = executor
        .execute(Command::HubGetDataset {
            name: "missing".to_owned(),
            hub_url: Some(base.clone()),
        })
        .expect_err("missing dataset refuses");
    assert_eq!(error.status().code(), "not_found.executor.hub_dataset");
    assert_eq!(
        error.message(),
        expected_hub_problem_message(&dataset_code, "resource is absent")
    );

    let resource_code = ErrorCode::ResourceRefNotFound;
    let error = executor
        .execute(Command::HubListYanked {
            since: None,
            hub_url: Some(base),
        })
        .expect_err("missing hub resource refuses");
    assert_eq!(error.status().code(), "not_found.executor.hub_resource");
    assert_eq!(
        error.message(),
        expected_hub_problem_message(&resource_code, "resource is absent")
    );
}

#[test]
fn hub_clone_progress_callback_reports_machine_readable_stages() {
    let source = tempfile::tempdir().expect("tempdir");
    build_source(source.path());
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    let manifest_hash =
        strata_hub::stratahub_protocol::hash_bytes(&output.manifest_canonical_bytes);
    let objects: HashMap<Hash, Vec<u8>> = output
        .objects
        .into_iter()
        .map(|object| (object.hash, object.bytes))
        .collect();
    let base = serve_bundle(output.manifest_canonical_bytes, &manifest_hash, objects);

    let workdir = tempfile::tempdir().expect("workdir");
    let dest = workdir.path().join("titanic.strata");
    let mut events = Vec::new();
    let mut executor = Executor::open_cache().expect("cache executor");
    let outcome = executor
        .execute_hub_clone_with_progress(
            "titanic",
            Some("default"),
            &dest.display().to_string(),
            Some(base),
            &mut |event| events.push(event),
        )
        .expect("clone succeeds");

    assert!(matches!(outcome, Output::HubCloneResult(_)));
    let stages = events
        .iter()
        .map(|event| match event {
            Output::HubCloneProgress(progress) => progress.stage,
            other => panic!("unexpected progress output: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(stages.len() >= 5, "expected at least one object event");
    assert_eq!(
        stages.first(),
        Some(&HubCloneProgressStage::Resolved),
        "first stage resolves the ref"
    );
    assert_eq!(
        stages.get(1),
        Some(&HubCloneProgressStage::ManifestFetched),
        "second stage counts the manifest"
    );
    assert_eq!(
        stages.get(stages.len().saturating_sub(2)),
        Some(&HubCloneProgressStage::Importing),
        "penultimate stage starts import"
    );
    assert_eq!(
        stages.last(),
        Some(&HubCloneProgressStage::Done),
        "last stage completes the clone"
    );
    assert!(
        !stages[2..stages.len() - 2].is_empty()
            && stages[2..stages.len() - 2]
                .iter()
                .all(|stage| *stage == HubCloneProgressStage::ObjectFetched),
        "manifest/object phase should contain only object fetch events between manifest and import"
    );
}

/// Small accessor shim so the assertion reads clearly.
trait OriginFacts {
    fn hub_url_or_dataset(&self) -> (&str, &str);
}
impl OriginFacts for strata_executor::RemoteOriginInfo {
    fn hub_url_or_dataset(&self) -> (&str, &str) {
        (self.dataset.as_str(), self.remote_url.as_str())
    }
}

#[test]
fn hub_clone_without_configuration_reports_the_hub_url_code() {
    // Explicitly break resolution for this invocation: an invalid flag
    // value fails at the flag layer, env-independent.
    let mut executor = Executor::open_cache().expect("cache executor");
    let error = executor
        .execute(Command::HubClone {
            dataset: "titanic".to_owned(),
            branch: None,
            dest: "unused".to_owned(),
            hub_url: Some("not a url".to_owned()),
        })
        .expect_err("malformed hub URL refuses");
    assert_eq!(error.status().code(), "invalid_argument.executor.hub_url");
}

#[test]
fn hub_clone_transport_failures_report_the_transport_code() {
    let workdir = tempfile::tempdir().expect("workdir");
    let mut executor = Executor::open_cache().expect("cache executor");
    // A live listener that 404s everything: resolve_ref fails on the wire.
    let server = tiny_http::Server::http("127.0.0.1:0").expect("binds");
    let port = server.server_addr().to_ip().expect("addr").port();
    std::thread::spawn(move || {
        // #2803: same sanitizer-startup window as the full mock above.
        while let Ok(Some(request)) = server.recv_timeout(std::time::Duration::from_secs(60)) {
            let _ = request.respond(tiny_http::Response::empty(404));
        }
    });
    let error = executor
        .execute(Command::HubClone {
            dataset: "titanic".to_owned(),
            branch: Some("default".to_owned()),
            dest: workdir.path().join("x").display().to_string(),
            hub_url: Some(format!("http://127.0.0.1:{port}")),
        })
        .expect_err("transport failure refuses");
    assert_eq!(error.status().code(), "unavailable.executor.hub_transport");
}

#[test]
fn hub_clone_of_an_incompatible_bundle_reports_the_precondition_code() {
    // A well-formed bundle whose manifest demands an engine this build cannot
    // satisfy. The pre-download compatibility gate refuses it, and the failure
    // surfaces through the executor as the non-retryable clone envelope
    // (TCP3.13). No objects are served: the gate fires before any download.
    let source = tempfile::tempdir().expect("tempdir");
    build_source(source.path());
    let mut engine = StrataCoreEngine::open(source.path()).expect("open");
    let output = engine
        .export_bundle(&EngineExportOptions::default())
        .expect("export");
    let mut manifest = output.manifest;
    manifest.engine_compatibility.required_engine_version = ">=99.0.0".to_owned();
    let manifest_bytes = manifest.canonical_bytes().expect("canonicalize");
    let manifest_hash = strata_hub::stratahub_protocol::hash_bytes(&manifest_bytes);
    let base = serve_bundle(manifest_bytes, &manifest_hash, HashMap::new());

    let workdir = tempfile::tempdir().expect("workdir");
    let dest = workdir.path().join("titanic.strata");
    let mut executor = Executor::open_cache().expect("cache executor");
    let error = executor
        .execute(Command::HubClone {
            dataset: "titanic".to_owned(),
            branch: Some("default".to_owned()),
            dest: dest.display().to_string(),
            hub_url: Some(base),
        })
        .expect_err("incompatible bundle refuses");
    assert_eq!(
        error.status().code(),
        "failed_precondition.executor.hub_clone"
    );
    assert!(
        !dest.exists(),
        "no destination state on an incompatible clone"
    );
}
