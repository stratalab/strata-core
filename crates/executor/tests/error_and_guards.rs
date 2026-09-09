//! Executor error-boundary and source-guard tests.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use strata_executor::{
    public_error_code_entries, public_error_code_entry, with_error_render_config, Bytes, Command,
    CommitOutcomeStatus, ErrorClass, ErrorCodeRegistryEntry, ErrorReferenceIdSource,
    ErrorRenderConfig, ErrorStatus, Executor, ExecutorError, ExecutorErrorClass, RetryPolicy,
};

#[test]
fn executor_errors_have_stable_public_shape() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let invalid_key = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: Bytes::new(Vec::new()),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("empty key fails");
    assert_eq!(invalid_key.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(invalid_key.public_class(), ErrorClass::InvalidArgument);
    assert_eq!(invalid_key.retry_policy(), RetryPolicy::Never);
    assert_eq!(
        invalid_key.commit_outcome(),
        CommitOutcomeStatus::NotStarted
    );
    assert!(!invalid_key.suggested_fix().is_empty());
    assert!(invalid_key.docs_url().ends_with(invalid_key.code()));
    assert!(invalid_key.reference_id().starts_with("err_local_"));
    assert!(invalid_key.code().contains(".engine."));

    let invalid_space = executor
        .execute(Command::KvPut {
            branch: None,
            space: Some("_system_".to_owned()),
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        })
        .expect_err("reserved space fails");
    assert_eq!(invalid_space.class(), ExecutorErrorClass::InvalidInput);

    let missing_branch = executor
        .execute(Command::KvPut {
            branch: Some("missing".to_owned()),
            space: None,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        })
        .expect_err("missing branch fails");
    assert_eq!(missing_branch.class(), ExecutorErrorClass::NotFound);

    executor.close().expect("close succeeds");
    let closed = executor
        .execute(Command::KvExists {
            branch: None,
            space: None,
            key: Bytes::from("key"),
        })
        .expect_err("closed executor fails");
    assert_eq!(closed.class(), ExecutorErrorClass::ClosedHandle);
    assert_eq!(closed.public_class(), ErrorClass::FailedPrecondition);
    assert_eq!(closed.retry_policy(), RetryPolicy::Never);
    assert_eq!(closed.commit_outcome(), CommitOutcomeStatus::NotStarted);
}

#[derive(Debug)]
struct FixedReferenceIdSource;

impl ErrorReferenceIdSource for FixedReferenceIdSource {
    fn next_reference_id(&self) -> String {
        "ref_test_000001".to_owned()
    }
}

#[test]
fn executor_error_rendering_uses_injected_boundary_config() {
    let config = ErrorRenderConfig::new(
        "https://docs.example.test/errors/",
        Arc::new(FixedReferenceIdSource),
    );

    let error = with_error_render_config(config, || {
        ExecutorError::new("invalid_argument.executor.batch_item", "public message")
    });

    assert_eq!(error.reference_id(), "ref_test_000001");
    assert_eq!(
        error.docs_url(),
        "https://docs.example.test/errors/e/invalid_argument.executor.batch_item"
    );
}

#[test]
fn executor_preserves_engine_error_codes_at_public_boundary() {
    let error: ExecutorError = strata_engine::EngineError::closed_runtime("runtime closed").into();

    assert_eq!(error.code(), "failed_precondition.engine.runtime_closed");
    assert_eq!(error.public_class(), ErrorClass::FailedPrecondition);
    assert_eq!(error.commit_outcome(), CommitOutcomeStatus::NotStarted);
    assert!(error.docs_url().ends_with(error.code()));
}

#[test]
fn serialized_errors_have_v1_status_shape() {
    let error = ExecutorError::new("invalid_argument.executor.batch_item", "public message");
    let encoded = serde_json::to_string(&error).expect("error serializes");
    let status: serde_json::Value = serde_json::from_str(&encoded).expect("json parses");

    assert_eq!(status["class"], "invalid_argument");
    assert_eq!(status["code"], "invalid_argument.executor.batch_item");
    assert_eq!(status["retry_policy"], "never");
    assert_eq!(status["retryable"], false);
    assert_eq!(status["commit_outcome"], "not_started");
    assert_eq!(status["message"], "public message");
    assert_eq!(
        status["suggested_fix"],
        "Correct the batch item input and retry."
    );
    assert_eq!(
        status["docs_url"],
        "https://stratadb.org/e/invalid_argument.executor.batch_item"
    );
    assert!(status["reference_id"]
        .as_str()
        .expect("reference id is a string")
        .starts_with("err_local_"));

    for forbidden in forbidden_lower_layer_terms() {
        assert!(
            !encoded.contains(forbidden),
            "serialized error leaked forbidden term `{forbidden}`: {encoded}"
        );
    }
}

fn status_from_json(value: serde_json::Value) -> ErrorStatus {
    serde_json::from_value(value).expect("error status deserializes")
}

#[test]
fn deserialized_error_status_derives_retryable_from_retry_policy() {
    let status = status_from_json(serde_json::json!({
        "class": "unavailable",
        "code": "unavailable.executor.lock_unavailable",
        "retry_policy": "same_request",
        "retryable": false,
        "commit_outcome": "not_started",
        "message": "lock unavailable",
        "suggested_fix": "Retry the same request after the lock becomes available.",
        "docs_url": "https://stratadb.org/e/unavailable.executor.lock_unavailable",
        "reference_id": "err-test-000001"
    }));

    assert_eq!(status.retry_policy(), RetryPolicy::SameRequest);
    assert!(status.retryable());
}

#[test]
fn unregistered_executor_errors_render_registered_internal_fallback() {
    let error = ExecutorError::new("invalid_argument.executor.test", "public message");

    assert_eq!(error.code(), "internal.executor.unregistered_code");
    assert_eq!(error.public_class(), ErrorClass::Internal);
    assert_eq!(error.retry_policy(), RetryPolicy::Unknown);
    assert_eq!(error.commit_outcome(), CommitOutcomeStatus::NotApplicable);
    assert!(error
        .status()
        .details()
        .iter()
        .any(|detail| detail.key() == "unregistered_code"
            && detail.value() == "invalid_argument.executor.test"));
}

/// A wire status is a DTO; `from_status` is where the boundary re-establishes
/// the registry as the authority. An unregistered code collapses to the
/// registered fallback, keeps the original code as a detail, and has its docs
/// URL rewritten to the fallback's page whatever base the sender used.
#[test]
fn from_status_normalizes_unregistered_codes_and_docs_urls() {
    let wire = |docs_url: &str| {
        status_from_json(serde_json::json!({
            "class": "invalid_argument",
            "code": "invalid_argument.executor.from_status_test",
            "retry_policy": "never",
            "retryable": false,
            "commit_outcome": "not_started",
            "message": "public message",
            "suggested_fix": "custom fix",
            "docs_url": docs_url,
            "reference_id": "err-test-from-status"
        }))
    };

    let error = ExecutorError::from_status(wire("https://example.invalid/errors#wrong-anchor"));
    assert_eq!(error.code(), "internal.executor.unregistered_code");
    assert_eq!(error.public_class(), ErrorClass::Internal);
    assert_eq!(error.retry_policy(), RetryPolicy::Unknown);
    assert_eq!(error.commit_outcome(), CommitOutcomeStatus::NotApplicable);
    assert_eq!(
        error.docs_url(),
        "https://stratadb.org/e/internal.executor.unregistered_code"
    );
    assert!(error
        .status()
        .details()
        .iter()
        .any(|detail| detail.key() == "unregistered_code"
            && detail.value() == "invalid_argument.executor.from_status_test"));

    let error = ExecutorError::from_status(wire("https://docs.example.test/errors/e#wrong-anchor"));
    assert_eq!(
        error.docs_url(),
        "https://docs.example.test/errors/e/internal.executor.unregistered_code"
    );
}

/// #3244: a registered code arriving over the wire with every row field wrong
/// (a stale peer, a hand-built status) crosses the boundary carrying the local
/// registry's class, retry policy, commit outcome and suggested fix — the
/// sender owns only the message, ids, details and hints.
#[test]
fn from_status_re_derives_the_registry_row_and_keeps_the_site_fields() {
    let code = "unavailable.executor.lock_unavailable";
    let entry = public_error_code_entry(code).expect("lock_unavailable is registered");
    let error = ExecutorError::from_status(status_from_json(serde_json::json!({
        "class": "internal",
        "code": code,
        "retry_policy": "never",
        "retryable": false,
        "commit_outcome": "maybe_committed",
        "message": "lock unavailable",
        "suggested_fix": "site text that must not survive",
        "docs_url": "https://stratadb.org/e/unavailable.executor.lock_unavailable",
        "reference_id": "err-test-000001",
        "trace_id": "trace-000001",
        "details": [{"key": "path", "value": "db"}],
        "hints": ["site hint that must survive"]
    })));

    assert_eq!(error.code(), code);
    assert_eq!(error.public_class(), entry.class);
    assert_eq!(error.retry_policy(), entry.retry_policy);
    assert_eq!(error.retryable(), error.status().retryable());
    assert_eq!(error.commit_outcome(), entry.commit_outcome);
    assert_eq!(error.suggested_fix(), entry.suggested_fix);
    assert_eq!(error.message(), "lock unavailable");
    assert_eq!(error.reference_id(), "err-test-000001");
    assert_eq!(error.status().trace_id(), Some("trace-000001"));
    assert_eq!(
        error.docs_url(),
        "https://stratadb.org/e/unavailable.executor.lock_unavailable"
    );
    assert!(error
        .status()
        .details()
        .iter()
        .any(|detail| detail.key() == "path" && detail.value() == "db"));
    assert_eq!(error.status().hints(), ["site hint that must survive"]);
}

#[test]
fn public_error_registry_has_reviewed_metadata() {
    let mut codes = std::collections::BTreeSet::new();
    for entry in public_error_code_entries() {
        assert!(
            codes.insert(entry.code),
            "duplicate public error code {}",
            entry.code
        );
        assert_eq!(
            entry.docs_slug, entry.code,
            "{} docs slug drifted",
            entry.code
        );
        assert!(
            !entry.suggested_fix.trim().is_empty(),
            "{} has an empty suggested fix",
            entry.code
        );
        assert!(
            !entry.message_template.trim().is_empty(),
            "{} has an empty message template",
            entry.code
        );
        assert!(
            !entry.suggested_fix.contains("Correct the command input"),
            "{} kept a generic executor suggested fix",
            entry.code
        );
        assert!(
            !entry.details_schema.trim().is_empty(),
            "{} has an empty details schema",
            entry.code
        );
        assert_registry_class_matches_prefix(entry);
    }
}

#[test]
fn every_executor_source_error_code_is_registered() {
    let mut unregistered = Vec::new();
    for file in source_files(&crate_root().join("src")) {
        if file.ends_with("error_registry.rs") {
            continue;
        }
        let text = fs::read_to_string(&file).expect("source reads");
        for code in extract_public_error_codes(&text, ".executor.") {
            if public_error_code_entry(&code).is_none() {
                unregistered.push(format!("{}:{}", file.display(), code));
            }
        }
    }

    assert!(
        unregistered.is_empty(),
        "executor source emits unregistered public error codes: {unregistered:?}"
    );
}

#[test]
fn every_inference_error_code_is_registered() {
    let inference_error_source =
        fs::read_to_string(workspace_root().join("crates/inference/src/error.rs"))
            .expect("inference error source reads");
    let unregistered: Vec<String> =
        extract_public_error_codes(&inference_error_source, "inference.")
            .into_iter()
            .filter(|code| public_error_code_entry(code).is_none())
            .collect();

    assert!(
        unregistered.is_empty(),
        "inference source emits unregistered public error codes: {unregistered:?}"
    );
}

#[test]
fn every_registry_docs_url_has_target() {
    let docs_target = workspace_root().join("docs/errors/registry.md");
    let docs = fs::read_to_string(&docs_target).expect("error registry docs target exists");

    assert!(
        docs.contains("# Error Code Registry"),
        "{} is not the error registry docs page",
        docs_target.display()
    );
    for entry in public_error_code_entries() {
        // Stable short per-code slug (first-run D4): the code is the final
        // path segment, so every code resolves to its own docs page.
        let url = format!("https://stratadb.org/e/{}", entry.docs_slug);
        assert!(
            url.ends_with(entry.code),
            "{} docs URL does not include code",
            entry.code
        );
        let anchor = format!(r#"<a id="{}"></a>"#, entry.docs_slug);
        assert!(
            docs.contains(&anchor),
            "{} is missing docs anchor {}",
            entry.code,
            anchor
        );
    }
}

#[test]
fn public_response_fixtures_are_valid_pretty_json_files() {
    for file in response_fixture_files() {
        let text = fs::read_to_string(&file).expect("response fixture reads");
        assert!(
            text.ends_with('\n'),
            "{} must end with a trailing newline",
            file.display()
        );
        assert!(
            text.contains("\n  "),
            "{} must be pretty-printed",
            file.display()
        );
        let _: Value = serde_json::from_str(&text)
            .unwrap_or_else(|error| panic!("{} is invalid JSON: {error}", file.display()));
    }
}

#[test]
fn public_response_fixtures_do_not_leak_lower_layer_details() {
    for file in response_fixture_files() {
        let text = fs::read_to_string(&file).expect("response fixture reads");
        for forbidden in forbidden_response_fixture_terms() {
            assert!(
                !text.contains(forbidden),
                "{} leaked lower-layer response detail `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn public_response_fixtures_preserve_page_contract() {
    for file in response_fixture_files() {
        let text = fs::read_to_string(&file).expect("response fixture reads");
        let value: Value = serde_json::from_str(&text).expect("response fixture parses");
        assert_page_contract(&value, &file.display().to_string());
    }
}

#[test]
fn public_response_fixtures_never_serialize_string_only_item_errors() {
    for file in response_fixture_files() {
        let text = fs::read_to_string(&file).expect("response fixture reads");
        let value: Value = serde_json::from_str(&text).expect("response fixture parses");
        assert_no_string_error_field(&value, &file.display().to_string());
    }
}

#[test]
fn executor_crate_does_not_depend_on_storage_crates() {
    let manifest = fs::read_to_string(crate_root().join("Cargo.toml")).expect("manifest reads");
    assert!(!manifest.contains("strata-storage"));
    assert!(!manifest.contains("strata_storage"));
}

#[test]
fn executor_sources_do_not_name_lower_layer_types() {
    for file in source_files(&crate_root().join("src")) {
        let text = fs::read_to_string(&file).expect("source reads");
        for forbidden in forbidden_lower_layer_terms() {
            assert!(
                !text.contains(forbidden),
                "{} leaked forbidden term `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn executor_event_sources_do_not_own_event_product_behavior() {
    for file in source_files(&crate_root().join("src")) {
        if file
            .file_name()
            .is_some_and(|name| name == "idl_tooling.rs" || name == "cli_metadata.rs")
        {
            continue;
        }
        let text = fs::read_to_string(&file).expect("source reads");
        for forbidden in forbidden_event_lower_layer_terms() {
            assert!(
                !text.contains(forbidden),
                "{} leaked forbidden event lower-layer term `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn executor_vector_sources_do_not_own_index_or_distance_behavior() {
    for file in source_files(&crate_root().join("src")) {
        let text = fs::read_to_string(&file).expect("source reads");
        for forbidden in forbidden_vector_index_lower_layer_terms() {
            assert!(
                !text.contains(forbidden),
                "{} leaked forbidden vector lower-layer term `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn command_and_output_are_serde_serializable() {
    let command_source =
        fs::read_to_string(crate_root().join("src/command.rs")).expect("command reads");
    let output_source =
        fs::read_to_string(crate_root().join("src/output.rs")).expect("output reads");

    assert!(command_source.contains("Serialize"));
    assert!(command_source.contains("Deserialize"));
    assert!(output_source.contains("Serialize"));
    assert!(output_source.contains("Deserialize"));
}

#[test]
fn convenience_facade_stays_command_shaped() {
    let mut facade = fs::read_to_string(crate_root().join("src/executor/facade.rs"))
        .expect("executor facade reads");
    for file in source_files(&crate_root().join("src/executor/facade")) {
        facade.push_str(&fs::read_to_string(file).expect("executor facade module reads"));
    }

    assert!(facade.contains("self.execute(Command::KvPut"));
    assert!(facade.contains("self.execute(Command::KvBatchPut"));
    assert!(facade.contains("self.execute(Command::JsonSet"));
    assert!(facade.contains("self.execute(Command::JsonGet"));
    assert!(facade.contains("self.execute(Command::JsonDelete"));
    assert!(facade.contains("self.execute(Command::JsonBatchSet"));
    assert!(facade.contains("self.execute(Command::JsonBatchGet"));
    assert!(facade.contains("self.execute(Command::JsonBatchDelete"));
    assert!(facade.contains("self.execute(Command::VectorCreateCollection"));
    assert!(facade.contains("self.execute(Command::VectorUpsert"));
    assert!(facade.contains("self.execute(Command::VectorGet"));
    assert!(facade.contains("self.execute(Command::VectorQuery"));
    assert!(facade.contains("self.execute(Command::VectorBatchUpsert"));
    assert!(facade.contains("self.execute(Command::VectorBatchGet"));
    assert!(facade.contains("self.execute(Command::VectorBatchDelete"));
    assert!(facade.contains("self.execute(Command::EventBatchAppend"));
    assert!(facade.contains("self.execute(Command::EventAppend"));
    assert!(facade.contains("self.execute(Command::EventGet"));
    assert!(facade.contains("self.execute(Command::EventExists"));
    assert!(facade.contains("self.execute(Command::EventCount"));
    assert!(facade.contains("self.execute(Command::EventRange"));
    assert!(facade.contains("self.execute(Command::EventRangeByTime"));
    assert!(facade.contains("self.execute(Command::EventListTypes"));
    assert!(facade.contains("self.execute(Command::EventList"));
    assert!(facade.contains("self.execute(Command::EventVerifyChain"));
    assert!(facade.contains("self.execute(Command::GraphCreate"));
    assert!(facade.contains("self.execute(Command::GraphDelete"));
    assert!(facade.contains("self.execute(Command::GraphList"));
    assert!(facade.contains("self.execute(Command::GraphGetMeta"));
    assert!(facade.contains("self.execute(Command::GraphAddNode"));
    assert!(facade.contains("self.execute(Command::GraphGetNode"));
    assert!(facade.contains("self.execute(Command::GraphRemoveNode"));
    assert!(facade.contains("self.execute(Command::GraphListNodes"));
    assert!(facade.contains("self.execute(Command::GraphAddEdge"));
    assert!(facade.contains("self.execute(Command::GraphGetEdge"));
    assert!(facade.contains("self.execute(Command::GraphRemoveEdge"));
    assert!(facade.contains("self.execute(Command::GraphNeighbors"));
    assert!(facade.contains("self.execute(Command::GraphBindingsForEntity"));
    assert!(facade.contains("self.execute(Command::GraphBatchWrite"));
    assert!(!facade.contains(".kv("));
    assert!(!facade.contains(".json("));
    assert!(!facade.contains(".vector("));
    assert!(!facade.contains(".event("));
    assert!(!facade.contains(".graph("));
    assert!(!facade.contains("json_service("));
    assert!(!facade.contains("vector_service("));
    assert!(!facade.contains("event_service("));
    assert!(!facade.contains("graph_service("));
    assert!(!facade.contains(".put("));
    assert!(!facade.contains(".put_batch("));
    assert!(!facade.contains(".set_or_create("));
    assert!(!facade.contains(".batch_set_or_create("));
    assert!(!facade.contains(".batch_delete_entries("));
    assert!(!facade.contains(".batch_upsert("));
    assert!(!facade.contains(".query("));
    assert!(!facade.contains(".append("));
    assert!(!facade.contains(".batch_append("));
    assert!(!facade.contains(".delete("));
    assert!(!facade.contains(".create_graph("));
    assert!(!facade.contains(".batch_write("));
}

#[test]
fn event_batch_append_handler_uses_engine_batch_api() {
    let source = fs::read_to_string(crate_root().join("src/executor/event.rs"))
        .expect("event handler reads");
    let handler = source
        .split("fn execute_event_batch_append")
        .nth(1)
        .expect("event batch handler is present")
        .split("fn execute_event_append")
        .next()
        .expect("event append handler follows batch handler");

    assert!(handler.contains(".batch_append("));
    assert!(!handler.contains("execute_event_append"));
    assert!(!handler.contains(".append("));
}

#[test]
fn source_contract_uses_kv_specific_value_outputs() {
    let output_source =
        fs::read_to_string(crate_root().join("src/output.rs")).expect("output reads");
    let mut tests_source =
        fs::read_to_string(crate_root().join("tests/command_contract.rs")).expect("tests read");
    for file in source_files(&crate_root().join("tests/command_contract")) {
        tests_source.push_str(&fs::read_to_string(file).expect("command contract module reads"));
    }
    // Every non-JSON point read answers with the shared `Maybe` envelope
    // carrying that capability's specific payload DTO, so absence is uniform
    // across primitives instead of a bare `null`.
    assert!(output_source.contains("KvVersionedValue(Maybe<VersionedValue>)"));
    assert!(output_source.contains("VectorData(Maybe<VectorVersionedData>)"));
    assert!(output_source.contains("EventRecord(Maybe<EventVersionedData>)"));
    assert!(output_source.contains("GraphNodeResult(Maybe<GraphNodeDataOutput>)"));
    assert!(output_source.contains("GraphEdgeResult(Maybe<GraphEdgeDataOutput>)"));
    // JSON keeps its bespoke envelope: a non-optional `value` preserves a
    // stored JSON null (found-null) that `Maybe<Value>` would collapse.
    assert!(output_source.contains("JsonValue(MaybeJsonValue)"));
    assert!(output_source.contains("JsonVersionedValue(MaybeJsonVersionedValue)"));
    assert!(tests_source.contains("MaybeJsonValue::missing"));
    assert!(tests_source.contains("MaybeJsonVersionedValue::missing"));
}

#[test]
fn executor_graph_sources_do_not_own_graph_storage_behavior() {
    for file in source_files(&crate_root().join("src")) {
        let text = fs::read_to_string(&file).expect("source reads");
        for forbidden in forbidden_graph_lower_layer_terms() {
            assert!(
                !text.contains(forbidden),
                "{} leaked forbidden graph lower-layer term `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn executor_graph_surface_excludes_deferred_old_commands() {
    for file in source_files(&crate_root().join("src")) {
        let text = fs::read_to_string(&file).expect("source reads");
        for forbidden in excluded_graph_command_names() {
            assert!(
                !text.contains(forbidden),
                "{} exposed deferred graph command `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn executor_admin_surface_excludes_deferred_old_commands() {
    for file in source_files(&crate_root().join("src")) {
        let text = fs::read_to_string(&file).expect("source reads");
        for forbidden in excluded_admin_command_names() {
            assert!(
                !text.contains(forbidden),
                "{} exposed deferred admin command `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn executor_benchmarks_do_not_bypass_commands() {
    let benchmark_root = workspace_root().join("benchmarks/src/bin");
    if !benchmark_root.exists() {
        return;
    }

    for file in source_files(&benchmark_root) {
        let text = fs::read_to_string(&file).expect("benchmark source reads");
        if !is_executor_benchmark_source(&text) {
            continue;
        }

        assert!(
            text.contains("Command::KvBatchPut")
                || text.contains("Command::JsonBatchSet")
                || text.contains("Command::VectorBatchUpsert")
                || text.contains("Command::EventBatchAppend")
                || text.contains("Command::GraphBatchWrite")
                || text.contains("Command::ArrowImport")
                || text.contains("Command::ArrowExport"),
            "{} must use serialized executor batch commands",
            file.display()
        );
        for forbidden in [
            "strata_storage",
            "StorageRuntime",
            "CommitBatch",
            ".put_batch(",
            ".commit(",
        ] {
            assert!(
                !text.contains(forbidden),
                "{} bypassed executor commands with `{forbidden}`",
                file.display()
            );
        }
    }
}

#[test]
fn executor_arrow_sources_stay_on_serialized_command_boundary() {
    let arrow_root = crate_root().join("src/arrow");
    for file in source_files(&arrow_root) {
        let text = fs::read_to_string(&file).expect("Arrow source reads");
        for forbidden in forbidden_arrow_lower_layer_terms() {
            assert!(
                !text.contains(forbidden),
                "{} bypassed executor commands with `{forbidden}`",
                file.display()
            );
        }
    }

    let import_source =
        fs::read_to_string(arrow_root.join("import.rs")).expect("Arrow import reads");
    assert!(import_source.contains("Command::KvBatchPut"));
    assert!(import_source.contains("Command::JsonBatchSet"));
    assert!(import_source.contains("Command::VectorBatchUpsert"));
    assert!(import_source.contains("Command::VectorListCollections"));

    let export_source =
        fs::read_to_string(arrow_root.join("export.rs")).expect("Arrow export reads");
    assert!(export_source.contains("Command::KvList"));
    assert!(export_source.contains("Command::KvBatchGet"));
    assert!(export_source.contains("Command::JsonList"));
    assert!(export_source.contains("Command::JsonGet"));
    assert!(export_source.contains("Command::EventRange"));
    assert!(export_source.contains("Command::VectorListKeys"));
    assert!(export_source.contains("Command::VectorBatchGet"));
    assert!(export_source.contains("Command::GraphListNodes"));
    assert!(export_source.contains("Command::GraphNeighbors"));
}

fn is_executor_benchmark_source(text: &str) -> bool {
    text.contains("strata_executor")
        || text.contains("Command::Kv")
        || text.contains("Command::Json")
        || text.contains("Command::Vector")
        || text.contains("Command::Event")
        || text.contains("Command::Graph")
        || text.contains("Command::Arrow")
}

fn assert_registry_class_matches_prefix(entry: ErrorCodeRegistryEntry) {
    let prefix = entry.code.split('.').next().expect("code has prefix");
    let expected = match prefix {
        "invalid_argument" => ErrorClass::InvalidArgument,
        "not_found" => ErrorClass::NotFound,
        "already_exists" => ErrorClass::AlreadyExists,
        "failed_precondition" => ErrorClass::FailedPrecondition,
        "access_denied" => ErrorClass::AccessDenied,
        "conflict" => ErrorClass::Conflict,
        "ambiguous_commit" => ErrorClass::AmbiguousCommit,
        "history_unavailable" => ErrorClass::HistoryUnavailable,
        "unsupported" => ErrorClass::Unsupported,
        "resource_exhausted" => ErrorClass::ResourceExhausted,
        "unavailable" => ErrorClass::Unavailable,
        "io" => ErrorClass::Io,
        "corruption" => ErrorClass::Corruption,
        "data_loss" => ErrorClass::DataLoss,
        "serialization" => ErrorClass::Serialization,
        "internal" => ErrorClass::Internal,
        "inference" => return,
        other => panic!("unknown error-code prefix `{other}` in {}", entry.code),
    };
    assert_eq!(entry.class, expected, "{} has wrong class", entry.code);
}

fn extract_public_error_codes(text: &str, infix: &str) -> Vec<String> {
    let mut codes = Vec::new();
    let mut search_start = 0;
    while let Some(relative_index) = text[search_start..].find(infix) {
        let index = search_start + relative_index;
        let mut start = index;
        while start > 0 && is_error_code_char(text.as_bytes()[start - 1]) {
            start -= 1;
        }
        let mut end = index + infix.len();
        while end < text.len() && is_error_code_char(text.as_bytes()[end]) {
            end += 1;
        }
        let delimited = start > 0
            && text.as_bytes()[start - 1] == b'"'
            && end < text.len()
            && text.as_bytes()[end] == b'"';
        if delimited {
            codes.push(text[start..end].to_owned());
        }
        search_start = end;
    }
    codes.sort();
    codes.dedup();
    codes
}

const fn is_error_code_char(byte: u8) -> bool {
    byte == b'_' || byte == b'.' || (byte >= b'a' && byte <= b'z')
}

fn forbidden_lower_layer_terms() -> &'static [&'static str] {
    &[
        "strata-storage",
        "strata_storage",
        "StorageRuntime",
        "CommitBatch",
        "CommitMutation",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "BranchRequest",
        "Wal",
        "TableRuntime",
        "Lifecycle",
        "Compaction",
        "storage_api",
    ]
}

fn forbidden_event_lower_layer_terms() -> &'static [&'static str] {
    &[
        "compute_event_hash",
        "EventRecordEnvelope",
        "EventLogMetadata",
        "EventHash",
        "encode_event_record",
        "decode_event_record",
        "encode_event_metadata",
        "decode_event_metadata",
        "event_raw_rows",
        "event_rows(",
        "event_address",
        "type_index_address",
        "metadata_address",
        "StoragePersistence",
        "PersistenceReadRow",
        "RowMutation",
        "RowAddress",
        "ReadSelector",
        "CommitPlan",
        "sha2",
        "Sha256",
        "shadow_embedding",
        "embedding_runtime",
        "export_hook",
        "ExportService",
        "SearchIndex",
        "search_index",
    ]
}

fn forbidden_graph_lower_layer_terms() -> &'static [&'static str] {
    &[
        "strata_engine::data::graph",
        "strata_engine::graph",
        "GraphMetadataRecord",
        "GraphNodeRecord",
        "GraphEdgeRecord",
        "GraphBindingRecord",
        "encode_graph_",
        "decode_graph_",
        "graph_metadata_row",
        "node_rows(",
        "edge_rows(",
        "reverse_edge_rows(",
        "binding_rows_for_space",
        "binding_address",
        "node_address",
        "edge_address",
        "reverse_edge_address",
        // GO4 lifted the ontology deferral and GA5 the analytics deferral:
        // commands delegate to the engine's D4 ontology and adjacency
        // snapshot surfaces.
    ]
}

fn forbidden_vector_index_lower_layer_terms() -> &'static [&'static str] {
    &[
        "strata_engine::data::vector",
        "VectorArtifact",
        "FlatVectorArtifact",
        "HnswVectorArtifact",
        "HnswRuntimeIndex",
        "VectorIndexManifest",
        "VectorIndexPolicy",
        "encode_flat_vector_artifact",
        "decode_flat_vector_artifact",
        "encode_hnsw_vector_artifact",
        "decode_hnsw_vector_artifact",
        "vector_score",
        "fast_hnsw",
        "Hnsw",
        "HNSW",
    ]
}

fn forbidden_arrow_lower_layer_terms() -> &'static [&'static str] {
    &[
        "strata_storage",
        "strata-storage",
        "strata_engine::data",
        "StorageRuntime",
        "CommitBatch",
        "CommitMutation",
        "RowMutation",
        "StorageKey",
        "StorageValue",
        "Wal",
        "TableRuntime",
        "Lifecycle",
        "Compaction",
        "database.",
        "self.database",
        ".kv(",
        ".json(",
        ".vector(",
        "kv_service(",
        "json_service(",
        "vector_service(",
        "event_service(",
        "graph_service(",
    ]
}

fn forbidden_response_fixture_terms() -> &'static [&'static str] {
    &[
        "strata_storage",
        "strata-storage",
        "StorageRuntime",
        "StorageKey",
        "StorageValue",
        "RowAddress",
        "CommitBatch",
        "CommitMutation",
        "engine-artifacts",
        "engine_artifacts",
        ".wal",
        ".sst",
        "/Users/",
        "\\Users\\",
        "target/debug",
        "_system_",
    ]
}

fn excluded_graph_command_names() -> &'static [&'static str] {
    &[
        // GO4 ported the ontology surface onto 8 commands, GA5 the
        // analytics surface onto 6, and GI3 bulk ingest; the v0.6
        // per-kind read commands below stay excluded by design — the one
        // canonical read is GraphGetOntology (status + all definitions).
        "GraphGetObjectType",
        "GraphListObjectTypes",
        "GraphGetLinkType",
        "GraphListLinkTypes",
        "GraphListOntologyTypes",
    ]
}

fn excluded_admin_command_names() -> &'static [&'static str] {
    &[
        "Command::Flush",
        "Flush {",
        "Command::Compact",
        "Compact {",
        "Command::TimeRange",
        "TimeRange {",
        "Command::DurabilityCounters",
        "DurabilityCounters",
        "ConfigSetAutoEmbed",
        "AutoEmbedStatus",
        "EmbedStatus",
        "ReindexEmbeddings",
        "ConfigureModel",
        "ConfigureSet",
        "RetentionApply",
        "RetentionPreview",
        "RetentionStats",
    ]
}

fn source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_source_files(root, &mut files);
    files
}

fn response_fixture_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_files_with_extension(
        &crate_root().join("tests/fixtures/responses/v1"),
        "json",
        &mut files,
    );
    files.sort();
    files
}

fn collect_source_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("directory reads") {
        let entry = entry.expect("directory entry reads");
        let path = entry.path();
        if path.is_dir() {
            collect_source_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn collect_files_with_extension(root: &Path, extension: &str, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("directory reads") {
        let entry = entry.expect("directory entry reads");
        let path = entry.path();
        if path.is_dir() {
            collect_files_with_extension(&path, extension, files);
        } else if path
            .extension()
            .is_some_and(|actual_extension| actual_extension == extension)
        {
            files.push(path);
        }
    }
}

fn assert_page_contract(value: &Value, path: &str) {
    match value {
        Value::Object(object) => {
            if let Some(has_more) = object.get("has_more") {
                let has_more = has_more
                    .as_bool()
                    .unwrap_or_else(|| panic!("{path}: has_more must be a boolean"));
                let cursor = object
                    .get("cursor")
                    .unwrap_or_else(|| panic!("{path}: page object is missing cursor"));
                if has_more {
                    assert!(
                        !cursor.is_null(),
                        "{path}: continued page must carry a non-null cursor"
                    );
                } else {
                    assert!(
                        cursor.is_null(),
                        "{path}: terminal page must serialize cursor as null"
                    );
                }
            }
            for (key, nested) in object {
                assert_page_contract(nested, &format!("{path}.{key}"));
            }
        }
        Value::Array(items) => {
            for (index, nested) in items.iter().enumerate() {
                assert_page_contract(nested, &format!("{path}[{index}]"));
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn assert_no_string_error_field(value: &Value, path: &str) {
    match value {
        Value::Object(object) => {
            if let Some(error) = object.get("error") {
                assert!(
                    error.is_null() || error.is_object(),
                    "{path}: public error fields must be null or structured ErrorStatus objects"
                );
            }
            for (key, nested) in object {
                assert_no_string_error_field(nested, &format!("{path}.{key}"));
            }
        }
        Value::Array(items) => {
            for (index, nested) in items.iter().enumerate() {
                assert_no_string_error_field(nested, &format!("{path}[{index}]"));
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// #3112 S3b: every command that can time-travel must offer BOTH clocks.
///
/// `as_of` (logical) and `as_of_time` (wall clock) ask the same question in
/// different units, so a command carrying one without the other is a gap a
/// caller discovers only at runtime — and the gap appears silently, just by
/// someone adding a new temporal command modelled on an old one. The pair is
/// checked against the generated schemas rather than the Rust source, because
/// the schemas ARE the wire contract clients program against.
#[test]
fn every_command_accepting_as_of_also_accepts_as_of_time() {
    let schemas = workspace_root().join("crates/executor/idl/v1/generated/schemas");
    let mut checked = 0;
    let mut missing = Vec::new();
    for entry in fs::read_dir(&schemas).expect("generated schemas directory exists") {
        let path = entry.expect("readable schema entry").path();
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let schema: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("schema file is readable"))
                .expect("schema file is valid JSON");
        let Some(properties) = schema.pointer("/request/properties") else {
            continue;
        };
        if properties.get("as_of").is_none() {
            continue;
        }
        checked += 1;
        if properties.get("as_of_time").is_none() {
            missing.push(
                path.file_stem()
                    .expect("schema file has a stem")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    assert!(
        checked > 0,
        "guard found no temporal commands at all — the schema shape it reads must have moved"
    );
    assert!(
        missing.is_empty(),
        "commands accept as_of without as_of_time: {missing:?}"
    );
}

fn workspace_root() -> PathBuf {
    crate_root()
        .parent()
        .and_then(Path::parent)
        .expect("crate is under workspace crates directory")
        .to_path_buf()
}

#[test]
fn every_emitted_error_code_literal_is_registered() {
    // Regression guard (audit S1): a code-shaped string literal in executor
    // source that is not in the public registry would render with the
    // silently-wrong `internal.executor.unregistered_code` fallback. This
    // catches the drift at its source. (Codes built dynamically via format!
    // are out of scope — this covers the common literal case.)
    const CLASSES: &[&str] = &[
        "invalid_argument.",
        "not_found.",
        "already_exists.",
        "failed_precondition.",
        "access_denied.",
        "conflict.",
        "ambiguous_commit.",
        "history_unavailable.",
        "unsupported.",
        "resource_exhausted.",
        "unavailable.",
        "io.",
        "corruption.",
        "serialization.",
        "internal.",
        "data_loss.",
        "inference.",
    ];
    let registered: std::collections::BTreeSet<&str> = public_error_code_entries()
        .map(|entry| entry.code)
        .collect();

    let sources = source_files(&workspace_root().join("crates/executor/src"));

    let mut unregistered: Vec<(String, String)> = Vec::new();
    for path in sources {
        // The IDL tooling module manipulates code strings and source
        // filenames (e.g. "inference.yaml") as data, not as emitted errors.
        if path.to_string_lossy().contains("idl_tooling") {
            continue;
        }
        let text = fs::read_to_string(&path).expect("source reads");
        for class in CLASSES {
            let needle = format!("\"{class}");
            let mut cursor = 0;
            while let Some(found) = text[cursor..].find(&needle) {
                let start = cursor + found + 1; // past the opening quote
                let rest = &text[start..];
                let Some(end) = rest.find('"') else { break };
                let code = &rest[..end];
                cursor = start + end;
                // Only a plausible code (segments of [a-z0-9_]); skips URLs
                // and any quoted prose that happens to start with a class.
                let plausible = code.split('.').all(|seg| {
                    !seg.is_empty()
                        && seg
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                });
                if plausible && !registered.contains(code) {
                    unregistered.push((path.display().to_string(), code.to_owned()));
                }
            }
        }
    }
    assert!(
        unregistered.is_empty(),
        "these emitted error-code literals are not registered: {unregistered:?}"
    );
}
