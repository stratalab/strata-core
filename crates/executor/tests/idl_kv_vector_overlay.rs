//! KV/vector IDL overlay conformance tests.
//!
//! Requires `inference` as well as `idl-tooling`: the suite resolves the full
//! command index and validates every command's request fixture against the
//! runtime `Command` enum, and the `inference.*` fixtures only deserialize when
//! the `inference` variants are compiled in. Without it, `resolve_default_index`
//! fails on `requests/v1/inference/*.json` (see #2982). Matches the gating on
//! the sibling full-catalog IDL tests (`idl_bin_dispatch`, `generated_conformance`).

#![cfg(all(feature = "idl-tooling", feature = "inference"))]

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use strata_executor::idl_tooling::{
    capture_examples, check, check_cli, check_docs, default_repo_root, resolve_cli_index,
    resolve_default_cli_index, resolve_default_index, resolve_default_schemas,
    to_generated_cli_json, to_generated_json, verify_examples, IdlError,
};

const REQUIRED_ADMIN: &[&str] = &[
    "admin.ping",
    "admin.info",
    "admin.health",
    "admin.metrics",
    "admin.describe",
    "admin.config",
    "admin.ipc_status",
    "admin.ipc_stop",
    "admin.remote",
    "admin.config_key",
    "admin.hub_clone",
];

const REQUIRED_ARROW: &[&str] = &["arrow.import", "arrow.export"];

const REQUIRED_BRANCH: &[&str] = &[
    "branch.list",
    "branch.get",
    "branch.diff",
    "branch.merge",
    "branch.preview",
    "branch.create",
    "branch.fork",
    "branch.fork_at_version",
    "branch.fork_at_timestamp",
    "branch.delete",
];

const REQUIRED_SPACE: &[&str] = &["space.list", "space.create", "space.exists", "space.delete"];

const REQUIRED_GRAPH: &[&str] = &[
    "graph.create",
    "graph.delete",
    "graph.list",
    "graph.meta",
    "graph.node.add",
    "graph.node.get",
    "graph.node.remove",
    "graph.node.list",
    "graph.sample",
    "graph.edge.add",
    "graph.edge.get",
    "graph.edge.remove",
    "graph.neighbors",
    "graph.bindings",
    "graph.batch_write",
    "graph.ontology.define_object_type",
    "graph.ontology.define_link_type",
    "graph.ontology.delete_object_type",
    "graph.ontology.delete_link_type",
    "graph.ontology.freeze",
    "graph.ontology.get",
    "graph.ontology.summary",
    "graph.nodes_by_type",
    "graph.analytics.wcc",
    "graph.analytics.lcc",
    "graph.analytics.sssp",
    "graph.analytics.pagerank",
    "graph.analytics.cdlp",
    "graph.analytics.bfs",
    "graph.bulk_insert",
    "graph.apply_delete_policy",
];

const REQUIRED_HUB: &[&str] = &[
    "hub.info",
    "hub.list_datasets",
    "hub.get_dataset",
    "hub.list_refs",
    "hub.list_yanked",
];

const REQUIRED_EVENT: &[&str] = &[
    "event.append",
    "event.batch_append",
    "event.get",
    "event.exists",
    "event.count",
    "event.range",
    "event.range_time",
    "event.types",
    "event.list",
    "event.verify_chain",
];

const REQUIRED_JSON: &[&str] = &[
    "json.set",
    "json.get",
    "json.delete",
    "json.history",
    "json.exists",
    "json.batch_exists",
    "json.batch_set",
    "json.batch_get",
    "json.batch_delete",
    "json.list",
    "json.scan",
    "json.count",
    "json.sample",
    "json.index.create",
    "json.index.drop",
    "json.index.list",
];

fn required_command_count() -> usize {
    REQUIRED_ADMIN.len()
        + REQUIRED_ARROW.len()
        + REQUIRED_BRANCH.len()
        + REQUIRED_EVENT.len()
        + REQUIRED_GRAPH.len()
        + REQUIRED_HUB.len()
        + REQUIRED_INFERENCE.len()
        + REQUIRED_JSON.len()
        + REQUIRED_KV.len()
        + REQUIRED_SPACE.len()
        + REQUIRED_VECTOR.len()
}

const REQUIRED_INFERENCE: &[&str] = &[
    "inference.models.list",
    "inference.models.local",
    "inference.models.pull",
    "inference.capability",
    "inference.generate",
    "inference.tokenize",
    "inference.detokenize",
    "inference.embed",
    "inference.rank",
    "inference.status",
    "inference.unload",
    "inference.cache_status",
];

const REQUIRED_KV: &[&str] = &[
    "kv.put",
    "kv.get",
    "kv.delete",
    "kv.list",
    "kv.scan",
    "kv.batch_put",
    "kv.batch_get",
    "kv.batch_delete",
    "kv.batch_exists",
    "kv.exists",
    "kv.history",
    "kv.count",
    "kv.sample",
];

const REQUIRED_VECTOR: &[&str] = &[
    "vector.collection.create",
    "vector.collection.delete",
    "vector.collection.list",
    "vector.collection.stats",
    "vector.collection.set_embedding_model",
    "vector.count",
    "vector.upsert",
    "vector.get",
    "vector.history",
    "vector.exists",
    "vector.batch_exists",
    "vector.keys",
    "vector.scan",
    "vector.sample",
    "vector.metadata.update",
    "vector.delete",
    "vector.delete_by_filter",
    "vector.delete_all",
    "vector.query",
    "vector.index.query",
    "vector.batch_upsert",
    "vector.batch_get",
    "vector.batch_delete",
];

#[test]
fn kv_and_vector_overlay_has_required_command_coverage() {
    let index = resolve_default_index().expect("IDL resolves");
    let ids: BTreeSet<&str> = index
        .commands
        .iter()
        .map(|command| command.id.as_str())
        .collect();

    for id in REQUIRED_ADMIN
        .iter()
        .chain(REQUIRED_ARROW.iter())
        .chain(REQUIRED_BRANCH.iter())
        .chain(REQUIRED_EVENT.iter())
        .chain(REQUIRED_GRAPH.iter())
        .chain(REQUIRED_HUB.iter())
        .chain(REQUIRED_INFERENCE.iter())
        .chain(REQUIRED_JSON.iter())
        .chain(REQUIRED_KV.iter())
        .chain(REQUIRED_SPACE.iter())
        .chain(REQUIRED_VECTOR.iter())
    {
        assert!(ids.contains(id), "missing required command `{id}`");
    }
    assert_eq!(ids.len(), required_command_count());
}

#[test]
fn generated_command_index_is_fresh_and_deterministic() {
    let root = default_repo_root();
    check(&root).expect("generated IDL is fresh");

    let first = resolve_default_index().expect("first resolve succeeds");
    let second = resolve_default_index().expect("second resolve succeeds");
    assert_eq!(first, second);

    let generated = to_generated_json(&first).expect("index serializes");
    let path = root
        .join("crates/executor/idl/v1/generated")
        .join("command-index.json");
    let checked_in = fs::read_to_string(path).expect("generated file is readable");
    assert_eq!(generated, checked_in);
}

#[test]
fn resolved_commands_are_explain_ready() {
    let index = resolve_default_index().expect("IDL resolves");
    let mut sorted_ids = Vec::new();
    for command in &index.commands {
        sorted_ids.push(command.id.as_str());
        assert_eq!(command.generated_family_and_op_id(), command.id.as_str());
        assert!(!command.title.trim().is_empty());
        assert!(!command.summary.trim().is_empty());
        assert!(!command.description.trim().is_empty());
        assert!(command.docs.starts_with("/docs/"));
        assert!(!command.cli.path.is_empty());
        assert!(command.mcp.name.starts_with("strata_"));
        assert!(command.input.starts_with("Command::"));
        assert!(command.output.starts_with("Output::"));
        assert!(!command.outputs.is_empty());
        assert!(
            command
                .outputs
                .iter()
                .any(|output| output == &command.output),
            "primary output must be listed in outputs for `{}`",
            command.id
        );
        assert!(
            matches!(command.wire_status.as_str(), "stable" | "transitional"),
            "unexpected wire status for `{}`",
            command.id
        );
        assert!(!command.response_model.trim().is_empty());
        assert!(!command.commit.trim().is_empty());
        assert!(!command.pagination.trim().is_empty());
        assert!(!command.batch.trim().is_empty());
        assert!(has_extension(&command.source.command, "yaml"));
        assert!(
            command
                .source
                .command
                .starts_with("crates/executor/idl/v1/commands/"),
            "command source should be executor-owned for `{}`",
            command.id
        );
        assert!(has_extension(&command.source.prose, "md"));
        assert!(command.fixtures.request.starts_with("requests/v1/"));
        assert!(command.fixtures.response.starts_with("responses/v1/"));
        assert!(
            command
                .errors
                .iter()
                .all(|error| error.docs.starts_with("https://stratadb.org/e/")),
            "all command errors should include docs URLs"
        );
    }

    let mut expected = sorted_ids.clone();
    expected.sort_unstable();
    assert_eq!(sorted_ids, expected, "commands must be sorted by id");
}

#[test]
fn kv_vector_concepts_resolve_to_expected_shared_models() {
    let index = resolve_default_index().expect("IDL resolves");
    let model = |id: &str| {
        index
            .commands
            .iter()
            .find(|command| command.id == id)
            .map(|command| command.response_model.as_str())
            .expect("command exists")
    };

    assert_eq!(model("kv.get"), "Maybe<VersionedValue>");
    assert_eq!(model("kv.list"), "Page<Bytes>");
    assert_eq!(model("kv.scan"), "Page<ScanItem>");
    assert_eq!(model("kv.batch_get"), "BatchResult<BatchGetItemResult>");
    assert_eq!(model("vector.collection.create"), "MutationAck");
    assert_eq!(
        model("vector.collection.stats"),
        "StatusResponse<VectorCollectionInfo>"
    );
    assert_eq!(
        model("vector.collection.list"),
        "Page<VectorCollectionInfo>"
    );
    assert_eq!(model("vector.keys"), "Page<string>");
    assert_eq!(model("vector.query"), "SearchResult<VectorMatch>");
    assert_eq!(
        model("vector.index.query"),
        "SearchResult<VectorMatch> + IndexDiagnostics"
    );
}

#[test]
fn kv_list_declares_a_single_keys_page_output() {
    // `Output::Keys` and `Output::KeysPage` were structurally identical and
    // collapsed into one paginated variant (a non-paginated list returns a
    // terminal page), so kv.list now declares exactly one wire output.
    let index = resolve_default_index().expect("IDL resolves");
    let command = index
        .commands
        .iter()
        .find(|command| command.id == "kv.list")
        .expect("kv.list exists");

    assert_eq!(command.output, "Output::KeysPage");
    assert_eq!(command.outputs, vec!["Output::KeysPage".to_owned()]);
    assert_eq!(command.fixtures.response, "responses/v1/kv/list_page.json");
    assert!(command.fixtures.responses.is_empty());
}

/// The reviewed set of commands whose wire is not yet frozen. Each one is a
/// row in `response-model-divergences.yaml` (#3313): the declaration is the
/// target shape and the wire is scheduled to be normalised. The set only
/// changes by a reviewed edit here, so a stray `wire_status: transitional`
/// (or a quietly dropped one) is visible.
#[test]
fn transitional_wire_shapes_are_explicit() {
    let index = resolve_default_index().expect("IDL resolves");
    let transitional: BTreeSet<&str> = index
        .commands
        .iter()
        .filter(|command| command.wire_status == "transitional")
        .map(|command| command.id.as_str())
        .collect();

    assert_eq!(
        transitional,
        BTreeSet::from([
            "admin.remote",
            "event.count",
            "graph.bulk_insert",
            "graph.ontology.freeze",
            "json.index.create",
            "json.index.drop",
            "vector.collection.create",
            "vector.collection.delete",
            "vector.collection.set_embedding_model",
            "vector.collection.stats"
        ])
    );
}

#[test]
fn idl_tooling_does_not_add_downstream_generators() {
    let root = default_repo_root();
    let mut source = String::new();
    for path in [
        root.join("crates/executor/src/idl_tooling.rs"),
        root.join("crates/executor/src/bin/strata-idl/main.rs"),
    ] {
        source.push_str(&fs::read_to_string(path).expect("IDL tooling source is readable"));
    }

    for forbidden in ["OpenAPI", "TypeScript", "Python SDK", "MCP server"] {
        assert!(
            !source.contains(forbidden),
            "IDL tooling must not add downstream generator code for {forbidden}"
        );
    }
}

#[test]
fn idl_packaging_is_executor_owned() {
    let root = default_repo_root();
    assert!(
        !root.join("crates/idl-next").exists(),
        "standalone IDL crate should be removed"
    );
    assert!(
        root.join("crates/executor/idl/v1/manifest.yaml").is_file(),
        "authored IDL should live under executor"
    );
    assert!(
        root.join("crates/executor/idl/v1/generated/command-index.json")
            .is_file(),
        "generated IDL should live under executor"
    );
    assert!(
        root.join("crates/executor/src/bin/strata-idl/main.rs")
            .is_file(),
        "executor should own the strata-idl dev binary"
    );

    let workspace_toml =
        fs::read_to_string(root.join("Cargo.toml")).expect("workspace Cargo.toml reads");
    assert!(
        !workspace_toml.contains("\"crates/idl-next\""),
        "workspace should not list the old standalone IDL crate"
    );

    let executor_toml = fs::read_to_string(root.join("crates/executor/Cargo.toml"))
        .expect("executor Cargo.toml reads");
    assert!(executor_toml.contains("idl-tooling = ["));
    assert!(executor_toml.contains("\"dep:serde_yaml\""));
    assert!(executor_toml.contains("\"dep:sha2\""));
    assert!(executor_toml.contains("name = \"strata-idl\""));
    assert!(executor_toml.contains("required-features = [\"idl-tooling\"]"));
}

#[test]
fn generated_cli_command_index_is_fresh_and_deterministic() {
    let root = default_repo_root();
    check_cli(&root).expect("generated CLI IDL is fresh");

    let first = resolve_default_cli_index().expect("first CLI resolve succeeds");
    let second = resolve_default_cli_index().expect("second CLI resolve succeeds");
    assert_eq!(first, second);
    assert!(first.generated);
    assert_eq!(first.schema_version, "strata.cli.v1");
    assert_eq!(first.generator_version, "strata-executor-cli-idl.3");
    assert_eq!(first.source.schema_version, "strata.idl.v1");
    assert_eq!(first.source.generator_version, "strata-executor-idl.1");
    assert_eq!(first.source.checksum_sha256.len(), 64);
    assert!(first
        .source
        .checksum_sha256
        .chars()
        .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()));
    assert_eq!(first.command_count, required_command_count());
    assert_eq!(first.command_count, first.commands.len());

    let generated = to_generated_cli_json(&first).expect("CLI index serializes");
    let path = root
        .join("crates/executor/idl/v1/generated")
        .join("cli-command-index.json");
    let checked_in = fs::read_to_string(path).expect("generated CLI file is readable");
    assert_eq!(generated, checked_in);
}

#[test]
fn generated_reference_docs_are_fresh_and_complete() {
    let root = default_repo_root();
    // Drift guard: every rendered page matches on disk and no stale file lingers.
    check_docs(&root).expect("generated reference docs are fresh");

    // Coverage: reference generation is total over the catalog, so every
    // command must have a page at its `docs` URL, plus a per-family index and
    // the machine-layer `llms.txt`.
    let index = resolve_default_index().expect("IDL resolves");
    let docs_dir = root.join("crates/executor/idl/v1/generated/docs");
    let mut families = BTreeSet::new();
    for command in &index.commands {
        let rel = command
            .docs
            .strip_prefix("/docs/")
            .expect("docs path is under /docs/");
        let page = docs_dir.join(format!("{rel}.md"));
        assert!(
            page.is_file(),
            "missing generated reference page for `{}` at {}",
            command.id,
            page.display()
        );
        families.insert(command.family.clone());
    }
    for family in &families {
        let family_index = docs_dir.join(family).join("index.md");
        assert!(
            family_index.is_file(),
            "missing generated family index for `{family}`"
        );
    }
    assert!(
        docs_dir.join("llms.txt").is_file(),
        "missing generated llms.txt"
    );
}

#[test]
fn canonical_examples_validate_execute_and_cover_the_catalog() {
    // verify_examples validates every examples/<id>.yaml against the schemas,
    // enforces the shrink-only missing-examples allowlist (every command has an
    // example or is listed), and replays each spec against a scratch cache
    // executor asserting miss-ness — so a stale or wrong example fails here.
    let root = default_repo_root();
    verify_examples(&root).expect("canonical examples validate, cover, and execute");
}

/// A scratch copy of everything the resolver reads: the IDL tree, the request
/// fixtures it validates, and the enum sources it scans for variant coverage.
fn scratch_idl_tree() -> tempfile::TempDir {
    let root = default_repo_root();
    let temp = tempfile::tempdir().expect("tempdir creates");
    for relative in ["crates/executor/idl/v1", "crates/executor/tests/fixtures"] {
        copy_dir(&root.join(relative), &temp.path().join(relative));
    }
    let src = temp.path().join("crates/executor/src");
    fs::create_dir_all(&src).expect("src dir creates");
    for file in ["command.rs", "output.rs"] {
        fs::copy(root.join("crates/executor/src").join(file), src.join(file))
            .expect("enum source copies");
    }
    temp
}

#[test]
fn a_lying_example_fails_verification() {
    // The replay asserts each step's miss-ness, so `verify_examples` is more
    // than a parser: an example that promises a value for an absent key is
    // refused, naming the example and the step.
    let temp = scratch_idl_tree();
    let example = temp
        .path()
        .join("crates/executor/idl/v1/examples/kv.get.yaml");
    let text = fs::read_to_string(&example).expect("kv.get example reads");
    assert_eq!(text.matches("    returns: null\n").count(), 1);
    fs::write(
        &example,
        text.replacen("    returns: null\n", "    returns: hello\n", 1),
    )
    .expect("example writes");
    match verify_examples(temp.path()) {
        Err(IdlError::Invalid(message)) => assert!(
            message.contains("example `kv.get` step 2")
                && message.contains("the call returned a miss"),
            "got: {message}"
        ),
        other => panic!("expected the lying example to be refused, got {other:?}"),
    }
}

#[test]
fn captured_example_runs_cover_every_example() {
    // `capture_examples` feeds the docs bundle's `command-examples.json`: one
    // run per `examples/<id>.yaml`, in id order, every step replayed with the
    // CLI line it renders and the wire envelope it produced.
    let root = default_repo_root();
    let runs = capture_examples(&root).expect("examples capture");
    let mut expected: Vec<String> = fs::read_dir(root.join("crates/executor/idl/v1/examples"))
        .expect("examples dir reads")
        .map(|entry| {
            let path = entry.expect("entry reads").path();
            path.file_stem()
                .expect("example file has a stem")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    expected.sort();
    let captured: Vec<&str> = runs.iter().map(|run| run.command_id.as_str()).collect();
    assert_eq!(captured, expected);
    for run in &runs {
        assert!(
            !run.steps.is_empty(),
            "{}: a run replays its steps",
            run.command_id
        );
        for step in &run.steps {
            assert!(
                step.cli_input.starts_with("strata "),
                "{}: a captured step renders a CLI line, got {:?}",
                run.command_id,
                step.cli_input
            );
            assert!(
                step.wire_output.get("type").is_some(),
                "{}: a captured step carries the wire envelope, got {}",
                run.command_id,
                step.wire_output
            );
        }
    }
}

#[test]
fn cli_command_index_has_required_coverage_and_lookup_tables() {
    let index = resolve_default_cli_index().expect("CLI IDL resolves");
    let ids: BTreeSet<&str> = index
        .commands
        .iter()
        .map(|command| command.id.as_str())
        .collect();
    for id in REQUIRED_ADMIN
        .iter()
        .chain(REQUIRED_ARROW.iter())
        .chain(REQUIRED_BRANCH.iter())
        .chain(REQUIRED_EVENT.iter())
        .chain(REQUIRED_GRAPH.iter())
        .chain(REQUIRED_INFERENCE.iter())
        .chain(REQUIRED_JSON.iter())
        .chain(REQUIRED_KV.iter())
        .chain(REQUIRED_SPACE.iter())
        .chain(REQUIRED_VECTOR.iter())
    {
        assert!(ids.contains(id), "missing required CLI command `{id}`");
    }

    let mut sorted_paths = index
        .commands
        .iter()
        .map(|command| command.path.clone())
        .collect::<Vec<_>>();
    let actual_paths = sorted_paths.clone();
    sorted_paths.sort();
    assert_eq!(actual_paths, sorted_paths, "commands must sort by CLI path");

    for (offset, command) in index.commands.iter().enumerate() {
        assert_eq!(command.path_display, command.path.join(" "));
        assert!(
            command.path.iter().all(|segment| !segment.contains('_')),
            "generated CLI path for `{}` should not leak command-id underscores",
            command.id
        );
        assert_eq!(index.lookup.by_id.get(&command.id), Some(&offset));
        assert_eq!(
            index.lookup.by_path.get(&command.path_display),
            Some(&command.id)
        );
        assert!(!command.title.trim().is_empty());
        assert!(!command.summary.trim().is_empty());
        assert!(!command.description.trim().is_empty());
        assert!(command.docs.starts_with("/docs/"));
        assert!(command.input.starts_with("Command::"));
        assert!(!command.outputs.is_empty());
        assert!(command
            .outputs
            .iter()
            .all(|output| output.starts_with("Output::")));
        assert!(!command.errors.is_empty());
    }

    let kv = index
        .families
        .iter()
        .find(|family| family.id == "kv")
        .expect("KV family exists");
    assert_eq!(kv.command_count, REQUIRED_KV.len());
    let vector = index
        .families
        .iter()
        .find(|family| family.id == "vector")
        .expect("vector family exists");
    assert_eq!(vector.command_count, REQUIRED_VECTOR.len());

    let path_for = |id: &str| {
        index
            .commands
            .iter()
            .find(|command| command.id == id)
            .map(|command| command.path_display.as_str())
            .expect("command exists")
    };
    assert_eq!(path_for("kv.batch_get"), "kv batch-get");
    assert_eq!(
        path_for("vector.delete_by_filter"),
        "vector delete-by-filter"
    );
    assert_eq!(
        path_for("vector.collection.create"),
        "vector collection create"
    );
}

#[test]
fn cli_generation_reads_command_facts_from_the_resolved_index_and_only_display_from_yaml() {
    let root = default_repo_root();
    let temp = tempfile::tempdir().expect("tempdir creates");
    let idl = "crates/executor/idl/v1";
    // What `generate-cli` reads: the resolved index and schemas for command
    // facts, `kinds.yaml` and `commands/*.yaml` for the CLI-only display
    // layer. Prose is deliberately absent.
    for relative in ["generated/command-index.json", "kinds.yaml"] {
        let target = temp.path().join(idl).join(relative);
        fs::create_dir_all(target.parent().expect("parent")).expect("dir creates");
        fs::copy(root.join(idl).join(relative), target).expect("file copies");
    }
    for relative in ["generated/schemas", "commands"] {
        copy_dir(
            &root.join(idl).join(relative),
            &temp.path().join(idl).join(relative),
        );
    }
    // A command fact edited in the YAML must not reach the CLI index: the
    // display layer is the only thing generate-cli takes from authored files.
    let kv_yaml = temp.path().join(idl).join("commands/kv.yaml");
    let text = fs::read_to_string(&kv_yaml).expect("kv.yaml reads");
    assert_eq!(text.matches("    title: Put KV value\n").count(), 1);
    fs::write(
        &kv_yaml,
        text.replacen("    title: Put KV value\n", "    title: TAMPERED\n", 1),
    )
    .expect("kv.yaml writes");

    let index = resolve_cli_index(temp.path()).expect("CLI index resolves");
    assert_eq!(index.command_count, required_command_count());
    assert_eq!(
        index.source.path,
        "crates/executor/idl/v1/generated/command-index.json"
    );
    let put = index
        .commands
        .iter()
        .find(|command| command.id == "kv.put")
        .expect("kv.put resolves");
    assert_eq!(
        put.title, "Put KV value",
        "facts come from the resolved index"
    );
    assert!(
        !temp
            .path()
            .join("crates/executor/idl/v1/prose/commands/kv.put.md")
            .exists(),
        "test fixture intentionally excludes authored prose"
    );
}

fn copy_dir(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("dir creates");
    for entry in fs::read_dir(source).expect("dir reads") {
        let entry = entry.expect("entry reads");
        let target = destination.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).expect("file copies");
        }
    }
}

#[test]
fn strata_idl_generates_cli_artifacts_without_user_explain() {
    let root = default_repo_root();
    let source = fs::read_to_string(root.join("crates/executor/src/bin/strata-idl/main.rs"))
        .expect("strata-idl source reads");

    assert!(source.contains("\"generate-cli\""));
    assert!(source.contains("\"check-cli\""));
    assert!(
        !source.contains("\"explain\""),
        "strata-idl must not introduce explain; user explain belongs to strata"
    );
}

trait ResolvedCommandExt {
    fn generated_family_and_op_id(&self) -> String;
}

impl ResolvedCommandExt for strata_executor::idl_tooling::ResolvedCommand {
    fn generated_family_and_op_id(&self) -> String {
        format!("{}.{}", self.family, self.op)
    }
}

fn has_extension(path: &str, extension: &str) -> bool {
    Path::new(path)
        .extension()
        .is_some_and(|actual| actual.eq_ignore_ascii_case(extension))
}

#[test]
fn schema_documents_cover_every_resolved_command() {
    let index = resolve_default_index().expect("index resolves");
    let schemas_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("idl/v1/generated/schemas");
    let mut expected = BTreeSet::new();
    for command in &index.commands {
        let path = schemas_dir.join(format!("{}.json", command.id));
        let text = fs::read_to_string(&path).expect("schema document exists for every command");
        let document: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
        assert_eq!(document["command"], serde_json::json!(command.id));
        assert!(
            document["request"].is_object(),
            "{} has a request schema",
            command.id
        );
        assert!(
            document["response"].is_object(),
            "{} has a response schema",
            command.id
        );
        expected.insert(format!("{}.json", command.id));
    }
    for entry in fs::read_dir(&schemas_dir).expect("schemas dir") {
        let name = entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .into_owned();
        assert!(expected.contains(&name), "no stray schema document: {name}");
    }
}

#[test]
fn schemas_accept_the_wire_and_reject_field_type_lies() {
    let documents = resolve_default_schemas().expect("schemas resolve");
    let document = documents.get("kv.put").expect("kv.put covered");
    // Standalone request schema: the sub-schema plus the document's $defs.
    let mut request = document["request"].clone();
    request["$defs"] = document["$defs"].clone();
    let validator = jsonschema::validator_for(&request).expect("schema compiles");

    let good: serde_json::Value =
        serde_json::json!({"type": "kv_put", "key": "YQ==", "value": "b25l"});
    assert!(validator.is_valid(&good), "real wire shape validates");

    let bad_key_type: serde_json::Value =
        serde_json::json!({"type": "kv_put", "key": 123, "value": "b25l"});
    assert!(
        !validator.is_valid(&bad_key_type),
        "non-string key is rejected"
    );

    let unknown_field: serde_json::Value = serde_json::json!(
        {"type": "kv_put", "key": "YQ==", "value": "b25l", "surprise": true});
    assert!(
        !validator.is_valid(&unknown_field),
        "deny_unknown_fields reaches the schema"
    );
}

#[test]
fn bytes_schema_stays_base64_on_the_wire() {
    let documents = resolve_default_schemas().expect("schemas resolve");
    let document = documents.get("kv.put").expect("kv.put covered");
    let bytes = &document["$defs"]["Bytes"];
    assert_eq!(bytes["type"], serde_json::json!("string"));
    assert_eq!(bytes["contentEncoding"], serde_json::json!("base64"));
}

#[test]
fn cli_surfaces_are_valid_and_batches_stay_wire_only() {
    let index = resolve_default_index().expect("index resolves");
    for command in &index.commands {
        assert!(
            command.cli.surface == "verb" || command.cli.surface == "wire",
            "{} has surface {}",
            command.id,
            command.cli.surface
        );
        if command.batch != "none" {
            assert_eq!(
                command.cli.surface, "wire",
                "{} is a batch command; batches have no clap verbs",
                command.id
            );
        }
    }
}

#[test]
fn uncovered_allowlist_is_disjoint_from_coverage() {
    let index = resolve_default_index().expect("index resolves");
    let covered: BTreeSet<String> = index
        .commands
        .iter()
        .map(|command| command.input.clone())
        .collect();
    let idl_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("idl/v1");
    let text =
        fs::read_to_string(idl_root.join("uncovered-commands.yaml")).expect("allowlist exists");
    for line in text.lines() {
        if let Some(entry) = line.trim().strip_prefix("- ") {
            assert!(
                !covered.contains(entry),
                "{entry} is covered; the allowlist may only shrink"
            );
        }
    }
}

#[test]
fn itemwise_batch_commands_return_a_batch_result() {
    // Regression guard for the batch-bypass family (kv_batch_exists, #2578):
    // any command declared batch.itemwise_* must actually return a
    // BatchResult<T> on the wire, so it can express per-item status. A flat
    // output (e.g. the old BoolList) would fail here — its schema `data`
    // would be an array, not a $ref to a BatchResult def.
    let index = resolve_default_index().expect("index resolves");
    let schemas_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("idl/v1/generated/schemas");
    let mut checked = 0;
    for command in &index.commands {
        if !command.kind.starts_with("batch.itemwise") {
            continue;
        }
        assert!(
            command.response_model.starts_with("BatchResult<"),
            "{} is itemwise but its response_model is {}",
            command.id,
            command.response_model
        );
        let text = fs::read_to_string(schemas_dir.join(format!("{}.json", command.id)))
            .expect("schema document exists");
        let document: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
        let data_ref = document["response"]["properties"]["data"]["$ref"]
            .as_str()
            .unwrap_or_else(|| {
                panic!(
                    "{} response `data` is not a $ref (not a BatchResult shape)",
                    command.id
                )
            });
        let def = data_ref
            .strip_prefix("#/$defs/")
            .expect("$ref points into $defs");
        assert!(
            def.starts_with("BatchResult"),
            "{} response `data` resolves to `{def}`, not a BatchResult",
            command.id
        );
        checked += 1;
    }
    assert!(
        checked >= 4,
        "expected several itemwise batch commands, saw {checked}"
    );
}
