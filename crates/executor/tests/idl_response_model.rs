//! The response-model derivation (#3313/#3322, S0c of the CLI output
//! contract #3314).
//!
//! Every command declares a `response_model` family *template*
//! (`Maybe<{payload}>`, `MutationAck`, ...) and every family implies a wire
//! shape. `generate` and `check` classify what the generated schema actually
//! carries under `response.data`, compare it with the template's family, and
//! fill `{payload}` from the schema: the `$def` at the family's payload slot,
//! or the wire-spelled primitive. Nothing authors the payload. The commands
//! where wire and family disagree live in the shrink-only
//! `response-model-divergences.yaml`, each marked `wire_status: transitional`,
//! and their row states the complete target model. Every model the surface
//! resolves to is listed in `dto-inventory.yaml`, and only those.
//! An accepted spelling of a family (`{found, value}` or a nullable `data`
//! for `Maybe<T>`, `{items}` or a bare array for `Maybe<Vec<T>>`) is an
//! *encoding*, resolved into `cli-command-index.json`, never a divergence.
//!
//! These tests drive the guard over a scratch copy of the real tree with one
//! authored change at a time, through the same entry points the CI gate uses,
//! so each rule is observed at its call site rather than in a helper.
//!
//! Requires `inference` as well as `idl-tooling` for the same reason as the
//! sibling full-catalog IDL tests: the base resolver validates every request
//! fixture against the runtime `Command` enum, and the `inference.*` fixtures
//! only deserialize when those variants are compiled in (#2982).

#![cfg(all(feature = "idl-tooling", feature = "inference"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use strata_executor::cli_metadata::{CliRenderRule, CliWireEncoding};
use strata_executor::idl_tooling::{
    check, check_cli, generate, generate_docs, resolve_default_cli_index, resolve_default_index,
    IdlError,
};

const LEDGER: &str = "response-model-divergences.yaml";
const INVENTORY: &str = "dto-inventory.yaml";

/// The ledger row for `json.index.drop` exactly as authored, the anchor for
/// the row-shaped edits below.
const JSON_INDEX_DROP_ROW: &str = "  - command: json.index.drop
    declared: MutationAck
    wire: scalar:boolean
    issue: 3313
";

/// The authored `kv.get` lines the command-shaped edits below anchor on.
const KV_GET_HEAD: &str = "    input: Command::KvGet
    output: Output::KvVersionedValue
";

struct Scratch {
    root: tempfile::TempDir,
}

impl Scratch {
    fn new() -> Self {
        let real = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("executor lives under crates/")
            .to_path_buf();
        let root = tempfile::tempdir().expect("scratch root");
        for relative in ["crates/executor/idl/v1", "crates/executor/tests/fixtures"] {
            copy_tree(&real.join(relative), &root.path().join(relative));
        }
        // The base resolver also scans the enum sources for variant coverage.
        let src = root.path().join("crates/executor/src");
        std::fs::create_dir_all(&src).expect("scratch src dir");
        for file in ["command.rs", "output.rs"] {
            std::fs::copy(real.join("crates/executor/src").join(file), src.join(file))
                .expect("copy enum source");
        }
        Self { root }
    }

    fn idl(&self) -> PathBuf {
        self.root.path().join("crates/executor/idl/v1")
    }

    fn replace_in(&self, relative: &str, from: &str, to: &str) {
        let path = self.idl().join(relative);
        let text = std::fs::read_to_string(&path).expect("read authored file");
        assert_eq!(
            text.matches(from).count(),
            1,
            "{relative}: the edit anchor must occur exactly once"
        );
        std::fs::write(&path, text.replacen(from, to, 1)).expect("write authored file");
    }

    fn set_budget(&self, from: usize, to: usize) {
        self.replace_in(
            LEDGER,
            &format!("budget: {from}\n"),
            &format!("budget: {to}\n"),
        );
    }

    /// `generate` resolves the authored YAML afresh, so an authored edit
    /// reaches the guard here.
    fn generate(&self) -> Result<(), IdlError> {
        generate(self.root.path())
    }

    /// `check` verifies the generated index is fresh before the guard runs,
    /// so only a ledger-only edit reaches the guard here.
    fn check(&self) -> Result<(), IdlError> {
        check(self.root.path())
    }

    fn docs_page(&self, relative: &str) -> String {
        std::fs::read_to_string(self.idl().join("generated/docs").join(relative))
            .expect("generated page exists")
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).expect("scratch mkdir");
    for entry in std::fs::read_dir(source).expect("scratch read_dir") {
        let entry = entry.expect("scratch entry");
        let target = destination.join(entry.file_name());
        if entry.file_type().expect("scratch file_type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("scratch copy");
        }
    }
}

/// Unwraps the authored-IDL rejection a guard produced.
fn rejection(result: Result<(), IdlError>) -> String {
    match result {
        Err(IdlError::Invalid(message)) => message,
        Err(other) => panic!("expected an authored-IDL rejection, got {other}"),
        Ok(()) => panic!("expected the scratch tree to be rejected"),
    }
}

/// Asserts a rejection names the command and gives every reason, so a guard
/// that fires on the wrong command (or for the wrong reason) is visible.
fn assert_rejects(result: Result<(), IdlError>, command: &str, reasons: &[&str]) {
    let message = rejection(result);
    assert!(
        message.contains(&format!("`{command}`")),
        "expected a rejection of `{command}`, got: {message}"
    );
    for reason in reasons {
        assert!(
            message.contains(reason),
            "expected the rejection of `{command}` to mention {reason:?}, got: {message}"
        );
    }
}

#[test]
fn the_checked_in_tree_conforms_or_is_ledgered() {
    let scratch = Scratch::new();
    scratch
        .check()
        .expect("every declaration matches its schema or has a row");
}

#[test]
fn a_conforming_command_declaring_the_wrong_family_is_rejected() {
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/kv.yaml",
        KV_GET_HEAD,
        &format!("{KV_GET_HEAD}    response_model: StatusValue<{{payload}}>\n"),
    );
    assert_rejects(
        scratch.generate(),
        "kv.get",
        &[
            "declares `StatusValue<{payload}>` (a bare scalar) but its schema carries a `{found, value}` record",
            "correct the declaration",
            "list the command in response-model-divergences.yaml",
        ],
    );
}

#[test]
fn a_divergent_command_needs_a_row() {
    let scratch = Scratch::new();
    scratch.replace_in(LEDGER, JSON_INDEX_DROP_ROW, "");
    scratch.set_budget(10, 9);
    assert_rejects(
        scratch.check(),
        "json.index.drop",
        &["declares `MutationAck` (a mutation acknowledgement) but its schema carries a bare `boolean`"],
    );
}

#[test]
fn a_row_for_a_conforming_command_is_stale() {
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        JSON_INDEX_DROP_ROW,
        &format!(
            "{JSON_INDEX_DROP_ROW}  - command: kv.get\n    declared: Maybe<VersionedValue>\n    wire: found_value\n    issue: 3313\n"
        ),
    );
    scratch.set_budget(10, 11);
    assert_rejects(
        scratch.check(),
        "kv.get",
        &[
            "now carries a `{found, value}` record",
            "remove its row (#3313) from response-model-divergences.yaml (the ledger may only shrink)",
        ],
    );
}

#[test]
fn correcting_a_declaration_retires_its_row_and_its_inventory_entry() {
    // `vector.collection.stats` declares `StatusResponse<{payload}>` but ships
    // a page. Declaring the page makes the command conform and its row stale;
    // removing the row (and lowering the budget) is the drain, after which
    // the model the row published is in use by nothing and leaves the
    // inventory too.
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/vector.yaml",
        "    response_model: StatusResponse<{payload}>\n",
        "    response_model: Page<{payload}>\n",
    );
    assert_rejects(
        scratch.generate(),
        "vector.collection.stats",
        &[
            "now carries a page as its `Page<{payload}>` declares",
            "remove its row (#3313)",
        ],
    );

    scratch.replace_in(
        LEDGER,
        "  - command: vector.collection.stats\n    declared: StatusResponse<VectorCollectionInfo>\n    wire: page\n    issue: 3313\n",
        "",
    );
    scratch.set_budget(10, 9);
    let message = rejection(scratch.generate());
    assert!(
        message.contains(
            "dto-inventory.yaml lists `StatusResponse<VectorCollectionInfo>` which no command resolves to; remove it"
        ),
        "got: {message}"
    );

    scratch.replace_in(INVENTORY, "  - StatusResponse<VectorCollectionInfo>\n", "");
    scratch
        .generate()
        .expect("a corrected declaration with its row and inventory entry drained passes");
}

#[test]
fn a_row_states_the_target_within_the_declared_family() {
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        "  - command: json.index.drop\n    declared: MutationAck\n",
        "  - command: json.index.drop\n    declared: StatusValue<boolean>\n",
    );
    assert_rejects(
        scratch.check(),
        "json.index.drop",
        &[
            "declares `StatusValue<boolean>` (a bare scalar) but the command declares `MutationAck` (a mutation acknowledgement)",
            "a row states the target within the declared family",
        ],
    );

    // The acknowledgement family carries no payload, so a row cannot give it one.
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        "  - command: json.index.drop\n    declared: MutationAck\n",
        "  - command: json.index.drop\n    declared: MutationAck<JsonIndexDrop>\n",
    );
    assert_rejects(
        scratch.check(),
        "json.index.drop",
        &["`MutationAck<JsonIndexDrop>` of an unknown family"],
    );
}

#[test]
fn a_row_payload_must_name_a_def_of_the_schema_or_a_primitive() {
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        "    declared: Maybe<RemoteOriginInfo>\n",
        "    declared: Maybe<RemoteOriginInfoo>\n",
    );
    assert_rejects(
        scratch.check(),
        "admin.remote",
        &["declares `Maybe<RemoteOriginInfoo>` but `RemoteOriginInfoo` is neither a `$def` of the command's schema nor a wire primitive"],
    );

    // A primitive passes the name check; what then fails is the inventory,
    // which no longer sees `Maybe<RemoteOriginInfo>` in use.
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        "    declared: Maybe<RemoteOriginInfo>\n",
        "    declared: Maybe<string>\n",
    );
    let message = rejection(scratch.check());
    assert!(
        message.contains("lists `Maybe<RemoteOriginInfo>` which no command resolves to"),
        "got: {message}"
    );
}

#[test]
fn a_row_must_record_the_wire_it_ledgers() {
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        JSON_INDEX_DROP_ROW,
        &JSON_INDEX_DROP_ROW.replace("wire: scalar:boolean", "wire: record"),
    );
    assert_rejects(
        scratch.check(),
        "json.index.drop",
        &["records wire `record` but the schema carries `scalar:boolean`"],
    );
}

#[test]
fn a_complete_model_is_rejected_where_a_template_belongs() {
    // At the command layer.
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/kv.yaml",
        KV_GET_HEAD,
        &format!("{KV_GET_HEAD}    response_model: Maybe<VersionedValue>\n"),
    );
    assert_rejects(
        scratch.generate(),
        "kv.get",
        &[
            "declares response_model `Maybe<VersionedValue>`",
            "the payload is derived from the schema, so declare the family template `Maybe<{payload}>`",
        ],
    );

    // At the kind layer, where every `read.get` command inherits it.
    let scratch = Scratch::new();
    scratch.replace_in(
        "kinds.yaml",
        "    response_model: Maybe<{payload}>\n",
        "    response_model: Maybe<VersionedValue>\n",
    );
    let message = rejection(scratch.generate());
    assert!(
        message.contains("declare the family template `Maybe<{payload}>`"),
        "got: {message}"
    );

    // The acknowledgement family has no slot to fill.
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/kv.yaml",
        "    input: Command::KvPut\n    output: Output::WriteResult\n",
        "    input: Command::KvPut\n    output: Output::WriteResult\n    response_model: MutationAck<KvWrite>\n",
    );
    assert_rejects(
        scratch.generate(),
        "kv.put",
        &["`MutationAck<KvWrite>` of an unknown family"],
    );
}

#[test]
fn an_authored_result_name_is_rejected() {
    // `result:` used to name the payload by hand; the field is gone and the
    // command-source field list refuses it, so a stale command file cannot
    // reintroduce a second spelling of the payload beside the derived one.
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/kv.yaml",
        KV_GET_HEAD,
        &format!("{KV_GET_HEAD}    result: VersionedValue\n"),
    );
    let message = rejection(scratch.generate());
    assert!(
        message.contains("commands/kv.yaml") && message.contains("unknown command field `result`"),
        "expected the source field list to reject `result:`, got: {message}"
    );
}

#[test]
fn the_inventory_lists_exactly_the_models_in_use() {
    // A derived model the inventory lacks: the review-once gate.
    let scratch = Scratch::new();
    scratch.replace_in(INVENTORY, "  - Maybe<VersionedValue>\n", "");
    assert_rejects(
        scratch.generate(),
        "kv.get",
        &[
            "resolves to response model `Maybe<VersionedValue>` which dto-inventory.yaml does not list",
            "add it (every published model is reviewed once)",
        ],
    );

    // A listed model nothing resolves to: the shrink gate.
    let scratch = Scratch::new();
    scratch.replace_in(
        INVENTORY,
        "  - Maybe<VersionedValue>\n",
        "  - Maybe<VersionedValue>\n  - Maybe<Ghost>\n",
    );
    let message = rejection(scratch.check());
    assert!(
        message.contains(
            "dto-inventory.yaml lists `Maybe<Ghost>` which no command resolves to; remove it (the inventory carries only the models in use)"
        ),
        "got: {message}"
    );

    let scratch = Scratch::new();
    scratch.replace_in(
        INVENTORY,
        "  - Maybe<VersionedValue>\n",
        "  - Maybe<VersionedValue>\n  - Maybe<VersionedValue>\n",
    );
    let message = rejection(scratch.check());
    assert!(
        message.contains("duplicate `Maybe<VersionedValue>` in dto-inventory.yaml"),
        "got: {message}"
    );
}

#[test]
fn resolved_models_are_spelled_by_the_schema() {
    // One command per family and per payload kind, observed through the
    // resolver the generators and the CLI index consume.
    let index = resolve_default_index().expect("index resolves");
    let expected: BTreeMap<&str, &str> = BTreeMap::from([
        ("kv.put", "MutationAck"),
        ("kv.get", "Maybe<VersionedValue>"),
        ("admin.config_key", "Maybe<string>"),
        ("kv.history", "Maybe<Vec<HistoryItem>>"),
        ("json.history", "Maybe<Vec<JsonHistoryItem>>"),
        ("kv.list", "Page<Bytes>"),
        ("branch.list", "Page<BranchItem>"),
        ("vector.keys", "Page<string>"),
        ("json.sample", "SamplePage<JsonSampleItem>"),
        ("vector.query", "SearchResult<VectorMatch>"),
        (
            "vector.index.query",
            "SearchResult<VectorMatch> + IndexDiagnostics",
        ),
        ("kv.exists", "StatusValue<boolean>"),
        ("branch.get", "StatusResponse<BranchItem>"),
        (
            "graph.analytics.pagerank",
            "AnalyticsResult<GraphPagerankData>",
        ),
        ("kv.batch_get", "BatchResult<BatchGetItemResult>"),
        ("inference.tokenize", "integer[]"),
        ("inference.detokenize", "string"),
        ("inference.generate", "ChatResponse"),
        // Ledgered: the row's target, not the wire.
        ("event.count", "StatusValue<integer>"),
        ("admin.remote", "Maybe<RemoteOriginInfo>"),
        (
            "vector.collection.stats",
            "StatusResponse<VectorCollectionInfo>",
        ),
    ]);
    let resolved: BTreeMap<&str, &str> = index
        .commands
        .iter()
        .filter(|command| expected.contains_key(command.id.as_str()))
        .map(|command| (command.id.as_str(), command.response_model.as_str()))
        .collect();
    assert_eq!(resolved, expected);

    // No command publishes a template, a cursor type, or a Rust spelling.
    for command in &index.commands {
        let model = &command.response_model;
        for stray in ["{payload}", ", ", "u64", "bool>", "String>", "Vec<Maybe"] {
            assert!(
                !model.contains(stray),
                "`{}` resolved `{model}`",
                command.id
            );
        }
    }
}

#[test]
fn a_ledgered_command_must_be_transitional() {
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/json.yaml",
        "    prose: commands/json.index.drop.md\n    wire_status: transitional\n",
        "    prose: commands/json.index.drop.md\n",
    );
    assert_rejects(
        scratch.generate(),
        "json.index.drop",
        &[
            "is listed in response-model-divergences.yaml but its wire_status is `stable`",
            "a ledgered divergence must be `transitional`",
        ],
    );
}

#[test]
fn a_transitional_command_that_conforms_needs_no_row() {
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/kv.yaml",
        "    prose: commands/kv.get.md\n",
        "    prose: commands/kv.get.md\n    wire_status: transitional\n",
    );
    scratch
        .generate()
        .expect("transitional marks a wire that is not frozen, not a divergence");
}

#[test]
fn the_budget_tracks_the_row_count() {
    let scratch = Scratch::new();
    scratch.set_budget(10, 11);
    let message = rejection(scratch.check());
    assert!(
        message.contains(LEDGER) && message.contains("debt is below its budget"),
        "got: {message}"
    );

    let scratch = Scratch::new();
    scratch.set_budget(10, 9);
    let message = rejection(scratch.check());
    assert!(
        message.contains(LEDGER) && message.contains("debt grew past its budget"),
        "got: {message}"
    );
}

#[test]
fn unknown_and_duplicate_rows_are_rejected() {
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        JSON_INDEX_DROP_ROW,
        &format!(
            "{JSON_INDEX_DROP_ROW}  - command: kv.nonexistent\n    declared: Maybe<Nothing>\n    wire: record\n    issue: 3313\n"
        ),
    );
    scratch.set_budget(10, 11);
    assert_rejects(
        scratch.check(),
        "kv.nonexistent",
        &["which is not a command id; remove it"],
    );

    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        JSON_INDEX_DROP_ROW,
        &format!("{JSON_INDEX_DROP_ROW}{JSON_INDEX_DROP_ROW}"),
    );
    scratch.set_budget(10, 11);
    assert_rejects(scratch.check(), "json.index.drop", &["duplicate"]);
}

#[test]
fn encodings_are_resolved_into_the_cli_index() {
    let index = resolve_default_cli_index().expect("CLI index resolves");
    let expected: BTreeMap<&str, CliWireEncoding> = BTreeMap::from([
        ("kv.get", CliWireEncoding::FoundValue),
        ("json.get", CliWireEncoding::FoundValue),
        ("event.get", CliWireEncoding::FoundValue),
        ("vector.get", CliWireEncoding::FoundValue),
        ("graph.node.get", CliWireEncoding::FoundValue),
        ("graph.edge.get", CliWireEncoding::FoundValue),
        ("admin.config_key", CliWireEncoding::Nullable),
        ("graph.meta", CliWireEncoding::Nullable),
        ("graph.ontology.get", CliWireEncoding::Nullable),
        ("graph.ontology.summary", CliWireEncoding::Nullable),
        ("kv.history", CliWireEncoding::Items),
        ("vector.history", CliWireEncoding::Items),
        ("json.history", CliWireEncoding::Array),
    ]);

    let resolved: BTreeMap<&str, CliWireEncoding> = index
        .commands
        .iter()
        .filter(|command| command.render != CliRenderRule::Page)
        .filter_map(|command| {
            command
                .encoding
                .map(|encoding| (command.id.as_str(), encoding))
        })
        .collect();
    assert_eq!(resolved, expected);

    // Every `page` command carries `page` or `sample_page`, and the sampled
    // ones are exactly the `*.sample` commands: the renderer's
    // `-- sampled N of M` notice follows this encoding, never `total_count`.
    let sampled: BTreeSet<&str> = index
        .commands
        .iter()
        .filter(|command| command.encoding == Some(CliWireEncoding::SamplePage))
        .map(|command| command.id.as_str())
        .collect();
    let sample_commands: BTreeSet<&str> = index
        .commands
        .iter()
        .map(|command| command.id.as_str())
        .filter(|id| id.ends_with(".sample"))
        .collect();
    assert!(
        !sample_commands.is_empty(),
        "the catalog has sample commands"
    );
    assert_eq!(sampled, sample_commands);
    for command in &index.commands {
        if command.render == CliRenderRule::Page {
            assert!(
                matches!(
                    command.encoding,
                    Some(CliWireEncoding::Page | CliWireEncoding::SamplePage)
                ),
                "`{}` resolved {:?}",
                command.id,
                command.encoding
            );
        }
    }

    // Every `optional`/`history`/`page` command carries an encoding, except
    // the one whose wire is a bare record and is ledgered as transitional for
    // it.
    for command in &index.commands {
        let reads_one = matches!(
            command.render,
            CliRenderRule::Optional | CliRenderRule::History | CliRenderRule::Page
        );
        match (command.id.as_str(), reads_one) {
            ("admin.remote", true) => {
                assert_eq!(command.encoding, None);
                assert_eq!(command.wire_status, "transitional");
            }
            (id, true) => assert!(command.encoding.is_some(), "`{id}` resolved no encoding"),
            (id, false) => assert_eq!(command.encoding, None, "`{id}` carries an encoding"),
        }
    }
}

#[test]
fn the_cli_index_refuses_a_stable_optional_wire_that_resolved_no_encoding() {
    // `admin.remote` renders `optional` over a bare-record wire, which is only
    // tolerable while the command is transitional. The CLI index generator
    // reads the generated command index, so the flip is made there.
    let scratch = Scratch::new();
    scratch.replace_in(
        "generated/command-index.json",
        "\"Output::RemoteOriginResult\"\n      ],\n      \"wire_status\": \"transitional\",\n",
        "\"Output::RemoteOriginResult\"\n      ],\n      \"wire_status\": \"stable\",\n",
    );
    assert_rejects(
        check_cli(scratch.root.path()),
        "admin.remote",
        &["renders `optional` but its stable wire resolved to no encoding"],
    );
}

#[test]
fn docs_say_which_shape_a_transitional_wire_carries() {
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/kv.yaml",
        "    prose: commands/kv.get.md\n",
        "    prose: commands/kv.get.md\n    wire_status: transitional\n",
    );
    generate_docs(scratch.root.path()).expect("reference pages render");

    // A ledgered divergence names the shape the wire carries today.
    let divergent = scratch.docs_page("event/count.md");
    assert!(divergent.contains("`StatusValue<integer>`."), "{divergent}");
    assert!(
        divergent.contains(
            "**Transitional wire:** the response currently carries a bare record rather than a bare scalar; the declaration is the target shape and the wire is scheduled to be normalised."
        ),
        "{divergent}"
    );

    // A transitional command that conforms only says the wire is not frozen.
    let conforming = scratch.docs_page("kv/get.md");
    assert!(
        conforming.contains(
            "**Transitional wire:** this command's response shape is not yet frozen and may change."
        ),
        "{conforming}"
    );
    assert!(
        !conforming.contains("the response currently carries"),
        "{conforming}"
    );

    // A stable command says nothing about its wire.
    let stable = scratch.docs_page("kv/put.md");
    assert!(!stable.contains("Transitional wire"), "{stable}");
}

#[test]
fn a_reference_page_shows_what_each_example_step_prints() {
    // R7 (#3314 S5): the CLI tab is a transcript. Each command is followed by
    // the text it printed, captured by replaying the example — so the page
    // shows a reader what they will actually see, and cannot drift from it.
    let scratch = Scratch::new();
    generate_docs(scratch.root.path()).expect("reference pages render");

    let page = scratch.docs_page("kv/get.md");
    assert!(
        page.contains("$ strata kv put greeting hello\ncreated greeting\n"),
        "{page}"
    );
    assert!(page.contains("$ strata kv get greeting\nhello\n"), "{page}");
    assert!(page.contains("$ strata kv get absent\n(nil)\n"), "{page}");
}

#[test]
fn a_page_whose_transcript_was_elided_says_what_the_placeholder_means() {
    let scratch = Scratch::new();
    generate_docs(scratch.root.path()).expect("reference pages render");

    // An event's timestamp is a real instant, so the capture elides it.
    let elided = scratch.docs_page("event/list.md");
    assert!(elided.contains("…"), "{elided}");
    assert!(
        elided.contains(
            "`…` stands for a value that varies by run or by machine — an instant, a version, an \
             id, a path."
        ),
        "{elided}"
    );

    // A page whose every step is reproducible shows no legend to explain.
    let exact = scratch.docs_page("kv/get.md");
    assert!(!exact.contains("varies by run or by machine"), "{exact}");
}

#[test]
fn a_capture_that_no_longer_matches_its_example_fails_the_docs_build() {
    // The captures are regenerated by a different crate, so the two artifacts
    // can be committed out of step. The page is built from both, and disagreement
    // means one of them is describing an example that no longer exists.
    let scratch = Scratch::new();
    scratch.replace_in(
        "generated/command-examples.json",
        "\"in\": \"strata kv get absent\"",
        "\"in\": \"strata kv get yesterdays-key\"",
    );
    let message = rejection(generate_docs(scratch.root.path()));
    assert!(
        message.contains("command-examples.json is stale")
            && message.contains("strata kv get yesterdays-key")
            && message.contains("--ignored regenerate"),
        "{message}"
    );
}

#[test]
fn a_capture_missing_a_step_fails_the_docs_build() {
    // The pairing is positional, so a capture that lost a step would otherwise
    // print the next command's output under this one — silently, and only for
    // the steps after the gap.
    let scratch = Scratch::new();
    scratch.replace_in(
        "generated/command-examples.json",
        ",\n      {\n        \"in\": \"strata kv get absent\",\n        \"out\": \"(nil)\",\n        \
         \"reproducible\": true\n      }",
        "",
    );
    let message = rejection(generate_docs(scratch.root.path()));
    assert!(
        message.contains("command-examples.json is stale")
            && message.contains("`kv.get` has 3 example steps but 2 captured"),
        "{message}"
    );
}
