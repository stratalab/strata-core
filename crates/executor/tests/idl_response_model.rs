//! The response-model guard (#3313, S0c of the CLI output contract #3314).
//!
//! Every command declares a `response_model` family and every family implies
//! a wire shape. `generate` and `check` classify what the generated schema
//! actually carries under `response.data` and compare it with the
//! declaration; the commands where the two disagree live in the shrink-only
//! `response-model-divergences.yaml`, each marked `wire_status: transitional`.
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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use strata_executor::cli_metadata::{CliRenderRule, CliWireEncoding};
use strata_executor::idl_tooling::{
    check, check_cli, generate, generate_docs, resolve_default_cli_index, IdlError,
};

const LEDGER: &str = "response-model-divergences.yaml";

/// The ledger row for `json.index.drop` exactly as authored, the anchor for
/// the row-shaped edits below.
const JSON_INDEX_DROP_ROW: &str = "  - command: json.index.drop
    declared: MutationAck<JsonIndexDrop>
    wire: scalar:boolean
    issue: 3313
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
        "    result: VersionedValue\n    prose: commands/kv.get.md\n",
        "    result: VersionedValue\n    response_model: StatusValue<bool>\n    prose: commands/kv.get.md\n",
    );
    assert_rejects(
        scratch.generate(),
        "kv.get",
        &[
            "declares `StatusValue<bool>` (a bare scalar) but its schema carries a `{found, value}` record",
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
        &["declares `MutationAck<JsonIndexDrop>` (a mutation acknowledgement) but its schema carries a bare `boolean`"],
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
fn correcting_a_declaration_retires_its_row() {
    // `vector.collection.stats` declares `StatusResponse<VectorCollectionInfo>`
    // but ships a page. Declaring the page makes the command conform and its
    // row stale; removing the row (and lowering the budget) is the drain.
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/vector.yaml",
        "    response_model: StatusResponse<VectorCollectionInfo>\n",
        "    response_model: Page<VectorCollectionInfo, String>\n",
    );
    assert_rejects(
        scratch.generate(),
        "vector.collection.stats",
        &["now carries a page", "remove its row (#3313)"],
    );

    scratch.replace_in(
        LEDGER,
        "  - command: vector.collection.stats\n    declared: StatusResponse<VectorCollectionInfo>\n    wire: page\n    issue: 3313\n",
        "",
    );
    scratch.set_budget(10, 9);
    scratch
        .generate()
        .expect("a corrected declaration with its row drained passes");
}

#[test]
fn a_row_must_record_the_declaration_and_wire_it_ledgers() {
    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        "    declared: MutationAck<JsonIndexDrop>\n",
        "    declared: MutationAck<Other>\n",
    );
    assert_rejects(
        scratch.check(),
        "json.index.drop",
        &["records declared `MutationAck<Other>` but the command declares `MutationAck<JsonIndexDrop>`"],
    );

    let scratch = Scratch::new();
    scratch.replace_in(
        LEDGER,
        "    declared: MutationAck<JsonIndexDrop>\n    wire: scalar:boolean\n",
        "    declared: MutationAck<JsonIndexDrop>\n    wire: record\n",
    );
    assert_rejects(
        scratch.check(),
        "json.index.drop",
        &["records wire `record` but the schema carries `scalar:boolean`"],
    );
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
        .filter_map(|command| {
            command
                .encoding
                .map(|encoding| (command.id.as_str(), encoding))
        })
        .collect();
    assert_eq!(resolved, expected);

    // Every `optional`/`history` command carries an encoding, except the one
    // whose wire is a bare record and is ledgered as transitional for it.
    for command in &index.commands {
        let reads_one = matches!(
            command.render,
            CliRenderRule::Optional | CliRenderRule::History
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
    assert!(divergent.contains("`StatusValue<u64>`."), "{divergent}");
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
