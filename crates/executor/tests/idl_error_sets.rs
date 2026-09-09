//! Named error sets in the IDL (#3250).
//!
//! The overlay's reuse layers are defaults → family → kind → command, so a
//! fact that crosses families or kinds — "everything the embeddings runtime
//! can fail with" is shared by `inference.embed`, `vector.upsert` and
//! `vector.query` — had no layer to live in and was copied by hand into each
//! command. `error-sets.yaml` names such a set once; any error list refers to
//! it as `set:<name>` and the resolver expands the reference in place, so every
//! generated surface keeps its shape. These tests drive the resolver over a
//! scratch copy of the real tree with one authored change at a time, so each
//! rule is observed through the call site the gates use (`check` resolves the
//! same index), not through a helper in isolation.
//!
//! Requires `inference` as well as `idl-tooling` for the same reason as the
//! sibling full-catalog IDL tests: the `inference.*` request fixtures only
//! deserialize when those `Command` variants are compiled in (#2982).

#![cfg(all(feature = "idl-tooling", feature = "inference"))]

use std::path::{Path, PathBuf};

use strata_executor::idl_tooling::{resolve_index, CommandIndex, IdlError, ResolvedCommand};

/// Two registered codes that no authored error list holds together, so a
/// scratch set built from them never turns the real tree's own lists into a
/// literal copy of the set (which the resolver rejects — see
/// `a_literal_list_holding_every_code_of_a_set_is_rejected`).
const MEMBER_A: &str = "inference.download_disabled";
const MEMBER_B: &str = "invalid_argument.engine.vector_filter";
/// A third such code for the nesting tests; it shares no list with A or B.
const MEMBER_C: &str = "invalid_argument.executor.hub_url";

/// The command every mechanism test edits: one `errors+` entry, an empty
/// family error list, and a kind without errors, so its resolved list is the
/// two defaults codes plus whatever the test authors.
const PROBE: &str = "inference.capability";
/// The lines bracketing the probe's `errors+` block; `set_probe_errors`
/// rewrites whatever lies between them, so it can be called repeatedly.
const PROBE_BEFORE: &str = "    prose: commands/inference.capability.md\n";
const PROBE_AFTER: &str = "    fixtures:\n      request: requests/v1/inference/capability.json\n";

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
        // The resolver also scans the enum sources for variant coverage.
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

    /// Appends list items to the authored `error-sets.yaml`, after the real
    /// sets (which the real commands reference, so they must stay).
    fn add_sets(&self, items: &str) {
        let path = self.idl().join("error-sets.yaml");
        let mut text = std::fs::read_to_string(&path).expect("read error-sets.yaml");
        assert!(
            text.contains("\nsets:\n"),
            "error-sets.yaml ends with the `sets:` list the items append to"
        );
        text.push_str(items);
        std::fs::write(&path, text).expect("write error-sets.yaml");
    }

    /// Rewrites the probe command's `errors+` block. `entries` are the raw
    /// YAML list items: codes or `set:` references.
    fn set_probe_errors(&self, entries: &[&str]) {
        let path = self.idl().join("commands/inference.yaml");
        let text = std::fs::read_to_string(&path).expect("read commands/inference.yaml");
        let start = text.find(PROBE_BEFORE).expect("probe prose line") + PROBE_BEFORE.len();
        let end = text[start..]
            .find(PROBE_AFTER)
            .map(|offset| start + offset)
            .expect("probe fixtures line");
        assert!(
            text[start..end].starts_with("    errors+:\n"),
            "the probe block between the anchors must be its errors+ list"
        );
        let mut block = String::from("    errors+:\n");
        for entry in entries {
            block.push_str("      - ");
            block.push_str(entry);
            block.push('\n');
        }
        let rewritten = format!("{}{block}{}", &text[..start], &text[end..]);
        std::fs::write(&path, rewritten).expect("write commands/inference.yaml");
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

    fn resolve(&self) -> Result<CommandIndex, IdlError> {
        resolve_index(self.root.path())
    }

    fn resolve_probe(&self) -> ResolvedCommand {
        let index = self.resolve().expect("scratch tree resolves");
        index
            .commands
            .into_iter()
            .find(|command| command.id == PROBE)
            .expect("probe command is in the index")
    }

    /// Resolves and returns the rejection message.
    fn rejection(&self) -> String {
        match self.resolve() {
            Err(IdlError::Invalid(message)) => message,
            Err(other) => panic!("expected an authored-IDL rejection, got {other}"),
            Ok(_) => panic!("expected the scratch tree to be rejected"),
        }
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

fn codes(command: &ResolvedCommand) -> Vec<&str> {
    command
        .errors
        .iter()
        .map(|error| error.code.as_str())
        .collect()
}

fn pair_set(name: &str) -> String {
    format!("  - id: {name}\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n")
}

#[test]
fn a_set_reference_expands_in_place_to_its_codes() {
    let scratch = Scratch::new();
    scratch.add_sets(&pair_set("probe.pair"));
    scratch.set_probe_errors(&["inference.invalid_request", "set:probe.pair"]);

    let probe = scratch.resolve_probe();
    assert_eq!(
        codes(&probe),
        [
            "failed_precondition.engine.runtime_closed",
            "not_found.engine.branch",
            "inference.invalid_request",
            MEMBER_A,
            MEMBER_B,
        ],
        "the set's codes land where the reference stood, after the inherited defaults"
    );
    assert!(
        probe
            .errors
            .iter()
            .all(|error| error.docs.starts_with("https://")),
        "expanded codes carry the same docs URL every other code does"
    );
}

#[test]
fn a_set_reference_dedupes_against_codes_already_present() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.pair\n    errors:\n      - {MEMBER_A}\n      - not_found.engine.branch\n"
    ));
    scratch.set_probe_errors(&["set:probe.pair", MEMBER_A]);

    let probe = scratch.resolve_probe();
    assert_eq!(
        codes(&probe),
        [
            "failed_precondition.engine.runtime_closed",
            "not_found.engine.branch",
            MEMBER_A,
        ],
        "a code inherited from defaults, or listed beside the set, appears once"
    );
}

#[test]
fn a_set_reference_in_errors_minus_removes_every_code_of_the_set() {
    let scratch = Scratch::new();
    scratch.add_sets(
        "  - id: probe.defaults\n    errors:\n      - failed_precondition.engine.runtime_closed\n      - not_found.engine.branch\n",
    );
    // `errors-` alone: the probe keeps nothing but what it lists itself.
    scratch.replace_in(
        "commands/inference.yaml",
        PROBE_AFTER,
        "    errors-:\n      - set:probe.defaults\n    fixtures:\n      request: requests/v1/inference/capability.json\n",
    );
    // The defaults layer itself now lists the set's two codes literally; it
    // must reference the set instead (the anti-copy rule), so make it.
    scratch.replace_in(
        "defaults.yaml",
        "errors:\n  - failed_precondition.engine.runtime_closed\n  - not_found.engine.branch\n",
        "errors:\n  - set:probe.defaults\n",
    );

    let probe = scratch.resolve_probe();
    assert_eq!(codes(&probe), ["inference.invalid_request"]);
}

#[test]
fn a_set_reference_is_accepted_on_every_layer() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.pair\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n  - id: probe.kind\n    errors:\n      - {MEMBER_C}\n      - {MEMBER_A}\n"
    ));
    // Family layer: the inference family declares no errors of its own.
    scratch.replace_in(
        "families.yaml",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors: []\n",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors:\n      - set:probe.pair\n",
    );
    // Kind layer: the probe's kind declares no errors of its own either.
    scratch.replace_in(
        "kinds.yaml",
        "  - id: inference.runtime_op\n    access: read\n    commit: none\n",
        "  - id: inference.runtime_op\n    access: read\n    commit: none\n    errors:\n      - set:probe.kind\n",
    );

    let probe = scratch.resolve_probe();
    assert_eq!(
        codes(&probe),
        [
            "failed_precondition.engine.runtime_closed",
            "not_found.engine.branch",
            MEMBER_A,
            MEMBER_B,
            MEMBER_C,
            "inference.invalid_request",
        ],
        "family and kind sets expand in layer order before the command's own additions, deduplicated"
    );
}

#[test]
fn set_ids_are_dotted_lowercase_segments() {
    // Through the call site, one scratch tree per id: the accepted ids must
    // resolve (referenced from the probe, so the orphan rule cannot mask the
    // verdict) and the rejected ones must fail on the id itself.
    for id in ["probe", "probe.pair_2", "p2.x_y"] {
        let scratch = Scratch::new();
        scratch.add_sets(&pair_set(id));
        scratch.set_probe_errors(&[&format!("set:{id}")]);
        let probe = scratch.resolve_probe();
        assert!(
            codes(&probe).contains(&MEMBER_A),
            "`{id}` is a well-formed set id and expands"
        );
    }
    for id in [
        "",
        "Probe.pair",
        "probe pair",
        "probe-pair",
        "probe.2x",
        "_probe",
        ".probe",
        "probe.",
        "probe..pair",
    ] {
        let scratch = Scratch::new();
        scratch.add_sets(&pair_set(&format!("\"{id}\"")));
        let message = scratch.rejection();
        assert!(
            message.contains("error set id") && message.contains(&format!("`{id}`")),
            "`{id}` is rejected as a malformed set id: {message}"
        );
    }
}

#[test]
fn a_set_may_reference_a_set_declared_above_it() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.inner\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n  - id: probe.outer\n    errors:\n      - set:probe.inner\n      - {MEMBER_C}\n"
    ));
    scratch.set_probe_errors(&["set:probe.outer"]);

    let probe = scratch.resolve_probe();
    assert_eq!(
        codes(&probe),
        [
            "failed_precondition.engine.runtime_closed",
            "not_found.engine.branch",
            MEMBER_A,
            MEMBER_B,
            MEMBER_C,
        ],
        "the inner set expands inside the outer one, in declaration order"
    );
}

#[test]
fn a_set_may_not_reference_a_set_declared_below_it() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.outer\n    errors:\n      - set:probe.inner\n      - {MEMBER_C}\n  - id: probe.inner\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n"
    ));
    scratch.set_probe_errors(&["set:probe.outer"]);

    let message = scratch.rejection();
    assert!(
        message.contains("probe.outer") && message.contains("probe.inner"),
        "names both sets: {message}"
    );
}

#[test]
fn a_set_may_not_reference_itself() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.loop\n    errors:\n      - set:probe.loop\n      - {MEMBER_A}\n"
    ));
    scratch.set_probe_errors(&["set:probe.loop"]);

    let message = scratch.rejection();
    assert!(message.contains("probe.loop"), "names the set: {message}");
}

#[test]
fn an_unknown_set_reference_is_rejected_and_names_the_site() {
    let scratch = Scratch::new();
    scratch.add_sets(&pair_set("probe.pair"));
    scratch.set_probe_errors(&["set:probe.pair", "set:probe.ghost"]);

    let message = scratch.rejection();
    assert!(
        message.contains("probe.ghost") && message.contains(PROBE),
        "names the unknown set and the command: {message}"
    );
}

#[test]
fn an_unknown_set_reference_on_a_layer_is_rejected_and_names_the_layer() {
    let scratch = Scratch::new();
    scratch.add_sets(&pair_set("probe.pair"));
    scratch.set_probe_errors(&["set:probe.pair"]);
    scratch.replace_in(
        "families.yaml",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors: []\n",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors:\n      - set:probe.ghost\n",
    );

    let message = scratch.rejection();
    assert!(
        message.contains("probe.ghost")
            && message.contains("family")
            && message.contains("inference"),
        "names the unknown set and the family layer: {message}"
    );
}

#[test]
fn a_set_nobody_references_is_rejected() {
    let scratch = Scratch::new();
    scratch.add_sets(&pair_set("probe.orphan"));

    let message = scratch.rejection();
    assert!(
        message.contains("probe.orphan") && message.contains("referenced"),
        "names the orphan set: {message}"
    );
}

#[test]
fn a_set_referenced_only_by_another_set_counts_as_referenced() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.inner\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n  - id: probe.outer\n    errors:\n      - set:probe.inner\n      - {MEMBER_C}\n"
    ));
    scratch.set_probe_errors(&["set:probe.outer"]);

    assert!(
        scratch.resolve().is_ok(),
        "probe.inner is used through probe.outer"
    );
}

#[test]
fn a_set_with_a_code_missing_from_the_registry_is_rejected() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.pair\n    errors:\n      - {MEMBER_A}\n      - inference.ghost_code\n"
    ));
    scratch.set_probe_errors(&["set:probe.pair"]);

    let message = scratch.rejection();
    assert!(
        message.contains("inference.ghost_code") && message.contains("probe.pair"),
        "names the code and the set: {message}"
    );
}

#[test]
fn a_set_that_lists_a_code_twice_is_rejected() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.pair\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n      - {MEMBER_A}\n"
    ));
    scratch.set_probe_errors(&["set:probe.pair"]);

    let message = scratch.rejection();
    assert!(
        message.contains(MEMBER_A) && message.contains("probe.pair"),
        "names the duplicated code and the set: {message}"
    );
}

#[test]
fn a_set_with_fewer_than_two_codes_is_rejected() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.single\n    errors:\n      - {MEMBER_A}\n"
    ));
    scratch.set_probe_errors(&["set:probe.single"]);

    // The reason must be the size floor: a one-code set is also a subset of
    // every list holding that code, so a weaker check would let the copy rule
    // reject the tree for the wrong reason and hide a broken floor.
    let message = scratch.rejection();
    assert!(
        message.contains("probe.single") && message.contains("fewer than two codes"),
        "names the set and the size floor: {message}"
    );
}

#[test]
fn a_duplicate_set_id_is_rejected() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.pair\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n  - id: probe.pair\n    errors:\n      - {MEMBER_B}\n      - {MEMBER_C}\n"
    ));
    scratch.set_probe_errors(&["set:probe.pair"]);

    let message = scratch.rejection();
    assert!(
        message.contains("duplicate") && message.contains("probe.pair"),
        "names the duplicated id: {message}"
    );
}

/// The rule that closes the copy class: once a set names a group of codes, an
/// authored list that spells out every one of them is a hand copy and must
/// reference the set instead. Exact, not heuristic — it fires only on a
/// literal superset, so a list that shares some codes with a set is fine.
#[test]
fn a_literal_list_holding_every_code_of_a_set_is_rejected() {
    let scratch = Scratch::new();
    scratch.add_sets(&pair_set("probe.pair"));
    // Someone else references the set, so this is not the orphan rule.
    scratch.replace_in(
        "families.yaml",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors: []\n",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors:\n      - set:probe.pair\n",
    );
    scratch.set_probe_errors(&[MEMBER_B, "inference.invalid_request", MEMBER_A]);

    let message = scratch.rejection();
    assert!(
        message.contains(PROBE) && message.contains("set:probe.pair"),
        "names the command and the set to reference: {message}"
    );

    // Direction control: a list sharing only some of the set's codes is not
    // a copy of it.
    scratch.set_probe_errors(&[MEMBER_A, "inference.invalid_request"]);
    assert!(scratch.resolve().is_ok(), "a partial overlap is not a copy");
}

#[test]
fn a_set_spelling_out_every_code_of_an_earlier_set_is_rejected() {
    let scratch = Scratch::new();
    scratch.add_sets(&format!(
        "  - id: probe.inner\n    errors:\n      - {MEMBER_A}\n      - {MEMBER_B}\n  - id: probe.outer\n    errors:\n      - {MEMBER_B}\n      - {MEMBER_C}\n      - {MEMBER_A}\n"
    ));
    scratch.set_probe_errors(&["set:probe.outer"]);
    scratch.replace_in(
        "families.yaml",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors: []\n",
        "  - id: inference\n    docs: /docs/inference/{op_path}\n    errors:\n      - set:probe.inner\n",
    );

    let message = scratch.rejection();
    assert!(
        message.contains("probe.outer") && message.contains("set:probe.inner"),
        "names the copying set and the set it should reference: {message}"
    );
}

#[test]
fn a_layer_list_holding_every_code_of_a_set_is_rejected() {
    let scratch = Scratch::new();
    // The two defaults codes, as a set nobody but the defaults layer copies.
    scratch.add_sets(
        "  - id: probe.defaults\n    errors:\n      - failed_precondition.engine.runtime_closed\n      - not_found.engine.branch\n",
    );
    scratch.set_probe_errors(&["set:probe.defaults"]);

    let message = scratch.rejection();
    assert!(
        message.contains("defaults") && message.contains("set:probe.defaults"),
        "names the defaults layer and the set: {message}"
    );
}

#[test]
fn a_command_may_not_reference_a_set_the_registry_overlay_does_not_cover() {
    // A set whose codes are registered but not declared in `errors.yaml`
    // would smuggle undeclared codes onto a command; the same rule that
    // rejects a bare undeclared code rejects it through the set.
    let scratch = Scratch::new();
    scratch.replace_in("errors.yaml", &format!("  - {MEMBER_A}\n"), "");
    scratch.replace_in(
        "uncovered-error-codes.yaml",
        "uncovered:\n",
        &format!("uncovered:\n  - {MEMBER_A}\n"),
    );
    // `inference.models.pull` declares MEMBER_A; drop it there so the only
    // route onto a command is the set.
    scratch.replace_in(
        "commands/inference.yaml",
        &format!("      - {MEMBER_A}\n"),
        "",
    );
    scratch.add_sets(&pair_set("probe.pair"));
    scratch.set_probe_errors(&["set:probe.pair"]);

    let message = scratch.rejection();
    assert!(
        message.contains(MEMBER_A) && message.contains("probe.pair"),
        "names the undeclared code and the set: {message}"
    );
}
