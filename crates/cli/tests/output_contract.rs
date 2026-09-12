//! The CLI output-contract matrix (`docs/design/cli-output-contract.md` §5,
//! tracker #3314). Every command × fixture × format, rendered through the
//! same functions the binary and the playground call, pinned byte-for-byte
//! under `tests/output-contract/<family>.txt`; plus a fixed script of real
//! binary exchanges pinned with stdout, stderr and exit code separately
//! (`binary.txt`) — the only place the channel contract (§4 R5) is
//! observable.
//!
//! S0 blesses **today's output, unchanged**. Nothing here asserts that a cell
//! is right; the contract (§4) is the aspiration and each slice S1–S4 moves
//! the cells §5.4 predicts for it, carrying the snapshot diff as its review
//! surface. A cell is red only when a change moved output it did not mean to.
//! Bless with `STRATA_OUTPUT_BLESS=1 cargo test -p strata-cli --test
//! output_contract` and review the diff.
//!
//! Dimensions (§5.1), derived — never a literal count:
//! - command: every entry of `generated/command-index.json`;
//! - fixture: the command's `fixtures.response` and alternates, every
//!   `error_cases[].expected_error`, and synthetic edges derived from the
//!   primary response wherever its shape admits one (`committed_at` of
//!   `null` and of a fixed instant, `applied: false`, an empty `items`
//!   page, a non-UTF-8 KV value);
//! - format: `human` and `raw` are pinned; `json` and `pretty` are asserted
//!   as invariants instead of data — each must be the unmodified wire record
//!   (compact on one line, or pretty-printed), which is the whole of their
//!   contract and the reason they never move in any slice.
//!
//! The in-process cells run in a child process with `TZ=UTC` and a scrubbed
//! environment, so the local-date formatter and every environment-sensitive
//! command render identically on every machine (the harness shape of
//! `crates/inference/tests/resolution_matrix.rs`). The `command-examples.json`
//! cross-check closes the loop with the docs bundle: every reproducible
//! example line's `out` must equal this matrix's human render of the same
//! wire, so the two artifacts cannot drift apart. The playground cells run
//! the binary's script through `strata_cli::run_line` — the browser's whole
//! path — and hold it equal to the binary's stdout ⧺ stderr (#3312).

#![deny(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use strata_cli::{error_to_string, render_output, Format, Invocation};
use strata_executor::{Executor, Output};

const CHILD_MODE: &str = "STRATA_OUTPUT_CONTRACT_CHILD";
const BLESS: &str = "STRATA_OUTPUT_BLESS";
/// 2026-09-10 20:19:44.000000 UTC, the exemplar instant of §10 Q3, in micros.
const FIXED_INSTANT_MICROS: u64 = 1_789_071_584_000_000;
/// base64 of the single byte `0xFF` — a KV value that is not text.
const NON_UTF8_BYTES: &str = "/w==";
/// The cursor a synthetic `has_more` page carries: base64 of `next` for the
/// opaque-cursor pages, a sequence number for event pages.
const FIXED_CURSOR: &str = "bmV4dA==";
const FIXED_CURSOR_SEQ: u64 = 7;
/// The population a synthetic sample edge reports: larger than any sample
/// fixture's item count, so the sample notice fires (§3 `SamplePage`, N < M).
const SAMPLED_POPULATION: u64 = 10;
/// Cell terminator in a snapshot file: the rendered text is everything between
/// the heading line and the first `∎` line, so an empty render, a render with
/// no trailing newline, and one with are all distinguishable.
const END: &str = "∎\n";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn snapshot_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/output-contract")
}

fn bless_mode() -> bool {
    std::env::var_os(BLESS).is_some()
}

// ---------------------------------------------------------------------------
// Snapshot files: `### <cell key>` heading, verbatim text, `∎` terminator.
// ---------------------------------------------------------------------------

type Cells = BTreeMap<String, String>;

fn encode(cells: &Cells) -> String {
    let mut text = String::new();
    for (key, rendered) in cells {
        assert!(
            !rendered.contains('∎'),
            "cell {key} contains the terminator glyph; pick another"
        );
        // `fmt::Write` for `String` is infallible.
        write!(text, "### {key}\n{rendered}{END}").expect("String never fails to write");
    }
    text
}

fn decode(text: &str) -> Cells {
    let mut cells = Cells::new();
    let mut rest = text;
    while let Some(start) = rest.strip_prefix("### ") {
        let (key, body) = start.split_once('\n').expect("heading line ends");
        let (rendered, tail) = body.split_once(END).expect("cell is terminated");
        assert!(
            cells.insert(key.to_owned(), rendered.to_owned()).is_none(),
            "duplicate cell {key}"
        );
        rest = tail;
    }
    assert!(
        rest.is_empty(),
        "trailing bytes after the last cell: {rest:?}"
    );
    cells
}

/// Compares `actual` with the committed snapshot for `name`, returning one
/// line per red cell (missing, extra, or different). In bless mode writes the
/// snapshot and returns nothing.
fn compare(name: &str, actual: &Cells) -> Vec<String> {
    let path = snapshot_dir().join(format!("{name}.txt"));
    if bless_mode() {
        std::fs::create_dir_all(snapshot_dir()).expect("snapshot dir");
        std::fs::write(&path, encode(actual)).expect("write snapshot");
        return Vec::new();
    }
    let committed = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{}: {error}; bless with {BLESS}=1", path.display()));
    let expected = decode(&committed);
    let mut red = Vec::new();
    for (key, want) in &expected {
        match actual.get(key) {
            None => red.push(format!("{name}: missing cell {key}")),
            Some(got) if got != want => {
                red.push(format!(
                    "{name}: {key}\n--- expected\n{want}--- actual\n{got}"
                ));
            }
            Some(_) => {}
        }
    }
    for key in actual.keys() {
        if !expected.contains_key(key) {
            red.push(format!("{name}: unblessed cell {key}\n{}", actual[key]));
        }
    }
    red
}

fn report(what: &str, red: &[String]) {
    if red.is_empty() {
        return;
    }
    // The full diff goes to stdout (visible under `--nocapture`); the panic
    // message lists the red cells so a plant's blast radius is one screen.
    for line in red {
        println!("{line}");
    }
    let keys: Vec<&str> = red
        .iter()
        .map(|line| line.lines().next().unwrap_or_default())
        .collect();
    panic!(
        "{what}: {} red cell(s); bless with {BLESS}=1 if the change is intended\n  {}",
        red.len(),
        keys.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// In-process cells.
// ---------------------------------------------------------------------------

struct Wire {
    command: String,
    /// The command's wire name (`kv_put`), which names its `display:`
    /// declaration in the embedded catalog.
    name: String,
    /// The command's request fixture — a declared receipt may quote it.
    request: Value,
    fixture: String,
    value: Value,
    is_error: bool,
    /// Whether the binary renders this wire through the command's `display:`
    /// declaration. A command's own response does; a second output it can
    /// emit — `clone`'s progress events — is streamed with no declaration
    /// (`run_clone`), so rendering it as the command's answer would pin a
    /// receipt no reader ever sees.
    declared: bool,
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn typed(command: &str, fixture: &str, wire: &Value) -> Output {
    serde_json::from_value(wire.clone()).unwrap_or_else(|error| {
        panic!("{command} · {fixture}: wire does not deserialize into Output: {error}")
    })
}

/// Every wire the command contributes: its response fixtures, the synthetic
/// edges its primary response admits, and its error fixtures.
fn wires_for(command: &Value, fixtures_root: &Path) -> Vec<Wire> {
    let id = command["id"].as_str().expect("command id").to_owned();
    let wire_name = command["wire"].as_str().expect("wire name").to_owned();
    let fixtures = &command["fixtures"];
    let request = read_json(&fixtures_root.join(fixtures["request"].as_str().expect("request")));
    let mut wires = Vec::new();
    let primary = fixtures["response"].as_str().expect("response fixture");
    let primary_tag = read_json(&fixtures_root.join(primary))["type"]
        .as_str()
        .expect("response fixture names its wire type")
        .to_owned();
    let mut responses = vec![primary];
    responses.extend(
        fixtures["responses"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|alternate| alternate.as_str().expect("alternate fixture")),
    );
    for fixture in responses {
        let value = read_json(&fixtures_root.join(fixture));
        let declared = value["type"].as_str() == Some(primary_tag.as_str());
        wires.push(Wire {
            command: id.clone(),
            name: wire_name.clone(),
            request: request.clone(),
            fixture: fixture.to_owned(),
            value,
            is_error: false,
            declared,
        });
    }
    let family = command["family"].as_str().expect("family");
    for (label, value) in edges(family, &wires[0].value) {
        wires.push(Wire {
            command: id.clone(),
            name: wire_name.clone(),
            request: request.clone(),
            fixture: label,
            value,
            is_error: false,
            declared: true,
        });
    }
    for case in fixtures["error_cases"].as_array().into_iter().flatten() {
        let fixture = case["expected_error"].as_str().expect("error fixture");
        wires.push(Wire {
            command: id.clone(),
            name: wire_name.clone(),
            request: request.clone(),
            fixture: fixture.to_owned(),
            value: read_json(&fixtures_root.join(fixture)),
            is_error: true,
            declared: false,
        });
    }
    wires
}

/// Whether any `committed_at` key appears anywhere in the wire (the checked-in
/// fixtures are replay captures, so today every one of them carries `null`).
fn has_committed_at(value: &Value, numeric_only: bool) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, child)| {
            (key == "committed_at" && (child.is_number() || (!numeric_only && child.is_null())))
                || has_committed_at(child, numeric_only)
        }),
        Value::Array(items) => items
            .iter()
            .any(|item| has_committed_at(item, numeric_only)),
        _ => false,
    }
}

fn set_committed_at(value: &mut Value, replacement: &Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "committed_at" && (child.is_number() || child.is_null()) {
                    *child = replacement.clone();
                } else {
                    set_committed_at(child, replacement);
                }
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| set_committed_at(item, replacement)),
        _ => {}
    }
}

/// Replaces every string under a `value` key with `replacement`; returns how
/// many were replaced. KV payloads travel as base64 and this is how the matrix
/// reaches the non-UTF-8 shape the fixtures never carry (#3116).
fn set_values(value: &mut Value, replacement: &str) -> usize {
    match value {
        Value::Object(map) => map
            .iter_mut()
            .map(|(key, child)| {
                if key == "value" && child.is_string() {
                    *child = Value::from(replacement);
                    1
                } else {
                    set_values(child, replacement)
                }
            })
            .sum(),
        Value::Array(items) => items
            .iter_mut()
            .map(|item| set_values(item, replacement))
            .sum(),
        _ => 0,
    }
}

/// Synthetic edge fixtures (§5.1) derived from the primary response wherever
/// its shape admits one. Each edge is a shape the contract makes a rule about
/// and that the checked-in fixtures do not all exercise.
fn edges(family: &str, primary: &Value) -> Vec<(String, Value)> {
    let mut edges = Vec::new();
    if has_committed_at(primary, true) {
        let mut null = primary.clone();
        set_committed_at(&mut null, &Value::Null);
        edges.push(("edge:committed_at=null".to_owned(), null));
    }
    if has_committed_at(primary, false) {
        let mut fixed = primary.clone();
        set_committed_at(&mut fixed, &Value::from(FIXED_INSTANT_MICROS));
        edges.push(("edge:committed_at=fixed".to_owned(), fixed));
    }
    if primary["data"]["effect"]["applied"] == Value::Bool(true) {
        let mut missed = primary.clone();
        missed["data"]["effect"] = serde_json::json!({
            "affected_count": 0, "applied": false, "kind": "not_found", "matched": false
        });
        edges.push(("edge:applied=false".to_owned(), missed));
    }
    if primary["data"]["found"] == Value::Bool(true) {
        let mut miss = primary.clone();
        miss["data"]["found"] = Value::Bool(false);
        miss["data"]["value"] = Value::Null;
        edges.push(("edge:found=false".to_owned(), miss));
    }
    let has_items = primary["data"]["items"]
        .as_array()
        .is_some_and(|items| !items.is_empty());
    if has_items && primary["data"]["has_more"] == Value::Bool(false) {
        // Cursors are opaque base64 on most pages and a sequence number on
        // event pages; the wire says which only by what deserializes.
        let more = [Value::from(FIXED_CURSOR), Value::from(FIXED_CURSOR_SEQ)]
            .into_iter()
            .map(|cursor| {
                let mut more = primary.clone();
                more["data"]["has_more"] = Value::Bool(true);
                let data = more["data"].as_object_mut().expect("data object");
                for key in ["cursor", "next_cursor"] {
                    if data.contains_key(key) {
                        data.insert(key.to_owned(), cursor.clone());
                    }
                }
                more
            })
            .find(|more| serde_json::from_value::<Output>(more.clone()).is_ok())
            .expect("a has_more page with one of the two cursor shapes deserializes");
        edges.push(("edge:has_more=true".to_owned(), more));
    }
    if has_items
        && primary["data"]["total_count"]
            .as_u64()
            .is_some_and(|total| total < SAMPLED_POPULATION)
    {
        let mut sampled = primary.clone();
        sampled["data"]["total_count"] = Value::from(SAMPLED_POPULATION);
        edges.push(("edge:total_count=more".to_owned(), sampled));
    }
    if has_items {
        let mut empty = primary.clone();
        let data = empty["data"].as_object_mut().expect("data object");
        data.insert("items".to_owned(), Value::Array(Vec::new()));
        for (key, cleared) in [
            ("has_more", Value::Bool(false)),
            ("cursor", Value::Null),
            ("next_cursor", Value::Null),
            ("total_count", Value::from(0)),
        ] {
            if data.contains_key(key) {
                data.insert(key.to_owned(), cleared);
            }
        }
        edges.push(("edge:items=[]".to_owned(), empty));
    }
    if family == "kv" {
        let mut binary = primary.clone();
        if set_values(&mut binary["data"], NON_UTF8_BYTES) > 0 {
            edges.push(("edge:value=non-utf8".to_owned(), binary));
        }
    }
    edges
}

/// Renders one wire in every format: `human` and `raw` become cells (their
/// stdout, plus a `· stderr` cell whenever the format writes feedback);
/// `json` and `pretty` are checked against the wire itself and must write
/// nothing to stderr (R5).
fn render_wire(wire: &Wire, cells: &mut Cells, invariants: &mut Vec<String>) {
    let key = |format: &str| format!("{} · {} · {format}", wire.command, wire.fixture);
    if wire.is_error {
        let render = |format| error_to_string(&wire.value, format);
        cells.insert(key("human"), render(Format::Human));
        cells.insert(key("raw"), render(Format::Raw));
        let expected = serde_json::json!({ "error": wire.value });
        check_envelopes(
            &key,
            &render(Format::Json),
            &render(Format::Pretty),
            &expected,
            invariants,
        );
        return;
    }
    let output = typed(&wire.command, &wire.fixture, &wire.value);
    let render = |format| {
        let invocation = if wire.declared {
            Invocation::for_wire(&wire.name, format, || Ok(wire.request.clone()))
                .unwrap_or_else(|error| panic!("{}: {error}", key("declaration")))
        } else {
            Invocation::none()
        };
        render_output(&output, &invocation, format)
            .unwrap_or_else(|error| panic!("{}: {error}", key("render")))
    };
    // A second output a command can emit — `clone`'s progress events — never
    // reaches a reader: the binary streams it under `--json` and nothing else
    // (`run_clone`, and `--progress jsonl` refuses any other format). Only its
    // envelope is pinned, because only its envelope exists.
    if wire.declared {
        for (name, format) in [("human", Format::Human), ("raw", Format::Raw)] {
            let rendered = render(format);
            cells.insert(key(name), rendered.stdout);
            if !rendered.stderr.is_empty() {
                cells.insert(key(&format!("{name} · stderr")), rendered.stderr);
            }
        }
    }
    let json = render(Format::Json);
    let pretty = render(Format::Pretty);
    for (name, rendered) in [("json", &json), ("pretty", &pretty)] {
        if !rendered.stderr.is_empty() {
            invariants.push(format!("{}: wrote to stderr", key(name)));
        }
    }
    // The binary newline-terminates an envelope; the invariant is about the
    // envelope itself.
    let envelope = |rendered: &strata_cli::Rendered| {
        rendered
            .stdout
            .strip_suffix('\n')
            .unwrap_or_else(|| panic!("{}: not newline-terminated", key("json")))
            .to_owned()
    };
    check_envelopes(
        &key,
        &envelope(&json),
        &envelope(&pretty),
        &serde_json::to_value(&output).expect("output serializes"),
        invariants,
    );
}

/// `json` and `pretty` are the unmodified wire record, compact on one line or
/// pretty-printed — the whole of their contract.
fn check_envelopes(
    key: &dyn Fn(&str) -> String,
    json: &str,
    pretty: &str,
    expected: &Value,
    invariants: &mut Vec<String>,
) {
    let parsed: Value = serde_json::from_str(json).expect("json cell parses");
    if &parsed != expected || json.contains('\n') {
        invariants.push(format!("{}: not the compact wire record", key("json")));
    }
    if pretty != serde_json::to_string_pretty(expected).expect("wire pretty-prints") {
        invariants.push(format!(
            "{}: not the pretty-printed wire record",
            key("pretty")
        ));
    }
}

fn in_process_cells(root: &Path) -> (BTreeMap<String, Cells>, Vec<String>) {
    let index = read_json(&root.join("crates/executor/idl/v1/generated/command-index.json"));
    let fixtures_root = root.join("crates/executor/tests/fixtures");
    let mut by_family: BTreeMap<String, Cells> = BTreeMap::new();
    let mut invariants = Vec::new();
    let mut wires = 0usize;
    for command in index["commands"].as_array().expect("commands") {
        let family = command["family"].as_str().expect("family").to_owned();
        let cells = by_family.entry(family).or_default();
        for wire in wires_for(command, &fixtures_root) {
            render_wire(&wire, cells, &mut invariants);
            wires += 1;
        }
    }
    println!(
        "output contract: {} commands, {wires} wires, {} pinned cells across {} families",
        index["commands"].as_array().map_or(0, Vec::len),
        by_family.values().map(Cells::len).sum::<usize>(),
        by_family.len()
    );
    (by_family, invariants)
}

/// `command-examples.json` must agree with this matrix: every reproducible
/// example line's `out` is the human render of the step's wire — stdout then
/// stderr, trimmed of the trailing newline, as the artifact stores it. A
/// non-reproducible line carries a masked shape, so only its presence is
/// checked.
fn examples_cross_check(root: &Path) -> Vec<String> {
    let committed = read_json(&root.join("crates/executor/idl/v1/generated/command-examples.json"));
    let runs = strata_executor::idl_tooling::capture_examples(root).expect("capture examples");
    let mut red = Vec::new();
    let mut checked = 0usize;
    for run in &runs {
        let lines = committed["commands"][&run.command_id]
            .as_array()
            .unwrap_or_else(|| panic!("{}: no committed example lines", run.command_id));
        if lines.len() != run.steps.len() {
            red.push(format!(
                "{}: {} committed lines, {} replayed steps",
                run.command_id,
                lines.len(),
                run.steps.len()
            ));
            continue;
        }
        for (position, (line, step)) in lines.iter().zip(&run.steps).enumerate() {
            if line["reproducible"] != Value::Bool(true) {
                continue;
            }
            let output = typed(&run.command_id, &step.cli_input, &step.wire_output);
            let invocation =
                Invocation::for_wire(&step.wire, Format::Human, || Ok(step.request.clone()))
                    .expect("declaration");
            let human = render_output(&output, &invocation, Format::Human)
                .expect("render")
                .stdout_then_stderr();
            let human = human.trim_end_matches('\n');
            if human != line["out"].as_str().expect("out") {
                red.push(format!(
                    "{} step {position} ({}): command-examples.json `out` differs from the matrix \
                     human cell\n--- command-examples.json\n{}\n--- matrix\n{human}\n",
                    run.command_id,
                    step.cli_input,
                    line["out"].as_str().unwrap_or_default()
                ));
            }
            checked += 1;
        }
    }
    println!(
        "output contract: {} example runs, {checked} reproducible lines cross-checked",
        runs.len()
    );
    red
}

/// The child: every in-process cell, under `TZ=UTC` in a scrubbed environment.
#[test]
#[ignore = "subprocess phase: re-invoked by `in_process_cells_hold`"]
fn in_process_child() {
    if std::env::var_os(CHILD_MODE).is_none() {
        return;
    }
    assert_eq!(
        std::env::var("TZ").as_deref(),
        Ok("UTC"),
        "the child renders local dates under UTC"
    );
    let root = repo_root();
    let (by_family, invariants) = in_process_cells(&root);
    let mut red = Vec::new();
    for (family, cells) in &by_family {
        red.extend(compare(family, cells));
    }
    report("in-process cells", &red);
    report("json/pretty invariants", &invariants);
    report(
        "command-examples.json cross-check",
        &examples_cross_check(&root),
    );
}

/// The parent: builds the scrubbed environment and runs the child.
#[test]
fn in_process_cells_hold() {
    let scrub = tempfile::tempdir().expect("scrub dir");
    let home = scrub.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let exe = std::env::current_exe().expect("current test binary");
    let mut command = Command::new(exe);
    command
        .args(["in_process_child", "--exact", "--ignored", "--nocapture"])
        .env_clear()
        .env(CHILD_MODE, "1")
        .env("TZ", "UTC")
        .env("HOME", &home);
    // Only what the child needs to run at all crosses over: nothing from the
    // developer's environment may reach a cell.
    for key in ["PATH", "TMPDIR", BLESS] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    let output = command.output().expect("spawn matrix child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "output-contract child failed:\n{stdout}\n{stderr}"
    );
    for line in stdout
        .lines()
        .filter(|line| line.starts_with("output contract:"))
    {
        println!("{line}");
    }
}

// ---------------------------------------------------------------------------
// Binary cells: the channel contract, observed through the real binary.
// ---------------------------------------------------------------------------

/// One exchange of the script: a label, the arguments after `--db <path>`.
/// The script is a sequence — later exchanges see the state earlier ones
/// left — so each format sees each situation once, and the mutating steps
/// rotate through the formats.
const SCRIPT: &[&[&str]] = &[
    &["kv", "put", "greeting", "hello"],
    &["--raw", "kv", "put", "greeting", "hello2"],
    &["--json", "kv", "put", "greeting", "hello3"],
    &["kv", "get", "greeting"],
    &["--raw", "kv", "get", "greeting"],
    &["--json", "kv", "get", "greeting"],
    &["kv", "get", "missing"],
    &["--raw", "kv", "get", "missing"],
    &["--json", "kv", "get", "missing"],
    &["kv", "delete", "missing"],
    &["--raw", "kv", "delete", "missing"],
    &["--json", "kv", "delete", "missing"],
    &["kv", "delete", "greeting"],
    &["kv", "history", "greeting"],
    &["--raw", "kv", "history", "greeting"],
    &["--json", "kv", "history", "greeting"],
    &["kv", "list"],
    &["--raw", "kv", "list"],
    &["--json", "kv", "list"],
    &["kv", "put", "b", "2"],
    &["kv", "put", "c", "3"],
    &["kv", "list", "--limit", "1"],
    &["--raw", "kv", "list", "--limit", "1"],
    &["--json", "kv", "list", "--limit", "1"],
    &["branch", "get", "nope"],
    &["--raw", "branch", "get", "nope"],
    &["--json", "branch", "get", "nope"],
    &["kv", "put"],
    &["--json", "kv", "put"],
    &["branch", "create", "exp"],
    &["--raw", "branch", "create", "exp2"],
    &["--json", "branch", "create", "exp3"],
    &["branch", "create", "exp"],
    &["info"],
    &["--raw", "info"],
];

/// Replaces the values that vary run to run or machine to machine with
/// placeholders, so the pinned text is the shape of the channel, not the
/// instant it was captured.
fn scrub(text: &str) -> String {
    let mut out = text.to_owned();
    for (pattern, placeholder) in [
        // A rendered local date-time (the binary runs under TZ=UTC; the
        // playground cell renders in the host's zone), a declared table's
        // UTC-second cell (Q3), and epoch micros — keyed in an envelope, a
        // bare 16-digit cell in a raw row (2001 through 2286).
        (
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [+-]\d{2}:\d{2}",
            "<instant>",
        ),
        (
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6} UTC",
            "<instant>",
        ),
        (r#"("committed_at":\s*)\d+"#, "${1}<micros>"),
        (r"\b\d{16}\b", "<micros>"),
        (r"err_local_[0-9a-f]+_[0-9]+", "<reference_id>"),
        (
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            "<uuid>",
        ),
        (
            r#"("(?:total_bytes|usable_host_bytes)":\s*)\d+"#,
            "${1}<bytes>",
        ),
        (r#"("version":\s*)"\d+\.\d+\.\d+""#, r#"${1}"<semver>""#),
        // The same two facts in a declared record: the reader's humanised
        // size under its `header:`, and the script's wire number under its
        // dotted key. Both are this host's memory, not the contract.
        (
            r"(?m)^(\s*(?:total|usable_host)\s+)[\d.]+ (?:bytes|[kMG]B)$",
            "${1}<bytes>",
        ),
        (
            "(?m)^(memory_budget\\.(?:total_bytes|usable_host_bytes)\t)\\d+$",
            "${1}<bytes>",
        ),
        // The binary's own version, wherever a record shows it.
        (r"(?m)^(version[ \t]+)\d+\.\d+\.\d+$", "${1}<semver>"),
    ] {
        out = regex::Regex::new(pattern)
            .expect("scrub pattern compiles")
            .replace_all(&out, placeholder)
            .into_owned();
    }
    out
}

/// The cell key of one exchange's channel: `NN strata <args> · <channel>`.
fn exchange_key(position: usize, args: &[&str], channel: &str) -> String {
    format!("{position:02} strata {} · {channel}", args.join(" "))
}

/// Runs the script through the real binary against a fresh durable database
/// under `dir`, one process per exchange, and returns every channel scrubbed.
fn binary_cells(dir: &Path) -> Cells {
    let home = dir.join("home");
    std::fs::create_dir_all(&home).expect("home");
    let db = dir.join("db").to_string_lossy().into_owned();
    let channel = |bytes: &[u8]| scrub(&String::from_utf8_lossy(bytes).replace(&db, "<db>"));
    let mut cells = Cells::new();
    for (position, args) in SCRIPT.iter().enumerate() {
        let mut command = Command::new(env!("CARGO_BIN_EXE_strata"));
        command
            .arg("--db")
            .arg(&db)
            .args(*args)
            .env_clear()
            .env("TZ", "UTC")
            .env("HOME", &home);
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        let output = command.output().expect("run strata binary");
        cells.insert(
            exchange_key(position, args, "stdout"),
            channel(&output.stdout),
        );
        cells.insert(
            exchange_key(position, args, "stderr"),
            channel(&output.stderr),
        );
        cells.insert(
            exchange_key(position, args, "exit"),
            format!("{}\n", output.status.code().expect("exit code")),
        );
    }
    cells
}

#[test]
fn binary_cells_hold() {
    let dir = tempfile::tempdir().expect("tmp");
    report(
        "binary cells",
        &compare("binary", &binary_cells(dir.path())),
    );
}

// ---------------------------------------------------------------------------
// Playground cells: the browser's one stream equals the binary's two.
// ---------------------------------------------------------------------------

/// The two surfaces run on different targets — the binary on a durable file,
/// the playground on a cache session — and a target may say so: `info`
/// describes it outright (that exchange is skipped), and a commit ack names
/// its durability (`standard` on the file, `not_durable` in cache; mapped
/// here). Nothing else legitimately differs between the two.
fn describes_the_target(args: &[&str]) -> bool {
    args.contains(&"info")
}

fn on_the_cache_target(binary_text: &str) -> String {
    binary_text.replace(
        r#""durability":"standard""#,
        r#""durability":"not_durable""#,
    )
}

/// §5.2 playground cell (#3312): for every exchange of the script, `run_line`
/// — the playground's whole path, parse through render — returns stdout ⧺
/// stderr of the binary's exchange, in the format the line chose. The two
/// surfaces run the same script against their own database, so the state each
/// exchange sees is the same on both sides.
#[test]
fn playground_cells_match_the_binary() {
    let dir = tempfile::tempdir().expect("tmp");
    let binary = binary_cells(dir.path());
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let mut red = Vec::new();
    for (position, args) in SCRIPT.iter().enumerate() {
        let line = args.join(" ");
        let got = scrub(&strata_cli::run_line(&mut executor, &line).expect("run_line renders"));
        if describes_the_target(args) {
            continue;
        }
        let want = on_the_cache_target(&format!(
            "{}{}",
            binary[&exchange_key(position, args, "stdout")],
            binary[&exchange_key(position, args, "stderr")]
        ));
        if got != want {
            red.push(format!(
                "playground: {position:02} strata {line}\n--- binary stdout ⧺ stderr\n{want}--- playground\n{got}"
            ));
        }
    }
    report("playground cells", &red);
}

#[test]
fn snapshot_encoding_round_trips_every_shape() {
    let mut cells = Cells::new();
    cells.insert("a · empty".to_owned(), String::new());
    cells.insert("b · newline".to_owned(), "line\n".to_owned());
    cells.insert("c · bare".to_owned(), "line".to_owned());
    cells.insert("d · multi".to_owned(), "one\n\ntwo\n".to_owned());
    cells.insert("e · hashes".to_owned(), "### not a heading\n".to_owned());
    assert_eq!(decode(&encode(&cells)), cells);
}
