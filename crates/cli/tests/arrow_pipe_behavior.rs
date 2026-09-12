//! CLI arrow + cross-cutting behavior (TCP3.11c).
//!
//! Ports the arrow/pipe/raw/error workflows from the shell scenario corpus
//! (`scripts/cli-corpus/05`/`06`) into real-binary integration tests. None of
//! these paths had Rust integration coverage, yet the corpus that first
//! exercised them pinned two contracts worth guarding: arrow graph export
//! splits its requested stem into concrete `_nodes`/`_edges` files (the stem
//! itself is never written), and pipe mode runs a newline-delimited command
//! stream against one process, skipping `#` comments. Structured error
//! envelopes (`class`/`code`/`retry_policy`/`retryable`) are pinned too.

#![deny(unsafe_code)]

use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use serde_json::Value;
use tempfile::TempDir;

/// Runs `strata --db <db> --json <args>`, asserting success, returning stdout JSON.
fn strata(db: &Path, args: &[&str]) -> Value {
    let output = run(db, "--json", args);
    assert!(
        output.status.success(),
        "command {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout is JSON")
}

fn run(db: &Path, mode: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strata"))
        .arg("--db")
        .arg(db)
        .arg(mode)
        .args(args)
        .env_remove("STRATA_DB")
        .output()
        .expect("run strata binary")
}

fn db(home: &TempDir) -> std::path::PathBuf {
    home.path().join("db")
}

// --- raw command print/run + pipe mode ------------------------------------

#[test]
fn command_print_echoes_and_run_executes() {
    let home = tempfile::tempdir().expect("temp home");
    let cmd = home.path().join("ping.json");
    std::fs::write(&cmd, r#"{"type":"ping"}"#).expect("write command");

    // `command print` validates and echoes the serialized command without a db.
    let printed = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["--json", "command", "print", "--file"])
        .arg(&cmd)
        .output()
        .expect("run strata binary");
    assert!(printed.status.success());
    let printed: Value = serde_json::from_slice(&printed.stdout).expect("json");
    assert_eq!(printed["type"], "ping");

    // `command run` dispatches it through the executor: ping -> pong.
    let ran = strata(
        &db(&home),
        &["command", "run", "--file", cmd.to_str().unwrap()],
    );
    assert_eq!(ran["type"], "pong");
}

#[test]
fn pipe_mode_runs_a_command_stream_and_skips_comments() {
    let home = tempfile::tempdir().expect("temp home");
    let db = db(&home);
    let script =
        "# comments are skipped\nkv put pipe-a A\nkv put pipe-b B\nkv get pipe-a\nkv get pipe-b\n";

    let mut child = Command::new(env!("CARGO_BIN_EXE_strata"))
        .arg("--db")
        .arg(&db)
        .arg("--raw")
        .env_remove("STRATA_DB")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn strata");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(script.as_bytes())
        .expect("write script");
    let out = child.wait_with_output().expect("wait");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Every command answers on stdout: a raw put prints the identity it wrote,
    // a raw get prints the value; the comment is skipped. A raw read carries
    // no newline of its own (#3116), so the stream — a transcript, not a file
    // — supplies the separator between answers.
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "pipe-a\npipe-b\nA\nB"
    );
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn cache_mode_is_process_local() {
    // A one-shot cache invocation starts from an empty database every time.
    let out = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["--cache", "--raw", "kv", "get", "transient"])
        .env_remove("STRATA_DB")
        .output()
        .expect("run strata binary");
    assert!(out.status.success());
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "");
}

// --- arrow export / import ------------------------------------------------

fn export_result(v: &Value) -> (&Value, u64, usize) {
    assert_eq!(v["type"], "arrow_export_result");
    let data = &v["data"];
    let rows = data["row_count"].as_u64().expect("row_count");
    let paths = data["paths"].as_array().expect("paths").len();
    (data, rows, paths)
}

#[test]
fn arrow_round_trips_kv_through_a_file() {
    let home = tempfile::tempdir().expect("temp home");
    let db = db(&home);
    strata(&db, &["kv", "put", "export-kv", "export-value"]);

    let dest = home.path().join("export-kv.jsonl");
    let exported = strata(
        &db,
        &[
            "arrow",
            "export",
            "--primitive",
            "kv",
            "--format",
            "jsonl",
            dest.to_str().unwrap(),
            "--limit",
            "10",
        ],
    );
    let (_, rows, paths) = export_result(&exported);
    assert!(rows >= 1);
    assert_eq!(paths, 1);
    assert!(std::fs::metadata(&dest).expect("export file").len() > 0);

    // Import a fresh CSV and read one imported row back through kv.
    let src = home.path().join("import-kv.csv");
    std::fs::write(&src, "key,value\nimported-a,alpha\nimported-b,beta\n").expect("write csv");
    let imported = strata(
        &db,
        &[
            "arrow",
            "import",
            src.to_str().unwrap(),
            "--format",
            "csv",
            "--target",
            "kv",
            "--key-column",
            "key",
            "--value-column",
            "value",
        ],
    );
    assert_eq!(imported["type"], "arrow_import_result");
    assert_eq!(imported["data"]["rows_imported"], 2);
    assert_eq!(imported["data"]["rows_skipped"], 0);

    let got = strata(&db, &["kv", "get", "imported-b"]);
    // kv values render as base64 in JSON mode; "YmV0YQ==" == "beta".
    assert_eq!(got["data"]["found"], true);
    assert_eq!(got["data"]["value"]["value"], "YmV0YQ==");
}

#[test]
fn arrow_graph_export_splits_the_stem_into_node_and_edge_files() {
    let home = tempfile::tempdir().expect("temp home");
    let db = db(&home);
    strata(&db, &["graph", "create", "export-graph"]);
    strata(&db, &["graph", "add-node", "export-graph", "n1"]);
    strata(&db, &["graph", "add-node", "export-graph", "n2"]);
    strata(
        &db,
        &[
            "graph",
            "add-edge",
            "export-graph",
            "n1",
            "relates",
            "n2",
            "--weight",
            "1.0",
        ],
    );

    let stem = home.path().join("export-graph.jsonl");
    let exported = strata(
        &db,
        &[
            "arrow",
            "export",
            "--primitive",
            "graph",
            "--format",
            "jsonl",
            stem.to_str().unwrap(),
            "--graph",
            "export-graph",
            "--limit",
            "10",
        ],
    );
    let (data, rows, path_count) = export_result(&exported);
    assert!(rows >= 2);
    assert_eq!(path_count, 2);

    let paths: Vec<&str> = data["paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    // The requested stem is never a reported path, and is never written.
    let stem_str = stem.to_str().unwrap();
    assert!(
        !paths.contains(&stem_str),
        "stem leaked into paths: {paths:?}"
    );
    assert!(!stem.exists(), "requested stem was consumed as a data file");
    // Concrete node/edge files are reported and non-empty.
    assert!(paths[0].ends_with("_nodes.jsonl"), "{paths:?}");
    assert!(paths[1].ends_with("_edges.jsonl"), "{paths:?}");
    for p in &paths {
        assert!(
            std::fs::metadata(p).expect("export path").len() > 0,
            "empty {p}"
        );
    }
}

#[test]
fn arrow_export_rejects_an_unknown_primitive() {
    let home = tempfile::tempdir().expect("temp home");
    let dest = home.path().join("bogus.jsonl");
    // clap value-parses --primitive against a fixed set; "bogus" is refused.
    let out = run(
        &db(&home),
        "--json",
        &[
            "arrow",
            "export",
            "--primitive",
            "bogus",
            "--format",
            "jsonl",
            dest.to_str().unwrap(),
        ],
    );
    assert!(!out.status.success());
    assert!(!dest.exists());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--primitive") && stderr.contains("bogus"),
        "{stderr}"
    );
}

// --- structured error envelopes -------------------------------------------

/// Runs a command expected to fail, returning the parsed `error` object.
/// Structured error envelopes are written to stderr, keeping stdout clean for
/// pipeable success output.
fn expect_error(db: &Path, args: &[&str]) -> Value {
    let out = run(db, "--json", args);
    assert!(!out.status.success(), "expected failure for {args:?}");
    assert!(
        out.stdout.is_empty(),
        "error left output on stdout: {:?}",
        out.stdout
    );
    let v: Value = serde_json::from_slice(&out.stderr).expect("error is JSON on stderr");
    v["error"].clone()
}

#[test]
fn missing_branch_renders_a_never_retryable_not_found_envelope() {
    let home = tempfile::tempdir().expect("temp home");
    let err = expect_error(
        &db(&home),
        &["--branch", "missing", "kv", "get", "anything"],
    );
    assert!(
        err["code"].as_str().unwrap().starts_with("not_found."),
        "{err}"
    );
    assert_eq!(err["class"], "not_found");
    assert_eq!(err["retry_policy"], "never");
    assert_eq!(err["retryable"], false);
}

#[test]
fn invalid_vector_dimension_renders_an_invalid_argument_envelope() {
    let home = tempfile::tempdir().expect("temp home");
    let db = db(&home);
    strata(
        &db,
        &[
            "vector",
            "collection",
            "create",
            "vc",
            "2",
            "--metric",
            "cosine",
        ],
    );
    let err = expect_error(&db, &["vector", "upsert", "vc", "bad", "1.0"]);
    assert!(
        err["code"]
            .as_str()
            .unwrap()
            .starts_with("invalid_argument."),
        "{err}"
    );
    assert_eq!(err["class"], "invalid_argument");
    assert_eq!(err["retry_policy"], "never");
    assert_eq!(err["retryable"], false);
}
