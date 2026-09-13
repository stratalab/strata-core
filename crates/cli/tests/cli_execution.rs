//! CLI integration suite: real `strata` binary invocations, each command in
//! its own process, against real cache and durable-local databases. This is
//! the cross-process execution coverage the CLI test plans call for — the
//! durability claims below only mean something because every step is a
//! separate OS process.

#![deny(unsafe_code)]

use std::path::Path;
use std::process::{Command, Output};

fn strata(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .env_remove("STRATA_DB")
        .output()
        .expect("run strata binary")
}

fn strata_env(args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_strata"));
    command.args(args).env_remove("STRATA_DB");
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("run strata binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_ok(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed (status {:?})\nstdout: {}\nstderr: {}",
        output.status.code(),
        stdout(output),
        stderr(output)
    );
}

fn db_arg(dir: &Path) -> String {
    dir.join("db").to_string_lossy().into_owned()
}

/// Pipes `script` into a session opened with `args` and returns the process
/// output once stdin closes — the piped REPL, one line per command.
fn pipe(args: &[&str], script: &[u8]) -> Output {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .env_remove("STRATA_DB")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn repl");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(script)
        .expect("pipe commands");
    child.wait_with_output().expect("repl completes on EOF")
}

// --- durable cross-process execution -----------------------------------

/// #3339: the CLI's own reports are not commands and have no declaration, so
/// their rendering is only proven through the binary that prints them.
/// #3116: bytes written through the CLI come back through the CLI.
///
/// The issue's own repro: three bytes that are not text, stored with
/// `--file` and read back with `--raw`. Before S4 every output mode answered
/// with base64 and nothing said so, which left no correct behaviour available
/// to a caller — decode everything and genuine text corrupts, decode nothing
/// and binary does.
#[test]
fn raw_reads_return_the_stored_bytes_verbatim() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = db_arg(dir.path());
    let payload = dir.path().join("payload.bin");
    let bytes: [u8; 4] = [0x00, 0x01, 0xff, 0x0a];
    std::fs::write(&payload, bytes).expect("write payload");

    assert_ok(
        &strata(&[
            "--db",
            &db,
            "kv",
            "put",
            "raw",
            "--file",
            &payload.display().to_string(),
        ]),
        "kv put --file",
    );

    let read = strata(&["--db", &db, "--raw", "kv", "get", "raw"]);
    assert_ok(&read, "--raw kv get");
    assert_eq!(
        read.stdout, bytes,
        "the stored bytes, and nothing of the renderer's own"
    );

    // A reader still gets a labelled rendering, never a mangled one.
    let human = strata(&["--db", &db, "kv", "get", "raw"]);
    assert_ok(&human, "kv get");
    assert_eq!(stdout(&human), "base64:AAH/Cg==\n");

    // Text keeps working as a shell idiom: the value, with no newline added.
    assert_ok(
        &strata(&["--db", &db, "kv", "put", "greeting", "hello"]),
        "kv put",
    );
    let text = strata(&["--db", &db, "--raw", "kv", "get", "greeting"]);
    assert_ok(&text, "--raw kv get text");
    assert_eq!(text.stdout, b"hello");

    // A miss still writes nothing at all, and still exits 0 (Q9).
    let missing = strata(&["--db", &db, "--raw", "kv", "get", "absent"]);
    assert_ok(&missing, "--raw kv get absent");
    assert!(missing.stdout.is_empty(), "{:?}", missing.stdout);
}

#[test]
fn a_cli_report_prints_lines_not_json() {
    let dir = tempfile::tempdir().expect("temp dir");
    let home = dir.path().display().to_string();
    let show = strata_env(&["config", "show"], &[("HOME", &home)]);
    assert_ok(&show, "config show");
    let lines = stdout(&show);
    assert!(
        lines.starts_with("hub.url  ") && lines.contains("\nsource   "),
        "a report reads as a record block: {lines:?}"
    );
    assert!(!lines.contains('{'), "no JSON reaches a reader: {lines:?}");

    let raw = strata_env(&["--raw", "config", "show"], &[("HOME", &home)]);
    assert_ok(&raw, "config show --raw");
    assert!(
        stdout(&raw).starts_with("hub.url\t"),
        "a script reads key<TAB>value: {:?}",
        stdout(&raw)
    );

    // `--json` is untouched: the envelope is the report.
    let json = strata_env(&["--json", "config", "show"], &[("HOME", &home)]);
    assert_ok(&json, "config show --json");
    let envelope: serde_json::Value =
        serde_json::from_str(stdout(&json).trim()).expect("an envelope");
    assert!(envelope.get("hub.url").is_some(), "envelope: {envelope}");
}

#[test]
fn kv_round_trip_survives_across_processes() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "alpha", "1"]), "put");

    let get = strata(&["--db", &db, "kv", "get", "alpha"]);
    assert_ok(&get, "get in a second process");
    assert_eq!(
        stdout(&get).trim(),
        "1",
        "durable value must survive the process boundary"
    );

    let exists = strata(&["--db", &db, "kv", "exists", "alpha"]);
    assert_ok(&exists, "exists");
    assert!(
        stdout(&exists).contains("true"),
        "exists: {}",
        stdout(&exists)
    );

    assert_ok(
        &strata(&["--db", &db, "kv", "delete", "alpha"]),
        "delete in a third process",
    );
    let gone = strata(&["--db", &db, "kv", "get", "alpha"]);
    assert_ok(&gone, "get after delete");
    assert_eq!(
        stdout(&gone).trim(),
        "(nil)",
        "deleted key must read nil in a fourth process"
    );
}

#[test]
fn kv_list_and_count_agree_across_processes() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    for (key, value) in [("user:a", "1"), ("user:b", "2"), ("other", "3")] {
        assert_ok(&strata(&["--db", &db, "kv", "put", key, value]), "seed put");
    }
    let count = strata(&["--db", &db, "--json", "kv", "count"]);
    assert_ok(&count, "count");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&count).trim()).expect("count emits valid JSON");
    let rendered = parsed.to_string();
    assert!(
        rendered.contains('3'),
        "count JSON should report 3: {parsed}"
    );
    let list = strata(&["--db", &db, "kv", "list"]);
    assert_ok(&list, "list");
    for key in ["user:a", "user:b", "other"] {
        assert!(
            stdout(&list).contains(key),
            "list missing {key}: {}",
            stdout(&list)
        );
    }
}

#[test]
fn json_output_is_machine_parseable_and_typed() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "k", "v"]), "put");
    let get = strata(&["--db", &db, "--json", "kv", "get", "k"]);
    assert_ok(&get, "json get");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&get).trim()).expect("kv get --json emits valid JSON");
    assert_eq!(
        parsed["type"], "kv_versioned_value",
        "typed envelope: {parsed}"
    );
    assert_eq!(parsed["data"]["found"], true, "found flag: {parsed}");
}

#[test]
fn raw_output_is_script_friendly() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(
        &strata(&["--db", &db, "--raw", "kv", "put", "r", "v"]),
        "raw put",
    );
    let get = strata(&["--db", &db, "--raw", "kv", "get", "r"]);
    assert_ok(&get, "raw get");
    // #3116: exactly the value — no newline of the renderer's own, so
    // `--raw kv get k > file` is the file that was put.
    assert_eq!(stdout(&get), "v", "raw get must emit exactly the value");
}

#[test]
fn missing_key_reads_nil_with_success_exit() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "seed", "x"]), "seed");
    let get = strata(&["--db", &db, "kv", "get", "absent"]);
    assert_ok(&get, "missing-key get");
    assert_eq!(stdout(&get).trim(), "(nil)");
}

// --- targeting and refusal contracts ------------------------------------

#[test]
fn no_database_refuses_with_typed_error_and_exit_two() {
    let output = strata(&["kv", "get", "k"]);
    assert_eq!(output.status.code(), Some(2), "refusal must exit 2");
    let err = stderr(&output);
    assert!(
        err.contains("invalid_argument.cli.no_database"),
        "typed error code expected: {err}"
    );
    assert!(
        err.contains("--cache"),
        "hint should mention --cache: {err}"
    );
}

#[test]
fn conflicting_database_targets_refuse() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let both = strata(&["--db", &db, "--cache", "kv", "put", "k", "v"]);
    assert_eq!(both.status.code(), Some(2), "--db with --cache must refuse");
    let positional = strata(&[&db, "--db", &db, "info"]);
    assert_eq!(
        positional.status.code(),
        Some(2),
        "positional DB with --db must refuse: {}",
        stderr(&positional)
    );
}

#[test]
fn env_var_targets_the_database() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let put = strata_env(&["kv", "put", "envk", "envv"], &[("STRATA_DB", &db)]);
    assert_ok(&put, "put via STRATA_DB");
    let get = strata_env(&["kv", "get", "envk"], &[("STRATA_DB", &db)]);
    assert_ok(&get, "get via STRATA_DB");
    assert_eq!(stdout(&get).trim(), "envv");
}

#[test]
fn cache_mode_is_ephemeral_per_process() {
    let put = strata(&["--cache", "kv", "put", "ghost", "1"]);
    assert_ok(&put, "cache put");
    let get = strata(&["--cache", "kv", "get", "ghost"]);
    assert_ok(&get, "cache get in a new process");
    assert_eq!(
        stdout(&get).trim(),
        "(nil)",
        "cache data must not survive the process"
    );
}

// --- branches ------------------------------------------------------------

#[test]
fn branch_scoped_writes_stay_isolated_across_processes() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(
        &strata(&["--db", &db, "branch", "create", "dev"]),
        "branch create",
    );
    assert_ok(
        &strata(&["--db", &db, "--branch", "dev", "kv", "put", "flag", "on"]),
        "branch-scoped put",
    );
    let default_read = strata(&["--db", &db, "kv", "get", "flag"]);
    assert_ok(&default_read, "default-branch read");
    assert_eq!(
        stdout(&default_read).trim(),
        "(nil)",
        "default branch must not see dev writes"
    );
    let dev_read = strata(&["--db", &db, "--branch", "dev", "kv", "get", "flag"]);
    assert_ok(&dev_read, "dev-branch read");
    assert_eq!(stdout(&dev_read).trim(), "on");
    let list = strata(&["--db", &db, "branch", "list"]);
    assert_ok(&list, "branch list");
    assert!(stdout(&list).contains("dev") && stdout(&list).contains("default"));
}

#[test]
fn branch_merge_promotes_and_honors_the_strategy() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "flag", "base"]), "seed");
    assert_ok(
        &strata(&["--db", &db, "branch", "fork", "default", "dev"]),
        "branch fork",
    );
    assert_ok(
        &strata(&["--db", &db, "--branch", "dev", "kv", "put", "flag", "tuned"]),
        "change on fork",
    );
    // Diverge the target too, so the two branches conflict on `flag`.
    assert_ok(
        &strata(&["--db", &db, "kv", "put", "flag", "other"]),
        "change on target",
    );

    // Strict (the default strategy) refuses the conflict, mutating nothing.
    let strict = strata(&["--db", &db, "branch", "merge", "dev", "default"]);
    assert!(
        !strict.status.success(),
        "strict merge must refuse a conflict"
    );
    assert_eq!(
        stdout(&strata(&["--db", &db, "kv", "get", "flag"])).trim(),
        "other",
        "a refused merge leaves the target unchanged"
    );

    // Source-wins applies the source (dev) value onto the target (default).
    assert_ok(
        &strata(&[
            "--db",
            &db,
            "branch",
            "merge",
            "dev",
            "default",
            "--strategy",
            "source-wins",
        ]),
        "source-wins merge",
    );
    assert_eq!(
        stdout(&strata(&["--db", &db, "kv", "get", "flag"])).trim(),
        "tuned",
        "source-wins promotes the fork's value onto the target",
    );
}

#[test]
fn branch_preview_reports_conflicts_without_mutating() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "flag", "base"]), "seed");
    assert_ok(
        &strata(&["--db", &db, "branch", "fork", "default", "dev"]),
        "branch fork",
    );
    assert_ok(
        &strata(&["--db", &db, "--branch", "dev", "kv", "put", "flag", "tuned"]),
        "change on fork",
    );
    assert_ok(
        &strata(&["--db", &db, "kv", "put", "flag", "other"]),
        "change on target",
    );

    let preview = strata(&["--db", &db, "branch", "preview", "dev", "default"]);
    assert_ok(&preview, "branch preview");
    // Preview is read-only: the target keeps its value.
    assert_eq!(
        stdout(&strata(&["--db", &db, "kv", "get", "flag"])).trim(),
        "other",
        "preview must not mutate the target",
    );
    let dev_read = strata(&["--db", &db, "--branch", "dev", "kv", "get", "flag"]);
    assert_eq!(
        stdout(&dev_read).trim(),
        "tuned",
        "preview must not mutate the source"
    );
}

// --- vectors ---------------------------------------------------------------

#[test]
fn vector_upsert_and_query_survive_across_processes() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(
        &strata(&["--db", &db, "vector", "collection", "create", "emb", "4"]),
        "collection create",
    );
    assert_ok(
        &strata(&["--db", &db, "vector", "upsert", "emb", "k1", "1,0,0,0"]),
        "upsert k1",
    );
    assert_ok(
        &strata(&["--db", &db, "vector", "upsert", "emb", "k2", "0,1,0,0"]),
        "upsert k2",
    );
    let query = strata(&[
        "--db", &db, "--json", "vector", "query", "emb", "1,0,0,0", "-k", "1",
    ]);
    assert_ok(&query, "query in a separate process");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&query).trim()).expect("vector query --json is valid JSON");
    let rendered = parsed.to_string();
    assert!(
        rendered.contains("k1") && !rendered.contains("k2"),
        "nearest neighbor must be k1 alone: {rendered}"
    );
    let get = strata(&["--db", &db, "vector", "get", "emb", "k1"]);
    assert_ok(&get, "vector get");
    assert!(
        stdout(&get).contains('1'),
        "vector get output: {}",
        stdout(&get)
    );
}

// --- REPL / pipe -----------------------------------------------------------

#[test]
fn piped_repl_commands_execute_and_persist_durably() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let output = pipe(&["--db", &db], b"kv put a 42\nkv get a\n");
    assert!(output.status.success(), "repl exit: {output:?}");
    let out = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(out.contains("42"), "repl session must echo the read: {out}");

    // The REPL's writes are durable: a fresh process sees them.
    let get = strata(&["--db", &db, "kv", "get", "a"]);
    assert_ok(&get, "post-repl get");
    assert_eq!(stdout(&get).trim(), "42");
}

#[test]
fn piped_repl_refuses_session_arguments_on_a_line() {
    // #3327: a session argument on a line used to parse and be ignored —
    // `--db /elsewhere kv get a` answered from this database and
    // `--read-only kv put` wrote. Each such line is now an error naming the
    // argument, the command does not run, and the session exits non-zero.
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let output = pipe(
        &["--db", &db],
        b"kv put a 1\n--read-only kv put a 2\n--db /elsewhere kv get a\n--cache kv get a\nfoo\nkv get a\n",
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a refused line is a pipe error: {output:?}"
    );
    let out = String::from_utf8_lossy(&output.stdout).into_owned();
    let err = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(
        out.lines().collect::<Vec<_>>(),
        ["created a", "1"],
        "only the plain lines ran, and the refused write did not: {out}\n{err}"
    );
    let refusals: Vec<&str> = err
        .lines()
        .filter(|line| line.starts_with("error: "))
        .collect();
    assert_eq!(refusals.len(), 4, "one refusal per offending line: {err}");
    for (refusal, named) in refusals
        .iter()
        .zip(["`--read-only`", "`--db`", "`--cache`", "`foo`"])
    {
        assert!(
            refusal.contains(named),
            "refusal must name {named}: {refusal}"
        );
    }
}

#[test]
fn piped_repl_renders_each_line_in_the_format_its_flags_chose() {
    // #3326: a line's `--json` / `--raw` parsed and was dropped, every line
    // rendering in the session's format. Each line now renders exactly what a
    // one-shot with the same flags prints. (`--output-format` went with Q5 in
    // #3314 S4: one way to choose a format, not two.)
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(
        &strata(&["--db", &db, "kv", "put", "greeting", "hello"]),
        "seed",
    );
    let one_shot = |flags: &[&str]| {
        let mut args = vec!["--db", db.as_str()];
        args.extend_from_slice(flags);
        args.extend_from_slice(&["kv", "get", "greeting"]);
        let output = strata(&args);
        assert_ok(&output, "one-shot read");
        stdout(&output)
    };
    // A one-shot's raw answer is the stored value alone (#3116); a stream of
    // lines separates its answers, so the raw line carries a newline the
    // one-shot does not.
    let expected = [
        one_shot(&["--json"]),
        format!("{}\n", one_shot(&["--raw"])),
        one_shot(&[]),
    ]
    .concat();

    let output = pipe(
        &["--db", &db],
        b"--json kv get greeting\n--raw kv get greeting\nkv get greeting\n",
    );
    assert_ok(&output, "a format flag on a line is honoured, not an error");
    assert_eq!(stdout(&output), expected);
    assert_eq!(stderr(&output), "");
}

#[test]
fn piped_repl_line_format_overrides_a_json_session() {
    // #3326, the inverse: under `--json`, a line that asks for raw output
    // gets it — for its answer and for its error alike — while lines that
    // choose nothing keep the session's JSON. (Since Q5 in #3314 S4 a line
    // chooses `--json` or `--raw`; the hidden `--output-format`, and with it
    // the only way to ask a line for human output, is gone.)
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(
        &strata(&["--db", &db, "kv", "put", "greeting", "hello"]),
        "seed",
    );
    let json_read = strata(&["--db", &db, "--json", "kv", "get", "greeting"]);
    assert_ok(&json_read, "one-shot json read");

    let output = pipe(
        &["--db", &db, "--json"],
        b"--raw kv get greeting\n--raw kv get greeting\nkv get greeting\n--raw branch get nope\nbranch get nope\n",
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a failed line is a pipe error: {output:?}"
    );
    // The two raw lines answer with the stored value; the stream separates
    // them (#3116), and the session's JSON line keeps its envelope.
    assert_eq!(
        stdout(&output),
        format!("hello\nhello\n{}", stdout(&json_read))
    );
    let err = stderr(&output);
    let mut lines = err.lines();
    let raw = lines.next().expect("the raw line's error");
    assert!(
        raw.starts_with("not_found.engine.branch:"),
        "a raw line's error is the code-led line, not an envelope: {err}"
    );
    let envelope: serde_json::Value = serde_json::from_str(
        lines.last().expect("the json line's error"),
    )
    .unwrap_or_else(|error| panic!("a json line's error is the JSON envelope: {error}\n{err}"));
    assert_eq!(
        envelope["error"]["code"], "not_found.engine.branch",
        "{err}"
    );
}

#[test]
fn piped_repl_reports_a_failed_line_in_the_format_the_line_chose() {
    // #3326 for the error path: a `--json` line under a human session fails
    // with the JSON error envelope, and a plain line with the human line.
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let output = pipe(&["--db", &db], b"--json branch get nope\nbranch get nope\n");
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(stdout(&output), "", "errors never answer on stdout");
    let err = stderr(&output);
    let mut lines = err.lines();
    let envelope: serde_json::Value = serde_json::from_str(
        lines.next().expect("the json line's error"),
    )
    .unwrap_or_else(|error| panic!("a json line's error is the JSON envelope: {error}\n{err}"));
    assert_eq!(
        envelope["error"]["code"], "not_found.engine.branch",
        "{err}"
    );
    let human = lines.next().expect("the plain line's error");
    assert!(
        human.starts_with("not_found.engine.branch:"),
        "a plain line's error is the session's human line: {err}"
    );
}

#[test]
fn piped_repl_prints_a_parse_error_once() {
    // #3328: the piped loop prefixed every failed line with `error: ` on top
    // of clap's own `error:`, so a parse error read `error: error: …`; the
    // interactive loop had already stopped doing that (#2998). One reporter
    // now serves both.
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let output = pipe(&["--db", &db], b"kv get\n");
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let err = stderr(&output);
    let first = err.lines().next().expect("a parse error is reported");
    assert!(
        first.starts_with("error: ") && !first.starts_with("error: error:"),
        "clap's message is printed once, not re-prefixed: {err}"
    );
    assert_eq!(err.matches("error:").count(), 1, "{err}");
}

// --- init / observability ---------------------------------------------------

#[test]
fn init_prepares_home_and_is_idempotent() {
    let home = tempfile::tempdir().expect("tmp");
    let home_str = home.path().to_string_lossy().into_owned();
    let first = strata_env(&["--json", "init"], &[("STRATA_HOME", &home_str)]);
    assert_ok(&first, "init");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&first).trim()).expect("init --json is valid JSON");
    assert_eq!(parsed["type"], "init", "typed envelope: {parsed}");
    assert_eq!(
        parsed["data"]["home"].as_str(),
        Some(home_str.as_str()),
        "init JSON names the prepared home: {parsed}"
    );
    let second = strata_env(&["init"], &[("STRATA_HOME", &home_str)]);
    assert_ok(&second, "second init must be idempotent");
}

#[test]
fn info_and_health_emit_valid_json_for_a_durable_database() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "seed", "1"]), "seed");
    for command in ["info", "health", "describe"] {
        let output = strata(&["--db", &db, "--json", command]);
        assert_ok(&output, command);
        serde_json::from_str::<serde_json::Value>(stdout(&output).trim()).unwrap_or_else(|err| {
            panic!("{command} --json must parse: {err}\n{}", stdout(&output))
        });
    }
}

// --- cross-process writer lock ----------------------------------------------

/// The durable writer lock is exclusive across processes, refusals are typed
/// and retryable, and an OS-level kill of the holder releases the lock.
#[test]
fn writer_lock_is_exclusive_across_processes_and_releases_on_kill() {
    use std::io::{BufRead, Write};

    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    // Clean first session so the store exists durably (see #2618 for the
    // killed-first-session case, parked below).
    assert_ok(&strata(&["--db", &db, "kv", "put", "seeded", "1"]), "seed");

    // Hold the database from a REPL child whose stdin never closes, and
    // prove it owns the lock before contending: command a write through its
    // stdin and wait for the echoed response on its stdout. (A silent child
    // may not have opened the database yet, and its single open attempt can
    // lose to the contender's rapid open/close cycles — the first version
    // of this test flaked exactly that way.)
    let mut holder = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["--db", &db])
        .env_remove("STRATA_DB")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn holder");
    holder
        .stdin
        .as_mut()
        .expect("holder stdin")
        .write_all(b"kv put holder 1\n")
        .expect("command the holder");
    let mut holder_stdout = std::io::BufReader::new(holder.stdout.take().expect("holder stdout"));
    let mut line = String::new();
    holder_stdout
        .read_line(&mut line)
        .expect("holder responds once it holds the database");
    assert!(
        line.contains("holder"),
        "holder's first response should echo its write: {line:?}"
    );

    let refused = strata(&["--db", &db, "kv", "put", "contender", "1"]);
    assert!(
        !refused.status.success(),
        "contender must be refused while the holder owns the lock"
    );
    assert!(
        stderr(&refused).contains("unavailable.engine.persistence"),
        "cross-process contention must surface the typed persistence refusal: {}",
        stderr(&refused)
    );

    // SIGKILL the holder: the OS releases the lock; a fresh process wins.
    holder.kill().expect("kill holder");
    let _ = holder.wait();
    let mut recovered = None;
    for _ in 0..200 {
        let retry = strata(&["--db", &db, "kv", "put", "after-kill", "2"]);
        if retry.status.success() {
            recovered = Some(retry);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        recovered.is_some(),
        "the writer lock must release when the holding process dies"
    );
    let read = strata(&["--db", &db, "kv", "get", "seeded"]);
    assert_ok(&read, "post-kill read");
    assert_eq!(stdout(&read).trim(), "1", "pre-kill durable data survives");
}

/// `strata ipc stop`, run against a live `--ipc host` holder, brokers to it and
/// tells it to stop hosting — a one-shot control command over the socket.
#[test]
fn ipc_stop_halts_a_running_host() {
    use std::io::{BufRead, Write};

    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "seed", "1"]), "seed");

    let mut holder = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["--db", &db, "--ipc", "host"])
        .env_remove("STRATA_DB")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn host holder");
    holder
        .stdin
        .as_mut()
        .expect("holder stdin")
        .write_all(b"kv put holder 1\n")
        .expect("command the holder");
    let mut holder_stdout = std::io::BufReader::new(holder.stdout.take().expect("holder stdout"));
    let mut line = String::new();
    holder_stdout
        .read_line(&mut line)
        .expect("holder responds once it hosts");

    // A one-shot `ipc stop` brokers to the host and stops its hosting.
    let stop = strata(&["--db", &db, "--json", "ipc", "stop"]);
    assert_ok(&stop, "ipc stop");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&stop).trim()).expect("ipc stop --json parses");
    assert_eq!(parsed["type"], "ipc_stop", "typed envelope: {parsed}");
    assert_eq!(
        parsed["data"]["stopped"], true,
        "the running host was stopped: {parsed}"
    );

    drop(holder.stdin.take());
    let _ = holder.wait();
}

/// `strata ipc status` reports this process's multi-process state. A one-shot
/// against an uncontended store is a (client-mode) owner that hosts nothing.
#[test]
fn ipc_status_reports_single_process_state() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "seed", "1"]), "seed");

    let status = strata(&["--db", &db, "--json", "ipc", "status"]);
    assert_ok(&status, "ipc status");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&status).trim()).expect("ipc status --json parses");
    assert_eq!(parsed["type"], "ipc_status", "typed envelope: {parsed}");
    assert_eq!(
        parsed["data"]["is_owner"], true,
        "the one-shot owns the store"
    );
    assert_eq!(parsed["data"]["hosting"], false, "a one-shot hosts nothing");
    assert_eq!(parsed["data"]["client_count"], 0, "no clients");
}

/// With an explicit IPC host, a second process brokers to the first over the
/// owner's socket instead of being refused — one engine, one store, two OS
/// processes. This is the multi-process access the writer-lock exclusion
/// otherwise forbids; only a designated host (here `--ipc host`) enables it.
#[test]
fn a_second_process_brokers_to_an_ipc_host_holder() {
    use std::io::{BufRead, Write};

    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(&strata(&["--db", &db, "kv", "put", "seeded", "1"]), "seed");

    // A host holder: a piped session forced to host a socket (a piped session
    // defaults to Client and would NOT host), kept alive by an open stdin.
    // Round-trip a command first to prove it holds and hosts before contending.
    let mut holder = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["--db", &db, "--ipc", "host"])
        .env_remove("STRATA_DB")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn host holder");
    holder
        .stdin
        .as_mut()
        .expect("holder stdin")
        .write_all(b"kv put holder 1\n")
        .expect("command the holder");
    let mut holder_stdout = std::io::BufReader::new(holder.stdout.take().expect("holder stdout"));
    let mut line = String::new();
    holder_stdout
        .read_line(&mut line)
        .expect("holder responds once it holds and hosts the database");
    assert!(line.contains("holder"), "holder echoes its write: {line:?}");

    // A one-shot contender now BROKERS to the host instead of being refused.
    let brokered = strata(&["--db", &db, "kv", "put", "contender", "1"]);
    assert_ok(
        &brokered,
        "the second process must broker to the host, not fail on the lock",
    );

    // The holder — the one and only engine — sees the brokered write, proving
    // both processes share a single store.
    holder
        .stdin
        .as_mut()
        .expect("holder stdin")
        .write_all(b"kv get contender\n")
        .expect("read the brokered write back through the holder");
    let mut got = String::new();
    holder_stdout
        .read_line(&mut got)
        .expect("holder responds to the get");
    assert!(got.contains('1'), "holder sees the brokered write: {got:?}");

    // Closing stdin ends the holder's session and unlinks the socket.
    drop(holder.stdin.take());
    let _ = holder.wait();
}

/// Regression for #2618 (fixed by the creation durability barrier): killing
/// the FIRST session on a fresh store leaves a usable database, not a
/// permanent `data_loss.engine.control_plane_missing` brick.
#[test]
fn sigkill_during_first_session_leaves_a_usable_database() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let mut first_session = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["--db", &db])
        .env_remove("STRATA_DB")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn first session");
    // Give creation time to begin, then kill without any clean close.
    std::thread::sleep(std::time::Duration::from_millis(1500));
    first_session.kill().expect("kill first session");
    let _ = first_session.wait();

    let put = strata(&["--db", &db, "kv", "put", "probe", "1"]);
    assert!(
        put.status.success(),
        "a store whose first session was killed must remain usable, got:\n{}",
        stderr(&put)
    );
}

// --- remote origin rendering --------------------------------------------

/// `strata remote` on a database that was never cloned reports "no origin"
/// as data (a null origin in a typed envelope), not as an error: exit 0 in
/// both output modes, and the JSON envelope carries the stable result type.
#[test]
fn remote_on_a_never_cloned_database_reports_null_origin() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert_ok(
        &strata(&["--db", &db, "kv", "put", "seed", "1"]),
        "seed the database",
    );

    let json = strata(&["--db", &db, "--json", "remote"]);
    assert_ok(&json, "remote --json");
    let envelope: serde_json::Value =
        serde_json::from_str(stdout(&json).trim()).expect("remote emits a JSON envelope");
    assert_eq!(
        envelope.get("type").and_then(serde_json::Value::as_str),
        Some("remote_origin_result"),
        "envelope: {envelope}"
    );
    assert!(
        envelope
            .get("data")
            .and_then(|data| data.get("origin"))
            .is_some_and(serde_json::Value::is_null),
        "never-cloned database must report a null origin: {envelope}"
    );

    // #3314 S3a: `remote` declares the origin record's fields, so a null
    // origin is a miss — `(nil)` on stdout, exit 0, nothing for a script.
    let human = strata(&["--db", &db, "remote"]);
    assert_ok(&human, "remote (human mode)");
    assert_eq!(
        stdout(&human),
        "(nil)\n",
        "a never-cloned database has no origin to show"
    );
    let raw = strata(&["--db", &db, "--raw", "remote"]);
    assert_ok(&raw, "remote (raw mode)");
    assert_eq!(stdout(&raw), "", "a script reads the miss as no output");
}

/// #3112 S5: the loop the whole epic exists to enable — read history, take a
/// date off a row, hand it straight back as a read bound.
///
/// This is an end-to-end test through the real binary on purpose. The two
/// halves live in different places (formatting on the way out, parsing on the
/// way in) and each is individually correct in unit tests; only running them
/// against each other proves the date a user actually sees is a date the tool
/// actually accepts.
///
/// It also pins the precision contract. An earlier build printed milliseconds,
/// which looked fine and round-tripped to the commit BEFORE the one it came
/// from — a silently wrong read rather than an error.
#[test]
fn a_date_printed_by_history_reads_back_the_value_from_that_commit() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = dir.path().join("db");
    let db = db.to_str().expect("utf8 path");

    assert!(strata(&[db, "kv", "put", "k", "one"]).status.success());
    // Separate the commits in wall-clock time so each is individually
    // addressable by date.
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(strata(&[db, "kv", "put", "k", "two"]).status.success());

    // History is a table (output contract S2): `VERSION  COMMITTED_AT  VALUE`,
    // newest first. Take the date cell off the oldest row, as a reader would.
    let history = stdout(&strata(&[db, "kv", "history", "k"]));
    let mut lines = history.lines().filter(|line| !line.trim().is_empty());
    let header: Vec<&str> = lines
        .next()
        .expect("history has a header")
        .split("  ")
        .filter(|cell| !cell.is_empty())
        .collect();
    let date_column = header
        .iter()
        .position(|name| *name == "COMMITTED_AT")
        .expect("history declares a COMMITTED_AT column");
    let oldest: Vec<&str> = lines
        .next_back()
        .expect("history has an oldest row")
        .split("  ")
        .map(str::trim)
        .filter(|cell| !cell.is_empty())
        .collect();
    let printed = oldest[date_column];

    // It reads as a date, not as a raw number.
    assert!(
        printed.contains('-') && printed.contains(':'),
        "expected a rendered date, got {printed}"
    );

    // And handing it straight back reaches that commit's value.
    let read_back = strata(&[db, "kv", "get", "k", "--as-of-time", printed]);
    assert!(
        read_back.status.success(),
        "reading at a printed date failed: {}",
        stderr(&read_back)
    );
    assert_eq!(
        stdout(&read_back).trim(),
        "one",
        "the date from the oldest row must read that row's value, not a \
         neighbouring commit's"
    );

    // Current state is still the newer value, so the read above was genuine
    // time travel rather than a plain read.
    assert_eq!(stdout(&strata(&[db, "kv", "get", "k"])).trim(), "two");
}

/// The two clocks stay mutually exclusive through the CLI, and an unparseable
/// date is refused with a message that shows a spelling that works.
#[test]
fn the_cli_refuses_both_clocks_and_explains_an_unparseable_date() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = dir.path().join("db");
    let db = db.to_str().expect("utf8 path");
    assert!(strata(&[db, "kv", "put", "k", "v"]).status.success());

    let both = strata(&[
        db,
        "kv",
        "get",
        "k",
        "--as-of",
        "3",
        "--as-of-time",
        "2026-09-05 15:00",
    ]);
    assert!(!both.status.success(), "supplying both clocks must fail");
    assert!(
        stderr(&both).contains("as_of_conflict"),
        "expected the mutual-exclusion error: {}",
        stderr(&both)
    );

    let bad = strata(&[db, "kv", "get", "k", "--as-of-time", "yesterday"]);
    assert!(!bad.status.success(), "an unparseable date must fail");
    let message = stderr(&bad);
    assert!(
        message.contains("2026-09-05"),
        "the refusal must show a working spelling: {message}"
    );
}

/// #3094: a binary of unknown provenance can say what is in it — offline, with
/// no database, matched to that exact build.
///
/// The reported failure was a documented, deliberately wire-visible error
/// reclassification that still broke a downstream build, because `CHANGELOG.md`
/// lived only in the repository while the consumer held a binary and a release
/// asset. These run through the real binary because the value is entirely in
/// the glue: embedding the file proves nothing if the verb is unreachable, needs
/// a database, or reports the wrong exit code.
#[test]
fn changelog_prints_offline_without_a_database() {
    let output = strata(&["changelog"]);
    assert!(
        output.status.success(),
        "changelog must not need a database: {}",
        stderr(&output)
    );
    let text = stdout(&output);
    assert!(text.starts_with("# Changelog"), "got: {text:.60}");

    // The build's own version is documented — otherwise the command answers
    // nothing about the binary you are holding, which is the original failure
    // one step later.
    let version = env!("CARGO_PKG_VERSION");
    assert!(
        text.contains(&format!("## [{version}]")),
        "changelog has no entry for this build ({version})"
    );
}

#[test]
fn changelog_can_print_a_single_release_and_refuses_an_unknown_one() {
    let version = env!("CARGO_PKG_VERSION");
    let one = strata(&["changelog", "--version", version]);
    assert!(one.status.success(), "{}", stderr(&one));
    let section = stdout(&one);
    assert!(section.starts_with(&format!("## [{version}]")));
    assert!(
        !section.starts_with("# Changelog"),
        "--version must print one section, not the whole file"
    );

    let missing = strata(&["changelog", "--version", "0.0.0-nope"]);
    assert!(
        !missing.status.success(),
        "an unknown version must fail, not print nothing successfully"
    );
    // The refusal lists what this build does document, so the caller can
    // correct themselves without cloning the repository.
    let message = stderr(&missing);
    assert!(
        message.contains(version),
        "refusal should name the versions available: {message}"
    );
}

/// #3316: two writers share stderr — `render_error` and the boundary tracing
/// subscriber — and `tracing_subscriber` colours by default no matter what
/// stderr is. A captured stderr got escape codes wrapped around a line the
/// other writer leaves plain. `Command::output` pipes, so this asserts the
/// piped case, which is every programmatic caller.
#[test]
fn a_boundary_error_log_carries_no_colour_into_a_piped_stderr() {
    let missing = "/nonexistent-parent-for-3316/db";
    for args in [
        vec!["--db", missing, "kv", "get", "x"],
        vec!["--json", "--db", missing, "kv", "get", "x"],
    ] {
        let output = strata(&args);
        let text = stderr(&output);
        assert!(
            !text.contains('\u{1b}'),
            "escape byte on a piped stderr for {args:?}: {text:?}"
        );
        // Direction control: the line itself must survive. It is what makes a
        // shown reference_id correlate with a real, inspectable record (ERR-2),
        // so decolouring it must not amount to deleting it.
        assert!(
            text.contains("engine error crossed the executor boundary"),
            "the boundary log went missing for {args:?}: {text:?}"
        );
    }
}
