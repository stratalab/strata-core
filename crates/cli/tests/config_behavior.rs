//! CLI user-config write-path behavior (TCP3.10c).
//!
//! `strata config set/unset/path/show` run before any database opens and write
//! the global user config (`hub.url`, `<provider>.api_key`). These drive the
//! real binary against a hermetic `HOME` and assert the write path: the file is
//! created 0600, secrets are redacted and never echoed, the environment wins
//! over the stored value, and unset falls back to the built-in default.

#![deny(unsafe_code)]

use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

/// The `strata` binary with a hermetic config home: `HOME` points at a temp
/// dir and every config/env override that could leak from the developer's
/// machine — including an exported provider key — is stripped.
fn config_command(home: &TempDir, args: &[&str], extra_env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_strata"));
    command
        .args(args)
        .env("HOME", home.path())
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("STRATA_HUB_URL")
        .env_remove("STRATA_DB")
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("GOOGLE_API_KEY");
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
}

fn config_cli(home: &TempDir, args: &[&str], extra_env: &[(&str, &str)]) -> Output {
    config_command(home, args, extra_env)
        .output()
        .expect("run strata binary")
}

/// Runs a piped session: `lines` on stdin, one command per line, the way a
/// script or an agent drives the binary without a terminal.
#[cfg(feature = "inference")]
fn config_cli_piped(home: &TempDir, args: &[&str], lines: &str) -> Output {
    use std::io::Write as _;
    use std::process::Stdio;

    let mut child = config_command(home, args, &[])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn strata binary");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(lines.as_bytes())
        .expect("write the session's lines");
    child.wait_with_output().expect("run strata binary")
}

fn json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout is JSON")
}

#[test]
fn hub_url_set_show_unset_roundtrip() {
    let home = tempfile::tempdir().expect("temp home");

    let set = json(&config_cli(
        &home,
        &["--json", "config", "set", "hub.url", "https://hub.example"],
        &[],
    ));
    assert_eq!(set["value"], "https://hub.example");

    // `show` resolves the hub URL and reports which layer supplied it.
    let show = json(&config_cli(&home, &["--json", "config", "show"], &[]));
    assert_eq!(show["hub.url"], "https://hub.example/");
    assert!(
        show["source"]
            .as_str()
            .expect("source string")
            .ends_with("config.toml"),
        "hub.url must be sourced from the config file: {show}"
    );

    config_cli(&home, &["config", "unset", "hub.url"], &[]);
    let after = json(&config_cli(&home, &["--json", "config", "show"], &[]));
    assert_eq!(
        after["source"], "built-in default",
        "after unset, the built-in default supplies the hub URL"
    );
}

#[test]
fn env_var_overrides_the_configured_hub_url() {
    let home = tempfile::tempdir().expect("temp home");
    config_cli(
        &home,
        &["config", "set", "hub.url", "https://config.example"],
        &[],
    );

    let show = json(&config_cli(
        &home,
        &["--json", "config", "show"],
        &[("STRATA_HUB_URL", "https://env.example")],
    ));
    assert_eq!(show["hub.url"], "https://env.example/");
    assert_eq!(
        show["source"], "STRATA_HUB_URL",
        "the environment wins over the stored config"
    );
}

#[cfg(unix)]
#[test]
fn config_file_is_written_0600() {
    use std::os::unix::fs::PermissionsExt;

    let home = tempfile::tempdir().expect("temp home");
    let set = json(&config_cli(
        &home,
        &["--json", "config", "set", "hub.url", "https://hub.example"],
        &[],
    ));
    let path = set["path"].as_str().expect("config path");
    let mode = std::fs::metadata(path)
        .expect("stat config file")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o600,
        "the user config may hold secrets and must be 0600"
    );
}

#[cfg(feature = "inference")]
#[test]
fn provider_api_key_is_redacted_and_never_echoed() {
    let home = tempfile::tempdir().expect("temp home");
    let output = config_cli(
        &home,
        &[
            "--json",
            "config",
            "set",
            "openai.api_key",
            "sk-topsecret-xyz",
        ],
        &[],
    );
    let rendered = String::from_utf8_lossy(&output.stdout);
    assert!(
        !rendered.contains("sk-topsecret-xyz"),
        "the raw API key must never be echoed back: {rendered}"
    );
    let value = json(&output);
    // Redaction keeps a short non-secret prefix (first 7 chars) plus `****`.
    assert_eq!(value["value"], "sk-tops****");

    // `config get-key` reads the stored key back from the file the executor's
    // provider settings read (#3221) — set and redacted, never raw.
    let output = config_cli(
        &home,
        &["--json", "config", "get-key", "openai.api_key"],
        &[],
    );
    let rendered = String::from_utf8_lossy(&output.stdout);
    assert!(
        !rendered.contains("sk-topsecret-xyz"),
        "`config get-key` must never echo the raw key: {rendered}"
    );
    let value = json(&output);
    assert_eq!(value["set"], true);
    assert_eq!(value["value"], "sk-tops****");

    // A provider the file does not name, and the key once unset, are unset.
    let value = json(&config_cli(
        &home,
        &["--json", "config", "get-key", "anthropic.api_key"],
        &[],
    ));
    assert_eq!(value["set"], false);
    assert_eq!(value["value"], Value::Null);
    config_cli(&home, &["config", "unset", "openai.api_key"], &[]);
    let value = json(&config_cli(
        &home,
        &["--json", "config", "get-key", "openai.api_key"],
        &[],
    ));
    assert_eq!(value["set"], false);
    assert_eq!(value["value"], Value::Null);
}

/// `inference status` names where a key actually came from. The runtime's
/// provider settings ask the environment first and the user config file
/// second (#3221), and `key_source` is the place that answered: the variable
/// for an exported key, the file's path for one stored with `config set
/// <provider>.api_key`. This drives that through the real binary against a
/// file the test wrote.
///
/// The three cases are the boundary on both sides: a config-backed key names
/// the file, an exported key keeps its variable even when the file also has
/// one (the environment wins), and a provider with no key has no source.
///
/// The session case guards the path an agent drives: `inference status`
/// mid-pipe reads the same settings as the one-shot — there is no per-process
/// bridge to have run first or to run again.
#[cfg(feature = "inference")]
#[test]
fn inference_status_names_where_a_key_came_from() {
    const SECRET: &str = "sk-from-config-file";

    let home = tempfile::tempdir().expect("temp home");
    let db = home.path().join("db");
    let db = db.to_str().expect("utf-8 temp path");
    let status_args = ["--db", db, "--json", "inference", "status"];

    let provider = |status: &Value, name: &str| -> Value {
        status["data"]["providers"]
            .as_array()
            .expect("providers")
            .iter()
            .find(|row| row["provider"] == name)
            .unwrap_or_else(|| panic!("provider {name} missing from status: {status}"))
            .clone()
    };

    config_cli(&home, &["config", "set", "openai.api_key", SECRET], &[]);

    // Not exported: the file supplied the key, and the source must be the file.
    let output = config_cli(&home, &status_args, &[]);
    let rendered = String::from_utf8_lossy(&output.stdout);
    assert!(
        !rendered.contains(SECRET),
        "status must never carry a key value: {rendered}"
    );
    let stored = provider(&json(&output), "openai");
    assert_eq!(stored["key_present"], true, "the file's key was read");
    let source = stored["key_source"]
        .as_str()
        .expect("a present key has a source");
    assert!(
        source.ends_with("strata/config.toml"),
        "a config-backed key names the file, not a variable: {source}"
    );

    // Exported: the environment wins and its variable is the honest source,
    // even though the file also holds a key.
    let exported = provider(
        &json(&config_cli(
            &home,
            &status_args,
            &[("OPENAI_API_KEY", "sk-env")],
        )),
        "openai",
    );
    assert_eq!(exported["key_source"], "OPENAI_API_KEY");

    // No key anywhere: no source.
    let absent = provider(&json(&config_cli(&home, &status_args, &[])), "anthropic");
    assert_eq!(absent["key_present"], false);
    assert_eq!(absent["key_source"], Value::Null);

    // Mid-session: the same command through the pipe path names the file
    // too.
    let session = config_cli_piped(&home, &["--db", db, "--json"], "inference status\n");
    let in_session = provider(&json(&session), "openai");
    assert_eq!(in_session["key_present"], true);
    let source = in_session["key_source"]
        .as_str()
        .expect("a loaded key has a source in a session too");
    assert!(
        source.ends_with("strata/config.toml"),
        "a session's `inference status` names the file for a config-backed \
         key, like the one-shot: {source}"
    );
}

/// `doctor` inspects the runtime every database opens with — the executor's
/// default, whose provider settings read the environment and then the user
/// config file (#3221) — so a config-backed key makes its provider ready here
/// exactly as an exported one does (`doctor_behavior` covers the exported
/// case). Until #3221 doctor answered from an environment nobody had bridged.
#[cfg(feature = "inference")]
#[test]
fn doctor_sees_a_config_file_key() {
    let home = tempfile::tempdir().expect("temp home");
    config_cli(
        &home,
        &["config", "set", "openai.api_key", "sk-from-config"],
        &[],
    );

    // Doctor's exit code reflects installation checks unrelated to keys, so
    // read the report rather than the status.
    let output = config_cli(&home, &["--json", "doctor"], &[]);
    let report: Value = serde_json::from_slice(&output.stdout).expect("doctor report is JSON");
    let ready = report["data"]["inference"]["ready_providers"]
        .as_array()
        .expect("ready providers array");
    assert!(
        ready.iter().any(|provider| provider == "openai"),
        "a config-backed key makes its provider ready: {report}"
    );
}

/// A one-shot `inference` command needs no database (R9 of #3261): it works
/// on models, so with no `--db`, path, or `STRATA_DB` it runs in an ephemeral
/// cache session instead of the no-database refusal. The boundary holds on
/// every other side: a data command with no target still refuses, and
/// `inference install-local` still refuses a target — it changes the binary.
#[cfg(feature = "inference")]
#[test]
fn an_inference_one_shot_needs_no_database() {
    let home = tempfile::tempdir().expect("temp home");
    let status = json(&config_cli(&home, &["--json", "inference", "status"], &[]));
    assert!(
        status["data"]["providers"].is_array(),
        "a bare `inference status` answers: {status}"
    );
    assert!(
        !home.path().join("wal").exists(),
        "an implicit inference one-shot never opens the current directory"
    );

    // A data command keeps the refusal: agents never write to an implicit
    // location.
    let refused = config_cli(&home, &["--json", "kv", "get", "k"], &[]);
    assert_eq!(refused.status.code(), Some(2), "bare `kv get` must refuse");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains("invalid_argument.cli.no_database"),
        "the data command keeps the typed refusal: {stderr}"
    );

    // The host command keeps refusing a target, cache included.
    let refused = config_cli(
        &home,
        &["--cache", "--json", "inference", "install-local"],
        &[],
    );
    assert_eq!(
        refused.status.code(),
        Some(2),
        "`inference install-local` with a target is a usage error"
    );
}

#[test]
fn config_path_reports_the_config_file() {
    let home = tempfile::tempdir().expect("temp home");
    let path = json(&config_cli(&home, &["--json", "config", "path"], &[]));
    assert!(
        path["path"]
            .as_str()
            .expect("path string")
            .ends_with("strata/config.toml"),
        "config path must point at the user config file: {path}"
    );
}

#[test]
fn unknown_config_key_is_rejected() {
    let home = tempfile::tempdir().expect("temp home");
    let output = config_cli(&home, &["config", "set", "bogus.key", "x"], &[]);
    assert_eq!(
        output.status.code(),
        Some(2),
        "an unknown config key is a usage error (exit 2)"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown config key"),
        "error names the bad key: {stderr}"
    );
}
