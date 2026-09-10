//! CLI user-config write-path behavior (TCP3.10c).
//!
//! `strata config set/unset/path/show` run before any database opens and write
//! the global user config (`hub.url`, `<provider>.api_key`,
//! `<provider>.base_url`). These drive the real binary against a hermetic
//! `HOME` and assert the write path: the file is created 0600, secrets are
//! redacted and never echoed, the environment wins over the stored value, and
//! unset falls back to the built-in default.

#![deny(unsafe_code)]

use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

/// The `strata` binary with a hermetic config home: `HOME` points at a temp
/// dir and every config/env override that could leak from the developer's
/// machine — including an exported provider key or base URL, named by the
/// executor's provider table rather than a copied list — is stripped.
fn config_command(home: &TempDir, args: &[&str], extra_env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_strata"));
    command
        .args(args)
        .env("HOME", home.path())
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("STRATA_HUB_URL")
        .env_remove("STRATA_DB");
    #[cfg(feature = "inference")]
    for info in strata_executor::INFERENCE_CLOUD_PROVIDER_KEYS {
        command
            .env_remove(info.env_var)
            .env_remove(info.base_url_env_var);
    }
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

/// `<provider>.base_url` is not a secret: it reads back as typed — no
/// redaction, no normalization — and unsets like a key. A value that is not
/// an `http(s)` URL is refused at `set`, where the typo is, rather than
/// surfacing later as a provider call that cannot connect (#3270).
#[cfg(feature = "inference")]
#[test]
fn provider_base_url_round_trips_as_typed_and_refuses_a_non_http_value() {
    const URL: &str = "http://127.0.0.1:8000/v1";

    let home = tempfile::tempdir().expect("temp home");
    let set = json(&config_cli(
        &home,
        &["--json", "config", "set", "openai.base_url", URL],
        &[],
    ));
    assert_eq!(set["value"], URL, "a base URL is echoed as typed");
    assert!(
        set["path"]
            .as_str()
            .expect("config path")
            .ends_with("strata/config.toml"),
        "{set}"
    );

    let got = json(&config_cli(
        &home,
        &["--json", "config", "get-key", "openai.base_url"],
        &[],
    ));
    assert_eq!(got["set"], true);
    assert_eq!(
        got["value"], URL,
        "read back as stored, no trailing slash added"
    );

    // Independent of the key: setting a base URL stored no key.
    let key = json(&config_cli(
        &home,
        &["--json", "config", "get-key", "openai.api_key"],
        &[],
    ));
    assert_eq!(key["set"], false);

    let unset = json(&config_cli(
        &home,
        &["--json", "config", "unset", "openai.base_url"],
        &[],
    ));
    assert_eq!(unset["unset"], true);
    assert_eq!(
        unset["path"], set["path"],
        "unset names the file it edited, as set did: {unset}"
    );
    let got = json(&config_cli(
        &home,
        &["--json", "config", "get-key", "openai.base_url"],
        &[],
    ));
    assert_eq!(got["set"], false);
    assert_eq!(got["value"], Value::Null);

    // Unsetting a key the file never held still names the file it went
    // through; with no file at all there is nothing to name — both succeed.
    let never_held = json(&config_cli(
        &home,
        &["--json", "config", "unset", "anthropic.base_url"],
        &[],
    ));
    assert_eq!(never_held["unset"], true);
    assert_eq!(never_held["path"], set["path"], "{never_held}");
    let fresh = tempfile::tempdir().expect("temp home");
    let no_file = json(&config_cli(
        &fresh,
        &["--json", "config", "unset", "openai.base_url"],
        &[],
    ));
    assert_eq!(no_file["unset"], true);
    assert_eq!(no_file["path"], Value::Null, "{no_file}");

    // The likely typo — no scheme — and a non-http scheme are usage errors
    // naming the key; nothing is stored.
    for bad in ["localhost:8000", "ftp://proxy.example/v1", "not a url"] {
        let refused = config_cli(&home, &["config", "set", "openai.base_url", bad], &[]);
        assert_eq!(
            refused.status.code(),
            Some(2),
            "`{bad}` is a usage error (exit 2)"
        );
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(
            stderr.contains("openai.base_url"),
            "names the key: {stderr}"
        );
        let got = json(&config_cli(
            &home,
            &["--json", "config", "get-key", "openai.base_url"],
            &[],
        ));
        assert_eq!(got["set"], false, "`{bad}` must not be stored");
    }
}

/// `inference status` names where a request goes and what sent it there
/// (#3270): the public endpoint with no source, the file's path for a base
/// URL stored with `config set <provider>.base_url`, and the provider's own
/// variable when it is exported — which wins over the file, like a key. And
/// the stored base URL is what a call uses: with the file redirecting the
/// provider to a closed loopback port, a generate that has a key does not
/// reach the public endpoint — it observes `provider_unavailable` at once.
#[cfg(feature = "inference")]
#[test]
fn inference_status_names_where_a_base_url_came_from_and_a_call_uses_it() {
    const CLOSED_PORT: &str = "http://127.0.0.1:1";

    let home = tempfile::tempdir().expect("temp home");
    let status_args = ["--json", "inference", "status"];
    let openai = |status: &Value| -> Value {
        status["data"]["providers"]
            .as_array()
            .expect("providers")
            .iter()
            .find(|row| row["provider"] == "openai")
            .unwrap_or_else(|| panic!("openai missing from status: {status}"))
            .clone()
    };

    // Nothing set: the public endpoint, from nowhere in particular.
    let default = openai(&json(&config_cli(&home, &status_args, &[])));
    assert_eq!(default["base_url"], "https://api.openai.com/v1");
    assert_eq!(default["base_url_source"], Value::Null);
    assert_eq!(default["base_url_env_var"], "OPENAI_BASE_URL");

    // Stored: the file supplied it, and the source is the file.
    config_cli(
        &home,
        &["config", "set", "openai.base_url", CLOSED_PORT],
        &[],
    );
    let stored = openai(&json(&config_cli(&home, &status_args, &[])));
    assert_eq!(stored["base_url"], CLOSED_PORT);
    let source = stored["base_url_source"]
        .as_str()
        .expect("a redirected provider has a source");
    assert!(
        source.ends_with("strata/config.toml"),
        "a config-backed base URL names the file: {source}"
    );

    // Exported: the environment wins over the file and names its variable.
    let exported = openai(&json(&config_cli(
        &home,
        &status_args,
        &[("OPENAI_BASE_URL", "http://127.0.0.1:2/v1")],
    )));
    assert_eq!(exported["base_url"], "http://127.0.0.1:2/v1");
    assert_eq!(exported["base_url_source"], "OPENAI_BASE_URL");

    // The stored base URL is where a call goes. A fake key gets the call
    // past the key check; the closed port proves nothing left the machine.
    config_cli(
        &home,
        &["config", "set", "openai.api_key", "sk-fake-never-sent"],
        &[],
    );
    let output = config_cli(
        &home,
        &["--json", "inference", "generate", "openai:gpt-test", "hi"],
        &[],
    );
    assert!(
        !output.status.success(),
        "a closed port cannot answer: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let error: Value = serde_json::from_slice(&output.stderr).expect("error is JSON on stderr");
    assert_eq!(error["error"]["code"], "inference.provider_unavailable");
    assert_eq!(error["error"]["class"], "unavailable");
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
