//! CLI `doctor` behavior (TCP3.16).
//!
//! `crates/cli/src/doctor.rs` had zero tests, so its four environment/database
//! precondition codes were carried on the error-code guard's allowlist as
//! "deferred". The Phase 3 exit-gate audit's honest re-examination showed that
//! was wrong: all four are reachable hermetically by perturbing one environment
//! axis (`HOME`/`STRATA_HOME`/`PATH`/`--db`) with the same real-binary pattern
//! the 3.11 CLI family tests use. This closes that gap and drops the four codes
//! off the allowlist.

#![deny(unsafe_code)]

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_strata")
}

fn bin_dir() -> PathBuf {
    PathBuf::from(bin())
        .parent()
        .expect("binary directory")
        .to_path_buf()
}

/// Runs `strata --json [--db <db>] doctor` with a controlled environment,
/// returning the parsed report and exit code. The base environment is healthy —
/// the binary's own directory on `PATH`, no stray `STRATA_HOME`/`STRATA_DB`,
/// and an empty config directory — so each test perturbs exactly one axis to
/// trigger exactly one issue.
///
/// `XDG_CONFIG_HOME` is pointed at a scratch directory rather than REMOVED.
/// Removing it does not neutralise the environment: the config path comes from
/// `dirs::config_dir()`, which falls back to `$HOME/.config` when the variable
/// is absent, so removing it guaranteed the developer's real
/// `~/.config/strata/config.toml` was read. A machine that had ever run
/// `strata config set` saw a stored provider key here and
/// `a_default_install_reports_inference_without_calling_it_broken` failed —
/// permanently, and only locally, since CI has no such file (#3398).
fn run_doctor(env: &[(&str, Option<&OsStr>)], db: Option<&Path>) -> (Value, i32) {
    let config_home = tempfile::tempdir().expect("scratch config home");
    let mut cmd = Command::new(bin());
    cmd.arg("--json");
    if let Some(db) = db {
        // `--db` is a global flag, parsed before the subcommand.
        cmd.arg("--db").arg(db);
    }
    cmd.arg("doctor")
        .env("PATH", bin_dir())
        .env_remove("STRATA_HOME")
        .env_remove("STRATA_DB")
        .env("XDG_CONFIG_HOME", config_home.path());
    for (key, value) in env {
        match value {
            Some(value) => cmd.env(key, value),
            None => cmd.env_remove(key),
        };
    }
    let output = cmd.output().expect("run strata binary");
    let report = serde_json::from_slice(&output.stdout).expect("doctor report is JSON on stdout");
    (report, output.status.code().expect("exit code"))
}

fn issue_codes(report: &Value) -> Vec<String> {
    report["data"]["issues"]
        .as_array()
        .expect("issues array")
        .iter()
        .map(|issue| issue["code"].as_str().expect("issue code").to_owned())
        .collect()
}

#[test]
fn a_healthy_environment_reports_no_issues_and_exits_zero() {
    let home = tempfile::tempdir().expect("temp home");
    let (report, code) = run_doctor(&[("HOME", Some(home.path().as_os_str()))], None);
    assert_eq!(report["type"], "doctor");
    assert_eq!(report["data"]["path_ok"], true);
    assert!(issue_codes(&report).is_empty(), "healthy env has no issues");
    assert_eq!(code, 0);
}

#[test]
fn a_missing_database_target_reports_the_database_path_code() {
    let home = tempfile::tempdir().expect("temp home");
    let missing = home.path().join("no-such-db");
    let (report, code) = run_doctor(&[("HOME", Some(home.path().as_os_str()))], Some(&missing));
    assert!(issue_codes(&report).contains(&"not_found.cli.database_path".to_owned()));
    assert_eq!(report["data"]["database"]["exists"], false);
    assert_eq!(code, 1);
}

#[test]
fn doctor_probes_a_live_owned_database_by_brokering() {
    use std::io::{BufRead, Write};
    use std::process::Stdio;

    let home = tempfile::tempdir().expect("temp home");
    let db_dir = tempfile::tempdir().expect("db dir");
    let db = db_dir.path().join("db");

    // Seed the store so it exists durably.
    let seed = Command::new(bin())
        .arg("--db")
        .arg(&db)
        .args(["kv", "put", "seed", "1"])
        .env("PATH", bin_dir())
        .env_remove("STRATA_DB")
        .output()
        .expect("seed the database");
    assert!(
        seed.status.success(),
        "seed failed: {}",
        String::from_utf8_lossy(&seed.stderr)
    );

    // A live process holds AND hosts the store (a raw hold would only refuse
    // the doctor; hosting lets it broker in).
    let mut holder = Command::new(bin())
        .arg("--db")
        .arg(&db)
        .args(["--ipc", "host"])
        .env("PATH", bin_dir())
        .env_remove("STRATA_DB")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
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

    // Doctor brokers to the owner and inspects the DB instead of failing on the
    // writer lock.
    let (report, code) = run_doctor(&[("HOME", Some(home.path().as_os_str()))], Some(&db));
    assert_eq!(report["data"]["database"]["exists"], true);
    assert_eq!(
        report["data"]["database"]["open_ok"], true,
        "the brokered probe succeeds: {report}"
    );
    assert!(
        !issue_codes(&report).contains(&"unavailable.engine.persistence".to_owned()),
        "brokering means no writer-lock refusal: {report}"
    );
    assert_eq!(code, 0, "a database it can probe is not an issue");

    drop(holder.stdin.take());
    let _ = holder.wait();
}

#[test]
fn a_non_directory_strata_home_reports_the_home_not_directory_code() {
    let home = tempfile::tempdir().expect("temp home");
    let file = home.path().join("strata-home-is-a-file");
    std::fs::write(&file, b"not a directory").expect("write file");
    let (report, code) = run_doctor(
        &[
            ("HOME", Some(home.path().as_os_str())),
            ("STRATA_HOME", Some(file.as_os_str())),
        ],
        None,
    );
    assert!(issue_codes(&report).contains(&"failed_precondition.cli.home_not_directory".to_owned()));
    assert_eq!(code, 1);
}

#[test]
fn an_unresolvable_home_reports_the_home_unresolved_code() {
    // Neither STRATA_HOME nor HOME set: `strata_home()` cannot resolve.
    let (report, code) = run_doctor(&[("HOME", None), ("STRATA_HOME", None)], None);
    assert!(issue_codes(&report).contains(&"failed_precondition.cli.home_unresolved".to_owned()));
    assert_eq!(report["data"]["home"], Value::Null);
    assert_eq!(code, 1);
}

#[test]
fn a_binary_off_path_reports_the_binary_not_on_path_code() {
    let home = tempfile::tempdir().expect("temp home");
    // A PATH pointing at a directory that does not contain the strata binary.
    let empty = tempfile::tempdir().expect("empty path dir");
    let (report, code) = run_doctor(
        &[
            ("HOME", Some(home.path().as_os_str())),
            ("PATH", Some(empty.path().as_os_str())),
        ],
        None,
    );
    assert!(issue_codes(&report).contains(&"failed_precondition.cli.binary_not_on_path".to_owned()));
    assert_eq!(report["data"]["path_ok"], false);
    assert_eq!(code, 1);
}

// ---------------------------------------------------------------------------
// Inference readiness (D11). `doctor` is the one place that answers "will
// inference work" without needing a database — `inference status` is an
// executor command and requires one.
// ---------------------------------------------------------------------------

/// A default install reports inference facts and stays **green**.
///
/// This is the assertion that matters most here. `doctor` exits non-zero on any
/// issue and `install.sh` ends with it, so a normal installation — no API key,
/// no local execution, released binary — must not be called broken. Having no
/// key is a choice; shipping without local models is the design.
#[test]
fn a_default_install_reports_inference_without_calling_it_broken() {
    let (report, code) = run_doctor(
        &[
            ("OPENAI_API_KEY", None),
            ("ANTHROPIC_API_KEY", None),
            ("GOOGLE_API_KEY", None),
        ],
        None,
    );

    let inference = &report["data"]["inference"];
    assert!(
        inference["models_dir"].is_string(),
        "the shared model directory is reported: {inference}"
    );
    assert!(inference["local_execution"].is_boolean());
    assert_eq!(
        inference["ready_providers"],
        serde_json::json!([]),
        "no keys set, so nothing is ready"
    );

    assert_eq!(
        code, 0,
        "a keyless install is not a broken install: {report}"
    );
    assert!(
        !issue_codes(&report)
            .iter()
            .any(|code| code.contains("inference")),
        "no inference issue on a default install: {report}"
    );
}

/// A key that is set to nothing IS reported, because it fails at call time with
/// a message about the provider rather than about the variable.
#[test]
fn an_empty_api_key_variable_is_reported_as_an_issue() {
    let (report, code) = run_doctor(&[("OPENAI_API_KEY", Some(OsStr::new("   ")))], None);

    assert!(
        issue_codes(&report).contains(&"failed_precondition.cli.inference_key_empty".to_owned()),
        "an empty key variable is a real misconfiguration: {report}"
    );
    assert_ne!(code, 0, "doctor exits non-zero when it finds an issue");
}

/// A key in the config file is found, not only one in the environment.
///
/// Every other readiness assertion here supplies the key through an environment
/// variable, so nothing covered the stored-key path at all — which is how
/// `run_doctor` could read the developer's real config for as long as it did
/// without any test being *about* config reading (#3398).
///
/// This is also the positive half of the isolation fix: it proves the harness
/// can point `doctor` at a config and have it read, so the default case
/// reporting nothing ready means "no config", not "config never consulted".
#[test]
fn a_stored_provider_key_is_reported_ready() {
    let config_home = tempfile::tempdir().expect("scratch config home");
    let strata_dir = config_home.path().join("strata");
    std::fs::create_dir_all(&strata_dir).expect("config dir");
    std::fs::write(
        strata_dir.join("config.toml"),
        "[providers.openai]\napi_key = \"sk-not-a-real-key\"\n",
    )
    .expect("write config");

    let (report, _) = run_doctor(
        &[
            ("OPENAI_API_KEY", None),
            ("ANTHROPIC_API_KEY", None),
            ("GOOGLE_API_KEY", None),
            ("XDG_CONFIG_HOME", Some(config_home.path().as_os_str())),
        ],
        None,
    );

    let ready = report["data"]["inference"]["ready_providers"]
        .as_array()
        .expect("ready providers array");
    assert!(
        ready.iter().any(|provider| provider == "openai"),
        "a key stored in the config file must count as ready: {report}"
    );
}

/// A key with a value makes its provider ready, and keeps doctor green.
#[test]
fn a_provider_with_a_key_reports_ready() {
    let (report, code) = run_doctor(
        &[
            ("OPENAI_API_KEY", Some(OsStr::new("sk-not-a-real-key"))),
            ("ANTHROPIC_API_KEY", None),
            ("GOOGLE_API_KEY", None),
        ],
        None,
    );

    let ready = report["data"]["inference"]["ready_providers"]
        .as_array()
        .expect("ready providers array");
    assert!(
        ready.iter().any(|provider| provider == "openai"),
        "a provider with a key is ready: {report}"
    );
    // Readiness is about configuration, not about the key being valid — doctor
    // does not call the provider, so it cannot and must not claim more.
    assert_eq!(code, 0, "{report}");
}

/// A model directory that is a file, not a directory, is reported.
///
/// `STRATA_MODELS_DIR` can point anywhere, and a file there means every model
/// operation fails later with a filesystem error that does not name the cause.
#[test]
fn a_non_directory_models_dir_reports_the_models_not_directory_code() {
    let home = tempfile::tempdir().expect("tempdir");
    let file = home.path().join("models-but-a-file");
    std::fs::write(&file, b"not a directory").expect("write the blocking file");

    let (report, code) = run_doctor(&[("STRATA_MODELS_DIR", Some(file.as_os_str()))], None);

    assert!(
        issue_codes(&report)
            .contains(&"failed_precondition.cli.inference_models_not_directory".to_owned()),
        "a file where the model directory should be is a real fault: {report}"
    );
    assert_ne!(code, 0);
}
