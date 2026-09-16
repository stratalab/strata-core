//! CLI inference deterministic-verb behavior (TCP3.11c).
//!
//! The inference *compute* verbs (generate/embed/rank/tokenize) need a model
//! and a provider key, so they are not hermetic and are covered elsewhere. But
//! four inference verbs are pure functions of the static model catalog and
//! provider facts — `models list`, `models local`, `cache-status`, and
//! `capability` — and had no CLI integration coverage. Run under a temp `HOME`
//! (so no locally-downloaded model leaks in) they are fully deterministic.
//!
//! `status` joins them for the fields #3423 added, under a temp config
//! directory as well: what it says about the user config file is a function of
//! that file alone.

#![deny(unsafe_code)]

use std::path::Path;
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;

/// Runs `strata --db <db> --json inference <args>` under a hermetic HOME.
fn inference(home: &TempDir, db: &Path, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_strata"))
        .arg("--db")
        .arg(db)
        .arg("--json")
        .arg("inference")
        .args(args)
        .env("HOME", home.path())
        .env_remove("STRATA_HOME")
        .env_remove("STRATA_DB")
        .output()
        .expect("run strata binary");
    assert!(
        output.status.success(),
        "inference {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout is JSON")
}

fn db(home: &TempDir) -> std::path::PathBuf {
    home.path().join("db")
}

/// Finds a catalog model by name in an `inference_models` page.
fn model<'a>(page: &'a Value, name: &str) -> &'a Value {
    page["data"]["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|m| m["name"] == name)
        .unwrap_or_else(|| panic!("model {name} not in catalog"))
}

#[test]
fn models_list_returns_the_static_catalog_with_nothing_local() {
    let home = tempfile::tempdir().expect("temp home");
    let page = inference(&home, &db(&home), &["models", "list"]);
    assert_eq!(page["type"], "inference_models");

    // The catalog is static: known models with fixed task/architecture facts.
    let minilm = model(&page, "miniLM");
    assert_eq!(minilm["task"], "embed");
    assert_eq!(minilm["architecture"], "bert");
    assert_eq!(minilm["embedding_dim"], 384);
    assert_eq!(model(&page, "tinyllama")["task"], "generate");
    assert_eq!(model(&page, "bge-m3")["task"], "embed");

    // Under a hermetic HOME nothing is downloaded, so nothing reports local.
    let any_local = page["data"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["is_local"] == true);
    assert!(!any_local, "a model reported is_local under a temp HOME");
}

#[test]
fn models_local_is_empty_without_downloads() {
    let home = tempfile::tempdir().expect("temp home");
    let page = inference(&home, &db(&home), &["models", "local"]);
    assert_eq!(page["type"], "inference_models");
    assert_eq!(page["data"]["items"], serde_json::json!([]));
}

#[test]
fn cache_status_reports_empty_pools_when_nothing_is_loaded() {
    let home = tempfile::tempdir().expect("temp home");
    let status = inference(&home, &db(&home), &["cache-status"]);
    assert_eq!(status["type"], "inference_cache_status");
    assert_eq!(status["data"]["embedding_models"], serde_json::json!([]));
    assert_eq!(status["data"]["generation_models"], serde_json::json!([]));
    assert_eq!(status["data"]["ranking_models"], serde_json::json!([]));
}

#[test]
fn capability_reports_static_facts_for_cloud_and_local_specs() {
    let home = tempfile::tempdir().expect("temp home");
    let db = db(&home);

    // A cloud spec: provider facts are static and need no key to inspect.
    let cloud = inference(&home, &db, &["capability", "openai:gpt-4o-mini"]);
    assert_eq!(cloud["type"], "inference_capability");
    assert_eq!(cloud["data"]["provider"], "openai");
    assert_eq!(cloud["data"]["model"], "gpt-4o-mini");
    assert_eq!(cloud["data"]["requires_api_key"], true);
    assert_eq!(cloud["data"]["requires_network"], true);
    assert_eq!(cloud["data"]["can_generate"], true);
    assert_eq!(cloud["data"]["supports_tools"], true);

    // A local embedding spec: no key, no network, fixed dimension — and, in this
    // build, nothing it can actually do.
    //
    // #3124 renegotiated what `can_*` means. It used to report what the MODEL
    // supports, which made this test assert `can_embed: true` in a test binary
    // built without `inference-local` — pinning the exact contradiction the
    // issue reported, where `can_embed: true` sat beside
    // `provider_feature_enabled: false`. It now reports what THIS BINARY can do,
    // so the flags follow the feature and the model's own shape stays visible
    // through `embedding_dim` and the catalog's task.
    let local = inference(&home, &db, &["capability", "miniLM"]);
    assert_eq!(local["data"]["provider"], "local");
    assert_eq!(local["data"]["requires_api_key"], false);
    assert_eq!(local["data"]["requires_network"], false);
    assert_eq!(
        local["data"]["can_embed"],
        cfg!(feature = "inference-local")
    );
    assert_eq!(
        local["data"]["can_tokenize"],
        cfg!(feature = "inference-local")
    );
    assert_eq!(
        local["data"]["provider_feature_enabled"],
        cfg!(feature = "inference-local"),
        "the two fields must now agree rather than contradict"
    );
    assert_eq!(local["data"]["embedding_dim"], 384);
}

/// Where the binary will look for the user config file, under `env`.
///
/// Asked of the binary rather than assembled here: `dirs::config_dir()` is
/// `$XDG_CONFIG_HOME` on Linux and `$HOME/Library/Application Support` on
/// macOS, so a path written by hand is right on one platform and invisible on
/// the other — the test would then assert `absent` and pass for the wrong
/// reason.
fn config_path(env: &[(&str, &Path)]) -> std::path::PathBuf {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_strata"));
    cmd.arg("--json").arg("config").arg("path");
    for (key, value) in env {
        cmd.env(key, value);
    }
    let output = cmd.output().expect("run strata binary");
    let reported: Value = serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    std::path::PathBuf::from(reported["path"].as_str().expect("a config path"))
}

/// Runs `strata --db <db> inference status` under `env`, in JSON and human
/// form, returning both.
fn status(env: &[(&str, &Path)], db: &Path) -> (Value, String) {
    let run = |json: bool| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_strata"));
        cmd.arg("--db").arg(db);
        if json {
            cmd.arg("--json");
        }
        cmd.arg("inference").arg("status");
        for (key, value) in env {
            cmd.env(key, value);
        }
        let output = cmd
            .env_remove("STRATA_HOME")
            .env_remove("STRATA_DB")
            .output()
            .expect("run strata binary");
        assert!(
            output.status.success(),
            "inference status failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("stdout is UTF-8")
    };
    (
        serde_json::from_str(&run(true)).expect("stdout is JSON"),
        run(false),
    )
}

/// The question #3423 is about: a provider whose stored key cannot be reached
/// must not read like one that was never configured.
///
/// Both halves are asserted against the same file, because the finding is a
/// *contrast*: every settings read folds a failure into "no value", so the
/// provider row is byte-identical in the two cases. `config_file` is the only
/// thing that separates them, and the remedies are opposite — one says set a
/// key, the other says a key is already there and the file is broken.
#[test]
fn status_tells_an_unreachable_stored_key_from_a_key_never_set() {
    let home = tempfile::tempdir().expect("temp home");
    let config_home = tempfile::tempdir().expect("scratch config home");
    let env: &[(&str, &Path)] = &[
        ("HOME", home.path()),
        ("XDG_CONFIG_HOME", config_home.path()),
    ];
    let db = db(&home);
    let path = config_path(env);
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("config dir");

    // No file: the default install.
    let (json, human) = status(env, &db);
    assert_eq!(json["data"]["config_file"]["state"], "absent");
    assert!(
        !human.contains("config\t"),
        "an absent file is the default install, not news: {human}"
    );

    // A key stored in a file that parses. This is the control that makes the
    // next step mean something: without it, `key_present: false` below could
    // be because nothing was ever stored.
    std::fs::write(
        &path,
        "[providers.openai]\napi_key = \"sk-not-a-real-key\"\n",
    )
    .expect("write config");
    let (json, human) = status(env, &db);
    assert_eq!(json["data"]["config_file"]["state"], "readable");
    assert_eq!(
        json["data"]["config_file"]["path"],
        Value::String(path.display().to_string())
    );
    assert_eq!(
        openai(&json)["key_present"],
        Value::Bool(true),
        "the stored key must reach the runtime, or the next step proves nothing"
    );
    assert!(human.contains("config\t"), "{human}");

    // The same key, in a file that no longer parses.
    std::fs::write(&path, "[providers.openai]\napi_key = [\"sk-\n").expect("write malformed");
    let (json, human) = status(env, &db);
    assert_eq!(json["data"]["config_file"]["state"], "malformed");
    assert_eq!(
        openai(&json)["key_present"],
        Value::Bool(false),
        "the stored key is unreachable -- which is exactly why the state must be reported"
    );
    assert_eq!(openai(&json)["key_source"], Value::Null);
    assert!(
        human.contains("not valid TOML"),
        "the human form must say the file is broken rather than only that no key is set: {human}"
    );

    // A directory where the file belongs: readable as a path, not as bytes.
    // The fourth state, and the fourth line the human form can print.
    std::fs::remove_file(&path).expect("clear the file");
    std::fs::create_dir_all(&path).expect("directory in the file's place");
    let (unreadable, unreadable_human) = status(env, &db);
    assert_eq!(unreadable["data"]["config_file"]["state"], "unreadable");
    assert!(
        unreadable_human.contains("cannot be read"),
        "{unreadable_human}"
    );

    // And the key itself never rides out on any of it (Rule 31).
    assert!(!human.contains("sk-not-a-real-key"), "{human}");
    assert!(
        !serde_json::to_string(&json)
            .expect("serializes")
            .contains("sk-not-a-real-key"),
        "a key must never reach the wire"
    );
}

/// The openai row of an `inference status` response.
fn openai(status: &Value) -> &Value {
    status["data"]["providers"]
        .as_array()
        .expect("providers")
        .iter()
        .find(|provider| provider["provider"] == "openai")
        .expect("openai is a catalogued provider")
}
