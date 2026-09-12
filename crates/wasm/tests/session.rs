//! Browser-boundary session tests, executed on wasm32-unknown-unknown: each
//! case drives the full executor → engine → storage cache stack through the
//! serialized command surface — the conformance plan's "cache substrate runs
//! on wasm" bar, proven by execution rather than compilation.

#![cfg(target_arch = "wasm32")]

use strata_wasm::{engine_version, StrataSession};
use wasm_bindgen_test::wasm_bindgen_test;

fn execute(session: &mut StrataSession, command: &str) -> serde_json::Value {
    let raw = session
        .execute(command)
        .expect("valid command executes without throwing");
    serde_json::from_str(&raw).expect("envelope is valid JSON")
}

// base64: "greeting" / "hello" / "flag" / "on"
const KEY_GREETING: &str = "Z3JlZXRpbmc=";
const VALUE_HELLO: &str = "aGVsbG8=";
const KEY_FLAG: &str = "ZmxhZw==";
const VALUE_ON: &str = "b24=";

#[wasm_bindgen_test]
fn kv_round_trip_through_the_serialized_surface() {
    let mut session = StrataSession::new().expect("session opens");
    let put = execute(
        &mut session,
        &format!(r#"{{"type":"kv_put","key":"{KEY_GREETING}","value":"{VALUE_HELLO}"}}"#),
    );
    assert!(put.get("error").is_none(), "put must not error: {put}");
    let get = execute(
        &mut session,
        &format!(r#"{{"type":"kv_get","key":"{KEY_GREETING}"}}"#),
    );
    assert!(
        get.to_string().contains(VALUE_HELLO),
        "get must return the stored value: {get}"
    );
}

#[wasm_bindgen_test]
fn invalid_command_json_throws_with_a_teaching_message() {
    let mut session = StrataSession::new().expect("session opens");
    assert!(
        session.execute("{not json").is_err(),
        "malformed JSON must throw"
    );
    assert!(
        session
            .execute(r#"{"type":"kv_levitate","key":"YQ=="}"#)
            .is_err(),
        "unknown command type must throw"
    );
}

#[wasm_bindgen_test]
fn executed_command_failures_are_error_envelopes_not_throws() {
    let mut session = StrataSession::new().expect("session opens");
    // A command that deserializes fine but fails at execution (reserved
    // branch name) must come back as an error envelope, not a throw —
    // throws are reserved for malformed command JSON.
    let envelope = execute(
        &mut session,
        r#"{"type":"branch_create","branch":"_system_"}"#,
    );
    assert!(
        envelope.get("error").is_some(),
        "executed failures surface as envelopes: {envelope}"
    );
}

#[wasm_bindgen_test]
fn branch_scoping_isolates_writes_in_the_browser_session() {
    let mut session = StrataSession::new().expect("session opens");
    assert_eq!(session.branch(), "default");
    let created = execute(&mut session, r#"{"type":"branch_create","branch":"dev"}"#);
    assert!(created.get("error").is_none(), "branch create: {created}");
    session.set_branch("dev").expect("valid branch");
    let put = execute(
        &mut session,
        &format!(r#"{{"type":"kv_put","key":"{KEY_FLAG}","value":"{VALUE_ON}"}}"#),
    );
    assert!(put.get("error").is_none(), "dev put: {put}");

    session.set_branch("default").expect("valid branch");
    let get = execute(
        &mut session,
        &format!(r#"{{"type":"kv_get","key":"{KEY_FLAG}"}}"#),
    );
    assert!(
        !get.to_string().contains(VALUE_ON),
        "default branch must not see dev writes: {get}"
    );

    assert!(
        session.set_branch("_system_").is_err(),
        "reserved branch names must throw"
    );
}

#[wasm_bindgen_test]
fn space_scoping_isolates_the_same_key_across_spaces() {
    // Spaces are the second isolation axis (branch is the first). The browser
    // surface exposes them via space()/setSpace(), and nothing tested them.
    let mut session = StrataSession::new().expect("session opens");
    assert_eq!(session.space(), "default");

    // Write the same key in two spaces with different values.
    session.set_space("docs").expect("valid space");
    let put_docs = execute(
        &mut session,
        &format!(r#"{{"type":"kv_put","key":"{KEY_FLAG}","value":"{VALUE_ON}"}}"#),
    );
    assert!(put_docs.get("error").is_none(), "docs put: {put_docs}");

    session.set_space("default").expect("valid space");
    let get_default = execute(
        &mut session,
        &format!(r#"{{"type":"kv_get","key":"{KEY_FLAG}"}}"#),
    );
    // base64("on") written under `docs` must not be visible under `default`.
    assert!(
        !get_default.to_string().contains(VALUE_ON),
        "default space must not see docs writes: {get_default}"
    );

    // The docs write is still there under its own space.
    session.set_space("docs").expect("valid space");
    let get_docs = execute(
        &mut session,
        &format!(r#"{{"type":"kv_get","key":"{KEY_FLAG}"}}"#),
    );
    assert!(
        get_docs.to_string().contains(VALUE_ON),
        "docs space must still see its own write: {get_docs}"
    );
}

#[wasm_bindgen_test]
fn invalid_space_name_throws() {
    let mut session = StrataSession::new().expect("session opens");
    assert!(
        session.set_space("_system_").is_err(),
        "reserved space names must throw"
    );
}

#[wasm_bindgen_test]
fn a_closed_session_reports_the_close_and_then_refuses_work() {
    let mut session = StrataSession::new().expect("session opens");
    let put = execute(
        &mut session,
        &format!(r#"{{"type":"kv_put","key":"{KEY_GREETING}","value":"{VALUE_HELLO}"}}"#),
    );
    assert!(put.get("error").is_none(), "pre-close put: {put}");

    session.close().expect("close succeeds");

    // After close the handle is unusable: executed commands come back as
    // error envelopes (the runtime is closed), not successful outputs.
    let after = execute(
        &mut session,
        &format!(r#"{{"type":"kv_get","key":"{KEY_GREETING}"}}"#),
    );
    assert!(
        after.get("error").is_some(),
        "a closed session must refuse further work: {after}"
    );
}

fn execute_cli(session: &mut StrataSession, line: &str) -> String {
    session
        .execute_cli(line)
        .expect("a CLI line renders without throwing")
}

#[wasm_bindgen_test]
fn execute_cli_renders_the_human_default() {
    let mut session = StrataSession::new().expect("session opens");
    assert_eq!(
        execute_cli(&mut session, "kv put greeting hello"),
        "created greeting\n"
    );
    assert_eq!(execute_cli(&mut session, "kv get greeting"), "hello\n");
    // A missed write is its feedback line, the way the binary's stderr
    // shows it after an empty stdout.
    assert_eq!(
        execute_cli(&mut session, "kv delete nope"),
        "no such key: nope\n"
    );
}

#[wasm_bindgen_test]
fn execute_cli_honours_json() {
    // #3312: `--json` used to be accepted and ignored, so the playground
    // rendered the human line where the binary prints the wire envelope.
    let mut session = StrataSession::new().expect("session opens");
    execute_cli(&mut session, "kv put greeting hello");
    let line = execute_cli(&mut session, "--json kv get greeting");
    assert!(
        line.ends_with('\n'),
        "newline-terminated like stdout: {line:?}"
    );
    let envelope: serde_json::Value = serde_json::from_str(&line).expect("compact JSON");
    assert_eq!(envelope["type"], "kv_versioned_value");
    assert_eq!(envelope["data"]["value"]["value"], VALUE_HELLO);
}

#[wasm_bindgen_test]
fn execute_cli_honours_raw() {
    let mut session = StrataSession::new().expect("session opens");
    execute_cli(&mut session, "kv put greeting hello");
    assert_eq!(
        execute_cli(&mut session, "--raw kv get greeting"),
        "hello\n"
    );
    assert_eq!(execute_cli(&mut session, "--raw kv get missing"), "");
}

#[wasm_bindgen_test]
fn execute_cli_renders_failures_in_the_chosen_format() {
    let mut session = StrataSession::new().expect("session opens");
    let human = execute_cli(&mut session, "branch get nope");
    assert!(
        human.starts_with("not_found.engine.branch:") && human.ends_with('\n'),
        "human error line: {human:?}"
    );
    let json = execute_cli(&mut session, "--json branch get nope");
    let envelope: serde_json::Value = serde_json::from_str(&json).expect("error envelope");
    assert_eq!(envelope["error"]["code"], "not_found.engine.branch");
}

#[wasm_bindgen_test]
fn execute_cli_refuses_session_arguments_on_a_line() {
    // #3327: a session argument on a playground line is refused by name, and
    // the command behind it does not run against the browser's database.
    let mut session = StrataSession::new().expect("session opens");
    execute_cli(&mut session, "kv put greeting hello");
    let refused = execute_cli(&mut session, "--db /elsewhere kv get greeting");
    assert!(refused.contains("`--db`"), "{refused}");
    let refused = execute_cli(&mut session, "--read-only kv put greeting changed");
    assert!(refused.contains("`--read-only`"), "{refused}");
    assert_eq!(
        execute_cli(&mut session, "--raw kv get greeting"),
        "hello\n",
        "the refused write must not have happened"
    );
}

#[wasm_bindgen_test]
fn engine_version_reports_a_non_empty_semver() {
    let version = engine_version();
    assert!(!version.is_empty(), "version must be reported");
    // Compiled-in crate version: at least `major.minor`.
    assert!(
        version.split('.').count() >= 2,
        "version looks like semver: {version}"
    );
}
