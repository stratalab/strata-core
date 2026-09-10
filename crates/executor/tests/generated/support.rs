//! Hand-written assertion helpers behind the TCP4.1 generated conformance
//! suite (`conformance_cases.rs`, emitted by `strata-idl generate-tests`).
//! The generated tests are thin calls into these functions, so the reviewable
//! logic lives here and the generated volume stays declarative.

use std::path::{Path, PathBuf};

use serde_json::Value;
use strata_executor::{Command, Executor, Output};

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn load(relative: &str) -> Value {
    let path = fixtures_root().join(relative);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read fixture {}: {err}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|err| panic!("parse fixture {}: {err}", path.display()))
}

/// The request fixture parses as [`Command`], and one serialize→parse cycle is
/// a serialization fixed point — an asymmetric serde impl (a field renamed on
/// one side, an enum arm missing on re-parse, a lossy conversion) breaks the
/// idempotence even when a one-directional replay looks healthy.
pub(crate) fn request_roundtrip_idempotent(request: &str) {
    roundtrip_idempotent::<Command>(request, "Command");
}

/// Response-side twin of [`request_roundtrip_idempotent`] over [`Output`].
pub(crate) fn response_roundtrip_idempotent(response: &str) {
    roundtrip_idempotent::<Output>(response, "Output");
}

fn roundtrip_idempotent<T>(relative: &str, label: &str)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let fixture = load(relative);
    let first: T = serde_json::from_value(fixture.clone())
        .unwrap_or_else(|err| panic!("{relative}: fixture must parse as {label}: {err}"));
    let value_one = serde_json::to_value(&first)
        .unwrap_or_else(|err| panic!("{relative}: {label} must serialize: {err}"));
    let second: T = serde_json::from_value(value_one.clone())
        .unwrap_or_else(|err| panic!("{relative}: serialized {label} must re-parse: {err}"));
    let value_two = serde_json::to_value(&second)
        .unwrap_or_else(|err| panic!("{relative}: re-parsed {label} must serialize: {err}"));
    assert_eq!(
        value_one, value_two,
        "{relative}: {label} wire round-trip is not idempotent"
    );
}

/// Injects an unknown key at each schema-closed object site of the request
/// fixture and asserts the [`Command`] parse rejects it — the recursive form
/// of the `deny_unknown_fields` contract (#2705/#2696 class). Sites are
/// computed at generation time from the derived schema, so open maps and
/// user-payload subtrees are never asserted against.
pub(crate) fn unknown_keys_rejected(request: &str, sites: &[&str]) {
    let fixture = load(request);
    // Guard against vacuous failure: the unmutated fixture must parse.
    let _: Command = serde_json::from_value(fixture.clone())
        .unwrap_or_else(|err| panic!("{request}: fixture must parse before mutation: {err}"));
    for site in sites {
        let mut mutated = fixture.clone();
        let target = mutated
            .pointer_mut(site)
            .unwrap_or_else(|| panic!("{request}: injection site {site:?} must exist"));
        target
            .as_object_mut()
            .unwrap_or_else(|| panic!("{request}: injection site {site:?} must be an object"))
            .insert("__tcp41_unknown_key__".to_owned(), Value::Bool(true));
        assert!(
            serde_json::from_value::<Command>(mutated).is_err(),
            "{request}: unknown key at {site:?} was accepted by a closed object"
        );
    }
}

/// Replays an authored error case and asserts the five stable envelope fields
/// (`code`, `class`, `retry_policy`, `retryable`, `commit_outcome`) match the pinned
/// fixture — the per-command form of the TCP3.8a contract.
pub(crate) fn error_case_envelope_matches(setup: &[&str], request: &str, expected_error: &str) {
    let mut executor = open_executor();
    run_setup(&mut executor, setup);
    let command = command_fixture(request);
    let Err(error) = executor.execute(command) else {
        panic!("{request}: execution must fail for an error case")
    };
    let actual = serde_json::to_value(error.status()).expect("serialize error status");
    let expected = load(expected_error);
    for field in [
        "code",
        "class",
        "retry_policy",
        "retryable",
        "commit_outcome",
    ] {
        assert_eq!(
            actual.get(field),
            expected.get(field),
            "{request}: error envelope field `{field}` diverges from {expected_error}"
        );
    }
}

/// Replays the command and asserts the observed output `type` tag is one the
/// command's response fixtures declare (#2596: an output variant the IDL does
/// not document must fail loudly, not ship silently).
pub(crate) fn replay_observes_declared(setup: &[&str], request: &str, declared_tags: &[&str]) {
    let mut executor = open_executor();
    run_setup(&mut executor, setup);
    let command = command_fixture(request);
    let output = executor
        .execute(command)
        .unwrap_or_else(|err| panic!("{request}: replay must succeed: {err}"));
    let value = serde_json::to_value(&output).expect("serialize output");
    let observed = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{request}: output must carry a `type` tag"));
    assert!(
        declared_tags.contains(&observed),
        "{request}: observed output tag `{observed}` is not declared by any response fixture \
         (declared: {declared_tags:?})"
    );
}

fn command_fixture(relative: &str) -> Command {
    serde_json::from_value(load(relative))
        .unwrap_or_else(|err| panic!("{relative}: fixture must parse as Command: {err}"))
}

fn run_setup(executor: &mut Executor, setup: &[&str]) {
    for relative in setup {
        let command = command_fixture(relative);
        executor
            .execute(command)
            .unwrap_or_else(|err| panic!("{relative}: setup must succeed: {err}"));
    }
}

/// A scratch cache executor with the deterministic testkit fake as its
/// inference runtime, so everything that reaches inference — the
/// `inference.*` commands and `vector --text` — replays against the same
/// world `strata-idl verify-fixtures` blessed the fixtures in.
fn open_executor() -> Executor {
    Executor::open_cache()
        .expect("open scratch cache executor")
        .with_inference_runtime(strata_inference::testkit::FakeInferenceService::new())
}
