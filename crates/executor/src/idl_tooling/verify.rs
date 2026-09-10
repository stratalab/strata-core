//! Fixture-behavior verification (drift guard #4).
//!
//! Schema validation alone cannot catch a frozen lie: a response fixture
//! can be schema-valid while pinning facts a real run never produces (the
//! `kv.scan` `has_more` finding, DSGN-2). This guard executes every request
//! fixture against a scratch cache executor — after replaying the entry's
//! declared `setup` fixtures — and diffs the serialized output against the
//! checked-in response fixture. With `update`, actual outputs are written
//! back (blessing), to be reviewed like any other diff.

use std::path::{Path, PathBuf};

use serde_json::Value;

use super::{
    invalid, CommandIndex, ErrorFixtureCase, FixtureCase, IdlError, ResolvedCommand, Result,
};
use crate::executor::Executor;
use crate::Command;

/// Whether this build carries the replay lane's inference runtime: the testkit
/// fake, injected into every scratch executor by [`open_executor`]. Without
/// it, what reaches inference is not replayed here — the `inference.*`
/// commands, and any error case pinning an `inference.*` code (`vector
/// --text` on a collection whose model the catalog does not know) — and the
/// coverage ratchet does not judge those codes. The CI drift-gate lane builds
/// both features and judges the whole corpus.
const INFERENCE_REPLAYABLE: bool = cfg!(all(feature = "inference", feature = "testkit"));

/// The area of an error code: `inference` for `inference.unknown_model`, the
/// whole code when it has no `.` to split at.
fn code_area(code: &str) -> &str {
    code.split_once('.').map_or(code, |(area, _)| area)
}

/// Whether a build that does (`has_fake`) or does not carry the replay lane's
/// inference runtime can replay a case that raises `code`: everything but the
/// `inference` area replays without it. Callers pass [`INFERENCE_REPLAYABLE`]
/// directly rather than through a this-build wrapper: under the replay lane's
/// features such a wrapper is `true` for every code, so pinning it to `true`
/// would be an equivalent program there — a survivor no test can catch.
fn replayable_with(has_fake: bool, code: &str) -> bool {
    has_fake || code_area(code) != "inference"
}

/// Inference commands (ids under `inference.`) replay only against the testkit
/// fake service, so their fixtures stay deterministic without a real model.
fn is_inference_command(entry: &ResolvedCommand) -> bool {
    entry.id.starts_with("inference.")
}

/// Opens the scratch executor for replaying `entry`: a cache executor with the
/// deterministic testkit fake as its inference runtime where the build has it,
/// so everything that reaches inference — the `inference.*` commands and
/// `vector --text` — replays against the same world.
fn open_executor(entry: &ResolvedCommand) -> Result<Executor> {
    let executor = Executor::open_cache().map_err(|error| {
        invalid(format!(
            "`{}`: scratch executor failed to open: {error}",
            entry.id
        ))
    })?;
    #[cfg(all(feature = "inference", feature = "testkit"))]
    let executor =
        executor.with_inference_runtime(strata_inference::testkit::FakeInferenceService::new());
    Ok(executor)
}

/// The code an error case's fixture pins, or `None` when the fixture is not
/// there yet (a first `--update` writes it).
fn pinned_code(repo_root: &Path, case: &ErrorFixtureCase) -> Result<Option<String>> {
    let path = fixture_path(repo_root, &case.expected_error);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(IdlError::Read { path, source }),
    };
    let expected: Value = serde_json::from_str(&text).map_err(|source| IdlError::Json {
        path: path.clone(),
        source,
    })?;
    Ok(expected
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_owned))
}

/// Executes every fixture pair; returns the list of blessed files when
/// `update` is set (empty means everything already matched).
pub(super) fn verify_fixtures(
    repo_root: &Path,
    index: &CommandIndex,
    update: bool,
) -> Result<Vec<PathBuf>> {
    let mut blessed = Vec::new();
    let mut replayed = std::collections::BTreeSet::new();
    let mut command_replays: Vec<(String, String)> = Vec::new();
    for entry in &index.commands {
        if let Some(reason) = &entry.fixtures.replay_skip {
            if reason.trim().is_empty() {
                return Err(invalid(format!(
                    "`{}`: replay_skip must state a reason",
                    entry.id
                )));
            }
            continue;
        }
        // Inference commands replay only against the testkit fake service —
        // their fixtures are blessed to its deterministic output. Without the
        // fake there is nothing deterministic to replay them against, so skip
        // them; the same coverage runs in the testkit CI lane.
        if is_inference_command(entry) && !INFERENCE_REPLAYABLE {
            continue;
        }
        let primary = FixtureCase {
            setup: entry.fixtures.setup.clone(),
            request: entry.fixtures.request.clone(),
            response: entry.fixtures.response.clone(),
        };
        for (position, case) in std::iter::once(&primary)
            .chain(&entry.fixtures.cases)
            .enumerate()
        {
            verify_case(repo_root, entry, case, position, update, &mut blessed)?;
        }
        enforce_alternates_have_cases(entry)?;
        for (position, case) in entry.fixtures.error_cases.iter().enumerate() {
            // A case that reaches inference on a non-inference command
            // (`vector --text`) needs the fake as much as the commands above.
            if pinned_code(repo_root, case)?
                .is_some_and(|code| !replayable_with(INFERENCE_REPLAYABLE, &code))
            {
                continue;
            }
            let code = verify_error_case(repo_root, entry, case, position, update, &mut blessed)?;
            replayed.insert(code.clone());
            command_replays.push((entry.id.clone(), code));
        }
    }
    // The replay-coverage ratchet is a property of the full corpus, so it runs
    // once all error cases have executed. Skip it while blessing (`update`):
    // the envelopes being written may not yet match the declared error lists.
    if !update {
        super::enforce_error_replay_coverage(
            repo_root,
            index,
            &replayed,
            &command_replays,
            &|code| replayable_with(INFERENCE_REPLAYABLE, code),
        )?;
    }
    Ok(blessed)
}

/// Replay an error fixture: run any setup, execute the request, and require it
/// to FAIL with an `ErrorStatus` whose stable fields (code, class, `retry_policy`,
/// `retryable`, `commit_outcome`) match the pinned fixture. This is the only replay
/// coverage of the engine->executor error mapping — a mis-mapped class or a
/// changed retry policy fails here instead of shipping to SDKs.
/// Returns the error code the replay actually produced, so the caller can
/// enforce declaration and coverage ratchets across the whole corpus.
fn verify_error_case(
    repo_root: &Path,
    entry: &ResolvedCommand,
    case: &ErrorFixtureCase,
    position: usize,
    update: bool,
    blessed: &mut Vec<PathBuf>,
) -> Result<String> {
    let mut executor = open_executor(entry)?;

    for setup_path in &case.setup {
        let setup_command = read_command(repo_root, setup_path)?;
        executor.execute(setup_command).map_err(|error| {
            invalid(format!(
                "`{}` error case {position}: setup fixture `{setup_path}` failed to execute: {error}",
                entry.id
            ))
        })?;
    }

    let command = read_command(repo_root, &case.request)?;
    let status = match executor.execute(command) {
        Ok(output) => {
            return Err(invalid(format!(
                "`{}` error case {position}: request fixture `{}` was expected to fail but                  succeeded with output {}",
                entry.id,
                case.request,
                compact(&serde_json::to_value(&output).unwrap_or(Value::Null)),
            )));
        }
        Err(error) => error.into_status(),
    };

    // Pin only the stable, rule-29 fields; prose and per-run identifiers churn.
    let full = serde_json::to_value(&status).map_err(|source| IdlError::Json {
        path: PathBuf::from(&case.request),
        source,
    })?;
    let actual = serde_json::json!({
        "code": full.get("code").cloned().unwrap_or(Value::Null),
        "class": full.get("class").cloned().unwrap_or(Value::Null),
        "retry_policy": full.get("retry_policy").cloned().unwrap_or(Value::Null),
        "retryable": full.get("retryable").cloned().unwrap_or(Value::Null),
        "commit_outcome": full.get("commit_outcome").cloned().unwrap_or(Value::Null),
    });

    let expected_path = fixture_path(repo_root, &case.expected_error);
    let expected: Value = match std::fs::read_to_string(&expected_path) {
        Ok(text) => serde_json::from_str(&text).map_err(|source| IdlError::Json {
            path: expected_path.clone(),
            source,
        })?,
        Err(error) if update && error.kind() == std::io::ErrorKind::NotFound => Value::Null,
        Err(source) => {
            return Err(IdlError::Read {
                path: expected_path,
                source,
            })
        }
    };

    if actual != expected {
        if update {
            let mut text =
                serde_json::to_string_pretty(&actual).map_err(|source| IdlError::Json {
                    path: expected_path.clone(),
                    source,
                })?;
            text.push('\n');
            std::fs::write(&expected_path, text).map_err(|source| IdlError::Write {
                path: expected_path.clone(),
                source,
            })?;
            blessed.push(expected_path);
        } else {
            return Err(invalid(format!(
                "`{}` error case {position}: error envelope `{}` does not match a real run.\n  expected (fixture): {}\n  actual   (replay):  {}\nRun `strata-idl verify-fixtures --update` and review the diff.",
                entry.id,
                case.expected_error,
                compact(&expected),
                compact(&actual),
            )));
        }
    }

    actual
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            invalid(format!(
                "`{}` error case {position}: replayed status carried no `code` field",
                entry.id
            ))
        })
}

/// Masks the non-deterministic wall-clock `committed_at` (#3112) to null wherever
/// it appears in a serialized envelope, so fixtures pin the field's presence and
/// shape without pinning its value. A genuinely-unknown `committed_at` is already
/// null, so this is idempotent on those.
fn mask_committed_at(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "committed_at" {
                    *child = Value::Null;
                } else {
                    mask_committed_at(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(mask_committed_at),
        _ => {}
    }
}

fn verify_case(
    repo_root: &Path,
    entry: &ResolvedCommand,
    case: &FixtureCase,
    position: usize,
    update: bool,
    blessed: &mut Vec<PathBuf>,
) -> Result<()> {
    let mut executor = open_executor(entry)?;

    for setup_path in &case.setup {
        let setup_command = read_command(repo_root, setup_path)?;
        executor.execute(setup_command).map_err(|error| {
            invalid(format!(
                "`{}` case {position}: setup fixture `{setup_path}` failed to execute: {error}",
                entry.id
            ))
        })?;
    }

    let command = read_command(repo_root, &case.request)?;
    let output = executor.execute(command).map_err(|error| {
        invalid(format!(
            "`{}` case {position}: request fixture `{}` failed to execute: {error}",
            entry.id, case.request
        ))
    })?;
    let mut actual = serde_json::to_value(&output).map_err(|source| IdlError::Json {
        path: PathBuf::from(&case.request),
        source,
    })?;
    // `committed_at` is a real wall-clock instant (#3112) — non-deterministic,
    // so a fixture cannot pin its value. Mask it to a fixed sentinel here so the
    // shape is still pinned while the replay stays reproducible.
    mask_committed_at(&mut actual);

    let response_path = fixture_path(repo_root, &case.response);
    let expected: Value = match std::fs::read_to_string(&response_path) {
        Ok(text) => serde_json::from_str(&text).map_err(|source| IdlError::Json {
            path: response_path.clone(),
            source,
        })?,
        Err(error) if update && error.kind() == std::io::ErrorKind::NotFound => Value::Null,
        Err(source) => {
            return Err(IdlError::Read {
                path: response_path,
                source,
            })
        }
    };

    if actual != expected {
        if update {
            let mut text =
                serde_json::to_string_pretty(&actual).map_err(|source| IdlError::Json {
                    path: response_path.clone(),
                    source,
                })?;
            text.push('\n');
            std::fs::write(&response_path, text).map_err(|source| IdlError::Write {
                path: response_path.clone(),
                source,
            })?;
            blessed.push(response_path);
        } else {
            return Err(invalid(format!(
                "`{}` case {position}: response fixture `{}` does not match a real run.\n  expected (fixture): {}\n  actual   (replay):  {}\nRun `strata-idl verify-fixtures --update` and review the diff.",
                entry.id,
                case.response,
                compact(&expected),
                compact(&actual),
            )));
        }
    }
    Ok(())
}

/// Every alternate wire output listed in `fixtures.responses` must be
/// reproduced by some case — otherwise it is a frozen, unverifiable shape.
fn enforce_alternates_have_cases(entry: &ResolvedCommand) -> Result<()> {
    for alternate in &entry.fixtures.responses {
        let reproduced = entry
            .fixtures
            .cases
            .iter()
            .any(|case| &case.response == alternate);
        if !reproduced {
            return Err(invalid(format!(
                "`{}`: alternate response fixture `{alternate}` has no fixtures.cases entry that reproduces it; add a case with a request that yields this shape",
                entry.id
            )));
        }
    }
    Ok(())
}

fn read_command(repo_root: &Path, relative: &str) -> Result<Command> {
    let path = fixture_path(repo_root, relative);
    let text = std::fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    serde_json::from_str(&text).map_err(|source| IdlError::Json { path, source })
}

fn fixture_path(repo_root: &Path, relative: &str) -> PathBuf {
    repo_root.join(super::FIXTURE_ROOT).join(relative)
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mask_committed_at_nulls_the_field_at_any_depth_and_touches_nothing_else() {
        // #3112: the fixture mask must null every `committed_at` (a volatile
        // wall-clock instant) wherever it appears — nested in objects and inside
        // arrays — and leave every other field untouched.
        let mut value = json!({
            "data": {
                "commit": {"committed_at": 123, "version": 1, "timestamp": 3},
                "items": [
                    {"commit": {"committed_at": 9}, "index": 0},
                    {"commit": {"committed_at": 10}, "index": 1},
                ],
            },
        });
        mask_committed_at(&mut value);
        assert_eq!(value["data"]["commit"]["committed_at"], Value::Null);
        assert_eq!(
            value["data"]["items"][0]["commit"]["committed_at"],
            Value::Null
        );
        assert_eq!(
            value["data"]["items"][1]["commit"]["committed_at"],
            Value::Null
        );
        // Everything else is preserved.
        assert_eq!(value["data"]["commit"]["version"], json!(1));
        assert_eq!(value["data"]["commit"]["timestamp"], json!(3));
        assert_eq!(value["data"]["items"][0]["index"], json!(0));
    }

    #[test]
    fn only_the_inference_area_needs_the_fake_to_replay() {
        // The area is what comes before the first `.`; a code with none is
        // its own area.
        assert_eq!(code_area("inference.unknown_model"), "inference");
        assert_eq!(code_area("not_found.engine.branch"), "not_found");
        assert_eq!(code_area("inference"), "inference");

        // With the fake, everything replays. Without it, everything but the
        // inference area does — whichever command raised the code.
        for code in ["inference.unknown_model", "not_found.engine.branch"] {
            assert!(replayable_with(true, code), "{code} replays with the fake");
        }
        assert!(!replayable_with(false, "inference.unknown_model"));
        assert!(!replayable_with(false, "inference.missing_model"));
        assert!(replayable_with(false, "not_found.engine.branch"));
        assert!(replayable_with(
            false,
            "invalid_argument.engine.vector_dimension"
        ));

        // This build's answer is the fake's presence applied to the same rule:
        // an engine code always replays; an inference code replays exactly
        // when the fake is here.
        assert!(replayable_with(
            INFERENCE_REPLAYABLE,
            "not_found.engine.branch"
        ));
        assert_eq!(
            replayable_with(INFERENCE_REPLAYABLE, "inference.unknown_model"),
            INFERENCE_REPLAYABLE
        );
    }

    /// An error case pinned at `expected_error` under a scratch repo root.
    fn case_pinned_at(expected_error: &str) -> ErrorFixtureCase {
        ErrorFixtureCase {
            setup: vec![],
            request: "requests/v1/kv/get.json".to_owned(),
            expected_error: expected_error.to_owned(),
        }
    }

    #[test]
    fn the_pinned_code_is_read_from_the_fixture_or_absent_when_it_is_not_there_yet() {
        let root = tempfile::tempdir().expect("scratch repo root");
        let errors = fixture_path(root.path(), "responses/v1/errors");
        std::fs::create_dir_all(&errors).expect("fixture directory");

        // A committed fixture pins its code; a fixture that is not there yet
        // (the first `--update` writes it) pins nothing — it is not an error.
        std::fs::write(
            errors.join("pinned.json"),
            r#"{"code": "inference.unknown_model", "class": "inference"}"#,
        )
        .expect("write fixture");
        assert_eq!(
            pinned_code(
                root.path(),
                &case_pinned_at("responses/v1/errors/pinned.json")
            )
            .expect("a readable fixture"),
            Some("inference.unknown_model".to_owned())
        );
        assert_eq!(
            pinned_code(
                root.path(),
                &case_pinned_at("responses/v1/errors/absent.json")
            )
            .expect("an absent fixture is not an error"),
            None
        );

        // A fixture without a `code` pins nothing either: the replay itself
        // reports the mismatch.
        std::fs::write(errors.join("codeless.json"), r#"{"class": "inference"}"#)
            .expect("write fixture");
        assert_eq!(
            pinned_code(
                root.path(),
                &case_pinned_at("responses/v1/errors/codeless.json")
            )
            .expect("a readable fixture"),
            None
        );

        // Only "not there" is absence. A fixture that cannot be parsed or
        // cannot be read (a directory in its place) is the error it is, so a
        // broken fixture never silently skips its case.
        std::fs::write(errors.join("broken.json"), "{").expect("write fixture");
        assert!(matches!(
            pinned_code(
                root.path(),
                &case_pinned_at("responses/v1/errors/broken.json")
            ),
            Err(IdlError::Json { .. })
        ));
        std::fs::create_dir(errors.join("dir.json")).expect("directory in the fixture's place");
        assert!(matches!(
            pinned_code(root.path(), &case_pinned_at("responses/v1/errors/dir.json")),
            Err(IdlError::Read { .. })
        ));
    }
}
