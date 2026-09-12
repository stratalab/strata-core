//! command-examples.json generator (#3059): the docs bundle shipped example
//! *inputs* but never what they print. Capturing the output needs both the
//! executor (to replay each step) and this crate's renderer (to turn the wire
//! output into the CLI text a reader sees) — so, like `cli-arg-spec.json`, the
//! artifact is generated here and consumed downstream. Each step records its
//! rendered input, its rendered output, and whether that output is reproducible.
//! Reproducibility is decided without ever inspecting the real values, so the
//! verdict is identical on every machine: the wire is rendered twice, each time
//! with a *different* sentinel substituted for the context-dependent fields
//! (versions, timestamps, ids, host memory, tempdir paths, model-cache state).
//! If the two renders agree, none of those fields reached the output — it is
//! reproducible, and the render equals the real one. The stored text is always
//! the first masked render, so the artifact is byte-identical on every machine
//! and run; the `reproducible` flag tells a consumer whether that text is the
//! literal output or a shape with placeholders it should not treat as exact. A
//! guard keeps the committed file in lockstep with a fresh replay; an `--ignored`
//! test writes it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use strata_executor::idl_tooling::CapturedStep;

use crate::options::Format;
use crate::render::Invocation;

/// Wire-output field names whose value is context-dependent (a version, a
/// timestamp, an id, a host-specific number, a per-run path). Masking them and
/// re-rendering reveals whether the CLI output actually exposes one — the
/// renderer hides most, so this only flags the outputs that genuinely vary run
/// to run or machine to machine. This list is the correctness surface: a
/// variant field missing here would leak into the stored output and break the
/// cross-machine replay guard on CI (the backstop that catches an omission).
const VOLATILE_FIELDS: &[&str] = &[
    // D11: `inference status` reports the caller's own model directory and how
    // many models they happen to have. Capturing those bakes the recording
    // machine into a committed file — the example passed locally and failed in
    // CI, where the path and the count are both different.
    "models_dir",
    "models_downloaded",
    "version",
    "timestamp",
    "document_version",
    "created_at",
    "deleted_at",
    "merged_timestamp",
    "fork_version",
    "source_generation",
    "state_revision",
    "generation",
    "owner_pid",
    "pid",
    "socket_path",
    "reference_id",
    "fetched_at",
    "branch_id",
    "source_branch_id",
    // Machine- or run-specific, not run-to-run: the host memory budget varies by
    // machine, and export/import echo a per-run tempdir path.
    "usable_host_bytes",
    "total_bytes",
    "file_path",
    "paths",
    // Event hashes chain from the event's wall-clock timestamp, so they vary run
    // to run even once the timestamp itself is masked in the render.
    "hash",
    "previous_hash",
    // Whether a model is present in the host's model cache — varies by machine
    // (a dev box has models downloaded; a fresh CI runner does not).
    "is_local",
    // Wall-clock instant a commit was applied — a real time, so it varies every
    // run (#3112). Distinct from the logical `timestamp`, which is stable.
    "committed_at",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExampleLine {
    #[serde(rename = "in")]
    input: String,
    out: String,
    reproducible: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct CommandExamples {
    commands: BTreeMap<String, Vec<ExampleLine>>,
}

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `crates/cli`; the IDL tooling resolves paths from
    // the workspace root (`IDL_DIR` = `crates/executor/idl/v1`).
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn spec_path() -> PathBuf {
    repo_root().join("crates/executor/idl/v1/generated/command-examples.json")
}

/// Renders a step's wire output to the CLI text a reader sees — the `Human`
/// format, through the typed `Output` and the command's `display:`
/// declaration, so KV bytes are decoded to text (a `get` prints `v2`, not
/// `djI=`) and a write prints its receipt (`created setting`). Stdout then
/// stderr, as a terminal shows them (a missed write is its `no such key: k`
/// feedback line), trimmed of the trailing newline; a read miss renders to
/// empty (nothing printed).
fn render_output(step: &CapturedStep, wire_output: &Value) -> String {
    let output: strata_executor::Output =
        serde_json::from_value(wire_output.clone()).expect("wire output deserializes into Output");
    let invocation = Invocation::for_wire(&step.wire, Format::Human, || Ok(step.request.clone()))
        .expect("declaration resolves");
    crate::render::render_output(&output, &invocation, Format::Human)
        .expect("output renders")
        .stdout_then_stderr()
        .trim_end_matches('\n')
        .to_owned()
}

/// Replaces every volatile field's value with a type-preserving sentinel drawn
/// from `variant`, recursively. Two distinct sentinel sets (variant 0 and 1) let
/// [`render_step`] tell whether a volatile field actually reaches the render
/// without ever inspecting the real value — the key to a machine-independent
/// reproducibility verdict. Type is preserved so the envelope still deserializes
/// into `Output`.
fn mask_volatile(value: &mut Value, variant: u8) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "committed_at" {
                    // #3112 S5: human rendering turns an instant into a LOCAL
                    // date, so a numeric sentinel here would bake the capturing
                    // machine's time zone into a committed file — and render
                    // the epoch as a 1969 date, the very symptom this epic
                    // exists to remove. `null` is already the wire's own
                    // "unknown", is machine-independent, and passes through
                    // rendering untouched.
                    *child = Value::Null;
                } else if VOLATILE_FIELDS.contains(&key.as_str()) {
                    mask_leaf(child, variant);
                } else {
                    mask_volatile(child, variant);
                }
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| mask_volatile(item, variant)),
        _ => {}
    }
}

/// Overwrites a volatile leaf with a same-typed sentinel. The two variants pick
/// *different* sentinels of each type, so a leaf that reaches the render renders
/// differently between them — the signal `render_step` reads. The two must
/// stay different through every presentation a declaration can apply, and a
/// presentation may drop precision (a size rounds to its unit; a date once
/// showed whole seconds), so the numeric sentinels sit a million apart rather
/// than one unit — `0` and `1` micros both read `1970-01-01 00:00:00 UTC`
/// under that date form, which passed an event list off as reproducible.
fn mask_leaf(value: &mut Value, variant: u8) {
    match value {
        Value::Number(_) => *value = Value::from(u64::from(variant) * 1_000_000),
        Value::String(_) => {
            *value = Value::from(if variant == 0 { "masked" } else { "masked-alt" });
        }
        Value::Bool(_) => *value = Value::from(variant != 0),
        Value::Array(items) => items.iter_mut().for_each(|item| mask_leaf(item, variant)),
        _ => {}
    }
}

/// Renders a step's stored output and its reproducibility, both independent of
/// the machine the capture runs on. The wire is rendered twice, each time with a
/// *different* sentinel substituted for the volatile fields; if the two renders
/// agree, no volatile field reached the output, so it is reproducible and the
/// render also equals the real one (the masked values never appeared). If they
/// differ, the output exposes a version/timestamp/id/host value: it is flagged
/// non-reproducible, and the stored text is the first masked render — a
/// deterministic shape whose varying leaves a consumer must not treat as exact.
fn render_step(step: &CapturedStep) -> (String, bool) {
    let mut variant_a = step.wire_output.clone();
    mask_volatile(&mut variant_a, 0);
    let mut variant_b = step.wire_output.clone();
    mask_volatile(&mut variant_b, 1);
    let render_a = render_output(step, &variant_a);
    let reproducible = render_a == render_output(step, &variant_b);
    (render_a, reproducible)
}

fn build() -> CommandExamples {
    let runs =
        strata_executor::idl_tooling::capture_examples(&repo_root()).expect("capture examples");
    let mut commands = BTreeMap::new();
    for run in runs {
        let lines = run
            .steps
            .into_iter()
            .map(|step| {
                let (out, reproducible) = render_step(&step);
                ExampleLine {
                    input: step.cli_input,
                    out,
                    reproducible,
                    note: step.note,
                }
            })
            .collect();
        commands.insert(run.command_id, lines);
    }
    CommandExamples { commands }
}

fn to_json(examples: &CommandExamples) -> String {
    let mut json = serde_json::to_string_pretty(examples).expect("examples serialize");
    json.push('\n');
    json
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_examples_match_a_fresh_replay() {
        let committed: CommandExamples = serde_json::from_str(
            &std::fs::read_to_string(spec_path()).expect("command-examples.json exists"),
        )
        .expect("command-examples.json parses");
        assert_eq!(
            committed,
            build(),
            "command-examples.json is stale; regenerate with \
             `cargo test -p strata-cli --lib command_examples -- --ignored regenerate`"
        );
    }

    #[test]
    #[ignore = "regenerates the committed command-examples.json; run explicitly"]
    fn regenerate() {
        std::fs::write(spec_path(), to_json(&build())).expect("write command-examples.json");
    }
}
