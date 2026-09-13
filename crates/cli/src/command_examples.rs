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
//! reproducible, and the stored text is that render, equal to the real one.
//! When they disagree, the spans where they disagree are exactly the volatile
//! values, so the stored text is the render with those spans elided to `…`
//! (#3314 S5) and `reproducible` is false. Either way the artifact is
//! byte-identical on every machine and run, and nothing in it claims to be
//! output the binary never printed. A guard keeps the committed file in
//! lockstep with a fresh replay; an `--ignored` test writes it.

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
/// showed whole seconds), so the numeric sentinels sit far apart rather than
/// one unit — `0` and `1` micros both read `1970-01-01 00:00:00 UTC` under
/// that date form, which passed an event list off as reproducible.
fn mask_leaf(value: &mut Value, variant: u8) {
    match value {
        // The pair is chosen so that a value which *does* reach the output
        // renders wholly differently, not a character apart: as a count the
        // digits differ and so does their number, as an instant both the date
        // and the time differ, as a byte size both the number and the unit do,
        // as a string not one token survives. A closer pair (0 and 1, say)
        // still decides reproducibility correctly, but leaves the elision in
        // `render_step` with only one differing character to go on — and the
        // rest of the sentinel, a 1970 date or the word `masked`, would stand
        // in the docs as though the binary had printed it (#3314 S5).
        Value::Number(_) => {
            *value = Value::from(if variant == 0 {
                0
            } else {
                1_700_000_000_000_000_u64
            });
        }
        Value::String(_) => {
            *value = Value::from(if variant == 0 { "masked" } else { "elided" });
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
    let render_b = render_output(step, &variant_b);
    if render_a == render_b {
        return (render_a, true);
    }
    (elide_varying(&render_a, &render_b), false)
}

/// The placeholder standing in for a value that varies by run or by machine.
/// One character, so it costs a column instead of a line, and visibly not
/// something the binary prints (#3314 S5) — the alternative is publishing a
/// sentinel (`masked`, an epoch-0 date, version `0`) as though it were real
/// output, which is the exact failure R7 exists to prevent.
const VARIES: char = '…';

/// Elides the text that differs between the two masked renders. Because the
/// two renders differ *only* where a volatile field reached the output, what
/// differs is precisely what a consumer must not treat as exact — no guessing,
/// no pattern matching against the values themselves. Line-wise while the two
/// renders agree on line count (the common case: a table whose cells vary),
/// whole-string when a volatile value changed the shape itself.
fn elide_varying(a: &str, b: &str) -> String {
    let a_lines: Vec<&str> = a.lines().collect();
    let b_lines: Vec<&str> = b.lines().collect();
    if a_lines.len() != b_lines.len() {
        return elide_span(a, b);
    }
    a_lines
        .iter()
        .zip(&b_lines)
        .map(|(x, y)| elide_line(x, y))
        .collect::<Vec<_>>()
        .join("\n")
}

/// One line: keeps every token the two renders agree on and replaces each
/// token they disagree on with [`VARIES`], padded to the width it replaced.
/// The unit is the whitespace-delimited token because that is the human
/// renderer's own unit — a table cell, a label, a value — so a varying field
/// never leaves half a sentinel behind the way a character-span diff does
/// (two instants a second apart share every character but one). Spacing comes
/// from `a`: a sentinel's width shifts the padding of every column after it,
/// and that shift is an artifact of masking, not information.
fn elide_line(a: &str, b: &str) -> String {
    let (a_tokens, b_tokens) = (split_tokens(a), split_tokens(b));
    if a_tokens.len() != b_tokens.len() {
        return elide_span(a, b);
    }
    // A column-aligned line (one a table or a label/value pair produced) holds
    // its alignment: the elision is padded to the width it replaced so the
    // columns after it stay put. A prose line has nothing to align to, so its
    // elision closes up rather than leaving a gap mid-sentence.
    let aligned = a_tokens
        .iter()
        .skip(1)
        .any(|(separator, _)| separator.chars().count() >= 2);
    let mut out = String::with_capacity(a.len());
    for ((separator, a_token), (_, b_token)) in a_tokens.iter().zip(&b_tokens) {
        out.push_str(separator);
        if a_token == b_token {
            out.push_str(a_token);
        } else {
            out.push(VARIES);
            if aligned {
                out.extend(std::iter::repeat_n(
                    ' ',
                    a_token.chars().count().saturating_sub(1),
                ));
            }
        }
    }
    // The renderer never emits trailing whitespace, so padding that runs to the
    // end of a line is padding this function added.
    out.trim_end().to_owned()
}

/// Splits a line into (leading whitespace, token) pairs. Rebuilding from them
/// in order reproduces the line exactly, which is what lets [`elide_line`]
/// swap one token without disturbing the rest.
fn split_tokens(line: &str) -> Vec<(&str, &str)> {
    let mut tokens = Vec::new();
    let mut rest = line;
    while !rest.is_empty() {
        let break_at = rest
            .find(|c: char| !c.is_whitespace())
            .unwrap_or(rest.len());
        let (separator, after) = rest.split_at(break_at);
        if after.is_empty() {
            break;
        }
        let break_at = after.find(char::is_whitespace).unwrap_or(after.len());
        let (token, remainder) = after.split_at(break_at);
        tokens.push((separator, token));
        rest = remainder;
    }
    tokens
}

/// The fallback when the two renders do not line up token for token — a
/// volatile value that changed the output's shape, not just a cell. Keeps what
/// they share at each end and elides everything between, which over-elides
/// rather than claiming text it cannot prove is stable.
fn elide_span(a: &str, b: &str) -> String {
    if a == b {
        return a.to_owned();
    }
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let prefix = a_chars
        .iter()
        .zip(&b_chars)
        .take_while(|(x, y)| x == y)
        .count();
    let headroom = a_chars.len().min(b_chars.len()) - prefix;
    let suffix = a_chars
        .iter()
        .rev()
        .zip(b_chars.iter().rev())
        .take_while(|(x, y)| x == y)
        .count()
        .min(headroom);
    let mut out: String = a_chars[..prefix].iter().collect();
    out.push(VARIES);
    out.extend(&a_chars[a_chars.len() - suffix..]);
    out.trim_end().to_owned()
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

    /// The number of example steps whose output exposes a run- or
    /// machine-dependent value. Shrink-only (#3314 S5): a step that starts
    /// exposing one is a regression in what the docs can show exactly, and a
    /// step that stops exposing one lowers the ceiling in the same PR. Nothing
    /// derives this number, so it is written down — but only here, and the
    /// assertion below names both directions.
    const NON_REPRODUCIBLE_CEILING: usize = 50;

    fn non_reproducible(examples: &CommandExamples) -> usize {
        examples
            .commands
            .values()
            .flatten()
            .filter(|line| !line.reproducible)
            .count()
    }

    #[test]
    fn non_reproducible_examples_only_ever_shrink() {
        let committed: CommandExamples = serde_json::from_str(
            &std::fs::read_to_string(spec_path()).expect("command-examples.json exists"),
        )
        .expect("command-examples.json parses");
        let count = non_reproducible(&committed);
        assert!(
            count <= NON_REPRODUCIBLE_CEILING,
            "{count} example steps expose a run- or machine-dependent value, up from \
             {NON_REPRODUCIBLE_CEILING}; a step whose output the docs could show exactly \
             no longer can — mask the field or drop it from the output, do not raise the ceiling"
        );
        assert_eq!(
            count, NON_REPRODUCIBLE_CEILING,
            "only {count} example steps are non-reproducible now — lower \
             NON_REPRODUCIBLE_CEILING to {count} in this PR so the gain is held"
        );
    }

    #[test]
    fn renders_that_agree_are_kept_character_for_character() {
        assert_eq!(
            elide_line("created config", "created config"),
            "created config"
        );
        assert_eq!(
            elide_varying("a\nb\nc", "a\nb\nc"),
            "a\nb\nc",
            "nothing varies, so nothing is elided"
        );
    }

    #[test]
    fn a_varying_cell_keeps_the_width_of_the_column_it_replaces() {
        let a = "a          0  1";
        let elided = elide_line(a, "a          1700000000000000  1");
        assert_eq!(elided, "a          …  1", "the VALUE column must not shift");
        assert_eq!(elided.chars().count(), a.chars().count());
    }

    #[test]
    fn every_part_of_an_instant_is_elided_not_just_the_digit_that_moved() {
        // The whole point of the widely-separated sentinel pair: a reader must
        // not be shown `1970-01-01` as though the binary printed it.
        let elided = elide_line(
            "committed  1970-01-01 00:00:00.000000 UTC",
            "committed  2023-11-14 22:13:20.000000 UTC",
        );
        assert_eq!(elided, "committed  …          …               UTC");
        assert!(!elided.contains("1970") && !elided.contains("00:00"));
    }

    #[test]
    fn a_column_that_only_moved_because_of_masking_is_left_alone() {
        // The sentinel is 16 digits wide, so variant B's table is wider. That
        // shift is an artifact of masking; every token still agrees.
        assert_eq!(
            elide_line("KEY  VERSION  VALUE", "KEY  VERSION           VALUE"),
            "KEY  VERSION  VALUE"
        );
    }

    #[test]
    fn a_value_at_the_end_of_a_line_leaves_no_trailing_padding() {
        assert_eq!(
            elide_line("version  masked", "version  elided"),
            "version  …"
        );
        assert_eq!(
            elide_line(
                "exported 1 row to masked (1.9 kB)",
                "exported 1 row to elided (1.9 kB)"
            ),
            "exported 1 row to … (1.9 kB)",
            "prose closes up; only a column-aligned line keeps the width"
        );
    }

    #[test]
    fn each_varying_token_is_elided_on_its_own() {
        assert_eq!(
            elide_line("branch a  active  gen 1", "branch b  active  gen 2"),
            "branch …  active  gen …",
            "`active` is the same in both renders, so it is not a guess"
        );
    }

    #[test]
    fn a_volatile_value_that_changed_the_shape_elides_across_lines() {
        assert_eq!(
            elide_varying("paths\n  /tmp/a\n  /tmp/b", "paths\n  /tmp/c"),
            "paths\n  /tmp/…"
        );
    }

    #[test]
    fn a_line_whose_token_count_moved_falls_back_to_a_span() {
        assert_eq!(elide_line("branch a", "branch a b"), "branch a…");
    }

    #[test]
    fn a_size_elides_its_unit_along_with_its_number() {
        assert_eq!(elide_line("size 1 B", "size 1.7 PB"), "size … …");
    }

    #[test]
    fn eliding_respects_character_boundaries() {
        assert_eq!(elide_line("café 1", "café 2"), "café …");
        assert_eq!(elide_line("1 café", "2 café"), "… café");
        assert_eq!(elide_span("café 1", "café 2"), "café …");
    }

    /// #3358 F3: the printed CLI line must produce the request that was
    /// captured. The docs assert the *output* half — the page renders its own
    /// `$` line and holds it equal to the captured one — but both sides come
    /// from the same renderer, so that compares two generated strings. This is
    /// the semantic half: split the line the way a shell would, parse it with
    /// the real clap grammar, and compare the command it yields with the one
    /// the step actually ran.
    /// Commands whose examples replay through the executor but which the CLI
    /// refuses to convert in an embedded session. See the note at the skip
    /// below; this list goes away with #3355.
    const NEEDS_A_HOST: &[&str] = &[
        "admin.ipc_status",
        "admin.ipc_stop",
        "inference.cache_status",
        "inference.capability",
        "inference.models.list",
        "inference.status",
        "inference.unload",
    ];

    #[test]
    fn every_printed_line_produces_the_request_it_documents() {
        let mut broken = Vec::new();
        let mut exercised = std::collections::BTreeSet::new();
        for run in
            strata_executor::idl_tooling::capture_examples(&repo_root()).expect("capture examples")
        {
            for step in run.steps {
                let Some(line) = step.cli_input.strip_prefix("strata ") else {
                    broken.push(format!(
                        "{}: `{}` is not a strata line",
                        run.command_id, step.cli_input
                    ));
                    continue;
                };
                let words = match crate::line::words(line) {
                    Ok(Some(words)) => words,
                    other => {
                        broken.push(format!(
                            "{}: `{}` does not split into words: {other:?}",
                            run.command_id, step.cli_input
                        ));
                        continue;
                    }
                };
                let parsed = match crate::line::SessionLine::parse(words) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        broken.push(format!(
                            "{}: `{}` does not parse: {error}",
                            run.command_id, step.cli_input
                        ));
                        continue;
                    }
                };
                // A command that needs a host environment cannot be converted
                // in process. Its line is still proven to *parse* — the clap
                // grammar accepted it above — which is all this guard can say
                // about it. The class has no name in the code today: it is a
                // `_` arm in `command_to_executor` returning a string, which
                // is the same missing classification that lets these commands
                // panic a session (#3355). When that lands, this ledger is
                // replaced by the typed predicate.
                if crate::deferred_top_command(&parsed.command).is_some()
                    || NEEDS_A_HOST.contains(&run.command_id.as_str())
                {
                    exercised.insert(run.command_id.clone());
                    continue;
                }
                let scope = crate::Scope {
                    branch: parsed.branch,
                    space: parsed.space,
                };
                match crate::command_to_executor(parsed.command, &scope) {
                    Ok(command) => {
                        let produced = serde_json::to_value(&command).expect("command serializes");
                        if produced != step.documented_request {
                            broken.push(format!(
                                "{}: `{}`\n     runs: {produced}\n  documents: {}",
                                run.command_id, step.cli_input, step.documented_request
                            ));
                        }
                    }
                    Err(error) => broken.push(format!(
                        "{}: `{}` does not convert: {error}",
                        run.command_id, step.cli_input
                    )),
                }
            }
        }
        assert!(
            broken.is_empty(),
            "{} printed example lines do not run what they claim:\n  {}",
            broken.len(),
            broken.join("\n  ")
        );
        // A stale entry would silently excuse a command this guard could
        // otherwise check.
        let stale: Vec<&&str> = NEEDS_A_HOST
            .iter()
            .filter(|id| !exercised.contains(**id))
            .collect();
        assert!(
            stale.is_empty(),
            "these no longer need a host and should leave the list: {stale:?}"
        );
    }

    #[test]
    #[ignore = "regenerates the committed command-examples.json; run explicitly"]
    fn regenerate() {
        std::fs::write(spec_path(), to_json(&build())).expect("write command-examples.json");
    }
}
