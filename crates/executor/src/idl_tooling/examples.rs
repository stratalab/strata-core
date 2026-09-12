//! Canonical command examples: load, validate, render, and execute.
//!
//! Each command may declare one example at `idl/v1/examples/<id>.yaml` as a
//! language-neutral **step list** — an ordered sequence of command calls with
//! named arguments and an optional expected result. From that one source the
//! docs generator renders CLI + wire example tabs (SDKs render their own
//! calls from the same spec), and `verify-examples` replays every step against
//! a scratch cache executor so a stale example fails CI. Coverage is tracked
//! by a shrink-only `missing-examples.yaml` allowlist, mirroring
//! `uncovered-commands.yaml`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

use super::{invalid, read_yaml, CommandIndex, IdlError, ResolvedCommand, Result};
use crate::executor::Executor;
use crate::Command;

const EXAMPLES_SUBDIR: &str = "examples";
const MISSING_EXAMPLES_FILE: &str = "missing-examples.yaml";
const CLI_ARG_SPEC_FILE: &str = "generated/cli-arg-spec.json";
const COMMAND_EXAMPLES_FILE: &str = "generated/command-examples.json";

/// The CLI argument spec (#3073), authored from the clap tree by strata-cli.
/// Maps a verb path (`"graph add-node"`) to its positional wire fields in order
/// and its wire-field → long-flag map, so CLI examples render with the CLI's
/// real spellings (`--type`, not `--object-type`) and fall back to
/// `command run` for any wire field the verb does not expose.
#[derive(Default, Deserialize)]
pub(super) struct CliArgSpec {
    verbs: BTreeMap<String, VerbArgSpec>,
}

#[derive(Deserialize)]
struct VerbArgSpec {
    positionals: Vec<String>,
    flags: BTreeMap<String, String>,
}

/// Loads the committed CLI argument spec.
pub(super) fn load_arg_spec(repo_root: &Path) -> Result<CliArgSpec> {
    let path = repo_root.join(super::IDL_DIR).join(CLI_ARG_SPEC_FILE);
    let text = fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    serde_json::from_str(&text).map_err(|source| IdlError::Json { path, source })
}

/// One captured step of a command's example: the CLI line, the text that line
/// printed, and whether that text is exact. Authored by nobody — strata-cli
/// replays the step and renders the output the way a reader sees it, which is
/// why the artifact is produced there and consumed here (#3059).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TranscriptStep {
    #[serde(rename = "in")]
    pub input: String,
    pub out: String,
    pub reproducible: bool,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Default, Deserialize)]
struct CommandTranscripts {
    commands: BTreeMap<String, Vec<TranscriptStep>>,
}

/// Loads the captured example transcripts, keyed by command id.
pub(super) fn load_transcripts(repo_root: &Path) -> Result<BTreeMap<String, Vec<TranscriptStep>>> {
    let path = repo_root.join(super::IDL_DIR).join(COMMAND_EXAMPLES_FILE);
    let text = fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    let parsed: CommandTranscripts =
        serde_json::from_str(&text).map_err(|source| IdlError::Json { path, source })?;
    Ok(parsed.commands)
}

/// Rationale for `.expect()` on `write!` into a `String`: the `fmt::Write`
/// impl for `String` never returns `Err`.
const INFALLIBLE: &str = "writing to a String is infallible";

/// A resolved example: an optional caption plus the ordered steps.
pub(super) struct Example {
    pub caption: Option<String>,
    pub steps: Vec<ExampleStep>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExampleSource {
    #[serde(default)]
    caption: Option<String>,
    steps: Vec<ExampleStep>,
}

/// One step: a command call with named args, an optional expected result, and
/// an optional rendered comment.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExampleStep {
    pub call: String,
    #[serde(default)]
    pub args: Map<String, Value>,
    #[serde(default, deserialize_with = "deserialize_expected")]
    pub returns: Option<ExpectedResult>,
    #[serde(default)]
    pub note: Option<String>,
    /// Optional per-step result-expression override for SDK example renderers
    /// (`{}` is the call). strata-core validates its shape but does not
    /// interpret it — CLI/wire rendering and replay use only the call + args.
    #[serde(default)]
    pub expr: Option<String>,
    /// Optional name binding this step's result so a later step can reference a
    /// field of it via `{name.path.to.field}` (e.g. a prior receipt's commit
    /// version). The SDK renderer binds the call to a variable of this name.
    #[serde(default)]
    pub bind: Option<String>,
}

/// A step's declared expectation. An absent `returns` (setup step) maps to
/// `None`; `returns: null` to `Miss`; any other `returns:` value to `Present`.
/// strata-core only asserts miss-ness — the exact value is asserted by the SDK
/// doctest harness, which renders the same spec to its own calls.
pub(super) enum ExpectedResult {
    Miss,
    Present,
}

fn deserialize_expected<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<ExpectedResult>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(match Option::<Value>::deserialize(deserializer)? {
        None => ExpectedResult::Miss,
        Some(_) => ExpectedResult::Present,
    }))
}

/// `missing-examples.yaml`: the shrink-only example-coverage allowlist, split
/// by *why* a command has no hermetic example.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MissingExamplesSource {
    /// A hermetic cache-mode example is possible but not yet written.
    #[serde(default)]
    uncovered: Vec<String>,
    /// Covered by a gated integration test or reference-only, because a
    /// cache-mode replay cannot supply the resource (hub, model, API key,
    /// network, machine disk). Not pending.
    #[serde(default)]
    non_hermetic: Vec<String>,
}

fn examples_dir(repo_root: &Path) -> PathBuf {
    repo_root.join(super::IDL_DIR).join(EXAMPLES_SUBDIR)
}

/// Loads every `examples/<id>.yaml`, keyed by command id (the file stem).
pub(super) fn load_examples(repo_root: &Path) -> Result<BTreeMap<String, Example>> {
    let dir = examples_dir(repo_root);
    let mut examples = BTreeMap::new();
    if !dir.exists() {
        return Ok(examples);
    }
    for entry in fs::read_dir(&dir).map_err(|source| IdlError::Read {
        path: dir.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| IdlError::Read {
            path: dir.clone(),
            source,
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
            return Err(invalid(format!(
                "examples dir contains a non-YAML file: {}",
                path.display()
            )));
        }
        let id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| invalid(format!("example file has no stem: {}", path.display())))?
            .to_owned();
        let source: ExampleSource = read_yaml(&path)?;
        if source.steps.is_empty() {
            return Err(invalid(format!("example `{id}` has no steps")));
        }
        examples.insert(
            id,
            Example {
                caption: source.caption,
                steps: source.steps,
            },
        );
    }
    Ok(examples)
}

/// Validates every example against the schemas and enforces coverage: each
/// example file names a real command, each step calls a real command with
/// valid args, and every catalog command has an example or is allowlisted.
pub(super) fn validate_examples(
    repo_root: &Path,
    index: &CommandIndex,
    schemas: &BTreeMap<String, Value>,
    examples: &BTreeMap<String, Example>,
) -> Result<()> {
    let ids: BTreeSet<&str> = index.commands.iter().map(|c| c.id.as_str()).collect();
    for (id, example) in examples {
        if !ids.contains(id.as_str()) {
            return Err(invalid(format!(
                "examples/{id}.yaml does not name a known command"
            )));
        }
        for (position, step) in example.steps.iter().enumerate() {
            let schema = schemas.get(&step.call).ok_or_else(|| {
                invalid(format!(
                    "example `{id}` step {position} calls unknown command `{}`",
                    step.call
                ))
            })?;
            validate_step_args(id, position, step, schema)?;
        }
    }
    enforce_example_coverage(repo_root, &ids, examples)
}

fn validate_step_args(id: &str, position: usize, step: &ExampleStep, schema: &Value) -> Result<()> {
    let props = request_properties(schema).ok_or_else(|| {
        invalid(format!(
            "example `{id}` step {position}: `{}` has no request properties",
            step.call
        ))
    })?;
    for name in step.args.keys() {
        if name == "type" {
            return Err(invalid(format!(
                "example `{id}` step {position}: must not set the wire discriminator `type`"
            )));
        }
        if !props.contains_key(name) {
            return Err(invalid(format!(
                "example `{id}` step {position}: `{}` has no argument `{name}`",
                step.call
            )));
        }
    }
    for required in schema_required(schema) {
        if required != "type" && !step.args.contains_key(required) {
            return Err(invalid(format!(
                "example `{id}` step {position}: `{}` is missing required argument `{required}`",
                step.call
            )));
        }
    }
    if let Some(expr) = &step.expr {
        if !expr.contains("{}") {
            return Err(invalid(format!(
                "example `{id}` step {position}: `expr` must contain the `{{}}` call placeholder"
            )));
        }
    }
    Ok(())
}

/// Every command must have an example file or appear in `missing-examples.yaml`.
/// The allowlist may only shrink: a listed command that gains an example must
/// be removed, an unknown listed command is rejected, and a new command with
/// neither an example nor a listing fails the build.
fn enforce_example_coverage(
    repo_root: &Path,
    ids: &BTreeSet<&str>,
    examples: &BTreeMap<String, Example>,
) -> Result<()> {
    let allowlist: MissingExamplesSource =
        read_yaml(&repo_root.join(super::IDL_DIR).join(MISSING_EXAMPLES_FILE))?;
    let mut listed = BTreeSet::new();
    for id in allowlist
        .uncovered
        .iter()
        .chain(allowlist.non_hermetic.iter())
    {
        if !ids.contains(id.as_str()) {
            return Err(invalid(format!(
                "{MISSING_EXAMPLES_FILE} lists `{id}` which is not a command"
            )));
        }
        if examples.contains_key(id) {
            return Err(invalid(format!(
                "`{id}` has an example; remove it from {MISSING_EXAMPLES_FILE} (the allowlist may only shrink)"
            )));
        }
        if !listed.insert(id.as_str()) {
            return Err(invalid(format!(
                "duplicate `{id}` in {MISSING_EXAMPLES_FILE}"
            )));
        }
    }
    for id in ids {
        if !examples.contains_key(*id) && !listed.contains(id) {
            return Err(invalid(format!(
                "command `{id}` has no example and is not listed in {MISSING_EXAMPLES_FILE}; add examples/{id}.yaml or list it"
            )));
        }
    }
    Ok(())
}

/// Replays every example against a scratch cache executor: each step must
/// execute, a `returns: null` step must produce a miss, and a step with a
/// non-null `returns` must produce a value. Exact-value assertions live in the
/// SDK doctest harness, which renders the same spec to its own calls.
pub(super) fn verify_examples(
    repo_root: &Path,
    index: &CommandIndex,
    schemas: &BTreeMap<String, Value>,
) -> Result<()> {
    let examples = load_examples(repo_root)?;
    validate_examples(repo_root, index, schemas, &examples)?;
    for (id, example) in &examples {
        // A per-example scratch dir backs `{tmpdir}` file-path placeholders (a
        // round-trip export/import writes and reads within one example).
        let tmpdir = tempfile::tempdir()
            .map_err(|error| invalid(format!("example `{id}`: scratch dir failed: {error}")))?;
        let tmpdir_path = tmpdir.path().to_string_lossy();
        let mut executor = Executor::open_cache().map_err(|error| {
            invalid(format!("example `{id}`: scratch executor failed: {error}"))
        })?;
        let mut bindings = BTreeMap::new();
        for (position, step) in example.steps.iter().enumerate() {
            let schema = schemas
                .get(&step.call)
                .ok_or_else(|| invalid(format!("example `{id}`: no schema for `{}`", step.call)))?;
            let wire = step_wire_json(id, position, step, schema, &tmpdir_path, &bindings)?;
            let command: Command =
                serde_json::from_value(wire).map_err(|source| IdlError::Json {
                    path: PathBuf::from(format!("examples/{id}.yaml")),
                    source,
                })?;
            let output = executor.execute(command).map_err(|error| {
                invalid(format!(
                    "example `{id}` step {position} (`{}`) failed to execute: {error}",
                    step.call
                ))
            })?;
            let value = serde_json::to_value(&output).map_err(|source| IdlError::Json {
                path: PathBuf::from(format!("examples/{id}.yaml")),
                source,
            })?;
            if let Some(expected) = &step.returns {
                let expect_miss = matches!(expected, ExpectedResult::Miss);
                assert_result(id, position, step, expect_miss, &value)?;
            }
            if let Some(name) = &step.bind {
                if let Some(data) = value.get("data") {
                    bindings.insert(name.clone(), data.clone());
                }
            }
        }
    }
    Ok(())
}

/// One example step with its rendered CLI input and the wire output it produced
/// when replayed against a scratch cache executor (#3059).
pub struct CapturedStep {
    /// Rendered CLI invocation (the `$ strata …` line).
    pub cli_input: String,
    /// The command's wire name (`kv_put`), which names its CLI `display:`
    /// declaration.
    pub wire: String,
    /// The command's wire request (serialized `Command`); a declared receipt
    /// may quote it.
    pub request: Value,
    /// The command's wire output envelope (serialized `Output`).
    pub wire_output: Value,
    /// Optional comment shown as `# …` after the CLI line, verbatim from the
    /// example (the same text `verify_examples` renders).
    pub note: Option<String>,
}

/// An example's captured input/output run, keyed by command id.
pub struct CapturedExample {
    /// Command id (`kv.put`).
    pub command_id: String,
    /// Steps in order.
    pub steps: Vec<CapturedStep>,
}

/// Replays every hermetic example against a scratch cache executor and captures
/// each step's rendered CLI input plus the wire output it produced, so the docs
/// bundle can ship what each example prints (#3059). Reuses the same replay
/// path as `verify_examples`; commands whose output is not hermetic are already
/// excluded by the coverage allowlist, so every loaded example replays here.
pub(super) fn capture_example_runs(
    repo_root: &Path,
    index: &CommandIndex,
    schemas: &BTreeMap<String, Value>,
    arg_spec: &CliArgSpec,
) -> Result<Vec<CapturedExample>> {
    let examples = load_examples(repo_root)?;
    validate_examples(repo_root, index, schemas, &examples)?;
    let by_id: BTreeMap<&str, &ResolvedCommand> = index
        .commands
        .iter()
        .map(|command| (command.id.as_str(), command))
        .collect();

    let mut runs = Vec::new();
    for (id, example) in &examples {
        let tmpdir = tempfile::tempdir()
            .map_err(|error| invalid(format!("example `{id}`: scratch dir failed: {error}")))?;
        let tmpdir_path = tmpdir.path().to_string_lossy();
        let mut executor = Executor::open_cache().map_err(|error| {
            invalid(format!("example `{id}`: scratch executor failed: {error}"))
        })?;
        let bindings = resolve_example_bindings(example, schemas);
        let mut steps = Vec::new();
        for (position, step) in example.steps.iter().enumerate() {
            let schema = schemas
                .get(&step.call)
                .ok_or_else(|| invalid(format!("example `{id}`: no schema for `{}`", step.call)))?;
            let cli_input = render_cli(&by_id, schemas, step, &bindings, arg_spec);
            let request = step_wire_json(id, position, step, schema, &tmpdir_path, &bindings)?;
            let command: Command =
                serde_json::from_value(request.clone()).map_err(|source| IdlError::Json {
                    path: PathBuf::from(format!("examples/{id}.yaml")),
                    source,
                })?;
            let wire = command.name().to_owned();
            let output = executor.execute(command).map_err(|error| {
                invalid(format!(
                    "example `{id}` step {position} (`{}`) failed to execute: {error}",
                    step.call
                ))
            })?;
            let wire_output = serde_json::to_value(&output).map_err(|source| IdlError::Json {
                path: PathBuf::from(format!("examples/{id}.yaml")),
                source,
            })?;
            steps.push(CapturedStep {
                cli_input,
                wire,
                request,
                wire_output,
                note: step.note.clone(),
            });
        }
        runs.push(CapturedExample {
            command_id: id.clone(),
            steps,
        });
    }
    Ok(runs)
}

/// Whether a point-read output represents a miss. The canonical `Maybe`
/// envelope serializes absence as `{found: false, value: null}` (absence never
/// aliases a bare `null`), so `data.found == false` is the authoritative flag;
/// a bare-null `data` is treated as a miss too.
fn is_miss(output: &Value) -> bool {
    match output.get("data") {
        Some(Value::Object(map)) => matches!(map.get("found"), Some(Value::Bool(false))),
        Some(Value::Null) => true,
        _ => false,
    }
}

fn assert_result(
    id: &str,
    position: usize,
    step: &ExampleStep,
    expect_miss: bool,
    output: &Value,
) -> Result<()> {
    let miss = is_miss(output);
    if expect_miss && !miss {
        return Err(invalid(format!(
            "example `{id}` step {position} (`{}`): declared `returns: null` but the call returned a value",
            step.call
        )));
    }
    if !expect_miss && miss {
        return Err(invalid(format!(
            "example `{id}` step {position} (`{}`): declared a value but the call returned a miss",
            step.call
        )));
    }
    Ok(())
}

/// Representative scratch directory shown in rendered docs wherever a spec
/// uses the `{tmpdir}` path placeholder; the replay substitutes a real
/// per-example temp dir instead.
const DOC_TMPDIR: &str = "/tmp/exports";

/// Substitutes the `{tmpdir}` placeholder in string arg values (at any depth)
/// with `tmpdir`. File-path arguments are written as `{tmpdir}/<file>` so the
/// replay targets a real scratch dir and the docs render a representative one.
fn resolve_tmpdir(value: &Value, tmpdir: &str) -> Value {
    match value {
        Value::String(text) if text.contains("{tmpdir}") => {
            Value::String(text.replace("{tmpdir}", tmpdir))
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| resolve_tmpdir(item, tmpdir))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), resolve_tmpdir(item, tmpdir)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Replaces a `{name.path.to.field}` reference string with the value navigated
/// from `bindings[name]` (a prior `bind:` step's `data`). A partial placeholder
/// like `{tmpdir}/f` (no wrapping braces) or an unbound name passes through.
fn resolve_refs(value: &Value, bindings: &BTreeMap<String, Value>) -> Value {
    match value {
        Value::String(text) => {
            let Some(path) = text.strip_prefix('{').and_then(|t| t.strip_suffix('}')) else {
                return value.clone();
            };
            let mut parts = path.split('.');
            let Some(root) = parts.next().and_then(|name| bindings.get(name)) else {
                return value.clone();
            };
            let mut current = root;
            for key in parts {
                match current.get(key) {
                    Some(next) => current = next,
                    None => return value.clone(),
                }
            }
            current.clone()
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| resolve_refs(item, bindings))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), resolve_refs(item, bindings)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Builds the wire command JSON for a step (`{ "type": <tag>, <args> }`),
/// base64-encoding `Bytes` arguments, substituting the `{tmpdir}` path
/// placeholder, and resolving `{name.path}` references against `bindings`.
fn step_wire_json(
    id: &str,
    position: usize,
    step: &ExampleStep,
    schema: &Value,
    tmpdir: &str,
    bindings: &BTreeMap<String, Value>,
) -> Result<Value> {
    let props = request_properties(schema).ok_or_else(|| {
        invalid(format!(
            "example `{id}` step {position}: `{}` has no request properties",
            step.call
        ))
    })?;
    let tag = props
        .get("type")
        .and_then(|t| t.get("const"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            invalid(format!(
                "example `{id}` step {position}: `{}` has no wire tag",
                step.call
            ))
        })?;
    let defs = schema.get("$defs").and_then(Value::as_object);
    let mut object = Map::new();
    object.insert("type".to_owned(), Value::String(tag.to_owned()));
    for (name, value) in &step.args {
        let resolved = resolve_refs(&resolve_tmpdir(value, tmpdir), bindings);
        let encoded = encode_arg(&resolved, props.get(name), defs);
        object.insert(name.clone(), encoded);
    }
    Ok(Value::Object(object))
}

/// Encodes an argument for the wire against its schema, resolving `$ref`s and
/// recursing through arrays, objects, and `anyOf` so a `Bytes` field at any
/// depth (e.g. `entries[].value`, `keys[]`, a nullable `prefix`) is
/// base64-encoded. Non-`Bytes` values pass through unchanged.
fn encode_arg(value: &Value, schema: Option<&Value>, defs: Option<&Map<String, Value>>) -> Value {
    let Some(schema) = schema else {
        return value.clone();
    };
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let name = reference.rsplit('/').next().unwrap_or(reference);
        if name == "Bytes" {
            return match value {
                Value::String(text) => {
                    Value::String(base64::engine::general_purpose::STANDARD.encode(text.as_bytes()))
                }
                other => other.clone(),
            };
        }
        return match defs.and_then(|d| d.get(name)) {
            Some(target) => encode_arg(value, Some(target), defs),
            None => value.clone(),
        };
    }
    if let Some(arms) = schema.get("anyOf").and_then(Value::as_array) {
        for arm in arms {
            if arm.get("type").and_then(Value::as_str) != Some("null") {
                return encode_arg(value, Some(arm), defs);
            }
        }
        return value.clone();
    }
    if schema_type_is(schema, "array") {
        if let Value::Array(items) = value {
            let item_schema = schema.get("items");
            return Value::Array(
                items
                    .iter()
                    .map(|item| encode_arg(item, item_schema, defs))
                    .collect(),
            );
        }
        return value.clone();
    }
    if schema_type_is(schema, "object") {
        if let (Value::Object(object), Some(props)) =
            (value, schema.get("properties").and_then(Value::as_object))
        {
            let mut out = Map::new();
            for (key, val) in object {
                out.insert(key.clone(), encode_arg(val, props.get(key), defs));
            }
            return Value::Object(out);
        }
        return value.clone();
    }
    value.clone()
}

fn schema_type_is(schema: &Value, wanted: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(name)) => name == wanted,
        Some(Value::Array(names)) => names.iter().any(|value| value.as_str() == Some(wanted)),
        _ => false,
    }
}

/// Renders the `## Examples` section (CLI + wire tabs) for one command, given
/// the whole catalog (steps may call sibling commands) and schemas.
/// Best-effort replay of an example against a scratch cache executor, returning
/// the `data`-level output of every `bind:` step keyed by name — so doc
/// rendering resolves `{name.path}` references to the value they hold at replay.
/// Empty when nothing binds; render still succeeds if replay errors (the
/// verify-examples guard is the authority on replay correctness).
fn resolve_example_bindings(
    example: &Example,
    schemas: &BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let mut bindings = BTreeMap::new();
    if example.steps.iter().all(|step| step.bind.is_none()) {
        return bindings;
    }
    let Ok(tmpdir) = tempfile::tempdir() else {
        return bindings;
    };
    let tmpdir_path = tmpdir.path().to_string_lossy();
    let Ok(mut executor) = Executor::open_cache() else {
        return bindings;
    };
    for step in &example.steps {
        let Some(schema) = schemas.get(&step.call) else {
            continue;
        };
        let Ok(wire) = step_wire_json("", 0, step, schema, &tmpdir_path, &bindings) else {
            continue;
        };
        let Ok(command) = serde_json::from_value::<Command>(wire) else {
            continue;
        };
        let Ok(output) = executor.execute(command) else {
            continue;
        };
        if let Some(name) = &step.bind {
            if let Ok(value) = serde_json::to_value(&output) {
                if let Some(data) = value.get("data") {
                    bindings.insert(name.clone(), data.clone());
                }
            }
        }
    }
    bindings
}

pub(super) fn render_section(
    id: &str,
    by_id: &BTreeMap<&str, &ResolvedCommand>,
    schemas: &BTreeMap<String, Value>,
    example: &Example,
    arg_spec: &CliArgSpec,
    transcript: &[TranscriptStep],
) -> Result<String> {
    if transcript.len() != example.steps.len() {
        return Err(stale_transcript(&format!(
            "`{id}` has {} example steps but {} captured",
            example.steps.len(),
            transcript.len()
        )));
    }
    let bindings = resolve_example_bindings(example, schemas);
    let mut out = String::from("## Examples\n\n");
    if let Some(caption) = &example.caption {
        out.push_str(caption.trim());
        out.push_str("\n\n");
    }

    // The CLI tab is a transcript, not a command list: each line is followed by
    // what it printed, so a reader sees the same text the binary produces
    // (#3314 R7). Both halves derive — the input from the step spec, the output
    // from the replay `command-examples.json` captured — and the two are checked
    // against each other here, so a stale artifact fails the docs gate instead
    // of publishing output that belongs to some earlier rendering.
    out.push_str("### CLI\n\n```console\n");
    let mut varies = false;
    for (step, captured) in example.steps.iter().zip(transcript) {
        let line = render_cli(by_id, schemas, step, &bindings, arg_spec);
        if captured.input != line {
            return Err(stale_transcript(&format!(
                "`{id}` renders the step as `{line}` but it was captured as `{}`",
                captured.input
            )));
        }
        let note = captured
            .note
            .as_deref()
            .map_or_else(String::new, |n| format!("  # {n}"));
        writeln!(out, "$ {line}{note}").expect(INFALLIBLE);
        varies |= !captured.reproducible;
        if !captured.out.is_empty() {
            out.push_str(&captured.out);
            out.push('\n');
        }
    }
    out.push_str("```\n\n");
    if varies {
        out.push_str(ELISION_LEGEND);
    }
    out.push_str("### Wire\n\n```json\n");
    for step in &example.steps {
        if let Some(schema) = schemas.get(&step.call) {
            if let Ok(wire) = step_wire_json("", 0, step, schema, DOC_TMPDIR, &bindings) {
                writeln!(out, "{}", compact(&wire)).expect(INFALLIBLE);
            }
        }
    }
    out.push_str("```\n\n");
    Ok(out)
}

/// Shown under a transcript that elided something, so `…` is never mistaken
/// for text the binary printed.
const ELISION_LEGEND: &str =
    "`…` stands for a value that varies by run or by machine — an instant, a version, an id, \
     a path.\n\n";

fn stale_transcript(detail: &str) -> IdlError {
    invalid(format!(
        "command-examples.json is stale: {detail}. Regenerate it with `cargo test -p strata-cli \
         --lib command_examples -- --ignored regenerate`, which replays every example."
    ))
}

/// Whether a clap verb can spell every field the step supplies — each must be
/// one of the verb's positionals or a known flag. When any field is not exposed
/// (a wire-only capability, a different arg shape), the caller falls back to
/// `command run` (#3073). Pure over the spec so the decision is unit-testable.
fn all_fields_expressible<'a>(spec: &VerbArgSpec, fields: impl Iterator<Item = &'a str>) -> bool {
    fields.into_iter().all(|field| {
        spec.positionals
            .iter()
            .any(|positional| positional == field)
            || spec.flags.contains_key(field)
    })
}

fn render_cli(
    by_id: &BTreeMap<&str, &ResolvedCommand>,
    schemas: &BTreeMap<String, Value>,
    step: &ExampleStep,
    bindings: &BTreeMap<String, Value>,
    arg_spec: &CliArgSpec,
) -> String {
    let Some(command) = by_id.get(step.call.as_str()) else {
        return step.call.clone();
    };
    let schema = schemas.get(&step.call);
    let resolve = |value: &Value| resolve_refs(&resolve_tmpdir(value, DOC_TMPDIR), bindings);
    let command_run = || {
        // The escape-hatch CLI form: the generic runner over the exact wire
        // JSON. Used for wire-only commands and for any verb step whose args a
        // clap verb cannot spell (#3073).
        schema
            .and_then(|s| step_wire_json("", 0, step, s, DOC_TMPDIR, bindings).ok())
            .map(|wire| format!("strata command run --command-json '{}'", compact(&wire)))
    };

    // A verb renders as its real clap invocation — but only when the clap tree
    // (via `cli-arg-spec.json`) actually spells every arg the step supplies.
    // Otherwise (a renamed field aside, a field the verb does not expose, a
    // wire-only command) fall back to the runner so the example still runs.
    let spec = (command.cli.surface == "verb")
        .then(|| arg_spec.verbs.get(&command.cli.path.join(" ")))
        .flatten();
    let Some(spec) = spec else {
        return command_run().unwrap_or_else(|| step.call.clone());
    };
    if !all_fields_expressible(spec, step.args.keys().map(String::as_str)) {
        return command_run().unwrap_or_else(|| step.call.clone());
    }

    let mut tokens = vec![String::from("strata")];
    tokens.extend(command.cli.path.iter().cloned());
    for field in &spec.positionals {
        if let Some(value) = step.args.get(field) {
            tokens.push(cli_token(&resolve(value)));
        }
    }
    for (field, flag) in &spec.flags {
        if let Some(value) = step.args.get(field) {
            tokens.push(format!("--{flag} {}", cli_token(&resolve(value))));
        }
    }
    tokens.join(" ")
}

fn cli_token(value: &Value) -> String {
    let text = match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    if text.is_empty() || text.contains(char::is_whitespace) {
        format!("{text:?}")
    } else {
        text
    }
}

fn request_properties(schema: &Value) -> Option<&Map<String, Value>> {
    schema
        .get("request")
        .and_then(|request| request.get("properties"))
        .and_then(Value::as_object)
}

fn schema_required(schema: &Value) -> Vec<&str> {
    schema
        .get("request")
        .and_then(|request| request.get("required"))
        .and_then(Value::as_array)
        .map_or_else(Vec::new, |array| {
            array.iter().filter_map(Value::as_str).collect()
        })
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn expressibility_gates_the_verb_form_on_the_arg_spec() {
        // graph add-node: `graph`/`node_id` positional, `object_type` a flag —
        // but NO `binding` flag (#3073).
        let spec = VerbArgSpec {
            positionals: vec!["graph".to_owned(), "node_id".to_owned()],
            flags: BTreeMap::from([("object_type".to_owned(), "type".to_owned())]),
        };
        // Every field maps to a positional or flag → renderable as a verb.
        assert!(all_fields_expressible(
            &spec,
            ["graph", "node_id", "object_type"].into_iter()
        ));
        // A field the verb does not expose (binding) → fall back to command run.
        assert!(!all_fields_expressible(
            &spec,
            ["graph", "node_id", "binding"].into_iter()
        ));
        // No args at all is trivially expressible.
        assert!(all_fields_expressible(&spec, std::iter::empty()));
    }

    #[test]
    fn is_miss_reads_the_maybe_found_flag_not_a_bare_null() {
        // The canonical point-read envelope: absence is {found:false,...},
        // presence is {found:true,...} — never a bare null.
        assert!(is_miss(
            &json!({"type": "kv_versioned_value", "data": {"found": false, "value": null}})
        ));
        assert!(!is_miss(
            &json!({"type": "kv_versioned_value", "data": {"found": true, "value": {}}})
        ));
        // A status/scalar read (e.g. exists=false) is never a miss.
        assert!(!is_miss(&json!({"type": "kv_bool", "data": false})));
        // A bare-null data still counts as a miss.
        assert!(is_miss(&json!({"data": null})));
    }

    #[test]
    fn encode_arg_base64s_bytes_at_any_depth() {
        let bytes = json!({"$ref": "#/$defs/Bytes"});
        assert_eq!(encode_arg(&json!("a1"), Some(&bytes), None), json!("YTE=")); // base64("a1")
                                                                                 // Non-Bytes args and unknown schemas pass through unchanged.
        assert_eq!(
            encode_arg(&json!(2), Some(&json!({"type": "integer"})), None),
            json!(2)
        );
        assert_eq!(encode_arg(&json!("raw"), None, None), json!("raw"));

        // Array of Bytes (kv.batch_get `keys`).
        let key_list = json!({"type": "array", "items": {"$ref": "#/$defs/Bytes"}});
        assert_eq!(
            encode_arg(&json!(["a1", "a1"]), Some(&key_list), None),
            json!(["YTE=", "YTE="])
        );

        // Bytes nested in an object behind a $ref (kv.batch_put `entries`).
        let mut defs = Map::new();
        defs.insert(
            "Entry".into(),
            json!({"type": "object", "properties": {"key": {"$ref": "#/$defs/Bytes"}}}),
        );
        let entries = json!({"type": "array", "items": {"$ref": "#/$defs/Entry"}});
        assert_eq!(
            encode_arg(&json!([{"key": "a1"}]), Some(&entries), Some(&defs)),
            json!([{"key": "YTE="}])
        );

        // Bytes inside anyOf (nullable `prefix`).
        let nullable = json!({"anyOf": [{"$ref": "#/$defs/Bytes"}, {"type": "null"}]});
        assert_eq!(
            encode_arg(&json!("a1"), Some(&nullable), None),
            json!("YTE=")
        );
    }

    #[test]
    fn deserialize_expected_distinguishes_absent_null_and_value() {
        #[derive(Deserialize)]
        struct Holder {
            #[serde(default, deserialize_with = "deserialize_expected")]
            returns: Option<ExpectedResult>,
        }
        let absent: Holder = serde_yaml::from_str("{}").expect("absent");
        assert!(absent.returns.is_none());
        let miss: Holder = serde_yaml::from_str("returns: null").expect("null");
        assert!(matches!(miss.returns, Some(ExpectedResult::Miss)));
        let present: Holder = serde_yaml::from_str("returns: hello").expect("value");
        assert!(matches!(present.returns, Some(ExpectedResult::Present)));
    }

    #[test]
    fn capture_examples_replays_examples_with_their_output() {
        // End-to-end guard on the capture pipeline: a body that returns
        // `Ok(vec![])` — the mutation gate's default substitution for both this
        // function and its `capture_examples` wrapper — leaves the docs bundle
        // with no example output, which asserting real captured content catches.
        let runs = super::super::capture_examples(&super::super::default_repo_root())
            .expect("capture examples against the real IDL tree");
        assert!(!runs.is_empty(), "captured no example runs");
        let kv_put = runs
            .iter()
            .find(|run| run.command_id == "kv.put")
            .expect("kv.put example captured");
        let first = kv_put.steps.first().expect("kv.put captured with no steps");
        assert!(
            first.cli_input.contains("kv put"),
            "unexpected cli input: {}",
            first.cli_input
        );
        assert!(
            first.wire_output.is_object(),
            "wire output is not an envelope: {}",
            first.wire_output
        );
    }
}
