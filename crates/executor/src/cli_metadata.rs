//! Runtime CLI command metadata generated from the V1 IDL overlay.

#![deny(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

mod display;
pub use display::{
    validate_display_shape, validate_encoding_shape, CliDisplay, CliDisplayAs, CliDisplayDecl,
    CliDisplayField, CliDisplayShape, CliDisplaySort, CliRenderRule, CliWireEncoding,
};

/// Embedded generated CLI command metadata JSON.
pub const EMBEDDED_CLI_COMMAND_INDEX_JSON: &str =
    include_str!("../idl/v1/generated/cli-command-index.json");

const SUPPORTED_SCHEMA_VERSION: &str = "strata.cli.v1";
const SUPPORTED_GENERATOR_VERSION: &str = "strata-executor-cli-idl.3";
// Crate-visible: the IPC hello echoes these same stamps (protocol.rs), so the
// hello and the embedded command index cannot disagree about the source IDL.
pub(crate) const SUPPORTED_SOURCE_SCHEMA_VERSION: &str = "strata.idl.v1";
pub(crate) const SUPPORTED_SOURCE_GENERATOR_VERSION: &str = "strata-executor-idl.1";

/// CLI metadata runtime result type.
pub type CliMetadataResult<T> = std::result::Result<T, CliMetadataError>;

/// CLI metadata runtime error.
#[derive(Debug, Error)]
pub enum CliMetadataError {
    /// Embedded metadata JSON could not be parsed.
    #[error("failed to parse generated CLI command metadata: {0}")]
    Json(#[from] serde_json::Error),
    /// Embedded metadata is internally inconsistent.
    #[error("invalid generated CLI command metadata: {0}")]
    Invalid(String),
}

/// Parsed and validated CLI command metadata catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliCommandCatalog {
    index: CliCommandIndex,
}

impl CliCommandCatalog {
    /// Parse and validate the embedded generated CLI command metadata.
    pub fn embedded() -> CliMetadataResult<Self> {
        Self::parse(EMBEDDED_CLI_COMMAND_INDEX_JSON)
    }

    /// Parse and validate generated CLI command metadata JSON.
    pub fn parse(json: &str) -> CliMetadataResult<Self> {
        let index: CliCommandIndex = serde_json::from_str(json)?;
        Self::from_index(index)
    }

    /// Validate an already-deserialized CLI command index.
    pub fn from_index(index: CliCommandIndex) -> CliMetadataResult<Self> {
        validate_index(&index)?;
        Ok(Self { index })
    }

    /// Returns the full generated index.
    pub const fn index(&self) -> &CliCommandIndex {
        &self.index
    }

    /// Returns all command entries sorted by CLI path.
    pub fn commands(&self) -> &[CliCommandEntry] {
        &self.index.commands
    }

    /// Returns all command family groups sorted by family id.
    pub fn families(&self) -> &[CliFamilyGroup] {
        &self.index.families
    }

    /// Finds a command by stable command id.
    pub fn command_by_id(&self, id: &str) -> Option<&CliCommandEntry> {
        self.index
            .lookup
            .by_id
            .get(id.trim())
            .and_then(|offset| self.index.commands.get(*offset))
    }

    /// Finds a command by CLI path segments.
    pub fn command_by_path<'a>(
        &self,
        path: impl IntoIterator<Item = &'a str>,
    ) -> Option<&CliCommandEntry> {
        let key = path
            .into_iter()
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        self.command_by_path_display(&key)
    }

    /// Finds a command by display path such as `kv put`.
    pub fn command_by_path_display(&self, path: &str) -> Option<&CliCommandEntry> {
        self.index
            .lookup
            .by_path
            .get(normalize_space(path).as_str())
            .and_then(|id| self.command_by_id(id))
    }

    /// Finds a command by stable id or CLI path display.
    pub fn command(&self, selector: &str) -> Option<&CliCommandEntry> {
        let selector = selector.trim();
        if selector.is_empty() {
            return None;
        }
        self.command_by_id(selector)
            .or_else(|| self.command_by_path_display(selector))
    }

    /// Returns a family group by family id.
    pub fn family(&self, family_id: &str) -> Option<&CliFamilyGroup> {
        let family_id = family_id.trim();
        self.index
            .families
            .iter()
            .find(|family| family.id == family_id)
    }

    /// Returns command entries in a family in CLI listing order.
    pub fn commands_for_family(&self, family_id: &str) -> Option<Vec<&CliCommandEntry>> {
        let family = self.family(family_id)?;
        let commands = family
            .commands
            .iter()
            .filter_map(|id| self.command_by_id(id))
            .collect::<Vec<_>>();
        Some(commands)
    }

    /// Returns deterministic command id and CLI path suggestions.
    pub fn suggestions(&self, selector: &str, limit: usize) -> CliCommandSuggestions {
        let selector = selector.trim();
        if selector.is_empty() || limit == 0 {
            return CliCommandSuggestions::default();
        }

        CliCommandSuggestions {
            command_ids: ranked_suggestions(
                selector,
                self.index
                    .commands
                    .iter()
                    .map(|command| command.id.as_str()),
                limit,
            ),
            paths: ranked_suggestions(
                selector,
                self.index
                    .commands
                    .iter()
                    .map(|command| command.path_display.as_str()),
                limit,
            ),
        }
    }
}

/// Deterministic command suggestions for an unknown CLI selector.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliCommandSuggestions {
    /// Suggested stable command ids.
    pub command_ids: Vec<String>,
    /// Suggested CLI path displays.
    pub paths: Vec<String>,
}

/// Generated CLI command metadata index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliCommandIndex {
    /// Generated-file marker.
    pub generated: bool,
    /// CLI artifact schema version.
    pub schema_version: String,
    /// CLI artifact generator version.
    pub generator_version: String,
    /// Source command-index facts.
    pub source: CliIndexSourceInfo,
    /// Number of commands included.
    pub command_count: usize,
    /// Command families sorted by family id.
    pub families: Vec<CliFamilyGroup>,
    /// Commands sorted by CLI path.
    pub commands: Vec<CliCommandEntry>,
    /// Lookup tables for runtime command resolution.
    pub lookup: CliLookupTables,
}

/// Source command-index facts for generated CLI metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliIndexSourceInfo {
    /// Source command-index path relative to the repo root.
    pub path: String,
    /// SHA-256 checksum of the source command-index JSON bytes.
    pub checksum_sha256: String,
    /// Source command-index schema version.
    pub schema_version: String,
    /// Source command-index generator version.
    pub generator_version: String,
}

/// CLI command family group.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliFamilyGroup {
    /// Family id.
    pub id: String,
    /// Number of commands in the family.
    pub command_count: usize,
    /// Command ids in CLI listing order.
    pub commands: Vec<String>,
}

/// CLI command entry optimized for command discovery and explanation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliCommandEntry {
    /// Stable command id.
    pub id: String,
    /// CLI path segments.
    pub path: Vec<String>,
    /// Display form of the CLI path.
    pub path_display: String,
    /// Which surface implements the path: `verb` (real clap subcommand)
    /// or `wire` (generic `command run`, MCP, SDKs only).
    pub surface: String,
    /// Command family.
    pub family: String,
    /// Operation id within the family.
    pub op: String,
    /// Operation kind id.
    pub kind: String,
    /// User-facing title.
    pub title: String,
    /// One-line summary.
    pub summary: String,
    /// Long Markdown description.
    pub description: String,
    /// Documentation path.
    pub docs: String,
    /// Product feature.
    pub feature: String,
    /// Access mode.
    pub access: String,
    /// Commit behavior.
    pub commit: String,
    /// Pagination behavior.
    pub pagination: String,
    /// Batch behavior.
    pub batch: String,
    /// Executor command variant reference.
    pub input: String,
    /// Executable wire name — the `type` literal a caller serializes to invoke
    /// this command (e.g. `kv_list`), distinct from the dotted `id` and path.
    pub wire: String,
    /// All executor output variants this command can produce on the current wire.
    pub outputs: Vec<String>,
    /// Shared response concept.
    pub response_model: String,
    /// Registered public errors that can be surfaced for this command.
    pub errors: Vec<CliErrorRef>,
    /// Checked-in fixture references.
    pub fixtures: CliFixtureRefs,
    /// Stability marker for the current executor wire shape.
    pub wire_status: String,
    /// Layout rule inherited from the command's kind (`render:`).
    pub render: CliRenderRule,
    /// The command's display declaration (`display:`), validated against
    /// `render` and, at authoring time, against the generated schema.
    pub display: CliDisplayDecl,
    /// How an `optional`/`history` value is spelled on the wire, resolved from
    /// the generated schema; `null` under every other rule.
    pub encoding: Option<CliWireEncoding>,
}

/// Registered public error reference for CLI metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliErrorRef {
    /// Public error code.
    pub code: String,
    /// Stable docs URL.
    pub docs: String,
}

/// Request/response fixture references for CLI metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliFixtureRefs {
    /// Request fixture path relative to `crates/executor/tests/fixtures`.
    pub request: String,
    /// Primary response fixture path relative to `crates/executor/tests/fixtures`.
    pub response: String,
    /// Request fixtures replayed before the primary request (behavior guard).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup: Vec<serde_json::Value>,
    /// Reason this entry is not replayed by the behavior guard, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_skip: Option<String>,
    /// Executed request/response fixture pairs (behavior guard).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<serde_json::Value>,
    /// Requests that must fail, each pinned to the structured fields of the
    /// `ErrorStatus` envelope they produce (error-envelope replay guard).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub error_cases: Vec<serde_json::Value>,
    /// Alternate response fixtures for commands with multiple current wire outputs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses: Vec<String>,
}

/// CLI runtime lookup tables.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliLookupTables {
    /// Command id to entry offset.
    pub by_id: BTreeMap<String, usize>,
    /// CLI path display string to command id.
    pub by_path: BTreeMap<String, String>,
}

fn validate_index(index: &CliCommandIndex) -> CliMetadataResult<()> {
    if !index.generated {
        return Err(invalid("CLI command index must be marked generated"));
    }
    if index.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported CLI command index schema `{}`",
            index.schema_version
        )));
    }
    if index.generator_version != SUPPORTED_GENERATOR_VERSION {
        return Err(invalid(format!(
            "unsupported CLI command index generator `{}`",
            index.generator_version
        )));
    }
    if index.command_count != index.commands.len() {
        return Err(invalid(format!(
            "command_count {} does not match {} command entries",
            index.command_count,
            index.commands.len()
        )));
    }
    if index.commands.is_empty() {
        return Err(invalid("CLI command index must include commands"));
    }
    validate_source(&index.source)?;
    validate_commands(index)?;
    validate_families(index)?;
    validate_lookups(index)
}

fn validate_source(source: &CliIndexSourceInfo) -> CliMetadataResult<()> {
    if source.path != "crates/executor/idl/v1/generated/command-index.json" {
        return Err(invalid(format!(
            "unexpected CLI metadata source path `{}`",
            source.path
        )));
    }
    if source.checksum_sha256.len() != 64
        || !source
            .checksum_sha256
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
    {
        return Err(invalid("source checksum must be lowercase SHA-256 hex"));
    }
    if source.schema_version != SUPPORTED_SOURCE_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported source command index schema `{}`",
            source.schema_version
        )));
    }
    if source.generator_version != SUPPORTED_SOURCE_GENERATOR_VERSION {
        return Err(invalid(format!(
            "unsupported source command index generator `{}`",
            source.generator_version
        )));
    }
    Ok(())
}

fn validate_commands(index: &CliCommandIndex) -> CliMetadataResult<()> {
    let mut seen_ids = BTreeSet::new();
    let mut seen_paths = BTreeSet::new();
    let mut previous_path: Option<&[String]> = None;
    for command in &index.commands {
        validate_command(command)?;
        if !seen_ids.insert(command.id.as_str()) {
            return Err(invalid(format!("duplicate command id `{}`", command.id)));
        }
        if !seen_paths.insert(command.path_display.as_str()) {
            return Err(invalid(format!(
                "duplicate CLI path `{}`",
                command.path_display
            )));
        }
        if let Some(previous) = previous_path {
            if previous > command.path.as_slice() {
                return Err(invalid("commands must be sorted by CLI path"));
            }
        }
        previous_path = Some(command.path.as_slice());
    }
    Ok(())
}

fn validate_command(command: &CliCommandEntry) -> CliMetadataResult<()> {
    validate_id_like("command id", &command.id)?;
    validate_id_like("family", &command.family)?;
    validate_id_like("operation", &command.op)?;
    let expected_id = format!("{}.{}", command.family, command.op);
    if command.id != expected_id {
        return Err(invalid(format!(
            "command `{}` identity does not match family/op `{expected_id}`",
            command.id
        )));
    }
    validate_non_empty("kind", &command.kind)?;
    validate_non_empty("title", &command.title)?;
    validate_non_empty("summary", &command.summary)?;
    validate_non_empty("description", &command.description)?;
    validate_docs_path(&command.docs)?;
    validate_cli_path(&command.path)?;
    if command.path_display != command.path.join(" ") {
        return Err(invalid(format!(
            "command `{}` path_display does not match path",
            command.id
        )));
    }
    validate_non_empty("feature", &command.feature)?;
    validate_non_empty("access", &command.access)?;
    validate_non_empty("commit", &command.commit)?;
    validate_non_empty("pagination", &command.pagination)?;
    validate_non_empty("batch", &command.batch)?;
    validate_executor_ref("input", &command.input, "Command::")?;
    validate_wire_name(&command.wire, &command.input, &command.id)?;
    if command.outputs.is_empty() {
        return Err(invalid(format!("command `{}` has no outputs", command.id)));
    }
    for output in &command.outputs {
        validate_executor_ref("output", output, "Output::")?;
    }
    validate_non_empty("response_model", &command.response_model)?;
    if command.errors.is_empty() {
        return Err(invalid(format!("command `{}` has no errors", command.id)));
    }
    for error in &command.errors {
        validate_non_empty("error code", &error.code)?;
        if !error.docs.starts_with("https://stratadb.org/e/") {
            return Err(invalid(format!(
                "command `{}` has invalid error docs URL `{}`",
                command.id, error.docs
            )));
        }
    }
    validate_fixture_path(&command.fixtures.request, "requests/v1")?;
    validate_fixture_path(&command.fixtures.response, "responses/v1")?;
    for response in &command.fixtures.responses {
        validate_fixture_path(response, "responses/v1")?;
    }
    match command.wire_status.as_str() {
        "stable" | "transitional" => {}
        _ => {
            return Err(invalid(format!(
                "command `{}` has invalid wire_status `{}`",
                command.id, command.wire_status
            )))
        }
    }
    validate_display_shape(&command.id, command.render, &command.display).map_err(invalid)?;
    validate_encoding_shape(
        &command.id,
        command.render,
        command.encoding,
        &command.wire_status,
    )
    .map_err(invalid)
}

fn validate_families(index: &CliCommandIndex) -> CliMetadataResult<()> {
    let command_families = index
        .commands
        .iter()
        .map(|command| (command.id.as_str(), command.family.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut grouped_command_ids = BTreeSet::new();
    for family in &index.families {
        validate_id_like("family", &family.id)?;
        if !seen.insert(family.id.as_str()) {
            return Err(invalid(format!("duplicate family `{}`", family.id)));
        }
        if family.command_count != family.commands.len() {
            return Err(invalid(format!(
                "family `{}` command_count does not match commands",
                family.id
            )));
        }
        if family.commands.is_empty() {
            return Err(invalid(format!("family `{}` has no commands", family.id)));
        }
        for command_id in &family.commands {
            let Some(command_family) = command_families.get(command_id.as_str()) else {
                return Err(invalid(format!(
                    "family `{}` references unknown command `{command_id}`",
                    family.id
                )));
            };
            if *command_family != family.id {
                return Err(invalid(format!(
                    "family `{}` references command `{command_id}` owned by family `{command_family}`",
                    family.id
                )));
            }
            if !grouped_command_ids.insert(command_id.as_str()) {
                return Err(invalid(format!(
                    "command `{command_id}` appears in more than one family group"
                )));
            }
        }
    }
    let all_command_ids = command_families.keys().copied().collect::<BTreeSet<_>>();
    if grouped_command_ids != all_command_ids {
        let missing = all_command_ids
            .difference(&grouped_command_ids)
            .copied()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(invalid(format!(
            "family groups do not cover every command; missing `{missing}`"
        )));
    }
    Ok(())
}

fn validate_lookups(index: &CliCommandIndex) -> CliMetadataResult<()> {
    if index.lookup.by_id.len() != index.commands.len()
        || index.lookup.by_path.len() != index.commands.len()
    {
        return Err(invalid("lookup tables must cover every command"));
    }
    for (offset, command) in index.commands.iter().enumerate() {
        if index.lookup.by_id.get(&command.id) != Some(&offset) {
            return Err(invalid(format!(
                "by_id lookup for `{}` points to the wrong offset",
                command.id
            )));
        }
        if index.lookup.by_path.get(&command.path_display) != Some(&command.id) {
            return Err(invalid(format!(
                "by_path lookup for `{}` points to the wrong command",
                command.path_display
            )));
        }
    }
    Ok(())
}

fn validate_id_like(field: &str, value: &str) -> CliMetadataResult<()> {
    validate_non_empty(field, value)?;
    if value
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '.')
    {
        Ok(())
    } else {
        Err(invalid(format!("{field} `{value}` has invalid characters")))
    }
}

fn validate_non_empty(field: &str, value: &str) -> CliMetadataResult<()> {
    if value.trim().is_empty() || value.contains('{') || value.contains('}') {
        Err(invalid(format!("{field} is empty or unresolved")))
    } else {
        Ok(())
    }
}

fn validate_docs_path(path: &str) -> CliMetadataResult<()> {
    if path.starts_with("/docs/") {
        Ok(())
    } else {
        Err(invalid(format!(
            "docs path `{path}` must start with /docs/"
        )))
    }
}

fn validate_cli_path(path: &[String]) -> CliMetadataResult<()> {
    if path.is_empty() {
        return Err(invalid("CLI path must not be empty"));
    }
    for segment in path {
        if segment.is_empty()
            || !segment
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
        {
            return Err(invalid(format!("invalid CLI path segment `{segment}`")));
        }
    }
    Ok(())
}

fn validate_executor_ref(field: &str, value: &str, prefix: &str) -> CliMetadataResult<()> {
    let Some(name) = value.strip_prefix(prefix) else {
        return Err(invalid(format!(
            "{field} `{value}` must start with {prefix}"
        )));
    };
    if name
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(invalid(format!(
            "{field} `{value}` has invalid variant name"
        )))
    }
}

/// Confirms the published wire name is the `snake_case` of the `input` variant,
/// so a catalog reader can serialize `{"type": wire}` and reach this command.
fn validate_wire_name(wire: &str, input: &str, command_id: &str) -> CliMetadataResult<()> {
    validate_non_empty("wire", wire)?;
    let Some(variant) = input.strip_prefix("Command::") else {
        return Err(invalid(format!(
            "command `{command_id}` input `{input}` must start with Command::"
        )));
    };
    let expected = pascal_to_snake(variant);
    if wire == expected {
        Ok(())
    } else {
        Err(invalid(format!(
            "command `{command_id}` wire name `{wire}` does not match input `{input}`"
        )))
    }
}

fn pascal_to_snake(name: &str) -> String {
    let mut output = String::with_capacity(name.len());
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index != 0 {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
        } else {
            output.push(ch);
        }
    }
    output
}

fn validate_fixture_path(path: &str, prefix: &str) -> CliMetadataResult<()> {
    if path.starts_with(prefix)
        && std::path::Path::new(path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        && !path.starts_with('/')
        && !path.contains("..")
    {
        Ok(())
    } else {
        Err(invalid(format!(
            "fixture path `{path}` must stay under `{prefix}` and end with .json"
        )))
    }
}

fn ranked_suggestions<'a>(
    selector: &str,
    candidates: impl Iterator<Item = &'a str>,
    limit: usize,
) -> Vec<String> {
    let normalized = normalize_for_score(selector);
    let mut scored = candidates
        .filter(|candidate| *candidate != selector)
        .map(|candidate| {
            let candidate_normalized = normalize_for_score(candidate);
            let score = edit_distance(&normalized, &candidate_normalized);
            (score, candidate.to_owned())
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    scored
        .into_iter()
        .take(limit)
        .map(|(_, candidate)| candidate)
        .collect()
}

fn normalize_space(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_for_score(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace(['.', '_', '-'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn edit_distance(left: &str, right: &str) -> usize {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_byte) in left.iter().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_byte) in right.iter().enumerate() {
            let substitution = usize::from(left_byte != right_byte);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + substitution);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()]
}

fn invalid(message: impl Into<String>) -> CliMetadataError {
    CliMetadataError::Invalid(message.into())
}
