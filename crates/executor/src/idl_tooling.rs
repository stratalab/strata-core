//! Resolver for Strata's thin V1 IDL overlay.

#![deny(unsafe_code)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

use crate::{public_error_code_entries, Command, Output};

mod docs;
mod examples;
pub use examples::{CapturedExample, CapturedStep};
mod schemas;
mod tests_gen;
mod verify;

pub use tests_gen::{check_tests, generate_tests};

const IDL_DIR: &str = "crates/executor/idl/v1";
const FIXTURE_ROOT: &str = "crates/executor/tests/fixtures";
const COMMAND_INDEX_FILE: &str = "command-index.json";
const CLI_COMMAND_INDEX_FILE: &str = "cli-command-index.json";
const UNCOVERED_COMMANDS_FILE: &str = "uncovered-commands.yaml";
const UNCOVERED_ERROR_CODES_FILE: &str = "uncovered-error-codes.yaml";
const UNREPLAYED_ERROR_CODES_FILE: &str = "unreplayed-error-codes.yaml";
const REPLAY_SKIPPED_COMMANDS_FILE: &str = "replay-skipped-commands.yaml";
const CLI_SURFACES: &[&str] = &["verb", "wire"];
const SUPPORTED_COMMAND_SCHEMA_VERSION: &str = "strata.idl.v1";
const SUPPORTED_COMMAND_GENERATOR_VERSION: &str = "strata-executor-idl.1";
const CLI_SCHEMA_VERSION: &str = "strata.cli.v1";
const CLI_GENERATOR_VERSION: &str = "strata-executor-cli-idl.1";
const COMMAND_SOURCE_FIELDS: &[&str] = &[
    "id",
    "kind",
    "title",
    "input",
    "output",
    "outputs",
    "result",
    "prose",
    "wire_status",
    "docs",
    "cli_path",
    "cli_surface",
    "mcp_name",
    "feature",
    "access",
    "commit",
    "pagination",
    "batch",
    "response_model",
    "snippets",
    "errors+",
    "errors-",
    "fixtures",
];

/// IDL resolver result type.
pub type Result<T> = std::result::Result<T, IdlError>;

/// IDL resolver error.
#[derive(Debug, Error)]
pub enum IdlError {
    /// A required file could not be read.
    #[error("failed to read {path}: {source}")]
    Read {
        /// File path.
        path: PathBuf,
        /// I/O error.
        source: std::io::Error,
    },
    /// A required file could not be written.
    #[error("failed to write {path}: {source}")]
    Write {
        /// File path.
        path: PathBuf,
        /// I/O error.
        source: std::io::Error,
    },
    /// A YAML source file is invalid.
    #[error("failed to parse YAML {path}: {source}")]
    Yaml {
        /// File path.
        path: PathBuf,
        /// YAML parse error.
        source: serde_yaml::Error,
    },
    /// JSON could not be serialized or deserialized.
    #[error("failed to process JSON {path}: {source}")]
    Json {
        /// File path.
        path: PathBuf,
        /// JSON error.
        source: serde_json::Error,
    },
    /// Authored IDL is invalid.
    #[error("{0}")]
    Invalid(String),
    /// Generated output is stale.
    #[error(
        "{path} is stale; run `cargo run -p strata-executor --features idl-tooling --bin strata-idl -- generate`"
    )]
    Stale {
        /// Generated file path.
        path: PathBuf,
    },
    /// Generated CLI output is stale.
    #[error(
        "{path} is stale; run `cargo run -p strata-executor --features idl-tooling --bin strata-idl -- generate-cli`"
    )]
    CliStale {
        /// Generated file path.
        path: PathBuf,
    },
    /// Generated reference documentation is stale.
    #[error(
        "{path} is stale; run `cargo run -p strata-executor --features idl-tooling --bin strata-idl -- generate-docs`"
    )]
    DocsStale {
        /// Generated file path.
        path: PathBuf,
    },
}

/// Resolved command index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandIndex {
    /// Generated-file marker.
    pub generated: bool,
    /// Source schema version.
    pub schema_version: String,
    /// Generator version.
    pub generator_version: String,
    /// Resolved commands sorted by stable command id.
    pub commands: Vec<ResolvedCommand>,
}

/// One resolved command entry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResolvedCommand {
    /// Stable command id.
    pub id: String,
    /// Command family.
    pub family: String,
    /// Operation id within the family.
    pub op: String,
    /// Operation kind id.
    pub kind: String,
    /// User-facing title.
    pub title: String,
    /// Short summary from prose frontmatter.
    pub summary: String,
    /// Long Markdown description.
    pub description: String,
    /// Documentation path.
    pub docs: String,
    /// Future CLI routing facts.
    pub cli: CliInfo,
    /// Future MCP metadata.
    pub mcp: McpInfo,
    /// Product feature.
    pub feature: String,
    /// Access mode.
    pub access: String,
    /// Executor command variant reference.
    pub input: String,
    /// Executable wire name — the `type` literal a caller serializes to invoke
    /// this command (e.g. `kv_list`), distinct from the dotted `id` and CLI path.
    pub wire: String,
    /// Executor output variant reference.
    pub output: String,
    /// All executor output variants this command can produce on the current wire.
    pub outputs: Vec<String>,
    /// Stability marker for the current executor wire shape.
    pub wire_status: String,
    /// Shared response concept.
    pub response_model: String,
    /// Commit behavior.
    pub commit: String,
    /// Pagination behavior.
    pub pagination: String,
    /// Batch behavior.
    pub batch: String,
    /// Registered public errors that can be surfaced for this command.
    pub errors: Vec<ErrorRef>,
    /// Checked-in fixture references.
    pub fixtures: FixtureRefs,
    /// Authored source locations.
    pub source: SourceInfo,
}

/// Future CLI routing facts.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct CliInfo {
    /// CLI path segments — the command's logical family/op path, kept for
    /// internal routing (flat catalog, dedup, example rendering). It is
    /// advertised in the generated index ONLY for a real clap verb; a `wire`
    /// surface omits it on the wire (see the `Serialize` impl, #3058).
    #[serde(default)]
    pub path: Vec<String>,
    /// Which surface implements this path: a real clap verb (`verb`) or
    /// the generic wire path only (`wire` — `command run`, MCP, SDKs).
    pub surface: String,
}

impl Serialize for CliInfo {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct as _;
        // #3058: a `wire` command is escape-hatch only (`command run`), so it
        // advertises NO runnable `path`. A consumer that renders `cli.path` as
        // the invocation then never prints a subcommand strata cannot resolve.
        let advertise_path = self.surface == "verb";
        let mut cli = serializer.serialize_struct("CliInfo", 1 + usize::from(advertise_path))?;
        if advertise_path {
            cli.serialize_field("path", &self.path)?;
        }
        cli.serialize_field("surface", &self.surface)?;
        cli.end()
    }
}

/// Future MCP metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct McpInfo {
    /// MCP tool name.
    pub name: String,
    /// MCP tool description.
    pub description: String,
}

/// Registered public error reference.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ErrorRef {
    /// Public error code.
    pub code: String,
    /// Stable docs URL.
    pub docs: String,
}

/// Request/response fixture references.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureRefs {
    /// Request fixture path relative to `crates/executor/tests/fixtures`.
    pub request: String,
    /// Primary response fixture path relative to `crates/executor/tests/fixtures`.
    pub response: String,
    /// Alternate response fixtures for commands with multiple current wire outputs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses: Vec<String>,
    /// Request fixtures replayed against a scratch executor before the
    /// primary request (fixture-behavior guard setup).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup: Vec<String>,
    /// When set, the behavior guard does not replay this entry; the value
    /// states why (nondeterministic wall-clock output, network, filesystem,
    /// feature-gated execution). Schema validation still applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_skip: Option<String>,
    /// Additional executed request/response pairs; every alternate response
    /// in `responses` must be reproduced by one of these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<FixtureCase>,
    /// Requests that must FAIL, each pinned to the stable structured fields of
    /// the `ErrorStatus` envelope they produce (TCP3.8a). Replayed like `cases`
    /// but the execution is expected to return an error, and the guard diffs
    /// the envelope's code/class/retry/commit-outcome against the fixture —
    /// the only replay coverage of the engine->executor error mapping.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub error_cases: Vec<ErrorFixtureCase>,
}

/// One executed fixture pair for the fixture-behavior guard.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureCase {
    /// Setup request fixtures replayed first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup: Vec<String>,
    /// Request fixture executed for this case.
    pub request: String,
    /// Response fixture the execution must reproduce.
    pub response: String,
}

/// One executed error fixture for the fixture-behavior guard: a request whose
/// execution must fail, pinned to the stable structured fields of the resulting
/// `ErrorStatus` (TCP3.8a).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorFixtureCase {
    /// Setup request fixtures replayed first (may seed state the failing
    /// request then trips over).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup: Vec<String>,
    /// Request fixture executed for this case; its execution must return an
    /// error.
    pub request: String,
    /// Fixture pinning the expected stable error fields (code, class,
    /// `retry_policy`, `retryable`, `commit_outcome`). Prose and per-run fields
    /// (`message`, `reference_id`, `trace_id`, `docs_url`) are deliberately not
    /// pinned — they churn and are asserted elsewhere.
    pub expected_error: String,
}

/// Authored source locations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Command YAML path relative to the repo root.
    pub command: String,
    /// Prose Markdown path relative to `crates/executor/idl/v1/prose`.
    pub prose: String,
}

/// Generated CLI command metadata index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

/// Source command-index facts for a generated CLI index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    pub errors: Vec<ErrorRef>,
    /// Checked-in fixture references.
    pub fixtures: FixtureRefs,
    /// Stability marker for the current executor wire shape.
    pub wire_status: String,
}

/// CLI runtime lookup tables.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CliLookupTables {
    /// Command id to entry offset.
    pub by_id: BTreeMap<String, usize>,
    /// CLI path display string to command id.
    pub by_path: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSource {
    schema_version: String,
    generator_version: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LayerFields {
    #[serde(default)]
    docs: Option<String>,
    #[serde(default)]
    cli_path: Option<Vec<String>>,
    #[serde(default)]
    cli_surface: Option<String>,
    #[serde(default)]
    mcp_name: Option<String>,
    #[serde(default)]
    feature: Option<String>,
    #[serde(default)]
    access: Option<String>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default)]
    pagination: Option<String>,
    #[serde(default)]
    batch: Option<String>,
    #[serde(default)]
    response_model: Option<String>,
    #[serde(default)]
    errors: Vec<String>,
    #[serde(default)]
    snippets: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DefaultsSource {
    #[serde(flatten)]
    fields: LayerFields,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FamiliesSource {
    families: Vec<NamedLayerSource>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KindsSource {
    kinds: Vec<NamedLayerSource>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedLayerSource {
    id: String,
    #[serde(flatten)]
    fields: LayerFields,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorsSource {
    errors: Vec<String>,
}

/// `error-sets.yaml`: named error sets (#3250). The reuse layers run
/// defaults → family → kind → command, so a fact that crosses families or
/// kinds ("everything the embeddings runtime can fail with") had no layer to
/// live in and was copied into each command. A set names it once; any error
/// list — a layer's `errors`, a command's `errors+`/`errors-`, or a set
/// declared below it — refers to it as `set:<id>` and the reference expands
/// in place, so generated surfaces keep their shape.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorSetsSource {
    sets: Vec<ErrorSetSource>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorSetSource {
    id: String,
    errors: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DtoInventorySource {
    response_models: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandsFileSource {
    commands: Vec<CommandSource>,
}

/// `uncovered-commands.yaml`: the shrink-only exhaustiveness allowlist.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncoveredCommandsSource {
    uncovered: Vec<String>,
}

/// `uncovered-error-codes.yaml`: the shrink-only error-code exhaustiveness
/// allowlist (drift guard, mirrors `uncovered-commands.yaml`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncoveredErrorCodesSource {
    uncovered: Vec<String>,
}

/// `replay-skipped-commands.yaml`: the shrink-only allowlist of commands whose
/// primary fixture is not replayed by the behavior guard (TCP3.8c). Every
/// command that sets `replay_skip` must be listed here; the list may only
/// shrink, so a new skip cannot be added silently — a command must be made
/// replayable or its skip justified and listed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaySkippedCommandsSource {
    /// Debt-count budget (W0b): MUST equal `skipped.len()`. The per-entry
    /// allowlist below is shrink-only; this budget makes the *total* debt a
    /// reviewed number — growth requires a justified raise in the same change,
    /// and draining forces the budget down to lock in the reduction. Enforced by
    /// `enforce_debt_budget`.
    budget: usize,
    skipped: Vec<String>,
}

/// `unreplayed-error-codes.yaml`: the shrink-only replay-coverage allowlist
/// (TCP3.8b). Distinct from `uncovered-error-codes.yaml`, which tracks whether
/// a code is *documented*; this tracks whether a declared code has an
/// error-case *replay fixture* pinning its `ErrorStatus` envelope. Every code a
/// command declares in `errors[]` must be replayed by at least one error case
/// or listed here; the list may only shrink.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnreplayedErrorCodesSource {
    /// Debt-count budget (W0b): MUST equal `unreplayed.len()`. The per-entry
    /// allowlist below is shrink-only; this budget caps the *total* debt so it
    /// cannot grow silently — see `enforce_debt_budget` (growth needs a reviewed
    /// raise, drains ratchet the budget down).
    budget: usize,
    unreplayed: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandSource {
    id: String,
    kind: String,
    title: String,
    input: String,
    output: String,
    #[serde(default)]
    outputs: Vec<String>,
    result: String,
    prose: String,
    #[serde(default)]
    wire_status: Option<String>,
    #[serde(default)]
    docs: Option<String>,
    #[serde(default)]
    cli_path: Option<Vec<String>>,
    #[serde(default)]
    cli_surface: Option<String>,
    #[serde(default)]
    mcp_name: Option<String>,
    #[serde(default)]
    feature: Option<String>,
    #[serde(default)]
    access: Option<String>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default)]
    pagination: Option<String>,
    #[serde(default)]
    batch: Option<String>,
    #[serde(default)]
    response_model: Option<String>,
    #[serde(default)]
    snippets: Vec<String>,
    #[serde(default, rename = "errors+")]
    errors_add: Vec<String>,
    #[serde(default, rename = "errors-")]
    errors_remove: Vec<String>,
    fixtures: FixtureRefs,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProseFrontmatter {
    summary: String,
    #[serde(default)]
    mcp_description: Option<String>,
}

struct Prose {
    summary: String,
    mcp_description: Option<String>,
    body: String,
}

#[derive(Clone, Debug, Default)]
struct ResolvedLayer {
    docs: Option<String>,
    cli_path: Option<Vec<String>>,
    cli_surface: Option<String>,
    mcp_name: Option<String>,
    feature: Option<String>,
    access: Option<String>,
    commit: Option<String>,
    pagination: Option<String>,
    batch: Option<String>,
    response_model: Option<String>,
    errors: Vec<String>,
    snippets: Vec<String>,
}

impl ResolvedLayer {
    fn apply(&mut self, fields: &LayerFields) {
        if fields.docs.is_some() {
            self.docs.clone_from(&fields.docs);
        }
        if fields.cli_path.is_some() {
            self.cli_path.clone_from(&fields.cli_path);
        }
        if fields.cli_surface.is_some() {
            self.cli_surface.clone_from(&fields.cli_surface);
        }
        if fields.mcp_name.is_some() {
            self.mcp_name.clone_from(&fields.mcp_name);
        }
        if fields.feature.is_some() {
            self.feature.clone_from(&fields.feature);
        }
        if fields.access.is_some() {
            self.access.clone_from(&fields.access);
        }
        if fields.commit.is_some() {
            self.commit.clone_from(&fields.commit);
        }
        if fields.pagination.is_some() {
            self.pagination.clone_from(&fields.pagination);
        }
        if fields.batch.is_some() {
            self.batch.clone_from(&fields.batch);
        }
        if fields.response_model.is_some() {
            self.response_model.clone_from(&fields.response_model);
        }
        append_unique(&mut self.errors, &fields.errors);
        append_unique(&mut self.snippets, &fields.snippets);
    }

    fn apply_command(&mut self, command: &CommandSource) {
        if command.docs.is_some() {
            self.docs.clone_from(&command.docs);
        }
        if command.cli_path.is_some() {
            self.cli_path.clone_from(&command.cli_path);
        }
        if command.cli_surface.is_some() {
            self.cli_surface.clone_from(&command.cli_surface);
        }
        if command.mcp_name.is_some() {
            self.mcp_name.clone_from(&command.mcp_name);
        }
        if command.feature.is_some() {
            self.feature.clone_from(&command.feature);
        }
        if command.access.is_some() {
            self.access.clone_from(&command.access);
        }
        if command.commit.is_some() {
            self.commit.clone_from(&command.commit);
        }
        if command.pagination.is_some() {
            self.pagination.clone_from(&command.pagination);
        }
        if command.batch.is_some() {
            self.batch.clone_from(&command.batch);
        }
        if command.response_model.is_some() {
            self.response_model.clone_from(&command.response_model);
        }
        append_unique(&mut self.errors, &command.errors_add);
        append_unique(&mut self.snippets, &command.snippets);
        if !command.errors_remove.is_empty() {
            self.errors
                .retain(|code| !command.errors_remove.iter().any(|removed| removed == code));
        }
    }
}

/// Returns the repository root inferred from this crate's manifest directory.
#[must_use]
pub fn default_repo_root() -> PathBuf {
    // Test-only escape hatch: lets the bin-dispatch test drive `strata-idl`
    // against a hermetic scratch copy of the IDL tree instead of the real
    // repository (never set in CI invocations of the real gates).
    if let Ok(root) = std::env::var("STRATA_IDL_REPO_ROOT") {
        return PathBuf::from(root);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("executor lives under crates/")
        .to_path_buf()
}

/// Resolves the default IDL source tree.
pub fn resolve_default_index() -> Result<CommandIndex> {
    resolve_index(&default_repo_root())
}

/// Resolves the per-command schema documents for the default IDL tree,
/// keyed by command ID. This is the artifact MCP tool schemas, SDK stubs,
/// and generic command-runner validation consume.
pub fn resolve_default_schemas() -> Result<BTreeMap<String, serde_json::Value>> {
    let index = resolve_index(&default_repo_root())?;
    schemas::schema_documents(&index)
}

/// Resolves the IDL source tree under `repo_root`.
pub fn resolve_index(repo_root: &Path) -> Result<CommandIndex> {
    let idl_root = repo_root.join(IDL_DIR);
    let manifest: ManifestSource = read_yaml(&idl_root.join("manifest.yaml"))?;
    let mut defaults: DefaultsSource = read_yaml(&idl_root.join("defaults.yaml"))?;
    let families: FamiliesSource = read_yaml(&idl_root.join("families.yaml"))?;
    let kinds: KindsSource = read_yaml(&idl_root.join("kinds.yaml"))?;
    let overlay_errors: ErrorsSource = read_yaml(&idl_root.join("errors.yaml"))?;
    let dto_inventory: DtoInventorySource = read_yaml(&idl_root.join("dto-inventory.yaml"))?;

    let mut family_layers = named_layers(families.families, "family")?;
    let mut kind_layers = named_layers(kinds.kinds, "kind")?;
    let registered_errors = registered_error_map();
    validate_error_overlay(&overlay_errors.errors, &registered_errors)?;
    enforce_error_code_exhaustiveness(&idl_root, &overlay_errors.errors, &registered_errors)?;

    // Named error sets expand before layering, so `ResolvedLayer` only ever
    // sees codes and every `set:` reference is resolved in exactly one place.
    let error_sets: ErrorSetsSource = read_yaml(&idl_root.join("error-sets.yaml"))?;
    let mut error_sets = ErrorSets::resolve(error_sets.sets, &overlay_errors.errors)?;
    error_sets.expand_in_place("defaults", &mut defaults.fields.errors)?;
    for (id, layer) in &mut family_layers {
        error_sets.expand_in_place(&format!("family `{id}`"), &mut layer.errors)?;
    }
    for (id, layer) in &mut kind_layers {
        error_sets.expand_in_place(&format!("kind `{id}`"), &mut layer.errors)?;
    }

    let command_refs = enum_variants(&repo_root.join("crates/executor/src/command.rs"))?;
    let output_refs = enum_variants(&repo_root.join("crates/executor/src/output.rs"))?;
    let response_models: BTreeSet<String> = dto_inventory.response_models.into_iter().collect();

    let mut command_entries = Vec::new();
    for file_name in [
        "admin.yaml",
        "arrow.yaml",
        "branch.yaml",
        "event.yaml",
        "graph.yaml",
        "hub.yaml",
        "inference.yaml",
        "json.yaml",
        "kv.yaml",
        "space.yaml",
        "vector.yaml",
    ] {
        let command_path = idl_root.join("commands").join(file_name);
        validate_command_source_text(&command_path)?;
        let command_file: CommandsFileSource = read_yaml(&command_path)?;
        for mut command in command_file.commands {
            let site = format!("command `{}`", command.id);
            error_sets.expand_in_place(&site, &mut command.errors_add)?;
            error_sets.expand_in_place(&site, &mut command.errors_remove)?;
            command_entries.push((command_path.clone(), command));
        }
    }
    error_sets.reject_unreferenced()?;

    let mut seen_ids = BTreeSet::new();
    let mut seen_cli_paths = BTreeMap::new();
    let mut seen_mcp_names = BTreeMap::new();
    let mut resolved = Vec::new();

    for (command_path, command) in command_entries {
        if !seen_ids.insert(command.id.clone()) {
            return Err(invalid(format!("duplicate command id `{}`", command.id)));
        }
        let entry = resolve_command(
            repo_root,
            &idl_root,
            &manifest,
            &defaults.fields,
            &family_layers,
            &kind_layers,
            &overlay_errors.errors,
            &registered_errors,
            &command_refs,
            &output_refs,
            &response_models,
            &command_path,
            &command,
        )?;

        let cli_key = entry.cli.path.join(" ");
        if let Some(existing) = seen_cli_paths.insert(cli_key.clone(), entry.id.clone()) {
            return Err(invalid(format!(
                "duplicate cli path `{cli_key}` for `{existing}` and `{}`",
                entry.id
            )));
        }
        if let Some(existing) = seen_mcp_names.insert(entry.mcp.name.clone(), entry.id.clone()) {
            return Err(invalid(format!(
                "duplicate mcp name `{}` for `{existing}` and `{}`",
                entry.mcp.name, entry.id
            )));
        }
        resolved.push(entry);
    }

    resolved.sort_by(|left, right| left.id.cmp(&right.id));
    enforce_command_exhaustiveness(&idl_root, &command_refs, &resolved)?;
    enforce_replay_skip_ratchet(&idl_root, &resolved)?;
    Ok(CommandIndex {
        generated: true,
        schema_version: manifest.schema_version,
        generator_version: manifest.generator_version,
        commands: resolved,
    })
}

/// Serializes a resolved index with stable formatting.
pub fn to_generated_json(index: &CommandIndex) -> Result<String> {
    let mut json = serde_json::to_string_pretty(index).map_err(|source| IdlError::Json {
        path: PathBuf::from("command-index.json"),
        source,
    })?;
    json.push('\n');
    Ok(json)
}

/// Generates `crates/executor/idl/v1/generated/command-index.json` and the
/// per-command schema documents under `generated/schemas/`.
pub fn generate(repo_root: &Path) -> Result<()> {
    let index = resolve_index(repo_root)?;
    let documents = schemas::schema_documents(&index)?;
    schemas::validate_fixtures(repo_root, &index, &documents)?;

    let json = to_generated_json(&index)?;
    let path = command_index_path(repo_root);
    fs::write(&path, json).map_err(|source| IdlError::Write { path, source })?;

    let schemas_dir = schemas_dir_path(repo_root);
    fs::create_dir_all(&schemas_dir).map_err(|source| IdlError::Write {
        path: schemas_dir.clone(),
        source,
    })?;
    let mut expected_files = BTreeSet::new();
    for (id, document) in &documents {
        let file = format!("{id}.json");
        let path = schemas_dir.join(&file);
        let json = schemas::to_schema_json(document)?;
        fs::write(&path, json).map_err(|source| IdlError::Write { path, source })?;
        expected_files.insert(file);
    }
    // Drop schema files for commands that no longer resolve, so the
    // generated tree never carries stale documents.
    for entry in fs::read_dir(&schemas_dir).map_err(|source| IdlError::Read {
        path: schemas_dir.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| IdlError::Read {
            path: schemas_dir.clone(),
            source,
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !expected_files.contains(&name) {
            fs::remove_file(entry.path()).map_err(|source| IdlError::Write {
                path: entry.path(),
                source,
            })?;
        }
    }
    Ok(())
}

/// Checks whether the generated command index and schema documents are fresh.
pub fn check(repo_root: &Path) -> Result<()> {
    let index = resolve_index(repo_root)?;
    let expected = to_generated_json(&index)?;
    let path = command_index_path(repo_root);
    let actual = fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    if actual != expected {
        return Err(IdlError::Stale { path });
    }

    let documents = schemas::schema_documents(&index)?;
    schemas::validate_fixtures(repo_root, &index, &documents)?;
    let schemas_dir = schemas_dir_path(repo_root);
    let mut expected_files = BTreeSet::new();
    for (id, document) in &documents {
        let file = format!("{id}.json");
        let path = schemas_dir.join(&file);
        let expected = schemas::to_schema_json(document)?;
        let actual = fs::read_to_string(&path).map_err(|source| IdlError::Read {
            path: path.clone(),
            source,
        })?;
        if actual != expected {
            return Err(IdlError::Stale { path });
        }
        expected_files.insert(file);
    }
    for entry in fs::read_dir(&schemas_dir).map_err(|source| IdlError::Read {
        path: schemas_dir.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| IdlError::Read {
            path: schemas_dir.clone(),
            source,
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !expected_files.contains(&name) {
            return Err(invalid(format!(
                "unexpected file in generated schemas dir: {name}"
            )));
        }
    }
    Ok(())
}

/// Resolves generated CLI command metadata from the checked-in command index.
pub fn resolve_default_cli_index() -> Result<CliCommandIndex> {
    resolve_cli_index(&default_repo_root())
}

/// Resolves generated CLI command metadata under `repo_root`.
pub fn resolve_cli_index(repo_root: &Path) -> Result<CliCommandIndex> {
    let path = command_index_path(repo_root);
    let text = fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    let source_checksum = checksum_sha256(text.as_bytes());
    let command_index: CommandIndex =
        serde_json::from_str(&text).map_err(|source| IdlError::Json {
            path: path.clone(),
            source,
        })?;
    cli_index_from_command_index(repo_root, &path, source_checksum, command_index)
}

/// Serializes generated CLI command metadata with stable formatting.
pub fn to_generated_cli_json(index: &CliCommandIndex) -> Result<String> {
    let mut json = serde_json::to_string_pretty(index).map_err(|source| IdlError::Json {
        path: PathBuf::from(CLI_COMMAND_INDEX_FILE),
        source,
    })?;
    json.push('\n');
    Ok(json)
}

/// Generates `crates/executor/idl/v1/generated/cli-command-index.json`.
pub fn generate_cli(repo_root: &Path) -> Result<()> {
    let index = resolve_cli_index(repo_root)?;
    let json = to_generated_cli_json(&index)?;
    let path = cli_command_index_path(repo_root);
    fs::write(&path, json).map_err(|source| IdlError::Write { path, source })
}

/// Verifies every fixture pair against a scratch executor run; with
/// `update`, blesses mismatching response fixtures instead of failing.
pub fn verify_fixtures(repo_root: &Path, update: bool) -> Result<Vec<PathBuf>> {
    let index = resolve_index(repo_root)?;
    verify::verify_fixtures(repo_root, &index, update)
}

/// Generates the reference documentation tree under `generated/docs/`
/// (per-command pages, per-family indexes, and `llms.txt`) from the IDL.
/// Replays every hermetic example and captures each step's rendered CLI input
/// plus the wire output it produced, so a consumer (strata-cli) can render the
/// output and ship `command-examples.json` in the docs bundle (#3059).
pub fn capture_examples(repo_root: &Path) -> Result<Vec<examples::CapturedExample>> {
    let index = resolve_index(repo_root)?;
    let documents = schemas::schema_documents(&index)?;
    let arg_spec = examples::load_arg_spec(repo_root)?;
    examples::capture_example_runs(repo_root, &index, &documents, &arg_spec)
}

pub fn generate_docs(repo_root: &Path) -> Result<()> {
    docs::generate_docs(repo_root)
}

/// Checks whether the generated reference documentation tree is fresh.
pub fn check_docs(repo_root: &Path) -> Result<()> {
    docs::check_docs(repo_root)
}

/// Validates and replays every canonical example (`idl/v1/examples/`) against
/// a scratch cache executor, enforcing the example-coverage allowlist.
pub fn verify_examples(repo_root: &Path) -> Result<()> {
    let index = resolve_index(repo_root)?;
    let schemas = schemas::schema_documents(&index)?;
    examples::verify_examples(repo_root, &index, &schemas)
}

/// Checks whether the generated CLI command metadata is fresh.
pub fn check_cli(repo_root: &Path) -> Result<()> {
    let index = resolve_cli_index(repo_root)?;
    let expected = to_generated_cli_json(&index)?;
    let path = cli_command_index_path(repo_root);
    let actual = fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    if actual == expected {
        Ok(())
    } else {
        Err(IdlError::CliStale { path })
    }
}

fn cli_index_from_command_index(
    repo_root: &Path,
    source_path: &Path,
    source_checksum: String,
    command_index: CommandIndex,
) -> Result<CliCommandIndex> {
    validate_cli_source_index(&command_index)?;
    let source = CliIndexSourceInfo {
        path: relative_to(source_path, repo_root),
        checksum_sha256: source_checksum,
        schema_version: command_index.schema_version.clone(),
        generator_version: command_index.generator_version.clone(),
    };

    let mut seen_ids = BTreeSet::new();
    let mut seen_paths = BTreeMap::new();
    let mut commands = Vec::with_capacity(command_index.commands.len());
    for command in command_index.commands {
        let entry = cli_entry_from_resolved(command)?;
        if !seen_ids.insert(entry.id.clone()) {
            return Err(invalid(format!(
                "duplicate command id `{}` in command index",
                entry.id
            )));
        }
        let path_key = cli_path_key(&entry.path);
        if let Some(existing) = seen_paths.insert(path_key.clone(), entry.id.clone()) {
            return Err(invalid(format!(
                "duplicate cli path `{path_key}` for `{existing}` and `{}`",
                entry.id
            )));
        }
        commands.push(entry);
    }

    commands.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut by_id = BTreeMap::new();
    let mut by_path = BTreeMap::new();
    let mut family_commands: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (index, command) in commands.iter().enumerate() {
        by_id.insert(command.id.clone(), index);
        by_path.insert(command.path_display.clone(), command.id.clone());
        family_commands
            .entry(command.family.clone())
            .or_default()
            .push(command.id.clone());
    }

    let families = family_commands
        .into_iter()
        .map(|(id, commands)| CliFamilyGroup {
            command_count: commands.len(),
            id,
            commands,
        })
        .collect::<Vec<_>>();

    Ok(CliCommandIndex {
        generated: true,
        schema_version: CLI_SCHEMA_VERSION.to_owned(),
        generator_version: CLI_GENERATOR_VERSION.to_owned(),
        source,
        command_count: commands.len(),
        families,
        commands,
        lookup: CliLookupTables { by_id, by_path },
    })
}

fn validate_cli_source_index(index: &CommandIndex) -> Result<()> {
    if !index.generated {
        return Err(invalid("command index must be marked generated"));
    }
    if index.schema_version != SUPPORTED_COMMAND_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported command index schema `{}`",
            index.schema_version
        )));
    }
    if index.generator_version != SUPPORTED_COMMAND_GENERATOR_VERSION {
        return Err(invalid(format!(
            "unsupported command index generator `{}`",
            index.generator_version
        )));
    }
    if index.commands.is_empty() {
        return Err(invalid("command index must contain at least one command"));
    }
    Ok(())
}

fn cli_entry_from_resolved(command: ResolvedCommand) -> Result<CliCommandEntry> {
    let wire = variant_wire_tag(&command.input, "Command")?;
    // A `wire` command omits its `cli.path` on the shipped command-index.json
    // (#3058), but the CLI-routing catalog still needs the logical path.
    // Reconstruct it from the command id — the same rule the generator used to
    // synthesize it — so this stays derivable from the generated index alone.
    let cli_path = if command.cli.path.is_empty() {
        default_cli_path_for_command_id(&command.id)
    } else {
        command.cli.path
    };
    let entry = CliCommandEntry {
        id: command.id,
        path_display: cli_path_key(&cli_path),
        surface: command.cli.surface,
        path: cli_path,
        family: command.family,
        op: command.op,
        kind: command.kind,
        title: command.title,
        summary: command.summary,
        description: command.description,
        docs: command.docs,
        feature: command.feature,
        access: command.access,
        commit: command.commit,
        pagination: command.pagination,
        batch: command.batch,
        input: command.input,
        wire,
        outputs: command.outputs,
        response_model: command.response_model,
        errors: command.errors,
        fixtures: command.fixtures,
        wire_status: command.wire_status,
    };
    validate_cli_entry(&entry)?;
    Ok(entry)
}

fn validate_cli_entry(entry: &CliCommandEntry) -> Result<()> {
    validate_command_id(&entry.id)?;
    validate_cli_path(&entry.path, &entry.id)?;
    let expected_path_display = cli_path_key(&entry.path);
    if entry.path_display != expected_path_display {
        return Err(invalid(format!(
            "command `{}` has mismatched path_display `{}`",
            entry.id, entry.path_display
        )));
    }
    for (field, value) in [
        ("family", entry.family.as_str()),
        ("op", entry.op.as_str()),
        ("kind", entry.kind.as_str()),
        ("title", entry.title.as_str()),
        ("summary", entry.summary.as_str()),
        ("description", entry.description.as_str()),
        ("docs", entry.docs.as_str()),
        ("feature", entry.feature.as_str()),
        ("access", entry.access.as_str()),
        ("commit", entry.commit.as_str()),
        ("pagination", entry.pagination.as_str()),
        ("batch", entry.batch.as_str()),
        ("input", entry.input.as_str()),
        ("wire", entry.wire.as_str()),
        ("response_model", entry.response_model.as_str()),
        ("wire_status", entry.wire_status.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(invalid(format!(
                "command `{}` has empty `{field}`",
                entry.id
            )));
        }
        if contains_placeholder(value) {
            return Err(invalid(format!(
                "command `{}` has unresolved placeholder in `{field}`",
                entry.id
            )));
        }
    }
    validate_docs_path(&entry.docs, &entry.id)?;
    validate_executor_ref_prefix(&entry.input, "Command")?;
    if entry.outputs.is_empty() {
        return Err(invalid(format!(
            "command `{}` has no output references",
            entry.id
        )));
    }
    for output in &entry.outputs {
        validate_executor_ref_prefix(output, "Output")?;
    }
    if entry.errors.is_empty() {
        return Err(invalid(format!(
            "command `{}` has no public error references",
            entry.id
        )));
    }
    for error in &entry.errors {
        if error.code.trim().is_empty() || error.docs.trim().is_empty() {
            return Err(invalid(format!(
                "command `{}` has incomplete public error reference",
                entry.id
            )));
        }
    }
    validate_wire_status(&entry.id, &entry.wire_status)?;
    Ok(())
}

fn validate_executor_ref_prefix(reference: &str, prefix: &str) -> Result<()> {
    variant_name(reference, prefix).map(|_| ())
}

fn cli_path_key(path: &[String]) -> String {
    path.join(" ")
}

fn default_cli_path_for_command_id(command_id: &str) -> Vec<String> {
    command_id
        .split('.')
        .map(|segment| segment.replace('_', "-"))
        .collect()
}

fn command_index_path(repo_root: &Path) -> PathBuf {
    repo_root
        .join(IDL_DIR)
        .join("generated")
        .join(COMMAND_INDEX_FILE)
}

fn schemas_dir_path(repo_root: &Path) -> PathBuf {
    repo_root.join(IDL_DIR).join("generated").join("schemas")
}

fn cli_command_index_path(repo_root: &Path) -> PathBuf {
    repo_root
        .join(IDL_DIR)
        .join("generated")
        .join(CLI_COMMAND_INDEX_FILE)
}

fn checksum_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn resolve_command(
    repo_root: &Path,
    idl_root: &Path,
    manifest: &ManifestSource,
    defaults: &LayerFields,
    family_layers: &BTreeMap<String, LayerFields>,
    kind_layers: &BTreeMap<String, LayerFields>,
    overlay_errors: &[String],
    registered_errors: &BTreeMap<String, String>,
    command_refs: &BTreeSet<String>,
    output_refs: &BTreeSet<String>,
    response_models: &BTreeSet<String>,
    command_path: &Path,
    command: &CommandSource,
) -> Result<ResolvedCommand> {
    validate_command_id(&command.id)?;
    let (family, op) = split_command_id(&command.id)?;
    let family_layer = family_layers.get(family).ok_or_else(|| {
        invalid(format!(
            "command `{}` references unknown family `{family}`",
            command.id
        ))
    })?;
    let kind_layer = kind_layers.get(&command.kind).ok_or_else(|| {
        invalid(format!(
            "command `{}` references unknown kind `{}`",
            command.id, command.kind
        ))
    })?;

    validate_executor_ref(&command.input, "Command", command_refs)?;
    validate_executor_ref(&command.output, "Output", output_refs)?;
    let outputs = resolve_output_refs(command, output_refs)?;
    validate_fixture_refs(repo_root, &command.fixtures, &command.input, &outputs)?;
    let wire_status = command
        .wire_status
        .clone()
        .unwrap_or_else(|| "stable".to_owned());
    validate_wire_status(&command.id, &wire_status)?;

    let mut layer = ResolvedLayer::default();
    layer.apply(defaults);
    layer.apply(family_layer);
    layer.apply(kind_layer);
    layer.apply_command(command);

    let context = PlaceholderContext::new(family, op, &command.result);
    let docs = expand_required(
        "docs",
        &required(layer.docs, "docs", &command.id)?,
        &context,
    )?;
    validate_docs_path(&docs, &command.id)?;

    let cli_path = match layer.cli_path {
        Some(path) => expand_cli_path(path, &context, &command.id)?,
        None => default_cli_path_for_command_id(&command.id),
    };
    validate_cli_path(&cli_path, &command.id)?;

    let cli_surface = layer.cli_surface.unwrap_or_else(|| "verb".to_owned());
    if !CLI_SURFACES.contains(&cli_surface.as_str()) {
        return Err(invalid(format!(
            "`{}` has unknown cli_surface `{cli_surface}`; expected one of {CLI_SURFACES:?}",
            command.id
        )));
    }

    let mcp_name_template = layer
        .mcp_name
        .unwrap_or_else(|| "strata_{family}_{op_slug}".to_owned());
    let mcp_name = expand_required("mcp_name", &mcp_name_template, &context)?;
    validate_mcp_name(&mcp_name, &command.id)?;

    let response_model = expand_required(
        "response_model",
        &required(layer.response_model, "response_model", &command.id)?,
        &context,
    )?;
    if !response_models.contains(&response_model) {
        return Err(invalid(format!(
            "command `{}` references unknown response model `{response_model}`",
            command.id
        )));
    }

    let prose = load_prose(idl_root, &command.prose, &layer.snippets)?;
    let errors = resolve_errors(
        &command.id,
        &layer.errors,
        overlay_errors,
        registered_errors,
    )?;

    Ok(ResolvedCommand {
        id: command.id.clone(),
        family: family.to_owned(),
        op: op.to_owned(),
        kind: command.kind.clone(),
        title: command.title.clone(),
        summary: prose.summary,
        description: prose.body,
        docs,
        cli: CliInfo {
            path: cli_path,
            surface: cli_surface,
        },
        mcp: McpInfo {
            name: mcp_name,
            description: prose
                .mcp_description
                .unwrap_or_else(|| format!("Run the {} command.", command.id)),
        },
        feature: required(layer.feature, "feature", &command.id)?,
        access: required(layer.access, "access", &command.id)?,
        input: command.input.clone(),
        wire: variant_wire_tag(&command.input, "Command")?,
        output: command.output.clone(),
        outputs,
        wire_status,
        response_model,
        commit: required(layer.commit, "commit", &command.id)?,
        pagination: required(layer.pagination, "pagination", &command.id)?,
        batch: required(layer.batch, "batch", &command.id)?,
        errors,
        fixtures: command.fixtures.clone(),
        source: SourceInfo {
            command: relative_to(command_path, repo_root),
            prose: command.prose.clone(),
        },
    })
    .and_then(|entry| {
        validate_generated_entry(&entry, manifest)?;
        Ok(entry)
    })
}

fn validate_generated_entry(entry: &ResolvedCommand, _manifest: &ManifestSource) -> Result<()> {
    for (field, value) in [
        ("title", entry.title.as_str()),
        ("summary", entry.summary.as_str()),
        ("description", entry.description.as_str()),
        ("feature", entry.feature.as_str()),
        ("access", entry.access.as_str()),
        ("wire_status", entry.wire_status.as_str()),
        ("commit", entry.commit.as_str()),
        ("pagination", entry.pagination.as_str()),
        ("batch", entry.batch.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(invalid(format!(
                "command `{}` has empty `{field}`",
                entry.id
            )));
        }
        if contains_placeholder(value) {
            return Err(invalid(format!(
                "command `{}` has unresolved placeholder in `{field}`",
                entry.id
            )));
        }
    }
    Ok(())
}

fn read_yaml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let text = fs::read_to_string(path).map_err(|source| IdlError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    serde_yaml::from_str(&text).map_err(|source| IdlError::Yaml {
        path: path.to_path_buf(),
        source,
    })
}

fn named_layers(
    sources: Vec<NamedLayerSource>,
    noun: &str,
) -> Result<BTreeMap<String, LayerFields>> {
    let mut layers = BTreeMap::new();
    for source in sources {
        if layers.insert(source.id.clone(), source.fields).is_some() {
            return Err(invalid(format!("duplicate {noun} id `{}`", source.id)));
        }
    }
    Ok(layers)
}

fn validate_command_source_text(path: &Path) -> Result<()> {
    let text = fs::read_to_string(path).map_err(|source| IdlError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    for forbidden in [
        "fields:",
        "schema:",
        "properties:",
        "request_schema:",
        "response_schema:",
    ] {
        if text.contains(forbidden) {
            return Err(invalid(format!(
                "{} defines DTO fields via `{forbidden}`; command YAML may only reference executor DTOs",
                path.display()
            )));
        }
    }
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('&') || trimmed.contains(": &") || trimmed.contains("<<:") {
            return Err(invalid(format!(
                "{} uses YAML anchors or merge keys; use explicit IDL layers instead",
                path.display()
            )));
        }
    }
    for field in extract_command_fields(&text) {
        if !COMMAND_SOURCE_FIELDS.contains(&field.as_str()) {
            return Err(invalid(format!(
                "{} contains unknown command field `{field}`",
                path.display()
            )));
        }
    }
    Ok(())
}

fn extract_command_fields(text: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut in_command = false;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("- id:") {
            in_command = true;
        }
        if !in_command {
            continue;
        }
        let indent = line.len() - trimmed.len();
        if indent == 4 {
            if let Some((field, _)) = trimmed.split_once(':') {
                let normalized = field.trim_start_matches("- ").trim();
                fields.push(normalized.to_owned());
            }
        }
    }
    fields
}

/// Exhaustiveness guard: every `Command` variant is either covered by a
/// resolved IDL entry or listed in `uncovered-commands.yaml`. The list may
/// only shrink — a listed variant that gains coverage must be removed, and
/// a brand-new variant fails resolution until it is covered or listed.
fn enforce_command_exhaustiveness(
    idl_root: &Path,
    command_refs: &BTreeSet<String>,
    resolved: &[ResolvedCommand],
) -> Result<()> {
    let allowlist: UncoveredCommandsSource = read_yaml(&idl_root.join(UNCOVERED_COMMANDS_FILE))?;
    let mut covered = BTreeSet::new();
    for entry in resolved {
        covered.insert(variant_name(&entry.input, "Command")?.to_owned());
    }
    enforce_exhaustiveness_lists(command_refs, &covered, &allowlist.uncovered)
}

/// Every command whose primary fixture sets `replay_skip` (so the behavior
/// guard never executes it) must be listed in `replay-skipped-commands.yaml`,
/// and the list may only shrink (TCP3.8c). A newly added skip fails the build
/// unless it is justified and listed; a listed command that becomes replayable
/// must be removed. This keeps golden-or-replay coverage from silently eroding.
fn enforce_replay_skip_ratchet(idl_root: &Path, resolved: &[ResolvedCommand]) -> Result<()> {
    let allowlist: ReplaySkippedCommandsSource =
        read_yaml(&idl_root.join(REPLAY_SKIPPED_COMMANDS_FILE))?;
    enforce_debt_budget(
        REPLAY_SKIPPED_COMMANDS_FILE,
        allowlist.skipped.len(),
        allowlist.budget,
    )?;
    let ids: BTreeSet<&str> = resolved.iter().map(|entry| entry.id.as_str()).collect();
    let skipped: BTreeSet<&str> = resolved
        .iter()
        .filter(|entry| entry.fixtures.replay_skip.is_some())
        .map(|entry| entry.id.as_str())
        .collect();
    enforce_replay_skip_lists(&ids, &skipped, &allowlist.skipped)
}

fn enforce_replay_skip_lists(
    ids: &BTreeSet<&str>,
    skipped: &BTreeSet<&str>,
    allowlist: &[String],
) -> Result<()> {
    let mut listed = BTreeSet::new();
    for id in allowlist {
        if !ids.contains(id.as_str()) {
            return Err(invalid(format!(
                "replay-skipped-commands.yaml lists `{id}` which is not a command id; remove it"
            )));
        }
        if !skipped.contains(id.as_str()) {
            return Err(invalid(format!(
                "`{id}` no longer sets replay_skip; remove it from replay-skipped-commands.yaml (the allowlist may only shrink)"
            )));
        }
        if !listed.insert(id.as_str()) {
            return Err(invalid(format!(
                "duplicate `{id}` in replay-skipped-commands.yaml"
            )));
        }
    }

    for id in skipped {
        if !listed.contains(id) {
            return Err(invalid(format!(
                "command `{id}` sets replay_skip but is not listed in replay-skipped-commands.yaml; make it replayable or justify and list the skip (the allowlist may only shrink)"
            )));
        }
    }
    Ok(())
}

/// W0b debt-count budget: a debt allowlist's length MUST EQUAL its committed
/// `budget`, so every change to the list is a deliberate, reviewed number.
///
/// The per-entry guards above are shrink-only (they reject stale or unlisted
/// entries) but do NOT cap the *total* — so the count could still creep up as
/// long as each new entry was properly listed (the audit found the unreplayed
/// list had grown 105→110 and the skip list 7→12 exactly this way). This budget
/// closes that: growth past it fails unless `budget` is raised in the same change
/// (a visible act carrying its own rationale — the "debt-budget ledger update"),
/// and a budget left above a drained list fails too, forcing the ratchet DOWN so
/// a reduction is locked in rather than silently leaving slack for later regrowth.
fn enforce_debt_budget(file: &str, count: usize, budget: usize) -> Result<()> {
    if count > budget {
        return Err(invalid(format!(
            "{file}: debt grew past its budget ({count} entries > budget {budget}). Draining is \
             the goal; if this addition is genuinely necessary, raise `budget` to {count} in the \
             same change with a rationale (owner + issue/slice + planned harness) — a reviewed, \
             deliberate act, not silent drift"
        )));
    }
    if count < budget {
        return Err(invalid(format!(
            "{file}: debt is below its budget ({count} entries < budget {budget}). The budget \
             ratchets DOWN — lower `budget` to {count} in the same change to lock in the reduction \
             (the debt-budget ledger must track the real count)"
        )));
    }
    Ok(())
}

fn enforce_exhaustiveness_lists(
    command_refs: &BTreeSet<String>,
    covered: &BTreeSet<String>,
    allowlist: &[String],
) -> Result<()> {
    let mut listed = BTreeSet::new();
    for reference in allowlist {
        let name = variant_name(reference, "Command")?.to_owned();
        if !command_refs.contains(&name) {
            return Err(invalid(format!(
                "uncovered-commands.yaml lists `{reference}` which is not a Command variant"
            )));
        }
        if covered.contains(&name) {
            return Err(invalid(format!(
                "`{reference}` is covered by the IDL; remove it from uncovered-commands.yaml (the allowlist may only shrink)"
            )));
        }
        if !listed.insert(name) {
            return Err(invalid(format!(
                "duplicate `{reference}` in uncovered-commands.yaml"
            )));
        }
    }

    for variant in command_refs {
        if !covered.contains(variant) && !listed.contains(variant) {
            return Err(invalid(format!(
                "Command::{variant} has no resolved IDL entry and is not listed in uncovered-commands.yaml; cover it or list it explicitly"
            )));
        }
    }
    Ok(())
}

fn registered_error_map() -> BTreeMap<String, String> {
    public_error_code_entries()
        .map(|entry| {
            (
                entry.code.to_owned(),
                format!("https://stratadb.org/e/{}", entry.docs_slug),
            )
        })
        .collect()
}

fn validate_error_overlay(
    overlay_errors: &[String],
    registered_errors: &BTreeMap<String, String>,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for code in overlay_errors {
        if !seen.insert(code) {
            return Err(invalid(format!(
                "duplicate error code `{code}` in errors.yaml"
            )));
        }
        if !registered_errors.contains_key(code) {
            return Err(invalid(format!(
                "unregistered error code `{code}` in errors.yaml"
            )));
        }
    }
    Ok(())
}

/// Every registered public error code must be declared in `errors.yaml` — so
/// SDK-facing docs surface it — or listed in `uncovered-error-codes.yaml`. The
/// allowlist may only shrink: a listed code that gains overlay coverage must be
/// removed, a listed code that is not registered is rejected, and a newly
/// registered code that is neither declared nor listed fails the build.
fn enforce_error_code_exhaustiveness(
    idl_root: &Path,
    overlay_errors: &[String],
    registered_errors: &BTreeMap<String, String>,
) -> Result<()> {
    let allowlist: UncoveredErrorCodesSource =
        read_yaml(&idl_root.join(UNCOVERED_ERROR_CODES_FILE))?;
    let registered: BTreeSet<&str> = registered_errors.keys().map(String::as_str).collect();
    enforce_error_code_lists(&registered, overlay_errors, &allowlist.uncovered)
}

fn enforce_error_code_lists(
    registered: &BTreeSet<&str>,
    overlay_errors: &[String],
    allowlist: &[String],
) -> Result<()> {
    let declared: BTreeSet<&str> = overlay_errors.iter().map(String::as_str).collect();
    let mut listed = BTreeSet::new();
    for code in allowlist {
        if !registered.contains(code.as_str()) {
            return Err(invalid(format!(
                "uncovered-error-codes.yaml lists `{code}` which is not a registered error code"
            )));
        }
        if declared.contains(code.as_str()) {
            return Err(invalid(format!(
                "`{code}` is declared in errors.yaml; remove it from uncovered-error-codes.yaml (the allowlist may only shrink)"
            )));
        }
        if !listed.insert(code.as_str()) {
            return Err(invalid(format!(
                "duplicate `{code}` in uncovered-error-codes.yaml"
            )));
        }
    }

    for code in registered {
        if !declared.contains(code) && !listed.contains(code) {
            return Err(invalid(format!(
                "registered error `{code}` is not declared in errors.yaml and not listed in uncovered-error-codes.yaml; declare it or list it explicitly"
            )));
        }
    }
    Ok(())
}

/// The `set:<id>` reference prefix inside an authored error list.
const ERROR_SET_REF: &str = "set:";

/// The named error sets of `error-sets.yaml`, expanded (#3250).
///
/// Every authored error list — a layer's `errors`, a command's `errors+` /
/// `errors-`, or a set declared below — may hold `set:<id>` entries, which
/// [`ErrorSets::expand_in_place`] replaces with the set's codes. Three rules
/// keep the file honest: a set nobody references is rejected (dead authored
/// data), a set may only reference sets declared above it (acyclic by
/// construction, and the reader can expand it top-down), and a list that
/// spells out every code of a set is a hand copy and must reference the set
/// instead — the exact defect the sets exist to end.
struct ErrorSets {
    /// Expanded, deduplicated codes per set, in declaration order.
    sets: Vec<(String, Vec<String>)>,
    /// Sets any list has referenced so far.
    referenced: BTreeSet<String>,
}

impl ErrorSets {
    fn resolve(sources: Vec<ErrorSetSource>, overlay_errors: &[String]) -> Result<Self> {
        let declared: BTreeSet<&str> = overlay_errors.iter().map(String::as_str).collect();
        let mut resolved = Self {
            sets: Vec::new(),
            referenced: BTreeSet::new(),
        };
        for source in sources {
            validate_error_set_id(&source.id)?;
            if resolved.lookup(&source.id).is_some() {
                return Err(invalid(format!("duplicate error set id `{}`", source.id)));
            }
            let mut seen = BTreeSet::new();
            for entry in &source.errors {
                if entry.starts_with(ERROR_SET_REF) {
                    continue;
                }
                if !declared.contains(entry.as_str()) {
                    return Err(invalid(format!(
                        "error set `{}` lists `{entry}` which is not declared in errors.yaml",
                        source.id
                    )));
                }
                if !seen.insert(entry.as_str()) {
                    return Err(invalid(format!(
                        "error set `{}` lists `{entry}` twice",
                        source.id
                    )));
                }
            }
            let site = format!("error set `{}`", source.id);
            let mut codes = source.errors;
            resolved.expand_in_place(&site, &mut codes)?;
            let mut expanded = Vec::new();
            append_unique(&mut expanded, &codes);
            if expanded.len() < 2 {
                return Err(invalid(format!(
                    "error set `{}` expands to fewer than two codes; a set names a group, list a single code directly",
                    source.id
                )));
            }
            resolved.sets.push((source.id, expanded));
        }
        Ok(resolved)
    }

    fn lookup(&self, id: &str) -> Option<&[String]> {
        self.sets
            .iter()
            .find(|(set_id, _)| set_id == id)
            .map(|(_, codes)| codes.as_slice())
    }

    /// Replaces every `set:<id>` entry of `list` with that set's codes, in
    /// place. `site` names the list in rejections, e.g. "command `kv.put`".
    fn expand_in_place(&mut self, site: &str, list: &mut Vec<String>) -> Result<()> {
        // `set:` entries never equal a code, so they cannot complete a copy.
        let literal: BTreeSet<&str> = list.iter().map(String::as_str).collect();
        for (id, codes) in &self.sets {
            if codes.iter().all(|code| literal.contains(code.as_str())) {
                return Err(invalid(format!(
                    "{site} lists every code of error set `{id}`; reference `{ERROR_SET_REF}{id}` instead"
                )));
            }
        }
        let mut expanded = Vec::with_capacity(list.len());
        for entry in list.drain(..) {
            match entry.strip_prefix(ERROR_SET_REF) {
                Some(id) => {
                    let codes = self.lookup(id).ok_or_else(|| {
                        invalid(format!(
                            "{site} references error set `{id}` which is not declared above it in error-sets.yaml"
                        ))
                    })?;
                    expanded.extend(codes.iter().cloned());
                    self.referenced.insert(id.to_owned());
                }
                None => expanded.push(entry),
            }
        }
        *list = expanded;
        Ok(())
    }

    /// Every declared set must be referenced somewhere, or it is dead
    /// authored data that will drift from the runtime unnoticed.
    fn reject_unreferenced(&self) -> Result<()> {
        for (id, _) in &self.sets {
            if !self.referenced.contains(id) {
                return Err(invalid(format!(
                    "error set `{id}` is never referenced; reference it as `{ERROR_SET_REF}{id}` or delete it"
                )));
            }
        }
        Ok(())
    }
}

/// Set ids read like command ids: dotted lowercase segments. An empty id or
/// an empty segment fails the first-character rule, so nothing else is needed.
fn validate_error_set_id(id: &str) -> Result<()> {
    let well_formed = id.split('.').all(|segment| {
        segment
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_lowercase())
            && segment
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    });
    if well_formed {
        Ok(())
    } else {
        Err(invalid(format!(
            "error set id `{id}` must be dotted lowercase segments (`family.name`)"
        )))
    }
}

/// Enforce error-envelope replay coverage (TCP3.8b). Given the set of codes an
/// error-case replay actually produced (`replayed`) and the per-command
/// replays (`command_replays`, pairs of command id and produced code), require:
///
/// * (B) consistency — every code a command replays is declared in that
///   command's `errors[]` (a proven-reachable code must appear in the
///   SDK-facing error list; fix by adding it via `errors+`);
/// * (A) coverage — every code any command declares in `errors[]` is either
///   replayed by an error case or listed in `unreplayed-error-codes.yaml`, and
///   the allowlist may only shrink (a now-replayed or no-longer-declared entry
///   must be removed).
pub(super) fn enforce_error_replay_coverage(
    repo_root: &Path,
    index: &CommandIndex,
    replayed: &BTreeSet<String>,
    command_replays: &[(String, String)],
) -> Result<()> {
    let idl_root = repo_root.join(IDL_DIR);
    let allowlist: UnreplayedErrorCodesSource =
        read_yaml(&idl_root.join(UNREPLAYED_ERROR_CODES_FILE))?;
    enforce_debt_budget(
        UNREPLAYED_ERROR_CODES_FILE,
        allowlist.unreplayed.len(),
        allowlist.budget,
    )?;

    let mut declared: BTreeSet<&str> = BTreeSet::new();
    let mut declared_by_command: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for command in &index.commands {
        let entry = declared_by_command.entry(command.id.as_str()).or_default();
        for error in &command.errors {
            declared.insert(error.code.as_str());
            entry.insert(error.code.as_str());
        }
    }

    let replayed: BTreeSet<&str> = replayed.iter().map(String::as_str).collect();
    enforce_replay_declaration(&declared_by_command, command_replays)?;
    enforce_replay_coverage_lists(&declared, &replayed, &allowlist.unreplayed)
}

/// (B) A replayed code proves its command can surface it, so the command's
/// declared `errors[]` (which feeds SDK docs) must name it.
fn enforce_replay_declaration(
    declared_by_command: &BTreeMap<&str, BTreeSet<&str>>,
    command_replays: &[(String, String)],
) -> Result<()> {
    for (command_id, code) in command_replays {
        let declared = declared_by_command
            .get(command_id.as_str())
            .ok_or_else(|| invalid(format!("replayed unknown command `{command_id}`")))?;
        if !declared.contains(code.as_str()) {
            return Err(invalid(format!(
                "command `{command_id}` replays error `{code}` but does not declare it in errors[]; add it via `errors+` so the SDK-facing error list names it"
            )));
        }
    }
    Ok(())
}

/// (A) Every code any command declares must be replayed by an error case or
/// listed in `unreplayed-error-codes.yaml`, and the allowlist may only shrink
/// (a now-replayed or no-longer-declared entry must be removed).
fn enforce_replay_coverage_lists(
    declared: &BTreeSet<&str>,
    replayed: &BTreeSet<&str>,
    allowlist: &[String],
) -> Result<()> {
    let mut listed = BTreeSet::new();
    for code in allowlist {
        if !declared.contains(code.as_str()) {
            return Err(invalid(format!(
                "unreplayed-error-codes.yaml lists `{code}` which no command declares in errors[]; remove it"
            )));
        }
        if replayed.contains(code.as_str()) {
            return Err(invalid(format!(
                "`{code}` now has an error-case replay fixture; remove it from unreplayed-error-codes.yaml (the allowlist may only shrink)"
            )));
        }
        if !listed.insert(code.as_str()) {
            return Err(invalid(format!(
                "duplicate `{code}` in unreplayed-error-codes.yaml"
            )));
        }
    }

    for code in declared {
        if !replayed.contains(code) && !listed.contains(code) {
            return Err(invalid(format!(
                "declared error `{code}` is surfaced by a command but has no error-case replay fixture; add an `error_cases` entry that pins its envelope, or list it in unreplayed-error-codes.yaml"
            )));
        }
    }
    Ok(())
}

fn resolve_errors(
    command_id: &str,
    errors: &[String],
    overlay_errors: &[String],
    registered_errors: &BTreeMap<String, String>,
) -> Result<Vec<ErrorRef>> {
    let allowed: BTreeSet<&str> = overlay_errors.iter().map(String::as_str).collect();
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::new();
    for code in errors {
        if !allowed.contains(code.as_str()) {
            return Err(invalid(format!(
                "command `{command_id}` references error `{code}` not listed in errors.yaml"
            )));
        }
        if seen.insert(code) {
            let docs = registered_errors.get(code).ok_or_else(|| {
                invalid(format!(
                    "command `{command_id}` references unregistered error `{code}`"
                ))
            })?;
            resolved.push(ErrorRef {
                code: code.clone(),
                docs: docs.clone(),
            });
        }
    }
    Ok(resolved)
}

fn enum_variants(path: &Path) -> Result<BTreeSet<String>> {
    let text = fs::read_to_string(path).map_err(|source| IdlError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let mut variants = BTreeSet::new();
    // Only lines inside the enum body count: the leading `use` block also
    // indents type names by four spaces and must not read as variants.
    let mut in_enum = false;
    for line in text.lines() {
        if line.starts_with("pub enum ") {
            in_enum = true;
            continue;
        }
        if in_enum && line == "}" {
            in_enum = false;
            continue;
        }
        if !in_enum {
            continue;
        }
        if !line.starts_with("    ") || line.starts_with("        ") {
            continue;
        }
        let trimmed = line.trim_start();
        let Some(first) = trimmed.chars().next() else {
            continue;
        };
        if !first.is_ascii_uppercase() {
            continue;
        }
        let name = trimmed
            .split([' ', '{', '(', ','])
            .next()
            .unwrap_or_default();
        if !name.is_empty() {
            variants.insert(name.to_owned());
        }
    }
    Ok(variants)
}

fn validate_executor_ref(reference: &str, prefix: &str, variants: &BTreeSet<String>) -> Result<()> {
    let name = variant_name(reference, prefix)?;
    if variants.contains(name) {
        Ok(())
    } else {
        Err(invalid(format!("unknown DTO reference `{reference}`")))
    }
}

fn variant_name<'a>(reference: &'a str, prefix: &str) -> Result<&'a str> {
    let expected_prefix = format!("{prefix}::");
    let Some(name) = reference.strip_prefix(&expected_prefix) else {
        return Err(invalid(format!(
            "DTO reference `{reference}` must start with `{expected_prefix}`"
        )));
    };
    Ok(name)
}

fn resolve_output_refs(
    command: &CommandSource,
    output_refs: &BTreeSet<String>,
) -> Result<Vec<String>> {
    let outputs = if command.outputs.is_empty() {
        vec![command.output.clone()]
    } else {
        command.outputs.clone()
    };
    if !outputs.iter().any(|output| output == &command.output) {
        return Err(invalid(format!(
            "command `{}` declares primary output `{}` but does not include it in `outputs`",
            command.id, command.output
        )));
    }
    let mut seen = BTreeSet::new();
    for output in &outputs {
        validate_executor_ref(output, "Output", output_refs)?;
        if !seen.insert(output) {
            return Err(invalid(format!(
                "command `{}` declares duplicate output `{output}`",
                command.id
            )));
        }
    }
    Ok(outputs)
}

fn validate_wire_status(command_id: &str, wire_status: &str) -> Result<()> {
    match wire_status {
        "stable" | "transitional" => Ok(()),
        _ => Err(invalid(format!(
            "command `{command_id}` has invalid wire_status `{wire_status}`"
        ))),
    }
}

fn validate_fixture_refs(
    repo_root: &Path,
    fixtures: &FixtureRefs,
    input: &str,
    outputs: &[String],
) -> Result<()> {
    let request_tag = variant_wire_tag(input, "Command")?;
    let output_tags = outputs
        .iter()
        .map(|output| variant_wire_tag(output, "Output"))
        .collect::<Result<Vec<_>>>()?;
    validate_fixture_ref(
        repo_root,
        &fixtures.request,
        "requests/v1",
        true,
        &[request_tag],
    )?;
    let mut response_tags = BTreeSet::new();
    response_tags.insert(validate_fixture_ref(
        repo_root,
        &fixtures.response,
        "responses/v1",
        false,
        &output_tags,
    )?);
    for response in &fixtures.responses {
        response_tags.insert(validate_fixture_ref(
            repo_root,
            response,
            "responses/v1",
            false,
            &output_tags,
        )?);
    }
    for output_tag in output_tags {
        if !response_tags.contains(&output_tag) {
            return Err(invalid(format!(
                "fixtures for `{input}` do not cover output type `{output_tag}`"
            )));
        }
    }
    Ok(())
}

fn validate_fixture_ref(
    repo_root: &Path,
    fixture: &str,
    prefix: &str,
    request: bool,
    expected_tags: &[String],
) -> Result<String> {
    let path = Path::new(fixture);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(invalid(format!(
            "fixture path `{fixture}` must stay under {FIXTURE_ROOT}"
        )));
    }
    if !fixture.starts_with(prefix) {
        return Err(invalid(format!(
            "fixture path `{fixture}` must start with `{prefix}`"
        )));
    }
    if !path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
    {
        return Err(invalid(format!(
            "fixture path `{fixture}` must end with .json"
        )));
    }
    let full_path = repo_root.join(FIXTURE_ROOT).join(fixture);
    let text = fs::read_to_string(&full_path).map_err(|source| IdlError::Read {
        path: full_path.clone(),
        source,
    })?;
    let actual_tag = fixture_type_tag(&full_path, &text)?;
    if !expected_tags.iter().any(|tag| tag == &actual_tag) {
        return Err(invalid(format!(
            "fixture `{fixture}` has type `{actual_tag}` but expected one of `{}`",
            expected_tags.join("`, `")
        )));
    }
    if request {
        let _: Command = serde_json::from_str(&text).map_err(|source| IdlError::Json {
            path: full_path.clone(),
            source,
        })?;
    } else {
        let _: Output = serde_json::from_str(&text).map_err(|source| IdlError::Json {
            path: full_path.clone(),
            source,
        })?;
    }
    Ok(actual_tag)
}

fn fixture_type_tag(path: &Path, text: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(text).map_err(|source| IdlError::Json {
        path: path.to_path_buf(),
        source,
    })?;
    value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            invalid(format!(
                "fixture `{}` is missing string field `type`",
                path.display()
            ))
        })
}

fn variant_wire_tag(reference: &str, prefix: &str) -> Result<String> {
    Ok(pascal_to_snake(variant_name(reference, prefix)?))
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

fn load_prose(idl_root: &Path, prose_path: &str, snippets: &[String]) -> Result<Prose> {
    let path = idl_root.join("prose").join(prose_path);
    let text = fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    let (frontmatter, body) = parse_frontmatter(&path, &text)?;
    if frontmatter.summary.trim().is_empty() {
        return Err(invalid(format!(
            "prose `{}` has empty summary",
            path.display()
        )));
    }
    if body.trim().is_empty() {
        return Err(invalid(format!(
            "prose `{}` has empty body",
            path.display()
        )));
    }

    let mut description = body.trim().to_owned();
    for snippet in snippets {
        let snippet_path = idl_root.join("prose/snippets").join(snippet);
        let snippet_text = fs::read_to_string(&snippet_path).map_err(|source| IdlError::Read {
            path: snippet_path.clone(),
            source,
        })?;
        if snippet_text.trim().is_empty() {
            return Err(invalid(format!(
                "snippet `{}` is empty",
                snippet_path.display()
            )));
        }
        description.push_str("\n\n");
        description.push_str(snippet_text.trim());
    }

    Ok(Prose {
        summary: frontmatter.summary,
        mcp_description: frontmatter.mcp_description,
        body: description,
    })
}

fn parse_frontmatter(path: &Path, text: &str) -> Result<(ProseFrontmatter, String)> {
    let Some(stripped) = text.strip_prefix("---\n") else {
        return Err(invalid(format!(
            "prose `{}` must start with YAML frontmatter",
            path.display()
        )));
    };
    let Some((frontmatter, body)) = stripped.split_once("\n---\n") else {
        return Err(invalid(format!(
            "prose `{}` is missing closing frontmatter marker",
            path.display()
        )));
    };
    let parsed = serde_yaml::from_str(frontmatter).map_err(|source| IdlError::Yaml {
        path: path.to_path_buf(),
        source,
    })?;
    Ok((parsed, body.to_owned()))
}

struct PlaceholderContext<'a> {
    family: &'a str,
    op: &'a str,
    op_path: String,
    op_slug: String,
    result: &'a str,
}

impl<'a> PlaceholderContext<'a> {
    fn new(family: &'a str, op: &'a str, result: &'a str) -> Self {
        Self {
            family,
            op,
            op_path: op.replace('.', "/"),
            op_slug: op.replace('.', "_"),
            result,
        }
    }

    fn value(&self, key: &str) -> Option<&str> {
        match key {
            "family" => Some(self.family),
            "op" => Some(self.op),
            "op_path" => Some(&self.op_path),
            "op_slug" => Some(&self.op_slug),
            "result" => Some(self.result),
            _ => None,
        }
    }
}

fn expand_required(
    field: &str,
    template: &str,
    context: &PlaceholderContext<'_>,
) -> Result<String> {
    let mut output = String::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        let (head, after_head) = rest.split_at(start);
        output.push_str(head);
        let Some(end) = after_head.find('}') else {
            return Err(invalid(format!(
                "unclosed placeholder in `{field}` template `{template}`"
            )));
        };
        let placeholder = &after_head[1..end];
        let value = context.value(placeholder).ok_or_else(|| {
            invalid(format!(
                "unknown placeholder `{{{placeholder}}}` in `{field}` template `{template}`"
            ))
        })?;
        output.push_str(value);
        rest = &after_head[end + 1..];
    }
    output.push_str(rest);
    if contains_placeholder(&output) {
        return Err(invalid(format!(
            "unresolved placeholder in `{field}` template `{template}`"
        )));
    }
    Ok(output)
}

fn expand_cli_path(
    path: Vec<String>,
    context: &PlaceholderContext<'_>,
    command_id: &str,
) -> Result<Vec<String>> {
    path.into_iter()
        .map(|segment| expand_required("cli_path", &segment, context))
        .collect::<Result<Vec<_>>>()
        .and_then(|expanded| {
            validate_cli_path(&expanded, command_id)?;
            Ok(expanded)
        })
}

fn contains_placeholder(value: &str) -> bool {
    value.contains('{') || value.contains('}')
}

fn split_command_id(id: &str) -> Result<(&str, &str)> {
    id.split_once('.')
        .ok_or_else(|| invalid(format!("command id `{id}` must contain a family and op")))
}

fn validate_command_id(id: &str) -> Result<()> {
    let (family, op) = split_command_id(id)?;
    if family.is_empty() || op.is_empty() {
        return Err(invalid(format!("command id `{id}` is malformed")));
    }
    for part in id.split('.') {
        if part.is_empty()
            || !part
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        {
            return Err(invalid(format!(
                "command id `{id}` must use lower snake-case dot segments"
            )));
        }
    }
    Ok(())
}

fn validate_docs_path(docs: &str, command_id: &str) -> Result<()> {
    if docs.starts_with("/docs/") && !contains_placeholder(docs) {
        Ok(())
    } else {
        Err(invalid(format!(
            "command `{command_id}` resolved invalid docs path `{docs}`"
        )))
    }
}

fn validate_cli_path(path: &[String], command_id: &str) -> Result<()> {
    if path.is_empty() {
        return Err(invalid(format!(
            "command `{command_id}` has empty cli path"
        )));
    }
    for segment in path {
        if segment.is_empty()
            || !segment
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
        {
            return Err(invalid(format!(
                "command `{command_id}` has invalid cli path segment `{segment}`"
            )));
        }
    }
    Ok(())
}

fn validate_mcp_name(name: &str, command_id: &str) -> Result<()> {
    if name.starts_with("strata_")
        && name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        Ok(())
    } else {
        Err(invalid(format!(
            "command `{command_id}` resolved invalid mcp name `{name}`"
        )))
    }
}

fn required(value: Option<String>, field: &str, command_id: &str) -> Result<String> {
    value.ok_or_else(|| invalid(format!("command `{command_id}` is missing `{field}`")))
}

fn append_unique(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

fn relative_to(path: &Path, base: &Path) -> String {
    path.strip_prefix(base)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn invalid(message: impl Into<String>) -> IdlError {
    IdlError::Invalid(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn as_of_descriptions_do_not_mislabel_the_timeline_position_as_microseconds() {
        // #3066: `as_of` takes a commit timestamp — a position on the commit
        // timeline (the `timestamp` from `history` output), not a wall-clock
        // microsecond value. Its schema description must not say "microseconds".
        let schemas_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("idl/v1/generated/schemas");
        let mut checked = 0usize;
        for entry in std::fs::read_dir(&schemas_dir)
            .expect("read schemas dir")
            .flatten()
        {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let doc: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read schema"))
                    .expect("parse schema");
            let Some(as_of) = doc
                .get("request")
                .and_then(|request| request.get("properties"))
                .and_then(|properties| properties.get("as_of"))
            else {
                continue;
            };
            let description = as_of
                .get("description")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            assert!(
                !description.to_lowercase().contains("microsecond"),
                "{}: `as_of` is a timeline position, not microseconds: {description}",
                path.display(),
            );
            checked += 1;
        }
        assert!(checked > 0, "expected `as_of` fields to check");
    }

    #[test]
    fn commit_clock_timestamp_outputs_are_not_labeled_wall_clock() {
        // #3112: the `timestamp` on write acks (`CommitReceipt`) and read
        // envelopes (`VersionedValue`, `ScanItem`, `JsonVersionedValue`,
        // `VectorVersionedData`) is a logical commit-timeline position, not a
        // wall-clock/epoch time. Its schema description must say so — a silent
        // or "microseconds" description lets a client render `timestamp: 3` as
        // 1970 (the VS Code bug). Events are excluded: their timestamp really is
        // epoch micros.
        const CLOCK_DEFS: &[&str] = &[
            "CommitReceipt",
            "VersionedValue",
            "ScanItem",
            "JsonVersionedValue",
            "VectorVersionedData",
        ];
        let schemas_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("idl/v1/generated/schemas");
        let mut checked = 0usize;
        for entry in std::fs::read_dir(&schemas_dir)
            .expect("read schemas dir")
            .flatten()
        {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let doc: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read schema"))
                    .expect("parse schema");
            let Some(defs) = doc.get("$defs").and_then(serde_json::Value::as_object) else {
                continue;
            };
            for def_name in CLOCK_DEFS {
                let Some(timestamp) = defs
                    .get(*def_name)
                    .and_then(|def| def.get("properties"))
                    .and_then(|properties| properties.get("timestamp"))
                else {
                    continue;
                };
                let description = timestamp
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_lowercase();
                // The positive signal proves the field is documented as the
                // logical commit clock. It fails on both an empty description
                // (today) and a bare "microseconds" label (the old mislabel),
                // while still allowing a description that negates those terms.
                assert!(
                    description.contains("commit")
                        && (description.contains("timeline") || description.contains("logical")),
                    "{}: `{def_name}.timestamp` must document the commit-timeline clock, not a wall-clock time: {description:?}",
                    path.display(),
                );
                checked += 1;
            }
        }
        assert!(
            checked > 0,
            "expected commit-clock `timestamp` fields to check"
        );
    }

    #[test]
    fn cli_info_advertises_a_path_only_for_verb_surface() {
        // #3058: a real clap verb advertises its runnable path; a `wire`
        // (escape-hatch) command omits it so consumers never render an
        // invocation strata cannot resolve.
        let verb = CliInfo {
            path: vec!["kv".to_owned(), "put".to_owned()],
            surface: "verb".to_owned(),
        };
        let verb_json = serde_json::to_value(&verb).expect("serializes");
        assert_eq!(
            verb_json["path"],
            serde_json::json!(["kv", "put"]),
            "a verb advertises its runnable path"
        );
        assert_eq!(verb_json["surface"], serde_json::json!("verb"));

        let wire = CliInfo {
            path: vec!["kv".to_owned(), "batch-put".to_owned()],
            surface: "wire".to_owned(),
        };
        let wire_json = serde_json::to_value(&wire).expect("serializes");
        assert!(
            wire_json.get("path").is_none(),
            "a wire command must not advertise a runnable cli.path: {wire_json}"
        );
        assert_eq!(wire_json["surface"], serde_json::json!("wire"));
    }

    #[test]
    fn exhaustiveness_accepts_covered_plus_listed() {
        let all = refs(&["KvPut", "KvGet", "GraphWcc"]);
        let covered = refs(&["KvPut", "KvGet"]);
        let listed = vec!["Command::GraphWcc".to_owned()];
        assert!(enforce_exhaustiveness_lists(&all, &covered, &listed).is_ok());
    }

    #[test]
    fn exhaustiveness_rejects_a_new_unlisted_variant() {
        let all = refs(&["KvPut", "BrandNew"]);
        let covered = refs(&["KvPut"]);
        let error = enforce_exhaustiveness_lists(&all, &covered, &[]).unwrap_err();
        assert!(error.to_string().contains("BrandNew"));
    }

    #[test]
    fn exhaustiveness_shrinks_only() {
        let all = refs(&["KvPut"]);
        let covered = refs(&["KvPut"]);
        let listed = vec!["Command::KvPut".to_owned()];
        let error = enforce_exhaustiveness_lists(&all, &covered, &listed).unwrap_err();
        assert!(error.to_string().contains("only shrink"));
    }

    #[test]
    fn exhaustiveness_rejects_unknown_allowlist_names() {
        let all = refs(&["KvPut"]);
        let covered = refs(&["KvPut"]);
        let listed = vec!["Command::Ghost".to_owned()];
        let error = enforce_exhaustiveness_lists(&all, &covered, &listed).unwrap_err();
        assert!(error.to_string().contains("Ghost"));
    }

    fn code_refs<'a>(codes: &'a [&str]) -> BTreeSet<&'a str> {
        codes.iter().copied().collect()
    }

    #[test]
    fn error_code_exhaustiveness_accepts_declared_plus_listed() {
        let registered = code_refs(&["a.b.c", "d.e.f", "g.h.i"]);
        let declared = vec!["a.b.c".to_owned(), "d.e.f".to_owned()];
        let listed = vec!["g.h.i".to_owned()];
        assert!(enforce_error_code_lists(&registered, &declared, &listed).is_ok());
    }

    #[test]
    fn error_code_exhaustiveness_rejects_a_new_undeclared_code() {
        let registered = code_refs(&["a.b.c", "new.code.here"]);
        let declared = vec!["a.b.c".to_owned()];
        let error = enforce_error_code_lists(&registered, &declared, &[]).unwrap_err();
        assert!(error.to_string().contains("new.code.here"));
    }

    #[test]
    fn error_code_exhaustiveness_shrinks_only() {
        let registered = code_refs(&["a.b.c"]);
        let declared = vec!["a.b.c".to_owned()];
        let listed = vec!["a.b.c".to_owned()];
        let error = enforce_error_code_lists(&registered, &declared, &listed).unwrap_err();
        assert!(error.to_string().contains("only shrink"));
    }

    #[test]
    fn error_code_exhaustiveness_rejects_unregistered_allowlist_codes() {
        let registered = code_refs(&["a.b.c"]);
        let declared = vec!["a.b.c".to_owned()];
        let listed = vec!["ghost.code.here".to_owned()];
        let error = enforce_error_code_lists(&registered, &declared, &listed).unwrap_err();
        assert!(error.to_string().contains("ghost.code.here"));
    }

    fn str_refs<'a>(names: &'a [&str]) -> BTreeSet<&'a str> {
        names.iter().copied().collect()
    }

    #[test]
    fn replay_coverage_accepts_replayed_plus_listed() {
        let declared = str_refs(&["a.b.c", "d.e.f", "g.h.i"]);
        let replayed = str_refs(&["a.b.c"]);
        let listed = vec!["d.e.f".to_owned(), "g.h.i".to_owned()];
        assert!(enforce_replay_coverage_lists(&declared, &replayed, &listed).is_ok());
    }

    #[test]
    fn replay_coverage_rejects_a_declared_code_neither_replayed_nor_listed() {
        let declared = str_refs(&["a.b.c", "d.e.f"]);
        let replayed = str_refs(&["a.b.c"]);
        let error = enforce_replay_coverage_lists(&declared, &replayed, &[]).unwrap_err();
        assert!(error.to_string().contains("d.e.f"));
        assert!(error.to_string().contains("no error-case replay fixture"));
    }

    #[test]
    fn replay_coverage_shrinks_only_when_a_listed_code_becomes_replayed() {
        let declared = str_refs(&["a.b.c"]);
        let replayed = str_refs(&["a.b.c"]);
        let listed = vec!["a.b.c".to_owned()];
        let error = enforce_replay_coverage_lists(&declared, &replayed, &listed).unwrap_err();
        assert!(error.to_string().contains("only shrink"));
    }

    #[test]
    fn replay_coverage_rejects_a_listed_code_no_command_declares() {
        let declared = str_refs(&["a.b.c"]);
        let replayed = str_refs(&[]);
        let listed = vec!["ghost.code.here".to_owned()];
        let error = enforce_replay_coverage_lists(&declared, &replayed, &listed).unwrap_err();
        assert!(error.to_string().contains("ghost.code.here"));
    }

    #[test]
    fn replay_declaration_accepts_a_replayed_declared_code() {
        let mut by_command: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        by_command.insert("kv.get", str_refs(&["not_found.engine.branch", "x.y.z"]));
        let replays = vec![("kv.get".to_owned(), "x.y.z".to_owned())];
        assert!(enforce_replay_declaration(&by_command, &replays).is_ok());
    }

    #[test]
    fn replay_declaration_rejects_a_replay_the_command_does_not_declare() {
        let mut by_command: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        by_command.insert("kv.get", str_refs(&["not_found.engine.branch"]));
        let replays = vec![("kv.get".to_owned(), "undeclared.code.here".to_owned())];
        let error = enforce_replay_declaration(&by_command, &replays).unwrap_err();
        assert!(error.to_string().contains("undeclared.code.here"));
        assert!(error.to_string().contains("does not declare it"));
    }

    #[test]
    fn replay_declaration_rejects_an_unknown_command() {
        let by_command: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        let replays = vec![("kv.ghost".to_owned(), "a.b.c".to_owned())];
        let error = enforce_replay_declaration(&by_command, &replays).unwrap_err();
        assert!(error.to_string().contains("kv.ghost"));
    }

    #[test]
    fn replay_skip_accepts_skipped_plus_listed() {
        let ids = str_refs(&["kv.put", "kv.get", "inference.embed"]);
        let skipped = str_refs(&["inference.embed"]);
        let listed = vec!["inference.embed".to_owned()];
        assert!(enforce_replay_skip_lists(&ids, &skipped, &listed).is_ok());
    }

    #[test]
    fn replay_skip_rejects_a_new_unlisted_skip() {
        let ids = str_refs(&["kv.put", "inference.embed"]);
        let skipped = str_refs(&["inference.embed"]);
        let error = enforce_replay_skip_lists(&ids, &skipped, &[]).unwrap_err();
        assert!(error.to_string().contains("inference.embed"));
        assert!(error.to_string().contains("not listed"));
    }

    #[test]
    fn replay_skip_shrinks_only() {
        // A listed command that no longer skips must be removed.
        let ids = str_refs(&["kv.get"]);
        let skipped = str_refs(&[]);
        let listed = vec!["kv.get".to_owned()];
        let error = enforce_replay_skip_lists(&ids, &skipped, &listed).unwrap_err();
        assert!(error.to_string().contains("only shrink"));
    }

    #[test]
    fn debt_budget_accepts_an_exact_match() {
        assert!(enforce_debt_budget("any.yaml", 110, 110).is_ok());
        assert!(enforce_debt_budget("any.yaml", 0, 0).is_ok());
    }

    #[test]
    fn debt_budget_rejects_growth_past_the_budget() {
        let error = enforce_debt_budget("unreplayed-error-codes.yaml", 111, 110).unwrap_err();
        let msg = error.to_string();
        assert!(msg.contains("grew past its budget"), "{msg}");
        assert!(msg.contains("111") && msg.contains("110"), "{msg}");
        assert!(msg.contains("raise `budget`"), "{msg}");
    }

    #[test]
    fn debt_budget_rejects_a_stale_budget_above_the_count() {
        // Draining an entry without lowering the budget must fail, forcing the
        // ratchet down so the reduction is locked in (not left as regrowth slack).
        let error = enforce_debt_budget("replay-skipped-commands.yaml", 11, 12).unwrap_err();
        let msg = error.to_string();
        assert!(msg.contains("ratchets DOWN"), "{msg}");
        assert!(msg.contains("lower `budget`"), "{msg}");
        assert!(msg.contains("11") && msg.contains("12"), "{msg}");
    }

    #[test]
    fn replay_skip_rejects_unknown_allowlist_ids() {
        let ids = str_refs(&["kv.put"]);
        let skipped = str_refs(&[]);
        let listed = vec!["kv.ghost".to_owned()];
        let error = enforce_replay_skip_lists(&ids, &skipped, &listed).unwrap_err();
        assert!(error.to_string().contains("kv.ghost"));
    }

    #[test]
    fn placeholder_expansion_rejects_unknown_placeholders() {
        let context = PlaceholderContext::new("kv", "put", "KvWrite");
        let error = expand_required("docs", "/docs/{unknown}", &context)
            .expect_err("unknown placeholder should fail");
        assert!(error.to_string().contains("unknown placeholder"));
    }

    #[test]
    fn executor_variant_refs_resolve_to_wire_tags() {
        assert_eq!(
            variant_wire_tag("Command::KvHistory", "Command").expect("command tag resolves"),
            "kv_history"
        );
        assert_eq!(
            variant_wire_tag("Output::KeysPage", "Output").expect("output tag resolves"),
            "keys_page"
        );
        assert_eq!(
            variant_wire_tag("Output::VectorIndexQuery", "Output").expect("output tag resolves"),
            "vector_index_query"
        );
    }

    #[test]
    fn resolved_commands_publish_executable_wire_tags() {
        // #2704: each resolved catalog entry carries the executable wire `type`
        // tag derived from its Command variant, so a tool reading the catalog
        // can construct a call. Exercises the ResolvedCommand construction.
        let index = resolve_default_index().expect("resolve default command index");
        let wire_of = |id: &str| -> String {
            index
                .commands
                .iter()
                .find(|command| command.id == id)
                .unwrap_or_else(|| panic!("`{id}` resolves"))
                .wire
                .clone()
        };
        assert_eq!(wire_of("kv.list"), "kv_list");
        assert_eq!(wire_of("json.scan"), "json_scan");
        assert_eq!(wire_of("admin.ping"), "ping");
        for command in &index.commands {
            assert!(
                !command.wire.is_empty(),
                "{} carries a wire tag",
                command.id
            );
            assert_ne!(
                command.wire, command.id,
                "{} wire tag is not its id",
                command.id
            );
        }
    }

    #[test]
    fn wire_status_allows_only_stable_or_transitional() {
        validate_wire_status("kv.put", "stable").expect("stable is valid");
        validate_wire_status("vector.collection.create", "transitional")
            .expect("transitional is valid");
        let error = validate_wire_status("kv.put", "experimental")
            .expect_err("unknown wire status should fail");
        assert!(error.to_string().contains("invalid wire_status"));
    }
}
