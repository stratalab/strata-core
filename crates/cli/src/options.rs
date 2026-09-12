//! Command-line option model.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use strata_executor::{HubDatasetSort, JsonIndexType};

/// Strata V1 command-line interface.
// A clap argument struct: each bool is an independent CLI switch, which is
// exactly the shape the excessive-bools lint exists to steer *API* types away
// from.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Parser)]
#[command(
    name = "strata",
    version,
    about = "Strata database CLI",
    after_help = "get started:\n  strata ./my-db kv put greeting hello          a database is created by writing to it\n  strata agents guide                           the full surface, written for agents\n  npx skills add stratalab/strata-agent-skills  install the Strata agent skills\n  https://stratadb.org                          docs"
)]
pub(crate) struct Cli {
    /// Durable database path.
    #[arg(value_name = "DB")]
    pub(crate) db_path: Option<PathBuf>,
    /// Durable database path. Cannot be combined with the positional DB path.
    #[arg(long, value_name = "PATH")]
    pub(crate) db: Option<PathBuf>,
    /// Use an in-memory cache database for this process.
    #[arg(long)]
    pub(crate) cache: bool,
    /// Commit durability for a durable database: `standard` (default)
    /// acknowledges from a buffered WAL and syncs at close/threshold;
    /// `always` syncs every commit before acknowledging it.
    #[arg(long, value_enum, value_name = "MODE", conflicts_with = "cache")]
    pub(crate) durability: Option<DurabilityArg>,
    /// Multi-process access for a durable database: `host` (default) hosts a
    /// socket other processes broker to; `client` brokers to an existing owner
    /// without hosting; `off` opts out (single-process only, no socket).
    #[arg(long, value_enum, value_name = "MODE", conflicts_with = "cache")]
    pub(crate) ipc: Option<IpcArg>,
    /// Open the durable database read-only: every write-classified command is
    /// rejected. Enforced by the owner's dispatch gate when brokered to
    /// another process, and at this connection otherwise.
    #[arg(long, conflicts_with = "cache")]
    pub(crate) read_only: bool,
    /// Default branch for commands that accept a branch.
    #[arg(long, global = true)]
    pub(crate) branch: Option<String>,
    /// Product space for commands that accept a space.
    #[arg(long, global = true)]
    pub(crate) space: Option<String>,
    /// Emit compact JSON.
    #[arg(long, global = true, conflicts_with_all = ["raw", "format"])]
    pub(crate) json: bool,
    /// Shell-composable output: bare values, one per line, tab-separated
    /// fields; no header, hint or summary (a write prints its identity).
    #[arg(long, global = true, conflicts_with_all = ["json", "format"])]
    pub(crate) raw: bool,
    /// Transitional output format flag.
    #[arg(long = "output-format", value_enum, global = true, hide = true)]
    pub(crate) format: Option<Format>,
    /// Command to run.
    #[command(subcommand)]
    pub(crate) command: Option<TopCommand>,
}

/// Commit durability mode flag values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum DurabilityArg {
    /// Buffered WAL; commits become durable at the next sync point.
    Standard,
    /// Every commit is synced before acknowledgement.
    Always,
}

impl DurabilityArg {
    pub(crate) const fn mode(self) -> strata_executor::DurabilityMode {
        match self {
            Self::Standard => strata_executor::DurabilityMode::Standard,
            Self::Always => strata_executor::DurabilityMode::Always,
        }
    }
}

/// Multi-process access mode flag values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum IpcArg {
    /// Win the lock and host a socket other processes can broker to.
    Host,
    /// Win the lock without hosting; broker to an existing owner on contention.
    Client,
    /// Single-process only: no socket, no broker fallback.
    Off,
}

impl IpcArg {
    pub(crate) const fn mode(self) -> strata_executor::IpcMode {
        match self {
            Self::Host => strata_executor::IpcMode::Host,
            Self::Client => strata_executor::IpcMode::Client,
            Self::Off => strata_executor::IpcMode::Off,
        }
    }
}

/// Output format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Format {
    /// Concise human output.
    Human,
    /// Compact JSON.
    Json,
    /// Pretty-printed JSON.
    Pretty,
    /// Script-friendly raw output.
    Raw,
}

impl Cli {
    /// The output format the flags chose, or `None` when they chose nothing.
    /// A one-shot's nothing is human; a session line's nothing is its
    /// session's format (#3326) — the fallback is the caller's to resolve.
    pub(crate) fn format_override(&self) -> Option<Format> {
        if self.raw {
            Some(Format::Raw)
        } else if self.json {
            Some(Format::Json)
        } else {
            self.format
        }
    }

    /// The one-shot's output format: the flags' choice, human by default.
    pub(crate) fn output_format(&self) -> Format {
        self.format_override().unwrap_or(Format::Human)
    }

    /// Why a line typed inside a session (the REPL, the playground) cannot be
    /// honoured as parsed, or `None` when it can. The top-level grammar is the
    /// binary's, so a line parses the session arguments — the database target
    /// and how it is opened — as readily as a command's own flags; a reader
    /// that picked only the fields it wanted silently answered `--db
    /// /elsewhere kv get k` from the current database (#3327). Both line
    /// readers ask here right after the parse.
    pub(crate) fn line_refusal(&self) -> Option<LineRefusal> {
        let session_flags = [
            ("--db", "--db <PATH>", self.db.is_some()),
            ("--cache", "--cache", self.cache),
            (
                "--durability",
                "--durability <MODE>",
                self.durability.is_some(),
            ),
            ("--ipc", "--ipc <MODE>", self.ipc.is_some()),
            ("--read-only", "--read-only", self.read_only),
        ];
        if let Some((flag, usage, _)) = session_flags.into_iter().find(|(_, _, set)| *set) {
            return Some(LineRefusal::SessionFlag { flag, usage });
        }
        let path = self.db_path.as_ref()?.display().to_string();
        Some(if self.command.is_some() {
            LineRefusal::DatabasePath(path)
        } else {
            // A lone word the grammar could only read as the positional
            // database path is far more likely a typo'd verb.
            LineRefusal::NotACommand(path)
        })
    }
}

/// The reason a session line is refused rather than run: it carried an
/// argument that opens a session, which cannot change from inside one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LineRefusal {
    /// A session flag (`--db`, `--cache`, `--durability`, `--ipc`,
    /// `--read-only`), with its usage form for the suggested new session.
    SessionFlag {
        flag: &'static str,
        usage: &'static str,
    },
    /// A positional database path in front of a command.
    DatabasePath(String),
    /// A lone word that names no command (the grammar read it as a database
    /// path).
    NotACommand(String),
}

impl std::fmt::Display for LineRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionFlag { flag, usage } => write!(
                f,
                "`{flag}` opens a session and cannot change inside one; start a new session with `strata {usage} <command>`"
            ),
            Self::DatabasePath(path) => write!(
                f,
                "`{path}` was read as a database path, which opens a session and cannot change inside one; start a new session with `strata {path} <command>`"
            ),
            Self::NotACommand(word) => {
                write!(f, "`{word}` is not a strata command; try `help`")
            }
        }
    }
}

/// Top-level command families.
// The `Inference` family carries the large `Generate` chat-flag set (see the
// same allow on `InferenceCommand`); clap subcommand enums are not boxed.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
pub(crate) enum TopCommand {
    /// Prepare the Strata home directory and print next steps.
    Init,
    /// Check the installation and, when a database is targeted, its health.
    Doctor,
    /// Self-describing surface for agents: guide, catalogs, repo onboarding.
    Agents(AgentsArgs),
    /// Print this build's changelog (#3094): what shipped, matched to the
    /// binary you are holding, offline.
    Changelog {
        /// Print only this release's entry (e.g. `1.2.0`) instead of the whole
        /// file.
        #[arg(long, value_name = "VERSION")]
        version: Option<String>,
    },
    /// Model Context Protocol server commands.
    Mcp(McpArgs),
    /// Lightweight liveness check.
    Ping,
    /// Print database information.
    Info,
    /// Print health facts.
    Health,
    /// Print metrics facts.
    Metrics,
    /// Print a compact database description.
    Describe,
    /// Configuration reads.
    Config(ConfigArgs),
    /// Multi-process IPC status and control.
    Ipc(IpcArgs),
    /// Host a durable database as a persistent broker owner and keep it alive
    /// (blocks until stopped) so other processes can attach to it.
    Start,
    /// Stop a durable database's broker owner.
    Stop,
    /// Show where this database was cloned from (its remote origin).
    Remote,
    /// Clone a dataset from a hub into a new local database.
    Clone(CloneArgs),
    /// Browse StrataHub datasets and refs.
    Hub(HubArgs),
    /// Branch lifecycle commands.
    Branch(BranchArgs),
    /// Product space commands.
    Space(SpaceArgs),
    /// KV commands.
    Kv(KvArgs),
    /// JSON document commands.
    Json(JsonArgs),
    /// Vector commands.
    Vector(VectorArgs),
    /// Event log commands.
    Event(EventArgs),
    /// Graph core commands.
    Graph(GraphArgs),
    /// Arrow import/export commands.
    Arrow(ArrowArgs),
    /// Model execution: local GGUF models and cloud providers.
    #[cfg(feature = "inference")]
    Inference(InferenceArgs),
    /// Raw serialized executor command.
    Command(CommandArgs),
    /// Deferred old search surface.
    #[command(hide = true)]
    Search(DeferredArgs),
    /// Deferred old recipe surface.
    #[command(hide = true)]
    Recipe(DeferredArgs),
    /// Deferred transaction helper.
    #[command(hide = true)]
    Txn(DeferredArgs),
    /// Deferred transaction begin.
    #[command(hide = true)]
    Begin,
    /// Deferred transaction commit.
    #[command(hide = true)]
    Commit,
    /// Deferred transaction rollback.
    #[command(hide = true)]
    Rollback,
    /// Intentionally unavailable maintenance command.
    #[command(hide = true)]
    Flush,
    /// Intentionally unavailable maintenance command.
    #[command(hide = true)]
    Compact,
    /// Deferred daemon lifecycle command.
    #[command(hide = true)]
    Up(DeferredArgs),
    /// Deferred daemon lifecycle command.
    #[command(hide = true)]
    Down(DeferredArgs),
    /// Remove the Strata installation from this machine.
    Uninstall(UninstallArgs),
    /// Update the Strata binary to the latest release.
    Update(UpdateArgs),
}

/// Arguments for `uninstall`.
#[derive(Debug, Args)]
pub(crate) struct UninstallArgs {
    /// Skip the confirmation prompt.
    #[arg(long)]
    pub(crate) yes: bool,
}

/// Arguments for `update`.
#[derive(Debug, Args)]
pub(crate) struct UpdateArgs {
    /// Report whether an update is available without installing it.
    #[arg(long)]
    pub(crate) check: bool,
    /// Install a specific version (for pinning or rollback) instead of the latest.
    #[arg(long, value_name = "X.Y.Z")]
    pub(crate) version: Option<String>,
}

/// Arguments accepted for known deferred commands.
#[derive(Debug, Args)]
pub(crate) struct DeferredArgs {
    /// Deferred command arguments.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub(crate) args: Vec<String>,
}

/// Config command wrapper.
#[derive(Debug, Args)]
pub(crate) struct ConfigArgs {
    /// Config command.
    #[command(subcommand)]
    pub(crate) command: ConfigCommand,
}

/// IPC command wrapper.
#[derive(Debug, Args)]
pub(crate) struct IpcArgs {
    /// IPC command.
    #[command(subcommand)]
    pub(crate) command: IpcSubcommand,
}

/// Multi-process IPC subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum IpcSubcommand {
    /// Report this process's multi-process IPC state.
    Status,
    /// Stop hosting the multi-process broker socket.
    Stop,
}

/// MCP command arguments.
#[derive(Debug, Args)]
pub(crate) struct McpArgs {
    /// MCP command.
    #[command(subcommand)]
    pub(crate) command: McpCommand,
}

/// MCP server commands.
#[derive(Debug, Subcommand)]
pub(crate) enum McpCommand {
    /// Serve MCP over stdio (newline-delimited JSON-RPC; logs on stderr).
    Serve,
}

/// Agent-surface command arguments.
#[derive(Debug, Args)]
pub(crate) struct AgentsArgs {
    /// Agents command.
    #[command(subcommand)]
    pub(crate) command: AgentsCommand,
}

/// Agent-facing self-description commands.
#[derive(Debug, Subcommand)]
pub(crate) enum AgentsCommand {
    /// Print the complete offline usage guide (markdown, version-matched).
    Guide,
    /// Print the machine-readable command catalog.
    Commands,
    /// Print the public error-code registry.
    Errors,
    /// Write repo onboarding files (.strata/AGENTS.md; --apply appends the
    /// pointer block to the repo's AGENTS.md or CLAUDE.md).
    Init {
        /// Append the pointer block to an existing AGENTS.md/CLAUDE.md.
        #[arg(long)]
        apply: bool,
    },
    /// Print the agent skill (markdown, version-matched); --write installs it
    /// for one or more coding agents in the current repo.
    Skill {
        /// Install into the repo instead of printing.
        #[arg(long)]
        write: bool,
        /// With --write: replace an existing Claude/Cursor skill file whose
        /// content differs.
        #[arg(long, requires = "write")]
        force: bool,
        /// Which agents to install for (repeatable): claude
        /// (.claude/skills/strata/SKILL.md), cursor (.cursor/rules/strata.mdc),
        /// codex (a marker-delimited section in AGENTS.md), or all.
        #[arg(long = "for", value_enum, requires = "write", default_values_t = [SkillTarget::Claude])]
        targets: Vec<SkillTarget>,
    },
}

/// Coding agents the skill installs for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum SkillTarget {
    Claude,
    Codex,
    Cursor,
    All,
}

/// Config commands.
#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    /// Print sanitized config.
    Get,
    /// Print one sanitized config value. Keys: `hub.url`, `<provider>.api_key`
    /// (printed redacted), `<provider>.base_url`; providers: `openai`,
    /// `anthropic`, `google`.
    GetKey {
        /// Config key.
        key: String,
    },
    /// Set a user-config key in the global strata config. Keys: `hub.url`,
    /// `<provider>.api_key`, `<provider>.base_url`; providers: `openai`,
    /// `anthropic`, `google`. The file is stored with 0600 permissions; the
    /// provider's own env var (`OPENAI_API_KEY`, `OPENAI_BASE_URL`, …) always
    /// overrides.
    Set {
        /// Config key.
        key: String,
        /// New value.
        value: String,
    },
    /// Remove a user-config key from the global config. Keys: `hub.url`,
    /// `<provider>.api_key`, `<provider>.base_url`.
    Unset {
        /// Config key.
        key: String,
    },
    /// Print the global strata config file path.
    Path,
    /// Print the resolved hub configuration and which layer supplied it.
    Show,
}

/// Clone command arguments.
#[derive(Debug, Args)]
pub(crate) struct CloneArgs {
    /// Dataset to clone (its hub slug).
    pub(crate) dataset: String,
    /// Destination directory. Defaults to `./<dataset>.strata`.
    pub(crate) dest: Option<std::path::PathBuf>,
    /// Branch to fetch. Defaults to the dataset's default branch.
    #[arg(long)]
    pub(crate) branch: Option<String>,
    /// Hub URL for this invocation (overrides env and config files).
    #[arg(long)]
    pub(crate) hub: Option<String>,
    /// Emit machine-readable progress events.
    #[arg(long, value_enum, value_name = "MODE")]
    pub(crate) progress: Option<CloneProgressFormat>,
}

/// Clone progress output format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CloneProgressFormat {
    /// Newline-delimited compact JSON output.
    #[value(alias = "ndjson")]
    Jsonl,
}

/// Hub command wrapper.
#[derive(Debug, Args)]
pub(crate) struct HubArgs {
    /// Hub command.
    #[command(subcommand)]
    pub(crate) command: HubCommand,
}

/// Hub browse commands.
#[derive(Debug, Subcommand)]
pub(crate) enum HubCommand {
    /// Read the hub capability advertisement.
    Info {
        /// Hub URL for this invocation (overrides env and config files).
        #[arg(long)]
        hub: Option<String>,
    },
    /// List hub datasets.
    ListDatasets(HubListDatasetsArgs),
    /// Read one dataset card.
    GetDataset {
        /// Dataset slug.
        name: String,
        /// Hub URL for this invocation (overrides env and config files).
        #[arg(long)]
        hub: Option<String>,
    },
    /// List live refs for one dataset.
    ListRefs {
        /// Dataset slug.
        dataset: String,
        /// Hub URL for this invocation (overrides env and config files).
        #[arg(long)]
        hub: Option<String>,
    },
    /// List yanked refs.
    ListYanked {
        /// RFC 3339 lower-bound timestamp.
        #[arg(long)]
        since: Option<String>,
        /// Hub URL for this invocation (overrides env and config files).
        #[arg(long)]
        hub: Option<String>,
    },
}

/// Arguments for `hub list-datasets`.
#[derive(Debug, Args)]
pub(crate) struct HubListDatasetsArgs {
    /// Hub URL for this invocation (overrides env and config files).
    #[arg(long)]
    pub(crate) hub: Option<String>,
    /// Task filter. Repeat to OR within the task dimension.
    #[arg(long = "task")]
    pub(crate) tasks: Vec<String>,
    /// Tag filter. Repeat to OR within the tag dimension.
    #[arg(long = "tag")]
    pub(crate) tags: Vec<String>,
    /// Primitive filter. Repeat to OR within the primitive dimension.
    #[arg(long = "primitive")]
    pub(crate) primitives: Vec<String>,
    /// License identifier filter.
    #[arg(long)]
    pub(crate) license: Option<String>,
    /// Minimum dataset size in bytes.
    #[arg(long)]
    pub(crate) size_min_bytes: Option<u64>,
    /// Maximum dataset size in bytes.
    #[arg(long)]
    pub(crate) size_max_bytes: Option<u64>,
    /// Sort key.
    #[arg(long, value_enum)]
    pub(crate) sort: Option<HubDatasetSortArg>,
    /// Page size. StrataHub V1 accepts 1..=200.
    #[arg(long)]
    pub(crate) limit: Option<u32>,
    /// Zero-based page offset.
    #[arg(long)]
    pub(crate) offset: Option<u32>,
}

/// Dataset-list sort key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum HubDatasetSortArg {
    /// Most-downloaded datasets first.
    Downloads,
    /// Most-recently-updated datasets first.
    Recent,
    /// Dataset-name lexicographic order.
    Name,
    /// Largest datasets first.
    Size,
}

impl From<HubDatasetSortArg> for HubDatasetSort {
    fn from(value: HubDatasetSortArg) -> Self {
        match value {
            HubDatasetSortArg::Downloads => Self::Downloads,
            HubDatasetSortArg::Recent => Self::Recent,
            HubDatasetSortArg::Name => Self::Name,
            HubDatasetSortArg::Size => Self::Size,
        }
    }
}

/// Branch command wrapper.
#[derive(Debug, Args)]
pub(crate) struct BranchArgs {
    /// Branch command.
    #[command(subcommand)]
    pub(crate) command: BranchCommand,
}

/// Branch commands.
#[derive(Debug, Subcommand)]
pub(crate) enum BranchCommand {
    /// List branches.
    List,
    /// Read one branch.
    Get {
        /// Branch name.
        branch: String,
    },
    /// Create an empty root branch.
    Create {
        /// Branch name.
        branch: String,
    },
    /// Fork a branch.
    Fork {
        /// Source branch.
        source: String,
        /// New branch name.
        branch: String,
        /// Fork from a retained source version.
        #[arg(long, conflicts_with = "timestamp")]
        version: Option<u64>,
        /// Fork from a retained source timestamp.
        #[arg(long, conflicts_with = "version")]
        timestamp: Option<u64>,
    },
    /// Delete a branch.
    #[command(alias = "del")]
    Delete {
        /// Branch name.
        branch: String,
    },
    /// Compare two branches.
    Diff {
        /// The first branch (the `A` side).
        branch_a: String,
        /// The second branch (the `B` side).
        branch_b: String,
        /// Compare each branch as of a position on the logical commit
        /// timeline: the `timestamp` from `history` output, not the
        /// `version`. This is a per-commit counter, never a calendar date.
        #[arg(long)]
        as_of: Option<u64>,
    },
    /// Promote one branch's changes into another.
    Merge {
        /// The branch whose changes are promoted.
        source: String,
        /// The branch that receives the promotion.
        target: String,
        /// Conflict-resolution strategy.
        #[arg(long, value_enum, default_value_t = CliMergeStrategy::Strict)]
        strategy: CliMergeStrategy,
    },
    /// Preview promoting one branch into another, reporting conflicts.
    Preview {
        /// The branch whose changes would be promoted.
        source: String,
        /// The branch that would receive the promotion.
        target: String,
        /// Conflict-resolution strategy to evaluate the preview under.
        #[arg(long, value_enum, default_value_t = CliMergeStrategy::Strict)]
        strategy: CliMergeStrategy,
    },
    /// Deferred branch tag command.
    #[command(hide = true)]
    Tag(DeferredArgs),
    /// Deferred branch note command.
    #[command(hide = true)]
    Note(DeferredArgs),
}

/// Product space command wrapper.
#[derive(Debug, Args)]
pub(crate) struct SpaceArgs {
    /// Space command.
    #[command(subcommand)]
    pub(crate) command: SpaceCommand,
}

/// Product space commands.
#[derive(Debug, Subcommand)]
pub(crate) enum SpaceCommand {
    /// List spaces.
    List,
    /// Create a space.
    Create {
        /// Space name.
        space: String,
    },
    /// Check whether a space exists.
    Exists {
        /// Space name.
        space: String,
    },
    /// Delete a space.
    #[command(alias = "del")]
    Delete {
        /// Space name.
        space: String,
        /// Delete visible data in the space before dropping the catalog entry.
        #[arg(long)]
        force: bool,
    },
}

/// KV command wrapper.
#[derive(Debug, Args)]
pub(crate) struct KvArgs {
    /// KV command.
    #[command(subcommand)]
    pub(crate) command: KvCommand,
}

/// KV commands.
#[derive(Debug, Subcommand)]
pub(crate) enum KvCommand {
    /// Write one key.
    Put {
        /// Key.
        key: String,
        /// Value. Use @path to read bytes from a file.
        value: Option<String>,
        /// Read value bytes from a file.
        #[arg(short = 'f', long, conflicts_with = "value")]
        file: Option<PathBuf>,
    },
    /// Read one key.
    Get {
        /// Key.
        key: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Delete one key.
    #[command(alias = "del")]
    Delete {
        /// Key.
        key: String,
    },
    /// List keys.
    List {
        /// Optional key prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Optional base64 continuation cursor as printed by the previous page.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Scan rows.
    Scan {
        /// Optional inclusive start key.
        #[arg(long, conflicts_with = "cursor")]
        start: Option<String>,
        /// Optional base64 continuation cursor as printed by the previous page.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Check key existence.
    Exists {
        /// Key.
        key: String,
    },
    /// Read version history.
    History {
        /// Key.
        key: String,
    },
    /// Count keys.
    Count {
        /// Optional key prefix.
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Sample rows.
    Sample {
        /// Optional key prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Optional sample count.
        #[arg(long)]
        count: Option<u64>,
    },
}

/// JSON command wrapper.
#[derive(Debug, Args)]
pub(crate) struct JsonArgs {
    /// JSON command.
    #[command(subcommand)]
    pub(crate) command: JsonCommand,
}

/// JSON commands.
#[derive(Debug, Subcommand)]
pub(crate) enum JsonCommand {
    /// Set a JSON path.
    Set {
        /// Document key.
        key: String,
        /// JSON path.
        path: String,
        /// JSON value. Non-JSON text is stored as a string. Use @path to read from a file.
        value: Option<String>,
        /// Read JSON value from a file.
        #[arg(short = 'f', long, conflicts_with = "value")]
        file: Option<PathBuf>,
    },
    /// Read a JSON path.
    Get {
        /// Document key.
        key: String,
        /// JSON path.
        path: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Delete a JSON path.
    #[command(alias = "del")]
    Delete {
        /// Document key.
        key: String,
        /// JSON path.
        path: String,
    },
    /// List JSON document keys.
    List {
        /// Optional document prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Optional continuation cursor.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Scan JSON documents (keys and values).
    Scan {
        /// Optional inclusive start document key.
        #[arg(long, conflicts_with = "cursor")]
        start: Option<String>,
        /// Optional continuation cursor as printed by the previous page.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Check whether a document exists.
    Exists {
        /// Document key.
        key: String,
    },
    /// Read document history.
    History {
        /// Document key.
        key: String,
    },
    /// Count documents.
    Count {
        /// Optional document prefix.
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Sample documents.
    Sample {
        /// Optional document prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Optional sample count.
        #[arg(long)]
        count: Option<u64>,
    },
    /// JSON secondary index commands.
    Index {
        /// Index command.
        #[command(subcommand)]
        command: JsonIndexCommand,
    },
}

/// JSON index commands.
#[derive(Debug, Subcommand)]
pub(crate) enum JsonIndexCommand {
    /// Create an index.
    Create {
        /// Index name.
        name: String,
        /// Indexed field path.
        field_path: String,
        /// Index type.
        #[arg(long, value_enum, default_value_t = CliJsonIndexType::Tag)]
        index_type: CliJsonIndexType,
    },
    /// Drop an index.
    Drop {
        /// Index name.
        name: String,
    },
    /// List indexes.
    List,
}

/// JSON index kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliJsonIndexType {
    /// Numeric index.
    Numeric,
    /// Tag/string index.
    Tag,
    /// Lowercase text index.
    Text,
}

impl From<CliJsonIndexType> for JsonIndexType {
    fn from(value: CliJsonIndexType) -> Self {
        match value {
            CliJsonIndexType::Numeric => Self::Numeric,
            CliJsonIndexType::Tag => Self::Tag,
            CliJsonIndexType::Text => Self::Text,
        }
    }
}

/// Vector command wrapper.
#[derive(Debug, Args)]
pub(crate) struct VectorArgs {
    /// Vector command.
    #[command(subcommand)]
    pub(crate) command: VectorCommand,
}

/// Vector commands.
#[derive(Debug, Subcommand)]
pub(crate) enum VectorCommand {
    /// Vector collection commands.
    Collection {
        /// Collection command.
        #[command(subcommand)]
        command: VectorCollectionCommand,
    },
    /// Upsert one vector.
    Upsert {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Vector as JSON array, comma-separated floats, or @path.
        vector: Option<String>,
        /// Read vector from a file.
        #[arg(short = 'f', long, conflicts_with = "vector")]
        file: Option<PathBuf>,
        /// Embed this text with the collection's recorded model instead of
        /// supplying a vector (D10).
        ///
        /// The collection must record a model — `--embedding-model` at
        /// create, or `vector collection set-embedding-model` later — which
        /// is what says which model to call.
        #[arg(long, conflicts_with_all = ["vector", "file"])]
        text: Option<String>,
        /// Optional metadata JSON object.
        #[arg(long)]
        metadata: Option<String>,
        /// Read metadata JSON object from a file.
        #[arg(long, conflicts_with = "metadata")]
        metadata_file: Option<PathBuf>,
    },
    /// Read one vector.
    Get {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Read vector history.
    History {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
    },
    /// Check vector existence.
    Exists {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
    },
    /// List vector keys.
    Keys {
        /// Collection name.
        collection: String,
        /// Optional key prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Optional continuation cursor.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Scan vectors (keys and values).
    Scan {
        /// Collection name.
        collection: String,
        /// Optional inclusive start key.
        #[arg(long, conflicts_with = "cursor")]
        start: Option<String>,
        /// Optional continuation cursor as printed by the previous page.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Patch vector metadata.
    UpdateMetadata {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Top-level metadata patch JSON or @path.
        patch: Option<String>,
        /// Read metadata patch from a file.
        #[arg(short = 'f', long, conflicts_with = "patch")]
        file: Option<PathBuf>,
    },
    /// Delete one vector.
    #[command(alias = "del")]
    Delete {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
    },
    /// Delete all vectors in a collection.
    DeleteAll {
        /// Collection name.
        collection: String,
    },
    /// Delete vectors matching a metadata filter JSON object.
    DeleteByFilter {
        /// Collection name.
        collection: String,
        /// Serialized `VectorMetadataFilter` JSON.
        #[arg(long, conflicts_with = "filter_file")]
        filter: Option<String>,
        /// Read filter JSON from a file.
        #[arg(long)]
        filter_file: Option<PathBuf>,
    },
    /// Search vectors.
    Query {
        /// Collection name.
        collection: String,
        /// Query vector as JSON array, comma-separated floats, or @path.
        query: Option<String>,
        /// Read query vector from a file.
        #[arg(short = 'f', long, conflicts_with = "query")]
        file: Option<PathBuf>,
        /// Embed this text with the collection's recorded model and search
        /// with it (D10).
        ///
        /// Uses the same model the collection was written with, so the query
        /// cannot accidentally be compared against another model's vectors.
        #[arg(long, conflicts_with_all = ["query", "file"])]
        text: Option<String>,
        /// Maximum number of matches.
        #[arg(short = 'k', long, default_value_t = 10)]
        k: u64,
        /// Serialized `VectorMetadataFilter` JSON.
        #[arg(long, conflicts_with = "filter_file")]
        filter: Option<String>,
        /// Read filter JSON from a file.
        #[arg(long)]
        filter_file: Option<PathBuf>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
        /// Include vector index diagnostics.
        #[arg(long)]
        diagnostics: bool,
    },
    /// Count vectors.
    Count {
        /// Collection name.
        collection: String,
    },
    /// Sample vectors (keys and values).
    Sample {
        /// Collection name.
        collection: String,
        /// Optional sample count.
        #[arg(long)]
        count: Option<u64>,
    },
}

/// Vector collection commands.
#[derive(Debug, Subcommand)]
pub(crate) enum VectorCollectionCommand {
    /// Create a collection.
    Create {
        /// Collection name.
        collection: String,
        /// Embedding dimension.
        dimension: u64,
        /// Distance metric.
        #[arg(long, value_enum, default_value_t = CliVectorMetric::Cosine)]
        metric: CliVectorMetric,
        /// Model that produces this collection's vectors, e.g. `miniLM` or
        /// `openai:text-embedding-3-small`.
        ///
        /// `--text` on `vector upsert` and `vector query` is then embedded
        /// with this model and no other, so text writes and searches cannot
        /// mix models — two models at the same width return neighbours that
        /// are ranked and meaningless. A vector you supply directly carries
        /// no model and is not checked: supplying one is your statement that
        /// this model produced it.
        #[arg(long)]
        embedding_model: Option<String>,
    },
    /// Delete a collection.
    #[command(alias = "del")]
    Delete {
        /// Collection name.
        collection: String,
    },
    /// List collections.
    List,
    /// Read collection stats.
    Stats {
        /// Collection name.
        collection: String,
    },
    /// Declare the model that produces a collection's vectors.
    ///
    /// A declaration, not a verification: stored vectors carry no model, so
    /// this takes your word for the ones present, and `--text` is embedded
    /// with this model from then on. Vectors you supply directly stay your
    /// word. Declared once: a collection that already records this model is
    /// left as it is, and one that records a different model is refused,
    /// since its stored vectors came from that model. A collection with no
    /// recorded model cannot embed `--text`.
    SetEmbeddingModel {
        /// Collection name.
        collection: String,
        /// Model id, e.g. `miniLM` or `openai:text-embedding-3-small`.
        model: String,
    },
}

/// Branch promotion (merge) conflict-resolution strategy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliMergeStrategy {
    /// Refuse the promotion when any conflict exists.
    Strict,
    /// Apply the source side's value or tombstone for each conflict.
    SourceWins,
}

impl From<CliMergeStrategy> for strata_executor::PromotionStrategy {
    fn from(value: CliMergeStrategy) -> Self {
        match value {
            CliMergeStrategy::Strict => Self::Strict,
            CliMergeStrategy::SourceWins => Self::SourceWins,
        }
    }
}

/// Vector metric.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliVectorMetric {
    /// Cosine similarity.
    Cosine,
    /// Euclidean similarity.
    Euclidean,
    /// Dot product.
    DotProduct,
}

impl From<CliVectorMetric> for strata_executor::VectorDistanceMetric {
    fn from(value: CliVectorMetric) -> Self {
        match value {
            CliVectorMetric::Cosine => Self::Cosine,
            CliVectorMetric::Euclidean => Self::Euclidean,
            CliVectorMetric::DotProduct => Self::DotProduct,
        }
    }
}

/// Event command wrapper.
#[derive(Debug, Args)]
pub(crate) struct EventArgs {
    /// Event command.
    #[command(subcommand)]
    pub(crate) command: EventCommand,
}

/// Event commands.
#[derive(Debug, Subcommand)]
pub(crate) enum EventCommand {
    /// Append one event.
    Append {
        /// Event type.
        event_type: String,
        /// Event payload JSON or @path.
        payload: Option<String>,
        /// Read event payload from a file.
        #[arg(short = 'f', long, conflicts_with = "payload")]
        file: Option<PathBuf>,
    },
    /// Read one event.
    Get {
        /// Event sequence.
        sequence: u64,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Check event existence.
    Exists {
        /// Event sequence.
        sequence: u64,
    },
    /// Count visible events.
    Count {
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// List events.
    List {
        /// Optional event type filter.
        #[arg(long)]
        event_type: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Optional exclusive sequence cursor.
        #[arg(long, alias = "cursor")]
        after_sequence: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// List event types.
    Types {
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// List events by type.
    ByType {
        /// Event type.
        event_type: String,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Optional exclusive sequence cursor.
        #[arg(long)]
        after_sequence: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Read an event sequence range.
    Range {
        /// Inclusive start sequence; with reverse direction, walk backward from this sequence.
        start_seq: u64,
        /// Optional exclusive end sequence; with reverse direction, exclusive lower bound.
        #[arg(long)]
        end_seq: Option<u64>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Range direction.
        #[arg(long, value_enum, default_value_t = CliEventDirection::Forward)]
        direction: CliEventDirection,
        /// Optional event type filter.
        #[arg(long)]
        event_type: Option<String>,
    },
    /// Read events by timestamp range.
    RangeTime {
        /// Inclusive start timestamp.
        start_ts: u64,
        /// Optional inclusive end timestamp.
        #[arg(long)]
        end_ts: Option<u64>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Range direction.
        #[arg(long, value_enum, default_value_t = CliEventDirection::Forward)]
        direction: CliEventDirection,
        /// Optional event type filter.
        #[arg(long)]
        event_type: Option<String>,
    },
    /// Verify sequence density and hash linkage.
    VerifyChain,
}

/// Event range direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliEventDirection {
    /// Forward order.
    Forward,
    /// Reverse order.
    Reverse,
}

impl From<CliEventDirection> for strata_executor::EventRangeDirection {
    fn from(value: CliEventDirection) -> Self {
        match value {
            CliEventDirection::Forward => Self::Forward,
            CliEventDirection::Reverse => Self::Reverse,
        }
    }
}

/// Graph command wrapper.
#[derive(Debug, Args)]
pub(crate) struct GraphArgs {
    /// Graph command.
    #[command(subcommand)]
    pub(crate) command: GraphCommand,
}

/// Graph commands.
#[derive(Debug, Subcommand)]
pub(crate) enum GraphCommand {
    /// Create a graph.
    Create {
        /// Graph name.
        graph: String,
    },
    /// Delete a graph.
    #[command(alias = "del")]
    Delete {
        /// Graph name.
        graph: String,
    },
    /// List graphs.
    List {
        /// Optional graph cursor.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Read graph metadata.
    Meta {
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Add or replace a node.
    AddNode {
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
        /// Optional properties JSON.
        #[arg(long, conflicts_with = "properties_file")]
        properties: Option<String>,
        /// Read properties JSON from a file.
        #[arg(long)]
        properties_file: Option<PathBuf>,
        /// Optional declared object type (validated once the ontology is frozen).
        #[arg(long = "type")]
        object_type: Option<String>,
    },
    /// Read a node.
    GetNode {
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Remove a node.
    RemoveNode {
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
    },
    /// List nodes.
    ListNodes {
        /// Graph name.
        graph: String,
        /// Optional node prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Optional node cursor.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Sample graph nodes.
    Sample {
        /// Graph name.
        graph: String,
        /// Optional sample count.
        #[arg(long)]
        count: Option<u64>,
    },
    /// Add or replace an edge.
    AddEdge {
        /// Graph name.
        graph: String,
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
        /// Optional edge weight.
        #[arg(long)]
        weight: Option<f64>,
        /// Optional properties JSON.
        #[arg(long, conflicts_with = "properties_file")]
        properties: Option<String>,
        /// Read properties JSON from a file.
        #[arg(long)]
        properties_file: Option<PathBuf>,
    },
    /// Read an edge.
    GetEdge {
        /// Graph name.
        graph: String,
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Remove an edge.
    RemoveEdge {
        /// Graph name.
        graph: String,
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
    },
    /// List neighbors.
    Neighbors {
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
        /// Traversal direction.
        #[arg(long, value_enum, default_value_t = CliGraphDirection::Outgoing)]
        direction: CliGraphDirection,
        /// Optional edge type filter.
        #[arg(long)]
        edge_type: Option<String>,
        /// Optional continuation cursor.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// List nodes declaring an object type.
    NodesByType {
        /// Graph name.
        graph: String,
        /// Object type name.
        object_type: String,
        /// Optional node cursor.
        #[arg(long)]
        cursor: Option<String>,
        /// Optional item limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Graph ontology commands.
    Ontology(GraphOntologyArgs),
    /// Compute weakly connected components.
    Wcc {
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Compute local clustering coefficients.
    Lcc {
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Compute shortest-path distances from a source node.
    Sssp {
        /// Graph name.
        graph: String,
        /// Source node id.
        source: String,
        /// Traversal direction.
        #[arg(long, value_enum, default_value_t = CliGraphDirection::Outgoing)]
        direction: CliGraphDirection,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Compute pagerank importance scores, optionally personalized.
    Pagerank {
        /// Graph name.
        graph: String,
        /// Optional damping factor (default 0.85).
        #[arg(long)]
        damping: Option<f64>,
        /// Optional iteration bound (default 20).
        #[arg(long)]
        max_iterations: Option<u64>,
        /// Optional convergence tolerance (default 1e-6).
        #[arg(long)]
        tolerance: Option<f64>,
        /// Optional seed weights as JSON, e.g. '{"node": 1.0}'.
        #[arg(long)]
        personalization: Option<String>,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Detect communities via label propagation.
    Cdlp {
        /// Graph name.
        graph: String,
        /// Optional iteration bound (default 10).
        #[arg(long)]
        max_iterations: Option<u64>,
        /// Propagation direction.
        #[arg(long, value_enum, default_value_t = CliGraphDirection::Both)]
        direction: CliGraphDirection,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Ingest nodes and edges from JSON in chunked commits.
    BulkInsert {
        /// Graph name.
        graph: String,
        /// Inline JSON payload: {"nodes": [...], "edges": [...]}.
        #[arg(long, conflicts_with = "file")]
        data: Option<String>,
        /// Path to a JSON payload file.
        #[arg(long)]
        file: Option<PathBuf>,
        /// Optional items-per-commit chunk size.
        #[arg(long)]
        chunk_size: Option<u64>,
    },
    /// Run a bounded breadth-first traversal.
    Bfs {
        /// Graph name.
        graph: String,
        /// Start node id.
        start: String,
        /// Optional depth bound (default 100).
        #[arg(long)]
        max_depth: Option<u64>,
        /// Optional visited-node bound (default 10000).
        #[arg(long)]
        max_nodes: Option<u64>,
        /// Optional edge-type restriction (repeatable).
        #[arg(long = "edge-type")]
        edge_types: Vec<String>,
        /// Traversal direction.
        #[arg(long, value_enum, default_value_t = CliGraphDirection::Outgoing)]
        direction: CliGraphDirection,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
}

/// Graph ontology command wrapper.
#[derive(Debug, Args)]
pub(crate) struct GraphOntologyArgs {
    /// Ontology command.
    #[command(subcommand)]
    pub(crate) command: GraphOntologyCommand,
}

/// Graph ontology commands.
#[derive(Debug, Subcommand)]
pub(crate) enum GraphOntologyCommand {
    /// Define (or, while draft, redefine) an object type.
    DefineObjectType {
        /// Graph name.
        graph: String,
        /// Object type name.
        name: String,
        /// Properties JSON, e.g. `{"prop": {"value_type": "string", "required": true}}`.
        #[arg(long, conflicts_with = "properties_file")]
        properties: Option<String>,
        /// Read properties JSON from a file.
        #[arg(long)]
        properties_file: Option<PathBuf>,
    },
    /// Define (or, while draft, redefine) a link type.
    DefineLinkType {
        /// Graph name.
        graph: String,
        /// Link type name.
        name: String,
        /// Declared source object type.
        source: String,
        /// Declared target object type.
        target: String,
        /// Optional cardinality hint (e.g. one-to-many).
        #[arg(long)]
        cardinality: Option<String>,
        /// Properties JSON, e.g. `{"prop": {"value_type": "string", "required": true}}`.
        #[arg(long, conflicts_with = "properties_file")]
        properties: Option<String>,
        /// Read properties JSON from a file.
        #[arg(long)]
        properties_file: Option<PathBuf>,
    },
    /// Delete a draft object type.
    DeleteObjectType {
        /// Graph name.
        graph: String,
        /// Object type name.
        name: String,
    },
    /// Delete a draft link type.
    DeleteLinkType {
        /// Graph name.
        graph: String,
        /// Link type name.
        name: String,
    },
    /// Freeze the ontology; writes then validate against it.
    Freeze {
        /// Graph name.
        graph: String,
    },
    /// Read the ontology (status plus every declared type).
    Get {
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
    /// Read the ontology with per-type usage counts.
    Summary {
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline: the
        /// `timestamp` from `history` output, not the `version`. This is a
        /// per-commit counter, never a calendar date — to read as of a real
        /// time, use `--as-of-time`.
        #[arg(long)]
        as_of: Option<u64>,
        /// Read as of a real time: a date (`2026-09-05`), a date and time
        /// (`2026-09-05 15:00`), an offset-bearing timestamp
        /// (`2026-09-05T15:00:00Z`), or raw epoch microseconds. A time without
        /// an offset is read in local time. Resolves to the commit at or
        /// before that moment. Cannot be combined with `--as-of`.
        #[arg(long, value_name = "TIME")]
        as_of_time: Option<String>,
    },
}

/// Graph direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliGraphDirection {
    /// Outgoing edges.
    Outgoing,
    /// Incoming edges.
    Incoming,
    /// Incoming and outgoing edges.
    Both,
}

impl From<CliGraphDirection> for strata_executor::GraphDirection {
    fn from(value: CliGraphDirection) -> Self {
        match value {
            CliGraphDirection::Outgoing => Self::Outgoing,
            CliGraphDirection::Incoming => Self::Incoming,
            CliGraphDirection::Both => Self::Both,
        }
    }
}

/// Arrow command wrapper.
#[derive(Debug, Args)]
pub(crate) struct ArrowArgs {
    /// Arrow command.
    #[command(subcommand)]
    pub(crate) command: ArrowCommand,
}

/// Arrow commands.
#[derive(Debug, Subcommand)]
pub(crate) enum ArrowCommand {
    /// Import an Arrow-compatible file.
    Import {
        /// Input file path.
        file_path: String,
        /// Optional input format.
        #[arg(long, value_enum, id = "arrow_import_format")]
        format: Option<CliArrowFormat>,
        /// Import target.
        #[arg(long, value_enum)]
        target: CliArrowImportTarget,
        /// Optional key column override.
        #[arg(long)]
        key_column: Option<String>,
        /// Optional value/document/embedding column override.
        #[arg(long)]
        value_column: Option<String>,
        /// Target vector collection for vector imports.
        #[arg(long)]
        collection: Option<String>,
        /// Target graph for graph imports.
        #[arg(long)]
        graph: Option<String>,
    },
    /// Export a primitive to an Arrow-compatible file.
    Export {
        /// Export primitive.
        #[arg(long, value_enum)]
        primitive: CliArrowExportPrimitive,
        /// Output format.
        #[arg(long, value_enum, id = "arrow_export_format")]
        format: CliArrowFormat,
        /// Output file path. Graph exports use this as a stem for node and edge files.
        path: String,
        /// Optional key/document/vector/node prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Optional row limit.
        #[arg(long)]
        limit: Option<u64>,
        /// Target vector collection for vector exports.
        #[arg(long)]
        collection: Option<String>,
        /// Target graph for graph exports.
        #[arg(long)]
        graph: Option<String>,
        /// Optional event type filter for event exports.
        #[arg(long)]
        event_type: Option<String>,
    },
}

/// Arrow file format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliArrowFormat {
    /// Parquet.
    Parquet,
    /// CSV.
    Csv,
    /// JSON lines.
    Jsonl,
}

impl From<CliArrowFormat> for strata_executor::ArrowFileFormat {
    fn from(value: CliArrowFormat) -> Self {
        match value {
            CliArrowFormat::Parquet => Self::Parquet,
            CliArrowFormat::Csv => Self::Csv,
            CliArrowFormat::Jsonl => Self::Jsonl,
        }
    }
}

/// Arrow import target.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliArrowImportTarget {
    /// KV primitive.
    Kv,
    /// JSON primitive.
    Json,
    /// Vector primitive.
    Vector,
    /// Graph primitive.
    Graph,
    /// Event primitive.
    Event,
}

impl From<CliArrowImportTarget> for strata_executor::ArrowImportTarget {
    fn from(value: CliArrowImportTarget) -> Self {
        match value {
            CliArrowImportTarget::Kv => Self::Kv,
            CliArrowImportTarget::Json => Self::Json,
            CliArrowImportTarget::Vector => Self::Vector,
            CliArrowImportTarget::Graph => Self::Graph,
            CliArrowImportTarget::Event => Self::Event,
        }
    }
}

/// Arrow export primitive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliArrowExportPrimitive {
    /// KV primitive.
    Kv,
    /// JSON primitive.
    Json,
    /// Event primitive.
    Event,
    /// Vector primitive.
    Vector,
    /// Graph primitive.
    Graph,
}

impl From<CliArrowExportPrimitive> for strata_executor::ArrowExportPrimitive {
    fn from(value: CliArrowExportPrimitive) -> Self {
        match value {
            CliArrowExportPrimitive::Kv => Self::Kv,
            CliArrowExportPrimitive::Json => Self::Json,
            CliArrowExportPrimitive::Event => Self::Event,
            CliArrowExportPrimitive::Vector => Self::Vector,
            CliArrowExportPrimitive::Graph => Self::Graph,
        }
    }
}

/// Raw serialized command wrapper.
#[derive(Debug, Args)]
pub(crate) struct CommandArgs {
    /// Raw command operation.
    #[command(subcommand)]
    pub(crate) command: CommandCommand,
}

/// Raw serialized command operations.
#[derive(Debug, Subcommand)]
pub(crate) enum CommandCommand {
    /// Execute a serialized executor command.
    Run {
        /// Serialized command JSON.
        #[arg(
            long = "command-json",
            id = "run_command_json",
            conflicts_with = "run_command_file"
        )]
        json: Option<String>,
        /// File containing serialized command JSON.
        #[arg(long, id = "run_command_file", conflicts_with = "run_command_json")]
        file: Option<PathBuf>,
    },
    /// Validate and print a serialized executor command without opening a database.
    Print {
        /// Serialized command JSON.
        #[arg(
            long = "command-json",
            id = "print_command_json",
            conflicts_with = "print_command_file"
        )]
        json: Option<String>,
        /// File containing serialized command JSON.
        #[arg(long, id = "print_command_file", conflicts_with = "print_command_json")]
        file: Option<PathBuf>,
    },
}

/// Inference command arguments.
#[cfg(feature = "inference")]
#[derive(Debug, Args)]
pub(crate) struct InferenceArgs {
    /// Inference command.
    #[command(subcommand)]
    pub(crate) command: InferenceCommand,
}

/// Model execution commands.
///
/// Output-format choice for `inference generate`.
#[cfg(feature = "inference")]
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum ResponseFormatArg {
    /// Free-form text.
    Text,
    /// Constrain to a single JSON object.
    JsonObject,
}

/// Instruction-tuned embedder input role for `inference embed`.
#[cfg(feature = "inference")]
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum InputTypeArg {
    /// Embed as a search query.
    Query,
    /// Embed as a document/passage.
    Document,
}

/// Model specs are catalog names (`tinyllama`, `qwen3:1.7b`), catalog
/// name:quant pairs (`tinyllama:q8_0`), local GGUF paths, or provider specs
/// (`anthropic:claude-...`) depending on the enabled features.
#[cfg(feature = "inference")]
#[derive(Debug, Subcommand)]
#[allow(
    clippy::large_enum_variant,
    reason = "clap subcommand enum; the Generate variant carries the full chat-flag set and boxing clap fields is not ergonomic"
)]
pub(crate) enum InferenceCommand {
    /// Model catalog and local model management.
    Models(InferenceModelsArgs),
    /// Show capability facts for one model spec.
    Capability {
        /// Model spec.
        model: String,
    },
    /// Generate text with a model.
    Generate {
        /// Model spec.
        model: String,
        /// Prompt for raw completion. Omit when using --message/--system; if
        /// given alongside chat flags it becomes a trailing user message.
        prompt: Option<String>,
        /// System instruction (chat).
        #[arg(long)]
        system: Option<String>,
        /// Chat message as "role:content" (repeatable; role = system|user|assistant|tool).
        #[arg(long = "message", value_name = "ROLE:CONTENT")]
        messages: Vec<String>,
        /// Chat messages as a JSON array (appended after --system/--message).
        #[arg(long)]
        messages_json: Option<String>,
        /// Full `ChatRequest` JSON body (escape hatch; overrides all other flags).
        #[arg(long)]
        json_body: Option<String>,
        /// Maximum completion tokens.
        #[arg(long)]
        max_tokens: Option<u32>,
        /// Sampling temperature (0.0 = greedy).
        #[arg(long)]
        temperature: Option<f32>,
        /// Top-k sampling cutoff.
        #[arg(long)]
        top_k: Option<u32>,
        /// Nucleus (top-p) sampling cutoff.
        #[arg(long)]
        top_p: Option<f32>,
        /// Min-p sampling cutoff.
        #[arg(long)]
        min_p: Option<f32>,
        /// Repetition penalty.
        #[arg(long)]
        repeat_penalty: Option<f32>,
        /// Frequency penalty.
        #[arg(long)]
        frequency_penalty: Option<f32>,
        /// Presence penalty.
        #[arg(long)]
        presence_penalty: Option<f32>,
        /// Deterministic sampling seed.
        #[arg(long)]
        seed: Option<u64>,
        /// Stop sequence (repeatable).
        #[arg(long = "stop", value_name = "TEXT")]
        stop_sequences: Vec<String>,
        /// Stop token id (repeatable; local models only).
        #[arg(long = "stop-token", value_name = "ID")]
        stop_tokens: Vec<u32>,
        /// Constrain output format.
        #[arg(long, value_enum)]
        response_format: Option<ResponseFormatArg>,
        /// GBNF grammar for constrained generation (local models).
        #[arg(long)]
        grammar: Option<String>,
        /// Context window size (local load param).
        #[arg(long)]
        n_ctx: Option<u32>,
        /// GPU layers to offload; -1 = all (local load param).
        #[arg(long)]
        n_gpu_layers: Option<i32>,
        /// Named chat template override, e.g. chatml/llama3/gemma (local).
        #[arg(long)]
        chat_format: Option<String>,
        /// Tools (functions) as a JSON array the model may call.
        #[arg(long)]
        tools_json: Option<String>,
        /// Tool choice: `auto` | `none` | `required` | a function name.
        #[arg(long)]
        tool_choice: Option<String>,
        /// JSON Schema for structured output (sets `response_format` to
        /// `json_schema`; overrides `--response-format`).
        #[arg(long)]
        response_schema: Option<String>,
        /// Name for the --response-schema output (default: "response").
        #[arg(long)]
        response_schema_name: Option<String>,
        /// Return per-token log-probabilities.
        #[arg(long)]
        logprobs: bool,
        /// Number of top alternatives per token (implies --logprobs).
        #[arg(long)]
        top_logprobs: Option<u32>,
    },
    /// Tokenize text with a local model.
    Tokenize {
        /// Model spec.
        model: String,
        /// Text to tokenize.
        text: String,
        /// Add the model's special tokens.
        #[arg(long)]
        special: bool,
    },
    /// Detokenize token ids with a local model.
    Detokenize {
        /// Model spec.
        model: String,
        /// Token ids.
        #[arg(required = true)]
        ids: Vec<u32>,
    },
    /// Embed one or more texts.
    Embed {
        /// Model spec.
        model: String,
        /// Text(s) to embed.
        #[arg(required = true)]
        inputs: Vec<String>,
        /// Truncate embeddings to N dimensions (matryoshka).
        #[arg(long)]
        dimensions: Option<u32>,
        /// L2-normalize the output vectors.
        #[arg(long)]
        normalize: bool,
        /// Instruction-tuned embedder role.
        #[arg(long, value_enum)]
        input_type: Option<InputTypeArg>,
    },
    /// Rank passages against a query.
    Rank {
        /// Model spec.
        model: String,
        /// Query text.
        query: String,
        /// Candidate passages.
        #[arg(required = true)]
        passages: Vec<String>,
    },
    /// Unload one cached model, or all cached models when omitted.
    Unload {
        /// Model spec (omit to unload everything).
        model: Option<String>,
    },
    /// Show runtime model-cache diagnostics.
    CacheStatus,
    /// Show what this build can do: providers, keys, and on-disk models.
    Status,
    /// Add local model execution to this installation.
    InstallLocal,
}

/// Inference model management arguments.
#[cfg(feature = "inference")]
#[derive(Debug, Args)]
pub(crate) struct InferenceModelsArgs {
    /// Models command.
    #[command(subcommand)]
    pub(crate) command: InferenceModelsCommand,
}

/// Inference model management commands.
#[cfg(feature = "inference")]
#[derive(Debug, Subcommand)]
pub(crate) enum InferenceModelsCommand {
    /// List catalog models.
    List,
    /// List locally available models.
    Local,
    /// Download a model into the local model directory.
    ///
    /// Honors `STRATA_MODELS_DIR`, `STRATA_HF_ENDPOINT`, and
    /// `STRATA_HF_TOKEN` (or `HF_TOKEN`) for gated repositories.
    Pull {
        /// Model spec or catalog name.
        model: String,
    },
}

#[cfg(test)]
pub(crate) mod tests {
    use clap::CommandFactory;
    #[cfg(feature = "inference")]
    use clap::Parser;

    /// Collects every leaf verb path (a subcommand with no further
    /// subcommands) from the clap tree, e.g. `"kv get"`, `"config set"`.
    fn leaf_verbs() -> Vec<String> {
        fn walk(command: &clap::Command, prefix: &str, out: &mut Vec<String>) {
            let mut subs = command.get_subcommands().peekable();
            if subs.peek().is_none() {
                if !prefix.is_empty() {
                    out.push(prefix.to_owned());
                }
                return;
            }
            for sub in subs {
                let path = if prefix.is_empty() {
                    sub.get_name().to_owned()
                } else {
                    format!("{prefix} {}", sub.get_name())
                };
                walk(sub, &path, out);
            }
        }
        let mut out = Vec::new();
        walk(&super::Cli::command(), "", &mut out);
        out.sort();
        out.dedup();
        out
    }

    /// Every leaf verb the `strata` clap tree exposes. This is the mechanical,
    /// executable inventory that replaced the hand-maintained (and drifted)
    /// `docs/architecture/cli-command-coverage.md`: the guard below fails when a
    /// verb is added or removed without updating this list, so the CLI surface
    /// can never silently drift again.
    const EXPECTED_VERBS: &[&str] = &[
        "agents commands",
        "agents errors",
        "agents guide",
        "agents init",
        "agents skill",
        "arrow export",
        "arrow import",
        "begin",
        "branch create",
        "branch delete",
        "branch diff",
        "branch fork",
        "branch get",
        "branch list",
        "branch merge",
        "branch note",
        "branch preview",
        "branch tag",
        "changelog",
        "clone",
        "command print",
        "command run",
        "commit",
        "compact",
        "config get",
        "config get-key",
        "config path",
        "config set",
        "config show",
        "config unset",
        "describe",
        "doctor",
        "down",
        "event append",
        "event by-type",
        "event count",
        "event exists",
        "event get",
        "event list",
        "event range",
        "event range-time",
        "event types",
        "event verify-chain",
        "flush",
        "graph add-edge",
        "graph add-node",
        "graph bfs",
        "graph bulk-insert",
        "graph cdlp",
        "graph create",
        "graph delete",
        "graph get-edge",
        "graph get-node",
        "graph lcc",
        "graph list",
        "graph list-nodes",
        "graph meta",
        "graph neighbors",
        "graph nodes-by-type",
        "graph ontology define-link-type",
        "graph ontology define-object-type",
        "graph ontology delete-link-type",
        "graph ontology delete-object-type",
        "graph ontology freeze",
        "graph ontology get",
        "graph ontology summary",
        "graph pagerank",
        "graph remove-edge",
        "graph remove-node",
        "graph sample",
        "graph sssp",
        "graph wcc",
        "health",
        "hub get-dataset",
        "hub info",
        "hub list-datasets",
        "hub list-refs",
        "hub list-yanked",
        "inference cache-status",
        "inference capability",
        "inference detokenize",
        "inference embed",
        "inference generate",
        "inference install-local",
        "inference models list",
        "inference models local",
        "inference models pull",
        "inference rank",
        "inference status",
        "inference tokenize",
        "inference unload",
        "info",
        "init",
        "ipc status",
        "ipc stop",
        "json count",
        "json delete",
        "json exists",
        "json get",
        "json history",
        "json index create",
        "json index drop",
        "json index list",
        "json list",
        "json sample",
        "json scan",
        "json set",
        "kv count",
        "kv delete",
        "kv exists",
        "kv get",
        "kv history",
        "kv list",
        "kv put",
        "kv sample",
        "kv scan",
        "mcp serve",
        "metrics",
        "ping",
        "recipe",
        "remote",
        "rollback",
        "search",
        "space create",
        "space delete",
        "space exists",
        "space list",
        "start",
        "stop",
        "txn",
        "uninstall",
        "up",
        "update",
        "vector collection create",
        "vector collection delete",
        "vector collection list",
        "vector collection set-embedding-model",
        "vector collection stats",
        "vector count",
        "vector delete",
        "vector delete-all",
        "vector delete-by-filter",
        "vector exists",
        "vector get",
        "vector history",
        "vector keys",
        "vector query",
        "vector sample",
        "vector scan",
        "vector update-metadata",
        "vector upsert",
    ];

    #[test]
    fn clap_verbs_match_the_enumerated_inventory() {
        let actual = leaf_verbs();
        let expected: Vec<String> = EXPECTED_VERBS.iter().map(|v| (*v).to_owned()).collect();
        assert_eq!(actual, expected, "\nACTUAL VERBS:\n{}", actual.join("\n"));
    }

    // ---- Reader-facing surfaces (#3238) ------------------------------------
    //
    // The README (packaged into every release tarball) and the inference guides
    // are read by coding agents that run whatever command the page prints
    // (`docs/design/inference-developer-experience.md` §3b). Two guards keep
    // those pages honest: every fenced `strata …` example must parse under the
    // real clap tree, and no page may send the reader to a Rust toolchain —
    // the lean binary's remedy is `strata inference install-local`. The runtime
    // half (a parsed verb that still refuses to run bare) is #3233's.

    /// Workspace-relative surfaces an agent copies verbatim. A directory is
    /// walked recursively for `.md` files.
    const READER_SURFACES: &[&str] = &["README.md", "docs/inference"];

    /// The inference design documents: every `.md` under this directory whose
    /// path below it starts with `inference`. An engineer reads these, not an
    /// agent, so they are not reader surfaces — they may discuss a cargo
    /// feature — but a command they sketch must still be one the tree has:
    /// a verb invented in a design is how `strata models pull` reached a
    /// product document (#3261).
    #[cfg(feature = "inference")]
    const DESIGN_SURFACES_DIR: &str = "docs/design";
    #[cfg(feature = "inference")]
    const DESIGN_SURFACES_PREFIX: &str = "inference";

    fn workspace_root() -> std::path::PathBuf {
        // CARGO_MANIFEST_DIR = crates/cli.
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root resolves")
    }

    fn reader_surfaces() -> Vec<(std::path::PathBuf, String)> {
        let root = workspace_root();
        let mut out = Vec::new();
        for surface in READER_SURFACES {
            let path = root.join(surface);
            if path.is_dir() {
                crate::arg_spec::tests::markdown_files(&path, &mut out);
            } else {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
                out.push((path, text));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(
            !out.is_empty(),
            "no reader-facing markdown found under {READER_SURFACES:?}"
        );
        out
    }

    #[cfg(feature = "inference")]
    fn design_surfaces() -> Vec<(std::path::PathBuf, String)> {
        let dir = workspace_root().join(DESIGN_SURFACES_DIR);
        let mut all = Vec::new();
        crate::arg_spec::tests::markdown_files(&dir, &mut all);
        let mut out: Vec<_> = all
            .into_iter()
            .filter(|(path, _)| {
                path.strip_prefix(&dir)
                    .expect("walked below the design dir")
                    .to_str()
                    .is_some_and(|below| below.starts_with(DESIGN_SURFACES_PREFIX))
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(
            !out.is_empty(),
            "no `{DESIGN_SURFACES_PREFIX}*` design documents found under {DESIGN_SURFACES_DIR}"
        );
        out
    }

    /// Whether a fence's info string marks a block an agent would paste into a
    /// shell: untagged, or tagged with a shell name. `rust`, `json`, `toml`,
    /// `text` and the like are not commands.
    fn is_shell_fence(info: &str) -> bool {
        matches!(
            info.split_whitespace().next().unwrap_or(""),
            "" | "bash" | "sh" | "shell" | "console" | "zsh"
        )
    }

    /// Every logical line inside a shell fence, with the 1-based number of the
    /// physical line it starts on. A leading `$ ` prompt is dropped and, as in
    /// bash, a trailing `\` is removed and the next physical line appended
    /// with nothing in between. A continuation never crosses a fence: one
    /// left dangling at the fence (or the end of the text) is flushed as it
    /// stands, so no line goes unchecked.
    fn fenced_shell_lines(markdown: &str) -> Vec<(usize, String)> {
        let mut fence: Option<bool> = None; // Some(is_shell) while inside a fence.
        let mut pending: Option<(usize, String)> = None;
        let mut out = Vec::new();
        for (index, raw) in markdown.lines().enumerate() {
            let trimmed = raw.trim_start();
            if let Some(info) = trimmed.strip_prefix("```") {
                fence = if fence.is_some() {
                    None
                } else {
                    Some(is_shell_fence(info))
                };
                out.extend(pending.take());
                continue;
            }
            if fence != Some(true) {
                continue;
            }
            let (line_no, joined) = if let Some((line_no, head)) = pending.take() {
                (line_no, head + raw)
            } else {
                (
                    index + 1,
                    trimmed.strip_prefix("$ ").unwrap_or(trimmed).to_owned(),
                )
            };
            if let Some(head) = joined.strip_suffix('\\') {
                pending = Some((line_no, head.to_owned()));
            } else {
                out.push((line_no, joined));
            }
        }
        out.extend(pending);
        out
    }

    /// Whether a whole shell word separates commands: `|` `||` `;` `&` `&&`,
    /// or a redirection (`>`, `>out`, `2>&1`, `<in`). Words are `shlex`'s,
    /// so an unspaced `ping;` or `k>out` stays one word (bash would split
    /// it) and a quoted `"|"` argument is indistinguishable from the
    /// operator; the reader surfaces do neither. A `<name>` placeholder
    /// (`<model>`, `<provider>.api_key`) is an argument the reader fills in,
    /// not a redirection, and is kept.
    fn is_shell_operator(word: &str) -> bool {
        !is_placeholder(word)
            && word
                .trim_start_matches(|c: char| c.is_ascii_digit())
                .starts_with(['|', ';', '&', '<', '>'])
    }

    /// Whether a shell word opens with an angle-bracketed placeholder name.
    fn is_placeholder(word: &str) -> bool {
        word.strip_prefix('<')
            .and_then(|rest| rest.split_once('>'))
            .is_some_and(|(name, _)| {
                !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
            })
    }

    /// Whether a shell word is a `NAME=value` environment prefix.
    fn is_env_assignment(word: &str) -> bool {
        let Some((name, _)) = word.split_once('=') else {
            return false;
        };
        let mut chars = name.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// The argv of every `strata` command on one shell line, in order: the
    /// line is split like a shell (`shlex`: quotes, escapes, a `#` comment),
    /// cut at each operator, and each command's leading `NAME=value` prefixes
    /// are skipped. `None` means the line does not tokenize (an unbalanced
    /// quote) — an agent pasting it would hit the same error.
    fn strata_invocations(line: &str) -> Option<Vec<Vec<String>>> {
        let words = shlex::split(line)?;
        Some(
            words
                .split(|word| is_shell_operator(word))
                .map(|command| {
                    command
                        .iter()
                        .skip_while(|word| is_env_assignment(word))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .filter(|argv| argv.first().is_some_and(|head| head == "strata"))
                .collect(),
        )
    }

    #[test]
    fn fenced_shell_lines_harvest_shell_fences_only() {
        let markdown = "\
prose `strata kv get` mention
```bash
$ strata --cache ping
export KEY=value
strata config path   # comment
```
strata outside a fence
```rust
strata::not_a_command();
```
```
strata init
strata ./mydb kv put user:ada \\
    '{\"role\":\"engineer\"}' \\
  --branch agent-a
```
";
        assert_eq!(
            fenced_shell_lines(markdown),
            vec![
                (3, "strata --cache ping".to_owned()),
                (4, "export KEY=value".to_owned()),
                (5, "strata config path   # comment".to_owned()),
                (12, "strata init".to_owned()),
                (
                    13,
                    r#"strata ./mydb kv put user:ada     '{"role":"engineer"}'   --branch agent-a"#
                        .to_owned()
                ),
            ]
        );
        // Tagged shell fences are harvested; other languages are not.
        for tag in ["sh", "shell", "console", "zsh", "bash filename=x"] {
            assert_eq!(
                fenced_shell_lines(&format!("```{tag}\nstrata ping\n```\n")),
                vec![(2, "strata ping".to_owned())],
                "fence tag {tag:?}"
            );
        }
        for tag in ["json", "toml", "text", "python"] {
            assert_eq!(
                fenced_shell_lines(&format!("```{tag}\nstrata ping\n```\n")),
                Vec::<(usize, String)>::new(),
                "fence tag {tag:?}"
            );
        }
        // A continuation joins with nothing added, as bash does, and never
        // leaks across a fence boundary: one dangling at the fence, or at the
        // end of the text, is flushed as it stands rather than dropped.
        assert_eq!(
            fenced_shell_lines("```\nstrata ping\\\n--json\n```\n"),
            vec![(2, "strata ping--json".to_owned())]
        );
        assert_eq!(
            fenced_shell_lines("```\nstrata ping \\\n```\n```\nstrata init\n```\n"),
            vec![
                (2, "strata ping ".to_owned()),
                (5, "strata init".to_owned())
            ]
        );
        assert_eq!(
            fenced_shell_lines("```\nstrata ping \\"),
            vec![(2, "strata ping ".to_owned())]
        );
        assert_eq!(fenced_shell_lines(""), Vec::<(usize, String)>::new());
    }

    #[test]
    fn shell_operator_and_env_assignment_words() {
        for word in [
            "|",
            "||",
            ";",
            "&",
            "&&",
            ">",
            ">out.json",
            "2>&1",
            "2>/dev/null",
            "<in",
        ] {
            assert!(is_shell_operator(word), "{word:?} is an operator");
        }
        for word in [
            "strata",
            "a|b",
            "k>out",
            "5",
            "-k",
            "",
            "--as-of=1",
            // Placeholders a reader fills in, not redirections.
            "<model>",
            "<provider>.api_key",
            "<KEY>",
            "<target-triple>",
        ] {
            assert!(!is_shell_operator(word), "{word:?} is not an operator");
        }
        for word in ["<in", "<>", "< model>", "<a b>", "<>x"] {
            assert!(!is_placeholder(word), "{word:?} is not a placeholder");
        }
        for word in ["KEY=value", "OPENAI_API_KEY=sk-...", "_x=", "a1="] {
            assert!(is_env_assignment(word), "{word:?} is an assignment");
        }
        for word in [
            "strata",
            "--as-of=1",
            "=v",
            "1a=b",
            "a-b=c",
            "openai.api_key",
            "",
        ] {
            assert!(!is_env_assignment(word), "{word:?} is not an assignment");
        }
    }

    #[test]
    fn strata_invocations_split_like_a_shell() {
        let argv = |line: &str| strata_invocations(line).expect("tokenizes");
        assert_eq!(
            argv(r#"strata ./mydb kv put user:ada '{"role":"engineer"}'"#),
            [[
                "strata",
                "./mydb",
                "kv",
                "put",
                "user:ada",
                r#"{"role":"engineer"}"#
            ]]
        );
        assert_eq!(
            argv(r#"strata --cache inference generate openai:gpt-4o-mini "Hello there""#),
            [[
                "strata",
                "--cache",
                "inference",
                "generate",
                "openai:gpt-4o-mini",
                "Hello there"
            ]]
        );
        assert_eq!(
            argv("strata config path   # where the config file lives"),
            [["strata", "config", "path"]]
        );
        assert_eq!(
            argv(r#"strata ./mydb json set config '$.model' '"claude"'"#),
            [[
                "strata",
                "./mydb",
                "json",
                "set",
                "config",
                "$.model",
                r#""claude""#
            ]]
        );
        // A `#` inside a word or a quote is data, not a comment.
        assert_eq!(
            argv("strata kv put k '#1' v#2"),
            [["strata", "kv", "put", "k", "#1", "v#2"]]
        );
        // Operators end a command; every `strata` on the line is returned and
        // the shell's other commands are not.
        assert_eq!(
            argv("strata ./mydb kv get k | jq .value"),
            [["strata", "./mydb", "kv", "get", "k"]]
        );
        assert_eq!(
            argv("strata ./mydb kv get k > out.json 2>&1"),
            [["strata", "./mydb", "kv", "get", "k"]]
        );
        assert_eq!(
            argv("cd mydb && strata ping ; strata info || echo down"),
            [vec!["strata", "ping"], vec!["strata", "info"]]
        );
        assert_eq!(
            argv(r#"strata kv put k "a|b" 'c>d'"#),
            [["strata", "kv", "put", "k", "a|b", "c>d"]]
        );
        // A placeholder is an argument, not a redirection; `<in` still is one.
        assert_eq!(
            argv("strata inference models pull <model> < in"),
            [["strata", "inference", "models", "pull", "<model>"]]
        );
        // A leading environment prefix is the shell's, not the command's.
        assert_eq!(
            argv("OPENAI_API_KEY=sk-... STRATA_LOG=debug strata ping"),
            [["strata", "ping"]]
        );
        assert_eq!(
            argv("export OPENAI_API_KEY=sk-..."),
            Vec::<Vec<String>>::new()
        );
        assert_eq!(
            argv("curl -fsSL https://stratadb.org/install.sh | sh"),
            Vec::<Vec<String>>::new()
        );
        assert_eq!(argv("   "), Vec::<Vec<String>>::new());
        // An unbalanced quote does not tokenize.
        assert_eq!(strata_invocations(r#"strata kv put k "open"#), None);
    }

    /// Whether an argv parses under the real clap tree. Help and version are
    /// what those verbs print, so they count as parsed; anything else is the
    /// error kind clap would show the reader.
    #[cfg(feature = "inference")]
    pub(crate) fn clap_parse_failure(argv: &[String]) -> Option<clap::error::ErrorKind> {
        use clap::error::ErrorKind;
        match super::Cli::try_parse_from(argv) {
            Ok(_) => None,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::DisplayHelp
                        | ErrorKind::DisplayVersion
                        | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
                ) =>
            {
                None
            }
            Err(err) => Some(err.kind()),
        }
    }

    /// The `strata …` commands prose names, as a reader would copy them: the
    /// text of every backtick span that starts with `strata`, and every line
    /// whose first word is `strata` (a refusal prints the command to run on
    /// its own indented line). A line that is itself a backtick span is
    /// returned once.
    pub(crate) fn named_commands(text: &str) -> Vec<String> {
        let mut out = Vec::new();
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.split_whitespace().next() == Some("strata") {
                out.push(trimmed.to_owned());
                continue;
            }
            let mut rest = trimmed;
            while let Some((_, after)) = rest.split_once('`') {
                let Some((span, tail)) = after.split_once('`') else {
                    break;
                };
                if span.split_whitespace().next() == Some("strata") {
                    out.push(span.to_owned());
                }
                rest = tail;
            }
        }
        out
    }

    /// Every `strata …` command in `commands` that does not parse, rendered
    /// as `where: command\n    kind` lines for an assertion message.
    #[cfg(feature = "inference")]
    pub(crate) fn commands_that_do_not_parse(
        commands: impl IntoIterator<Item = (String, String)>,
    ) -> Vec<String> {
        commands
            .into_iter()
            .filter_map(|(source, command)| {
                let Some(argv) = strata_invocations(&command) else {
                    return Some(format!(
                        "{source}: {command}\n    does not tokenize (unbalanced quote)"
                    ));
                };
                argv.into_iter().find_map(|argv| {
                    clap_parse_failure(&argv)
                        .map(|kind| format!("{source}: {}\n    {kind:?}", argv.join(" ")))
                })
            })
            .collect()
    }

    #[test]
    fn named_commands_are_backtick_spans_and_bare_lines() {
        let text = "\
Run `strata inference models pull <model>`, then retry.
  strata inference models pull miniLM
Check `strata inference models list` or `strata config path`; not `cargo build`.
strata::not_a_command(); `strata` alone
`unbalanced strata ping
";
        assert_eq!(
            named_commands(text),
            [
                "strata inference models pull <model>",
                "strata inference models pull miniLM",
                "strata inference models list",
                "strata config path",
                "strata",
            ]
        );
        assert_eq!(named_commands(""), Vec::<String>::new());
    }

    #[cfg(feature = "inference")]
    #[test]
    fn commands_that_do_not_parse_name_the_failing_command() {
        let source = |c: &str| ("here".to_owned(), c.to_owned());
        assert_eq!(
            commands_that_do_not_parse([
                source("strata inference models list"),
                source("strata --help"),
                source("strata ./mydb kv get k | jq ."),
            ]),
            Vec::<String>::new()
        );
        let failures = commands_that_do_not_parse([
            source("strata models pull miniLM"),
            source(r#"strata kv put k "open"#),
        ]);
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(
            failures[0].starts_with("here: strata models pull miniLM\n"),
            "{failures:?}"
        );
        assert!(
            failures[1].ends_with("does not tokenize (unbalanced quote)"),
            "{failures:?}"
        );
    }

    /// An agent copies these lines verbatim, so every `strata` command on a
    /// reader surface must parse under the real clap tree, and every shell
    /// line must tokenize. A design document's fenced sketches are held to
    /// the same tree, but only the lines that open with `strata`: a sketch
    /// draws flow lines around its commands, and those are not shell (#3261).
    /// Gated on `inference` because it is the only feature that shapes the
    /// clap tree, and the docs describe the shipped binary, which carries it.
    #[cfg(feature = "inference")]
    #[test]
    fn reader_surfaces_name_only_commands_the_clap_tree_parses() {
        let mut commands = Vec::new();
        for (path, text) in reader_surfaces() {
            for (line_no, line) in fenced_shell_lines(&text) {
                commands.push((format!("{}:{line_no}", path.display()), line));
            }
        }
        let mut sketches = 0;
        for (path, text) in design_surfaces() {
            for (line_no, line) in fenced_shell_lines(&text) {
                if line.split_whitespace().next() == Some("strata") {
                    sketches += 1;
                    commands.push((format!("{}:{line_no}", path.display()), line));
                }
            }
        }
        assert!(
            commands.len() > sketches,
            "no fenced shell lines found on the reader surfaces"
        );
        assert!(
            sketches > 0,
            "no fenced `strata …` sketches in the design documents"
        );
        let failures = commands_that_do_not_parse(commands);
        assert!(
            failures.is_empty(),
            "fenced examples an agent would copy do not parse:\n{}",
            failures.join("\n")
        );
    }

    /// A registry fix or message is what the reader sees when a command is
    /// refused, so every `strata …` it names must parse under the real clap
    /// tree — `strata models list` in a hint sent readers to a verb that does
    /// not exist (#3256). Placeholders like `<model>` are one word to the
    /// shell and any word to a positional, so they parse as the reader's
    /// substitution would.
    #[cfg(feature = "inference")]
    #[test]
    fn registry_hints_name_only_commands_the_clap_tree_parses() {
        let mut commands = Vec::new();
        for entry in strata_executor::public_error_code_entries() {
            for (field, text) in [
                ("suggested_fix", entry.suggested_fix),
                ("message_template", entry.message_template),
            ] {
                for command in named_commands(text) {
                    commands.push((format!("{} {field}", entry.code), command));
                }
            }
        }
        assert!(
            !commands.is_empty(),
            "no registry hint or message names a `strata …` command"
        );
        let failures = commands_that_do_not_parse(commands);
        assert!(
            failures.is_empty(),
            "registry text names commands the clap tree does not parse:\n{}",
            failures.join("\n")
        );
    }

    /// A reader-facing surface never names a cargo feature flag: the remedy
    /// for a lean binary is `strata inference install-local`, and
    /// `--features` is the crate's business, not the reader's (#3238).
    #[test]
    fn reader_surfaces_never_name_a_cargo_feature_flag() {
        let mut offenders = Vec::new();
        for (path, text) in reader_surfaces() {
            for (index, line) in text.lines().enumerate() {
                if line.contains("--features") {
                    offenders.push(format!("{}:{}: {}", path.display(), index + 1, line.trim()));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "reader-facing surfaces must name `strata inference install-local`, \
             not a cargo feature flag:\n{}",
            offenders.join("\n")
        );
    }
}
