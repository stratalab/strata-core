//! Clap command tree definition.
//!
//! Builds the full `clap::Command` tree used by both shell mode (directly)
//! and REPL mode (via `try_get_matches_from`).

use clap::{Arg, Command};

/// Build the complete CLI command tree.
///
/// This is shared between shell mode and REPL mode.
pub fn build_cli() -> Command {
    Command::new("strata")
        .about("Redis-inspired CLI for the Strata database")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(false)
        .arg(
            Arg::new("db")
                .long("db")
                .value_name("PATH")
                .help("Database path (default: .strata)")
                .global(true),
        )
        .arg(
            Arg::new("cache")
                .long("cache")
                .help("Ephemeral in-memory database, no disk")
                .action(clap::ArgAction::SetTrue)
                .conflicts_with("db")
                .global(true),
        )
        .arg(
            Arg::new("branch")
                .long("branch")
                .short('b')
                .value_name("NAME")
                .help("Initial branch (default: default)")
                .global(true),
        )
        .arg(
            Arg::new("space")
                .long("space")
                .short('s')
                .value_name("NAME")
                .help("Initial space (default: default)")
                .global(true),
        )
        .arg(
            Arg::new("json")
                .long("json")
                .short('j')
                .help("JSON output mode")
                .action(clap::ArgAction::SetTrue)
                .conflicts_with("raw")
                .global(true),
        )
        .arg(
            Arg::new("raw")
                .long("raw")
                .short('r')
                .help("Raw output mode (no type prefixes, no quotes)")
                .action(clap::ArgAction::SetTrue)
                .global(true),
        )
        .arg(
            Arg::new("read-only")
                .long("read-only")
                .help("Open database in read-only mode")
                .action(clap::ArgAction::SetTrue)
                .global(true),
        )
        .arg(
            Arg::new("follower")
                .long("follower")
                .help("Open as read-only follower (no lock, can read while primary is running)")
                .action(clap::ArgAction::SetTrue)
                .conflicts_with("cache")
                .global(true),
        )
        .subcommand(build_kv())
        .subcommand(build_json())
        .subcommand(build_event())
        .subcommand(build_state())
        .subcommand(build_vector())
        .subcommand(build_branch())
        .subcommand(build_space())
        .subcommand(build_txn_begin())
        .subcommand(build_txn_commit())
        .subcommand(build_txn_rollback())
        .subcommand(build_txn())
        .subcommand(build_ping())
        .subcommand(build_info())
        .subcommand(build_flush())
        .subcommand(build_compact())
        .subcommand(build_describe())
        .subcommand(build_search())
        .subcommand(build_setup())
        .subcommand(build_uninstall())
        .subcommand(build_configure_model())
        .subcommand(build_embed())
        .subcommand(build_models())
        .subcommand(build_generate())
        .subcommand(build_tokenize())
        .subcommand(build_detokenize())
        .subcommand(build_graph())
        .subcommand(build_config())
}

/// Build a command tree for REPL mode (no global flags).
pub fn build_repl_cmd() -> Command {
    Command::new("repl")
        .multicall(true)
        .subcommand_required(true)
        .subcommand(build_kv())
        .subcommand(build_json())
        .subcommand(build_event())
        .subcommand(build_state())
        .subcommand(build_vector())
        .subcommand(build_branch())
        .subcommand(build_space())
        .subcommand(build_txn_begin())
        .subcommand(build_txn_commit())
        .subcommand(build_txn_rollback())
        .subcommand(build_txn())
        .subcommand(build_ping())
        .subcommand(build_info())
        .subcommand(build_flush())
        .subcommand(build_compact())
        .subcommand(build_search())
        .subcommand(build_configure_model())
        .subcommand(build_embed())
        .subcommand(build_models())
        .subcommand(build_generate())
        .subcommand(build_tokenize())
        .subcommand(build_detokenize())
        .subcommand(build_graph())
        .subcommand(build_config())
}

// =========================================================================
// KV
// =========================================================================

fn build_kv() -> Command {
    Command::new("kv")
        .about("Key-value operations")
        .subcommand_required(true)
        .subcommand(
            Command::new("put")
                .about("Set one or more key-value pairs")
                .arg(
                    Arg::new("pairs")
                        .num_args(1..)
                        .value_name("KEY VALUE")
                        .help("Key-value pairs (key1 val1 key2 val2 ...)"),
                )
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .value_name("PATH")
                        .help("Read value from file (use with single key, '-' for stdin)"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get one or more values by key")
                .arg(
                    Arg::new("keys")
                        .required(true)
                        .num_args(1..)
                        .value_name("KEY")
                        .help("Key(s) to retrieve"),
                )
                .arg(
                    Arg::new("with-version")
                        .long("with-version")
                        .short('v')
                        .action(clap::ArgAction::SetTrue)
                        .help("Include version and timestamp in output"),
                ),
        )
        .subcommand(
            Command::new("del").about("Delete one or more keys").arg(
                Arg::new("keys")
                    .required(true)
                    .num_args(1..)
                    .value_name("KEY")
                    .help("Key(s) to delete"),
            ),
        )
        .subcommand(
            Command::new("list")
                .about("List keys")
                .arg(
                    Arg::new("prefix")
                        .long("prefix")
                        .short('p')
                        .value_name("PREFIX")
                        .help("Key prefix filter"),
                )
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('n')
                        .value_name("COUNT")
                        .value_parser(clap::value_parser!(u64))
                        .help("Maximum keys to return"),
                )
                .arg(
                    Arg::new("cursor")
                        .long("cursor")
                        .short('c')
                        .value_name("CURSOR")
                        .help("Pagination cursor"),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .short('a')
                        .action(clap::ArgAction::SetTrue)
                        .conflicts_with_all(["limit", "cursor"])
                        .help("Fetch all keys (automatic pagination)"),
                ),
        )
        .subcommand(
            Command::new("history")
                .about("Get version history for a key")
                .arg(Arg::new("key").required(true).help("Key name")),
        )
}

// =========================================================================
// JSON
// =========================================================================

fn build_json() -> Command {
    Command::new("json")
        .about("JSON document operations")
        .subcommand_required(true)
        .subcommand(
            Command::new("set")
                .about("Set a value at a path in a JSON document")
                .arg(Arg::new("key").required(true).help("Document key"))
                .arg(Arg::new("path").required(true).help("JSON path"))
                .arg(
                    Arg::new("value")
                        .required_unless_present("file")
                        .help("JSON value"),
                )
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .value_name("PATH")
                        .help("Read value from JSON file ('-' for stdin)"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get a value from a JSON document")
                .arg(Arg::new("key").required(true).help("Document key"))
                .arg(
                    Arg::new("path")
                        .default_value("$")
                        .help("JSON path (default: $)"),
                )
                .arg(
                    Arg::new("with-version")
                        .long("with-version")
                        .short('v')
                        .action(clap::ArgAction::SetTrue)
                        .help("Include version and timestamp in output"),
                ),
        )
        .subcommand(
            Command::new("del")
                .about("Delete a value at a path")
                .arg(Arg::new("key").required(true).help("Document key"))
                .arg(Arg::new("path").required(true).help("JSON path")),
        )
        .subcommand(
            Command::new("list")
                .about("List JSON documents")
                .arg(
                    Arg::new("prefix")
                        .long("prefix")
                        .short('p')
                        .value_name("PREFIX")
                        .help("Key prefix filter"),
                )
                .arg(
                    Arg::new("cursor")
                        .long("cursor")
                        .short('c')
                        .value_name("CURSOR")
                        .help("Pagination cursor"),
                )
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('n')
                        .value_name("COUNT")
                        .value_parser(clap::value_parser!(u64))
                        .help("Maximum documents to return"),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .short('a')
                        .action(clap::ArgAction::SetTrue)
                        .conflicts_with_all(["limit", "cursor"])
                        .help("Fetch all keys (automatic pagination)"),
                ),
        )
        .subcommand(
            Command::new("history")
                .about("Get version history for a document")
                .arg(Arg::new("key").required(true).help("Document key")),
        )
}

// =========================================================================
// Event
// =========================================================================

fn build_event() -> Command {
    Command::new("event")
        .about("Event log operations")
        .subcommand_required(true)
        .subcommand(
            Command::new("append")
                .about("Append an event")
                .arg(Arg::new("type").required(true).help("Event type"))
                .arg(
                    Arg::new("payload")
                        .required_unless_present("file")
                        .help("JSON payload"),
                )
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .value_name("PATH")
                        .help("Read payload from JSON file ('-' for stdin)"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get event by sequence number")
                .arg(Arg::new("sequence").required(true).help("Sequence number")),
        )
        .subcommand(
            Command::new("list")
                .about("List events by type")
                .arg(Arg::new("type").required(true).help("Event type"))
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('n')
                        .value_name("COUNT")
                        .value_parser(clap::value_parser!(u64))
                        .help("Maximum events to return"),
                )
                .arg(
                    Arg::new("after")
                        .long("after")
                        .value_name("SEQ")
                        .value_parser(clap::value_parser!(u64))
                        .help("After sequence number"),
                ),
        )
        .subcommand(Command::new("len").about("Get total event count"))
}

// =========================================================================
// State
// =========================================================================

fn build_state() -> Command {
    Command::new("state")
        .about("State cell operations")
        .subcommand_required(true)
        .subcommand(
            Command::new("set")
                .about("Set a state cell value")
                .arg(Arg::new("cell").required(true).help("Cell name"))
                .arg(
                    Arg::new("value")
                        .required_unless_present("file")
                        .help("Value"),
                )
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .value_name("PATH")
                        .help("Read value from file ('-' for stdin)"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get a state cell value")
                .arg(Arg::new("cell").required(true).help("Cell name"))
                .arg(
                    Arg::new("with-version")
                        .long("with-version")
                        .short('v')
                        .action(clap::ArgAction::SetTrue)
                        .help("Include version and timestamp in output"),
                ),
        )
        .subcommand(
            Command::new("del")
                .about("Delete a state cell")
                .arg(Arg::new("cell").required(true).help("Cell name")),
        )
        .subcommand(
            Command::new("init")
                .about("Initialize a state cell (only if not exists)")
                .arg(Arg::new("cell").required(true).help("Cell name"))
                .arg(Arg::new("value").required(true).help("Initial value")),
        )
        .subcommand(
            Command::new("cas")
                .about("Compare-and-swap on a state cell")
                .arg(Arg::new("cell").required(true).help("Cell name"))
                .arg(
                    Arg::new("expected")
                        .required(true)
                        .help("Expected version (or 'none' for unset)"),
                )
                .arg(Arg::new("value").required(true).help("New value")),
        )
        .subcommand(
            Command::new("list")
                .about("List state cells")
                .arg(
                    Arg::new("prefix")
                        .long("prefix")
                        .short('p')
                        .value_name("PREFIX")
                        .help("Cell name prefix filter"),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .short('a')
                        .action(clap::ArgAction::SetTrue)
                        .help("Fetch all cells (automatic pagination)"),
                ),
        )
        .subcommand(
            Command::new("history")
                .about("Get version history for a cell")
                .arg(Arg::new("cell").required(true).help("Cell name")),
        )
}

// =========================================================================
// Vector
// =========================================================================

fn build_vector() -> Command {
    Command::new("vector")
        .about("Vector store operations")
        .subcommand_required(true)
        .subcommand(
            Command::new("upsert")
                .about("Insert or update a vector")
                .arg(
                    Arg::new("collection")
                        .required(true)
                        .help("Collection name"),
                )
                .arg(Arg::new("key").required(true).help("Vector key"))
                .arg(
                    Arg::new("vector")
                        .required(true)
                        .help("Vector as JSON array, e.g. [1.0,2.0,3.0]"),
                )
                .arg(
                    Arg::new("metadata")
                        .long("metadata")
                        .value_name("JSON")
                        .help("Metadata as JSON"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get a vector by key")
                .arg(
                    Arg::new("collection")
                        .required(true)
                        .help("Collection name"),
                )
                .arg(Arg::new("key").required(true).help("Vector key")),
        )
        .subcommand(
            Command::new("del")
                .about("Delete a vector")
                .arg(
                    Arg::new("collection")
                        .required(true)
                        .help("Collection name"),
                )
                .arg(Arg::new("key").required(true).help("Vector key")),
        )
        .subcommand(
            Command::new("search")
                .about("Search for similar vectors")
                .arg(
                    Arg::new("collection")
                        .required(true)
                        .help("Collection name"),
                )
                .arg(
                    Arg::new("query")
                        .required(true)
                        .help("Query vector as JSON array"),
                )
                .arg(
                    Arg::new("top-k")
                        .long("top-k")
                        .short('k')
                        .default_value("10")
                        .value_name("COUNT")
                        .value_parser(clap::value_parser!(u64))
                        .help("Number of results to return (default: 10)"),
                )
                .arg(
                    Arg::new("metric")
                        .long("metric")
                        .value_name("METRIC")
                        .help("Distance metric: cosine, euclidean, dotproduct"),
                )
                .arg(
                    Arg::new("filter")
                        .long("filter")
                        .value_name("JSON")
                        .help("Metadata filter as JSON"),
                ),
        )
        .subcommand(
            Command::new("create")
                .about("Create a vector collection")
                .arg(Arg::new("name").required(true).help("Collection name"))
                .arg(
                    Arg::new("dim")
                        .required(true)
                        .value_parser(clap::value_parser!(u64))
                        .help("Vector dimension"),
                )
                .arg(
                    Arg::new("metric")
                        .long("metric")
                        .default_value("cosine")
                        .value_name("METRIC")
                        .help("Distance metric"),
                ),
        )
        .subcommand(
            Command::new("drop")
                .visible_alias("del-collection")
                .about("Delete a vector collection")
                .arg(Arg::new("name").required(true).help("Collection name")),
        )
        .subcommand(Command::new("collections").about("List all vector collections"))
        .subcommand(
            Command::new("stats")
                .about("Get collection statistics")
                .arg(
                    Arg::new("collection")
                        .required(true)
                        .help("Collection name"),
                ),
        )
        .subcommand(
            Command::new("batch-upsert")
                .about("Batch insert/update vectors")
                .arg(
                    Arg::new("collection")
                        .required(true)
                        .help("Collection name"),
                )
                .arg(
                    Arg::new("json")
                        .required(true)
                        .help("JSON array of {key, vector, metadata?}"),
                ),
        )
}

// =========================================================================
// Branch
// =========================================================================

fn build_branch() -> Command {
    Command::new("branch")
        .about("Branch operations")
        .subcommand_required(true)
        .subcommand(
            Command::new("create")
                .about("Create a new branch")
                .arg(Arg::new("name").help("Branch name (auto-generated if omitted)")),
        )
        .subcommand(
            Command::new("info")
                .visible_alias("get")
                .about("Get branch info")
                .arg(Arg::new("name").required(true).help("Branch name")),
        )
        .subcommand(
            Command::new("list").about("List all branches").arg(
                Arg::new("limit")
                    .long("limit")
                    .short('n')
                    .value_name("COUNT")
                    .value_parser(clap::value_parser!(u64))
                    .help("Maximum branches"),
            ),
        )
        .subcommand(
            Command::new("exists")
                .about("Check if a branch exists")
                .arg(Arg::new("name").required(true).help("Branch name")),
        )
        .subcommand(
            Command::new("del")
                .about("Delete a branch")
                .arg(Arg::new("name").required(true).help("Branch name")),
        )
        .subcommand(
            Command::new("fork")
                .about("Fork current branch to a new branch")
                .arg(
                    Arg::new("dest")
                        .required(true)
                        .help("Destination branch name"),
                ),
        )
        .subcommand(
            Command::new("diff")
                .about("Compare two branches")
                .arg(Arg::new("a").required(true).help("Branch A"))
                .arg(Arg::new("b").required(true).help("Branch B")),
        )
        .subcommand(
            Command::new("merge")
                .about("Merge source branch into current branch")
                .arg(Arg::new("source").required(true).help("Source branch"))
                .arg(
                    Arg::new("strategy")
                        .long("strategy")
                        .default_value("lww")
                        .value_name("STRATEGY")
                        .help("Merge strategy: lww or strict"),
                ),
        )
        .subcommand(
            Command::new("export")
                .about("Export a branch to a bundle file")
                .arg(Arg::new("branch").required(true).help("Branch name"))
                .arg(Arg::new("path").required(true).help("Output file path")),
        )
        .subcommand(
            Command::new("import")
                .about("Import a branch from a bundle file")
                .arg(Arg::new("path").required(true).help("Bundle file path")),
        )
        .subcommand(
            Command::new("validate")
                .about("Validate a branch bundle file")
                .arg(Arg::new("path").required(true).help("Bundle file path")),
        )
}

// =========================================================================
// Space
// =========================================================================

fn build_space() -> Command {
    Command::new("space")
        .about("Space operations")
        .subcommand_required(true)
        .subcommand(Command::new("list").about("List all spaces"))
        .subcommand(
            Command::new("create")
                .about("Create a space")
                .arg(Arg::new("name").required(true).help("Space name")),
        )
        .subcommand(
            Command::new("del")
                .about("Delete a space")
                .arg(Arg::new("name").required(true).help("Space name"))
                .arg(
                    Arg::new("force")
                        .long("force")
                        .action(clap::ArgAction::SetTrue)
                        .help("Force delete even if non-empty"),
                ),
        )
        .subcommand(
            Command::new("exists")
                .about("Check if a space exists")
                .arg(Arg::new("name").required(true).help("Space name")),
        )
}

// =========================================================================
// Transaction
// =========================================================================

fn build_txn_begin() -> Command {
    Command::new("begin").about("Begin a new transaction").arg(
        Arg::new("txn-read-only")
            .long("txn-read-only")
            .action(clap::ArgAction::SetTrue)
            .help("Start a read-only transaction"),
    )
}

fn build_txn_commit() -> Command {
    Command::new("commit").about("Commit the current transaction")
}

fn build_txn_rollback() -> Command {
    Command::new("rollback").about("Rollback the current transaction")
}

fn build_txn() -> Command {
    Command::new("txn")
        .about("Transaction info")
        .subcommand_required(true)
        .subcommand(Command::new("info").about("Get current transaction info"))
        .subcommand(Command::new("active").about("Check if a transaction is active"))
}

// =========================================================================
// Database
// =========================================================================

fn build_ping() -> Command {
    Command::new("ping").about("Ping the database")
}

fn build_info() -> Command {
    Command::new("info").about("Get database information")
}

fn build_flush() -> Command {
    Command::new("flush").about("Flush pending writes to disk")
}

fn build_compact() -> Command {
    Command::new("compact").about("Trigger compaction")
}

fn build_describe() -> Command {
    Command::new("describe").about("Show database snapshot for agent introspection")
}

// =========================================================================
// Search
// =========================================================================

fn build_search() -> Command {
    Command::new("search")
        .about("Search across multiple primitives")
        .arg(Arg::new("query").required(true).help("Search query"))
        .arg(
            Arg::new("top-k")
                .long("top-k")
                .short('k')
                .value_name("COUNT")
                .value_parser(clap::value_parser!(u64))
                .help("Number of results to return"),
        )
        .arg(
            Arg::new("primitives")
                .long("primitives")
                .value_name("LIST")
                .help("Comma-separated list of primitives to search"),
        )
        .arg(
            Arg::new("time-start")
                .long("time-start")
                .requires("time-end")
                .value_name("TIMESTAMP")
                .help("Time range start (ISO 8601, e.g. 2026-02-07T00:00:00Z)"),
        )
        .arg(
            Arg::new("time-end")
                .long("time-end")
                .requires("time-start")
                .value_name("TIMESTAMP")
                .help("Time range end (ISO 8601, e.g. 2026-02-09T00:00:00Z)"),
        )
        .arg(
            Arg::new("mode")
                .long("mode")
                .value_name("MODE")
                .help("Search mode: keyword, hybrid (default: hybrid)"),
        )
        .arg(
            Arg::new("expand")
                .long("expand")
                .action(clap::ArgAction::SetTrue)
                .help("Enable query expansion"),
        )
        .arg(
            Arg::new("rerank")
                .long("rerank")
                .action(clap::ArgAction::SetTrue)
                .help("Enable reranking"),
        )
}

// =========================================================================
// Setup
// =========================================================================

fn build_setup() -> Command {
    Command::new("setup").about("Download model files for auto-embedding")
}

fn build_uninstall() -> Command {
    Command::new("uninstall")
        .about("Remove Strata from this system")
        .arg(
            Arg::new("yes")
                .long("yes")
                .short('y')
                .help("Skip confirmation prompt")
                .action(clap::ArgAction::SetTrue),
        )
}

// =========================================================================
// Configure Model
// =========================================================================

fn build_configure_model() -> Command {
    Command::new("configure-model")
        .about("Configure an inference model endpoint for intelligent search")
        .arg(
            Arg::new("endpoint")
                .required(true)
                .help("OpenAI-compatible API endpoint URL"),
        )
        .arg(
            Arg::new("model")
                .required(true)
                .help("Model name (e.g. qwen3:1.7b)"),
        )
        .arg(
            Arg::new("api-key")
                .long("api-key")
                .value_name("TOKEN")
                .help("Bearer token for the endpoint"),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .value_name("MS")
                .value_parser(clap::value_parser!(u64))
                .help("Request timeout in milliseconds (default: 5000)"),
        )
}

// =========================================================================
// Embed
// =========================================================================

fn build_embed() -> Command {
    Command::new("embed").about("Embed text into a vector").arg(
        Arg::new("texts")
            .required(true)
            .num_args(1..)
            .value_name("TEXT")
            .help("Text(s) to embed (single text → embed, multiple → embed-batch)"),
    )
}

// =========================================================================
// Models
// =========================================================================

fn build_models() -> Command {
    Command::new("models")
        .about("Model management")
        .subcommand_required(true)
        .subcommand(Command::new("list").about("List all available models"))
        .subcommand(Command::new("local").about("List locally downloaded models"))
        .subcommand(
            Command::new("pull").about("Download a model").arg(
                Arg::new("name")
                    .required(true)
                    .help("Model name (e.g. miniLM, nomic-embed)"),
            ),
        )
}

// =========================================================================
// Generate
// =========================================================================

fn build_generate() -> Command {
    Command::new("generate")
        .about("Generate text from a prompt using a local model")
        .arg(
            Arg::new("model")
                .required(true)
                .help("Model name (e.g. qwen3:8b)"),
        )
        .arg(Arg::new("prompt").required(true).help("Input prompt text"))
        .arg(
            Arg::new("max-tokens")
                .long("max-tokens")
                .value_name("COUNT")
                .value_parser(clap::value_parser!(usize))
                .help("Maximum tokens to generate (default: 256)"),
        )
        .arg(
            Arg::new("temperature")
                .long("temperature")
                .value_name("TEMP")
                .value_parser(clap::value_parser!(f32))
                .help("Sampling temperature (0.0 = greedy)"),
        )
        .arg(
            Arg::new("top-k")
                .long("top-k")
                .value_name("K")
                .value_parser(clap::value_parser!(usize))
                .help("Top-K sampling (0 = disabled)"),
        )
        .arg(
            Arg::new("top-p")
                .long("top-p")
                .value_name("P")
                .value_parser(clap::value_parser!(f32))
                .help("Top-P nucleus sampling (1.0 = disabled)"),
        )
        .arg(
            Arg::new("seed")
                .long("seed")
                .value_name("SEED")
                .value_parser(clap::value_parser!(u64))
                .help("Random seed for reproducibility"),
        )
        .arg(
            Arg::new("stop")
                .long("stop")
                .num_args(1..)
                .value_name("TEXT")
                .help("Stop sequences (text strings that stop generation)"),
        )
}

// =========================================================================
// Tokenize
// =========================================================================

fn build_tokenize() -> Command {
    Command::new("tokenize")
        .about("Tokenize text into token IDs")
        .arg(Arg::new("model").required(true).help("Model name"))
        .arg(Arg::new("text").required(true).help("Text to tokenize"))
        .arg(
            Arg::new("no-special")
                .long("no-special")
                .action(clap::ArgAction::SetTrue)
                .help("Do not add special tokens (BOS/EOS)"),
        )
}

// =========================================================================
// Detokenize
// =========================================================================

fn build_detokenize() -> Command {
    Command::new("detokenize")
        .about("Decode token IDs back to text")
        .arg(Arg::new("model").required(true).help("Model name"))
        .arg(
            Arg::new("ids")
                .required(true)
                .num_args(1..)
                .help("Token IDs to decode"),
        )
}

// =========================================================================
// Graph
// =========================================================================

fn build_graph() -> Command {
    Command::new("graph")
        .about("Graph operations")
        .subcommand_required(true)
        // Lifecycle
        .subcommand(
            Command::new("create")
                .about("Create a graph")
                .arg(Arg::new("name").required(true).help("Graph name"))
                .arg(
                    Arg::new("cascade-policy")
                        .long("cascade-policy")
                        .value_name("POLICY")
                        .help("Cascade policy: cascade, detach, ignore (default: ignore)"),
                ),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a graph")
                .arg(Arg::new("name").required(true).help("Graph name")),
        )
        .subcommand(Command::new("list").about("List all graphs"))
        .subcommand(
            Command::new("info")
                .about("Get graph metadata")
                .arg(Arg::new("name").required(true).help("Graph name")),
        )
        // Nodes
        .subcommand(
            Command::new("add-node")
                .about("Add a node to a graph")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(Arg::new("node-id").required(true).help("Node ID"))
                .arg(
                    Arg::new("entity-ref")
                        .long("entity-ref")
                        .value_name("URI")
                        .help("Entity reference URI"),
                )
                .arg(
                    Arg::new("properties")
                        .long("properties")
                        .value_name("JSON")
                        .help("Node properties as JSON"),
                )
                .arg(
                    Arg::new("type")
                        .long("type")
                        .value_name("TYPE")
                        .help("Object type name"),
                ),
        )
        .subcommand(
            Command::new("get-node")
                .about("Get a node from a graph")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(Arg::new("node-id").required(true).help("Node ID")),
        )
        .subcommand(
            Command::new("remove-node")
                .about("Remove a node from a graph")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(Arg::new("node-id").required(true).help("Node ID")),
        )
        .subcommand(
            Command::new("list-nodes")
                .about("List all nodes in a graph")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(
                    Arg::new("type")
                        .long("type")
                        .value_name("TYPE")
                        .help("Filter by object type"),
                ),
        )
        // Edges
        .subcommand(
            Command::new("add-edge")
                .about("Add an edge to a graph")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(Arg::new("src").required(true).help("Source node ID"))
                .arg(Arg::new("dst").required(true).help("Destination node ID"))
                .arg(Arg::new("edge-type").required(true).help("Edge type"))
                .arg(
                    Arg::new("weight")
                        .long("weight")
                        .value_name("WEIGHT")
                        .value_parser(clap::value_parser!(f64))
                        .help("Edge weight (f64)"),
                )
                .arg(
                    Arg::new("properties")
                        .long("properties")
                        .value_name("JSON")
                        .help("Edge properties as JSON"),
                ),
        )
        .subcommand(
            Command::new("remove-edge")
                .about("Remove an edge from a graph")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(Arg::new("src").required(true).help("Source node ID"))
                .arg(Arg::new("dst").required(true).help("Destination node ID"))
                .arg(Arg::new("edge-type").required(true).help("Edge type")),
        )
        .subcommand(
            Command::new("neighbors")
                .about("Get neighbors of a node")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(Arg::new("node-id").required(true).help("Node ID"))
                .arg(
                    Arg::new("direction")
                        .long("direction")
                        .value_name("DIR")
                        .help("Direction: outgoing, incoming, both"),
                )
                .arg(
                    Arg::new("edge-type")
                        .long("edge-type")
                        .value_name("TYPE")
                        .help("Filter by edge type"),
                ),
        )
        // Bulk & Traversal
        .subcommand(
            Command::new("bulk-insert")
                .about("Bulk insert nodes and edges")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(
                    Arg::new("json")
                        .required_unless_present("file")
                        .help("JSON with {nodes: [...], edges: [...]}"),
                )
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .value_name("PATH")
                        .help("Read JSON from file ('-' for stdin)"),
                )
                .arg(
                    Arg::new("chunk-size")
                        .long("chunk-size")
                        .value_name("SIZE")
                        .value_parser(clap::value_parser!(usize))
                        .help("Chunk size for batching"),
                ),
        )
        .subcommand(
            Command::new("bfs")
                .about("Breadth-first search traversal")
                .arg(Arg::new("graph").required(true).help("Graph name"))
                .arg(Arg::new("start").required(true).help("Start node ID"))
                .arg(
                    Arg::new("max-depth")
                        .long("max-depth")
                        .required(true)
                        .value_name("DEPTH")
                        .value_parser(clap::value_parser!(usize))
                        .help("Maximum traversal depth"),
                )
                .arg(
                    Arg::new("max-nodes")
                        .long("max-nodes")
                        .value_name("COUNT")
                        .value_parser(clap::value_parser!(usize))
                        .help("Maximum nodes to visit"),
                )
                .arg(
                    Arg::new("edge-types")
                        .long("edge-types")
                        .value_name("TYPES")
                        .help("Comma-separated edge types to follow"),
                )
                .arg(
                    Arg::new("direction")
                        .long("direction")
                        .value_name("DIR")
                        .help("Direction: outgoing, incoming, both"),
                ),
        )
        // Ontology (nested)
        .subcommand(
            Command::new("ontology")
                .about("Ontology operations (define, get, list, delete, freeze, status, summary)")
                .subcommand_required(true)
                .subcommand(
                    Command::new("define")
                        .about("Define an object or link type (auto-detected from JSON)")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(
                            Arg::new("json")
                                .required_unless_present("file")
                                .help("Type definition as JSON"),
                        )
                        .arg(
                            Arg::new("file")
                                .long("file")
                                .short('f')
                                .value_name("PATH")
                                .help("Read JSON from file ('-' for stdin)"),
                        ),
                )
                .subcommand(
                    Command::new("get")
                        .about("Get a type definition")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(Arg::new("name").required(true).help("Type name"))
                        .arg(
                            Arg::new("kind")
                                .long("kind")
                                .value_name("KIND")
                                .help("Type kind: object or link (default: try object first)"),
                        ),
                )
                .subcommand(
                    Command::new("list")
                        .about("List ontology types")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(
                            Arg::new("kind")
                                .long("kind")
                                .value_name("KIND")
                                .help("Type kind: object or link (default: list both)"),
                        ),
                )
                .subcommand(
                    Command::new("delete")
                        .about("Delete a type definition")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(Arg::new("name").required(true).help("Type name"))
                        .arg(
                            Arg::new("kind")
                                .long("kind")
                                .value_name("KIND")
                                .help("Type kind: object or link (default: try object first)"),
                        ),
                )
                .subcommand(
                    Command::new("freeze")
                        .about("Freeze the graph ontology")
                        .arg(Arg::new("graph").required(true).help("Graph name")),
                )
                .subcommand(
                    Command::new("status")
                        .about("Get ontology status")
                        .arg(Arg::new("graph").required(true).help("Graph name")),
                )
                .subcommand(
                    Command::new("summary")
                        .about("Get ontology summary")
                        .arg(Arg::new("graph").required(true).help("Graph name")),
                ),
        )
        // Analytics (nested)
        .subcommand(
            Command::new("analytics")
                .about("Graph analytics algorithms (wcc, cdlp, pagerank, lcc, sssp)")
                .subcommand_required(true)
                .subcommand(
                    Command::new("wcc")
                        .about("Weakly Connected Components")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(
                            Arg::new("top-n")
                                .long("top-n")
                                .value_name("COUNT")
                                .value_parser(clap::value_parser!(usize))
                                .help("Number of top groups to return (default: 10)"),
                        )
                        .arg(
                            Arg::new("include-all")
                                .long("include-all")
                                .action(clap::ArgAction::SetTrue)
                                .help("Include full raw results"),
                        ),
                )
                .subcommand(
                    Command::new("cdlp")
                        .about("Community Detection via Label Propagation")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(
                            Arg::new("max-iterations")
                                .long("max-iterations")
                                .required(true)
                                .value_name("COUNT")
                                .value_parser(clap::value_parser!(usize))
                                .help("Maximum iterations"),
                        )
                        .arg(
                            Arg::new("direction")
                                .long("direction")
                                .value_name("DIR")
                                .help("Direction: outgoing, incoming, both"),
                        )
                        .arg(
                            Arg::new("top-n")
                                .long("top-n")
                                .value_name("COUNT")
                                .value_parser(clap::value_parser!(usize))
                                .help("Number of top groups to return (default: 10)"),
                        )
                        .arg(
                            Arg::new("include-all")
                                .long("include-all")
                                .action(clap::ArgAction::SetTrue)
                                .help("Include full raw results"),
                        ),
                )
                .subcommand(
                    Command::new("pagerank")
                        .about("PageRank importance scoring")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(
                            Arg::new("damping")
                                .long("damping")
                                .value_name("FACTOR")
                                .value_parser(clap::value_parser!(f64))
                                .help("Damping factor (default: 0.85)"),
                        )
                        .arg(
                            Arg::new("max-iterations")
                                .long("max-iterations")
                                .value_name("COUNT")
                                .value_parser(clap::value_parser!(usize))
                                .help("Maximum iterations (default: 20)"),
                        )
                        .arg(
                            Arg::new("tolerance")
                                .long("tolerance")
                                .value_name("EPSILON")
                                .value_parser(clap::value_parser!(f64))
                                .help("Convergence tolerance (default: 1e-6)"),
                        )
                        .arg(
                            Arg::new("top-n")
                                .long("top-n")
                                .value_name("COUNT")
                                .value_parser(clap::value_parser!(usize))
                                .help("Number of top nodes to return (default: 10)"),
                        )
                        .arg(
                            Arg::new("include-all")
                                .long("include-all")
                                .action(clap::ArgAction::SetTrue)
                                .help("Include full raw results"),
                        ),
                )
                .subcommand(
                    Command::new("lcc")
                        .about("Local Clustering Coefficient")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(
                            Arg::new("top-n")
                                .long("top-n")
                                .value_name("COUNT")
                                .value_parser(clap::value_parser!(usize))
                                .help("Number of top nodes to return (default: 10)"),
                        )
                        .arg(
                            Arg::new("include-all")
                                .long("include-all")
                                .action(clap::ArgAction::SetTrue)
                                .help("Include full raw results"),
                        ),
                )
                .subcommand(
                    Command::new("sssp")
                        .about("Single-Source Shortest Path (Dijkstra)")
                        .arg(Arg::new("graph").required(true).help("Graph name"))
                        .arg(Arg::new("source").required(true).help("Source node ID"))
                        .arg(
                            Arg::new("direction")
                                .long("direction")
                                .value_name("DIR")
                                .help("Direction: outgoing, incoming, both"),
                        )
                        .arg(
                            Arg::new("top-n")
                                .long("top-n")
                                .value_name("COUNT")
                                .value_parser(clap::value_parser!(usize))
                                .help("Number of top nodes to return (default: 10)"),
                        )
                        .arg(
                            Arg::new("include-all")
                                .long("include-all")
                                .action(clap::ArgAction::SetTrue)
                                .help("Include full raw results"),
                        ),
                ),
        )
}

// =========================================================================
// Config
// =========================================================================

fn build_config() -> Command {
    Command::new("config")
        .about("Configuration operations")
        .subcommand_required(true)
        .subcommand(
            Command::new("set")
                .about("Set a configuration value")
                .arg(Arg::new("key").required(true).help("Configuration key"))
                .arg(Arg::new("value").required(true).help("Configuration value")),
        )
        .subcommand(
            Command::new("get")
                .about("Get a configuration value")
                .arg(Arg::new("key").required(true).help("Configuration key")),
        )
        .subcommand(Command::new("list").about("Show all configuration values"))
}
