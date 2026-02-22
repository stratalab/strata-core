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
        .subcommand_required(false)
        .arg(
            Arg::new("db")
                .long("db")
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
                .help("Initial branch (default: default)")
                .global(true),
        )
        .arg(
            Arg::new("space")
                .long("space")
                .help("Initial space (default: default)")
                .global(true),
        )
        .arg(
            Arg::new("json")
                .long("json")
                .help("JSON output mode")
                .action(clap::ArgAction::SetTrue)
                .conflicts_with("raw")
                .global(true),
        )
        .arg(
            Arg::new("raw")
                .long("raw")
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
            Arg::new("auto-embed")
                .long("auto-embed")
                .help("Enable automatic text embedding for semantic search")
                .action(clap::ArgAction::SetTrue)
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
        .subcommand(build_search())
        .subcommand(build_setup())
        .subcommand(build_configure_model())
        .subcommand(build_embed())
        .subcommand(build_models())
        .subcommand(build_generate())
        .subcommand(build_tokenize())
        .subcommand(build_detokenize())
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
                        .help("Key prefix filter"),
                )
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('n')
                        .help("Maximum keys to return"),
                )
                .arg(
                    Arg::new("cursor")
                        .long("cursor")
                        .short('c')
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
                        .help("Key prefix filter"),
                )
                .arg(
                    Arg::new("cursor")
                        .long("cursor")
                        .short('c')
                        .help("Pagination cursor"),
                )
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('n')
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
                .arg(Arg::new("limit").long("limit").help("Maximum events"))
                .arg(
                    Arg::new("after")
                        .long("after")
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
                .arg(Arg::new("k").default_value("10").help("Number of results"))
                .arg(
                    Arg::new("metric")
                        .long("metric")
                        .help("Distance metric: cosine, euclidean, dotproduct"),
                )
                .arg(
                    Arg::new("filter")
                        .long("filter")
                        .help("Metadata filter as JSON"),
                ),
        )
        .subcommand(
            Command::new("create")
                .about("Create a vector collection")
                .arg(Arg::new("name").required(true).help("Collection name"))
                .arg(Arg::new("dim").required(true).help("Vector dimension"))
                .arg(
                    Arg::new("metric")
                        .long("metric")
                        .default_value("cosine")
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
            Command::new("list")
                .about("List all branches")
                .arg(Arg::new("limit").long("limit").help("Maximum branches")),
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
            .long("read-only")
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

// =========================================================================
// Search
// =========================================================================

fn build_search() -> Command {
    Command::new("search")
        .about("Search across multiple primitives")
        .arg(Arg::new("query").required(true).help("Search query"))
        .arg(Arg::new("k").long("k").help("Number of results"))
        .arg(
            Arg::new("primitives")
                .long("primitives")
                .help("Comma-separated list of primitives to search"),
        )
        .arg(
            Arg::new("time-start")
                .long("time-start")
                .help("Time range start (ISO 8601, e.g. 2026-02-07T00:00:00Z)"),
        )
        .arg(
            Arg::new("time-end")
                .long("time-end")
                .help("Time range end (ISO 8601, e.g. 2026-02-09T00:00:00Z)"),
        )
        .arg(
            Arg::new("mode")
                .long("mode")
                .help("Search mode: keyword, hybrid (default: hybrid)"),
        )
        .arg(
            Arg::new("expand")
                .long("expand")
                .help("Enable/disable query expansion (true/false)"),
        )
        .arg(
            Arg::new("rerank")
                .long("rerank")
                .help("Enable/disable reranking (true/false)"),
        )
}

// =========================================================================
// Setup
// =========================================================================

fn build_setup() -> Command {
    Command::new("setup").about("Download model files for auto-embedding")
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
                .help("Bearer token for the endpoint"),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
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
                .help("Maximum tokens to generate (default: 256)"),
        )
        .arg(
            Arg::new("temperature")
                .long("temperature")
                .help("Sampling temperature (0.0 = greedy)"),
        )
        .arg(
            Arg::new("top-k")
                .long("top-k")
                .help("Top-K sampling (0 = disabled)"),
        )
        .arg(
            Arg::new("top-p")
                .long("top-p")
                .help("Top-P nucleus sampling (1.0 = disabled)"),
        )
        .arg(
            Arg::new("seed")
                .long("seed")
                .help("Random seed for reproducibility"),
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
