//! ArgMatches → Command/BranchOp/MetaCommand conversion.
//!
//! Translates clap's parsed arguments into the appropriate action:
//! - Standard commands → `CliAction::Execute(Command)`
//! - Branch power ops → `CliAction::BranchOp`
//! - REPL meta-commands → `CliAction::Meta`
//! - Multi-key operations → `CliAction::MultiPut/MultiGet/MultiDel`
//! - Pagination → `CliAction::ListAll`

use std::io::Read;

use clap::ArgMatches;
use strata_executor::{
    BatchVectorEntry, BranchId, BulkGraphEdge, BulkGraphNode, Command, DistanceMetric,
    ExportFormat, ExportPrimitive, MergeStrategy, MetadataFilter, SearchQuery, TxnOptions, Value,
};

use crate::state::SessionState;
use crate::value::{parse_json_value, parse_value, parse_vector};

/// The result of parsing user input.
#[allow(dead_code)]
pub enum CliAction {
    /// A standard command to execute via Session.
    Execute(Command),
    /// A branch power-API operation (fork/diff/merge).
    BranchOp(BranchOp),
    /// A REPL-only meta-command.
    Meta(MetaCommand),
    /// Multi-key put operation.
    MultiPut {
        branch: Option<BranchId>,
        space: Option<String>,
        pairs: Vec<(String, Value)>,
    },
    /// Multi-key get operation.
    MultiGet {
        branch: Option<BranchId>,
        space: Option<String>,
        keys: Vec<String>,
        with_version: bool,
    },
    /// Multi-key delete operation.
    MultiDel {
        branch: Option<BranchId>,
        space: Option<String>,
        keys: Vec<String>,
    },
    /// List all with automatic pagination.
    ListAll {
        branch: Option<BranchId>,
        space: Option<String>,
        prefix: Option<String>,
        primitive: Primitive,
    },
    /// Get with version flag (wraps existing command).
    GetWithVersion {
        command: Command,
        with_version: bool,
    },
}

/// Primitive type for ListAll pagination.
#[derive(Debug, Clone, Copy)]
pub enum Primitive {
    Kv,
    Json,
}

/// Branch operations that bypass the Command enum.
pub enum BranchOp {
    Fork {
        destination: String,
    },
    Diff {
        branch_a: String,
        branch_b: String,
    },
    Merge {
        source: String,
        strategy: MergeStrategy,
    },
}

/// REPL meta-commands.
pub enum MetaCommand {
    Use {
        branch: String,
        space: Option<String>,
    },
    Help {
        command: Option<String>,
    },
    Quit,
    Clear,
}

/// Check for REPL meta-commands before delegating to clap.
///
/// Returns `Some(MetaCommand)` if the line is a meta-command, `None` otherwise.
pub fn check_meta_command(line: &str) -> Option<MetaCommand> {
    let trimmed = line.trim();
    let mut parts = trimmed.splitn(3, char::is_whitespace);
    let cmd = parts.next()?;

    match cmd {
        "quit" | "exit" => Some(MetaCommand::Quit),
        "clear" => Some(MetaCommand::Clear),
        "help" => {
            let command = parts.next().map(|s| s.trim().to_string());
            Some(MetaCommand::Help { command })
        }
        "use" => {
            let branch = parts.next()?.trim().to_string();
            let space = parts.next().map(|s| s.trim().to_string());
            Some(MetaCommand::Use { branch, space })
        }
        _ => None,
    }
}

/// Build an actionable error message for an unknown subcommand, with "did you mean?" suggestion.
fn unknown_subcommand(category: &str, input: &str, valid: &[&str]) -> String {
    let best = valid
        .iter()
        .filter_map(|c| {
            let d = strsim::levenshtein(&input.to_ascii_lowercase(), &c.to_ascii_lowercase());
            (d <= 2 && d > 0).then_some((*c, d))
        })
        .min_by_key(|(_, d)| *d);
    let options = valid.join(", ");
    let prefix = if category.is_empty() {
        format!("Unknown command \"{}\"", input)
    } else {
        format!("Unknown {} subcommand \"{}\"", category, input)
    };
    match best {
        Some((s, _)) => format!(
            "{}. Did you mean \"{}\"?\nValid options: {}",
            prefix, s, options
        ),
        None => format!("{}.\nValid options: {}", prefix, options),
    }
}

/// Convert clap ArgMatches into a CliAction.
pub fn matches_to_action(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub_name, sub_matches) = matches
        .subcommand()
        .ok_or_else(|| "No command provided".to_string())?;

    match sub_name {
        "kv" => parse_kv(sub_matches, state),
        "json" => parse_json(sub_matches, state),
        "event" => parse_event(sub_matches, state),
        "vector" => parse_vector_cmd(sub_matches, state),
        "graph" => parse_graph(sub_matches, state),
        "branch" => parse_branch(sub_matches, state),
        "space" => parse_space(sub_matches, state),
        "begin" => parse_begin(sub_matches, state),
        "commit" => Ok(CliAction::Execute(Command::TxnCommit)),
        "rollback" => Ok(CliAction::Execute(Command::TxnRollback)),
        "txn" => parse_txn(sub_matches),
        "ping" => Ok(CliAction::Execute(Command::Ping)),
        "info" => Ok(CliAction::Execute(Command::Info)),
        "health" => Ok(CliAction::Execute(Command::Health)),
        "metrics" => Ok(CliAction::Execute(Command::Metrics)),
        "flush" => Ok(CliAction::Execute(Command::Flush)),
        "compact" => Ok(CliAction::Execute(Command::Compact)),
        "describe" => Ok(CliAction::Execute(Command::Describe {
            branch: branch(state),
        })),
        "search" => parse_search(sub_matches, state),
        "config" => parse_config(sub_matches),
        "recipe" => parse_recipe(sub_matches, state),
        "configure-model" => parse_configure_model(sub_matches),
        "embed" => parse_embed(sub_matches),
        "models" => parse_models(sub_matches),
        "generate" => parse_generate(sub_matches),
        "tokenize" => parse_tokenize(sub_matches),
        "detokenize" => parse_detokenize(sub_matches),
        "export" => parse_db_export(sub_matches, state),
        "import" => parse_db_import(sub_matches, state),
        other => Err(unknown_subcommand(
            "",
            other,
            &[
                "kv",
                "json",
                "event",
                "vector",
                "graph",
                "branch",
                "space",
                "begin",
                "commit",
                "rollback",
                "txn",
                "ping",
                "info",
                "health",
                "metrics",
                "init",
                "flush",
                "compact",
                "describe",
                "search",
                "config",
                "recipe",
                "configure-model",
                "embed",
                "models",
                "generate",
                "tokenize",
                "detokenize",
                "export",
                "import",
            ],
        )),
    }
}

// =========================================================================
// Branch/space helpers
// =========================================================================

fn branch(state: &SessionState) -> Option<BranchId> {
    Some(BranchId::from(state.branch()))
}

fn space(state: &SessionState) -> Option<String> {
    Some(state.space().to_string())
}

// =========================================================================
// File reading helper
// =========================================================================

/// Read a value from a file or stdin.
///
/// If `source` is "-", reads from stdin.
/// Attempts to parse as JSON first, falls back to string.
fn read_value_from_source(source: &str) -> Result<Value, String> {
    let content = if source == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| format!("Failed to read stdin: {}", e))?;
        buf
    } else {
        std::fs::read_to_string(source)
            .map_err(|e| format!("Failed to read '{}': {}", source, e))?
    };

    // Try JSON first
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
        return Ok(Value::from(json));
    }

    // Fall back to string (trimmed)
    Ok(Value::String(content.trim().to_string()))
}

/// Read JSON value from a file or stdin.
///
/// If `source` is "-", reads from stdin.
/// Must be valid JSON.
fn read_json_from_source(source: &str) -> Result<Value, String> {
    let content = if source == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| format!("Failed to read stdin: {}", e))?;
        buf
    } else {
        std::fs::read_to_string(source)
            .map_err(|e| format!("Failed to read '{}': {}", source, e))?
    };

    let json: serde_json::Value =
        serde_json::from_str(&content).map_err(|e| format!("Invalid JSON in file: {}", e))?;
    Ok(Value::from(json))
}

// =========================================================================
// KV
// =========================================================================

fn parse_kv(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No kv subcommand")?;
    match sub {
        "put" => {
            if let Some(file_path) = m.get_one::<String>("file") {
                // File mode: requires exactly one key in pairs
                let pairs: Vec<String> = m
                    .get_many::<String>("pairs")
                    .map(|v| v.cloned().collect())
                    .unwrap_or_default();

                if pairs.len() != 1 {
                    return Err("--file requires exactly one key argument".to_string());
                }

                let value = read_value_from_source(file_path)?;
                Ok(CliAction::Execute(Command::KvPut {
                    branch: branch(state),
                    space: space(state),
                    key: pairs[0].clone(),
                    value,
                }))
            } else {
                // Normal mode: key-value pairs from args
                let pairs: Vec<String> = m
                    .get_many::<String>("pairs")
                    .ok_or("Missing key-value pairs")?
                    .cloned()
                    .collect();

                if pairs.len() < 2 {
                    return Err("kv put requires at least one key-value pair".to_string());
                }

                if !pairs.len().is_multiple_of(2) {
                    return Err("Key-value pairs must come in pairs".to_string());
                }

                if pairs.len() == 2 {
                    // Single pair
                    let key = pairs[0].clone();
                    let value = parse_value(&pairs[1]);
                    Ok(CliAction::Execute(Command::KvPut {
                        branch: branch(state),
                        space: space(state),
                        key,
                        value,
                    }))
                } else {
                    // Multiple pairs
                    let kv_pairs: Vec<(String, Value)> = pairs
                        .chunks(2)
                        .map(|c| (c[0].clone(), parse_value(&c[1])))
                        .collect();
                    Ok(CliAction::MultiPut {
                        branch: branch(state),
                        space: space(state),
                        pairs: kv_pairs,
                    })
                }
            }
        }
        "get" => {
            let keys: Vec<String> = m.get_many::<String>("keys").unwrap().cloned().collect();
            let with_version = m.get_flag("with-version");

            if keys.len() == 1 {
                let cmd = Command::KvGet {
                    branch: branch(state),
                    space: space(state),
                    key: keys[0].clone(),
                    as_of: None,
                };
                if with_version {
                    Ok(CliAction::GetWithVersion {
                        command: cmd,
                        with_version: true,
                    })
                } else {
                    Ok(CliAction::Execute(cmd))
                }
            } else {
                Ok(CliAction::MultiGet {
                    branch: branch(state),
                    space: space(state),
                    keys,
                    with_version,
                })
            }
        }
        "del" => {
            let keys: Vec<String> = m.get_many::<String>("keys").unwrap().cloned().collect();

            if keys.len() == 1 {
                Ok(CliAction::Execute(Command::KvDelete {
                    branch: branch(state),
                    space: space(state),
                    key: keys[0].clone(),
                }))
            } else {
                Ok(CliAction::MultiDel {
                    branch: branch(state),
                    space: space(state),
                    keys,
                })
            }
        }
        "list" => {
            let all = m.get_flag("all");
            let prefix = m.get_one::<String>("prefix").cloned();

            if all {
                Ok(CliAction::ListAll {
                    branch: branch(state),
                    space: space(state),
                    prefix,
                    primitive: Primitive::Kv,
                })
            } else {
                let limit = m.get_one::<u64>("limit").copied();
                let cursor = m.get_one::<String>("cursor").cloned();
                Ok(CliAction::Execute(Command::KvList {
                    branch: branch(state),
                    space: space(state),
                    prefix,
                    cursor,
                    limit,
                    as_of: None,
                }))
            }
        }
        "history" => {
            let key = m.get_one::<String>("key").unwrap().clone();
            Ok(CliAction::Execute(Command::KvGetv {
                branch: branch(state),
                space: space(state),
                key,
                as_of: None,
            }))
        }
        other => Err(unknown_subcommand(
            "kv",
            other,
            &["put", "get", "del", "list", "history"],
        )),
    }
}

// =========================================================================
// JSON
// =========================================================================

fn parse_json(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No json subcommand")?;
    match sub {
        "set" => {
            let key = m.get_one::<String>("key").unwrap().clone();
            let path = m.get_one::<String>("path").unwrap().clone();

            let value = if let Some(file_path) = m.get_one::<String>("file") {
                read_json_from_source(file_path)?
            } else {
                let raw = m.get_one::<String>("value").unwrap();
                parse_json_value(raw)?
            };

            Ok(CliAction::Execute(Command::JsonSet {
                branch: branch(state),
                space: space(state),
                key,
                path,
                value,
            }))
        }
        "get" => {
            let key = m.get_one::<String>("key").unwrap().clone();
            let path = m.get_one::<String>("path").unwrap().clone();
            let with_version = m.get_flag("with-version");

            let cmd = Command::JsonGet {
                branch: branch(state),
                space: space(state),
                key,
                path,
                as_of: None,
            };

            if with_version {
                Ok(CliAction::GetWithVersion {
                    command: cmd,
                    with_version: true,
                })
            } else {
                Ok(CliAction::Execute(cmd))
            }
        }
        "del" => {
            let key = m.get_one::<String>("key").unwrap().clone();
            let path = m.get_one::<String>("path").unwrap().clone();
            Ok(CliAction::Execute(Command::JsonDelete {
                branch: branch(state),
                space: space(state),
                key,
                path,
            }))
        }
        "list" => {
            let all = m.get_flag("all");
            let prefix = m.get_one::<String>("prefix").cloned();

            if all {
                Ok(CliAction::ListAll {
                    branch: branch(state),
                    space: space(state),
                    prefix,
                    primitive: Primitive::Json,
                })
            } else {
                let cursor = m.get_one::<String>("cursor").cloned();
                let limit = m.get_one::<u64>("limit").copied().unwrap_or(100);
                Ok(CliAction::Execute(Command::JsonList {
                    branch: branch(state),
                    space: space(state),
                    prefix,
                    cursor,
                    limit,
                    as_of: None,
                }))
            }
        }
        "history" => {
            let key = m.get_one::<String>("key").unwrap().clone();
            Ok(CliAction::Execute(Command::JsonGetv {
                branch: branch(state),
                space: space(state),
                key,
                as_of: None,
            }))
        }
        other => Err(unknown_subcommand(
            "json",
            other,
            &["set", "get", "del", "list", "history"],
        )),
    }
}

// =========================================================================
// Event
// =========================================================================

fn parse_event(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No event subcommand")?;
    match sub {
        "append" => {
            let event_type = m.get_one::<String>("type").unwrap().clone();

            let payload = if let Some(file_path) = m.get_one::<String>("file") {
                read_json_from_source(file_path)?
            } else {
                let raw = m.get_one::<String>("payload").unwrap();
                parse_json_value(raw)?
            };

            Ok(CliAction::Execute(Command::EventAppend {
                branch: branch(state),
                space: space(state),
                event_type,
                payload,
            }))
        }
        "get" => {
            let sequence = m
                .get_one::<String>("sequence")
                .unwrap()
                .parse::<u64>()
                .map_err(|e| format!("Invalid sequence: {}", e))?;
            Ok(CliAction::Execute(Command::EventGet {
                branch: branch(state),
                space: space(state),
                sequence,
                as_of: None,
            }))
        }
        "list" => {
            let event_type = m.get_one::<String>("type").unwrap().clone();
            let limit = m.get_one::<u64>("limit").copied();
            let after_sequence = m.get_one::<u64>("after").copied();
            Ok(CliAction::Execute(Command::EventGetByType {
                branch: branch(state),
                space: space(state),
                event_type,
                limit,
                after_sequence,
                as_of: None,
            }))
        }
        "len" => Ok(CliAction::Execute(Command::EventLen {
            branch: branch(state),
            space: space(state),
        })),
        other => Err(unknown_subcommand(
            "event",
            other,
            &["append", "get", "list", "len"],
        )),
    }
}

// =========================================================================
// Vector
// =========================================================================

fn parse_metric(s: &str) -> Result<DistanceMetric, String> {
    match s.to_lowercase().as_str() {
        "cosine" => Ok(DistanceMetric::Cosine),
        "euclidean" => Ok(DistanceMetric::Euclidean),
        "dotproduct" | "dot_product" | "dot" => Ok(DistanceMetric::DotProduct),
        other => Err(unknown_subcommand(
            "metric",
            other,
            &["cosine", "euclidean", "dotproduct"],
        )),
    }
}

fn parse_vector_cmd(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No vector subcommand")?;
    match sub {
        "upsert" => {
            let collection = m.get_one::<String>("collection").unwrap().clone();
            let key = m.get_one::<String>("key").unwrap().clone();
            let vector = parse_vector(m.get_one::<String>("vector").unwrap())?;
            let metadata = m
                .get_one::<String>("metadata")
                .map(|s| parse_json_value(s))
                .transpose()?;
            Ok(CliAction::Execute(Command::VectorUpsert {
                branch: branch(state),
                space: space(state),
                collection,
                key,
                vector,
                metadata,
            }))
        }
        "get" => {
            let collection = m.get_one::<String>("collection").unwrap().clone();
            let key = m.get_one::<String>("key").unwrap().clone();
            Ok(CliAction::Execute(Command::VectorGet {
                branch: branch(state),
                space: space(state),
                collection,
                key,
                as_of: None,
            }))
        }
        "del" => {
            let collection = m.get_one::<String>("collection").unwrap().clone();
            let key = m.get_one::<String>("key").unwrap().clone();
            Ok(CliAction::Execute(Command::VectorDelete {
                branch: branch(state),
                space: space(state),
                collection,
                key,
            }))
        }
        "search" => {
            let collection = m.get_one::<String>("collection").unwrap().clone();
            let query = parse_vector(m.get_one::<String>("query").unwrap())?;
            let k = *m.get_one::<u64>("top-k").unwrap();
            let metric = m
                .get_one::<String>("metric")
                .map(|s| parse_metric(s))
                .transpose()?;
            let filter = m
                .get_one::<String>("filter")
                .map(|s| -> Result<Vec<MetadataFilter>, String> {
                    serde_json::from_str(s).map_err(|e| format!("Invalid filter JSON: {}", e))
                })
                .transpose()?;
            Ok(CliAction::Execute(Command::VectorSearch {
                branch: branch(state),
                space: space(state),
                collection,
                query,
                k,
                filter,
                metric,
                as_of: None,
            }))
        }
        "create" => {
            let collection = m.get_one::<String>("name").unwrap().clone();
            let dimension = *m.get_one::<u64>("dim").unwrap();
            let metric = parse_metric(m.get_one::<String>("metric").unwrap())?;
            Ok(CliAction::Execute(Command::VectorCreateCollection {
                branch: branch(state),
                space: space(state),
                collection,
                dimension,
                metric,
            }))
        }
        "drop" => {
            let collection = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::VectorDeleteCollection {
                branch: branch(state),
                space: space(state),
                collection,
            }))
        }
        "collections" => Ok(CliAction::Execute(Command::VectorListCollections {
            branch: branch(state),
            space: space(state),
        })),
        "stats" => {
            let collection = m.get_one::<String>("collection").unwrap().clone();
            Ok(CliAction::Execute(Command::VectorCollectionStats {
                branch: branch(state),
                space: space(state),
                collection,
            }))
        }
        "batch-upsert" => {
            let collection = m.get_one::<String>("collection").unwrap().clone();
            let raw = m.get_one::<String>("json").unwrap();
            let entries: Vec<BatchVectorEntry> =
                serde_json::from_str(raw).map_err(|e| format!("Invalid batch JSON: {}", e))?;
            Ok(CliAction::Execute(Command::VectorBatchUpsert {
                branch: branch(state),
                space: space(state),
                collection,
                entries,
            }))
        }
        other => Err(unknown_subcommand(
            "vector",
            other,
            &[
                "upsert",
                "get",
                "del",
                "search",
                "create",
                "drop",
                "collections",
                "stats",
                "batch-upsert",
            ],
        )),
    }
}

// =========================================================================
// Graph
// =========================================================================

fn parse_graph(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No graph subcommand")?;
    match sub {
        // Lifecycle
        "create" => {
            let graph = m.get_one::<String>("name").unwrap().clone();
            let cascade_policy = m.get_one::<String>("cascade-policy").cloned();
            Ok(CliAction::Execute(Command::GraphCreate {
                branch: branch(state),
                graph,
                cascade_policy,
            }))
        }
        "delete" => {
            let graph = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::GraphDelete {
                branch: branch(state),
                graph,
            }))
        }
        "list" => Ok(CliAction::Execute(Command::GraphList {
            branch: branch(state),
        })),
        "info" => {
            let graph = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::GraphGetMeta {
                branch: branch(state),
                graph,
            }))
        }
        // Nodes
        "add-node" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            let node_id = m.get_one::<String>("node-id").unwrap().clone();
            let entity_ref = m.get_one::<String>("entity-ref").cloned();
            let properties = m
                .get_one::<String>("properties")
                .map(|s| parse_json_value(s))
                .transpose()?;
            let object_type = m.get_one::<String>("type").cloned();
            Ok(CliAction::Execute(Command::GraphAddNode {
                branch: branch(state),
                graph,
                node_id,
                entity_ref,
                properties,
                object_type,
            }))
        }
        "get-node" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            let node_id = m.get_one::<String>("node-id").unwrap().clone();
            Ok(CliAction::Execute(Command::GraphGetNode {
                branch: branch(state),
                graph,
                node_id,
            }))
        }
        "remove-node" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            let node_id = m.get_one::<String>("node-id").unwrap().clone();
            Ok(CliAction::Execute(Command::GraphRemoveNode {
                branch: branch(state),
                graph,
                node_id,
            }))
        }
        "list-nodes" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            if let Some(object_type) = m.get_one::<String>("type").cloned() {
                Ok(CliAction::Execute(Command::GraphNodesByType {
                    branch: branch(state),
                    graph,
                    object_type,
                }))
            } else {
                Ok(CliAction::Execute(Command::GraphListNodes {
                    branch: branch(state),
                    graph,
                }))
            }
        }
        // Edges
        "add-edge" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            let src = m.get_one::<String>("src").unwrap().clone();
            let dst = m.get_one::<String>("dst").unwrap().clone();
            let edge_type = m.get_one::<String>("edge-type").unwrap().clone();
            let weight = m.get_one::<f64>("weight").copied();
            let properties = m
                .get_one::<String>("properties")
                .map(|s| parse_json_value(s))
                .transpose()?;
            Ok(CliAction::Execute(Command::GraphAddEdge {
                branch: branch(state),
                graph,
                src,
                dst,
                edge_type,
                weight,
                properties,
            }))
        }
        "remove-edge" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            let src = m.get_one::<String>("src").unwrap().clone();
            let dst = m.get_one::<String>("dst").unwrap().clone();
            let edge_type = m.get_one::<String>("edge-type").unwrap().clone();
            Ok(CliAction::Execute(Command::GraphRemoveEdge {
                branch: branch(state),
                graph,
                src,
                dst,
                edge_type,
            }))
        }
        "neighbors" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            let node_id = m.get_one::<String>("node-id").unwrap().clone();
            let direction = m.get_one::<String>("direction").cloned();
            let edge_type = m.get_one::<String>("edge-type").cloned();
            Ok(CliAction::Execute(Command::GraphNeighbors {
                branch: branch(state),
                graph,
                node_id,
                direction,
                edge_type,
            }))
        }
        // Bulk & Traversal
        "bulk-insert" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();

            let raw_json = if let Some(file_path) = m.get_one::<String>("file") {
                let content = if file_path == "-" {
                    let mut buf = String::new();
                    std::io::stdin()
                        .read_to_string(&mut buf)
                        .map_err(|e| format!("Failed to read stdin: {}", e))?;
                    buf
                } else {
                    std::fs::read_to_string(file_path)
                        .map_err(|e| format!("Failed to read '{}': {}", file_path, e))?
                };
                content
            } else {
                m.get_one::<String>("json").unwrap().clone()
            };

            let parsed: serde_json::Value =
                serde_json::from_str(&raw_json).map_err(|e| format!("Invalid JSON: {}", e))?;

            let nodes: Vec<BulkGraphNode> = parsed
                .get("nodes")
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()
                .map_err(|e| format!("Invalid nodes: {}", e))?
                .unwrap_or_default();

            let edges: Vec<BulkGraphEdge> = parsed
                .get("edges")
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()
                .map_err(|e| format!("Invalid edges: {}", e))?
                .unwrap_or_default();

            let chunk_size = m.get_one::<usize>("chunk-size").copied();

            Ok(CliAction::Execute(Command::GraphBulkInsert {
                branch: branch(state),
                graph,
                nodes,
                edges,
                chunk_size,
            }))
        }
        "bfs" => {
            let graph = m.get_one::<String>("graph").unwrap().clone();
            let start = m.get_one::<String>("start").unwrap().clone();
            let max_depth = *m.get_one::<usize>("max-depth").unwrap();
            let max_nodes = m.get_one::<usize>("max-nodes").copied();
            let edge_types = m
                .get_one::<String>("edge-types")
                .map(|s| s.split(',').map(|t| t.trim().to_string()).collect());
            let direction = m.get_one::<String>("direction").cloned();
            Ok(CliAction::Execute(Command::GraphBfs {
                branch: branch(state),
                graph,
                start,
                max_depth,
                max_nodes,
                edge_types,
                direction,
            }))
        }
        // Ontology (nested)
        "ontology" => {
            let (onto_sub, onto_m) = m.subcommand().ok_or("No ontology subcommand")?;
            match onto_sub {
                "define" => {
                    let graph = onto_m.get_one::<String>("graph").unwrap().clone();
                    let definition = if let Some(file_path) = onto_m.get_one::<String>("file") {
                        read_json_from_source(file_path)?
                    } else {
                        let raw = onto_m.get_one::<String>("json").unwrap();
                        parse_json_value(raw)?
                    };
                    // Auto-detect: if JSON has "source" and "target" keys, it's a link type
                    let is_link = match &definition {
                        Value::Object(map) => {
                            map.contains_key("source") && map.contains_key("target")
                        }
                        _ => false,
                    };
                    if is_link {
                        Ok(CliAction::Execute(Command::GraphDefineLinkType {
                            branch: branch(state),
                            graph,
                            definition,
                        }))
                    } else {
                        Ok(CliAction::Execute(Command::GraphDefineObjectType {
                            branch: branch(state),
                            graph,
                            definition,
                        }))
                    }
                }
                "get" => {
                    let graph = onto_m.get_one::<String>("graph").unwrap().clone();
                    let name = onto_m.get_one::<String>("name").unwrap().clone();
                    let kind = onto_m.get_one::<String>("kind").map(|s| s.as_str());
                    match kind {
                        Some("link") => Ok(CliAction::Execute(Command::GraphGetLinkType {
                            branch: branch(state),
                            graph,
                            name,
                        })),
                        _ => Ok(CliAction::Execute(Command::GraphGetObjectType {
                            branch: branch(state),
                            graph,
                            name,
                        })),
                    }
                }
                "list" => {
                    let graph = onto_m.get_one::<String>("graph").unwrap().clone();
                    let kind = onto_m.get_one::<String>("kind").map(|s| s.as_str());
                    match kind {
                        Some("object") => Ok(CliAction::Execute(Command::GraphListObjectTypes {
                            branch: branch(state),
                            graph,
                        })),
                        Some("link") => Ok(CliAction::Execute(Command::GraphListLinkTypes {
                            branch: branch(state),
                            graph,
                        })),
                        None => Ok(CliAction::Execute(Command::GraphListOntologyTypes {
                            branch: branch(state),
                            graph,
                        })),
                        Some(other) => Err(format!(
                            "Invalid --kind '{}'. Use 'object' or 'link'.",
                            other
                        )),
                    }
                }
                "delete" => {
                    let graph = onto_m.get_one::<String>("graph").unwrap().clone();
                    let name = onto_m.get_one::<String>("name").unwrap().clone();
                    let kind = onto_m.get_one::<String>("kind").map(|s| s.as_str());
                    match kind {
                        Some("link") => Ok(CliAction::Execute(Command::GraphDeleteLinkType {
                            branch: branch(state),
                            graph,
                            name,
                        })),
                        _ => Ok(CliAction::Execute(Command::GraphDeleteObjectType {
                            branch: branch(state),
                            graph,
                            name,
                        })),
                    }
                }
                "freeze" => {
                    let graph = onto_m.get_one::<String>("graph").unwrap().clone();
                    Ok(CliAction::Execute(Command::GraphFreezeOntology {
                        branch: branch(state),
                        graph,
                    }))
                }
                "status" => {
                    let graph = onto_m.get_one::<String>("graph").unwrap().clone();
                    Ok(CliAction::Execute(Command::GraphOntologyStatus {
                        branch: branch(state),
                        graph,
                    }))
                }
                "summary" => {
                    let graph = onto_m.get_one::<String>("graph").unwrap().clone();
                    Ok(CliAction::Execute(Command::GraphOntologySummary {
                        branch: branch(state),
                        graph,
                    }))
                }
                other => Err(unknown_subcommand(
                    "ontology",
                    other,
                    &[
                        "define", "get", "list", "delete", "freeze", "status", "summary",
                    ],
                )),
            }
        }
        // Analytics (nested)
        "analytics" => {
            let (alg_sub, alg_m) = m.subcommand().ok_or("No analytics subcommand")?;
            // Shared analytics options
            let top_n = alg_m.get_one::<usize>("top-n").copied();
            let include_all = if alg_m.get_flag("include-all") {
                Some(true)
            } else {
                None
            };
            match alg_sub {
                "wcc" => {
                    let graph = alg_m.get_one::<String>("graph").unwrap().clone();
                    Ok(CliAction::Execute(Command::GraphWcc {
                        branch: branch(state),
                        graph,
                        top_n,
                        include_all,
                    }))
                }
                "cdlp" => {
                    let graph = alg_m.get_one::<String>("graph").unwrap().clone();
                    let max_iterations = *alg_m.get_one::<usize>("max-iterations").unwrap();
                    let direction = alg_m.get_one::<String>("direction").cloned();
                    Ok(CliAction::Execute(Command::GraphCdlp {
                        branch: branch(state),
                        graph,
                        max_iterations,
                        direction,
                        top_n,
                        include_all,
                    }))
                }
                "pagerank" => {
                    let graph = alg_m.get_one::<String>("graph").unwrap().clone();
                    let damping = alg_m.get_one::<f64>("damping").copied();
                    let max_iterations = alg_m.get_one::<usize>("max-iterations").copied();
                    let tolerance = alg_m.get_one::<f64>("tolerance").copied();
                    Ok(CliAction::Execute(Command::GraphPagerank {
                        branch: branch(state),
                        graph,
                        damping,
                        max_iterations,
                        tolerance,
                        top_n,
                        include_all,
                    }))
                }
                "lcc" => {
                    let graph = alg_m.get_one::<String>("graph").unwrap().clone();
                    Ok(CliAction::Execute(Command::GraphLcc {
                        branch: branch(state),
                        graph,
                        top_n,
                        include_all,
                    }))
                }
                "sssp" => {
                    let graph = alg_m.get_one::<String>("graph").unwrap().clone();
                    let source = alg_m.get_one::<String>("source").unwrap().clone();
                    let direction = alg_m.get_one::<String>("direction").cloned();
                    Ok(CliAction::Execute(Command::GraphSssp {
                        branch: branch(state),
                        graph,
                        source,
                        direction,
                        top_n,
                        include_all,
                    }))
                }
                other => Err(unknown_subcommand(
                    "analytics",
                    other,
                    &["wcc", "cdlp", "pagerank", "lcc", "sssp"],
                )),
            }
        }
        other => Err(unknown_subcommand(
            "graph",
            other,
            &[
                "create",
                "delete",
                "list",
                "info",
                "add-node",
                "get-node",
                "remove-node",
                "list-nodes",
                "add-edge",
                "remove-edge",
                "neighbors",
                "bulk-insert",
                "bfs",
                "ontology",
                "analytics",
            ],
        )),
    }
}

// =========================================================================
// Branch
// =========================================================================

fn parse_branch(matches: &ArgMatches, _state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No branch subcommand")?;
    match sub {
        "create" => {
            let branch_id = m.get_one::<String>("name").cloned();
            Ok(CliAction::Execute(Command::BranchCreate {
                branch_id,
                metadata: None,
            }))
        }
        "info" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::BranchGet {
                branch: BranchId::from(name),
            }))
        }
        "list" => {
            let limit = m.get_one::<u64>("limit").copied();
            Ok(CliAction::Execute(Command::BranchList {
                state: None,
                limit,
                offset: None,
            }))
        }
        "exists" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::BranchExists {
                branch: BranchId::from(name),
            }))
        }
        "del" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::BranchDelete {
                branch: BranchId::from(name),
            }))
        }
        "fork" => {
            let destination = m.get_one::<String>("dest").unwrap().clone();
            Ok(CliAction::BranchOp(BranchOp::Fork { destination }))
        }
        "diff" => {
            let branch_a = m.get_one::<String>("a").unwrap().clone();
            let branch_b = m.get_one::<String>("b").unwrap().clone();
            Ok(CliAction::BranchOp(BranchOp::Diff { branch_a, branch_b }))
        }
        "merge" => {
            let source = m.get_one::<String>("source").unwrap().clone();
            let strategy = match m.get_one::<String>("strategy").map(|s| s.as_str()) {
                Some("strict") => MergeStrategy::Strict,
                Some("lww") | Some("last-writer-wins") | Some("last_writer_wins") | None => {
                    MergeStrategy::LastWriterWins
                }
                Some(other) => {
                    return Err(format!(
                        "Unknown merge strategy \"{}\". Valid options: lww, strict",
                        other
                    ))
                }
            };
            Ok(CliAction::BranchOp(BranchOp::Merge { source, strategy }))
        }
        "export" => {
            let branch_id = m.get_one::<String>("branch").unwrap().clone();
            let path = m.get_one::<String>("path").unwrap().clone();
            Ok(CliAction::Execute(Command::BranchExport {
                branch_id,
                path,
            }))
        }
        "import" => {
            let path = m.get_one::<String>("path").unwrap().clone();
            Ok(CliAction::Execute(Command::BranchImport { path }))
        }
        "validate" => {
            let path = m.get_one::<String>("path").unwrap().clone();
            Ok(CliAction::Execute(Command::BranchBundleValidate { path }))
        }
        other => Err(unknown_subcommand(
            "branch",
            other,
            &[
                "create", "info", "list", "exists", "del", "fork", "diff", "merge", "export",
                "import", "validate",
            ],
        )),
    }
}

// =========================================================================
// Space
// =========================================================================

fn parse_space(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No space subcommand")?;
    match sub {
        "list" => Ok(CliAction::Execute(Command::SpaceList {
            branch: branch(state),
        })),
        "create" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::SpaceCreate {
                branch: branch(state),
                space: name,
            }))
        }
        "del" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            let force = m.get_flag("force");
            Ok(CliAction::Execute(Command::SpaceDelete {
                branch: branch(state),
                space: name,
                force,
            }))
        }
        "exists" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::SpaceExists {
                branch: branch(state),
                space: name,
            }))
        }
        other => Err(unknown_subcommand(
            "space",
            other,
            &["list", "create", "del", "exists"],
        )),
    }
}

// =========================================================================
// Transaction
// =========================================================================

fn parse_begin(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let read_only = matches.get_flag("txn-read-only");
    Ok(CliAction::Execute(Command::TxnBegin {
        branch: branch(state),
        options: Some(TxnOptions { read_only }),
    }))
}

fn parse_txn(matches: &ArgMatches) -> Result<CliAction, String> {
    let (sub, _) = matches.subcommand().ok_or("No txn subcommand")?;
    match sub {
        "info" => Ok(CliAction::Execute(Command::TxnInfo)),
        "active" => Ok(CliAction::Execute(Command::TxnIsActive)),
        other => Err(unknown_subcommand("txn", other, &["info", "active"])),
    }
}

// =========================================================================
// Search
// =========================================================================

// =========================================================================
// Configure Model
// =========================================================================

fn parse_config(matches: &ArgMatches) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No config subcommand")?;
    match sub {
        "set" => {
            let key = m.get_one::<String>("key").unwrap().clone();
            let value = m.get_one::<String>("value").unwrap().clone();
            Ok(CliAction::Execute(Command::ConfigureSet { key, value }))
        }
        "get" => {
            let key = m.get_one::<String>("key").unwrap().clone();
            Ok(CliAction::Execute(Command::ConfigureGetKey { key }))
        }
        "list" => Ok(CliAction::Execute(Command::ConfigGet)),
        other => Err(unknown_subcommand("config", other, &["set", "get", "list"])),
    }
}

fn parse_recipe(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No recipe subcommand")?;
    match sub {
        "show" => Ok(CliAction::Execute(Command::RecipeGetDefault {
            branch: branch(state),
        })),
        "set" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            let json = m.get_one::<String>("recipe_json").unwrap().clone();
            Ok(CliAction::Execute(Command::RecipeSet {
                branch: branch(state),
                name,
                recipe_json: json,
            }))
        }
        "get" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::RecipeGet {
                branch: branch(state),
                name,
            }))
        }
        "list" => Ok(CliAction::Execute(Command::RecipeList {
            branch: branch(state),
        })),
        other => Err(unknown_subcommand(
            "recipe",
            other,
            &["show", "set", "get", "list"],
        )),
    }
}

fn parse_configure_model(matches: &ArgMatches) -> Result<CliAction, String> {
    let endpoint = matches.get_one::<String>("endpoint").unwrap().clone();
    let model = matches.get_one::<String>("model").unwrap().clone();
    let api_key = matches.get_one::<String>("api-key").cloned();
    let timeout_ms = matches.get_one::<u64>("timeout").copied();
    Ok(CliAction::Execute(Command::ConfigureModel {
        endpoint,
        model,
        api_key,
        timeout_ms,
    }))
}

fn parse_embed(matches: &ArgMatches) -> Result<CliAction, String> {
    let texts: Vec<String> = matches
        .get_many::<String>("texts")
        .ok_or("Missing text argument")?
        .cloned()
        .collect();

    if texts.len() == 1 {
        Ok(CliAction::Execute(Command::Embed {
            text: texts.into_iter().next().unwrap(),
        }))
    } else {
        Ok(CliAction::Execute(Command::EmbedBatch { texts }))
    }
}

fn parse_models(matches: &ArgMatches) -> Result<CliAction, String> {
    let (sub, m) = matches.subcommand().ok_or("No models subcommand")?;
    match sub {
        "list" => Ok(CliAction::Execute(Command::ModelsList)),
        "local" => Ok(CliAction::Execute(Command::ModelsLocal)),
        "pull" => {
            let name = m.get_one::<String>("name").unwrap().clone();
            Ok(CliAction::Execute(Command::ModelsPull { name }))
        }
        other => Err(unknown_subcommand(
            "models",
            other,
            &["list", "local", "pull"],
        )),
    }
}

fn parse_generate(matches: &ArgMatches) -> Result<CliAction, String> {
    let model = matches.get_one::<String>("model").unwrap().clone();
    let prompt = matches.get_one::<String>("prompt").unwrap().clone();
    let max_tokens = matches.get_one::<usize>("max-tokens").copied();
    let temperature = matches.get_one::<f32>("temperature").copied();
    let top_k = matches.get_one::<usize>("top-k").copied();
    let top_p = matches.get_one::<f32>("top-p").copied();
    let seed = matches.get_one::<u64>("seed").copied();
    let stop_sequences: Option<Vec<String>> = matches
        .get_many::<String>("stop")
        .map(|vals| vals.cloned().collect());
    Ok(CliAction::Execute(Command::Generate {
        model,
        prompt,
        max_tokens,
        temperature,
        top_k,
        top_p,
        seed,
        stop_tokens: None,
        stop_sequences,
    }))
}

fn parse_tokenize(matches: &ArgMatches) -> Result<CliAction, String> {
    let model = matches.get_one::<String>("model").unwrap().clone();
    let text = matches.get_one::<String>("text").unwrap().clone();
    let no_special = matches.get_flag("no-special");
    let add_special_tokens = if no_special { Some(false) } else { None };
    Ok(CliAction::Execute(Command::Tokenize {
        model,
        text,
        add_special_tokens,
    }))
}

fn parse_detokenize(matches: &ArgMatches) -> Result<CliAction, String> {
    let model = matches.get_one::<String>("model").unwrap().clone();
    let ids: Vec<u32> = matches
        .get_many::<String>("ids")
        .ok_or("Missing token IDs")?
        .map(|s| s.parse::<u32>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| format!("Invalid token ID: {}", e))?;
    Ok(CliAction::Execute(Command::Detokenize { model, ids }))
}

fn parse_search(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let query = matches.get_one::<String>("query").unwrap().clone();
    let k = matches.get_one::<u64>("top-k").copied();

    Ok(CliAction::Execute(Command::Search {
        branch: branch(state),
        space: space(state),
        search: SearchQuery {
            query,
            recipe: None,
            precomputed_embedding: None,
            k,
        },
    }))
}

fn parse_db_export(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let primitive_str = matches.get_one::<String>("primitive").unwrap();
    let primitive = match primitive_str.as_str() {
        "kv" => ExportPrimitive::Kv,
        "json" => ExportPrimitive::Json,
        "events" => ExportPrimitive::Events,
        "vector" => ExportPrimitive::Vector,
        "graph" => ExportPrimitive::Graph,
        other => {
            return Err(format!(
                "Unknown primitive '{other}'. Expected: kv, json, events, vector, graph"
            ))
        }
    };

    let format_str = matches
        .get_one::<String>("format")
        .map(|s| s.as_str())
        .unwrap_or("csv");
    let format = match format_str {
        "csv" => ExportFormat::Csv,
        "json" => ExportFormat::Json,
        "jsonl" => ExportFormat::Jsonl,
        "parquet" => ExportFormat::Parquet,
        other => {
            return Err(format!(
                "Unknown format '{other}'. Expected: parquet, csv, jsonl, json"
            ))
        }
    };

    Ok(CliAction::Execute(Command::DbExport {
        branch: branch(state),
        space: space(state),
        primitive,
        format,
        prefix: matches.get_one::<String>("prefix").cloned(),
        limit: matches.get_one::<u64>("limit").copied(),
        path: matches.get_one::<String>("output").cloned(),
        collection: matches.get_one::<String>("collection").cloned(),
        graph: matches.get_one::<String>("graph").cloned(),
    }))
}

fn parse_db_import(matches: &ArgMatches, state: &SessionState) -> Result<CliAction, String> {
    let file_path = matches.get_one::<String>("file").unwrap().clone();
    let target = matches.get_one::<String>("into").unwrap().clone();

    Ok(CliAction::Execute(Command::ArrowImport {
        branch: branch(state),
        space: space(state),
        file_path,
        target,
        key_column: matches.get_one::<String>("key-column").cloned(),
        value_column: matches.get_one::<String>("value-column").cloned(),
        collection: matches.get_one::<String>("collection").cloned(),
        format: matches.get_one::<String>("format").cloned(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::build_repl_cmd;

    fn test_state() -> SessionState {
        let db = strata_executor::Strata::cache().unwrap();
        SessionState::new(db, "default".to_string(), "default".to_string())
    }

    fn parse(args: &[&str]) -> Result<CliAction, String> {
        let state = test_state();
        let cmd = build_repl_cmd();
        let matches = cmd.try_get_matches_from(args).map_err(|e| e.to_string())?;
        matches_to_action(&matches, &state)
    }

    fn parse_cmd(args: &[&str]) -> Command {
        match parse(args) {
            Ok(CliAction::Execute(cmd)) => cmd,
            Ok(_) => panic!("Expected CliAction::Execute"),
            Err(e) => panic!("Parse failed: {}", e),
        }
    }

    fn parse_err(args: &[&str]) -> String {
        match parse(args) {
            Err(e) => e,
            Ok(_) => panic!("Expected parse error"),
        }
    }

    // =========================================================================
    // Graph lifecycle
    // =========================================================================

    #[test]
    fn graph_create_minimal() {
        let cmd = parse_cmd(&["graph", "create", "social"]);
        assert_eq!(
            cmd,
            Command::GraphCreate {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                cascade_policy: None,
            }
        );
    }

    #[test]
    fn graph_create_with_cascade_policy() {
        let cmd = parse_cmd(&["graph", "create", "social", "--cascade-policy", "cascade"]);
        assert_eq!(
            cmd,
            Command::GraphCreate {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                cascade_policy: Some("cascade".into()),
            }
        );
    }

    #[test]
    fn graph_delete() {
        let cmd = parse_cmd(&["graph", "delete", "social"]);
        assert_eq!(
            cmd,
            Command::GraphDelete {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    #[test]
    fn graph_list() {
        let cmd = parse_cmd(&["graph", "list"]);
        assert_eq!(
            cmd,
            Command::GraphList {
                branch: Some(BranchId::from("default")),
            }
        );
    }

    #[test]
    fn graph_info() {
        let cmd = parse_cmd(&["graph", "info", "social"]);
        assert_eq!(
            cmd,
            Command::GraphGetMeta {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    // =========================================================================
    // Nodes
    // =========================================================================

    #[test]
    fn graph_add_node_minimal() {
        let cmd = parse_cmd(&["graph", "add-node", "social", "alice"]);
        assert_eq!(
            cmd,
            Command::GraphAddNode {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                node_id: "alice".into(),
                entity_ref: None,
                properties: None,
                object_type: None,
            }
        );
    }

    #[test]
    fn graph_add_node_all_options() {
        let cmd = parse_cmd(&[
            "graph",
            "add-node",
            "social",
            "alice",
            "--entity-ref",
            "kv://main/alice",
            "--properties",
            r#"{"age": 30}"#,
            "--type",
            "Person",
        ]);
        assert_eq!(
            cmd,
            Command::GraphAddNode {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                node_id: "alice".into(),
                entity_ref: Some("kv://main/alice".into()),
                properties: Some(Value::from(serde_json::json!({"age": 30}))),
                object_type: Some("Person".into()),
            }
        );
    }

    #[test]
    fn graph_get_node() {
        let cmd = parse_cmd(&["graph", "get-node", "social", "alice"]);
        assert_eq!(
            cmd,
            Command::GraphGetNode {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                node_id: "alice".into(),
            }
        );
    }

    #[test]
    fn graph_remove_node() {
        let cmd = parse_cmd(&["graph", "remove-node", "social", "alice"]);
        assert_eq!(
            cmd,
            Command::GraphRemoveNode {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                node_id: "alice".into(),
            }
        );
    }

    #[test]
    fn graph_list_nodes() {
        let cmd = parse_cmd(&["graph", "list-nodes", "social"]);
        assert_eq!(
            cmd,
            Command::GraphListNodes {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    // =========================================================================
    // Edges
    // =========================================================================

    #[test]
    fn graph_add_edge_minimal() {
        let cmd = parse_cmd(&["graph", "add-edge", "social", "alice", "bob", "FOLLOWS"]);
        assert_eq!(
            cmd,
            Command::GraphAddEdge {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                src: "alice".into(),
                dst: "bob".into(),
                edge_type: "FOLLOWS".into(),
                weight: None,
                properties: None,
            }
        );
    }

    #[test]
    fn graph_add_edge_with_weight_and_properties() {
        let cmd = parse_cmd(&[
            "graph",
            "add-edge",
            "social",
            "alice",
            "bob",
            "FOLLOWS",
            "--weight",
            "0.85",
            "--properties",
            r#"{"since": "2024"}"#,
        ]);
        assert_eq!(
            cmd,
            Command::GraphAddEdge {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                src: "alice".into(),
                dst: "bob".into(),
                edge_type: "FOLLOWS".into(),
                weight: Some(0.85),
                properties: Some(Value::from(serde_json::json!({"since": "2024"}))),
            }
        );
    }

    #[test]
    fn graph_remove_edge() {
        let cmd = parse_cmd(&["graph", "remove-edge", "social", "alice", "bob", "FOLLOWS"]);
        assert_eq!(
            cmd,
            Command::GraphRemoveEdge {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                src: "alice".into(),
                dst: "bob".into(),
                edge_type: "FOLLOWS".into(),
            }
        );
    }

    #[test]
    fn graph_neighbors_minimal() {
        let cmd = parse_cmd(&["graph", "neighbors", "social", "alice"]);
        assert_eq!(
            cmd,
            Command::GraphNeighbors {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                node_id: "alice".into(),
                direction: None,
                edge_type: None,
            }
        );
    }

    #[test]
    fn graph_neighbors_with_filters() {
        let cmd = parse_cmd(&[
            "graph",
            "neighbors",
            "social",
            "alice",
            "--direction",
            "incoming",
            "--edge-type",
            "FOLLOWS",
        ]);
        assert_eq!(
            cmd,
            Command::GraphNeighbors {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                node_id: "alice".into(),
                direction: Some("incoming".into()),
                edge_type: Some("FOLLOWS".into()),
            }
        );
    }

    // =========================================================================
    // BFS
    // =========================================================================

    #[test]
    fn graph_bfs_minimal() {
        let cmd = parse_cmd(&["graph", "bfs", "social", "alice", "--max-depth", "3"]);
        assert_eq!(
            cmd,
            Command::GraphBfs {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                start: "alice".into(),
                max_depth: 3,
                max_nodes: None,
                edge_types: None,
                direction: None,
            }
        );
    }

    #[test]
    fn graph_bfs_all_options() {
        let cmd = parse_cmd(&[
            "graph",
            "bfs",
            "social",
            "alice",
            "--max-depth",
            "5",
            "--max-nodes",
            "100",
            "--edge-types",
            "FOLLOWS,LIKES",
            "--direction",
            "outgoing",
        ]);
        assert_eq!(
            cmd,
            Command::GraphBfs {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                start: "alice".into(),
                max_depth: 5,
                max_nodes: Some(100),
                edge_types: Some(vec!["FOLLOWS".into(), "LIKES".into()]),
                direction: Some("outgoing".into()),
            }
        );
    }

    #[test]
    fn graph_bfs_invalid_depth() {
        let err = parse_err(&[
            "graph",
            "bfs",
            "social",
            "alice",
            "--max-depth",
            "not_a_number",
        ]);
        assert!(
            err.contains("invalid") || err.contains("Invalid"),
            "got: {}",
            err
        );
    }

    // =========================================================================
    // Bulk insert
    // =========================================================================

    #[test]
    fn graph_bulk_insert_inline() {
        let json = r#"{"nodes":[{"node_id":"a"},{"node_id":"b"}],"edges":[{"src":"a","dst":"b","edge_type":"LINK"}]}"#;
        let cmd = parse_cmd(&["graph", "bulk-insert", "social", json]);
        assert_eq!(
            cmd,
            Command::GraphBulkInsert {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                nodes: vec![
                    BulkGraphNode {
                        node_id: "a".into(),
                        entity_ref: None,
                        properties: None,
                        object_type: None,
                    },
                    BulkGraphNode {
                        node_id: "b".into(),
                        entity_ref: None,
                        properties: None,
                        object_type: None,
                    },
                ],
                edges: vec![BulkGraphEdge {
                    src: "a".into(),
                    dst: "b".into(),
                    edge_type: "LINK".into(),
                    weight: None,
                    properties: None,
                }],
                chunk_size: None,
            }
        );
    }

    #[test]
    fn graph_bulk_insert_nodes_only() {
        let json = r#"{"nodes":[{"node_id":"x"}]}"#;
        let cmd = parse_cmd(&["graph", "bulk-insert", "g", json]);
        match cmd {
            Command::GraphBulkInsert { nodes, edges, .. } => {
                assert_eq!(nodes.len(), 1);
                assert!(edges.is_empty());
            }
            _ => panic!("Expected GraphBulkInsert"),
        }
    }

    #[test]
    fn graph_bulk_insert_edges_only() {
        let json = r#"{"edges":[{"src":"a","dst":"b","edge_type":"E"}]}"#;
        let cmd = parse_cmd(&["graph", "bulk-insert", "g", json]);
        match cmd {
            Command::GraphBulkInsert { nodes, edges, .. } => {
                assert!(nodes.is_empty());
                assert_eq!(edges.len(), 1);
            }
            _ => panic!("Expected GraphBulkInsert"),
        }
    }

    #[test]
    fn graph_bulk_insert_with_chunk_size() {
        let json = r#"{"nodes":[],"edges":[]}"#;
        let cmd = parse_cmd(&["graph", "bulk-insert", "g", json, "--chunk-size", "500"]);
        match cmd {
            Command::GraphBulkInsert { chunk_size, .. } => {
                assert_eq!(chunk_size, Some(500));
            }
            _ => panic!("Expected GraphBulkInsert"),
        }
    }

    #[test]
    fn graph_bulk_insert_invalid_json() {
        let err = parse_err(&["graph", "bulk-insert", "g", "not json"]);
        assert!(err.contains("Invalid JSON"), "got: {}", err);
    }

    #[test]
    fn graph_bulk_insert_invalid_node_schema() {
        let json = r#"{"nodes":[{"bad_field":"x"}]}"#;
        let err = parse_err(&["graph", "bulk-insert", "g", json]);
        assert!(err.contains("Invalid nodes"), "got: {}", err);
    }

    // =========================================================================
    // Ontology — Object Types (via `graph ontology` subcommands)
    // =========================================================================

    #[test]
    fn graph_define_object_type_inline() {
        let json = r#"{"name":"Person","properties":{"age":"int"}}"#;
        let cmd = parse_cmd(&["graph", "ontology", "define", "social", json]);
        assert_eq!(
            cmd,
            Command::GraphDefineObjectType {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                definition: Value::from(
                    serde_json::json!({"name": "Person", "properties": {"age": "int"}})
                ),
            }
        );
    }

    #[test]
    fn graph_get_object_type() {
        let cmd = parse_cmd(&["graph", "ontology", "get", "social", "Person"]);
        assert_eq!(
            cmd,
            Command::GraphGetObjectType {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                name: "Person".into(),
            }
        );
    }

    #[test]
    fn graph_list_object_types() {
        let cmd = parse_cmd(&["graph", "ontology", "list", "social", "--kind", "object"]);
        assert_eq!(
            cmd,
            Command::GraphListObjectTypes {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    #[test]
    fn graph_delete_object_type() {
        let cmd = parse_cmd(&["graph", "ontology", "delete", "social", "Person"]);
        assert_eq!(
            cmd,
            Command::GraphDeleteObjectType {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                name: "Person".into(),
            }
        );
    }

    // =========================================================================
    // Ontology — Link Types (via `graph ontology` subcommands)
    // =========================================================================

    #[test]
    fn graph_define_link_type_inline() {
        let json = r#"{"name":"FOLLOWS","source":"Person","target":"Person"}"#;
        let cmd = parse_cmd(&["graph", "ontology", "define", "social", json]);
        assert_eq!(
            cmd,
            Command::GraphDefineLinkType {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                definition: Value::from(
                    serde_json::json!({"name": "FOLLOWS", "source": "Person", "target": "Person"})
                ),
            }
        );
    }

    #[test]
    fn graph_get_link_type() {
        let cmd = parse_cmd(&[
            "graph", "ontology", "get", "social", "FOLLOWS", "--kind", "link",
        ]);
        assert_eq!(
            cmd,
            Command::GraphGetLinkType {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                name: "FOLLOWS".into(),
            }
        );
    }

    #[test]
    fn graph_list_link_types() {
        let cmd = parse_cmd(&["graph", "ontology", "list", "social", "--kind", "link"]);
        assert_eq!(
            cmd,
            Command::GraphListLinkTypes {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    #[test]
    fn graph_delete_link_type() {
        let cmd = parse_cmd(&[
            "graph", "ontology", "delete", "social", "FOLLOWS", "--kind", "link",
        ]);
        assert_eq!(
            cmd,
            Command::GraphDeleteLinkType {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                name: "FOLLOWS".into(),
            }
        );
    }

    // =========================================================================
    // Ontology — Management (via `graph ontology` subcommands)
    // =========================================================================

    #[test]
    fn graph_freeze_ontology() {
        let cmd = parse_cmd(&["graph", "ontology", "freeze", "social"]);
        assert_eq!(
            cmd,
            Command::GraphFreezeOntology {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    #[test]
    fn graph_ontology_status() {
        let cmd = parse_cmd(&["graph", "ontology", "status", "social"]);
        assert_eq!(
            cmd,
            Command::GraphOntologyStatus {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    #[test]
    fn graph_ontology_summary() {
        let cmd = parse_cmd(&["graph", "ontology", "summary", "social"]);
        assert_eq!(
            cmd,
            Command::GraphOntologySummary {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
            }
        );
    }

    #[test]
    fn graph_nodes_by_type() {
        let cmd = parse_cmd(&["graph", "list-nodes", "social", "--type", "Person"]);
        assert_eq!(
            cmd,
            Command::GraphNodesByType {
                branch: Some(BranchId::from("default")),
                graph: "social".into(),
                object_type: "Person".into(),
            }
        );
    }

    // =========================================================================
    // Error cases
    // =========================================================================

    #[test]
    fn graph_add_edge_invalid_weight() {
        let err = parse_err(&[
            "graph",
            "add-edge",
            "g",
            "a",
            "b",
            "E",
            "--weight",
            "not_a_float",
        ]);
        assert!(
            err.contains("invalid") || err.contains("Invalid"),
            "got: {}",
            err
        );
    }

    #[test]
    fn graph_add_node_invalid_properties_json() {
        let err = parse_err(&["graph", "add-node", "g", "n", "--properties", "not json"]);
        assert!(!err.is_empty());
    }

    #[test]
    fn graph_bfs_invalid_max_nodes() {
        let err = parse_err(&[
            "graph",
            "bfs",
            "g",
            "start",
            "--max-depth",
            "3",
            "--max-nodes",
            "xyz",
        ]);
        assert!(
            err.contains("invalid") || err.contains("Invalid"),
            "got: {}",
            err
        );
    }

    #[test]
    fn graph_bulk_insert_invalid_chunk_size() {
        let json = r#"{"nodes":[]}"#;
        let err = parse_err(&["graph", "bulk-insert", "g", json, "--chunk-size", "abc"]);
        assert!(
            err.contains("invalid") || err.contains("Invalid"),
            "got: {}",
            err
        );
    }

    #[test]
    fn graph_missing_required_arg() {
        // graph create without name should fail at clap level
        assert!(parse(&["graph", "create"]).is_err());
    }

    #[test]
    fn graph_add_edge_missing_edge_type() {
        // only 2 positional args instead of 4
        assert!(parse(&["graph", "add-edge", "g", "a"]).is_err());
    }

    // =========================================================================
    // Edge-type comma splitting
    // =========================================================================

    #[test]
    fn graph_bfs_edge_types_with_spaces() {
        let cmd = parse_cmd(&[
            "graph",
            "bfs",
            "social",
            "alice",
            "--max-depth",
            "2",
            "--edge-types",
            "FOLLOWS, LIKES, BLOCKS",
        ]);
        match cmd {
            Command::GraphBfs { edge_types, .. } => {
                assert_eq!(
                    edge_types,
                    Some(vec![
                        "FOLLOWS".to_string(),
                        "LIKES".to_string(),
                        "BLOCKS".to_string(),
                    ])
                );
            }
            _ => panic!("Expected GraphBfs"),
        }
    }
}
