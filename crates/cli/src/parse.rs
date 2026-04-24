use std::fs;
use std::io::Read;
use std::sync::OnceLock;

use clap::{Arg, ArgAction, ArgMatches, Command as ClapCommand};
use strata_executor::{
    BatchVectorEntry, BranchId, BranchStatus, BulkGraphEdge, BulkGraphNode, Command,
    DistanceMetric, ExportFormat, ExportPrimitive, MergeStrategy, MetadataFilter, ScanDirection,
    SearchQuery, TxnOptions, Value,
};

use crate::context::Context;
use crate::request::{CliRequest, MetaCommand};

pub(crate) fn build_cli() -> ClapCommand {
    register_subcommands(
        ClapCommand::new("strata")
            .about("Thin CLI shell over strata-executor")
            .version(env!("CARGO_PKG_VERSION"))
            .subcommand_required(false)
            .arg(global_db_arg())
            .arg(global_cache_arg())
            .arg(global_branch_arg())
            .arg(global_space_arg())
            .arg(global_json_arg())
            .arg(global_raw_arg())
            .arg(global_read_only_arg())
            .arg(global_follower_arg()),
    )
    .subcommand(build_init())
    .subcommand(build_up())
    .subcommand(build_down())
    .subcommand(build_uninstall())
}

pub(crate) fn build_repl_cli() -> ClapCommand {
    static REPL_CLI: OnceLock<ClapCommand> = OnceLock::new();

    REPL_CLI
        .get_or_init(|| {
            register_subcommands(
                ClapCommand::new("repl")
                    .multicall(true)
                    .subcommand_required(true),
            )
        })
        .clone()
}

fn register_subcommands(cmd: ClapCommand) -> ClapCommand {
    cmd.subcommand(build_ping())
        .subcommand(build_info())
        .subcommand(build_health())
        .subcommand(build_metrics())
        .subcommand(build_flush())
        .subcommand(build_compact())
        .subcommand(build_describe())
        .subcommand(build_durability_counters())
        .subcommand(build_kv())
        .subcommand(build_json())
        .subcommand(build_event())
        .subcommand(build_vector())
        .subcommand(build_graph())
        .subcommand(build_branch())
        .subcommand(build_space())
        .subcommand(build_begin())
        .subcommand(build_commit())
        .subcommand(build_rollback())
        .subcommand(build_txn())
        .subcommand(build_search())
        .subcommand(build_config())
        .subcommand(build_recipe())
        .subcommand(build_configure_model())
        .subcommand(build_embed())
        .subcommand(build_models())
        .subcommand(build_generate())
        .subcommand(build_tokenize())
        .subcommand(build_detokenize())
        .subcommand(build_export())
        .subcommand(build_import())
}

fn build_init() -> ClapCommand {
    ClapCommand::new("init")
        .about("Set up a new Strata database with guided setup")
        .arg(
            Arg::new("non-interactive")
                .long("non-interactive")
                .help("Accept all defaults, skip prompts (for CI and automation)")
                .action(ArgAction::SetTrue),
        )
}

fn build_up() -> ClapCommand {
    ClapCommand::new("up")
        .about("Start IPC server for shared database access")
        .long_about(
            "Start a background daemon that holds the database and listens on a Unix socket.\n\
             Other processes can then access the database concurrently via IPC.\n\
             Similar to `docker compose up -d`.",
        )
        .arg(
            Arg::new("foreground")
                .long("fg")
                .help("Run in foreground (for debugging, systemd, or Docker)")
                .action(ArgAction::SetTrue),
        )
}

fn build_down() -> ClapCommand {
    ClapCommand::new("down")
        .about("Stop the IPC server")
        .long_about(
            "Stop a running Strata IPC server by reading the PID file and sending SIGTERM.\n\
             Cleans up the socket and PID files.",
        )
}

fn build_uninstall() -> ClapCommand {
    ClapCommand::new("uninstall")
        .about("Remove Strata from this system")
        .arg(
            Arg::new("yes")
                .long("yes")
                .short('y')
                .help("Skip confirmation prompt")
                .action(ArgAction::SetTrue),
        )
}

pub(crate) fn matches_to_request(
    matches: &ArgMatches,
    context: &Context,
) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No command provided".to_string())?;

    match name {
        "ping" => Ok(CliRequest::Execute(Command::Ping)),
        "info" => Ok(CliRequest::Execute(Command::Info)),
        "health" => Ok(CliRequest::Execute(Command::Health)),
        "metrics" => Ok(CliRequest::Execute(Command::Metrics)),
        "flush" => Ok(CliRequest::Execute(Command::Flush)),
        "compact" => Ok(CliRequest::Execute(Command::Compact)),
        "describe" => Ok(CliRequest::Execute(Command::Describe {
            branch: branch(context),
        })),
        "durability-counters" => Ok(CliRequest::Execute(Command::DurabilityCounters)),
        "kv" => parse_kv(submatches, context),
        "json" => parse_json(submatches, context),
        "event" => parse_event(submatches, context),
        "vector" => parse_vector(submatches, context),
        "graph" => parse_graph(submatches, context),
        "branch" => parse_branch(submatches, context),
        "space" => parse_space(submatches, context),
        "begin" => parse_begin_request(submatches, context),
        "commit" => Ok(CliRequest::Execute(Command::TxnCommit)),
        "rollback" => Ok(CliRequest::Execute(Command::TxnRollback)),
        "txn" => parse_txn(submatches),
        "search" => parse_search(submatches, context),
        "config" => parse_config(submatches),
        "recipe" => parse_recipe(submatches, context),
        "configure-model" => parse_configure_model(submatches),
        "embed" => parse_embed(submatches),
        "models" => parse_models(submatches),
        "generate" => parse_generate(submatches),
        "tokenize" => parse_tokenize(submatches),
        "detokenize" => parse_detokenize(submatches),
        "export" => parse_export(submatches, context),
        "import" => parse_import(submatches, context),
        other => Err(format!("Unsupported command: {other}")),
    }
}

pub(crate) fn parse_repl_line(line: &str, context: &Context) -> Result<CliRequest, String> {
    let tokens = shlex::split(line).ok_or_else(|| "Invalid quoting".to_string())?;
    if tokens.is_empty() {
        return Err("No command provided".to_string());
    }

    if let Some(meta) = meta_command_from_tokens(&tokens) {
        return Ok(CliRequest::Meta(meta));
    }

    let matches = build_repl_cli()
        .try_get_matches_from(tokens)
        .map_err(|error| error.to_string())?;
    matches_to_request(&matches, context)
}

pub(crate) fn help_text(command: Option<&str>, repl: bool) -> String {
    let mut root = if repl { build_repl_cli() } else { build_cli() };
    let mut buffer = Vec::new();

    if let Some(command) = command {
        if let Some(subcommand) = root.find_subcommand_mut(command) {
            if subcommand.write_long_help(&mut buffer).is_ok() {
                return String::from_utf8_lossy(&buffer).into_owned();
            }
        }
    }

    if root.write_long_help(&mut buffer).is_ok() {
        String::from_utf8_lossy(&buffer).into_owned()
    } else {
        "No help available".to_string()
    }
}

fn parse_kv(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No kv subcommand".to_string())?;
    match name {
        "put" => Ok(CliRequest::Execute(Command::KvPut {
            branch: branch(context),
            space: space(context),
            key: required_string(submatches, "key")?,
            value: parse_value_arg(submatches, "value", "file")?,
        })),
        "get" => Ok(CliRequest::Execute(Command::KvGet {
            branch: branch(context),
            space: space(context),
            key: required_string(submatches, "key")?,
            as_of: optional_u64(submatches, "as-of"),
        })),
        "del" => {
            let keys = required_string_list(submatches, "keys")?;
            if keys.len() == 1 {
                Ok(CliRequest::Execute(Command::KvDelete {
                    branch: branch(context),
                    space: space(context),
                    key: keys[0].clone(),
                }))
            } else {
                Ok(CliRequest::Execute(Command::KvBatchDelete {
                    branch: branch(context),
                    space: space(context),
                    keys,
                }))
            }
        }
        "list" => Ok(CliRequest::Execute(Command::KvList {
            branch: branch(context),
            space: space(context),
            prefix: submatches.get_one::<String>("prefix").cloned(),
            cursor: submatches.get_one::<String>("cursor").cloned(),
            limit: optional_u64(submatches, "limit"),
            as_of: optional_u64(submatches, "as-of"),
        })),
        "scan" => Ok(CliRequest::Execute(Command::KvScan {
            branch: branch(context),
            space: space(context),
            start: submatches.get_one::<String>("start").cloned(),
            limit: optional_u64(submatches, "limit"),
        })),
        "count" => Ok(CliRequest::Execute(Command::KvCount {
            branch: branch(context),
            space: space(context),
            prefix: submatches.get_one::<String>("prefix").cloned(),
        })),
        "history" | "getv" => Ok(CliRequest::Execute(Command::KvGetv {
            branch: branch(context),
            space: space(context),
            key: required_string(submatches, "key")?,
        })),
        _ => Err("Unsupported kv subcommand".to_string()),
    }
}

fn parse_json(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No json subcommand".to_string())?;
    match name {
        "set" => Ok(CliRequest::Execute(Command::JsonSet {
            branch: branch(context),
            space: space(context),
            key: required_string(submatches, "key")?,
            path: required_string(submatches, "path")?,
            value: parse_json_arg(submatches, "value", "file")?,
        })),
        "get" => Ok(CliRequest::Execute(Command::JsonGet {
            branch: branch(context),
            space: space(context),
            key: required_string(submatches, "key")?,
            path: required_string(submatches, "path")?,
            as_of: optional_u64(submatches, "as-of"),
        })),
        "del" => Ok(CliRequest::Execute(Command::JsonDelete {
            branch: branch(context),
            space: space(context),
            key: required_string(submatches, "key")?,
            path: required_string(submatches, "path")?,
        })),
        "list" => Ok(CliRequest::Execute(Command::JsonList {
            branch: branch(context),
            space: space(context),
            prefix: submatches.get_one::<String>("prefix").cloned(),
            cursor: submatches.get_one::<String>("cursor").cloned(),
            limit: submatches.get_one::<u64>("limit").copied().unwrap_or(100),
            as_of: optional_u64(submatches, "as-of"),
        })),
        "count" => Ok(CliRequest::Execute(Command::JsonCount {
            branch: branch(context),
            space: space(context),
            prefix: submatches.get_one::<String>("prefix").cloned(),
        })),
        "history" | "getv" => Ok(CliRequest::Execute(Command::JsonGetv {
            branch: branch(context),
            space: space(context),
            key: required_string(submatches, "key")?,
        })),
        _ => Err("Unsupported json subcommand".to_string()),
    }
}

fn parse_event(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No event subcommand".to_string())?;
    match name {
        "append" => Ok(CliRequest::Execute(Command::EventAppend {
            branch: branch(context),
            space: space(context),
            event_type: required_string(submatches, "event-type")?,
            payload: parse_json_arg(submatches, "payload", "file")?,
        })),
        "get" => Ok(CliRequest::Execute(Command::EventGet {
            branch: branch(context),
            space: space(context),
            sequence: required_u64(submatches, "sequence")?,
            as_of: optional_u64(submatches, "as-of"),
        })),
        "list" => Ok(CliRequest::Execute(Command::EventList {
            branch: branch(context),
            space: space(context),
            event_type: submatches.get_one::<String>("type").cloned(),
            limit: optional_u64(submatches, "limit"),
            as_of: optional_u64(submatches, "as-of"),
        })),
        "len" => Ok(CliRequest::Execute(Command::EventLen {
            branch: branch(context),
            space: space(context),
            as_of: optional_u64(submatches, "as-of"),
        })),
        "range" => Ok(CliRequest::Execute(Command::EventRange {
            branch: branch(context),
            space: space(context),
            start_seq: required_u64(submatches, "start-seq")?,
            end_seq: optional_u64(submatches, "end-seq"),
            limit: optional_u64(submatches, "limit"),
            direction: parse_direction(submatches.get_one::<String>("direction"))?,
            event_type: submatches.get_one::<String>("type").cloned(),
        })),
        "range-time" => Ok(CliRequest::Execute(Command::EventRangeByTime {
            branch: branch(context),
            space: space(context),
            start_ts: required_u64(submatches, "start-ts")?,
            end_ts: optional_u64(submatches, "end-ts"),
            limit: optional_u64(submatches, "limit"),
            direction: parse_direction(submatches.get_one::<String>("direction"))?,
            event_type: submatches.get_one::<String>("type").cloned(),
        })),
        "types" => Ok(CliRequest::Execute(Command::EventListTypes {
            branch: branch(context),
            space: space(context),
            as_of: optional_u64(submatches, "as-of"),
        })),
        _ => Err("Unsupported event subcommand".to_string()),
    }
}

fn parse_vector(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No vector subcommand".to_string())?;
    match name {
        "upsert" => Ok(CliRequest::Execute(Command::VectorUpsert {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
            key: required_string(submatches, "key")?,
            vector: parse_vector_literal(&required_string(submatches, "vector")?)?,
            metadata: optional_json(submatches, "metadata")?,
        })),
        "get" => Ok(CliRequest::Execute(Command::VectorGet {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
            key: required_string(submatches, "key")?,
            as_of: optional_u64(submatches, "as-of"),
        })),
        "del" => Ok(CliRequest::Execute(Command::VectorDelete {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
            key: required_string(submatches, "key")?,
        })),
        "search" => Ok(CliRequest::Execute(Command::VectorQuery {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
            query: parse_vector_literal(&required_string(submatches, "query")?)?,
            k: submatches.get_one::<u64>("k").copied().unwrap_or(10),
            filter: optional_metadata_filters(submatches, "filter")?,
            metric: optional_metric(submatches, "metric")?,
            as_of: optional_u64(submatches, "as-of"),
        })),
        "create" => Ok(CliRequest::Execute(Command::VectorCreateCollection {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
            dimension: required_u64(submatches, "dimension")?,
            metric: parse_metric(submatches.get_one::<String>("metric"))?,
        })),
        "drop" => Ok(CliRequest::Execute(Command::VectorDeleteCollection {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
        })),
        "collections" => Ok(CliRequest::Execute(Command::VectorListCollections {
            branch: branch(context),
            space: space(context),
        })),
        "stats" => Ok(CliRequest::Execute(Command::VectorCollectionStats {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
        })),
        "batch-upsert" => Ok(CliRequest::Execute(Command::VectorBatchUpsert {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
            entries: parse_batch_vector_entries(submatches)?,
        })),
        "history" | "getv" => Ok(CliRequest::Execute(Command::VectorGetv {
            branch: branch(context),
            space: space(context),
            collection: required_string(submatches, "collection")?,
            key: required_string(submatches, "key")?,
        })),
        _ => Err("Unsupported vector subcommand".to_string()),
    }
}

fn parse_graph(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No graph subcommand".to_string())?;
    match name {
        "create" => Ok(CliRequest::Execute(Command::GraphCreate {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            cascade_policy: submatches.get_one::<String>("cascade-policy").cloned(),
        })),
        "delete" => Ok(CliRequest::Execute(Command::GraphDelete {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
        })),
        "list" => Ok(CliRequest::Execute(Command::GraphList {
            branch: branch(context),
            space: space(context),
        })),
        "info" => Ok(CliRequest::Execute(Command::GraphGetMeta {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
        })),
        "add-node" => Ok(CliRequest::Execute(Command::GraphAddNode {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            node_id: required_string(submatches, "node-id")?,
            entity_ref: submatches.get_one::<String>("entity-ref").cloned(),
            properties: optional_json(submatches, "properties")?,
            object_type: submatches.get_one::<String>("object-type").cloned(),
        })),
        "get-node" => Ok(CliRequest::Execute(Command::GraphGetNode {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            node_id: required_string(submatches, "node-id")?,
            as_of: optional_u64(submatches, "as-of"),
        })),
        "remove-node" => Ok(CliRequest::Execute(Command::GraphRemoveNode {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            node_id: required_string(submatches, "node-id")?,
        })),
        "list-nodes" => {
            let graph = required_string(submatches, "graph")?;
            let object_type = submatches.get_one::<String>("type").cloned();
            let limit = submatches.get_one::<usize>("limit").copied();
            let cursor = submatches.get_one::<String>("cursor").cloned();
            let as_of = optional_u64(submatches, "as-of");

            if let Some(object_type) = object_type {
                if limit.is_some() || cursor.is_some() || as_of.is_some() {
                    return Err(
                        "--type cannot be combined with --limit, --cursor, or --as-of".to_string(),
                    );
                }
                Ok(CliRequest::Execute(Command::GraphNodesByType {
                    branch: branch(context),
                    space: space(context),
                    graph,
                    object_type,
                }))
            } else if limit.is_some() || cursor.is_some() {
                if as_of.is_some() {
                    return Err("--limit/--cursor cannot be combined with --as-of".to_string());
                }
                Ok(CliRequest::Execute(Command::GraphListNodesPaginated {
                    branch: branch(context),
                    space: space(context),
                    graph,
                    limit: limit.unwrap_or(100),
                    cursor,
                }))
            } else {
                Ok(CliRequest::Execute(Command::GraphListNodes {
                    branch: branch(context),
                    space: space(context),
                    graph,
                    as_of,
                }))
            }
        }
        "add-edge" => Ok(CliRequest::Execute(Command::GraphAddEdge {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            src: required_string(submatches, "src")?,
            dst: required_string(submatches, "dst")?,
            edge_type: required_string(submatches, "edge-type")?,
            weight: submatches.get_one::<f64>("weight").copied(),
            properties: optional_json(submatches, "properties")?,
        })),
        "remove-edge" => Ok(CliRequest::Execute(Command::GraphRemoveEdge {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            src: required_string(submatches, "src")?,
            dst: required_string(submatches, "dst")?,
            edge_type: required_string(submatches, "edge-type")?,
        })),
        "neighbors" => Ok(CliRequest::Execute(Command::GraphNeighbors {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            node_id: required_string(submatches, "node-id")?,
            direction: submatches.get_one::<String>("direction").cloned(),
            edge_type: submatches.get_one::<String>("edge-type").cloned(),
            as_of: optional_u64(submatches, "as-of"),
        })),
        "bulk-insert" => Ok(CliRequest::Execute(Command::GraphBulkInsert {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            nodes: parse_bulk_graph_nodes(submatches)?,
            edges: parse_bulk_graph_edges(submatches)?,
            chunk_size: submatches.get_one::<usize>("chunk-size").copied(),
        })),
        "bfs" => Ok(CliRequest::Execute(Command::GraphBfs {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            start: required_string(submatches, "start-node")?,
            max_depth: submatches
                .get_one::<usize>("max-depth")
                .copied()
                .unwrap_or(10),
            max_nodes: submatches.get_one::<usize>("max-nodes").copied(),
            edge_types: string_list(submatches, "edge-type"),
            direction: submatches.get_one::<String>("direction").cloned(),
        })),
        "ontology" => parse_graph_ontology(submatches, context),
        "analytics" => parse_graph_analytics(submatches, context),
        _ => Err("Unsupported graph subcommand".to_string()),
    }
}

fn parse_graph_ontology(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No graph ontology subcommand".to_string())?;
    match name {
        "status" => Ok(CliRequest::Execute(Command::GraphOntologyStatus {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
        })),
        "summary" => Ok(CliRequest::Execute(Command::GraphOntologySummary {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
        })),
        "freeze" => Ok(CliRequest::Execute(Command::GraphFreezeOntology {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
        })),
        "define" => {
            let definition = parse_json_value(&required_string(submatches, "definition-json")?)?;
            let is_link = matches!(
                definition,
                Value::Object(ref map) if map.contains_key("source") && map.contains_key("target")
            );
            if is_link {
                Ok(CliRequest::Execute(Command::GraphDefineLinkType {
                    branch: branch(context),
                    space: space(context),
                    graph: required_string(submatches, "graph")?,
                    definition,
                }))
            } else {
                Ok(CliRequest::Execute(Command::GraphDefineObjectType {
                    branch: branch(context),
                    space: space(context),
                    graph: required_string(submatches, "graph")?,
                    definition,
                }))
            }
        }
        "get" => match submatches.get_one::<String>("kind").map(String::as_str) {
            Some("object") => Ok(CliRequest::Execute(Command::GraphGetObjectType {
                branch: branch(context),
                space: space(context),
                graph: required_string(submatches, "graph")?,
                name: required_string(submatches, "name")?,
            })),
            Some("link") => Ok(CliRequest::Execute(Command::GraphGetLinkType {
                branch: branch(context),
                space: space(context),
                graph: required_string(submatches, "graph")?,
                name: required_string(submatches, "name")?,
            })),
            None => Err("--kind is required for `graph ontology get`.".to_string()),
            Some(other) => Err(format!("Invalid --kind '{other}'. Use 'object' or 'link'.")),
        },
        "list" => match submatches.get_one::<String>("kind").map(String::as_str) {
            Some("object") => Ok(CliRequest::Execute(Command::GraphListObjectTypes {
                branch: branch(context),
                space: space(context),
                graph: required_string(submatches, "graph")?,
            })),
            Some("link") => Ok(CliRequest::Execute(Command::GraphListLinkTypes {
                branch: branch(context),
                space: space(context),
                graph: required_string(submatches, "graph")?,
            })),
            None => Ok(CliRequest::Execute(Command::GraphListOntologyTypes {
                branch: branch(context),
                space: space(context),
                graph: required_string(submatches, "graph")?,
            })),
            Some(other) => Err(format!("Invalid --kind '{other}'. Use 'object' or 'link'.")),
        },
        "delete" => match submatches.get_one::<String>("kind").map(String::as_str) {
            Some("object") => Ok(CliRequest::Execute(Command::GraphDeleteObjectType {
                branch: branch(context),
                space: space(context),
                graph: required_string(submatches, "graph")?,
                name: required_string(submatches, "name")?,
            })),
            Some("link") => Ok(CliRequest::Execute(Command::GraphDeleteLinkType {
                branch: branch(context),
                space: space(context),
                graph: required_string(submatches, "graph")?,
                name: required_string(submatches, "name")?,
            })),
            None => Err("--kind is required for `graph ontology delete`.".to_string()),
            Some(other) => Err(format!("Invalid --kind '{other}'. Use 'object' or 'link'.")),
        },
        _ => Err("Unsupported graph ontology subcommand".to_string()),
    }
}

fn parse_graph_analytics(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No graph analytics subcommand".to_string())?;
    match name {
        "wcc" => Ok(CliRequest::Execute(Command::GraphWcc {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            top_n: submatches.get_one::<usize>("top").copied(),
            include_all: flag_option(submatches, "all"),
        })),
        "cdlp" => Ok(CliRequest::Execute(Command::GraphCdlp {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            max_iterations: submatches
                .get_one::<usize>("max-iterations")
                .copied()
                .unwrap_or(20),
            direction: submatches.get_one::<String>("direction").cloned(),
            top_n: submatches.get_one::<usize>("top").copied(),
            include_all: flag_option(submatches, "all"),
        })),
        "pagerank" => Ok(CliRequest::Execute(Command::GraphPagerank {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            damping: submatches.get_one::<f64>("damping").copied(),
            max_iterations: submatches.get_one::<usize>("max-iterations").copied(),
            tolerance: submatches.get_one::<f64>("tolerance").copied(),
            top_n: submatches.get_one::<usize>("top").copied(),
            include_all: flag_option(submatches, "all"),
        })),
        "lcc" => Ok(CliRequest::Execute(Command::GraphLcc {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            top_n: submatches.get_one::<usize>("top").copied(),
            include_all: flag_option(submatches, "all"),
        })),
        "sssp" => Ok(CliRequest::Execute(Command::GraphSssp {
            branch: branch(context),
            space: space(context),
            graph: required_string(submatches, "graph")?,
            source: required_string(submatches, "source-node")?,
            direction: submatches.get_one::<String>("direction").cloned(),
            top_n: submatches.get_one::<usize>("top").copied(),
            include_all: flag_option(submatches, "all"),
        })),
        _ => Err("Unsupported graph analytics subcommand".to_string()),
    }
}

fn parse_branch(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No branch subcommand".to_string())?;
    match name {
        "create" => Ok(CliRequest::Execute(Command::BranchCreate {
            branch_id: submatches.get_one::<String>("name").cloned(),
            metadata: None,
        })),
        "info" => Ok(CliRequest::Execute(Command::BranchGet {
            branch: required_string(submatches, "name")?.into(),
        })),
        "list" => Ok(CliRequest::Execute(Command::BranchList {
            state: None::<BranchStatus>,
            limit: optional_u64(submatches, "limit"),
            offset: optional_u64(submatches, "offset"),
        })),
        "exists" => Ok(CliRequest::Execute(Command::BranchExists {
            branch: required_string(submatches, "name")?.into(),
        })),
        "del" => Ok(CliRequest::Execute(Command::BranchDelete {
            branch: required_string(submatches, "name")?.into(),
        })),
        "fork" => Ok(CliRequest::Execute(Command::BranchFork {
            source: branch(context)
                .map_or_else(|| "default".to_string(), |value| value.to_string()),
            destination: required_string(submatches, "destination")?,
            message: submatches.get_one::<String>("message").cloned(),
            creator: None,
        })),
        "diff" => Ok(CliRequest::Execute(Command::BranchDiff {
            branch_a: required_string(submatches, "branch-a")?,
            branch_b: required_string(submatches, "branch-b")?,
            filter_primitives: None,
            filter_spaces: string_list(submatches, "space"),
            as_of: optional_u64(submatches, "as-of"),
        })),
        "merge" => Ok(CliRequest::Execute(Command::BranchMerge {
            source: required_string(submatches, "source")?,
            target: branch(context)
                .map_or_else(|| "default".to_string(), |value| value.to_string()),
            strategy: parse_merge_strategy(submatches.get_one::<String>("strategy"))?,
            message: submatches.get_one::<String>("message").cloned(),
            creator: None,
        })),
        "merge-base" => Ok(CliRequest::Execute(Command::BranchMergeBase {
            branch_a: required_string(submatches, "branch-a")?,
            branch_b: required_string(submatches, "branch-b")?,
        })),
        "diff3" => Ok(CliRequest::Execute(Command::BranchDiffThreeWay {
            branch_a: required_string(submatches, "branch-a")?,
            branch_b: required_string(submatches, "branch-b")?,
        })),
        "revert" => Ok(CliRequest::Execute(Command::BranchRevert {
            branch: required_string(submatches, "branch")?,
            from_version: required_u64(submatches, "from-version")?,
            to_version: required_u64(submatches, "to-version")?,
        })),
        "cherry-pick" => {
            let keys = parse_space_key_pairs(submatches, "pick")?;
            let filter_spaces = string_list(submatches, "space");
            let filter_keys = string_list(submatches, "key");
            if keys.is_some() && (filter_spaces.is_some() || filter_keys.is_some()) {
                return Err(
                    "--pick cannot be combined with --space or --key filter arguments".to_string(),
                );
            }
            Ok(CliRequest::Execute(Command::BranchCherryPick {
                source: required_string(submatches, "source")?,
                target: required_string(submatches, "target")?,
                keys,
                filter_spaces,
                filter_keys,
                filter_primitives: None,
            }))
        }
        "export" => Ok(CliRequest::Execute(Command::BranchExport {
            branch_id: required_string(submatches, "branch")?,
            path: required_string(submatches, "path")?,
        })),
        "import" => Ok(CliRequest::Execute(Command::BranchImport {
            path: required_string(submatches, "path")?,
        })),
        "validate" => Ok(CliRequest::Execute(Command::BranchBundleValidate {
            path: required_string(submatches, "path")?,
        })),
        _ => Err("Unsupported branch subcommand".to_string()),
    }
}

fn parse_space(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No space subcommand".to_string())?;
    match name {
        "list" => Ok(CliRequest::Execute(Command::SpaceList {
            branch: branch(context),
        })),
        "create" => Ok(CliRequest::Execute(Command::SpaceCreate {
            branch: branch(context),
            space: required_string(submatches, "name")?,
        })),
        "del" => Ok(CliRequest::Execute(Command::SpaceDelete {
            branch: branch(context),
            space: required_string(submatches, "name")?,
            force: submatches.get_flag("force"),
        })),
        "exists" => Ok(CliRequest::Execute(Command::SpaceExists {
            branch: branch(context),
            space: required_string(submatches, "name")?,
        })),
        _ => Err("Unsupported space subcommand".to_string()),
    }
}

fn parse_begin_request(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    Ok(CliRequest::Execute(Command::TxnBegin {
        branch: branch(context),
        options: if matches.get_flag("read-only") {
            Some(TxnOptions { read_only: true })
        } else {
            None
        },
    }))
}

fn parse_txn(matches: &ArgMatches) -> Result<CliRequest, String> {
    let (name, _submatches) = matches
        .subcommand()
        .ok_or_else(|| "No txn subcommand".to_string())?;
    match name {
        "info" => Ok(CliRequest::Execute(Command::TxnInfo)),
        "active" => Ok(CliRequest::Execute(Command::TxnIsActive)),
        _ => Err("Unsupported txn subcommand".to_string()),
    }
}

fn parse_search(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let recipe = matches
        .get_one::<String>("recipe")
        .map(|value| parse_recipe_value(value))
        .transpose()?;
    let diff_start = optional_u64(matches, "diff-start");
    let diff_end = optional_u64(matches, "diff-end");
    let diff = match (diff_start, diff_end) {
        (Some(start), Some(end)) => Some((start, end)),
        (None, None) => None,
        _ => return Err("Both --diff-start and --diff-end are required together".to_string()),
    };

    Ok(CliRequest::Execute(Command::Search {
        branch: branch(context),
        space: space(context),
        search: SearchQuery {
            query: required_string(matches, "query")?,
            recipe,
            precomputed_embedding: None,
            k: optional_u64(matches, "k"),
            as_of: optional_u64(matches, "as-of"),
            diff,
        },
    }))
}

fn parse_config(matches: &ArgMatches) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No config subcommand".to_string())?;
    match name {
        "set" => Ok(CliRequest::Execute(Command::ConfigureSet {
            key: required_string(submatches, "key")?,
            value: required_string(submatches, "value")?,
        })),
        "get" => Ok(CliRequest::Execute(Command::ConfigureGetKey {
            key: required_string(submatches, "key")?,
        })),
        "list" => Ok(CliRequest::Execute(Command::ConfigGet)),
        _ => Err("Unsupported config subcommand".to_string()),
    }
}

fn parse_recipe(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No recipe subcommand".to_string())?;
    match name {
        "show" => Ok(CliRequest::Execute(Command::RecipeGetDefault {
            branch: branch(context),
        })),
        "get" => Ok(CliRequest::Execute(Command::RecipeGet {
            branch: branch(context),
            name: submatches
                .get_one::<String>("name")
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
        })),
        "set" => Ok(CliRequest::Execute(Command::RecipeSet {
            branch: branch(context),
            name: submatches
                .get_one::<String>("name")
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
            recipe_json: required_string(submatches, "recipe-json")?,
        })),
        "list" => Ok(CliRequest::Execute(Command::RecipeList {
            branch: branch(context),
        })),
        "seed" => Ok(CliRequest::Execute(Command::RecipeSeed)),
        "delete" => Ok(CliRequest::Execute(Command::RecipeDelete {
            branch: branch(context),
            name: submatches
                .get_one::<String>("name")
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
        })),
        _ => Err("Unsupported recipe subcommand".to_string()),
    }
}

fn parse_configure_model(matches: &ArgMatches) -> Result<CliRequest, String> {
    Ok(CliRequest::Execute(Command::ConfigureModel {
        endpoint: required_string(matches, "endpoint")?,
        model: required_string(matches, "model")?,
        api_key: matches.get_one::<String>("api-key").cloned(),
        timeout_ms: optional_u64(matches, "timeout-ms"),
    }))
}

fn parse_embed(matches: &ArgMatches) -> Result<CliRequest, String> {
    Ok(CliRequest::Execute(Command::Embed {
        text: required_string(matches, "text")?,
    }))
}

fn parse_models(matches: &ArgMatches) -> Result<CliRequest, String> {
    let (name, submatches) = matches
        .subcommand()
        .ok_or_else(|| "No models subcommand".to_string())?;
    match name {
        "list" => Ok(CliRequest::Execute(Command::ModelsList)),
        "local" => Ok(CliRequest::Execute(Command::ModelsLocal)),
        "pull" => Ok(CliRequest::Execute(Command::ModelsPull {
            name: required_string(submatches, "name")?,
        })),
        _ => Err("Unsupported models subcommand".to_string()),
    }
}

fn parse_generate(matches: &ArgMatches) -> Result<CliRequest, String> {
    Ok(CliRequest::Execute(Command::Generate {
        model: required_string(matches, "model")?,
        prompt: required_string(matches, "prompt")?,
        max_tokens: matches.get_one::<usize>("max-tokens").copied(),
        temperature: matches.get_one::<f32>("temperature").copied(),
        top_k: matches.get_one::<usize>("top-k").copied(),
        top_p: matches.get_one::<f32>("top-p").copied(),
        seed: optional_u64(matches, "seed"),
        stop_tokens: matches
            .get_many::<u32>("stop-token")
            .map(|values| values.copied().collect()),
        stop_sequences: matches
            .get_many::<String>("stop-sequence")
            .map(|values| values.cloned().collect()),
    }))
}

fn parse_tokenize(matches: &ArgMatches) -> Result<CliRequest, String> {
    Ok(CliRequest::Execute(Command::Tokenize {
        model: required_string(matches, "model")?,
        text: required_string(matches, "text")?,
        add_special_tokens: Some(!matches.get_flag("no-special-tokens")),
    }))
}

fn parse_detokenize(matches: &ArgMatches) -> Result<CliRequest, String> {
    let ids = matches
        .get_many::<u32>("ids")
        .ok_or_else(|| "Missing token ids".to_string())?
        .copied()
        .collect();
    Ok(CliRequest::Execute(Command::Detokenize {
        model: required_string(matches, "model")?,
        ids,
    }))
}

fn parse_export(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    Ok(CliRequest::Execute(Command::DbExport {
        branch: branch(context),
        space: space(context),
        primitive: parse_export_primitive(matches.get_one::<String>("primitive"))?,
        format: parse_export_format(matches.get_one::<String>("format"))?,
        prefix: matches.get_one::<String>("prefix").cloned(),
        limit: optional_u64(matches, "limit"),
        path: matches.get_one::<String>("path").cloned(),
        collection: matches.get_one::<String>("collection").cloned(),
        graph: matches.get_one::<String>("graph").cloned(),
    }))
}

fn parse_import(matches: &ArgMatches, context: &Context) -> Result<CliRequest, String> {
    Ok(CliRequest::Execute(Command::ArrowImport {
        branch: branch(context),
        space: space(context),
        file_path: required_string(matches, "file-path")?,
        target: required_string(matches, "target")?,
        key_column: matches.get_one::<String>("key-column").cloned(),
        value_column: matches.get_one::<String>("value-column").cloned(),
        collection: matches.get_one::<String>("collection").cloned(),
        format: matches.get_one::<String>("format").cloned(),
    }))
}

#[cfg(test)]
pub(crate) fn check_meta_command(line: &str) -> Option<MetaCommand> {
    let tokens = shlex::split(line.trim())?;
    meta_command_from_tokens(&tokens)
}

fn meta_command_from_tokens(tokens: &[String]) -> Option<MetaCommand> {
    let command = tokens.first()?.as_str();
    match command {
        "quit" | "exit" => Some(MetaCommand::Quit),
        "clear" => Some(MetaCommand::Clear),
        "help" => Some(MetaCommand::Help {
            command: tokens.get(1).cloned(),
        }),
        "use" => Some(MetaCommand::Use {
            branch: tokens.get(1)?.clone(),
            space: tokens.get(2).cloned(),
        }),
        _ => None,
    }
}

fn branch(context: &Context) -> Option<BranchId> {
    Some(context.branch().into())
}

fn space(context: &Context) -> Option<String> {
    Some(context.space().to_string())
}

fn required_string(matches: &ArgMatches, key: &str) -> Result<String, String> {
    matches
        .get_one::<String>(key)
        .cloned()
        .ok_or_else(|| format!("Missing required argument: {key}"))
}

fn required_u64(matches: &ArgMatches, key: &str) -> Result<u64, String> {
    matches
        .get_one::<u64>(key)
        .copied()
        .ok_or_else(|| format!("Missing required argument: {key}"))
}

fn required_string_list(matches: &ArgMatches, key: &str) -> Result<Vec<String>, String> {
    matches
        .get_many::<String>(key)
        .map(|values| values.cloned().collect())
        .ok_or_else(|| format!("Missing required argument: {key}"))
}

fn optional_u64(matches: &ArgMatches, key: &str) -> Option<u64> {
    matches.get_one::<u64>(key).copied()
}

fn string_list(matches: &ArgMatches, key: &str) -> Option<Vec<String>> {
    matches
        .get_many::<String>(key)
        .map(|values| values.cloned().collect())
}

fn flag_option(matches: &ArgMatches, key: &str) -> Option<bool> {
    matches.get_flag(key).then_some(true)
}

fn parse_value_arg(matches: &ArgMatches, value_key: &str, file_key: &str) -> Result<Value, String> {
    if let Some(file_path) = matches.get_one::<String>(file_key) {
        return read_value_from_source(file_path);
    }
    let value = matches
        .get_one::<String>(value_key)
        .ok_or_else(|| format!("Missing required argument: {value_key}"))?;
    Ok(parse_value(value))
}

fn parse_json_arg(matches: &ArgMatches, value_key: &str, file_key: &str) -> Result<Value, String> {
    if let Some(file_path) = matches.get_one::<String>(file_key) {
        return read_json_from_source(file_path);
    }
    let value = matches
        .get_one::<String>(value_key)
        .ok_or_else(|| format!("Missing required argument: {value_key}"))?;
    parse_json_value(value)
}

fn optional_json(matches: &ArgMatches, key: &str) -> Result<Option<Value>, String> {
    matches
        .get_one::<String>(key)
        .map(|value| parse_json_value(value))
        .transpose()
}

fn parse_direction(value: Option<&String>) -> Result<ScanDirection, String> {
    match value.map(String::as_str) {
        None | Some("forward") => Ok(ScanDirection::Forward),
        Some("reverse") => Ok(ScanDirection::Reverse),
        Some(other) => Err(format!("Unsupported direction: {other}")),
    }
}

fn parse_metric_str(value: &str) -> Result<DistanceMetric, String> {
    match value {
        "cosine" => Ok(DistanceMetric::Cosine),
        "euclidean" => Ok(DistanceMetric::Euclidean),
        "dot-product" => Ok(DistanceMetric::DotProduct),
        other => Err(format!("Unsupported metric: {other}")),
    }
}

fn parse_metric(value: Option<&String>) -> Result<DistanceMetric, String> {
    match value {
        None => Ok(DistanceMetric::Cosine),
        Some(value) => parse_metric_str(value),
    }
}

fn parse_merge_strategy(value: Option<&String>) -> Result<MergeStrategy, String> {
    match value.map(String::as_str) {
        None | Some("lww" | "last-writer-wins" | "last_writer_wins") => {
            Ok(MergeStrategy::LastWriterWins)
        }
        Some("strict") => Ok(MergeStrategy::Strict),
        Some(other) => Err(format!("Unsupported merge strategy: {other}")),
    }
}

fn optional_metric(matches: &ArgMatches, key: &str) -> Result<Option<DistanceMetric>, String> {
    matches
        .get_one::<String>(key)
        .map(|value| parse_metric_str(value))
        .transpose()
}

fn optional_metadata_filters(
    matches: &ArgMatches,
    key: &str,
) -> Result<Option<Vec<MetadataFilter>>, String> {
    matches
        .get_one::<String>(key)
        .map(|value| {
            serde_json::from_str(value).map_err(|error| format!("Invalid filter JSON: {error}"))
        })
        .transpose()
}

fn parse_batch_vector_entries(matches: &ArgMatches) -> Result<Vec<BatchVectorEntry>, String> {
    let json = if let Some(file_path) = matches.get_one::<String>("file") {
        read_string_from_source(file_path)?
    } else {
        required_string(matches, "entries")?
    };
    serde_json::from_str(&json).map_err(|error| format!("Invalid vector entry JSON: {error}"))
}

fn parse_space_key_pairs(
    matches: &ArgMatches,
    key: &str,
) -> Result<Option<Vec<(String, String)>>, String> {
    let values = match matches.get_many::<String>(key) {
        Some(values) => values.cloned().collect::<Vec<_>>(),
        None => return Ok(None),
    };
    if values.len() % 2 != 0 {
        return Err(format!("Argument '{key}' requires space/key pairs"));
    }
    let pairs = values
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect::<Vec<_>>();
    Ok(Some(pairs))
}

fn parse_bulk_graph_nodes(matches: &ArgMatches) -> Result<Vec<BulkGraphNode>, String> {
    let json = if let Some(file_path) = matches.get_one::<String>("nodes-file") {
        read_string_from_source(file_path)?
    } else {
        matches
            .get_one::<String>("nodes")
            .cloned()
            .unwrap_or_else(|| "[]".to_string())
    };
    serde_json::from_str(&json).map_err(|error| format!("Invalid graph node JSON: {error}"))
}

fn parse_bulk_graph_edges(matches: &ArgMatches) -> Result<Vec<BulkGraphEdge>, String> {
    let json = if let Some(file_path) = matches.get_one::<String>("edges-file") {
        read_string_from_source(file_path)?
    } else {
        matches
            .get_one::<String>("edges")
            .cloned()
            .unwrap_or_else(|| "[]".to_string())
    };
    serde_json::from_str(&json).map_err(|error| format!("Invalid graph edge JSON: {error}"))
}

fn parse_recipe_value(value: &str) -> Result<serde_json::Value, String> {
    if value.starts_with('{') || value.starts_with('[') || value.starts_with('"') {
        serde_json::from_str(value).map_err(|error| format!("Invalid recipe JSON: {error}"))
    } else {
        Ok(serde_json::Value::String(value.to_string()))
    }
}

fn parse_export_format(value: Option<&String>) -> Result<ExportFormat, String> {
    match value.map(String::as_str) {
        None | Some("json") => Ok(ExportFormat::Json),
        Some("jsonl") => Ok(ExportFormat::Jsonl),
        Some("csv") => Ok(ExportFormat::Csv),
        Some("parquet") => Ok(ExportFormat::Parquet),
        Some(other) => Err(format!("Unsupported export format: {other}")),
    }
}

fn parse_export_primitive(value: Option<&String>) -> Result<ExportPrimitive, String> {
    match value.map(String::as_str) {
        Some("kv") => Ok(ExportPrimitive::Kv),
        Some("json") => Ok(ExportPrimitive::Json),
        Some("events") => Ok(ExportPrimitive::Events),
        Some("graph") => Ok(ExportPrimitive::Graph),
        Some("vector") => Ok(ExportPrimitive::Vector),
        Some(other) => Err(format!("Unsupported export primitive: {other}")),
        None => Err("Missing required argument: primitive".to_string()),
    }
}

fn read_value_from_source(source: &str) -> Result<Value, String> {
    let content = read_string_from_source(source)?;
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
        return Ok(Value::from(json));
    }
    Ok(Value::String(content.trim().to_string()))
}

fn read_json_from_source(source: &str) -> Result<Value, String> {
    let content = read_string_from_source(source)?;
    parse_json_value(&content)
}

fn read_string_from_source(source: &str) -> Result<String, String> {
    if source == "-" {
        let mut buffer = String::new();
        std::io::stdin()
            .read_to_string(&mut buffer)
            .map_err(|error| format!("Failed to read stdin: {error}"))?;
        Ok(buffer)
    } else {
        fs::read_to_string(source).map_err(|error| format!("Failed to read {source}: {error}"))
    }
}

fn parse_value(value: &str) -> Value {
    if value.starts_with('{') || value.starts_with('[') || value.starts_with('"') {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(value) {
            return Value::from(json);
        }
    }
    if value == "null" {
        return Value::Null;
    }
    if value == "true" {
        return Value::Bool(true);
    }
    if value == "false" {
        return Value::Bool(false);
    }
    if let Ok(number) = value.parse::<i64>() {
        return Value::Int(number);
    }
    if value.contains(['.', 'e', 'E']) {
        if let Ok(number) = value.parse::<f64>() {
            return Value::Float(number);
        }
    }
    Value::String(value.to_string())
}

fn parse_json_value(value: &str) -> Result<Value, String> {
    serde_json::from_str::<serde_json::Value>(value)
        .map(Value::from)
        .map_err(|error| format!("Invalid JSON: {error}"))
}

fn parse_vector_literal(value: &str) -> Result<Vec<f32>, String> {
    let json: serde_json::Value =
        serde_json::from_str(value).map_err(|error| format!("Invalid vector JSON: {error}"))?;
    match json {
        serde_json::Value::Array(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .as_f64()
                    .map(|number| number as f32)
                    .ok_or_else(|| format!("Vector element {index} is not numeric"))
            })
            .collect(),
        _ => Err("Vector must be a JSON array".to_string()),
    }
}

fn global_db_arg() -> Arg {
    Arg::new("db")
        .long("db")
        .value_name("PATH")
        .help("Database path")
        .global(true)
}

fn global_cache_arg() -> Arg {
    Arg::new("cache")
        .long("cache")
        .help("Use an ephemeral in-memory database")
        .action(ArgAction::SetTrue)
        .conflicts_with("db")
        .global(true)
}

fn global_branch_arg() -> Arg {
    Arg::new("branch")
        .long("branch")
        .short('b')
        .value_name("NAME")
        .help("Initial branch")
        .global(true)
}

fn global_space_arg() -> Arg {
    Arg::new("space")
        .long("space")
        .short('s')
        .value_name("NAME")
        .help("Initial space")
        .global(true)
}

fn global_json_arg() -> Arg {
    Arg::new("json")
        .long("json")
        .short('j')
        .help("Pretty JSON output")
        .action(ArgAction::SetTrue)
        .conflicts_with("raw")
        .global(true)
}

fn global_raw_arg() -> Arg {
    Arg::new("raw")
        .long("raw")
        .short('r')
        .help("Minimal output for scripts")
        .action(ArgAction::SetTrue)
        .global(true)
}

fn global_read_only_arg() -> Arg {
    Arg::new("read-only")
        .long("read-only")
        .help("Open database in read-only mode")
        .action(ArgAction::SetTrue)
        .global(true)
}

fn global_follower_arg() -> Arg {
    Arg::new("follower")
        .long("follower")
        .help("Open as read-only follower")
        .action(ArgAction::SetTrue)
        .conflicts_with("cache")
        .global(true)
}

fn build_ping() -> ClapCommand {
    ClapCommand::new("ping").about("Check connectivity")
}

fn build_info() -> ClapCommand {
    ClapCommand::new("info").about("Database info")
}

fn build_health() -> ClapCommand {
    ClapCommand::new("health").about("Health report")
}

fn build_metrics() -> ClapCommand {
    ClapCommand::new("metrics").about("Database metrics")
}

fn build_flush() -> ClapCommand {
    ClapCommand::new("flush").about("Flush pending writes")
}

fn build_compact() -> ClapCommand {
    ClapCommand::new("compact").about("Trigger compaction")
}

fn build_describe() -> ClapCommand {
    ClapCommand::new("describe").about("Structured database snapshot")
}

fn build_durability_counters() -> ClapCommand {
    ClapCommand::new("durability-counters").about("WAL durability counters")
}

fn build_kv() -> ClapCommand {
    ClapCommand::new("kv")
        .about("Key-value operations")
        .subcommand_required(true)
        .subcommand(
            ClapCommand::new("put")
                .about("Write a key")
                .arg(Arg::new("key").required(true))
                .arg(Arg::new("value"))
                .arg(Arg::new("file").long("file").short('f').value_name("PATH")),
        )
        .subcommand(
            ClapCommand::new("get")
                .about("Read a key")
                .arg(Arg::new("key").required(true))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("del")
                .about("Delete one or more keys")
                .arg(Arg::new("keys").required(true).num_args(1..)),
        )
        .subcommand(
            ClapCommand::new("list")
                .about("List keys")
                .arg(arg_prefix())
                .arg(arg_limit())
                .arg(Arg::new("cursor").long("cursor").value_name("CURSOR"))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("scan")
                .about("Scan key-value pairs")
                .arg(Arg::new("start").long("start").value_name("KEY"))
                .arg(arg_limit()),
        )
        .subcommand(
            ClapCommand::new("count")
                .about("Count keys")
                .arg(arg_prefix()),
        )
        .subcommand(
            ClapCommand::new("history")
                .about("Key history")
                .arg(Arg::new("key").required(true)),
        )
        .subcommand(
            ClapCommand::new("getv")
                .about("Get key version history")
                .arg(Arg::new("key").required(true)),
        )
}

fn build_json() -> ClapCommand {
    ClapCommand::new("json")
        .about("JSON document operations")
        .subcommand_required(true)
        .subcommand(
            ClapCommand::new("set")
                .about("Set JSON at a path")
                .arg(Arg::new("key").required(true))
                .arg(Arg::new("path").required(true))
                .arg(Arg::new("value"))
                .arg(Arg::new("file").long("file").short('f').value_name("PATH")),
        )
        .subcommand(
            ClapCommand::new("get")
                .about("Get JSON at a path")
                .arg(Arg::new("key").required(true))
                .arg(Arg::new("path").required(true))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("del")
                .about("Delete JSON at a path")
                .arg(Arg::new("key").required(true))
                .arg(Arg::new("path").required(true)),
        )
        .subcommand(
            ClapCommand::new("list")
                .about("List JSON documents")
                .arg(arg_prefix())
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("100"),
                )
                .arg(Arg::new("cursor").long("cursor").value_name("CURSOR"))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("count")
                .about("Count JSON documents")
                .arg(arg_prefix()),
        )
        .subcommand(
            ClapCommand::new("history")
                .about("Document history")
                .arg(Arg::new("key").required(true)),
        )
        .subcommand(
            ClapCommand::new("getv")
                .about("Get document version history")
                .arg(Arg::new("key").required(true)),
        )
}

fn build_event() -> ClapCommand {
    ClapCommand::new("event")
        .about("Event log operations")
        .subcommand_required(true)
        .subcommand(
            ClapCommand::new("append")
                .about("Append an event")
                .arg(Arg::new("event-type").required(true))
                .arg(Arg::new("payload"))
                .arg(Arg::new("file").long("file").short('f').value_name("PATH")),
        )
        .subcommand(
            ClapCommand::new("get")
                .about("Get an event by sequence")
                .arg(
                    Arg::new("sequence")
                        .required(true)
                        .value_parser(clap::value_parser!(u64)),
                )
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("list")
                .about("List visible events")
                .arg(Arg::new("type").long("type").value_name("EVENT_TYPE"))
                .arg(arg_limit())
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("len")
                .about("Event count")
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("range")
                .about("Range by sequence")
                .arg(
                    Arg::new("start-seq")
                        .required(true)
                        .value_parser(clap::value_parser!(u64)),
                )
                .arg(arg_end_seq())
                .arg(arg_limit())
                .arg(arg_direction())
                .arg(Arg::new("type").long("type").value_name("EVENT_TYPE")),
        )
        .subcommand(
            ClapCommand::new("range-time")
                .about("Range by timestamp")
                .arg(
                    Arg::new("start-ts")
                        .required(true)
                        .value_parser(clap::value_parser!(u64)),
                )
                .arg(arg_end_ts())
                .arg(arg_limit())
                .arg(arg_direction())
                .arg(Arg::new("type").long("type").value_name("EVENT_TYPE")),
        )
        .subcommand(
            ClapCommand::new("types")
                .about("List event types")
                .arg(arg_as_of()),
        )
}

fn build_vector() -> ClapCommand {
    ClapCommand::new("vector")
        .about("Vector operations")
        .subcommand_required(true)
        .subcommand(
            ClapCommand::new("upsert")
                .about("Upsert a vector")
                .arg(Arg::new("collection").required(true))
                .arg(Arg::new("key").required(true))
                .arg(Arg::new("vector").required(true).help("JSON array"))
                .arg(Arg::new("metadata").long("metadata").value_name("JSON")),
        )
        .subcommand(
            ClapCommand::new("get")
                .about("Get a vector")
                .arg(Arg::new("collection").required(true))
                .arg(Arg::new("key").required(true))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("del")
                .about("Delete a vector")
                .arg(Arg::new("collection").required(true))
                .arg(Arg::new("key").required(true)),
        )
        .subcommand(
            ClapCommand::new("search")
                .about("Search a vector collection")
                .arg(Arg::new("collection").required(true))
                .arg(Arg::new("query").required(true).help("JSON array"))
                .arg(
                    Arg::new("k")
                        .long("k")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("10"),
                )
                .arg(Arg::new("metric").long("metric").value_parser([
                    "cosine",
                    "euclidean",
                    "dot-product",
                ]))
                .arg(Arg::new("filter").long("filter").value_name("JSON"))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("create")
                .about("Create a collection")
                .arg(Arg::new("collection").required(true))
                .arg(
                    Arg::new("dimension")
                        .required(true)
                        .value_parser(clap::value_parser!(u64)),
                )
                .arg(
                    Arg::new("metric")
                        .long("metric")
                        .value_parser(["cosine", "euclidean", "dot-product"])
                        .default_value("cosine"),
                ),
        )
        .subcommand(
            ClapCommand::new("drop")
                .about("Delete a collection")
                .arg(Arg::new("collection").required(true)),
        )
        .subcommand(ClapCommand::new("collections").about("List collections"))
        .subcommand(
            ClapCommand::new("stats")
                .about("Collection stats")
                .arg(Arg::new("collection").required(true)),
        )
        .subcommand(
            ClapCommand::new("batch-upsert")
                .about("Bulk upsert vectors")
                .arg(Arg::new("collection").required(true))
                .arg(Arg::new("entries"))
                .arg(Arg::new("file").long("file").short('f').value_name("PATH")),
        )
        .subcommand(
            ClapCommand::new("history")
                .about("Vector history")
                .arg(Arg::new("collection").required(true))
                .arg(Arg::new("key").required(true)),
        )
        .subcommand(
            ClapCommand::new("getv")
                .about("Get vector version history")
                .arg(Arg::new("collection").required(true))
                .arg(Arg::new("key").required(true)),
        )
}

fn build_graph() -> ClapCommand {
    ClapCommand::new("graph")
        .about("Graph operations")
        .subcommand_required(true)
        .subcommand(
            ClapCommand::new("create")
                .about("Create a graph")
                .arg(Arg::new("graph").required(true))
                .arg(
                    Arg::new("cascade-policy")
                        .long("cascade-policy")
                        .value_name("POLICY"),
                ),
        )
        .subcommand(
            ClapCommand::new("delete")
                .about("Delete a graph")
                .arg(Arg::new("graph").required(true)),
        )
        .subcommand(ClapCommand::new("list").about("List graphs"))
        .subcommand(
            ClapCommand::new("info")
                .about("Graph metadata")
                .arg(Arg::new("graph").required(true)),
        )
        .subcommand(
            ClapCommand::new("add-node")
                .about("Add or update a node")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("node-id").required(true))
                .arg(Arg::new("entity-ref").long("entity-ref").value_name("REF"))
                .arg(Arg::new("properties").long("properties").value_name("JSON"))
                .arg(
                    Arg::new("object-type")
                        .long("object-type")
                        .value_name("TYPE"),
                ),
        )
        .subcommand(
            ClapCommand::new("get-node")
                .about("Get a node")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("node-id").required(true))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("remove-node")
                .about("Remove a node")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("node-id").required(true)),
        )
        .subcommand(
            ClapCommand::new("list-nodes")
                .about("List node ids")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("type").long("type").value_name("TYPE"))
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .value_parser(clap::value_parser!(usize))
                        .value_name("N"),
                )
                .arg(Arg::new("cursor").long("cursor").value_name("CURSOR"))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("add-edge")
                .about("Add or update an edge")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("src").required(true))
                .arg(Arg::new("dst").required(true))
                .arg(Arg::new("edge-type").required(true))
                .arg(
                    Arg::new("weight")
                        .long("weight")
                        .value_parser(clap::value_parser!(f64)),
                )
                .arg(Arg::new("properties").long("properties").value_name("JSON")),
        )
        .subcommand(
            ClapCommand::new("remove-edge")
                .about("Remove an edge")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("src").required(true))
                .arg(Arg::new("dst").required(true))
                .arg(Arg::new("edge-type").required(true)),
        )
        .subcommand(
            ClapCommand::new("neighbors")
                .about("Get neighbors")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("node-id").required(true))
                .arg(
                    Arg::new("direction")
                        .long("direction")
                        .value_parser(["outgoing", "incoming", "both"]),
                )
                .arg(Arg::new("edge-type").long("edge-type").value_name("TYPE"))
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("bulk-insert")
                .about("Bulk insert graph data")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("nodes").long("nodes").value_name("JSON"))
                .arg(Arg::new("nodes-file").long("nodes-file").value_name("PATH"))
                .arg(Arg::new("edges").long("edges").value_name("JSON"))
                .arg(Arg::new("edges-file").long("edges-file").value_name("PATH"))
                .arg(
                    Arg::new("chunk-size")
                        .long("chunk-size")
                        .value_parser(clap::value_parser!(usize)),
                ),
        )
        .subcommand(
            ClapCommand::new("bfs")
                .about("Breadth-first traversal")
                .arg(Arg::new("graph").required(true))
                .arg(Arg::new("start-node").required(true))
                .arg(
                    Arg::new("max-depth")
                        .long("max-depth")
                        .value_parser(clap::value_parser!(usize))
                        .value_name("N"),
                )
                .arg(
                    Arg::new("max-nodes")
                        .long("max-nodes")
                        .value_parser(clap::value_parser!(usize))
                        .value_name("N"),
                )
                .arg(
                    Arg::new("edge-type")
                        .long("edge-type")
                        .action(ArgAction::Append)
                        .value_name("TYPE"),
                )
                .arg(
                    Arg::new("direction")
                        .long("direction")
                        .value_parser(["outgoing", "incoming", "both"]),
                ),
        )
        .subcommand(
            ClapCommand::new("ontology")
                .about("Graph ontology operations")
                .subcommand_required(true)
                .subcommand(
                    ClapCommand::new("status")
                        .about("Ontology status")
                        .arg(Arg::new("graph").required(true)),
                )
                .subcommand(
                    ClapCommand::new("summary")
                        .about("Ontology summary")
                        .arg(Arg::new("graph").required(true)),
                )
                .subcommand(
                    ClapCommand::new("freeze")
                        .about("Freeze ontology")
                        .arg(Arg::new("graph").required(true)),
                )
                .subcommand(
                    ClapCommand::new("define")
                        .about("Define an object or link type")
                        .arg(Arg::new("graph").required(true))
                        .arg(Arg::new("definition-json").required(true)),
                )
                .subcommand(
                    ClapCommand::new("get")
                        .about("Get a type definition")
                        .arg(Arg::new("graph").required(true))
                        .arg(Arg::new("name").required(true))
                        .arg(
                            Arg::new("kind")
                                .long("kind")
                                .required(true)
                                .value_parser(["object", "link"]),
                        ),
                )
                .subcommand(
                    ClapCommand::new("list")
                        .about("List ontology types")
                        .arg(Arg::new("graph").required(true))
                        .arg(
                            Arg::new("kind")
                                .long("kind")
                                .value_parser(["object", "link"]),
                        ),
                )
                .subcommand(
                    ClapCommand::new("delete")
                        .about("Delete a type definition")
                        .arg(Arg::new("graph").required(true))
                        .arg(Arg::new("name").required(true))
                        .arg(
                            Arg::new("kind")
                                .long("kind")
                                .required(true)
                                .value_parser(["object", "link"]),
                        ),
                ),
        )
        .subcommand(
            ClapCommand::new("analytics")
                .about("Graph analytics algorithms")
                .subcommand_required(true)
                .subcommand(
                    ClapCommand::new("wcc")
                        .about("Weakly connected components")
                        .arg(Arg::new("graph").required(true))
                        .arg(
                            Arg::new("top")
                                .long("top")
                                .value_parser(clap::value_parser!(usize))
                                .value_name("N"),
                        )
                        .arg(Arg::new("all").long("all").action(ArgAction::SetTrue)),
                )
                .subcommand(
                    ClapCommand::new("cdlp")
                        .about("Community detection via label propagation")
                        .arg(Arg::new("graph").required(true))
                        .arg(
                            Arg::new("max-iterations")
                                .long("max-iterations")
                                .value_parser(clap::value_parser!(usize))
                                .value_name("N"),
                        )
                        .arg(
                            Arg::new("direction")
                                .long("direction")
                                .value_parser(["outgoing", "incoming", "both"]),
                        )
                        .arg(
                            Arg::new("top")
                                .long("top")
                                .value_parser(clap::value_parser!(usize))
                                .value_name("N"),
                        )
                        .arg(Arg::new("all").long("all").action(ArgAction::SetTrue)),
                )
                .subcommand(
                    ClapCommand::new("pagerank")
                        .about("PageRank importance scoring")
                        .arg(Arg::new("graph").required(true))
                        .arg(
                            Arg::new("damping")
                                .long("damping")
                                .value_parser(clap::value_parser!(f64))
                                .value_name("VALUE"),
                        )
                        .arg(
                            Arg::new("max-iterations")
                                .long("max-iterations")
                                .value_parser(clap::value_parser!(usize))
                                .value_name("N"),
                        )
                        .arg(
                            Arg::new("tolerance")
                                .long("tolerance")
                                .value_parser(clap::value_parser!(f64))
                                .value_name("VALUE"),
                        )
                        .arg(
                            Arg::new("top")
                                .long("top")
                                .value_parser(clap::value_parser!(usize))
                                .value_name("N"),
                        )
                        .arg(Arg::new("all").long("all").action(ArgAction::SetTrue)),
                )
                .subcommand(
                    ClapCommand::new("lcc")
                        .about("Local clustering coefficient")
                        .arg(Arg::new("graph").required(true))
                        .arg(
                            Arg::new("top")
                                .long("top")
                                .value_parser(clap::value_parser!(usize))
                                .value_name("N"),
                        )
                        .arg(Arg::new("all").long("all").action(ArgAction::SetTrue)),
                )
                .subcommand(
                    ClapCommand::new("sssp")
                        .about("Single-source shortest path")
                        .arg(Arg::new("graph").required(true))
                        .arg(Arg::new("source-node").required(true))
                        .arg(
                            Arg::new("direction")
                                .long("direction")
                                .value_parser(["outgoing", "incoming", "both"]),
                        )
                        .arg(
                            Arg::new("top")
                                .long("top")
                                .value_parser(clap::value_parser!(usize))
                                .value_name("N"),
                        )
                        .arg(Arg::new("all").long("all").action(ArgAction::SetTrue)),
                ),
        )
}

fn build_branch() -> ClapCommand {
    ClapCommand::new("branch")
        .about("Branch lifecycle and power operations")
        .subcommand_required(true)
        .subcommand(
            ClapCommand::new("create")
                .about("Create a branch")
                .arg(Arg::new("name")),
        )
        .subcommand(
            ClapCommand::new("info")
                .about("Branch info")
                .arg(Arg::new("name").required(true)),
        )
        .subcommand(
            ClapCommand::new("list")
                .about("List branches")
                .arg(arg_limit())
                .arg(
                    Arg::new("offset")
                        .long("offset")
                        .value_parser(clap::value_parser!(u64)),
                ),
        )
        .subcommand(
            ClapCommand::new("exists")
                .about("Check branch existence")
                .arg(Arg::new("name").required(true)),
        )
        .subcommand(
            ClapCommand::new("del")
                .about("Delete a branch")
                .arg(Arg::new("name").required(true)),
        )
        .subcommand(
            ClapCommand::new("fork")
                .about("Fork a branch")
                .arg(Arg::new("destination").required(true))
                .arg(Arg::new("message").long("message").value_name("TEXT")),
        )
        .subcommand(
            ClapCommand::new("diff")
                .about("Diff two branches")
                .arg(Arg::new("branch-a").required(true))
                .arg(Arg::new("branch-b").required(true))
                .arg(
                    Arg::new("space")
                        .long("space")
                        .action(ArgAction::Append)
                        .value_name("NAME"),
                )
                .arg(arg_as_of()),
        )
        .subcommand(
            ClapCommand::new("merge")
                .about("Merge one branch into another")
                .arg(Arg::new("source").required(true))
                .arg(
                    Arg::new("strategy")
                        .long("strategy")
                        .value_parser(["lww", "last-writer-wins", "last_writer_wins", "strict"])
                        .default_value("lww"),
                )
                .arg(Arg::new("message").long("message").value_name("TEXT")),
        )
        .subcommand(
            ClapCommand::new("merge-base")
                .about("Find the merge base of two branches")
                .arg(Arg::new("branch-a").required(true))
                .arg(Arg::new("branch-b").required(true)),
        )
        .subcommand(
            ClapCommand::new("diff3")
                .about("Compute a three-way diff between two branches")
                .arg(Arg::new("branch-a").required(true))
                .arg(Arg::new("branch-b").required(true)),
        )
        .subcommand(
            ClapCommand::new("revert")
                .about("Revert a version range on a branch")
                .arg(Arg::new("branch").required(true))
                .arg(
                    Arg::new("from-version")
                        .required(true)
                        .value_parser(clap::value_parser!(u64)),
                )
                .arg(
                    Arg::new("to-version")
                        .required(true)
                        .value_parser(clap::value_parser!(u64)),
                ),
        )
        .subcommand(
            ClapCommand::new("cherry-pick")
                .about("Cherry-pick specific or filtered changes between branches")
                .arg(Arg::new("source").required(true))
                .arg(Arg::new("target").required(true))
                .arg(
                    Arg::new("pick")
                        .long("pick")
                        .num_args(2)
                        .action(ArgAction::Append)
                        .value_names(["SPACE", "KEY"]),
                )
                .arg(
                    Arg::new("space")
                        .long("space")
                        .action(ArgAction::Append)
                        .value_name("NAME"),
                )
                .arg(
                    Arg::new("key")
                        .long("key")
                        .action(ArgAction::Append)
                        .value_name("USER_KEY"),
                ),
        )
        .subcommand(
            ClapCommand::new("export")
                .about("Export a branch bundle")
                .arg(Arg::new("branch").required(true))
                .arg(Arg::new("path").required(true)),
        )
        .subcommand(
            ClapCommand::new("import")
                .about("Import a branch bundle")
                .arg(Arg::new("path").required(true)),
        )
        .subcommand(
            ClapCommand::new("validate")
                .about("Validate a branch bundle")
                .arg(Arg::new("path").required(true)),
        )
}

fn build_space() -> ClapCommand {
    ClapCommand::new("space")
        .about("Space operations")
        .subcommand_required(true)
        .subcommand(ClapCommand::new("list").about("List spaces"))
        .subcommand(
            ClapCommand::new("create")
                .about("Create a space")
                .arg(Arg::new("name").required(true)),
        )
        .subcommand(
            ClapCommand::new("del")
                .about("Delete a space")
                .arg(Arg::new("name").required(true))
                .arg(Arg::new("force").long("force").action(ArgAction::SetTrue)),
        )
        .subcommand(
            ClapCommand::new("exists")
                .about("Check space existence")
                .arg(Arg::new("name").required(true)),
        )
}

fn build_begin() -> ClapCommand {
    ClapCommand::new("begin").about("Begin a transaction").arg(
        Arg::new("read-only")
            .long("read-only")
            .action(ArgAction::SetTrue),
    )
}

fn build_commit() -> ClapCommand {
    ClapCommand::new("commit").about("Commit the active transaction")
}

fn build_rollback() -> ClapCommand {
    ClapCommand::new("rollback").about("Rollback the active transaction")
}

fn build_txn() -> ClapCommand {
    ClapCommand::new("txn")
        .about("Transaction state")
        .subcommand_required(true)
        .subcommand(ClapCommand::new("info").about("Transaction info"))
        .subcommand(ClapCommand::new("active").about("Check if a transaction is active"))
}

fn build_search() -> ClapCommand {
    ClapCommand::new("search")
        .about("Search across primitives")
        .arg(Arg::new("query").required(true))
        .arg(Arg::new("recipe").long("recipe").value_name("RECIPE"))
        .arg(arg_limit().id("k"))
        .arg(arg_as_of())
        .arg(
            Arg::new("diff-start")
                .long("diff-start")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("diff-end")
                .long("diff-end")
                .value_parser(clap::value_parser!(u64)),
        )
}

fn build_config() -> ClapCommand {
    ClapCommand::new("config")
        .about("Configuration")
        .subcommand_required(true)
        .subcommand(
            ClapCommand::new("set")
                .about("Set a config key")
                .arg(Arg::new("key").required(true))
                .arg(Arg::new("value").required(true)),
        )
        .subcommand(
            ClapCommand::new("get")
                .about("Get a config key")
                .arg(Arg::new("key").required(true)),
        )
        .subcommand(ClapCommand::new("list").about("List the current config"))
}

fn build_recipe() -> ClapCommand {
    ClapCommand::new("recipe")
        .about("Recipe operations")
        .subcommand_required(true)
        .subcommand(ClapCommand::new("show").about("Show the default recipe"))
        .subcommand(
            ClapCommand::new("get")
                .about("Get a named recipe")
                .arg(Arg::new("name")),
        )
        .subcommand(
            ClapCommand::new("set")
                .about("Set a named recipe")
                .arg(Arg::new("recipe-json").required(true))
                .arg(Arg::new("name").long("name").value_name("NAME")),
        )
        .subcommand(ClapCommand::new("list").about("List recipe names"))
        .subcommand(ClapCommand::new("seed").about("Seed built-in recipes"))
        .subcommand(
            ClapCommand::new("delete")
                .about("Delete a recipe")
                .arg(Arg::new("name")),
        )
}

fn build_configure_model() -> ClapCommand {
    ClapCommand::new("configure-model")
        .about("Configure a model endpoint")
        .arg(Arg::new("endpoint").required(true))
        .arg(Arg::new("model").required(true))
        .arg(Arg::new("api-key").long("api-key").value_name("KEY"))
        .arg(
            Arg::new("timeout-ms")
                .long("timeout-ms")
                .value_parser(clap::value_parser!(u64)),
        )
}

fn build_embed() -> ClapCommand {
    ClapCommand::new("embed")
        .about("Embed text")
        .arg(Arg::new("text").required(true))
}

fn build_models() -> ClapCommand {
    ClapCommand::new("models")
        .about("Model operations")
        .subcommand_required(true)
        .subcommand(ClapCommand::new("list").about("List available models"))
        .subcommand(ClapCommand::new("local").about("List local models"))
        .subcommand(
            ClapCommand::new("pull")
                .about("Download a model")
                .arg(Arg::new("name").required(true)),
        )
}

fn build_generate() -> ClapCommand {
    ClapCommand::new("generate")
        .about("Generate text")
        .arg(Arg::new("model").required(true))
        .arg(Arg::new("prompt").required(true))
        .arg(
            Arg::new("max-tokens")
                .long("max-tokens")
                .value_parser(clap::value_parser!(usize)),
        )
        .arg(
            Arg::new("temperature")
                .long("temperature")
                .value_parser(clap::value_parser!(f32)),
        )
        .arg(
            Arg::new("top-k")
                .long("top-k")
                .value_parser(clap::value_parser!(usize)),
        )
        .arg(
            Arg::new("top-p")
                .long("top-p")
                .value_parser(clap::value_parser!(f32)),
        )
        .arg(
            Arg::new("seed")
                .long("seed")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("stop-token")
                .long("stop-token")
                .value_parser(clap::value_parser!(u32))
                .action(ArgAction::Append),
        )
        .arg(
            Arg::new("stop-sequence")
                .long("stop-sequence")
                .action(ArgAction::Append)
                .value_name("TEXT"),
        )
}

fn build_tokenize() -> ClapCommand {
    ClapCommand::new("tokenize")
        .about("Tokenize text")
        .arg(Arg::new("model").required(true))
        .arg(Arg::new("text").required(true))
        .arg(
            Arg::new("no-special-tokens")
                .long("no-special-tokens")
                .action(ArgAction::SetTrue),
        )
}

fn build_detokenize() -> ClapCommand {
    ClapCommand::new("detokenize")
        .about("Detokenize token ids")
        .arg(Arg::new("model").required(true))
        .arg(
            Arg::new("ids")
                .required(true)
                .num_args(1..)
                .value_parser(clap::value_parser!(u32)),
        )
}

fn build_export() -> ClapCommand {
    ClapCommand::new("export")
        .about("Export primitive data")
        .arg(
            Arg::new("primitive")
                .required(true)
                .value_parser(["kv", "json", "events", "graph", "vector"]),
        )
        .arg(
            Arg::new("format")
                .long("format")
                .value_parser(["json", "jsonl", "csv", "parquet"])
                .default_value("json"),
        )
        .arg(arg_prefix())
        .arg(arg_limit())
        .arg(Arg::new("path").long("path").value_name("PATH"))
        .arg(Arg::new("collection").long("collection").value_name("NAME"))
        .arg(Arg::new("graph").long("graph").value_name("NAME"))
}

fn build_import() -> ClapCommand {
    ClapCommand::new("import")
        .about("Import primitive data")
        .arg(Arg::new("file-path").required(true))
        .arg(Arg::new("target").required(true))
        .arg(Arg::new("key-column").long("key-column").value_name("NAME"))
        .arg(
            Arg::new("value-column")
                .long("value-column")
                .value_name("NAME"),
        )
        .arg(Arg::new("collection").long("collection").value_name("NAME"))
        .arg(Arg::new("format").long("format").value_name("FORMAT"))
}

fn arg_prefix() -> Arg {
    Arg::new("prefix").long("prefix").value_name("PREFIX")
}

fn arg_limit() -> Arg {
    Arg::new("limit")
        .long("limit")
        .value_parser(clap::value_parser!(u64))
}

fn arg_as_of() -> Arg {
    Arg::new("as-of")
        .long("as-of")
        .value_parser(clap::value_parser!(u64))
}

fn arg_direction() -> Arg {
    Arg::new("direction")
        .long("direction")
        .value_parser(["forward", "reverse"])
        .default_value("forward")
}

fn arg_end_seq() -> Arg {
    Arg::new("end-seq")
        .long("end-seq")
        .value_parser(clap::value_parser!(u64))
}

fn arg_end_ts() -> Arg {
    Arg::new("end-ts")
        .long("end-ts")
        .value_parser(clap::value_parser!(u64))
}

#[cfg(test)]
mod tests {
    use super::{
        build_cli, build_repl_cli, check_meta_command, matches_to_request, parse_json_value,
        parse_value, parse_vector_literal,
    };
    use crate::context::Context;
    use crate::request::MetaCommand;
    use strata_executor::{Command, MergeStrategy, ScanDirection, TxnOptions, Value};

    fn context() -> Context {
        Context::new(
            "default".to_string(),
            "main".to_string(),
            "analytics".to_string(),
        )
    }

    fn parse_shell(args: &[&str]) -> Command {
        let matches = build_cli()
            .try_get_matches_from(args)
            .expect("args should parse");
        match matches_to_request(&matches, &context()).expect("request should parse") {
            crate::request::CliRequest::Execute(command) => command,
            crate::request::CliRequest::Meta(_) => panic!("expected executor command"),
        }
    }

    #[test]
    fn meta_command_detection_works() {
        match check_meta_command("use main analytics") {
            Some(MetaCommand::Use { branch, space }) => {
                assert_eq!(branch, "main");
                assert_eq!(space.as_deref(), Some("analytics"));
            }
            _ => panic!("expected use meta command"),
        }

        match check_meta_command("use main \"analytics space\"") {
            Some(MetaCommand::Use { branch, space }) => {
                assert_eq!(branch, "main");
                assert_eq!(space.as_deref(), Some("analytics space"));
            }
            _ => panic!("expected quoted use meta command"),
        }
    }

    #[test]
    fn parse_value_autodetects_simple_scalars() {
        assert_eq!(parse_value("true"), Value::Bool(true));
        assert_eq!(parse_value("12"), Value::Int(12));
        assert_eq!(parse_value("3.5"), Value::Float(3.5));
    }

    #[test]
    fn parse_json_requires_valid_json() {
        assert!(parse_json_value("{").is_err());
        assert!(parse_json_value("{\"ok\":true}").is_ok());
    }

    #[test]
    fn parse_vector_requires_json_array() {
        assert_eq!(
            parse_vector_literal("[1,2,3]").expect("vector should parse"),
            vec![1.0, 2.0, 3.0]
        );
        assert!(parse_vector_literal("{\"x\":1}").is_err());
    }

    #[test]
    fn global_flag_conflicts_are_rejected() {
        assert!(build_cli()
            .try_get_matches_from(["strata", "--cache", "--follower", "ping"])
            .is_err());
        assert!(build_cli()
            .try_get_matches_from(["strata", "--json", "--raw", "ping"])
            .is_err());
    }

    #[test]
    fn kept_command_groups_parse_into_executor_commands() {
        assert!(matches!(parse_shell(&["strata", "ping"]), Command::Ping));
        assert!(matches!(
            parse_shell(&["strata", "kv", "scan", "--start", "a", "--limit", "5"]),
            Command::KvScan { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "json", "count", "--prefix", "doc:"]),
            Command::JsonCount { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "event",
                "range",
                "1",
                "--end-seq",
                "5",
                "--direction",
                "reverse"
            ]),
            Command::EventRange {
                direction: ScanDirection::Reverse,
                ..
            }
        ));
        assert!(matches!(
            parse_shell(&["strata", "vector", "history", "emb", "k"]),
            Command::VectorGetv { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "kv", "getv", "key"]),
            Command::KvGetv { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "json", "getv", "doc"]),
            Command::JsonGetv { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "vector", "getv", "emb", "k"]),
            Command::VectorGetv { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "neighbors", "g", "n1"]),
            Command::GraphNeighbors { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "graph",
                "list-nodes",
                "g",
                "--limit",
                "25",
                "--cursor",
                "next"
            ]),
            Command::GraphListNodesPaginated { limit: 25, .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "list-nodes", "g", "--type", "Person"]),
            Command::GraphNodesByType { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "graph",
                "bfs",
                "g",
                "A",
                "--max-depth",
                "4",
                "--edge-type",
                "knows"
            ]),
            Command::GraphBfs { max_depth: 4, .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "analytics", "wcc", "g", "--top", "5"]),
            Command::GraphWcc { top_n: Some(5), .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "graph",
                "analytics",
                "cdlp",
                "g",
                "--max-iterations",
                "9",
                "--direction",
                "both"
            ]),
            Command::GraphCdlp {
                max_iterations: 9,
                ..
            }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "graph",
                "analytics",
                "pagerank",
                "g",
                "--damping",
                "0.9"
            ]),
            Command::GraphPagerank {
                damping: Some(_),
                ..
            }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "analytics", "lcc", "g", "--all"]),
            Command::GraphLcc {
                include_all: Some(true),
                ..
            }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "graph",
                "analytics",
                "sssp",
                "g",
                "A",
                "--direction",
                "outgoing"
            ]),
            Command::GraphSssp { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "status", "g"]),
            Command::GraphOntologyStatus { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "summary", "g"]),
            Command::GraphOntologySummary { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "freeze", "g"]),
            Command::GraphFreezeOntology { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "graph",
                "ontology",
                "define",
                "g",
                "{\"name\":\"Person\",\"fields\":{}}"
            ]),
            Command::GraphDefineObjectType { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "list", "g"]),
            Command::GraphListOntologyTypes { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "get", "g", "Person", "--kind", "object"]),
            Command::GraphGetObjectType { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata", "graph", "ontology", "delete", "g", "Person", "--kind", "object"
            ]),
            Command::GraphDeleteObjectType { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "graph",
                "ontology",
                "define",
                "g",
                "{\"name\":\"KNOWS\",\"source\":\"Person\",\"target\":\"Person\"}"
            ]),
            Command::GraphDefineLinkType { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "get", "g", "KNOWS", "--kind", "link"]),
            Command::GraphGetLinkType { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "list", "g", "--kind", "link"]),
            Command::GraphListLinkTypes { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "graph", "ontology", "delete", "g", "KNOWS", "--kind", "link"]),
            Command::GraphDeleteLinkType { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "del", "feature"]),
            Command::BranchDelete { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "fork", "feature"]),
            Command::BranchFork { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "diff", "main", "feature"]),
            Command::BranchDiff { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "branch",
                "merge",
                "feature",
                "--strategy",
                "strict"
            ]),
            Command::BranchMerge {
                strategy: MergeStrategy::Strict,
                ..
            }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "merge-base", "main", "feature"]),
            Command::BranchMergeBase { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "diff3", "main", "feature"]),
            Command::BranchDiffThreeWay { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "revert", "main", "10", "12"]),
            Command::BranchRevert { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "branch",
                "cherry-pick",
                "feature",
                "main",
                "--pick",
                "default",
                "user:1",
                "--pick",
                "analytics",
                "user:2"
            ]),
            Command::BranchCherryPick { .. }
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "branch",
                "export",
                "main",
                "main.branchbundle.tar.zst"
            ]),
            Command::BranchExport { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "import", "main.branchbundle.tar.zst"]),
            Command::BranchImport { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "branch", "validate", "main.branchbundle.tar.zst"]),
            Command::BranchBundleValidate { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "space", "exists", "analytics"]),
            Command::SpaceExists { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "begin", "--read-only"]),
            Command::TxnBegin {
                options: Some(TxnOptions { read_only: true }),
                ..
            }
        ));
        assert!(matches!(
            parse_shell(&["strata", "txn", "active"]),
            Command::TxnIsActive
        ));
        assert!(matches!(
            parse_shell(&[
                "strata",
                "search",
                "hello",
                "--diff-start",
                "1",
                "--diff-end",
                "3"
            ]),
            Command::Search { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "config", "get", "durability.mode"]),
            Command::ConfigureGetKey { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "recipe", "list"]),
            Command::RecipeList { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "configure-model", "local", "tiny"]),
            Command::ConfigureModel { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "embed", "hello"]),
            Command::Embed { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "models", "pull", "tiny"]),
            Command::ModelsPull { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "generate", "tiny", "hello"]),
            Command::Generate { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "tokenize", "tiny", "hello"]),
            Command::Tokenize { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "detokenize", "tiny", "1", "2"]),
            Command::Detokenize { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "export", "kv", "--format", "json"]),
            Command::DbExport { .. }
        ));
        assert!(matches!(
            parse_shell(&["strata", "import", "data.arrow", "kv"]),
            Command::ArrowImport { .. }
        ));
    }

    #[test]
    fn context_is_applied_to_data_commands() {
        match parse_shell(&["strata", "kv", "put", "key", "1"]) {
            Command::KvPut {
                branch,
                space,
                key,
                value,
            } => {
                assert_eq!(
                    branch.map(|value| value.to_string()),
                    Some("main".to_string())
                );
                assert_eq!(space.as_deref(), Some("analytics"));
                assert_eq!(key, "key");
                assert_eq!(value, Value::Int(1));
            }
            other => panic!("expected KvPut, got {other:?}"),
        }
    }

    #[test]
    fn recipe_seed_is_restored_to_the_parser_tree() {
        assert!(build_cli()
            .try_get_matches_from(["strata", "recipe", "seed"])
            .is_ok());
    }

    #[test]
    fn shell_only_local_commands_are_available_but_not_in_repl() {
        for args in [
            vec!["strata", "init"],
            vec!["strata", "up"],
            vec!["strata", "down"],
            vec!["strata", "uninstall"],
        ] {
            assert!(
                build_cli().try_get_matches_from(args.clone()).is_ok(),
                "shell command should parse"
            );
        }

        for args in [
            vec!["repl", "init"],
            vec!["repl", "up"],
            vec!["repl", "down"],
            vec!["repl", "uninstall"],
        ] {
            assert!(
                build_repl_cli().try_get_matches_from(args).is_err(),
                "REPL should reject shell-only command"
            );
        }
    }

    #[test]
    fn cherry_pick_rejects_mixed_direct_and_filter_modes() {
        let result = build_cli().try_get_matches_from([
            "strata",
            "branch",
            "cherry-pick",
            "feature",
            "main",
            "--pick",
            "default",
            "user:1",
            "--space",
            "default",
        ]);
        let matches = result.expect("args should parse");
        match matches_to_request(&matches, &context()) {
            Ok(_) => panic!("request should fail"),
            Err(err) => assert!(err.contains("--pick cannot be combined")),
        }
    }

    #[test]
    fn graph_list_nodes_rejects_mixed_modes() {
        let result = build_cli().try_get_matches_from([
            "strata",
            "graph",
            "list-nodes",
            "g",
            "--type",
            "Person",
            "--limit",
            "10",
        ]);
        let matches = result.expect("args should parse");
        match matches_to_request(&matches, &context()) {
            Ok(_) => panic!("request should fail"),
            Err(err) => assert!(err.contains("--type cannot be combined")),
        }
    }

    #[test]
    fn graph_ontology_define_uses_json_name_directly() {
        match parse_shell(&[
            "strata",
            "graph",
            "ontology",
            "define",
            "g",
            "{\"name\":\"Person\",\"fields\":{\"name\":\"string\"}}",
        ]) {
            Command::GraphDefineObjectType { definition, .. } => match definition {
                Value::Object(map) => {
                    assert_eq!(map.get("name"), Some(&Value::String("Person".to_string())));
                }
                other => panic!("expected object definition, got {other:?}"),
            },
            other => panic!("expected GraphDefineObjectType, got {other:?}"),
        }
    }

    #[test]
    fn graph_ontology_list_rejects_invalid_kind() {
        assert!(build_cli()
            .try_get_matches_from(["strata", "graph", "ontology", "list", "g", "--kind", "bad"])
            .is_err());
    }

    #[test]
    fn graph_ontology_get_requires_kind() {
        assert!(build_cli()
            .try_get_matches_from(["strata", "graph", "ontology", "get", "g", "Person"])
            .is_err());
    }

    #[test]
    fn graph_ontology_delete_requires_kind() {
        assert!(build_cli()
            .try_get_matches_from(["strata", "graph", "ontology", "delete", "g", "Person"])
            .is_err());
    }
}
