//! MCP server transport (first-run D8).
//!
//! `strata mcp serve` speaks Model Context Protocol over stdio: newline-
//! delimited JSON-RPC 2.0 on stdin/stdout (logs stay on stderr). It is a thin
//! transport over executor exactly like the CLI — the same command wire
//! shapes, the same output envelopes, the same error codes — so an agent
//! debugging across channels sees one product.
//!
//! The tool surface is curated (~20 tools covering the six primitives' core
//! verbs plus branching) instead of exhaustive: 80+ tools blow out client
//! tool budgets and drown models in choices. Two meta-tools keep the whole
//! surface reachable: `strata_guide` returns the version-locked usage guide,
//! and `strata_command` executes any cataloged command by raw wire JSON.
//! Every tool argument set is validated by the executor's own command
//! deserializer (`deny_unknown_fields`), so invalid input produces the exact
//! field-level teaching error the wire contract defines.

use std::io::{BufRead, Write};

use base64::Engine as _;
use serde_json::{json, Map, Value};
use strata_executor::ipc::Connection;
use strata_executor::Command;

use crate::{agents, CliError};

const PROTOCOL_VERSION: &str = "2025-06-18";

/// The MCP protocol revisions this server actually implements. `initialize`
/// answers with the client's requested version only when it is one of these,
/// and otherwise with [`PROTOCOL_VERSION`] (the latest) — never with an
/// unsupported version the client merely offered, which would tell the client
/// this server speaks a protocol it does not (#2572).
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[PROTOCOL_VERSION];

/// Runs the stdio server loop until stdin closes. Returns the exit code.
pub(crate) fn serve(connection: &Connection) -> Result<i32, CliError> {
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(response) = handle_message(connection, &line) {
            let mut stdout = std::io::stdout().lock();
            serde_json::to_writer(&mut stdout, &response)?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }
    Ok(0)
}

fn handle_message(connection: &Connection, line: &str) -> Option<Value> {
    let message: Value = match serde_json::from_str(line) {
        Ok(message) => message,
        Err(error) => {
            return Some(rpc_error(
                Value::Null,
                -32700,
                format!("parse error: {error}"),
            ));
        }
    };
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    // Requests carry an id; notifications (initialized, cancelled, …) do not
    // and must not be answered.
    let id = message.get("id").cloned()?;

    Some(match method.as_str() {
        "initialize" => rpc_result(id, initialize_result(&params)),
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(id, json!({ "tools": tool_list() })),
        "tools/call" => handle_tool_call(connection, id, &params),
        _ => rpc_error(id, -32601, format!("method `{method}` is not supported")),
    })
}

/// Negotiate an MCP protocol version (#2572): the requested version when it is
/// supported, otherwise `latest`. Pulled out of `initialize_result` so the
/// echo-vs-fallback decision is observable with more than one supported version
/// — the shipped list has exactly one (equal to the fallback), so a call-site
/// test alone cannot tell the guard from a constant.
fn negotiate_protocol_version<'a>(
    requested: Option<&'a str>,
    supported: &[&'a str],
    latest: &'a str,
) -> &'a str {
    match requested {
        Some(version) if supported.contains(&version) => version,
        _ => latest,
    }
}

fn initialize_result(params: &Value) -> Value {
    // Negotiate per the MCP spec (#2572): respond with the client's requested
    // version when this server actually supports it, otherwise with our latest.
    let negotiated = negotiate_protocol_version(
        params.get("protocolVersion").and_then(Value::as_str),
        SUPPORTED_PROTOCOL_VERSIONS,
        PROTOCOL_VERSION,
    );
    json!({
        "protocolVersion": negotiated,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": "strata", "version": env!("CARGO_PKG_VERSION") },
        "instructions": "Strata is an embedded multi-model database (KV, JSON, vectors, \
    events, graphs) with branches and time travel. When unsure, call the `strata_guide` tool first — \
    it returns the complete usage guide for this exact binary version. The curated tools cover core \
    verbs; `strata_command` executes any cataloged command by raw wire JSON (catalog: guide's command \
    section). Results and errors use the same JSON envelopes and stable error codes as the CLI. \
    Skills that teach this surface (usage, branching, time travel): `npx skills add \
    stratalab/strata-agent-skills`.",
    })
}

fn handle_tool_call(connection: &Connection, id: Value, params: &Value) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return rpc_error(id, -32602, "tools/call requires a `name`".to_owned());
    };
    let arguments = match params.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return rpc_error(id, -32602, "`arguments` must be an object".to_owned());
        }
    };

    let run = match tool_run(name, arguments) {
        Ok(run) => run,
        Err(ToolFailure::UnknownTool) => {
            return rpc_error(
                id,
                -32602,
                format!("unknown tool `{name}`; list tools with tools/list"),
            );
        }
        Err(ToolFailure::Input(message)) => return tool_result(id, &message, true),
    };

    match run {
        ToolRun::Guide => match agents::guide_markdown() {
            Ok(guide) => tool_result(id, &guide, false),
            Err(error) => tool_result(id, &format!("guide unavailable: {error}"), true),
        },
        ToolRun::Command(command_json) => {
            let command: Command = match serde_json::from_value(command_json) {
                Ok(command) => command,
                Err(error) => {
                    // The executor's own deserializer names the offending
                    // field and the valid set — the error is the teaching.
                    return tool_result(
                        id,
                        &format!("invalid arguments for `{name}`: {error}"),
                        true,
                    );
                }
            };
            match connection.execute(command) {
                Ok(output) => {
                    let envelope = serde_json::to_string(&output)
                        .unwrap_or_else(|error| format!("output serialization failed: {error}"));
                    tool_result(id, &envelope, false)
                }
                Err(error) => {
                    // The same structured envelope the CLI emits on stderr:
                    // code, class, hint, and the stable per-code ref.
                    let envelope = serde_json::to_string(&json!({ "error": error }))
                        .unwrap_or_else(|error| format!("error serialization failed: {error}"));
                    tool_result(id, &envelope, true)
                }
            }
        }
    }
}

// ---- tool dispatch ----------------------------------------------------------

enum ToolRun {
    Guide,
    Command(Value),
}

#[derive(Debug)]
enum ToolFailure {
    UnknownTool,
    Input(String),
}

/// Builds the executor command for one tool call. Tool arguments pass through
/// onto the command wire (one canonical validation path); the only transforms
/// are ergonomic: KV text becomes base64 bytes, and `strata_branch_fork`
/// selects its wire variant from the anchor argument.
fn tool_run(name: &str, mut args: Map<String, Value>) -> Result<ToolRun, ToolFailure> {
    let wire_type = match name {
        "strata_guide" => return Ok(ToolRun::Guide),
        "strata_command" => {
            let command = args.remove("command").ok_or_else(|| {
                ToolFailure::Input(
                    "strata_command requires `command`: a serialized command object like \
{\"type\":\"kv_scan\",\"limit\":10}"
                        .to_owned(),
                )
            })?;
            return Ok(ToolRun::Command(command));
        }
        "strata_kv_put" => {
            text_to_bytes(&mut args, &["key", "value"])?;
            "kv_put"
        }
        "strata_kv_get" => {
            text_to_bytes(&mut args, &["key"])?;
            "kv_get"
        }
        "strata_kv_delete" => {
            text_to_bytes(&mut args, &["key"])?;
            "kv_delete"
        }
        "strata_kv_list" => {
            // `cursor` stays as the opaque base64 token a previous page printed.
            text_to_bytes(&mut args, &["prefix"])?;
            "kv_list"
        }
        "strata_json_set" => "json_set",
        "strata_json_get" => "json_get",
        "strata_json_delete" => "json_delete",
        "strata_vector_create_collection" => "vector_create_collection",
        "strata_vector_upsert" => "vector_upsert",
        "strata_vector_query" => "vector_query",
        "strata_event_append" => "event_append",
        "strata_event_list" => "event_list",
        "strata_graph_create" => "graph_create",
        "strata_graph_add_node" => "graph_add_node",
        "strata_graph_add_edge" => "graph_add_edge",
        "strata_graph_neighbors" => "graph_neighbors",
        "strata_branch_list" => "branch_list",
        "strata_branch_fork" => {
            let has_version = args.contains_key("version");
            let has_timestamp = args.contains_key("timestamp");
            match (has_version, has_timestamp) {
                (true, true) => {
                    return Err(ToolFailure::Input(
                        "pass either `version` or `timestamp`, not both".to_owned(),
                    ));
                }
                (true, false) => "branch_fork_at_version",
                (false, true) => "branch_fork_at_timestamp",
                (false, false) => "branch_fork_current",
            }
        }
        _ => return Err(ToolFailure::UnknownTool),
    };

    args.insert("type".to_owned(), Value::String(wire_type.to_owned()));
    Ok(ToolRun::Command(Value::Object(args)))
}

/// Base64-encodes text arguments destined for `Bytes` wire fields.
fn text_to_bytes(args: &mut Map<String, Value>, fields: &[&str]) -> Result<(), ToolFailure> {
    for field in fields {
        let Some(value) = args.get(*field) else {
            continue; // absence is the command deserializer's call
        };
        let Some(text) = value.as_str() else {
            return Err(ToolFailure::Input(format!("`{field}` must be a string")));
        };
        let encoded = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
        args.insert((*field).to_owned(), Value::String(encoded));
    }
    Ok(())
}

// ---- tool catalog -----------------------------------------------------------

// A data table, not logic: one entry per curated tool.
#[allow(clippy::too_many_lines)]
fn tool_list() -> Vec<Value> {
    let scope =
        |mut properties: Map<String, Value>, extra: &[(&str, Value)]| -> Map<String, Value> {
            for (key, value) in extra {
                properties.insert((*key).to_owned(), value.clone());
            }
            properties.insert(
                "branch".to_owned(),
                string_prop("Target branch (defaults to the session branch)"),
            );
            properties.insert(
                "space".to_owned(),
                string_prop("Product space (defaults to `default`)"),
            );
            properties
        };
    let as_of = || {
        (
            "as_of",
            json!({
                "type": "integer",
                "description": "Read at this commit timestamp (from a write receipt's data.commit.timestamp)",
            }),
        )
    };

    vec![
        tool(
            "strata_guide",
            "Return the complete Strata usage guide (markdown, version-matched to this server). \
Call this first when unsure how anything works.",
            json!({"type": "object", "properties": {}}),
        ),
        tool(
            "strata_command",
            "Escape hatch: execute any cataloged Strata command as raw wire JSON. Example: \
{\"command\":{\"type\":\"kv_scan\",\"limit\":10}}. The catalog is in the guide; byte fields \
(KV keys/values, cursors) are base64. Invalid input returns the exact field-level wire error.",
            json!({
                "type": "object",
                "properties": {
                    "command": {"type": "object", "description": "Serialized command ({\"type\": ..., ...})"},
                },
                "required": ["command"],
            }),
        ),
        tool(
            "strata_kv_put",
            "Write one key-value pair (text). Returns a commit receipt with version and timestamp. \
Example: {\"key\":\"greeting\",\"value\":\"hello\"}. Errors: invalid_argument.engine.kv_key.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("key", string_prop("Key (text)")),
                        ("value", string_prop("Value (text; use strata_command for binary)")),
                    ],
                ),
                &["key", "value"],
            ),
        ),
        tool(
            "strata_kv_get",
            "Read one key. data.value is base64 on the wire. Example: {\"key\":\"greeting\"}. \
Errors: not_found.engine.branch.",
            object_schema(
                scope(Map::new(), &[("key", string_prop("Key (text)")), as_of()]),
                &["key"],
            ),
        ),
        tool(
            "strata_kv_delete",
            "Delete one key. The effect reports not_found when the key never existed. \
Example: {\"key\":\"greeting\"}.",
            object_schema(scope(Map::new(), &[("key", string_prop("Key (text)"))]), &["key"]),
        ),
        tool(
            "strata_kv_list",
            "List keys, optionally by prefix, with honest pagination: pass the returned base64 \
cursor back verbatim. Example: {\"prefix\":\"app:\",\"limit\":100}.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("prefix", string_prop("Key prefix (text)")),
                        ("cursor", string_prop("Base64 continuation cursor from the previous page")),
                        ("limit", integer_prop("Maximum keys to return")),
                        as_of(),
                    ],
                ),
                &[],
            ),
        ),
        tool(
            "strata_json_set",
            "Set a JSON document path. Path `$` replaces the whole document. Example: \
{\"key\":\"user:1\",\"path\":\"$.name\",\"value\":\"Ada\"}. Errors: invalid_argument.engine.json_path.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("key", string_prop("Document key")),
                        ("path", string_prop("JSON path, e.g. $ or $.field")),
                        ("value", json!({"description": "Any JSON value"})),
                    ],
                ),
                &["key", "path", "value"],
            ),
        ),
        tool(
            "strata_json_get",
            "Read a JSON document path. Example: {\"key\":\"user:1\",\"path\":\"$.name\"}.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("key", string_prop("Document key")),
                        ("path", string_prop("JSON path")),
                        as_of(),
                    ],
                ),
                &["key", "path"],
            ),
        ),
        tool(
            "strata_json_delete",
            "Delete a JSON document path (path `$` deletes the document). Example: \
{\"key\":\"user:1\",\"path\":\"$.stale\"}.",
            object_schema(
                scope(
                    Map::new(),
                    &[("key", string_prop("Document key")), ("path", string_prop("JSON path"))],
                ),
                &["key", "path"],
            ),
        ),
        tool(
            "strata_vector_create_collection",
            "Create a vector collection. Example: {\"collection\":\"docs\",\"dimension\":384}. \
Errors: invalid_argument.engine.vector_collection_exists.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("collection", string_prop("Collection name")),
                        ("dimension", integer_prop("Embedding dimension")),
                        ("metric", string_prop("cosine | euclidean | dot_product (default cosine)")),
                    ],
                ),
                &["collection", "dimension"],
            ),
        ),
        tool(
            "strata_vector_upsert",
            "Upsert one embedding with optional metadata. Example: {\"collection\":\"docs\",\
\"key\":\"doc-1\",\"vector\":[0.1,0.9],\"metadata\":{\"kind\":\"note\"}}. \
Errors: invalid_argument.engine.vector_dimension, not_found.engine.vector_collection.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("collection", string_prop("Collection name")),
                        ("key", string_prop("Vector key")),
                        ("vector", number_array_prop("Embedding (collection dimension)")),
                        ("metadata", json!({"type": "object", "description": "Optional metadata object"})),
                    ],
                ),
                &["collection", "key", "vector"],
            ),
        ),
        tool(
            "strata_vector_query",
            "Similarity search. Filter shape: {\"conditions\":[{\"field\":\"kind\",\"op\":\"eq\",\
\"value\":{\"type\":\"string\",\"value\":\"note\"}}]}. Example: {\"collection\":\"docs\",\
\"query\":[0.1,0.9],\"k\":5}. Errors: invalid_argument.engine.vector_dimension.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("collection", string_prop("Collection name")),
                        ("query", number_array_prop("Query embedding")),
                        ("k", integer_prop("Maximum matches (default 10)")),
                        ("filter", json!({"type": "object", "description": "AND-composed metadata filter"})),
                        as_of(),
                    ],
                ),
                &["collection", "query"],
            ),
        ),
        tool(
            "strata_event_append",
            "Append one immutable event (hash-chained, dense sequences). Example: \
{\"event_type\":\"user.created\",\"payload\":{\"id\":1}}. Errors: invalid_argument.engine.event_type.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("event_type", string_prop("Event type, e.g. user.created")),
                        ("payload", json!({"description": "Any JSON payload"})),
                    ],
                ),
                &["event_type", "payload"],
            ),
        ),
        tool(
            "strata_event_list",
            "List events, optionally by type. Example: {\"event_type\":\"user.created\",\"limit\":50}.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("event_type", string_prop("Filter by event type")),
                        ("limit", integer_prop("Maximum events")),
                        ("after_sequence", integer_prop("Exclusive sequence cursor")),
                        as_of(),
                    ],
                ),
                &[],
            ),
        ),
        tool(
            "strata_graph_create",
            "Create a graph. Example: {\"graph\":\"deps\"}.",
            object_schema(scope(Map::new(), &[("graph", string_prop("Graph name"))]), &["graph"]),
        ),
        tool(
            "strata_graph_add_node",
            "Add or replace a node. Example: {\"graph\":\"deps\",\"node_id\":\"core\",\
\"properties\":{\"lang\":\"rust\"}}. Errors: not_found.engine.graph.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("graph", string_prop("Graph name")),
                        ("node_id", string_prop("Node id")),
                        ("properties", json!({"type": "object", "description": "Optional properties"})),
                    ],
                ),
                &["graph", "node_id"],
            ),
        ),
        tool(
            "strata_graph_add_edge",
            "Add or replace a typed, optionally weighted edge. Example: {\"graph\":\"deps\",\
\"src\":\"cli\",\"edge_type\":\"depends_on\",\"dst\":\"core\",\"weight\":2.5}.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("graph", string_prop("Graph name")),
                        ("src", string_prop("Source node id")),
                        ("edge_type", string_prop("Edge type")),
                        ("dst", string_prop("Destination node id")),
                        ("weight", json!({"type": "number", "description": "Optional weight"})),
                        ("properties", json!({"type": "object", "description": "Optional properties"})),
                    ],
                ),
                &["graph", "src", "edge_type", "dst"],
            ),
        ),
        tool(
            "strata_graph_neighbors",
            "Traverse a node's neighbors. Example: {\"graph\":\"deps\",\"node_id\":\"core\",\
\"direction\":\"incoming\"}.",
            object_schema(
                scope(
                    Map::new(),
                    &[
                        ("graph", string_prop("Graph name")),
                        ("node_id", string_prop("Node id")),
                        ("direction", string_prop("outgoing | incoming | both (default outgoing)")),
                        ("edge_type", string_prop("Filter by edge type")),
                        ("limit", integer_prop("Maximum neighbors")),
                        as_of(),
                    ],
                ),
                &["graph", "node_id"],
            ),
        ),
        tool(
            "strata_branch_list",
            "List branches with status and lineage.",
            json!({"type": "object", "properties": {}}),
        ),
        tool(
            "strata_branch_fork",
            "Fork a branch: current state by default, or anchored to a retained `version` or \
commit `timestamp` for time travel. Writes after the fork are isolated per branch. Example: \
{\"source\":\"default\",\"branch\":\"experiment\"}. Errors: not_found.engine.branch.",
            json!({
                "type": "object",
                "properties": {
                    "source": string_prop("Source branch"),
                    "branch": string_prop("New branch name"),
                    "version": integer_prop("Optional retained source commit version"),
                    "timestamp": integer_prop("Optional retained source commit timestamp"),
                },
                "required": ["source", "branch"],
            }),
        ),
    ]
}

fn tool(name: &str, description: &str, input_schema: Value) -> Value {
    let mut map = Map::new();
    map.insert("name".to_owned(), Value::String(name.to_owned()));
    map.insert(
        "description".to_owned(),
        Value::String(description.to_owned()),
    );
    map.insert("inputSchema".to_owned(), input_schema);
    Value::Object(map)
}

fn object_schema(properties: Map<String, Value>, required: &[&str]) -> Value {
    let mut map = Map::new();
    map.insert("type".to_owned(), Value::String("object".to_owned()));
    map.insert("properties".to_owned(), Value::Object(properties));
    map.insert(
        "required".to_owned(),
        Value::Array(
            required
                .iter()
                .map(|field| Value::String((*field).to_owned()))
                .collect(),
        ),
    );
    Value::Object(map)
}

fn string_prop(description: &str) -> Value {
    json!({"type": "string", "description": description})
}

fn integer_prop(description: &str) -> Value {
    json!({"type": "integer", "description": description})
}

fn number_array_prop(description: &str) -> Value {
    json!({"type": "array", "items": {"type": "number"}, "description": description})
}

// ---- JSON-RPC ---------------------------------------------------------------

fn tool_result(id: Value, text: &str, is_error: bool) -> Value {
    rpc_result(
        id,
        json!({
            "content": [{"type": "text", "text": text}],
            "isError": is_error,
        }),
    )
}

fn rpc_result(id: Value, result: Value) -> Value {
    let mut map = Map::new();
    map.insert("jsonrpc".to_owned(), Value::String("2.0".to_owned()));
    map.insert("id".to_owned(), id);
    map.insert("result".to_owned(), result);
    Value::Object(map)
}

fn rpc_error(id: Value, code: i64, message: String) -> Value {
    let mut error = Map::new();
    error.insert("code".to_owned(), json!(code));
    error.insert("message".to_owned(), Value::String(message));
    let mut map = Map::new();
    map.insert("jsonrpc".to_owned(), Value::String("2.0".to_owned()));
    map.insert("id".to_owned(), id);
    map.insert("error".to_owned(), Value::Object(error));
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The initialize result is the MCP client's first impression: it must
    /// echo an offered protocol version (or default it), name the server,
    /// and carry instructions routing agents to the guide tool, the raw
    /// command escape hatch, and the skills.
    #[test]
    fn initialize_result_names_the_server_and_teaches_the_entry_points() {
        // #2572: negotiate, don't echo. A supported version comes back as-is; an
        // unsupported or bogus one gets our latest, never the client's fiction.
        let supported = initialize_result(&json!({ "protocolVersion": PROTOCOL_VERSION }));
        assert_eq!(supported["protocolVersion"], json!(PROTOCOL_VERSION));
        for unsupported in ["2024-11-05", "1999-01-01", ""] {
            let negotiated = initialize_result(&json!({ "protocolVersion": unsupported }));
            assert_eq!(
                negotiated["protocolVersion"],
                json!(PROTOCOL_VERSION),
                "an unsupported protocolVersion must not be echoed ({unsupported:?})"
            );
        }

        let defaulted = initialize_result(&json!({}));
        assert_eq!(defaulted["protocolVersion"], json!(PROTOCOL_VERSION));
        assert_eq!(defaulted["serverInfo"]["name"], json!("strata"));
        let instructions = defaulted["instructions"].as_str().expect("instructions");
        assert!(instructions.contains("strata_guide"));
        assert!(instructions.contains("strata_command"));
        assert!(instructions.contains("npx skills add"));
    }

    /// #2572: the negotiation function itself, exercised with a multi-element
    /// supported list so echoing a supported *non-latest* version is observably
    /// different from falling back to the latest. The shipped list has one entry
    /// equal to the fallback, so a call-site test alone cannot tell the guard
    /// from a constant.
    #[test]
    fn negotiate_protocol_version_echoes_supported_else_falls_back() {
        let supported = ["v-old", "v-new"];
        // A supported version that is NOT the latest comes back unchanged — this
        // is what distinguishes real negotiation from always answering `latest`.
        assert_eq!(
            negotiate_protocol_version(Some("v-old"), &supported, "v-new"),
            "v-old"
        );
        assert_eq!(
            negotiate_protocol_version(Some("v-new"), &supported, "v-new"),
            "v-new"
        );
        // Unsupported or absent -> the latest (fallback), never the client's ask.
        assert_eq!(
            negotiate_protocol_version(Some("v-bogus"), &supported, "v-new"),
            "v-new"
        );
        assert_eq!(
            negotiate_protocol_version(None, &supported, "v-new"),
            "v-new"
        );
    }

    /// Every advertised tool must dispatch, and dispatch must not accept
    /// tools that are not advertised — the list and the match are kept in one
    /// file precisely so this test can hold them together.
    #[test]
    fn advertised_tools_and_dispatch_agree() {
        let tools = tool_list();
        let mut names: Vec<String> = tools
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name").to_owned())
            .collect();
        let total = names.len();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), total, "tool names must be unique");

        for name in &names {
            let mut args = Map::new();
            if name == "strata_command" {
                args.insert("command".to_owned(), json!({"type": "ping"}));
            }
            assert!(
                !matches!(tool_run(name, args), Err(ToolFailure::UnknownTool)),
                "advertised tool `{name}` does not dispatch"
            );
        }
        assert!(matches!(
            tool_run("strata_nope", Map::new()),
            Err(ToolFailure::UnknownTool)
        ));
    }

    #[test]
    fn every_tool_carries_teaching_description_and_schema() {
        for tool in tool_list() {
            let name = tool["name"].as_str().expect("name");
            let description = tool["description"].as_str().expect("description");
            assert!(
                description.len() > 20,
                "`{name}` description must teach, not label"
            );
            assert_eq!(tool["inputSchema"]["type"], "object", "`{name}` schema");
        }
    }

    #[test]
    fn handle_message_lists_tools_and_a_tool_call_round_trips_through_the_connection() {
        let connection = Connection::cache(
            strata_executor::Executor::open_cache().expect("cache executor opens"),
        );

        // tools/list returns the curated set through the request path.
        let list = handle_message(
            &connection,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
        )
        .expect("tools/list carries an id");
        assert_eq!(list["id"], json!(1));
        let tools = list["result"]["tools"].as_array().expect("tools array");
        assert!(!tools.is_empty(), "tools/list returns the curated tools");

        // tools/call reaches the executor: a kv put stores, a kv get reads back.
        let put = handle_message(
            &connection,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"strata_kv_put","arguments":{"key":"greeting","value":"hello"}}}"#,
        )
        .expect("tools/call carries an id");
        assert_eq!(
            put["result"]["isError"],
            json!(false),
            "put must succeed: {put}"
        );

        let got = handle_message(
            &connection,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"strata_kv_get","arguments":{"key":"greeting"}}}"#,
        )
        .expect("tools/call carries an id");
        let text = got["result"]["content"][0]["text"]
            .as_str()
            .expect("tool result text");
        assert!(
            text.contains("aGVsbG8="),
            "the get tool sees the put's value (base64 of `hello`): {text}"
        );
    }

    #[test]
    fn tools_call_without_a_name_is_an_invalid_params_error() {
        let connection = Connection::cache(
            strata_executor::Executor::open_cache().expect("cache executor opens"),
        );
        let response = handle_message(
            &connection,
            r#"{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{}}"#,
        )
        .expect("a request with an id gets a response");
        assert_eq!(
            response["error"]["code"],
            json!(-32602),
            "a missing tool name is an invalid-params error: {response}"
        );
    }

    #[test]
    fn kv_text_arguments_become_wire_bytes() {
        let mut args = Map::new();
        args.insert("key".to_owned(), json!("greeting"));
        args.insert("value".to_owned(), json!("hello"));
        let ToolRun::Command(command) = tool_run("strata_kv_put", args).expect("dispatch") else {
            panic!("kv_put is a command tool");
        };
        assert_eq!(command["type"], "kv_put");
        assert_eq!(command["key"], "Z3JlZXRpbmc=");
        assert_eq!(command["value"], "aGVsbG8=");
        // The transformed JSON is a valid wire command.
        assert!(serde_json::from_value::<Command>(command).is_ok());
    }

    #[test]
    fn branch_fork_selects_its_wire_variant_from_the_anchor() {
        let base = || {
            let mut args = Map::new();
            args.insert("source".to_owned(), json!("default"));
            args.insert("branch".to_owned(), json!("fork"));
            args
        };
        let ToolRun::Command(current) = tool_run("strata_branch_fork", base()).expect("fork")
        else {
            panic!("fork is a command tool");
        };
        assert_eq!(current["type"], "branch_fork_current");

        let mut versioned = base();
        versioned.insert("version".to_owned(), json!(7));
        let ToolRun::Command(at_version) = tool_run("strata_branch_fork", versioned).expect("fork")
        else {
            panic!("fork is a command tool");
        };
        assert_eq!(at_version["type"], "branch_fork_at_version");

        let mut both = base();
        both.insert("version".to_owned(), json!(7));
        both.insert("timestamp".to_owned(), json!(70));
        assert!(matches!(
            tool_run("strata_branch_fork", both),
            Err(ToolFailure::Input(_))
        ));
    }
}
