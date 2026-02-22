# MCP Server Reference

The StrataDB MCP (Model Context Protocol) server exposes StrataDB as a tool provider for AI assistants like Claude.

## Installation

```bash
# From crates.io
cargo install strata-mcp

# From source
git clone https://github.com/stratadb-labs/strata-mcp
cd strata-mcp
cargo install --path .
```

## Configuration

### Claude Desktop

Add to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "stratadb": {
      "command": "strata-mcp",
      "args": ["/path/to/data"]
    }
  }
}
```

### In-Memory Mode

For ephemeral databases:

```json
{
  "mcpServers": {
    "stratadb": {
      "command": "strata-mcp",
      "args": ["--memory"]
    }
  }
}
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `<PATH>` | Path to the database directory |
| `--memory` | Use ephemeral in-memory database |
| `-h, --help` | Show help |
| `-V, --version` | Show version |

---

## Tools

The MCP server exposes the following tools to AI assistants.

### Database Tools

#### strata_db_ping

Check database connectivity.

**Parameters:** None

**Returns:**
```json
{"pong": true, "version": "0.6.0"}
```

#### strata_db_info

Get database information.

**Parameters:** None

**Returns:**
```json
{
  "version": "0.6.0",
  "uptime_secs": 3600,
  "branch_count": 3,
  "total_keys": 1500
}
```

#### strata_db_flush

Flush pending writes to disk.

**Parameters:** None

#### strata_db_compact

Trigger database compaction.

**Parameters:** None

#### strata_db_time_range

Get the available time range for the current branch. Use the returned timestamps with the `as_of` parameter on read tools for time-travel queries.

**Parameters:** None

**Returns:**
```json
{
  "oldest_ts": 1700000000000000,
  "latest_ts": 1700001000000000
}
```

Returns `null` timestamps if the branch has no data.

---

### KV Store Tools

#### strata_kv_put

Store a key-value pair.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The key to store |
| `value` | any | Yes | The value (string, number, boolean, object, array) |

**Returns:** `{"version": 1}`

#### strata_kv_get

Get a value by key.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The key to retrieve |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** The value, or `null` if not found

#### strata_kv_delete

Delete a key.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The key to delete |

**Returns:** `true` or `false`

#### strata_kv_list

List keys with optional prefix filter.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `prefix` | string | No | Filter keys by prefix |
| `limit` | integer | No | Maximum keys to return |
| `cursor` | string | No | Pagination cursor |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** Array of key names

#### strata_kv_history

Get version history for a key.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The key |
| `as_of` | integer | No | Microsecond timestamp — get history up to that point |

**Returns:** Array of `{value, version, timestamp}`

#### strata_kv_put_many

Store multiple key-value pairs in a single operation.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `items` | object[] | Yes | Array of `{key, value}` objects |

**Returns:** Array of version numbers

#### strata_kv_get_many

Get multiple keys in a single operation.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `keys` | string[] | Yes | Array of key names |

**Returns:** Array of values (null for missing keys)

#### strata_kv_delete_many

Delete multiple keys in a single operation.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `keys` | string[] | Yes | Array of key names |

**Returns:** Array of booleans (true if key existed)

---

### State Cell Tools

#### strata_state_set

Set a state cell value.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cell` | string | Yes | The cell name |
| `value` | any | Yes | The value to set |

**Returns:** `{"version": 1}`

#### strata_state_get

Get a state cell value.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cell` | string | Yes | The cell name |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** The value, or `null` if not found

#### strata_state_init

Initialize a state cell only if it doesn't exist.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cell` | string | Yes | The cell name |
| `value` | any | Yes | The initial value |

**Returns:** `{"version": 1}`

#### strata_state_cas

Compare-and-swap update.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cell` | string | Yes | The cell name |
| `value` | any | Yes | The new value |
| `expected_counter` | integer | No | Expected current version |

**Returns:** New version on success, `null` on CAS failure

#### strata_state_delete

Delete a state cell.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cell` | string | Yes | The cell name |

**Returns:** `true` or `false`

#### strata_state_list

List state cell names.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `prefix` | string | No | Filter by prefix |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** Array of cell names

#### strata_state_history

Get version history for a state cell.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cell` | string | Yes | The cell name |
| `as_of` | integer | No | Microsecond timestamp — get history up to that point |

**Returns:** Array of `{value, version, timestamp}`

---

### Event Log Tools

#### strata_event_append

Append an event to the log.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `event_type` | string | Yes | The event type |
| `payload` | any | Yes | The event payload |

**Returns:** `{"version": 0}` (sequence number)

#### strata_event_get

Get an event by sequence number.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `sequence` | integer | Yes | The sequence number |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** `{value, version, timestamp}` or `null`

#### strata_event_list

List events by type.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `event_type` | string | Yes | The event type |
| `limit` | integer | No | Maximum events |
| `after_sequence` | integer | No | Return events after this sequence |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** Array of `{value, version, timestamp}`

#### strata_event_len

Get total event count.

**Parameters:** None

**Returns:** Event count (number)

---

### JSON Store Tools

#### strata_json_set

Set a value at a JSONPath.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The document key |
| `path` | string | Yes | JSONPath (use `$` for root) |
| `value` | any | Yes | The value to set |

**Returns:** `{"version": 1}`

#### strata_json_get

Get a value at a JSONPath.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The document key |
| `path` | string | Yes | JSONPath |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** The value, or `null` if not found

#### strata_json_delete

Delete a value at a JSONPath.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The document key |
| `path` | string | Yes | JSONPath |

**Returns:** Count of elements deleted (number)

#### strata_json_list

List JSON document keys.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `prefix` | string | No | Filter by prefix |
| `limit` | integer | No | Maximum keys (default: 100) |
| `cursor` | string | No | Pagination cursor |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** `{"keys": [...], "cursor": "..."}`

#### strata_json_history

Get version history for a JSON document.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string | Yes | The document key |
| `as_of` | integer | No | Microsecond timestamp — get history up to that point |

**Returns:** Array of `{value, version, timestamp}`

---

### Vector Store Tools

#### strata_vector_create_collection

Create a vector collection.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |
| `dimension` | integer | Yes | Vector dimension |
| `metric` | string | No | `cosine` (default), `euclidean`, `dot_product` |

**Returns:** `{"version": 1}`

#### strata_vector_delete_collection

Delete a vector collection.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |

**Returns:** `true` or `false`

#### strata_vector_list_collections

List all vector collections.

**Parameters:** None

**Returns:** Array of collection info objects

#### strata_vector_stats

Get detailed statistics for a collection.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |

**Returns:** Collection info object

#### strata_vector_upsert

Insert or update a vector.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |
| `key` | string | Yes | Vector key |
| `vector` | number[] | Yes | Vector embedding |
| `metadata` | any | No | Optional metadata |

**Returns:** `{"version": 1}`

#### strata_vector_get

Get a vector by key.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |
| `key` | string | Yes | Vector key |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Returns:** `{key, embedding, metadata, version, timestamp}` or `null`

#### strata_vector_delete

Delete a vector.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |
| `key` | string | Yes | Vector key |

**Returns:** `true` or `false`

#### strata_vector_search

Search for similar vectors.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |
| `query` | number[] | Yes | Query vector |
| `k` | integer | Yes | Number of results |
| `metric` | string | No | Override distance metric |
| `filter` | object[] | No | Metadata filters |
| `as_of` | integer | No | Microsecond timestamp for time-travel reads |

**Filter format:**
```json
[
  {"field": "category", "op": "eq", "value": "science"},
  {"field": "year", "op": "gte", "value": 2020}
]
```

**Filter operators:** `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`

**Returns:** Array of `{key, score, metadata}`

#### strata_vector_batch_upsert

Batch insert/update vectors.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `collection` | string | Yes | Collection name |
| `entries` | object[] | Yes | Array of `{key, vector, metadata?}` |

**Returns:** Array of version numbers

---

### Branch Tools

#### strata_branch_create

Create a new empty branch.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Branch name |

#### strata_branch_get

Get branch metadata.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Branch name |

**Returns:** Branch info or `null`

#### strata_branch_list

List all branches.

**Parameters:** None

**Returns:** Array of branch info objects

#### strata_branch_exists

Check if a branch exists.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Branch name |

**Returns:** `true` or `false`

#### strata_branch_delete

Delete a branch.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Branch name |

#### strata_branch_fork

Fork a branch with all its data.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `source` | string | Yes | Source branch |
| `destination` | string | Yes | New branch name |

**Returns:** `{source, destination, keys_copied}`

#### strata_branch_diff

Compare two branches.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `branch_a` | string | Yes | First branch |
| `branch_b` | string | Yes | Second branch |

**Returns:** Diff summary

#### strata_branch_merge

Merge branches.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `source` | string | Yes | Source branch |
| `target` | string | Yes | Target branch |
| `strategy` | string | No | `last_writer_wins` (default) or `strict` |

**Returns:** Merge result with conflicts

#### strata_branch_use

Switch current branch context.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Branch name |

#### strata_current_branch

Get current branch name.

**Parameters:** None

**Returns:** Branch name (string)

---

### Space Tools

#### strata_space_create

Create a new space.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Space name |

#### strata_space_list

List all spaces.

**Parameters:** None

**Returns:** Array of space names

#### strata_space_exists

Check if a space exists.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Space name |

**Returns:** `true` or `false`

#### strata_space_delete

Delete a space.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Space name |
| `force` | boolean | No | Delete even if non-empty |

#### strata_space_use

Switch current space context.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Space name |

#### strata_current_space

Get current space name.

**Parameters:** None

**Returns:** Space name (string)

---

### Transaction Tools

#### strata_txn_begin

Begin a new transaction.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `read_only` | boolean | No | Read-only transaction |

#### strata_txn_commit

Commit the current transaction.

**Parameters:** None

**Returns:** `{"status": "committed", "version": 5}`

#### strata_txn_rollback

Rollback the current transaction.

**Parameters:** None

#### strata_txn_info

Get current transaction info.

**Parameters:** None

**Returns:** `{id, status, started_at}` or `null`

#### strata_txn_is_active

Check if a transaction is active.

**Parameters:** None

**Returns:** `true` or `false`

---

### Search Tools

#### strata_search

Search across multiple primitives.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | Yes | Search query |
| `k` | integer | No | Maximum results |
| `primitives` | string[] | No | Primitives to search (`kv`, `json`, `state`, `event`) |

**Returns:** Array of `{entity, primitive, score, rank, snippet}`

---

### Bundle Tools

#### strata_bundle_export

Export a branch to a bundle file.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `branch` | string | Yes | Branch name |
| `path` | string | Yes | Output file path |

**Returns:** `{branch_id, path, entry_count, bundle_size}`

#### strata_bundle_import

Import a branch from a bundle file.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `path` | string | Yes | Bundle file path |

**Returns:** `{branch_id, transactions_applied, keys_written}`

#### strata_bundle_validate

Validate a bundle file.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `path` | string | Yes | Bundle file path |

**Returns:** `{branch_id, format_version, entry_count, checksums_valid}`

---

### Retention Tools

#### strata_retention_apply

Apply the retention policy to expire old data.

**Parameters:** None

---

## Time Travel

StrataDB supports point-in-time queries via the `as_of` parameter. Timestamps are in **microseconds since Unix epoch**.

### Workflow

1. Call `strata_db_time_range` to discover the available timestamp range
2. Pass an `as_of` timestamp to any supported read tool

### Supported Tools

The following tools accept the optional `as_of` parameter:

| Category | Tools |
|----------|-------|
| KV | `strata_kv_get`, `strata_kv_list`, `strata_kv_history` |
| State | `strata_state_get`, `strata_state_list`, `strata_state_history` |
| Events | `strata_event_get`, `strata_event_list` |
| JSON | `strata_json_get`, `strata_json_list`, `strata_json_history` |
| Vector | `strata_vector_get`, `strata_vector_search` |

### Example

```
User: "What was the value of 'config' 5 minutes ago?"

Claude:
1. Calls strata_db_time_range → {"oldest_ts": 1700000000000000, "latest_ts": 1700001000000000}
2. Calculates: 5 minutes ago = latest_ts - 300000000
3. Calls strata_kv_get with key="config", as_of=1700000700000000
```

---

## Error Handling

Tool errors are returned in the standard MCP error format:

```json
{
  "error": {
    "code": -32000,
    "message": "strata error: Branch not found: nonexistent"
  }
}
```

Common error codes:
- `-32602`: Invalid parameters (missing args, wrong types, not found)
- `-32603`: Internal error
- `-32601`: Unknown tool

---

## Usage with Claude

Once configured, Claude can use StrataDB tools naturally:

**User:** "Store my name as Alice"

**Claude:** I'll store that for you.
*[Calls strata_kv_put with key="name", value="Alice"]*

Done! I've stored your name as "Alice" in the database.

**User:** "Create a branch called 'experiment' and switch to it"

**Claude:** I'll create that branch and switch to it.
*[Calls strata_branch_create with name="experiment"]*
*[Calls strata_branch_use with name="experiment"]*

Done! Created the "experiment" branch and switched to it. Any data operations will now happen on this branch.

**User:** "What was stored under 'name' before I changed it?"

**Claude:** Let me check the history.
*[Calls strata_db_time_range]*
*[Calls strata_kv_get with key="name", as_of=<earlier_timestamp>]*

The previous value was "Alice".
