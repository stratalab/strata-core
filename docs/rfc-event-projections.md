# RFC: Event Projections to Other Primitives

**Status:** Proposal
**Related Issues:** #1269 (Hybrid Search), #1270 (Graph-Augmented Search), #1272 (Recursive Query), #1273 (Native RAG), #1274 (Agent-First API)

## Problem

Today, Strata's primitives are islands. You write to KV, JSON, State, Events, and Graph independently. If you want to maintain a graph that reflects your JSON documents, or keep a KV summary that aggregates your events, you do it in application code — two separate writes, no atomicity, no guarantee of consistency.

The Event primitive is an append-only, hash-chained, immutable log. This is exactly the shape of an **event source** — the single source of truth from which other data structures are derived. But right now, events just sit there. Nothing reads them to produce derived state.

Event projections close this gap: **user-defined rules that automatically materialize events into KV entries, JSON documents, graph nodes/edges, or state cells.** The event log becomes the authoritative record, and all other primitives become queryable, searchable, indexable views.

This is event sourcing built into the database — no Kafka, no separate projection service, no eventual consistency across systems.

## Design

### Projection Definition

A projection is a declarative rule that maps events of a specific type to operations on other primitives:

```rust
pub struct Projection {
    /// Unique name for this projection
    pub name: String,
    /// Event types this projection subscribes to (supports prefix matching)
    pub event_types: Vec<String>,
    /// Source event space (default: "default")
    pub source_space: Option<String>,
    /// The materialization rules
    pub actions: Vec<ProjectionAction>,
    /// Whether the projection is active
    pub enabled: bool,
    /// Last processed sequence number (projection cursor)
    pub cursor: u64,
}

pub enum ProjectionAction {
    /// Write a KV entry derived from the event
    KvSet {
        /// Key template with event field interpolation: "user:{payload.user_id}"
        key_template: String,
        /// Value template or "payload" for the full event payload
        value_template: ValueTemplate,
        /// Target space
        space: Option<String>,
    },
    /// Upsert a JSON document derived from the event
    JsonSet {
        /// Collection to write to
        collection: String,
        /// Document key template: "order:{payload.order_id}"
        key_template: String,
        /// Value: full payload, subset, or transformed
        value_template: ValueTemplate,
    },
    /// Add a graph node derived from the event
    GraphAddNode {
        /// Target graph
        graph: String,
        /// Node ID template: "{payload.user_id}"
        node_id_template: String,
        /// Node type (object type name)
        node_type: Option<String>,
        /// Properties extracted from event payload
        properties_template: Option<ValueTemplate>,
    },
    /// Add a graph edge derived from the event
    GraphAddEdge {
        /// Target graph
        graph: String,
        /// Source node ID template
        source_template: String,
        /// Target node ID template
        target_template: String,
        /// Edge type
        edge_type: String,
        /// Edge properties
        properties_template: Option<ValueTemplate>,
    },
    /// Update a state cell derived from the event
    StateSet {
        /// Cell name template
        cell_template: String,
        /// Value template
        value_template: ValueTemplate,
        /// Target space
        space: Option<String>,
    },
}

pub enum ValueTemplate {
    /// Use the full event payload as-is
    Payload,
    /// Extract a single field from the payload
    Field(String),
    /// Construct a new object from event fields
    /// e.g., {"name": "{payload.name}", "count": "{payload.items.length}"}
    Template(serde_json::Value),
}
```

### Example: E-Commerce Event Projections

```
# Events are the source of truth
event append order_created '{"order_id": "ORD-123", "user_id": "alice", "items": [{"sku": "WIDGET-A", "qty": 2}], "total": 49.99}'
event append order_shipped '{"order_id": "ORD-123", "carrier": "fedex", "tracking": "FX123456"}'
event append user_signup  '{"user_id": "bob", "email": "bob@example.com", "plan": "pro"}'

# Projection: materialize orders into JSON documents
projection create order_view \
  --event-types order_created,order_shipped,order_cancelled \
  --action json-set \
    --collection orders \
    --key "order:{payload.order_id}" \
    --value payload

# Projection: build a user→order graph
projection create user_orders \
  --event-types order_created \
  --action graph-add-node \
    --graph commerce \
    --node-id "{payload.user_id}" \
    --node-type customer \
  --action graph-add-node \
    --graph commerce \
    --node-id "{payload.order_id}" \
    --node-type order \
    --properties '{"total": "{payload.total}"}' \
  --action graph-add-edge \
    --graph commerce \
    --source "{payload.user_id}" \
    --target "{payload.order_id}" \
    --edge-type PLACED_ORDER

# Projection: maintain a state cell tracking order count per user
projection create user_stats \
  --event-types order_created \
  --action state-set \
    --cell "order_count:{payload.user_id}" \
    --value '{"count": "+1"}'  # increment semantics (see below)
```

After these events are appended, the projections automatically:
1. Create/update a JSON document `order:ORD-123` in the `orders` collection
2. Create graph nodes `alice` (customer) and `ORD-123` (order) with a `PLACED_ORDER` edge
3. Increment the `order_count:alice` state cell

All of this data is then:
- **Full-text searchable** via BM25 (auto-indexed)
- **Vector searchable** via auto-embed (shadow collections)
- **Graph traversable** (find all orders for a user)
- **Time-travel capable** (see state at any point in time)
- **RAG queryable** via `db.ask("what did alice order?")`

### Projection Execution Model

#### Synchronous (Default)

Projections run inline during `event_append()`, after the event is committed but before the response is returned. This gives strong consistency — after `event_append()` returns, all projected data is guaranteed to be visible.

```rust
// In event_append handler
pub fn event_append(p: &Primitives, cmd: EventAppend) -> Result<Output, Error> {
    // 1. Append event (normal path)
    let version = p.event().append(branch_id, &space, &cmd.event_type, cmd.payload.clone())?;
    let sequence = match &version { Version::Sequence(s) => *s, _ => unreachable!() };

    // 2. Run projections
    let projections = get_active_projections(p, branch_id, &space, &cmd.event_type)?;
    for proj in &projections {
        let event_context = EventContext {
            sequence,
            event_type: &cmd.event_type,
            payload: &cmd.payload,
            timestamp: now_micros(),
        };
        // Best-effort: log errors, don't fail the append
        if let Err(e) = execute_projection(p, branch_id, proj, &event_context) {
            tracing::warn!(
                "projection '{}' failed for event {}: {e}",
                proj.name, sequence
            );
        }
    }

    // 3. Update projection cursors
    for proj in &projections {
        update_cursor(p, branch_id, &space, &proj.name, sequence)?;
    }

    // 4. Auto-embed (existing hook, unchanged)
    // ...

    Ok(Output::Version(version))
}
```

#### Catch-Up Replay

When a new projection is created, or when a projection falls behind (e.g., after a failure), it can replay from its cursor:

```
projection replay order_view --from 0
projection replay order_view --from 1000
projection replay order_view  # resumes from cursor
```

This reads events sequentially from the cursor position and re-executes the projection actions. Because events are immutable and projections are deterministic, replay produces identical results.

### Template Interpolation

Templates use a simple `{field.path}` syntax to extract values from the event:

```
Key template: "user:{payload.user_id}"
Event payload: {"user_id": "alice", "name": "Alice"}
Result: "user:alice"
```

Implementation:

```rust
fn interpolate(template: &str, event: &EventContext) -> Result<String, String> {
    let mut result = template.to_string();
    let re = regex::Regex::new(r"\{([\w.]+)\}").unwrap();

    for cap in re.captures_iter(template) {
        let path = &cap[1];
        let value = resolve_path(event, path)?;
        let replacement = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            other => serde_json::to_string(other).unwrap(),
        };
        result = result.replace(&cap[0], &replacement);
    }
    Ok(result)
}

fn resolve_path<'a>(event: &'a EventContext, path: &str) -> Result<&'a Value, String> {
    let parts: Vec<&str> = path.split('.').collect();
    match parts[0] {
        "payload" => json_pointer(&event.payload, &parts[1..]),
        "event_type" => Ok(&Value::String(event.event_type.to_string())),
        "sequence" => Ok(&Value::Number(event.sequence.into())),
        _ => Err(format!("unknown root: {}", parts[0])),
    }
}
```

### Commands

| Command | Description |
|---------|-------------|
| `ProjectionCreate { name, event_types, actions, source_space }` | Define a new projection |
| `ProjectionDelete { name }` | Remove a projection |
| `ProjectionList` | List all projections with cursor positions |
| `ProjectionGet { name }` | Get projection definition and status |
| `ProjectionEnable { name }` | Enable a disabled projection |
| `ProjectionDisable { name }` | Disable without deleting (stops processing, preserves cursor) |
| `ProjectionReplay { name, from_sequence }` | Replay events from a given sequence |
| `ProjectionStatus { name }` | Show cursor, lag (events behind head), last error |

### Storage

Projections are stored as JSON documents in a well-known key pattern:

```
Key: __projection__{name}
TypeTag: Event (stored in the event namespace)
Value: serialized Projection
```

Cursor position is part of the `Projection` struct and updated atomically with a CAS on the projection document.

### Estimated Scope

| File | Change | Lines |
|------|--------|-------|
| `crates/executor/src/command.rs` | Add 8 Projection* command variants | ~60 |
| `crates/executor/src/output.rs` | Add `ProjectionStatus`, `ProjectionList` output variants | ~20 |
| `crates/executor/src/handlers/projection.rs` | New: projection CRUD, execution engine, replay, template interpolation | ~500 |
| `crates/executor/src/handlers/event.rs` | Hook: run projections after event append | ~40 |
| `crates/executor/src/types.rs` | `Projection`, `ProjectionAction`, `ValueTemplate` structs | ~80 |
| `crates/cli/src/commands.rs` | Add `projection` subcommand group | ~120 |
| `crates/cli/src/parse.rs` | Parse projection commands | ~100 |
| `crates/cli/src/repl.rs` | Tab completion for projection commands | ~10 |
| **Total** | | **~930** |

This is the largest of the three proposals — roughly 4x the audit trail. The template engine and multi-action execution are the bulk of the complexity.

## Integration with Product Roadmap

### Write-Time Enrichment (#1270 Phase 6)

Issue #1270's Phase 6 mentions "write-time enrichment" as a future direction — auto-entity extraction, learned sparse expansion, section-hierarchy indexing. Event projections are the **mechanism** for write-time enrichment:

```
# When a document is ingested, emit an event
event append document_ingested '{"doc_id": "D1", "title": "...", "body": "..."}'

# Projection: extract entities and build knowledge graph
projection create entity_extraction \
  --event-types document_ingested \
  --action graph-add-node \
    --graph knowledge \
    --node-id "{payload.doc_id}" \
    --node-type document \
    --properties '{"title": "{payload.title}"}'
```

The first version uses template-based extraction (extracting fields that already exist in the payload). A future version could integrate with the intelligence crate to run NER or entity extraction as part of the projection pipeline, creating graph nodes for people, organizations, and concepts mentioned in documents.

### Graph-Augmented Search (#1270)

Projections naturally create the graph structure that #1270's search pipeline traverses:

1. Events flow in (e.g., `user_signup`, `order_created`, `review_posted`)
2. Projections materialize a knowledge graph (users → orders → products → reviews)
3. Graph-augmented search traverses this graph to improve retrieval
4. `db.ask()` uses the graph-enriched results for RAG

The key insight: **the graph doesn't need to be manually maintained.** Events are the input, projections build the graph automatically, and search leverages the graph transparently.

### Hybrid Search Pipeline (#1269)

Every projected value — KV entries, JSON documents, graph nodes — goes through auto-embed and inverted index hooks. This means projections automatically feed the hybrid search pipeline:

- BM25 indexes the projected text
- Vector search indexes the projected embeddings
- RRF fuses results across all projected materializations

A single event can produce multiple searchable artifacts: a JSON document (full content), a KV entry (summary), and graph nodes (relationships). The search pipeline finds the most relevant artifact regardless of which projection created it.

### Recursive Query Execution (#1272)

Tier 2's sandbox can inspect projections:

```python
# RLM sandbox code
projections = db.projection_list()
for p in projections:
    status = db.projection_status(p.name)
    if status.lag > 100:
        # This projection is behind — recent events may not be in the materialized view
        # Fall back to direct event search instead
        events = db.event_get_by_type(p.event_types[0], limit=50)
```

The recursive query agent can reason about projection freshness and choose between searching materialized views (fast, possibly stale) or raw events (authoritative, slower).

### Native RAG — db.ask() (#1273)

`db.ask()` searches across all primitives. With projections, a single event stream produces KV, JSON, and Graph artifacts that are all searchable. This means `db.ask()` can:

1. Find the JSON document (full content, high recall)
2. Find the graph node (relationships, high precision)
3. Find the KV summary (overview, quick context)
4. Find the original event (authoritative record, timestamp/sequence)

Four different representations of the same data, each optimized for a different search signal. RRF fusion picks the best results across all of them.

### Agent-First API (#1274)

`describe()` surfaces projection definitions:

```json
{
  "projections": {
    "order_view": {
      "event_types": ["order_created", "order_shipped", "order_cancelled"],
      "targets": ["json:orders"],
      "cursor": 1547,
      "lag": 0,
      "enabled": true
    },
    "user_orders": {
      "event_types": ["order_created"],
      "targets": ["graph:commerce"],
      "cursor": 1200,
      "lag": 347,
      "enabled": true
    }
  }
}
```

An agent can understand the data flow: events → projections → materialized views. It can check projection health, identify stale materializations, and decide whether to query the materialized view or the raw event stream.

The MCP server (#1274) exposes projection management as tools:

```
strata_projection_create(name, event_types, actions)
strata_projection_status(name) → {cursor, lag, last_error, enabled}
strata_projection_replay(name, from_sequence)
```

## What This Enables That Nothing Else Can

### vs. Kafka + Consumers

Kafka provides event streaming. Consumers (Flink, KSQL, custom services) project events into databases. But:
- **3+ systems** to manage (Kafka, consumer service, target database)
- **No cross-primitive search**: you can't search across the event log and the materialized views in one query
- **No graph integration**: no knowledge graph built from the event stream that feeds back into search
- **No agent discoverability**: no `describe()` that shows the projection topology
- **No branch isolation**: no way to experiment with different projections in an isolated branch

### vs. Event Sourcing Frameworks (EventStoreDB, Axon)

Event sourcing frameworks manage events and projections, but:
- **Projections are code**: you write a handler function, deploy it, manage its lifecycle
- **No natural language search**: you can't `db.ask("what happened?")` across both events and materialized views
- **No graph-augmented retrieval**: events don't automatically build a knowledge graph that enhances search
- **Single-primitive output**: projections typically produce read models in a SQL database, not a multi-primitive data platform

### Strata's Unique Position

Strata is the only system where:
1. **Events are append-only, hash-chained, tamper-evident** (integrity guarantee)
2. **Projections produce multi-primitive artifacts** (KV + JSON + Graph + State from one event)
3. **All artifacts are auto-embedded and searchable** (hybrid search across everything)
4. **Graph structure emerges from events** and **feeds back into search** (#1270)
5. **Agents can discover and reason about the projection topology** (#1274)
6. **Branch isolation** lets you experiment with new projections on a copy-on-write branch
7. **`db.ask()`** searches across events, projected views, and graph in one natural language query

## Phased Rollout

Given the scope (~930 lines), this should be built incrementally:

### Phase 1: Core Infrastructure (~400 lines)
- `Projection` struct and storage
- Template interpolation engine
- `ProjectionCreate`, `ProjectionDelete`, `ProjectionList`, `ProjectionGet` commands
- Single action type: `KvSet` (simplest target)
- Synchronous execution in `event_append()`
- CLI commands for basic CRUD

### Phase 2: Multi-Primitive Actions (~300 lines)
- Add `JsonSet`, `GraphAddNode`, `GraphAddEdge`, `StateSet` action types
- Multi-action projections (one event → multiple materialized artifacts)
- Projection replay (`ProjectionReplay` command)
- `ProjectionStatus` with cursor/lag tracking

### Phase 3: Agent Integration (~230 lines)
- Surface projections in `describe()` (#1274)
- `ProjectionEnable`/`ProjectionDisable` for lifecycle management
- Explain mode integration: "this search result came from projection X processing event Y"
- MCP tool wrappers

## Open Questions

1. **Error handling policy**: If a projection action fails (e.g., graph validation rejects a node), should we (a) skip that action and continue, (b) disable the projection, or (c) retry? Recommendation: skip + log for sync mode, disable after N consecutive failures.

2. **Ordering guarantees**: For multi-action projections, do actions execute in order? If action 2 depends on action 1 (e.g., add graph node then add edge), ordering matters. Recommendation: actions execute sequentially in definition order within a single event.

3. **Idempotency**: If a projection replays, it re-executes actions. `KvSet` and `StateSet` are naturally idempotent (overwrite). `GraphAddNode` is also idempotent (upsert semantics). `GraphAddEdge` may not be — duplicate edges could be created. Recommendation: `GraphAddEdge` uses upsert semantics (same src/dst/type = same edge).

4. **Template expressiveness vs. simplicity**: The current design uses simple `{field.path}` interpolation. Should we support expressions like `{payload.count + 1}` or conditionals? Recommendation: start minimal. If users need complex logic, Tier 2 recursive queries (#1272) can handle it.

5. **Cross-space projections**: Can an event in space "ingest" project to KV in space "cache"? Current design assumes same space. Cross-space projections would be more flexible but harder to reason about.

6. **Projection composition**: Can one projection's output trigger another projection? (e.g., event → state cell update → audit event → another projection). This creates chains. Recommendation: no recursive projections in v1. The audit trail generates events but those events are in a different type namespace (`state.*`) and don't match user-defined projections unless explicitly subscribed.

7. **Performance at scale**: With many projections, `event_append()` latency increases linearly with the number of matching projections. For high-throughput scenarios, async projections (a background thread that reads from the cursor) may be necessary. Defer to Phase 4.
