# RFC: State-Event Audit Trail

**Status:** Proposal
**Related Issues:** #1270 (Graph-Augmented Search), #1272 (Recursive Query), #1273 (Native RAG), #1274 (Agent-First API)

## Problem

State cells today are opaque boxes. A CAS-protected `pipeline_status` cell can go from `"running"` to `"failed"` to `"retrying"` to `"succeeded"`, but there's no record of *when* each transition happened, *who* triggered it, or *what context* surrounded it. The value is overwritten in place; history exists only through `getv()` which returns raw versions without any causal metadata.

Meanwhile, the Event primitive already provides exactly the infrastructure needed to capture this history: append-only, SHA-256 hash-chained, per-type indexed, time-travel capable, and auto-embedded into vector search.

These two primitives sit next to each other but don't talk. Connecting them creates something no other database offers: **a versioned key-value store with a built-in, tamper-evident, searchable audit log that agents can query in natural language.**

## Design

### Core Mechanism

Every successful state mutation (`set`, `init`, `cas`, `batch_set`) automatically emits an event to a well-known event sequence within the same branch and space.

```
State: set("pipeline_status", "failed")
  → Version::Counter(3)

Event (auto-emitted):
  sequence: 147
  event_type: "state.set"
  payload: {
    "cell": "pipeline_status",
    "value": "failed",
    "prev_value": "running",
    "version": 3,
    "prev_version": 2,
    "operation": "set",
    "actor": null
  }
  prev_hash: <SHA-256 of event 146>
  hash: <SHA-256 of this event>
```

### Event Types

| Operation | Event Type | Additional Fields |
|-----------|-----------|-------------------|
| `state_set` | `state.set` | `cell`, `value`, `prev_value`, `version`, `prev_version` |
| `state_init` | `state.init` | `cell`, `value`, `version` (no prev — cell is new) |
| `state_cas` (success) | `state.cas` | `cell`, `value`, `prev_value`, `version`, `expected_version` |
| `state_cas` (failure) | `state.cas_rejected` | `cell`, `expected_version`, `actual_version` |
| `state_delete` | `state.delete` | `cell`, `prev_value`, `prev_version` |
| `state_batch_set` | `state.batch_set` | `cells: [{cell, value, prev_value, version}]` |

CAS rejections are logged as events too. This is critical for debugging contention patterns — an agent can ask "why did my CAS keep failing?" and search through `state.cas_rejected` events.

### Event Sequence Naming

Audit events go to a dedicated event space per state space:

```
State space: "default"  →  Event type prefix: "state.*" in space "default"
State space: "pipeline" →  Event type prefix: "state.*" in space "pipeline"
```

Events share the same branch and space as the state cell they're tracking. This means:
- Branch isolation is preserved (branch A's audit trail is separate from branch B's)
- Space scoping works naturally (pipeline state changes stay in the pipeline space)
- Time-travel reads on events align with time-travel reads on state

### Configuration

Audit trail is opt-in per database, controllable at two levels:

```rust
// Database level (StrataConfig)
pub struct AuditConfig {
    /// Master switch for state→event audit trail
    pub enabled: bool,
    /// Include previous values in audit events (increases storage but enables diff queries)
    pub include_prev_values: bool,
    /// Log CAS rejections (useful for debugging contention, noisy in high-contention scenarios)
    pub log_rejections: bool,
    /// Optional actor field populated from session/connection context
    pub actor_source: ActorSource,
}

pub enum ActorSource {
    /// No actor tracking
    None,
    /// Static string (e.g., service name)
    Static(String),
    /// Read from session metadata key
    SessionKey(String),
}
```

This is intentionally simple. The audit trail should be a light switch, not a control panel.

### Implementation: Where It Hooks In

The natural integration point is the **executor handler layer** (`crates/executor/src/handlers/state.rs`), not the engine layer. This keeps the engine primitive pure and composable, and lets the executor orchestrate cross-primitive coordination.

```rust
// In state_set handler (simplified)
pub fn state_set(p: &Primitives, cmd: StateSet) -> Result<Output, Error> {
    let branch_id = resolve_branch(&cmd.branch, p)?;
    let space = cmd.space.unwrap_or_default();

    // 1. Read previous value (for audit trail)
    let prev = if p.config().audit.include_prev_values {
        p.state().get_versioned(branch_id, &space, &cmd.cell)?
    } else {
        None
    };

    // 2. Perform the state set
    let version = p.state().set(branch_id, &space, &cmd.cell, cmd.value.clone())?;

    // 3. Emit audit event (best-effort, same pattern as auto-embed)
    if p.config().audit.enabled {
        let payload = build_audit_payload(
            "set", &cmd.cell, &cmd.value, prev.as_ref(), &version,
            p.config().audit.actor_source.resolve(p),
        );
        if let Err(e) = p.event().append(branch_id, &space, "state.set", payload) {
            tracing::warn!("audit trail emit failed: {e}");
        }
    }

    // 4. Auto-embed (existing hook, unchanged)
    // ...

    Ok(Output::Version(version))
}
```

Key design decisions:
- **Best-effort emission**: Audit event failure never fails the state write. Same pattern as auto-embed hooks — log warnings, never propagate errors.
- **Same transaction? No.** The state write and the event append happen as separate operations. This is intentional — the audit trail is an observer, not a participant. If we made them transactional, a failure in the event log would roll back the state write, which violates the principle that the audit trail should never impact the primary operation.
- **Previous value read**: Only performed when `include_prev_values` is enabled. This adds one extra read per write, but it's a local cache hit in almost all cases (the state was just read for CAS or validation).

### Estimated Scope

| File | Change | Lines |
|------|--------|-------|
| `crates/engine/src/database/config.rs` | Add `AuditConfig` to `StrataConfig` | ~30 |
| `crates/executor/src/handlers/state.rs` | Add audit emission after each mutation | ~80 |
| `crates/executor/src/handlers/audit.rs` | New: `build_audit_payload()` helper + `ActorSource::resolve()` | ~60 |
| `crates/executor/src/command.rs` | Add `ConfigureAudit` command variant | ~10 |
| `crates/executor/src/handlers/config.rs` | Handle audit config command | ~20 |
| `crates/cli/src/commands.rs` | Add `audit` subcommand group | ~40 |
| `crates/cli/src/parse.rs` | Parse audit commands | ~30 |
| **Total** | | **~270** |

## Integration with Product Roadmap

### Agent-First Observability (#1274)

The `describe()` endpoint already plans to surface `events: { sequences: ["audit_log"], count: 5420 }`. With the audit trail, this becomes real and automatic — every database with state cells gains a searchable audit log without any user configuration.

An agent using the MCP server can:
```
strata_describe() → sees state cells and audit event counts
strata_search("what changed in pipeline_status last hour") → finds audit events
strata_ask("why did the pipeline fail?") → RAG over audit trail + state values
```

The `explain` mode from #1274 can include audit trail metadata in search results: "Found via state.set audit event at sequence 147, showing pipeline_status changed from running to failed."

### Recursive Query Execution (#1272)

Tier 2's sandbox API should expose `db.event_get_by_type("state.set")` and `db.event_get_by_type("state.cas_rejected")`. This lets recursive queries reason about state change patterns:

```python
# RLM code generated by the LLM in Tier 2 sandbox
recent_failures = db.event_get_by_type("state.cas_rejected", limit=50)
contention_cells = Counter(e.payload.cell for e in recent_failures)
most_contended = contention_cells.most_common(3)
# Follow up with targeted search on those cells
for cell, count in most_contended:
    history = db.event_get_by_type("state.set", after=..., limit=10)
    # ... analyze patterns
```

This is a natural fit for the recursive query paradigm: the first iteration discovers which cells are interesting, the second iteration dives into their history.

### Native RAG — db.ask() (#1273)

Because audit events are auto-embedded into the vector index (via the existing auto-embed pipeline), they become searchable through `db.ask()`:

```
db.ask("What happened to the deployment pipeline between 2pm and 3pm?")
```

The search pipeline finds:
1. BM25 hits on "deployment" and "pipeline" in audit events
2. Vector similarity hits on semantic meaning
3. Time-range filtering narrows to the 2-3pm window
4. RRF fusion ranks the audit events
5. LLM generates a narrative: "At 2:15pm, pipeline_status was set to 'deploying'. At 2:23pm, a CAS rejection occurred on deployment_lock (expected version 5, actual version 6, indicating concurrent deployment attempt). At 2:31pm, pipeline_status was set to 'failed' with the previous value 'deploying'."

This is the "magical" capability — a database that can narrate its own history in natural language.

### Graph-Augmented Search (#1270)

If state cells have entity refs in the graph (e.g., a `pipeline_status` node connected to `service_A` and `team_backend` nodes), then graph-augmented search can traverse from a team node to find all audit events for that team's state cells:

```
Query: "What state changes did team_backend make?"
Graph walk: team_backend → [OWNS] → service_A → [HAS_STATE] → pipeline_status
Entity ref: State { branch_id, name: "pipeline_status" }
Audit events: filter by cell == "pipeline_status"
```

Phase 4's ontology-guided query expansion (#1270) would understand that "state changes" should map to `state.*` event types, and that team ownership is expressed through graph links.

## What This Enables That Nothing Else Can

Redis has key-value with CAS. Kafka has append-only event logs. No single system gives you:

1. **CAS-protected state** with **automatic, hash-chained audit events**
2. **Vector-searchable** audit trail (ask "what changed?" in natural language)
3. **Graph-connected** audit events (traverse from entity to its change history)
4. **Time-travel** across both dimensions (state values at time T, plus the event that caused the transition)
5. **Branch-isolated** audit (experiment branches get their own audit trail, discarded with the branch)
6. **Agent-queryable** via `db.ask("why did X fail?")` with source citations back to specific state transitions

The audit trail transforms state from a dumb value store into a **self-documenting, queryable operational record** that agents can reason about.

## Open Questions

1. **Retention policy**: Should audit events be subject to automatic pruning? State cells might have millions of mutations over time. Options: TTL-based, count-based, or leave it to the user.

2. **Batch atomicity**: For `batch_set`, should we emit one event per cell or one consolidated event? Per-cell is more searchable; consolidated is more efficient and represents the logical operation.

3. **Cross-branch audit**: When a branch is merged, should audit events from the source branch be replayed into the target branch's audit trail? This adds complexity but preserves history.

4. **Actor tracking at scale**: The `ActorSource::SessionKey` approach works for single-user scenarios. For multi-tenant or multi-agent scenarios, we may need richer actor identification (agent ID, session ID, request ID).

5. **Should failed CAS rejections trigger auto-embed?** They're useful for debugging but could be noisy in the vector index. Consider a separate configuration knob.
