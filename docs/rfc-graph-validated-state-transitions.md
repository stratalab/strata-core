# RFC: Graph-Validated State Transitions

**Status:** Proposal
**Related Issues:** #1270 (Graph-Augmented Search), #1272 (Recursive Query), #1274 (Agent-First API)

## Problem

State cells with CAS give you optimistic concurrency — you can ensure no one else changed the value between your read and write. But CAS doesn't enforce *what* values are valid. A `pipeline_status` cell can go from `"running"` to `"banana"` as long as the version matches.

Meanwhile, the Graph primitive has a full ontology system: object types with property schemas, link types with cardinality constraints, freeze/unfreeze lifecycle. This ontology system is currently used only for graph node/edge validation.

The insight: **a graph ontology is a state machine definition.** Object types are states, link types are transitions. If we let the graph ontology validate state transitions, we get a database-native state machine engine that:

- Prevents invalid transitions (you can't go from `"succeeded"` to `"running"`)
- Is discoverable by agents via `describe()` and `ontology_summary()`
- Is queryable via graph traversal ("what transitions are possible from the current state?")
- Is evolvable (add new states/transitions by updating the ontology)

No other database does this. State machines are typically application-layer code that's invisible to the database and impossible for agents to reason about.

## Design

### State Machine as Graph Ontology

A state machine is defined as a graph within the same database. Object types represent valid states, link types represent valid transitions.

```
# Define the state machine as a graph ontology

graph create pipeline_fsm

# Define valid states as object types
graph define-object-type pipeline_fsm '{
  "name": "idle",
  "properties": {},
  "description": "Pipeline is idle, waiting for trigger"
}'
graph define-object-type pipeline_fsm '{
  "name": "running",
  "properties": { "started_at": "string" },
  "description": "Pipeline is actively executing"
}'
graph define-object-type pipeline_fsm '{
  "name": "succeeded",
  "properties": { "completed_at": "string", "duration_ms": "number" },
  "description": "Pipeline completed successfully"
}'
graph define-object-type pipeline_fsm '{
  "name": "failed",
  "properties": { "error": "string", "failed_at": "string" },
  "description": "Pipeline failed with an error"
}'
graph define-object-type pipeline_fsm '{
  "name": "retrying",
  "properties": { "attempt": "number", "max_attempts": "number" },
  "description": "Pipeline is retrying after failure"
}'

# Define valid transitions as link types
graph define-link-type pipeline_fsm '{
  "name": "start",
  "source_type": "idle",
  "target_type": "running",
  "description": "Trigger pipeline execution"
}'
graph define-link-type pipeline_fsm '{
  "name": "complete",
  "source_type": "running",
  "target_type": "succeeded",
  "description": "Pipeline finished successfully"
}'
graph define-link-type pipeline_fsm '{
  "name": "fail",
  "source_type": "running",
  "target_type": "failed",
  "description": "Pipeline encountered an error"
}'
graph define-link-type pipeline_fsm '{
  "name": "retry",
  "source_type": "failed",
  "target_type": "retrying",
  "description": "Attempt to re-run after failure"
}'
graph define-link-type pipeline_fsm '{
  "name": "resume",
  "source_type": "retrying",
  "target_type": "running",
  "description": "Retrying re-enters running state"
}'
graph define-link-type pipeline_fsm '{
  "name": "reset",
  "source_type": "succeeded",
  "target_type": "idle",
  "description": "Reset pipeline for next run"
}'
graph define-link-type pipeline_fsm '{
  "name": "reset",
  "source_type": "failed",
  "target_type": "idle",
  "description": "Abandon retry and reset to idle"
}'

# Lock the state machine definition
graph freeze-ontology pipeline_fsm
```

### Binding State Cells to State Machines

A new command binds a state cell to a graph-defined state machine:

```
state bind pipeline_status --fsm pipeline_fsm
```

This creates a binding record:

```rust
pub struct StateMachineBinding {
    /// The state cell name
    pub cell: String,
    /// The graph containing the FSM ontology
    pub fsm_graph: String,
    /// How the cell value maps to states (see below)
    pub value_mapping: ValueMapping,
}

pub enum ValueMapping {
    /// Cell value is a string matching an object type name
    /// e.g., value "running" → object type "running"
    StringField,
    /// Cell value is a JSON object with a designated field containing the state name
    /// e.g., value {"status": "running", "progress": 50} with field "status"
    ObjectField(String),
}
```

### Validation on Write

When a bound state cell is written (`set` or `cas`), the handler validates the transition:

```rust
// Pseudocode for validated state_set
fn state_set_validated(
    p: &Primitives,
    cell: &str,
    new_value: &Value,
    binding: &StateMachineBinding,
) -> Result<Version, Error> {
    // 1. Extract current state name from current cell value
    let current_value = p.state().get(branch_id, space, cell)?;
    let current_state = extract_state_name(current_value.as_ref(), &binding.value_mapping);

    // 2. Extract target state name from new value
    let target_state = extract_state_name(Some(new_value), &binding.value_mapping)?;

    // 3. Check if a link type exists from current_state → target_state
    let valid = p.graph().ontology_has_link(
        &binding.fsm_graph,
        &current_state, // source object type
        &target_state,  // target object type
    )?;

    if !valid {
        return Err(Error::InvalidTransition {
            cell: cell.to_string(),
            from: current_state,
            to: target_state,
            fsm: binding.fsm_graph.clone(),
            // Include valid transitions for agent-friendly error messages
            valid_targets: p.graph().ontology_valid_targets(
                &binding.fsm_graph, &current_state
            )?,
        });
    }

    // 4. Validate target state properties against object type schema
    if let ValueMapping::ObjectField(_) = &binding.value_mapping {
        p.graph().validate_properties(
            &binding.fsm_graph,
            &target_state,
            new_value,
        )?;
    }

    // 5. Proceed with normal state_set
    p.state().set(branch_id, space, cell, new_value.clone())
}
```

Special cases:
- **First write (cell doesn't exist)**: Any state is valid as an initial state. Optionally, mark specific object types as `initial: true` in their definition.
- **Delete**: Always allowed (removing a cell doesn't require a transition).
- **Unbound cells**: No validation. This is opt-in, not forced.

### Error Messages (Agent-First, #1274)

Invalid transitions produce rich, actionable errors:

```json
{
  "error": "InvalidTransition",
  "message": "Cannot transition pipeline_status from 'succeeded' to 'running'",
  "cell": "pipeline_status",
  "current_state": "succeeded",
  "attempted_state": "running",
  "fsm": "pipeline_fsm",
  "valid_transitions": [
    {
      "target": "idle",
      "transition": "reset",
      "description": "Reset pipeline for next run"
    }
  ],
  "suggestion": "To re-run the pipeline, first reset to 'idle' via the 'reset' transition, then 'start' to enter 'running'"
}
```

This is designed for LLM consumption (#1274). An agent receiving this error can:
1. Understand *why* the transition failed
2. See *what* transitions are valid
3. Get a natural-language suggestion for how to achieve its goal
4. Self-correct without human intervention

### Discoverability via describe() and ontology_summary()

The `describe()` endpoint (#1274) surfaces FSM bindings:

```json
{
  "state": {
    "cells": ["pipeline_status", "last_sync", "deploy_lock"],
    "fsm_bindings": {
      "pipeline_status": {
        "fsm_graph": "pipeline_fsm",
        "states": ["idle", "running", "succeeded", "failed", "retrying"],
        "current_state": "idle"
      }
    }
  }
}
```

The `ontology_summary()` for the FSM graph returns the complete state machine definition — states, transitions, descriptions — in a format an LLM can reason about.

### Implementation

#### New Commands

| Command | Description |
|---------|-------------|
| `StateBind { cell, fsm_graph, value_mapping }` | Bind a state cell to an FSM graph |
| `StateUnbind { cell }` | Remove FSM binding |
| `StateValidTransitions { cell }` | Query valid transitions from current state |
| `StateBindings { prefix? }` | List all FSM bindings |

#### Storage

Bindings are stored as a JSON document in a well-known key:

```
Key: __fsm_binding__{cell_name}
TypeTag: State
Value: serialized StateMachineBinding
```

This keeps bindings in the same storage layer as state cells, scoped by branch and space.

#### Validation Hook Point

Same as the audit trail: the executor handler layer. The engine primitive stays pure.

```rust
// In state_set handler
pub fn state_set(p: &Primitives, cmd: StateSet) -> Result<Output, Error> {
    // Check for FSM binding
    if let Some(binding) = get_fsm_binding(p, branch_id, &space, &cmd.cell)? {
        validate_transition(p, branch_id, &space, &cmd.cell, &cmd.value, &binding)?;
    }

    // Normal state_set flow
    let version = p.state().set(branch_id, &space, &cmd.cell, cmd.value)?;

    // Audit trail emission (if enabled, from RFC: State-Event Audit Trail)
    // ...

    Ok(Output::Version(version))
}
```

#### Estimated Scope

| File | Change | Lines |
|------|--------|-------|
| `crates/executor/src/command.rs` | Add `StateBind`, `StateUnbind`, `StateValidTransitions`, `StateBindings` | ~40 |
| `crates/executor/src/handlers/state.rs` | Add validation hook in `state_set`, `state_cas`, `state_init` | ~60 |
| `crates/executor/src/handlers/fsm.rs` | New: FSM binding CRUD + transition validation logic | ~200 |
| `crates/executor/src/output.rs` | Add `ValidTransitions` output variant | ~10 |
| `crates/engine/src/primitives/graph.rs` | Add `ontology_has_link()`, `ontology_valid_targets()` helpers | ~40 |
| `crates/cli/src/commands.rs` | Add `state bind`, `state unbind`, `state transitions`, `state bindings` | ~60 |
| `crates/cli/src/parse.rs` | Parse new state subcommands | ~50 |
| **Total** | | **~460** |

## Integration with Product Roadmap

### Graph-Augmented Search (#1270)

The FSM graph is a regular graph — it participates in graph-augmented search. This means:

**Ontology-guided query understanding (Phase 4)**: When an agent asks "what happened to the pipeline?", the LLM sees the `pipeline_fsm` ontology in `ontology_summary()` and understands the state machine structure. It can decompose the query into:
1. Graph traversal: current state of `pipeline_status` node
2. Text search: audit events matching "pipeline"
3. Graph walk: valid transitions from current state

**Graph-context reranking (Phase 2)**: Search results about pipeline state can be enriched with FSM context — "this state cell is currently in the 'failed' state, valid next transitions are 'retry' and 'reset'."

### Recursive Query Execution (#1272)

Tier 2's sandbox can expose FSM-aware operations:

```python
# RLM sandbox code
current = db.state_get("pipeline_status")
transitions = db.state_valid_transitions("pipeline_status")

if "retry" in [t.transition for t in transitions]:
    # Analyze retry history before recommending
    retries = db.event_get_by_type("state.cas", limit=10)
    retry_count = sum(1 for e in retries if e.payload.value == "retrying")

    if retry_count > 3:
        result = "Pipeline has been retried 3+ times. Recommend 'reset' to idle and investigate root cause."
    else:
        result = f"Recommend 'retry' transition (attempt {retry_count + 1})"
```

The recursive query can reason about the state machine, check transition history, and make informed recommendations — all within the database.

### Agent-First API (#1274)

FSM bindings are the poster child for the agent-first design philosophy:

1. **`describe()`** surfaces FSM bindings with current states — the agent immediately knows which cells have state machine semantics
2. **Actionable errors** on invalid transitions include valid alternatives — the agent self-corrects
3. **`ontology_summary()`** gives the agent the complete state machine definition in one call
4. **`state_valid_transitions()`** tells the agent exactly what it can do next — no trial-and-error

This eliminates the "blind agent" problem where an LLM has to guess valid state values. The database tells the agent what's allowed.

### Combined with Audit Trail

When both the audit trail (RFC: State-Event Audit Trail) and FSM validation are active, you get:

1. Agent sets `pipeline_status` to `"running"` (valid transition from `"idle"`)
2. Audit event emitted: `state.set` with `from: "idle"`, `to: "running"`, `transition: "start"`
3. The audit event includes the transition name, making the log semantically rich
4. Later, an agent asks: "Show me the pipeline's state transition history"
5. `db.ask()` searches audit events, the LLM sees FSM-annotated transitions, and generates:

   > "The pipeline started at 2:15pm (idle → running via 'start'), failed at 2:23pm (running → failed via 'fail'), was retried at 2:25pm (failed → retrying via 'retry'), re-entered running at 2:25pm (retrying → running via 'resume'), and succeeded at 2:31pm (running → succeeded via 'complete')."

This narrative is only possible because the database knows both the state machine and the transition history.

## What This Enables That Nothing Else Can

- **PostgreSQL**: You can model state machines with constraints and triggers, but they're invisible to query planners and impossible to introspect programmatically. An agent can't ask "what transitions are valid?" without reading application code.
- **Redis**: State machines are pure application logic. Redis has no concept of valid transitions.
- **Temporal/Step Functions**: Workflow engines enforce state machines, but they're separate systems. The state machine definition isn't queryable alongside the data it governs.

Strata makes the state machine a **first-class, queryable, graph-defined entity** that lives in the same database as the data, is discoverable by agents, and produces a rich audit trail.

## Open Questions

1. **Frozen ontology requirement**: Should FSM graphs be required to have a frozen ontology? A frozen ontology guarantees the state machine definition is stable. An unfrozen one allows runtime evolution but risks breaking active state cells. Recommendation: require frozen for `state bind`, allow `unfreeze` only after `state unbind`.

2. **Initial state enforcement**: Should we require marking specific object types as valid initial states? Currently any state is valid for a new cell. Strictness here prevents accidental creation of cells in invalid states.

3. **Transition metadata**: Link types have `weight` and `properties`. Should transition validation check edge properties (e.g., "retry transition requires `attempt < max_attempts`")? This would make FSMs much more powerful but significantly more complex.

4. **Multi-field state**: The `ValueMapping::ObjectField` approach handles `{"status": "running", "progress": 50}`, but what about state that's determined by multiple fields? Keep it simple for now — one field maps to one state.

5. **Performance**: FSM validation adds a graph ontology lookup to every state write. The ontology is small (typically <50 states/transitions) and cached, so this should be <1ms overhead. Benchmark to confirm.

6. **Concurrent FSM evolution**: What happens if someone modifies the FSM graph while state cells are bound to it? The frozen ontology requirement (question 1) mostly solves this, but edge cases remain around `unfreeze → modify → refreeze` sequences.
