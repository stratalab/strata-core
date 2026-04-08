# Gap 04: Branch Semantics Depend on Embedding-Level Registration

## Summary

Several important branch behaviors are not intrinsic to the engine runtime. They only become
active if an embedding layer performs global startup registration for graph merge, vector
merge, and branch-DAG hooks.

The engine therefore has multiple semantic modes depending on who opened it and what global
registrations happened first.

## What The Code Does Today

### Graph merge is callback-registered

The graph crate documents that test fixtures and application startup must call
`register_graph_semantic_merge()` before any `merge_branches` invocation.

If registration never happens, the engine falls back to tactical refusal of divergent graph
merges.

Code evidence:

- `crates/graph/src/merge_handler.rs:18-24`
- `crates/graph/src/merge_handler.rs:39-46`
- `crates/engine/src/branch_ops/primitive_merge.rs:1063-1080`

### Vector merge is callback-registered

The vector crate documents the same startup requirement for
`register_vector_semantic_merge()`.

If registration does not happen:

- vector precheck becomes a no-op
- vector merge falls back to generic KV classification
- no in-memory HNSW rebuild happens until a later full recovery

Code evidence:

- `crates/vector/src/merge_handler.rs:36-42`
- `crates/vector/src/merge_handler.rs:55-61`
- `crates/engine/src/branch_ops/primitive_merge.rs:902-928`
- `crates/engine/src/branch_ops/primitive_merge.rs:982-1044`

### Branch-DAG bookkeeping is also hook-registered

The engine side defines process-global DAG hooks through `OnceCell`.
If no hook bundle is registered, branch operations proceed without DAG bookkeeping.

Code evidence:

- `crates/engine/src/branch_ops/dag_hooks.rs:121-151`

The branch operations call those hooks only if present:

- fork
- merge
- revert
- cherry-pick

Code evidence:

- `crates/engine/src/branch_ops/mod.rs:878-884`
- `crates/engine/src/branch_ops/mod.rs:2108-2112`
- `crates/engine/src/branch_ops/mod.rs:2832-2834`
- `crates/engine/src/branch_ops/mod.rs:2937-2938`

### Production startup compensates for this

The executor explicitly performs the registrations in `ensure_merge_handlers_registered()`.

Code evidence:

- `crates/executor/src/api/mod.rs:71-99`

## Why This Is An Architectural Gap

This is bigger than a normal plugin boundary.

The affected behaviors are not optional extras. They materially change branch semantics:

- whether divergent graph merges are possible
- whether vector merges rebuild runtime indexes correctly
- whether branch lineage events are recorded
- whether repeated-merge ancestry can be reconstructed from DAG state

That means the engine's effective behavior depends on startup choreography outside the core
database open path.

## Failure Modes

### Silent semantic downgrade

A caller can successfully open a database and run branch operations, but get degraded
behavior because registration never happened.

Examples:

- graph merges reject cases production would handle
- vector merges leave HNSW state stale until a later reopen
- branch DAG history is missing even though branch operations "worked"

### Different embedders can expose different branch semantics over the same engine

The executor registers the full set. A lower-level embedder that uses `DatabaseBuilder`
directly may install subsystems but still forget the global merge-handler registration.

That leads to a real semantic split between "engine opened successfully" and "engine is
running with the production branch semantics."

## Why The Current Layering Exists

The code comments make the reason clear: the engine cannot depend directly on graph or
vector without creating cyclic dependencies.

That explains the registration pattern, but it does not eliminate the architectural cost.
The current solution is workable, but it leaves correctness-sensitive behavior outside the
engine's normal composition boundary.

## Likely Remediation Direction

Inference from the current layering:

1. Keep the dependency cycle broken, but make capability registration part of an explicit
   runtime bundle instead of an ambient global startup side effect.
2. Fail fast when required handlers for enabled primitives are missing, rather than allowing
   silent fallback.
3. Expose a single "production core runtime" constructor that installs:
   - subsystem recovery
   - merge handlers
   - branch DAG hooks
   as one unit.
4. Add startup diagnostics that report which semantic handlers are active for the process.

## Bottom Line

Branch behavior is currently composition-sensitive. The engine core does not own all of its
effective merge and lineage semantics; some of them only appear after external registration.
