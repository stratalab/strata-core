# Workstream A: Branch DAG and Status Ownership

This note audits the branch DAG and branch-status path across `engine`,
`graph`, and `executor`.

The question is not whether the DAG exists.
It does.
The question is whether it is authoritative runtime state.
It is not.

## Scope

- `crates/engine/src/branch_ops/dag_hooks.rs`
- `crates/engine/src/primitives/branch/index.rs`
- `crates/engine/src/branch_ops/mod.rs`
- `crates/graph/src/branch_dag.rs`
- `crates/graph/src/branch_status_cache.rs`
- `crates/graph/src/dag_hook_impl.rs`
- `crates/executor/src/handlers/branch.rs`
- `crates/executor/src/api/mod.rs`
- `crates/executor/src/types.rs`
- `crates/executor/src/bridge.rs`
- `crates/core/src/branch_dag.rs`

## Runtime role

The branch DAG is a graph stored on the `_system_` branch in
`_branch_dag`.
It records branch create, delete, fork, merge, revert, and cherry-pick
events.

But the engine does not own it as authoritative branch state.

The actual runtime model is split:

- engine owns branch existence and storage isolation
- executor owns DAG hook registration and DAG-based merge-base lookup
- graph owns DAG persistence and the richer branch-status vocabulary

That split matters because the executor uses DAG data to improve merge-base
selection, but the engine treats the DAG as optional best-effort side data.

## Confirmed architecture

### 1. The DAG is an optional sidecar, not core branch state

The engine only defines hook signatures in
`crates/engine/src/branch_ops/dag_hooks.rs`.
The hook bundle is process-global `OnceCell` state.
If nothing registers the hooks, branch operations still succeed and simply
skip DAG writes.

The graph implementation in
`crates/graph/src/dag_hook_impl.rs` explicitly treats every hook as
best-effort.
Failures are logged and never fail the underlying branch operation.

`init_system_branch()` in `crates/graph/src/branch_dag.rs` is also
best-effort.
If `_system_` or `_branch_dag` initialization fails, open still succeeds.

Conclusion:

- branch lineage recording is not part of the engine's correctness boundary

### 2. Executor depends on the DAG selectively

`crates/executor/src/handlers/branch.rs` computes a
`merge_base_override` from DAG history by calling:

- `find_last_merge_version()`
- `find_fork_version()`

That override is then passed into:

- `merge_branches_with_metadata()`
- `diff_three_way()`
- `get_merge_base()`
- `cherry_pick_from_diff()`

So the executor does rely on DAG history for repeated merges and for
materialized branches where storage-level fork ancestry is gone.

But branch creation, existence, listing, and deletion do not use DAG state
as the source of truth.
Those still use engine branch metadata.

Conclusion:

- DAG state is optional for the engine
- DAG state is operationally important for some executor behaviors
- the two layers do not agree on the DAG's authority

### 3. GraphSubsystem exists, but the standard runtime does not use it

`crates/graph/src/branch_dag.rs` defines `GraphSubsystem`, whose `recover()`
does exactly the two things the live runtime would need:

- `init_system_branch(db)`
- `load_status_cache_readonly(db)`

But there are no call sites for `with_subsystem(GraphSubsystem)`.
The standard executor builder only registers:

- `VectorSubsystem`
- `SearchSubsystem`

Instead, executor open manually calls `init_system_branch(&db)` on some
entrypoints.

That means:

- graph lifecycle is not represented in the subsystem model
- DAG initialization is open-path specific
- the status-cache recovery path is effectively dormant in normal runtime

### 4. Open-path behavior is inconsistent

Confirmed paths:

- `Strata::open_with()` direct local-open path calls `init_system_branch(&db)`
- `Strata::cache()` also calls `init_system_branch(&db)`
- `Strata::open_with()` stale-socket retry path does not call it
- `Strata::from_database_with_mode()` does not call it
- raw `Database::open*()` does not call it through `GraphSubsystem`

So branch DAG availability depends on which open path built the database.

Conclusion:

- DAG presence is composition-sensitive
- branch lineage is not uniformly initialized even inside the product layer

### 5. Identity is fragmented across three models

The branch DAG keys nodes by branch name.
For example, `dag_add_branch()`, `dag_mark_deleted()`,
`find_last_merge_version()`, and `find_fork_version()` all operate on names.

The executor resolves branch names to deterministic core `BranchId`s via
UUID v5 in `crates/executor/src/bridge.rs`.

The branch metadata stored by `BranchIndex` also carries a random UUID
inside `BranchMetadata::new`, and delete logic in
`crates/engine/src/primitives/branch/index.rs` has to reconcile the
executor-resolved ID with that metadata ID.

So there are at least three identities in play:

- DAG node identity: branch name
- storage / commit-lock identity: resolved core `BranchId`
- metadata-carried branch identity: random UUID string

Conclusion:

- the DAG does not share the engine's identity model
- lineage, storage isolation, and lifecycle are correlated by convention,
  not by one canonical identifier

### 6. Recreated branch names collapse generations

`BranchIndex::create_branch()` only checks whether branch metadata exists.
After `delete_branch()`, the name can be reused.

But the DAG node key is still the branch name.
`dag_mark_deleted()` preserves the node and flips status to `deleted`.
Later `dag_add_branch()` writes a fresh active node at the same name.

That means branch generations are conflated:

- old fork/merge/revert/cherry-pick edges still point at the same name-keyed node
- a recreated branch inherits the old DAG identity
- `find_last_merge_version()` and `find_fork_version()` can consult old
  lineage for a new branch generation

This is a real architectural bug, not just a cosmetic modeling issue.

Conclusion:

- DAG lineage is not generation-safe under branch-name reuse

### 7. Rich branch status exists only in the graph layer

`crates/core/src/branch_dag.rs` defines:

- `Active`
- `Archived`
- `Merged`
- `Deleted`

and `DagBranchStatus::is_writable()`.

But engine branch metadata only exposes `Active`.
Executor branch status also only exposes `Active`.
`branch_list()` in `crates/executor/src/handlers/branch.rs` explicitly
ignores the requested status filter.

No engine write path checks `DagBranchStatus`.
No executor branch mutation path checks `BranchStatusCache`.
The only hard lifecycle gate in the live runtime is the engine's
"branch is being deleted" path.

Conclusion:

- archived / merged / deleted statuses are descriptive graph metadata
- they are not authoritative lifecycle controls

### 8. BranchStatusCache is not part of the live control plane

`BranchStatusCache` is a `DashMap<String, DagBranchStatus>` extension.
It defaults missing entries to writable.

The only population path found is `load_status_cache_readonly()`, which is
called from `GraphSubsystem::recover()`.
But `GraphSubsystem` is not installed by the standard builder.

Even if it were populated, no live write path checks the cache.

Conclusion:

- the status cache is currently dormant and non-enforcing

## Main risks

### Risk 1: Executor correctness partially depends on optional metadata

Repeated-merge and materialized-branch merge-base correctness depends on
DAG history, but DAG writes are best-effort and registration-dependent.

If hooks are missing or DAG writes fail:

- branch operations still succeed
- lineage can become incomplete
- later merge-base selection degrades in ways the engine cannot detect

### Risk 2: Name-keyed lineage can be wrong after branch recreation

Because the DAG uses branch names as durable identity:

- delete + recreate reuses the same DAG node
- historical edges remain attached
- merge-base inference can consult stale ancestry from a prior branch generation

This is the sharpest issue in this audit.

### Risk 3: Status semantics are advertised but not enforced

The codebase has a richer branch-status model than the runtime actually uses.
That creates false confidence:

- "archived" does not make a branch read-only in engine write paths
- "merged" does not close a branch
- "deleted" in the DAG is not the authoritative delete gate

### Risk 4: Runtime behavior depends on which opener ran first

Because DAG hooks are process-global and DAG initialization is open-path
specific:

- some runtimes may have lineage recording
- some may not
- same-path instance reuse can hide composition mistakes

## Architectural judgment

The branch DAG is currently a mixed-purpose subsystem:

- audit trail
- queryable lineage graph
- merge-base hint store
- attempted branch-status model

But it is not promoted cleanly into authoritative branch lifecycle state.

That leaves Strata in an unstable middle ground:

- too important to be optional
- too optional to be trusted

## Recommended follow-up

1. Decide whether the branch DAG is authoritative or advisory.
2. If authoritative, move initialization and recovery into the standard
   subsystem stack and make merge-base correctness engine-owned.
3. If advisory, remove executor correctness dependence on DAG state.
4. Replace name-keyed lineage identity with a stable branch-generation ID.
5. Unify branch status across core, engine, executor, and graph, or strip
   the unused richer status model until it is enforceable.
