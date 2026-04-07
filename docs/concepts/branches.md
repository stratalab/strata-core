# Branches

A **branch** is an isolated namespace for data. All data in StrataDB lives inside a branch. Branches are the core isolation mechanism — they keep data from different agent sessions, experiments, or workflows separate from each other.

## The Git Analogy

If you know git, you already understand branches:

| Git | StrataDB | Description |
|-----|----------|-------------|
| Repository | Database | The whole storage, opened once per path |
| Working directory | CLI session | Your view into the database with a current branch |
| Branch | Branch | An isolated namespace for data |
| HEAD | Current branch | The branch all operations target |
| `main` | `"default"` | The auto-created branch you start on |

Just as git branches isolate file changes, branches isolate data changes. Switching branches changes which data you see, without copying anything.

## How Branches Work

When you open the CLI, you start on the **default branch**:

```
$ strata --cache
strata:default/default> kv put key value
(version) 1
```

You can create additional branches and switch between them:

```
$ strata --cache
strata:default/default> kv put key default-value
(version) 1
strata:default/default> branch create experiment
OK
strata:default/default> use experiment
strata:experiment/default> kv get key
(nil)
strata:experiment/default> kv put key experiment-value
(version) 1
strata:experiment/default> use default
strata:default/default> kv get key
"default-value"
```

## Data Isolation

Every primitive (KV, EventLog, StateCell, JSON, Vector) is isolated by branch. Data written in one branch is invisible from another:

```
$ strata --cache
strata:default/default> kv put kv-key 1
(version) 1
strata:default/default> state set cell active
(version) 1
strata:default/default> event append log '{"msg":"hello"}'
(seq) 1
strata:default/default> branch create isolated
OK
strata:default/default> use isolated
strata:isolated/default> kv get kv-key
(nil)
strata:isolated/default> state get cell
(nil)
strata:isolated/default> event len
0
```

## Branch Lifecycle

| Operation | CLI Command | Notes |
|-----------|-------------|-------|
| Create | `branch create <name>` | Creates an empty branch. Stays on current branch. |
| Switch | `use <branch>` | Switches current branch. Branch must exist. |
| List | `branch list` | Returns all branch names. |
| Delete | `branch del <name>` | Deletes branch and all its data. Cannot delete current or default branch. |
| Check info | `branch info <name>` | Returns branch details. |
| Check existence | `branch exists <name>` | Returns whether the branch exists. |

### Safety Rules

- You **cannot delete the current branch**. Switch to a different branch first.
- You **cannot delete the "default" branch**. It always exists.
- You **cannot switch to a branch that doesn't exist**. Create it first.
- Creating a branch does **not** switch to it. You must call `use` explicitly.

## When to Use Branches

| Scenario | Pattern |
|----------|---------|
| Each agent session gets its own state | One branch per session ID |
| A/B testing different strategies | One branch per variant |
| Safe experimentation | Fork-like: create branch, experiment, delete if bad |
| Audit trail | Keep completed branches around for review |
| Multi-tenant isolation | One branch per tenant |

## Branch Operations

For advanced branch operations:

```
$ strata --cache
strata:default/default> branch create my-branch
OK
strata:default/default> branch list
- default
- my-branch
strata:default/default> branch exists my-branch
true
strata:default/default> branch del my-branch
OK
```

Or from the shell:

```bash
strata --cache branch create my-branch
strata --cache branch list
strata --cache branch exists my-branch
strata --cache branch del my-branch
```

## How merges work

Strata supports three-way branch merges. When you merge `source` into `target`, the engine computes the **merge base** — the common ancestor where the two branches diverged (the fork point, or the version of a previous merge if the branches have been merged before) — then reads three snapshots: ancestor, source, and target. Each primitive's data is routed through a per-primitive handler that knows that primitive's invariants.

The handlers are not generic. **KV** uses a 14-case classification matrix at the cell level. **JSON** walks each document recursively and merges disjoint paths automatically. **Vector** rebuilds affected HNSW collections per-collection and refuses dimension/metric mismatches. **Graph** decodes adjacency lists, validates referential integrity, and merges disjoint nodes and edges additively. **Event** refuses divergent appends to the same hash-chained stream. The merge transaction is atomic across all five — every primitive's merged state is durable in the WAL by the time the merge call returns, and a crash mid-merge cannot leave the target half-merged.

Merges take a strategy: `Strict` refuses any conflict (target unchanged); `LastWriterWins` auto-resolves by taking the source side. Some conflicts are always fatal regardless of strategy — referential integrity violations, dimension mismatches, hash-chain divergence — because they represent structural impossibilities, not value disagreements. See the [Merge Semantics](../guides/merge-semantics.md) guide for the per-primitive details and the [Branching Strategies](../guides/branching-strategies.md) guide for when to use which.

## Storage model: copy-on-write

Forks are O(1). The destination branch shares storage with its parent through inherited segment layers — no data is copied at fork time. Reads on the child fall through to the parent's segments via the inheritance chain. Writes only ever land in the writing branch's own segments, so the parent is never disturbed by what its children do.

This means you can fork millions of branches and pay only the per-branch metadata cost. A branch that never writes costs essentially nothing in storage. A branch that diverges by 1% of the parent's data costs roughly 1% of the parent's storage.

The trade-off is read-path overhead on deep fork chains: a read on a 10-level-deep chain may walk up to 10 inherited layers before finding the latest version. For long-lived branches that serve heavy read traffic, **materialize** the branch to collapse its inherited layers into own segments. See [Branching Strategies §When to materialize](../guides/branching-strategies.md#when-to-materialize).

Under the hood, every key in storage is prefixed with its branch ID. Branch isolation is automatic — no filtering needed, because the keys are simply different. Deleting a branch is O(branch size), scanning only that branch's keys. Cross-branch operations (fork, diff, merge, cherry-pick, materialize, revert, tag, note) are described in the [Branch Management Guide](../guides/branch-management.md) and the [Branching API Reference](../reference/branching-api.md).

## Next

- [Primitives](primitives.md) — the six data types
- [Branch Management Guide](../guides/branch-management.md) — complete CLI walkthrough
- [Merge Semantics](../guides/merge-semantics.md) — what each merge does, per primitive
- [Branching Strategies](../guides/branching-strategies.md) — when to use fork, merge, cherry-pick, materialize, tag
- [Branching API Reference](../reference/branching-api.md) — every operation, every field
