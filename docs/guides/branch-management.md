# Branch Management Guide

This guide covers the complete CLI for creating, switching, listing, and deleting branches. For the conceptual overview, see [Concepts: Branches](../concepts/branches.md).

## Opening and Default Branch

When you open the CLI, a "default" branch is automatically created and set as current:

```
$ strata --cache
strata:default/default>
```

## Creating Branches

`branch create` creates a new empty branch. It does **not** switch to it:

```
$ strata --cache
strata:default/default> branch create experiment-1
OK
strata:default/default> branch create experiment-1
(error) BranchExists: branch "experiment-1" already exists
```

## Switching Branches

`use <branch>` changes the current branch. All subsequent data operations target the new branch:

```
$ strata --cache
strata:default/default> branch create my-branch
OK
strata:default/default> use my-branch
strata:my-branch/default> use nonexistent
(error) BranchNotFound: branch "nonexistent" does not exist
```

## Listing Branches

`branch list` returns all user-visible branch names:

```
$ strata --cache
strata:default/default> branch create branch-a
OK
strata:default/default> branch create branch-b
OK
strata:default/default> branch list
- branch-a
- branch-b
- default
```

The list deliberately excludes engine-internal branches whose names start with `_system`. Strata always has a `_system_` branch that auto-creates on database open and stores cross-branch metadata (the branch DAG, tags, and notes) — `branch list` filters it out so it doesn't pollute the user's view. See [Concepts: The `_system_` branch](../concepts/branches.md#the-_system_-branch) for what lives there and why you don't normally see it.

## Deleting Branches

`branch del` removes a branch and all its data (KV, Events, State, JSON, Vectors):

```
$ strata --cache
strata:default/default> branch create temp
OK
strata:default/default> branch del temp
OK
```

### Safety Rules

```
$ strata --cache
strata:default/default> branch create my-branch
OK
strata:default/default> use my-branch
strata:my-branch/default> branch del my-branch
(error) ConstraintViolation: cannot delete current branch
strata:my-branch/default> use default
strata:default/default> branch del my-branch
OK
strata:default/default> branch del default
(error) ConstraintViolation: cannot delete default branch
```

## Branch Info

Get detailed information about a branch:

```
$ strata --cache
strata:default/default> branch create my-branch
OK
strata:default/default> branch info my-branch
id: my-branch
status: active
```

## Branch Existence Check

```
$ strata --cache
strata:default/default> branch create experiment
OK
strata:default/default> branch exists experiment
true
strata:default/default> branch exists nonexistent
false
```

## Fork a Branch

Fork creates a copy-on-write copy of the current branch. No data is copied — the destination shares storage with the source through inherited segment layers, and only diverging writes create new data. Forking a branch with a million keys costs the same as forking an empty one.

```
$ strata --cache
strata:default/default> kv put key value
(version) 1
strata:default/default> branch fork experiment-1
OK
strata:default/default> use experiment-1
strata:experiment-1/default> kv get key
"value"
```

The destination branch reads through to the source until it diverges. Writes on the destination land only in the destination — the source is never disturbed.

For when to fork (per-session isolation, A/B experiments, hypothesis exploration), see [Branching Strategies](branching-strategies.md). For the storage model, see [Concepts: Branches §Storage model](../concepts/branches.md#storage-model-copy-on-write).

## Diff Branches

Compare two branches and get a per-space breakdown of what's different:

```bash
strata --cache branch diff branch-a branch-b
```

The output groups changes by space and reports:

- **added** — keys present in `branch-b` but not `branch-a`
- **removed** — keys present in `branch-a` but not `branch-b`
- **modified** — keys present in both with different values

For the full result schema (`BranchDiffResult`, `SpaceDiff`, `BranchDiffEntry`, `DiffSummary`), see the [Branching API Reference §Diff](../reference/branching-api.md#diff). For point-in-time diffs and primitive/space filtering, see [`diff_with_options`](../reference/branching-api.md#diff).

## Merge Branches

Merge data from a source branch into the current target branch using a three-way merge.

```bash
# Last-writer-wins: source values overwrite target on conflict
strata --cache branch merge source --strategy lww

# Strict: fails if any conflicts exist
strata --cache branch merge source --strategy strict
```

The two strategies differ in how they handle cell-level conflicts:

| Strategy | What it does | When to use |
|---|---|---|
| `lww` (`LastWriterWins`) | Source's value wins. Conflicts are still reported in the result for inspection. | Experiments, hypothesis branches, when source is authoritative |
| `strict` | The merge fails on the first conflict. Target is unchanged. | Production merges where you want the system to refuse divergence |

**Some conflicts are always fatal regardless of strategy** — vector dimension mismatches, graph dangling edges, event hash-chain divergence. These represent structural impossibilities and need to be resolved manually before re-merging.

**The merge is per-primitive.** KV uses cell-level classification, JSON walks each document recursively and merges disjoint paths automatically, Vector rebuilds affected HNSW collections, Graph validates referential integrity, and Event refuses divergent appends to the same stream. Every primitive's merged state is durable in the WAL by the time the merge call returns.

For the full per-primitive semantics and the error reference, see [Merge Semantics](merge-semantics.md). For the API surface and result fields, see [Branching API Reference §Merge](../reference/branching-api.md#merge).

## Cherry-Pick (SDK)

Cherry-pick lets you copy a filtered subset of changes from source into target. It goes through the same per-handler dispatch as merge — JSON path-level merge, vector HNSW rebuild, and graph referential integrity all still apply to the picked subset.

Cherry-pick is currently SDK-only (not exposed in the CLI). From Rust:

```rust
use strata_engine::branch_ops::CherryPickFilter;
use strata_core::PrimitiveType;

// Take only JSON changes from "source", and only in the "products" space.
let filter = CherryPickFilter {
    spaces: Some(vec!["products".to_string()]),
    primitives: Some(vec![PrimitiveType::Json]),
    keys: None,
};
db.branches().cherry_pick_filtered("source", "target", filter)?;
```

The filter has three orthogonal dimensions (`spaces`, `primitives`, `keys`) that AND together. See [Branching API Reference §Cherry-pick](../reference/branching-api.md#cherry-pick) for the complete schema and [Branching Strategies §Selective change adoption](branching-strategies.md#selective-change-adoption-cherry-pick) for typical patterns.

## Materialize, Revert, Tag, Note (SDK)

These four operations are SDK-only.

**Materialize** collapses inherited COW layers on a long-lived fork chain so reads stop walking up the inheritance chain. Use it when a deep fork chain starts hurting read performance. Materialize is engine-level only (no CLI form, no `db.branches()` method):

```rust
use strata_engine::branch_ops;
branch_ops::materialize_branch(&engine_db, "leaf-branch")?;
```

**Revert** undoes a version range on a branch, restoring keys to their pre-range state while preserving any work done after the range:

```rust
db.branches().revert("main", 100, 150)?;
```

**Tag** bookmarks a branch at a specific MVCC version. Tags are immutable once created. Use them for release points and stable references:

```rust
db.branches().create_tag("main", "v1.0", Some("first GA release"))?;
let tag = db.branches().resolve_tag("main", "v1.0")?;
```

**Note** attaches a version-annotated message to a branch. Use notes for decision context, debug breadcrumbs, or audit trails:

```rust
db.branches().add_note("main", db.current_version(), "approved by SOC2 review", Some("alice"), None)?;
let notes = db.branches().get_notes("main", None)?;
```

See [Branching API Reference](../reference/branching-api.md) for the complete signatures, and [Branching Strategies §Branch lifecycle hygiene](branching-strategies.md#branch-lifecycle-hygiene) for when to use each.

## Shell Mode

All CLI branch operations work from the shell too:

```bash
strata --cache branch create experiment
strata --cache branch list
strata --cache branch exists experiment
strata --cache --branch experiment kv put key value
strata --cache branch fork experiment-2
strata --cache branch diff experiment experiment-2
strata --cache branch merge experiment-2 --strategy lww
strata --cache branch del experiment
```

## Next

- [Merge Semantics](merge-semantics.md) — what each merge does, per primitive
- [Branching Strategies](branching-strategies.md) — when to use fork, merge, cherry-pick, materialize, tag
- [Branching API Reference](../reference/branching-api.md) — complete API surface
- [Sessions and Transactions](sessions-and-transactions.md) — multi-operation atomicity
- [Branch Bundles](branch-bundles.md) — exporting and importing branches
