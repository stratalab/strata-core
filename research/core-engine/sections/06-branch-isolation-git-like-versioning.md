# Branch Isolation (Git-like Versioning): Focused Analysis

This note is derived from the code, not from the existing docs.
Scope for this pass:

- `crates/engine/src/branch_ops/mod.rs`
- `crates/engine/src/branch_ops/primitive_merge.rs`
- `crates/engine/src/branch_ops/dag_hooks.rs`
- `crates/engine/src/primitives/branch/index.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/graph/src/branch_dag.rs`
- `crates/graph/src/branch_status_cache.rs`
- `crates/graph/src/dag_hook_impl.rs`
- `crates/core/src/branch_types.rs`
- `crates/executor/src/handlers/branch.rs`

## Executive view

The branch layer is real and fairly sophisticated.
The important pieces are:

1. zero-copy inherited-layer forks in storage
2. a COW-aware read path with `fork_version` visibility
3. on-demand and automatic materialization of deep ancestry chains
4. two-way diff, three-way merge, cherry-pick, revert, tags, and notes
5. optional DAG lineage and status helpers in the graph crate

The branch model is strongest at the storage level.
That is where the actual isolation lives.
The engine branch API on top of it is useful, but it also has more seams
than the inventory suggests.

The biggest gaps in this section are:

- branch identity and branch status are fragmented across multiple incompatible models
- fork is zero-copy for existing segments, but not zero-stall and not purely metadata-only
- merge-base correctness for repeated merges and materialized branches is externalized to DAG-derived overrides
- branch DAG and status tracking are optional runtime integrations, not intrinsic engine behavior
- `diff_three_way` is not using the same point-in-time snapshot discipline as the merge path
- follower mode is pull-based and best-effort, not a fully convergent replication protocol

## Inventory validation

| # | Feature | Verdict | Code-derived note |
|:-:|---------|---------|-------------------|
| 56 | O(1) Branch Fork | Mostly true | Existing segments are shared via inherited layers and refcounts rather than copied. But fork still forces source memtable flushes, requires a disk-backed database, and globally quiesces commits while capturing the snapshot (`crates/engine/src/branch_ops/mod.rs`, `crates/storage/src/segmented/mod.rs`). |
| 57 | Inherited Layers | True | Child branches carry ordered inherited layers with `source_branch_id`, `fork_version`, and a segment snapshot. Reads rewrite branch IDs and respect the layer's fork boundary (`crates/storage/src/segmented/mod.rs`). |
| 58 | Branch Materialization | True | `materialize_branch()` collapses inherited layers deepest-first, and background write/maintenance flow auto-materializes when inherited depth exceeds 4 (`crates/engine/src/branch_ops/mod.rs`, `crates/engine/src/database/transaction.rs`, `crates/storage/src/segmented/mod.rs`). |
| 59 | Two-Way Branch Diff | True | The engine has a general full-scan diff plus a COW-aware O(W) fast path for direct parent/child comparisons (`crates/engine/src/branch_ops/mod.rs`). |
| 60 | Three-Way Diff | Mostly true | Three-way classification exists and powers merge/cherry-pick planning. But the public `diff_three_way()` path reads current source/target state rather than a single captured snapshot version, so it is weaker under concurrency than the merge path (`crates/engine/src/branch_ops/mod.rs`). |
| 61 | Merge Base Computation | Partially true | The engine can derive merge base from direct storage fork info, but repeated merges and materialized branches depend on an externally supplied DAG-derived override (`crates/engine/src/branch_ops/mod.rs`, `crates/executor/src/handlers/branch.rs`). |
| 62 | Last-Writer-Wins Merge | Partially true, and the name is misleading | Conflict resolution is source-wins, not timestamp-based "newest value wins." On conflict the merge plan writes the source-side value into the target when using `LastWriterWins` (`crates/engine/src/branch_ops/mod.rs`). |
| 63 | Strict Merge | True | `Strict` aborts when any conflicts remain after planning, before writes are applied (`crates/engine/src/branch_ops/mod.rs`). |
| 64 | Cherry-Pick | Mostly true | There are two forms: `cherry_pick_keys()` copies current live values for specific keys, while `cherry_pick_from_diff()` performs a filtered diff-based selective merge that can also propagate deletes (`crates/engine/src/branch_ops/mod.rs`). |
| 65 | Version Revert | Mostly true | Range revert restores keys to their pre-range state, but only when the current value still matches the range-end state, preserving later edits (`crates/engine/src/branch_ops/mod.rs`). |
| 66 | Branch Tags | Mostly true | Tags are implemented as JSON-serialized KV rows on the `_system_` branch, keyed by `tag:{branch}:{name}` (`crates/engine/src/branch_ops/mod.rs`). |
| 67 | Version Notes | Mostly true | Notes are likewise `_system_` KV rows keyed by `note:{branch}:{version}`. This is one note slot per `(branch, version)`, not an append-only note log (`crates/engine/src/branch_ops/mod.rs`). |
| 68 | Branch DAG | Partially true | The graph crate implements a real DAG on the `_system_` branch, but engine branch ops only emit DAG events if hook implementations are registered out-of-band (`crates/engine/src/branch_ops/dag_hooks.rs`, `crates/graph/src/branch_dag.rs`, `crates/graph/src/dag_hook_impl.rs`). |
| 69 | Branch Status Tracking | Partially true | A `DashMap` cache exists, but it is optional, follower-oriented, and not authoritative. The actual code has multiple incompatible branch-status models, and the engine does not enforce DAG status on writes (`crates/graph/src/branch_status_cache.rs`, `crates/graph/src/branch_dag.rs`, `crates/engine/src/primitives/branch/index.rs`, `crates/core/src/branch_types.rs`). |
| 70 | Follower Mode | Partially true | Followers are read-only WAL-replay replicas with manual `refresh()`. They do not acquire the primary lock and can tail new WAL, but replication is pull-based and best-effort rather than continuous or strongly convergent (`crates/engine/src/database/open.rs`, `crates/engine/src/database/lifecycle.rs`). |

## Real execution model

### 1. Branch identity is more fragmented than it looks

There are at least three branch-identity and lifecycle models in the code:

- runtime storage/engine branch IDs derived deterministically from the branch name via `resolve_branch_name()`
- branch metadata records in `BranchIndex`, where `BranchMetadata::new()` stores a fresh random UUID string in `branch_id`
- DAG branch nodes keyed by branch name on the `_system_` branch

The mismatch is real enough that `delete_branch()` has to clean up both:

- the deterministic executor/runtime branch ID
- the random UUID stored in branch metadata, if it differs

That is a strong signal that the branch metadata record is not the
authoritative identity for the branch's actual storage namespace.

The same fragmentation exists in status:

- `engine::primitives::branch::BranchStatus` has only `Active`
- `core::branch_types::BranchStatus` has `Active / Completed / Orphaned / NotFound`
- `core::branch_dag::DagBranchStatus` has `Active / Archived / Merged / Deleted`

Those are different models for different layers, not one coherent branch
lifecycle state machine.

### 2. Fork is zero-copy for segments, but not zero-work

The fork path in `fork_branch_with_metadata()` does more than attach metadata.

At the engine level it:

- verifies source and destination branch metadata
- rejects forking from a branch being deleted
- acquires the global commit-quiesce write lock
- calls the storage COW fork
- creates branch metadata
- copies space registrations

At the storage level, `SegmentedStore::fork_branch()`:

- rotates the source active memtable
- flushes frozen memtables
- takes an exclusive branch guard on the source
- inline-flushes any straggler writes still sitting in the active memtable
- captures `fork_version` from the source branch's applied `max_version`
- snapshots the source segment version and inherited layers
- increments refcounts on every shared segment
- installs the inherited layer chain on the child
- writes the destination manifest

So the real claim is:

- no existing SST data is copied
- but pending source writes may be flushed to new SSTs during fork
- and all commits across the database are temporarily quiesced while the fork snapshot is taken

That is still a strong design.
It just is not "pure metadata only" in operational terms.

### 3. Inherited layers are the real branch-isolation mechanism

Child branches do not copy the parent's dataset.
They read parent state through inherited layers.

An inherited layer stores:

- `source_branch_id`
- `fork_version`
- a frozen `SegmentVersion`
- a materialization status

Reads from inherited layers rewrite the branch ID to the child and enforce
the layer's `fork_version`, so post-fork parent writes do not leak into the
child.

Layer order is nearest-ancestor-first.
That matters because closer layers shadow older ancestry, and later
materialization needs to process the deepest layer first.

### 4. Materialization turns inherited ancestry into owned data

Materialization is real and relatively careful.

`materialize_branch()` repeatedly materializes the deepest inherited layer.
`materialize_layer()`:

- flushes the child's own active/frozen memtables first
- snapshots the target inherited layer plus any closer layers
- marks the layer `Materializing`
- collects only entries not shadowed by the child's own state or by closer layers
- writes those entries into new L0 SSTs
- removes the inherited layer by identity
- decrements shared-segment refcounts and runs orphan GC

Automatic materialization is also real.
The storage layer flags branches whose inherited depth exceeds
`MAX_INHERITED_LAYERS = 4`, and the engine background transaction path
materializes one deepest layer per run.

This is why the branch model can stay zero-copy for shallow trees but
eventually collapse deep ancestry into local data.

### 5. Two-way diff is better than the inventory summary suggests

The two-way diff implementation has two modes.

The general path:

- lists spaces in both branches
- reads entries by type tag
- uses a captured `snapshot_version` for non-`as_of` diffs
- groups results by space and classifies added/removed/modified entries

The optimized path is more interesting.
If one branch is a direct child of the other and there is no timestamped
`as_of` query, `cow_diff_branches()` runs an O(W) diff:

- scan only the child's own entries
- scan only the parent's own entries written after `fork_version`
- point-lookup both branches for those potentially changed keys
- classify only the touched keys

That is a good example of the storage COW model paying off at the API layer.

### 6. Merge is a typed three-way planner plus one target-side OCC transaction

The merge pipeline is:

1. resolve source and target branch IDs
2. compute merge base
3. collect the union of spaces
4. capture a `snapshot_version`
5. gather typed ancestor/source/target entry slices
6. run per-primitive `precheck`
7. run per-primitive `plan`
8. reject conflicts early for `Strict`
9. register new spaces on the target
10. apply all merge actions in one target transaction
11. run per-primitive `post_commit`
12. fire the DAG merge hook best-effort

The typed handler layer matters:

- KV uses the generic 14-case classifier
- JSON has a semantic three-way merge that can merge disjoint document edits and update index rows atomically
- Event precheck refuses divergent event-stream merges in the same space
- Vector precheck/post-commit come from vector-crate callbacks
- Graph semantic merge comes from a graph-crate callback

The transaction apply step also performs target-side OCC validation by
reading the expected target value first.
That protects against target drift between diff and apply.

### 7. Merge base is only self-sufficient for direct live fork relationships

The engine's `compute_merge_base()` has a narrow fallback chain:

1. executor-provided `merge_base_override`
2. storage-level fork info from active inherited layers
3. no merge base

That works for direct live COW ancestry.
It does not cover the full logical branch history by itself.

The production runtime relies on `executor/src/handlers/branch.rs` to
supply a DAG-derived override:

- first prefer the latest `merge_version` between the branches
- otherwise prefer DAG-recorded fork version
- otherwise let the engine fall back to storage-level fork info

That is the real runtime story for repeated merges and materialized branches.

### 8. Three-way diff is not the same quality as the merge planner

This is an important asymmetry.

`merge_branches()` and `cherry_pick_from_diff()` gather typed entries using a
captured `snapshot_version`.
That is the stronger path.

The public `diff_three_way()` path does not.
It reads:

- ancestor at `merge_base.version`
- source current state via `list_by_type()`
- target current state via `list_by_type()`

So the inspection API can observe source and target at different effective
moments under concurrency, even though the merge planner itself is more careful.

### 9. Revert and cherry-pick are selective state transforms, not full replay

Revert is conservative:

- compute branch state before the range
- compute branch state at the end of the range
- compare current live state
- only revert keys whose current state still matches the range-end state

That preserves later edits rather than blindly rewinding the branch.

Cherry-pick also has two different semantics:

- `cherry_pick_keys()` copies current live source values for named keys
- `cherry_pick_from_diff()` performs a filtered three-way selective merge

Only the diff-based path can propagate deletes.

### 10. Tags and notes are `_system_`-branch metadata rows

Tags and notes are not special storage objects.
They are KV rows on the `_system_` branch:

- tags: `tag:{branch}:{name}`
- notes: `note:{branch}:{version}`

They are serialized as JSON strings.
This is simple and workable.
It also means they inherit normal KV semantics rather than a dedicated
metadata index or branch-history table.

### 11. DAG lineage and status are optional overlays

The engine itself only defines DAG hook signatures and a global `OnceCell`
registration point.

The graph crate provides:

- `_system_` branch initialization
- `_branch_dag` graph creation
- DAG write helpers
- follower-only status cache loading
- the hook implementation that records fork/merge/revert/cherry-pick/create/delete events

But none of that is intrinsic to `Database::open()`.
Default engine open hardcodes `SearchSubsystem`, not `GraphSubsystem`.
The executor works only because it performs extra setup:

- `ensure_merge_handlers_registered()` registers graph/vector merge callbacks and DAG hooks
- `init_system_branch(&db)` is called after open

So branch DAG lineage is a real subsystem, but not a built-in engine guarantee.

### 12. Follower mode is a manual pull replica

Follower open:

- acquires no exclusive lock
- has no WAL writer
- replays WAL and segments into local read-only state
- exposes `refresh()` to pull newer WAL records later

That is a useful read-scaling and cross-process-read feature.

But it is not continuous replication.
The follower only advances when `refresh()` is called.
And refresh is explicitly best-effort:

- corrupt payloads are skipped
- storage-apply failures are skipped
- watermark still advances to the highest seen `txn_id`

So follower mode is operationally "catch up as far as possible," not
"guaranteed exact convergence."

## Major architectural gaps in this section

### 1. Branch identity and status are architecturally fragmented

This is the deepest branch-layer gap.

The code has multiple incompatible models for "what branch is this?" and
"what state is this branch in?":

- deterministic branch IDs from branch name
- random UUIDs stored in `BranchMetadata`
- DAG nodes keyed by branch name
- at least three different branch-status enums across engine, core durability, and DAG

That is not just cosmetic.
It leaks into behavior:

- `delete_branch()` has to clean up both the deterministic namespace and the metadata UUID namespace
- `BranchMetadata.parent_branch` is never populated by fork
- `BranchMetadata.status` never advances beyond `Active`
- DAG status is a separate overlay, not the branch metadata truth

This should be unified.

### 2. Fork is zero-copy, but not cheap enough to treat as a pure metadata op

The inventory framing hides two real operational costs:

- fork globally quiesces all commits while it captures the snapshot
- fork may flush outstanding source memtable data to disk before the child is created

That means fork latency depends on pending write state, not just metadata churn.
For branch-heavy control-plane workloads, this is a meaningful database-wide
coordination point.

### 3. Merge-base correctness is still externalized

The engine core only understands direct storage fork relationships.
Repeated merges and fully materialized branches require an externally supplied
override from the DAG layer.

That means the correctness of high-level merge-base selection depends on:

- DAG event hooks being registered
- `_branch_dag` being initialized
- the executor actually supplying `merge_base_override`

Without that stack, branch merge is materially less complete.

### 4. Branch DAG and status tracking are optional, and status is mostly observational

The inventory overstates how intrinsic this is.

What is true:

- there is a DAG
- there is a status cache
- there are status helpers

What is not true:

- default engine open does not initialize or recover the graph subsystem
- DAG hooks are not registered by the engine itself
- the cache is not authoritative
- the engine does not consult DAG status before allowing writes
- merge/fork operations do not automatically move branches into `Merged` or `Archived`

In practice, only delete has an automatic DAG status mutation, and even that
depends on the hook layer being registered.

### 5. `LastWriterWins` is really source-wins, not time-based resolution

This is a semantic mismatch that matters.

The code does not compare timestamps, commit IDs, or causal order to find the
"latest writer."
On conflict it simply chooses the source-side action.

That is deterministic and may be acceptable.
But it is not what most engineers will infer from the name.

### 6. `diff_three_way()` is not snapshot-consistent enough

The inspection API is weaker than the merge path.
It does not capture source and target through one point-in-time snapshot.

That means:

- an operator can inspect a three-way diff that never actually existed as one stable state
- merge planning may see a different state than the inspection API just showed

For a branching system, that is a real observability seam.

### 7. Follower refresh can permanently diverge

This is the branching section's biggest follower-mode weakness.

If `refresh()` skips a record due to decode or storage failure, it still
advances the follower's watermark to the highest seen `txn_id`.
That record will not be retried on the next refresh.

So follower mode can become permanently behind the primary's logical history
while still claiming forward progress.

## Bottom line

The branch layer has a strong core:

- inherited-layer COW storage
- materialization
- COW-aware diff
- typed three-way merge
- selective revert and cherry-pick

But the architectural boundaries above that core are uneven.
Branch identity, status, merge-base computation, DAG lineage, and follower
replication are spread across multiple partially connected systems rather than
one coherent branch-runtime model.
