# Stage 1: Shadow-Stack Sweep

This pass looked for modules that appear runtime-authoritative from their API
shape, docs, or code completeness, but are not on Strata's live product path.

The goal was not just to find dead code.
It was to separate four cases:

1. confirmed dormant alternate runtime stacks
2. partial migrations where a newer stack exists beside the live one
3. live split-authority mechanisms that bypass the canonical subsystem path
4. false positives that only look suspicious in isolation

## Scope

- `crates/durability`
- `crates/engine`
- `crates/concurrency`
- `crates/storage`
- `crates/vector`
- `crates/graph`
- `crates/executor`

## Method

For each candidate WAL, snapshot, recovery, scheduler, or subsystem path, the
audit checked:

- whether higher-level open/commit/refresh/freeze code actually calls it
- whether it assumes a different on-disk layout
- whether it is only exercised by tests or crate-local examples
- whether the same concern is handled elsewhere by the live runtime

The live runtime references used as the comparison baseline were:

- `crates/executor/src/api/mod.rs`
- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/concurrency/src/recovery.rs`
- `crates/engine/src/recovery/subsystem.rs`

## Confirmed dormant shadow stacks

### 1. `durability::database` is a second database runtime

`crates/durability/src/database/handle.rs` exposes a complete database
lifecycle API:

- `DatabaseHandle::create`
- `DatabaseHandle::open`
- `DatabaseHandle::open_or_create`
- `DatabaseHandle::recover`
- `DatabaseHandle::append_wal`
- `DatabaseHandle::flush_wal`
- `DatabaseHandle::checkpoint`
- `DatabaseHandle::close`

It owns its own `ManifestManager`, `WalWriter`, `CheckpointCoordinator`, and
`durability::RecoveryCoordinator`.

Code anchors:

- `crates/durability/src/database/handle.rs:29`
- `crates/durability/src/database/handle.rs:173`
- `crates/durability/src/database/handle.rs:199`
- `crates/durability/src/database/handle.rs:222`

Why this is not the live path:

- the engine opens through `crates/engine/src/database/open.rs`, not through
  `DatabaseHandle`
- workspace caller search found non-test usage only inside
  `crates/durability/src/database/mod.rs`
- the live engine recovery path uses `strata_concurrency::RecoveryCoordinator`
  instead

Consequence:

- Strata still carries two persistence runtimes with different ownership
  boundaries and different filesystem assumptions

### 2. `durability::recovery` is a second recovery architecture

`crates/durability/src/recovery/coordinator.rs` implements a richer recovery
planner with explicit `MANIFEST -> snapshot -> WAL` logic.

Code anchors:

- `crates/durability/src/recovery/coordinator.rs:38`
- `crates/durability/src/recovery/coordinator.rs:60`
- `crates/durability/src/recovery/coordinator.rs:65`
- `crates/durability/src/recovery/coordinator.rs:89`

Why it is shadowing rather than assisting the live runtime:

- engine open uses `strata_concurrency::RecoveryCoordinator::new(wal_dir)` in
  `crates/engine/src/database/open.rs:452` and
  `crates/engine/src/database/open.rs:730`
- the durability coordinator expects a root database directory with uppercase
  `WAL/` and `SNAPSHOTS/`
- the live engine uses lowercase `wal/`, `segments/`, and `snapshots/`

Code anchors:

- `crates/durability/src/database/paths.rs:6`
- `crates/durability/src/database/paths.rs:43`
- `crates/durability/src/database/paths.rs:48`
- `crates/engine/src/database/open.rs:446`
- `crates/engine/src/database/open.rs:449`
- `crates/engine/src/database/compaction.rs:68`

Consequence:

- recovery logic is duplicated at architectural, not helper, level
- the richer manifest/checkpoint-aware stack cannot be trusted just because it
  exists

### 3. `vector::wal` is a dormant parallel WAL stack

`crates/vector/src/wal.rs` defines vector-specific WAL opcodes, payload types,
helper constructors, and a `VectorWalReplayer`.

Code anchors:

- `crates/vector/src/wal.rs:28`
- `crates/vector/src/wal.rs:42`
- `crates/vector/src/wal.rs:187`
- `crates/vector/src/wal.rs:295`

Why it is dormant:

- workspace caller search found no live non-test callers of
  `VectorWalReplayer`
- the live engine commit path already persists vector writes through the
  engine WAL and `TypeTag::Vector`
- vector recovery rebuilds from engine KV state plus mmap caches in
  `crates/vector/src/recovery.rs`

Code anchors:

- `crates/vector/src/recovery.rs:466`
- `crates/vector/src/recovery.rs:501`
- `crates/vector/src/lib.rs:66`

Consequence:

- Strata carries a second vector durability story that the engine never honors

### 4. `vector::snapshot` is a dormant parallel snapshot format

`crates/vector/src/snapshot.rs` provides deterministic vector-store snapshot
serialization and deserialization.

Code anchors:

- `crates/vector/src/snapshot.rs:97`
- `crates/vector/src/snapshot.rs:262`

Why it is dormant:

- workspace caller search found usage only in the module's own tests
- the live checkpoint path is engine-driven via
  `crates/engine/src/database/compaction.rs` and durability checkpoint
  sections, not vector-native snapshot files

Consequence:

- there are two snapshot narratives for vectors, but only one participates in
  actual restart behavior

### 5. `storage::CompactionScheduler` is an unused alternate scheduler

`crates/storage/src/compaction.rs` contains a full size-tiered
`CompactionScheduler` and `TierMergeCandidate` model.

Code anchors:

- `crates/storage/src/compaction.rs:205`
- `crates/storage/src/compaction.rs:212`
- `crates/storage/src/compaction.rs:223`

Why it is dormant:

- workspace caller search found no live non-test callers outside the re-export
  in `crates/storage/src/lib.rs`
- live compaction selection happens in the segmented storage engine and engine
  background flow, not through this scheduler object

Code anchors:

- `crates/storage/src/lib.rs:35`
- `crates/storage/src/segmented/compaction.rs`
- `crates/engine/src/background.rs`

Consequence:

- the codebase still carries an alternate compaction policy surface that looks
  production-ready but is not actually steering runtime behavior

## Split-authority live mechanisms

These are not dead stacks.
They are active side channels that weaken the idea that `DatabaseBuilder` plus
`Subsystem` is the one composition model.

### 1. Subsystem recovery/freeze is separate from merge and DAG composition

The declared engine subsystem model is:

- `Subsystem::recover`
- `Subsystem::freeze`
- builder-installed ordered subsystem list

Code anchor:

- `crates/engine/src/recovery/subsystem.rs`

But graph/vector merge behavior is installed through global once-only callback
registration at process startup, not through the subsystem list:

- `strata_graph::register_graph_semantic_merge()`
- `strata_vector::register_vector_semantic_merge()`
- `strata_graph::register_branch_dag_hook_implementation()`

Code anchors:

- `crates/executor/src/api/mod.rs:79`
- `crates/graph/src/merge_handler.rs:45`
- `crates/vector/src/merge_handler.rs:60`
- `crates/graph/src/dag_hook_impl.rs:51`

Consequence:

- recovery/freeze composition and merge/DAG composition are different runtime
  channels
- a database can be "opened correctly" yet still have different branch-merge
  semantics depending on process-global registration state

### 2. Follower refresh uses opportunistic extension hooks, not declared subsystems

Follower refresh is mediated by `RefreshHook` and `RefreshHooks`, stored as
lazy database extensions.

Code anchors:

- `crates/engine/src/database/refresh.rs:26`
- `crates/engine/src/database/refresh.rs:59`
- `crates/engine/src/database/refresh.rs:73`
- `crates/engine/src/database/mod.rs:891`
- `crates/engine/src/database/lifecycle.rs:129`
- `crates/engine/src/database/lifecycle.rs:156`

Vector recovery installs a refresh hook through this side channel:

- `crates/vector/src/recovery.rs:466`
- `crates/vector/src/recovery.rs:501`

Meanwhile search refresh is handled directly in `Database::refresh()` via the
search index extension, not through the same hook interface.

Code anchor:

- `crates/engine/src/database/lifecycle.rs:151`

Consequence:

- subsystem participation in follower refresh is not owned by the builder
  subsystem list
- refresh semantics are partly declarative and partly hardcoded

### 3. `GraphSubsystem` exists, but standard product open does not use it

`crates/graph/src/branch_dag.rs` defines `GraphSubsystem`, and its recovery
does exactly the sort of work a canonical subsystem should own:

- `init_system_branch(db)`
- `load_status_cache_readonly(db)`

Code anchors:

- `crates/graph/src/branch_dag.rs:87`
- `crates/graph/src/branch_dag.rs:106`
- `crates/graph/src/branch_dag.rs:848`
- `crates/graph/src/branch_dag.rs:850`

But the standard product builder in `crates/executor/src/api/mod.rs` only
installs:

- `VectorSubsystem`
- `SearchSubsystem`

Code anchor:

- `crates/executor/src/api/mod.rs:101`

Workspace caller search found no live non-test `with_subsystem(GraphSubsystem)`
use.

Consequence:

- branch DAG/status recovery exists in a canonical-looking subsystem form, but
  production relies instead on executor-side `init_system_branch()` plus global
  DAG hook registration

### 4. Background control is fragmented across scheduler and dedicated threads

The canonical engine background plane is `BackgroundScheduler`.

Code anchors:

- `crates/engine/src/background.rs:110`
- `crates/engine/src/database/open.rs:516`

But important lifecycle work still runs outside that scheduler:

- WAL flush thread from `spawn_wal_flush_thread`
- executor embed refresh thread
- manual `run_maintenance()` path with no background maintenance loop

Code anchors:

- `crates/engine/src/database/open.rs:595`
- `crates/executor/src/executor.rs:120`
- `crates/engine/src/database/lifecycle.rs:86`

Consequence:

- there is no single background execution model
- scheduler reasoning does not automatically cover all long-running control
  loops

## False positives

### `graph::snapshot`

`crates/graph/src/snapshot.rs` looks snapshot-related, but it is graph-store
algorithm/test support, not a competing persistence runtime.

### `SearchSubsystem`

`crates/engine/src/search/recovery.rs` is on the live path.
It is not a shadow stack.

### `VectorSubsystem`

`crates/vector/src/recovery.rs` is live whenever the builder installs it.
The zombie vector pieces are the separate WAL and snapshot modules, not the
recovery subsystem.

### `BackgroundScheduler`

The scheduler is real and live.
The issue is fragmentation around it, not dormancy of the scheduler itself.

## Cleanup implications

### Delete candidates

- `crates/vector/src/wal.rs`
- `crates/vector/src/snapshot.rs`
- `crates/storage/src/compaction.rs` size-tier scheduler surface, unless it is
  going to be promoted back into live policy

### Promote-or-replace candidates

- `durability::database`
- `durability::recovery`
- `GraphSubsystem`

These should not remain in the current ambiguous state.
Each needs an explicit decision:

- promote into the canonical runtime
- absorb their useful parts into the live path
- or delete them

### Governance fixes

- make `Subsystem` the only sanctioned per-database composition plane, or make
  the side channels first-class and declared
- eliminate global registration as a hidden prerequisite for branch semantics
- converge on one filesystem layout and one recovery stack
- converge on one background-work model

## Bottom line

The shadow-stack sweep confirmed that Strata's runtime drift is not limited to
small leftovers.

There are at least five real alternate or dormant implementations that look
production-grade:

- durability database runtime
- durability recovery runtime
- vector WAL
- vector snapshot
- size-tier compaction scheduler

And there is a second class of problem that is arguably more dangerous:

- live behavior is spread across builder subsystems, lazy extensions, global
  registrations, and dedicated background loops

That means the codebase does not just contain zombie migrations.
It also contains multiple composition planes that can each become
runtime-authoritative for different features.
