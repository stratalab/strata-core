# Workstream A: Runtime Spine

This document captures the initial runtime-spine audit.
The final ownership synthesis now lives in
`research/workstream-a-ownership-matrix.md`.
Its purpose is to establish the active entrypoints, the current ownership map,
and the first set of confirmed architectural seams.

## Scope

- `crates/executor`
- `crates/engine`
- `crates/concurrency`
- `crates/storage`
- `crates/durability`

## Initial runtime map

### Primary product open

Default product open goes through:

1. `Strata::open()` -> `Strata::open_with()`
2. `ensure_merge_handlers_registered()`
3. `strata_db_builder()`
4. `DatabaseBuilder::open_with_config()`
5. `Database::open_with_config_and_subsystems()`
6. `Database::open_internal_with_subsystems()`
7. `Database::acquire_primary_db()`
8. `Database::open_finish()`
9. `strata_concurrency::RecoveryCoordinator::recover()`
10. `Database::recover_segments_and_bump()`
11. subsystem recovery in registration order
12. `db.set_subsystems(...)`
13. executor initialization and default-branch checks

Code anchors:

- `crates/executor/src/api/mod.rs`
- `crates/engine/src/database/builder.rs`
- `crates/engine/src/database/open.rs`

### Raw engine open

Raw engine open is not the same path semantically.
`Database::open()` and `Database::open_with_config()` route through
`Database::open_internal()`, which hardcodes only `SearchSubsystem`.

That means:

- vector recovery is not intrinsic to the raw engine open path
- production composition is not owned by `engine`
- executor open is the stronger path

Code anchors:

- `crates/engine/src/database/open.rs`
- `crates/engine/src/recovery/mod.rs`

### Follower open

Follower open is a separate runtime shape:

1. canonicalize path and read config
2. `Database::acquire_follower_db()`
3. read-only WAL recovery via `RecoveryCoordinator`
4. segment recovery
5. build `Database` with `wal_writer: None`, `_lock_file: None`, `follower: true`
6. run follower subsystem recovery
7. later use `Database::refresh()` to tail new WAL records

Follower semantics are not just "primary open with writes disabled".
It has different lock, recovery, refresh, and shutdown behavior.

Code anchors:

- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/lifecycle.rs`

### Cache open

Cache open is another distinct runtime:

- no files
- no WAL
- no registry entry
- no lock file
- subsystem recovery still runs through the builder path

This means cache semantics are intentionally different from disk-backed open,
even though the same high-level APIs can reach both.

Code anchors:

- `crates/executor/src/api/mod.rs`
- `crates/engine/src/database/builder.rs`
- `crates/engine/src/database/open.rs`

### IPC fallback

`Strata::open_with()` can also fall back to IPC if direct local open fails
because another process holds the database lock and `strata.sock` exists.

That means the public product open path is conditional:

- local engine/runtime path if lock acquisition succeeds
- IPC client path if another process already owns the runtime

So `Strata::open()` is not one runtime path.
It is a policy layer that selects between two backends.

Code anchor:

- `crates/executor/src/api/mod.rs`

## Initial ownership map

### Executor

Owns:

- production subsystem composition through `strata_db_builder()`
- merge-handler registration
- branch DAG hook registration
- IPC fallback policy
- high-level access-mode and default-branch checks

### Engine

Owns:

- database object lifecycle
- primary and follower open orchestration
- lock-file acquisition for primaries
- scheduler, flush thread, shutdown, and drop behavior
- subsystem hook invocation

### Concurrency

Owns:

- WAL replay coordinator used by the live engine open path
- transaction coordination, versions, and active-transaction tracking

### Storage

Owns:

- segmented store runtime
- segment recovery
- branch-visible state installation

### Durability

Owns on the live path:

- WAL writer and reader format
- MANIFEST access
- durability mode implementation

Also contains a second database/runtime abstraction that is not the active
engine open path.

Code anchor:

- `crates/durability/src/database/handle.rs`

## Confirmed seams

### 1. Production composition lives above the engine

The engine declares the subsystem model, but executor decides the standard
production subsystem set.
That means core runtime semantics depend on which opener the caller uses.

Implication:

- the engine is not the sole source of truth for what a "normal" database is

### 2. Raw engine open and product open are different products

`Database::open*` and `Strata::open*` are not just different ergonomic layers.
They can recover different subsystem sets and therefore produce different
runtime state from the same on-disk database.

Implication:

- composition drift is a first-class architectural issue, not an edge case

### 3. First opener wins for same-path opens

`OPEN_DATABASES` singletonizes a database path inside the process.
If an earlier caller opens a path with one subsystem list, later callers get
that existing instance unchanged.

Implication:

- composition is path-global and order-sensitive inside a process

Code anchors:

- `crates/engine/src/database/registry.rs`
- `crates/engine/src/database/open.rs`

### 4. Follower is a separate runtime architecture

Follower mode has:

- no lock file
- no WAL writer
- its own recovery path shape
- explicit `refresh()` replay
- early-return shutdown behavior

Implication:

- follower correctness cannot be inferred from primary correctness

Additional seam confirmed later:

- follower refresh is best-effort pull replication, not strict convergence

### 5. Public open is backend-selecting, not just database-opening

`Strata::open_with()` may yield:

- a local engine-backed runtime
- a follower runtime
- an IPC client talking to another runtime

Implication:

- public API semantics depend on environment state, not just options

### 6. A second durability runtime still exists

`durability::database::handle` exposes `open`, `open_or_create`, and `recover`
for a cleaner-looking persistence stack, but the live engine open path does not
route through it.

Implication:

- recovery/persistence ownership is still split across two architectures

### 7. Lower-layer direct callers are not raw runtime rewrites, but the
public boundary is still porous

The reverse-audit did not find live product callers outside `engine` directly
constructing `TransactionManager`, `SegmentedStore`, `WalWriter`, or
`RecoveryCoordinator`.

The real direct-caller problem is different:

- `Strata::from_database` wraps arbitrary `Database` instances and skips part
  of the canonical product-open contract
- `Executor::new*` and `Session::new*` accept arbitrary `Arc<Database>`
- graph and vector intentionally extend `TransactionContext` directly
- vector recovery and merge logic read `db.storage()` directly as subsystem
  internals

Implications:

- the engine is not being widely bypassed by raw lower-layer reconstruction
- the product/runtime contract is still bypassable through sanctioned
  raw-`Database` wrappers
- lower-layer types are part of the real primitive extension surface

Code anchor:

- `research/workstream-a-lower-layer-direct-callers.md`

### 8. Branch DAG and branch status are overlay state, not core runtime state

The branch DAG is implemented as a graph-side, name-keyed overlay on the
`_system_` branch.
Executor selectively depends on it for merge-base override, but the engine
treats DAG hooks and DAG writes as optional best-effort behavior.

`GraphSubsystem` exists but is not part of the standard builder path, so DAG
initialization and status-cache loading are not intrinsic runtime behavior.

Implications:

- branch lineage correctness is partially executor-owned
- branch status is descriptive, not authoritative
- branch-name reuse can contaminate DAG ancestry across generations

## Initial audit targets inside Workstream A

### A1. Commit-path ownership

Trace:

- `engine::database::transaction`
- `engine::coordinator`
- `concurrency::manager`
- WAL append path
- storage apply path

Questions:

- where version visibility is advanced
- where commit durability is actually decided
- which invariants are engine-owned vs concurrency-owned

### A2. Shutdown and drop exact sequencing

Trace:

- `engine::database::lifecycle`
- `engine::database::mod::Drop`
- `engine::background`
- executor drop-owned background work

Status:

- completed in `research/workstream-a-shutdown-drop-freeze.md`

- explicit `shutdown()`
- `Drop for Database`
- follower short-circuit
- freeze-hook behavior
- flush-thread termination

Questions:

- whether explicit shutdown and drop are semantically identical
- what can still fail after `shutdown_complete`
- what state is only best-effort on drop

### A3. Shadow durability boundary

Trace:

- `durability::database::handle`
- `durability::recovery`
- all callers outside the durability crate

Questions:

- which parts are genuinely unused
- which parts still influence live code through shared formats
- whether this stack is promotable or should be deleted

### A4. Follower refresh correctness

Trace:

- follower open
- `Database::refresh()`
- WAL reader watermark behavior
- search inline refresh
- vector refresh hooks

Status:

- completed in `research/workstream-a-follower-refresh.md`

Questions:

- whether refresh is truly convergent
- whether refresh policy matches open-time recovery policy
- which subsystem behaviors are opener-dependent

### A5. Composition matrix

Compare:

- `Strata::open`
- `Database::open`
- `DatabaseBuilder::open`
- follower opens
- cache opens
- IPC fallback

Questions:

- what extensions, subsystems, hooks, and checks each path installs
- which path should be considered authoritative

## Immediate next reads

- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/coordinator.rs`
- `crates/concurrency/src/manager.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/durability/src/database/handle.rs`
- `crates/durability/src/recovery`

## Working hypothesis

The runtime spine already shows the main architectural pattern we need to test
across the rest of the codebase:

- there is usually one active path
- there is often a cleaner or newer-looking parallel path beside it
- the public surface can reach materially different runtime shapes depending on
  opener, environment, and process history

If that pattern keeps holding, the audit should prioritize eliminating
composition-sensitive semantics and shadow runtimes before adding new features.

## Follow-up deep dive

The commit-path deep dive for this workstream is in:

- `research/workstream-a-commit-path.md`

The branch-operation locking deep dive for this workstream is in:

- `research/workstream-a-branch-ops-locking.md`
