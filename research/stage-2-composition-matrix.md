# Stage 2: Composition Matrix

This pass compares the behavior of Strata's real open paths rather than
treating them as cosmetic wrappers around one canonical runtime.

The result is that Strata does not have one open contract.
It has several, and the differences are material:

- different subsystem sets
- different branch/bootstrap behavior
- different background lifecycle
- different backend type
- different same-path singleton behavior

## Scope

Compared entrypoints:

- `Strata::open`
- `Strata::open_with`
- `Database::open`
- `DatabaseBuilder::open`
- `Database::open_follower`
- `DatabaseBuilder::open_follower`
- `Strata::cache`
- `Database::cache`
- IPC fallback from `Strata::open_with`

Primary code anchors:

- `crates/executor/src/api/mod.rs`
- `crates/executor/src/executor.rs`
- `crates/engine/src/database/builder.rs`
- `crates/engine/src/database/open.rs`
- `crates/engine/src/search/recovery.rs`
- `crates/engine/src/database/registry.rs`

## Baseline facts

### `Strata::open` is not its own path

`Strata::open(path)` is only:

- `Strata::open_with(path, OpenOptions::default())`

So all meaningful composition differences live in `Strata::open_with`.

Code anchor:

- `crates/executor/src/api/mod.rs:149`

### `DatabaseBuilder` is the only explicit subsystem composition API

The builder is the only place where the caller can declare the recovery/freeze
subsystem list directly.

Code anchor:

- `crates/engine/src/database/builder.rs`

### Raw `Database::open*` hardcodes only `SearchSubsystem`

The raw engine convenience APIs delegate to subsystem-aware open, but with a
hardcoded subsystem list containing only `SearchSubsystem`.

Code anchors:

- `crates/engine/src/database/open.rs:287`
- `crates/engine/src/database/open.rs:299`
- `crates/engine/src/database/open.rs:537`
- `crates/engine/src/database/open.rs:545`

## Composition matrix

| Path | Backend shape | Subsystems at open | Lock / registry | Recovery / persistence shape | Post-open extras | Main drift |
|---|---|---|---|---|---|---|
| `Strata::open` | Delegates to `Strata::open_with(default)` | `VectorSubsystem` + `SearchSubsystem` on local path | Primary local path uses lock + `OPEN_DATABASES`; IPC path avoids both locally | Local path is builder-backed primary open; IPC path defers to another process | Merge/DAG registration, possible system-branch init, recipe seeding, executor wrapper, embed refresh thread, default branch bootstrap | Not a single runtime; just policy dispatch |
| `Strata::open_with` primary local | Local `Database` wrapped in `Executor` | `VectorSubsystem` + `SearchSubsystem` | Exclusive `.lock`; same-path singleton via `OPEN_DATABASES` | Primary recovery, WAL writer, flush thread, scheduler | `init_system_branch`, built-in recipe seeding, `ensure_default_branch` or `verify_default_branch`, executor embed refresh thread | Product path is stronger than raw engine open |
| `Strata::open_with` follower | Local follower `Database` wrapped in `Executor` | `VectorSubsystem` + `SearchSubsystem` | No lock, no registry | Read-only follower recovery, `wal_writer: None`, later `refresh()` | Merge/DAG registration, executor wrapper, access mode forced read-only; skips default-branch verification | Follower is a distinct runtime, not just read-only primary |
| IPC fallback from `Strata::open_with` | `Backend::Ipc` client, not local `Database` | None on client side | No local lock/registry ownership | Delegated to already-running server process | Only does client connect + `Ping`; no local branch/bootstrap work | Backend type changes based on environment |
| `Database::open` | Raw local `Database` | Hardcoded `SearchSubsystem` only | Exclusive `.lock`; same-path singleton via `OPEN_DATABASES` | Primary recovery, WAL writer, flush thread, scheduler | None beyond engine open | No vector recovery, no merge/DAG registration, no branch/bootstrap policy |
| `DatabaseBuilder::open` | Raw local `Database` | Caller-declared subsystem list | Exclusive `.lock`; same-path singleton via `OPEN_DATABASES` | Primary recovery, WAL writer, flush thread, scheduler | None beyond builder-driven subsystem recovery | Canonical engine path, but only if it is first opener |
| `Database::open_follower` | Raw follower `Database` | Hardcoded `SearchSubsystem` only | No lock, no registry | Follower recovery only, later `refresh()` | None beyond engine follower open | Raw follower misses vector recovery |
| `DatabaseBuilder::open_follower` | Raw follower `Database` | Caller-declared subsystem list | No lock, no registry | Follower recovery only, later `refresh()` | None beyond builder-driven subsystem recovery | Canonical follower path, but separate from primary semantics |
| `Database::cache` | Raw ephemeral `Database` | No declared subsystems; search index enabled directly | No lock, no registry | No WAL, no files, no recovery | Search index enabled directly in engine | Cache open is not compositionally equivalent to builder/product cache |
| `Strata::cache` | `DatabaseBuilder::cache()` wrapped in `Executor` | `VectorSubsystem` + `SearchSubsystem` | No lock, no registry | Ephemeral `Database::cache()` plus builder subsystem recovery | Merge/DAG registration, system-branch init, default branch bootstrap, executor embed refresh thread | Product cache is a different product from raw engine cache |

## Per-path analysis

### 1. `Strata::open_with` is a policy router

`Strata::open_with` first registers process-global merge handlers and branch
DAG hooks, then chooses one of three outcomes:

1. local primary open
2. local follower open
3. IPC client fallback

Code anchors:

- `crates/executor/src/api/mod.rs:79`
- `crates/executor/src/api/mod.rs:164`

This means the public product API is not just "open a database".
It is a backend-selection policy layer.

### 2. Product primary open is stronger than raw engine open

The normal successful local `Strata::open_with` path does all of the following:

- uses `strata_db_builder()` with `VectorSubsystem` then `SearchSubsystem`
- runs `strata_graph::branch_dag::init_system_branch(&db)`
- seeds built-in recipes
- wraps in `Executor::new_with_mode`
- ensures or verifies the default branch

Code anchors:

- `crates/executor/src/api/mod.rs:100`
- `crates/executor/src/api/mod.rs:217`
- `crates/executor/src/api/mod.rs:219`
- `crates/executor/src/api/mod.rs:221`
- `crates/executor/src/api/mod.rs:226`
- `crates/executor/src/executor.rs:77`

By contrast, `Database::open`:

- hardcodes `SearchSubsystem`
- does not initialize the system branch
- does not seed recipes
- does not ensure a default branch
- does not install executor-level background behavior

Code anchors:

- `crates/engine/src/database/open.rs:294`
- `crates/engine/src/database/open.rs:299`

So raw engine open and product open are materially different products.

### 3. Read-only primary is not follower mode

`Strata::open_with(... access_mode(ReadOnly))` still goes through the local
primary path when `opts.follower` is false.

That means it still:

- creates the directory if needed
- writes/reads `strata.toml`
- acquires the exclusive primary lock
- opens WAL writer / flush thread as needed
- uses the primary recovery path

The read-only behavior is enforced by `Executor` access-mode checks, not by the
engine opening a follower database.

Code anchors:

- `crates/executor/src/api/mod.rs:192`
- `crates/executor/src/api/mod.rs:217`
- `crates/executor/src/executor.rs:77`
- `crates/executor/src/executor.rs:181`

This is an important semantic distinction:

- primary read-only is a wrapper policy
- follower is a separate engine runtime

### 4. Follower open is a separate runtime family

Follower open:

- does not acquire a file lock
- does not use `OPEN_DATABASES`
- has `wal_writer: None`
- is marked `follower: true`
- relies on explicit `refresh()` for new WAL records
- skips freeze-on-drop semantics

Code anchors:

- `crates/engine/src/database/open.rs:438`
- `crates/engine/src/database/open.rs:497`
- `crates/engine/src/database/open.rs:518`
- `crates/engine/src/database/open.rs:560`

`Strata::open_with(follower = true)` adds another layer of drift on top:

- forces `AccessMode::ReadOnly`
- deliberately skips default-branch verification
- still registers global merge handlers before opening

Code anchors:

- `crates/executor/src/api/mod.rs:169`
- `crates/executor/src/api/mod.rs:175`
- `crates/executor/src/api/mod.rs:177`

### 5. Same-path singleton behavior only applies to primaries

Primary opens go through `OPEN_DATABASES` and return an existing `Arc<Database>`
if the path is already open.

Code anchors:

- `crates/engine/src/database/registry.rs:30`
- `crates/engine/src/database/open.rs:243`
- `crates/engine/src/database/open.rs:246`

This means subsystem composition is path-global for primaries.
If a raw `Database::open()` happens first, later `DatabaseBuilder::open()` or
`Strata::open()` calls for that same path get the existing instance unchanged.

Code anchor:

- `crates/engine/src/database/open.rs:324`

Followers do not use the registry, so multiple follower instances can exist for
the same path at once.

### 6. Builder open is canonical only if it wins the race

`DatabaseBuilder::open()` and `DatabaseBuilder::open_follower()` are the only
clean subsystem-declaration APIs.

Code anchors:

- `crates/engine/src/database/builder.rs:58`
- `crates/engine/src/database/builder.rs:87`

But for primaries, builder composition is not authoritative if some other
primary path opened the same filesystem path earlier in the process.

That is the core Stage 2 result:

- composition is caller-dependent
- then path-global
- then sticky for the rest of the process lifetime

### 7. Cache opens are not one thing either

Raw `Database::cache()`:

- creates an ephemeral `Database`
- does not install any subsystems
- directly enables the search index extension
- does not initialize the system branch
- does not ensure a default branch

Code anchors:

- `crates/engine/src/database/open.rs:918`
- `crates/engine/src/database/open.rs:979`
- `crates/engine/src/database/open.rs:981`

`Strata::cache()`:

- registers global merge/DAG handlers
- uses `strata_db_builder().cache()` with `VectorSubsystem` and `SearchSubsystem`
- initializes the system branch
- ensures the default branch exists
- wraps in `Executor`, which starts the embed refresh thread

Code anchors:

- `crates/executor/src/api/mod.rs:335`
- `crates/executor/src/api/mod.rs:341`
- `crates/executor/src/api/mod.rs:345`
- `crates/executor/src/executor.rs:99`

So "cache database" means different things depending on whether the caller came
through the raw engine or the product wrapper.

### 8. IPC fallback is not compositionally equivalent to local open

On lock failure, `Strata::open_with` may connect to `strata.sock` and return an
IPC client backend after a `Ping`.

Code anchors:

- `crates/executor/src/api/mod.rs:237`
- `crates/executor/src/api/mod.rs:245`

This path:

- does not locally recover the database
- does not locally run subsystem recovery
- does not locally initialize the system branch
- does not locally seed recipes
- does not locally ensure or verify the default branch

So the public product API can return either:

- a local runtime whose composition this call actively shaped
- or a client attached to a runtime shaped earlier by another process

That is a major composition boundary.

### 9. The stale-socket retry path is compositionally inconsistent

If IPC connect fails and `strata.sock` is removed, `Strata::open_with` retries a
local builder open.

Code anchor:

- `crates/executor/src/api/mod.rs:264`

But this retry-success branch does not repeat the normal local post-open steps:

- it creates `Executor`
- it ensures or verifies the default branch
- but it does not call `init_system_branch(&db)`
- and it does not seed built-in recipes

Code anchors:

- `crates/executor/src/api/mod.rs:277`
- `crates/executor/src/api/mod.rs:281`

This is a concrete composition bug inside the public open path itself.

### 10. Follower open is stricter on path existence than primary open

Primary opens create the data directory and config if needed.
Follower opens canonicalize the path first and require the directory to already
exist.

Code anchors:

- `crates/engine/src/database/open.rs:102`
- `crates/engine/src/database/open.rs:394`
- `crates/engine/src/database/open.rs:420`

That means:

- `Strata::open_with(follower = false)` can create a new database
- `Strata::open_with(follower = true)` cannot

### 11. Executor-owned lifecycle exists only on product/wrapper paths

Local `Strata` paths wrap the database in `Executor`, which adds:

- access-mode enforcement
- embed refresh thread
- typed command surface

Code anchors:

- `crates/executor/src/executor.rs:77`
- `crates/executor/src/executor.rs:99`
- `crates/executor/src/executor.rs:181`

Raw `Database::*` paths do not get this layer.

So even when the underlying `Database` instance is the same, the runtime
surface above it is different.

## First-opener-wins mismatch list

These mismatches are possible within one process because primary opens share
the path registry:

### 1. Raw engine open can suppress vector recovery for later product opens

Sequence:

1. `Database::open(path)` opens first with only `SearchSubsystem`
2. later `Strata::open(path)` runs
3. `OPEN_DATABASES` returns the existing instance
4. `VectorSubsystem::recover` never runs

### 2. Builder-opened custom subsystem sets can be silently ignored

Sequence:

1. caller A opens path with subsystem set A
2. caller B opens same path with subsystem set B
3. caller B gets the existing instance with subsystem set A's already-recovered state

### 3. Product bootstrap policy is also first-opener-sensitive

If a raw engine open happens first, later product code inherits a database that
may lack:

- system-branch initialization
- built-in recipe seeding
- default branch bootstrap

Those are not intrinsic engine-open guarantees.

## Bottom line

The composition matrix confirms that Strata's runtime contract is not unified.

The system currently has:

- one raw engine family
- one builder-based engine family
- one product wrapper family
- one follower family
- one cache family
- one IPC client family

They overlap, but they are not semantically identical.

The most important conclusions are:

1. `DatabaseBuilder` is the clean subsystem API, but it is not authoritative
   because primary path registry makes first opener win
2. `Strata::open_with` is a backend/policy router, not a single open path
3. raw engine open, product open, follower open, and cache open are different
   products, not just convenience layers
4. there is at least one confirmed composition bug inside the product open path
   itself: the stale-socket retry branch skips system-branch init and recipe
   seeding
