# Workstream A: Ownership Matrix

This is the final synthesis for the runtime-spine pass.
It ties open, commit, refresh, freeze, and shutdown into one code-derived
ownership model.

The goal is not to pretend Strata has one all-owning runtime module.
It does not.
The goal is to identify which layer is authoritative for each lifecycle
transition, which layers only supply inputs, and where the handoff seams live.

## One authoritative model

Strata's runtime is layered.
The authoritative model is:

- `executor` owns product-open policy
- `DatabaseBuilder` owns subsystem list declaration
- `engine::database` owns runtime object construction, lifecycle orchestration,
  follower refresh, freeze orchestration, and shutdown/drop
- `engine::coordinator` owns transaction bookkeeping, timeouts, and the handoff
  into commit protocol
- `concurrency::TransactionManager` owns validation, version allocation, WAL
  commit ordering, storage apply, and visible-version advancement
- `durability` owns WAL format, WAL append/sync behavior, and MANIFEST access
- `storage` owns persisted state installation and segment recovery
- subsystem crates own their own recovery and freeze implementations

That is the real runtime spine.

## Ownership matrix

| Transition | Public entry shape | Authoritative owner | Collaborators | What the owner decides | Important seam |
|---|---|---|---|---|---|
| Open | `Strata::open*`, `DatabaseBuilder::open*`, `Database::open*` | `executor` for product policy, then `engine::database::open` for runtime construction | `concurrency::RecoveryCoordinator`, `storage`, `durability`, registered subsystems | which open path is used, whether the DB is primary/follower/cache, how the runtime object is built, when subsystem recovery runs | raw `Database::open*` and product `Strata::open*` are not semantically identical |
| Commit | `Database::transaction*`, manual `begin/commit/end`, `Session` txn flow | `engine::database::transaction` for admission and handoff, then `engine::coordinator` + `concurrency` for commit protocol | `durability::WalWriter`, `storage` | whether commit is allowed, whether WAL is passed, timeout enforcement, bookkeeping, validation/version/WAL/storage ordering | `Session` orchestrates manual transactions above engine closure API, but commit still routes through engine/coordinator |
| Refresh | `Database::refresh()` on followers | `engine::database::lifecycle` | `durability::WalReader`, `concurrency::TransactionPayload`, `storage`, search index, refresh hooks | follower-only replay loop, payload decode, storage apply, secondary-index catch-up, watermark advancement | refresh is a separate lifecycle from open and is best-effort rather than strictly convergent |
| Freeze | `shutdown()`, `Drop`, cache-drop, implicit drop | `engine::database` for orchestration | registered subsystems | when freeze runs and in what order | engine owns the freeze chain, but subsystems own actual persisted derived state |
| Shutdown | `Database::shutdown()` | `engine::database::lifecycle` | `engine::coordinator`, scheduler, flush thread, WAL flush, freeze hooks | admission stop, wait-for-idle, flush-thread stop, final flush, freeze orchestration | `shutdown()` is not a full close; `Drop` still performs registry cleanup and fallback flush/freeze |
| Drop | `Drop for Database`, `Executor::drop` | split: `Executor` drops embed timer; `Database` does final runtime cleanup | scheduler, flush thread, subsystem freeze hooks, registry | last-resort cleanup when explicit shutdown was skipped or incomplete | product teardown is not fully engine-owned end to end |

## Handoff table

### 1. Open

Primary product open starts in `executor`, not `engine`.

`Strata::open_with()` owns:

- merge-handler registration
- branch-DAG hook registration
- standard builder composition via `strata_db_builder()`
- `init_system_branch(&db)`
- built-in recipe seeding
- IPC fallback policy

Code anchors:

- `crates/executor/src/api/mod.rs:164`
- `crates/executor/src/api/mod.rs:217`

Once the builder calls into `Database::open_with_config_and_subsystems()`,
authority moves to `engine::database::open`.

`engine` then owns:

- lock acquisition / registry lookup
- WAL recovery kickoff
- segment recovery
- `Database` object construction
- space-metadata repair
- subsystem recovery invocation
- subsystem installation for freeze-on-drop

Code anchors:

- `crates/engine/src/database/builder.rs:59`
- `crates/engine/src/database/open.rs:318`
- `crates/engine/src/database/open.rs:690`

Within that engine-owned open:

- `concurrency::RecoveryCoordinator` owns WAL replay planning and base recovered state
- `storage` owns segment installation and version bump
- `durability` owns MANIFEST and WAL writer construction

So open is authoritative in two stages:

1. product policy in `executor`
2. runtime construction in `engine`

### 2. Commit

Commit starts in `engine::database::transaction`, even when the caller uses
manual transactions or `executor::Session`.

`Database::begin_transaction()` owns:

- admission check
- snapshot choice
- visible-version wait
- transaction start bookkeeping
- pool acquisition

Code anchor:

- `crates/engine/src/database/transaction.rs:657`

`Database::commit_transaction()` and closure-based `transaction()` own:

- write-backpressure admission
- choosing whether WAL participates
- scheduling flush after successful writes
- routing to `commit_internal()`

Code anchors:

- `crates/engine/src/database/transaction.rs:527`
- `crates/engine/src/database/transaction.rs:760`
- `crates/engine/src/database/transaction.rs:791`

`TransactionCoordinator` then owns:

- timeout enforcement
- commit/abort bookkeeping
- active transaction accounting
- handoff into `TransactionManager`

Code anchors:

- `crates/engine/src/coordinator.rs:220`
- `crates/engine/src/coordinator.rs:257`
- `crates/engine/src/coordinator.rs:271`

`concurrency::TransactionManager` is the authoritative commit-protocol owner.
It owns:

- validation
- version allocation
- per-branch commit locking
- WAL append ordering
- storage apply
- visible-version advancement

This is where the `DurableButNotVisible` seam lives if WAL append succeeds and
storage apply fails afterward.

Code anchors:

- `crates/concurrency/src/manager.rs:568`
- `crates/concurrency/src/manager.rs:705`

`durability` is authoritative only for WAL append/sync semantics, not for the
overall commit decision.

### 3. Refresh

Refresh is entirely engine-owned and only meaningful for followers.

`Database::refresh()` owns:

- follower-only gating
- reading WAL after current watermark
- decoding payloads
- storage replay via `apply_recovery_atomic`
- secondary-index and hook catch-up
- local txn/version counter catch-up
- watermark advancement

Code anchor:

- `crates/engine/src/database/lifecycle.rs:129`

Its collaborators are:

- `durability::WalReader` for bytes on disk
- `concurrency::TransactionPayload` for writeset decode
- `storage` for replay application
- search and refresh hooks for derived state

Refresh is therefore not owned by `concurrency` recovery once the follower is
already open.
It is a separate engine lifecycle.

### 4. Freeze

Freeze orchestration is engine-owned.

The builder-installed subsystem list is the sole freeze target list.
`run_freeze_hooks()` iterates the installed subsystems in reverse order and
attempts all freezes even if one fails.

Code anchors:

- `crates/engine/src/database/builder.rs:3`
- `crates/engine/src/database/mod.rs:444`

This means:

- subsystem ordering is declared by the opener
- freeze orchestration is owned by engine
- actual freeze implementation is owned by each subsystem

Followers do not participate in freeze-on-drop.

Code anchor:

- `crates/engine/src/database/open.rs:552`

### 5. Shutdown

`Database::shutdown()` is the authoritative graceful-stop method, but it is not
the entire close story.

It owns:

- stopping admission
- draining the scheduler
- waiting for active transactions to reach zero
- stopping and joining the WAL flush thread
- final flush
- freeze-hook orchestration
- setting `shutdown_complete`

Code anchor:

- `crates/engine/src/database/lifecycle.rs:401`

Its main collaborator is `TransactionCoordinator::wait_for_idle()`.

Code anchor:

- `crates/engine/src/coordinator.rs:550`

The critical nuance is that `shutdown()` stops at a "graceful stop" boundary.
It does not:

- remove the instance from `OPEN_DATABASES`
- drop the lock file
- own the full product-level teardown

### 6. Drop

`Drop for Database` is the authoritative last-resort cleanup path.

It owns:

- compaction cancellation
- scheduler shutdown
- flush-thread shutdown/join
- fallback flush/freeze if `shutdown()` did not complete
- registry removal

Code anchor:

- `crates/engine/src/database/mod.rs:1106`

That means `Drop` is not just defensive cleanup.
It is still part of the real runtime lifecycle.

At the product layer, `Executor` also owns its own embed refresh thread and
joins it in `drop`, so full product teardown is split between executor and
engine.

## Authoritative owner by question

If the question is "who should be changed if this lifecycle step is wrong?",
the answer is:

- "Open chose the wrong runtime shape" -> `executor` and `DatabaseBuilder`
- "Open recovered or constructed runtime state incorrectly" -> `engine::database::open`
- "Commit admitted or sequenced work incorrectly" -> `engine::database::transaction` and `engine::coordinator`
- "Validation/version/WAL/storage ordering is wrong" -> `concurrency::TransactionManager`
- "WAL bytes or fsync semantics are wrong" -> `durability`
- "Follower replay/refresh is wrong" -> `engine::database::lifecycle`
- "Freeze ordering is wrong" -> `engine`
- "What gets frozen or rebuilt is wrong" -> the owning subsystem
- "Shutdown leaves runtime state behind" -> `engine::database::lifecycle` plus `Drop for Database`

## Final runtime-spine conclusion

There is no single module that authoritatively owns all of open, commit,
refresh, freeze, and shutdown.

The correct authoritative model is a layered one:

1. `executor` chooses the product runtime shape
2. `engine` constructs and governs the database runtime
3. `coordinator` bridges engine lifecycle into transaction semantics
4. `concurrency` owns commit protocol correctness
5. `durability` owns WAL bytes and sync behavior
6. `storage` owns installed persisted state
7. subsystems own their own derived-state recovery and freeze logic

The main architectural risk is not that these owners are impossible to identify.
It is that the boundaries between them are too easy to bypass or split, which is
why the earlier Workstream A findings cluster around composition drift,
shutdown/drop split-brain, follower divergence, and sanctioned raw-`Database`
wrappers.
