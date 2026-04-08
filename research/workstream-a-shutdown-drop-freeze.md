# Workstream A: Shutdown, Drop, and Freeze Sequencing

This note audits how Strata closes a runtime.

The core question is not whether there is a shutdown path.
There is.
The real question is whether:

- explicit shutdown
- implicit drop
- subsystem freeze
- background task termination

all describe the same lifecycle boundary.

They do not.

## Scope

- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/mod.rs`
- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/background.rs`
- `crates/engine/src/recovery/mod.rs`
- `crates/engine/src/recovery/subsystem.rs`
- `crates/executor/src/api/mod.rs`
- `crates/executor/src/executor.rs`
- `crates/executor/src/handlers/embed_hook.rs`

## The actual close paths

There are two materially different close paths in the live runtime.

### 1. Explicit engine shutdown

`Database::shutdown()` does this:

1. `accepting_transactions = false`
2. if follower, return early
3. `scheduler.drain()`
4. `coordinator.wait_for_idle(timeout)`
5. signal WAL flush thread shutdown
6. join WAL flush thread
7. `flush()` WAL
8. `run_freeze_hooks()`
9. set `shutdown_complete = true`

Source:

- `crates/engine/src/database/lifecycle.rs`

### 2. Implicit production close

The product layer does not expose a `Strata::shutdown()`.
Normal product shutdown is effectively:

1. `Drop for Executor`
2. stop embed refresh timer thread
3. best-effort `flush_embed_buffer()`
4. eventually `Drop for Database` when the final `Arc<Database>` is released

`Drop for Database` then does this:

1. set `compaction_cancelled = true`
2. `scheduler.shutdown()`
3. stop WAL flush thread
4. if `shutdown_complete == false` and not follower:
5. `flush()`
6. `run_freeze_hooks()`
7. remove path from `OPEN_DATABASES`

Sources:

- `crates/executor/src/executor.rs`
- `crates/engine/src/database/mod.rs`

## Confirmed architecture

### 1. Shutdown and drop are not equivalent lifecycle operations

`shutdown()`:

- drains the scheduler, but does not shut it down
- does not cancel compaction
- does not remove the database from the global registry
- does not release the lock file immediately

`Drop`:

- does cancel compaction
- does shut down the scheduler
- does remove the instance from `OPEN_DATABASES`
- does release the lock file by dropping the `Database`

Conclusion:

- explicit shutdown is only a partial close
- final runtime teardown still happens only at drop

### 2. The documented symmetry promise is weaker than the real lifecycle

The subsystem model promises:

- recover in registration order
- freeze in reverse order

That part is true.

But the subsystem freeze chain is only one part of close.
The stronger runtime guarantees depend on machinery outside the subsystem
model:

- background scheduler
- WAL flush thread
- compaction cancellation
- global registry removal
- executor-owned embed timer thread

Conclusion:

- subsystem symmetry is real but incomplete
- full runtime shutdown is not subsystem-owned

### 3. `shutdown()` drains too early

This is the sharpest sequencing problem in the engine shutdown path.

`shutdown()` drains the scheduler before waiting for in-flight transactions
to finish.
But transactions that were already in flight are still allowed to complete,
and the commit path schedules background work after commit via
`schedule_flush_if_needed()`.

That means this sequence is possible:

1. `shutdown()` sets `accepting_transactions = false`
2. `shutdown()` drains the scheduler
3. an in-flight transaction commits
4. that commit schedules background flush/compaction
5. `shutdown()` continues to WAL flush and subsystem freeze
6. background tasks continue running after shutdown returns

The scheduler is still live because `shutdown()` never calls
`scheduler.shutdown()`.

Sources:

- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/transaction.rs`

Conclusion:

- explicit shutdown does not establish a true "no more background work" boundary

### 4. `shutdown()` leaves a dead instance in the path registry

Primary opens are singletonized by `OPEN_DATABASES`.
`acquire_primary_db()` returns an existing instance unchanged if the weak
reference upgrades.

`Database::shutdown()` does not remove the instance from that registry.
Only `Drop for Database` removes it.

So if a caller:

1. opens a primary database
2. calls `db.shutdown()`
3. keeps an `Arc<Database>` alive
4. opens the same path again

the open path can return the existing shut-down instance.

That instance has:

- `accepting_transactions = false`
- possibly no flush thread
- same lock file still held

Conclusion:

- shutdown is not a terminal reopening boundary
- the path registry models liveness by `Arc` lifetime, not by open/closed state

### 5. Shutdown does not release the lock file

The exclusive `.lock` file is owned by the `Database` object.
It is released only when the file handle is dropped.

Because `shutdown()` does not destroy the `Database`, the process keeps the
lock after shutdown.

Conclusion:

- a shut-down primary still blocks other processes until final drop

### 6. Freeze errors are terminal even when shutdown returns an error

`run_freeze_hooks()` attempts all hooks and returns the first error.
`shutdown()` stores `shutdown_complete = true` before propagating that error.

That means:

- caller receives an error from `shutdown()`
- but `Drop for Database` will skip flush/freeze retry because
  `shutdown_complete` is already true

This may be intentional to avoid double-freeze, but it means shutdown failure
is not recoverable by just letting the object drop naturally.

Conclusion:

- freeze failure is sticky
- drop will not retry a failed shutdown freeze

### 7. Followers take a third lifecycle shape

`shutdown()` on followers:

1. sets `accepting_transactions = false`
2. returns immediately

Followers do not:

- drain scheduler
- join a flush thread
- run freeze hooks
- set `shutdown_complete = true`

`Drop for Database` still shuts down the scheduler and joins the flush thread
slot if present, but skips flush/freeze because `follower == true`.

Conclusion:

- follower shutdown is not just primary shutdown without writes
- it is a separate lifecycle contract

### 8. Production close is executor-first, engine-second

`Executor` owns a background embed refresh timer thread.
`Drop for Executor`:

- wakes and joins that thread
- then calls `flush_embed_buffer()`

`Database::shutdown()` does not know about that executor-owned thread.
So explicit engine shutdown and normal product close are not aligned.

If the engine is shut down while an executor still exists:

- the embed timer thread can still exist until executor drop
- executor drop still attempts a best-effort embedding flush afterward
- those write attempts can fail silently after engine shutdown

Sources:

- `crates/executor/src/executor.rs`
- `crates/executor/src/handlers/embed_hook.rs`

Conclusion:

- some runtime-owned persistence work lives above the engine
- explicit engine shutdown is not the whole product shutdown boundary

## Mode-specific summary

### Primary

- explicit `shutdown()` is partial
- final teardown happens only at `Drop`
- registry and lock survive until final drop

### Follower

- `shutdown()` is mostly a write-gate flip
- no freeze path
- lifecycle is intentionally asymmetric

### Cache

- `shutdown()` still runs freeze hooks
- persistence methods no-op because there is no disk path
- drop still shuts down scheduler and other runtime threads

### IPC-backed `Strata`

IPC fallback is not the same local lifecycle at all.
The client handle does not own the engine runtime.
Server shutdown is separate.

## Main risks

### Risk 1: Background work can outlive explicit shutdown

Because `shutdown()` drains before waiting for transactions and never shuts
the scheduler down, late commit-triggered tasks can race past the shutdown
boundary.

### Risk 2: Reopen-after-shutdown can hand back a dead instance

Because the registry is keyed to `Arc` liveness rather than open state,
shutdown without final drop can poison future opens on the same path.

### Risk 3: Product close is mostly drop-based, not shutdown-based

The real product runtime usually closes through:

- `Executor::drop`
- then `Database::drop`

not through explicit `Database::shutdown()`.

That means the audited explicit shutdown path is not the whole production
story.

### Risk 4: Freeze failure is non-retriable by drop

Once `shutdown()` reaches the freeze stage, any reported failure still marks
the instance as shutdown-complete and suppresses drop-time retry.

## Architectural judgment

Strata currently has three different notions of "closed":

- not accepting new transactions
- explicitly shut down
- fully dropped and deregistered

Those should probably collapse to one authoritative lifecycle boundary.
Right now they do not.

## Recommended follow-up

1. Make `shutdown()` perform full terminal teardown, or rename it to reflect
   that it is only a pre-drop quiesce.
2. Move scheduler shutdown and registry removal into explicit shutdown, not
   only drop.
3. Add a second scheduler drain after in-flight transactions finish, or
   otherwise prevent late commit-triggered tasks after shutdown begins.
4. Decide whether failed freeze should permit a drop-time retry.
5. Unify engine shutdown with executor-owned background work, especially the
   embed refresh path.
