# ST3 Generic Transaction Runtime Absorption Plan

## Purpose

`ST3` is the first lower-runtime absorption epic after `ST2`.

Its purpose is to move the now-primitive-agnostic transaction runtime out of
`strata-concurrency` and into `strata-storage` without dragging WAL and replay
coupling into `storage` too early.

This document is grounded in the current codebase after `ST2`, not the older
pre-extraction shape. It names the exact source modules that still exist in
`concurrency`, the exact call sites that still depend on them, and the exact
pieces that must wait for `ST4`.

Read this together with:

- [storage-minimal-surface-implementation-plan.md](./storage-minimal-surface-implementation-plan.md)
- [storage-charter.md](./storage-charter.md)
- [storage-crate-map.md](./storage-crate-map.md)
- [concurrency-crate-map.md](./concurrency-crate-map.md)
- [durability-crate-map.md](./durability-crate-map.md)
- [storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md)
- [st2-primitive-transaction-semantics-extraction-plan.md](./st2-primitive-transaction-semantics-extraction-plan.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)

## ST3 Verdict

After `ST2`, `strata-concurrency` no longer owns JSON or event semantics.

What remains there is mostly the generic transaction runtime:

- `TransactionContext`
- generic OCC validation
- lock-ordering and commit-safety rules
- version / txn-id allocation
- visible-version tracking
- branch commit locks
- deletion and quiesce guards

That is exactly the runtime that should belong to `storage`.

What does **not** cleanly belong to `ST3` is the durability-coupled tail:

- WAL record envelope serialization
- WAL writer integration
- recovery coordination
- snapshot / manifest / codec handling

Those still depend directly on `strata-durability` and should move in `ST4`,
not by forcing a premature `storage -> durability` edge during `ST3`.

## Why `ST3` Is Not “Move The Whole Crate”

`crates/concurrency/src` is no longer one coherent unit.

Two groups remain:

1. Generic transaction runtime that should move now.
2. Durability-coupled coordinator and replay code that should wait for `ST4`.

If `ST3` tries to move `payload.rs`, `recovery.rs`, and the WAL-aware parts of
`manager.rs` wholesale, then `storage` will either:

- gain a direct dependency on `strata-durability` before the durability merge,
  or
- grow temporary adapter seams that are more confusing than the current split.

So the right `ST3` move is:

- absorb the generic runtime into `storage`
- reduce `concurrency` to a thin durability shell
- let `ST4` absorb that shell together with the durability runtime

## Current Code Map

### Move In `ST3`

These files are now generic and should move into `storage`.

#### `crates/concurrency/src/transaction.rs`

This file owns the generic transaction state:

- `CommitError`
- `ApplyResult`
- `PendingOperations`
- `TransactionStatus`
- `CASOperation`
- `TransactionContext`

It also owns the core transaction API used throughout engine, graph, and
vector:

- `new()`
- `with_store()`
- `reset()`
- `get()`
- `put()`
- `put_with_ttl()`
- `delete()`
- `scan_prefix()`
- `cas()`
- `branch_id()`
- `txn_id()`
- status / lifecycle helpers

There is no remaining JSON/event ownership here after `ST2`.

#### `crates/concurrency/src/validation.rs`

This file now owns only generic OCC validation:

- `ConflictType::ReadWriteConflict`
- `ConflictType::CASConflict`
- `ValidationResult`
- `validate_read_set()`
- `validate_cas_set()`
- `validate_transaction()`

This is a storage-runtime concern and should move verbatim.

#### `crates/concurrency/src/lock_ordering.rs`

This file documents the lock hierarchy for the generic commit runtime:

- quiesce lock
- per-branch commit mutex
- deletion barrier
- WAL mutex
- dashmap shard guards

Even though part of that hierarchy still mentions WAL, the generic lock-order
contract belongs with the transaction runtime, not in a standalone crate.

### Split During `ST3`

#### `crates/concurrency/src/manager.rs`

This file mixes two kinds of ownership.

##### Generic manager core that should move in `ST3`

State fields:

- `version`
- `next_txn_id`
- `commit_locks`
- `commit_quiesce`
- `visible_version`
- `pending_versions`
- `deleting_branches`

Generic methods:

- `new()`
- `with_txn_id()`
- `current_version()`
- `visible_version()`
- `mark_version_applied()`
- `bump_version_floor()`
- `quiesced_version()`
- `quiesce_commits()`
- `next_txn_id()`
- `allocate_version()`
- `remove_branch_lock()`
- `mark_branch_deleting()`
- `is_branch_deleting()`
- `unmark_branch_deleting()`
- `branch_commit_lock()`
- `catch_up_version()`
- `set_visible_version()`
- `catch_up_txn_id()`

These are generic storage-runtime responsibilities.

##### Durability-coupled manager pieces that should wait for `ST4`

- `WalMode`
- `writer_halted_commit_error()`
- thread-local WAL serialization buffers
- apply-failure injection hooks
- `commit_inner()`
- `commit()`
- `commit_with_wal_arc()`
- `commit_with_version()`

These pieces directly depend on:

- `strata_durability::__internal::WalWriterEngineExt`
- `strata_durability::wal::WalWriter`
- `crate::payload::serialize_wal_record_into()`

They should not be moved into `storage` until the durability merge is ready.

### Wait For `ST4`

#### `crates/concurrency/src/payload.rs`

Even though `TransactionPayload::from_transaction()` only depends on
`TransactionContext`, the file as a whole is still WAL-format plumbing:

- `TransactionPayload`
- `serialize_wal_record_into()`
- `PayloadError`

In particular, `serialize_wal_record_into()` writes the
`strata_durability::format::WalRecord` envelope directly. That is `ST4`
material, not `ST3`.

#### `crates/concurrency/src/recovery.rs`

This file is firmly durability-coupled:

- `RecoveryCoordinator`
- `RecoveryPlan`
- `CoordinatorRecoveryError`
- `RecoveryResult`
- `RecoveryStats`
- `manifest_error_to_strata_error()`
- `apply_wal_record_to_memory_storage()`

It depends directly on:

- `DatabaseLayout`
- `ManifestManager`
- `SnapshotReader`
- `WalReader`
- `WalRecord`
- snapshot / manifest / codec errors

This is exactly the runtime that should move together with `durability` in
`ST4`.

## Active Callers That `ST3` Must Repoint

### Direct `TransactionContext` Users

Engine internals still import `TransactionContext` from `strata-concurrency`
in many places, including:

- `crates/engine/src/lib.rs`
- `crates/engine/src/transaction/{context,owned,pool}.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/primitives/{kv,event,json,branch,space}.rs`
- `crates/engine/src/branch_ops/{mod.rs,branch_control_store.rs}`

Neighboring crates also depend on the same type directly:

- `crates/graph/src/{ext.rs,edges.rs}`
- `crates/vector/src/ext.rs`

These should move to a storage-owned path during `ST3A` / `ST3B`.

### Direct `TransactionManager` Users

The main coordinator/recovery consumers still live in engine:

- `crates/engine/src/coordinator.rs`
- `crates/engine/src/database/recovery.rs`

These should stop depending on a standalone concurrency crate once the generic
manager core is storage-owned.

### Durability-Coupled Callers That Must Wait

These are still explicitly tied to `payload.rs` / `recovery.rs` and should be
left for `ST4`:

- `crates/engine/src/database/lifecycle.rs`
  - `TransactionPayload::from_bytes`
- `crates/engine/src/database/recovery.rs`
  - `RecoveryCoordinator`
  - `RecoveryResult`
  - `RecoveryStats`
  - `CoordinatorRecoveryError`
- `crates/engine/tests/recovery_parity.rs`

## Destination Layout In `storage`

`ST3` should create a dedicated transaction-runtime subtree in `storage`:

- `crates/storage/src/txn/mod.rs`
- `crates/storage/src/txn/context.rs`
- `crates/storage/src/txn/validation.rs`
- `crates/storage/src/txn/lock_ordering.rs`
- `crates/storage/src/txn/manager.rs`

Recommended ownership:

- `context.rs`
  - `TransactionContext`
  - `CommitError`
  - `TransactionStatus`
  - `CASOperation`
  - `ApplyResult`
  - `PendingOperations`
- `validation.rs`
  - `ConflictType`
  - `ValidationResult`
  - `validate_*`
- `lock_ordering.rs`
  - lock-order contract docs
- `manager.rs`
  - storage-owned manager core only

Public surface recommendation:

- internal canonical path: `strata_storage::txn::*`
- crate-root re-exports for active runtime types:
  - `TransactionContext`
  - `TransactionStatus`
  - `CommitError`
  - `TransactionManager` once the storage-owned manager core exists

That gives `engine`, `graph`, and `vector` one stable lower-runtime import
path while keeping the actual storage runtime organized internally.

## Execution Order

`ST3` should be executed as three slices.

## ST3A — Move Transaction State And Validation

### Goal

Make `storage` the physical owner of the generic transaction state and
validation surface.

### Changes

1. Create `storage::txn` modules.
2. Move from `concurrency` into `storage`:
   - `transaction.rs`
   - `validation.rs`
   - `lock_ordering.rs`
3. Re-export those moved types from `strata-concurrency` as a temporary
   compatibility shell.
4. Repoint direct users to `strata-storage`:
   - `engine::lib` root re-export
   - engine transaction wrappers and pools
   - graph/vector extension traits
   - engine primitive extension traits that type on `TransactionContext`
5. Move the generic runtime tests with the ownership:
   - `tests/concurrency/occ_invariants.rs`
   - `tests/concurrency/cas_operations.rs`
   - `tests/concurrency/transaction_states.rs`
   - `tests/concurrency/transaction_lifecycle.rs`
   - `tests/concurrency/snapshot_isolation.rs`

### Definition Of Done

- `TransactionContext` is physically defined in `storage`
- generic validation is physically defined in `storage`
- active code imports those surfaces from `storage`, not `concurrency`
- `concurrency` only forwards them

## ST3B — Extract The Storage-Owned Manager Core

### Goal

Move generic commit coordination state into `storage` without introducing a
premature `storage -> durability` dependency.

### Changes

1. Split `manager.rs` into:
   - storage-owned generic manager core
   - concurrency-owned WAL adapter shell
2. Move the manager state and helper methods listed above into
   `storage::txn::manager`.
3. Extract the WAL-independent portion of commit coordination into a
   storage-owned core method.

The required refactor is to separate:

- validation under branch lock
- version allocation and pending-version bookkeeping
- storage apply
- visible-version advancement
- transaction status transitions

from:

- WAL record serialization
- WAL writer locking / append
- writer-halted error mapping

### Required Mechanism

Do **not** make `storage` call `WalWriter` directly in `ST3`.

Instead, the storage-owned manager core should expose a hook/callback boundary
for “durable append before apply”, and the remaining `concurrency` shell
should implement that boundary using the current WAL path.

That keeps the dependency direction honest:

- `storage` owns the generic commit protocol
- `concurrency` temporarily supplies the durability adapter
- `ST4` later absorbs that adapter with the durability runtime

### Definition Of Done

- generic manager state is physically defined in `storage`
- `concurrency::TransactionManager` is reduced to a durability-aware wrapper
  or forwarder around the storage-owned core
- no new `storage -> durability` edge exists yet

## ST3C — Concurrency Contraction

### Goal

Reduce `concurrency` to only the durability-coupled pieces that really must
wait for `ST4`.

### Changes

Keep in `concurrency` only:

- the WAL-aware commit adapter
- `payload.rs`
- `recovery.rs`
- fault-injection hooks for the adapter path

Everything else should already be storage-owned by this point.

Update the crate docs and crate map so `concurrency` is described honestly as
a temporary durability shell, not as the owner of the generic transaction
runtime.

### Definition Of Done

- `concurrency` no longer physically owns transaction state or generic OCC
  validation
- `concurrency` no longer physically owns lock/version/quiesce/deletion state
- `concurrency` contains only the durability-coupled residue that `ST4` will
  absorb

## Constraints

### 1. No Primitive Regression

`ST2` already moved JSON/event semantics upward. `ST3` must not recreate them
below `engine` through new helpers or compatibility shortcuts.

### 2. No Premature `storage -> durability` Edge

If a piece still imports `WalWriter`, `WalRecord`, `ManifestManager`,
`SnapshotReader`, or other durability runtime types directly, it is not ready
to move in `ST3`.

### 3. One Runtime Type Identity

Once `TransactionContext`, `CommitError`, and `TransactionStatus` move into
`storage`, do not keep parallel definitions in `concurrency`. Use forwarders
or re-exports only.

### 4. No Silent Commit-Semantics Redesign

This epic is an ownership correction. Do not change:

- first-committer-wins OCC
- per-branch commit serialization
- visible-version behavior
- deletion/quiesce safety

unless a correctness bug forces it and the change is called out explicitly.

## Verification

At minimum, each slice should keep these green:

### For `ST3A`

- `cargo test -p strata-storage --quiet`
- `cargo test -p strata-concurrency --quiet`
- `cargo test -p strata-engine --quiet`
- `cargo test -p strata-graph --quiet`
- `cargo test -p strata-vector --quiet`

### For `ST3B`

- all of the above, plus:
- `cargo test -p strata-engine coordinator --quiet`
- `cargo test -p stratadb --test engine --quiet`
- `cargo test -p stratadb --test integration --quiet`

### For `ST3C`

- all of the above, plus:
- `cargo check --workspace --all-targets`
- grep audit that active code imports generic runtime surfaces from
  `strata-storage`, not `strata-concurrency`

## ST3 Definition Of Done

`ST3` is complete only when all of the following are true:

1. `TransactionContext` is physically defined in `strata-storage`.
2. Generic OCC validation is physically defined in `strata-storage`.
3. The generic manager core is physically defined in `strata-storage`.
4. Active engine/graph/vector/runtime callers use storage-owned transaction
   runtime surfaces.
5. `strata-concurrency` no longer owns generic transaction state or generic
   OCC validation.
6. `strata-concurrency` contains only the durability-coupled residue that is
   explicitly queued for `ST4`.
