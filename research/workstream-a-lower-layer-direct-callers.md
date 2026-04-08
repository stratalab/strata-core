# Workstream A: Lower-Layer Direct Callers

This note reverse-audits the workspace for live callers that reach into
`concurrency`, `storage`, or `durability` in ways that bypass the intended
engine-owned runtime path.

The question here is narrow:

- are there production callers outside `engine` that directly construct or own
  raw WAL, recovery, transaction-manager, or segmented-store machinery
- or are the real bypasses higher-level APIs that accept a pre-opened
  `Database` and therefore skip the standard product composition contract

## Scope

- `crates/executor`
- `crates/vector`
- `crates/graph`
- `crates/search`
- `crates/cli`
- `crates/intelligence`
- `crates/inference`
- `crates/security`

Tests and benches were excluded where possible.
Inline test modules inside production files were manually ignored.

## Bottom line

The reverse-audit did not find a second live production path that directly
constructs `TransactionManager`, `SegmentedStore`, `WalWriter`, or
`RecoveryCoordinator` outside the engine-owned open path.

The main bypasses are different:

- public constructors that wrap an arbitrary `Arc<Database>`
- executor/session code that uses the engine's manual transaction API directly
- primitive crates that intentionally implement behavior on
  `TransactionContext`
- subsystem code that reads `db.storage()` directly for recovery or merge work

So the architecture problem is not "rogue crates are rebuilding the engine."
It is that the abstraction boundary above the engine is porous enough that
callers can sidestep the standard product-open contract.

## Confirmed non-finding

Across the higher-level crates in scope, there were no live non-test callers of:

- `TransactionManager::new`
- `SegmentedStore::new`
- `SegmentedStore::with_dir`
- `WalWriter::new`
- `WalReader::new`
- `concurrency::RecoveryCoordinator::new`

The one serious exception remains the dormant durability runtime in
`crates/durability/src/database/handle.rs`, which still exposes its own
database-level create/open/recover abstraction and directly owns
`ManifestManager`, `WalWriter`, and `CheckpointCoordinator`.

That stack is a shadow architecture, but it is not a live higher-level caller
in the current product path.

## Confirmed live bypass surfaces

### 1. `Strata::from_database` is a public composition bypass

`Strata::open_with()` is the canonical product open.
It goes through the standard builder, then performs product-level side effects:

- `strata_db_builder().open_with_config(...)`
- `strata_graph::branch_dag::init_system_branch(&db)`
- `strata_engine::recipe_store::seed_builtin_recipes(&db)`
- executor creation and default-branch checks

Code anchor:

- `crates/executor/src/api/mod.rs:164`
- `crates/executor/src/api/mod.rs:217`

By contrast, `Strata::from_database_with_mode()` does only:

- `ensure_merge_handlers_registered()`
- `Executor::new_with_mode(db, access_mode)`
- default-branch create/verify

It does not enforce that the supplied `Database` came from the standard builder
path, and it does not run `init_system_branch()` or recipe seeding.

Code anchor:

- `crates/executor/src/api/mod.rs:400`
- `crates/executor/src/api/mod.rs:410`

This is the cleanest confirmed example of a sanctioned runtime bypass.

## 2. `Executor::new` and `Executor::new_with_mode` accept arbitrary `Database`

`Executor` is documented as the single entry point to the engine, but its
constructors accept any `Arc<Database>` without checking how that database was
opened or composed.

Code anchor:

- `crates/executor/src/executor.rs:70`

This does not create a second commit implementation.
It does mean callers can build product behavior on top of a raw engine handle
that may be missing the standard subsystem set and product-open initialization.

`Session::new*` and IPC server startup both inherit this property.

## 3. `Session::new*` uses the engine manual transaction API directly

`Session` accepts an arbitrary `Arc<Database>` and manages its own
`TransactionContext`.

Code anchors:

- `crates/executor/src/session.rs:73`
- `crates/executor/src/session.rs:84`
- `crates/executor/src/ipc/server.rs:192`

The transaction lifecycle path is:

- `db.begin_transaction(...)`
- hold `TransactionContext` inside `Session`
- `db.commit_transaction(...)`
- `db.end_transaction(...)`

Code anchor:

- `crates/executor/src/session.rs:270`

This is not a lower-layer durability bypass, because commit still routes
through the engine-owned commit path.
But it is a real public runtime path that bypasses the higher-level
closure-style transaction API and keeps transaction orchestration in
`executor`.

## 4. Graph and vector intentionally extend `TransactionContext`

Both graph and vector implement their transactional behavior directly on
`strata_concurrency::TransactionContext`.

Code anchors:

- `crates/graph/src/ext.rs:1`
- `crates/vector/src/ext.rs:1`

This is an intentional pattern, not an accidental misuse.
It allows:

- standalone stores to wrap operations in `db.transaction(...)`
- executor `Session` to reuse the same transaction-aware primitive logic

The important architectural consequence is different:

- primitive transaction semantics are not wholly owned by `engine`
- `concurrency::TransactionContext` is part of the real primitive extension
  surface
- vector especially carries post-commit side effects outside OCC, via
  `StagedVectorOp`

Code anchor:

- `crates/vector/src/ext.rs:28`

So this is not a rogue caller problem.
It is a layering decision that makes lower-layer types part of the active
primitive contract.

## 5. Vector subsystem code reads `db.storage()` directly

The vector crate directly scans storage during recovery and merge-related work.

Code anchors:

- `crates/vector/src/recovery.rs:37`
- `crates/vector/src/recovery.rs:108`
- `crates/vector/src/merge_handler.rs:75`

This is also intentional.
The vector subsystem needs branch-wide scans and collection rebuild logic that
do not map cleanly to the high-level primitive facade.

The important distinction is:

- these are subsystem internals
- they are not alternate commit or recovery owners
- they still widen the number of engine-internal APIs that must remain stable

## Classification

### Real production-relevant bypasses

- `Strata::from_database`
  - bypass type: product composition bypass
  - risk: wraps a `Database` without enforcing standard builder/open side effects

- `Executor::new` / `Executor::new_with_mode`
  - bypass type: executor-over-raw-database entrypoint
  - risk: lets higher layers attach product behavior to a thinner runtime

- `Session::new` / `Session::new_with_mode`
  - bypass type: manual transaction orchestration over arbitrary `Database`
  - risk: keeps transaction lifecycle semantics in `executor`, not only `engine`

### Intentional lower-layer integrations, not primary findings

- `GraphStoreExt for TransactionContext`
- `VectorStoreExt for TransactionContext`
- vector recovery and merge-handler direct `db.storage()` scans

These are important because they define the real layering boundary.
They are not evidence of a hidden second runtime spine.

### Dormant shadow architecture

- `durability::database::DatabaseHandle`

This remains the strongest example of a production-grade alternate stack that
the engine still does not use.

## What this means for Workstream A

The runtime-spine hypothesis tightens:

1. The workspace does not contain many live rogue callers that instantiate raw
   lower layers outside the engine.
2. The more important issue is that the product API surface allows callers to
   enter above the engine with a prebuilt `Database`.
3. Primitive crates intentionally depend on lower-layer types as extension
   points, so the engine boundary is semantically thinner than the public API
   suggests.

That means the next remediation questions are:

- should `from_database`, `Executor::new`, and `Session::new` remain public
- if they remain public, what runtime preconditions must they validate
- should primitive transaction extensions stay on `TransactionContext`, or be
  hidden behind engine-owned traits

## Recommended follow-up

- treat raw-`Database` wrappers as composition-sensitive APIs and document them
  as such
- consider a runtime fingerprint or capability check before wrapping a
  `Database` in product APIs
- keep the dormant durability runtime on the shadow-stack shortlist
- treat direct `TransactionContext` extensions as part of the active
  architecture, not as implementation detail
