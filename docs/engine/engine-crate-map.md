# Engine Crate Map

## Purpose

This document is a baseline map of `crates/engine` as it exists today.

It is not a target design. It is a description of current ownership and
behavior, written to support the next serious engine/storage boundary cleanup.

For the broader engine cleanup ledger, see
[engine-pending-items.md](./engine-pending-items.md).

For the cross-boundary audit with storage, see
[../storage/storage-engine-ownership-audit.md](../storage/storage-engine-ownership-audit.md).

The important takeaway is that `strata-engine` is not just a database-kernel
crate. It is currently:

- the database open/runtime/bootstrap layer
- the branch-domain and primitive-semantics owner
- the subsystem composition host
- the search/runtime behavior host
- the branch-bundle import/export host
- and, in a smaller set of remaining places, the adapter layer that turns raw
  storage facts into engine policy and public database behavior

## High-Level Shape

Top-level source files:

- [crates/engine/src/lib.rs](../../crates/engine/src/lib.rs)
- [crates/engine/src/error.rs](../../crates/engine/src/error.rs)
- [crates/engine/src/branch_domain.rs](../../crates/engine/src/branch_domain.rs)
- [crates/engine/src/limits.rs](../../crates/engine/src/limits.rs)
- [crates/engine/src/background.rs](../../crates/engine/src/background.rs)
- [crates/engine/src/bundle/mod.rs](../../crates/engine/src/bundle/mod.rs)
- [crates/engine/src/coordinator.rs](../../crates/engine/src/coordinator.rs)
- [crates/engine/src/transaction_ops.rs](../../crates/engine/src/transaction_ops.rs)

Major subtrees:

- `database/` — database open/close/runtime orchestration, recovery, config,
  follower handling, retention reporting, and compatibility seams
- `primitives/` — KV/JSON/event/space behavior and primitive extension traits
- `semantics/` — branch/limits/event/json/vector/value surfaces used as the
  authoritative engine-side import boundary
- `search/` — search subsystem runtime, manifest, indexing, tokenization, and
  search-facing behavior
- `transaction/` — engine transaction wrappers and pooling
- `branch_ops/` — branch-domain workflow, merge, and branch-control helpers
- `recovery/` — subsystem recovery/freeze API

The crate is large. The heaviest ownership points today are:

- [database/mod.rs](../../crates/engine/src/database/mod.rs)
- [primitives/json/mod.rs](../../crates/engine/src/primitives/json/mod.rs)
- [database/recovery.rs](../../crates/engine/src/database/recovery.rs)
- [database/compaction.rs](../../crates/engine/src/database/compaction.rs)
- [database/snapshot_install.rs](../../crates/engine/src/database/snapshot_install.rs)
- [bundle/mod.rs](../../crates/engine/src/bundle/mod.rs)
- [search/mod.rs](../../crates/engine/src/search/mod.rs)
- [branch_ops/mod.rs](../../crates/engine/src/branch_ops/mod.rs)

## Public Surface

The public re-exports in [lib.rs](../../crates/engine/src/lib.rs) define the
crate's current effective surface.

Today `strata-engine` re-exports:

- database/runtime types:
  - `Database`
  - `StrataConfig`
  - `StorageConfig`
  - `HealthReport`
  - `RetentionReport`
  - `RecoveryError`
  - metrics and subsystem-health types
- branch-domain types:
  - `BranchRef`
  - `BranchControlRecord`
  - `BranchGeneration`
  - `ForkAnchor`
  - `BranchEventOffsets`
  - branch DAG/status types
- primitive semantics:
  - `JsonPath`
  - `JsonPatch`
  - `JsonValue`
  - JSON helper functions
  - `VectorConfig`
  - `DistanceMetric`
  - `Event`
  - `ChainVerification`
  - `extractable_text`
  - `Limits`
  - `LimitError`
- transaction/runtime types:
  - `Transaction`
  - `ScopedTransaction`
  - `TransactionPool`
  - `TransactionContext`
  - `DurabilityMode`
- search/runtime types:
  - `SearchRequest`
  - `SearchResponse`
  - `SearchHit`
  - `SearchSubsystem`
- bundle/export-import types:
  - `BundleInfo`
  - `ExportInfo`
  - `ImportInfo`

This means engine is currently both:

1. the semantic/domain owner
2. the database runtime owner
3. the search/runtime owner
4. the host of storage-boundary adapters and public database policy

Those are related, but they are not identical roles.

## Current Dependency Shape

### Direct Outgoing Workspace Dependencies

`strata-engine` currently depends on:

- `strata-core`
- `strata-storage`
- `strata-security`

At runtime and in active test/development paths, that is now the clean semantic
stack we wanted. The old `core-legacy` compat crate is no longer in engine’s
dependency graph at all.

### Incoming Workspace Dependents

The internal incoming graph today is:

- `strata-executor`
- `strata-executor-legacy`
- `strata-graph`
- `strata-search`
- `strata-vector`
- `strata-intelligence`

And then, above those:

- `strata-cli`
- `stratadb`

This confirms that engine is already the main semantic/runtime hub of the
workspace.

## What Engine Owns Today

### 1. Database Runtime And Lifecycle

In [database/](../../crates/engine/src/database), the crate owns:

- open/close behavior
- primary vs follower runtime modes
- lifecycle coordination
- health reporting
- refresh and blocked-state handling
- retention reporting
- runtime configuration

This is legitimate engine territory.

### 2. Branch Domain

In [branch_domain.rs](../../crates/engine/src/branch_domain.rs) and
[branch_ops/](../../crates/engine/src/branch_ops), the crate owns:

- branch lifecycle/control
- branch DAG semantics
- merge/cherry-pick/revert workflow
- branch-level business rules

This is clearly engine-owned.

### 3. Primitive Semantics

In [semantics/](../../crates/engine/src/semantics) and
[primitives/](../../crates/engine/src/primitives), the crate owns:

- JSON path and patch behavior
- event semantics
- vector config and metric semantics
- search-facing value extraction
- per-primitive transaction surfaces

This is also clearly engine-owned.

### 4. Product-Facing Runtime Workflows

In [bundle/mod.rs](../../crates/engine/src/bundle/mod.rs) and
[search/](../../crates/engine/src/search), the crate owns:

- branch export/import workflows
- search subsystem runtime
- search manifest/runtime logic
- search-facing orchestration

These are engine-level workflows built on top of lower persistence/runtime
machinery.

## Storage-Boundary Adapters Engine Still Owns

The ES2-ES4 cleanup moved the generic checkpoint, decoded-row snapshot
install, and recovery bootstrap mechanics into `strata-storage`. Engine still
contains adapter modules at those boundaries because the public APIs,
primitive decode, lifecycle policy, and public error taxonomy remain
engine-owned.

The remaining storage-boundary modules are:

- [database/compaction.rs](../../crates/engine/src/database/compaction.rs)
  - public `Database::checkpoint()` / `Database::compact()` orchestration
  - conversion of storage checkpoint and retention facts into engine reports
  - lifecycle-aware shutdown and retention policy around storage mechanics
- [database/snapshot_install.rs](../../crates/engine/src/database/snapshot_install.rs)
  - primitive snapshot section decode
  - primitive-shaped install telemetry
  - construction of a generic decoded-row install plan for storage
- [database/recovery.rs](../../crates/engine/src/database/recovery.rs)
  - `Database::run_recovery()` orchestration
  - engine snapshot-install callback around primitive decode
  - mapping `StorageRecoveryError` into `RecoveryError`
  - degraded-storage policy and lossy recovery reporting
  - `TransactionCoordinator`, follower-state, and watermark bootstrap
- parts of [database/open.rs](../../crates/engine/src/database/open.rs)
  - public open/lifecycle sequencing
  - WAL-writer wiring

The lower storage mechanics behind these adapters now live in storage-owned
modules such as `durability/checkpoint_runtime.rs`,
`durability/decoded_snapshot_install.rs`, and
`durability/recovery_bootstrap.rs`. ES5 is expected to narrow the remaining
storage-configuration and WAL-writer runtime seams.

## Current Architectural Role

If you describe the crate honestly as it exists today, `strata-engine` is:

- the database runtime and lifecycle hub
- the branch and primitive semantics owner
- the search/runtime host
- the branch bundle workflow host
- the layer that converts raw storage recovery/checkpoint facts into public
  database behavior

## Main Takeaway

The next cleanup should not treat current engine boundaries as already clean.

The important ownership question is not whether engine is too large. It is
whether code in the crate is:

- semantic/domain behavior that should stay in `engine`, or
- generic persistence/runtime machinery that should move to `storage`

After ES2-ES4, the semantic side of engine is real and the most critical
durability mechanics have sunk into storage. The remaining cleanup is to keep
the adapter code explicit while ES5 finishes the storage-configuration split.
