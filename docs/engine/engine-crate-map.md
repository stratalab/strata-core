# Engine Crate Map

## Purpose

This document maps `crates/engine` after the storage-boundary normalization.

It describes the settled engine responsibilities at the storage boundary and
the remaining higher-layer consolidation work that will happen above that
boundary.

For the broader engine cleanup ledger, see
[engine-pending-items.md](./archive/engine-pending-items.md).

For the planned consolidation of graph, vector, search, and legacy bootstrap
into engine, see
[engine-consolidation-plan.md](./engine-consolidation-plan.md).

For the cross-boundary audit with storage, see
[../storage/storage-engine-ownership-audit.md](../storage/storage-engine-ownership-audit.md).

The important takeaway is that `strata-engine` is the database semantics and
orchestration layer. It is currently:

- the database open/runtime/bootstrap layer
- the branch-domain and primitive-semantics owner
- the subsystem composition host
- the search/runtime behavior host
- the branch-bundle import/export host
- the adapter layer that turns raw storage facts into engine policy and public
  database behavior

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
  follower handling, retention reporting, and storage-boundary adapters
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
  - `AccessMode`
  - `OpenOptions`
  - `StrataConfig`
  - `StorageConfig`
  - `SensitiveString`
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

At runtime and in active test/development paths, the lower dependency stack is
now:

```text
strata-core -> strata-storage -> strata-engine
```

`EG2` absorbed the old security/open-options surface into engine, so
`AccessMode`, `OpenOptions`, and `SensitiveString` are now engine-owned types.

The old `core-legacy` compat crate is no longer in engine's dependency graph at
all.

### Incoming Workspace Dependents

The direct normal incoming graph today is:

- `strata-executor`
- `strata-search`
- `strata-intelligence`

And then, above those:

- `strata-cli`
- `stratadb`

`strata-intelligence` also depends on `strata-inference`, which is currently
outside the engine stack and supplies model/inference support.

This confirms that engine is already the main semantic/runtime hub of the
workspace, but it is not yet the only runtime owner above storage.

### Current Normal Workspace Graph

The verified normal dependency graph for engine-adjacent crates is below.
`EG5H` re-verified this graph on 2026-05-06 after deleting the
`strata-vector` crate and closing out vector absorption. The phase tracking
plans are
[eg1-implementation-plan.md](./eg1-implementation-plan.md),
[eg3-implementation-plan.md](./eg3-implementation-plan.md), and
[eg5-implementation-plan.md](./eg5-implementation-plan.md).

```text
strata-storage       -> strata-core
strata-engine        -> strata-core, strata-storage
strata-search        -> strata-core, strata-engine, strata-storage
strata-intelligence  -> strata-core, strata-engine, strata-inference, strata-search
strata-executor      -> strata-core, strata-engine,
                        strata-intelligence, strata-search,
                        strata-storage
strata-cli           -> strata-executor, strata-intelligence
stratadb             -> strata-executor
```

This graph is the reason engine consolidation should finish vector and search
absorption before designing `storage-next`. Security/open options are already
engine-owned after `EG2`, product open/bootstrap is engine-owned after `EG3`,
graph is engine-owned after `EG4`, and vector is engine-owned after `EG5`.
Today the remaining peer crates are direct engine consumers, but several of them
also still bypass engine and reach storage directly.

The current inverse normal storage graph is:

```text
strata-storage
|-- strata-engine
|-- strata-executor
`-- strata-search
```

`strata-engine` is the intended permanent dependent. `strata-executor`
and `strata-search` are transitional direct storage bypasses that the engine
consolidation plan removes. `EG4` removed `strata-graph` by
moving graph implementation, storage-key mapping, transaction extension,
runtime hooks, and branch DAG behavior into engine. `EG5` moved vector
implementation, storage-key mapping, transaction extension, recovery hooks,
merge behavior, and sidecar-cache policy into engine, then deleted the retired
`strata-vector` package after search, intelligence, executor, benchmarks, and
root tests moved to `strata_engine::vector`.

Because most upper crates also depend on engine, the full inverse tree shows
executor, intelligence, and search under the engine branch as ordinary engine
consumers. That is not itself a storage bypass; the bypasses that matter here
are the direct normal storage edges above engine.

### Direct Storage Bypasses Above Engine

These normal production direct storage dependencies are accurate today and
should be treated as consolidation inputs. The source-level inventory is
tracked in [eg1-implementation-plan.md](./eg1-implementation-plan.md):

- `strata-search` uses storage keys/namespaces in retrieval substrate code.
- `strata-executor` uses storage keys, namespaces, type tags, validation, and
  storage errors directly.

`strata-intelligence`, `strata-cli`, and the root `stratadb` package do not
currently have direct normal storage dependencies.
Root dev-dependencies and storage-facing tests do import storage directly; those
are tracked separately in the `EG1` implementation plan for the `EG1D` guard
policy.

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

The storage-boundary cleanup moved generic checkpoint, WAL compaction, decoded-row
snapshot install, recovery bootstrap, storage runtime config application, and
storage retention mechanics into `strata-storage`. Engine still contains
adapter modules at those boundaries because the public APIs, primitive decode,
lifecycle policy, product config, and public error taxonomy remain
engine-owned.

The remaining storage-boundary modules are:

- [database/compaction.rs](../../crates/engine/src/database/compaction.rs)
  - public `Database::checkpoint()` / `Database::compact()` orchestration
  - conversion of storage checkpoint, compaction, snapshot-prune, and
    retention facts into engine reports
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
  - deriving storage runtime config from public engine config before calling
    storage-owned application helpers
- [database/config.rs](../../crates/engine/src/database/config.rs)
  - public `StrataConfig` and `StorageConfig` compatibility surface
  - adapter logic that builds storage-owned `StorageRuntimeConfig`

The lower storage mechanics behind these adapters now live in storage-owned
modules such as `durability/checkpoint_runtime.rs`,
`durability/decoded_snapshot_install.rs`, and
`durability/recovery_bootstrap.rs`, plus `runtime_config.rs`.

## Final Storage Boundary Map

| Surface | Storage Owns | Engine Owns | Intentional Seam |
|---|---|---|---|
| Checkpoint | File construction, raw checkpoint facts, manifest/watermark mechanics | `Database::checkpoint()`, lifecycle checks, public result/error mapping | Engine API calls storage runtime helper |
| WAL compaction | WAL pruning and manifest active-segment mechanics | `Database::compact()`, lifecycle checks, public result/error mapping | Engine API calls storage runtime helper |
| Snapshot pruning | Filesystem pruning and raw prune facts | lifecycle-aware retention report | Engine maps raw prune facts to public report |
| Snapshot install | Generic decoded-row validation and install | primitive section decode, primitive install stats, recovery policy | Engine builds decoded-row plan, storage installs it |
| Recovery bootstrap | MANIFEST prep, codec validation, replay, raw recovery facts | open policy, degraded/lossy policy, subsystem recovery, coordinator bootstrap | Engine calls storage recovery and interprets outcome |
| Storage config | storage runtime config builder, effective storage values, store/global application | public `StrataConfig`, product profiles, config compatibility | Engine adapts public config into storage runtime config |
| Retention | storage pruning mechanics and minimum-retain invariant | public retention policy and lifecycle behavior | Engine passes storage-shaped retention input |
| Test observability | storage-local runtime snapshots/accessors under test gates | characterization tests and production-use guardrails | Guardrails prevent production dependence |

## Current Architectural Role

If you describe the crate honestly as it exists today, `strata-engine` is:

- the database runtime and lifecycle hub
- the branch and primitive semantics owner
- the search/runtime host
- the branch bundle workflow host
- the layer that converts raw storage recovery/checkpoint facts into public
  database behavior

## Main Takeaway

The next cleanup should treat the storage boundary as settled but keep
engine's internal domain/runtime boundaries explicit.

The important ownership question is not whether engine is too large. It is
whether code in the crate is:

- semantic/domain behavior that should stay in `engine`, or
- generic persistence/runtime machinery that should stay delegated to
  `storage`

After the storage-boundary closeout, the semantic side of engine is real and
the targeted lower storage mechanics have sunk into storage. The remaining
cleanup is above this boundary: engine can absorb graph, vector, and search
responsibilities only if the substrate/mechanics boundary documented here stays
explicit.
