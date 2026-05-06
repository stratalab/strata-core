# EG5 Implementation Plan

## Purpose

`EG5` absorbs the current `strata-vector` crate into `strata-engine`.

Vector is not an independent layer in the target architecture. It is an
engine-owned primitive/runtime area because it:

- maps vector collection and vector row semantics onto storage keys and
  `TypeTag::Vector`
- extends engine transactions with vector operations
- owns in-memory index backends and post-commit maintenance
- owns mmap heap and HNSW graph sidecar cache policy
- participates in engine recovery, branch deletion, space deletion, and merge
  behavior
- feeds engine-owned search and intelligence workflows
- supplies product runtime lifecycle behavior through `VectorSubsystem`

After `EG5`, vector code may still use storage types, but only because that
code is physically inside engine. No production crate above engine should
import storage for vector behavior, and no production crate above engine should
instantiate `VectorSubsystem`.

This is the single implementation plan for the `EG5` phase. Lettered sections
such as `EG5A`, `EG5B`, and `EG5C` are tracked in this file rather than in
separate letter-specific documents.

Read this with:

- [engine-consolidation-plan.md](./engine-consolidation-plan.md)
- [engine-crate-map.md](./engine-crate-map.md)
- [eg1-implementation-plan.md](./eg1-implementation-plan.md)
- [eg2-implementation-plan.md](./eg2-implementation-plan.md)
- [eg3-implementation-plan.md](./eg3-implementation-plan.md)
- [eg4-implementation-plan.md](./eg4-implementation-plan.md)
- [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)

## Scope

`EG5` owns:

- moving the vector implementation into an engine-owned module
- preserving vector storage key formats and value formats
- preserving public vector DTO behavior already exposed by engine
- preserving vector implementation DTO behavior that previously lived in
  `strata-vector`
- preserving `VectorStore` behavior for collection CRUD, vector CRUD, search,
  history, source refs, metadata filters, and system shadow collections
- preserving vector transaction extension behavior on `TransactionContext`
- preserving post-commit index maintenance and abort cleanup behavior
- preserving vector recovery behavior, including degraded collection handling
- preserving mmap heap cache and HNSW graph sidecar cache behavior
- preserving follower versus primary cache rules
- preserving branch deletion, space deletion, and collection deletion purge
  behavior
- preserving vector merge precheck and post-commit rebuild behavior
- cutting search, intelligence, executor, root tests, and vector unit tests over
  to engine-owned imports
- removing the product-open external vector subsystem bridge
- removing normal production `strata-vector` dependencies from workspace crates
- deleting `crates/vector`
- tightening guard tests so vector storage bypasses cannot return outside engine

`EG5` does not own:

- redesigning vector indexing algorithms
- redesigning HNSW, segmented HNSW, brute force, quantization, or heap behavior
- changing vector storage bytes, collection record bytes, key layout, WAL
  behavior, snapshots, checkpoints, or manifests
- moving vector sidecar caches into storage
- changing `.vec` or `.hgr` sidecar cache formats
- absorbing the search crate (`EG6`)
- removing executor direct storage imports unrelated to vector (`EG7`)
- redesigning executor command/session APIs
- deleting all `OpenSpec::with_subsystem` test hooks
- starting storage-next or engine-next architecture work

## Load-Bearing Constraint

There must never be a normal dependency cycle:

```text
strata-engine -> strata-vector -> strata-engine
```

The vector implementation has to move physically into engine before engine can
own vector runtime registration. A duplicate live implementation is also not an
acceptable architecture. If a temporary `strata-vector` shell is needed for
compile sequencing, it must only re-export engine vector types and must not
contain vector logic or storage imports.

The closeout state is:

- `strata-engine` owns the implementation
- `strata-vector` is deleted
- no production code imports `strata_vector`
- no production code outside engine constructs vector storage keys
- no production code outside engine constructs `VectorSubsystem`
- product open does not accept an externally supplied vector subsystem bundle

## Pre-Move Code Map

The following inventory records the pre-`EG5` vector crate layout that guided
the move. After `EG5G`, `crates/vector` is deleted and the implementation lives
under `crates/engine/src/vector/`.

Pre-`EG5` vector crate files:

- `lib.rs` - public facade, re-exports, and `graph_dir` sidecar path helper
- `types.rs` - re-exports engine-owned public vector DTOs, plus
  implementation-specific records and options:
  `VectorRecord`, `CollectionRecord`, `VectorConfigSerde`, `SearchOptions`,
  `VectorMatchWithSource`, `IndexBackendType`, and inline metadata
- `error.rs` - `VectorError`, `VectorResult`, and conversion into
  `StrataError`
- `collection.rs` - collection name and vector key validation helpers
- `distance.rs` - distance scoring and normalization helpers
- `filter.rs` - metadata filter evaluation helpers
- `heap.rs` - vector heap storage, ID allocation, inline metadata, timestamp
  tracking, and mmap load/freeze support
- `quantize.rs` - scalar and binary quantization helpers
- `backend.rs` - backend trait and backend factory
- `brute_force.rs` - brute-force backend
- `hnsw.rs` - single-segment HNSW backend and graph serialization
- `segmented.rs` - segmented HNSW backend and sealed segment handling
- `mmap.rs` - mmap heap persistence helpers
- `mmap_graph.rs` - mmap-backed HNSW graph sidecar helpers
- `store/mod.rs` - `VectorStore`, `VectorBackendState`, pending operations,
  runtime wiring, backend initialization, search trait integration, and many
  unit tests
- `store/collections.rs` - collection create/list/delete and collection record
  persistence
- `store/crud.rs` - vector insert/update/delete/get/history behavior
- `store/search.rs` - vector search, filtered search, source-ref search, and
  time-range search
- `store/system.rs` - `_system_` shadow collection behavior used by search and
  intelligence
- `store/recovery.rs` - collection reload and rebuild helpers used by recovery
  and merge paths
- `ext.rs` - `VectorStoreExt` transaction extension trait, staged vector ops,
  and post-commit index application
- `recovery.rs` - `VectorSubsystem`, startup recovery, lifecycle/freeze hooks,
  sidecar cache policy, branch cache purge, and recovery tests
- `merge_handler.rs` - vector merge precheck and post-commit rebuild callbacks
- `benches/vector_benchmarks.rs` - HNSW, segmented HNSW, and brute-force
  benchmark coverage

Pre-`EG5` engine vector files:

- `crates/engine/src/semantics/vector.rs` - canonical public vector DTOs:
  `DistanceMetric`, `StorageDtype`, `VectorConfig`, `VectorId`,
  `VectorEntry`, `VectorMatch`, `CollectionInfo`, `CollectionId`,
  `MetadataFilter`, `FilterOp`, `FilterCondition`, and `JsonScalar`

Before `EG5`, the type split was already partial absorption. `strata-vector`
re-exported many engine-owned public DTOs, but implementation records, search
options, runtime state, storage mapping, recovery, merge, and sidecars still
lived outside engine. `EG5` finished that move without creating a second vector
type universe.

## Pre-Move Storage Touchpoints

Pre-`EG5` vector storage touchpoints:

- `crates/vector/Cargo.toml` - normal dependency on `strata-storage`
- `crates/vector/src/store/mod.rs` - `Key`, `Namespace`, backend state loaded
  from engine database storage, and tests that inspect storage keys
- `crates/vector/src/store/collections.rs` - `Key::new_vector_config`,
  `Key::new_vector_config_prefix`, and `Storage`
- `crates/vector/src/store/crud.rs` - `Key::new_vector`, transactional reads,
  history, and storage scan/get paths
- `crates/vector/src/store/search.rs` - `Key::new_vector`,
  `Key::vector_collection_prefix`, and search-time KV validation/hydration
- `crates/vector/src/store/system.rs` - system shadow collection persistence
- `crates/vector/src/store/recovery.rs` - vector config/vector row scans and
  rebuild helpers
- `crates/vector/src/ext.rs` - `TransactionContext`, `Key`, and `Namespace`
  for transaction-scoped vector writes
- `crates/vector/src/merge_handler.rs` - `Key`, `Namespace`, and `Storage` for
  merge config precheck
- `crates/vector/src/recovery.rs` - `Key`, `Namespace`, `Storage`, and
  `TypeTag` for startup recovery, branch/space scans, degradation, tests, and
  sidecar purge

Those touchpoints are legitimate only after the code moves into engine. They
should not be wrapped in a new upper-crate facade and they should not move into
storage.

## Consumer Cutover Map

Pre-EG5C production consumers of `strata-vector`:

- `crates/executor/src/compat.rs` supplied `VectorSubsystem` through the
  temporary product-open bridge. `EG5C` removed this; product open now composes
  vector internally.
- `crates/executor/src/bridge.rs` constructs `PrimitiveVectorStore` and maps
  vector metrics and filters into vector DTOs
- `crates/executor/src/handlers/vector.rs` previously imported
  `VectorStoreExt`, `VectorEntry`, and `VectorMatch`. `EG5D` removed the
  extension-trait import; executor now calls engine-owned `VectorStore`
  transaction methods.
- `crates/executor/src/arrow/ingest.rs` and `crates/executor/src/arrow/export.rs`
  convert `VectorError`
- `crates/search/src/substrate.rs` constructs `VectorStore`, uses
  `VectorResult`, and consumes `VectorMatchWithSource`
- `crates/intelligence/src/shadow.rs` constructs `VectorStore`
- `crates/intelligence/src/embed/runtime.rs` constructs `VectorStore`, handles
  `VectorError::CollectionAlreadyExists`, and tests with `VectorSubsystem`

Pre-EG5F manifest consumers:

- root `Cargo.toml` has a workspace dependency on `strata-vector`
- `crates/search/Cargo.toml`
- `crates/intelligence/Cargo.toml`
- `crates/executor/Cargo.toml`
- `crates/vector/Cargo.toml`

After `EG5F`, only `crates/vector/Cargo.toml` remains; that package is the
temporary zero-logic compatibility shell retired by `EG5G`.

Pre-EG5F test consumers:

- `tests/common/mod.rs`
- `tests/executor_ex6_runtime.rs`
- `tests/executor/*`
- `tests/integration/*`
- `tests/intelligence/*`
- `tests/durability/*`
- `crates/search/src/substrate.rs` tests
- `crates/intelligence/src/embed/runtime.rs` tests
- `crates/vector` unit tests and benchmarks

`EG5` should distinguish production cutover from test cutover. Tests may keep
inspecting vector internals, but they must import those internals from engine
after the move. `EG5F` migrated root, executor, integration, intelligence, and
search tests to engine-owned vector imports; `crates/vector` tests disappear
when the compatibility shell is deleted in `EG5G`.

## Current Behavior Contract

Unless explicitly called out in an implementation review, `EG5` must preserve:

- vector row keys continuing to use the current vector key layout
- vector config keys continuing to use `__config__/{collection}`
- all vector rows continuing to use `TypeTag::Vector`
- collection config bytes continuing to use `CollectionRecord` MessagePack
  encoding
- vector value bytes continuing to use `VectorRecord` MessagePack encoding
- `DistanceMetric::to_byte` / `from_byte` values
- `StorageDtype::to_byte` / `from_byte` values
- `IndexBackendType` byte values
- `VectorId` monotonic allocation and non-reuse behavior
- update behavior preserving an existing vector ID
- validation behavior for collection names, vector keys, dimensions, empty
  vectors, NaN, and infinity
- `VectorStore` behavior for create/list/delete collection, insert, get,
  get-at, getv/history, delete, exists, search, filtered search, source-ref
  search, and system shadow search
- metadata filter semantics, including equality, advanced conditions, `In`,
  and `Contains`
- source-ref persistence and hydration used by search and intelligence
- `_system_` shadow collection behavior
- collection isolation across `(branch, space, collection)`
- `VectorBackendState` sharing across `VectorStore` instances for one database
- pending vector operations applying only after successful commit
- pending vector operations clearing on abort or failed commit
- best-effort post-commit HNSW maintenance warnings remaining non-fatal
- storage becoming durable even if post-commit index maintenance fails
- startup recovery rebuilding backends from KV when sidecars are missing,
  corrupt, stale, or rejected
- degraded collection behavior on config decode or shape conversion failures
- cache databases skipping vector recovery
- followers not trusting primary sidecar caches and rebuilding from
  follower-visible KV state
- primaries using `.vec` and `.hgr` caches only as accelerators
- sidecar layout under `{data_dir}/vectors/{branch_hex}/{space}/`
- heap mmap file name `{collection}.vec`
- graph sidecar directory `{collection}_graphs/`
- branch deletion purging branch vector sidecar cache trees
- collection and space deletion purging relevant vector sidecar caches
- vector merge precheck rejecting incompatible shared collections by dimension,
  metric, or storage dtype
- vector merge post-commit rebuilding only affected `(space, collection)` pairs
  and warning, not failing, after KV commit
- product runtime subsystem order becoming engine-owned:

```text
graph -> vector -> search
```

The vector move must not alter storage, WAL, manifest, checkpoint, snapshot, or
recovery file formats.

## Target Engine Shape

Preferred module placement:

```text
crates/engine/src/vector/
```

Preferred public import paths:

```text
strata_engine::VectorStore
strata_engine::VectorSubsystem
strata_engine::VectorError
strata_engine::VectorResult
strata_engine::VectorConfig
strata_engine::VectorEntry
strata_engine::VectorMatch
strata_engine::VectorMatchWithSource
strata_engine::vector::SearchOptions
strata_engine::vector::IndexBackendType
```

The exact re-export set can be tightened during implementation, but the
ownership shape should not change:

- vector implementation lives in engine
- vector storage key helpers live in engine
- vector transaction extension trait lives in engine
- vector subsystem lifecycle lives in engine
- vector sidecar cache policy lives in engine
- vector merge handlers live in engine
- search/executor/intelligence/tests consume vector through engine

The existing public DTOs in `crates/engine/src/semantics/vector.rs` should
remain the canonical public DTOs unless implementation proves that a small
move/rename is cleaner. The moved runtime should import those types from
engine, not recreate a parallel type system.

Implementation-specific DTOs currently in `crates/vector/src/types.rs` can move
with the runtime. The important distinction is conceptual:

- public vector API DTOs are engine semantic surface
- record bytes, backend options, sidecar metadata, and recovery internals are
  engine implementation details

## Temporary Bridge Policy

`EG3` introduced a temporary product-open bridge because graph and vector still
lived outside engine. `EG4` removed graph from upper-crate subsystem assembly.
`EG5` must remove vector from that assembly.

The target after `EG5` is:

- executor calls product open without supplying vector subsystems
- engine product open internally assembles graph, vector, and search lifecycle
  behavior
- no production crate above engine names `VectorSubsystem`
- `OpenSpec::with_subsystem` remains only as a low-level/test hook until a
  later phase narrows or deletes it

Do not replace the temporary vector bridge with a polished abstraction. The
subsystem-instantiation architecture is intentionally being retired.

## Shared Commands

Current `EG5` verification commands:

```bash
cargo test -p strata-engine --lib vector
cargo test -p strata-search
cargo test -p strata-intelligence
cargo test -p strata-executor
cargo test -p stratadb --test engine_consolidation_vector_characterization
cargo test -p stratadb --test executor_ex6_runtime
cargo test -p stratadb --test storage_surface_imports
```

Useful focused inventories:

```bash
rg -n "strata_vector|strata-vector|VectorSubsystem|VectorStoreExt|VectorMatchWithSource|VectorError" \
  crates tests benchmarks Cargo.toml Cargo.lock -g 'Cargo.toml' -g '*.rs' -g '!target/**'

rg -n "strata_storage::|use strata_storage|strata-storage|strata_storage" \
  crates/engine/src/vector crates/search crates/executor crates/intelligence crates/cli \
  -g 'Cargo.toml' -g '*.rs'
```

During movement:

```bash
cargo fmt --check
cargo check -p strata-engine
cargo check -p strata-search
cargo check -p strata-intelligence
cargo check -p strata-executor
cargo test -p strata-engine --lib vector
cargo test -p stratadb --test storage_surface_imports
```

Closeout guards:

```bash
cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[] | select(.name=="strata-vector") | .name'

cargo tree -p strata-search --edges normal | rg "strata-vector"
cargo tree -p strata-intelligence --edges normal | rg "strata-vector"
cargo tree -p strata-executor --edges normal | rg "strata-vector"

rg -n "strata_vector|strata-vector" crates tests benchmarks Cargo.toml Cargo.lock \
  -g 'Cargo.toml' -g '*.rs' -g '!target/**'
```

At `EG5` closeout, `strata-search` can still have direct storage access because
that is removed in `EG6`. `strata-executor` can still have direct storage
access unrelated to vector because that is removed in `EG7`.

## Section Status

| Section | Status | Purpose |
| --- | --- | --- |
| `EG5A` | Complete | Vector code map and characterization |
| `EG5B` | Complete | Engine vector module and canonical type consolidation |
| `EG5C` | Complete | Runtime wiring and product-open composition |
| `EG5D` | Complete | Transaction extension and backend state absorption |
| `EG5E` | Complete | Recovery, sidecar, lifecycle, and purge migration |
| `EG5F` | Complete | Merge, search, intelligence, and executor cutover |
| `EG5G` | Complete | Retire vector crate and tighten guards |
| `EG5H` | Complete | Closeout review |

## EG5A - Vector Code Map And Characterization

**Goal:**

Pin the existing vector behavior before moving runtime code.

**Work:**

- refresh the vector module map from the current tree
- refresh the vector storage-touchpoint inventory
- refresh the consumer map for search, intelligence, executor, root tests, and
  vector unit tests
- identify which current vector types are already engine-owned and which are
  implementation-owned
- identify the current product-open bridge entry points that supply
  `VectorSubsystem`
- identify tests that already cover the current behavior contract
- add characterization tests where behavior is important but only indirectly
  covered

Characterization should cover at least:

- product open installs vector runtime with graph and search
- `VectorStore` instances share one `VectorBackendState` per database
- transaction upsert/delete update KV immediately inside the transaction but
  update the index only after commit
- abort and failed commit clear staged vector operations
- current recovery can rebuild from KV without sidecars
- corrupt or missing sidecars fall back to KV recovery
- followers rebuild from follower-visible KV and do not trust primary sidecars
- branch delete and collection/space delete purge sidecars
- merge precheck rejects dimension, metric, and storage dtype mismatches
- merge post-commit rebuilds affected collections only
- source refs and metadata survive recovery and feed search/intelligence
- non-default spaces and `_system_` shadow collections remain isolated

**Acceptance:**

- vector code map and storage-touchpoint inventory are current
- baseline tests or explicit gaps are recorded before module movement
- any added characterization tests pass against the pre-move implementation
- no code ownership movement is mixed into `EG5A`

**Implementation evidence as of 2026-05-06:**

- The current code map, storage-touchpoint inventory, consumer map, type split,
  behavior contract, and bridge policy in this document reflect the pre-move
  vector tree.
- The public vector DTO split is explicit: engine already owns canonical
  public DTOs in `crates/engine/src/semantics/vector.rs`, while
  `crates/vector/src/types.rs` re-exports those DTOs and still owns
  implementation records/options such as `VectorRecord`, `CollectionRecord`,
  `VectorConfigSerde`, `SearchOptions`, `VectorMatchWithSource`, and
  `IndexBackendType`.
- Existing vector coverage already pins backend-state sharing, staged
  transaction apply/abort behavior, branch-delete sidecar purge, follower cache
  behavior, and vector merge mismatch rejection.
- Added `tests/engine_consolidation_vector_characterization.rs` with focused
  EG5A boundary coverage:
  - product open installs `graph -> vector -> search`
  - vector recovery rebuilds non-default-space vectors and `_system_` source
    refs from KV after sidecars are removed
  - collection delete purges `.vec` and `_graphs/` sidecars and prevents stale
    vector resurrection on same-name collection recreate/reopen
- Verified with:

```bash
cargo test -p stratadb --test engine_consolidation_vector_characterization
```

## EG5B - Engine Vector Module And Type Consolidation

**Goal:**

Move the vector implementation into engine without changing behavior or storage
format, and prevent duplicate vector type ownership.

**Work:**

- create the engine-owned vector module, preferably `crates/engine/src/vector/`
- move vector implementation files into that module with mechanical path
  updates
- keep public vector DTOs canonical in engine
- move implementation-specific DTOs with the runtime
- adjust module visibility and re-exports so current in-repo consumers can
  import vector behavior from `strata_engine`
- update stale docs/comments that say vector implementation lives in
  `strata-vector`
- keep vector runtime storage-key construction inside the moved engine module;
  cross-primitive engine mechanics such as checkpointing, space inventory, and
  branch merge classification may still mention `TypeTag::Vector`
- preserve tests with the moved files where practical
- keep any temporary `strata-vector` shell zero-logic if compile sequencing
  requires it

**Implementation notes:**

- `crates/vector/src/types.rs` already re-exports engine-owned DTOs. Preserve
  that direction: moved runtime code should use engine DTOs as canonical.
- Do not copy `VectorConfig`, `DistanceMetric`, `StorageDtype`, `VectorEntry`,
  `VectorMatch`, `CollectionInfo`, `CollectionId`, or metadata filter DTOs into
  a second independent module.
- If `strata_engine::vector::*` is exposed, it can re-export canonical DTOs plus
  runtime-specific types. That is an import convenience, not a separate
  semantic layer.

**Acceptance:**

- vector implementation compiles as engine-owned code
- vector public DTO behavior is still single-owned by engine
- vector runtime storage-key construction exists inside engine-owned vector code;
  remaining `TypeTag::Vector` use outside that module must be engine
  cross-primitive mechanics or explicitly allowlisted executor cleanup work
- `cargo test -p strata-engine --lib vector` passes or the exact renamed test
  target is recorded
- any temporary `strata-vector` shell has no storage dependency and no vector
  logic

**Status:** Complete.

**Implementation notes:**

- Moved the vector runtime files from `crates/vector/src/` into
  `crates/engine/src/vector/`.
- Added `strata_engine::vector` plus root re-exports for the vector runtime
  types, including `VectorStore`, `VectorSubsystem`, vector transaction
  extension traits, vector backends, implementation records, and errors.
- Preserved public vector DTO ownership in
  `crates/engine/src/semantics/vector.rs`; the moved vector module re-exports
  those canonical DTOs instead of defining a second copy.
- Reduced `crates/vector` to a zero-logic compatibility shell that re-exports
  `strata_engine::vector::*`.
- Moved the vector benchmark target to `strata-engine` so `strata-vector` has no
  benchmark or dev-target logic.
- Removed the vector entries from the direct-storage bypass allowlist because
  the compatibility shell has no normal `strata-storage` dependency.
- Added a guard that fails if `strata-vector` grows Rust files, bench targets,
  dev-dependencies, or non-engine dependencies again.

**Verification:**

```bash
cargo check -p strata-engine --all-targets
cargo test -p strata-engine --lib vector
cargo test -p stratadb --test engine_consolidation_vector_characterization
cargo test -p stratadb --test storage_surface_imports
cargo fmt --check
```

`cargo test -p strata-engine --lib vector` passed with 401 vector-related tests
passing and 8 ignored. The characterization test passed 3/3, and the storage
surface guard passed 13/13.

`EG5B` temporarily checked the zero-logic `strata-vector` shell before `EG5G`
deleted it; after `EG5G`, verify absence with `cargo metadata` instead of
checking the retired package directly.

## EG5C - Runtime Wiring And Product Open Composition

**Goal:**

Make vector lifecycle registration engine-owned and delete the external vector
subsystem bridge from production product open.

**Work:**

- move `VectorSubsystem` into engine
- wire vector recovery, initialize, bootstrap, and freeze hooks from the
  engine-owned runtime composition path
- update product open so engine internally composes:

```text
graph -> vector -> search
```

- delete executor's temporary function that supplies `VectorSubsystem`
- simplify product open entry points so executor no longer supplies
  `Vec<Box<dyn Subsystem>>` for vector
- preserve low-level `OpenSpec::with_subsystem` behavior for tests until a
  later phase narrows it
- add or tighten guards that reject production upper-crate `VectorSubsystem`
  references

**Acceptance:**

- production executor code does not name `VectorSubsystem`
- product open installs vector runtime without an executor-supplied bridge
- product-open tests assert the expected runtime composition
- direct engine `OpenSpec` tests that intentionally use subsystems still work
- no new product-open facade replaces the subsystem bridge

**Implementation Notes:**

- Changed `open_product_database(path, options)` and `open_product_cache()` so
  callers no longer pass subsystem bundles.
- Product open now composes `GraphSubsystem`, `VectorSubsystem`, and
  `SearchSubsystem` internally in that order for primary, follower, and cache
  opens.
- Removed executor's transitional vector-subsystem supplier.
- Tightened `tests/storage_surface_imports.rs` with a production-source guard
  rejecting upper-crate `VectorSubsystem` assembly, mirroring the graph guard.
- Updated EG5 characterization coverage so cache, disk, disk reopen, and
  follower product-open paths prove vector behavior through engine-owned
  runtime composition.

**Verified:**

```bash
cargo check -p strata-engine --all-targets
cargo check -p strata-executor --all-targets
cargo test -p strata-engine --lib database::product_open
cargo test -p strata-executor --lib compat
cargo test -p stratadb --test engine_consolidation_vector_characterization
cargo test -p stratadb --test storage_surface_imports
cargo fmt --check
```

Product-open tests passed 11/11, executor compatibility tests passed 6/6,
vector characterization passed 6/6, and the storage surface guard passed 15/15.

## EG5D - Transaction Extension And Backend State Absorption

**Goal:**

Move transaction-scoped vector behavior and backend state maintenance into
engine.

**Work:**

- move transaction-scoped vector writes into engine
- move `StagedVectorOp`, `apply_staged_vector_op`, and pending-op maintenance
  into engine-internal vector modules
- update executor vector handlers to use engine-owned `VectorStore`
  transaction methods and vector DTOs from engine
- preserve read-your-writes behavior inside `TransactionContext`
- preserve vector ID allocation behavior and update-in-place semantics
- preserve queue-on-write, apply-on-commit, and clear-on-abort behavior
- ensure runtime wiring remains idempotent for repeated `VectorStore::new`
  calls

**Acceptance:**

- transaction vector tests pass under engine-owned imports
- executor vector transaction paths do not import `strata_vector` or internal
  staged-op APIs
- staged operations are applied exactly once after successful commit
- staged operations are not applied on abort or failed commit
- post-commit index maintenance remains best-effort and non-fatal

**Implementation Notes:**

- The `VectorStoreExt` implementation, `StagedVectorOp`, and staged-op apply
  helper are engine-internal. Public callers use
  `VectorStore::{insert,delete,get}_in_transaction`, so they cannot manually
  apply a staged op or queue work onto an arbitrary backend state.
- Engine-internal vector transaction writes queue staged backend ops by
  transaction id; the commit observer is the only path that applies them. Abort
  observers clear queued ops without applying them.
- `VectorStore::insert()` and `VectorStore::delete()` no longer manually apply
  the returned staged op after `Database::transaction()`, preventing duplicate
  backend application when the commit observer has already drained the queue.
- executor vector transaction handlers import `VectorEntry` and `VectorMatch`
  from `strata_engine` and call the engine-owned `VectorStore` transaction
  methods.
- `storage_surface_imports` guards that executor production source does not
  consume the `strata-vector` compatibility shell or internal staged-op API.

## EG5E - Recovery, Sidecar, Lifecycle, And Purge Migration

**Goal:**

Move vector recovery and sidecar cache policy into engine while preserving the
cache-as-accelerator contract.

**Work:**

- move startup recovery into the engine vector module
- move lifecycle/freeze hooks into engine
- move mmap heap and mmap graph sidecar helpers into engine
- move branch cache, collection cache, and space purge helpers into engine
- preserve branch/space-aware sidecar paths
- preserve cache database skip behavior
- preserve follower rebuild behavior
- preserve degraded collection marking on config decode/shape failures
- preserve KV fallback when sidecars are missing, corrupt, stale, empty, or
  dimension-incompatible
- update lifecycle docs/comments in engine that reference
  `VectorSubsystem::freeze`

**Acceptance:**

- vector recovery tests pass under engine-owned imports
- primary reopen can use sidecar caches as accelerators
- follower reopen rebuilds from KV and does not trust primary sidecars
- missing/corrupt sidecars do not cause data loss
- branch delete, collection delete, and space delete cannot resurrect stale
  sidecars on next recovery
- no sidecar cache behavior is moved into storage

**Implementation Notes:**

- Vector recovery, lifecycle hooks, mmap heap helpers, mmap graph helpers, and
  branch/collection/space sidecar purge helpers live under
  `crates/engine/src/vector`.
- Recovery tests now import recovery-visible vector state from
  `strata_engine`, not the `strata-vector` compatibility shell.
- Added engine-owned characterization coverage for:
  - primary reopen loading a valid `.vec` cache as an accelerator
  - corrupt `.vec` cache fallback to KV without data loss or degradation
  - production `space_delete --force` removing orphan `.vec` and `_graphs/`
    sidecars before same-name collection recreation and lazy reload
- Existing engine vector recovery tests cover follower refresh/restart paths,
  blocked-follower cache distrust, degraded collection marking, branch-cache
  purge, and orphan branch-cache removal.
- The sidecar contract remains cache-only: KV rows remain authoritative and no
  vector sidecar behavior moved into storage.

## EG5F - Merge, Search, Intelligence, And Executor Cutover

**Goal:**

Cut all production vector consumers over to engine-owned vector surfaces.

**Work:**

- move vector merge precheck and post-commit rebuild callbacks into engine
- remove any merge-registry function-pointer bridge that only existed because
  vector was a peer crate, unless keeping it temporarily is simpler and
  explicitly documented
- update `strata-search` to import `VectorStore`, vector result types, and
  vector match/source types from engine
- update `strata-intelligence` to import `VectorStore`, `VectorError`, and
  vector DTOs from engine
- update executor bridge, handlers, and Arrow conversion code to import vector
  surfaces from engine
- remove vector metric/filter conversion functions that become identity
  conversions after the type move
- update root tests and shared test helpers to import vector surfaces from
  engine
- move vector benchmarks or explicitly document their new home

**Acceptance:**

- search has no `strata-vector` dependency
- intelligence has no `strata-vector` dependency
- executor has no `strata-vector` dependency
- production source outside engine has no `strata_vector` imports
- existing search/intelligence/executor vector behavior tests pass
- search may still have direct storage access until `EG6`, but not because of
  vector

**Implementation Notes:**

- `strata-search`, `strata-intelligence`, `strata-executor`, and root tests now
  consume vector APIs through `strata_engine`.
- Root, search, intelligence, and executor manifests no longer depend on the
  `strata-vector` compatibility shell.
- The vector merge precheck and post-commit callbacks are engine-owned. The
  function-pointer slots remain as an internal primitive-merge registry seam
  until the later merge-registry cleanup.
- Executor metric/filter conversion remains because executor command DTOs are
  still a separate public API from engine vector DTOs.
- `storage_surface_imports` now guards that production consumers above engine do
  not import `strata_vector` or depend on `strata-vector`.

## EG5G - Retire The Vector Crate And Tighten Guards

**Goal:**

Delete the old vector crate edge and make its return difficult.

**Work:**

- remove `strata-vector` from workspace dependency metadata
- remove normal dependencies on `strata-vector` from all manifests
- delete `crates/vector`
- remove vector entries from the direct-storage bypass allowlist
- add a retired vector crate guard similar to the graph and security guards
- add a guard rejecting production upper-crate `VectorSubsystem` assembly
- refresh `Cargo.lock`
- refresh engine crate map and active docs that mention vector as a peer crate

**Acceptance:**

- `cargo metadata` reports no `strata-vector` package
- `cargo tree -p strata-search --edges normal` has no `strata-vector`
- `cargo tree -p strata-intelligence --edges normal` has no `strata-vector`
- `cargo tree -p strata-executor --edges normal` has no `strata-vector`
- `tests/storage_surface_imports.rs` has no `EG5` vector allowlist entries
- guard tests fail if `strata-vector` or production `VectorSubsystem` assembly
  is reintroduced

**Completed changes:**

- Deleted the temporary `crates/vector` compatibility shell.
- Removed `strata-vector` from workspace membership and `Cargo.lock`.
- Replaced the zero-logic shell guard with a retired-crate guard that scans
  root, benchmark, and workspace production source/manifests.
- Added manifest regression checks for `strata-vector` references.
- Refreshed active engine/storage crate-map docs to describe vector as
  engine-owned and the vector crate as deleted.

## EG5H - Closeout Review

**Goal:**

Review all vector movement before starting search absorption.

**Work:**

- review remaining `strata_vector` and `strata-vector` matches
- review remaining vector-related `strata_storage` matches outside engine
- review product-open composition and subsystem-instantiation references
- review vector docs/comments for stale crate ownership claims
- review storage consumption contract for the engine-owned vector endpoints
- review direct-storage bypass allowlist for stale vector entries
- run the focused test matrix and record known pre-existing failures separately

**Acceptance:**

- engine owns vector runtime, transaction extension, recovery, merge, sidecar,
  purge, and product lifecycle behavior
- production crates above engine consume vector only through engine
- product open no longer accepts an executor-supplied vector subsystem bundle
- vector storage key construction exists only inside engine
- `strata-vector` is deleted
- `EG6` can start with search as the next remaining peer runtime crate

**Completed changes:**

- Re-ran the closeout inventories for `strata_vector`, `strata-vector`,
  `crates/vector`, vector-related storage imports, product-open runtime
  composition, and direct vector storage bypass allowlist entries.
- Added `VectorStore::delete_space_data_in_transaction` so force space
  deletion can delete vector rows in the existing all-primitive delete
  transaction through engine-owned vector semantics instead of constructing
  `TypeTag::Vector` prefixes in executor.
- Removed the remaining non-test benchmark subsystem assembly for vector by
  changing the materialize regression smoke to use engine-owned product open.
- Added a guard that rejects vector storage-key layout construction in
  production crates above engine.
- Updated the storage consumption contract and active crate-map docs so vector is
  recorded as engine-owned and the retired vector crate is not listed in runnable
  guard paths.
- Confirmed remaining `VectorSubsystem` assembly outside engine is test-only or
  engine-owned product-open composition.
- Confirmed remaining direct storage imports outside engine are search/executor
  migration debt for `EG6`/`EG7`, not vector crate residue.
- Confirmed `strata-vector` is absent from workspace metadata and normal
  dependency trees for search, intelligence, and executor.

## Risks

- Type duplication. Engine already owned public vector DTOs before the move,
  while `strata-vector` owned implementation records. `EG5B` resolved this by
  moving implementation records into engine and reusing the canonical DTOs.
- Transaction double-apply. The current code queues staged ops for commit
  observers and also returns staged ops for backward compatibility. Cutover
  must preserve exactly-once post-commit behavior.
- Sidecar stale resurrection. Deleted branches, spaces, or collections must not
  return after restart because a `.vec` or `.hgr` cache survived.
- Follower cache misuse. Followers intentionally rebuild from visible KV state
  instead of trusting primary sidecars.
- Recovery degradation semantics. Config decode or shape failures must continue
  to fail closed for the affected collection.
- Non-default spaces. Collection identity is `(branch, space, collection)`;
  sidecar paths, backend maps, recovery scans, and merge rebuilds must preserve
  the space component.
- `_system_` shadow collections. Intelligence/search shadow collections live in
  the system space but point back to user-space source refs.
- Merge callback simplification. Once vector is inside engine, the callback
  bridge may become unnecessary. Removing it is safe only if behavior and error
  paths stay identical.
- Search cutover. `strata-search` still has its own storage bypass until `EG6`.
  EG5 should remove its vector crate dependency without attempting to absorb the
  search substrate early.
- Benchmarks. `EG5B` moved the vector benchmark target to engine so benchmarks
  do not keep the old crate alive.

## Closeout Definition

`EG5` is complete when:

1. `strata-engine` owns vector runtime, vector subsystem code, vector storage
   mapping, vector transaction extension behavior, vector recovery, vector
   merge behavior, and vector sidecar cache policy.
2. `strata-vector` is deleted.
3. Search, intelligence, executor, and tests import vector behavior from
   engine.
4. No production crate above engine constructs `VectorSubsystem`.
5. No production crate above engine imports `strata_vector`.
6. Direct vector storage bypass allowlist entries have been removed.
7. Product open internally composes graph, vector, and search runtime behavior.
8. Storage, WAL, snapshot, checkpoint, manifest, vector record, collection
   record, and sidecar file formats are unchanged.
9. Focused vector, search, intelligence, executor, and guard tests pass or
   known pre-existing failures are explicitly recorded.
