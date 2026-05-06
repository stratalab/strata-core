# EG4 Implementation Plan

## Purpose

`EG4` absorbs the current `strata-graph` crate into `strata-engine`.

Graph is not an independent layer in the target architecture. It is an
engine-owned primitive/runtime area because it:

- maps graph semantics onto storage keys and `TypeTag::Graph`
- extends engine transactions with graph operations
- registers engine lifecycle hooks and branch DAG hooks
- contributes primitive-aware merge behavior
- participates in engine-owned search indexing and follower refresh
- bootstraps `_system_` / `_branch_dag` branch metadata

After `EG4`, graph code may still use storage types, but only because that code
is physically inside engine. No production crate above engine should import
storage for graph behavior, and no production crate above engine should
instantiate `GraphSubsystem`.

This is the single implementation plan for the `EG4` phase. Lettered sections
such as `EG4A`, `EG4B`, and `EG4C` are tracked in this file rather than in
separate letter-specific documents.

Read this with:

- [engine-consolidation-plan.md](./engine-consolidation-plan.md)
- [engine-crate-map.md](./engine-crate-map.md)
- [eg1-implementation-plan.md](./eg1-implementation-plan.md)
- [eg2-implementation-plan.md](./eg2-implementation-plan.md)
- [eg3-implementation-plan.md](./eg3-implementation-plan.md)
- [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)

## Scope

`EG4` owns:

- moving the graph implementation into an engine-owned module
- preserving graph storage key formats and value formats
- preserving `GraphStore` / `PrimitiveGraphStore` behavior
- preserving graph transaction extension behavior
- preserving graph search indexing, commit/abort observers, and follower
  refresh hooks
- preserving branch DAG projection, branch status cache, audit observer, and
  graph subsystem lifecycle behavior
- preserving graph semantic merge behavior and conflict reporting
- cutting search, executor, intelligence tests, and root integration tests over
  to engine-owned graph imports
- reducing the product-open subsystem bridge so graph is no longer supplied by
  executor
- removing normal production `strata-graph` dependencies from workspace crates
- deleting `crates/graph`, or leaving only a temporary re-export shell with a
  same-phase deletion criterion
- tightening guard tests so graph storage bypasses cannot return outside engine

`EG4` does not own:

- redesigning graph storage format
- redesigning graph query semantics, graph analytics, ontology, or merge rules
- moving graph semantics into storage
- absorbing vector (`EG5`)
- absorbing search crate behavior (`EG6`)
- removing executor direct storage imports unrelated to graph (`EG7`)
- redesigning executor command/session APIs
- redesigning `OpenSpec` or deleting all subsystem test hooks
- changing WAL, manifest, checkpoint, snapshot, or segment formats

## Load-Bearing Constraint

There must never be a normal dependency cycle:

```text
strata-engine -> strata-graph -> strata-engine
```

The graph implementation has to move physically into engine before engine can
own graph runtime registration. A duplicate live implementation is also not an
acceptable architecture. If a temporary `strata-graph` shell is needed for
compile sequencing, it must only re-export engine graph types and must not
contain graph logic or storage imports.

The intended closeout state is:

- `strata-engine` owns the implementation
- `strata-graph` is deleted, or is a zero-logic shell with an explicit removal
  date inside `EG4`
- no production code imports `strata_graph`
- no production code outside engine constructs graph storage keys
- no production code outside engine constructs `GraphSubsystem`

## Current Code Map

Current graph crate files:

- `lib.rs` - public facade, `GraphStore`, `PrimitiveGraphStore`, and
  `Searchable` implementation
- `types.rs` - public graph DTOs, options, stats, pagination, ontology types
- `keys.rs` - graph key layout, graph catalog keys, node/edge/ontology key
  helpers, and `GRAPH_SPACE`
- `store.rs` - `GraphBackendState`, staged search-index operations,
  commit/abort observers, and follower refresh hook
- `ext.rs` - `GraphStoreExt` transaction extension trait for
  `TransactionContext`
- `nodes.rs`, `edges.rs`, `bulk.rs`, `snapshot.rs` - core graph persistence
  and bulk/snapshot helpers
- `branch_dag.rs` - `_system_` / `_branch_dag` projection helpers,
  `GraphSubsystem`, branch status cache hydration, audit observer registration
- `branch_status_cache.rs` - advisory branch lifecycle cache
- `dag_hook_impl.rs` - engine branch DAG hook implementation
- `merge.rs`, `merge_handler.rs`, `integrity.rs` - graph semantic merge and
  engine merge-registry adapter
- `traversal.rs`, `analytics.rs`, `ontology.rs`, `adjacency.rs`, `packed.rs`,
  `boost.rs` - graph algorithms, ontology behavior, adjacency encoding, search
  boost behavior

Current graph storage touchpoints:

- `strata_storage::Key`
- `strata_storage::Namespace`
- `strata_storage::TypeTag`
- `strata_storage::TransactionContext`
- storage user-key parsing for graph refresh and merge
- graph snapshot and scan paths over engine database storage

Those touchpoints are legitimate only after the code moves into engine. They
should not be wrapped in a new upper-crate facade.

## Current Consumer Map

Current production consumers of `strata-graph`:

- `crates/executor/src/bridge.rs` constructs `PrimitiveGraphStore`
- `crates/executor/src/handlers/graph.rs` imports `GraphStoreExt` and graph DTOs
- `crates/executor/src/handlers/graph_impl.rs` imports graph DTOs
- `crates/executor/src/handlers/database.rs` refers to graph stats
- `crates/executor/src/compat.rs` supplies `GraphSubsystem` to product open
- `crates/search/src/substrate.rs` constructs `GraphStore` for BM25 fan-out

Current dev/test manifest consumers of `strata-graph` also include root
`Cargo.toml` and `crates/intelligence/Cargo.toml`. Intelligence has no normal
`strata-graph` edge today; its graph use is test-only and must still be cut over
so the retired crate does not remain alive through dev-dependencies.

Current test consumers:

- `tests/common/mod.rs` and `tests/common/branching.rs`
- `tests/executor/*`
- `tests/executor_ex6_runtime.rs`
- `tests/integration/branching*.rs`
- `tests/integration/recovery_cross_crate.rs`
- `crates/search/src/substrate.rs` tests
- `crates/graph` unit tests
- `crates/intelligence/src/embed/runtime.rs` tests
- `crates/intelligence/tests/expand_cache_fork_test.rs`

`EG4` should distinguish production cutover from test cutover. Tests may keep
inspecting graph internals, but they must import those internals from engine
after the move.

## Current Behavior Contract

Unless explicitly called out in an implementation review, `EG4` must preserve:

- graph key layout, including catalog, meta, node, adjacency, ontology, ref
  index, type index, and counter key shapes
- all graph rows continuing to use `TypeTag::Graph`
- `_graph_` graph system space behavior
- `_system_` branch and `_branch_dag` graph bootstrap behavior
- `dag_branch_node_id_for_ref` canonical branch-ref node encoding
- `GraphStore` CRUD behavior for graphs, nodes, edges, ontology, traversal,
  analytics, search indexing, and snapshot stats
- `GraphStoreExt` transaction behavior on `TransactionContext`
- staged graph search-index work applying only after successful commit
- staged graph work clearing on abort or failed commit
- follower refresh indexing graph nodes through graph semantics
- branch status cache hydration and cleanup on deleted branch lifecycle
- branch DAG hook installation and fail-fast branch lifecycle behavior
- audit event emission as a branch operation observer
- graph semantic merge behavior, including fatal referential-integrity
  conflicts and conflict-entry formatting
- search behavior for `PrimitiveType::Graph`, graph `EntityRef` values, and
  graph BM25 candidate ordering
- product runtime subsystem order while vector remains external:

```text
graph -> vector -> search
```

The graph move must not alter storage, WAL, manifest, checkpoint, snapshot, or
recovery file formats.

## Target Engine Shape

Preferred module placement:

```text
crates/engine/src/graph/
```

Preferred public import paths:

```text
strata_engine::GraphStore
strata_engine::PrimitiveGraphStore
strata_engine::GraphSubsystem
strata_engine::graph::GraphStoreExt
strata_engine::graph::types::NodeData
strata_engine::graph::keys::GRAPH_SPACE
```

The exact re-export set can be tightened during implementation, but the
ownership shape should not change:

- graph implementation lives in engine
- graph storage key helpers live in engine
- graph transaction extension trait lives in engine
- graph subsystem lifecycle lives in engine
- search/executor/intelligence/tests consume graph through engine

The `strata_engine::graph` module can expose submodules that mirror the current
crate during movement. Later architecture work can decide whether graph should
be reorganized into `engine-next`. `EG4` is a consolidation phase, not a graph
redesign phase.

## Temporary Bridge Policy

`EG3` left a temporary executor helper named for graph/vector absorption:

```text
legacy_product_subsystems_until_graph_vector_absorption()
```

`EG4` must remove graph from that bridge. After graph is engine-owned, product
open should no longer require executor to provide `GraphSubsystem`.

Vector is still external until `EG5`, so a vector-only temporary bridge may
remain. It must be named as vector-only transitional architecture, and it must
preserve the runtime order:

```text
engine graph subsystem -> supplied vector subsystem -> engine search subsystem
```

Do not introduce a polished subsystem-profile abstraction. The target is to
delete upper-crate subsystem assembly, not to make it more comfortable.

For `EG4`, this is a hard acceptance criterion rather than a style preference:

- production executor/search/intelligence code must not construct
  `GraphSubsystem` after graph moves into engine
- product open must not receive graph as part of a caller-supplied subsystem
  list
- any remaining subsystem handoff after `EG4` must be vector-only and named as
  a deletion target for `EG5`
- tests may still use narrow runtime hooks while graph/vector/search absorption
  is in progress, but tests must not force product-open architecture to keep a
  broad subsystem-bundle API

## Shared Commands

Before code movement, capture the baseline:

```bash
cargo test -p strata-graph
cargo test -p strata-search
cargo test -p strata-executor
cargo test -p stratadb --test integration branching
cargo test -p stratadb --test integration branching_convergence_differential
cargo test -p stratadb --test integration recovery_cross_crate
cargo test -p stratadb --test executor graph
cargo test -p stratadb --test executor_ex6_runtime
cargo test -p strata-intelligence
cargo test -p stratadb --test storage_surface_imports
```

When the local inference vendor tree is available, also run:

```bash
cargo test -p strata-intelligence --features embed
```

During and after each implementation section, run the smallest relevant subset
plus formatting:

```bash
cargo fmt --check
cargo check -p strata-engine
cargo check -p strata-search
cargo check -p strata-executor
cargo test -p strata-engine --lib graph
cargo test -p strata-search
cargo test -p strata-executor
cargo test -p stratadb --test storage_surface_imports
```

Closeout commands:

```bash
rg -n "strata_graph|strata-graph" Cargo.toml crates src tests \
  -g '!target/**'

rg -n "strata_storage::|use strata_storage|strata-storage" \
  crates/{search,executor,intelligence,cli} \
  -g 'Cargo.toml' -g '*.rs'

test ! -e crates/graph || rg -n "strata_storage::|use strata_storage|strata-storage" \
  crates/graph -g 'Cargo.toml' -g '*.rs'

cargo metadata --format-version 1 --no-deps
cargo tree -p strata-engine --edges normal
cargo tree -p strata-search --edges normal
cargo tree -p strata-executor --edges normal
```

At closeout:

- `strata-graph` should be deleted, or exist only as an explicitly accepted
  zero-logic shell with no normal production dependents
- graph entries should be removed from the direct-storage bypass allowlist
- production `strata_graph` imports should be gone
- `crates/graph` should not exist unless the implementation review explicitly
  accepts a zero-logic shell and records its deletion criterion

## Section Status

| Section | Status | Intent |
| --- | --- | --- |
| `EG4A` | Completed | map graph API/storage/runtime behavior and pin tests |
| `EG4B` | Completed | move graph implementation into engine |
| `EG4C` | Completed | wire engine-owned graph runtime and product open composition |
| `EG4D` | Completed | settle graph transaction/key boundary inside engine |
| `EG4E` | Completed | cut search, executor, intelligence tests, and root tests over |
| `EG4F` | Completed | relocate graph tests and characterize integration behavior |
| `EG4G` | Completed | retire `strata-graph` and tighten guards |
| `EG4H` | Completed | full EG4 review, docs, and closeout verification |

## EG4A - Graph Code Map And Characterization

Goal:

Map the graph crate in enough detail that movement is mechanical and behavior
is pinned before files move.

Work:

- inventory every public item exported by `strata-graph`
- classify public items as root re-export, `graph::*` submodule export,
  engine-private, or test-only
- inventory every `strata_storage` touchpoint in `crates/graph`
- inventory every non-test and test consumer of `strata_graph`
- identify graph unit tests that must move to engine with the module
- identify root integration tests that already characterize graph branch,
  merge, recovery, search, and transaction behavior
- add missing characterization tests before movement if a behavior has no
  coverage

Coverage to confirm:

- graph CRUD and graph listing
- node/edge add, remove, neighbors, pagination, and as-of reads
- ontology define/delete/freeze/status behavior
- traversal and analytics output stability
- graph transaction staging, commit publish, abort cleanup
- follower refresh indexing graph nodes
- graph semantic merge and fatal conflict handling
- branch DAG bootstrap, branch status cache hydration, and deleted-branch
  cleanup
- graph search hit shape and deterministic ordering

Acceptance:

- public surface inventory is documented in the implementation notes or review
- storage touchpoint inventory matches `tests/storage_surface_imports.rs`
  allowlist
- current graph tests and key integration tests run before movement
- any newly discovered unpinned behavior is covered or explicitly deferred with
  rationale

EG4A inventory captured on 2026-05-05:

- root graph exports today are `GraphStore`, `PrimitiveGraphStore`,
  `GraphSubsystem`, `GraphBackendState`, `StagedGraphOp`, and branch DAG
  constants/types re-exported from engine
- public graph modules today are `adjacency`, `analytics`, `boost`,
  `branch_dag`, `branch_status_cache`, `dag_hook_impl`, `ext`, `integrity`,
  `keys`, `merge`, `merge_handler`, `ontology`, `packed`, `store`,
  `traversal`, and `types`
- private implementation modules today are `bulk`, `edges`, `lifecycle`,
  `nodes`, and `snapshot`
- root engine re-exports needed for the first movement are `GraphStore`,
  `PrimitiveGraphStore`, and `GraphSubsystem`
- `strata_engine::graph::*` should initially expose the current public graph
  submodules so consumer cutover can be mechanical; later v1 architecture work
  can tighten that surface
- `GraphBackendState`, `StagedGraphOp`, `BranchStatusCache`, graph key helpers,
  and graph merge internals should remain visible during EG4 only where current
  executor/tests require them; they are not evidence that graph internals should
  stay broad after engine-next

EG4A storage touchpoint inventory:

- `crates/graph/Cargo.toml` depends on `strata-storage`
- `crates/graph/src/keys.rs` owns graph `Key`, `Namespace`, and
  `TypeTag::Graph` construction/parsing
- `crates/graph/src/ext.rs` implements `GraphStoreExt` for storage
  `TransactionContext`
- `crates/graph/src/bulk.rs` builds batched graph `Key::new_graph` rows and
  carries a `Namespace` helper signature
- `crates/graph/src/edges.rs` uses `TransactionContext` for edge-type counter
  maintenance
- `crates/graph/src/store.rs` inspects `Key` / `TypeTag::Graph` for graph
  commit/abort/follower refresh indexing
- `crates/graph/src/merge.rs` decodes typed graph cells from storage `Key`
  values into graph merge state
- `crates/graph/src/merge_handler.rs` filters `TypeTag::Graph` and emits graph
  `MergeAction`s
- `crates/graph/src/lib.rs` uses storage `Key` in the `Searchable`
  implementation

At `EG4A`, these nine files matched the `EG4` entries in
`ALLOWED_ENGINE_CONSOLIDATION_STORAGE_USES`. `EG4B` removed those allowlist
entries once graph storage use moved under engine and `crates/graph` became a
zero-logic shell.

EG4A normal production manifest consumer inventory:

- `crates/search/Cargo.toml` and `crates/executor/Cargo.toml` still have normal
  `strata-graph` dependencies

EG4A dev/test manifest consumer inventory:

- root `Cargo.toml` still has a dev-dependency on `strata-graph` for root
  integration tests
- `crates/intelligence/Cargo.toml` still has a dev-dependency on
  `strata-graph` for embed-related tests

EG4A production Rust consumer inventory:

- `crates/executor/src/bridge.rs` constructs `PrimitiveGraphStore`
- `crates/executor/src/handlers/graph.rs` imports `GraphStoreExt` and graph
  DTOs for transactional graph commands
- `crates/executor/src/handlers/graph_impl.rs` imports graph DTOs/options for
  command output shaping
- `crates/executor/src/handlers/database.rs` refers to graph stats
- `crates/executor/src/compat.rs` supplies `GraphSubsystem` to product open
- `crates/search/src/substrate.rs` constructs `GraphStore` for BM25 fan-out

EG4A test consumer inventory:

- `tests/common/mod.rs` re-exports `GraphStore` and assembles graph/vector/search
  test runtimes
- `tests/common/branching.rs` reads branch DAG graph state through `GraphStore`
  and graph key helpers
- `tests/executor/*` uses `GraphSubsystem`, `GraphStore`, graph DTOs, and
  branch DAG helpers in command/session tests
- `tests/executor_ex6_runtime.rs` pins product-open runtime bridge behavior and
  graph/vector/search subsystem ordering while the bridge remains transitional
- `tests/integration/branching*.rs` uses graph DTOs, `GraphStore`,
  `GraphSubsystem`, `BranchStatusCache`, branch DAG helpers, graph neighbors,
  and graph key constants
- `tests/integration/recovery_cross_crate.rs` verifies graph space metadata and
  cross-crate recovery behavior
- `crates/search/src/substrate.rs` tests inspect graph key helpers
- `crates/intelligence/src/embed/runtime.rs` uses `GraphSubsystem` only in its
  test module
- `crates/intelligence/tests/expand_cache_fork_test.rs` imports
  `GraphSubsystem` and opens disk runtimes for embed cache fork behavior
- `crates/graph` unit tests cover graph-local key, format, algorithm, merge,
  branch DAG, transaction, search-refresh, and recovery behavior

EG4A characterization coverage:

- graph-local behavior is pinned by `cargo test -p strata-graph`:
  489 tests passed, including graph CRUD, key layout, namespace cache, packed
  adjacency encoding, node/edge integrity, ontology, traversal, analytics,
  snapshots, semantic merge, branch DAG, transaction abort cleanup, follower
  refresh, and graph search recovery
- search consumer behavior is pinned by `cargo test -p strata-search`:
  91 tests passed, including graph BM25 fan-out and temporal graph filtering
- executor consumer behavior is pinned by `cargo test -p strata-executor`:
  113 tests passed, including graph command helpers and typed facade behavior
- product-open bridge behavior is pinned by
  `cargo test -p stratadb --test executor_ex6_runtime`: 14 tests passed
- boundary guard behavior is pinned by
  `cargo test -p stratadb --test storage_surface_imports`: 6 tests passed with
  the current EG4 graph allowlist still active
- broad root branching behavior is pinned by
  `cargo test -p stratadb --test integration branching`: 245 tests passed, 1
  ignored, 56 filtered
- branch DAG/status-cache/search convergence is pinned by
  `cargo test -p stratadb --test integration branching_convergence_differential`:
  24 tests passed
- cross-crate graph recovery/space registration is pinned by
  `cargo test -p stratadb --test integration recovery_cross_crate`: 5 tests
  passed
- executor graph command behavior is pinned by
  `cargo test -p stratadb --test executor graph`: 14 tests passed, 203
  filtered
- intelligence default test harness is pinned by
  `cargo test -p strata-intelligence`: 0 tests ran because the graph-relevant
  intelligence tests are gated behind the `embed` feature
- intelligence embed-feature graph consumers are inventoried but not locally
  characterized in `EG4A`: `cargo test -p strata-intelligence --features embed`
  failed before test execution because `crates/inference/vendor/llama.cpp` does
  not contain `CMakeLists.txt` in this checkout

No new characterization test was added in `EG4A`. The current graph-local and
root integration coverage already pins the movement-critical behavior. The
known characterization gap is intelligence's embed-feature graph runtime tests;
run them once the inference vendor tree is present or keep the cutover limited
to import-path changes that compile without changing behavior. The next coverage
obligation is `EG4B`/`EG4F`: move the graph unit tests with the implementation
and keep the root integration tests importing graph through engine.

## EG4B - Engine Graph Module Move

Goal:

Move the graph implementation into engine without changing behavior.

Work:

- create `crates/engine/src/graph/mod.rs`
- move graph modules into `crates/engine/src/graph/`
- update internal imports from `crate::...` / `strata_engine::...` to the new
  engine-local module shape
- add engine re-exports for the graph surfaces needed by executor, search, and
  tests
- preserve module-level docs that explain branch DAG authority, graph merge,
  transaction staging, and follower refresh behavior
- avoid duplicate live implementation

Preferred movement rule:

- use `git mv` / direct file move semantics
- if `crates/graph` remains temporarily, make it a re-export shell only
- do not copy implementation into engine while leaving the old implementation
  live

Acceptance:

- `cargo check -p strata-engine` passes with graph implementation inside engine
- graph storage imports exist only under `crates/engine/src/graph` or other
  engine modules
- root graph re-exports compile from `strata_engine`
- no graph format/key/value behavior changes are introduced

EG4B implementation notes:

- moved the graph implementation from `crates/graph/src` into
  `crates/engine/src/graph`
- exposed the engine-owned module as `strata_engine::graph`
- re-exported `GraphStore`, `PrimitiveGraphStore`, and `GraphSubsystem` from
  the engine root for current in-repo consumers
- reduced `crates/graph` to a zero-logic shell that re-exports
  `strata_engine::graph::*` and has no storage dependency
- removed graph entries from the direct-storage bypass allowlist because the
  remaining storage imports now live under engine
- removed same-crate-unreachable wildcard arms in graph branch DAG matches that
  became invalid after the move; this was a warning cleanup only, not a
  behavior change

EG4B verification:

- `cargo fmt --check`: passed
- `cargo check -p strata-engine`: passed
- `cargo check -p strata-graph`: passed
- `cargo check -p strata-search`: passed
- `cargo check -p strata-executor`: passed
- `cargo test -p strata-engine --lib graph`: 492 passed, 0 failed
- `cargo test -p strata-graph`: 0 passed, 0 failed
- `cargo test -p stratadb --test storage_surface_imports`: 6 passed, 0 failed
- `rg -n "strata_storage::|use strata_storage|strata-storage" crates/graph`:
  no matches
- `rg -n "strata_engine::|strata_graph|crate::graph::graph"
  crates/engine/src/graph crates/graph/src`: only the intended
  `crates/graph/src/lib.rs` re-export remained

Implementation note: use `cargo test -p strata-engine --lib graph` for the
graph-local moved tests. `cargo test -p strata-engine graph` also passes the
graph unit tests, but then Cargo walks unrelated engine integration-test
binaries with the filter, which is slow and not useful for `EG4B`.

## EG4C - Runtime Wiring And Product Open Composition

Goal:

Make engine own graph runtime registration and remove graph from upper-crate
subsystem assembly.

Work:

- move `GraphSubsystem` into engine and implement `Subsystem` there
- register graph merge planning through engine's merge registry from the
  engine-owned subsystem
- install branch DAG hooks, branch op audit observer, branch status cache, and
  graph commit/abort/refresh hooks from engine-owned code
- ensure product primary/cache/follower open paths include graph internally
  where product runtime needs graph
- reduce executor's temporary subsystem helper from graph/vector/search to a
  vector-only bridge until `EG5`
- preserve graph -> vector -> search runtime order
- update product-open tests that assert subsystem ordering

Acceptance:

- no production crate above engine instantiates `GraphSubsystem`
- product open still boots graph behavior for primary/cache/follower handles
- graph merge handler registration still occurs before graph merges need it
- branch DAG hooks and audit observer still install exactly once per database
- vector remains the only product-open subsystem supplied by executor
- product open no longer exposes graph/search as caller-supplied subsystem
  composition from executor

EG4C implementation notes:

- product-open composition now wraps the temporary external subsystem bridge
  with engine-owned graph and search subsystems
- product primary, cache, and follower specs install runtime subsystems in the
  preserved order `graph -> external bridge -> search`
- product open rejects caller-supplied `graph`, `search`, arbitrary non-vector,
  and duplicate vector subsystem instances before runtime open, so the external
  bridge is explicitly vector-only while vector remains outside engine
- executor's product-open helper was reduced from graph/vector/search to a
  vector-only bridge named for deletion during `EG5`
- product-open tests now assert engine-owned graph/search installation and the
  `graph -> vector -> search` order when the vector bridge is supplied
- product-open tests now also assert graph runtime behavior by checking both
  DAG hook installation and graph merge registration on a product-open handle
- the follower no-default-branch test now opens its setup primary with the same
  engine-composed runtime subsystem list, avoiding a false runtime-signature
  mismatch while still omitting product default-branch bootstrap
- `crates/engine/src/database/spec.rs` no longer describes product open as a
  caller-built graph/vector/search bundle

EG4C verification:

- `cargo fmt --check`: passed
- `git diff --check`: passed
- `cargo check -p strata-engine`: passed
- `cargo check -p strata-executor`: passed
- `cargo test -p strata-engine --lib database::product_open`: 13 passed, 0
  failed
- `cargo test -p strata-executor`: 113 passed, 0 failed
- `cargo test -p stratadb --test executor_ex6_runtime`: 14 passed, 0 failed
- `cargo test -p stratadb --test storage_surface_imports`: 6 passed, 0
  failed
- `rg -n "legacy_product_subsystems_until_graph_vector_absorption|strata_graph::GraphSubsystem|Box::new\\(strata_graph::GraphSubsystem\\)|Box::new\\(strata_engine::SearchSubsystem\\)"
  crates/executor/src crates/search/src crates/intelligence/src -g '*.rs'`:
  one remaining match in a `#[cfg(test)]` intelligence module; no production
  executor/search/intelligence product-open graph/search composition remains

## EG4D - Transaction Extension And Storage-Key Boundary

Goal:

Ensure graph transaction and key mapping behavior is engine-owned and no longer
appears as an upper-crate storage bypass.

Work:

- move `GraphStoreExt` into `strata_engine::graph`
- keep the trait implementation for storage `TransactionContext` engine-local
- keep graph key construction helpers under engine-owned graph modules
- update executor transaction handlers to import `GraphStoreExt` and graph DTOs
  from engine
- ensure graph key helpers are not used from production crates except through
  engine graph APIs
- preserve space registration behavior for graph writes

Acceptance:

- executor graph transaction commands behave unchanged
- graph transaction write commands still stage search-index operations
- graph abort/failed-commit paths clear staged work
- production `crates/graph` storage bypass entries are gone or shell-only
- storage-surface guard remains green with no graph allowlist entries

EG4D implementation notes:

- executor graph command handlers now import `GraphStoreExt`, graph DTOs, and
  graph stats from the root `strata_engine::graph` API instead of the
  `strata_graph` re-export shell
- executor primitive wiring now constructs `PrimitiveGraphStore` from
  `strata_engine`, so executor graph transactions consume the engine-owned
  graph facade directly
- `GraphStoreExt` and its `TransactionContext` implementation remain
  engine-local; the executor sees only the trait surface and never imports
  storage key helpers
- graph key helpers remain under `strata_engine::graph::keys`; the only
  remaining upper-crate graph-key helper use is a search test fixture slated
  for the broader consumer cutover in `EG4E`

EG4D verification:

- `cargo fmt --check`: passed
- `cargo check -p strata-executor`: passed
- `rg -n "strata_graph|strata-graph" crates/executor -g 'Cargo.toml' -g '*.rs'`:
  0 matches
- `cargo tree -p strata-executor --edges normal` still reaches
  `strata-graph` transitively through `strata-search`; removing that edge is
  `EG4E` consumer-cutover scope
- `cargo test -p strata-engine --lib graph::store::tests`: 7 passed, 0 failed
- `cargo test -p strata-engine --lib graph::bulk::tests`: 38 passed, 0 failed
- `cargo test -p strata-executor`: 113 passed, 0 failed
- `cargo test -p stratadb --test storage_surface_imports`: 6 passed, 0
  failed
- `rg -n "strata_graph" crates/executor/src -g '*.rs'`: 0 matches
- `rg -n "strata_graph::keys|strata_engine::graph::keys|graph::keys|keys::(node_key|edge_key|graph_key|storage_key|GRAPH_SPACE)"
  crates/executor/src crates/search/src crates/intelligence/src -g '*.rs'`:
  one remaining match in a search test fixture covered by `EG4E`; no executor
  graph-key helper usage remains

## EG4E - Consumer Cutover

Goal:

Remove normal production `strata-graph` use from crates above engine, and remove
test/dev graph crate use once test imports are cut over.

Work:

- update executor `bridge`, graph handlers, database handlers, session tests,
  and product-open helpers to use engine graph imports
- update search substrate to construct `strata_engine::GraphStore`
- update search tests that inspect graph keys/types to import from engine
- update intelligence tests to import engine graph subsystem or use the new
  engine product runtime helper
- update root integration and executor tests to use engine graph imports
- remove normal `strata-graph` dependencies from `strata-search` and
  `strata-executor` once imports are gone
- remove dev/test `strata-graph` dependencies from root `Cargo.toml`,
  `strata-intelligence`, and any other test manifests once test imports are gone

Acceptance:

- `rg -n "strata_graph|strata-graph" crates tests Cargo.toml` shows only
  accepted temporary shell references, if any
- `cargo check -p strata-search` passes without `strata-graph`
- `cargo check -p strata-executor` passes without `strata-graph`
- graph command tests and search substrate graph tests pass

EG4E implementation notes:

- `strata-search` now imports `GraphStore` from `strata-engine`; graph key and
  type references in search tests use `strata_engine::graph`
- executor, intelligence, and root integration tests now import graph types,
  keys, branch DAG helpers, `GraphStore`, and `GraphSubsystem` from
  `strata-engine`
- normal `strata-graph` manifest dependencies were removed from
  `strata-search`; the executor normal dependency was already removed in
  `EG4D`
- dev/test `strata-graph` manifest dependencies were removed from root
  `Cargo.toml` and `strata-intelligence`
- the only remaining source/manifest `strata-graph` match is the transitional
  graph shell package name in `crates/graph/Cargo.toml`; `Cargo.lock` also
  keeps the expected package entry while the shell remains in the workspace
- retiring the graph shell and removing the lockfile package entry is `EG4G`
  scope

EG4E verification:

- `cargo check -p strata-search`: passed
- `cargo check -p strata-executor`: passed
- `cargo check -p strata-intelligence`: passed
- `cargo test -p strata-search graph`: 2 passed
- `cargo test -p strata-executor`: 113 passed
- `cargo test -p stratadb --test executor graph`: 14 passed
- `cargo test -p stratadb --test executor_ex6_runtime`: 14 passed
- `cargo test -p stratadb --test integration branching::`: 95 passed
- `cargo test -p stratadb --test integration branching_adversarial_history`:
  4 passed
- `cargo test -p stratadb --test integration recovery_cross_crate`: 5 passed
- `cargo test -p stratadb --test integration branching_convergence_differential`:
  24 passed
- `cargo test -p strata-intelligence`: 0 passed, 0 failed
- `cargo check -p strata-intelligence --features embed --tests`: blocked by
  the pre-existing missing inference vendor tree
  (`crates/inference/vendor/llama.cpp/CMakeLists.txt`); rerun this before
  closing `EG4` once the vendor tree is available
- `cargo tree -p strata-search --edges normal`: no `strata-graph` dependency
- `cargo tree -p strata-executor --edges normal`: no `strata-graph` dependency

## EG4F - Tests And Integration Relocation

Goal:

Move graph tests to the engine-owned implementation and keep cross-crate
behavior characterization strong.

Work:

- move `crates/graph` unit tests with the implementation into engine modules or
  engine integration tests
- keep tests close to the behavior they verify when possible
- update root integration helpers in `tests/common` to import graph from engine
- ensure branch DAG tests still inspect canonical `_branch_dag` state through
  engine graph APIs
- ensure recovery tests still prove graph rows survive checkpoint/snapshot/WAL
  paths
- ensure search tests still prove graph nodes participate in BM25 retrieval
- preserve tests that prove graph lower-level APIs can access reserved graph
  spaces where branch DAG and recovery require it

Acceptance:

- graph unit tests run under `strata-engine`
- root integration graph/branch/recovery tests pass
- executor graph command tests pass
- search graph fan-out tests pass
- no test uses a deleted `strata_graph` crate unless an accepted temporary
  shell exists

EG4F implementation notes:

- graph-local unit tests now live with the engine-owned graph implementation in
  `crates/engine/src/graph`
- `crates/graph` has no local tests and remains only as the explicitly
  accepted zero-logic re-export shell until `EG4G`
- root integration helpers, executor command tests, search graph tests, and
  intelligence tests were already cut over to `strata_engine::graph` in `EG4E`
- branch DAG characterization still inspects canonical `_branch_dag` state
  through engine graph APIs
- graph recovery characterization is split across specific existing tests:
  `branching::dag_survives_database_reopen` and
  `branching::cross_primitive_merge_rollback_via_reopen` cover WAL/reopen
  graph survival, `test_checkpoint_data_preserves_graph_type_and_space_metadata`
  covers checkpoint graph type/space collection, and
  `install_routes_graph_tag_into_graph_storage` covers snapshot install routing
  for graph-tagged KV-section rows
- reserved graph-space behavior remains covered by
  `branching::graph_store_accepts_reserved_system_dag_space`
- deleting the shell crate, removing the workspace package, and adding the
  retired-graph guard remain `EG4G` scope

EG4F verification:

- `cargo test -p strata-engine --lib graph`: 493 passed, 0 failed
- `cargo test -p strata-graph`: 0 passed, 0 failed
- `cargo test -p strata-search graph`: 2 passed, 0 failed
- `cargo test -p stratadb --test executor graph`: 14 passed, 0 failed
- `cargo test -p stratadb --test integration branching::`: 95 passed, 0 failed
- `cargo test -p stratadb --test integration recovery_cross_crate`: 5 passed,
  0 failed
- `cargo test -p stratadb --test integration branching_convergence_differential`:
  24 passed, 0 failed
- `cargo test -p strata-engine --lib test_checkpoint_data_preserves_graph_type_and_space_metadata`:
  1 passed, 0 failed
- `cargo test -p strata-engine --lib install_routes_graph_tag_into_graph_storage`:
  1 passed, 0 failed
- `rg -n "strata_graph|strata-graph" crates/engine crates/graph crates/search crates/executor crates/intelligence tests Cargo.toml -g 'Cargo.toml' -g '*.rs'`:
  only the transitional package name in `crates/graph/Cargo.toml`

## EG4G - Retire The Graph Crate And Tighten Guards

Goal:

Remove the graph crate as a normal package and make the new boundary
executable.

Work:

- delete `crates/graph` once all imports are cut over
- remove `strata-graph` from root workspace membership and all manifests
- verify graph entries remain absent from `ALLOWED_ENGINE_CONSOLIDATION_STORAGE_USES`
- add a retired-graph guard similar to the security and executor-legacy guards
- update `docs/engine/engine-crate-map.md`
- update `docs/storage/v1-storage-consumption-contract.md` if the direct
  storage bypass inventory changes
- update `docs/storage/storage-crate-map.md` if it still names graph as a
  storage consumer

Acceptance:

- `cargo metadata` has no `strata-graph` package
- `cargo tree -p strata-search --edges normal` has no `strata-graph`
- `cargo tree -p strata-executor --edges normal` has no `strata-graph`
- `test ! -e crates/graph` passes, unless the implementation review accepted a
  zero-logic shell with an immediate deletion criterion
- storage-surface guard passes with graph removed from the allowlist
- retired-graph guard fails if `strata_graph`, `strata-graph`, or
  `crates/graph` references return in production manifests/source

EG4G implementation notes:

- deleted the temporary `crates/graph` re-export shell
- removed `crates/graph` from root workspace membership
- removed `crates/graph` from the direct-storage-bypass scan roots
- added a retired-graph guard that rejects production source/manifest
  references to `strata_graph`, `strata-graph`, `../graph`, or `crates/graph`
- refreshed the engine and storage crate maps to show graph as engine-owned
- refreshed the storage consumption contract so graph is no longer listed as a
  remaining transitional direct storage importer

EG4G verification:

- `test ! -e crates/graph`: passed
- `cargo metadata --format-version 1 --no-deps`: no `strata-graph` package
- `cargo tree -p strata-search --edges normal`: no `strata-graph` dependency
- `cargo tree -p strata-executor --edges normal`: no `strata-graph` dependency
- `cargo test -p stratadb --test storage_surface_imports`: guard suite passed
- `rg -n "strata_graph|strata-graph|crates/graph" crates tests Cargo.toml Cargo.lock -g 'Cargo.toml' -g '*.rs' -g 'Cargo.lock'`:
  no matches

## EG4H - Closeout Review

Goal:

Review all of `EG4` as one ownership move before starting vector absorption.

Work:

- review every remaining `strata_graph` / `strata-graph` match
- review every remaining graph-related `strata_storage` match outside engine
- review product-open composition to ensure graph is engine-owned and vector is
  the only remaining bridge
- review graph branch DAG, merge, search, transaction, and recovery behavior
  against the current behavior contract
- review docs for stale descriptions of graph as a peer crate
- run closeout command set

Acceptance:

- graph implementation is engine-owned
- graph storage/key/transaction behavior exists only through engine
- no production upper crate constructs graph subsystem instances
- search and executor consume graph from engine
- graph crate is retired or shell-only with explicit deletion criterion
- EG5 can begin with vector as the next remaining peer primitive crate

EG4H implementation notes:

- reviewed remaining `strata_graph`, `strata-graph`, `../graph`, and
  `crates/graph` matches; production source/manifests are clean, while
  remaining documentation matches are historical phase notes or explicit
  completed-state records
- reviewed graph-related `strata_storage` usage; graph storage/key/transaction
  code now lives under `crates/engine/src/graph`, and the only remaining
  production upper-crate storage bypasses are the documented `EG5`/`EG6`/`EG7`
  vector/search/executor seams
- reviewed product-open composition; engine now installs graph and search around
  the temporary external vector bridge, and executor supplies only
  `VectorSubsystem`
- added a closeout guard proving production upper crates do not name
  `GraphSubsystem`; `#[cfg(test)]` graph runtime assembly remains allowed for
  characterization tests
- refreshed current-state consolidation docs so graph is no longer described as
  a current peer crate, current storage bypass, or later deletion target

EG4H verification:

- `cargo test -p stratadb --test storage_surface_imports`: guard suite passed
- `cargo check -p strata-engine`: passed
- `cargo check -p strata-search`: passed
- `cargo check -p strata-executor`: passed
- `test ! -e crates/graph`: passed
- `cargo metadata --format-version 1 --no-deps | rg "strata-graph|crates/graph"`:
  no matches
- `cargo tree -p strata-search --edges normal | rg "strata-graph|strata_graph"`:
  no matches
- `cargo tree -p strata-executor --edges normal | rg "strata-graph|strata_graph"`:
  no matches
- `rg -n "strata_graph|strata-graph|crates/graph|\\.\\./graph" Cargo.toml crates src tests -g 'Cargo.toml' -g '*.rs'`:
  no matches
- `rg -n "GraphSubsystem" crates/{vector,search,executor,intelligence,cli}/src src -g '*.rs'`:
  only `#[cfg(test)]` characterization usage remains outside engine
- `rg -n "strata_storage::|use strata_storage|strata-storage" crates/{search,executor,intelligence,cli} -g 'Cargo.toml' -g '*.rs'`:
  remaining matches are the documented `EG6`/`EG7` search/executor seams
- `cargo fmt --check`: passed
- `git diff --check`: passed
- `git diff --cached --check`: passed

## Risks

Graph absorption has a few high-risk edges:

- Branch DAG behavior is broad. It touches branch lifecycle, audit events,
  graph storage, status cache, and system branch bootstrap.
- Graph search indexing is staged-publish behavior. Commit, abort, failed
  commit, and follower refresh paths must all keep their current ordering.
- Product open still has vector outside engine. The temporary bridge should
  become smaller, not more general.
- Search remains a peer crate until `EG6`. Search may import engine graph
  surfaces during `EG4`, but search should not become the owner of graph
  behavior.
- Integration tests use graph internals heavily. That is useful for coverage,
  but imports must be updated so tests do not keep the retired crate alive.

## Closeout Definition

`EG4` is complete when:

- `strata-engine` owns graph runtime, graph key mapping, graph transaction
  extension behavior, graph merge, graph search integration, and graph branch
  DAG behavior
- `strata-graph` is deleted, or any explicitly accepted same-phase shell is
  zero-logic, has no storage imports, and is not a normal dependency of
  production crates
- no production crate above engine imports graph through anything other than
  `strata_engine`
- no production crate above engine imports storage to perform graph behavior
- product open no longer receives graph subsystem instances from executor
- the storage-surface guard has no graph allowlist entries
- the engine crate map and storage consumption contract reflect the new graph
  ownership
