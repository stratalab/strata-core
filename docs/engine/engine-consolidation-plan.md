# Engine Consolidation Plan

## Purpose

This document defines the next architecture cleanup after the storage/engine
boundary normalization.

The storage cleanup made the lower substrate healthier: storage now owns
generic persistence mechanics, transaction/runtime mechanics, WAL, snapshot,
manifest, checkpoint, recovery bootstrap, and storage-only runtime config
application. Engine now sits on top as the database semantics and orchestration
layer.

The next problem is above that boundary. Several crates are already engine
concepts in practice, but they still exist as peer crates and several of them
reach directly into storage:

- `strata-graph`
- `strata-vector`
- `strata-search`
- `strata-executor-legacy`

`EG2` has already absorbed and deleted `strata-security`; it remains in this
plan only as a completed phase record.

This plan consolidates those responsibilities into `strata-engine` so the
workspace can settle into the intended stack:

```text
core -> storage -> engine -> intelligence -> executor -> cli
```

The goal is not to make engine-next yet. The goal is to make the current engine
crate the only owner of database semantics, primitive runtimes, product open
policy, and storage consumption. This consolidation makes the codebase honest
enough to design from; it does not authorize an immediate storage-next or
engine-next implementation.

This plan should be read with:

- [engine-crate-map.md](./engine-crate-map.md)
- [engine-storage-boundary-normalization-plan.md](./archive/engine-storage-boundary-normalization-plan.md)
- [boundary-closeout-plan.md](./archive/boundary-closeout-plan.md)
- [engine-pending-items.md](./archive/engine-pending-items.md)
- [../storage/storage-engine-ownership-audit.md](../storage/storage-engine-ownership-audit.md)
- [../storage/storage-minimal-surface-implementation-plan.md](../storage/storage-minimal-surface-implementation-plan.md)
- [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)

## Current Verified Starting Point

The verified normal workspace dependency graph for the engine-adjacent crates
is:

```text
strata-storage         -> strata-core
strata-engine          -> strata-core, strata-storage
strata-graph           -> strata-core, strata-engine, strata-storage
strata-vector          -> strata-core, strata-engine, strata-storage
strata-search          -> strata-core, strata-engine, strata-graph,
                          strata-storage, strata-vector
strata-intelligence    -> strata-core, strata-engine, strata-inference,
                          strata-search, strata-vector
strata-executor        -> strata-core, strata-engine, strata-executor-legacy,
                          strata-graph, strata-intelligence, strata-search,
                          strata-storage, strata-vector
strata-executor-legacy -> strata-core, strata-engine, strata-graph,
                          strata-vector
strata-cli             -> strata-executor, strata-intelligence
stratadb               -> strata-executor
```

The direct storage bypasses above engine are:

- `strata-graph`
  - storage keys, namespaces, type tags, and transaction contexts
- `strata-vector`
  - storage keys, namespaces, type tags, storage scans, transaction contexts,
    and storage trait calls
- `strata-search`
  - storage keys and namespaces in retrieval substrate code
- `strata-executor`
  - storage keys, namespaces, type tags, validation helpers, and storage errors

`strata-executor-legacy` and `strata-intelligence` do not currently have direct
normal storage imports, but they depend on crates that do.

## Direct Storage Rule

No production crate above engine may access `strata-storage` directly.

After this consolidation, the only normal production crate allowed to depend on
`strata-storage` is `strata-engine`. Upper crates must consume storage-backed
behavior through engine-owned APIs.

Allowed exceptions must be explicit and documented in the file that creates the
edge:

- `strata-storage` itself
- `strata-engine`
- storage-specific tests, benches, fuzz targets, and format/recovery tools
- future `storage-next` migration or validation tools whose purpose is to test
  or build the storage substrate
- temporary migration shims named in this plan and deleted by closeout

Executor, intelligence, CLI, graph, vector, search, and product code are not
storage tests. If they need a storage fact, engine must expose a semantic API or
a narrow runtime API for that fact.

This rule is the main acceptance condition for the plan.

## Compatibility Posture

Strata is still pre-v1. This plan does not need to preserve old crate import
paths for hypothetical external users.

That means:

- deleting `strata-graph`, `strata-vector`, `strata-search`, and
  `strata-executor-legacy` is allowed
- `strata-security` has already been absorbed and deleted by `EG2`
- long-lived re-export crates are not required for compatibility
- API compatibility is valuable only where it protects current in-repo callers,
  current CLI/executor behavior, or test characterization
- on-disk format and recovery behavior should still be preserved unless a later
  explicit migration says otherwise

This is why the plan favors physical absorption and crate deletion over facade
layers.

## Rewrite Rules

### 1. Consolidate Ownership, Not Just Imports

The work is not done when `strata-engine` re-exports types from graph, vector,
search, or executor-legacy. The work is done when the code is
physically owned by engine or an explicitly documented transitional shim can be
deleted.

### 2. Preserve The Storage Boundary

Moving graph, vector, search, and product open policy into engine must not push
their semantics down into storage.

Storage remains responsible for:

- bytes and physical keyspace
- generic transactions
- WAL, snapshot, checkpoint, manifest, compaction, and recovery mechanics
- storage-local runtime config and storage-local errors

Engine is responsible for:

- branch/domain semantics
- primitive semantics
- graph/vector/search behavior
- product open/bootstrap policy
- security/open option types
- conversion from storage facts into public engine behavior

### 3. No New Crate Graph Loops

The target graph is linear. Do not introduce temporary edges that make the
architecture harder to reason about:

- no `storage -> engine`
- no `engine -> executor`
- no `engine -> intelligence`
- no `search -> vector -> engine` style cycle through a transitional module
- no product/bootstrap logic in storage

### 4. Characterize Before Moving

Graph, vector, search, and product open behavior are high-risk runtime surfaces.
Each epic or implementation slice starts with characterization tests or explicit
evidence that existing tests already pin the behavior being moved.

The migration should prefer:

- move module, preserve behavior, run tests
- update imports to the new engine module
- delete the old crate edge
- add an import/dependency guard

It should not mix large behavior redesigns into ownership moves.

### 5. Temporary Shims Must Have Deletion Criteria

Short-lived compatibility modules are acceptable when they reduce risk, but they
must state:

- what old path they preserve
- which downstream callers still need it
- which `EG` slice deletes it
- which guard fails if it becomes permanent by accident

Do not introduce broad facades that hide storage access. That recreates the
problem in a new location.

### 6. Engine Must Expose The Right Storage-Backed Endpoints

Upper crates should not need storage keys, namespaces, type tags, or raw storage
transactions for product behavior.

If executor or intelligence needs behavior that currently requires direct
storage access, add an engine-owned endpoint with domain language. Examples:

- delete a space and all primitive/runtime side effects
- validate or normalize a space name
- list or inspect graph/vector/search state
- perform search against graph/vector/text substrates
- open product runtime with default subsystems

The endpoint should expose semantic outcomes, not raw storage mechanics.

### 7. Retire Public Subsystem Instantiation

The current pattern where callers instantiate subsystem structs and pass them
into `OpenSpec` is a migration artifact, not target architecture.

The lifecycle phases are useful:

- recover
- initialize
- bootstrap
- freeze

The public subsystem-instantiation model is not. Product callers should not
know that graph, vector, search, and primitive runtimes are assembled from
separate subsystem objects. Engine should own runtime module registration and
product-open composition internally.

During `EG3`, graph and vector are still peer crates that depend on engine, so a
temporary subsystem handoff may be needed to remove `strata-executor-legacy`
without creating a crate cycle. That handoff must be named as transitional and
deleted after graph/vector absorption. Do not introduce a polished
`ProductSubsystemProfile` abstraction that survives closeout.

## End-State Definition

The consolidation is complete when:

1. `strata-engine` owns graph runtime and graph subsystem code.
2. `strata-engine` owns vector runtime, vector subsystem code, and vector
   sidecar cache policy.
3. `strata-engine` owns search substrate, fusing, query expansion/rerank
   orchestration interfaces, and search subsystem behavior.
4. `strata-engine` owns access-mode/open-option/security types. This is already
   true after `EG2`.
5. `strata-engine` owns the product open/bootstrap policy currently hosted in
   `strata-executor-legacy`.
6. `strata-executor` no longer depends on `strata-storage`, `strata-graph`,
   `strata-vector`, `strata-search`, `strata-security`, or
   `strata-executor-legacy`.
7. `strata-intelligence` depends on `strata-engine` and `strata-inference`, not
   `strata-search` or `strata-vector`.
8. The only normal production path into storage is `strata-engine`.
9. The crate graph is enforceable with tests and `cargo metadata` guards.
10. No production crate above engine instantiates engine subsystem structs or
    supplies subsystem bundles to product open.

The intended normal graph after this plan is:

```text
strata-core
  -> strata-storage
      -> strata-engine
          -> strata-intelligence
              -> strata-executor
                  -> strata-cli
                  -> stratadb
```

`strata-inference` remains below or beside `strata-intelligence` as the model
provider/inference crate. Engine must not depend on it.

The CLI should also stop depending on intelligence directly by closeout. It is
not a storage bypass today, but the target stack is easier to enforce if CLI
commands enter through executor rather than selectively bypassing it.

## Non-Goals

This plan does not:

- create `storage-next`
- create `engine-next`
- change WAL, manifest, checkpoint, snapshot, or segment formats
- redesign vector indexing algorithms
- redesign graph semantics
- redesign search ranking semantics
- redesign the public executor API
- remove intelligence or inference
- make storage support OpenDAL
- build StrataHub

Those are future workstreams. This plan prepares the graph so those workstreams
can happen without upper crates bypassing engine or engine relying on peer
crates for its own primitives.

## Post-Consolidation Architecture Gate

When engine consolidation is complete, the next step is a design phase, not an
implementation phase.

The order is:

1. Collapse engine-owned behavior into engine and remove storage bypasses above
   engine.
2. Write the correct Strata v1 architecture in documents.
3. Review the target architecture, testing strategy, and product/developer
   experience until the design is coherent.
4. Build only after the architecture is clear enough to survive implementation
   pressure.

The v1 architecture design phase should produce at least:

- Strata v1 target architecture
- storage-next target architecture
- engine-next target architecture
- storage/engine consumption contracts
- testing strategy for unit, integration, fuzz, fault-injection,
  crash-recovery, and deterministic replay coverage
- product and developer experience for storage and engine
- OpenDAL integration architecture
- StrataHub architecture
- migration and implementation roadmap

This matters because the current work is the classic foundational sequence:
make it exist, make ownership true, then make it excellent. Consolidation puts
the mess in the correct crate so it can be studied honestly. The following
architecture phase decides the shape of v1 before a rebuild begins.

## Epic Structure

This plan is organized as nine engine consolidation epics. The list below
is the intended execution order.

- `EG1` - Baseline, Guards, And API Inventory
- `EG2` - Security Absorption
- `EG3` - Product Open And Bootstrap Absorption
- `EG4` - Graph Absorption
- `EG5` - Vector Absorption
- `EG6` - Search Absorption
- `EG7` - Executor Storage Bypass Removal
- `EG8` - Intelligence Dependency Cleanup
- `EG9` - Crate Deletion And Workspace Closeout

Each epic gets one phase implementation plan document. The lettered sections
for that epic, such as `EG1A`, `EG1B`, `EG1C`, then `EG2A`, `EG2B`, and so on,
are tracked inside that phase document. Do not create separate
`EG1A`/`EG1B`-style plan files, and do not use nested `A.1` / `A.2` headings.
If a section needs more detail, add it under that lettered section in the
phase document.

## EG1 - Baseline, Guards, And API Inventory

**Goal:**

Freeze the current graph and make the storage-access rule executable before code
starts moving.

**Scope:**

- finalize the current engine crate map
- record direct storage bypasses by crate and file
- inventory the engine APIs upper crates should use instead
- identify missing engine endpoints that force direct storage access today
- add a guard test in an allowlisted form so it can be tightened epic by epic

**Required inventory:**

The current bypass inventory should include at least:

- graph storage key construction and transaction extension code
- vector storage scans, key construction, recovery, merge handling, and
  transaction extension code
- search substrate use of storage key/namespace construction
- executor direct storage use in:
  - `handlers/space_delete.rs`
  - `handlers/kv.rs`
  - `handlers/json.rs`
  - `handlers/event.rs`
  - `session.rs`
  - `convert.rs`
  - public re-exports in `lib.rs`
  - `validate_space_name` calls in `bridge.rs` and `compat.rs`

**Guard shape:**

Add or prepare a test similar to the existing surface import guards. It should
scan manifests and Rust source for direct `strata-storage` usage outside the
current allowlist.

Early in the plan the allowlist will include the transitional crates. Each
epic removes entries from that allowlist.

The final allowlist should be:

- `crates/storage/**`
- `crates/engine/**`
- storage-specific tests/benches/fuzz targets/tools
- docs

**Acceptance:**

- current dependency graph is documented
- current direct storage bypasses are documented
- guard test exists or the exact guard test design is documented
- every later epic knows which allowlist entries it must remove

**Non-goals:**

- no module moves yet
- no public API changes yet

Implementation tracking:
[eg1-implementation-plan.md](./eg1-implementation-plan.md).

### EG1A - Dependency Graph Baseline

Refresh the normal dependency graph for all engine-adjacent crates and record it
in [engine-crate-map.md](./engine-crate-map.md). Use `cargo metadata` or
`cargo tree`, not hand-maintained assumptions.

Current status: complete as of 2026-05-04.

### EG1B - Direct Storage Bypass Inventory

Record every direct normal production `strata-storage` use above engine by crate
and ownership reason. Each entry should say whether it is expected to disappear
in `EG4`, `EG5`, `EG6`, or `EG7`.

Current status: complete as of 2026-05-04.

### EG1C - Engine Endpoint Gap Ledger

List the engine APIs missing today that force upper crates to use storage
directly. This is the work queue for `EG7`, and it should distinguish semantic
APIs from narrow runtime APIs.

Current status: complete as of 2026-05-04.

### EG1D - Transitional Guard Setup

Add or reserve a dependency/import guard with an explicit allowlist. The guard
should be loose enough to pass before movement starts and precise enough to
tighten after each later epic.

Current status: complete as of 2026-05-04.

## EG2 - Security Absorption

**Goal:**

Move the small security/open-options surface into engine so product open policy
does not depend on a separate security crate.

Detailed implementation plan:
[eg2-implementation-plan.md](./eg2-implementation-plan.md).

**Scope:**

Move into engine:

- `AccessMode`
- `OpenOptions`
- `SensitiveString`

Update callers to import these from `strata_engine`.

Remove normal dependencies on `strata-security` from:

- `strata-engine`
- `strata-executor`
- `strata-executor-legacy`

**Design notes:**

This epic should land early because it is small and unlocks product open
absorption without carrying an extra peer crate into engine.

`OpenOptions` is product/database open configuration, not a storage concept.
It belongs beside `OpenSpec`, `DatabaseMode`, and product open helpers.

**Acceptance:**

- `strata-engine` no longer depends on `strata-security`
- executor imports open/access/security types from engine
- executor-legacy imports open/access/security types from engine if it still
  exists at this point
- `strata-security` has been deleted from the workspace

**Non-goals:**

- do not redesign authorization
- do not add user/role/permission systems

### EG2A - Security Surface Characterization

Pin the current behavior and public shape of `AccessMode`, `OpenOptions`, and
`SensitiveString`, including formatting/redaction expectations for
secret-bearing values.

### EG2B - Move Open And Security Types Into Engine

Physically move or recreate the small security/open surface in engine. Engine
should own the types directly, not import them from a permanent peer crate.

### EG2C - Caller Cutover

Update executor and executor-legacy callers to import from engine, then remove
normal `strata-security` dependencies.

### EG2D - Retire Security Crate And Guard

Delete the old security crate from the workspace and add guard coverage so
production manifests and Rust source cannot reintroduce the retired crate edge.

Current status: complete.

## EG3 - Product Open And Bootstrap Absorption

**Goal:**

Move the product open/bootstrap shell out of `strata-executor-legacy` and into
engine.

Detailed implementation plan:
[eg3-implementation-plan.md](./eg3-implementation-plan.md).

**Scope:**

Move or recreate in engine:

- product primary open
- product follower open
- product cache open
- default subsystem profile contract
- default branch bootstrap
- built-in recipe seeding
- lock-to-IPC fallback classification currently used by the legacy bootstrap
  shell

Executor should call engine-owned product open APIs directly.

**Current code to absorb:**

The relevant starting point is `crates/executor-legacy/src/bootstrap.rs`:

- `Strata::open`
- `Strata::open_with`
- `Strata::cache`
- `default_product_spec`
- `default_product_follower_spec`
- `default_product_cache_spec`
- `seed_builtin_recipes`
- `ensure_default_branch`

`OpenSpec` already lives in engine, but its docs currently describe product
defaults as executor-owned. That documentation must change when this epic
lands.

**Design notes:**

The engine should expose a product runtime entry point that executor can use
without knowing how subsystems are composed.

The product open API should return an engine-owned result shape. If IPC fallback
remains a product behavior, it should be represented explicitly instead of
forcing executor to string-match engine errors.

Because graph and vector still depend on engine before `EG4` and `EG5`, `EG3`
must not make engine depend on `strata-graph` or `strata-vector`. The detailed
plan allows a narrow temporary subsystem-instantiation bridge so executor can
supply the current graph/vector/search subsystem set without keeping
`strata-executor-legacy` alive. That bridge is explicitly bad architecture debt
and is deleted after graph and vector are absorbed.

**Acceptance:**

- executor no longer depends on `strata-executor-legacy`
- product subsystem assembly is represented by an engine-owned open contract,
  with any executor-supplied graph/vector instances marked as a temporary bridge
  only until `EG4` and `EG5`
- default branch and built-in recipe bootstrap policy are engine-owned
- `OpenSpec` docs no longer claim product defaults are executor-owned
- existing executor open/cache/follower behavior is characterized and preserved

**Non-goals:**

- do not redesign IPC
- do not remove CLI behavior
- do not redesign config persistence

### EG3A - Product Open Characterization

Characterize primary, follower, cache, IPC-fallback, default-subsystem, default
branch, and built-in recipe behavior before moving product bootstrap code.

### EG3B - Engine Product Open API

Add the engine-owned product open entry points and result shape. The API should
let executor ask for product behavior without knowing subsystem assembly
details.

### EG3C - Bootstrap Policy Move

Move the product open contract, default branch bootstrap, and built-in recipe
seeding into engine. Update `OpenSpec` docs to describe engine ownership and the
temporary graph/vector subsystem-instantiation bridge.

### EG3D - Executor Cutover And Legacy Edge Removal

Route executor open/cache/follower paths through the engine API, remove the
normal `strata-executor-legacy` dependency, and either delete the crate or leave
a documented `EG9` compatibility shell.

## EG4 - Graph Absorption

**Goal:**

Move graph runtime, graph subsystem, graph merge behavior, and graph storage-key
mapping into engine.

**Scope:**

Move the `strata-graph` implementation into an engine module, likely under one
of:

- `strata_engine::graph`
- `strata_engine::primitives::graph`

The exact module path should follow the surrounding engine layout after a local
file map review.

Move or preserve:

- `GraphStore`
- `PrimitiveGraphStore`
- graph key construction
- graph transaction extension behavior
- graph merge handlers
- graph branch DAG subsystem behavior
- graph sidecar/bootstrap behavior
- graph search integration

**Boundary requirement:**

Graph may use storage keys, namespaces, type tags, and transaction contexts only
after it is physically inside engine.

No crate above engine should import storage types to perform graph behavior.

**Design notes:**

Graph is tightly coupled to engine:

- it implements engine `Subsystem`
- it uses `Database`
- it participates in search
- it participates in merge/recovery/branch behavior
- it maps graph semantics onto storage key families

Those are engine responsibilities. The storage-specific key mapping is a private
engine implementation detail once graph moves into engine.

**Acceptance:**

- `strata-graph` has no normal storage dependency, or the crate is deleted
- executor imports graph surfaces from engine
- search imports graph surfaces from engine until search is absorbed
- graph tests either move with the module or become engine tests
- no graph production code outside engine imports `strata_storage`

**Non-goals:**

- do not redesign graph storage format
- do not redesign graph query semantics
- do not move graph semantics into storage

### EG4A - Graph Code Map And Characterization

Map graph modules, storage touchpoints, subsystem registration, merge behavior,
branch DAG behavior, and search integration. Add or identify tests that pin
those behaviors before movement.

### EG4B - Move Graph Runtime Into Engine

Physically move graph runtime and storage-key mapping into an engine-owned
module. Preserve format, key families, and transaction behavior.

### EG4C - Cut Over Graph Consumers

Update search and executor imports to engine-owned graph surfaces. Remove graph
storage access outside engine as soon as the code physically moves.

### EG4D - Retire The Graph Crate

Delete `strata-graph` when no normal dependents remain, or reduce it to a
documented temporary shell with an `EG9` deletion criterion.

## EG5 - Vector Absorption

**Goal:**

Move vector runtime, vector subsystem, vector storage mapping, and vector
sidecar cache policy into engine.

**Scope:**

Move the `strata-vector` implementation into engine, including:

- `VectorStore`
- vector config/value/domain types if not already engine-owned
- collection metadata/runtime state
- vector transaction extension behavior
- vector key construction
- vector recovery
- vector merge behavior
- mmap heap cache and HNSW graph sidecar cache policy
- vector purge behavior used by space/branch deletion

**Boundary requirement:**

Vector may scan storage and use storage keys only after it is physically inside
engine.

Vector sidecar files are engine runtime caches, not storage files. They should
remain separate from WAL/snapshot/manifest/segment mechanics and must not be
moved down into storage.

**Design notes:**

The vector crate currently has the broadest direct storage use above engine.
Particular care is needed around:

- startup recovery scans
- follower vs primary sidecar-cache rules
- branch deletion and cache purge
- collection delete and space delete behavior
- merge handler behavior
- extension state initialization

**Acceptance:**

- `strata-vector` has no normal storage dependency, or the crate is deleted
- intelligence imports vector types from engine
- executor imports vector surfaces from engine
- search imports vector surfaces from engine until search is absorbed
- vector tests either move with the module or become engine tests
- no vector production code outside engine imports `strata_storage`

**Non-goals:**

- do not redesign vector indexing
- do not redesign sidecar cache format
- do not move vector sidecar caches into storage

### EG5A - Vector Code Map And Characterization

Map vector storage touchpoints, recovery scans, sidecar cache policy, merge
handlers, branch deletion, space purge, and extension initialization. Add or
identify runtime tests before moving code.

### EG5B - Move Vector Runtime Into Engine

Physically move vector runtime, key mapping, transaction extension behavior, and
collection metadata into engine without changing storage format or index
semantics.

### EG5C - Move Sidecar And Recovery Paths

Move vector recovery, sidecar cache rebuild, follower/primary cache policy, and
purge behavior into engine-owned modules.

### EG5D - Cut Over Vector Consumers

Update search, intelligence, and executor imports to engine-owned vector
surfaces. Remove all vector storage access outside engine.

### EG5E - Retire The Vector Crate

Delete `strata-vector` when no normal dependents remain, or reduce it to a
documented temporary shell with an `EG9` deletion criterion.

## EG6 - Search Absorption

**Goal:**

Move search runtime and model-free retrieval orchestration into engine.

**Scope:**

Move the `strata-search` implementation into engine, including:

- retrieval substrate
- fusing
- query expansion interfaces
- rerank interfaces and blend logic
- prompt/parser support where it is part of search orchestration
- `QueryEmbedder` trait or its engine-owned replacement
- search integration with graph, vector, text, and engine primitives

**Intelligence boundary:**

Engine may own search orchestration and model-independent search APIs.

`strata-intelligence` should continue to own model/provider/inference
implementation and depend on engine, not the other way around.

The `QueryEmbedder`-style boundary is the right direction: engine can accept an
embedding provider trait object or adapter, while intelligence supplies the
implementation.

**Design notes:**

Search currently depends on graph, vector, storage, and engine. Once graph and
vector are engine modules, search should be moved into engine to avoid keeping a
peer crate that is mostly a facade over engine internals.

Feature flags such as `expand` and `rerank` should be reviewed during this
move. If they remain optional, they should be engine feature flags or
intelligence feature flags with a clear ownership rule.

**Acceptance:**

- `strata-search` has no normal storage dependency, or the crate is deleted
- intelligence imports search traits/types from engine
- executor imports search surfaces from engine
- search tests either move with the module or become engine tests
- no production search code outside engine imports `strata_storage`

**Non-goals:**

- do not make engine depend on `strata-inference`
- do not redesign ranking math unless a test-proven bug requires it
- do not move provider credentials or model download policy into engine

### EG6A - Search Code Map And Characterization

Map retrieval substrate, fusion, expansion, rerank, parser/prompt support,
graph/vector/text integration, and current feature flags. Add or identify tests
before moving runtime behavior.

### EG6B - Move Model-Free Search Runtime Into Engine

Physically move search substrate and orchestration into engine after graph and
vector are engine-owned modules.

### EG6C - Define The Intelligence Provider Boundary

Keep model execution in intelligence/inference. Engine should own the trait or
adapter boundary needed for embeddings, expansion, and rerank without depending
on `strata-inference`.

### EG6D - Cut Over Search Consumers And Retire Search

Update intelligence and executor imports to engine-owned search surfaces. Delete
`strata-search` when no normal dependents remain, or leave only a documented
temporary shell with an `EG9` deletion criterion.

## EG7 - Executor Storage Bypass Removal

**Goal:**

Remove direct storage access from executor after engine owns the primitives and
product runtime behavior executor needs.

**Scope:**

Replace executor direct storage usage with engine APIs:

- storage `Key`, `Namespace`, and `TypeTag` re-exports
- `validate_space_name`
- `StorageError` conversion
- direct transactional scans/deletes in space deletion
- raw transaction context use where executor is expressing product behavior

**Engine APIs likely needed:**

At minimum, engine should expose domain-level operations for:

- validating or normalizing space names
- deleting a space and all primitive data in that space
- deleting or purging graph/vector/search side effects for a space
- converting storage-origin errors into public engine errors before executor
  sees them

`handlers/space_delete.rs` is the clearest starting example. It currently loops
over storage `TypeTag` families directly, then separately purges search,
vector, embedding, and space metadata. That orchestration should become an
engine operation because it is a primitive/runtime consistency operation.

**Acceptance:**

- `strata-executor` has no normal dependency on `strata-storage`
- `strata-executor` no longer re-exports `Key` or `Namespace` from storage
- executor behavior tests still pass through engine-owned APIs
- no executor production file imports `strata_storage`

**Non-goals:**

- do not redesign the command API
- do not collapse executor into engine
- do not redesign user-facing error enums beyond the minimum needed for engine
  error conversion

### EG7A - Executor Storage Import Rebaseline

Re-run the executor storage import inventory after graph, vector, search,
security, and product open movement. Separate dead imports from missing engine
API cases.

### EG7B - Add Engine Runtime APIs For Executor-Owned Commands

Add semantic engine operations for space validation, space deletion, primitive
side-effect cleanup, and storage-origin error conversion. The APIs should not
expose raw storage keys or type tags.

### EG7C - Cut Over Executor Handlers

Route executor handlers and session code through engine-owned APIs. Preserve
command behavior and public response shapes unless a bug fix is explicitly
called out.

### EG7D - Remove Executor Storage Surface

Delete executor storage imports, storage re-exports, and direct storage error
conversion paths. Tighten the direct-storage guard for executor to zero.

## EG8 - Intelligence Dependency Cleanup

**Goal:**

Make intelligence depend on engine for search/vector types and on inference for
model execution, with no peer primitive/runtime crate dependencies.

**Scope:**

Remove direct dependencies from intelligence to:

- `strata-search`
- `strata-vector`
- dev-only `strata-graph` unless the tests are rewritten to use engine

Update imports to engine-owned search/vector surfaces.

**Design notes:**

Intelligence should not learn storage. It should also not need graph/vector peer
crates after those domains are engine-owned.

The intended shape is:

```text
engine <- intelligence -> inference
```

Engine owns database/search/vector contracts. Intelligence owns embeddings,
model-backed expansion/rerank implementations, and inference/provider wiring.

**Acceptance:**

- intelligence has no normal dependency on graph, vector, search, security, or
  storage crates
- intelligence tests use engine public surfaces
- engine still has no dependency on intelligence or inference

**Non-goals:**

- do not redesign inference providers
- do not redesign embedding cache policy unless required by the import move

### EG8A - Intelligence Dependency Rebaseline

Re-run intelligence dependency and import inventories after search/vector moves.
Confirm no direct storage access exists and identify remaining peer crate
imports.

### EG8B - Cut Over Search And Vector Imports

Update intelligence to use engine-owned search/vector traits, DTOs, and runtime
surfaces.

### EG8C - Preserve The Inference Boundary

Keep model execution, provider wiring, and inference-specific configuration in
intelligence/inference. Confirm engine still has no inference dependency.

### EG8D - Tighten Intelligence Dependency Guard

Remove intelligence dependencies on graph, vector, search, security, and storage
from manifests and guard allowlists.

## EG9 - Crate Deletion And Workspace Closeout

**Goal:**

Delete or retire the peer crates absorbed by engine and enforce the final graph.

**Scope:**

Remove from the workspace when no normal dependents remain:

- `crates/executor-legacy`
- `crates/graph`
- `crates/vector`
- `crates/search`

`crates/security` was deleted by `EG2D`.

Remove workspace dependency entries and feature plumbing for deleted crates.

Remove direct CLI dependencies on intelligence unless there is a documented
reason for a CLI command to bypass executor.

Tighten guard tests from transitional allowlists to final allowlists.

**Acceptance:**

The final normal workspace graph should satisfy:

```text
strata-storage      -> strata-core
strata-engine       -> strata-core, strata-storage
strata-intelligence -> strata-core, strata-engine, strata-inference
strata-executor     -> strata-core, strata-engine, strata-intelligence
strata-cli          -> strata-executor
stratadb            -> strata-executor
```

There should be no normal production dependency on:

- `strata-executor-legacy`
- `strata-graph`
- `strata-vector`
- `strata-search`

unless the crate is intentionally retained as a compatibility shell with a
documented removal date and no storage access.

`strata-security` is no longer a workspace crate and is guarded against
reintroduction.

**Non-goals:**

- do not perform broad engine-next modular redesign in this closeout
- do not start storage-next in this closeout

### EG9A - Delete Absorbed Crates And Workspace Metadata

Remove absorbed crates from `crates/`, workspace members, dependency manifests,
feature lists, and CI/test configuration after normal dependents are gone.

### EG9B - Tighten Final Storage And Dependency Guards

Make direct storage access above engine fail by default. The only normal
production storage dependent should be engine.

### EG9C - Refresh Architecture Documents

Update the engine crate map, storage consumption contract, and active
architecture docs so they describe the consolidated graph rather than the
migration.

### EG9D - Full Verification And Closeout Ledger

Run the full required test matrix, record known residual risks, and explicitly
name any temporary compatibility shell that survives closeout.

## Guard Commands

These commands are the baseline review tools for this plan.

Current graph inspection:

```bash
cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[]
      | select((.name|test("^strata-(storage|engine|graph|vector|search|intelligence|executor|executor-legacy|cli)$")) or .name=="stratadb")
      | "\(.name) -> \([.dependencies[] | select(.kind == null and .path != null) | .name] | join(", "))"'
```

Current production direct storage bypass inventory:

```bash
rg -n "strata_storage::|use strata_storage|strata-storage|strata_storage" \
  src crates/{graph,vector,search,executor,executor-legacy,intelligence,cli} \
  -g 'Cargo.toml' -g '*.rs'
```

Current workspace test/dev context inventory:

```bash
rg -n "strata_storage::|use strata_storage|strata-storage|strata_storage" . \
  -g 'Cargo.toml' -g '*.rs' \
  -g '!crates/storage/**' -g '!crates/engine/**' -g '!target/**'
```

Final production direct storage bypass guard:

```bash
rg -n "strata_storage::|use strata_storage|strata-storage|strata_storage" \
  src crates/{graph,vector,search,executor,executor-legacy,intelligence,cli} \
  -g 'Cargo.toml' -g '*.rs'
```

At closeout, the final production guard should return no matches because those
crates are deleted, retired, or no longer import storage. Root dev-dependencies
and storage-facing tests need their own explicit `EG1D` exception policy; they
should not be hidden by the production guard.

Final inverse dependency guard:

```bash
cargo tree -i strata-storage --workspace --edges normal --depth 2
```

At closeout, normal production dependents of storage should be engine only.
Storage-specific tests/tools can be documented exceptions, but product/runtime
crates above engine must not appear.

Final executor dependency guard:

```bash
cargo tree -p strata-executor --edges normal --depth 1
```

At closeout, executor should not depend on storage, graph, vector, search,
security, or executor-legacy.

Final intelligence dependency guard:

```bash
cargo tree -p strata-intelligence --edges normal --depth 1
```

At closeout, intelligence should depend on engine and inference, not graph,
vector, search, security, or storage.

## Testing Strategy

Every `EG` section needs tests at the level where behavior is consumed.

Recommended minimum gates:

- `cargo fmt --check`
- `cargo check --workspace --all-targets`
- moved crate or moved module test subset
- `cargo test -p strata-engine`
- `cargo test -p strata-executor`
- `cargo test -p strata-intelligence`
- targeted integration tests for open/bootstrap, graph, vector, search, space
  delete, recovery, and follower/cache modes

For high-risk moves, run characterization before moving code:

- graph branch DAG recovery and merge behavior
- vector collection recovery, sidecar cache rebuild, branch delete, space purge
- search retrieval/fusion/rerank/expand behavior
- product open/cache/follower behavior
- executor command compatibility

When a moved module has existing tests in its old crate, prefer moving those
tests with the implementation instead of rewriting them from memory.

## Risks

### Graph And Vector Are Stateful Runtime Systems

Graph and vector are not just type libraries. They participate in recovery,
merge, branch delete, search, and transaction behavior. Moving them requires
characterization at the engine/runtime level.

### Search Crosses Engine And Intelligence

Search orchestration belongs in engine, but model execution belongs in
intelligence/inference. The move must preserve that direction. Engine should
accept model-backed adapters; it should not depend on inference providers.

### Executor Has Accumulated Primitive Runtime Knowledge

Executor currently performs some operations by directly constructing storage
keys or calling primitive stores. That is too much runtime knowledge for the
command layer. Removing those paths may require new engine APIs, not just import
renames.

### Compatibility Shells Can Hide In The Graph

If graph/vector/search/executor-legacy remain as re-export crates, the
workspace may look consolidated while behavior is still split. Any compatibility
shell must be documented and guarded with a deletion criterion.

### Engine Will Get Bigger Before It Gets Cleaner

This plan intentionally accepts a larger engine crate. The cleanup objective is
crate graph correctness and storage-boundary discipline, not perfect internal
engine modularity. Engine-next should be designed later as part of the v1
architecture phase, after consolidation gives us a truthful engine to study and
before any major rebuild begins.

## Follow-Up Documents

Each `EG` phase should have one implementation plan when implementation starts.
The lettered sections for that phase live inside the phase plan. Likely phase
plans:

- `eg1-implementation-plan.md`
- `eg2-implementation-plan.md`
- `eg3-product-open-bootstrap-plan.md`
- `eg4-graph-absorption-plan.md`
- `eg5-vector-absorption-plan.md`
- `eg6-search-absorption-plan.md`
- `eg7-executor-storage-bypass-removal-plan.md`
- `eg8-intelligence-dependency-cleanup-plan.md`
- `eg9-crate-deletion-workspace-closeout-plan.md`
- `strata-v1-target-architecture.md`
- `storage-next-target-architecture.md`
- `engine-next-target-architecture.md`
- `strata-v1-testing-strategy.md`
- `storage-engine-product-experience.md`

The phase plans should keep the same storage-access rule: no crate above engine
talks to storage directly unless the exception is explicit, narrow, and
temporary.
