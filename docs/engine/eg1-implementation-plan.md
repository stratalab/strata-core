# EG1 Implementation Plan

## Purpose

`EG1` freezes the current engine-adjacent crate graph, records every direct
storage bypass above engine, identifies the missing engine endpoints that make
those bypasses necessary, and prepares the guard that later epics will tighten.

This is the single implementation plan for the `EG1` phase. Lettered sections
such as `EG1A`, `EG1B`, `EG1C`, and `EG1D` are tracked in this file rather
than in separate letter-specific documents.

Read this with:

- [engine-consolidation-plan.md](./engine-consolidation-plan.md)
- [engine-crate-map.md](./engine-crate-map.md)
- [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)

## Scope

`EG1` owns:

- verifying the normal dependency graph for engine-adjacent crates
- verifying the inverse normal dependency tree for `strata-storage`
- recording direct normal production storage use above engine
- separating production bypasses from root/test/dev storage use
- classifying bypasses by removal epic
- identifying missing engine APIs that force executor storage access today
- defining the transitional storage-import guard and allowlist policy

`EG1` does not own:

- moving graph, vector, search, security, or legacy bootstrap code into engine
- removing production storage imports
- redesigning engine APIs beyond the endpoint gap ledger
- changing storage, WAL, manifest, checkpoint, snapshot, or recovery behavior
- creating `storage-next` or `engine-next`

## Section Status

| Section | Status | Deliverable |
| --- | --- | --- |
| `EG1A` | Complete | Verified dependency graph and inverse storage graph. |
| `EG1B` | Complete | Direct storage bypass inventory and test/dev context split. |
| `EG1C` | Complete | Engine endpoint gap ledger for executor and post-absorption callers. |
| `EG1D` | Complete | Transitional import/dependency guard with explicit allowlist. |

## Shared Commands

Dependency graph baseline:

```bash
cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[]
      | select((.name|test("^strata-(storage|engine|graph|vector|search|intelligence|executor|executor-legacy|cli)$")) or .name=="stratadb")
      | "\(.name) -> \([.dependencies[] | select(.kind == null and .path != null) | .name] | join(", "))"'
```

Inverse storage dependency tree:

```bash
cargo tree -i strata-storage --workspace --edges normal --depth 2
```

Executor and intelligence incoming-edge checks:

```bash
cargo tree -p strata-executor --edges normal --depth 1
cargo tree -p strata-intelligence --edges normal --depth 1
```

Production direct storage bypass inventory:

```bash
rg -n "strata_storage::|use strata_storage|strata-storage|strata_storage" \
  src crates/{graph,vector,search,executor,executor-legacy,intelligence,cli} \
  -g 'Cargo.toml' -g '*.rs'
```

Workspace test/dev storage context inventory:

```bash
rg -n "strata_storage::|use strata_storage|strata-storage|strata_storage" . \
  -g 'Cargo.toml' -g '*.rs' \
  -g '!crates/storage/**' -g '!crates/engine/**' -g '!target/**'
```

Endpoint-gap discovery helper:

```bash
rg -n "pub trait TransactionOps|struct SpaceIndex|fn is_empty|fn delete|validate_space_name|StorageError|pub use strata_storage|Namespace|TypeTag" \
  crates/{engine,executor}/src -g '*.rs'
```

## EG1A - Dependency Graph Baseline

### Goal

Freeze the current normal dependency graph before engine consolidation starts
moving code.

### Work

- Run the dependency graph baseline command.
- Run the inverse `strata-storage` tree.
- Run direct executor and intelligence dependency checks.
- Record which crates are direct normal storage dependents above engine.
- Update [engine-crate-map.md](./engine-crate-map.md) if the graph has drifted.

### Verified Result

`EG1A` was run on 2026-05-04.

The verified normal workspace graph is:

```text
strata-storage         -> strata-core
strata-engine          -> strata-core, strata-security, strata-storage
strata-graph           -> strata-core, strata-engine, strata-storage
strata-intelligence    -> strata-core, strata-engine, strata-inference,
                          strata-search, strata-vector
strata-search          -> strata-core, strata-engine, strata-graph,
                          strata-storage, strata-vector
strata-vector          -> strata-core, strata-engine, strata-storage
strata-cli             -> strata-executor, strata-intelligence
strata-executor        -> strata-core, strata-engine, strata-executor-legacy,
                          strata-graph, strata-intelligence, strata-search,
                          strata-security, strata-storage, strata-vector
strata-executor-legacy -> strata-core, strata-engine, strata-graph,
                          strata-security, strata-vector
stratadb               -> strata-executor
```

The current direct normal storage dependents are:

```text
strata-storage
|-- strata-engine
|-- strata-executor
|-- strata-graph
|-- strata-search
`-- strata-vector
```

Classification:

- Permanent storage dependent:
  - `strata-engine`
- Transitional direct storage bypasses above engine:
  - `strata-executor`
  - `strata-graph`
  - `strata-search`
  - `strata-vector`
- No direct normal storage edge today:
  - `strata-security`
  - `strata-executor-legacy`
  - `strata-intelligence`
  - `strata-cli`
  - `stratadb`

Important distinction: executor-legacy, graph, intelligence, search, and vector
also appear under the engine branch in the full inverse storage tree because
they depend on engine. That is not itself a storage bypass. The bypasses that
matter for `EG1` are direct normal storage edges above engine.

### Acceptance

`EG1A` is complete when:

- the active engine crate map records the verified normal dependency graph
- the active engine crate map records the current inverse normal storage graph
- direct structural storage bypasses above engine are classified
- `EG1B` has a clear source-level import inventory target

Status: complete.

## EG1B - Direct Storage Bypass Inventory

### Goal

Record every direct normal production `strata-storage` use above engine by
crate, file, storage surface, ownership reason, and removal epic.

### Work

- Scan normal production source and manifests above engine for direct storage
  use.
- Classify each production bypass by crate, surface, and ownership reason.
- Assign each bypass to `EG4`, `EG5`, `EG6`, or `EG7`.
- Prove executor-legacy, intelligence, CLI, and root production code do not
  contain hidden normal production storage imports.
- Record root/test/dev storage uses separately for `EG1D`.

### Scan Result Summary

`EG1B` was run on 2026-05-04.

Direct normal production storage uses exist in:

- `crates/graph`
- `crates/vector`
- `crates/search`
- `crates/executor`

No direct normal production storage imports or normal manifest dependencies were
found in:

- `crates/executor-legacy`
- `crates/intelligence`
- `crates/cli`
- root `stratadb` package source and normal dependencies

### Manifest Inventory

| Crate | Normal direct `strata-storage` dependency | Dev/test storage dependency | Removal epic | Notes |
| --- | --- | --- | --- | --- |
| `strata-graph` | Yes | No | `EG4` | Graph is a storage-backed primitive peer crate today. |
| `strata-vector` | Yes | No | `EG5` | Vector owns storage-backed records plus sidecar runtime caches today. |
| `strata-search` | Yes | No | `EG6` | Search uses storage keys for visibility checks and hit materialization. |
| `strata-executor` | Yes | No | `EG7` | Executor constructs raw storage keys and converts storage errors. |
| `strata-executor-legacy` | No | No | `EG3` / `EG9` | Depends on engine/security/graph/vector, not storage directly. |
| `strata-intelligence` | No | No | `EG8` | Depends on search/vector, not storage directly. |
| `strata-cli` | No | No | `EG9` | Enters through executor/intelligence today. |
| root `stratadb` package | No | Yes | `EG1D` policy | Normal dependency is executor only; root dev-dependencies include storage for tests. |

### Production Bypass Inventory

#### Graph - Remove In EG4

Direct files:

- `crates/graph/Cargo.toml`
- `crates/graph/src/keys.rs`
- `crates/graph/src/lib.rs`
- `crates/graph/src/ext.rs`
- `crates/graph/src/store.rs`
- `crates/graph/src/merge.rs`
- `crates/graph/src/merge_handler.rs`
- `crates/graph/src/bulk.rs`
- `crates/graph/src/edges.rs`

Storage surfaces:

- `Key`
- `Namespace`
- `TypeTag`
- `TransactionContext`

Ownership reason:

Graph is currently implemented as a peer primitive crate that maps graph domain
state onto storage key families. It builds graph namespaces and keys, implements
transaction-extension graph operations, filters merge cells by
`TypeTag::Graph`, and uses storage keys as the physical graph row identity.

Removal path:

`EG4` physically moves graph runtime, graph key mapping, transaction extension
behavior, merge planning, and graph subsystem code into engine. After that move,
these storage surfaces become private engine implementation details.

#### Vector - Remove In EG5

Direct files:

- `crates/vector/Cargo.toml`
- `crates/vector/src/ext.rs`
- `crates/vector/src/merge_handler.rs`
- `crates/vector/src/recovery.rs`
- `crates/vector/src/store/mod.rs`
- `crates/vector/src/store/collections.rs`
- `crates/vector/src/store/crud.rs`
- `crates/vector/src/store/recovery.rs`
- `crates/vector/src/store/search.rs`
- `crates/vector/src/store/system.rs`

Storage surfaces:

- `Key`
- `Namespace`
- `Storage`
- `TypeTag`
- `TransactionContext`

Ownership reason:

Vector currently owns collection metadata, vector records, transactional vector
writes, storage-backed search filtering, startup recovery scans, merge
prechecks, post-merge rebuilds, and vector sidecar-cache rebuild policy as a
peer crate.

Removal path:

`EG5` physically moves vector runtime, storage key mapping, transaction
extension behavior, recovery, merge handling, and sidecar cache policy into
engine. Executor and intelligence still need cutovers to engine-owned vector
surfaces after the move.

#### Search - Remove In EG6

Direct files:

- `crates/search/Cargo.toml`
- `crates/search/src/substrate.rs`

Storage surfaces:

- `Key`
- `Namespace`

Ownership reason:

Search currently constructs storage keys from search hit entity references to
perform point-in-time and time-window visibility checks. Tests in the same file
also construct storage keys to observe commit timestamps.

Removal path:

`EG6` moves the search substrate into engine after graph and vector are
engine-owned modules. The storage-key visibility helper should either remain
local to the engine search module or become an engine-internal primitive
visibility helper.

#### Executor - Remove In EG7

Direct files:

- `crates/executor/Cargo.toml`
- `crates/executor/src/handlers/space_delete.rs`
- `crates/executor/src/handlers/kv.rs`
- `crates/executor/src/handlers/json.rs`
- `crates/executor/src/handlers/event.rs`
- `crates/executor/src/session.rs`
- `crates/executor/src/bridge.rs`
- `crates/executor/src/compat.rs`
- `crates/executor/src/convert.rs`
- `crates/executor/src/lib.rs`

Storage surfaces:

- `Key`
- `Namespace`
- `TypeTag`
- `StorageError`
- `validate_space_name`

Ownership reason:

Executor currently performs some engine/runtime work itself:

- `space_delete.rs` iterates raw `TypeTag` families and deletes storage rows
  directly, then separately cleans search/vector/embedding side effects.
- `kv.rs`, `json.rs`, `event.rs`, and `session.rs` construct storage
  namespaces and keys for in-transaction command execution.
- `bridge.rs` and `compat.rs` call storage-owned space-name validation.
- `convert.rs` maps `StorageError` directly into executor errors.
- `lib.rs` publicly re-exports storage `Key` and `Namespace`.

Removal path:

`EG7` replaces these with engine-owned semantic/runtime APIs after graph,
vector, search, security, and product open are absorbed.

### Test And Dev Storage Uses

The workspace-wide context scan also finds direct storage uses outside the
normal production crate graph. These are not `EG4` through `EG7` production
bypasses, but they must be visible to `EG1D` so guard exceptions are explicit.

Root manifest:

- [Cargo.toml](../../Cargo.toml) has a root `stratadb` dev-dependency on
  `strata-storage` for integration and surface tests.

Current test/dev categories:

| Category | Examples | Guard disposition |
| --- | --- | --- |
| Storage substrate tests | `tests/storage/**`, `tests/transaction_runtime/**` | Intentional storage tests; should remain allowed. |
| Engine tests that poison or inspect storage state | `tests/engine/**`, `tests/integration/recovery_cross_crate.rs`, `tests/integration/branching_degraded_primitive_paths.rs` | Allow only when the test name/path documents boundary or corruption/recovery intent. |
| Retention, quarantine, and degraded recovery integration tests | `tests/integration/branching_gc_quarantine_recovery.rs`, `tests/integration/branching_retention_matrix.rs` | Likely intentional because they assert storage-owned recovery artifacts. |
| Surface/import tests | `tests/engine_error_surface.rs`, `tests/storage_surface_imports.rs` | Intentional while they assert crate-boundary behavior. |

`EG1D` decides whether these stay as broad path-based exceptions or move behind
narrower allowlist markers. They should not be counted as production storage
bypasses.

### Acceptance

`EG1B` is complete when:

- every normal production direct storage dependency above engine is assigned to
  `EG4`, `EG5`, `EG6`, or `EG7`
- executor-legacy, intelligence, CLI, and root `stratadb` production code are
  explicitly checked for hidden direct storage imports
- root/test/dev storage uses are recorded separately for `EG1D`
- executor-specific storage uses that need new engine APIs are handed off to
  `EG1C`

Status: complete.

## EG1C - Engine Endpoint Gap Ledger

### Goal

Convert the bypass inventory into a concrete endpoint work queue for later
epics. `EG1C` should distinguish:

- engine APIs that already exist and only need caller cutover
- semantic engine APIs that are missing
- narrow engine runtime APIs that are missing
- public storage surfaces that should be removed rather than replaced
- storage access that becomes engine-internal after graph/vector/search
  absorption

### Work

- Inspect executor storage uses in `handlers/space_delete.rs`,
  `handlers/kv.rs`, `handlers/json.rs`, `handlers/event.rs`, `session.rs`,
  `bridge.rs`, `compat.rs`, `convert.rs`, and `lib.rs`.
- Compare those uses with existing engine surfaces such as `SpaceIndex`,
  `TransactionOps`, engine transaction wrappers, and `StrataError` conversion.
- Produce a ledger of missing or insufficient engine endpoints.
- Assign each endpoint gap to its consuming epic, usually `EG7`.
- Record dependencies on `EG4`, `EG5`, and `EG6` when a clean executor cutover
  requires graph, vector, or search to be inside engine first.

### Verified Existing Engine Surfaces

`EG1C` was run on 2026-05-04.

The useful engine surfaces that already exist are:

- `SpaceIndex::register`, `exists`, `list`, and `is_empty`.
- `SpaceIndex::delete`, but this deletes the metadata key only. It is not a
  full space deletion operation.
- `TransactionOps` and `transaction::context::Transaction` cover basic
  transaction-scoped KV, event, and JSON primitive operations.
- `transaction::owned::Transaction` owns manual commit/abort state and exposes
  `scoped(namespace)`, but the caller must still build a storage `Namespace`.
- `EventLog::get_by_type`, `get_by_type_at`, `list_types`, and `list_types_at`
  exist for non-manual transaction callers.
- `InvertedIndex::remove_documents_in_space` exists in engine search.
- Vector collection cleanup exists today through the peer vector crate. Shadow
  embedding row cleanup exists through intelligence helpers that currently use
  vector storage. `EG5` can move the durable vector/shadow-row mechanics into
  engine, but model-backed embedding extraction and queueing must remain above
  engine in intelligence.
- `From<StrataError> for executor::Error` already exists. Direct
  `From<StorageError>` exists only because executor still calls raw storage
  APIs.

The existing surfaces are not enough to remove executor's storage dependency.
The remaining gaps are command-shape and ownership gaps, not missing raw
storage mechanics.

### Endpoint Gap Ledger

| ID | Current bypass | Existing engine surface | Missing endpoint or contract | Kind | Owner | Dependencies |
| --- | --- | --- | --- | --- | --- | --- |
| `EG1C-1` | `compat.rs` and `bridge.rs` call `strata_storage::validate_space_name`. | None. Space validation rules live in storage layout. | Add engine-owned space-name validation, likely as `strata_engine::validate_space_name` or a `SpaceIndex` validation helper. Executor should not import storage for product input validation. | Semantic validation | `EG7` | None |
| `EG1C-2` | `session.rs` constructs `Namespace::for_branch_space` before dispatching transaction commands. | `Database::begin_transaction` returns an engine-owned `Transaction`, but `Transaction::scoped` still requires a storage `Namespace`. | Add an engine-owned way to scope a manual transaction by semantic space, such as `Transaction::scoped_space(space)` or a command transaction adapter. The caller should pass branch/space strings, not storage namespaces. | Runtime adapter | `EG7` | None for KV/Event/JSON; `EG4`/`EG5` before graph/vector transaction cutover |
| `EG1C-3` | `handlers/kv.rs` builds `Key` and `TypeTag::KV` for in-transaction get/list/scan/delete/batch operations. | `TransactionOps` has `kv_get`, `kv_put`, `kv_delete`, `kv_exists`, and `kv_list`. | Extend engine transaction APIs or add a command adapter for executor's command shapes: cursor/limit key pages, start/limit scans, batch get/delete/exists result semantics, and post-commit side-effect recording. | Runtime adapter | `EG7` | `EG1C-8` for side effects |
| `EG1C-4` | `handlers/event.rs` builds event type index keys with `Key::new_event_type_idx_prefix` and reads event keys directly. | `TransactionOps` has append/get/len/range. `EventLog` has type queries outside manual transactions. | Add transaction-scoped event type query support, or route command execution through an engine command transaction adapter that can perform `EventGetByType` without exposing event index keys. This must preserve read-your-writes behavior: events appended earlier in the same manual transaction write type-index keys into the transaction context, and `EventGetByType` must see them before commit. Do not replace this with the non-transactional `EventLog::get_by_type` path. | Runtime adapter | `EG7` | None |
| `EG1C-5` | `handlers/json.rs` imports `Namespace` because it scopes an engine `Transaction` with a storage namespace. | Owned engine transactions already carry JSON transaction state and `json_list_keys`. | Remove the namespace leak with the same semantic transaction scoping from `EG1C-2`. JSON-specific command coverage is mostly present once scoping no longer requires storage types. | Runtime adapter | `EG7` | None |
| `EG1C-6` | Executor obtains raw `TransactionContext` with `context_mut()` and passes it to KV/event/graph/vector handlers. | `Transaction` owns the context but still exposes it. | Define the intended upper-crate transaction contract. Executor should operate through an engine transaction facade, not through `TransactionContext`. Raw context access can remain engine-internal. | Runtime contract | `EG7` | `EG4` and `EG5` if the facade includes graph/vector commands |
| `EG1C-7` | `handlers/space_delete.rs` creates a storage namespace, iterates `TypeTag::{KV, Event, Json, Vector, Graph}`, scans raw prefixes, and deletes raw rows. | `SpaceIndex::is_empty` exists. `SpaceIndex::delete` is metadata-only. Search has `remove_documents_in_space`; vector has `purge_collections_in_space`; shadow embedding cleanup is currently above engine. | Add an engine-owned semantic space deletion operation. It must reject `default` and `_system_`, optionally enforce emptiness when `force == false`, delete all primitive rows, purge search docs, purge vector collections, remove durable shadow embedding rows, and then delete space metadata. It must preserve today's error policy unless EG7 explicitly characterizes and changes it: raw row deletion and final metadata deletion are fatal; missing search index extension is ignored; vector/shadow cleanup failures are best-effort and must not mask successful row deletion. | Semantic API | `EG7` | `EG4`, `EG5`, and `EG6`; durable shadow-row cleanup can become engine-owned after vector moves, but model execution stays in intelligence |
| `EG1C-8` | `session.rs` tracks `TxnSideEffects` for KV/JSON/Event mutations and applies search/embed updates after commit. | Engine search index and shadow collection constants exist, but the side-effect ledger is executor-owned. | Move storage-backed post-commit bookkeeping out of executor without making engine depend on intelligence. Engine may own search-index refresh, commit observer events, and durable shadow-row cleanup APIs. Intelligence must continue to own embedding text extraction, model selection, embedding queueing, and inference-backed work by registering against engine-owned hooks. Executor can remain a temporary caller until `EG8`, but the target must keep engine below intelligence: engine must not call or depend on intelligence. Side-effect failures after a durable commit must not turn `TxnCommit` into a failure. | Semantic runtime | `EG7` / `EG8` | `EG5`/`EG6`; `EG8` defines the intelligence-owned embedding hook so engine remains below intelligence |
| `EG1C-9` | Executor graph transaction commands call peer `strata-graph` extension traits over raw transaction context. | Graph is not engine-owned yet. | No new public storage endpoint. Absorb graph into engine in `EG4`, then expose engine-owned graph transaction methods or include graph commands in the command transaction adapter. | Import cutover after absorption | `EG4` then `EG7` | `EG4` |
| `EG1C-10` | Executor vector transaction commands call peer `strata-vector` extension traits over raw transaction context. | Vector is not engine-owned yet. | No new public storage endpoint. Absorb vector into engine in `EG5`, then expose engine-owned vector transaction methods or include vector commands in the command transaction adapter. | Import cutover after absorption | `EG5` then `EG7` | `EG5` |
| `EG1C-11` | `convert.rs` implements `From<StorageError> for executor::Error`. | `From<StrataError> for executor::Error` already maps engine errors. | Delete direct storage error conversion once raw storage calls are gone. Executor should only convert engine or executor-domain errors. | Removed surface | `EG7` | `EG1C-2` through `EG1C-7` |
| `EG1C-12` | `lib.rs` publicly re-exports storage `Key` and `Namespace`; root `stratadb` re-exports the executor surface wholesale. | No in-repo production caller was found for `strata_executor::Key` or `strata_executor::Namespace`. | Delete the re-exports unless `EG7` discovers a current in-repo caller. This is also a root `stratadb::Key` / `stratadb::Namespace` public API removal because `src/lib.rs` does `pub use strata_executor::*`. Pre-v1 compatibility posture allows that, but EG7 should call it out. Do not replace them with engine-owned types unless product APIs actually need such identifiers. | Removed surface | `EG7` | None |

### Executor Cutover Order

The safest executor storage-dependency removal order is:

1. Add engine-owned space-name validation and cut over `compat.rs` /
   `bridge.rs`.
2. Add semantic transaction scoping so executor no longer constructs
   `Namespace`.
3. Move KV/Event/JSON in-transaction command handling behind engine transaction
   APIs or a command transaction adapter.
4. After `EG4` and `EG5`, cut over graph/vector in-transaction commands to
   engine-owned surfaces.
5. After `EG5` and `EG6`, move storage-backed transaction side-effect ownership
   and full space deletion into engine. Preserve the current durable-commit
   policy: once the transaction is committed, side-effect refresh failures must
   be logged/reported separately rather than making `TxnCommit` fail.
6. Delete `From<StorageError> for executor::Error`, the executor
   `Key`/`Namespace` re-exports, and the executor normal `strata-storage`
   manifest dependency.

### Non-Endpoint Classifications

Graph, vector, and search storage imports are not executor endpoint gaps by
themselves:

- Graph storage use becomes engine-internal when `EG4` absorbs graph.
- Vector storage use becomes engine-internal when `EG5` absorbs vector.
- Search storage use becomes engine-internal when `EG6` absorbs search.

Do not create broad engine facades solely to preserve these peer crates. The
preferred move is physical absorption, then import cutover.

### Guard Inputs For EG1D

`EG1D` should be able to assert these production allowlist removal targets:

- `crates/graph/**` storage imports disappear in `EG4`.
- `crates/vector/**` storage imports disappear in `EG5`.
- `crates/search/**` storage imports disappear in `EG6`.
- `crates/executor/**` storage imports disappear in `EG7`.

For executor specifically, the guard should keep failing until all of these are
gone:

- `use strata_storage::validate_space_name`
- `use strata_storage::{Key, Namespace, TypeTag}`
- `use strata_storage::{Key, Namespace}`
- `use strata_storage::Namespace`
- `use strata_storage::StorageError`
- `pub use strata_storage::{Key, Namespace}`
- the normal `strata-storage` manifest dependency

Executor also has a test-only storage import in `session.rs` for a corrupt JSON
fixture. `EG1D` should either mark that as an explicit test/dev exception or
require EG7 to rewrite the fixture through an engine-owned test hook before the
executor manifest dependency is removed.

### Acceptance

`EG1C` is complete when:

- every executor production storage import is classified as an existing engine
  API cutover, missing engine endpoint, missing runtime adapter, or removable
  public surface
- every missing endpoint has an owning later epic
- graph/vector/search storage imports are explicitly classified as absorption
  work rather than public endpoint work
- `EG7` has a concrete work queue for removing executor's storage dependency

Status: complete.

## EG1D - Transitional Guard Setup

### Goal

Add or reserve a dependency/import guard that passes at the beginning of engine
consolidation and can be tightened after each later epic.

### Work

- Add or update a guard test near the existing surface/import guards.
- Scan manifests and Rust source for direct `strata-storage` usage outside the
  current allowlist.
- Keep production and test/dev storage uses separate.
- Document each temporary production allowlist entry with its removal epic.
- After each absorption epic, remove the corresponding allowlist entry.

### Implemented Guard

`EG1D` was implemented in
[tests/storage_surface_imports.rs](../../tests/storage_surface_imports.rs).

The guard test is:

```bash
cargo test -p stratadb --test storage_surface_imports \
  engine_consolidation_direct_storage_bypasses_are_allowlisted
```

The test scans production source and manifests under:

- `src`
- `crates/graph`
- `crates/vector`
- `crates/search`
- `crates/executor`
- `crates/executor-legacy`
- `crates/intelligence`
- `crates/cli`

It fails when a direct `strata-storage` reference appears outside the explicit
`EG1D` allowlist. Each allowlist entry names the epic that removes it. The test
also fails when an allowlist entry goes stale, so later epics must delete their
allowlist entries as storage bypasses disappear.

This is a production guard. It deliberately does not hide workspace test/dev
storage uses inside the production allowlist.

### Initial Production Allowlist

The guard may initially allow direct production storage use in:

- `crates/engine/**`
- `crates/graph/**`, removed by `EG4`
- `crates/vector/**`, removed by `EG5`
- `crates/search/**`, removed by `EG6`
- `crates/executor/**`, removed by `EG7`

The implemented allowlist is file-level rather than directory-level so new
storage imports in those crates require an explicit architecture decision.

The final production allowlist should be:

- `crates/storage/**`
- `crates/engine/**`

### Test And Dev Policy

Storage-facing tests and tools need their own exception policy. They should not
be hidden inside the production allowlist.

Allowed test/dev exceptions should be narrow and intentional:

- storage substrate tests
- storage recovery/corruption tests
- storage format/surface guard tests
- future storage-next migration or validation tools

Engine, executor, intelligence, CLI, and product tests should use engine APIs
unless the test is explicitly verifying storage-boundary behavior.

### Acceptance

`EG1D` is complete when:

- a guard test exists with commands, allowlist, and deletion criteria
- the guard separates production bypasses from test/dev storage use
- each temporary production allowlist entry names the epic that removes it
- the guard can be tightened after `EG4`, `EG5`, `EG6`, and `EG7`

Status: complete.

## EG1 Closeout

`EG1` is complete when:

- `EG1A`, `EG1B`, `EG1C`, and `EG1D` are complete
- [engine-consolidation-plan.md](./engine-consolidation-plan.md) links to this
  single phase implementation plan
- [engine-crate-map.md](./engine-crate-map.md) points to this plan for the
  verified graph and bypass inventory
- no active `EG1A`/`EG1B`/`EG1C`/`EG1D` standalone implementation-plan files
  exist in `docs/engine`

Status: complete.

The next implementation phase is `EG2`.
