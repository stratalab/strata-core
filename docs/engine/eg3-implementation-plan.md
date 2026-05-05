# EG3 Implementation Plan

## Purpose

`EG3` absorbs the product open/bootstrap shell currently hosted in
`strata-executor-legacy` into `strata-engine`, then removes the normal executor
dependency on the legacy crate.

The legacy crate is now only a bootstrap seam. It owns no storage behavior
directly, but it still decides how product database handles are opened:

- primary, follower, and cache open modes
- product subsystem registration
- default branch bootstrap
- built-in recipe seeding
- lock-to-IPC fallback classification
- selected access mode for the returned handle

Those decisions are engine/database runtime policy. Executor should create
sessions and execute commands; it should not host the canonical product open
contract.

This is the single implementation plan for the `EG3` phase. Lettered sections
such as `EG3A`, `EG3B`, `EG3C`, and `EG3D` are tracked in this file rather
than in separate letter-specific documents.

Read this with:

- [engine-consolidation-plan.md](./engine-consolidation-plan.md)
- [engine-crate-map.md](./engine-crate-map.md)
- [eg1-implementation-plan.md](./eg1-implementation-plan.md)
- [eg2-implementation-plan.md](./eg2-implementation-plan.md)
- [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)

## Scope

`EG3` owns:

- characterizing current `strata_executor::Strata` open/cache/follower behavior
- adding an engine-owned product open API and result shape
- moving lock-to-IPC fallback classification out of executor-legacy
- moving built-in recipe seeding policy into the engine product-open path
- making default branch bootstrap engine-owned product-open behavior
- updating `OpenSpec` docs so product defaults are no longer described as
  executor-owned
- cutting executor over from `strata-executor-legacy` to engine-owned product
  open
- retiring `crates/executor-legacy` if no normal dependents remain, or reducing
  it to an explicitly documented temporary shell
- adding guard coverage so the retired legacy edge cannot return silently

`EG3` does not own:

- moving graph implementation into engine (`EG4`)
- moving vector implementation into engine (`EG5`)
- moving search crate behavior into engine (`EG6`; engine already owns the
  current `SearchSubsystem`)
- removing executor direct storage imports (`EG7`)
- moving IPC transport/server code into engine
- redesigning CLI behavior, command/session APIs, or executor error enums
- changing storage, WAL, manifest, checkpoint, snapshot, segment, or config
  formats

## Load-Bearing Constraint

Engine cannot directly instantiate `GraphSubsystem` or `VectorSubsystem` during
`EG3`.

Today:

```text
strata-graph  -> strata-engine
strata-vector -> strata-engine
```

Adding `strata-engine -> strata-graph` or `strata-engine -> strata-vector`
would create a crate cycle and violate the consolidation plan's no-loop rule.
Therefore `EG3` must split product open ownership from temporary subsystem
construction unless graph/vector absorption is pulled earlier.

The target after `EG3` is:

- engine owns the product open API, default branch policy, recipe seeding
  policy, access-mode outcome, and lock-to-IPC classification
- executor no longer depends on `strata-executor-legacy`
- executor may temporarily provide the current graph/vector/search subsystem
  bundle to the engine product-open API until `EG4` and `EG5` move graph/vector
  into engine, but this is an explicit bridge over a bad architecture, not a
  design pattern to preserve

This temporary subsystem handoff must be narrow and named. It is not a storage
bypass, but it is still transitional architecture. Its deletion criterion is:

- after `EG4` and `EG5`, engine can construct the full product subsystem set
  itself
- at that point the product-open API should stop requiring executor to supply
  graph/vector subsystems

## Subsystem Instantiation Retirement Target

The current "instantiate subsystem structs and pass them into an open spec"
architecture is a cleanup target, not a long-term extension point.

The useful idea is the lifecycle ordering:

- recover
- initialize
- bootstrap
- freeze

The bad idea is making upper crates assemble that lifecycle by constructing
subsystem objects. Product callers should not need to know that graph, vector,
or search are separate subsystem structs. Once graph, vector, and search are
engine-owned modules, product open should select the engine's product runtime
internally.

Target end state:

- no production crate above engine instantiates `GraphSubsystem`,
  `VectorSubsystem`, `SearchSubsystem`, or boxed `Subsystem` values
- no product open path exposes a `ProductSubsystemProfile`,
  `ProductRuntimeProfile`, or equivalent public subsystem bundle
- `OpenSpec::with_subsystem` is either removed from the public engine surface,
  narrowed to engine-internal/test-support usage, or replaced by an internal
  module registration mechanism
- engine owns lifecycle ordering and module registration for its graph, vector,
  search, and primitive runtimes
- tests may still use narrow test-only hooks, but those hooks must not become
  product architecture

`EG3` should avoid introducing a polished replacement for subsystem profiles.
If a temporary subsystem handoff is required to remove `strata-executor-legacy`
before `EG4`/`EG5`, name it as transitional and make the deletion criterion
obvious in code and docs.

## Retired Code Absorbed

The bootstrap shell absorbed during `EG3` was:

- `crates/executor-legacy/src/bootstrap.rs`
- `crates/executor-legacy/src/lib.rs`
- `crates/executor-legacy/src/error.rs`

Those files and the `crates/executor-legacy` workspace package were deleted in
`EG3D` after executor cut over to engine product open.

Current executor call sites:

- [crates/executor/src/compat.rs](../../crates/executor/src/compat.rs)
- [crates/executor/Cargo.toml](../../crates/executor/Cargo.toml)

Current product-open tests and behavior coverage:

- [tests/executor_ex6_runtime.rs](../../tests/executor_ex6_runtime.rs)
- [tests/engine_security_surface.rs](../../tests/engine_security_surface.rs)
- [crates/executor/src/compat.rs](../../crates/executor/src/compat.rs) test module
- [crates/cli/src/open.rs](../../crates/cli/src/open.rs) test module

Current engine open substrate:

- [crates/engine/src/database/spec.rs](../../crates/engine/src/database/spec.rs)
- [crates/engine/src/database/open.rs](../../crates/engine/src/database/open.rs)
- [crates/engine/src/database/open_options.rs](../../crates/engine/src/database/open_options.rs)
- [crates/engine/src/recipe_store.rs](../../crates/engine/src/recipe_store.rs)

## Current Behavior Contract

`EG3` must preserve these behaviors unless a change is explicitly called out in
the implementation review:

- `Strata::open(path)` is equivalent to `Strata::open_with(path,
  OpenOptions::default())`.
- `OpenOptions::default()` opens a primary product database with
  `AccessMode::ReadWrite`.
- `OpenOptions::access_mode(AccessMode::ReadOnly)` opens the same primary
  product runtime but executor/session writes are rejected.
- `OpenOptions::follower(true)` opens a follower runtime and returns
  `AccessMode::ReadOnly` even if the option's `access_mode` is `ReadWrite`.
- Product primary/cache opens register graph, vector, and search subsystems in
  the current order: graph, vector, search.
- Product primary/cache opens use default branch `"default"`.
- Product follower opens do not create or bootstrap default branch state.
- Successful primary/cache opens seed built-in recipes. Seeding failure is
  logged as a warning and does not fail open.
- Successful read-write primary/cache handles ensure the selected default
  branch exists.
- `Strata::cache()` opens a local read-write ephemeral database with the product
  subsystem set and default branch `"default"`.
- If primary disk open fails because another process holds the database lock and
  `<data_dir>/strata.sock` exists, `Strata::open_with` returns an IPC-backed
  handle.
- If primary disk open fails because another process holds the database lock and
  no socket exists, open fails with the current user-facing message:
  `Database is locked by another process. Run \`strata up\` to enable shared
  access, or use --follower for read-only access.`
- `Strata::database()` still panics on IPC-backed handles.
- `Strata::new_handle()` preserves the existing local/IPC behavior and refreshes
  the runtime default branch.
- CLI open/init/up paths continue to call through `strata_executor::Strata`.

## Target Engine API Shape

Preferred module placement:

- `crates/engine/src/database/product_open.rs`

Preferred public engine types:

- `ProductOpenOutcome`
- `ProductOpenError`

The exact names can change during implementation, but the ownership shape
should not.

The engine-owned outcome should distinguish local and IPC-fallback results:

```text
ProductOpenOutcome::Local {
    db: Arc<Database>,
    access_mode: AccessMode,
}

ProductOpenOutcome::Ipc {
    data_dir: PathBuf,
    socket_path: PathBuf,
    access_mode: AccessMode,
}
```

The outcome must not contain executor `IpcClient` or `IpcServer` values. IPC
transport remains executor-owned during `EG3`; engine only classifies whether
the product open should fall back to IPC.

The engine-owned error should distinguish at least:

- ordinary engine open failure
- cache open failure
- lock-without-IPC-socket failure

Default-branch bootstrap runs inside `Database::open_runtime`, which currently
does not expose a stable lifecycle phase in its error type. Until that changes,
default-branch failures surface as the source of ordinary disk/cache open
failures rather than through a separate product-open variant.

All public engine error/result enums added in this phase must be
`#[non_exhaustive]`.

### Transitional Subsystem Bridge

Until graph and vector move into engine, executor will need to build a narrow
bridge containing:

- `strata_graph::GraphSubsystem`
- `strata_vector::VectorSubsystem`
- `strata_engine::SearchSubsystem`

The bridge should contain only subsystem instances. It must not contain storage
keys, namespaces, type tags, storage errors, or raw storage handles.

Do not create a long-lived public `ProductSubsystemProfile` abstraction unless
the implementation proves there is no narrower route. Prefer the smallest
transitional input that can be deleted after graph/vector absorption. Executor's
temporary helper should be small and named for deletion, for example
`legacy_product_subsystems_until_eg5()`. The name should make the temporary
nature obvious in review.

## Section Status

| Section | Status | Deliverable |
| --- | --- | --- |
| `EG3A` | Complete | Product open/cache/follower/IPC characterization. |
| `EG3B` | Complete | Engine-owned product open API and result/error types. |
| `EG3C` | Complete | Bootstrap policy moved into engine product open. |
| `EG3D` | Complete | Executor cutover, legacy crate retirement, and guards. |

## Shared Commands

Legacy dependency inventory:

```bash
rg -n "strata_executor_legacy|strata-executor-legacy|executor-legacy" \
  crates src tests Cargo.toml docs/engine \
  -g '!target/**'
```

Retired legacy package guard:

```bash
if cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[].name' \
  | rg '^strata-executor-legacy$'; then
  echo "strata-executor-legacy is still a workspace package"
  exit 1
fi
```

Executor dependency check:

```bash
cargo tree -p strata-executor --edges normal --depth 1
```

Product open call-site inventory:

```bash
rg -n "Strata::open|Strata::open_with|Strata::cache|OpenOptions|AccessMode|IpcServer|strata.sock|strata.pid" \
  crates/executor crates/cli src tests \
  -g '*.rs'
```

Engine product-open substrate inventory:

```bash
rg -n "OpenSpec|open_runtime|seed_builtin_recipes|default_branch|ensure_default_branch|already in use by another process|OPEN_DATABASES" \
  crates/engine/src crates/executor/src \
  -g '*.rs'
```

Focused verification:

```bash
cargo test -p strata-executor
cargo test -p stratadb --test executor_ex6_runtime
cargo test -p stratadb --test engine_security_surface
cargo test -p stratadb --test storage_surface_imports
cargo check -p strata-engine
cargo check -p strata-executor
cargo fmt --check
```

After `EG3D`, `cargo test -p strata-executor-legacy` is intentionally gone
because the crate was deleted.

## EG3A - Product Open Characterization

### Goal

Pin the current product open behavior before moving the bootstrap shell.

### Work

- Add or extend tests that exercise `strata_executor::Strata` through the public
  executor surface rather than through `strata-executor-legacy` internals.
- Characterize default disk open:
  - local handle, not IPC
  - `AccessMode::ReadWrite`
  - runtime default branch resolves to `"default"` on a new database
  - writes work
  - close releases the local runtime for reopen
- Characterize read-only disk open:
  - local handle, not IPC
  - `AccessMode::ReadOnly`
  - reads work
  - writes are rejected through executor/session access-mode checks
- Characterize follower open:
  - existing disk database can be opened as follower
  - returned access mode is `ReadOnly`
  - writes are rejected
  - follower does not create missing default state
- Characterize cache open:
  - local handle, not IPC
  - `AccessMode::ReadWrite`
  - default branch is `"default"`
  - built-in recipes are available after open
- Characterize IPC fallback:
  - locked database plus socket returns an IPC-backed handle
  - IPC handle can create sessions and execute typed methods
  - locked database without socket returns the current actionable error message
- Characterize runtime default branch reuse:
  - a database opened with a persisted default branch such as `"main"` returns
    new handles rooted at that branch
  - `new_handle()` refreshes default branch for local and IPC handles
- Record any discovered behavior that differs from the current contract before
  moving code.

### Acceptance

`EG3A` is complete when:

- product open/cache/follower/read-only/IPC behavior is covered at the executor
  API level
- tests do not directly import `strata_executor_legacy`
- no product-open code has moved yet
- any untestable current behavior is listed explicitly in this document before
  `EG3B`

Status: complete.

Implemented in:

- [../../tests/executor_ex6_runtime.rs](../../tests/executor_ex6_runtime.rs)

The characterization coverage exercises the public `strata_executor::Strata`
surface and does not import `strata_executor_legacy` directly. It pins local
primary open, read-only primary open, follower runtime mode, follower
access-mode forcing, follower reads plus typed/session write rejection, cache
open, product subsystem registration order, built-in recipe seeding, IPC
fallback with a socket, read-only IPC fallback access-mode propagation, public
IPC `new_handle()` default-branch refresh, the IPC `database()` panic contract,
and the exact actionable lock error when no socket is present. The tests also
serialize this integration file's product-open cases so the deliberate
`OPEN_DATABASES` registry manipulation used for IPC simulation does not race
sibling tests.

The current public executor branch-existence command is not follower-safe: it
routes through an engine branch-index transaction and fails on a follower before
it can report `false`. Because of that existing behavior, `EG3A` records
follower non-bootstrap of a missing default branch with a narrow engine
observation on the local follower handle rather than a public executor
`BranchExists` assertion.

The seed-failure warning-only path is not forced in `EG3A`: there is no public
fault hook that makes `seed_builtin_recipes()` fail after database open succeeds
without changing product code. `EG3C` must preserve the warning-only behavior
when moving seeding into engine-owned product open, and should add an
engine-local characterization if a narrow fault hook already exists or is added
for that move.

`EG3A` also fixed an executor access-mode bug found while adding read-only IPC
coverage: typed methods on an IPC-backed `Strata` handle now reject write
commands client-side when the handle's selected access mode is `ReadOnly`.

## EG3B - Engine Product Open API

### Goal

Introduce the engine-owned product open API and typed outcome without cutting
executor over yet.

### Work

- Add a new engine module, preferably
  `crates/engine/src/database/product_open.rs`.
- Add the engine-owned outcome and error types.
- Do not add a long-lived subsystem-profile abstraction. If a temporary bridge
  is unavoidable, accept boxed engine `Subsystem` implementations through the
  narrowest possible API without naming graph or vector crates from engine.
- Add product open functions for:
  - disk open with `OpenOptions`
  - cache open
- Keep IPC transport out of engine. The engine outcome may return the socket
  path and selected access mode, but executor remains responsible for creating
  `IpcClient`.
- Move lock-to-IPC classification into engine:
  - primary open lock failure + socket exists => `ProductOpenOutcome::Ipc`
  - primary open lock failure + no socket => typed product-open error with the
    current user-facing message
- Avoid adding `strata-graph` or `strata-vector` dependencies to engine.
- Re-export the new product-open types from `strata_engine::database` and, if
  needed by executor ergonomics, from the engine crate root.
- Add the new public types to the D4 surface in [../../CLAUDE.md](../../CLAUDE.md).

### Acceptance

`EG3B` is complete when:

- engine has a product open API that can represent local and IPC-fallback
  outcomes
- executor still compiles through the old legacy path
- engine has no normal dependency on graph, vector, executor, intelligence, or
  CLI
- all new public engine error/result enums are `#[non_exhaustive]`
- focused engine checks pass

Status: complete.

Implemented in:

- [../../crates/engine/src/database/product_open.rs](../../crates/engine/src/database/product_open.rs)
- [../../crates/engine/src/database/mod.rs](../../crates/engine/src/database/mod.rs)
- [../../crates/engine/src/lib.rs](../../crates/engine/src/lib.rs)
- [../../CLAUDE.md](../../CLAUDE.md)

`EG3B` adds public `ProductOpenOutcome`, `ProductOpenError`, and
`ProductOpenResult` types plus crate-visible `open_product_database()` and
`open_product_cache()` functions. The functions are intentionally not
cross-crate public in `EG3B` because they do not yet run full product bootstrap:
default-branch policy and recipe seeding move in `EG3C`, and only then should
executor be able to cut over to them. The function shape accepts only boxed
engine `Subsystem` instances as the temporary EG3 bridge, so engine still does
not name or depend on graph/vector crates. The outcome can represent either a
local `Arc<Database>` or an IPC fallback marker containing the requested data
directory, socket path, and selected access mode; executor remains responsible
for constructing IPC transport.

Primary lock failure classification now exists in engine product open:

- lock failure with `<data_dir>/strata.sock` present returns
  `ProductOpenOutcome::Ipc`
- lock failure without the socket returns
  `ProductOpenError::LockedWithoutIpcSocket` with the current user-facing
  message

This section intentionally does not move product bootstrap policy yet. Default
branch selection and built-in recipe seeding remain in the legacy caller until
`EG3C`, so executor behavior is unchanged in `EG3B`.

## EG3C - Bootstrap Policy Move

### Goal

Make engine product open own default branch and built-in recipe bootstrap
policy.

### Work

- Build product primary/follower/cache `OpenSpec` values inside the engine
  product-open module.
- Product primary/cache specs must set default branch `"default"`.
- Product follower specs must not create default branch state.
- Product open must run built-in recipe seeding after successful primary/cache
  local opens.
- Recipe seeding failures must remain warn-only.
- Product open must preserve selected access-mode semantics:
  - normal primary uses the requested `OpenOptions::access_mode`
  - follower forces `AccessMode::ReadOnly`
  - cache returns `AccessMode::ReadWrite`
- Preserve the current default branch behavior. If engine `OpenSpec` already
  ensures the branch, do not add a second divergent branch-creation path.
  If a second idempotent ensure remains for behavior parity, keep it
  engine-local and explain why in a short comment.
- Update `OpenSpec` docs so product defaults are engine-owned. The docs should
  distinguish:
  - low-level runtime opening with explicit subsystems
  - product opening through the new product-open API
  - the temporary EG3 subsystem-instantiation bridge until graph/vector move
- Add focused engine tests for the new product-open module where possible.
  Executor-level characterization remains the main parity gate until executor
  cuts over in `EG3D`.

### Acceptance

`EG3C` is complete when:

- default branch and recipe seeding policy live in engine product open
- executor-legacy is no longer the only owner of bootstrap policy
- `OpenSpec` docs no longer say product defaults are executor-owned
- no storage behavior or physical format changes were made
- `cargo check -p strata-engine` passes

### Result

`EG3C` makes engine product open build primary/follower/cache `OpenSpec`
values. Primary and cache specs set the product default branch `"default"`;
follower specs intentionally do not request default-branch bootstrap. Built-in
recipe seeding now runs in engine product open after successful local primary
and cache opens, and seeding failures remain warning-only.

Default-branch creation still uses the existing `OpenSpec` bootstrap path. No
second product-open branch-creation path was added. The product-open functions
are now cross-crate public and re-exported from `strata_engine` for the `EG3D`
executor cutover. Focused engine tests cover default-branch bootstrap,
follower non-bootstrap, subsystem bridge ordering, recipe seeding success, and
warning-only recipe seeding failure.

Status: complete.

## EG3D - Executor Cutover And Legacy Edge Removal

### Goal

Make executor call engine product open directly and remove the normal
`strata-executor-legacy` edge.

### Work

- Replace `open_with_legacy_bootstrap()` and `cache_with_legacy_bootstrap()` in
  [crates/executor/src/compat.rs](../../crates/executor/src/compat.rs) with
  calls to engine product open.
- Map `ProductOpenOutcome::Local` to the existing local executor backend.
- Map `ProductOpenOutcome::Ipc` to the existing executor `IpcClient` connection
  path.
- Map `ProductOpenError` to the existing executor `Error` variants while
  preserving current user-facing messages.
- Keep `Strata::open`, `Strata::open_with`, `Strata::cache`, `Strata::database`,
  `Strata::access_mode`, `Strata::is_ipc`, `Strata::session`, and
  `Strata::new_handle` behavior stable.
- Remove `strata-executor-legacy` from `crates/executor/Cargo.toml`.
- If no normal dependents remain, delete `crates/executor-legacy` and remove it
  from the workspace.
- If deletion is not practical in the same slice, reduce it to a documented
  temporary shell with:
  - no normal workspace dependents
  - no graph/vector/storage dependencies unless explicitly justified
  - an `EG9` deletion criterion
- Add or extend a guard test so normal production code cannot reintroduce
  `strata-executor-legacy` as a dependency or import.
- Update [engine-crate-map.md](./engine-crate-map.md) after the graph changes.
- Update [engine-consolidation-plan.md](./engine-consolidation-plan.md) status
  once `EG3` is complete.

### Acceptance

`EG3D` is complete when:

- `strata-executor` has no normal dependency on `strata-executor-legacy`
- `cargo tree -i strata-executor-legacy --workspace --edges normal --depth 2`
  shows no executor edge, or the crate no longer exists
- production manifests/source are guarded against reintroducing the legacy edge
- executor open/cache/follower/IPC characterization tests pass
- CLI open/init/up tests still pass through `strata_executor::Strata`
- the transitional subsystem-instantiation helper is documented with its EG4/EG5
  deletion criterion

### Result

Executor now calls `strata_engine::open_product_database()` and
`strata_engine::open_product_cache()` directly from
[../../crates/executor/src/compat.rs](../../crates/executor/src/compat.rs).
`ProductOpenOutcome::Local` maps to the existing local `Executor` backend and
`ProductOpenOutcome::Ipc` maps to the existing executor-owned `IpcClient`
connection path. `ProductOpenError` maps to the existing executor `Internal`
error shape, preserving the current user-facing lock-without-socket message.

The temporary subsystem handoff is isolated in
`legacy_product_subsystems_until_graph_vector_absorption()`. It constructs the
current graph, vector, search subsystem order for engine product open while
graph and vector still live outside engine. Delete this helper after the graph
and vector absorption phases make engine able to build the full product runtime
internally.

`crates/executor/Cargo.toml` no longer depends on
`strata-executor-legacy`; the legacy crate was removed from the workspace and
its source files were deleted. The storage-surface guard now rejects
production manifest or source references to the retired package/import/path so
the edge cannot return silently.

Status: complete.

## EG3 Closeout

`EG3` is complete when:

- `EG3A`, `EG3B`, `EG3C`, and `EG3D` are complete
- executor no longer depends on `strata-executor-legacy`
- product open/bootstrap policy is engine-owned
- executor remains the owner of IPC transport and command/session behavior
- engine still has no graph/vector crate dependency
- the only remaining graph/vector product-open coupling is the documented
  temporary subsystem-instantiation bridge
- crate graph docs reflect the new graph
- guard coverage prevents accidental legacy-edge regression

Status: complete.

The next implementation phase after `EG3` is `EG4`.
