# Stage 5: Resource and Isolation Audit

This stage asks a different question from the runtime-spine work:

What state is actually isolated per branch, per database, or per process,
and where does Strata quietly share mutable behavior across those boundaries?

The main conclusion is that Strata has three distinct sharing layers:

- true process-global state
- per-`Database` state that becomes path-global because of `OPEN_DATABASES`
- on-disk local cache state shared by all runtimes pointed at the same data dir

That is not fatal, but it means "branch isolation" and "database isolation"
are only fully true at the storage-value layer. Above that layer, several
control and cache surfaces are shared more broadly than the API shape implies.

## Scope

Primary code paths inspected in this pass:

- `crates/engine/src/database/registry.rs`
- `crates/engine/src/database/mod.rs`
- `crates/engine/src/database/refresh.rs`
- `crates/engine/src/database/profile.rs`
- `crates/storage/src/block_cache.rs`
- `crates/executor/src/api/mod.rs`
- `crates/engine/src/branch_ops/primitive_merge.rs`
- `crates/engine/src/branch_ops/dag_hooks.rs`
- `crates/engine/src/primitives/branch/index.rs`
- `crates/engine/src/primitives/kv.rs`
- `crates/engine/src/system_space.rs`
- `crates/graph/src/keys.rs`
- `crates/graph/src/branch_dag.rs`
- `crates/graph/src/branch_status_cache.rs`
- `crates/vector/src/store/mod.rs`
- `crates/vector/src/recovery.rs`
- `crates/engine/src/search/recovery.rs`

## Executive summary

The most important Stage 5 findings are:

1. Strata has multiple process-global control registries:
   `OPEN_DATABASES`, the global block cache, merge-handler registration, and
   branch-DAG hook registration.
2. Database extensions are per-`Database`, not process-global, but primary
   same-path opens share the same `Arc<Database>`, so extension state becomes
   path-global and first-opener-defined.
3. Branch identity is split across deterministic `BranchId`s, random metadata
   UUIDs, and DAG/status state keyed by branch name strings.
4. Search and vector mmap/manifest state are not sources of truth, but they
   are shared local cache layers under the database directory and therefore
   part of the isolation model.
5. Namespace caches are process-global and have uneven invalidation rigor.
   KV and `_system_` caches are invalidated on delete; graph namespace cache
   does not have an equivalent production cleanup path.

## Global-state inventory

### 1. Path-global database singleton registry

`OPEN_DATABASES` is a process-global registry:

- key: canonicalized `PathBuf`
- value: `Weak<Database>`

This means primary opens to the same path within a process are not independent
database runtimes. They share:

- the same `Database`
- the same extensions
- the same scheduler
- the same runtime config object
- the same subsystem list

This is a deliberate design choice, but it makes runtime composition and
in-memory state path-global inside a process.

### 2. Global block cache

The storage block cache is genuinely process-global:

- `GLOBAL_CACHE: OnceLock<BlockCache>`
- `GLOBAL_CAPACITY: AtomicUsize`

Every database in the process shares:

- one cache instance
- one capacity setting

That means one database's `update_config()` can change block-cache capacity
for every other database in the process. This is the clearest confirmed
cross-instance resource leak in the current architecture.

### 3. Process-global merge and DAG registration

The merge / DAG behavior plane is also process-global.

Confirmed global registrations:

- executor `MERGE_HANDLERS_INIT: Once`
- engine `VECTOR_MERGE_CALLBACKS: OnceCell<_>`
- engine `GRAPH_MERGE_PLAN_FN: OnceCell<_>`
- engine `BRANCH_DAG_HOOKS: OnceCell<_>`

All of these are first-registration-wins.

That means semantic merge behavior and branch-DAG hook behavior are not
database-scoped. They are process-scoped. If one embedding or test harness
registers them first, later openers cannot choose a different behavior for a
different `Database` in the same process.

### 4. Process-global namespace caches

Strata also keeps several process-global namespace caches:

- KV: `crates/engine/src/primitives/kv.rs`
- system space: `crates/engine/src/system_space.rs`
- graph: `crates/graph/src/keys.rs`

These caches are lower risk than the control registries above, but they still
matter because they are keyed by branch and/or space and can outlive the
logical lifetime of a branch.

Invalidation rigor is uneven:

- KV has `invalidate_kv_namespace_cache(branch_id)` and production delete calls it
- `_system_` has `invalidate_cache(branch_id)` and production delete calls it
- graph only has `invalidate_namespace_cache(branch_id, space)`, and I found
  test usage but no equivalent production branch-delete cleanup

So even the "small" process-global caches are not governed uniformly.

### 5. Harmless process-global caches

`database/profile.rs` memoizes detected hardware in `CACHED_HW: OnceLock<_>`.
This is process-global, but it is read-only and benign compared to the mutable
registries above.

## Isolation boundary map

### Per-database extension state

`Database::extension<T>()` stores type-erased state inside the database
instance. This is not process-global by itself.

Important examples:

- `InvertedIndex`
- `VectorBackendState`
- `BranchStatusCache`
- `RefreshHooks`

This is a good pattern in isolation.
The architectural catch is that primary same-path opens share the same
`Database` via `OPEN_DATABASES`, so extension state becomes path-global in
practice for primaries in the same process.

That means:

- extension lifetime is tied to the shared `Database` object
- extension composition is first-opener-wins for that path
- wrapping an existing `Arc<Database>` preserves all existing extension state

So these extensions are not "per handle" or "per session" state.
They are "per live database object" state.

### Refresh hooks are per-database but opportunistic

`RefreshHooks` is a database extension containing a vector of registered hook
objects. This is better than a process-global hook table, but it still has two
important isolation properties:

- hooks are installed only if the relevant subsystem recovery path runs
- hook presence therefore depends on runtime composition, not just on the
  existence of persisted data

Vector recovery registers its refresh hook through this mechanism.
Search does not; search refresh is engine-hardcoded in `Database::refresh()`.

So the extension/hook model is only partially the authority for follower
incremental behavior.

### Branch identity is not single-source-of-truth

Branch identity spans multiple incompatible keys:

1. deterministic engine `BranchId` from `resolve_branch_name(name)`
2. random `branch_id` string stored in `BranchMetadata`
3. DAG nodes keyed by branch name string
4. `BranchStatusCache` keyed by branch name string

This matters because "same branch name" and "same branch identity" are not
equivalent concepts across the stack.

The sharp consequence is that deleting and recreating a branch name is not a
clean identity reset:

- deterministic branch resolution reuses the same executor-side `BranchId`
- DAG history and status cache are name-keyed
- metadata also stores a separate random UUID

So lineage, status, and storage identity do not all advance together.

This is the same seam already seen in the branch-DAG audit, but Stage 5 makes
the isolation consequence explicit: branch name reuse can inherit stale
non-storage state.

### Follower state is isolated in memory but shared on disk

Follower mode is a separate runtime shape with:

- its own `Database`
- no lock file
- no WAL writer
- its own extension population path

That gives followers memory isolation from the primary runtime inside the same
process when they are distinct objects.

But followers still share the same on-disk secondary state under `data_dir`,
including:

- WAL files
- storage segment manifests
- search manifest and sealed `.sidx` mmap files
- vector `.vec` mmap caches and graph-cache directories

Those files are intended as persisted state or rebuild accelerators, not as
independent sources of truth, but they still form a shared local resource
plane between primary and follower runtimes.

### Search and vector caches are local shared state, not truth

Search and vector both use on-disk accelerator state:

- search: `data_dir/search/search.manifest` plus mmap-loaded sealed segments
- vector: `data_dir/vectors/{branch_hex}/{space}/{collection}.vec` plus graph
  cache directories

These caches are better than zombie stacks because they sit under the live
recovery paths, but they still matter for isolation:

- they are shared by every runtime pointed at the same path
- they can be stale or missing independently of storage truth
- correctness depends on recovery/fallback behavior, not on isolation alone

Search is relatively well-contained because its recovery path explicitly
treats the mmap state as a cache and falls back to KV rebuild.

Vector is more delicate because the runtime also maintains in-memory backend
state, refresh hooks, and collection-specific mmap caches.

## Confirmed scaling and isolation risks

### High: one database can resize another database's block cache

Because the block cache is process-global, any call path that sets global
capacity affects all databases in the process.

Confirmed writers include:

- open-time sizing in `open.rs`
- hot config application in `Database::apply_storage_config_inner()`
- cache-mode setup in `Database::cache()`

This is not just observability drift. It is a real shared-resource control
plane.

### High: merge semantics are process-scoped, not database-scoped

Because vector merge callbacks, graph merge planners, and branch DAG hooks are
stored in process-global `OnceCell`s, merge behavior cannot vary safely per
database instance within one process.

This is especially awkward because subsystem recovery is database-scoped, while
merge and DAG behavior are process-scoped.

### High: primary same-path opens are compositionally shared

Because `OPEN_DATABASES` returns the existing `Arc<Database>`, the following
state becomes path-global and first-opener-owned:

- extension population
- subsystem list
- scheduler instance
- runtime config object
- refresh-hook registration

This is the same root cause behind first-opener-wins composition drift.
Stage 5 frames it as an isolation issue instead of only a composition issue.

### High: branch name reuse is not a clean lifecycle reset

Because DAG and status state are keyed by branch name while storage uses
branch IDs and metadata stores another UUID, branch deletion and recreation can
leave stale lineage/status interpretations attached to a reused name.

### Medium: graph namespace cache cleanup is incomplete

Graph namespace caching is process-global, but unlike KV and `_system_`
namespace caches, I did not find a production branch-delete cleanup path that
invalidates graph namespace entries.

This is mostly a memory-lifetime issue, not a correctness failure, but it
shows inconsistent isolation hygiene across primitives.

### Medium: local mmap/manifest caches form a secondary shared resource plane

Search and vector caches are not a second source of truth, but they are shared
local state under the database path. That means any audit of isolation has to
treat them as part of the runtime, not just as opaque implementation details.

### Low: hardware profile memoization is process-global but benign

`CACHED_HW` is worth noting for completeness, but it is not a practical
architectural gap.

## What Is Actually Well Isolated

Stage 5 is not all bad news. Some boundaries are reasonably clean:

- storage values themselves are still branch-scoped by namespace and `BranchId`
- database extensions are at least tied to a concrete `Database`, not a global
  singleton
- follower runtimes are memory-separate when they are separate objects
- search recovery explicitly treats mmap state as cache and falls back to KV

So the isolation model is not broken everywhere.
The problem is that the higher-level control and cache planes are much more
shared than the primitive APIs imply.

## Conclusions

The shortest accurate summary of Stage 5 is:

- data isolation is strongest in the storage layer
- runtime-behavior isolation is much weaker
- resource isolation is weakest where mutable singletons exist

The architectural pattern is:

- storage tries to be branch-scoped
- extensions try to be database-scoped
- product composition and several control hooks are still process-scoped

That is survivable, but it makes scaling, embedding, and multi-database
operation much riskier than the crate boundaries suggest.

## Priority fixes from this stage

1. Make block-cache capacity database-scoped or explicitly introduce a
   first-class shared-cache manager instead of an ambient singleton.
2. Move merge and DAG registration into database-scoped runtime composition,
   or at minimum make the process-global policy explicit and validated.
3. Unify branch identity so storage, metadata, and DAG/status layers do not
   key the same lifecycle on different concepts.
4. Treat same-path singleton sharing as part of the public runtime contract:
   either reject composition mismatches loudly or make the runtime bundle
   immutable and explicit.
5. Normalize namespace-cache cleanup across primitives, especially graph.
6. Document search/vector mmap and manifest artifacts as shared local cache
   planes, not as invisible implementation details.
