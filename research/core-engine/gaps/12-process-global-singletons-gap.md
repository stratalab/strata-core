# Gap 12: Process-Global Singletons Reduce Database-Instance Isolation

## Summary

Several important runtime behaviors are process-global rather than database-instance-local.
That means one database open can affect another database in the same process.

The biggest examples are:

- block cache capacity
- vector merge callbacks
- graph merge callback
- branch-DAG hooks

## What The Code Does Today

### Block cache is a global singleton

The storage crate defines:

- `GLOBAL_CACHE`
- `GLOBAL_CAPACITY`
- `set_global_capacity()`
- `global_cache()`

Code evidence:

- `crates/storage/src/block_cache.rs:879-905`

The database open path sets that global capacity before reads:

- disk-backed open sets it from config or auto-detect
- cache open can also set it

Code evidence:

- `crates/engine/src/database/open.rs:811-821`
- `crates/engine/src/database/open.rs:926-933`

### Branch-DAG hooks are process-global and first-call-wins

`BRANCH_DAG_HOOKS` is a `OnceCell` and the first registration wins.

Code evidence:

- `crates/engine/src/branch_ops/dag_hooks.rs:121-140`

### Vector and graph merge handlers are also process-global and first-call-wins

`VECTOR_MERGE_CALLBACKS` and `GRAPH_MERGE_PLAN_FN` are `OnceCell` registrations.

Code evidence:

- `crates/engine/src/branch_ops/primitive_merge.rs:902-928`
- `crates/engine/src/branch_ops/primitive_merge.rs:1063-1080`

## Why This Is An Architectural Gap

The engine isolates data by database path, but not all runtime behavior by database instance.

That produces hidden coupling between databases sharing a process:

- cache sizing is shared
- handler installation is shared
- embedding order affects all later databases

This is not automatically wrong for a single-tenant executable, but it is a real
architectural limitation for libraries, tests, multi-tenant runtimes, and mixed embedders.

## Concrete Consequences

### One database can resize the cache for all others

Opening a database with a different block-cache budget mutates the global capacity. That
changes the live cache behavior of every database using the singleton.

### Mixed embedders cannot safely install different merge semantics

Because handler registration is first-call-wins, one embedding can install the process-global
behavior and later embedders inherit it whether or not they intended to.

### Test isolation becomes weaker

Global state tends to create order-sensitive tests and harder-to-reason-about fixtures,
especially when tests open databases with different runtime expectations.

## Why This Is Separate From Gap 06

Gap 06 is about singleton reuse for the same database path.

This gap is broader:

- it affects different database paths
- it affects all instances in the process
- it comes from shared runtime singletons, not path deduplication

## Likely Remediation Direction

Inference from the current code shape:

1. Move cache ownership under a database runtime object, or at least under an explicit
   shared runtime that callers construct intentionally.
2. Replace process-global merge-hook registration with per-runtime capability tables that are
   attached to the database or its enclosing engine context.
3. Keep optional global defaults only as convenience, not as the only architecture.

## Bottom Line

The engine behaves like a per-process runtime in several important places, even though the
API surface mostly looks instance-oriented. That weakens isolation and makes embedding order
matter.
