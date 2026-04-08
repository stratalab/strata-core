# Composition Paths: Raw Engine vs Builder vs Executor vs Follower

This is a focused composition audit for four runtime shapes:

- raw engine open
- builder open
- executor/product open
- follower open

The purpose is to compare what these paths actually construct, not what their
API names imply.

## Scope

Primary code paths inspected:

- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/builder.rs`
- `crates/executor/src/api/mod.rs`
- `crates/executor/src/executor.rs`

## Short verdict

These are not four cosmetic wrappers over the same product.
They are four materially different runtime compositions:

- raw engine open = engine convenience path with hardcoded `SearchSubsystem`
- builder open = explicit subsystem-composed engine runtime
- executor open = builder open plus process-global merge/DAG registration and
  product bootstrap
- follower open = separate read-only runtime architecture with replay/refresh
  semantics

The most important conclusion is:

**Builder is the real composition primitive, executor is the real product
policy, and raw engine open is a reduced convenience path.**

## Comparison matrix

| Path | Entry | Subsystems | Merge / DAG registration | Post-open bootstrap | Lock / WAL writer | Runtime shape |
|---|---|---|---|---|---|---|
| Raw engine | `Database::open*` | Hardcoded `[SearchSubsystem]` | None | None | Exclusive `.lock`, WAL writer present | Primary |
| Builder primary | `DatabaseBuilder::open*` | Explicit caller-supplied list | None | None | Exclusive `.lock`, WAL writer present | Primary |
| Executor primary | `Strata::open_with` (non-follower, local path) | Standard builder list: `VectorSubsystem`, `SearchSubsystem` | Yes, process-global `Once` registration before open | `init_system_branch`, recipe seeding, default-branch ensure/verify, executor thread startup | Exclusive `.lock`, WAL writer present | Primary product runtime |
| Raw follower | `Database::open_follower` | Hardcoded `[SearchSubsystem]` | None | None | No lock, no WAL writer | Follower |
| Builder follower | `DatabaseBuilder::open_follower` | Explicit caller-supplied list | None | None | No lock, no WAL writer | Follower |
| Executor follower | `Strata::open_with(... follower=true)` | Standard builder list: `VectorSubsystem`, `SearchSubsystem` | Yes, same process-global registration | Wrap in read-only executor, deliberately skip default-branch verification | No lock, no WAL writer | Follower product runtime |

## Path-by-path

### 1. Raw engine open

`Database::open()` and `Database::open_with_config()` end up in:

1. `open_internal(...)`
2. `open_internal_with_subsystems(...)`

But `open_internal(...)` hardcodes:

- `vec![Box::new(crate::search::SearchSubsystem)]`

So raw engine open is not "engine with default subsystems".
It is:

- engine primary open
- WAL recovery
- storage recovery
- space metadata repair
- `SearchSubsystem` recovery only

What it does not do:

- vector recovery
- merge-handler registration
- branch-DAG hook registration
- `_system_` branch initialization
- built-in recipe seeding
- default-branch ensure/verify
- executor startup

This is the clearest reason raw engine open and product open are different
products.

### 2. Builder primary open

`DatabaseBuilder` is the cleanest composition mechanism in the codebase.

It lets the caller declare an explicit ordered subsystem list, and that same
list is used for:

- recovery on open, in registration order
- freeze on shutdown/drop, in reverse order

This is stronger than raw engine open because there is no hidden hardcoded
subsystem list beyond what the builder caller supplies.

However, builder open is still only an engine composition primitive.
It does not add:

- process-global merge/DAG registration
- system-branch bootstrap
- recipe seeding
- branch policy checks
- executor lifecycle

So builder primary open is the canonical engine composition path, but not the
full product runtime.

### 3. Executor / product primary open

`Strata::open_with()` is not just a wrapper around engine open.
Its local-primary path does several extra things:

1. `ensure_merge_handlers_registered()`
2. `strata_db_builder()`
3. builder open with standard subsystem list
4. `strata_graph::branch_dag::init_system_branch(&db)`
5. `strata_engine::recipe_store::seed_builtin_recipes(&db)`
6. wrap in `Executor::new_with_mode(...)`
7. ensure or verify the default branch depending on access mode

The standard subsystem list is:

- `VectorSubsystem`
- `SearchSubsystem`

So executor open is the first path that actually composes the runtime Strata
appears to want to ship as the normal product.

Important additional executor-owned behavior:

- merge and branch-DAG behavior is installed before database open through
  process-global registration
- `Executor::new_with_mode(...)` starts the embed refresh thread

So executor open is not only "builder plus convenience". It is builder plus
global runtime policy plus product bootstrap.

### 4. Follower open

Follower open is not just primary open with writes disabled.

Its runtime shape is different at construction time:

- no `.lock` acquisition
- no WAL writer
- `durability_mode` on the `Database` is set to `Cache` as an internal
  placeholder
- read-only WAL recovery path
- later incremental catch-up via `refresh()`

Raw follower open mirrors raw primary open in one important way:

- it hardcodes only `SearchSubsystem`

Builder follower open mirrors builder primary open:

- explicit subsystem list is the sole recovery driver

Executor follower open mirrors executor primary composition in one important
way:

- it still uses the standard builder list (`VectorSubsystem`, `SearchSubsystem`)

But it deliberately does less product bootstrap:

- it does not run `init_system_branch`
- it does not seed built-in recipes
- it deliberately skips default-branch verification because the normal
  `BranchExists` path would route through transaction machinery that rejects
  follower commits

So follower is compositionally closer to "separate runtime architecture" than
"mode bit".

## The important seams

### Raw engine vs builder

This seam is about subsystem authority.

Raw engine open:

- hardcodes search only

Builder open:

- uses the caller's explicit subsystem list

So builder is the only engine path that can honestly claim explicit runtime
composition.

### Builder vs executor

This seam is about product policy.

Builder decides:

- which subsystems recover and freeze

Executor additionally decides:

- whether process-global merge and DAG hooks are installed
- whether system branch infrastructure is initialized
- whether built-in recipes exist
- whether default-branch policy is enforced
- whether an embed refresh thread exists

So executor, not engine, currently owns the full "normal product runtime"
definition.

### Primary vs follower

This seam is about architecture, not access mode.

Follower differs in:

- lock ownership
- WAL writer presence
- recovery shape
- refresh model
- shutdown behavior
- bootstrap policy in executor

So follower correctness and composition must be reasoned about separately from
primary correctness.

### First-opener-wins still applies

Primary open paths share one additional seam:

- `open_internal_with_subsystems(...)` returns an existing `Database`
  unchanged if the path is already open in `OPEN_DATABASES`

That means a later builder or executor open does not recompose the runtime.
It inherits whatever subsystem/extension state the first opener created.

This is why raw engine open followed by product open is dangerous:

- the product opener can receive a search-only runtime and continue as if the
  standard subsystem set exists

## Final verdict

The composition hierarchy is:

1. **Raw engine**: reduced convenience path
2. **Builder**: canonical engine composition primitive
3. **Executor**: canonical product policy layer
4. **Follower**: separate runtime architecture available through raw, builder,
   and executor entry shapes

If Strata wants one authoritative runtime definition, it needs to decide
whether that authority belongs in:

- engine + builder
- or executor

Right now the answer is:

- engine owns object construction
- builder owns subsystem-list composition
- executor owns product semantics

That split is workable, but only if raw engine open is clearly positioned as a
reduced low-level path and composition mismatches are made explicit instead of
silently inheriting first-opener state.
