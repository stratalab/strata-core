# Engine Pending Items

## Purpose

This document is the working ledger of engine-related cleanup items that are
known, intentional, and still pending.

It exists because several recent refactors were done in stages on purpose:

- ownership moved before definitions moved
- public boundaries were corrected before lower dependencies were rewritten
- cleanup work was deferred until the engine itself becomes the active rewrite
  target

This document makes those staged decisions explicit so they do not look like
accidental leftovers later.

It should be read together with:

- [engine-error-architecture.md](./engine-error-architecture.md)
- [../core/core-minimal-surface-implementation-plan.md](../core/core-minimal-surface-implementation-plan.md)
- [../core/core-error-review.md](../core/core-error-review.md)
- [../storage/storage-charter.md](../storage/storage-charter.md)

## Current State

The current architecture is already pointing at `engine` as the right owner of
database/runtime semantics, but several concrete implementations still live
elsewhere because the dependency graph is not ready for the final move yet.

The most important example is `StrataError`:

- `engine` is now the public architectural owner
- downstream layers should import it from `strata_engine`
- but the actual enum definition still physically lives in `core-legacy`

That is intentional. Similar staged states are likely to appear in later
engine cleanup work.

## Pending Items

### 1. Move the `StrataError` definition from `core-legacy` into `engine`

Current state:

- `strata_engine` re-exports `StrataError` and `StrataResult`
- the actual definition still lives in `crates/core-legacy/src/error.rs`

Why it is still pending:

- `storage` still depends on the legacy parent error
- moving the definition today would either create a `storage -> engine`
  dependency or force a larger boundary rewrite in the same step

What completion looks like:

- `StrataError` is physically defined in `engine`
- `core` no longer owns or forwards the parent database/runtime error
- `core-legacy` no longer acts as the host of the operational monolith

### 2. Make `storage` consistently own `StorageError`

Current state:

- `StorageError` exists and is the right storage-local error type
- but storage still has legacy conversions into `StrataError`
- many storage APIs still return `StrataResult`

Why it is still pending:

- older call chains still assume the parent error is available at the lower
  boundary

What completion looks like:

- `storage` returns `StorageResult<T>` consistently at its own layer boundary
- storage no longer depends on `StrataError`
- storage-to-engine conversion becomes explicit and semantic

### 3. Move product open/bootstrap policy into `engine`

Current state:

- product open policy still lives outside the intended engine boundary
- the final `executor-legacy` bootstrap seam depends on that fact

Why it is still pending:

- the engine runtime/product layer has not been formalized yet

What completion looks like:

- open/bootstrap policy lives under an engine-owned runtime/product area
- executor no longer depends on a legacy bootstrap shell
- the final executor legacy seam can be deleted cleanly

Representative policy that should end up in engine:

- product open defaults
- cache/follower/product mode behavior
- startup policy around lock/socket/IPC handoff
- recipe seeding and similar product boot composition

### 4. Define the engine runtime/product pillar explicitly

Current state:

- we know `engine` should own:
  - database kernel
  - product runtime/bootstrap
  - primitives
- but that three-pillar structure is not yet reflected cleanly in the crate

Why it is still pending:

- the engine crate is still the old mixed shape

What completion looks like:

- the engine crate has an explicit internal structure for:
  - kernel
  - runtime/product
  - primitives
- executor can dispatch into engine without having to host product/runtime
  behavior itself

### 5. Absorb the remaining core-owned engine domain types and helpers

Current state:

- several engine-domain or primitive-domain items still live in `core-legacy`
  because `CO4` and `CO5` are not done yet
- branch-domain and limits ownership have already moved in active code
- JSON/vector active callers above the lower seam now import through
  `strata-engine`
- but the physical JSON/vector helper implementations still live in
  `core-legacy`

Examples already identified in the core plan:

- branch lifecycle/control/DAG schema
- limits and validation policy
- JSON helper behavior
- vector helper behavior and model presets
- `Event`
- `ChainVerification`

Why it is still pending:

- these moves belong to the later core cleanup phases and the engine rewrite,
  not to `CO1`–`CO3`
- the remaining primitive-family move is now blocked by the current dependency
  graph:
  - `engine` depends on `core-legacy`
  - `core-legacy` cannot depend back on `engine` without a cycle
  - `concurrency` is a lower layer and still depends directly on the legacy
    JSON helper surface

What is specifically still pending here:

- move the physical JSON helper implementation out of
  `crates/core-legacy/src/primitives/json.rs`
- move the physical vector helper/config/preset implementation out of
  `crates/core-legacy/src/primitives/vector.rs`
- remove the lower-layer `concurrency -> core-legacy` JSON helper seam
- replace the current `engine::semantics::{json,vector}` forwarding layer
  with truly engine-owned implementations

### 5a. Finish the remaining branch/limits/event/value definition move

Current state:

- active code imports these families from `strata-engine`
- `core-legacy` no longer presents them as the canonical home
- but the physical definitions for several of those families still live in:
  - `crates/core-legacy/src/branch.rs`
  - `crates/core-legacy/src/branch_dag.rs`
  - `crates/core-legacy/src/branch_types.rs`
  - `crates/core-legacy/src/limits.rs`
  - `crates/core-legacy/src/primitives/event.rs`
  - `crates/core-legacy/src/value.rs`

Why it is still pending:

- `engine` still depends on `core-legacy`
- `core-legacy` cannot depend back to `engine` without a package cycle
- the current state is a presentation and import-ownership contraction, not a
  full physical definition inversion

What completion looks like:

- the physical definitions move into `engine` or the final owning crate
- `core-legacy` stops being the host of these engine-domain families
- the remaining root exports become removable rather than just non-canonical

One staging detail here is intentional:

- `branch_domain`, `semantics::json`, and `semantics::vector` are still
  engine-facing re-export seams
- `limits` and `semantics::event` already exist as engine-side parallel
  definitions with explicit conversions to and from the older surface

That asymmetry is not accidental. The re-export seams are still waiting on the
dependency rewrite that removes the lower-layer cycles. The parallel
definitions were pulled forward because engine-local policy and behavior tests
already needed a stable home before the final physical move.

What completion looks like:

- engine owns its own domain schema and primitive behavior
- core retains only foundational shared language

### 6. Implement the real engine-owned error system, not just the ownership handoff

Current state:

- the ownership direction is documented
- `engine` is the public import surface for the parent error
- but the actual engine-native error model is not yet implemented

What is still pending:

- decide the final `StrataError` variant layout
- decide the nested reason-type structure
- implement retryability and ambiguous-commit semantics explicitly
- implement stable classification at the engine layer
- replace legacy core-hosted helpers/conversions with engine-owned ones

What completion looks like:

- the engine error system matches [engine-error-architecture.md](./engine-error-architecture.md)
- `StrataError` is truly engine-native, not just re-exported from legacy core

### 7. Reduce executor-owned product workflow where engine should own it

Current state:

- the executor boundary is much cleaner than before
- but some workflow-heavy behavior is still likely above the ideal engine
  boundary

Known candidate:

- search workflow composition

Why it is still pending:

- this is not required to complete the immediate executor cleanup
- it is better handled once engine runtime/product structure exists

What completion looks like:

- executor stays a thin boundary
- workflow-heavy product/runtime behavior moves down into engine where
  appropriate

### 8. Remove the final legacy forwarders after the engine boundary is ready

Current state:

- several current states are intentionally transitional:
  - engine-owned `StrataError` symbol backed by a legacy definition
  - executor cleanup blocked on product open policy move

Why it is still pending:

- these deletions depend on the engine cleanup items above

What completion looks like:

- no engine-critical behavior is hosted in legacy crates
- legacy forwarders and bootstrap shims are removable rather than merely thin

## Intended Order

Not every item above has to land in one sequence, but the broad dependency
order is:

1. define the engine runtime/product pillar
2. move product open/bootstrap policy into engine
3. make storage own `StorageError` cleanly
4. move `StrataError` definition into engine
5. absorb remaining engine-domain and primitive-domain ownership from core
6. remove final legacy forwarders and seams

The search-workflow question can be handled either during or after that work,
depending on how invasive the runtime/product cleanup becomes.

## Rules While These Items Are Pending

Until the engine cleanup starts in earnest:

1. Do not move new product/runtime logic into executor if engine is the right
   eventual owner.
2. Do not add a replacement parent error type to `core`.
3. Do not make `storage` depend on `engine`.
4. Do not mistake transitional forwarders for final ownership.
5. Prefer explicit staging notes like this document over silent architectural
   debt.
