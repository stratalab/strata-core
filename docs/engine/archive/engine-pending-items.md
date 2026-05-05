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

- [engine-crate-map.md](../engine-crate-map.md)
- [st5-branch-bundle-lift-plan.md](./st5-branch-bundle-lift-plan.md)
- [st6-legacy-core-dependency-severance-plan.md](./st6-legacy-core-dependency-severance-plan.md)
- [st7-core-legacy-deletion-plan.md](./st7-core-legacy-deletion-plan.md)
- [engine-error-architecture.md](./engine-error-architecture.md)
- [../core/core-minimal-surface-implementation-plan.md](../../core/core-minimal-surface-implementation-plan.md)
- [../core/core-error-review.md](../../core/core-error-review.md)
- [../storage/storage-charter.md](../../storage/storage-charter.md)
- [../storage/storage-engine-ownership-audit.md](../../storage/storage-engine-ownership-audit.md)
- [../storage/storage-minimal-surface-implementation-plan.md](../../storage/storage-minimal-surface-implementation-plan.md)

## Current State

The current architecture is already pointing at `engine` as the right owner of
database/runtime semantics. The old `core-legacy` split is gone, and the
remaining engine work is now about tightening the settled runtime boundaries
rather than deleting transitional crates.

The most important example is `StrataError`:

- `engine` is now the public architectural owner
- downstream layers should import it from `strata_engine`
- the actual enum definition now physically lives in `engine`

## Pending Items

### 1. Make `storage` consistently own `StorageError`

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

### 2. Move product open/bootstrap policy into `engine`

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

### 3. Define the engine runtime/product pillar explicitly

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

### 4. Absorb the remaining core-owned engine domain types and helpers

Current state:

- branch-domain, limits, event/value helpers, JSON helpers, vector helpers,
  and the parent error now all physically live in `engine`
- active upper crates now depend on modern `strata-core` for foundational DTOs
- `core-legacy` is no longer an active owner of those families

Examples already identified in the core plan:

- branch lifecycle/control/DAG schema
- limits and validation policy
- JSON helper behavior
- vector helper behavior and model presets
- `Event`
- `ChainVerification`

Why it is still pending:

- the physical ownership move is done, but the compatibility shell still
  exists while parity and legacy-only DTO seams are being contracted

What is specifically still pending here:

- delete the temporary cross-owner parity seams once `core-legacy` is ready to
  disappear
- remove any remaining legacy-only DTO/test dependencies that are not part of
  the final compatibility story

### 4a. Finish the remaining branch/limits/event/value definition move

Current state:

- active code imports these families from `strata-engine`
- the physical definitions now also live in `engine`
- `core-legacy` is reduced to compatibility forwarding

Why it is still pending:

- the legacy shell still exists and should be deleted rather than left as a
  permanent forwarding layer

What completion looks like:

- the compatibility forwarders disappear with the final legacy-crate deletion

One staging detail here is intentional:

- `branch_domain` is now physically engine-owned, including `BranchRef`
- `semantics::json` and `semantics::vector` are now physically engine-owned
  definitions
- `limits`, `semantics::event`, and `semantics::value` are also physically
  engine-owned

That asymmetry is not accidental. The re-export seams are still waiting on the
last compatibility contraction. The definitions were pulled forward because
engine-local policy and behavior tests already needed a stable home before the
legacy shell could disappear.

What completion looks like:

- engine owns its own domain schema and primitive behavior
- core retains only foundational shared language

### 5. Implement the real engine-owned error system, not just the ownership handoff

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

### 6. Reduce executor-owned product workflow where engine should own it

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

### 7. Remove the final legacy forwarders after the engine boundary is ready

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
