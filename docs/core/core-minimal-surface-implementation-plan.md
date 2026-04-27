# Core Minimal Surface Implementation Plan

## Purpose

This document turns the core charter into a concrete rewrite plan.

The goal is not to preserve the legacy mixed-scope core implementation and
gradually trim
it by intuition. The goal is to arrive at a clean `strata-core` that has the
right scope:

- shared IDs
- shared values
- stable contract DTOs
- a very small set of inseparable low-level helpers

At the end of this plan, `strata-core` should have achieved its proper scope
and the legacy core crate should exist only as migration scaffolding or be
removable.

This plan should be read together with:

- [core-charter.md](./core-charter.md)
- [core-crate-map.md](./core-crate-map.md)
- [core-error-review.md](./core-error-review.md)
- [co4-storage-boundary-eviction-plan.md](./co4-storage-boundary-eviction-plan.md)
- [../architecture/architecture-recovery-target.md](../architecture/architecture-recovery-target.md)

## Rewrite Rules

These rules apply to every epic in this plan.

### 1. Core By Subtraction

The rewrite is primarily about moving things out of `core`, not inventing a
new feature surface inside it.

### 2. Shared Language, Not Shared Policy

If a type or helper encodes product behavior, runtime policy, primitive
semantics, or storage policy, it should leave `core`.

### 3. Bottom-Layer Changes Need High Scrutiny

Anything that stays in `core` becomes hard to move later because every other
layer depends on it. The bar for additions and retained behavior must be high.

### 4. Compatibility Is Proven From Above

The correctness gates for this rewrite are not just `core` unit tests. They are
also the engine, executor, CLI, and top-level regression suites that depend on
the cleaned foundation.

### 5. Do Not Freeze Accidental Ownership

The current crate contains a large amount of logic that is only in `core`
because the old boundaries drifted. The rewrite must not treat current location
as proof of rightful ownership.

## Current Starting Point

Today the legacy core implementation in `crates/core-legacy` still owns:

- foundational IDs and values
- contract DTOs
- a broad error taxonomy
- the `Storage` abstraction and `WriteMode`
- branch-control, DAG, and replay schema
- limits and validation policy
- primitive helper logic in JSON and vector modules
- event-chain semantics
- search/runtime-facing value helpers

This means `core` is currently both a foundational language crate and a
behavior host for lower and higher layers.

## End-State Definition

The new core is considered properly scoped when all of the following are true:

1. `strata-core` owns only stable shared domain language.
2. `strata-core` no longer owns storage abstractions.
3. `strata-core` no longer owns storage-layout types.
4. `strata-core` no longer owns branch/runtime domain schema.
5. `strata-core` no longer owns primitive algorithms.
6. `strata-core` no longer owns product or model policy.
7. The bottom-layer error surface is small and intentionally owned.
8. `strata-core` is small enough that its role is obvious:
   - roughly `<= 3K` LOC
   - roughly `5-6` modules
   - no workspace-crate dependencies

## Open Ownership Decisions Closed Up Front

The following ownership decisions are not deferred:

- `Storage` moves to `storage`
- `WriteMode` moves to `storage`
- `Key` moves to `storage`
- `TypeTag` moves to `storage`
- `Namespace` moves to `storage`
- branch lifecycle/control/DAG and replay schema move to `engine`
- limits and validation policy move to `engine`
- JSON path/patch/mutation helpers move to `engine`
- vector config behavior and model presets move to `engine`
- `Event` and `ChainVerification` move to `engine`
- `Value::extractable_text()` moves to `engine`
- `error.rs` is reduced only after the other ownership moves land

## Epic Structure

This plan is organized as six epics. The list below is the intended execution
order.

- `CO1` — Legacy Split And New Core Skeleton
- `CO2` — Foundational IDs, Values, And Contract Types
- `CO3` — Error Surface Reduction
- `CO4` — Storage Boundary Eviction
- `CO5` — Engine Domain And Primitive Eviction
- `CO6` — Final Core Convergence

Each epic has a narrow objective and explicit non-goals.

## CO1 — Legacy Split And New Core Skeleton

### Goal

Create a clean migration shape for `core` without forcing the rest of the
workspace to rewrite in one jump.

### Scope

- rename the current package to `strata-core-legacy`
- create a new `strata-core`
- establish a minimal `lib.rs` and module layout for the new crate
- keep only the narrowest temporary compatibility surface needed to keep the
  workspace building

### Deliverables

1. A new `strata-core` package with its own local module structure.
2. A clearly transitional `strata-core-legacy`.
3. Documentation describing which new-core modules are authoritative and which
   legacy exports are temporary.

### Constraints

- Do not treat wholesale re-export of legacy as the desired end state.
- Do not start moving runtime or primitive behavior into the new core.
- Do not restructure engine or storage in the same epic.

### Acceptance

- The workspace can depend on the new `strata-core` package name.
- The migration split is explicit.
- Future epics can move types into the new core intentionally instead of
  continuing to edit the old hornet's nest in place.

### Non-Goals

- no real scope cleanup yet
- no storage or engine boundary cleanup yet

## CO2 — Foundational IDs, Values, And Contract Types

### Goal

Define the true foundational language of Strata directly in the new core.

### Scope

Implement locally in the new crate:

- IDs:
  - `BranchId`
  - `TxnId`
  - `CommitVersion`
- canonical value model:
  - `Value`
- stable contract DTOs:
  - `EntityRef`
  - `PrimitiveType`
  - `Timestamp`
  - `Version`
  - `Versioned<T>`
  - `VersionedHistory`
  - `BranchName` if it survives as a pure contract type

### Deliverables

1. Local definitions for the true bottom-layer shared types.
2. Serialization and equality tests for those types.
3. Removal of temporary legacy forwarding for those definitions.

### Constraints

- Do not automatically carry over `types.rs` wholesale.
- Do not automatically retain the `primitives/` subtree in the new core.
- Do not keep helpers merely because many crates happen to use them today.

### Acceptance

- The new core can express the shared language of the stack without pulling in
  runtime, storage, or primitive behavior.
- The most commonly depended-on types are locally owned by the new crate.

### Non-Goals

- no large error rewrite yet
- no storage-trait move yet

## CO3 — Error Surface Reduction

### Goal

Reduce `core` error ownership to the minimal bottom-layer substrate that truly
belongs there by explicitly re-homing the broader system error model to the
right architectural layers while preserving one canonical engine-owned parent
error.

### Scope

- use [core-error-review.md](./core-error-review.md) as the ownership baseline
- use [error-research.md](./error-research.md) as the external design basis
- use [../engine/engine-error-architecture.md](../engine/engine-error-architecture.md)
  as the future engine-side implementation target
- audit the current `StrataError` and supporting error types
- identify which families belong in:
  - `core`
  - `storage`
  - `engine`
  - `executor`
- keep the new `strata-core` free of a new global operational error model
- define the target role of `StrataError` / `StrataResult` explicitly:
  - not core-owned
  - not universal internal currency
  - yes as the engine-owned parent error
- define the target role of `StorageError` explicitly:
  - yes as the storage-owned parent for persistence failures
  - no dependency on `engine`
- define the target role of executor error ownership explicitly:
  - yes as the public boundary and wire-policy owner
  - no reuse of `StrataError` as the public protocol error shape
- prepare later epics to move policy-bearing and domain-bearing error families
  out of `core-legacy`

### Deliverables

1. An explicit ownership decision record for the workspace error model.
2. A smaller, intentional error surface in the new `strata-core`.
3. Documentation of which error families are intentionally no longer owned by
   `core`.
4. A strategy for re-homing `StrataError` as the engine-owned parent error
   while keeping storage, executor, and leaf-local error ownership intact.
5. Freeze rules for the migration so new code does not deepen the legacy
   core-owned monolith while the move is in progress.

### Constraints

- Do not let “shared by many crates” be the only reason an error stays in core.
- Do not freeze product/runtime details in bottom-layer error variants.
- Do not rebuild `StrataError` inside the new `strata-core`.
- Do not require every niche subsystem to expose its own peer public top-level
  error contract.
- Do not treat CO3 as the final collapse of `error.rs`; CO4/CO5 should still
  make it materially smaller by removing whole owners.
- Do not overfit CO3 to one exact future wire-code encoding or one exact
  executor serialization format.
- Do not use CO3 to justify adding `anyhow` to library crates.
- Do not keep `StrataError` in `core` merely because the migration is
  inconvenient.

### Acceptance

- The new `strata-core` still does not own a global operational error model.
- `error.rs` in the new core is materially smaller and more intentional.
- The ownership target for `StrataError`, `StorageError`, executor-facing
  errors, and leaf-local errors is explicit.
- `StrataError` is explicitly targeted as the engine-owned parent error rather
  than as a core-owned universal internal result type.
- The intended boundary rule is explicit:
  - `storage` owns storage failures
  - `engine` owns the parent database/runtime error
  - `executor` owns the public boundary error
- The remaining legacy-core error surface is small enough that CO4/CO5 can
  remove whole error families instead of editing around a monolith.
- New code is frozen against deepening the old ownership model while the
  migration proceeds.

### Non-Goals

- no full engine error redesign in the same epic
- no claim that final error convergence is complete before CO4/CO5 land
- no claim that `StrataError` itself disappears during CO3
- no requirement to settle the final stable wire-code registry in the same
  epic

## CO4 — Storage Boundary Eviction

### Goal

Move storage-facing abstractions and policies out of `core`.

Detailed execution sequence:

- [co4-storage-boundary-eviction-plan.md](./co4-storage-boundary-eviction-plan.md)

### Scope

Remove from `core` ownership of:

- `Storage`
- `WriteMode`
- `Key`
- `TypeTag`
- `Namespace`
- other persistence-facing abstractions that belong to the future `storage`
  layer

These are not open audit items. They move to `storage`.

### Deliverables

1. Storage-facing traits and policies moved out of new `core`.
2. Keyspace/layout types moved out of new `core`.
3. Updated downstream dependencies so `engine` and `storage` no longer rely on
   `core` as a persistence abstraction layer.

### Constraints

- Do not finalize lower-layer crate merges in the same epic.
- Do not let storage layout concerns continue to masquerade as generic core
  vocabulary.
- Do not leave `Key`/`TypeTag`/`Namespace` in `core` as an “audit later” item.

### Acceptance

- `strata-core` no longer owns the storage abstraction layer.
- `strata-core` no longer owns unified-storage layout types.
- The future `storage` cleanup is no longer blocked on hidden ownership in
  core.

### Non-Goals

- no storage crate merge yet

## CO5 — Engine Domain And Primitive Eviction

### Goal

Move engine-domain schema, limits policy, and primitive helper behavior out of
`core`.

### Scope

Remove from core ownership of:

- branch lifecycle/control/DAG domain schema
- replay/durability branch metadata
- limits and validation policy
- JSON path/patch/mutation helpers
- vector config behavior and model presets
- `Event` and `ChainVerification`
- `Value::extractable_text()`
- other primitive-family helper logic that is not purely foundational

These are not open audit items. They move to `engine`.

### Deliverables

1. Branch-domain types moved to their real engine-owned home.
2. Limits/validation policy moved to `engine`.
3. Primitive helper logic moved out of `core`.
4. Event-chain semantics moved out of `core`.
5. Runtime/search-facing value helpers moved out of `core`.
6. A clear answer on whether any primitive DTOs remain in core and why.

### Constraints

- Do not leave large helper modules in `core` just to avoid touching engine.
- Do not move product/model presets into the new core under any circumstances.
- Do not defer `Event`, `ChainVerification`, or `extractable_text()` as
  “borderline” cases.

### Acceptance

- The new `core` no longer owns branch-control domain behavior.
- The new `core` no longer owns primitive algorithms.
- The new `core` no longer owns event-chain semantics.
- The new `core` no longer owns runtime/search-facing value helpers.
- The future `engine` rewrite can proceed on top of a clean lower boundary.

### Non-Goals

- no full engine rewrite yet

## CO6 — Final Core Convergence

### Goal

Finish the core cleanup so the workspace depends on the clean foundation rather
than on a mixed legacy crate.

### Scope

- remove the remaining temporary dependency on `strata-core-legacy`
- let `error.rs` collapse around what actually remains after CO4/CO5
- delete dead legacy-core modules
- update architecture docs to reflect the cleaned boundary
- run broad downstream verification and final sign-off

### Deliverables

1. Removal of core-side compatibility wiring.
2. Final collapse of error ownership around what actually remains in
   `strata-core`.
3. Final audit of what remains in `strata-core` and why.
4. Final test pass proving engine, executor, CLI, and top-level suites work on
   the cleaned core.

### Constraints

- Do not claim completion while major storage or engine policy still lives in
  the new core.
- Do not keep `strata-core-legacy` around as a second authoritative source of
  bottom-layer types.
- Do not let “it got smaller” count as completion if the remaining surface
  still violates the charter.

### Acceptance

- `error.rs` has collapsed around what actually remains in `core`.
- `strata-core` is a small foundational language crate:
  - roughly `<= 3K` LOC
  - roughly `5-6` modules
  - no workspace-crate dependencies
- `strata-core-legacy` is removable or clearly outside the normal build path.
- The next engine restructuring work can build on the clean core boundary.

### Non-Goals

- no requirement to finish the engine rewrite in the same epic sequence
- no requirement to finish the storage crate merge in the same epic sequence

## Suggested Move Order Inside The Epics

Within the epics above, the recommended move order is:

1. identifiers
2. value model
3. contract DTOs
4. error substrate
5. storage abstractions
6. keyspace/layout types
7. branch-domain schema
8. limits/validation
9. primitive helper logic
10. event-chain semantics
11. runtime/search-facing value helpers
12. final legacy-core deletion
10. dead-module deletion and final doc lock
