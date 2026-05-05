# ST6 Legacy Core Dependency Severance Plan

## Purpose

`ST6` is the last substantive ownership move before final crate deletion.

`ST2` through `ST5` removed the lower-runtime reasons that made
`strata-core-legacy` look structurally necessary. What remains is mostly an
engine-facing host problem:

- `engine` already owns the active public homes and physical definitions
- `core-legacy` is now a compatibility shell, not an active owner
- the remaining work is deleting or constraining the explicit compatibility
  seams so the shell can disappear cleanly

This document turns the high-level `ST6` section of the storage plan into an
explicit execution sequence grounded in the current codebase.

Read this together with:

- [engine-pending-items.md](./engine-pending-items.md)
- [engine-error-architecture.md](./engine-error-architecture.md)
- [../storage/storage-minimal-surface-implementation-plan.md](../../storage/storage-minimal-surface-implementation-plan.md)
- [../core/co5-engine-domain-and-primitive-eviction-plan.md](../../core/co5-engine-domain-and-primitive-eviction-plan.md)

## Goal

Break the remaining package and definition dependencies that still make
`crates/core-legacy` part of the runtime graph.

At the end of `ST6`:

1. `strata-engine` no longer depends on `strata-core-legacy`
2. upper runtime crates no longer depend on `strata-core-legacy`
3. engine-owned families are physically defined in `engine`
4. foundational DTOs come from modern `crates/core`
5. `core-legacy` is either an inert forwarder shell or obviously deletable in
   `ST7`

## Current Runtime Graph

As of the post-`ST6E` tree:

- `strata-storage` already depends on modern `crates/core`
- active upper runtime crates now also depend on modern `crates/core`
- `strata-core-legacy` is no longer on the active runtime path
- no active crate depends on `strata-core-legacy`, even in dev-only paths

That means the remaining `ST6` work is no longer a graph-breaker. It is now a
deletion-preparation problem.

## Current Seam Map

### Modern Core Already Owns

Modern `crates/core` already hosts the foundational shared language:

- [crates/core/src/id.rs](../../../crates/core/src/id.rs)
- [crates/core/src/branch.rs](../../../crates/core/src/branch.rs)
- [crates/core/src/value.rs](../../../crates/core/src/value.rs)
- [crates/core/src/contract/*](../../../crates/core/src/contract)

Those exports are now used by `storage`, `engine`, and the active upper crates.

### Core-Legacy Is Now A Pure Forwarder Shell

The remaining files under `crates/core-legacy` are no longer authoritative
owners. They now forward older compatibility paths to:

- modern `strata-core` for foundational DTOs and contracts
- `strata-engine` for branch/limits/error/primitive helpers
- `strata-storage` for storage layout and trait surfaces

### Engine Already Presents The Right Public Homes

The important thing is that the public surface is already mostly corrected:

- branch-domain seam:
  - [crates/engine/src/branch_domain.rs](../../../crates/engine/src/branch_domain.rs)
- limits:
  - [crates/engine/src/limits.rs](../../../crates/engine/src/limits.rs)
- primitive seams:
  - [crates/engine/src/semantics/json.rs](../../../crates/engine/src/semantics/json.rs)
  - [crates/engine/src/semantics/vector.rs](../../../crates/engine/src/semantics/vector.rs)
  - [crates/engine/src/semantics/event.rs](../../../crates/engine/src/semantics/event.rs)
  - [crates/engine/src/semantics/value.rs](../../../crates/engine/src/semantics/value.rs)
- parent error seam:
  - [crates/engine/src/error.rs](../../../crates/engine/src/error.rs)

`ST6` should therefore collapse these seams into real ownership rather than
inventing new module structure.

## Existing Guardrails

The repo already contains the ratchets that should drive the rest of `ST6`:

- [tests/engine_surface_imports.rs](../../tests/engine_surface_imports.rs)
  - now bans any new `strata-core-legacy` manifest edges outside
    `crates/core-legacy` itself
  - now bans new engine-surface backslides to legacy branch/error/json/vector
    import paths
- [tests/engine_error_surface.rs](../../tests/engine_error_surface.rs)
  - proves engine is the active parent-error owner
- [crates/core-legacy/tests/compat_shell.rs](../../../crates/core-legacy/tests/compat_shell.rs)
  - keeps the forwarding shell pinned against the modern core/engine owners
    while the crate still exists

These tests should tighten after each `ST6` slice. Do not leave them as
permanent “allowed seam” tests after the physical move lands.

## What ST6 Must Actually Change

There are two distinct classes of work:

### 1. Definition Moves

Physically move engine-owned families out of `core-legacy` and into `engine`.

This applies to:

- branch-domain schema
- limits/validation policy
- event semantics
- JSON path/patch helpers
- vector config/filter helpers
- runtime value helper `extractable_text()`
- `StrataError` and `PrimitiveDegradedReason`

### 2. Package/Import Cutovers

After the definition moves, runtime crates must stop depending on the legacy
package and split their imports correctly:

- foundational DTOs from modern `strata-core`
- engine-owned families from `strata-engine`

This second step is what makes `ST7` small instead of another architectural
rewrite.

## Recommended Slice Order

`ST6` should land in five slices.

### ST6A: Branch, Limits, Event, Value Physical Move

Goal:
- remove the easiest remaining engine-owned hosts from `core-legacy`

Move:

- `branch.rs`
- `branch_dag.rs`
- `branch_types.rs`
- `limits.rs`
- `primitives/event.rs`
- `value.rs::extractable_text`

Destination:

- expand [crates/engine/src/branch_domain.rs](../../../crates/engine/src/branch_domain.rs)
  into the authoritative branch-domain owner
- keep [crates/engine/src/limits.rs](../../../crates/engine/src/limits.rs) as the
  authoritative limits owner
- keep [crates/engine/src/semantics/event.rs](../../../crates/engine/src/semantics/event.rs)
  as the authoritative event owner
- keep [crates/engine/src/semantics/value.rs](../../../crates/engine/src/semantics/value.rs)
  as the authoritative runtime-value-helper owner

Required cleanup:

- remove `From<strata_core::...>` compatibility conversions where they become
  redundant
- tighten [tests/engine_surface_imports.rs](../../tests/engine_surface_imports.rs)
  so these files are no longer exempted
- update [tests/event_runtime_surface.rs](../../tests/event_runtime_surface.rs)
  from parity-with-legacy to true compatibility/backward-shape coverage

Why this slice is first:

- the destination modules already exist
- the behavior is narrower than JSON/vector
- it shrinks legacy ownership immediately without mixing in the parent error

### ST6B: JSON And Vector Helper Physical Move

Goal:
- remove the two heaviest primitive helper hosts from `core-legacy`

Move:

- [crates/core-legacy/src/primitives/json.rs](../../../crates/core-legacy/src/primitives/json.rs)
- [crates/core-legacy/src/primitives/vector.rs](../../../crates/core-legacy/src/primitives/vector.rs)

Destination:

- [crates/engine/src/semantics/json.rs](../../../crates/engine/src/semantics/json.rs)
- [crates/engine/src/semantics/vector.rs](../../../crates/engine/src/semantics/vector.rs)

Caller fallout already visible in the current tree:

- [crates/vector/src/filter.rs](../../../crates/vector/src/filter.rs)
  and [crates/vector/src/types.rs](../../../crates/vector/src/types.rs)
  needed to be repointed to the engine-owned helper surface
- root tests still needed their engine-surface seam checks tightened so direct
  legacy helper imports stop compiling unnoticed

Required cleanup:

- remove the remaining `semantics/json.rs` and `semantics/vector.rs` exemptions
  from [tests/engine_surface_imports.rs](../../tests/engine_surface_imports.rs)
- repoint any upper-crate imports of vector/json helper types to
  `strata_engine`, including legacy `strata_core::json::*` aliases
- add a temporary coexistence parity regression while both helper owners still
  live
- audit moved doctests and examples so they stop teaching legacy paths

Why this slice is separate:

- these are the largest surviving helper families
- they are the most likely to create avoidable mixed semantic/code-motion churn

### ST6C: Parent Error Host Move

Goal:
- move the canonical parent database/runtime error out of `core-legacy`

Move:

- [crates/core-legacy/src/error.rs](../../../crates/core-legacy/src/error.rs)

Destination:

- [crates/engine/src/error.rs](../../../crates/engine/src/error.rs)

This slice includes:

- `StrataError`
- `StrataResult<T>`
- `PrimitiveDegradedReason`
- the storage-lift boundary currently encoded in the legacy error host

Critical callsite families called out by the current audit:

- `graph`:
  - `StrataError` / `StrataResult`
- `vector`:
  - `StrataError`
  - `PrimitiveDegradedReason`
  - `StrataResult`
- `search` and `intelligence`:
  - `StrataResult`
- `executor` and `executor-legacy`:
  - direct `StrataError` handling and translation

Constraints:

- do not hide this move inside a giant “repoint all imports” patch
- do not move `StrataError` until the engine-owned families above are already
  physically stable
- do not recreate `storage -> engine` or `core -> engine` cycles via
  compatibility forwarding

What success looks like:

- engine owns the parent error definition
- upper crates import the parent error from `strata_engine`
- `core-legacy` no longer hosts the operational database/runtime error

### ST6D: Manifest And Foundational Import Cutover

Goal:
- stop using the legacy package as the convenience bucket for foundational
  types

Manifest cutovers needed:

- [crates/engine/Cargo.toml](../../../crates/engine/Cargo.toml)
- [crates/graph/Cargo.toml](../../../crates/graph/Cargo.toml)
- [crates/vector/Cargo.toml](../../../crates/vector/Cargo.toml)
- [crates/search/Cargo.toml](../../../crates/search/Cargo.toml)
- [crates/intelligence/Cargo.toml](../../../crates/intelligence/Cargo.toml)
- [crates/executor/Cargo.toml](../../../crates/executor/Cargo.toml)
- [crates/executor-legacy/Cargo.toml](../../../crates/executor-legacy/Cargo.toml)

Target split after the cutover:

- modern `strata-core` for:
  - `BranchId`
  - `CommitVersion`
  - `TxnId`
  - `Value`
  - `Timestamp`
  - `Version`
  - `Versioned`
  - `VersionedHistory`
  - `EntityRef`
  - `PrimitiveType`
- `strata-engine` for:
  - branch-domain families
  - limits
  - event semantics
  - JSON/vector helper families
  - `StrataError`
  - `PrimitiveDegradedReason`
  - search/request response types already owned by engine

Important fallout from the current audit:

- root and integration tests still contain legacy-core search/request imports
  that should become engine imports
- upper crates mostly no longer import forbidden engine-owned families directly;
  the remaining blocker is package ownership, not widespread type misuse

Guardrail to add in this slice:

- a manifest ratchet that bans new `strata-core-legacy` edges outside
  `crates/core-legacy` itself during the transition

### ST6E: Legacy Contraction

Goal:
- leave `core-legacy` in a clearly deletable state for `ST7`

Work:

- reduce any remaining legacy modules to the narrowest compatibility layer
  still required by tests or by the final deletion step
- update docs to stop describing `core-legacy` as an active owner
- tighten import guards so no new runtime crate can regress to legacy core

Acceptance for this slice:

- `cargo tree -i strata-core-legacy --workspace --depth 3` shows either:
  - no runtime dependents, or
  - only explicitly temporary seams already named in the `ST7` deletion step

## Explicit Non-Goals

`ST6` should not absorb unrelated work:

- no engine crate-wide `kernel / runtime / primitives` re-layout
- no executor protocol redesign
- no search product redesign
- no new lower-runtime abstractions in `storage`
- no opportunistic semantic redesign of JSON/vector/event behavior during the
  host move

## Verification Gates

Every slice should at minimum run:

- `cargo check --workspace --all-targets`
- `cargo test -p strata-engine --quiet`
- `cargo test -p stratadb --test engine_surface_imports --quiet`
- `cargo test -p stratadb --test engine_error_surface --quiet`

And once manifest cutovers begin:

- `cargo tree -i strata-core-legacy --workspace --depth 3`
- targeted crate tests for:
  - `strata-graph`
  - `strata-vector`
  - `strata-search`
  - `strata-intelligence`
  - `strata-executor`

## Definition Of Done

`ST6` is complete only when all of the following are true:

1. `strata-engine` no longer depends on `strata-core-legacy`.
2. No active runtime crate depends on `strata-core-legacy` for an engine-owned
   family.
3. Engine-owned families are physically defined in `engine`, not just
   re-exported from legacy core.
4. Foundational DTOs come from modern `strata-core`.
5. The engine and manifest ratchets no longer tolerate the temporary seam
   modules.
6. `ST7` is reduced to final legacy crate deletion and cleanup, not another
   ownership move.
