# CO5 Engine Domain And Primitive Eviction Plan

## Purpose

`CO5` is the second major subtraction epic in the core cleanup.

`CO4` removed storage ownership from `core`. `CO5` removes the remaining
engine-owned and primitive-owned behavior that still lives in legacy core.

This document turns the `CO5` section of the main core plan into an explicit
execution sequence so the work does not degrade into opportunistic code motion
or partial cleanup that leaves core with hidden engine authority.

Read this together with:

- [core-charter.md](./core-charter.md)
- [core-crate-map.md](./core-crate-map.md)
- [core-minimal-surface-implementation-plan.md](./core-minimal-surface-implementation-plan.md)
- [core-error-review.md](./core-error-review.md)
- [../engine/archive/engine-error-architecture.md](../engine/archive/engine-error-architecture.md)
- [../engine/archive/engine-pending-items.md](../engine/archive/engine-pending-items.md)

## CO5 Verdict

The following ownership decisions are closed:

- branch lifecycle/control/DAG schema -> `engine`
- replay/durability branch metadata -> `engine`
- limits and validation policy -> `engine`
- JSON path/patch/mutation helpers -> `engine`
- vector config behavior and model presets -> `engine`
- `Event` and `ChainVerification` -> `engine`
- runtime/search-facing value helpers such as `extractable_text()` -> `engine`

These are not audit-later items. They leave `core`.

## Why This Epic Matters

As long as these families remain in `core`, the architecture is still unstable:

- `core` still acts like a disguised engine domain crate
- `engine` cannot become the obvious owner of branch/runtime/primitive behavior
- the future engine restructuring starts from the wrong lower boundary
- `strata-core-legacy` continues to look necessary even after storage cleanup

`CO5` is therefore the move that stops core from being the hidden owner of
database-runtime semantics.

## In Scope

### Authoritative Ownership Move

The authoritative definitions and helper logic move out of these legacy-core
areas:

- [crates/core-legacy/src/branch.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/branch.rs)
- [crates/core-legacy/src/branch_dag.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/branch_dag.rs)
- [crates/core-legacy/src/branch_types.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/branch_types.rs)
- [crates/core-legacy/src/limits.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/limits.rs)
- [crates/core-legacy/src/primitives/event.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/primitives/event.rs)
- [crates/core-legacy/src/primitives/json.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/primitives/json.rs)
- [crates/core-legacy/src/primitives/vector.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/primitives/vector.rs)
- [crates/core-legacy/src/value.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/value.rs)

### Engine-Owned Surface After CO5

At the end of the epic, `engine` should be the clear owner of:

- branch/domain schema and lifecycle helpers
- runtime-facing limits and validation policy
- primitive-family behavioral helpers
- event-chain semantics
- search/runtime-facing helpers over foundational values

The exact internal module names can change, but the owner must be `engine`.

### Downstream Cutover

All downstream crates that currently import these symbols from `strata_core`
must be repointed to `strata_engine` or to narrow compatibility shims during
the transition.

## Out Of Scope

The following are explicitly not part of `CO5`:

- full engine crate restructuring into `kernel` / `runtime` / `primitives`
- deleting `strata-core-legacy`
- final `StrataError` definition move into `engine`
- storage/concurrency/durability crate merge
- redesigning primitive semantics during the move
- broad search workflow extraction out of executor

`CO5` only fixes engine-domain and primitive ownership.

## Current Source Of Truth

Today the moved surface is split like this:

### In Legacy Core

- `branch.rs`
- `branch_dag.rs`
- `branch_types.rs`
- `limits.rs`
- `primitives/event.rs`
- `primitives/json.rs`
- `primitives/vector.rs`
- compatibility helpers in `value.rs`

### In Downstream Crates

The current dependency surface reaches at least:

- `engine`
- `executor`
- `search`
- `graph`
- `vector`
- `intelligence`
- integration tests and helper modules across those crates

This means `CO5` must be executed as a deliberate workspace cutover, not as a
local module move.

One staging detail is intentional:

- `branch_domain` still acts as an engine-facing compatibility seam because
  `BranchRef` is staged behind the parent error move
- `semantics::json` and `semantics::vector` are already physically moved into
  `engine`
- `limits` and `semantics::event` already carry engine-side parallel
  definitions plus explicit legacy conversions

That split is deliberate. The first group is still waiting on the lower-layer
dependency rewrite before the physical definition move can happen cleanly. The
second group already needed engine-local behavior and tests that were easier to
express as engine-native definitions ahead of the final package-graph cleanup.

## Execution Strategy

`CO5` should run in six phases.

### Phase 1: Establish Engine-Owned Surface

Goal:
- establish the engine-owned homes for the moved families without mixing in
  large semantic redesign

Work:
- add engine-owned modules for:
  - branch/domain schema
  - limits/validation helpers
  - event semantics
  - JSON helper logic
  - vector helper logic
  - runtime/search-facing value helpers
- introduce the narrowest compatibility seams needed to preserve workspace
  stability during the wider cutover

Constraints:
- no broad engine crate reshaping in the same phase
- no primitive behavior changes mixed into the move
- no attempt to solve every engine cleanup item at once

Acceptance:
- `engine` has real homes for each moved family
- the engine-owned API is visible and coherent
- any remaining dependency on legacy core is isolated to explicit seam modules

### Phase 2: Move Branch-Domain Ownership First

Goal:
- remove branch lifecycle/control/DAG ownership from `core`

Work:
- move branch-domain schema and helpers into `engine`
- repoint engine and downstream callers to the engine-owned path
- keep behavior unchanged

Why this phase comes first:
- branch/domain schema is the clearest engine-owned family
- it shrinks legacy core without entangling primitive helper moves immediately

Acceptance:
- active branch-domain imports no longer treat `strata_core` as the owner
- legacy core holds at most narrow forwarding compatibility for these items

### Phase 3: Move Limits And Validation Policy

Goal:
- remove runtime-facing limits and validation policy from `core`

Work:
- move limit constants, validation helpers, and related policy to `engine`
- repoint imports across engine/executor/tests

Acceptance:
- `limits.rs` in legacy core is no longer an authoritative owner
- active callers import limits/validation policy from `engine`

### Phase 4: Move Primitive Helper Families

Goal:
- remove primitive helper logic from `core`

Primary targets:
- JSON helper logic
- vector behavior/config/model presets

Supporting targets:
- other primitive-family helpers that are not foundational DTOs

Work:
- move helper logic into engine-owned primitive modules
- repoint downstream imports
- do not redesign primitive semantics during the move
- if a lower-layer crate cannot depend upward on `engine` without a cycle,
  isolate that dependency as an explicit compatibility seam and document it

Acceptance:
- active JSON/vector helper imports no longer point at `strata_core`
- any remaining direct legacy-core dependency for these families is limited to
  explicit compatibility seams
- in the staged workspace, the expected remaining seams are:
  - `concurrency` for lower-layer JSON helpers
  - `engine::semantics::{json,vector}` as engine-facing compatibility modules
- the eventual physical definition move remains a later convergence step once
  those seams can be removed without introducing dependency cycles

### Phase 5: Move Event-Chain And Runtime/Search Helpers

Goal:
- remove the remaining obviously non-foundational behavior from `core`

Primary targets:
- `Event`
- `ChainVerification`
- runtime/search-facing value helpers such as `extractable_text()`

Work:
- move event-chain semantics into `engine`
- move remaining runtime/search value helpers into engine-owned runtime/search
  modules
- repoint engine/executor/search imports

Acceptance:
- event-chain semantics are engine-owned
- runtime/search-facing value helpers are engine-owned
- legacy core no longer presents these as foundational concerns

### Phase 6: Contract Legacy Core Ownership

Goal:
- contract legacy-core presentation and isolate any remaining CO5 seams

Work:
- convert remaining legacy-core definitions in scope to narrow forwarding shims
  when the dependency graph allows it
- update docs so legacy core no longer presents engine-domain or primitive
  ownership as canonical
- identify any remaining import sites as explicit debt
- where `core-legacy -> engine` would create a package cycle, contract
  presentation first and record the physical definition move as explicit
  follow-up work

Acceptance:
- active code no longer treats `core-legacy` as the owner of the CO5 families
- any remaining hosting inside `core-legacy` is documented as explicit debt
- any remaining forwarding is visibly transitional where forwarding is possible
- `CO6` can focus on final convergence rather than hidden ownership discovery

## Compatibility Rules

These rules are mandatory during the cutover.

### 1. Owner First, Cleanup Later

Do not mix ownership transfer with large semantic redesign. The point of `CO5`
is to move engine-owned behavior out of `core`, not to reinvent the engine in
the same patch.

### 2. Compatibility Shims Must Be Narrow

Temporary forwarding from legacy core is acceptable only as a compatibility
edge. It must not remain the normal import path for new code.

### 3. Engine Must Not Depend On Executor

Any solution that forces `engine` to import executor-facing policy is invalid.

### 4. No New Engine-Owned Behavior In Core

From the start of `CO5`, no new branch-domain helpers, primitive helper logic,
event-chain semantics, or runtime/search-facing value helpers may be added to
`strata-core` or `strata-core-legacy`.

### 5. Preserve Foundational/Core Distinction

If a type or helper is truly foundational, keep it in `core`. If it encodes
database runtime behavior or primitive semantics, move it to `engine`.

## Verification Expectations

Every phase should prove both owner cutover and behavioral stability.

Suggested checks:

1. import ownership audit
```bash
rg -n "strata_core(::|::primitives::|::branch|::branch_dag|::branch_types|::limits|::value::).*" crates tests
```

2. engine and executor suites
```bash
cargo test -p strata-engine --quiet
cargo test -p strata-executor --quiet
```

3. primitive-adjacent crates
```bash
cargo test -p strata-graph --quiet
cargo test -p strata-vector --quiet
cargo test -p strata-search --quiet
```

4. top-level targeted suites
```bash
cargo test -p stratadb --test storage --quiet
cargo test -p stratadb --test concurrency --quiet
```

If a future import-guard test like CO4’s becomes useful, prefer adding it
early rather than relying on manual grep forever.

## Done Definition

`CO5` is done when all of the following are true:

- `strata-core` no longer owns branch/domain behavior
- `strata-core` no longer owns limits/validation policy
- `strata-core` no longer owns primitive helper logic
- `strata-core` no longer owns event-chain semantics
- `strata-core` no longer owns runtime/search-facing value helpers
- active imports above core point at `engine`, not `strata_core`, for these
  families
- any remaining legacy-core forwarding is visibly compatibility-only

At that point, `CO6` can focus on final convergence and legacy-crate deletion
instead of discovering hidden engine authority still trapped in core.
