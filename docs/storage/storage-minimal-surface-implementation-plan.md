# Storage Minimal Surface Implementation Plan

## Purpose

This document turns the storage charter into a concrete consolidation plan.

The goal is not to preserve the current three-crate lower-runtime split and
trim it opportunistically. The goal is to arrive at a clean storage/runtime
layer with the right scope:

- one persistence substrate
- one generic transaction runtime
- one durability runtime
- one recovery/replay runtime
- one on-disk format owner
- no primitive-specific semantics below `engine`

At the end of this plan:

- `strata-storage` should own the full lower persistence/runtime substrate
- `strata-engine` should own the primitive/domain semantics that leaked into
  lower layers
- `strata-concurrency` should be deleted
- `strata-durability` should be deleted
- `strata-core-legacy` should be deleted

This plan should be read together with:

- [storage-charter.md](./storage-charter.md)
- [storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md)
- [st2-primitive-transaction-semantics-extraction-plan.md](./st2-primitive-transaction-semantics-extraction-plan.md)
- [st3-generic-transaction-runtime-absorption-plan.md](./st3-generic-transaction-runtime-absorption-plan.md)
- [storage-crate-map.md](./storage-crate-map.md)
- [concurrency-crate-map.md](./concurrency-crate-map.md)
- [durability-crate-map.md](./durability-crate-map.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)
- [../core/core-minimal-surface-implementation-plan.md](../core/core-minimal-surface-implementation-plan.md)
- [../engine/engine-pending-items.md](../engine/engine-pending-items.md)
- [../architecture/architecture-recovery-target.md](../architecture/architecture-recovery-target.md)

## Rewrite Rules

These rules apply to every epic in this plan.

### 1. Substrate By Consolidation, Not By Naming

The work is not done when the right crates import the wrong code through a
different path. The work is done when the lower runtime is physically owned by
`storage` and the temporary split crates are removable.

### 2. No Primitive Semantics Below `engine`

`storage` may own generic persistence mechanics. It must not own:

- JSON path semantics
- JSON patch semantics
- event-chain meaning
- vector metric/model policy
- branch-domain workflow policy

If lower layers currently know those things, the rewrite must remove that
knowledge instead of normalizing it.

### 3. No Silent Semantic Redesign

This workstream is about ownership and graph correction first. Do not mix in
behavior changes unless they are required by correctness and explicitly called
out.

### 4. No Format Churn Without Cause

Moving WAL, snapshot, manifest, and codec ownership into `storage` does not by
itself justify changing on-disk formats. Compatibility should be preserved
unless a later, explicit format migration says otherwise.

### 5. Compatibility Is Proven From Above

The correctness gates are not just unit tests in `storage`. They are also:

- engine suites
- executor suites
- intelligence suites
- top-level integration and regression suites

The consolidation is only valid if the upper layers still behave correctly.

### 6. Do Not Deepen Transitional Dependencies

While this plan is in progress:

- no new crate may depend on `strata-concurrency`
- no new crate may depend on `strata-durability`
- no new runtime dependency may be added on `strata-core-legacy`
- no new primitive semantics may be added to lower runtime crates

## Current Starting Point

Today the lower runtime of the workspace has already been consolidated into
`strata-storage`, and the final `core-legacy` compatibility shell has been
deleted.

At the same time:

- `storage` already owns the physical keyspace, storage boundary, and MVCC
  engine
- `storage` now also owns the generic transaction runtime plus the WAL payload
  and durability commit adapter
- `storage` now also owns the recovery/replay runtime
- `concurrency` has been deleted in `ST4D`
- `durability` has been deleted in `ST5C`
- `engine` now owns the extracted JSON/event transaction semantics from `ST2`
- `engine` now owns the branch-bundle workflow from `ST5`
- `engine` no longer depends on `strata-concurrency`
- `core-legacy` has been deleted in `ST7`

The remaining text below is the historical execution plan that produced this
settled graph.

## End-State Definition

The storage workstream is complete only when all of the following are true:

1. `strata-storage` owns the full lower persistence/runtime substrate.
2. `strata-storage` owns:
   - storage layout
   - MVCC state
   - transaction runtime
   - durability runtime
   - replay/recovery runtime
   - on-disk formats
3. `strata-engine` owns:
   - JSON path/patch semantics
   - event continuity semantics
   - branch bundle import/export
   - remaining engine-domain families still hosted by `core-legacy`
4. No lower runtime crate depends on `strata-core-legacy`.
5. `strata-concurrency` is deleted.
6. `strata-durability` is deleted.
7. `strata-core-legacy` is deleted.
8. The resulting stack is legible:
   - `core`
   - `storage`
   - `engine`
   - `intelligence`
   - `executor`
   - `cli`

## Ownership Decisions Closed Up Front

The following ownership decisions are not deferred:

- generic transaction state moves to `storage`
- generic OCC validation moves to `storage`
- generic commit coordination moves to `storage`
- WAL payload and replay plumbing move to `storage`
- WAL/snapshot/manifest/layout/codec/compaction move to `storage`
- on-disk format ownership moves to `storage`
- JSON path/patch buffering and overlap policy move to `engine`
- JSON write materialization policy moves to `engine`
- event continuity bookkeeping moves to `engine`
- `branch_bundle` moves to `engine`
- remaining engine-owned families still living in `core-legacy` move to
  `engine` before the plan closes

## Epic Structure

This plan is organized as seven epics. The list below is the intended
execution order.

- `ST1` — Boundary Freeze And Dependency Baseline
- `ST2` — Primitive Transaction Semantics Extraction
- `ST3` — Generic Transaction Runtime Absorption
- `ST4` — Durability Runtime Absorption
- `ST5` — Branch Bundle Lift (completed)
- `ST6` — Legacy Core Dependency Severance
- `ST7` — Final Crate Convergence And Deletion

Each epic has a narrow objective and explicit non-goals.

## ST1 — Boundary Freeze And Dependency Baseline

### Goal

Lock the intended ownership boundaries before the large graph rewrite begins.

### Scope

- document the current dependency and ownership state
- make the allowed transitional seams explicit
- add or tighten dependency/import guards where needed
- freeze new use of `concurrency`, `durability`, and `core-legacy` in active
  code

### Deliverables

1. Baseline crate maps for:
   - `storage`
   - `concurrency`
   - `durability`
2. A baseline crate map for `engine` where it intersects the storage boundary.
3. A storage↔engine ownership audit that classifies current cross-boundary
   misplacements.
4. A storage-side implementation plan that defines the execution order.
5. Guardrails preventing new lower-layer ownership drift during the rewrite.

### Constraints

- Do not start the code merge before the allowed seams are explicit.
- Do not treat the current graph as proof of rightful ownership.

### Acceptance

- the current split is documented and frozen
- new code cannot casually deepen the transitional graph
- later epics have an agreed ownership baseline to execute against

### Non-Goals

- no runtime consolidation yet
- no crate deletion yet

## ST2 — Primitive Transaction Semantics Extraction

### Goal

Remove engine-shaped primitive semantics from `concurrency` before any lower
runtime merge into `storage`.

Detailed execution plan:

- [st2-primitive-transaction-semantics-extraction-plan.md](./st2-primitive-transaction-semantics-extraction-plan.md)

### Scope

Move upward into `engine`:

- JSON path read tracking
- JSON patch buffering
- JSON path overlap policy
- JSON write materialization policy
- event continuity bookkeeping currently attached to transaction context

Leave in lower layers only:

- generic reads
- generic writes
- generic deletes
- generic CAS
- generic snapshot/version tracking

### Deliverables

1. An engine-owned transaction-semantic layer for JSON/event behavior.
2. Removal of primitive-aware JSON helpers from the lower transaction runtime.
3. Upper-layer tests proving JSON transactional behavior still works through
   the engine boundary.

### Constraints

- do not push primitive semantics downward into `storage`
- do not use this epic to redesign the public executor command surface
- do not change JSON behavior unless required by correctness

### Acceptance

- `concurrency` no longer owns JSON path/patch conflict semantics
- `concurrency` no longer owns event continuity semantics
- lower transaction machinery is primitive-agnostic again

### Non-Goals

- no full merge into `storage` yet
- no WAL/snapshot move yet

## ST3 — Generic Transaction Runtime Absorption

### Goal

Move the generic transaction runtime from `concurrency` into `storage`
without prematurely dragging WAL and replay ownership across the durability
boundary.

Detailed execution plan:

- [st3-generic-transaction-runtime-absorption-plan.md](./st3-generic-transaction-runtime-absorption-plan.md)

### Scope

Move into `storage`:

- transaction context state
- generic conflict and validation logic
- lock-ordering and commit-safety contracts
- generic manager state
- version allocation and visibility tracking
- branch commit locks
- quiesce and deletion safety
- WAL-independent commit coordination

Do not move in `ST3`:

- WAL record envelope serialization
- recovery coordination
- snapshot / manifest / codec plumbing
- other code that still depends directly on `strata-durability`

Repoint active callers above the lower layer to the storage-owned runtime
surface.

### Deliverables

1. Storage-owned modules for the generic transaction runtime.
2. A storage-owned public surface for transaction state and generic OCC.
3. A storage-owned manager core separated from the WAL/recovery adapter path.
4. Active engine/graph/vector callers cut over to `storage` for generic
   transaction runtime types.

### Constraints

- do not carry the JSON/event seams back down while merging
- do not create a premature `storage -> durability` edge
- do not preserve `concurrency` as a renamed peer crate
- do not change commit semantics without explicit justification

### Acceptance

- transaction state and generic OCC validation are physically defined in
  `storage`
- the generic manager core is physically defined in `storage`
- active callers no longer rely on `concurrency` for generic transaction
  state or validation
- `concurrency` is reduced to a temporary durability shell that becomes
  deletable once `ST4` lands

### Non-Goals

- no branch-bundle move yet
- no recovery/payload move yet
- no final crate deletion yet

## ST4 — Durability Runtime Absorption

### Goal

Move the substrate durability runtime from `durability` into `storage`.

Detailed execution plan:

- [st4-durability-runtime-absorption-plan.md](./st4-durability-runtime-absorption-plan.md)

### Scope

Move into `storage`:

- WAL writer/reader/configuration
- durability modes
- snapshot writer/reader/loading
- checkpoint coordination
- manifest management
- codec abstraction
- database directory layout
- compaction/tombstone support
- persistent byte formats for WAL/manifests/snapshots/writesets

Keep `branch_bundle` out of this move and treat it separately.

### Deliverables

1. Storage-owned WAL runtime.
2. Storage-owned snapshot/runtime format modules.
3. Storage-owned layout/codec/compaction modules.
4. Upper-layer callers repointed off `durability` for substrate concerns.

### Constraints

- do not mix branch-domain import/export concerns into the storage move
- do not change disk format behavior without an explicit migration decision

### Acceptance

- substrate durability runtime is physically owned by `storage`
- active code no longer needs `durability` for WAL/snapshot/format concerns
- `durability` contains only the remaining branch-bundle workflow or is ready
  for deletion once that workflow moves

### Non-Goals

- no branch-bundle move in this epic
- no final `core-legacy` deletion yet

## ST5 — Branch Bundle Lift

### Goal

Move the branch-bundle workflow out of the lower runtime and into `engine`.

Detailed execution plan:

- [../engine/st5-branch-bundle-lift-plan.md](../engine/st5-branch-bundle-lift-plan.md)

### Scope

Move into `engine`:

- bundle manifest/info DTOs that are branch-domain facing
- branch archive read/write workflow
- branch export/import checksums and presentation rules
- branchlog archive payload helpers

Keep lower durability helpers in `storage` and use them from above instead of
letting branch workflows live beside them.

### Deliverables

1. Engine-owned branch-bundle modules and tests.
2. Upper-layer callers cut over to the engine-owned bundle workflow.
3. Removal of branch-bundle ownership from the lower durability layer.

### Constraints

- do not recreate a pseudo-durability layer inside `engine`
- do not broaden branch-bundle into a general archive subsystem

### Acceptance

- branch-bundle import/export is clearly engine-owned
- no branch-domain archive workflow remains in the lower runtime

### Non-Goals

- no broad branch-domain redesign here
- no executor protocol redesign here

## ST6 — Legacy Core Dependency Severance

Detailed execution plan:

- [../engine/st6-legacy-core-dependency-severance-plan.md](../engine/st6-legacy-core-dependency-severance-plan.md)

### Goal

Break the dependency chain that still makes `core-legacy` structurally
necessary.

### Scope

Use the cleaned lower graph to finish the remaining owner moves that could not
land while `engine`, `concurrency`, and `durability` were still entangled.

That includes physically moving out of `core-legacy` the remaining
engine-owned families such as:

- branch-domain families
- limits/validation policy
- event/chain verification
- runtime/search-facing value helpers
- JSON/vector helper implementations that were previously blocked by lower
  cycles
- the engine-owned parent error if it still remains in legacy at that point

### Deliverables

1. No runtime dependency from `storage` or `engine` to `core-legacy`.
2. Remaining engine-owned families physically defined in `engine`.
3. Legacy forwarding reduced to the minimum needed for final removal, or
   removed entirely.

### Constraints

- do not stop at import-path cleanup; this epic is about physical ownership
- do not leave `core-legacy` structurally required by a hidden lower seam

### Acceptance

- the workspace runtime graph no longer needs `core-legacy`
- remaining legacy surfaces are either gone or obviously ready for deletion
- the core workstream is unblocked for final convergence

### Non-Goals

- no fresh foundational-scope expansion in `core`
- no standalone legacy preservation

## ST7 — Final Crate Convergence And Deletion

Detailed execution plan:

- [../engine/st7-core-legacy-deletion-plan.md](../engine/st7-core-legacy-deletion-plan.md)

### Goal

Delete the temporary split crates and close the workstream.

### Scope

- remove remaining compatibility forwarders
- repoint any final imports or Cargo dependencies
- delete:
  - `strata-core-legacy`
- refresh documentation to describe the settled architecture rather than the
  migration path

### Deliverables

1. A final workspace graph with no dependency on the deleted crates.
2. Updated docs reflecting the settled storage/engine/core split.
3. Guardrails that prevent those deleted boundaries from being reintroduced.

### Constraints

- do not leave empty shells behind just to avoid changing dependency stanzas
- do not preserve dead compatibility layers after the graph is clean

### Acceptance

- the three transitional crates are gone from the workspace
- `cargo tree` shows the intended stack clearly
- the test suite proves the consolidated graph still behaves correctly

### Non-Goals

- no unrelated executor or CLI redesign
- no broad product-policy rewrite beyond what this consolidation requires

## Program Verification Gates

Each epic should prove correctness from both below and above.

The recurring verification gates for this workstream should include:

- `cargo check --workspace --all-targets`
- `cargo test -p strata-storage --quiet`
- `cargo test -p strata-engine --quiet`
- `cargo test -p strata-executor --quiet`
- `cargo test -p strata-intelligence --quiet`
- `cargo test -p stratadb --tests --quiet`

And, where appropriate, dependency and ownership audits such as:

- `cargo tree -p strata-storage --depth 2`
- `cargo tree -i strata-storage --workspace --depth 2`
- `cargo tree -p strata-engine --depth 2`
- `rg`-based import/dependency guards for forbidden surfaces

The exact guard commands can evolve, but the principle must not:

- no new primitive semantics below `engine`
- no new runtime dependencies on the transitional crates
- no silent ownership regression once a surface has moved

## Final Deletion Gate

This plan is not complete until all of the following are true at once:

1. `strata-storage` is the only lower persistence/runtime crate.
2. `strata-engine` owns the remaining primitive/domain semantics that belong
   above storage.
3. `cargo tree --workspace` shows no package dependency on:
   - `strata-concurrency`
   - `strata-durability`
   - `strata-core-legacy`
4. Active code and tests no longer rely on transitional import surfaces.
5. The broad regression suites above storage remain green.

That is the real closeout condition for this workstream.
