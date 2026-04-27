# CO4 Storage Boundary Eviction Plan

## Purpose

`CO4` is the first core cleanup epic that changes the architectural ownership
of the system rather than just preparing for later moves.

The purpose of this document is to turn the `CO4` section of the main core
plan into an executable cutover sequence. The scope is narrow in concept but
wide in blast radius: a small set of storage-facing symbols currently owned by
legacy core are used throughout the workspace.

This document exists to prevent the epic from turning into a long series of
incremental import fixes with no stable definition of done.

Read this together with:

- [core-charter.md](./core-charter.md)
- [core-crate-map.md](./core-crate-map.md)
- [core-minimal-surface-implementation-plan.md](./core-minimal-surface-implementation-plan.md)

## CO4 Verdict

The following symbols are definitively storage-owned:

- `Storage`
- `WriteMode`
- `Key`
- `TypeTag`
- `Namespace`
- `validate_space_name`

These are not audit-later items. They leave `core`.

## Why This Epic Matters

As long as these symbols live in `core`, the architecture remains unstable:

- `storage` cannot be the obvious owner of the persistence substrate
- `engine` is forced to build on a false lower boundary
- `core` continues to act as a hidden storage kernel
- later crate reshaping will keep inheriting the wrong ownership model

`CO4` is therefore not a cosmetic module move. It is the move that stops
`core` from being the workspace's persistence abstraction layer.

## In Scope

### Authoritative Ownership Move

The authoritative definitions move out of:

- [crates/core-legacy/src/traits.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/traits.rs)
- [crates/core-legacy/src/types.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/core-legacy/src/types.rs)

And into `crates/storage`.

### Storage-Owned Surface After CO4

At the end of the epic, the storage crate should own a clear public surface
for:

- storage trait and result conventions
- unified storage layout types
- key constructors and helpers
- space-name validation tied to storage namespace rules

The likely end-state surface is:

- `strata_storage::Storage`
- `strata_storage::WriteMode`
- `strata_storage::Namespace`
- `strata_storage::TypeTag`
- `strata_storage::Key`
- `strata_storage::validate_space_name`

Internal module names can differ, but the owner must be `storage`.

### Downstream Cutover

All downstream crates that currently import these symbols from `strata_core`
must be repointed to `strata_storage` or to narrow compatibility shims during
the transition.

## Out Of Scope

The following are explicitly not part of `CO4`:

- merging `storage`, `concurrency`, and `durability`
- moving branch/domain schema
- moving limits and validation policy outside storage layout concerns
- moving JSON, vector, event, or graph helper logic
- full engine cleanup
- full legacy-core deletion

`CO4` only fixes storage-facing ownership.

## Current Source Of Truth

Today the moved surface is split like this:

### In Legacy Core

- `traits.rs`
  - `Storage`
  - `WriteMode`
- `types.rs`
  - `Namespace`
  - `TypeTag`
  - `Key`
  - `validate_space_name`

### In Downstream Crates

The current dependency surface reaches at least:

- `storage`
- `engine`
- `concurrency`
- `durability`
- `executor`
- `graph`
- `vector`
- `search`
- `intelligence`
- benches, tests, and helper modules across those crates

This means `CO4` must be executed as a deliberate workspace cutover, not as a
local file move.

## Execution Strategy

`CO4` should run in five phases.

### Phase 1: Establish Storage-Owned Surface

Goal:
- establish the storage-owned surface without changing behavior or type identity

Work:
- add storage-owned modules for:
  - key/layout types
  - storage trait
  - write mode
- introduce the narrowest compatibility seam needed to preserve existing type
  identity during the wider workspace cutover
- re-export the new owner surface from `crates/storage/src/lib.rs`

Constraints:
- no behavioral cleanup mixed into the move
- no format changes
- no new constructors or naming policy

Acceptance:
- `storage` can compile and test against its own owned surface
- the storage-owned API is visible and complete
- any remaining dependency on legacy core is isolated to the new surface modules

### Phase 2: Cut Storage Itself Over First

Goal:
- make `storage` self-hosted before touching the rest of the workspace

Work:
- repoint all `storage` crate source, tests, benches, and docs to use its own
  symbols
- remove internal `strata_core` imports for `Storage`, `WriteMode`, `Key`,
  `TypeTag`, `Namespace`, and `validate_space_name`

Why this phase comes first:
- `storage` must stop depending on `core` for its own substrate language
- it proves the new owner surface is real before the wider cutover begins

Acceptance:
- `crates/storage` no longer imports the moved symbols directly from `strata_core`
  outside the storage-owned surface modules
- the only remaining legacy-core dependency for the moved surface is the narrow
  compatibility seam that preserves type identity for later phases

### Phase 3: Cut Over Storage-Adjacent Lower Layers

Goal:
- repoint the crates that sit closest to storage semantics

Primary targets:
- `concurrency`
- `durability`

Secondary targets if they still consume the old surface directly:
- `graph`
- `vector`

Work:
- repoint imports
- keep behavior unchanged
- avoid mixing in engine-domain cleanup

Acceptance:
- these crates no longer treat `strata_core` as the source of storage layout
  and storage traits

### Phase 4: Cut Over Engine And Everything Above It

Goal:
- make `engine` and higher layers consume storage-owned symbols explicitly

Primary targets:
- `engine`
- `search`
- `intelligence`
- `executor`

Supporting targets:
- benches
- integration tests
- helper modules

Acceptance:
- all normal non-legacy imports of the moved symbols point at `strata_storage`

### Phase 5: Contract Legacy Core Ownership

Goal:
- remove legacy core as an authoritative owner of the moved surface

Work:
- convert legacy-core definitions into narrow forwarding shims if they are
  still temporarily needed
- update docs so legacy core no longer presents storage ownership as canonical
- identify remaining import sites as explicit debt if any are kept temporarily

Staging note:
- Phase 5 initially assumed `strata-storage` would still depend on
  `strata-core-legacy`, which would have blocked legacy-core forwarding without
  a package cycle
- the landed implementation took the cleaner route instead:
  `strata-storage` now depends only on new `strata-core`, which allows
  `strata-core-legacy` to depend upward on `strata-storage` as a thin
  compatibility forwarder
- the correct Phase 5 end state is therefore explicit forwarding from legacy
  core, plus documentation and import-surface contraction that make the
  compatibility role obvious

Acceptance:
- `core-legacy` is no longer the real home of storage abstractions
- any remaining forwarding is visibly transitional

## Compatibility Rules

These rules are mandatory during the cutover.

### 1. Owner First, Cleanup Later

Do not mix ownership transfer with semantic redesign. The point of `CO4` is to
change who owns the substrate types, not to reinvent them at the same time.

### 2. Compatibility Shims Must Be Narrow

Temporary forwarding from legacy core is acceptable only as a compatibility
edge. It must not remain the normal import path for new code.

### 3. Storage Must Not Depend On Engine

Any solution that forces `storage` to import `engine` is invalid.

### 4. Preserve On-Disk And Key Encoding Behavior

`TypeTag` values, `Key` ordering, `Namespace` formatting, and helper
constructors are format-sensitive. `CO4` is not allowed to change those
semantics.

### 5. No New Storage Surface In Core

Once the move starts, no new storage-facing symbol or helper may be added to
`core` or `core-legacy`.

## Verification Gates

`CO4` is only complete when all of the following are true.

### Ownership Gates

- `crates/storage` owns the authoritative definitions
- `crates/core` owns none of the moved surface
- `crates/core-legacy` is at most a forwarding shim for the moved surface

### Import Gates

- `rg` over the workspace shows no non-legacy imports of the moved symbols from
  `strata_core`
- `storage` itself imports none of the moved symbols from `strata_core`

### Behavior Gates

- storage unit suites remain green
- engine suites that exercise key construction, namespace behavior, and scans
  remain green
- executor and top-level integration suites remain green

### Documentation Gates

- the main core plan still reflects the actual post-CO4 state
- crate docs no longer imply that `core` owns persistence abstractions

## Suggested Verification Commands

At minimum:

```bash
cargo test -p strata-storage --quiet
cargo test -p strata-concurrency --quiet
cargo test -p strata-durability --quiet
cargo test -p strata-engine --quiet
cargo test -p strata-executor --quiet
cargo test -p stratadb --quiet
rg -n "strata_core::(traits|types)|strata_core::(Storage|WriteMode|Key|TypeTag|Namespace)" crates -g '*.rs'
```

The final `rg` should only leave intentionally transitional legacy references,
if any.

## Done Definition

`CO4` is done when:

1. `storage` is the real owner of the storage substrate symbols.
2. `core` is no longer the workspace source of persistence abstractions.
3. the remaining `core-legacy` surface, if any, is visibly compatibility-only.
4. the workspace builds and tests against the new ownership model.

At that point, the architecture stops snapping back to the old shape at the
bottom layer, and `CO5` can proceed on a much cleaner boundary.
