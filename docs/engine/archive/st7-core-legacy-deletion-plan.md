# ST7 Core-Legacy Deletion Plan

## Purpose

`ST7` is the final closeout step for the legacy-core split.

`ST6E` already removed `strata-core-legacy` from the active runtime graph. What
remains is not another ownership move. It is deletion preparation and cleanup:

- remove the compatibility shell itself
- remove the shell-local parity scaffolding
- resolve the deferred test files that still mention the old compat alias
- tighten the ratchets from “no new legacy edges” to “legacy must not exist”

This document turns the short `ST7` note in the storage plan into an explicit
deletion checklist based on the current tree.

Read this together with:

- [st6-legacy-core-dependency-severance-plan.md](./st6-legacy-core-dependency-severance-plan.md)
- [engine-pending-items.md](./engine-pending-items.md)
- [../storage/storage-minimal-surface-implementation-plan.md](../../storage/storage-minimal-surface-implementation-plan.md)
- [../core/core-minimal-surface-implementation-plan.md](../../core/core-minimal-surface-implementation-plan.md)

## Goal

Delete `crates/core-legacy` cleanly and leave no active or deferred code path
depending on the legacy import surface.

At the end of `ST7`:

1. `crates/core-legacy` is gone from the workspace.
2. No Cargo manifest in the workspace references `strata-core-legacy`.
3. No active or deferred source file references `strata_core_legacy_compat`.
4. Shell-local parity tests are gone with the shell.
5. Guardrails ban reintroduction of the deleted crate and alias.

## Current State

As of the post-`ST6E` tree:

- `cargo tree -i strata-core-legacy --workspace --depth 3` reports only the
  crate itself
- active runtime crates already import modern `strata-core`, `strata-storage`,
  and `strata-engine`
- `core-legacy` is a compatibility shell only
- the remaining work is repository cleanup, not graph surgery

That is the right precondition for deletion.

## Current Seam Map

### 1. The Shell Crate Still Exists

The crate is still a workspace member in:

- [Cargo.toml](../../Cargo.toml)

And still physically exists under:

- [crates/core-legacy/](../../../crates/core-legacy)

Even though it no longer has reverse dependents.

### 2. Shell-Local Compatibility Tests Still Exist

The shell is still pinned by:

- [crates/core-legacy/tests/compat_shell.rs](../../../crates/core-legacy/tests/compat_shell.rs)

That test is correct for `ST6E`, but it must disappear with the crate. Do not
move it somewhere else and preserve the shell story by accident.

### 3. Deferred Intelligence Tests Still Carry The Old Alias

The deferred intelligence harness is explicitly quarantined in:

- [tests/intelligence/main.rs.deferred](../../tests/intelligence/main.rs.deferred)

But these files still reference `strata_core_legacy_compat` today:

- [tests/intelligence/identity.rs](../../tests/intelligence/identity.rs)
- [tests/intelligence/hybrid.rs](../../tests/intelligence/hybrid.rs)
- [tests/intelligence/explainability.rs](../../tests/intelligence/explainability.rs)
- [tests/intelligence/stress.rs](../../tests/intelligence/stress.rs)
- [tests/intelligence/budget_semantics.rs](../../tests/intelligence/budget_semantics.rs)
- [tests/intelligence/search_all_primitives.rs](../../tests/intelligence/search_all_primitives.rs)
- [tests/intelligence/fusion.rs](../../tests/intelligence/fusion.rs)
- [tests/intelligence/architectural_invariants.rs](../../tests/intelligence/architectural_invariants.rs)
- [tests/intelligence/search_correctness.rs](../../tests/intelligence/search_correctness.rs)
- [tests/intelligence/indexing.rs](../../tests/intelligence/indexing.rs)
- [tests/intelligence/search_hybrid_orchestration.rs](../../tests/intelligence/search_hybrid_orchestration.rs)

These files are not in the active root test matrix today, but they are still
repo-visible source. `ST7` must either:

- repoint them to the settled engine/core search surface, or
- delete/retire them as outdated deferred scaffolding

What `ST7` must not do is delete the crate while leaving these files behind as
dead references to a non-existent alias.

### 4. The Guardrail Still Talks About A Quarantine

The current ratchet in:

- [tests/engine_surface_imports.rs](../../tests/engine_surface_imports.rs)

still allows `strata_core_legacy_compat` inside the deferred intelligence
quarantine. That is correct for `ST6E`, but after crate deletion that exception
must be removed entirely.

### 5. Docs Still Describe The Shell As Present

The living architecture docs now accurately describe `core-legacy` as
compatibility-only, but several of them still assume the shell remains present
until a final deletion step. `ST7` should rewrite those docs from “transitional
shell exists” to “shell deleted”.

The primary live docs to update are:

- [docs/engine/archive/engine-pending-items.md](./engine-pending-items.md)
- [docs/engine/archive/st6-legacy-core-dependency-severance-plan.md](./st6-legacy-core-dependency-severance-plan.md)
- [docs/storage/storage-minimal-surface-implementation-plan.md](../../storage/storage-minimal-surface-implementation-plan.md)

Historical baseline docs under `docs/core/` can continue to mention
`strata-core-legacy` where they are clearly describing the old state.

## Required Decision Before Deletion

There is only one real product question in `ST7`:

### What do we do with the deferred intelligence suite?

Recommended answer:

- repoint any still-valid tests to the settled `strata_engine` / `strata_core`
  search and identity surface
- delete the files that only make sense against the old DTO split

Why this is the right answer:

- those files are already out of the active matrix
- many of the referenced types now exist on the live engine/core surface
- keeping obviously stale dead references after deleting the crate makes the
  repository harder to trust

What not to do:

- do not preserve a fake compat alias somewhere else just to keep these files
  compiling unchanged
- do not move the quarantine exception from one ratchet to another

## Recommended Slice Order

`ST7` should land in two small slices.

### ST7A: Resolve Remaining Source-Level Legacy References

Goal:
- remove the remaining explicit `strata_core_legacy_compat` source references
  outside the shell crate

Scope:

- repoint or delete the deferred intelligence files listed above
- remove the quarantine language from
  [tests/intelligence/main.rs.deferred](../../tests/intelligence/main.rs.deferred)
  once no old alias remains
- tighten [tests/engine_surface_imports.rs](../../tests/engine_surface_imports.rs)
  from “compat is quarantined” to “compat must not appear anywhere”

Acceptance:

- `rg -n "strata_core_legacy_compat" crates tests` returns no source hits
  outside the crate that is about to be deleted, or no hits at all

Why this slice should come first:

- it makes the actual crate deletion mechanical
- it avoids deleting the crate while leaving broken dead source behind

### ST7B: Delete The Shell Crate

Goal:
- remove `core-legacy` from the workspace completely

Scope:

- remove `crates/core-legacy` from [Cargo.toml](../../Cargo.toml)
- delete `crates/core-legacy/`
- delete shell-local parity tests with it
- tighten the manifest ratchet so any new `strata-core-legacy` reference is a
  violation everywhere
- update the living docs so they no longer describe the shell as present

Acceptance:

- `cargo tree --workspace` contains no `strata-core-legacy`
- `rg -n "strata-core-legacy|strata_core_legacy_compat" Cargo.toml crates tests`
  returns only intentional historical-doc references, or none

## Verification Gates

The `ST7` gates should be narrower than `ST6`, because the work is now mostly
deletion and cleanup:

- `cargo check --workspace --all-targets`
- `cargo test -p strata-engine --quiet`
- `cargo test -p stratadb --test engine_surface_imports --quiet`
- `cargo test -p stratadb --test core_foundation_surface --quiet`
- `cargo test -p stratadb --test integration --quiet`

And the deletion-specific audits:

- `cargo tree --workspace | rg "strata-core-legacy"` should produce no match
- `rg -n "strata_core_legacy_compat" crates tests` should produce no match
- `rg -n "strata-core-legacy" Cargo.toml crates tests` should produce no active
  manifest/reference match

## Definition of Done

`ST7` is complete only when all of the following are true at once:

1. `crates/core-legacy` is gone from the workspace.
2. No workspace manifest depends on `strata-core-legacy`.
3. No active or deferred source file imports `strata_core_legacy_compat`.
4. The shell-local compatibility tests are gone rather than relocated.
5. The guardrails ban reintroduction of the deleted crate and alias.
6. The living engine/storage plans describe the settled post-legacy state.

## Non-Goals

- no new engine/domain ownership move
- no fresh error-architecture redesign
- no search API redesign beyond what is required to repoint or retire the
  deferred intelligence files
- no executor/bootstrap rewrite
