# Core Crate Map

## Purpose

This document is a baseline map of the pre-split core implementation, which now
lives in `crates/core-legacy`.

It is not a target design. It is a description of the legacy ownership and
behavior that the new `strata-core` rewrite is unwinding.

For the target boundary of the clean rewrite, see
[core-charter.md](./core-charter.md).

For the staged rewrite plan, see
[core-minimal-surface-implementation-plan.md](./core-minimal-surface-implementation-plan.md).

For the broader CO3 error-ownership review, see
[core-error-review.md](./core-error-review.md).

The important takeaway is that the legacy core implementation is not just a
minimal shared-language crate. It is currently:

- the foundational ID and value vocabulary
- a large cross-layer error taxonomy
- a storage abstraction layer
- a branch-domain schema crate
- a host for primitive-family helper logic
- a validation and limit-policy crate

That is why it had to be split before the deeper `engine` rewrite.

## High-Level Shape

Legacy top-level source files:

- [crates/core-legacy/src/lib.rs](../../crates/core-legacy/src/lib.rs)
- [crates/core-legacy/src/error.rs](../../crates/core-legacy/src/error.rs)
- [crates/core-legacy/src/types.rs](../../crates/core-legacy/src/types.rs)
- [crates/core-legacy/src/value.rs](../../crates/core-legacy/src/value.rs)
- [crates/core-legacy/src/traits.rs](../../crates/core-legacy/src/traits.rs)
- [crates/core-legacy/src/limits.rs](../../crates/core-legacy/src/limits.rs)
- [crates/core-legacy/src/branch.rs](../../crates/core-legacy/src/branch.rs)
- [crates/core-legacy/src/branch_dag.rs](../../crates/core-legacy/src/branch_dag.rs)
- [crates/core-legacy/src/branch_types.rs](../../crates/core-legacy/src/branch_types.rs)
- [crates/core-legacy/src/id.rs](../../crates/core-legacy/src/id.rs)
- [crates/core-legacy/src/instrumentation.rs](../../crates/core-legacy/src/instrumentation.rs)

Major subtrees:

- `contract/` — versioned/addressing contract DTOs
- `primitives/` — event/json/vector types plus substantial helper logic

The legacy crate is large for something called “core”. The heaviest files are:

- [error.rs](../../crates/core-legacy/src/error.rs)
- [primitives/json.rs](../../crates/core-legacy/src/primitives/json.rs)
- [types.rs](../../crates/core-legacy/src/types.rs)
- [primitives/vector.rs](../../crates/core-legacy/src/primitives/vector.rs)
- [value.rs](../../crates/core-legacy/src/value.rs)
- [contract/entity_ref.rs](../../crates/core-legacy/src/contract/entity_ref.rs)
- [limits.rs](../../crates/core-legacy/src/limits.rs)
- [traits.rs](../../crates/core-legacy/src/traits.rs)

## Public Surface

The public re-exports in [lib.rs](../../crates/core-legacy/src/lib.rs) define the
crate’s current effective surface.

Today `strata-core-legacy` re-exports:

- branch-domain types:
  - `BranchControlRecord`
  - `BranchGeneration`
  - `BranchLifecycleStatus`
  - `BranchRef`
  - `ForkAnchor`
  - `BranchEventOffsets`
  - `BranchMetadata`
  - `BranchStatus`
- error types:
  - `StrataError`
  - `StrataResult`
  - `ErrorCode`
  - `ErrorDetails`
  - many supporting enums
- identifiers:
  - `BranchId`
  - `TxnId`
  - `CommitVersion`
- keyspace types:
  - `Namespace`
  - `Key`
  - `TypeTag`
  - `validate_space_name`
- foundational values:
  - `Value`
- storage abstraction:
  - `Storage`
  - `WriteMode`
- contract DTOs:
  - `EntityRef`
  - `PrimitiveType`
  - `Timestamp`
  - `Version`
  - `Versioned`
  - `VersionedHistory`
  - `VersionedValue`
  - `BranchName`
- primitive types and helpers:
  - `Event`
  - `ChainVerification`
  - `JsonValue`
  - `JsonPath`
  - `JsonPatch`
  - `VectorConfig`
  - `VectorEntry`
  - `DistanceMetric`
  - many helper functions such as:
    - `apply_patches`
    - `merge_patch`
    - `set_at_path`
    - `get_at_path`
    - `delete_at_path`

This means `core` is currently both:

1. a foundational type crate
2. a behavior host for storage-facing and primitive-facing logic

Those are separate roles, but today they live in one crate and are tightly
coupled.

## Module Inventory

### `id.rs`

[id.rs](../../crates/core-legacy/src/id.rs) owns foundational numeric/runtime IDs such
as:

- `TxnId`
- `CommitVersion`

This is true core material.

### `value.rs`

[value.rs](../../crates/core-legacy/src/value.rs) owns the canonical `Value` model.

It includes:

- the eight-variant value enum
- type/introspection helpers
- `extractable_text()`
- serde and equality semantics

This is mostly legitimate core territory, although `extractable_text()` already
leans toward runtime/search policy rather than pure shared language.

That ownership should not remain open. `extractable_text()` should move to
`engine`.

### `contract/`

The `contract/` subtree owns stable contract DTOs:

- [entity_ref.rs](../../crates/core-legacy/src/contract/entity_ref.rs)
- [primitive_type.rs](../../crates/core-legacy/src/contract/primitive_type.rs)
- [timestamp.rs](../../crates/core-legacy/src/contract/timestamp.rs)
- [version.rs](../../crates/core-legacy/src/contract/version.rs)
- [versioned.rs](../../crates/core-legacy/src/contract/versioned.rs)
- [versioned_history.rs](../../crates/core-legacy/src/contract/versioned_history.rs)
- [branch_name.rs](../../crates/core-legacy/src/contract/branch_name.rs)

This is the cleanest part of the crate. These files are closest to the intended
future scope of `core`.

### `types.rs`

[types.rs](../../crates/core-legacy/src/types.rs) owns:

- `BranchId`
- `Namespace`
- `TypeTag`
- `Key`
- `validate_space_name`
- key construction helpers

This file mixes truly foundational types with storage-layout assumptions and
namespace policy.

Important note:

`TypeTag` and `Key` are not just abstract vocabulary. They encode concrete
unified-storage layout assumptions. That means this file already straddles the
line between pure core language and storage-facing design.

That ownership should not remain open. `Key`, `TypeTag`, and `Namespace` belong
to `storage`.

### `traits.rs`

[traits.rs](../../crates/core-legacy/src/traits.rs) defines:

- `Storage`
- `WriteMode`

This is a major scope drift point.

These are not just shared types. They are architectural abstractions for the
persistence substrate and write semantics. They are much closer to the future
`storage` layer than to a minimal bottom-language crate.

### `limits.rs`

[limits.rs](../../crates/core-legacy/src/limits.rs) defines:

- `Limits`
- `LimitError`
- validation over keys, values, vectors, encoded size, and non-finite floats

This is another boundary-mixing file.

It looks like “shared validation,” but it is actually runtime policy:

- default limits
- structural validation rules
- serialization-compat assumptions
- fuzzy-scan limits

Some of this may stay shared, but in its current form it is much richer than a
minimal core helper.

### `error.rs`

[error.rs](../../crates/core-legacy/src/error.rs) is one of the biggest files in the
crate and owns a very broad workspace error taxonomy.

It currently tries to be:

- a bottom-layer error vocabulary
- a wire-facing canonical error-code map
- a home for primitive degradation reasons
- a common typed error surface across multiple layers

This is a major ownership concentration point. Even if some base error substrate
belongs in `core`, the current file is much broader than “minimal shared
language.”

### `branch.rs`

[branch.rs](../../crates/core-legacy/src/branch.rs) owns canonical branch-truth types:

- `BranchId::from_user_name`
- `aliases_default_branch_sentinel`
- `BranchGeneration`
- `BranchRef`
- `BranchLifecycleStatus`
- `ForkAnchor`
- `BranchControlRecord`

This file is well thought through, but it is not purely neutral vocabulary. It
freezes branch-control semantics and branch lifecycle schema that are very close
to engine-owned domain behavior.

### `branch_dag.rs`

[branch_dag.rs](../../crates/core-legacy/src/branch_dag.rs) owns:

- reserved system-branch helpers
- DAG event IDs
- fork/merge/revert/cherry-pick record structs
- branch DAG constants and status types

This is another clear sign that `core` currently owns engine-domain schema, not
just bottom-layer language.

### `branch_types.rs`

[branch_types.rs](../../crates/core-legacy/src/branch_types.rs) owns replay- and
durability-oriented branch lifecycle types:

- `BranchStatus`
- `BranchMetadata`
- `BranchEventOffsets`

This is not generic core vocabulary. It is recovery/durability/engine-adjacent
domain data.

### `primitives/event.rs`

[primitives/event.rs](../../crates/core-legacy/src/primitives/event.rs) defines event
types:

- `Event`
- `ChainVerification`

This file is relatively close to “shared DTOs,” although even here the event
hash-chain semantics are more domain-rich than a minimal core usually wants.

That ownership should not remain open. `Event` and `ChainVerification` belong
to `engine`.

### `primitives/json.rs`

[primitives/json.rs](../../crates/core-legacy/src/primitives/json.rs) is one of the
largest files in the crate and is a major scope drift point.

It owns:

- `JsonValue`
- `JsonPath`
- `PathSegment`
- `JsonPatch`
- document limits
- path parsing
- mutation helpers
- patch application
- merge-patch semantics
- validation logic

This is not just type definition. It is primitive behavior.

### `primitives/vector.rs`

[primitives/vector.rs](../../crates/core-legacy/src/primitives/vector.rs) is another
major scope drift point.

It owns:

- `DistanceMetric`
- `StorageDtype`
- `VectorConfig`
- `VectorId`
- `VectorEntry`
- `CollectionId`
- `CollectionInfo`
- `VectorMatch`
- metadata filters

It also includes behavior and policy such as:

- parsing/serialization helpers
- collection config validation
- model-specific presets:
  - OpenAI
  - MiniLM
  - MPNet

That last point is especially strong evidence that `core` is currently carrying
product/runtime assumptions.

### `instrumentation.rs`

[instrumentation.rs](../../crates/core-legacy/src/instrumentation.rs) owns performance
trace structs and aggregation helpers.

This is cross-cutting, but not obviously foundational shared language. It may
eventually belong elsewhere depending on how observability is regrouped.

## What `strata-core` Is Currently Doing

Taken together, the current crate owns six different categories of authority:

### 1. Foundational Shared Language

This is the part `core` should keep:

- IDs
- values
- refs
- versions
- timestamps
- a few stable contract DTOs

### 2. Storage Abstractions

This is currently represented by:

- `Storage`
- `WriteMode`
- `Key`
- `TypeTag`
- parts of `Namespace`

This is much closer to the future `storage` layer.

### 3. Engine / Runtime Domain Schema

This is currently represented by:

- `BranchControlRecord`
- `BranchRef`
- branch lifecycle types
- branch DAG types
- replay metadata

This is much closer to the future `engine` layer.

### 4. Primitive Semantics

This is currently represented by:

- JSON path logic
- JSON patch and merge behavior
- vector configuration logic
- event-chain types

This is much closer to engine-owned primitive behavior.

### 5. Validation / Limits Policy

This is currently represented by:

- default limits
- value validation
- vector dimension policy
- non-finite float handling

This is not purely language; it is runtime policy.

### 6. Cross-Layer Error Ownership

This is currently represented by:

- the large `StrataError` hierarchy
- error codes
- detail maps
- degradation reasons

This freezes too much behavior into the bottom layer.

## Internal Contradictions

There are a few especially important contradictions in the current crate.

### `primitives/mod.rs` Still Describes A Different Architecture

[primitives/mod.rs](../../crates/core-legacy/src/primitives/mod.rs) still describes an
older layering:

- `strata-core` defines semantic types
- `strata-primitives` provides implementation logic
- `strata-engine` orchestrates transactions and recovery

That is no longer the direction we want. The current documentation in this file
therefore encodes an architecture that should be treated as transitional or
stale.

### Branch Types Are Split Across Multiple Models

`branch.rs`, `branch_dag.rs`, and `branch_types.rs` each carry a different
slice of branch truth:

- canonical branch identity and lifecycle
- DAG lineage state
- durability/replay lifecycle state

Even where each piece is internally defensible, the fact that all of them sit
in `core` makes the bottom layer a host for engine-domain schema.

### Root Re-Exports Make Behavior Look Foundational

Because [lib.rs](../../crates/core-legacy/src/lib.rs) re-exports functions like:

- `apply_patches`
- `merge_patch`
- `set_at_path`
- `delete_at_path`

primitive behavior looks like bottom-layer shared language. That is one of the
main reasons the current crate boundary is misleading.

## Why This Matters Before The Engine Rewrite

If `engine` is restructured on top of the current `core`, the new engine will
inherit a polluted lower boundary.

That would cause three problems:

1. engine cleanup would preserve bad ownership just because the code is
   already “below” it
2. future `storage` cleanup would be harder because storage-facing abstractions
   would remain split across `core` and `storage`
3. the final dependency graph would still lie about who owns behavior

So this crate map should be read as a warning:

**`strata-core` is not currently a clean foundation.**

## Likely Future Split

Based on the current crate shape, the likely eventual disposition is:

### Likely To Stay In Clean Core

- `id.rs`
- most of `value.rs`
- much of `contract/`
- only the subset of `types.rs` that is still truly foundational after storage
  layout types leave

### Likely To Move To Storage

- `traits.rs`
- write-mode semantics
- `Key`
- `TypeTag`
- `Namespace`
- storage-facing key/layout abstractions

### Likely To Move To Engine

- branch lifecycle/control/DAG schema
- limits and validation policy
- primitive helper logic in `primitives/json.rs`
- vector config behavior and presets
- `Event`
- `ChainVerification`
- `Value::extractable_text()`
- any runtime-facing branch or primitive semantics

### Likely To Shrink Or Be Rebuilt Last

- `error.rs`
- `instrumentation.rs`

## Desired Reading Of This Document

This document should be used as a baseline before the `core` rewrite begins.

It should help answer:

- which parts of `core` are actually foundational
- which parts are just currently convenient
- which parts are already acting as `storage` or `engine` in disguise

The answer today is clear:

`strata-core` is an over-scoped crate whose current contents do not yet match
the role defined in [core-charter.md](./core-charter.md).
