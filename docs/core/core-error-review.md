# Core Error Review

## Purpose

This document is the error-ownership review for `CO3`.

The immediate trigger is `core`: the legacy core shell still owns a very large
cross-layer error model, while the new `strata-core` is intentionally almost
error-free. But the real problem is broader than `core` alone. The workspace
currently has two competing instincts:

1. one parent `StrataError` for the database/runtime stack
2. a growing set of rightful crate-local typed errors

`CO3` has to resolve that tension correctly. If we only shrink
`crates/core-legacy/src/error.rs` mechanically, we will preserve the wrong
ownership model under a smaller file.

This review should be read together with:

- [error-research.md](./error-research.md)
- [../engine/engine-error-architecture.md](../engine/engine-error-architecture.md)

That research memo is the external and cross-project justification for the
direction below. The engine document is the target implementation home for the
engine-owned parent error. This document is the Strata-specific ownership
decision record.

## Current Topology

### 1. New `strata-core` is already close to the right shape

The new core currently has no global system error type.

Its only real error type today is local and type-specific:

- [crates/core/src/contract/branch_name.rs](../../crates/core/src/contract/branch_name.rs)
  - `BranchNameError`

This is a strong signal that the clean core boundary should remain nearly
error-free. Foundational types may have local parse/validation errors, but the
new core should not become the owner of the database/runtime parent error.

### 2. Legacy core still owns the monolith

The legacy shell still exports:

- [crates/core-legacy/src/error.rs](../../crates/core-legacy/src/error.rs)
  - `PrimitiveDegradedReason`
  - `ErrorCode`
  - `ErrorDetails`
  - `ConstraintReason`
  - `StrataError`
  - `StrataResult`

This is not just a type alias file. It is still acting as:

- a system-wide operational error vocabulary
- a wire/error-code vocabulary
- a retry/severity policy layer
- a catch-all conversion sink for lower-level failures

That is far beyond the proper scope of `core`.

### 3. Crate-local errors already exist across the workspace

Representative examples:

- [crates/storage/src/error.rs](../../crates/storage/src/error.rs)
  - `StorageError`
  - `StorageResult<T>`
- [crates/engine/src/database/recovery_error.rs](../../crates/engine/src/database/recovery_error.rs)
  - `RecoveryError`
- [crates/concurrency/src/transaction.rs](../../crates/concurrency/src/transaction.rs)
  - `CommitError`
- [crates/concurrency/src/payload.rs](../../crates/concurrency/src/payload.rs)
  - `PayloadError`
- [crates/vector/src/error.rs](../../crates/vector/src/error.rs)
  - `VectorError`
  - `VectorResult<T>`
- [crates/executor/src/error.rs](../../crates/executor/src/error.rs)
  - executor-facing `Error`
- [crates/search/src/llm_client.rs](../../crates/search/src/llm_client.rs)
  - `LlmClientError`
- [crates/inference/src/error.rs](../../crates/inference/src/error.rs)
  - `InferenceError`

So the workspace already knows how to define crate-owned typed errors. The
problem is not the absence of local error vocabularies. The problem is that the
current `StrataError` model is still both:

- core-owned
- the dominant return surface in many places

## Inventory Findings

### 1. `StrataError` / `StrataResult` are still widespread outside core

Files referencing `StrataError` or `StrataResult` today:

- `engine/src`: `43`
- `graph/src`: `17`
- `vector/src`: `7`
- `storage/src`: `4`
- `concurrency/src`: `4`
- `durability/src`: `4`
- `executor/src`: `4`
- `intelligence/src`: `2`
- `search/src`: `1`

This is the central CO3 problem: the lower and middle layers still treat
`StrataResult` as a normal internal result surface.

### 2. Error-definition files are already distributed by crate

Files defining or implementing local errors:

- `durability/src`: `16`
- `engine/src`: `8`
- `storage/src`: `2`
- `concurrency/src`: `3`
- `vector/src`: `1`
- `search/src`: `1`
- `inference/src`: `1`
- `executor/src`: `1`

This confirms that the codebase has already moved away from a single-error
world in practice. The real question is not whether one parent error should
exist at all. The question is where that parent belongs and which layers should
use it directly.

### 3. The most problematic mixed-ownership seams are storage and engine

Storage already has a crate-local error model:

- [crates/storage/src/error.rs](../../crates/storage/src/error.rs)

But storage still exports `StrataResult` heavily in core implementations:

- [crates/storage/src/segmented/mod.rs](../../crates/storage/src/segmented/mod.rs)
- [crates/storage/src/segment.rs](../../crates/storage/src/segment.rs)

That means `StorageError` exists, but storage is not consistently using its own
error surface as the normal public result type.

The same pattern exists in engine:

- engine has local typed errors such as `RecoveryError`,
  `EventLogValidationError`, `BranchDagError`, `ObserverError`, and others
- but most engine public and internal APIs still return `StrataResult`

Representative examples:

- [crates/engine/src/primitives/kv.rs](../../crates/engine/src/primitives/kv.rs)
- [crates/engine/src/primitives/json/mod.rs](../../crates/engine/src/primitives/json/mod.rs)
- [crates/engine/src/primitives/event.rs](../../crates/engine/src/primitives/event.rs)
- [crates/engine/src/database/open.rs](../../crates/engine/src/database/open.rs)
- [crates/engine/src/database/transaction.rs](../../crates/engine/src/database/transaction.rs)

### 4. Conversions back into `StrataError` are often lossy or policy-bearing

Representative examples:

- [crates/storage/src/error.rs](../../crates/storage/src/error.rs)
  converts `StorageError` back into `StrataError`
- [crates/engine/src/database/recovery_error.rs](../../crates/engine/src/database/recovery_error.rs)
  converts `RecoveryError` back into `StrataError`
- [crates/vector/src/error.rs](../../crates/vector/src/error.rs)
  converts `VectorError` into `StrataError`
- [crates/concurrency/src/transaction.rs](../../crates/concurrency/src/transaction.rs)
  converts `CommitError` into `StrataError`

This creates two recurring problems:

1. information gets flattened into strings or broader buckets
2. policy that belongs to higher layers is forced into lower-layer conversions

### 5. Executor already has the correct public-boundary pattern

Executor has its own public `Error`:

- [crates/executor/src/error.rs](../../crates/executor/src/error.rs)

and a single explicit conversion layer:

- [crates/executor/src/convert.rs](../../crates/executor/src/convert.rs)

That part of the architecture is directionally correct: the public boundary
owns its own error shape and converts from lower-layer failures. The problem is
that executor still converts from `StrataError`, which means the lower boundary
is still too global and too policy-heavy.

## Structural Diagnosis

The workspace currently has three overlapping error roles:

### 1. Foundational type-local validation errors

Examples:

- `BranchNameError`
- JSON path parse errors in legacy primitive helpers
- event payload validation enums

These belong next to the specific type or primitive family they validate.

### 2. Layer-owned operational errors

Examples:

- `StorageError`
- `RecoveryError`
- executor `Error`
- local primitive/runtime reason types

These should define the public operational vocabulary of each architectural
layer.

### 3. Parent database/runtime errors

This is where `StrataError` should live.

The system does benefit from one canonical parent error for the database/runtime
layer. The mistake in the old architecture was not having a parent error. The
mistake was making that parent error:

- owned by `core`
- used as the direct internal result surface of nearly every crate

The target is:

- `core` owns only type-local validation and parse errors
- `storage` owns `StorageError`
- `engine` owns `StrataError` and `StrataResult`
- `executor` owns the public boundary error and wire contract
- `cli` owns presentation/reporting only

That is the durable architectural answer. It is more important than any
particular enum shape.

## Research-backed decisions

The research in [error-research.md](./error-research.md) supports several
concrete decisions that should now be treated as settled for `CO3`.

### 1. `StrataError` should survive, but it must move to `engine`

The system does benefit from one canonical parent database/runtime error. The
mistake in the old architecture was not the existence of `StrataError`. The
mistake was:

- putting it in `core`
- letting it become the dominant direct return type of unrelated crates

The canonical parent error should therefore survive, but as an `engine`-owned
type that represents the database's own vocabulary of failure.

### 2. `core` should remain nearly error-free

The new `strata-core` should not grow a replacement monolith. It should own
only:

- type-local parse/validation errors
- small inseparable helper errors attached to foundational types

It should not own:

- retryability policy
- wire error codes
- storage failure semantics
- transaction failure semantics
- executor-facing classification

### 3. `storage` should consistently own storage failures

`StorageError` should be the normal storage-layer result surface, not a side
type that immediately funnels back into a core-owned parent.

That means:

- storage should return `StorageResult<T>`
- storage should not depend on `engine`
- storage should not know about transaction semantics or executor policy

### 4. `executor` should keep a distinct boundary error

The research confirms the direction executor already follows: the public
boundary should own its own error shape and convert from lower-layer failures.

For `CO3`, the architectural requirement is:

- executor converts from `engine::StrataError`
- executor remains the owner of wire/public classification policy

The exact future wire-code taxonomy can evolve separately from the core
ownership move. `CO3` does not need to settle every serialization detail.

### 5. `anyhow` should stay out of library crates

The research is clear that `anyhow` belongs at application/reporting boundaries
rather than typed library APIs. For Strata that means:

- `cli` may use `anyhow`
- `core`, `storage`, `engine`, and `executor` should not expose
  `anyhow::Result` in normal APIs

### 6. Explicit conversions are preferred at layer boundaries

The research strongly supports explicit, reviewable conversions at major layer
boundaries instead of blanket `From`-driven coercion.

For Strata, the practical implication is:

- lower layers may keep local typed errors
- conversion into `engine::StrataError` should be explicit where semantic
  interpretation matters
- conversion from `engine::StrataError` into `executor::Error` should stay
  explicit and centralized

### 7. Stable codes and retryability are design targets, but not the whole of `CO3`

The research is persuasive that Strata eventually needs:

- stable machine-readable error codes
- explicit retryability classification
- explicit ambiguous-commit semantics

Those are good target properties for the engine-owned parent error.
But they should not distract from the immediate `CO3` ownership correction:

- get `StrataError` out of `core`
- make `storage` and `executor` own their own boundaries consistently
- stop new code from deepening the legacy monolith

- one parent `StrataError`
- owned by `engine`
- used at the engine boundary
- fed by narrower lower-level and leaf-local errors

## Ownership Decisions For CO3

### 1. New `strata-core` should not own the parent system error

The new core should remain nearly error-free.

It may own:

- type-local validation errors inseparable from foundational types

It should not own:

- `StrataError`
- `StrataResult`
- wire-facing error codes
- retryability policy
- severity policy
- primitive degradation policy

### 2. `StrataError` survives, but is re-homed to `engine`

`StrataError` should remain the canonical parent error for the database/runtime
layer.

But it should be:

- owned by `engine`
- exposed as the engine boundary error
- fed by lower-level errors such as `StorageError` and leaf-local typed errors

It should not be:

- owned by `core`
- the universal internal currency of every crate
- the place where all subsystem detail is flattened prematurely

Implications:

- no new core-owned `StrataError` variants should be added as part of CO3
- no new subsystems should choose `StrataResult` as their normal result type
  unless they are genuinely operating at the engine boundary
- conversions into `StrataError` should be treated as engine-boundary
  conversions, not as the default shape of every internal seam

### 3. Storage owns storage failures

`storage` should own:

- `StorageError`
- storage recovery / manifest / segment / I/O failures that are genuinely
  storage-layer concerns

Its normal result surface should be `StorageResult<T>`, not `StrataResult<T>`.

### 4. Engine owns the parent database/runtime error surface

Engine should own:

- `StrataError`
- `StrataResult<T>`

This should become the normal result surface for engine public APIs and for
cross-crate engine-facing behavior.

Engine should also own domain-wide runtime failures that are currently stranded
in `core-legacy`, especially:

- `PrimitiveDegradedReason`
- branch/runtime lifecycle failures
- primitive-family validation and operational failures once those owners are
  consolidated

### 5. Executor owns the public boundary error surface

Executor should keep owning:

- [crates/executor/src/error.rs](../../crates/executor/src/error.rs)

If canonical wire codes remain necessary, they should live with the public
boundary, not in `core`.

That means `ErrorCode`, `ErrorDetails`, and any retry/severity/public-class
policy should move toward executor or another explicit API-boundary home, not
into the new core.

### 6. Leaf crates keep local typed errors, but not as peer public parents

Crates like `vector`, `search`, `inference`, `concurrency`, and others should
keep their local typed errors where they are useful.

The conversion target for those errors should be their immediate owner layer:

- leaf error -> `StorageError` or `StrataError`
- storage/engine error -> executor `Error`

That avoids two bad extremes:

- one giant core-owned global junk drawer
- one public top-level error enum per niche subsystem

Primitive- or subsystem-specific errors may remain local or become nested
reason types under engine-owned variants. They should not all become peer
public contracts.

## Recommended CO3 Strategy

`CO3` should be treated as an ownership-and-boundary epic, not as a local file
reduction epic.

### Phase 1. Freeze the direction

Immediate rule:

- no new `StrataError` variants
- no new `StrataResult` APIs in crates that already have typed local errors

This prevents the old core-owned/global-by-default surface from re-growing
during the rest of the rewrite.

### Phase 2. Codify the new-core stance

The new `strata-core` should explicitly remain without a global operational
error type.

Acceptance:

- `crates/core/src/error.rs` stays minimal
- only type-local errors remain in the new core

### Phase 3. Re-home `StrataError` into engine

Before the larger engine rewrite completes, move the parent error to its proper
owner:

- `StrataError`
- `StrataResult<T>`

as engine-owned boundary types rather than core-owned global types.

This gives storage, concurrency, graph, vector, search, and intelligence a
real parent to convert into without keeping `core` as the policy owner.

### Phase 4. Push storage back onto `StorageResult`

Storage already has the right error crate. The next move is consistency:

- `storage` internals and public APIs should return `StorageResult<T>`
  wherever storage is the owner
- `StrataResult<T>` should stop being the normal storage surface

### Phase 5. Move policy-bearing error families out of legacy core

As engine and executor boundaries harden, move these out of
`crates/core-legacy/src/error.rs`:

- `PrimitiveDegradedReason` -> `engine`
- `ErrorCode` / `ErrorDetails` / public classification policy -> `executor`
- `ConstraintReason` -> whichever layer truly owns the surviving public
  constraint taxonomy, but not `core`

### Phase 6. Collapse the compatibility shell

Once storage and engine no longer treat core-owned `StrataError` as their
normal result surface, `core-legacy/error.rs` can collapse quickly:

- fewer variants
- fewer `From` impls
- narrower compatibility role

That is the real success condition for `CO3`.

## What CO3 Is Not

CO3 is not:

- a full engine error redesign in one patch
- a requirement to standardize every crate on `thiserror` immediately
- a requirement to remove every string payload from every local error enum
- a reason to grow a new monolithic `error.rs` inside the clean core

Those are secondary cleanup tasks. The primary goal is to put error ownership
in the right layers.

## Acceptance Criteria For The Strategy

The CO3 strategy is considered correct if it leads to all of the following:

1. The clean `strata-core` still does not own a global operational error model.
2. `StrataError` survives as the engine-owned parent error, not a core-owned
   universal internal currency.
3. `storage` owns storage failures through `StorageError`.
4. `engine` owns database/runtime failures through `StrataError`.
5. `executor` owns the public boundary error shape and public classification.
6. Lower-level crates stop converting directly into a global core-owned error
   as their normal path.

That is the architecture-consistent route for `CO3`.
