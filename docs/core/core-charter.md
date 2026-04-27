# Core Charter

## Purpose

This document defines what `strata-core` is supposed to be.

It exists because the legacy core implementation was too large and owned logic
that belongs in lower or higher layers. Before restructuring `storage` or
`engine`, we needed a stable answer to a simpler question:

**what does the bottom shared-language layer actually own?**

This document is not a migration plan. It is the target scope definition that
should guide future cleanup.

For the staged rewrite plan, see
[core-minimal-surface-implementation-plan.md](./core-minimal-surface-implementation-plan.md).

For the broader CO3 error-ownership review, see
[core-error-review.md](./core-error-review.md).

## Core Thesis

`strata-core` is the shared domain language of Strata.

That means it should define the smallest set of types and contracts that the
rest of the stack needs in order to talk about the same world:

- values
- ids
- refs
- versions
- timestamps
- a few stable contract DTOs

It should not be the place where Strata accumulates:

- storage abstractions
- branch-control policy
- primitive algorithms
- product defaults
- model assumptions
- database lifecycle behavior

The guiding rule is:

**`core` defines shared language, not shared behavior.**

## Quantitative Target

The clean `strata-core` should be small enough that its role is obvious from a
quick read.

The target shape is:

- roughly `<= 3K` lines of Rust
- roughly `5-6` modules
- no workspace-crate dependencies

This is not a compatibility contract or an exact budgeting rule. It is a
guardrail against re-growing a bottom-layer junk drawer and then calling it
"shared language" because it is smaller than before.

## Target Stack

The intended stack for the next recovery phase is:

1. `core`
2. `storage`
3. `engine`
4. `executor`
5. `cli`

In that stack:

- `core` owns stable shared domain vocabulary
- `storage` owns persistence and low-level coordination
- `engine` owns database semantics, runtime, and primitives
- `executor` owns the public command boundary
- `cli` owns the shell

This charter is written for that stack, not for the older
`core -> storage -> engine -> primitives -> intelligence -> executor -> cli`
target.

## What Core Must Be

The clean `strata-core` should be small, boring, and stable.

It may own:

- shared ids and refs
- the canonical `Value` model
- stable contract DTOs
- low-level domain types that are truly universal
- small helpers that are inseparable from those types
- small type-local validation errors if they are inseparable from those types

It should be possible to explain `core` as:

“the types every other layer must agree on before anything else can work.”

## What Core May Own

### 1. Canonical IDs

`core` may own canonical identifiers such as:

- `BranchId`
- `TxnId`
- `CommitVersion`

Reason:

These are foundational identity types used across storage, engine, executor,
and tests. They are part of the shared language of the system.

### 2. Canonical Value Model

`core` may own the canonical value model:

- `Value`

Reason:

This is the lowest-level user data language shared across the whole system and
all public surfaces.

### 3. Stable Contract DTOs

`core` may own stable contract types such as:

- `EntityRef`
- `PrimitiveType`
- `Version`
- `Versioned<T>`
- `VersionedHistory`
- `Timestamp`
- `BranchName` if it remains a pure contract type

Reason:

These are not database policies. They are shared typed vocabulary for
addressing data and describing versioned results.

### 4. Truly Universal Low-Level Constants

`core` may own constants or small helper methods that are inseparable from the
foundational types themselves.

Examples:

- byte encoding constants tied directly to a bottom-level wire-safe type
- parsing helpers that are part of the type’s own contract

Reason:

Some behavior is part of the definition of a type. That is acceptable so long
as it does not pull in broader policy.

## What Core Must Not Own

### 1. Storage Abstractions

`core` must not own:

- `Storage` traits
- write-mode policies
- persistence-facing batching contracts
- low-level storage coordination interfaces

Reason:

Those belong to the `storage` layer. If they live in `core`, then `core`
ceases to be just shared language and becomes a low-level architecture layer
with policy.

The ownership decision is explicit:

- `Storage` moves to `storage`
- `WriteMode` moves to `storage`
- `Key` moves to `storage`
- `TypeTag` moves to `storage`
- `Namespace` moves to `storage`

### 2. Database Lifecycle Or Runtime Policy

`core` must not own:

- open/runtime policy
- branch startup policy
- recovery coordination
- transaction-management behavior
- retention policy

Reason:

Those are engine responsibilities.

### 3. Primitive Algorithms

`core` must not own:

- JSON path mutation logic
- JSON patch execution
- merge-patch behavior
- vector collection policy
- distance metric behavior beyond enum definition
- graph traversal or indexing behavior

Reason:

Those are primitive semantics and belong with the database/runtime layer in the
future `engine` shape.

The ownership decision is explicit:

- JSON path/patch/mutation logic moves to `engine`
- vector config behavior and model presets move to `engine`
- `Event` and `ChainVerification` move to `engine`
- `Value::extractable_text()` moves to `engine`

### 4. Product Or Model Policy

`core` must not own:

- embedding-model presets
- product defaults
- search assumptions
- recipe behavior
- executor-facing validation policy

Reason:

That is not shared language. It is product/runtime behavior.

### 5. The Parent Database Error

`core` must not own the canonical parent error for the database/runtime layer.

That includes:

- `StrataError`
- `StrataResult`
- retryability and severity policy
- public error-code classification

Reason:

The system does benefit from one canonical parent database/runtime error, but
that error belongs to `engine`, not to the foundational language layer.

### 6. Giant Cross-Layer Error Ownership

`core` should not be the dumping ground for every other error case in the
system.

Reason:

Once the bottom layer owns every error variant, every higher layer is tempted
to push its policy downward and freeze it too early. A minimal core may still
define some base error vocabulary, but domain-rich error logic should live in
the domain layer that actually owns the behavior.

## Core Design Rules

### 1. No Shared Policy

If a type or helper encodes a decision about how Strata behaves rather than
what Strata is talking about, it probably does not belong in `core`.

### 2. No Hidden Downstream Ownership

`core` should not quietly own logic just because many crates happen to use it.

Shared usage is not enough. The question is:

**does this logic belong at the bottom of the stack?**

### 3. Favor DTOs Over Engines

If a concept can be represented as a plain data type instead of a behavioral
subsystem, `core` should prefer the plain data type.

### 4. Keep The Bottom Layer Stable

Anything placed in `core` becomes hard to move later because everything depends
on it. That means the bar for new additions must be high.

### 5. Convenience Is Not A Scope Argument

It is not enough that a helper would be convenient in multiple crates.

The helper must also be conceptually bottom-layer behavior.

## Current Over-Scoping Signals

The legacy core implementation is materially over-scoped.

Concrete signals from the current tree:

- `traits.rs` defines `Storage` and `WriteMode`
- `primitives/json.rs` contains JSON path parsing, patch application, merge
  semantics, and validation logic
- `primitives/vector.rs` contains vector config behavior and model-specific
  presets such as OpenAI and MiniLM helpers
- `branch.rs`, `branch_dag.rs`, and `branch_types.rs` carry domain schema that
  is closer to engine-owned branch semantics than to bottom-layer shared
  language
- `error.rs` is large enough that it is likely freezing cross-layer behavior
  too early

None of those automatically mean the code is wrong. They mean the ownership is
wrong for a minimal `core`.

## Proposed Core Boundary

If we rebuilt `strata-core` today, the default assumption should be:

### Keep In New Core

- `BranchId`
- `TxnId`
- `CommitVersion`
- `Value`
- `EntityRef`
- `PrimitiveType`
- `Version`
- `Versioned<T>`
- `VersionedHistory`
- `Timestamp`
- a minimal `BranchName` contract if still needed
- only the smallest truly shared error substrate

### Move Out Of Core

To `storage`:

- `Storage`
- `WriteMode`
- persistence-facing storage contracts
- `Key`
- `TypeTag`
- `Namespace`

To `engine`:

- branch lifecycle/control/DAG domain types
- limits and validation policy if they remain runtime-owned
- JSON path/patch/mutation logic
- vector config behavior and model presets
- `Event`
- `ChainVerification`
- `Value::extractable_text()`
- primitive-specific validation and helper logic
- any runtime-facing or primitive-facing error detail

Possibly delete or radically shrink:

- any helper that exists only because current crate boundaries are muddled

## What This Means For Restructuring

This charter implies the following sequencing rule:

**scope `core` before deeply restructuring `engine`.**

Reason:

If `engine` is rebuilt on top of an overgrown `core`, then the new `engine`
will inherit the same bad lower boundary and we will mistake dependency cleanup
for architectural cleanup.

The likely migration shape is:

1. define the clean `core` target
2. rename the current crate to `strata-core-legacy`
3. create a new minimal `strata-core`
4. move only true bottom-layer language into it
5. move the rest downward or upward to its real owner
6. then restructure `engine` against that cleaned foundation

## Non-Goals

This charter does not require:

- solving the whole storage merge immediately
- solving the whole engine rewrite immediately
- deleting every current type from `core` in one step
- freezing the exact final error design right now

It only defines what `core` is allowed to be.

## Desired End State

At the end of this cleanup, `strata-core` should read like a small foundational
crate that every layer can depend on without importing product behavior.

Someone opening `core` should find:

- shared language
- stable contracts
- minimal helpers

and should **not** find:

- product policy
- runtime policy
- primitive algorithms
- storage abstractions
- database behavior

That is the standard the future `core` should meet.
