# Engine Error Architecture

## Purpose

This document records the intended error architecture for `strata-engine`.

It exists because the core cleanup only answers part of the problem. `CO3`
establishes that the canonical parent database/runtime error should not live in
`core`. That still leaves an engine-side design question:

- what the engine-owned parent error should be responsible for
- what it should not absorb
- how lower and higher layers should relate to it

This document is the engine-facing landing zone for that work.

It should be read together with:

- [../core/core-error-review.md](../../core/core-error-review.md)
- [../core/error-research.md](../../core/error-research.md)
- [../core/core-minimal-surface-implementation-plan.md](../../core/core-minimal-surface-implementation-plan.md)
- [engine-pending-items.md](./engine-pending-items.md)

The `core` docs establish the ownership move. This document establishes the
engine-side target state that the future engine cleanup should implement.

## Why Engine Owns The Parent Error

`strata-engine` is the layer that owns:

- database lifecycle
- transaction semantics
- branch and snapshot semantics
- recovery semantics
- primitive composition into database behavior
- the runtime/product layer that turns lower subsystems into the Strata
  database

That means `engine` is the first layer that can interpret lower-level failures
as database semantics rather than raw subsystem conditions.

Examples:

- a storage checksum mismatch is not merely a storage issue by the time it
  reaches engine; it is a database corruption condition
- a WAL fsync failure during commit is not merely an IO problem; it may become
  an ambiguous commit condition
- a validation failure at the edge of a primitive write is not just a parse
  error; it may become an invalid database operation

So the parent error belongs in `engine`, not because engine should own
everything, but because engine is the layer that owns the database's own
vocabulary of failure.

## Engine Error Ownership

The engine should own:

- `StrataError`
- `StrataResult<T>`
- engine-local nested reason types
- retryability and ambiguous-commit semantics
- canonical database/runtime classification

The engine should not own:

- foundational type-local validation errors that belong in `core`
- raw storage taxonomy that belongs in `storage`
- public wire/IPC/client error policy that belongs in `executor`
- CLI/application reporting concerns

The engine is therefore the semantic middle:

- lower layers feed raw conditions into engine
- engine interprets them as database/runtime failures
- higher layers translate them into boundary-specific contracts

## Target Layer Relationship

The intended ownership split is:

- `core`
  - small type-local validation errors only
- `storage`
  - `StorageError`
  - raw persistence and integrity conditions at the storage layer
- `engine`
  - `StrataError`
  - canonical parent database/runtime error
- `executor`
  - public boundary error
  - wire-facing codes and boundary policy
- `cli`
  - presentation only

This means `StrataError` is neither:

- the global error type for the entire workspace
- nor an unnecessary extra wrapper over every local failure

It is the error type for the database/runtime layer.

## What `StrataError` Should Represent

`StrataError` should represent failures that have meaning in the vocabulary of
the Strata database itself.

Representative top-level classes likely include:

- invalid argument or invalid operation
- not found / already exists
- transaction conflict
- invalid transaction state
- timeout / cancellation
- ambiguous commit
- resource exhaustion
- IO-derived runtime failure
- corruption
- unsupported operation
- internal invariant failure

The exact variant list can evolve during engine cleanup. The important point is
that these are database/runtime classes, not crate names and not raw subsystem
labels.

That means engine should prefer:

- semantic classes like `Conflict`, `AmbiguousCommit`, `Corruption`

over:

- subsystem-shaped top-level variants like `WalError`, `LsmError`,
  `VectorError`, `JsonError`

Subsystem-specific detail should usually live in nested reason types or source
errors, not as the primary public shape of the parent error.

## What Should Stay Below `StrataError`

Lower layers and local domains may still have their own error types.

That is not a design failure. It is necessary structure.

Examples:

- `StorageError` remains storage-owned
- local recovery reasons may remain engine-local
- local validation errors may remain attached to a type or primitive family

What changes is where those errors stop being the final answer.

When a lower error crosses into engine and must be interpreted as a
database/runtime failure, engine should decide whether it becomes:

- a top-level `StrataError` class
- a nested engine-owned reason type
- or a preserved source error under a broader engine class

## Boundary Rules

### 1. Storage To Engine

Storage should not know about `StrataError`.

Engine should convert from `StorageError` explicitly, preserving source
information and adding semantic meaning where appropriate.

Engine should avoid blanket, context-free lifting when the database meaning
depends on operation phase or surrounding runtime context.

### 2. Engine Internal Domains

Engine-local subsystems may keep local error types if they add real structure.
But engine APIs should converge on `StrataResult<T>` at the layer boundary.

This means the engine may internally have:

- local recovery errors
- local observer errors
- local primitive or branch reason types

while still exposing one coherent parent error to the rest of the stack.

### 3. Engine To Executor

Executor should not reuse `StrataError` as its public wire/boundary error
shape.

Executor should explicitly translate from `engine::StrataError` into
`executor::Error`.

That translation is where:

- wire-facing code families
- boundary-safe detail shaping
- public retry guidance
- public IPC semantics

are finalized.

## Retryability And Ambiguous Commit

The research strongly supports explicit retry and commit-ambiguity semantics as
part of the engine-owned parent error.

That means the future engine error model should be able to express at least:

- definitely retryable and definitely not committed
- maybe committed / ambiguous commit
- definitely non-retryable

The exact API shape can be decided during implementation, but the architectural
requirement is already clear:

- retryability should not be inferred from display strings
- ambiguous commit should be a first-class condition
- executor should be able to derive boundary policy from engine-owned
  semantics rather than inventing that policy itself

## Stable Classification

The research also strongly supports stable machine-readable classification.

For the engine cleanup, this means the parent error should eventually support a
stable classifier, whether that takes the form of:

- a kind enum
- a numeric code
- or both

The exact encoding can be refined later. The durable rule is:

- display text is not the programmatic contract
- stable classification belongs at or below the engine-owned parent error
- executor may expose a separate boundary-oriented classification on top

## Migration Rules For The Engine Cleanup

When the engine cleanup begins, it should follow these rules:

1. No new parent operational error is added to `core`.
2. No new `StrataResult` usage is introduced outside the engine boundary unless
   it is transitional and explicitly documented.
3. No new storage-to-engine conversions should flatten semantic context into
   generic strings.
4. No new executor boundary APIs should expose `StrataError` directly as the
   public wire contract.
5. Local error types are acceptable only when they add real structure; they
   should not become uncontrolled peer public parents.

## Non-Goals

This document does not settle:

- the exact final `StrataError` enum layout
- the exact final stable numeric code registry
- the exact final executor wire serialization format
- every local engine sub-error that will survive as a reason type

Those are implementation questions for the engine cleanup.

What this document does settle is the architectural target:

- `core` sheds the parent error
- `engine` becomes the home of the canonical database/runtime parent error
- `storage` and `executor` keep their own rightful boundaries
- the future engine cleanup should implement that model deliberately rather
  than rediscovering it ad hoc
