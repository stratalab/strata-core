# Storage Charter

## Purpose

This document defines what `strata-storage` is supposed to be.

It exists because the old crate graph split lower-level runtime concerns across
multiple crates:

- `storage`
- `concurrency`
- `durability`

That split made the architecture harder to explain and allowed higher-level
primitive semantics to leak downward into lower layers.

This charter defines the intended storage-side target state:

- `storage` owns the persistence substrate and transaction runtime
- `engine` owns primitive and product semantics
- standalone lower runtime crates are eliminated

This is not a phase checklist. It is the scope definition that should govern
the consolidation work.

It should be read together with:

- [storage-minimal-surface-implementation-plan.md](./storage-minimal-surface-implementation-plan.md)
- [storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md)
- [../core/core-charter.md](../core/core-charter.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)
- [../core/co4-storage-boundary-eviction-plan.md](../core/co4-storage-boundary-eviction-plan.md)
- [../engine/engine-pending-items.md](../engine/engine-pending-items.md)
- [storage-crate-map.md](./storage-crate-map.md)
- [concurrency-crate-map.md](./concurrency-crate-map.md)
- [durability-crate-map.md](./durability-crate-map.md)

## Storage Thesis

`strata-storage` is the persistence and runtime substrate of Strata.

That means it owns the machinery required to make durable, versioned,
transactional state work at all:

- physical keyspace layout
- MVCC storage state
- optimistic concurrency control
- commit coordination
- WAL and snapshot runtime
- recovery and replay
- on-disk formats and codecs

It does not own user-facing primitive semantics.

The guiding rule is:

**`storage` owns generic persistence and transaction mechanics, not
primitive-specific behavior.**

## Target Stack

The intended stack is:

1. `core`
2. `storage`
3. `engine`
4. `intelligence`
5. `executor`
6. `cli`

In that stack:

- `core` owns stable shared language
- `storage` owns the persistence substrate and runtime coordination
- `engine` owns database semantics and primitives
- `intelligence` owns embedding, inference, and higher-level AI/runtime
  workflows layered on top of engine semantics
- `executor` owns the public command boundary
- `cli` owns presentation

The goal of this charter is to make `storage` fully worthy of its position in
that stack.

## What Storage Must Own

The clean `strata-storage` should own the full lower runtime substrate.

### 1. Physical Storage Layout

`storage` owns:

- `Key`
- `Namespace`
- `TypeTag`
- `WriteMode`
- physical key construction and storage layout rules

Reason:

These are persistence-addressing concerns, not shared domain language and not
engine semantics.

### 2. MVCC Storage State

`storage` owns:

- `SegmentedStore`
- versioned reads and writes
- tombstones and TTL handling
- branch-scoped physical storage state

Reason:

These are the substrate data structures that make persistence work.

### 3. Transaction Runtime

`storage` owns the generic lower-level responsibilities that were formerly
split across `strata-concurrency`.

That includes:

- transaction context state
- read/write/delete/CAS tracking
- snapshot version tracking
- OCC validation for generic keys
- commit coordination
- commit version allocation
- visibility tracking
- branch commit locks
- quiesce and deletion safety

Reason:

These are generic transaction mechanics tightly coupled to the storage
substrate. They are not engine-domain behavior.

### 4. Durability Runtime

`storage` owns the substrate runtime that was formerly split across
`strata-durability`.

That includes:

- WAL writer/reader/configuration
- durability modes
- snapshot writing and loading
- checkpoint coordination
- manifest management
- compaction and tombstone indexing
- canonical on-disk layout
- codec abstraction
- recovery-time record streaming and replay plumbing

Reason:

These are persistence-runtime concerns. They belong with the storage substrate,
not in a peer crate beside it.

### 5. On-Disk Formats

`storage` owns the byte-level formats for:

- WAL records and segments
- manifests
- snapshots
- writesets
- primitive-tag sectioning

Reason:

Format ownership belongs with the subsystem that owns persistence and replay,
not with the engine that interprets higher-level meaning.

## What Storage Must Not Own

### 1. Primitive Semantics

`storage` must not own:

- JSON path mutation semantics
- JSON patch buffering rules
- JSON path overlap policy
- event-chain meaning
- vector metric behavior
- vector model presets
- branch lifecycle semantics

Reason:

Those are primitive or engine semantics. Storage may persist their effects, but
it should not define their meaning.

### 2. Product Or Runtime Policy Above The Substrate

`storage` must not own:

- database open/bootstrap policy
- product defaults
- public command behavior
- public retry policy
- executor-facing contracts

Reason:

Those are higher-layer concerns.

### 3. Branch Bundle Import/Export

`storage` must not own:

- branch archive format as a product workflow
- branch export/import rules
- branch-level metadata presentation for portable artifacts

Reason:

Branch bundle is an engine/domain import-export workflow, not a substrate
durability primitive.

## Consolidation Decisions

The intended consolidation is explicit.

### `strata-concurrency`

The standalone `strata-concurrency` crate should not survive long-term.
That deletion is now complete.

Its generic runtime responsibilities move into `storage`:

- transaction context
- validation
- commit manager
- WAL payload serialization
- replay/recovery coordination
- lock ordering and related substrate runtime helpers

What must leave before that merge is finalized:

- JSON path and patch semantics
- event continuity semantics attached to primitive behavior

### `strata-durability`

The standalone `strata-durability` crate should also not survive long-term.

Its substrate responsibilities move into `storage`:

- WAL runtime
- snapshots
- manifest/layout
- codecs
- compaction
- on-disk formats

What should move upward instead:

- `branch_bundle`

## Engine-Side Countermoves

To keep the lower layer clean, the following responsibilities should move to or
remain in `engine`:

- JSON path and patch semantics
- JSON patch buffering and materialization policy
- event continuity across higher-level transaction wrappers
- branch bundle import/export
- branch-domain interpretation layered on top of storage/runtime records

The point is not merely to move code. It is to restore the correct semantic
boundary:

- storage persists and coordinates
- engine interprets and composes

## Dependency Rules

The intended dependency direction is:

- `storage -> core`
- `engine -> storage + core`
- `executor -> engine + storage + core` as needed at public boundaries

The following shapes should disappear:

- `engine -> core-legacy`
- `concurrency -> core-legacy`
- `durability` as a peer runtime crate beside storage
- lower-layer dependence on engine-owned primitive semantics

## End-State Definition

The storage consolidation is considered complete when all of the following are
true:

1. `strata-storage` owns the persistence substrate, transaction runtime, and
   durability runtime.
2. Generic OCC/commit/recovery code no longer lives in a separate
   deleted `strata-concurrency` crate.
3. WAL/snapshot/manifest/layout/codec code no longer lives in a separate
   `strata-durability` crate.
4. JSON path semantics no longer live below `engine`.
5. Branch bundle no longer lives below `engine`.
6. `engine` no longer depends on `core-legacy` in order to reach lower runtime
   machinery.
7. The remaining `core-legacy` surface is small enough to delete in the final
   convergence step.

## Design Guardrails

The consolidation should follow these rules:

### 1. Do Not Bury A Bad Boundary

If primitive semantics are misplaced today, do not merge them into `storage`
just to make a crate disappear.

### 2. Prefer One Honest Lower Runtime Over Two Misleading Peer Crates

If a subsystem is really part of the persistence runtime, it should live with
`storage` even if that makes the crate larger.

### 3. Keep Storage Generic

The storage/runtime layer may know about generic mutations and versioned state.
It should not need to understand primitive-specific business rules.

### 4. Delete Transitional Owners

The purpose of this work is not to preserve `concurrency` and `durability` as
named architectural pillars. The purpose is to end with a cleaner graph and
fewer false owners.
