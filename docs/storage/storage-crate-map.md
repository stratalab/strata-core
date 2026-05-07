# Storage Crate Map

## Purpose

This document maps `crates/storage` after the storage consolidation and the
storage-boundary normalization.

It describes the settled lower-runtime substrate that engine and the current
upper crates build on.

For the target boundary of the clean storage layer, see
[storage-charter.md](./storage-charter.md).

For the explicit allowlist of storage operations the consolidated engine may
consume, see
[v1-storage-consumption-contract.md](./v1-storage-consumption-contract.md).

For the cross-boundary ownership question with engine, see
[storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md).

The important takeaway is that `strata-storage` is the real lower runtime
anchor of the system. Engine consolidation has absorbed security/open options,
product open/bootstrap, graph, vector, and search. Storage should remain the
generic persistence substrate while the remaining upper-layer cleanup removes
executor's direct storage bypass.

## High-Level Shape

Top-level source files:

- [crates/storage/src/lib.rs](../../crates/storage/src/lib.rs)
- [crates/storage/src/layout.rs](../../crates/storage/src/layout.rs)
- [crates/storage/src/traits.rs](../../crates/storage/src/traits.rs)
- [crates/storage/src/error.rs](../../crates/storage/src/error.rs)
- [crates/storage/src/segment.rs](../../crates/storage/src/segment.rs)
- [crates/storage/src/segmented/mod.rs](../../crates/storage/src/segmented/mod.rs)

Major supporting files:

- [block_cache.rs](../../crates/storage/src/block_cache.rs)
- [memtable.rs](../../crates/storage/src/memtable.rs)
- [index.rs](../../crates/storage/src/index.rs)
- [stored_value.rs](../../crates/storage/src/stored_value.rs)
- [ttl.rs](../../crates/storage/src/ttl.rs)
- [quarantine.rs](../../crates/storage/src/quarantine.rs)
- [manifest.rs](../../crates/storage/src/manifest.rs)
- [compaction.rs](../../crates/storage/src/compaction.rs)
- [key_encoding.rs](../../crates/storage/src/key_encoding.rs)
- [runtime_config.rs](../../crates/storage/src/runtime_config.rs)

Major subtrees:

- `segmented/` — the real storage engine surface, including recovery,
  compaction, ref tracking, and quarantine protocol
- `durability/` — WAL, snapshot/checkpoint, MANIFEST, decoded-row install,
  and recovery-bootstrap mechanics
- `txn/` — generic transaction context, validation, and manager mechanics

The crate is already substantial. The heaviest ownership points are:

- [segmented/mod.rs](../../crates/storage/src/segmented/mod.rs)
- [segment.rs](../../crates/storage/src/segment.rs)
- [error.rs](../../crates/storage/src/error.rs)
- [layout.rs](../../crates/storage/src/layout.rs)
- [traits.rs](../../crates/storage/src/traits.rs)

## Public Surface

The public re-exports in [lib.rs](../../crates/storage/src/lib.rs) define the
crate's effective surface.

Today `strata-storage` re-exports:

- storage layout/addressing types:
  - `Key`
  - `Namespace`
  - `TypeTag`
  - `validate_space_name`
- storage boundary types:
  - `Storage`
  - `WriteMode`
  - `StorageError`
  - `StorageResult`
- storage engine types:
  - `SegmentedStore`
  - `VersionedEntry`
  - `StoredValue`
- storage runtime config types:
  - `StorageRuntimeConfig`
  - `StorageBlockCacheConfig`
- durability runtime types:
  - WAL reader/writer, codec, layout, and format types
  - checkpoint, compaction, snapshot-prune, and MANIFEST-sync helpers
  - decoded snapshot-row install helpers
  - storage recovery bootstrap input/outcome/error types
- compaction and quarantine support:
  - compaction helpers
  - quarantine helpers

This means storage owns the persistence substrate boundary in active code.

## Current Dependency Shape

### Direct Outgoing Workspace Dependency

`strata-storage` currently depends on:

- `strata-core`

That is the clean shape we wanted from the earlier storage boundary cleanup.

### Incoming Workspace Dependents

The internal incoming graph today is:

- `strata-engine`
- `strata-executor`

The root `stratadb` package also depends on storage in dev/test paths.

This confirms that storage is already the effective substrate node of the
workspace. The direct upper-crate storage edges are current transitional
workspace shape, not permission for upper layers to drive database recovery,
checkpoint, open, retention, or product policy below engine.

`EG4` removed `strata-graph` as a separate storage consumer by moving the graph
implementation into `strata-engine`. `EG5` removed `strata-vector` as a separate
storage consumer by moving vector implementation into `strata-engine` and
deleting the retired vector crate. `EG6` removed `strata-search` as a separate
storage consumer by moving search implementation into `strata-engine` and
deleting the retired search crate.

## What Storage Already Owns

Today the crate already owns the following responsibilities directly:

### 1. Physical Storage Layout

In [layout.rs](../../crates/storage/src/layout.rs), storage owns:

- `Key`
- `Namespace`
- `TypeTag`
- `validate_space_name`

These are no longer living in `core` as active ownership.

### 2. Storage Interface Boundary

In [traits.rs](../../crates/storage/src/traits.rs), storage owns:

- `Storage`
- `WriteMode`

This is the lower persistence boundary the rest of the stack builds on.

### 3. MVCC Storage Engine

In [segmented/mod.rs](../../crates/storage/src/segmented/mod.rs) and
[segment.rs](../../crates/storage/src/segment.rs), storage owns:

- versioned reads and writes
- atomic apply operations
- TTL and tombstone behavior
- scan/range/count operations
- branch-scoped physical state
- quarantine and corruption handling

### 4. Storage-Local Error Taxonomy

In [error.rs](../../crates/storage/src/error.rs), storage owns:

- `StorageError`
- `StorageResult`
- storage corruption and IO classification

### 5. Durability Runtime

In [durability/](../../crates/storage/src/durability), storage owns:

- WAL read/write, codec, and payload mechanics
- snapshot/checkpoint file mechanics
- generic MANIFEST load/create/update mechanics
- checkpoint, WAL compaction, snapshot pruning, and MANIFEST sync helpers
- generic decoded-row snapshot install into `SegmentedStore`
- recovery bootstrap mechanics through
  [recovery_bootstrap.rs](../../crates/storage/src/durability/recovery_bootstrap.rs)

### 6. Storage Runtime Configuration

In [runtime_config.rs](../../crates/storage/src/runtime_config.rs), storage
owns:

- storage runtime config defaults
- effective storage knob derivation from raw storage inputs
- store-local application to `SegmentedStore`
- process-global block-cache capacity application

Engine owns the public `StrataConfig` / `StorageConfig` surface and adapts it
into this storage-owned runtime config.

## What Storage Now Owns

Storage now owns the full lower runtime substrate.

The major substrate responsibilities now physically owned here are:

- generic transaction runtime
- OCC validation
- commit coordination
- WAL runtime
- snapshot runtime
- checkpoint and WAL compaction runtime
- generic decoded-row snapshot install runtime
- recovery bootstrap and replay coordination
- storage runtime config derivation and application

## Current Architectural Role

If you describe the crate honestly as it exists today, `strata-storage` is:

- the physical keyspace owner
- the MVCC persistence engine
- the storage error owner
- the durability runtime owner
- the substrate API boundary for the rest of the stack

It is not merely a bag of data structures. It is the real lower-runtime owner
of the workspace. The lower-runtime peer crates have already been collapsed
into it.

## Main Takeaway

The next storage cleanup should not invent a new role for this crate. The role
is already visible.

The remaining work is above the substrate now:

- keep primitive semantics out of the lower layer
- let `engine` converge on its own clean domain/runtime boundary
- keep the documented storage seams raw, mechanical, and policy-free

## Engine Boundary Seams

Storage exposes a small set of storage-owned runtime helpers to engine. These
helpers are intentional seams, not invitations for engine policy to move down:

| Surface | Storage Owns |
|---|---|
| Checkpoint and WAL compaction | `checkpoint_runtime.rs` helpers that operate on checkpoint files, WAL segments, MANIFEST state, and raw outcomes |
| Snapshot install | `decoded_snapshot_install.rs` validation and install for already decoded storage rows |
| Recovery bootstrap | `recovery_bootstrap.rs` MANIFEST prep, codec validation, replay, segment recovery, and raw recovery facts |
| Runtime config | `runtime_config.rs` effective storage values and application to stores/global cache |
| Retention | storage snapshot pruning mechanics and minimum-retain invariant |

The matching engine responsibilities are documented in
[../engine/engine-crate-map.md](../engine/engine-crate-map.md) and
[storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md).
