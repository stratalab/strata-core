# Storage Crate Map

## Purpose

This document is a baseline map of `crates/storage` as it exists today.

It is not a target design. It is a description of the current ownership and
behavior in the storage crate, written to support the next major storage-side
consolidation effort.

For the target boundary of the clean storage layer, see
[storage-charter.md](./storage-charter.md).

For the cross-boundary ownership question with engine, see
[storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md).

The important takeaway is that `strata-storage` is already the real lower
runtime anchor of the system. The remaining work is no longer about finding
the right lower-runtime owner. It is about severing the final legacy
dependencies above it cleanly.

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

Major subtrees:

- `segmented/` — the real storage engine surface, including recovery,
  compaction, ref tracking, and quarantine protocol
- `durability/` — WAL, snapshot/checkpoint, MANIFEST, decoded-row install,
  and recovery-bootstrap mechanics

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
  - `VersionedValue`
  - `StoredValue`
- durability runtime types:
  - WAL reader/writer, codec, layout, and format types
  - checkpoint, compaction, snapshot-prune, and MANIFEST-sync helpers
  - decoded snapshot-row install helpers
  - storage recovery bootstrap input/outcome/error types
- compaction and quarantine support:
  - compaction helpers
  - quarantine helpers

This means storage already owns the persistence substrate boundary in active
code, even though some neighboring runtime responsibilities still live in
other crates.

## Current Dependency Shape

### Direct Outgoing Workspace Dependency

`strata-storage` currently depends on:

- `strata-core`

That is the clean shape we wanted from the earlier storage boundary cleanup.

### Incoming Workspace Dependents

The internal incoming graph today is:

- `strata-engine`
- `strata-executor`
- `strata-graph`
- `strata-search`
- `strata-vector`

The root `stratadb` package also depends on storage in dev/test paths.

This confirms that storage is already the effective substrate node of the
workspace.

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
