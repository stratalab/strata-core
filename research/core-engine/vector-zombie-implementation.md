# Vector Zombie Implementation

This note is derived from the code, not from the existing docs.
It focuses on the vector-specific persistence and recovery paths and asks a
simple question:

Which vector durability path is actually live, and which parts are a dormant
parallel stack?

## Executive view

The vector subsystem already has one real persistence architecture:

- persistent vector state is stored as normal engine keys under `TypeTag::Vector`
- writes go through the engine transaction and WAL path
- restart recovery is performed by `VectorSubsystem`, which rebuilds state from
  KV plus mmap caches
- checkpoints use the engine's `CheckpointData` / `VectorSnapshotEntry` path

Alongside that live design, the vector crate still exposes two separate,
production-shaped durability modules that are not on the active engine path:

- `crates/vector/src/wal.rs`
- `crates/vector/src/snapshot.rs`

Those two modules are the vector-specific zombie implementation.

They are substantial enough to look like a real persistence design, but the
engine does not actually use them.

## 1. The live vector durability path

### Writes are normal engine transactions over `TypeTag::Vector`

The real vector write path is not a vector-private WAL.

Collection configuration is stored as a normal vector-typed KV row under
`__config__/{collection}`:

- [`Key::new_vector_config`](/home/anibjoshi/Documents/GitHub/strata-core/crates/core/src/types.rs)
- [`collections.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/store/collections.rs)

Vector records are stored as normal `Value::Bytes` payloads in the engine's
transaction path:

- [`crud.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/store/crud.rs)
- [`store/mod.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/store/mod.rs)

That means vector mutations are already covered by the same commit pipeline as
the rest of the engine:

- transaction buffer
- engine WAL append
- storage apply
- MVCC visibility

This is the authoritative vector durability path today.

### Recovery is `VectorSubsystem` rebuilding from KV plus caches

The real restart path is in
[`recovery.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/recovery.rs).

`VectorSubsystem` is the active lifecycle hook:

- `recover()` scans vector config rows and vector records from storage
- it reconstructs backends from KV state
- it tries mmap-backed heap and graph caches first
- it registers a refresh hook for follower catch-up
- `freeze()` delegates to `db.freeze_vector_heaps()`

This is not speculative code. It is the subsystem the executor deliberately
installs in production.

### The live crash/restart accelerator is mmap cache, not vector-native snapshot

The real persisted acceleration path is:

- `.vec` heap mmap files
- `.hgr` sealed graph files
- freeze on shutdown/drop
- reload on reopen via `VectorSubsystem`

See:

- [`recovery.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/recovery.rs)
- [`lifecycle.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/lifecycle.rs)

That is the active vector fast-recovery design.

### Checkpoints use the engine durability format, not vector-native snapshots

The engine checkpoint path in
[`database/compaction.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/compaction.rs)
does not call vector-native snapshot serialization.

Instead it:

- scans live vector config rows and vector records from storage
- converts them into durability-format `VectorCollectionSnapshotEntry` and
  `VectorSnapshotEntry`
- appends them to `CheckpointData`

So the real snapshot/checkpoint path for vectors is already the general engine
checkpoint system, not a vector-private one.

## 2. The vector-specific WAL stack

### What it implements

[`wal.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/wal.rs)
defines a full alternate WAL layer for vectors:

- dedicated entry type constants
- typed payload structs for collection create/delete and vector upsert/delete
- MessagePack serialization
- `VectorWalReplayer`
- helper constructors for vector WAL payloads

The module comments describe this as a real replay model, including a warning
about full embeddings being stored directly in the WAL payload.

This is not toy code.

### Why it is a zombie implementation

The problem is not that the module is fake.
The problem is that the engine does not use it.

Repo-wide caller inspection outside `crates/vector/src` found no live runtime
callers for:

- `VectorWalReplayer`
- `create_wal_upsert`
- `create_wal_delete`
- `create_wal_collection_create`
- `create_wal_collection_delete`

The actual vector write path goes through:

- `db.transaction(...)`
- `txn.put(...)`
- normal engine WAL

So the vector crate exposes a second WAL design that the live engine never
replays.

### Why it is architecturally harmful

This dormant stack creates the false impression that vectors have a dedicated
durability contract when they do not.

That confusion shows up in three ways:

1. There appear to be two answers to "how are vector writes persisted?"
2. The dormant WAL stores full embeddings directly in payloads, which is
   structurally different from the live engine path.
3. The crate root re-exports the vector WAL API in
   [`lib.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/lib.rs),
   which makes the dormant path look public and supported.

## 3. The vector-specific snapshot stack

### What it implements

[`snapshot.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/snapshot.rs)
implements a full vector-native snapshot format:

- deterministic collection ordering
- explicit snapshot format versioning
- collection headers with `next_id` and `free_slots`
- full vector embedding serialization
- deserialization back into `VectorStore`

Again, this is a real format, not a placeholder.

### Why it is a zombie implementation

The active engine checkpoint path does not call:

- `snapshot_serialize`
- `snapshot_deserialize`

Instead the engine collects vector data itself and writes it through the
durability crate's generic checkpoint primitives.

So the vector crate still contains a second snapshot design that is not part of
the real checkpoint/recovery chain.

### Why it is architecturally harmful

This creates a second answer to "how are vectors checkpointed?"

The dormant answer is:

- vector-native binary snapshot format

The live answer is:

- engine checkpoint data using `VectorCollectionSnapshotEntry` and
  `VectorSnapshotEntry`

Only the second one is real in production.

That duplication makes the codebase look more flexible than it actually is,
while increasing maintenance burden and reviewer confusion.

## 4. Why these modules are zombie implementations rather than just optional helpers

Not every unused module is a zombie migration.
These are, because all of the following are true:

1. they model core persistence concerns, not incidental tooling
2. they are production-shaped and documented as real formats
3. the engine already has another authoritative persistence path for the same
   primitive
4. the dormant path is still exported from the vector crate root

That combination is what makes them dangerous.

If they were only tests or private conversion helpers, they would be harmless.
They are not.

## 5. What the vector architecture actually wants

The live code suggests a simpler, cleaner vector durability model:

1. vectors persist as normal engine data under `TypeTag::Vector`
2. engine WAL is the only WAL
3. `VectorSubsystem` reconstructs runtime index state from persisted data
4. mmap heap/graph files are caches and recovery accelerators
5. engine checkpoints own snapshot format and recovery coverage

That is already the de facto design.

The vector-specific WAL and snapshot modules are leftovers from a parallel
design that is no longer authoritative.

## 6. Recommended direction

Based on the code as it exists today, the cleanest direction is:

1. treat `crates/vector/src/wal.rs` as dead architecture unless there is a real
   plan to move vector durability onto a separate WAL, which the current engine
   design does not need
2. treat `crates/vector/src/snapshot.rs` as dead architecture unless the engine
   is going to stop using `CheckpointData` for vectors, which would be a step
   backward
3. keep and strengthen the real path:
   - transaction-backed vector rows
   - `VectorSubsystem` recovery
   - mmap cache freeze/reload
   - engine checkpoint collection
4. stop re-exporting dormant persistence APIs from the vector crate root if they
   are not part of the supported architecture

## Bottom line

The vector subsystem already has one real persistence story:

- engine WAL
- vector rows in storage
- subsystem rebuild on open
- mmap caches for acceleration
- engine checkpoint data for snapshots

`vector::wal` and `vector::snapshot` are not part of that story.
They are production-shaped parallel implementations that the engine no longer
uses.

That makes them zombie architecture, not just unused code.
