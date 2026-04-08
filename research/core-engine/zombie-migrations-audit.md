# Zombie Migration Audit

This note is code-derived.
It looks for modules that have the shape of a newer or more complete subsystem,
but are not on the engine's active runtime path because older or parallel code
still owns production behavior.

The test for a "zombie migration" in this audit is:

1. the module is substantial enough to represent a real subsystem or format
2. its comments or API surface imply intended runtime use
3. the live engine path still routes around it

## Executive view

The codebase does have additional zombie-migration-style modules beyond the
ones already called out in the section notes.

The strongest confirmed cases are:

1. the lower-level durability database handle and recovery stack
2. the vector-specific WAL stack
3. the vector-specific snapshot stack

There is also one strong likely case:

4. the multi-process WAL coordination path

And there are a few previously documented control-plane modules that fit the
same pattern, but were already covered elsewhere:

- `storage::CompactionScheduler`
- `storage::TTLIndex`
- `storage::MemoryPressure`
- `durability::TombstoneIndex`

## Confirmed Shadow Stacks

### 1. `strata_durability::database::*` is a real lower-level database runtime that the engine does not use

Why it looks like the newer architecture:

- [`handle.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/database/handle.rs) defines `DatabaseHandle`, which owns:
  - MANIFEST
  - WAL writer
  - checkpoint coordinator
  - codec
  - recovery entrypoint
- its module comments explicitly say it is a lower-level API used by the engine layer
- [`paths.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/database/paths.rs) defines a full database directory model
- [`config.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/database/config.rs) defines a parallel persistence config surface

Why it is shadowed:

- the live engine open path in [`open.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/open.rs) does not call `DatabaseHandle`
- the active runtime stitches persistence together itself from:
  - `strata_concurrency::RecoveryCoordinator`
  - `strata_durability::ManifestManager`
  - direct `WalWriter` setup
  - engine-managed directory layout
- repo-wide caller search outside `crates/durability/src` found no engine/executor/runtime calls to `DatabaseHandle` or `DatabasePaths`

Why this matters:

- this is not just a helper left around for tests
- it is a full alternate database/persistence ownership layer with its own layout and lifecycle assumptions
- it is exactly the kind of module that suggests an unfinished migration toward a cleaner persistence boundary

Verdict:

- confirmed zombie migration / parallel persistence stack

### 2. `strata_durability::recovery::RecoveryCoordinator` is a richer recovery planner shadowed by the engine's older recovery path

Why it looks like the newer architecture:

- [`coordinator.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/recovery/coordinator.rs) implements a full:
  - MANIFEST load
  - snapshot discovery
  - snapshot watermark handling
  - WAL replay-after-watermark
  - partial-tail truncation
  - `.meta` rebuild
- it returns a `RecoveryPlan` and `RecoverySnapshot`, which is a cleaner architecture than the engine's direct WAL replay path

Why it is shadowed:

- the engine open path still uses [`concurrency/src/recovery.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/concurrency/src/recovery.rs)
- the engine runtime does not call the durability recovery coordinator at open
- this richer coordinator assumes the durability crate's own layout conventions:
  - `WAL/`
  - `SNAPSHOTS/`
  - `DATA/`
- the engine runtime actually uses:
  - `wal/`
  - `segments/`
  - `snapshots/`

Why this matters:

- this is the concrete module-level explanation for why checkpoint recovery exists in infrastructure but not in the real engine open path
- it is stronger evidence than a generic "recovery is split" statement because the replacement stack is already implemented

Verdict:

- confirmed zombie migration / parallel recovery stack

### 3. `strata_vector::wal` is a vector-specific WAL subsystem that the live engine does not use

Why it looks production-grade:

- [`wal.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/wal.rs) defines:
  - typed vector WAL payloads
  - messagepack serialization
  - replay logic via `VectorWalReplayer`
  - explicit entry types for collection create/delete and vector upsert/delete
- the module comments describe transaction-aware replay and format evolution

Why it is shadowed:

- repo-wide caller search outside `crates/vector/src` found no engine/executor/runtime calls to:
  - `VectorWalReplayer`
  - `create_wal_upsert`
  - `create_wal_delete`
  - `create_wal_collection_create`
  - `create_wal_collection_delete`
- the live vector path instead uses:
  - generic engine WAL of typed KV writes
  - `VectorSubsystem` recovery from KV state plus mmap caches in [`recovery.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/recovery.rs)

Why this matters:

- this is not just unused serialization code
- it is a full alternate durability strategy for vectors that the engine has not adopted
- it increases conceptual surface area around "how vectors persist"

Verdict:

- confirmed zombie migration / dormant vector durability path

### 4. `strata_vector::snapshot` is a vector-specific snapshot format shadowed by the engine checkpoint path

Why it looks production-grade:

- [`snapshot.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/vector/src/snapshot.rs) implements:
  - deterministic vector snapshot serialization
  - explicit format versioning
  - collection headers with ID allocator state
  - full snapshot deserialization back into `VectorStore`

Why it is shadowed:

- repo-wide caller search outside `crates/vector/src` found no engine/executor/runtime calls to:
  - `snapshot_serialize`
  - `snapshot_deserialize`
- the engine's real checkpoint path in [`database/compaction.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/compaction.rs) collects durability-format `VectorSnapshotEntry` values into `CheckpointData`
- so the active checkpoint stack uses the durability crate's primitive snapshot format, not the vector crate's own snapshot format

Why this matters:

- there are two separate vector checkpoint stories in the codebase:
  - vector-native snapshot serialization
  - durability primitive snapshot entries
- only one is on the real engine path

Verdict:

- confirmed zombie migration / dormant vector checkpoint format

## Likely Dormant Stacks

### 5. `strata_durability::coordination` looks like an abandoned multi-process commit path

Why it looks like a serious subsystem:

- [`coordination.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/coordination.rs) defines:
  - `WalFileLock`
  - stale-lock recovery
  - `CounterFile` for durable `(max_version, max_txn_id)`
- its comments describe exactly the kind of cross-process coordination the engine would need for true multi-process commit safety

Why it appears dormant:

- caller search found no engine/concurrency/runtime references to `WalFileLock` or `CounterFile`
- the module is feature-gated behind `multi_process` in [`durability/Cargo.toml`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/Cargo.toml) and [`durability/src/lib.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/lib.rs)
- the live engine still uses:
  - an engine-level `.lock` file for database-open exclusion
  - in-memory allocator state in the transaction manager

Why this is only "likely" and not "confirmed":

- unlike the durability recovery stack, this path is explicitly feature-gated
- it may be an intentionally incomplete future path rather than a migration that was supposed to finish

Verdict:

- likely dormant migration / future multi-process path

## Previously Documented, Same Pattern

These are not new findings from this audit, but they do fit the same
"implemented subsystem, not authoritative runtime path" pattern:

### `storage::CompactionScheduler`

- implemented in [`storage/src/compaction.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/compaction.rs)
- no live engine background scheduler calls it
- leveled compaction remains the actual runtime path

### `storage::TTLIndex`

- implemented in [`storage/src/ttl.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/ttl.rs)
- no runtime wiring from engine maintenance
- TTL remains read-time and compaction-time behavior

### `storage::MemoryPressure`

- implemented in [`storage/src/pressure.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/pressure.rs)
- normal runtime still opens the store with pressure disabled
- custom engine backpressure code owns real policy

### `durability::TombstoneIndex`

- implemented in [`durability/src/compaction/tombstone.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/compaction/tombstone.rs)
- no live delete/compaction/recovery wiring
- exists as a library subsystem, not active engine behavior

## Checked And Rejected

These modules looked suspicious at first, but are not good zombie-migration
candidates:

### `engine::background`

- active runtime scheduler
- used by flush and compaction in [`database/transaction.rs`](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/transaction.rs)

### `storage::manifest`

- active segment-placement persistence
- used by `SegmentedStore` recovery and branch lifecycle

### `engine::bundle` + `durability::branch_bundle`

- active through executor branch import/export flows
- not shadowed by an older competing path

### `vector::recovery`

- active when `VectorSubsystem` is installed
- composition-sensitive, but not dormant

### `graph::snapshot`

- test module only
- not a production subsystem that the engine routes around

## Bottom Line

The strongest additional zombie migrations are:

1. the durability crate's database/recovery stack
2. the vector crate's WAL stack
3. the vector crate's snapshot stack

If I were prioritizing cleanup, I would treat them like this:

1. either promote the durability database/recovery stack into the engine's
   real restart path
2. or delete/narrow the durability database facade so the persistence
   architecture is not pretending to be more unified than it is
3. decide whether vector wants its own WAL/snapshot formats at all
4. if not, remove or internalize them so the only supported vector durability
   story is the KV/mmap/subsystem path
