# ST4 Durability Runtime Absorption Plan

## Purpose

`ST4` is the lower-runtime absorption epic that follows `ST3`.

Its purpose is to move the remaining durability runtime out of
`strata-durability` and `strata-concurrency` into `strata-storage` without
mixing in the branch-oriented `branch_bundle` workflow that belongs in
`engine`.

This document is grounded in the current post-`ST3` codebase. It names the
exact remaining source modules, the exact engine call sites that still depend
on them, and the exact pieces that must wait for `ST5`.

Read this together with:

- [storage-minimal-surface-implementation-plan.md](./storage-minimal-surface-implementation-plan.md)
- [storage-charter.md](./storage-charter.md)
- [storage-crate-map.md](./storage-crate-map.md)
- [concurrency-crate-map.md](./concurrency-crate-map.md)
- [durability-crate-map.md](./durability-crate-map.md)
- [storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md)
- [st3-generic-transaction-runtime-absorption-plan.md](./st3-generic-transaction-runtime-absorption-plan.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)
- [../architecture/architecture-recovery-target.md](../architecture/architecture-recovery-target.md)

Status note:

- `ST4A` is complete: the pure durability runtime now lives under
  `strata_storage::durability`.
- `ST4B` is complete: WAL payload serialization and durability commit
  adaptation now live under `strata_storage::durability`.
- `ST4C` is complete: recovery now lives under
  `strata_storage::durability`.
- `ST4D` is complete: `strata-concurrency` has been deleted, and
  `strata-durability` has been reduced to `branch_bundle` only.

## ST4 Verdict

After `ST3`, the generic transaction runtime is already storage-owned.

`ST4` is now complete.

What remains outside `storage` is no longer lower runtime. It is the
engine-owned `branch_bundle` workflow:

- `crates/durability/src/branch_bundle/*`
- `crates/engine/src/bundle.rs`
- `tests/integration/branching_generation_migration.rs`

What does **not** belong in `ST4` is:

- `crates/durability/src/branch_bundle/*`
- `crates/engine/src/bundle.rs`
- branch-oriented bundle DTOs and archive workflows

Those are engine-owned and should move in `ST5`, not be normalized into the
storage runtime.

## Why `ST4` Is Not “Move The Whole Crate”

`crates/durability/src` is not one coherent ownership unit.

Most of it is substrate runtime:

- `wal/`
- `disk_snapshot/`
- `format/`
- `codec/`
- `layout.rs`
- `compaction/`

But one subtree is clearly higher-level:

- `branch_bundle/`

At the same time, `storage` already has its own:

- `layout.rs`
- `manifest.rs`
- `compaction.rs`

So `ST4` cannot be “dump every durability file into storage root and see what
survives.” It needs a deliberate destination layout that:

- keeps the lower runtime physically owned by `storage`
- avoids name collisions with existing storage modules
- does not reintroduce a fake middle layer inside `storage`
- leaves `branch_bundle` out of scope until `ST5`

## Historical Code Map

This section records the pre-`ST4` source layout that the move was based on.
It is no longer the live crate graph after `ST4D`.

### Move In `ST4`

These files are lower-runtime substrate code and should move into `storage`.

#### `crates/concurrency/src/payload.rs`

This file owns the WAL payload envelope used by committed transactions:

- `TransactionPayload`
- `PayloadError`
- `serialize_wal_record_into(...)`

It already depends on storage-owned `TransactionContext`, and its only
remaining reason to live in `concurrency` is that it writes
`strata_durability::format::WalRecord` bytes directly.

That is now a storage concern.

#### `crates/concurrency/src/recovery.rs`

This file owns the lower recovery driver:

- `RecoveryCoordinator`
- `RecoveryPlan`
- `CoordinatorRecoveryError`
- `RecoveryResult`
- `RecoveryStats`
- `manifest_error_to_strata_error(...)`
- `apply_wal_record_to_memory_storage(...)`

It is callback-driven and lower-layer by design. It constructs no engine
runtime state and should be a storage-owned recovery seam.

#### `crates/concurrency/src/manager.rs`

After `ST3`, the generic manager core is already gone from this file.

What remains here is the WAL-aware durability adapter:

- thread-local WAL serialization buffers
- `WalMode`
- apply-failure injection hooks
- `commit_inner(...)`
- `commit_with_wal(...)`
- `commit_with_wal_arc(...)`
- `commit_with_version(...)`

These functions already take a storage-owned `TransactionManager`. They are the
commit-time durability hook and should move next to the WAL payload/runtime in
`storage`.

#### `crates/durability/src/codec/*`

This subtree owns:

- `StorageCodec`
- `CodecError`
- `IdentityCodec`
- AES-GCM codec support
- `clone_codec(...)`
- `get_codec(...)`

This is substrate byte-encoding ownership.

#### `crates/durability/src/format/*`

This subtree owns the durable byte formats:

- `Manifest`, `ManifestManager`, `ManifestError`
- `WalRecord`, `WalSegment`, segment header constants and errors
- snapshot headers and primitive snapshot DTOs
- segment metadata
- writeset and watermark types
- `primitive_tags`
- `SnapshotSerializer`

This belongs with the persistence substrate, not in a sibling crate.

#### `crates/durability/src/wal/*`

This subtree owns:

- `WalWriter`
- `WalReader`
- `WalConfig`
- `DurabilityMode`
- `WalCounters`
- `WalDiskUsage`
- read-stop / iterator helpers

This is core storage runtime.

#### `crates/durability/src/disk_snapshot/*`

This subtree owns:

- `SnapshotWriter`
- `SnapshotReader`
- `LoadedSnapshot`
- `LoadedSection`
- `SnapshotSection`
- `CheckpointCoordinator`
- `CheckpointData`
- snapshot read/write errors

This is also lower runtime.

#### `crates/durability/src/layout.rs`

This file owns `DatabaseLayout`, the canonical disk path model for:

- `wal/`
- `segments/`
- `snapshots/`
- `MANIFEST`
- follower state / audit files

It is substrate infrastructure and should move into `storage`.

#### `crates/durability/src/compaction/*`

This subtree owns:

- `WalOnlyCompactor`
- `CompactMode`
- `CompactInfo`
- `CompactionError`
- tombstone tracking DTOs

This is durability/runtime behavior, not engine-domain logic.

#### `crates/durability/src/lib.rs`

The crate root is also part of the move because it currently:

- re-exports the lower runtime surface
- contains `now_micros()`
- exposes the `engine-internal` `__internal` seam

The engine-only helpers in `__internal` are still lower-runtime details:

- `BackgroundSyncError`
- `BackgroundSyncHandle`
- `WalWriterEngineExt`

They should move with the WAL runtime instead of preserving a standalone
durability crate only for internal traits.

### Wait For `ST5`

These files should **not** move in `ST4`:

- `crates/durability/src/branch_bundle/error.rs`
- `crates/durability/src/branch_bundle/mod.rs`
- `crates/durability/src/branch_bundle/reader.rs`
- `crates/durability/src/branch_bundle/types.rs`
- `crates/durability/src/branch_bundle/wal_log.rs`
- `crates/durability/src/branch_bundle/writer.rs`
- `crates/engine/src/bundle.rs`
- `tests/integration/branching_generation_migration.rs`

`branch_bundle` is branch-domain archive behavior layered on top of lower
durability primitives. `ST4` should leave it where it is and let `ST5` lift it
into `engine`.

## Active Callers That `ST4` Must Repoint

### Engine Runtime Callers

The main runtime call sites still consuming `concurrency` / `durability`
directly are:

- `crates/engine/src/coordinator.rs`
  - `commit_durable_with_wal`
  - `commit_durable_with_wal_arc`
  - `commit_durable_with_version`
- `crates/engine/src/database/recovery.rs`
  - `RecoveryCoordinator`
  - `RecoveryResult`
  - `RecoveryStats`
  - `CoordinatorRecoveryError`
  - `DatabaseLayout`
  - `ManifestManager`
  - codec resolution
- `crates/engine/src/database/recovery_error.rs`
  - typed references to `CoordinatorRecoveryError`
  - `ManifestError`
  - `SnapshotReadError`
  - `WalReaderError`
- `crates/engine/src/database/lifecycle.rs`
  - `ManifestManager`
  - WAL reader construction
  - `TransactionPayload::from_bytes(...)`
- `crates/engine/src/database/open.rs`
  - `DatabaseLayout`
  - `WalWriter`
  - `WalConfig`
  - `DurabilityMode`
  - codec resolution
  - `WalWriterEngineExt`
- `crates/engine/src/database/mod.rs`
  - `WalWriter`
  - `WalCounters`
  - `WalDiskUsage`
  - `WalWriterHealth` background sync wiring
  - `WalWriterEngineExt`
- `crates/engine/src/database/compaction.rs`
  - `ManifestManager`
  - `CheckpointCoordinator`
  - `CheckpointData`
  - `WalOnlyCompactor`
  - snapshot entry DTOs
  - `WalWriterEngineExt`
- `crates/engine/src/database/transaction.rs`
  - `ManifestManager`
  - `WalOnlyCompactor`
- `crates/engine/src/database/snapshot_install.rs`
  - `LoadedSnapshot`
  - `SnapshotSerializer`
  - `primitive_tags`

There are also crate-root engine re-exports that should move from
`strata_durability` to `strata_storage`:

- `crates/engine/src/lib.rs`
  - `DurabilityMode`
  - `WalCounters`

### Engine Tests

The test surface still imports many durability/runtime types directly:

- `crates/engine/tests/flush_pipeline_tests.rs`
- `crates/engine/tests/recovery_parity.rs`
- `crates/engine/src/database/tests/mod.rs`
- `crates/engine/src/database/tests/open.rs`
- `crates/engine/src/database/tests/checkpoint.rs`
- `crates/engine/src/database/tests/codec.rs`
- `crates/engine/src/database/tests/regressions.rs`
- `crates/engine/src/database/tests/shutdown.rs`
- `crates/engine/src/database/tests/snapshot_retention.rs`

`ST4` must move those tests to the canonical storage-owned durability surface,
not leave them on a compatibility path.

## Destination Layout In `storage`

Because `storage` already has root modules named `layout`, `manifest`, and
`compaction`, the durability runtime should land in a dedicated subtree rather
than at crate root.

Recommended destination:

- `crates/storage/src/durability/mod.rs`
- `crates/storage/src/durability/codec/`
- `crates/storage/src/durability/format/`
- `crates/storage/src/durability/wal/`
- `crates/storage/src/durability/disk_snapshot/`
- `crates/storage/src/durability/layout.rs`
- `crates/storage/src/durability/compaction/`
- `crates/storage/src/durability/payload.rs`
- `crates/storage/src/durability/recovery.rs`

Recommended ownership inside that subtree:

- `codec/`
  - storage codecs and codec helpers
- `format/`
  - durable byte-format ownership
- `wal/`
  - WAL runtime
- `disk_snapshot/`
  - snapshot/checkpoint runtime
- `layout.rs`
  - `DatabaseLayout`
- `compaction/`
  - WAL-only compaction and tombstones
- `payload.rs`
  - transaction payload serialization / WAL envelope glue
- `recovery.rs`
  - callback-driven lower recovery coordinator

Recommended internal seam for engine-only helpers:

- `crates/storage/src/durability/__internal.rs`

That file should own the current background-sync integration helpers instead of
leaving them behind as `strata_durability::__internal`.

Public surface recommendation:

- canonical path: `strata_storage::durability::*` (no crate-root re-exports)

The durability runtime is a self-contained subsystem with ~25 active types. Re-
exporting them at the `strata_storage` crate root would splatter the namespace
and obscure the layering for readers; keeping them under
`strata_storage::durability::{wal,disk_snapshot,format,codec,...}` lets the
import paths describe the architecture. Engine, graph, and vector consume the
fully qualified paths consistently.

This gives upper crates one canonical import path while keeping the lower
runtime organized inside `storage`.

## Execution Order

`ST4` should be executed as four small slices.

### ST4A — Durability Subtree Scaffolding And Pure Runtime Move

Create the storage durability subtree and move the pure durability building
blocks first:

- `codec/*`
- `format/*`
- `wal/*`
- `disk_snapshot/*`
- `layout.rs`
- `compaction/*`
- crate-root utility helpers such as `now_micros()`
- `__internal` background-sync traits

This slice should:

- update `crates/storage/Cargo.toml` with only the dependencies required by
  the moved runtime
- avoid moving branch-bundle-only archive dependencies such as `tar`, `zstd`,
  and checksum/archive helpers if they are only needed by `branch_bundle`
- keep `strata-durability` as a compatibility re-export shim for moved runtime
  modules while `branch_bundle` still lives there

This slice is mostly physical relocation and import repointing. It should not
change engine behavior.

### ST4B — WAL Payload And Commit Adapter Absorption

Move the last durability-coupled commit glue out of `concurrency`:

- `crates/concurrency/src/payload.rs`
- the WAL-aware adapter functions from `crates/concurrency/src/manager.rs`

This slice should:

- place payload serialization under the storage durability subtree
- place the WAL-aware commit adapter next to the storage-owned
  `TransactionManager`
- repoint `crates/engine/src/coordinator.rs` to the storage-owned adapter
- move the apply-failure test hooks with the adapter
- remove the remaining runtime reason for `engine` to depend on
  `strata-concurrency`

At the end of this slice, `strata-concurrency` should be reduced to recovery
only, or a near-empty shim waiting for the recovery move.

### ST4C — Recovery Coordinator Absorption

Move `crates/concurrency/src/recovery.rs` into the storage durability subtree.

This slice should:

- make `RecoveryCoordinator` storage-owned
- move `RecoveryPlan`, `RecoveryResult`, `RecoveryStats`, and
  `CoordinatorRecoveryError` with it
- move `apply_wal_record_to_memory_storage(...)`
- repoint `crates/engine/src/database/recovery.rs`
- repoint `crates/engine/src/database/recovery_error.rs`
- repoint `crates/engine/tests/recovery_parity.rs`

This is the slice that should make `strata-concurrency` deletable.

### ST4D — Import Contraction And Transitional Crate Deletion

Once the durability subtree and recovery seam are storage-owned:

- repoint all remaining engine runtime imports from `strata_durability` to
  `strata_storage`
- repoint engine tests to the same canonical path
- delete `strata-concurrency`
- reduce `strata-durability` to `branch_bundle` only
- tighten import guards so new runtime code cannot slide back to the old crates

At the end of `ST4`:

- `concurrency` should be gone
- `durability` should be only the branch-bundle outlier waiting for `ST5`

## Constraints

### 1. No `storage -> core-legacy`

`strata-durability` and `strata-concurrency` currently depend on
`strata-core-legacy`. `ST4` must not recreate that edge in `storage`.

When moved code still imports `strata_core::*`, those imports must be rewritten
to the canonical `strata-core` / `strata-storage` surfaces or replaced with
storage-local DTOs as needed.

### 2. No Branch Bundle In `ST4`

Do not quietly normalize `branch_bundle` into `storage`.

If archive-specific dependencies remain after moving the substrate runtime,
they are a signal that `branch_bundle` has not been isolated yet, not a reason
to broaden `storage`.

### 3. No On-Disk Format Churn

Moving runtime ownership does not justify changing:

- WAL record bytes
- snapshot bytes
- MANIFEST bytes
- directory layout
- checkpoint payload shape

If a format migration is needed, it must be its own explicit change.

### 4. No Engine API Redesign Here

`Database::open`, `checkpoint`, `compact`, `refresh`, `shutdown`,
`wal_writer_health`, and related engine APIs should keep the same public
contracts. `ST4` is an ownership rewrite, not a product-surface redesign.

### 5. Do Not Move Engine Orchestration Wholesale

The storage↔engine audit identified lower-runtime implementation inside engine
files such as:

- `database/recovery.rs`
- `database/compaction.rs`
- `database/open.rs`

`ST4` may shrink their imports and helper seams, but it does not need to move
those whole modules into `storage`. The goal here is to absorb the standalone
durability runtime first.

## Definition Of Done

`ST4` is complete only when all of the following are true:

1. WAL / snapshot / MANIFEST / codec / layout / compaction ownership is
   physically in `strata-storage`.
2. Recovery coordination is physically in `strata-storage`.
3. Transaction WAL payload serialization and durability commit adapters are
   physically in `strata-storage`.
4. Active engine runtime callers no longer import substrate durability types
   from `strata-durability`.
5. Active engine runtime callers no longer import durability commit/recovery
   seams from `strata-concurrency`.
6. `strata-concurrency` is deleted.
7. `strata-durability` contains only `branch_bundle` or is a minimal shim
   pending `ST5`.
8. `strata-storage` still has no dependency on `strata-core-legacy`.

## Verification Gates

Every `ST4` slice should run its own local suite, but the epic is not done
until the full matrix is green.

Recommended gates:

- `cargo test -p strata-storage`
- `cargo test -p strata-durability`
- `cargo test -p strata-engine`
- `cargo test -p strata-graph`
- `cargo test -p strata-vector`
- `cargo test -p strata-executor`
- `cargo test -p stratadb --test engine`
- `cargo test -p stratadb --test integration`
- `cargo check --workspace --all-targets`

Additional ST4-specific checks:

- grep audit proving active runtime imports of lower durability surfaces now
  come from `strata_storage`
- import guard(s) preventing new use of `strata-durability` for substrate
  runtime APIs
- import guard(s) preventing new use of `strata-concurrency` after its
  remaining shell is absorbed

## End-State After `ST4`

If `ST4` lands cleanly, the lower graph should look like this:

- `strata-storage`
  - storage engine
  - transaction runtime
  - durability runtime
  - recovery/replay runtime
- `strata-engine`
  - database semantics consuming storage-owned runtime
- `strata-durability`
  - only `branch_bundle` until `ST5`

That is the point where `ST5` becomes a clean, contained branch-bundle lift
instead of another mixed lower-runtime consolidation.
