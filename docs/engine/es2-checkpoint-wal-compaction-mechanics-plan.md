# ES2 Checkpoint And WAL Compaction Mechanics Plan

## Purpose

`ES2` is the first runtime-code movement epic in the engine/storage boundary
normalization workstream.

Its purpose is to move generic checkpoint, WAL compaction, snapshot pruning,
flush-time WAL truncation, and MANIFEST watermark/persist mechanics out of
`strata-engine` and into `strata-storage`, while keeping engine as the public
database API and policy owner.

This epic is split into straight lettered phases:

- `ES2A` - boundary API sketch
- `ES2B` - checkpoint and compaction characterization
- `ES2C` - storage checkpoint runtime
- `ES2D` - engine checkpoint wrapper
- `ES2E` - storage WAL compaction runtime
- `ES2F` - engine compaction wrapper
- `ES2G` - residue cleanup and guard pass

Read this together with:

- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
- [es1-boundary-baseline-and-guardrails-plan.md](./es1-boundary-baseline-and-guardrails-plan.md)
- [../storage/storage-engine-ownership-audit.md](../storage/storage-engine-ownership-audit.md)
- [../storage/storage-charter.md](../storage/storage-charter.md)
- [../architecture/architecture-recovery-target.md](../architecture/architecture-recovery-target.md)

Before ES2C code movement starts, also write:

- [es2-es4-storage-runtime-boundary-api-sketch.md](./es2-es4-storage-runtime-boundary-api-sketch.md)

Checkpoint output feeds snapshot install, and snapshot install feeds recovery.
ES2C through ES2G can move code sequentially, but ES2 should not design
checkpoint boundary types in isolation from ES3 and ES4.

## ES2 Verdict

The following should move to `strata-storage`:

- checkpoint runtime around `CheckpointCoordinator`
- snapshot directory creation and permission hardening
- MANIFEST load/create/update mechanics needed by checkpoint and compaction
- existing snapshot watermark extraction from MANIFEST
- checkpoint coordinator construction and execution
- post-checkpoint snapshot pruning mechanics
- WAL compactor construction and execution around `WalOnlyCompactor`
- flush-time WAL truncation mechanics after engine-owned flush decisions
- generic MANIFEST fsync/persist mechanics used by disk-primary shutdown
- raw checkpoint and compaction outcome types

The following must remain in `strata-engine`:

- `Database::checkpoint()`
- `Database::compact()`
- shutdown sequencing and lifecycle checks
- ephemeral and follower no-op policy
- WAL flush through engine shutdown semantics
- coordinator quiescing and transaction watermark choice
- primitive checkpoint data materialization
- public engine error wording and `StrataError` conversion
- database-level logging and operator-facing interpretation

The key rule: storage should own what to do with already-materialized
durability DTOs. Engine should own deciding when to checkpoint, what primitive
state goes into the DTOs, and how raw storage facts are presented to callers.

ES2 intentionally includes the storage mechanics behind the shutdown
`fsync_manifest()` path because that path only needs generic MANIFEST
load/create/active-segment/persist behavior. Engine still owns the shutdown
barrier, final flush, freeze hooks, registry/file-lock release, and all public
shutdown semantics. ES4 remains responsible for the broader recovery/open
manifest policy move.

## Current Code Map

The ES2 target surface is concentrated in:

- [database/compaction.rs](../../crates/engine/src/database/compaction.rs)

Relevant storage-owned types already live below engine:

- `strata_storage::durability::CheckpointCoordinator`
- `strata_storage::durability::CheckpointData`
- `strata_storage::durability::CheckpointError`
- `strata_storage::durability::CompactionError`
- `strata_storage::durability::ManifestManager`
- `strata_storage::durability::ManifestError`
- `strata_storage::durability::WalOnlyCompactor`
- `strata_storage::durability::SnapshotWatermark`
- storage codec helpers under `strata_storage::durability::codec`

### `Database::checkpoint`

The current method mixes engine policy with storage mechanics:

- engine-owned:
  - `check_not_shutting_down()`
  - ephemeral/follower no-op
  - `flush()`
  - `coordinator.quiesced_version()`
  - `collect_checkpoint_data()`
  - `StrataError` mapping
  - database log message
- storage-owned:
  - `snapshots` directory creation
  - MANIFEST load/create
  - existing snapshot watermark reconstruction
  - codec lookup for checkpoint serialization
  - `CheckpointCoordinator` construction
  - checkpoint execution
  - MANIFEST snapshot watermark update
  - snapshot pruning mechanics

### `Database::compact`

The current method also mixes policy with storage mechanics:

- engine-owned:
  - `check_not_shutting_down()`
  - ephemeral/follower no-op
  - writer active segment observation
  - public invalid-input wording when no checkpoint exists
  - database log message
- storage-owned:
  - WAL directory selection from database layout
  - MANIFEST load/create
  - `WalOnlyCompactor` construction
  - codec-aware compactor setup
  - compaction execution
  - raw removed-segment and reclaimed-byte facts

### `Database::collect_checkpoint_data`

This function should not move wholesale in ES2.

It walks `SegmentedStore`, but it also knows primitive materialization rules:

- graph rows are emitted through the KV snapshot section with
  `TypeTag::Graph`
- event metadata keys are skipped
- branch index keys are skipped
- vector collection config rows are recognized by key convention
- vector record bytes are decoded to populate vector snapshot metadata

The ES2 split should assume this stays engine-owned for now. Storage should
accept `CheckpointData` as an input, not learn how to create all primitive
sections from live engine state.

Longer term, storage may own generic scan helpers if they can be kept
primitive-agnostic. The primitive materialization rules should remain in
engine or primitive-owned adapters.

### `Database::load_or_create_manifest`

This helper is generic storage mechanics, but it is shared with nearby engine
open/recovery code.

ES2 should avoid duplicating it. The preferred direction is to move a generic
manifest helper into storage as part of the checkpoint/compaction runtime. If
that creates awkward ES4 coupling, ES2 may leave an engine wrapper temporarily,
but the helper should have a clear owner by the ES4 recovery move.

## Target Storage Surface

Exact names can change during implementation, but ES2C should introduce one
storage-owned runtime module for checkpoint and WAL compaction mechanics.

Candidate module:

```text
crates/storage/src/durability/checkpoint_runtime.rs
```

Candidate exports:

```text
StorageCheckpointInput
StorageCheckpointOutcome
run_storage_checkpoint(input) -> StorageResult<StorageCheckpointOutcome>

StorageWalCompactionInput
StorageWalCompactionOutcome
compact_storage_wal(input) -> Result<StorageWalCompactionOutcome, StorageWalCompactionError>

StorageManifestSyncInput
sync_storage_manifest(input) -> Result<(), StorageManifestRuntimeError>

StorageFlushWalTruncationInput
StorageFlushWalTruncationOutcome
truncate_storage_wal_after_flush(input) -> Result<Option<StorageFlushWalTruncationOutcome>, StorageFlushWalTruncationError>
```

If the existing durability error hierarchy is sufficient, do not create new
error types just to rename them. If a new error type is needed, it should be
storage-local and should not mention `StrataError`, engine health reports, or
operator policy.

### `StorageCheckpointInput`

The input should contain only storage facts and already-materialized data:

```text
layout
database_uuid
checkpoint_codec
manifest_create_codec_id
watermark_txn
checkpoint_data
active_wal_segment
```

Resolved by ES2A:

- use `DatabaseLayout`, not ad-hoc `data_dir` paths
- engine passes an already-created checkpoint codec or an equivalent codec
  handle, plus an explicit `manifest_create_codec_id`
- active WAL segment is a checkpoint input so storage can persist the
  MANIFEST active segment before checkpoint/compaction decisions
- active WAL segment is represented as `NonZeroU64`; segment `0` is rejected
  at the engine boundary instead of being persisted into MANIFEST

Resolved by ES2D:

- snapshot pruning is split out of the checkpoint outcome; storage owns the
  raw pruning helper, while engine keeps lifecycle/configuration policy and
  preserves non-fatal post-checkpoint warning behavior

### `StorageCheckpointOutcome`

The output should return raw facts:

```text
snapshot_id
watermark_txn
checkpoint_timestamp_micros
active_wal_segment
```

Engine should translate this outcome into existing database logging and public
behavior.

ES2C must preserve current missing-MANIFEST behavior unless it explicitly
documents a behavior change. Today `Database::load_or_create_manifest()`
creates a missing MANIFEST with `"identity"` in the checkpoint/compact path.
Passing the configured codec id instead may be the right fix, but it must not
be silent.

### `StorageWalCompactionInput`

The input should contain only compaction mechanics:

```text
layout
database_uuid
manifest_create_codec_id
active_wal_segment
wal_codec
```

ES2A chose `DatabaseLayout` for this boundary.

Resolved by ES2E:

- `active_wal_segment` is `Option<NonZeroU64>` so storage can preserve the
  existing engine behavior exactly: `Some(segment)` updates MANIFEST and
  supplies the one-based active override, while `None` leaves MANIFEST
  unchanged and uses the compactor's existing zero-override fallback.
- `database_uuid` and `manifest_create_codec_id` are included so the storage
  helper can preserve current missing-MANIFEST compact behavior.

### `StorageWalCompactionOutcome`

The output should return raw facts already produced by `WalOnlyCompactor`:

```text
wal_segments_removed
reclaimed_bytes
snapshot_watermark
```

Storage should preserve `CompactionError::NoSnapshot` or an equivalent
storage-local variant. Engine should continue mapping that one case to the
existing invalid-input message:

```text
No checkpoint exists yet. Run checkpoint() before compact().
```

## Engine Wrapper Shape

After ES2, `Database::checkpoint()` should be thin:

```text
check_not_shutting_down()
return Ok for ephemeral/follower
flush()
watermark = coordinator.quiesced_version()
data = collect_checkpoint_data()
active_wal_segment = NonZeroU64(wal_writer.current_segment())
outcome = storage::durability::run_storage_checkpoint(...)
log database-level outcome
return Ok
```

After ES2, `Database::compact()` should be thin:

```text
check_not_shutting_down()
return Ok for ephemeral/follower
active_wal_segment = wal_writer.current_segment_if_present()
outcome = storage::durability::compact_storage_wal(...)
map NoSnapshot to existing invalid input error
log database-level outcome
return Ok
```

The wrappers should remain in engine even if they become small. They are part
of the public database API and carry lifecycle semantics.

## Implementation Plan

ES2 should land as straight lettered phases unless the code review finds a
smaller split is needed:

- `ES2A` writes the joint ES2-ES4 API sketch.
- `ES2B` locks characterization coverage for checkpoint and WAL compaction
  behavior.
- `ES2C` moves generic checkpoint mechanics into storage.
- `ES2D` thins the engine checkpoint wrapper.
- `ES2E` moves generic WAL compaction mechanics into storage.
- `ES2F` thins the engine compaction wrapper.
- `ES2G` cleans residue and reruns ownership guards.

### ES2A Boundary Sketch

Write [es2-es4-storage-runtime-boundary-api-sketch.md](./es2-es4-storage-runtime-boundary-api-sketch.md)
before moving code.

The sketch should define the relationship between:

- `StorageCheckpointOutcome`
- snapshot install stats from ES3
- `StorageRecoveryOutcome` from ES4

It should also decide whether common storage layout and manifest helpers are
introduced now or deferred to ES4.

### ES2B Characterization Before Movement

Add or identify tests that characterize current behavior before refactoring.

Minimum ES2B coverage:

- checkpoint creates a snapshot and updates the MANIFEST watermark
- repeated checkpoint of unchanged state is deterministic where current
  formats make determinism meaningful
- checkpoint preserves tombstones, TTL, timestamps, and versions
- checkpoint includes graph-as-KV rows
- checkpoint skips event metadata keys
- checkpoint skips branch index keys
- checkpoint includes vector collection config rows and vector record rows
- checkpoint then compact then reopen preserves readable state
- `compact()` before checkpoint returns the existing invalid-input behavior
- WAL compaction uses the active writer segment override
- snapshot pruning after checkpoint remains non-fatal on pruning failure
- missing-MANIFEST checkpoint/compaction behavior around MANIFEST codec id is
  either preserved or deliberately changed with compatibility coverage

Prefer existing engine integration tests where the behavior crosses public
database APIs. Add storage-layer tests only where the new storage helper can
be tested without primitive semantics.

ES2B is complete when the characterization suite is green against the pre-move
engine implementation.

### ES2C Add Storage Checkpoint Runtime

Create the storage module and move generic checkpoint mechanics behind a
storage-owned function.

The first version should preserve behavior closely:

- create the snapshots directory
- apply existing Unix `0700` permission behavior
- load or create MANIFEST
- preserve or explicitly change the current missing-MANIFEST codec id
  behavior
- reconstruct existing snapshot watermark
- create `CheckpointCoordinator`
- call checkpoint with supplied `CheckpointData`
- set MANIFEST snapshot watermark
- run snapshot pruning if this is already generic and can be moved cleanly,
  while keeping pruning failure nonfatal to checkpoint success

If snapshot pruning currently depends on engine config types, split the raw
storage pruning helper from the engine config adapter rather than moving
engine config into storage.

### ES2D Thin Engine Checkpoint Wrapper

Update `Database::checkpoint()` to call the storage checkpoint runtime.

Keep in engine:

- lifecycle guard
- ephemeral/follower no-op
- `flush()`
- coordinator quiescing
- checkpoint data collection
- storage error to `StrataError` conversion
- database-level logging

Do not move `collect_checkpoint_data()` in this step.

### ES2E Add Storage WAL Compaction Runtime

Move generic WAL compaction construction and execution behind a storage-owned
function.

The storage helper should:

- load/create MANIFEST through the same manifest helper used by checkpoint
- construct `WalOnlyCompactor`
- install the codec-aware reader path
- compact with active segment override
- return raw removed segment and reclaimed byte counts

### ES2F Thin Engine Compaction Wrapper

Update `Database::compact()` to call the storage WAL compaction runtime.

Keep in engine:

- lifecycle guard
- ephemeral/follower no-op
- active WAL segment observation from the writer
- public mapping of no-checkpoint compaction to invalid input
- database-level logging

### ES2G Clean Residue And Re-run Guards

After the code movement, remove engine imports that should no longer be
needed:

- `CheckpointCoordinator`
- `CheckpointError`
- `CompactionError` unless still used for mapping
- `ManifestManager`
- `ManifestError`
- `WalOnlyCompactor`
- `SnapshotWatermark`

ES2G also routes remaining generic MANIFEST mechanics through storage-owned
helpers where doing so does not move engine policy:

- disk-primary shutdown MANIFEST fsync uses `sync_storage_manifest`, while
  shutdown sequencing remains in engine
- flush-time WAL truncation uses `truncate_storage_wal_after_flush`, while
  engine still decides that the post-flush truncation is best-effort

Then run the guard commands from ES1 and record any intentional remaining
matches.

## Error Mapping

Storage should return storage-local errors. Engine should map them.
Public storage-local error enums should be `#[non_exhaustive]`; engine mappings
must keep a catch-all arm so future storage variants do not become semver
breaks.

Preserve existing public behavior:

- checkpoint coordinator failures remain internal engine errors at the public
  API boundary
- MANIFEST load/create/persist failures remain internal engine errors at the
  public API boundary
- `compact()` before checkpoint remains invalid input with the current message
- post-checkpoint pruning failure remains non-fatal and warning-level

Do not add `StrataError` to storage and do not make storage format
operator-facing recovery language.

## Snapshot Pruning

Snapshot pruning belongs with checkpoint mechanics if it can be expressed in
storage terms:

- current snapshots
- retention count/age/bytes policy
- filesystem deletion results
- raw pruning stats

Engine owns the public configuration vocabulary and any product defaults.

If the current pruning path is too entangled with `StrataConfig`, ES2 should
move the filesystem pruning helper and leave engine as the adapter from public
config to storage pruning options. The direction should still be toward
storage owning the pruning mechanics.

## Manifest Ownership

MANIFEST mechanics are shared by checkpoint, compaction, snapshot install, and
recovery.

ES2C should introduce only the manifest helpers needed for checkpoint and WAL
compaction, but the shape should anticipate ES4:

- load existing MANIFEST
- create missing MANIFEST with database UUID and codec information
- update active WAL segment
- set snapshot watermark
- persist changes

The helper should not decide primary/follower recovery policy or shutdown
success semantics. Those belong to engine now; the recovery/open policy move
belongs to ES4.

## Verification Gates

Run the standard formatting and relevant tests:

```sh
cargo fmt --check
cargo test -p strata-storage --quiet
cargo test -p strata-engine --quiet
```

If the change touches workspace-visible API exports, also run:

```sh
cargo check --workspace --all-targets
```

Ownership guards:

```sh
cargo tree -p strata-storage --depth 2
cargo tree -p strata-engine --depth 1
rg -n 'use strata_engine|strata_engine::|strata-engine' crates/storage/src crates/storage/Cargo.toml
rg -n 'JsonPath|JsonPatch|SearchSubsystem|Recipe|VectorConfig|DistanceMetric|ChainVerification|BranchLifecycle|RetentionReport|HealthReport|executor' crates/storage/src
rg -n 'CheckpointCoordinator|WalOnlyCompactor|ManifestManager|RecoveryCoordinator|SnapshotSerializer|install_snapshot|apply_storage_config' crates/engine/src/database
```

Expected ES2G direction:

- no new `storage -> engine` dependency
- no primitive semantic vocabulary in the new storage checkpoint module
- checkpoint and WAL compaction coordinator usage disappears from engine
  wrappers or remains only in tests/comments
- recovery and snapshot install residue remains until ES3/ES4

## ES2G Guard Results

Recorded after the ES2G cleanup:

- `cargo tree -p strata-storage --depth 2`: clean. `strata-storage`
  depends on `strata-core` and external crates, not `strata-engine`.
- `cargo tree -p strata-engine --depth 1`: expected dependency on
  `strata-storage`; no reverse edge.
- `rg -n 'use strata_engine|strata_engine::|strata-engine' crates/storage/src crates/storage/Cargo.toml`:
  no matches.
- `rg -n 'JsonPath|JsonPatch|SearchSubsystem|Recipe|VectorConfig|DistanceMetric|ChainVerification|BranchLifecycle|RetentionReport|HealthReport|executor' crates/storage/src`:
  no matches.
- `rg -n 'CheckpointCoordinator|WalOnlyCompactor|ManifestManager|RecoveryCoordinator|SnapshotSerializer|install_snapshot|apply_storage_config' crates/engine/src/database`:
  intentional remaining matches:
  - `StorageCheckpointError::CheckpointCoordinator` in
    `compaction.rs` is public error mapping for storage-owned checkpoint
    runtime; engine no longer constructs the checkpoint coordinator there.
  - `snapshot_install.rs` keeps `SnapshotSerializer` and `install_snapshot`
    until ES3.
  - `recovery.rs` keeps `RecoveryCoordinator`, `ManifestManager`, and
    snapshot install callbacks until ES4.
  - `open.rs` and `mod.rs` keep `apply_storage_config` as engine-owned
    configuration policy.
  - `recovery_error.rs` has documentation text for recovery-owned
    `ManifestManager::create` failures.
  - `lifecycle.rs` and `transaction.rs` no longer match this guard; shutdown
    MANIFEST fsync and flush-time WAL truncation call storage helpers.
  - test matches use `ManifestManager` for assertions, corrupt fixture setup,
    shutdown coverage, and characterization.

ES2G also removes the stale disk-backed cache mode from primary opens:
`durability = "cache"` is rejected by `StrataConfig`. Cache remains an
explicit open mode through `Database::cache()` and `OpenSpec::cache()`.

## Acceptance Checklist

ES2A is complete when:

1. The ES2-ES4 joint API sketch exists.
2. The sketch's own ES2A acceptance checklist is satisfied.

ES2B is complete when:

1. Checkpoint characterization exists before behavior-preserving movement.
2. WAL compaction characterization exists before behavior-preserving
   movement.

ES2C is complete when:

1. Storage owns generic checkpoint runtime around `CheckpointCoordinator`.

ES2D is complete when:

1. Engine `Database::checkpoint()` remains public and thin.
2. `collect_checkpoint_data()` remains engine-owned or is split through a
   primitive-materialization callback that keeps semantics out of storage.

ES2E is complete when:

1. Storage owns generic WAL compaction runtime around `WalOnlyCompactor`.

ES2F is complete when:

1. Engine `Database::compact()` remains public and thin.
2. Existing checkpoint, compaction, and reopen behavior is unchanged.
3. Storage APIs expose raw facts and storage-local errors only.
4. No on-disk format changes are introduced.

ES2G is complete when:

1. ES1 guard commands have been rerun and intentional residue is recorded.

ES2 as a whole is complete when ES2A through ES2G are complete.

## Non-Goals

ES2 does not:

- move snapshot install machinery
- move recovery bootstrap machinery
- move storage configuration application
- change WAL, snapshot, or MANIFEST formats
- change checkpoint or compaction public API behavior
- move primitive checkpoint materialization into storage
- redesign snapshot retention policy
- alter recovery policy or lossy/degraded behavior
- make executor or CLI call storage checkpoint/compaction APIs directly
