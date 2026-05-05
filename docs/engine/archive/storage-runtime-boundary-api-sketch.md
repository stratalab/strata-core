# Storage Runtime Boundary API Sketch

## Purpose

This is the initial design sketch for the storage/runtime boundary shared by:

- `checkpoint/WAL cleanup` - checkpoint and WAL compaction mechanics
- `snapshot-install cleanup` - snapshot decode and install mechanics
- `recovery-bootstrap cleanup` - recovery bootstrap mechanics

The code moves are sequential, but the APIs are coupled:

- checkpoint creates the snapshot and MANIFEST state that compaction and
  recovery consume
- snapshot install is the inverse of checkpoint materialization
- recovery composes MANIFEST preparation, snapshot install, WAL replay, and
  segmented-store bootstrap

This sketch defines the intended type families before the checkpoint/WAL cleanup
moves checkpoint mechanics down into storage.

Read this together with:

- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
- [boundary-baseline-and-guardrails-plan.md](./boundary-baseline-and-guardrails-plan.md)
- [checkpoint-wal-compaction-mechanics-plan.md](./checkpoint-wal-compaction-mechanics-plan.md)
- [recovery-bootstrap-mechanics-plan.md](./recovery-bootstrap-mechanics-plan.md)

## Boundary Decision

Use storage-owned runtime APIs around `DatabaseLayout`.

Do not pass ad-hoc `data_dir.join(...)` paths across the boundary. Storage
already owns `strata_storage::durability::DatabaseLayout`, and recovery already
uses it. The checkpoint/WAL cleanup should extend checkpoint and compaction to
use the same layout vocabulary.

Engine remains the public API and policy owner:

- `Database::checkpoint()`
- `Database::compact()`
- database open/recovery orchestration
- lifecycle checks and shutdown behavior
- primitive checkpoint materialization
- recovery policy and operator-facing reports
- `TransactionCoordinator` bootstrap
- `StrataError` and `RecoveryError` conversion

Storage owns raw mechanics:

- snapshot directory and MANIFEST mechanics
- checkpoint coordinator execution
- WAL compaction execution
- snapshot section decode/install mechanics
- recovery coordinator replay mechanics
- raw replay, install, and segment recovery facts

Storage APIs should return storage-local errors and raw facts. Engine converts
those facts into public database behavior.

## Coordinator Decision

`TransactionCoordinator` stays engine-owned for recovery bootstrap.

This is the least invasive split and matches the intended architecture:
storage recovers durable substrate state, while engine owns transaction-facing
runtime orchestration and public database semantics.

Recovery bootstrap should therefore not make storage call into engine per
replayed record to update coordinator state. Storage should replay WAL records directly into
`SegmentedStore` using storage-owned mechanics, then return `RecoveryStats`
and `RecoveredState`. Engine then constructs or updates
`TransactionCoordinator` from those raw facts.

If later work wants to move `TransactionCoordinator`, that should be a
separate coordinator ownership plan, not an incidental recovery-bootstrap side
effect.

## Shared Layout And Manifest Helpers

The shared input vocabulary should center on:

```text
DatabaseLayout
StorageCodec / codec_id
database_uuid
active_wal_segment
write_buffer_size
StorageRuntimeConfig
CheckpointData
```

The checkpoint/WAL cleanup should introduce only the manifest helpers needed
for checkpoint and WAL compaction, but the helper shape should be reusable by
recovery bootstrap.

Candidate internal storage helpers:

```text
load_manifest(layout) -> StorageResult<ManifestManager>
load_or_create_manifest(layout, database_uuid, manifest_create_codec_id) -> StorageResult<ManifestManager>
update_manifest_active_wal_segment(manifest, active_wal_segment) -> StorageResult<()>
current_snapshot_watermark(manifest) -> Option<SnapshotWatermark>
set_manifest_snapshot_watermark(manifest, snapshot_id, watermark_txn) -> StorageResult<()>
```

These helpers should not decide:

- primary vs follower open policy
- codec mismatch presentation
- lossy recovery policy
- degraded storage acceptance
- public error wording

Those remain engine decisions.

## Checkpoint Boundary

Candidate module:

```text
crates/storage/src/durability/checkpoint_runtime.rs
```

Candidate input:

```text
StorageCheckpointInput {
    layout: DatabaseLayout,
    database_uuid: [u8; 16],
    checkpoint_codec: Box<dyn StorageCodec>,
    manifest_create_codec_id: String,
    checkpoint_data: CheckpointData,
    watermark_txn: TxnId,
    active_wal_segment: NonZeroU64,
}
```

`checkpoint_data` is supplied by engine. checkpoint/WAL cleanup should not move
`Database::collect_checkpoint_data()` wholesale because that function contains
primitive materialization rules:

- graph rows are encoded in the KV section with `TypeTag::Graph`
- event metadata keys are skipped
- branch index keys are skipped
- vector config keys and vector record payloads are interpreted

`active_wal_segment` is non-zero by construction. Engine rejects a writer that
reports segment `0` before entering the storage checkpoint runtime.

Candidate output:

```text
StorageCheckpointOutcome {
    snapshot_id: u64,
    watermark_txn: TxnId,
    checkpoint_timestamp_micros: u64,
    active_wal_segment: u64,
}
```

The outcome is deliberately raw. Engine logs the database-level message and
maps storage errors into `StrataError`.

Phase D split snapshot pruning out of the checkpoint outcome. Storage owns the
raw pruning helper and retention minimum, while engine keeps the lifecycle and
configuration adapter. `Database::checkpoint()` runs pruning after the
checkpoint and MANIFEST update, preserving the existing non-fatal warning
behavior for pruning failure.

`manifest_create_codec_id` is explicit because current
`Database::load_or_create_manifest()` writes `"identity"` when it creates a
missing MANIFEST in the checkpoint/compact path. Phase C must either preserve
that behavior or call out an intentional behavior change with compatibility
tests before passing the configured checkpoint codec id instead.

## WAL Compaction Boundary

Candidate input:

```text
StorageWalCompactionInput {
    layout: DatabaseLayout,
    wal_codec: Box<dyn StorageCodec>,
    database_uuid: [u8; 16],
    manifest_create_codec_id: String,
    active_wal_segment: Option<NonZeroU64>,
}
```

`active_wal_segment` is optional to preserve the current engine wrapper
semantics: when a WAL writer exists, engine passes `Some(segment)` with a
one-based segment number and storage persists that segment into MANIFEST
before compaction; when there is no writer, engine passes `None`, storage
leaves MANIFEST unchanged, and the underlying compactor receives the existing
zero-override fallback.

`database_uuid` and `manifest_create_codec_id` are explicit because current
`Database::compact()` can create a missing MANIFEST before returning the
no-checkpoint error. Phase F should preserve that behavior unless it deliberately
changes the missing-MANIFEST compatibility contract.

The checkpoint/WAL cleanup also uses a small `StorageManifestSyncInput` for shutdown-time MANIFEST
fsync. That boundary is intentionally limited to generic MANIFEST
load/create/active-segment/persist mechanics. Engine still owns shutdown
sequencing and recovery bootstrap still owns recovery/open MANIFEST policy.

Candidate output:

```text
StorageWalCompactionOutcome {
    wal_segments_removed: usize,
    reclaimed_bytes: u64,
    snapshot_watermark: Option<u64>,
}
```

`snapshot_watermark` mirrors `CompactInfo::snapshot_watermark`, the raw
retention fact currently produced by `WalOnlyCompactor`. It is useful for
tests and diagnostics, but engine should not turn it into an operator report
in checkpoint/WAL cleanup.

Storage should preserve `CompactionError::NoSnapshot` or an equivalent
storage-local variant. Engine continues mapping that case to the existing
public invalid-input message:

```text
No checkpoint exists yet. Run checkpoint() before compact().
```

## snapshot-install cleanup Snapshot Install Boundary

Candidate module:

```text
crates/storage/src/durability/decoded_snapshot_install.rs
```

Storage candidate input:

```text
StorageDecodedSnapshotInstallInput<'a> {
    storage: &'a SegmentedStore,
    plan: &'a StorageDecodedSnapshotInstallPlan,
}

StorageDecodedSnapshotInstallPlan {
    groups: Vec<StorageDecodedSnapshotInstallGroup>,
}

StorageDecodedSnapshotInstallGroup {
    branch_id: BranchId,
    type_tag: TypeTag,
    entries: Vec<DecodedSnapshotEntry>,
}
```

Storage candidate output:

```text
StorageDecodedSnapshotInstallStats {
    groups_installed: usize,
    rows_installed: usize,
}
```

The storage API starts after primitive snapshot decode. It may know
`SegmentedStore`, `TypeTag` as an opaque storage-family router,
`DecodedSnapshotEntry`, tombstones, TTLs, versions, and timestamps.

Storage must not learn:

- `LoadedSnapshot` install policy
- `SnapshotSerializer`
- primitive snapshot DTOs such as `KvSnapshotEntry`
- primitive section tags such as `primitive_tags::KV`
- JSON path or patch semantics
- event-chain verification
- vector metric/model policy
- branch workflow policy
- graph query semantics

Engine remains responsible for converting a loaded snapshot into this generic
storage-row plan. The snapshot-install cleanup engine wrapper should become the function recovery-bootstrap cleanup
recovery calls when the `RecoveryCoordinator` supplies a loaded snapshot.

## recovery-bootstrap cleanup Recovery Boundary

Candidate module:

```text
crates/storage/src/durability/bootstrap.rs
```

Candidate input:

```text
StorageRecoveryInput {
    layout: DatabaseLayout,
    mode: StorageRecoveryMode,
    configured_codec_id: String,
    write_buffer_size: usize,
    runtime_config: StorageRuntimeConfig,
    allow_lossy_wal_replay: bool,
    snapshot_install: RecoverySnapshotInstallCallback,
}

RecoverySnapshotInstallCallback {
    install_snapshot(
        snapshot: &LoadedSnapshot,
        install_codec: &dyn StorageCodec,
        storage: &SegmentedStore,
    ) -> Result<(), StorageError>,
}
```

The recovery-bootstrap cleanup implementation plan refines this callback to pass the resolved
snapshot install codec as well. That keeps MANIFEST/codec resolution in
storage while letting engine continue to own primitive snapshot decode.

Candidate mode:

```text
StorageRecoveryMode {
    PrimaryCreateManifestIfMissing,
    FollowerNeverCreateManifest,
}
```

This mode is storage-neutral. Engine maps its own `RecoveryMode` into it and
still owns the policy meaning.

The follower mode name is intentionally about MANIFEST behavior, not
read-only database policy. Follower recovery may still create missing storage
directories and replay durable state into a local `SegmentedStore`; it must
not create a MANIFEST.

Candidate output:

```text
StorageRecoveryOutcome {
    database_uuid: [u8; 16],
    wal_codec: Box<dyn StorageCodec>,
    storage: SegmentedStore,
    wal_replay: RecoveryStats,
    segment_recovery: RecoveredState,
    lossy_wal_replay: Option<StorageLossyWalReplayFacts>,
}
```

`StorageRecoveryOutcome` intentionally does not contain primitive snapshot
install stats. Storage invokes the engine-supplied `snapshot_install` callback
when recovery loads a snapshot, but storage does not inspect or own the
callback's primitive decode counters. If engine needs install telemetry, it
captures `InstallStats` in the engine wrapper that supplies the callback.

Candidate lossy facts:

```text
StorageLossyWalReplayFacts {
    records_applied_before_failure: u64,
    version_reached_before_failure: CommitVersion,
    discarded_on_wipe: bool,
    source: CoordinatorRecoveryError,
}
```

Engine decides whether to request lossy WAL replay from config. Storage may
perform the mechanical wipe only after engine opts in via
`allow_lossy_wal_replay: true`. The recovery-bootstrap implementation keeps that opt-in
inside the opaque storage replay object so engine cannot pass a mismatched
policy flag, replay result, or pre-failure applied-record count into the lossy
outcome helper. Engine still owns `LossyRecoveryReport` wording and public
error classification.

Lossy replay must preserve the current hard-fail bypass rules. Even when
`allow_lossy_wal_replay` is true, storage must not wipe and continue for
recovery planning failures, MANIFEST failures, snapshot plan failures, or
legacy-format failures where `CoordinatorRecoveryError::should_bypass_lossy()`
returns `true`. Storage must also bypass lossy replay for snapshot install
callback failures because those are primitive snapshot decode/install failures,
not WAL replay failures. Those errors remain hard failures and engine maps them
into `RecoveryError`.

Engine also keeps degraded-storage policy. Storage returns
`RecoveredState.health`; engine decides whether `DataLoss`,
`PolicyDowngrade`, or `Telemetry` permits opening under the active config.

`RecoveryStats::final_version` in `StorageRecoveryOutcome.wal_replay` must
already include the snapshot-version fold:

```text
wal_replay.final_version = max(wal_replay.final_version, CommitVersion(storage.version()))
```

The current `RecoveryCoordinator` does not fold snapshot-installed entry
versions into `RecoveryStats`; the current engine does that before
`TransactionCoordinator` bootstrap. Recovery bootstrap must preserve that behavior.

Storage recovery must also preserve the current ordering around storage
configuration:

```text
engine snapshot callback / WAL replay
snapshot-version fold
apply StorageRuntimeConfig to SegmentedStore
recover_segments()
return StorageRecoveryOutcome
```

Until the storage-runtime-config cleanup completes the public config split, engine builds
`StorageRuntimeConfig` from `StrataConfig.storage` and passes it down. This
does not move public config ownership into storage; it only prevents recovery
bootstrap from running `recover_segments()` with default storage knobs.

## Recovery Bootstrap Engine Wrapper Shape

After recovery bootstrap, `Database::run_recovery()` should remain the engine entry point.
It should do roughly:

```text
build StorageRecoveryInput from StrataConfig and RecoveryMode
outcome = storage::durability::run_storage_recovery(input)
map raw storage errors, including codec-init failures, into RecoveryError
apply engine degraded/lossy policy and reports
result = RecoveryResult { storage: outcome.storage, stats: outcome.wal_replay }
coordinator = TransactionCoordinator::from_recovery_with_limits(&result, ...)
coordinator.apply_storage_recovery(outcome.segment_recovery)
restore follower state when needed
build RecoveryOutcome for open.rs
```

Storage should not return a `TransactionCoordinator`, `RecoveryError`,
`LossyRecoveryReport`, or engine follower state.

## Type Relationship

The three outcome families should compose without circular ownership:

```text
StorageCheckpointOutcome
    -> records snapshot_id and watermark persisted in MANIFEST
    -> WAL compaction consumes MANIFEST watermark to delete covered WAL
    -> recovery uses MANIFEST snapshot_id/watermark to install snapshot and skip WAL

StorageDecodedSnapshotInstallStats
    -> returned by storage decoded-row install in snapshot-install cleanup
    -> may be observed inside the engine snapshot callback
    -> is not embedded in StorageRecoveryOutcome because storage recovery does
       not own primitive decode or primitive install telemetry

StorageRecoveryOutcome
    -> returns raw WAL replay stats, segmented-store recovery health, and
       optional lossy replay facts
    -> engine builds coordinator, reports, and public open result
```

## Error Boundary

Storage errors should stay storage-local:

```text
StorageCheckpointError
StorageWalCompactionError
StorageManifestRuntimeError
StorageFlushWalTruncationError
StorageDecodedSnapshotInstallError
StorageRecoveryError
StorageSnapshotPruneError
```

Public storage-local errors, enums, and data structs should be
`#[non_exhaustive]`, and engine adapters should include catch-all mappings.
Storage can add more precise phases or facts later without forcing every engine
release to exhaustively enumerate them. Public data structs should also expose
constructors or defaults so callers do not need to depend on struct literal
exhaustiveness.

Existing lower errors may be reused if they are already precise enough:

- `CheckpointError`
- `CompactionError`
- `ManifestError`
- `SnapshotReadError`
- `CoordinatorRecoveryError`
- `StorageError`

Engine maps these into:

- `StrataError`
- `RecoveryError`
- `LossyRecoveryReport`
- database log messages

Do not add an engine dependency, engine error type, operator report type, or
product vocabulary to storage.

## Open Questions Deferred To Subplans

The subplans may refine names and ownership details, but should not reopen the
major boundary decisions above.

Deferred details:

- whether checkpoint/WAL cleanup stores `checkpoint_codec` by object or codec id
- whether pruning stats include bytes
- whether recovery bootstrap returns `StorageRecoveryOutcome.storage` by value or
  through a smaller recovered-store wrapper
- whether manifest helpers are public within `durability` or private to the
  checkpoint/recovery modules

Not deferred:

- use `DatabaseLayout` rather than ad-hoc paths
- keep `TransactionCoordinator` in engine for recovery bootstrap
- keep primitive checkpoint materialization out of storage
- preserve snapshot-version folding before coordinator bootstrap
- preserve storage-config-before-`recover_segments()` ordering
- preserve lossy hard-fail bypass behavior
- keep engine policy and public error/report conversion out of storage

## Phase A Acceptance

Phase A is complete when:

1. This sketch exists and is linked from the checkpoint/WAL cleanup plan.
2. Checkpoint and WAL compaction APIs are sketched using
   `DatabaseLayout`.
3. Snapshot install stats are sketched as physical storage counters.
4. Recovery bootstrap outcome is sketched without `TransactionCoordinator`.
5. The coordinator ownership decision is explicit.
6. The sketch explains how checkpoint, snapshot install, and recovery
   outcomes compose.
7. Recovery bootstrap invariants are explicit:
   - snapshot-version fold before coordinator bootstrap
   - storage config before `recover_segments()`
   - lossy hard-fail bypass preservation
8. checkpoint/WAL cleanup checkpoint edge cases are explicit:
   - missing-MANIFEST codec id behavior cannot change silently
   - pruning failure remains nonfatal to checkpoint success
