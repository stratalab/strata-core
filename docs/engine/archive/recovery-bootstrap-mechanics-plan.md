# Recovery Bootstrap Mechanics Plan

## Purpose

`recovery-bootstrap cleanup` is the recovery-bootstrap epic in the engine/storage boundary
normalization workstream.

`checkpoint/WAL cleanup` moved checkpoint, WAL compaction, snapshot pruning, and generic MANIFEST
sync mechanics toward storage. `snapshot-install cleanup` moved only generic decoded-row snapshot
install mechanics into storage while keeping primitive snapshot decode in
engine. `recovery-bootstrap cleanup` completes the next link in that chain: recovery should use
storage-owned mechanics for MANIFEST preparation, WAL replay, snapshot install
callback wiring, lossy WAL replay mechanics, storage runtime config
application, and segment recovery.

The goal is not to redesign database recovery. The goal is to move the lower
durability runtime out of `strata-engine` while keeping engine in charge of
database open policy, public recovery classification, and transaction runtime
bootstrap.

Read this together with:

- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
- [boundary-baseline-and-guardrails-plan.md](./boundary-baseline-and-guardrails-plan.md)
- [storage-runtime-boundary-api-sketch.md](./storage-runtime-boundary-api-sketch.md)
- [checkpoint-wal-compaction-mechanics-plan.md](./checkpoint-wal-compaction-mechanics-plan.md)
- [snapshot-decode-install-mechanics-plan.md](./snapshot-decode-install-mechanics-plan.md)
- [../storage/storage-engine-ownership-audit.md](../../storage/storage-engine-ownership-audit.md)
- [../storage/storage-charter.md](../../storage/storage-charter.md)
- [../architecture/architecture-recovery-target.md](../../architecture/architecture-recovery-target.md)

## Boundary Rule

Storage owns durable substrate recovery mechanics.

Engine owns database recovery meaning.

Storage may know:

- `DatabaseLayout`
- `ManifestManager` and MANIFEST load/create mechanics
- storage codec ids and `StorageCodec` values
- `RecoveryCoordinator`
- `RecoveryStats`
- `LoadedSnapshot` as a snapshot container supplied to a callback
- `SegmentedStore`
- `RecoveredState` and `RecoveryHealth`
- storage runtime knobs needed by `SegmentedStore`
- WAL record application into `SegmentedStore`
- storage-local recovery errors and raw lossy replay facts

Storage must not know:

- `Database::open` or `Database::run_recovery` public semantics
- primary/follower product policy beyond storage-neutral MANIFEST behavior
- `TransactionCoordinator`
- `RecoveryOutcome`
- engine `RecoveryError` or `StrataError` wording
- `LossyRecoveryReport` or `LossyErrorKind`
- follower blocked-state persistence or restore policy
- subsystem recovery ordering above raw storage replay
- primitive snapshot section decode or primitive install stats
- degraded-storage open policy

The recovery-bootstrap cleanup target is:

```text
storage recovers durable state and returns raw facts
engine decides whether those facts permit database open
```

## Non-Negotiables

`TransactionCoordinator` stays engine-owned for recovery-bootstrap cleanup.

This is the load-bearing decision from storage-runtime boundary API sketch. Storage should
not call into engine per replayed record to mutate coordinator state. Storage
should replay durable bytes into `SegmentedStore`, return `RecoveryStats` and
`RecoveredState`, and let engine bootstrap `TransactionCoordinator` after the
storage recovery call returns.

recovery-bootstrap cleanup must preserve these current ordering guarantees:

- configured codec validation occurs before any recovery-managed directory or
  MANIFEST creation
- primary first-open may create a MANIFEST; follower without a MANIFEST must
  not create one
- a recovery snapshot callback must not silently skip install when no install
  codec exists
- snapshot-installed versions are folded into `RecoveryStats::final_version`
  before `TransactionCoordinator` bootstrap
- storage runtime config is applied before `recover_segments()` runs
- `RecoveredState` is applied to the coordinator after coordinator bootstrap
- follower-state restore runs after raw storage recovery and coordinator
  bootstrap, not inside storage
- lossy replay bypass rules remain hard failures for planning, MANIFEST,
  snapshot-plan, and legacy-format errors

recovery-bootstrap cleanup must not:

- change public recovery/open behavior
- weaken corruption or quarantine behavior
- move primitive snapshot decode into storage
- move branch-domain recovery policy into storage
- expose engine error types from storage
- introduce a storage dependency on engine
- turn lossy recovery into a storage-owned product policy

## Existing Code Map

The target surface is concentrated in:

- [database/recovery.rs](../../../crates/engine/src/database/recovery.rs)
  - configured codec validation
  - `prepare_manifest`
  - WAL codec resolution
  - `SegmentedStore::with_dir`
  - `run_coordinator_recovery`
  - `install_recovery_snapshot`
  - `handle_wal_recovery_outcome`
  - snapshot-version fold
  - `apply_storage_config`
  - `recover_segments`
  - degraded-storage policy
  - coordinator bootstrap
  - follower-state restore
  - watermark construction
- [database/recovery_error.rs](../../../crates/engine/src/database/recovery_error.rs)
  - engine recovery taxonomy
  - public `StrataError` conversion
  - primary/follower error wording
- [database/open.rs](../../../crates/engine/src/database/open.rs)
  - `apply_storage_config`
  - directory permission helper currently used by recovery
  - primary/follower open tail after recovery
- [database/snapshot_install.rs](../../../crates/engine/src/database/snapshot_install.rs)
  - engine-owned primitive decode
  - snapshot-install cleanup decoded-row install adapter
  - recovery snapshot install helper target callback

The storage-owned building blocks already exist:

- `strata_storage::durability::DatabaseLayout`
- `strata_storage::durability::ManifestManager`
- `strata_storage::durability::RecoveryCoordinator`
- `strata_storage::durability::RecoveryStats`
- `strata_storage::durability::RecoveryResult`
- `strata_storage::durability::apply_wal_record_to_memory_storage`
- `strata_storage::SegmentedStore`
- `SegmentedStore::recover_segments`

recovery-bootstrap cleanup should assemble these storage-owned pieces inside storage instead of
inside engine.

## Target Module

Use a storage-owned recovery bootstrap module:

```text
crates/storage/src/durability/recovery_bootstrap.rs
```

`bootstrap.rs` was the name in the original storage-runtime boundary sketch. The more explicit
`recovery_bootstrap.rs` is preferred for implementation because `durability`
already contains several bootstrap-like paths around checkpoint, WAL, and
MANIFEST setup.

The completed recovery-bootstrap cleanup module should be re-exported through
`strata_storage::durability` with a small public surface:

```text
run_storage_recovery
StorageRecoveryInput
StorageRecoveryMode
StorageRecoveryOutcome
StorageRuntimeConfig
StorageRecoveryError
StorageLossyWalReplayFacts
RecoverySnapshotInstallCallback
```

Phase B intentionally introduces and re-exports only the type/callback surface.
`run_storage_recovery` is introduced in Phase G once the lower-runtime behavior
has moved far enough to implement the function without a panic or fake no-op.

Phase C temporarily exposed `prepare_storage_manifest_for_recovery` and
`StorageManifestRecoveryPreparation` only through storage's
`engine-internal`/`__internal` seam so engine could delegate MANIFEST and codec
preparation before `run_storage_recovery` existed. Phase G removes that public
transitional bridge; storage may keep smaller private helper seams inside
`recovery_bootstrap.rs`, but higher layers should call only
`run_storage_recovery`.

All public storage-local errors, enums, and data structs introduced for recovery-bootstrap cleanup
should be `#[non_exhaustive]`. Public data structs should also have
constructors or defaults so later recovery-bootstrap cleanup steps can add fields without requiring
external callers to use struct literals.

## Target API

The storage recovery input should be primitive-neutral and engine-policy
neutral:

```text
StorageRecoveryInput<'a> {
    layout: DatabaseLayout,
    mode: StorageRecoveryMode,
    configured_codec_id: String,
    write_buffer_size: usize,
    runtime_config: StorageRuntimeConfig,
    allow_lossy_wal_replay: bool,
    snapshot_install: &'a dyn RecoverySnapshotInstallCallback,
}
```

The mode should describe storage behavior, not database policy:

```text
StorageRecoveryMode {
    PrimaryCreateManifestIfMissing,
    FollowerNeverCreateManifest,
}
```

The follower mode name is intentionally about MANIFEST behavior. Follower
recovery may still create a missing segments directory and replay durable
state into a local `SegmentedStore`; it must not create a MANIFEST.

`StorageRuntimeConfig` should contain only the knobs storage needs before
`recover_segments()`:

```text
StorageRuntimeConfig {
    max_branches: usize,
    max_versions_per_key: usize,
    max_immutable_memtables: usize,
    target_file_size: u64,
    level_base_bytes: u64,
    data_block_size: usize,
    bloom_bits_per_key: usize,
    compaction_rate_limit: u64,
}
```

Engine builds this from `StrataConfig.storage` until storage-runtime-config cleanup completes the public
configuration split. That adapter does not make storage own product defaults.
`StorageRuntimeConfig::default()` uses the same storage defaults as
`SegmentedStore`, so a partially wired test or synthetic construction never
creates zero-sized segment/block settings by accident.

The snapshot callback must receive the install codec:

```text
RecoverySnapshotInstallCallback {
    install_snapshot(
        snapshot: &LoadedSnapshot,
        install_codec: &dyn StorageCodec,
        storage: &SegmentedStore,
    ) -> Result<(), StorageError>
}
```

This refines the older storage-runtime boundary sketch. Once storage owns MANIFEST preparation
and codec resolution, storage is the layer that knows which codec the snapshot
install callback must use. Passing the codec to the engine callback keeps
primitive decode in engine without forcing engine to redo MANIFEST prep.

The outcome should remain raw:

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

`StorageRecoveryOutcome` must not contain:

- `TransactionCoordinator`
- `RecoveryOutcome`
- `RecoveryError`
- `LossyRecoveryReport`
- follower persisted state
- primitive install stats

If engine wants snapshot install telemetry, it should capture `InstallStats`
inside the callback it passes to storage.

Lossy replay facts should be mechanical:

```text
StorageLossyWalReplayFacts {
    records_applied_before_failure: u64,
    version_reached_before_failure: CommitVersion,
    discarded_on_wipe: bool,
    source: CoordinatorRecoveryError,
}
```

Engine uses these facts to build `LossyRecoveryReport`, choose
`LossyErrorKind`, and emit operator-facing logs.

## Error Contract

Storage should return a storage-local error:

```text
StorageRecoveryError {
    CodecInit { codec_id, detail },
    ManifestLoad { path, source },
    ManifestCreate { source },
    ManifestCodecMismatch { stored, configured },
    Io { source },
    Coordinator { source },
    SegmentRecovery { source },
}
```

The exact variant names can change during implementation, but the contract
must hold:

- storage errors do not mention engine `RecoveryError`
- storage errors do not include primary/follower public wording
- storage errors preserve enough source structure for engine to reconstruct
  today's public error behavior
- legacy-format errors remain structurally recoverable by engine
- coordinator errors are not stringified before engine maps them
- callback failures preserve their `StorageError` source

Engine remains responsible for mapping storage errors into:

- `RecoveryError::ManifestLoad`
- `RecoveryError::ManifestLegacyFormat`
- `RecoveryError::ManifestCodecMismatch`
- `RecoveryError::ManifestCreate`
- `RecoveryError::CodecInit`
- `RecoveryError::CoordinatorPlan`
- `RecoveryError::SnapshotMissing`
- `RecoveryError::SnapshotRead`
- `RecoveryError::WalLegacyFormat`
- `RecoveryError::WalCodecDecode`
- `RecoveryError::WalChecksum`
- `RecoveryError::WalRead`
- `RecoveryError::PayloadDecode`
- `RecoveryError::WalRecoveryFailed`
- `RecoveryError::StorageDegraded`
- `RecoveryError::Storage`
- `RecoveryError::Io`

The mapping can move code around, but it must preserve public `StrataError`
conversion behavior.

## Engine Wrapper Shape

After recovery-bootstrap cleanup, `Database::run_recovery()` remains the engine entry point.

Target shape:

```text
validate / build engine-facing recovery inputs
build StorageRuntimeConfig from StrataConfig.storage
build engine snapshot callback around install_snapshot()
outcome = strata_storage::durability::run_storage_recovery(input)
map StorageRecoveryError into RecoveryError
apply degraded-storage policy from outcome.segment_recovery.health
build RecoveryResult { storage: outcome.storage, stats: outcome.wal_replay }
coordinator = TransactionCoordinator::from_recovery_with_limits(...)
coordinator.apply_storage_recovery(outcome.segment_recovery)
restore follower state when needed
construct ContiguousWatermark
construct RecoveryOutcome
```

Engine still owns:

- `RecoveryMode`
- conversion to `StorageRecoveryMode`
- degraded-storage policy
- lossy report wording and warnings
- `TransactionCoordinator` construction
- follower-state restore
- watermark construction
- `RecoveryOutcome`

Engine no longer owns:

- MANIFEST load/create mechanics
- WAL codec resolution mechanics
- `RecoveryCoordinator` construction
- WAL replay callback that applies records into storage
- lossy replay wipe mechanics
- snapshot-version fold
- storage config application before segment recovery
- `recover_segments()` execution

## Phase A - Characterization And Inventory

Goal: lock the current behavior before moving recovery mechanics.

Tasks:

- inventory every branch in `Database::run_recovery`
- inventory every `RecoveryError` mapping reachable from recovery
- document current primary and follower MANIFEST behavior
- document current lossy fallback bypass behavior
- document current degraded-storage policy behavior
- add or tighten tests before moving code

Characterization should cover:

- invalid configured codec fails before creating recovery-managed directories
  or MANIFEST files
- primary first-open creates MANIFEST with the configured codec id
- primary reopen rejects MANIFEST/config codec mismatch with the existing
  public error class and wording shape
- follower without MANIFEST does not create a MANIFEST and uses WAL-only
  recovery behavior
- follower with MANIFEST uses the stored codec path and preserves follower
  mismatch wording
- recovery snapshot callback hard-fails when a loaded snapshot reaches the
  callback without an install codec
- snapshot install failure maps through the coordinator callback path
- WAL replay success returns the same `RecoveryStats`
- snapshot-installed versions are folded into final recovery version before
  coordinator bootstrap
- lossy replay is bypassed for coordinator planning errors
- lossy replay is bypassed for legacy-format errors
- lossy replay discards partial storage state when enabled for replay failure
- lossy replay report fields preserve records-applied and version-reached
  facts
- storage degraded `DataLoss`, `PolicyDowngrade`, and `Telemetry` classes keep
  the same open/refuse behavior under current config flags
- `recover_segments()` sees the configured storage knobs, not default knobs
- follower persisted state restore still runs after recovery and still clears
  invalid blocked state

Suggested commands:

```text
cargo test -p strata-engine --lib database::recovery -- --nocapture
cargo test -p strata-engine --test recovery_parity -- --nocapture
cargo test -p strata-engine --lib checkpoint -- --nocapture
```

Acceptance:

- recovery-bootstrap cleanup behavior matrix exists in this document or in test names
- characterization tests fail on at least one intentionally broken ordering
  if locally mutated
- no production code moves in Phase A unless needed for test seams

Phase A characterization status:

| Behavior | Guard |
| --- | --- |
| Invalid configured codec fails before recovery-managed artifacts are created. | `first_open_rejects_invalid_codec_without_touching_disk` |
| Primary first-open creates a MANIFEST with the configured codec and a real database UUID. | `primary_first_open_creates_manifest_with_configured_codec` |
| Runtime storage config is applied before segment recovery. | `recovery_applies_storage_runtime_config` |
| Primary/follower corrupt MANIFEST error classes match coordinator planning errors. | `parity_missing_magic`, `parity_checksum_mismatch`, `parity_codec_id_mismatch`, `parity_unsupported_version`, `parity_truncated` |
| Follower without MANIFEST does not create one and uses the configured WAL codec. | `follower_without_manifest_does_not_create_manifest` |
| Follower invalid persisted state is restored only after replay validation and cleared when stale. | `follower_recovery_clears_invalid_persisted_state_after_replay` |
| Recovery snapshot callback hard-fails when no install codec exists. | `coordinator_replay_rejects_loaded_snapshot_without_install_codec` |
| Snapshot callback failures map through the public recovery path. | `recovery_snapshot_install_failure_maps_through_callback_public_error_path` |
| Snapshot-installed versions are folded into coordinator bootstrap. | `snapshot_versions_fold_into_coordinator_before_bootstrap` |
| WAL replay success still bootstraps storage, coordinator, and watermark from replay stats. | `wal_replay_success_bootstraps_storage_and_coordinator` |
| Snapshot planning failures bypass lossy replay. | `snapshot_plan_failure_bypasses_lossy_fallback` |
| Snapshot callback failures bypass lossy replay. | `lossy_wal_replay_snapshot_callback_failure_preserves_partial_storage`, `recovery_snapshot_install_failure_bypasses_lossy_fallback_when_enabled` |
| Legacy WAL format failures bypass lossy replay. | `wal_legacy_failure_bypasses_lossy_fallback` |
| Lossy replay discards partial storage and reports progress facts. | `lossy_replay_discards_partial_storage_and_reports_progress`, `test_lossy_recovery_discards_valid_data_before_corruption`, `test_lossy_recovery_report_on_immediate_failure` |
| Degraded-storage refusal remains engine policy. | `degraded_storage_policy_matrix_is_engine_owned` |
| Snapshot-only recovery still works with non-identity snapshot header codecs. | `test_aes_checkpoint_compact_reopen_installs_snapshot` |
| Checkpoint-only and checkpoint-plus-delta recovery remain covered. | `test_checkpoint_only_restart_recovers_kv_from_snapshot`, `test_checkpoint_plus_delta_wal_replay_merges_sources` |

## Phase B - Storage API Skeleton

Goal: introduce the storage-owned type surface without moving behavior.

Tasks:

- add `crates/storage/src/durability/recovery_bootstrap.rs`
- define `StorageRecoveryInput`
- define `StorageRecoveryMode`
- define `StorageRuntimeConfig`
- define `StorageRecoveryOutcome`
- define `StorageLossyWalReplayFacts`
- define `StorageRecoveryError`
- define the snapshot install callback shape
- re-export only the intended public storage surface
- add compile-only or synthetic unit tests for type construction and error
  display where useful
- do not expose `run_storage_recovery` yet; Phase B is type-only, and Phase G adds
  the function when it can delegate to real moved behavior

Rules:

- no engine types in storage
- no primitive DTOs or primitive tags in the recovery bootstrap module
- no `TransactionCoordinator`
- no `RecoveryError`
- no `LossyRecoveryReport`

Acceptance:

- storage compiles with the new API surface
- engine still uses the old recovery implementation
- storage ownership grep guards stay clean

## Phase C - Manifest And Codec Preparation

Goal: move generic MANIFEST preparation and WAL codec resolution into storage.

Current engine code to split:

- configured codec validation
- `prepare_manifest`
- `ManifestPreparation`
- WAL codec id choice
- segments directory creation for first-open/follower-without-MANIFEST
- best-effort directory permission restriction if that remains part of storage
  directory creation

Storage should preserve:

- configured codec validation before side effects
- primary missing-MANIFEST creates a MANIFEST
- follower missing-MANIFEST never creates a MANIFEST
- follower missing-MANIFEST may create the segments directory
- manifest codec mismatch remains typed enough for engine role-specific
  public wording
- missing-MANIFEST `database_uuid` behavior remains `[0u8; 16]` for follower
- the no-snapshot/no-codec cases remain explicit and cannot silently skip a
  loaded snapshot install

Implementation direction:

```text
prepare_storage_manifest_for_recovery(input) -> StorageManifestRecoveryPreparation

StorageManifestRecoveryPreparation {
    database_uuid: [u8; 16],
    wal_codec: Box<dyn StorageCodec>,
    snapshot_install_codec: Option<Box<dyn StorageCodec>>,
}
```

Because `run_storage_recovery` intentionally waits until Phase G, this helper is
temporarily exposed through storage's `engine-internal`/`__internal` seam for
the engine wrapper during Phase C. It should become storage-private again once
Phase G routes recovery through the completed storage bootstrap API, unless storage-runtime-config cleanup
needs the narrower helper directly.

Acceptance:

- `Database::run_recovery` no longer imports `ManifestManager`
- public primary/follower manifest errors remain unchanged
- first-open invalid codec still leaves no poisoned storage directory
- follower missing-MANIFEST still does not create a MANIFEST

## Phase D - Coordinator Replay Driver

Goal: move `RecoveryCoordinator` construction and replay driving into storage.

Current engine code to move:

- `run_coordinator_recovery`
- `RecoveryCoordinator::new(...).with_lossy_recovery(...).with_codec(...)`
- WAL record callback around `apply_wal_record_to_memory_storage`
- records-applied counter
- callback wiring around a loaded snapshot

Storage should call the engine snapshot callback as:

```text
snapshot_install.install_snapshot(snapshot, install_codec, &storage)
```

Storage should return `StorageRecoveryError::Coordinator { source }` on hard
coordinator failure. It should not map coordinator failures into public engine
errors.

The no-codec snapshot callback invariant should live in storage once storage
owns manifest prep:

```text
if loaded_snapshot_callback_fired && snapshot_install_codec.is_none() {
    return Err(StorageRecoveryError::Coordinator {
        source: CoordinatorRecoveryError::Callback(StorageError::corruption(...)),
    });
}
```

The exact wrapping can differ, but the behavior must remain a hard failure
with snapshot id context.

Because `run_storage_recovery` intentionally waits until Phase G, Phase D uses a
temporary `run_storage_coordinator_replay` helper exposed only through
storage's `engine-internal`/`__internal` seam. The helper returns the raw
coordinator result and the records-applied count so Phase F can move lossy replay
mechanics without changing report fields. Phase G should absorb this helper into
the completed storage recovery API.

Acceptance:

- `Database::run_recovery` no longer imports `RecoveryCoordinator`
- `Database::run_recovery` no longer imports
  `apply_wal_record_to_memory_storage`
- engine snapshot install remains engine-owned
- primitive install stats, if logged, are captured in the engine callback
- callback install failures still map through the same public recovery path

## Phase E - Snapshot Fold, Runtime Config, And Segment Recovery

Goal: move the post-replay storage mechanics into storage while keeping
engine policy outside storage.

Current engine code to move:

- `stats.final_version = max(stats.final_version, CommitVersion(storage.version()))`
- `apply_storage_config(&storage, &cfg.storage)` mechanics
- `storage.recover_segments()`
- raw `RecoveredState` return

Storage should run this order:

```text
snapshot install / WAL replay
snapshot-version fold
apply StorageRuntimeConfig to SegmentedStore
recover_segments()
return StorageRecoveryOutcome
```

Storage should not decide whether degraded state permits database open.
Storage returns `RecoveredState` and its `RecoveryHealth` unchanged.

Because `run_storage_recovery` intentionally waits until Phase G, Phase E uses a
temporary `complete_storage_recovery_after_replay` helper exposed only through
storage's `engine-internal`/`__internal` seam. The helper consumes the replayed
store and raw `RecoveryStats`, folds the installed snapshot version into those
stats, applies `StorageRuntimeConfig`, runs `recover_segments()`, and returns a
raw `StorageRecoveryOutcome`. Phase G should absorb this helper into the completed
storage recovery API.

Engine should still:

- log database-level recovery completion
- apply `policy_refuses`
- decide `allow_lossy_recovery` override behavior for degraded storage
- convert disallowed degraded state into `RecoveryError::StorageDegraded`
- apply `RecoveredState` to `TransactionCoordinator`

Acceptance:

- `apply_storage_config` no longer needs to be called from recovery
- `SegmentedStore::recover_segments()` is no longer called by engine recovery
- degraded-storage behavior remains public-policy equivalent
- coordinator bootstrap still sees the folded final version

## Phase F - Lossy WAL Replay Mechanics

Goal: move the mechanical lossy wipe into storage while keeping lossy policy
and operator reporting in engine.

Current engine code to split:

- `handle_wal_recovery_outcome`
- `records_applied_before_failure`
- `version_reached_before_failure`
- partial-state discard via fresh `SegmentedStore::with_dir`
- bypass handling through `CoordinatorRecoveryError::should_bypass_lossy()`
- conversion of coordinator error into lossy report text
- warning logs

Storage should own:

- checking whether lossy replay was requested
- preserving hard-fail bypass rules
- sampling records-applied and version-before-wipe facts
- replacing partial storage state with a fresh store after an allowed replay
  failure
- returning `StorageLossyWalReplayFacts`

Engine should own:

- whether `allow_lossy_recovery` is set in public config
- constructing `LossyRecoveryReport`
- choosing `LossyErrorKind`
- public warning wording and tracing targets
- mapping non-lossy and bypassed failures into `RecoveryError`

Important rule:

Even with `allow_lossy_wal_replay=true`, storage must not wipe and continue
for any error where `CoordinatorRecoveryError::should_bypass_lossy()` is true,
or for failures from the snapshot install callback. The fallback is for WAL
replay failures, not primitive snapshot decode/install failures.

Acceptance:

- lossy replay still discards partial pre-failure state
- lossy replay report fields remain unchanged
- planning, MANIFEST, snapshot-plan, snapshot-callback, and legacy-format
  failures still hard-fail
- storage does not construct `LossyRecoveryReport`

Implementation shape:

- introduce a storage-local `handle_storage_wal_replay_outcome`
- route it through the temporary `durability::__internal` engine bridge
- have storage consume the opaque `StorageCoordinatorReplay` returned by
  `run_storage_coordinator_replay`; that object carries the replay result,
  pre-failure applied-record count, and lossy-enabled flag so engine cannot
  pass a mismatched policy/count pair back into storage
- have storage apply bypass and lossy-enabled checks, wipe partial storage when
  allowed, and return `StorageLossyWalReplayFacts`
- thread `StorageLossyWalReplayFacts` through
  `StorageRecoveryOutcome::lossy_wal_replay`
- have engine convert outcome facts into `LossyRecoveryReport`,
  `LossyErrorKind`, and warning logs
- leave public `allow_lossy_recovery` configuration and public error mapping in
  engine

## Phase G - Engine Wrapper Cleanup

Goal: reduce `Database::run_recovery` to engine orchestration and policy.

Tasks:

- define and re-export `run_storage_recovery`
- replace direct lower-runtime assembly with `run_storage_recovery`
- map `StorageRecoveryError` into `RecoveryError`
- keep `RecoveryMode` and role-specific public error conversion in engine
- keep degraded-storage policy in engine
- keep lossy report construction in engine
- keep follower persisted state restore in engine
- keep watermark construction in engine
- keep `TransactionCoordinator` bootstrap in engine
- remove obsolete private helpers from `database/recovery.rs`

The resulting engine flow should be short enough that each remaining branch is
clearly policy or public-open orchestration.

Acceptance:

- `Database::run_recovery` remains the only engine recovery entry point
- `RecoveryOutcome` shape remains engine-local
- `open.rs` does not learn about storage recovery internals
- engine no longer manually wires `RecoveryCoordinator`
- engine no longer manually applies WAL records into `SegmentedStore`

Implementation notes:

- `strata_storage::durability::run_storage_recovery` is the public Phase G wrapper
  around MANIFEST/codec preparation, `SegmentedStore` construction,
  coordinator replay, mechanical lossy WAL fallback, storage runtime config,
  and segment recovery
- `Database::run_recovery` supplies only the primitive snapshot install
  callback, maps `StorageRecoveryError`, builds `LossyRecoveryReport`, applies
  degraded-storage policy, restores follower state, bootstraps
  `TransactionCoordinator`, and constructs the watermark
- the temporary Phase C/Phase D/Phase E/Phase F engine bridge helpers are no longer
  exported through `durability::__internal`
- snapshot-plan failures (`SnapshotMissing` and `SnapshotRead`) explicitly
  bypass the lossy WAL fallback alongside MANIFEST planning and legacy-format
  failures
- snapshot callback failures explicitly bypass the lossy WAL fallback; lossy
  replay remains limited to failures from WAL replay, not primitive snapshot
  decode/install

## Phase H - Guards, Documentation, And Full Parity

Goal: finish the cleanup with explicit guardrails.

Guards:

```text
rg -n 'use strata_engine|strata_engine::|strata-engine' crates/storage/src crates/storage/Cargo.toml
rg -n 'RecoveryCoordinator|ManifestManager|apply_wal_record_to_memory_storage' crates/engine/src/database/recovery.rs
rg -n 'strata_engine::.*(TransactionCoordinator|RecoveryError|LossyRecoveryReport|LossyErrorKind)|use .*\b(TransactionCoordinator|RecoveryError|LossyRecoveryReport|LossyErrorKind)\b' crates/storage/src
rg -n 'primitive_tags|SnapshotSerializer|KvSnapshotEntry|EventSnapshotEntry|JsonSnapshotEntry|VectorSnapshotEntry|BranchSnapshotEntry' crates/storage/src/durability/recovery_bootstrap.rs
```

The second guard may need a narrow exception if `RecoveryError` mapping still
references storage coordinator error types indirectly. It should not allow
engine to keep constructing or driving `RecoveryCoordinator`.

Test commands:

```text
cargo fmt --all --check
git diff --check
cargo check -p strata-storage --all-targets
cargo check -p strata-engine --all-targets
cargo test -p strata-storage recovery_bootstrap -- --nocapture
cargo test -p strata-engine --lib database::recovery -- --nocapture
cargo test -p strata-engine --test recovery_parity -- --nocapture
cargo test -p strata-engine --test recovery_storage_policy -- --nocapture
cargo test -p strata-engine --lib checkpoint -- --nocapture
```

Full assurance before declaring recovery-bootstrap cleanup complete:

```text
cargo test -p strata-engine
```

Known pre-existing unrelated failures should be called out with exact test
names if the full package suite is not green.

Phase H full-package status:

`cargo test -p strata-engine` currently passes the recovery-bootstrap and checkpoint
coverage. `cargo test -p strata-engine --test recovery_storage_policy -- --nocapture`
also passes separately. The full package suite remains blocked by the same
broader architecture-cleanup-period failures outside recovery-bootstrap work:

- `database::branch_mutation::tests::test_rollback_delete_true_surfaces_storage_cleanup_failure`
- `database::tests::shutdown::shutdown_timeout_halt_interleaving_preserves_invariant`
- `database::tests::shutdown::shutdown_timeout_preserves_writer_halt_signal`
- `database::tests::shutdown::test_background_sync_failure_halts_writer_and_rejects_manual_commit`
- `database::tests::shutdown::test_begin_sync_failure_halts_writer_and_rejects_manual_commit`
- `database::tests::shutdown::test_commit_sync_failure_halts_writer_and_rejects_manual_commit`
- `database::tests::shutdown::test_resume_waits_for_inflight_halt_publication_before_restoring_accepting`
- `database::tests::shutdown::test_resume_while_still_failing_increments_failed_sync_count`
- `database::tests::shutdown::test_set_durability_mode_spawn_failure_rolls_back_state`
- `primitives::branch::index::tests::test_complete_delete_post_commit_classifies_default_marker_clear_failure`

Documentation updates:

- [engine-crate-map.md](../engine-crate-map.md) now describes
  `database/recovery.rs` as an engine policy/public-error adapter over
  `run_storage_recovery`, not as the owner of lower replay mechanics.
- [../storage/storage-crate-map.md](../../storage/storage-crate-map.md) now
  lists `durability/recovery_bootstrap.rs` as storage-owned lower durability
  runtime.
- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
  records the recovery-bootstrap cleanup implementation status and the one intentional correctness
  tightening.
- This document records the Phase H guard list, behavior-change ledger, and
  completion criteria.

Phase H implementation status:

| Check | Status |
| --- | --- |
| Storage owns the recovery bootstrap implementation. | `run_storage_recovery` is re-exported from `strata_storage::durability` and owns MANIFEST/codec prep, coordinator replay, lossy WAL replay mechanics, runtime config application, and segment recovery. |
| Engine owns recovery policy and public recovery conversion. | `Database::run_recovery()` supplies the primitive snapshot callback, maps `StorageRecoveryError`, applies degraded policy, builds `LossyRecoveryReport`, bootstraps `TransactionCoordinator`, restores follower state, and constructs the watermark. |
| Temporary recovery-bootstrap cleanup bridge helpers are not public. | `prepare_storage_manifest_for_recovery`, `run_storage_coordinator_replay`, `handle_storage_wal_replay_outcome`, and `complete_storage_recovery_after_replay` are private to `recovery_bootstrap.rs`. |
| Primitive semantics stay out of storage recovery bootstrap. | `recovery_bootstrap.rs` carries `LoadedSnapshot`, codecs, `SegmentedStore`, and raw recovery facts only; primitive section decode remains in engine. |

Acceptance:

- storage owns the recovery bootstrap implementation
- engine owns recovery policy and public recovery conversion
- all recovery-bootstrap cleanup guard commands are clean or have documented intentional exceptions
- all recovery-bootstrap cleanup characterization tests still pass

## Behavior Changes

No broad behavior redesign is intended for recovery-bootstrap cleanup. The implementation did uncover
one recovery correctness bug that is intentionally tightened below.

If implementation discovers a correctness bug that must be fixed while moving
recovery code, the PR must call it out explicitly in this section before the
fix lands. The call-out must include:

- the old behavior
- the new behavior
- why preserving the old behavior would be unsafe
- compatibility risk
- focused tests proving the new behavior

Examples of changes that require explicit call-out:

- changing whether follower-without-MANIFEST creates a MANIFEST
- changing which errors bypass lossy recovery
- changing when partial state is discarded during lossy replay
- changing public codec mismatch classification
- changing legacy-format handling
- changing degraded-storage open/refuse behavior
- changing snapshot trailing-data or WAL trailing-data acceptance

### Snapshot Install Callback Failure Lossy Bypass

Old behavior:

When `allow_lossy_recovery=true`, a primitive snapshot install callback failure
could be treated like a lossy WAL replay failure. Recovery could wipe
partially installed state and continue with an empty recovered store even
though the failure came from primitive snapshot decode/install, not from WAL
replay.

New behavior:

Snapshot install callback failures hard-fail and bypass the lossy WAL replay
fallback even when lossy recovery is enabled. Storage records that the
coordinator failure came from snapshot installation and returns the original
coordinator callback error without wiping partial storage.

Why preserving the old behavior would be unsafe:

The lossy fallback is a whole-store discard for WAL replay corruption. Applying
it to primitive snapshot decode/install failures can hide authoritative
snapshot corruption or incompatible primitive bytes behind a successful empty
open.

Compatibility risk:

Databases that previously opened by silently discarding state after a corrupt
or incompatible recovery snapshot now fail recovery. That is intentional:
successful empty open would be data loss, while a hard failure preserves the
evidence and forces operator action.

Focused tests:

- `lossy_wal_replay_snapshot_callback_failure_preserves_partial_storage`
- `recovery_snapshot_install_failure_bypasses_lossy_fallback_when_enabled`

## Residual Ownership After recovery-bootstrap cleanup

Some recovery-adjacent code should intentionally remain in engine after recovery-bootstrap cleanup:

- `RecoveryError` public taxonomy
- `RecoveryMode`
- `RecoveryOutcome`
- `TransactionCoordinator` bootstrap
- degraded-storage policy
- lossy report construction
- follower persisted state restore
- watermark construction
- subsystem recovery integration
- primitive snapshot install callback

Some recovery-adjacent work should wait for storage-runtime-config cleanup:

- public `StrataConfig` split
- storage-owned defaults for every storage runtime knob
- remaining open-path config application outside recovery
- block-cache global capacity configuration
- WAL writer runtime construction cleanup

Some work should remain explicitly out of scope:

- moving `TransactionCoordinator`
- branch bundle recovery/import/export semantics
- search/index recovery redesign
- primitive snapshot format redesign
- WAL/snapshot/MANIFEST format migrations

## Completion Criteria

recovery-bootstrap cleanup is complete when:

1. `Database::run_recovery` calls a storage-owned recovery bootstrap API for
   raw recovery mechanics.
2. Engine no longer constructs `RecoveryCoordinator`.
3. Engine no longer wires WAL record replay into `SegmentedStore`.
4. Engine no longer performs MANIFEST recovery prep directly.
5. Engine no longer calls `recover_segments()` directly in recovery.
6. Storage returns raw recovery facts and storage-local errors.
7. Engine maps those facts into `RecoveryError`, `StrataError`,
   `LossyRecoveryReport`, `TransactionCoordinator`, and `RecoveryOutcome`.
8. Snapshot install still decodes primitive sections in engine through the snapshot-install cleanup
   callback.
9. `TransactionCoordinator` remains engine-owned.
10. The configured-codec, snapshot-version-fold, runtime-config, lossy-bypass,
    and follower-state ordering guarantees are characterized by tests.
11. Storage has no engine dependency and no primitive snapshot install logic.
12. The recovery-bootstrap cleanup guard commands pass.
