# Boundary Baseline And Guardrails Plan

## Purpose

This document is the detailed execution plan for `boundary baseline` in
[engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md).

`boundary baseline` does not move runtime code. It establishes the inventory, split
decisions, and guardrails needed before the later mechanics moves:

- `checkpoint/WAL cleanup` - checkpoint and WAL compaction mechanics
- `snapshot-install cleanup` - snapshot decode and install mechanics
- `recovery-bootstrap cleanup` - recovery bootstrap mechanics
- `storage-runtime-config cleanup` - storage configuration application

The point of this phase is to make the boundary concrete enough that the next
PRs can move code without accidentally moving engine policy or primitive
semantics down into `storage`.

This workstream is worth doing because engine is expected to become broader in
the next cleanup phase. It may absorb graph, vector, search, executor legacy,
and security responsibilities. That makes the storage boundary more important:
engine can be broad, but it must not be blurry.

## Baseline Result

The current crate graph is structurally correct:

- `strata-storage` has no Cargo dependency on `strata-engine`
- `strata-engine` depends on `strata-storage`
- upper crates should continue to enter database runtime behavior through
  engine, not storage

Observed boundary baseline guard results:

- `cargo tree -p strata-storage --depth 2` shows no `strata-engine`
  dependency.
- `cargo tree -p strata-engine --depth 1` shows `strata-engine` depending on
  `strata-storage`, which is the intended direction.
- the storage import guard finds documentation references to
  `strata_engine::...`, but no storage code imports or Cargo dependency on
  engine.
- the engine lower-runtime residue guard still finds checkpoint, compaction,
  snapshot install, recovery coordinator, manifest, and config-application
  mechanics under `crates/engine/src/database`; those matches are the boundary baseline
  baseline for the earlier mechanics cleanup phases to reduce.

The remaining problem is physical ownership inside engine:

- several `crates/engine/src/database/*` modules host lower storage-runtime
  mechanics
- those mechanics should move to storage behind raw substrate APIs
- engine should keep public APIs, lifecycle orchestration, policy, reporting,
  and error interpretation

## Files In Scope

The boundary baseline inventory covers these files:

- [database/compaction.rs](../../crates/engine/src/database/compaction.rs)
- [database/snapshot_install.rs](../../crates/engine/src/database/snapshot_install.rs)
- [database/recovery.rs](../../crates/engine/src/database/recovery.rs)
- [database/open.rs](../../crates/engine/src/database/open.rs)
- [database/config.rs](../../crates/engine/src/database/config.rs)

The plan intentionally does not inventory all of `engine::database`. Modules
such as lifecycle, follower refresh, health reporting, retention reporting,
and compatibility remain engine-shaped unless a later subplan proves a smaller
storage mechanic is hidden inside them.

## Classification Labels

Each surface is classified as one of:

- `Stay Engine` - public API, lifecycle orchestration, product/runtime policy,
  operator interpretation, primitive semantics, branch semantics, or engine
  error conversion.
- `Move Storage` - generic persistence mechanics, WAL/snapshot/manifest
  mechanics, segmented-store install/replay mechanics, or storage-only config
  application.
- `Split` - currently mixes both. Later epics should extract a storage-owned
  helper and leave an engine wrapper or policy layer behind.
- `Watch` - not moved by this program now, but should be protected from
  drifting into the wrong layer.

## Inventory: `database/compaction.rs`

This module currently mixes public engine methods with storage-owned
checkpoint mechanics.

| Surface | Current role | Classification | Target epic |
|---|---|---|---|
| `Database::flush` | Public guarded WAL flush API. Checks shutdown state and delegates to the unguarded body. | Stay Engine | None |
| `Database::flush_internal` | WAL writer flush body used by public flush and shutdown paths. It is tied to engine shutdown semantics and WAL writer health. | Stay Engine, Watch | None |
| `Database::checkpoint` | Public API plus storage mechanics: WAL flush, quiesced watermark, checkpoint data collection, snapshot dir creation, manifest load/update, checkpoint coordinator call, snapshot pruning. | Split | checkpoint/WAL cleanup |
| `Database::compact` | Public API plus storage mechanics: manifest load, active WAL segment lookup, codec-aware WAL compactor, compaction result mapping. | Split | checkpoint/WAL cleanup |
| `Database::disk_usage` | Public diagnostic API aggregating WAL writer facts and snapshot directory size. | Stay Engine | None |
| `Database::collect_checkpoint_data` | Walks `SegmentedStore` by physical type tags and builds storage durability snapshot DTOs. Includes primitive-specific checkpoint serialization details. | Split | checkpoint/WAL cleanup or snapshot-install cleanup |
| `Database::load_or_create_manifest` | Loads/creates storage manifest and updates active WAL segment. | Move Storage | checkpoint/WAL cleanup / recovery-bootstrap cleanup |

### checkpoint/WAL cleanup split decision

`Database::checkpoint()` should stay as the public engine method. The storage
side should own a helper shaped roughly like:

```text
storage_checkpoint(input) -> StorageCheckpointOutcome
```

The exact type names belong in the checkpoint/WAL cleanup subplan, but the ownership split is:

- engine supplies lifecycle-safe inputs: storage handle, data directory or
  layout, codec id, database UUID, WAL writer facts, quiesced watermark
- storage performs checkpoint coordinator construction, manifest updates,
  WAL/snapshot mechanics, and returns raw outcome facts
- engine logs and maps storage errors into engine errors

### Checkpoint collector caution

`collect_checkpoint_data` is the most sensitive part of `compaction.rs`.

It is physically close to storage because it walks `SegmentedStore` and builds
snapshot DTOs. But it also knows how engine primitives are represented:

- graph rows are currently routed through the KV section with
  `TypeTag::Graph`
- event metadata keys are skipped
- branch index keys are skipped
- vector config rows and vector record bytes are interpreted

checkpoint/WAL cleanup should not blindly move this function into storage as-is. The likely
split is:

- storage owns generic snapshot DTO construction and storage scan mechanics
- engine or primitive-owned adapters provide primitive record materialization
  where semantic knowledge is required
- storage must not learn branch-index, event-chain, vector-model, or search
  meaning

This is likely the long-term split, not merely a fallback. Checkpoint
construction naturally needs primitive materialization knowledge that should
not sink into storage. checkpoint/WAL cleanup should assume the collector, or at least the
primitive materialization part of it, remains engine-owned and is supplied to
storage as data or callbacks. The storage-owned part should be the generic
checkpoint runtime around that materialized data.

## Inventory: `database/snapshot_install.rs`

This module looked storage-shaped during boundary baseline because it installs rows into
`SegmentedStore`, but the snapshot-install cleanup deep pass corrected the boundary: primitive
section dispatch and DTO decode are engine concerns. Storage owns only the
generic decoded-row install helper that starts after engine has produced
storage rows.

| Surface | Current role | Classification | Target epic |
|---|---|---|---|
| `InstallStats` | Primitive-specific counts of entries installed by decoded snapshot section. | Stay Engine | snapshot-install cleanup |
| `InstallStats::total_installed` | Engine invariant over primitive-specific counters. | Stay Engine | snapshot-install cleanup |
| `install_snapshot` | Engine wrapper: validates codec policy, decodes primitive sections, builds generic storage-row plan, calls storage install helper. | Stay Engine, Split | snapshot-install cleanup |
| `decode_kv_section` | Decodes KV section, preserves tombstones/TTL, dispatches by `TypeTag`. | Stay Engine | snapshot-install cleanup |
| `decode_event_section` | Decodes Event section and reconstructs event user keys from sequence numbers. | Stay Engine | snapshot-install cleanup |
| `decode_json_section` | Decodes JSON section and installs doc IDs as user keys. | Stay Engine | snapshot-install cleanup |
| `decode_branch_section` | Decodes branch snapshot section and installs branch metadata rows. | Stay Engine | snapshot-install cleanup |
| `decode_vector_section` | Decodes vector collection/config rows and reinstalls raw vector record bytes. | Stay Engine | snapshot-install cleanup |
| `decode_value_json` | Decodes persisted `Value` bytes from primitive snapshot sections. | Stay Engine | snapshot-install cleanup |
| `install_decoded_snapshot_rows` | Validates and installs already-decoded generic storage rows. | Move Storage | snapshot-install cleanup |

### snapshot-install cleanup split decision

The original boundary baseline instinct was to move most of the install path downward because
it performs inverse persistence mechanics:

- decode snapshot section DTOs
- reconstruct storage keys
- preserve tombstones, TTL, versions, and timestamps
- call `SegmentedStore::install_snapshot_entries`

snapshot-install cleanup tightened that caution into the actual ownership rule. The primitive DTO
decode remains engine-owned because `KV`, `Event`, `Json`, `Vector`, `Branch`,
and Graph-as-KV routing are engine primitive persistence semantics, not generic
storage mechanics. Storage receives only generic decoded row groups.

Storage may know:

- `TypeTag` as an opaque storage-family router
- `DecodedSnapshotEntry`
- tombstones, TTLs, versions, timestamps, spaces, and user keys
- generic preflight validation and row installation

Storage must not learn:

- primitive section byte tags
- primitive snapshot DTO schemas
- `SnapshotSerializer`
- JSON path behavior
- event-chain verification
- vector metric/model policy
- branch workflow policy
- graph semantics

## Inventory: `database/recovery.rs`

This module intentionally centralizes recovery, but it mixes three layers:

1. engine open orchestration
2. engine recovery policy and classification
3. lower storage recovery bootstrap mechanics

| Surface | Current role | Classification | Target epic |
|---|---|---|---|
| `RecoveryMode` | Engine open mode: primary vs follower. Used for policy and error role. | Stay Engine | recovery-bootstrap cleanup may introduce storage-neutral mode facts |
| `RecoveryMode::as_error_role` | Engine error presentation mapping. | Stay Engine | None |
| `RecoveryOutcome` | Engine open result bundle: UUID, codec, storage, coordinator, watermark, lossy report, follower state. | Stay Engine, Split | recovery-bootstrap cleanup |
| `Database::run_recovery` | High-level recovery sequence: config validation, manifest prep, WAL codec, storage construction, coordinator recovery, lossy fallback, segment recovery, follower state, watermark. | Split | recovery-bootstrap cleanup |
| `policy_refuses` | Engine policy for degraded storage classes. | Stay Engine | None |
| `ManifestPreparation` | Manifest result containing database UUID and snapshot install codec. | Move Storage or Split | recovery-bootstrap cleanup |
| `prepare_manifest` | Manifest load/create, codec validation, segments dir creation, follower no-manifest fallback. | Split | recovery-bootstrap cleanup |
| `run_coordinator_recovery` | Builds `RecoveryCoordinator`, wires snapshot install and WAL record application callbacks. | Move Storage, with callback split | recovery-bootstrap cleanup |
| `handle_wal_recovery_outcome` | Engine lossy-recovery policy and report construction. | Stay Engine, with storage raw facts | recovery-bootstrap cleanup |
| `coordinator_error_to_lossy_strata_error` | Engine error conversion for lossy report classification. | Stay Engine | None |
| `restore_follower_state` | Engine follower-state validation and cleanup. | Stay Engine | None |

### recovery-bootstrap cleanup split decision

`Database::run_recovery` should remain the engine entry point. It composes
database open semantics and returns engine-owned state.

The storage-owned helper should take over lower replay mechanics:

- manifest preparation/load/create mechanics
- WAL codec resolution for replay
- segmented-store recovery orchestration
- snapshot install callback wiring
- WAL record application
- raw replay stats and storage health facts

Engine should keep:

- primary/follower open policy
- whether degraded storage may open
- lossy fallback decision and report wording
- follower state restore
- `TransactionCoordinator` bootstrap if coordinator remains engine-owned
- conversion into `RecoveryError` and `StrataError`

### Coordinator ownership caution

`TransactionCoordinator` is currently in engine even though earlier storage
work moved generic transaction runtime into storage. recovery-bootstrap cleanup should not casually
move coordinator policy while moving recovery bootstrap. If coordinator
ownership changes, that needs its own subplan or a clearly scoped recovery-bootstrap cleanup section.

This is a load-bearing recovery-bootstrap cleanup decision. If storage owns WAL replay mechanics while
`TransactionCoordinator` stays engine-owned, the boundary probably needs a
callback or adapter shape for per-record replay and final coordinator
bootstrap. That can be correct, but it must be designed intentionally before
recovery code moves.

## Inventory: `database/open.rs`

Most of `open.rs` is engine-owned runtime/product orchestration. Only small
storage-facing utilities and WAL runtime wiring are candidates for movement.

| Surface | Current role | Classification | Target epic |
|---|---|---|---|
| `apply_storage_config` | Applies seven storage knobs to `SegmentedStore`. | Move Storage | storage-runtime-config cleanup |
| `restrict_dir` | Database data directory permission hardening. | Stay Engine, Watch | None |
| `sanitize_config` | Engine/product behavior for feature-gated `auto_embed`. | Stay Engine | None |
| `restrict_file` | Lock/config support file permission hardening. | Stay Engine, Watch | None |
| `AcquiredDatabase` | Engine registry/open orchestration state. | Stay Engine | None |
| `Database::open` | Public/internal database opener with default subsystem shape. | Stay Engine | None |
| `Database::open_with_config` | Public/internal config-driven opener. | Stay Engine | None |
| `Database::acquire_primary_db` | Registry, path lock, subsystem recovery, lifecycle publication. | Stay Engine | None |
| `Database::repair_space_metadata_on_open` | Engine primitive/space metadata repair. | Stay Engine | None |
| `Database::open_follower` | Engine follower opener. | Stay Engine | None |
| `Database::acquire_follower_db` | Engine follower construction around recovery outcome. | Stay Engine | recovery-bootstrap cleanup touches recovery call only |
| `Database::spawn_wal_flush_thread` | WAL background sync thread with engine shutdown and health latching. | Split / Watch | storage-runtime-config cleanup or later |
| `Database::open_finish` | Primary open tail: recovery, support dirs, WAL writer, block cache, DB struct, flush thread, compaction scheduling. | Split | recovery-bootstrap cleanup / storage-runtime-config cleanup |
| `Database::cache` | Engine cache-mode public opener. | Stay Engine | None |
| `Database::create_ephemeral_bare` | Engine cache DB construction plus storage config application. | Split | storage-runtime-config cleanup |
| `Database::open_runtime` and mode helpers | Engine product/runtime composition entry point. | Stay Engine | boundary closeout |

### storage-runtime-config cleanup split decision

`apply_storage_config` should move first. It is a narrow, low-risk helper that
already takes only `SegmentedStore` and `StorageConfig`.

The likely storage API shape is:

```text
SegmentedStore::apply_runtime_config(&StorageRuntimeConfig)
```

or:

```text
storage::runtime_config::apply_to_store(&SegmentedStore, &StorageRuntimeConfig)
```

The exact naming belongs in storage-runtime-config cleanup. The important ownership point is that engine
should not keep a hand-written list of storage setter calls once storage owns
those knobs.

### WAL flush thread caution

`spawn_wal_flush_thread` is mixed:

- lower mechanics: periodic WAL sync, writer background sync handles,
  meta-file publishing
- engine policy: accepting transaction halt, health latch, shutdown signal,
  test hooks, lifecycle integration

Do not move it as part of boundary baseline. A later storage-runtime-config cleanup or follow-up runtime subplan should
decide whether storage owns a generic WAL sync worker while engine owns
health/shutdown callbacks.

## Inventory: `database/config.rs`

`config.rs` contains both public engine config and storage-only runtime knobs.

| Surface | Current role | Classification | Target epic |
|---|---|---|---|
| `SHADOW_KV`, `SHADOW_JSON`, `SHADOW_EVENT` | Intelligence/embedding shadow collection names. | Stay Engine now, possible intelligence/runtime follow-up | boundary closeout |
| `CONFIG_FILE_NAME` | Engine database config artifact name. | Stay Engine | None |
| `ModelConfig` | Inference/search model endpoint config. | Stay Engine or Intelligence later | boundary closeout |
| `StorageConfig` | Public engine config section containing storage knobs. | Split | storage-runtime-config cleanup |
| `StorageConfig::effective_*` | Storage-only derived values from memory budget. | Move Storage or Split | storage-runtime-config cleanup |
| `Default for StorageConfig` and storage default helpers | Storage-only defaults currently exposed through engine config. | Split | storage-runtime-config cleanup |
| `SnapshotRetentionPolicy` | Public config for storage checkpoint retention. | Split | storage-runtime-config cleanup |
| `StrataConfig` | Public database/product config surface. | Stay Engine | None |
| `StrataConfig::vector_storage_dtype` | Engine/vector semantic config helper. | Stay Engine | boundary closeout if vector folds into engine |
| `StrataConfig::durability_mode` | Public config parse into storage WAL durability mode. | Split | storage-runtime-config cleanup |
| `StrataConfig::default_toml` | User/operator-facing config template. | Stay Engine | None |
| `StrataConfig::from_file` | Engine config file parse, validation, env secret overrides. | Stay Engine | None |
| `StrataConfig::apply_env_overrides` | Product/runtime secret injection. | Stay Engine | None |
| `StrataConfig::write_default_if_missing` | Engine config artifact management. | Stay Engine | None |
| `StrataConfig::write_to_file` | Engine config serialization. | Stay Engine | None |
| `atomic_write_config` | Crash-safe config file write. It is file persistence, but for engine product config, not storage MANIFEST. | Stay Engine | None |

### storage-runtime-config cleanup split decision

`StrataConfig` remains the public engine config.

Storage should get its own storage-runtime config type that can be built from
the public config section:

```text
StorageRuntimeConfig
```

or:

```text
SegmentedStoreConfig
```

Engine should own the adapter:

```text
impl From<&StorageConfig> for StorageRuntimeConfig
```

if that impl does not introduce a `storage -> engine` dependency. More likely,
engine calls a storage constructor/builder with field values:

```text
StorageRuntimeConfig::builder()
    .memory_budget(...)
    .max_branches(...)
    ...
```

The exact shape belongs in storage-runtime-config cleanup. The key rule is:

- storage owns the meaning and application of storage-only knobs
- engine owns the public config file and product-facing names

## Candidate Storage APIs By Epic

These are not final signatures. They are the API families later subplans
should refine.

Before checkpoint/WAL cleanup moves code, write one joint API sketch for checkpoint/WAL cleanup, snapshot-install cleanup, and recovery-bootstrap cleanup:

```text
docs/engine/storage-runtime-boundary-api-sketch.md
```

Checkpoint output feeds snapshot install, and snapshot install feeds recovery.
Designing these boundary types independently will likely create rework.

### checkpoint/WAL cleanup checkpoint / compaction

Candidate module:

```text
crates/storage/src/durability/checkpoint_runtime.rs
```

Candidate surfaces:

```text
StorageCheckpointInput
StorageCheckpointOutcome
run_storage_checkpoint(input) -> StorageResult<StorageCheckpointOutcome>

StorageWalCompactionInput
StorageWalCompactionOutcome
compact_wal(input) -> StorageResult<StorageWalCompactionOutcome>
```

Engine responsibilities remain:

- call `check_not_shutting_down`
- handle ephemeral/follower no-op behavior
- flush WAL through engine shutdown semantics
- quiesce coordinator
- provide checkpoint data or primitive callbacks
- map `NoSnapshot` into the existing invalid-input behavior for compact

### snapshot-install cleanup snapshot install

Settled modules:

```text
crates/engine/src/database/snapshot_install.rs
crates/storage/src/durability/decoded_snapshot_install.rs
```

Settled surfaces:

```text
engine::database::install_snapshot(snapshot, codec, storage) -> StrataResult<InstallStats>
storage::durability::install_decoded_snapshot_rows(input)
    -> Result<StorageDecodedSnapshotInstallStats, StorageDecodedSnapshotInstallError>
```

Engine responsibilities:

- iterate `LoadedSnapshot.sections`
- dispatch by primitive section tags
- decode primitive snapshot DTOs with `SnapshotSerializer`
- reconstruct primitive-owned storage row keys
- retain primitive-specific `InstallStats`
- decide when install runs
- log recovery outcome
- classify install failure in recovery policy

Storage responsibilities:

- validate generic decoded row groups before mutation
- install already-decoded rows into `SegmentedStore`
- return generic row/group stats and storage-local errors
- avoid `LoadedSnapshot`, `SnapshotSerializer`, primitive tags, and primitive
  snapshot DTOs in runtime install modules

### recovery-bootstrap work

Candidate module:

```text
crates/storage/src/durability/bootstrap.rs
```

Candidate surfaces:

```text
StorageRecoveryMode
StorageRecoveryInput
StorageRecoveryOutcome
run_storage_recovery(input) -> StorageResult<StorageRecoveryOutcome>
```

Engine responsibilities remain:

- map primary/follower modes to storage-neutral inputs
- decide degraded/lossy policy
- restore follower state
- bootstrap engine coordinator or adapt from storage stats
- convert storage recovery errors into `RecoveryError`

### storage-runtime-config cleanup storage config

Candidate module:

```text
crates/storage/src/runtime_config.rs
```

Candidate surfaces:

```text
StorageRuntimeConfig
StorageRuntimeConfig::apply_to(&self, store: &SegmentedStore)
```

Engine responsibilities remain:

- parse `strata.toml`
- apply product profiles and feature-gated config sanitization
- preserve config file compatibility
- adapt public `StorageConfig` into storage runtime config

## Guardrails

These guardrails should be run during boundary baseline and reused in later epics.

### Cargo dependency guard

Storage must not depend on engine:

```sh
cargo tree -p strata-storage --depth 2
```

Expected current baseline:

- no `strata-engine` in the `strata-storage` dependency tree
- storage depends on `strata-core` but not on engine

Engine may depend on storage:

```sh
cargo tree -p strata-engine --depth 1
```

Expected current baseline:

- `strata-engine` depends on `strata-storage`

### Source import guard

Storage code must not import engine:

```sh
rg -n 'use strata_engine|strata_engine::|strata-engine' crates/storage/src crates/storage/Cargo.toml
```

Documentation comments may mention engine to describe upper-layer error
mapping or historical context. Code imports and Cargo dependencies are the
guarded surface.

### Primitive semantic guard for moved storage modules

Once the earlier mechanics cleanup phases create new storage modules, they must not accumulate primitive
semantics:

```sh
rg -n 'JsonPath|JsonPatch|SearchSubsystem|Recipe|VectorConfig|DistanceMetric|ChainVerification|BranchLifecycle|RetentionReport|HealthReport|executor' crates/storage/src
```

Expected behavior:

- no matches in newly moved storage-runtime modules
- existing documentation matches must be reviewed before treating them as
  failures

### Engine lower-runtime residue guard

After each move, check that engine is shrinking toward orchestration:

```sh
rg -n 'CheckpointCoordinator|WalOnlyCompactor|ManifestManager|RecoveryCoordinator|SnapshotSerializer|install_snapshot|apply_storage_config' crates/engine/src/database
```

Expected behavior:

- boundary baseline: matches are expected and form the baseline
- checkpoint/WAL cleanup: checkpoint/compaction matches should disappear or remain only in
  engine wrappers
- snapshot-install cleanup: `SnapshotSerializer` and `install_snapshot` remain only as intentional
  engine-owned primitive decode and recovery policy residue; storage install
  modules must not import primitive snapshot DTOs or tags
- recovery-bootstrap cleanup: recovery coordinator and manifest-prep mechanics should move down or
  become storage API calls
- storage-runtime-config cleanup: `apply_storage_config` should disappear from engine

### Upper-layer bypass guard

Executor and CLI should not drive storage recovery/checkpoint/open policy
directly:

```sh
rg -n 'strata_storage::.*(RecoveryCoordinator|CheckpointCoordinator|WalOnlyCompactor|ManifestManager|SnapshotSerializer)' crates/executor crates/cli
```

Expected behavior:

- no matches
- upper layers continue through engine/executor abstractions

## Characterization Requirements

Boundary baseline should make the later characterization burden explicit. The
earlier mechanics cleanup phases should not move code first and backfill parity
later.

Before the relevant code move, add or identify characterization coverage for:

- checkpoint determinism and manifest watermark behavior
- checkpoint then compact then reopen behavior
- snapshot install round-trip coverage for:
  - KV rows
  - graph rows stored in the KV section with `TypeTag::Graph`
  - event rows and skipped event metadata keys
  - JSON rows and tombstones
  - branch metadata rows and skipped branch index keys
  - vector collection config rows
  - vector record rows, including tombstones
  - TTL, timestamps, and version preservation
- recovery outcomes for:
  - normal replay
  - degraded storage accepted by policy
  - degraded storage refused by policy
  - lossy fallback
  - follower state restore and invalid persisted follower state cleanup

For performance-sensitive moves, use the existing benchmark vocabulary rather
than inventing a one-off microbenchmark:

- redb/YCSB-style storage characterization where checkpoint/recovery affects
  storage paths
- search-facing benchmarks where recovery or snapshot install affects
  recovered search state

## Acceptance Checklist

Boundary baseline is complete when:

1. This inventory document exists and is linked from the main engine/storage
   normalization plan.
2. The target surfaces for the earlier mechanics cleanup phases are classified
   as `Stay Engine`, `Move Storage`, `Split`, or `Watch`.
3. Candidate storage API families are named well enough for later subplans to
   refine them.
4. Guard commands are documented with expected current behavior.
5. The storage-runtime boundary joint API sketch is called out as a
   prerequisite before checkpoint/WAL cleanup code movement.
6. The `TransactionCoordinator` ownership question is explicitly marked as an
   recovery-bootstrap cleanup design decision.
7. Characterization requirements are documented before any runtime code moves.
8. No runtime code has moved as part of boundary baseline.

## Non-Goals

Boundary baseline does not:

- move checkpoint code
- move snapshot install code
- move recovery code
- move storage config code
- rename public database APIs
- change on-disk formats
- change open/recovery behavior
- change executor or CLI behavior

Those changes belong in the earlier mechanics cleanup phases, each with a narrower implementation plan.
