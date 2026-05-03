# ES3 Snapshot Install Boundary Plan

## Purpose

`ES3` is the second runtime cleanup epic in the engine/storage boundary
normalization workstream.

The original ES3 direction was too broad: it would have moved primitive
snapshot section decode into `strata-storage`. That creates the inverse of the
current problem. Instead of engine owning storage mechanics, storage would
start owning engine primitive concerns.

This revised ES3 keeps the ownership rule strict:

```text
storage owns durable row mechanics
engine owns primitive meaning and primitive persistence encodings
```

The goal is to extract only the generic decoded-row install mechanics that
belong to storage, while keeping snapshot primitive section dispatch and decode
in engine.

Read this together with:

- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
- [es1-boundary-baseline-and-guardrails-plan.md](./es1-boundary-baseline-and-guardrails-plan.md)
- [es2-es4-storage-runtime-boundary-api-sketch.md](./es2-es4-storage-runtime-boundary-api-sketch.md)
- [es2-checkpoint-wal-compaction-mechanics-plan.md](./es2-checkpoint-wal-compaction-mechanics-plan.md)
- [../storage/storage-engine-ownership-audit.md](../storage/storage-engine-ownership-audit.md)
- [../storage/storage-charter.md](../storage/storage-charter.md)
- [../architecture/architecture-recovery-target.md](../architecture/architecture-recovery-target.md)

## Boundary Rule

Storage may know:

- `SegmentedStore`
- branch ids as storage routing identifiers
- storage family identifiers such as the existing `TypeTag`
- namespace space strings
- opaque user-key bytes
- `DecodedSnapshotEntry`
- `DecodedSnapshotValue`
- commit version, timestamp, TTL, and tombstone metadata
- generic decoded row groups
- filesystem/container mechanics for WALs, snapshots, manifests, and segments
- storage-local errors for row install failures

Storage must not know:

- primitive snapshot section dispatch
- `primitive_tags::EVENT`, `primitive_tags::JSON`, `primitive_tags::VECTOR`,
  `primitive_tags::BRANCH`, or graph-as-KV meaning in install logic
- `EventSnapshotEntry`, `JsonSnapshotEntry`, `VectorSnapshotEntry`,
  `BranchSnapshotEntry`, or `KvSnapshotEntry` decode during install
- Event sequence-key reconstruction
- JSON doc-id key reconstruction
- Vector config key or vector record key conventions
- Branch metadata key conventions
- Graph row interpretation beyond receiving an already selected storage family
- `SnapshotSerializer` use for primitive sections inside storage install logic
- public `StrataError`, recovery reports, lossy/degraded policy, or operator
  guidance

`TypeTag` is an existing storage family identifier with primitive-shaped
variant names. ES3 should not expand that leak. Storage install code may carry
`TypeTag` as an opaque routing value because `SegmentedStore` already uses it,
but it should not branch on primitive-specific variants for snapshot install
statistics or behavior. A later cleanup can consider replacing this with an
opaque storage family id.

## ES3 Verdict

The following should remain in `strata-engine`:

- `LoadedSnapshot` section iteration for install
- dispatch by `primitive_tags`
- primitive section decode through `SnapshotSerializer`
- choice of canonical primitive-section codec
- KV snapshot DTO decode
- graph-as-KV interpretation through `TypeTag::Graph`
- Event sequence-key reconstruction
- JSON doc-id key reconstruction
- Branch metadata row reconstruction
- Vector config and vector record row reconstruction
- primitive-specific install stats
- mapping decode failures to public engine corruption errors
- deciding when snapshot install runs
- recovery callback wiring around `RecoveryCoordinator`
- lossy/degraded recovery policy
- database lifecycle orchestration and operator-facing logging

The following may move to `strata-storage`:

- installing an already-decoded set of storage row groups into
  `SegmentedStore`
- generic preflight validation of decoded row groups before mutation
- generic row install stats such as total rows and groups installed
- storage-local install errors around invalid generic input and
  `SegmentedStore` write failures

The important distinction is that engine converts primitive snapshot sections
into generic storage rows. Storage only installs those rows.

## Existing Code Map

The ES3 target surface is concentrated in:

- [database/snapshot_install.rs](../../crates/engine/src/database/snapshot_install.rs)
- [database/recovery.rs](../../crates/engine/src/database/recovery.rs)
- `SegmentedStore::install_snapshot_entries`

Existing storage format modules already define snapshot DTOs and primitive tag
constants. ES3 does not need to erase that existing format ownership. The line
is runtime behavior: storage should not gain a runtime snapshot installer that
decodes or interprets those primitive sections.

## Target Shape

Engine should continue to expose its current recovery-facing helper while ES3
is in progress:

```text
engine::database::install_snapshot(
    snapshot: &LoadedSnapshot,
    codec: &dyn StorageCodec,
    storage: &SegmentedStore,
) -> StrataResult<EngineSnapshotInstallStats>
```

Inside that helper, engine should do two separate phases:

```text
decoded_plan = engine_decode_snapshot_sections(snapshot)
storage_stats = storage::install_decoded_snapshot_rows(decoded_plan, storage)
engine_stats = merge primitive decode stats + storage install stats
```

The storage API should take only generic decoded rows:

```text
StorageDecodedSnapshotInstallInput<'a> {
    plan: &'a StorageDecodedSnapshotInstallPlan,
    storage: &'a SegmentedStore,
}

StorageDecodedSnapshotInstallPlan {
    groups: Vec<StorageDecodedSnapshotInstallGroup>,
}

StorageDecodedSnapshotInstallGroup {
    branch_id: BranchId,
    type_tag: TypeTag,
    entries: Vec<DecodedSnapshotEntry>,
}

StorageDecodedSnapshotInstallStats {
    groups_installed: usize,
    rows_installed: usize,
}
```

Names can change during implementation, but the API must not accept
`LoadedSnapshot`, `SnapshotSerializer`, primitive tags, or primitive DTOs.

## Codec Contract

There are two codec markers involved in snapshot recovery:

- Snapshot container codec id: the codec id recorded in the snapshot file
  header and validated by `SnapshotReader` as a database compatibility marker.
  The snapshot container does not encode section bytes through this codec.
- Primitive section codec: the codec used by `SnapshotSerializer` for section
  DTO payloads.

Checkpoint construction currently serializes primitive section DTOs with the
canonical identity section codec, while the snapshot header records the
configured storage codec id. Therefore snapshot install must not decode
primitive sections with the snapshot header codec.

Ownership:

- Storage may own snapshot file container read/write mechanics and codec-id
  validation.
- Engine owns primitive section decode during install.
- Engine should use the same canonical primitive-section codec that checkpoint
  construction used.
- `SnapshotReader` remains responsible for snapshot header codec validation
  before engine receives `LoadedSnapshot`.

ES3 must add characterization for non-identity snapshot files so this contract
does not regress.

## Partial Mutation Contract

Corrupt snapshot data must not partially mutate storage.

The corrected flow is:

```text
engine decodes every section
engine validates every primitive row conversion
engine builds complete generic storage row plan
storage validates generic row groups
storage mutates SegmentedStore
```

Decode failures, unknown primitive tags, invalid KV `TypeTag` bytes, corrupt
JSON `Value` payloads, and primitive DTO shape errors should all fail before
any storage write.

Storage does not need to understand why a plan was produced. It only needs to
avoid mutating on generic input validation failures that it can detect before
calling `SegmentedStore::install_snapshot_entries`.

Generic storage preflight validation should include empty groups, empty storage
space names, reserved zero commit versions, and duplicate row identities across
the complete plan. The duplicate identity is only storage routing data:
`(branch_id, storage_family, space, user_key, version)`.

## Stats Contract

Engine may keep primitive-specific stats:

```text
EngineSnapshotInstallStats {
    kv_rows
    graph_rows
    event_rows
    json_rows
    vector_config_rows
    vector_rows
    branch_rows
    sections_skipped
}
```

Storage stats should be generic:

```text
StorageDecodedSnapshotInstallStats {
    groups_installed
    rows_installed
}
```

Storage should not return `json_rows`, `vector_rows`, `event_rows`, or similar
primitive-shaped counters.

## Error Contract

Engine owns errors for primitive decode and public recovery interpretation:

- unknown primitive tag
- corrupt primitive section payload
- corrupt JSON-encoded `Value`
- invalid type tag byte in KV snapshot DTO
- public `StrataError`
- lossy/degraded classification

Storage owns errors for generic decoded-row install mechanics:

```text
StorageDecodedSnapshotInstallError {
    EmptyGroup { ... }
    EmptySpace { ... }
    ZeroVersion { ... }
    DuplicateRow { ... }
    RowCountMismatch { ... }
    Install { storage_family, source }
}
```

The exact shape can change, but storage-local errors must be
`#[non_exhaustive]` if public, must not mention primitive names, and must not
mention engine policy.

## Explicit Behavior Changes

ES3 is primarily a boundary cleanup, but two strictness changes intentionally
land with it because they close recovery/snapshot correctness gaps exposed by
the new characterization:

- Recovery now hard-fails if a loaded snapshot reaches the engine install
  callback without an install codec. `None` is still used for deliberate
  no-snapshot paths such as primary first-open and follower-without-MANIFEST;
  those paths have no manifest-recorded snapshot, so the callback should not
  fire. If it does, silently skipping snapshot install would be data loss.
- Snapshot primitive deserializers and the outer snapshot section parser reject
  trailing bytes after the declared entries/sections. Writers do not emit those
  bytes, so this is a strict-input hardening change rather than a format change
  for valid snapshots.

The same format-hardening pass also removes declared-count preallocation from
primitive deserializers so hostile counts fail by EOF instead of requesting huge
allocations. That is defensive implementation detail, not a new boundary owner.

## Implementation Plan

ES3 restarts from `ES3A`. Each phase is a straight lettered slice.

### ES3A Snapshot Install Characterization Rebaseline

Restore the pre-move crate state and pin current behavior in engine before any
new extraction.

Coverage should include:

- KV section installs live entries
- KV section preserves commit version and timestamp
- KV section preserves tombstones
- KV section preserves TTL
- KV section routes graph-as-KV rows into graph storage
- invalid KV `TypeTag` returns a hard error
- Event section reconstructs big-endian sequence keys
- JSON section uses doc id as the user key
- JSON section preserves tombstones
- Branch section installs branch metadata rows
- Branch section preserves branch tombstones
- Vector section installs collection config rows
- Vector section installs raw vector record rows
- Vector section preserves vector tombstones
- standalone non-empty Graph section preserves current skip behavior if still
  compatible
- unknown primitive section tag returns a hard error
- corrupt section payloads return section-specific decode errors
- corrupt JSON-encoded `Value` payloads return section-specific value errors
- checkpoint + compact + reopen preserves tombstones, TTL, and branch metadata
- follower open installs checkpoint snapshot before delta WAL replay
- non-identity snapshot header codec id does not drive primitive section decode
- invalid later section does not partially install earlier valid sections
- invalid KV type tag does not partially install earlier valid KV rows

ES3A should not move code into storage. It should make the existing behavior
and the desired no-partial-mutation behavior explicit.

Because the pre-ES3A engine installer decoded and mutated section-by-section,
some ES3A characterization targets cannot be made true with tests alone. ES3A
may therefore include engine-local fixes that do not move code across the
storage boundary:

- build the complete engine decoded-row plan before calling storage
- validate KV-section `TypeTag` bytes before mutation
- keep primitive section decode on the canonical checkpoint section codec

If those fixes land during ES3A, the later ES3C and ES3D phases should be
treated as phase-accounting checkpoints rather than reimplemented work.

### ES3B Generic Storage Row Install Surface

Add a storage-owned helper that installs already-decoded generic row groups.

Allowed storage inputs:

- `BranchId`
- `TypeTag` as an opaque storage family id
- `DecodedSnapshotEntry`
- `SegmentedStore`

Forbidden storage inputs:

- `LoadedSnapshot`
- `SnapshotSerializer`
- `primitive_tags`
- primitive snapshot DTOs
- primitive-specific stats

Storage tests should use synthetic decoded row groups. They should not build
snapshot primitive sections.

ES3B is complete when storage can install generic decoded row groups without
knowing which primitive produced them.

### ES3C Engine Decode Plan Extraction

Refactor engine `database::snapshot_install` into a decode-then-install flow.

If ES3A already introduced this engine-local decode plan to satisfy
no-partial-mutation characterization, ES3C should verify and preserve that
shape while integrating the storage generic row install surface from ES3B.

Engine should:

- iterate `LoadedSnapshot.sections`
- dispatch by primitive section tag
- decode each primitive DTO section
- reconstruct storage row keys
- decode JSON `Value` blobs where required by the existing format
- validate `TypeTag` bytes before any storage write
- build a full generic storage row install plan
- retain primitive-specific stats

This phase should fix partial mutation from corrupt snapshots by ensuring the
full plan is built before calling the storage helper.

### ES3D Canonical Section Codec Fix

Make the engine decode plan use the canonical primitive-section codec used by
checkpoint construction, not the snapshot header codec id.

If ES3A already made this codec separation explicit, ES3D should retain the
coverage and ensure the new ES3B storage install surface does not reintroduce
snapshot header codec use for primitive section payloads.

Add or keep tests proving:

- an AES/non-identity snapshot file can be loaded with the configured storage
  codec id
- primitive sections inside that snapshot are decoded with the canonical
  section codec
- checkpoint + compact + reopen restores rows from the snapshot-only path

If this fix naturally lands with ES3C, the PR should still call it out as the
ES3D acceptance item.

### ES3E Recovery Wrapper Integration

Wire the engine snapshot install helper to call the generic storage row install
surface after engine decode succeeds.

Engine keeps:

- `install_codec` option handling
- recovery callback ownership
- snapshot-id and watermark logging
- primitive-specific install stats
- storage error mapping into the current callback error type
- lossy/degraded recovery classification

Storage keeps:

- decoded row group install mechanics only

### ES3F Residue Cleanup And Guards

Clean up duplicate code and document intentional residue.

Expected result:

- engine still owns primitive snapshot decode
- storage owns only generic decoded row install
- no storage snapshot install module imports `SnapshotSerializer`
- no storage snapshot install module imports `primitive_tags`
- no storage snapshot install module imports primitive snapshot DTOs
- engine recovery/open MANIFEST policy remains until ES4

Intentional ES3 residue after cleanup:

- `crates/engine/src/database/snapshot_install.rs` keeps
  `SnapshotSerializer`, `primitive_tags`, primitive snapshot DTO decode, and
  primitive-specific `InstallStats`. This is the desired boundary because the
  section layout encodes engine primitive persistence semantics.
- `crates/storage/src/durability/decoded_snapshot_install.rs` is the only new
  storage install surface. It accepts `StorageDecodedSnapshotInstallPlan`
  values made of generic row groups and must not import primitive tags,
  primitive snapshot DTOs, `LoadedSnapshot`, or `SnapshotSerializer`.
- `crates/storage/src/durability/format/*` and
  `crates/storage/src/durability/disk_snapshot/*` may still define and carry
  primitive snapshot tags/DTOs as on-disk format and container mechanics. That
  is not runtime install ownership.
- The strict trailing-data checks and count-preallocation removal in the
  snapshot format/container modules are intentional format-hardening residue
  bundled with ES3. They do not move primitive install semantics into storage.
- `RecoveryCoordinator`, MANIFEST/open policy, and recovery outcome mapping
  remain engine/recovery residue for ES4.

## Guardrails

Run these ownership checks after each implementation phase that touches
storage install code:

```sh
rg -n 'use strata_engine|strata_engine::|strata-engine' crates/storage/src crates/storage/Cargo.toml
rg -n 'SnapshotSerializer|primitive_tags|KvSnapshotEntry|EventSnapshotEntry|JsonSnapshotEntry|VectorSnapshotEntry|BranchSnapshotEntry|LoadedSnapshot' crates/storage/src/durability -g '*install*.rs'
rg -n 'JsonPath|JsonPatch|SearchSubsystem|Recipe|VectorConfig|DistanceMetric|ChainVerification|BranchLifecycle|RetentionReport|HealthReport|executor' crates/storage/src
```

Interpretation:

- Existing format modules may legitimately define primitive snapshot DTOs and
  tag constants.
- Generic install modules must not use those DTOs or tags. The `*install*.rs`
  guard is intentionally broader than the current
  `decoded_snapshot_install.rs` file so a future storage install module cannot
  bypass the ES3 boundary by choosing a new filename.
- Any new match in storage outside established format/container code needs an
  explicit boundary justification.
- A broad search over all `crates/storage/src/durability` is still useful as an
  audit, but it is expected to report established format/container modules.

Run these engine residue checks:

```sh
rg -n 'install_decoded_snapshot|StorageDecodedSnapshotInstall' crates/engine/src/database
rg -n 'RecoveryCoordinator|ManifestManager|apply_storage_config' crates/engine/src/database
```

`RecoveryCoordinator`, `ManifestManager`, and open/recovery policy residue are
not ES3 cleanup targets. They belong to ES4.

## Verification Gates

Run the standard formatting and relevant tests:

```sh
cargo fmt --all --check
cargo check -p strata-storage --all-targets
cargo check -p strata-engine --all-targets
cargo test -p strata-engine --lib database::snapshot_install -- --nocapture
cargo test -p strata-engine --lib database::tests::checkpoint -- --nocapture
cargo test -p strata-engine --lib database::tests::open -- --nocapture
cargo test -p strata-engine --lib database::tests::codec -- --nocapture
cargo test -p strata-engine --test recovery_parity -- --nocapture
```

When storage generic install code is introduced, add and run the focused
storage tests for that helper:

```sh
cargo test -p strata-storage decoded_snapshot_install -- --nocapture
```

Run recovery and follower tests when callback wiring changes:

```sh
cargo test -p strata-engine --test recovery_tests -- --nocapture
cargo test -p strata-engine --test follower_tests -- --nocapture
cargo test -p strata-engine --lib test_issue_1730 -- --nocapture
```

## Acceptance Checklist

ES3A is complete when:

1. Crate code is back to the pre-move boundary.
2. Engine snapshot install characterization covers current primitive decode
   behavior.
3. Tests cover canonical section codec behavior for non-identity snapshot
   files.
4. Tests cover no partial mutation for decode/validation failures that happen
   before storage install.

ES3B is complete when:

1. Storage exposes only a generic decoded row install surface.
2. Storage install stats are generic.
3. Storage install errors are storage-local and primitive-agnostic.
4. Storage tests use synthetic decoded row groups, not primitive snapshot DTOs.

ES3C is complete when:

1. Engine builds a complete decoded row install plan before storage mutation.
2. Engine retains primitive section decode and primitive-specific stats.
3. Invalid primitive data fails before any storage write.

ES3D is complete when:

1. Primitive section decode uses the canonical section codec.
2. Snapshot header codec validation remains separate.
3. Non-identity checkpoint + compact + reopen coverage is green.

ES3E is complete when:

1. Engine calls the storage generic row install helper only after successful
   primitive decode.
2. Recovery callback policy remains engine-owned.
3. Public error behavior is preserved.

ES3F is complete when:

1. Storage has no new primitive runtime install concerns.
2. Engine decode residue is intentional and documented.
3. Ownership guards and verification tests pass.

## Non-Goals

ES3 does not move:

- `RecoveryCoordinator`
- MANIFEST/open policy
- lossy/degraded recovery classification
- primitive checkpoint DTO definitions
- primitive query/index/rebuild behavior
- branch lifecycle behavior
- vector/search/graph semantics

Those are either engine responsibilities or later ES4-plus decisions.
