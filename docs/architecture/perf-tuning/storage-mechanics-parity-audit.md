# Storage Mechanics Parity Audit

## Purpose

This audit tracks whether storage preserved the important mechanics from
the old storage engine. The goal is not to prove that similarly named types
exist. The goal is to prove that storage has the same invariants,
behavioral results, and asymptotic read/write costs that made the old storage
engine viable at scale.

The old storage engine is the executable reference. For each mechanic, this
document records the old files to inspect, the storage files to inspect,
the expected invariant, the current status, and the test/perf evidence needed
before we call the mechanic restored.

## Current Benchmark Signal

The current storage L9 benchmark results show a source-fanout scaling
failure.

| Mode | Scale | Load | Point latest | Point throughput | Scan prefix | Scan range |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| storage cache | 10M | 12,620 ops/s | 3,326 ops/s | 3,227 ops/s | 2,828 ops/s | 2,588 ops/s |
| storage standard | 10M | 11,643 ops/s | 3,276 ops/s | 3,229 ops/s | 2,836 ops/s | 2,582 ops/s |
| old cache | 10M | 285,173 ops/s | 82,091 ops/s | 111,430 ops/s | 13,867 ops/s | 9,868 ops/s |

Storage 10M point reads visited `1,000,000` rows for `10,000` reads, or
about 100 source probes per read. Storage 10M scans performed `1,010,000`
cursor seeks for `10,000` scans, also about 100 source seeks per scan.

Relevant result files:

- `benchmarks/results/storage-l9/storage-l9-scale-2026-06-05T07-13-25Z-12d2790b.json`
- `benchmarks/results/storage-l9/storage-l9-scale-2026-06-05T13-19-49Z-12d2790b.json`
- `benchmarks/results/storage-old-cache/storage-old-cache-scale-2026-06-05T07-14-14Z-12d2790b.json`

## Audit Todo List

- [x] Audit LSM layout and level invariants.
- [x] Audit point-read source pruning.
- [x] Audit scan source planning and iterator behavior.
- [x] Audit compaction selection, output shape, and installation.
- [x] Audit MVCC, tombstone, TTL, and history semantics.
- [x] Audit branch inheritance, fork, and materialization mechanics.
- [x] Audit durability, manifest, WAL, and recovery mechanics.
- [x] Audit cache, standard, wasm-none, and memory budget modes.
- [x] Define differential tests and perf counters for gaps.
- [x] Write final parity matrix and prioritized fix list.

## Status Labels

- `Confirmed`: storage preserves the old invariant and has test/perf proof.
- `Partial`: storage has some structures or behavior, but not full parity.
- `Missing`: storage does not preserve the old mechanic.
- `Unknown`: not yet audited.

## L1-L9 Architecture Layer Audit

This layer audit is separate from the serving-path audit above. Its purpose is
to verify that storage did not lose important old-storage mechanics while
preserving the L1-L9 architecture design in
`docs/architecture/storage/`. Missing mechanics should be assigned to the
existing layer that owns them. The audit must not create new architecture layers
or benchmark-only bypasses.

Layer audit todo list:

- [x] L1 Backend IO.
- [x] L2 Object Layout.
- [x] L3 Durable Format / Codec.
- [x] L4 Log / Manifest / Snapshot Services.
- [x] L5 Table Runtime.
- [x] L6 Branch-Isolated LSM Runtime.
- [x] L7 Commit Runtime.
- [x] L8 Lifecycle / Recovery / Maintenance.
- [x] L9 Storage API Boundary.
- [x] Final cross-layer parity matrix.

### L1. Backend IO

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l1-backend-io.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/engine/src/database/open.rs`: local directory creation, `.lock`
  file, and `fs2` single-writer exclusion.
- `crates/storage/src/durability/layout.rs`: filesystem-shaped database
  layout, including WAL, segments, snapshots, and MANIFEST paths.
- `crates/storage/src/durability/wal/writer.rs` and
  `crates/storage/src/durability/format/wal_record.rs`: WAL segment file
  creation, append, sync, and parent-directory fsync on segment creation and
  rotation.
- `crates/storage/src/manifest.rs`,
  `crates/storage/src/segment_builder.rs`,
  `crates/storage/src/durability/disk_snapshot/writer.rs`, and
  `crates/storage/src/quarantine.rs`: repeated temp-write, file sync, rename,
  and parent-directory sync publish sequences.
- `crates/storage/src/durability/checkpoint_runtime.rs` and
  `crates/storage/src/segmented/quarantine_protocol.rs`: durable cleanup paths
  where deletes or cross-directory moves are followed by parent-directory sync.

Storage evidence:

- `crates/storage/src/backend/mod.rs`: object-first `Backend` contract,
  capability vocabulary, ranges, metadata, append facts, publish facts, writer
  guard, and unsupported defaults.
- `crates/storage/src/backend/memory.rs`: cache-mode memory backend with
  read, range read, write, delete, list, metadata, and non-durable publish.
- `crates/storage/src/backend/local_fs.rs`: local filesystem backend with
  object-name to file mapping, symlink rejection, append, object sync, writer
  lock, durable publish through temp file, temp sync, final publish, and parent
  sync, plus publish fault injection.
- `crates/storage/src/backend/conformance.rs`: shared basic object
  backend conformance tests and storage-mode capability validation.
- `crates/storage/src/config/mode.rs` and
  `crates/storage/src/lifecycle/capability.rs`: cache, durable local, and
  object-durable-candidate capability validation.
- `crates/storage/src/api/backend.rs` and
  `crates/storage/src/api/options.rs`: public opaque backend handle,
  explicit cache mode, explicit durable-local mode, and V1 rejection of
  unsupported object/distributed candidate modes.

Confirmed parity:

- Storage has an L1 backend contract. Higher layers can move opaque bytes
  by `ObjectName` instead of by local paths.
- Cache mode is visibly non-durable. `MemoryBackend` satisfies cache
  requirements and rejects durable publish modes.
- Durable local mode has an L1 local filesystem backend. On Unix it advertises
  durable publish, durable sync, append, metadata, range reads, listing, and a
  single-writer lock.
- The old temp-write, sync, rename/link, parent-sync publish sequence has been
  pulled down into L1 `LocalFsBackend::publish_object`.
- Publish failures are classified by visibility/durability window:
  `FailedBeforeVisibility`, `VisibilityUnknown`, and
  `VisibleDurabilityUnconfirmed`.
- The single-writer guard moved from engine open code into the backend layer,
  with the documented L1/L2 bootstrap exception for the reserved writer-lock
  object.
- A production-code scan shows direct `std::fs` usage is limited to
  `crates/storage/src/backend/local_fs.rs`. Other production references
  are path-shaped API convenience or comments, not lower-layer file IO.
- Object durable and distributed modes are not silently accepted. They are
  capability-gated internally and rejected at the public V1 API boundary.

Intentional architecture changes, not gaps:

- Storage is object-first instead of path-first. The local filesystem
  backend maps object names to files internally; higher layers should not know
  those paths.
- `LocalFsBackend` does not implement conditional create/update fences. That is
  acceptable for V1 because object-durable mode is not a production mode.
- Follower-state paths from old `DatabaseLayout` are not restored. Follower mode
  is explicitly excluded from storage V1.

Gaps to fill:

1. Durable deletion lacks an L1 durability outcome.
   - Old storage sometimes followed cleanup with parent-directory sync, for
     example snapshot pruning in
     `crates/storage/src/durability/checkpoint_runtime.rs` and quarantine
     cross-directory movement in
     `crates/storage/src/segmented/quarantine_protocol.rs`.
   - Storage services delete WAL, snapshot, sidecar, and quarantine
     objects through `Backend::delete_object`, but L1 currently has no
     durable-delete operation, namespace sync operation, or delete outcome that
     can distinguish "deleted and durably removed" from "removed but parent
     durability unconfirmed."
   - Owner: L1 contract plus L4 service use.
   - Exit gate: add a durable deletion or namespace-sync contract, then prove
     WAL truncation, snapshot pruning, sidecar cleanup, and quarantine purge do
     not leak filesystem-specific sync logic above L1.

2. Durable-local conformance is split across local tests instead of a full L1
   suite.
   - `backend/conformance.rs` currently proves the basic object contract and
     mode validation.
   - `backend/local_fs.rs` has strong local tests for publish, append, sync,
     writer locks, symlink rejection, and publish fault windows, but those are
     not yet a reusable durable-backend conformance suite.
   - Owner: L1.
   - Exit gate: add a durable backend conformance suite that covers append,
     sync, durable publish create/replace, publish fault classification,
     writer-lock exclusion, durable delete/namespace sync once defined, and
     unsupported durable operations on memory/cache backends.

3. CI does not yet enforce the L1 IO boundary.
   - The current source scan is clean for production `std::fs` usage outside
     `backend/local_fs.rs`, but this should become a regression guard.
   - Owner: L1/cross-cutting proof.
   - Exit gate: add a source-level test or lint that fails if production
     storage code outside the local filesystem backend starts using
     `std::fs`, `std::fs::File`, `OpenOptions`, direct rename, direct
     directory sync, or direct deletion.

L1 conclusion:

- The L1 architecture is not the source of the current point-read or scan
  performance regression.
- The core old filesystem durability primitive was preserved and placed in the
  correct layer.
- The remaining L1 work is targeted hardening: durable deletion semantics,
  reusable durable conformance, and a guard against higher layers reintroducing
  direct filesystem IO.

### L2. Object Layout

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l2-object-layout.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/storage/src/durability/layout.rs`: old filesystem-shaped database
  layout, including `wal/`, `segments/`, `snapshots/`, `MANIFEST`,
  `follower_state.json`, and `follower_audit.log`.
- `crates/storage/src/durability/wal/mod.rs` and
  `crates/storage/src/durability/format/wal_record.rs`: old WAL segment naming
  evidence.
- `crates/storage/src/durability/format/snapshot.rs`: old snapshot file naming
  evidence.
- `crates/storage/src/manifest.rs`: old manifest path and publish evidence.
- `crates/storage/src/quarantine.rs` and
  `crates/storage/src/segmented/quarantine_protocol.rs`: old quarantine object
  and manifest naming evidence.

Storage evidence:

- `crates/storage/src/object/mod.rs`: validated `ObjectName` and
  `ObjectPrefix` types.
- `crates/storage/src/layout/mod.rs`: canonical object families and
  constructors for manifest, WAL, WAL metadata, tables, snapshots, temporary
  objects, quarantine, locks, and database metadata.
- `crates/storage/src/layout/tests.rs`: constructor, prefix, ordering,
  invalid-component, reserved-family, and old-name absence tests.
- `crates/storage/src/service/wal.rs`,
  `crates/storage/src/service/manifest.rs`,
  `crates/storage/src/service/snapshot/`,
  `crates/storage/src/service/quarantine.rs`,
  `crates/storage/src/service/quarantine_manifest.rs`,
  `crates/storage/src/lifecycle/durable.rs`, and
  `crates/storage/src/service/sidecar.rs`: service-layer use of L2
  constructors for storage objects.
- `crates/storage/src/lifecycle/table_reachability.rs`: lifecycle code
  that still parses table object names with raw string checks.

Confirmed parity:

- Storage has a real L2 object-layout layer. Upper layers can work in
  validated `ObjectName` and `ObjectPrefix` values instead of local filesystem
  paths.
- The canonical reserved families exist: `manifest/`, `wal/`, `tables/`,
  `snapshots/`, `tmp/`, `quarantine/`, `locks/`, and `meta/`.
- Constructors exist for the documented object families:
  `manifest/current`, WAL segments, WAL metadata sidecars, branch table
  manifests, branch table objects, snapshots, temporary objects, quarantine
  objects, writer lock, and database metadata.
- WAL segment IDs and snapshot IDs use fixed-width lowercase hex. Table levels
  use `l0000`-style fixed-width level components, and levels above `9999` are
  rejected.
- Object names reject path traversal, empty components, absolute paths,
  platform path separators, invalid bytes, and trailing slash names before L1
  maps them to backend paths or keys.
- Layout tests explicitly reject old target-absent names such as `MANIFEST`,
  `wal-`, `snap-`, `segments.manifest`, `quarantine.manifest`,
  `__quarantine__`, `follower_state`, and `follower_audit`.
- Most storage services use `ObjectLayout` constructors rather than
  formatting object paths locally.
- Local filesystem escaping remains an L1 responsibility. L2 provides validated
  object names; `LocalFsBackend` owns object-name to path mapping.

Intentional architecture changes, not gaps:

- Storage should not preserve old filesystem filenames. Old names are
  evidence for mechanics, not binding target object names.
- Follower-state object names are intentionally absent because follower mode is
  not a V1 storage product path.
- `tmp/` is a reserved L2 object family, but backend-private publish temp files
  inside `LocalFsBackend` do not need to become visible storage objects.
- Manifest history is intentionally not exposed through object names for V1;
  `manifest/current` is the database-level durable manifest location.
- Object-store hot-prefix partitioning remains deferred. It should be handled
  as an object-store capability/tuning decision, not retrofitted into durable
  local V1 naming.

Gaps to fill:

1. L2 documentation has drifted behind implemented manifest objects.
   - `ObjectLayout` now constructs `manifest/branch-catalog` and
     `manifest/pending-releases`.
   - `docs/architecture/storage/l2-object-layout.md` still lists only
     `manifest/current` in the implemented canonical layout block.
   - Owner: L2 docs plus L3/L4 manifest-format references.
   - Exit gate: update the L2 architecture doc and any storage format spec that
     enumerates manifest-family objects, and add explicit layout tests for
     `manifest/branch-catalog` and `manifest/pending-releases`.

2. Table object shape parsing leaks above L2.
   - `crates/storage/src/lifecycle/table_reachability.rs` classifies table
     objects with raw `starts_with("tables/")`, `ends_with("/manifest")`, and
     slash-count checks.
   - That code is currently correct for the present layout, but it duplicates
     L2 knowledge in L8 retention logic.
   - Owner: L2 helper API, consumed by L8 lifecycle.
   - Exit gate: add L2-owned table object classification or parser helpers,
     then replace lifecycle raw string parsing with those helpers.

3. CI does not yet enforce the L2 naming boundary.
   - The current source scan is mostly clean: production services generally use
     `ObjectLayout`, while decoding code uses `ObjectName::new` to validate
     persisted object names.
   - There is no regression guard preventing a future service from formatting
     `wal/...`, `tables/...`, or `snapshots/...` directly.
   - Owner: L2/cross-cutting proof.
   - Exit gate: add a source-level test or lint that allows raw `ObjectName`
     decoding/validation in format and backend tests, but fails if production
     service/lifecycle code constructs canonical storage objects without
     `ObjectLayout`.

4. `tmp/` namespace semantics need an explicit V1 decision.
   - The namespace is reserved and tested, but current production temp files
     used for durable publish are backend-private L1 paths, not L2 temporary
     objects.
   - That is acceptable if `tmp/` is reserved only for future object-visible
     operations, but the architecture doc should say so directly.
   - Owner: L2 with L4/L8 service policy.
   - Exit gate: either add a documented user of `tmp/` in a durable service, or
     document that V1 reserves the namespace while L1 publish temps remain
     backend-private.

L2 conclusion:

- The L2 architecture is not the source of the current point-read or scan
  performance regression.
- The old path-building mechanics were replaced by a cleaner object-name layer
  without losing the essential namespace invariants.
- The remaining L2 work is boundary hardening: sync docs to implementation,
  move table-name parsing back into L2 helpers, add a source guard against raw
  canonical object construction, and clarify the reserved `tmp/` namespace.

### L3. Durable Format / Codec

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l3-durable-format-codec.md`
- `docs/spec/strata-storage-format-v1.md`
- `docs/architecture/storage/storage-space-id-registry.md`

Old-storage evidence:

- `crates/storage/src/durability/format/wal_record.rs`: old WAL record bytes,
  WAL record versions, and CRC behavior.
- `crates/storage/src/durability/format/manifest.rs`: old database manifest
  bytes.
- `crates/storage/src/durability/format/snapshot.rs`: old snapshot container
  and section-envelope bytes.
- `crates/storage/src/durability/format/segment_meta.rs` and
  `crates/storage/src/durability/format/watermark.rs`: old sidecar and
  watermark bytes.
- `crates/storage/src/durability/format/writeset.rs`,
  `crates/storage/src/durability/format/primitives.rs`,
  `crates/storage/src/durability/format/primitive_tags.rs`, and
  `crates/storage/src/durability/payload.rs`: old primitive-shaped and
  MessagePack-shaped commit/snapshot payload evidence.
- `crates/storage/src/durability/codec/`: old identity and AES-GCM codec
  evidence.
- `crates/storage/src/key_encoding.rs`: old internal key ordering.
- `crates/storage/src/segment_builder.rs` and `crates/storage/src/segment.rs`:
  old immutable table bytes, block frames, compression, and corruption checks.

Storage evidence:

- `crates/storage/src/format/mod.rs`: centralized L3 format exports,
  V1 constants, and typed `FormatError`.
- `crates/storage/src/format/manifest.rs`: database manifest V1 codec.
- `crates/storage/src/format/wal.rs` and
  `crates/storage/src/format/wal/commit_payload.rs`: WAL segment header,
  WAL record envelope, WAL record, and row-native commit payload codecs.
- `crates/storage/src/format/snapshot.rs`: snapshot header, section
  envelope, materialized container, and borrowed section visitor.
- `crates/storage/src/format/storage_row.rs` and
  `crates/storage/src/format/key.rs`: storage-row and physical/internal
  key codecs.
- `crates/storage/src/format/table/`: immutable table header, footer,
  data block, index block, properties block, block frame, zstd, and artifact
  codecs.
- `crates/storage/src/format/table_manifest.rs`,
  `crates/storage/src/format/branch_catalog_manifest.rs`,
  `crates/storage/src/format/pending_releases_manifest.rs`,
  `crates/storage/src/format/quarantine.rs`,
  `crates/storage/src/format/segment_metadata.rs`, and
  `crates/storage/src/format/watermark.rs`: service metadata codecs.
- `crates/storage/src/format/tests.rs`,
  `crates/storage/src/format/table/golden_tests.rs`, and
  `crates/storage/testdata/goldens/storage-format-v1/`: golden vector
  tests and stored V1 fixtures.
- `crates/storage/src/format/fuzzing.rs`: testkit/fuzz routing for many
  byte decoders.
- `crates/storage/src/lifecycle/recovery.rs`: checkpoint row-section
  payload bytes currently encoded/decoded outside L3.
- `crates/storage/src/lifecycle/retained_history_extension.rs`: durable
  table-manifest extension payload bytes currently encoded/decoded outside L3.

Confirmed parity:

- Storage has a centralized durable-format layer. WAL, manifest, snapshot,
  table, row, key, sidecar, watermark, quarantine, and table-manifest codecs
  are no longer scattered across path/service/runtime code the way old storage
  was.
- Stable storage byte formats start at V1, and pre-launch development
  versions are rejected instead of treated as compatibility inputs.
- `FormatError` gives storage-owned typed failures for insufficient bytes,
  invalid magic/version, pre-V1 formats, future formats, checksum mismatch,
  unsupported compression, invalid lengths/tags/flags, tombstone payload
  violations, invalid storage-space IDs, and trailing data.
- WAL segment headers preserve explicit magic, segment number, database id, and
  CRC. WAL records preserve self-delimiting length fields and CRC validation.
- WAL commit payloads are storage-row-native. Storage did not reintroduce
  old primitive writesets or MessagePack transaction payloads as the durable
  storage contract.
- Snapshot containers preserve the old storage-owned container mechanics:
  `SNAP` magic, fixed header, codec id, length-delimited section envelopes, and
  footer CRC. Primitive snapshot DTOs are intentionally not storage-owned V1
  payloads.
- Storage-row bytes carry physical key, commit version, commit timestamp,
  expiry timestamp, tombstone marker, row flags, and value bytes.
- Internal key encoding preserves the old asymptotic ordering property:
  physical key ascending, commit version descending for the same physical key.
- Immutable table bytes moved into L3 ownership through `format/table/`.
  Readers validate table CRC, block CRCs, block ordering, offsets, nested
  row/key consistency, compression codec, and trailing bytes.
- Golden fixtures exist for core storage-format V1 objects, including manifest,
  WAL, row, key, snapshot, watermark, table, table manifest, quarantine,
  segment metadata, branch catalog, and pending releases.
- Strict corruption tests exist for many critical codecs, including WAL,
  manifest, snapshot, table artifacts, table manifest, storage rows, sidecars,
  watermark, and quarantine inventory.

Intentional architecture changes, not gaps:

- Old durable byte versions are not preserved as compatibility formats.
  Storage is allowed to reject old development databases because Strata is
  still pre-launch.
- The old table format (`STRAKV`/version 7), old manifest path format, and old
  primitive snapshot payloads are evidence only, not required V1 inputs.
- Primitive product semantics remain outside storage. Storage owns row, key,
  table, WAL, manifest, and snapshot mechanics; engine owns JSON, event,
  vector, graph, search, and inference meaning.
- Identity is the required V1 codec. AES-GCM remains old implementation
  evidence until encryption configuration and key management are productized.
- Optional sidecars may be softer at the service layer, but L3 still reports
  corrupt sidecar bytes precisely.

Gaps to fill:

1. Checkpoint row-section payload bytes live in L8 recovery code.
   - `crates/storage/src/lifecycle/recovery.rs` defines
     `SNAPSHOT_ROW_SECTION_KIND`, `SNAPSHOT_ROWS_MAGIC`,
     `SNAPSHOT_ROWS_VERSION`, `encode_checkpoint_row_section`, and
     `decode_checkpoint_row_payload`.
   - Those are durable bytes inside snapshot sections, so L3 should own the
     codec and the spec should describe it. L8 should own when the section is
     written, which sections are installable, and how decoded rows are routed.
   - Owner: L3 codec, consumed by L8 recovery.
   - Exit gate: move row-section encode/decode into `format/snapshot` or a
     dedicated L3 snapshot-row module, add strict/golden/fuzz tests, and update
     `docs/spec/strata-storage-format-v1.md` section 13.

2. Retained-history extension payload bytes live in L8 lifecycle code.
   - `crates/storage/src/lifecycle/retained_history_extension.rs`
     documents and encodes a 24-byte durable table-manifest extension payload
     for `storage.retained_history`.
   - The semantic decision belongs to lifecycle, but the extension payload
     bytes are a durable L3 format.
   - Owner: L3 extension payload codec with L8 semantic owner.
   - Exit gate: move the payload codec into `format/`, keep L8 as the semantic
     consumer/producer, add strict decode tests for length, flag, and reserved
     bytes, add a golden vector, and specify the extension in the format spec.

3. Manifest-family metadata formats are implemented but under-specified and
   under-wired in golden tests.
   - `format/branch_catalog_manifest.rs` and
     `format/pending_releases_manifest.rs` have V1 codecs and golden fixture
     files under `crates/storage/testdata/goldens/storage-format-v1/`.
   - `docs/spec/strata-storage-format-v1.md` does not describe these formats,
     and `format/tests.rs` does not assert those golden fixtures in the normal
     test suite.
   - Owner: L3 spec/golden coverage plus L4 manifest services.
   - Exit gate: add spec sections and default golden assertions for branch
     catalog and pending releases manifests.

4. Fuzz routing is incomplete for the full public decoder surface.
   - `format/fuzzing.rs` routes key, manifest, quarantine, segment metadata,
     snapshot envelope, storage row, table artifact, table block, table
     manifest, WAL commit payload, WAL record, WAL segment header, and
     watermark.
   - It does not yet route branch catalog manifests, pending releases
     manifests, retained-history extension payloads, or checkpoint row-section
     payloads.
   - Owner: L3 test/fuzz harness.
   - Exit gate: add routing and seeded corpus entries for every L3 decoder,
     then wire these into the documented fuzz/testkit harness.

5. Codec behavior is identity-only and service-local.
   - That is acceptable for V1, but the implementation currently keeps the WAL
     identity codec boundary inside `service/wal.rs` rather than a reusable L3
     codec abstraction.
   - Owner: L3 codec boundary with L4 service application.
   - Exit gate: either document that V1 identity codec is represented only by
     exact codec-id validation plus no-op service application, or add a small
     L3 codec API so WAL/snapshot/table services do not grow separate codec
     switches later.

L3 conclusion:

- L3 is substantially stronger than old storage. Most durable bytes are now in
  one explicit format layer with V1 versions, strict decoders, CRCs, and
  golden fixtures.
- L3 is not the source of the current point-read or scan fanout regression.
  Its gaps are correctness, compatibility, and boundary-hardening issues, not
  the known serving-path performance issue.
- The important next L3 work is to pull the remaining durable payload codecs
  out of lifecycle code, finish spec/golden/fuzz coverage for every implemented
  format, and keep the codec boundary from fragmenting across services.

### L4. Log / Manifest / Snapshot Services

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l4-log-manifest-snapshot-services.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/storage/src/durability/wal/mod.rs`,
  `crates/storage/src/durability/wal/writer.rs`,
  `crates/storage/src/durability/wal/reader.rs`,
  `crates/storage/src/durability/wal/config.rs`, and
  `crates/storage/src/durability/wal/mode.rs`: old WAL segment creation,
  append, sync policy, replay, tail handling, and segment lifecycle.
- `crates/storage/src/durability/format/wal_record.rs`,
  `crates/storage/src/durability/format/segment_meta.rs`,
  `crates/storage/src/durability/payload.rs`, and
  `crates/storage/src/durability/format/watermark.rs`: old WAL and sidecar
  service bytes consumed by L4-style services.
- `crates/storage/src/durability/format/manifest.rs`,
  `crates/storage/src/manifest.rs`, and
  `crates/storage/src/durability/commit_adapter.rs`: old manifest loading,
  durable publication, and commit-visible durability boundary.
- `crates/storage/src/durability/checkpoint_runtime.rs`,
  `crates/storage/src/durability/disk_snapshot/mod.rs`,
  `crates/storage/src/durability/disk_snapshot/writer.rs`,
  `crates/storage/src/durability/disk_snapshot/reader.rs`,
  `crates/storage/src/durability/disk_snapshot/checkpoint.rs`, and
  `crates/storage/src/durability/format/snapshot.rs`: old checkpoint and
  snapshot service ordering.
- `crates/storage/src/segment_builder.rs`,
  `crates/storage/src/segmented/mod.rs`,
  `crates/storage/src/segmented/compaction.rs`, and
  `crates/storage/src/segmented/tests/publish_failures.rs`: old immutable
  table publication and publish-failure behavior.
- `crates/storage/src/quarantine.rs` and
  `crates/storage/src/segmented/quarantine_protocol.rs`: old quarantine
  object movement, inventory, and cleanup mechanics.

Storage evidence:

- `crates/storage/src/service/mod.rs`: L4 service module boundary and
  exports.
- `crates/storage/src/service/publish.rs`: centralized durable and
  non-durable object publication service.
- `crates/storage/src/service/wal.rs` and
  `crates/storage/src/service/wal/tests/`: WAL service open, append,
  sync, read, repair, rotation, retention deletion, partial-tail tests, and
  retention reopen tests.
- `crates/storage/src/service/manifest.rs`: database, table, branch
  catalog, and pending-release manifest services.
- `crates/storage/src/service/snapshot.rs` and
  `crates/storage/src/service/snapshot/`: snapshot publish, load, list,
  borrowed section visitor, fault-window tests, and prune mechanics.
- `crates/storage/src/service/checkpoint.rs`: mechanical checkpoint
  sequencing across manifest active-WAL publication, snapshot publication, and
  final manifest watermark publication.
- `crates/storage/src/service/table.rs`: durable immutable table object
  publication, inventory listing, reader opening, and exact-byte validation.
- `crates/storage/src/service/sidecar.rs`: optional WAL segment metadata
  sidecar publish, load, corrupt-state reporting, and deletion reports.
- `crates/storage/src/service/quarantine.rs`: quarantine inventory load,
  publish, reconciliation, object quarantine, and purge mechanics.
- `crates/storage/src/service/cache_mode_absence_tests.rs`: cache-mode
  absence tests proving durable services reject cache backends before mutating
  durable object families.
- `crates/storage/src/lifecycle/durable/bootstrap.rs`,
  `crates/storage/src/lifecycle/durable/maintenance.rs`,
  `crates/storage/src/lifecycle/recovery.rs`,
  `crates/storage/src/lifecycle/checkpoint.rs`,
  `crates/storage/src/lifecycle/table_manifest.rs`, and
  `crates/storage/src/lifecycle/wal_growth.rs`: higher-layer consumers
  of L4 services.

Confirmed parity:

- Storage has a real L4 service layer. Durable services consume L1
  backend IO, L2 object names, and L3 format codecs instead of reaching around
  them.
- `ObjectPublisher` centralizes durable create/replace and non-durable replace
  publication. Durable publication requires `DurablePublish` and
  `DurableSync`, and service callers validate object, byte count, and durable
  outcome metadata.
- Manifest mechanics are service-owned. Storage has database manifest,
  table manifest, branch catalog, and pending-release services with typed load,
  create, replace, publish, codec, role, and publish-metadata errors.
- WAL mechanics are service-owned. `WalService` opens or creates active
  segments, validates segment size, encodes record frames, appends through the
  backend, tracks dirty bytes/records, forces durability for
  `DurabilityPolicy::Always`, rotates after syncing the old segment, replays
  complete segments, repairs partial latest tails through durable replace, and
  protects the active segment during retention deletion.
- WAL reads preserve the old complete-prefix behavior: complete non-latest
  segments must decode cleanly, while the latest segment can report a
  truncation fact for repair.
- Snapshot mechanics are service-owned. Snapshot publish uses durable create,
  load validates snapshot id, database id, codec id, section envelopes, and
  byte counts, listing is object-layout based, and pruning protects the live
  snapshot and newest retained snapshots.
- Checkpoint sequencing preserves the old durability ordering:
  publish the manifest fact for the active WAL segment, publish the snapshot,
  then publish the final manifest watermark. Tests distinguish orphan snapshot
  windows from final manifest uncertainty.
- Immutable table object publication goes through L4. `TableObjectService`
  decodes caller-supplied table bytes before publication, derives table facts,
  publishes with durable create, validates publish metadata, and opens readers
  through object-backed table sources.
- Sidecars are modeled as optional accelerators, not authoritative state.
  Missing and corrupt sidecars are explicit load states so recovery can fall
  back to WAL bytes.
- Quarantine inventory and object movement are service-owned, with identity,
  codec, gate, source, inventory-token, and publish errors reported as typed
  storage service errors.
- Cache mode is kept out of durable L4 services. The cache-mode absence tests
  prove durable manifest, table, WAL, sidecar, snapshot, checkpoint, and
  quarantine mutation paths reject cache backends before mutating durable
  object families, while non-durable cache publication remains available for
  cache objects.
- Publish-failure windows are explicitly tested through service and checkpoint
  paths, including visible-but-durability-unconfirmed and visibility-unknown
  cases.

Intentional architecture changes, not gaps:

- Storage uses object publication services instead of old local
  path-specific temp-file and rename sequences. The local filesystem publish
  mechanics now belong to L1.
- Cache mode intentionally has no durable WAL, manifests, snapshots, table
  objects, or quarantine mutation. L7 owns the non-durable runtime path for
  cache mode.
- L4 does not decide checkpoint cadence, compaction selection, retention
  policy, branch materialization, or recovery orchestration. Those remain L8
  and branch-runtime responsibilities.
- Sidecars remain optional. They can be published and deleted by L4, but
  authoritative recovery must still be correct without them.
- Object-store conditional fencing is not implemented for V1 because
  object-durable and distributed modes are rejected at the public API boundary.
- L4 currently applies the V1 identity codec boundary locally in services. That
  is acceptable while identity is the only supported storage codec, but it
  should not grow separate encryption/compression switches per service later.

Gaps to fill:

1. Durable delete and prune results inherit the L1 delete gap.
   - Old storage had cleanup paths where deletes or moves were followed by
     parent-directory durability work, for example snapshot pruning in
     `crates/storage/src/durability/checkpoint_runtime.rs` and quarantine
     movement in
     `crates/storage/src/segmented/quarantine_protocol.rs`.
   - Storage WAL retention uses
     `crates/storage/src/service/wal.rs::delete_covered_segments`,
     snapshot pruning uses
     `crates/storage/src/service/snapshot/listing.rs::prune_snapshots`,
     sidecar cleanup uses `crates/storage/src/service/sidecar.rs`, and
     quarantine purge uses `crates/storage/src/service/quarantine.rs`.
     These paths call `Backend::delete_object`, which currently reports only
     success or backend failure.
   - Owner: L1 delete contract plus L4 cleanup services.
   - Exit gate: add a durable-delete or namespace-sync outcome at L1, then
     plumb typed L4 reports that distinguish deleted, already absent, failed,
     and deletion-durability-uncertain cleanup.

2. L4 documentation has drifted behind implemented manifest services.
   - `docs/architecture/storage/l4-log-manifest-snapshot-services.md`
     describes database, branch/table, and quarantine manifest families.
   - Storage now also has explicit branch catalog and pending-release
     manifest services in `crates/storage/src/service/manifest.rs`.
   - Owner: L4 docs, coordinated with L2 object layout and L3 manifest-format
     docs.
   - Exit gate: update the L4 architecture doc so every implemented manifest
     service has a documented role, owner, publication rule, recovery use, and
     cache-mode absence expectation.

3. L4 conformance is broad but still mostly service-local.
   - WAL, snapshot, manifest, table object, sidecar, quarantine, checkpoint,
     cache-mode absence, and lifecycle capability tests exist.
   - There is no single reusable L4 service-conformance suite that can be run
     against every durable backend implementation once object-durable support
     is added.
   - Owner: L4 test harness, backed by L1 backend conformance.
   - Exit gate: add a durable-service conformance suite covering manifest
     create/replace/load, WAL append/read/repair/delete, snapshot
     publish/load/list/prune, table publish/open, sidecar present/missing/
     corrupt, quarantine inventory/purge, publish-failure windows, and
     cache-mode rejection.

4. WAL durability policy parity should remain under test as modes evolve.
   - Storage preserves the important V1 distinction:
     `DurabilityPolicy::Always` forces WAL sync during append, while standard
     mode tracks dirty WAL state and close/maintenance can force durability.
   - Old storage also had explicit WAL mode/config files in
     `crates/storage/src/durability/wal/config.rs` and
     `crates/storage/src/durability/wal/mode.rs`. If background sync or
     future policies return, L4 needs tests proving the policy changes only
     durability timing, not visibility or replay semantics.
   - Owner: L4 WAL service plus L8 lifecycle scheduler.
   - Exit gate: keep policy-specific WAL append/close/recovery tests whenever
     a new durability policy is introduced.

5. Object publication fencing remains V1-deferred.
   - `ObjectPublisher` supports durable create and durable replace, but does
     not expose compare-and-swap or generation-fenced replace.
   - That is acceptable for durable-local V1 with a single-writer lock and for
     rejected object-durable modes, but it must be revisited before enabling
     object-durable or distributed storage.
   - Owner: L1/L4 object-durable candidate work.
   - Exit gate: before object-durable mode is accepted, define fenced publish
     semantics and prove manifest, checkpoint, table, snapshot, and quarantine
     services use them where required.

L4 conclusion:

- The old WAL, manifest, snapshot, immutable-table publication, sidecar, and
  quarantine mechanics were mostly preserved and assigned to the right
  storage layer.
- L4 is not the source of the current point-read fanout regression. It can
  affect large-scale stability through retention, checkpoint, recovery, and
  cleanup, but the known 10M serving-path issue sits above L4 in L5-L8 table,
  branch, and lifecycle mechanics.
- The main L4 follow-up is targeted hardening: durable cleanup semantics,
  documentation parity for all manifest services, reusable service conformance,
  policy-specific WAL tests as modes evolve, and future fenced publication for
  object-durable mode.

### L5. Table Runtime

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l5-table-runtime.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/storage/src/memtable.rs`: old mutable and frozen in-memory table
  mechanics, ordered internal-key storage, range/prefix iteration, commit-range
  facts, and frozen-memtable bloom checks for absent point reads.
- `crates/storage/src/key_encoding.rs`: old internal-key ordering evidence
  used by table-local and branch-local reads.
- `crates/storage/src/segment_builder.rs`: old immutable table builder,
  block construction, prefix-compressed entries, index/filter/property blocks,
  compression, output splitting, and crash-safe local publication evidence.
- `crates/storage/src/segment.rs`: old `KVSegment` reader, table properties,
  key/commit range facts, partitioned bloom filter, block/index lookup, block
  cache usage, point lookup, prefix/range iteration, `OwnedSegmentIter`, and
  `LevelSegmentIter`.
- `crates/storage/src/block_cache.rs`, `crates/storage/src/bloom.rs`, and
  `crates/storage/src/index.rs`: old block cache, bloom filter, and table
  index implementation evidence.
- `crates/storage/src/merge_iter.rs` and `crates/storage/src/seekable.rs`: old
  raw cursor/merge mechanics mixed with MVCC and inherited-layer wrappers.
- `crates/storage/src/compaction.rs` and
  `crates/storage/src/segmented/compaction.rs`: old table compaction iterator,
  tombstone/TTL/version-retention behavior, splitting builder use, and
  grandparent-overlap split predicate evidence.

Storage evidence:

- `crates/storage/src/table/mod.rs`: L5 module boundary and exports.
- `crates/storage/src/table/key.rs`: table internal-key bytes,
  physical-key prefix bytes, table rows, key bounds, range/prefix matching,
  strict ordering validation, and sorted-row helpers.
- `crates/storage/src/table/mutable.rs`: mutable and frozen table
  implementations backed by `BTreeMap`, append rollback snapshots, memory
  facts, ordered cursors, and physical-key seek helpers.
- `crates/storage/src/table/builder.rs`: immutable table builder,
  builder-limit validation, sorted unique row validation, artifact generation,
  and decoded fact validation.
- `crates/storage/src/table/reader.rs`: immutable table reader, table
  byte-source abstraction, source reads, metadata reads, exact lookup,
  physical-key seek, cursors, and bounded cursors.
- `crates/storage/src/table/cursor.rs`: raw table cursor trait,
  memory-table cursor, bounded cursor, and merge cursor with linear/heap paths.
- `crates/storage/src/table/cache.rs`: database-local table block cache
  and table-local bloom filter scaffolding.
- `crates/storage/src/table/compaction.rs`: generic table compactor with
  caller-supplied row policy, drop reasons, output splitting, output artifacts,
  and reports.
- `crates/storage/src/service/table.rs`: L4 table-object service that
  opens object-backed `ImmutableTableReader`s through the L5 byte-source
  boundary.
- `crates/storage/src/table/tests/` and
  `crates/storage/src/format/table/*_tests.rs`: table key, mutable,
  builder, reader, cursor, cache, bloom, compaction, format, golden, and
  corruption tests.

Confirmed parity:

- Storage has a distinct L5 table-runtime module. Table mechanics are no
  longer hidden inside branch state, durability code, or public API code.
- Mutable and frozen tables preserve the basic old ordered-table mechanics:
  rows are keyed by encoded internal key, insertion rejects duplicate internal
  keys, facts track approximate bytes and commit range, freeze preserves sorted
  rows, and cursors can seek by encoded table key.
- Table keys preserve the important ordering substrate: physical key bytes sort
  first, commit-version suffix sorts in the storage-owned internal-key order,
  and table code exposes physical-key prefix/range helpers without owning
  branch policy.
- Immutable table building is L5-owned. The builder validates non-empty,
  sorted, unique rows, enforces key/row/block/table limits before encoding,
  emits L3 table bytes, decodes the artifact back, and derives table runtime
  facts from decoded bytes.
- Immutable table reading is object-source capable. `ImmutableTableReader` can
  open from in-memory bytes or an arbitrary `TableByteSource`, and
  `TableObjectService` supplies an L4 object-backed source rather than letting
  L5 know object names, paths, or backend details.
- Raw table cursors exist. Storage has memory cursors, immutable-table
  cursors, bounded cursors, and a merge cursor with a small-source linear path
  and a heap path above `MERGE_HEAP_THRESHOLD`.
- Raw range and prefix bounds are represented at L5. `TableKeyBounds` supports
  exact ranges, inclusive/exclusive ranges, byte prefixes, and physical-key
  ranges.
- L5 compaction is policy-injected. `TableCompactor` asks a caller-supplied
  `TableCompactionPolicy` whether to keep or drop each row and reports generic
  drop reasons such as older version, tombstone elision, and expiry. It does
  not hard-code branch, snapshot, event, or product policy.
- L5 table cache and bloom-filter scaffolding exists with deterministic tests,
  database-local cache instances, stable table identity keys, bounded capacity,
  eviction, resize, table invalidation, concurrency, and no-false-negative
  bloom tests.
- L5 has broad direct tests independent of engine primitives: key ordering,
  mutable/frozen behavior, builder/reader roundtrips, source read failures,
  cursor movement, range/prefix bounds, compression, corrupt table bytes, cache
  behavior, bloom behavior, compaction policy, output splitting, and
  deterministic output identity.

Intentional architecture changes, not gaps:

- Storage table bytes are the formal L3 V1 format, not the old
  `KVSegment` file format. Old segment bytes are evidence for mechanics, not a
  compatibility target.
- L5 treats branch id, storage space id, timestamps, TTL, tombstones, and
  commit versions as row/key metadata. Branch visibility, fork gates,
  tombstone safety, TTL retention, and snapshot floors belong above L5.
- Durable publication moved out of table building. L5 produces table artifacts
  and facts; L4 publishes them; L6 installs them into branch reachability.
- The old process-global block cache is intentionally not restored. The target
  storage owner is database-local cache state with explicit budget.
- Old local-path cache keys are not restored. L5 cache identity should come
  from stable table identity or object facts supplied by upper layers.

Gaps to fill:

1. Immutable table readers eagerly materialize all rows.
   - Old `KVSegment` in `crates/storage/src/segment.rs` opened table metadata,
     bloom/index/filter state, and then served point lookups through bloom,
     index lookup, block cache, and block-local scans.
   - Storage `ImmutableTableReader` stores `rows: Vec<TableRow>`.
     `open_source` reads metadata, then `read_rows_from_metadata` walks every
     data-block entry and decodes all rows into memory before the reader can
     serve lookups.
   - This preserves correctness and simplifies current branch code, but it is
     not old scalable table-reader parity. It increases open/recovery memory
     pressure and prevents table-local block cache and bloom accelerators from
     doing useful work.
   - Owner: L5 reader.
   - Exit gate: replace eager full-row materialization with a metadata-first
     reader that can binary-search index entries, read/cache only needed data
     blocks, and still expose a streaming cursor for scans and compaction.

2. Table-local bloom filters and block cache are not integrated into reads.
   - `crates/storage/src/table/cache.rs` defines `TableBlockCache` and
     `TableBloomFilter`, and tests prove their standalone behavior.
   - Production references show the reader and service paths do not use
     `TableBlockCache` or `TableBloomFilter` for point lookups or scans.
   - Old storage used frozen-memtable bloom checks in
     `crates/storage/src/memtable.rs` and segment partitioned bloom checks in
     `crates/storage/src/segment.rs` to avoid unnecessary source work.
   - Owner: L5 reader/cache integration, consumed by L6 source planning.
   - Exit gate: wire optional table-local bloom/filter data and block cache
     into point lookup and cursor paths, then add counters proving absent
     table probes can be rejected without data-block reads.

3. L5 does not yet expose old streaming table iteration parity.
   - Old storage had `SegmentIter`, `OwnedSegmentIter`, and
     `LevelSegmentIter` in `crates/storage/src/segment.rs`, plus seekable
     wrappers in `crates/storage/src/seekable.rs`.
   - Storage has raw cursors, but immutable cursors currently iterate an
     already-materialized row vector. They do not lazily load table blocks or
     hold the "current block plus index position" state used by old segment
     iterators.
   - Owner: L5 cursor/reader.
   - Exit gate: add block-backed immutable cursors with seek, advance,
     range/prefix bounds, corruption propagation, and tests proving scans do
     not read unrelated blocks.

4. L5 compaction is eager instead of streaming.
   - Old `CompactionIterator` streamed a sorted merge and
     `SplittingSegmentBuilder` emitted outputs as the iterator advanced.
   - Storage `TableCompactionSource::from_cursor` collects all rows from
     each cursor, `merged_rows` creates one merged vector, and `compact_tables`
     sorts that full vector before applying the caller policy.
   - This is clean scaffolding for correctness, but it is not old memory
     behavior and will scale poorly for large compactions.
   - Owner: L5 compactor.
   - Exit gate: make compaction consume sorted cursors through a streaming
     merge cursor, apply the caller policy incrementally, and emit output
     artifacts without retaining all input rows at once.

5. Output splitting lacks caller-supplied overlap split constraints.
   - Old `compact_level` in `crates/storage/src/segmented/compaction.rs`
     supplied a grandparent-overlap split predicate to
     `SplittingSegmentBuilder::build_split_with_predicate`.
   - Storage L5 splitting currently uses target approximate bytes and
     avoids splitting a physical-key version group, but it does not expose a
     generic overlap-threshold predicate or split boundary hint supplied by L6.
   - Owner: L5 split API, consumed by L6 compaction planning.
   - Exit gate: add an optional caller-supplied split constraint so L6 can
     preserve non-overlap and grandparent-overlap behavior without embedding
     branch policy in L5.

6. Frozen table negative lookup acceleration is absent.
   - Old frozen memtables built a bloom filter lazily in
     `crates/storage/src/memtable.rs` and used it to skip absent point reads.
   - Storage frozen tables use ordered `BTreeMap` seek, which is correct,
     but they do not have a frozen-table bloom equivalent.
   - Owner: L5 mutable/frozen table runtime.
   - Exit gate: either document that BTreeMap seek is sufficient for V1 frozen
     tables, or add an optional frozen-table bloom with tests for tombstones,
     multiversion keys, and no false negatives.

7. L5 perf/conformance gates are not yet tied to old asymptotic behavior.
   - Existing L5 tests prove many correctness properties, but they do not
     assert table-open bytes read, blocks read per point lookup, cache hit/miss
     behavior in a real reader, or memory retained per table.
   - Owner: L5 tests and perf counters.
   - Exit gate: add L5 counters for metadata reads, data-block reads,
     decoded-row count, cache hits/misses, bloom rejections, cursor seeks, and
     compaction peak buffered rows; then add regression tests that compare
     small and large tables against expected asymptotic bounds.

L5 conclusion:

- Storage has the right L5 architecture boundary and substantial
  correctness scaffolding: sorted mutable/frozen tables, table artifacts,
  object-backed readers, raw cursors, cache/bloom primitives, and
  policy-injected compaction.
- L5 is still not old runtime parity. The current reader and compactor are
  eager, row-vector based implementations. That is simpler and correct for V1
  scaffolding, but it leaves important old mechanics un-restored: block/index
  lookup, bloom rejection, block cache integration, lazy block-backed cursors,
  streaming compaction, and overlap-aware output splitting.
- The known 10M point-read source-fanout regression is primarily L6/L8 source
  planning and compaction scheduling, but L5 gaps will matter as table sizes
  grow. Fixing L6 fanout without restoring L5 lazy table mechanics would still
  leave storage exposed to memory and scan-throughput limits at larger
  scales.

### L6. Branch-Isolated LSM Runtime

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l6-branch-isolated-lsm-runtime.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/storage/src/segmented/mod.rs`: old branch state, active/frozen
  memtables, per-branch `SegmentVersion`, COW inherited layers,
  `get_versioned_from_branch`, snapshot-based read path, `StorageIterator`,
  `build_seekable_pipeline`, fork, materialization, range scans, branch
  clearing, and runtime branch lifecycle behavior.
- `crates/storage/src/segment.rs`: old `KVSegment`, `OwnedSegmentIter`,
  `LevelSegmentIter`, table key-range facts, point lookup, and lazy per-level
  scan behavior.
- `crates/storage/src/segmented/compaction.rs` and
  `crates/storage/src/compaction.rs`: old LSM compaction planning, level
  targets, overlap selection, retention, and split behavior consumed by branch
  state.
- `crates/storage/src/merge_iter.rs` and `crates/storage/src/seekable.rs`: old
  MVCC merge, tombstone/expiry filtering, inherited-row rewriting, and
  seekable iterator behavior used by branch reads.
- `crates/storage/src/segmented/tests/fork.rs`,
  `crates/storage/src/segmented/tests/materialize.rs`,
  `crates/storage/src/segmented/tests/leveled.rs`,
  `crates/storage/src/segmented/tests/flush.rs`,
  `crates/storage/src/segmented/tests/resurrection.rs`,
  `crates/storage/src/segmented/tests/concurrency.rs`,
  `crates/storage/src/segmented/tests/post_restart_branch.rs`, and
  `crates/storage/src/segmented/tests/publish_failures.rs`: old branch COW,
  materialization, level invariants, fork-frontier, recovery, and failure
  semantics.

Storage evidence:

- `crates/storage/src/branch/mod.rs`: L6 module boundary and exports.
- `crates/storage/src/branch/state.rs`: branch-local active, frozen,
  owned levels, inherited layers, facts, level installation, reachability
  snapshots, duplicate internal-key checks, and observed row facts.
- `crates/storage/src/branch/read.rs`: branch read bounds, inherited
  effective bounds, point reads, history, borrowed scan cursors, scan heap
  merge, inherited row/key rewriting, tombstone/TTL visibility, and source
  ordering.
- `crates/storage/src/branch/state/read_hooks.rs`: branch read-view
  capture, timestamp-to-version resolution, facts, and read-facing accessors.
- `crates/storage/src/branch/state/fork.rs`: empty-child fork and
  inherited-layer attachment.
- `crates/storage/src/branch/state/materialization.rs`: inherited-layer
  materialization, materialization handles, retry/recovery outcomes,
  replacement table installation, fork-version filtering, shadow checks, and
  reachability binding.
- `crates/storage/src/branch/state/compaction.rs`: branch-owned
  compaction planning, overlap selection, prepared output installation,
  pruning proof validation, replacement table handling, and stale-candidate
  checks.
- `crates/storage/src/branch/state/snapshot.rs`: snapshot row grouping,
  branch replacement/create policy, output table construction, and staged
  all-or-nothing branch install.
- `crates/storage/src/branch/pruning.rs`,
  `crates/storage/src/branch/facts.rs`, and
  `crates/storage/src/branch/state/manifest_recovery.rs`: branch
  compaction pruning proofs, shared table reachability, table references,
  retention safety, and table-manifest recovery.
- `crates/storage/src/branch/tests/`: branch identity, read view,
  immutable reads, compaction, row pruning, snapshot install, facts,
  inheritance, fork, materialization, and manifest-recovery tests.
- `crates/storage/src/lifecycle/flush.rs`,
  `crates/storage/src/lifecycle/compaction.rs`, and
  `crates/storage/src/lifecycle/branch_lifecycle.rs`: L8 consumers of
  L6 flush installation, compaction, materialization, fork, clear/delete, and
  reachability APIs.

Confirmed parity:

- Storage has a distinct L6 branch-runtime layer. Branch mechanics are no
  longer hidden in public API code, WAL services, table format code, or backend
  IO.
- `BranchLocalState` has the expected branch-local LSM state: active mutable
  table, frozen tables, branch-owned immutable levels, inherited layers,
  branch config, max commit version, timestamp facts, timestamp coverage, and
  put/tombstone counters.
- L0 and L1+ shape is modeled. `install_owned_table_at_level` inserts L0 at
  the front, nonzero levels are sorted by physical range, and install
  validation rejects overlapping nonzero-level tables.
- Read bounds and inherited-layer fork gates are explicit. `BranchReadBound`
  supports latest, version, and timestamp reads; inherited effective bounds cap
  reads by `fork_version`; materialized/unavailable layers are skipped or
  rejected according to status.
- Branch point reads, history, and scans preserve the core logical ordering:
  active, frozen, owned tables, then nearest inherited layer. Candidate
  selection sorts by commit version and source precedence, and tombstones/TTL
  are applied at L6 instead of L5.
- Storage has COW branch inheritance. Fork into an empty child captures
  source owned levels plus active inherited layers, records nearest-first
  inherited layer order, rejects self-fork and unavailable layers, and carries
  fork-version facts.
- Materialization exists and is structured around branch-owned state. It marks
  layers materializing, captures reachability before replacement, rewrites
  source rows into child keys, skips post-fork rows, detects exact duplicate
  replacement rows, rejects higher-precedence collisions, installs replacement
  tables, removes the inherited layer, and handles retry/recovery cases.
- Branch compaction has a real L6 plan/install boundary. It plans L0, L0 to
  L1, and nonzero-level compactions, selects overlapping target-level tables,
  validates stale candidates, delegates row merging and row policy to L5, then
  installs replacement output tables with branch-level invariant checks.
- Snapshot row install is represented at L6. Snapshot rows are grouped by
  branch, sorted by internal key, split into output L0 tables, staged before
  replacement, and committed all-or-nothing across branch state.
- Branch reachability and shared-table safety are explicit. L6 produces
  `BranchReachabilitySnapshot`s and `BranchTableRef`s, while pruning proofs can
  require shared-table registry and table-manifest coverage before dropping
  older rows, tombstones, or expired rows.
- Storage has broad L6 tests. The branch test suite covers identity,
  active/frozen state, read views, immutable reads, owned compaction, row
  pruning, snapshot install, inherited-layer validation, fork,
  materialization, retries, reachability, and manifest recovery.

Intentional architecture changes, not gaps:

- L6 does not own commit validation, version allocation, WAL-before-visible
  ordering, checkpoint cadence, background scheduling, or public mode
  selection. Those belong to L7, L8, and L9.
- L6 delegates table bytes, table-local index/cache/bloom behavior, and table
  compaction row streaming to L5.
- Durable table-object publication and table-manifest persistence are outside
  L6. L6 installs branch-local table descriptors and reachability facts; L4/L8
  persist and recover those facts.
- Runtime refcounts from the old engine are not directly restored as a branch
  data structure. Storage uses reachability snapshots, table refs, pinned
  reachability, table manifests, and retention proofs instead.
- Fork currently requires the source branch to have flushed active and frozen
  rows before inheritance capture. That is a stricter V1 architecture boundary
  than the old storage-level ephemeral fork behavior; L7/L8 should own any
  future quiesce-and-flush orchestration needed to make fork ergonomic.

Gaps to fill:

1. L1+ point-read source pruning is incomplete.
   - Old `get_versioned_from_branch` in `crates/storage/src/segmented/mod.rs`
     searched L0 linearly but used `point_lookup_level_preencoded` to binary
     search each non-overlapping L1+ level and probe at most one segment per
     level. Inherited layers used the same L0 linear plus L1+ binary-search
     pattern after branch-id key rewriting.
   - Storage `visible_point_candidates` in
     `crates/storage/src/branch/read.rs` now uses table-local
     `seek_physical_key`, which is better than full row filtering, but it
     still calls that seek on every owned table in every level and every
     inherited table.
   - This preserves correctness but loses the old level-range pruning
     asymptotic. At large scale, non-overlapping L1+ levels should behave like
     `O(levels * table_seek)`, not `O(table_count * table_seek)`.
   - Owner: L6 source planner, using L5 table facts/key ranges.
   - Exit gate: add a branch-level source planner that probes all L0 tables,
     binary-searches sorted L1+ table ranges, applies the same rule to each
     readable inherited layer after key rewrite, and adds counters proving
     point reads visit at most all active/frozen/L0 sources plus one L1+ table
     per level/layer.

2. Scan source planning creates one cursor per table instead of one lazy cursor
   per non-overlapping level.
   - Old `StorageIterator::build_seekable_pipeline` and
     `build_branch_merge_iter` in `crates/storage/src/segmented/mod.rs` created
     individual iterators for L0, but used `LevelSeekableIter` or
     `LevelSegmentIter` for L1+. Those iterators binary-searched to the first
     relevant segment and opened subsequent segments lazily.
   - Storage borrowed scans in `crates/storage/src/branch/read.rs`
     use `BranchScanCursor` and a heap merge, which is the right standard
     machinery direction, but `scan_cursors_for_sources` still pushes a
     bounded cursor for every owned table and every inherited table.
   - This is the likely scan-range-throughput gap at large table counts. A
     small bounded range should not pay setup and heap cost for every L1+
     table in the branch.
   - Owner: L6 scan source planner plus L5 level/table cursor support.
   - Exit gate: add a level cursor abstraction for sorted non-overlapping
     `BranchOwnedTable` levels, keep per-table cursors for L0, apply prefix/
     range overlap pruning before cursor creation, and prove setup cost scales
     with active/frozen/L0 plus level count rather than total table count.

3. Read-view capture is correct but not old pinned snapshot parity.
   - Old `BranchSnapshot` in `crates/storage/src/segmented/mod.rs` cloned
     `Arc` handles to the active memtable, frozen memtables, and an
     `ArcSwap`-pinned `SegmentVersion`, then released the branch map guard.
   - Storage `capture_read_view` in
     `crates/storage/src/branch/state/read_hooks.rs` clones the active
     table, frozen tables, owned levels, inherited layers, and branch facts.
     Because `BranchOwnedTable` contains `ImmutableTableReader` and current
     L5 readers contain `Vec<TableRow>`, this is much more expensive than the
     old pinned-superversion model.
   - Owner: L6 read-view shape, coordinated with L5 reader ownership.
   - Exit gate: make read views pin cheap immutable handles for active/frozen
     and table readers, then add counters proving read-view capture does not
     scale with total retained rows.

4. Timestamp-to-version resolution scans branch rows.
   - Old branch state tracked min/max timestamps and max applied version in
     atomics. Timestamp and timeline semantics were not implemented by walking
     every retained row on the read path.
   - Storage `resolve_timestamp_to_commit_version` walks active, frozen,
     owned, and inherited rows to find the best commit version at or before a
     timestamp.
   - This is correct for current scaffolding but not scalable for timestamp
     reads once retained history grows.
   - Owner: L6 timestamp facts, with L7 commit timeline input.
   - Exit gate: introduce a branch timeline index/facts surface populated by
     L7 commit application and L6 table install/recovery, then prove timestamp
     resolution is logarithmic or bounded by compact timeline state instead of
     retained row count.

5. Branch facts recomputation walks retained rows.
   - `BranchLocalState::facts` calls `observe_rows`, and inherited facts walk
     inherited table rows subject to fork-version filtering.
   - Old branch state kept hot facts such as max version, min/max timestamps,
     entry/deletion counts, level targets, and version snapshots incrementally.
   - Storage already stores some incremental fields, but the facts API
     still recomputes from rows in important places, including read-view
     capture.
   - Owner: L6 branch facts.
   - Exit gate: make branch facts incremental and recovery-derived, reserve
     full scans for validation/debug builds or explicit rebuilds, and add
     counters proving normal facts calls do not scan table rows.

6. Branch compaction source preparation is eager and row-vector based.
   - L6 planning correctly identifies L0, L0-to-L1, nonzero-level input, and
     overlap refs, but `compaction_sources` converts each table into
     `TableCompactionSource::from_rows(table.rows().to_vec())`.
   - The old compaction path streamed segment iterators and used splitting
     builders without loading all compaction input rows into one L6-owned
     vector.
   - This overlaps the L5 streaming-compaction gap, but L6 is the layer that
     should pass table refs/cursors and source metadata instead of cloning
     table rows.
   - Owner: L6 compaction source preparation plus L5 streaming compactor.
   - Exit gate: change L6 compaction preparation to pass sorted source cursors
     or table handles, and add peak-buffered-row counters for branch
     compaction.

7. Materialization is eager and fixed-chunk L0 output.
   - Storage materialization preserves semantics, but
     `collect_materialization_rows` scans the target inherited layer into a
     row vector, builds replacement artifacts with a fixed
     `MATERIALIZATION_ROWS_PER_OUTPUT_TABLE`, and installs them as child-owned
     L0 replacement tables.
   - Old materialization also had heavy work, but it was anchored in segment
     iterators and old builder mechanics. Storage should eventually share
     the same streaming and split-boundary mechanics used by branch
     compaction, rather than remaining a separate eager path.
   - Owner: L6 materialization plus L5 streaming builder/compactor.
   - Exit gate: materialization consumes level/table cursors with
     fork-version and shadow filters, emits replacement tables through the
     same streaming artifact path as compaction, and preserves current retry
     and reachability semantics.

8. Fork ergonomics are stricter than old storage-level behavior.
   - Old `fork_branch` could capture active/frozen/segment state under branch
     locking and used per-branch max-version facts to avoid allocated-but-not-
     applied version gaps.
   - Storage `fork_into_empty_child` rejects sources with active or
     frozen rows and requires at least one retained row. This is a clean V1
     L6 invariant, but it means higher layers must flush/quiesce before fork
     if the public API is expected to match old behavior.
   - Owner: L7/L8 fork orchestration, using L6 fork mechanics.
   - Exit gate: document the public fork contract at L9 and add L7/L8 tests
     proving fork either flushes/quiesces before capture or reports a typed
     precondition without losing old fork-frontier correctness.

9. L6 tests do not yet assert old asymptotic source counts.
   - Existing tests cover many logical semantics, but they do not pin
     branch-level point source counts, scan cursor source counts, read-view
     clone row counts, facts row scans, or compaction peak buffered rows.
   - Owner: L6 perf counters and differential tests.
   - Exit gate: add L6 regression tests with many L1+ tables and inherited
     layers, asserting source counts and rows visited against old-engine
     asymptotic expectations.

L6 conclusion:

- Storage has the right L6 boundary and preserved many high-value branch
  mechanics: branch-local LSM state, L0/L1+ invariants, fork-version
  inheritance, materialization, snapshot install, compaction planning,
  pruning proofs, reachability, and tests.
- The major missing old mechanics are not another architecture layer. They are
  L6 source-planning and pinned-view gaps inside the standard branch runtime:
  point reads do not use non-overlapping level range pruning, scans do not use
  lazy per-level iterators, read-view/facts/timestamp paths can scale with
  retained rows, and compaction/materialization still clone rows eagerly.
- The next performance restoration should stay inside L6/L5 contracts:
  restore old source-selection asymptotics and cheap pinned read views without
  adding benchmark-only fast paths or bypassing storage's L1-L9 design.

### L7. Commit Runtime

Status: `Confirmed with documented V1 deltas`

Architecture source:

- `docs/architecture/storage/l7-commit-runtime.md`
- `docs/architecture/storage/commit-timeline-substrate.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/storage/src/txn/context.rs`: old transaction request/context shape,
  read set, CAS facts, write batches, durability hooks, and commit options.
- `crates/storage/src/txn/manager.rs`: old version allocation, pending-version
  tracking, visible-version advancement, branch commit locks, quiesce guard,
  deleting-branch guard, commit hook, durable-but-not-visible classification,
  and recovery catch-up.
- `crates/storage/src/txn/validation.rs`: old read-set and CAS validation
  against branch state.
- `crates/storage/src/txn/lock_ordering.rs`: old lock ordering for branch and
  quiesce operations.
- `crates/storage/src/durability/commit_adapter.rs`: old WAL-before-storage
  bridge, direct/shared WAL modes, thread-local serialization buffers, forced
  durability handling, and fault injection around post-WAL storage apply.
- `crates/storage/src/durability/payload.rs` and
  `crates/storage/src/durability/format/wal_record.rs`: old WAL payload shape
  and commit record serialization.
- `crates/storage/src/segmented/mod.rs`: old atomic branch write application,
  version/timestamp stamping, branch max-version facts, and post-commit read
  visibility.
- `crates/storage/src/segmented/tests/batch.rs`,
  `crates/storage/src/segmented/tests/concurrency.rs`,
  `crates/storage/src/segmented/tests/publish_failures.rs`,
  `crates/storage/src/segmented/tests/basic.rs`, and
  `crates/storage/src/segmented/tests/fork.rs`: old commit batching,
  concurrency, publish failure, MVCC, and fork-frontier coverage.

Storage evidence:

- `crates/storage/src/commit/mod.rs`: L7 module boundary and exports.
- `crates/storage/src/commit/batch.rs`: internal `CommitBatch`,
  mutation/fact/options shape, batch validation, duplicate checks, storage-space
  rejection, expiry validation, row stamping, and read-only diagnostic batch.
- `crates/storage/src/commit/allocator.rs`: commit-version allocator,
  timestamp guard, generated/explicit timestamp handling, and recovery catch-up.
- `crates/storage/src/commit/cache.rs`: cache-mode mutating commit flow,
  conflict validation, row preparation, L6 apply, visible publish, and
  applied-but-not-visible classification.
- `crates/storage/src/commit/durable.rs`: durable mutating commit flow,
  WAL policy validation, WAL-before-L6-apply ordering, forced durability,
  durable-but-not-visible classification, and visible publication.
- `crates/storage/src/commit/branch_registry.rs` and
  `crates/storage/src/commit/guard.rs`: branch generation registry,
  deleting/deleted branch state, branch commit guards, and quiesce guard.
- `crates/storage/src/commit/conflict.rs`: read-set and CAS validation
  against a branch read source.
- `crates/storage/src/commit/durable_gate.rs`: unresolved durable commit
  gate, durable-not-applied/applied-not-visible facts, and follow-up admission
  blocking.
- `crates/storage/src/commit/visibility.rs`: visible-version tracker and
  publish/catch-up rules.
- `crates/storage/src/commit/replay.rs`: WAL replay request validation,
  idempotent duplicate-row classification, allocator catch-up, visible
  publication, and unresolved-gate reconciliation.
- `crates/storage/src/commit/timeline.rs`: commit timestamp/version
  timeline rows under the storage-owned commit timeline space.
- `crates/storage/src/commit/facts.rs`,
  `crates/storage/src/commit/outcome.rs`,
  `crates/storage/src/commit/error.rs`, and
  `crates/storage/src/commit/config.rs`: commit facts, outcomes, typed
  errors, and runtime limits.
- `crates/storage/src/lifecycle/cache.rs`: cache lifecycle commit entry
  point using `CommitCacheRuntime`.
- `crates/storage/src/lifecycle/durable/bootstrap.rs`: durable lifecycle
  commit entry point using `CommitDurableRuntime`, and recovery bootstrap using
  `CommitReplayRuntime`.
- `crates/storage/src/api/runtime.rs` and
  `crates/storage/src/api/commit.rs`: public L9 commit shell, durability
  resolution by open runtime mode, timestamp selection, generation guard
  mapping, and API-to-L7 batch mapping.
- `crates/storage/src/commit/tests/`,
  `crates/storage/src/api/tests/commit.rs`,
  `crates/storage/src/lifecycle/tests/cache.rs`,
  `crates/storage/src/lifecycle/tests/durable.rs`,
  `crates/storage/src/lifecycle/tests/recovery.rs`,
  `crates/storage/src/lifecycle/tests/commit_hardening.rs`,
  `crates/storage/src/lifecycle/tests/budget_runtime.rs`, and
  `crates/storage/src/lifecycle/tests/checkpoint.rs`: current L7,
  lifecycle, recovery, and public API coverage.

Confirmed parity:

- Storage has a distinct L7 commit-runtime layer. Commit ordering,
  version allocation, timestamp allocation, branch guards, conflict validation,
  WAL integration, row installation, and visible publication are not embedded
  in L9 API code or L6 table mechanics.
- `CommitBatch` is storage-internal and single-branch by construction.
  Mutations carry physical keys and values; validation facts carry read/CAS
  observations; options carry durability, conflict-validation, duplicate-key,
  timestamp, and origin policy.
- Batch validation preserves important old safety checks: mutating batches must
  be non-empty, diagnostic read-only batches must have no writes, mutation and
  fact branches must match the target branch, storage-owned user mutations are
  rejected, duplicate mutations/facts are rejected, invalid observed versions
  are rejected, TTL/expiry facts are validated, and runtime limits cap batch
  size.
- Version and timestamp ownership moved cleanly into L7.
  `CommitFactAllocator` allocates monotonic commit versions, enforces timestamp
  monotonicity for generated and explicit timestamps, allows failed
  pre-visible commits to leave version gaps, and catches up after recovery.
- Cache commits preserve the no-WAL commit shape: validate, admit branch,
  validate conflicts when requested, allocate stamp, prepare L6 rows plus
  timeline rows, atomically append to L6, publish visible version, and return a
  typed `CommitOutcome`.
- Durable commits preserve WAL-before-visible discipline: validate/admit,
  allocate stamp, prepare rows, append the WAL record through L4, enforce
  `Always` durability when requested, apply rows to L6, publish visible facts,
  and classify post-WAL failures as durable but not visible instead of silently
  losing the commit.
- Recovery replay is represented in L7. `CommitReplayRuntime` validates each
  durable record, routes it to the target branch, classifies absent/exact/
  partial/mismatched row state, applies missing rows idempotently, catches up
  the allocator, publishes visibility, and reconciles the unresolved durable
  gate.
- Branch deletion and generation guards exist. The registry rejects missing,
  deleting, deleted, and generation-mismatched branches before mutating commit
  execution.
- Read-only commit behavior is explicit. Diagnostic read-only batches return a
  snapshot outcome without allocating a commit version.
- Commit timeline rows are written by the commit runtime, not by callers. Each
  visible mutating commit produces storage-owned timestamp-to-version and
  version-to-timestamp rows, giving timestamp reads a durable substrate.
- The public API no longer defaults every commit to cache semantics. Public
  `CommitDurability::RuntimeDefault` resolves to cache only for an explicitly
  opened cache runtime, to `Standard` for durable-local standard, and to
  `Always` for durable-local always.
- Lifecycle integration exists. Cache lifecycle commits use `CommitCacheRuntime`;
  durable lifecycle commits use `CommitDurableRuntime`; durable recovery
  bootstrap replays WAL records through `CommitReplayRuntime`.

Intentional storage changes:

- Storage L7 owns internal commit batches, not public transaction sessions.
  Public transaction/session ergonomics remain an L9 or engine concern.
- Storage does not preserve old durable transaction IDs as a separate
  public storage concept. Commit version is the storage-local ordering fact.
- Storage V1 is single-branch per commit. Cross-branch atomic commits,
  distributed consensus, and object-store commit protocols remain outside L7
  V1.
- Fast-fail quiesce/admission is a V1 lifecycle shape. If public callers need
  old blocking semantics, L8 should layer retry/deadline behavior on top of the
  L7 guard result instead of making L7 wait internally.

Closed and deferred findings:

1. Independent branch commit concurrency is narrower than old storage.
   - Old `TransactionManager` had per-branch commit locks plus a quiesce
     `RwLock`, so independent branch commits could proceed concurrently while
     still preserving same-branch ordering.
   - Storage has per-branch guards, but `CommitUnresolvedDurableGate`
     also has a single `active_admission` slot. That serializes all mutating
     commits across branches, even when there is no unresolved durable commit.
   - This is accepted as an explicit V1 semantic decision, documented in
     `docs/architecture/storage/l7-commit-runtime.md`.
   - Owner: L8/L9 for retry/deadline policy; future L7 work only if the global
     gate is relaxed.
   - Replacement proof: same-branch and cross-branch contention tests return
     typed admission facts, and unresolved durable/applied-not-visible tests
     prove later commits cannot advance visibility unsafely.

2. Visible-version tracking no longer has old pending-version machinery.
   - Old `TransactionManager` maintained a `pending_versions` `BTreeSet` and
     advanced `visible_version` to the highest version before the first pending
     commit. That allowed allocated commits to finish out of order without
     publishing future versions over gaps.
   - Storage `VisibleVersionTracker` is a simple monotonic scalar. This is
     safe today because mutating commits are globally admitted one at a time
     and unresolved durable/applied-not-visible states block unsafe follow-up
     commits.
   - Status: closed as a documented semantic decision for V1.
   - Future gate: before loosening global admission, add pending-version facts
     or equivalent visibility advancement tests covering out-of-order post-WAL
     apply/publish results.

3. Conflict validation can inherit L6 read-view cost.
   - Old validation read directly through branch state using point-read
     mechanics.
   - Storage conflict validation can use `BranchReadView` as its source.
     Where that read view is captured eagerly, validation cost inherits the
     L6 captured-read-view row-scaling gap.
   - Status: closed for L7. Blind writes and empty validation sets skip source
     capture; read/CAS validation builds at most one pinned source and the
     remaining source cost is L6 read-view/source-shape work.
   - Owner of remaining cost: L6 pinned read view plus source planning.

4. WAL record construction is less allocation-conscious than old storage.
   - Old `commit_adapter` serialized WAL payloads into reusable thread-local
     buffers and appended pre-serialized bytes.
   - Storage `CommitDurableRuntime` currently prepares combined rows,
     clones them into a WAL record, then applies the rows to L6. The batch
     limits make this bounded, but it is not the old zero-extra-row-copy hot
     path.
   - Status: closed for parity. Row preparation is one pass and perf counters
     report WAL encode bytes, rows, and buffer reuse/allocation facts.
   - Future optimization: switch to a borrowed or pre-serialized WAL append
     path only if durable-write profiles justify it.

5. Commit timeline lookup is storage-backed but not yet proven efficient.
   - L7 writes timeline rows for every commit, which is the right substrate.
   - `CommitTimelineView::version_at_or_before` is vector-scan based after
     rows are collected. L6 timestamp reads can still scale with retained row
     count if timeline rows are discovered by broad branch scans.
   - Status: closed for L7 view construction and lookup mechanics. Timeline
     rows are isolated in `COMMIT_TIMELINE_SPACE`, reconciliation avoids nested
     scans, timestamp lookup uses sorted/indexed entries, and counters prove no
     user-row scans.
   - Owner of remaining source-shape work: L6 timestamp resolution using L7
     timeline rows.

6. Internal commit defaults still lean cache.
   - The public L9 runtime resolves `CommitDurability::RuntimeDefault` based
     on the opened runtime, which is the desired API shape.
   - Lower-layer `CommitBatchOptions::default()` still sets
     `CommitDurabilityMode::Cache`. That is fine for cache-specific test
     helpers, but risky if new internal call sites use the default in durable
     paths.
   - Status: closed. L9 runtime-default durability maps from the opened
     runtime; durable runtimes reject cache-only commit batches; source guards
     prove durable production paths do not use cache-default options.

7. Branch registry lookup is vector based.
   - Old storage used map/set structures for transaction locks and deleting
     branches.
   - Storage `CommitBranchRegistry` stores descriptors in a `Vec` because
     `BranchId` currently lacks `Ord`. This makes branch validation O(branch
     count).
   - Status: closed as a documented V1 bound. Branch-count scale tests record
     descriptor probes and keep generation/deletion behavior covered.
   - Future optimization: switch registry state to an indexed structure only if
     branch-count workloads exceed the documented envelope.

8. Quiesce behavior is fast-fail instead of old blocking drain.
   - Old quiesce used a write lock that naturally waited for in-flight commit
     read guards to drain.
   - Storage `CommitBranchGuardSet::try_begin_quiesce` returns a typed
     busy result if a commit guard is active.
   - Status: closed as an L7 primitive. Guard tests prove fast-fail facts and
     release behavior.
   - Owner of retry/deadline/close orchestration: L8 lifecycle consuming L7
     guards.

L7 conclusion:

- Storage preserves the core commit-runtime architecture. The major old
  correctness mechanics are present: internal batch validation, version and
  timestamp allocation, WAL-before-visible ordering, durable-but-not-visible
  classification, branch generation/deletion guards, conflict validation,
  visible-version publication, commit timeline rows, and replay/catch-up.
- L7 is confirmed with documented V1 deltas. The global mutating-admission
  gate, flat visible tracking, and fast-fail quiesce are explicit decisions
  with replacement tests rather than hidden compatibility gaps.
- The current 10M read/scan throughput gap should not be fixed in L7. The L7
  work should stay narrow: preserve commit correctness, make durability and
  concurrency tradeoffs explicit, and avoid moving L5/L6 source planning into
  commit-time fast paths.

### L8. Lifecycle / Recovery / Maintenance

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l8-lifecycle-recovery-maintenance.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/engine/src/database/open.rs`: old product/storage open sequence,
  config validation, directory/lock handling, recovery ordering, WAL writer
  construction, runtime config application, flush-thread setup, and
  post-recovery scheduling.
- `crates/engine/src/database/recovery.rs`: old primary/follower recovery
  orchestration, codec validation before side effects, storage recovery bridge,
  snapshot install callback, coordinator bootstrap, lossy fallback facts, and
  engine policy split.
- `crates/engine/src/database/lifecycle.rs`: old checkpoint, snapshot pruning,
  shutdown, GC, follower cleanup, and storage maintenance entry points.
- `crates/engine/src/database/transaction.rs`: old write-admission
  backpressure, coalesced flush scheduling after commit, background compaction
  scheduling, flush watermark update, and WAL truncation after flush.
- `crates/engine/src/background.rs`: old background task scheduler used for
  flush, compaction, checkpoint, and deferred maintenance.
- `crates/storage/src/durability/recovery_bootstrap.rs`,
  `crates/storage/src/durability/recovery.rs`,
  `crates/storage/src/durability/checkpoint_runtime.rs`,
  `crates/storage/src/durability/compaction/wal_only.rs`, and
  `crates/storage/src/durability/disk_snapshot/`: old storage-owned manifest,
  snapshot, WAL replay, checkpoint, WAL compaction, and snapshot pruning
  mechanics.
- `crates/storage/src/segmented/mod.rs`,
  `crates/storage/src/segmented/compaction.rs`,
  `crates/storage/src/segmented/recovery.rs`,
  `crates/storage/src/segmented/quarantine_protocol.rs`, and
  `crates/storage/src/segmented/ref_registry.rs`: old branch flush,
  compaction scoring, compaction chain, segment recovery, quarantine, purge,
  retention snapshot, and shared-table deletion barrier mechanics.
- `crates/storage/src/pressure.rs`, `crates/storage/src/rate_limiter.rs`,
  `crates/storage/src/memory_stats.rs`, and
  `crates/storage/src/runtime_config.rs`: old pressure, rate-limit, budget, and
  runtime configuration evidence.
- `crates/storage/src/segmented/tests/flush.rs`,
  `crates/storage/src/segmented/tests/leveled.rs`,
  `crates/storage/src/segmented/tests/lifecycle.rs`,
  `crates/storage/src/segmented/tests/post_restart_branch.rs`, and
  `crates/storage/src/segmented/tests/quarantine_reconciliation.rs`: old
  lifecycle, compaction, restart, quarantine, and recovery coverage.

Storage evidence:

- `crates/storage/src/lifecycle/mod.rs`: L8 module boundary and exported
  lifecycle surface.
- `crates/storage/src/lifecycle/facts.rs`,
  `crates/storage/src/lifecycle/state.rs`,
  `crates/storage/src/lifecycle/outcome.rs`,
  `crates/storage/src/lifecycle/config.rs`,
  `crates/storage/src/lifecycle/health.rs`, and
  `crates/storage/src/lifecycle/capability.rs`: storage modes, open plan,
  state machine, admission rules, open/maintenance/close outcomes, recovery
  health, lifecycle config, and backend capability validation.
- `crates/storage/src/lifecycle/cache.rs`: cache-mode lifecycle runtime,
  cache open, cache commit integration, cache diagnostics, maintenance queue,
  flush, compaction, materialization, and close behavior.
- `crates/storage/src/lifecycle/durable.rs`,
  `crates/storage/src/lifecycle/durable/bootstrap.rs`,
  `crates/storage/src/lifecycle/durable/maintenance.rs`, and
  `crates/storage/src/lifecycle/durable/close.rs`: durable-local service
  assembly, manifest/WAL/table/snapshot/quarantine services, recovery
  bootstrap, branch catalog rebuild, WAL replay, durable commit integration,
  maintenance dispatch, WAL growth policy, and close ordering.
- `crates/storage/src/lifecycle/recovery.rs`: checkpoint, WAL, table
  manifest, quarantine, flush-watermark, lossy/degraded recovery, and
  multi-branch recovery bootstrap orchestration.
- `crates/storage/src/lifecycle/maintenance.rs`: deterministic
  maintenance executor, priorities, scopes, coalescing, close policies,
  cancellation, drain, and fault hooks.
- `crates/storage/src/lifecycle/flush.rs`,
  `crates/storage/src/lifecycle/compaction.rs`,
  `crates/storage/src/lifecycle/checkpoint.rs`,
  `crates/storage/src/lifecycle/retention.rs`,
  `crates/storage/src/lifecycle/quarantine.rs`,
  `crates/storage/src/lifecycle/table_manifest.rs`,
  `crates/storage/src/lifecycle/table_reachability.rs`,
  `crates/storage/src/lifecycle/rewrite_publication.rs`, and
  `crates/storage/src/lifecycle/wal_growth.rs`: concrete L8 operations
  for flush, pressure, table rewrite, checkpoint, flush watermark, WAL
  truncation, retention, table-object reachability, quarantine, manifest-backed
  rewrite publication, and WAL growth.
- `crates/storage/src/lifecycle/budget.rs` and
  `crates/storage/src/lifecycle/branch_lifecycle.rs`: storage budget
  ledger, runtime usage facts, branch catalog, fork, clear, delete, and
  pending release integration.
- `crates/storage/src/api/runtime.rs`: L9 open/close/maintenance/
  diagnostics calls into L8, including explicit enqueue, run-next, drain,
  diagnostics, pressure reports, and WAL growth maintenance.
- `crates/storage/src/lifecycle/tests/`: state, capability, cache,
  durable, recovery, flush, flush watermark, checkpoint, compaction, retention,
  quarantine, table-object retention, budget, close, branch lifecycle, and
  maintenance tests.

Confirmed parity:

- Storage has a distinct L8 lifecycle layer. Open, recovery, maintenance,
  checkpoint, retention, quarantine, and close are not hidden in L9 API code,
  L6 branch state, or L7 commit code.
- The lifecycle state model exists and is explicit. `LifecycleStateMachine`
  tracks `New`, `Opening`, `Recovering`, `Open`, `Closing`, `Closed`, and
  `Failed`, and admission checks gate open, read, commit, recovery,
  maintenance, close, close-retry, and health queries.
- `StorageOpenPlan` and `StorageOpenOutcome` are storage-shaped. They carry
  mode, codec, recovery strictness, lifecycle config, backend capabilities,
  durable identity, recovered visibility, checkpoint/WAL/table/quarantine
  facts, bootstrap facts, budget snapshot, and lifecycle stats without pulling
  engine primitive semantics into L8.
- Capability validation runs before mode-specific open. Cache, durable-local
  standard, durable-local always, and object-durable-candidate capability
  requirements are represented as storage-mode requests.
- Cache and durable-local runtimes are separate. Cache open starts empty with
  no durable recovery; durable-local open assembles manifest, table manifest,
  branch catalog manifest, pending releases manifest, WAL, snapshot, table
  object, checkpoint, quarantine, sidecar, and writer-guard services.
- Durable recovery is substantially restored. L8 loads checkpoint rows, recovers
  quarantine inventory facts, recovers table manifests, validates flush
  watermark recoverability, chooses WAL replay start, replays WAL through L7,
  rebuilds the branch catalog, installs non-seeded checkpoint rows, loads
  pending releases, applies per-branch manifests, catches up allocator and
  visibility, and returns typed recovery health.
- L8 owns a deterministic maintenance executor. Tasks have kind, priority,
  scope, close policy, coalescing key, queue depth, stats, drain/cancel hooks,
  active-task state, and fault injection points.
- Flush, table rewrite, checkpoint, retention, quarantine, purge, repair, and
  WAL growth have L8-shaped request/outcome objects. Each operation returns
  typed maintenance outcomes and recovery-health debt instead of boolean-only
  success.
- Durable checkpointing is row-native. It writes storage rows into checkpoint
  sections, updates manifest snapshot facts, can persist flush-watermark proof,
  and can trigger WAL truncation through L4 services.
- Flush-watermark and WAL truncation are conservative. Storage requires a
  checkpoint or table-manifest coverage proof before persisting a flush
  watermark, and WAL truncation consumes a retention proof rather than deleting
  covered segments opportunistically.
- Retention and quarantine are proof-driven. Recovery health can block unsafe
  reclaim, retention records decisions by object family, table-object
  reachability produces proof tokens, quarantine publishes inventory, purge
  requires a fresh proof, and repair/reconciliation produce typed health debt.
- Durable close is ordered and idempotent. It cancels ordinary pending
  maintenance, drains close-required work (since 1.2.6: a bounded reclaim
  drive, a flush of any large unflushed delta, and a checkpoint of every
  branch that truncates the WAL and prunes the superseded snapshot — DUR-019), records maintenance health, attempts
  commit quiesce, refuses clean close with unresolved durable commits, closes
  WAL, forces a final manifest publish when health changed, releases the writer
  guard, records the close outcome, and returns stable idempotent facts on
  later close calls.
- Follower mode is intentionally not preserved in storage L8. The old
  follower refresh and persisted follower-state paths remain old-engine
  evidence, not storage restoration targets.

Intentional storage changes:

- L8 is storage-internal. Product open policy, primitive snapshot semantics,
  IPC, public UX, and product recovery wording stay above L8.
- Storage checkpoints are row-native with optional extra sections. L8 can
  carry opaque extra snapshot sections, but it does not materialize graph,
  vector, JSON, search, event, or intelligence state.
- Object durable mode is represented as a candidate capability shape, not a V1
  fully-supported lifecycle runtime.
- The maintenance executor is deterministic and in-process. That is useful for
  tests and L9 explicit maintenance, but it is not by itself equivalent to the
  old background scheduling loop.

Gaps:

1. Automatic maintenance scheduling is not restored.
   - Old writes called `schedule_flush_if_needed` after successful mutating
     commits. That coalesced flush tasks, drained all frozen memtables across
     branches, then scheduled a background compaction chain.
   - Storage can report pressure and can enqueue maintenance explicitly,
     but normal commits do not automatically enqueue the pressure-suggested
     flush, compaction, or materialization task. Durable commits only evaluate
     WAL growth policy and may enqueue checkpoint work.
   - This is directly related to the benchmark signal: L0/source fanout grows
     unless the caller manually drains enough maintenance.
   - Owner: L8 maintenance scheduling, called from L7/L9 commit completion
     without moving read-path behavior into L9.
   - Exit gate: after sustained writes, L8 automatically schedules and drains
     flush/compaction/materialization work until source fanout is bounded, and
     L9 100K/1M/5M/10M benchmarks no longer require benchmark-specific manual
     maintenance calls.

2. Write-admission backpressure is diagnostic-only today.
   - Old `maybe_apply_write_backpressure` ran before commit. It synchronously
     flushed frozen memtables when needed, slowed writes at the L0 slowdown
     threshold, stalled at the L0 stop threshold, sampled memtable bytes and
     segment metadata pressure, and woke stalled writers after compaction.
   - Storage `collect_storage_pressure` has
     `BlockMutatingAdmission`, `Urgent`, and `Background` severities, but the
     normal commit path does not enforce those facts before accepting a
     mutating commit.
   - Owner: L8 admission policy consuming L6 pressure facts and L7 guard
     results.
   - Exit gate: mutating commit admission consults L8 pressure facts, can
     enqueue/drive maintenance, and either slows/stalls/rejects with a typed
     storage error before the commit or documents an intentional no-stall V1
     policy with separate bounded-fanout proof.

3. Flush drain semantics are weaker than old storage.
   - Old flush scheduling drained all branches needing flush and looped over
     each branch until no frozen memtable remained, with a bounded retry loop
     to cover the race where writers froze more state during the drain.
   - Storage `Flush` task flushes one selected frozen table for one
     branch per task run. The executor can be drained by a caller, but no L8
     policy currently turns one pressure event into a complete frozen-state
     drain.
   - Owner: L8 flush scheduler/executor policy.
   - Exit gate: one coalesced flush scheduling event drains all currently
     eligible frozen state, handles freeze-during-drain, updates durable table
     manifests and flush-watermark candidates as it goes, and reports any
     deferred/failure facts.

4. Compaction scheduling lacks old scoring and chain behavior.
   - Old storage computed per-level compaction scores, picked the
     highest-scoring branch/level, performed one compaction, then resubmitted
     the chain until no score exceeded target. L0 was count-based; L1+ was
     byte-target based; nonzero compaction advanced compact pointers.
   - Storage pressure currently suggests level-0 compaction at fixed L0
     count thresholds. `compaction_request_from_maintenance_task` maps nonzero
     levels to table index 0, and there is no L8 scheduler that picks the
     highest score across branches and levels or keeps re-running until the
     level structure is healthy.
   - Owner: L8 compaction scheduler using L6 compaction candidates and L5
     table facts.
   - Exit gate: add score-based branch/level selection, one-compaction-per-task
     chaining, coalescing, and counters for score, selected level, input/output
     table counts, and post-drain L0/L1+ shape.

5. Flush-watermark proof is not fully multi-branch yet.
   - Storage has conservative checkpoint/table-manifest proofs, but
     `persist_table_manifest_flush_watermark` currently requires active
     branches to equal the single seeded branch and rejects broader catalogs
     with a forward-compat guard.
   - Old flush watermark logic computed a global lower bound over flushed
     branch state and excluded branches without flushed state.
   - Owner: L8 flush-watermark proof over branch catalog and table manifests.
   - Exit gate: table-manifest flush-watermark proof loads every active
     branch's manifest/state, computes the global recoverable lower bound, and
     has restart tests for branches with no flushed tables, deleted branches,
     inherited layers, and pending releases.

6. Pending-release durability is still incomplete.
   - Storage has a PendingReleasesManifest service and recovery loader,
     but the durable runtime still carries an active `pending_releases` buffer
     whose bootstrap comment describes it as in-memory-only and says durable
     persistence of release tombstones is tracked as separate closeout work.
   - Old storage's segment reference registry plus quarantine protocol made
     replaced/removed table cleanup recovery-aware.
   - Owner: L8 branch lifecycle, pending releases manifest, retention, and
     quarantine integration.
   - Exit gate: branch clear/delete/rewrite release plans survive every
     crash/restart window until retention drains or quarantines them, with
     tests for missed, duplicated, stale, and partially-published release
     manifests.

7. Close quiesce is one-shot instead of deadline-driven.
   - Old shutdown waited for idle commits and had deadline-aware shutdown
     orchestration above storage.
   - Storage durable close calls `try_begin_quiesce` once and returns a
     retry-pending/timeout-style error when commit quiesce is unavailable.
   - This is a reasonable L7 primitive, but L8 does not yet provide the
     deadline/retry loop that makes close robust under normal concurrent use.
   - Owner: L8 close plan consuming L7 quiesce.
   - Exit gate: close supports configured deadline/retry behavior, returns
     stable timeout facts, and has tests for in-flight commit, active
     maintenance, WAL close failure, manifest fsync failure, and idempotent
     retry.

8. Budget and rate-limiting are not old-runtime parity.
   - Storage has `StorageBudgetLedger` checks for generated artifacts,
     manifest/catalog budget, table readers, rotation, and maintenance enqueue.
   - Old runtime also had compaction rate limiting, memtable byte pressure,
     segment metadata pressure, and writer slow/stop thresholds tied to
     backpressure.
   - Owner: L8 budget/pressure policy over L5/L6 facts.
   - Exit gate: resolved runtime budget drives flush, compaction,
     materialization, checkpoint, and write-admission behavior, with metrics
     showing memory, table-reader, manifest, WAL, and generated-artifact
     pressure.

9. End-to-end lifecycle crash/perf proof is not complete.
   - Storage has many targeted lifecycle tests, but the parity proof
     still needs full transition-window coverage: flush publish before install,
     table manifest publication debt, compaction replacement, materialization
     replacement, checkpoint publication, manifest snapshot update, flush
     watermark persistence, WAL truncation, pending release drain, quarantine
     publish, purge, branch delete, and close.
   - Owner: L8 recovery/maintenance tests plus L9 benchmark harness.
   - Exit gate: crash/restart tests classify each partial publication window,
     and sustained-load benchmarks prove maintenance keeps L0/source fanout
     bounded at 100K, 1M, 5M, 10M, and larger scales.

L8 conclusion:

- Storage has a serious L8 implementation, not just stubs. It restored
  typed lifecycle state, open outcomes, recovery health, durable service
  assembly, checkpoint/WAL/table/quarantine recovery, commit bootstrap,
  maintenance task execution, durable close ordering, and proof-driven
  retention/quarantine primitives.
- L8 is still `Partial` because the old automatic maintenance and
  write-admission loop has not been restored. The system can describe pressure
  and run maintenance explicitly, but normal writes do not yet drive
  flush/compaction/materialization hard enough to preserve old LSM source
  shape.
- The immediate performance work should keep the layer boundary intact: L8
  should schedule and enforce maintenance based on L6/L5 facts, while L6/L5
  keep owning table source planning and compaction mechanics. No L9
  benchmark-only maintenance bypass should be introduced.

### L9. Storage API Boundary

Status: `Partial`

Architecture source:

- `docs/architecture/storage/l9-storage-api-boundary.md`
- `docs/architecture/storage-architecture.md`

Old-storage evidence:

- `crates/storage/src/traits.rs`: old storage-owned MVCC trait surface for
  latest/versioned reads, history, prefix scan, current version, versioned
  writes, tombstones, and atomic batch application.
- `crates/storage/src/runtime_config.rs`: old storage-owned runtime knobs for
  cache sizing, write buffer sizing, version retention, immutable memtables,
  target file size, level size, block size, bloom bits, and compaction rate.
- `crates/storage/src/txn/context.rs`,
  `crates/storage/src/txn/manager.rs`, and
  `crates/storage/src/txn/validation.rs`: old transaction boundary evidence
  for read-set tracking, CAS validation, branch generation guards, read-only
  skips, write buffering, and all-or-nothing commit application.
- `crates/engine/src/database/open.rs`,
  `crates/engine/src/database/recovery.rs`,
  `crates/engine/src/database/lifecycle.rs`, and
  `crates/engine/src/database/transaction.rs`: old engine/storage integration,
  open policy, recovery policy, maintenance scheduling, backpressure, and
  storage commit consumption.
- `crates/engine/src/transaction/context.rs`: old product transaction wrapper
  that kept JSON/event/KV semantics above storage while relying on storage
  transaction mechanics below it.

Storage evidence:

- `crates/storage/src/lib.rs`: declares `api` as the supported public
  boundary, documents durable-local native open, explicit volatile open, and
  rejects `localfs` on `wasm32`.
- `crates/storage/src/api/mod.rs`: reexports the L9 storage-shaped API
  vocabulary and keeps lower storage layers private.
- `crates/storage/src/api/options.rs`: storage mode, durability,
  budget, WAL-growth, and open-option validation.
- `crates/storage/src/api/runtime.rs`: open, close, commit, read,
  branch, maintenance, diagnostics, timestamp/timeline lookup, and lower-layer
  mapping.
- `crates/storage/src/api/atoms.rs`,
  `crates/storage/src/api/commit.rs`,
  `crates/storage/src/api/read.rs`,
  `crates/storage/src/api/branch.rs`,
  `crates/storage/src/api/maintenance.rs`,
  `crates/storage/src/api/diagnostics.rs`,
  `crates/storage/src/api/outcome.rs`, and
  `crates/storage/src/api/error.rs`: public atoms, request shells,
  outcome summaries, diagnostics, and error categories.
- `crates/storage/tests/api_conformance.rs`: current L9 conformance
  tests.
- `crates/storage/tests/lifecycle_source_guard.rs`,
  `crates/storage/tests/commit_runtime_source_guard.rs`, and related
  source-guard tests: evidence that lower layers are meant to stay below L9.

Confirmed parity:

- Storage has a real L9 API module. The public crate-level documentation
  names `api` as the supported boundary and makes native directory-backed
  opens go through `StorageRuntime::open_local(root)`.
- Volatile storage is explicit. `StorageRuntime::open_cache()` and
  `StorageRuntime::open_ephemeral()` are separate entry points, while
  `StorageOpenOptions::cache()` documents non-durable intent. There is no
  default `StorageOpenOptions` that silently selects cache mode.
- Durable local storage is explicit and capability-gated. `open_local(root)`
  uses standard durability, `open_durable_local(root, policy)` accepts an
  explicit policy, and builds without `localfs` return an
  unsupported-capability error instead of falling back to cache.
- Unsupported V1 modes are represented without pretending to work.
  `ObjectDurableCandidate` and `DistributedCandidate` validate to typed
  unsupported-capability errors.
- Public atoms are storage-shaped. `StorageSpaceId`, `StorageKey`, and
  `StorageValue` are byte wrappers; `ReadLimit` and `ScanRange` validate
  storage mechanics rather than product primitives.
- Public reads cover the expected storage mechanics: latest, version-bounded,
  timestamp-bounded point reads; prefix scans; range scans; per-key history;
  timestamp-to-version lookup; version-to-timestamp lookup; and timeline
  bounds.
- Public branch operations are storage mechanics: create, describe, list, fork
  current, fork at retained version, fork at timestamp, clear, and delete.
  Outcomes expose generation, parent, fork, deletion, state-revision, and
  cleanup facts without merge, diff, restore UX, or branch naming semantics.
- Public commits are single-branch storage batches. `CommitBatch` carries
  puts, deletes, optional TTL, CAS-style conditions, durability options, and an
  optional branch generation guard. It does not expose WAL records, table
  names, object names, engine transaction sessions, JSON paths, graph edges,
  vector payloads, search documents, event chains, IPC, or StrataHub concepts.
- L9 maps cache and durable commits into L7/L8 rather than reimplementing
  lower mechanics. Runtime-default commit durability resolves based on the
  opened storage mode, and invalid mode/durability combinations fail before
  commit.
- Maintenance and diagnostics are storage-shaped. L9 exposes flush,
  compaction, materialization, checkpoint, retention, snapshot pruning,
  reclaim, quarantine, purge, repair, WAL growth, queue status, drain,
  recovery health, budget, pressure, table reachability, branch catalog,
  timeline, and close facts.
- Close is idempotent at the boundary. A second close returns stable closed
  facts rather than trying to run lower shutdown twice.
- Test and fault access stays gated. Raw row append, timestamp coverage, writer
  guard release, branch reachability pinning, and forced commit timestamp
  helpers are behind `cfg(test)` or `testkit`.

Intentional storage changes:

- L9 currently exposes a concrete `StorageRuntime` handle instead of a public
  storage trait like old `Storage`. That is acceptable for the current
  storage shape as long as engine depends only on L9 and not lower
  modules.
- Product transaction sessions are not L9 APIs. Engine should own public
  transaction ergonomics and translate storage-shaped read facts, writes, CAS
  expectations, and durability requests into L9 requests.
- Product semantics remain above storage. The scan for product terms in
  `crates/storage/src/api/` does not show JSON, graph, vector, search,
  event, IPC, inference, intelligence, or StrataHub API concepts in the normal
  L9 surface.
- L9 benchmarks are proof gates. They should exercise normal storage APIs, but
  benchmark drivers must not force L9-only fast paths that bypass L5/L6/L8
  mechanics.

Gaps:

1. There is no production engine consumer yet.
   - The L9 architecture says engine is the only normal production
     consumer, but this workspace currently has `crates/engine` and no
     `crates/engine`.
   - Current storage imports are concentrated in storage tests,
     fuzz targets, conformance tests, testkit, and source-guard tests.
   - The old engine still consumes `strata_storage` directly through
     `SegmentedStore`, `TransactionContext`, and old durability integration.
   - Owner: engine integration plan plus dependency/source guards.
   - Exit gate: engine opens, commits, reads, scans, branches,
     checkpoints, recovers, and closes through L9 only; crates above
     engine do not depend on `strata-storage` in normal production
     code.

2. Public read-set validation is not restored through L9.
   - Old `TransactionContext` recorded every snapshot read in `read_set`, and
     `validate_transaction` checked the recorded versions against current
     storage before commit. CAS facts were separate from read-set facts.
   - Storage L7 has internal `CommitReadFact`,
     `CommitValidationFacts`, and read-set conflict validation.
   - L9 `CommitBatch` exposes CAS-style `CommitCondition` values but no public
     storage-shaped read facts. `map_api_commit_batch` currently constructs
     `CommitValidationFacts::new(Vec::new(), cas_set)`.
   - `CommitOptions::require_conflict_check(true)` therefore only switches the
     lower validation mode to `Validate`; without public read facts or CAS
     conditions there is nothing meaningful to validate.
   - Owner: L9 commit API shape and engine transaction adapter, backed by
     L7 validation.
   - Exit gate: engine can pass read-set facts for storage-shaped rows
     through L9 without exposing product transaction sessions, and tests prove
     stale reads conflict while blind writes still follow the intended old
     semantics.

3. Write-stall and backpressure facts are not complete at the boundary.
   - Old engine/storage commit returned write-stall and durability failure
     information through the transaction path, and old writes actively slowed
     or stalled based on L0 and memory pressure.
   - Storage L8 can report pressure and L7 has conflict/durable error
     categories, but L9 `CommitSummary` only reports branch, version,
     timestamp, durability, mutation counts, timeline row count, and
     visibility.
   - L9 cannot yet tell engine that a commit was delayed, stalled,
     rejected by storage pressure, or accepted under pressure debt.
   - Owner: L8 admission/backpressure policy and L9 commit outcome/error
     mapping.
   - Exit gate: normal mutating commits either enforce L8 pressure policy or
     return explicit storage-shaped pressure/stall facts that engine can
     map to product diagnostics.

4. Maintenance is exposed, but not a normal lifecycle policy.
   - L9 exposes direct flush, compaction, materialization, checkpoint,
     retention, reclaim, quarantine, purge, repair, WAL growth, run-next, and
     drain controls.
   - The old runtime scheduled flush and compaction automatically after
     writes; product users did not need to manually run storage maintenance to
     keep source fanout bounded.
   - The boundary is correct only if direct maintenance remains an engine,
     diagnostic, test, or benchmark control over L8. It must not become the
     normal way users keep the database healthy.
   - Owner: L8 maintenance scheduling and L9 documentation/tests.
   - Exit gate: sustained-write benchmarks through L9 remain healthy without
     benchmark-specific manual maintenance scripts, and explicit maintenance
     remains an optional control surface with typed facts.

5. Checkpoint payload extension is not exposed through L9.
   - L9 exposes checkpoint as `MaintenanceTask::Checkpoint`.
   - The architecture allows engine to provide opaque derived-state
     checkpoint sections while keeping committed row recovery storage-native.
   - The current L9 checkpoint request has no way to pass opaque engine-owned
     derived sections or receive section-level publication facts.
   - Owner: L9 checkpoint request/outcome shape over L8 checkpoint services.
   - Exit gate: if engine needs derived checkpoint sections, L9 exposes a
     primitive-neutral payload API with tests proving storage can recover
     committed rows without those sections.

6. Diagnostics are intentionally broad but still partial.
   - `DiagnosticsReadActivityReport` is currently `unknown()` for cache and
     durable runtimes.
   - Cache table reachability, retention, quarantine, and checkpoint reports
     are unsupported or unknown where appropriate, but durable quarantine is
     still unknown and table/read metrics are not enough to prove source-class
     behavior.
   - Owner: L5/L6/L8 diagnostics feeding L9 reports.
   - Exit gate: L9 diagnostics can report the source-layout and maintenance
     facts required by the performance audit: active/frozen rows, L0 table
     count, nonzero-level count, inherited layers, read probes by source
     class, scan cursor setup by source class, maintenance debt, and
     durable-only recovery debt.

7. Wasm-none mode remains a target/feature shape, not a documented L9 mode.
   - `crates/storage/src/lib.rs` rejects `localfs` on `wasm32`, and
     cache mode can be opened without local filesystem support.
   - The L9 API does not yet name a wasm-none-supported subset or test it as a
     first-class boundary contract.
   - Owner: L1 capability validation, L8 open policy, and L9 mode
     documentation/tests.
   - Exit gate: wasm-none builds document and test the supported L9 subset,
     with durable-local failures classified as unsupported capability rather
     than accidental cache fallback.

8. Timeline lookups are boundary-correct but not scale-proved.
   - L9 exposes timestamp/version lookup and timeline bounds instead of raw
     timeline rows, which matches the architecture.
   - Current implementation builds a `CommitTimelineView` by scanning timeline
     rows through a branch read view. That is acceptable as a lower-layer
     implementation detail only if L7/L6 prove the timeline path does not
     become a scale bottleneck.
   - Owner: L7 timeline indexing or L6 timeline source planning, surfaced
     through L9 lookup tests.
   - Exit gate: timestamp and version lookup costs are bounded and tested
     independently from product history APIs.

L9 conclusion:

- Storage has a clean, storage-shaped L9 surface. It correctly keeps
  product meaning above storage, keeps lower modules private, makes volatile
  storage explicit, makes durable local storage explicit, exposes the expected
  read/scan/history/branch/maintenance/diagnostics/close mechanics, and routes
  behavior through L7/L8 rather than reimplementing storage internals at the
  API layer.
- L9 is still `Partial` because the production consumer is not present, the
  old read-set validation contract is not reachable through public L9 commits,
  diagnostics are not sufficient to prove performance parity, direct
  maintenance could still be mistaken for lifecycle policy, and wasm-none is
  not yet a first-class documented mode contract.
- The next implementation work should not add L9 fast paths. L9 should stay
  the clean proof boundary while L5/L6/L8 restore the old storage mechanics
  underneath it. The only near-term L9 API change that looks architecturally
  necessary is a storage-shaped read-set fact surface for engine
  transactions.

## Restoration Source Map

These old-storage files should be treated as the primary reference when restoring
mechanics in storage.

| Mechanic | Old reference files | Storage files to compare |
| --- | --- | --- |
| Internal key ordering | `crates/storage/src/key_encoding.rs`, `crates/storage/src/memtable.rs` | `crates/storage/src/format/key.rs`, `crates/storage/src/table/key.rs` |
| Mutable/frozen table lookup | `crates/storage/src/memtable.rs` | `crates/storage/src/table/mutable.rs`, `crates/storage/src/table/cursor.rs` |
| Segment/table reader lookup | `crates/storage/src/segment.rs` | `crates/storage/src/table/reader.rs`, `crates/storage/src/table/cursor.rs` |
| Branch point reads | `crates/storage/src/segmented/mod.rs` | `crates/storage/src/branch/read.rs`, `crates/storage/src/branch/state/read_hooks.rs` |
| Merge/MVCC iteration | `crates/storage/src/merge_iter.rs`, `crates/storage/src/seekable.rs`, `crates/storage/src/segmented/mod.rs` | `crates/storage/src/branch/read.rs`, `crates/storage/src/table/cursor.rs` |
| Lazy level iteration | `crates/storage/src/segment.rs`, `crates/storage/src/seekable.rs`, `crates/storage/src/segmented/mod.rs` | `crates/storage/src/branch/read.rs`, `crates/storage/src/table/cursor.rs`, `crates/storage/src/table/reader.rs` |
| LSM compaction scoring | `crates/storage/src/segmented/compaction.rs`, `crates/storage/src/compaction.rs` | `crates/storage/src/lifecycle/compaction.rs`, `crates/storage/src/lifecycle/maintenance.rs` |
| L0 to L1 and L+1 compaction | `crates/storage/src/segmented/compaction.rs`, `crates/storage/src/segment_builder.rs` | `crates/storage/src/branch/state/compaction.rs`, `crates/storage/src/table/compaction.rs` |
| LSM invariant tests | `crates/storage/src/segmented/tests/leveled.rs`, `crates/storage/src/segmented/tests/flush.rs` | `crates/storage/src/branch/tests/owned_compaction.rs`, `crates/storage/src/lifecycle/tests/compaction/` |
| Tombstones, TTL, history | `crates/storage/src/memtable.rs`, `crates/storage/src/merge_iter.rs`, `crates/storage/src/ttl.rs`, `crates/storage/src/compaction.rs`, `crates/storage/src/segmented/mod.rs`, `crates/storage/src/segmented/tests/basic.rs`, `crates/storage/src/segmented/tests/resurrection.rs` | `crates/storage/src/api/runtime.rs`, `crates/storage/src/api/tests/read.rs`, `crates/storage/src/commit/batch.rs`, `crates/storage/src/branch/read.rs`, `crates/storage/src/branch/pruning.rs`, `crates/storage/src/table/compaction.rs`, `crates/storage/src/branch/tests/read_view.rs`, `crates/storage/src/branch/tests/row_pruning/` |
| Branch COW inheritance | `crates/storage/src/merge_iter.rs`, `crates/storage/src/segmented/mod.rs`, `crates/storage/src/segmented/tests/fork.rs` | `crates/storage/src/branch/read.rs`, `crates/storage/src/branch/state/materialization.rs`, `crates/storage/src/branch/tests/inheritance_materialization/` |
| Snapshot install and recovery | `crates/storage/src/durability/decoded_snapshot_install.rs`, `crates/storage/src/durability/recovery_bootstrap.rs`, `crates/storage/src/segmented/recovery.rs` | `crates/storage/src/service/snapshot/`, `crates/storage/src/lifecycle/recovery.rs`, `crates/storage/src/branch/state/manifest_recovery.rs` |
| WAL/checkpoint/manifest | `crates/storage/src/durability/wal/`, `crates/storage/src/durability/checkpoint_runtime.rs`, `crates/storage/src/durability/format/manifest.rs` | `crates/storage/src/service/wal.rs`, `crates/storage/src/service/checkpoint/`, `crates/storage/src/lifecycle/table_manifest.rs` |
| Commit runtime | `crates/storage/src/txn/context.rs`, `crates/storage/src/txn/manager.rs`, `crates/storage/src/txn/validation.rs`, `crates/storage/src/txn/lock_ordering.rs`, `crates/storage/src/durability/commit_adapter.rs`, `crates/storage/src/durability/payload.rs`, `crates/storage/src/segmented/mod.rs` | `crates/storage/src/commit/`, `crates/storage/src/lifecycle/cache.rs`, `crates/storage/src/lifecycle/durable/bootstrap.rs`, `crates/storage/src/api/runtime.rs`, `crates/storage/src/api/commit.rs` |
| Lifecycle/recovery/maintenance | `crates/engine/src/database/open.rs`, `crates/engine/src/database/recovery.rs`, `crates/engine/src/database/lifecycle.rs`, `crates/engine/src/database/transaction.rs`, `crates/engine/src/background.rs`, `crates/storage/src/durability/recovery_bootstrap.rs`, `crates/storage/src/durability/recovery.rs`, `crates/storage/src/durability/checkpoint_runtime.rs`, `crates/storage/src/segmented/compaction.rs`, `crates/storage/src/segmented/quarantine_protocol.rs`, `crates/storage/src/pressure.rs`, `crates/storage/src/rate_limiter.rs`, `crates/storage/src/memory_stats.rs`, `crates/storage/src/runtime_config.rs` | `crates/storage/src/lifecycle/`, `crates/storage/src/api/runtime.rs`, `crates/storage/src/service/`, `crates/storage/src/branch/state/compaction.rs`, `crates/storage/src/branch/state/materialization.rs` |
| Memory pressure and maintenance | `crates/storage/src/runtime_config.rs`, `crates/storage/src/segmented/compaction.rs` | `crates/storage/src/lifecycle/budget.rs`, `crates/storage/src/lifecycle/compaction.rs`, `crates/storage/src/api/maintenance.rs` |
| Storage API boundary | `crates/storage/src/traits.rs`, `crates/storage/src/runtime_config.rs`, `crates/storage/src/txn/context.rs`, `crates/storage/src/txn/manager.rs`, `crates/storage/src/txn/validation.rs`, `crates/engine/src/database/open.rs`, `crates/engine/src/database/recovery.rs`, `crates/engine/src/database/lifecycle.rs`, `crates/engine/src/database/transaction.rs`, `crates/engine/src/transaction/context.rs` | `crates/storage/src/lib.rs`, `crates/storage/src/api/`, `crates/storage/tests/api_conformance.rs`, `crates/storage/tests/lifecycle_source_guard.rs`, `crates/storage/tests/commit_runtime_source_guard.rs` |

## Audit Matrix

### 1. LSM Layout And Level Invariants

Status: `Partial`

Old invariant:

- L0 may contain overlapping flushed segments.
- L1+ segments are sorted by key range and non-overlapping.
- Point reads search L0 linearly but use key-range pruning for L1+.
- Scans use lazy level iteration for L1+ instead of opening/seeking every file.

Old reference files:

- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/compaction.rs`
- `crates/storage/src/segment.rs`
- `crates/storage/src/segmented/tests/leveled.rs`

Storage files inspected:

- `crates/storage/src/branch/state.rs`
- `crates/storage/src/branch/read.rs`
- `crates/storage/src/branch/state/compaction.rs`
- `crates/storage/src/lifecycle/compaction.rs`

Findings:

- Storage has `BranchLocalState::owned_levels`.
- L0 installation inserts newest-first and allows overlap.
- Nonzero levels enforce sorted, non-overlapping physical key ranges during
  install and validation.
- Storage has `CompactL0ToLevelOne` and `CompactLevel` planning.
- The benchmark load path explicitly calls `MaintenanceTask::Flush` every
  100K rows, which produces L0 tables.
- That path does not drain enough compaction to turn L0 fanout into useful
  lower-level structure.
- The storage pressure collector suggests L0 compaction only after thresholds;
  it is not equivalent to the old storage compaction scoring loop.

Evidence:

- At 10M and `flush_every=100000`, storage behaves as if it has about 100
  read sources: 100 source probes per point read and 101 source seeks per scan.
- Standard mode shows the same fanout profile as cache mode, so this is not a
  cache-only divergence.

Required proof before `Confirmed`:

- Add or expose a level-count diagnostic after load: L0 count, L1 count, L2+
  count, and total owned table count.
- Assert that sustained load eventually reduces L0 table count and moves data
  into non-overlapping L1+ levels.
- Add a perf counter showing point reads probe active/frozen/L0 plus at most
  one table per nonzero level.

### 2. Point-Read Source Pruning

Status: `Partial`

Old invariant:

- Encode the typed key once.
- Seek active/frozen memtables with ordered-key lookup.
- Probe L0 segments newest-first because they overlap.
- For each nonzero level, binary-search by key range and probe at most one
  segment.
- Stop as soon as the newest visible row is found.

Old reference files:

- `crates/storage/src/memtable.rs`
- `crates/storage/src/segment.rs`
- `crates/storage/src/segmented/mod.rs`

Storage files to audit:

- `crates/storage/src/branch/read.rs`
- `crates/storage/src/table/mutable.rs`
- `crates/storage/src/table/reader.rs`
- `crates/storage/src/table/cursor.rs`

Findings:

- Old storage's `get_versioned_from_branch` in
  `crates/storage/src/segmented/mod.rs` encoded the typed key once, checked the
  active memtable, checked frozen memtables newest-first, checked L0 segments
  newest-first, and then called `point_lookup_level_preencoded` for each
  nonzero level.
- Old storage's `point_lookup_level_preencoded` in
  `crates/storage/src/segmented/mod.rs` binary-searched the level's sorted
  segment key ranges and probed at most one segment per nonzero level.
- Old storage's memtable path in `crates/storage/src/memtable.rs` used
  `get_versioned_preencoded`, which performs an ordered range seek on the
  preencoded key and stops when the physical key changes. Frozen memtables also
  use a bloom check before the seek.
- Old storage's segment path in `crates/storage/src/segment.rs` used
  `point_lookup_preencoded`, which applies a bloom check and then performs an
  index/block-local lookup instead of scanning unrelated rows.
- Storage latest point reads call `read_latest_point` from
  `crates/storage/src/api/runtime.rs`, so the hot latest-read path can use
  borrowed runtime state instead of always capturing an owned `BranchReadView`.
- Storage table-local seek mechanics exist:
  `MutableTable::seek_physical_key` and `FrozenTable::seek_physical_key` in
  `crates/storage/src/table/mutable.rs`, plus
  `ImmutableTableReader::seek_physical_key` in
  `crates/storage/src/table/reader.rs`.
- Storage's `visible_point_candidates` in
  `crates/storage/src/branch/read.rs` uses those table-local seeks, so the
  old row-scan regression has been partially removed for latest/borrowed point
  reads.
- Storage still does not restore old source pruning. The current point
  candidate loop seeks every owned immutable table in every owned level, even
  though L1+ levels are non-overlapping and should allow one candidate table
  per level.
- Storage inherited point reads also miss level pruning:
  `collect_visible_inherited_point_candidates` in
  `crates/storage/src/branch/read.rs` rewrites the physical key to the
  source branch and then seeks every inherited table in every inherited level.
- Storage's captured read-view history path still contains a row-scan
  point candidate helper. `BranchReadView::history` calls
  `BranchReadView::point_candidates` in
  `crates/storage/src/branch/read.rs`; that helper filters active,
  frozen, owned, and inherited rows by physical key. History may need multiple
  versions, but it should still seek the target key within each source rather
  than scan unrelated rows.

Diagnosis:

- Point-read parity is partial, not missing. Storage has the ordered
  table-local seek building blocks, and latest point reads use them.
- The missing mechanic is old-storage source-level pruning:
  - L0: probe all tables newest-first because ranges may overlap.
  - L1+: binary-search sorted non-overlapping table ranges and probe at most
    one table per level.
  - Inherited layers: apply the same level-aware lookup after branch-key
    rewrite and inherited read-bound handling.
- If all flushed tables are still stuck in L0, point reads will still probe all
  L0 tables. That is a compaction/maintenance failure, not a point-read planner
  failure. The point-read fix needs level-aware pruning, and the compaction
  audit must separately prove sustained load moves tables out of L0.

Required proof:

- Add counters by source class: active probes, frozen probes, L0 table probes,
  nonzero-level searches, nonzero-level table probes, inherited L0 probes,
  inherited nonzero-level probes, and row visits.
- Add a 10M assertion target: nonzero-level probes should be bounded by level
  count, not by flushed-table count.
- Compare against old `get_versioned_from_branch` and
  `point_lookup_level_preencoded`.
- Add a history/as-of assertion target: history for one key must not visit rows
  whose physical key differs from the requested key.
- Preserve branch inheritance semantics while pruning: source branch rewrite,
  fork visibility cap, tombstone visibility, and child-owned shadowing must be
  covered by differential tests.

### 3. Scan Source Planning And Iterator Behavior

Status: `Partial`

Old invariant:

- Build a merge iterator over logical sources, not every physical file in all
  nonzero levels.
- L0 uses individual segment iterators because it can overlap.
- L1+ uses `LevelSegmentIter`: binary-search to the relevant segment, open only
  that segment, and advance to later segments lazily.
- Range scans push down the start key and limit.

Old reference files:

- `crates/storage/src/merge_iter.rs`
- `crates/storage/src/seekable.rs`
- `crates/storage/src/segment.rs`
- `crates/storage/src/segmented/mod.rs`

Storage files to audit:

- `crates/storage/src/branch/read.rs`
- `crates/storage/src/table/cursor.rs`
- `crates/storage/src/table/reader.rs`

Findings:

- The benchmarked old scan path uses `StorageIterator` in
  `crates/storage/src/segmented/mod.rs`, not the older vector-returning
  snapshot scan path.
- `StorageIterator` builds a persistent seekable pipeline on first seek and
  reuses that pipeline across later seeks. This mirrors RocksDB's
  `MergingIterator` shape.
- The old seekable pipeline lives in `crates/storage/src/seekable.rs`:
  `SeekableIterator`, `MemtableSeekableIter`, `SegmentSeekableIter`,
  `LevelSeekableIter`, `MergeSeekableIter`, `MvccSeekableIter`, and
  `RewritingSeekableIter`.
- Old scan source planning in `StorageIterator::build_seekable_pipeline`
  creates:
  - one memtable seekable iterator for active;
  - one memtable seekable iterator per frozen table;
  - one `SegmentSeekableIter` per L0 segment because L0 can overlap;
  - one `LevelSeekableIter` per nonzero level because L1+ is sorted and
    non-overlapping;
  - inherited equivalents wrapped in `RewritingSeekableIter`.
- Old `LevelSeekableIter` wraps `LevelSegmentIter` from
  `crates/storage/src/segment.rs`. It binary-searches level key ranges, opens
  only the relevant segment, and advances to later segments lazily.
- Old `MergeSeekableIter::seek` re-seeks existing children in place and
  re-heapifies. It does not rebuild all physical table cursors for every scan
  request.
- Storage latest public scans do use the borrowed runtime path:
  `StorageRuntime::scan_prefix` and `StorageRuntime::scan_range` in
  `crates/storage/src/api/runtime.rs` call
  `scan_latest_including_tombstones_for_branch`, which eventually calls
  `BranchLocalState::scan_including_tombstones_borrowed`.
- Storage borrowed scan uses `scan_including_tombstones_from_sources` in
  `crates/storage/src/branch/read.rs`.
- `scan_cursors_for_sources` currently creates a `BoundedTableCursor` over
  active, every frozen table, every owned table in every level, and every
  inherited table in every inherited level.
- `scan_including_tombstones_from_sources` then calls `seek_to_first` on every
  cursor before merge begins.
- `MergeTableCursor` in `crates/storage/src/table/cursor.rs` can merge
  child cursors, but it also seeks every child on `seek_to_first`/`seek`. It is
  not a level iterator and does not know how to binary-search nonzero table
  ranges.
- Storage table metadata is sufficient for restoration:
  `TableRuntimeFacts::key_range` in `crates/storage/src/table/facts.rs`
  records first/last internal keys, and branch validation already enforces
  sorted, non-overlapping nonzero levels.
- The captured `BranchReadView` bounded/as-of scan path is worse than the
  borrowed latest path. `collect_matching_scan_candidates` still scans rows
  into a `BTreeMap` by filtering active, frozen, owned, and inherited rows.

Evidence:

- Storage 10M standard scan-prefix performed `1,010,000`
  `scan_cursor_seeks` for `10,000` scan samples, with
  `branch_scan_source_setup_ns=3,008,512,152` and
  `branch_scan_merge_ns=433,583,048`.
- Storage 10M standard scan-range performed `1,010,000`
  `scan_cursor_seeks` for `10,000` scan samples, with
  `branch_scan_source_setup_ns=3,250,675,313` and
  `branch_scan_merge_ns=524,067,767`.
- Old 10M cache scan-prefix performed `10,000`
  `storage_iterator_seeks` for `10,000` scan samples and returned `640,000`
  rows.
- Old 10M cache scan-range performed `10,000`
  `storage_iterator_seeks` for `10,000` scan samples and returned `640,000`
  rows.

Diagnosis:

- Scan parity is partial. Storage has sorted table cursors, bounded
  cursors, heap merge, MVCC selection, inherited branch rewrite, and a borrowed
  latest path.
- The missing mechanic is the old seekable source hierarchy:
  - persistent scan pipeline;
  - one source per nonzero level instead of one source per physical table;
  - level key-range binary search;
  - lazy transition to the next table in the level;
  - inherited level iteration with branch-key rewriting.
- Source setup is currently the dominant 10M scan cost. Local costs remain
  visible after setup is fixed, especially row clones, logical-key encoding,
  and repeated bound checks, but those are secondary to the lost level iterator
  architecture.
- This should not be fixed by adding a special benchmark fast path. The normal
  scan machinery should adopt the old seekable source shape.

Required proof:

- Add counters by scan source class: active seeks, frozen seeks, L0 table seeks,
  nonzero-level seeks, nonzero-level table opens, inherited L0 table seeks, and
  inherited nonzero-level seeks.
- Add setup counters for number of physical tables considered, number of
  physical cursors opened, number of level cursors opened, and number of tables
  skipped by range/prefix overlap.
- Restore a storage equivalent of old `SeekableIterator`/`LevelSeekableIter`
  inside the standard branch scan machinery, not as a benchmark-specific API.
- Preserve current branch scan semantics: latest/as-of bounds, tombstone
  inclusion for API mapping, TTL filtering, inherited branch rewrite, fork
  version caps, and child-owned shadowing.
- Add a 10M assertion target: scan seeks should be bounded by active + frozen +
  L0 table count + nonzero level count + inherited equivalent, not by total
  table count.
- Add a bounded/as-of read-view target: scan should not row-scan unrelated rows
  when ordered table cursors can seek to the requested range.

### 4. Compaction Selection, Output Shape, And Installation

Status: `Partial`

Old invariant:

- Compaction has scoring and level selection.
- L0 to L1 compaction merges all L0 with overlapping L1 and preserves
  non-overlapping L1.
- Nonzero compaction chooses an input table, merges overlapping next-level
  tables, and installs sorted non-overlapping output.
- Compaction can split outputs by target file size.
- Pick-and-compact keeps maintenance moving across levels.

Old reference files:

- `crates/storage/src/segmented/compaction.rs`
- `crates/storage/src/segment_builder.rs`
- `crates/storage/src/segmented/tests/leveled.rs`

Storage files to audit:

- `crates/storage/src/branch/state/compaction.rs`
- `crates/storage/src/table/compaction.rs`
- `crates/storage/src/lifecycle/compaction.rs`
- `crates/storage/src/lifecycle/rewrite_publication.rs`

Findings:

- Old storage's leveled compaction logic is concentrated in
  `crates/storage/src/segmented/compaction.rs`.
- Old `recalculate_level_targets` computes dynamic byte targets for L1+ using
  a RocksDB-style base-level calculation. L0 is scored by file count; L1+ are
  scored by bytes over target.
- Old `compute_compaction_scores` builds per-level scores, sorts them
  descending, and old `pick_and_compact` runs the highest-scored eligible level.
  This keeps maintenance moving beyond L0.
- Old `compact_l0_to_l1` snapshots the current branch version, merges all L0
  files with overlapping L1 files, preserves non-overlapping L1 files, installs
  new L1 outputs sorted by key range, writes the manifest, and only then
  reclaims old files.
- Old `compact_level` chooses one nonzero input by compact pointer, merges
  overlapping next-level files, can do a trivial move when safe, splits output
  with grandparent-overlap awareness, advances the compact pointer, writes the
  manifest, and then deletes/quarantines obsolete files.
- Old output construction uses `SplittingSegmentBuilder` from
  `crates/storage/src/segment_builder.rs`, including
  `build_split_with_predicate` for grandparent-aware splits.
- Old behavior is covered heavily by
  `crates/storage/src/segmented/tests/leveled.rs`: L0 to L1 overlap and
  non-overlap cases, concurrent flush handling, repeated compaction, recovery,
  L1 to L2, round-robin selection, trivial move, grandparent splitting, and
  multi-level scan correctness.
- Storage has branch-local compaction primitives in
  `crates/storage/src/branch/state/compaction.rs`:
  `CompactL0`, `CompactL0ToLevelOne`, and `CompactLevel`.
- Storage `plan_l0_to_level_one_compaction` does merge all L0 tables with
  overlapping L1 tables and installs sorted L1 outputs.
- Storage `plan_nonzero_level_compaction` can merge one requested table
  with overlapping next-level tables, but the caller must pass a concrete
  `table_index`.
- Storage `TableCompactor` in
  `crates/storage/src/table/compaction.rs` can split output by target
  output bytes and avoids splitting within the same physical key.
- Storage installation removes compacted tables by stale table reference,
  inserts outputs into the target level, validates level invariants, and updates
  runtime facts.
- Storage lifecycle compaction in
  `crates/storage/src/lifecycle/compaction.rs` only suggests compaction
  from L0 table-count pressure. It does not compute byte targets, score L1+,
  or choose the highest-pressure level.
- Storage maps suggested nonzero compaction tasks to `table_index: 0`;
  there is no compact pointer or round-robin selection equivalent.
- Storage public runtime compaction currently maps maintenance compaction
  to L0-to-L1, so explicit public maintenance does not naturally compact deeper
  levels.
- Storage nonzero compaction returns `NotEnoughInputTables` for a single
  non-overlapping input and therefore does not preserve the old trivial-move
  optimization.
- Storage table compaction materializes all input rows into memory and
  sorts them before output construction. The old path used streaming segment
  sources, `MergeIterator`, `CompactionIterator`, and split builders.
- Storage output splitting is target-size based, but there is no audited
  equivalent of old grandparent-overlap split predicates.

Evidence:

- The 10M benchmark load path force-flushed every 100K rows because no-flush
  hit the mutable budget, producing roughly 100 immutable sources.
- Storage point reads and scans still observed about 100 source probes or
  seeks after load, which is consistent with insufficient compaction scheduling
  or incomplete deeper-level advancement.
- Cache and standard storage modes showed the same fanout profile, so this
  is a core lifecycle/compaction issue rather than a cache-only serving issue.
- The audit has not yet recorded level counts after load and after maintenance
  drain, so the exact split between "compaction not run" and "compaction only
  ran into shallow levels" still needs direct proof.

Diagnosis:

- Compaction parity is partial. Storage has the branch-local operations
  needed to compact L0 to L1 and to compact a selected nonzero table forward.
- The missing old mechanic is the compaction control loop: dynamic level
  targets, scoring, highest-score selection, nonzero compact pointers,
  round-robin input choice, trivial moves, and grandparent-aware output
  splitting.
- This gap is large enough to explain source-fanout scaling failures at 1M+ and
  10M+ rows. If flushed tables are not driven through the leveled structure,
  point reads and scans are forced to touch many physical sources even after
  the read path itself is fixed.
- The fix should restore the old leveled maintenance methodology inside
  storage lifecycle/branch compaction. It should not add benchmark-only
  fast paths or bypass storage's table/branch abstractions.

Required proof:

- Record compaction input/output levels and table counts under sustained load.
- Verify repeated maintenance drains L0 and advances data to deeper levels.
- Verify output table ranges are sorted and non-overlapping for all nonzero
  levels.
- Compare row pruning and tombstone retention behavior against old compaction.
- Add counters for scored levels, selected level, input table count, overlap
  table count, output table count, output bytes, rows materialized, rows
  emitted, output splits, trivial moves, and grandparent-triggered splits.
- Add level-count diagnostics after load and after maintenance drain: L0 table
  count, per-level table count, per-level bytes, and owned table count.
- Port the old `leveled.rs` coverage before calling this restored:
  L0-to-L1 overlap/non-overlap, repeated compaction, concurrent flush
  preservation, manifest/recovery publication, L1-to-L2, round-robin selection,
  trivial move, grandparent split behavior, and multi-level scan correctness.

### 5. MVCC, Tombstone, TTL, And History Semantics

Status: `Partial`

Old invariant:

- Internal keys sort newest commit first for the same logical key.
- MVCC iteration emits the newest visible row per logical key.
- Tombstones hide older values for normal reads but are preserved where history
  or retention requires them.
- TTL is evaluated at read time and during pruning decisions.
- History reads preserve version order and tombstone/expiry metadata.

Old reference files:

- `crates/storage/src/memtable.rs`
- `crates/storage/src/merge_iter.rs`
- `crates/storage/src/ttl.rs`
- `crates/storage/src/compaction.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/tests/basic.rs`
- `crates/storage/src/segmented/tests/resurrection.rs`

Storage files to audit:

- `crates/storage/src/api/runtime.rs`
- `crates/storage/src/api/tests/read.rs`
- `crates/storage/src/commit/batch.rs`
- `crates/storage/src/branch/read.rs`
- `crates/storage/src/branch/pruning.rs`
- `crates/storage/src/table/compaction.rs`
- `crates/storage/src/branch/tests/read_view.rs`
- `crates/storage/src/branch/tests/row_pruning/`
- `crates/storage/src/format/storage_row.rs`

Findings:

- Old `Memtable` in `crates/storage/src/memtable.rs` stores entries ordered by
  `(typed key asc, commit id desc)`. Point reads seek to `(key, MAX)` and return
  the first entry whose commit id is within the snapshot.
- Old `MvccIterator` in `crates/storage/src/merge_iter.rs` merges sorted
  sources, groups by typed key prefix, skips versions newer than the requested
  snapshot, and emits the first visible row for each logical key.
- Old normal reads and scans call `entry.is_tombstone || entry.is_expired()` and
  suppress the key when the selected latest visible row is a tombstone or has
  expired.
- Old timestamp reads use `is_expired_at(max_timestamp)` and also suppress
  fallthrough: if the newest row visible at that timestamp is expired or a
  tombstone, older values are not returned.
- Old `get_history` uses ordered per-source key seeks to collect only versions
  for the requested key, sorts newest-first, de-duplicates duplicate commit ids,
  applies `before_version`, filters expired rows using wall-clock
  `is_expired()`, and returns tombstones as historical entries.
- Old `CompactionIterator` in `crates/storage/src/compaction.rs` keeps all rows
  above the retention floor plus at most one below-floor survivor per key. It
  preserves below-floor tombstones in non-bottommost compaction, can elide them
  at bottommost, and only drops expired TTL rows in bottommost compaction below
  the prune floor.
- Old `TTLIndex` in `crates/storage/src/ttl.rs` supports efficient expired-key
  lookup, but `SegmentedStore::expire_ttl_keys` is a no-op; segmented storage
  handles TTL at read time and during compaction.
- Storage `encode_internal_key` in
  `crates/storage/src/format/key.rs` appends the bitwise inverse commit
  version in big-endian order, so ascending byte order returns newest versions
  first for the same physical key.
- Storage `StorageRow` stores an absolute `expires_at` timestamp. Public
  `CommitMutation::Put { ttl }` is converted in
  `crates/storage/src/api/runtime.rs` through `map_expiry`, which adds the
  TTL duration to the actual commit timestamp before the internal row is
  stamped in `crates/storage/src/commit/batch.rs`.
- Storage tombstones are stricter on disk than old rows:
  `decode_storage_row` rejects tombstones with value bytes or expiry facts.
- Storage at-version and at-timestamp API reads resolve the selected
  commit frontier to a timestamp before applying TTL. The API tests in
  `crates/storage/src/api/tests/read.rs` cover TTL behavior for
  at-version, at-timestamp, and scans.
- Storage `BranchReadView` tests in
  `crates/storage/src/branch/tests/read_view.rs` cover timestamp
  tombstone boundaries, TTL boundaries, history ordering, tombstone inclusion,
  tombstone filtering, and inherited history behavior.
- Storage compaction pruning is proof-gated in
  `crates/storage/src/branch/pruning.rs`. Tombstone and TTL elision require
  retention floors, timestamp coverage, no readable inherited layers, shared
  table safety, recovery-health attestation, and bottommost candidate checks.
- Storage row-pruning tests cover tombstone resurrection risk, bottommost
  tombstone elision, non-bottommost tombstone preservation/rejection,
  TTL cutoff safety, inherited-layer safety, materialized tombstone safety, and
  max-version retention.

Evidence:

- Storage has direct tests for the old core semantics:
  `branch_read_view_timestamp_tombstones_suppress_fallthrough`,
  `branch_read_view_timestamp_ttl_boundaries_suppress_fallthrough`,
  `branch_read_view_history_preserves_tombstones_limits_and_before_version`,
  `read_at_version_applies_ttl_at_selected_frontier`, and
  `scan_at_version_applies_ttl_at_selected_frontier`.
- Storage also has proof tests under
  `crates/storage/src/branch/tests/row_pruning/required_plan.rs` and
  `crates/storage/src/branch/tests/row_pruning/tombstone_ttl.rs` that cover
  many old compaction-retention safety cases.
- The storage tests intentionally assert that latest reads do not invent a
  wall-clock timestamp. In
  `branch_read_view_timestamp_ttl_boundaries_suppress_fallthrough`, latest
  returns an otherwise expired-looking TTL row because no timestamp bound was
  supplied.

Diagnosis:

- MVCC ordering and tombstone suppression are mostly preserved.
- Storage's TTL representation changed from old `{write timestamp,
  ttl_ms}` to absolute `expires_at`. That representation is acceptable if the
  public API computes expiry from the commit timestamp, which it does.
- Latest-read TTL semantics are not old-engine parity. Old `get_versioned`,
  scans, and history used wall-clock `entry.is_expired()` for unbounded/latest
  reads. Storage latest point reads and latest scans pass no selected
  timestamp, so `row_is_expired_at` does not filter expired rows.
- History TTL semantics also differ. Old `get_history` filtered expired rows
  with `entry.is_expired()`. Storage API history maps all rows from
  `BranchReadView::history` and preserves `expires_at` facts without filtering
  by wall clock.
- Captured storage history is still inefficient. `BranchReadView::history`
  calls `point_candidates`, and `point_candidates` scans active, every frozen
  table, every owned table, and inherited tables to filter rows matching the
  key. Old `get_all_versions_from_snapshot` sought each source to the requested
  key and stopped once the physical key changed.
- This audit does not decide whether the new latest/history TTL behavior is a
  product change or an accidental rewrite drift. It must be decided explicitly
  before implementation work.

Required proof:

- Add differential old-vs-storage tests for:
  put/delete/put resurrection, latest tombstone suppression, as-of tombstone
  suppression, latest TTL expiration, as-of TTL expiration, history ordering,
  history tombstone inclusion/filtering, and history TTL filtering.
- Decide and document latest TTL semantics. If storage should match old
  storage, latest reads/scans/history need a runtime wall-clock timestamp and
  must suppress expired rows without falling through to older values.
- If storage intentionally exposes expired rows for latest/history, update
  API docs and differential expectations so this is an explicit product
  decision, not an accidental parity gap.
- Replace captured `BranchReadView::history` source collection with ordered
  key seeks over active/frozen/owned/inherited sources. It should not scan rows
  whose physical key differs from the requested key.
- Add history perf counters: active rows visited, frozen rows visited, owned
  rows visited, inherited rows visited, source seeks, history rows emitted, and
  rows skipped by before-version/tombstone/TTL filters.
- Keep proof-gated pruning behavior, but add an old-case matrix that maps every
  old `CompactionIterator` tombstone/TTL/max-version test to a storage
  pruning test or an intentional behavior difference.

### 6. Branch Inheritance, Fork, And Materialization Mechanics

Status: `Partial`

Old invariant:

- Child branches inherit parent sources at a fork frontier.
- Inherited reads rewrite source branch keys to child branch keys.
- Fork visibility caps inherited rows at the fork version.
- Child-owned writes shadow inherited rows.
- Materialization converts inherited rows into child-owned rows without changing
  visible results.

Old implementation details:

- `crates/storage/src/merge_iter.rs` uses `RewritingIterator` for inherited
  scans. It skips rows newer than the fork version and rewrites inherited
  source branch keys to the child branch before MVCC grouping.
- `crates/storage/src/segmented/mod.rs` `fork_branch` flushes source memtables,
  holds an exclusive source guard while capturing the source max applied
  version, captures source-owned segments as the nearest inherited layer, copies
  existing inherited layers, increments shared segment refcounts, and attaches
  the layers to an empty child branch.
- Old fork intentionally resets copied inherited layers with `Materializing`
  status back to `Active` so a parent-side materialization does not permanently
  block a newly forked child.
- `materialize_layer` flushes the child branch first, marks the inherited layer
  `Materializing`, publishes that status, collects unshadowed inherited rows,
  sorts them, writes child-owned L0 replacement segments, removes the inherited
  layer by `(source_branch_id, fork_version)`, publishes the manifest, and
  rolls back or cleans up created segments on failure.
- `collect_unshadowed_entries` skips inherited rows above the fork version,
  rewrites rows to the child branch, skips rows shadowed by child-owned sources
  or closer inherited layers, and treats shadow-check corruption
  conservatively to avoid resurrecting stale inherited data.

Storage findings:

- `crates/storage/src/branch/read.rs` has explicit
  `BranchInheritedLayer` and `InheritedLayerDescriptor` objects. Validation
  checks table count, unique table identities, unique internal keys, source
  branch identity, source-branch row ownership, and `row.commit_version <=
  fork_version`.
- `crates/storage/src/branch/state/fork.rs` `fork_into_empty_child`
  requires the source branch to have no active or frozen rows, derives
  `fork_version` from the source retained rows, creates a nearest inherited
  layer from source-owned tables, and appends forkable inherited layers from the
  source branch.
- Storage preserves `Materializing` status when cloning inherited layers
  for a child. That differs from old storage's reset-to-`Active` behavior and
  needs an explicit architecture decision or recovery proof.
- Inherited reads use `BranchEffectiveReadBound::for_inherited_layer` to cap
  visible inherited rows at the fork version. Point reads rewrite the requested
  child key to each source branch, seek inherited tables, then rewrite matching
  rows back to the child branch. Scans create inherited cursors over source
  bounds and rewrite inherited rows before grouping.
- `InheritedLayerStatus::Active` and `Materializing` are readable;
  `Materialized` layers are skipped by reads; `Unavailable` layers are rejected
  by validation paths.
- `crates/storage/src/branch/state/materialization.rs` materialization
  includes higher-precedence rows from child active rows, frozen tables,
  child-owned tables, and closer inherited layers. It rewrites target inherited
  rows to the child branch, skips post-fork rows, skips exact duplicates,
  rejects non-identical internal-key collisions, sorts output rows, builds L0
  replacement tables, installs them behind ordinary child-owned L0 tables, and
  removes the inherited source layer.
- Existing storage tests under
  `crates/storage/src/branch/tests/inheritance_materialization/` cover
  inherited point/scans/history, fork gates, tombstones, timestamps, layer
  ordering, materialization retry, replacement collision handling, empty
  materialization, and materialized scan parity.

Diagnosis:

- Branch-local COW inheritance is not missing. The core visibility mechanics
  are represented and tested in storage.
- Parity is still partial because storage inherited point reads and scans
  inherit the same source-fanout scaling problems found in earlier audit steps:
  they still seek or cursor across every reachable table instead of using the
  old LSM range/level pruning shape.
- The copied inherited-layer status behavior is a semantic drift from the old
  implementation. Keeping `Materializing` readable may be valid, but it needs a
  deliberate lifecycle/recovery invariant rather than an accidental divergence.
- The branch-local materialization shadow check includes active and frozen
  child rows, so that specific old flush-before-shadow invariant has an
  in-memory equivalent. The durable path still needs to prove that concurrent
  publication, recovery, and manifest replay cannot expose stale inherited rows
  or orphan replacement tables.
- Old storage's shared segment refcounting, manifest rollback, orphan cleanup,
  and crash-recovery behavior are not proven by this section. Those mechanics
  must be audited in the durability/manifest/WAL/recovery step.

Old reference files:

- `crates/storage/src/merge_iter.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/tests/fork.rs`
- `crates/storage/src/segmented/tests/materialize.rs`

Storage files to audit:

- `crates/storage/src/branch/read.rs`
- `crates/storage/src/branch/state/fork.rs`
- `crates/storage/src/branch/state/materialization.rs`
- `crates/storage/src/branch/tests/inheritance_materialization/`
- `crates/storage/src/lifecycle/compaction.rs`

Required proof:

- Map old `fork.rs` and `materialize.rs` cases one-for-one to storage
  tests, especially fork chains, copied materialization status, parent
  compaction after fork, child shadowing, inherited tombstones, inherited TTL,
  materialization retry, and materialized scan parity.
- Add a deliberate test or design note for copied `Materializing` inherited
  layers: either normalize them to `Active` like old storage or prove why
  readable `Materializing` layers cannot block/poison child recovery.
- Add perf counters for inherited point seeks, inherited scan cursors,
  inherited rows visited, inherited rows rewritten, and branch-key rewrite cost.
- Verify durable materialization publication and recovery in the next audit
  step, including partial output, manifest sync failure, and orphan cleanup.

### 7. Durability, Manifest, WAL, And Recovery Mechanics

Status: `Partial`

Old invariant:

- WAL replay, checkpoint recovery, manifest recovery, and snapshot install
  reconstruct the same branch-visible state.
- Durable compaction publication does not expose partial outputs.
- Recovery preserves levels and branch inheritance metadata.
- Retention and quarantine do not delete reachable tables.

Old implementation details:

- `crates/storage/src/durability/recovery_bootstrap.rs`
  `run_storage_recovery` validates or creates the durable storage manifest,
  opens `SegmentedStore::with_dir`, replays checkpoint/WAL state through the
  recovery coordinator, optionally applies lossy WAL fallback, and then calls
  `SegmentedStore::recover_segments()`.
- `crates/storage/src/durability/recovery_bootstrap.rs`
  `prepare_storage_manifest_for_recovery` only creates a missing `MANIFEST`
  for primary storage. Follower recovery may create the segments directory, but
  it does not create the primary manifest.
- `crates/storage/src/durability/recovery_bootstrap.rs`
  `complete_storage_recovery_after_replay` folds in
  `storage.current_version()`, applies runtime config, runs segment recovery,
  and reports segment-only versions through the recovery result.
- `crates/storage/src/durability/checkpoint_runtime.rs`
  `run_storage_checkpoint` persists the active WAL segment before publishing a
  snapshot, writes the checkpoint snapshot through the coordinator, and only
  then updates the durable manifest snapshot watermark.
- `crates/storage/src/durability/checkpoint_runtime.rs`
  `truncate_storage_wal_after_flush` computes a global flush watermark only
  from branches that have flushed segments. Branches without flushed segments
  remain WAL-covered.
- `crates/storage/src/durability/format/manifest.rs` stores the physical
  storage manifest: database UUID, codec id, active WAL segment, snapshot id
  and watermark, flushed-through commit id, version marker, and CRC.
- `crates/storage/src/durability/format/wal_record.rs` defines the v3 WAL
  segment envelope, segment naming, per-record CRC, database UUID validation,
  and segment number validation.
- `crates/storage/src/durability/decoded_snapshot_install.rs` validates decoded
  snapshot row groups before mutation. It rejects empty groups, empty space,
  zero versions, duplicate row identities, and verifies the installed row
  count.
- `crates/storage/src/segmented/mod.rs` `recover_segments` walks
  `segments_dir/<branch_id>/`, loads per-branch `segments.manifest` files,
  reserves segment ids, reconstructs branch levels, rebuilds branch max
  versions, resolves inherited layers in a second pass, rebuilds shared segment
  references from inherited layers, reconciles quarantine, and publishes
  `last_recovery_health`.
- `crates/storage/src/segmented/mod.rs` treats a valid segment manifest as
  authoritative. Orphan `.sst` files not listed in the manifest are skipped so
  stale files cannot be resurrected after a crash or failed publish.
- `crates/storage/src/segmented/mod.rs` uses no-manifest fallback only as a
  backward-compatibility downgrade. It promotes discovered segments to L0 and
  records `NoManifestFallbackUsed`.
- `crates/storage/src/segmented/recovery.rs` classifies recovery faults as
  corrupt segments/manifests, missing manifest-listed segments, inherited layer
  loss, no-manifest fallback, IO failure, or quarantine inventory mismatch.
- `crates/storage/src/segmented/quarantine_protocol.rs` only reclaims files
  after manifest proof. It refuses reclaim when recovery health has data loss,
  policy downgrade, or non-telemetry debt; publishes quarantine inventory with
  temp/write/fsync/rename/fsync-dir ordering; and prefers retention on reopen
  reconciliation mismatch.

Storage findings:

- `crates/storage/src/lifecycle/recovery.rs`
  `LifecycleRecoveryRuntime::recover` recovers checkpoint state, quarantine
  state, table manifests, flush watermark recoverability, WAL tail state, and
  recovery health before installing checkpoint or table-manifest state.
- `crates/storage/src/lifecycle/recovery.rs` chooses the replay start from
  the max of checkpoint watermark and flush watermark. When both checkpoint and
  table manifest are available, table manifest state may be used as the base if
  it covers checkpoint rows and the flush watermark is newer.
- `crates/storage/src/lifecycle/recovery.rs` currently rejects nonzero
  table-object recovery references in `LifecycleRecoveryRequest::validate` and
  `recover_tables`. That is an explicit incomplete recovery path.
- `crates/storage/src/lifecycle/recovery.rs` has strict/lossy health
  behavior. Strict recovery must not return degraded health; lossy recovery can
  repair the latest WAL tail, record faults, and continue when the error class
  is allowed.
- `crates/storage/src/service/wal.rs` validates codec identity, segment
  size, append offsets, append lengths, object sizes, and read ordering. Tail
  repair is limited to the latest active segment, and uncertain repair blocks
  append until reopen.
- `crates/storage/src/service/checkpoint.rs` preserves the old
  checkpoint ordering shape: persist active WAL segment, publish snapshot, then
  persist snapshot facts in the durable manifest. It classifies partial
  snapshot publish and final manifest uncertainty.
- `crates/storage/src/service/snapshot.rs` validates snapshot identity,
  database id, codec id, nonzero watermark, section count limits, and snapshot
  bytes before publishing or loading sections.
- `crates/storage/src/lifecycle/table_manifest.rs` is the storage
  table topology manifest layer. It records branch-owned levels, inherited
  layers, table object facts, table provenance, retained-history extension, and
  durable table catalog sequence.
- `crates/storage/src/branch/state/manifest_recovery.rs`
  `install_table_manifest_recovery` requires empty branch state, validates
  compaction levels, inherited layers, duplicate table identities, reachability
  snapshot, and read view before swapping recovered state in.
- `crates/storage/src/lifecycle/rewrite_publication.rs` publishes durable
  rewrite outputs first, records them in a cloned durable table catalog,
  installs the branch mutation, updates the catalog, and then publishes the
  table manifest. If table manifest publication fails after the branch install,
  the outcome carries manifest debt instead of rolling back the installed
  branch state.
- `crates/storage/src/service/quarantine.rs`,
  `crates/storage/src/service/quarantine/mutation.rs`,
  `crates/storage/src/service/quarantine/reconcile.rs`, and
  `crates/storage/src/lifecycle/quarantine.rs` implement branch-scoped
  quarantine inventories, durable quarantine copy/delete ordering,
  inventory reconciliation, purge tokens, and recovery-health gates.
- Storage has broad tests under
  `crates/storage/src/lifecycle/tests/recovery.rs`,
  `crates/storage/src/lifecycle/tests/table_manifest_recovery.rs`,
  `crates/storage/src/lifecycle/tests/checkpoint.rs`,
  `crates/storage/src/lifecycle/tests/durable.rs`,
  `crates/storage/src/service/wal/tests/`,
  `crates/storage/src/service/checkpoint/tests/`,
  `crates/storage/src/service/snapshot/`, and
  `crates/storage/src/service/quarantine/tests/`.

Diagnosis:

- Durability is not missing in storage. WAL, checkpoint, snapshot,
  manifest, table-manifest, recovery-health, and quarantine services exist and
  have meaningful unit and lifecycle coverage.
- Parity is still partial because storage changed the durable topology
  source of truth. Old storage recovered branch topology by walking
  `segments_dir/<branch_id>/segments.manifest`; storage recovers through a
  database manifest, branch catalog facts, per-branch table manifests, table
  object facts, and WAL/checkpoint rows. That can be correct, but it must be
  proven against the old fault windows.
- The most concrete missing piece is table-object reference recovery:
  `LifecycleRecoveryRequest` rejects nonzero table object references. Until
  that is implemented or proven obsolete, storage cannot claim full
  table-backed durable recovery parity.
- Durable rewrite publication has different failure semantics from old segment
  publish. Old storage treated the branch manifest as the authoritative segment
  visibility set and skipped orphan files. Storage may install branch
  output before table-manifest publication completes and then report manifest
  debt. That needs crash/reopen tests proving unmanifested outputs are either
  recovered correctly or retained as unreachable orphans without affecting
  reads.
- Old recovery rebuilt shared segment references from inherited layers.
  Storage replaces that with durable table catalog, reachability,
  retained-history,
  and quarantine gates. This is architecturally reasonable, but parity requires
  proof that owned, inherited, materialization-replacement, and
  checkpoint-restored table objects cannot be deleted while reachable.
- Storage checkpoint recovery is row-section based rather than table
  object based. That is acceptable for checkpoint rows, but it does not remove
  the need for table-object manifest recovery when durable tables are the
  post-flush source of truth.
- Quarantine mechanics are present, but full parity needs an all-branch reopen
  proof. The inspected recovery hook is branch-scoped; the test matrix must
  prove active, deleted, forked, materialized, and inherited branches all
  participate in quarantine/reachability reconciliation.

Old reference files:

- `crates/storage/src/durability/wal/`
- `crates/storage/src/durability/checkpoint_runtime.rs`
- `crates/storage/src/durability/recovery_bootstrap.rs`
- `crates/storage/src/durability/recovery.rs`
- `crates/storage/src/durability/format/manifest.rs`
- `crates/storage/src/durability/format/wal_record.rs`
- `crates/storage/src/durability/decoded_snapshot_install.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/recovery.rs`
- `crates/storage/src/segmented/quarantine_protocol.rs`
- `crates/storage/src/segmented/tests/materialize.rs`
- `crates/storage/src/segmented/tests/post_restart_branch.rs`
- `crates/storage/src/segmented/tests/quarantine_reconciliation.rs`
- `crates/storage/src/segmented/tests/publish_failures.rs`

Storage files to audit:

- `crates/storage/src/service/wal.rs`
- `crates/storage/src/service/checkpoint.rs`
- `crates/storage/src/service/snapshot.rs`
- `crates/storage/src/service/manifest.rs`
- `crates/storage/src/lifecycle/recovery.rs`
- `crates/storage/src/lifecycle/checkpoint.rs`
- `crates/storage/src/lifecycle/table_manifest.rs`
- `crates/storage/src/lifecycle/rewrite_publication.rs`
- `crates/storage/src/lifecycle/health.rs`
- `crates/storage/src/lifecycle/quarantine.rs`
- `crates/storage/src/branch/state/manifest_recovery.rs`
- `crates/storage/src/service/quarantine.rs`
- `crates/storage/src/service/quarantine/mutation.rs`
- `crates/storage/src/service/quarantine/reconcile.rs`

Required proof:

- Restart differential tests after commit, flush, compaction, branch fork,
  branch materialization, checkpoint, WAL truncation, branch clear/delete, and
  quarantine/purge.
- Fault-window tests around durable rewrite output publish, branch install,
  table catalog update, table manifest sync, checkpoint snapshot publish, and
  durable manifest sync.
- Implement or explicitly remove the `table_object_references` recovery gap.
  If removed, document why table manifests/catalog facts are the complete
  replacement.
- Add a reopen invariant: branch-owned levels, inherited layers,
  materialization replacement provenance, retained-history tables, and visible
  read results must match before/after restart.
- Add a reachability invariant: no table object reachable from a live branch,
  inherited layer, retained-history extension, checkpoint, or quarantine
  inventory can be deleted by orphan cleanup or purge.
- Add an orphan invariant: durable table objects not referenced by the
  authoritative manifest/catalog state are not resurrected after restart.
- Add recovery-health mapping tests from old faults to storage faults:
  corrupt manifest, missing manifest-listed table, corrupt WAL, missing
  checkpoint snapshot, inherited layer loss, no-manifest fallback equivalent,
  quarantine inventory mismatch, and tail repair.
- Add level-count parity before and after recovery, including L0 order and L1+
  non-overlap.

### 8. Cache, Standard, Wasm-None, And Memory Budget Modes

Status: `Partial`

Old invariant:

- Cache and durable serving paths share the same core read/scan algorithms.
- Durable mode adds WAL, checkpoint, manifest, and object persistence around
  the same serving mechanics.
- Memory pressure triggers flush/compaction without changing visible behavior.

Old implementation details:

- `crates/storage/src/durability/wal/mode.rs` defines `DurabilityMode::Cache`,
  `DurabilityMode::Standard`, and `DurabilityMode::Always`. Cache bypasses WAL
  entirely; Standard and Always require WAL persistence and differ only in sync
  policy.
- `crates/storage/src/segmented/mod.rs` uses the same `SegmentedStore` for
  ephemeral and directory-backed storage. `SegmentedStore::new()` has no
  `segments_dir`; `SegmentedStore::with_dir()` and
  `SegmentedStore::with_dir_and_pressure()` add segment persistence and
  pressure tracking but keep the same branch, memtable, segment, read, scan,
  flush, and compaction machinery.
- `crates/storage/src/runtime_config.rs` maps public runtime memory knobs into
  storage-owned settings. With `memory_budget > 0`, old storage derives half
  the budget for block cache, one quarter for active write buffer, and one
  retained immutable memtable so active plus frozen write memory accounts for
  the other half.
- `crates/storage/src/segmented/mod.rs` tracks memtable bytes, frozen count,
  and segment metadata bytes. `pressure_level()` includes segment metadata, not
  just mutable rows, so LSM metadata growth is visible to pressure policy.
- `crates/storage/src/segmented/mod.rs` exposes
  `branches_needing_flush()`, `flush_oldest_frozen()`,
  `max_flushed_commit()`, and `pick_and_compact()`. Mode differences do not
  create separate read/scan implementations.
- Old durable recovery applies `StorageRuntimeConfig` to the recovered
  `SegmentedStore`, so recovered durable state re-enters the same serving
  engine as cache/ephemeral state.

Storage findings:

- `crates/storage/src/lib.rs` documents the public boundary:
  native callers with a database directory should open durable local storage
  through `api::StorageRuntime::open_local(root)`, while volatile storage is
  available only through explicit cache/ephemeral APIs.
- `crates/storage/src/api/options.rs` makes cache intent explicit.
  `StorageOpenOptions::cache()` and `ephemeral()` are non-durable, while
  `StorageOpenOptions::durable_local(policy)` requires a durable backend.
  Object-durable and distributed modes are rejected as unsupported candidates.
- `crates/storage/src/api/runtime.rs` maps public
  `StorageMode::DurableLocal { Standard | Always }` into lifecycle
  `DurableLocalStandard` or `DurableLocalAlways`, and maps cache into lifecycle
  `Cache`.
- `crates/storage/src/api/runtime.rs` opens cache through
  `LifecycleCacheRuntime::open` and durable through
  `LifecycleDurableLocalShell::assemble`, `LifecycleRecoveryRuntime::recover`,
  and `complete_recovery`.
- `crates/storage/src/lifecycle/cache.rs` and
  `crates/storage/src/lifecycle/durable/bootstrap.rs` both serve through
  `BranchLocalState`, branch read views, branch-local commit application, and
  the same table/read modules.
- Cache and durable maintenance are split by runtime:
  `crates/storage/src/lifecycle/cache.rs` supports cache-relevant flush,
  compaction, materialization, and health collection, while durable-only tasks
  such as checkpoint, WAL truncation, retention, quarantine, purge, and repair
  are rejected or deferred at the API boundary.
- `crates/storage/src/lifecycle/durable/maintenance.rs` uses durable
  wrappers around the same branch-level maintenance concepts, adding table
  object publication, table manifest publication, checkpoint, flush watermark,
  WAL truncation, retention, and quarantine.
- `crates/storage/src/lib.rs` forbids `localfs` on `wasm32` at compile
  time. `crates/storage/src/api/runtime.rs` returns an unsupported
  capability error for `open_local` when the `localfs` feature is absent. There
  is no separate public `wasm-none` storage mode; wasm-none is currently a
  target/feature shape where durable-local localfs mechanics are absent and
  cache/basic object mechanics remain available.
- `crates/storage/src/config/mode.rs` allows cache mode to validate
  against browser-like basic object backends without metadata, durable publish,
  durable sync, append, or writer-lock capabilities.
- `crates/storage/src/lifecycle/budget.rs` introduces a pool-based
  `StorageBudgetLedger`: block cache, table reader, active mutable, frozen
  mutable, maintenance queue, generated artifacts, and manifest catalog.
- Storage budget accounting is intentionally partial in V1. The file
  states that table readers, generated artifacts, and manifest catalog work use
  admission checks for one allocation rather than held cumulative reservations.
  Active mutable, frozen mutable, and maintenance queue usage are reported from
  live runtime state.
- `crates/storage/src/lifecycle/compaction.rs`
  `collect_storage_pressure` reports active row count, frozen table count, L0
  table count, owned table count, inherited layer count, and pending
  maintenance. It suggests flush/compaction/materialization tasks at thresholds
  including L0 counts of 2, 4, and 8.
- Storage has tests for explicit cache opens, durable opens, durable-only
  maintenance rejection in cache mode, WAL growth no-op in cache mode, budget
  pool validation, budget rejection before state mutation/publication, low
  memory profiles, and one active-budget parity test between cache and durable.

Diagnosis:

- The public API shape is now closer to the desired product intent than the
  earlier cache-default concern: cache is explicit, durable-local is explicit,
  and `open_local` is documented as the native directory-backed path.
- Serving-mechanics parity is plausible but not proven. Cache and durable both
  use `BranchLocalState` and the same table/read modules, but they have
  separate lifecycle runtimes and separate maintenance wrappers. The audit
  needs differential counters proving that cache and durable have the same
  branch-local source counts, row visits, seeks, and level shapes after
  persistence overhead is excluded.
- wasm-none is not a storage mode with a documented mechanics matrix. It is
  mostly represented by `wasm32`/`localfs` compile gates and cache-capability
  tests. That is acceptable only if the API docs and tests explicitly state
  which durable mechanics are absent and which core serving mechanics remain
  identical.
- Storage budget mechanics are useful guardrails, but they are not the old
  unified memory-budget mapping. Old storage derived write-buffer and block
  cache sizes from one budget and included segment metadata in pressure.
  Storage uses independent pool budgets and partial per-allocation checks.
- Storage pressure reports and suggests maintenance, but this audit did
  not find proof that budget pressure automatically keeps L0 fanout bounded
  under sustained load. Given the benchmark evidence, the missing proof matters:
  pressure suggestions are not enough if callers must explicitly drive enough
  maintenance to drain L0.
- Cache and standard benchmark results showed similar point/scan source
  fanout, which is good evidence that the performance regression is not a
  durable-only mode artifact. It is also evidence that both modes currently
  share the same unresolved LSM maintenance/source-planning problem.

Old reference files:

- `crates/storage/src/durability/wal/mode.rs`
- `crates/storage/src/runtime_config.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/compaction.rs`
- `crates/storage/src/durability/`

Storage files to audit:

- `crates/storage/src/lib.rs`
- `crates/storage/src/api/options.rs`
- `crates/storage/src/api/runtime.rs`
- `crates/storage/src/api/maintenance.rs`
- `crates/storage/src/config/mode.rs`
- `crates/storage/src/lifecycle/cache.rs`
- `crates/storage/src/lifecycle/durable.rs`
- `crates/storage/src/lifecycle/durable/bootstrap.rs`
- `crates/storage/src/lifecycle/durable/maintenance.rs`
- `crates/storage/src/lifecycle/budget.rs`
- `crates/storage/src/lifecycle/maintenance.rs`
- `crates/storage/src/lifecycle/wal_growth.rs`
- `crates/storage/src/lifecycle/tests/cache.rs`
- `crates/storage/src/lifecycle/tests/durable.rs`
- `crates/storage/src/lifecycle/tests/budget.rs`
- `crates/storage/src/lifecycle/tests/budget_runtime.rs`
- `crates/storage/src/lifecycle/tests/maintenance.rs`
- `crates/storage/src/api/tests/maintenance.rs`
- `crates/storage/src/service/cache_mode_absence_tests.rs`

Required proof:

- Cache and standard mode must show the same branch-local counters after
  persistence overhead is excluded: active rows, frozen tables, L0 tables,
  L1+ tables, inherited layers, point source probes, scan source cursors, row
  visits, and output rows.
- Add a cache-vs-durable differential test that loads the same rows, runs the
  same flush/compaction/materialization maintenance sequence, and asserts the
  same visible reads plus the same level layout.
- Add a wasm-none mechanics note and target-gated tests: cache opens without
  localfs, durable-local returns unsupported without `localfs`, and serving
  behavior matches native cache for commits, reads, scans, flush, and cache
  compaction.
- Add budget/pressure tests that prove suggested maintenance actually drains
  backlog when run: frozen tables go to zero after flush, L0 table count drops
  after compaction, and sustained load does not leave unbounded L0 fanout.
- Decide whether storage should preserve old unified memory-budget
  derivation or keep the pool budget model. If the pool model is intentional,
  document the migration and add tests mapping old `memory_budget`,
  `block_cache_size`, `write_buffer_size`, and `max_immutable_memtables` cases
  to storage expectations.
- Replace per-allocation-only admission with held reservations where cumulative
  concurrent generated artifacts, table readers, and manifest catalog usage can
  exceed the configured pool limits.
- Add mode-specific benchmark output fields so future regressions cannot hide
  behind mode differences: mode, durability policy, backend kind, localfs
  feature, target architecture, budget policy, block cache bytes, table reader
  budget, active/frozen budget, and maintenance queue depth.

### 9. Differential Tests And Perf Counters

Status: `Partial`

Purpose:

- Turn the partial audit findings into executable proof.
- Prevent performance regressions from hiding behind wall-clock benchmark
  variance.
- Keep fixes tied to old-storage behavior instead of speculative storage
  rewrites.

Existing evidence:

- `crates/storage/src/observability/perf_trace.rs` already tracks many
  useful hot-path counters: commit mapping/runtime time, validation time,
  duplicate-key checks, row preparation, append insertion, branch-fact scans,
  read-view capture/cloning, point rows visited, point candidates, scan rows
  visited, scan candidates, cursor seeks, scan phase timings, row clone counts,
  table seeks, and bound checks.
- `benchmarks/src/bin/storage_next_l9_scale.rs` serializes the current
  storage perf trace into benchmark JSON and reports load-phase
  maintenance time and maintenance run counts.
- `crates/storage/src/perf_trace.rs` tracks old scan iterator counters:
  iterator seeks, pipeline builds, rows yielded, KV scan calls, rows returned,
  and scan phase timings.
- `benchmarks/src/bin/storage_old_cache_scale.rs` serializes the old scan
  counters and load-phase timings.
- Old-storage source code in `crates/storage/src/segmented/mod.rs` contains
  point-read implementation details that should be mirrored in counters even
  though the legacy perf-trace surface does not expose all point-read source
  class counters.

Missing evidence:

- Storage benchmark output does not yet record final level shape:
  active row count, frozen table count, owned table counts by level, L0 table
  count, inherited layer count, inherited table counts by level, retained
  history table count, total table object count, and max table fanout observed
  during load.
- Point-read counters are too coarse. They report rows visited and candidates,
  but not which source class caused the work: active, frozen, owned L0, owned
  nonzero levels, inherited L0, inherited nonzero levels, retained history, or
  table-manifest recovered tables.
- Scan counters do not distinguish L0 table cursors from nonzero-level lazy
  level cursors. Without that split, we cannot prove whether scans are using
  old `LevelSegmentIter`-style lazy level traversal or opening every physical
  table.
- Maintenance counters do not yet prove backlog drain. Benchmarks should show
  flush tasks requested/completed, compaction tasks requested/completed,
  materialization tasks requested/completed, checkpoint tasks, WAL truncation,
  skipped/deferred tasks, final queue depth, and reason classes.
- Mode metadata is incomplete for cross-run comparison. Every benchmark result
  should include storage mode, durability policy, backend kind, target
  architecture, localfs feature state, budget policy, budget pool sizes,
  block-cache capacity, maintenance queue depth, and whether perf-trace was
  enabled.
- The old and new benchmark traces are not schema-aligned. Old scans report
  iterator pipelines and rows yielded; storage reports cursor seeks and
  row visits. The comparison needs normalized derived fields such as
  `scan_source_seeks_per_call`, `scan_rows_visited_per_row_returned`,
  `point_source_probes_per_read`, and `l0_tables_after_load`.

Differential test plan:

1. LSM layout and maintenance drain:
   - Old anchors:
     `crates/storage/src/segmented/tests/leveled.rs`,
     `crates/storage/src/segmented/tests/flush.rs`.
   - Storage placement:
     `crates/storage/src/lifecycle/tests/compaction/`,
     `crates/storage/src/branch/tests/owned_compaction.rs`.
   - Test shape: load rows in repeated flush-sized batches, run maintenance
     until no suggested task remains, then assert L0 has drained, L1+ levels
     are sorted/non-overlapping, and visible reads are unchanged.

2. Point-read source pruning:
   - Old anchors:
     `crates/storage/src/segmented/mod.rs` `get_versioned_from_branch` and
     `point_lookup_level_preencoded`.
   - Storage placement:
     `crates/storage/src/branch/tests/read_view.rs` and a new
     perf-trace gated branch-read test module.
   - Test shape: construct active, frozen, owned L0, owned L1+, inherited L0,
     and inherited L1+ sources. Assert returned rows match the old model and
     nonzero-level table probes are bounded by level count, not table count.

3. Scan source planning:
   - Old anchors:
     `crates/storage/src/seekable.rs`,
     `crates/storage/src/merge_iter.rs`,
     `crates/storage/src/segmented/mod.rs` `StorageIterator`.
   - Storage placement:
     `crates/storage/src/branch/tests/read_view.rs`,
     `crates/storage/src/table/tests/cursor.rs`, and API scan tests.
   - Test shape: scan prefix and range over many non-overlapping L1+ tables.
     Assert visible rows match old storage and scan source setup creates one
     lazy cursor per nonzero level rather than one cursor per table.

4. MVCC, tombstone, TTL, and history:
   - Old anchors:
     `crates/storage/src/segmented/tests/basic.rs`,
     `crates/storage/src/segmented/tests/resurrection.rs`,
     `crates/storage/src/merge_iter.rs`, `crates/storage/src/ttl.rs`.
   - Storage placement:
     `crates/storage/src/api/tests/read.rs`,
     `crates/storage/src/api/tests/commit.rs`,
     `crates/storage/src/branch/tests/row_pruning/`.
   - Test shape: run put/delete/put resurrection, at-version tombstone
     suppression, latest TTL expiration, timestamp TTL expiration, history
     tombstone inclusion, and history TTL filtering. Explicitly decide whether
     latest/history TTL behavior should match old wall-clock filtering.

5. Branch inheritance and materialization:
   - Old anchors:
     `crates/storage/src/segmented/tests/fork.rs`,
     `crates/storage/src/segmented/tests/materialize.rs`.
   - Storage placement:
     `crates/storage/src/branch/tests/inheritance_materialization/`,
     `crates/storage/src/lifecycle/tests/branch_lifecycle/`.
   - Test shape: fork chains, child shadowing, inherited tombstones, parent
     compaction after fork, materialization retry, materialized read parity,
     and copied `Materializing` status behavior.

6. Durable recovery and publication:
   - Old anchors:
     `crates/storage/src/segmented/tests/post_restart_branch.rs`,
     `crates/storage/src/segmented/tests/materialize.rs`,
     `crates/storage/src/segmented/tests/publish_failures.rs`,
     `crates/storage/src/segmented/tests/quarantine_reconciliation.rs`.
   - Storage placement:
     `crates/storage/src/lifecycle/tests/recovery.rs`,
     `crates/storage/src/lifecycle/tests/table_manifest_recovery.rs`,
     `crates/storage/src/lifecycle/tests/durable.rs`,
     `crates/storage/src/service/quarantine/tests/`.
   - Test shape: restart after commit, flush, compaction, materialization,
     fork, checkpoint, WAL truncation, branch delete/clear, quarantine, and
     purge. Assert visible reads, level layout, inherited layers, recovery
     health, and reachable object inventory.

7. Mode and budget parity:
   - Old anchors:
     `crates/storage/src/runtime_config.rs`,
     `crates/storage/src/segmented/mod.rs`,
     `crates/storage/src/durability/wal/mode.rs`.
   - Storage placement:
     `crates/storage/src/lifecycle/tests/cache.rs`,
     `crates/storage/src/lifecycle/tests/durable.rs`,
     `crates/storage/src/lifecycle/tests/budget.rs`,
     `crates/storage/src/lifecycle/tests/budget_runtime.rs`,
     `crates/storage/src/api/tests/maintenance.rs`.
   - Test shape: run the same branch-local workload in cache and standard
     durable mode, assert identical visible rows and level layout, and verify
     budget/pressure suggestions drain frozen and L0 backlog when executed.

Required storage perf counters:

- Source layout:
  `active_rows`, `frozen_tables`, `frozen_rows`, `owned_l0_tables`,
  `owned_level_table_counts`, `owned_total_tables`, `inherited_layers`,
  `inherited_l0_tables`, `inherited_level_table_counts`,
  `retained_history_tables`, `total_table_objects`.
- Point reads:
  `point_reads`, `point_active_probes`, `point_frozen_probes`,
  `point_owned_l0_table_probes`, `point_owned_nonzero_level_searches`,
  `point_owned_nonzero_table_probes`, `point_inherited_layer_searches`,
  `point_inherited_l0_table_probes`,
  `point_inherited_nonzero_level_searches`,
  `point_inherited_nonzero_table_probes`, `point_table_seeks`,
  `point_rows_visited`, `point_candidates_materialized`,
  `point_hit_active`, `point_hit_frozen`, `point_hit_l0`,
  `point_hit_nonzero`, `point_hit_inherited`, `point_misses`.
- Scans:
  `scan_calls`, `scan_active_cursors`, `scan_frozen_cursors`,
  `scan_owned_l0_cursors`, `scan_owned_nonzero_level_cursors`,
  `scan_owned_nonzero_table_cursors_opened`,
  `scan_inherited_l0_cursors`, `scan_inherited_nonzero_level_cursors`,
  `scan_inherited_nonzero_table_cursors_opened`, `scan_cursor_seeks`,
  `scan_rows_visited`, `scan_rows_returned`, `scan_candidate_rows_cloned`,
  `scan_candidate_row_clone_bytes`.
- Maintenance and pressure:
  `flush_tasks_requested`, `flush_tasks_completed`,
  `compaction_tasks_requested`, `compaction_tasks_completed`,
  `materialization_tasks_requested`, `materialization_tasks_completed`,
  `maintenance_tasks_deferred`, `maintenance_queue_depth_final`,
  `pressure_severity_final`, `pressure_reason_final`,
  `l0_tables_max_during_load`, `l0_tables_final`.
- Durability:
  `wal_segments_retained`, `wal_bytes_retained`,
  `checkpoint_watermark`, `flush_watermark`, `table_manifest_sequence`,
  `table_manifest_debt`, `orphan_table_objects_detected`,
  `quarantine_inventory_entries`, `recovery_fault_count`.
- Benchmark metadata:
  `engine`, `storage_mode`, `durability_policy`, `backend_kind`,
  `target_arch`, `localfs_feature`, `budget_policy`, `block_cache_bytes`,
  `table_reader_budget_bytes`, `active_mutable_budget_bytes`,
  `frozen_mutable_budget_bytes`, `maintenance_queue_budget_bytes`,
  `perf_trace_enabled`, `git_rev`.

Derived comparison fields:

- `point_source_probes_per_read`.
- `point_nonzero_table_probes_per_read`.
- `scan_source_cursors_per_call`.
- `scan_table_cursors_opened_per_call`.
- `scan_rows_visited_per_row_returned`.
- `load_maintenance_ms_per_million_rows`.
- `l0_tables_per_million_rows_after_load`.
- `compaction_tasks_per_flush_task`.
- `old_to_new_throughput_ratio` for point, scan prefix, scan range, and load.

Fail-fast invariants:

- After maintenance drain, L0 table count must not scale linearly with total
  row count for the benchmark load shape.
- For point reads over nonzero levels, table probes must be bounded by level
  count, not table count.
- For scans over nonzero levels, cursor setup must be bounded by level count
  and should open tables lazily as the scan advances.
- Captured history for a single key must not scan unrelated physical keys.
- Cache and standard durable modes must have identical branch-local level shape
  for the same workload after durable-only persistence facts are ignored.
- Reopen after durable rewrite publication must not resurrect unmanifested
  table objects and must not lose reachable objects.

Immediate implementation order:

1. Add source-layout counters and benchmark metadata first. These are the
   cheapest and make all later benchmark results interpretable.
2. Add point-read source-class counters and a perf-trace gated point-pruning
   test.
3. Add scan source-class counters and a perf-trace gated lazy-level scan test.
4. Add cache-vs-durable differential tests for identical branch-local level
   shape.
5. Add durable restart differential tests around rewrite publication and table
   manifest debt.
6. Add wasm-none target documentation/tests once the native parity tests pass.

### 10. Final Parity Matrix And Architecture-Aligned Gap Plan

Status: `Partial`

Overall conclusion:

- Storage has substantial architecture and tests, but it has not restored
  old-storage production mechanics yet.
- No audited category is `Confirmed`. Every category is `Partial` because the
  code either lacks the old asymptotic behavior, lacks direct differential
  proof, or has an explicit product/architecture decision still pending.
- The most important confirmed performance problem is source fanout. At 10M
  keys, storage still behaves as if point reads and scans must touch about
  100 flushed sources. Old storage avoided that with LSM level invariants,
  binary point lookup in L1+, and lazy level iteration for scans.
- The most important correctness risk is durable topology/recovery parity.
  Storage changed the durable source of truth from branch
  `segments.manifest` files to database/branch/table manifests and table object
  facts. That can be correct, but fault windows and reachability need direct
  restart proof before this is production-ready.
- The cleanup/refactor work made the code shape clearer, but performance
  parity now depends on deliberately restoring old serving mechanics rather
  than adding benchmark-specific shortcuts.

Final parity matrix:

| Area | Status | Risk | Owning storage layer | Primary gap |
| --- | --- | --- | --- | --- |
| LSM layout and level invariants | `Partial` | High performance | L6 primary, with L8 scheduling and L5 table output support | L0 fanout is not proven to drain under sustained load; level diagnostics are missing. |
| Point-read source pruning | `Partial` | High performance | L6 read planning, with L5 table seek/filter support | Latest reads use table seeks, but source-class pruning for L1+ and inherited layers is not fully proven; history still scans unrelated keys. |
| Scan source planning and iterator behavior | `Partial` | High performance | L6 scan planning, with L5 raw cursor/merge support | Storage has cursor mechanics, but old lazy nonzero-level iteration is not proven by counters/tests. |
| Compaction selection, output shape, and installation | `Partial` | High performance and correctness | L8 scheduling, L6 level mutation, L5 table compaction, L4 publication | Old scoring, output sizing, L0-to-L1 drain, and durable install fault windows are not fully mapped. |
| MVCC, tombstone, TTL, and history | `Partial` | Medium correctness | L6 visibility and retention facts, with L5 row metadata and L8 pruning | Core MVCC/tombstones exist; latest/history TTL semantics differ from old wall-clock filtering, and history collection is inefficient. |
| Branch inheritance, fork, and materialization | `Partial` | High correctness and performance | L6 branch COW/materialization, with L8 scheduling and L4 durable publication | Core COW mechanics exist; copied `Materializing` status differs, inherited serving inherits fanout gaps, and durable publication/recovery proof is incomplete. |
| Durability, manifest, WAL, and recovery | `Partial` | Critical correctness | L4 durable services and L8 recovery, with L6 reachability facts | Table-object reference recovery is rejected; rewrite publication/manifest debt and reachability/orphan behavior need restart proof. |
| Cache, standard, wasm-none, and budget modes | `Partial` | Medium product and performance | L9 mode boundary, L8 lifecycle/budget use, L1 capability validation | Public API intent is cleaner; mode parity, wasm-none documentation, and budget-driven backlog drain are not proven. |
| Differential tests and perf counters | `Partial` | Critical proof gap | Cross-cutting proof owned by the layer being tested | Existing counters are useful but not sufficient to prove level shape, source class, backlog drain, durability reachability, or mode parity. |

Layer-aligned planning rules:

1. Every correction must be owned by the storage layer whose contract was
   lost or weakened. L9 benchmarks and APIs are proof gates, not places to add
   serving-path shortcuts.
2. L5 restores table mechanics: table seek, filters, indexes, block cache,
   raw cursors, raw merge, table building, and table compaction algorithms.
3. L6 restores the branch-isolated LSM: branch-local source topology,
   inherited layers, fork-version gates, MVCC visibility, source pruning, and
   materialization state transitions.
4. L8 restores maintenance orchestration: flush pressure, compaction
   scheduling, materialization scheduling, retention, repair, and health facts.
5. L4 restores durable service correctness: WAL, manifest, snapshot, table
   publication, table-object reachability, and publish/restart fault windows.
6. L7 is audited only for commit/runtime drift: version ordering,
   WAL-before-visible discipline, commit visibility, and write-path
   backpressure. It should not absorb L5/L6 read-path fixes.
7. Observability must be attached to the owning layer. A counter that proves
   L6 source pruning belongs in L6 or a storage-wide trace sink fed by L6, not
   in a benchmark-only adapter.

Architecture-aligned implementation packages:

1. `GAP-L5`: restore table-runtime parity.
   - Architecture source:
     `docs/architecture/storage/l5-table-runtime.md`.
   - Old evidence files: `crates/storage/src/memtable.rs`,
     `crates/storage/src/segment.rs`,
     `crates/storage/src/segment_builder.rs`,
     `crates/storage/src/index.rs`, `crates/storage/src/bloom.rs`,
     `crates/storage/src/block_cache.rs`,
     `crates/storage/src/merge_iter.rs`,
     `crates/storage/src/seekable.rs`, and table-oriented logic in
     `crates/storage/src/compaction.rs`.
   - Storage target areas: table readers/builders, table cursor stack,
     table compaction, table cache, and table-level perf facts.
   - Work: verify table key ordering, point seek, range seek, prefix seek,
     block/index/filter usage, raw lazy level cursor support, raw merge cursor
     behavior, output splitting, and database-local cache ownership.
   - Exit gate: L5 tests can prove table operations without branch semantics,
     and no L5 production code reaches into backend IO, branch state, commit
     policy, lifecycle scheduling, or L9 benchmark concerns.

2. `GAP-L6`: restore branch-isolated LSM topology and serving mechanics.
   - Architecture source:
     `docs/architecture/storage/l6-branch-isolated-lsm-runtime.md`.
   - Old evidence files: `crates/storage/src/segmented/mod.rs`,
     `crates/storage/src/key_encoding.rs`,
     `crates/storage/src/merge_iter.rs`,
     `crates/storage/src/seekable.rs`,
     `crates/storage/src/stored_value.rs`,
     `crates/storage/src/segmented/ref_registry.rs`, and
     `crates/storage/src/manifest.rs`.
   - Storage target areas: branch state, branch read planner, branch scan
     planner, inherited-layer handling, materialization, and branch-level
     source/layout diagnostics.
   - Work: restore the old LSM shape as a branch-owned contract: active
     mutable table, frozen mutable tables, overlapping L0, non-overlapping
     L1+, inherited layers nearest ancestor first, fork-version gates, child
     shadowing, and key rewriting. Point reads should be bounded by active +
     frozen + L0 + level count. Scans should use normal L6 machinery where L0
     contributes table cursors and each nonzero level contributes a lazy level
     cursor.
   - Exit gate: latest reads, version reads, prefix scans, range scans, and
     history reads match old-storage results across MVCC, tombstones,
     inheritance, and materialization. Source-count counters prove L1+ work is
     bounded by level count, not table count.

3. `GAP-L8`: restore lifecycle and maintenance drain.
   - Architecture source:
     `docs/architecture/storage/l8-lifecycle-recovery-maintenance.md`.
   - Old evidence files: `crates/engine/src/background.rs`,
     `crates/engine/src/database/compaction.rs`,
     `crates/engine/src/database/lifecycle.rs`,
     `crates/storage/src/segmented/compaction.rs`,
     `crates/storage/src/segmented/recovery.rs`,
     `crates/storage/src/segmented/quarantine_protocol.rs`,
     `crates/storage/src/pressure.rs`, `crates/storage/src/rate_limiter.rs`,
     and `crates/storage/src/memory_stats.rs`.
   - Storage target areas: lifecycle compaction scheduler, flush
     scheduler, maintenance pressure facts, materialization scheduler,
     retention/pruning, quarantine/reclaim, and budget consumption.
   - Work: restore automatic maintenance behavior so sustained load does not
     strand flushed tables in L0. L8 should decide when to flush, compact,
     materialize, checkpoint, prune, quarantine, purge, and repair; it should
     call L5/L6/L4 primitives instead of embedding their logic.
   - Exit gate: after maintenance drain at 1M, 5M, 10M, and larger scale
     tiers, L0 table count and source fanout do not scale linearly with total
     row count. Maintenance metrics explain every skipped or failed task.

4. `GAP-L4/L8`: close durable topology and recovery parity.
   - Architecture sources:
     `docs/architecture/storage/l4-log-manifest-snapshot-services.md` and
     `docs/architecture/storage/l8-lifecycle-recovery-maintenance.md`.
   - Old evidence files: `crates/storage/src/durability/wal/`,
     `crates/storage/src/durability/disk_snapshot/`,
     `crates/storage/src/durability/checkpoint_runtime.rs`,
     `crates/storage/src/durability/recovery.rs`,
     `crates/storage/src/durability/recovery_bootstrap.rs`,
     `crates/storage/src/manifest.rs`,
     `crates/storage/src/segmented/recovery.rs`, and
     `crates/storage/src/segmented/quarantine_protocol.rs`.
   - Storage target areas: WAL service, table manifest service,
     snapshot/checkpoint service, rewrite publication, table-object
     reachability, recovery bootstrap, and quarantine reconciliation.
   - Work: prove restart behavior across table publication, branch manifest
     publication, compaction replacement, materialization replacement,
     checkpoint, WAL truncation, branch delete/clear, quarantine, and purge.
   - Exit gate: crash/restart tests pass for every durable transition window,
     and recovery can classify reachable, orphaned, missing, quarantined, and
     corrupt table objects without rejecting valid old-style table topology.

5. `GAP-L7`: verify commit-runtime parity and keep it narrow.
   - Architecture source:
     `docs/architecture/storage/l7-commit-runtime.md`.
   - Old evidence files: `crates/storage/src/txn/context.rs`,
     `crates/storage/src/txn/manager.rs`,
     `crates/storage/src/txn/validation.rs`,
     `crates/storage/src/txn/lock_ordering.rs`, and
     `crates/storage/src/durability/commit_adapter.rs`.
   - Storage target areas: commit batch, version allocation, commit
     visibility, branch commit guards, WAL-before-visible bridge, and write
     stall/backpressure facts.
   - Work: confirm the read/scan performance gap is not caused by commit-time
     stamping, duplicate validation, or write-buffer state. Fix only real L7
     drift, and do not move L5/L6 source-planning behavior into commit code.
   - Exit gate: commit ordering, validation, WAL-before-visible discipline,
     visible-version tracking, and backpressure tests match the old runtime
     contract or document an intentional V1 change.

6. `GAP-L9`: keep the storage API boundary explicit.
   - Architecture source:
     `docs/architecture/storage/l9-storage-api-boundary.md`.
   - Old evidence files: `crates/storage/src/traits.rs`,
     `crates/storage/src/runtime_config.rs`, and engine call sites that
     consume storage through the public boundary.
   - Storage target areas: open/create, explicit storage mode,
     durability policy, L9 read/scan/history/fork/materialize APIs,
     benchmark drivers, and raw health/metrics outcomes.
   - Work: preserve explicit cache versus durable local mode selection, reject
     unsupported wasm-none durable paths clearly, expose raw storage mechanics
     to engine, and keep product semantics out of storage. Benchmarks
     should run through L9 but should not shape lower-layer algorithms.
   - Exit gate: cache, durable local `standard`, durable local `always`, and
     wasm-none-supported subsets are documented and tested through L9.

Intentional product decisions still required:

- Whether storage latest reads and history should match old wall-clock TTL
  filtering.
- Whether copied inherited layers in `Materializing` status should reset to
  `Active` like old storage or remain readable as storage currently does.
- Whether table-object reference recovery is obsolete under the new manifest
  architecture or must be implemented.
- Whether the pool-based budget model replaces old unified memory-budget
  derivation.
- Whether wasm-none is only a target/feature shape or should become an
  explicit documented storage mode.

Layer-aligned execution sequence:

1. Start with `GAP-L6` serving topology, because the confirmed performance
   regression is a violation of the L6 branch-isolated LSM contract. Add only
   the L5/L6 observability needed to prove source shape and cursor setup, then
   implement the L6 planner corrections in the normal read/scan machinery.
2. Run L9 old-vs-new benchmarks at 100K, 1M, 5M, and 10M after the L6 serving
   package. The benchmark result is the proof gate, not the implementation
   owner.
3. Move to `GAP-L8` maintenance drain once the serving path is no longer
   structurally opening/probing every source. This restores scale behavior
   under load.
4. Move to `GAP-L4/L8` durable topology and recovery proof after branch-local
   cache-mode serving and compaction behavior are stable.
5. Run `GAP-L7` as a narrow audit/fix pass to make sure commit mechanics did
   not drift while lower-layer source topology was restored.
6. Finish with `GAP-L9` mode/API/documentation alignment so engine gets a
   clean boundary over restored lower-layer mechanics.

## Immediate Next Step

Start `GAP-L6`: branch-isolated LSM serving topology.

The first implementation unit should be a focused L6 plan that maps the old
`SegmentedStore` read and scan source topology to storage branch state.
It should include the minimum L5/L6 counters needed to prove the layer contract:
active/frozen source count, L0 table count, nonzero-level count, inherited-layer
count, point probes by source class, scan cursor setup by source class, and rows
visited per returned row.

This keeps observability in service of the architecture. We are not adding a
generic benchmark counter phase, and we are not adding L9 fast paths. We are
restoring the L6 branch-isolated LSM contract using L5 table primitives, then
using L9 benchmarks to prove the restored behavior.
