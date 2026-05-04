# Storage Engine Ownership Audit

## Purpose

This document records the settled storage/engine boundary between
`strata-storage` and `strata-engine`.

The original audit existed because crate location was not reliable evidence of
rightful ownership: lower-runtime checkpoint, recovery, snapshot, and config
mechanics were still hosted in engine. The storage-boundary normalization corrected those targeted
surfaces without flattening the architecture.

The current rule is:

- `strata-storage` owns generic persistence, transaction, durability, recovery,
  retention, and storage-runtime mechanics
- `strata-engine` owns database orchestration, public APIs, primitive
  semantics, branch/domain semantics, search/runtime behavior, and product
  policy
- cross-boundary calls are allowed only where engine-owned semantics or policy
  intentionally invoke storage-owned substrate mechanics

This audit should be read together with:

- [storage-charter.md](./storage-charter.md)
- [storage-crate-map.md](./storage-crate-map.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)
- [../engine/engine-storage-boundary-normalization-plan.md](../engine/engine-storage-boundary-normalization-plan.md)
- [../engine/boundary-closeout-plan.md](../engine/boundary-closeout-plan.md)

## Current Verdict

The targeted storage/engine ownership split is documented and guarded for the
storage-boundary normalization surfaces. This closeout record does not replace
the final broad regression gate in the main normalization plan; the full
workstream is complete only after that gate is run and recorded.

That does not mean engine has no storage calls. The intended architecture is a
layered call direction:

```text
core -> storage -> engine -> intelligence -> executor -> cli
```

Engine is expected to call storage because engine is the database orchestration
layer. The closeout condition is that those calls now cross explicit seams from
engine-owned semantics and policy into storage-owned mechanics.

The crate graph remains one-way: `strata-storage` depends on `strata-core` and
does not depend on `strata-engine`. `strata-engine` depends on
`strata-storage`.

## Storage-Owned Surfaces

The following areas are storage-owned and should stay below engine.

### Physical Layout And Addressing

- [layout.rs](../../crates/storage/src/layout.rs)
- [key_encoding.rs](../../crates/storage/src/key_encoding.rs)

Storage owns:

- `Key`
- `Namespace`
- `TypeTag`
- physical key encoding and space-name validation

These are substrate identifiers. `TypeTag` may be used as an opaque storage
family byte, but storage must not attach primitive semantics to it.

### MVCC Storage State

- [segment.rs](../../crates/storage/src/segment.rs)
- [memtable.rs](../../crates/storage/src/memtable.rs)
- [stored_value.rs](../../crates/storage/src/stored_value.rs)
- [seekable.rs](../../crates/storage/src/seekable.rs)
- [merge_iter.rs](../../crates/storage/src/merge_iter.rs)
- [ttl.rs](../../crates/storage/src/ttl.rs)
- [segmented/](../../crates/storage/src/segmented)

Storage owns:

- versioned state
- reads, writes, scans, and counts
- tombstones and TTL
- segment and memtable mechanics
- branch-scoped physical state
- quarantine and corruption handling

Branch identifiers are physical storage partition keys here, not branch
workflow policy.

### Generic Transaction Runtime

- [txn/](../../crates/storage/src/txn)
- [durability/commit_adapter.rs](../../crates/storage/src/durability/commit_adapter.rs)

Storage owns:

- transaction context storage state
- OCC validation
- CAS and read-set validation
- storage commit adaptation into WAL-backed or memory-backed persistence

Primitive transaction semantics remain in engine.

### Durability Runtime

- [durability/](../../crates/storage/src/durability)
- [durability/checkpoint_runtime.rs](../../crates/storage/src/durability/checkpoint_runtime.rs)
- [durability/decoded_snapshot_install.rs](../../crates/storage/src/durability/decoded_snapshot_install.rs)
- [durability/recovery_bootstrap.rs](../../crates/storage/src/durability/recovery_bootstrap.rs)
- [runtime_config.rs](../../crates/storage/src/runtime_config.rs)

Storage owns:

- WAL read/write, codec, and payload mechanics
- snapshot/checkpoint file mechanics
- generic MANIFEST load/create/update mechanics
- checkpoint, WAL compaction, snapshot pruning, and MANIFEST sync helpers
- generic decoded-row snapshot install into `SegmentedStore`
- storage recovery bootstrap and replay coordination
- storage runtime config derivation and application

These APIs expose raw substrate facts and storage-local errors. They must not
grow engine reports, `StrataError`, recovery policy wording, or product config
vocabulary.

### Storage-Local Error And Retention Facts

- [error.rs](../../crates/storage/src/error.rs)
- [segmented/quarantine_protocol.rs](../../crates/storage/src/segmented/quarantine_protocol.rs)
- [segmented/recovery.rs](../../crates/storage/src/segmented/recovery.rs)

Storage owns:

- `StorageError`
- storage corruption and IO classification
- raw recovery health and degradation facts
- raw retention and inherited-layer facts

Engine turns these into public errors, health reports, and operator-facing
retention reports.

## Engine-Owned Surfaces

The following areas are engine-owned and should stay above storage.

### Database Runtime And Lifecycle

- [database/](../../crates/engine/src/database)

Engine owns:

- `Database::open_runtime()`
- `Database::checkpoint()`
- `Database::compact()`
- lifecycle/open-state checks
- primary vs follower mode policy
- subsystem recovery and initialization ordering
- public health, retention, recovery, and lossiness reporting
- conversion into `StrataError` and `RecoveryError`

Engine may call storage runtime helpers, but it remains the public database API
owner and policy interpreter.

### Branch Domain And Workflow

- [branch_domain.rs](../../crates/engine/src/branch_domain.rs)
- [branch_ops/](../../crates/engine/src/branch_ops)

Engine owns:

- branch lifecycle/control
- branch DAG semantics
- merge/cherry-pick/revert workflow
- branch-level business rules

Storage may store branch-scoped physical rows and inherited layers. It must not
own branch workflow policy.

### Primitive Semantics

- [semantics/](../../crates/engine/src/semantics)
- [primitives/](../../crates/engine/src/primitives)
- [search/](../../crates/engine/src/search)

Engine owns:

- JSON path and patch behavior
- event semantics
- vector config and metric semantics
- search-facing value extraction
- primitive snapshot section decode
- primitive-shaped install and recovery telemetry
- search indexing and runtime behavior

These semantics must not move into storage, even when their durable
representation is KV rows.

### Product-Facing Runtime Workflows

- [bundle/](../../crates/engine/src/bundle)
- [background.rs](../../crates/engine/src/background.rs)
- [database/profile.rs](../../crates/engine/src/database/profile.rs)
- [database/config.rs](../../crates/engine/src/database/config.rs)

Engine owns:

- branch bundle import/export
- product and hardware profiles
- public `StrataConfig` and compatibility behavior
- background task orchestration above storage mechanics

Storage owns only the storage-shaped runtime config derived from that public
engine config.

## Intentional Boundary Seams

| Surface | Storage Owns | Engine Owns | Why The Seam Remains |
|---|---|---|---|
| Checkpoint | File construction, raw checkpoint facts, manifest and watermark mechanics | `Database::checkpoint()`, lifecycle checks, public result and error mapping | Checkpoint is a public database operation, but its durable mechanics are generic storage work. |
| WAL compaction | WAL pruning and manifest active-segment mechanics | `Database::compact()`, lifecycle checks, public result and error mapping | Engine decides when compaction is requested; storage executes the lower WAL/manifest mechanics. |
| Snapshot pruning | Filesystem pruning and raw prune facts | lifecycle-aware retention report | Storage knows files and counts; engine knows operator-facing retention vocabulary. |
| Snapshot install | Generic decoded-row validation and install | primitive section decode, primitive install stats, recovery policy | Primitive snapshot DTOs and section meaning are engine semantics; decoded row install is storage mechanics. |
| Recovery bootstrap | MANIFEST prep, codec validation, replay, raw recovery facts | open policy, degraded/lossy policy, subsystem recovery, coordinator bootstrap | Storage recovers durable substrate state; engine decides whether and how the database opens. |
| Storage config | `StorageRuntimeConfig`, effective storage values, store/global application | public `StrataConfig`, product profiles, compatibility | Public config is an engine API; effective storage knobs belong to storage. |
| Retention | storage pruning mechanics and minimum-retain invariant | public retention policy and lifecycle behavior | Storage preserves mechanical invariants; engine owns user-facing policy and reports. |
| Test observability | storage-local runtime snapshots/accessors under test gates | characterization tests and production-use guardrails | Tests need visibility into storage state; production engine code must not depend on test-only accessors. |

## Accepted Residue

The following residue is intentional after boundary closeout:

- `strata_storage::durability::__internal` is gated by the
  `engine-internal` feature and exposes the WAL background-sync extension seam
  used by engine lifecycle orchestration.
- `StorageConfig::effective_*` wrappers remain as public compatibility methods
  in engine, but production engine code uses `StorageRuntimeConfig` for storage
  derivation and application.
- `SnapshotRetentionPolicy::retain_count` remains an engine-owned public field;
  storage normalizes retention input through `StorageSnapshotRetention`.
- block-cache capacity is process-global storage runtime state. Sequential
  applications overwrite earlier values; concurrent database opens race and do
  not promise a deterministic winner.
- upper crates still have some direct storage dependencies for current
  transitional primitive/runtime crates. They must not bypass engine to drive
  checkpoint, recovery, open, retention, or database policy. The next engine
  consolidation phase is expected to absorb graph, vector, search,
  executor-legacy, and security responsibilities into engine.

## Regression Guards

Useful closeout checks:

```bash
cargo tree -p strata-storage --depth 2
cargo tree -p strata-engine --depth 1
cargo tree -i strata-storage --workspace --depth 2
rg -n 'use strata_engine|strata_engine::|strata-engine' crates/storage/src crates/storage/Cargo.toml
rg -n 'JsonPath|JsonPatch|SearchSubsystem|Recipe|VectorConfig|DistanceMetric|ChainVerification|BranchLifecycle|RetentionReport|HealthReport|StrataConfig|StrataError|executor' crates/storage/src
rg -n 'fn apply_storage_config|apply_storage_config' crates/engine/src
```

Expected results:

- `strata-storage` depends on `strata-core` and external crates, not
  `strata-engine`
- storage source has no engine imports
- moved generic storage modules have no primitive semantics, and storage source
  has no engine report or product policy vocabulary outside storage-owned
  durable format surfaces
- engine has no `apply_storage_config` helper or hand-written storage setter
  list

## Closeout Conclusion

The accepted storage-boundary ownership split is now documented for the target
surfaces, with guard commands recorded above and the main plan still carrying
the final broad regression gate.

`strata-storage` is the lower persistence/runtime substrate. `strata-engine`
is the database orchestration and semantics layer. The remaining
engine-to-storage calls are intentional seams where engine-owned public APIs,
semantics, or policy invoke storage-owned mechanics.
