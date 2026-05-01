# Storage Engine Ownership Audit

## Purpose

This document audits the current boundary between `strata-storage` and
`strata-engine`.

It exists because the storage consolidation work cannot safely proceed by
assuming either crate is already clean. The lower-runtime split across
`storage`, `concurrency`, and `durability` already proves that current crate
location is not reliable evidence of rightful ownership.

This audit answers one question directly:

**What in `storage` truly belongs in `storage`, what in `engine` truly belongs
in `engine`, and what code is currently sitting on the wrong side of that
boundary?**

It should be read together with:

- [storage-charter.md](./storage-charter.md)
- [storage-crate-map.md](./storage-crate-map.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)
- [concurrency-crate-map.md](./concurrency-crate-map.md)
- [durability-crate-map.md](./durability-crate-map.md)

## Audit Rule

The classification rule is simple:

- if code implements **generic persistence or runtime mechanics**, it belongs
  in `storage`
- if code implements **primitive meaning, branch/domain meaning, or
  product/runtime behavior above the substrate**, it belongs in `engine`

This audit does not use crate names as proof.

## Summary Verdict

The current boundary is not clean.

The dirt is uneven:

- `storage` is already mostly a real storage crate
- `engine` still contains a meaningful amount of lower storage-runtime code

So the main corrective movement is likely:

- **more code from `engine` down into `storage`**
- not a large amount of storage code up into `engine`

There are still a few storage-side surfaces that need careful watching so they
do not become engine policy by accident, but the larger ownership problem is
on the engine side.

## What Clearly Belongs In `storage`

The following `storage` areas look correctly placed and should stay there.

### 1. Physical Layout And Addressing

- [layout.rs](../../crates/storage/src/layout.rs)
- [key_encoding.rs](../../crates/storage/src/key_encoding.rs)

These own:

- `Key`
- `Namespace`
- `TypeTag`
- physical key encoding/layout rules

This is substrate ownership, not engine semantics.

### 2. MVCC Storage State

- [segment.rs](../../crates/storage/src/segment.rs)
- [memtable.rs](../../crates/storage/src/memtable.rs)
- [stored_value.rs](../../crates/storage/src/stored_value.rs)
- [seekable.rs](../../crates/storage/src/seekable.rs)
- [merge_iter.rs](../../crates/storage/src/merge_iter.rs)
- [ttl.rs](../../crates/storage/src/ttl.rs)
- [segmented/mod.rs](../../crates/storage/src/segmented/mod.rs)

These implement:

- versioned state
- reads/writes/scans
- tombstones and TTL
- segment/memtable mechanics

This is exactly what storage should own.

### 3. Storage-Local Error And Health Surface

- [error.rs](../../crates/storage/src/error.rs)

This owns storage-local corruption, manifest, quarantine, and recovery fault
classification. That is lower-runtime health, not engine behavior.

### 4. Storage-Retention Mechanics

- [manifest.rs](../../crates/storage/src/manifest.rs)
- [quarantine.rs](../../crates/storage/src/quarantine.rs)
- [segmented/quarantine_protocol.rs](../../crates/storage/src/segmented/quarantine_protocol.rs)
- [segmented/recovery.rs](../../crates/storage/src/segmented/recovery.rs)

These own:

- segment manifests
- quarantine manifests
- retention protocol mechanics
- raw recovery outcomes

These are storage/runtime concerns even though they mention branches and
recovery classes.

## Storage-Side Surfaces That Need Careful Discipline

These do not obviously need to move today, but they can drift upward into
engine-shaped policy if left unchecked.

### 1. Retention Fact Surfaces

- `StorageBranchRetention`
- `StorageInheritedLayerInfo`

Defined in:

- [segmented/quarantine_protocol.rs](../../crates/storage/src/segmented/quarantine_protocol.rs)

These are acceptable **only as raw storage facts**:

- physical bytes
- inherited-layer bytes
- quarantined bytes
- manifest-derived reachability facts

They should not accumulate:

- lifecycle attribution
- operator-facing policy
- user-facing report semantics

That richer interpretation belongs in engine code like
[database/retention_report.rs](../../crates/engine/src/database/retention_report.rs).

### 2. Branch-Scoped Physical Operations

- `materialize_layer`
- reclaim/quarantine protocol
- inherited-layer state

These may look branch-flavored, but they are still storage concerns as long as
they remain about **physical retention and copy-on-write mechanics**, not
branch lifecycle semantics.

The line is:

- physical branch storage mechanics stay in `storage`
- branch workflow semantics stay in `engine`

## What Clearly Belongs In `engine`

The following engine areas look correctly placed and should stay there.

### 1. Branch Domain And Workflow

- [branch_domain.rs](../../crates/engine/src/branch_domain.rs)
- [branch_ops/](../../crates/engine/src/branch_ops)

These own:

- branch lifecycle/control
- branch DAG semantics
- merge/cherry-pick/revert workflow
- branch-level business rules

That is engine territory.

### 2. Primitive Semantics

- [semantics/event.rs](../../crates/engine/src/semantics/event.rs)
- [semantics/json.rs](../../crates/engine/src/semantics/json.rs)
- [semantics/vector.rs](../../crates/engine/src/semantics/vector.rs)
- [semantics/value.rs](../../crates/engine/src/semantics/value.rs)
- [primitives/](../../crates/engine/src/primitives)

These own:

- JSON path/patch behavior
- event semantics
- vector metric/config behavior
- search/runtime-facing value extraction

This should not sink into storage.

### 3. Branch Bundle Import/Export

- [bundle.rs](../../crates/engine/src/bundle.rs)

This is a product/domain workflow layered on top of lower persistence formats.
It belongs in engine, not storage.

### 4. Search And Higher Runtime Behavior

- [search/](../../crates/engine/src/search)
- [background.rs](../../crates/engine/src/background.rs) for non-storage tasks

Search indexing, search manifests, tokenization, and higher runtime behavior
belong above storage.

## What Is Probably On The Wrong Side Today

This is the main outcome of the audit.

Several engine modules look much closer to storage-runtime implementation than
to engine semantics.

### 1. Checkpoint / WAL Compaction Mechanics

- [database/compaction.rs](../../crates/engine/src/database/compaction.rs)

This module currently owns:

- checkpoint creation
- WAL compaction
- snapshot pruning
- MANIFEST watermark updates

Those are substrate runtime mechanics. Engine may expose public methods such as
`Database::checkpoint()` or `Database::compact()`, but the underlying
implementation wants to live in `storage`.

### 2. Snapshot Decode And Install Mechanics

- [database/snapshot_install.rs](../../crates/engine/src/database/snapshot_install.rs)

This module decodes snapshot sections and installs them into
`SegmentedStore`. That is very close to durability/storage replay mechanics,
not semantic engine behavior.

### 3. Recovery Bootstrap Mechanics

- [database/recovery.rs](../../crates/engine/src/database/recovery.rs)

This module currently owns:

- MANIFEST preparation
- WAL codec resolution
- `SegmentedStore` recovery orchestration
- replay bootstrap
- lossiness/degradation branching around storage recovery

Some of the final classification and operator policy belongs in engine, but a
large part of this module looks like lower-runtime recovery code that should
sink into `storage`.

### 4. Storage Configuration Application

- [database/open.rs](../../crates/engine/src/database/open.rs)
- [database/config.rs](../../crates/engine/src/database/config.rs)

The public `StrataConfig` belongs in engine because it is part of database
runtime orchestration. But the **mechanics** of:

- applying storage knobs
- wiring WAL writer settings
- interpreting storage-only config

want a stronger storage-side home.

The likely end state is split ownership:

- engine owns the database-facing config surface
- storage owns the storage-runtime config application and defaults for its own
  subsystem

## Split Surfaces: API In `engine`, Mechanics In `storage`

A number of surfaces should not move wholesale in either direction.

They need a split between:

- engine-owned public database/runtime API
- storage-owned implementation machinery

The main examples are:

### 1. `Database::checkpoint()` / `Database::compact()`

Public API should stay in engine.

But the checkpoint/WAL/snapshot/MANIFEST machinery underneath should become
storage-owned.

### 2. Open / Recovery Entry Points

`Database::open_runtime()` stays in engine because engine composes the runtime.

But:

- storage recovery
- snapshot install
- WAL/snapshot replay mechanics
- durability bootstrap

should move downward as much as possible.

### 3. Retention Reporting

The raw retention snapshot belongs in storage.

The lifecycle-aware, branch-vocabulary, operator-facing report belongs in
engine.

That split is already mostly correct and should be preserved.

## What Should Not Move Down Even If It Mentions Transactions

The most important caution is JSON.

Engine JSON code in:

- [primitives/json/mod.rs](../../crates/engine/src/primitives/json/mod.rs)

still interacts with `TransactionContext`, but that does **not** mean JSON
path/patch semantics belong in storage.

The correct move is:

- generic transaction mechanics go to `storage`
- JSON transaction semantics stay in `engine`

This is the same rule that should govern the `concurrency` absorption work.

## Audit Conclusion

The storage consolidation should not be treated as only:

- `concurrency -> storage`
- `durability -> storage`

It also needs an explicit **engine -> storage ownership correction**.

The most likely broad shape is:

- keep the semantic/domain side of engine where it is
- keep the substrate/mechanical side of storage where it is
- move lower-runtime checkpoint/recovery/snapshot/durability mechanics from
  engine down into storage
- keep only the public database/runtime orchestration API in engine

In short:

- `storage` is mostly already the right crate
- `engine` still hosts too much lower-runtime implementation
- the next plan must treat that as part of the consolidation, not as a later
  afterthought
