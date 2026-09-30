# L8. Lifecycle / Recovery / Maintenance

Status: current — describes shipped 1.2.x behaviour (#3134)

Depends on:

- [L1. Backend IO](./l1-backend-io.md)
- [L2. Object Layout](./l2-object-layout.md)
- [L3. Durable Format / Codec](./l3-durable-format-codec.md)
- [L4. Log / Manifest / Snapshot Services](./l4-log-manifest-snapshot-services.md)
- [L5. Table Runtime](./l5-table-runtime.md)
- [L6. Branch-Isolated LSM Runtime](./l6-branch-isolated-lsm-runtime.md)
- [L7. Commit Runtime](./l7-commit-runtime.md)

Consumed by:

- [L9. Storage API Boundary](./l9-storage-api-boundary.md)
- engine, through storage lifecycle outcomes and maintenance facts

## Purpose

L8 owns storage lifecycle orchestration.

The lower layers define backend operations, object names, durable bytes,
durable services, table mechanics, branch-aware LSM state, and commit ordering.
None of those layers should decide when a database is opened, recovered,
checkpointed, compacted, pruned, repaired, quiesced, or closed.

L8 is that orchestration layer.

It is still storage-internal. It should expose raw storage facts upward, but it
must not turn those facts into product meaning. Engine decides product open
policy, public error wording, IPC behavior, primitive snapshot reconstruction,
and user-facing recovery UX.

## Core Decision

Storage should have one explicit lifecycle layer rather than scattered
maintenance helpers.

The target shape is:

```text
engine open request
  |
  v
L9 storage boundary
  |
  v
L8 LifecycleRuntime
  validate backend/storage mode capabilities
  open or create durable services
  recover storage state
  classify raw storage health
  build commit runtime
  start storage maintenance hooks
  |
  v
StorageRuntime
  commit, read, scan, history, checkpoint trigger, close
```

Maintenance should be automatic during normal use. Users should not have to
think about flush, compaction, checkpoint, pruning, WAL truncation, quarantine,
or recovery.

L8 should make those operations observable and testable storage internals.

## Semantic Decisions

### Compaction Shape Policy

Storage uses the segmented level-target calculation for lifecycle
compaction pressure. Empty layouts start L1 at 1 MiB, and each deeper nonzero
level multiplies the previous target by 10. Non-empty layouts derive the base
target from the largest populated nonzero level, clamp the base between 1 MiB
and 256 MiB, and raise the base level when the unclamped base would be too
small. Table compaction output target bytes are separate from level pressure
target bytes; table output sizing remains owned by the table runtime compaction
configuration.

Queued nonzero compaction uses a compact pointer for each nonzero level. It
selects the first table whose max physical key is greater than the pointer and
wraps to the first table when all current tables are at or before the pointer.
Ordinary write-pressure scoring skips the final configured level; bottommost
consolidation is only selected by an explicit level-scoped maintenance task.
Stateless maintenance-task conversion is only allowed to build L0 compaction
requests; nonzero queued execution must inspect current branch state before
producing a branch compaction request.

Grandparent-overlap output split budgeting remains owned by the lower branch and
table compaction layers. L8 records deeper-overlap bytes and a deferred
split-budget fact, but metadata-promotion eligibility does not imply lifecycle
ownership of output splitting.

The write amplification these shape rules produce is derived term by term, with
the SD/flash endurance envelope and the operator levers, in
[Write Amplification And Flash Endurance](./write-amplification-and-flash-endurance.md)
(SCALE-005, #2906).

### Resource Throttling And Memory Release

Lifecycle compaction records table bytes read for rewrite operations, output
bytes written, metadata-only bytes avoided, elapsed time, and weighted bytes
rewritten per input row for completed rewrites. Metadata-only promotion reports
the source bytes it avoided rewriting without consuming the compaction IO byte
budget. The default compaction IO policy is unlimited, but
`LifecycleCompactionIoPolicy::PerTaskByteBudget` can bound a single maintenance
task deterministically. A compaction whose estimated input-plus-output bytes
exceed that per-task budget is deferred with a retryable maintenance outcome and
telemetry health debt rather than reported as a failure.
Queued and explicit fixed-point compaction drains share this policy. Crate-internal
single-rewrite helpers remain raw building blocks, but still emit
compaction facts when used directly.

Flush pressure preempts queued compaction for the same branch. If frozen mutable
state is present when a compaction task starts, lifecycle defers the compaction
with a retryable outcome so the flush drain can make progress first.

Memory release remains measure-first. Flush drains record active and frozen
mutable bytes before and after the drain plus a retained-byte re-evaluation
threshold. Storage does not call nonportable allocator release hooks in V1;
the counter surface is the handoff for deciding whether such a hook belongs in
storage or a lower allocator/backend layer.

### Snapshot And Pruning Ownership

Snapshot object pruning is separate from source-shape maintenance. Storage
does not implement an implicit `set_snapshot_floor` or `gc_safe_point`
equivalent during flush, compaction, materialization, checkpoint, or close.
Snapshot-floor advancement is owned by the caller-supplied retention proof: the
public snapshot lifecycle above storage decides the floor, persists any public
snapshot state, and passes manifest-derived proof facts down to lifecycle.

Pruning callers are explicit retention and snapshot-pruning maintenance
requests, plus the two proof-driven modes of the space-reclamation contract
(DUR-019, 1.2.6). Every caller's proof shape is the current manifest snapshot
id plus snapshot watermark, with recovery-health facts attached; incomplete or
unsafe proofs defer before backend deletion. The explicit verb prunes by the
requested newest-snapshot window. `SnapshotPruneMode::Superseded` is chained
after every completed checkpoint and at a clean close: it deletes only ids
below the live one, never one at or above it, which may be a publish in flight.
`SnapshotPruneMode::ReconcileToAttested` is queued at reopen: it deletes every
snapshot but the attested one (including crash orphans), and it is safe because
nothing publishes during the open's reclaim-only window. Flush drains,
compaction chains, materialization, and benchmark source-shape drains still
never advance the floor or prune snapshots.

Durability and recovery remain tied to the manifest proof. Recovery reloads the
current manifest snapshot facts and rebuilds retention proof state on demand
rather than replaying a lifecycle-owned floor variable. Benchmarks must report
source-shape maintenance separately from pruning; pruning counters are proof
diagnostics, not evidence that compaction or flush moved the retention floor.

### Maintenance Coverage Trigger Model

Storage runs an in-process maintenance coverage pass after a successful
mutating commit has evaluated the committing branch's post-commit maintenance
pressure. The pass scans the deterministic live branch list and can discover
quiet branches with frozen-table, L0, nonzero-level, or inherited-layer
backlog.

The committing branch is inspected for scan accounting but is not enqueued by
the coverage pass, because the source branch was already handled by the normal
post-commit scheduler. Quiet branches enqueue only the current pressure
suggestion returned by the storage pressure model, preserving the existing
flush-before-compaction ordering for each branch. Coverage tasks coalesce by
their branch and maintenance scope, so repeated coverage passes cannot grow an
unbounded duplicate queue.

`DeterministicInline` policy still runs only the source branch's post-commit
task inline. Cross-branch coverage work is queued deterministically and is not
driven inline by the coverage pass. `Disabled` policy skips coverage entirely.

Coverage idle rounds are consecutive coverage passes, triggered by later
mutating commits, that find no eligible quiet-branch work. (Reclaim has its
own idle trigger since 1.2.6: while reclaim is owed, one delayed low-tier wake
arms on the `MaintenanceClock` after the writer goes quiet; see DUR-019.) The in-process idle anchor consumes at most five
idle rounds before recording an idle-limit stop. Healthy, idle-limit,
queue-full, and failure stops are recorded separately. Coverage does not enqueue
ordinary maintenance when close-required drain or closing state owns the
lifecycle.

## Responsibilities

L8 owns:

- storage open/create mechanics below engine policy
- backend capability validation for requested storage mode
- storage-mode specific lifecycle selection
- raw recovery execution
- snapshot/WAL/segment recovery orchestration
- storage recovery health classification and reporting
- lossy or degraded storage recovery facts
- commit-runtime bootstrap from recovered facts
- storage writer/flush lifecycle below engine admission policy
- flush scheduling hooks
- flush watermark maintenance
- checkpoint scheduling hooks and explicit checkpoint execution
- WAL truncation trigger mechanics
- table/branch compaction scheduling hooks
- inherited-layer materialization scheduling hooks
- retention/pruning mechanics
- quarantine/reclaim/purge orchestration
- repair and reconciliation mechanics
- storage shutdown/close ordering below product close policy
- storage maintenance metrics and health facts
- lifecycle fault injection hooks
- lifecycle characterization, crash, and recovery test seams

L8 does not own:

- public database open policy
- IPC
- multi-process product behavior
- public lifecycle commands
- user-facing maintenance workflows
- primitive snapshot materialization
- JSON, event, graph, vector, search, or intelligence semantics
- product branch workflows
- product time-travel UX
- product recovery wording
- ACID marketing claims
- StrataHub fleet behavior
- distributed consensus
- backend IO implementation
- byte format layout
- table merge algorithms
- commit version allocation
- public transaction sessions
- follower mode

## Layer Boundary

```text
L9 Storage API Boundary
  exposes storage open, commit, read, maintenance facts
        |
        v
L8 Lifecycle / Recovery / Maintenance
  orders recovery, flush, checkpoint, compaction, retention, repair, close
  (the background drain services pending low-tier work — retention/
   quarantine/purge/repair — by suspected reclaim debt as well as a minimum
   ratio (`should_service_low_tier`), so sustained flush/compaction load
   cannot starve table-object reclaim)
        |
        +--> L7 Commit Runtime
        |      quiesce, commit facts, visible version, commit bootstrap
        |
        +--> L6 Branch-Isolated LSM Runtime
        |      branch/table recovery facts, flushable state, reachability facts
        |
        +--> L5 Table Runtime
        |      table validation, table build/merge/repair primitives
        |
        +--> L4 Durable Services
        |      WAL, manifest, snapshot, table manifest, quarantine publish
        |
        +--> L3/L2/L1
               codecs, layout, backend capability and IO
```

Lower layers expose facts and primitives. L8 chooses the operation order.

L8 may call lower layers. Lower layers must not call L8.

L8 should not expose lower-layer implementation objects directly to engine.
Engine should receive typed storage outcomes through L9.

## Current Code Reference Map

The current implementation already has most L8 mechanics, but they are split
between engine database orchestration and storage durability/segmented modules.

### Engine-Owned Lifecycle Evidence

- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/recovery.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/compaction.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/background.rs`

Current roles:

- `open_finish` performs primary open sequencing: recovery, support directory
  creation, WAL writer construction, runtime config application, flush thread
  start, and post-recovery compaction scheduling.
- `spawn_wal_flush_thread` owns periodic WAL sync, sync-failure latching, and
  final flush on shutdown.
- `Database::run_recovery` bridges storage-owned recovery with engine-owned
  primitive snapshot install and product recovery policy.
- `Database::checkpoint` flushes WAL, quiesces commits, collects primitive
  checkpoint data, calls storage checkpoint mechanics, and triggers pruning.
- `Database::compact` calls storage WAL compaction.
- `schedule_flush_if_needed` and `schedule_background_compaction` submit
  background flush, materialization, and compaction tasks.
- `maybe_apply_write_backpressure` gates writes based on L0 count, memtable
  pressure, and segment metadata pressure.
- `shutdown_with_deadline` owns ordered close: reject new transactions, drain
  background work, wait for idle commits, stop flush thread, flush WAL, sync
  manifest, run freeze hooks, release registry and file lock.

These are L8-shaped operations, but engine currently also owns product policy,
primitive callbacks, subsystem freeze hooks, and public error mapping. Those
parts should stay above storage.

### Storage-Owned Durability Evidence

- `crates/storage/src/durability/recovery_bootstrap.rs`
- `crates/storage/src/durability/recovery.rs`
- `crates/storage/src/durability/checkpoint_runtime.rs`
- `crates/storage/src/durability/compaction/wal_only.rs`
- `crates/storage/src/durability/disk_snapshot/checkpoint.rs`

Current roles:

- `run_storage_recovery` prepares MANIFEST/codec state, constructs
  `SegmentedStore`, drives snapshot and WAL replay, applies mechanical lossy
  fallback, applies runtime config, and runs segment recovery.
- `RecoveryCoordinator` reads MANIFEST, snapshot, and WAL records and applies
  them through callbacks.
- `run_storage_checkpoint` loads or creates MANIFEST, updates active WAL
  segment, writes a snapshot, and persists snapshot watermark.
- `compact_storage_wal` and `truncate_storage_wal_after_flush` use snapshot and
  flush watermarks to remove covered WAL segments.
- `sync_storage_manifest` is the storage helper used by shutdown manifest sync.
- `prune_storage_snapshots` retains newest snapshots plus the live MANIFEST
  snapshot and removes old snapshot files.

These are the closest current approximation of storage-owned L8 mechanics.
They should become lifecycle services over L4 rather than standalone helper
functions over paths.

### Branch-LSM Maintenance Evidence

- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/compaction.rs`
- `crates/storage/src/segmented/recovery.rs`
- `crates/storage/src/segmented/quarantine_protocol.rs`
- `crates/storage/src/segmented/ref_registry.rs`
- `crates/storage/src/compaction.rs`

Current roles:

- `flush_oldest_frozen` builds and installs L0 table files for branch-local
  frozen mutable tables.
- `recover_segments` rebuilds branch/table state from table manifests and
  classifies storage recovery health.
- `compute_compaction_scores`, `pick_and_compact`, and `compact_level` select
  and perform branch-local compactions.
- `delete_segment_if_unreferenced` routes old tables into quarantine rather
  than deleting them immediately.
- `quarantine_segment_if_unreferenced`, `purge_all_quarantines`,
  `retention_snapshot`, and `reconcile_quarantine_on_recovery` implement the
  current reclaim and repair protocol.
- `SegmentRefRegistry` is a runtime accelerator and deletion barrier for
  shared immutable table references.
- `CompactionScheduler` is a small current selection helper for tiered merge
  candidates.

This evidence shows that L8 must coordinate with L6, not replace it. Branch
LSM remains the owner of branch/table reachability facts. L8 decides when to
flush, compact, materialize, quarantine, purge, and repair.

### Removal Evidence

- `crates/engine/src/database/refresh.rs`
- follower paths in `database/open.rs`, `database/recovery.rs`, and
  `database/lifecycle.rs`

Follower mode is not part of storage. It currently adds persisted follower
state, refresh gates, blocked transaction watermarks, and alternate recovery
paths. The V1 product direction is IPC for multi-user local access, not
follower mode. L8 should not preserve follower-specific lifecycle concepts.

## Target Concepts

L8 should use a small set of repeatable concepts.

### StorageLifecycleRuntime

`StorageLifecycleRuntime` is the opened storage runtime below L9.

It owns references to:

- backend capability facts
- layout
- durable services
- branch LSM runtime
- commit runtime
- recovery health
- maintenance scheduler or hooks
- lifecycle health state
- storage metrics

It should not contain engine primitive registries, IPC handles, product
subsystems, or public user command state.

### StorageOpenPlan

`StorageOpenPlan` describes how storage should open before side effects begin.

It should include:

- storage mode: cache or durable local
- backend handle/configuration
- layout root
- durability policy: none for cache, `standard` or `always` for durable modes
- runtime config
- codec config
- recovery strictness knobs that are storage-mechanical
- lifecycle hook configuration for tests/fault injection

It should not include product access mode, IPC fallback behavior, primitive
extension lists, or engine subsystem wiring.

Capability validation must run before creating MANIFEST, WAL, table, or
snapshot objects.

### StorageOpenOutcome

`StorageOpenOutcome` should report raw storage facts:

- opened existing vs created new
- backend capability set used
- database UUID, if durable mode owns one
- codec id
- recovered visible version
- recovered max transaction id, if retained
- snapshot recovery facts
- WAL replay facts
- segment recovery facts
- degraded recovery health
- lossy fallback facts
- maintenance state

Engine may turn these facts into product diagnostics. Storage should not
hide or over-interpret them.

### RecoveryHealth

Storage recovery health should stay storage-shaped.

The current `RecoveryHealth`, `DegradationClass`, and `RecoveryFault` direction
is sound:

- `Healthy`
- `Degraded { faults, class }`
- classes such as `DataLoss`, `PolicyDowngrade`, and `Telemetry`
- typed faults for corrupt tables, corrupt manifests, missing manifest-listed
  objects, inherited-layer loss, no-manifest fallback, IO failures, and
  quarantine inventory mismatch

Cutover note: these types are storage-owned in V1. They currently appear
on the engine public surface; the V1 cutover must either re-export/wrap
storage-owned recovery health through engine or retire the engine-owned
definitions.

Storage should classify the facts. Engine decides whether a product open
accepts or rejects a degraded outcome.

### MaintenanceTask

L8 should make maintenance work explicit.

Expected task families:

- flush frozen mutable tables
- update flush watermark
- truncate covered WAL objects
- compact branch table levels
- materialize inherited layers
- write checkpoint
- prune snapshots
- quarantine unreferenced table objects
- purge safe quarantine inventory
- repair or reconcile storage metadata
- collect storage health/metrics

Tasks should be deterministic and unit-testable. A task should have a clear
input, a clear output, and a bounded set of lower-layer calls.

### MaintenanceScheduler

The scheduler should be storage-internal and replace ad hoc task submission.

It should support:

- explicit task priorities
- coalescing of redundant flush/compaction work
- drain-before-close (the reclaim kinds — retention, quarantine, purge,
  snapshot pruning — via `close_policy_for_kind`)
- cancellation before close (every other kind)
- metrics
- deterministic single-threaded execution in tests
- fault injection at task boundaries

The current engine `BackgroundScheduler` is useful evidence, but storage
should avoid coupling maintenance scheduling to engine product objects.

### CheckpointPlan

Checkpointing is storage-owned for committed row state.

Storage can own:

- commit quiesce request against L7
- row-native committed storage-state collection
- durable snapshot object publication through L4
- MANIFEST snapshot watermark update
- WAL truncation eligibility after checkpoint
- snapshot retention/pruning mechanics

Engine must own:

- optional derived-state checkpoint sections
- optional derived-state install or rebuild policy during recovery
- user-facing checkpoint diagnostics

The clean boundary is row-native storage snapshots for committed rows, plus
optional opaque engine-owned sections for derived or rebuildable state. L8 can
orchestrate the checkpoint without materializing graph, vector, JSON, event, or
search DTOs.

### RetentionPlan

Retention should be fact-driven.

Inputs:

- live MANIFEST snapshot
- newest snapshot count
- branch/table reachability facts from L6
- quarantine inventory
- recovery health gate
- backend capability facts

Outputs:

- retained object set
- pruned object set
- skipped object set with reasons
- reclaimed bytes where known
- warnings/faults

Retention must never delete data that is only probably unreachable. If the
proof is incomplete, the correct behavior is to keep the object and report
retention debt.

### QuarantineProtocol

Quarantine is a safety buffer between "not referenced now" and "deleted
forever."

Storage should preserve the current staged idea:

1. block reclaim under unsafe degraded recovery
2. prove the object is not referenced by live manifests and inherited layers
3. publish quarantine inventory durably
4. move or mark the object as quarantined
5. purge later only after a fresh safe proof

For local filesystem, quarantine may be implemented by rename/move. For object
backends, it may need to be a manifest-marked state because rename is not a
portable primitive.

L8 owns the protocol. L4 owns durable publication. L6 supplies reachability
facts.

### StorageClosePlan

Storage close should be ordered and idempotent.

Storage-owned close sequence:

1. stop accepting new storage commits and stop the background workers
2. cancel queued maintenance that is not drain-before-close
3. run the bounded reclaim drive: a fresh table-object mark, then sweep and
   purge rounds within `ReclaimBudget` (default 500 ms; `Disabled` skips it)
4. flush every branch whose unflushed delta reaches 64 KiB into tables, then
   checkpoint every branch, truncate the WAL behind the snapshot, and prune the
   snapshot it superseded (skipped for a disabled budget, a structural
   deferral such as a fresh fork's inherited layers, or nothing new)
5. wait for L7 commit quiescence or return a typed timeout
6. flush durable WAL state when durability requires it
7. persist required storage manifests
8. publish final storage health facts
9. release storage-owned backend guards/leases/locks

Every close-time reclaim step is best-effort: a failure or deferral is health
debt, never a close failure, and whatever it leaves the next open settles
(DUR-019).

Engine-owned close sequence may wrap this with primitive freeze hooks,
IPC shutdown, product handle registry release, and public error mapping.

## Lifecycle State Model

L8 should have an explicit state machine.

Minimum states:

```text
New
  -> Opening
  -> Recovering
  -> Open
  -> Closing
  -> Closed

Opening/Recovering/Closing
  -> Failed
```

The state machine should define:

- which operations are allowed in each state
- whether commits are accepted
- whether reads are accepted
- whether maintenance tasks are accepted
- whether close can be retried
- which failures are sticky

Close should be idempotent after `Closed`.

Open failures should clean up temporary objects when the backend can do so
safely, but should not delete existing durable objects unless a lower-layer
operation has a precise rollback contract.

## Recovery Sequence

Durable recovery should be explicit and testable:

```text
validate backend capabilities
validate codec configuration
load or create database manifest according to storage mode
open durable services
load checkpoint snapshot if present
ask engine callback to install primitive snapshot payload, if snapshot exists
replay WAL records after checkpoint/flush watermark
recover branch/table manifests and table objects
reconcile quarantine inventory
classify recovery health; per branch, an orphaned checkpoint delta (a branch the
  snapshot records as holding a durable base whose table manifest is gone)
  recovers its WAL prefix with data-loss health
bootstrap commit runtime from recovered max version/txn facts
on reopen: queue the table-object mark, the ReconcileToAttested snapshot prune,
  and a quarantine purge when quarantine is non-empty; arm one reclaim-only wake
return StorageOpenOutcome
```

`open` runs no reclaim inline (DUR-018); the armed wake does it, admitting only
the reclaim tier until the first commit.

Cache mode skips durable recovery and starts from empty storage state.

Follower recovery does not exist in storage.

## Flush And WAL Truncation Sequence

Flush is the bridge between mutable branch state and durable table state:

```text
L7/L6 indicates frozen mutable tables exist
L8 schedules or runs flush task
L6 snapshots oldest frozen table
L5 builds table object
L4 publishes table object and branch/table manifest
L6 installs table into branch LSM state
L8 updates global flush watermark when safe
L4 truncates covered WAL objects
L8 reports outcome and health
```

The flush watermark is a global lower bound over flushed branch state. Branches
without flushed table state must not advance the watermark by absence.

Flush publication failure after partial progress should surface as storage
health, not as silent success. If data is visible but manifest durability is
unconfirmed, L8 should preserve that fact until recovery proves the state clean
again.

## Checkpoint Sequence

Checkpointing should be storage-ordered but primitive-neutral:

```text
reject if storage is closing (the close path's own checkpoint excepted)
quiesce commits through L7
obtain checkpoint watermark
ask engine for primitive-neutral checkpoint payload
L4 writes snapshot/checkpoint object
L4 updates database manifest snapshot watermark
L8 optionally schedules WAL truncation
L8 optionally schedules snapshot pruning
return raw checkpoint outcome
```

Storage must not reconstruct graph, vector, JSON, event, search, or
intelligence semantics during checkpoint. It receives bytes or storage-shaped
snapshot sections.

## Compaction And Materialization Sequence

Compaction should be split cleanly:

- L5 owns table merge algorithms.
- L6 owns branch level state and inherited-layer visibility.
- L8 owns scheduling, coalescing, cancellation, and health reporting.

The current design where compaction also triggers inherited-layer
materialization is reasonable, but it should be explicit:

```text
flush task may create new L0 tables
L8 schedules compaction task
L6 reports branch/level compaction candidates
L5 merges tables
L4 publishes output table and manifest
L6 swaps branch level state
L8 quarantines replaced tables when unreferenced
L8 wakes write backpressure waiters
L8 may run inherited-layer materialization when no compaction is ready
```

Write stalls should be tied to storage facts such as L0 count, mutable-table
memory, and metadata pressure. The product error policy belongs above storage,
but L8 should expose typed stall facts.

L8 scheduling should be driven by the resolved storage runtime budget supplied
through L9. Storage owns how to react to storage pressure, but engine owns the
product resource profile that decided the memory envelope, worker limits, and
maintenance posture.

## Health And Metrics

L8 should expose raw health and metrics suitable for engine, tests, and
diagnostic tools.

Health facts:

- recovery health
- last durable publish failure
- WAL writer/sync health
- maintenance scheduler health
- quarantine debt
- retention debt
- degraded table/manifest facts
- backend capability mismatch

Metrics:

- recovery duration and replay counts
- WAL bytes/segments
- snapshot count and bytes
- table count and bytes by branch/level
- frozen mutable-table count
- flush task counts and failures
- compaction task counts and reclaimed bytes
- quarantine object count and bytes
- maintenance queue depth and active tasks
- write stall counts and durations
- selected storage runtime budget facts

These facts should stay raw. Engine can render them for CLI, Strata AI, or
future StrataHub diagnostics.

## Backend Requirements

L8 must not assume local filesystem semantics.

Cache/browser mode:

- no crash recovery promise
- no durable checkpoint promise unless the backend explicitly supports it
- no WAL, MANIFEST, snapshot, checkpoint, table, or quarantine objects
- no `standard` or `always` durability policy
- L6 branch state and L7 commit/version/timestamp state are in-memory only
- maintenance is memory cleanup, compaction of in-memory structures where
  useful, and metrics
- close is best-effort

Local filesystem durable mode:

- requires durable publish and sync barriers
- supports exclusive local writer guard
- supports crash recovery from WAL, MANIFEST, snapshots, and tables
- supports `standard` and `always` durability policies
- is the V1 reference implementation

Future object-store/OpenDAL-backed mode:

- must declare capabilities honestly
- should not rely on rename or append unless the adapter provides a correct
  emulation
- may require chunked WAL, lease/lock services, and manifest generation checks
- should fail open if requested durability cannot be provided

Storage should design L8 with object-store constraints in mind, but the
first implementation does not need to ship production S3 durability.

## Storage Mode Capability Matrix

L8 owns open-time validation of the selected storage mode against L1 backend
capabilities. This table is the V1 source of truth.

| Storage mode | Required capabilities | Required services | Explicitly absent |
| --- | --- | --- | --- |
| cache/browser-cache | read object/range, write object, delete object, list prefix, or equivalent in-memory operations | in-memory L6 branch state, L7 commit/version/timestamp allocation, raw health/metrics | durable WAL, database MANIFEST, durable snapshots, durable tables, quarantine, crash recovery, writer lock |
| durable local filesystem / standard | read object/range, write object, append object, delete object, list prefix, metadata, durable publish, durable sync, single-writer guard | WAL, database MANIFEST, row-native snapshots, table objects, branch/table manifests, quarantine, recovery, background/periodic WAL sync, close sync | hidden network sync, object-store fencing, per-commit force-durability promise |
| durable local filesystem / always | read object/range, write object, append object, delete object, list prefix, metadata, durable publish, durable sync, single-writer guard | WAL, database MANIFEST, row-native snapshots, table objects, branch/table manifests, quarantine, recovery, per-commit WAL force durability, close sync | hidden network sync, object-store fencing |
| object durable candidate | read object/range, write object, delete object, list prefix, metadata, conditional publish or conditional create/update, monotonic metadata or equivalent fence, documented list consistency | only experimental services proven by a focused design | production durability claim until conformance, recovery, retention, and compaction tests pass |

Unsupported combinations must fail before storage creates durable objects or
starts recovery.

## Failure Rules

L8 should prefer typed outcomes over stringly errors.

Important failure classes:

- capability mismatch before side effects
- codec mismatch
- manifest load/create/publish failure
- snapshot load/install/write failure
- WAL replay corruption
- lossy WAL fallback used
- branch/table manifest corruption
- table object missing or corrupt
- inherited layer lost
- quarantine inventory mismatch
- reclaim blocked by degraded recovery
- retention proof incomplete
- writer sync halted
- maintenance queue full
- close timeout waiting for commits
- backend IO failure

Failure handling rules:

1. Validate capabilities and codec before creating durable objects.
2. Never delete objects without a current reachability proof.
3. Never report recovery as healthy when data loss or policy downgrade was
   observed.
4. Preserve partial-progress facts when an operation made state visible but
   durability is uncertain.
5. Treat background maintenance failure as health debt unless the failed task
   is required for commit correctness.
6. Make close retryable until storage reaches `Closed`.

## Testing Strategy

L8 is where storage should become reference-grade.

Required test families:

- deterministic open/create tests by storage mode
- backend capability rejection before side effects
- recovery from empty database
- recovery from MANIFEST + WAL
- recovery from checkpoint-only state
- recovery from checkpoint + WAL tail
- codec mismatch recovery rejection
- WAL partial-tail truncation
- WAL corruption strict failure
- lossy WAL fallback characterization, if retained
- segment/table recovery health classification
- inherited-layer recovery and loss classification
- quarantine reconciliation
- reclaim blocked under unsafe degraded recovery
- retention proof incomplete keeps objects
- checkpoint determinism
- checkpoint then WAL truncation
- flush watermark monotonicity
- concurrent flush/branch-delete races
- compaction publish failure rollback
- shutdown/close idempotence
- close timeout and retry
- scheduler drain/cancel behavior
- crash injection between every durable publication step
- fuzz tests for recovery input ordering and corrupted manifests

The first implementation should provide a deterministic single-threaded
maintenance executor for tests. Background concurrency can be tested separately
after the operation contracts are stable.

## V1 Minimum

The V1 storage L8 minimum is:

1. cache-mode open/close with no durable recovery claim
2. local filesystem durable open/create
3. strict codec/capability validation before durable side effects
4. MANIFEST + WAL + snapshot + segment recovery
5. primitive-neutral snapshot install callback boundary
6. recovery health facts with typed degradation
7. commit-runtime bootstrap from recovered storage facts
8. flush frozen mutable tables
9. flush watermark and WAL truncation mechanics
10. explicit checkpoint execution
11. snapshot retention/pruning
12. branch/table compaction scheduling hook
13. inherited-layer materialization scheduling hook
14. quarantine/reclaim/purge protocol
15. storage close ordering with drain/quiesce/flush/sync
16. raw health and metrics
17. deterministic lifecycle/fault test seams

Not required for V1 storage:

- production OpenDAL/S3 durability
- distributed locks or consensus
- follower mode
- public manual maintenance commands
- product recovery assistant UX
- StrataHub fleet reporting
- changing the physical storage format

## Open Questions

1. Should storage keep lossy WAL fallback as a supported operator escape
   hatch, or should that become a diagnostic recovery tool outside normal open?
2. How much of the current WAL flush thread belongs inside storage L8 versus
   engine lifecycle wrapping?
3. Should checkpointing be entirely explicit at first, or should L8 include an
   automatic checkpoint policy in V1?
4. What is the minimum storage health surface engine needs for Strata AI
   and future StrataHub diagnostics?
5. Should L8 expose a maintenance scheduler trait, or a concrete deterministic
   executor with optional threaded implementation?

## Implementation Notes

Storage should avoid copying current one-off helper growth into L8.

Prefer repeatable concepts:

- one `StorageOpenPlan`
- one `StorageOpenOutcome`
- one `StorageLifecycleRuntime`
- one `MaintenanceTask` family
- one `MaintenanceOutcome` family
- one `RecoveryHealth` model
- one `RetentionPlan`
- one `QuarantineProtocol`
- one lifecycle state machine

Each operation should have a clear lower-layer dependency list and a clear
test harness. If a helper exists only to keep a temporary architecture compiling
and cannot be tested independently, it is probably not an L8 concept.
