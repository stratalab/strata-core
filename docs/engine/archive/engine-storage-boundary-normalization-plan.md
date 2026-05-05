# Engine / Storage Boundary Normalization Plan

## Purpose

This document turns the storage/engine ownership audit into a concrete
normalization plan.

When this plan started, the storage consolidation work had already collapsed
the old lower-runtime split into `strata-storage`. That solved the most visible
crate graph problem, but it had not fully normalized the boundary above the
substrate. Some storage-runtime mechanics still lived under `strata-engine`,
especially around database recovery, checkpointing, snapshot install, and
storage configuration.

The goal of this plan is to finish the ownership correction without flattening
the architecture:

- `strata-storage` owns generic persistence and lower-runtime mechanics
- `strata-engine` owns database orchestration, primitive semantics, branch
  semantics, product/runtime policy, and public database APIs
- public engine APIs remain stable while lower mechanics move behind
  storage-owned implementation surfaces
- no primitive or branch-domain semantics move down into storage

This work is not primarily feature-unblocking. The crate graph is already
structurally correct: storage does not depend on engine. The return on this
work is architectural clarity and future-proofing before engine absorbs more
semantic/runtime crates. A broader engine crate is acceptable only if its
internal boundary between substrate mechanics and engine semantics remains
explicit.

At the end of this plan:

- engine should expose checkpoint, compact, open, recovery, and health APIs
  as database/runtime operations
- storage should physically own the substrate mechanics behind checkpointing,
  WAL compaction, generic decoded-row snapshot install, manifest preparation,
  storage recovery, and storage-only config application
- engine should be easier to describe as semantic/runtime orchestration rather
  than a mixed host for lower persistence machinery

This plan should be read together with:

- [engine-crate-map.md](../engine-crate-map.md)
- [engine-pending-items.md](./engine-pending-items.md)
- [engine-error-architecture.md](./engine-error-architecture.md)
- [../storage/storage-charter.md](../../storage/storage-charter.md)
- [../storage/storage-crate-map.md](../../storage/storage-crate-map.md)
- [../storage/storage-engine-ownership-audit.md](../../storage/storage-engine-ownership-audit.md)
- [../storage/storage-minimal-surface-implementation-plan.md](../../storage/storage-minimal-surface-implementation-plan.md)
- [../storage/st2-primitive-transaction-semantics-extraction-plan.md](../../storage/st2-primitive-transaction-semantics-extraction-plan.md)
- [../storage/st3-generic-transaction-runtime-absorption-plan.md](../../storage/st3-generic-transaction-runtime-absorption-plan.md)
- [storage-runtime-boundary-api-sketch.md](./storage-runtime-boundary-api-sketch.md)

## Rewrite Rules

These rules apply to every epic in this plan.

### 1. Preserve Engine APIs, Move Mechanics

The work is not to delete `Database::checkpoint()`, `Database::compact()`,
or engine-owned open/recovery entry points.

The work is to make those APIs thin orchestration surfaces over
storage-owned mechanics where the behavior is genuinely generic substrate
runtime.

### 2. No Primitive Semantics In `storage`

Storage may own durable bytes, versioned records, generic transaction
runtime, WAL replay mechanics, generic decoded-row snapshot install mechanics,
and manifest updates.

Storage must not own:

- JSON path or patch semantics
- event-chain meaning
- vector metric/model semantics
- search indexing behavior
- branch lifecycle or workflow policy
- operator-facing branch report language

If a migration is forced to pass primitive meaning through storage, the design
is wrong and needs to be split differently.

### 3. No Silent Runtime Redesign

This workstream is about ownership and boundary correction first.

Do not change:

- recovery semantics
- data-loss/degradation policy
- checkpoint behavior
- WAL compaction behavior
- snapshot format behavior
- database open behavior

unless a subplan explicitly calls out the behavior change and its
compatibility story.

### 4. No Format Churn Without A Migration

Moving mechanics into storage does not justify changing on-disk formats.

WAL, snapshot, manifest, and segmented-store compatibility should be
preserved unless a later explicit format migration says otherwise.

### 5. Engine Owns Interpretation And Policy

Storage can return raw recovery facts, retention facts, replay outcomes,
and storage health information.

Engine owns:

- operator-facing classification
- database lifecycle decisions
- branch/report vocabulary
- product defaults
- public error conversion into `StrataError`
- executor-facing behavior through higher layers

### 6. No Transitional Dependency Drift

While this plan is in progress:

- do not add `storage -> engine`
- do not add primitive-specific behavior to storage APIs
- do not make executor call storage directly for database-runtime behavior
- do not add new engine modules that deepen lower-runtime ownership
- do not move product policy into storage as a convenience

## Starting Point

The workspace is already past the older storage consolidation milestones:

- `strata-concurrency` has been deleted
- `strata-durability` has been deleted
- `strata-core-legacy` has been deleted
- `strata-storage` now owns the lower transaction and durability runtime
- `strata-engine` now owns primitive/domain families that previously lived
  in transitional locations
- storage depends only on foundational lower crates and must stay below engine

The remaining problem at the start of this plan was not a temporary peer
crate. It was lower-runtime code still physically hosted inside engine.

The strongest original candidates were:

- [database/compaction.rs](../../../crates/engine/src/database/compaction.rs)
  - checkpoint creation
  - WAL compaction
  - snapshot pruning
  - MANIFEST watermark updates
- [database/snapshot_install.rs](../../../crates/engine/src/database/snapshot_install.rs)
  - snapshot section decoding
  - replay/install into `SegmentedStore`
- [database/recovery.rs](../../../crates/engine/src/database/recovery.rs)
  - MANIFEST preparation
  - WAL codec resolution
  - storage recovery orchestration
  - replay bootstrap
  - storage recovery degradation/lossiness plumbing
- [database/open.rs](../../../crates/engine/src/database/open.rs)
  - storage-only configuration application
  - WAL writer settings and runtime wiring
- [database/config.rs](../../../crates/engine/src/database/config.rs)
  - storage-facing config defaults and interpretation mixed with
    engine-facing config surface

Some of this code will remain engine-owned orchestration. The plan is to split
the boundary explicitly instead of moving files wholesale.

## End-State Definition

The boundary normalization is complete only when all of the following are
true:

1. Public database/runtime APIs remain engine-owned.
2. Storage physically owns generic mechanics for:
   - checkpoint construction
   - WAL compaction
   - snapshot pruning
   - storage manifest/watermark updates
   - generic decoded-row snapshot install machinery
   - storage recovery bootstrap
   - storage-only config application
3. Engine physically owns:
   - open/runtime orchestration
   - product/database lifecycle policy
   - recovery policy and operator-facing classification
   - branch and primitive semantics
   - search/runtime behavior
   - branch bundle import/export
4. Storage APIs used by engine are primitive-agnostic and branch-domain
   neutral, except for raw physical branch identifiers required by the storage
   substrate.
5. Engine converts storage facts into engine errors, health reports,
   retention reports, and product decisions explicitly.
6. No upper crate bypasses engine to drive storage recovery/checkpoint/open
   policy directly.

## Ownership Decisions Closed Up Front

The following decisions are not deferred:

- `Database::checkpoint()` remains in engine as public API.
- `Database::compact()` remains in engine as public API.
- database open and runtime composition remain in engine.
- storage owns checkpoint/WAL/MANIFEST mechanics, snapshot file mechanics, and
  generic decoded-row snapshot install.
- storage owns storage recovery and replay mechanics.
- engine owns recovery policy, public error classification, and operator
  reporting.
- storage owns storage-only config application and defaults.
- engine owns the public `StrataConfig` surface and product defaults.
- storage owns raw retention facts.
- engine owns lifecycle-aware retention reports.
- JSON/event/vector/search semantics do not move down.
- branch bundle import/export stays in engine.

## Cleanup Structure

This plan is organized as six cleanup areas. The list below is the intended
execution order.

- Boundary Baseline And Guardrails
- Checkpoint And WAL Compaction Mechanics
- Snapshot Decode And Install Mechanics
- Recovery Bootstrap Mechanics
- Storage Configuration Application
- Boundary Closeout, Engine Shape, And Documentation

Each area may get its own detailed subplan as the cleanup reaches that
surface. This document defines the main sequence and ownership contract.

## Boundary Baseline And Guardrails

### Goal

Freeze the intended storage/engine boundary before moving runtime mechanics.

Detailed execution plan:

- [boundary-baseline-and-guardrails-plan.md](./boundary-baseline-and-guardrails-plan.md)

### Scope

- inventory current lower-runtime mechanics still hosted in engine
- identify public APIs that must remain engine-owned
- identify private helper surfaces that can move to storage
- add or refresh dependency/import guardrails where useful
- decide the naming convention for storage-owned runtime helper modules

### Deliverables

1. A current code inventory for:
   - `database/compaction.rs`
   - `database/snapshot_install.rs`
   - `database/recovery.rs`
   - storage-facing parts of `database/open.rs`
   - storage-facing parts of `database/config.rs`
2. A list of engine APIs that must remain as wrappers/orchestrators.
3. A list of storage APIs needed to absorb the lower mechanics.
4. Guard commands that detect:
   - new `storage -> engine` dependencies
   - primitive semantics added to storage-owned modules
   - new lower-runtime mechanics added under `engine::database`

### Constraints

- do not move code before the split between API and mechanics is explicit
- do not treat file location alone as proof of rightful ownership
- do not create storage APIs that mention executor/product behavior

### Acceptance

- the boundary correction targets are enumerated
- follow-up epics have clear migration surfaces
- future PRs can be evaluated against written ownership rules

### Non-Goals

- no checkpoint migration yet
- no recovery migration yet
- no public API redesign

## Checkpoint And WAL Compaction Mechanics

### Goal

Move generic checkpoint, WAL compaction, snapshot pruning, and storage
manifest update mechanics from engine into storage.

Detailed execution plan:

- [checkpoint-wal-compaction-mechanics-plan.md](./checkpoint-wal-compaction-mechanics-plan.md)

Prerequisite design sketch:

- [storage-runtime-boundary-api-sketch.md](./storage-runtime-boundary-api-sketch.md)

The checkpoint/WAL cleanup is split into straight lettered phases:

- Phase A - boundary API sketch
- Phase B - checkpoint and compaction characterization
- Phase C - storage checkpoint runtime
- Phase D - engine checkpoint wrapper
- Phase E - storage WAL compaction runtime
- Phase F - engine compaction wrapper
- Phase G - residue cleanup and guard pass

The checkpoint/WAL, snapshot-install, and recovery-bootstrap cleanups are
sequential code moves, but they are not independent designs. Checkpoint output
feeds snapshot install, and snapshot install feeds recovery. Before Phase C code
moves, write a joint API sketch for the checkpoint, snapshot install, and
recovery boundary types so this cleanup does not choose a storage API shape that
the later snapshot-install or recovery-bootstrap work immediately has to undo.

### Scope

Move downward into storage where generic:

- checkpoint construction
- snapshot pruning
- WAL compaction sequencing
- manifest watermark updates
- low-level checkpoint result facts
- storage-side compaction coordination

Keep in engine:

- `Database::checkpoint()`
- `Database::compact()`
- public result translation
- lifecycle/open-state checks
- engine error conversion
- product/runtime policy about when checkpoint/compact is called

### Deliverables

1. Storage-owned checkpoint/compaction implementation surface.
2. Engine `Database` methods reduced to orchestration and translation.
3. Tests proving checkpoint and compaction behavior is unchanged.
4. No new primitive or product policy in storage.

### Constraints

- do not change checkpoint file formats
- do not change WAL compaction semantics
- do not make storage aware of engine health/report types
- do not collapse engine lifecycle checks into storage

### Acceptance

- checkpoint/WAL mechanics are physically owned by storage
- engine checkpoint/compact APIs remain stable
- recovery after checkpoint/compaction remains covered by tests
- storage APIs expose raw substrate facts, not operator-facing reports
- checkpoint characterization exists before behavior-preserving refactors
  land

### Non-Goals

- no full recovery bootstrap migration
- no snapshot install migration unless required as a narrow helper agreed in
  storage-runtime boundary API sketch
- no config split yet

### Implementation Status

The checkpoint/WAL cleanup is complete. Storage owns the generic checkpoint, WAL compaction,
snapshot-prune, MANIFEST sync, and flush-time WAL truncation mechanics through
the storage-owned
[checkpoint_runtime.rs](../../../crates/storage/src/durability/checkpoint_runtime.rs)
module, re-exported through public durability helpers such as
`run_storage_checkpoint`, `compact_storage_wal`, `prune_storage_snapshots`,
`sync_storage_manifest`, and flush-time WAL truncation support. Engine keeps
`Database::checkpoint()`, `Database::compact()`, shutdown sequencing,
flush-policy decisions, lifecycle checks, and public error/result mapping.

The intentional seam is that engine passes already materialized checkpoint
facts and public-operation context into storage helpers. Storage returns raw
checkpoint, compaction, prune, and manifest facts; engine turns those facts
into public database behavior.

## Snapshot Decode And Install Mechanics

### Goal

Normalize the snapshot install boundary without moving primitive semantics into
storage. Engine keeps `LoadedSnapshot` section dispatch and primitive snapshot
DTO decode. Storage owns only the generic mechanics for installing already
decoded storage rows into `SegmentedStore`.

Detailed execution plan:

- [snapshot-decode-install-mechanics-plan.md](./snapshot-decode-install-mechanics-plan.md)

### Scope

Move downward into storage where generic:

- validation of decoded storage row groups before mutation
- installation of decoded rows into `SegmentedStore`
- generic row/group install statistics
- storage-local errors for invalid generic row input and lower storage install
  failure

Keep in engine:

- `LoadedSnapshot` section iteration
- primitive tag dispatch
- `SnapshotSerializer` primitive DTO decode
- primitive-specific snapshot install statistics
- decisions about when a snapshot should be installed
- recovery policy around degraded or lossy outcomes
- public error/report conversion
- database lifecycle orchestration around the install

### Deliverables

1. Storage-owned decoded-row install API.
2. Engine snapshot install code that builds a complete decoded storage-row plan
   before any storage mutation.
3. Engine recovery/open code calling storage only for generic row install
   mechanics.
4. Tests proving existing snapshots still install and recover identically.
5. Guardrails proving storage install code does not import primitive snapshot
   DTOs, primitive tags, or `SnapshotSerializer`.

### Constraints

- do not change snapshot format
- do not move primitive decode semantics into storage
- do not let storage decide engine recovery policy
- do not entangle snapshot install with branch bundle import/export

### Acceptance

- `database/snapshot_install.rs` is reduced to intentional engine-owned
  primitive decode, install policy, and public error mapping glue
- storage owns the generic decoded-row install machinery
- engine still owns primitive decode, recovery decisions, and public reporting
- storage install modules do not import primitive snapshot DTOs, primitive
  tags, or `SnapshotSerializer`
- snapshot install round-trip characterization exists before
  behavior-preserving refactors land

### Non-Goals

- no branch bundle changes
- no public recovery API redesign
- no search/index recovery redesign

### Implementation Status

snapshot-install cleanup is complete. Storage owns only generic decoded-row validation and install
through `strata_storage::durability::install_decoded_snapshot_rows`. Engine
keeps `LoadedSnapshot` section iteration, primitive tag dispatch,
`SnapshotSerializer` primitive DTO decode, primitive-shaped install telemetry,
snapshot-install recovery policy, and public error mapping.

The snapshot-install correction explicitly rejected moving primitive snapshot decode into
storage. Engine builds a complete decoded-row plan before storage mutation;
storage validates and installs rows without knowing JSON, event, vector,
search, or branch-domain meaning.

## Recovery Bootstrap Mechanics

### Goal

Move lower storage recovery bootstrap and replay orchestration from engine
into storage, while keeping database-level recovery policy in engine.

Detailed execution plan:

- [recovery-bootstrap-mechanics-plan.md](./recovery-bootstrap-mechanics-plan.md)

Storage runtime boundary sketch baseline decision:

- `TransactionCoordinator` stays engine-owned during recovery bootstrap.

This is load-bearing. Storage should own recovery replay mechanics and return
raw `RecoveryStats` / `RecoveredState` facts. Engine should bootstrap
`TransactionCoordinator` from those facts after storage recovery returns. If
later work wants to move coordinator ownership, that needs a separate
coordinator ownership plan, not an incidental recovery-bootstrap side effect.

Recovery bootstrap must also preserve two current recovery ordering guarantees:

- snapshot-installed versions are folded into `RecoveryStats::final_version`
  before `TransactionCoordinator` bootstrap
- storage runtime config is applied before `recover_segments()` runs

During the recovery-bootstrap cleanup, before the storage-runtime-config cleanup
completed the public config split, engine built a storage-owned runtime config
from `StrataConfig.storage` and passed it into the storage recovery API. The
storage-runtime-config cleanup later made that adapter the standard open/config
path.

### Scope

Move downward into storage where generic:

- MANIFEST preparation
- WAL codec resolution for storage replay
- segmented-store recovery orchestration
- replay bootstrap
- raw recovery outcomes
- storage-local loss/degradation facts
- storage runtime config application needed before segment recovery

Keep in engine:

- `Database::open_runtime()` orchestration
- database mode selection
- lifecycle policy
- operator-facing recovery classification
- conversion into `StrataError` / `RecoveryError`
- subsystem recovery integration above raw storage replay

### Deliverables

1. Storage-owned recovery bootstrap API.
2. Engine recovery code reduced to orchestration, policy, and conversion.
3. Raw storage recovery outcome types that do not mention engine policy.
4. Regression tests for normal, degraded, and lossy recovery paths.

### Constraints

- do not change recovery correctness semantics
- do not weaken corruption/quarantine behavior
- do not move product policy into storage
- do not expose engine error types from storage

### Acceptance

- engine no longer performs low-level storage recovery assembly itself
- storage returns raw recovery facts and errors
- engine still decides what those facts mean for database open behavior
- recovery tests pass from storage and engine layers
- normal, degraded, and lossy recovery outcomes have characterization before
  refactoring

### Implementation Status

The recovery-bootstrap cleanup now routes `Database::run_recovery()` through
`strata_storage::durability::run_storage_recovery`. Storage owns MANIFEST and
codec preparation, `RecoveryCoordinator` replay, generic WAL record
application, mechanical lossy WAL replay fallback, storage runtime config
application before segment recovery, and `SegmentedStore::recover_segments()`.

Engine still owns the primitive snapshot-install callback, public
`RecoveryError` conversion, degraded-storage open policy,
`LossyRecoveryReport`, `TransactionCoordinator` bootstrap, follower-state
restore, and watermark construction.

The one intentional correctness tightening found during recovery bootstrap is
documented in the recovery-bootstrap behavior-change ledger: primitive snapshot
install callback failures now bypass lossy WAL replay instead of allowing an
empty lossy open.

### Non-Goals

- no broad lifecycle redesign
- no branch-domain recovery policy migration into storage
- no operator report redesign beyond necessary translation updates

## Storage Configuration Application

### Goal

Split public database configuration from storage-only runtime configuration
application.

Detailed execution plan:

- [storage-config-application-plan.md](./storage-config-application-plan.md)

### Scope

Move downward into storage where storage-only:

- config defaults
- WAL writer settings
- snapshot/checkpoint knobs
- block/cache/storage runtime knobs
- validation of storage-local config combinations
- conversion into lower runtime structs

Recovery bootstrap introduced the first storage-owned runtime config needed to
preserve recovery ordering. The storage-runtime-config cleanup completed that
split across all open paths and removed the remaining engine-side hand-written
storage setter application.

Keep in engine:

- public `StrataConfig`
- product defaults
- profile-level runtime choices
- database open policy
- compatibility behavior for existing config files
- user/operator-facing config vocabulary

### Deliverables

1. Storage-owned runtime config type or builder for storage mechanics.
2. Engine-owned adapter from `StrataConfig` into storage config.
3. Reduced storage-facing logic in `database/open.rs` and
   `database/config.rs`.
4. Tests proving existing configs still open the same way.

### Constraints

- do not make storage own product profiles
- do not make storage parse executor/CLI-facing concepts
- do not break existing config serialization without a migration

### Acceptance

- storage-only config interpretation is storage-owned
- engine remains the public config owner
- database open behavior is unchanged
- config-related tests pass across engine and CLI-facing paths

### Non-Goals

- no product bootstrap redesign
- no new config format unless explicitly required
- no executor command redesign

### Implementation Status

The storage-runtime-config cleanup is complete. Storage owns `StorageRuntimeConfig`,
`StorageBlockCacheConfig`, effective storage-value derivation, store-local
application to `SegmentedStore`, and process-global block-cache capacity
application. Engine owns public `StrataConfig` / `StorageConfig`, product and
hardware profiles, compatibility behavior, open policy, and the adapter from
public config into storage runtime config.

The old engine-side `apply_storage_config` helper and hand-written store
setter list are gone. Public `StorageConfig::effective_*` methods remain as
compatibility wrappers, but production engine storage setup uses
`StorageRuntimeConfig`.

## Boundary Closeout, Engine Shape, And Documentation

### Goal

Close the normalization workstream, make the new boundary enforceable, and
document engine's remaining responsibilities explicitly after lower mechanics
move down.

Detailed execution plan:

- [boundary-closeout-plan.md](./boundary-closeout-plan.md)

### Scope

- delete or collapse obsolete engine-side lower-runtime modules
- remove temporary forwarding/adapters that only supported migration
- refresh documentation to describe the settled architecture
- add dependency/import checks where practical
- create a final inventory of intentionally remaining engine/storage seams
- clarify internal structure for:
  - database kernel/runtime orchestration
  - product/open/bootstrap policy
  - primitive semantics
  - branch/domain workflow
  - search/runtime behavior
- record executor-owned workflow cleanup as future engine-consolidation work
  where it would otherwise blur runtime/product ownership
- keep storage-facing adapters narrow and explicit

### Deliverables

1. Updated:
   - `engine-crate-map.md`
   - `storage-crate-map.md`
   - `storage-engine-ownership-audit.md` or a successor closeout note
2. A clear internal ownership split for engine runtime/product/primitives.
3. Guardrails preventing:
   - primitive semantics in storage
   - storage mechanics regressing into engine
   - direct upper-layer bypasses of engine runtime policy
4. A final list of accepted split surfaces and why they remain split.
5. Follow-up notes for executor-owned workflows that may need to move into
   engine during the later engine-consolidation phase.
6. No reintroduction of lower-runtime mechanics into engine.

### Constraints

- do not leave migration-only APIs as permanent surface area
- do not preserve old module names if they now misrepresent ownership
- do not rewrite history in docs; mark what changed and why
- do not move primitive semantics down
- do not move executor presentation concerns into engine
- do not use this epic as an excuse for broad product redesign

### Acceptance

- the documented crate maps match the code
- lower storage mechanics are storage-owned
- engine public APIs remain stable
- remaining cross-boundary calls are explicit and justified
- engine can be described as orchestration and semantics, not storage
  mechanics
- executor workflow cleanup is recorded as future engine-consolidation work
  rather than treated as a storage-boundary deliverable
- docs and code structure agree on the main engine pillars

### Non-Goals

- no new architecture initiative after closeout
- no CLI presentation changes
- no broad search redesign unless separately planned
- no unrelated executor/CLI cleanup

### Implementation Status

The final documentation pass completes the closeout for the storage boundary. The full
workstream is not complete until the final broad regression gate below is run
and recorded. The closeout keeps the remaining seams explicit rather than
trying to remove every engine call into storage.

The accepted post-boundary closeout shape is:

- storage owns generic mechanics and raw substrate facts
- engine owns public database APIs, lifecycle/open policy, primitive and
  branch semantics, product config, and public error/report conversion
- storage test-observability helpers remain behind test/fault-injection gates
  and are guarded against production engine dependence
- block-cache capacity remains process-global storage runtime state:
  sequential applications overwrite earlier values, while concurrent opens
  race with no deterministic winner
- direct upper-crate storage dependencies are current transitional workspace
  shape and must not bypass engine for checkpoint, recovery, open, retention,
  or database policy

## Program Verification Gates

Each epic should prove correctness from both below and above.

These surfaces are recovery/durability/storage-runtime surfaces. Later code
movement PRs should be treated as high-assurance refactors: characterize
current behavior before moving code, then prove parity after the move.

The recurring verification gates for this workstream should include:

- `cargo check --workspace --all-targets`
- `cargo test -p strata-storage --quiet`
- `cargo test -p strata-engine --quiet`
- `cargo test -p strata-executor --quiet`
- `cargo test -p strata-intelligence --quiet`
- `cargo test -p stratadb --tests --quiet`

Where feature availability or local vendored dependencies block a full gate,
the epic should record the blocker explicitly and run the closest valid
subset.

Ownership guard checks should include:

- `cargo tree -p strata-storage --depth 2`
- `cargo tree -p strata-engine --depth 2`
- `cargo tree -i strata-storage --workspace --depth 2`
- `rg` checks for forbidden imports or primitive vocabulary in moved
  storage modules
- `rg` checks for remaining lower-runtime mechanics in engine database
  modules after each epic

The exact guard commands can evolve. The principles cannot:

- no `storage -> engine`
- no primitive semantics below engine
- no public engine API break from mechanical moves
- no silent on-disk format churn
- no operator/product policy moved into storage

Characterization gates should include, before the relevant epic changes code:

- checkpoint determinism and manifest watermark behavior
- checkpoint then compact then reopen behavior
- snapshot install round-trip coverage for KV, graph-as-KV, event, JSON,
  branch, vector config, vector rows, tombstones, TTL, and timestamps
- recovery outcomes for normal replay, degraded storage, lossy fallback, and
  follower state restore where applicable
- performance characterization appropriate to the touched surface, such as
  redb/YCSB-style storage paths and search benchmarks where recovery affects
  search-facing state

## Final Closeout Gate

This plan is not complete until all of the following are true at once:

1. `strata-storage` owns generic persistence, transaction, durability,
   checkpoint, snapshot file, generic decoded-row snapshot install, recovery,
   and storage-config mechanics.
2. `strata-engine` owns database orchestration, public APIs, primitive
   semantics, branch semantics, search/runtime behavior, and product policy.
3. Engine database modules no longer host low-level storage recovery,
   generic decoded-row snapshot install, or checkpoint/WAL compaction
   implementations.
4. Storage APIs used by engine expose raw substrate facts and storage-local
   errors, not engine reports or product vocabulary.
5. Engine converts storage facts into `StrataError`, health reports,
   retention reports, and public database behavior explicitly.
6. The broad regression suites above storage remain green.
7. The crate maps and ownership audit have been refreshed to describe the
   settled boundary.

That is the real closeout condition: not fewer files in engine, but a clear
and enforceable split between substrate mechanics and engine semantics.
