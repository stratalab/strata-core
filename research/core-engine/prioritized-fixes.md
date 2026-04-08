# Prioritized Fix Backlog

This backlog is synthesized from the full `research/core-engine/` document set:

- the section notes in `sections/`
- the cross-cutting summaries in `storage-engine.md`, `durability-recovery.md`, and `transactions-branching.md`
- the detailed architectural gap notes in `gaps/`

The priorities here are not a measure of implementation effort.
They are a measure of architectural risk.

- `Critical`: correctness, durability, or convergence bugs that can leave the engine in the wrong state
- `High`: major semantic or operational gaps that make the runtime inconsistent, incomplete, or hard to reason about
- `Medium`: important capability, lifecycle, and performance gaps that do not appear to break the core durability contract today
- `Low`: cleanup, naming, and dormant-surface issues that matter, but should follow the higher-risk work

## Critical

### 1. Unify recovery behind one authoritative restart path

Fix:

- make `Database::open` restore from checkpoints before replaying WAL
- consume `MANIFEST` watermarks as part of the real recovery plan
- align engine and durability crate on-disk layout and recovery ownership
- make snapshot restore and codec handling part of the active open path

Why this is first:

- checkpoint creation and restart semantics currently describe two different durability models
- WAL deletion and snapshot coverage are harder to reason about until restart actually honors checkpoints
- several other fixes depend on having one real recovery architecture instead of two partial ones

Primary source docs:

- `architectural-gaps.md` gap 01
- `gaps/01-checkpoint-recovery-gap.md`
- `sections/04-recovery-crash-safety.md`

### 2. Make follower replication convergent, not best-effort hole-skipping

Fix:

- advance follower watermark only through the highest contiguous successfully applied record
- treat decode/apply failures as degraded follower state, not normal forward progress
- keep follower open and follower recovery truly read-only

Why this is critical:

- current refresh can skip a committed WAL record forever and still claim forward progress
- once that happens, the follower can become permanently divergent from primary history

Primary source docs:

- `architectural-gaps.md` gap 09
- `gaps/09-follower-refresh-divergence-gap.md`
- `sections/04-recovery-crash-safety.md`
- `sections/06-branch-isolation-git-like-versioning.md`

### 3. Fix Standard-mode WAL durability bookkeeping

Fix:

- do not clear unsynced state until out-of-lock `sync_all()` succeeds
- collapse the duplicated Standard-mode flush-thread implementations into one path
- align exposed durability knobs with actual behavior

Why this is critical:

- current Standard mode can mark data as synced before the background sync actually succeeds
- that is a durability-contract bug, not just a tuning issue

Primary source docs:

- `sections/03-write-ahead-log.md`
- `durability-recovery.md`

## High

### 4. Make WAL recovery codec-aware so durable encrypted deployments are possible

Fix:

- add codec-aware WAL envelope/read path
- validate codec identity before replay
- support replay across encrypted WAL segments and rotation

Why this is high:

- there is currently no durable encrypted deployment mode
- the engine correctly rejects unsafe configurations today, so this is not silent corruption, but it is a major product limitation
- this should follow the recovery unification work, not precede it

Primary source docs:

- `architectural-gaps.md` gap 03
- `gaps/03-wal-codec-encryption-gap.md`
- `sections/03-write-ahead-log.md`
- `sections/04-recovery-crash-safety.md`

### 5. Replace ambient runtime composition with an explicit, deterministic engine runtime

Fix:

- define one authoritative runtime bundle for subsystems, merge handlers, and DAG hooks
- make default primary and follower open use the same composition story
- fail fast on incompatible reopen requests instead of silently returning the first-opened instance
- move process-global behavior behind explicit runtime ownership where possible

Why this is high:

- branch semantics, subsystem recovery, and even cache behavior currently vary by opener order and embedding
- successful open does not guarantee production semantics
- the same database path can behave differently depending on who opened it first

Primary source docs:

- `architectural-gaps.md` gaps 04, 05, 06, 12
- `gaps/04-branch-hook-registration-gap.md`
- `gaps/05-default-open-subsystem-gap.md`
- `gaps/06-first-opener-wins-gap.md`
- `gaps/12-process-global-singletons-gap.md`

### 6. Internalize branch ancestry and unify branch identity/status

Fix:

- make merge-base computation engine-owned instead of executor-supplied
- unify branch identity across storage namespace, metadata, and DAG surfaces
- unify branch lifecycle/status into one authoritative model
- make `diff_three_way()` use the same snapshot discipline as merge planning

Why this is high:

- repeated-merge correctness currently depends on external DAG-derived override
- branch identity and status are fragmented enough that delete paths already need dual cleanup logic
- operator-facing branch inspection is weaker than actual merge planning

Primary source docs:

- `architectural-gaps.md` gap 07
- `gaps/07-merge-base-externalization-gap.md`
- `sections/06-branch-isolation-git-like-versioning.md`

### 7. Repair the write-pressure and compaction control loop

Fix:

- remove the deadlock path between L0 stalls and rate-limited compaction
- re-enable safe nonzero L0 slowdown/stop defaults
- add metrics around compaction debt, stall duration, and L0 growth

Why this is high:

- the core backpressure mechanism exists but is disabled by default because the control loop is not safe
- that leaves read amplification and ingest stability more exposed than the feature surface suggests

Primary source docs:

- `architectural-gaps.md` gap 10
- `gaps/10-write-stall-defaults-gap.md`
- `sections/02-compaction-maintenance.md`

### 8. Build a real maintenance subsystem instead of relying on compaction side effects

Fix:

- add runtime-owned maintenance scheduling
- connect `gc_safe_point()` to real automatic pruning
- either wire TTL into proactive expiration or narrow the public API to match current behavior
- decide whether `MemoryPressure` is the real policy engine and wire it accordingly

Why this is high:

- GC, TTL, and retention currently span manual APIs, compaction-time pruning, and unused utility types
- the control plane is incomplete even though the underlying compaction machinery is solid

Primary source docs:

- `architectural-gaps.md` gap 02
- `gaps/02-maintenance-ttl-gap.md`
- `sections/02-compaction-maintenance.md`

## Medium

### 9. Tighten the public OCC contract around JSON and transaction lifecycle

Fix:

- either implement path-granular JSON conflict semantics on the public engine path or narrow the claim to document-level OCC
- centralize snapshot-start semantics so they are not split across engine and concurrency layers
- add a complete abort/cleanup path for manual transactions

Why this is medium:

- the public engine path is conservative and mostly safe today, but the semantic boundary is misleading
- abandoned manual transactions can keep GC accounting conservative longer than intended

Primary source docs:

- `sections/05-transactions-occ.md`
- `transactions-branching.md`

### 10. Add a first-class event branch merge or event rebase workflow

Fix:

- implement an event-aware merge/replay strategy for concurrent same-space appends
- or expose an explicit engine-owned rebase workflow instead of hard refusal

Why this is medium:

- current refusal is safe
- this is a real primitive-capability gap, but not a core durability bug

Primary source docs:

- `architectural-gaps.md` gap 08
- `gaps/08-event-merge-gap.md`
- `transactions-branching.md`

### 11. Reduce storage metadata and open-cost pathologies

Fix:

- put harder budgeting around pinned segment metadata
- dedupe inherited-layer metadata accounting
- make flush output honor target file sizing
- consider parallelizing compaction where branch isolation allows it

Why this is medium:

- these are real resource-shaping and latency risks
- they do not look like core correctness failures in the current design

Primary source docs:

- `sections/01-core-storage-engine-lsm.md`
- `sections/02-compaction-maintenance.md`
- `storage-engine.md`

## Low

### 12. Decide whether tombstone auditing is a real product feature

Fix:

- either wire `TombstoneIndex` into live delete/retention/compaction paths and recovery
- or remove/narrow the dormant surface so the architecture is less misleading

Why this is low:

- the current engine already has MVCC tombstones for correctness
- the separate tombstone-audit subsystem is dormant rather than actively breaking the runtime

Primary source docs:

- `architectural-gaps.md` gap 11
- `gaps/11-tombstone-audit-gap.md`
- `sections/02-compaction-maintenance.md`

### 13. Clean up misleading names and feature-surface mismatches

Fix:

- rename or redefine `LastWriterWins`, which is currently source-wins
- remove dead or drifted knobs from Standard WAL mode
- narrow oversold descriptions such as “lock-free concurrent write buffer” where the real behavior is weaker

Why this is low:

- these mismatches cause confusion and bad operator expectations
- they matter, but they should follow the correctness and runtime-architecture fixes above

Primary source docs:

- `sections/01-core-storage-engine-lsm.md`
- `sections/03-write-ahead-log.md`
- `sections/06-branch-isolation-git-like-versioning.md`

## Recommended Execution Order

If this were turned into an engineering program, the sequence I would use is:

1. Recovery and durability correctness:
   checkpoint recovery, follower convergence, Standard WAL sync correctness
2. Runtime composition and branch correctness:
   deterministic runtime bundle, subsystem/open-path cleanup, branch ancestry and identity unification
3. Control-plane hardening:
   safe write-stall defaults, real maintenance scheduler, GC/TTL integration
4. Capability and performance follow-through:
   encrypted durable mode, event merge, storage resource shaping, tombstone-audit decision

## Short Version

If only five things get funded soon, they should be:

1. unify recovery so checkpoints are real restart inputs
2. make follower refresh contiguous and convergent
3. fix Standard WAL sync bookkeeping
4. eliminate ambient/global runtime composition as the source of semantics
5. internalize branch ancestry and status instead of depending on executor/DAG overrides
