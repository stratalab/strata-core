# Major Architectural Gaps

These are the biggest mismatches between the feature surface and the current code path.
This list is intentionally evidence-based; each item points to the code that demonstrates the gap.

## 1. Checkpoints exist, but checkpoint-based recovery does not

### What the code does today

- `Database::checkpoint()` writes crash-safe snapshots and updates MANIFEST watermarks (`crates/engine/src/database/compaction.rs:49`).
- `Database::open_finish()` recovers by WAL replay plus segment recovery, not by loading a checkpoint (`crates/engine/src/database/open.rs:728`, `crates/engine/src/database/open.rs:839`).
- `strata_concurrency::RecoveryCoordinator` explicitly documents snapshot recovery as not implemented (`crates/concurrency/src/recovery.rs:11`, `crates/concurrency/src/recovery.rs:69`, `crates/concurrency/src/recovery.rs:310`).

### Why this is a gap

The engine can create checkpoints, but checkpoints are not part of the authoritative open-path recovery model.
That means:

- checkpoints do not reduce restart cost the way their API implies
- snapshot watermarks cannot safely drive WAL deletion by themselves
- the durability model is harder to reason about than the surface API suggests

### Evidence the codebase already treats this as a real bug

- `test_issue_1730_checkpoint_compact_recovery_data_loss` describes the exact failure mode: delete WAL after checkpoint, then reopen, and lose data because open never loads the snapshot (`crates/engine/src/database/tests.rs:1463`).
- `WalOnlyCompactor` now uses flush watermark only, with an explicit `#1730` comment, to avoid trusting snapshot coverage (`crates/durability/src/compaction/wal_only.rs:80`).

## 2. Retention and expiry maintenance are only partially implemented

### What the code does today

- `run_gc()` and `run_maintenance()` are explicitly documented as not automatic (`crates/engine/src/database/lifecycle.rs:56`, `crates/engine/src/database/lifecycle.rs:80`).
- `SegmentedStore::gc_branch()` is a no-op stub (`crates/storage/src/segmented/mod.rs:2283`).
- `SegmentedStore::expire_ttl_keys()` is also a no-op stub (`crates/storage/src/segmented/mod.rs:2289`).
- `TTLIndex` exists as a standalone type, but there is no engine call path that wires it into runtime maintenance (`crates/storage/src/ttl.rs`; usage search shows no integration outside tests and exports).

### Why this is a gap

The engine does have MVCC pruning inside compaction, but the advertised maintenance surface is broader than the runtime behavior:

- manual maintenance APIs exist without a complete maintenance engine behind them
- TTL cleanup is effectively read-time and compaction-time behavior, not an active expiration subsystem
- there is no background maintenance loop for GC/TTL

This is not a correctness bug in normal reads, but it is an architectural gap in lifecycle management.

## 3. Durable encrypted storage is blocked by WAL codec limitations

### What the code does today

- `Database::open_finish()` rejects any non-identity codec when WAL durability is enabled (`crates/engine/src/database/open.rs:705`).
- The error text is explicit that encrypted durability requires WAL reader codec support and is not yet implemented (`crates/engine/src/database/open.rs:709`).

### Why this is a gap

The engine supports configurable storage codecs and presents encryption-at-rest as a storage concern, but durable operation still depends on raw WAL readability.
So the current matrix is:

- encrypted + cache durability: allowed
- encrypted + standard/always durability: rejected

That leaves no durable encrypted deployment mode in the core engine.

## 4. Branch semantic correctness depends on embedding-level registration

### What the code does today

Graph:

- graph semantic merge must be registered before merge use (`crates/graph/src/merge_handler.rs:20`)
- without registration, the engine falls back to tactical refusal of divergent graph merges (`crates/graph/src/merge_handler.rs:23`, `crates/engine/src/branch_ops/mod.rs:299`)

Vector:

- vector semantic merge must also be registered at startup (`crates/vector/src/merge_handler.rs:38`)
- vector post-commit rebuild failures are logged and deferred to later recovery (`crates/vector/src/merge_handler.rs:188`)

Branch DAG:

- branch DAG writes are best-effort and never propagated as branch-op failures (`crates/graph/src/branch_dag.rs:8`, `crates/graph/src/branch_dag.rs:82`)
- core branch operations call DAG hooks only if present (`crates/engine/src/branch_ops/mod.rs:878`, `crates/engine/src/branch_ops/mod.rs:2108`)

Embedding:

- the executor layer explicitly performs these registrations (`crates/executor/src/api/mod.rs:79`)
- the engine open path does not

### Why this is a gap

Important branch behavior is not fully intrinsic to the engine core.
It depends on who embeds the engine and whether they remembered to perform the right global registrations.

That creates two problems:

- the engine's real semantics vary by embedding
- tests or alternate embedders can accidentally get degraded merge behavior without obvious failures during open

## 5. Plain `Database::open` does not recover the full primitive stack

### What the code does today

- `Database::open` routes to `open_internal`, which hardcodes `[SearchSubsystem]` as the subsystem list (`crates/engine/src/database/open.rs:287`).
- the same is true for follower open (`crates/engine/src/database/open.rs:535`).
- vector recovery is explicitly only available through `DatabaseBuilder` with `VectorSubsystem` (`crates/engine/src/database/open.rs:289`, `crates/vector/src/lib.rs:16`).
- `GraphSubsystem` exists, but neither `Database::open` nor the builder convenience path installs it by default (`crates/graph/src/branch_dag.rs:842`).
- the executor layer compensates by manually registering merge handlers / DAG hooks and initializing the system branch after open (`crates/executor/src/api/mod.rs:79`, `crates/executor/src/api/mod.rs:219`).

### Why this is a gap

The core engine has a pluggable subsystem model, but its default entrypoints do not produce the same recovered runtime as the production embedding.
Behavior depends on which entrypoint the caller used:

- plain engine open
- builder with explicit subsystems
- executor wrapper

That means some "core" behavior is still composition-specific rather than intrinsic.

## 6. Subsystem composition is first-opener-wins for a database path

### What the code does today

- the process keeps a global `OPEN_DATABASES` registry and returns the existing `Arc<Database>` for the same canonical path (`crates/engine/src/database/registry.rs:1`, `crates/engine/src/database/open.rs:244`).
- `open_internal_with_subsystems` explicitly says that if an existing instance was opened earlier, possibly with a different subsystem list, it returns that instance unchanged (`crates/engine/src/database/open.rs:324`).
- `open_with_config` writes `strata.toml` before routing into that path (`crates/engine/src/database/open.rs:137`).

### Why this is a gap

For a given path, the first opener effectively defines the live subsystem set for the process.
A later caller cannot reliably "upgrade" the already-open database to add missing subsystems or different recovery/freeze behavior.

This creates two classes of fragility:

- lifecycle behavior depends on opener order
- on-disk config can be rewritten for future restarts while the live in-memory database remains whatever the first opener created

## 7. Merge-base correctness for repeated merges is partially externalized

### What the code does today

- `compute_merge_base` gives priority to an executor-provided `merge_base_override` derived from DAG state, specifically to cover repeated merges and materialized branches (`crates/engine/src/branch_ops/mod.rs:1439`).
- `merge_branches` and `get_merge_base` both document that this override comes from the executor (`crates/engine/src/branch_ops/mod.rs:1845`, `crates/engine/src/branch_ops/mod.rs:2246`).
- the executor comments say that without branch DAG hooks, `compute_merge_base_from_dag` cannot advance the merge base across repeated merges of the same source/target pair (`crates/executor/src/api/mod.rs:91`).

### Why this is a gap

The engine's branch API alone does not own merge-base computation for all supported histories.
Correct repeated-merge behavior depends on:

- DAG event recording being present
- some external layer supplying the DAG-derived override

That is more than an optimization; it affects how much historical branch state a merge reconsiders.

## 8. Event logs still do not have a semantic branch merge

### What the code does today

- `check_event_merge_divergence` rejects merges when both source and target appended to the same event space since the fork (`crates/engine/src/branch_ops/mod.rs:245`).
- the error string is explicit that concurrent event appends are "not yet supported" (`crates/engine/src/branch_ops/mod.rs:286`).

### Why this is a gap

Branching is not uniformly primitive-aware.
For events, the engine protects correctness by refusing a whole class of branch merges instead of providing a semantic merge like the graph path does.

That is a valid safety choice, but it is still a feature gap in the branching model.

## 9. Follower refresh is best-effort and can permanently diverge

### What the code does today

- `Database::refresh()` explicitly documents itself as best-effort and skips individual records on payload or storage errors (`crates/engine/src/database/lifecycle.rs:121`).
- inside the loop, `max_txn_id` is advanced before decode/apply succeeds (`crates/engine/src/database/lifecycle.rs:161`).
- even if a record is skipped, the final follower watermark is still updated to that `max_txn_id` (`crates/engine/src/database/lifecycle.rs:350`).

### Why this is a gap

This is stronger than "the follower may be stale for a moment".
If refresh skips a record and still advances the watermark, that record will not be retried on the next refresh.

So the current follower design does not guarantee convergence after partial refresh failures.

## 10. L0 write stalling exists in code but is disabled by default

### What the code does today

- `Database::maybe_apply_write_backpressure()` implements L0 slowdown, L0 stop-writes, memtable pressure checks, and metadata-pressure sleeps (`crates/engine/src/database/transaction.rs:344`).
- default config sets `l0_slowdown_writes_trigger = 0` and `l0_stop_writes_trigger = 0` (`crates/engine/src/database/config.rs:167`, `crates/engine/src/database/config.rs:174`).
- the inline comment says these are disabled because enabling them with rate-limited compaction creates a deadlock (`crates/engine/src/database/config.rs:168`).

### Why this is a gap

The engine has a write-stall mechanism, but the production default is to avoid the main L0 protection path because the interaction with rate-limited compaction is unsafe.
That means the system currently relies more heavily on:

- flush scheduling
- background compaction keeping up
- soft sleeps on memory or metadata pressure

rather than on the classic L0-based stop-writes behavior that the config surface suggests.

## 11. Tombstone audit infrastructure is present but not integrated

### What the code does today

- `TombstoneIndex`, `Tombstone`, and `TombstoneReason` are implemented in `crates/durability/src/compaction/tombstone.rs`.
- repository-wide usage shows they are exported and tested, but there is no engine path that records deletes or compaction events into that index.

### Why this is a gap

The codebase has a concrete tombstone-audit data model, but it is not part of the live durability or storage pipeline.
So the feature exists as reusable library code, not as active engine behavior.

## 12. Process-global singletons reduce database-instance isolation

### What the code does today

- block cache is a process-global singleton (`GLOBAL_CACHE`) with a mutable process-global capacity (`crates/storage/src/block_cache.rs:879`).
- opening a database sets that global capacity (`crates/engine/src/database/open.rs:820`, `crates/engine/src/database/open.rs:932`).
- merge handlers and DAG hooks are also process-global `OnceCell` registrations where the first call wins (`crates/engine/src/branch_ops/dag_hooks.rs:121`, `crates/engine/src/branch_ops/primitive_merge.rs:902`, `crates/engine/src/branch_ops/primitive_merge.rs:1063`).

### Why this is a gap

The engine isolates data by database path, but not all runtime behavior by database instance.
In one process:

- one database open can resize the block cache for all others
- one embedding can install global merge/DAG behavior that all databases then share

That makes embedding order and co-location matter more than an instance-oriented architecture usually should.

## Bottom line

The strongest parts of the core engine are the LSM read/write path, WAL-first OCC commit path, and copy-on-write branch storage.

The weakest parts are the boundaries between subsystems:

- checkpoint creation vs checkpoint recovery
- storage feature surface vs active maintenance plumbing
- engine core vs embedding-registered branch semantics
- storage codec surface vs WAL durability reality

## Detailed notes

- `gaps/01-checkpoint-recovery-gap.md`
- `gaps/02-maintenance-ttl-gap.md`
- `gaps/03-wal-codec-encryption-gap.md`
- `gaps/04-branch-hook-registration-gap.md`
- `gaps/05-default-open-subsystem-gap.md`
- `gaps/06-first-opener-wins-gap.md`
- `gaps/07-merge-base-externalization-gap.md`
- `gaps/08-event-merge-gap.md`
- `gaps/09-follower-refresh-divergence-gap.md`
- `gaps/10-write-stall-defaults-gap.md`
- `gaps/11-tombstone-audit-gap.md`
- `gaps/12-process-global-singletons-gap.md`
