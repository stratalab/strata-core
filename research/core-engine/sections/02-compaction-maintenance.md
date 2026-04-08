# Compaction & Maintenance: Focused Analysis

This note is derived from the code, not from the existing docs.
Scope for this pass:

- `crates/storage/src/segmented/compaction.rs`
- `crates/storage/src/compaction.rs`
- `crates/storage/src/ttl.rs`
- `crates/storage/src/rate_limiter.rs`
- `crates/storage/src/pressure.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/config.rs`
- `crates/engine/src/database/compaction.rs`
- `crates/engine/src/database/open.rs`
- `crates/concurrency/src/recovery.rs`
- `crates/durability/src/compaction/wal_only.rs`
- `crates/durability/src/compaction/tombstone.rs`

## Executive view

The compaction and maintenance layer is split across three different systems:

- storage-level compaction primitives in `crates/storage`
- engine-level background orchestration and write backpressure in `crates/engine`
- durability-level WAL truncation in `crates/durability`

The leveled compaction core is real and fairly mature:

- dynamic level targets
- per-level scoring
- L0 full-range compaction into L1
- round-robin file picking for L1+
- trivial moves
- grandparent-aware output splitting
- version-pruning logic that understands tombstones, active snapshots, TTL, and event logs

The weak point is not the mechanics of merging files.
It is the maintenance control plane.

Several advertised features exist only as utilities or isolated modules:

- size-tiered scheduling is not part of the live scheduler
- GC and TTL maintenance are mostly not wired into automatic runtime execution
- tombstone auditing is not connected to the actual storage path
- `MemoryPressure` exists, but the production open path does not enable it

The result is an engine that can compact levels correctly, but does not yet have
a fully coherent retention and maintenance story.

## Inventory validation

| # | Feature | Verdict | Code-derived note |
|:-:|---------|---------|-------------------|
| 16 | Leveled Compaction | True | `compute_compaction_scores()` scores L0 by file count and L1+ by `actual_bytes / target_bytes`, and `pick_and_compact()` runs the highest overfull level (`crates/storage/src/segmented/compaction.rs`). |
| 17 | Size-Tiered Scheduling | Partially true | `CompactionScheduler` and `TierMergeCandidate` exist in `crates/storage/src/compaction.rs`, but the runtime background compactor does not call them. They are utility/test-path machinery, not the live scheduler. |
| 18 | Version Pruning | True, with runtime caveat | `CompactionIterator` implements prune-floor semantics and one-below-floor retention per key, plus tombstone/snapshot/TTL/event exceptions (`crates/storage/src/compaction.rs`). The engine's automatic compaction currently passes `prune_floor = 0`, so this logic is not used for normal background GC. |
| 19 | Max Versions Per Key | True, with nuance | `max_versions_per_key` is a real config knob and is applied in compaction via `with_max_versions()`, but pruning happens only at compaction time, not at write time (`crates/engine/src/database/config.rs`, `crates/storage/src/segmented/mod.rs`, `crates/storage/src/compaction.rs`). |
| 20 | Tombstone Tracking | Exists but not integrated | `Tombstone`, `TombstoneReason`, and `TombstoneIndex` are implemented in `crates/durability/src/compaction/tombstone.rs`, but there are no runtime callers wiring them into deletes, reads, or compaction. |
| 21 | TTL Index | Exists but not integrated | `TTLIndex` is fully implemented in `crates/storage/src/ttl.rs`, but there are no runtime call sites. The live store handles TTL at read time and in compaction logic, while `expire_ttl_keys()` is a stub. |
| 22 | Compaction Rate Limiting | True | `RateLimiter` is a token bucket, and the segmented store threads it through both `ThrottledSegmentIter` input reads and `SegmentBuilder` output writes (`crates/storage/src/rate_limiter.rs`, `crates/storage/src/segmented/compaction.rs`). |
| 23 | Memory Pressure Detection | Partially true | `MemoryPressure` and tracked-byte accounting exist, but the production open path creates `SegmentedStore::with_dir()` rather than `with_dir_and_pressure()`, so the pressure tracker is disabled in normal runtime (`crates/storage/src/pressure.rs`, `crates/storage/src/segmented/mod.rs`, `crates/concurrency/src/recovery.rs`). |
| 24 | Write Stall / Backpressure | Partially true | The engine has L0 stop/slowdown logic and memory-based sleeps in `maybe_apply_write_backpressure()`, but the default L0 thresholds are disabled in config due to a deadlock concern (`crates/engine/src/database/transaction.rs`, `crates/engine/src/database/config.rs`). |
| 25 | WAL Compaction | True, with nuance | `WalOnlyCompactor` deletes covered WAL segments using the MANIFEST flush watermark and `.meta` sidecars when available. It is used both manually and as best-effort post-flush cleanup (`crates/durability/src/compaction/wal_only.rs`, `crates/engine/src/database/transaction.rs`). |

## Real execution model

### 1. Runtime topology

There are really two separate compaction worlds in the code:

- LSM segment compaction in `strata-storage`
- WAL segment truncation in `strata-durability`

The engine glues them together like this:

1. a write commits
2. `schedule_flush_if_needed()` updates `snapshot_floor`
3. frozen memtables are flushed in the background
4. flush updates the MANIFEST flush watermark
5. best-effort WAL truncation runs
6. a background compaction chain picks one highest-priority LSM compaction

The compaction chain is global, not per branch.
`schedule_background_compaction()` guards the entire database with one
`compaction_in_flight` flag, and each round only runs one compaction.

Code evidence:

- `crates/engine/src/database/transaction.rs`

### 2. Leveled compaction scoring

The leveled scheduler is branch-local in the storage layer, but the engine
chooses the single best branch across the whole database.

Per branch:

- L0 score = `file_count / L0_COMPACTION_TRIGGER`
- L1..L5 score = `actual_bytes / target_bytes`
- L6 is excluded because it has no lower level

Targets are dynamic, not fixed constants.
`recalculate_level_targets()` computes level byte goals from the current shape
of the tree, similar to RocksDB's dynamic base-level logic.

Code evidence:

- `crates/storage/src/segmented/compaction.rs`

### 3. L0 -> L1 compaction

`compact_l0_to_l1()` is a real leveled compaction pass:

- take all L0 files
- compute their total key range
- find overlapping L1 files
- stream-merge L0 plus overlapping L1
- split output by `target_file_size`
- apply L1 bloom/compression policy
- atomically replace L0 and L1

That is the important structural property:
L1 is restored to sorted, non-overlapping shape after the compaction.

Code evidence:

- `crates/storage/src/segmented/compaction.rs`

### 4. L1+ compaction

For levels above L0, the store uses a more classical leveled policy:

- pick one input file using a compact pointer
- find overlaps in the next level
- track grandparent files
- either do a trivial move or stream a merge into new output files

Two details matter here:

- trivial moves are allowed when the file has no overlap in the next level and
  acceptable grandparent overlap
- split boundaries are grandparent-aware, limiting future overlap explosion

Code evidence:

- `crates/storage/src/segmented/compaction.rs`

### 5. Compaction-time pruning semantics

`CompactionIterator` is the real retention policy implementation.
Its behavior is more nuanced than the inventory summary.

Rules:

- versions at or above `prune_floor` are emitted
- below the floor, keep at most one surviving version per logical key
- bottommost compactions can elide dead tombstones
- non-bottommost compactions must preserve tombstones that still shadow lower levels
- `max_versions` applies only during compaction
- `snapshot_floor` protects versions needed by active snapshots
- event entries are exempt from all pruning
- TTL dropping only happens when `drop_expired == true` and the expired entry is below `prune_floor`

One critical runtime detail:

- the engine's automatic compaction path currently calls `pick_and_compact(&branch_id, 0)`

That means the prune-floor part of the policy is disabled during normal
background compaction.

Code evidence:

- `crates/storage/src/compaction.rs`
- `crates/engine/src/database/transaction.rs`

### 6. Rate limiting

Compaction rate limiting is not just a config field.
It is actually threaded through the merge path.

The same limiter is used for:

- streaming segment reads via `ThrottledSegmentIter`
- output block writes via `SegmentBuilder` / `SplittingSegmentBuilder`

The limiter is simple and global for the store:

- one token bucket
- one-second burst cap
- callers sleep outside the lock

Code evidence:

- `crates/storage/src/rate_limiter.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/compaction.rs`

### 7. Backpressure and maintenance

The engine's actual backpressure logic lives in `maybe_apply_write_backpressure()`.
It is not driven by `MemoryPressure`.

The real runtime checks are:

1. if L0 thresholds are active, flush frozen memtables synchronously first
2. if `l0_stop_writes_trigger` is exceeded, wait on a condvar for compaction
3. if `l0_slowdown_writes_trigger` is exceeded, sleep 1 ms
4. every 64 writes, sample memtable bytes and segment metadata
5. if memtable bytes exceed a threshold, sleep 1 ms
6. if segment metadata exceeds block-cache capacity, schedule maintenance and sleep 1 ms

This is operationally important:

- L0 protection is the only true blocking backpressure path
- everything else is a short yield, not a firm maintenance policy

Code evidence:

- `crates/engine/src/database/transaction.rs`

### 8. WAL cleanup

WAL cleanup is tied to flush progress, not to LSM compaction scheduling.

After a frozen memtable flush succeeds, the engine computes a global flush
watermark from `max_flushed_commit()` across branches that actually have SSTs.
It then persists that watermark in the MANIFEST and invokes `WalOnlyCompactor`.

`WalOnlyCompactor`:

- never deletes the active segment
- prefers `.meta` sidecars for O(1) coverage checks
- falls back to a full WAL scan if metadata is missing or corrupt
- uses `flushed_through_commit_id` as the effective watermark

One nuance matters:

- it explicitly ignores snapshot watermark and uses flush watermark only

Code evidence:

- `crates/engine/src/database/transaction.rs`
- `crates/durability/src/compaction/wal_only.rs`

## Important unlisted strengths

### Manifest-before-delete compaction safety

LSM compaction writes the branch manifest before deleting old segment files.
That keeps crash recovery from referencing a segment layout whose files were
already removed.

Code evidence:

- `crates/storage/src/segmented/compaction.rs`

### Corruption-aware compaction abort

Streaming compaction propagates corruption flags from source segment iterators.
If a source data block is corrupt, the compaction aborts instead of quietly
rewriting a partially damaged view of the keyspace.

Code evidence:

- `crates/storage/src/segmented/compaction.rs`

### Grandparent-aware split control

The output splitter for leveled compaction explicitly limits grandparent
overlap. That reduces the chance that one large output file explodes the cost
of the next compaction step.

Code evidence:

- `crates/storage/src/segmented/compaction.rs`

## Compaction-and-maintenance-specific constraints and gaps

### 1. The live scheduler is leveled-only

The codebase contains a size-tiered scheduler, but the engine's background
compaction path never uses it.

The runtime path is:

- scan branches
- take highest leveled compaction score
- run `pick_and_compact()`

`CompactionScheduler::pick_candidates()` appears to be a utility and test path,
not a production scheduling policy.

Impact:

- the advertised hybrid story is stronger than the live runtime behavior
- size-tiered behavior is not part of actual steady-state maintenance

Code evidence:

- `crates/storage/src/compaction.rs`
- `crates/engine/src/database/transaction.rs`

### 2. Safe-point GC is not wired into automatic compaction

The storage layer has `prune_floor` support.
The engine computes `gc_safe_point()`.
But the automatic compaction path does not connect the two.

The engine currently does this:

- `set_snapshot_floor(gc_safe_point())`
- `pick_and_compact(..., 0)`

That protects active snapshots from `max_versions` pruning, but it does not
use the GC safe point to prune historical versions below a floor.

At the same time:

- `run_gc()` is manual
- `gc_branch()` in `SegmentedStore` is a stub returning `0`

So there is no fully wired automatic historical-version GC path today.

Impact:

- old versions remain until a compaction happens to remove them via `max_versions`
- safe-point-based history trimming is mostly conceptual, not operational

Code evidence:

- `crates/engine/src/database/lifecycle.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/engine/src/database/transaction.rs`

### 3. TTL reclamation is architecturally incomplete

TTL has three separate forms in the code:

- per-entry TTL fields in storage
- a standalone `TTLIndex`
- a manual maintenance API

What is actually wired:

- reads filter expired entries
- compaction can drop expired entries only when below `prune_floor`

What is not wired:

- `TTLIndex` is never updated or queried by runtime code
- `expire_ttl_keys()` is a stub
- `run_maintenance()` is manual
- background compaction uses `prune_floor = 0`, which means expired entries are not dropped there

That means TTL is mostly a visibility rule today, not an eager reclamation system.

Code evidence:

- `crates/storage/src/ttl.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/compaction.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/transaction.rs`

### 4. Tombstone auditing is not part of the real delete path

The tombstone-tracking module is substantial, but it is isolated.
Search results show no runtime wiring from:

- user deletes
- retention pruning
- compaction pruning

into `TombstoneIndex`.

So today there are two different ideas called "tombstone":

- actual LSM deletion markers in storage
- audit tombstones in durability code

Only the first is live.

Impact:

- no persistent audit trail for why data was removed
- the feature exists as a library, not as part of the engine's maintenance semantics

Code evidence:

- `crates/durability/src/compaction/tombstone.rs`
- repository-wide caller search

### 5. `MemoryPressure` exists, but production runtime does not use it

`MemoryPressure` is a clean abstraction.
`SegmentedStore` also exposes tracked-byte accounting and a `pressure_level()`.

But the production open path does not instantiate the store with pressure tracking.
Recovery builds the store with `SegmentedStore::with_dir()` or `SegmentedStore::new()`,
both of which use `MemoryPressure::disabled()`.

So the advertised pressure levels are not the active policy engine in normal runtime.
The real runtime policy is the custom backpressure code in `transaction.rs`.

Impact:

- pressure thresholds do not directly trigger flush/compaction as the inventory implies
- there is no unified maintenance policy keyed off `PressureLevel`

Code evidence:

- `crates/storage/src/pressure.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/concurrency/src/recovery.rs`

### 6. Write-stall defaults are effectively disabled

The write-stall machinery exists, but the defaults are:

- `l0_slowdown_writes_trigger = 0`
- `l0_stop_writes_trigger = 0`

The config comments explain why:

- enabling them together with rate-limited compaction can deadlock writes behind slow compaction

This is an explicit architectural compromise.
The code has the stall mechanism, but the shipped defaults avoid it because the
broader control loop is not robust enough yet.

Impact:

- out-of-the-box L0 protection is weaker than the feature list suggests
- the engine relies more on compaction keeping up than on hard throttling

Code evidence:

- `crates/engine/src/database/config.rs`
- `crates/engine/src/database/transaction.rs`

### 7. Compaction parallelism is globally serialized

The config exposes `background_threads`, but LSM compaction itself is still
single-filed at the database level.

There is exactly one `compaction_in_flight` flag and one global chain that:

- picks one best branch
- runs one compaction
- re-submits itself

That means multiple branches cannot compact in parallel even if:

- they do not overlap
- the scheduler has spare worker threads

Impact:

- branch-heavy workloads can see maintenance lag even on larger machines
- `background_threads` helps with flush and other tasks, but not with parallel LSM compaction

Code evidence:

- `crates/engine/src/database/transaction.rs`

### 8. `max_versions_per_key` is compaction-time retention, not immediate retention

The config text says versions are auto-pruned.
That is technically true only when compaction runs.

The write path does not enforce the cap.
The cap is applied only inside `CompactionIterator`, and even there:

- active-snapshot protection can keep extra versions
- event entries are exempt
- no compaction means no pruning

So this is not a strict bound on live history.
It is a compaction-time cleanup policy.

Code evidence:

- `crates/engine/src/database/config.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/compaction.rs`

## Bottom line

Compaction itself is not the weak part of this subsystem.
The leveled merge machinery is credible and fairly well thought through.

The main gaps are in orchestration and maintenance policy:

- retention floors are not actually driving automatic pruning
- TTL cleanup is mostly visibility, not reclamation
- pressure tracking is present but not active
- write-stall defaults are off because the control loop is not yet safe
- compaction remains globally serialized

So the right summary is:

- strong compaction mechanics
- partial maintenance integration
- incomplete retention automation
