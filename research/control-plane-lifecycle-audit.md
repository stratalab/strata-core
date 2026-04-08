# Control Plane and Lifecycle Audit

Date: 2026-04-08

Scope:

- config knobs
- maintenance and GC
- follower refresh
- shutdown and drop
- background work ownership

Method: code inspection only.

## Verdict

Strata has a real control plane, but it is narrower and more fragmented than the public surface suggests.

The active runtime is mostly driven by:

- `Database::open*` open-time configuration application
- write-path backpressure in `engine/src/database/transaction.rs`
- `BackgroundScheduler` for flush and compaction tasks
- a separate Standard-mode WAL flush thread
- caller-driven `refresh()` and `run_maintenance()`
- shutdown logic split between `shutdown()` and `Drop`
- executor-owned embed refresh timing above the engine

The main architectural problem is not the absence of lifecycle machinery. It is split authority:

- some knobs are live
- some are open-time only
- some are drifted from implementation
- some point to future maintenance behavior that does not actually run

## What Is Actually Live

### Open-time config application

At open, the engine does apply a real subset of storage controls:

- branch limits
- max versions per key
- immutable memtable limit
- target file size
- level base size
- data block size
- bloom bits per key
- compaction rate limit
- block cache capacity
- write buffer entry limit in the coordinator

That happens in `apply_storage_config()` in [open.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/open.rs) and `apply_storage_config_inner()` in [mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/mod.rs).

### Background scheduler

The engine has one real general-purpose worker pool:

- `BackgroundScheduler`
- fixed worker count from `storage.background_threads`
- fixed queue depth of `4096`

It is used for:

- deferred frozen-memtable flush
- background compaction
- executor-submitted embed buffer flushes

The scheduler itself is solid. The problem is that it is only one of several control surfaces.

### WAL flush thread

Standard durability mode uses a dedicated WAL flush thread, separate from the scheduler. That thread is spawned in [open.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/open.rs), not through the scheduler.

### Write-path backpressure

The real write-pressure policy is in `maybe_apply_write_backpressure()` in [transaction.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/transaction.rs):

- L0 stall / slowdown thresholds
- synchronous memtable drain before L0-based decisions
- memtable-byte sampling
- segment-metadata-byte sampling

This is the active control loop. It matters more than the dormant `MemoryPressure` abstraction.

## Config Truth Table

### Knobs that are materially live

- `max_branches`
- `max_write_buffer_entries`
- `max_versions_per_key`
- `block_cache_size` / `memory_budget`
- `write_buffer_size`
- `max_immutable_memtables`
- `target_file_size`
- `level_base_bytes`
- `data_block_size`
- `bloom_bits_per_key`
- `compaction_rate_limit`
- `write_stall_timeout_ms`
- `background_threads` at open time

### Knobs that are drifted or misleading

#### `DurabilityMode::Standard.batch_size`

The public type and docs still say Standard mode syncs on time or batch size. The implementation in [writer.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/wal/writer.rs) is now interval-driven only. `batch_size` is still present in the type and docs in [mode.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/wal/mode.rs), but the write path explicitly says there is no batch-size trigger.

This is control-plane drift, not just a comment bug.

#### `WalConfig.buffered_sync_bytes`

`WalConfig` still exposes `buffered_sync_bytes` in [config.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/durability/src/wal/config.rs), but the active Standard-mode sync path no longer uses it.

That means part of the WAL tuning surface is effectively dead.

#### L0 stall defaults

The config surface advertises RocksDB-style slowdown and stop thresholds, but the defaults in [config.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/config.rs) are both `0`, explicitly disabled due to a deadlock concern with rate-limited compaction.

So the mechanism exists, but the shipped default policy is "off."

### Knobs that are open-time only in practice

#### `background_threads`

The scheduler thread count is fixed when the database opens. `update_config()` does not rebuild or resize the scheduler, so changing `background_threads` after open does not reconfigure the live worker pool.

#### durability in config vs runtime durability

Durability from `strata.toml` is parsed on open. Runtime switching uses `Database::set_durability_mode()` in [mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/mod.rs), which is a separate path and is not persisted back to `strata.toml`.

That means:

- the file config is one authority
- the live WAL writer is another authority

## Maintenance and GC

The maintenance surface is much weaker than it looks.

### `run_gc()` is manual and mostly a shell

`run_gc()` in [lifecycle.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/lifecycle.rs) computes a safe point and calls `storage.gc_branch()` across branches.

But `SegmentedStore::gc_branch()` in [segmented/mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/segmented/mod.rs) is a stub that returns `0`.

So explicit GC currently computes a boundary but does not directly reclaim storage versions through this path.

### `run_maintenance()` is manual and partially stubbed

`run_maintenance()` just calls:

- `run_gc()`
- `storage.expire_ttl_keys(now)`

But `expire_ttl_keys()` in [segmented/mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/segmented/mod.rs) is also a stub that returns `0`.

So the explicit maintenance API is mostly a placeholder wrapper around unimplemented storage entrypoints.

### `TTLIndex` exists, but the live maintenance path does not use it

[ttl.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/ttl.rs) contains a real `TTLIndex`, but the active storage maintenance entrypoint is still a no-op stub. TTL currently matters at read time and during compaction, not via a real explicit cleanup cycle.

### `MemoryPressure` exists, but the active engine loop does not use it

[pressure.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/pressure.rs) is explicitly framed for a future caller. The store constructors in [segmented/mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/storage/src/segmented/mod.rs) use `MemoryPressure::disabled()`, and the engine instead runs its own backpressure checks in the write path.

This is another example of a good abstraction that has not yet become the live control plane.

## Refresh

`refresh()` in [lifecycle.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/lifecycle.rs) is a real follower lifecycle path, but it is still caller-driven and composition-sensitive.

Important properties:

- refresh is follower-only
- it is not scheduled automatically by the engine
- it reads WAL records after the follower watermark
- it applies storage mutations, then updates secondary structures

The control-plane significance is:

- follower convergence is not background-managed by the engine
- refresh policy belongs to the embedding layer or caller
- search refresh is hardcoded inside `Database::refresh()`
- vector refresh is delegated through `RefreshHooks`

So even within refresh, there is no single extension contract.

## Shutdown and Drop

Shutdown is not the sole authoritative close path.

### What `shutdown()` does

`shutdown()` in [lifecycle.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/lifecycle.rs) does:

- set `accepting_transactions = false`
- `scheduler.drain()`
- wait for active transactions to complete
- stop and join the WAL flush thread
- final `flush()`
- run subsystem freeze hooks
- set `shutdown_complete = true`

### Main lifecycle hazards

#### Scheduler drain happens before transaction quiescence

`shutdown()` drains the scheduler before waiting for in-flight transactions to finish. But transactions that finish after the drain can still call `schedule_flush_if_needed()` and `schedule_background_compaction()`.

That means background work can be enqueued after the scheduler drain phase. Since the scheduler is not shut down until `Drop`, `shutdown()` is not a full lifecycle barrier.

#### Freeze failure is sticky

`shutdown_complete` is set before freeze errors are returned to the caller. If freeze fails, later `Drop` will skip retrying the freeze path because it sees shutdown as complete.

#### `Drop` still performs essential work

`Drop` in [mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/mod.rs) still:

- shuts down the scheduler
- stops the WAL flush thread
- does final flush if `shutdown()` did not complete
- runs freeze hooks if `shutdown()` did not complete
- removes the DB from `OPEN_DATABASES`

So explicit `shutdown()` is only a partial close. Final authority still lives in `Drop`.

#### Registry and lock lifecycle are not ended by `shutdown()`

Because removal from `OPEN_DATABASES` happens in `Drop`, a shut-down instance can still exist as the registered live instance for that path until the final `Arc` is dropped.

## Background Work Ownership

There is no single background-work owner.

### Engine-owned

- `BackgroundScheduler`
- background flush tasks
- background compaction
- Standard-mode WAL flush thread

### Engine-owned but manual

- `run_gc()`
- `run_maintenance()`
- follower `refresh()`

### Executor-owned

`Executor` in [executor.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/executor/src/executor.rs) spawns its own `strata-embed-refresh` timer thread. That thread wakes every second and submits embedding flush work into the engine scheduler.

So the embed-refresh lifecycle is not engine-owned end to end. The timer policy lives above the engine; the actual work executes inside the engine scheduler.

This is another split-authority seam.

## Concrete Bugs and Gaps

### 1. `schedule_flush_if_needed()` can strand `flush_in_flight`

In [transaction.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/transaction.rs), flush scheduling sets `flush_in_flight = true` before submitting to the scheduler, but the `submit()` result is ignored:

- if scheduler submission fails
- the flag is never cleared
- future flush scheduling is suppressed

This is a real control-plane bug, not just architectural drift.

### 2. Runtime durability switching is split-authority and stale

`Database::set_durability_mode()` updates the `WalWriter`, but the `Database` struct keeps its own `durability_mode` field unchanged.

That stale field is still consulted by:

- health reporting in [mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/mod.rs)
- transaction commit entrypoints in [transaction.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/transaction.rs)

Today the direct behavioral impact is limited because runtime switching only supports `Standard <-> Always`, and both still require WAL. So commit behavior does not fully bifurcate here. But the database object is still left with stale control-plane state, and health / lifecycle logic can reason about the wrong mode.

### 3. WAL flush thread logic is duplicated

There are two Standard-mode flush-thread implementations:

- `spawn_wal_flush_thread()` in [open.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/open.rs)
- the inline thread creation inside `set_durability_mode()` in [mod.rs](/home/anibjoshi/Documents/GitHub/strata-core/crates/engine/src/database/mod.rs)

They are not equivalent. The open-time thread snapshots active `.meta` state and writes sidecars outside the lock; the runtime-switch path has different ordering and behavior.

That is lifecycle duplication inside the control plane.

### 4. `background_threads` overstates parallelism

The scheduler worker count is configurable, but compaction itself is still gated by a single `compaction_in_flight` chain. So higher worker counts help some classes of deferred work, but not parallel compaction across many branches.

### 5. Maintenance is exposed as if it exists, but it is not automated

The code comments are honest in places, but the overall surface still looks more complete than it is:

- `run_gc()` and `run_maintenance()` exist
- `MemoryPressure` exists
- `TTLIndex` exists
- compaction modules exist

But the actual maintenance plane is still mostly explicit/manual or stubbed.

## Architectural Interpretation

The control plane is not absent. It is layered in three increasingly problematic tiers:

### Tier 1: solid runtime machinery

- scheduler
- write-path backpressure
- WAL flush thread
- open-time config application

### Tier 2: partially integrated lifecycle APIs

- refresh
- shutdown
- runtime config mutation
- subsystem freeze hooks

### Tier 3: aspirational or drifted surfaces

- manual maintenance APIs with stubbed storage backends
- `MemoryPressure` as future policy
- Standard-mode batch-size and buffered-sync knobs
- duplicated durability-thread lifecycle

The control plane therefore feels like a partially completed consolidation:

- enough exists to run the engine
- not enough is unified to make behavior obvious from the public surface

## Recommended Fix Direction

### Critical

- Fix `flush_in_flight` so scheduler submission failure cannot permanently suppress flush work.
- Make runtime durability switching update one authoritative state source and route thread spawning through one shared implementation.
- Make shutdown a real close barrier: either quiesce transactions before scheduler drain, or keep draining until no new work can appear.

### High

- Decide whether maintenance is manual-only or automatic, then make the code and config surface reflect that choice.
- Remove or clearly mark drifted WAL knobs (`batch_size`, `buffered_sync_bytes`) until they are real again.
- Move executor-owned embed refresh timing behind an engine-owned lifecycle contract if that behavior is considered core.

### Medium

- Either wire `MemoryPressure` into the live engine loop or keep it internal until it has a real caller.
- Clarify which config fields are live-updateable versus open-time-only.
- Make scheduler queue depth configurable if it is meant to be a real backpressure surface.

## Overall Assessment

The control plane is usable, but not yet trustworthy as a clean operational model.

The core issue is not missing code. It is that operational behavior is spread across:

- config file parsing
- live config mutation
- write-path heuristics
- scheduler tasks
- a dedicated WAL thread
- manual maintenance calls
- caller-driven follower refresh
- executor-owned periodic work
- `Drop`

That is why the lifecycle feels harder to reason about than the storage core itself.
