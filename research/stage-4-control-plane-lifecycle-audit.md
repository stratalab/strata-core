# Stage 4: Control Plane and Lifecycle Audit

This pass asks a narrow question:

When Strata exposes an operational control, does the live runtime actually
honor it, and does it behave the way the surrounding code suggests?

The answer is mixed. Some controls are real and authoritative. Others are
manual-only, disabled by default, stubbed, or split across multiple layers.
The biggest problem in this stage is not a single bad knob. It is that Strata
does not have one unified operational control plane. Runtime behavior is split
across:

- persisted config in `strata.toml`
- engine-side hot config via `Database::update_config()`
- separate runtime-only setters like `Database::set_durability_mode()`
- `BackgroundScheduler`
- a separate WAL flush thread
- manual maintenance APIs
- `Drop` fallback behavior

## Scope

Primary code paths inspected in this pass:

- `crates/engine/src/database/config.rs`
- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/mod.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/background.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/pressure.rs`
- `crates/storage/src/rate_limiter.rs`
- `crates/storage/src/ttl.rs`
- `crates/durability/src/wal/writer.rs`
- `crates/durability/src/wal/config.rs`
- `crates/durability/src/wal/mode.rs`
- `crates/executor/src/handlers/config.rs`

## Executive summary

The strongest control surfaces are:

- memory-budget-derived storage sizing
- live storage/coordinator limits pushed by `update_config()`
- compaction I/O rate limiting

The weakest control surfaces are:

- maintenance and TTL cleanup
- write-stall defaults
- Standard WAL sync knobs
- runtime durability-mode switching
- any operational meaning attached to `background_threads`

The most important confirmed architectural findings are:

1. maintenance is mostly manual, and the storage entrypoints it calls are
   mostly stubs
2. write-stall defaults are intentionally disabled because the current control
   loop can deadlock under rate-limited compaction
3. Standard WAL policy has drifted from its public surface; time-based syncing
   is real, count/byte thresholds are not
4. background work is fragmented across separate loops and not governed by one
   scheduler or one lifecycle contract
5. runtime durability switching is only partially wired and leaves the
   `Database`'s own mode state stale
6. flush scheduling has a concrete failure-handling bug: if scheduler submit
   fails, `flush_in_flight` can remain stuck `true`

## Control-surface truth table

### Authoritative and live

These knobs materially affect the runtime on the active path.

#### Unified memory budget

`StorageConfig::memory_budget` is real. When non-zero, it overrides individual
 sizing knobs through:

- `effective_block_cache_size()`
- `effective_write_buffer_size()`
- `effective_max_immutable_memtables()`

This derived sizing is used during open and hot config application:

- `open.rs` applies the effective values at construction time
- `Database::apply_storage_config_inner()` re-applies them on `update_config()`

This is one of the cleaner control surfaces in the engine.

#### Live storage/coordinator config application

`Database::update_config()` persists `strata.toml` and immediately pushes a
subset of storage/coordinator/cache parameters into the live runtime.

Confirmed live-applied settings:

- `max_branches`
- `max_versions_per_key`
- `max_immutable_memtables` via effective value
- `write_buffer_size` via effective value
- `target_file_size`
- `level_base_bytes`
- `data_block_size`
- `bloom_bits_per_key`
- `compaction_rate_limit`
- `max_write_buffer_entries`
- global block-cache capacity

This is the strongest hot-config path currently in the engine.

#### Compaction rate limiting

`compaction_rate_limit` is a real knob.

- `open.rs` installs it when opening disk-backed stores
- `apply_storage_config_inner()` updates it live
- `storage::rate_limiter` is a real token-bucket implementation

This control is operationally real, even if it interacts badly with other
backpressure assumptions.

### Partially wired or split-authority

These controls exist, but their semantics are split across constructors,
runtime methods, or separate layers.

#### Durability mode

Durability mode is not one control surface. It is three:

1. persisted config string in `StrataConfig`
2. open-time mode selection through `cfg.durability_mode()`
3. runtime WAL policy mutation through `Database::set_durability_mode()`

The product `CONFIG SET durability` path in `executor` coordinates both
`update_config()` and `set_durability_mode()`, which papers over the split for
that one entrypoint. But the engine itself does not provide one authoritative
API that updates persisted config, runtime writer behavior, thread lifecycle,
and engine state together.

Two concrete seams are confirmed:

- `Database::set_durability_mode()` updates the `WalWriter`, but
  `Database::durability_mode` is an immutable field and never changes after
  construction
- the runtime setter spawns a flush thread when switching into `Standard`, but
  does not symmetrically tear one down when switching to `Always`

The stale `Database::durability_mode` field does not break commit durability
between `Standard` and `Always`, because both still require WAL and
`commit_internal()` only uses the field to decide whether to pass a WAL at all.
But it does mean the database object's own state, health reporting, and flush
thread reasoning can drift from the actual writer policy.

#### Background thread count

`background_threads` is real only in a narrow sense:

- it sizes `BackgroundScheduler` at database construction time

It is not a general background concurrency knob because:

- it is not hot-applied by `update_config()`
- it does not govern the separate WAL flush thread
- there is no automatic maintenance worker for it to govern
- LSM compactions are still globally serialized through
  `compaction_in_flight`

So `background_threads` currently means:

"how many workers exist in the generic scheduler"

It does not mean:

"how many maintenance, flush, and compaction tasks can run independently"

#### Write-buffer sizing

Write-buffer sizing is partly constructor-owned and partly hot-config-owned.

- disk-backed open paths derive the write-buffer size during store
  construction/recovery setup
- later hot updates push `set_write_buffer_size(...)`

That is workable, but it means the storage-config application is not actually
fully centralized in `apply_storage_config(...)`.

### Disabled, manual-only, or stubbed

These surfaces exist in the API, but are not active as an automatic control
plane.

#### L0 write-stall defaults

The L0 slowdown/stop knobs exist, but the shipped defaults are explicitly
disabled:

- `default_l0_slowdown_writes_trigger() -> 0`
- `default_l0_stop_writes_trigger() -> 0`

The code comment is explicit about why: enabling these thresholds together
with the current rate-limited compaction behavior can deadlock writes behind a
throttled L0 drain.

That means the advertised RocksDB-like stall policy exists in code, but the
default product configuration intentionally does not use it.

#### Maintenance, GC, and TTL

`Database::run_gc()` and `Database::run_maintenance()` are explicit manual
APIs. Their own comments say they are not called automatically.

The storage entrypoints they call are mostly non-operative:

- `SegmentedStore::gc_branch()` returns `0` and is documented as a no-op stub
- `SegmentedStore::expire_ttl_keys()` returns `0` and is documented as a no-op
  stub

So the maintenance plane currently has this shape:

- safe-point computation exists
- manual engine APIs exist
- storage-side mutation entrypoints exist
- the live implementation mostly does nothing and relies on compaction-time
  pruning instead

This is not a real autonomous maintenance subsystem.

#### `MemoryPressure`

`storage::pressure::MemoryPressure` is a clean abstraction, but it is not the
live engine policy.

Confirmed facts:

- `SegmentedStore::new()` uses `MemoryPressure::disabled()`
- `SegmentedStore::with_dir()` uses `MemoryPressure::disabled()`
- `SegmentedStore::with_dir_and_pressure()` exists, but I found only test
  usage in the storage crate

The live engine instead uses custom logic in
`Database::maybe_apply_write_backpressure()` based on:

- L0 file count
- total memtable bytes
- total segment metadata bytes versus block-cache capacity

So the engine has a memory-pressure abstraction and a separate real
backpressure policy. The abstraction is not the authority.

## Misleading or drifted controls

### Standard WAL count/byte thresholds

This is the clearest config drift in the control plane.

`WalConfig` still exposes:

- `segment_size`
- `buffered_sync_bytes`

`DurabilityMode::Standard` still exposes:

- `interval_ms`
- `batch_size`

But the live `WalWriter` Standard-mode logic no longer uses count/byte
thresholds the way the surrounding surface suggests:

- `maybe_sync()` is now effectively interval-driven
- the inline comment explicitly says Standard mode no longer syncs on
  `batch_size`
- `prepare_background_sync()` also checks elapsed time, not byte thresholds
- `bytes_since_sync` and `writes_since_sync` are still tracked, but no longer
  drive the real decision

So the public-facing control surface still implies:

"sync when enough time or enough writes/bytes have elapsed"

The live implementation is closer to:

"sync when enough time has elapsed"

This is architectural drift, not just stale comments.

### Background worker description

`StorageConfig.background_threads` is described as:

"Number of background worker threads for compaction, flush, and maintenance."

That is not currently true as an operational statement:

- periodic WAL flushing uses a separate dedicated thread
- maintenance is not automatic
- compaction is single-chain serialized at the database level

The knob exists, but its description overclaims its reach.

### Config model drift

The header comment in `config.rs` still says:

"edit the file and restart"

But the real system now also has:

- hot config application through `Database::update_config()`
- runtime durability switching
- executor-level `CONFIG SET`

This is smaller than the runtime bugs above, but it reflects the broader
problem: the control plane evolved without one authoritative operational model.

## Background execution model

The runtime currently has several distinct operational loops:

### 1. Generic scheduler

`BackgroundScheduler` is used for:

- async frozen-memtable flush
- background compaction
- embedding and other deferred tasks

It is a real worker pool with:

- queue backpressure
- priorities
- drain
- shutdown

### 2. Separate WAL flush thread

Standard durability also creates a separate dedicated thread that periodically:

- calls `prepare_background_sync()`
- runs `sync_all()` outside the WAL lock
- best-effort writes `.meta` sidecar state in the open-path implementation

This loop is not under `BackgroundScheduler`.

### 3. Manual maintenance plane

GC and TTL maintenance do not have a background loop.
They remain manual API calls.

### 4. Follower refresh loop

Follower refresh is neither scheduled by the generic scheduler nor governed by
the same lifecycle rules as primary background work. It is an explicit replay
operation with its own error-handling model.

The result is a fragmented control plane rather than one coherent background
runtime.

## Failure-handling matrix

### Flush-task submission failure is mishandled

This is the sharpest new bug in Stage 4.

`schedule_flush_if_needed()` uses `flush_in_flight` as a coalescing flag.
If the compare-exchange succeeds, it calls `scheduler.submit(...)` and ignores
the result.

That means if scheduler submission fails, for example because:

- the queue is full
- the scheduler is shutting down

then:

- no flush task runs
- `flush_in_flight` remains `true`
- future flush scheduling attempts can be permanently suppressed

Compaction submission handles this correctly by clearing
`compaction_in_flight` when submit fails.
Flush submission does not.

This is a real lifecycle/control-plane bug, not just a missing feature.

### WAL background sync failure is best-effort after state reset

`WalWriter::prepare_background_sync()` resets sync counters before the
out-of-lock `sync_all()` actually succeeds.

That creates a temporary state where:

- the writer has cleared its unsynced bookkeeping
- the actual durability barrier has not yet completed

If `sync_all()` then fails, the runtime has already stepped over its own
bookkeeping boundary.

This was already identified in the WAL-focused audit, but it belongs in the
control-plane story too because it is the main durability failure-handling seam
in background operation.

### Shutdown tolerates timeout and continues

`Database::shutdown()`:

1. stops accepting new transactions
2. drains the scheduler
3. waits for idle transactions with a timeout
4. stops and joins the flush thread
5. flushes
6. runs freeze hooks

If `wait_for_idle()` times out, shutdown logs a warning and continues.
That is a reasonable last-resort posture, but it means shutdown is not a hard
synchronization barrier.

This matters even more because the scheduler is drained before the transaction
idle wait, so in-flight commits can still finish after the drain point and
enqueue new flush/compaction work.

### `Drop` remains part of the lifecycle contract

`shutdown()` is not the only close path.
`Drop` still:

- shuts down the generic scheduler
- joins the WAL flush thread
- performs final flush/freeze if `shutdown_complete` was not set
- removes the database from `OPEN_DATABASES`

So the runtime lifecycle is not:

"open -> run -> shutdown"

It is:

"open -> run -> maybe shutdown -> final cleanup in Drop"

That is recoverable, but it is not yet a clean single-phase lifecycle.

## Lifecycle sequences

### Normal write path

The active write control loop is:

1. `transaction()` / `commit_transaction()`
2. `maybe_apply_write_backpressure()`
3. `commit_internal()`
4. `TransactionCoordinator::commit_with_wal_arc(...)`
5. if the transaction wrote data: `schedule_flush_if_needed()`
6. `schedule_background_compaction()`

This is a functional control loop, but it mixes:

- synchronous admission control
- background flush scheduling
- background compaction scheduling
- a separate WAL durability loop

### Shutdown path

The active shutdown path is:

1. set `accepting_transactions = false`
2. early-return for followers
3. `scheduler.drain()`
4. `wait_for_idle(timeout)`
5. stop and join WAL flush thread
6. `flush()`
7. run freeze hooks
8. set `shutdown_complete`
9. later, `Drop` still performs final cleanup duties if needed

This sequence is workable, but it is not yet a cleanly sealed runtime barrier.

## Conclusions

Stage 4 does not show that the control plane is absent.
It shows that the control plane is fragmented and unevenly trustworthy.

The shortest accurate summary is:

- storage sizing is mostly real
- compaction throttling is real
- runtime config hot-apply exists
- maintenance is mostly manual/stubbed
- some advertised knobs are disabled by default
- some durability knobs are stale or misleading
- background work does not live under one authority
- shutdown and `Drop` still jointly define lifecycle truth

## Priority fixes from this stage

1. Fix `schedule_flush_if_needed()` so failed scheduler submission clears
   `flush_in_flight` and surfaces the failure path cleanly.
2. Unify durability-mode control so one API updates persisted config,
   `WalWriter` policy, thread lifecycle, and the database's own reported mode.
3. Either ship a real background maintenance subsystem or remove the illusion
   that `run_gc()` / `run_maintenance()` are part of normal runtime behavior.
4. Replace misleading Standard WAL byte/count knobs or restore behavior that
   matches them.
5. Rework the write-stall/compaction interaction so non-zero L0 stall defaults
   can be safely enabled.
6. Consolidate background execution under a clearer ownership model, or at
   least document the separate scheduler, WAL thread, follower refresh, and
   manual maintenance planes as distinct runtime mechanisms.
