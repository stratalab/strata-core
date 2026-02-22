# Strata Automatic Maintenance

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-02-21

---

## 1. Problem Statement

Strata currently has **zero automatic maintenance**. Garbage collection, WAL compaction, tombstone cleanup, TTL enforcement, and search segment management all require explicit API calls that no caller makes today. In production this leads to:

- **Memory bloat** — MVCC version chains grow without bound. A key updated once per second accumulates 86,400 versions per day; reads degrade linearly because `VersionChain::latest()` must skip stale versions.
- **Disk bloat** — WAL segments accumulate indefinitely. Every committed transaction appends to the WAL but segments are never reclaimed, even when fully superseded by a snapshot.
- **Tombstone accumulation** — Deleted keys leave tombstone entries in the `TombstoneIndex`. Without cleanup these grow proportionally to total deletes ever performed.
- **TTL non-enforcement** — Keys with TTL are only filtered at read time (`is_expired()` checks). Expired keys are never actually removed, so they continue consuming memory and disk.
- **Search index fragmentation** — Sealed search segments accumulate without merging. Query-time merge across N segments degrades as N grows.
- **Orphaned data** — Branches that crash without an `end_branch` marker are detected and marked `Orphaned` during recovery, but their data is never cleaned up.

### Concrete Scenario

```
Key "sensor/temp" updated at 1 Hz:
  Day 1:   86,400 versions → scan overhead: ~0.2ms per read
  Day 7:  604,800 versions → scan overhead: ~1.4ms per read
  Day 30: 2,592,000 versions → scan overhead: ~6ms per read
  + WAL: ~30 segments/day × 30 days = 900 segments never reclaimed
  + Tombstones: any deletes accumulate forever
```

The `BackgroundScheduler` already exists and is general-purpose — auto-embedding proves the pattern works (`embed_hook.rs` submits batches to the scheduler at `TaskPriority::Normal`). We need maintenance tasks that follow the same model.

---

## 2. Current State Audit

| # | Issue | Severity | Manual API Exists | File |
|---|-------|----------|-------------------|------|
| 1 | MVCC version chains never GC'd | CRITICAL | `gc()`, `gc_branch()` | `crates/storage/src/sharded.rs:115` |
| 2 | WAL segments never compacted | CRITICAL | `WalOnlyCompactor::compact()` | `crates/durability/src/compaction/wal_only.rs` |
| 3 | Tombstones accumulate forever | HIGH | `cleanup_before()` | `crates/durability/src/compaction/tombstone.rs:347` |
| 4 | TTL expired keys never removed | HIGH | `find_expired()` | `crates/storage/src/ttl.rs:59` |
| 5 | Search segments never merged | MEDIUM | None | `crates/engine/src/search/index.rs` |
| 6 | Orphaned branches never cleaned up | MEDIUM | `mark_orphaned()` | `crates/engine/src/recovery/replay.rs:175` |
| 7 | Stale snapshots no timeout | MEDIUM | None | `crates/storage/src/sharded.rs` |
| 8 | Clone-based snapshots (M2 design) | SEVERE | N/A (design limitation) | `crates/storage/src/sharded.rs:892` |
| 9 | Vector soft-deletes never purged | MEDIUM | `soft_delete()` | `crates/engine/src/primitives/vector/segmented.rs:124` |
| 10 | No maintenance observability | MEDIUM | Only `SchedulerStats` | `crates/engine/src/background.rs:38` |

### Detailed Findings

**1. MVCC Version Chains (CRITICAL)**

`VersionChain::gc(&mut self, min_version: u64) -> usize` at `sharded.rs:115` prunes versions older than `min_version`, always keeping at least one version. `ShardedStore::gc_branch()` at line 613 iterates all entries in a branch shard and calls `gc()` on each chain. Neither is ever called automatically. There is no safe-point computation — no code determines what `min_version` is safe given active transactions.

**2. WAL Segments (CRITICAL)**

`WalOnlyCompactor::compact()` in `crates/durability/src/compaction/wal_only.rs` removes segments covered by the snapshot watermark. `CompactMode::WALOnly` is the safe mode (preserves all version history); `CompactMode::Full` also applies retention policy. The compaction module header explicitly states: "Compaction is **user-triggered**: No background compaction" (`compaction/mod.rs:16`). Exposed via `Database::compact()` but never auto-called.

**3. Tombstones (HIGH)**

`TombstoneIndex::cleanup_before(&mut self, cutoff: u64) -> usize` at `tombstone.rs:347` removes tombstones with `created_at < cutoff`. Tombstones track deletions with `TombstoneReason` (UserDelete, RetentionPolicy, Compaction). They accumulate proportionally to total deletes. No background cleanup.

**4. TTL Index (HIGH)**

`TTLIndex` in `crates/storage/src/ttl.rs` provides `find_expired(now: Timestamp) -> Vec<Key>` and `remove_expired(now: Timestamp) -> usize`. The index maps `expiry_timestamp → Set<Key>` using a BTreeMap for O(expired count) range queries. However, TTL is only checked passively during reads via `StoredValue::is_expired()` checks scattered throughout `sharded.rs` (lines 539, 565, 936, 965, etc.). Expired keys are never actually deleted from storage or from the TTL index.

**5. Search Segments (MEDIUM)**

The search index (`crates/engine/src/search/index.rs`) uses a Lucene-inspired segmented architecture: an active DashMap-based segment absorbs writes, and when it hits `seal_threshold` documents it seals into an immutable mmap-backed `.sidx` file. `score_top_k()` merges results across all segments at query time (line 30). There is no segment merging or compaction — sealed segments accumulate indefinitely. Query cost scales linearly with segment count.

**6. Orphaned Branches (MEDIUM)**

`BranchStatus::Orphaned` is defined in `crates/core/src/branch_types.rs:39`. During recovery, `BranchIndex::mark_orphaned()` (`recovery/replay.rs:175`) marks branches that were never ended (no `end_branch` marker in WAL). The branch handler at `executor/src/handlers/branch.rs:169` acknowledges: "branch metadata is already deleted and data will be orphaned but harmless." The data is never cleaned up.

**7. Stale Snapshots (MEDIUM)**

`ShardedSnapshot` (`sharded.rs:892`) is lightweight — just `(version: u64, store: Arc<ShardedStore>)`. Snapshots are O(1) to create (Arc clone + atomic version load). They don't accumulate in a traditional sense, but long-lived snapshots pin the GC safe point: if any snapshot holds version V, no versions >= V can be collected. There is no timeout or forced release mechanism.

**8. Clone-Based Snapshots (SEVERE — Design)**

This is a documented M2 limitation. Snapshots use `Arc::clone` (line 818), not deep copies. A snapshot shares the underlying store, meaning GC cannot reclaim versions that any live snapshot might read. This is architecturally correct for MVCC but means long-lived snapshots directly prevent version chain cleanup.

**9. Vector Soft-Deletes (MEDIUM)**

`SegmentedVectorStore` (`crates/engine/src/primitives/vector/segmented.rs`) supports soft-deletion of vectors (line 124: `soft_delete(&mut self, id: VectorId, deleted_at: u64)`). Deleted vectors are marked but never purged from sealed HNSW segments. Over time, sealed segments accumulate dead entries that waste memory and slow search (HNSW traversal visits deleted nodes).

**10. No Maintenance Observability (MEDIUM)**

The only observability is `SchedulerStats` (`background.rs:38`) which reports queue depth, active tasks, completed count, and worker count. There are no metrics for: versions pruned, bytes reclaimed, tombstones cleaned, segments merged, expired keys removed, or time since last maintenance pass.

---

## 3. Industry Comparison

### Version Garbage Collection

| System | Mechanism | Trigger | Default Retention |
|--------|-----------|---------|-------------------|
| PostgreSQL | Autovacuum removes dead tuples | `threshold + scale_factor × reltuples` dead tuples | 50 + 20% of table |
| CockroachDB | MVCC GC queue per node | TTL-based: `gc.ttlseconds` | 14,400s (4 hours) |
| TiKV | Safe-point-driven distributed GC | `tikv_gc_run_interval` (periodic) | `tikv_gc_life_time` = 10 min |

**PostgreSQL autovacuum** uses a proportional trigger: vacuum fires when dead tuples exceed `autovacuum_vacuum_threshold` (50) + `autovacuum_vacuum_scale_factor` (0.2) × table row count. This scales naturally — small tables vacuum aggressively, large tables tolerate proportionally more dead tuples. Key settings: `autovacuum_naptime` = 1 min between checks, `autovacuum_max_workers` = 3 concurrent workers.

**CockroachDB** uses a time-based approach: `gc.ttlseconds` (default 14,400s / 4 hours) determines when MVCC revisions become GC-eligible. A version is expired when its timestamp is older than the TTL *and* a newer version exists. The **protected timestamps** subsystem prevents GC of data needed by backups or changefeeds. This decouples GC from operational concerns.

**TiKV** coordinates through a **safe point** — a timestamp below which all old versions can be deleted. TiDB computes `safe_point = now - gc_life_time`, uploads it to the Placement Driver (PD), and each TiKV node fetches and applies it locally. Since v5.0, GC piggybacks on RocksDB's compaction filter: old versions are dropped during normal compaction I/O, effectively getting GC "for free."

### WAL Management

| System | Mechanism | Trigger | Default Threshold |
|--------|-----------|---------|-------------------|
| SQLite | `wal_autocheckpoint` | Page count threshold | 1,000 pages |
| DuckDB | `checkpoint_threshold` | WAL size threshold | 16 MB |
| PostgreSQL | Checkpoint | Time or WAL size | `max_wal_size` = 1 GB, `checkpoint_timeout` = 5 min |

**SQLite** auto-checkpoints when the WAL reaches 1,000 pages (`PRAGMA wal_autocheckpoint`). Checkpointing runs inline (no background thread) in the committing connection's context, using PASSIVE mode so it never blocks readers. On close, a final checkpoint attempt is made.

**DuckDB** checkpoints when the WAL reaches `checkpoint_threshold` (16 MB). Like SQLite, it runs inline with no background daemon. `FORCE CHECKPOINT` can abort active transactions for administrative operations.

**PostgreSQL** triggers checkpoints when WAL reaches `max_wal_size` (1 GB) or `checkpoint_timeout` (5 min) elapses. A dedicated checkpointer process handles this asynchronously.

### Compaction Strategies

**RocksDB** offers three compaction strategies, each trading different amplification factors:

- **Leveled** (default): Data organized into levels L0..Ln, each ~10× larger. Minimizes space amplification (~1.1×) at higher write amplification (~10-30×). Best for read-heavy workloads. Triggers at `level0_file_num_compaction_trigger` = 4 L0 files.
- **Universal**: All sorted runs treated uniformly. Lower write amplification but higher space amplification (up to 2×). Best for write-heavy workloads.
- **FIFO**: Oldest files dropped when total exceeds `max_table_files_size`. Write amplification = 1. Only for TTL/time-series data.

Key settings: `max_background_jobs` = 2, `max_bytes_for_level_base` = 256 MB, `target_file_size_base` = 64 MB, `max_bytes_for_level_multiplier` = 10.

### Resource Throttling

| System | Mechanism | Key Parameters |
|--------|-----------|----------------|
| PostgreSQL | Cost-based vacuum throttling | `vacuum_cost_page_hit` = 1, `page_miss` = 2, `page_dirty` = 20, `cost_limit` = 200, `cost_delay` = 2 ms |
| RocksDB | Token-bucket rate limiter | `rate_bytes_per_sec`, `refill_period_us` = 100 ms |
| RocksDB | Write stall | `level0_slowdown_writes_trigger` = 20, `level0_stop_writes_trigger` = 36 |

**PostgreSQL's cost model** assigns costs to I/O operations during vacuum: buffer hit = 1, disk read = 2, dirty write = 20. When accumulated cost reaches `vacuum_cost_limit` (200), vacuum sleeps for `autovacuum_vacuum_cost_delay` (2 ms) then resets. This rate-limits vacuum I/O to ~10% of disk bandwidth under default settings.

**RocksDB's rate limiter** caps total compaction+flush I/O using a token bucket with configurable `rate_bytes_per_sec` and 100 ms refill interval. A fairness parameter (default 10) prioritizes flush over compaction. Write stalls provide a hard backstop: writes slow at 20 L0 files and stop completely at 36.

### Configuration Patterns

| System | Config Method | Per-Object | Runtime Tunable |
|--------|-------------|------------|-----------------|
| PostgreSQL | `postgresql.conf` + per-table `ALTER TABLE SET (autovacuum_...)` | Yes | Yes (`ALTER SYSTEM SET`) |
| RocksDB | C++ options / `options` file | Per column family | `SetOptions()` API |
| SQLite | `PRAGMA` statements | No | Yes |
| CockroachDB | SQL `CONFIGURE ZONE USING` | Per table/index | Yes |
| DuckDB | SQL `SET` statements | No | Yes |

**Key pattern**: Every production system provides (a) a config file for defaults, (b) programmatic override at open/runtime, and (c) at least some runtime tunability without restart.

---

## 4. Proposed Features

### Feature 1: Auto-GC

**Description:** Periodically garbage-collect old MVCC versions from all branches, using a computed safe point to avoid reclaiming versions still needed by active transactions or snapshots.

**Builds on:** `ShardedStore::gc_branch()` (`sharded.rs:613`), `TransactionCoordinator::active_count()` (`coordinator.rs:239`)

**Config knobs:**

| Knob | Type | Default | Description |
|------|------|---------|-------------|
| `auto_gc` | `bool` | `true` | Enable automatic GC |
| `gc_interval_secs` | `u64` | `300` | Seconds between GC passes |
| `gc_version_retention` | `u64` | `1000` | Keep at least this many recent versions (safety margin) |

**Default justification:** 300s (5 min) matches PostgreSQL's `autovacuum_naptime` = 1 min order of magnitude, balanced against Strata's simpler workloads. CockroachDB uses 4h TTL but that's for cross-node MVCC; single-node GC can be more aggressive.

**Safe point computation:**
```
safe_point = current_version - gc_version_retention
safe_point = min(safe_point, min_active_transaction_version)
safe_point = min(safe_point, min_live_snapshot_version)
```

### Feature 2: Auto-Compaction

**Description:** Automatically compact WAL segments when accumulated size or segment count exceeds a threshold.

**Builds on:** `WalOnlyCompactor::compact()` (`compaction/wal_only.rs`), `CompactMode::WALOnly`

**Config knobs:**

| Knob | Type | Default | Description |
|------|------|---------|-------------|
| `auto_compact` | `bool` | `true` | Enable automatic compaction |
| `compact_threshold_mb` | `u64` | `256` | Trigger compaction when WAL exceeds this size |
| `compact_mode` | `String` | `"wal_only"` | Default compaction mode (`"wal_only"` or `"full"`) |

**Default justification:** 256 MB is between SQLite's conservative 4 MB (1000 × 4 KB pages) and PostgreSQL's 1 GB `max_wal_size`. DuckDB uses 16 MB but targets embedded analytics. 256 MB balances disk usage against compaction frequency for a general-purpose database.

### Feature 3: Tombstone Cleanup

**Description:** Periodically remove tombstone entries older than a configurable age.

**Builds on:** `TombstoneIndex::cleanup_before()` (`tombstone.rs:347`)

**Config knobs:**

| Knob | Type | Default | Description |
|------|------|---------|-------------|
| `tombstone_cleanup` | `bool` | `true` | Enable automatic tombstone cleanup |
| `tombstone_ttl_secs` | `u64` | `3600` | Remove tombstones older than this (seconds) |

**Default justification:** 1 hour is long enough for any in-flight operations to complete (Strata transactions are short-lived) but short enough to prevent unbounded growth. CockroachDB's 4h GC TTL is a reasonable upper bound; 1h is conservative for a single-node system.

### Feature 4: TTL Enforcement

**Description:** Periodically scan the TTL index for expired keys and remove them from storage.

**Builds on:** `TTLIndex::find_expired()` (`ttl.rs:59`), `TTLIndex::remove_expired()` (`ttl.rs:70`)

**Config knobs:**

| Knob | Type | Default | Description |
|------|------|---------|-------------|
| `ttl_enforcement` | `bool` | `true` | Enable automatic TTL cleanup |
| `ttl_check_interval_secs` | `u64` | `60` | Seconds between TTL enforcement passes |

**Default justification:** 60s provides sub-minute enforcement latency. TiKV's `tikv_gc_run_interval` = 10 min is for cluster-wide coordination; single-node TTL checks are cheap (BTreeMap range query) and can run more frequently.

### Feature 5: Search Segment Merging

**Description:** Merge multiple small sealed search segments into larger ones to reduce query-time merge overhead.

**Builds on:** `InvertedIndex` sealed segment architecture (`search/index.rs`), existing `seal_active()` and `freeze_to_disk()` methods

**Config knobs:**

| Knob | Type | Default | Description |
|------|------|---------|-------------|
| `segment_merge` | `bool` | `true` | Enable automatic segment merging |
| `segment_merge_threshold` | `usize` | `10` | Merge when sealed segment count exceeds this |
| `segment_merge_max_size_mb` | `u64` | `512` | Don't merge segments larger than this |

**Default justification:** Lucene's default `mergeFactor` is 10 (merge when 10 segments at a level accumulate). This is the most well-tested threshold in the search industry. The 512 MB max prevents merging already-large segments, following RocksDB's `target_file_size_base` philosophy.

### Feature 6: Orphaned Branch Cleanup

**Description:** After recovery marks branches as orphaned, automatically clean up their data after a grace period.

**Builds on:** `BranchIndex::mark_orphaned()` (`recovery/replay.rs:175`), `BranchStatus::Orphaned` (`branch_types.rs:39`)

**Config knobs:**

| Knob | Type | Default | Description |
|------|------|---------|-------------|
| `cleanup_orphaned_branches` | `bool` | `true` | Enable orphaned branch cleanup |
| `orphan_grace_period_secs` | `u64` | `300` | Wait this long before cleaning orphaned branches |

**Default justification:** 5-minute grace period allows investigation of crash-orphaned branches while preventing indefinite accumulation. Aligns with the GC interval.

### Feature 7: Maintenance Observability

**Description:** Expose detailed maintenance metrics via a new command and tracing events.

**Builds on:** `SchedulerStats` (`background.rs:38`), existing `EmbedStatusInfo` pattern

**Proposed metrics:**

```rust
pub struct MaintenanceStatus {
    /// Versions pruned in last GC pass
    pub last_gc_versions_pruned: u64,
    /// Bytes reclaimed in last compaction
    pub last_compact_bytes_reclaimed: u64,
    /// Tombstones removed in last cleanup
    pub last_tombstones_removed: u64,
    /// Expired keys removed in last TTL pass
    pub last_expired_keys_removed: u64,
    /// Search segments merged in last pass
    pub last_segments_merged: u64,
    /// Orphaned branches cleaned in last pass
    pub last_orphans_cleaned: u64,
    /// Timestamp of last maintenance cycle (epoch micros)
    pub last_run_timestamp: u64,
    /// Total maintenance cycles completed
    pub total_cycles: u64,
    /// Current scheduler stats
    pub scheduler: SchedulerStats,
}
```

**Exposure:** New `Command::MaintenanceStatus` returning `Output::MaintenanceStatus(MaintenanceStatus)`, following the `EmbedStatus` pattern in `executor.rs:200`.

---

## 5. Architecture

### MaintenanceCoordinator

A new struct that runs a background loop, periodically submitting maintenance tasks to the existing `BackgroundScheduler`:

```rust
pub struct MaintenanceCoordinator {
    config: Arc<RwLock<MaintenanceConfig>>,
    db: Arc<Database>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    status: Arc<Mutex<MaintenanceStatus>>,
}
```

**Lifecycle:**
1. Created during `Database::open()` if any maintenance feature is enabled
2. Spawns a single background thread named `strata-maint`
3. Thread sleeps with `Condvar::wait_timeout` (same pattern as embed batch timer)
4. On wake, checks each maintenance task's interval and submits due tasks to `BackgroundScheduler` at `TaskPriority::Low`
5. On `Database::close()`, sets `shutdown` flag and notifies condvar for instant wake + exit

**Task submission pattern** (mirrors `embed_hook.rs:162-171`):
```rust
if self.db.scheduler()
    .submit(TaskPriority::Low, move || {
        run_gc(&db_clone, &config);
    })
    .is_err()
{
    // Backpressure — skip this cycle, try next interval
    warn!(target: "strata::maintenance", "Scheduler full, skipping GC cycle");
}
```

### Safe Point Computation

GC requires knowing the minimum version that any active reader might need:

```rust
fn compute_safe_point(db: &Database, config: &MaintenanceConfig) -> u64 {
    let current = db.transaction_coordinator().current_version();
    let retention = config.gc_version_retention;

    // Start with version-based retention
    let mut safe = current.saturating_sub(retention);

    // Lower to protect active transactions
    let min_active = db.transaction_coordinator().min_active_version();
    if let Some(v) = min_active {
        safe = safe.min(v);
    }

    // Lower to protect live snapshots
    let min_snapshot = db.snapshot_registry().min_version();
    if let Some(v) = min_snapshot {
        safe = safe.min(v);
    }

    safe
}
```

This follows TiKV's safe-point model: GC never reclaims versions that any live reader might reference.

### Error Handling

Maintenance is **best-effort**:
- Errors are logged via `tracing::warn!`, never propagated to callers
- A failed maintenance task does not prevent the next cycle
- Individual task failures don't affect other tasks in the same cycle
- Follows the embed hook pattern where `flush_embed_buffer` logs warnings and continues

### Shutdown Coordination

```rust
// In MaintenanceCoordinator::shutdown()
self.shutdown.store(true, Ordering::SeqCst);
self.condvar.notify_all();  // Wake sleeping thread immediately
if let Some(handle) = self.thread.take() {
    handle.join().ok();
}
// BackgroundScheduler::drain() handles in-flight tasks
```

This mirrors the pattern used for `BackgroundScheduler::shutdown()` and ensures clean teardown during `Database::close()`.

---

## 6. Configuration Design

### MaintenanceConfig

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceConfig {
    // -- GC --
    #[serde(default = "default_true")]
    pub auto_gc: bool,
    #[serde(default = "default_gc_interval")]
    pub gc_interval_secs: u64,          // 300
    #[serde(default = "default_gc_retention")]
    pub gc_version_retention: u64,      // 1000

    // -- Compaction --
    #[serde(default = "default_true")]
    pub auto_compact: bool,
    #[serde(default = "default_compact_threshold")]
    pub compact_threshold_mb: u64,      // 256
    #[serde(default = "default_compact_mode")]
    pub compact_mode: String,           // "wal_only"

    // -- Tombstones --
    #[serde(default = "default_true")]
    pub tombstone_cleanup: bool,
    #[serde(default = "default_tombstone_ttl")]
    pub tombstone_ttl_secs: u64,        // 3600

    // -- TTL --
    #[serde(default = "default_true")]
    pub ttl_enforcement: bool,
    #[serde(default = "default_ttl_interval")]
    pub ttl_check_interval_secs: u64,   // 60

    // -- Search segments --
    #[serde(default = "default_true")]
    pub segment_merge: bool,
    #[serde(default = "default_merge_threshold")]
    pub segment_merge_threshold: usize, // 10

    // -- Orphans --
    #[serde(default = "default_true")]
    pub cleanup_orphaned_branches: bool,
    #[serde(default = "default_orphan_grace")]
    pub orphan_grace_period_secs: u64,  // 300
}
```

### strata.toml Integration

Nested under `[maintenance]` in the existing `strata.toml`:

```toml
durability = "standard"
auto_embed = false

[maintenance]
# MVCC garbage collection
auto_gc = true
gc_interval_secs = 300
gc_version_retention = 1000

# WAL compaction
auto_compact = true
compact_threshold_mb = 256
compact_mode = "wal_only"

# Tombstone cleanup
tombstone_cleanup = true
tombstone_ttl_secs = 3600

# TTL enforcement
ttl_enforcement = true
ttl_check_interval_secs = 60

# Search segment merging
segment_merge = true
segment_merge_threshold = 10

# Orphaned branch cleanup
cleanup_orphaned_branches = true
orphan_grace_period_secs = 300
```

### StrataConfig Changes

Add `MaintenanceConfig` as an optional field in `StrataConfig` (`crates/engine/src/database/config.rs:66`):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrataConfig {
    #[serde(default = "default_durability_str")]
    pub durability: String,
    #[serde(default)]
    pub auto_embed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed_batch_size: Option<usize>,
    // ... existing fields ...

    /// Maintenance configuration. Defaults to all-enabled with conservative intervals.
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
}
```

### Open-Time Override

`Database::open_with_config()` already writes the provided `StrataConfig` to `strata.toml` and uses it for the session. Adding `MaintenanceConfig` inside `StrataConfig` means callers can override maintenance settings at open time:

```rust
let config = StrataConfig {
    maintenance: MaintenanceConfig {
        auto_gc: true,
        gc_interval_secs: 60,  // More aggressive for this workload
        ..MaintenanceConfig::default()
    },
    ..StrataConfig::default()
};
let db = Database::open_with_config("/path/to/data", config)?;
```

### Runtime Tunability

Expose through the existing `config` `RwLock` pattern (same as `set_auto_embed()`):

```rust
impl Database {
    pub fn update_maintenance_config<F>(&self, f: F)
    where
        F: FnOnce(&mut MaintenanceConfig),
    {
        let mut cfg = self.config.write();
        f(&mut cfg.maintenance);
        // MaintenanceCoordinator picks up changes on next wake cycle
    }
}
```

No restart required — the `MaintenanceCoordinator` reads `config` through the shared `Arc<RwLock<StrataConfig>>` on each wake cycle.

---

## 7. Implementation Roadmap

### Phase 1: Critical (Auto-GC + Auto-Compaction)

**Goal:** Eliminate the two most impactful maintenance gaps — unbounded memory growth from version chains and unbounded disk growth from WAL segments.

**Tasks:**
1. Add `MaintenanceConfig` to `StrataConfig` with serde defaults
2. Implement `MaintenanceCoordinator` with condvar-based sleep loop
3. Implement auto-GC with safe point computation
   - Add `min_active_version()` to `TransactionCoordinator`
   - Add snapshot version tracking (or use existing `active_count()` as proxy)
   - Submit `gc_branch()` calls via `BackgroundScheduler` at `TaskPriority::Low`
4. Implement auto-compaction
   - Query WAL directory size on each wake
   - Submit `compact(CompactMode::WALOnly)` when threshold exceeded
5. Wire `MaintenanceCoordinator` into `Database::open()` and `Database::close()`
6. Add `[maintenance]` section to default `strata.toml` template
7. Tests: verify GC respects safe point, compaction triggers at threshold, shutdown is clean

### Phase 2: High (Tombstone Cleanup + TTL Enforcement)

**Goal:** Prevent accumulation of deleted data markers and enforce TTL semantics.

**Tasks:**
1. Add tombstone cleanup task to `MaintenanceCoordinator`
   - Compute cutoff: `now_micros - tombstone_ttl_secs × 1_000_000`
   - Call `TombstoneIndex::cleanup_before(cutoff)`
2. Add TTL enforcement task
   - Call `TTLIndex::find_expired(now)` to get expired keys
   - Delete expired keys from storage (issue delete commands)
   - Call `TTLIndex::remove_expired(now)` to clean the index
3. Tests: verify tombstones cleaned after TTL, expired keys removed, no premature cleanup

### Phase 3: Medium (Segment Merging + Orphan Cleanup + Observability)

**Goal:** Address remaining maintenance gaps and provide operational visibility.

**Tasks:**
1. Implement search segment merging
   - Add `merge_sealed_segments()` to `InvertedIndex`
   - Merge N smallest segments into one, rebuild posting lists, write new `.sidx`
   - Remove old segment files, update manifest
2. Implement orphaned branch cleanup
   - After grace period, delete orphaned branch data from storage
   - Remove branch from `BranchIndex`
3. Implement `MaintenanceStatus` and `Command::MaintenanceStatus`
   - Track per-task metrics in `MaintenanceCoordinator`
   - Expose via executor command, following `EmbedStatus` pattern
4. Add tracing spans for each maintenance task (`tracing::info_span!("maintenance::gc")`)
5. Tests: verify segment merging reduces count, orphan cleanup respects grace period, status reports accurately
