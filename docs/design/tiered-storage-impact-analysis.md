# Tiered Storage: Impact Analysis

**Status:** Reference
**Date:** 2026-03-11
**Scope:** Per-branch KV segment store (bounded memory). No branching model changes.
**Related:** [Implementation Scope](tiered-storage-implementation-scope.md), [Design Review](tiered-storage-review.md)

> This document maps every current crate and module to its fate under the tiered storage implementation:
> **survives** (unchanged), **adapts** (interface changes), **rewrites** (new implementation), or **obsolete** (removed).

---

## The Key Insight: Storage Trait Is the Firewall

The `Storage` trait (`crates/core/src/traits.rs:69-244`) is the abstraction boundary. Everything above it — concurrency, engine, executor — consumes storage through this trait. If the new segment-based storage implements the same `Storage` trait, **everything above survives unchanged.**

```
                    ┌──────────────┐
                    │   executor   │  survives (no storage dependency)
                    ├──────────────┤
                    │    engine    │  adapts (background flush/compaction scheduling)
                    ├──────────────┤
                    │ concurrency  │  survives (uses Storage trait for OCC validation)
                    ├──────────────┤
                    │  durability  │  adapts (recovery rewrite, WAL truncation)
                    ╞══════════════╡
                    │ Storage trait│  ← FIREWALL — new impl goes below this line
                    ╞══════════════╡
                    │   storage    │  REWRITE — ShardedStore → SegmentedStore
                    ├──────────────┤
                    │     core     │  survives (defines traits, types, contracts)
                    └──────────────┘
```

---

## Crate-by-Crate Impact

### 1. `strata-core` — SURVIVES (0 changes)

| Module | Lines | Verdict | Notes |
|--------|-------|---------|-------|
| `traits.rs` (Storage, SnapshotView, WriteMode) | ~244 | **Survives** | The new storage implements this exact trait |
| `primitive_ext.rs` (PrimitiveStorageExt) | ~169 | **Survives** | WAL integration unchanged |
| `contract/` (Key, Value, VersionedValue, BranchId) | ~1,200 | **Survives** | Core types unchanged |
| `types.rs` (TypeTag, Namespace, etc.) | ~400 | **Survives** | No storage dependency |
| `error.rs` (StrataError) | ~300 | **Survives** | May add new error variants later |

**Total impact: 0 lines changed.**

---

### 2. `strata-storage` — REWRITE (~7,600 lines)

This is where the work happens. The current crate is ~7,600 lines. The new implementation replaces the in-memory backend with a segment-based backend while preserving the `Storage` trait interface.

| Module | Lines | Verdict | Details |
|--------|-------|---------|---------|
| **sharded.rs** | 5,966 | | |
| — `ShardedStore` | ~1,100 | **Rewrite** | Replace `DashMap<BranchId, Shard>` with per-branch segment store. New struct `SegmentedStore` implements `Storage` trait. |
| — `Shard` | ~340 | **Obsolete** | `FxHashMap<Key, VersionChain>` + `BTreeSet<Key>` replaced by memtable + segments |
| — `VersionChain` | ~280 | **Obsolete** | MVCC versions now live in segments sorted by `(TypedKey ASC, commit_id DESC)`. No in-memory version chain. |
| — `ShardedSnapshot` | ~210 | **Rewrite** | New impl: `(version, Arc<SegmentedStore>)` that reads through memtable → segments with version filter |
| — Storage trait impl | ~400 | **Rewrite** | New impl delegates to memtable + segment read path |
| — Branch ops (list, clear, gc, fork) | ~600 | **Adapts** | Branch listing, clearing survive. GC becomes segment compaction. Fork stays O(N). |
| — Stats/diagnostics | ~200 | **Adapts** | New metrics: segment count, memtable size, block cache hit rate |
| **stored_value.rs** | 631 | **Survives** | TTL bitpacking, tombstone encoding — still needed for WAL records and value transport. Segments store values differently (block-encoded) but StoredValue is used at the API boundary. |
| **index.rs** | 328 | **Survives** | `BranchIndex` and `TypeIndex` used by search/recovery, not by core storage path. Unchanged. |
| **ttl.rs** | 235 | **Adapts** | TTL tracking needs rethinking. Current `BTreeMap<Timestamp, HashSet<Key>>` is unbounded. Options: per-segment TTL metadata, or bounded in-memory TTL index for recent entries only. |
| **registry.rs** | 443 | **Survives** | `PrimitiveRegistry` routes WAL entries. Unchanged. |
| **primitive_ext.rs** | 6 | **Survives** | Re-export wrapper. Unchanged. |
| **lib.rs** | 29 | **Adapts** | Export `SegmentedStore` instead of / alongside `ShardedStore` |

#### New modules to create:

| New Module | Est. Lines | Purpose |
|------------|-----------|---------|
| `memtable.rs` | ~800 | Concurrent skiplist memtable with `ArcSwap` publication. `get_versioned()`, `iter_seek()`, `put()`, `freeze()`. Uses `crossbeam-skiplist` or custom. |
| `segment.rs` | ~1,200 | KVSegment: block-based SST format, bloom filter, index block, prefix compression. `point_lookup()`, `iter_seek()`, `build()` from sorted iterator. |
| `segment_builder.rs` | ~600 | Builds KVSegment from frozen memtable. Data blocks, index block, filter block, properties block, footer. |
| `block_cache.rs` | ~400 | Sharded LRU/CLOCK cache for index blocks, filter blocks, hot data blocks. |
| `merge_iter.rs` | ~300 | K-way merge iterator across memtables and segments. MVCC-aware prefix scan. |
| `flush.rs` | ~400 | Flush coordinator: freeze memtable → build segment → publish → drop memtable. |
| `compaction.rs` | ~600 | Per-branch segment compaction: merge overlapping segments, prune old versions, produce new segment. |
| `key_encoding.rs` | ~200 | `TypedKeyBytes || EncodeDesc(commit_id)` encoding shared by memtable and segments. |
| `branch_store.rs` | ~500 | Per-branch state: `ArcSwap<Memtable>`, immutable memtable list, segment list, commit head. |
| `segmented_store.rs` | ~800 | Top-level `SegmentedStore` implementing `Storage` trait. Delegates to per-branch `BranchStore`. |

**Estimated new code: ~5,800 lines.**
**Estimated removed code: ~4,900 lines** (ShardedStore, Shard, VersionChain, old snapshot).
**Net: ~+900 lines.**

---

### 3. `strata-durability` — SIGNIFICANT ADAPTATION (~18,400 lines, ~1,000 changed)

The WAL format, writer, reader, codec, and coordination infrastructure all survive unchanged. But **recovery is fundamentally different** with segment-based storage, and **WAL truncation is a new concept** that doesn't exist today. This section was initially underestimated.

#### Why recovery changes fundamentally

Today's recovery:
```
Startup → replay ALL WAL entries → build ShardedStore from scratch
```

With segments:
```
Startup → discover existing KV segments on disk
        → determine flushed_through_commit_id (what's already in segments)
        → replay only WAL entries AFTER last flush → build memtable with delta
        → handle crash edge cases (orphaned segments, partial flushes)
```

This is **simpler at runtime** (replay 1,000 entries instead of 10M) but **more complex in code** (segment discovery, watermark coordination, crash edge cases).

#### Why WAL truncation is new

Today, WAL segments are truncated based on snapshot watermarks (the `snapshot_watermark` in MANIFEST). With segment-based storage, a second truncation criterion emerges: WAL segments whose contents are fully captured in KV segments can be deleted. This requires:

1. A new `flushed_through_commit_id` field in MANIFEST
2. `wal_only.rs` compaction needs to use `max(snapshot_watermark, flushed_through_commit_id)` as the safe truncation point
3. Coordination between flush completion (storage crate) and MANIFEST update (durability crate)

#### New cross-crate coordination: flush → WAL truncation

A new integration point emerges that doesn't exist today:

```
Storage: flush complete (memtable → KV segment, through commit_id=X)
    ↓
Durability: advance flushed_through_commit_id to X in MANIFEST
    ↓
WAL compaction: segments with max_txn_id ≤ X are safe to delete
```

This coordination can be handled by:
- A callback from storage to durability on flush completion
- The engine's BackgroundScheduler polling and coordinating
- A `FlushWatermarkNotifier` that bridges the two crates

The recommended approach is a simple callback: `SegmentedStore` takes an `on_flush_complete(commit_id)` callback at construction, and the engine wires it to the MANIFEST update.

#### Module-by-module assessment

| Module | Lines | Verdict | Details |
|--------|-------|---------|---------|
| **wal/writer.rs** | 1,521 | **Survives** | WAL append, segment rotation, durability modes — all unchanged. |
| **wal/reader.rs** | 1,268 | **Survives** | WAL reading, corruption recovery — unchanged. |
| **wal/mode.rs** | 107 | **Survives** | Durability mode enum — unchanged. |
| **wal/config.rs** | 133 | **Survives** | WAL configuration — unchanged. |
| **format/wal_record.rs** | 905 | **Survives** | Record format, segment headers, CRC — unchanged. |
| **format/writeset.rs** | 712 | **Survives** | Mutation serialization — unchanged. Entity types stay the same. |
| **format/manifest.rs** | 544 | **Adapts (~100 lines)** | Needs new fields: `flushed_through_commit_id` (highest commit_id captured in KV segments), `segment_inventory` (list of known KV segment files with commit ranges). Serialization/deserialization updated. MANIFEST is still atomic-write-fsync-rename. |
| **format/primitives.rs** | 968 | **Survives** | Snapshot serialization per primitive — unchanged for now. |
| **format/snapshot.rs** | 405 | **Survives** | Snapshot file format — unchanged. |
| **format/segment_meta.rs** | 387 | **Survives** | Per-segment metadata hints — unchanged. |
| **format/watermark.rs** | 303 | **Adapts (~50 lines)** | Needs to track both snapshot watermark and flush watermark. `CheckpointInfo` may gain a `flushed_through` field. |
| **recovery/coordinator.rs** | 1,162 | **Significant adaptation (~500 lines)** | Recovery algorithm changes substantially. New steps: (1) scan `segments/kv/` for existing segment files, (2) validate segment integrity (check headers/footers), (3) determine `flushed_through_commit_id` from segment metadata, (4) replay only WAL entries with `commit_id > flushed_through_commit_id` into fresh memtable, (5) handle orphaned segments (segment file exists but MANIFEST doesn't reference it — adopt or delete), (6) handle partial flush (`.partial` file in `tmp/` — delete and re-derive from WAL). The core recovery algorithm is rewritten but the WAL scanning infrastructure (`WalReplayer`) is reused. |
| **recovery/replayer.rs** | 514 | **Survives** | WAL record iteration — unchanged. Replayer yields records; coordinator filters and applies them. |
| **compaction/wal_only.rs** | 780 | **Adapts (~100 lines)** | Safe truncation point becomes `max(snapshot_watermark, flushed_through_commit_id)`. Must check both watermarks before deleting WAL segments. New method: `compact_with_flush_watermark(flushed_through)`. |
| **compaction/tombstone.rs** | 815 | **Survives** | Tombstone tracking — survives as-is. Useful for segment compaction version pruning. |
| **compaction/mod.rs** | 247 | **Survives** | CompactMode enum (WALOnly only). |
| **coordination.rs** | 200 | **Survives** | File locks, counter files — unchanged. Multi-process coordination preserved. |
| **database/*.rs** | ~1,050 | **Adapts (~150 lines)** | `DatabaseHandle` needs awareness of segment directory structure. `paths.rs` needs new directories: `segments/kv/`, `tmp/`. `config.rs` may gain storage engine selection. `handle.rs` needs segment directory creation on first open. |
| **codec/*.rs** | ~400 | **Survives** | Codec abstraction — unchanged. |
| **retention/*.rs** | ~600 | **Survives** | Retention policies — unchanged. Used by segment compaction to determine version pruning floor. |
| **branch_bundle/*.rs** | ~2,500 | **Adapts (~100 lines)** | Export/import needs to handle segment files. Options: (1) bundle segment files directly (fastest import, largest bundle), (2) re-derive from WAL on import (current approach, still works), (3) export segments + WAL delta. Option 2 works today with no changes; option 1 or 3 can be deferred. |
| **snapshot.rs** | ~100 | **Survives** | SnapshotSerializable trait — unchanged. |
| **disk_snapshot/*.rs** | ~1,400 | **Survives** | Snapshot write/read/checkpoint — unchanged. With segments, full snapshots become less critical for recovery (segments ARE durable), but remain useful for fast cold-start and branch bundles. |

**Summary: ~1,000 lines of changes across 6 modules. Recovery coordinator is the largest single change (~500 lines). WAL infrastructure (writer, reader, format) is completely unchanged.**

**Net effect on recovery:** More complex code, but dramatically faster runtime. Replaying 1,000 WAL entries (delta since last flush) vs 10M entries (everything since database creation) is orders of magnitude faster. Cold start goes from "rebuild entire database from WAL" to "load segments + replay small delta."

---

### 4. `strata-concurrency` — SURVIVES (~6,250 lines)

The concurrency layer interacts with storage exclusively through the `Storage` trait. If the new `SegmentedStore` implements `Storage`, the concurrency layer is unchanged.

| Module | Lines | Verdict | Details |
|--------|-------|---------|---------|
| **transaction.rs** | 2,278 | **Survives** | TransactionContext calls `store.get_version_only()`, `store.put_with_version()`, `store.delete_with_version()`. These are all `Storage` trait methods. Unchanged. |
| **manager.rs** | 1,704 | **Survives** | Commit orchestration, blind write detection, per-branch locks. All generic over `S: Storage`. Unchanged. |
| **validation.rs** | 418 | **Survives** | OCC validation calls `store.get_version_only()`. Generic over `S: Storage`. Unchanged. |
| **conflict.rs** | 268 | **Survives** | JSON path conflict detection. No storage dependency. |
| **payload.rs** | 143 | **Survives** | WAL payload serialization. No storage dependency. |
| **recovery.rs** | 932 | **Adapts** | Currently creates `ShardedStore::new()` and replays into it. Needs to create `SegmentedStore::new()` instead. ~10 lines change. |
| **snapshot.rs** | 480 | **Survives** | ClonedSnapshotView (test/recovery only). Unchanged. |

**Total impact: ~10 lines changed** (recovery.rs storage type).

**Performance note:** OCC validation (`get_version_only()` per read-set key) may now hit segments (disk I/O) instead of in-memory hash lookup. This is a latency trade-off, not a correctness issue. For recently-written keys (the common case during OCC validation), the memtable will serve the read — no disk I/O.

---

### 5. `strata-engine` — MODERATE CHANGES (~57,800 lines, ~500 changed)

The engine accesses storage through `Arc<ShardedStore>` on the `Database` struct and via the `Storage` trait. Most changes are type annotation swaps, but **background flush/compaction scheduling** is genuinely new work that doesn't exist today.

#### Why background scheduling matters

Today, the `BackgroundScheduler` exists but `submit()` is never called (issue #1408). With segment-based storage, background work becomes critical:

- **Memtable flush** must happen when the memtable hits its size threshold, or on a timer, or under memory pressure. If flush doesn't happen, the memtable grows unbounded and the WAL grows unbounded (can't truncate entries still in the memtable).
- **Segment compaction** must run periodically to merge small segments, prune old versions, and reclaim disk space. Without compaction, read performance degrades (more segments to scan) and disk grows without bound.
- **Memory pressure monitoring** must trigger emergency flushes when the memory budget is approached. This is the core mechanism that prevents OOM.

These are new responsibilities for the engine layer. The storage layer provides the primitives (`flush()`, `compact()`, `memory_usage()`), but the engine decides *when* to invoke them.

#### Module-by-module assessment

| Module | Lines | Verdict | Details |
|--------|-------|---------|---------|
| **database/mod.rs** | 3,660 | **Adapts (~100 lines)** | `storage: Arc<ShardedStore>` → `Arc<SegmentedStore>`. Recovery flow constructs `SegmentedStore` with data directory for segment storage. Needs to wire flush-completion callback to durability MANIFEST update. Needs to start background flush/compaction tasks on open. |
| **database/config.rs** | 831 | **Adapts (~80 lines)** | New `StorageConfig` fields: `write_buffer_size` (memtable size, default 128MB), `block_cache_size` (default 2GB), `bloom_bits_per_key` (default 10), `max_immutable_memtables` (default 2), `compaction_threshold` (segment count trigger). Presets for constrained/default/server. |
| **coordinator.rs** | 1,653 | **Adapts (~10 lines)** | `use strata_storage::ShardedStore` → `SegmentedStore`. Type annotation change only. |
| **transaction_ops.rs** | 689 | **Survives** | Uses `Storage` trait generically. |
| **transaction/*.rs** | ~1,560 | **Survives** | Transaction wrapper. Uses `Storage` trait. |
| **branch_ops.rs** | 1,546 | **Survives** | Fork copies data via transactions. Works the same regardless of storage backend. |
| **bundle.rs** | 483 | **Survives** | Export/import via transactions. Backend-agnostic. |
| **recovery/*.rs** | ~1,130 | **Adapts (~20 lines)** | Recovery builds storage. Type change from ShardedStore → SegmentedStore. Recovery coordinator in durability handles segment discovery; engine just passes config. |
| **primitives/kv.rs** | 1,385 | **Adapts (~20 lines)** | Calls `db.storage().get_value_direct()` and `db.storage().get_history()`. These are ShardedStore-specific methods (not on Storage trait). Need equivalents on SegmentedStore. |
| **primitives/event.rs** | 1,474 | **Adapts (~5 lines)** | Calls `db.storage().get()` directly. Same method needed on new store. |
| **primitives/state.rs** | 1,133 | **Adapts (~5 lines)** | Calls `db.storage().get()` directly. |
| **primitives/json.rs** | 2,013 | **Adapts (~5 lines)** | Calls `db.storage().get()` directly. |
| **primitives/vector/*.rs** | ~18,000 | **Adapts (~20 lines)** | Calls `db.storage().create_snapshot()` and `db.storage().branch_ids()`. These are ShardedStore-specific. Need equivalents on SegmentedStore. |
| **graph/*.rs** | ~12,500 | **Survives** | All graph ops go through transactions. Backend-agnostic. |
| **search/*.rs** | ~6,600 | **Survives** | Search has its own segment lifecycle. Independent of KV storage. |
| **background.rs** | 612 | **Adapts (~200 lines)** | This is the most significant engine change. BackgroundScheduler needs new scheduled tasks: (1) periodic memtable flush check — if any branch's memtable exceeds threshold, trigger flush, (2) periodic compaction check — if any branch has too many segments, trigger merge, (3) memory pressure monitor — check total memory usage against budget, trigger emergency flush if critical. Also needs flush-completion hook to notify durability of new `flushed_through_commit_id`. |
| **All other modules** | ~3,500 | **Survives** | Instrumentation, GC, extensions — no storage dependency. |

**Total impact: ~500 lines changed.** Most is new background scheduling logic (~200 lines) and config (~80 lines). Type annotation changes across primitives are mechanical (~100 lines).

**Key risk:** Several primitives call `db.storage()` and use ShardedStore-specific methods (`get_value_direct`, `get_history`, `create_snapshot`, `branch_ids`). These are NOT on the `Storage` trait. Two options:
1. Add these methods to `SegmentedStore` directly (matching ShardedStore's API)
2. Add them to the `Storage` trait (cleaner but wider blast radius)

Option 1 is simpler. The engine already knows its concrete storage type.

---

### 6. `strata-executor` — SURVIVES (~12,000 lines)

The executor has zero direct storage dependency. It accesses everything through engine primitives.

| Module | Lines | Verdict | Details |
|--------|-------|---------|---------|
| All handler modules | ~8,000 | **Survives** | Call engine primitives (KV, JSON, Event, etc.). No storage awareness. |
| Bridge/dispatch | ~2,000 | **Survives** | Routes commands to primitives. |
| CLI | ~2,000 | **Survives** | User interface. |

**Total impact: 0 lines changed.**

---

### 7. `strata-intelligence` — SURVIVES (~6,000 lines)

Embedding, search orchestration, model loading. No storage dependency.

**Total impact: 0 lines changed.**

---

### 8. `strata-security` — SURVIVES (~500 lines)

Security/permissions layer. No storage dependency.

**Total impact: 0 lines changed.**

---

## Summary: Lines of Code Impact

| Crate | Total Lines | Changed/New | % Changed | Verdict |
|-------|-------------|-------------|-----------|---------|
| `strata-core` | ~2,300 | 0 | 0% | Survives |
| `strata-storage` | ~7,600 | ~5,800 new + ~4,900 removed | 100% | **Rewrite** |
| `strata-durability` | ~18,400 | ~1,000 | ~5% | **Significant adaptation** (recovery rewrite, WAL truncation, MANIFEST fields) |
| `strata-concurrency` | ~6,250 | ~10 | <1% | Survives |
| `strata-engine` | ~57,800 | ~500 | <1% | Moderate changes (background scheduling, config, type annotations) |
| `strata-executor` | ~12,000 | 0 | 0% | Survives |
| `strata-intelligence` | ~6,000 | 0 | 0% | Survives |
| `strata-security` | ~500 | 0 | 0% | Survives |
| **Total** | **~110,850** | **~7,300** | **~6.6%** | |

The work concentrates in three crates:
- **`strata-storage`** — full rewrite (~5,800 new lines, the core engineering)
- **`strata-durability`** — recovery rewrite + WAL truncation (~1,000 lines, the hardest integration work)
- **`strata-engine`** — background scheduling + config (~500 lines, new operational logic)

Everything else (93% of the codebase) is untouched. The `Storage` trait firewall holds — concurrency, executor, intelligence, and security are unaffected.

---

## New Dependencies Required

| Crate | Purpose | Maturity |
|-------|---------|----------|
| `crossbeam-skiplist` | Lock-free concurrent skiplist for memtable | Stable, widely used |
| `arc-swap` | Atomic pointer swap for memtable/segment publication | Stable, 37M downloads |
| `crc32fast` | Block/record checksums (may already be a dep) | Stable |
| `lz4_flex` or `zstd` | Optional block compression | Stable |

No exotic dependencies. All are battle-tested Rust crates.

---

## What Becomes Obsolete

| Component | Lines | Why |
|-----------|-------|-----|
| `VersionChain` (Single/Multi enum) | ~280 | MVCC versions live in segments, sorted by `(key, commit_id DESC)`. No in-memory version chain. |
| `Shard` (FxHashMap + BTreeSet) | ~340 | Replaced by memtable (skiplist) + segment files. No per-branch hash map. |
| `ShardedStore` (DashMap core) | ~1,100 | Replaced by `SegmentedStore` with per-branch `BranchStore`. |
| `ShardedSnapshot` | ~210 | Replaced by segment-aware snapshot that reads through memtable → segments. |
| `BTreeSet<Arc<Key>>` ordered index | ~200 | Prefix scans handled by merge iterators over sorted memtable + sorted segments. No separate ordered index. |

**Total obsolete: ~2,130 lines** — all within `sharded.rs`.

---

## What Survives Unchanged

| Component | Lines | Why |
|-----------|-------|-----|
| `StoredValue` (TTL bitpacking) | 631 | Still used at API boundary for WAL records and value transport |
| `BranchIndex` / `TypeIndex` | 328 | Used by search/recovery, not by core storage path |
| `PrimitiveRegistry` | 443 | WAL entry routing — unchanged |
| `TTLIndex` | 235 | TTL tracking — adapts slightly but core logic survives |
| All WAL infrastructure | ~4,800 | WAL format, writer, reader — unchanged |
| All codec infrastructure | ~400 | Encryption seam — unchanged |
| All coordination | ~200 | File locks, counter files — unchanged |
| All retention policies | ~600 | Version pruning rules — unchanged, used by segment compaction |
| OCC validation | ~418 | Conflict detection via `Storage` trait — unchanged |
| Transaction context | ~2,278 | Read/write/delete sets — unchanged |
| Commit manager | ~1,704 | Blind write optimization, per-branch locks — unchanged |
| All engine primitives | ~47,000 | KV, JSON, Event, State, Vector, Graph — ~100 lines of type annotation changes |
| All executor handlers | ~12,000 | Command dispatch — unchanged |

---

## Migration Strategy

### Phase 1: Build SegmentedStore alongside ShardedStore

1. Create new modules in `crates/storage/src/`: memtable, segment, segment_builder, block_cache, merge_iter, flush, compaction, key_encoding, branch_store, segmented_store
2. `SegmentedStore` implements `Storage` trait
3. Both `ShardedStore` and `SegmentedStore` coexist
4. Feature flag: `storage = "segmented"` (default) vs `"memory"` (legacy)
5. Run existing test suite against both implementations

### Phase 2: Integrate with engine

1. `Database` struct takes `Arc<dyn Storage>` or `enum StorageBackend { Memory(ShardedStore), Segmented(SegmentedStore) }`
2. Add ShardedStore-equivalent convenience methods to SegmentedStore (`get_value_direct`, `create_snapshot`, `branch_ids`)
3. Update recovery to construct appropriate storage type
4. Update BackgroundScheduler to trigger flush/compaction

### Phase 3: Remove ShardedStore

1. Once SegmentedStore passes all tests and benchmarks
2. Remove `ShardedStore`, `Shard`, `VersionChain`
3. Remove feature flag, make segmented the only option

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| OCC validation latency increases (disk reads under commit lock) | Medium | Medium | Memtable serves recently-written keys; bloom filters skip cold segments |
| Prefix scan performance regression (merge iterator overhead) | Medium | Medium | Block cache warms hot segments; benchmark against current BTreeSet scan |
| Recovery correctness with segments + WAL delta | Medium | High | This is the hardest integration point. Extensive crash testing needed: crash mid-flush, crash after segment write but before MANIFEST update, crash during compaction. |
| WAL truncation coordination bugs (truncate WAL entry still needed by memtable) | Medium | High | Conservative approach: only truncate WAL segments where `max_txn_id < min(snapshot_watermark, flushed_through_commit_id)`. Never truncate the active WAL segment. |
| Data corruption in segment format | Low | High | Per-block CRC32, comprehensive property tests, run existing test suite |
| Background flush not keeping up with write rate | Medium | Medium | Memory pressure system triggers emergency flush; throttle ingestion at critical threshold |
| Memory accounting inaccuracy | Medium | Low | Start with conservative memtable budget; tune based on production metrics |
| TTL expiration behavior changes | Low | Medium | Existing TTLIndex survives for recent entries; segments carry TTL metadata |
| Flush-completion callback lost (crash between segment write and MANIFEST update) | Low | Medium | Recovery detects orphaned segments and either adopts them or deletes them |

---

## Files That Need Changes (Ordered by Implementation Phase)

### Phase 1: Storage crate — core segment store

1. `crates/storage/src/key_encoding.rs` — **New.** Foundation for everything else.
2. `crates/storage/src/memtable.rs` — **New.** In-memory write buffer.
3. `crates/storage/src/segment.rs` — **New.** On-disk immutable segment format.
4. `crates/storage/src/segment_builder.rs` — **New.** Build segment from sorted iterator.
5. `crates/storage/src/merge_iter.rs` — **New.** K-way merge for reads.
6. `crates/storage/src/block_cache.rs` — **New.** Bounded segment block cache.
7. `crates/storage/src/branch_store.rs` — **New.** Per-branch memtable + segment list.
8. `crates/storage/src/flush.rs` — **New.** Memtable → segment pipeline.
9. `crates/storage/src/compaction.rs` — **New.** Per-branch segment merge.
10. `crates/storage/src/segmented_store.rs` — **New.** Top-level `Storage` trait impl.
11. `crates/storage/src/lib.rs` — **Adapt.** Export new types alongside ShardedStore.

### Phase 2: Durability crate — recovery and WAL truncation

12. `crates/durability/src/format/manifest.rs` — **Adapt.** Add `flushed_through_commit_id`, `segment_inventory` fields. Update serialization.
13. `crates/durability/src/format/watermark.rs` — **Adapt.** Track flush watermark alongside snapshot watermark.
14. `crates/durability/src/recovery/coordinator.rs` — **Significant rewrite.** New recovery algorithm: discover segments → determine flush watermark → replay WAL delta → handle crash edge cases (orphaned segments, partial flushes).
15. `crates/durability/src/compaction/wal_only.rs` — **Adapt.** Use `max(snapshot_watermark, flushed_through_commit_id)` as safe truncation point.
16. `crates/durability/src/database/paths.rs` — **Adapt.** New directories: `segments/kv/`, `tmp/`.
17. `crates/durability/src/database/handle.rs` — **Adapt.** Create segment directories on first open.
### Phase 3: Engine + concurrency — integration

19. `crates/engine/src/database/config.rs` — **Adapt.** New `StorageConfig` with memtable/cache/bloom settings.
20. `crates/engine/src/database/mod.rs` — **Adapt.** Storage type change, wire flush callback to MANIFEST, start background tasks.
21. `crates/engine/src/background.rs` — **Adapt.** New scheduled tasks: flush check, compaction check, memory pressure monitor.
22. `crates/engine/src/coordinator.rs` — **Adapt.** Type annotation change.
23. `crates/engine/src/recovery/*.rs` — **Adapt.** Storage type change.
24. `crates/engine/src/primitives/kv.rs` — **Adapt.** Route to SegmentedStore equivalents of `get_value_direct`, `get_history`.
25. `crates/engine/src/primitives/vector/store.rs` — **Adapt.** Route to SegmentedStore equivalents of `create_snapshot`, `branch_ids`.
26. `crates/concurrency/src/recovery.rs` — **Adapt.** Storage type change (~10 lines).
