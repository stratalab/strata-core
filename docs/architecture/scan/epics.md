# Scan Pipeline Epics — Implementation Roadmap

> **Parent**: [lazy-iterator-design.md](lazy-iterator-design.md)
>
> Five epics, ordered by dependency. Each epic is independently shippable —
> it leaves the codebase in a correct, tested state. Later epics build on
> earlier ones but never invalidate them.

---

## Epic 1 — Storage primitives: `iter_range` + `OwnedSegmentIter::new_seek`

**Goal**: The two building blocks that every later epic depends on. No consumer
changes yet — existing behavior unchanged.

**Scope**:

| # | Task | File | Est. lines |
|---|------|------|-----------|
| 1a | Add `Memtable::iter_range(start_key, match_prefix)` | `crates/storage/src/memtable.rs` | ~15 |
| 1b | Unit test: `iter_range` seeks to target, not prefix start | `crates/storage/src/memtable.rs` (tests) | ~30 |
| 1c | Add `prefix_bytes: Option<Vec<u8>>` field to `OwnedSegmentIter` | `crates/storage/src/segment.rs` | ~5 |
| 1d | Add `OwnedSegmentIter::new_seek(segment, start_key, prefix_bytes)` constructor | `crates/storage/src/segment.rs` | ~25 |
| 1e | Add prefix matching to `OwnedSegmentIter::next()` (skip before, stop past) | `crates/storage/src/segment.rs` | ~15 |
| 1f | Add `OwnedSegmentIter::corruption_detected()` convenience method | `crates/storage/src/segment.rs` | ~5 |
| 1g | Existing `new()` sets `prefix_bytes: None` — verify compaction unchanged | `crates/storage/src/segment.rs` | ~2 |
| 1h | Unit test: `new_seek` with prefix matches `iter_seek` output | `crates/storage/src/segment.rs` (tests) | ~40 |
| 1i | Unit test: `new_seek` stops when past prefix range | `crates/storage/src/segment.rs` (tests) | ~25 |

**Acceptance**:
- `cargo test -p strata-storage` passes
- `OwnedSegmentIter::new_seek` produces identical entries to `SegmentIter::iter_seek` on the same segment
- Existing compaction tests unaffected (backward-compatible `new()`)

**Invariants verified**: LSM-001 (key ordering preserved in seek), LSM-007 (segment files unchanged)

---

## Epic 2 — Lazy merge builder + consumer refactors

**Goal**: Introduce `build_branch_merge_iter` and refactor all four eager-collect
consumers to use it. This is the core performance fix — eliminates per-source
`.collect()` across the entire scan pipeline.

**Depends on**: Epic 1

**Scope**:

| # | Task | File | Est. lines |
|---|------|------|-----------|
| 2a | Add `check_corruption()` helper | `crates/storage/src/segmented/mod.rs` | ~10 |
| 2b | Add `build_branch_merge_iter<'b>()` — lazy sources in LSM-003 order | `crates/storage/src/segmented/mod.rs` | ~80 |
| 2c | Refactor `scan_prefix_from_branch` to use `build_branch_merge_iter` | `crates/storage/src/segmented/mod.rs` | ~20 (net reduction) |
| 2d | Refactor `count_prefix_from_branch` to use `build_branch_merge_iter` | `crates/storage/src/segmented/mod.rs` | ~20 (net reduction) |
| 2e | Refactor `scan_prefix_at_timestamp` to use `build_branch_merge_iter` | `crates/storage/src/segmented/mod.rs` | ~30 (net reduction) |
| 2f | Verify: all existing storage tests pass | — | — |
| 2g | Verify: all existing engine/executor tests pass | — | — |

**Acceptance**:
- `cargo test` — full suite passes (semantics preserved)
- The three refactored functions produce byte-identical output to pre-refactor versions
- No new public API — this is an internal refactor

**Invariants verified**: LSM-003 (source ordering), MVCC-001 (version visibility),
MVCC-002 (tombstone semantics), COW-003 (inherited layer version gate)

**Performance impact**: Full prefix scans and counts no longer allocate intermediate
`Vec`s per source. Memory usage drops from `sum(source_sizes) + final_vec` to
`final_vec + K block_buffers`. Throughput improves for large datasets where
source collection was the bottleneck.

---

## Epic 3 — Bounded range scan: `scan_range` + `KVStore::scan()` rewrite

**Goal**: Add `scan_range` with seek + limit pushdown. Rewrite `KVStore::scan()`
to bypass the transaction layer and use `current_version()` for snapshot
isolation. This directly fixes #2183.

**Depends on**: Epic 2

**Scope**:

| # | Task | File | Est. lines |
|---|------|------|-----------|
| 3a | Add `scan_range_from_branch` (merge → MVCC → start_key filter → take limit) | `crates/storage/src/segmented/mod.rs` | ~25 |
| 3b | Add `SegmentedStore::scan_range()` public method | `crates/storage/src/segmented/mod.rs` | ~15 |
| 3c | Add `Database::scan_range()` pass-through | `crates/engine/src/database/mod.rs` | ~10 |
| 3d | Rewrite `KVStore::scan()` — bypass transaction, use `current_version()` | `crates/engine/src/primitives/kv.rs` | ~15 |
| 3e | Unit test: `scan_range` returns exactly `limit` entries | `crates/storage/src/segmented/tests/` | ~30 |
| 3f | Unit test: `scan_range` block imprecision — entries before `start_key` filtered | `crates/storage/src/segmented/tests/` | ~30 |
| 3g | Unit test: `scan_range` with `start` past all keys returns empty | `crates/storage/src/segmented/tests/` | ~15 |
| 3h | Unit test: `scan_range` with `limit=None` returns all from start | `crates/storage/src/segmented/tests/` | ~15 |
| 3i | Unit test: `scan_range` across memtable + segments produces correct merge | `crates/storage/src/segmented/tests/` | ~40 |
| 3j | Integration test: `kv_scan` with limit matches old behavior | `crates/engine/` tests | ~20 |

**Acceptance**:
- `cargo test` — full suite passes
- `kv_scan(Some(mid_key), Some(10))` on 50K records: throughput jumps from ~20 ops/s to thousands
- `cargo bench --bench memory_efficiency` in strata-benchmarks confirms improvement

**Invariants verified**: MVCC-001 (`current_version()` snapshot), ARCH-002
(consistent read boundary), block imprecision correctness

---

## Epic 4 — `Arc<Memtable>` + `BranchSnapshot`

**Goal**: Structural change that enables persistent iterators. Change
`BranchState.active` from `Memtable` to `Arc<Memtable>`. Add `BranchSnapshot`
type and `snapshot_branch()` capture method.

**Depends on**: Epic 2 (refactored consumers work with `Arc<Memtable>` transparently)

**Scope**:

| # | Task | File | Est. lines |
|---|------|------|-----------|
| 4a | Change `BranchState.active: Memtable` → `active: Arc<Memtable>` | `crates/storage/src/segmented/mod.rs` | ~5 |
| 4b | Update `BranchState::new()` — wrap in `Arc::new()` | `crates/storage/src/segmented/mod.rs` | ~2 |
| 4c | Update 3 rotation sites — remove `Arc::new(old)`, old is already Arc | `crates/storage/src/segmented/mod.rs` | ~6 |
| 4d | Add `BranchSnapshot` struct | `crates/storage/src/segmented/mod.rs` | ~10 |
| 4e | Add `SegmentedStore::snapshot_branch()` — capture under short guard | `crates/storage/src/segmented/mod.rs` | ~15 |
| 4f | Add `build_snapshot_merge_iter()` — owned merge for persistent mode | `crates/storage/src/segmented/mod.rs` | ~60 |
| 4g | Unit test: snapshot survives memtable rotation | `crates/storage/src/segmented/tests/` | ~40 |
| 4h | Unit test: snapshot survives compaction (segment Arc pinning) | `crates/storage/src/segmented/tests/` | ~40 |
| 4i | Verify: `cargo test` — full suite passes (Arc change transparent) | — | — |

**Acceptance**:
- `cargo test` — full suite passes
- `BranchSnapshot` holds valid data after concurrent rotation and compaction
- No public API change (snapshot_branch is `pub(crate)`)

**Invariants verified**: LSM-004 (rotation atomicity preserved), COW-001
(Arc pinning prevents segment deletion)

---

## Epic 5 — Persistent `StorageIterator` + `KVStore::scan_iter()`

**Goal**: Public persistent iterator API for cursor-based pagination and
streaming. Completes the RocksDB alignment.

**Depends on**: Epic 4

**Scope**:

| # | Task | File | Est. lines |
|---|------|------|-----------|
| 5a | Add `StorageIterator` struct (snapshot + pipeline + corruption flags) | `crates/storage/src/segmented/mod.rs` | ~30 |
| 5b | Implement `StorageIterator::new()` | `crates/storage/src/segmented/mod.rs` | ~10 |
| 5c | Implement `StorageIterator::seek()` — rebuild pipeline from target | `crates/storage/src/segmented/mod.rs` | ~25 |
| 5d | Implement `StorageIterator::next()` — advance pipeline | `crates/storage/src/segmented/mod.rs` | ~10 |
| 5e | Implement `StorageIterator::valid()` + `check_corruption()` | `crates/storage/src/segmented/mod.rs` | ~15 |
| 5f | Export `StorageIterator` from storage crate | `crates/storage/src/lib.rs` | ~2 |
| 5g | Add `Database::storage_iterator()` pass-through | `crates/engine/src/database/mod.rs` | ~10 |
| 5h | Add `KVStore::scan_iter()` — returns `StorageIterator` | `crates/engine/src/primitives/kv.rs` | ~15 |
| 5i | Unit test: seek + 10 next calls returns correct sequence | `crates/storage/src/segmented/tests/` | ~40 |
| 5j | Unit test: re-seek to different position works | `crates/storage/src/segmented/tests/` | ~30 |
| 5k | Unit test: seek past all keys → valid() returns false | `crates/storage/src/segmented/tests/` | ~15 |
| 5l | Unit test: cursor pagination — 5 pages of 10 entries each | `crates/storage/src/segmented/tests/` | ~40 |
| 5m | Integration test: `scan_iter` matches `kv_scan` results | `crates/engine/` tests | ~25 |

**Acceptance**:
- `cargo test` — full suite passes
- `StorageIterator` supports Seek → N×Next → re-Seek → N×Next pattern
- Cursor pagination over 50K records works correctly
- Iterator remains valid after concurrent writes/rotation (snapshot pinning)

**Invariants verified**: All invariants from Epics 1–4, plus ARCH-002
(atomic publication boundary via snapshot)

---

## Dependency Graph

```
Epic 1 ─── Storage primitives (iter_range, OwnedSegmentIter::new_seek)
  │
  v
Epic 2 ─── Lazy merge builder + consumer refactors
  │
  ├──────────────────────┐
  v                      v
Epic 3                 Epic 4
Bounded scan_range     Arc<Memtable> + BranchSnapshot
(fixes #2183)            │
                         v
                       Epic 5
                       Persistent StorageIterator
```

Epics 3 and 4 are independent of each other and can be worked in parallel.
Epic 5 requires Epic 4.

---

## Estimated Effort

| Epic | New/changed lines | Tests | Risk |
|------|------------------|-------|------|
| 1 | ~65 | ~95 | Low — additive, no existing code changed |
| 2 | ~130 (net reduction ~80) | existing suite | Medium — refactors 3 hot-path functions |
| 3 | ~65 | ~150 | Low — new code path, old path unchanged until KV rewrite |
| 4 | ~140 | ~80 | Medium — structural change to BranchState |
| 5 | ~105 | ~150 | Low — new API, no existing code changed |
| **Total** | **~505** | **~475** | |

---

## Milestone: #2183 Resolved

**Epic 3 completion** resolves the issue. At that point:
- `kv_scan(start, limit=10)` on 50K records: ~20 ops/s → thousands ops/s
- `scan_prefix` / `count_prefix` / `scan_at_timestamp`: no intermediate Vec allocations
- All 50 engine invariants hold

Epics 4–5 are the forward-looking investment for cursor pagination and the
persistent iterator API.
