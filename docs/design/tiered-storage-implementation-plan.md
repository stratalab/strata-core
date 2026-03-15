# Tiered Storage: Implementation Plan & Epics

**Status:** Draft
**Date:** 2026-03-11
**Scope:** Per-branch KV segment store (bounded memory). No branching model changes.
**Methodology:** Test-Driven Development (TDD). Tests are written FIRST. Implementation follows.
**Related:**
- [Implementation Scope Decision](tiered-storage-implementation-scope.md) — what's in/out of scope
- [Impact Analysis](tiered-storage-impact-analysis.md) — crate-by-crate change map
- [Design Review](tiered-storage-review.md) — 20 issues identified during review
- [Architecture Overview](tiered-storage.md) — full design vision
- [File Layout](storage-file-layout.md) — on-disk formats
- [Read/Write Algorithms](read-write-algorithms.md) — concurrency model
- [Memtable & Flush](memtable-flush-file.md) — memtable design, flush pipeline

---

## Overview

This plan implements bounded-memory KV storage for Strata. The current architecture holds all KV data in memory (`ShardedStore` with `DashMap<BranchId, Shard>` where each shard is `FxHashMap<Key, VersionChain>`). This OOMs at scale: ~1.5GB at 1M chunks, ~7.2GB at 5M chunks.

The new architecture introduces a per-branch segment store: memtable (bounded in-memory write buffer) + immutable KV segments (on disk, mmap-able, bloom-filtered). The `Storage` trait is the firewall — everything above it (concurrency, engine, executor) is unchanged.

**Total estimated work: ~7,300 lines of code across 3 crates.**

```
strata-storage:   ~5,800 new lines (10 new modules), ~4,900 removed    ← core work
strata-durability: ~1,000 changed lines (recovery, MANIFEST, WAL truncation)
strata-engine:      ~500 changed lines (background scheduling, config)
strata-concurrency:  ~10 changed lines (storage type in recovery)
```

---

## TDD Methodology

Every epic follows strict Red-Green-Refactor:

1. **RED:** Write failing tests that define the expected behavior. Tests compile but fail (return wrong results, panic, or call unimplemented stubs).
2. **GREEN:** Implement the minimum code to make tests pass. No optimization, no cleverness.
3. **REFACTOR:** Clean up implementation while keeping tests green. Optimize if profiling shows need.

**Rules:**
- No implementation code is written without a corresponding test already in place.
- Tests define the contract. If the test passes, the implementation is correct.
- Integration tests from earlier epics are NEVER deleted — they form the regression suite.
- Every epic ends with `cargo test --workspace` passing. No exceptions.

### Test Categories

| Category | Purpose | Location | When Run |
|----------|---------|----------|----------|
| **Unit tests** | Test individual functions/structs in isolation | `#[cfg(test)] mod tests` inline in source files | Every commit |
| **Contract tests** | Verify Storage/SnapshotView trait contracts | `tests/storage/` | Every commit |
| **Integration tests** | Full-stack: Database → transactions → storage → WAL → recovery | `tests/integration/`, `crates/engine/tests/` | Every commit |
| **Regression tests** | Existing tests run against BOTH ShardedStore and SegmentedStore | `tests/storage/`, parameterized | Every commit |
| **Crash/recovery tests** | Fault injection at every crash boundary | `tests/durability/`, `crates/durability/src/testing/` | Every commit |
| **Property tests** | Randomized invariant verification (proptest) | Inline in source files | Every commit |
| **Stress tests** | Heavy workload, concurrency, memory pressure | `tests/storage/stress.rs` (extended) | `#[ignore]`, CI nightly |
| **Benchmarks** | Performance regression detection | `crates/engine/benches/` (extended) | CI nightly |

---

## Architectural Decision: No Transaction Bypasses — Make Transactions Fast Instead

**Status:** Accepted
**Problem:** Some operations need high throughput (bulk graph ingestion, batch KV writes). Transaction overhead (per-branch mutex, OCC validation, WAL records, per-entry apply) limits throughput to ~1K-10K ops/sec per branch. The temptation is to bypass the transaction layer and write directly to storage, but this breaks ACID, MVCC versioning, and branch isolation.

**Decision:** ALL data mutations go through the transaction layer. No exceptions. No "direct storage write" escape hatch. Instead, we make transactions fast enough that bypasses aren't tempting.

**Current state:** An audit of the codebase confirms zero bypasses exist today. `put_direct` was removed in #1384. All primitives (KV, StateCell, EventLog, JsonStore, VectorStore, GraphStore) buffer writes in `TransactionContext` and commit through `TransactionManager`. Recovery and multi-process refresh replay WAL records, which is legitimate. This discipline must be preserved.

### Why Bypasses Are Dangerous

| Bypass | What breaks |
|--------|------------|
| Write directly to `Storage::put_with_version()` | No WAL → data lost on crash. No version allocation → MVCC broken. No commit lock → concurrent writes corrupt state. |
| Skip OCC validation | Stale reads not detected → non-serializable histories. CAS semantics broken. |
| Skip WAL | Committed data lost on crash. Recovery replays partial state. |
| Skip per-branch lock | Two concurrent commits allocate same version range → phantom versions. TOCTOU in validation. |

### How We Make Transactions Fast Enough

The transaction path has 5 steps. Each has a known optimization:

| Step | Current Cost | Optimization | Result |
|------|-------------|-------------|--------|
| **1. Per-branch mutex** | Serializes all writes to a branch | Batch N operations into 1 transaction → 1 lock acquire for N ops | Amortized to ~0 |
| **2. OCC validation** | O(\|read_set\|) storage lookups | Blind writes already skip this (empty read set). Batch APIs produce blind writes. | Already free for bulk loads |
| **3. Version allocation** | 1 atomic fetch_add per txn | Already ~1ns. Batching reduces from N atomics to 1. | Already negligible |
| **4. WAL write** | 1 record per txn, fsync per policy | Batch → 1 WAL record for N ops. Group commit (Standard mode) batches fsyncs. | 1 fsync per batch |
| **5. Storage apply** | Per-entry `put_with_version()` | Use `apply_batch()` → single memtable insertion pass | Amortized allocator + cache pressure |

**The key insight:** The overhead is per-transaction, not per-operation. A transaction with 10,000 writes pays the same fixed costs (1 lock, 1 validation, 1 version, 1 WAL record) as a transaction with 1 write. The fix is batching, not bypassing.

### Transaction Tiers

Three transaction modes, all going through the same commit path:

| Mode | Read tracking | OCC validation | Lock | WAL | Use case |
|------|--------------|----------------|------|-----|----------|
| **Full** | Yes | Yes | Yes | Yes | Read-modify-write, CAS, JSON patches |
| **Blind write** | No | Skipped (empty read set) | Yes | Yes | `batch_put`, `batch_add_edges`, appends |
| **Bulk load** | No | Skipped | Yes | Optional | Initial import, `--bulk-load` flag |

All three preserve MVCC versioning (every write gets a commit_id) and branch isolation (per-branch state). The difference is in conflict detection and durability guarantees.

**Bulk load mode** is the most aggressive optimization: it can skip WAL for initial imports where the source data is still available for re-import on crash. But it still goes through `TransactionManager.commit()` — it just sets a flag that the WAL step checks:

```rust
// In TransactionManager::commit():
if has_mutations && self.wal.is_some() && !txn.bulk_load_mode() {
    wal.append(payload)?;
}
```

This is NOT a bypass — it's a documented, tested, opt-in durability tradeoff within the transaction system.

### Enforcement

To prevent future bypasses:

1. **`Storage` trait methods that mutate (`put_with_version`, `delete_with_version`, `apply_batch`) are `pub(crate)` in strata-storage.** Only the transaction commit path (in strata-concurrency) can call them. Engine and executor layers cannot.
2. **Integration test: `all_writes_produce_wal_records`** — verify that for any write through any primitive, a corresponding WAL record exists (unless bulk load mode).
3. **Code review rule:** Any PR that adds a direct `storage.put_*()` call outside `transaction.rs::apply_writes()` or `recovery.rs::recover()` is rejected.

---

## Prerequisites

Before starting tiered storage, complete the audit cleanup issues (#1398-#1426, except #1406). These clean up the codebase that will be modified or replaced. Engine crate split (#1406) is deferred AFTER tiered storage.

---

## Epics and Implementation Order

```
Epic 1: Key Encoding & Memtable         ← foundation, no disk I/O
Epic 2: KV Segment Format               ← on-disk format, read-only
Epic 3: Integrated Read/Write Path      ← SegmentedStore implements Storage trait
Epic 4: Flush Pipeline                   ← memtable → segment, background work
Epic 5: Durability Integration           ← recovery, WAL truncation, MANIFEST
Epic 6: Compaction & Memory Pressure     ← long-running background, production-ready
Epic 7: Cutover & Cleanup               ← remove ShardedStore, final validation
Epic 8: Bulk Ingestion Performance       ← batch APIs, graph O(n²) fix, parallel ingest
```

Each epic builds on the previous. Each has a clear "done" gate: all tests (unit + contract + integration + regression) pass.

---

## Epic 1: Key Encoding & Memtable

**Goal:** Build the in-memory write buffer that replaces `FxHashMap<Key, VersionChain>`.

**Done when:** Memtable passes all unit tests for put, get_versioned, prefix scan, freeze, and memory accounting. Property tests confirm encoding invariants. No integration with existing crates yet.

### 1.1 InternalKey Encoding

**File:** `crates/storage/src/key_encoding.rs` (new, ~200 lines)
**Spec:** [storage-file-layout.md §8](storage-file-layout.md), [memtable-flush-file.md §2.2](memtable-flush-file.md)

```
InternalKey = TypedKeyBytes || EncodeDesc(commit_id)
TypedKeyBytes = varint(len(namespace)) | namespace | u32_be(type_tag) | varint(len(user_key)) | user_key
EncodeDesc(commit_id) = u64_be(!commit_id)
```

#### Step 1: Write Tests (RED)

**New dependency:** `proptest` (add to `[dev-dependencies]`)

```rust
// crates/storage/src/key_encoding.rs — #[cfg(test)] mod tests

// --- Property Tests (proptest) ---

#[test]
fn prop_typed_key_bytes_preserves_lexicographic_order() {
    // For any two Keys (k1, k2) where k1 < k2 lexicographically,
    // encode(k1) < encode(k2) byte-wise.
    // This is the FUNDAMENTAL invariant — if this breaks, scans are wrong.
}

#[test]
fn prop_commit_id_descending_within_same_key() {
    // For any Key k and commit_ids c1 > c2:
    // InternalKey(k, c1) < InternalKey(k, c2) byte-wise.
    // Ensures newest version sorts first within a key.
}

#[test]
fn prop_encode_decode_roundtrip() {
    // For any Key k and commit_id c:
    // decode(encode(k, c)) == (k, c)
}

#[test]
fn prop_prefix_extraction_matches_typed_key_portion() {
    // For any InternalKey ik:
    // extract_typed_key_prefix(ik) == encode_typed_key(ik.key)
    // Bloom filters use this — if wrong, bloom gives false negatives.
}

// --- Concrete Edge Case Tests ---

#[test]
fn empty_namespace_empty_user_key() {
    // Key { namespace: "", type_tag: 0, user_key: "" }
    // Must encode/decode correctly. Must not panic.
}

#[test]
fn max_commit_id_sorts_first() {
    // InternalKey(k, u64::MAX) < InternalKey(k, 0) byte-wise
}

#[test]
fn commit_id_zero_sorts_last() {
    // InternalKey(k, 0) is the "oldest possible" version of k
}

#[test]
fn different_type_tags_sort_correctly() {
    // Key with type_tag=1 < Key with type_tag=2
    // (u32_be encoding ensures this)
}

#[test]
fn varint_encoding_handles_large_namespaces() {
    // Namespace with 10,000 byte string — varint length encodes correctly
}

#[test]
fn binary_user_keys_handled() {
    // user_key with embedded nulls, 0xFF bytes, etc.
}
```

#### Step 2: Implement (GREEN)

| # | Task | Details |
|---|------|---------|
| 1.1.1 | `InternalKey` type | Wrapper around `Vec<u8>` implementing `Ord` via byte comparison. |
| 1.1.2 | `encode_typed_key_bytes(key: &Key) -> Vec<u8>` | Namespace + type_tag + user_key encoding. |
| 1.1.3 | `encode_internal_key(key: &Key, commit_id: u64) -> InternalKey` | TypedKeyBytes + EncodeDesc. |
| 1.1.4 | `decode_internal_key(bytes: &[u8]) -> (Key, u64)` | Inverse of encode. |
| 1.1.5 | `extract_typed_key_prefix(internal_key: &[u8]) -> &[u8]` | Return TypedKeyBytes portion (everything before commit_id). |
| 1.1.6 | varint helpers | `encode_varint(u64) -> [u8]`, `decode_varint(&[u8]) -> (u64, usize)`. |

#### Step 3: Refactor

- Optimize allocation: consider `SmallVec` or stack buffer for common key sizes
- Ensure `InternalKey::cmp` is branch-free for hot path

### 1.2 Concurrent Skiplist Memtable

**File:** `crates/storage/src/memtable.rs` (new, ~800 lines)
**Spec:** [memtable-flush-file.md §2](memtable-flush-file.md), [read-write-algorithms.md §2.2](read-write-algorithms.md)
**New dependency:** `crossbeam-skiplist`

#### Step 1: Write Tests (RED)

```rust
// crates/storage/src/memtable.rs — #[cfg(test)] mod tests

// --- Basic CRUD ---

#[test]
fn put_then_get_returns_value() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 1, Value::Int(42), false);
    let result = mt.get_versioned(&key("ns", "k1"), u64::MAX);
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn get_nonexistent_returns_none() {
    let mt = Memtable::new(0);
    assert!(mt.get_versioned(&key("ns", "k1"), u64::MAX).is_none());
}

#[test]
fn put_tombstone_then_get_returns_tombstone() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 1, Value::Null, false);
    mt.put(&key("ns", "k1"), 2, Value::Null, true); // delete
    let result = mt.get_versioned(&key("ns", "k1"), u64::MAX);
    assert!(result.unwrap().is_tombstone);
}

// --- MVCC Versioning ---

#[test]
fn get_versioned_returns_correct_version() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 1, Value::Int(10), false);
    mt.put(&key("ns", "k1"), 2, Value::Int(20), false);
    mt.put(&key("ns", "k1"), 3, Value::Int(30), false);

    // Snapshot at version 2 sees value 20
    assert_eq!(mt.get_versioned(&key("ns", "k1"), 2).unwrap().value, Value::Int(20));
    // Snapshot at version 1 sees value 10
    assert_eq!(mt.get_versioned(&key("ns", "k1"), 1).unwrap().value, Value::Int(10));
    // Snapshot at version 0 sees nothing
    assert!(mt.get_versioned(&key("ns", "k1"), 0).is_none());
}

#[test]
fn get_version_only_returns_latest_commit_id() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 5, Value::Int(50), false);
    mt.put(&key("ns", "k1"), 10, Value::Int(100), false);
    assert_eq!(mt.get_version_only(&key("ns", "k1")), Some(10));
}

#[test]
fn tombstone_at_snapshot_hides_older_versions() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 1, Value::Int(10), false);
    mt.put(&key("ns", "k1"), 2, Value::Null, true); // tombstone
    mt.put(&key("ns", "k1"), 3, Value::Int(30), false);

    // Snapshot at 2 sees tombstone (not value 10)
    assert!(mt.get_versioned(&key("ns", "k1"), 2).unwrap().is_tombstone);
    // Snapshot at 3 sees value 30
    assert_eq!(mt.get_versioned(&key("ns", "k1"), 3).unwrap().value, Value::Int(30));
    // Snapshot at 1 sees value 10
    assert_eq!(mt.get_versioned(&key("ns", "k1"), 1).unwrap().value, Value::Int(10));
}

// --- Prefix Scan ---

#[test]
fn prefix_scan_returns_matching_keys() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "user:1"), 1, Value::Int(1), false);
    mt.put(&key("ns", "user:2"), 1, Value::Int(2), false);
    mt.put(&key("ns", "order:1"), 1, Value::Int(100), false);

    let results: Vec<_> = mt.iter_seek(&key("ns", "user:")).collect();
    assert_eq!(results.len(), 2); // user:1, user:2 only
}

#[test]
fn prefix_scan_respects_mvcc() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 1, Value::Int(10), false);
    mt.put(&key("ns", "k1"), 3, Value::Int(30), false);
    mt.put(&key("ns", "k2"), 2, Value::Int(20), false);

    // Scan at snapshot=2: k1 should return version 1 (value 10), k2 returns version 2
    let results: Vec<_> = mt.iter_seek(&key("ns", "k")).collect();
    // Iterator yields raw entries; MVCC filtering happens at merge_iter level
    assert!(results.len() >= 3); // all entries, including multiple versions
}

#[test]
fn prefix_scan_with_tombstones() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 1, Value::Int(10), false);
    mt.put(&key("ns", "k1"), 2, Value::Null, true); // tombstone
    mt.put(&key("ns", "k2"), 1, Value::Int(20), false);

    // Raw scan yields tombstone entry — merge_iter layer will filter
    let results: Vec<_> = mt.iter_seek(&key("ns", "k")).collect();
    assert!(results.len() >= 3);
}

// --- Freeze ---

#[test]
fn frozen_memtable_is_readable() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "k1"), 1, Value::Int(42), false);
    mt.freeze();
    assert_eq!(mt.get_versioned(&key("ns", "k1"), u64::MAX).unwrap().value, Value::Int(42));
}

#[test]
#[should_panic]
fn frozen_memtable_rejects_writes() {
    let mt = Memtable::new(0);
    mt.freeze();
    mt.put(&key("ns", "k1"), 1, Value::Int(42), false); // should panic
}

#[test]
fn frozen_memtable_sorted_iter_is_ordered() {
    let mt = Memtable::new(0);
    mt.put(&key("ns", "b"), 1, Value::Int(2), false);
    mt.put(&key("ns", "a"), 1, Value::Int(1), false);
    mt.put(&key("ns", "c"), 1, Value::Int(3), false);
    mt.freeze();

    let entries: Vec<_> = mt.sorted_iter().collect();
    // Must be in InternalKey order: a < b < c
    assert!(entries[0].0 < entries[1].0);
    assert!(entries[1].0 < entries[2].0);
}

// --- Memory Accounting ---

#[test]
fn approx_bytes_increases_on_insert() {
    let mt = Memtable::new(0);
    let before = mt.approx_bytes();
    mt.put(&key("ns", "k1"), 1, Value::Bytes(vec![0u8; 1000].into()), false);
    let after = mt.approx_bytes();
    assert!(after > before);
    assert!(after - before >= 1000); // at least value size
}

#[test]
fn entry_count_tracks_insertions() {
    let mt = Memtable::new(0);
    assert_eq!(mt.entry_count(), 0);
    mt.put(&key("ns", "k1"), 1, Value::Int(1), false);
    mt.put(&key("ns", "k1"), 2, Value::Int(2), false); // second version
    assert_eq!(mt.entry_count(), 2); // two entries (not two keys)
}

// --- Concurrency ---

#[test]
fn concurrent_reads_while_writing() {
    let mt = Arc::new(Memtable::new(0));
    // Pre-populate
    for i in 0..100 {
        mt.put(&key("ns", &format!("k{}", i)), 1, Value::Int(i), false);
    }

    let mt_read = Arc::clone(&mt);
    let reader = thread::spawn(move || {
        for _ in 0..10_000 {
            for i in 0..100 {
                let _ = mt_read.get_versioned(&key("ns", &format!("k{}", i)), u64::MAX);
            }
        }
    });

    // Writer adds new versions concurrently
    for v in 2..=100u64 {
        for i in 0..100 {
            mt.put(&key("ns", &format!("k{}", i)), v, Value::Int(v as i64), false);
        }
    }

    reader.join().unwrap(); // must not panic
}

// --- Property Tests ---

#[test]
fn prop_get_versioned_never_returns_future_version() {
    // For any sequence of puts with commit_ids [c1, c2, ..., cn]
    // and any snapshot_commit s:
    // get_versioned(key, s) either returns None or a value with commit_id <= s
}

#[test]
fn prop_get_versioned_returns_newest_visible() {
    // For any sequence of puts and any snapshot s:
    // if get_versioned returns Some(v), there is no other version v' with
    // v.commit_id < v'.commit_id <= s
}
```

#### Step 2: Implement (GREEN)

| # | Task | Details |
|---|------|---------|
| 1.2.1 | `Memtable` struct | `id: u64`, `state: AtomicU8`, `approx_bytes: AtomicU64`, `max_commit: AtomicU64`, `map: SkipMap<InternalKey, ValueRef>` |
| 1.2.2 | `ValueRef` | `kind: u8` (PUT=1, DEL=2), `value: Option<Bytes>`. |
| 1.2.3 | `put()` | Encode InternalKey, insert into skiplist, update accounting. |
| 1.2.4 | `get_versioned()` | Seek to `(key, u64::MAX)`, scan forward, return first with `commit_id <= snapshot`. |
| 1.2.5 | `get_version_only()` | Same seek, return commit_id only. |
| 1.2.6 | `iter_seek()` | Seek to prefix, return iterator. |
| 1.2.7 | `freeze()` / `sorted_iter()` | Set state, provide ordered iteration for flush. |

---

## Epic 2: KV Segment Format

**Goal:** Immutable on-disk segment format. Segments built from sorted iterators, queryable via point lookup and prefix scan.

**Done when:** Round-trip property tests pass (build → read). Bloom filter FPR is within spec. Edge cases (empty segments, large values, multi-version keys) are covered.

### 2.1 Segment Builder & Reader

**Files:**
- `crates/storage/src/segment.rs` (new, ~1,200 lines) — reading
- `crates/storage/src/segment_builder.rs` (new, ~600 lines) — writing

**Spec:** [storage-file-layout.md §5](storage-file-layout.md)
**New dependency:** `crc32fast`

#### Step 1: Write Tests (RED)

```rust
// crates/storage/src/segment.rs — #[cfg(test)] mod tests

// --- Round-Trip Tests ---

#[test]
fn build_and_read_single_entry() {
    let dir = tempdir();
    let entries = vec![(ikey("ns", "k1", 1), vref_put(b"hello"))];
    let seg = build_segment(&dir, entries);

    let result = seg.point_lookup(&key("ns", "k1"), 1);
    assert_eq!(result.unwrap().value, b"hello");
}

#[test]
fn build_and_read_many_entries() {
    let dir = tempdir();
    let entries: Vec<_> = (0..10_000)
        .map(|i| (ikey("ns", &format!("key_{:06}", i), 1), vref_put(format!("val_{}", i).as_bytes())))
        .collect();
    let seg = build_segment(&dir, entries);

    // Verify every entry
    for i in 0..10_000 {
        let result = seg.point_lookup(&key("ns", &format!("key_{:06}", i)), 1);
        assert!(result.is_some(), "Entry {} missing", i);
    }
}

#[test]
fn point_lookup_nonexistent_key_returns_none() {
    let dir = tempdir();
    let entries = vec![(ikey("ns", "k1", 1), vref_put(b"hello"))];
    let seg = build_segment(&dir, entries);

    assert!(seg.point_lookup(&key("ns", "k2"), u64::MAX).is_none());
}

// --- MVCC Tests ---

#[test]
fn point_lookup_respects_snapshot_commit() {
    let dir = tempdir();
    let entries = vec![
        (ikey("ns", "k1", 3), vref_put(b"v3")),  // newest (sorts first)
        (ikey("ns", "k1", 2), vref_put(b"v2")),
        (ikey("ns", "k1", 1), vref_put(b"v1")),  // oldest
    ];
    let seg = build_segment(&dir, entries);

    assert_eq!(seg.point_lookup(&key("ns", "k1"), 3).unwrap().value, b"v3");
    assert_eq!(seg.point_lookup(&key("ns", "k1"), 2).unwrap().value, b"v2");
    assert_eq!(seg.point_lookup(&key("ns", "k1"), 1).unwrap().value, b"v1");
    assert!(seg.point_lookup(&key("ns", "k1"), 0).is_none()); // before first
}

#[test]
fn tombstone_in_segment() {
    let dir = tempdir();
    let entries = vec![
        (ikey("ns", "k1", 2), vref_del()),      // tombstone
        (ikey("ns", "k1", 1), vref_put(b"v1")), // older value
    ];
    let seg = build_segment(&dir, entries);

    let result = seg.point_lookup(&key("ns", "k1"), 2);
    assert!(result.unwrap().is_tombstone);
    assert_eq!(seg.point_lookup(&key("ns", "k1"), 1).unwrap().value, b"v1");
}

// --- Bloom Filter Tests ---

#[test]
fn bloom_filter_no_false_negatives() {
    let dir = tempdir();
    let entries: Vec<_> = (0..1000)
        .map(|i| (ikey("ns", &format!("k{}", i), 1), vref_put(b"v")))
        .collect();
    let seg = build_segment(&dir, entries);

    // Every key that EXISTS must pass bloom check
    for i in 0..1000 {
        assert!(seg.bloom_maybe_contains(&key("ns", &format!("k{}", i))),
            "Bloom false negative for key k{}", i);
    }
}

#[test]
fn bloom_filter_false_positive_rate_within_spec() {
    let dir = tempdir();
    let entries: Vec<_> = (0..10_000)
        .map(|i| (ikey("ns", &format!("k{}", i), 1), vref_put(b"v")))
        .collect();
    let seg = build_segment(&dir, entries);

    // Test 10,000 non-existent keys
    let mut false_positives = 0;
    for i in 10_000..20_000 {
        if seg.bloom_maybe_contains(&key("ns", &format!("k{}", i))) {
            false_positives += 1;
        }
    }

    let fpr = false_positives as f64 / 10_000.0;
    assert!(fpr < 0.02, "Bloom FPR {} exceeds 2% target (10 bits/key)", fpr);
}

// --- Prefix Scan / Iterator Tests ---

#[test]
fn iter_seek_returns_entries_in_order() {
    let dir = tempdir();
    let entries = vec![
        (ikey("ns", "a", 1), vref_put(b"1")),
        (ikey("ns", "b", 1), vref_put(b"2")),
        (ikey("ns", "c", 1), vref_put(b"3")),
    ];
    let seg = build_segment(&dir, entries);

    let results: Vec<_> = seg.iter_seek(&key("ns", "a")).collect();
    assert_eq!(results.len(), 3);
    // Must be in InternalKey order
    assert!(results[0].0 < results[1].0);
    assert!(results[1].0 < results[2].0);
}

#[test]
fn iter_seek_with_prefix_filters() {
    let dir = tempdir();
    let entries = vec![
        (ikey("ns", "user:1", 1), vref_put(b"1")),
        (ikey("ns", "user:2", 1), vref_put(b"2")),
        (ikey("ns", "order:1", 1), vref_put(b"3")),
    ];
    let seg = build_segment(&dir, entries);

    let results: Vec<_> = seg.iter_seek(&key("ns", "user:"))
        .take_while(|(ik, _)| ik.starts_with_prefix(&encode_typed_key(&key("ns", "user:"))))
        .collect();
    assert_eq!(results.len(), 2);
}

// --- Edge Cases ---

#[test]
fn empty_segment() {
    let dir = tempdir();
    let seg = build_segment(&dir, vec![]);
    assert!(seg.point_lookup(&key("ns", "k1"), u64::MAX).is_none());
    assert_eq!(seg.iter_seek(&key("ns", "")).count(), 0);
}

#[test]
fn single_entry_segment() {
    let dir = tempdir();
    let entries = vec![(ikey("ns", "k1", 1), vref_put(b"v"))];
    let seg = build_segment(&dir, entries);
    assert!(seg.point_lookup(&key("ns", "k1"), 1).is_some());
}

#[test]
fn large_value_segment() {
    // 1MB value in a single entry
    let dir = tempdir();
    let big = vec![0xABu8; 1_000_000];
    let entries = vec![(ikey("ns", "big", 1), vref_put(&big))];
    let seg = build_segment(&dir, entries);

    let result = seg.point_lookup(&key("ns", "big"), 1).unwrap();
    assert_eq!(result.value.len(), 1_000_000);
}

#[test]
fn segment_with_100k_entries() {
    // Ensure multi-block segments work correctly
    let dir = tempdir();
    let entries: Vec<_> = (0..100_000)
        .map(|i| (ikey("ns", &format!("k{:08}", i), 1), vref_put(format!("v{}", i).as_bytes())))
        .collect();
    let seg = build_segment(&dir, entries);

    // Spot-check
    assert!(seg.point_lookup(&key("ns", "k00000000"), 1).is_some());
    assert!(seg.point_lookup(&key("ns", "k00050000"), 1).is_some());
    assert!(seg.point_lookup(&key("ns", "k00099999"), 1).is_some());
    assert!(seg.point_lookup(&key("ns", "k00100000"), u64::MAX).is_none());
}

// --- Corruption Detection ---

#[test]
fn corrupted_block_checksum_detected() {
    let dir = tempdir();
    let entries = vec![(ikey("ns", "k1", 1), vref_put(b"v"))];
    let seg_path = build_segment_file(&dir, entries);

    // Corrupt a byte in the middle of the file
    corrupt_file_at_offset(&seg_path, 100);

    // Opening or reading should detect corruption
    let result = KVSegment::open(&seg_path);
    // Either open fails or point_lookup fails with CRC error
    // (depends on which block was corrupted)
}

#[test]
fn truncated_segment_detected() {
    let dir = tempdir();
    let entries: Vec<_> = (0..1000)
        .map(|i| (ikey("ns", &format!("k{}", i), 1), vref_put(b"v")))
        .collect();
    let seg_path = build_segment_file(&dir, entries);

    // Truncate the file (removes footer)
    truncate_file(&seg_path, file_size(&seg_path) / 2);

    let result = KVSegment::open(&seg_path);
    assert!(result.is_err(), "Truncated segment should fail to open");
}

// --- Property Tests ---

#[test]
fn prop_round_trip_any_entries() {
    // For any sorted Vec<(InternalKey, ValueRef)>:
    // build_segment(entries) followed by point_lookup for each entry
    // returns the correct value
}

#[test]
fn prop_point_lookup_consistent_with_iter() {
    // For any segment and any key k:
    // point_lookup(k, c) == iter_seek(k).find(|e| e.key == k && e.commit <= c)
}
```

#### Step 2: Implement (GREEN)

| # | Task | Details |
|---|------|---------|
| 2.1.1 | Block framing constants | Magic, block types, CRC. |
| 2.1.2 | `BlockBuilder` | Prefix compression, restart points, CRC. |
| 2.1.3 | `BlockReader` | Binary search within block, prefix decompression. |
| 2.1.4 | Data block entry encoding | Per [storage-file-layout.md §5.3](storage-file-layout.md). |
| 2.1.5 | `KVHeader` / `Footer` | Read/write. Magic validation. |
| 2.1.6 | `SegmentBuilder` | Sorted iterator → data blocks → index → bloom → props → footer. Atomic write (`.partial` → rename). |
| 2.1.7 | `KVSegment` (reader) | mmap open, footer parse, `point_lookup()`, `iter_seek()`. |
| 2.1.8 | Bloom filter | Build and query. Standard bloom, 10 bits/key. |
| 2.1.9 | `IndexBlock` | Binary search for block location. |
| 2.1.10 | `PropertiesBlock` | min/max key, counts, commit range. |

### 2.2 Block Cache

**File:** `crates/storage/src/block_cache.rs` (new, ~400 lines)
**Spec:** [read-write-algorithms.md §2.4](read-write-algorithms.md)

#### Step 1: Write Tests (RED)

```rust
#[test]
fn cache_miss_calls_loader() { /* loader called, value returned and cached */ }

#[test]
fn cache_hit_does_not_call_loader() { /* second get returns cached, loader not called */ }

#[test]
fn eviction_respects_capacity() {
    // Insert 10MB of blocks into 5MB cache → oldest evicted
    // Verify total cached bytes <= capacity
}

#[test]
fn evict_segment_removes_all_entries() {
    // Insert blocks from segments A and B
    // evict_segment(A) → A's blocks gone, B's blocks remain
}

#[test]
fn concurrent_access_no_deadlock() {
    // 8 threads reading/inserting simultaneously, 10K ops each
    // Must complete without deadlock
}

#[test]
fn stats_track_hits_and_misses() {
    // Insert, hit, miss → verify counters
}
```

#### Step 2: Implement (GREEN)

| # | Task |
|---|------|
| 2.2.1 | Cache key: `(seg_id: u128, block_offset: u64)` |
| 2.2.2 | Sharded LRU (16 shards, per-shard `Mutex<LruMap>`) |
| 2.2.3 | `get_or_load()`, `insert()`, `evict_segment()` |
| 2.2.4 | Memory accounting and eviction |

---

## Epic 3: Integrated Read/Write Path

**Goal:** `SegmentedStore` implements `Storage` trait. All existing storage contract tests pass against BOTH `ShardedStore` and `SegmentedStore`.

**Done when:** The existing test suites in `tests/storage/` (mvcc_invariants, branch_isolation, snapshot_isolation, stress) pass against SegmentedStore with ZERO modifications to test logic. Only the store construction changes.

### 3.0 Regression Test Infrastructure (FIRST — before any implementation)

This is the most critical testing step. We parameterize the EXISTING test suites to run against both backends.

#### Step 1: Create Backend-Parameterized Test Harness

**File:** `tests/storage/backend.rs` (new)

```rust
//! Storage backend test parameterization.
//!
//! Every storage contract test runs against BOTH ShardedStore (reference)
//! and SegmentedStore (new). If a test passes for ShardedStore but fails
//! for SegmentedStore, that is a regression.

pub enum StorageBackend {
    Sharded,
    Segmented,
}

pub fn create_store(backend: StorageBackend) -> Box<dyn Storage> {
    match backend {
        StorageBackend::Sharded => Box::new(ShardedStore::new()),
        StorageBackend::Segmented => {
            let dir = tempdir();
            Box::new(SegmentedStore::open(dir.path(), StorageConfig::default()).unwrap())
        }
    }
}

/// Macro to duplicate a test for both backends
macro_rules! test_both_backends {
    ($name:ident, $body:expr) => {
        paste::paste! {
            #[test]
            fn [<$name _sharded>]() { $body(StorageBackend::Sharded); }
            #[test]
            fn [<$name _segmented>]() { $body(StorageBackend::Segmented); }
        }
    };
}
```

#### Step 2: Parameterize Existing Tests

Convert every test in `tests/storage/mvcc_invariants.rs`, `branch_isolation.rs`, `snapshot_isolation.rs`, and `stress.rs` to use `test_both_backends!`. The test body is identical — only store construction differs.

**Example conversion:**

```rust
// BEFORE (tests/storage/mvcc_invariants.rs):
#[test]
fn version_chain_stores_newest_first() {
    let store = ShardedStore::new();
    // ... test body ...
}

// AFTER:
test_both_backends!(version_chain_stores_newest_first, |backend| {
    let store = create_store(backend);
    // ... identical test body ...
});
```

This creates the regression suite. Every test runs twice. ShardedStore tests pass immediately (they already do). SegmentedStore tests fail (RED). As we implement Epic 3, they turn GREEN one by one.

**Tests to parameterize (from existing test files):**

| File | Tests | Count |
|------|-------|-------|
| `mvcc_invariants.rs` | version_chain_stores_newest_first, get_at_version_returns_value_lte_version, get_at_version_before_first_returns_none, version_chain_preserves_all_versions, expired_values_filtered_at_read_time, non_expired_values_returned, no_ttl_never_expires, tombstone_preserves_snapshot_isolation, tombstone_not_returned_to_user, delete_nonexistent_key_succeeds, version_counter_monotonically_increases, concurrent_increments_are_unique, history_pagination_works, history_of_nonexistent_key_is_empty | 14 |
| `branch_isolation.rs` | different_branches_have_separate_namespaces, clear_branch_only_affects_target_branch, delete_in_one_branch_doesnt_affect_other, concurrent_writes_to_different_branches, concurrent_reads_and_writes_different_branches, branch_ids_lists_all_active_branches, branch_entry_count, list_branch_keys, get_from_nonexistent_branch_returns_none, clear_nonexistent_branch_succeeds, branch_entry_count_for_empty_branch | 11 |
| `snapshot_isolation.rs` | snapshot_captures_current_version, snapshot_acquisition_is_fast, multiple_snapshots_independent, snapshot_ignores_concurrent_writes, snapshot_sees_pre_delete_value, repeated_reads_return_same_value, multi_key_consistency_within_snapshot, concurrent_readers_dont_block, snapshot_survives_store_modifications, snapshot_cache_provides_isolation, snapshot_scan_sees_consistent_state, snapshot_list_sees_all_keys | 12 |
| `stress.rs` | (5 stress tests, `#[ignore]`) | 5 |
| **Total** | | **42 tests × 2 backends = 84 test executions** |

### 3.1 Merge Iterator

**File:** `crates/storage/src/merge_iter.rs` (new, ~300 lines)
**Spec:** [read-write-algorithms.md §4.3](read-write-algorithms.md)

#### Step 1: Write Tests (RED)

```rust
// --- Merge Correctness ---

#[test]
fn merge_two_non_overlapping() {
    // Stream A: [a@1, b@1], Stream B: [c@1, d@1]
    // Merged: [a@1, b@1, c@1, d@1]
}

#[test]
fn merge_overlapping_keys_newest_first() {
    // Stream A (newer segment): [k1@3], Stream B (older segment): [k1@1]
    // Merged: [k1@3, k1@1] — newer version first
}

#[test]
fn merge_three_streams() {
    // Memtable + 2 segments, keys interleaved
    // Verify correct total ordering
}

#[test]
fn merge_empty_stream() {
    // One empty, one non-empty → non-empty results
}

#[test]
fn merge_all_empty() {
    // All empty → empty result
}

// --- MVCC-Aware Dedup ---

#[test]
fn mvcc_dedup_returns_newest_visible_per_key() {
    // k1@3=30, k1@2=20, k1@1=10 across sources
    // at snapshot=2: returns k1=20
    // at snapshot=3: returns k1=30
}

#[test]
fn mvcc_dedup_tombstone_suppresses_key() {
    // k1@2=TOMBSTONE, k1@1=10
    // at snapshot=2: k1 is absent from results (tombstone wins)
    // at snapshot=1: k1=10
}

#[test]
fn mvcc_dedup_tombstone_then_rewrite() {
    // k1@3=30, k1@2=TOMBSTONE, k1@1=10
    // at snapshot=3: k1=30
    // at snapshot=2: k1 absent
    // at snapshot=1: k1=10
}

// --- Property Tests ---

#[test]
fn prop_merge_output_is_sorted() {
    // For any N sorted input streams, merged output is sorted
}

#[test]
fn prop_merge_contains_all_entries() {
    // Merged output count == sum of input counts
}

#[test]
fn prop_mvcc_dedup_never_returns_future_version() {
    // For any snapshot s, all returned entries have commit_id <= s
}
```

#### Step 2: Implement (GREEN)

| # | Task |
|---|------|
| 3.1.1 | `SortedStream` trait (next/peek) |
| 3.1.2 | `MergeIterator` (min-heap over N streams) |
| 3.1.3 | `MvccIterator` wrapper (dedup by TypedKey, snapshot filter, tombstone suppress) |
| 3.1.4 | `MemtableIterator` adapter |
| 3.1.5 | `SegmentIterator` adapter |

### 3.2 BranchStore and SegmentedStore

**Files:**
- `crates/storage/src/branch_store.rs` (new, ~500 lines)
- `crates/storage/src/segmented_store.rs` (new, ~800 lines)

#### Step 1: Write Tests (RED)

The regression suite from 3.0 is the primary test. Additionally:

```rust
// --- SegmentedStore-specific tests ---

#[test]
fn segmented_store_implements_storage_trait() {
    // Compile-time check: SegmentedStore: Storage
    let _: Box<dyn Storage> = Box::new(SegmentedStore::open(dir, config).unwrap());
}

#[test]
fn data_survives_across_memtable_and_segments() {
    // Write entries → manually flush memtable to segment → verify reads
    // still work (data now in segment, not memtable)
    let store = SegmentedStore::open(dir, config).unwrap();
    store.put_with_version(k1, v1, 1, None).unwrap();
    store.flush_branch(&branch_id); // manual flush
    assert_eq!(store.get(&k1).unwrap().unwrap().value, v1);
}

#[test]
fn memtable_data_preferred_over_segment_data() {
    // Put v1 → flush → put v2 (in memtable)
    // get() returns v2 (from memtable), not v1 (from segment)
}

#[test]
fn fork_copies_both_memtable_and_segment_data() {
    // Write to branch A, flush some, keep some in memtable
    // Fork A → B
    // B should see ALL data (from A's segments + memtable)
}

#[test]
fn snapshot_pins_memtable_and_segment_state() {
    // Take snapshot → flush → write new data
    // Snapshot sees data as it was at snapshot time
}

#[test]
fn get_value_direct_works() {
    // ShardedStore-compatible method used by KV primitive
}

#[test]
fn get_history_works_across_memtable_and_segments() {
    // Versions split between memtable and segments
    // get_history returns all versions, newest first
}

#[test]
fn create_snapshot_returns_functional_snapshot_view() {
    // snapshot = store.create_snapshot(branch, version)
    // snapshot.get(key) works
    // snapshot.scan_prefix(prefix) works
}

#[test]
fn branch_ids_lists_all_branches() {
    // Create 3 branches → branch_ids() returns all 3
}
```

#### Step 2: Implement (GREEN)

| # | Task |
|---|------|
| 3.2.1 | `BranchStore` struct (ArcSwap memtable, ArcSwap segments) |
| 3.2.2 | Point lookup path (memtable → immutable memtables → segments) |
| 3.2.3 | Prefix scan via merge iterator |
| 3.2.4 | `SegmentedStore` implementing all 12 `Storage` trait methods |
| 3.2.5 | `SegmentedSnapshot` implementing `SnapshotView` |
| 3.2.6 | Branch operations (create, fork, clear, list) |
| 3.2.7 | ShardedStore-compatible convenience methods |

### 3.3 Epic 3 Gate: Regression Suite Passes

**All 42 parameterized tests pass for BOTH backends.** This is the quality gate. No proceeding to Epic 4 until every test in the regression suite is GREEN for `StorageBackend::Segmented`.

---

## Epic 4: Flush Pipeline

**Goal:** Background flush from frozen memtable to KV segment. Memory is now bounded.

**Done when:** Memtable rotation and flush work correctly under load. Data is readable from segments after flush. Write stalling works when flush falls behind.

### 4.1 Flush Coordinator

**File:** `crates/storage/src/flush.rs` (new, ~400 lines)
**Spec:** [memtable-flush-file.md §4-6](memtable-flush-file.md) (simplified: no flush file)

#### Step 1: Write Tests (RED)

```rust
// --- Basic Flush ---

#[test]
fn flush_moves_data_from_memtable_to_segment() {
    let store = SegmentedStore::open(dir, config_small_memtable()).unwrap();
    // Write entries
    for i in 0..100 {
        store.put_with_version(key(i), Value::Int(i), i as u64 + 1, None).unwrap();
    }
    // Manually flush
    store.flush_branch(&branch_id);
    // Memtable should be empty (or have a new active one)
    assert!(store.branch_memtable_bytes(&branch_id) < 100);
    // Data should still be readable
    for i in 0..100 {
        assert!(store.get(&key(i)).unwrap().is_some());
    }
}

#[test]
fn flush_produces_valid_segment_file() {
    // After flush, a .sst file exists in segments/kv/
    // File has valid header and footer
}

#[test]
fn flush_updates_segment_inventory() {
    // After flush, segment inventory file lists the new segment
}

// --- Rotation Trigger ---

#[test]
fn memtable_rotates_when_size_exceeded() {
    let config = config_with_memtable_size(1024); // tiny: 1KB
    let store = SegmentedStore::open(dir, config).unwrap();
    // Write enough to exceed 1KB
    for i in 0..100 {
        store.put_with_version(key(i), Value::Bytes(vec![0u8; 100].into()), i as u64 + 1, None).unwrap();
    }
    // Should have rotated at least once
    assert!(store.branch_immutable_memtable_count(&branch_id) >= 1
        || store.branch_segment_count(&branch_id) >= 1);
}

// --- MVCC Correctness Across Flush ---

#[test]
fn mvcc_correct_with_data_split_across_memtable_and_segment() {
    // Write version 1 → flush → write version 2
    // get(snapshot=1) → from segment
    // get(snapshot=2) → from memtable
    // get(latest) → from memtable
}

#[test]
fn snapshot_taken_before_flush_still_works_after_flush() {
    // Take snapshot → flush → read from snapshot
    // Snapshot was holding Arc to the memtable, which is now dropped
    // But Arc keeps it alive — reads still work
}

#[test]
fn prefix_scan_correct_after_flush() {
    // Write keys with prefix "user:" → flush half → write more
    // scan_prefix("user:") returns all keys, from both segment and memtable
}

// --- Write Stalling ---

#[test]
fn writes_stall_when_max_immutable_memtables_reached() {
    // Set max_immutable_memtables = 1, disable background flush
    // Trigger rotation (fills memtable)
    // Trigger second rotation → should stall or return back-pressure error
}

// --- Concurrent Reads During Flush ---

#[test]
fn reads_never_block_during_flush() {
    // Writer thread fills memtable
    // Flusher thread flushes
    // Reader thread reads continuously
    // Reader must never block or see stale data
}

#[test]
fn concurrent_flush_and_write_correctness() {
    // Spawn: writer filling memtable, flusher flushing
    // After both finish: all data must be readable
    // No entries lost, no duplicates
}

// --- Multiple Flushes ---

#[test]
fn multiple_sequential_flushes_produce_multiple_segments() {
    // Fill memtable → flush → fill again → flush → fill again → flush
    // Should have 3 segments
    // All data from all 3 flushes readable
}

#[test]
fn segment_ordering_newest_first() {
    // After multiple flushes, segments are ordered newest-first
    // Point lookup checks newest segment first
}

// --- Crash Safety ---

#[test]
fn crash_during_flush_no_data_loss() {
    // Write data → start flush → kill mid-flush
    // (.partial file exists, no .sst yet)
    // Reopen → .partial cleaned up → WAL replayed → data recovered
}

#[test]
fn crash_after_segment_written_before_inventory_update() {
    // .sst exists but inventory not updated
    // Reopen → segment discovered and adopted → data correct
}
```

### 4.2 Background Flush Scheduling

**File:** `crates/engine/src/background.rs` (modify, ~200 lines)

#### Step 1: Write Tests (RED)

```rust
#[test]
fn background_flush_triggers_when_memtable_full() {
    // Create database with small memtable
    // Write enough to fill it
    // Wait for background flush
    // Verify segment created and memtable size reduced
}

#[test]
fn inactivity_flush_frees_dormant_branch_memory() {
    // Write to branch A, wait for inactivity timeout
    // Memtable should be flushed even though not full
}

#[test]
fn background_flush_does_not_block_foreground_writes() {
    // Measure write latency while flush is running
    // p99 should not spike more than 2x vs no-flush baseline
}
```

---

## Epic 5: Durability Integration

**Goal:** Recovery discovers segments on disk. WAL replay is delta-only. MANIFEST tracks flush watermark. WAL truncation works.

**Done when:** Full Database lifecycle works: open → write → flush → close → reopen → data correct. Crash at every boundary recovers correctly.

### 5.1 MANIFEST Changes

**File:** `crates/durability/src/format/manifest.rs` (modify, ~100 lines)

#### Step 1: Write Tests (RED)

```rust
#[test]
fn manifest_with_flush_watermark_round_trip() {
    let m = Manifest { flushed_through_commit_id: Some(42), ..defaults() };
    let bytes = m.serialize();
    let m2 = Manifest::deserialize(&bytes).unwrap();
    assert_eq!(m2.flushed_through_commit_id, Some(42));
}

#[test]
fn manifest_with_segment_inventory_round_trip() {
    let m = Manifest {
        segment_inventory: vec![
            SegmentMeta { seg_id: 1, commit_min: 1, commit_max: 100, .. },
        ],
        ..defaults()
    };
    let bytes = m.serialize();
    let m2 = Manifest::deserialize(&bytes).unwrap();
    assert_eq!(m2.segment_inventory.len(), 1);
}

#[test]
fn old_manifest_without_new_fields_loads_with_defaults() {
    // Serialize a manifest WITHOUT the new fields (old format)
    // Deserialize → flushed_through = None, segment_inventory = empty
}

#[test]
fn advance_flush_watermark_writes_new_manifest() {
    let mut mgr = ManifestManager::new(dir);
    mgr.advance_flush_watermark(50);
    let reloaded = ManifestManager::load(dir).unwrap();
    assert_eq!(reloaded.manifest().flushed_through_commit_id, Some(50));
}
```

### 5.2 Recovery Coordinator

**File:** `crates/durability/src/recovery/coordinator.rs` (modify, ~500 lines)

#### Step 1: Write Tests (RED)

```rust
// --- Segment Discovery ---

#[test]
fn recovery_discovers_segments_on_disk() {
    // Create database, write, flush, close
    // Recovery should find the segment files
}

#[test]
fn recovery_validates_segment_integrity() {
    // Create segment, corrupt its header CRC
    // Recovery should reject the corrupted segment
}

// --- WAL Delta Replay ---

#[test]
fn recovery_replays_only_wal_delta() {
    // Write 1000 entries, flush (commits 1-500 in segment)
    // Write 500 more (commits 501-1000 in WAL only)
    // Close and reopen
    // Recovery should replay only 500 entries, not 1000
    // Verify: all 1000 entries accessible
}

#[test]
fn recovery_with_no_segments_replays_full_wal() {
    // Fresh database, no segments
    // Recovery = full WAL replay (backward compatible)
}

// --- Crash Boundary Tests ---

#[test]
fn crash_before_any_flush_recovers_from_wal() {
    // Write data, never flush, crash
    // Reopen → full WAL replay → all data present
}

#[test]
fn crash_after_segment_write_before_manifest_update() {
    // Segment file exists but MANIFEST doesn't reference it
    // Recovery should detect orphan and adopt it
    // Data in orphaned segment should be accessible
}

#[test]
fn crash_during_segment_write_partial_file() {
    // .partial file exists in tmp/
    // Recovery should delete it and re-derive from WAL
}

#[test]
fn crash_after_manifest_update_before_memtable_drop() {
    // Segment published in MANIFEST, but frozen memtable still exists
    // Recovery: segment is authoritative, memtable data is redundant
    // No duplicate entries
}

// --- Multi-Branch Recovery ---

#[test]
fn recovery_with_different_flush_states_per_branch() {
    // Branch A: flushed through commit 100
    // Branch B: never flushed
    // Recovery: A loads segments + small WAL delta, B replays full WAL
}

#[test]
fn recovery_preserves_branch_isolation() {
    // After recovery, branch A's data is NOT visible in branch B and vice versa
}

// --- Integration with Full Database ---

#[test]
fn database_reopen_preserves_all_data() {
    // Full lifecycle:
    // 1. Open database
    // 2. Write 1000 KV entries across 3 branches
    // 3. Flush some branches
    // 4. Write 500 more entries
    // 5. Close
    // 6. Reopen
    // 7. Verify ALL 1500 entries present, branch isolation intact
}

#[test]
fn database_reopen_multiple_times() {
    // Open → write → flush → close → reopen → write → flush → close → reopen
    // All data preserved across both cycles
}
```

### 5.3 WAL Truncation

**File:** `crates/durability/src/compaction/wal_only.rs` (modify, ~100 lines)

#### Step 1: Write Tests (RED)

```rust
#[test]
fn wal_truncation_uses_flush_watermark() {
    // Write 1000 entries → flush (flushed_through=500)
    // Run WAL compaction with flush watermark
    // WAL segments with max_txn_id < 500 should be deleted
    // WAL segments with max_txn_id >= 500 should survive
}

#[test]
fn wal_truncation_conservative_with_both_watermarks() {
    // flushed_through=500, snapshot_watermark=300
    // Safe point = min(500, 300) = 300
    // Only segments with max_txn_id < 300 deleted
}

#[test]
fn wal_truncation_never_deletes_active_segment() {
    // Even if all data is flushed, the currently active WAL segment is never deleted
}

#[test]
fn recovery_after_wal_truncation() {
    // Write → flush → truncate WAL → close → reopen
    // Data from segments + remaining WAL entries all present
    // Truncated WAL entries NOT needed (they're in segments)
}
```

### 5.4 Engine Integration

**Files:** `crates/engine/src/database/mod.rs`, `config.rs`, `coordinator.rs`, `recovery/*.rs`

#### Step 1: Write Tests (RED)

These use the existing full-stack integration test infrastructure (`TestDb`).

```rust
// --- TestDb with SegmentedStore ---

// Extend TestDb to support segmented backend:
// TestDb::new_segmented() — creates database with SegmentedStore

#[test]
fn all_primitives_work_with_segmented_store() {
    let db = TestDb::new_segmented();
    // KV
    db.kv().put(branch, "k1", Value::Int(1)).unwrap();
    assert_eq!(db.kv().get(branch, "k1").unwrap().value, Value::Int(1));
    // JSON
    db.json().set(branch, "doc", json!({"a": 1})).unwrap();
    // Event
    db.event().append(branch, "stream", Value::Int(1)).unwrap();
    // State
    db.state().init(branch, "cell", Value::Int(0)).unwrap();
}

#[test]
fn transactions_work_with_segmented_store() {
    let db = TestDb::new_segmented();
    db.db.transaction(branch, |txn| {
        txn.put("k1", Value::Int(1))?;
        txn.put("k2", Value::Int(2))?;
        Ok(())
    }).unwrap();
    assert_eq!(db.kv().get(branch, "k1").unwrap().value, Value::Int(1));
}

#[test]
fn occ_conflict_detection_works_with_segmented_store() {
    // Two transactions reading same key, first commits, second should conflict
}

#[test]
fn blind_writes_skip_occ_with_segmented_store() {
    // Two blind-write transactions to same key → both succeed (no conflict)
}

#[test]
fn recovery_after_crash_with_segmented_store() {
    let db = TestDb::new_segmented_strict();
    db.kv().put(branch, "k1", Value::Int(1)).unwrap();
    // Drop and reopen
    let db2 = db.reopen();
    assert_eq!(db2.kv().get(branch, "k1").unwrap().value, Value::Int(1));
}
```

**Existing engine integration tests to re-run with segmented backend:**

Run the full existing test suite in `crates/engine/tests/` and `tests/engine/` with the segmented backend. These are NOT parameterized (too complex) but are run separately:

```bash
# Feature flag approach
STRATA_STORAGE=segmented cargo test --workspace
```

| Test File | Count | Purpose |
|-----------|-------|---------|
| `crates/engine/tests/database_transaction_tests.rs` | ~20 | Transaction API |
| `crates/engine/tests/recovery_tests.rs` | ~10 | Recovery across all primitives |
| `crates/engine/tests/branch_isolation_tests.rs` | ~10 | Branch isolation via transactions |
| `crates/engine/tests/adversarial_tests.rs` | ~10 | Edge cases, rapid restarts |
| `crates/engine/tests/concurrency_tests.rs` | ~10 | OCC, blind writes under contention |
| `tests/engine/acid_properties.rs` | ~8 | ACID guarantees |
| `tests/engine/primitives/*.rs` | ~30 | All 6 primitives |
| `tests/concurrency/*.rs` | ~20 | OCC, conflicts, snapshot isolation |
| `tests/durability/*.rs` | ~15 | Crash recovery, WAL lifecycle |
| **Total** | **~133** | **Full-stack regression** |

---

## Epic 6: Compaction & Memory Pressure

**Goal:** Segment compaction reclaims disk. Memory pressure system prevents OOM. System is production-ready.

**Done when:** Under sustained ingestion (1M+ keys), memory stays within budget, segment count stays manageable, disk space is reclaimed. Crash during compaction recovers correctly.

### 6.1 Per-Branch Compaction

**File:** `crates/storage/src/compaction.rs` (new, ~600 lines)
**Spec:** [tiered-storage.md §11](tiered-storage.md) (simplified per [implementation-scope.md](tiered-storage-implementation-scope.md))

#### Step 1: Write Tests (RED)

```rust
// --- Basic Compaction ---

#[test]
fn compact_two_segments_into_one() {
    // Create segment A (keys 1-100), segment B (keys 50-150)
    // Compact → single segment with keys 1-150
    // All entries accessible from merged segment
}

#[test]
fn compact_preserves_all_live_versions() {
    // Segment A: k1@3=30, k2@2=20
    // Segment B: k1@1=10, k3@1=100
    // prune_floor = 0 (keep everything)
    // Result: all 4 entries present
}

#[test]
fn compact_prunes_old_versions() {
    // k1@3=30, k1@2=20, k1@1=10
    // prune_floor = 2
    // Result: k1@3=30, k1@2=20 (version 1 pruned)
}

#[test]
fn compact_removes_fully_tombstoned_keys() {
    // k1@2=TOMBSTONE, k1@1=10
    // prune_floor = 2
    // Result: k1 completely removed
}

#[test]
fn compact_keeps_tombstone_above_prune_floor() {
    // k1@2=TOMBSTONE, k1@1=10
    // prune_floor = 1
    // Result: k1@2=TOMBSTONE (tombstone above floor, must keep)
}

// --- Atomic Swap ---

#[test]
fn readers_see_old_segments_during_compaction() {
    // Take snapshot → start compaction → read from snapshot
    // Snapshot sees old segments (held via Arc)
}

#[test]
fn readers_see_new_segment_after_compaction() {
    // After compaction publishes, new reads use merged segment
}

#[test]
fn old_segment_files_deleted_after_compaction() {
    // After compaction and all readers release old view,
    // old .sst files are deleted from disk
}

// --- Crash Safety ---

#[test]
fn crash_during_compaction_no_data_loss() {
    // Start compaction → kill mid-merge
    // .partial exists, old segments still exist
    // Reopen → .partial cleaned up, old segments still valid
}

#[test]
fn crash_after_new_segment_before_old_deletion() {
    // New segment written, inventory updated, old segments not yet deleted
    // Reopen → old segments cleaned up, new segment is authoritative
}

// --- Compaction Trigger ---

#[test]
fn compaction_triggers_when_segment_count_exceeds_threshold() {
    // Set threshold = 3
    // Create 5 segments (via flush)
    // Background compaction should merge some
    // Segment count should decrease
}

// --- Property Tests ---

#[test]
fn prop_compaction_preserves_all_live_data() {
    // For any set of segments and prune_floor:
    // compacted segment returns same results as pre-compaction for all
    // keys and snapshot_commits >= prune_floor
}

#[test]
fn prop_compaction_reduces_segment_count() {
    // Compacting N segments (N > 1) always produces < N segments
}
```

### 6.2 Memory Pressure System

**Spec:** [tiered-storage.md §10](tiered-storage.md)

#### Step 1: Write Tests (RED)

```rust
#[test]
fn memory_usage_reported_correctly() {
    let store = SegmentedStore::open(dir, config).unwrap();
    let before = store.memory_usage();
    // Write 1MB of data
    for i in 0..1000 {
        store.put_with_version(key(i), Value::Bytes(vec![0; 1024].into()), i as u64 + 1, None).unwrap();
    }
    let after = store.memory_usage();
    assert!(after > before);
    assert!(after - before >= 1_000_000); // at least 1MB added
}

#[test]
fn warning_pressure_triggers_flush() {
    // Set memory_limit very low (e.g., 1MB)
    // Write enough to exceed 70%
    // Verify flush triggered
}

#[test]
fn critical_pressure_force_flushes_all() {
    // Exceed 90% of budget
    // All branches' memtables should be flushed
}

#[test]
fn dormant_branch_memtable_flushed_on_inactivity() {
    // Write to branch A, wait 60+ seconds
    // Branch A's memtable should be flushed
    // Memory freed
}

// --- Stress: Bounded Memory Under Load ---

#[test]
#[ignore] // nightly CI
fn stress_memory_bounded_under_sustained_ingest() {
    let config = StorageConfig {
        memory_limit: 256 * 1024 * 1024, // 256MB
        write_buffer_size: 16 * 1024 * 1024, // 16MB
        ..defaults()
    };
    let store = SegmentedStore::open(dir, config).unwrap();

    // Write 1M entries (~100MB of data)
    for i in 0..1_000_000 {
        store.put_with_version(key(i), Value::Bytes(vec![0; 100].into()), i as u64 + 1, None).unwrap();
    }

    // Memory should stay within budget
    assert!(store.memory_usage() < 256 * 1024 * 1024,
        "Memory {} exceeds 256MB budget", store.memory_usage());

    // All data should be readable
    for i in [0, 100_000, 500_000, 999_999] {
        assert!(store.get(&key(i)).unwrap().is_some());
    }
}

#[test]
#[ignore] // nightly CI
fn stress_many_branches_bounded_memory() {
    // Create 100 branches, write 1000 keys each
    // Memory should stay within budget
    // Dormant branches flushed automatically
}
```

---

## Epic 7: Cutover & Cleanup

**Goal:** Remove ShardedStore. SegmentedStore is the only backend. Full validation.

### 7.1 Final Regression & Benchmark Gate

Before removing ShardedStore, pass the following gates:

| Gate | Criterion | How to Verify |
|------|-----------|---------------|
| **Contract regression** | All 42 parameterized storage tests pass for both backends | `cargo test --test storage` |
| **Full-stack regression** | All ~133 engine/concurrency/durability tests pass with segmented backend | `STRATA_STORAGE=segmented cargo test --workspace` |
| **Crash recovery** | All crash boundary tests pass | `cargo test crash` |
| **Memory bound** | 1M keys within 256MB budget | `cargo test stress_memory_bounded -- --ignored` |
| **Read latency** | Hot-path p50 < 2x ShardedStore regression | `cargo bench -p strata-engine` |
| **Write throughput** | >= 90% of ShardedStore throughput | `cargo bench -p strata-engine` |
| **Recovery time** | Cold start 10x faster with segments vs full WAL replay at 100K entries | `cargo bench recovery` |
| **No data loss** | Property test: for any write sequence, all committed data survives reopen | proptest |

### 7.2 Cleanup Tasks

| # | Task | Details |
|---|------|---------|
| 7.2.1 | Remove `ShardedStore`, `Shard`, `VersionChain`, `ShardedSnapshot` | ~4,900 lines deleted from `sharded.rs`. |
| 7.2.2 | Remove `StorageBackend::Sharded` from feature flags | SegmentedStore is the only option. |
| 7.2.3 | Remove `test_both_backends!` parameterization | Tests only run against SegmentedStore now. Simplify. |
| 7.2.4 | Update all imports | `use strata_storage::ShardedStore` → `use strata_storage::SegmentedStore` everywhere. |
| 7.2.5 | Update documentation | Architecture book, inline docs. |
| 7.2.6 | Run `cargo clippy --workspace -- -D warnings` | Clean. |
| 7.2.7 | Run full CI pipeline | Everything green. |

### 7.3 New Benchmarks to Add

```rust
// crates/engine/benches/segment_benchmarks.rs (new)

fn bench_segment_point_lookup(c: &mut Criterion) {
    // Build segment with 100K entries
    // Benchmark point_lookup for: existing key, non-existing key
}

fn bench_segment_prefix_scan(c: &mut Criterion) {
    // Build segment with 100K entries, 100 prefixes × 1000 keys each
    // Benchmark scan of 1000-key prefix
}

fn bench_flush_throughput(c: &mut Criterion) {
    // Fill 128MB memtable → flush → measure time
}

fn bench_compaction_throughput(c: &mut Criterion) {
    // Merge 5 segments of 100K entries each → measure time
}

fn bench_recovery_with_segments(c: &mut Criterion) {
    // Database with 100K entries in segments + 1K in WAL
    // Measure recovery time (should be << full WAL replay)
}
```

---

## Epic 8: Bulk Ingestion Performance

**Goal:** Achieve within 2-3x of Neo4j for graph ingestion at 100M+ scale. Address bulk ingestion bottlenecks for ALL data types (KV, graph, vector, JSON) at 100M-1B scale. The storage architecture changes in Epics 1-7 create the right foundation, but bulk performance requires changes ABOVE the Storage trait too.

**Why now:** The current architecture is 50x slower than Neo4j at graph ingestion (graph500-22 benchmark). KV `batch_put` is already reasonable. Graph has no batch edge API. The packed adjacency format forces O(n²) read-modify-write per edge. The per-branch commit lock serializes all writes. `apply_writes()` in the transaction layer processes entries one-at-a-time instead of using the batch API. These are structural problems that should be fixed alongside the storage architecture change.

**Crates touched:** `strata-engine` (~800 lines), `strata-concurrency` (~300 lines), `strata-storage` (~200 lines), `strata-core` (~50 lines)

### 8.0 Bottleneck Analysis

| Bottleneck | Layer | Impact | Fix |
|-----------|-------|--------|-----|
| 1 transaction per edge | engine/graph | 8-10 KV ops × N edges = N transactions | Batch edge API |
| O(n²) adjacency list RMW | engine/graph/packed.rs | Full decode + re-encode per edge add | Append-friendly adjacency format |
| Per-branch commit lock serialization | concurrency/manager.rs | ~1K-10K ops/sec single branch | Batch commit (one lock acquisition for N edges) |
| `apply_writes()` per-entry puts | concurrency/transaction.rs:1414 | Doesn't use `apply_batch()` | Use batch apply in commit path |
| WAL fsync per commit | durability | Latency per batch | Group commit (already in Standard mode: 100ms/1K ops) |
| No parallel branch loading | engine | Sequential per-branch recovery | Parallel recovery across branches |

### 8.1 Step 1: Write Tests (RED)

#### Batch Edge API Tests

```rust
// crates/engine/tests/graph_bulk.rs (new)

#[test]
fn batch_add_edges_inserts_all_edges() {
    let db = TestDb::new();
    let graph = db.graph();
    let branch = db.default_branch();

    let edges: Vec<(String, String, String, Option<Value>)> = (0..100)
        .map(|i| (format!("node_{}", i), format!("node_{}", i + 1), "KNOWS".into(), None))
        .collect();

    let result = graph.batch_add_edges(branch, &edges);
    assert!(result.is_ok());

    // Verify all edges exist
    for (from, to, label, _) in &edges {
        let neighbors = graph.get_neighbors(branch, from, label, Direction::Outgoing).unwrap();
        assert!(neighbors.contains(&to.to_string()));
    }
}

#[test]
fn batch_add_edges_single_transaction() {
    // Verify atomicity: either all edges are added or none
    let db = TestDb::new();
    let graph = db.graph();
    let branch = db.default_branch();

    let edges: Vec<_> = (0..50)
        .map(|i| (format!("a_{}", i), format!("b_{}", i), "LINKS".into(), None))
        .collect();

    graph.batch_add_edges(branch, &edges).unwrap();

    // All or nothing — count must be exactly 50
    let count = edges.iter()
        .filter(|(from, to, label, _)| {
            graph.get_neighbors(branch, from, label, Direction::Outgoing)
                .unwrap().contains(&to.to_string())
        })
        .count();
    assert_eq!(count, 50);
}

#[test]
fn batch_add_edges_1000_edges_under_100ms() {
    let db = TestDb::new();
    let graph = db.graph();
    let branch = db.default_branch();

    let edges: Vec<_> = (0..1000)
        .map(|i| (format!("src_{}", i % 100), format!("dst_{}", i), "EDGE".into(), None))
        .collect();

    let start = Instant::now();
    graph.batch_add_edges(branch, &edges).unwrap();
    let elapsed = start.elapsed();

    assert!(elapsed < Duration::from_millis(100),
        "1000 edges took {:?}, expected < 100ms", elapsed);
}

#[test]
fn batch_add_edges_idempotent_no_duplicates() {
    // Adding same edge twice in a batch should not create duplicates
    let db = TestDb::new();
    let graph = db.graph();
    let branch = db.default_branch();

    let edges = vec![
        ("A".into(), "B".into(), "LINK".into(), None),
        ("A".into(), "B".into(), "LINK".into(), None), // duplicate
    ];

    graph.batch_add_edges(branch, &edges).unwrap();

    let neighbors = graph.get_neighbors(branch, "A", "LINK", Direction::Outgoing).unwrap();
    // Should appear exactly once
    assert_eq!(neighbors.iter().filter(|n| *n == "B").count(), 1);
}
```

#### Adjacency Format Tests

```rust
// crates/engine/src/graph/packed.rs — inline tests

#[test]
fn append_neighbor_without_full_decode() {
    // Old format: decode all → push → re-encode (O(n) per add)
    // New format: append directly (O(1) amortized per add)
    let mut adj = AdjacencyList::new();

    // Add 1000 neighbors
    for i in 0..1000 {
        adj.append_neighbor(format!("node_{}", i), None);
    }

    // Verify all present
    let neighbors = adj.to_vec();
    assert_eq!(neighbors.len(), 1000);

    // Verify append is fast (not O(n²))
    let start = Instant::now();
    for i in 1000..2000 {
        adj.append_neighbor(format!("node_{}", i), None);
    }
    let elapsed = start.elapsed();
    // 1000 appends should be < 1ms (not O(n) decode per append)
    assert!(elapsed < Duration::from_millis(10),
        "1000 appends took {:?}, suggests O(n) decode per append", elapsed);
}

#[test]
fn adjacency_list_remove_still_works() {
    // Remove requires full decode — that's OK, removes are rare
    let mut adj = AdjacencyList::new();
    for i in 0..100 {
        adj.append_neighbor(format!("node_{}", i), None);
    }
    adj.remove_neighbor("node_50");
    let neighbors = adj.to_vec();
    assert_eq!(neighbors.len(), 99);
    assert!(!neighbors.iter().any(|n| n.id == "node_50"));
}
```

#### Batch Apply Tests

```rust
// crates/concurrency/src/transaction.rs — inline tests

#[test]
fn apply_writes_uses_batch_api() {
    // Verify that committing a transaction with N writes
    // calls apply_batch once, not put N times
    let store = MockStorage::new();
    let mut tx = Transaction::new(branch, store.snapshot());
    for i in 0..100 {
        tx.put(key(i), value(i));
    }
    tx.commit().unwrap();

    // MockStorage should record 1 batch call, not 100 individual puts
    assert_eq!(store.batch_call_count(), 1);
    assert_eq!(store.individual_put_count(), 0);
}
```

#### Transaction Bypass Prevention Tests

```rust
// tests/integration/no_bypass.rs (new)

#[test]
fn all_writes_produce_wal_records() {
    // Every write through every primitive must produce a WAL record.
    // This test catches any future bypass that writes directly to storage.
    let db = TestDb::new_strict(); // strict durability mode (fsync always)

    // Write through every primitive
    db.kv().put(branch, "ns", "key1", value("v1")).unwrap();
    db.state().set(branch, "ns", "cell1", value("s1")).unwrap();
    db.event().append(branch, "ns", "stream1", value("e1")).unwrap();
    db.json().put(branch, "ns", "doc1", json!({"a": 1})).unwrap();
    // graph and vector write through KV internally

    // Count WAL records
    let wal_records = db.inner().wal().read_all_records().unwrap();
    let write_records: Vec<_> = wal_records.iter()
        .filter(|r| r.has_mutations())
        .collect();

    // Must have at least 4 WAL records (one per primitive write)
    assert!(write_records.len() >= 4,
        "Expected >= 4 WAL records for 4 primitive writes, got {}. \
         A write may be bypassing the transaction layer.", write_records.len());
}

#[test]
fn bulk_load_mode_skips_wal_but_preserves_mvcc() {
    let db = TestDb::new();
    let branch = db.default_branch();

    // Bulk load: no WAL, but data is versioned and readable
    db.inner().begin_bulk_load(branch).unwrap();
    db.kv().batch_put_bulk(branch, "ns", &entries(1000)).unwrap();
    db.inner().end_bulk_load(branch).unwrap();

    // Data is readable (MVCC intact)
    for i in 0..1000 {
        assert!(db.kv().get(branch, "ns", &format!("key_{}", i)).unwrap().is_some());
    }

    // No WAL records for bulk load writes
    let wal_records = db.inner().wal().read_all_records().unwrap();
    assert_eq!(wal_records.iter().filter(|r| r.has_mutations()).count(), 0,
        "Bulk load mode should not produce WAL records");

    // After end_bulk_load, data survives reopen (flushed to segment)
    let db = db.reopen().unwrap();
    for i in [0, 500, 999] {
        assert!(db.kv().get(branch, "ns", &format!("key_{}", i)).unwrap().is_some());
    }
}

#[test]
fn batch_add_edges_produces_single_wal_record() {
    let db = TestDb::new_strict();
    let branch = db.default_branch();

    let edges: Vec<_> = (0..100)
        .map(|i| (format!("a_{}", i), format!("b_{}", i), "LINKS".into(), None))
        .collect();

    let wal_before = db.inner().wal().record_count().unwrap();
    db.graph().batch_add_edges(branch, &edges).unwrap();
    let wal_after = db.inner().wal().record_count().unwrap();

    // One transaction = one WAL record, not 100
    assert_eq!(wal_after - wal_before, 1,
        "batch_add_edges should produce exactly 1 WAL record, got {}", wal_after - wal_before);
}
```

#### Bulk Ingestion Benchmarks

```rust
// crates/engine/benches/bulk_ingest.rs (new)

fn bench_graph_bulk_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_bulk_ingest");
    group.sample_size(10);

    for edge_count in [1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("batch_add_edges", edge_count),
            &edge_count,
            |b, &n| {
                b.iter_with_setup(
                    || {
                        let db = TestDb::new();
                        let edges: Vec<_> = (0..n)
                            .map(|i| (format!("s_{}", i % 1000), format!("d_{}", i), "E".into(), None))
                            .collect();
                        (db, edges)
                    },
                    |(db, edges)| {
                        db.graph().batch_add_edges(db.default_branch(), &edges).unwrap();
                    },
                );
            },
        );
    }
    group.finish();
}

fn bench_kv_bulk_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_bulk_ingest");
    group.sample_size(10);

    for count in [10_000, 100_000, 1_000_000] {
        group.bench_with_input(
            BenchmarkId::new("batch_put", count),
            &count,
            |b, &n| {
                b.iter_with_setup(
                    || {
                        let db = TestDb::new();
                        let entries: Vec<_> = (0..n)
                            .map(|i| (format!("key_{}", i), Value::Bytes(vec![0u8; 100].into())))
                            .collect();
                        (db, entries)
                    },
                    |(db, entries)| {
                        db.kv().batch_put(db.default_branch(), "bulk", &entries).unwrap();
                    },
                );
            },
        );
    }
    group.finish();
}

fn bench_vector_bulk_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_bulk_ingest");
    group.sample_size(10);

    for count in [1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("batch_insert", count),
            &count,
            |b, &n| {
                b.iter_with_setup(
                    || {
                        let db = TestDb::new();
                        let config = VectorConfig::new(128, DistanceMetric::Cosine, ScalarType::F32);
                        db.vector().create_collection(db.default_branch(), "bulk", config).unwrap();
                        let vectors: Vec<_> = (0..n)
                            .map(|i| (format!("vec_{}", i), vec![0.1f32; 128]))
                            .collect();
                        (db, vectors)
                    },
                    |(db, vectors)| {
                        db.vector().batch_insert(db.default_branch(), "bulk", &vectors).unwrap();
                    },
                );
            },
        );
    }
    group.finish();
}
```

#### Scale Stress Tests

```rust
// tests/stress/bulk_ingest.rs (new)

#[test]
#[ignore] // nightly CI
fn stress_graph_1m_edges() {
    // Target: 1M edges in < 60 seconds (>16K edges/sec)
    // Neo4j does ~80K edges/sec, so 2-3x slower = 25-40K edges/sec
    let db = TestDb::new();
    let graph = db.graph();
    let branch = db.default_branch();

    let start = Instant::now();
    let batch_size = 10_000;
    for batch in 0..100 {
        let edges: Vec<_> = (0..batch_size)
            .map(|i| {
                let idx = batch * batch_size + i;
                (format!("n_{}", idx % 10_000), format!("n_{}", idx % 10_000 + 1), "KNOWS".into(), None)
            })
            .collect();
        graph.batch_add_edges(branch, &edges).unwrap();
    }
    let elapsed = start.elapsed();
    let edges_per_sec = 1_000_000.0 / elapsed.as_secs_f64();

    eprintln!("1M edges in {:?} ({:.0} edges/sec)", elapsed, edges_per_sec);
    assert!(edges_per_sec > 16_000.0,
        "Only {:.0} edges/sec, target > 16K edges/sec (3x slower than Neo4j)", edges_per_sec);
}

#[test]
#[ignore] // nightly CI
fn stress_kv_10m_entries() {
    // Target: 10M KV entries in < 120 seconds
    let db = TestDb::new();
    let kv = db.kv();
    let branch = db.default_branch();

    let start = Instant::now();
    for batch in 0..100 {
        let entries: Vec<_> = (0..100_000)
            .map(|i| {
                let idx = batch * 100_000 + i;
                (format!("k_{}", idx), Value::Bytes(vec![0u8; 100].into()))
            })
            .collect();
        kv.batch_put(branch, "bulk", &entries).unwrap();
    }
    let elapsed = start.elapsed();
    let ops_per_sec = 10_000_000.0 / elapsed.as_secs_f64();

    eprintln!("10M KV entries in {:?} ({:.0} ops/sec)", elapsed, ops_per_sec);
    assert!(ops_per_sec > 80_000.0,
        "Only {:.0} ops/sec, target > 80K ops/sec", ops_per_sec);
}

#[test]
#[ignore] // nightly CI
fn stress_parallel_branch_ingest() {
    // Multiple branches can ingest in parallel (no global lock contention)
    let db = Arc::new(TestDb::new());
    let branch_count = 8;
    let entries_per_branch = 100_000;

    let start = Instant::now();
    let handles: Vec<_> = (0..branch_count)
        .map(|b| {
            let db = db.clone();
            std::thread::spawn(move || {
                let branch_name = format!("branch_{}", b);
                db.inner().create_branch(&branch_name).unwrap();
                let branch = bridge::to_core_branch_id(&branch_name);
                let entries: Vec<_> = (0..entries_per_branch)
                    .map(|i| (format!("k_{}", i), Value::Bytes(vec![0u8; 100].into())))
                    .collect();
                db.kv().batch_put(branch, "bulk", &entries).unwrap();
            })
        })
        .collect();

    for h in handles { h.join().unwrap(); }
    let elapsed = start.elapsed();
    let total_ops = branch_count * entries_per_branch;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    eprintln!("{} entries across {} branches in {:?} ({:.0} ops/sec)",
        total_ops, branch_count, elapsed, ops_per_sec);

    // Parallel should be significantly faster than sequential
    // At minimum, 4x speedup over single-branch (8 branches, some contention)
    assert!(ops_per_sec > 200_000.0,
        "Parallel ingest only {:.0} ops/sec, expected > 200K", ops_per_sec);
}
```

### 8.2 Step 2: Implement (GREEN)

#### 8.2.1 Batch Edge API (`strata-engine`)

**File:** `crates/engine/src/graph/mod.rs`

Add `batch_add_edges()` to `GraphStore`:

```rust
/// Add multiple edges in a single transaction.
/// All edges share one commit — atomic, one WAL write, one lock acquisition.
pub fn batch_add_edges(
    &self,
    branch: BranchId,
    edges: &[(String, String, String, Option<Value>)], // (from, to, label, properties)
) -> Result<(), GraphError> {
    self.db.transaction(branch, |txn| {
        // Group edges by (from_node, label) to batch adjacency updates
        let mut grouped: HashMap<(String, String), Vec<(String, Option<Value>)>> = HashMap::new();
        for (from, to, label, props) in edges {
            grouped.entry((from.clone(), label.clone()))
                .or_default()
                .push((to.clone(), props.clone()));
        }

        // For each (from, label) group: one read + one write (not N reads + N writes)
        for ((from, label), targets) in &grouped {
            let adj_key = adjacency_key(from, label, Direction::Outgoing);
            let mut adj = match txn.get(&adj_key)? {
                Some(v) => AdjacencyList::decode(&v)?,
                None => AdjacencyList::new(),
            };

            for (to, props) in targets {
                adj.append_neighbor(to.clone(), props.clone());
            }

            txn.put(&adj_key, adj.encode())?;

            // Reverse edges
            for (to, props) in targets {
                let rev_key = adjacency_key(to, label, Direction::Incoming);
                let mut rev_adj = match txn.get(&rev_key)? {
                    Some(v) => AdjacencyList::decode(&v)?,
                    None => AdjacencyList::new(),
                };
                rev_adj.append_neighbor(from.clone(), props.clone());
                txn.put(&rev_key, rev_adj.encode())?;
            }
        }

        // Node existence markers (batch)
        for (from, to, _, _) in edges {
            ensure_node_exists(txn, from)?;
            ensure_node_exists(txn, to)?;
        }

        Ok(())
    })
}
```

**Key improvement:** For 1000 edges from 100 source nodes, the old code did 1000 transactions × 8-10 KV ops = 8,000-10,000 operations. The new code does 1 transaction with ~200 KV ops (100 outgoing adjacency reads + 100 writes + reverse edges). That's a **40-50x reduction** in KV operations.

#### 8.2.2 Append-Friendly Adjacency Format (`strata-engine`)

**File:** `crates/engine/src/graph/packed.rs`

The current packed format requires full decode to add a neighbor (deserialize entire `Vec<Neighbor>`, push, re-serialize). For high-degree nodes this is O(n) per edge.

**Change:** Add a length-prefixed append section to the binary format:

```rust
/// Adjacency list binary format (v2):
///
/// [header: u32 version=2]
/// [count: u32 total neighbors]
/// [neighbor_0: length-prefixed (id_len:u16 | id_bytes | props_len:u32 | props_bytes)]
/// [neighbor_1: ...]
/// ...
///
/// append_neighbor() only:
///   1. Increment count at offset 4
///   2. Append new neighbor entry at end
///   No decode of existing entries.
///
/// remove_neighbor() still requires full decode (rare operation).
impl AdjacencyList {
    pub fn append_neighbor(&mut self, id: String, props: Option<Value>) {
        // Increment count in header
        let count = u32::from_le_bytes(self.data[4..8].try_into().unwrap());
        self.data[4..8].copy_from_slice(&(count + 1).to_le_bytes());
        // Append entry
        let id_bytes = id.as_bytes();
        self.data.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        self.data.extend_from_slice(id_bytes);
        match props {
            Some(v) => {
                let prop_bytes = v.encode();
                self.data.extend_from_slice(&(prop_bytes.len() as u32).to_le_bytes());
                self.data.extend_from_slice(&prop_bytes);
            }
            None => {
                self.data.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }
}
```

**Impact:** Adding N neighbors to a node goes from O(N²) total work to O(N). For a node with 10,000 neighbors (common in graph500), this is the difference between ~50M bytes of decode/encode vs ~500K bytes of appends.

#### 8.2.3 Batch Apply in Transaction Commit (`strata-concurrency`)

**File:** `crates/concurrency/src/transaction.rs`

Current `apply_writes()` (~line 1414) calls `storage.put_with_version()` per entry. Change to collect writes and call `storage.apply_batch()`:

```rust
fn apply_writes(&self, storage: &dyn Storage, commit_id: u64) -> Result<()> {
    let writes: Vec<BatchEntry> = self.write_set.iter()
        .map(|(key, op)| BatchEntry {
            key: key.clone(),
            value: op.value.clone(),
            version: commit_id,
            ttl: op.ttl,
        })
        .collect();

    if !writes.is_empty() {
        storage.apply_batch(&writes)?;
    }

    // Deletes handled separately (tombstone writes)
    let deletes: Vec<BatchEntry> = self.delete_set.iter()
        .map(|key| BatchEntry {
            key: key.clone(),
            value: Value::tombstone(),
            version: commit_id,
            ttl: None,
        })
        .collect();

    if !deletes.is_empty() {
        storage.apply_batch(&deletes)?;
    }

    Ok(())
}
```

**Impact:** For SegmentedStore, batch apply can insert into the memtable skiplist with a single memory fence instead of per-entry atomic operations. For ShardedStore (before cutover), `apply_batch()` already exists and handles shard grouping.

#### 8.2.4 Bulk Load Mode (`strata-concurrency` + `strata-storage`)

Bulk load mode is a **transaction mode**, not a storage bypass. It goes through `TransactionManager::commit()` but opts out of WAL durability for initial imports where the source data is re-importable on crash.

**File:** `crates/concurrency/src/manager.rs`

```rust
/// Commit with bulk load flag — skips WAL write but preserves:
/// - MVCC versioning (all writes get a commit_id)
/// - Branch isolation (per-branch state)
/// - Per-branch commit lock (serialization)
/// - OCC validation skip (blind write — empty read set)
///
/// Caller contract: source data is available for re-import on crash.
/// Without WAL, committed data survives only if memtable is flushed to segment.
pub fn commit_bulk_load(&self, txn: &mut TransactionContext, store: &dyn Storage) -> Result<u64> {
    // Same commit path, but WAL step is skipped
    let bs = self.branch_state(txn.branch_id());
    let _lock = bs.commit_lock.lock();

    // Blind write: no validation (bulk loads have empty read set)
    let commit_id = self.version.fetch_add(1, Ordering::AcqRel);

    // NO WAL write — this is the only difference from normal commit

    // Apply to storage (same as normal commit)
    txn.apply_writes(store, commit_id)?;

    Ok(commit_id)
}
```

**File:** `crates/storage/src/segmented/mod.rs`

The storage layer supports a `begin_bulk_load()`/`end_bulk_load()` hint that optimizes for sequential write patterns:

```rust
impl SegmentedStore {
    /// Hint: bulk load starting. Increase memtable size, defer compaction.
    pub fn begin_bulk_load(&self, branch: BranchId) -> Result<()> {
        let bs = self.branch_state(branch);
        bs.set_memtable_size_override(self.config.bulk_load_memtable_size); // e.g., 512MB
        bs.defer_compaction(true);
        Ok(())
    }

    /// Hint: bulk load complete. Flush memtable, trigger compaction, restore defaults.
    pub fn end_bulk_load(&self, branch: BranchId) -> Result<()> {
        let bs = self.branch_state(branch);
        bs.flush_memtable()?;           // Force flush to segment
        bs.set_memtable_size_override(self.config.write_buffer_size); // Restore default
        bs.defer_compaction(false);
        bs.trigger_compaction()?;       // Compact the bulk-loaded segments
        Ok(())
    }
}
```

**What this is NOT:** `BulkLoader` is NOT a way to write directly to SST segments bypassing the transaction layer. All writes go through `TransactionManager` → `apply_writes()` → `Storage::apply_batch()`. The only optimization is skipping WAL (opt-in, documented tradeoff).

#### 8.2.5 WAL Group Commit Optimization (`strata-durability`)

The existing WAL already supports `Standard` mode (fsync every 100ms/1000 ops). For bulk ingestion, ensure the batch edge API benefits from group commit:

- A `batch_add_edges(1000 edges)` produces ONE WAL record (one transaction), ONE fsync
- vs old code: 1000 WAL records, potentially 1000 fsyncs (in Strict mode) or 10 fsyncs (in Standard mode)

No code change needed — the batch API at the engine layer automatically reduces WAL pressure.

#### 8.2.6 Multi-Branch Parallel Ingestion (`strata-engine`)

Different branches have independent commit locks — they can ingest in parallel with zero contention. Document and test this:

```rust
/// Bulk ingest across multiple branches in parallel.
/// Each branch has its own commit lock, memtable, and segment namespace.
/// Use rayon or std::thread for parallelism.
pub fn parallel_ingest<F>(
    db: &Database,
    branch_ids: &[BranchId],
    ingest_fn: F,
) -> Result<Vec<Result<()>>>
where
    F: Fn(&Database, BranchId) -> Result<()> + Send + Sync,
{
    branch_ids.par_iter()
        .map(|&branch| ingest_fn(db, branch))
        .collect()
}
```

### 8.3 Step 3: Refactor (REFACTOR)

After all bulk ingestion tests are green:

1. **Profile actual bottlenecks** — Run graph500-22 ingest and profile with `perf`. Identify remaining hot spots.
2. **Tune batch sizes** — Find optimal batch size for `batch_add_edges()` (likely 5K-50K edges per call).
3. **Measure and report** — Add bulk ingestion throughput to CI benchmark dashboard.
4. **Verify no bypasses crept in** — Run the `all_writes_produce_wal_records` integration test (except bulk load mode). Grep for any new direct `storage.put_*()` calls outside `apply_writes()` and `recover()`.

### 8.4 Performance Targets

| Workload | Current | Target | Improvement |
|----------|---------|--------|-------------|
| Graph: 1M edges (graph500-scale) | ~50x slower than Neo4j (~1.6K edges/sec) | Within 3x of Neo4j (~25K edges/sec) | **15x** |
| KV: 10M entries (100B values) | ~50K ops/sec (estimated) | >80K ops/sec | **1.5x** |
| Vector: 1M 128-dim vectors | ~30K ops/sec (estimated) | >50K ops/sec | **1.5x** |
| JSON: 1M documents | ~40K ops/sec (estimated) | >60K ops/sec | **1.5x** |

**Where the 15x graph improvement comes from:**

| Optimization | Improvement Factor | Cumulative |
|-------------|-------------------|------------|
| Batch edges in single transaction (eliminate 1-txn-per-edge) | 8-10x (eliminate per-txn overhead: lock + WAL + validate) | 8-10x |
| Group adjacency updates (1 read + 1 write per source node, not per edge) | 2-3x (reduce KV operations from 8-10 per edge to ~2 amortized) | 16-30x |
| Append-friendly adjacency (O(1) append vs O(n) decode-push-encode) | 1.5-3x (for high-degree nodes, more for very high degree) | 24-90x |
| Batch apply in transaction commit | 1.2-1.5x | 29-135x |

Conservative estimate: **15x improvement** (enough to hit the 3x-of-Neo4j target). The optimizations compound because they eliminate redundant work at every layer.

### 8.5 Storage Trait: Visibility and Bypass Prevention

The `Storage` trait in `crates/core/src/traits.rs` already has `apply_batch()`. Two changes to enforce the no-bypass invariant:

**1. Mutation methods are `pub(crate)` in the implementation:**

The trait itself is public (needed for the `SnapshotView` consumer interface), but the concrete `SegmentedStore` mutation methods (`put_with_version`, `delete_with_version`, `apply_batch`) should only be callable from `strata-concurrency` (the transaction commit path). Since Rust's `pub(crate)` doesn't cross crate boundaries, enforcement is structural: engine and executor depend on `strata-core` (the trait) not `strata-storage` (the implementation). Only `strata-concurrency` holds a concrete reference.

**2. Bulk load hints on the trait:**

```rust
trait Storage {
    // Existing:
    fn apply_batch(&self, entries: &[BatchEntry]) -> Result<()>;

    // New: bulk load hints (default no-op, SegmentedStore overrides)
    fn begin_bulk_load(&self, branch: BranchId) -> Result<()> { Ok(()) }
    fn end_bulk_load(&self, branch: BranchId) -> Result<()> { Ok(()) }
}
```

These are **hints**, not bypass mechanisms. Writes still go through `apply_batch()` within the transaction commit path. The hints let storage optimize (larger memtable, deferred compaction).

### 8.6 Dependency on Earlier Epics

Epic 8 can begin after Epic 3 (SegmentedStore implements Storage trait), but full benchmarking requires Epic 5 (durability) and Epic 6 (compaction). The implementation order within Epic 8:

```
8.2.2 Adjacency format (no dependencies)        ← can start immediately
8.2.1 Batch edge API (depends on 8.2.2)         ← uses new adjacency format
8.2.3 Batch apply (no dependencies)              ← can start immediately
8.2.4 Bulk loader (depends on Epic 2)            ← uses segment builder
8.2.5 WAL group commit (no changes needed)       ← verify only
8.2.6 Parallel ingest (depends on Epic 3)        ← uses SegmentedStore
```

Tasks 8.2.2 and 8.2.3 can start in parallel with the main segment store work (Epics 1-7) since they're above the Storage trait.

---

## Test Execution Summary

| Phase | New Tests Written | Existing Tests Run | Total |
|-------|-------------------|--------------------|-------|
| Epic 1 | ~25 unit + ~8 property | 0 | ~33 |
| Epic 2 | ~30 unit + ~4 property + ~3 corruption | 0 | ~37 |
| Epic 3 | ~15 new + 42 parameterized | 42 existing (ShardedStore side) | ~99 |
| Epic 4 | ~15 flush + ~3 concurrent | 84 regression (both backends) | ~102 |
| Epic 5 | ~20 recovery + ~5 WAL + ~8 integration | 84 regression + ~133 engine | ~250 |
| Epic 6 | ~15 compaction + ~8 pressure + ~3 stress | 84 regression + ~133 engine | ~243 |
| Epic 7 | ~5 benchmarks | ALL | ALL + benchmarks |
| Epic 8 | ~15 batch + ~5 format + ~3 stress + ~3 benchmarks | ALL | ALL + ~26 |

**By the end, every commit runs ~275+ tests that cover:**
- Storage trait contract (MVCC, isolation, branch, snapshot)
- Segment format correctness (round-trip, corruption detection)
- Flush pipeline (rotation, flush, concurrent access)
- Recovery (every crash boundary, delta replay, orphan handling)
- Compaction (version pruning, tombstone cleanup, crash safety)
- Memory pressure (bounded usage, dormant branch eviction)
- Full-stack (all 6 primitives, transactions, OCC, blind writes)
- Bulk ingestion (batch edges, batch apply, adjacency format, parallel ingest, scale targets)

---

## New Dependencies

| Crate | Purpose | `[dependencies]` or `[dev-dependencies]` |
|-------|---------|------------------------------------------|
| `crossbeam-skiplist` | Concurrent skiplist for memtable | dependencies |
| `arc-swap` | Atomic pointer swap for publication | dependencies |
| `crc32fast` | Block/record checksums | dependencies |
| `proptest` | Property-based testing | dev-dependencies |
| `paste` | Test macro helper for parameterization | dev-dependencies |

---

## Review Issues Resolution Map

| Issue | Epic | Resolution |
|-------|------|------------|
| #1 Fork correctness | Eliminated | Fork copies data O(N), same as today. Tested in `fork_copies_both_memtable_and_segment_data`. |
| #2 Per-branch memtables | 6 | 1024 branch limit + inactivity flush + memory pressure. Tested in `stress_many_branches_bounded_memory`. |
| #3 Research doc contradicts | N/A | Not relevant to reduced scope. |
| #4 Tombstone pseudocode | 3 | Correct implementation verified by `mvcc_dedup_tombstone_suppresses_key` and parameterized regression suite. |
| #5 Blind writes | 5 | Preserved. Tested in `blind_writes_skip_occ_with_segmented_store`. |
| #6 OCC may hit disk | 3 | Accepted. Regression suite verifies correctness. Benchmark verifies latency. |
| #7 WAL format | Eliminated | Using existing format. |
| #8 No checkpoint spec | 5 | `flushed_through_commit_id` in MANIFEST. Tested in `recovery_replays_only_wal_delta`. |
| #9 Multi-process | 5 | Existing coordination preserved. |
| #10 Flush file | 4 | Eliminated. Direct memtable → SST. Tested in `flush_moves_data_from_memtable_to_segment`. |
| #11 Compaction strategy | 6 | Size-tiered. Tested in `compact_two_segments_into_one` and property tests. |
| #12-20 | Various | See inline notes per epic. |
