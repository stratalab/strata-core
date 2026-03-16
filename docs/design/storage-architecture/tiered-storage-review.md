# Tiered Storage Design Review

**Status:** Critical Review
**Date:** 2026-03-11
**Reviewer:** Architecture Audit
**Scope:** All 5 tiered storage design documents

> This document captures issues found during critical review of the tiered storage proposal.
> Each issue is prioritized (P0-P3) and must be resolved before or during implementation.
>
> **Documents reviewed:**
> - [Architecture Overview](tiered-storage.md)
> - [File Layout Specification](storage-file-layout.md)
> - [Read/Write Algorithms](read-write-algorithms.md)
> - [Memtable and Flush Pipeline](memtable-flush-file.md)
> - [Industry Research](tiered-storage-research.md)

---

## P0 — Correctness Issues (Block Implementation)

### 1. Fork correctness: memtable data invisible to forked branches

**Documents:** tiered-storage.md §5.3, read-write-algorithms.md §2.1

The design claims fork is O(1) manifest clone. But when branch A is forked to create branch B, A's active memtable may contain committed data that hasn't been flushed to any segment. B's manifest points to A's segments, and B gets a fresh empty memtable. Result: **committed data in A's memtable is invisible to branch B.**

Current behavior (verified in `crates/engine/src/branch_ops.rs:215-313`): fork copies ALL data entry-by-entry in transactions — O(N), not O(1). The new design promises O(1) but breaks correctness.

**Options:**
- Force-flush A's memtable before forking — expensive, defeats O(1)
- Give B a read-only reference to A's frozen memtable snapshot — adds complexity to the read path (B must check A's frozen memtable in addition to its own)
- Copy A's memtable entries into B — defeats O(1)
- Shared memtable with branch-aware visibility — most complex but preserves O(1)

**Resolution required before implementation.**

---

### 2. Per-branch memtables blow the memory budget

**Documents:** tiered-storage.md §10, read-write-algorithms.md §2.1

Each branch gets its own `mem_active` (128MB default) + `mem_immut` list. With 50 branches (normal for a branching-first database), that's 50 × 128MB = 6.4GB of memtables alone — nearly the entire 8GB default budget, leaving nothing for the block cache.

The memory model table (tiered-storage.md §10) says "Active memtable: 128MB, Fixed" as if there's only one. This is fundamentally wrong for a multi-branch system.

**Options:**
- A shared memtable pool with per-branch partitions and a global budget
- A global memtable budget (like RocksDB's `WriteBufferManager`) that throttles branches and uses smaller per-branch memtables
- Dynamically sized memtables: `budget / active_branch_count` per branch
- Distinguish "active" branches (with memtables) from "dormant" branches (memtable flushed, reads go to segments only)

**Resolution required before implementation.**

---

### 3. Research document contradicts the architecture document

**Documents:** tiered-storage-research.md §10, tiered-storage.md §3

The research doc §10 "Architecture Recommendation" proposes a completely different architecture than what tiered-storage.md designs:

| Aspect | Research Recommendation (§10) | Architecture Design |
|--------|-------------------------------|-------------------|
| Key index | `FxHashMap<Arc<Key>, KeyMeta>` always in RAM | No resident key index — bloom filters on segments |
| Ordered index | `BTreeSet<Arc<Key>>` always in RAM | Prefix scan via merge iterators over segments |
| Cooling model | DragonflyDB mark-sweep | LSM-style memtable → flush → segment |
| Cold tier | Append-only log or redb | Immutable SST-like segments |

The architecture overview (§3) explicitly rejects the research recommendation's approach: *"Page cache (wrap FxHashMap with LRU): Bounds values but leaves key metadata unbounded. At 100M keys, ~23GB. Dead end."*

A reader following the research doc's recommendation would build the wrong system. The research doc needs its §10 updated to match the actual design, or the disconnect needs to be called out explicitly with a note that the recommendation was superseded.

---

### 4. Prefix scan tombstone handling is ambiguous

**Document:** read-write-algorithms.md §4.3

The pseudocode for prefix scan shows:

```
if entry.key != last_key:
    if entry.commit_id <= snapshot_commit:
        if entry.is_tombstone: skip
        else: yield(entry.key, entry.value)
        last_key = entry.key
```

The `skip` semantics are ambiguous. If `skip` means `continue` (go to next entry), then `last_key = entry.key` must execute BEFORE the skip to suppress older versions of the same tombstoned key. The current indentation suggests `last_key = entry.key` is set regardless of tombstone status (it's at the same level as the tombstone check), but this is unclear.

The pseudocode should be rewritten to make control flow explicit:

```
if entry.key != last_key:
    if entry.commit_id <= snapshot_commit:
        last_key = entry.key          // always set, even for tombstones
        if not entry.is_tombstone:
            yield(entry.key, entry.value)
    else:
        continue
else:
    continue
```

Without this fix, an implementer may write code that fails to suppress older versions behind a tombstone.

---

## P1 — Significant Design Gaps

### 5. Blind writes completely absent from the design

**Documents:** read-write-algorithms.md §3.2, tiered-storage.md §8

The current system has a critical optimization: blind writes (transactions with no reads, no CAS, no JSON snapshots) skip OCC validation entirely. This is used heavily by the embed hook, event log appends, and graph adjacency updates.

Verified in codebase: `crates/concurrency/src/manager.rs:243-260` — blind write detection is a core part of the current commit path.

The design's commit algorithm always acquires the commit lock and runs full OCC validation. Blind writes are never mentioned anywhere in the 5 documents. This optimization must be preserved — without it, write throughput for append-heavy workloads (event logs, embeddings) would regress significantly.

---

### 6. OCC validation may hit disk under commit lock

**Document:** read-write-algorithms.md §3.2

The commit algorithm step 1 does `get_version_only(bs, key)` — described as a "lock-free read" — to validate each entry in the read set. But this read follows the full read path: memtable → immutable memtables → **segments (disk)**. If the read set contains keys whose latest versions are in cold segments, OCC validation triggers disk I/O while holding the per-branch commit lock.

This makes the "small critical section" potentially very large. A transaction with 100 keys in its read set, all cold, could hold the commit lock for tens of milliseconds while paging in segment blocks.

**Mitigation options:**
- Pre-fetch read set keys before acquiring the commit lock (speculative validation)
- Cache latest-version commit_ids in a separate lightweight structure
- Accept the cost and document as a known trade-off (most recently-written keys will be in memtable)
- Perform OCC validation outside the lock, then re-validate only conflicting keys inside

---

### 7. WAL format doesn't cover vector/BM25/graph operations

**Document:** storage-file-layout.md §3.2

The `WriteOp` format only has: `PUT=1, DEL=2, CAS_PUT=3, CAS_DEL=4` with `key_bytes` and `value_bytes`. But the current WAL encodes 7 entity types:

| Entity Type | Current Tag | WAL Encoding |
|-------------|-------------|-------------|
| `ENTITY_KV` | 0x01 | Key + Value bytes |
| `ENTITY_EVENT` | 0x02 | Key + Value bytes |
| `ENTITY_STATE` | 0x03 | Key + Value bytes |
| `ENTITY_RUN` | 0x05 | Key + Value bytes |
| `ENTITY_JSON` | 0x06 | Key + Value bytes |
| `ENTITY_VECTOR` | 0x07 | MessagePack serialized (`WalVectorUpsert`, etc.) |
| BM25/Searchable | (none) | Not in WAL today |

Source: `crates/durability/src/format/writeset.rs`, `crates/engine/src/primitives/vector/wal.rs`

The design needs either:
- An `entity_type` field in WriteOp to distinguish KV from vector from BM25
- Separate WAL record types per primitive
- An explicit statement that all non-KV operations are re-encoded as typed KV operations (with the encoding specified)

---

### 8. No WAL truncation or checkpoint specification

**Documents:** storage-file-layout.md §3.1, memtable-flush-file.md §7

The design mentions `CHECKPOINT` as a WAL record type (`record_type=0x02`) but never specifies:

- **Payload format:** What data does a CHECKPOINT record contain?
- **When it's written:** After flush? After manifest publish? After compaction?
- **Recovery semantics:** How does recovery use checkpoints to skip already-flushed data?
- **WAL truncation:** When can old WAL segments be deleted?

Without this specification, recovery must replay the entire WAL from the beginning. At 100M commits over days of operation, that's catastrophic startup time. RocksDB uses WAL sequence numbers tied to flushed memtable boundaries. The design needs an equivalent mechanism.

---

### 9. Multi-process coordination completely ignored

**Document:** All 5 documents (absence)

The current system has explicit multi-process support:

- `WalFileLock`: advisory file lock on `{wal_dir}/.wal-lock` with PID-based stale detection
- `CounterFile`: shared `(max_version, max_txn_id)` allocation across processes (16 bytes)
- Source: `crates/durability/src/coordination.rs:1-200`

The new design never mentions multi-process access. With mmap segments, shared manifests, and per-branch memtables, multi-process coordination becomes significantly more complex:

- Who owns the memtable? (Only one process can write to a skiplist)
- How are manifest updates coordinated? (Concurrent manifest publishes would corrupt state)
- Can multiple processes mmap the same segment files safely? (Read-only mmap is safe; concurrent segment creation needs coordination)

The design needs to either:
- Explicitly drop multi-process support (document as breaking change, justify why)
- Design a coordination protocol for shared access to manifests and WAL

---

### 10. Flush file adds unnecessary write amplification

**Documents:** memtable-flush-file.md §4-5

The flush pipeline writes each entry through 4 locations:

```
WAL (durable) → memtable (memory) → flush file (disk) → SST segment (disk)
```

The flush file exists "to simplify recovery reasoning" (§4). But:

1. The WAL already provides durability for unflushed memtable data
2. The frozen memtable is pinned in memory by `Arc` until the segment is published
3. If crash happens mid-SST-build, recovery replays WAL → rebuilds memtable → retries flush
4. RocksDB flushes directly from immutable memtable to SST — no intermediate file

The doc acknowledges this: *"Segment build can also be done directly from the memtable."*

The flush file doubles the disk write I/O for every flush. At sustained ingestion (128MB memtable flushed every few seconds), this is significant. Recommend removing the flush file from the design and flushing directly from frozen memtable to SST, with WAL replay as the crash recovery path.

If the flush file is retained for operational reasons (decoupling serialization from index building), this trade-off should be explicitly quantified and justified.

---

### 11. No compaction strategy specified

**Documents:** tiered-storage.md §11, read-write-algorithms.md (absence)

The compaction section describes merge mechanics but never specifies a segment selection strategy:

- **How are segments chosen for compaction?** Size-tiered? Leveled? Universal? The design says "Segment count exceeds threshold (e.g., >10 overlay segments)" — this is a trigger, not a strategy.
- **How many levels/tiers exist?** RocksDB has L0-L6. TigerBeetle has 7 levels. The design has "overlay segments" and "base segments" but no level structure.
- **What's the target space amplification?** Leveled: ~10-20%. Universal: up to 2x. Size-tiered: unbounded.
- **What's the expected write amplification?** Not discussed anywhere.
- **How does compaction interact with branching?** If branch A and B share segments, can those segments be compacted? The design says "compute prune_floor = min(low_watermark_commit) across all branches referencing {S1..Sn}" but doesn't address the TOCTOU issue: a new branch could be forked (cloning a manifest that references S1) between computing the prune_floor and deleting old segments.

Every system in the research doc has an explicit compaction strategy. "Merge when count exceeds threshold" is insufficient — without a strategy, write amplification is unbounded and space amplification is unpredictable.

---

## P2 — Design Weaknesses

### 12. SegmentRef.ref_count is Arc\<AtomicU32\> but manifests are immutable files

**Documents:** tiered-storage.md §5.3, storage-file-layout.md §4.3

The `BranchManifest` struct shows `ref_count: Arc<AtomicU32>` on `SegmentRef`. But manifests are described as immutable snapshot files on disk. `Arc<AtomicU32>` is an in-memory construct that cannot be serialized to a manifest file.

The design acknowledges this inconsistency: *"Reference counting is either stored separately (REFCOUNTS chunk) or derived by scanning branch entries during manifest build."*

The in-memory representation and on-disk representation need to be clearly separated. The `BranchManifest` struct as shown mixes both. Recommend:
- On-disk: no ref_count field; derive from branch entry scanning during manifest load
- In-memory: `SegmentHandle` wraps `SegmentEntry` with runtime `Arc<AtomicU32>`

---

### 13. Block cache + mmap is double-caching

**Documents:** read-write-algorithms.md §2.4, tiered-storage.md §9-10

The design has both mmap for segment access (OS page cache) AND an application-level block cache (2GB default). Hot segment blocks will exist in both caches simultaneously — wasting memory. At 2GB block cache + OS caching the same 2GB of mmap pages, the effective memory cost is 4GB for 2GB of data.

**Options (choose one):**
- Use `O_DIRECT` for segment reads (block cache is authoritative, OS cache bypassed) — RocksDB approach
- Skip the application block cache entirely (OS cache is authoritative) — LMDB approach
- Accept the waste and document it — simplest but wasteful
- Use `madvise(MADV_DONTNEED)` after reading into block cache to evict from OS cache — fragile

The research doc analyzes this exact trade-off for RocksDB (§3.2) and LMDB (§6.2) but the design doesn't pick a position.

---

### 14. SEGMENT_PUBLISHED WAL record type is undefined

**Document:** storage-file-layout.md §3.1

WAL record_type `0x03 = SEGMENT_PUBLISHED` appears in the framing specification but has no payload definition anywhere. This record type is critical for:

- WAL truncation: marking which WAL entries are already captured in segments
- Recovery optimization: skipping replay of entries already in published segments
- Multi-process coordination: signaling segment visibility to other processes

Needs a payload specification:

```
SegmentPublishedRecord {
    segment_id: u128
    segment_type: u8
    branch_id: u128
    commit_range_min: u64
    commit_range_max: u64
    manifest_id: u64        // the manifest that adopted this segment
}
```

---

### 15. Branch leases add overhead unusual for embedded databases

**Documents:** tiered-storage.md §12

24-hour leases with heartbeats on every `get()`, `put()`, `scan_prefix()`, and `begin_transaction()` add per-operation overhead (atomic store to `last_active`). For an embedded database running inside an application process, branches don't "go away" — they exist until the application explicitly deletes them.

Leases solve a real problem (stale branches blocking compaction) but could be solved more simply:
- Explicit `db.delete_branch()` or `db.archive_branch()` — application controls lifecycle
- Per-branch retention policies alone (already in the design)
- Compaction skips branches with no writes in N hours — no per-operation heartbeat needed

The WiredTiger and RocksDB research sections show that production databases don't use lease mechanisms. They rely on explicit lifecycle management.

---

### 16. No large value handling

**Documents:** tiered-storage.md §8, storage-file-layout.md §5

A single 50MB `Value::Bytes` goes into a 128MB memtable, consuming nearly half the buffer. Flushed into a segment, it creates a massive data block. During compaction, the entire 50MB value is rewritten even if it hasn't changed.

Write amplification for large values under leveled compaction: `50MB × levels × compaction_ratio` — potentially hundreds of megabytes of I/O for a single value.

RocksDB's BlobDB (research §3.4) separates large values into blob files, reducing write amplification by 36-90%. The research doc analyzes this but the design doesn't address it.

**Recommendation:** Define a `large_value_threshold` (e.g., 64KB). Values above this threshold are stored in separate blob files, with the segment storing only a blob reference. This is a Phase 2+ concern but the design should acknowledge the problem and reserve space in the segment format for blob references.

---

### 17. No migration path from current WAL to new WAL format

**Documents:** storage-file-layout.md §3, All documents (absence)

The current WAL format:
- No magic bytes
- Length-prefixed records with format_version byte
- Entity-typed writesets with MessagePack for vectors
- Source: `crates/durability/src/format/wal_record.rs`

The proposed WAL format:
- `STRW` magic bytes
- Different framing (magic + version + record_type + flags + length + CRC + payload)
- KV-only WriteOp (no entity types)

These are completely incompatible. How does an existing database upgrade?

**Options:**
- Require full export + reimport (data loss risk, downtime)
- Dual-reader that detects format by magic bytes (complex but seamless)
- Version gate: old WAL replayed into new format on first open, old WAL archived

This needs to be addressed in the implementation roadmap.

---

## P3 — Minor Issues / Clarity

### 18. Snapshot model inconsistency across documents

**Documents:** tiered-storage.md §13, read-write-algorithms.md §1.2

| Document | Snapshot Definition |
|----------|-------------------|
| tiered-storage.md §13 | `(branch_id, commit_id)` — "No copying. No pinning memory. No registering with a snapshot tracker." |
| read-write-algorithms.md §1.2 | `(branch_id, snapshot_commit_id, manifest_epoch)` + `Arc` to immutable state roots |

These are different models. The architecture overview says snapshots are trivially cheap (two integers). The algorithms doc says snapshots carry an `Arc` to state roots (which pins those roots in memory until the snapshot is dropped).

Both can be true (the `Arc` keeps state alive but is O(1) to create), but the documents should use consistent language. The architecture overview's "no pinning memory" claim is misleading if snapshots hold `Arc` references that prevent segment handle deallocation.

---

### 19. seg_id type inconsistency

**Documents:** tiered-storage.md §5.3, storage-file-layout.md §2, §4.3, §5.2

| Location | Type |
|----------|------|
| `tiered-storage.md` §5.3 `SegmentRef` | `segment_id: u64` |
| `storage-file-layout.md` §2 | `seg_id: unique u128 (or u64 + random suffix)` |
| `storage-file-layout.md` §4.3 `SegmentEntry` | `seg_id: u128` |
| `storage-file-layout.md` §5.2 `KVHeader` | `seg_id: u128` |

Pick one. Recommendation: `u128` — avoids collision across crashes without requiring coordination. The architecture overview should be updated to match.

---

### 20. branch_id type inconsistency

**Documents:** Multiple locations across all documents

| Location | Type |
|----------|------|
| Most struct definitions | `u128` |
| Some struct definitions | `u128 (or u64)` |
| Current codebase (`BranchId`) | UUID v5 from string names (128-bit) |
| `storage-file-layout.md` §3.2 CommitRecord | `branch_id: u128 (or u64)` |

The `(or u64)` hedging should be removed. The current codebase uses UUID v5 (128-bit). All documents should consistently use `u128` / `BranchId`.

---

## Summary

| Priority | Count | Key Themes |
|----------|-------|------------|
| **P0** | 4 | Fork correctness with unflushed memtable data; per-branch memory explosion; research doc contradicts design; tombstone pseudocode ambiguity |
| **P1** | 7 | Missing blind writes; OCC may hit disk; WAL format incomplete; no checkpoint spec; multi-process ignored; unnecessary flush file; no compaction strategy |
| **P2** | 6 | ref_count inconsistency; double-caching; missing WAL record spec; branch leases overhead; no large value handling; no migration path |
| **P3** | 3 | Snapshot model mismatch; seg_id type; branch_id type |

**P0 items #1 and #2 are the most critical.** They affect the core value proposition: O(1) branching + bounded memory. Both need architectural resolution before implementation begins.
