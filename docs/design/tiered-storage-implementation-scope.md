# Tiered Storage: Implementation Scope Decision

**Status:** Accepted
**Date:** 2026-03-11
**Decision:** Decouple bounded memory from O(1) branching. Ship bounded memory first.

---

## Decision

The tiered storage design documents describe two orthogonal improvements:

1. **Bounded memory** — KV segment store (memtable → flush → SST → compaction)
2. **O(1) branching** — manifest-based visibility, segment sharing, ref-counted GC

These are decoupled into separate projects. **Only bounded memory is in scope for implementation now.** O(1) branching is deferred indefinitely — the current branching model (per-branch data shards, O(N) fork via data copy) is sufficient.

---

## Rationale

### Bounded memory is the urgent problem

The current architecture OOMs at scale. At 1M chunks (~1.5GB), the system is already straining. At 5M chunks (~7.2GB), it's unusable on most machines. Users are hitting this today. Bounded memory fixes this.

### O(1) branching is not needed

Strata has a hard upper limit of **1024 branches** per database. O(N) fork across 1024 shards is trivial — even a full data copy of a shard with 100K keys takes milliseconds. Users don't fork/merge branches at millisecond latency. Branches are created occasionally and most are dormant. Old branches are culled.

The current branching model (each branch owns its shard, fork copies data) is simple, correct, and well-tested. There is no user demand to change it.

### Coupling creates unnecessary complexity

Combining LSM mechanics with manifest-based branching makes every component harder:

- Fork must handle unflushed memtable data (P0 correctness issue, no clean solution)
- Compaction must coordinate across branches sharing segments (TOCTOU risks)
- Per-branch memtables blow the memory budget with many branches
- Segment ref counting, branch leases, and cross-branch GC add significant complexity
- Every bug in the segment store interacts with every bug in the branching model

Decoupling eliminates all of these problems. The segment store becomes well-understood engineering (memtable, SST, bloom filters, merge iterators — solved problems with reference implementations in RocksDB, LevelDB, Pebble).

---

## What's In Scope

A per-branch KV segment store that replaces the in-memory `VersionChain` with bounded-memory SST segments. Each branch gets its own segment namespace — no segment sharing, no manifests, no ref counting.

### From the design documents, IN SCOPE:

| Component | Document | Status |
|-----------|----------|--------|
| KV segment format (block-based, bloom, prefix compression) | storage-file-layout.md §5 | Use as-is |
| Memtable (concurrent skiplist, bounded size) | memtable-flush-file.md §2 | Use as-is |
| Memtable rotation and freeze | memtable-flush-file.md §3 | Use as-is |
| Flush pipeline (memtable → SST) | memtable-flush-file.md §4-6 | Simplify: skip flush file, flush directly from frozen memtable |
| Key encoding (TypedKeyBytes \|\| CommitId DESC) | storage-file-layout.md §8 | Use as-is |
| Point lookup (memtable → immutable memtables → segments) | read-write-algorithms.md §4.1-4.2 | Use as-is |
| Prefix scan (merge iterators) | read-write-algorithms.md §4.3 | Use as-is |
| Block cache (sharded LRU/CLOCK) | read-write-algorithms.md §2.4 | Use as-is |
| Lock-free publication (ArcSwap) | read-write-algorithms.md §5 | Use as-is |
| Memory pressure system | tiered-storage.md §10 | Simplify: per-branch memtable budget |
| WAL integration | storage-file-layout.md §3 | Adapt to existing WAL format |
| Recovery (WAL replay → memtable) | memtable-flush-file.md §7 | Simplify: no flush file recovery |
| Basic compaction (merge overlapping segments) | tiered-storage.md §11 | Simplified: per-branch only, no cross-branch coordination |

### From the design documents, OUT OF SCOPE:

| Component | Document | Why deferred |
|-----------|----------|-------------|
| Branch manifests | tiered-storage.md §5.3 | Not needed — branches own their segments |
| Segment sharing / ref counting | tiered-storage.md §5.3, §12 | Not needed — no sharing |
| O(1) fork (manifest clone) | tiered-storage.md §5.3 | Not needed — current O(N) fork is fine |
| Branch leases / heartbeats | tiered-storage.md §12 | Not needed — branches managed explicitly |
| Cross-branch compaction coordination | tiered-storage.md §11 | Not needed — per-branch compaction only |
| Unified segment lifecycle (vector/BM25 under same manifest) | tiered-storage.md Phase 2 | Separate project — vector/BM25 already have their own segment lifecycle |
| Graph segments | tiered-storage.md §5.2 | Future work |
| New WAL format (STRW magic) | storage-file-layout.md §3 | Adapt to existing WAL, don't replace it |
| Flush file intermediate format | memtable-flush-file.md §5 | Unnecessary — flush directly from memtable |

---

## Simplifications Enabled by This Scope

1. **No fork-memtable problem.** Each branch owns its memtable and segments. Fork copies data, same as today. No shared state to coordinate.

2. **No memory budget explosion.** With 1024 branch limit and most branches dormant, memtable budget is manageable. Active branches get memtables; dormant branches have segments only. A simple policy: flush and drop memtable after N seconds of inactivity.

3. **No compaction coordination.** Each branch compacts its own segments independently. No ref counting, no cross-branch prune floors, no TOCTOU issues.

4. **No WAL format change.** Adapt the segment store to work with the existing WAL format. Add new entity types if needed, but don't replace the WAL framing.

5. **No manifest format.** Per-branch segment lists can be stored as simple metadata (e.g., a segment inventory file per branch, or inline in the existing manifest).

6. **Recovery is simpler.** WAL replay rebuilds memtable. Segments are immutable on disk. No manifest adoption, no orphaned segment resolution, no flush file recovery.

---

## Architecture After Implementation

```
Per Branch:
  ┌─────────────────────────────────┐
  │ Active Memtable (bounded)       │  ArcSwap<Memtable>, concurrent skiplist
  ├─────────────────────────────────┤
  │ Immutable Memtables (1-2)       │  Frozen, awaiting flush
  ├─────────────────────────────────┤
  │ KV Segments (immutable, mmap)   │  Newest → oldest, bloom filtered
  └─────────────────────────────────┘

Shared:
  ┌─────────────────────────────────┐
  │ Block Cache (sharded LRU)       │  Bounded, shared across branches
  ├─────────────────────────────────┤
  │ WAL (existing format, adapted)  │  Durability layer, unchanged
  └─────────────────────────────────┘

Branching: unchanged (ShardedStore per-branch model, O(N) fork)
Vector/BM25: unchanged (existing segment lifecycle)
```

---

## Relationship to Design Documents

The 5 design documents describe the correct **end state** for Strata's storage architecture. They remain valuable as a long-term vision. This scope decision selects a subset that delivers the most critical value (bounded memory) with the least risk.

If O(1) branching becomes a user requirement in the future, the segment store built here is the foundation. Adding manifests on top of a stable, per-branch segment store is a cleaner project than building both simultaneously.

---

## Review Issues Resolved by This Scope

From `tiered-storage-review.md`:

| Issue | Priority | Resolution |
|-------|----------|------------|
| #1 Fork correctness (memtable data) | P0 | **Eliminated** — fork copies data, no shared memtables |
| #2 Per-branch memtables blow budget | P0 | **Mitigated** — 1024 branch limit, dormant branches flush memtables |
| #3 Research doc contradicts design | P0 | **Deferred** — research doc recommendations not relevant to this scope |
| #5 Blind writes absent | P1 | **Must address** — preserve blind write optimization in new commit path |
| #6 OCC validation may hit disk | P1 | **Must address** — accept trade-off, document, consider pre-fetch |
| #7 WAL format incomplete | P1 | **Eliminated** — using existing WAL format |
| #8 No checkpoint spec | P1 | **Must address** — needed for WAL truncation after flush |
| #9 Multi-process ignored | P1 | **Must address** — preserve existing coordination |
| #10 Flush file unnecessary | P1 | **Accepted** — flush file removed from scope |
| #11 No compaction strategy | P1 | **Must address** — needed, but simpler without cross-branch coordination |
| #12-17 P2 issues | P2 | Most eliminated or simplified by reduced scope |
| #18-20 P3 issues | P3 | Consistency fixes, apply when updating docs |
