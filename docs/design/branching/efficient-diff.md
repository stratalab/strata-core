# Efficient Branch Diff: From O(N) Full Scan to O(Δ) Change-Proportional

**Issue**: [#1571](https://github.com/stratalab/strata-core/issues/1571)
**Status**: Design
**Date**: 2026-03-25

## Problem

`diff_branches_with_options()` and `three_way_diff()` in `branch_ops.rs` perform full scans of all entries in every branch involved, build complete `HashMap`s in memory, and compare key-by-key. This is **O(N)** in time and space where N = total entries across all branches — regardless of how many keys actually changed.

For a database with 100M keys where a branch modifies 50, the diff scans all 100M+ entries to find those 50 changes.

### Current Algorithm

```
diff_branches(branch_a, branch_b):
  for each TypeTag (KV, Event, Json, Vector, VectorConfig):
    for each space:
      A_entries = storage.list_by_type(&branch_a, tag)   // Full scan
      B_entries = storage.list_by_type(&branch_b, tag)   // Full scan
      build HashMap(user_key → value) for each
      compare all keys across both HashMaps

three_way_diff(source, target, merge_base):
  for each TypeTag and space:
    ancestor = storage.list_by_type_at_version(base, tag, fork_version)  // Full scan
    source   = storage.list_by_type(&source, tag)                        // Full scan
    target   = storage.list_by_type(&target, tag)                        // Full scan
    build three HashMaps, union all keys, classify each via 14-case matrix
```

`list_by_type()` traverses: active memtable → frozen memtables → all on-disk segments (L0–L6) → all inherited layers → MergeIterator → MvccIterator → tombstone filter → collect to Vec.

---

## State of the Art

### Git: Merkle Trees with Recursive Subtree Skipping

Git's object model is a content-addressed Merkle tree. Each tree node's SHA-1 is computed from its contents (entries + child hashes). Diffing two commits:

1. Compare root tree hashes. If equal → identical, done.
2. Walk both sorted entry lists in lockstep.
3. For each entry: if hashes match → skip entire subtree in O(1).
4. Only recurse into subtrees where hashes diverge.

**Complexity**: O(changed_files × tree_depth). Changing 3 files in a 100K-file repo examines ~3 root-to-leaf paths, not 100K entries.

**Three-way merge** operates entirely on 20-byte hash comparisons at the tree level:
- base == ours && theirs differs → take theirs
- base == theirs && ours differs → take ours
- all three equal → unchanged
- all three differ → conflict (read blobs only here)

### Dolt: Prolly Trees with Content-Defined Chunking

Dolt uses **Prolly Trees** — content-addressed B-trees where node boundaries are determined by a rolling hash (content-defined chunking). Critical property: **history independence** — the tree shape is a deterministic function of contents, not insertion order, guaranteeing maximum structural sharing.

**Diff algorithm** (dual-cursor with `FastForwardUntilUnequal`):
1. Two cursors walk left-to-right through both Prolly Trees.
2. When values are equal, compare content addresses on cursor paths (root to leaf).
3. Find deepest level where both paths share identical chunk addresses.
4. Jump past the entire shared subtree via `NextAtLevel(level + 1)`.
5. Only descend when hashes diverge.

**Complexity**: O(d × log(n)) where d = differences, n = total data. For a 1M-row table with a single change, diff completes in ~698μs. A 5M-row three-way merge completes in ~0.01s with their node-level optimization.

### lakeFS: Two-Layer Merkle Tree of Graveler Ranges

lakeFS represents each committed state as:
- **Ranges**: Immutable SSTables (1–8MB), each covering a contiguous sorted key range, named by content hash.
- **MetaRange**: An SSTable listing all Range IDs, keyed by last key in each range. MetaRange hash derives from constituent range hashes.

**Diff**: Walk two MetaRanges in parallel. If a range hash matches → skip the entire range (potentially thousands of keys) in O(1). Only open ranges where hashes differ.

**Complexity**: O(changed_ranges × keys_per_range). For 200M objects where 5–20% of ranges change, only changed ranges are read.

### Nessie: Per-Commit Change Manifests

Nessie records which content keys changed in each commit entry. Diff = walk commit log from common ancestor to each tip, collect changed key sets. No full-scan needed.

**Complexity**: O(commits_since_ancestor × keys_per_commit). Simple, no storage engine changes needed, but requires faithful recording of change sets.

### TerminusDB: Immutable Layer Stack with Delta Encoding

Each layer has a positive plane (+additions) and negative plane (-deletions). Diff = collect +/- planes from ancestor to each branch tip. Net change = union with cancellations.

### Summary

| System | Data Structure | Diff Complexity | Key Mechanism |
|--------|---------------|----------------|---------------|
| Git | Merkle tree of tree objects | O(Δ × depth) | Content-addressed subtree skipping |
| Dolt | Prolly tree (content-chunked) | O(Δ × log N) | FastForward via cursor path comparison |
| lakeFS | MetaRange/Range SSTables | O(changed_ranges) | Content-addressed range hashing |
| Nessie | Commit log + key manifests | O(Σ commits × keys/commit) | Per-commit change journal |
| TerminusDB | Layer stack with ±planes | O(Σ layers × changes/layer) | Explicit delta encoding |
| **Strata** | **Full scan + HashMap** | **O(N total)** | **None** |

---

## Design: Segment-Hash Diff for Strata

We propose a **two-tier approach** that delivers most of the benefit with minimal disruption to the existing storage engine.

### Tier 1: Segment Content Hashing (lakeFS pattern)

The core insight: Strata already partitions data into immutable, sorted segments. If each segment carries a content hash, two branches sharing segments (via COW inherited layers or identical compaction output) can skip shared segments during diff.

#### 1.1 Segment Content Hash

Add a **content hash** to every segment, computed at build time from the sorted key-value entries:

```rust
// In segment_builder.rs — PropertiesBlock extension
struct PropertiesBlock {
    entry_count: u64,
    commit_min: u64,
    commit_max: u64,
    key_min: Vec<u8>,
    key_max: Vec<u8>,
    // NEW:
    content_hash: [u8; 32],   // BLAKE3 hash of all (key, value, commit_id) entries
}
```

**Hash computation**: Stream entries through BLAKE3 during segment build. BLAKE3 is ~3GB/s on modern CPUs — negligible overhead relative to I/O.

**What to hash**: The hash covers `(typed_key, value_bytes, commit_id)` for every entry in sorted order. This means two segments with identical logical content (same keys, same values, same versions) produce the same hash, even if built at different times.

**Branch-neutral hashing**: The typed key includes the branch ID prefix. For COW-inherited segments, the content hash is computed against the *source* branch's key namespace. When comparing across branches, the diff algorithm must account for this (see §1.3).

#### 1.2 Branch Manifest

Introduce a **branch manifest** — a compact summary of a branch's segment state at a point in time:

```rust
/// Ordered list of (key_range, content_hash) covering the branch's keyspace.
/// Analogous to lakeFS MetaRange.
struct BranchManifest {
    branch_id: BranchId,
    version: u64,
    /// Segments sorted by key_min, with content hashes.
    /// L1+ segments are non-overlapping by construction.
    entries: Vec<ManifestEntry>,
}

struct ManifestEntry {
    segment_id: SegmentId,
    level: u8,
    key_min: Vec<u8>,    // First typed_key in segment
    key_max: Vec<u8>,    // Last typed_key in segment
    content_hash: [u8; 32],
    entry_count: u64,
    commit_min: u64,
    commit_max: u64,
}
```

The manifest is derived from the existing `SegmentVersion` — no new persistence required for the manifest itself, only the content hash in each segment's properties block.

#### 1.3 Segment-Level Diff Algorithm

```
segment_diff(branch_a, branch_b):
  manifest_a = build_manifest(branch_a)  // From SegmentVersion + inherited layers
  manifest_b = build_manifest(branch_b)

  // Phase 1: Segment-level comparison (L1+ non-overlapping segments)
  Walk manifest_a and manifest_b in key-sorted order:
    If segments cover same key range AND content_hash matches:
      SKIP — no changes in this range                          // O(1) per segment
    If segments cover same key range AND content_hash differs:
      DESCEND — diff entries within these segments only         // O(entries in segment)
    If key ranges don't overlap:
      Emit bulk add/remove for the non-overlapping range

  // Phase 2: L0 segments and memtables (small, recent writes)
  Scan L0 segments + memtables normally (these are small by definition)

  // Phase 3: Inherited layer fast path
  For COW-forked branches sharing inherited segments:
    Inherited segments with no shadowing writes → SKIP entirely
    Only diff the child's own segments against inherited baseline
```

**Complexity**: O(S + Δ) where S = number of segments (typically hundreds, not millions) and Δ = entries in changed segments. For a 100M-key database with 50 changed keys confined to 1–2 segments, the diff examines ~S segment hashes + entries in those 1–2 segments.

#### 1.4 COW-Aware Fast Path

For the common case of diffing a child branch against its parent (or two siblings):

```
cow_diff(child, parent):
  // Child's own segments contain ALL of its modifications
  // Inherited segments are by definition unchanged from parent
  // Therefore: only scan child's own segments (memtable + L0 + compacted)

  child_own_segments = child.segment_version  // Excludes inherited layers
  for entry in scan(child_own_segments):
    parent_value = point_lookup(parent, entry.key)
    if entry.value != parent_value:
      emit diff entry

  // Also check: keys in parent that were deleted by child (tombstones in child's own segments)
  for tombstone in scan_tombstones(child_own_segments):
    if point_lookup(parent, tombstone.key).is_some():
      emit deletion
```

**Complexity**: O(W) where W = entries written to the child branch since fork. This is optimal — you cannot do better than reading the entries that actually changed.

**When applicable**: When `get_fork_info()` returns an active inherited layer for the child-parent relationship. This covers the most common agent workflow (fork → modify → diff → merge).

### Tier 2: Per-Version Change Manifest (Nessie pattern)

For scenarios where segment hashing is insufficient (e.g., both branches have undergone heavy compaction that destroys segment identity), maintain an explicit change journal.

#### 2.1 Change Manifest

Record the set of modified keys with each write batch:

```rust
/// Stored as a system KV entry: _system_:changelog:{branch}:{version}
struct ChangeManifest {
    version: u64,
    branch_id: BranchId,
    changes: Vec<ChangeEntry>,
}

struct ChangeEntry {
    space: String,
    key: Vec<u8>,
    type_tag: TypeTag,
    kind: ChangeKind,  // Put | Delete
}
```

#### 2.2 Change-Manifest Diff Algorithm

```
manifest_diff(branch_a, branch_b, merge_base):
  // Collect all change manifests from merge_base to each branch tip
  changes_a = collect_changes(branch_a, merge_base.version..branch_a.version)
  changes_b = collect_changes(branch_b, merge_base.version..branch_b.version)

  // Net changes per branch (last-write-wins within each branch)
  net_a = deduplicate(changes_a)  // key → final ChangeKind
  net_b = deduplicate(changes_b)

  // Three-way classification
  for key in union(net_a.keys(), net_b.keys()):
    match (net_a.get(key), net_b.get(key)):
      (Some, None) → source-only change
      (None, Some) → target-only change
      (Some, Some) → potential conflict, read values for comparison
```

**Complexity**: O(C_a + C_b) where C = total change entries since merge base. For branches with thousands of modifications this is efficient; for branches that have diverged for millions of writes, the change manifest itself becomes large.

#### 2.3 Storage Cost

Each change entry is ~50–200 bytes (space string + key bytes + type tag + kind). For a transaction modifying 100 keys, the manifest is ~10–20KB — negligible relative to the data writes themselves.

Manifests are stored as regular KV entries on the `_system_` branch and participate in normal compaction. Old manifests (below the oldest active merge base) can be pruned.

---

## Implementation Plan

### Phase 1: Segment Content Hashing (Tier 1.1 + 1.4)

**Goal**: Enable O(W) COW-aware diff for the parent-child case.

1. **Add content hash to PropertiesBlock**
   - Extend `SegmentBuilder` to compute BLAKE3 hash during segment build
   - Extend `PropertiesBlock` with `content_hash: Option<[u8; 32]>` (None for legacy segments)
   - Bump properties block format version; graceful fallback for old segments

2. **COW-aware diff fast path**
   - New function `cow_diff()` in `branch_ops.rs`
   - Detects parent-child relationship via `get_fork_info()`
   - Scans only child's own segments (excludes inherited layers)
   - For each entry in child's own segments, point-lookup parent for comparison
   - Falls back to full scan if no fork relationship found

3. **Wire into existing API**
   - `diff_branches_with_options()` tries COW fast path first
   - `three_way_diff()` uses COW fast path for source-vs-ancestor and target-vs-ancestor
   - Transparent to callers — same `BranchDiffResult` / `ThreeWayDiffResult` types

### Phase 2: Segment-Level Diff (Tier 1.2 + 1.3)

**Goal**: Enable O(S + Δ) diff for branches that have diverged and been compacted.

4. **Build BranchManifest from SegmentVersion**
   - Extract `ManifestEntry` from each segment's PropertiesBlock
   - Sort by key_min for L1+ (already non-overlapping)
   - Handle L0 segments separately (overlapping, must be scanned)

5. **Segment-level diff algorithm**
   - Walk two manifests in lockstep
   - Skip segments with matching content hashes
   - Descend into segments with differing hashes for entry-level diff
   - Handle key range gaps (bulk additions/removals)

6. **Inherited layer segment identity**
   - COW-shared segments have the same `SegmentId` and content hash on both branches
   - After materialization, segments are rebuilt → new IDs, but content hash matches if data unchanged

### Phase 3: Change Manifests (Tier 2)

**Goal**: Enable O(C) diff using explicit change tracking.

7. **Record change manifests on write**
   - After each write batch commits, record `ChangeManifest` as `_system_` KV entry
   - Keys: `_system_:changelog:{branch_id}:{version}`
   - Best-effort (log warning on failure, never block writes)

8. **Change-manifest diff path**
   - Collect manifests between merge base version and branch tip
   - Deduplicate to net changes per branch
   - Three-way classify using net change sets
   - Fall back to segment diff for pre-manifest data

9. **Manifest pruning**
   - Prune manifests below the oldest active merge base version
   - Participate in normal compaction lifecycle

### Phase 4: Streaming and Memory Optimization

10. **Streaming diff iterator**
    - Replace HashMap-based comparison with streaming merge of sorted iterators
    - Emit diff entries one at a time instead of materializing entire result
    - Enables early termination (e.g., `LIMIT` on diff queries)

11. **Segment-level parallel diff**
    - Independent segments can be diffed in parallel (rayon)
    - Especially beneficial for L1+ non-overlapping segments

---

## Complexity Comparison

| Scenario | Current | Phase 1 (COW) | Phase 2 (Segment) | Phase 3 (Manifest) |
|----------|---------|---------------|-------------------|-------------------|
| Child vs parent, W writes | O(N) | **O(W)** | O(W) | O(W) |
| Siblings, W writes each | O(N) | O(N) fallback | **O(S + Δ)** | **O(C_a + C_b)** |
| Diverged branches, heavy compaction | O(N) | O(N) fallback | **O(S + Δ)** | **O(C_a + C_b)** |
| Unrelated branches | O(N) | O(N) fallback | O(N) worst case | N/A (no merge base) |

Where: N = total entries, W = entries written since fork, S = number of segments, Δ = entries in changed segments, C = change manifest entries.

---

## Why Not Prolly Trees?

Dolt's Prolly Tree approach delivers the theoretical optimum (O(Δ × log N) with history-independent structural sharing). However, it requires **replacing the storage engine's fundamental data structure**. Strata's LSM-based segment architecture is well-suited for high write throughput and the tiered storage project already underway. A Prolly Tree rewrite would:

1. Conflict with the tiered storage project's segment-based design
2. Sacrifice LSM write amplification advantages for workloads that are write-heavy
3. Require rewriting compaction, recovery, mmap acceleration, and the entire read path

The segment-hash approach captures ~90% of the benefit (O(S + Δ) vs O(Δ × log N)) with ~10% of the implementation cost, because it builds on the existing segment infrastructure rather than replacing it.

Prolly Trees remain the right choice if Strata ever moves to a content-addressed storage engine. For now, the segment-hash approach is the pragmatic path.

---

## References

- [Efficient Diff on Prolly-Trees (DoltHub, 2020)](https://www.dolthub.com/blog/2020-06-16-efficient-diff-on-prolly-trees/)
- [Prolly Trees Architecture (DoltHub Docs)](https://docs.dolthub.com/architecture/storage-engine/prolly-tree)
- [Dolt Diff vs SQLite Diff — O(d) vs O(n) (DoltHub, 2022)](https://www.dolthub.com/blog/2022-06-03-dolt-diff-vs-sqlite-diff/)
- [Three-Way Merge in a SQL Database (DoltHub, 2024)](https://www.dolthub.com/blog/2024-06-19-threeway-merge/)
- [Fast Merge via Node-Level Prolly Tree Operations (DoltHub, 2025)](https://www.dolthub.com/blog/2025-07-16-announcing-fast-merge/)
- [lakeFS Versioning Internals — Graveler Ranges](https://docs.lakefs.io/understand/how/versioning-internals/)
- [Concrete Graveler: Committing Data to Pebble SSTables (lakeFS, 2023)](https://lakefs.io/blog/concrete-graveler-committing-data-to-pebbledb-sstables/)
- [Project Nessie Commit Kernel](https://projectnessie.org/develop/kernel/)
- [TerminusDB Internals: Change is Gonna Come](https://terminusdb.com/blog/terminusdb-internals-2/)
- [Git Internals — Tree Diff (tree-diff.c)](https://github.com/git/git/blob/master/tree-diff.c)
- [Git Internals — Merge Base (commit-reach.c)](https://github.com/git/git/blob/master/commit-reach.c)
- [Merkle-CRDTs: Merkle-DAGs meet CRDTs (arXiv, 2020)](https://arxiv.org/pdf/2004.00107)
- [Decibel: The Relational Dataset Branching System (VLDB, 2016)](http://www.vldb.org/pvldb/vol9/p624-maddox.pdf)
