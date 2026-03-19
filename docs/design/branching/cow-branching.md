# Copy-on-Write Branching: O(1) Fork via Inherited Segment Layers

**Date:** 2026-03-19
**Priority:** P0
**Prerequisites:** [Current State Analysis](current-state.md), [Three-Way Merge Design](implementation-design.md)
**Status:** Design

---

## 1. Problem Statement

Branch creation (`fork_branch`) is currently O(n) where n = total entries on the source branch. It deep-copies every key-value pair, rewriting branch_id prefixes and assigning new commit_ids. For a branch with 1M keys, this takes seconds and doubles storage consumption.

This was an acceptable tradeoff during the storage engine rewrite (Epics 26-31), but it is not the right end state. Branching is a fundamental Strata primitive — Strata AI creates branches per-conversation, per-hypothesis, per-experiment. O(n) fork makes this prohibitively expensive at scale.

**Goal:** O(1) branch creation. A fork should take constant time regardless of how much data the source branch holds.

---

## 2. Design Constraints

1. **Branch_id stays in key encoding.** The key format `branch_id(16B) || space || type_tag || user_key || !commit_id` guarantees branch isolation at the storage level. Every key is physically namespaced to its branch. This is non-negotiable — branches in Strata are not like branches in git. Activity on one branch must never pollute another.

2. **The branch DAG provides lineage tracking.** The `_branch_dag` graph on `_system_` already records fork/merge events with `fork_version`. This is the source of truth for parent-child relationships and layer resolution.

3. **Three-way merge must continue to work.** The merge base computation (reading ancestor state at `fork_version`) must function correctly with COW. The ancestor state lives in shared parent segments, not deep-copied child segments.

4. **Compaction/GC must be safe.** Shared segments must not be deleted while any branch references them. Reference counting is required.

5. **Read performance must not regress unboundedly.** The inherited layer chain (A←B←C←D) must be bounded. Background materialization collapses chains when they grow too deep.

---

## 3. Architecture Overview

```
CURRENT (O(n) fork):
┌──────────┐     fork_branch()     ┌──────────┐
│  main    │ ──── deep copy ────→  │ feature  │
│ segments │    rewrites all keys  │ segments │
│  (100MB) │                       │  (100MB) │
└──────────┘                       └──────────┘
  Total disk: 200MB
  Fork time: O(n)

PROPOSED (O(1) fork):
┌──────────┐     fork_branch()     ┌──────────┐
│  main    │ ──── share refs ────→ │ feature  │
│ segments │    Arc<KVSegment>     │ inherited │
│  (100MB) │                       │  layers  │
└──────────┘                       └──────────┘
  Total disk: 100MB (shared)
  Fork time: O(1)

  Feature's new writes go to its own memtable → segments.
  Reads walk: own segments → inherited layers (with branch_id rewrite).
```

### Core Idea

At fork time, instead of copying data, the child branch receives a list of **inherited segment layers** — references (`Arc<KVSegment>`) to the parent's on-disk segments at that point in time. Reads on the child walk its own data first, then fall through to inherited layers with branch_id prefix rewriting. Writes always go to the child's own memtable/segments, never to inherited data.

---

## 4. Data Structures

### 4.1 InheritedLayer

```rust
/// A layer of segments inherited from an ancestor branch at fork time.
///
/// The child reads these segments as if the data belonged to it, by
/// rewriting the branch_id prefix during key comparison.
struct InheritedLayer {
    /// The branch_id of the ancestor that owns these segments.
    source_branch_id: BranchId,

    /// The global version at fork time. Only entries with
    /// commit_id <= fork_version are visible through this layer.
    fork_version: u64,

    /// Snapshot of the ancestor's segments at fork time.
    /// These are Arc references — the segments are shared, not copied.
    segments: Arc<SegmentVersion>,

    /// Materialization status.
    status: LayerStatus,
}

enum LayerStatus {
    /// Layer is active — reads fall through to it.
    Active,
    /// Background materialization is in progress.
    Materializing,
    /// Materialization complete — layer can be removed.
    Materialized,
}
```

### 4.2 Extended BranchState

```rust
struct BranchState {
    // --- existing fields ---
    active: Memtable,
    frozen: Vec<Arc<Memtable>>,
    version: ArcSwap<SegmentVersion>,      // branch's OWN segments
    min_timestamp: AtomicU64,
    max_timestamp: AtomicU64,
    compact_pointers: Vec<Option<Vec<u8>>>,

    // --- new fields ---
    /// Inherited segment layers, ordered nearest-ancestor-first.
    /// Empty for branches that were created without forking (e.g., main).
    inherited_layers: Vec<InheritedLayer>,
}
```

### 4.3 Segment Reference Counting

```rust
/// Global registry of segment references across all branches.
/// A segment file cannot be deleted until its refcount drops to zero.
struct SegmentRefRegistry {
    /// segment_id → number of branches (including inherited layers)
    /// that reference this segment.
    refs: DashMap<SegmentId, AtomicUsize>,
}

impl SegmentRefRegistry {
    fn increment(&self, segment_id: &SegmentId) { ... }
    fn decrement(&self, segment_id: &SegmentId) -> bool { /* true if zero */ }
}
```

Each `Arc<KVSegment>` is reference-counted by Rust's Arc, but we also need logical tracking at the storage layer to prevent file deletion during compaction. When a branch inherits segments, the registry increments the refcount for each segment. When an inherited layer is materialized (or the child branch is deleted), the registry decrements.

---

## 5. Fork Path (O(1))

### 5.1 Algorithm

```
fork_branch(source, destination):

1. Verify source exists, destination doesn't
2. Create destination branch (BranchIndex)
3. Register source spaces on destination (SpaceIndex)
4. Capture fork_version = db.current_version()
5. Snapshot source's current SegmentVersion (Arc::clone)
6. Create InheritedLayer {
       source_branch_id: source_id,
       fork_version,
       segments: snapshot,
       status: Active,
   }
7. Attach layer to destination's BranchState.inherited_layers
8. Increment segment refcounts for all segments in the snapshot
9. Record fork event in DAG with fork_version
10. Reload vector backends (existing behavior)
```

**What's NOT done:** No entry scanning, no key rewriting, no transactions. Steps 5-8 are O(S) where S = number of segment files (typically 10-50), not O(n) where n = number of entries (potentially millions).

### 5.2 Memtable Handling

The source branch's active and frozen memtables are NOT shared. Only flushed segments are inherited. This is deliberate:

- Memtables are mutable (the active one) or pending-flush (frozen ones). Sharing them would require synchronization.
- Memtable data is ephemeral and typically small relative to segments.
- If the source has unflushed data at fork time, those entries won't be visible to the child until the source flushes and the child's inherited layer snapshot is updated. **We accept this trade-off.** Alternatively, we can force a flush on the source before forking — this adds O(memtable_size) but is bounded by write_buffer_size (typically 64MB).

**Recommended approach:** Force-flush the source's memtable before capturing the segment snapshot. This ensures the child sees the source's complete state at fork time, consistent with the current deep-copy behavior.

```rust
// In fork_branch, before snapshot:
self.flush_memtable(source_id)?;  // flush active → frozen → segment
let snapshot = Arc::clone(&source_branch.version.load());
```

---

## 6. Read Path (Multi-Layer)

### 6.1 Point Lookup

```
get(child_branch_id, key):

1. Search child's active memtable
2. Search child's frozen memtables (newest first)
3. Search child's own segments (L0 → L6)
4. For each inherited layer (nearest ancestor first):
   a. Rewrite key: replace child_branch_id with layer.source_branch_id
   b. Search layer.segments (L0 → L6)
   c. Filter: only consider entries with commit_id <= layer.fork_version
   d. If found → rewrite result key back to child_branch_id, return
5. Not found → return None
```

### 6.2 Key Rewriting

The key format is: `branch_id(16B) || space || type_tag || user_key || !commit_id`

Branch_id is a fixed-size 16-byte prefix. Rewriting is a simple memcpy of the first 16 bytes — no encoding/decoding, no allocation for the common case.

```rust
/// Rewrite a key's branch_id prefix for inherited layer lookup.
/// Returns a stack-allocated key with the source branch_id.
fn rewrite_branch_id(key: &[u8], source_branch_id: &BranchId) -> [u8; MAX_KEY_LEN] {
    let mut rewritten = [0u8; MAX_KEY_LEN];
    let len = key.len();
    rewritten[..16].copy_from_slice(source_branch_id.as_bytes());
    rewritten[16..len].copy_from_slice(&key[16..]);
    rewritten
}
```

For bloom filter lookups, the typed_key_prefix used for hashing includes the branch_id. So we must also rewrite for bloom filter probes:

```rust
fn probe_inherited_bloom(segment: &KVSegment, typed_key: &[u8], source_branch_id: &BranchId) -> bool {
    let rewritten = rewrite_typed_key_prefix(typed_key, source_branch_id);
    segment.bloom_may_contain(&rewritten)
}
```

### 6.3 Version Filtering

Inherited layers have a `fork_version` cutoff. When scanning an inherited segment, entries with `commit_id > fork_version` must be skipped — they were written to the parent AFTER the fork and should not be visible to the child.

This integrates naturally with the existing MVCC machinery. The `MvccIterator` already accepts a `max_version` parameter. For inherited layer reads, we pass `fork_version` instead of `u64::MAX`:

```rust
// Reading from inherited layer:
let mvcc = MvccIterator::new(merge_iter, layer.fork_version);
```

For point lookups in `scan_block_for_key`, the `snapshot_commit` parameter serves the same purpose:

```rust
// In the inherited layer read path:
segment.scan_block_for_key(&index_entry, &rewritten_key, layer.fork_version)
```

### 6.4 Range Scans and Iteration

Range scans must merge results across all layers. The existing `MergeIterator` handles multi-source merging. For inherited layers, we add iterators with branch_id rewriting:

```rust
fn create_full_merge_iterator(
    &self,
    branch: &BranchState,
    prefix: &[u8],
    max_version: u64,
) -> MvccIterator {
    let mut sources = Vec::new();

    // 1. Own data sources (memtable + segments)
    sources.extend(self.create_branch_iterators(branch, prefix));

    // 2. Inherited layer sources
    for layer in &branch.inherited_layers {
        let rewritten_prefix = rewrite_prefix(prefix, &layer.source_branch_id);
        let layer_iters = self.create_segment_iterators(
            &layer.segments, &rewritten_prefix
        );
        // Wrap each iterator with branch_id rewriting + version filter
        for iter in layer_iters {
            sources.push(RewritingIterator::new(
                iter,
                layer.source_branch_id,
                branch_id,         // rewrite back to child
                layer.fork_version, // version cutoff
            ));
        }
    }

    let merge = MergeIterator::new(sources);
    MvccIterator::new(merge, max_version)
}
```

### 6.5 Performance Characteristics

| Operation | Current (deep copy) | COW (no materialization) | COW (after materialization) |
|-----------|-------------------|-------------------------|---------------------------|
| Point lookup (own data) | O(log n) | O(log n) | O(log n) |
| Point lookup (inherited) | N/A | O(L × log n) per layer | O(log n) |
| Range scan | O(n) | O(n) across layers | O(n) |
| Fork | O(n) | O(S) segments | O(S) segments |

Where L = number of inherited layers, S = number of segment files, n = entries.

---

## 7. Write Path (Unchanged)

All writes go to the child branch's own memtable, with the child's branch_id in the key encoding. This is identical to the current behavior:

```
put(child_branch_id, key, value):
  1. Encode key with child_branch_id prefix
  2. Insert into child's active memtable
  3. Eventually flush → child's own segments
```

A write to a key that exists in an inherited layer effectively shadows it. Future reads will find the child's version first (in its own memtable/segments) before reaching the inherited layer.

Deletes work the same way — a tombstone written to the child's memtable shadows the inherited entry.

---

## 8. Fork Chains (A ← B ← C)

When branch C is forked from branch B, which was itself forked from A:

```
Branch A (main):
  own_segments: [seg_1, seg_2, seg_3]
  inherited_layers: []

Branch B (forked from A at version 100):
  own_segments: [seg_4, seg_5]
  inherited_layers: [
    { source: A, fork_version: 100, segments: [seg_1, seg_2, seg_3] }
  ]

Branch C (forked from B at version 200):
  own_segments: [seg_6]
  inherited_layers: [
    { source: B, fork_version: 200, segments: [seg_4, seg_5] },     // nearest
    { source: A, fork_version: 100, segments: [seg_1, seg_2, seg_3] } // transitive
  ]
```

**Key design decision:** When forking C from B, we copy B's inherited layers into C's list (with the same fork_version cutoffs). C gets a flattened view of its full ancestry. This means:

- Reads on C walk: own → B's segments (at v200) → A's segments (at v100)
- No recursive resolution at read time — the layer list is pre-computed at fork time
- Fork is still O(S_total) where S_total = sum of segments across all ancestor layers

### 8.1 Chain Depth Bound

Unbounded chain depth degrades read performance. We enforce a maximum:

```rust
const MAX_INHERITED_LAYERS: usize = 4;
```

When a fork would exceed this limit, we have two options:

1. **Eager materialization:** Force-materialize the deepest layer(s) before forking (makes fork O(n) for deep chains, but only when limit is hit).
2. **Background materialization with temporary over-limit:** Allow the fork, schedule immediate background materialization, accept temporarily degraded reads.

**Recommended:** Option 2. Fork is always fast. Background materialization fires immediately for chains exceeding the limit.

---

## 9. Background Materialization

Materialization converts inherited entries into the child's own segments, collapsing a layer.

### 9.1 Algorithm

```
materialize(child_branch_id, layer_index):

1. Set layer.status = Materializing
2. Create a segment builder
3. For each entry in layer.segments where commit_id <= layer.fork_version:
   a. Check if child already has a newer version of this key
      (in own segments or closer inherited layers)
   b. If not shadowed:
      - Rewrite branch_id to child_branch_id
      - **Preserve original commit_id** (do NOT assign new version)
      - Add to segment builder
4. Flush segment builder → new segment files for child

**Critical: Commit_ids must be preserved, not reassigned.** Materialization only rewrites the branch_id prefix. If new commit_ids were assigned, the ancestor state at `fork_version` would become invisible to MVCC (new commit_id > fork_version → filtered out), breaking merge base computation. Preserving original commit_ids works because: (a) the child's own writes at higher commit_ids naturally shadow the materialized entries via MVCC, and (b) reading at `fork_version` correctly returns the materialized entries as the ancestor state.
5. Add new segments to child's SegmentVersion (ArcSwap)
6. Remove layer from inherited_layers
7. Decrement segment refcounts for the materialized layer's segments
8. Set layer.status = Materialized
```

### 9.2 Scheduling

Materialization is triggered by:

1. **Chain depth exceeds `MAX_INHERITED_LAYERS`** — materialize deepest layer immediately
2. **Background compaction cycle** — piggyback materialization on the compaction scheduler
3. **Explicit request** — admin API to force materialization

Materialization is asynchronous and does not block reads or writes. During materialization, the layer remains in the read path. After completion, the atomic ArcSwap adds the materialized segments to the child's own version, and the layer is removed from `inherited_layers`.

### 9.3 Interaction with Compaction

Materialization produces segments at L0 (unsorted, like a memtable flush). Normal L0→L1 compaction then integrates them into the tiered structure. This reuses the existing compaction pipeline without special-casing.

---

## 10. Segment Lifecycle and GC

### 10.1 The Problem

With shared segments, we cannot delete a segment file when the owning branch compacts it away — another branch may still reference it through an inherited layer.

### 10.2 Solution: Reference Counting

Each segment file has a logical reference count in `SegmentRefRegistry`:

| Event | Refcount Change |
|-------|----------------|
| Segment created (flush/compaction) | +1 (owning branch) |
| Segment inherited (fork) | +1 per inheriting branch |
| Segment replaced (compaction on owner) | -1 from owner |
| Layer materialized | -1 from child |
| Branch deleted | -1 for each owned + inherited segment |

When refcount reaches 0, the segment file is eligible for deletion.

### 10.3 Implementation

```rust
impl SegmentedStore {
    /// Delete a segment file only if no branch references it.
    fn try_delete_segment(&self, segment_id: &SegmentId) -> bool {
        if self.ref_registry.decrement(segment_id) {
            // Refcount is zero — safe to delete
            std::fs::remove_file(self.segment_path(segment_id)).ok();
            true
        } else {
            false
        }
    }
}
```

### 10.4 Persistence

The refcount registry must survive restarts. Options:

1. **Reconstruct from branch manifests** — each branch's manifest lists its own segments and inherited layers. On recovery, scan all manifests to rebuild refcounts. This is O(branches × segments) but only happens at startup.

2. **Persist refcounts to a dedicated file** — more efficient but adds another file to maintain.

**Recommended:** Option 1. Branch manifests already exist for segment recovery. Add inherited layer metadata to the manifest format. Refcounts are derived, not stored.

### 10.5 Branch Manifest Extension

```
# Current manifest format (per branch):
level:0 segment:abc123.sst
level:1 segment:def456.sst

# Extended format:
level:0 segment:abc123.sst
level:1 segment:def456.sst
inherited:0 source:branch_id_hex fork_version:100 level:0 segment:ghi789.sst
inherited:0 source:branch_id_hex fork_version:100 level:1 segment:jkl012.sst
inherited:1 source:branch_id_hex fork_version:50 level:0 segment:mno345.sst
```

---

## 11. Interaction with Three-Way Merge

### 11.1 Merge Base Computation

With deep copy, the child branch at `fork_version` holds a full copy of the ancestor state. With COW, the ancestor state lives in the inherited layer's segments (which belong to the parent).

The merge base computation changes subtly:

**Deep copy (current):** Read child branch at `fork_version` → MVCC returns the deep-copied entries.

**COW:** Read child branch at `fork_version` → child's own segments have nothing (no deep copy happened). The data is in the inherited layer, which is reached via the multi-layer read path.

**This just works.** The multi-layer read path already handles this — reading the child at `fork_version` will find no entries in the child's own data, fall through to the inherited layer, and return the parent's entries (filtered by `commit_id <= fork_version`). The ancestor state is correctly reconstructed without any special-casing in the merge code.

### 11.2 Merge Execution

After three-way merge computes the changes to apply, it writes puts and deletes to the target branch's memtable. This is unchanged — writes always go to the branch's own data.

### 11.3 Post-Merge State

After merging source→target, the target has new entries in its own segments that shadow some inherited entries. The inherited layer is NOT removed by merge — it still provides the base data for keys not touched by the merge. Background materialization can collapse it later.

---

## 12. Interaction with Existing Operations

### 12.1 Branch Deletion

When a branch with inherited layers is deleted:
1. Decrement refcounts for all segments in inherited layers
2. Delete the branch's own segments (if refcount allows)
3. Remove branch state from `DashMap`

### 12.2 Diff

`diff_branches` reads all keys from both branches. With COW, this naturally walks inherited layers on both sides. No change needed — the multi-layer read path is transparent to the diff algorithm.

### 12.3 Listing

`list_branch_inner`, `list_by_type`, etc. all build merge iterators. The extended `create_full_merge_iterator` (Section 6.4) includes inherited layer sources. Existing listing operations work without modification.

### 12.4 Time Travel

`get_versioned_from_branch(branch, key, max_version)` already uses MVCC with a version cutoff. For inherited layers, the effective cutoff is `min(max_version, layer.fork_version)`. This ensures time-travel reads don't see parent data written after the fork.

### 12.5 Vector Search

Vector backends are loaded from segments. Inherited segments contain vector data under the parent's branch_id. The vector reload after fork (`post_merge_reload_vectors_from`) must be updated to index inherited segments with branch_id rewriting, or vector search must explicitly walk inherited layers.

**Recommended:** Materialize vector data eagerly at fork time. Vector search requires specialized index structures (HNSW graphs) that don't support transparent branch_id rewriting. This means fork is O(V) where V = vector entries, but vectors are typically a small fraction of total data. Non-vector data remains O(1).

---

## 13. Migration Path

### 13.1 Backward Compatibility

- **Existing branches** (created with deep copy) have `inherited_layers = []`. They work exactly as before.
- **New branches** (created after COW ships) use inherited layers.
- **Mixed operation** is safe — the read path checks `inherited_layers` and only walks them if non-empty.

### 13.2 Format Version

The branch manifest format must be versioned to distinguish old (no inherited layers) from new (with inherited layers). Extend the manifest header:

```
# manifest_version:2
# branch_id:abc123
level:0 segment:...
inherited:0 source:... fork_version:...
```

Old manifests (no `manifest_version` or version 1) are read as before.

### 13.3 Rollout Steps

1. **Add `InheritedLayer` and `SegmentRefRegistry` to storage** — no behavioral change yet
2. **Add inherited layer persistence to manifests** — backward compatible (empty layers)
3. **Update the read path** to walk inherited layers
4. **Switch `fork_branch` from deep copy to COW** — the actual cutover
5. **Add background materialization** — chain depth management
6. **Remove deep-copy code** — cleanup after validation

---

## 14. Implementation Plan

### Phase A: Storage Foundation

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Define `InheritedLayer`, `LayerStatus` | `segmented/mod.rs` | Struct with source_branch_id, fork_version, segments, status |
| 2 | Add `inherited_layers: Vec<InheritedLayer>` to `BranchState` | `segmented/mod.rs` | Default empty |
| 3 | Implement `SegmentRefRegistry` | `segmented/ref_registry.rs` (new) | DashMap<SegmentId, AtomicUsize>, increment/decrement/is_zero |
| 4 | **Wire refcounting into compaction** | `segmented/compaction.rs` | Replace `std::fs::remove_file()` with `try_delete_segment()` that checks refcount. **MUST ship before Phase C** — without this, parent compaction deletes shared segment files |
| 5 | Extend branch manifest format | `segmented/mod.rs` | Write/read inherited layer metadata |
| 6 | Rebuild refcounts on recovery | `segmented/mod.rs` | Scan all manifests at startup |

### Phase B: Multi-Layer Read Path

| Step | What | File | Detail |
|------|------|------|--------|
| 6 | Implement `rewrite_branch_id` for key/prefix rewriting | `key_encoding.rs` | Fixed 16-byte prefix swap |
| 7 | Implement `RewritingIterator` | `merge_iter.rs` or new file | Wraps segment iterator, rewrites branch_id + version filter |
| 8 | Update `create_merge_iterator` to include inherited layers | `segmented/mod.rs` | Used by list/range operations |
| 9 | Update point lookup to walk inherited layers | `segmented/mod.rs` | `get` falls through to inherited |
| 10 | Update `block_data_end`, bloom probes for inherited keys | `segment.rs` | Rewrite typed_key_prefix for bloom |

### Phase C: O(1) Fork

| Step | What | File | Detail |
|------|------|------|--------|
| 11 | Implement COW fork path | `branch_ops.rs` + `segmented/mod.rs` | Flush source → snapshot → create inherited layer → increment refs |
| 12 | Handle fork chains (flatten ancestor layers) | `segmented/mod.rs` | Copy parent's inherited_layers into child |
| 13 | Update vector reload for inherited segments | `primitives/vector/store.rs` | Eager materialize vectors or walk layers |
| 14 | Remove deep-copy code path | `branch_ops.rs` | Delete O(n) loop |

### Phase D: Background Materialization

| Step | What | File | Detail |
|------|------|------|--------|
| 15 | Implement `materialize_layer` | `segmented/mod.rs` | Iterate inherited → rewrite → build segments |
| 16 | Shadow detection during materialization | `segmented/mod.rs` | Skip entries already in child's own data |
| 17 | Schedule materialization on chain depth violation | `database/mod.rs` | Trigger when inherited_layers.len() > MAX |
| 18 | Integrate with compaction scheduler | `database/mod.rs` | Piggyback on existing compaction loop |

### Phase E: Cleanup and Hardening

| Step | What | File | Detail |
|------|------|------|--------|
| 20 | Branch deletion decrements inherited refs | `segmented/mod.rs` | Walk inherited_layers, decrement all |
| 21 | Time-travel version clamping for inherited layers | `segmented/mod.rs` | `min(user_version, fork_version)` |
| 22 | Comprehensive test suite | Tests | See Section 15 |

> **Note:** Safe segment deletion via refcount was moved to Phase A (step 4) because it is a prerequisite for COW — without it, parent compaction deletes shared segment files.

---

## 15. Test Plan

### Unit Tests

| Test | What It Verifies |
|------|-----------------|
| `cow_fork_creates_inherited_layer` | Fork creates InheritedLayer with correct source_branch_id and fork_version |
| `cow_fork_no_data_copy` | Child's own segments are empty after fork |
| `cow_fork_o1_timing` | Fork of 1M-key branch completes in < 100ms |
| `cow_read_falls_through_to_inherited` | Get on child returns parent's data via inherited layer |
| `cow_write_shadows_inherited` | Put on child shadows inherited entry |
| `cow_delete_shadows_inherited` | Delete tombstone on child hides inherited entry |
| `cow_version_filter` | Parent writes after fork are invisible to child |
| `cow_range_scan_merges_layers` | Range scan yields union of own + inherited data |
| `cow_fork_chain_3_levels` | A←B←C: reads on C walk all three layers correctly |
| `cow_fork_chain_flattened` | C's inherited_layers contains both B and A layers |
| `cow_materialization_collapses_layer` | After materialization, layer is removed, data in own segments |
| `cow_materialization_skips_shadowed` | Materialization skips entries already in child's own data |
| `cow_refcount_prevents_deletion` | Compaction on parent doesn't delete segments inherited by child |
| `cow_refcount_allows_deletion` | After materialization + parent compaction, orphan segments are deleted |
| `cow_branch_delete_decrements_refs` | Deleting child with inherited layers decrements all refcounts |
| `cow_manifest_roundtrip` | Inherited layers survive write → recovery |

### Integration Tests

| Test | What It Verifies |
|------|-----------------|
| `cow_three_way_merge_works` | Fork (COW) → modify both → merge correctly computes ancestor from inherited layer |
| `cow_merge_base_from_inherited` | Merge base reads ancestor state through inherited layer, not deep copy |
| `cow_repeated_merge_after_cow_fork` | Fork → merge → modify → merge again (merge_version as base) |
| `cow_diff_across_inherited_layers` | Diff sees all keys including inherited ones |
| `cow_compaction_with_inherited_segments` | Compaction on parent doesn't break child reads |
| `cow_concurrent_fork_and_write` | Fork while parent is actively being written to |
| `cow_time_travel_respects_fork_version` | Time travel at version > fork_version doesn't leak parent's later writes |

### Stress Tests

| Test | What It Verifies |
|------|-----------------|
| `cow_fork_1m_keys_under_100ms` | Performance target met |
| `cow_chain_depth_triggers_materialization` | Forking beyond MAX_INHERITED_LAYERS triggers background materialization |
| `cow_100_branches_shared_segments` | 100 branches forked from same parent, all read correctly |

---

## 16. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Read amplification from deep chains | Medium | Medium | MAX_INHERITED_LAYERS bound + background materialization |
| Segment file leak (refcount bug) | Low | High | Reconstruct refcounts from manifests on startup; periodic audit |
| Bloom filter FPR increase from branch_id rewriting | Low | Low | Bloom probe uses correctly rewritten prefix; no FPR change |
| Vector search incompatibility | Certain | Medium | Eager vector materialization at fork time; bounded by vector count |
| Manifest format migration | Low | Medium | Version header, backward-compatible reader |
| Memory overhead from inherited Arc refs | Low | Low | Arc<KVSegment> is ~200 bytes; 100 inherited segments = 20KB |
| Race between materialization and compaction | Medium | High | Materialization holds read lock on layer; compaction respects refcounts |

---

## 17. Alternatives Considered

### 17.1 Remove branch_id from Key Encoding

**Rejected.** Branch_id in key encoding is the fundamental isolation guarantee. Without it, branch isolation would depend on correct routing logic rather than physical separation. A bug in the routing layer could cause cross-branch pollution. The current design makes this impossible by construction.

### 17.2 Lazy Prefix Rewriting at Compaction Time

Instead of rewriting branch_id at read time, defer rewriting to compaction. Inherited segments keep the parent's branch_id; compaction eventually rewrites them under the child's namespace.

**Rejected.** This creates a window where the child's data lives under two different branch_ids, complicating every operation that scans by branch_id prefix. The read-time rewriting approach (Section 6) is simpler and more predictable.

### 17.3 Virtual Branch_id Mapping

Maintain a mapping table: "child branch_id X inherits from parent branch_id Y." Reads check the mapping and search under both branch_ids.

**Rejected.** This is functionally equivalent to our inherited layers approach but less explicit. The inherited layer struct carries all necessary metadata (fork_version, segments snapshot) in one place.

### 17.4 Full Copy-on-Write with Page-Level Sharing

Like a filesystem COW (ZFS/btrfs): share data blocks, copy only on modification.

**Rejected.** This would require rewriting the entire segment format for block-level sharing. Our approach (segment-level sharing with entry-level materialization) is far simpler and achieves the same O(1) fork goal with bounded read overhead.

---

## Sources

- [Current State Analysis](current-state.md) — audit of existing branching implementation
- [Three-Way Merge Design](implementation-design.md) — merge algorithm that must work with COW
- [Implementation Plan](implementation-plan-epics.md) — phased plan where COW was Phase 3 future work (now P0)
