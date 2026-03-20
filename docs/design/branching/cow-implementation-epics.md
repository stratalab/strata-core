# COW Branching: Implementation Epics

**Date:** 2026-03-19
**Prerequisites:** Format version consolidation (#1596, in progress), Three-Way Merge (Phase 1 from `implementation-plan-epics.md`)
**Design:** [`cow-branching.md`](cow-branching.md)
**Priority:** P0

---

## Overview

Four epics transform branch fork from O(n) deep-copy to O(memtable) COW with inherited segment layers. Each epic is independently testable and leaves the system in a working state.

**Dependency chain:**

```
Epic A: Storage Foundation ──→ Epic B: Multi-Layer Read Path ──→ Epic C: O(1) Fork ──→ Epic D: Background Materialization
```

Three-way merge (Phase 1 from `implementation-plan-epics.md`) can proceed in parallel with Epics A-B. Epic C depends on three-way merge being complete (COW fork + three-way merge must ship together since the old deep-copy merge relied on deep-copied ancestor data).

**Total estimate:** ~4-5 weeks

---

## Epic A: Storage Foundation (~1 week)

**Goal:** Add `InheritedLayer`, `SegmentRefRegistry`, and manifest persistence for inherited layers. No behavioral change — all branches have `inherited_layers = []`. The critical safety net: refcounted segment deletion prevents shared-segment corruption when Epic C ships.

### A.1 Define InheritedLayer and LayerStatus

**File:** `crates/storage/src/segmented/mod.rs`

```rust
/// A layer of segments inherited from an ancestor branch at fork time.
struct InheritedLayer {
    /// The branch_id of the ancestor that owns these segments.
    source_branch_id: BranchId,
    /// Only entries with commit_id <= fork_version are visible through this layer.
    fork_version: u64,
    /// Snapshot of the ancestor's segments at fork time.
    segments: Arc<SegmentVersion>,
    /// Materialization status.
    status: LayerStatus,
}

enum LayerStatus {
    Active,
    Materializing,
    Materialized,
}
```

Add `inherited_layers: Vec<InheritedLayer>` to `BranchState`. Default empty.

### A.2 Implement SegmentRefRegistry

**File:** `crates/storage/src/segmented/ref_registry.rs` (new)

```rust
/// Global registry of segment references across all branches.
/// A segment file cannot be deleted until its refcount drops to zero.
struct SegmentRefRegistry {
    refs: DashMap<SegmentId, AtomicUsize>,
}

impl SegmentRefRegistry {
    fn increment(&self, id: &SegmentId);
    fn decrement(&self, id: &SegmentId) -> bool; // true if zero
    fn is_referenced(&self, id: &SegmentId) -> bool;
    fn rebuild_from_branches(&self, branches: &[BranchState]); // recovery
}
```

`SegmentId` is derived from file path hash (same as `file_id` used by block cache).

### A.3 Wire refcounting into compaction

**File:** `crates/storage/src/segmented/compaction.rs`

Replace every `std::fs::remove_file(segment_path)` with:

```rust
if self.ref_registry.decrement(&segment_id) {
    std::fs::remove_file(segment_path).ok();
    self.block_cache.invalidate_file(file_id);
}
// else: another branch still references this segment — leave it
```

When compaction creates new segments, increment their refcount. When the owning branch's old segments are replaced, decrement.

**This is the safety-critical step.** Without it, parent compaction deletes segments that a forked child still references.

### A.4 Extend manifest format for inherited layers

**File:** `crates/storage/src/manifest.rs`

Replace the current manifest format entirely (no backward compat per decision). New format:

```
STRAMFST_V2
branch_id:<hex>
level:0 segment:<filename> min_key:<hex> max_key:<hex> size:<bytes>
level:1 segment:<filename> min_key:<hex> max_key:<hex> size:<bytes>
...
inherited:0 source:<branch_id_hex> fork_version:<u64>
inherited:0 level:0 segment:<filename>
inherited:0 level:1 segment:<filename>
inherited:1 source:<branch_id_hex> fork_version:<u64>
inherited:1 level:0 segment:<filename>
...
compact_pointer:1 <hex>
compact_pointer:2 <hex>
```

Write: serialize `BranchState` including `inherited_layers`.
Read: parse back into `BranchState` with inherited layers.

### A.5 Rebuild refcounts on recovery

**File:** `crates/storage/src/segmented/mod.rs`

During `recover_segments()`, after loading all branch manifests:

1. Initialize empty `SegmentRefRegistry`
2. For each branch's own segments: increment refcount
3. For each branch's inherited layer segments: increment refcount
4. Result: every referenced segment has correct refcount

### A.6 Tests

| Test | Verifies |
|------|----------|
| `ref_registry_increment_decrement` | Basic refcount operations |
| `ref_registry_decrement_returns_true_at_zero` | Deletion signal |
| `ref_registry_concurrent_access` | Thread-safe DashMap usage |
| `compaction_respects_refcount` | Compaction skips deletion when refcount > 0 |
| `compaction_deletes_when_refcount_zero` | Normal deletion still works |
| `manifest_roundtrip_with_inherited_layers` | Write manifest with inherited layers, read back, verify equality |
| `manifest_roundtrip_empty_inherited` | Branches with no inherited layers serialize/deserialize correctly |
| `recovery_rebuilds_refcounts` | After recovery, refcounts match expected state |
| All existing storage tests pass | No regression |

---

## Epic B: Multi-Layer Read Path (~1.5 weeks)

**Goal:** Point lookups and range scans transparently walk inherited layers with branch_id rewriting and version filtering. No fork behavior change yet — layers are empty, so the new code paths are never hit in production. But they can be tested by manually attaching inherited layers in tests.

### B.1 Implement branch_id rewriting for keys

**File:** `crates/storage/src/key_encoding.rs`

```rust
/// Rewrite the branch_id prefix of an encoded key.
/// Branch_id is the first 16 bytes. This is a simple memcpy.
pub fn rewrite_branch_id(encoded_key: &[u8], new_branch_id: &BranchId) -> Vec<u8> {
    let mut rewritten = encoded_key.to_vec();
    rewritten[..16].copy_from_slice(new_branch_id.as_bytes());
    rewritten
}

/// Rewrite just the typed_key_prefix (for bloom probes and MVCC grouping).
pub fn rewrite_typed_key_prefix(prefix: &[u8], new_branch_id: &BranchId) -> Vec<u8> {
    let mut rewritten = prefix.to_vec();
    rewritten[..16].copy_from_slice(new_branch_id.as_bytes());
    rewritten
}
```

### B.2 Update point lookup to walk inherited layers

**File:** `crates/storage/src/segmented/mod.rs`

In `get_versioned_from_branch()`, after checking own memtables and segments (steps 1-3 of the current read path), add:

```
4. For each inherited layer (nearest ancestor first):
   a. Rewrite key: swap child branch_id → layer.source_branch_id
   b. Search layer.segments (L0 linear, L1+ binary search)
      - Rewrite typed_key_prefix for bloom probes
      - Pass min(snapshot_version, layer.fork_version) as max_version
   c. If found: return entry (no need to rewrite result — the value is the same)
5. Not found → return None
```

The key insight: the `scan_block_for_key` and bloom filter probes need the rewritten key (with source branch_id). But the returned value is branch-agnostic — we don't need to rewrite the result.

### B.3 Implement RewritingIterator for range scans

**File:** `crates/storage/src/merge_iter.rs` (or new `rewriting_iter.rs`)

```rust
/// Wraps a segment iterator, rewriting branch_id in keys and filtering by fork_version.
struct RewritingIterator<I> {
    inner: I,
    source_branch_id: BranchId,  // the ancestor's ID (what's in the segments)
    target_branch_id: BranchId,  // the child's ID (what we rewrite TO)
    fork_version: u64,           // version cutoff
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for RewritingIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (ik, entry) = self.inner.next()?;
            // Skip entries written after fork
            if ik.commit_id() > self.fork_version {
                continue;
            }
            // Rewrite branch_id in the key to target
            let rewritten_ik = ik.with_branch_id(&self.target_branch_id);
            return Some((rewritten_ik, entry));
        }
    }
}
```

### B.4 Update create_merge_iterator to include inherited layers

**File:** `crates/storage/src/segmented/mod.rs`

In the method that builds merge iterators for scans/listings, after adding own-data sources:

```rust
for layer in &branch.inherited_layers {
    let rewritten_prefix = rewrite_typed_key_prefix(prefix, &layer.source_branch_id);
    for seg in layer.segments.all_segments() {
        let iter = seg.iter_prefix(&rewritten_prefix);
        sources.push(Box::new(RewritingIterator::new(
            iter,
            layer.source_branch_id,
            branch_id,
            layer.fork_version,
        )));
    }
}
```

### B.5 Update list_by_type_at_version for COW compatibility

**File:** `crates/storage/src/segmented/mod.rs`

The `list_by_type_at_version()` method (added by three-way merge Epic 1.3) must use the full merge iterator including inherited layers. Verify this works — if the method already calls `create_merge_iterator`, inherited layers are included automatically.

### B.6 Tests

| Test | Verifies |
|------|----------|
| `rewrite_branch_id_roundtrip` | Rewriting and un-rewriting produces original key |
| `rewrite_branch_id_preserves_sort_order` | Rewritten keys sort correctly within the new branch namespace |
| `inherited_layer_point_lookup` | Manually attach inherited layer, verify child reads parent data |
| `inherited_layer_version_filter` | Entries with commit_id > fork_version are invisible |
| `inherited_layer_write_shadows` | Write to child shadows inherited entry |
| `inherited_layer_delete_shadows` | Tombstone on child hides inherited entry |
| `inherited_layer_range_scan` | Range scan merges own + inherited data correctly |
| `inherited_layer_bloom_rewrite` | Bloom probes use rewritten prefix — no false negatives |
| `inherited_layer_two_levels` | A←B←C: C reads through B's data and A's data |
| `inherited_layer_version_clamping` | Time-travel read uses min(user_version, fork_version) |
| All existing storage tests pass | No regression (inherited_layers empty → no behavior change) |

---

## Epic C: O(1) Fork (~1 week)

**Goal:** Replace deep-copy fork with COW. Fork creates an inherited layer pointing to the source's segments. The deep-copy code path is removed.

**Prerequisite:** Three-way merge (Phase 1) must be complete. The old merge relied on deep-copied ancestor data in the child. With COW, ancestor data is accessed through inherited layers via the multi-layer read path. Three-way merge's `compute_merge_base()` returns the child branch + fork_version, and `list_by_type_at_version()` walks inherited layers transparently.

### C.1 Implement COW fork path

**File:** `crates/engine/src/branch_ops.rs`

Replace the deep-copy loop with:

```rust
pub fn fork_branch(db: &Arc<Database>, source: &str, dest: &str) -> Result<ForkInfo> {
    // 1. Verify source exists, destination doesn't
    // 2. Create destination branch (BranchIndex)
    // 3. Register source spaces on destination (SpaceIndex)

    // 4. Capture fork_version
    let fork_version = db.current_version();

    // 5. Force-flush source memtable so all data is in segments
    db.storage().flush_memtable(&source_id)?;

    // 6. Snapshot source's current SegmentVersion
    let source_segments = db.storage().snapshot_version(&source_id);

    // 7. Build inherited layers:
    //    - One layer for source's own segments
    //    - Plus all of source's inherited layers (flattened)
    let mut inherited = vec![InheritedLayer {
        source_branch_id: source_id,
        fork_version,
        segments: source_segments,
        status: LayerStatus::Active,
    }];
    // Flatten: copy source's inherited layers into child
    inherited.extend(db.storage().inherited_layers(&source_id).clone());

    // 8. Attach layers to destination's BranchState
    db.storage().set_inherited_layers(&dest_id, inherited);

    // 9. Increment segment refcounts for all inherited segments
    for layer in db.storage().inherited_layers(&dest_id) {
        for seg in layer.segments.all_segments() {
            db.storage().ref_registry().increment(&seg.segment_id());
        }
    }

    // 10. Write manifest for destination (includes inherited layers)
    db.storage().write_manifest(&dest_id)?;

    // 11. Record fork event in DAG with fork_version
    // (done by executor handler, which receives fork_version in ForkInfo)

    Ok(ForkInfo {
        source: source.to_string(),
        destination: dest.to_string(),
        fork_version,
        keys_copied: 0,  // no deep copy
        spaces_copied: space_count,
    })
}
```

### C.2 Handle fork chains

When forking C from B, and B has inherited layers from A:

```
C.inherited_layers = [
    { source: B, fork_version: current_version, segments: B.own_segments },
    { source: A, fork_version: B.inherited[0].fork_version, segments: A.segments },
]
```

The fork_version for transitive layers is preserved from the original fork. C sees A's data as it was when B was forked from A, not as it is now.

### C.3 Enforce MAX_INHERITED_LAYERS

```rust
const MAX_INHERITED_LAYERS: usize = 4;
```

After constructing the inherited layers list, if `len() > MAX_INHERITED_LAYERS`, schedule immediate background materialization of the deepest layer (Epic D). The fork still completes in O(memtable) — materialization runs asynchronously.

### C.4 Remove deep-copy code path

Delete the `for type_tag in DATA_TYPE_TAGS { ... list_by_type ... transaction ... }` loop from `fork_branch()`. This was the O(n) bottleneck.

### C.5 Update vector reload

Vector backends need special handling. At fork time, vector data is in inherited segments under the parent's branch_id. The HNSW index can't transparently rewrite branch_ids during graph traversal.

**For now:** After COW fork, vector search on inherited data is not available until background materialization runs. The vector reload step (`post_merge_reload_vectors_from`) only loads the child's own segments (which are empty right after fork). Vector search returns no results for inherited vectors.

**Acceptable because:** Vector materialization is triggered by the chain depth check and runs quickly for typical vector collection sizes. Users who need immediate vector search after fork can call a force-materialize API (Epic D).

**Future:** Implement branch_id-aware vector search that walks inherited layers (tracked by #1581 and vector-specific design work).

### C.6 Tests

| Test | Verifies |
|------|----------|
| `cow_fork_creates_inherited_layer` | Fork creates InheritedLayer with correct source_branch_id and fork_version |
| `cow_fork_no_data_copy` | Child's own segments are empty after fork |
| `cow_fork_timing` | Fork of 1M-key branch completes in < 1 second (vs minutes with deep copy) |
| `cow_fork_read_through` | Get on child returns parent's data via inherited layer |
| `cow_fork_write_then_read` | Write to child, read returns child's value (not parent's) |
| `cow_fork_delete_shadows` | Delete on child hides inherited entry |
| `cow_fork_parent_write_after_fork_invisible` | Parent writes after fork are NOT visible to child |
| `cow_fork_scan` | Range scan on child includes inherited data |
| `cow_fork_chain_3_levels` | A←B←C: reads on C correctly walk all three layers |
| `cow_fork_chain_flattened` | C's inherited_layers includes both B and A layers with correct fork_versions |
| `cow_fork_spaces_registered` | All source spaces are registered on destination |
| `cow_fork_refcounts_incremented` | All inherited segments have refcount > 0 |
| `cow_fork_plus_three_way_merge` | Fork (COW) → modify both sides → three-way merge works correctly |
| `cow_fork_merge_base_from_inherited` | Merge base reads ancestor state through inherited layer (not deep copy) |
| `cow_fork_repeated_merge` | Fork → merge → modify → merge again (merge_version as base) |
| `cow_fork_diff_includes_inherited` | Diff sees all keys including those in inherited layers |
| All existing branch tests pass | No regression (new fork path must be compatible with all existing operations) |

---

## Epic D: Background Materialization (~1 week)

**Goal:** Collapse inherited layers by rewriting entries into the child's own segments. Runs asynchronously without blocking reads or writes.

### D.1 Implement materialize_layer

**File:** `crates/storage/src/segmented/mod.rs`

```rust
/// Materialize an inherited layer — copy entries into the child's own segments.
///
/// This converts inherited data into the child's namespace, allowing the
/// inherited layer to be removed. Runs in the background.
fn materialize_layer(
    &self,
    child_branch_id: &BranchId,
    layer_index: usize,
) -> io::Result<MaterializeResult> {
    let layer = &self.branch(child_branch_id).inherited_layers[layer_index];

    // 1. Mark as materializing
    layer.status = LayerStatus::Materializing;

    // 2. Iterate inherited entries (version-filtered)
    let mut builder = SplittingSegmentBuilder::new(TARGET_FILE_SIZE);
    for entry in layer.segments.iter_all() {
        if entry.commit_id() > layer.fork_version {
            continue; // written after fork — not ours
        }

        // 3. Check if child already has this key (shadowed)
        let child_key = rewrite_branch_id(entry.key(), child_branch_id);
        if self.key_exists_in_own_data(child_branch_id, &child_key) {
            continue; // child's own write shadows this
        }

        // Also check closer inherited layers (layers with lower index)
        if self.key_exists_in_closer_layers(child_branch_id, &child_key, layer_index) {
            continue; // a nearer ancestor's version shadows this
        }

        // 4. Rewrite branch_id, PRESERVE original commit_id
        let materialized_key = rewrite_branch_id(entry.key(), child_branch_id);
        builder.add(materialized_key, entry.value(), entry.commit_id());
    }

    // 5. Flush to new segment files
    let new_segments = builder.finish()?;

    // 6. Add to child's own SegmentVersion (ArcSwap)
    self.add_segments_to_branch(child_branch_id, new_segments, 0)?; // level 0

    // 7. Remove layer from inherited_layers
    self.remove_inherited_layer(child_branch_id, layer_index);

    // 8. Decrement refcounts for materialized segments
    for seg in layer.segments.all_segments() {
        self.ref_registry.decrement(&seg.segment_id());
    }

    Ok(MaterializeResult { entries_materialized, segments_created })
}
```

**Critical: commit_ids are preserved, not reassigned.** See `cow-branching.md` Section 9.1 for the rationale — reassigning would break merge base computation.

### D.2 Shadow detection

During materialization, an entry is "shadowed" if the child (or a closer inherited layer) already has a version of the same key. Shadowed entries are skipped — they'd be invisible to reads anyway.

This is a point-lookup per inherited key. For large inherited layers, this dominates materialization cost. Optimization: batch the shadow check using bloom filters on the child's own segments before doing expensive point lookups.

### D.3 Schedule materialization

**File:** `crates/engine/src/database/mod.rs`

Materialization is triggered by:

1. **Fork with chain depth > MAX_INHERITED_LAYERS** — materialize deepest layer immediately after fork
2. **Compaction cycle** — check inherited layer depths during the post-compaction loop, materialize if needed
3. **Explicit API** — `db.materialize_branch(branch_name)` for manual triggering

```rust
// In the post-flush/compaction loop:
for branch in active_branches {
    if branch.inherited_layers.len() > MAX_INHERITED_LAYERS {
        let deepest = branch.inherited_layers.len() - 1;
        storage.materialize_layer(&branch.branch_id, deepest)?;
    }
}
```

### D.4 Branch deletion with inherited layers

**File:** `crates/storage/src/segmented/mod.rs`

When deleting a branch that has inherited layers:

1. For each inherited layer, decrement refcounts for all segments
2. Delete the branch's own segments (if refcount allows)
3. Remove branch state
4. Write (empty) manifest / remove manifest file

### D.5 Force-materialize API

**File:** `crates/engine/src/branch_ops.rs`

```rust
/// Force-materialize all inherited layers on a branch.
/// After this returns, the branch has no inherited layers —
/// all data is in its own segments.
pub fn materialize_branch(db: &Arc<Database>, branch: &str) -> Result<MaterializeInfo> {
    let storage = db.storage();
    let branch_id = resolve_branch_id(db, branch)?;

    let mut total = MaterializeInfo::default();
    // Materialize from deepest to nearest
    while !storage.inherited_layers(&branch_id).is_empty() {
        let deepest = storage.inherited_layers(&branch_id).len() - 1;
        let result = storage.materialize_layer(&branch_id, deepest)?;
        total.entries += result.entries_materialized;
        total.segments += result.segments_created;
    }
    Ok(total)
}
```

### D.6 Tests

| Test | Verifies |
|------|----------|
| `materialize_collapses_layer` | After materialization, layer is removed, data accessible in own segments |
| `materialize_preserves_commit_ids` | Materialized entries have original commit_ids (not new ones) |
| `materialize_skips_shadowed_entries` | Entries already in child's own data are not duplicated |
| `materialize_skips_post_fork_entries` | Entries with commit_id > fork_version are not materialized |
| `materialize_deepest_first` | Chain A←B←C: materializing C's deepest layer (A) collapses correctly |
| `materialize_refcount_decremented` | After materialization, parent's segment refcounts decrease |
| `materialize_during_concurrent_reads` | Reads continue to work during background materialization |
| `materialize_triggers_on_chain_depth` | Fork that exceeds MAX_INHERITED_LAYERS triggers auto-materialization |
| `branch_delete_decrements_inherited_refs` | Deleting child with inherited layers decrements all refcounts |
| `branch_delete_allows_parent_gc` | After child deletion, parent compaction can delete previously-shared segments |
| `force_materialize_all_layers` | `materialize_branch()` collapses all layers, branch has none remaining |
| `materialize_then_merge` | Materialized branch still merges correctly (commit_ids preserved) |
| `cow_fork_materialize_roundtrip` | Fork (COW) → materialize → all data identical to deep-copy fork |
| All existing tests pass | No regression |

---

## Sequencing & Dependencies

```
Week 1:     Epic A (Storage Foundation)
            ├── A.1-A.2: InheritedLayer + RefRegistry structs
            ├── A.3: Wire refcounting into compaction (SAFETY CRITICAL)
            ├── A.4-A.5: Manifest + recovery
            └── A.6: Tests

Week 2-3:   Epic B (Multi-Layer Read Path) + Three-Way Merge (parallel)
            ├── B.1-B.2: Key rewriting + point lookup
            ├── B.3-B.4: RewritingIterator + scan integration
            └── B.5-B.6: COW-compatible list_at_version + tests

Week 3-4:   Epic C (O(1) Fork)
            ├── C.1-C.2: COW fork + chain handling
            ├── C.3: Chain depth enforcement
            ├── C.4: Remove deep-copy code
            ├── C.5: Vector stub (deferred materialization)
            └── C.6: Integration tests with three-way merge

Week 4-5:   Epic D (Background Materialization)
            ├── D.1-D.2: Materialize + shadow detection
            ├── D.3: Scheduling
            ├── D.4-D.5: Branch deletion + force-materialize API
            └── D.6: Tests
```

## What's NOT in These Epics

- **Vector search on inherited data** — deferred. Vectors are materialized eagerly by chain depth triggers or explicit API call. Branch_id-aware HNSW traversal is future work.
- **Distributed branch sync** — post-1.0. COW is single-node only.
- **Transitive merge base** — deferred to Phase 3 of the branching plan.
- **Version pinning for GC** — deferred. No MVCC GC exists yet. When it ships, `compute_gc_safe_version()` must account for both fork_versions and inherited layer references.

## Success Criteria

1. `fork_branch` of a 1M-key branch completes in < 1 second (was: minutes)
2. All existing branch tests pass without modification
3. Three-way merge works correctly with COW-forked branches
4. Parent compaction does not corrupt child's inherited data
5. Background materialization collapses layers without data loss
6. `fork → write → read → merge → delete` lifecycle works end-to-end

## References

- [`cow-branching.md`](cow-branching.md) — full design document
- [`implementation-design.md`](implementation-design.md) — three-way merge design
- [`implementation-plan-epics.md`](implementation-plan-epics.md) — phased branching plan (COW = elevated P0)
- [`current-state.md`](current-state.md) — audit of current branching system
- #1569 — Branch fork is O(n), not O(1)
- #1570 — Branch merge lacks three-way merge
- #1571 — Branch diff is O(n) full scan
