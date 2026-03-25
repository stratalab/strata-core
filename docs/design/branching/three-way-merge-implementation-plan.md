# Three-Way Merge Implementation Plan

**Date:** 2026-03-24
**Issues:** #1537, #1570, #1692
**Prerequisites:** COW branching (complete), [Current State Analysis](current-state.md), [Implementation Design](implementation-design.md)
**Status:** Implementation Plan

---

## Context

Strata's merge is broken. `merge_branches()` uses a two-way diff that cannot distinguish "deleted on source" from "created on target after fork." This causes two critical bugs:
- **#1537**: Deletes are not propagated (merge ignores `removed` entries entirely)
- **#1692**: Target-only changes are overwritten by source's stale values under LWW

COW branching is fully implemented (O(1) fork, inherited layers, RewritingIterator, materialization). The MVCC infrastructure already supports point-in-time reads. The only missing pieces are: (1) recording merge versions in the DAG, (2) a storage method that reads at a specific version without filtering tombstones, (3) the three-way diff/merge algorithm, and (4) wiring it together.

### Research Validation

Surveyed Dolt, lakeFS, git merge-ort, TardisDB, CouchDB, Neon, and academic literature (Decibel VLDB 2016, TARDiS SIGMOD 2016, OrpheusDB VLDB 2017).

**Confirmed design decisions:**
- **lakeFS** uses the identical 14-case decision matrix — our `classify_change()` is correct
- **Dolt** confirms primary key = cross-version identity (matches our `(space, type_tag, user_key)` tuple)
- **Git merge-ort**: tree-level only, no content merge for opaque values — exactly our case
- All systems (Dolt, lakeFS, git) store merge metadata in a **separate DAG**, not inline
- **Criss-cross merges**: safe to reject for now (Dolt also rejects; only git handles them via recursive base merging)
- **Full-scan O(n)** for V1 is acceptable (Dolt started there, optimized to O(diff) later with Prolly Trees; lakeFS uses Merkle tree range skipping)
- **Delete propagation via three-way** is the universal approach — no shortcut exists

### Key Sources

- [Dolt Cell-Level Three-Way Merge](https://www.dolthub.com/blog/2020-07-15-three-way-merge/)
- [Dolt Efficient Diff on Prolly Trees](https://www.dolthub.com/blog/2020-06-16-efficient-diff-on-prolly-trees/)
- [lakeFS Merge Internals](https://docs.lakefs.io/v1.61/understand/how/merge/)
- [Git merge-ort scaling](https://github.blog/engineering/scaling-merge-ort-across-github/)
- [Git trivial-merge decision table](https://github.com/git/git/blob/master/Documentation/technical/trivial-merge.adoc)
- [Decibel: Version Control for Datasets (VLDB 2016)](http://www.vldb.org/pvldb/vol9/p624-maddox.pdf)

---

## Approach

Replace `merge_branches()` with a three-way implementation. The function signature gains one parameter (`merge_base_override: Option<(BranchId, u64)>`) that the executor populates from the DAG. Merging requires a fork or merge relationship between branches — unrelated branches produce a clear error. The old two-way merge code has been fully excised.

**Architecture**: The executor queries the DAG for the merge base version and passes it down to the engine. This cleanly separates concerns (engine doesn't depend on graph crate) and handles the "materialized branch lost fork info" edge case. The engine also checks storage-level `InheritedLayer` as a fast path for the common case.

---

## Phase 1: DAG — Add merge_version tracking

### Step 1.1: Add `merge_version` to `MergeRecord`

**File**: `crates/core/src/branch_dag.rs`

Add field to `MergeRecord` struct (after line 127):
```rust
pub merge_version: Option<u64>,
```
`Option` for backward compat with existing DAG events that lack it.

### Step 1.2: Store merge_version in `dag_record_merge()`

**File**: `crates/graph/src/branch_dag.rs`

In `dag_record_merge()` (~line 246): read `merge_info.merge_version` and store it in the event node properties JSON, same pattern as `fork_version` in `dag_record_fork()`.

### Step 1.3: Extract merge_version in `find_merge_history()`

**File**: `crates/graph/src/branch_dag.rs`

In `find_merge_history()` (~line 501): extract `merge_version` from event node properties, same pattern as `fork_version` in `find_fork_origin()`.

### Step 1.4: Add `find_last_merge_version()` helper

**File**: `crates/graph/src/branch_dag.rs`

New public function:
```rust
pub fn find_last_merge_version(
    db: &Arc<Database>,
    source: &str,
    target: &str,
) -> StrataResult<Option<u64>>
```
Walks merge history for `source`, filters for merges where target matches, returns most recent `merge_version`. Also checks reverse direction (target's merge history where source was the source).

### Step 1.5: Add `find_fork_version()` helper

**File**: `crates/graph/src/branch_dag.rs`

New public function:
```rust
pub fn find_fork_version(
    db: &Arc<Database>,
    source: &str,
    target: &str,
) -> StrataResult<Option<(String, u64)>>  // (child_branch_name, fork_version)
```
Checks if source was forked from target or target from source. Returns the child name and fork_version. This is needed when inherited layers have been materialized (storage no longer has fork info).

**Tests** (Phase 1): Record merge with merge_version, retrieve it, verify round-trip. Test `find_last_merge_version` with merges in both directions. Test `find_fork_version`.

---

## Phase 2: Storage — Version-scoped listing with tombstones

### Step 2.1: Add `VersionedEntry` type

**File**: `crates/storage/src/segmented/mod.rs`

```rust
/// Entry from version-scoped listing that preserves tombstone status.
pub struct VersionedEntry {
    pub key: Key,
    pub value: Value,
    pub is_tombstone: bool,
    pub commit_id: u64,
}
```

### Step 2.2: Add `list_by_type_at_version()`

**File**: `crates/storage/src/segmented/mod.rs`

New public method near `list_by_type()` (~line 1377):
```rust
pub fn list_by_type_at_version(
    &self,
    branch_id: &BranchId,
    type_tag: TypeTag,
    max_version: u64,
) -> Vec<VersionedEntry>
```

Implementation: same as `list_branch_inner()` (lines 1251-1313) but:
1. Uses `max_version` instead of `u64::MAX` in `MvccIterator::new(merge, max_version)`
2. Does NOT filter tombstones — emits them with `is_tombstone: true`
3. Filters by `type_tag` on decode (like `list_by_type()`)
4. Does NOT filter expired entries (ancestor state may have TTL'd entries that are still relevant for diff)

The merge iterator construction already handles inherited layers via `RewritingIterator` — this works transparently for COW branches.

**Tests**: Write key, delete key, write new version. Call at different versions, verify tombstone visibility.

---

## Phase 3: Engine — Three-way diff and merge

### Step 3.1: Add `MergeBase` struct

**File**: `crates/engine/src/branch_ops.rs`

```rust
/// Common ancestor state for three-way merge.
#[derive(Debug, Clone)]
struct MergeBase {
    /// Branch to read ancestor state from.
    branch_id: BranchId,
    /// MVCC version to read at.
    version: u64,
}
```

### Step 3.2: Add `compute_merge_base()`

**File**: `crates/engine/src/branch_ops.rs`

```rust
fn compute_merge_base(
    db: &Arc<Database>,
    source_id: BranchId,
    target_id: BranchId,
    merge_base_override: Option<(BranchId, u64)>,
) -> Option<MergeBase>
```

Logic:
1. If `merge_base_override` is `Some((branch_id, v))`, return `MergeBase { branch_id, version: v }` directly. This is the executor-provided override from DAG data (covers repeated merges and materialized branches).
2. Otherwise, check storage for fork relationship:
   - `storage.get_fork_info(&source_id)` — if source's parent is target, return `MergeBase { branch_id: target_id, version: fork_version }`
   - `storage.get_fork_info(&target_id)` — if target's parent is source, return `MergeBase { branch_id: source_id, version: fork_version }`
3. Return `None` (unrelated branches → error).

### Step 3.3: Add `get_fork_info()` to storage

**File**: `crates/storage/src/segmented/mod.rs`

```rust
/// Get the fork origin of a branch from its nearest active inherited layer.
/// Returns None if the branch has no inherited layers (never forked, or fully materialized).
pub fn get_fork_info(&self, branch_id: &BranchId) -> Option<(BranchId, u64)> {
    let branch = self.branches.get(branch_id)?;
    let layer = branch.inherited_layers.first()?;
    if layer.status == LayerStatus::Materialized {
        return None;
    }
    Some((layer.source_branch_id, layer.fork_version))
}
```

### Step 3.4: Add `ThreeWayChange` enum

**File**: `crates/engine/src/branch_ops.rs`

14-variant enum matching the decision matrix (same as git trivial-merge and lakeFS):

```rust
#[derive(Debug, Clone, PartialEq)]
enum ThreeWayChange {
    Unchanged,
    SourceChanged { value: Value },
    TargetChanged,
    BothChangedSame,
    Conflict { source_value: Value, target_value: Value },
    SourceAdded { value: Value },
    TargetAdded,
    BothAddedSame,
    BothAddedDifferent { source_value: Value, target_value: Value },
    SourceDeleted,
    TargetDeleted,
    BothDeleted,
    DeleteModifyConflict { target_value: Value },
    ModifyDeleteConflict { source_value: Value },
}
```

### Step 3.5: Add `classify_change()` — the decision matrix

**File**: `crates/engine/src/branch_ops.rs`

Pure function:
```rust
fn classify_change(
    ancestor: Option<&Value>,  // None = absent or tombstoned at ancestor
    source: Option<&Value>,    // None = absent in current source (live listing)
    target: Option<&Value>,    // None = absent in current target (live listing)
) -> ThreeWayChange
```

The 14-case match on `(ancestor, source, target)`:

| Ancestor | Source | Target | Result |
|----------|--------|--------|--------|
| ∅ | ∅ | ∅ | Unchanged |
| A | A | A | Unchanged |
| A | **S** | A | SourceChanged |
| A | A | **T** | TargetChanged |
| A | **S** | **S** | BothChangedSame |
| A | **S** | **T** | **Conflict** |
| ∅ | S | ∅ | SourceAdded |
| ∅ | ∅ | T | TargetAdded |
| ∅ | S | S | BothAddedSame |
| ∅ | **S** | **T** | **BothAddedDifferent** |
| A | ∅ | A | SourceDeleted |
| A | ∅ | **T** | **DeleteModifyConflict** |
| A | A | ∅ | TargetDeleted |
| A | **S** | ∅ | **ModifyDeleteConflict** |
| A | ∅ | ∅ | BothDeleted |

### Step 3.6: Add three-way diff and merge action types

**File**: `crates/engine/src/branch_ops.rs`

```rust
struct MergeAction {
    space: String,
    raw_key: Vec<u8>,
    type_tag: TypeTag,
    action: MergeActionKind,
}

enum MergeActionKind {
    Put(Value),
    Delete,
}
```

### Step 3.7: Add `three_way_diff()`

**File**: `crates/engine/src/branch_ops.rs`

```rust
fn three_way_diff(
    db: &Arc<Database>,
    source_id: BranchId,
    target_id: BranchId,
    merge_base: &MergeBase,
    spaces: &[String],
    strategy: MergeStrategy,
) -> StrataResult<(Vec<MergeAction>, Vec<ConflictEntry>)>
```

For each space × type_tag in `DATA_TYPE_TAGS`:
1. **Ancestor**: `storage.list_by_type_at_version(&merge_base.branch_id, type_tag, merge_base.version)` — build HashMap keyed by user_key bytes; tombstoned entries map to None
2. **Source**: `storage.list_by_type(&source_id, type_tag)` — build HashMap keyed by user_key bytes
3. **Target**: `storage.list_by_type(&target_id, type_tag)` — build HashMap keyed by user_key bytes
4. Union all keys across three maps
5. For each key: `classify_change(ancestor, source, target)`
6. Map results to actions:
   - `SourceChanged{v}` / `SourceAdded{v}` → `MergeAction::Put(v)`
   - `SourceDeleted` → `MergeAction::Delete`
   - Conflict variants → resolved per strategy or added to conflicts list
   - Everything else (Unchanged, TargetChanged, TargetAdded, TargetDeleted, BothDeleted, BothChangedSame, BothAddedSame) → no action needed

### Step 3.8: Rewrite `merge_branches()` with three-way path

**File**: `crates/engine/src/branch_ops.rs`

Change signature:
```rust
pub fn merge_branches(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    strategy: MergeStrategy,
    merge_base_override: Option<(BranchId, u64)>,
) -> StrataResult<MergeInfo>
```

New logic:
1. Resolve branch IDs
2. `compute_merge_base(db, source_id, target_id, merge_base_override)`
3. **If merge base found**: run `three_way_diff()`, then apply actions
4. **If no merge base**: return error (branches must be related by fork or merge)
5. Apply puts + deletes in a single `transaction_with_version()` call
6. Capture `merge_version` from the transaction return value
7. Reload secondary index backends (existing behavior)
8. Return `MergeInfo` with new fields

Conflict handling per strategy:
- `Strict`: any conflict → return error with conflict list, no writes
- `LastWriterWins` (SourceWins): conflicts resolve to source's value (puts for Conflict/BothAddedDifferent/ModifyDeleteConflict, delete for DeleteModifyConflict)

### Step 3.9: Update `MergeInfo`

**File**: `crates/engine/src/branch_ops.rs`

Add fields to struct:
```rust
pub merge_version: Option<u64>,
pub keys_deleted: u64,
```

---

## Phase 4: Executor — Wire DAG → engine

### Step 4.1: Update `branch_merge` handler

**File**: `crates/executor/src/handlers/branch.rs`

In `branch_merge()` (~line 295):

1. Before calling merge, compute the merge base version from DAG:
```rust
// Check for previous merge first (takes priority over fork)
let merge_base_override = match branch_dag::find_last_merge_version(&p.db, &source, &target) {
    Ok(Some(v)) => {
        let target_id = resolve_branch_name(&target);
        Some((target_id, v))
    }
    _ => {
        // No previous merge — check for fork relationship in DAG
        // (covers materialized branches where storage lost fork info)
        match branch_dag::find_fork_version(&p.db, &source, &target) {
            Ok(Some((child_name, fork_version))) => {
                let child_id = resolve_branch_name(&child_name);
                Some((child_id, fork_version))
            }
            _ => None,  // Let engine try storage-level fork info
        }
    }
};
```

2. Pass to engine:
```rust
let info = strata_engine::branch_ops::merge_branches(
    &p.db, &source, &target, strategy, merge_base_override
)?;
```

3. DAG recording already passes `&info` which now includes `merge_version`.

### Step 4.2: Public API

**File**: `crates/executor/src/api/branches.rs`

`merge()` and `merge_with_options()` return `MergeInfo` — the new fields (`merge_version`, `keys_deleted`) are automatically available. No signature changes needed.

---

## Phase 5: Tests

### Step 5.1: Unit tests for `classify_change()` (14 cases)

**File**: `crates/engine/src/branch_ops.rs` (test module)

One test per row of the decision matrix. Pure function tests — no database needed.

### Step 5.2: Delete propagation test (#1537)

```
Fork parent→child. Parent has {a:1, b:2}. Child deletes "b".
Merge child→parent. Assert parent has {a:1}, b is gone.
```

### Step 5.3: Target-only changes preserved (#1692)

```
Fork parent→child. Parent has {a:1}. Parent adds b:2 after fork. Child unchanged.
Merge child→parent. Assert parent has {a:1, b:2}.
```

### Step 5.4: Disjoint changes merge cleanly

```
Fork parent→child. Parent has {a:1, b:2, c:3}.
Parent changes a→10. Child changes b→20.
Merge child→parent. Assert {a:10, b:20, c:3}.
```
(This is the ignored test `cow_three_way_merge_with_inherited_layers` — remove `#[ignore]`)

### Step 5.5: Both-modified conflict under Strict

```
Fork parent→child. Parent has {a:1}. Parent changes a→10. Child changes a→20.
Strict merge → error with conflict. LWW merge → source wins (a=20).
```

### Step 5.6: Repeated merge

```
Fork→merge→modify more→merge again.
Second merge uses first merge's version as base.
Only changes after first merge are considered.
```
(This requires the full DAG flow: handler queries `find_last_merge_version`)

### Step 5.7: Backward compat — existing tests pass

All existing merge tests rewritten to use `fork_branch()` (three-way merge requires fork/merge relationship). Added `test_merge_unrelated_branches_errors` to verify unrelated branches are rejected.

### Step 5.8: Enable ignored integration test

**File**: `tests/integration/branching.rs:852`

Remove `#[ignore = "three-way merge not yet implemented"]`.

---

## Critical Files

| File | Changes |
|------|---------|
| `crates/core/src/branch_dag.rs` | Add `merge_version` to `MergeRecord` |
| `crates/graph/src/branch_dag.rs` | Store/extract `merge_version`, add `find_last_merge_version()`, `find_fork_version()` |
| `crates/storage/src/segmented/mod.rs` | Add `VersionedEntry`, `list_by_type_at_version()`, `get_fork_info()` |
| `crates/engine/src/branch_ops.rs` | Core: `MergeBase`, `ThreeWayChange`, `classify_change()`, `three_way_diff()`, rewrite `merge_branches()`, update `MergeInfo` |
| `crates/executor/src/handlers/branch.rs` | Query DAG for merge base, pass to engine |
| `tests/integration/branching.rs` | Enable ignored test |

## Verification

1. `cargo test -p strata-engine -- branch` — all existing + new tests pass
2. `cargo test -p strata-storage` — new `list_by_type_at_version` tests pass
3. `cargo test -p strata-graph -- dag` — merge_version round-trip tests pass
4. `cargo test --test branching` — integration tests including the previously-ignored three-way merge test
5. Manual: fork → delete key on source → merge → verify key is deleted on target (the #1537 scenario)

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Ancestor versions GC'd by compaction | Low | High | No MVCC GC today; version pinning is future work (Epic 3.4) |
| Full-scan perf for large branches | Medium | Medium | Acceptable for V1; optimize to O(diff) later with version-range scanning |
| Materialized branch + no DAG data | Low | Medium | Executor queries DAG as primary source; storage fork info is fallback only |
| Existing tests break | Low | Low | All merge tests rewritten to use fork; unrelated branch merge explicitly tested |

---

## Sources

- [Current State Analysis](current-state.md)
- [Git Reference Architecture](git-reference-architecture.md)
- [Three-Way Merge Design](implementation-design.md)
- [COW Branching Design](cow-branching.md)
- [Implementation Plan (Epics)](implementation-plan-epics.md)
- Issue #1537 — branch_merge does not propagate deletes
- Issue #1570 — Branch merge lacks three-way merge
- Issue #1692 — Branch merge is two-way diff/apply
