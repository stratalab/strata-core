# Branching System: Current State Analysis

**Date:** 2026-03-18
**Issue:** #1537 (branch_merge does not propagate deletes)
**Scope:** Full audit of the branching system to inform a proper redesign

---

## 1. Architecture Overview

Branches in Strata are **isolated data namespaces**. Every key in storage is prefixed with a branch-specific UUID. After forking, branches share no storage — each operates on a completely independent copy of the data.

```
┌────────────────────────────────────────────────────────┐
│  User API (Strata / Branches<'a>)                      │
│  branch.create / fork / diff / merge / delete          │
├────────────────────────────────────────────────────────┤
│  Executor Handlers (handlers/branch.rs)                │
│  Command dispatch, DAG recording, validation           │
├────────────────────────────────────────────────────────┤
│  Engine (branch_ops.rs)                                │
│  fork_branch / diff_branches / merge_branches          │
├────────────────────────────────────────────────────────┤
│  Storage (segmented/mod.rs)                            │
│  Per-branch memtables + segments, MVCC via commit_id   │
│  Key encoding: branch_id || space || type_tag ||       │
│                user_key || !commit_id                   │
└────────────────────────────────────────────────────────┘
```

### Key Files

| File | Purpose |
|------|---------|
| `crates/engine/src/branch_ops.rs` | Fork, diff, merge implementations (~2000 lines) |
| `crates/engine/src/branch_dag.rs` | DAG tracking on `_system_` branch (~1200 lines) |
| `crates/engine/src/branch_status_cache.rs` | In-memory status cache (43 lines) |
| `crates/engine/src/primitives/branch/index.rs` | BranchMetadata, BranchIndex (~500 lines) |
| `crates/storage/src/segmented/mod.rs` | Storage engine, MVCC, listing |
| `crates/storage/src/merge_iter.rs` | MvccIterator — snapshot-aware deduplication |
| `crates/concurrency/src/manager.rs` | Global version counter, transaction commit |
| `crates/executor/src/api/branches.rs` | Public API (Branches struct) |
| `crates/executor/src/command.rs` | Command enum (lines 643-730 for branch commands) |

---

## 2. What We Have

### 2.1 Global Monotonic Version Counter

**Location:** `crates/concurrency/src/manager.rs` lines 61-174

A single `AtomicU64` shared across ALL branches. Every committed transaction increments it via `fetch_add(1, AcqRel)`. This gives us:

- **Global ordering** — any two writes across any branches can be compared by version
- **Monotonicity** — versions never go backwards
- **One version per transaction** — all puts/deletes in the same transaction share the same commit_version
- **May have gaps** — failed commits leave version gaps (by design)

```rust
// crates/concurrency/src/manager.rs:164
pub fn allocate_version(&self) -> Result<u64, CommitError> {
    let prev = self.version.fetch_add(1, Ordering::AcqRel);
    Ok(prev + 1)
}
```

The database exposes `db.current_version()` (line 1042) and `db.transaction_with_version()` (line 1978) which returns the commit_version alongside the result.

### 2.2 MVCC with Snapshot Reads

**Location:** `crates/storage/src/merge_iter.rs` lines 83-125

Every key is stored with `!commit_id` appended (bitwise NOT for descending sort). The `MvccIterator` deduplicates: for each logical key, it emits only the first entry with `commit_id ≤ max_version`.

```rust
// merge_iter.rs:117 (simplified)
if ik.commit_id() <= self.max_version {
    self.last_emitted_prefix = Some(prefix);
    return Some((ik, entry));
}
```

This means we can read any branch's state at any past version — **point-in-time reads are built in**.

### 2.3 Versioned Tombstones

**Location:** `crates/storage/src/stored_value.rs`, `crates/storage/src/memtable.rs`

Deletes write tombstone entries with the same commit_id mechanism as regular writes:

```rust
// memtable.rs:29-38
pub struct MemtableEntry {
    pub value: Value,
    pub is_tombstone: bool,  // explicit deletion marker
    pub timestamp: Timestamp,
    pub ttl_ms: u64,
}
```

Tombstones participate in MVCC — reading a key at a snapshot that includes the tombstone's commit_id returns the tombstone (with `is_tombstone=true`). This distinguishes "key was deleted at version V" from "key never existed".

### 2.4 Branch Isolation via Namespace

Each branch gets a UUID (deterministic v5 from name). All data for a branch is prefixed with this UUID in the storage key encoding:

```
InternalKey = branch_id(16B) || space(null-term) || type_tag(1B) || user_key(escaped) || !commit_id(8B)
```

Branches are completely independent at the storage level — no shared data, no copy-on-write.

### 2.5 Branch DAG

**Location:** `crates/engine/src/branch_dag.rs`

A graph on the reserved `_system_` branch tracks branch lifecycle events:

- **Branch nodes** — `object_type: "branch"`, properties: status, created_at, message, creator
- **Fork event nodes** — `object_type: "fork"`, edges: `parent --[parent]--> fork_event --[child]--> child`
- **Merge event nodes** — `object_type: "merge"`, edges: `source --[source]--> merge_event --[target]--> target`

DAG writes are **best-effort** — failures are logged but never propagated. The DAG is a metadata overlay; branch data integrity is never at risk.

### 2.6 BranchMetadata

**Location:** `crates/engine/src/primitives/branch/index.rs` lines 90-150

```rust
pub struct BranchMetadata {
    pub name: String,
    pub branch_id: String,              // Random UUID v4
    pub parent_branch: Option<String>,  // EXISTS but NEVER populated
    pub status: BranchStatus,           // Only "Active" in MVP
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub completed_at: Option<Timestamp>,// Post-MVP, unused
    pub error: Option<String>,          // Post-MVP, unused
    pub version: u64,                   // Internal version counter
}
```

---

## 3. How Fork Works Today

**Location:** `crates/engine/src/branch_ops.rs` lines 240-338

```
fork_branch(db, "main", "feature"):

1. Verify source exists, destination doesn't
2. Create destination branch (BranchIndex)
3. Register source spaces on destination (SpaceIndex)
4. For each DATA_TYPE_TAG (KV, Event, State, Json, Vector, VectorConfig):
   a. entries = storage.list_by_type(source_id, type_tag)  // reads at u64::MAX
   b. Rewrite each key with destination namespace
   c. db.transaction(dest_id, |txn| { txn.put(key, value) })
      → assigns NEW commit_ids, discards original VersionedValue metadata
5. Reload vector backends for immediate searchability
```

### What Gets Lost

- **Version history** — only the current value is copied, not the version chain
- **Original commit_id** — the destination gets new commit_ids from the fork transaction
- **Fork point** — no record of WHAT the source looked like when forked
- **Parent relationship** — `parent_branch` field exists but is never set

### What Gets Recorded

The executor handler calls `dag_record_fork()` after `fork_branch()` returns, which stores:
- `timestamp` (wall-clock time of the fork event)
- `parent` and `child` branch names
- Optional `message` and `creator`

**Missing:** No `fork_version` (the global commit_id at fork time).

---

## 4. How Diff Works Today

**Location:** `crates/engine/src/branch_ops.rs` lines 374-549

```
diff_branches_with_options(db, "main", "feature", options):

1. For each DATA_TYPE_TAG:
   a. entries_a = storage.list_by_type(branch_a_id, type_tag)  // or list_by_type_at_timestamp
   b. entries_b = storage.list_by_type(branch_b_id, type_tag)
2. Group entries by (space, user_key)
3. Categorize:
   - ADDED: key in B but not A → SpaceDiff.added
   - REMOVED: key in A but not B → SpaceDiff.removed
   - MODIFIED: key in both, different value → SpaceDiff.modified
```

### The Tombstone Problem

`list_by_type()` calls `list_branch_inner()` which filters tombstones at line 356:

```rust
// segmented/mod.rs:355-358
mvcc.filter_map(|(ik, entry)| {
    if entry.is_tombstone || entry.is_expired() {
        return None;  // TOMBSTONES INVISIBLE
    }
    ...
})
```

The diff only sees **live keys**. A key deleted on one branch simply appears as "in A but not B" (removed) — indistinguishable from "created on B after fork, never existed on A."

### `as_of` Support

`DiffOptions.as_of` is a **timestamp** (microseconds since epoch), not a commit_id. It delegates to `storage.list_by_type_at_timestamp()`.

---

## 5. How Merge Works Today

**Location:** `crates/engine/src/branch_ops.rs` lines 555-714

```
merge_branches(db, "feature", "main", LastWriterWins):

1. diff = diff_branches(target="main", source="feature")
2. If Strict mode and conflicts exist → error
3. For each SpaceDiff:
   a. entries_to_apply = space_diff.added
                       + space_diff.modified (if LWW)
   b. IGNORES space_diff.removed ← THE BUG
   c. db.transaction(target_id, |txn| { txn.put(key, value) })
```

### The Bug (#1537)

Lines 643-652:

```rust
let entries_to_apply: Vec<&BranchDiffEntry> = space_diff
    .added
    .iter()
    .chain(if strategy == MergeStrategy::LastWriterWins {
        space_diff.modified.iter()
    } else {
        [].iter()
    })
    .collect();
// space_diff.removed is NEVER included
```

Merge is **add-and-overwrite only**. Keys deleted on the source branch are silently ignored. This makes merge fundamentally broken for any workflow where deletions matter.

### Why It Can't Be Fixed Without Three-Way Merge

Simply applying all `removed` entries as deletes would be wrong. The diff categorizes keys as "removed" when they're "in target but not source." This conflates two different scenarios:

1. **Key deleted on source after fork** — should propagate the delete ✓
2. **Key created on target after fork** — should NOT be deleted ✗

Without knowing what existed at fork time (the common ancestor), we can't distinguish these cases. This is why git uses three-way merge.

---

## 6. What's Missing vs Git

| Concept | Git | Strata | Gap |
|---------|-----|--------|-----|
| **Content storage** | Content-addressable blobs | MVCC versioned values | ✅ Equivalent |
| **Commit objects** | Explicit commit DAG with parent pointers | Global commit_id counter | Different model, functionally equivalent |
| **Branch refs** | Named pointers to commits | Named isolated namespaces | ✅ Present |
| **Fork point** | Natural from commit parents | **NOT RECORDED** | **Critical gap** |
| **Merge base (LCA)** | LCA algorithm on commit DAG | **NOT IMPLEMENTED** | **Critical gap** |
| **Three-way diff** | diff(base, ours) + diff(base, theirs) | Only two-way diff | **Critical gap** |
| **Delete propagation** | Natural from three-way merge | **BROKEN** (ignored) | **Critical gap** |
| **Tombstones** | Not needed (content-addressed) | Versioned tombstones exist but hidden from diff | Underutilized |
| **Version GC safety** | Refcounting / reachability | No pinning of merge-base versions | Future concern |

---

## 7. Available MVCC Primitives (What We Can Build On)

| Capability | Method | Location |
|-----------|--------|----------|
| Read any key at specific version | `get_versioned_from_branch(branch, key, max_version)` | segmented/mod.rs:1107 |
| Read any key at specific timestamp | `get_at_timestamp(key, max_ts)` | segmented/mod.rs:480 |
| List all live keys (filters tombstones) | `list_branch_inner(branch)` | segmented/mod.rs:325 |
| List all live keys of a type | `list_by_type(branch_id, type_tag)` | segmented/mod.rs:370 |
| List keys at a timestamp | `list_by_type_at_timestamp(branch_id, type_tag, ts)` | segmented/mod.rs:418 |
| Full version history of a key (includes tombstones) | `get_history(key, before_version)` | segmented/mod.rs:1319 |
| MVCC snapshot iterator | `MvccIterator::new(merge, max_version)` | merge_iter.rs:83 |
| Get current global version | `db.current_version()` | database/mod.rs:1042 |
| Transaction with version return | `db.transaction_with_version(branch_id, f)` | database/mod.rs:1978 |
| Delete (write tombstone) | `txn.delete(key)` | concurrency/transaction.rs |

### What's Missing in Storage

- **`list_branch_at_version(branch_id, max_version)`** — like `list_branch_inner` but with a specific `max_version` instead of `u64::MAX`, and WITHOUT filtering tombstones. Needed for ancestor state reconstruction. The internal machinery (`MvccIterator`) already supports this — we just need a public method that skips the tombstone filter.

---

## 8. The Key Insight

**We don't need to rewrite the storage model.** The MVCC system already preserves full version history. After `fork_branch()` copies data from parent to child:

- The **child's initial state** (at the fork transaction's commit_ids) IS the common ancestor
- Reading the child branch at `fork_version` gives us the ancestor state
- Reading the child at `u64::MAX` gives us the current target state
- Reading the source at `u64::MAX` gives us the current source state

If we record `fork_version` at fork time, we have everything needed for three-way merge — using infrastructure that already exists.

For repeated merges between the same branches, we record `merge_version` on merge events and use the most recent merge's version as the new base.

---

## 9. Test Coverage

34 existing tests in `branch_ops.rs`:
- **Fork:** 6 tests (data copy, multi-space, source unchanged, vectors searchable)
- **Diff:** 15 tests (empty, populated, modifications, filters, as_of, value fidelity)
- **Merge:** 13 tests (LWW, strict, conflicts, binary keys, vectors, metadata)

**Notable gap:** No test for delete propagation through merge (because it doesn't work).
