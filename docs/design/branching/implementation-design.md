# Three-Way Merge: Implementation Design

**Date:** 2026-03-18
**Issue:** #1537 (branch_merge does not propagate deletes)
**Prerequisites:** [Current State Analysis](current-state.md), [Git Reference Architecture](git-reference-architecture.md)

---

## 1. Design Philosophy

Three constraints shape this design:

1. **The storage architecture is mid-rewrite.** The engine is being rearchitected for billion-scale (Epics 26-31: dynamic level sizing, prefix compression, L0 pinning, partitioned bloom filters). Branch isolation is deliberately O(n) fork during this period. **This design does not depend on COW** — it works with both deep-copy fork and COW fork. See [`cow-branching.md`](cow-branching.md) for the O(1) COW branching design (elevated to P0 as of 2026-03-19). Under COW, the ancestor state at `fork_version` is accessed through inherited segment layers rather than deep-copied entries — the multi-layer read path handles this transparently.

2. **Leverage existing MVCC.** Strata already has global monotonic versions, point-in-time reads, and versioned tombstones. The missing pieces are metadata (recording fork/merge versions) and algorithm (three-way diff/merge). No new storage primitives are needed.

3. **Use the `_system_` branch.** The `_system_` branch is an immutable, capability-gated namespace with the full Strata primitive set (KV, JSON, State, Event, Graph). The branch DAG already lives here as a graph. We extend it — not replace it.

---

## 2. What Needs to Change

### 2.1 DAG: Add Version Tracking

**Current state:** Fork and merge events in `_branch_dag` record timestamps but no global version numbers.

**Change:** Add `fork_version` and `merge_version` properties to DAG event nodes.

```
Fork event properties (today):
  { timestamp, message?, creator? }

Fork event properties (new):
  { timestamp, fork_version, message?, creator? }
  ─────────────────────────────
  fork_version = db.current_version() captured BEFORE the fork transaction

Merge event properties (today):
  { timestamp, keys_applied, spaces_merged, conflicts, strategy?, message?, creator? }

Merge event properties (new):
  { timestamp, merge_version, keys_applied, spaces_merged, conflicts, strategy?, message?, creator? }
  ─────────────────────────────
  merge_version = commit_version returned by the merge transaction
```

**Why `fork_version` is captured BEFORE the fork transaction:**
The fork transaction writes copies of all keys with NEW commit_ids. We need the version that represents the source branch's state at the moment of forking — i.e., the last committed version before the fork writes. This is the common ancestor.

**Why `merge_version` is the merge transaction's commit_version:**
After a merge, the merge version becomes the new base for future merges between the same branch pair. It represents the state of the target branch after incorporating the source's changes.

### 2.2 Records: Add Version Fields

```rust
// branch_dag.rs — extend existing structs

pub struct ForkRecord {
    pub event_id: DagEventId,
    pub parent: String,
    pub child: String,
    pub timestamp: u64,
    pub fork_version: Option<u64>,  // NEW — global version at fork time
    pub message: Option<String>,
    pub creator: Option<String>,
}

pub struct MergeRecord {
    pub event_id: DagEventId,
    pub source: String,
    pub target: String,
    pub timestamp: u64,
    pub merge_version: Option<u64>,  // NEW — commit version of the merge txn
    pub strategy: Option<String>,
    pub keys_applied: Option<u64>,
    pub spaces_merged: Option<u64>,
    pub conflicts: Option<u64>,
    pub message: Option<String>,
    pub creator: Option<String>,
}
```

`Option<u64>` for backward compatibility — existing DAG events from before this change will have `None`.

### 2.3 DAG Write Helpers: Pass Version

```rust
// dag_record_fork — add fork_version parameter
pub fn dag_record_fork(
    db: &Arc<Database>,
    parent: &str,
    child: &str,
    fork_version: u64,        // NEW
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    // ... existing code ...
    let mut props = serde_json::json!({
        "timestamp": now,
        "fork_version": fork_version,  // NEW
    });
    // ...
}

// dag_record_merge — add merge_version parameter
pub fn dag_record_merge(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    merge_info: &MergeInfo,
    merge_version: u64,        // NEW
    strategy: Option<&str>,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    // ... existing code ...
    let mut props = serde_json::json!({
        "timestamp": now,
        "merge_version": merge_version,  // NEW
        "keys_applied": merge_info.keys_applied,
        // ...
    });
    // ...
}
```

### 2.4 DAG Read Helpers: Extract Version

```rust
// find_fork_origin — extract fork_version from properties
fn find_fork_origin(db: &Arc<Database>, name: &str) -> StrataResult<Option<ForkRecord>> {
    // ... existing graph traversal ...
    Ok(Some(ForkRecord {
        // ... existing fields ...
        fork_version: props
            .and_then(|p| p.get("fork_version"))
            .and_then(|v| v.as_u64()),  // NEW
    }))
}

// find_merge_history — extract merge_version from properties
fn find_merge_history(db: &Arc<Database>, name: &str) -> StrataResult<Vec<MergeRecord>> {
    // ... existing graph traversal ...
    records.push(MergeRecord {
        // ... existing fields ...
        merge_version: props
            .and_then(|p| p.get("merge_version"))
            .and_then(|v| v.as_u64()),  // NEW
    });
}
```

### 2.5 Fork: Capture Version

```rust
// branch_ops.rs — fork_branch (lines 240-338)
pub fn fork_branch(db: &Arc<Database>, source: &str, dest: &str) -> Result<ForkResult> {
    // ... existing validation ...

    // NEW: capture version BEFORE the fork transaction
    let fork_version = db.current_version();

    // ... existing deep copy logic ...

    Ok(ForkResult {
        // ... existing fields ...
        fork_version,  // NEW
    })
}
```

The executor handler then passes `fork_version` to `dag_record_fork()`.

---

## 3. Merge Base Computation

### 3.1 The Algorithm

For two branches A and B, the **merge base** is the version at which they last shared state. This is determined by walking the DAG:

```
MERGE_BASE(source, target):

1. Find all fork/merge events between source and target
   by walking the DAG (graph edges on _branch_dag)

2. The merge base version is the MOST RECENT of:
   a. The fork_version where target was forked from source
      (or source from target)
   b. The merge_version of the most recent merge between
      source and target (in either direction)

3. If no fork or merge relationship exists → error
   (can't merge unrelated branches)
```

### 3.2 Which Branch Holds the Ancestor State?

**Key insight from the current-state analysis:** After `fork_branch()` deep-copies data from parent to child, the CHILD branch's initial state (at `fork_version`) IS the common ancestor. Reading the child at `fork_version` via MVCC gives us the ancestor snapshot.

But there's a subtlety with direction:

| Scenario | Ancestor is on... |
|----------|-------------------|
| `fork(main → feature)` then `merge(feature → main)` | `feature` at `fork_version` |
| `fork(main → feature)` then `merge(main → feature)` | `feature` at `fork_version` |
| After first merge, subsequent merges | `target` at `merge_version` |

In all cases, the ancestor state can be reconstructed from MVCC point-in-time reads. The question is which branch ID to use for the read.

**For fork:** The forked branch (child) received copies of all parent data at `fork_version`. So the child branch at `fork_version` = ancestor state.

**For repeated merges:** After merging source→target, the target branch at `merge_version` contains the merged state. This becomes the ancestor for the next merge.

### 3.3 Implementation

```rust
// NEW: branch_ops.rs

/// The merge base: which branch to read at which version for the ancestor state.
pub struct MergeBase {
    /// The branch whose data at `version` represents the common ancestor.
    pub branch_id: String,
    /// The version to read the ancestor at (for MVCC snapshot).
    pub version: u64,
    /// How this base was determined.
    pub source: MergeBaseSource,
}

pub enum MergeBaseSource {
    /// From a fork event (first merge between these branches).
    Fork { event_id: DagEventId },
    /// From a previous merge event (repeated merge).
    PreviousMerge { event_id: DagEventId },
}

/// Compute the merge base for merging `source` into `target`.
pub fn compute_merge_base(
    db: &Arc<Database>,
    source: &str,
    target: &str,
) -> Result<MergeBase> {
    // 1. Get merge history between these branches (both directions)
    let source_info = dag_get_branch_info(db, source)?
        .ok_or_else(|| error("source branch not found in DAG"))?;
    let target_info = dag_get_branch_info(db, target)?
        .ok_or_else(|| error("target branch not found in DAG"))?;

    // 2. Find the most recent merge between source and target
    //    Check merges where source→target OR target→source
    let mut best_merge: Option<&MergeRecord> = None;

    for m in &source_info.merges {
        if m.target == target {
            if let Some(v) = m.merge_version {
                if best_merge.map_or(true, |b| b.merge_version.unwrap_or(0) < v) {
                    best_merge = Some(m);
                }
            }
        }
    }
    // Also check if target was merged into source
    for m in &target_info.merges {
        if m.target == source {
            if let Some(v) = m.merge_version {
                if best_merge.map_or(true, |b| b.merge_version.unwrap_or(0) < v) {
                    best_merge = Some(m);
                }
            }
        }
    }

    // 3. If we have a previous merge, use its version as the base
    if let Some(m) = best_merge {
        let merge_version = m.merge_version.unwrap(); // checked above
        return Ok(MergeBase {
            // After merge(A→B), B at merge_version has the merged state.
            // After merge(B→A), A at merge_version has the merged state.
            branch_id: m.target.clone(),
            version: merge_version,
            source: MergeBaseSource::PreviousMerge {
                event_id: m.event_id.clone(),
            },
        });
    }

    // 4. No previous merge — find the fork relationship
    //    Case A: target was forked from source
    if let Some(fork) = &target_info.forked_from {
        if fork.parent == source {
            if let Some(v) = fork.fork_version {
                return Ok(MergeBase {
                    branch_id: target.to_string(), // child has the ancestor data
                    version: v,
                    source: MergeBaseSource::Fork {
                        event_id: fork.event_id.clone(),
                    },
                });
            }
        }
    }
    //    Case B: source was forked from target
    if let Some(fork) = &source_info.forked_from {
        if fork.parent == target {
            if let Some(v) = fork.fork_version {
                return Ok(MergeBase {
                    branch_id: source.to_string(), // child has the ancestor data
                    version: v,
                    source: MergeBaseSource::Fork {
                        event_id: fork.event_id.clone(),
                    },
                });
            }
        }
    }

    // 5. Walk further up the fork chain for transitive relationships
    //    (e.g., A forked from B, B forked from C — merging A into C)
    //    For MVP, reject this case. Transitive merge base is a future enhancement.

    Err(error("no fork or merge relationship found between branches"))
}
```

### 3.4 Transitive Relationships (Future)

For MVP, we only support merging branches that are directly related by fork or have been previously merged. Transitive relationships (A forked from B, B forked from C, merge A into C) require walking the full fork chain and are deferred.

---

## 4. Storage: Version-Scoped Listing with Tombstones

### 4.1 The Missing Primitive

The current `list_branch_inner()` (segmented/mod.rs:325) always:
- Uses `u64::MAX` as `max_version` (sees all writes)
- Filters tombstones (line 356: `if entry.is_tombstone { return None; }`)

For three-way merge, we need to list a branch's keys at a specific version INCLUDING tombstones. The internal machinery already supports this — `MvccIterator` accepts any `max_version`.

### 4.2 New Method

```rust
// segmented/mod.rs — new public method

/// List all entries for a branch at a specific version, INCLUDING tombstones.
///
/// Returns `(InternalKey, StoredEntry)` pairs where `commit_id <= max_version`.
/// Unlike `list_branch_inner`, tombstones are NOT filtered — the caller sees
/// deletion markers alongside live values. This is required for three-way
/// merge ancestor state reconstruction.
pub fn list_branch_at_version(
    &self,
    branch_id: &str,
    max_version: u64,
) -> Vec<(InternalKey, StoredEntry)> {
    let branch_prefix = /* branch_id as key prefix */;
    let merge = self.create_merge_iterator(branch_prefix);
    let mvcc = MvccIterator::new(merge, max_version);

    mvcc.collect()  // no tombstone filtering
}
```

> **COW compatibility note:** Under COW branching ([`cow-branching.md`](cow-branching.md)), a forked child has no deep-copied entries at `fork_version`. The ancestor data lives in inherited segment layers. This method MUST use the full merge iterator (including inherited layers with branch_id rewriting) to correctly reconstruct the ancestor state. The multi-layer read path handles this transparently — no special-casing needed in the merge algorithm.

### 4.3 Type-Scoped Variant

```rust
/// List entries of a specific type for a branch at a specific version,
/// INCLUDING tombstones.
pub fn list_by_type_at_version(
    &self,
    branch_id: &str,
    type_tag: u8,
    max_version: u64,
) -> Vec<(InternalKey, StoredEntry)> {
    let prefix = /* branch_id + type_tag */;
    let merge = self.create_merge_iterator(prefix);
    let mvcc = MvccIterator::new(merge, max_version);

    mvcc.collect()  // no tombstone filtering
}
```

---

## 5. Three-Way Diff

### 5.1 Data Structure

```rust
/// Result of a three-way diff for a single key.
#[derive(Debug, Clone, PartialEq)]
pub enum ThreeWayChange {
    /// No change — all three agree.
    Unchanged,
    /// Only source changed — auto-resolve by taking source value.
    SourceChanged { value: Value },
    /// Only target changed — auto-resolve by keeping target value.
    TargetChanged,
    /// Source deleted — auto-resolve by deleting.
    SourceDeleted,
    /// Target deleted — auto-resolve by keeping deletion.
    TargetDeleted,
    /// Both deleted — auto-resolve (no-op, already gone on both sides).
    BothDeleted,
    /// Both changed to same value — auto-resolve.
    BothChangedIdentically { value: Value },
    /// Source added (not in ancestor or target) — auto-resolve by applying.
    SourceAdded { value: Value },
    /// Target added (not in ancestor or source) — auto-resolve (already there).
    TargetAdded,
    /// Both added identically — auto-resolve.
    BothAddedIdentically { value: Value },
    /// CONFLICT: Both changed to different values.
    ConflictBothModified { source_value: Value, target_value: Value },
    /// CONFLICT: Both added different values.
    ConflictBothAdded { source_value: Value, target_value: Value },
    /// CONFLICT: Source modified, target deleted.
    ConflictSourceModifiedTargetDeleted { source_value: Value },
    /// CONFLICT: Target modified, source deleted.
    ConflictTargetModifiedSourceDeleted { target_value: Value },
}

impl ThreeWayChange {
    pub fn is_conflict(&self) -> bool {
        matches!(self,
            ThreeWayChange::ConflictBothModified { .. } |
            ThreeWayChange::ConflictBothAdded { .. } |
            ThreeWayChange::ConflictSourceModifiedTargetDeleted { .. } |
            ThreeWayChange::ConflictTargetModifiedSourceDeleted { .. }
        )
    }
}

/// A three-way diff entry for a specific key.
pub struct ThreeWayDiffEntry {
    pub space: String,
    pub key: String,
    pub type_tag: u8,
    pub change: ThreeWayChange,
}

/// Full three-way diff result.
pub struct ThreeWayDiff {
    pub merge_base: MergeBase,
    pub entries: Vec<ThreeWayDiffEntry>,
}
```

### 5.2 The Algorithm

```rust
/// Compute the three-way diff between source and target using the merge base.
pub fn three_way_diff(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    merge_base: &MergeBase,
) -> Result<ThreeWayDiff> {
    let storage = db.storage();

    let source_id = resolve_branch_name(source);
    let target_id = resolve_branch_name(target);
    let ancestor_id = resolve_branch_name(&merge_base.branch_id);

    let mut entries = Vec::new();

    for type_tag in ALL_DATA_TYPE_TAGS {
        // 1. Read ancestor state (at merge base version, WITH tombstones)
        let ancestor_entries = storage.list_by_type_at_version(
            ancestor_id, type_tag, merge_base.version
        );

        // 2. Read source state (current, WITHOUT tombstones — live keys only)
        let source_entries = storage.list_by_type(source_id, type_tag);

        // 3. Read target state (current, WITHOUT tombstones — live keys only)
        let target_entries = storage.list_by_type(target_id, type_tag);

        // 4. Build lookup maps: (space, user_key) → Value
        let ancestor_map = build_key_map(&ancestor_entries);  // includes tombstones
        let source_map = build_key_map(&source_entries);
        let target_map = build_key_map(&target_entries);

        // 5. Union of all keys across the three snapshots
        let all_keys: HashSet<(String, String)> = ancestor_map.keys()
            .chain(source_map.keys())
            .chain(target_map.keys())
            .cloned()
            .collect();

        // 6. Apply decision matrix for each key
        for (space, key) in all_keys {
            let ancestor = ancestor_map.get(&(space.clone(), key.clone()));
            let source = source_map.get(&(space.clone(), key.clone()));
            let target = target_map.get(&(space.clone(), key.clone()));

            let change = classify_change(ancestor, source, target);

            if !matches!(change, ThreeWayChange::Unchanged) {
                entries.push(ThreeWayDiffEntry {
                    space: space.clone(),
                    key: key.clone(),
                    type_tag,
                    change,
                });
            }
        }
    }

    Ok(ThreeWayDiff { merge_base: merge_base.clone(), entries })
}
```

### 5.3 The Decision Matrix Implementation

This implements the 14-case matrix from the git reference architecture:

```rust
/// Classify a single key's change across ancestor, source, and target.
///
/// Ancestor entries may be tombstones (is_tombstone=true), which means the key
/// was explicitly deleted before or at the ancestor version. For the purpose of
/// this classification, a tombstoned ancestor key is treated as ∅ (absent).
fn classify_change(
    ancestor: Option<&KeyState>,  // None or Some(value/tombstone)
    source: Option<&Value>,       // None = absent (live listing, no tombstones)
    target: Option<&Value>,       // None = absent
) -> ThreeWayChange {
    // Normalize ancestor: tombstone → None (treat as "didn't exist")
    let base = match ancestor {
        Some(ks) if ks.is_tombstone => None,
        Some(ks) => Some(&ks.value),
        None => None,
    };

    match (base, source, target) {
        // All three agree (or all absent)
        (None, None, None) => ThreeWayChange::Unchanged,
        (Some(b), Some(s), Some(t)) if b == s && s == t => ThreeWayChange::Unchanged,

        // Only source changed
        (Some(b), Some(s), Some(t)) if b == t && b != s => {
            ThreeWayChange::SourceChanged { value: s.clone() }
        }
        // Only target changed
        (Some(b), Some(s), Some(t)) if b == s && b != t => {
            ThreeWayChange::TargetChanged
        }
        // Both changed identically
        (Some(b), Some(s), Some(t)) if b != s && s == t => {
            ThreeWayChange::BothChangedIdentically { value: s.clone() }
        }
        // Both changed differently → CONFLICT
        (Some(_), Some(s), Some(t)) => {
            ThreeWayChange::ConflictBothModified {
                source_value: s.clone(),
                target_value: t.clone(),
            }
        }

        // Source deleted (was in base and target, not in source)
        (Some(b), None, Some(t)) if b == t => ThreeWayChange::SourceDeleted,
        // Source modified, target deleted → CONFLICT
        (Some(_), Some(s), None) if base.map_or(false, |b| b != s) => {
            ThreeWayChange::ConflictSourceModifiedTargetDeleted {
                source_value: s.clone(),
            }
        }
        // Source unchanged, target deleted
        (Some(b), Some(s), None) if b == s => ThreeWayChange::TargetDeleted,
        // Target modified, source deleted → CONFLICT
        (Some(_), None, Some(t)) if base.map_or(false, |b| b != t) => {
            ThreeWayChange::ConflictTargetModifiedSourceDeleted {
                target_value: t.clone(),
            }
        }

        // Both deleted
        (Some(_), None, None) => ThreeWayChange::BothDeleted,

        // Source added (not in base or target)
        (None, Some(s), None) => {
            ThreeWayChange::SourceAdded { value: s.clone() }
        }
        // Target added (not in base or source)
        (None, None, Some(_)) => ThreeWayChange::TargetAdded,
        // Both added identically
        (None, Some(s), Some(t)) if s == t => {
            ThreeWayChange::BothAddedIdentically { value: s.clone() }
        }
        // Both added differently → CONFLICT
        (None, Some(s), Some(t)) => {
            ThreeWayChange::ConflictBothAdded {
                source_value: s.clone(),
                target_value: t.clone(),
            }
        }

        // Fallback (should be unreachable if all cases covered)
        _ => ThreeWayChange::Unchanged,
    }
}
```

---

## 6. Three-Way Merge

### 6.1 Merge Strategies

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MergeStrategy {
    /// Reject on any conflict.
    Strict,
    /// Auto-resolve conflicts by taking the source (incoming) value.
    /// This is the equivalent of git's "theirs" when merging source into target.
    SourceWins,
    /// Auto-resolve conflicts by keeping the target (existing) value.
    /// This is the equivalent of git's "ours" when merging source into target.
    TargetWins,
}
```

`LastWriterWins` is renamed to `SourceWins` for clarity. `TargetWins` is new — it allows merging while always preferring the existing value on conflicts.

### 6.2 The Algorithm

```rust
/// Merge source branch into target branch using three-way merge.
pub fn merge_branches_three_way(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    strategy: MergeStrategy,
) -> Result<MergeResult> {
    // 1. Compute merge base
    let merge_base = compute_merge_base(db, source, target)?;

    // 2. Compute three-way diff
    let diff = three_way_diff(db, source, target, &merge_base)?;

    // 3. Separate auto-resolved changes from conflicts
    let mut to_apply: Vec<MergeAction> = Vec::new();
    let mut conflicts: Vec<ConflictEntry> = Vec::new();

    for entry in &diff.entries {
        match &entry.change {
            // Auto-resolved: apply source's value
            ThreeWayChange::SourceChanged { value } |
            ThreeWayChange::SourceAdded { value } => {
                to_apply.push(MergeAction::Put {
                    space: entry.space.clone(),
                    key: entry.key.clone(),
                    type_tag: entry.type_tag,
                    value: value.clone(),
                });
            }

            // Auto-resolved: delete on target
            ThreeWayChange::SourceDeleted => {
                to_apply.push(MergeAction::Delete {
                    space: entry.space.clone(),
                    key: entry.key.clone(),
                    type_tag: entry.type_tag,
                });
            }

            // Auto-resolved: no action needed (target already has the right state)
            ThreeWayChange::Unchanged |
            ThreeWayChange::TargetChanged |
            ThreeWayChange::TargetDeleted |
            ThreeWayChange::BothDeleted |
            ThreeWayChange::TargetAdded |
            ThreeWayChange::BothChangedIdentically { .. } |
            ThreeWayChange::BothAddedIdentically { .. } => {
                // Target already has the correct value — no action needed
            }

            // Conflicts: handle based on strategy
            ThreeWayChange::ConflictBothModified { source_value, target_value } => {
                match strategy {
                    MergeStrategy::Strict => {
                        conflicts.push(ConflictEntry {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                            kind: ConflictKind::BothModified,
                            source_value: Some(source_value.clone()),
                            target_value: Some(target_value.clone()),
                        });
                    }
                    MergeStrategy::SourceWins => {
                        to_apply.push(MergeAction::Put {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                            value: source_value.clone(),
                        });
                    }
                    MergeStrategy::TargetWins => {
                        // Target already has its value — no action
                    }
                }
            }

            ThreeWayChange::ConflictBothAdded { source_value, target_value } => {
                match strategy {
                    MergeStrategy::Strict => {
                        conflicts.push(ConflictEntry {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                            kind: ConflictKind::BothAdded,
                            source_value: Some(source_value.clone()),
                            target_value: Some(target_value.clone()),
                        });
                    }
                    MergeStrategy::SourceWins => {
                        to_apply.push(MergeAction::Put {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                            value: source_value.clone(),
                        });
                    }
                    MergeStrategy::TargetWins => {}
                }
            }

            ThreeWayChange::ConflictSourceModifiedTargetDeleted { source_value } => {
                match strategy {
                    MergeStrategy::Strict => {
                        conflicts.push(ConflictEntry {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                            kind: ConflictKind::ModifyDeleteConflict,
                            source_value: Some(source_value.clone()),
                            target_value: None,
                        });
                    }
                    MergeStrategy::SourceWins => {
                        to_apply.push(MergeAction::Put {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                            value: source_value.clone(),
                        });
                    }
                    MergeStrategy::TargetWins => {
                        // Target deleted — keep deletion (no action)
                    }
                }
            }

            ThreeWayChange::ConflictTargetModifiedSourceDeleted { target_value } => {
                match strategy {
                    MergeStrategy::Strict => {
                        conflicts.push(ConflictEntry {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                            kind: ConflictKind::ModifyDeleteConflict,
                            source_value: None,
                            target_value: Some(target_value.clone()),
                        });
                    }
                    MergeStrategy::SourceWins => {
                        // Source deleted — propagate deletion
                        to_apply.push(MergeAction::Delete {
                            space: entry.space.clone(),
                            key: entry.key.clone(),
                            type_tag: entry.type_tag,
                        });
                    }
                    MergeStrategy::TargetWins => {
                        // Target modified — keep modification (no action)
                    }
                }
            }
        }
    }

    // 4. If strict and conflicts exist, return early
    if strategy == MergeStrategy::Strict && !conflicts.is_empty() {
        return Ok(MergeResult {
            success: false,
            merge_version: None,
            keys_applied: 0,
            keys_deleted: 0,
            conflicts,
            spaces_merged: 0,
        });
    }

    // 5. Apply changes in a single transaction
    let (puts, deletes) = partition_actions(&to_apply);
    let (_, merge_version) = db.transaction_with_version(target_id, |txn| {
        for put in &puts {
            txn.put(put.encoded_key(), put.value.clone());
        }
        for del in &deletes {
            txn.delete(del.encoded_key());
        }
        Ok(())
    })?;

    // 6. Record merge in DAG with version
    dag_record_merge(db, source, target, &merge_info, merge_version, ...)?;

    Ok(MergeResult {
        success: true,
        merge_version: Some(merge_version),
        keys_applied: puts.len() as u64,
        keys_deleted: deletes.len() as u64,
        conflicts,
        spaces_merged: count_unique_spaces(&to_apply),
    })
}
```

### 6.3 Supporting Types

```rust
enum MergeAction {
    Put { space: String, key: String, type_tag: u8, value: Value },
    Delete { space: String, key: String, type_tag: u8 },
}

#[derive(Debug, Clone)]
pub enum ConflictKind {
    BothModified,
    BothAdded,
    ModifyDeleteConflict,
}

#[derive(Debug, Clone)]
pub struct ConflictEntry {
    pub space: String,
    pub key: String,
    pub type_tag: u8,
    pub kind: ConflictKind,
    pub source_value: Option<Value>,
    pub target_value: Option<Value>,
}

pub struct MergeResult {
    pub success: bool,
    pub merge_version: Option<u64>,
    pub keys_applied: u64,
    pub keys_deleted: u64,
    pub conflicts: Vec<ConflictEntry>,
    pub spaces_merged: u64,
}
```

---

## 7. Version Pinning for GC Safety

### 7.1 The Problem

Strata's compaction process may garbage-collect old MVCC versions. If the ancestor version is compacted away, we lose the ability to reconstruct the ancestor state, breaking merge base computation.

### 7.2 The Solution

**Pin versions referenced by active branches.** When computing what versions are safe to GC, the compactor must respect:

1. All `fork_version` values from fork events where the child branch is still Active
2. The most recent `merge_version` between any pair of Active branches

These are read from the `_branch_dag` graph at compaction time.

### 7.3 Implementation (Deferred)

This is not urgent because:
- Compaction currently does not GC old MVCC versions (it deduplicates within segments but preserves the version chain)
- When MVCC GC is implemented, version pinning must be part of that design

**When the time comes:**

```rust
/// Compute the minimum version that must be retained across all branches.
pub fn compute_gc_safe_version(db: &Arc<Database>) -> u64 {
    let branch_index = BranchIndex::new(db.clone());
    let active_branches = branch_index.list_active();

    let mut min_version = u64::MAX;

    for branch in &active_branches {
        if let Ok(Some(info)) = dag_get_branch_info(db, &branch.name) {
            // Pin fork versions
            if let Some(fork) = &info.forked_from {
                if let Some(v) = fork.fork_version {
                    min_version = min_version.min(v);
                }
            }
            // Pin most recent merge version per pair
            for merge in &info.merges {
                if let Some(v) = merge.merge_version {
                    min_version = min_version.min(v);
                }
            }
        }
    }

    min_version
}
```

---

## 8. `_system_` Branch Usage

### 8.1 Current Usage

| Primitive | What's Stored | Location |
|-----------|---------------|----------|
| Graph (`_branch_dag`) | Branch nodes, fork events, merge events, status | `branch_dag.rs` |

### 8.2 Extended Usage (This Design)

| Primitive | What's Stored | Purpose |
|-----------|---------------|---------|
| Graph (`_branch_dag`) | Branch nodes + fork/merge events **with versions** | Merge base computation |
| Events | Append-only branch audit log | Immutable history of all branch operations |

### 8.3 Branch Audit Log via Events (Future Enhancement)

The `_system_` branch has an Events primitive — an append-only log. This is a natural fit for recording an immutable audit trail of branch operations:

```
Event { type: "branch.fork",   payload: { parent, child, fork_version, ... } }
Event { type: "branch.merge",  payload: { source, target, merge_version, strategy, ... } }
Event { type: "branch.delete", payload: { name, deleted_at, ... } }
Event { type: "branch.create", payload: { name, created_at, ... } }
```

**Why events alongside the graph?**
- The graph is mutable (nodes get updated — e.g., status changes). Events are append-only.
- Events give temporal ordering naturally (sequence numbers).
- Events can be streamed/replayed for debugging or replication.

This is a future enhancement, not required for three-way merge. The graph alone is sufficient.

---

## 9. Public API Changes

### 9.1 Diff API

```rust
// Current
pub fn diff(&self, other: &str) -> Result<BranchDiff>

// New (backward compatible — adds optional parameter)
pub fn diff(&self, other: &str) -> Result<BranchDiff>           // two-way (unchanged)
pub fn diff_three_way(&self, other: &str) -> Result<ThreeWayDiff>  // new
```

### 9.2 Merge API

```rust
// Current
pub fn merge(&self, source: &str, strategy: MergeStrategy) -> Result<MergeInfo>

// New (replaces implementation, keeps surface API similar)
pub fn merge(&self, source: &str, strategy: MergeStrategy) -> Result<MergeResult>
```

The `merge()` method switches to three-way merge internally. The strategy enum changes from `{ Strict, LastWriterWins }` to `{ Strict, SourceWins, TargetWins }`. `LastWriterWins` becomes an alias for `SourceWins` for backward compatibility.

### 9.3 Merge Base API (New)

```rust
/// Get the merge base for two branches (useful for debugging/UI).
pub fn merge_base(&self, other: &str) -> Result<MergeBase>
```

---

## 10. Implementation Order

### Phase 1: Foundation (Fixes #1537)

1. **Add `fork_version` to DAG** — modify `dag_record_fork()`, `ForkRecord`, `find_fork_origin()`
2. **Capture version in `fork_branch()`** — call `db.current_version()` before fork transaction
3. **Add `merge_version` to DAG** — modify `dag_record_merge()`, `MergeRecord`, `find_merge_history()`
4. **Add `list_by_type_at_version()`** — new storage method, no tombstone filtering
5. **Implement `compute_merge_base()`** — DAG walk for fork/merge relationships
6. **Implement `classify_change()`** — the 14-case decision matrix
7. **Implement `three_way_diff()`** — builds on #4 and #6
8. **Implement `merge_branches_three_way()`** — builds on #5 and #7
9. **Wire into executor** — replace old `merge_branches()` calls
10. **Tests** — new test suite covering all 14 decision matrix cases, delete propagation, repeated merges

### Phase 2: Polish

11. **Add `diff_three_way()` to public API**
12. **Add `merge_base()` to public API**
13. **Rename `LastWriterWins` → `SourceWins`** (with alias for compat)
14. **Add `TargetWins` strategy**

### Phase 3: Future (Not Blocked)

15. **Branch audit log via Events** — append-only record on `_system_` branch
16. **Version pinning for GC** — when MVCC GC is implemented
17. **Transitive merge base** — for merging non-directly-related branches
18. **O(1) COW branching** — elevated to P0, see [`cow-branching.md`](cow-branching.md)

---

## 11. Testing Strategy

### Unit Tests

| Test | What It Verifies |
|------|-----------------|
| `classify_change` × 14 cases | Every row of the decision matrix produces the correct `ThreeWayChange` |
| `compute_merge_base_fork` | Fork relationship returns correct branch_id and version |
| `compute_merge_base_after_merge` | Previous merge version supersedes fork version |
| `compute_merge_base_reverse_fork` | Works when source was forked from target (not vice versa) |
| `compute_merge_base_unrelated` | Returns error for unrelated branches |

### Integration Tests

| Test | What It Verifies |
|------|-----------------|
| `merge_propagates_deletes` | Key deleted on source → deleted on target after merge (**#1537**) |
| `merge_preserves_target_additions` | Key created on target after fork → NOT deleted by merge |
| `merge_detects_modify_modify_conflict` | Same key changed on both → conflict in Strict mode |
| `merge_detects_modify_delete_conflict` | Key modified on one, deleted on other → conflict |
| `merge_source_wins_on_conflict` | SourceWins strategy auto-resolves by taking source |
| `merge_target_wins_on_conflict` | TargetWins strategy auto-resolves by keeping target |
| `merge_both_added_same_value` | Both branches add same key with same value → no conflict |
| `merge_both_deleted` | Both branches delete same key → clean merge |
| `merge_repeated` | Fork → modify → merge → modify again → merge again (uses previous merge as base) |
| `merge_across_spaces` | Keys in different spaces merge independently |
| `merge_across_types` | KV, JSON, State, Event, Vector all participate |
| `fork_records_version` | Fork captures `fork_version` in DAG |
| `merge_records_version` | Merge captures `merge_version` in DAG |

---

## 12. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Old DAG events lack version fields | Certain (backward compat) | Medium | `Option<u64>` fields; error on merge if base version unknown |
| Performance of full-branch listing for diff | Low (branches are typically small) | Medium | Profile; optimize with targeted key-range reads if needed |
| Ancestor MVCC data compacted away | Low (no MVCC GC today) | High | Defer; implement version pinning when GC ships |
| Multiple merge bases (criss-cross) | Very low (unlikely in embedded DB) | Low | Reject; transitive merge base is Phase 3 |

---

## Sources

- [Current State Analysis](current-state.md) — audit of existing branching implementation
- [Git Reference Architecture](git-reference-architecture.md) — git's architecture distilled for Strata
- Issue #1537 — branch_merge does not propagate deletes
