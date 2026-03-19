# Git-Like Branching: Phased Implementation Plan

## Context

Issue #1537: `branch_merge` does not propagate deletes. Root cause: the branching system uses two-way diff (no common ancestor), so it can't distinguish "deleted on source" from "created on target after fork." This is the tip of a larger problem — Strata's branching lacks the git fundamentals (merge base, three-way merge, version-tracked DAG) needed for Strata AI to operate like Claude Code operates on a codebase.

**Design docs:** `docs/design/branching/` — `current-state.md`, `git-reference-architecture.md`, `implementation-design.md`, `git-command-survey.md`

**Goal:** Build git-like branching primitives in phases, where each phase has a logical end point and delivers standalone value. Strata AI (built on Anthropic Agent SDK + Claude API) is the primary consumer.

---

## Phase 1: Three-Way Merge Foundation

**Fixes #1537. Enables: speculative execution, branch-per-conversation, hypothesis testing.**

After this phase, `branch merge` correctly propagates deletes, detects real conflicts, and supports SourceWins/TargetWins strategies. Strata AI can fork → operate → diff → merge safely.

### Epic 1.1: Version-Tracked DAG

Record `fork_version` and `merge_version` on DAG events so we can compute merge bases.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Add `fork_version: Option<u64>` to `ForkRecord` | `crates/engine/src/branch_dag.rs:114` | Option for backward compat with existing DAG events |
| 2 | Add `merge_version: Option<u64>` to `MergeRecord` | `crates/engine/src/branch_dag.rs:130` | Same |
| 3 | Add `fork_version: u64` param to `dag_record_fork()` | `crates/engine/src/branch_dag.rs:335` | Write `"fork_version": fork_version` to node props |
| 4 | Add `merge_version: u64` param to `dag_record_merge()` | `crates/engine/src/branch_dag.rs:396` | Write `"merge_version": merge_version` to node props |
| 5 | Extract `fork_version` in `find_fork_origin()` | `crates/engine/src/branch_dag.rs:593` | Read from node props, same pattern as timestamp |
| 6 | Extract `merge_version` in `find_merge_history()` | `crates/engine/src/branch_dag.rs:648` | Read from node props |
| 7 | Update tests for version fields | `crates/engine/src/branch_dag.rs:771+` | Verify round-trip of version fields |

### Epic 1.2: Fork Version Capture

Capture `fork_version` in `fork_branch()` and pass it through the executor.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Add `fork_version: u64` to `ForkInfo` struct | `crates/engine/src/branch_ops.rs:45` | |
| 2 | Capture `db.current_version()` BEFORE fork transaction | `crates/engine/src/branch_ops.rs:~250` | Before the copy loop |
| 3 | Return `fork_version` in `ForkInfo` | `crates/engine/src/branch_ops.rs:~330` | |
| 4 | Pass `fork_version` to `dag_record_fork()` in handler | `crates/executor/src/handlers/branch.rs:~245` | `info.fork_version` |
| 5 | Test: fork captures version, DAG stores it | `crates/engine/src/branch_ops.rs` (tests) | |

### Epic 1.3: Version-Scoped Listing (Tombstone-Aware)

Add `list_by_type_at_version()` to storage — like `list_by_type()` but with a specific `max_version` and WITHOUT filtering tombstones.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Add `list_by_type_at_version(branch_id, type_tag, max_version)` | `crates/storage/src/segmented/mod.rs` | Use `MvccIterator::new(merge, max_version)`, skip tombstone filter. Return `Vec<(Key, VersionedValue)>` with a flag indicating tombstone status |
| 2 | Expose tombstone status in return type | `crates/storage/src/segmented/mod.rs` | Either extend `VersionedValue` with `is_tombstone: bool` or return a new type |
| 3 | Test: list at version sees only entries ≤ version | `crates/storage/src/segmented/` (tests) | |
| 4 | Test: list at version includes tombstones | Same | Verify tombstoned key appears in results |

### Epic 1.4: Merge Base Computation

Implement `compute_merge_base()` — walk DAG to find LCA version.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Define `MergeBase` and `MergeBaseSource` structs | `crates/engine/src/branch_ops.rs` | `branch_id: String, version: u64, source: Fork\|PreviousMerge` |
| 2 | Implement `compute_merge_base(db, source, target)` | `crates/engine/src/branch_ops.rs` | Walk DAG: check merges (both directions), then fork origin. Most recent merge_version wins over fork_version. |
| 3 | Test: merge base from fork | Tests | Fork A→B, merge base = B at fork_version |
| 4 | Test: merge base from reverse fork | Tests | Fork B→A, merge A→B, base = A at fork_version |
| 5 | Test: merge base after previous merge | Tests | Fork→merge→modify→merge again, base = target at previous merge_version |
| 6 | Test: unrelated branches → error | Tests | |

### Epic 1.5: Three-Way Diff

Implement the 14-case decision matrix.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Define `ThreeWayChange` enum (14 variants) | `crates/engine/src/branch_ops.rs` | Unchanged, SourceChanged, TargetChanged, SourceDeleted, ..., ConflictBothModified, etc. |
| 2 | Define `ThreeWayDiffEntry` and `ThreeWayDiff` structs | Same | Entry = (space, key, type_tag, change). Diff = (merge_base, entries). |
| 3 | Implement `classify_change(ancestor, source, target)` | Same | Pure function, 14-case match. Tombstoned ancestor = absent. |
| 4 | Implement `three_way_diff(db, source, target, merge_base)` | Same | For each DATA_TYPE_TAG: read ancestor (at version, with tombstones), source (live), target (live). Build maps, union keys, classify. |
| 5 | Unit tests: all 14 cases of `classify_change` | Tests | One test per row of decision matrix |
| 6 | Integration test: three_way_diff across fork | Tests | Fork, modify both sides, verify correct classification |

### Epic 1.6: Three-Way Merge

Replace the broken two-way merge with three-way merge.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Add `SourceWins` and `TargetWins` to `MergeStrategy` | `crates/engine/src/branch_ops.rs:119` | Keep `LastWriterWins` as alias for `SourceWins` |
| 2 | Define `MergeResult` (replaces `MergeInfo` return) | Same | `success, merge_version, keys_applied, keys_deleted, conflicts, spaces_merged` |
| 3 | Implement `merge_branches_three_way()` | Same | Compute merge base → three-way diff → apply puts + deletes → record DAG with merge_version |
| 4 | Wire merge_version into `dag_record_merge()` call | `crates/executor/src/handlers/branch.rs` | Use `transaction_with_version()` to get merge_version |
| 5 | Replace old `merge_branches()` with new implementation | `crates/engine/src/branch_ops.rs` | Old function becomes a wrapper or is replaced |
| 6 | Update executor handler | `crates/executor/src/handlers/branch.rs:~288` | Call new merge, pass merge_version to DAG |
| 7 | Update public API | `crates/executor/src/api/branches.rs` | `merge()` returns `MergeResult`, strategy enum updated |
| 8 | Update CLI merge output | `crates/cli/` | Show keys_deleted count, conflict details |

### Epic 1.7: Test Suite

Comprehensive tests proving three-way merge works correctly.

| Test | What It Proves |
|------|---------------|
| `merge_propagates_deletes` | **#1537 fix** — key deleted on source appears deleted on target |
| `merge_preserves_target_additions` | Key created on target after fork is NOT deleted |
| `merge_detect_modify_modify_conflict` | Same key changed differently on both → Strict fails |
| `merge_detect_modify_delete_conflict` | Modified on one, deleted on other → Strict fails |
| `merge_source_wins_resolves_conflicts` | SourceWins takes source value on conflict |
| `merge_target_wins_resolves_conflicts` | TargetWins keeps target value on conflict |
| `merge_both_added_identically` | Both add same key with same value → clean merge |
| `merge_both_deleted` | Both delete same key → clean merge |
| `merge_repeated` | Fork → merge → modify → merge again (previous merge as base) |
| `merge_across_spaces` | Keys in different spaces merge independently |
| `merge_across_types` | KV, JSON, State, Event, Vector all participate |
| `merge_source_deletes_propagate_to_target` | Delete on source + unchanged on target → delete on target |
| `merge_target_deletes_not_overwritten` | Delete on target + unchanged on source → stays deleted |

---

## Phase 2: AI Agent Workflows

**Enables: safe rollback, selective changes, semantic bookmarks, full audit trail.**

After this phase, Strata AI can revert specific operations, cherry-pick changes between experiments, tag meaningful checkpoints, and produce complete provenance chains. These are the capabilities that make Strata AI operate like Claude Code.

### Epic 2.1: Branch Audit Log (Events on `_system_`)

Append-only event log recording every branch lifecycle operation.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Define event types and payload schemas | `crates/engine/src/branch_dag.rs` | `branch.create`, `branch.fork`, `branch.merge`, `branch.delete` |
| 2 | Emit events alongside DAG writes | `crates/executor/src/handlers/branch.rs` | `system_branch.event_append(...)` best-effort after each operation |
| 3 | Add `branch log` CLI command | `crates/cli/` | Read events, display chronological branch history |
| 4 | Add `branches().log()` public API | `crates/executor/src/api/branches.rs` | Query events on `_system_` branch |

### Epic 2.2: Tags

Named version bookmarks stored on `_system_` KV.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Tag operations in engine | `crates/engine/src/branch_ops.rs` or new `tag_ops.rs` | `create_tag(db, branch, name, version, message)`, `delete_tag`, `list_tags`, `resolve_tag` |
| 2 | Store as KV on `_system_`: `tag:{branch}:{name} → {version, message, timestamp}` | Engine layer | JSON-encoded value |
| 3 | Add `tag` CLI subcommands | `crates/cli/` | `strata tag create <name>`, `strata tag list`, `strata tag delete <name>` |
| 4 | Add `branches().tag()` / `branches().tags()` public API | `crates/executor/src/api/branches.rs` | |
| 5 | Emit `branch.tag` event on `_system_` | Handler | Audit trail |

### Epic 2.3: Revert

Undo a specific version range while preserving subsequent work.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Implement `revert_version_range(db, branch, from_version, to_version)` | `crates/engine/src/branch_ops.rs` | For each key modified in [from, to]: read value at (from-1) via MVCC, write it as current value |
| 2 | Need `list_keys_modified_in_range(branch, from, to)` storage method | `crates/storage/src/segmented/mod.rs` | Scan MVCC entries where `from <= commit_id <= to` |
| 3 | Add `branch revert` CLI command | `crates/cli/` | `strata branch revert --version <V>` or `--range <from>..<to>` |
| 4 | Add `branches().revert()` public API | `crates/executor/src/api/branches.rs` | |
| 5 | Emit `branch.revert` event on `_system_` | Handler | Record what was reverted and why |

### Epic 2.4: Cherry-Pick

Apply specific key changes from one branch to another without full merge.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Implement `cherry_pick(db, source, target, keys)` | `crates/engine/src/branch_ops.rs` | Read specified keys from source, write to target in single transaction |
| 2 | Implement `cherry_pick_from_diff(db, source, target, filter)` | Same | Use three-way diff but only apply entries matching filter (by space, key prefix, primitive type) |
| 3 | Add `branch cherry-pick` CLI command | `crates/cli/` | `strata branch cherry-pick <source> --keys key1,key2` or `--space <space>` |
| 4 | Add `branches().cherry_pick()` public API | `crates/executor/src/api/branches.rs` | |
| 5 | Emit `branch.cherry_pick` event on `_system_` | Handler | Record what was picked and from where |

### Epic 2.5: Three-Way Diff Public API

Expose three-way diff to users and Strata AI.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Add `branches().diff_three_way(a, b)` | `crates/executor/src/api/branches.rs` | Returns `ThreeWayDiff` with change classification per key |
| 2 | Add `branches().merge_base(a, b)` | Same | Returns `MergeBase` for debugging/UI |
| 3 | Add `branch diff3` CLI command | `crates/cli/` | Shows three-way diff with conflict markers |
| 4 | Add `branch merge-base` CLI command | Same | Shows merge base version and source |

### Epic 2.6: Notes

AI-authored annotations on specific versions, stored as JSON on `_system_`.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Store as JSON on `_system_`: `note:{branch}:{version}` | Engine layer | `{ message, author, timestamp, metadata }` |
| 2 | Add `branches().add_note()` / `branches().get_notes()` public API | `crates/executor/src/api/branches.rs` | |
| 3 | Add `branch note` CLI command | `crates/cli/` | `strata branch note add --version <V> "message"` |

---

## Phase 3: Advanced Git Semantics

**Enables: automated debugging, learned conflict resolution, operation replay.**

After this phase, Strata AI can binary-search through history to find data regressions, remember how users resolve conflicts and auto-apply those decisions, and replay sequences of operations on different base states.

### Epic 3.1: Bisect

Binary search through version history to find when data went wrong.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Implement bisect state machine | `crates/engine/src/branch_ops.rs` or new `bisect.rs` | `BisectState { branch, good, bad, current, history }` stored on `_system_` StateCell |
| 2 | `bisect_start(branch, good_version, bad_version)` | Engine | Initialize state, set current to midpoint |
| 3 | `bisect_step(branch, verdict: Good\|Bad)` | Engine | Narrow range, update current |
| 4 | `bisect_result(branch)` → version that introduced the issue | Engine | Return version + diff against previous |
| 5 | Add `branch bisect` CLI commands | `crates/cli/` | `bisect start`, `bisect good`, `bisect bad`, `bisect reset` |
| 6 | Add `branches().bisect_*()` public API | `crates/executor/src/api/branches.rs` | |

### Epic 3.2: Rerere (Reuse Recorded Resolution)

Remember conflict resolutions and auto-apply them.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Define resolution record format | Engine | `{ key_pattern, resolution: SourceWins\|TargetWins\|CustomValue, reason, created_by }` |
| 2 | Store on `_system_` KV: `rerere:{hash(key)}` | Engine | Hash of (space, type_tag, key) for lookup |
| 3 | Check rerere during merge conflict resolution | `merge_branches_three_way()` | Before marking as conflict, check if resolution exists |
| 4 | Record resolution after user resolves conflict | Handler | Write rerere entry when user explicitly resolves |
| 5 | Add `branch rerere` CLI commands | `crates/cli/` | `rerere list`, `rerere forget <key>` |

### Epic 3.3: Reflog

Complete operation log for recovery — every branch operation with before/after state.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Extend audit log events with before/after versions | `crates/executor/src/handlers/branch.rs` | Each event records `{ before_version, after_version, operation, ... }` |
| 2 | Add `branch reflog` CLI command | `crates/cli/` | Show all operations on a branch with versions |
| 3 | Add `branches().reflog()` public API | `crates/executor/src/api/branches.rs` | |
| 4 | Add `branch recover <reflog_entry>` | Engine + CLI | Revert to a specific reflog entry's state |

### ~~Epic 3.5: O(1) COW Branching~~ → **Elevated to P0**

> **Note (2026-03-19):** O(1) COW branching has been elevated to P0 priority and has its own design document: [`cow-branching.md`](cow-branching.md). It is no longer deferred to Phase 3. The storage architecture (Epics 26-31) has stabilized sufficiently to support COW.

### Epic 3.4: Version Pinning for GC

Ensure compaction never removes MVCC versions needed for merge bases.

| Step | What | File | Detail |
|------|------|------|--------|
| 1 | Implement `compute_gc_safe_version(db)` | `crates/engine/src/branch_ops.rs` | Walk DAG, find minimum fork_version and merge_version across active branches |
| 2 | Integrate with compaction scheduler | `crates/engine/src/database/` | Pass safe version to compaction, skip GC of entries ≥ safe version |
| 3 | Test: compaction respects pinned versions | Tests | Verify ancestor state is still readable after compaction |

---

## Phase 4: Data Portability

**Enables: cross-database changes, branch export/import, offline handoff.**

### Epic 4.1: Patch Export/Import

Portable changesets that can be applied to any compatible branch.

| Step | What | Detail |
|------|------|--------|
| 1 | Define patch format (serialized diff) | `{ base_version, entries: [{space, key, type_tag, action: Put(value)\|Delete}] }` |
| 2 | `branch format-patch <source> <target>` → patch file | Export three-way diff as portable changeset |
| 3 | `branch apply <patch_file>` → apply to current branch | Deserialize and apply in transaction |
| 4 | Public API: `branches().export_patch()` / `branches().apply_patch()` | |

### Epic 4.2: Branch Export/Archive

Export a branch snapshot at a specific version.

| Step | What | Detail |
|------|------|--------|
| 1 | `branch archive <name> --version <V> --output <path>` | Serialize all keys at version V |
| 2 | `branch import <path> --as <name>` | Create branch and populate from archive |
| 3 | Public API: `branches().archive()` / `branches().import_archive()` | |

---

## Critical Files

| File | Phases | Changes |
|------|--------|---------|
| `crates/engine/src/branch_dag.rs` | 1, 2 | Version fields on records, event emission |
| `crates/engine/src/branch_ops.rs` | 1, 2, 3 | Fork version capture, merge base, three-way diff/merge, revert, cherry-pick, bisect |
| `crates/storage/src/segmented/mod.rs` | 1, 2 | `list_by_type_at_version()`, `list_keys_modified_in_range()` |
| `crates/storage/src/merge_iter.rs` | 1 | Verify MvccIterator works with specific max_version (should already work) |
| `crates/executor/src/handlers/branch.rs` | 1, 2 | Pass fork_version/merge_version to DAG, emit events |
| `crates/executor/src/api/branches.rs` | 1, 2, 3 | New public API methods |
| `crates/cli/src/commands.rs` | 2, 3, 4 | New CLI subcommands |

## Verification

Each phase has its own verification:

**Phase 1:** `cargo test -p strata-engine -- branch` passes all new three-way merge tests + all 34 existing branch tests. Manual test: fork → delete key on source → merge → key is deleted on target.

**Phase 2:** All Phase 1 tests still pass. New tests for revert, cherry-pick, tags. Manual test: fork → modify → tag → revert → verify state matches tag.

**Phase 3:** All prior tests pass. Bisect tests with synthetic version history. Rerere tests with repeated conflicts.

**Phase 4:** Patch round-trip test: export patch from branch A→B, apply to branch C, verify C matches B.
