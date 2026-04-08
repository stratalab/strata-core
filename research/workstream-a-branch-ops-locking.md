# Workstream A: Branch Operations Locking and Lifecycle

This document audits the synchronization model for branch lifecycle and branch
mutation operations.

The goal is to answer:

- which operations are globally quiesced
- which operations are only branch-serialized
- which operations rely only on OCC
- where branch lifecycle state can outlive the branch itself

## Scope

- `crates/engine/src/branch_ops/mod.rs`
- `crates/engine/src/primitives/branch/index.rs`
- `crates/engine/src/database/mod.rs`
- `crates/engine/src/coordinator.rs`
- `crates/concurrency/src/manager.rs`

## Operation matrix

| Operation | Planning snapshot | Explicit serialization | Apply-time protection | Notes |
|---|---|---|---|---|
| `fork_branch` | storage state under quiesce | global `quiesce_commits()` | storage fork only, then KV create | strongest branch op |
| `delete_branch` | branch metadata read + delete txn | `mark_branch_deleting` + target `branch_commit_lock()` | commit rejection on deleted branch | no global quiesce |
| `merge_branches` | `db.current_version()` | none during planning | target-key OCC reads in apply txn | source not serialized |
| `cherry_pick_from_diff` | `db.current_version()` | none during planning | target-key OCC reads in apply txn | similar to merge |
| `revert_version_range` | mixed historical + current live reads | none | blind write txn | weaker than merge |
| `cherry_pick_keys` | live source scan | none | blind write txn | weakest path |
| `diff_branches` / `diff_three_way` | `db.current_version()` | none | n/a | snapshot horizon not quiesced |
| `create_tag` default version | `db.current_version()` | none | n/a | tag may point at allocated, not yet visible, version |

## Strongest paths

### Fork is the only branch mutation that fully quiesces commits

`fork_branch_with_metadata()` takes the global commit quiesce write lock before
reading the source state and before calling `storage.fork_branch()`.

That gives it two strong properties:

- all in-flight commits finish applying before the fork version is captured
- no new commits can start while the storage fork is being created

The code explicitly avoids taking the branch commit lock inside the quiesce
section because commit order is `quiesce.read() -> branch_commit_lock`, and
reversing that would deadlock.

Conclusion:

- `fork_branch` is the strongest and most carefully synchronized branch op

### Delete is branch-serialized, not globally quiesced

`BranchIndex::delete_branch()` does this:

1. mark the executor branch id as deleting
2. mark the metadata branch id as deleting if it differs
3. acquire the executor branch's commit lock
4. run the logical delete transaction on the global branch
5. drain the scheduler
6. clear storage files
7. remove the commit lock entry
8. intentionally keep the deleting mark

This is enough to stop future commits on the branch and to drain in-flight
commits on that branch, but it does not globally quiesce other branches.

Conclusion:

- delete uses branch-local serialization plus a persistent lifecycle tombstone

## Confirmed seams

### 1. Branch deletion state outlives the branch name, so branch-name reuse appears broken

After successful `delete_branch()`, the code intentionally keeps the deleting
mark:

- it removes the branch commit lock entry
- it does not call `unmark_branch_deleting()`

At the same time, executor-visible branch ids are deterministic by branch name
via `resolve_branch_name(name)`.

`create_branch()`:

- does not check `is_branch_deleting()`
- does not clear the deleting mark

That means recreating a branch with the same name on the same live database
instance appears to reuse the same executor branch id while inheriting the
stale deleting mark until the process is reopened.
Subsequent commits on that recreated branch should be rejected as
`BranchDeleting`.

Why this matters:

- successful delete likely makes the branch name toxic for the lifetime of the
  current database instance
- branch lifecycle has no generation model, only a sticky branch-id tombstone

Severity:

- `critical`

### 2. Create and fork do not defend against destination branches that are mid-delete

`fork_branch()` checks whether the source branch is being deleted.
It does not check whether the destination branch id is currently marked as deleting.

`create_branch()` also does not check deleting state.

That means a branch can be recreated while a prior delete of the same logical
name is still unwinding:

- logical metadata delete has committed
- background scheduler is still draining
- storage clear is still pending
- deleting mark is still set

Why this matters:

- branch creation can race branch teardown
- the recreated branch can inherit stale lifecycle state from the deleted one

Severity:

- `high`

### 3. Merge and `cherry_pick_from_diff` do not serialize planning with source or target branches

`merge_branches()` and `cherry_pick_from_diff()`:

- do not take `quiesce_commits()`
- do not take target or source branch commit locks during planning
- compute their plan from a point-in-time snapshot version
- rely on OCC in the apply transaction to catch target-side drift

The #1917 fix is real for target-branch drift:

- they build `expected_targets`
- the apply transaction reads target keys via `txn.get()`
- OCC detects concurrent target modification

But source-side changes after the planning snapshot are not serialized.
The merge is effectively "merge a source snapshot", not "merge the latest source".

That may be acceptable, but it is an important semantic boundary.

Severity:

- `medium`

### 4. Revert is weaker than merge: it plans outside the transaction and applies blind writes

`revert_version_range_with_metadata()` computes:

- `before` state at `from_version - 1`
- `after` state at `to_version`
- current live state

It then decides what to revert outside the transaction and finally performs a
plain `transaction_with_version()` that only does `put()` and `delete()`.

It does not:

- re-read the target keys inside the apply transaction
- populate a read set for OCC on the keys being restored or deleted

So if the branch changes after the planning scan and before the revert commit,
the revert can overwrite newer work silently.

Why this matters:

- revert does not have the same safety model as merge
- branch history repair is weaker than branch merge even though both are
  branch-shaping operations

Severity:

- `high`

### 5. `cherry_pick_keys` is the weakest mutation path in branch ops

`cherry_pick_keys()`:

- scans the source with `storage.list_by_type()` using live current state
- does not capture a version-scoped source snapshot
- does not validate target keys inside the apply transaction
- performs blind writes to the target branch

Compared with `merge_branches()` and `cherry_pick_from_diff()`, this path has:

- weaker source-side consistency
- no target-side OCC protection

Why this matters:

- targeted cherry-pick is more race-prone than full diff-based cherry-pick
- callers could reasonably assume these two APIs share the same safety model, but they do not

Severity:

- `critical`

### 6. Diff and merge snapshot capture uses `current_version()`, not a safe visible watermark

`diff_branches()`, `merge_branches()`, and `cherry_pick_from_diff()` capture
their planning snapshot with `db.current_version()`.

In the transaction path, the engine had to add a `visible_version()` wait
because `current_version()` can run ahead of fully-applied storage state.

Branch ops do not do that.

Inference from the code:

- branch ops can name a version horizon that includes allocated but not yet
  fully applied commits
- the reads themselves are version-bounded, but the chosen watermark is not
  proven to be fully visible the way transaction snapshots are

This is weaker evidence than the issues above because I have not reproduced a
bug from it in this pass, but the ownership mismatch is real.

Severity:

- `high`

### 7. Tags default to `current_version()`, so a tag can point at a non-quiesced horizon

`create_tag(..., version=None, ...)` defaults to `db.current_version()`.

That is the allocated version counter, not a quiesced or explicitly visible
watermark.

Why this matters:

- tag creation inherits the same version-horizon ambiguity as branch diff/merge
- version references in branch control metadata are not consistently tied to
  a fully-applied snapshot boundary

Severity:

- `medium`

### 8. Delete drains the whole background scheduler, creating cross-branch coupling

Before `clear_branch_storage()`, `delete_branch()` calls `db.scheduler().drain()`.
The code comment explicitly acknowledges that this waits for all scheduled work,
not just work for the branch being deleted.

Why this matters:

- branch deletion is not isolated from unrelated background work
- in a busy deployment, one branch delete can block on other branches' tasks

Severity:

- `medium`

## Architectural summary

Branch operations are not synchronized under one model.

They split into three classes:

- strong lifecycle operations: `fork_branch`, `delete_branch`
- OCC-protected planning/apply operations: `merge_branches`, `cherry_pick_from_diff`
- weak blind-write operations: `revert_version_range`, `cherry_pick_keys`

That asymmetry is the main issue.
The API surface looks like one coherent branch-manipulation system, but the
actual synchronization guarantees vary a lot by operation.

## Highest-priority follow-ups

1. Fix branch-name reuse after delete.
   The deleting mark needs a lifecycle or generation model, not permanent residency.
2. Make `create_branch` and fork destination creation reject or coordinate with deleting state.
3. Upgrade `cherry_pick_keys` to the same target-side OCC model as merge.
4. Upgrade revert to validate target keys inside the apply transaction.
5. Decide whether branch ops should snapshot from `visible_version()` or a quiesced watermark instead of `current_version()`.

## Immediate next reads

- any direct callers that recreate branch names after delete
- bundle/import paths that create branches during restore
- branch status / DAG code to see whether lifecycle state is duplicated there too
