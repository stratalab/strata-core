# Engine Invariant Check

You are reviewing a code change to StrataDB, a Rust-based embedded database with an LSM tree storage engine, COW branching, MVCC versioning, and ACID transactions.

Your job is to determine whether this change violates any engine invariant — and if it does, fix it.

## Instructions

1. Read the engine invariant catalog at `docs/audit/ENGINE_INVARIANTS.md`. This defines 28 correctness properties organized into 7 categories (LSM, CMP, COW, MVCC, ACID, ARCH). Each invariant has an **Audit** instruction.

2. Read the git diff provided below (or attached).

3. **Triage**: For each invariant, assess whether the changed code could affect it. An invariant is "affected" if the diff touches code that participates in the property — even indirectly. Err on the side of inclusion. List the affected invariants by ID.

4. **Verify**: For each affected invariant, execute its Audit instruction against the **current codebase** (post-change). Read the actual source files — do not rely on the diff alone, because the diff shows what changed but not the surrounding context that the invariant depends on.

5. **Report**: For each affected invariant, state one of:
   - **HOLDS** — the invariant is satisfied. State why in one sentence.
   - **WEAKENED** — the invariant is not violated but the safety margin is reduced (see step 6).
   - **VIOLATED** — the invariant is broken (see step 6).
   - **INCONCLUSIVE** — you cannot determine compliance without more context. State what you need.

6. **Fix violations**: For every invariant that is VIOLATED or WEAKENED:

   a. **Diagnose root cause.** Identify the exact code that violates the invariant. Show a concrete
      scenario — a sequence of operations with version numbers, branch names, and crash points — that
      triggers the bug. Do not speculate; demonstrate.

   b. **Design the fix.** The fix must:
      - Restore the invariant without weakening any other invariant. Check the dependency chain:
        a compaction fix (CMP-*) must not break MVCC visibility (MVCC-001) or COW safety (COW-001).
      - Be minimal — change only what is necessary to restore the invariant. Do not refactor
        surrounding code, add features, or "improve" unrelated logic.
      - Preserve existing tests. If existing tests relied on the buggy behavior, those tests were
        wrong — fix them too, but call it out explicitly.

   c. **Implement the fix.** Write the corrected code. Use the Edit tool to apply changes to the
      actual source files.

   d. **Write a regression test.** The test must:
      - Reproduce the exact scenario from step (a) — if the fix were reverted, the test would fail.
      - Be named to reference the invariant: `test_cmp001_tombstone_preserved_in_non_bottommost`.
      - Live in the appropriate test module (storage tests for LSM/CMP, integration tests for ARCH).

   e. **Re-verify.** After applying the fix, re-run the Audit instruction for the fixed invariant
      AND for every invariant that shares code with the fix. Confirm no new violations were introduced.

## Output Format

```
## Triage

Affected invariants: LSM-003, CMP-001, CMP-002, MVCC-001, ARCH-005
Not affected: [all others — no code in this diff participates in those properties]

## Verification

### LSM-003: Read path level ordering
**HOLDS** — The read path in get_versioned_from_branch still checks sources in the
correct order (active → frozen → L0 → L1-L6 → inherited). The change does not
alter source ordering.

### CMP-001: Tombstone preservation in non-bottommost compaction
**VIOLATED**

**Bug**: The new compaction path in compact_level() drops tombstones unconditionally:
    if entry.is_tombstone && commit_id < prune_floor {
        continue; // BUG: no bottommost check
    }

**Scenario**: Key K has value@3 in L2 and tombstone@5 in L1. compact_level(1, ...)
drops tombstone@5 (below prune_floor=6). L2 still has value@3. Next read of K
returns value@3 — resurrecting deleted data.

**Fix**: Gate tombstone drop on `is_bottommost`:
    if entry.is_tombstone && commit_id < prune_floor && is_bottommost {
        continue;
    }

**Regression test**: test_cmp001_tombstone_preserved_in_non_bottommost
  - Write value@3 to L2 (via compaction)
  - Write tombstone@5 to L1
  - Compact L1 with prune_floor=6, is_bottommost=false
  - Assert tombstone@5 exists in output
  - Read key K — assert returns None (not value@3)

**Cross-check**: Fix does not affect CMP-002 (pruning logic) or MVCC-001
(version visibility). The tombstone is preserved, so shadowing is intact.

### CMP-002: Version pruning respects prune_floor
**HOLDS** — Pruning logic unchanged; only the segment selection code was modified.

[... etc ...]

## Summary

| Result | Count | Invariants |
|--------|-------|------------|
| HOLDS | 3 | LSM-003, CMP-002, ARCH-005 |
| VIOLATED | 1 | CMP-001 |
| WEAKENED | 1 | MVCC-001 |
| INCONCLUSIVE | 0 | — |

## Fixes Applied

| Invariant | File(s) changed | Test added |
|-----------|-----------------|------------|
| CMP-001 | crates/storage/src/segmented/compaction.rs | test_cmp001_tombstone_preserved_in_non_bottommost |
```

## Important

- **Read the actual source files.** The diff shows the change; the source shows the context. An invariant can be violated by code the diff didn't touch if the change alters assumptions that code relies on.
- **Second-order effects matter.** A change to key encoding (LSM-001) affects bloom filter probes (LSM-005), MVCC reads (MVCC-001), COW rewriting (COW-003), and compaction (CMP-002). Follow the dependency chain.
- **Do not weaken invariants.** If the code violates an invariant, the code is wrong — not the invariant. Invariants change only when the architecture changes.
- **Fixes must not introduce new violations.** After every fix, re-verify the fixed invariant AND its neighbors. A fix to CMP-001 that breaks MVCC-001 is not a fix.
- **Be specific.** "This might cause issues" is not useful. Show the exact code path, the exact version numbers, the exact failure mode.
- **Minimal fixes only.** Do not refactor, do not add features, do not improve code quality. Restore the invariant and nothing else.

## The Diff

```
<PASTE GIT DIFF HERE>
```

To generate the diff for the most recent commit:
```bash
git diff HEAD~1
```

For a specific commit:
```bash
git show <commit-hash>
```

For a PR branch against main:
```bash
git diff main...HEAD
```
