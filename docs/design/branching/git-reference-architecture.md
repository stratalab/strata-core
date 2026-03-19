# Git Reference Architecture for Strata Branching

**Date:** 2026-03-18
**Purpose:** Reference document distilling git's architecture into principles applicable to Strata's branching system

---

## 1. Git's Object Model

Git is a **content-addressable filesystem**. Every piece of data is stored as an immutable object identified by its SHA-1 hash.

### Four Object Types

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│  BLOB   │     │  TREE   │     │ COMMIT  │     │   TAG   │
│         │     │         │     │         │     │         │
│ Raw file│     │ Dir     │     │ Snapshot│     │ Named   │
│ content │     │ listing │     │ + meta  │     │ ref     │
│         │     │ (blobs  │     │ + parent│     │         │
│ No name │     │  + sub- │     │  links  │     │         │
│ No path │     │  trees) │     │         │     │         │
└─────────┘     └─────────┘     └─────────┘     └─────────┘
```

**Blob**: Raw content, no filename or metadata. Two files with identical content share the same blob.

**Tree**: A directory listing. Maps `(mode, name)` → blob or subtree SHA. Represents a complete snapshot of the filesystem at one level.

**Commit**: The core abstraction. Contains:
```
tree      <sha1>           ← root tree (full project snapshot)
parent    <sha1>           ← zero or more parent commits
author    <name> <email> <timestamp>
committer <name> <email> <timestamp>
<blank line>
<message>
```

**Key property**: A commit does NOT store diffs. It references a complete tree (full snapshot). Diffs are computed on the fly by comparing trees.

### The DAG

Commits form a **Directed Acyclic Graph** through parent pointers:

```
      A---B---C  (main)
     /         \
    D---E---F---G  (merge commit, parents: C and F)
         \
          H---I  (feature)
```

- Linear history: each commit has one parent
- Merge commits: have two (or more) parents
- Root commits: have zero parents
- The DAG is the permanent, immutable record of all project history

### Branches Are Just Pointers

```
refs/heads/main    → commit G
refs/heads/feature → commit I
HEAD               → refs/heads/main
```

A branch is a **40-byte file** containing a commit SHA. Creating a branch is O(1). There is no data copying. Branching is cheap because branches don't own data — they point to commits, which point to trees, which point to blobs. All objects are shared.

---

## 2. Git's Three-Way Merge Algorithm

### Why Three-Way, Not Two-Way

Two-way merge (comparing only the two branch tips) creates fatal ambiguity:

| Scenario | Two-Way Sees | Reality |
|----------|-------------|---------|
| Key in A, not in B | "Removed from B" | Could be: deleted on B, OR created on A after fork |
| Key in B, not in A | "Added to B" | Could be: added on B, OR deleted on A after fork |

**The common ancestor resolves all ambiguity.** By comparing each branch against the base, we know exactly what each side changed.

### The Decision Matrix

For each item (file/key), compare its state across three versions:

| Base | Ours | Theirs | Action |
|------|------|--------|--------|
| A | A | A | No change → keep A |
| A | **B** | A | Only we changed → take **B** |
| A | A | **B** | Only they changed → take **B** |
| A | **B** | **B** | Both changed identically → take **B** |
| A | **B** | **C** | Both changed differently → **CONFLICT** |
| ∅ | **B** | ∅ | We added → take **B** |
| ∅ | ∅ | **B** | They added → take **B** |
| ∅ | **B** | **C** | Both added differently → **CONFLICT** |
| ∅ | **B** | **B** | Both added identically → take **B** |
| A | ∅ | A | We deleted → **delete** |
| A | A | ∅ | They deleted → **delete** |
| A | ∅ | ∅ | Both deleted → **delete** |
| A | **B** | ∅ | We modified, they deleted → **CONFLICT** |
| A | ∅ | **B** | They modified, we deleted → **CONFLICT** |

Where:
- `∅` = does not exist (never created, or was deleted)
- `A`, `B`, `C` = distinct values

### The Algorithm (Step by Step)

```
MERGE(ours_commit, theirs_commit):

1. FIND MERGE BASE
   base = merge_base(ours_commit, theirs_commit)
   → Uses LCA algorithm on the commit DAG
   → Returns the "best" common ancestor

2. COMPUTE THREE-WAY DIFF
   For each item in union(base, ours, theirs):
     base_val  = state of item in base tree
     ours_val  = state of item in ours tree
     theirs_val = state of item in theirs tree

     Apply decision matrix (above)
     → Produces: auto-resolved items + conflicts

3. BUILD RESULT
   If no conflicts:
     Create new tree from resolved items
     Create merge commit with two parents (ours, theirs)
   If conflicts:
     Mark conflicting items for manual resolution
     Do NOT create merge commit
```

### Git's Levels of Merging

Git merges operate at **two levels**:

**Tree-level (structural)**: Determines which files changed, were added, deleted, or renamed by comparing tree objects. This is analogous to our key-level comparison.

**Content-level (textual)**: For files modified on both sides, runs the diff3 algorithm on file contents (line-by-line). This does NOT apply to Strata — our values are opaque (not line-oriented text).

For Strata, we only need tree-level merging. Our "files" are key-value pairs. If both sides modified the same key to different values, that's a conflict (no line-level merge possible).

---

## 3. Merge Base: The LCA Algorithm

### Definition

The **merge base** of two commits A and B is their **best common ancestor** in the DAG:
- A common ancestor C is **better** than another common ancestor D if D is an ancestor of C
- A **merge base** is a common ancestor that has no better common ancestor
- There may be multiple merge bases (criss-cross merges)

### Simple Case

```
      o---o---A
     /
o---C---o---o---B
```

Merge base of A and B = C. Found by walking parent links from both commits until paths converge.

### Algorithm

```
MERGE_BASE(A, B):
  visited_a = BFS from A following parent links
  visited_b = BFS from B following parent links

  common = visited_a ∩ visited_b

  // Filter to "best" — remove any ancestor that is itself
  // an ancestor of another common ancestor
  bases = {c ∈ common : ¬∃ c' ∈ common where c is ancestor of c'}

  return bases  // may be multiple (criss-cross)
```

### Multiple Merge Bases (Criss-Cross)

```
  A---M1---o---X
 / \ /
o   ×        ← criss-cross: A and B each merged the other
 \ / \
  B---M2---o---Y
```

Both A and B are merge bases of X and Y. Neither is better (neither is an ancestor of the other).

**Git's solution (recursive/ort strategy)**: Merge the multiple bases into a **virtual merge base**, then use that as the ancestor for the final three-way merge. If the virtual merge has conflicts, they are left as conflict markers in the virtual base.

### Repeated Merges

```
o---A---o---o---M1---o---o---M2---X  (main)
     \       /         /
      B---o---C---o---D---o---Y      (feature)
```

- First merge (M1): base = A (original fork point)
- Second merge (M2): base = M1 (the previous merge commit)

Git handles this naturally because M1 is a commit in the DAG with two parents. The LCA of X and Y is M1, which captures the state after the first merge. Only changes after M1 are considered.

---

## 4. How Git Handles Deletions

Deletions are naturally handled by the three-way merge because the base provides the reference:

```
At base:  key "X" exists with value V
At ours:  key "X" exists with value V  (unchanged)
At theirs: key "X" does not exist       (deleted)

Decision: They deleted → delete key "X" from result
```

```
At base:  key "X" does not exist
At ours:  key "X" exists with value V  (we added it)
At theirs: key "X" does not exist       (unchanged)

Decision: We added → keep key "X" with value V
```

Without the base, both scenarios look identical: "key in ours, not in theirs." The base resolves the ambiguity.

---

## 5. Dolt: Git's Model Applied to a Database

Dolt (a SQL database with git-style versioning) provides the closest prior art to what Strata needs. Key learnings:

### Storage: Prolly Trees

Dolt uses **Prolly Trees** (probabilistic B-trees) instead of traditional B-trees. Key property: **history independence** — the same dataset produces the same tree regardless of insertion order. This enables content-addressing (like git's SHA-based objects) and efficient structural diffing.

**Strata parallel**: Our LSM tree + MVCC gives us version history per key. We don't need content-addressing because we have global monotonic versions.

### Merge Algorithm (6 Steps)

1. **Find merge base** — BFS on commit graph (same as git)
2. **Schema merge** — merge table structure changes (N/A for Strata — we have fixed primitives)
3. **Resolve schema conflicts** — interactive (N/A for Strata)
4. **Data merge** — three-way merge on rows using primary key as the identifier:
   - Same row, same column changed differently → CONFLICT
   - Same row, different columns changed → auto-merge
   - Row added on one side only → apply
   - Row deleted on one side only → apply deletion
5. **Resolve data conflicts** — interactive
6. **Resolve constraint violations** — check referential integrity post-merge

### Key Insight from Dolt

> "Primary keys serve as cross-version row identifiers."

This maps directly to Strata: our `(space, type_tag, user_key)` tuple serves as the row identifier across branches. Values are opaque (no column-level merge), so any two-sided modification to the same key is a conflict.

### Limitation

Dolt does NOT handle criss-cross merges (multiple merge bases). It fails and requires manual intervention. For Strata's use case (embedded database, not distributed VCS), this is acceptable.

---

## 6. PlanetScale: Schema-Level Three-Way Merge

PlanetScale applies three-way merge to database schema changes:

```
diff1 = diff(main, branch1)
diff2 = diff(main, branch2)

Test: Can diff1(diff2(main)) execute? And vice versa?
If both orderings produce identical schemas → merge succeeds
Otherwise → conflict
```

**Strata parallel**: Our "schema" is fixed (KV, JSON, State, Event, Vector). We don't need schema merge. But the principle of testing commutativity of diffs is relevant for advanced conflict detection.

---

## 7. Design Principles Extracted

### From Git

1. **Branches are cheap** — just pointers, not data copies. (Strata currently uses O(n) deep-copy fork for isolation. O(1) COW branching via inherited segment layers is designed — see [`cow-branching.md`](cow-branching.md).)

2. **The DAG is the history** — all operations (fork, merge) create nodes in the DAG. The DAG is the single source of truth for ancestry and merge base computation.

3. **Three-way merge requires a base** — you cannot merge correctly without knowing the common ancestor. Two-way merge is fundamentally broken for deletions and additions.

4. **The merge base is computed, not stored** — git doesn't store "this is the fork point." It computes the LCA from the DAG. But it can do this because commits are immutable objects in the DAG with parent pointers.

5. **Merge creates a new commit** — the merge result is a new commit with two parents. This is how the DAG records merges and enables future merge base computation.

6. **Conflicts are explicit** — when the algorithm can't decide, it stops and asks the user. It never silently drops data.

### From Dolt

7. **Primary key = identity** — in a key-value database, the key tuple is the identity across versions. Values are compared for equality to detect modifications.

8. **Deletion is a first-class operation** — three-way merge naturally handles deletions through the base comparison. No special tombstone logic needed at the merge level.

9. **Version GC must respect merge bases** — if you garbage-collect old versions, you may lose the ability to compute merge bases. Pin versions that are referenced by active branches.

### For Strata Specifically

10. **We have the MVCC, we just need the DAG** — Strata already has global versions, point-in-time reads, and tombstones. The missing piece is recording fork/merge points in the DAG with version numbers, and implementing the three-way merge algorithm.

11. **Fork point = merge base** — since fork copies data (creating independent branches), the fork point is the natural merge base. We record the version at fork time and use it to reconstruct the ancestor state.

12. **Merge point = new base** — after merging, the merge's version becomes the base for future merges between the same branches. This handles repeated merges.

---

## 8. Mapping Git Concepts to Strata

| Git Concept | Strata Equivalent | Status |
|-------------|-------------------|--------|
| Object store (blobs) | MVCC value store | ✅ Exists |
| Tree (directory snapshot) | Branch namespace at version V | ✅ Exists (via MvccIterator) |
| Commit (snapshot + parents) | Transaction commit_id | ✅ Exists (global counter) |
| Branch ref (pointer to commit) | BranchMetadata + namespace UUID | ✅ Exists |
| HEAD (current branch) | CLI session current_branch | ✅ Exists |
| Commit DAG | Branch DAG on `_system_` branch | ⚠️ Partial (no version tracking) |
| merge-base (LCA) | Walk DAG fork/merge events | ❌ Not implemented |
| Three-way diff | diff(ancestor, source) + diff(ancestor, target) | ❌ Not implemented |
| Three-way merge | Apply three-way diff with conflict detection | ❌ Not implemented |
| Delete propagation | Natural from three-way merge | ❌ Broken |
| Merge commit (two parents) | Merge event in DAG with merge_version | ⚠️ Partial (no version) |

### What We Need to Build

1. **Version-aware DAG events** — record `fork_version` and `merge_version` on DAG fork/merge events
2. **Merge base computation** — walk the DAG to find the LCA, return the version to use as the ancestor
3. **Version-scoped listing** — `list_branch_at_version(branch_id, max_version)` that doesn't filter tombstones
4. **Three-way diff** — diff ancestor state against both branches
5. **Three-way merge** — apply the decision matrix with proper conflict detection and delete propagation

---

## Sources

- [Git Internals - Git Objects (git-scm.com)](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects)
- [Three-Way Merge (revctrl.org)](https://tonyg.github.io/revctrl.org/ThreeWayMerge.html)
- [The Magic of 3-Way Merge](https://blog.git-init.com/the-magic-of-3-way-merge/)
- [git-merge-base Documentation](https://git-scm.com/docs/git-merge-base)
- [Git Merge Strategies Documentation](https://git-scm.com/docs/merge-strategies)
- [Criss-Cross Merge (revctrl.org)](https://tonyg.github.io/revctrl.org/CrissCrossMerge.html)
- [diff3 Merge Algorithm](https://blog.jcoglan.com/2017/05/08/merging-with-diff3/)
- [Three-Way Merge in Dolt (DoltHub)](https://www.dolthub.com/blog/2024-06-19-threeway-merge/)
- [PlanetScale Three-Way Merge for Schema](https://planetscale.com/blog/database-branching-three-way-merge-schema-changes)
- [Git Internals: How Merge Really Works](https://dev.to/shrsv/git-internals-how-git-merge-really-works-2dn5)
- [Git merge-ort.c source](https://github.com/git/git/blob/master/merge-ort.c)
- [Git turns 20: Q&A with Linus Torvalds](https://github.blog/open-source/git/git-turns-20-a-qa-with-linus-torvalds/)
- [Recursive Merge Strategy (Plastic SCM)](https://blog.plasticscm.com/2012/01/more-on-recursive-merge-strategy.html)
- [A Deep Dive into Git Internals](https://dev.to/__whyd_rf/a-deep-dive-into-git-internals-blobs-trees-and-commits-1doc)
