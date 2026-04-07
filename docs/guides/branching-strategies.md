# Branching Strategies

This guide is for developers designing a workflow with Strata branches. Strata gives you eight branching operations and two merge strategies, and the choice between them is what makes branches useful instead of just bookkeeping. This page is the decision guide.

For *what each merge does*, see [Merge Semantics](merge-semantics.md). For the complete API surface, see [Branching API Reference](../reference/branching-api.md).

## The branching toolkit

| Operation | What it does | When to use |
|---|---|---|
| **Fork** | O(1) copy-on-write copy of a branch | You want a new isolated workspace from a known starting point |
| **Diff** | Compare two branches | You want to see what's changed between them before merging |
| **Merge** | Three-way merge source into target | You want to combine two branches into one |
| **Cherry-pick** | Apply a filtered subset of source's changes to target | You want only some of source's changes, not all |
| **Materialize** | Collapse inherited COW layers into own segments | A fork chain has gotten deep and reads are getting slow |
| **Tag** | Bookmark a branch at a specific version | You want a stable name for a release point or experiment outcome |
| **Note** | Attach a message to a specific version | You want decision context, debug breadcrumbs, or audit trails |
| **Revert** | Undo a version range on a branch | A bad write needs to be reversed without losing post-write work |

These compose. A typical workflow uses several of them together — fork, mutate, diff, merge, tag the result, delete the working branch.

---

## Common workflows

### Per-session isolation

**Pattern:** each agent session, request handler, or test run gets its own branch. Work happens in isolation. Successful sessions either merge their results back or get archived; failed sessions get deleted.

```rust
let session_id = format!("session-{}", uuid::Uuid::new_v4());
db.branches().fork("default", &session_id)?;
db.set_branch(&session_id)?;

// All subsequent data ops on `db` target the session branch.
db.kv_put("key", value)?;

// At session end:
db.set_branch("default")?;  // switch back before merging
if successful {
    db.branches().merge(&session_id, "default", MergeStrategy::LastWriterWins)?;
}
db.branches().delete(&session_id)?;
```

**Why fork instead of just transactions?** Fork is O(1) and the session can run for arbitrarily long, do arbitrarily many writes, and read its own intermediate state. Transactions are bounded by the txn lifetime and don't survive across requests.

**Why this works:** fork is COW, so you pay nothing in storage until the session actually writes. A session that never writes costs zero bytes beyond the branch metadata.

### A/B experimentation

**Pattern:** fork the same base branch into two variants, run different strategies on each, diff to compare results, merge the winner.

```rust
db.branches().fork("baseline", "variant-a")?;
db.branches().fork("baseline", "variant-b")?;

// Run strategy A against variant-a, strategy B against variant-b ...

// Compare
let diff = db.branches().diff("variant-a", "variant-b")?;
println!("Variants differ on {} keys", diff.summary.total_modified);

// Promote the winner
db.branches().merge(winner, "baseline", MergeStrategy::LastWriterWins)?;

// Clean up
db.branches().delete("variant-a")?;
db.branches().delete("variant-b")?;
```

The two variants don't see each other and don't see baseline's later writes (if any) — they're proper snapshots from the moment of fork. The diff is per-space, per-primitive, so you can compare exactly what each strategy did.

### Hypothesis exploration

**Pattern:** fork to a working branch, mutate state to test a hypothesis, validate the result, and either merge it back (hypothesis confirmed) or delete the branch (hypothesis rejected).

```rust
db.branches().fork("main", "try-new-pricing")?;
update_prices(&db, "try-new-pricing")?;

if validate_pricing(&db, "try-new-pricing")? {
    db.branches().merge("try-new-pricing", "main", MergeStrategy::Strict)?;
    db.branches().delete("try-new-pricing")?;
} else {
    db.branches().delete("try-new-pricing")?;
}
```

This is the "speculative execution" pattern. Strata branches make it cheap because the failed-hypothesis path costs only the storage you actually wrote, and that gets reclaimed on delete.

### Long-lived feature branch

**Pattern:** a branch that lives for days or weeks while you build a feature. Periodically merge upstream changes into your branch to stay current. Tag release points along the way. Final merge back when the feature is done.

```rust
db.branches().fork("main", "feature-x")?;

// Day 1: work on feature-x
db.branches().create_tag("feature-x", "checkpoint-1", Some("end of day 1"))?;

// Day 2: pull upstream changes from main into feature-x
db.branches().merge("main", "feature-x", MergeStrategy::LastWriterWins)?;
// keep working...

// Day N: feature done, merge back to main
db.branches().merge("feature-x", "main", MergeStrategy::Strict)?;
db.branches().create_tag("main", "release-v2", Some("feature-x landed"))?;
db.branches().delete("feature-x")?;
```

The Strict merge at the end is important: if upstream main has changed in a way that conflicts with feature-x, you want to know about it explicitly rather than silently overwriting.

### Selective change adoption (cherry-pick)

**Pattern:** source branch has many changes; you want only some of them on target. Use `cherry_pick_filtered` with a `CherryPickFilter` to pick by space, by primitive, or by exact key.

```rust
use strata_engine::branch_ops::CherryPickFilter;
use strata_core::PrimitiveType;

// Take only JSON changes from source, and only in the "products" space.
let filter = CherryPickFilter {
    spaces: Some(vec!["products".to_string()]),
    primitives: Some(vec![PrimitiveType::Json]),
    keys: None,
};
db.branches().cherry_pick_filtered("source", "target", filter)?;
```

The filter has three orthogonal dimensions:

- `spaces` — only changes inside these named spaces
- `primitives` — only changes of these primitive types (KV, Json, Vector, Graph, Event)
- `keys` — only changes for these specific user keys (within whichever space matches)

Cherry-pick goes through the same per-handler dispatch as merge, so JSON path-level merge, vector HNSW rebuild, and graph referential integrity all still apply to the picked subset.

> **Graph atomicity:** the `keys` filter will refuse to split a graph cell. If you list `g/n/alice` but not `g/fwd/alice`, the cherry-pick fails fast with an error rather than leaving target's graph adjacency in an inconsistent state.

### Time-travel debugging with branches

**Pattern:** something looks wrong on `main` today. Reproduce yesterday's behavior in a branch by reading at an `as_of` timestamp.

```rust
// Snapshot main to an investigation branch.
db.branches().fork("main", "yesterday-debug")?;
db.set_branch("yesterday-debug")?;

// Read with as_of to see what the data looked like yesterday.
let snapshot_ts = chrono::Utc::now().timestamp_micros() - 86_400_000_000;
let value = db.kv_get_at("suspicious-key", Some(snapshot_ts as u64))?;

// Or diff today's main against an as-of snapshot.
use strata_engine::branch_ops::DiffOptions;
let diff = db.branches().diff_with_options("main", "yesterday-debug", DiffOptions {
    filter: None,
    as_of: Some(snapshot_ts),
})?;
```

Branches and time-travel compose: every branch has its own MVCC version line, and every key in every branch can be read as-of any past timestamp.

---

## Choosing a merge strategy

| You want… | Use |
|---|---|
| The system to refuse divergence and force you to resolve manually | `Strict` |
| Source side to win on conflicts (you trust source's changes) | `LastWriterWins` |
| To inspect conflicts without applying them | `diff_three_way` first, then decide |
| Only some changes, not all | `cherry_pick_filtered` instead of merge |

**Strict** is the right default for production merges where you want the system to be loud about anything it can't safely auto-resolve. It mirrors how git refuses to merge conflicting files instead of guessing.

**LastWriterWins** is the right default for experimental work where source is authoritative. It's also the only choice when you're merging the result of an experiment back into main and you've already validated source's correctness.

### What can't be auto-resolved

Some conflicts are *always* fatal regardless of strategy. These represent structural impossibilities, not value conflicts:

- **Event hash-chain divergence** (both sides appended to the same stream) — there is no canonical merged sequence.
- **Vector dimension/metric/dtype mismatch** — there is no way to combine vectors of different shapes into one HNSW.
- **Graph referential integrity violation** (dangling edge, orphaned reference) — the merged graph would be broken.

If you hit one of these, the fix is to *unbreak* the structural conflict before re-merging. See [Merge Semantics](merge-semantics.md) for the resolution recipes.

---

## Branch lifecycle hygiene

### When to materialize

Fork creates a child branch that reads through inherited COW layers from the parent. Reading a key on the child does up to N namespace lookups for an N-deep fork chain. For a chain like `root → a → b → c → d → e → leaf`, reads on `leaf` walk up to 6 layers.

If a branch is going to live a long time and serve a lot of reads, **materialize** it to collapse the inherited layers into own segments. Materialize is engine-level only (no CLI form, no `db.branches()` method):

```rust
use strata_engine::branch_ops;
branch_ops::materialize_branch(&engine_db, "leaf")?;
```

Materialize is O(data) — it actually copies. Don't run it speculatively. Heuristics:

- **Materialize after a long-lived fork chain stabilizes.** A branch that's been forked from a parent that you've now archived: materialize it so reads don't keep walking the parent.
- **Materialize before a heavy read workload.** If you're about to run analytics on a branch with 10+ inherited layers, the up-front materialize cost is amortized across the read load.
- **Don't materialize in the hot path.** Fork chains under ~5 deep are fine to leave alone; the inheritance lookup is fast.

### When to delete

Branches retain their data until you explicitly delete them. A finished session, a rejected hypothesis, a merged feature branch — all should be deleted to reclaim storage.

```rust
db.branches().delete("session-abc")?;
```

You **cannot** delete:

- The current branch (switch first)
- The `default` branch
- A branch that other branches depend on via fork chain (delete the children first, or materialize them first)

### When to tag

Use tags as immutable bookmarks for points you want to refer to later. Tag a release, tag a successful experiment outcome, tag the version a downstream system pinned.

```rust
db.branches().create_tag("main", "v1.0", Some("first GA release"))?;

// Later
let tag = db.branches().resolve_tag("main", "v1.0")?;
println!("v1.0 was at version {}", tag.unwrap().version);
```

Tags are stored separately from branch data — deleting a branch doesn't affect its tags, but the tags become orphaned references. List with `list_tags`, delete individually with `delete_tag`.

### When to add notes

Notes are version-annotated messages on a branch. Use them for:

- **Decision context.** "Switched pricing model at v423 because of FCC regulation X." A future you reading the audit log knows why.
- **Debug breadcrumbs.** "Reproduced bug #1234 here — see investigation branch repro-1234."
- **Audit trails.** Required for compliance? Notes give you per-version commentary without modifying the data.

```rust
db.branches().add_note(
    "main",
    db.current_version(),
    "Approved by SOC2 review",
    Some("alice"),
    None,
)?;
```

Notes are queryable: `get_notes("main", None)` returns every note on the branch, ordered by version.

---

## Performance and cost model

### Fork cost

Fork is **O(1)**. The destination branch is registered in `BranchIndex`, the storage layer creates a copy-on-write reference to the source's segments, and that's it. No data is copied. The destination starts out reading 100% from the source's segments via inheritance.

Storage cost of an empty fork: a single branch metadata entry. Fork a million empty branches and you've used ~megabytes of metadata, not gigabytes of data.

### Merge cost

Merge is **O(diff)** — proportional to the amount of diverged data, not the total branch size. A branch that's diverged by 100 keys merges in ~100 actions regardless of whether the branch has 10 keys or 10 million.

Merge runs in a single transaction. The transaction's cost scales with the number of `MergeAction`s it produces. Cell-level conflicts add a few bytes to the conflict list but don't materially increase cost.

**Per-primitive overhead** for the merge transaction:

| Primitive | Extra work in merge |
|---|---|
| KV | None — pure 14-case classification |
| Event | Hash-chain divergence check (fast — reads two metadata cells) |
| JSON | Per-doc path-level merge for divergent docs + secondary index entry emission |
| Vector | Per-collection HNSW rebuild in post_commit (proportional to affected collection size) |
| Graph | Decoded edge diffing + adjacency rebuild (proportional to affected edges) |

The HNSW rebuild and graph adjacency rebuild are the heaviest operations. They run inside the merge call but only for collections / graphs the merge actually touched. Untouched collections are not touched.

### Storage cost of long-lived branches

Branches share storage via COW. The storage cost of a branch is the **diverged bytes**, not the total bytes. A branch that has touched 1% of its parent's data costs ~1% of the parent's storage, not 100%.

Two factors increase storage cost beyond the diverged bytes:

1. **Versioning.** Strata is MVCC; every overwrite of a key adds a new version. Old versions stay until compaction removes them. A branch that overwrites the same key 1000 times stores 1000 versions until compaction.
2. **Tombstones.** Deleted keys leave a tombstone. Tombstones are tiny (just metadata) but they accumulate.

Both are bounded by compaction. If a branch is going to live forever, periodically running `compact()` keeps storage growth in check.

### Read cost on deep fork chains

A read on a forked branch may need to walk up to N inherited layers for an N-deep fork chain. Each layer is a fast hash lookup, but on a chain with 10+ levels the per-read overhead adds up. Materialize when chains get deep.

### When to use real transactions instead

Branches are not transactions. A branch lives for as long as you want; a transaction is bounded by its scope and rolls back atomically on error. For:

- **Multi-key writes that must succeed or fail together** — use a transaction (`db.transaction(...)`), not a branch.
- **Read-your-own-writes within a single request** — use a transaction.
- **Optimistic concurrency control** — use a transaction.
- **Multi-day workspaces with their own state** — use a branch.
- **Isolation between agent sessions / requests / users** — use a branch.

Don't use a branch where a transaction would do. Don't use a transaction where a branch would do.

---

## Anti-patterns

| Don't | Why |
|---|---|
| Fork on every read | Forks are cheap but not free; use sessions / namespaces instead for ephemeral state |
| Leave fork chains 50 deep forever | Read overhead grows linearly with depth; materialize periodically |
| Use `Strict` when you expect conflicts | The merge will fail every time; use `LastWriterWins` or `cherry_pick` |
| Use a branch as a transaction | Branches don't have rollback semantics; use `db.transaction(...)` |
| Use a transaction as a branch | Transactions don't survive across requests; use a branch |
| Fork two branches off `main` and never merge them back | Storage will grow unboundedly; either merge or delete |
| Tag every commit | Tags are for stable bookmarks, not change-history; use notes if you want per-version metadata |
| Cherry-pick a partial graph cell with `keys` filter | The atomicity guard will refuse it; widen the filter or use `merge` |

---

## See also

- [Merge Semantics](merge-semantics.md) — what each merge actually does, per primitive
- [Branch Management](branch-management.md) — CLI walkthrough
- [Branching API Reference](../reference/branching-api.md) — every operation, every field, every error
- [Concepts: Branches](../concepts/branches.md) — the mental model
- [Concepts: Time Travel](../concepts/time-travel.md) — `as_of` reads compose with branches
