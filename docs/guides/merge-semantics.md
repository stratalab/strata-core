# Merge Semantics

This guide explains exactly what `branch merge` does for each primitive in Strata. If `branch fork` is the easy half of branching, `branch merge` is the part where you need to know what you're going to get back. This page is the answer.

For when to merge, see [Branching Strategies](branching-strategies.md). For the complete API surface (every option, return field, error variant), see the [Branching API Reference](../reference/branching-api.md).

## The Merge Model in One Page

Strata merges are **three-way merges**. When you merge `source` into `target`, the engine:

1. Computes the **merge base** — the common ancestor where the two branches diverged. This is the fork point if the branches share a fork relationship, or the version of the previous merge if they've been merged before.
2. Reads three snapshots: ancestor (at the merge base), source (current), target (current).
3. Routes each primitive's data through a **per-primitive handler** that knows that primitive's invariants.
4. Applies the merged state to the target branch in a single transaction.

The merge transaction is atomic: every primitive's merged state — including derived state like JSON secondary indexes — is durable in the WAL by the time `branches().merge(...)` returns. A crash mid-merge cannot leave you in a half-merged state.

### Strict vs. LastWriterWins

Every merge takes a `MergeStrategy`:

| Strategy | What it does | When to use |
|---|---|---|
| `Strict` | Fails the merge if any conflict is detected. Target is unchanged. | Production merges where you want the system to refuse divergence and force you to resolve it manually. |
| `LastWriterWins` | Auto-resolves conflicts by taking the source side's value. The merge always succeeds. | Experimental work, hypothesis branches, scenarios where source is authoritative. |

**Some conflicts are always fatal regardless of strategy.** Referential-integrity violations (graph dangling edges, vector dimension mismatches, event hash-chain divergence) cannot be auto-resolved by picking a winner — they represent structural impossibilities. The merge will fail under both strategies and return a structured error explaining what's wrong. See the per-primitive sections below.

### Conflicts as data

The result of `branch merge` is a `MergeInfo` value with a `conflicts: Vec<ConflictEntry>` field. Under `LastWriterWins`, conflicts are reported but the merge still succeeds — you can inspect them after the fact to know which keys were resolved by picking source over target. Under `Strict`, conflicts are returned via the error path and the target is untouched.

From the Rust SDK, the `Branches` namespace exposes the underlying API:

```rust
use strata_engine::MergeStrategy;

let info = db.branches().merge("source", "target", MergeStrategy::LastWriterWins)?;
println!("Applied {} keys, deleted {}", info.keys_applied, info.keys_deleted);
for c in &info.conflicts {
    println!("conflict at {} ({}): source={:?} target={:?}",
        c.key, c.space, c.source_value, c.target_value);
}
```

---

## KV merge

KV is the simplest case. Each KV cell is `(branch, space, user_key) → value`, and the merge handler classifies each key with a 14-case decision matrix over `(ancestor, source, target)`. There's no semantic structure inside the value — KV values are opaque to the merge.

### The 14-case matrix

| Ancestor | Source | Target | Classification | Action |
|---|---|---|---|---|
| absent | absent | absent | impossible | — |
| absent | absent | present | TargetAdded | keep target |
| absent | present | absent | SourceAdded | apply source |
| absent | s = t | s = t | BothAddedSame | keep target |
| absent | s ≠ t | s ≠ t | **BothAddedDifferent** | conflict |
| present | a | a | Unchanged | keep target |
| present | s ≠ a | a | SourceChanged | apply source |
| present | a | t ≠ a | TargetChanged | keep target |
| present | s = t ≠ a | s = t ≠ a | BothChangedSame | keep target |
| present | s ≠ a | t ≠ a, s ≠ t | **Conflict** | conflict |
| present | absent | a | SourceDeleted | delete on target |
| present | absent | t ≠ a | **DeleteModifyConflict** | conflict |
| present | a | absent | TargetDeleted | keep target (deleted) |
| present | s ≠ a | absent | **ModifyDeleteConflict** | conflict |

The four bolded rows are the conflict cases. Under `Strict`, any conflict aborts the merge. Under `LastWriterWins`, source's value (or its absence, in the modify-delete case) wins and the conflict is reported in `MergeInfo.conflicts`.

### Example

```text
strata:default/default> use target
strata:target/default> kv put shared 0
(version) 1
strata:target/default> branch fork source
strata:target/default> use source
strata:source/default> kv put shared 11
(version) 2
strata:source/default> use target
strata:target/default> kv put shared 22
(version) 3

# Strict refuses
strata:target/default> branch merge source --strategy strict
(error) merge conflict: 1 keys differ between 'source' and 'target'

# LWW takes source's value
strata:target/default> branch merge source --strategy lww
(merged) keys_applied=1, conflicts=1
strata:target/default> kv get shared
11
```

---

## Event merge

Events are append-only by design — each event is hash-chained to the previous one in its stream. This is what makes Strata's event log auditable. It's also what makes event merge fundamentally different from KV merge.

### The append-only invariant

If both source and target have appended events to the **same stream** since the merge base, there is no way to combine them without breaking the hash chain or rewriting history. Strata refuses these merges.

The handler accepts:

- **Single-sided appends.** Source appended, target didn't (or vice versa): apply the new events to target, hash chain stays valid.
- **Cross-stream divergence.** Source appended to stream A, target appended to stream B (different stream IDs in the same space, or different spaces): both sets of events apply, no chain conflict.
- **Both sides idle.** Neither side touched any event: no-op.

The handler refuses:

- **Same-stream divergence.** Both sides appended to the same `(space, stream_id)` since the merge base. The merge fails with a structured error regardless of `MergeStrategy`.

### Example

```text
strata:default/default> use target
strata:target/default> event append audit '{"v":1}'
(seq) 1
strata:target/default> branch fork source

# Both branches append to the same stream after fork → divergent.
strata:target/default> use source
strata:source/default> event append audit '{"v":2}'
(seq) 2
strata:source/default> use target
strata:target/default> event append audit '{"v":3}'
(seq) 2

# Refused under both Strict and LWW.
strata:target/default> branch merge source --strategy lww
(error) event merge unsupported: divergent appends to stream "audit" since merge base
```

### Resolving event divergence

If you hit this, the system is telling you that the two streams genuinely diverged and a deterministic merge is not possible. Your options:

1. **Pick a winner.** Discard one branch's events by reverting that branch's appends, then re-merge. The merge will accept it as a single-sided change.
2. **Replay manually.** Read the divergent events on one side, and re-append them to the other side as new events (with a new sequence). This rewrites history but at least it's an explicit, intentional rewrite.
3. **Avoid the divergence.** Use a different stream ID per branch to prevent the conflict from arising. This is the cleanest pattern for per-session event logs.

---

## JSON merge

JSON merge is the most semantically rich case. Strata does a **per-document recursive three-way merge** that walks JSON objects path by path. Disjoint edits to the same document combine cleanly; same-path edits to different values report a path-level conflict.

### The algorithm

For each `(ancestor, source, target)` triple of JSON values:

1. If all three (or both extant ones) are JSON **objects**, walk the union of their keys and recurse per-key. Each subtree is merged independently.
2. Otherwise (scalars, arrays, or type-mixed sides), apply standard three-way classification at the value level: source-only change keeps source, target-only change keeps target, divergent change is a path-level conflict that LWW resolves source-wins.

**Arrays are treated as opaque values.** Element-level array merging requires LCS or operational-transform algorithms that are out of scope for the current implementation. If both sides modify the same array, the merge reports a conflict at the array path; LWW takes source's array verbatim.

### Example: disjoint paths auto-merge

```text
strata:default/default> use target
strata:target/default> json set doc-1 . '{"a": 1, "b": 2}'
(version) 1
strata:target/default> branch fork source

# Both branches modify the same doc but at different top-level keys.
strata:target/default> use source
strata:source/default> json set doc-1 a 10
(version) 2
strata:source/default> use target
strata:target/default> json set doc-1 b 20
(version) 2

strata:target/default> branch merge source --strategy strict
(merged) keys_applied=1, conflicts=0
strata:target/default> json get doc-1 .
{"a": 10, "b": 20}
```

### Example: same-path conflict

```text
strata:default/default> use target
strata:target/default> json set doc-1 . '{"name": "Alice"}'
(version) 1
strata:target/default> branch fork source

# Both branches edit the same path to different values.
strata:target/default> use source
strata:source/default> json set doc-1 name '"Bob"'
(version) 2
strata:source/default> use target
strata:target/default> json set doc-1 name '"Carol"'
(version) 2

# Strict refuses.
strata:target/default> branch merge source --strategy strict
(error) merge conflict: 1 keys differ between 'source' and 'target'

# LWW source-wins on the conflicting path.
strata:target/default> branch merge source --strategy lww
(merged) keys_applied=1, conflicts=1
strata:target/default> json get doc-1 .
{"name": "Bob"}
```

### Subtree delete vs. child edit

If one side deletes a parent object and the other side edits a key inside that object, the merge reports a conflict at the **parent path** (not the leaf). LWW source-wins drops the entire subtree.

```text
strata:default/default> use target
strata:target/default> json set doc-1 . '{"user": {"name": "Alice", "email": "a@example.com"}, "active": true}'
(version) 1
strata:target/default> branch fork source

# Source deletes the whole "user" object.
strata:target/default> use source
strata:source/default> json del doc-1 user
(version) 2

# Target edits user.email.
strata:source/default> use target
strata:target/default> json set doc-1 user.email '"alice@new.com"'
(version) 2

strata:target/default> branch merge source --strategy lww
(merged) keys_applied=1, conflicts=1
strata:target/default> json get doc-1 .
# LWW source-wins → "user" is dropped entirely.
{"active": true}
```

### Secondary indexes are atomic

When you create a JSON secondary index (`json create-index price_idx $.price numeric`) and a merge changes an indexed field, the merge writes both the document update **and** the index entry update inside the same transaction. There is no window between them where a crash could leave the index pointing at the pre-merge value.

This means after `branches().merge(...)` returns successfully, an `index::lookup_eq("price_idx", new_value)` is guaranteed to return the doc and `index::lookup_eq("price_idx", old_value)` is guaranteed to return nothing — even after a process crash and restart.

---

## Vector merge

Vectors live in HNSW indexes that are partly in-memory and partly persisted in KV. The merge handler treats each `(space, collection)` pair as the unit of merge, and each merge can rebuild zero or more collections without touching the others.

### What's checked, what's merged

**Precheck** (always fatal, regardless of strategy):

- **Dimension mismatch.** If source and target have a collection with the same name but different vector dimensions, the merge is refused. There is no way to combine vectors of different dimensions into one HNSW index.
- **Distance metric mismatch.** Same name, different metric (Cosine vs. L2 vs. Dot) — refused.
- **Storage dtype mismatch.** Same name, different `storage_dtype` (F32 vs. Int8) — refused.

**Plan**: walk the KV cells under `TypeTag::Vector`, classify each cell with the standard 14-case matrix. Vectors are KV-shaped at the cell level so the generic classifier produces correct puts and deletes. The handler tracks which `(space, collection)` pairs were actually mutated.

**Post-commit**: for each affected collection, rebuild its HNSW backend incrementally from the new KV state. Untouched collections are not touched. The rebuild is per-collection, not per-branch — adding 100 vectors to one collection doesn't force a rebuild of every other collection in the same branch.

### What works

| Scenario | Result |
|---|---|
| Source adds vectors to collection A; target adds to collection B | Both collections present after merge, both searchable |
| Source and target add **different vector keys** to the same collection | Both keys present, HNSW reflects merged set |
| Source updates vector X's embedding; target leaves it alone | Source's update wins (unmodified-on-target fast path) |
| Source and target both update the same vector key to different embeddings | Conflict; LWW takes source's embedding |
| Source deletes a vector; target leaves it alone | Vector deleted on target |
| Source creates a new collection; target leaves it alone | Collection created on target |

### What fails

```text
# Same collection name, different dimensions → refused.
strata:default/default> use source
strata:source/default> vector create embeddings --dim 384 --metric cosine
strata:source/default> use target
strata:target/default> vector create embeddings --dim 768 --metric cosine
strata:target/default> branch merge source --strategy lww
(error) merge unsupported: vector collection 'embeddings' in space 'default'
        has incompatible dimensions (source=384, target=768)
```

The fix is to recreate the collection on one side with a consistent dimension before merging. There is no way to "auto-convert" vectors between dimensions.

### HNSW search after merge

After a merge that touched a collection, an HNSW k-NN search on the target branch returns the merged set immediately — both source's and target's vectors are findable, no manual reindex required. The post-commit rebuild has already run by the time `branches().merge(...)` returns.

---

## Graph merge

Graph merge is **semantic**: the handler decodes packed adjacency lists, computes a per-edge diff, validates referential integrity across the merge, and re-encodes the result. This is what lets two branches add disjoint edges to the same graph without conflicting and without leaving the adjacency in an inconsistent state.

### What's enforced

**Referential integrity** (always fatal, regardless of strategy):

- **Dangling edge.** Source adds an edge `A → B` but target deletes node `B` — the merged graph would have an edge to a nonexistent node. Refused.
- **Orphaned reference.** Source deletes node `A`, target adds edge `A → B` — same issue. Refused.

**Cell-level property conflicts** honor `MergeStrategy`:

- Source and target both modify the same node's properties to different values → conflict; LWW takes source.
- Source and target both modify the same edge's data to different values → conflict; LWW takes source.

**Additive merging** (no conflict):

- Source adds nodes and edges that target doesn't have, target adds different nodes and edges. Both sets land on target. Bidirectional adjacency (forward and reverse edges) is updated atomically.
- Source and target both add the **same** node or edge with the same properties — no-op, no conflict.

**Catalog merge** (additive, no conflict):

- Source calls `graph_create("g1")`, target calls `graph_create("g2")`. After the merge, target has both graphs in its catalog. The catalog is merged as a per-name set union.

### Example

```text
strata:default/default> use target
strata:target/default> graph create g
strata:target/default> graph add-node --graph g --node-id alice
strata:target/default> graph add-node --graph g --node-id bob
strata:target/default> branch fork source

# Source adds carol + edge alice→carol
strata:target/default> use source
strata:source/default> graph add-node --graph g --node-id carol
strata:source/default> graph add-edge --graph g --src alice --dst carol --edge-type follows

# Target adds dave + edge bob→dave
strata:source/default> use target
strata:target/default> graph add-node --graph g --node-id dave
strata:target/default> graph add-edge --graph g --src bob --dst dave --edge-type follows

# Disjoint additions merge cleanly under Strict.
strata:target/default> branch merge source --strategy strict
(merged) keys_applied=4, conflicts=0

# Both sides' nodes and edges are present, adjacency is consistent.
strata:target/default> graph get-node --graph g --node-id carol
{"id": "carol", ...}
strata:target/default> graph get-node --graph g --node-id dave
{"id": "dave", ...}
```

### Dangling edge example

```text
strata:default/default> use target
strata:target/default> graph create g
strata:target/default> graph add-node --graph g --node-id alice
strata:target/default> graph add-node --graph g --node-id bob
strata:target/default> branch fork source

# Source adds edge alice→bob.
strata:target/default> use source
strata:source/default> graph add-edge --graph g --src alice --dst bob --edge-type follows

# Target deletes bob.
strata:source/default> use target
strata:target/default> graph remove-node --graph g --node-id bob

# Refused — the merge would leave alice→bob dangling.
strata:target/default> branch merge source --strategy lww
(error) dangling edge reference: edge alice → bob but node 'bob' missing on target
```

The fix: revert one of the two changes (either restore `bob` on target, or remove the edge on source) and re-merge.

---

## Crash recovery

Every primitive's merged state is in the write-ahead log by the time `branches().merge(...)` returns. After a clean restart, the merged state is recovered automatically:

- **KV, Event, JSON document data, JSON secondary indexes, Graph adjacency** — replayed from WAL during `Database::open`.
- **Vector HNSW backends** — rebuilt from KV state during recovery (HNSW lives in memory, not on disk; the merge txn writes vectors to KV, recovery re-derives the index).
- **JSON BM25 inverted index** — rebuilt from KV state during recovery (also in memory).

This means the rollback test in the engine test suite (`cross_primitive_merge_rollback_via_reopen`) merges across all five primitives, drops the database, reopens it, and verifies every invariant — including secondary index lookups returning the merged value — without any manual recovery step.

You don't need to run a "post-merge fsck" or anything like it. Merge atomicity is a property of the engine.

---

## Error reference

| Error | Strategy that triggers it | What it means | How to resolve |
|---|---|---|---|
| `merge conflict: N keys differ` | `Strict` | Cell-level conflicts (KV/JSON property/Vector value/Graph property) under Strict | Switch to `LastWriterWins`, or use `cherry_pick_filtered` to apply only the non-conflicting subset |
| `event merge unsupported: divergent appends to stream X` | both | Both branches appended to the same `(space, stream)` since merge base | Revert one side's appends, or use a different stream ID per branch |
| `merge unsupported: vector collection X has incompatible dimensions` | both | Same collection name, different `dimension` | Recreate the collection with a consistent dimension before merging |
| `merge unsupported: vector collection X has incompatible distance metrics` | both | Same collection name, different `metric` | Recreate with a consistent metric |
| `dangling edge reference: edge X → Y but Y missing` | both | Source added edge whose endpoint target deleted (or vice versa) | Restore the missing node, or remove the edge, then re-merge |
| `orphaned edge reference: node X has incoming edges` | both | Source deleted a node that target's added edges still reference | Restore the node, or remove the referencing edges, then re-merge |
| `merge target concurrently modified` | both | A concurrent write modified the target between gather and apply (OCC) | Retry the merge — it's a transient race |
| `Cannot merge: no fork or merge relationship found` | both | The two branches were never forked from each other and have never been merged before | Use `cherry_pick` to copy specific keys, or fork one from the other first |

---

## See also

- [Branching Strategies](branching-strategies.md) — when to fork vs merge vs cherry-pick vs materialize
- [Branch Management](branch-management.md) — CLI walkthrough
- [Branching API Reference](../reference/branching-api.md) — every operation, every field, every error
- [Concepts: Branches](../concepts/branches.md) — the mental model
- [Design: Primitive-Aware Merge](../design/branching/primitive-aware-merge.md) — implementation deep dive
