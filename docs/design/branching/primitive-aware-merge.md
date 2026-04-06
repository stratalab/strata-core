# Primitive-Aware Branch Merge

**Date:** 2026-04-06
**Claim:** [12-architectural-claims.md §4 — "All primitives inherit branching uniformly"](../../coding-standards/12-architectural-claims.md) (currently `PARTIAL`)
**Roadmap:** [14-core-claims-verification-roadmap.md §4](../../coding-standards/14-core-claims-verification-roadmap.md)
**Prerequisites:** Three-way merge (complete, see [three-way-merge-implementation-plan.md](three-way-merge-implementation-plan.md)), COW branching (complete)
**Status:** Design proposal

---

## Context

Strata's three-way merge correctly diffs and reconciles `(space, type_tag, user_key)` triples across branches. That is enough for flat key/value semantics, but every non-KV primitive in Strata layers additional invariants on top of the raw storage keys:

- **Event** maintains a global `next_sequence` counter, per-stream metadata, and a SHA-256 hash chain rooted at `EventLogMeta.head_hash`.
- **Graph** stores nodes, edges, and adjacency in separate keys under a reserved `_graph_` space; secondary adjacency indexes must stay consistent.
- **JSON** values carry path-addressable document structure and are mirrored into secondary JSON indexes.
- **Vector** embeddings are mirrored into HNSW graphs, segment manifests, and mmap-backed heap files.

The current merge path (`crates/engine/src/branch_ops.rs:1365`) ignores every one of these. It loops through all five primitive type tags, diffs them identically as opaque `(key, value)` pairs, applies the winning side via raw `txn.put`/`txn.delete`, and calls `reload_secondary_backends()` to refresh the derived state.

That is why Claim 4 is rated `PARTIAL`: fork is real, but merge is **bytes-level**, not **semantics-level**. The most visible failure is Event — merging two branches that both appended to the same stream silently corrupts the hash chain and leaves `EventLogMeta.next_sequence` stale, even though the operation reports success.

This document defines a staged path to make merge primitive-aware across all five primitives, and to remove the remaining space-symmetry gap for Graph.

### Research reference

The three-way diff/merge framework is already validated against Dolt, lakeFS, and git merge-ort (see [three-way-merge-implementation-plan.md](three-way-merge-implementation-plan.md) §Research). This document builds on top of that framework; it does not revisit the diff algorithm. Where this doc cites prior art, it focuses on **per-primitive reconciliation** — Dolt's cell-level merge for structured values, git's rename/content handlers, lakeFS's primitive-specific validators.

---

## Current State (Code-First Audit)

### The generic merge path

**Entry:** `crates/engine/src/branch_ops.rs:1365` — `merge_branches()`

1. `compute_merge_base()` — resolves ancestor version from DAG or storage fork info.
2. `three_way_diff()` (`branch_ops.rs:1150`) — iterates all spaces × all five `DATA_TYPE_TAGS` (`branch_ops.rs:31`) and runs `classify_change()` on every `(space, tag, user_key)` triple. No primitive-specific logic.
3. Merge actions are flattened into a single `puts`/`deletes` pair and applied inside one transaction via `txn.put(...)`/`txn.delete(...)` (`branch_ops.rs:1459-1481`).
4. `reload_secondary_backends()` (`branch_ops.rs:1332`) fires `RefreshHooks` for vector, graph, and JSON indexes.

**Consequence:** every primitive gets the same treatment regardless of whether that treatment is semantically valid.

### Primitive-specific invariants the current path breaks

**Event** (`crates/engine/src/primitives/event.rs`)
- `append_event_in_txn()` (`event.rs:292`) computes `SHA-256(prev_hash || body)` and writes it into the event record.
- `EventLogMeta` (`event.rs:95`) tracks `next_sequence`, `head_hash`, and hash algorithm version.
- `StreamMeta` (`event.rs:56`) tracks `count`, `first_sequence`, `last_sequence`, `first_timestamp`, `last_timestamp` per stream.
- Merge does **none** of the following:
  - recompute hash chain after merged events land
  - update `EventLogMeta.next_sequence` / `head_hash` for the merged state
  - reassign sequence numbers when both branches appended past the fork point
  - update `StreamMeta` counts/bounds
- Result: a merge of two disjoint event writes produces a branch whose metadata is inconsistent with its events, and whose `head_hash` still points at the pre-merge tip. Downstream `event_get_by_type`, `range_by_time`, and hash-chain verification all become unreliable.

**Graph** (`crates/graph/src/keys.rs:93`)
- Hard-wired to the reserved `_graph_` space via `const GRAPH_SPACE: &str = "_graph_";`.
- Not space-symmetric with KV/JSON/Event/Vector — a user cannot put graph data in their own space.
- Nodes, edges, and adjacency are separate keys; a merge that keeps a node but drops one of its edges via LWW produces a dangling reference.
- Secondary adjacency indexes are rebuilt via `reload_secondary_backends()`, which is whole-backend reload, not incremental delta application.

**JSON**
- Values are mirrored into secondary JSON indexes keyed by path.
- Generic merge treats JSON documents as opaque blobs; there is no path-level reconciliation, so a LWW resolution on a conflicting document discards the target side entirely even when the two sides edited disjoint paths.

**Vector**
- Payload lives in KV; HNSW graphs and mmap heap files are derived state maintained by `SegmentedHnswBackend`.
- Raw key replay does not trigger HNSW neighbor reconstruction for the merged keys — only `reload_secondary_backends()` reloads the backend wholesale.
- Merge semantics for vector metadata updates, collection deletion, and segment sealing are undefined.

**KV** — the only primitive where the generic path is actually correct, because the storage keys *are* the primitive.

### Test coverage gap

`tests/integration/branching.rs` covers KV merge paths well (LWW, Strict, disjoint changes, three-way with inherited layers, repeated merge, issue #1695). There are **zero** tests for:

- Event merge producing a valid hash chain
- Event merge after both branches appended to the same stream
- JSON merge with disjoint path edits
- Vector merge correctness (index state after merge)
- Graph merge leaving adjacency indexes consistent
- Cross-primitive merges (KV + Event + JSON in one operation)
- Fork/merge invariant parity across primitives

---

## Design Principles

1. **Storage-layer diff stays generic.** The three-way diff over `(space, type_tag, user_key)` triples is correct for identity and change detection; only the **reconciliation and application** step needs to become primitive-aware.
2. **Primitives declare their merge contract in code, not policy.** A `PrimitiveMergeHandler` trait is the single place per-primitive semantics live. No `match` statements scattered across `branch_ops.rs`.
3. **Unsafe is explicit, not silent.** Any primitive without a validated handler must reject merge with a structured error instead of executing the unsafe generic replay path.
4. **Derived state recovery is part of the merge contract.** A handler that mutates KV is responsible for either producing the incremental deltas needed by its secondary backends, or declaring that a full rebuild is required.
5. **No new DAG shape.** The three-way framework, merge base resolution, and OCC TOCTOU checks already exist. This work plugs into them; it does not replace them.
6. **Regression gates come first.** Every primitive-aware handler lands with red tests that prove the prior generic path was broken and the new handler fixes it.

---

## Architecture

### The `PrimitiveMergeHandler` trait

**New file:** `crates/engine/src/branch_ops/primitive_merge.rs`

```rust
/// A handler for per-primitive merge semantics.
///
/// The three-way diff produces a stream of `(user_key, MergeDecision)` pairs
/// scoped to a single (space, type_tag). Each primitive gets its own handler
/// that turns those decisions into a set of transactional write actions and
/// optional secondary-state deltas, while validating primitive invariants.
pub trait PrimitiveMergeHandler: Send + Sync {
    fn type_tag(&self) -> TypeTag;

    /// Validate that this merge is even allowed.
    ///
    /// Called once per merge, before any actions are produced, with a summary
    /// of what the diff found on both sides. Handlers that cannot safely merge
    /// a particular shape (e.g. Event with divergent writes to the same stream)
    /// return `Err(MergeError::Unsupported { .. })` here, which aborts the
    /// entire merge cleanly.
    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> Result<(), MergeError>;

    /// Produce the concrete write plan for this primitive.
    ///
    /// Handlers return a `PrimitiveMergePlan` rather than applying writes
    /// directly so the engine can still apply all primitives inside one
    /// transaction and keep OCC conflict detection intact.
    fn plan(&self, ctx: &MergePlanCtx<'_>) -> Result<PrimitiveMergePlan, MergeError>;

    /// Apply post-commit fix-ups to secondary state.
    ///
    /// Called after the main merge transaction commits successfully. Handlers
    /// use this to refresh HNSW graphs, rebuild adjacency indexes, recompute
    /// EventLog metadata, etc. Must be idempotent — a retry after crash must
    /// converge to the same state.
    fn post_commit(&self, ctx: &MergePostCommitCtx<'_>) -> Result<(), MergeError>;
}

pub struct PrimitiveMergePlan {
    /// Raw storage writes to apply inside the merge transaction.
    pub puts: Vec<(InternalKey, Vec<u8>)>,
    pub deletes: Vec<InternalKey>,
    /// Structured conflicts surfaced by this handler (separate from raw
    /// KV-level conflicts so the caller can report them accurately).
    pub conflicts: Vec<PrimitiveConflict>,
}
```

### Dispatch

`merge_branches()` changes from:

```text
for tag in DATA_TYPE_TAGS:
    diff → actions → flat puts/deletes
apply in one txn
reload secondary backends
```

to:

```text
for tag in DATA_TYPE_TAGS:
    handler = registry.get(tag)
    decisions = three_way_diff(tag)
    handler.precheck(decisions)           ← can abort whole merge
    plan = handler.plan(decisions)        ← may reject specific keys
    collect(plan.puts, plan.deletes, plan.conflicts)
apply combined plans in one txn
for tag in DATA_TYPE_TAGS:
    handler.post_commit(...)              ← primitive-aware fix-ups
```

The **registry** is constructed once per merge and contains the five handlers. The three-way diff helpers in `branch_ops.rs:1150` are refactored to yield decisions per `(space, type_tag)` slice rather than flattening everything into one action vector.

Critically:
- Conflict detection at the raw KV level (the existing 14-case `classify_change` matrix) stays exactly where it is. Handlers operate on the **output** of that matrix.
- The OCC TOCTOU check in the merge transaction (`branch_ops.rs:1464`) is unchanged.
- `reload_secondary_backends()` is kept as a fallback for handlers that declare "full rebuild required", but its use becomes explicit per-handler rather than a blanket post-merge sweep.

### Handler responsibilities at a glance

| Primitive | Precheck | Plan | Post-commit |
|---|---|---|---|
| **KV** | None | Pass-through of generic decisions | None |
| **JSON** | None (v1); path-aware conflict detection (v2) | Pass-through + optional 3-way document merge | Refresh JSON indexes for affected keys only |
| **Event** | Reject divergent same-stream writes (v1); stream-aware resequencing (v2) | Compute stream-aware writes + recomputed `EventLogMeta` | Rebuild hash chain head, refresh `StreamMeta` |
| **Vector** | Reject collection-level divergence (v1) | Compute vector payload writes + mark affected collections | Rebuild HNSW for affected collections |
| **Graph** | Reject merges that would leave dangling edges | Compute node/edge writes with referential integrity | Rebuild adjacency indexes for affected nodes only |

This table is the north star. Each slice below fills in one row.

---

## Phase 1 — Event safety (4.1 + 4.2)

**Goal:** stop silent Event-merge corruption today. Make the failure mode explicit and regression-gated.

**This phase does not introduce the `PrimitiveMergeHandler` trait yet** — it is a targeted safety fix that buys time to land the trait cleanly in Phase 2.

### Step 1.1: Red test — Event merge breaks hash chain

**File:** `tests/integration/branching.rs`

New test `event_merge_currently_corrupts_hash_chain`:

1. Create DB, append events `e1`, `e2` on `main`.
2. Fork `main → branch_a`.
3. Append `e3_a` on `branch_a`, append `e3_b` on `main` (same stream, disjoint content).
4. Merge `branch_a → main` with `LastWriterWins`.
5. Assert that verifying the hash chain from `e1` forward on `main` returns `Err(HashChainBroken)`.

The test is marked `#[ignore]` initially with a comment pointing to this doc; it gets un-ignored (and inverted) in Step 1.3.

### Step 1.2: Detect divergent Event writes in merge

**File:** `crates/engine/src/branch_ops.rs`

Add a helper `detect_event_divergence()` that runs after `three_way_diff` but before action application. It scans the diff output for `TypeTag::Event` entries and returns `true` if both sides have any non-trivial change (including adds) to Event keys since the merge base.

### Step 1.3: Reject divergent Event merges

Introduce `MergeError::EventMergeUnsupported { source_count: usize, target_count: usize }`.

In `merge_branches()`, immediately after `three_way_diff`:

```rust
if detect_event_divergence(&diff) {
    return Err(StrataError::Merge(MergeError::EventMergeUnsupported { .. }));
}
```

Un-ignore the Phase 1.1 test and update its assertion: the merge call must now return `MergeError::EventMergeUnsupported`, not silently succeed.

### Step 1.4: Positive test — single-sided Event merges still work

New test `event_merge_single_sided_succeeds`:

1. Fork `main → branch_a`.
2. Append events **only** on `branch_a`.
3. Merge `branch_a → main`.
4. Assert merge succeeds, hash chain on `main` verifies, `EventLogMeta` reflects the merged events correctly.

Note: this test exposes whether even the "easy case" (one-sided merge) produces correct `EventLogMeta`. If it fails, Step 1.5 is required.

### Step 1.5: One-sided Event merge metadata fix-up (conditional)

If Step 1.4 fails, add a narrow fix-up that runs when `detect_event_divergence()` returns `false` but there **are** Event puts from the source:

- After the merge transaction commits, reload `EventLogMeta` for the target branch.
- Recompute `next_sequence` from the highest merged sequence + 1.
- Recompute `head_hash` by walking the merged events in sequence order.
- Persist the updated `EventLogMeta`.

This fix-up is intentionally kept local to `branch_ops.rs` at this phase — it is not yet the general handler infrastructure.

### Phase 1 acceptance bar

- Merging branches with divergent Event writes fails with a clear error.
- Merging branches with one-sided Event writes succeeds **and** leaves a verifiable hash chain + correct `EventLogMeta`.
- Regression tests for both cases pass deterministically.

---

## Phase 2 — `PrimitiveMergeHandler` trait + KV/JSON handlers

**Goal:** introduce the dispatch architecture without changing behavior for already-working primitives.

### Step 2.1: Create the trait and registry

**New file:** `crates/engine/src/branch_ops/primitive_merge.rs`

Define `PrimitiveMergeHandler`, `MergePrecheckCtx`, `MergePlanCtx`, `MergePostCommitCtx`, `PrimitiveMergePlan`, `PrimitiveConflict`, `MergeError::Unsupported { .. }`.

Define `MergeHandlerRegistry`:

```rust
pub struct MergeHandlerRegistry {
    handlers: BTreeMap<TypeTag, Arc<dyn PrimitiveMergeHandler>>,
}
```

`BTreeMap` (not `HashMap`) for deterministic iteration order, matching the existing `DATA_TYPE_TAGS` ordering.

### Step 2.2: Refactor `three_way_diff` output

**File:** `crates/engine/src/branch_ops.rs`

Change `three_way_diff` to return decisions grouped by `(space, type_tag)` rather than a flat `Vec<MergeAction>`. The classification matrix and conflict detection are unchanged; only the shape of the return value changes.

### Step 2.3: Route merge through handlers

`merge_branches()` becomes:

```rust
let diff = three_way_diff(...);
let registry = build_merge_registry(&db);

// Precheck all handlers before producing any writes.
for tag in DATA_TYPE_TAGS {
    let handler = registry.get(tag);
    handler.precheck(&MergePrecheckCtx { .. })?;
}

// Produce plans.
let mut combined_puts = Vec::new();
let mut combined_deletes = Vec::new();
let mut conflicts = Vec::new();
for tag in DATA_TYPE_TAGS {
    let plan = registry.get(tag).plan(&MergePlanCtx { .. })?;
    combined_puts.extend(plan.puts);
    combined_deletes.extend(plan.deletes);
    conflicts.extend(plan.conflicts);
}

// Apply in one transaction (unchanged).
db.transaction(target, |txn| { ... })?;

// Post-commit fix-ups.
for tag in DATA_TYPE_TAGS {
    registry.get(tag).post_commit(&MergePostCommitCtx { .. })?;
}
```

### Step 2.4: `KvMergeHandler`

Trivial pass-through. `precheck` is a no-op. `plan` converts the 14-case decisions straight into puts/deletes — literally the current logic, lifted into the handler. `post_commit` is a no-op.

### Step 2.5: `JsonMergeHandler` (v1 — pass-through parity)

Same as `KvMergeHandler` for v1. `post_commit` refreshes **only** the JSON index entries for keys this handler touched, not the whole backend. This is an incremental improvement over `reload_secondary_backends()` but preserves existing semantics.

Path-level semantic merge is deferred to Phase 5 (see §Phase 5).

### Step 2.6: `EventMergeHandler`

Absorbs the Phase 1 safety logic into the trait:
- `precheck` returns `Unsupported` for divergent Event writes (the Phase 1.2 detector moves here).
- `plan` handles the one-sided case (the Phase 1.5 logic moves here).
- `post_commit` recomputes `EventLogMeta` and `StreamMeta`.

### Step 2.7: `VectorMergeHandler` (placeholder)

`precheck` returns `Unsupported` for any divergent vector writes. `plan` handles only the trivial "no vector changes either side" case. This is a deliberate hold until Phase 4.

### Step 2.8: `GraphMergeHandler` (placeholder)

Same shape as Vector — `precheck` rejects divergent graph writes; single-sided graph merges flow through a trivial pass-through that also invokes the existing adjacency reload. Full semantic handler is Phase 3.

### Step 2.9: Regression sweep

Every existing test in `tests/integration/branching.rs` must pass unchanged. Because KV is pass-through and JSON is pass-through+narrower index refresh, no behavior change is expected.

### Phase 2 acceptance bar

- All five primitives route through `PrimitiveMergeHandler`.
- KV and JSON merges behave identically to today at user-visible level.
- Vector and Graph divergent merges now return `Unsupported` with clear error messages instead of silently producing inconsistent state.
- Event handler is Phase 1's logic, restructured into the new trait.
- `reload_secondary_backends()` is either removed or reduced to a handler-internal helper (not a blanket post-merge operation).

---

## Phase 3 — Graph-aware merge

**Goal:** Graph handler with real node/edge reconciliation and incremental adjacency rebuild.

### Step 3.1: Graph merge model

Graph state in the reserved `_graph_` space decomposes into:

- **Node records** — identity + properties.
- **Edge records** — identity + src/dst + properties.
- **Adjacency index entries** — `(node_id, direction) → edge_id` postings.

Edges have referential integrity against nodes. Adjacency is derived from edge records.

Merge decisions per category:

| Category | v1 Rule |
|---|---|
| Node add/modify/delete with no conflict | Apply directly |
| Node conflict (both sides modified properties) | LWW or Strict (unchanged from generic) |
| Edge add where both endpoints exist post-merge | Apply directly |
| Edge add where an endpoint is missing post-merge | **Reject** with `DanglingEdgeReference` conflict |
| Node delete where edges still reference the node post-merge | **Reject** with `OrphanedEdgeReference` conflict |

These two rejection cases are the minimum referential-integrity guarantees; richer semantic merge (property cell-level merge, topology-aware conflict detection) is out of scope for v1.

### Step 3.2: Plan construction

`GraphMergeHandler::plan` runs a two-pass algorithm over the diff output scoped to `TypeTag::Graph`:

1. **Pass 1** — compute the projected final state of nodes (which node IDs exist after merge).
2. **Pass 2** — validate every edge write against the projected node set; reject edges whose endpoints would be absent.

Adjacency index writes are **not** included in the plan's `puts`/`deletes`. They are rebuilt in `post_commit` from the merged edge set.

### Step 3.3: Incremental adjacency rebuild

`GraphMergeHandler::post_commit`:

1. Collect the set of `affected_node_ids` from the merge decisions.
2. For each affected node, drop the existing adjacency entries and re-emit them from the current edge state.
3. Do **not** touch adjacency for unaffected nodes.

This is strictly narrower than `reload_secondary_backends()`, which today re-initializes the entire graph backend.

### Step 3.4: Graph merge tests

- `graph_merge_disjoint_nodes_succeeds`
- `graph_merge_disjoint_edges_succeeds`
- `graph_merge_dangling_edge_rejected`
- `graph_merge_orphaned_edge_on_node_delete_rejected`
- `graph_merge_adjacency_consistent_after_merge` — property-style: after merge, traversing adjacency indexes reproduces exactly the set of edges in storage.
- `graph_merge_cross_primitive` — combined KV + Graph merge, verify both land atomically.

### Phase 3 acceptance bar

- Graph merge never produces dangling edges or orphaned adjacency entries.
- Adjacency rebuild is incremental (cost is proportional to the merge diff, not the graph size).
- All graph merge tests pass.

---

## Phase 4 — Vector-aware merge

**Goal:** real vector merge with HNSW consistency.

### Step 4.1: Vector merge model

Vector state decomposes into:

- **Vector payload** — the embedding + metadata, stored in KV.
- **Collection config** — stored in KV.
- **HNSW graph** — per-collection derived state, stored in segmented HNSW backend.
- **Segment manifest** — on-disk state.

Merge decisions per category:

| Category | v1 Rule |
|---|---|
| Disjoint vector adds on different collections | Apply, rebuild affected collections |
| Disjoint vector adds on the same collection | Apply, rebuild that collection |
| Same vector ID written on both sides with different embeddings | LWW or Strict |
| Collection creation/deletion conflict | Strict rejection in v1 |
| Metric/dimension mismatch | Strict rejection (never automatically mergeable) |

### Step 4.2: Plan construction

`VectorMergeHandler::plan`:
- Produces KV puts/deletes for vector payload and collection config.
- Tracks `affected_collections: BTreeSet<CollectionId>` — any collection with at least one vector write on either side since the merge base.

### Step 4.3: HNSW rebuild

`VectorMergeHandler::post_commit`:
- For each affected collection, call the vector backend's rebuild entry point (the same path used by recovery — this already exists; see `crates/engine/src/primitives/vector/recovery.rs`).
- For unaffected collections, do nothing.

### Step 4.4: Vector merge tests

- `vector_merge_disjoint_collections`
- `vector_merge_same_collection_disjoint_ids`
- `vector_merge_conflicting_id_lww`
- `vector_merge_dimension_mismatch_rejected`
- `vector_merge_hnsw_search_correct_after_merge` — after merge, a k-NN search returns the expected set including vectors from both sides.

### Phase 4 acceptance bar

- Vector merge is no longer blocked by Phase 2's placeholder `Unsupported`.
- Merged collections return correct k-NN results without a full backend reload.
- HNSW rebuild is per-collection, not global.

---

## Phase 5 — JSON path-level merge

**Goal:** merge disjoint path edits on the same JSON document instead of discarding one side via LWW.

### Step 5.1: Document-level three-way diff

Add a JSON diff helper that, given `(ancestor_doc, source_doc, target_doc)`, returns a per-path change set. Reuse an existing JSON path library where possible; do not hand-roll a new diff algorithm.

### Step 5.2: Path-level conflict rules

- Both sides edited disjoint paths → merge both edits.
- Both sides edited the same path to the same value → no conflict.
- Both sides edited the same path to different values → conflict (LWW or Strict).
- One side deleted a subtree the other edited → conflict.

### Step 5.3: Plan construction

`JsonMergeHandler::plan` (v2) produces a single merged document value per conflicting key, replacing the v1 pass-through behavior.

### Step 5.4: JSON index refresh

`post_commit` refreshes JSON index entries for the affected document keys. Same narrow scope as Phase 2.

### Step 5.5: JSON merge tests

- `json_merge_disjoint_paths_auto_merges`
- `json_merge_same_path_same_value_no_conflict`
- `json_merge_same_path_different_values_lww`
- `json_merge_subtree_delete_vs_edit_conflict`

### Phase 5 acceptance bar

- JSON merges auto-merge disjoint path edits without user intervention.
- Index state is consistent with merged document state.

---

## Phase 6 — Graph space symmetry

**Goal:** remove the hard-wired `_graph_` reservation so Graph is space-symmetric with the other primitives.

### Step 6.1: Parameterize `GRAPH_SPACE`

**File:** `crates/graph/src/keys.rs`

Replace `pub const GRAPH_SPACE: &str = "_graph_";` with a per-operation `space: &str` parameter, threaded through the Graph API surface. Default to `_graph_` for backward compatibility.

### Step 6.2: Graph API updates

Graph executor commands accept an optional `space` argument. When absent, they use the default. When present, they operate on the caller's user space.

### Step 6.3: Merge handler update

`GraphMergeHandler` already iterates `(space, TypeTag::Graph)` tuples from the diff — no change needed beyond removing any code paths that assume `_graph_` is the only graph space.

### Step 6.4: Tests

- `graph_in_user_space_survives_fork`
- `graph_in_user_space_survives_merge`
- `graph_in_multiple_spaces_independent`

### Phase 6 acceptance bar

- Graph data can live in any user space.
- Fork and merge treat graph data in user spaces identically to graph data in `_graph_`.
- Claim 4's "Graph is not space-symmetric" caveat in docs/12 can be removed.

---

## Phase 7 — Cross-primitive parity test suite

**Goal:** prove primitive-aware branching is uniform by exercising it end-to-end.

### Tests

1. **`fork_isolation_all_primitives`** — fork a branch, mutate each primitive independently on each side, verify fork isolation at every primitive.
2. **`merge_all_primitives_disjoint`** — on each side of a fork, mutate a different primitive; merge; verify every primitive's final state is correct.
3. **`merge_all_primitives_overlapping`** — on both sides, mutate every primitive with overlapping keys; merge with LWW; verify each primitive's conflict handling is semantically correct (not just bytes-correct).
4. **`merge_invariant_sweep`** — after a cross-primitive merge, run each primitive's invariant checker:
   - Event: hash chain verifies + `EventLogMeta` is correct
   - Graph: no dangling edges, adjacency matches edge records
   - Vector: HNSW k-NN returns full merged set
   - JSON: index consistent with documents
   - KV: no spurious writes
5. **`merge_rollback_all_primitives`** — simulate a crash after WAL commit but before `post_commit` handlers run; verify recovery converges to the correct merged state for every primitive.

### Phase 7 acceptance bar

- Every primitive passes its invariant checker after every cross-primitive merge scenario.
- The phrase "all primitives inherit branching uniformly" is justified by behavior, not storage plumbing.

---

## Error model

New variants on `MergeError`:

```rust
pub enum MergeError {
    // existing variants ...

    /// A primitive handler rejected the merge during precheck.
    Unsupported {
        type_tag: TypeTag,
        reason: String,
    },

    /// Event handler rejected divergent writes to the same stream.
    EventMergeDivergent {
        stream_id: String,
        source_seq_range: (u64, u64),
        target_seq_range: (u64, u64),
    },

    /// Graph handler rejected an edge whose endpoint would be absent post-merge.
    DanglingEdgeReference {
        edge_id: String,
        missing_node: String,
    },

    /// Graph handler rejected a node delete that would orphan edges.
    OrphanedEdgeReference {
        node_id: String,
        edge_ids: Vec<String>,
    },

    /// Vector handler rejected a collection with incompatible dimension/metric.
    VectorCollectionIncompatible {
        collection: String,
        reason: String,
    },
}
```

All variants carry enough context for the caller to understand the rejection without re-running the merge.

---

## Acceptance Criteria for Claim 4 `VERIFIED`

Claim 4 moves from `PARTIAL` to `VERIFIED` when:

1. Every primitive routes through `PrimitiveMergeHandler` — no generic replay fallback.
2. Event merge either succeeds with a verifiable hash chain + correct metadata, or rejects with `EventMergeDivergent`. No silent corruption is possible.
3. Graph merge enforces referential integrity and rebuilds adjacency incrementally.
4. Vector merge rebuilds affected HNSW collections and not the whole backend.
5. JSON merge auto-merges disjoint path edits.
6. Graph supports user-space placement, not just `_graph_`.
7. Cross-primitive parity test suite (Phase 7) passes.
8. `docs/coding-standards/12-architectural-claims.md` §Claim 4 is rewritten to reflect the new guarantees, including a test citation list.

---

## Open Questions

1. **Strict vs LWW for primitive conflicts** — should every primitive-specific conflict type (dangling edge, divergent event, dimension mismatch) honor `MergeStrategy::LastWriterWins`, or are some conflicts always fatal regardless of strategy? Current proposal: referential integrity and dimension-mismatch errors are **always fatal**; cell-level property conflicts honor strategy.

2. **Crash recovery for `post_commit` handlers** — if a merge transaction commits but the process crashes during `post_commit`, what guarantees does the user get on next open? Options:
   - Re-run all `post_commit` handlers on open (requires them to be fully idempotent).
   - Record a "merge in progress" marker in the branch metadata and refuse reads until `post_commit` completes.
   - Make `post_commit` synchronous and part of the same WAL transaction (expensive; may not be possible for HNSW rebuild).

   Current proposal: idempotent handlers + re-run on open. Phase 7 test #5 exercises this path.

3. **Graph space symmetry rollout** — should Phase 6 happen before the per-primitive handlers or after? Doing it first means handlers are built space-generic from the start; doing it later avoids mixing a handler refactor with an API change.

   Current proposal: Phase 6 after the handlers land, as a follow-up.

4. **Vector merge under segmented HNSW** — does the current `SegmentedHnswBackend` support per-collection rebuild without touching other collections' segments? The recovery path suggests yes, but this needs to be validated before Phase 4 starts.

5. **Event resequencing for the "both sides appended disjointly" case** — Phase 1 rejects this outright. A v2 Event handler could resequence: pick a canonical ordering (e.g., timestamp, then source branch ID as tiebreaker), reassign sequence numbers, and recompute the hash chain. Is that behavior desirable, or should divergent Event streams always require explicit user action?

   These are non-blocking for Phase 1 but need answers before Event handler v2.

---

## Recommended Execution Order

1. **Phase 1** (Event safety) — smallest, ships a user-visible safety fix, sets up the regression gate. Target: one PR.
2. **Phase 2** (handler trait + KV/JSON pass-through + Event handler absorption) — architectural scaffold, no behavior change for KV/JSON. Target: one PR.
3. **Phase 3** (Graph handler) — first real semantic handler, proves the architecture.
4. **Phase 4** (Vector handler) — second semantic handler.
5. **Phase 5** (JSON path-level merge) — v2 of the JSON handler.
6. **Phase 6** (Graph space symmetry) — API change, isolated from handler work.
7. **Phase 7** (cross-primitive parity suite) — final gate for Claim 4 `VERIFIED`.

Phases 1 and 2 together close the acute safety gap. Phases 3–5 deliver the positive correctness story. Phase 6 cleans up the remaining Claim 4 caveat. Phase 7 is the proof.
