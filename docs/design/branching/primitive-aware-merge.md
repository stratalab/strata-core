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

> **Phased trait surface.** The trait above is the eventual shape. Phase 2 ships only `precheck` + `post_commit` — the `plan` method is added in Phase 3+ when individual handlers actually need to produce per-primitive write plans. Adding `plan` in Phase 2 with every handler returning a pass-through wrapper would be dead architecture.

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

## Phase 2 — `PrimitiveMergeHandler` dispatch scaffold

**Goal:** introduce the dispatch architecture and migrate the Phase 1 Event check into a real `EventMergeHandler`. Pure architectural plumbing — no new user-visible behavior beyond what Phase 1 already shipped.

### Scope deviation from earlier draft

An earlier draft of this section said Phase 2 should also add "Vector and Graph divergent merges return `Unsupported`." Code-first exploration of the post-Phase-1 codebase revealed that rule is wrong:

- **Vector merges are actually safe today.** `VectorRefreshHook::post_merge_reload` (`crates/engine/src/recovery.rs:566`) does a *full* HNSW rebuild from KV state on every merge. Disjoint and conflicting vector merges both produce correct HNSW after the rebuild. There is no silent corruption to refuse — Phase 4's job is to make the rebuild *incremental*, not to add a refusal.
- **Graph merges have a real staleness gap** — there is no `GraphRefreshHook`, so the in-memory adjacency cache is stale post-merge until the next compaction. Fixing this properly requires new graph backend code (a `GraphRefreshHook` implementation), which belongs in Phase 3 alongside the rest of the graph-aware merge work.
- **JSON merges have a real staleness gap** too — there is no `JsonRefreshHook`, so JSON inverted-index entries are stale post-merge. Fixing this requires per-key index refresh plumbing, which belongs in Phase 5 alongside path-level merge.

Phase 2 is therefore scoped to **scaffolding only**. Every per-primitive semantic improvement lives in its dedicated phase where the primitive gets focused attention.

### Step 2.1: Convert `branch_ops.rs` to a directory module

**File move:** `crates/engine/src/branch_ops.rs` → `crates/engine/src/branch_ops/mod.rs` (no content change in this step). Adds `pub mod primitive_merge;`. Cleaner than dumping ~200 more lines into the existing 2,500-line file.

### Step 2.2: Create the trait and registry

**New file:** `crates/engine/src/branch_ops/primitive_merge.rs`

```rust
pub trait PrimitiveMergeHandler: Send + Sync {
    fn type_tag(&self) -> TypeTag;
    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> StrataResult<()>;
    fn post_commit(&self, ctx: &MergePostCommitCtx<'_>) -> StrataResult<()>;
}

pub struct MergePrecheckCtx<'a> {
    pub source_id: BranchId,
    pub target_id: BranchId,
    pub merge_base: &'a MergeBase,
    pub strategy: MergeStrategy,
    pub typed_entries: &'a TypedEntries,
}

pub struct MergePostCommitCtx<'a> {
    pub db: &'a Arc<Database>,
    pub source_id: BranchId,
    pub target_id: BranchId,
    pub merge_version: Option<u64>,
}

pub struct MergeHandlerRegistry {
    handlers: BTreeMap<TypeTag, Arc<dyn PrimitiveMergeHandler>>,
}
```

Note that **`plan` is not on the trait yet** — see the "Phased trait surface" callout in §Architecture above. Phases 3+ will add it when individual handlers need per-primitive write plans.

### Step 2.3: Split `three_way_diff` into gather + classify

**File:** `crates/engine/src/branch_ops/mod.rs`

`three_way_diff` is split into two functions so the gathered entry slices can be reused by `EventMergeHandler::precheck`:

```rust
pub(crate) struct TypedEntries {
    pub cells: BTreeMap<(String, TypeTag), TypedEntryCell>,
}

pub(crate) struct TypedEntryCell {
    pub ancestor: Vec<VersionedEntry>,
    pub source: Vec<(Key, VersionedValue)>,
    pub target: Vec<(Key, VersionedValue)>,
}

fn gather_typed_entries(...) -> StrataResult<TypedEntries> { ... }
fn classify_typed_entries(typed: &TypedEntries, strategy: MergeStrategy)
    -> (Vec<MergeAction>, Vec<ConflictEntry>) { ... }

fn three_way_diff(...) -> StrataResult<(Vec<MergeAction>, Vec<ConflictEntry>)> {
    let typed = gather_typed_entries(...)?;
    Ok(classify_typed_entries(&typed, strategy))
}
```

The 14-case `classify_change` matrix is unchanged. `cherry_pick_from_diff` keeps calling `three_way_diff` and consuming the same flat `(actions, conflicts)` shape — no caller-side change.

The Phase 1 Event divergence check is **removed from the per-cell loop** in this step. It moves to `EventMergeHandler::precheck` (Step 2.5) and is also called explicitly from `cherry_pick_from_diff` (Step 2.6) so the cherry-pick API keeps the safety contract.

### Step 2.4: Route `merge_branches` through handlers

```rust
let typed = gather_typed_entries(...)?;
let registry = build_merge_registry();
let precheck_ctx = MergePrecheckCtx { source_id, target_id, merge_base: &merge_base, strategy, typed_entries: &typed };
for tag in DATA_TYPE_TAGS {
    registry.get(tag).precheck(&precheck_ctx)?;     // ← EventHandler runs Phase 1 check here
}
let (actions, conflicts) = classify_typed_entries(&typed, strategy);
// ... existing strict-conflict short-circuit + OCC TOCTOU + apply transaction ...
let post_ctx = MergePostCommitCtx { db, source_id, target_id, merge_version };
for tag in DATA_TYPE_TAGS {
    registry.get(tag).post_commit(&post_ctx)?;      // ← all no-ops in Phase 2
}
reload_secondary_backends(db, target_id, source_id);
```

`reload_secondary_backends` stays put after the post_commit loop — see Step 2.7.

### Step 2.5: Five handler structs

| Handler | `precheck` | `post_commit` |
|---|---|---|
| `KvMergeHandler` | no-op | no-op |
| `JsonMergeHandler` | no-op | no-op (Phase 5 will add per-key index refresh) |
| `EventMergeHandler` | iterates `ctx.typed_entries.cells`, calls `check_event_merge_divergence` for each `(space, TypeTag::Event)` cell | no-op |
| `VectorMergeHandler` | no-op | no-op (Vector is safe today via full HNSW rebuild) |
| `GraphMergeHandler` | no-op | no-op (Phase 3 will add adjacency rebuild + dangling-edge detection) |

Each handler is a unit struct. The `check_event_merge_divergence` helper from Phase 1 stays as a free function in `branch_ops/mod.rs`; `EventMergeHandler` calls it via the `super::` path.

### Step 2.6: Update `cherry_pick_from_diff`

`cherry_pick_from_diff` does not use the handler registry (it has its own filtering pipeline). To preserve the Phase 1 Event safety contract for cherry-pick, the function is updated to call `gather_typed_entries` directly and run the Event check before classification:

```rust
let typed = gather_typed_entries(db, source_id, target_id, &merge_base, &all_spaces, snapshot_version)?;
for ((space, type_tag), cell) in &typed.cells {
    if *type_tag == TypeTag::Event {
        check_event_merge_divergence(space, &cell.ancestor, &cell.source, &cell.target)?;
    }
}
let (actions, _conflicts) = classify_typed_entries(&typed, MergeStrategy::LastWriterWins);
// ... existing filter + apply unchanged ...
```

One safety helper, two callers.

### Step 2.7: `reload_secondary_backends` stays untouched

The earlier draft proposed removing or reducing `reload_secondary_backends` in Phase 2. With the scoped-down handler set (every Phase 2 handler has a no-op `post_commit`), nothing replaces what the existing reload sweep does for Vector. Touching `reload_secondary_backends` now without per-handler refresh logic is churn. It is kept exactly as-is and gets retired incrementally as Phases 3 (Graph adjacency rebuild) and 5 (JSON per-key refresh) introduce real handler-owned post_commit logic.

### Step 2.8: Regression sweep

Every existing test in `tests/integration/branching.rs` must pass unchanged. The Phase 1 event_merge_* tests in particular must keep passing because the Event safety is now sourced from `EventMergeHandler::precheck` instead of an inline check inside `three_way_diff`. One new unit test pins the registry contract: `build_merge_registry()` returns exactly the five expected `TypeTag` keys.

### Phase 2 acceptance bar

- All five primitives route through `PrimitiveMergeHandler` via the registry.
- KV, JSON, Vector, and Graph merges behave identically to today at the user-visible level.
- The Phase 1 Event divergence check is now sourced from `EventMergeHandler::precheck`, not from an inline check inside `three_way_diff`.
- `cherry_pick_from_diff` continues to refuse divergent Event input via the same shared helper.
- `reload_secondary_backends` is unchanged. Phases 3 and 5 will retire it incrementally.
- All 33 existing branching tests + 4 Phase 1 event merge tests pass without modification.

---

## Phase 3 — Graph merge tactical refusal

**Goal:** refuse divergent graph branch merges with a structured error, mirroring the Phase 1 Event safety pattern. Single-sided graph merges continue to work. Real semantic graph merge is deferred to a follow-up phase (see "Phase 3b" below).

### Storage model correction

An earlier draft of this section assumed graph storage decomposed into separate node records, edge records, and an adjacency index. Code-first exploration of `crates/graph/` revealed this model is wrong:

- Edges are not stored as standalone records. Each edge has exactly two physical representations: an entry in `{graph}/fwd/{src}` (the source's outgoing list, packed binary) and an entry in `{graph}/rev/{dst}` (the destination's incoming list, packed binary). The packed adjacency lists ARE the edge storage. See `crates/graph/src/keys.rs:118-136` and `crates/graph/src/packed.rs`.
- There is no in-memory adjacency cache to "rebuild post-merge". `AdjacencyIndex` (`crates/graph/src/adjacency.rs:12`) is built on demand by `build_adjacency_index`, not maintained as backend state. The earlier claim that "adjacency is stale post-merge" was incorrect — there's no cache to be stale.
- The real merge problem is **maintaining bidirectional consistency between `fwd/*` and `rev/*` entries**, which the generic LWW path treats as independent KV keys.

The Phase 3 spec is therefore being split:

- **Phase 3 (this section, ships now)** — tactical refusal of divergent graph merges, mirroring Phase 1's Event pattern.
- **Phase 3b (future, ships later)** — real semantic graph merge with referential integrity, additive edge merging, decoded-edge-level diffing, and the `plan` trait method. Spec captured below.

### Corruption modes the tactical refusal closes

**Scenario A — bidirectional adjacency inconsistency under LWW.** Both branches add an outgoing edge from the same node (different targets). Generic merge classifies `fwd/X` as `Conflict`; LWW picks one whole list, losing the other side's edge. The losing side's `rev/{Z}` (which the winning side never touched) still references X, but X's `fwd/X` no longer references Z. `incoming_neighbors(Z)` returns X; `outgoing_neighbors(X)` does not return Z. Silent corruption.

**Scenario B — concurrent node delete and edge add.** Source deletes node X (drops `n/X`, `fwd/X`, and updates every `rev/*` that referenced X). Target adds edge X→Y (writes `fwd/X` and `rev/Y` with the new entry). LWW resolves `n/X` as `DeleteModifyConflict` → source wins → `n/X` deleted. `fwd/X` same. But `rev/Y` is `TargetAdded` → kept on target. Result: `rev/Y` references X but `n/X` doesn't exist. Dangling reference.

Neither scenario is theoretical. Both fall out of standard `LastWriterWins` whenever two branches independently modify the same neighborhood. There are zero existing tests covering graph merge today, so neither corruption mode is regression-gated.

### Step 3.1: Add `check_graph_merge_divergence` helper

**File:** `crates/engine/src/branch_ops/mod.rs`

Add a free function next to the existing `check_event_merge_divergence`. Same parameter shape (space + ancestor entries + source entries + target entries). Builds three maps via the existing `build_ancestor_map` / `build_live_map` helpers, then checks "did source's keys differ from ancestor's at any key, AND did target's keys differ at any (possibly different) key". Returns `Err(StrataError::invalid_input("merge unsupported: divergent graph writes in space '{space}' since fork. ..."))` if both sides diverged.

### Step 3.2: Wire into `GraphMergeHandler::precheck`

**File:** `crates/engine/src/branch_ops/primitive_merge.rs`

`GraphMergeHandler::precheck` (currently a no-op) iterates `ctx.typed_entries.cells` for `(space, TypeTag::Graph)` cells and calls the helper for each. Structurally identical to `EventMergeHandler::precheck`.

### Step 3.3: Extend `cherry_pick_from_diff`

**File:** `crates/engine/src/branch_ops/mod.rs`

The existing typed-cells safety loop in `cherry_pick_from_diff` already calls `check_event_merge_divergence` for Event cells. Add a sibling call for Graph cells inside the same loop.

### Step 3.4: Tests

**File:** `tests/integration/branching.rs`

Five new tests in a "Graph Merge Safety" section:
- `graph_merge_divergent_rejects` — fork, both sides add nodes/edges, merge under LWW and Strict, both reject
- `graph_merge_single_sided_source_succeeds` — only source has graph writes; merge succeeds; bidirectional consistency holds via `outgoing_neighbors`/`incoming_neighbors`
- `graph_merge_single_sided_target_succeeds` — symmetric
- `graph_merge_no_changes_succeeds` — fork with KV-only writes on both sides; graph divergence check does not falsely fire
- `graph_merge_concurrent_node_delete_and_edge_add_rejected` — Scenario B above (most important — pins down the most subtle corruption pattern)

### Phase 3 acceptance bar

- Divergent graph branch merges always refused with a structured error.
- Single-sided graph merges still work and produce bidirectionally consistent state.
- `cherry_pick_from_diff` also refuses divergent graph cherry-picks.
- All Phase 1 + Phase 2 tests still pass without modification.

---

## Phase 3b — Semantic graph merge (implemented)

**Status:** **Implemented.** Replaces Phase 3's tactical refusal with a real semantic merge.

**Goal:** real graph merge that preserves referential integrity, merges disjoint edges from both sides additively, and maintains bidirectional consistency between `fwd/*` and `rev/*` lists.

### Algorithm

For each `(space, TypeTag::Graph)` cell that the merge touches:

1. **Decode** all packed adjacency lists from ancestor / source / target via `crates/graph/src/packed.rs::decode`. Materialize the logical edge set per side: `HashMap<(src, dst, edge_type), EdgeData>`. Materialize the node set per side from `n/{node_id}` keys. Per-graph state lives in `PerGraphState`; the cell holds a `BTreeMap<graph_name, PerGraphState>` plus an optional catalog blob.
2. **Compute three-way diff at the node level.** For each `node_id`, apply a generic `classify_three_way<NodeData>` 14-case matrix. Build the projected node set.
3. **Compute three-way diff at the edge level.** For each `(src, dst, edge_type)` triple, apply `classify_three_way<EdgeData>`. Build the projected edge set.
4. **Validate referential integrity.** Every projected edge's endpoints must be in the projected node set; every node deletion must leave no projected edges referencing it. Violations are *always* fatal regardless of `MergeStrategy`.
5. **Catalog merge.** Phase 3b shipped this as opaque-KV with `CatalogDivergence` refusal; Phase 3c rewrote it as per-name additive set-union. See the Phase 3c subsection for the projection rule and the `None`-vs-empty-set fallback semantics.
6. **Project the post-merge state** into the canonical fwd/rev encoding. Sort each per-node edge list lexicographically before encoding so two equivalent merges produce byte-equal outputs.
7. **Edge counters** are derived from the projected edge set (count edges per type), not merged independently.
8. **Emit KV writes** for affected `n/{id}`, `fwd/{src}`, `rev/{dst}`, `__edge_count__/{type}`, and meta keys. Compare against target's current encoding to skip no-op writes.
9. **post_commit**: nothing — storage IS the adjacency, no separate index to refresh.

### Architecture

The graph crate cannot be a direct dependency of the engine crate (graph already depends on engine — adding the reverse edge would be a cycle). Phase 3b uses a registration callback pattern, mirroring the existing `register_recovery_participant` / `register_vector_recovery` hooks:

- **`crates/graph/src/merge.rs`** — pure algorithm. `compute_graph_merge`, decode helpers, `GraphMergeOutput`/`GraphMergeConflict` types. Independent of engine dispatch.
- **`crates/graph/src/merge_handler.rs`** — engine bridge. Defines a `graph_plan_fn(&MergePlanCtx) -> StrataResult<PrimitiveMergePlan>` function that decodes typed entries, calls `compute_graph_merge`, converts the output into engine-side `MergeAction`/`ConflictEntry`, and returns the plan. `pub fn register_graph_semantic_merge()` registers the function with the engine via `strata_engine::register_graph_merge_plan`.
- **`crates/engine/src/branch_ops/primitive_merge.rs`** — `GraphMergeHandler::plan` dispatches to the registered function if present. If unset (e.g. engine-only unit tests), falls back to Phase 3's tactical refusal of divergent graph merges + the default classify path. The trait gains a `plan` method with a default impl (`classify_typed_entries_for_tag`) that KV/JSON/Vector/Event handlers use unchanged.
- **Test fixtures** call `strata_graph::register_graph_semantic_merge()` from `ensure_recovery_registered` so all integration tests exercise the semantic merge path.

### Conflict semantics

| Conflict type | Strict | LastWriterWins |
|---|---|---|
| Node properties differ | reported, abort | reported, source wins |
| Edge data differs | reported, abort | reported, source wins |
| Modify-vs-delete (node or edge) | reported, abort | reported, source wins |
| Meta differ | reported, abort | reported, source wins |
| Other key (ontology, ref idx, type idx) differ | reported, abort | reported, source wins |
| **Dangling edge after merge** | **always reject** | **always reject** |
| **Orphan reference after node delete** | **always reject** | **always reject** |
| **Catalog divergence** | **always reject** | **always reject** |

The three "always reject" conflicts return `StrataError::invalid_input("merge unsupported: graph referential integrity violation in space '{space}': ...")` from the plan function, aborting the merge before any writes. Cell-level conflicts flow through as `ConflictEntry` rows in `MergeInfo.conflicts` and are caught by `merge_branches`'s strict-strategy check under `Strict`.

### Phase 3b acceptance bar (met)

- ✅ Two divergent graph branches with disjoint edge additions merge cleanly into a union of both sides' edges.
- ✅ A merge that would produce a dangling edge is refused with a clear error naming the offending edge and missing node.
- ✅ A merge that would produce an orphaned reference is refused with a clear error.
- ✅ After any successful merge, traversing `outgoing_neighbors(X)` and `incoming_neighbors(Y)` always agree on the existence of edge X→Y (verified by tests).
- ✅ The `plan` trait method is in place and KV/JSON/Vector/Event handlers use the default impl unchanged.

### Tests

- 12 unit tests in `crates/graph/src/merge.rs::tests` covering decode, classify, disjoint merges, dangling edges, orphan references, catalog conflicts, LWW vs Strict for property conflicts, and bidirectional consistency.
- 9 integration tests in `tests/integration/branching.rs::Graph Merge Safety` covering the same scenarios end-to-end through `merge_branches`, plus single-sided merges, KV-only regression, and Phase 3 scenarios that now succeed under Phase 3b.

---

## Phase 3c — Cherry-pick semantic merge + additive catalog merging

**Status:** implemented.

Phase 3b deferred two pieces of the original design that landed together as Phase 3c. (The doc previously called this slot "Phase 3b.5"; renamed to Phase 3c for consistency with the implementation history.)

### Cherry-pick semantic graph merge

Before Phase 3c, `cherry_pick_from_diff` called `check_graph_merge_divergence` directly and used `classify_typed_entries` for action production — bypassing the per-handler `plan` dispatch and the Phase 3b semantic graph merge entirely. Divergent graph cherry-picks were unconditionally refused even when the underlying changes were compatible.

Phase 3c refactors `cherry_pick_from_diff` to use the same `gather → precheck → plan → filter → apply` pipeline `merge_branches` uses. Concretely:

1. Build the `MergeHandlerRegistry` once.
2. Run each handler's `precheck` against the gathered typed entries.
3. Call each handler's `plan` to produce per-primitive `MergeAction`s. Cherry-pick always uses `MergeStrategy::LastWriterWins`, so non-fatal cell conflicts (NodeProperty, EdgeData, etc.) get silently resolved by the graph handler. Fatal conflicts (DanglingEdge / OrphanedReference) propagate as `Err` from the plan function and abort cherry-pick before any writes.
4. Run a graph-action atomicity check on the unfiltered actions (see below).
5. Apply the existing `CherryPickFilter` predicate to the actions and write the survivors in a single transaction.

The unfiltered `classify_typed_entries` wrapper (used only by cherry-pick) is removed; the filtered `classify_typed_entries_for_tag` stays for the default `plan` impl.

#### Graph action atomicity guard

Graph plan actions are interdependent — for one logical change like "source added node carol and edge alice→carol", the plan emits `n/carol`, `fwd/alice`, `rev/carol`, and `__edge_count__/follows`. Applying any subset breaks bidirectional consistency (e.g. node added but no adjacency entry). Cherry-pick's `CherryPickFilter` has three orthogonal dimensions:

- `spaces`: a (space, type_tag) cell is either entirely in or entirely out — atomic.
- `primitives`: `Graph` is either fully included or fully excluded — atomic.
- `keys`: a user-supplied set of `user_key` strings — *can* split actions within a cell.

Phase 3c adds `check_graph_action_atomicity(actions, filter)` which runs only when `filter.keys` is `Some(_)`. For each `(space, Graph)` cell, it counts how many of the cell's graph actions pass the full filter. If 0 pass → drop them all (fine). If all pass → keep them all (fine). Otherwise → return a structured `StrataError::invalid_input("cherry-pick: graph data must be applied atomically per cell. ... Use --primitives or --spaces filters instead of --keys when cherry-picking graph data.")`.

The check runs BEFORE the existing filter loop, so the error message reports the original counts before filtering modifies them.

### Additive catalog merging

The catalog (`__catalog__`) is a `Value::String` containing a JSON array of graph names. Phase 3b treated it as opaque KV via the 14-case matrix and reported any divergent edit as fatal `CatalogDivergence` — concurrent `create_graph` on different names was unmergeable.

Phase 3c rewrites `merge_catalog` in `crates/graph/src/merge.rs` to project per-name presence:

```text
projected = (anc - deleted_by_source - deleted_by_target)
            ∪ added_by_source ∪ added_by_target
```

Equivalently, for each name in `anc ∪ src ∪ tgt`:
- If src deleted it AND tgt didn't add it back → drop
- If tgt deleted it AND src didn't add it back → drop
- Otherwise → keep

Both sides are decoded into `BTreeSet<String>` and the projected set is re-encoded into the canonical JSON array. A `Put` is emitted only when `projected != tgt` (no spurious writes on no-op merges).

Because the projection is purely set-membership over unit-typed presence values, **no conflict variant ever fires** from the new `merge_catalog`. `GraphMergeConflict::CatalogDivergence` is kept in the enum (it's `pub`; removing it would be a breaking API change with no benefit) and documented as "reserved — current Phase 3c additive merging never produces this variant". `is_fatal()` continues to return `true` for it, so any future caller that DID produce it (e.g. an opt-in strict-catalog merge mode) would still abort the merge.

### Ontology semantic merge (still deferred)

Ontology entries (`{graph}/__types__/object/...`, `{graph}/__types__/link/...`) continue to be merged via the 14-case matrix at the raw KV level. Phase 3c does NOT introduce ontology-aware semantic merge. If users need richer ontology merging (e.g. additive type definitions, schema migration semantics), it warrants its own design pass.

### Phase 3c acceptance bar (met)

- ✅ `cherry_pick_from_diff` of two divergent graph branches with disjoint additions succeeds, with bidirectional consistency on the merged edges.
- ✅ A cherry-pick that would produce a dangling edge or orphaned reference is rejected with the same referential-integrity error `merge_branches` produces.
- ✅ `cherry_pick_from_diff` with a partial-graph-key filter is rejected with a clear "graph cherry-pick must be applied atomically" error naming the cell.
- ✅ `cherry_pick_from_diff` with `primitives = [KV]` cleanly drops all graph actions when source has divergent graph state, with no atomicity error.
- ✅ Concurrent `create_graph("g_src")` + `create_graph("g_tgt")` merge produces a catalog containing both new graphs.
- ✅ `delete_graph("g_drop")` on source + `create_graph("g_new")` on target merges into a catalog with `g_new` added and `g_drop` dropped — both intents honored.
- ✅ `CatalogDivergence` is reserved but no longer reachable from `merge_catalog`.

### Tests

- 4 new unit tests in `crates/graph/src/merge.rs::tests` covering additive catalog merging (disjoint creates, mixed delete/create, both-delete-different, no-op when target already has the union).
- 7 new integration tests in `tests/integration/branching.rs`:
  - 4 cherry-pick scenarios: disjoint graph divergences, dangling-edge rejection, atomicity guard with `keys` filter, `primitives = [KV]` exclusion.
  - 1 KV-only cherry-pick regression check (no graph changes).
  - 2 additive catalog scenarios: disjoint creates, mixed delete + create.

---

## Phase 4 — Vector-aware merge (implemented)

**Goal:** real vector merge with HNSW consistency.

### Step 4.1: Vector merge model

Vector state decomposes into:

- **Vector payload** — the embedding + metadata, stored in KV.
- **Collection config** — stored in KV.
- **HNSW graph** — per-collection derived state, stored in segmented HNSW backend.
- **Segment manifest** — on-disk state.

Merge decisions per category:

| Category | Rule |
|---|---|
| Disjoint vector adds on different collections | Apply, rebuild affected collections |
| Disjoint vector adds on the same collection | Apply, rebuild that collection |
| Same vector key written on both sides with different embeddings | LWW or Strict (per `MergeStrategy`) |
| Collection creation/deletion conflict | Strict rejection |
| Dimension / metric / storage_dtype mismatch | Always rejected (never automatically mergeable) |

### Step 4.2: Plan construction

`VectorMergeHandler::plan` (in `crates/engine/src/branch_ops/primitive_merge.rs`):
- Runs the trait's default 14-case classifier — vectors are KV-shaped at the cell level, so generic classification produces correct puts and deletes.
- Walks each `(space, TypeTag::Vector)` cell's source / target / ancestor slices and records the `(space, collection)` pair for every user_key it sees, populating `self.affected: Mutex<BTreeSet<(String, String)>>`.
- Both vector data keys (`{collection}/{vector_key}`) and config keys (`__config__/{collection}`) are extracted, so collection-only changes (creation, deletion, metadata-only updates) also trigger a rebuild.
- Each merge gets a fresh `VectorMergeHandler` instance via `build_merge_registry()`, so the mutex is uncontended.

### Step 4.3: Precheck — config mismatch refusal

`VectorMergeHandler::precheck` dispatches to a vector-crate callback registered via `register_vector_merge` (mirroring the graph crate's callback pattern, since `vector → engine` rules out engine calling vector code directly). The callback (in `crates/vector/src/merge_handler.rs::vector_precheck_fn`):

- For every space that exists on both source and target, scans `__config__/` and decodes `CollectionRecord` for each collection.
- For each collection that appears on both sides, compares `dimension`, `metric`, and `storage_dtype`.
- Any mismatch returns `StrataError::invalid_input` with a user-facing message — the merge aborts before any writes happen, regardless of `MergeStrategy`. Combining vectors of different dimensions into one HNSW would corrupt the index; switching metrics post-hoc would silently change search semantics for already-indexed vectors.

### Step 4.4: HNSW rebuild

`VectorMergeHandler::post_commit` dispatches to `vector_post_commit_fn`, which iterates the `affected` set and calls `VectorStore::rebuild_collection_after_merge(target, Some(source), space, collection_name)` per pair. This is a public per-collection helper extracted from the existing `post_merge_reload_vectors_from`:

- Re-read `__config__/{name}` from KV (drops the in-memory backend if the row is gone — handles wholesale collection deletion).
- Build a fresh backend via `IndexBackendFactory::from_type(record.backend_type())`.
- Scan all `{collection}/` vector entries; resolve each embedding (KV record, source-branch backend for legacy lite records, or old target backend), allocate a fresh `VectorId` from the new backend's counter, and queue any KV writes for ID remapping.
- Write the remapped IDs back to KV in one transaction. If the write fails, restore the old backend and surface the error — never install a new backend with mismatched IDs.
- Call `rebuild_index()` and atomically swap the backend in `VectorBackendState`.

The previous `VectorRefreshHook::post_merge_reload` (which scanned only the `"default"` namespace and rebuilt every collection in the branch) is now a no-op — the handler owns the merge path. `apply_refresh` (follower path) and `freeze_to_disk` (shutdown path) are unchanged.

### Step 4.5: Vector merge tests

`tests/integration/branching.rs`:

- `vector_merge_disjoint_collections` — source writes to "alpha", target writes to "beta", merge → both visible and searchable.
- `vector_merge_same_collection_disjoint_ids` — both branches write disjoint keys to the same collection, merge → all four searchable in target.
- `vector_merge_conflicting_id_lww` — both sides write the same key with different embeddings → LWW takes source.
- `vector_merge_dimension_mismatch_rejected` — different dimensions on the same collection name → merge errors regardless of strategy; target's data unchanged.
- `vector_merge_metric_mismatch_rejected` — different metrics → merge errors with metric-specific message.
- `vector_merge_hnsw_search_correct_after_merge` — disjoint adds on a single collection; post-merge k-NN search returns the union of both sides with correct ranking.
- `vector_merge_does_not_touch_unaffected_collections` — regression: untouched collection's `VectorId` is stable across merge (proves the per-collection rebuild scoping).

### Phase 4 acceptance bar

- ✅ Vector merge is no longer blocked by Phase 2's placeholder no-op.
- ✅ Merged collections return correct k-NN results without a full backend reload.
- ✅ HNSW rebuild is per-collection, not global.
- ✅ Dimension / metric / dtype mismatches are caught in precheck and refused regardless of strategy.

---

## Phase 5 — JSON path-level merge (implemented)

**Status:** **Implemented.** Replaces Phase 2's `JsonMergeHandler` no-op with a real per-document path-level three-way merge plus index refresh in `post_commit`.

**Goal:** merge disjoint path edits on the same JSON document instead of discarding one side via LWW.

### Step 5.1: Document-level three-way diff (shipped)

`crates/engine/src/branch_ops/json_merge.rs` provides `merge_json_values(ancestor, source, target, strategy)` — a recursive object-level three-way merge over `JsonValue`s. The helper has no dependencies on engine internals, so it's unit-tested in isolation (12 tests covering disjoint top-level keys, nested paths, same-path-same-value convergence, same-path-different-values LWW, key-add, key-delete, delete-vs-edit, opaque arrays, no-ancestor-both-add, deep-conflict path reporting). Arrays are intentionally treated as opaque values — element-level array merging needs LCS / OT algorithms that are out of scope for the first JSON cut.

### Step 5.2: Path-level conflict rules (shipped)

The recursion in `json_merge.rs::merge_recursive` implements the four rules verbatim:

- Both sides edited disjoint paths → merge both edits.
- Both sides edited the same path to the same value → no conflict.
- Both sides edited the same path to different values → path-level conflict (LWW or Strict).
- One side deleted a subtree the other edited → opaque divergence at the parent path → path-level conflict.

Conflict paths are dot-joined strings (`user.profile.name`) so a single doc surfaces one `ConflictEntry` per conflicting subtree, not just one for the whole doc.

### Step 5.3: Plan construction (shipped)

`JsonMergeHandler::plan` in `crates/engine/src/branch_ops/primitive_merge.rs` overrides the trait default. It walks `(space, TypeTag::Json)` cells, deserializes `JsonDoc`s on each side, and dispatches to a per-doc decision (`merge_one_doc`) covering:

- Both sides identical → no action.
- Single-sided (source unchanged from ancestor or target unchanged from ancestor) → fast path that skips `merge_json_values`.
- True three-way divergence → `merge_json_values` produces the merged value, which is re-enveloped into a fresh `JsonDoc` (`version = max(src, tgt) + 1`, `created_at = min(src, tgt)`, `updated_at = max(src, tgt)`) and re-serialized via `JsonStore::serialize_doc`.
- Delete/modify and modify/delete cases produce structured `ConflictEntry`s and resolve source-wins under LWW.
- Bytes that fail `JsonStore::deserialize_doc` (corrupt rows) fall through to a 14-case-equivalent opaque-bytes path so a single bad row never poisons the whole merge.

Each merge produces a fresh handler instance via `build_merge_registry()`, so the `Mutex<Vec<JsonAffectedDoc>>` that carries state from `plan` to `post_commit` is uncontended in practice.

### Step 5.4: JSON index refresh (shipped)

`JsonMergeHandler::post_commit` drains the affected list and, per space:

1. Loads index definitions via `JsonStore::load_indexes` and replays per-doc deltas through `JsonStore::update_index_entries(old_target_value, new_value, indexes)`. Same delta logic as the per-write path; secondary indexes for affected docs become consistent with the merged document state.
2. Refreshes the BM25 `InvertedIndex` extension via `JsonStore::index_json_doc` (for puts) or `deindex_json_doc` (for deletes). Calls are no-ops when BM25 is disabled.

`load_indexes`, `update_index_entries`, `index_json_doc`, and `deindex_json_doc` were promoted from `fn` to `pub(crate) fn` so the merge handler can reuse the canonical helpers instead of rolling its own. Failures inside `post_commit` are logged and swallowed (mirroring `VectorMergeHandler`) so a refresh failure never rolls back a successful KV merge — the next compaction / db open falls back on the existing recovery path.

### Known limitation: cross-merge index def propagation

`_idx_meta/{space}` and `_idx/{space}/{idx_name}` are written through `Key::new_json` directly without registering with `SpaceIndex`, so they don't appear in `merge_branches`'s gathered cells. As a result, **a new index created on source after fork will not propagate to target via merge** — `post_commit` will refresh entries for the indexes that already exist on target only. Documents touched by the merge end up correctly indexed against pre-existing target indexes; collecting the new source-only index defs is a follow-up that can be tracked under "Phase 5b" if it becomes a real user need. Pre-Phase-5 fork operations already inherit `_idx_*` data via COW, so the typical flow (create indexes on parent, fork, write child) is fully covered.

### Step 5.5: JSON merge tests (shipped)

Six integration tests in `tests/integration/branching.rs::Phase 5: JSON path-level merge`:

- `json_merge_disjoint_paths_auto_merges` — disjoint top-level key edits combine without conflict under Strict.
- `json_merge_nested_disjoint_paths_auto_merge` — disjoint nested-object edits exercise the recursive object-walk path.
- `json_merge_same_path_same_value_no_conflict` — both sides converge to the same value, Strict succeeds.
- `json_merge_same_path_different_values_lww` — same-path different-values: Strict refuses, LWW resolves source-wins.
- `json_merge_subtree_delete_vs_edit_conflict` — delete vs edit on same path: Strict refuses, LWW source-wins drops the deleted key.
- `json_merge_secondary_index_refreshed_post_commit` — `post_commit` refreshes a numeric `IndexDef` so a query for the new value resolves to the merged doc and a query for the pre-merge value returns no hits.

Plus 12 unit tests inside `crates/engine/src/branch_ops/json_merge.rs::tests` exercising the helper directly.

### Phase 5 acceptance bar (met)

- ✅ JSON merges auto-merge disjoint path edits without user intervention.
- ✅ Same-path different-values are reported as conflicts; Strict rejects, LWW resolves source-wins inside the merged doc.
- ✅ Secondary index state (`_idx/{space}/{name}`) is consistent with merged document state for indexes that exist on target.
- ✅ BM25 `InvertedIndex` is refreshed for affected documents.
- ✅ All Phase 1–4 regression tests still pass without modification.

---

## Phase 6 — Graph space symmetry

**Status:** implemented.

**Goal:** remove the hard-wired `_graph_` reservation so Graph is space-symmetric with the other primitives.

### What shipped

1. **`crates/graph/src/keys.rs`** — `pub const GRAPH_SPACE: &str = "_graph_";` is kept but its meaning narrowed: it's now the well-known space name where the system branch DAG (`branch_dag.rs`) lives, NOT the default for user graph CRUD. The three core helpers (`graph_namespace`, `graph_key`, `storage_key`) gained a `space: &str` parameter and are threaded through every callsite. `NS_CACHE` is rekeyed by `(BranchId, String)` so different spaces get distinct cached namespaces.

2. **`crates/graph/src/ext.rs`** — every method on the `GraphStoreExt` trait gained `space: &str` after `branch_id`. `ensure_graph_space_registered` accepts the target space and registers it with the SpaceIndex (so user-space graphs become visible to `merge_branches`). All internal `keys::storage_key(...)` calls thread `space` through.

3. **`crates/graph/src/{nodes, edges, lifecycle, bulk, ontology, snapshot, traversal, integrity, analytics, boost}.rs`** — every public CRUD method on `GraphStore` gained `space: &str` after `branch_id`. ~25 methods updated.

4. **`crates/graph/src/branch_dag.rs`** — pinned to `keys::GRAPH_SPACE` explicitly. The system DAG keeps living in `_graph_`, isolated from user-space graphs.

5. **`crates/executor/src/command.rs`** — every Graph* command variant gained `#[serde(default, skip_serializing_if = "Option::is_none")] space: Option<String>` matching the existing KV/JSON/Vector pattern exactly. Default-skip serde keeps backwards compatibility with persisted commands.

6. **`crates/executor/src/api/graph.rs`** — every `Strata::graph_*` method now passes `space: self.space_id()` when constructing the command, so graph operations honor `Strata::current_space` (default `"default"`) just like `kv_put` / `json_set` / `vector_upsert`. Method signatures didn't change — existing user code keeps compiling.

7. **`crates/executor/src/handlers/graph.rs`** — every Graph handler accepts `space: String` as its second argument and threads it into the GraphStore call.

8. **`crates/executor/src/executor.rs`** — every Graph dispatch arm destructures `space` from the command, resolves `let space = space.unwrap_or_else(|| "default".to_string());`, and passes it to the handler.

9. **`crates/executor/src/session.rs`** — `dispatch_in_txn` takes `space: &str` and threads it into `ctx.graph_*` calls. `PostCommitOp::GraphIndexNode` / `GraphDeindexNode` carry the space through to post-commit search index updates. The `extract space from command` match arm covers all 32 Graph* variants.

10. **Search integration (`lib.rs`)** — `index_node_for_search` and `deindex_node_for_search` accept `space: &str` for forward-compat. `EntityRef::Graph` does NOT yet carry the space — that's a known cross-primitive limitation (KV / JSON / Vector all have the same issue with same-key cross-space collisions in the search index) and is tracked separately.

### Behavior change for users

After Phase 6, `db.graph_create("g")` writes to `current_space/g` (default `"default"/g`), NOT `_graph_/g`. Graph operations honor `Strata::set_space(...)` exactly like KV / JSON / Vector / Event do.

`_graph_` is no longer reachable through the user API: space-name validation rejects names starting with `_`, so `db.set_space("_graph_")` errors out. The constant `keys::GRAPH_SPACE = "_graph_"` is now used **only** by `branch_dag.rs` to keep the system DAG isolated from user spaces — it's no longer a user-facing default for anything.

Pre-Phase-6 databases that have user data in `_graph_` will need to be migrated outside the public API (or treated as new). This is acceptable because Phase 6 is part of the Claim 4 sequence that hasn't shipped a stable user-facing release yet.

The lower-level `GraphStore::*` API takes `space: &str` as a required parameter — no defaults — so test fixtures and `branch_dag.rs` are explicit about which space they target.

### Phase 6 acceptance bar (met)

- ✅ Graph data can live in any user space (`graph_in_user_space_create_and_query_succeeds`, GraphStore API).
- ✅ Two graphs with the same name in different spaces are independent (`graph_in_two_spaces_independent`, GraphStore API).
- ✅ Fork inherits user-space graph data (`graph_in_user_space_survives_fork`).
- ✅ Phase 3b semantic merge applies to user-space graphs (`graph_in_user_space_phase_3b_semantic_merge_succeeds`).
- ✅ Phase 3b referential-integrity rejection applies to user-space graphs (`graph_in_user_space_referential_integrity_rejects_dangling`).
- ✅ The lower-level `GraphStore::*` API can target the reserved `_graph_` system DAG space that `branch_dag.rs` uses (`graph_store_accepts_reserved_system_dag_space`).
- ✅ `Strata::graph_*` honors `current_space` end-to-end (`graph_honors_current_space_via_strata_api` in `crates/executor/src/tests/spaces.rs`).
- ✅ Three independent user spaces don't bleed into each other (`graph_three_user_spaces_independent_via_strata_api`).
- ✅ Claim 4's "Graph is not space-symmetric" caveat removed from the architectural-claims doc.

### Tests

- 6 new integration tests in `tests/integration/branching.rs::Phase 6: Graph space symmetry` (GraphStore-direct API).
- 2 new Strata-API end-to-end tests in `crates/executor/src/tests/spaces.rs` (verifies `set_space` + `graph_*` auto-fill).
- All 466 graph crate unit tests pass with mechanical signature updates (added `"default"` argument at every call site).
- All Phase 1–3c regression tests pass (the existing tests use `"default"` space, which is what `current_space` resolves to).
- Arrow-feature build verified: `cargo check -p strata-executor --features arrow` clean.

### Out of scope

- **`EntityRef::Graph` carrying space**: same cross-primitive limitation as KV/JSON/Vector. Adding a `space` field to `EntityRef` is a breaking change to the search index format and ripples through the recovery path. Tracked separately.
- **Multi-space graph aggregation in `describe`**: `describe` summarizes the `"default"` space's graphs. Multi-space describe — aggregating graphs across every space with data — is a separate concern.
- **strata-python SDK API parity**: lives in a separate repo; coordination is a follow-up.

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
