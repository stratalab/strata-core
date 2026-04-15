# Strata-Core: Claude Code Instructions

## Project Overview

Strata is an **embedded** database (like SQLite/DuckDB, not a server). It is a Rust workspace
with 13 crates in a strict dependency DAG:

```
core → storage → concurrency → durability → engine
engine → graph, vector, search, intelligence, inference
engine → executor → cli
security (leaf, used by engine)
```

The `engine` crate is the authority layer. The `executor` crate is a thin transport/session
adapter — it must never own semantics.

## Active Program: Architecture + Quality Cleanup

This repository is executing a bounded cleanup program defined by 10 scope documents,
2 guardrail documents, and 1 execution plan framework. **Every conversation must respect
the constraints below.**

### Program Corpus (normative — do not contradict)

Architecture scopes:
- `docs/design/architecture-cleanup/runtime-consolidation-scope.md`
- `docs/design/architecture-cleanup/durability-recovery-scope.md`
- `docs/design/architecture-cleanup/branch-primitives-scope.md`
- `docs/design/architecture-cleanup/product-control-surface-scope.md`
- `docs/design/architecture-cleanup/search-intelligence-inference-scope.md`

Quality scopes:
- `docs/design/quality-cleanup/workspace-foundation-scope.md`
- `docs/design/quality-cleanup/error-taxonomy-scope.md`
- `docs/design/quality-cleanup/type-safety-scope.md`
- `docs/design/quality-cleanup/visibility-and-module-structure-scope.md`
- `docs/design/quality-cleanup/test-coverage-scope.md`

Guardrails:
- `docs/design/architecture-cleanup/non-regression-requirements.md`
- `docs/design/architecture-cleanup/implementation-quality-guidelines.md`

Planning:
- `docs/design/architecture-cleanup/master-execution-plan-framework.md`
- `docs/design/post-cleanup/post-cleanup-closeout-framework.md`

---

## Active Tranche

> **UPDATE THIS SECTION when starting a new tranche via `/tranche-setup`.**

```
tranche: 3
name: Durability and Lifecycle
change_class: cutover
assurance_class: S4
benchmark_required: true
```

---

## Hard Architectural Rules

These are locked decisions from the scope documents. Violating any of these requires a
formal scope amendment to the owning document — not just a code change.

### Authority Rules

1. **Engine owns all semantics.** Executor is a thin adapter (transport, session, typed
   wrappers). If you're adding business logic to executor, stop — it belongs in engine.
2. **One canonical path per operation.** No two public surfaces may expose the same
   behavior. If a second path exists, delete it or mark it explicitly transitional.
3. **No process-global semantic state.** Use per-Database registries, not global merge
   handlers, DAG hooks, or first-caller-wins patterns.
4. **Engine never depends on intelligence or inference.** Engine owns adapter traits
   (`QueryEmbedder`, `QueryExpander`, `ResultReranker`, `RagGenerator`);
   `IntelligenceSubsystem` installs implementations via per-Database slots.

### Runtime Rules

5. **One `OpenSpec` with mode constructors** (`primary`, `follower`, `cache`). No
   competing open ladders.
6. **`BranchService` via `db.branches()` is the one canonical branch path.**
7. **`RetrievalService` via `db.search()` is the one canonical search path.**
8. **Product-default subsystem composition:**
   `[CorePrimitivesSubsystem, GraphSubsystem, VectorSubsystem, SearchSubsystem, IntelligenceSubsystem]`
   — 5 subsystems. `IntelligenceSubsystem` always present; per-adapter feature gating.

### Durability Rules

9. **WAL writer halts on fsync failure.** Recovery via explicit `Database::resume_wal_writer()`.
10. **Follower refresh is structurally contiguous** via `ContiguousWatermark`. No skips.
11. **Shutdown is an authoritative ordered close barrier** — `Database::shutdown()`.
12. **Codec uniform across WAL, snapshots, MANIFEST.** One `CompatibilitySignature`.

### Branch & Primitive Rules

13. **One canonical `BranchId`**, derivation in one place (engine-owned).
14. **Branch generations are monotonic**, scoped per name, on `BranchMetadata`.
15. **Every primitive declares lifecycle** via `PrimitiveLifecycleContract` + `ModeBehavior`.
16. **Event branch merge is strict refusal** on divergent concurrent history.
17. **Graph merge is capability-gated**, no silent fallback.

### Product & Control Surface Rules

18. **One `ProductOpenPlan`** describing every supported open path.
19. **One code-owned `ControlRegistry`** for every public control knob.
20. **Delete decorative/unsupported knobs** — no documentation-only rehabilitation.
21. **Secret policy: plaintext with 0o600 on write AND read.**
22. **IPC trust: same-user via `SO_PEERCRED`.**
23. **Provider/config validated at open time** (fail-fast, not fail-late).

### Quality Rules

24. **`[workspace.lints]` is single source of truth** for lint config.
25. **All public error enums get `#[non_exhaustive]`.**
26. **Typed reason enums replace string-factory error methods.**
27. **Newtypes use `#[repr(transparent)]` + `#[serde(transparent)]`** for wire stability.
28. **`pub(crate)` by default**; `pub` only for items on the D4 public API surface.
29. **`unreachable_pub` escalates to deny** after visibility tightening.
30. **`#![deny(unsafe_code)]` on safe crates:** core, graph, search, intelligence, inference.

---

## Engine Public API Surface (D4)

Adding a new public type to `strata_engine` requires a cross-scope amendment to ALL FIVE
architecture scope documents. The current allowed surface:

**Runtime:** `Database`, `DatabaseBuilder`, `DatabaseMode`, `OpenSpec`, `Transaction`,
`Subsystem` trait, `CommitObserver`/`CommitInfo`, `ReplayObserver`/`ReplayInfo`,
`BranchOpObserver`/`BranchOpEvent`, `ObserverError`/`ObserverErrorKind`

**Branch:** `BranchService` (full API), `BranchDagHook`, `BranchDagError`, `BranchId`,
`BranchRef`, `BranchLifecycleStatus`, `EventStreamLifecycleStatus`, `ConflictPolicy`,
`BranchMutationContract`, `PrimitiveLifecycleContract`, `ModeBehavior`

**Durability:** `WalWriterHealth`, `FollowerStatus`, `ContiguousWatermark`,
`RefreshOutcome`, `BlockedTxn`, `BlockReason`, `DatabaseLayout`, `RefreshHookError`,
`AdvanceError`, `UnblockError`

**Product:** `SystemBranchCapability` (`pub` type, `pub(crate)` constructor), `Provider`,
`ControlRegistry`, `ControlEntry`, `ControlOwner`, `BehaviorClass`, `PersistenceClass`

**Search/Retrieval:** `RetrievalService`, `SearchRequest`, `SearchResponse`,
`DiffSearchRequest`, `DiffSearchResponse`, `ResolvedRecipe`, `RecipeInfo`,
`RetrievalCapabilities`, `QueryEmbedder`, `QueryExpander`, `ResultReranker`,
`RagGenerator`, `AutoEmbedPipeline`, `AutoEmbedConfig`

**Errors:** `StrataError`, `executor::Error`, `ErrorSeverity`, `BranchDagError`/`Kind`,
`ObserverError`/`Kind`, `RefreshHookError`, `AdvanceError`, `UnblockError`,
`InferenceError`, `BranchBundleError`, `CompactionError`, `ManifestError`

---

## Non-Regression Protocol

### Change Classes

- **Refactor-only:** Code movement, wrapper deletion, visibility changes. NO behavior,
  storage format, or public semantic changes. Benchmark must show no regression.
- **Cutover:** Old path deleted only after parity tests + parity benchmarks exist and
  new path is exercised by acceptance suite.
- **Intentional semantic change:** Only with scope authorization. Old/new behavior stated.
  Acceptance suite updated. Baseline benchmarks updated. Explicit reviewer approval.

### Assurance Classes

- **S4 (highest):** Runtime open/shutdown, recovery/replay, WAL/manifest/checkpoint, commit
  ordering, branch identity/merge/lifecycle, access control, destructive admin. Requires
  characterization tests, failure tests, crash/restart tests, benchmarks, second reviewer.
- **S3:** Vector/BM25 lifecycle, search, recipe execution, auto-embed, inference, graph
  integrity, follower behavior. Requires differential tests, negative tests, benchmarks.
- **S2:** Visibility, module decomposition, wrapper reduction, CI/lint. Requires proof
  tests still pass, benchmark if on measured path.

### Benchmark Obligations

Three benchmark suites run after every epic that touches storage/engine/runtime code:

| Suite | Dataset | Thresholds |
|-------|---------|-----------|
| redb | 5M records | throughput: max 5% regression, median latency: max 5%, tail: max 10% |
| ycsb_compare | 100K records | throughput: max 5% regression, median latency: max 5%, tail: max 10% |
| beir | 4 small sets | nDCG@10: max 0.01 absolute drop, ANN recall: max 0.5pt drop |

**Characterization-before-refactor rule:** Before refactoring S4/S3 subsystems, add
characterization tests for existing behavior. Do not refactor high-risk paths understood
only informally.

---

## PR and Code Discipline

### Every PR Must

1. Have one obvious owner for every changed behavior
2. Delete or mark-transitional the old competing path (if any)
3. Narrow (not widen) the public API surface
4. Reduce (not redistribute) complexity
5. Include integrated tests for the changed behavior
6. State its change class and assurance class

### Never Do

- Add business logic to executor (it routes, never decides)
- Add a second public way to do the same operation
- Keep both old and new implementations alive without a cutover boundary
- Mix unrelated changes in one PR
- Add process-global semantic state
- Suppress errors without a rationale comment (`let _ =`, `.ok()`, `.unwrap_or_default()`)
- Add `pub` to items not on the D4 surface without a scope amendment
- Skip benchmarks for PRs touching storage/engine/runtime

### Prefer

- Authority clarity over flexibility
- Shorter canonical paths over more options
- Deletion of obsolete code over documentation of it
- Moving semantics into engine over wrapping in executor
- Explicit enums over boolean-control APIs
- `pub(crate)` over `pub` by default
- Integrated behavioral tests over unit tests of internal helpers

---

## Workspace Commands

```bash
# Build
cargo build                      # debug build
cargo build --release            # release build

# Test
cargo test                       # all tests
cargo test -p strata_engine      # single crate
cargo test --test integration    # integration tests only

# Lint
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check

# Feature matrix (when cargo-hack is installed)
cargo hack check --workspace --feature-powerset --depth 2

# Regression benchmarks
cargo run --release -p strata-benchmarks --bin regression -- --quick          # quick (~1s)
cargo run --release -p strata-benchmarks --bin regression                     # full (redb 5M, ycsb 100K, beir 4 datasets)
cargo run --release -p strata-benchmarks --bin regression -- --capture-baseline  # save baseline
cargo run --release -p strata-benchmarks --bin regression -- --tranche 2 --epic "RC-1"  # with metadata

# Full comparison benchmarks (vs rocksdb, redb, sqlite, etc.)
cargo bench -p strata-benchmarks --bench redb_benchmark
cargo bench -p strata-benchmarks --bench ycsb_compare
```

---

## Crate Dependency Rules

The DAG is strict and CI-enforced. Never introduce:
- engine → intelligence (engine owns traits, intelligence implements them)
- engine → inference (same pattern)
- executor → storage/concurrency/durability (executor only depends on engine)
- Any cycle in the crate graph

---

## Skills Reference

The following slash commands are available for the cleanup program:

| Skill | When to use |
|-------|------------|
| `/tranche-setup` | Start of each tranche — updates this file's Active Tranche section |
| `/epic-setup` | Start of each epic — extracts scope phases, builds task checklist |
| `/program-check` | Before commit — cross-scope invariant verification |
| `/regression-check` | After epic — runs redb/ycsb/beir benchmarks against baseline |
| `/scope-review` | Before PR — reviews diff against scope locked decisions |
| `/tranche-gate` | End of tranche — full exit-criteria check |
| `/acceptance-matrix` | Anytime — read/update acceptance-matrix.toml |
| `/review` | Anytime — general code review (already exists) |
