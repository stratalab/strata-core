# Tranche 4 closeout — branching

Tranche 4 closes the branching correctness sprint under the narrowed B6
acceptance now reflected in
`docs/design/branching/branching-execution-plan.md`. Six epics (B1–B6)
landed on `main`; B6 contributes a unified restart/degradation history
lane, benchmark smoke, runbooks, and closeout artifacts, while
follower/race proofs remain in dedicated suites and two operator-facing
surface expansions remain deliberately deferred.

This document is the tranche-4 acceptance record: what shipped, what
each invariant-class reads like post-closeout, where the regression
gates live, and what tranche 4 **deliberately did not ship**.

## 1. Eight-bar evidence map

The execution plan called out eight structural bars that tranche 4
must clear before closeout. Each bar cites the landing PR and the
regression gate that now pins it.

| # | Bar | Landed in | Regression gate |
|---|---|---|---|
| 1 | Canonical `BranchId` derivation + generation monotonicity | B2 (#2399–#2403) | `tests/integration/branching_generation_migration.rs`, `branching_control_store_recovery.rs` |
| 2 | `BranchService` as the one canonical mutation surface; lifecycle gate over every mutation path | B1 (#2377–#2396), B4 (#2440–#2458) | `branching_lifecycle_gate.rs`, `branching_lifecycle_restart.rs`, `branching_guardrails.rs` |
| 3 | Same-name recreate lifecycle isolation (attribution, storage, DAG) | B3 (#2411–#2437) | `branching_recreate_state_machine.rs`, `branching_same_name_race.rs` |
| 4 | Merge lineage correctness (merge base + edges, capability-gated merge) | B3.3 (#2429) | `branching_merge_lineage_edges.rs` |
| 5 | Generation-aware retention attribution via `RetentionReport` | B5.1–B5.2 (#2456–#2459) | `branching_retention_matrix.rs`, `branching_retention_state_machine.rs` |
| 6 | Convergence closure + differential corpus | B5.3 (#2460) | `branching_convergence_differential.rs` |
| 7 | Fail-closed degraded primitives (vector config mismatch, JSON `_idx` load) | B5.4 (#2461) | `branching_degraded_primitive_paths.rs` |
| 8 | Narrowed B6 closeout: unified restart/degradation history gate + benchmark smoke + runbooks | B6 (this PR) | `branching_adversarial_history.rs`, dedicated follower/race suites, `benchmarks/baselines/b6-baseline.json`, `docs/design/branching/runbooks/` |

## 2. Invariant re-audit

The branching COW and ACID invariants from
`docs/design/branching/branching-contract.md` are pinned by regression
coverage after tranche 4. This section walks each one.

### COW invariants

- **COW-001 — Fork captures source frontier atomically.** Pinned by
  the fork-frontier preservation postcondition in
  `branching_adversarial_history.rs` (postcondition 3) and the
  B3 recreate state machine.
- **COW-002 — Descendant reads clamp to `fork_version`.** Pinned by
  `branching_retention_matrix.rs` (`retention_report_attributes_bytes_to_live_branch_refs`,
  `retention_report_recreate_does_not_fold_old_descendant_bytes_into_new_main`).
- **COW-003 — Parent delete preserves descendant visibility.** Pinned
  by `branching_retention_matrix.rs::delete_after_fork_retains_parent_segments_for_child`
  and the orphan-storage postcondition in the adversarial suite.
- **COW-004 — Materialize copies bytes into descendant without
  changing visible results.** Pinned by
  `branching_retention_matrix.rs::materialize_before_reopen_preserves_visible_result_set`
  and `materialize_after_reopen_preserves_visible_result_set`.
- **COW-005 — GC never reclaims a manifest-reachable segment.**
  Pinned by the retention state machine + quarantine recovery suite.
- **COW-006 — Same-name recreate never aliases retention by name
  alone.** Pinned by `retention_report_recreate_does_not_fold_old_descendant_bytes_into_new_main`
  and the B6 adversarial suite's postcondition 5.

### ACID invariants (branching-scoped)

- **ACID-005 — Branch create/delete are atomic against concurrent
  writes.** Pinned by `branching_same_name_race.rs` (multiple
  scenarios) and the B4 lifecycle gate.
- **ACID-006 — Branch mutations serialize per name via the
  branch-commit lock.** Pinned by `branching_same_name_race.rs`
  (`scenario_double_fork_to_same_destination_yields_single_winner`).
- **ACID-007 — Rollback never completes post-commit cleanup.** Pinned
  by the B5 state-machine contract and the `branch_mutation.rs`
  rollback path (`complete_delete_post_commit` is reused but the
  recording side-effect is at the committed-delete call site only).
- **ACID-008 — Reopen preserves visible history.** Pinned by
  `branching_lifecycle_restart.rs` and the adversarial suite's Reopen
  op.
- **ACID-009 — Merge is a pure-function derivation of state; refuses
  on divergent concurrent history.** Pinned by
  `branching_merge_lineage_edges.rs`.
- **ACID-010 — Follower refresh is structurally contiguous.** Pinned
  by the follower refresh + contiguous watermark tests.
- **ACID-011 — Degraded primitives fail closed; attribution is
  generation-aware.** Pinned by `branching_degraded_primitive_paths.rs`
  and the adversarial suite's postcondition 6.

## 3. Benchmark delta

B6 adds one measurement pair to the regression harness. Baseline
captured at commit `6b94483c` on `feat/B6-tranche-closeout`:

| Metric | Value (µs) | Threshold |
|---|---|---|
| `materialize_p50_us` | ~76 000 | 5% regression (p50 pattern, auto-gated) |
| `materialize_p95_us` | ~86 000 | visibility-only (not gated) |
| `materialize_p99_us` | ~105 000 | 10% regression (p99 pattern, auto-gated) |

The 5 pre-existing branch metrics (`create_p*`, `fork_p*`, `diff_p*`,
`delete_p*`, `merge_small_p*`, `merge_large_us`) stay gated at S4
thresholds (5% median, 10% tail). This smoke harness ends at `reopen`;
follower catch-up remains covered by dedicated follower tests, not the
benchmark binary. Baseline file:
`benchmarks/baselines/b6-baseline.json`.

Run future regression comparisons via:

```
cargo run --release -p strata-benchmarks --bin regression -- \
    --baseline-name b6-baseline.json --tranche 4 --epic B6
```

## 4. Sign-off

- **Change class:** closeout is additive (one test file, one benchmark
  measurement block, runbook docs, closeout doc, matrix entries,
  CLAUDE.md note). No Engine public API surface changes. No storage
  layout changes. No wire format changes.
- **Assurance class:** S4. Regression gates above cover the
  invariants. Benchmark baseline captured. Adversarial history suite
  runs 16 cases × 1..20 ops every `cargo test -p stratadb --test
  integration`; follower and race coverage remain in their dedicated
  suites and are part of the closeout evidence rather than the
  generated-history alphabet.
- **D4 surface:** unchanged from post-B5.4 shape. No new public
  types. No new `#[non_exhaustive]` enums. No field additions to
  existing public structs.

## 5. Deferred items under narrowed B6 acceptance

Two items originally scoped for B6 were explicitly dropped during
planning after a review round flagged the additions as abstraction
accretion. They are recorded here with re-open criteria so a future
PR has a landing place.

### 5.1 Typed `StrataError::BranchLineageUnavailable` variant

**Status:** deferred.

**Current behavior:** lineage refusals surface as
`StrataError::InvalidOperation` with a reason-prefix string starting
with `branch_lineage_unavailable:`. The stable detection surface is
`StrataError::is_branch_lineage_unavailable()`
(`crates/core/src/error.rs`), which callers use without touching the
prefix directly.

**Why deferred:** promoting this to a dedicated variant plus a
`LineageUnavailableReason` sub-enum adds two new types and a
`#[non_exhaustive]` enum to `strata_core`. The predicate already
isolates callers from the wire shape, so the net improvement is
cosmetic. Runbook
`docs/design/branching/runbooks/blocked-follower-refresh.md` explains
how operators detect the condition today.

**Re-open criteria:** re-open when any of the following is true:
1. A third caller outside the `branch_control_store` shows up that
   cannot use the predicate (e.g. an external wire consumer that
   needs typed match exhaustiveness).
2. The `InvalidOperation` reason-prefix is destabilized by another
   change (e.g. taking on additional prefix forms that conflict).
3. A follow-up epic adds typed sub-reasons that differ in
   operator-routing semantics (not just detection).

### 5.2 Programmatic cleanup-debt pull surface

**Status:** deferred.

**Current behavior:** advisory and post-commit failures from
`complete_delete_post_commit` surface as `tracing::warn!` lines at
`crates/engine/src/database/branch_service.rs:552-566` and
`crates/storage/src/segmented/mod.rs:2042`. Operators see them in
logs.

**Why deferred:** the draft plan built a `CleanupDebtRegistry` +
`CleanupDebtEntry` + `CleanupDebtKind` + `DebtId` +
`RetentionReport::cleanup_debt` + free-function acknowledge API. Net
cost: one new module, three new public types, one new public enum, a
field addition to `RetentionReport`. No operator is currently asking
for programmatic access; the logs suffice. Adding the surface now
would expand the D4 surface and paint us into a corner on
`Arc<StrataError>` wrapping (required because `StrataError` is not
`Clone`).

**Re-open criteria:** re-open when any of the following is true:
1. A concrete operator or downstream tool requests programmatic
   routing of cleanup debt (e.g. an ops dashboard, a support
   procedure that needs to enumerate outstanding debt without log
   parsing).
2. A scope amendment moves materialize-induced debt into the same
   observability contract — at that point both delete and
   materialize debt share a surface and the design cost amortizes.
3. The convergence-and-observability doc adds cleanup debt to
   §"Required push events" as non-deferrable, which would require
   the D4 amendment anyway.

### 5.3 Materialize-induced retention debt

**Status:** deferred (out-of-scope for B6 from the outset).

**Current behavior:** storage-level materialize debt surfaces as a
`tracing::warn!` at `crates/storage/src/segmented/mod.rs:2042`.

**Why deferred:** plumbing this into a programmatic surface requires
a breaking change to `storage::MaterializeResult` at
`crates/storage/src/segmented/mod.rs:190` (public struct, not
`#[non_exhaustive]`, re-exported from `crates/storage/src/lib.rs:48`).
A breaking storage API change in a closeout PR was ruled out.

**Re-open criteria:** re-open when the cleanup-debt programmatic
surface (5.2) is un-deferred, so both breaking changes can share one
PR with proper D4 amendment.

## 6. Next steps

Tranche 4 closes the branching sprint. The repository returns to the
normal cleanup-program cadence. Items reserved for future tranches
remain in `docs/design/branching/branching-execution-plan.md`
appendices. No branching-specific epic is active after this tranche.
