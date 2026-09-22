# #3521 — Background-compaction pruning dispatch: analysis & implementation plan

Status: **PLAN — awaiting sign-off to implement.** Follow-up to #3502 slice D/D2.
All file:line references verified by code trace (three read-only investigations,
2026-09-22).

## 1. Problem & scope

Opt-in `KeepRecentVersions` pruning (#3502) fires only through the
**flush-followup** foreground path (`run_compaction_maintenance_task`,
maintenance.rs:3361) and the testkit force-compaction seam. The **background**
autonomous compaction path (`start_background_compaction_task`,
maintenance.rs:3460) — the steady-state disk-reclaim path — builds its request
plain and never attaches a retention policy or proof, so it runs `KeepAll`. A
database that opts into retention therefore reclaims little under normal
operation; the measured ~67% table-byte win (slice E2) is mostly unrealized.

**Scope facts:**
- `strata compact` (the explicit user command) was **removed** in V1, so the
  explicit fixed-point drain (`compact_branch_tables_to_fixed_point`,
  maintenance.rs:652) is not product-reachable. Wiring it is a ~5-line uniformity
  addition (Q3), not the value.
- **The value is entirely in the off-lock background path** — the delicate one.

## 2. Verified mechanics

### The three-phase background compaction (all maintenance.rs unless noted)

1. **START (under lock)** `start_background_compaction_task:3460` — builds
   `request` via `current_compaction_request_from_maintenance_task_with_budget`
   (:3506), takes `&mut branch` (:3491), snapshots `branch.clone()` (:3546), and
   returns `DurableBackgroundMaintenanceBuild::Compaction { task, branch_id,
   level, request, branch_snapshot, table_object, table_reader, budget, inflight }`
   (variant def :195-205). **The request is never given a retention policy/proof.**
2. **BUILD (off-lock)** `DurableBackgroundMaintenanceBuild::build():267`, Compaction
   arm :339-361 — runs `prepare_durable_compaction_publication` on the SNAPSHOT
   (rewrite_publication.rs:113), producing `PreparedDurableCompaction`
   (rewrite_publication.rs:34-46). Proof validation
   (`proof.validate_for_branch`, branch/state/compaction.rs:637) runs only in the
   non-`KeepAll` arm — today unreached because the request is `KeepAll`.
3. **PUBLISH (under lock)** `begin_publish_phase:2112` → `begin_compaction_publish:2336`
   → `install_prepared_durable_compaction_without_publish` (rewrite_publication.rs:441)
   → `install_branch_compaction_prepared_plan` (branch/state/compaction.rs:688).
   Re-validates the compaction CANDIDATE against the live branch via
   `require_candidate_current` (compaction.rs:711); a superseded candidate →
   `is_stale_compaction_candidate` → **Deferred** (maintenance.rs:2375). The
   layout commits at compaction.rs:739; the retained-history floor publishes at
   compaction.rs:748 — **no proof re-validation exists between them.**

### Threading is structurally ready

`LifecycleCompactionRequest` already carries `retention_policy` + `pruning_proof`
(lifecycle/compaction.rs:61-62) with setters `.with_retention_policy()`
(:481) / `.with_pruning_proof()` (:493) — the same the foreground runner uses
(maintenance.rs:4836). The background starter simply never calls them.

### Hazard A — shared-table TOCTOU, NOT defended (correctness-critical)

`validate_for_branch`'s shared-table gate (`validate_shared_table_safety`,
pruning.rs:527-534) inspects **only the proof's frozen `candidate_tables_not_shared`
boolean** — it does not rebuild a live registry. A **fork of this branch that
appears during the off-lock window** re-references this branch's input tables but
changes **neither** this branch's own rows/tables (so the fingerprint is
unchanged) **nor** that frozen boolean. So the existing `validate_for_branch`
would **pass** a proof whose shared-table safety is now false, and publishing the
pruned output would rewrite tables a COW child still reads (COW-005/006
violation). The foreground path is safe only because the lock excludes forks
build→apply; off-lock has no such guarantee. **A live shared-table re-check at
publish is mandatory.**

### Hazard B — the content fingerprint is too strict off-lock

`branch_pruning_fingerprint` (pruning.rs:469-491) hashes the **active memtable
rows** (`active_row_count()` + every `branch.active().iter()` row, lines 472-475).
Any concurrent commit lands in the memtable → fingerprint changes → the
`ProofStale` gate (pruning.rs:325-329) and the `visible_version` gate (:337-342)
trip. On a branch taking writes, a full `validate_for_branch` at publish would
**defer on essentially every attempt**. The fingerprint was built for the
synchronous path (lock excludes commits). **The publish re-validation must skip
the content fingerprint + visible-version gates.**

The semantic that makes skipping them sound: **dropping below-floor versions is
invariant to concurrent above-floor commits.** A new commit lands above the
retained floor, in the memtable — it touches neither the below-floor rows being
dropped nor the input tables being rewritten. Input-table freshness is already
covered by `require_candidate_current` (compaction.rs:711); the floor rises
monotonically with `visible`, so a version dropped below the snapshot floor is
also below any later live floor — unless the floor moved *down* (only a
concurrent prune with a smaller floor, or a branch-op, can do that), which the
new floor-movement check catches.

## 3. Design

Build the proof under the lock at START (from the live branch, exactly as the
foreground path does at maintenance.rs:3408), thread `(policy, proof)` through the
off-lock phases, apply pruning in the BUILD phase against the snapshot
(fingerprint matches by construction), and **re-validate only the pruning-safety
gates against the live branch at PUBLISH, under the lock, before the layout
commit** — deferring (not failing) when stale.

### New: `revalidate_pruning_for_publish` (partial, live)

A runtime-level check in `begin_compaction_publish` (it needs sibling-branch
access the `BranchLocalState` method lacks), composed from existing pieces
(Agent 2 confirmed the smallest set):

1. **Live shared-table** (Hazard A): rebuild `SharedTableRegistry::rebuild_from_snapshots`
   over `active_branch_ids()` → `reachability_snapshot()` (as
   `branch_tables_unshared_with_other_branches`, maintenance.rs:3256 does) and
   require `derive_candidate_tables_not_shared(candidate, &registry)`
   (pruning.rs:245) — candidate-scoped, live.
2. **Floor movement** (net-new): require `live_branch.retained_history_floor()`
   (read_hooks.rs:275) not advanced past `proof.retained_version_floor()`
   (pruning.rs:130); refuse if a concurrent prune already moved the floor.
3. **Reuse** `validate_timestamp_floor(live coverage, proof floor)` (pruning.rs:494),
   `live_branch.inherited_layers().is_empty()`, and
   `self.current_recovery_health.is_healthy()`.
4. **Skip** the content fingerprint (:325) and visible-version (:337) gates
   (Hazard B).

On failure → discard the pruned output and **Deferred** (mirror the existing
stale-candidate handling, maintenance.rs:2375); the next pass re-derives a fresh
proof + candidate. On success → proceed with the existing install (which commits
the layout and publishes the floor).

**ARCH-005 preserved:** below-floor deletion still happens only under a proof
whose safety gates hold **at the moment of install, under the lock** — the
build-time validation against the snapshot is the fast path; the publish
re-validation is the authoritative gate.

### Open questions — resolved

- **Q1 (granularity):** dedicated `revalidate_pruning_for_publish` (shared-table +
  floor + coverage/inheritance/health), NOT a relaxed fingerprint. **Confirmed** —
  a "relaxed fingerprint" cannot express "ignore the memtable but catch a fork,"
  because a fork doesn't change this branch's fingerprint at all.
- **Q2 (floor movement):** defer if live floor > proof floor. **Confirmed net-new.**
- **Q3 (explicit drain):** include (~5 lines, lock-held, uniform dispatch).
- **Q4 (autonomous trigger):** ride the existing compaction cadence for V1; a
  retention-pressure trigger is a separate follow-up.

## 4. TDD implementation — sliced

Each slice is test-first (red → green), mutation-pre-empted, and gate-clean.
Estimated ~150–220 LOC non-test across slices; if it exceeds ~250, split the PR.

### Slice 1 — the partial re-validator (pure, truth-tabled) + RED wiring test
- **Test first:** an in-crate `durable.rs` test `test_background_compaction_prunes_under_retention` — seed 2 confirmed L0 tables under `with_version_retention_window(Some(1))`, drive a background compaction to completion (no interleaving), assert the retained floor advanced and a below-floor at-version read raises `RetainedHistoryUnavailable`. **RED today** (background never prunes).
- Add `revalidate_pruning_for_publish` as a runtime method (or a pruning.rs helper taking the live registry + live floor). Extract its decision core into a pure predicate with a **truth table** (shared/unshared × floor-moved/not × coverage), plus the end-to-end wiring test above as the call-site observation (mutation gate needs both).

### Slice 2 — thread `(policy, proof)` through the background phases
- Build the proof at `start_background_compaction_task` before `branch.clone()`
  (reuse `build_version_pruning_for_compaction(branch_id)`); add an
  `Option<(BranchCompactionRetentionPolicy, BranchCompactionPruningProof)>` field
  to `DurableBackgroundMaintenanceBuild::Compaction` (:195) and
  `PreparedDurableCompaction` (rewrite_publication.rs:34); apply
  `.with_retention_policy()/.with_pruning_proof()` to the request in the BUILD
  phase so pruning runs on the snapshot. Slice-1's test flips toward GREEN.

### Slice 3 — re-validate at PUBLISH + defer-on-stale (the safety slice)
- Call `revalidate_pruning_for_publish` in `begin_compaction_publish` before the
  install; on stale → Deferred (discard output). Slice-1's test now fully GREEN.
- **Concurrency tests (the core proof), in-crate, phase-sequenced** — template:
  `cache.rs:574`, `durable.rs:4219`, `durable.rs:4483`:
  - `test_background_prune_defers_when_fork_shares_tables_mid_window`: `build()`
    the compaction, then `fork_current(...)` (bootstrap.rs:1319) sharing the input
    tables, then `begin_publish_phase` → assert **Deferred**; and the fork's
    below-fork `as_of` read still resolves (COW intact). **Hazard A closed.**
  - `test_background_prune_publishes_despite_concurrent_above_floor_commit`:
    `build()`, then `execute_durable_commit(...)` (bootstrap.rs:1935) above the
    floor, then publish → assert the compaction **publishes and prunes** (NOT
    deferred), floor advanced, below-floor read raises, the new commit reads.
    **Hazard B resolved — proves we didn't just defend by deferral.**
  - `test_background_prune_defers_when_floor_moved`: interleave a concurrent prune
    that lowers the floor → assert Deferred (Q2).

### Slice 4 — explicit fixed-point drain (Q3, small)
- Attach `build_version_pruning_for_compaction` output to
  `compact_branch_tables_to_fixed_point` (lock-held, foreground — same safety as
  flush-followup, no off-lock re-validation needed). Test:
  `test_explicit_drain_prunes_under_retention`.

## 5. Test plan (summary)

- **Location constraint (Agent 3):** every phase method (`build`,
  `begin_publish_phase`, `persist_off_lock`, `finish_publish_phase`) and
  `execute_durable_commit`/`fork_current` is `pub(crate)`;
  `retained_history_floor_for_test` is `#[cfg(all(test, feature="localfs"))]`.
  **All concurrency tests MUST live in the storage crate's `#[cfg(test)]` tree
  (`crates/storage/src/lifecycle/tests/durable.rs`)** — `crates/storage/tests/`
  and the DST sim cannot reach these seams.
- Non-vacuity: assert a "background pruning completed" signal (floor advanced +
  below-floor raise) on the clean interleaving, so a no-op fix can't pass.
- Direction controls: unshared branch prunes; shared/floor-moved defers; KeepAll
  never prunes on any path.
- Standard gates: workspace suite, storage fault-injection lane 62/0,
  mutation-on-diff (0 missed; the partial validator's decision core is a pure
  truth-tabled predicate — dodges the alias/bare-enum blind spots), fmt/clippy,
  feature-powerset (localfs gating).
- The whole-db DST sim stays KeepAll-atomic (it can't model this); its E1 pruning
  lane (`pruning_enforces_the_retained_floor_on_reads_and_forks`, whole_db.rs:1223)
  is untouched.

## 6. Invariants

- **ARCH-005** — re-validation at install preserves "pruning only under a fresh,
  complete proof"; the authoritative gate moves to publish for the off-lock path.
  **New catalog note:** "off-lock background pruning re-validates its safety gates
  (live shared-table + floor movement) at publish under the lock; the below-floor
  drop is invariant to concurrent above-floor commits, so the content fingerprint
  is deliberately NOT re-checked there."
- **COW-005/006** — the live shared-table re-check closes the fork-mid-window
  TOCTOU (Hazard A).
- **CMP-002 / MVCC-005 / MVCC-009** — unchanged floor semantics; a below-floor
  drop never strands a still-reachable version (floor-movement guard).

## 7. Risks & mitigations

- **Wasted off-lock work on defer:** a fork/floor-move discards a completed
  off-lock compaction. Acceptable (rare; the sweep reclaims the orphaned output,
  same as the existing stale-candidate path). Mitigation: none needed for V1.
- **Missing a content gate that mattered:** the only content the fingerprint
  guards beyond the input tables (covered by `require_candidate_current`) is the
  active memtable, which is provably irrelevant to below-floor pruning. Pinned by
  the "publishes despite concurrent commit" test.
- **Scope creep:** if slices 2-3 exceed ~250 LOC together, land Slice 1
  (validator + red test) and Slice 4 (explicit drain) first, then the background
  wiring as its own PR.

## Critical files

- `crates/storage/src/lifecycle/durable/maintenance.rs` — start (:3460), Build
  variant (:195), build() (:339), begin_compaction_publish (:2336), the new
  runtime re-validation, explicit drain (:652).
- `crates/storage/src/lifecycle/rewrite_publication.rs` — `PreparedDurableCompaction`
  (:34), `install_prepared_durable_compaction_without_publish` (:441).
- `crates/storage/src/branch/pruning.rs` — `derive_candidate_tables_not_shared`
  (:245), `validate_timestamp_floor` (:494), the partial re-validator's helper.
- `crates/storage/src/branch/facts.rs` — `SharedTableRegistry::rebuild_from_snapshots`
  (:876).
- `crates/storage/src/lifecycle/tests/durable.rs` — phase-sequenced concurrency
  tests (templates at cache.rs:574, durable.rs:4219/4483).

[[project_storage_amplification]] [[project_temporal_contract]] [[feedback_cow_gc_invariant]]
