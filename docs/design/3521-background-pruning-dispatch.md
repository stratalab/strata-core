# #3521 — Wiring opt-in MVCC pruning into background compaction

Status: **DESIGN — awaiting sign-off.** Follow-up to #3502 slice D/D2.

## Problem

Opt-in `KeepRecentVersions` pruning (#3502) fires only through the
**flush-followup** compaction path (`run_compaction_maintenance_task`) and the
testkit force-compaction seam. The two other compaction dispatch paths never
build the pruning proof:

- **Background compaction** (`start_background_compaction_task`) — the
  autonomous, off-lock, steady-state reclaim path. Compresses, never prunes.
- **Explicit `maintenance(Compact)`** — the synchronous fixed-point drain.
  Neither compresses-with-prune nor prunes.

A database that opts into retention therefore reclaims disk only on the
flush-followup path — so under normal steady-state operation it reclaims
little. The measured win (≈67% table bytes, slice E2) is mostly unrealized.

**Scoping fact:** `strata compact` (the explicit user command) was **removed**
in V1. So the explicit drain is not a product-reachable path, and wiring it
alone delivers almost none of the issue's value. **The value lives entirely in
the background path**, which is the delicate one.

## Why the background path is delicate

`start_background_compaction_task` is **three-phase**:

1. `start` — under the runtime lock: build the compaction request, snapshot the
   branch (`branch.clone()`).
2. `build` — **off-lock**: run the compaction on the snapshot, producing output
   tables (`PreparedDurableCompaction`).
3. `publish` — under the lock: `install_prepared_durable_compaction_without_publish`
   installs the output onto the **live** branch. It already re-validates the
   *compaction candidate* against the live branch and **defers** (not fails) on a
   stale-candidate race (`is_stale_compaction_candidate`).

The pruning proof (`BranchCompactionPruningProof`, ARCH-005) is
**fingerprint-bound** to the exact branch state and gated on **shared-table
safety** (COW-005/006). In the foreground path the lock is held build→apply, so
the proof stays fresh and `validate_for_branch` (which **hard-fails** on a stale
proof) never fires spuriously. Off-lock, two hazards appear:

### Hazard A — shared-table TOCTOU (correctness-critical)

The proof's `candidate_tables_not_shared` gate is checked at `start`. If a
**fork appears during the off-lock window** that re-references the input tables,
the gate is now stale: publishing the pruned output would rewrite tables a COW
child still reads, stranding its pre-floor history (COW-005/006 violation). The
existing publish re-validation checks the *candidate inputs*, **not** the
pruning proof's shared-table gate — so this TOCTOU is currently open the moment
pruning is wired in.

### Hazard B — the content fingerprint is too strict for off-lock (key finding)

`branch_pruning_fingerprint` binds to the **whole branch content** (active +
frozen + owned levels + inherited). Off-lock, **any concurrent commit** mutates
the active memtable → the fingerprint mismatches → `validate_for_branch` would
reject at publish. On a branch taking writes (the normal case), background
pruning would therefore **defer on essentially every attempt** and rarely
complete. The fingerprint was designed for the *synchronous* path where the lock
excludes concurrent commits; it does not fit the off-lock path.

The semantic insight that resolves B: **dropping below-floor versions is
invariant to concurrent above-floor commits.** A new commit lands above the
retained floor and touches neither the below-floor rows being dropped nor the
shared-table safety of the input tables. So the *pruning* decision does not need
whole-content freshness — it needs only its own safety gates to still hold.

## Proposed design

Build the proof at `start` (under the lock, from the **live** branch — matching
the foreground pattern), thread `(policy, proof)` into the off-lock build, apply
it in `build` against the snapshot (fingerprint matches the snapshot by
construction), then **re-validate the pruning-safety gates — not the whole
fingerprint — at `publish` under the lock**, and install the pruned output only
when they still hold:

- **Publish-time pruning re-validation** (new, distinct from the existing
  candidate re-validation): against the live branch, re-check the gates that a
  concurrent change *can* invalidate —
  - `candidate_tables_not_shared` (rebuild `SharedTableRegistry` over current
    active branches; the input tables must still be branch-private) — closes
    Hazard A;
  - the retained-history floor still published and ≤ the versions actually
    dropped;
  - the input tables the pruned output was built from are still the branch's
    owned tables (already covered by the candidate re-validation).
  Deliberately **not** the content fingerprint (Hazard B) — a concurrent
  above-floor commit must not veto a below-floor drop.
- **Degradation policy: defer, don't fail.** If a pruning gate is stale at
  publish, **discard the pruned output and defer** (re-derive a fresh proof +
  candidate next pass), mirroring the existing stale-candidate → `Deferred`
  handling. Falling back to "install unpruned" is not possible — the off-lock
  build already produced pruned tables; a second build would be needed.
- **ARCH-005 preserved:** below-floor rows are still deleted only under a proof
  whose safety gates are validated **at the moment of install, under the lock**.
  The build-time validation against the snapshot is a fast-path; the publish
  re-validation is the authoritative gate.

### Open design questions (for sign-off)

1. **Re-validation granularity.** Introduce a `revalidate_pruning_for_publish`
   that checks only shared-table + floor (proposed), vs. reuse
   `validate_for_branch` with a *relaxed* fingerprint that excludes the active
   memtable? The former is explicit and testable; the latter risks re-introducing
   Hazard B subtly. **Recommendation: the former.**
2. **Floor movement.** Can the published `retained_history_floor` move *down*
   between start and publish? Under keep-newer-than-window it only rises with
   `visible`; a fork/branch-op is the only mover. Confirm the floor at publish is
   ≥ the floor the output was pruned against (else a now-retained version was
   dropped). **Proposed: refuse (defer) if the live floor < the proof's floor.**
3. **Explicit drain.** Wire it too (synchronous, trivial) for internal callers
   and future re-exposure, or leave it out since `strata compact` is removed?
   **Recommendation: include it — it is a ~5-line lock-held addition mirroring
   the flush-followup path, and it makes the pruning dispatch uniform.**
4. **Autonomous trigger.** Background compaction is dispatched by coverage
   scoring; does an opt-in-retention branch need a pruning-specific trigger so
   reclaim happens even without table-count pressure, or is riding the existing
   compaction cadence sufficient for V1? **Proposed: ride the existing cadence;
   a retention-pressure trigger is a separate follow-up.**

## Test / proof plan

- **DST concurrency (the core proof).** Extend the whole-DB sim to interleave,
  within the off-lock window of a background compaction on a retention-enabled
  branch:
  - a concurrent **above-floor commit** → pruning MUST still publish (proves
    Hazard B is resolved, not merely defended by deferral);
  - a concurrent **fork that shares the input tables** → pruning MUST defer /
    skip, and the fork's pre-floor `as_of` reads MUST still resolve (proves
    Hazard A is closed);
  - the floor-moved case (Q2).
  Non-vacuity: assert a background-pruning-completed counter > 0 on the clean
  interleaving.
- **Deterministic unit** (storage): background compaction under
  `KeepRecentVersions` with no concurrency prunes and republishes; below-floor
  `as_of` raises; latest exact. Direction control: a fork mid-window defers.
- **Invariant check:** ARCH-005 (re-validation at install), COW-005/006
  (shared-table TOCTOU), CMP-002, MVCC-005/009. New catalog note: "off-lock
  pruning re-validates its safety gates at publish under the lock; below-floor
  drop is invariant to concurrent above-floor commits."
- Standard gates: workspace suite, fault-injection lane 62/0, mutation-on-diff,
  fmt/clippy.

## Critical files

- `crates/storage/src/lifecycle/durable/maintenance.rs` —
  `start_background_compaction_task` (build proof at start),
  `begin_compaction_publish` / `install_prepared_durable_compaction_without_publish`
  (add pruning re-validation), the `DurableBackgroundMaintenanceBuild::Compaction`
  carrier (thread `version_pruning`), `compact_branch_tables_to_fixed_point`
  (explicit drain, Q3).
- `crates/storage/src/branch/state/compaction.rs` — the proof apply
  (`validate_for_branch`, install_branch_compaction_plan) and the new
  publish-time pruning re-validation.
- `crates/storage/src/branch/pruning.rs` — the gate helpers to re-run at publish.
- `crates/storage/src/testkit/simulation/whole_db.rs` — DST interleavings.

[[project_storage_amplification]] [[project_temporal_contract]] [[feedback_cow_gc_invariant]]
