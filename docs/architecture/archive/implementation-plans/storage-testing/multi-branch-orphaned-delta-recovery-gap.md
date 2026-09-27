# Multi-branch orphaned-delta recovery gap — finding + fix plan

**Status:** **fixed** (2026-09-27, space-reclamation contract §3.2, slices 11–12) — the per-branch fix below shipped: every checkpoint snapshot records the set of branches holding a durable table-manifest base (snapshot section kind 4, `docs/design/3596-space-reclamation-contract.md`), and recovery decides per branch (`branch_recovery_disposition` in `lifecycle/recovery.rs`, applied to the seeded branch in Phase 1 and to every non-seeded branch in `durable/bootstrap.rs`): a recorded branch whose manifest survives COMBINES, a recorded branch whose manifest is gone is an orphaned delta — its snapshot rows are discarded and only its WAL-contiguous prefix from the first version is replayed, under DataLoss health with a `MissingTableManifestBase` fault naming the branch (strict recovery refuses) — and an unrecorded branch is a self-contained full snapshot. The guard (`non_seeded_branch_has_durable_base`, `DeferredNonSeededBranchBase`, the close drain's any-non-seeded-branch predicate) is deleted; multi-branch checkpoints complete. Regressions: `multi_branch_checkpoint_completes_and_a_lost_non_seeded_manifest_recovers_its_wal_prefix`, `lost_non_seeded_manifest_after_a_truncating_close_recovers_empty_with_data_loss_health`, `lost_non_seeded_manifest_refuses_a_strict_open`, `a_branch_re_created_after_the_snapshot_is_not_an_orphan_of_the_recorded_generation` (`lifecycle/tests/recovery.rs`). The text below is the finding and plan as written while the guard stood.

**Status (superseded):** **guarded** (2026-06-19) — the gap is unreachable. A checkpoint defers while any branch other than the recovery-seeded branch holds a durable table-manifest base (`non_seeded_branch_has_durable_base` in `lifecycle/checkpoint.rs`, enforced on both the synchronous and background drain paths), so no snapshot that recovery could not undo is ever recorded. Root cause was proven by a deterministic repro, now converted into a passing guard regression. The **per-branch fix** that lifts the guard (a frozen-format per-branch flushed-branch set + per-branch recovery, re-enabling multi-branch checkpoints) remains deferred to its own slice, coordinated with the post-V1 multi-branch durable-maintenance work.
**Severity:** **high** (latent) — without the guard: silent, non-contiguous data loss (a recovered "gap", not a tolerated prefix loss) for a non-seeded branch, under a multi-branch database + a crash that drops that branch's table manifest. The guard reduces this to a bounded, documented limitation (multi-branch checkpoints defer; the WAL grows on disk until the configuration is recoverable again — recoverable, no data loss).
**Found by:** assessing task #91 (multi-branch precision of the seed-155 fix's global signals).
**Guard regression:** `crates/storage-next/src/lifecycle/tests/recovery.rs::multi_branch_checkpoint_defers_so_lost_non_seeded_manifest_recovers_cleanly` (passing — asserts the checkpoint defers and recovery is clean after the dropped manifest).
**Pre-existing:** the seed-155 fix (commit `6a9acf79`) closed this for the single/seeded branch only; it neither introduced nor worsened the non-seeded case, but it did not close it either.

## Summary

A checkpoint snapshot is a **bounded delta**: it carries only the rows still in a branch's active/frozen memtable, not the rows already flushed into that branch's **table manifest** (`tables/<branch>/manifest`), which is the durable base. So a flushed branch is reconstructable only from **{table manifest (base) + snapshot (delta) + WAL}** together, and a checkpoint truncates the WAL.

If a crash drops a branch's table manifest, recovery has the delta (snapshot survives) but not the base (manifest gone, WAL truncated) → it installs the delta alone → a non-contiguous **gap** (a later commit present, an earlier one missing). The seed-155 fix detects this and recovers a clean prefix instead — **but only for the seeded branch**. Non-seeded branches are reconstructed in a separate bootstrap phase that has no orphan detection, so the gap is silent for them.

## Guard (implemented — the "guard now" decision)

The V1-scope question (see Risks) was settled **guard-now / fix-later**: V1 does not yet support multiple branches each flushing independently, so rather than build per-branch recovery now, the gap is made **unreachable** by deferring the checkpoint.

**Mechanism.** Before a checkpoint records a snapshot, `non_seeded_branch_has_durable_base(branch_catalog, seeded_branch_id)` scans the active branches; if any branch other than the recovery-seeded (initial) branch has `owned_table_count() > 0` (a durable table-manifest base), the checkpoint returns `Deferred` (`LifecycleCheckpointStatus::DeferredNonSeededBranchBase`) instead of publishing. It is enforced on **all three** publish paths — the synchronous runtime collector (`checkpoint_durable_runtime_with_budget`, used by the explicit and synchronous-maintenance paths), the background builder (`start_next_background_checkpoint_maintenance`), and the close drain (`DurableCloseMaintenanceRunner::run_checkpoint`, which re-runs checkpoint tasks stranded in the active list by a detached or panicked worker). The close-drain rule is stricter: it defers whenever **any** non-seeded branch exists, because its collector is seeded-only, so a published snapshot would additionally drop never-flushed non-seeded WAL rows on a clean close+reopen (found by TCP2.7, issue #2624; regression `close_drained_checkpoint_does_not_bypass_the_multi_branch_guard`).

**Why this closes it.** The gap needs a snapshot whose watermark sits above a non-seeded branch's base: recovery's `replay_start` then skips the WAL prefix holding that base, and the bootstrap rebuilds the branch from {snapshot delta + per-branch manifest} without the WAL. Deferring means no such snapshot exists — the branch's rows stay in the WAL, and a later full replay reconstructs every branch even if a crash drops the manifest. (The flush itself is deliberately **not** gated: flushing is the only path that drains frozen memtables, so gating it would trade the gap for unbounded memory growth. Deferring the checkpoint trades it for bounded WAL growth on disk instead — recoverable, no data loss.)

**Cost / limitation.** A multi-branch database in which a non-seeded branch flushes stops checkpointing until it returns to a recoverable shape (no non-seeded base). The WAL then grows on disk and recovery replays more of it — surfaced as `Deferred` checkpoint outcomes, never data loss. Typical multi-branch use (time-travel reads, short-lived forks that never accumulate enough writes to flush) never trips the guard.

**To lift the guard:** implement the per-branch fix below, replace the defer with per-branch orphan detection, re-enable multi-branch checkpoints, and update the guard regression to assert a completed checkpoint + clean recovery.

## Proven mechanism

Two branches, `main` (seeded) and `feature` (non-seeded). For each: commit a `base` row → flush (base → table manifest) → commit a `delta` row (active) → one global checkpoint (snapshot = `{base-delta, feature-delta}`; each base in its branch's manifest; WAL truncated). Then a crash drops **only `feature`'s** table manifest.

On reopen:
- **Phase 1** (`recovery.rs::recover`) rebuilds the seeded branch `main` and runs the seed-155 orphan check — `main`'s manifest is present, so it recovers cleanly.
- **Phase 2** (bootstrap) rebuilds `feature`: `install_non_seeded_checkpoint_rows` installs `feature`'s delta from the snapshot, and `recover_per_branch_table_manifests` (`bootstrap.rs:1313`) loads table manifests via `load_all_current` — which **iterates only the manifests that still exist**. `feature`'s manifest is gone, so it is simply absent from the list and **silently skipped** — no fault, no orphan check.
- Result: `feature` recovers `{feature-delta}` without `{feature-base}` → **gap**. The repro asserts a clean prefix and fails with `base_present=false, delta_present=true`.

## Root cause — why a local patch can't fix it

To catch a non-seeded branch's orphan, recovery must answer: *"was this branch flushed (so it needs a base) and is that base now gone?"* It cannot, with today's durable signals:

1. **No detection of a missing manifest.** `recover_per_branch_table_manifests` enumerates present manifests; a missing one is invisible. Recovery never learns a manifest *should* have been there.
2. **The base-floor signal is global, not per-branch.** The database manifest records a single `flushed_through_commit_id` (the seed-155 fix records the *max* across branches). It cannot say what any individual branch's base floor was.
3. **The full-vs-delta ambiguity.** A branch with snapshot rows but no manifest is indistinguishable between:
   - *flushed, manifest lost* → a **delta over a missing base** (must discard → clean prefix), and
   - *never flushed* → a **full, self-contained snapshot** (must keep).
   Guess "orphan" → you delete a never-flushed branch's healthy data. Guess "full" → you keep the gap.

The missing ingredient is a **durable, per-branch record of which branches were flushed** (have a table-manifest base) as of the checkpoint — recorded somewhere that survives the manifest loss (the database manifest or the snapshot both survive; the per-branch table manifest does not). That record does not exist today, and the durable format is **frozen at M3** (golden-gated), so adding it is a format change, not a local recovery patch.

## Implementation plan

The fix has four parts. It should land with the broader multi-branch durable-maintenance work (relaxing the three single-branch flush-watermark guards), because they share the same per-branch requirement.

### 1. Durable format: a per-branch "flushed branches" record (the discriminator)

Record, in the **database manifest** (which survives the manifest-loss crash), the set of branches that have a durable table manifest as of the checkpoint — i.e. the branches whose snapshot rows are a delta over a base. The minimal sufficient form is a **set of branch ids** (the discriminator); a per-branch base-floor *map* (`branch_id → flushed_through`) is the richer alternative if a future need wants the floor values.

- Recommended: a per-branch **flushed-branch set**. It is the exact signal recovery needs ("this branch has a base"), is lighter than per-branch floors, and subsumes the seed-155 global `flushed_through` for orphan detection. Keep the existing global `flushed_through` for its other uses (replay_start, WAL-truncation proofs).
- Files: `src/format/manifest.rs` (`DatabaseManifest` encode/decode + a format-version bump or an explicit extension section — V1 is a clean break, so a version bump is acceptable), `src/service/manifest.rs` (`with_recovery_facts` / a new `persist_*` surface that writes the set atomically with the snapshot facts).
- Constraint: this is the frozen-format change. It needs the format-version
  handling + golden regeneration; `format_golden` will need new/updated vectors.

### 2. Write-side: the checkpoint records the flushed-branch set

The checkpoint collectors already walk every active branch and compute each branch's `branch_checkpoint_flush_boundary` (`src/lifecycle/checkpoint.rs:1054`). Extend them to collect the set of branches with `owned_table_count() > 0` (flushed) and thread it — alongside the existing `flush_boundary` — into `persist_snapshot_facts_*`, recorded atomically with the snapshot facts (same atomic-write discipline as the seed-155 base floor). Touch points mirror the seed-155 plumbing: `checkpoint_durable_runtime_with_budget` (~1336), `checkpoint_durable_branch_with_budget` (~1314), `publish_checkpoint`/`publish_checkpoint_rows`, the `DurableBackgroundMaintenanceBuild::Checkpoint` build arm (`durable/maintenance.rs` ~213), and `CheckpointRequest` (`service/checkpoint.rs`).

### 3. Recovery: per-branch orphan detection in BOTH phases

- **Phase 1 — seeded branch** (`recovery.rs::recover`, ~125): replace the global `flushed_through`-vs-watermark orphan condition with the per-branch signal — "the seeded branch is in the recorded flushed-branch set AND its `table_manifest_stage` is absent" → orphaned. (This also corrects the seeded branch's multi-branch imprecision, where the global `max` floor was not its own.)
- **Phase 2 — non-seeded branches** (`durable/bootstrap.rs::recover_per_branch_table_manifests`, ~1313): drive the loop off the **recorded flushed-branch set**, not `load_all_current`. For each branch in the set: if its manifest is present → apply it (existing combine); if its manifest is **absent** → it is an orphaned delta → discard that branch's installed snapshot-delta rows and recover its WAL-contiguous prefix (empty when the base is unrecoverable, mirroring `recover_wal`'s contiguity guard). Branches *not* in the set are full self-contained snapshots → keep (no change).
- Record `RecoveryFaultKind::MissingTableManifestBase` per affected branch → `DataLoss` health (reuse the existing variant).

### 4. WAL-contiguity + clean-prefix recovery per branch

The seed-155 `recover_wal` contiguity guard discards an orphaned WAL tail for the global replay. For non-seeded orphan recovery, the per-branch discard (3) must compose with the WAL replay so each orphaned branch ends at a contiguous prefix from its base, not a gap. Verify the interaction with the shared (global) WAL + per-branch state install order in the bootstrap.

### Coordinated: relax the single-branch flush-watermark guards

The three `active_branch_ids() != vec![branch_id]` guards (`durable/maintenance.rs:921, 2148, 3092`) gate the per-branch flush-watermark *proof* to single-branch. Their comments already note they must be expanded for multi-branch. This fix and that expansion are the same slice — do them together so multi-branch durable maintenance is correct end-to-end (per-branch flush watermarks + per-branch orphan recovery).

## Test plan

1. **Flip the guard regression** `multi_branch_checkpoint_defers_so_lost_non_seeded_manifest_recovers_cleanly`: once per-branch recovery lands the checkpoint should run rather than defer, so update it to assert a *completed* checkpoint plus a clean recovery (non-seeded branch recovers a clean prefix, not a gap), replacing today's "defer + clean recovery" guard assertion.
2. **Write-side unit tests:** a multi-branch checkpoint records the flushed-branch set correctly; a never-flushed branch is absent from the set; the set round-trips through manifest encode/decode (extend the `service/manifest.rs` proptest).
3. **Recovery unit tests (per-branch):** (a) non-seeded branch flushed + manifest dropped → clean prefix + `DataLoss`; (b) non-seeded branch **never flushed** (full snapshot) → fully retained (guard against the over-aggressive false positive); (c) seeded branch flushed + manifest dropped in a multi-branch DB → clean prefix using its own floor (not the global max); (d) both branches healthy → both fully recovered.
4. **Multi-branch crash harness (new infra — the larger item):** the STH-2/3/4 fault + crash harnesses are single-branch (`default_branch` only) and the recovery oracle/model are per-branch-single. Extend them to N branches: create + flush + delta per branch, crash dropping seed-chosen objects (including per-branch table manifests), and oracle-verify **every** branch recovers a clean prefix (no gap). Add the deep multi-branch soak alongside the existing single-branch one.
5. **Golden vectors:** regenerate `format_golden` vectors for the new manifest
   field/version; confirm the manifest proptest.
6. **Full gate:** `--lib` + integration (crash/fs-model/recovery/maintenance/simulation), `clippy --all-features --all-targets -D warnings`, fmt, default + no-default builds, and the multi-branch soak clean end-to-end.

## Risks + scope

- **Frozen-format change (M3).** The database-manifest addition is the heaviest part: format-version handling + golden regeneration. V1 is a clean break (no pre-V1 compat), which simplifies it, but it must be deliberate.
- **New multi-branch test infrastructure.** The fault/crash oracle + model + harness are single-branch today; multi-branch crash coverage is net-new and is itself a meaningful slice.
- **Recovery rework across two phases.** Per-branch orphan detection in both `recover()` and the bootstrap is delicate, durability-critical code; the per-branch discard must compose with the shared WAL replay.
- **Coordinate, don't fragment.** Do this with the flush-watermark guard relaxation so multi-branch durable maintenance is correct as a whole, rather than patching recovery against a write path that's still single-branch-guarded.
- **V1-scope question — settled (guard-now / fix-later):** V1 does not yet support multiple branches each with independent flushes/owned tables, so the gap is *guarded* now — the checkpoint defers while a non-seeded branch holds a durable base (see "Guard" above) — and the per-branch fix is deferred to its own slice. Lifting the guard is gated on that per-branch work landing.

## References

- Guard regression: `multi_branch_checkpoint_defers_so_lost_non_seeded_manifest_recovers_cleanly` (`src/lifecycle/tests/recovery.rs`).
- Single-branch precedent (the fix to generalize): `sth-4-finding-splitrename-power-loss-gap.md` (commit `6a9acf79`).
- Code anchors: `recovery.rs::recover` orphan detector (~125); `bootstrap.rs::recover_per_branch_table_manifests` (1313) + `install_non_seeded_checkpoint_rows` (1375); `checkpoint.rs::branch_checkpoint_flush_boundary` (1054); `service/manifest.rs` `persist_snapshot_facts_with_flush_boundary`; the flush-watermark guards (`durable/maintenance.rs:921, 2148, 3092`).
