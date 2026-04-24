# Branching operator runbooks

Four named operational scenarios that arise from tranche-4 branching
behavior. Each runbook follows the same shape:

- **Symptom** — what the operator observes (log line, typed error,
  failed call).
- **Diagnosis** — which typed error class or surface this maps to and
  how to read it programmatically.
- **Recovery** — the sanctioned procedure to restore service.
- **Verification** — how to confirm the recovery succeeded, including
  which assertion in the adversarial-history suite
  (`tests/integration/branching_adversarial_history.rs`) pins the
  invariant.

These runbooks cite the typed error surfaces that already exist on
`StrataError` and the existing pull surfaces on `RetentionReport`.
They do **not** introduce new public API — the tranche-4 closeout
deliberately stops at regression coverage, leaving typed-failure
promotion and push observers as deferred work
(see `docs/design/branching/tranche-4-closeout.md` §"Deferred items").

## Index

| Runbook | Trigger | Typed surface |
|---|---|---|
| [recreate-lineage-repair.md](recreate-lineage-repair.md) | Operator deleted + recreated a branch whose descendants are still live; old bytes surface as orphan storage. | `RetentionReport::orphan_storage`, `OrphanReason::DescendantInheritance` |
| [blocked-follower-refresh.md](blocked-follower-refresh.md) | Follower reads refused with lineage-unavailable; merge-base/ancestor queries fail. | `StrataError::is_branch_lineage_unavailable()` predicate |
| [retention-debt-gc-refusal.md](retention-debt-gc-refusal.md) | `retention_report()` refused or reclaim is blocked; GC cannot drain. | `StrataError::RetentionReportUnavailable`, `ReclaimStatus::BlockedDegradedRecovery` |
| [degraded-recovery-branch-operations.md](degraded-recovery-branch-operations.md) | A named primitive (vector collection, JSON `_idx`) fails closed on read. | `StrataError::PrimitiveDegraded`, `RetentionReport::degraded_primitives` |

## Common vocabulary

- **Lifecycle instance** — one `BranchRef = (BranchId, BranchGeneration)`.
  Same-name delete + recreate produces two distinct lifecycle instances
  with the same name and `BranchId` but different `generation`.
- **Orphan storage** — bytes on disk under a `BranchId` directory that
  have no *live* `BranchControlRecord` to attribute them to
  (typically because the owning lifecycle was tombstoned but
  descendants still reference its segments through inherited layers).
- **Degraded primitive** — a vector collection or JSON secondary-index
  that the engine fail-closes on read because its persisted
  configuration or data is inconsistent (B5.4). The branch as a whole
  stays readable; only the named primitive is refused until
  operator-led repair.
- **Reclaim** — Stage-5 of the quarantine protocol: physical purge of
  quarantined segments whose refcount has dropped to zero. Reclaim
  runs only under healthy recovery.

## When to re-open a runbook

The runbook text is stable as long as the typed errors it cites keep
their current shapes. Renames or removals of a cited variant must be
caught by the adversarial-history suite (which matches on variant
shape) before the runbook goes stale. If you're writing a PR that
reshapes one of the cited surfaces, update the runbook in the same
commit.
