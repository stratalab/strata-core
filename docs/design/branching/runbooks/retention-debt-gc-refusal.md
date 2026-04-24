# Retention debt and GC refusal

## Symptom

Either:

- `db.retention_report()` returns
  `Err(StrataError::RetentionReportUnavailable { class })` where
  `class` is `"DataLoss"` or a `PolicyDowngrade` variant; or
- The report succeeds but `report.reclaim_status` is
  `ReclaimStatus::BlockedDegradedRecovery { class }`; or
- `db.storage().gc_orphan_segments()` returns a storage error and the
  log shows retention debt in `__quarantine__/`.

Disk usage stays high because Stage-5 (physical purge) refuses to
run.

## Diagnosis

Retention report and GC share one refusal rule: reclaim must be
attribution-safe. The engine distinguishes two cases:

1. **Hard refusal** (`RetentionReportUnavailable`) — recovery left the
   system in a state where manifest-derived attribution would be
   fabricated. The contract
   (`docs/design/branching/branching-gc/branching-retention-contract.md`
   §"Recovery-health contract") refuses to produce a report rather
   than guess. This is the `DataLoss` case and the
   `QuarantineInventoryMismatch`-driven `PolicyDowngrade` case.
2. **Soft refusal** (`ReclaimStatus::BlockedDegradedRecovery`) — the
   report is trustworthy but reclaim is paused. GC stops at Stage 4
   (attribution) and does not perform Stage-5 purge. Bytes stay on
   disk until recovery clears.

### How to read the report

```rust
match db.retention_report() {
    Ok(report) => {
        match &report.reclaim_status {
            ReclaimStatus::Allowed => { /* reclaim will run */ }
            ReclaimStatus::BlockedDegradedRecovery { class } => {
                eprintln!("reclaim paused: {class}");
            }
        }
    }
    Err(StrataError::RetentionReportUnavailable { class }) => {
        eprintln!("attribution fail-closed: {class}");
    }
    Err(e) => return Err(e),
}
```

## Recovery

### Hard refusal

A `RetentionReportUnavailable` fault traces back to the last recovery
pass. `Database::recovery_health()` returns the authoritative class.
Typical paths:

- `DataLoss` — authoritative data is missing. Restore from backup or
  re-seed the missing branch directories. Re-open the database; the
  next recovery pass should report `Healthy` and the report
  surfaces normally.
- `QuarantineInventoryMismatch` — the quarantine manifest on disk
  disagrees with the observed inventory. The recovery subsystem logs
  the specific mismatch. Resolution is case-by-case (often a manual
  inventory repair); once recovery completes with the mismatch
  resolved, the report is available again.

### Soft refusal (`BlockedDegradedRecovery`)

This is not an emergency — attribution is correct, reclaim is just
paused. Operators can:

1. Inspect the `class` field to understand why (no-manifest fallback,
   sparse quarantine, etc.).
2. Fix the underlying degradation. For no-manifest fallback, setting
   `StrataConfig::allow_missing_manifest = false` on the next open
   refuses the degraded path; operators then either provide the
   missing manifest or accept that reclaim stays paused.
3. Once the next recovery pass returns `Healthy`, reclaim resumes
   automatically on the next GC cycle.

### GC refusal

If `gc_orphan_segments()` fails with a storage error, treat it as a
hard refusal — the same paths apply. Do not retry in a loop; fix the
underlying recovery health first.

## Verification

1. `db.recovery_health()` returns `Healthy`.
2. `db.retention_report()` returns `Ok(...)` with
   `reclaim_status == ReclaimStatus::Allowed`.
3. `db.storage().gc_orphan_segments()` completes without error.
4. The adversarial-history suite pins the reclaim-status sanity
   assertion at `tests/integration/branching_adversarial_history.rs` —
   on healthy reopens with only primitive degradations injected,
   `reclaim_status` stays `Allowed`. A regression that flipped the
   status on primitive degradation alone (no `RecoveryHealth` fault)
   would fail that assertion.

## Retention debt observability gap

Advisory warnings and post-commit failures from the delete path
currently surface only as `tracing::warn!` lines at
`crates/engine/src/database/branch_service.rs:552-566` and
`crates/storage/src/segmented/mod.rs:2042`. Tranche 4 explicitly
declined to promote these into a programmatic registry — see
`tranche-4-closeout.md` §"Deferred items" for the rationale. Operators
who need programmatic routing today must parse the tracing output;
the deferral documents the re-open criterion ("concrete operator ask
for programmatic access") for future work.
