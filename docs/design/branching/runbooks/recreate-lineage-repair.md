# Recreate lineage repair

## Symptom

- The operator deleted a branch (call it `main`) and later recreated
  it under the same name, but `retention_report()` shows unexpected
  non-zero bytes attributed to `orphan_storage` for the `BranchId`
  matching `main`.
- Disk usage does not drop after the delete even though no other live
  branch "obviously" references the old lifecycle's data.
- `branches().info("main")` confirms a live, fresh generation, but
  `retention_report().branches[…].shared_bytes` on the new `main`
  entry is `0` — all the weight is in `orphan_storage`.

## Diagnosis

This is the expected shape when:

1. A branch was deleted while it still had **live descendants**
   (forks taken before the delete).
2. The branch was then recreated under the same name.
3. Those descendants are still live and still reference segments
   owned by the *old* (tombstoned) lifecycle through their
   inherited-layer manifests.

The retention contract
(`docs/design/branching/branching-gc/branching-retention-contract.md`,
§"Same-name recreate") keeps old-lifecycle bytes visible by routing
them through `RetentionReport::orphan_storage` with
`OrphanReason::DescendantInheritance`, **never** folding them into the
recreated branch's entry. This is the correct behavior. The old bytes
are held by the descendants, not by the new lifecycle.

### How to read the report

```rust
let report = db.retention_report()?;
for orphan in &report.orphan_storage {
    match orphan.reason {
        OrphanReason::DescendantInheritance => {
            // Live descendant holds this parent's old-lifecycle bytes.
            println!(
                "orphan branch_id={:?} bytes={} (held by a descendant)",
                orphan.branch_id, orphan.bytes,
            );
        }
        OrphanReason::UntrackedLifecycle => {
            // No live descendant; candidate for reclaim once GC runs.
        }
    }
}
```

## Recovery

The only way to release these bytes is to drop the descendant's
reference. Two paths:

1. **Materialize the descendants** — for each live descendant whose
   fork anchor points at the old-lifecycle bytes, call
   `db.storage().materialize_layer(&descendant_id, 0)` to copy the
   inherited data into the descendant's own segments. Once every
   descendant has materialized, the old-lifecycle bytes become
   eligible for reclaim.
2. **Delete the descendants** — if the descendant branches are no
   longer needed, deleting them through
   `db.branches().delete(descendant_name)` drops their inherited-layer
   references. Reclaim runs at the next GC cycle under healthy
   recovery.

Choose (1) when the descendants must stay readable. Choose (2) when
the retention debt is acceptable only because the descendants
themselves are disposable.

## Verification

1. After materialize or delete, run `db.retention_report()` again and
   confirm:
   - `orphan_storage` no longer carries the targeted `branch_id`, or
   - `totals.shared_bytes` dropped by the expected amount.
2. Invoke the GC loop via `db.storage().gc_orphan_segments()`; it
   should complete without returning a typed error.
3. The adversarial-history suite pins postcondition 5 at
   `tests/integration/branching_adversarial_history.rs` —
   "recreated parents with live descendants don't inherit
   old-lifecycle `shared_bytes`". A regression that folds old bytes
   into the new entry would fail that assertion.
