# Blocked follower refresh

## Symptom

- A read-only follower database refuses lineage queries with an
  `InvalidOperation` error whose reason text begins with
  `branch_lineage_unavailable:`.
- `StrataError::is_branch_lineage_unavailable()` returns `true` on the
  returned error.
- Operations on the follower like `branches().find_merge_base()`,
  `edges_for(...)`, or `list_active()` refuse while the follower holds
  legacy-format branch metadata that has not yet been migrated by the
  primary.

## Diagnosis

This is the follower-synthesis-refusal behavior from B3 (AD5). When a
follower opens against an unmigrated primary, fork anchors can be
synthesized from storage but merge/revert edges cannot. Rather than
fabricate an incomplete lineage, the follower refuses.

### How to detect programmatically

```rust
match db.branches().find_merge_base(a, b) {
    Ok(result) => { /* normal path */ }
    Err(e) if e.is_branch_lineage_unavailable() => {
        // Follower needs primary migration to answer lineage queries.
    }
    Err(e) => return Err(e),
}
```

The predicate `is_branch_lineage_unavailable()` is stable and
isolates callers from the underlying wire shape.

## Recovery

Lineage unavailability on a follower clears when the primary completes
migration:

1. On the **primary**, confirm migration ran to completion — legacy
   `branch_metadata` records have been re-serialized into the B3
   control-store form. Re-opening the primary after a
   post-B3 upgrade triggers migration on first open.
2. Trigger a follower refresh: on the follower, call
   `db.refresh()` (or the equivalent executor API). The follower reads
   the primary's post-migration snapshot; the next lineage query
   succeeds.
3. If the primary cannot be migrated (e.g., the operator is running a
   deliberately legacy primary for compatibility), the follower's
   lineage queries remain refused by design. Consumers that don't
   need lineage (raw KV reads, existing primitives) continue to work.

## Verification

1. Re-run the failing lineage call on the follower. It should return
   `Ok(...)` once migration completes and the follower refreshes.
2. `is_branch_lineage_unavailable()` should return `false` on any
   subsequent non-lineage `InvalidOperation` (i.e. the predicate is
   narrowly scoped).
3. The adversarial-history suite pins postcondition 4 at
   `tests/integration/branching_adversarial_history.rs`: model-predicted
   rejections return the existing typed error. A regression that
   changes the predicate's match shape would fail that assertion.

## Why this is not a promoted variant

`branch_lineage_unavailable` is still surfaced as
`StrataError::InvalidOperation` with a reason-prefix string. The
`is_branch_lineage_unavailable()` predicate (`crates/core/src/error.rs`)
is the sanctioned detection surface — callers never inspect the
prefix directly. Tranche 4 explicitly declines to promote this to a
dedicated variant; see `tranche-4-closeout.md` §"Deferred items" for
the rationale and re-open criteria.
