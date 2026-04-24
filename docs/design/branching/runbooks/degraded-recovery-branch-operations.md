# Degraded recovery: branch-scoped primitives

## Symptom

- A call against a named vector collection or JSON secondary index
  returns `StrataError::PrimitiveDegraded { branch, primitive, name, reason }`.
- Other primitives on the same branch still work; sibling branches
  are unaffected.
- `db.retention_report()` includes an entry in `degraded_primitives`
  matching the failing `BranchRef` + `primitive` + `name`.

## Diagnosis

B5.4 (branching-gc convergence doc §"Degraded-state closure targets")
requires that named primitive surfaces fail closed when their
persisted state is inconsistent. Rather than silently serving stale or
wrong data, the engine marks the primitive degraded in the per-Database
`PrimitiveDegradationRegistry` and returns the typed error on every
subsequent read.

Reasons currently modeled by `PrimitiveDegradedReason`:

- `ConfigDecodeFailure` — the collection's persisted configuration
  bytes failed to deserialize.
- `ConfigShapeConversion` — the persisted config decoded but refers to
  a legacy shape the current engine no longer supports.
- `ConfigMismatch` — persisted configuration disagrees with persisted
  data (e.g. declared vector dimension != stored embedding length).
- `IndexMetadataCorrupt` — secondary-index metadata bytes failed to
  decode.
- `IndexEntryCorrupt` — a secondary-index entry key could not be
  decoded.

### How to read the report

```rust
let report = db.retention_report()?;
for entry in &report.degraded_primitives {
    println!(
        "branch={} primitive={:?} name={} reason={:?}",
        entry.name, entry.primitive, entry.primitive_name, entry.reason,
    );
}
```

The report is **generation-equality-filtered**: entries whose
`BranchRef.generation` does not match the currently-live control
record are dropped. Same-name recreate therefore produces a clean
slate — the new lifecycle does not inherit the old lifecycle's
degradations.

## Recovery

Recovery depends on the `reason`:

### `ConfigDecodeFailure` / `ConfigShapeConversion` / `IndexMetadataCorrupt`

The on-disk configuration bytes are unusable. Repair paths:

1. **Drop and recreate the collection** — if data loss is acceptable,
   `db.branches().delete(branch)` + recreate + re-ingest clears the
   registry entry (the physical cleanup hook clears the registry on
   delete-commit).
2. **Manual config repair** — operators with out-of-band backups can
   write a replacement configuration record at the known key
   (`__config__/{collection_name}` for vectors, `_idx_meta/{space}/{name}`
   for JSON) via the transaction API. The next read triggers a fresh
   load and the registry entry clears if the replacement is valid.

### `ConfigMismatch`

The data and config disagree on shape (e.g. dimension). This is
almost always a sign that the primitive state was partially migrated.
Treat as `ConfigShapeConversion` — repair the config to match the
data, or drop and re-ingest.

### `IndexEntryCorrupt`

A single row in the secondary index is corrupt but the surrounding
metadata is intact. Rebuild the index: the engine supports dropping
the `_idx/{space}/{name}` prefix and re-scanning the underlying
collection to repopulate. Consult the JSON secondary-index
implementation for the exact procedure.

## Verification

1. The failing read returns a non-degraded result (or a normal
   not-found, not the degraded error).
2. `db.retention_report().degraded_primitives` no longer contains
   the repaired entry.
3. Reopening the database and re-running the read still succeeds —
   the registry is in-memory, so a reopen by itself does not clear
   degradations; the next read must re-attempt the load and succeed.
4. The adversarial-history suite pins postcondition 6 at
   `tests/integration/branching_adversarial_history.rs`: injected
   degradations stay attributed to the exact `BranchRef` they were
   marked against, never leak to siblings, and respect the
   generation-equality filter. A regression in registry attribution
   would fail that assertion.

## Cross-branch isolation

A degraded primitive on one branch never affects sibling branches —
the registry key is `(BranchId, PrimitiveType, String)` and the
retention-report join is generation-aware. Operators should see one
entry per degraded (branch, primitive) and may freely continue using
other branches and other primitives on the same branch.
