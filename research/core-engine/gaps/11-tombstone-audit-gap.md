# Gap 11: Tombstone Audit Infrastructure Exists, but Is Not Integrated

## Summary

The durability crate contains a substantial tombstone-audit implementation, including reasons,
serialization, indexing, and retention cleanup. But the live engine path does not record
deletes or compaction removals into that index.

So tombstone auditing exists as library code, not as active engine behavior.

## What The Code Does Today

`tombstone.rs` implements:

- `Tombstone`
- `TombstoneReason`
- `TombstoneIndex`
- serialization / deserialization
- lookup by key and version
- cleanup by age

The module comments describe two intended purposes:

- read filtering
- audit trail

Code evidence:

- `crates/durability/src/compaction/tombstone.rs:1-18`
- `crates/durability/src/compaction/tombstone.rs:21-56`
- `crates/durability/src/compaction/tombstone.rs:270-360`

The compaction module exports these types publicly.

Code evidence:

- `crates/durability/src/compaction/mod.rs:28`

Repository usage inspection shows no engine/storage/concurrency/graph/vector runtime call
sites that add or maintain a live `TombstoneIndex`. The matches outside the tombstone module
are exports, comments, or unrelated `record_*` functions.

Code evidence:

- repo-wide search over `crates/engine`, `crates/storage`, `crates/concurrency`,
  `crates/graph`, and `crates/vector` for `TombstoneIndex`, `TombstoneReason`, and
  `cleanup_before()` produced no live integration points

## Why This Is An Architectural Gap

There is a difference between:

- a type existing in the codebase
- that type participating in the storage/durability pipeline

Today the tombstone model is in the first category, not the second.

That matters because the presence of a concrete tombstone index suggests capabilities the
runtime does not actually provide:

- auditable reasons for deletion
- a persisted deletion history
- an indexable lifecycle for compaction-removed data

## Practical Consequences

### No engine-level deletion audit trail

The library model can represent deletion reason codes, but the runtime does not appear to
populate that model on:

- user deletes
- retention pruning
- compaction removal

### No cleanup loop for tombstone audit state

`cleanup_before()` exists, but there is no evidence that a live tombstone index is being
maintained and periodically cleaned.

### The comment-level design is ahead of the runtime

The module comment talks about read filtering and snapshot persistence. Because the index is
not wired into the live path, those properties do not currently describe actual engine
behavior for the dedicated tombstone-audit structure.

Inference: MVCC tombstones in the storage engine remain the real deletion mechanism; the
separate audit index is not part of that live read path today.

## Likely Remediation Direction

Inference from the current code shape:

1. Decide whether tombstone auditing is a committed product feature or an unused library.
2. If committed, wire it into:
   - user delete path
   - retention pruning path
   - compaction rewrite path
3. Persist it as part of checkpoint/recovery or another engine-owned metadata surface.
4. Expose query and retention semantics intentionally, rather than leaving the type dormant.
5. If the feature is not intended to ship, narrow or remove the surface so the architecture is
   less misleading.

## Bottom Line

The tombstone audit model is implemented, but it is not operationally integrated. That makes
it a latent subsystem rather than part of the core engine architecture.
