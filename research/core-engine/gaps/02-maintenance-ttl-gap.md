# Gap 02: Retention and TTL Maintenance Are Only Partially Implemented

## Summary

The engine exposes maintenance entry points for GC and TTL expiration, but the runtime
does not have a complete maintenance subsystem behind those APIs. The public surface is
wider than the actual maintenance behavior.

This is not the same as saying MVCC pruning does nothing. Compaction still prunes versions.
The gap is that explicit maintenance orchestration is incomplete.

## What The Code Does Today

### Maintenance APIs exist, but they are manual

`Database::run_gc()` and `Database::run_maintenance()` both say they are not called
automatically and that a future background maintenance thread is expected.

Code evidence:

- `crates/engine/src/database/lifecycle.rs:56-92`

### The storage hooks behind those APIs are stubs

`SegmentedStore::gc_branch()` returns `0` and is documented as a no-op because pruning is
handled via compaction.

`SegmentedStore::expire_ttl_keys()` also returns `0` and is documented as a no-op because
TTL is handled at read time and during compaction.

Code evidence:

- `crates/storage/src/segmented/mod.rs:2283-2292`

### TTL has a dedicated index type, but not an active runtime loop

There is a concrete `TTLIndex` implementation in `crates/storage/src/ttl.rs`, but the
runtime path does not wire it into `run_maintenance()` or any background expiration worker.

The code-derived evidence here is the combination of:

- the stubbed `expire_ttl_keys()` entry point
- the explicit "not automatic" maintenance API
- the absence of a runtime expiration worker in the engine open/lifecycle path

Relevant files:

- `crates/storage/src/ttl.rs`
- `crates/engine/src/database/lifecycle.rs:80-92`
- `crates/storage/src/segmented/mod.rs:2289-2292`

## Why This Is An Architectural Gap

The engine currently mixes three different notions of retention:

- compaction-time MVCC pruning
- read-time TTL visibility checks
- explicit maintenance APIs

Those pieces do not add up to a coherent maintenance architecture.

What is missing is an active control loop that answers:

- when GC runs
- when TTL expiration runs
- how aggressively it runs
- what backpressure or observability it has

Right now the user-facing surface suggests that maintenance is a first-class subsystem.
The implementation is still closer to "manual hooks plus compaction side effects."

## Concrete Operational Consequences

### Expired data may remain physically present for a long time

If TTL is enforced only at read time and during future compaction, expired keys may become
logically invisible before they are physically reclaimed.

### Maintenance behavior depends on write traffic

Because compaction is carrying part of the retention burden, reclamation is better when the
database is actively writing and compacting than when it is mostly idle.

### The GC APIs over-promise

`run_gc()` computes a safe point and iterates branches, but the per-branch implementation is
a stub. That means the API reads like active garbage collection while the actual reclamation
work still lives elsewhere.

## Why Compaction Alone Does Not Close The Gap

Compaction is necessary, but it is not the same thing as a maintenance architecture.

Compaction answers:

- how files are merged
- how older versions are pruned while rewriting segments

It does not fully answer:

- when TTL expirations are scanned proactively
- whether cold branches are cleaned up without new writes
- how maintenance progress is surfaced operationally
- how retention work is scheduled relative to ingest pressure

## Likely Remediation Direction

Inference from the current code shape:

1. Add a real maintenance scheduler owned by the database runtime.
2. Decide whether `gc_branch()` and `expire_ttl_keys()` should become real mutation paths
   or whether the public APIs should be narrowed to match compaction-based reclamation.
3. If TTL is meant to be proactive, wire `TTLIndex` into the write path and into a periodic
   expiration worker.
4. Add metrics for:
   - expired keys pending
   - reclaimed versions
   - maintenance lag
   - last successful maintenance cycle
5. Test idle-database behavior, because that is where "read-time plus compaction-time"
   retention usually shows its blind spots.

## Bottom Line

Strata has retention-related mechanisms, but it does not yet have a fully operational
maintenance subsystem. The architecture currently relies on compaction and manual calls more
than the surface API implies.
