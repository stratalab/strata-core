# Strata Core Engine: Code-Derived Notes

This document set was written from the code, not from the existing docs.
It focuses on the execution paths that actually define the engine today.

## What is in this set

- `storage-engine.md`: memtables, SST segments, read path, flush path, compaction, and COW branch storage.
- `durability-recovery.md`: WAL format and durability modes, MANIFEST state, recovery, checkpoints, and follower refresh.
- `transactions-branching.md`: OCC transaction flow, visibility rules, branch fork/merge behavior, and primitive-aware merge hooks.
- `architectural-gaps.md`: major gaps visible in the current implementation, with code evidence.
- `prioritized-fixes.md`: cross-document fix backlog ranked by architectural risk and execution order.
- `zombie-migrations-audit.md`: audit of production-grade modules and alternate stacks that are implemented but not on the live engine path.
- `vector-zombie-implementation.md`: focused note on the dormant vector-specific WAL and snapshot stack and the real vector durability path that replaced it.
- `gaps/README.md`: one detailed note per major architectural gap.
- `sections/01-core-storage-engine-lsm.md`: focused, code-derived analysis of the LSM storage engine.
- `sections/02-compaction-maintenance.md`: focused, code-derived analysis of compaction, retention, backpressure, and WAL cleanup.
- `sections/03-write-ahead-log.md`: focused, code-derived analysis of WAL layout, sync behavior, rotation, and replay.
- `sections/04-recovery-crash-safety.md`: focused, code-derived analysis of recovery order, MANIFEST state, checkpoints, subsystem hooks, and lock semantics.
- `sections/05-transactions-occ.md`: focused, code-derived analysis of OCC semantics, visibility rules, CAS, JSON conflicts, coordinator tracking, and transaction lifecycle.
- `sections/06-branch-isolation-git-like-versioning.md`: focused, code-derived analysis of COW branching, inherited layers, merge semantics, DAG/status integration, and follower behavior.

## Source roots used

- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/compaction.rs`
- `crates/storage/src/memtable.rs`
- `crates/storage/src/segment.rs`
- `crates/storage/src/block_cache.rs`
- `crates/storage/src/bloom.rs`
- `crates/storage/src/merge_iter.rs`
- `crates/storage/src/key_encoding.rs`
- `crates/durability/src/wal/writer.rs`
- `crates/durability/src/format/wal_record.rs`
- `crates/durability/src/format/manifest.rs`
- `crates/durability/src/compaction/wal_only.rs`
- `crates/durability/src/disk_snapshot/checkpoint.rs`
- `crates/concurrency/src/manager.rs`
- `crates/concurrency/src/validation.rs`
- `crates/concurrency/src/conflict.rs`
- `crates/concurrency/src/recovery.rs`
- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/database/compaction.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/branch_ops/mod.rs`
- `crates/engine/src/branch_ops/dag_hooks.rs`
- `crates/engine/src/primitives/branch/index.rs`
- `crates/core/src/branch_types.rs`
- `crates/graph/src/merge_handler.rs`
- `crates/graph/src/branch_dag.rs`
- `crates/graph/src/branch_status_cache.rs`
- `crates/graph/src/dag_hook_impl.rs`
- `crates/executor/src/handlers/branch.rs`
- `crates/vector/src/merge_handler.rs`

## Engine shape at a glance

1. `Database::open` creates directories, loads config, validates codec constraints, replays WAL through `strata_concurrency::RecoveryCoordinator`, then recovers on-disk segments and starts background work (`crates/engine/src/database/open.rs:689`, `crates/engine/src/database/open.rs:728`, `crates/engine/src/database/open.rs:839`).
2. Writes land in a per-branch lock-free memtable, then rotate into frozen memtables, then flush into L0 SST segments (`crates/storage/src/segmented/mod.rs:306`, `crates/storage/src/segmented/mod.rs:2546`, `crates/storage/src/segmented/mod.rs:2573`).
3. Reads walk the MVCC sources in order: active memtable, frozen memtables, L0, L1+, then inherited branch layers (`crates/storage/src/segmented/mod.rs:3334`).
4. OCC commit runs under a branch commit lock, writes the WAL before applying to storage, and only advances global visibility after the storage install completes (`crates/concurrency/src/manager.rs:395`, `crates/concurrency/src/manager.rs:585`).
5. Branches fork by inheriting segment snapshots and memtable state instead of copying data; merge is a three-way typed diff with optional primitive-specific handlers (`crates/storage/src/segmented/mod.rs:1500`, `crates/engine/src/branch_ops/mod.rs:1446`, `crates/engine/src/branch_ops/mod.rs:1865`).

## How to read these notes

- Start with `storage-engine.md` if you want the physical data model.
- Use `sections/01-core-storage-engine-lsm.md` if you want the deeper storage-engine assessment and feature validation pass.
- Use `sections/02-compaction-maintenance.md` for the deeper compaction, GC, TTL, backpressure, and WAL-cleanup assessment.
- Use `sections/03-write-ahead-log.md` for the deeper WAL, sync, rotation, and replay assessment.
- Use `sections/04-recovery-crash-safety.md` for the deeper recovery, checkpoint, MANIFEST, subsystem, and lock-behavior assessment.
- Use `sections/05-transactions-occ.md` for the deeper OCC, snapshot, CAS, JSON-conflict, and transaction-lifecycle assessment.
- Use `sections/06-branch-isolation-git-like-versioning.md` for the deeper branching, COW lineage, merge-base, DAG/status, and follower-mode assessment.
- Read `durability-recovery.md` next if you want crash semantics.
- Read `transactions-branching.md` for user-visible consistency and branching behavior.
- Read `architectural-gaps.md` last; it assumes the other three.
- Read `prioritized-fixes.md` after the gap notes if you want an implementation order rather than just a gap inventory.
- Read `zombie-migrations-audit.md` if you want the module-level audit of dormant or shadowed implementation stacks.
- Read `vector-zombie-implementation.md` if you want the focused breakdown of the vector crate's dormant parallel persistence stack.
- Use `gaps/` when you want the deep-dive analysis for a specific architectural weakness.
