# Detailed Gap Notes

This directory expands the high-level list in `research/core-engine/architectural-gaps.md`
into one code-derived note per major architectural gap.

Each note uses the same structure:

- what the code does today
- why that behavior creates an architectural seam
- concrete failure modes and operational consequences
- code evidence
- likely remediation direction

## Gap Inventory

1. `01-checkpoint-recovery-gap.md`
   Checkpoints are written, but restart still depends on WAL replay plus segment recovery.
2. `02-maintenance-ttl-gap.md`
   GC and TTL maintenance APIs exist, but the runtime maintenance loop is incomplete.
3. `03-wal-codec-encryption-gap.md`
   Durable encrypted operation is blocked because WAL recovery does not decode non-identity codecs.
4. `04-branch-hook-registration-gap.md`
   Primitive-aware merge semantics and branch-DAG bookkeeping depend on startup registration outside the engine core.
5. `05-default-open-subsystem-gap.md`
   `Database::open` and follower open do not recover the same subsystem set as production.
6. `06-first-opener-wins-gap.md`
   The first opener of a database path defines the live subsystem composition for the whole process.
7. `07-merge-base-externalization-gap.md`
   Correct merge-base computation for repeated merges depends on executor-supplied DAG state.
8. `08-event-merge-gap.md`
   Event logs still have no semantic branch merge and reject concurrent appends in the same space.
9. `09-follower-refresh-divergence-gap.md`
   Follower refresh can skip bad records permanently and still advance its WAL watermark.
10. `10-write-stall-defaults-gap.md`
    The L0 write-stall mechanism exists, but the default thresholds are disabled because of a deadlock concern.
11. `11-tombstone-audit-gap.md`
    The tombstone audit model exists as a library type, but it is not wired into the live engine pipeline.
12. `12-process-global-singletons-gap.md`
    Several runtime behaviors are process-global rather than database-instance-local.

## Cross-Cutting Themes

- Recovery is still fundamentally WAL-first. Checkpoints, subsystem state, and follower catch-up all inherit from that.
- Production semantics live partly above the engine crate. Subsystem composition, merge handlers, and DAG behavior are not intrinsic to `Database::open`.
- Several features exist as types or APIs without a fully operational control loop behind them. Maintenance, tombstone auditing, and some branch semantics fall into this category.
- Global process state is doing work that would normally belong to a database runtime object. That makes embedding order matter.
