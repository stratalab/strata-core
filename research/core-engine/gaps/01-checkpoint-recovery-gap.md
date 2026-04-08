# Gap 01: Checkpoints Exist, but Checkpoint-Based Recovery Does Not

## Summary

Strata can create crash-safe checkpoints, persist snapshot watermarks into `MANIFEST`,
and use those watermarks to reason about WAL compaction. But the authoritative open path
still rebuilds the database from WAL replay plus segment recovery. The checkpoint is not
loaded during normal `Database::open`.

This is a major architectural gap because the system exposes checkpoint creation as a
durability primitive, while restart semantics are still WAL-first.

## What The Code Does Today

### Checkpoint creation is real

`Database::checkpoint()`:

- flushes the WAL first
- drains in-flight commits with `quiesced_version()`
- collects storage state
- writes snapshot sections through `CheckpointCoordinator`
- persists `snapshot_id` and `snapshot_watermark` into `MANIFEST`

Code evidence:

- `crates/engine/src/database/compaction.rs:49-125`

### Recovery does not consult checkpoints

`Database::open_finish()`:

- constructs a `RecoveryCoordinator`
- replays WAL records
- then recovers SST segments from disk
- then starts the runtime

There is no checkpoint load step in that path.

Code evidence:

- `crates/engine/src/database/open.rs:689-876`

`RecoveryCoordinator` explicitly documents snapshot loading as not implemented:

- "Load snapshot (if exists) - not implemented in M2"
- `with_snapshot_path()` exists only as future-facing plumbing
- `RecoveryStats::from_checkpoint` is always false

Code evidence:

- `crates/concurrency/src/recovery.rs:9-15`
- `crates/concurrency/src/recovery.rs:28-33`
- `crates/concurrency/src/recovery.rs:69-76`
- `crates/concurrency/src/recovery.rs:308-311`

## Why This Is An Architectural Gap

The problem is not that checkpoint files are fake. They are real. The problem is that
the restart contract and the checkpoint contract are different contracts.

Today there are effectively two durability surfaces:

- a snapshot writer that produces checkpoint artifacts and MANIFEST watermarks
- a restart path that still trusts WAL replay and segment recovery as the source of truth

That mismatch creates ambiguity about what a checkpoint means:

- It is not a restart accelerator in the normal open path.
- It is not an authoritative recovery cutover point.
- It cannot safely justify WAL deletion on its own.

The system therefore has checkpoint artifacts without checkpoint-based restart semantics.

## Concrete Failure Mode

The codebase already has a regression test for the core failure mode:

1. write data
2. create a checkpoint
3. compact WAL based only on snapshot coverage
4. restart
5. lose data because open never loads the snapshot

That exact scenario is captured in `test_issue_1730_checkpoint_compact_recovery_data_loss`.

Code evidence:

- `crates/engine/src/database/tests.rs:1463-1521`

The WAL-only compactor was hardened specifically to avoid trusting snapshot coverage.
It now uses the flush watermark instead, with an inline `#1730` note.

Code evidence:

- `crates/durability/src/compaction/wal_only.rs:80-89`

## Architectural Consequences

### Restart cost does not improve the way the API suggests

Because the open path still replays WAL and recovers segments, checkpoint creation does
not currently bound restart work in the way operators usually expect from checkpoints.

### Checkpoint and compaction have an awkward relationship

Checkpointing wants to say "everything up to watermark X is durable elsewhere."
Recovery currently cannot honor that statement. So compaction has to distrust the snapshot
watermark and fall back to flush coverage.

### The durability model is harder to reason about

A reader looking only at the checkpoint code could infer:

- checkpoint creates a durable point-in-time image
- MANIFEST remembers its coverage
- restart can use that image

The open path does not make that inference true.

## Why The Existing Partial Design Is Not Enough

The current design does have useful pieces:

- checkpoint creation is quiesced correctly
- checkpoints are crash-safe on disk
- `MANIFEST` records snapshot identifiers and watermarks

But those pieces are still disconnected from the component that decides how the database
reconstitutes itself after restart.

Until the recovery coordinator can load checkpoint state first and replay only the delta
after the snapshot watermark, the checkpoint mechanism remains structurally incomplete.

## Likely Remediation Direction

Inference from the current code shape:

1. Move checkpoint loading into `RecoveryCoordinator`.
2. Make snapshot load happen before WAL replay.
3. Thread codec, database UUID, and snapshot directory through the open path in the same
   way WAL replay is threaded today.
4. Redefine WAL compaction coverage in terms of the recovery path's actual checkpoint load
   semantics, not in terms of a watermark that only the writer understands.
5. Extend restart tests to cover:
   - checkpoint-only reopen
   - checkpoint plus post-checkpoint WAL delta replay
   - crash during checkpoint
   - checkpoint recovery with non-identity codecs

## Bottom Line

Strata has checkpoint artifacts, but not checkpoint-based recovery. Until those two are
joined, checkpointing remains a secondary durability feature rather than part of the core
restart architecture.
