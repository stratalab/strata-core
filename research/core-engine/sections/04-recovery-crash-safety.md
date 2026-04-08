# Recovery & Crash Safety: Focused Analysis

This note is derived from the code, not from the existing docs.
Scope for this pass:

- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/mod.rs`
- `crates/engine/src/database/compaction.rs`
- `crates/engine/src/database/builder.rs`
- `crates/engine/src/recovery/mod.rs`
- `crates/engine/src/recovery/subsystem.rs`
- `crates/concurrency/src/recovery.rs`
- `crates/durability/src/format/manifest.rs`
- `crates/durability/src/format/watermark.rs`
- `crates/durability/src/disk_snapshot/writer.rs`
- `crates/durability/src/disk_snapshot/reader.rs`
- `crates/durability/src/disk_snapshot/checkpoint.rs`
- `crates/durability/src/recovery/coordinator.rs`
- `crates/durability/src/database/handle.rs`
- `crates/durability/src/database/paths.rs`
- `crates/durability/src/coordination.rs`

## Executive view

Recovery is not one coherent stack today.
It is two partially overlapping stacks:

- the engine runtime stack, which uses `strata_concurrency::RecoveryCoordinator` to replay WAL from `wal/`, then separately loads `segments/`
- the durability-layer stack, which uses `MANIFEST + snapshot + WAL` recovery from `WAL/` and `SNAPSHOTS/`

The default engine open path is coherent for WAL-first restart:

1. acquire a database lock
2. replay WAL into `SegmentedStore`
3. load or create `MANIFEST`
4. reopen the WAL writer
5. recover on-disk segments
6. run subsystem recovery hooks

That is real crash recovery.
But it is not checkpoint-based recovery.

The most important gaps in this section are:

- checkpoint restore is not part of `Database::open`
- the engine and durability crates have drifted to different on-disk layouts
- snapshot codec plumbing is inconsistent and mostly inert
- `MANIFEST` watermarks are persisted but not used by the default engine recovery path
- follower open is documented as read-only but still routes through WAL-tail truncation
- subsystem crash safety depends on caller-supplied registration and first-opener-wins behavior

## Inventory validation

| # | Feature | Verdict | Code-derived note |
|:-:|---------|---------|-------------------|
| 34 | Automatic WAL Recovery | Mostly true | Disk-backed primary and follower opens replay WAL automatically through `strata_concurrency::RecoveryCoordinator`. The engine then separately loads SST segments from disk. This is WAL-first recovery, not checkpoint-first recovery (`crates/engine/src/database/open.rs`, `crates/concurrency/src/recovery.rs`). |
| 35 | MANIFEST | Partially true | `MANIFEST` is a real crash-safe metadata file with UUID, codec, active WAL segment, snapshot info, and flush watermark. But the default engine open path loads it after WAL replay and uses it mainly for UUID/codec validation, not as the main recovery plan (`crates/durability/src/format/manifest.rs`, `crates/engine/src/database/open.rs`). |
| 36 | Flushed-Through Commit ID | Partially true | `flushed_through_commit_id` is real and is updated after flush to support WAL cleanup. But the default engine recovery path does not use it for delta-only WAL replay on open (`crates/durability/src/format/manifest.rs`, `crates/engine/src/database/transaction.rs`, `crates/concurrency/src/recovery.rs`). |
| 37 | Snapshot Watermark | Partially true | `SnapshotWatermark` is real, and the durability-layer recovery coordinator uses it to skip older WAL records. The engine open path does not use that coordinator, so snapshot watermark does not drive normal startup recovery today (`crates/durability/src/format/watermark.rs`, `crates/durability/src/recovery/coordinator.rs`, `crates/engine/src/database/open.rs`). |
| 38 | Lossy Recovery Mode | True, with strong semantics | `allow_lossy_recovery` enables lossy WAL scanning and, if recovery still fails, allows the database to start with empty state. That is broader than "skip a few bad records" semantics (`crates/concurrency/src/recovery.rs`, `crates/engine/src/database/open.rs`). |
| 39 | Recovery Participant System | True, but current code lives elsewhere | The current mechanism is the `Subsystem` trait under `engine/recovery/`, not `participant.rs`. Recovery hooks are real, but composition is external to the default engine open path (`crates/engine/src/recovery/mod.rs`, `crates/engine/src/recovery/subsystem.rs`). |
| 40 | Subsystem Freeze Hooks | True, with best-effort behavior | Registered subsystems get `freeze()` calls on shutdown/drop in reverse order. Explicit shutdown returns the first error after trying all hooks; drop logs failures and keeps going (`crates/engine/src/database/mod.rs`, `crates/engine/src/database/lifecycle.rs`). |
| 41 | Disk Snapshots | Partially true | Snapshot writer, reader, format, CRC validation, and atomic rename are all real. But the default engine open path does not restore from snapshots, and there is no active runtime snapshot deserialization path in the engine (`crates/durability/src/disk_snapshot/writer.rs`, `crates/durability/src/disk_snapshot/reader.rs`). |
| 42 | Checkpoint Coordinator | Partially true | The engine can create checkpoints and persist snapshot watermark in `MANIFEST`. But checkpoint cleanup is unused, restore is not wired into engine open, and the codec path is inconsistent (`crates/durability/src/disk_snapshot/checkpoint.rs`, `crates/engine/src/database/compaction.rs`). |
| 43 | Exclusive Lock File | Mostly true | Primary open acquires `.lock` with `fs2::try_lock_exclusive` and also uses the process-local `OPEN_DATABASES` registry. Followers and cache databases skip this lock. This is advisory process coordination, not cluster coordination (`crates/engine/src/database/open.rs`). |

## Real execution model

### 1. There are two recovery stacks

The codebase currently has two different recovery architectures.

The engine runtime uses:

- `crates/concurrency/src/recovery.rs`
- WAL directory directly
- `SegmentedStore::with_dir(...)`
- replay of committed transaction payloads into storage

The durability layer uses:

- `crates/durability/src/recovery/coordinator.rs`
- `MANIFEST`
- snapshot discovery and loading
- WAL replay after watermark
- `.meta` repair for closed WAL segments

The default engine open path uses the first one, not the second one.
`DatabaseHandle` in `strata_durability` says it is the lower-level persistence API used by the engine layer, but there are no engine call sites using `DatabaseHandle`.

That matters because the durability-layer stack is the only stack that actually models:

- manifest-driven recovery planning
- snapshot loading
- replay after snapshot watermark
- snapshot-path existence validation
- conservative watermark reconciliation

The engine stack does not.

### 2. Default primary open is WAL-first, then segment recovery, then subsystem recovery

The real primary open order in `Database::open_finish()` is:

1. validate the configured codec exists
2. reject non-identity codecs when WAL durability is enabled
3. create `wal/` and `segments/`
4. run `strata_concurrency::RecoveryCoordinator::recover()`
5. load or create `MANIFEST`
6. reopen `WalWriter`
7. construct coordinator and storage wrappers
8. run `recover_segments_and_bump()`
9. build `Database`
10. schedule background compaction

Then `open_internal_with_subsystems()` does:

1. repair space metadata
2. run each registered subsystem's `recover(&db)`
3. install the subsystem list for later freeze-on-drop

So the real recovery sequence is:

- WAL replay first
- segment discovery second
- subsystem rebuild third

Checkpoint restore is not on this path.

### 3. `MANIFEST` is durable, but not the main engine recovery plan

`ManifestManager` is solid as a persistence primitive.
It uses write-fsync-rename and parent-directory sync.

The file really stores:

- `database_uuid`
- `codec_id`
- `active_wal_segment`
- `snapshot_watermark`
- `snapshot_id`
- `flushed_through_commit_id`

But the engine mostly uses it in narrow ways:

- at open, to validate UUID/codec compatibility after WAL replay
- after flush, to persist the flush watermark used by WAL cleanup
- after checkpoint, to persist snapshot watermark and snapshot ID

Important nuance:

- `active_wal_segment` is not continuously maintained by the normal write path
- the obvious runtime update is in the checkpoint/compaction path's `load_or_create_manifest()`

So the inventory description overstates how central `MANIFEST` currently is to startup recovery.
It is durable metadata, but not the authoritative engine recovery plan.

### 4. Snapshot and checkpoint support is real on disk, but mostly detached from engine startup

The snapshot writer is crash-safe in the narrow filesystem sense:

1. write `.tmp`
2. `sync_all()`
3. atomic rename to `snap-XXXXXX.chk`
4. sync parent directory

The reader is also real:

- validates header
- validates magic/version
- validates codec ID string
- validates CRC
- parses typed sections

The engine checkpoint path is also real:

1. compute a quiesced watermark
2. collect primitive snapshot data
3. create `snapshots/`
4. create a `CheckpointCoordinator`
5. write the snapshot
6. update `MANIFEST` snapshot watermark/id

But restore is not wired into `Database::open`.

The cleanest proof is in `crates/concurrency/src/recovery.rs`:

- the docs still say "Load snapshot (if exists) - not implemented in M2"
- `RecoveryStats.from_checkpoint` is explicitly documented as always `false`

So the code has checkpoint creation, but not checkpoint-based engine recovery.

### 5. Snapshot codec handling is inconsistent

This is a deeper problem than just "restore not wired yet."

`CheckpointCoordinator` does two different things with codecs:

- it passes the configured codec to `SnapshotWriter`
- it hardcodes `IdentityCodec` for `SnapshotSerializer`

That means primitive section payloads are serialized with identity encoding even when the configured snapshot codec is something else.

At the file level:

- `SnapshotWriter` stores the codec ID in the snapshot header
- but it does not call `codec.encode()` on section bytes before writing them

At read time:

- `SnapshotReader` validates that the codec ID string matches
- but it does not call `codec.decode()` on section bytes

And there are no engine runtime call sites deserializing snapshot sections back into engine state.

So snapshot codec support is not just incomplete.
It is internally inconsistent:

- the header carries a codec identity
- the section serializer is pinned to identity
- the file writer/reader do not apply whole-file codec transforms

This makes the current checkpoint stack structurally unready for real encrypted or non-identity snapshot recovery.

### 6. Delta replay is only partially real

The inventory claims:

- `flushed_through_commit_id` enables delta-only WAL replay

That is only partially true in the current engine.

What does happen:

- after flush, the engine computes a global watermark across branches with SSTs
- it persists that watermark in `MANIFEST`
- `WalOnlyCompactor` uses it to remove covered WAL segments

What does not happen in the default engine recovery path:

- WAL replay is not planned from `MANIFEST`
- `flushed_through_commit_id` is not consulted by `strata_concurrency::RecoveryCoordinator`
- open still replays the surviving WAL and only then loads SST segments

So the current design relies on WAL truncation to shrink replay work, not on manifest-guided delta recovery during startup.

### 7. Lossy recovery is broader than the inventory summary suggests

There are two layers of "lossy" behavior:

1. `WalReader::with_lossy_recovery()` scans past corrupted regions instead of erroring immediately
2. `Database::open` / follower open can still fall back to `RecoveryResult::empty()` when recovery fails and `allow_lossy_recovery` is enabled

That second behavior is operationally important.
This mode does not just skip bad bytes.
It can discard recovered state and start from empty.

So the feature is real, but the actual behavior is stronger and riskier than a casual reading suggests.

### 8. Subsystem recovery and freeze are real, but composition lives above the engine

The current recovery-participant abstraction is the `Subsystem` trait.

Its lifecycle is:

- `recover()` after storage recovery on open
- `freeze()` during shutdown/drop in reverse order

This mechanism is real and useful.
But it is not intrinsically complete.

The default `Database::open` path hardcodes only:

- `SearchSubsystem`

Vector recovery is only available through:

- `DatabaseBuilder`
- executor-owned composition above the engine crate

That means recovery completeness for non-core subsystems depends on how the caller opens the database.

The registry contract makes this more brittle:

- `open_internal_with_subsystems()` returns an existing `Arc<Database>` unchanged if the path is already open
- so a later caller with a richer subsystem list does not get those recovery hooks retroactively applied

This is part of the crash-safety story because subsystem persistence and rebuild correctness are caller-composed, not intrinsic to the database open path.

### 9. Freeze hooks are best-effort, and shutdown/drop semantics differ

`run_freeze_hooks()` tries all registered subsystems in reverse order and remembers the first error.

On explicit `shutdown()`:

- WAL flush happens first
- then freeze hooks run
- the first freeze error is returned after all hooks are attempted

On `Drop for Database`:

- WAL flush happens
- freeze errors are only logged
- drop continues

So subsystem persistence is not part of one atomic crash-safe transaction boundary.
It is a best-effort post-flush phase.

That may be acceptable for caches and rebuildable indexes.
It is much weaker if any subsystem depends on freeze artifacts for correct or fast reopen.

### 10. Exclusive locking is real for primaries, but not universal

Primary open does two things:

- process-local dedupe via `OPEN_DATABASES`
- advisory filesystem locking on `.lock`

Followers skip the database lock entirely.
Cache databases also skip it.

This means the "exclusive lock file" is specifically a primary-open guard.
It is not the same thing as the WAL-level coordination primitives in `strata_durability::coordination`, which use `.wal-lock`.

## Main architectural gaps in this section

### 1. Checkpoint restore is not wired into `Database::open`

This is the central gap.

The durability layer has a snapshot-aware recovery coordinator.
The engine open path does not use it.
Instead, engine open still uses the concurrency-layer WAL coordinator whose own docs state checkpoint recovery is not implemented and whose stats always report `from_checkpoint = false`.

Practical result:

- checkpoints can be created
- `MANIFEST` can record them
- startup still behaves as WAL replay plus segment scan

### 2. The engine and durability crates have diverged on on-disk layout

The durability layer expects:

- `WAL/`
- `SNAPSHOTS/`
- `DATA/`

The engine runtime uses:

- `wal/`
- `segments/`
- `snapshots/`

That is not a cosmetic difference on case-sensitive filesystems.
It means the lower-level durability abstractions are not directly pointed at the same physical layout the engine actually produces.

This drift explains why the engine does not route through `DatabaseHandle` or the durability recovery coordinator today.
It is a genuine architectural split, not just duplicated code.

### 3. Snapshot codec plumbing is structurally incomplete

The checkpoint stack currently carries codec identity in metadata without a coherent end-to-end codec story.

Problems:

- `CheckpointCoordinator` serializes primitive sections with `IdentityCodec`
- `SnapshotWriter` stores the configured codec ID but does not encode section bytes
- `SnapshotReader` validates codec ID but does not decode section bytes
- no runtime restore path exercises snapshot deserialization back into engine state

This makes checkpoint recovery materially unready for encrypted or non-identity snapshot storage.

### 4. `MANIFEST` watermarks are persisted, but the default engine recovery path does not consume them

`snapshot_watermark` and `flushed_through_commit_id` both exist for real reasons.
But the engine's normal open path does not use either one to plan startup recovery.

Current state:

- snapshot watermark helps the durability-layer recovery path
- flush watermark helps WAL compaction
- engine open ignores both and replays WAL directly

So the manifest is not yet acting as the single authoritative recovery plan for the runtime engine.

### 5. Follower recovery is not actually read-only

`acquire_follower_db()` is documented as:

- read-only
- no truncation
- no file writes

But it uses the same `strata_concurrency::RecoveryCoordinator::recover()` as the primary open path.
That helper unconditionally calls `truncate_partial_tail()` after replay.

So follower open can still mutate the WAL by truncating a partial tail.
That is a real contract mismatch in a crash-safety-sensitive path.

### 6. Subsystem crash safety is caller-composed and first-opener-wins

The `Subsystem` model is sound as an extension point.
The architectural gap is that the engine does not define the complete production subsystem set itself.

Consequences:

- default open only installs `SearchSubsystem`
- vector recovery depends on builder/executor composition
- opening a path once fixes the subsystem set for that in-process database instance

That makes full recovery behavior depend on external composition order, not just on the persisted database state.

### 7. Snapshot temp-file cleanup exists but is not on the active runtime path

`SnapshotWriter::cleanup_temp_files()` and `CheckpointCoordinator::cleanup()` are implemented.
There are no runtime call sites using them during engine open or recovery.

This is not as severe as the gaps above, but it is another sign that the snapshot stack is only partially integrated into the real recovery lifecycle.

## Bottom line

The crash-safety foundation is stronger than a quick read suggests:

- WAL records are durable and checksummed
- `MANIFEST` persistence is crash-safe
- snapshot files are written atomically
- subsystem hooks exist
- primary opens are advisory-locked

But the recovery architecture is still split.

Today, the live engine is fundamentally a:

- WAL replay engine
- plus segment discovery
- plus optional subsystem rebuild

Checkpoint recovery, manifest-driven planning, and snapshot-based restart are present as infrastructure, not as the default engine reality.
