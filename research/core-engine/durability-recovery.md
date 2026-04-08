# Durability And Recovery

This is the crash-safety path as implemented today.

## 1. WAL is the first durable write target

The engine uses a segmented WAL in `crates/durability/src/wal/writer.rs`.
Key properties:

- auto-rotating segment files
- three durability modes: `Cache`, `Standard`, `Always`
- `Standard` mode fsync runs in the background, outside the write lock
- record-level CRC coverage in `crates/durability/src/format/wal_record.rs`
- sidecar metadata files for fast segment coverage checks in `crates/durability/src/format/segment_meta.rs`

Commit order is intentionally WAL-first, storage-second.
The transaction manager writes the WAL entry before applying the write set to storage so replay can rebuild committed state after a crash.

## 2. `Database::open` is a two-stage recovery path

The disk-backed open path is `Database::open_finish` (`crates/engine/src/database/open.rs:689`).
At a high level it does this:

1. validate the configured storage codec
2. reject non-identity codec when WAL durability is enabled (`crates/engine/src/database/open.rs:705`)
3. build a `strata_concurrency::RecoveryCoordinator`
4. replay WAL into a `SegmentedStore` (`crates/engine/src/database/open.rs:728`)
5. recover persisted SST segments and bump the coordinator's version floor (`crates/engine/src/database/open.rs:839`)
6. open the WAL writer for future appends
7. start background compaction in case recovery rebuilt a backlog

The crucial detail is the split between logical and physical recovery:

- WAL replay reconstructs committed writes into storage state
- segment recovery reattaches already-flushed SST files and inherited-layer manifests

## 3. Recovery is still WAL-centric

The recovery coordinator in `crates/concurrency/src/recovery.rs` explicitly documents that snapshot-based recovery is not implemented:

- procedure comment: `load snapshot (if exists) - not implemented in M2` (`crates/concurrency/src/recovery.rs:11`)
- `with_snapshot_path` exists but is marked as future extensibility (`crates/concurrency/src/recovery.rs:69`)
- `RecoveryStats::from_checkpoint` is documented as always false today (`crates/concurrency/src/recovery.rs:308`)

So the authoritative recovery model is:

- replay WAL records
- then reattach SST segments

It is not:

- load checkpoint snapshot
- then replay only newer WAL

That distinction drives the biggest gap in the current design.

## 4. MANIFEST is the cross-run metadata anchor

`MANIFEST` stores database-wide metadata in `crates/durability/src/format/manifest.rs`, including:

- database UUID
- active WAL segment
- configured codec id
- snapshot watermark
- flushed-through commit id

The open path uses it to validate the codec and continue the same database identity across restarts (`crates/engine/src/database/open.rs:761`).

The flush path updates `flushed_through_commit_id` through `Database::update_flush_watermark` (`crates/engine/src/database/transaction.rs:452`).
That watermark is the durability signal that some committed versions are now safely covered by SST segments rather than by WAL replay.

## 5. Checkpointing exists, but open does not consume it

`Database::checkpoint` in `crates/engine/src/database/compaction.rs:49` does real work:

- flushes the WAL first
- quiesces commits so the watermark is stable
- gathers primitive state from storage
- writes a crash-safe snapshot under `snapshots/`
- updates MANIFEST with `snapshot_id` and `snapshot_watermark`

That makes checkpoints real persisted artifacts, not stubs.
But current `Database::open` does not load them.

## 6. WAL compaction is guarded by flush coverage, not snapshot coverage

`WalOnlyCompactor` now computes its effective watermark from the flush watermark, with an explicit comment referencing issue `#1730` (`crates/durability/src/compaction/wal_only.rs:80`).
That is why `compact()` can no longer safely assume that a snapshot watermark is enough.

From the engine side:

- `checkpoint()` says snapshots can cover WAL (`crates/engine/src/database/compaction.rs:40`)
- `compact()` delegates to the WAL compactor (`crates/engine/src/database/compaction.rs:137`)
- the compactor itself refuses to trust snapshot-only coverage because open-path recovery does not load snapshots

This is a deliberate safety patch around a deeper architectural mismatch.

## 7. Follower mode tails the WAL

Follower instances are read-only replicas that refresh from new WAL records.
`Database::refresh` in `crates/engine/src/database/lifecycle.rs`:

- reads WAL records after a stored txn watermark
- replays them into storage with original timestamps and TTL
- updates search and extension hooks best-effort

This is not snapshot shipping or segment replication.
Follower freshness comes from WAL tailing.

One important limitation is that follower refresh is intentionally best-effort on a per-record basis.
If a record fails payload decode or storage apply, the code logs and skips that record, then continues.
The same loop still advances `max_txn_id`, and the follower stores that watermark at the end of refresh (`crates/engine/src/database/lifecycle.rs:161`, `crates/engine/src/database/lifecycle.rs:198`, `crates/engine/src/database/lifecycle.rs:350`).

That means skipped records are not retried on the next refresh cycle.
In failure cases, follower mode can permanently diverge rather than remain only temporarily stale.

## 8. Recovery safety boundaries

The current implementation is strong in these areas:

- committed versions are replayed exactly, not revalidated
- partial WAL tails are truncated on reopen
- segment manifests prevent orphan SST resurrection
- directory fsync is used for WAL segment creation durability
- lossy recovery is explicit and opt-in

The current implementation is weak in these areas:

- checkpoints are not part of the authoritative recovery chain
- durable encrypted databases are blocked by WAL codec limitations
- follower refresh can permanently advance past skipped WAL records
- some secondary indexes and primitive hooks rely on best-effort replay/update behavior
