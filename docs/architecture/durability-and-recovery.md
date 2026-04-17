# Durability and Recovery

> **Current state as of commit `5025903e` (2026-04-17), Tranche 3 Epics 1–12 (all of T3 except this doc's own landing).** This document describes the shipped behavior at that point. Any claim about behavior that has not yet landed is labeled **[TARGET STATE]** inline; everything else describes what the code does today. If you add new target-state material, label it inline so the doc-truth regression test at `crates/engine/tests/architecture_doc_truth.rs` does not fail.

Strata's durability and recovery spine sits in three crates:

- `strata_durability` owns physical formats — WAL segments, snapshots, MANIFEST — and the codec trait.
- `strata_concurrency` owns `RecoveryCoordinator`, which plans recovery from the MANIFEST and drives snapshot install + WAL replay through callbacks.
- `strata_engine` owns the `Database` runtime — open, commit, background flush, follower refresh, shutdown — and wires the coordinator into the open path.

Nothing else writes the WAL, the MANIFEST, or snapshots, and no other crate is allowed to decide recovery ordering.

## Recovery path on open

`Database::open` walks the recovery pipeline once before the engine serves any traffic. The pipeline is the same for every `OpenSpec` mode (`primary`, `follower`, `cache`); only the post-recovery behavior differs (for example, `follower` skips `Subsystem::bootstrap` writes).

1. **Plan recovery.** `RecoveryCoordinator::plan_recovery(expected_codec_id)` reads the MANIFEST if it exists. It produces a `RecoveryPlan` containing the manifest-present flag, the codec id, optional snapshot id and watermark, the flushed-through commit id, and the database UUID. Codec validation happens here: if the MANIFEST codec id does not match the coordinator's expected codec id, `plan_recovery` short-circuits with `StrataError::Corruption { message: "codec mismatch: …" }` before any WAL byte is read. A fresh database (no MANIFEST) returns a zero-valued plan.
2. **Load snapshot.** When a codec has been installed via `RecoveryCoordinator::with_codec` and the MANIFEST records a snapshot, the coordinator reads the snapshot file, validates its magic/CRC/codec, and invokes the caller-supplied `on_snapshot` callback with the decoded `LoadedSnapshot`. The snapshot carries retention-complete state: tombstones and TTL metadata are installed verbatim alongside keys and versions, so restart does not resurrect deleted data or reset TTL expiry. When no codec is installed, snapshot load is skipped and recovery falls back to full WAL replay.
3. **Delta-WAL replay.** The coordinator streams committed WAL records one segment at a time via `WalReader::iter_all`. Records with `txn_id <= snapshot_watermark` are skipped (they are already reflected in the installed snapshot); remaining records are handed to the caller-supplied `on_record` callback in arrival order. `Database::open` supplies an `on_record` closure that applies each record to the engine-owned `SegmentedStore` using `apply_wal_record_to_memory_storage`.
4. **Partial-tail truncation.** After streaming, the coordinator truncates any partially-written bytes at the tail of the last WAL segment so the reopened WAL writer lands on a clean boundary.
5. **Sidecar rebuild.** If a segment's `.meta` sidecar is missing or fails CRC, the coordinator rebuilds it by scanning the segment header. Existing sidecars are trusted.
6. **Stats.** The coordinator returns `RecoveryStats` summarizing records replayed, records skipped below the snapshot watermark, writes and deletes applied, the max commit version, and the max txn id. The engine uses these to bootstrap the `TransactionManager` counters above the last committed state.

The coordinator does not construct storage, does not open the WAL writer, and does not start any background thread. Those are engine responsibilities so the recovery coordinator has no latent global state of its own.

## WAL write path

Commits drive a three-phase WAL sync that prevents counter-reset races:

1. **Append.** The WAL writer appends the record to the active segment buffer and returns an unsynced in-flight handle to the caller.
2. **Fsync.** A background sync loop (one thread per database) fsyncs batches of in-flight handles. Only after the fsync returns does the writer's "durable-through" counter advance.
3. **Visibility.** `Transaction::commit` blocks on its handle until the durable-through counter crosses the commit's record position, then publishes the commit via `CommitObserver`.

If the fsync fails, the writer halts: it stores the error in `bg_error`, refuses further appends, and surfaces the failure through `Database::wal_writer_health()` as `WalWriterHealth::Halted { reason, first_observed_at, failed_sync_count }`. Commits that were appended and fsynced at the point of halt observe `StrataError::DurableButNotVisible { txn_id, commit_version }` (the record is on disk but the durable-through watermark could not advance); commits arriving after the halt observe `StrataError::WriterHalted`. Both are retryable after a successful resume. The writer only clears its halt on an explicit `Database::resume_wal_writer(confirm_reason)` call, which requires a fresh sync to succeed before the health flips back to `Healthy`. There is no silent retry — operator intent is required because a halt typically indicates a full disk, permission change, or filesystem corruption that must be addressed out-of-band.

## Follower refresh

Followers receive replicated records and apply them on `Database::refresh`. The shipped path serializes refresh through a single-flight gate and advances two watermarks:

- **Received watermark** — the highest txn id accepted into the follower's local WAL.
- **Applied (contiguous) watermark** — the highest txn id for which every prior record has been successfully applied to storage and every registered `RefreshHook` has returned.

`ContiguousWatermark` is the contract: `Database::refresh` returns a `RefreshOutcome` whose applied watermark must never skip. If a `RefreshHook` returns an error, the failing record and every later record stay unreported to observers; the follower surfaces `BlockedTxn` with the offending txn id and `BlockReason` via `Database::follower_status()`. Operators skip a blocked record via `Database::admin_skip_blocked_record(txn_id, reason)`, which requires an exact txn id match (mismatch returns `UnblockError`) and writes a durable audit entry to `follower_audit.log`.

`ReplayObserver::on_replay(ReplayInfo)` fires only after the applied watermark advances past a record, so observers never see replayed state that a later hook will reject. The replay observer and the commit observer are mutually exclusive (commit observers fire on primary writes; replay observers fire on follower replay).

## Snapshots and checkpoints

Snapshots are written by the background compaction pipeline, not by commit. A snapshot is a point-in-time materialization of a branch's committed state at a specific `snapshot_watermark` txn id, written to disk in the codec's on-disk format. The MANIFEST records the snapshot id, watermark, and the flushed-through commit id, so recovery can pick up from the snapshot and only replay the delta WAL above its watermark.

Payload fidelity is retention-complete: tombstones and TTL expiry state round-trip through checkpoint install so restart preserves delete/expiry barriers. This is a deliberate Tranche 3 invariant — earlier drafts shipped a lossy checkpoint payload that required WAL replay to rebuild tombstones. That class of lossy reopen is gone.

Pre-DR-5 `.chk` fixtures (from snapshots that were written but never loaded) are still consumable: Tranche 3 reads the existing snapshot format without a migration step.

## Authoritative shutdown

`Database::shutdown` is the one authoritative close barrier. It is the only supported way to close a database that guarantees on-disk durability of in-memory state. The fixed ordering:

1. **Quiesce new work.** Flip `accepting_transactions` to `false` so `Database::begin_transaction` returns an `InvalidInput` error (`"Database is shutting down"`) and drain the background scheduler so no new transactions spawn through internal paths. On a subsequent timeout, the flag is restored so the database stays usable.
2. **Wait for idle.** Block until active transactions drain. `shutdown_with_deadline(d)` returns `StrataError::ShutdownTimeout { active_txn_count }` without forcing abort if the deadline elapses; the database stays open and the caller decides whether to abort, extend, or retry.
3. **Freeze subsystems.** Call `Subsystem::freeze()` on every registered subsystem in installation order.
4. **Drain WAL.** Block until the WAL writer reports the durable-through counter equals the last appended record position.
5. **Flush storage.** Drive a final memtable rotation and flush so the on-disk segment state reflects every durable commit.
6. **Fsync MANIFEST.** Rewrite and fsync the MANIFEST so the next open's `plan_recovery` sees a consistent checkpoint id, flushed-through commit id, and codec id.
7. **Stop background threads.** Signal the sync loop, the follower refresh loop (if any), and the compaction loop to exit, then join them.

`ShutdownTimeout` is retryable — the caller may call `shutdown_with_deadline` again after intervening work completes. There is no warn-and-proceed fallback: shutdown either completes every step or returns the first failure with the database still open and its state unchanged.

**[TARGET STATE]** Forced transaction abort on `ShutdownTimeout` is not provided. Tranche 3 deliberately returns the timeout and leaves retry/abort decisions to callers; a future product-layer policy may layer forced abort on top.

## Codec uniformity

One `CompatibilitySignature` describes the on-disk encoding for a database and is shared by the WAL, snapshots, and the MANIFEST. The codec id in the signature is the single fact that recovery, snapshot load, and runtime reuse (`OPEN_DATABASES` registry) all validate against. There is no separate "WAL codec" vs. "snapshot codec" — a mismatched codec fails at `plan_recovery` before any WAL byte is read and before any cached `Database` handle is reused.

### WAL codec threading (Tranche 3 Epic 12)

Every WAL read path decodes through the configured codec. A database configured with `codec = "aes-gcm-256"` + `durability = "standard"` writes encrypted WAL records and recovers them on restart without special-case rejection. Four reader construction sites are threaded with the codec: `RecoveryCoordinator::recover` at open time, `Database::refresh` on follower replay via a cached `Database.wal_codec` field, `WalReader::iter_all` for segment-at-a-time streaming, and the writer's `rebuild_meta_for_segment` for metadata-sidecar rebuilds on writer reopen.

Each WAL record on disk uses the v3 per-record envelope:

```
[u32 outer_len] [u32 outer_len_crc] [ codec.encode(WalRecord::to_bytes()) ]
```

`outer_len_crc = crc32(&outer_len.to_le_bytes())` — mirrors the inner `LenCRC` pattern so a torn write to the outer length field is detectable before the reader tries to consume `outer_len` bytes. For the identity codec the envelope adds 8 bytes per record; for `aes-gcm-256` the codec itself adds 28 bytes (12-byte nonce + 16-byte auth tag) around the same inner record.

`SEGMENT_FORMAT_VERSION` is 3 as of Tranche 3 Epic 12. Pre-v3 segments surface as `StrataError::LegacyFormat { found_version, hint }` on open — the hint names the operator action (delete `wal/` and reopen). The lossy-recovery wipe path does not bypass format incompatibility; both strict and lossy open reject pre-v3 segments with the same typed error. Legacy-format is classified separately from the whole-DB wipe triggered by codec-decode failures (see below).

Codec decode failures — wrong key, AES-GCM auth-tag mismatch, corrupt ciphertext — surface as `WalReaderError::CodecDecode { offset, detail }` at the reader layer and `StrataError::CodecDecode` at the engine boundary. Under `allow_lossy_recovery = true` the decode failure routes to the whole-DB wipe-and-reopen path (same path used for other recoverable corruption) and the resulting `LossyRecoveryReport` carries `error_kind = LossyErrorKind::CodecDecode`, letting operators distinguish wrong-key scenarios from raw-byte storage corruption programmatically.

A follower opening against a database without a persisted MANIFEST falls back to `get_codec(&cfg.storage.codec)` so encrypted WAL-only recovery works even before the first checkpoint has written a MANIFEST.

## Insertion points for callers

Callers that extend the recovery path work through a small set of engine-owned traits and callbacks:

- **`CommitObserver`** — fires after a commit is visible on the primary. Used by triggers, intelligence subsystems, and downstream indexers.
- **`ReplayObserver`** — fires on a follower after the applied watermark advances. Never fires on the primary.
- **`RefreshHook`** (in-tree; not part of the public D4 surface) — fallible per-record follower hook. A hook failure blocks the contiguous watermark.
- **`Subsystem::freeze`** — per-subsystem shutdown hook, invoked by `Database::shutdown` before the MANIFEST is fsynced.

`RecoveryCoordinator::with_codec` is internal wiring, not a user-facing insertion point: `Database::open` installs the codec that matches the MANIFEST, and external callers never construct a coordinator directly.

## What this document does not cover

- Branch-aware storage retention above the recovery fidelity delivered in Tranche 3. That contract is defined by the branch-primitives scope (Tranche 4).
- Product-layer shutdown policy such as forced abort on timeout. See `product-control-surface-scope.md` for how `ControlRegistry` classifies shutdown knobs.
- Compaction scheduling and segment-merge policy beyond what is needed for recovery correctness. That lives in the storage and compaction design docs.

## Where to look in the code

| Concern | Crate | Entry point |
|---------|-------|-------------|
| Recovery planning and driver | `strata_concurrency` | `crates/concurrency/src/recovery.rs` |
| WAL reader and segment format | `strata_durability` | `crates/durability/src/wal/` and `crates/durability/src/format/` |
| Snapshot reader/writer | `strata_durability` | `crates/durability/src/disk_snapshot/` |
| MANIFEST manager | `strata_durability` | `crates/durability/src/format/manifest.rs` |
| Database layout | `strata_durability` | `crates/durability/src/layout.rs` |
| Open path | `strata_engine` | `crates/engine/src/database/open.rs` |
| WAL writer halt/resume | `strata_engine` | `crates/engine/src/database/mod.rs` (`wal_writer_health`, `resume_wal_writer`) |
| Follower refresh and contiguous watermark | `strata_engine` | `crates/engine/src/database/lifecycle.rs` (`refresh`, `follower_status`, `admin_skip_blocked_record`); machinery in `refresh.rs` |
| Shutdown barrier | `strata_engine` | `crates/engine/src/database/lifecycle.rs` (`shutdown`, `shutdown_with_deadline`) |
