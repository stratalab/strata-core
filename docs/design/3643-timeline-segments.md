# #3643: sealed timeline segments

Status: **IMPLEMENTING, 2026-09-28** (slice 1 #3654; slice 2 in review) (milestone 1.2.6; follows #3596, and is the P1 finding of the external #3596 review). Direction chosen with the user on 2026-09-28: sealed segment objects. The rejected alternative was a separate, larger timeline decode bound. Every symbol below was verified on `main` @ `551bb800`.

## 1. Problem

A checkpoint snapshot carries the database's retained commit timeline, one entry per retained commit:

- **Format.** Kind-3 section, `format/snapshot_timeline.rs`. Each entry is 24 bytes: `commit_version`, `commit_timestamp`, `committed_at`.
- **Sole durable record.** Since W3.1c, commits no longer write timeline rows. Once the WAL is truncated behind a checkpoint, this section is the **only durable record** of retained history. It is what `as_of` by timestamp and by wall-clock instant resolve against.
- **Never pruned.** No retention policy shrinks it (MVCC-009: the timeline minimum is never pruned), so under the default `KeepAll` it grows with every commit.

Three defects follow.

1. **The ceiling (#3643).**
   - The section counts against `MAX_MATERIALIZED_SNAPSHOT_PAYLOAD_BYTES` (64 MiB, DUR-017), and flushing cannot shrink it.
   - At about 2.8 M retained commits across branches, every checkpoint defers with `DeferredDeltaExceedsCap`, and the flush-and-retry chain never converges.
   - The snapshot watermark then freezes, the WAL is never truncated, and a clean close cannot checkpoint.
   - For scale: the field database behind #3596 holds 1.89 M events, which is about two thirds of the way to the ceiling.
2. **Rewrite cost.** Every checkpoint re-encodes the whole timeline: O(history) bytes written per checkpoint, however small the delta.
3. **Fork duplication.** A forked child copies its parent's pre-fork entries into its own index (`seed_child_timeline_from_parent`), and every checkpoint writes each copy again. A fork chain repeats the shared prefix once per descendant.

## 2. Design

The timeline is stored as immutable, independently bounded **segment objects**. The snapshot keeps only a small, bounded **reference** to each.

### 2.1 Chunking

- A branch's retained entries, in version order, are cut into fixed **chunks** of `TIMELINE_CHUNK_ENTRIES = 65,536` entries, about 1.5 MiB each.
- Chunk `k` holds entries `[k·N, (k+1)·N)` of that branch's list.
- The last, partial chunk is the branch's **tail**.
- Chunks are aligned by entry count from the start of the branch's history. A child forked at version `F` has exactly its parent's entries `≤ F` as a prefix, so every *full* chunk wholly at or below `F` is byte-identical to the parent's (§2.4).

### 2.2 Segment object (new object family `timeline`)

- **Name.** `timeline/<16-hex sealing snapshot id>/<16-hex ordinal>`: the id of the checkpoint that first wrote it, plus an ordinal within that checkpoint. The naming is what makes garbage collection mirror the snapshot rules (§2.6).
- **Layout.** A 32-byte header, then the entries, then a CRC32 footer over everything before it. Entry encoding is identical to kind 3.
  - Header fields: magic `TLSG`, format version 1, `entry_count u32`, `first_version u64`, `last_version u64`, and reserved zeroes.
- **Validation.** Entries must be strictly ascending, nonzero, and bounded by the header, with `entry_count ≤ TIMELINE_CHUNK_ENTRIES`. Decode fails closed on any violation.
- **Bounded decode.** One segment's decode is bounded by its own size, well under the payload cap. DUR-017 holds per object, which strengthens it.
- **Write.** Segments are written with `publish_durable_replace`: temp file, fsync, rename, then a directory fsync. Replace is safe because a segment is sealed under an id above the live snapshot (`validate_snapshot_id_advances`), so no live reference can name it yet, and it is required because a checkpoint that failed after writing segments retries under the same id. All of a checkpoint's segments are durable **before** its snapshot is published.

### 2.3 Snapshot reference section (new kind 5)

- **Shape.** `SNAPSHOT_TIMELINE_SEGMENTS_SECTION_KIND = 5` holds per-branch groups: `branch_id`, then a ref list.
  - Each ref is 40 bytes: `sealing_snapshot_id u64`, `ordinal u32`, `entry_count u32`, `first_version u64`, `last_version u64`, `crc32 u32`, `reserved u32`.
- **Rules.** Within a group, refs are contiguous and strictly ascending. Every ref except the last covers exactly `N` entries, and the last ref (the tail) covers `1..=N`. Groups are ascending and unique by `branch_id`, like kind 4.
- **Size.** At 40 bytes per 65,536 commits, a billion retained commits cost about 600 KiB of references.
- **Kind 3.** It is no longer written. It stays decodable (as kind 2 already is), so a database checkpointed by an earlier build recovers from its kind-3 section and writes segments at its next checkpoint. No migration step is needed.
- **Cap.** The payload gate counts the reference section like any other. The timeline can no longer push a checkpoint over the cap.

### 2.4 Checkpoint writer

For each active branch whose index is complete at the checkpoint's visible version:

1. The in-memory index carries `sealed: Vec<SegmentRef>`, the refs of its full chunks already durable. A full chunk is sealed **once** and re-referenced by every later checkpoint.
   - The index adopts refs **only after the checkpoint that wrote them completes**, meaning after the manifest re-points to its snapshot. Recovery seeds `sealed` from the attested snapshot.
   - So every ref a checkpoint re-uses is referenced by the live snapshot at that moment. This is what makes the `Superseded` rule of §2.6 safe against a checkpoint that wrote segments and then deferred or failed: those segments are never re-used, and they are reclaimed.
2. New full chunks since the last checkpoint are written as segments. **Dedup (slice 3):** before writing a chunk, the writer looks it up in the refs of the manifest-live snapshot, keyed by `(first_version, last_version, entry_count, crc32)`, and re-references a match.
   - This is how a fork shares its parent's chunks without tracking lineage.
   - It is sound because commit versions are database-global (l7 §"Commit versions are monotonically increasing"; one runtime-wide counter). An entry with a given version is the same commit fact on every branch. Matching version ranges and counts therefore name the same commits, and the CRC guards the one field that can differ, `committed_at` (known versus unknown).
3. The tail, if any, is written as a fresh segment on every checkpoint whose tail changed. That bounds the per-branch timeline write to about 1.5 MiB plus new full chunks, where today it is the whole history.
4. The snapshot's kind-5 section lists every branch's refs.

A checkpoint that defers or fails leaves its freshly written segments unreferenced; they are reclaimed as in §2.6.

### 2.5 Recovery

1. The manifest-attested snapshot's kind-5 section yields each branch's refs.
2. Segments load one at a time. Each is checked against its ref (`entry_count`, the version range, the CRC) and for contiguity with the previous ref. The last entry must not exceed the snapshot watermark. A segment shared by several branches is read once.
3. The branch index is seeded exactly as from a kind-3 group, and `sealed` is set to the full-chunk refs so the next checkpoint reuses them. WAL replay then appends entries above the watermark, unchanged.
4. **A missing or corrupt segment** mirrors the table-object pattern (`table_read_error`):
   - Under `AllowExplicitLossyFallback`, it records `RecoveryFaultKind::MissingSnapshotObject` for the branch (DataLoss health). The segment is part of the snapshot family, and reusing the existing kind keeps the fault vocabulary, and everything that maps it, unchanged. The branch index stays **incomplete**, so timestamp and wall-clock `as_of` refuse instead of resolving against a hole (DUR-015: never a wrong answer). Version-addressed reads are unaffected.
   - Under strict recovery, it is a corruption error.
5. Fork recovery (`seed_forked_branch_timelines_from_parents`) is unchanged. A fork with its own group seeds from its refs.

### 2.6 Reclaim

Segments join the snapshot prune, under the same `Complete` retention proof (`build_retention_proof_from_facts`):

- **`Superseded`**, after every completed checkpoint and at close: delete a segment when it is **unreferenced by the manifest-live snapshot and its sealing snapshot id is below the live id**.
  - This is the snapshot rule verbatim. A checkpoint in flight seals under an id above the live one, so its new segments are never candidates.
  - Chunks shared across branches, and chunks re-referenced from older checkpoints, stay live because the live snapshot references them. Reachability is over every branch's refs in one snapshot, which satisfies the COW reachability requirement across all branches.
- **`ReconcileToAttested`**, at open, when no publish is in flight: delete every segment the attested snapshot does not reference. This reclaims crash orphans (segments whose snapshot never became live).
- **Footprint.** No wire change. The Audit tier counts live segments inside the live-snapshot family bytes, and unreferenced segments inside `superseded_snapshot_bytes`. Documented as "snapshot family = snapshot objects + timeline segments".

### 2.7 What does not change

- Retention and MVCC-009: the timeline is still never pruned.
- The in-memory index and its exactness contract.
- WAL replay.
- The kind-4 durable-base section.
- The payload cap's value.

The residual unbounded quantity is the in-memory index, about 24 bytes per retained commit. It is the same as today and is inherent to a never-pruned timeline under `KeepAll`. It is not a decode-bound violation: each object's decode is bounded, and the index is the runtime's own state.

## 3. Invariants

| ID | Effect |
|---|---|
| DUR-017 | **Strengthened.** Every durable decode is bounded per object, and the timeline no longer counts toward the snapshot cap. |
| DUR-019 | Snapshot-family reclaim covers segments, with the same proof and the same `Superseded` / `ReconcileToAttested` rules. |
| MVCC-009 | Holds. Timeline history is never dropped, and a lost segment refuses `as_of` instead of resolving wrongly. |
| COW-001 | Holds. Shared chunks are reachable through every referencing branch in the live snapshot. |
| DUR-010 / ARCH-008 | Holds. Per-branch recovery is unchanged apart from the timeline source. |
| DUR-015 | Holds. A missing segment is a DataLoss fault with an incomplete index, never a silent gap. |

## 4. Slices (each PR pairs implementation with its tests)

1. **Format.**
   - Implementation: the `ObjectFamily::Timeline` layout constructor and classifier; the segment codec; the kind-5 codec; the known-kinds guard (1..=5); spec §6, §12 and §13 updates; golden vectors; fuzz targets.
   - Behavior change: none.
   - Tests: round trips, adversarial decodes, goldens, and the family and literal guards.
2. **Write, recover, reclaim.**
   - Implementation: in-memory `sealed` and `tail` refs; the checkpoint writer (§2.4, without dedup); kind 5 written in place of kind 3; recovery (§2.5), recording `MissingSnapshotObject`; the `Superseded` and `ReconcileToAttested` segment prune (§2.6).
   - GC ships in the same slice as the writer, so `main` never accumulates orphan tails.
   - Tests:
     - The review's `review_3596_fully_flushed_history_can_checkpoint_under_cap`, renamed.
     - A timeline-dominated scenario with WAL-truncation and close/reopen assertions.
     - Fork sharing: parent and child reference one object.
     - A crash between segment publish and snapshot publish, reclaimed at open.
     - Missing and corrupt segments, under lossy and strict recovery.
     - Legacy kind-3 recovery followed by a first segment checkpoint.
     - Truth tables for the chunking and dedup decisions.
3. **Fork dedup, observability and oracles.**
   - Fork dedup (§2.4 step 2): a chunk matching a live reference is re-referenced, not rewritten.
   - Footprint folding (§2.6), the whole-database DST footprint oracle over segments, and the catalog amendments for DUR-017, DUR-019 and MVCC-009.
   - A CHANGELOG line: a database written by this build cannot be opened by 1.2.5. That is already one-way through the v2 event formats.
