# StrataDB Engine Invariant Catalog

> **Purpose**: Durable specification of correctness properties for the StrataDB engine.
> Survives code refactors — anchored to architectural properties, not file paths or line numbers.
> Used as a standing audit checklist: after any significant change, verify the relevant invariants.
>
> **How to use**: Each invariant has an **Audit** instruction. Execute it against the current codebase.
> The instruction tells you *what to find*, not *where to find it*. If the code moved, the invariant
> still applies — just search for it.
>
> **Maintenance**: Update when the *architecture* changes, not when code is refactored.
> If a new compaction strategy is added, add invariants for it. If a function is renamed, do nothing.
>
> **Categories**: LSM (7), CMP (6), COW (6), MVCC (6), ACID (7), ARCH (8), SCALE (10) = 50 invariants

---

## LSM — Storage Engine Invariants

### LSM-001: InternalKey sort order

InternalKey byte ordering MUST produce ascending order by `(branch_id, space, type_tag, user_key)`
and descending order by `commit_id` within the same logical key. This is achieved by bitwise-NOT
encoding of the commit_id suffix. If this invariant is broken, every MVCC read, every merge
iteration, and every compaction produces wrong results.

**Audit**: Find the InternalKey encoding function. Verify that the commit_id is inverted (`!commit_id`)
before big-endian encoding. Construct test cases: two keys with the same logical prefix but different
commit_ids — verify the higher commit_id sorts first in byte order.

### LSM-002: Byte-stuffing roundtrip

The user_key encoding uses byte-stuffing (`0x00` → `0x00 0x01`, terminated by `0x00 0x00`) to preserve
lexicographic ordering for arbitrary binary keys. Encoding then decoding MUST produce the original
bytes for all inputs, including: empty input, input containing `0x00`, input containing `0x00 0x01`,
input ending with `0x00`.

**Audit**: Find `encode_escaped` and `decode_escaped`. Verify roundtrip correctness. Check that no
valid encoded sequence is a prefix of another (unambiguous parsing). Verify the terminator `0x00 0x00`
cannot appear within the encoded data.

### LSM-003: Read path level ordering

The read path MUST check sources in this order: active memtable → frozen memtables (newest first) →
L0 segments (newest first) → L1 → L2 → ... → L6 → inherited COW layers (nearest ancestor first).
The first match wins. This relies on the invariant that newer sources contain newer versions of any key.

**Audit**: Find the main point-lookup function (e.g., `get_versioned_from_branch`). Trace the source
ordering. Verify no early return skips a newer source. Verify L0 segments are ordered by commit_max
descending. Verify L1+ levels are checked in ascending order.

### LSM-004: Memtable rotation atomicity

When the active memtable exceeds `write_buffer_size`, the freeze-and-swap MUST be atomic with respect
to concurrent readers and writers. A reader must never see a state where both the old (frozen) and
new (active) memtable are missing entries. A writer must never insert into a frozen memtable.

**Audit**: Find the memtable rotation code (freeze + new active). Check if there is a window where
a write could go to neither memtable. Verify the `frozen` flag prevents further writes. Verify
concurrent readers see either the pre-rotation or post-rotation state, never an intermediate.

### LSM-005: Bloom filter correctness with rewritten keys

Bloom filters are built from `typed_key_prefix` bytes that include the branch_id. When a COW child
queries an inherited segment, it rewrites the key to the source's branch_id before probing. The
rewritten key MUST produce the exact same bytes that were used when building the bloom filter.
Any divergence causes false negatives — data silently invisible.

**Audit**: Find the bloom probe code path for inherited layer lookups. Verify the key rewrite function
produces byte-identical output to what the segment builder used during bloom construction. Test with
a key that exists in a parent segment — verify bloom returns `maybe_contains = true` after rewrite.

### LSM-006: Block cache keying independence from branches

The block cache is keyed by `(file_id, block_offset)` where `file_id` is derived from the file path.
Shared segments (accessed from multiple branches via COW) MUST use the same cache entries regardless
of which branch is accessing them. Cache keys MUST NOT include branch_id.

**Audit**: Find the block cache key computation. Verify it uses only file-level identity (path hash),
not caller identity. Verify a shared segment's blocks are cached once, not per-branch.

### LSM-007: Segment file immutability

Once a segment file is written and closed, it MUST NOT be modified. All reads use `pread` (positional
read) against the immutable file. Compaction creates NEW segment files; it never modifies existing ones.
This guarantees that concurrent readers holding Arc references to old segments always see consistent data.

**Audit**: Grep for any `write`, `truncate`, or `set_len` call on segment `.sst` files outside of
`SegmentBuilder`. Verify no compaction or maintenance operation modifies an existing segment.

---

## LSM — Compaction Invariants

### CMP-001: Tombstone preservation in non-bottommost compaction

A tombstone entry MUST NOT be dropped during compaction unless the compaction is bottommost
(no lower levels contain data for this key range). Dropping a non-bottommost tombstone resurrects
the entry it shadows in a lower level.

**Audit**: Find all code paths that skip or drop tombstone entries during compaction output.
For each, verify it is gated on a bottommost check. Cross-reference with RocksDB's
`BottommostLevelCompaction` logic. Note: the `drop_expired` flag controls TTL cleanup (bottommost only),
but tombstone cleanup for dead keys may be a separate code path — verify both.

### CMP-002: Version pruning respects prune_floor

The `CompactionIterator` (or equivalent) MUST:
- Emit all entries with `commit_id ≥ prune_floor`
- Emit exactly ONE entry below the floor per logical key (the "floor entry"), unless it's a tombstone
- Skip all remaining below-floor entries
- Respect `max_versions` cap

When `prune_floor == 0`, all versions pass through unchanged (no pruning).

**Audit**: Find the compaction iterator. Verify the pruning logic against these rules. Test edge cases:
`prune_floor == 0`, `prune_floor == u64::MAX`, key with only tombstone below floor, key with only
one version exactly at floor.

### CMP-003: Grandparent overlap control

When compacting L_n → L_{n+1}, output segment splitting MUST respect the `MAX_GRANDPARENT_OVERLAP`
threshold to prevent pathological write amplification during the *next* compaction (L_{n+1} → L_{n+2}).
The splitting predicate tracks cumulative overlap with L_{n+2} files and forces a split when exceeded.

**Audit**: Find the grandparent-aware split predicate. Verify the byte accumulation is correct. Check
what happens when the output key jumps past multiple grandparent files at once. Compare with RocksDB's
`MaxGrandParentOverlapBytes`.

### CMP-004: Compaction manifest-before-delete ordering

The storage manifest MUST be updated BEFORE old segment files are deleted. If the system crashes after
deleting old files but before writing the manifest, recovery would reference missing segments.

**Audit**: Find every compaction method that deletes old segments. Verify `write_branch_manifest()` is
called BEFORE any `delete_segment_if_unreferenced()` call. Verify the manifest write is crash-safe
(temp file + rename).

### CMP-005: Dynamic level sizing correctness

The dynamic level target computation (based on RocksDB's `CalculateBaseBytes`) MUST:
1. Find the largest non-empty non-L0 level
2. Compute base = bottom_bytes / multiplier^(bottom_level - 1)
3. Clamp base between MIN_BASE_BYTES and MAX_BASE_BYTES
4. Forward-compute all targets with saturation

Targets MUST be refreshed after every compaction and flush.

**Audit**: Find the level target computation function. Step through the algorithm with concrete numbers.
Verify it handles: empty database (all levels empty), single-level spike (L3 has 10GB, others empty),
very small databases (< MIN_BASE_BYTES total). Verify refresh is called after every segment version swap.

### CMP-006: Concurrent compaction and flush safety

A flush may insert a new L0 segment while compaction is reading L0 segments. Compaction snapshots the
segment list at start. The swap step filters out compacted segments (by Arc identity) and preserves
any segments added concurrently by flush. New segments MUST NOT be lost or duplicated.

**Audit**: Find the segment version swap in each compaction method. Verify the filter uses `Arc::ptr_eq`
(identity, not value equality). Verify the new version includes both the compaction output AND any
concurrently-flushed segments.

---

## COW — Copy-on-Write Branching Invariants

### COW-001: Shared segment deletion requires zero refcount

A segment file referenced by any branch's inherited layer MUST NOT be deleted. Every `.sst` file
deletion path must check the `SegmentRefRegistry`. Untracked segments (refcount never incremented)
are treated as unshared and can be deleted freely.

**Audit**: Grep for every `fs::remove_file` call on `.sst` files. Verify each goes through
`delete_segment_if_unreferenced` or equivalent refcount check. A single missing check means
compaction on a parent branch can silently corrupt all child branches.

### COW-002: Fork refcount increment before guard release

During `fork_branch`, segment refcounts for all inherited segments MUST be incremented BEFORE the
source branch's DashMap guard is released. If the guard is released first, a concurrent compaction
on the source could delete a segment between guard release and refcount increment.

**Audit**: Find the fork_branch implementation. Trace the ordering: source guard acquisition →
segment snapshot → guard release → refcount increment. If there is a gap between release and
increment, identify the race window and assess impact.

### COW-003: Inherited layer version gate

When reading through an inherited layer, the effective version ceiling is `min(max_version, fork_version)`.
Entries with `commit_id > fork_version` MUST be invisible to the child branch. This ensures branch
isolation — parent writes after the fork are invisible to the child.

**Audit**: Find every read path that accesses inherited layers (point lookup, range scan, history,
list). Verify each applies the `min(max_version, fork_version)` filter. Verify the RewritingIterator
applies the version gate.

### COW-004: Materialization preserves commit_ids

When inherited entries are materialized into the child's own segments, the original `commit_id`
MUST be preserved, not reassigned. Reassigning would break: (a) MVCC reads at the fork_version
(the entries would be invisible), (b) merge base computation (ancestor state would be lost).

**Audit**: Find the materialization function. Verify the InternalKey rewrite only changes the
branch_id prefix (first 16 bytes) and preserves the commit_id suffix (last 8 bytes).

### COW-005: Recovery resolves inherited layers after all branches are loaded

Inherited layer resolution requires looking up source branch segments by filename. All source
branches MUST be loaded (with their segments opened) before any child branch resolves its
inherited layers. A two-pass recovery (first pass: load own segments; second pass: resolve
inherited) satisfies this.

**Audit**: Find the recovery/segment-loading code. Verify inherited layers are resolved in a
second pass after all branches' own segments are loaded. Check: what happens if a source branch
is missing (directory deleted)? Is this a hard error or a warning with data loss?

### COW-006: Refcount rebuild on recovery

After crash recovery, segment refcounts MUST be rebuilt from all branches' inherited layers.
The rebuilt refcounts MUST match what they were before the crash (accounting for any in-flight
operations that were not durable).

**Audit**: Find the refcount rebuild code in recovery. Verify it iterates ALL branches' inherited
layers and increments for each referenced segment. Verify no double-counting.

---

## MVCC — Multi-Version Concurrency Control Invariants

### MVCC-001: Version visibility boundary

A reader at snapshot version V MUST NEVER see an entry with `commit_id > V`. This holds across
active memtable, frozen memtables, all segment levels, and inherited COW layers. This is the
foundational guarantee for snapshot isolation.

**Audit**: Find every point-lookup and scan code path. Verify each filters by `max_version` / `commit_id`.
Check that no fallthrough path bypasses the filter. Check inherited layer reads apply the
`min(max_version, fork_version)` ceiling.

### MVCC-002: Tombstone semantics

A tombstone at `commit_id = V` means "key deleted at version V." For readers at `max_version ≥ V`,
the key MUST return "not found" — the tombstone shadows all older versions. For readers at
`max_version < V`, the key exists at its previous version. Tombstones MUST be emitted by
`MvccIterator` (the caller decides filtering). Point lookups MUST treat tombstones as "not found."
History queries MUST include tombstones.

**Audit**: Find the point lookup caller that checks for tombstones. Verify it returns `None` (not the
tombstone value). Find the history query. Verify it includes tombstones. Find the scan/list path.
Verify it filters out tombstones.

### MVCC-003: Global version counter monotonicity

The global version counter (`TransactionManager::version`) MUST be monotonically increasing.
`allocate_version()` MUST return unique, increasing values. No two transactions may receive the
same commit_version. Version gaps are acceptable (failed transactions).

**Audit**: Find the version allocation function. Verify it uses `fetch_add(1)` or equivalent
atomic operation. Verify the overflow check at `u64::MAX`. Verify recovery restores the counter
to at least the maximum committed version from WAL replay.

### MVCC-004: Snapshot capture happens before version allocation

A transaction's snapshot version (`start_version`) MUST be captured BEFORE any concurrent
transaction's `allocate_version()` could make new data visible. Otherwise, the snapshot
could include data from a transaction that hasn't finished applying yet.

**Audit**: Find where `start_version` is captured (transaction creation). Verify it reads
`current_version()` which reflects only fully-applied transactions. Verify that a transaction's
version is allocated AFTER its writes are validated but the writes are not yet visible.

### MVCC-005: No snapshot pinning gap

Active transactions pin the GC safe point. The `gc_safe_version` MUST be ≤ the minimum snapshot
version of any active transaction. If GC prunes versions below `gc_safe_version`, and an active
transaction holds a snapshot at a lower version, that transaction will see missing data.

**Audit**: Find the `gc_safe_version` advancement logic. Verify it only advances when `active_count`
drains to 0. Verify `gc_safe_point()` returns `min(current_version - 1, gc_safe_version)` and
returns 0 when active transactions exist but no drain has occurred. Verify the prune_floor
passed to compaction never exceeds `gc_safe_point()`.

### MVCC-006: TTL expiration does not resurrect old versions

When the newest visible version of a key has expired (TTL elapsed), the read path MUST return
"not found" — it MUST NOT fall through to an older, non-expired version. The expired version
is the authoritative state. (Design decision: expired = gone, not expired = reveal older.)

**Audit**: Find the point lookup path. Verify that when the first matching entry is expired,
the function returns `None` without checking older versions. Verify this is the intended semantic.

---

## ACID — Atomicity, Consistency, Isolation, Durability Invariants

### ACID-001: Single WAL record per transaction

Each transaction MUST produce exactly ONE `WalRecord` containing the complete writeset (all puts,
deletes, CAS results). This ensures atomicity of recovery — a transaction is either fully replayed
or not at all. Partial WAL records (truncated by crash) are detected by CRC32 and discarded.

**Audit**: Find the WAL record construction in the commit protocol. Verify it bundles all writes
into a single `TransactionPayload` → single `WalRecord`. Verify no code path writes multiple
WAL records for a single transaction.

### ACID-002: WAL before storage

The commit protocol MUST write the WAL record BEFORE applying writes to storage. If storage
application fails after WAL write, the transaction is still durable (recovery replays from WAL).
If WAL write fails, the transaction MUST NOT be applied to storage.

**Audit**: Find the commit method. Verify the ordering: validate → allocate version → WAL append →
apply_writes. Verify that WAL failure causes abort (no storage application). Verify that storage
failure after WAL is logged but the commit is still reported as successful.

### ACID-003: Per-branch lock prevents TOCTOU

For read-write transactions (non-blind-write), the per-branch commit lock MUST be held from
validation through version allocation and storage application. Without this, a concurrent
transaction could modify storage between validation and apply, causing a stale validation
to be acted upon.

**Audit**: Find the per-branch lock acquisition. Verify it is held continuously from validation
start through apply_writes completion. Verify blind writes (empty read_set) correctly skip the
lock without violating isolation.

### ACID-004: Blind write safety

Blind writes (no prior reads, no CAS, no JSON snapshots) skip the per-branch lock and validation.
This is safe because: (a) each blind write gets a unique version via atomic `allocate_version()`,
(b) concurrent blind writes create distinct versions — highest wins at read time, (c) MVCC
prevents readers from seeing partially-applied blind writes (all entries share the same version).

**Audit**: Find the blind write fast path. Verify it correctly identifies blind writes (all read-related
sets empty). Verify no reader can snapshot at the blind write's version before apply completes.
Specifically: `allocate_version()` increments the global counter. A concurrent `current_version()`
call returns the new value. A reader creating a snapshot at this version would try to read data
that might not be in the memtable yet. Verify whether this race exists and its impact.

### ACID-005: Recovery replay is idempotent

WAL replay MUST be idempotent — replaying the same record twice produces the same state as
replaying it once. This is required because a crash during recovery could cause a second
recovery to re-replay some records. Idempotence holds because memtable put with the same
`(key, version)` overwrites with the same value.

**Audit**: Find the WAL replay callback. Verify it uses `put_with_version_mode` with the original
commit_version from the WAL record. Verify that inserting the same `(key, version)` pair twice
into the SkipMap produces one entry, not two.

### ACID-006: DurabilityMode::Always provides fsync-per-commit

In `Always` mode, the WAL MUST be fsynced to stable storage before the commit returns.
After the commit returns, a crash MUST NOT lose the transaction.

**Audit**: Find the `maybe_sync` function. Verify that in `Always` mode, `segment.sync()` is called
on every append. Verify `sync()` calls `flush()` (BufWriter → OS) then `sync_all()` or `sync_data()`
(OS → disk). If `flush()` is missing, data in the BufWriter is lost on crash even after "fsync."

### ACID-007: Standard mode durability window is bounded

In `Standard` mode, fsync is periodic (every `interval_ms` or `batch_size` commits, whichever first).
The data loss window MUST NOT exceed `interval_ms`. If the background flush thread stalls beyond
`interval_ms` without a sync, an inline fsync MUST be triggered.

**Audit**: Find the Standard mode logic in `maybe_sync`. Verify the safety-net threshold. Find the
background flush thread (if it exists). Verify it calls `sync_if_overdue` periodically. If the
background thread doesn't exist, verify the safety-net covers all cases.

---

## ARCH — Cross-Cutting Architectural Invariants

### ARCH-001: One version domain for GC

All data-bearing entries in KV storage (including EventLog events, StateCell values, JSON documents,
Vector records, and Branch metadata) are stored as KV entries with a `Txn(u64)` commit_version.
The GC safe point (`gc_safe_version`) is computed in the Txn version domain and protects ALL
entries regardless of their primitive type.

EventLog's `Sequence(u64)` and StateCell's `Counter(u64)` are metadata stored WITHIN the KV value,
not independent version axes. GC prunes by Txn commit_version, which correctly prunes all types.

**Audit**: Find where EventLog entries are stored in KV. Verify the KV entry has a Txn commit_version
(from the transaction). Find where StateCell values are stored. Verify same. Verify that `gc_safe_point`
and `prune_floor` operate in the Txn domain and that this correctly governs all primitive types.

### ARCH-002: One atomic publication boundary

All writes in a transaction share a single commit_version. A reader at `max_version < commit_version`
sees NONE of them. A reader at `max_version ≥ commit_version` sees ALL of them. There is no
intermediate state where some writes are visible and others are not.

Secondary indexes (HNSW vector, BM25 search) are updated AFTER storage application and are
NOT part of the atomic boundary. They are eventually consistent — a query through the search
index may temporarily miss newly-committed entries. KV queries are always authoritative.

**Audit**: Verify all entries in a transaction get the same commit_version. Verify no reader can
snapshot at commit_version V while `apply_writes` for V is still in progress (for read-write
transactions, the per-branch lock prevents this; for blind writes, verify the race analysis).
Verify secondary index updates are documented as eventually consistent.

### ARCH-003: KV storage is the single source of truth

All persistent state lives in the `SegmentedStore`. Secondary indexes (HNSW, BM25) are derived
and can be fully rebuilt from KV storage. Losing a secondary index MUST NOT lose any data —
only search/query performance is affected until rebuild.

**Audit**: For each secondary index (vector HNSW, search BM25), verify that a full rebuild from
KV storage produces identical results. Find the recovery code for each — verify it scans KV
and rebuilds from scratch. Verify no secondary index contains information that cannot be
derived from KV.

### ARCH-004: One recovery model with deterministic ordering

Recovery follows a fixed sequence: MANIFEST → snapshot → WAL replay → segment recovery →
inherited layer resolution → refcount rebuild → version counter restoration → secondary index rebuild.

Each step's output MUST NOT depend on a later step's output (no circular dependencies).
Replay MUST be deterministic: same WAL records → same final state. Replay MUST be idempotent:
multiple recoveries → same result.

**Audit**: Find the master recovery sequence (Database startup). Trace the ordering of each step.
Verify no step reads state that a later step produces. Verify the WAL watermark (filtering by
`txn_id`) is consistent with the version domain (commit_version) — specifically that txn_id
ordering and commit_version ordering don't diverge in a way that causes missed or duplicate replays.

### ARCH-005: GC respects active snapshots AND branch inheritance

Compaction pruning (prune_floor) MUST be ≤ `gc_safe_point()`, which is ≤ the minimum active
snapshot version. Additionally, COW inherited layers hold Arc references to old segment files,
preventing their deletion. But compaction creates NEW segments with pruned entries — the pruning
is safe because the old segments (with unpruned entries) remain accessible through the Arc references.

**Audit**: Find where `prune_floor` is passed to compaction. Verify it comes from `gc_safe_point()`.
Verify `gc_safe_point()` accounts for active transactions and returns 0 when safety requires it.
Verify that COW children read from Arc'd segment snapshots (not the parent's current version),
so compaction on the parent doesn't affect the child's view.

### ARCH-006: Transaction timeout prevents GC starvation

Long-running transactions pin `gc_safe_version`, preventing version pruning. The system MUST
enforce a transaction timeout (default: 5 minutes) that rejects commits of expired transactions.
Without this, a leaked transaction handle could cause unbounded version accumulation.

**Audit**: Find the transaction timeout constant. Find where it's checked (commit path). Verify
that an expired transaction's commit fails and its active_count is decremented, allowing
gc_safe_version to advance.

### ARCH-007: Manifest and WAL are separate authority domains

The system has TWO manifest files that serve different purposes:
1. **Durability MANIFEST** (database-level): UUID, codec, snapshot info, active WAL segment
2. **Storage manifests** (per-branch): segment filenames, levels, inherited layers

These MUST NOT conflict. The durability MANIFEST governs the recovery protocol (which snapshot, which
WAL segments). Storage manifests govern segment loading (which files, which levels). Neither depends
on the other's correctness — they are independent authority domains.

**Audit**: Verify the two manifest systems are independent. Find each manifest's read and write paths.
Verify no code reads a durability MANIFEST field to make a storage manifest decision, or vice versa.

### ARCH-008: Fork_version creates an implicit retention floor for shared segments

When branch B inherits segments from A at fork_version V, the entries in those segments with
commit_id ≤ V MUST remain accessible to B. Since B holds Arc references to the OLD segment files
(pre-compaction), and segment files are immutable (LSM-007), compaction on A does not modify
the shared files. A's compaction creates new segments and tries to delete old ones — but the
refcount check (COW-001) prevents deletion while B references them.

**Audit**: Verify the complete chain: (1) B holds Arc to A's segments, (2) A's compaction creates
new files, (3) old file deletion is blocked by refcount, (4) B reads from the unchanged old files.
If any link breaks, B sees corrupted or missing data.

---

## SCALE — Scale-Span Invariants (Pi Zero to Billion-Key Server)

### SCALE-001: No hardcoded constant assumes server-class resources

Every memory-consuming default MUST be either configurable via `StorageConfig` or derived from a
resource-aware computation (e.g., `auto_detect_capacity()`). No hardcoded constant should cause
OOM on a 512MB device with default configuration.

Specifically:
- Block cache default MUST NOT exceed 25% of available RAM on the target device
- Write buffer size MUST NOT exceed 10% of available RAM
- `auto_detect_capacity()` MUST NOT clamp to a minimum that exceeds available RAM
- Level sizing constants (`LEVEL_BASE_BYTES`, `MAX_GRANDPARENT_OVERLAP`) MUST be derived from
  actual data size, not hardcoded to server-scale values

**Audit**: Find every constant in the storage crate that controls a memory allocation size or
threshold. For each, compute what percentage of 350MB (Pi Zero usable RAM) it represents at its
default value. Flag any constant ≥ 25% of 350MB that is not configurable or auto-scaled.

### SCALE-002: Memory budget propagates to all consumers

A single `memory_budget` configuration (or equivalent set of `StorageConfig` fields) MUST control
the total memory footprint. Setting `block_cache_size = 16MB` and `write_buffer_size = 4MB` MUST
actually result in those limits being respected — no internal code path should override, ignore,
or supplement these with additional uncapped allocations.

**Audit**: For each `StorageConfig` field, trace from config parsing to the point where the value
is consumed by the storage engine. Verify no intermediate code overrides the value. Verify there
are no major memory consumers outside `StorageConfig` control (e.g., DashMap growth, segment index
pinning, bloom filter pinning, HNSW index). For uncapped consumers, quantify their memory usage
as a function of data size.

### SCALE-003: On-disk format is word-size portable

A database created on a 64-bit server MUST be openable on a 32-bit ARM device, and vice versa.
All on-disk formats (segment files, WAL records, manifests, snapshots) MUST use fixed-width integer
types (`u32`, `u64`), not `usize`. Endianness MUST be explicitly little-endian (`to_le_bytes`),
never native (`to_ne_bytes`).

**Audit**: Find every serialization/deserialization path in segment, WAL, manifest, and snapshot code.
Grep for `usize` in any `to_bytes` / `from_bytes` / serialization function. Grep for `to_ne_bytes`.
Verify all on-disk integers are explicit-width and explicit-endian.

### SCALE-004: AtomicU64 operations are bounded per user operation

On 32-bit ARM (Pi Zero 1 / ARMv6), `AtomicU64` is emulated via a global lock — every `fetch_add`,
`load`, and `store` on any `AtomicU64` contends on the same mutex. The number of `AtomicU64`
operations per read and per write MUST be bounded and small (ideally ≤ 5 per operation).

**Audit**: Trace a single point-read (`get_versioned`) and a single write (`put_with_version_mode`)
through the storage engine. Count every `AtomicU64` operation touched (version counter, memtable
stats, block cache hit/miss counters, timestamp tracking). Assess whether the count is acceptable
on a single-core ARM device with emulated atomics.

### SCALE-005: Write amplification is bounded and documented

LSM write amplification directly impacts SD card write endurance. The theoretical write amplification
for the default configuration MUST be documented. On flash storage, total write amplification
(user writes × LSM amplification × filesystem/FTL amplification) determines device lifetime.

**Audit**: Calculate write amplification for the default config: L0 trigger=4, multiplier=10,
7 levels. For each level transition, compute the amplification factor. Sum to get worst-case total.
Then compute: for a 32GB SD card with 10K P/E cycles, how many total user bytes can be written
before the card wears out? Is this acceptable for the target workload (10K keys, moderate write rate)?

### SCALE-006: Compaction does not starve user operations on single-core

On a single-core device, compaction I/O competes directly with user reads and writes. Compaction
MUST be throttleable to prevent starvation. The `RateLimiter` (if used) MUST be configurable to
a rate appropriate for SD card bandwidth.

**Audit**: Find every compaction code path. Verify each uses the `RateLimiter` (if configured).
Find where the rate limiter is instantiated — is the rate configurable via `StorageConfig`? What
is the default rate? On a 10 MB/s SD card, is the default rate appropriate? Is there a way to
pause compaction entirely (for burst write periods on slow storage)?

### SCALE-007: Thread count is bounded and configurable

On a single-core device, each background thread adds scheduling overhead and ~8MB stack memory.
The total number of threads spawned by the engine MUST be bounded and should be configurable.
On a Pi Zero, no more than 2-3 threads total (main + background flush + optional compaction).

**Audit**: Find every `thread::spawn`, `thread::Builder`, `rayon`, or `tokio::spawn` call in the
engine and storage crates. List all background threads. Verify they can be disabled or reduced
on resource-constrained devices. Compute the total stack memory overhead.

### SCALE-008: Billion-key segment metadata fits in memory

At billion-key scale with thousands of segments, pinned segment metadata (bloom filter partitions,
index blocks) must fit within the block cache budget. The total pinned memory MUST NOT exceed the
pinned budget (10% of block cache capacity by default).

**Audit**: For a dataset with 1B keys at 100 bytes each (100GB total data), calculate:
- Number of segments across 7 levels at `TARGET_FILE_SIZE=64MB`
- Pinned bloom filter memory per segment (at configured bits/key per level)
- Pinned index block memory per segment
- Total pinned memory vs pinned budget (10% of block cache)
If pinned metadata exceeds the budget, assess whether bloom/index pinning degrades gracefully
(evicted to non-pinned cache tier) or fails hard.

### SCALE-009: MergeIterator scales to high source counts

The `MergeIterator` uses linear scan (O(N) per `next()` call, where N = source count). This is
optimal for 1-3 sources but degrades at higher counts. At billion-key scale, compaction merges
can involve 10+ sources (L0 files + overlapping L1 files). The iterator MUST either switch to a
heap-based merge at high source counts or document the performance cliff.

**Audit**: Find the MergeIterator. Verify the source count at which linear scan becomes slower than
a binary heap (typically N ≥ 6-8). Find the maximum source count that can occur in practice for
each compaction type (compact_branch, compact_l0_to_l1, compact_level). If any can exceed 8 sources,
assess the performance impact.

### SCALE-010: Feature degradation is explicit, not OOM

Features that require significant memory (HNSW vector indexes, BM25 search, auto-embed model
loading) MUST be disableable without affecting core KV/JSON/Event/State functionality. Running
out of memory MUST produce a clear error, not a silent crash or data corruption.

**Audit**: For each optional feature (vector search, BM25 search, auto-embed), verify:
1. It can be disabled via configuration
2. Disabling it does not break core primitives
3. Enabling it on a memory-constrained device produces a clear error if memory is insufficient,
   rather than OOM-killing the process
4. There is a brute-force fallback for vector search on small collections

---

## How to Use This Catalog

### After a code change

1. Identify which invariants the change could affect (use the category prefixes: LSM, CMP, COW, MVCC, ACID, ARCH, SCALE)
2. Execute the **Audit** instruction for each affected invariant against the current codebase
3. If an invariant is violated, fix the code — do not weaken the invariant

### After an architectural change

1. Review all invariants for relevance — some may need updating
2. Add new invariants for new architectural properties
3. Remove invariants for removed architectural properties
4. The invariant IDs are stable — do not renumber (use gaps if removing)

### During code review

Reference invariants by ID: "This change could violate CMP-001 — verify tombstone handling
in the new compaction path."
