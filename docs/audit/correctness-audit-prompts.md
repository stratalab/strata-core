# Correctness Audit Prompts

Send each section as a separate conversation with full codebase context. Each prompt is self-contained.

---

## Audit 1: WAL Crash Safety & Recovery

**Goal**: Verify that committed data is never lost and uncommitted data is never visible after a crash at any point in the commit sequence.

**Read these files in full before beginning:**

- `crates/durability/src/wal/writer.rs` — WAL append, fsync, segment rotation
- `crates/durability/src/wal/reader.rs` — WAL segment reading
- `crates/durability/src/wal/config.rs` — durability modes
- `crates/durability/src/wal/mode.rs` — Cache/Standard/Always behavior
- `crates/durability/src/format/wal_record.rs` — record framing, CRC, serialization
- `crates/durability/src/format/manifest.rs` — MANIFEST format and updates
- `crates/durability/src/format/watermark.rs` — watermark tracking
- `crates/durability/src/recovery/coordinator.rs` — recovery orchestration
- `crates/durability/src/recovery/replayer.rs` — WAL replay logic
- `crates/durability/src/disk_snapshot/checkpoint.rs` — snapshot creation
- `crates/durability/src/disk_snapshot/writer.rs` — snapshot write path
- `crates/durability/src/disk_snapshot/reader.rs` — snapshot read path
- `crates/durability/src/coordination.rs` — flush coordination
- `crates/engine/src/coordinator.rs` — engine-side recovery
- `crates/engine/src/recovery/mod.rs` — recovery participants
- `crates/engine/src/recovery/participant.rs` — participant trait

**Evaluate the following:**

### 1.1 WAL Record Integrity
- Trace the exact bytes written for a single WAL record: length prefix → format version → payload → CRC. Is the CRC computed over the right bytes?
- Is the length prefix itself protected against torn writes? If the first 4 bytes of a record are partially written, can the reader detect this or does it silently skip to a wrong offset?
- What happens when a record spans the boundary between two segments? Is this prevented or handled?
- Are there any code paths where `append()` returns success but the record hasn't been fsynced (in Always mode)?

### 1.2 Crash Point Analysis
Walk through every crash point in the commit sequence and determine the outcome:

1. **Crash during WAL append (bytes partially written)**: Is the partial record detected and truncated on recovery?
2. **Crash after WAL append but before fsync**: In Standard mode, is the data recoverable? In Always mode, can this happen?
3. **Crash after WAL fsync but before storage apply**: Is the record replayed on recovery? Does replay produce the same result as the original apply?
4. **Crash during storage apply (partially applied)**: Is the replay idempotent? Does applying a record twice produce the same state as applying it once?
5. **Crash during segment rotation**: Old segment closed, new segment not yet opened. Is the rotation completed on recovery?
6. **Crash during snapshot creation**: Temporary file exists but isn't renamed. Is the temp file cleaned up?
7. **Crash after snapshot but before MANIFEST update**: Snapshot exists but MANIFEST doesn't reference it. Is the snapshot discovered or orphaned?
8. **Crash during MANIFEST write**: Is the MANIFEST itself crash-safe? Does it use write-fsync-rename?

### 1.3 Recovery Completeness
- After recovery, is the database in exactly the same state as if all committed transactions had been applied and no uncommitted transactions had been applied?
- Does recovery replay records in the correct order (by txn_id)?
- If the WAL contains records for transactions that span multiple branches, is the replay order correct?
- What happens if a recovery participant (VectorStore, SearchIndex) fails during replay? Is the error propagated or swallowed?
- After recovery, are all counters (version, txn_id) restored to the correct values? Is there any gap that could cause duplicate IDs?

### 1.4 Standard Mode Durability Window
- In Standard mode, what is the exact maximum data loss window? Trace the code path from `append()` through the background flush thread.
- What triggers the "inline sync fallback" when the background thread is stalled? Is the 3x deadline check correct?
- Can Standard mode lose committed transactions that the application believes are durable?

### 1.5 Multi-Process Safety
- If two processes open the same database directory, what prevents concurrent WAL writes? Is there file locking?
- The `reopen_if_needed()` method detects segment rotation by another process. Is this race-free?

**Output**: For each crash point, state: SAFE (data preserved), UNSAFE (data loss possible), or DEGRADED (recovery succeeds but with side effects). For any UNSAFE finding, describe the exact sequence of events that leads to data loss.

---

## Audit 2: Compaction Correctness

**Goal**: Verify that compaction never loses data, never resurrects deleted keys, and maintains all MVCC invariants.

**Read these files in full:**

- `crates/storage/src/compaction.rs` — CompactionIterator, version pruning logic
- `crates/storage/src/segmented/compaction.rs` — level-based compaction, file selection, merge
- `crates/storage/src/segmented/mod.rs` — SegmentedStore, SegmentVersion, read path
- `crates/storage/src/merge_iter.rs` — K-way merge iterator, MVCC deduplication
- `crates/storage/src/segment_builder.rs` — segment output writing
- `crates/storage/src/manifest.rs` — manifest updates after compaction
- `crates/durability/src/compaction/wal_only.rs` — WAL compaction
- `crates/durability/src/compaction/tombstone.rs` — tombstone handling

**Evaluate the following:**

### 2.1 Version Pruning Correctness
The `CompactionIterator` decides which versions to keep and which to drop. This is the most critical code in the entire storage engine — a bug here permanently deletes data.

- Trace the pruning logic for these scenarios (provide the expected output for each):
  - Key K has versions [10, 8, 6, 4, 2], prune_floor = 5 → should keep [10, 8, 6, 4]? Or [10, 8, 6]? What's the exact rule?
  - Key K has versions [10, 8, 6, 4, 2], prune_floor = 5, max_versions = 2 → what's kept?
  - Key K has tombstone at version 10, value at version 8, prune_floor = 5 → is the tombstone preserved? Is the value below it preserved?
  - Key K has tombstone at version 3, value at version 2, prune_floor = 5 → both below floor. Is the key fully removed? Can it reappear ("zombie key")?
  - Key K has only a tombstone at version 3, prune_floor = 5 → is the tombstone dropped? What if there's data for this key in a lower level that hasn't been compacted yet?

- **The zombie key problem**: If a tombstone is dropped during compaction of level L, but there's an older value for the same key in level L+1 that hasn't been compacted yet, does the old value become visible again? How is this prevented?

### 2.2 Level-Based Compaction
- **File selection**: When compacting level L, how are input files chosen? Is the overlapping file detection in level L+1 correct? Could it miss a file whose key range overlaps?
- **Boundary files**: If two versions of the same user key are split across two L+1 files, are both files included in the compaction? Missing one could cause data loss.
- **Grandparent overlap**: Is the output file splitting based on L+2 overlap implemented? If not, what's the write amplification consequence?
- **Trivial move**: When a file can be moved from L to L+1 without rewriting (no overlap), is this correctly detected? Is the manifest updated atomically?

### 2.3 Atomic Version Swap
- After compaction produces new segments, the `SegmentVersion` is swapped atomically via `ArcSwap`. Can a reader that started before the swap see an inconsistent state (old segments removed, new segments not yet visible)?
- Is the old segment files' deletion deferred until no reader holds a reference? What mechanism ensures this?
- Is the block cache invalidated for old segments? Can stale cache entries cause incorrect reads?

### 2.4 Compaction + Concurrent Reads
- While compaction is running (reading input segments, writing output), can concurrent reads still access the input segments? Is there a reference count or lifetime mechanism?
- If a flush happens during compaction, does the new L0 segment interact correctly with the in-progress compaction?

### 2.5 Dynamic Level Sizing
- Is `recalculate_level_targets()` called after every compaction? Are the targets used correctly in scoring?
- When base_level changes (e.g., from L1 to L2 as data grows), do existing segments in the old base level get compacted down? Or do they become orphaned?

**Output**: For each scenario in 2.1, state the exact versions kept/dropped and whether the behavior is correct. For 2.2-2.5, identify any invariant violations.

---

## Audit 3: OCC Validation & Conflict Detection

**Goal**: Verify that no transaction can commit with stale data — every committed transaction must have seen a consistent snapshot.

**Read these files in full:**

- `crates/concurrency/src/transaction.rs` — TransactionContext, read/write/delete/CAS sets
- `crates/concurrency/src/validation.rs` — read-set validation, CAS validation, JSON validation
- `crates/concurrency/src/conflict.rs` — conflict types and reporting
- `crates/concurrency/src/manager.rs` — commit path, per-branch locking, version allocation
- `crates/concurrency/src/payload.rs` — transaction payload serialization
- `crates/concurrency/src/recovery.rs` — transaction recovery

**Evaluate the following:**

### 3.1 Read-Set Validation
- When a transaction reads key K at version V, what exactly is stored in the read set? The key and the version?
- At commit time, what is checked? Is it "current version of K == V" or "current version of K <= snapshot version"?
- If T1 reads K (version 5), T2 writes K (now version 6), T1 tries to commit — does T1 correctly abort?
- If T1 reads K (version 0, meaning "doesn't exist"), T2 creates K (now version 1), T1 tries to commit — is this phantom detected?

### 3.2 Read-Your-Writes Isolation
- If a transaction writes K then reads K, does it see its own write? Does this read add K to the read set? (It shouldn't — reading your own write doesn't create a dependency on external state.)
- If a transaction deletes K then reads K, does it see the deletion? Same question about read set.

### 3.3 Per-Branch Commit Lock
- The commit path acquires a per-branch mutex before validation. This prevents TOCTOU between validation and apply. Verify:
  - The lock is acquired before validation begins
  - The lock is held through validation, WAL write, and storage apply
  - The lock is released after all three steps complete
  - Two transactions on different branches can commit in parallel (different locks)
  - Two transactions on the same branch are serialized (same lock)

### 3.4 Version Allocation
- Version numbers are allocated via `fetch_add` on an atomic counter. Is the allocated version used consistently in the WAL record and the storage apply?
- Can two transactions get the same version number? (They shouldn't — `fetch_add` is atomic.)
- After a crash, is the version counter restored to the correct value? Is there a gap?

### 3.5 Blind Writes
- A write without a preceding read should not add to the read set (this is "blind write" semantics). Verify that two transactions blind-writing the same key do NOT conflict.
- Is this behavior intentional and documented? What are the implications?

### 3.6 CAS (Compare-And-Swap)
- CAS operations store an expected version. At commit time, the current version must match. Is this check separate from the read-set check?
- Can a CAS and a blind write to the same key coexist in the same transaction?

### 3.7 JSON-Specific Validation
- JSON documents have their own conflict detection path. Is it document-level or path-level?
- If T1 reads path `$.a` and T2 writes path `$.b` on the same document, does T1 abort? (Current expectation: yes, it's document-level.)

### 3.8 Edge Cases
- Empty transaction (no reads, no writes) — does it commit? Does it allocate a version number?
- Read-only transaction — does it skip validation? Does it allocate a version?
- Transaction that reads and writes the same key — does it conflict with itself?
- Transaction on a branch that doesn't exist yet — error handling?

**Output**: For each scenario, state: CORRECT (behaves as expected), BUG (violates snapshot isolation), or DESIGN CHOICE (intentional deviation, document the tradeoff).

---

## Audit 4: Key Encoding & MVCC Ordering

**Goal**: Verify that the order-preserving key encoding maintains correct sort order for all key types, branch IDs, type tags, and MVCC versions.

**Read these files in full:**

- `crates/storage/src/key_encoding.rs` — InternalKey encoding and decoding
- `crates/core/src/types.rs` — Key, BranchId, Namespace, TypeTag definitions
- `crates/core/src/value.rs` — Value type (for understanding what gets stored)

**Evaluate the following:**

### 4.1 Sort Order Invariant
The encoded key format is `{BranchId}:{TypeTag}:{UserKey}:{!CommitId}`. The critical invariant: for the same logical key (same branch, type, user key), higher commit IDs must sort FIRST (newest version first). This is achieved by bitwise-NOT of the commit_id.

- Verify that `!commit_id` (bitwise NOT) produces the correct descending sort order when compared as big-endian bytes.
- What happens at the boundary? commit_id = 0 → !0 = u64::MAX. commit_id = u64::MAX → !u64::MAX = 0. Is the full range handled?
- Are commit IDs compared as 8-byte big-endian? If little-endian, the sort order is wrong.

### 4.2 Byte-Stuffing for User Keys
User keys can contain arbitrary bytes, including 0x00. The encoding uses byte-stuffing (0x00 → 0x00 0x01) with 0x00 0x00 as terminator.

- Is encoding/decoding round-trip correct for all byte values (0x00 through 0xFF)?
- Is encoding/decoding correct for empty user keys?
- Is the encoding order-preserving? Specifically: if user_key_a < user_key_b (lexicographic), is encode(a) < encode(b)?
- Can the terminator 0x00 0x00 appear inside a stuffed key? (It shouldn't — 0x00 is always followed by 0x01 inside the key.)

### 4.3 Prefix Matching
- `typed_key_prefix` is used for MVCC deduplication (grouping versions of the same logical key). Is it defined as `{BranchId}:{TypeTag}:{UserKey}` (everything except commit_id)?
- Can two different logical keys produce the same `typed_key_prefix`? (They shouldn't.)
- Can the same logical key at different versions produce different `typed_key_prefix` values? (They shouldn't.)

### 4.4 Cross-Branch Isolation
- Two keys on different branches should never compare as equal, even if they have the same type tag and user key. Verify that BranchId (16-byte UUID) is the first component and provides complete isolation.
- What happens if two BranchIds share a common prefix? Does the sort order still work?

### 4.5 Type Tag Ordering
- Keys with different TypeTags should sort into separate groups. Verify that TypeTag is encoded as a single byte after BranchId.
- What are the TypeTag values? Are they stable (won't change across versions)?

### 4.6 Property Tests
- Do property tests exist for key encoding? Do they cover:
  - Round-trip (encode → decode = original)
  - Order preservation (a < b → encode(a) < encode(b))
  - Prefix correctness (same logical key → same typed_key_prefix)
  - Adversarial inputs (keys with 0x00, 0xFF, empty, very long)

**Output**: For each invariant, state HOLDS or VIOLATED with a specific counterexample if violated.

---

## Audit 5: Vector Store Integrity

**Goal**: Verify that vector operations are safe under concurrency, recovery produces correct indexes, and HNSW graph integrity is maintained.

**Read these files in full:**

- `crates/engine/src/primitives/vector/store.rs` — VectorStore facade, operation dispatch
- `crates/engine/src/primitives/vector/hnsw.rs` — HNSW graph implementation
- `crates/engine/src/primitives/vector/segmented.rs` — segmented HNSW (active + sealed)
- `crates/engine/src/primitives/vector/backend.rs` — VectorIndexBackend trait
- `crates/engine/src/primitives/vector/distance.rs` — SIMD distance computation
- `crates/engine/src/primitives/vector/heap.rs` — vector data storage
- `crates/engine/src/primitives/vector/mmap.rs` — memory-mapped vector heap
- `crates/engine/src/primitives/vector/mmap_graph.rs` — memory-mapped HNSW graph
- `crates/engine/src/primitives/vector/recovery.rs` — WAL-based recovery
- `crates/engine/src/primitives/vector/snapshot.rs` — snapshot persistence
- `crates/engine/src/primitives/vector/wal.rs` — WAL record types
- `crates/engine/src/primitives/vector/collection.rs` — collection management
- `crates/engine/src/primitives/vector/filter.rs` — metadata filtering
- `crates/engine/src/primitives/vector/types.rs` — type definitions

**Evaluate the following:**

### 5.1 Concurrency Safety
- What lock protects the HNSW graph during concurrent insert and search? Is it a RwLock (multiple readers, single writer)?
- Can a search see a partially-inserted vector (node added but neighbors not yet linked)?
- During segment sealing (active buffer → sealed segment), can concurrent searches see inconsistent state?
- The known TOCTOU issue: KV existence check happens outside the write lock. Trace the exact race condition and its consequences.

### 5.2 HNSW Graph Integrity
- After inserting N vectors and deleting M of them (soft-delete), is the graph still connected? Can search reach all non-deleted vectors?
- When a vector is soft-deleted, it remains in the graph as a traversal bridge. What prevents search from returning it as a result?
- Does the neighbor selection heuristic maintain the graph's small-world property? Specifically: after many insertions, can the graph degrade into disconnected components?
- Is the level assignment deterministic? Given the same sequence of insertions, does it produce the same graph?

### 5.3 Distance Computation
- The SIMD distance functions (NEON, AVX2) contain `unsafe` code. Verify:
  - Are the pointer accesses within bounds?
  - Is the alignment correct for SIMD loads?
  - Do the SIMD and scalar code paths produce the same results (within floating-point tolerance)?
  - Are there any NaN or infinity edge cases in cosine similarity (e.g., zero-magnitude vectors)?

### 5.4 Recovery Correctness
- After a crash, vector indexes are rebuilt from KV records. Does this rebuild produce the same HNSW graph as the original? (It should, given deterministic construction.)
- If a crash occurs between vector backend update and KV record write, is the phantom vector cleaned up on recovery?
- Are mmap-cached graphs invalidated correctly when the underlying data changes?

### 5.5 Metadata Filtering
- Post-filter with adaptive over-fetch: is the over-fetch multiplier correctly applied? Can it return fewer results than requested even when enough matching vectors exist?
- Is the metadata equality check correct for all value types (string, int, float, bool)?

### 5.6 Unsafe Code Review
Read every `unsafe` block in the vector module (distance.rs, heap.rs, hnsw.rs, mmap.rs, mmap_graph.rs). For each one:
- What invariant does it rely on?
- Is that invariant enforced by the caller?
- Can any input from outside the module violate it?

**Output**: List every unsafe block with its file:line, the invariant it assumes, and whether that invariant is guaranteed.

---

## Audit 6: Branch Operations

**Goal**: Verify that fork, merge, and diff produce correct results and don't lose or corrupt data.

**Read these files in full:**

- `crates/engine/src/branch_ops.rs` — fork, merge, diff implementations
- `crates/engine/src/branch_dag.rs` — branch DAG lineage tracking
- `crates/engine/src/branch_status_cache.rs` — branch status caching
- `crates/engine/src/primitives/branch/mod.rs` — branch primitive
- `crates/engine/src/primitives/branch/handle.rs` — branch handle
- `crates/engine/src/primitives/branch/index.rs` — branch index
- `crates/core/src/branch_types.rs` — branch type definitions

**Evaluate the following:**

### 6.1 Fork Correctness
- Fork copies all data from source to destination. Walk through the code and verify:
  - All type tags are iterated (KV, Event, State, JSON, Vector, VectorConfig). Are any missing?
  - Are values deep-copied or reference-shared? (They should be deep-copied since they go to a new namespace.)
  - Are version numbers preserved from the source or assigned fresh in the destination?
  - If the source branch has tombstones, are they copied to the destination? (They probably shouldn't be — the destination has no older versions to mask.)
  - If a concurrent write happens to the source branch during fork, does the fork get a consistent snapshot or a torn state?

### 6.2 Merge Correctness
- With `LastWriterWins` strategy:
  - Does "source value overwrites target" handle all cases: added in source, modified in both, deleted in source?
  - Issue #1537: does merge propagate deletes (tombstones) from source to target? If source deleted a key that target still has, is the target's key deleted?
  - What happens to version history? Does the merged key in target have the source's version or a new version?

- With `Strict` strategy:
  - Does it correctly detect ALL modifications in both branches? What counts as "modified" — any version change, or only value changes?
  - If both branches modified the same key to the same value, is that a conflict?

### 6.3 Diff Correctness
- Diff builds HashMaps of both branches and compares. Verify:
  - "Added" = in B but not in A. "Removed" = in A but not in B. "Modified" = in both but different values. Is this classification correct?
  - Value comparison: are values compared by content or by version number? (Content comparison is correct; version comparison would be wrong since forked values get new versions.)
  - Are tombstoned keys included in the diff? (They probably should be — a tombstone in one branch and a value in the other is a meaningful difference.)
  - The `as_of` timestamp parameter — does it correctly filter to only versions at or before the timestamp?

### 6.4 Branch DAG Consistency
- When a fork or merge happens, is the DAG updated atomically with the data operation?
- If the data operation succeeds but the DAG update fails, is the system in an inconsistent state?
- Can the DAG have cycles? (It shouldn't — forks and merges create a DAG, not a general graph.)

### 6.5 Vector and Graph Data During Branch Operations
- When forking, are vector collections and their HNSW indexes handled? Or only the KV records?
- When merging, are vector collections merged? What does it mean to merge two HNSW indexes?
- Are graph nodes and edges (stored under `_graph_` space) included in fork/merge/diff?

**Output**: For each operation (fork, merge, diff), describe what happens to each primitive type (KV, Event, State, JSON, Vector, Graph) and flag any gaps.

---

## Audit 7: Event Log Integrity

**Goal**: Verify that the event log maintains its append-only invariant, hash chain integrity, and monotonic sequence numbers.

**Read these files in full:**

- `crates/engine/src/primitives/event.rs` — event log implementation
- `crates/core/src/primitives/event.rs` — event types and hash chain logic

**Evaluate the following:**

### 7.1 Append-Only Invariant
- Can an event be modified after it's written? Is there any code path that overwrites an existing event?
- Can an event be deleted? Are tombstones supported for events? (They probably shouldn't be.)
- If a transaction that published an event is rolled back, is the event removed? How?

### 7.2 Hash Chain Integrity
- Each event's hash should include the previous event's hash (forming a chain). Verify:
  - What exactly is hashed? (event type + sequence number + payload + previous hash?)
  - What hash algorithm is used?
  - Is the first event in a stream handled correctly (no previous hash)?
  - If events are read back, can the chain be verified? Is there a verification function?

### 7.3 Sequence Numbers
- Sequence numbers must be monotonically increasing within an event type. Verify:
  - How are sequence numbers assigned? Is there an atomic counter per event type?
  - Can two concurrent publishers get the same sequence number?
  - After a crash and recovery, does the sequence counter resume from the correct value?
  - Are sequence numbers per-branch? (They should be — different branches should have independent sequences.)

### 7.4 Branch Operations
- When a branch is forked, are events copied? Do they keep their original sequence numbers?
- When branches are merged, what happens to events with overlapping sequence numbers?

**Output**: State whether each invariant (append-only, hash chain, monotonic sequences) HOLDS or is VIOLATED.

---

## Audit 8: JSON Path Mutations

**Goal**: Verify that JSON path operations are atomic, correctly handle nested structures, and enforce limits.

**Read these files in full:**

- `crates/engine/src/primitives/json.rs` — JSON store implementation
- `crates/core/src/primitives/json.rs` — JSON types, path parsing, patch operations

**Evaluate the following:**

### 8.1 Path Operations
- For each operation (Set, Delete, Append, Merge), trace the code path:
  - `$.user.name` SET "Alice" — does it create intermediate objects if they don't exist?
  - `$.items[0]` SET value — does it handle array index out of bounds?
  - `$.items[-1]` — is negative indexing supported? What does it mean?
  - `$.nonexistent.path` DELETE — does it silently succeed or return an error?
  - `$` SET value — does it replace the entire document?

### 8.2 Atomicity
- If a transaction applies multiple path mutations to the same document, are they applied in order?
- If one mutation in a batch fails (e.g., path not found for delete), does the entire batch roll back or do partial mutations persist?
- Is the document version incremented once per transaction or once per mutation?

### 8.3 Limits Enforcement
- Document size limit (16MB) — is it checked after every mutation or only at commit time? A sequence of small mutations could exceed the limit incrementally.
- Nesting depth limit (100 levels) — is it checked during SET operations that create nested objects?
- Array size limit (1M elements) — is it checked during Append operations?
- Path segment limit (256) — is it checked during path parsing?

### 8.4 JSONPath Parsing
- Are paths correctly parsed for: dot notation (`$.a.b`), bracket notation (`$["a"]["b"]`), array indices (`$.a[0]`), wildcards (`$.a[*]`)?
- What happens with special characters in keys (dots, brackets, quotes)?
- Is the parser safe against pathological inputs (deeply nested paths, very long paths)?

### 8.5 Serialization Round-Trip
- JSON documents are serialized (bincode) for storage. Does the round-trip preserve:
  - Number precision (f64 edge cases: NaN, Infinity, -0.0)?
  - Key ordering in objects?
  - Unicode characters?
  - Empty objects, empty arrays, null values?

**Output**: For each operation type, state whether it's CORRECT, has EDGE CASE ISSUES, or is BROKEN.

---

## Audit 9: Concurrency & Thread Safety

**Goal**: Identify data races, deadlocks, and incorrect atomic orderings across the codebase.

**Read these files (focus on lock acquisition patterns and unsafe code):**

- `crates/concurrency/src/manager.rs` — transaction manager, per-branch locks
- `crates/engine/src/coordinator.rs` — engine coordinator, flush/compaction orchestration
- `crates/engine/src/database/mod.rs` — Database struct, extension registry
- `crates/engine/src/database/registry.rs` — type-erased extension storage
- `crates/engine/src/background.rs` — background task scheduling
- `crates/durability/src/coordination.rs` — durability flush coordination (contains unsafe)
- `crates/durability/src/database/handle.rs` — database handle with locks
- `crates/storage/src/block_cache.rs` — sharded cache with locks (contains unsafe)
- `crates/storage/src/segmented/mod.rs` — ArcSwap for version snapshots
- `crates/engine/src/primitives/vector/store.rs` — vector store locks
- `crates/engine/src/primitives/vector/hnsw.rs` — HNSW with RwLock (contains unsafe)
- `crates/engine/src/search/segment.rs` — search index locks (contains unsafe)

**Evaluate the following:**

### 9.1 Lock Ordering
Map every lock in the system and identify the acquisition order. A deadlock occurs if two code paths acquire the same locks in different orders.

- List every Mutex, RwLock, and per-branch lock in the codebase
- For each code path that acquires multiple locks, document the order
- Identify any potential deadlock cycles

### 9.2 Atomic Ordering
For every `AtomicU64`, `AtomicBool`, `AtomicUsize` in the codebase:
- What ordering is used (SeqCst, AcqRel, Relaxed)?
- Is it correct? Common mistakes:
  - Using Relaxed for a counter that other threads read to make decisions
  - Using Relaxed for a flag that guards a data structure
  - Using AcqRel when SeqCst is needed for a happens-before relationship across multiple atomics

Focus on the critical atomics:
- Version counter in `manager.rs`
- Transaction ID counter in `manager.rs`
- Memtable frozen flag
- Any atomics in the block cache

### 9.3 ArcSwap Safety
`ArcSwap<SegmentVersion>` is used for lock-free reader access during compaction. Verify:
- Readers call `load()` which returns a guard. Does the guard keep the old version alive if a swap happens mid-read?
- Writers call `store()` to swap. Is the old version dropped only after all guards are released?
- Can a reader see a partially-constructed SegmentVersion?

### 9.4 Unsafe Code Review
For every `unsafe` block in the files listed above:
- What is the safety invariant?
- Is it documented?
- Can any caller (including concurrent callers) violate it?
- Is there a safe alternative that would work?

### 9.5 DashMap Usage
DashMap is used in several places. Verify:
- Are there any iterate-then-modify patterns that could miss entries or see duplicates?
- Are there any get-then-insert patterns that could race (TOCTOU)?
- Is entry lifetime correct when using `entry()` API?

**Output**: For each finding, classify as: SAFE, POTENTIAL RACE (needs more analysis), or BUG (confirmed data race or deadlock).

---

## Audit 10: TTL, Timestamps & Time-Travel

**Goal**: Verify that TTL expiration is correct, timestamps are consistent, and time-travel queries return the right data.

**Read these files in full:**

- `crates/storage/src/ttl.rs` — TTL implementation
- `crates/storage/src/memtable.rs` — TTL checking in memtable reads
- `crates/storage/src/segmented/mod.rs` — TTL in segment reads
- `crates/engine/src/primitives/kv.rs` — KV operations with time awareness
- `crates/engine/src/transaction/context.rs` — transaction time context
- `crates/engine/src/database/mod.rs` — database versioning and time
- `crates/engine/src/branch_ops.rs` — time-based diff filtering

**Evaluate the following:**

### 10.1 TTL Expiration
- When is TTL checked? On every read? On compaction? Both?
- What clock is used for TTL comparison? Wall clock (`SystemTime::now()`)? Or a logical clock?
- Issue #1534 states that TTL checks use wall-clock time even for time-travel queries. Verify: if I query at version V (which was 1 hour ago), and a key had a 30-minute TTL, does the key appear expired (wrong, because at version V it was still alive) or alive (correct)?
- If the system clock jumps backward, can non-expired keys suddenly appear expired?
- Is TTL set at write time and immutable? Or can it be modified?

### 10.2 Timestamp Consistency
- Where do timestamps come from? `SystemTime::now()` at write time?
- Are timestamps monotonic within a branch? Can a later commit have an earlier timestamp (if the clock drifts backward)?
- Are timestamps used for any ordering decisions (other than TTL)? If timestamps are used for MVCC ordering (they shouldn't be — commit IDs are), a clock skew could corrupt the database.

### 10.3 Time-Travel Queries
- The core mechanism: reads are bounded by `max_version` (the snapshot commit ID). Verify that this works:
  - Reading at version V returns the latest value with commit_id <= V
  - If the latest version is a tombstone, the key appears deleted (not the version before the tombstone)
  - If no version exists at or before V, the key appears non-existent

- Time-travel + TTL interaction:
  - If I query at version V, should I see keys that have since expired? (Arguably yes — I'm looking at the state as of V.)
  - What's the current behavior?

### 10.4 Compaction and Time-Travel
- If compaction prunes old versions, time-travel queries to those versions will return different results. Is this expected?
- Is there a mechanism to prevent compaction from pruning versions that are still needed for time-travel? (e.g., a retention policy)
- Can a user configure "keep all versions since time T" to support time-travel up to T?

### 10.5 Timestamps in WAL Records
- Are timestamps stored in WAL records? If the WAL is replayed at a later time, do the replayed entries get the original timestamp or the current time?
- If original timestamps are preserved, recovery produces correct temporal state.
- If current time is used, recovery corrupts temporal state.

**Output**: For each question, state the CURRENT BEHAVIOR and whether it's CORRECT, INCORRECT, or UNDEFINED (no specified behavior).

---

## How to Use These Prompts

1. Send each prompt as a separate conversation with full codebase access
2. Instruct the model to read every listed file completely before starting evaluation
3. Collect findings into issues — tag with `priority-critical` for data loss bugs, `priority-high` for correctness issues, `priority-medium` for edge cases
4. After all 10 audits, prioritize fixes by blast radius: Layer 0 > Layer 1 > Layer 2 > Layer 3 > Layer 4
