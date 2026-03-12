# Strata Architecture

> Comprehensive reference for the Strata embedded database. Covers all core crates,
> data structures, protocols, and design decisions. Version 0.6.0.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Core Types (`strata-core`)](#2-core-types)
3. [Storage Layer (`strata-storage`)](#3-storage-layer)
4. [Durability Layer (`strata-durability`)](#4-durability-layer)
5. [Concurrency Layer (`strata-concurrency`)](#5-concurrency-layer)
6. [Engine Layer (`strata-engine`)](#6-engine-layer)
7. [Executor Layer (`strata-executor`)](#7-executor-layer)
8. [Cross-Cutting Design Decisions](#8-cross-cutting-design-decisions)
9. [Known Limitations & Future Work](#9-known-limitations--future-work)

---

## 1. Overview

### What Strata Is

Strata is an **embedded MVCC database** with AI-native primitives. It provides:

- **6 built-in primitives**: KV, StateCell, EventLog, JsonStore, VectorStore, GraphStore
- **Snapshot isolation** via optimistic concurrency control (OCC)
- **Write-ahead logging** with configurable durability modes
- **Embedded vector search** (HNSW) with automatic embedding
- **Single-binary, zero-config** deployment

### Design Philosophy

- **Embedded**: Library linked into the application process. No separate server.
- **Single-binary**: All capabilities ship in one artifact.
- **Zero-config**: Sensible defaults. `strata.toml` for optional tuning.
- **MVCC**: Every write creates a new version. Reads never block writes.
- **Layered**: Each crate has a single responsibility and clear dependency direction.

### Crate Dependency Graph

```
                     ┌──────────┐
                     │   cli    │
                     └────┬─────┘
                          │
                     ┌────▼─────┐
                     │ executor │
                     └──┬───┬───┘
                        │   │
          ┌─────────────┤   ├──────────────┐
          │             │   │              │
   ┌──────▼──────┐  ┌──▼───▼──┐  ┌────────▼────────┐
   │ intelligence │  │ search  │  │    security     │
   └──────┬───────┘  └──┬──────┘  └─────────────────┘
          │             │
          └──────┬──────┘
                 │
            ┌────▼─────┐
            │  engine   │
            └──┬──┬──┬──┘
               │  │  │
     ┌─────────┘  │  └──────────┐
     │            │             │
┌────▼──────┐ ┌──▼───────┐ ┌───▼──────────┐
│ storage   │ │ durability│ │ concurrency  │
└────┬──────┘ └──┬───────┘ └───┬──────────┘
     │           │             │
     └───────────┼─────────────┘
                 │
            ┌────▼─────┐
            │   core   │
            └──────────┘
```

**Dependency rules:**
- `core` has zero internal dependencies.
- `storage`, `durability` depend only on `core`.
- `concurrency` depends on `core` + `storage` + `durability`.
- `engine` depends on `core` + `storage` + `concurrency` + `durability`.
- `executor` depends on `engine` + `intelligence` + `search` + `security`.
- `cli` depends only on `executor`.

---

## 2. Core Types

> Source: `crates/core/src/`

The core crate defines the foundational type system. It has zero internal
dependencies and is imported by every other crate.

### 2.1 Value Model

```rust
// crates/core/src/value.rs:44
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Box<Vec<Value>>),
    Object(Box<HashMap<String, Value>>),
}
```

**8 variants, frozen.** No additional variants will be added.

**Design invariants:**

| Rule | Description |
|------|-------------|
| VAL-1 | Exactly 8 canonical types. No future additions. |
| VAL-2 | No implicit coercions. `Int(1) != Float(1.0)` always. |
| VAL-3 | Cross-type `PartialEq` always returns `false`. |
| VAL-5 | IEEE-754 semantics: `NaN != NaN`, `-0.0 == 0.0`. |

**Memory optimization:** `Array` and `Object` are `Box`-wrapped, reducing `Value` size
from ~56 to ~24 bytes. At 128M entries this saves ~3.8 GB.

### 2.2 Key Model

```rust
// crates/core/src/types.rs:231
pub struct Key {
    pub namespace: Arc<Namespace>,
    pub type_tag: TypeTag,
    pub user_key: Box<[u8]>,
}
```

Keys are composite: `namespace + type_tag + user_key`. BTreeMap ordering follows
this field order, enabling efficient prefix scans.

**Namespace** encodes the hierarchy:

```rust
// crates/core/src/types.rs:60
pub struct Namespace {
    pub tenant: String,
    pub app: String,
    pub agent: String,
    pub branch_id: BranchId,
    pub space: String,   // Default: "default"
}
```

`Arc<Namespace>` is shared across keys in the same namespace, reducing allocation
overhead.

`user_key` uses `Box<[u8]>` instead of `Vec<u8>`, saving 8 bytes per key
(no capacity field). At 128M entries this saves ~1 GB.

**Validation:** Keys starting with `_strata/` are reserved for internal use and
rejected at the validation layer.

### 2.3 TypeTag

```rust
// crates/core/src/types.rs:150
#[repr(u8)]
pub enum TypeTag {
    KV     = 0x01,
    Event  = 0x02,
    State  = 0x03,
    Trace  = 0x04,   // Deprecated, preserved for deserialization
    Branch = 0x05,
    Space  = 0x06,
    Vector = 0x10,
    Json   = 0x11,
    VectorConfig = 0x12,
}
```

**Frozen enum.** Values are burned into on-disk WAL format. The `repr(u8)` ensures
direct byte encoding. New primitive types require a new `TypeTag` variant and
corresponding WAL/snapshot handling.

### 2.4 BranchId

```rust
// crates/core/src/types.rs:14
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BranchId(Uuid);
```

Wraps a UUID v4 (random). Used as the partition key in `DashMap` for
branch-sharded storage.

**API:** `new()` (random), `from_bytes([u8; 16])`, `from_string(&str)`.

### 2.5 Storage Trait

```rust
// crates/core/src/traits.rs:69
pub trait Storage: Send + Sync {
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>>;
    fn get_versioned(&self, key: &Key, max_version: u64)
        -> StrataResult<Option<VersionedValue>>;
    fn get_history(&self, key: &Key, limit: Option<usize>, before_version: Option<u64>)
        -> StrataResult<Vec<VersionedValue>>;
    fn put(&self, key: Key, value: Value, ttl: Option<Duration>) -> StrataResult<u64>;
    fn delete(&self, key: &Key) -> StrataResult<Option<VersionedValue>>;
    fn scan_prefix(&self, prefix: &Key, max_version: u64)
        -> StrataResult<Vec<(Key, VersionedValue)>>;
    fn scan_by_branch(&self, branch_id: BranchId, max_version: u64)
        -> StrataResult<Vec<(Key, VersionedValue)>>;
    fn current_version(&self) -> u64;
    fn put_with_version(&self, key: Key, value: Value, version: u64, ttl: Option<Duration>)
        -> StrataResult<()>;
    fn put_with_version_mode(&self, key: Key, value: Value, version: u64,
        ttl: Option<Duration>, mode: WriteMode) -> StrataResult<()>;
    fn delete_with_version(&self, key: &Key, version: u64) -> StrataResult<()>;
    fn get_version_only(&self, key: &Key) -> StrataResult<Option<u64>>;
}
```

**MVCC semantics:** `get_versioned(key, max_version)` returns the latest version
<= `max_version`, enabling snapshot isolation without data copies.

**Visibility guarantee:** `put()` on thread A happens-before `get()` on thread B.
Implementors must provide internal synchronization.

### 2.6 SnapshotView Trait

```rust
// crates/core/src/traits.rs:246
pub trait SnapshotView: Send + Sync {
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>>;
    fn scan_prefix(&self, prefix: &Key) -> StrataResult<Vec<(Key, VersionedValue)>>;
    fn get_value_and_version(&self, key: &Key) -> StrataResult<Option<(Value, u64)>>;
    fn version(&self) -> u64;
}
```

**Immutable point-in-time view.** Repeated reads return identical results.
All results reflect state at exactly `version()`. Snapshots hold `Arc` references
to underlying storage, which can prevent garbage collection of old versions.

### 2.7 WriteMode

```rust
// crates/core/src/traits.rs:13
pub enum WriteMode {
    #[default]
    Append,   // Standard MVCC: push new version, keep history
    Replace,  // Single-version: drop old versions, keep only latest
}
```

**`Append`** is the default for user-facing data (KV, JSON). Full version history
is preserved.

**`Replace`** is used internally by graph adjacency lists, which write
thousands of times per operation. Keeping history for graph edges would waste
unbounded memory with no utility.

### 2.8 Version and Timestamp

```rust
// crates/core/src/contract/version.rs:26
pub enum Version {
    Txn(u64),       // Transaction-based (KV, JSON, Vector, Branch)
    Sequence(u64),  // Position in append-only log (EventLog)
    Counter(u64),   // Per-entity mutation counter (StateCell)
}
```

**Cross-variant comparison returns `None`** — comparing a transaction version to
an event sequence number is meaningless.

```rust
// crates/core/src/contract/timestamp.rs:29
pub struct Timestamp(u64);  // Microseconds since Unix epoch
```

Precision: 1 microsecond. Range: ~584,554 years. Constructed via `Timestamp::now()`,
`from_micros(u64)`, `from_millis(u64)`, `from_secs(u64)`.

### 2.9 Error Hierarchy

10 canonical error codes:

| Code | Meaning | Retryable? |
|------|---------|------------|
| `NotFound` | Entity or key doesn't exist | No |
| `WrongType` | Wrong primitive or value type | No |
| `InvalidKey` | Key validation failed | No |
| `InvalidPath` | JSON path invalid | No |
| `HistoryTrimmed` | Version unavailable (GC'd) | No |
| `ConstraintViolation` | Structural failure (dimension mismatch, capacity) | No |
| `Conflict` | Version mismatch, write conflict | **Yes** |
| `SerializationError` | Invalid Value encoding | No |
| `StorageError` | Disk or WAL failure | No |
| `InternalError` | Bug or invariant violation | No |

`Conflict` is the only retryable error. Application code should retry the
transaction from scratch on conflict.

---

## 3. Storage Layer

> Source: `crates/storage/src/`

The storage layer provides the in-memory MVCC store. It is a pure in-memory
data structure with no disk I/O — durability is handled by the durability layer.

### 3.1 ShardedStore

```rust
// crates/storage/src/sharded.rs:377
pub struct ShardedStore {
    shards: DashMap<BranchId, Shard>,
    version: AtomicU64,
    max_branches: AtomicUsize,  // 0 = unlimited
}
```

**`DashMap`** provides 16-way lock-free sharding by `BranchId`. Different branches
never contend. The global `version` counter is monotonically increasing and used
for snapshot creation.

### 3.2 Shard

```rust
// crates/storage/src/sharded.rs:279
pub struct Shard {
    pub(crate) data: FxHashMap<Arc<Key>, VersionChain>,
    pub(crate) ordered_keys: Option<BTreeSet<Arc<Key>>>,
    pub(crate) ttl_index: TTLIndex,
}
```

- **`FxHashMap`**: Non-cryptographic hash for O(1) lookups.
- **`Arc<Key>`**: Shared between `FxHashMap` and `BTreeSet` to reduce allocation.
- **`ordered_keys`**: Lazy — built on first `scan_prefix()`, maintained incrementally
  after. Saves memory when scans are rare.
- **`TTLIndex`**: Maps expiry timestamps to key sets for O(expired) cleanup.

### 3.3 VersionChain

```rust
// crates/storage/src/sharded.rs:46
enum VersionStorage {
    Single(StoredValue),            // No heap allocation
    Multi(VecDeque<StoredValue>),   // Newest-first ordering
}

pub struct VersionChain {
    versions: VersionStorage,
}
```

**Single/Multi optimization:** Most keys have exactly one version. `Single` avoids
the 72-byte VecDeque allocation entirely. On the second write, `Single` promotes
to `Multi` via `VecDeque::with_capacity(2)`.

**Newest-first ordering:** `latest()` is O(1) because the hot path reads the
most recent version. This is the common case for application reads.

**WriteMode handling:**

```rust
pub fn put(&mut self, value: StoredValue, mode: WriteMode) {
    match mode {
        WriteMode::Append     => self.push(value),            // Add version
        WriteMode::KeepLast(1) => self.replace_latest(value), // Drop history
        WriteMode::KeepLast(n) => { self.push(value); self.truncate_to(n); }
    }
}
```

**GC:**

```rust
pub fn gc(&mut self, min_version: u64) -> usize {
    // Remove versions < min_version, always keeping at least one.
    // Returns number of versions pruned.
}

pub fn is_dead(&self) -> bool {
    // True if single tombstone or expired entry — can be fully removed.
    self.version_count() == 1
        && self.latest().is_some_and(|sv| sv.is_tombstone() || sv.is_expired())
}
```

### 3.4 StoredValue

```rust
// crates/storage/src/stored_value.rs:56
pub struct StoredValue {
    value: Value,          // 24 bytes (boxed variants)
    version: u64,          // 8 bytes (raw, not Version enum)
    timestamp: Timestamp,  // 8 bytes
    ttl_and_flags: u64,    // 8 bytes (packed)
}
// Total: 56 bytes (compile-time assertion)
```

**TTL and tombstone packing:**

```
Bit 63:    Tombstone flag (1 = tombstone)
Bits 0-62: TTL in milliseconds (0 = no TTL)
```

This packing saves 16 bytes per entry compared to `Option<Duration>` + `bool`.
At 128M entries, the savings total ~2.9 GB.

Sub-millisecond TTLs truncate to zero and read back as `None`. In practice,
TTLs are seconds or longer.

### 3.5 TTLIndex

```rust
// crates/storage/src/ttl.rs:19
pub struct TTLIndex {
    index: BTreeMap<Timestamp, HashSet<Key>>,
}
```

Operations:

- `insert(expiry, key)` — Register key with expiry time.
- `remove(expiry, key)` — Deregister key.
- `find_expired(now)` — O(expired) range query via `BTreeMap::range(..=now)`.
- `remove_expired(now)` — Clean up stale entries.

### 3.6 Branch Limits

Branch limits prevent runaway branch creation. Enforcement is inline in `put()`
and `apply_batch()`:

```rust
// Snapshot len() BEFORE entry() to avoid DashMap deadlock
let max = self.max_branches.load(Ordering::Relaxed);
let current_len = if max > 0 { self.shards.len() } else { 0 };

let shard = match self.shards.entry(branch_id) {
    Occupied(e) => e.into_ref(),
    Vacant(e) => {
        if max > 0 && current_len >= max {
            return Err(StrataError::capacity_exceeded(...));
        }
        e.insert(Shard::default())
    }
};
```

The `len()` snapshot before `entry()` prevents a deadlock: `DashMap::len()` acquires
read locks on all shards, which would deadlock if we already held a write lock
via `entry()`.

### 3.7 Garbage Collection

```rust
// Called explicitly — not automatic
pub fn gc_branch(&self, branch_id: &BranchId, min_version: u64) -> GcStats {
    // For each key in branch:
    //   1. Remove versions < min_version (keep at least one)
    //   2. Remove fully dead keys (single tombstone/expired)
}
```

GC is coordinated with active snapshots via `min_active_version`. No version
visible to any active snapshot is ever pruned.

### 3.8 Performance Summary

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `get()` | O(1) | DashMap read + FxHashMap lookup |
| `put()` | O(1) amortized | FxHashMap insert + version chain push |
| `get_versioned()` | O(1)–O(4) | Usually O(1) with <4 versions per key |
| `scan_prefix()` | O(log n + k) | BTreeSet range + FxHashMap lookups |
| `scan_by_branch()` | O(branch_size) | Via BranchIndex |
| `find_expired()` | O(expired) | BTreeMap range query |
| `gc()` | O(pruned) | Per-key version pruning |

---

## 4. Durability Layer

> Source: `crates/durability/src/`

The durability layer provides crash recovery via a write-ahead log (WAL),
a codec system for at-rest encryption, and a MANIFEST for recovery metadata.

### 4.1 WAL Architecture

#### Durability Modes

```rust
// crates/durability/src/wal/mode.rs
pub enum DurabilityMode {
    Cache,                                       // No fsync, all data lost on crash
    Always,                                      // fsync after every commit
    Standard { interval_ms: u64, batch_size: usize }, // Periodic fsync (default)
}
```

| Mode | fsync Behavior | Latency | Data Loss Window |
|------|---------------|---------|-----------------|
| `Cache` | Never | <3 us | Everything |
| `Always` | Every commit | ~10 ms | Zero |
| `Standard` | Every 100ms or 1000 commits | <30 us | Up to batch interval |

Default: `Standard { interval_ms: 100, batch_size: 1000 }`.

#### Segment Format

```
┌─────────────────────────────────────┐
│ Segment Header (36 bytes, v2)       │
│ ┌─────────────────────────────────┐ │
│ │ Magic: "STRA" (4 bytes)         │ │
│ │ Format version: u32 LE          │ │
│ │ Segment number: u64 LE          │ │
│ │ Database UUID: [u8; 16]         │ │
│ │ Header CRC32: u32 LE            │ │
│ └─────────────────────────────────┘ │
├─────────────────────────────────────┤
│ Record 1                            │
│ Record 2                            │
│ ...                                 │
└─────────────────────────────────────┘
```

Constants:

- `SEGMENT_MAGIC`: `0x53545241` ("STRA")
- `SEGMENT_FORMAT_VERSION`: 2
- `SEGMENT_HEADER_SIZE_V2`: 36 bytes

#### WAL Record Format

```
┌──────────┬──────────┬──────────────────┬──────────┐
│ Len (u32)│ Fmt (u8) │ Payload (var)    │ CRC32    │
└──────────┴──────────┴──────────────────┴──────────┘

Payload:
┌──────────┬──────────────┬───────────┬──────────────┐
│ TxnId    │ BranchId     │ Timestamp │ Writeset     │
│ (u64 LE) │ ([u8; 16])   │ (u64 LE)  │ (variable)   │
└──────────┴──────────────┴───────────┴──────────────┘
```

Each record is length-prefixed and CRC32-checksummed. The payload contains a
complete committed transaction.

#### Segment Rotation

Segments rotate when they exceed the configured size threshold (default: 64 MB).
The writer creates a new segment file, bumps the segment number, and continues.

#### Pre-Serialization Optimization

Records are serialized and CRC-computed **outside** the WAL writer lock. Only
the final `append()` (a memcpy) happens inside the lock. This reduces lock hold
time to microseconds.

#### WAL Writer

```rust
// crates/durability/src/wal/writer.rs
pub struct WalWriter {
    segment: Option<WalSegment>,         // None in Cache mode
    durability: DurabilityMode,
    wal_dir: PathBuf,
    database_uuid: [u8; 16],
    config: WalConfig,
    codec: Box<dyn StorageCodec>,
    // Sync tracking
    bytes_since_sync: u64,
    writes_since_sync: usize,
    last_sync_time: Instant,
    // ...
}
```

**Cache mode:** No segment file is created. `append()` is a no-op.

**Standard mode:** A background flush thread runs fsync periodically (~100ms).
A safety-net inline fsync triggers if the background thread stalls (3x interval).

#### WAL Reader

```rust
// crates/durability/src/wal/reader.rs
pub struct WalReader {
    codec: Box<dyn StorageCodec>,
}
```

**Corruption handling:** On invalid record, scans forward up to 8 MB
(`MAX_RECOVERY_SCAN_WINDOW`) seeking the next valid record. Tracks `ReadStopReason`
(EndOfSegment, Corruption, IncompleteRecord).

### 4.2 Codec System

```rust
// crates/durability/src/codec/traits.rs
pub trait StorageCodec: Send + Sync {
    fn encode(&self, data: &[u8]) -> Vec<u8>;
    fn encode_cow<'a>(&self, data: &'a [u8]) -> Cow<'a, [u8]>;
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError>;
    fn codec_id(&self) -> &str;
    fn clone_box(&self) -> Box<dyn StorageCodec>;
}
```

**IdentityCodec** (default): Zero-copy via `Cow::Borrowed`. No overhead.

**AES-256-GCM codec**: Reads 32-byte key from `STRATA_ENCRYPTION_KEY` env var.
Full authenticated encryption. Available but not activated by default.

The codec is determined at database creation time and stored in MANIFEST.
A codec mismatch at open time produces a `CodecError::CodecMismatch`.

**Known limitation:** No codec migration path. Upgrading from identity to AES-GCM
requires export/delete/reimport.

### 4.3 MANIFEST

```rust
// crates/durability/src/format/manifest.rs
pub struct Manifest {
    pub format_version: u32,
    pub database_uuid: [u8; 16],
    pub codec_id: String,
    pub active_wal_segment: u64,
    pub snapshot_watermark: Option<u64>,
    pub snapshot_id: Option<u64>,
}
```

**Binary layout:**

```
Magic "STRM" (4)
Format Version (u32 LE)
Database UUID (16 bytes)
Codec ID Length (u32 LE)
Codec ID (UTF-8, variable)
Active WAL Segment (u64 LE)
Snapshot Watermark (u64 LE, 0 = none)
Snapshot ID (u64 LE, 0 = none)
CRC32 (u32 LE)
```

### 4.4 Recovery Flow

```
1. Load MANIFEST → validate codec → determine recovery path
2. If snapshot exists: load snapshot → replay WAL after watermark
3. If no snapshot: replay all WAL segments in order
4. Truncate partial records at WAL tail
5. Initialize version counter from max recovered version
```

Recovery is **deterministic** (same inputs → same state) and **idempotent**
(multiple recoveries → same result). WAL records contain only committed
transactions, so validation is not re-run during replay.

### 4.5 Compaction

Two modes:

| Mode | Behavior | Safety |
|------|----------|--------|
| `WALOnly` | Remove segments covered by snapshot | Safe — all history preserved |
| `Full` | Remove segments + apply retention policy | Aggressive — removes old versions |

**Invariants:**
- Compaction is user-triggered (no background compaction).
- Compaction is logically invisible (read results unchanged).
- **Version IDs never change** during compaction.

### 4.6 Branch Bundles

Format: `.branchbundle.tar.zst` (Zstandard-compressed tar archive).

```
branchbundle/
├── MANIFEST.json     — format version, XXH3 checksums
├── BRANCH.json       — branch metadata (name, state, timestamps)
└── WAL.branchlog     — binary log (msgpack v2, one entry per transaction)
```

**Export constraint:** Only terminal branches (Completed, Failed, Cancelled,
Archived) can be exported. Bundles are immutable and deterministic.

### 4.7 Multi-Process Coordination

#### WAL File Lock

```rust
// crates/durability/src/coordination.rs
pub struct WalFileLock {
    _file: File,  // Exclusive lock held while alive
}
```

Lock file: `{wal_dir}/.wal-lock`. Writes PID for stale-lock detection.

**Stale lock recovery:** If the lock holder process is dead (`kill(pid, 0)` returns
ESRCH), the lock file is removed and reacquired.

#### Counter File

```
// {wal_dir}/counters — 16 bytes
[0..8):  max_version (u64 LE)
[8..16): max_txn_id  (u64 LE)
```

Read and written while holding `WalFileLock`. `sync_all()` ensures durability.

---

## 5. Concurrency Layer

> Source: `crates/concurrency/src/`

The concurrency layer implements optimistic concurrency control (OCC) with
snapshot isolation. It manages transaction lifecycle, conflict detection,
and commit protocol.

### 5.1 Transaction Lifecycle

```
    ┌──────────┐
    │  Active   │ ← begin_transaction()
    └────┬─────┘
         │
         ├── (abort) ──────────────────┐
         │                             │
    ┌────▼──────┐                      │
    │ Validating │                     │
    └────┬──────┘                      │
         │                             │
         ├── (conflicts) ─────────┐    │
         │                        │    │
    ┌────▼──────┐           ┌─────▼────▼─┐
    │ Committed  │           │  Aborted    │
    └───────────┘           └─────────────┘
```

`Committed` and `Aborted` are terminal states.

### 5.2 TransactionContext

```rust
// crates/concurrency/src/transaction.rs:370
pub struct TransactionContext {
    // Identity
    pub txn_id: u64,
    pub branch_id: BranchId,
    pub start_version: u64,

    // Snapshot
    snapshot: Option<Box<dyn SnapshotView>>,

    // Operation tracking
    pub read_set: HashMap<Key, u64>,       // Key → version read
    pub write_set: HashMap<Key, Value>,    // Key → uncommitted value
    pub delete_set: HashSet<Key>,          // Keys marked for deletion
    pub cas_set: Vec<CASOperation>,        // CAS operations

    // JSON operations (lazy allocation)
    json_reads: Option<Vec<JsonPathRead>>,
    json_writes: Option<Vec<JsonPatchEntry>>,
    json_snapshot_versions: Option<HashMap<Key, u64>>,

    // State
    pub status: TransactionStatus,
    start_time: Instant,

    // Configuration
    read_only: bool,                    // Per-txn read-only mode flag
    max_write_buffer_entries: usize,    // 0 = unlimited
}
```

**Read-your-writes:** When reading a key, the transaction checks in order:

1. `write_set` → return uncommitted value (if written in this txn)
2. `delete_set` → return `None` (if deleted in this txn)
3. `snapshot` → return value, track in `read_set` with version

### 5.3 OCC Validation

**Conflict detection** is read-set based, not write-set based:

```rust
// crates/concurrency/src/validation.rs:176
pub fn validate_read_set<S: Storage>(
    read_set: &HashMap<Key, u64>,
    store: &S,
) -> StrataResult<ValidationResult> {
    for (key, read_version) in read_set {
        let current_version = store.get_version_only(key)?;
        if current_version != *read_version {
            // Conflict: another transaction modified this key
            conflicts.push(ConflictType::ReadWriteConflict { ... });
        }
    }
}
```

**First-committer-wins:** The first transaction to commit wins. Later transactions
that read the same keys will see version changes and be aborted.

**Conflict types:**

| Type | Trigger |
|------|---------|
| `ReadWriteConflict` | Key version changed since read |
| `CASConflict` | Expected version doesn't match current |
| `JsonDocConflict` | JSON document version changed |
| `JsonPathReadWriteConflict` | Overlapping JSON paths |
| `JsonPathWriteWriteConflict` | Overlapping JSON write paths |

### 5.4 Write Skew

**Write skew is intentionally allowed.** Strata uses Snapshot Isolation (SI),
not Serializable Isolation. The classic write skew scenario:

```
T1: read(A), write(B)  — passes validation (A unchanged, B not in read set)
T2: read(B), write(A)  — passes validation (B unchanged, A not in read set)
Both commit. Invariant A+B=constant violated.
```

**Mitigation strategies** (application-level):
1. Use CAS for atomic check-and-set
2. Read and write the same key
3. Application-level locking for cross-key invariants

### 5.5 CAS Validation

```rust
// crates/concurrency/src/transaction.rs:167
pub struct CASOperation {
    pub key: Key,
    pub expected_version: u64,  // 0 = key must not exist
    pub new_value: Value,
}
```

CAS is validated independently from the read set. `expected_version=0` means
"key must not exist" (insert-if-not-exists pattern).

### 5.6 Commit Protocol

**Per-branch commit lock** serializes commits within a branch:

```rust
// crates/concurrency/src/manager.rs
pub struct TransactionManager {
    version: AtomicU64,
    next_txn_id: AtomicU64,
    commit_locks: DashMap<BranchId, Mutex<()>>,
}
```

**Commit sequence (under per-branch lock):**

```
1. Acquire per-branch commit lock
2. Validate read set against current storage state
3. Validate CAS set
4. Validate JSON document versions
5. Mark transaction Committed
6. Allocate commit version (atomic fetch_add)
7. Build TransactionPayload (writes + deletes)
8. Append WalRecord ← DURABILITY POINT
9. Apply writes to storage (apply_batch)
10. Release lock
```

**TOCTOU prevention:** Steps 2-9 are all under the per-branch lock. No other
transaction on the same branch can modify storage between validation and apply.

**Durability guarantee:** The WAL write (step 8) is the durability point. If
the process crashes before the WAL write, the transaction is lost. If it crashes
after, the transaction is recovered during WAL replay.

#### Blind Write Fast Path

Transactions with no reads (empty read set) skip the commit lock entirely.
Blind writes are commutative — the only ordering guarantee is version
monotonicity, which is ensured by `fetch_add`. No TOCTOU risk exists because
there is no time-of-check.

### 5.7 Version Allocation

```rust
pub fn allocate_version(&self) -> u64 {
    self.version.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
}
```

`fetch_add` with `wrapping_add` — overflow wraps after 2^64 versions. At 1 billion
transactions per second, this takes ~584 years.

Gaps in version numbers are accepted. If a transaction allocates a version but
then fails to commit (e.g., WAL write error), that version number is simply
skipped. This does not affect correctness.

### 5.8 Transaction Pool

`TransactionContext` objects are reused from a thread-local pool to avoid repeated
allocation of `HashMap`, `HashSet`, and `VecDeque` structures. After warmup,
`begin_transaction()` and `end_transaction()` are zero-alloc.

### 5.9 GC Coordination

```
min_active_version = min(active_snapshot.version for all active snapshots)
```

Garbage collection must not prune any version >= `min_active_version`. This
ensures active snapshots always see consistent data.

### 5.10 Recovery

WAL replay via `RecoveryCoordinator` + `WalReplayer`:

```rust
// crates/concurrency/src/recovery.rs
pub struct RecoveryCoordinator {
    wal_dir: PathBuf,
    snapshot_path: Option<PathBuf>,  // Future phase
}
```

**Replay semantics:**
- Replays do NOT re-run conflict detection.
- Replays apply commit decisions, not re-execute logic.
- Replays are single-threaded.
- Versions are preserved exactly (not recalculated).

---

## 6. Engine Layer

> Source: `crates/engine/src/`

The engine layer orchestrates storage, concurrency, and durability into a
unified `Database` type. It provides the 6 primitives and the transaction API.

### 6.1 Database

```rust
// crates/engine/src/database/mod.rs:163
pub struct Database {
    data_dir: PathBuf,
    storage: Arc<ShardedStore>,
    wal_writer: Option<Arc<ParkingMutex<WalWriter>>>,
    persistence_mode: PersistenceMode,         // Ephemeral vs Disk
    coordinator: TransactionCoordinator,
    durability_mode: DurabilityMode,
    accepting_transactions: AtomicBool,
    extensions: DashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    config: parking_lot::RwLock<StrataConfig>,
    flush_shutdown: Arc<AtomicBool>,
    flush_handle: ParkingMutex<Option<JoinHandle<()>>>,
    scheduler: BackgroundScheduler,
    _lock_file: Option<std::fs::File>,
    wal_dir: PathBuf,
}
```

**Key design decisions:**
- `PersistenceMode` (Ephemeral vs Disk) is orthogonal to `DurabilityMode` (Cache/Standard/Always).
- `parking_lot` mutexes prevent lock poisoning.
- `AtomicBool` for shutdown coordination (no locking needed).
- Flush thread runs in Standard mode, syncing WAL periodically.

### 6.2 Transaction API

**Closure API** (recommended):

```rust
pub fn transaction<F, T>(&self, branch_id: BranchId, f: F) -> StrataResult<T>
where F: FnOnce(&mut TransactionContext) -> StrataResult<T>
```

Single attempt, auto-rollback on error.

**Retry API:**

```rust
pub(crate) fn transaction_with_retry<F, T>(
    &self, branch_id: BranchId, config: RetryConfig, f: F,
) -> StrataResult<T>
where F: Fn(&mut TransactionContext) -> StrataResult<T>
```

Exponential backoff: `delay = base * 2^attempt`, capped at `max_delay_ms`.
Only conflict errors are retried.

**RetryConfig defaults:** `max_retries: 3`, `base_delay_ms: 10`, `max_delay_ms: 100`.

**commit_internal flow:**

1. `begin_transaction(branch_id)` — Acquire context from thread-local pool
2. Execute closure
3. `commit_with_wal_arc(txn, wal)` — Validate + version + WAL + apply
4. `end_transaction(txn)` — Return context to pool

### 6.3 The 6 Primitives

All primitives are **stateless facades** over `Arc<Database>`. Multiple instances
share nothing; all operations go through the `Database`.

#### KVStore

```rust
// crates/engine/src/primitives/kv.rs:51
pub struct KVStore { db: Arc<Database> }
```

Standard key-value with MVCC history. Operations: `get`, `put`, `delete`, `list`,
`batch_put`, `get_versioned`.

#### StateCell

```rust
// crates/engine/src/primitives/state.rs:78
pub struct StateCell { db: Arc<Database> }
```

Atomic cells with compare-and-swap. Operations: `init`, `get`, `set`, `cas`.
Returns `Versioned<Value>` (value + version metadata) for CAS operations.

#### EventLog

```rust
// crates/engine/src/primitives/event.rs:264
pub struct EventLog { db: Arc<Database> }
```

Append-only with SHA-256 hash chaining. Operations: `append`, `get`,
`get_by_type`, `verify_chain`. Each event includes a causal hash linking
to the previous event.

#### JsonStore

```rust
// crates/engine/src/primitives/json.rs:168
pub struct JsonStore { db: Arc<Database> }
```

JSON documents with path-based access. Operations: `create`, `get` (with path),
`set` (at path), `delete` (at path), `list`. Supports region-based conflict
detection for concurrent JSON modifications.

#### VectorStore

```rust
// crates/engine/src/primitives/vector/store.rs:126
pub struct VectorStore { db: Arc<Database> }
```

HNSW vector index with mmap acceleration. Operations: `create_collection`,
`upsert`, `get`, `delete`, `search`, `collection_stats`.

**Backend state** is stored as a type-erased extension: `db.extension::<VectorBackendState>()`
ensures all VectorStore instances share the same in-memory HNSW backends.

#### GraphStore

```rust
// crates/engine/src/graph/mod.rs:32
pub struct GraphStore { db: Arc<Database> }
```

Property graph overlay on KV storage. Uses `WriteMode::KeepLast(1)` for adjacency
lists (no history needed). Operations: `create_graph`, `add_node`, `add_edge`,
`remove_node`, `remove_edge`, `neighbors`, `list_graphs`.

### 6.4 Vector Index Deep Dive

#### Single-Heap Design

```
┌───────────────────────────┐
│       VectorHeap          │  ← Single authoritative source
│  (BTreeMap<VectorId,      │     for all embeddings
│   Vec<f32>>)              │
└──────────┬────────────────┘
           │ shared reference
   ┌───────┼───────────┐
   │       │           │
   ▼       ▼           ▼
Active   Sealed₁     Sealed₂    ← Segments have graph-only
Buffer   (Compact    (Compact      structure, no embedding
         HnswGraph)  HnswGraph)    duplication
```

Before the single-heap design, each sealed segment owned a copy of its
embeddings, causing ~50% memory duplication. Now the global `VectorHeap`
is the single source of truth.

#### SegmentedHnswBackend

```rust
// crates/engine/src/primitives/vector/segmented.rs:247
pub struct SegmentedHnswBackend {
    config: SegmentedHnswConfig,
    vector_config: VectorConfig,
    heap: VectorHeap,                        // Global embedding storage
    active: ActiveBuffer,                    // O(1) insert buffer (brute-force)
    sealed: Vec<SealedSegment>,              // HNSW segments
    next_segment_id: u64,
    pending_timestamps: BTreeMap<VectorId, u64>,
    flush_path: Option<PathBuf>,
}
```

| Config | Default | Description |
|--------|---------|-------------|
| `seal_threshold` | 50,000 | Active buffer size that triggers seal |
| `heap_flush_threshold` | 500,000 | Heap size that triggers mmap flush |

When the active buffer reaches `seal_threshold`, it is sealed into a
`CompactHnswGraph` and moved to the `sealed` list. Search fans out across
all segments.

#### CompactHnswGraph

```rust
// crates/engine/src/primitives/vector/hnsw.rs
pub(crate) struct CompactHnswGraph {
    neighbor_data: NeighborData,                    // Flat u64 array
    dense_nodes: Vec<Option<CompactHnswNode>>,      // O(1) lookup
    id_offset: u64,
    node_count: usize,
    entry_point: Option<VectorId>,
    max_level: usize,
}

pub(crate) enum NeighborData {
    Owned(Vec<u64>),
    Mmap { mmap: memmap2::Mmap, byte_offset: usize, len: usize },
}
```

Per-neighbor overhead: 8 bytes (one `u64`), down from ~48 bytes with `BTreeSet`.

#### Mmap Acceleration

| File | Format | Content |
|------|--------|---------|
| `.vec` | Binary F32 array | Heap embeddings |
| `.hgr` | SHGR magic + node entries | Graph structure |
| `segments.manifest` | JSON | Segment metadata |

**Recovery:** Load heap from `.vec` mmap → scan KV for timestamps → load graphs
from `.hgr` mmap or rebuild from heap if missing.

**Alignment:** Neighbor data in `.hgr` files is padded to 8-byte alignment for
safe `u64` reinterpretation. Requires little-endian architecture (compile-time check).

### 6.5 Extensions

```rust
// crates/engine/src/database/mod.rs:193
extensions: DashMap<TypeId, Arc<dyn Any + Send + Sync>>
```

Type-erased shared state keyed by `TypeId`. Accessed via `db.extension::<T>()`.

**Current extensions:**
- `VectorBackendState` — Shared HNSW backends for all VectorStore instances
- `AutoEmbedState` — Tracks created shadow collections (embed feature)
- `EmbedBuffer` — Write-behind embedding queue

**Tradeoff:** Flexibility (any type can be stored) at the cost of compile-time
safety (no static guarantee that an extension exists).

### 6.6 Configuration

```rust
// crates/engine/src/database/config.rs:99
pub struct StrataConfig {
    pub durability: String,           // "standard" | "always" | "cache"
    pub auto_embed: bool,             // Default: false
    pub embed_model: String,          // Default: "miniLM"
    pub provider: String,             // Default: "local"
    pub embed_batch_size: Option<usize>,  // Default: 512
    pub bm25_k1: Option<f32>,         // Default: 0.9
    pub bm25_b: Option<f32>,          // Default: 0.4
    pub storage: StorageConfig,       // Resource limits
    pub allow_lossy_recovery: bool,
    // API keys, model endpoints...
}
```

Loaded from `strata.toml`. Supports live reconfiguration via `RwLock`.

### 6.7 GC and Maintenance

GC is **explicit**, not automatic:

- `run_gc()` — Compact storage, remove tombstones
- `run_maintenance()` — Periodic housekeeping

A `BackgroundScheduler` exists for deferred work (embedding flushes, future
automatic GC), but GC itself must be triggered by the application.

---

## 7. Executor Layer

> Source: `crates/executor/src/`

The executor layer provides the command-output interface for the database. It
translates serializable `Command` enums into primitive operations and returns
`Output` enums.

### 7.1 Command/Output Dispatch

**114 command variants** organized by category:

| Category | Count | Examples |
|----------|-------|---------|
| KV | 6 | KvPut, KvGet, KvDelete, KvList, KvBatchPut, KvGetv |
| JSON | 8+ | JsonSet, JsonGet, JsonDelete, JsonList, etc. |
| Event | 4 | EventAppend, EventGet, EventGetByType, EventLen |
| State | 7 | StateSet, StateGet, StateCas, StateInit, etc. |
| Vector | 7+ | VectorUpsert, VectorSearch, VectorCreateCollection, etc. |
| Branch | 6 | BranchCreate, BranchGet, BranchDelete, BranchMerge, etc. |
| Graph | 11+ | GraphCreate, GraphAddNode, GraphAddEdge, etc. |
| Transaction | 4 | TxnBegin, TxnCommit, TxnRollback, TxnInfo |
| Database | 4 | Ping, Info, Flush, Compact |
| Config | 6+ | ConfigGet, ConfigureSet, ConfigSetAutoEmbed, etc. |
| Search | 1 | Search (hybrid BM25 + vector) |
| Embed | 4+ | Embed, EmbedBatch, EmbedStatus, ConfigureModel |
| Generation | 4+ | Generate, Tokenize, Detokenize, GenerateUnload |

Each `Command` maps to exactly one `Output` variant:

| Command | Output |
|---------|--------|
| `KvPut` | `Version` |
| `KvGet` | `Maybe(Option<Value>)` |
| `KvDelete` | `Bool` |
| `VectorSearch` | `VectorMatches` |
| `Ping` | `Pong` |

### 7.2 Bridge Pattern

```rust
// crates/executor/src/bridge.rs:34
pub struct Primitives {
    pub db: Arc<Database>,
    pub kv: PrimitiveKVStore,
    pub json: PrimitiveJsonStore,
    pub event: PrimitiveEventLog,
    pub state: PrimitiveStateCell,
    pub branch: PrimitiveBranchIndex,
    pub vector: PrimitiveVectorStore,
    pub space: PrimitiveSpaceIndex,
    pub graph: GraphStore,
    pub limits: Limits,
}
```

**Direct primitive access** — no middleware API layer. All primitives share the
same `Arc<Database>`.

### 7.3 Type Conversions

**BranchId conversion:**

```rust
pub fn to_core_branch_id(branch: &BranchId) -> Result<strata_core::BranchId> {
    // "default" → UUID nil (all zeros)
    // Valid UUID string → parse and use
    // Any other string → UUID v5(BRANCH_NAMESPACE, string)
}
```

Human-readable branch names ("main", "experiment-1") are deterministically
mapped to UUIDs via UUID v5.

**Value ↔ JsonValue:** Handles conversion between internal `Value` and JSON.
Rejects infinite/NaN floats.

**Validation helpers:** `validate_key()`, `validate_value()`, `validate_vector()`,
`validate_not_internal_collection()`.

### 7.4 Access Control

```rust
// crates/executor/src/executor.rs:49
pub struct Executor {
    primitives: Arc<Primitives>,
    access_mode: AccessMode,  // ReadOnly or ReadWrite
}

pub fn execute(&self, mut cmd: Command) -> Result<Output> {
    if self.access_mode == AccessMode::ReadOnly && cmd.is_write() {
        return Err(Error::AccessDenied { ... });
    }
    // ... dispatch
}
```

**Early check** before any dispatch prevents mutations in read-only mode.
`cmd.is_write()` matches all mutating operations across all primitives.

### 7.5 Feature Gating

```
root Cargo.toml (features = ["embed"])
  → executor Cargo.toml (features = ["embed"])
    → intelligence Cargo.toml (provides embed implementation)
```

Feature variants: `embed`, `embed-cuda`, `embed-metal`, `anthropic`, `openai`, `google`.

When compiled **without** the `embed` feature:
- `maybe_embed_text()` is a no-op
- `flush_embed_buffer()` is a no-op
- Zero runtime overhead

### 7.6 Embed Hook

```rust
// crates/executor/src/handlers/embed_hook.rs
pub struct EmbedBuffer {
    pending: Mutex<Vec<PendingEmbed>>,
    flush_lock: Mutex<()>,          // Serializes flushes (GPU safety)
    total_queued: AtomicU64,
    total_embedded: AtomicU64,
    total_failed: AtomicU64,
}
```

**Write-behind buffering flow:**

1. Write handler extracts text from value
2. Text buffered in `EmbedBuffer` extension
3. When buffer reaches `batch_size`: submitted to background scheduler
4. `flush_embed_buffer()` computes embeddings via intelligence layer
5. Vectors inserted into shadow collections

**Shadow collections:**

| Collection | Source Primitive |
|------------|----------------|
| `_system_embed_kv` | KV |
| `_system_embed_json` | JSON |
| `_system_embed_event` | Event |
| `_system_embed_state` | State |

**Properties:**
- **Best-effort:** Failures logged, never propagated to caller.
- **NRT (Near Real-Time):** Automatic refresh every 1 second.
- **Eventually consistent:** Deletes may race with in-flight flushes.

### 7.7 Search

```rust
// crates/executor/src/handlers/search.rs
fn build_hybrid_search(db: &Arc<Database>) -> HybridSearch {
    #[cfg(feature = "embed")]
    {
        let embedder = Arc::new(IntelligenceEmbedder { db: db.clone() });
        HybridSearch::with_embedder(db.clone(), embedder)
    }
    #[cfg(not(feature = "embed"))]
    {
        HybridSearch::new(db.clone())  // BM25 only
    }
}
```

Combines BM25 keyword search with vector semantic search. When the top BM25
score >= 0.85 with a >= 0.15 gap to #2, the result is considered a strong signal
and query expansion is skipped.

### 7.8 Handler Organization

All handlers live in `crates/executor/src/handlers/`:

| Module | Commands |
|--------|----------|
| `kv.rs` | KV CRUD + batch |
| `json.rs` | JSON CRUD + batch |
| `event.rs` | Event append + query |
| `state.rs` | State CRUD + CAS + batch |
| `vector.rs` | Vector CRUD + search |
| `branch.rs` | Branch CRUD + fork/merge |
| `graph.rs` | Graph CRUD |
| `embed.rs` | Embed operations |
| `embed_hook.rs` | Write-behind buffering |
| `search.rs` | Hybrid search |
| `config.rs` | Config management |
| `space.rs` | Space auto-registration |
| `generate.rs` | LLM generation |

**Handler signature pattern:**

```rust
pub fn kv_put(
    p: &Arc<Primitives>,
    branch: strata_core::BranchId,
    space: String,
    key: String,
    value: Value,
) -> Result<Output>
```

All handlers: receive `Arc<Primitives>`, validate inputs, call primitive methods,
convert errors, return `Output`.

---

## 8. Cross-Cutting Design Decisions

### Why MVCC instead of in-place updates

Every write creates a new version, preserving history. Benefits:
- Readers never block writers (snapshot isolation without locks).
- Built-in audit trail and time-travel queries.
- GC can reclaim old versions independently of active operations.

### Why OCC instead of pessimistic locking

Optimistic concurrency control assumes conflicts are rare. Benefits:
- No lock acquisition on reads — reads are free.
- Higher throughput under low contention.
- Simpler deadlock-free design.

Cost: Transactions that conflict must be retried. Under high contention on
the same keys, pessimistic locking would be more efficient.

### Why DashMap instead of RwLock<HashMap>

`DashMap` provides 16-way internal sharding, enabling concurrent access to
different branches without contention. `RwLock<HashMap>` would serialize
all branch operations behind a single lock.

### Why per-branch commit locks (not global lock)

Transactions on different branches have no conflicts. A global lock would
serialize all commits, while per-branch locks allow full parallelism across
branches.

### Why blind writes skip the lock

Blind writes (empty read set) are commutative — they don't depend on the
current state of any key. The only ordering guarantee needed is version
monotonicity, which `fetch_add` provides. Skipping the lock avoids contention
for write-heavy workloads like ingestion.

### Why version gaps are acceptable

If a transaction allocates a version but fails to commit (e.g., WAL write
error), that version number is skipped. Consumers use version ordering
(before/after), not version arithmetic (v+1 == next). Gaps don't affect
correctness.

### Why GC is explicit (not automatic)

Automatic GC requires background threads, coordination with active snapshots,
and introduces unpredictable latency. Explicit GC gives the application full
control over when cleanup happens. A `BackgroundScheduler` exists for future
automatic GC integration.

### Why type-erased extensions

`DashMap<TypeId, Arc<dyn Any>>` lets primitives store shared state (vector
backends, embed buffers) without the `Database` struct knowing about them.
This keeps the engine layer decoupled from specific primitive implementations.

Trade-off: No compile-time guarantee that an extension exists. Runtime
panics on missing extensions are caught during testing.

### Why IdentityCodec as default

The identity codec is a zero-cost seam for encryption. When not encrypting,
`encode_cow()` returns `Cow::Borrowed(data)` — zero copies. When encryption
is needed, swap to AES-GCM codec with no API changes.

### Why WAL pre-serialization

Records are serialized and CRC-computed outside the WAL writer lock. Only
the final `append()` (memcpy into the segment buffer) runs under the lock.
This reduces lock hold time from milliseconds to microseconds, enabling
higher write throughput.

### Why single-heap vector design

Before: each sealed HNSW segment owned a copy of its embeddings (~50% memory
duplication). After: a single `VectorHeap` owns all embeddings; sealed segments
store graph structure only. Per-neighbor overhead dropped from ~48 bytes
(`BTreeSet`) to 8 bytes (flat `u64` array).

---

## 9. Known Limitations & Future Work

### TTL and GC Not Coordinated

`expire_ttl_keys()` and `gc_branch()` are independent operations. Expired keys
accumulate versions until GC runs separately. This is not a correctness issue
but can waste memory if TTL expiration is not followed by GC.

### No Codec Migration Path

The MANIFEST stores the codec ID, but there is no way to upgrade from one codec
to another (e.g., identity → AES-GCM) without exporting the entire database,
deleting it, and reimporting with the new codec.

### Snapshot-Based Recovery Not Yet Implemented

The recovery infrastructure supports snapshot-based recovery (load snapshot,
replay WAL after watermark), but snapshot creation is not yet integrated. All
recovery currently replays the full WAL.

### Transactions Not Exposed in Executor

`TxnBegin`, `TxnCommit`, `TxnRollback` commands exist in the `Command` enum
but are not yet implemented. They require session state management (tracking
which transaction belongs to which client session).

### Retention Policies Designed But Not Enforced

Retention policies are defined in the durability layer but not integrated with
any enforcement mechanism. Compaction can apply retention, but no automatic
process triggers it.

### GC Not Automatic

`run_gc()` and `run_maintenance()` must be called explicitly. The
`BackgroundScheduler` exists but is not yet wired to trigger automatic GC.

### VersionChain Out-of-Order Insertion

Under concurrent blind writes, version chain ordering may be temporarily
violated (newer version pushed before older version arrives). This is
documented in issue #1383 and does not affect correctness for typical
read patterns (which read the latest version).

### Commit Method Duplication

The concurrency layer has three commit methods (`commit`, `commit_with_wal_arc`,
`commit_with_version`) with duplicated post-validation logic. Extracting the
shared WAL+apply logic into a private helper would reduce maintenance burden.

---

*Generated from Strata v0.6.0 codebase. Last updated: 2026-03-06.*
