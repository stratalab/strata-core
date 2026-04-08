# Engine Vector Subsystem Architecture

**Subsystem:** `crates/engine/src/primitives/vector/`
**Dependencies:** `strata-core`, `strata-storage`, `strata-concurrency`, `strata-durability`
**Source files:** 17 modules (~18,000 lines of implementation and tests)

## Overview

The vector subsystem provides **embedding storage and approximate nearest-neighbor (ANN) search** for Strata. It is the largest single subsystem in the engine crate, implementing:

1. **VectorStore** — stateless facade over `Database` for collection management and search
2. **VectorHeap** — single-heap embedding storage with three backing tiers (in-memory, mmap, tiered)
3. **HNSW graph** — O(log n) approximate nearest-neighbor search with incremental insert/delete
4. **CompactHnswGraph** — sealed, read-optimized graph with flat `Vec<u64>` neighbor arrays
5. **SegmentedHnswBackend** — production backend: active buffer + sealed HNSW segments + fan-out search
6. **BruteForceBackend** — O(n) linear scan for small collections (<10K vectors)
7. **Distance metrics** — SIMD-accelerated similarity computation (NEON, AVX2, scalar)
8. **mmap acceleration** — `.vec` heap files (SVEC format) and `.hgr` graph files (SHGR format)
9. **WAL integration** — crash-safe vector operations via write-ahead log
10. **Snapshot/recovery** — deterministic serialization and mmap-accelerated restart

## Module Map

```
vector/
├── mod.rs              Public API re-exports, graph_dir() helper              (74 lines)
├── types.rs            VectorRecord, InlineMeta, CollectionRecord, VectorId   (615 lines)
├── error.rs            VectorError enum (16 variants), VectorResult           (313 lines)
├── filter.rs           MetadataFilter re-export, equality-based filtering     (160 lines)
├── backend.rs          VectorIndexBackend trait (~30 methods), factory enum   (325 lines)
├── collection.rs       Name validation rules, system collection prefix       (256 lines)
├── heap.rs             VectorHeap: embedding storage, norm cache, mmap tiers  (1,998 lines)
├── distance.rs         SIMD distance: NEON 4x, AVX2 2x, scalar fallback      (1,066 lines)
├── brute_force.rs      BruteForceBackend: O(n) search over VectorHeap        (385 lines)
├── hnsw.rs             HnswGraph, CompactHnswGraph, VisitedSet, ScoredId     (3,096 lines)
├── segmented.rs        SegmentedHnswBackend: active buffer + sealed segments  (3,175 lines)
├── mmap.rs             SVEC format: heap freeze/load with CompactIndex        (567 lines)
├── mmap_graph.rs       SHGR format: sealed graph freeze/load                 (610 lines)
├── recovery.rs         mmap-accelerated VectorSubsystem (recover + freeze)    (362 lines)
├── wal.rs              WAL entry types 0x70-0x73, MessagePack serialization   (492 lines)
├── snapshot.rs         Deterministic snapshot v0x01, raw f32 LE embeddings    (633 lines)
└── store.rs            VectorStore facade, VectorBackendState, upsert logic   (4,496 lines)
```

---

## 1. Single-Heap Architecture (`heap.rs`)

The central design decision: **one global `VectorHeap` per collection owns all embeddings**. Sealed segments contain graph-only `CompactHnswGraph` structures that reference the heap for distance computation, eliminating embedding duplication.

### 1.1 VectorHeap Fields

| Field | Type | Purpose |
|-------|------|---------|
| `data` | `VectorData` | Backing storage (InMemory, Mmap, or Tiered) |
| `id_to_offset` | `BTreeMap<VectorId, usize>` | Maps VectorId to slot offset in embedding array |
| `free_slots` | `Vec<usize>` | Reusable slots from deleted vectors |
| `next_id` | `AtomicU64` | Monotonically increasing ID counter |
| `dimension` | `usize` | Embedding dimension (e.g., 384 for MiniLM) |
| `metric` | `DistanceMetric` | Similarity metric for this heap |
| `config` | `VectorConfig` | Full collection configuration |
| `norm_cache` | `HashMap<VectorId, f32>` | Pre-computed L2 norms for cosine acceleration |
| `inline_meta` | `HashMap<VectorId, InlineMeta>` | O(1) search resolution metadata |

### 1.2 Three Storage Tiers

```
VectorData enum
├── InMemory(Vec<f32>)         Fresh collection, no mmap file yet
├── Mmap(MmapVectorData)       Loaded from .vec file, zero-copy reads
└── Tiered {                   Mmap base + in-memory overlay
        base: MmapVectorData,    Frozen embeddings (read-only)
        overlay: Vec<f32>,       New embeddings since last freeze
        overlay_id_to_offset     Overlay-local offset map
    }
```

**Lifecycle:**

```
InMemory ──freeze_to_disk──> .vec file ──from_mmap──> Mmap
                                                        │
                                            promote_to_tiered
                                                        │
                                                        v
                                                     Tiered
                                                        │
                                            flush_overlay_to_disk
                                                        │
                                                        v
                                              New .vec (merged)
                                              reopen as Mmap
                                              promote again
```

### 1.3 Slot Reuse

When a vector is deleted, its slot index is pushed onto `free_slots`. The next `insert()` pops from `free_slots` instead of extending the embedding array. This bounds memory usage for workloads with high churn.

### 1.4 Norm Cache

For cosine similarity, L2 norms are expensive to recompute on every distance call. The heap caches `||v||` for each vector on insert. `compute_similarity_cached()` uses the cached norms, saving approximately 13% per distance call.

---

## 2. HNSW Algorithm (`hnsw.rs`)

HNSW (Hierarchical Navigable Small World) builds a multi-layer proximity graph for O(log n) approximate nearest-neighbor search.

### 2.1 Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `M` | 16 | Max connections per node per layer (layer 0: 2M = 32) |
| `ef_construction` | 200 | Beam width during graph construction |
| `ef_search` | 100 | Beam width during search |
| `ml` | 1/ln(M) ≈ 0.361 | Level multiplier for random layer assignment |

### 2.2 Layer Assignment

Each new node is assigned a random max layer using:

```
level = floor(-ln(uniform(0,1)) * ml)
```

With `ml = 1/ln(16)`, roughly:
- 93.75% of nodes appear only in layer 0
- 5.86% reach layer 1
- 0.37% reach layer 2
- Higher layers are exponentially rarer

**Determinism:** A SplitMix64 RNG is seeded with `42` and a monotonic counter ensures identical level sequences across runs.

### 2.3 Insert Algorithm

```
insert(id, embedding):
  1. Assign random level L for new node
  2. If graph is empty: set as entry point, return
  3. Greedy search from entry_point down to layer L+1
     (single nearest neighbor per layer — narrows entry)
  4. For layers L down to 0:
     a. search_layer(query, entry, ef_construction) → candidates
     b. Select neighbors using diversity heuristic (Algorithm 4)
     c. Add bidirectional edges
     d. Prune oversized neighbor lists
  5. If L > current max layer: update entry_point
```

### 2.4 Search Algorithm

```
search(query, k):
  1. Start at entry_point on top layer
  2. Greedy search from top to layer 1 (single nearest neighbor)
  3. At layer 0: beam search with ef_search candidates
     - max-heap of candidates (best-first expansion)
     - min-heap of results (worst result on top for eviction)
     - Expand: pop best candidate, visit all unvisited neighbors
     - Stop when best candidate is worse than worst result
  4. Return top-k from results, sorted by (score desc, VectorId asc)
```

### 2.5 Neighbor Selection (Diversity Heuristic)

Uses the heuristic from the HNSW paper (Algorithm 4):

```
select_neighbors(candidates, M):
  result = []
  for each candidate c in candidates (best score first):
    if c is closer to query than to any node already in result:
      result.push(c)         // diverse neighbor
    if result.len() == M: break
  return result
```

This produces a graph with better connectivity than simple top-M selection by ensuring neighbors are spread across different regions of the embedding space.

### 2.6 Soft Deletion

Nodes are not removed from the graph. Instead, `deleted_at` is set to a timestamp. Search filters out deleted nodes from results. This preserves graph connectivity — removing bridge nodes would fragment the graph and degrade recall.

### 2.7 HnswNode Structure

| Field | Type | Purpose |
|-------|------|---------|
| `neighbors` | `Vec<Vec<VectorId>>` | Per-layer sorted neighbor lists |
| `max_layer` | `usize` | Highest layer this node appears in |
| `created_at` | `u64` | Creation timestamp (microseconds, 0 = legacy) |
| `deleted_at` | `Option<u64>` | Soft-delete timestamp |

---

## 3. CompactHnswGraph (`hnsw.rs`)

When a mutable `HnswGraph` is sealed, it is converted to a read-optimized `CompactHnswGraph` for search performance.

### 3.1 Key Optimizations

| Aspect | HnswGraph | CompactHnswGraph |
|--------|-----------|------------------|
| Node lookup | `HashMap<VectorId, HnswNode>` — O(1) amortized | `Vec<Option<CompactHnswNode>>` indexed by `id.0 - id_offset` — O(1) guaranteed |
| Neighbor storage | `Vec<Vec<VectorId>>` per node — pointer-heavy | Flat `Vec<u64>` shared array, per-node `(start, count)` ranges — cache-friendly |
| Memory per neighbor | ~48 bytes (Vec + BTreeSet overhead) | 8 bytes (single u64 in contiguous array) |
| Visited tracking | `HashSet<VectorId>` per search — O(n) allocation | Thread-local `VisitedSet` with generation counter — O(1) reset |

### 3.2 CompactHnswNode

```rust
struct CompactHnswNode {
    layer_ranges: Vec<(u32, u16)>,  // (start_offset, count) into neighbor_data
    created_at: u64,
    deleted_at: Option<u64>,
}
```

Each `(start, count)` pair indexes into the shared `neighbor_data` array. Layer 0's neighbors are at `neighbor_data[start..start+count]`, layer 1 at the next range, etc.

### 3.3 NeighborData Variants

```
NeighborData
├── Owned(Vec<u64>)              In-memory after seal
└── Mmap { mmap, byte_offset, len }  Zero-copy from .hgr file (LE only)
```

Both expose `as_slice() -> &[u64]`, making search code agnostic to the backing. The Mmap variant requires little-endian architecture (compile-time check) for zero-cost `u64` reinterpretation.

### 3.4 VisitedSet with Generation Counter

```
search():
  visited.advance_generation()    // O(1) — increment counter
  ...
  if visited.is_visited(id):      // O(1) — check generation[id] == current
    skip
  visited.mark_visited(id)        // O(1) — set generation[id] = current
```

No clearing required between searches. A thread-local pool (`RefCell<Vec<VisitedSet>>`) avoids allocation on every search call.

### 3.5 Prefetch Hints

During the HNSW inner search loop, the next neighbor's embedding pointer is prefetched while computing distance for the current neighbor:

- **aarch64:** `PRFM PLDL1KEEP` (prefetch to L1, keep in cache)
- **x86_64:** `PREFETCHT0` (prefetch into all cache levels)

This hides DRAM latency (~100ns) when embeddings exceed L2 cache.

---

## 4. Segmented Backend (`segmented.rs`)

The production backend. Balances insert throughput (O(1) into active buffer) with search quality (HNSW graph for sealed segments).

### 4.1 Architecture

```
SegmentedHnswBackend
├── heap: VectorHeap              Global embedding storage (shared)
├── active: ActiveBuffer          Recent vectors, brute-force searchable
├── segments: Vec<SealedSegment>  Frozen HNSW graphs
├── config: SegmentedHnswConfig   Thresholds for seal/flush/compact
└── hnsw_config: HnswConfig       Shared HNSW parameters for all segments
```

### 4.2 Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `seal_threshold` | 50,000 | Active buffer vectors before auto-seal |
| `heap_flush_threshold` | 500,000 | Overlay embeddings before mmap flush |
| `auto_compact_threshold` | `usize::MAX` | Segments before auto-compact (disabled) |

### 4.3 Insert Path

```
insert(id, embedding):
  1. heap.insert(id, embedding)           // store embedding globally
  2. active.ids.push(id)                  // track in active buffer
  3. active.timestamps.insert(id, (ts,None))  // record creation time
  4. if active.len() >= seal_threshold:
       seal()                             // drain active → sealed segment
```

### 4.4 Seal Operation

```
seal():
  1. Drain active buffer (sorted by VectorId for determinism)
  2. Build HnswGraph:
     - Insert vectors in VectorId order (deterministic)
     - RNG seed=42 + monotonic counter (deterministic levels)
     - Apply pending timestamps
  3. Convert HnswGraph → CompactHnswGraph (dense array + flat neighbors)
  4. Push SealedSegment { segment_id, graph, live_count }
  5. If segments.len() >= auto_compact_threshold: compact()
```

### 4.5 Search — Fan-out with K-way Merge

```
                    query
                      │
          ┌───────────┼───────────┐
          v           v           v
    ┌──────────┐ ┌─────────┐ ┌─────────┐
    │  Active  │ │Segment 0│ │Segment 1│ ...
    │  Buffer  │ │ (HNSW)  │ │ (HNSW)  │
    │(brute-   │ │ search  │ │ search  │
    │ force)   │ │         │ │         │
    └────┬─────┘ └────┬────┘ └────┬────┘
         │            │           │
         └────────────┼───────────┘
                      v
              K-way merge heap
           (score desc, VectorId asc)
                      │
                      v
                  Top-k results
```

**Active buffer search:** Linear scan with a min-heap of size k. O(n) where n = active buffer size.

**Sealed segment search:** Each segment's `CompactHnswGraph` is searched independently with `ef_search` beam width.

**Parallel search:** When `segments.len() >= 4`, searches execute on a dedicated Rayon thread pool (capped at 4 threads) to avoid contention with the global Rayon pool.

**K-way merge:** A max-heap of `KWayEntry { score, id, set_idx }` merges results from all segments. Deduplication ensures each VectorId appears at most once (highest score wins).

### 4.6 Compact

Merges all sealed segments into a single monolithic HNSW graph. Benefits:
- Better recall (single graph vs. fragmented segments)
- Faster search (no fan-out overhead, no k-way merge)
- Reduces segment count

### 4.7 ActiveBuffer

| Field | Type | Purpose |
|-------|------|---------|
| `ids` | `Vec<VectorId>` | Vector IDs in insertion order |
| `timestamps` | `BTreeMap<VectorId, (u64, Option<u64>)>` | (created_at, deleted_at) per vector |

O(1) insert, O(n) search with min-heap top-k selection.

### 4.8 SealedSegment

| Field | Type | Purpose |
|-------|------|---------|
| `segment_id` | `u64` | Monotonically increasing identifier |
| `graph` | `CompactHnswGraph` | Read-optimized HNSW graph (no embedding ownership) |
| `live_count` | `usize` | Number of non-deleted vectors (for statistics) |
| `source_branch` | `Option<BranchId>` | For future branch merge (segment adoption) |

---

## 5. Distance Metrics and SIMD (`distance.rs`)

### 5.1 Metrics

All scores normalized to "higher = more similar" (Invariant R2):

| Metric | Formula | Score Range |
|--------|---------|-------------|
| Cosine | `dot(a,b) / (||a|| * ||b||)` | [-1, 1] |
| Euclidean | `1 / (1 + L2_distance)` | (0, 1] |
| DotProduct | `dot(a, b)` | (-inf, inf) |

### 5.2 SIMD Implementations

**aarch64 (NEON):** Always available, no runtime detection.

```
4x unrolled loop, 12 float32x4_t accumulators:
  sum0..sum3  — primary dot product (or squared differences)
  sum4..sum7  — norm_a squared (for cosine)
  sum8..sum11 — norm_b squared (for cosine)

Process 16 floats per iteration, reduce to scalar at end.
Tail loop handles remaining 1-15 elements.
```

**x86_64 (AVX2 + FMA):** Runtime-detected via `is_x86_feature_detected!("avx2")`.

```
2x unrolled loop, 6 __m256 accumulators:
  sum0, sum1 — dot product
  norm_a0, norm_a1 — ||a||^2
  norm_b0, norm_b1 — ||b||^2

Process 16 floats per iteration (2 x 8-wide), reduce with horizontal adds.
Fallback to scalar if AVX2 not available.
```

**Scalar fallback:** Simple loop, used on architectures without SIMD support.

### 5.3 Prefetch

```rust
pub(crate) fn prefetch_read(ptr: *const u8) {
    // aarch64: PRFM PLDL1KEEP
    // x86_64:  _mm_prefetch(..., _MM_HINT_T0)
    // other:   no-op
}
```

Used in the HNSW search loop to prefetch the next neighbor's embedding before computing distance for the current neighbor. Hides ~100ns DRAM latency.

---

## 6. mmap Acceleration

### 6.1 Heap mmap — SVEC Format (`mmap.rs`)

```
┌─────────────────────────────────────────────┐
│  Header                                      │
│    magic: "SVEC" (4 bytes)                   │
│    version: u32                              │
│    dimension: u32                            │
│    count: u64                                │
│    next_id: u64                              │
│─────────────────────────────────────────────│
│  ID-to-offset map                            │
│    count entries x (vector_id: u64,          │
│                     offset: u64)             │
│─────────────────────────────────────────────│
│  Free slots                                  │
│    free_count: u64                           │
│    free_count x slot_index: u64              │
│─────────────────────────────────────────────│
│  Embeddings                                  │
│    Contiguous f32 LE values                  │
│    Each slot = dimension x 4 bytes           │
└─────────────────────────────────────────────┘
```

**CompactIndex:** The mmap reader uses a sorted `Vec<(u64, u64)>` for O(log n) binary-search lookups (16 bytes per entry), compared to ~72 bytes per `BTreeMap` entry for in-memory heaps.

**Atomic write:** `freeze_to_disk` writes to a temp file, fsyncs, then atomically renames over the target path. A crash mid-write leaves the old file intact.

### 6.2 Graph mmap — SHGR Format (`mmap_graph.rs`)

```
┌─────────────────────────────────────────────┐
│  Header (48 bytes)                           │
│    magic: "SHGR" (4 bytes)                   │
│    version: u32                              │
│    entry_point: u64                          │
│    max_level: u32                            │
│    node_count: u64                           │
│    neighbor_data_len: u64                    │
│    node_section_size: u64                    │
│    reserved: [u8; 4]                         │
│─────────────────────────────────────────────│
│  Node entries (variable length per node)     │
│    vector_id: u64                            │
│    created_at: u64                           │
│    deleted_at: u64 (0 = alive)               │
│    num_layers: u32                           │
│    per-layer: (start: u32, count: u16)       │
│─────────────────────────────────────────────│
│  Padding to 8-byte alignment                 │
│─────────────────────────────────────────────│
│  Neighbor data                               │
│    Flat array of u64 LE values               │
│    Directly reinterpretable as &[u64]        │
│    on little-endian architectures            │
└─────────────────────────────────────────────┘
```

**NeighborData::Mmap:** On LE architectures, the neighbor data section is mapped directly from the file as `&[u64]` via pointer reinterpret — zero deserialization cost. A compile-time assertion prevents use on big-endian platforms.

**Segment manifest:** `freeze_graphs_to_disk` writes a `segments.manifest` file listing segment IDs, then writes individual `seg_{id}.hgr` files. `load_graphs_from_disk` validates the manifest against current heap state to detect staleness.

### 6.3 File Layout on Disk

```
{data_dir}/vectors/{branch_hex}/
├── {collection}.vec                    Heap mmap (SVEC format)
└── {collection}_graphs/
    ├── segments.manifest               Segment ID list
    ├── seg_0.hgr                       Sealed segment 0 (SHGR format)
    ├── seg_1.hgr                       Sealed segment 1 (SHGR format)
    └── ...
```

---

## 7. Collection Lifecycle (`collection.rs`, `store.rs`)

### 7.1 Name Validation

| Rule | Constraint |
|------|-----------|
| Length | 1-256 characters |
| Forbidden characters | `/`, null byte (`\0`) |
| Reserved prefix | `_` prefix reserved for system collections |
| System prefix | `_system_` required for system collections |
| Vector keys | 1-1024 characters |

### 7.2 Create Collection

```
create_collection(branch_id, name, config):
  1. Validate collection name
  2. Check for duplicates in VectorBackendState
  3. Persist CollectionRecord to KV (namespace-scoped key)
  4. Create IndexBackend via factory (default: SegmentedHnsw)
  5. Insert into VectorBackendState.backends
  6. Write WAL entry (type 0x70)
```

### 7.3 Vector Upsert

```
insert(branch_id, collection, key, embedding, metadata):
  1. validate_query_values(embedding)        // reject NaN/Infinity
  2. Acquire write lock on backends
  3. Check for existing vector with same key:
     a. If exists: delete old vector from backend
     b. Allocate new VectorId (monotonically increasing)
  4. Insert into backend (heap + active buffer)
  5. Persist VectorRecord to KV
  6. Set InlineMeta (key + source_ref) for O(1) search resolution
  7. Write WAL entry (type 0x72)
```

**TOCTOU prevention (#936):** The existence check and insert are performed under a single write lock to prevent concurrent upserts from creating duplicate entries.

**KV-before-backend ordering (#937):** KV persistence happens before backend insertion so that a crash after KV write but before backend insert is recovered correctly (the recovery scan will find the KV entry).

---

## 8. VectorStore Facade (`store.rs`)

### 8.1 Stateless Design

`VectorStore` holds only `Arc<Database>` — no private state. All mutable state lives in `VectorBackendState`, stored in the Database via the type-erased extension mechanism (`DashMap<TypeId, Arc<dyn Any>>`):

```rust
pub struct VectorStore {
    db: Arc<Database>,
}

pub struct VectorBackendState {
    pub backends: RwLock<BTreeMap<CollectionId, Box<dyn VectorIndexBackend>>>,
}
```

Multiple `VectorStore` instances for the same `Database` share the same backend state. This avoids the data-loss bug where each `VectorStore::new()` previously created a private, empty backends map.

### 8.2 VectorIndexBackend Trait

The trait defines ~30 methods covering:

- **Core operations:** `allocate_id`, `insert`, `delete`, `search`
- **Temporal:** `insert_with_timestamp`, `search_at`, `search_in_range`
- **Recovery:** `insert_with_id`, `rebuild_index`, `register_mmap_vector`
- **mmap:** `freeze_heap_to_disk`, `replace_heap`, `freeze_graphs_to_disk`, `load_graphs_from_disk`
- **Inline metadata:** `set_inline_meta`, `get_inline_meta`, `remove_inline_meta`
- **Snapshot:** `vector_ids`, `snapshot_state`, `restore_snapshot_state`
- **Maintenance:** `compact`, `seal_remaining_active`, `flush_heap_to_disk_if_needed`

Most methods have default no-op implementations, allowing simpler backends (BruteForce) to only implement core operations.

### 8.3 IndexBackendFactory

```rust
enum IndexBackendFactory {
    BruteForce,                              // O(n) linear scan
    Hnsw(HnswConfig),                        // O(log n) single-graph HNSW
    SegmentedHnsw(SegmentedHnswConfig),       // O(1) insert, fan-out search (default)
}
```

Default is `SegmentedHnsw` with default configuration.

### 8.4 InlineMeta — O(1) Search Resolution

```rust
struct InlineMeta {
    key: String,         // user-provided vector key
    source_ref: Option<EntityRef>,  // optional source reference
}
```

Stored alongside each VectorId in the heap, enabling direct key lookup from search results. Without this, resolving a VectorId to its key requires an O(n) KV prefix scan.

---

## 9. WAL Integration (`wal.rs`)

### 9.1 Entry Types

| Type Byte | Operation | Payload |
|-----------|-----------|---------|
| `0x70` | Collection create | collection name, VectorConfig |
| `0x71` | Collection delete | collection name |
| `0x72` | Vector upsert | collection, key, VectorId, full embedding, metadata |
| `0x73` | Vector delete | collection, key, VectorId |

All payloads are MessagePack-serialized for compact binary encoding.

### 9.2 Embedding Storage in WAL

**Current format:** Full embeddings are written to the WAL (~3KB per 768-dimension vector). This is a known bloat issue — a 768-dim embedding adds 3,072 bytes per WAL entry. The format is documented as temporary, with future plans to reference embeddings by heap offset instead.

### 9.3 WAL Replay

`VectorWalReplayer` dispatches each WAL entry to the appropriate `VectorStore` method:
- `0x70` → `replay_collection_create()`
- `0x71` → `replay_collection_delete()`
- `0x72` → `replay_vector_upsert()` (uses `insert_with_id` to preserve original VectorIds)
- `0x73` → `replay_vector_delete()`

---

## 10. Recovery Flow (`recovery.rs`)

### 10.1 Three-Phase Recovery

```
Phase 1: Scan and Load
  For each branch in storage:
    For each collection config in KV:
      1. Create backend via IndexBackendFactory
      2. Try loading heap from .vec mmap cache:
         - Success → set heap as Mmap, scan KV for timestamps only
                     (register_mmap_vector for each vector ID)
         - Failure → full KV scan, insert embeddings into heap

Phase 2: Build Graphs
  For each collection:
    1. Try load_graphs_from_disk():
       - Success → graphs loaded from .hgr mmap, skip rebuild
       - Failure → rebuild_index() (build HNSW from heap)
    2. seal_remaining_active() (ensure no vectors in brute-force buffer)

Phase 3: Freeze to Disk
  For each collection (if data_dir is set):
    1. freeze_heap_to_disk() → write .vec mmap cache
    2. Configure flush paths for subsequent heap growth
```

### 10.2 mmap Acceleration Benefits

| Recovery Path | Operation | I/O |
|--------------|-----------|-----|
| Cold start (no mmap) | Full KV scan + embedding parse + HNSW build | O(n) reads + O(n log n) graph build |
| Warm start (mmap) | mmap heap + timestamp scan + mmap graphs | O(1) mmap + O(n) timestamp scan |

The mmap path avoids deserializing embeddings and rebuilding graphs, reducing restart time from seconds to milliseconds for large collections.

### 10.3 RecoveryStats

```rust
struct RecoveryStats {
    collections_created: usize,
    collections_deleted: usize,
    vectors_upserted: usize,        // full KV path
    vectors_mmap_registered: usize,  // mmap fast path
    vectors_deleted: usize,
}
```

### 10.4 Fallback Safety

All mmap files are **caches**. If missing, corrupt, or dimension-mismatched, recovery falls back transparently to full KV-based rebuild with no data loss.

---

## 11. Snapshot (`snapshot.rs`)

### 11.1 Format (Version 0x01)

Deterministic binary serialization for branch snapshots:

```
┌──────────────────────────────────┐
│  Collection header               │
│    name_len + name (UTF-8)       │
│    VectorConfig (dim, metric,    │
│                   storage_dtype) │
│    next_id: u64                  │
│    free_slots_count + slots      │
│    vector_count: u64             │
│──────────────────────────────────│
│  Per-vector entries              │
│    vector_id: u64                │
│    dimension x f32 LE            │
│──────────────────────────────────│
│  (repeat for next collection)    │
└──────────────────────────────────┘
```

**Determinism:** Collections are sorted by name; vectors within each collection are sorted by VectorId.

**Critical invariant (T4):** `next_id` and `free_slots` are persisted in the snapshot. Without these, restored collections could allocate duplicate VectorIds, violating uniqueness.

---

## 12. Determinism Invariants

The vector subsystem enforces several determinism invariants across all backends:

| Invariant | Description | Implementation |
|-----------|-------------|----------------|
| R2 | Higher score = more similar | All metrics normalize to this convention |
| R3 | Deterministic iteration | BTreeMap for backends, sorted VectorIds in snapshots |
| R4 | Sort order: (score desc, VectorId asc) | ScoredId ordering, KWayEntry ordering, ActiveScored ordering |
| R8 | Single-threaded distance computation | No parallel SIMD; deterministic floating-point |
| R9 | No implicit vector normalization | Vectors stored and compared as-is |
| T4 | VectorId uniqueness across restarts | Snapshot persists next_id + free_slots |

**RNG determinism:** `HnswGraph` uses SplitMix64 with seed 42 and a monotonic counter. Two databases processing identical insert sequences produce identical graphs.

**Tie-breaking:** When multiple vectors have the same similarity score, the one with the lower `VectorId` (numerically) is ranked higher. This is enforced in `ScoredId::cmp`, `ActiveScored::cmp`, and `KWayEntry::cmp`.

---

## 13. Temporal Search

HNSW nodes carry `created_at` and `deleted_at` timestamps (microseconds since epoch).

### 13.1 search_at(query, k, as_of_ts)

Returns vectors that were alive at `as_of_ts`:
- `created_at <= as_of_ts`
- `deleted_at` is None or `deleted_at > as_of_ts`

### 13.2 search_in_range(query, k, start_ts, end_ts)

Returns vectors created within `[start_ts, end_ts]` that have not been deleted:
- `start_ts <= created_at <= end_ts`
- `deleted_at` is None or `deleted_at > end_ts`

Both temporal search variants traverse the full HNSW graph but filter candidates during the beam search, maintaining O(log n) complexity with a constant-factor overhead for timestamp checks.

---

## 14. Error Handling (`error.rs`)

`VectorError` has 16 variants covering:
- Dimension mismatches
- Collection not found / already exists
- Invalid embedding (NaN, Infinity)
- Invalid collection or key names
- Snapshot deserialization failures
- mmap I/O errors
- Configuration conflicts during WAL replay

All variants convert to `StrataError` with optional branch context for user-facing error messages.

---

## 15. End-to-End Data Flow

```
User: store.insert(branch, "docs", "key1", [0.1, 0.2, ...], metadata)
  │
  ├──1──> validate_query_values([0.1, 0.2, ...])
  ├──2──> VectorBackendState.backends.write_lock()
  ├──3──> Check for existing "key1" in KV → delete if exists
  ├──4──> backend.allocate_id() → VectorId(42)
  ├──5──> backend.insert(VectorId(42), [0.1, 0.2, ...])
  │         ├──> heap.insert(id, embedding)        // store in global heap
  │         └──> active_buffer.push(id)            // track for future seal
  ├──6──> Persist VectorRecord to KV
  ├──7──> Set InlineMeta { key: "key1", source_ref: None }
  └──8──> Write WAL entry (type 0x72)

                    ... active buffer fills to 50,000 ...

Auto-seal triggers:
  ├──> Drain active buffer (sorted by VectorId)
  ├──> Build HnswGraph (seed=42, deterministic levels)
  ├──> Convert to CompactHnswGraph (dense array + flat neighbors)
  └──> Push SealedSegment

User: store.search(branch, "docs", [0.3, 0.4, ...], k=10)
  │
  ├──1──> validate_query_values([0.3, 0.4, ...])
  ├──2──> VectorBackendState.backends.read_lock()
  ├──3──> backend.search([0.3, 0.4, ...], 10)
  │         ├──> Brute-force active buffer → partial results
  │         ├──> HNSW search each sealed segment → partial results
  │         │      (parallel if >= 4 segments, dedicated 4-thread pool)
  │         └──> K-way merge → top-10 by (score desc, VectorId asc)
  ├──4──> Resolve InlineMeta for each VectorId → user keys
  └──5──> Return Vec<VectorMatch> with keys, scores, metadata
```
