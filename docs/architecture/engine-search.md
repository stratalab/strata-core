# Engine Search Subsystem Architecture

**Location:** `crates/engine/src/search/`
**Source files:** 9 modules (~6,582 lines total)
**Role:** Full-text keyword search with BM25 scoring, inverted index, segmented persistence, and mmap-accelerated recovery

## Overview

The search subsystem provides optional full-text search across all engine primitives (KV, State, Event). It implements a Lucene-inspired segmented inverted index with BM25-lite scoring, Porter stemming, and sealed mmap-backed segments for persistence.

Key design principles:
- **Optional**: search works without the index (via full scan); when disabled, all operations are NOOP with zero overhead
- **Segmented**: active segment absorbs writes via DashMap; sealed segments are immutable, mmap-backed `.sidx` files
- **No data movement**: the index stores compact u32 doc IDs (12 bytes per posting entry), not content; a single `DocIdMap` holds one copy of each `EntityRef`

## Module Map

```
search/
├── mod.rs              Module organization, public re-exports (32 lines)
├── index.rs            InvertedIndex, PostingList, DocIdMap, score_top_k (2,841 lines)
├── searchable.rs       Scorer trait, BM25LiteScorer, SimpleScorer, response builders (776 lines)
├── stemmer.rs          Porter stemmer (636 lines)
├── types.rs            SearchRequest, SearchResponse, SearchHit, SearchStats (608 lines)
├── segment.rs          SealedSegment (.sidx format), varint codec, PostingIter (942 lines)
├── tokenizer.rs        UAX#29 tokenization pipeline (297 lines)
├── manifest.rs         Search manifest persistence (MessagePack) (248 lines)
└── recovery.rs         Recovery participant (fast/slow path) (202 lines)
```

---

## 1. Inverted Index (`index.rs`)

The central data structure. Stores term-to-document mappings using a segmented architecture.

### 1.1 Core Types

| Type | Size | Purpose |
|------|------|---------|
| `PostingEntry` | 12 bytes, `Copy` | `{doc_id: u32, tf: u32, doc_len: u32}` |
| `PostingList` | `Vec<PostingEntry>` | All entries for a single term |
| `ScoredDocId` | 8 bytes, `Copy` | `{doc_id: u32, score: f32}` -- result of in-index scoring |
| `DocIdMap` | bidirectional | `EntityRef <-> u32` mapping (one copy per doc, not per term) |

### 1.2 DocIdMap

Bidirectional mapping between `EntityRef` (~87 bytes with heap) and compact `u32` doc IDs.

```
DocIdMap
├── id_to_ref: RwLock<Vec<EntityRef>>     append-only, indexed by doc_id
└── ref_to_id: DashMap<EntityRef, u32>    O(1) reverse lookup
```

Memory savings: at 5.4M docs with ~60 terms/doc, `DocIdMap` uses ~918 MB vs ~28 GB if each posting entry cloned the full `EntityRef`.

`get_or_insert()` uses a double-checked locking pattern: fast-path DashMap read, slow-path write-lock + re-check.

### 1.3 InvertedIndex Fields

```
InvertedIndex
├── Active segment (mutable, DashMap-based)
│   ├── postings: DashMap<String, PostingList>    term -> entries
│   ├── doc_freqs: DashMap<String, usize>         term -> df
│   ├── doc_lengths: RwLock<Vec<Option<u32>>>      doc_id -> token count
│   └── doc_terms: RwLock<Vec<Option<Vec<String>>>>  forward index for O(terms) removal
│
├── Sealed segments (immutable, mmap-backed)
│   └── sealed: RwLock<Vec<SealedSegment>>
│
├── Shared state (spans all segments)
│   ├── doc_id_map: DocIdMap                global EntityRef <-> u32
│   ├── total_docs: AtomicUsize             across ALL segments
│   ├── total_doc_len: AtomicUsize          sum of all doc lengths
│   ├── enabled: AtomicBool                 zero-overhead disable
│   ├── version: AtomicU64                  consistency watermark
│   └── branch_ids: RwLock<HashSet<BranchId>>  single-branch optimization
│
└── Persistence
    ├── next_segment_id: AtomicU64
    ├── seal_threshold: usize               default 100,000 docs
    ├── data_dir: RwLock<Option<PathBuf>>
    ├── active_doc_count: AtomicUsize
    └── sealing: AtomicBool                 CAS guard against concurrent seals
```

### 1.4 Index Document

```
index_document(doc_ref, text) ->
  1. NOOP if disabled
  2. Get or assign doc_id via DocIdMap
  3. Track branch_id (skip write lock if already present)
  4. Check if already indexed -> remove_document() first (fixes #609)
  5. Tokenize text (UAX#29 + stem + stopwords)
  6. Count term frequencies
  7. Update posting lists (DashMap entry per term)
  8. Store forward index (doc_id -> terms) for O(terms) removal
  9. Store doc_length for accurate stats on removal
 10. Increment total_docs, total_doc_len, active_doc_count, version
 11. If active_doc_count >= seal_threshold -> try_seal_active()
```

### 1.5 Remove Document

Handles both active and sealed segments:

```
remove_document(doc_ref) ->
  1. Resolve EntityRef -> doc_id
  2. Read and clear doc_length and forward index
  3. Active segment: iterate forward index terms, remove from DashMap -- O(terms_in_doc)
  4. Sealed segments: add tombstone to ALL segments (harmless if doc not present)
  5. Update global stats (total_docs, total_doc_len)
```

### 1.6 Seal Active Segment

Triggered when `active_doc_count >= seal_threshold` (default 100K). Uses `AtomicBool` CAS to prevent concurrent seals.

```
seal_active() ->
  1. Drain DashMap -> BTreeMap<String, Vec<PostingEntry>> (sorted by term)
  2. Clear doc_freqs (sealed segments have their own df)
  3. Compute actual doc_count and total_doc_len from drained postings
  4. Assign segment_id (fetch_add)
  5. Build SealedSegment via segment::build_sealed_segment()
  6. Flush to disk as seg_{id}.sidx (if data_dir set)
  7. Append to sealed Vec
  8. Reset active_doc_count to 0
```

**Important**: `doc_lengths` is NOT cleared on seal -- it is a global map that persists across seals for re-index detection and accurate `total_doc_len` adjustment on removal.

### 1.7 In-Index BM25 Scoring (`score_top_k`)

Scores documents entirely within the index, merging results across active and sealed segments.

```
score_top_k(query_terms, branch_id, k, k1, b) ->
  1. Bail if disabled, empty query, or k=0
  2. Acquire sealed RwLock ONCE
  3. Combined IDF + posting location caching:
     For each (term, segment): single binary search -> cache (df, offset, len)
     Compute IDF from total df across all segments
  4. Single-branch optimization: if only 1 branch, skip per-doc branch check
  5. Dense Vec<f32> accumulator indexed by doc_id (not HashMap)
  6. Score active segment postings
  7. Score sealed segment postings (using cached offsets, skip tombstones)
  8. Partial sort: select_nth_unstable_by for O(n) top-k extraction
  9. Sort only the k elements
```

**Optimizations:**
- Cached `find_term`: one binary search per (segment, term), reused for both IDF and posting iteration
- Tombstone `AtomicBool`: skips `RwLock` read on segments with no tombstones
- Single-branch detection: skips per-doc `branch_id` check when only one branch exists
- Dense `Vec<f32>` accumulator: O(1) score lookup by doc_id vs HashMap overhead
- Precomputed BM25 constants: `k1*(1-b)`, `k1*b/avgdl`, `k1+1` computed once

---

## 2. Scoring Infrastructure (`searchable.rs`)

### 2.1 Scorer Trait

```rust
trait Scorer: Send + Sync {
    fn score(&self, doc: &SearchDoc, query: &str, ctx: &ScorerContext) -> f32;
    fn name(&self) -> &str;
}
```

`ScorerContext` carries corpus-level statistics:

| Field | Purpose |
|-------|---------|
| `total_docs` | N for IDF calculation |
| `doc_freqs` | Per-term document frequency |
| `avg_doc_len` | Average doc length for BM25 normalization |
| `now_micros` | Current time for recency boost |

IDF formula: `ln((N - df + 0.5) / (df + 0.5) + 1)`

### 2.2 BM25LiteScorer

BM25-inspired scorer with Anserini/Pyserini defaults (k1=0.9, b=0.4), which outperform Lucene's raw defaults (1.2/0.75) on BEIR benchmarks.

**BM25 formula** (per query term t):

```
score += IDF(t) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
```

Additional signals:
- **Recency boost** (default factor 0.1): `score *= 1 + boost * 1/(1 + age_hours/24)`
- **Title match boost**: 20% boost if any query term appears in the document title

Two scoring paths:
- `score()`: tokenizes document text at query time (slow path)
- `score_precomputed()`: uses pre-computed term frequencies from the inverted index, skipping re-tokenization (fast path)

### 2.3 SimpleScorer

Legacy keyword matcher scoring based on token overlap with length normalization. Returns scores in [0.0, 1.0]. Used as fallback when the inverted index is disabled or absent.

### 2.4 Response Building

```
build_search_response_with_scorer(candidates, query, k, ...) ->
  If index enabled and non-empty:
    1. Build ScorerContext from index stats (total_docs, avg_doc_len, per-term df)
    2. Tokenize query once
    3. Score each candidate:
       - Fast path: precomputed_tf present -> score_precomputed()
       - Slow path: create SearchDoc -> score()
    4. Filter score > 0, sort descending, take top-k
    5. Generate snippets via truncate_text(100 bytes)
  Else:
    Fall back to SimpleScorer::score_and_rank()
```

---

## 3. Tokenization Pipeline (`tokenizer.rs`)

Seven-stage pipeline matching Lucene's StandardTokenizer + analysis chain:

```
Input text
  |
  v
1. UAX#29 word boundaries (unicode_words)     "John's state-of-the-art"
  |                                            -> ["John's", "state", "of", "the", "art"]
  v
2. Strip English possessives ('s / \u2019s)    -> ["John", "state", "of", "the", "art"]
  |
  v
3. Remove non-alphanumeric chars               -> ["John", "state", "of", "the", "art"]
  |
  v
4. Lowercase (ASCII fast path)                 -> ["john", "state", "of", "the", "art"]
  |
  v
5. Filter tokens < 2 chars                     (no change)
  |
  v
6. Remove stopwords (33-word Lucene set)       -> ["john", "state", "art"]
  |
  v
7. Porter stem                                 -> ["john", "state", "art"]
```

Stopwords: 33 entries from Lucene's default set. Linear scan (all fit in a cache line).

`tokenize_unique()`: deduplicates after stemming via `HashSet`. Used for query processing where "testing tests TESTS" should produce a single `["test"]`.

Unicode handling: non-ASCII characters are preserved and lowercased via `char::to_lowercase()`. The ASCII fast path (`to_ascii_lowercase()`) avoids the iterator overhead for the common case.

---

## 4. Porter Stemmer (`stemmer.rs`)

Zero-dependency implementation of the Porter stemming algorithm (Porter 1980). Operates on ASCII lowercase input only; non-ASCII words are returned unchanged.

### 4.1 Algorithm Steps

```
stem(word) ->
  Skip if len <= 2 or non-ASCII
  Step 1a: Plurals         (sses->ss, ies->i, ss->ss, s->_)
  Step 1b: Past/gerund     (eed->ee if m>0, ed/ing->_ if vowel)
    Fixup: at/bl/iz->+e, double consonant->single, m=1+CVC->+e
  Step 1c: Y->I            (y->i if stem has vowel)
  Step 2:  Double suffixes  (ational->ate, tional->tion, ...)
  Step 3:  Derivational     (icate->ic, ative->_, alize->al, ...)
  Step 4:  Remove suffixes  (al, ance, ence, er, ... if m>1)
  Step 5a: Final -e         (remove if m>1, or m=1 and not CVC)
  Step 5b: -ll -> -l        (if m>1)
```

### 4.2 Helper Functions

| Function | Purpose |
|----------|---------|
| `is_consonant(b, i)` | Byte-level consonant test (y is consonant iff preceded by vowel) |
| `measure(word)` | Count VC sequences (the "m" value) |
| `contains_vowel(word)` | Any vowel in the stem? |
| `ends_double_consonant(word)` | Last two bytes identical and consonant? |
| `ends_cvc(word)` | Consonant-vowel-consonant ending (not w/x/y)? |

Validated against Porter's canonical 23,531-word test vocabulary from `tartarus.org`.

---

## 5. Sealed Segments (`segment.rs`)

Immutable, mmap-able segments storing term dictionaries and delta-encoded posting lists.

### 5.1 .sidx File Format

```
SIDX Segment File (seg_{id}.sidx):
+--------------------------------------------------+
| HEADER (48 bytes)                                |
|   magic: "SIDX"              4B                  |
|   version: u32 LE             4B                  |
|   segment_id: u64 LE          8B                  |
|   doc_count: u32 LE           4B                  |
|   term_count: u32 LE          4B                  |
|   total_doc_len: u64 LE       8B                  |
|   term_offsets_offset: u64 LE  8B   -> offset tbl |
|   postings_offset: u64 LE     8B   -> postings    |
+--------------------------------------------------+
| TERM DICTIONARY (sorted by term)                 |
|   per term:                                       |
|     term_len: u16 LE                              |
|     term_bytes: [u8; term_len]                    |
|     df: u32 LE                                    |
|     posting_offset: u32 LE  (rel. to postings)    |
|     posting_byte_len: u32 LE                      |
+--------------------------------------------------+
| TERM OFFSET TABLE (term_count x 4 bytes)         |
|   per term: dict_offset u32 LE                    |
|   (enables O(log n) binary search)               |
+--------------------------------------------------+
| POSTINGS SECTION                                 |
|   per term:                                       |
|     num_entries: u32 LE                           |
|     delta-encoded triples:                        |
|       delta_doc_id: varint (LEB128)               |
|       tf: varint                                  |
|       doc_len: varint                             |
+--------------------------------------------------+
```

### 5.2 Term Dictionary Lookup

Binary search on the term offset table:
1. Jump to `offset_table[mid]` to get dictionary position
2. Read term bytes at that position
3. Compare with target term
4. Return `(df, posting_offset, posting_byte_len)` on match

`find_term_info()` exposes this for callers that want to cache the result and later use `posting_iter_from_offset()` to avoid a redundant binary search.

### 5.3 Delta Encoding

Posting lists store entries sorted by `doc_id`. Each entry is encoded as three LEB128 varints:

```
(delta_doc_id, tf, doc_len)
```

where `delta_doc_id = current_doc_id - previous_doc_id`. This compresses well for clustered doc IDs (e.g., doc_id 100, 10000, 1000000 stores deltas 100, 9900, 990000 instead of absolute values).

Varint sizes: 0-127 = 1 byte, 128-16383 = 2 bytes, up to u32::MAX = 5 bytes.

### 5.4 PostingIter

Zero-allocation iterator that lazily decodes delta-encoded varints directly from segment data:

```rust
struct PostingIter<'a> {
    data: &'a [u8],       // raw segment bytes
    pos: usize,           // current read position
    remaining: u32,       // entries left
    prev_doc_id: u32,     // for delta decoding
}
```

Implements `Iterator<Item = PostingEntry>` with accurate `size_hint`.

### 5.5 Tombstones

Deleted documents are tracked per-segment via `RwLock<HashSet<u32>>`:
- `add_tombstone(doc_id)`: marks a doc as deleted
- `has_tombstones()`: `AtomicBool` fast path avoids `RwLock` read
- `tombstone_guard()`: acquires read lock once for batch lookups
- Physical removal happens at compaction time (not currently implemented)

### 5.6 Storage Backing

```rust
enum SegmentData {
    Owned(Vec<u8>),       // in-memory (before flush)
    Mmap(memmap2::Mmap),  // memory-mapped file
}
```

`write_to_file()` uses atomic temp + rename pattern.

---

## 6. Manifest (`manifest.rs`)

Persists the entire search index state for fast recovery.

### 6.1 Manifest Format

```
search.manifest:
  magic: "SMNF" (4B)
  version: u32 LE (4B)
  payload: MessagePack-encoded ManifestData
```

Written atomically via temp + fsync + rename.

### 6.2 ManifestData

| Field | Type | Purpose |
|-------|------|---------|
| `total_docs` | `u64` | Total docs across all segments |
| `total_doc_len` | `u64` | Sum of all doc lengths |
| `next_segment_id` | `u64` | Next ID to assign |
| `segments` | `Vec<SegmentManifestEntry>` | Per-segment metadata + tombstones |
| `doc_id_map` | `Vec<EntityRef>` | Global DocIdMap serialization |
| `doc_lengths` | `Vec<Option<u32>>` | Global doc lengths (persists across seals) |
| `doc_terms` | `Vec<Option<Vec<String>>>` | Forward index for O(terms) removal |

Each `SegmentManifestEntry` contains: `segment_id`, `doc_count`, `total_doc_len`, `tombstones: HashSet<u32>`.

---

## 7. Recovery (`recovery.rs`)

Integrates with the engine's `RecoveryParticipant` system.

### 7.1 Recovery Flow

```
Database::open()
  |
  v
recover_search_state(db)
  |
  +-- Skip if cache database
  |
  +-- Get InvertedIndex from db.extension::<InvertedIndex>()
  |
  +-- Set data_dir for persistence
  |
  +-- FAST PATH: load_from_disk()
  |     |
  |     +-- Load search.manifest (SMNF magic + MessagePack)
  |     +-- Restore DocIdMap from serialized Vec
  |     +-- Restore global stats (total_docs, total_doc_len)
  |     +-- mmap each seg_{id}.sidx as SealedSegment
  |     +-- Restore tombstone sets
  |     +-- Restore doc_lengths and doc_terms
  |     +-- Rebuild branch_ids from DocIdMap
  |     +-- Enable index, return
  |
  +-- SLOW PATH: rebuild from storage
        |
        +-- Scan all branches:
        |     +-- KV entries -> extract_indexable_text() -> index_document()
        |     +-- State entries -> extract_indexable_text() -> index_document()
        |     +-- Event entries (skip __meta__, __tidx__) -> index_document()
        |
        +-- freeze_to_disk() for next startup
        +-- Enable index
```

### 7.2 Text Extraction

`extract_indexable_text(value)` determines which `Value` variants are searchable:

| Value Type | Indexed? | Extraction |
|------------|----------|------------|
| `String` | Yes | Direct clone |
| `Null` | No | -- |
| `Bool` | No | -- |
| `Bytes` | No | -- |
| Other (numbers, arrays, maps) | Yes | `serde_json::to_string()` |

### 7.3 Fallback Behavior

All mmap files are caches. If missing, corrupt, or version-mismatched, recovery falls back transparently to full KV-based rebuild with no data loss.

---

## 8. Search Types (`types.rs`)

### 8.1 SearchRequest

Universal request type for all search operations (primitive-level and composite).

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `branch_id` | `BranchId` | required | Branch isolation |
| `query` | `String` | required | Query text |
| `k` | `usize` | 10 | Top-k results |
| `budget` | `SearchBudget` | 100ms/10K | Time and candidate limits |
| `mode` | `SearchMode` | Keyword | Keyword/Vector/Hybrid |
| `primitive_filter` | `Option<Vec<PrimitiveType>>` | None (all) | Limit to specific primitives |
| `time_range` | `Option<(u64, u64)>` | None | Timestamp filter (microseconds) |
| `tags_any` | `Vec<String>` | empty | Tag filter (match any) |
| `precomputed_embedding` | `Option<Vec<f32>>` | None | Skip embedder in hybrid search |

### 8.2 SearchBudget

| Field | Default | Purpose |
|-------|---------|---------|
| `max_wall_time_micros` | 100,000 (100ms) | Hard stop on wall time |
| `max_candidates` | 10,000 | Total candidates to consider |
| `max_candidates_per_primitive` | 2,000 | Per-primitive limit |

### 8.3 SearchResponse

| Field | Type | Purpose |
|-------|------|---------|
| `hits` | `Vec<SearchHit>` | Ranked results (highest score first) |
| `truncated` | `bool` | Budget caused early termination |
| `stats` | `SearchStats` | Execution metadata |

`SearchHit` contains: `doc_ref: EntityRef`, `score: f32`, `rank: u32` (1-indexed), `snippet: Option<String>`.

`SearchStats` tracks: `elapsed_micros`, `candidates_considered`, `candidates_by_primitive`, `index_used`.

**Invariant**: All search methods return `SearchResponse`. No primitive-specific result types.

---

## Data Flow: Search Request Lifecycle

```
SearchRequest { query: "bacterial infections", k: 10, ... }
  |
  v
Primitive::search(req)   (e.g., KVStore::search)
  |
  v
Is InvertedIndex enabled?
  |
  +-- YES: In-Index Fast Path
  |     |
  |     v
  |   tokenize_unique(query) -> ["bacteri", "infect"]
  |     |
  |     v
  |   index.score_top_k(terms, branch_id, k, k1=0.9, b=0.4)
  |     |
  |     +-- Compute IDF per term across active + sealed segments
  |     +-- Dense Vec<f32> accumulator
  |     +-- Score active segment postings (DashMap)
  |     +-- Score sealed segment postings (mmap, PostingIter)
  |     +-- Partial sort -> top-k ScoredDocId
  |     |
  |     v
  |   Resolve doc_ids -> EntityRef -> fetch text from storage
  |     |
  |     v
  |   Build SearchCandidate with precomputed_tf
  |     |
  |     v
  |   build_search_response_with_index(candidates, ...)
  |     +-- BM25LiteScorer::score_precomputed()  (skip re-tokenization)
  |     +-- Sort, take top-k, generate snippets
  |
  +-- NO: Full Scan Path
        |
        v
      Iterate all entries in primitive (snapshot)
        |
        v
      Build SearchCandidate per entry
        |
        v
      build_search_response(candidates, ...)
        +-- SimpleScorer::score_and_rank()
        +-- Sort, take top-k, generate snippets
  |
  v
SearchResponse { hits, truncated, stats }
```

---

## Data Flow: Index Persistence

```
                   WRITES
                     |
                     v
           +-------------------+
           | Active Segment    |      DashMap-based, mutable
           | (DashMap postings)|
           +--------+----------+
                    |
           active_doc_count >= 100K
                    |
                    v
           +-------------------+
           | seal_active()     |      Drain DashMap -> BTreeMap
           | build_sealed_     |      Delta-encode postings
           | segment()         |      Write seg_{id}.sidx
           +--------+----------+
                    |
                    v
    +-------+-------+--------+-------+
    |       |                |       |
    v       v                v       v
+------+ +------+       +------+ +------+
|seg_0 | |seg_1 |  ...  |seg_n | |active|
|.sidx | |.sidx |       |.sidx | |      |
+------+ +------+       +------+ +------+
    Sealed (mmap)                DashMap

               freeze_to_disk()
                    |
                    v
           +-------------------+
           | search.manifest   |   SMNF magic + MessagePack
           | (DocIdMap, stats,  |   Atomic: temp + fsync + rename
           |  segment list,     |
           |  tombstones)       |
           +-------------------+

               load_from_disk()
                    |
                    v
           mmap each .sidx file
           restore DocIdMap, stats
```

---

## Concurrency Model

| Component | Synchronization | Pattern |
|-----------|----------------|---------|
| `postings` (active) | `DashMap` | Lock-free sharded concurrent map |
| `doc_freqs` (active) | `DashMap` | Lock-free sharded concurrent map |
| `doc_lengths` | `RwLock<Vec>` | Write on index/remove, read on scoring |
| `doc_terms` | `RwLock<Vec>` | Write on index/remove |
| `sealed` | `RwLock<Vec<SealedSegment>>` | Read during search, write on seal |
| `doc_id_map.id_to_ref` | `RwLock<Vec>` | Append-only, double-checked insert |
| `doc_id_map.ref_to_id` | `DashMap` | Lock-free reverse lookup |
| `total_docs`, `total_doc_len` | `AtomicUsize` | Lock-free counters |
| `enabled`, `version` | `AtomicBool`/`AtomicU64` | Lock-free flags |
| `sealing` | `AtomicBool` CAS | Prevents concurrent auto-seals |
| Per-segment tombstones | `RwLock<HashSet<u32>>` | Write on remove, read on search |
| Per-segment `has_tombstones` | `AtomicBool` | Fast path skips RwLock |

### Lock Ordering

Score path acquires locks in this order:
1. `sealed` (RwLock read)
2. `doc_id_map.id_to_ref` (RwLock read)
3. Per-segment `tombstone_guard` (RwLock read, only if `has_tombstones`)

### Contention Profile

- **Low contention**: searches are read-only (RwLock read on sealed, DashMap reads on active)
- **Medium contention**: indexing acquires DashMap write shards and RwLock writes on doc_lengths/doc_terms
- **Rare**: sealing is serialized via AtomicBool CAS (one seal at a time)

---

## Design Decisions

### Optional Index
The index is entirely optional. When disabled, `is_enabled()` returns false and all operations bail immediately. Search falls back to full scan with `SimpleScorer`. This ensures zero overhead for deployments that don't need full-text search.

### Forward Index for O(terms) Removal
`doc_terms` stores `doc_id -> terms` mapping. Without this, removing a document would require scanning the entire vocabulary (O(vocabulary)) to find and remove posting entries. With the forward index, removal is O(terms_in_doc).

### Global doc_lengths
The `doc_lengths` map persists across seals (not cleared when active segment is sealed). This is critical for:
- Re-index detection: if a doc_id already has a length, the document was previously indexed
- Accurate `total_doc_len` adjustment when removing docs from sealed segments

### Single-Branch Optimization
When only one branch exists in the index (common case), `score_top_k` skips the per-document branch_id check entirely. This avoids one `Vec` lookup per posting entry.

### Dense Vec Accumulator
`score_top_k` uses `Vec<f32>` indexed by doc_id instead of `HashMap<u32, f32>`. Since doc IDs are contiguous 0..N-1, this provides O(1) access with no hashing overhead. The tradeoff is allocating space for all doc IDs, but this is bounded by `doc_id_map.len()`.
