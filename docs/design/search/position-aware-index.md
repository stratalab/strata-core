# Position-Aware Inverted Index

**Issue**: #2238
**Status**: Design
**Dependencies**: None (foundation for #2239 phrase queries, #2240 proximity scoring)

## Problem

Strata's inverted index stores `{doc_id, tf, doc_len}` per posting — 12 bytes, no
position information. This is sufficient for bag-of-words BM25 but makes phrase
queries and proximity scoring impossible. The query `"machine learning"` matches
any document containing both words in any order at any distance.

## Current State

### PostingEntry (12 bytes)

```rust
struct PostingEntry {
    doc_id: u32,    // integer document identifier
    tf: u32,        // term frequency in document
    doc_len: u32,   // document length in tokens
}
```

### .sidx Sealed Segment Format

```
HEADER (48 bytes)
TERM DICTIONARY (sorted, variable-length)
TERM OFFSET TABLE (term_count × 4 bytes)
POSTINGS SECTION:
  per term:
    num_entries: u32
    entries: [(delta_doc_id, tf, doc_len)]  // varint-encoded triples
```

### Tokenizer

`tokenize(text) -> Vec<String>` — returns stemmed tokens with stopwords removed.
Positions are discarded.

---

## Design

### Principle: Separate Position Storage

Following Lucene and Tantivy, positions are stored **separately** from the core
posting data (doc_id, tf, doc_len). This is the single most important design
decision:

- Queries that don't need positions (bag-of-words BM25, boolean) never touch
  position data — zero overhead on the hot path.
- Position data is loaded lazily, only when a phrase or proximity query
  needs it for a candidate document that already passed doc-level intersection.
- The `.sidx` format gains a new positions section without changing the existing
  postings section layout.

### 1. In-Memory PostingEntry Extension

```rust
struct PostingEntry {
    doc_id: u32,
    tf: u32,
    doc_len: u32,
    // NEW: term positions within the document (0-indexed word offsets).
    // Empty when positions are disabled via recipe config.
    positions: SmallVec<[u32; 4]>,
}
```

**Why SmallVec<[u32; 4]>**: Most terms appear 1-3 times per document. SmallVec
inlines up to 4 elements on the stack (16 bytes), avoiding heap allocation for
the common case. Only long-tail high-TF entries (tf > 4) spill to the heap.

**Memory overhead per posting**:
- tf <= 4: 16 bytes inline (SmallVec overhead) → total 28 bytes vs 12 bytes = +133%
- tf > 4: 24 bytes (SmallVec heap ptr/len/cap) + 4×tf bytes on heap

For a corpus where average tf ≈ 1.5, the weighted average overhead is ~+110%.
This is the in-memory active segment cost; sealed segments compress much better.

### 2. Tokenizer: Position Tracking

Change the tokenizer to return position information:

```rust
/// A token with its position in the document.
pub struct Token {
    pub term: String,
    pub position: u32,
}

/// Tokenize text, returning stemmed tokens with positions.
///
/// Positions are 0-indexed word offsets. Stopwords create gaps —
/// the position counter increments even when a stopword is removed.
/// This preserves accurate distance information for phrase/proximity queries.
pub fn tokenize_with_positions(text: &str) -> Vec<Token> { ... }
```

**Stopword handling**: Stopwords increment the position counter but produce no
token. This follows the Lucene convention (`PositionIncrementAttribute`).

Example: `"The quick brown fox"`
- "The" → stopword, position 0 consumed, no token emitted
- "quick" → Token { term: "quick", position: 1 }
- "brown" → Token { term: "brown", position: 2 }
- "fox" → Token { term: "fox", position: 3 }

Phrase query `"quick brown"` matches because positions differ by exactly 1.

**Backward compatibility**: The existing `tokenize(text) -> Vec<String>` function
remains unchanged. `tokenize_with_positions` is a new function. `index_document()`
switches to using `tokenize_with_positions` internally.

### 3. Indexing: Capture Positions

`index_document()` changes:

```rust
// Before
let tokens = tokenize(text);
let mut term_freqs: HashMap<String, u32> = HashMap::new();
for token in &tokens {
    *term_freqs.entry(token.clone()).or_default() += 1;
}

// After
let tokens = tokenize_with_positions(text);
let mut term_positions: HashMap<String, Vec<u32>> = HashMap::new();
for token in &tokens {
    term_positions
        .entry(token.term.clone())
        .or_default()
        .push(token.position);
}
// tf = positions.len(), doc_len = max position + 1 (or token count)
```

Each PostingEntry is constructed with positions:

```rust
PostingEntry {
    doc_id,
    tf: positions.len() as u32,
    doc_len,
    positions: SmallVec::from_vec(positions),
}
```

### 4. Sealed Segment Format: .sidx v2

The `.sidx` format gains a **positions section** appended after the existing
postings section. The version field bumps to 2. Version 1 segments (no
positions) remain readable — they simply have no positions section.

```
HEADER (56 bytes):                              // was 48, +8 for positions_offset
  [0..4]    magic "SIDX"
  [4..8]    version = 2                         // bumped from 1
  [8..16]   segment_id
  [16..20]  doc_count
  [20..24]  term_count
  [24..32]  total_doc_len
  [32..40]  term_offsets_offset
  [40..48]  postings_offset
  [48..56]  positions_offset                    // NEW: byte offset to positions section

TERM DICTIONARY (unchanged)
TERM OFFSET TABLE (unchanged)

POSTINGS SECTION (unchanged):
  per term:
    num_entries: u32
    entries: [(delta_doc_id, tf, doc_len)]      // same varint triples

POSITIONS SECTION (NEW):
  per term (same order as postings):
    per document in posting list:
      positions: [varint; tf]                   // delta-encoded within document
```

**Position encoding**: Delta-encoded varints within each document. For positions
`[3, 7, 15, 42]`, store `[3, 4, 8, 27]`. First position is absolute (delta from 0).

**Why delta encoding**: Position gaps within a document are typically small (1-20
for common terms). Delta encoding reduces most values to 1 byte with varint.

**Bytes per position**: Approximately 1.0-1.5 bytes with delta + varint, compared
to 4.0 bytes for raw u32. For an average of 1.5 positions per posting, this adds
~1.5-2.3 bytes per posting entry, compared to the existing ~6-9 bytes for
(delta_doc_id, tf, doc_len).

**Index size impact**: Approximately **+25-35%** for the sealed segment file size
with position data enabled. This is lower than the 2-4x figure cited in IR
textbooks because Strata's corpus profile (short-to-medium documents, moderate
TF) produces small position lists.

### 5. PostingIter: Lazy Position Loading

The existing `PostingIter` decodes (doc_id, tf, doc_len) from the postings
section. A new `PositionReader` reads positions from the positions section:

```rust
/// Reads position data from a sealed segment's positions section.
struct PositionReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PositionReader<'a> {
    /// Decode positions for one document (tf positions, delta-encoded).
    fn read_positions(&mut self, tf: u32) -> SmallVec<[u32; 4]> {
        let mut positions = SmallVec::with_capacity(tf as usize);
        let mut prev = 0u32;
        for _ in 0..tf {
            let (delta, n) = decode_varint(&self.data[self.pos..]).unwrap();
            self.pos += n;
            prev += delta;
            positions.push(prev);
        }
        positions
    }

    /// Skip positions for one document without decoding.
    fn skip_positions(&mut self, tf: u32) { ... }
}
```

**Lazy loading**: During BM25 scoring, `PostingIter` reads doc_id/tf/doc_len as
before. Position data is **not decoded**. Only when a phrase or proximity query
needs positions for a specific candidate document does `PositionReader` decode
that document's positions.

This is critical for performance: BM25-only queries pay zero position decode
cost. The positions section stays out of the CPU cache.

### 6. Score Functions: No Change for BM25

`score_top_k()` and `score_precomputed()` continue to use only `doc_id`, `tf`,
and `doc_len` from PostingEntry. Positions are ignored during BM25 scoring.

Phrase queries (#2239) and proximity scoring (#2240) will add new scoring paths
that read positions. These are separate issues.

### 7. Recipe Knob

```rust
// In recipe.rs, BM25Config
pub struct BM25Config {
    pub k: Option<usize>,
    pub k1: Option<f32>,
    pub b: Option<f32>,
    // ... existing fields ...

    /// Store term positions in the inverted index.
    /// Enables phrase queries and proximity scoring.
    /// Default: true. Set to false for minimal index size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positions: Option<bool>,
}
```

When `positions` is `false`:
- `index_document()` uses `tokenize()` (no positions), stores empty positions vec
- Sealed segments write version 1 format (no positions section)
- Phrase/proximity queries return an error: "positions not enabled"

When `positions` is `true` (default):
- `index_document()` uses `tokenize_with_positions()`
- Sealed segments write version 2 format with positions section
- Phrase/proximity queries work

### 8. Segment Merge

During compaction, positions are document-local (word offsets within the
document), so they don't change when segments merge. The merge process:

1. Walk both segments' posting lists in term-sorted order
2. Merge posting entries (doc IDs may be remapped)
3. Copy positions verbatim for each retained document
4. Tombstoned documents: drop their positions entirely
5. Re-encode into fresh v2 sealed segment

### 9. Recovery

Position data is part of sealed segment files and is recovered by memory-mapping
the `.sidx` file. The version field in the header indicates whether positions are
present (v1 = no, v2 = yes).

For the active segment at crash time, positions are reconstructed by replaying the
WAL: each document is re-tokenized with `tokenize_with_positions()`, producing
identical positions (tokenization is deterministic). No special recovery logic
needed.

### 10. Backward Compatibility

- Version 1 sealed segments (no positions) remain readable. When loaded, they
  simply report empty positions for all posting entries.
- Mixed v1/v2 segments work correctly: BM25 scoring uses both, phrase queries
  only find matches in v2 segments (v1 segments contribute zero phrase hits,
  which is correct — they lack position data, not documents).
- No migration needed. New segments are written as v2; old segments age out
  through compaction.

---

## Implementation Plan

### Step 1: Tokenizer — `tokenize_with_positions()` (~50 lines)

Add `Token` struct and `tokenize_with_positions()` to `tokenizer.rs`. Keep
existing `tokenize()` unchanged. Unit tests: verify positions are correct,
stopwords create gaps, stemming doesn't affect positions.

### Step 2: PostingEntry — Add positions field (~30 lines)

Add `positions: SmallVec<[u32; 4]>` to `PostingEntry`. Update `PostingEntry::new()`
with a `positions` parameter. Add `PostingEntry::new_without_positions()` for the
no-positions path.

### Step 3: index_document() — Capture positions (~40 lines)

Switch from `tokenize()` to `tokenize_with_positions()` when positions are
enabled. Build `term_positions: HashMap<String, Vec<u32>>` instead of
`term_freqs: HashMap<String, u32>`.

### Step 4: .sidx v2 — Persist positions (~120 lines)

Extend `build_sealed_segment()` to write the positions section after the
postings section. Bump version to 2. Add `positions_offset` to header.
Update `SealedSegment` loading to handle both v1 and v2.

### Step 5: PositionReader — Lazy decode (~60 lines)

Add `PositionReader` to `segment.rs`. Expose a method on `SealedSegment` to
read positions for a specific term + document. Not called by BM25 — will be
used by phrase/proximity queries.

### Step 6: Recipe knob — `positions` in BM25Config (~10 lines)

Add the field to `BM25Config`. Wire it through `index_document()` to choose
between `tokenize()` and `tokenize_with_positions()`.

### Step 7: Tests (~100 lines)

- Tokenizer position correctness (stopword gaps, stemming, edge cases)
- PostingEntry with positions round-trip (index → seal → load → verify)
- .sidx v2 format (write, read, positions match)
- v1 backward compat (load v1 segment, verify empty positions)
- BM25 scoring unchanged (same nDCG@10 on NFCorpus before/after)

**Estimated total: ~410 lines of code + tests**

---

## Performance Budget

| Metric | Without positions | With positions | Delta |
|--------|-------------------|----------------|-------|
| PostingEntry memory | 12 bytes | ~28 bytes (avg) | +133% |
| .sidx file size | baseline | +25-35% | |
| index_document() time | baseline | +5-10% (position tracking) | |
| BM25 score_top_k() time | baseline | **unchanged** (positions not read) | +0% |
| Phrase query time | N/A | ~3-10x vs BM25 (position decode) | |

The key constraint: **BM25-only queries must not regress**. Position data is
stored but never touched during bag-of-words scoring.

---

## References

- Lucene90PostingsFormat: positions in separate `.pos` file, blocks of 128, PForDelta
- Tantivy: positions module, delta-encoded, bitpacked
- Manning, Raghavan, Schutze — "Introduction to IR", Ch. 2.4: Positional indexes
- Lemire et al. — "Decoding billions of integers per second through vectorization" (2015)
- TurboPFor analysis — ~11 bits/position with advanced compression
- ClickHouse RFC #101473 — Roaringish positional phrase query approach
