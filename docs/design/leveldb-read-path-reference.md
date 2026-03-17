# LevelDB Read Path Reference — Lessons for Strata

**Status:** Reference Document
**Date:** 2026-03-17
**Source:** LevelDB source code at `/tmp/leveldb` (cloned from github.com/google/leveldb)

This document traces LevelDB's point lookup implementation line-by-line and maps each design decision to Strata's current SegmentedStore, identifying concrete optimizations.

---

## 1. The Point Lookup: `Table::InternalGet`

**File:** `table/table.cc:214-241`

```cpp
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);                                          // [1] Binary search index block
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {        // [2] Bloom filter check
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());  // [3] Load data block (cached)
      block_iter->Seek(k);                                 // [4] Binary search within block
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());  // [5] Return key + value
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  delete iiter;
  return s;
}
```

### Key observations:

1. **Index block is ALREADY in memory** — `rep_->index_block` is loaded once at `Table::Open` time and kept for the lifetime of the table. No I/O on the read path.

2. **Bloom filter is checked AFTER index lookup** — LevelDB knows which data block the key would be in, then checks the bloom filter for that specific block offset. This is a per-block bloom check, not per-SST.

3. **Block cache is integrated at `BlockReader`** — The data block is looked up in the shared block cache before any file I/O. Cache key is `(cache_id, block_offset)`.

4. **`block_iter->Seek(k)` does binary search WITHIN the block** — this is the critical difference from Strata. LevelDB does NOT linearly scan all entries. It uses restart points for binary search.

5. **Value is a zero-copy Slice** — `block_iter->value()` returns a `Slice` pointing directly into the block's memory. No deserialization, no allocation.

---

## 2. The Block Iterator: Binary Search via Restart Points

**File:** `table/block.cc:164-227`

This is the most important code for our optimization:

```cpp
void Seek(const Slice& target) override {
    // Binary search in restart array to find the last restart point
    // with a key < target
    uint32_t left = 0;
    uint32_t right = num_restarts_ - 1;

    while (left < right) {
      uint32_t mid = (left + right + 1) / 2;
      uint32_t region_offset = GetRestartPoint(mid);
      uint32_t shared, non_shared, value_length;
      const char* key_ptr =
          DecodeEntry(data_ + region_offset, data_ + restarts_, &shared,
                      &non_shared, &value_length);
      Slice mid_key(key_ptr, non_shared);               // [A] Only reads the KEY, not the value
      if (Compare(mid_key, target) < 0) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }

    SeekToRestartPoint(left);
    // Linear search (within restart block) for first key >= target
    while (true) {
      if (!ParseNextKey()) return;
      if (Compare(key_, target) >= 0) return;            // [B] Stops as soon as key >= target
    }
}
```

### Critical design decisions:

**[A] During binary search, only the KEY is read.** At restart points, `shared = 0` (full key stored). `DecodeEntry` reads `shared_bytes`, `non_shared_bytes`, `value_length` from the header, then the code constructs a `Slice` over just the key bytes (`key_ptr, non_shared`). **The value bytes are never touched.** They're skipped over by pointer arithmetic (`p + non_shared` to get past the key, then `+ value_length` to get to the next entry).

**[B] Linear scan within a restart interval is short.** With the default restart interval of 16, at most 15 entries are scanned linearly. Each scan step in `ParseNextKey` reconstructs the key via prefix compression (`key_.resize(shared); key_.append(p, non_shared)`) and sets `value_` to a Slice pointing into the block data. **No value deserialization ever happens** — the value is just a byte range.

### The `ParseNextKey` function:

```cpp
bool ParseNextKey() {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;

    uint32_t shared, non_shared, value_length;
    p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);

    key_.resize(shared);
    key_.append(p, non_shared);                    // Reconstruct key from prefix
    value_ = Slice(p + non_shared, value_length);  // Value is just a pointer+length
    return true;
}
```

**Value is NEVER deserialized.** It's a `Slice(pointer, length)` — a zero-copy view into the block's raw bytes. The caller gets the raw bytes and deserializes only if needed.

### The `DecodeEntry` function:

```cpp
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_length) {
    // Fast path: all three values fit in one byte each
    if ((*shared | *non_shared | *value_length) < 128) {
        p += 3;  // Just 3 bytes of overhead per entry
    } else {
        // Varint decode (slower path)
    }
    return p;  // Returns pointer to key delta bytes
}
```

**Entry header is 3 bytes** in the common case (all lengths < 128). Compare to Strata's entry header: `ik_len(4) + value_kind(1) + timestamp(8) + ttl_ms(8) + value_len(4) = 25 bytes`. LevelDB's is 8x smaller.

---

## 3. Block Cache Integration

**File:** `table/table.cc:153-206`

```cpp
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
    Cache* block_cache = table->rep_->options.block_cache;
    Block* block = nullptr;
    Cache::Handle* cache_handle = nullptr;

    if (block_cache != nullptr) {
      // Cache key = (cache_id, block_offset) — 16 bytes
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));

      cache_handle = block_cache->Lookup(key);       // [1] Check cache
      if (cache_handle != nullptr) {
        block = block_cache->Value(cache_handle);    // [2] Cache hit — zero I/O
      } else {
        ReadBlock(table->rep_->file, options, handle, &contents);  // [3] Cache miss — read from file
        block = new Block(contents);
        cache_handle = block_cache->Insert(key, block, block->size(),
                                           &DeleteCachedBlock);   // [4] Insert into cache
      }
    }
    return block->NewIterator(comparator);  // [5] Return iterator over block
}
```

**Key difference from Strata:** LevelDB caches the `Block` object (parsed restart points + raw data), not just the raw decompressed bytes. This means the restart point array is computed once and reused across cache hits.

---

## 4. Entry Format Comparison

### LevelDB entry format:
```
shared_bytes:   varint32  (typically 1 byte)
non_shared_bytes: varint32  (typically 1 byte)
value_length:   varint32  (typically 1 byte)
key_delta:      char[non_shared_bytes]
value:          char[value_length]
```
**Overhead per entry: 3 bytes** (common case). Value is raw bytes — no serialization format.

### Strata entry format:
```
ik_len:         u32 LE    (4 bytes, fixed)
ik_bytes:       [u8]      (variable, typically 40-60 bytes)
value_kind:     u8        (1 byte)
timestamp:      u64 LE    (8 bytes)
ttl_ms:         u64 LE    (8 bytes)
value_len:      u32 LE    (4 bytes, fixed)
value_bytes:    [u8]      (variable, bincode-serialized Value enum)
```
**Overhead per entry: 25 bytes** (fixed). Value is bincode-serialized — requires deserialization.

### Impact:
- LevelDB: 3 bytes overhead → more entries per block → better cache utilization
- Strata: 25 bytes overhead → fewer entries per block → more blocks per lookup
- LevelDB: value is raw bytes → zero-copy on read
- Strata: value is bincode → must deserialize on read (even for non-matching keys!)

---

## 5. Mapping to Strata: Concrete Optimizations

### Optimization 1: Skip Value Deserialization (HIGH IMPACT, LOW EFFORT)

**The problem:** Strata's `scan_block_for_key` calls `decode_entry()` which deserializes EVERY value via `bincode::deserialize()`, even for entries that don't match the target key. At 64 entries per block, ~32 bincode deserializations are wasted per lookup.

**LevelDB's approach:** Value bytes are NEVER deserialized during lookup. `ParseNextKey` sets `value_ = Slice(p + non_shared, value_length)` — just pointer arithmetic. Only the caller deserializes if needed.

**Strata fix:** Split `decode_entry` into two phases:
1. `decode_entry_header(data) -> Option<(InternalKey, bool, u64, u64, usize, usize)>` — parses key + metadata + value_len, returns (key, is_tombstone, timestamp, ttl_ms, value_start, total_consumed). Does NOT deserialize value bytes.
2. Only call `bincode::deserialize` on the matching entry's value bytes.

**Expected impact:** ~60% reduction in per-block scan time. The dominant cost shifts from bincode deserialization to key comparison.

### Optimization 2: Binary Search Within Blocks (MEDIUM IMPACT, MEDIUM EFFORT)

**The problem:** Strata linearly scans all entries in a block until finding the target key. With 64 entries per block, this is O(64) key comparisons.

**LevelDB's approach:** Binary search on restart points (every 16 entries) narrows to a 16-entry window, then linear scan within. Total: ~4 binary search comparisons + ~8 linear comparisons = ~12 comparisons.

**Strata fix:** Add restart points to the segment builder. Store full InternalKey every N entries (e.g., 16), with a restart offset array at the end of the block. The block iterator binary-searches restart points, then linear-scans the interval.

**Complexity:** Requires changes to both `SegmentBuilder` (write restart points) and `KVSegment` (read with binary search). Format change — new segments use restart points, old segments use linear scan.

**Expected impact:** ~4x fewer key comparisons per block. Combined with skip-value-deserialization, per-block scan drops from ~19us to ~2-3us.

### Optimization 3: Zero-Copy Value Access (MEDIUM IMPACT, HIGH EFFORT)

**The problem:** Strata deserializes values with `bincode::deserialize()`, which allocates a new `Value` enum on the heap. LevelDB returns a `Slice` (pointer + length) into the block's memory.

**Strata challenge:** Strata's `Value` is a Rust enum (`Int`, `String`, `Bytes`, etc.) serialized with bincode. Switching to zero-copy would require either:
- A custom serialization format where `Value::String` is stored as a length-prefixed UTF-8 slice that can be borrowed from the block
- Or keeping bincode but deferring deserialization until the caller actually needs the value

**Practical approach:** For point lookups, we only deserialize the ONE matching value. After Optimization 1, this is a single `bincode::deserialize` per lookup — acceptable. Zero-copy is only valuable for scans iterating many values.

**Expected impact:** Negligible for point lookups (already only 1 deserialization). Significant for scans.

### Optimization 4: Prefix-Compressed Keys (LOW PRIORITY)

**The problem:** Strata stores full InternalKey bytes for every entry. LevelDB uses prefix compression (shared_bytes + non_shared_bytes) to reduce key storage by ~60-80%.

**Impact:** Smaller blocks → more entries fit in cache → better cache hit rate. Also enables restart points (which require prefix compression to make binary search work).

**Complexity:** High — changes the block format significantly.

**Priority:** Do this together with Optimization 2 (restart points require prefix compression).

---

## 6. Recommended Implementation Order

```
Step 1: Skip value deserialization in scan_block_for_key     [2 hours, ~50 LOC]
        → Expected: 1M reads from 24K/s to 50-80K/s

Step 2: Binary search within blocks (restart points)         [1-2 days, ~300 LOC]
        → Expected: 1M reads from 50-80K/s to 150-250K/s

Step 3: Prefix-compressed keys                               [1-2 days, ~400 LOC]
        → Expected: Better cache hit rate, smaller segments

Step 4: Leveled compaction (L1+ non-overlapping)              [2-3 days, ~500 LOC]
        → Expected: 1M reads from 150-250K/s to 300-500K/s
```

Step 1 is the immediate win — it's a pure code change in `scan_block_for_key` and `decode_entry` with no format changes. Steps 2-3 require segment format changes (format version 3). Step 4 is an architectural change in the compaction scheduler.

---

## 7. Key Architectural Difference: Value Semantics

The fundamental difference between LevelDB and Strata is **value semantics**:

- **LevelDB:** Values are opaque byte strings. The database never interprets them. Zero deserialization overhead.
- **Strata:** Values are typed (`Value::Int`, `Value::String`, `Value::Object`, etc.) serialized with bincode. The database must deserialize to return typed values.

This means Strata will always have higher per-entry overhead than LevelDB for value-heavy operations. The optimization strategy is to **minimize how many values get deserialized** (skip non-matching entries) rather than eliminating deserialization entirely.

For the point lookup hot path, the goal is: **deserialize exactly 1 value per lookup** (the matching entry). Currently we deserialize ~32 values per lookup.
