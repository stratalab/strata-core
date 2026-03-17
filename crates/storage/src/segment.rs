//! KV segment reader — opens mmap'd segment files for queries.
//!
//! A `KVSegment` is an immutable sorted file produced by [`SegmentBuilder`].
//! It supports:
//!
//! - **Point lookups** via bloom filter + index block binary search + block scan
//! - **Prefix scans** via ordered iteration from a seek position
//! - **MVCC filtering** via per-entry commit_id
//!
//! [`SegmentBuilder`]: crate::segment_builder::SegmentBuilder

use crate::bloom::BloomFilter;
use crate::key_encoding::{encode_typed_key, encode_typed_key_prefix, InternalKey};
use crate::segment_builder::{
    decode_entry, decode_entry_header_ref, decode_entry_value, parse_footer, parse_framed_block,
    parse_header, parse_index_block, parse_properties_block, EntryHeader, Footer, IndexEntry,
    KVHeader, PropertiesBlock, FOOTER_SZ, FRAME_OVERHEAD, HEADER_SIZE,
};
use strata_core::types::Key;
use strata_core::value::Value;

use memmap2::Mmap;
use std::io;
use std::path::Path;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// SegmentEntry — what queries return
// ---------------------------------------------------------------------------

/// A single entry read from a KV segment.
#[derive(Debug, Clone)]
pub struct SegmentEntry {
    /// The value (Null for tombstones).
    pub value: Value,
    /// Whether this is a deletion tombstone.
    pub is_tombstone: bool,
    /// The commit_id of this entry.
    pub commit_id: u64,
    /// Microseconds since epoch (0 for v1 segments).
    pub timestamp: u64,
    /// TTL in milliseconds (0 = no TTL, 0 for v1 segments).
    pub ttl_ms: u64,
}

// ---------------------------------------------------------------------------
// KVSegment
// ---------------------------------------------------------------------------

/// An immutable, mmap'd KV segment file.
///
/// Opened via [`KVSegment::open`], provides point lookups and prefix iteration
/// with MVCC snapshot filtering.
pub struct KVSegment {
    mmap: Mmap,
    header: KVHeader,
    #[allow(dead_code)] // used by future compaction/GC
    footer: Footer,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    #[allow(dead_code)] // used by future compaction/GC
    props: PropertiesBlock,
    /// Path to the .sst file (for cleanup after compaction).
    file_path: std::path::PathBuf,
    /// Hash of file_path for block cache keying.
    file_id: u64,
}

impl KVSegment {
    /// Open and parse a KV segment file.
    ///
    /// Validates header magic, footer magic, and block checksums on the
    /// metadata blocks (index, bloom, properties). Data block CRCs are
    /// checked lazily on access.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        // SAFETY: The file is immutable after creation. We hold no mutable
        // references. The mmap lifetime is tied to this struct.
        let mmap = unsafe { Mmap::map(&file)? };

        if mmap.len() < HEADER_SIZE + FOOTER_SZ {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment file too small",
            ));
        }

        // Parse header
        let header_bytes: &[u8; HEADER_SIZE] = mmap[..HEADER_SIZE]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "header size mismatch"))?;
        let header = parse_header(header_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid segment header"))?;

        // Parse footer
        let footer_start = mmap.len() - FOOTER_SZ;
        let footer_bytes: &[u8; FOOTER_SZ] = mmap[footer_start..]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "footer size mismatch"))?;
        let footer = parse_footer(footer_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid segment footer (bad magic)",
            )
        })?;

        // Helper: validate block offset + length fits within the mmap.
        let check_block_bounds =
            |offset: u64, len: u32, name: &str| -> io::Result<(usize, usize)> {
                let start = offset as usize;
                let end = start.checked_add(len as usize).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{} offset+len overflows", name),
                    )
                })?;
                if end > mmap.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{} extends past end of file", name),
                    ));
                }
                Ok((start, end))
            };

        // Parse index block
        let (idx_start, idx_end) = check_block_bounds(
            footer.index_block_offset,
            footer.index_block_len,
            "index block",
        )?;
        let (_, idx_data) = parse_framed_block(&mmap[idx_start..idx_end]).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "index block CRC mismatch")
        })?;
        let index = parse_index_block(idx_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed index block"))?;

        // Parse bloom filter block
        let (bloom_start, bloom_end) = check_block_bounds(
            footer.filter_block_offset,
            footer.filter_block_len,
            "bloom block",
        )?;
        let (_, bloom_data) =
            parse_framed_block(&mmap[bloom_start..bloom_end]).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "bloom block CRC mismatch")
            })?;
        let bloom = BloomFilter::from_bytes(bloom_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed bloom filter"))?;

        // Parse properties block
        let (props_start, props_end) = check_block_bounds(
            footer.props_block_offset,
            footer.props_block_len,
            "properties block",
        )?;
        let (_, props_data) =
            parse_framed_block(&mmap[props_start..props_end]).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "properties block CRC mismatch")
            })?;
        let props = parse_properties_block(props_data).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "malformed properties block")
        })?;

        let file_path = path.to_path_buf();
        let file_id = crate::block_cache::file_path_hash(&file_path);
        Ok(Self {
            mmap,
            header,
            footer,
            index,
            bloom,
            props,
            file_path,
            file_id,
        })
    }

    /// Check the bloom filter for a key (without commit_id).
    ///
    /// Returns `false` if the key definitely does not exist in this segment.
    pub fn bloom_maybe_contains(&self, key: &Key) -> bool {
        let typed = encode_typed_key(key);
        self.bloom.maybe_contains(&typed)
    }

    /// Point lookup: find the newest version of `key` with commit_id ≤ `snapshot_commit`.
    ///
    /// Returns `None` if:
    /// - Bloom filter says key is absent
    /// - No matching entry exists at or below the snapshot
    pub fn point_lookup(&self, key: &Key, snapshot_commit: u64) -> Option<SegmentEntry> {
        // 1. Bloom check
        if !self.bloom_maybe_contains(key) {
            return None;
        }

        let typed_key = encode_typed_key(key);

        // 2. Find candidate data block via binary search on index.
        //
        // Index keys are full InternalKey bytes (first key of each block).
        // Because InternalKey encodes commit_id in descending order, the seek
        // key `(key, u64::MAX)` sorts BEFORE all actual entries for that key.
        // So Err(0) does NOT mean the key is absent — it may be in block 0.
        let seek_ik = InternalKey::encode(key, u64::MAX);
        let seek_bytes = seek_ik.as_bytes();

        let block_idx = match self
            .index
            .binary_search_by(|e| e.key.as_slice().cmp(seek_bytes))
        {
            Ok(i) => i,
            Err(0) => 0, // key may still be in the first block
            Err(i) => i - 1,
        };

        // Scan this block (and possibly the next if the key spans a block boundary)
        for bi in block_idx..self.index.len() {
            let ie = &self.index[bi];

            // If this block's first key has a typed_key_prefix > our typed_key, stop
            if bi > block_idx {
                let block_first_ik = match InternalKey::try_from_bytes(ie.key.clone()) {
                    Some(ik) => ik,
                    None => break, // Corrupt index key — stop scanning
                };
                if block_first_ik.typed_key_prefix() > typed_key.as_slice() {
                    break;
                }
            }

            if let Some(entry) = self.scan_block_for_key(ie, &typed_key, snapshot_commit) {
                return Some(entry);
            }
        }

        None
    }

    /// Iterate entries starting from `prefix`, yielding `(InternalKey, SegmentEntry)` pairs.
    ///
    /// Entries are in InternalKey order. The caller is responsible for MVCC dedup.
    /// Iteration stops when entries no longer match the prefix.
    pub fn iter_seek<'a>(&'a self, prefix: &Key) -> SegmentIter<'a> {
        let prefix_bytes = encode_typed_key_prefix(prefix);
        let seek_ik = InternalKey::encode(prefix, u64::MAX);
        let seek_bytes = seek_ik.as_bytes().to_vec();

        // Find the starting block
        let start_block = match self
            .index
            .binary_search_by(|e| e.key.as_slice().cmp(seek_bytes.as_slice()))
        {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };

        SegmentIter {
            segment: self,
            prefix_bytes,
            block_idx: start_block,
            block_offset: 0,
            block_data: None,
            done: false,
        }
    }

    /// The `(commit_min, commit_max)` range for this segment.
    pub fn commit_range(&self) -> (u64, u64) {
        (self.header.commit_min, self.header.commit_max)
    }

    /// Total entry count (all versions).
    pub fn entry_count(&self) -> u64 {
        self.header.entry_count
    }

    /// File size in bytes (from the underlying mmap).
    pub fn file_size(&self) -> u64 {
        self.mmap.len() as u64
    }

    /// File identity hash (for block cache keying and invalidation).
    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    /// Path to the .sst file on disk.
    pub fn file_path(&self) -> &std::path::Path {
        &self.file_path
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Read and verify a data block, checking the global block cache first.
    ///
    /// On cache hit, returns the cached decompressed block (zero decompression cost).
    /// On cache miss, reads from mmap, decompresses if needed, caches, and returns.
    fn read_data_block(&self, ie: &IndexEntry) -> Option<Arc<Vec<u8>>> {
        let cache = crate::block_cache::global_cache();
        let block_offset = ie.block_offset;

        // Check cache first
        if let Some(cached) = cache.get(self.file_id, block_offset) {
            return Some(cached);
        }

        // Cache miss: read from mmap and decompress
        let start = block_offset as usize;
        let framed_len = FRAME_OVERHEAD + ie.block_data_len as usize;
        let end = start + framed_len;
        if end > self.mmap.len() {
            return None;
        }

        let raw = &self.mmap[start..end];
        let codec_byte = raw[1];
        let (_, data) = parse_framed_block(raw)?;

        let decompressed = match codec_byte {
            0 => data.to_vec(),                // Uncompressed
            1 => zstd::decode_all(data).ok()?, // Zstd
            _ => return None,                  // Unknown codec
        };

        // Cache the decompressed block
        Some(cache.insert(self.file_id, block_offset, decompressed))
    }

    /// Scan a single data block for the newest version of a typed key at or below snapshot.
    ///
    /// Uses zero-copy two-phase decoding:
    /// 1. Parse key bytes + metadata WITHOUT allocating (EntryHeaderRef)
    /// 2. Only allocate + deserialize value for the matching entry
    ///
    /// This eliminates ~32 wasted bincode::deserialize calls and ~32 Vec
    /// allocations per block lookup.
    fn scan_block_for_key(
        &self,
        ie: &IndexEntry,
        typed_key: &[u8],
        snapshot_commit: u64,
    ) -> Option<SegmentEntry> {
        let block_data = self.read_data_block(ie)?;
        let data = &**block_data;
        let mut pos = 0;
        while pos < data.len() {
            // Phase 1: zero-copy key decode (no allocation)
            let ref_header = decode_entry_header_ref(&data[pos..])?;
            let entry_data_start = pos;
            pos += ref_header.total_len;

            if ref_header.typed_key_prefix() != typed_key {
                if ref_header.typed_key_prefix() > typed_key {
                    break;
                }
                continue;
            }

            let commit_id = ref_header.commit_id();
            if commit_id <= snapshot_commit {
                // Phase 2: allocate + deserialize ONLY for the match
                let header = EntryHeader {
                    ik: InternalKey::try_from_bytes(ref_header.ik_bytes.to_vec())?,
                    is_tombstone: ref_header.is_tombstone,
                    timestamp: ref_header.timestamp,
                    ttl_ms: ref_header.ttl_ms,
                    value_start: ref_header.value_start,
                    value_len: ref_header.value_len,
                    total_len: ref_header.total_len,
                };
                let value = decode_entry_value(&data[entry_data_start..], &header)?;
                return Some(SegmentEntry {
                    value,
                    is_tombstone: header.is_tombstone,
                    commit_id,
                    timestamp: header.timestamp,
                    ttl_ms: header.ttl_ms,
                });
            }
        }

        None
    }

    /// Iterate ALL entries in the segment, in InternalKey order.
    ///
    /// Used by `SegmentedStore::list_branch` to scan every entry.
    pub fn iter_seek_all(&self) -> SegmentIter<'_> {
        SegmentIter {
            segment: self,
            prefix_bytes: Vec::new(), // empty prefix matches everything
            block_idx: 0,
            block_offset: 0,
            block_data: None,
            done: self.index.is_empty(),
        }
    }
}

// ---------------------------------------------------------------------------
// SegmentIter — prefix scan iterator
// ---------------------------------------------------------------------------

/// Iterator over segment entries matching a prefix.
pub struct SegmentIter<'a> {
    segment: &'a KVSegment,
    prefix_bytes: Vec<u8>,
    block_idx: usize,
    block_offset: usize,
    block_data: Option<Arc<Vec<u8>>>,
    done: bool,
}

impl<'a> Iterator for SegmentIter<'a> {
    type Item = (InternalKey, SegmentEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }

            // Load current block if needed
            if self.block_data.is_none() {
                if self.block_idx >= self.segment.index.len() {
                    self.done = true;
                    return None;
                }
                let ie = &self.segment.index[self.block_idx];
                match self.segment.read_data_block(ie) {
                    Some(data) => {
                        self.block_data = Some(data);
                        self.block_offset = 0;
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }

            let data = self.block_data.as_ref().unwrap();

            if self.block_offset >= data.len() {
                // Move to next block
                self.block_data = None;
                self.block_idx += 1;
                continue;
            }

            match decode_entry(&data[self.block_offset..]) {
                Some((ik, is_tomb, value, timestamp, ttl_ms, consumed)) => {
                    self.block_offset += consumed;

                    // Check prefix match
                    if !ik.typed_key_prefix().starts_with(&self.prefix_bytes) {
                        // If past the prefix range, stop
                        if ik.typed_key_prefix() > self.prefix_bytes.as_slice() {
                            self.done = true;
                            return None;
                        }
                        // Before prefix range, skip
                        continue;
                    }

                    let commit_id = ik.commit_id();
                    return Some((
                        ik,
                        SegmentEntry {
                            value,
                            is_tombstone: is_tomb,
                            commit_id,
                            timestamp,
                            ttl_ms,
                        },
                    ));
                }
                None => {
                    // Malformed entry, stop
                    self.done = true;
                    return None;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use crate::segment_builder::SegmentBuilder;
    use std::sync::Arc;
    use strata_core::types::{BranchId, Namespace, TypeTag};

    fn branch() -> BranchId {
        BranchId::from_bytes([1; 16])
    }

    fn kv_key(user_key: &str) -> Key {
        let ns = Arc::new(Namespace::new(branch(), "default".to_string()));
        Key::new(ns, TypeTag::KV, user_key.as_bytes().to_vec())
    }

    fn build_segment(mt: &Memtable, path: &Path) {
        let builder = SegmentBuilder::default();
        builder.build_from_iter(mt.iter_all(), path).unwrap();
    }

    fn build_segment_small_blocks(mt: &Memtable, path: &Path) {
        let builder = SegmentBuilder {
            data_block_size: 256,
            bloom_bits_per_key: 10,
        };
        builder.build_from_iter(mt.iter_all(), path).unwrap();
    }

    // ===== Open and validate =====

    #[test]
    fn open_valid_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("k1"), 1, Value::Int(42), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.entry_count(), 1);
        assert_eq!(seg.commit_range(), (1, 1));
    }

    #[test]
    fn open_rejects_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.sst");
        std::fs::write(&path, b"too short").unwrap();
        assert!(KVSegment::open(&path).is_err());
    }

    #[test]
    fn open_rejects_bad_footer_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("good.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("k1"), 1, Value::Int(1), false);
        mt.freeze();
        build_segment(&mt, &path);

        // Corrupt the footer magic
        let mut data = std::fs::read(&path).unwrap();
        let magic_offset = data.len() - 8;
        data[magic_offset] = b'X';
        let corrupt_path = dir.path().join("corrupt.sst");
        std::fs::write(&corrupt_path, &data).unwrap();
        assert!(KVSegment::open(&corrupt_path).is_err());
    }

    // ===== Point lookups =====

    #[test]
    fn point_lookup_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("a"), 1, Value::Int(10), false);
        mt.put(&kv_key("b"), 2, Value::Int(20), false);
        mt.put(&kv_key("c"), 3, Value::String("hello".into()), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        let e = seg.point_lookup(&kv_key("a"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert!(!e.is_tombstone);

        let e = seg.point_lookup(&kv_key("b"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(20));

        let e = seg.point_lookup(&kv_key("c"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::String("hello".into()));
    }

    #[test]
    fn point_lookup_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("a"), 1, Value::Int(1), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(seg.point_lookup(&kv_key("z"), u64::MAX).is_none());
    }

    #[test]
    fn point_lookup_mvcc_filtering() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("k"), 1, Value::Int(10), false);
        mt.put(&kv_key("k"), 5, Value::Int(50), false);
        mt.put(&kv_key("k"), 10, Value::Int(100), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Snapshot at 10: see version 10
        let e = seg.point_lookup(&kv_key("k"), 10).unwrap();
        assert_eq!(e.value, Value::Int(100));
        assert_eq!(e.commit_id, 10);

        // Snapshot at 7: see version 5
        let e = seg.point_lookup(&kv_key("k"), 7).unwrap();
        assert_eq!(e.value, Value::Int(50));
        assert_eq!(e.commit_id, 5);

        // Snapshot at 1: see version 1
        let e = seg.point_lookup(&kv_key("k"), 1).unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert_eq!(e.commit_id, 1);

        // Snapshot at 0: nothing visible
        assert!(seg.point_lookup(&kv_key("k"), 0).is_none());
    }

    #[test]
    fn point_lookup_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("k"), 1, Value::Int(10), false);
        mt.put(&kv_key("k"), 2, Value::Null, true);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Snapshot at 2: see tombstone
        let e = seg.point_lookup(&kv_key("k"), 2).unwrap();
        assert!(e.is_tombstone);
        assert_eq!(e.commit_id, 2);

        // Snapshot at 1: see the value
        let e = seg.point_lookup(&kv_key("k"), 1).unwrap();
        assert_eq!(e.value, Value::Int(10));
    }

    // ===== Bloom filter =====

    #[test]
    fn bloom_filter_no_false_negatives() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        for i in 0..100u32 {
            mt.put(
                &kv_key(&format!("key_{}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        for i in 0..100u32 {
            assert!(
                seg.bloom_maybe_contains(&kv_key(&format!("key_{}", i))),
                "false negative for key_{}",
                i,
            );
        }
    }

    // ===== Prefix iteration =====

    #[test]
    fn iter_seek_prefix_scan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("user:1"), 1, Value::Int(1), false);
        mt.put(&kv_key("user:2"), 1, Value::Int(2), false);
        mt.put(&kv_key("user:3"), 1, Value::Int(3), false);
        mt.put(&kv_key("order:1"), 1, Value::Int(100), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let results: Vec<_> = seg.iter_seek(&kv_key("user:")).collect();
        assert_eq!(results.len(), 3);

        // Should be in order: user:1, user:2, user:3
        let (ik1, _) = &results[0];
        let (ik2, _) = &results[1];
        let (ik3, _) = &results[2];
        assert!(ik1 < ik2);
        assert!(ik2 < ik3);
    }

    #[test]
    fn iter_seek_returns_all_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("k"), 1, Value::Int(10), false);
        mt.put(&kv_key("k"), 5, Value::Int(50), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let results: Vec<_> = seg.iter_seek(&kv_key("k")).collect();
        // Both versions (commit 5 first due to descending commit order)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.commit_id, 5);
        assert_eq!(results[1].1.commit_id, 1);
    }

    #[test]
    fn iter_seek_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("a"), 1, Value::Int(1), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let results: Vec<_> = seg.iter_seek(&kv_key("z")).collect();
        assert!(results.is_empty());
    }

    // ===== Round-trip integration: memtable → segment → read =====

    #[test]
    fn full_roundtrip_all_value_types() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("null"), 1, Value::Null, false);
        mt.put(&kv_key("bool"), 1, Value::Bool(true), false);
        mt.put(&kv_key("int"), 1, Value::Int(42), false);
        mt.put(&kv_key("float"), 1, Value::Float(3.14), false);
        mt.put(&kv_key("string"), 1, Value::String("hello".into()), false);
        mt.put(
            &kv_key("bytes"),
            1,
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            false,
        );
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        assert_eq!(
            seg.point_lookup(&kv_key("null"), u64::MAX).unwrap().value,
            Value::Null,
        );
        assert_eq!(
            seg.point_lookup(&kv_key("bool"), u64::MAX).unwrap().value,
            Value::Bool(true),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("int"), u64::MAX).unwrap().value,
            Value::Int(42),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("float"), u64::MAX).unwrap().value,
            Value::Float(3.14),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("string"), u64::MAX).unwrap().value,
            Value::String("hello".into()),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("bytes"), u64::MAX).unwrap().value,
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        );
    }

    // ===== Multi-block lookups =====

    #[test]
    fn point_lookup_across_multiple_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let mt = Memtable::new(0);
        for i in 0..500u32 {
            let k = kv_key(&format!("key_{:06}", i));
            let val = Value::String(format!("value_{}", "x".repeat(50)));
            mt.put(&k, i as u64 + 1, val, false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.entry_count(), 500);

        // Look up specific keys
        let e = seg.point_lookup(&kv_key("key_000000"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 1);

        let e = seg.point_lookup(&kv_key("key_000250"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 251);

        let e = seg.point_lookup(&kv_key("key_000499"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 500);

        // Non-existent key
        assert!(seg.point_lookup(&kv_key("key_999999"), u64::MAX).is_none());
    }

    #[test]
    fn iter_across_multiple_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = kv_key(&format!("item_{:04}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Iterate all with empty prefix
        let all: Vec<_> = seg.iter_seek(&kv_key("item_")).collect();
        assert_eq!(all.len(), 200);

        // Check ordering
        for i in 1..all.len() {
            assert!(all[i - 1].0 < all[i].0, "entries must be in order");
        }
    }

    // ===== CRC corruption detection =====

    #[test]
    fn detects_data_block_crc_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("k"), 1, Value::Int(1), false);
        mt.freeze();
        build_segment(&mt, &path);

        let data = std::fs::read(&path).unwrap();

        // Corrupt the CRC of the data block specifically.
        // Data block starts at HEADER_SIZE. The CRC is the last 4 bytes of the framed block.
        // Frame: type(1) + codec(1) + reserved(2) + data_len(4) + data(N) + crc(4).
        // Read data_len to find the CRC offset.
        let data_len =
            u32::from_le_bytes(data[HEADER_SIZE + 4..HEADER_SIZE + 8].try_into().unwrap()) as usize;
        let crc_offset = HEADER_SIZE + 8 + data_len;

        let mut corrupt = data.clone();
        corrupt[crc_offset] ^= 0xFF; // flip a CRC byte

        let corrupt_path = dir.path().join("corrupt_crc.sst");
        std::fs::write(&corrupt_path, &corrupt).unwrap();

        // Open should succeed (data blocks are lazily verified)
        let seg = KVSegment::open(&corrupt_path).unwrap();
        // But point_lookup must fail because read_data_block checks CRC
        assert!(
            seg.point_lookup(&kv_key("k"), u64::MAX).is_none(),
            "corrupt data block CRC should cause lookup to return None",
        );
    }

    // ===== Properties =====

    #[test]
    fn commit_range_matches_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("a"), 5, Value::Int(1), false);
        mt.put(&kv_key("b"), 10, Value::Int(2), false);
        mt.put(&kv_key("c"), 3, Value::Int(3), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.commit_range(), (3, 10));
        assert_eq!(seg.entry_count(), 3);
    }

    // ===== Empty segment =====

    #[test]
    fn empty_segment_open_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SegmentBuilder::default();
        builder.build_from_iter(std::iter::empty(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.entry_count(), 0);
        assert_eq!(seg.commit_range(), (0, 0));
        assert!(seg.point_lookup(&kv_key("anything"), u64::MAX).is_none());
        assert_eq!(seg.iter_seek(&kv_key("")).collect::<Vec<_>>().len(), 0);
    }

    // ===== Array and Object value roundtrip =====

    #[test]
    fn roundtrip_array_and_object_values() {
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("complex.sst");

        let array_val = Value::array(vec![Value::Int(1), Value::String("two".into())]);
        let mut map = HashMap::new();
        map.insert("nested".to_string(), Value::Bool(true));
        let object_val = Value::object(map);

        let mt = Memtable::new(0);
        mt.put(&kv_key("arr"), 1, array_val.clone(), false);
        mt.put(&kv_key("obj"), 1, object_val.clone(), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        assert_eq!(
            seg.point_lookup(&kv_key("arr"), u64::MAX).unwrap().value,
            array_val,
        );
        assert_eq!(
            seg.point_lookup(&kv_key("obj"), u64::MAX).unwrap().value,
            object_val,
        );
    }

    // ===== Large segment property test =====

    #[test]
    fn large_segment_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.sst");

        let mt = Memtable::new(0);
        let n = 10_000u32;
        for i in 0..n {
            let k = kv_key(&format!("k_{:08}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 4096,
            bloom_bits_per_key: 10,
        };
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.entry_count(), n as u64);

        // Verify every entry via point lookup
        for i in 0..n {
            let k = kv_key(&format!("k_{:08}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .expect(&format!("point lookup failed for k_{:08}", i));
            assert_eq!(e.value, Value::Int(i as i64));
            assert_eq!(e.commit_id, i as u64 + 1);
        }
    }

    // ===== Timestamp & TTL (v2 format) =====

    #[test]
    fn point_lookup_returns_timestamp_and_ttl() {
        use crate::memtable::MemtableEntry;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2ts.sst");

        let mt = Memtable::new(0);
        let ts = strata_core::Timestamp::from_micros(1_700_000_000_000_000);
        mt.put_entry(
            &kv_key("k1"),
            1,
            MemtableEntry {
                value: Value::Int(42),
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 30_000,
            },
        );
        mt.put_entry(
            &kv_key("k2"),
            2,
            MemtableEntry {
                value: Value::Null,
                is_tombstone: true,
                timestamp: strata_core::Timestamp::from_micros(555),
                ttl_ms: 10_000,
            },
        );
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        let e = seg.point_lookup(&kv_key("k1"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(42));
        assert_eq!(e.timestamp, 1_700_000_000_000_000);
        assert_eq!(e.ttl_ms, 30_000);

        let e = seg.point_lookup(&kv_key("k2"), u64::MAX).unwrap();
        assert!(e.is_tombstone);
        assert_eq!(e.timestamp, 555);
        assert_eq!(e.ttl_ms, 10_000);
    }

    #[test]
    fn iter_seek_returns_timestamp_and_ttl() {
        use crate::memtable::MemtableEntry;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2iter.sst");

        let mt = Memtable::new(0);
        mt.put_entry(
            &kv_key("item:1"),
            1,
            MemtableEntry {
                value: Value::Int(1),
                is_tombstone: false,
                timestamp: strata_core::Timestamp::from_micros(100_000),
                ttl_ms: 5_000,
            },
        );
        mt.put_entry(
            &kv_key("item:2"),
            2,
            MemtableEntry {
                value: Value::Int(2),
                is_tombstone: false,
                timestamp: strata_core::Timestamp::from_micros(200_000),
                ttl_ms: 0,
            },
        );
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let results: Vec<_> = seg.iter_seek(&kv_key("item:")).collect();
        assert_eq!(results.len(), 2);

        assert_eq!(results[0].1.timestamp, 100_000);
        assert_eq!(results[0].1.ttl_ms, 5_000);

        assert_eq!(results[1].1.timestamp, 200_000);
        assert_eq!(results[1].1.ttl_ms, 0);
    }

    #[test]
    fn compressed_and_uncompressed_coexist() {
        // Verify that segments with compressed blocks work end-to-end
        // Build with small block size to get multiple blocks
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.sst");

        let mt = Memtable::new(0);
        for i in 0..500u32 {
            let k = kv_key(&format!("k_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::String("x".repeat(50)), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.entry_count(), 500);
        // Spot check a few entries
        let e = seg.point_lookup(&kv_key("k_000000"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 1);
        let e = seg.point_lookup(&kv_key("k_000499"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 500);
    }

    #[test]
    fn compressed_segment_reduces_file_size() {
        // Build two segments with the same data: one with highly compressible data
        // and verify file size is reasonable (segment with repetitive data should compress)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compressible.sst");

        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = kv_key(&format!("key_{:06}", i));
            // Highly repetitive data that compresses well
            let val = Value::String("A".repeat(500));
            mt.put(&k, i as u64 + 1, val, false);
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 4096,
            bloom_bits_per_key: 10,
        };
        let meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();
        assert_eq!(meta.entry_count, 200);

        // Verify all data is readable
        let seg = KVSegment::open(&path).unwrap();
        for i in 0..200u32 {
            let k = kv_key(&format!("key_{:06}", i));
            let e = seg.point_lookup(&k, u64::MAX).unwrap();
            assert_eq!(e.value, Value::String("A".repeat(500)));
        }

        // The compressed file should be smaller than the uncompressed data payload
        // 200 entries * ~500 bytes each = ~100KB of value data alone
        // With compression, the file should be significantly smaller
        assert!(
            seg.file_size() < 100_000,
            "compressed segment should be much smaller than raw data; got {} bytes",
            seg.file_size(),
        );
    }
}
