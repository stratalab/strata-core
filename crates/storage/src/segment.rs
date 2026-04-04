//! KV segment reader — opens segment files for queries via pread + block cache.
//!
//! A `KVSegment` is an immutable sorted file produced by [`SegmentBuilder`].
//! It supports:
//!
//! - **Point lookups** via bloom filter + index block binary search + block scan
//! - **Prefix scans** via ordered iteration from a seek position
//! - **MVCC filtering** via per-entry commit_id
//!
//! ## I/O model
//!
//! Segment metadata (header, footer, index, filter index, properties) is loaded
//! into memory at open time via `pread`. Bloom filter partitions and data blocks
//! are read on demand via `pread` and cached in the global
//! [`BlockCache`](crate::block_cache). This avoids mmap page-fault overhead
//! and OS double-caching.
//!
//! [`SegmentBuilder`]: crate::segment_builder::SegmentBuilder

use crate::bloom::BloomFilter;
use crate::key_encoding::{
    encode_typed_key, encode_typed_key_prefix, InternalKey, COMMIT_ID_SUFFIX_LEN,
};
use crate::segment_builder::{
    decode_entry_header_ref_v4, decode_entry_header_v4, decode_entry_v4, decode_entry_v4_raw,
    decode_entry_value, parse_filter_index, parse_footer, parse_framed_block,
    parse_framed_block_raw, parse_header, parse_index_block, parse_properties_block, EntryHeader,
    FilterIndexEntry, IndexEntry, KVHeader, PropertiesBlock, FOOTER_SZ, FRAME_OVERHEAD,
    HEADER_SIZE, IDX_TYPE_PARTITIONED,
};
use strata_core::error::StrataError;
use strata_core::types::Key;
use strata_core::value::Value;

use std::cell::RefCell;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

// ── Segment block read profiling (STRATA_PROFILE_SEGMENT=1) ─────────────

struct SegmentProfile {
    total_reads: u64,
    cache_hits: u64,
    cache_misses: u64,
    pread_ns: u64,
    pread_bytes: u64,
}

thread_local! {
    static SEG_PROF: RefCell<SegmentProfile> = const { RefCell::new(SegmentProfile {
        total_reads: 0, cache_hits: 0, cache_misses: 0,
        pread_ns: 0, pread_bytes: 0,
    }) };
}

struct BlockScanProfile {
    count: u64,
    read_block_ns: u64,
    scan_ns: u64,
    blocks_per_lookup: u64,
    lookups: u64,
}

struct LookupProfile {
    count: u64,
    bloom_ns: u64,
    index_and_block_ns: u64,
    partitioned_count: u64,
    top_search_ns: u64,
    sub_lookup_ns: u64,
}

thread_local! {
    static LOOKUP_PROF: RefCell<LookupProfile> = const { RefCell::new(LookupProfile {
        count: 0, bloom_ns: 0, index_and_block_ns: 0, partitioned_count: 0,
        top_search_ns: 0, sub_lookup_ns: 0,
    }) };
}

thread_local! {
    static BLOCK_SCAN_PROF: RefCell<BlockScanProfile> = const { RefCell::new(BlockScanProfile {
        count: 0, read_block_ns: 0, scan_ns: 0, blocks_per_lookup: 0, lookups: 0,
    }) };
}

/// Shorthand for `io::Error::new(io::ErrorKind::InvalidData, ...)`.
macro_rules! invalid_data {
    ($msg:expr) => {
        io::Error::new(io::ErrorKind::InvalidData, $msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        io::Error::new(io::ErrorKind::InvalidData, format!($fmt, $($arg)*))
    };
}

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
    /// Pre-encoded bincode value bytes for zero-copy compaction passthrough.
    /// When set, `encode_entry_v4` uses these directly instead of
    /// re-serializing `value`, eliminating the deserialize→serialize round-trip.
    pub(crate) raw_value: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// KVSegment
// ---------------------------------------------------------------------------

/// Partitioned bloom filter with partition data pinned in memory.
///
/// The filter index maps key ranges to partitions. Each partition's raw
/// bytes are loaded into memory at segment open time — no block cache
/// lookup or pread on the read hot path.
struct PartitionedBloom {
    /// Top-level index mapping key ranges → partition offsets.
    index: Vec<FilterIndexEntry>,
    /// Raw bytes of each bloom partition, parallel to `index`.
    /// Used directly by `maybe_contains_raw` (zero-copy).
    partitions: Vec<Arc<Vec<u8>>>,
}

/// Cache-friendly flat index: all keys packed in a contiguous buffer.
///
/// Replaces `Vec<IndexEntry>` where each `IndexEntry.key` was a separate
/// heap allocation. Binary search over 25K entries caused ~15 L2 cache
/// misses at ~100ns each = 1.5-1.9us. With FlatIndex, all key data is
/// in one `Vec<u8>` and offsets in one `Vec<u32>` — binary search touches
/// contiguous memory with good cache locality.
#[derive(Debug)]
struct FlatIndex {
    /// All keys packed contiguously: key_0 || key_1 || ... || key_n
    key_data: Vec<u8>,
    /// Byte offset into key_data where each key starts.
    /// Length = num_entries + 1 (sentinel at end = key_data.len())
    key_offsets: Vec<u32>,
    /// Block file offset for each entry.
    block_offsets: Vec<u64>,
    /// Block data length for each entry.
    block_data_lens: Vec<u32>,
}

impl FlatIndex {
    /// Parse an on-disk index block directly into a FlatIndex.
    ///
    /// Same wire format as `parse_index_block`:
    /// `[count:u32] [key_len:u32 | key | offset:u64 | data_len:u32]*`
    fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        let count = u32::from_le_bytes(data[..4].try_into().ok()?) as usize;
        let mut key_data = Vec::new();
        let mut key_offsets = Vec::with_capacity(count + 1);
        let mut block_offsets = Vec::with_capacity(count);
        let mut block_data_lens = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 4 > data.len() {
                return None;
            }
            let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + key_len + 8 + 4 > data.len() {
                return None;
            }
            key_offsets.push(key_data.len() as u32);
            key_data.extend_from_slice(&data[pos..pos + key_len]);
            pos += key_len;
            block_offsets.push(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            pos += 8;
            block_data_lens.push(u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?));
            pos += 4;
        }
        key_offsets.push(key_data.len() as u32); // sentinel

        Some(FlatIndex {
            key_data,
            key_offsets,
            block_offsets,
            block_data_lens,
        })
    }

    /// Convert from parsed IndexEntry slice (for backward compatibility).
    fn from_entries(entries: &[IndexEntry]) -> Self {
        let mut key_data = Vec::new();
        let mut key_offsets = Vec::with_capacity(entries.len() + 1);
        let mut block_offsets = Vec::with_capacity(entries.len());
        let mut block_data_lens = Vec::with_capacity(entries.len());

        for e in entries {
            key_offsets.push(key_data.len() as u32);
            key_data.extend_from_slice(&e.key);
            block_offsets.push(e.block_offset);
            block_data_lens.push(e.block_data_len);
        }
        key_offsets.push(key_data.len() as u32);

        FlatIndex {
            key_data,
            key_offsets,
            block_offsets,
            block_data_lens,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.block_offsets.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.block_offsets.is_empty()
    }

    /// Get the key bytes for entry `i`.
    #[inline]
    fn key(&self, i: usize) -> &[u8] {
        let start = self.key_offsets[i] as usize;
        let end = self.key_offsets[i + 1] as usize;
        &self.key_data[start..end]
    }

    #[inline]
    fn block_offset(&self, i: usize) -> u64 {
        self.block_offsets[i]
    }

    #[inline]
    fn block_data_len(&self, i: usize) -> u32 {
        self.block_data_lens[i]
    }

    /// Binary search for `seek_bytes`. Returns the block index to start from.
    ///
    /// Finds the last entry whose key is <= seek_bytes. Equivalent to the old
    /// `entries.binary_search_by(|e| e.key.cmp(seek_bytes))` with Err(0)→0, Err(i)→i-1.
    fn search(&self, seek_bytes: &[u8]) -> usize {
        let n = self.len();
        if n == 0 {
            return 0;
        }
        // partition_point returns the first index where key > seek_bytes.
        // We want the last index where key <= seek_bytes = partition_point - 1.
        let pp = self.partition_point(seek_bytes);
        if pp == 0 { 0 } else { pp - 1 }
    }

    /// Returns the first index where key > seek_bytes (like slice::partition_point).
    #[inline]
    fn partition_point(&self, seek_bytes: &[u8]) -> usize {
        let mut lo = 0usize;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.key(mid) <= seek_bytes {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Total heap bytes used by this index.
    fn heap_bytes(&self) -> usize {
        self.key_data.capacity()
            + self.key_offsets.capacity() * std::mem::size_of::<u32>()
            + self.block_offsets.capacity() * std::mem::size_of::<u64>()
            + self.block_data_lens.capacity() * std::mem::size_of::<u32>()
    }
}

/// Two-level or monolithic index for a segment.
enum SegmentIndex {
    /// All index entries loaded in memory (small segments or legacy format).
    Monolithic(FlatIndex),
    /// Two-level partitioned index with all sub-indexes pinned in memory.
    /// No block cache lookup or pread on the read hot path.
    Partitioned {
        top_level: FlatIndex,
        /// Pre-parsed sub-index entries, parallel to `top_level`.
        sub_indexes: Vec<FlatIndex>,
    },
}

impl SegmentIndex {
    /// Return the sub-index for a given partition, or `None` if out of bounds.
    fn partition(&self, idx: usize) -> Option<&FlatIndex> {
        match self {
            SegmentIndex::Monolithic(fi) => {
                if idx == 0 {
                    Some(fi)
                } else {
                    None
                }
            }
            SegmentIndex::Partitioned { sub_indexes, .. } => sub_indexes.get(idx),
        }
    }

    /// Whether the index is empty (no entries at all).
    fn is_empty(&self) -> bool {
        match self {
            SegmentIndex::Monolithic(fi) => fi.is_empty(),
            SegmentIndex::Partitioned { sub_indexes, .. } => sub_indexes.is_empty(),
        }
    }

    /// Find the starting partition and block for a seek key.
    ///
    /// Returns `(partition_idx, block_within_partition)`.
    fn seek_position(&self, seek_bytes: &[u8]) -> (usize, usize) {
        match self {
            SegmentIndex::Monolithic(fi) => (0, fi.search(seek_bytes)),
            SegmentIndex::Partitioned {
                top_level,
                sub_indexes,
            } => {
                let part = top_level.search(seek_bytes);
                let block_in = sub_indexes[part].search(seek_bytes);
                (part, block_in)
            }
        }
    }
}

/// An immutable KV segment file backed by pread + block cache.
///
/// Opened via [`KVSegment::open`], provides point lookups and prefix iteration
/// with MVCC snapshot filtering.
pub struct KVSegment {
    /// Open file handle for pread I/O.
    file: std::fs::File,
    header: KVHeader,
    index: SegmentIndex,
    bloom: PartitionedBloom,
    props: PropertiesBlock,
    /// Path to the .sst file (for cleanup after compaction).
    file_path: std::path::PathBuf,
    /// Hash of file_path for block cache keying.
    file_id: u64,
    /// File size in bytes.
    file_size: u64,
}

/// Read exactly `len` bytes at `offset` from a file via pread.
fn pread_exact(file: &std::fs::File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

impl KVSegment {
    /// Open and parse a KV segment file.
    ///
    /// Reads metadata (header, footer, index, filter index, properties) into
    /// memory at open time via pread. Bloom partitions and data blocks are
    /// read on demand through the block cache.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < (HEADER_SIZE + FOOTER_SZ) as u64 {
            return Err(invalid_data!("segment file too small"));
        }

        // Parse header via pread
        let header_buf = pread_exact(&file, 0, HEADER_SIZE)?;
        let header_bytes: &[u8; HEADER_SIZE] = header_buf
            .as_slice()
            .try_into()
            .map_err(|_| invalid_data!("header size mismatch"))?;
        let header =
            parse_header(header_bytes).ok_or_else(|| invalid_data!("invalid segment header"))?;

        // Parse footer via pread (last FOOTER_SZ bytes)
        let footer_offset = file_size - FOOTER_SZ as u64;
        let footer_buf = pread_exact(&file, footer_offset, FOOTER_SZ)?;
        let footer_bytes: &[u8; FOOTER_SZ] = footer_buf
            .as_slice()
            .try_into()
            .map_err(|_| invalid_data!("footer size mismatch"))?;
        let footer = parse_footer(footer_bytes)
            .ok_or_else(|| invalid_data!("invalid segment footer (bad magic)"))?;

        // Helper: validate block offset + length fits within the file.
        let check_block_bounds = |offset: u64, len: u32, name: &str| -> io::Result<(u64, usize)> {
            let end = offset
                .checked_add(len as u64)
                .ok_or_else(|| invalid_data!("{} offset+len overflows", name))?;
            if end > file_size {
                return Err(invalid_data!("{} extends past end of file", name));
            }
            Ok((offset, len as usize))
        };

        // Parse index block via pread
        let (idx_off, idx_len) = check_block_bounds(
            footer.index_block_offset,
            footer.index_block_len,
            "index block",
        )?;
        let idx_buf = pread_exact(&file, idx_off, idx_len)?;
        let (_, idx_data) = parse_framed_block(&idx_buf)
            .ok_or_else(|| invalid_data!("index block CRC mismatch"))?;
        let index_entries =
            parse_index_block(idx_data).ok_or_else(|| invalid_data!("malformed index block"))?;
        let index = if footer.index_type == IDX_TYPE_PARTITIONED {
            // Eagerly load all sub-index partitions into memory as FlatIndex.
            let mut sub_indexes = Vec::with_capacity(index_entries.len());
            for entry in &index_entries {
                let raw = pread_exact(&file, entry.block_offset, entry.block_data_len as usize)?;
                let (_, data) = parse_framed_block(&raw)
                    .ok_or_else(|| invalid_data!("sub-index partition CRC mismatch"))?;
                let sub = FlatIndex::parse(data)
                    .ok_or_else(|| invalid_data!("malformed sub-index partition"))?;
                sub_indexes.push(sub);
            }
            SegmentIndex::Partitioned {
                top_level: FlatIndex::from_entries(&index_entries),
                sub_indexes,
            }
        } else {
            SegmentIndex::Monolithic(
                FlatIndex::parse(idx_data)
                    .ok_or_else(|| invalid_data!("malformed index block (flat)"))?
            )
        };

        // Parse filter index block via pread (v5 partitioned bloom)
        let (fi_off, fi_len) = check_block_bounds(
            footer.filter_block_offset,
            footer.filter_block_len,
            "filter index block",
        )?;
        let fi_buf = pread_exact(&file, fi_off, fi_len)?;
        let (_, fi_data) = parse_framed_block(&fi_buf)
            .ok_or_else(|| invalid_data!("filter index block CRC mismatch"))?;
        let filter_index =
            parse_filter_index(fi_data).ok_or_else(|| invalid_data!("malformed filter index"))?;

        // Eagerly load all bloom partition data into memory.
        let mut bloom_partitions = Vec::with_capacity(filter_index.len());
        for entry in &filter_index {
            let raw = pread_exact(&file, entry.block_offset, entry.block_data_len as usize)?;
            let (_, data) = parse_framed_block(&raw)
                .ok_or_else(|| invalid_data!("bloom partition CRC mismatch"))?;
            bloom_partitions.push(Arc::new(data.to_vec()));
        }
        let bloom = PartitionedBloom {
            index: filter_index,
            partitions: bloom_partitions,
        };

        // Parse properties block via pread
        let (props_off, props_len) = check_block_bounds(
            footer.props_block_offset,
            footer.props_block_len,
            "properties block",
        )?;
        let props_buf = pread_exact(&file, props_off, props_len)?;
        let (_, props_data) = parse_framed_block(&props_buf)
            .ok_or_else(|| invalid_data!("properties block CRC mismatch"))?;
        let props = parse_properties_block(props_data)
            .ok_or_else(|| invalid_data!("malformed properties block"))?;

        let file_path = path.to_path_buf();
        let file_id = crate::block_cache::file_path_hash(&file_path);
        Ok(Self {
            file,
            header,
            index,
            bloom,
            props,
            file_path,
            file_id,
            file_size,
        })
    }

    /// Check the bloom filter for a key (without commit_id).
    ///
    /// Returns `false` if the key definitely does not exist in this segment.
    pub fn bloom_maybe_contains(&self, key: &Key) -> bool {
        let typed = encode_typed_key(key);
        self.partitioned_bloom_check(&typed)
    }

    /// Point lookup: find the newest version of `key` with commit_id ≤ `snapshot_commit`.
    ///
    /// Returns `None` if:
    /// - Bloom filter says key is absent
    /// - No matching entry exists at or below the snapshot
    pub fn point_lookup(
        &self,
        key: &Key,
        snapshot_commit: u64,
    ) -> Result<Option<SegmentEntry>, StrataError> {
        let typed_key = encode_typed_key(key);
        let seek_ik = InternalKey::encode(key, u64::MAX);
        self.point_lookup_preencoded(&typed_key, seek_ik.as_bytes(), snapshot_commit)
    }

    /// Point lookup using pre-encoded key bytes. Avoids redundant key encoding
    /// when the caller already has the typed key and seek bytes.
    pub fn point_lookup_preencoded(
        &self,
        typed_key: &[u8],
        seek_bytes: &[u8],
        snapshot_commit: u64,
    ) -> Result<Option<SegmentEntry>, StrataError> {
        let profiling = Self::segment_profile_enabled();

        // 1. Bloom check
        let t0 = if profiling { Some(std::time::Instant::now()) } else { None };
        if !self.partitioned_bloom_check(typed_key) {
            return Ok(None);
        }
        let bloom_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        // 2. Index lookup + block scan
        let t1 = if profiling { Some(std::time::Instant::now()) } else { None };
        let result = match &self.index {
            SegmentIndex::Monolithic(fi) => {
                self.point_lookup_with_flat_index(fi, typed_key, seek_bytes, snapshot_commit)
            }
            SegmentIndex::Partitioned {
                top_level,
                sub_indexes,
            } => {
                let tt = if profiling { Some(std::time::Instant::now()) } else { None };
                let part_idx = top_level.search(seek_bytes);
                let top_ns = tt.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

                let ts = if profiling { Some(std::time::Instant::now()) } else { None };
                let mut found = None;
                for pi in part_idx..top_level.len() {
                    if pi > part_idx {
                        let prev_key = top_level.key(pi - 1);
                        if prev_key.len() >= COMMIT_ID_SUFFIX_LEN {
                            let prefix = &prev_key[..prev_key.len() - COMMIT_ID_SUFFIX_LEN];
                            if prefix > typed_key {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    if let Some(entry) = self.point_lookup_with_flat_index(
                        &sub_indexes[pi],
                        typed_key,
                        seek_bytes,
                        snapshot_commit,
                    )? {
                        found = Some(entry);
                        break;
                    }
                }
                let sub_ns = ts.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

                if profiling {
                    LOOKUP_PROF.with(|p| {
                        let mut p = p.borrow_mut();
                        p.top_search_ns += top_ns;
                        p.sub_lookup_ns += sub_ns;
                    });
                }
                Ok(found)
            }
        };
        let index_and_block_ns = t1.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        if profiling {
            LOOKUP_PROF.with(|p| {
                let mut p = p.borrow_mut();
                p.count += 1;
                p.bloom_ns += bloom_ns;
                p.index_and_block_ns += index_and_block_ns;
                // Track index type
                if matches!(&self.index, SegmentIndex::Partitioned { .. }) {
                    p.partitioned_count += 1;
                }
                if p.count % 100_000 == 0 {
                    let n = 100_000f64;
                    eprintln!(
                        "[lookup-prof] {} lookups | avg(us): bloom={:.2} top={:.2} sub+block={:.2} total={:.2}",
                        p.count,
                        p.bloom_ns as f64 / n / 1000.0,
                        p.top_search_ns as f64 / n / 1000.0,
                        p.sub_lookup_ns as f64 / n / 1000.0,
                        (p.bloom_ns + p.index_and_block_ns) as f64 / n / 1000.0,
                    );
                    p.bloom_ns = 0;
                    p.index_and_block_ns = 0;
                    p.partitioned_count = 0;
                    p.top_search_ns = 0;
                    p.sub_lookup_ns = 0;
                }
            });
        }

        result.map(|opt| opt)
    }

    // ── Segment read-path profiling (STRATA_PROFILE_SEGMENT=1) ──────────

    fn segment_profile_enabled() -> bool {
        use std::sync::atomic::{AtomicBool, Ordering};
        static ENABLED: AtomicBool = AtomicBool::new(false);
        static CHECKED: AtomicBool = AtomicBool::new(false);
        if !CHECKED.load(Ordering::Relaxed) {
            ENABLED.store(std::env::var("STRATA_PROFILE_SEGMENT").is_ok(), Ordering::Relaxed);
            CHECKED.store(true, Ordering::Relaxed);
        }
        ENABLED.load(Ordering::Relaxed)
    }

    fn maybe_print_seg_profile(p: &mut SegmentProfile) {
        if p.total_reads > 0 && p.total_reads % 100_000 == 0 {
            let n = 100_000f64;
            let hit_rate = p.cache_hits as f64 / n * 100.0;
            let miss_rate = p.cache_misses as f64 / n * 100.0;
            let avg_pread_us = if p.cache_misses > 0 {
                p.pread_ns as f64 / p.cache_misses as f64 / 1000.0
            } else {
                0.0
            };
            eprintln!(
                "[seg-profile] {} block reads | cache hit={:.1}% miss={:.1}% | \
                 avg pread={:.1}us ({} bytes/miss)",
                p.total_reads,
                hit_rate,
                miss_rate,
                avg_pread_us,
                if p.cache_misses > 0 { p.pread_bytes / p.cache_misses } else { 0 },
            );
            p.cache_hits = 0;
            p.cache_misses = 0;
            p.pread_ns = 0;
            p.pread_bytes = 0;
        }
    }

    /// Point lookup using FlatIndex (cache-friendly contiguous keys).
    fn point_lookup_with_flat_index(
        &self,
        index: &FlatIndex,
        typed_key: &[u8],
        seek_bytes: &[u8],
        snapshot_commit: u64,
    ) -> Result<Option<SegmentEntry>, StrataError> {
        let profiling = Self::segment_profile_enabled();
        let ts = if profiling { Some(std::time::Instant::now()) } else { None };
        let block_idx = index.search(seek_bytes);
        let search_ns = ts.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        let mut blocks_scanned = 0u32;
        for bi in block_idx..index.len() {
            if bi > block_idx {
                let prev_key = index.key(bi - 1);
                if prev_key.len() >= COMMIT_ID_SUFFIX_LEN {
                    let prefix = &prev_key[..prev_key.len() - COMMIT_ID_SUFFIX_LEN];
                    if prefix > typed_key {
                        break;
                    }
                } else {
                    break;
                }
            }

            blocks_scanned += 1;
            let ie = IndexEntry {
                key: Vec::new(),
                block_offset: index.block_offset(bi),
                block_data_len: index.block_data_len(bi),
            };
            if let Some(entry) = self.scan_block_for_key(&ie, typed_key, snapshot_commit)? {
                if profiling {
                    BLOCK_SCAN_PROF.with(|p| {
                        let mut p = p.borrow_mut();
                        p.blocks_per_lookup += blocks_scanned as u64;
                        p.lookups += 1;
                    });
                }
                return Ok(Some(entry));
            }
        }
        if profiling {
            BLOCK_SCAN_PROF.with(|p| {
                let mut p = p.borrow_mut();
                p.blocks_per_lookup += blocks_scanned as u64;
                p.lookups += 1;
            });
            LOOKUP_PROF.with(|p| {
                let mut p = p.borrow_mut();
                p.top_search_ns += search_ns; // reuse field for sub-index search timing
            });
        }
        Ok(None)
    }

    /// Iterate entries starting from `prefix`, yielding `(InternalKey, SegmentEntry)` pairs.
    ///
    /// Entries are in InternalKey order. The caller is responsible for MVCC dedup.
    /// Iteration stops when entries no longer match the prefix.
    pub fn iter_seek<'a>(&'a self, prefix: &Key) -> SegmentIter<'a> {
        let prefix_bytes = encode_typed_key_prefix(prefix);
        let done = self.index.is_empty();
        let (partition_idx, block_within_partition) = if done {
            (0, 0)
        } else {
            let seek_ik = InternalKey::encode(prefix, u64::MAX);
            self.index.seek_position(seek_ik.as_bytes())
        };

        SegmentIter {
            segment: self,
            prefix_bytes,
            partition_idx,
            block_within_partition,
            block_offset: 0,
            block_data_end: 0,
            block_data: None,
            done,
            prev_key: Vec::new(),
            corruption_detected: false,
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

    /// Key range of this segment: `(key_min, key_max)` from the properties block.
    ///
    /// These are full `InternalKey` bytes (typed_key_prefix + commit_id).
    /// Returns `(&[], &[])` for empty segments.
    pub fn key_range(&self) -> (&[u8], &[u8]) {
        (&self.props.key_min, &self.props.key_max)
    }

    /// Binary search the segment index for the block containing `seek_bytes`.
    ///
    /// Returns `(partition_idx, block_within_partition)` for direct positioning.
    /// Used by [`SeekableIterator`](crate::seekable::SeekableIterator) implementations.
    pub fn index_seek_position(&self, seek_bytes: &[u8]) -> (usize, usize) {
        self.index.seek_position(seek_bytes)
    }

    /// Whether this segment's index is empty (no blocks).
    pub fn index_is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// File size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// File identity hash (for block cache keying and invalidation).
    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    /// Path to the .sst file on disk.
    pub fn file_path(&self) -> &std::path::Path {
        &self.file_path
    }

    /// Approximate heap bytes used by eagerly-loaded segment metadata
    /// (bloom filter partitions + index blocks). These are NOT tracked by
    /// the block cache and contribute to RSS outside any configured budget.
    pub fn metadata_bytes(&self) -> u64 {
        let bloom: u64 = self
            .bloom
            .partitions
            .iter()
            .map(|p| p.len() as u64 + std::mem::size_of::<Arc<Vec<u8>>>() as u64)
            .sum::<u64>()
            + self
                .bloom
                .index
                .iter()
                .map(|e| e.max_key.len() as u64 + std::mem::size_of::<FilterIndexEntry>() as u64)
                .sum::<u64>();

        let index: u64 = match &self.index {
            SegmentIndex::Monolithic(fi) => fi.heap_bytes() as u64,
            SegmentIndex::Partitioned {
                top_level,
                sub_indexes,
            } => {
                let top = top_level.heap_bytes() as u64;
                let subs: u64 = sub_indexes.iter().map(|fi| fi.heap_bytes() as u64).sum();
                top + subs
            }
        };

        bloom + index
    }

    /// Format version from the segment header.
    #[cfg(test)]
    pub(crate) fn format_version(&self) -> u16 {
        self.header.format_version
    }

    /// Number of entries in the top-level index (or monolithic index).
    #[cfg(test)]
    fn index_entry_count(&self) -> usize {
        match &self.index {
            SegmentIndex::Monolithic(fi) => fi.len(),
            SegmentIndex::Partitioned { top_level, .. } => top_level.len(),
        }
    }

    /// Whether this segment uses a partitioned (two-level) index.
    #[cfg(test)]
    fn is_partitioned_index(&self) -> bool {
        matches!(&self.index, SegmentIndex::Partitioned { .. })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Check the partitioned bloom filter for a typed key.
    ///
    /// Binary searches the filter index, loads the relevant partition from
    /// the block cache (pread on miss), and checks the bloom filter.
    /// Returns `true` on I/O error or malformed data (safe default).
    fn partitioned_bloom_check(&self, typed_key: &[u8]) -> bool {
        if self.bloom.index.is_empty() {
            return true; // no bloom info → assume key might exist
        }

        // Binary search for the partition covering this key
        let idx = self
            .bloom
            .index
            .partition_point(|e| e.max_key.as_slice() < typed_key);
        if idx >= self.bloom.index.len() {
            return false; // key is beyond all partitions
        }

        // Bloom data is pinned in memory — no cache lookup, no pread.
        let data = &self.bloom.partitions[idx];
        BloomFilter::maybe_contains_raw(data, typed_key).unwrap_or(true)
    }

    /// Read and verify a data block, checking the global block cache first.
    ///
    /// On cache hit, returns the cached decompressed block (zero decompression cost).
    /// On cache miss, reads from file via pread, decompresses if needed, caches, and returns.
    fn read_data_block(&self, ie: &IndexEntry) -> Result<Arc<Vec<u8>>, StrataError> {
        let cache = crate::block_cache::global_cache();
        let block_offset = ie.block_offset;
        let profiling = Self::segment_profile_enabled();

        // Check cache first
        if let Some(cached) = cache.get(self.file_id, block_offset) {
            if profiling {
                SEG_PROF.with(|p| {
                    let mut p = p.borrow_mut();
                    p.cache_hits += 1;
                    p.total_reads += 1;
                    Self::maybe_print_seg_profile(&mut p);
                });
            }
            return Ok(cached);
        }

        // Cache miss: pread from file and decompress
        let t_pread = if profiling { Some(std::time::Instant::now()) } else { None };
        let framed_len = FRAME_OVERHEAD + ie.block_data_len as usize;
        let raw = pread_exact(&self.file, block_offset, framed_len).map_err(|e| {
            StrataError::corruption(format!(
                "pread failed reading data block at offset {} in {:?}: {}",
                block_offset, self.file_path, e
            ))
        })?;

        let (_block_type, codec_byte, reserved, data, stored_crc) = parse_framed_block_raw(&raw)
            .ok_or_else(|| {
                StrataError::corruption(format!(
                    "data block truncation at offset {} in {:?}",
                    block_offset, self.file_path
                ))
            })?;

        let pre_compression_crc = reserved & 1 == 1;

        // For legacy blocks (reserved bit 0 = 0), verify CRC on compressed data
        if !pre_compression_crc {
            let computed_crc = crc32fast::hash(data);
            if stored_crc != computed_crc {
                return Err(StrataError::corruption(format!(
                    "data block CRC mismatch at offset {} in {:?}",
                    block_offset, self.file_path
                )));
            }
        }

        let decompressed = match codec_byte {
            0 => data.to_vec(), // Uncompressed
            1 => zstd::decode_all(data).map_err(|e| {
                StrataError::corruption(format!(
                    "zstd decompression failed at offset {} in {:?}: {}",
                    block_offset, self.file_path, e
                ))
            })?,
            other => {
                return Err(StrataError::corruption(format!(
                    "unknown compression codec {} at offset {} in {:?}",
                    other, block_offset, self.file_path
                )));
            }
        };

        // For new blocks (reserved bit 0 = 1), verify CRC on uncompressed data.
        // This ensures the crc32fast buffer is always >= data_block_size (~4KB),
        // above the 64-byte threshold for PCLMULQDQ hardware acceleration.
        if pre_compression_crc {
            let computed_crc = crc32fast::hash(&decompressed);
            if stored_crc != computed_crc {
                return Err(StrataError::corruption(format!(
                    "data block CRC mismatch (pre-compression) at offset {} in {:?}",
                    block_offset, self.file_path
                )));
            }
        }

        // Cache the decompressed block
        if profiling {
            let pread_ns = t_pread.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
            SEG_PROF.with(|p| {
                let mut p = p.borrow_mut();
                p.cache_misses += 1;
                p.total_reads += 1;
                p.pread_ns += pread_ns;
                p.pread_bytes += framed_len as u64;
                Self::maybe_print_seg_profile(&mut p);
            });
        }
        Ok(cache.insert(self.file_id, block_offset, decompressed))
    }

    /// Scan a single data block for the newest version of a typed key at or below snapshot.
    ///
    /// Uses zero-copy two-phase decoding:
    /// 1. Parse key bytes + metadata WITHOUT allocating (EntryHeaderRef)
    /// 2. Only allocate + deserialize value for the matching entry
    ///
    /// For v3 blocks, binary-searches the restart point array to skip most
    /// entries, then linear-scans only the ~16-entry interval.
    fn scan_block_for_key(
        &self,
        ie: &IndexEntry,
        typed_key: &[u8],
        snapshot_commit: u64,
    ) -> Result<Option<SegmentEntry>, StrataError> {
        let profiling = Self::segment_profile_enabled();

        let t0 = if profiling { Some(std::time::Instant::now()) } else { None };
        let block_data = self.read_data_block(ie)?;
        let read_block_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        let t0 = if profiling { Some(std::time::Instant::now()) } else { None };
        let data = &**block_data;

        // Strip hash index if present (v6+)
        let (block, hash_index) = strip_hash_index(data);

        // Determine scan bounds via restart trailer
        let (scan_start, data_end) = if let Some((de, num_restarts)) = parse_restart_trailer(block)
        {
            let start = if let Some(ref hi) = hash_index {
                // O(1) hash lookup for the restart interval
                let h = xxhash_rust::xxh3::xxh3_64(typed_key) as usize;
                let bucket = h % hi.num_buckets;
                let interval = hi.buckets[bucket];
                if interval != 0xFF && (interval as usize) < num_restarts {
                    restart_offset_at(block, de, interval as usize)
                        .map(|o| o as usize)
                        .unwrap_or_else(|| {
                            binary_search_restarts(block, de, num_restarts, typed_key)
                        })
                } else {
                    // Empty bucket or out of range: fall back to binary search
                    binary_search_restarts(block, de, num_restarts, typed_key)
                }
            } else {
                binary_search_restarts(block, de, num_restarts, typed_key)
            };
            (start.min(de), de)
        } else {
            // Malformed trailer — fall back to full linear scan
            (0, block.len())
        };

        let result = self.linear_scan_block_v4(data, scan_start, data_end, typed_key, snapshot_commit);
        let scan_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        if profiling {
            BLOCK_SCAN_PROF.with(|p| {
                let mut p = p.borrow_mut();
                p.count += 1;
                p.read_block_ns += read_block_ns;
                p.scan_ns += scan_ns;
                if p.count % 100_000 == 0 {
                    let n = 100_000f64;
                    let bpl = if p.lookups > 0 { p.blocks_per_lookup as f64 / p.lookups as f64 } else { 0.0 };
                    eprintln!(
                        "[block-scan] {} scans | avg(us): read_block={:.2} scan={:.2} total={:.2} | blocks/lookup={:.2}",
                        p.count,
                        p.read_block_ns as f64 / n / 1000.0,
                        p.scan_ns as f64 / n / 1000.0,
                        (p.read_block_ns + p.scan_ns) as f64 / n / 1000.0,
                        bpl,
                    );
                    p.read_block_ns = 0;
                    p.scan_ns = 0;
                    p.blocks_per_lookup = 0;
                    p.lookups = 0;
                }
            });
        }

        Ok(result)
    }

    /// Linear scan entries in `data[start..end]` for `typed_key` at or below `snapshot_commit`.
    ///
    /// v4 format: prefix-compressed keys. Reconstructs keys using `prev_key` buffer.
    fn linear_scan_block_v4(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        typed_key: &[u8],
        snapshot_commit: u64,
    ) -> Option<SegmentEntry> {
        let mut pos = start;
        let mut prev_key: Vec<u8> = Vec::new();
        while pos < end {
            let entry_data = &data[pos..end];
            let hdr = decode_entry_header_v4(entry_data, &mut prev_key)?;
            let entry_start = pos;
            pos += hdr.total_len;

            // prev_key now holds the full reconstructed InternalKey bytes
            if prev_key.len() < COMMIT_ID_SUFFIX_LEN {
                return None;
            }
            let tkp = &prev_key[..prev_key.len() - COMMIT_ID_SUFFIX_LEN];
            if tkp != typed_key {
                if tkp > typed_key {
                    break;
                }
                continue;
            }

            let len = prev_key.len();
            let commit_id = !u64::from_be_bytes(prev_key[len - 8..].try_into().ok()?);
            if commit_id <= snapshot_commit {
                let header = EntryHeader {
                    is_tombstone: hdr.is_tombstone,
                    timestamp: hdr.timestamp,
                    ttl_ms: hdr.ttl_ms,
                    value_start: hdr.value_start,
                    value_len: hdr.value_len,
                };
                let value = decode_entry_value(&data[entry_start..end], &header)?;
                return Some(SegmentEntry {
                    value,
                    is_tombstone: header.is_tombstone,
                    commit_id,
                    timestamp: header.timestamp,
                    ttl_ms: header.ttl_ms,
                    raw_value: None,
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
            partition_idx: 0,
            block_within_partition: 0,
            block_offset: 0,
            block_data_end: 0,
            block_data: None,
            done: self.index.is_empty(),
            prev_key: Vec::new(),
            corruption_detected: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Restart point helpers (v3 format)
// ---------------------------------------------------------------------------

/// Parse the restart trailer from a decompressed data block.
///
/// Returns `(data_end, num_restarts)` where `data_end` is the byte offset
/// where entry data ends (i.e. the start of the restart array).
pub(crate) fn parse_restart_trailer(data: &[u8]) -> Option<(usize, usize)> {
    if data.len() < 4 {
        return None;
    }
    let num = u32::from_le_bytes(data[data.len() - 4..].try_into().ok()?) as usize;
    if num == 0 || num > data.len() / 4 {
        return None; // 0 restarts invalid; num > data.len()/4 can't fit
    }
    let trailer_size = (1 + num) * 4; // num_restarts offsets + count
    if trailer_size > data.len() {
        return None;
    }
    Some((data.len() - trailer_size, num))
}

/// Read the i-th restart offset from the trailer. Returns `None` on corrupt data.
#[inline]
fn restart_offset_at(data: &[u8], data_end: usize, index: usize) -> Option<u32> {
    let off = data_end + index * 4;
    let bytes: [u8; 4] = data.get(off..off + 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

/// Binary search restart points to find the interval containing `typed_key`.
///
/// Returns the byte offset to start linear scanning from. Matches the
/// LevelDB `Block::Iter::Seek` pattern: find the last restart whose key
/// is ≤ the target, then linear scan from there.
fn binary_search_restarts(
    data: &[u8],
    data_end: usize,
    num_restarts: usize,
    typed_key: &[u8],
) -> usize {
    let mut left = 0usize;
    let mut right = num_restarts - 1;

    while left < right {
        let mid = (left + right + 1) / 2;
        let offset = match restart_offset_at(data, data_end, mid) {
            Some(o) => o as usize,
            None => {
                right = mid - 1;
                continue;
            }
        };
        if offset < data_end {
            // At restart points, shared=0 always, so key is self-contained.
            let cmp = decode_entry_header_ref_v4(&data[offset..data_end])
                .map(|hdr| hdr.typed_key_prefix() < typed_key);
            match cmp {
                Some(true) => left = mid,
                Some(false) => right = mid - 1,
                None => right = mid - 1, // Corrupt — be conservative
            }
        } else {
            // Corrupt offset — skip this restart point
            right = mid - 1;
        }
    }

    let start = restart_offset_at(data, data_end, left)
        .map(|o| o as usize)
        .unwrap_or(data_end);
    // Clamp to data_end so the caller's linear scan safely produces no results
    start.min(data_end)
}

/// Hash index parsed from the end of a data block (v6+).
struct HashIndex<'a> {
    buckets: &'a [u8],
    num_buckets: usize,
}

/// Strip hash index from the end of block data if present.
///
/// Returns the data without hash index and the hash index (if any).
/// The hash index sentinel is `0x01` as the last byte of the decompressed block.
fn strip_hash_index(data: &[u8]) -> (&[u8], Option<HashIndex<'_>>) {
    // Minimum size: 1 bucket + 2 (num_buckets) + 1 (sentinel) = 4
    // Plus at least 4 bytes for the restart trailer before it.
    if data.len() < 8 || data[data.len() - 1] != 0x01 {
        return (data, None);
    }
    let num_buckets = match data[data.len() - 3..data.len() - 1].try_into() {
        Ok(bytes) => u16::from_le_bytes(bytes) as usize,
        Err(_) => return (data, None),
    };
    if num_buckets == 0 {
        return (data, None);
    }
    let hash_size = num_buckets + 2 + 1; // buckets + u16 + sentinel
    if hash_size + 4 > data.len() {
        // need at least 4 bytes for restart trailer
        return (data, None);
    }
    let hash_start = data.len() - hash_size;
    let hi = HashIndex {
        buckets: &data[hash_start..hash_start + num_buckets],
        num_buckets,
    };
    (&data[..hash_start], Some(hi))
}

/// Compute `data_end` for a block — strips the hash index and restart trailer.
fn block_data_end(data: &[u8]) -> usize {
    let (block, _) = strip_hash_index(data);
    if let Some((de, _)) = parse_restart_trailer(block) {
        return de;
    }
    data.len()
}

// ---------------------------------------------------------------------------
// SegmentIter — prefix scan iterator
// ---------------------------------------------------------------------------

/// Common block-loading logic shared by `SegmentIter` and `OwnedSegmentIter`.
///
/// Attempts to load the block at `(partition_idx, block_within_partition)` from
/// the segment's index. On success, sets the block data and resets the offset.
/// On partition exhaustion, advances partitions until a non-empty one is found
/// or all partitions are consumed.
///
/// Returns `Some(block_data)` on success, `None` if all partitions exhausted,
/// or the corruption error string if a block read fails.
#[allow(clippy::type_complexity)]
fn load_next_block(
    segment: &KVSegment,
    partition_idx: &mut usize,
    block_within_partition: &mut usize,
) -> Result<Option<(Arc<Vec<u8>>, usize)>, String> {
    let sub = loop {
        match segment.index.partition(*partition_idx) {
            Some(sub) if *block_within_partition < sub.len() => break sub,
            Some(_) | None => {
                *partition_idx += 1;
                *block_within_partition = 0;
                if segment.index.partition(*partition_idx).is_none() {
                    return Ok(None);
                }
            }
        }
    };
    let ie = IndexEntry {
        key: Vec::new(),
        block_offset: sub.block_offset(*block_within_partition),
        block_data_len: sub.block_data_len(*block_within_partition),
    };
    match segment.read_data_block(&ie) {
        Ok(data) => {
            let de = block_data_end(&data);
            Ok(Some((data, de)))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Iterator over segment entries matching a prefix.
pub struct SegmentIter<'a> {
    segment: &'a KVSegment,
    prefix_bytes: Vec<u8>,
    partition_idx: usize,
    block_within_partition: usize,
    block_offset: usize,
    /// End of entry data in the current block (excludes restart trailer for v3+).
    block_data_end: usize,
    block_data: Option<Arc<Vec<u8>>>,
    done: bool,
    /// Buffer for v4 prefix-compressed key reconstruction (reused across entries).
    prev_key: Vec<u8>,
    /// Set to true if iteration was stopped by a corruption error (#1749).
    corruption_detected: bool,
}

impl SegmentIter<'_> {
    /// Returns true if iteration was stopped early due to data block corruption.
    pub fn corruption_detected(&self) -> bool {
        self.corruption_detected
    }
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
                match load_next_block(
                    self.segment,
                    &mut self.partition_idx,
                    &mut self.block_within_partition,
                ) {
                    Ok(Some((data, de))) => {
                        self.block_data_end = de;
                        self.block_data = Some(data);
                        self.block_offset = 0;
                        self.prev_key.clear();
                    }
                    Ok(None) => {
                        self.done = true;
                        return None;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "segment iterator stopping due to corruption");
                        self.corruption_detected = true;
                        self.done = true;
                        return None;
                    }
                }
            }

            let data = self.block_data.as_ref().unwrap();

            if self.block_offset >= self.block_data_end {
                // Move to next block
                self.block_data = None;
                self.block_within_partition += 1;
                continue;
            }

            let result = decode_entry_v4(
                &data[self.block_offset..self.block_data_end],
                &mut self.prev_key,
            );

            match result {
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
                            raw_value: None,
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
// OwnedSegmentIter — streaming iterator with Arc ownership
// ---------------------------------------------------------------------------

/// Streaming iterator that owns its segment via `Arc`.
///
/// Unlike `SegmentIter` (which borrows `&KVSegment`), this can be passed
/// to `MergeIterator` without materializing all entries via `.collect()`.
/// This reduces compaction memory from O(total entries) to O(block size).
pub struct OwnedSegmentIter {
    segment: Arc<KVSegment>,
    partition_idx: usize,
    block_within_partition: usize,
    block_offset: usize,
    /// End of entry data in the current block (excludes restart trailer for v3+).
    block_data_end: usize,
    block_data: Option<Arc<Vec<u8>>>,
    done: bool,
    /// Buffer for v4 prefix-compressed key reconstruction (reused across entries).
    prev_key: Vec<u8>,
    /// Monotonically increasing global block counter (for ThrottledSegmentIter).
    global_block_idx: usize,
    /// When true, carry pre-encoded bincode bytes instead of deserializing
    /// values. Used for zero-copy compaction passthrough (#1765).
    raw_values: bool,
    /// Set to true if iteration was stopped by a corruption error (#1677).
    corruption_detected: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// Optional prefix filter for seek-based iteration. When set, entries
    /// whose typed_key_prefix doesn't start with this are skipped; entries
    /// past it stop iteration. `None` = iterate all (existing behavior).
    prefix_bytes: Option<Vec<u8>>,
}

impl OwnedSegmentIter {
    /// Create a streaming iterator over all entries in the segment.
    pub fn new(segment: Arc<KVSegment>) -> Self {
        let done = segment.index.is_empty();
        Self {
            segment,
            partition_idx: 0,
            block_within_partition: 0,
            block_offset: 0,
            block_data_end: 0,
            block_data: None,
            done,
            prev_key: Vec::new(),
            global_block_idx: 0,
            raw_values: false,
            corruption_detected: None,
            prefix_bytes: None,
        }
    }

    /// Create a streaming iterator that seeks to `start_key` and filters
    /// by `prefix_bytes`.
    ///
    /// Combines the seek capability of [`KVSegment::iter_seek`] with the
    /// `Arc` ownership of `OwnedSegmentIter`. Enables lazy iteration
    /// without borrowing the segment.
    pub fn new_seek(segment: Arc<KVSegment>, start_key: &Key, prefix_bytes: Vec<u8>) -> Self {
        let done = segment.index.is_empty();
        let (partition_idx, block_within_partition) = if done {
            (0, 0)
        } else {
            let seek_ik = InternalKey::encode(start_key, u64::MAX);
            segment.index.seek_position(seek_ik.as_bytes())
        };
        Self {
            segment,
            partition_idx,
            block_within_partition,
            block_offset: 0,
            block_data_end: 0,
            block_data: None,
            done,
            prev_key: Vec::new(),
            global_block_idx: 0,
            raw_values: false,
            corruption_detected: None,
            prefix_bytes: Some(prefix_bytes),
        }
    }

    /// Enable raw value mode: carry pre-encoded bincode bytes instead of
    /// deserializing values. Eliminates the deserialize→serialize round-trip
    /// during compaction (#1765).
    pub fn with_raw_values(mut self) -> Self {
        self.raw_values = true;
        self
    }

    /// Attach a shared corruption flag that will be set if a data block
    /// cannot be read due to corruption (#1677). Callers (e.g., compaction)
    /// check this flag after consuming the iterator.
    pub fn with_corruption_flag(
        mut self,
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.corruption_detected = Some(flag);
        self
    }

    /// Set a prefix filter without seeking. Used by `LevelSegmentIter` when
    /// opening subsequent segments from the beginning (seek already done via
    /// binary search on the segment list).
    pub fn with_prefix_filter(mut self, prefix_bytes: Vec<u8>) -> Self {
        self.prefix_bytes = Some(prefix_bytes);
        self
    }
}

impl OwnedSegmentIter {
    /// Reposition to the first entry >= `start_key` within this segment.
    ///
    /// Reuses the existing `Arc<KVSegment>` — only resets the block position
    /// via the in-memory index (O(log B) binary search, no I/O unless the
    /// target block isn't cached). Equivalent to RocksDB's per-SST
    /// `InternalIterator::Seek()`.
    pub fn seek(&mut self, start_key: &Key) {
        if self.segment.index_is_empty() {
            self.done = true;
            return;
        }
        let seek_ik = InternalKey::encode(start_key, u64::MAX);
        let (partition_idx, block_within_partition) =
            self.segment.index_seek_position(seek_ik.as_bytes());
        self.partition_idx = partition_idx;
        self.block_within_partition = block_within_partition;
        self.block_offset = 0;
        self.block_data_end = 0;
        self.block_data = None; // force block reload
        self.done = false;
        self.prev_key.clear();
    }

    /// Reset block position directly (used by `SegmentSeekableIter`).
    ///
    /// Avoids the Key → InternalKey → seek_position round-trip when the
    /// caller already has the encoded InternalKey bytes.
    pub fn reset_position(&mut self, partition_idx: usize, block_within_partition: usize) {
        self.partition_idx = partition_idx;
        self.block_within_partition = block_within_partition;
        self.block_offset = 0;
        self.block_data_end = 0;
        self.block_data = None;
        self.done = false;
        self.prev_key.clear();
    }

    /// Access the underlying segment (for index queries by seekable iterators).
    pub fn segment(&self) -> &Arc<KVSegment> {
        &self.segment
    }

    /// Return a monotonically increasing block index (used by `ThrottledSegmentIter`).
    pub(crate) fn current_block_idx(&self) -> usize {
        self.global_block_idx
    }

    /// Returns true if iteration was halted by data block corruption.
    pub fn corruption_detected(&self) -> bool {
        self.corruption_detected
            .as_ref()
            .is_some_and(|f| f.load(std::sync::atomic::Ordering::Relaxed))
    }
}

impl Iterator for OwnedSegmentIter {
    type Item = (InternalKey, SegmentEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }

            if self.block_data.is_none() {
                match load_next_block(
                    &self.segment,
                    &mut self.partition_idx,
                    &mut self.block_within_partition,
                ) {
                    Ok(Some((data, de))) => {
                        self.block_data_end = de;
                        self.block_data = Some(data);
                        self.block_offset = 0;
                        self.prev_key.clear();
                    }
                    Ok(None) => {
                        self.done = true;
                        return None;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "segment iterator stopping due to corruption");
                        if let Some(ref flag) = self.corruption_detected {
                            flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        self.done = true;
                        return None;
                    }
                }
            }

            let data = self.block_data.as_ref().unwrap();

            if self.block_offset >= self.block_data_end {
                self.block_data = None;
                self.block_within_partition += 1;
                self.global_block_idx += 1;
                continue;
            }

            if self.raw_values {
                // Raw mode: skip bincode deserialization, carry raw bytes (#1765)
                let result = decode_entry_v4_raw(
                    &data[self.block_offset..self.block_data_end],
                    &mut self.prev_key,
                );
                match result {
                    Some((ik, is_tomb, raw_val, timestamp, ttl_ms, consumed)) => {
                        self.block_offset += consumed;

                        if let Some(ref pb) = self.prefix_bytes {
                            if !ik.typed_key_prefix().starts_with(pb) {
                                if ik.typed_key_prefix() > pb.as_slice() {
                                    self.done = true;
                                    return None;
                                }
                                continue;
                            }
                        }

                        let commit_id = ik.commit_id();
                        return Some((
                            ik,
                            SegmentEntry {
                                value: Value::Null,
                                is_tombstone: is_tomb,
                                commit_id,
                                timestamp,
                                ttl_ms,
                                raw_value: raw_val,
                            },
                        ));
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }

            let result = decode_entry_v4(
                &data[self.block_offset..self.block_data_end],
                &mut self.prev_key,
            );

            match result {
                Some((ik, is_tomb, value, timestamp, ttl_ms, consumed)) => {
                    self.block_offset += consumed;

                    if let Some(ref pb) = self.prefix_bytes {
                        if !ik.typed_key_prefix().starts_with(pb) {
                            if ik.typed_key_prefix() > pb.as_slice() {
                                self.done = true;
                                return None;
                            }
                            continue;
                        }
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
                            raw_value: None,
                        },
                    ));
                }
                None => {
                    self.done = true;
                    return None;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LevelSegmentIter — lazy per-level iterator (RocksDB LevelIterator pattern)
// ---------------------------------------------------------------------------

/// Lazy iterator over a sorted, non-overlapping level of segments.
///
/// Analogous to RocksDB's `LevelIterator` (version_set.cc). Instead of
/// opening iterators for ALL segments at creation time, this:
/// 1. Binary-searches the segment list by `key_range()` to find the first
///    relevant file (O(log N), no I/O)
/// 2. Opens only that file's `OwnedSegmentIter` (1 block read)
/// 3. When exhausted, lazily opens the next file
///
/// For a 10-entry scan at L2 with 1000 segments, this opens 1-2 files
/// instead of 1000. See issue #2213.
pub struct LevelSegmentIter {
    segments: Vec<Arc<KVSegment>>,
    current_idx: usize,
    current_iter: Option<OwnedSegmentIter>,
    prefix_bytes: Vec<u8>,
    corruption_flag: Arc<std::sync::atomic::AtomicBool>,
}

impl LevelSegmentIter {
    /// Create a new level iterator. No files are opened until iteration.
    pub fn new(
        segments: Vec<Arc<KVSegment>>,
        prefix_bytes: Vec<u8>,
        corruption_flag: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            segments,
            current_idx: 0,
            current_iter: None,
            prefix_bytes,
            corruption_flag,
        }
    }

    /// Seek to the first segment whose key range includes `start_key`.
    ///
    /// Uses binary search on `key_range().max` (O(log N), no I/O).
    /// Opens only the matching segment's iterator.
    pub fn seek(&mut self, start_key: &Key) {
        use crate::key_encoding::{encode_typed_key, COMMIT_ID_SUFFIX_LEN};

        let start_bytes = encode_typed_key(start_key);

        // Binary search: find first segment whose max_key >= start_bytes.
        // Segments are sorted by key range (non-overlapping at L1+).
        let idx = self.segments.partition_point(|seg| {
            let (_, max_ik) = seg.key_range();
            if max_ik.len() >= COMMIT_ID_SUFFIX_LEN {
                let max_typed = &max_ik[..max_ik.len() - COMMIT_ID_SUFFIX_LEN];
                max_typed < start_bytes.as_slice()
            } else {
                false
            }
        });

        self.current_idx = idx;
        if idx < self.segments.len() {
            self.current_iter = Some(
                OwnedSegmentIter::new_seek(
                    Arc::clone(&self.segments[idx]),
                    start_key,
                    self.prefix_bytes.clone(),
                )
                .with_corruption_flag(Arc::clone(&self.corruption_flag)),
            );
        } else {
            self.current_iter = None;
        }
    }

    /// Seek from raw InternalKey bytes (used by `LevelSeekableIter`).
    ///
    /// Includes RocksDB's same-file optimization: if target is within the
    /// current file's key range, repositions within it (O(log B)) instead
    /// of binary-searching the file list (O(log F) + O(log B)).
    pub fn seek_bytes(&mut self, target: &[u8]) {
        use crate::key_encoding::COMMIT_ID_SUFFIX_LEN;

        // Extract the typed_key portion (without commit_id suffix) for comparison
        let target_typed = if target.len() >= COMMIT_ID_SUFFIX_LEN {
            &target[..target.len() - COMMIT_ID_SUFFIX_LEN]
        } else {
            target
        };

        // RocksDB optimization: check if target is within current file's
        // [min_key, max_key] range. Must check BOTH bounds — if target is
        // before current file's min, an earlier file may contain the entry.
        if let Some(ref mut iter) = self.current_iter {
            if self.current_idx < self.segments.len() {
                let (min_ik, max_ik) = self.segments[self.current_idx].key_range();
                let min_typed = if min_ik.len() >= COMMIT_ID_SUFFIX_LEN {
                    &min_ik[..min_ik.len() - COMMIT_ID_SUFFIX_LEN]
                } else {
                    min_ik
                };
                let max_typed = if max_ik.len() >= COMMIT_ID_SUFFIX_LEN {
                    &max_ik[..max_ik.len() - COMMIT_ID_SUFFIX_LEN]
                } else {
                    max_ik
                };
                if target_typed >= min_typed && target_typed <= max_typed {
                    // Target is within current file — seek in place
                    let (part_idx, block_idx) =
                        self.segments[self.current_idx].index_seek_position(target);
                    iter.reset_position(part_idx, block_idx);
                    return;
                }
            }
        }

        // Target beyond current file — binary search for new file
        let idx = self.segments.partition_point(|seg| {
            let (_, max_ik) = seg.key_range();
            if max_ik.len() >= COMMIT_ID_SUFFIX_LEN {
                let max_typed = &max_ik[..max_ik.len() - COMMIT_ID_SUFFIX_LEN];
                max_typed < target_typed
            } else {
                false
            }
        });

        self.current_idx = idx;
        if idx < self.segments.len() {
            let (part_idx, block_idx) = self.segments[idx].index_seek_position(target);
            self.current_iter = Some(
                OwnedSegmentIter::new(Arc::clone(&self.segments[idx]))
                    .with_prefix_filter(self.prefix_bytes.clone())
                    .with_corruption_flag(Arc::clone(&self.corruption_flag)),
            );
            if let Some(ref mut iter) = self.current_iter {
                iter.reset_position(part_idx, block_idx);
            }
        } else {
            self.current_iter = None;
        }
    }

    /// Open the next segment's iterator (lazy file opening).
    fn advance_to_next_segment(&mut self) {
        self.current_idx += 1;
        if self.current_idx < self.segments.len() {
            // Open from the beginning of the next segment (no seek needed —
            // segments are sorted, so the next entry is at the start).
            self.current_iter = Some(
                OwnedSegmentIter::new(Arc::clone(&self.segments[self.current_idx]))
                    .with_prefix_filter(self.prefix_bytes.clone())
                    .with_corruption_flag(Arc::clone(&self.corruption_flag)),
            );
        } else {
            self.current_iter = None;
        }
    }
}

impl Iterator for LevelSegmentIter {
    type Item = (InternalKey, SegmentEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut iter) = self.current_iter {
                if let Some(item) = iter.next() {
                    return Some(item);
                }
                // Current segment exhausted — try next
            } else {
                return None;
            }
            self.advance_to_next_segment();
        }
    }
}

// ---------------------------------------------------------------------------
// ThrottledSegmentIter — rate-limited wrapper for compaction reads
// ---------------------------------------------------------------------------

/// Wraps an [`OwnedSegmentIter`] and charges a [`RateLimiter`] each time a
/// new data block is loaded. Used during compaction to throttle read I/O.
pub(crate) struct ThrottledSegmentIter {
    inner: OwnedSegmentIter,
    limiter: std::sync::Arc<crate::rate_limiter::RateLimiter>,
    last_block_idx: usize,
}

impl ThrottledSegmentIter {
    pub(crate) fn new(
        inner: OwnedSegmentIter,
        limiter: std::sync::Arc<crate::rate_limiter::RateLimiter>,
    ) -> Self {
        Self {
            // Use usize::MAX so the first block load (block_idx == 0) is charged.
            last_block_idx: usize::MAX,
            inner,
            limiter,
        }
    }
}

impl Iterator for ThrottledSegmentIter {
    type Item = (InternalKey, SegmentEntry);

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner.next()?;
        let cur = self.inner.current_block_idx();
        if cur != self.last_block_idx {
            // Charge ~64 KiB per block transition (matches default data_block_size).
            self.limiter.acquire(64 * 1024);
            self.last_block_idx = cur;
        }
        Some(result)
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
            compression: crate::segment_builder::CompressionCodec::default(),
            rate_limiter: None,
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

        let e = seg.point_lookup(&kv_key("a"), u64::MAX).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert!(!e.is_tombstone);

        let e = seg.point_lookup(&kv_key("b"), u64::MAX).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(20));

        let e = seg.point_lookup(&kv_key("c"), u64::MAX).unwrap().unwrap();
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
        assert!(seg.point_lookup(&kv_key("z"), u64::MAX).unwrap().is_none());
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
        let e = seg.point_lookup(&kv_key("k"), 10).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(100));
        assert_eq!(e.commit_id, 10);

        // Snapshot at 7: see version 5
        let e = seg.point_lookup(&kv_key("k"), 7).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(50));
        assert_eq!(e.commit_id, 5);

        // Snapshot at 1: see version 1
        let e = seg.point_lookup(&kv_key("k"), 1).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert_eq!(e.commit_id, 1);

        // Snapshot at 0: nothing visible
        assert!(seg.point_lookup(&kv_key("k"), 0).unwrap().is_none());
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
        let e = seg.point_lookup(&kv_key("k"), 2).unwrap().unwrap();
        assert!(e.is_tombstone);
        assert_eq!(e.commit_id, 2);

        // Snapshot at 1: see the value
        let e = seg.point_lookup(&kv_key("k"), 1).unwrap().unwrap();
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
        mt.put(&kv_key("float"), 1, Value::Float(2.78), false);
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
            seg.point_lookup(&kv_key("null"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Null,
        );
        assert_eq!(
            seg.point_lookup(&kv_key("bool"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Bool(true),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("int"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(42),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("float"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Float(2.78),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("string"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::String("hello".into()),
        );
        assert_eq!(
            seg.point_lookup(&kv_key("bytes"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
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
        let e = seg
            .point_lookup(&kv_key("key_000000"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.commit_id, 1);

        let e = seg
            .point_lookup(&kv_key("key_000250"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.commit_id, 251);

        let e = seg
            .point_lookup(&kv_key("key_000499"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.commit_id, 500);

        // Non-existent key
        assert!(seg
            .point_lookup(&kv_key("key_999999"), u64::MAX)
            .unwrap()
            .is_none());
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
        // point_lookup must return an error because read_data_block checks CRC
        let result = seg.point_lookup(&kv_key("k"), u64::MAX);
        assert!(
            result.is_err(),
            "corrupt data block CRC should cause lookup to return Err, got {:?}",
            result,
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
        assert!(seg
            .point_lookup(&kv_key("anything"), u64::MAX)
            .unwrap()
            .is_none());
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
            seg.point_lookup(&kv_key("arr"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            array_val,
        );
        assert_eq!(
            seg.point_lookup(&kv_key("obj"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
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
            compression: crate::segment_builder::CompressionCodec::default(),
            rate_limiter: None,
        };
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.entry_count(), n as u64);

        // Verify every entry via point lookup
        for i in 0..n {
            let k = kv_key(&format!("k_{:08}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap_or_else(|_| panic!("point lookup failed for k_{:08}", i))
                .unwrap();
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
                raw_value: None,
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
                raw_value: None,
            },
        );
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        let e = seg.point_lookup(&kv_key("k1"), u64::MAX).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(42));
        assert_eq!(e.timestamp, 1_700_000_000_000_000);
        assert_eq!(e.ttl_ms, 30_000);

        let e = seg.point_lookup(&kv_key("k2"), u64::MAX).unwrap().unwrap();
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
                raw_value: None,
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
                raw_value: None,
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
        let e = seg
            .point_lookup(&kv_key("k_000000"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.commit_id, 1);
        let e = seg
            .point_lookup(&kv_key("k_000499"), u64::MAX)
            .unwrap()
            .unwrap();
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
            compression: crate::segment_builder::CompressionCodec::default(),
            rate_limiter: None,
        };
        let meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();
        assert_eq!(meta.entry_count, 200);

        // Verify all data is readable
        let seg = KVSegment::open(&path).unwrap();
        for i in 0..200u32 {
            let k = kv_key(&format!("key_{:06}", i));
            let e = seg.point_lookup(&k, u64::MAX).unwrap().unwrap();
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

    // ===== key_range tests =====

    #[test]
    fn key_range_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kr.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("only"), 1, Value::Int(1), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let (min, max) = seg.key_range();
        assert_eq!(min, max, "single entry should have min == max");
        assert!(!min.is_empty());
    }

    #[test]
    fn key_range_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kr_multi.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("aaa"), 1, Value::Int(1), false);
        mt.put(&kv_key("zzz"), 2, Value::Int(2), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let (min, max) = seg.key_range();
        assert!(
            min < max,
            "min should be less than max for multiple entries"
        );
        assert!(!min.is_empty());
        assert!(!max.is_empty());
    }

    #[test]
    fn key_range_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kr_empty.sst");

        let builder = SegmentBuilder::default();
        builder.build_from_iter(std::iter::empty(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        let (min, max) = seg.key_range();
        assert!(min.is_empty());
        assert!(max.is_empty());
    }

    // ===== OwnedSegmentIter tests =====

    #[test]
    fn owned_iter_matches_borrowed_iter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned.sst");

        let mt = Memtable::new(0);
        for i in 0..100u32 {
            mt.put(
                &kv_key(&format!("key_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = Arc::new(KVSegment::open(&path).unwrap());

        // Collect from borrowed iter
        let borrowed: Vec<_> = seg.iter_seek_all().collect();

        // Collect from owned iter
        let owned: Vec<_> = super::OwnedSegmentIter::new(Arc::clone(&seg)).collect();

        assert_eq!(borrowed.len(), owned.len());
        for (b, o) in borrowed.iter().zip(owned.iter()) {
            assert_eq!(b.0.as_bytes(), o.0.as_bytes());
            assert_eq!(b.1.commit_id, o.1.commit_id);
            assert_eq!(b.1.value, o.1.value);
        }
    }

    #[test]
    fn owned_iter_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned_empty.sst");

        let builder = SegmentBuilder::default();
        builder.build_from_iter(std::iter::empty(), &path).unwrap();

        let seg = Arc::new(KVSegment::open(&path).unwrap());
        let entries: Vec<_> = super::OwnedSegmentIter::new(seg).collect();
        assert!(entries.is_empty());
    }

    // ===== OwnedSegmentIter::new_seek tests =====

    #[test]
    fn owned_iter_new_seek_matches_iter_seek() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned_seek.sst");

        let mt = Memtable::new(0);
        for i in 0..50u32 {
            mt.put(
                &kv_key(&format!("key_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = Arc::new(KVSegment::open(&path).unwrap());
        let prefix = kv_key("");
        let prefix_bytes = encode_typed_key_prefix(&prefix);

        // Collect from borrowed iter_seek
        let borrowed: Vec<_> = seg.iter_seek(&prefix).collect();

        // Collect from owned new_seek
        let owned: Vec<_> =
            OwnedSegmentIter::new_seek(Arc::clone(&seg), &prefix, prefix_bytes).collect();

        assert_eq!(
            borrowed.len(),
            owned.len(),
            "new_seek and iter_seek should yield same count"
        );
        for (b, o) in borrowed.iter().zip(owned.iter()) {
            assert_eq!(b.0.as_bytes(), o.0.as_bytes());
            assert_eq!(b.1.commit_id, o.1.commit_id);
            assert_eq!(b.1.value, o.1.value);
        }
    }

    #[test]
    fn owned_iter_new_seek_mid_key_skips_earlier_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned_seek_mid.sst");

        let mt = Memtable::new(0);
        for i in 0..50u32 {
            mt.put(
                &kv_key(&format!("key_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = Arc::new(KVSegment::open(&path).unwrap());
        let prefix = kv_key("");
        let prefix_bytes = encode_typed_key_prefix(&prefix);

        // Seek to "key_0025" — should skip key_0000..key_0024
        let start = kv_key("key_0025");
        let results: Vec<_> =
            OwnedSegmentIter::new_seek(Arc::clone(&seg), &start, prefix_bytes).collect();

        // Due to block-level seek, entries before key_0025 in the same block
        // may be emitted. But no entry from a PRIOR block should appear.
        // All entries should be in the namespace.
        assert!(
            !results.is_empty(),
            "should yield entries from key_0025 onward"
        );
        // The LAST entry should always be key_0049 (no entries lost at the end)
        let last_ik = &results.last().unwrap().0;
        let (last_key, _) = last_ik.decode().unwrap();
        assert_eq!(last_key.user_key_string().unwrap(), "key_0049");
    }

    #[test]
    fn owned_iter_new_seek_stops_at_prefix_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned_seek_prefix.sst");

        let mt = Memtable::new(0);
        // Two key ranges: "aaa:*" and "bbb:*"
        for i in 0..20u32 {
            mt.put(
                &kv_key(&format!("aaa:{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        for i in 0..20u32 {
            mt.put(
                &kv_key(&format!("bbb:{:04}", i)),
                i as u64 + 21,
                Value::Int(i as i64 + 100),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = Arc::new(KVSegment::open(&path).unwrap());

        // Seek with prefix for "aaa:" only
        let prefix = kv_key("aaa:");
        let prefix_bytes = encode_typed_key_prefix(&prefix);
        let results: Vec<_> =
            OwnedSegmentIter::new_seek(Arc::clone(&seg), &prefix, prefix_bytes).collect();

        assert_eq!(results.len(), 20, "should only yield aaa: entries");

        // Verify all entries have the right prefix
        for (ik, _) in &results {
            let decoded_key = ik.typed_key_prefix();
            assert!(
                decoded_key.starts_with(&encode_typed_key_prefix(&kv_key("aaa:"))),
                "entry should be in aaa: range"
            );
        }
    }

    // ===== Restart point binary search tests (v3 format) =====

    #[test]
    fn binary_search_first_interval() {
        // Key in entries 0-15 found correctly via binary search
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart_first.sst");

        let mt = Memtable::new(0);
        for i in 0..48u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        // Use large block size so all entries fit in one block
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        // Look up a key in the first interval (entries 0-15)
        let e = seg
            .point_lookup(&kv_key("k_0005"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(5));
        assert_eq!(e.commit_id, 6);
    }

    #[test]
    fn binary_search_last_interval() {
        // Key in last interval found correctly
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart_last.sst");

        let mt = Memtable::new(0);
        for i in 0..48u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        // Key in last interval (entries 32-47)
        let e = seg
            .point_lookup(&kv_key("k_0045"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(45));
    }

    #[test]
    fn binary_search_at_restart_boundary() {
        // Key exactly at restart point entry 16
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart_boundary.sst");

        let mt = Memtable::new(0);
        for i in 0..48u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        // Entry 16 is exactly at a restart point
        let e = seg
            .point_lookup(&kv_key("k_0016"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(16));
        assert_eq!(e.commit_id, 17);
    }

    #[test]
    fn binary_search_mvcc_versions() {
        // 5 versions of same key, correct version returned
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart_mvcc.sst");

        let mt = Memtable::new(0);
        // Insert many unique keys to push MVCC versions across restart boundaries
        for i in 0..30u32 {
            mt.put(
                &kv_key(&format!("a_{:04}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        // 5 versions of key "b_0000" — entries 30-34 span restart boundary at 32
        for v in 1..=5u64 {
            mt.put(&kv_key("b_0000"), v, Value::Int(v as i64 * 10), false);
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Snapshot at 5: see version 5
        let e = seg.point_lookup(&kv_key("b_0000"), 5).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(50));
        assert_eq!(e.commit_id, 5);

        // Snapshot at 3: see version 3
        let e = seg.point_lookup(&kv_key("b_0000"), 3).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(30));
        assert_eq!(e.commit_id, 3);

        // Snapshot at 1: see version 1
        let e = seg.point_lookup(&kv_key("b_0000"), 1).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(10));
    }

    #[test]
    fn binary_search_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart_miss.sst");

        let mt = Memtable::new(0);
        for i in 0..48u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(seg
            .point_lookup(&kv_key("zzz_missing"), u64::MAX)
            .unwrap()
            .is_none());
        assert!(seg
            .point_lookup(&kv_key("aaa_missing"), u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn iter_respects_data_end() {
        // Verify that iterators stop at data_end and don't parse restart trailer as entries
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart_iter.sst");

        let mt = Memtable::new(0);
        for i in 0..48u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 48, "iterator should yield exactly 48 entries");

        // Verify ordering
        for i in 1..all.len() {
            assert!(all[i - 1].0 < all[i].0, "entries must be in order");
        }
    }

    #[test]
    fn multi_block_binary_search() {
        // Binary search works correctly across multiple blocks
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart_multi.sst");

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

        // Verify lookups across different blocks
        let e = seg
            .point_lookup(&kv_key("key_000000"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.commit_id, 1);

        let e = seg
            .point_lookup(&kv_key("key_000250"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.commit_id, 251);

        let e = seg
            .point_lookup(&kv_key("key_000499"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.commit_id, 500);

        assert!(seg
            .point_lookup(&kv_key("key_999999"), u64::MAX)
            .unwrap()
            .is_none());

        // Verify iteration yields all entries
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 500);
    }

    #[test]
    fn compaction_output_has_restart_points() {
        // Segments produced by compaction (via SplittingSegmentBuilder → SegmentBuilder)
        // should be v3 with restart trailers.
        use crate::segment_builder::SplittingSegmentBuilder;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.sst");

        let mt = Memtable::new(0);
        for i in 0..100u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        // Read entries from the input segment
        let seg = KVSegment::open(&path).unwrap();
        let entries: Vec<_> = seg.iter_seek_all().collect();

        // Re-build via SplittingSegmentBuilder (used by compaction)
        let out_dir = dir.path().join("compacted");
        std::fs::create_dir_all(&out_dir).unwrap();
        let splitter = SplittingSegmentBuilder::default();
        let iter = entries.iter().map(|(ik, se)| {
            let me = crate::memtable::MemtableEntry {
                value: se.value.clone(),
                is_tombstone: se.is_tombstone,
                timestamp: strata_core::Timestamp::from_micros(se.timestamp),
                ttl_ms: se.ttl_ms,
                raw_value: None,
            };
            (ik.clone(), me)
        });
        let results = splitter
            .build_split(iter, |idx| out_dir.join(format!("{:06}.sst", idx)))
            .unwrap();

        // Verify output is v4 and readable
        for (p, meta) in &results {
            let out_seg = KVSegment::open(p).unwrap();
            assert_eq!(out_seg.header.format_version, 7);
            assert_eq!(out_seg.entry_count(), meta.entry_count);
        }
    }

    // ===== v4 prefix compression integration tests =====

    #[test]
    fn v4_binary_search_works() {
        // Build a multi-restart block segment and verify binary search finds correct intervals.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4_bsearch.sst");

        let mt = Memtable::new(0);
        // 500 entries with small blocks → many restarts per block
        for i in 0..500u32 {
            let k = kv_key(&format!("key_{:06}", i));
            let val = Value::String(format!("val_{}", "x".repeat(50)));
            mt.put(&k, i as u64 + 1, val, false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.header.format_version, 7);

        // Point lookup every entry via binary search
        for i in 0..500u32 {
            let k = kv_key(&format!("key_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("missing key_{:06}", i));
            assert_eq!(e.value, Value::String(format!("val_{}", "x".repeat(50))));
            assert_eq!(e.commit_id, i as u64 + 1);
        }

        // Non-existent key
        assert!(seg
            .point_lookup(&kv_key("key_999999"), u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn v4_iter_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4_iter.sst");

        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = kv_key(&format!("item_{:04}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.header.format_version, 7);

        let all: Vec<_> = seg.iter_seek(&kv_key("item_")).collect();
        assert_eq!(all.len(), 200);

        // Check ordering
        for i in 1..all.len() {
            assert!(all[i - 1].0 < all[i].0, "entries must be in order");
        }
    }

    #[test]
    fn v4_owned_iter_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4_owned_iter.sst");

        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = kv_key(&format!("item_{:04}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = Arc::new(KVSegment::open(&path).unwrap());
        let all: Vec<_> = OwnedSegmentIter::new(seg).collect();
        assert_eq!(all.len(), 200);

        for i in 1..all.len() {
            assert!(all[i - 1].0 < all[i].0, "entries must be in order");
        }
    }

    // ===== Partitioned bloom filter tests =====

    #[test]
    fn partitioned_bloom_no_false_negatives() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        for i in 0..10_000u32 {
            mt.put(
                &kv_key(&format!("key_{:06}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        for i in 0..10_000u32 {
            assert!(
                seg.bloom_maybe_contains(&kv_key(&format!("key_{:06}", i))),
                "false negative for key_{:06}",
                i,
            );
        }
    }

    #[test]
    fn partitioned_bloom_multiple_partitions() {
        // Use 256-byte blocks so many data blocks are created,
        // forcing multiple bloom partitions (1 per 16 data blocks).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        for i in 0..2000u32 {
            mt.put(
                &kv_key(&format!("item_{:06}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Verify multiple partitions were created
        assert!(
            seg.bloom.index.len() > 1,
            "expected multiple bloom partitions, got {}",
            seg.bloom.index.len()
        );

        // All keys should be found (no false negatives)
        for i in 0..2000u32 {
            let k = kv_key(&format!("item_{:06}", i));
            assert!(
                seg.bloom_maybe_contains(&k),
                "false negative for item_{:06}",
                i
            );
        }

        // Point lookups should all succeed
        for i in 0..2000u32 {
            let k = kv_key(&format!("item_{:06}", i));
            let e = seg.point_lookup(&k, u64::MAX).unwrap();
            assert!(e.is_some(), "point lookup failed for item_{:06}", i);
            assert_eq!(e.unwrap().value, Value::Int(i as i64));
        }
    }

    #[test]
    fn partitioned_bloom_loads_one_partition() {
        // Verify that bloom lookups work correctly with multiple partitions:
        // existing keys return true, keys beyond all partitions return false.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        for i in 0..2000u32 {
            mt.put(
                &kv_key(&format!("item_{:06}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let num_partitions = seg.bloom.index.len();
        assert!(num_partitions > 1, "need multiple partitions for this test");

        // Existing key returns true
        let k = kv_key("item_001000");
        assert!(seg.bloom_maybe_contains(&k));

        // Non-existent key beyond all partitions should return false
        let k = kv_key("zzz_not_exist");
        assert!(!seg.bloom_maybe_contains(&k));

        // Non-existent key within key range may return true (false positive)
        // or false — either is acceptable for a bloom filter. Just verify no panic.
        let _result = seg.bloom_maybe_contains(&kv_key("item_999999"));
    }

    #[test]
    fn partitioned_bloom_roundtrip_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SegmentBuilder::default();
        builder.build_from_iter(std::iter::empty(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.entry_count(), 0);
        // Empty segment: bloom check returns true (no filter info)
        assert!(seg.bloom.index.is_empty());
        assert!(seg.bloom_maybe_contains(&kv_key("anything")));
    }

    // ===== Hash index (v6) tests =====

    #[test]
    fn hash_index_roundtrip_multi_block() {
        // Multi-block segment: 500 keys with small blocks forces many blocks,
        // each with its own hash index. Verifies every key is found.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_rt.sst");

        let mt = Memtable::new(0);
        for i in 0..500u32 {
            let k = kv_key(&format!("hk_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.header.format_version, 7);
        // Must span multiple blocks
        assert!(seg.index_entry_count() > 1, "expected multi-block segment");

        // Lookup every key from first, middle, and last blocks
        for i in 0..500u32 {
            let k = kv_key(&format!("hk_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("key hk_{:06} not found", i));
            assert_eq!(e.value, Value::Int(i as i64));
            assert_eq!(e.commit_id, i as u64 + 1);
        }
    }

    #[test]
    fn hash_index_missing_key_in_populated_block() {
        // Build a segment with many keys. Look up keys that don't exist but
        // whose hash may collide with existing bucket entries.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_miss.sst");

        let mt = Memtable::new(0);
        for i in 0..100u32 {
            let k = kv_key(&format!("exist_{:04}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Keys that don't exist — various positions relative to existing keys
        assert!(seg
            .point_lookup(&kv_key("aaa"), u64::MAX)
            .unwrap()
            .is_none()); // before all
        assert!(seg
            .point_lookup(&kv_key("exist_0050x"), u64::MAX)
            .unwrap()
            .is_none()); // between
        assert!(seg
            .point_lookup(&kv_key("zzz"), u64::MAX)
            .unwrap()
            .is_none()); // after all
                         // Keys with similar prefixes to stress hash collisions
        for i in 0..50u32 {
            let k = kv_key(&format!("exist_{:04}_GHOST", i));
            assert!(
                seg.point_lookup(&k, u64::MAX).unwrap().is_none(),
                "ghost key exist_{:04}_GHOST should not exist",
                i
            );
        }
    }

    #[test]
    fn hash_index_collision_fallback() {
        // With many keys in small blocks, hash collisions are inevitable.
        // Verify: (a) all present keys found, (b) absent keys not found.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_coll.sst");

        let mt = Memtable::new(0);
        for i in 0..500u32 {
            let k = kv_key(&format!("col_{:06}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // (a) All present keys must be found with correct values
        for i in 0..500u32 {
            let k = kv_key(&format!("col_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("key col_{:06} not found", i));
            assert_eq!(e.value, Value::Int(i as i64));
        }

        // (b) Absent keys in the same key-space must not be found
        for i in 500..600u32 {
            let k = kv_key(&format!("col_{:06}", i));
            assert!(
                seg.point_lookup(&k, u64::MAX).unwrap().is_none(),
                "absent key col_{:06} should not be found",
                i
            );
        }
    }

    #[test]
    fn hash_index_mvcc_versions() {
        // Many versions of the same key spanning multiple restart intervals.
        // All versions map to the same hash bucket, so the hash index must
        // point to restart interval 0 and forward scan must reach all versions.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_mvcc.sst");

        let mt = Memtable::new(0);
        let k = kv_key("versioned");
        // 50 versions → spans restart intervals 0..3 (at 16 entries per restart)
        for commit in 1..=50u64 {
            mt.put(&k, commit, Value::Int(commit as i64 * 10), false);
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Verify specific snapshot points
        let e = seg.point_lookup(&k, 1).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert_eq!(e.commit_id, 1);

        let e = seg.point_lookup(&k, 25).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(250));
        assert_eq!(e.commit_id, 25);

        let e = seg.point_lookup(&k, u64::MAX).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(500));
        assert_eq!(e.commit_id, 50);

        // snapshot_commit=0 → no version visible
        assert!(seg.point_lookup(&k, 0).unwrap().is_none());
    }

    #[test]
    fn hash_index_single_entry_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_single.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("only"), 1, Value::Int(42), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        let e = seg
            .point_lookup(&kv_key("only"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(42));
        // Absent key in single-entry segment
        assert!(seg
            .point_lookup(&kv_key("nope"), u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn hash_index_iter_correct_data_end() {
        // Iterator must correctly strip hash index and yield all entries
        // with correct keys and values across multiple blocks.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_iter.sst");

        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = kv_key(&format!("it_{:04}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(seg.index_entry_count() > 1, "expected multi-block segment");

        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 200, "iterator should yield all 200 entries");

        // Verify ordering AND values
        for (idx, (ik, se)) in all.iter().enumerate() {
            let expected_key = format!("it_{:04}", idx);
            // Verify the typed_key_prefix contains the expected user key
            let tkp = ik.typed_key_prefix();
            assert!(
                tkp.windows(expected_key.len())
                    .any(|w| w == expected_key.as_bytes()),
                "entry {} has wrong key",
                idx
            );
            assert_eq!(
                se.value,
                Value::Int(idx as i64),
                "entry {} has wrong value",
                idx
            );
        }

        // Verify strict ordering
        for i in 1..all.len() {
            assert!(all[i - 1].0 < all[i].0, "entries must be in order at {}", i);
        }
    }

    #[test]
    fn hash_index_tombstone() {
        // Verify tombstones work correctly through hash-indexed lookup.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_tomb.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("alive"), 1, Value::Int(1), false);
        mt.put(&kv_key("dead"), 1, Value::Int(10), false);
        mt.put(&kv_key("dead"), 2, Value::Null, true); // tombstone
        mt.put(&kv_key("revived"), 1, Value::Int(20), false);
        mt.put(&kv_key("revived"), 2, Value::Null, true);
        mt.put(&kv_key("revived"), 3, Value::Int(30), false); // re-put after delete
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        let e = seg
            .point_lookup(&kv_key("alive"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(1));
        assert!(!e.is_tombstone);

        let e = seg
            .point_lookup(&kv_key("dead"), u64::MAX)
            .unwrap()
            .unwrap();
        assert!(e.is_tombstone);
        assert_eq!(e.commit_id, 2);

        // "dead" at snapshot 1 → see the value
        let e = seg.point_lookup(&kv_key("dead"), 1).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert!(!e.is_tombstone);

        // "revived" at latest → see the re-put
        let e = seg
            .point_lookup(&kv_key("revived"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(30));
        assert!(!e.is_tombstone);

        // "revived" at snapshot 2 → see tombstone
        let e = seg.point_lookup(&kv_key("revived"), 2).unwrap().unwrap();
        assert!(e.is_tombstone);
    }

    #[test]
    fn hash_index_prefix_scan_across_blocks() {
        // Prefix scan via iter_seek must work across hash-indexed blocks.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_prefix.sst");

        let mt = Memtable::new(0);
        // Two groups: "alpha_*" and "beta_*"
        for i in 0..100u32 {
            mt.put(
                &kv_key(&format!("alpha_{:04}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        for i in 0..100u32 {
            mt.put(
                &kv_key(&format!("beta_{:04}", i)),
                1,
                Value::Int(1000 + i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Prefix scan for "alpha_" keys only
        let alpha: Vec<_> = seg.iter_seek(&kv_key("alpha_")).collect();
        assert_eq!(alpha.len(), 100, "expected 100 alpha keys");
        for (_, se) in &alpha {
            assert!(
                matches!(se.value, Value::Int(v) if v < 1000),
                "alpha value should be < 1000"
            );
        }

        // Prefix scan for "beta_" keys only
        let beta: Vec<_> = seg.iter_seek(&kv_key("beta_")).collect();
        assert_eq!(beta.len(), 100, "expected 100 beta keys");
        for (_, se) in &beta {
            assert!(
                matches!(se.value, Value::Int(v) if v >= 1000),
                "beta value should be >= 1000"
            );
        }
    }

    // ===== Partitioned index tests (Epic 35) =====

    fn build_segment_tiny_blocks(mt: &Memtable, path: &Path) {
        let builder = SegmentBuilder {
            data_block_size: 128,
            bloom_bits_per_key: 10,
            compression: crate::segment_builder::CompressionCodec::None,
            rate_limiter: None,
        };
        builder.build_from_iter(mt.iter_all(), path).unwrap();
    }

    #[test]
    fn partitioned_index_point_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_point.sst");

        let mt = Memtable::new(0);
        for i in 0..1000u32 {
            let k = kv_key(&format!("pk_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.format_version(), 7);
        assert!(
            matches!(&seg.index, SegmentIndex::Partitioned { .. }),
            "expected partitioned index"
        );

        // Verify every point lookup
        for i in 0..1000u32 {
            let k = kv_key(&format!("pk_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("key pk_{:06} not found", i));
            assert_eq!(e.value, Value::Int(i as i64));
            assert_eq!(e.commit_id, i as u64 + 1);
        }

        // Missing keys return None
        assert!(seg
            .point_lookup(&kv_key("pk_999999"), u64::MAX)
            .unwrap()
            .is_none());
        assert!(seg
            .point_lookup(&kv_key("zz_missing"), u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn partitioned_index_iter_seek() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_seek.sst");

        let mt = Memtable::new(0);
        // Insert entries with two prefixes to test prefix scan across partitions
        for i in 0..500u32 {
            let k = kv_key(&format!("aa_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        for i in 0..500u32 {
            let k = kv_key(&format!("bb_{:06}", i));
            mt.put(&k, i as u64 + 1001, Value::Int(i as i64 + 1000), false);
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(matches!(&seg.index, SegmentIndex::Partitioned { .. }));

        // Prefix scan for "aa_" should yield all 500 entries with correct values
        let aa: Vec<_> = seg.iter_seek(&kv_key("aa_")).collect();
        assert_eq!(aa.len(), 500, "expected 500 aa_ entries");
        for (idx, (ik, se)) in aa.iter().enumerate() {
            assert_eq!(se.value, Value::Int(idx as i64));
            assert_eq!(se.commit_id, idx as u64 + 1);
            let tkp = ik.typed_key_prefix();
            let expected = format!("aa_{:06}", idx);
            assert!(
                tkp.windows(expected.len())
                    .any(|w| w == expected.as_bytes()),
                "aa entry {} has wrong key",
                idx
            );
        }

        // Verify ordering
        for i in 0..aa.len() - 1 {
            assert!(aa[i].0.as_bytes() <= aa[i + 1].0.as_bytes());
        }

        // Prefix scan for "bb_" should yield all 500 entries with correct values
        let bb: Vec<_> = seg.iter_seek(&kv_key("bb_")).collect();
        assert_eq!(bb.len(), 500, "expected 500 bb_ entries");
        for (idx, (_, se)) in bb.iter().enumerate() {
            assert_eq!(se.value, Value::Int(idx as i64 + 1000));
            assert_eq!(se.commit_id, idx as u64 + 1001);
        }
    }

    #[test]
    fn partitioned_index_full_iter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_full.sst");

        let mt = Memtable::new(0);
        let n = 800u32;
        for i in 0..n {
            let k = kv_key(&format!("fi_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(matches!(&seg.index, SegmentIndex::Partitioned { .. }));

        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), n as usize);

        // Verify correct ordering AND values for every entry
        for (idx, (ik, se)) in all.iter().enumerate() {
            assert_eq!(
                se.value,
                Value::Int(idx as i64),
                "wrong value at idx {}",
                idx
            );
            assert_eq!(se.commit_id, idx as u64 + 1, "wrong commit at idx {}", idx);
            let tkp = ik.typed_key_prefix();
            let expected = format!("fi_{:06}", idx);
            assert!(
                tkp.windows(expected.len())
                    .any(|w| w == expected.as_bytes()),
                "entry {} has wrong key",
                idx
            );
        }

        // Verify ordering
        for i in 0..all.len() - 1 {
            assert!(all[i].0.as_bytes() <= all[i + 1].0.as_bytes());
        }
    }

    #[test]
    fn partitioned_small_segment_monolithic() {
        // A small segment should fall back to monolithic index
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_mono.sst");

        let mt = Memtable::new(0);
        for i in 0..10u32 {
            let k = kv_key(&format!("sm_{:04}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.format_version(), 7);
        assert!(
            matches!(&seg.index, SegmentIndex::Monolithic(_)),
            "expected monolithic index for small segment"
        );

        // Point lookups still work
        for i in 0..10u32 {
            let k = kv_key(&format!("sm_{:04}", i));
            let e = seg.point_lookup(&k, u64::MAX).unwrap().unwrap();
            assert_eq!(e.value, Value::Int(i as i64));
        }

        // Full iteration works
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 10);
    }

    #[test]
    fn partitioned_index_owned_iter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_owned.sst");

        let mt = Memtable::new(0);
        let n = 800u32;
        for i in 0..n {
            let k = kv_key(&format!("oi_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = Arc::new(KVSegment::open(&path).unwrap());
        assert!(matches!(&seg.index, SegmentIndex::Partitioned { .. }));

        let iter = OwnedSegmentIter::new(seg);
        let all: Vec<_> = iter.collect();
        assert_eq!(all.len(), n as usize);

        // Verify correct ordering AND values
        for (idx, (_, se)) in all.iter().enumerate() {
            assert_eq!(
                se.value,
                Value::Int(idx as i64),
                "wrong value at idx {}",
                idx
            );
            assert_eq!(se.commit_id, idx as u64 + 1, "wrong commit at idx {}", idx);
        }
        for i in 0..all.len() - 1 {
            assert!(all[i].0.as_bytes() <= all[i + 1].0.as_bytes());
        }
    }

    #[test]
    fn format_version_7_in_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v7.sst");

        let mt = Memtable::new(0);
        mt.put(&kv_key("a"), 1, Value::Int(1), false);
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.format_version(), 7);
    }

    #[test]
    fn partitioned_index_mvcc_snapshot() {
        // Multiple versions of same key across partitioned segments
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_mvcc.sst");

        let mt = Memtable::new(0);
        // Write enough entries to force partitioning, with multiple versions
        for i in 0..500u32 {
            let k = kv_key(&format!("mv_{:06}", i));
            mt.put(&k, 1, Value::Int(i as i64), false); // version 1
            mt.put(&k, 5, Value::Int(i as i64 + 1000), false); // version 5
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(matches!(&seg.index, SegmentIndex::Partitioned { .. }));

        // Snapshot at commit 1 should see original values
        for i in 0..500u32 {
            let k = kv_key(&format!("mv_{:06}", i));
            let e = seg
                .point_lookup(&k, 1)
                .unwrap()
                .unwrap_or_else(|| panic!("key mv_{:06} not found at snapshot 1", i));
            assert_eq!(
                e.value,
                Value::Int(i as i64),
                "wrong value at snapshot 1 for key {}",
                i
            );
            assert_eq!(e.commit_id, 1);
        }

        // Snapshot at commit 5 should see updated values
        for i in 0..500u32 {
            let k = kv_key(&format!("mv_{:06}", i));
            let e = seg
                .point_lookup(&k, 5)
                .unwrap()
                .unwrap_or_else(|| panic!("key mv_{:06} not found at snapshot 5", i));
            assert_eq!(
                e.value,
                Value::Int(i as i64 + 1000),
                "wrong value at snapshot 5 for key {}",
                i
            );
            assert_eq!(e.commit_id, 5);
        }

        // Snapshot at commit 0 should find nothing
        let k = kv_key("mv_000000");
        assert!(seg.point_lookup(&k, 0).unwrap().is_none());
    }

    #[test]
    fn partitioned_index_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_tomb.sst");

        let mt = Memtable::new(0);
        // Write enough entries to force partitioning, with some tombstones
        for i in 0..500u32 {
            let k = kv_key(&format!("tb_{:06}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
            // Delete even-numbered keys at commit 2
            if i % 2 == 0 {
                mt.put(&k, 2, Value::Null, true);
            }
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(matches!(&seg.index, SegmentIndex::Partitioned { .. }));

        // At snapshot 2: even keys should be tombstones, odd keys should be values
        for i in 0..500u32 {
            let k = kv_key(&format!("tb_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("key tb_{:06} not found", i));
            if i % 2 == 0 {
                assert!(e.is_tombstone, "key {} should be tombstone", i);
                assert_eq!(e.commit_id, 2);
            } else {
                assert!(!e.is_tombstone, "key {} should not be tombstone", i);
                assert_eq!(e.value, Value::Int(i as i64));
                assert_eq!(e.commit_id, 1);
            }
        }

        // At snapshot 1: all keys should be non-tombstone values
        for i in (0..500u32).step_by(50) {
            let k = kv_key(&format!("tb_{:06}", i));
            let e = seg.point_lookup(&k, 1).unwrap().unwrap();
            assert!(!e.is_tombstone);
            assert_eq!(e.value, Value::Int(i as i64));
        }
    }

    #[test]
    fn partitioned_index_boundary_crossing_iter() {
        // Verify iteration seamlessly crosses partition boundaries by
        // confirming no entries are dropped at the boundary.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_boundary.sst");

        let mt = Memtable::new(0);
        // Use enough entries to get several partitions
        let n = 1500u32;
        for i in 0..n {
            let k = kv_key(&format!("bd_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(matches!(&seg.index, SegmentIndex::Partitioned { .. }));

        // Verify the top-level has multiple partitions
        let num_partitions = match &seg.index {
            SegmentIndex::Partitioned { top_level, .. } => top_level.len(),
            _ => panic!("expected partitioned"),
        };
        assert!(
            num_partitions >= 3,
            "expected at least 3 partitions, got {}",
            num_partitions
        );

        // Full iteration must yield exactly n entries with no gaps
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), n as usize);
        for (idx, (_, se)) in all.iter().enumerate() {
            assert_eq!(
                se.value,
                Value::Int(idx as i64),
                "gap or reorder at idx {}",
                idx
            );
        }

        // Compare SegmentIter vs OwnedSegmentIter — must produce identical results
        let owned_iter = OwnedSegmentIter::new(Arc::new(KVSegment::open(&path).unwrap()));
        let owned_all: Vec<_> = owned_iter.collect();
        assert_eq!(all.len(), owned_all.len());
        for (i, ((ik1, se1), (ik2, se2))) in all.iter().zip(owned_all.iter()).enumerate() {
            assert_eq!(ik1.as_bytes(), ik2.as_bytes(), "key mismatch at {}", i);
            assert_eq!(se1.value, se2.value, "value mismatch at {}", i);
        }
    }

    #[test]
    fn partitioned_footer_index_type_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        // Partitioned segment
        let path_part = dir.path().join("ft_part.sst");
        let mt = Memtable::new(0);
        for i in 0..1000u32 {
            let k = kv_key(&format!("ft_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_tiny_blocks(&mt, &path_part);

        let seg = KVSegment::open(&path_part).unwrap();
        assert!(seg.is_partitioned_index());

        // Monolithic segment
        let path_mono = dir.path().join("ft_mono.sst");
        let mt2 = Memtable::new(0);
        mt2.put(&kv_key("a"), 1, Value::Int(1), false);
        mt2.freeze();
        build_segment(&mt2, &path_mono);

        let seg2 = KVSegment::open(&path_mono).unwrap();
        assert!(!seg2.is_partitioned_index());
    }

    #[test]
    fn partitioned_index_boundary_at_129_blocks() {
        // 129 data blocks is the minimum for partitioned (128+1 → 2 chunks).
        // Use a block size that makes each entry fill exactly one block so we
        // can control the block count precisely.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part_129.sst");

        // With 64-byte block size + CompressionCodec::None, each entry (~70-80
        // bytes of InternalKey + value) forces a new block. We need at least
        // 129 entries → 129 blocks to trigger the 2-chunk threshold.
        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = kv_key(&format!("b129_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 64, // very small to ensure many blocks
            bloom_bits_per_key: 10,
            compression: crate::segment_builder::CompressionCodec::None,
            rate_limiter: None,
        };
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        // With 200 entries at 64B block size, we should definitely exceed 128 blocks
        assert!(
            matches!(&seg.index, SegmentIndex::Partitioned { top_level, .. } if top_level.len() >= 2),
            "expected partitioned index with >=2 partitions"
        );

        // All point lookups work
        for i in 0..200u32 {
            let k = kv_key(&format!("b129_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("key b129_{:06} not found", i));
            assert_eq!(e.value, Value::Int(i as i64));
        }

        // Full iteration yields all entries
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 200);
        for (idx, (_, se)) in all.iter().enumerate() {
            assert_eq!(se.value, Value::Int(idx as i64));
        }
    }

    // ===== Additional coverage tests =====

    #[test]
    fn multi_partition_bloom_rejects_absent_keys() {
        // Build a segment with many small blocks to force multiple bloom partitions,
        // then verify the bloom filter correctly rejects absent keys with low FPR.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_reject.sst");

        let mt = Memtable::new(0);
        for i in 0..2000u32 {
            mt.put(
                &kv_key(&format!("present_{:06}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert!(
            seg.bloom.index.len() > 1,
            "expected multiple bloom partitions, got {}",
            seg.bloom.index.len()
        );

        // All present keys must pass bloom check (no false negatives)
        for i in 0..2000u32 {
            assert!(
                seg.bloom_maybe_contains(&kv_key(&format!("present_{:06}", i))),
                "false negative for present_{:06}",
                i
            );
        }

        // Absent keys: measure false positive rate across partitions
        let mut false_positives = 0u32;
        let probe_count = 10_000u32;
        for i in 0..probe_count {
            if seg.bloom_maybe_contains(&kv_key(&format!("absent_{:06}", i))) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / probe_count as f64;
        assert!(
            fpr < 0.05,
            "bloom FPR across partitions too high: {:.4} ({} / {})",
            fpr,
            false_positives,
            probe_count,
        );
    }

    #[test]
    fn hash_index_collision_fallback_to_binary_search() {
        // Insert keys that are likely to share hash buckets (similar prefixes).
        // Verify all keys are still findable — the hash index must fall back to
        // binary search on collision and still locate the correct restart interval.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_collision.sst");

        let mt = Memtable::new(0);
        // Use keys with identical long prefix differing only in the last few bytes.
        // These will have similar xxh3 hashes and tend to collide in the hash index.
        for i in 0..500u32 {
            let k = kv_key(&format!("shared_prefix_collision_test_key_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.header.format_version, 7);

        // Every key must be findable despite hash collisions
        for i in 0..500u32 {
            let k = kv_key(&format!("shared_prefix_collision_test_key_{:06}", i));
            let e = seg.point_lookup(&k, u64::MAX).unwrap().unwrap_or_else(|| {
                panic!("key shared_prefix_collision_test_key_{:06} not found", i)
            });
            assert_eq!(e.value, Value::Int(i as i64));
            assert_eq!(e.commit_id, i as u64 + 1);
        }

        // Non-existent keys with similar prefix must not be found
        for i in 500..550u32 {
            let k = kv_key(&format!("shared_prefix_collision_test_key_{:06}", i));
            assert!(seg.point_lookup(&k, u64::MAX).unwrap().is_none());
        }
    }

    #[test]
    fn index_shortening_long_shared_prefix_lookup() {
        // Keys that share a long common prefix and differ only in the last
        // few bytes or in commit_id. Index key shortening must preserve enough
        // bytes for binary search to route to the correct block.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short_prefix.sst");

        let mt = Memtable::new(0);
        // Long shared prefix, tiny suffix difference
        for i in 0..300u32 {
            let k = kv_key(&format!(
                "very_long_shared_prefix_that_exercises_shortening_{:04}",
                i
            ));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        // Same key, multiple commit versions (differ only in commit_id)
        let multi_ver_key = kv_key("very_long_shared_prefix_that_exercises_shortening_0150");
        for commit in 2..=10u64 {
            mt.put(
                &multi_ver_key,
                commit,
                Value::Int(commit as i64 * 100),
                false,
            );
        }
        mt.freeze();
        build_segment_small_blocks(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // All keys must be retrievable
        for i in 0..300u32 {
            let k = kv_key(&format!(
                "very_long_shared_prefix_that_exercises_shortening_{:04}",
                i
            ));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("key ..._shortening_{:04} not found", i));
            // Key 150 was overwritten; others have their original value
            if i == 150 {
                assert_eq!(e.value, Value::Int(1000)); // commit 10 * 100
            } else {
                assert_eq!(e.value, Value::Int(i as i64));
            }
        }

        // MVCC: snapshot at commit 5 for the multi-version key
        let e = seg.point_lookup(&multi_ver_key, 5).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(500)); // commit 5 * 100

        // MVCC: snapshot at commit 1 gets the original value
        let e = seg.point_lookup(&multi_ver_key, 1).unwrap().unwrap();
        assert_eq!(e.value, Value::Int(150)); // original from loop
    }

    #[test]
    fn cache_local_bloom_fpr_measurement() {
        // End-to-end FPR measurement through the full segment bloom path
        // (build segment → open → bloom_maybe_contains).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fpr_measure.sst");

        let mt = Memtable::new(0);
        for i in 0..5000u32 {
            mt.put(
                &kv_key(&format!("fpr_{:06}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        build_segment(&mt, &path);

        let seg = KVSegment::open(&path).unwrap();

        // Measure FPR with keys guaranteed absent
        let mut fps = 0u32;
        let probes = 50_000u32;
        for i in 0..probes {
            if seg.bloom_maybe_contains(&kv_key(&format!("miss_{:06}", i))) {
                fps += 1;
            }
        }
        let fpr = fps as f64 / probes as f64;
        // At 10 bits/key, theoretical FPR ≈ 1%. Cache-local blocked bloom
        // may be slightly higher. Allow up to 2%.
        assert!(
            fpr < 0.02,
            "end-to-end bloom FPR too high: {:.4} ({} / {})",
            fpr,
            fps,
            probes,
        );
    }

    /// Issue #1765: OwnedSegmentIter with raw_values mode should carry
    /// pre-encoded bincode bytes through the entry, and a compaction
    /// round-trip using those raw bytes should produce identical values.
    #[test]
    fn test_issue_1765_compaction_raw_value_passthrough() {
        use crate::memtable::MemtableEntry;
        use crate::segment_builder::SegmentBuilder;
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let src_path = dir.path().join("src.sst");
        let dst_path = dir.path().join("dst.sst");

        // Build a segment with diverse value types
        let mt = Memtable::new(0);
        let ts = strata_core::Timestamp::from_micros(1_000_000);
        mt.put_entry(
            &kv_key("k_int"),
            1,
            MemtableEntry {
                value: Value::Int(42),
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 0,
                raw_value: None,
            },
        );
        mt.put_entry(
            &kv_key("k_str"),
            2,
            MemtableEntry {
                value: Value::String("hello world".to_string()),
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 0,
                raw_value: None,
            },
        );
        mt.put_entry(
            &kv_key("k_bytes"),
            3,
            MemtableEntry {
                value: Value::Bytes([0xDE, 0xAD, 0xBE, 0xEF, 0xFF].repeat(50)),
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 0,
                raw_value: None,
            },
        );
        mt.put_entry(
            &kv_key("k_tomb"),
            4,
            MemtableEntry {
                value: Value::Null,
                is_tombstone: true,
                timestamp: ts,
                ttl_ms: 0,
                raw_value: None,
            },
        );
        mt.put_entry(
            &kv_key("k_obj"),
            5,
            MemtableEntry {
                value: Value::object({
                    let mut m = HashMap::new();
                    m.insert("nested".to_string(), Value::Int(99));
                    m
                }),
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 0,
                raw_value: None,
            },
        );
        // Edge case: non-tombstone Value::Null (user stored null)
        mt.put_entry(
            &kv_key("k_null"),
            6,
            MemtableEntry {
                value: Value::Null,
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 0,
                raw_value: None,
            },
        );
        // Edge case: empty Bytes
        mt.put_entry(
            &kv_key("k_empty_bytes"),
            7,
            MemtableEntry {
                value: Value::Bytes(vec![]),
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 0,
                raw_value: None,
            },
        );
        // Edge case: entry with TTL (must be preserved through raw path)
        mt.put_entry(
            &kv_key("k_ttl"),
            8,
            MemtableEntry {
                value: Value::Int(99),
                is_tombstone: false,
                timestamp: strata_core::Timestamp::from_micros(2_000_000),
                ttl_ms: 30_000,
                raw_value: None,
            },
        );
        mt.freeze();

        let builder = SegmentBuilder::default();
        builder.build_from_iter(mt.iter_all(), &src_path).unwrap();

        // Read entries via OwnedSegmentIter with raw values enabled
        let seg = Arc::new(KVSegment::open(&src_path).unwrap());
        let iter = OwnedSegmentIter::new(Arc::clone(&seg)).with_raw_values();
        let entries: Vec<_> = iter.collect();

        assert_eq!(entries.len(), 8);

        // Non-tombstone entries must have raw_value set
        for (ik, se) in &entries {
            if se.is_tombstone {
                assert!(
                    se.raw_value.is_none(),
                    "tombstone should not have raw_value"
                );
            } else {
                assert!(
                    se.raw_value.is_some(),
                    "non-tombstone entry {:?} should have raw_value set",
                    ik.typed_key_prefix(),
                );
            }
        }

        // Build new segment from raw entries (simulating compaction passthrough)
        let raw_iter = entries
            .into_iter()
            .map(|(ik, se)| (ik, crate::segmented::segment_entry_to_memtable_entry(se)));
        builder.build_from_iter(raw_iter, &dst_path).unwrap();

        // Verify output segment has identical values
        let dst_seg = KVSegment::open(&dst_path).unwrap();
        let e = dst_seg
            .point_lookup(&kv_key("k_int"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(42));

        let e = dst_seg
            .point_lookup(&kv_key("k_str"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::String("hello world".to_string()));

        let e = dst_seg
            .point_lookup(&kv_key("k_bytes"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(
            e.value,
            Value::Bytes([0xDE, 0xAD, 0xBE, 0xEF, 0xFF].repeat(50))
        );

        let e = dst_seg
            .point_lookup(&kv_key("k_tomb"), u64::MAX)
            .unwrap()
            .unwrap();
        assert!(e.is_tombstone);

        let e = dst_seg
            .point_lookup(&kv_key("k_obj"), u64::MAX)
            .unwrap()
            .unwrap();
        let obj = e.value.as_object().unwrap();
        assert_eq!(obj.get("nested"), Some(&Value::Int(99)));

        // Edge case: non-tombstone Null preserved
        let e = dst_seg
            .point_lookup(&kv_key("k_null"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Null);
        assert!(!e.is_tombstone);

        // Edge case: empty Bytes preserved
        let e = dst_seg
            .point_lookup(&kv_key("k_empty_bytes"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Bytes(vec![]));

        // Edge case: TTL and timestamp preserved through raw path
        let e = dst_seg
            .point_lookup(&kv_key("k_ttl"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e.value, Value::Int(99));
        assert_eq!(e.timestamp, 2_000_000);
        assert_eq!(e.ttl_ms, 30_000);

        // Verify raw bytes match what bincode::serialize would produce
        let src_entries: Vec<_> = OwnedSegmentIter::new(Arc::clone(&seg)).collect();
        let raw_entries: Vec<_> = OwnedSegmentIter::new(Arc::clone(&seg))
            .with_raw_values()
            .collect();

        for ((_, normal), (_, raw)) in src_entries.iter().zip(raw_entries.iter()) {
            if !normal.is_tombstone {
                let expected_bytes = bincode::serialize(&normal.value).unwrap();
                assert_eq!(
                    raw.raw_value.as_ref().unwrap(),
                    &expected_bytes,
                    "raw_value bytes must match bincode::serialize output"
                );
            }
        }
    }
}
