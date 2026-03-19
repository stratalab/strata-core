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

use crate::block_cache::{self, Priority};
use crate::bloom::BloomFilter;
use crate::key_encoding::{encode_typed_key, encode_typed_key_prefix, InternalKey};
use crate::segment_builder::{
    decode_entry, decode_entry_header_ref, decode_entry_header_ref_v4, decode_entry_header_v4,
    decode_entry_v4, decode_entry_value, parse_filter_index, parse_footer, parse_framed_block,
    parse_header, parse_index_block, parse_properties_block, EntryHeader, FilterIndexEntry, Footer,
    IndexEntry, KVHeader, PropertiesBlock, FOOTER_SZ, FRAME_OVERHEAD, HEADER_SIZE,
};
use strata_core::types::Key;
use strata_core::value::Value;

use std::io;
use std::os::unix::fs::FileExt;
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

/// Partitioned bloom filter — loaded on demand through the block cache.
///
/// Only the filter index (small) is kept in memory. Individual ~4KB bloom
/// partitions are loaded from disk via pread + block cache on each lookup.
struct PartitionedBloom {
    /// Top-level index mapping key ranges → partition offsets.
    index: Vec<FilterIndexEntry>,
}

/// An immutable KV segment file backed by pread + block cache.
///
/// Opened via [`KVSegment::open`], provides point lookups and prefix iteration
/// with MVCC snapshot filtering.
pub struct KVSegment {
    /// Open file handle for pread I/O.
    file: std::fs::File,
    header: KVHeader,
    #[allow(dead_code)] // used by future compaction/GC
    footer: Footer,
    index: Vec<IndexEntry>,
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment file too small",
            ));
        }

        // Parse header via pread
        let header_buf = pread_exact(&file, 0, HEADER_SIZE)?;
        let header_bytes: &[u8; HEADER_SIZE] = header_buf
            .as_slice()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "header size mismatch"))?;
        let header = parse_header(header_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid segment header"))?;

        // Parse footer via pread (last FOOTER_SZ bytes)
        let footer_offset = file_size - FOOTER_SZ as u64;
        let footer_buf = pread_exact(&file, footer_offset, FOOTER_SZ)?;
        let footer_bytes: &[u8; FOOTER_SZ] = footer_buf
            .as_slice()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "footer size mismatch"))?;
        let footer = parse_footer(footer_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid segment footer (bad magic)",
            )
        })?;

        // Helper: validate block offset + length fits within the file.
        let check_block_bounds = |offset: u64, len: u32, name: &str| -> io::Result<(u64, usize)> {
            let end = offset.checked_add(len as u64).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} offset+len overflows", name),
                )
            })?;
            if end > file_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} extends past end of file", name),
                ));
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
        let (_, idx_data) = parse_framed_block(&idx_buf).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "index block CRC mismatch")
        })?;
        let index = parse_index_block(idx_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed index block"))?;

        // Parse filter index block via pread (v5 partitioned bloom)
        let (fi_off, fi_len) = check_block_bounds(
            footer.filter_block_offset,
            footer.filter_block_len,
            "filter index block",
        )?;
        let fi_buf = pread_exact(&file, fi_off, fi_len)?;
        let (_, fi_data) = parse_framed_block(&fi_buf).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "filter index block CRC mismatch",
            )
        })?;
        let filter_index = parse_filter_index(fi_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed filter index"))?;
        let bloom = PartitionedBloom {
            index: filter_index,
        };

        // Parse properties block via pread
        let (props_off, props_len) = check_block_bounds(
            footer.props_block_offset,
            footer.props_block_len,
            "properties block",
        )?;
        let props_buf = pread_exact(&file, props_off, props_len)?;
        let (_, props_data) = parse_framed_block(&props_buf).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "properties block CRC mismatch")
        })?;
        let props = parse_properties_block(props_data).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "malformed properties block")
        })?;

        let file_path = path.to_path_buf();
        let file_id = crate::block_cache::file_path_hash(&file_path);
        Ok(Self {
            file,
            header,
            footer,
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
    pub fn point_lookup(&self, key: &Key, snapshot_commit: u64) -> Option<SegmentEntry> {
        // 1. Bloom check
        if !self.bloom_maybe_contains(key) {
            return None;
        }

        let typed_key = encode_typed_key(key);

        // 2. Find candidate data block via binary search on index.
        //
        // Index keys are shortened separators (or full InternalKey bytes for
        // legacy/unshortened entries).  The seek key `(key, u64::MAX)` sorts
        // BEFORE all actual entries for that key due to descending commit_id
        // encoding.  Err(0) does NOT mean the key is absent — it may be in
        // block 0.  The forward scan below handles off-by-one from the binary
        // search when index keys are separators (upper bounds) rather than
        // first keys.
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

            // Check whether we can stop scanning.  Index keys are shortened
            // separators: index[bi-1] is an upper bound for block bi-1 and a
            // lower bound for block bi.  If its prefix is already past our
            // typed_key, no later block can contain the key.
            //
            // This works for both shortened separators AND legacy full-key
            // index entries (raw byte comparison is valid for both).
            if bi > block_idx {
                let prev_key = &self.index[bi - 1].key;
                if prev_key.len() >= 8 {
                    let prefix = &prev_key[..prev_key.len() - 8];
                    if prefix > typed_key.as_slice() {
                        break;
                    }
                } else {
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
            block_data_end: 0,
            block_data: None,
            done: false,
            prev_key: Vec::new(),
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

    /// Format version from the segment header.
    #[cfg(test)]
    pub(crate) fn format_version(&self) -> u16 {
        self.header.format_version
    }

    /// Pin all bloom partitions in the block cache with Pinned priority.
    ///
    /// Called for L0 segments so their bloom partitions are never evicted.
    pub fn pin_bloom_partitions(&self) {
        let cache = block_cache::global_cache();
        for entry in &self.bloom.index {
            if cache.get(self.file_id, entry.block_offset).is_none() {
                // Load partition from disk and insert as Pinned
                if let Ok(raw) = pread_exact(
                    &self.file,
                    entry.block_offset,
                    entry.block_data_len as usize,
                ) {
                    if let Some((_, data)) = parse_framed_block(&raw) {
                        cache.insert_with_priority(
                            self.file_id,
                            entry.block_offset,
                            data.to_vec(),
                            Priority::Pinned,
                        );
                    }
                }
            }
            // Unconditionally promote: handles both the case where the entry
            // was already cached (at High from a prior lookup) and the TOCTOU
            // race where a concurrent bloom check inserted at High between
            // our get() and insert_with_priority() above.
            cache.promote_to_pinned(self.file_id, entry.block_offset);
        }
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
        let entry = &self.bloom.index[idx];

        // Load partition from block cache (cache miss → pread)
        let cache = block_cache::global_cache();
        let data = if let Some(cached) = cache.get(self.file_id, entry.block_offset) {
            cached
        } else {
            // Cache miss: pread from file
            let raw = match pread_exact(
                &self.file,
                entry.block_offset,
                entry.block_data_len as usize,
            ) {
                Ok(buf) => buf,
                Err(_) => return true, // I/O error → assume might exist
            };
            let (_, data) = match parse_framed_block(&raw) {
                Some(parsed) => parsed,
                None => return true, // corrupt → assume might exist
            };
            cache.insert_with_priority(
                self.file_id,
                entry.block_offset,
                data.to_vec(),
                Priority::High,
            )
        };

        match BloomFilter::from_bytes(&data) {
            Some(bloom) => bloom.maybe_contains(typed_key),
            None => true, // malformed bloom → assume might exist
        }
    }

    /// Read and verify a data block, checking the global block cache first.
    ///
    /// On cache hit, returns the cached decompressed block (zero decompression cost).
    /// On cache miss, reads from file via pread, decompresses if needed, caches, and returns.
    fn read_data_block(&self, ie: &IndexEntry) -> Option<Arc<Vec<u8>>> {
        let cache = crate::block_cache::global_cache();
        let block_offset = ie.block_offset;

        // Check cache first
        if let Some(cached) = cache.get(self.file_id, block_offset) {
            return Some(cached);
        }

        // Cache miss: pread from file and decompress
        let framed_len = FRAME_OVERHEAD + ie.block_data_len as usize;
        let raw = match pread_exact(&self.file, block_offset, framed_len) {
            Ok(buf) => buf,
            Err(e) => {
                tracing::warn!(
                    path = %self.file_path.display(),
                    offset = block_offset,
                    len = framed_len,
                    error = %e,
                    "pread failed reading data block"
                );
                return None;
            }
        };

        let codec_byte = raw[1];
        let (_, data) = parse_framed_block(&raw)?;

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
    /// For v3 blocks, binary-searches the restart point array to skip most
    /// entries, then linear-scans only the ~16-entry interval.
    fn scan_block_for_key(
        &self,
        ie: &IndexEntry,
        typed_key: &[u8],
        snapshot_commit: u64,
    ) -> Option<SegmentEntry> {
        let block_data = self.read_data_block(ie)?;
        let data = &**block_data;
        let fv = self.header.format_version;

        // Strip hash index if present (v6+)
        let (block, hash_index) = strip_hash_index(data, fv);

        // Determine scan bounds based on format version
        let (scan_start, data_end) = if fv >= 3 {
            if let Some((de, num_restarts)) = parse_restart_trailer(block) {
                let start = if let Some(ref hi) = hash_index {
                    // O(1) hash lookup for the restart interval
                    let h = xxhash_rust::xxh3::xxh3_64(typed_key) as usize;
                    let bucket = h % hi.num_buckets;
                    let interval = hi.buckets[bucket];
                    if interval != 0xFF && (interval as usize) < num_restarts {
                        restart_offset_at(block, de, interval as usize) as usize
                    } else {
                        // Empty bucket or out of range: fall back to binary search
                        binary_search_restarts(block, de, num_restarts, typed_key, fv)
                    }
                } else {
                    binary_search_restarts(block, de, num_restarts, typed_key, fv)
                };
                (start.min(de), de)
            } else {
                // Malformed trailer — fall back to full linear scan
                (0, block.len())
            }
        } else {
            (0, block.len())
        };

        if fv >= 4 {
            self.linear_scan_block_v4(data, scan_start, data_end, typed_key, snapshot_commit)
        } else {
            self.linear_scan_block(data, scan_start, data_end, typed_key, snapshot_commit)
        }
    }

    /// Linear scan entries in `data[start..end]` for `typed_key` at or below `snapshot_commit`.
    fn linear_scan_block(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        typed_key: &[u8],
        snapshot_commit: u64,
    ) -> Option<SegmentEntry> {
        let mut pos = start;
        while pos < end {
            let ref_header = decode_entry_header_ref(&data[pos..end])?;
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
                let header = EntryHeader {
                    ik: InternalKey::try_from_bytes(ref_header.ik_bytes.to_vec())?,
                    is_tombstone: ref_header.is_tombstone,
                    timestamp: ref_header.timestamp,
                    ttl_ms: ref_header.ttl_ms,
                    value_start: ref_header.value_start,
                    value_len: ref_header.value_len,
                    total_len: ref_header.total_len,
                };
                let value = decode_entry_value(&data[entry_data_start..end], &header)?;
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
            if prev_key.len() < 8 {
                return None;
            }
            let tkp = &prev_key[..prev_key.len() - 8];
            if tkp != typed_key {
                if tkp > typed_key {
                    break;
                }
                continue;
            }

            let len = prev_key.len();
            let commit_id = !u64::from_be_bytes(prev_key[len - 8..].try_into().ok()?);
            if commit_id <= snapshot_commit {
                let ik = InternalKey::try_from_bytes(prev_key.clone())?;
                let header = EntryHeader {
                    ik,
                    is_tombstone: hdr.is_tombstone,
                    timestamp: hdr.timestamp,
                    ttl_ms: hdr.ttl_ms,
                    value_start: hdr.value_start,
                    value_len: hdr.value_len,
                    total_len: hdr.total_len,
                };
                let value = decode_entry_value(&data[entry_start..end], &header)?;
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
            block_data_end: 0,
            block_data: None,
            done: self.index.is_empty(),
            prev_key: Vec::new(),
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

/// Read the i-th restart offset from the trailer.
#[inline]
fn restart_offset_at(data: &[u8], data_end: usize, index: usize) -> u32 {
    let off = data_end + index * 4;
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
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
    format_version: u16,
) -> usize {
    let mut left = 0usize;
    let mut right = num_restarts - 1;

    while left < right {
        let mid = (left + right + 1) / 2;
        let offset = restart_offset_at(data, data_end, mid) as usize;
        if offset < data_end {
            // At restart points, shared=0 always, so key is self-contained.
            // Dispatch to v3 or v4 decoder for zero-copy key access.
            let cmp = if format_version >= 4 {
                decode_entry_header_ref_v4(&data[offset..data_end])
                    .map(|hdr| hdr.typed_key_prefix() < typed_key)
            } else {
                decode_entry_header_ref(&data[offset..data_end])
                    .map(|hdr| hdr.typed_key_prefix() < typed_key)
            };
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

    let start = restart_offset_at(data, data_end, left) as usize;
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
fn strip_hash_index(data: &[u8], format_version: u16) -> (&[u8], Option<HashIndex<'_>>) {
    // Hash index only in v6+, minimum size: 1 bucket + 2 (num_buckets) + 1 (sentinel) = 4
    // Plus at least 4 bytes for the restart trailer before it.
    if format_version < 6 || data.len() < 8 || data[data.len() - 1] != 0x01 {
        return (data, None);
    }
    let num_buckets =
        u16::from_le_bytes(data[data.len() - 3..data.len() - 1].try_into().unwrap()) as usize;
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

/// Compute `data_end` for a block, handling both v2 (no trailer) and v3+ (with trailer).
/// For v6+, also strips the hash index before parsing the restart trailer.
fn block_data_end(data: &[u8], format_version: u16) -> usize {
    if format_version >= 3 {
        let (block, _) = strip_hash_index(data, format_version);
        if let Some((de, _)) = parse_restart_trailer(block) {
            return de;
        }
    }
    data.len()
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
    /// End of entry data in the current block (excludes restart trailer for v3+).
    block_data_end: usize,
    block_data: Option<Arc<Vec<u8>>>,
    done: bool,
    /// Buffer for v4 prefix-compressed key reconstruction (reused across entries).
    prev_key: Vec<u8>,
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
                        let de = block_data_end(&data, self.segment.header.format_version);
                        self.block_data_end = de;
                        self.block_data = Some(data);
                        self.block_offset = 0;
                        self.prev_key.clear();
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }

            let data = self.block_data.as_ref().unwrap();

            if self.block_offset >= self.block_data_end {
                // Move to next block
                self.block_data = None;
                self.block_idx += 1;
                continue;
            }

            let fv = self.segment.header.format_version;
            let result = if fv >= 4 {
                decode_entry_v4(
                    &data[self.block_offset..self.block_data_end],
                    &mut self.prev_key,
                )
            } else {
                decode_entry(&data[self.block_offset..self.block_data_end])
            };

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
    block_idx: usize,
    block_offset: usize,
    /// End of entry data in the current block (excludes restart trailer for v3+).
    block_data_end: usize,
    block_data: Option<Arc<Vec<u8>>>,
    done: bool,
    /// Buffer for v4 prefix-compressed key reconstruction (reused across entries).
    prev_key: Vec<u8>,
}

impl OwnedSegmentIter {
    /// Create a streaming iterator over all entries in the segment.
    pub fn new(segment: Arc<KVSegment>) -> Self {
        let done = segment.index.is_empty();
        Self {
            segment,
            block_idx: 0,
            block_offset: 0,
            block_data_end: 0,
            block_data: None,
            done,
            prev_key: Vec::new(),
        }
    }
}

impl OwnedSegmentIter {
    /// Return the current data block index (used by `ThrottledSegmentIter`).
    pub(crate) fn current_block_idx(&self) -> usize {
        self.block_idx
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
                if self.block_idx >= self.segment.index.len() {
                    self.done = true;
                    return None;
                }
                let ie = &self.segment.index[self.block_idx];
                match self.segment.read_data_block(ie) {
                    Some(data) => {
                        let de = block_data_end(&data, self.segment.header.format_version);
                        self.block_data_end = de;
                        self.block_data = Some(data);
                        self.block_offset = 0;
                        self.prev_key.clear();
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }

            let data = self.block_data.as_ref().unwrap();

            if self.block_offset >= self.block_data_end {
                self.block_data = None;
                self.block_idx += 1;
                continue;
            }

            let fv = self.segment.header.format_version;
            let result = if fv >= 4 {
                decode_entry_v4(
                    &data[self.block_offset..self.block_data_end],
                    &mut self.prev_key,
                )
            } else {
                decode_entry(&data[self.block_offset..self.block_data_end])
            };

            match result {
                Some((ik, is_tomb, value, timestamp, ttl_ms, consumed)) => {
                    self.block_offset += consumed;
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
                    self.done = true;
                    return None;
                }
            }
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
            compression: crate::segment_builder::CompressionCodec::default(),
            rate_limiter: None,
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
        let e = seg.point_lookup(&kv_key("k_0005"), u64::MAX).unwrap();
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
        let e = seg.point_lookup(&kv_key("k_0045"), u64::MAX).unwrap();
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
        let e = seg.point_lookup(&kv_key("k_0016"), u64::MAX).unwrap();
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
        let e = seg.point_lookup(&kv_key("b_0000"), 5).unwrap();
        assert_eq!(e.value, Value::Int(50));
        assert_eq!(e.commit_id, 5);

        // Snapshot at 3: see version 3
        let e = seg.point_lookup(&kv_key("b_0000"), 3).unwrap();
        assert_eq!(e.value, Value::Int(30));
        assert_eq!(e.commit_id, 3);

        // Snapshot at 1: see version 1
        let e = seg.point_lookup(&kv_key("b_0000"), 1).unwrap();
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
        assert!(seg.point_lookup(&kv_key("zzz_missing"), u64::MAX).is_none());
        assert!(seg.point_lookup(&kv_key("aaa_missing"), u64::MAX).is_none());
    }

    #[test]
    fn v3_iter_respects_data_end() {
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
    fn v3_multi_block_binary_search() {
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
        let e = seg.point_lookup(&kv_key("key_000000"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 1);

        let e = seg.point_lookup(&kv_key("key_000250"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 251);

        let e = seg.point_lookup(&kv_key("key_000499"), u64::MAX).unwrap();
        assert_eq!(e.commit_id, 500);

        assert!(seg.point_lookup(&kv_key("key_999999"), u64::MAX).is_none());

        // Verify iteration yields all entries
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 500);
    }

    #[test]
    fn v2_fallback_linear_scan() {
        // Build a v3-format segment, then patch the header to v2 to simulate a legacy file.
        // The reader must fall back to linear scan and still return correct results.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2_compat.sst");

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
        // Use v3 builder so block data uses the old uncompressed-key format
        let builder = SegmentBuilder::default();
        builder.build_from_iter_v3(mt.iter_all(), &path).unwrap();

        // Patch format_version in the header from 3 to 2 (bytes 8..10 LE)
        let mut raw = std::fs::read(&path).unwrap();
        raw[8] = 2;
        raw[9] = 0;
        let v2_path = dir.path().join("v2_patched.sst");
        std::fs::write(&v2_path, &raw).unwrap();

        let seg = KVSegment::open(&v2_path).unwrap();
        assert_eq!(seg.header.format_version, 2);

        // Point lookups still work (linear scan ignores restart trailer)
        let e = seg.point_lookup(&kv_key("k_0005"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(5));

        let e = seg.point_lookup(&kv_key("k_0045"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(45));

        assert!(seg.point_lookup(&kv_key("zzz"), u64::MAX).is_none());

        // Iteration still works — but will include restart trailer bytes as
        // "entries" and fail to parse them, stopping iteration early. For a
        // genuine v2 file (no trailer), iteration would yield all entries.
        // With a patched v2 header over v3 data, the iterator hits the trailer
        // and stops. This is expected and safe — it returns a truncated but
        // correct prefix of entries.
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
            };
            (ik.clone(), me)
        });
        let results = splitter
            .build_split(iter, |idx| out_dir.join(format!("{:06}.sst", idx)))
            .unwrap();

        // Verify output is v4 and readable
        for (p, meta) in &results {
            let out_seg = KVSegment::open(p).unwrap();
            assert_eq!(out_seg.header.format_version, 6);
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
        assert_eq!(seg.header.format_version, 6);

        // Point lookup every entry via binary search
        for i in 0..500u32 {
            let k = kv_key(&format!("key_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap_or_else(|| panic!("missing key_{:06}", i));
            assert_eq!(e.value, Value::String(format!("val_{}", "x".repeat(50))));
            assert_eq!(e.commit_id, i as u64 + 1);
        }

        // Non-existent key
        assert!(seg.point_lookup(&kv_key("key_999999"), u64::MAX).is_none());
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
        assert_eq!(seg.header.format_version, 6);

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

    #[test]
    fn v4_compaction_mixed_versions() {
        // Read a v3 segment via OwnedSegmentIter, write v4 via build_from_iter
        use crate::segment_builder::SegmentBuilder;

        let dir = tempfile::tempdir().unwrap();
        let v3_path = dir.path().join("v3_input.sst");

        let mt = Memtable::new(0);
        for i in 0..50u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        let builder = SegmentBuilder::default();
        builder.build_from_iter_v3(mt.iter_all(), &v3_path).unwrap();

        let v3_seg = Arc::new(KVSegment::open(&v3_path).unwrap());
        assert_eq!(v3_seg.header.format_version, 3);

        // Collect entries via OwnedSegmentIter (v3 read path)
        let entries: Vec<_> = OwnedSegmentIter::new(v3_seg).collect();
        assert_eq!(entries.len(), 50);

        // Write as v4
        let v4_path = dir.path().join("v4_output.sst");
        let iter = entries.iter().map(|(ik, se)| {
            let me = crate::memtable::MemtableEntry {
                value: se.value.clone(),
                is_tombstone: se.is_tombstone,
                timestamp: strata_core::Timestamp::from_micros(se.timestamp),
                ttl_ms: se.ttl_ms,
            };
            (ik.clone(), me)
        });
        let builder = SegmentBuilder::default();
        builder.build_from_iter(iter, &v4_path).unwrap();

        let v4_seg = KVSegment::open(&v4_path).unwrap();
        assert_eq!(v4_seg.header.format_version, 6);

        // Verify all entries readable
        for i in 0..50u32 {
            let k = kv_key(&format!("k_{:04}", i));
            let e = v4_seg.point_lookup(&k, u64::MAX).unwrap();
            assert_eq!(e.value, Value::Int(i as i64));
        }
    }

    #[test]
    fn v3_backward_compat() {
        // Build a v3-format segment and verify it reads correctly with new code
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v3_compat.sst");

        let mt = Memtable::new(0);
        for i in 0..50u32 {
            mt.put(
                &kv_key(&format!("k_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();
        let builder = SegmentBuilder::default();
        builder.build_from_iter_v3(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.header.format_version, 3);

        // Point lookups
        for i in 0..50u32 {
            let k = kv_key(&format!("k_{:04}", i));
            let e = seg.point_lookup(&k, u64::MAX).unwrap();
            assert_eq!(e.value, Value::Int(i as i64));
        }

        // Iteration
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 50);
        for i in 1..all.len() {
            assert!(all[i - 1].0 < all[i].0);
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
            let e = seg.point_lookup(&k, u64::MAX);
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
        assert_eq!(seg.header.format_version, 6);
        // Must span multiple blocks
        assert!(seg.index.len() > 1, "expected multi-block segment");

        // Lookup every key from first, middle, and last blocks
        for i in 0..500u32 {
            let k = kv_key(&format!("hk_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
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
        assert!(seg.point_lookup(&kv_key("aaa"), u64::MAX).is_none()); // before all
        assert!(seg.point_lookup(&kv_key("exist_0050x"), u64::MAX).is_none()); // between
        assert!(seg.point_lookup(&kv_key("zzz"), u64::MAX).is_none()); // after all
                                                                       // Keys with similar prefixes to stress hash collisions
        for i in 0..50u32 {
            let k = kv_key(&format!("exist_{:04}_GHOST", i));
            assert!(
                seg.point_lookup(&k, u64::MAX).is_none(),
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
                .unwrap_or_else(|| panic!("key col_{:06} not found", i));
            assert_eq!(e.value, Value::Int(i as i64));
        }

        // (b) Absent keys in the same key-space must not be found
        for i in 500..600u32 {
            let k = kv_key(&format!("col_{:06}", i));
            assert!(
                seg.point_lookup(&k, u64::MAX).is_none(),
                "absent key col_{:06} should not be found",
                i
            );
        }
    }

    #[test]
    fn hash_index_space_overhead() {
        // Verify hash index adds roughly 4-6% overhead per block.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash_size.sst");

        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = kv_key(&format!("sz_{:06}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();

        // Build v6 (with hash index)
        let builder = SegmentBuilder::default();
        let v6_meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();

        // Build v3 (without hash index) for comparison
        let v3_path = dir.path().join("v3_size.sst");
        let v3_meta = builder.build_from_iter_v3(mt.iter_all(), &v3_path).unwrap();

        // Hash index overhead should be under 10%
        let overhead = v6_meta.file_size as f64 / v3_meta.file_size as f64;
        assert!(
            overhead < 1.10,
            "hash index overhead too large: v6={}, v3={}, ratio={:.3}",
            v6_meta.file_size,
            v3_meta.file_size,
            overhead,
        );
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
        let e = seg.point_lookup(&k, 1).unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert_eq!(e.commit_id, 1);

        let e = seg.point_lookup(&k, 25).unwrap();
        assert_eq!(e.value, Value::Int(250));
        assert_eq!(e.commit_id, 25);

        let e = seg.point_lookup(&k, u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(500));
        assert_eq!(e.commit_id, 50);

        // snapshot_commit=0 → no version visible
        assert!(seg.point_lookup(&k, 0).is_none());
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
        let e = seg.point_lookup(&kv_key("only"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(42));
        // Absent key in single-entry segment
        assert!(seg.point_lookup(&kv_key("nope"), u64::MAX).is_none());
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
        assert!(seg.index.len() > 1, "expected multi-block segment");

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

        let e = seg.point_lookup(&kv_key("alive"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(1));
        assert!(!e.is_tombstone);

        let e = seg.point_lookup(&kv_key("dead"), u64::MAX).unwrap();
        assert!(e.is_tombstone);
        assert_eq!(e.commit_id, 2);

        // "dead" at snapshot 1 → see the value
        let e = seg.point_lookup(&kv_key("dead"), 1).unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert!(!e.is_tombstone);

        // "revived" at latest → see the re-put
        let e = seg.point_lookup(&kv_key("revived"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(30));
        assert!(!e.is_tombstone);

        // "revived" at snapshot 2 → see tombstone
        let e = seg.point_lookup(&kv_key("revived"), 2).unwrap();
        assert!(e.is_tombstone);
    }

    #[test]
    fn hash_index_v3_backward_compat() {
        // A v3 segment (no hash index) must still be readable by the v6 reader.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v3_compat.sst");

        let mt = Memtable::new(0);
        for i in 0..50u32 {
            let k = kv_key(&format!("bc_{:04}", i));
            mt.put(&k, 1, Value::Int(i as i64), false);
        }
        mt.freeze();

        let builder = SegmentBuilder::default();
        builder.build_from_iter_v3(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.header.format_version, 3);

        // Point lookups must work (binary search path, no hash index)
        for i in 0..50u32 {
            let k = kv_key(&format!("bc_{:04}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap_or_else(|| panic!("v3 key bc_{:04} not found", i));
            assert_eq!(e.value, Value::Int(i as i64));
        }

        // Iterator must work
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 50);
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
}
