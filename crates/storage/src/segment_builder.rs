//! Segment builder — flushes sorted entries into an immutable KV segment file.
//!
//! Takes a sorted iterator of `(InternalKey, MemtableEntry)` and produces a
//! segment file with the layout:
//!
//! ```text
//! | KVHeader (64 bytes)       |
//! | DataBlock 0..N-1          |
//! | SubIndexBlock 0..P-1      |  (v7 partitioned only; omitted for monolithic)
//! | BloomPartition 0..K-1     |
//! | FilterIndexBlock          |
//! | TopLevelIndex / IndexBlock|
//! | PropertiesBlock           |
//! | Footer (56 bytes)         |
//! ```
//!
//! Each block is framed with a type byte, codec byte, length, and CRC32.

use crate::bloom::BloomFilter;
use crate::key_encoding::InternalKey;
use crate::memtable::MemtableEntry;
use strata_core::value::Value;

use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes for KV segment header: "STRAKV\0\0"
const KV_HEADER_MAGIC: [u8; 8] = *b"STRAKV\0\0";

/// Magic bytes for segment footer: "STRAKEND"
const FOOTER_MAGIC: [u8; 8] = *b"STRAKEND";

/// Current format version.
const FORMAT_VERSION: u16 = 7;

/// Number of entries between restart points within a data block.
/// Entry 0 always gets a restart point; subsequent restarts are placed every
/// RESTART_INTERVAL entries. Binary search over restart points replaces
/// a full linear scan, reducing from ~30 to ~4+8 comparisons per lookup.
const RESTART_INTERVAL: usize = 16;

/// Fixed header size in bytes.
const KV_HEADER_SIZE: usize = 64;

/// Fixed footer size in bytes.
const FOOTER_SIZE: usize = 56;

/// Block type tags.
const BLOCK_TYPE_DATA: u8 = 1;
const BLOCK_TYPE_INDEX: u8 = 2;
const BLOCK_TYPE_FILTER: u8 = 3;
const BLOCK_TYPE_PROPS: u8 = 4;
const BLOCK_TYPE_FILTER_INDEX: u8 = 5;
const BLOCK_TYPE_SUB_INDEX: u8 = 6;

/// Number of data blocks per sub-index partition (partitioned index).
const INDEX_PARTITION_BLOCK_COUNT: usize = 128;

/// Index type tags for footer.
const INDEX_TYPE_MONOLITHIC: u8 = 0;
const INDEX_TYPE_PARTITIONED: u8 = 1;

/// Number of data blocks per bloom partition.
const BLOOM_PARTITION_BLOCK_COUNT: usize = 16;

/// Block frame overhead: type(1) + codec(1) + reserved(2) + data_len(4) + crc32(4) = 12
const BLOCK_FRAME_OVERHEAD: usize = 12;

/// Value kind tags within data entries.
const VALUE_KIND_PUT: u8 = 1;
const VALUE_KIND_DEL: u8 = 2;

// ---------------------------------------------------------------------------
// SegmentMeta — returned after building a segment
// ---------------------------------------------------------------------------

/// Metadata about a built segment, returned by [`SegmentBuilder::build_from_iter`].
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// Number of entries (all versions) in the segment.
    pub entry_count: u64,
    /// Minimum commit_id in the segment.
    pub commit_min: u64,
    /// Maximum commit_id in the segment.
    pub commit_max: u64,
    /// File size in bytes.
    pub file_size: u64,
}

// ---------------------------------------------------------------------------
// CompressionCodec
// ---------------------------------------------------------------------------

/// Compression strategy for data blocks within a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    /// No compression (codec byte = 0x00). Best for hot levels (L0-L2).
    None,
    /// Zstd compression at the given level. Level 3 for warm (L3-L5), level 6 for cold (L6).
    Zstd(i32),
}

impl Default for CompressionCodec {
    fn default() -> Self {
        CompressionCodec::Zstd(3)
    }
}

// ---------------------------------------------------------------------------
// SegmentBuilder
// ---------------------------------------------------------------------------

/// Builds an immutable KV segment file from sorted entries.
pub struct SegmentBuilder {
    /// Target data block size in bytes (before framing).
    pub data_block_size: usize,
    /// Bloom filter bits per key.
    pub bloom_bits_per_key: usize,
    /// Compression codec for data blocks.
    pub compression: CompressionCodec,
    /// Optional rate limiter for throttling data block writes (compaction).
    pub rate_limiter: Option<std::sync::Arc<crate::rate_limiter::RateLimiter>>,
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        Self {
            data_block_size: 64 * 1024, // 64 KiB
            bloom_bits_per_key: 10,
            compression: CompressionCodec::default(),
            rate_limiter: None,
        }
    }
}

impl SegmentBuilder {
    /// Set the bloom filter bits per key.
    pub fn with_bloom_bits(mut self, bits_per_key: usize) -> Self {
        self.bloom_bits_per_key = bits_per_key;
        self
    }

    /// Set the compression codec for data blocks.
    pub fn with_compression(mut self, codec: CompressionCodec) -> Self {
        self.compression = codec;
        self
    }

    /// Set a rate limiter for throttling data block writes during compaction.
    pub fn with_rate_limiter(
        mut self,
        limiter: std::sync::Arc<crate::rate_limiter::RateLimiter>,
    ) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    /// Build a segment file from a sorted iterator of memtable entries.
    ///
    /// Writes to a temporary file then atomically renames to `path`.
    /// The iterator MUST yield entries in `InternalKey` order (ascending).
    pub fn build_from_iter<I>(&self, iter: I, path: &Path) -> io::Result<SegmentMeta>
    where
        I: Iterator<Item = (InternalKey, MemtableEntry)>,
    {
        let parent = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;

        let tmp_path = path.with_extension("tmp");
        let file = std::fs::File::create(&tmp_path)?;
        let mut w = BufWriter::new(file);

        // 1. Write placeholder header (backfill later)
        let header_placeholder = [0u8; KV_HEADER_SIZE];
        w.write_all(&header_placeholder)?;

        // Accumulators
        let mut entry_count: u64 = 0;
        let mut commit_min: u64 = u64::MAX;
        let mut commit_max: u64 = 0;
        let mut first_key: Option<Vec<u8>> = None;
        let mut last_key: Option<Vec<u8>> = None;

        // Current data block buffer
        let mut block_buf = Vec::with_capacity(self.data_block_size + 1024);

        // Index entries: (first_key_bytes, block_offset, block_data_len)
        let mut index_entries: Vec<(Vec<u8>, u64, u32)> = Vec::new();

        // Partitioned bloom: keys per partition, max typed_key per partition
        let mut partition_bloom_keys: Vec<Vec<Vec<u8>>> = vec![Vec::new()];
        let mut partition_max_keys: Vec<Vec<u8>> = vec![Vec::new()];
        let mut last_typed_key: Option<Vec<u8>> = None;
        let mut data_block_count: usize = 0;

        // Track current block's first key
        let mut block_first_key: Option<Vec<u8>> = None;

        // Restart point tracking
        let mut block_entry_count: usize = 0;
        let mut restart_offsets: Vec<u32> = vec![0]; // entry 0 always at offset 0

        // v4 prefix compression state
        let mut prev_key: Vec<u8> = Vec::new();

        // Hash index: (xxh3_hash, restart_interval) per unique typed_key in block
        let mut block_hash_entries: Vec<(u64, usize)> = Vec::new();
        let mut block_typed_key_for_hash: Option<Vec<u8>> = None;

        let mut file_offset = KV_HEADER_SIZE as u64;

        // Pending index entry from previous block: (last_key, offset, data_len).
        // We defer index entry creation until the next block's first key is known,
        // so we can shorten the index key to the shortest separator.
        let mut pending_index: Option<(Vec<u8>, u64, u32)> = None;

        for (ik, entry) in iter {
            let commit_id = ik.commit_id();
            commit_min = commit_min.min(commit_id);
            commit_max = commit_max.max(commit_id);

            if first_key.is_none() {
                first_key = Some(ik.as_bytes().to_vec());
            }
            last_key = Some(ik.as_bytes().to_vec());

            // Collect unique typed keys for partitioned bloom filter
            let typed = ik.typed_key_prefix().to_vec();
            if last_typed_key.as_ref() != Some(&typed) {
                partition_bloom_keys.last_mut().unwrap().push(typed.clone());
                *partition_max_keys.last_mut().unwrap() = typed.clone();
                last_typed_key = Some(typed);
            }

            if block_first_key.is_none() {
                block_first_key = Some(ik.as_bytes().to_vec());
            }

            // Record restart point if this entry starts a new interval
            if block_entry_count > 0 && block_entry_count % RESTART_INTERVAL == 0 {
                restart_offsets.push(block_buf.len() as u32);
            }

            // Encode entry into block buffer (v4 prefix compression)
            let is_restart = block_entry_count == 0 || block_entry_count % RESTART_INTERVAL == 0;
            encode_entry_v4(&prev_key, &ik, &entry, &mut block_buf, is_restart);
            prev_key.clear();
            prev_key.extend_from_slice(ik.as_bytes());

            // Track hash index entry (before incrementing block_entry_count)
            let restart_interval = block_entry_count / RESTART_INTERVAL;
            let tkp = ik.typed_key_prefix();
            if block_typed_key_for_hash.as_deref() != Some(tkp) {
                let hash = xxhash_rust::xxh3::xxh3_64(tkp);
                block_hash_entries.push((hash, restart_interval));
                block_typed_key_for_hash = Some(tkp.to_vec());
            }

            entry_count += 1;
            block_entry_count += 1;

            // Flush block when it reaches target size
            if block_buf.len() >= self.data_block_size {
                // Append restart trailer and hash index before compression
                append_restart_trailer(&mut block_buf, &restart_offsets);
                append_hash_index(&mut block_buf, &block_hash_entries);

                let bfk = block_first_key.take().unwrap();
                let block_last = prev_key.clone(); // last key written to this block

                // Resolve pending entry from previous block
                if let Some((pending_last, offset, len)) = pending_index.take() {
                    let shortened = shorten_index_key(&pending_last, &bfk);
                    index_entries.push((shortened, offset, len));
                }

                let framed_size = write_framed_block_compressed(
                    &mut w,
                    BLOCK_TYPE_DATA,
                    &block_buf,
                    self.compression,
                )?;
                let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
                if let Some(ref limiter) = self.rate_limiter {
                    limiter.acquire(framed_size as u64);
                }
                pending_index = Some((block_last, file_offset, on_disk_data_len));
                file_offset += framed_size as u64;
                block_buf.clear();
                restart_offsets.clear();
                restart_offsets.push(0);
                block_entry_count = 0;
                prev_key.clear();
                block_hash_entries.clear();
                block_typed_key_for_hash = None;

                data_block_count += 1;

                // Rotate to a new bloom partition every BLOOM_PARTITION_BLOCK_COUNT blocks
                if data_block_count % BLOOM_PARTITION_BLOCK_COUNT == 0 {
                    partition_bloom_keys.push(Vec::new());
                    partition_max_keys.push(Vec::new());
                }
            }
        }

        // Flush final partial block
        if !block_buf.is_empty() {
            // Append restart trailer and hash index before compression
            append_restart_trailer(&mut block_buf, &restart_offsets);
            append_hash_index(&mut block_buf, &block_hash_entries);

            let bfk = block_first_key.take().unwrap_or_default();
            let block_last = prev_key.clone();

            // Resolve pending entry from previous block
            if let Some((pending_last, offset, len)) = pending_index.take() {
                let shortened = shorten_index_key(&pending_last, &bfk);
                index_entries.push((shortened, offset, len));
            }

            let framed_size = write_framed_block_compressed(
                &mut w,
                BLOCK_TYPE_DATA,
                &block_buf,
                self.compression,
            )?;
            let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
            if let Some(ref limiter) = self.rate_limiter {
                limiter.acquire(framed_size as u64);
            }
            let shortened = shorten_final_index_key(&block_last);
            index_entries.push((shortened, file_offset, on_disk_data_len));
            file_offset += framed_size as u64;
            block_buf.clear();
        }

        // Handle trailing pending entry (single-block segment)
        if let Some((pending_last, offset, len)) = pending_index.take() {
            let shortened = shorten_final_index_key(&pending_last);
            index_entries.push((shortened, offset, len));
        }

        // Handle empty segment
        if entry_count == 0 {
            commit_min = 0;
            commit_max = 0;
        }

        // 2. Decide index type and write sub-index blocks if partitioned
        let chunks: Vec<&[(Vec<u8>, u64, u32)]> =
            index_entries.chunks(INDEX_PARTITION_BLOCK_COUNT).collect();
        let use_partitioned = chunks.len() >= 2;
        let index_type;

        // For partitioned: write sub-index blocks first (before blooms),
        // defer top-level index until after filter index.
        let mut top_level_entries: Vec<(Vec<u8>, u64, u32)> = Vec::new();
        if use_partitioned {
            index_type = INDEX_TYPE_PARTITIONED;
            for chunk in &chunks {
                let last_key_in_chunk = chunk.last().unwrap().0.clone();
                let sub_offset = file_offset;
                let sub_data = encode_index_block(chunk);
                write_framed_block(&mut w, BLOCK_TYPE_SUB_INDEX, &sub_data)?;
                let sub_len = (BLOCK_FRAME_OVERHEAD + sub_data.len()) as u32;
                file_offset += sub_len as u64;
                top_level_entries.push((last_key_in_chunk, sub_offset, sub_len));
            }
        } else {
            index_type = INDEX_TYPE_MONOLITHIC;
        }

        // 3. Write bloom partition blocks
        let mut filter_index_entries: Vec<FilterIndexEntry> = Vec::new();
        for (keys, max_key) in partition_bloom_keys.iter().zip(partition_max_keys.iter()) {
            if keys.is_empty() {
                continue;
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            let bloom = BloomFilter::build(&key_refs, self.bloom_bits_per_key);
            let data = bloom.to_bytes();
            let partition_offset = file_offset;
            write_framed_block(&mut w, BLOCK_TYPE_FILTER, &data)?;
            let framed_len = (BLOCK_FRAME_OVERHEAD + data.len()) as u32;
            filter_index_entries.push(FilterIndexEntry {
                max_key: max_key.clone(),
                block_offset: partition_offset,
                block_data_len: framed_len,
            });
            file_offset += framed_len as u64;
        }

        // 4. Write filter index block
        let filter_block_offset = file_offset;
        let filter_index_data = encode_filter_index(&filter_index_entries);
        write_framed_block(&mut w, BLOCK_TYPE_FILTER_INDEX, &filter_index_data)?;
        let filter_block_len = (BLOCK_FRAME_OVERHEAD + filter_index_data.len()) as u32;
        file_offset += filter_block_len as u64;

        // 5. Write top-level or monolithic index block
        let (index_block_offset, index_block_len) = if use_partitioned {
            let tl_offset = file_offset;
            let tl_data = encode_index_block(&top_level_entries);
            write_framed_block(&mut w, BLOCK_TYPE_INDEX, &tl_data)?;
            let tl_len = (BLOCK_FRAME_OVERHEAD + tl_data.len()) as u32;
            file_offset += tl_len as u64;
            (tl_offset, tl_len)
        } else {
            let idx_offset = file_offset;
            let index_data = encode_index_block(&index_entries);
            write_framed_block(&mut w, BLOCK_TYPE_INDEX, &index_data)?;
            let idx_len = (BLOCK_FRAME_OVERHEAD + index_data.len()) as u32;
            file_offset += idx_len as u64;
            (idx_offset, idx_len)
        };

        // 6. Write PropertiesBlock
        let props_block_offset = file_offset;
        let props_data = encode_properties(
            entry_count,
            commit_min,
            commit_max,
            first_key.as_deref().unwrap_or(&[]),
            last_key.as_deref().unwrap_or(&[]),
        );
        write_framed_block(&mut w, BLOCK_TYPE_PROPS, &props_data)?;
        let props_block_len = (BLOCK_FRAME_OVERHEAD + props_data.len()) as u32;
        file_offset += props_block_len as u64;

        // 7. Write Footer
        let footer = encode_footer(
            index_block_offset,
            index_block_len,
            filter_block_offset,
            filter_block_len,
            props_block_offset,
            props_block_len,
            index_type,
        );
        w.write_all(&footer)?;
        file_offset += FOOTER_SIZE as u64;

        // 7. Backfill header
        w.seek(SeekFrom::Start(0))?;
        let header = encode_header(entry_count, commit_min, commit_max, self.data_block_size);
        w.write_all(&header)?;

        // 8. Flush and sync
        w.flush()?;
        w.get_ref().sync_all()?;
        drop(w);

        // 9. Atomic rename
        std::fs::rename(&tmp_path, path)?;

        Ok(SegmentMeta {
            entry_count,
            commit_min,
            commit_max,
            file_size: file_offset,
        })
    }
}

/// Test-only: build a segment with v3 entry encoding (no prefix compression)
/// but v5 partitioned bloom format, for backward-compat testing.
#[cfg(test)]
impl SegmentBuilder {
    pub(crate) fn build_from_iter_v3<I>(&self, iter: I, path: &Path) -> io::Result<SegmentMeta>
    where
        I: Iterator<Item = (InternalKey, MemtableEntry)>,
    {
        let parent = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;

        let tmp_path = path.with_extension("tmp");
        let file = std::fs::File::create(&tmp_path)?;
        let mut w = BufWriter::new(file);

        let header_placeholder = [0u8; KV_HEADER_SIZE];
        w.write_all(&header_placeholder)?;

        let mut entry_count: u64 = 0;
        let mut commit_min: u64 = u64::MAX;
        let mut commit_max: u64 = 0;
        let mut first_key: Option<Vec<u8>> = None;
        let mut last_key: Option<Vec<u8>> = None;
        let mut block_buf = Vec::with_capacity(self.data_block_size + 1024);
        let mut index_entries: Vec<(Vec<u8>, u64, u32)> = Vec::new();
        let mut partition_bloom_keys: Vec<Vec<Vec<u8>>> = vec![Vec::new()];
        let mut partition_max_keys: Vec<Vec<u8>> = vec![Vec::new()];
        let mut last_typed_key: Option<Vec<u8>> = None;
        let mut data_block_count: usize = 0;
        let mut block_first_key: Option<Vec<u8>> = None;
        let mut block_entry_count: usize = 0;
        let mut restart_offsets: Vec<u32> = vec![0];
        let mut file_offset = KV_HEADER_SIZE as u64;

        for (ik, entry) in iter {
            let commit_id = ik.commit_id();
            commit_min = commit_min.min(commit_id);
            commit_max = commit_max.max(commit_id);
            if first_key.is_none() {
                first_key = Some(ik.as_bytes().to_vec());
            }
            last_key = Some(ik.as_bytes().to_vec());
            let typed = ik.typed_key_prefix().to_vec();
            if last_typed_key.as_ref() != Some(&typed) {
                partition_bloom_keys.last_mut().unwrap().push(typed.clone());
                *partition_max_keys.last_mut().unwrap() = typed.clone();
                last_typed_key = Some(typed);
            }
            if block_first_key.is_none() {
                block_first_key = Some(ik.as_bytes().to_vec());
            }
            if block_entry_count > 0 && block_entry_count % RESTART_INTERVAL == 0 {
                restart_offsets.push(block_buf.len() as u32);
            }
            encode_entry(&ik, &entry, &mut block_buf);
            entry_count += 1;
            block_entry_count += 1;

            if block_buf.len() >= self.data_block_size {
                append_restart_trailer(&mut block_buf, &restart_offsets);
                let bfk = block_first_key.take().unwrap();
                let framed_size = write_framed_block_compressed(
                    &mut w,
                    BLOCK_TYPE_DATA,
                    &block_buf,
                    self.compression,
                )?;
                let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
                index_entries.push((bfk, file_offset, on_disk_data_len));
                file_offset += framed_size as u64;
                block_buf.clear();
                restart_offsets.clear();
                restart_offsets.push(0);
                block_entry_count = 0;

                data_block_count += 1;
                if data_block_count % BLOOM_PARTITION_BLOCK_COUNT == 0 {
                    partition_bloom_keys.push(Vec::new());
                    partition_max_keys.push(Vec::new());
                }
            }
        }
        if !block_buf.is_empty() {
            append_restart_trailer(&mut block_buf, &restart_offsets);
            let bfk = block_first_key.take().unwrap_or_default();
            let framed_size = write_framed_block_compressed(
                &mut w,
                BLOCK_TYPE_DATA,
                &block_buf,
                self.compression,
            )?;
            let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
            index_entries.push((bfk, file_offset, on_disk_data_len));
            file_offset += framed_size as u64;
            block_buf.clear();
        }
        if entry_count == 0 {
            commit_min = 0;
            commit_max = 0;
        }

        // Write bloom partition blocks
        let mut filter_index_entries: Vec<FilterIndexEntry> = Vec::new();
        for (keys, max_key) in partition_bloom_keys.iter().zip(partition_max_keys.iter()) {
            if keys.is_empty() {
                continue;
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            let bloom = BloomFilter::build(&key_refs, self.bloom_bits_per_key);
            let data = bloom.to_bytes();
            let partition_offset = file_offset;
            write_framed_block(&mut w, BLOCK_TYPE_FILTER, &data)?;
            let framed_len = (BLOCK_FRAME_OVERHEAD + data.len()) as u32;
            filter_index_entries.push(FilterIndexEntry {
                max_key: max_key.clone(),
                block_offset: partition_offset,
                block_data_len: framed_len,
            });
            file_offset += framed_len as u64;
        }

        // Write filter index block
        let filter_block_offset = file_offset;
        let filter_index_data = encode_filter_index(&filter_index_entries);
        write_framed_block(&mut w, BLOCK_TYPE_FILTER_INDEX, &filter_index_data)?;
        let filter_block_len = (BLOCK_FRAME_OVERHEAD + filter_index_data.len()) as u32;
        file_offset += filter_block_len as u64;

        let index_block_offset = file_offset;
        let index_data = encode_index_block(&index_entries);
        write_framed_block(&mut w, BLOCK_TYPE_INDEX, &index_data)?;
        let index_block_len = (BLOCK_FRAME_OVERHEAD + index_data.len()) as u32;
        file_offset += index_block_len as u64;

        let props_block_offset = file_offset;
        let props_data = encode_properties(
            entry_count,
            commit_min,
            commit_max,
            first_key.as_deref().unwrap_or(&[]),
            last_key.as_deref().unwrap_or(&[]),
        );
        write_framed_block(&mut w, BLOCK_TYPE_PROPS, &props_data)?;
        let props_block_len = (BLOCK_FRAME_OVERHEAD + props_data.len()) as u32;
        file_offset += props_block_len as u64;

        let footer = encode_footer(
            index_block_offset,
            index_block_len,
            filter_block_offset,
            filter_block_len,
            props_block_offset,
            props_block_len,
            INDEX_TYPE_MONOLITHIC,
        );
        w.write_all(&footer)?;
        file_offset += FOOTER_SIZE as u64;

        // Write header with format_version = 3 (v3 entry encoding)
        w.seek(SeekFrom::Start(0))?;
        let mut h = [0u8; KV_HEADER_SIZE];
        h[0..8].copy_from_slice(&KV_HEADER_MAGIC);
        h[8..10].copy_from_slice(&3u16.to_le_bytes()); // v3
        h[16..24].copy_from_slice(&commit_min.to_le_bytes());
        h[24..32].copy_from_slice(&commit_max.to_le_bytes());
        h[32..40].copy_from_slice(&entry_count.to_le_bytes());
        h[40..44].copy_from_slice(&(self.data_block_size as u32).to_le_bytes());
        w.write_all(&h)?;

        w.flush()?;
        w.get_ref().sync_all()?;
        drop(w);
        std::fs::rename(&tmp_path, path)?;

        Ok(SegmentMeta {
            entry_count,
            commit_min,
            commit_max,
            file_size: file_offset,
        })
    }
}

// ---------------------------------------------------------------------------
// Partitioned bloom filter index
// ---------------------------------------------------------------------------

/// An entry in the filter index block, mapping a key range to a bloom partition.
#[derive(Debug, Clone)]
pub(crate) struct FilterIndexEntry {
    /// Max typed_key_prefix covered by this bloom partition.
    pub max_key: Vec<u8>,
    /// File offset of the framed bloom partition block.
    pub block_offset: u64,
    /// Total on-disk size of the framed bloom partition block (including frame overhead).
    pub block_data_len: u32,
}

/// Encode a filter index block.
///
/// Format: `[count: u32 LE] [key_len: u32][key_bytes][offset: u64][data_len: u32] ...`
fn encode_filter_index(entries: &[FilterIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        buf.extend_from_slice(&(e.max_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&e.max_key);
        buf.extend_from_slice(&e.block_offset.to_le_bytes());
        buf.extend_from_slice(&e.block_data_len.to_le_bytes());
    }
    buf
}

/// Parse a filter index block into entries.
pub(crate) fn parse_filter_index(data: &[u8]) -> Option<Vec<FilterIndexEntry>> {
    if data.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(data[..4].try_into().ok()?) as usize;
    let mut pos = 4;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > data.len() {
            return None;
        }
        let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + key_len + 8 + 4 > data.len() {
            return None;
        }
        let max_key = data[pos..pos + key_len].to_vec();
        pos += key_len;
        let block_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let block_data_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        entries.push(FilterIndexEntry {
            max_key,
            block_offset,
            block_data_len,
        });
    }
    Some(entries)
}

// ---------------------------------------------------------------------------
// Entry encoding (within data blocks)
// ---------------------------------------------------------------------------

/// Encode a single entry into a data block buffer (v2/v3 format, no prefix compression).
///
/// v2 format: `| ik_len: u32 | ik_bytes | value_kind: u8 | timestamp: u64 LE | ttl_ms: u64 LE | value_len: u32 | value_bytes |`
///
/// Retained for v3 backward-compat testing and decode roundtrips.
#[cfg(test)]
fn encode_entry(ik: &InternalKey, entry: &MemtableEntry, buf: &mut Vec<u8>) {
    let ik_bytes = ik.as_bytes();
    buf.extend_from_slice(&(ik_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(ik_bytes);

    if entry.is_tombstone {
        buf.push(VALUE_KIND_DEL);
        buf.extend_from_slice(&entry.timestamp.as_micros().to_le_bytes());
        buf.extend_from_slice(&entry.ttl_ms.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    } else {
        buf.push(VALUE_KIND_PUT);
        buf.extend_from_slice(&entry.timestamp.as_micros().to_le_bytes());
        buf.extend_from_slice(&entry.ttl_ms.to_le_bytes());
        let value_bytes =
            bincode::serialize(&entry.value).expect("Value serialization should not fail");
        buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&value_bytes);
    }
}

/// Append restart point trailer to a data block buffer.
///
/// Format: `[restart_0: u32 LE] ... [restart_K: u32 LE] [num_restarts: u32 LE]`
/// The trailer is appended in-place before compression.
fn append_restart_trailer(buf: &mut Vec<u8>, restart_offsets: &[u32]) {
    for &offset in restart_offsets {
        buf.extend_from_slice(&offset.to_le_bytes());
    }
    buf.extend_from_slice(&(restart_offsets.len() as u32).to_le_bytes());
}

/// Append a hash index after the restart trailer.
///
/// Format: `[bucket_0..bucket_N: u8] [num_buckets: u16 LE] [0x01: sentinel]`
///
/// Each bucket holds a restart interval index (0-254) or `0xFF` (empty).
/// Load factor 75%: `num_buckets = ceil(num_entries / 0.75)`.
fn append_hash_index(buf: &mut Vec<u8>, entries: &[(u64, usize)]) {
    if entries.is_empty() {
        return;
    }
    let num_buckets = ((entries.len() as f64 / 0.75).ceil() as usize)
        .max(1)
        .min(u16::MAX as usize);
    let mut buckets = vec![0xFFu8; num_buckets];
    for &(hash, restart_idx) in entries {
        let bucket = (hash as usize) % num_buckets;
        if buckets[bucket] == 0xFF && restart_idx <= 254 {
            buckets[bucket] = restart_idx as u8;
        }
        // Collision or restart_idx > 254: skip (reader falls back to binary search)
    }
    buf.extend_from_slice(&buckets);
    buf.extend_from_slice(&(num_buckets as u16).to_le_bytes());
    buf.push(0x01); // sentinel
}

/// Decoded entry header — key + metadata without value deserialization.
pub(crate) struct EntryHeader {
    pub ik: InternalKey,
    pub is_tombstone: bool,
    pub timestamp: u64,
    pub ttl_ms: u64,
    /// Byte offset where the value bytes start (within the data slice).
    pub value_start: usize,
    /// Length of the value bytes.
    pub value_len: usize,
    /// Total bytes consumed by this entry.
    pub total_len: usize,
}

/// Zero-copy entry header — borrows key bytes from the block data.
///
/// Used by `scan_block_for_key` to compare keys without allocating
/// an `InternalKey` for every entry. Only the matching entry's key
/// is promoted to an owned `InternalKey`.
pub(crate) struct EntryHeaderRef<'a> {
    /// Raw InternalKey bytes (borrowed from block data).
    pub ik_bytes: &'a [u8],
    pub is_tombstone: bool,
    pub timestamp: u64,
    pub ttl_ms: u64,
    pub value_start: usize,
    pub value_len: usize,
    pub total_len: usize,
}

impl<'a> EntryHeaderRef<'a> {
    /// Typed key prefix (everything except trailing 8-byte commit_id).
    #[inline]
    pub fn typed_key_prefix(&self) -> &[u8] {
        &self.ik_bytes[..self.ik_bytes.len() - 8]
    }

    /// Extract commit_id from the trailing 8 bytes.
    #[inline]
    pub fn commit_id(&self) -> u64 {
        let len = self.ik_bytes.len();
        let bytes: [u8; 8] = self.ik_bytes[len - 8..].try_into().unwrap();
        !u64::from_be_bytes(bytes)
    }
}

/// Decode entry header with zero-copy key reference into `data`.
///
/// Parses InternalKey bytes, value_kind, timestamp, ttl_ms, and value_len,
/// but does NOT deserialize value bytes. Borrows key bytes directly from
/// `data` — no allocation.
pub(crate) fn decode_entry_header_ref(data: &[u8]) -> Option<EntryHeaderRef<'_>> {
    if data.len() < 4 {
        return None;
    }
    let mut pos = 0;

    let ik_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if pos + ik_len > data.len() || ik_len < 28 {
        return None;
    }
    let ik_bytes = &data[pos..pos + ik_len];
    pos += ik_len;

    if pos >= data.len() {
        return None;
    }
    let value_kind = data[pos];
    pos += 1;

    if pos + 16 > data.len() {
        return None;
    }
    let timestamp = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let ttl_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;

    if pos + 4 > data.len() {
        return None;
    }
    let value_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    let is_tombstone = value_kind == VALUE_KIND_DEL;
    let value_start = pos;

    if !is_tombstone && value_kind != VALUE_KIND_PUT {
        return None;
    }

    if !is_tombstone {
        if pos + value_len > data.len() {
            return None;
        }
        pos += value_len;
    }

    Some(EntryHeaderRef {
        ik_bytes,
        is_tombstone,
        timestamp,
        ttl_ms,
        value_start,
        value_len,
        total_len: pos,
    })
}

/// Deserialize value bytes from a previously decoded entry header.
///
/// Call ONLY for the matching entry after key comparison confirms a match.
pub(crate) fn decode_entry_value(data: &[u8], header: &EntryHeader) -> Option<Value> {
    if header.is_tombstone {
        return Some(Value::Null);
    }
    let end = header.value_start + header.value_len;
    if end > data.len() {
        return None;
    }
    bincode::deserialize(&data[header.value_start..end]).ok()
}

/// Decode a single entry from a data block at the given offset.
///
/// Returns `(internal_key, is_tombstone, value, timestamp_micros, ttl_ms, bytes_consumed)`.
///
/// Full decode path used by iterators. For point lookups, prefer
/// `decode_entry_header_ref` + `decode_entry_value`.
pub(crate) fn decode_entry(data: &[u8]) -> Option<(InternalKey, bool, Value, u64, u64, usize)> {
    let ref_header = decode_entry_header_ref(data)?;
    let ik = InternalKey::try_from_bytes(ref_header.ik_bytes.to_vec())?;
    let header = EntryHeader {
        ik,
        is_tombstone: ref_header.is_tombstone,
        timestamp: ref_header.timestamp,
        ttl_ms: ref_header.ttl_ms,
        value_start: ref_header.value_start,
        value_len: ref_header.value_len,
        total_len: ref_header.total_len,
    };
    let value = decode_entry_value(data, &header)?;
    Some((
        header.ik,
        header.is_tombstone,
        value,
        header.timestamp,
        header.ttl_ms,
        header.total_len,
    ))
}

// ---------------------------------------------------------------------------
// Varint utilities (v4 prefix compression)
// ---------------------------------------------------------------------------

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// Index key shortening (Epic 29)
// ---------------------------------------------------------------------------

/// Find the shortest separator between `a` and `b` such that `a <= result < b`.
///
/// RocksDB-style: at the first differing byte, if incrementing `a`'s byte
/// still yields a value strictly less than `b`'s byte, truncate and return.
fn shortest_separator(a: &[u8], b: &[u8]) -> Vec<u8> {
    let prefix_len = common_prefix_len(a, b);

    // a is a prefix of b, or they are equal — no shortening possible
    if prefix_len >= a.len() || prefix_len >= b.len() {
        return a.to_vec();
    }

    let diff_byte = a[prefix_len];
    // Guard: avoid u8 overflow when diff_byte == 0xFF (can happen if a > b,
    // which shouldn't occur but we want to be safe in debug builds).
    if diff_byte < 0xFF && diff_byte + 1 < b[prefix_len] {
        let mut result = a[..prefix_len + 1].to_vec();
        *result.last_mut().unwrap() = diff_byte + 1;
        result
    } else {
        a.to_vec()
    }
}

/// Find the shortest key that is >= `a` by incrementing the first non-0xFF byte.
///
/// Used for the last block in a segment where there is no upper bound.
fn shortest_successor(a: &[u8]) -> Vec<u8> {
    for (i, &byte) in a.iter().enumerate() {
        if byte != 0xFF {
            let mut result = a[..i + 1].to_vec();
            *result.last_mut().unwrap() = byte + 1;
            return result;
        }
    }
    // All 0xFF — return unchanged
    a.to_vec()
}

/// Shorten an index key given the last key of the previous block and the first
/// key of the next block. Operates on the typed_key_prefix portion (everything
/// except trailing 8-byte commit_id) and re-appends `u64::MAX` as suffix.
fn shorten_index_key(prev_last: &[u8], next_first: &[u8]) -> Vec<u8> {
    if prev_last.len() < 8 || next_first.len() < 8 {
        return prev_last.to_vec();
    }
    let a_prefix = &prev_last[..prev_last.len() - 8];
    let b_prefix = &next_first[..next_first.len() - 8];
    let shortened = shortest_separator(a_prefix, b_prefix);
    if shortened.as_slice() == a_prefix {
        // Prefix was NOT shortened (keys differ only in commit_id, or the
        // differing byte was adjacent).  Fall back to the full original key
        // to preserve the invariant: prev_last <= separator < next_first.
        // Appending u64::MAX here would produce a separator > next_first
        // when the typed_key_prefix portions are identical.
        prev_last.to_vec()
    } else {
        // Prefix was genuinely shortened to a value strictly between the two
        // prefixes.  Append u64::MAX (sorts before all real commit_ids in
        // Strata's descending order) so the separator >= prev_last.
        let mut result = shortened;
        result.extend_from_slice(&(!0_u64).to_be_bytes());
        result
    }
}

/// Shorten the index key for the last block in a segment.
/// Uses `shortest_successor` on the typed_key_prefix, re-appends `u64::MAX`.
fn shorten_final_index_key(last_key: &[u8]) -> Vec<u8> {
    if last_key.len() < 8 {
        return last_key.to_vec();
    }
    let prefix = &last_key[..last_key.len() - 8];
    let mut shortened = shortest_successor(prefix);
    shortened.extend_from_slice(&(!0_u64).to_be_bytes());
    shortened
}

fn encode_varint32(buf: &mut Vec<u8>, mut val: u32) {
    while val >= 0x80 {
        buf.push((val as u8) | 0x80);
        val >>= 7;
    }
    buf.push(val as u8);
}

/// Returns (value, bytes_consumed). Fast path: single byte if < 128.
#[inline]
fn decode_varint32(data: &[u8]) -> Option<(u32, usize)> {
    let b = *data.first()?;
    if b < 0x80 {
        return Some((b as u32, 1));
    }
    decode_varint32_slow(data)
}

fn decode_varint32_slow(data: &[u8]) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    for (i, &byte) in data.iter().enumerate().take(5) {
        let val = (byte & 0x7F) as u32;
        // 5th byte (i=4, shift=28) can only contribute 4 bits to a u32.
        // Reject corrupt data that would overflow.
        if i == 4 && val > 0x0F {
            return None;
        }
        result |= val << (i as u32 * 7);
        if byte < 0x80 {
            return Some((result, i + 1));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// v4 entry encoding (prefix-compressed keys)
// ---------------------------------------------------------------------------

/// Encode a single entry with prefix compression into a data block buffer.
///
/// v4 format: `| shared: varint32 | non_shared: varint32 | key_delta[non_shared] | value_kind: u8 | timestamp: u64 LE | ttl_ms: u64 LE | value_len: u32 LE | value_bytes |`
///
/// At restart points, `shared` is forced to 0 so the key is self-contained.
fn encode_entry_v4(
    prev_key: &[u8],
    ik: &InternalKey,
    entry: &MemtableEntry,
    buf: &mut Vec<u8>,
    is_restart: bool,
) {
    let ik_bytes = ik.as_bytes();
    let shared = if is_restart {
        0
    } else {
        common_prefix_len(prev_key, ik_bytes)
    };
    let non_shared = ik_bytes.len() - shared;

    encode_varint32(buf, shared as u32);
    encode_varint32(buf, non_shared as u32);
    buf.extend_from_slice(&ik_bytes[shared..]);

    // Value encoding: identical to v3
    if entry.is_tombstone {
        buf.push(VALUE_KIND_DEL);
        buf.extend_from_slice(&entry.timestamp.as_micros().to_le_bytes());
        buf.extend_from_slice(&entry.ttl_ms.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    } else {
        buf.push(VALUE_KIND_PUT);
        buf.extend_from_slice(&entry.timestamp.as_micros().to_le_bytes());
        buf.extend_from_slice(&entry.ttl_ms.to_le_bytes());
        let value_bytes =
            bincode::serialize(&entry.value).expect("Value serialization should not fail");
        buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&value_bytes);
    }
}

/// Decoded v4 entry header — metadata without key allocation or value deserialization.
///
/// The caller passes a `prev_key: &mut Vec<u8>` which is updated in-place to hold
/// the reconstructed full key after decoding.
pub(crate) struct EntryHeaderV4 {
    pub is_tombstone: bool,
    pub timestamp: u64,
    pub ttl_ms: u64,
    pub value_start: usize,
    pub value_len: usize,
    pub total_len: usize,
}

/// Decode v4 entry, reconstructing the full key into `prev_key` (in/out).
pub(crate) fn decode_entry_header_v4(data: &[u8], prev_key: &mut Vec<u8>) -> Option<EntryHeaderV4> {
    let (shared, n1) = decode_varint32(data)?;
    let (non_shared, n2) = decode_varint32(&data[n1..])?;
    let shared = shared as usize;
    let non_shared = non_shared as usize;
    // Early bounds check prevents usize overflow in key_start + non_shared on 32-bit
    if non_shared > data.len() || shared > prev_key.len() {
        return None;
    }
    let key_start = n1 + n2;
    let key_end = key_start + non_shared;
    if key_end > data.len() {
        return None;
    }

    // Reconstruct key in prev_key buffer (single allocation reuse)
    prev_key.truncate(shared);
    prev_key.extend_from_slice(&data[key_start..key_end]);

    // Value portion: same layout as v3
    let mut pos = key_end;
    if pos >= data.len() {
        return None;
    }
    let value_kind = data[pos];
    pos += 1;

    if pos + 16 > data.len() {
        return None;
    }
    let timestamp = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let ttl_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;

    if pos + 4 > data.len() {
        return None;
    }
    let value_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    let is_tombstone = value_kind == VALUE_KIND_DEL;
    let value_start = pos;

    if !is_tombstone && value_kind != VALUE_KIND_PUT {
        return None;
    }

    if !is_tombstone {
        if value_len > data.len() || pos + value_len > data.len() {
            return None;
        }
        pos += value_len;
    }

    Some(EntryHeaderV4 {
        is_tombstone,
        timestamp,
        ttl_ms,
        value_start,
        value_len,
        total_len: pos,
    })
}

/// Decode a v4 entry fully, reconstructing key into `prev_key` (in/out).
///
/// Returns `(internal_key, is_tombstone, value, timestamp_micros, ttl_ms, bytes_consumed)`.
pub(crate) fn decode_entry_v4(
    data: &[u8],
    prev_key: &mut Vec<u8>,
) -> Option<(InternalKey, bool, Value, u64, u64, usize)> {
    let hdr = decode_entry_header_v4(data, prev_key)?;
    let ik = InternalKey::try_from_bytes(prev_key.clone())?;
    let value = if hdr.is_tombstone {
        Value::Null
    } else {
        let end = hdr.value_start + hdr.value_len;
        if end > data.len() {
            return None;
        }
        bincode::deserialize(&data[hdr.value_start..end]).ok()?
    };
    Some((
        ik,
        hdr.is_tombstone,
        value,
        hdr.timestamp,
        hdr.ttl_ms,
        hdr.total_len,
    ))
}

/// Zero-copy decode of a v4 restart entry (where shared=0).
///
/// Returns `EntryHeaderRef` borrowing key bytes directly from the block data,
/// just like `decode_entry_header_ref` for v3. Only valid at restart points.
pub(crate) fn decode_entry_header_ref_v4(data: &[u8]) -> Option<EntryHeaderRef<'_>> {
    let (shared, n1) = decode_varint32(data)?;
    if shared != 0 {
        return None; // Not a restart point
    }
    let (non_shared, n2) = decode_varint32(&data[n1..])?;
    let non_shared = non_shared as usize;
    // Early bounds check prevents usize overflow in key_start + non_shared on 32-bit
    if non_shared > data.len() || non_shared < 28 {
        return None;
    }
    let key_start = n1 + n2;
    let key_end = key_start + non_shared;
    if key_end > data.len() {
        return None;
    }
    let ik_bytes = &data[key_start..key_end];

    let mut pos = key_end;
    if pos >= data.len() {
        return None;
    }
    let value_kind = data[pos];
    pos += 1;

    if pos + 16 > data.len() {
        return None;
    }
    let timestamp = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let ttl_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;

    if pos + 4 > data.len() {
        return None;
    }
    let value_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    let is_tombstone = value_kind == VALUE_KIND_DEL;
    let value_start = pos;

    if !is_tombstone && value_kind != VALUE_KIND_PUT {
        return None;
    }

    if !is_tombstone {
        if value_len > data.len() || pos + value_len > data.len() {
            return None;
        }
        pos += value_len;
    }

    Some(EntryHeaderRef {
        ik_bytes,
        is_tombstone,
        timestamp,
        ttl_ms,
        value_start,
        value_len,
        total_len: pos,
    })
}

// ---------------------------------------------------------------------------
// Block framing
// ---------------------------------------------------------------------------

/// Write a framed block: `| type: u8 | codec: u8 | reserved: u16 | data_len: u32 | data | crc32: u32 |`
fn write_framed_block<W: Write>(w: &mut W, block_type: u8, data: &[u8]) -> io::Result<()> {
    w.write_all(&[block_type])?; // block_type
    w.write_all(&[0u8])?; // codec = none
    w.write_all(&[0u8; 2])?; // reserved
    w.write_all(&(data.len() as u32).to_le_bytes())?; // data_len
    w.write_all(data)?;
    let crc = crc32fast::hash(data);
    w.write_all(&crc.to_le_bytes())?;
    Ok(())
}

/// Write a framed block with the specified compression codec.
///
/// - `CompressionCodec::None` — writes uncompressed (codec byte 0).
/// - `CompressionCodec::Zstd(level)` — compresses with zstd at the given level.
///   Falls back to uncompressed if compression doesn't reduce size.
///
/// The codec byte in the frame header indicates the compression used:
/// - 0 = uncompressed
/// - 1 = zstd
///
/// Returns the total framed size written (overhead + data).
fn write_framed_block_compressed<W: Write>(
    w: &mut W,
    block_type: u8,
    data: &[u8],
    codec: CompressionCodec,
) -> io::Result<usize> {
    let (write_data, codec_byte) = match codec {
        CompressionCodec::Zstd(level) if !data.is_empty() => {
            match zstd::encode_all(std::io::Cursor::new(data), level) {
                Ok(compressed) if compressed.len() < data.len() => {
                    (compressed, 1u8) // zstd compressed
                }
                _ => (data.to_vec(), 0u8), // fallback to uncompressed
            }
        }
        _ => (data.to_vec(), 0u8),
    };

    w.write_all(&[block_type])?;
    w.write_all(&[codec_byte])?;
    w.write_all(&[0u8; 2])?; // reserved
    w.write_all(&(write_data.len() as u32).to_le_bytes())?;
    w.write_all(&write_data)?;
    let crc = crc32fast::hash(&write_data);
    w.write_all(&crc.to_le_bytes())?;
    Ok(BLOCK_FRAME_OVERHEAD + write_data.len())
}

/// Parse a framed block from a byte slice. Returns `(block_type, data_slice)`.
///
/// Verifies CRC32 integrity. Returns `None` on corruption or truncation.
pub(crate) fn parse_framed_block(raw: &[u8]) -> Option<(u8, &[u8])> {
    if raw.len() < BLOCK_FRAME_OVERHEAD {
        return None;
    }
    let block_type = raw[0];
    // codec = raw[1], reserved = raw[2..4] — ignored for v1
    let data_len = u32::from_le_bytes(raw[4..8].try_into().ok()?) as usize;
    if raw.len() < 8 + data_len + 4 {
        return None;
    }
    let data = &raw[8..8 + data_len];
    let stored_crc = u32::from_le_bytes(raw[8 + data_len..8 + data_len + 4].try_into().ok()?);
    let computed_crc = crc32fast::hash(data);
    if stored_crc != computed_crc {
        return None;
    }
    Some((block_type, data))
}

// ---------------------------------------------------------------------------
// Index block encoding
// ---------------------------------------------------------------------------

/// Encode index block: array of `(key_len: u32, key_bytes, block_offset: u64, block_data_len: u32)`.
fn encode_index_block(entries: &[(Vec<u8>, u64, u32)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (key, offset, len) in entries {
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&len.to_le_bytes());
    }
    buf
}

/// Parsed index entry.
#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    pub key: Vec<u8>,
    pub block_offset: u64,
    pub block_data_len: u32,
}

/// Parse an index block into a list of entries.
pub(crate) fn parse_index_block(data: &[u8]) -> Option<Vec<IndexEntry>> {
    if data.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(data[..4].try_into().ok()?) as usize;
    let mut pos = 4;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > data.len() {
            return None;
        }
        let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + key_len + 8 + 4 > data.len() {
            return None;
        }
        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;
        let block_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let block_data_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        entries.push(IndexEntry {
            key,
            block_offset,
            block_data_len,
        });
    }
    Some(entries)
}

// ---------------------------------------------------------------------------
// Properties block encoding
// ---------------------------------------------------------------------------

/// Encode the properties block.
fn encode_properties(
    entry_count: u64,
    commit_min: u64,
    commit_max: u64,
    key_min: &[u8],
    key_max: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&entry_count.to_le_bytes());
    buf.extend_from_slice(&commit_min.to_le_bytes());
    buf.extend_from_slice(&commit_max.to_le_bytes());
    buf.extend_from_slice(&(key_min.len() as u32).to_le_bytes());
    buf.extend_from_slice(key_min);
    buf.extend_from_slice(&(key_max.len() as u32).to_le_bytes());
    buf.extend_from_slice(key_max);
    buf
}

/// Parsed properties block.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields read by future compaction/GC
pub(crate) struct PropertiesBlock {
    pub entry_count: u64,
    pub commit_min: u64,
    pub commit_max: u64,
    pub key_min: Vec<u8>,
    pub key_max: Vec<u8>,
}

/// Parse a properties block.
pub(crate) fn parse_properties_block(data: &[u8]) -> Option<PropertiesBlock> {
    if data.len() < 24 + 4 {
        return None;
    }
    let mut pos = 0;
    let entry_count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let commit_min = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let commit_max = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;

    if pos + 4 > data.len() {
        return None;
    }
    let key_min_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if pos + key_min_len > data.len() {
        return None;
    }
    let key_min = data[pos..pos + key_min_len].to_vec();
    pos += key_min_len;

    if pos + 4 > data.len() {
        return None;
    }
    let key_max_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if pos + key_max_len > data.len() {
        return None;
    }
    let key_max = data[pos..pos + key_max_len].to_vec();

    Some(PropertiesBlock {
        entry_count,
        commit_min,
        commit_max,
        key_min,
        key_max,
    })
}

// ---------------------------------------------------------------------------
// Header / Footer encoding
// ---------------------------------------------------------------------------

/// Encode the KV header (64 bytes fixed).
///
/// Layout:
/// ```text
/// magic: [u8; 8]          — "STRAKV\0\0"
/// format_version: u16 LE
/// reserved_a: [u8; 6]
/// commit_min: u64 LE
/// commit_max: u64 LE
/// entry_count: u64 LE
/// data_block_size: u32 LE
/// reserved_b: [u8; 20]
/// ```
fn encode_header(
    entry_count: u64,
    commit_min: u64,
    commit_max: u64,
    data_block_size: usize,
) -> [u8; KV_HEADER_SIZE] {
    let mut h = [0u8; KV_HEADER_SIZE];
    h[0..8].copy_from_slice(&KV_HEADER_MAGIC);
    h[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    // 10..16: reserved
    h[16..24].copy_from_slice(&commit_min.to_le_bytes());
    h[24..32].copy_from_slice(&commit_max.to_le_bytes());
    h[32..40].copy_from_slice(&entry_count.to_le_bytes());
    h[40..44].copy_from_slice(&(data_block_size as u32).to_le_bytes());
    // 44..64: reserved
    h
}

/// Parse the KV header from 64 bytes.
pub(crate) fn parse_header(data: &[u8; KV_HEADER_SIZE]) -> Option<KVHeader> {
    if data[0..8] != KV_HEADER_MAGIC {
        return None;
    }
    let format_version = u16::from_le_bytes(data[8..10].try_into().ok()?);
    if !(2..=FORMAT_VERSION).contains(&format_version) {
        return None;
    }
    let commit_min = u64::from_le_bytes(data[16..24].try_into().ok()?);
    let commit_max = u64::from_le_bytes(data[24..32].try_into().ok()?);
    let entry_count = u64::from_le_bytes(data[32..40].try_into().ok()?);
    let data_block_size = u32::from_le_bytes(data[40..44].try_into().ok()?);
    Some(KVHeader {
        format_version,
        commit_min,
        commit_max,
        entry_count,
        data_block_size,
    })
}

/// Parsed KV file header.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields read by future format versioning/compaction
pub(crate) struct KVHeader {
    pub format_version: u16,
    pub commit_min: u64,
    pub commit_max: u64,
    pub entry_count: u64,
    pub data_block_size: u32,
}

/// Encode the footer (56 bytes fixed).
///
/// Layout:
/// ```text
/// index_block_offset: u64 LE
/// index_block_len: u32 LE
/// filter_block_offset: u64 LE
/// filter_block_len: u32 LE
/// props_block_offset: u64 LE
/// props_block_len: u32 LE
/// index_type: u8             (0=monolithic, 1=partitioned)
/// reserved: [u8; 11]
/// magic: [u8; 8] — "STRAKEND"
/// ```
fn encode_footer(
    index_offset: u64,
    index_len: u32,
    filter_offset: u64,
    filter_len: u32,
    props_offset: u64,
    props_len: u32,
    index_type: u8,
) -> [u8; FOOTER_SIZE] {
    let mut f = [0u8; FOOTER_SIZE];
    f[0..8].copy_from_slice(&index_offset.to_le_bytes());
    f[8..12].copy_from_slice(&index_len.to_le_bytes());
    f[12..20].copy_from_slice(&filter_offset.to_le_bytes());
    f[20..24].copy_from_slice(&filter_len.to_le_bytes());
    f[24..32].copy_from_slice(&props_offset.to_le_bytes());
    f[32..36].copy_from_slice(&props_len.to_le_bytes());
    f[36] = index_type;
    // 37..48: reserved
    f[48..56].copy_from_slice(&FOOTER_MAGIC);
    f
}

/// Parse the footer from 56 bytes.
pub(crate) fn parse_footer(data: &[u8; FOOTER_SIZE]) -> Option<Footer> {
    if data[48..56] != FOOTER_MAGIC {
        return None;
    }
    Some(Footer {
        index_block_offset: u64::from_le_bytes(data[0..8].try_into().ok()?),
        index_block_len: u32::from_le_bytes(data[8..12].try_into().ok()?),
        filter_block_offset: u64::from_le_bytes(data[12..20].try_into().ok()?),
        filter_block_len: u32::from_le_bytes(data[20..24].try_into().ok()?),
        props_block_offset: u64::from_le_bytes(data[24..32].try_into().ok()?),
        props_block_len: u32::from_le_bytes(data[32..36].try_into().ok()?),
        index_type: data[36],
    })
}

/// Parsed footer.
#[derive(Debug, Clone)]
pub(crate) struct Footer {
    pub index_block_offset: u64,
    pub index_block_len: u32,
    pub filter_block_offset: u64,
    pub filter_block_len: u32,
    pub props_block_offset: u64,
    pub props_block_len: u32,
    pub index_type: u8,
}

// Re-export constants for the segment reader.
pub(crate) const HEADER_SIZE: usize = KV_HEADER_SIZE;
pub(crate) const FOOTER_SZ: usize = FOOTER_SIZE;
pub(crate) const FRAME_OVERHEAD: usize = BLOCK_FRAME_OVERHEAD;
pub(crate) const IDX_TYPE_PARTITIONED: u8 = INDEX_TYPE_PARTITIONED;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use crate::segment::KVSegment;
    use std::sync::Arc;
    use strata_core::types::{BranchId, Key, Namespace, TypeTag};

    fn branch() -> BranchId {
        BranchId::from_bytes([1; 16])
    }

    fn key(user_key: &str) -> Key {
        let ns = Arc::new(Namespace::new(branch(), "default".to_string()));
        Key::new(ns, TypeTag::KV, user_key.as_bytes().to_vec())
    }

    #[test]
    fn build_from_memtable_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mt = Memtable::new(0);
        mt.put(&key("a"), 1, Value::Int(1), false);
        mt.put(&key("b"), 2, Value::Int(2), false);
        mt.put(&key("c"), 3, Value::String("hello".into()), false);
        mt.freeze();

        let builder = SegmentBuilder::default();
        let meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();

        assert_eq!(meta.entry_count, 3);
        assert_eq!(meta.commit_min, 1);
        assert_eq!(meta.commit_max, 3);
        assert!(path.exists());
        assert!(meta.file_size > 0);
    }

    #[test]
    fn build_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SegmentBuilder::default();
        let meta = builder.build_from_iter(std::iter::empty(), &path).unwrap();

        assert_eq!(meta.entry_count, 0);
        assert!(path.exists());
    }

    #[test]
    fn build_with_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tomb.sst");

        let mt = Memtable::new(0);
        mt.put(&key("x"), 1, Value::Int(10), false);
        mt.put(&key("x"), 2, Value::Null, true);
        mt.freeze();

        let builder = SegmentBuilder::default();
        let meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();
        assert_eq!(meta.entry_count, 2);
    }

    #[test]
    fn entry_encode_decode_roundtrip() {
        let ik = InternalKey::encode(&key("hello"), 42);
        let entry = MemtableEntry {
            value: Value::String("world".into()),
            is_tombstone: false,
            timestamp: strata_core::Timestamp::now(),
            ttl_ms: 0,
        };

        let mut buf = Vec::new();
        encode_entry(&ik, &entry, &mut buf);

        let (decoded_ik, is_tomb, decoded_val, ts, ttl, consumed) = decode_entry(&buf).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(!is_tomb);
        assert_eq!(decoded_val, Value::String("world".into()));
        assert_eq!(ts, entry.timestamp.as_micros());
        assert_eq!(ttl, 0);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn tombstone_entry_roundtrip() {
        let ik = InternalKey::encode(&key("gone"), 99);
        let entry = MemtableEntry {
            value: Value::Null,
            is_tombstone: true,
            timestamp: strata_core::Timestamp::now(),
            ttl_ms: 0,
        };

        let mut buf = Vec::new();
        encode_entry(&ik, &entry, &mut buf);

        let (decoded_ik, is_tomb, _, ts, ttl, consumed) = decode_entry(&buf).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(is_tomb);
        assert_eq!(ts, entry.timestamp.as_micros());
        assert_eq!(ttl, 0);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn header_roundtrip() {
        let header = encode_header(100, 5, 42, 65536);
        let parsed = parse_header(&header).unwrap();
        assert_eq!(parsed.entry_count, 100);
        assert_eq!(parsed.commit_min, 5);
        assert_eq!(parsed.commit_max, 42);
        assert_eq!(parsed.data_block_size, 65536);
    }

    #[test]
    fn footer_roundtrip() {
        let footer = encode_footer(1000, 200, 1200, 50, 1250, 80, INDEX_TYPE_MONOLITHIC);
        let parsed = parse_footer(&footer).unwrap();
        assert_eq!(parsed.index_block_offset, 1000);
        assert_eq!(parsed.index_block_len, 200);
        assert_eq!(parsed.filter_block_offset, 1200);
        assert_eq!(parsed.filter_block_len, 50);
        assert_eq!(parsed.props_block_offset, 1250);
        assert_eq!(parsed.props_block_len, 80);
        assert_eq!(parsed.index_type, INDEX_TYPE_MONOLITHIC);
    }

    #[test]
    fn footer_rejects_bad_magic() {
        let mut footer = encode_footer(0, 0, 0, 0, 0, 0, INDEX_TYPE_MONOLITHIC);
        footer[48] = b'X'; // corrupt magic
        assert!(parse_footer(&footer).is_none());
    }

    #[test]
    fn index_block_roundtrip() {
        let entries = vec![
            (b"key_a".to_vec(), 64u64, 1024u32),
            (b"key_b".to_vec(), 1100u64, 2048u32),
        ];
        let data = encode_index_block(&entries);
        let parsed = parse_index_block(&data).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].key, b"key_a");
        assert_eq!(parsed[0].block_offset, 64);
        assert_eq!(parsed[0].block_data_len, 1024);
        assert_eq!(parsed[1].key, b"key_b");
    }

    #[test]
    fn many_entries_creates_multiple_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let mt = Memtable::new(0);
        // Insert enough entries to exceed one 64KB data block
        for i in 0..2000u32 {
            let k = key(&format!("key_{:06}", i));
            let val = Value::String(format!("value_{}", "x".repeat(100)));
            mt.put(&k, i as u64 + 1, val, false);
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 4096, // small blocks for test
            bloom_bits_per_key: 10,
            compression: CompressionCodec::default(),
            rate_limiter: None,
        };
        let meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();
        assert_eq!(meta.entry_count, 2000);
        assert!(meta.file_size > 4096); // must span multiple blocks
    }

    #[test]
    fn v2_entry_encode_decode_with_timestamp_and_ttl() {
        let ik = InternalKey::encode(&key("ts_key"), 7);
        let ts = strata_core::Timestamp::from_micros(1_700_000_000_000_000);
        let entry = MemtableEntry {
            value: Value::String("with_ts".into()),
            is_tombstone: false,
            timestamp: ts,
            ttl_ms: 30_000,
        };

        let mut buf = Vec::new();
        encode_entry(&ik, &entry, &mut buf);

        let (decoded_ik, is_tomb, decoded_val, decoded_ts, decoded_ttl, consumed) =
            decode_entry(&buf).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(!is_tomb);
        assert_eq!(decoded_val, Value::String("with_ts".into()));
        assert_eq!(decoded_ts, 1_700_000_000_000_000);
        assert_eq!(decoded_ttl, 30_000);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn v2_tombstone_preserves_timestamp_and_ttl() {
        let ik = InternalKey::encode(&key("del_key"), 10);
        let ts = strata_core::Timestamp::from_micros(42_000_000);
        let entry = MemtableEntry {
            value: Value::Null,
            is_tombstone: true,
            timestamp: ts,
            ttl_ms: 5_000,
        };

        let mut buf = Vec::new();
        encode_entry(&ik, &entry, &mut buf);

        let (decoded_ik, is_tomb, _, decoded_ts, decoded_ttl, consumed) =
            decode_entry(&buf).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(is_tomb);
        assert_eq!(decoded_ts, 42_000_000);
        assert_eq!(decoded_ttl, 5_000);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn v2_segment_build_open_preserves_timestamps() {
        use crate::segment::KVSegment;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2ts.sst");

        let mt = Memtable::new(0);
        let ts = strata_core::Timestamp::from_micros(1_600_000_000_000_000);
        mt.put_entry(
            &key("a"),
            1,
            MemtableEntry {
                value: Value::Int(10),
                is_tombstone: false,
                timestamp: ts,
                ttl_ms: 60_000,
            },
        );
        mt.put_entry(
            &key("b"),
            2,
            MemtableEntry {
                value: Value::Null,
                is_tombstone: true,
                timestamp: strata_core::Timestamp::from_micros(999),
                ttl_ms: 0,
            },
        );
        mt.freeze();

        let builder = SegmentBuilder::default();
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();

        let e = seg.point_lookup(&key("a"), u64::MAX).unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert_eq!(e.timestamp, 1_600_000_000_000_000);
        assert_eq!(e.ttl_ms, 60_000);

        let e = seg.point_lookup(&key("b"), u64::MAX).unwrap();
        assert!(e.is_tombstone);
        assert_eq!(e.timestamp, 999);
        assert_eq!(e.ttl_ms, 0);
    }

    #[test]
    fn compressed_segment_roundtrip() {
        use crate::segment::KVSegment;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compressed.sst");

        let mt = Memtable::new(0);
        // Write repetitive data that compresses well
        for i in 0..100u32 {
            let k = key(&format!("key_{:06}", i));
            let val = Value::String(format!("value_{}", "abcdefgh".repeat(20)));
            mt.put(&k, i as u64 + 1, val, false);
        }
        mt.freeze();

        let builder = SegmentBuilder::default();
        let meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();
        assert_eq!(meta.entry_count, 100);

        // Reopen and verify all entries
        let seg = KVSegment::open(&path).unwrap();
        for i in 0..100u32 {
            let k = key(&format!("key_{:06}", i));
            let e = seg.point_lookup(&k, u64::MAX).unwrap();
            assert_eq!(
                e.value,
                Value::String(format!("value_{}", "abcdefgh".repeat(20)))
            );
        }
    }

    #[test]
    fn compressed_segment_multi_block_roundtrip() {
        use crate::segment::KVSegment;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compressed_multi.sst");

        let mt = Memtable::new(0);
        // Write enough repetitive data to span multiple blocks
        for i in 0..500u32 {
            let k = key(&format!("key_{:06}", i));
            let val = Value::String(format!("value_{}", "abcdefgh".repeat(20)));
            mt.put(&k, i as u64 + 1, val, false);
        }
        mt.freeze();

        // Use small block size to force multiple blocks
        let builder = SegmentBuilder {
            data_block_size: 4096,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::default(),
            rate_limiter: None,
        };
        let meta = builder.build_from_iter(mt.iter_all(), &path).unwrap();
        assert_eq!(meta.entry_count, 500);

        // Reopen and verify all entries via point lookup
        let seg = KVSegment::open(&path).unwrap();
        for i in 0..500u32 {
            let k = key(&format!("key_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap_or_else(|| panic!("missing key_{:06}", i));
            assert_eq!(
                e.value,
                Value::String(format!("value_{}", "abcdefgh".repeat(20)))
            );
            assert_eq!(e.commit_id, i as u64 + 1);
        }

        // Also verify iteration works across compressed blocks
        let all: Vec<_> = seg.iter_seek_all().collect();
        assert_eq!(all.len(), 500);
    }

    // ===== SplittingSegmentBuilder tests =====

    #[test]
    fn splitting_builder_single_file_if_small() {
        let dir = tempfile::tempdir().unwrap();

        let mt = Memtable::new(0);
        for i in 0..10u32 {
            mt.put(
                &key(&format!("k{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();

        let builder = SplittingSegmentBuilder::new(64 * 1024 * 1024); // 64MB
        let outputs = builder
            .build_split(mt.iter_all(), |idx| dir.path().join(format!("{}.sst", idx)))
            .unwrap();

        assert_eq!(outputs.len(), 1, "small input should produce 1 file");
        assert_eq!(outputs[0].1.entry_count, 10);

        let seg = crate::segment::KVSegment::open(&outputs[0].0).unwrap();
        assert_eq!(seg.entry_count(), 10);
    }

    #[test]
    fn splitting_builder_respects_target_size() {
        let dir = tempfile::tempdir().unwrap();

        // Create enough data to exceed a tiny target
        let mt = Memtable::new(0);
        for i in 0..1000u32 {
            mt.put(
                &key(&format!("k{:06}", i)),
                i as u64 + 1,
                Value::String("x".repeat(500)),
                false,
            );
        }
        mt.freeze();

        // 10KB target — should produce many files
        let builder = SplittingSegmentBuilder::new(10 * 1024);
        let outputs = builder
            .build_split(mt.iter_all(), |idx| dir.path().join(format!("{}.sst", idx)))
            .unwrap();

        assert!(
            outputs.len() > 1,
            "should produce multiple files with 10KB target, got {}",
            outputs.len()
        );

        // Total entries across all files should match input
        let total_entries: u64 = outputs.iter().map(|(_, m)| m.entry_count).sum();
        assert_eq!(total_entries, 1000);

        // Each file should be openable and readable
        for (path, meta) in &outputs {
            let seg = crate::segment::KVSegment::open(path).unwrap();
            assert_eq!(seg.entry_count(), meta.entry_count);
        }
    }

    #[test]
    fn splitting_builder_splits_at_key_boundaries() {
        let dir = tempfile::tempdir().unwrap();

        // Create entries with multiple versions per key
        let mt = Memtable::new(0);
        for i in 0..100u32 {
            let k = key(&format!("k{:04}", i));
            mt.put(&k, i as u64 * 2 + 1, Value::Int(1), false);
            mt.put(&k, i as u64 * 2 + 2, Value::Int(2), false);
        }
        mt.freeze();

        // Tiny target to force many splits
        let builder = SplittingSegmentBuilder::new(1024);
        let outputs = builder
            .build_split(mt.iter_all(), |idx| dir.path().join(format!("{}.sst", idx)))
            .unwrap();

        assert!(outputs.len() > 1);

        // Verify no key is split across files: each file's max key_prefix
        // should differ from the next file's min key_prefix
        for i in 1..outputs.len() {
            let prev_seg = crate::segment::KVSegment::open(&outputs[i - 1].0).unwrap();
            let curr_seg = crate::segment::KVSegment::open(&outputs[i].0).unwrap();
            let (_, prev_max) = prev_seg.key_range();
            let (curr_min, _) = curr_seg.key_range();
            // Strip commit_id to compare typed_key_prefix
            if prev_max.len() >= 8 && curr_min.len() >= 8 {
                let prev_prefix = &prev_max[..prev_max.len() - 8];
                let curr_prefix = &curr_min[..curr_min.len() - 8];
                assert!(
                    prev_prefix < curr_prefix,
                    "keys should not span file boundaries"
                );
            }
        }
    }

    // ===== Restart point tests (v3 format) =====

    #[test]
    fn restart_trailer_roundtrip() {
        // Build a block buffer manually and verify trailer offsets
        let mut buf = Vec::new();
        let mut offsets = vec![0u32];
        let mut entry_count = 0usize;

        // Write 32 entries, capturing offsets at restart boundaries
        for i in 0..32u32 {
            if entry_count > 0 && entry_count % RESTART_INTERVAL == 0 {
                offsets.push(buf.len() as u32);
            }
            let k = key(&format!("k{:04}", i));
            let ik = InternalKey::encode(&k, i as u64 + 1);
            let entry = MemtableEntry {
                value: Value::Int(i as i64),
                is_tombstone: false,
                timestamp: strata_core::Timestamp::now(),
                ttl_ms: 0,
            };
            encode_entry(&ik, &entry, &mut buf);
            entry_count += 1;
        }

        assert_eq!(offsets.len(), 2); // offsets at entry 0 and entry 16

        // Append trailer
        let data_end = buf.len();
        append_restart_trailer(&mut buf, &offsets);

        // Verify trailer can be parsed
        let (parsed_data_end, num_restarts) = crate::segment::parse_restart_trailer(&buf).unwrap();
        assert_eq!(parsed_data_end, data_end);
        assert_eq!(num_restarts, 2);
    }

    #[test]
    fn restart_interval_boundaries() {
        use crate::segment::parse_restart_trailer;

        let build_and_count_restarts = |n: u32| -> usize {
            let mut buf = Vec::new();
            let mut offsets = vec![0u32];
            let mut count = 0usize;
            for i in 0..n {
                if count > 0 && count % RESTART_INTERVAL == 0 {
                    offsets.push(buf.len() as u32);
                }
                let k = key(&format!("k{:04}", i));
                let ik = InternalKey::encode(&k, i as u64 + 1);
                let entry = MemtableEntry {
                    value: Value::Int(i as i64),
                    is_tombstone: false,
                    timestamp: strata_core::Timestamp::now(),
                    ttl_ms: 0,
                };
                encode_entry(&ik, &entry, &mut buf);
                count += 1;
            }
            append_restart_trailer(&mut buf, &offsets);
            let (_, num) = parse_restart_trailer(&buf).unwrap();
            num
        };

        // 1 entry → 1 restart (at offset 0)
        assert_eq!(build_and_count_restarts(1), 1);
        // 16 entries → 1 restart (at offset 0; entry 16 would be the next)
        assert_eq!(build_and_count_restarts(16), 1);
        // 17 entries → 2 restarts (at entry 0 and entry 16)
        assert_eq!(build_and_count_restarts(17), 2);
        // 32 entries → 2 restarts (at entry 0 and entry 16)
        assert_eq!(build_and_count_restarts(32), 2);
        // 33 entries → 3 restarts (at entry 0, 16, 32)
        assert_eq!(build_and_count_restarts(33), 3);
        // 48 entries → 3 restarts (at entry 0, 16, 32)
        assert_eq!(build_and_count_restarts(48), 3);
    }

    // ===== v4 prefix compression tests =====

    #[test]
    fn varint32_roundtrip() {
        for val in [0u32, 1, 127, 128, 255, 16383, 16384, u32::MAX] {
            let mut buf = Vec::new();
            encode_varint32(&mut buf, val);
            let (decoded, consumed) = decode_varint32(&buf).unwrap();
            assert_eq!(decoded, val, "mismatch for {}", val);
            assert_eq!(consumed, buf.len(), "consumed mismatch for {}", val);
        }
    }

    #[test]
    fn common_prefix_len_basic() {
        assert_eq!(common_prefix_len(b"hello", b"hello"), 5);
        assert_eq!(common_prefix_len(b"hello", b"help"), 3);
        assert_eq!(common_prefix_len(b"hello", b"world"), 0);
        assert_eq!(common_prefix_len(b"", b"hello"), 0);
        assert_eq!(common_prefix_len(b"hello", b""), 0);
        assert_eq!(common_prefix_len(b"", b""), 0);
        assert_eq!(common_prefix_len(b"abc", b"abcdef"), 3);
    }

    #[test]
    fn varint32_rejects_overflow() {
        // 5 bytes with the 5th byte having value 0x10 (16), which exceeds
        // the 4 bits available at bit position 28-31 of a u32.
        // This must return None, not panic.
        let corrupt = [0x80, 0x80, 0x80, 0x80, 0x10];
        assert!(decode_varint32(&corrupt).is_none());

        // 5 bytes all with continuation bit set — no terminator
        let no_term = [0x80, 0x80, 0x80, 0x80, 0x80];
        assert!(decode_varint32(&no_term).is_none());

        // 5th byte with max valid value (0x0F) should succeed
        let max_valid = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
        let (val, consumed) = decode_varint32(&max_valid).unwrap();
        assert_eq!(val, u32::MAX);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn v4_entry_encode_decode_roundtrip() {
        let ik = InternalKey::encode(&key("hello"), 42);
        let entry = MemtableEntry {
            value: Value::String("world".into()),
            is_tombstone: false,
            timestamp: strata_core::Timestamp::now(),
            ttl_ms: 0,
        };

        let mut buf = Vec::new();
        encode_entry_v4(&[], &ik, &entry, &mut buf, true);

        let mut prev_key = Vec::new();
        let (decoded_ik, is_tomb, decoded_val, ts, ttl, consumed) =
            decode_entry_v4(&buf, &mut prev_key).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(!is_tomb);
        assert_eq!(decoded_val, Value::String("world".into()));
        assert_eq!(ts, entry.timestamp.as_micros());
        assert_eq!(ttl, 0);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn v4_tombstone_entry_roundtrip() {
        let ik = InternalKey::encode(&key("gone"), 99);
        let entry = MemtableEntry {
            value: Value::Null,
            is_tombstone: true,
            timestamp: strata_core::Timestamp::now(),
            ttl_ms: 5_000,
        };

        let mut buf = Vec::new();
        encode_entry_v4(&[], &ik, &entry, &mut buf, true);

        let mut prev_key = Vec::new();
        let (decoded_ik, is_tomb, _, ts, ttl, consumed) =
            decode_entry_v4(&buf, &mut prev_key).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(is_tomb);
        assert_eq!(ts, entry.timestamp.as_micros());
        assert_eq!(ttl, 5_000);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn v4_prefix_shared_bytes_correct() {
        let k1 = key("user:alice");
        let k2 = key("user:alice_data");
        let ik1 = InternalKey::encode(&k1, 1);
        let ik2 = InternalKey::encode(&k2, 2);

        let entry = MemtableEntry {
            value: Value::Int(1),
            is_tombstone: false,
            timestamp: strata_core::Timestamp::now(),
            ttl_ms: 0,
        };

        let mut buf = Vec::new();
        // First entry: restart, shared=0
        encode_entry_v4(&[], &ik1, &entry, &mut buf, true);
        let first_len = buf.len();

        // Second entry: non-restart, should share prefix
        encode_entry_v4(ik1.as_bytes(), &ik2, &entry, &mut buf, false);

        // Decode first entry
        let mut prev_key = Vec::new();
        let (dec_ik1, ..) = decode_entry_v4(&buf[..first_len], &mut prev_key).unwrap();
        assert_eq!(dec_ik1, ik1);

        // Decode second entry (prev_key is now ik1)
        let (dec_ik2, ..) = decode_entry_v4(&buf[first_len..], &mut prev_key).unwrap();
        assert_eq!(dec_ik2, ik2);

        // Verify sharing actually happened: second entry should be smaller
        let second_len = buf.len() - first_len;
        let shared = common_prefix_len(ik1.as_bytes(), ik2.as_bytes());
        assert!(shared > 0, "keys should share some prefix");
        assert!(
            second_len < first_len,
            "prefix-compressed entry should be smaller"
        );
    }

    #[test]
    fn v4_restart_entries_have_shared_zero() {
        let mut buf = Vec::new();
        let mut prev_key: Vec<u8> = Vec::new();

        // Write 32 entries to get at least 2 restart points
        for i in 0..32u32 {
            let k = key(&format!("k{:04}", i));
            let ik = InternalKey::encode(&k, i as u64 + 1);
            let entry = MemtableEntry {
                value: Value::Int(i as i64),
                is_tombstone: false,
                timestamp: strata_core::Timestamp::now(),
                ttl_ms: 0,
            };
            let is_restart = i == 0 || i % RESTART_INTERVAL as u32 == 0;
            encode_entry_v4(&prev_key, &ik, &entry, &mut buf, is_restart);
            prev_key = ik.as_bytes().to_vec();
        }

        // Scan and verify restart entries have shared=0
        let mut pos = 0;
        let mut entry_idx = 0u32;
        let mut pk = Vec::new();
        while pos < buf.len() {
            let (shared, _n1) = decode_varint32(&buf[pos..]).unwrap();
            if entry_idx == 0 || entry_idx % RESTART_INTERVAL as u32 == 0 {
                assert_eq!(
                    shared, 0,
                    "entry {} should be a restart with shared=0",
                    entry_idx
                );
            } else {
                assert!(shared > 0, "entry {} should share prefix", entry_idx);
            }
            // Advance to next entry
            let hdr = decode_entry_header_v4(&buf[pos..], &mut pk).unwrap();
            pos += hdr.total_len;
            entry_idx += 1;
        }
        assert_eq!(entry_idx, 32);
    }

    #[test]
    fn v4_build_and_read_roundtrip() {
        use crate::segment::KVSegment;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4_roundtrip.sst");

        let mt = Memtable::new(0);
        for i in 0..100u32 {
            let k = key(&format!("key_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();

        let builder = SegmentBuilder::default();
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        assert_eq!(seg.format_version(), 7);

        // Point lookup every entry
        for i in 0..100u32 {
            let k = key(&format!("key_{:06}", i));
            let e = seg
                .point_lookup(&k, u64::MAX)
                .unwrap_or_else(|| panic!("missing key_{:06}", i));
            assert_eq!(e.value, Value::Int(i as i64));
            assert_eq!(e.commit_id, i as u64 + 1);
        }
    }

    #[test]
    fn v4_compression_ratio() {
        let mt = Memtable::new(0);
        for i in 0..200u32 {
            let k = key(&format!("key_{:06}", i));
            mt.put(&k, i as u64 + 1, Value::Int(i as i64), false);
        }
        mt.freeze();

        let dir = tempfile::tempdir().unwrap();

        // Build v3
        let v3_path = dir.path().join("v3.sst");
        let builder = SegmentBuilder::default();
        let v3_meta = builder.build_from_iter_v3(mt.iter_all(), &v3_path).unwrap();

        // Build v4
        let v4_path = dir.path().join("v4.sst");
        let v4_meta = builder.build_from_iter(mt.iter_all(), &v4_path).unwrap();

        // v4 (with prefix compression) should be close to v3 in size.
        // v6 adds a hash index (~4-5% overhead per block), so v4+hash may be
        // slightly larger than v3 for small datasets. Check it's within 10%.
        let ratio = v4_meta.file_size as f64 / v3_meta.file_size as f64;
        assert!(
            ratio < 1.10,
            "v4/v6 ({}) should be within 10% of v3 ({}), ratio={:.3}",
            v4_meta.file_size,
            v3_meta.file_size,
            ratio,
        );
    }

    #[test]
    fn v4_mvcc_versions() {
        use crate::segment::KVSegment;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4_mvcc.sst");

        let mt = Memtable::new(0);
        let k = key("k");
        mt.put(&k, 1, Value::Int(10), false);
        mt.put(&k, 5, Value::Int(50), false);
        mt.put(&k, 10, Value::Int(100), false);
        mt.freeze();

        let builder = SegmentBuilder::default();
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();

        let e = seg.point_lookup(&k, 10).unwrap();
        assert_eq!(e.value, Value::Int(100));
        assert_eq!(e.commit_id, 10);

        let e = seg.point_lookup(&k, 7).unwrap();
        assert_eq!(e.value, Value::Int(50));
        assert_eq!(e.commit_id, 5);

        let e = seg.point_lookup(&k, 1).unwrap();
        assert_eq!(e.value, Value::Int(10));
        assert_eq!(e.commit_id, 1);

        assert!(seg.point_lookup(&k, 0).is_none());
    }

    // -----------------------------------------------------------------------
    // Index key shortening tests (Epic 29)
    // -----------------------------------------------------------------------

    #[test]
    fn test_shortest_separator_basic() {
        // a[2]=c, b[2]=e, c+1=d, d < e → true → "abd"
        assert_eq!(shortest_separator(b"abcd", b"abef"), b"abd");
    }

    #[test]
    fn test_shortest_separator_adjacent() {
        // a[2]=c, b[2]=d, c+1=d, d < d → false → unchanged
        assert_eq!(shortest_separator(b"abc", b"abd"), b"abc".to_vec());
    }

    #[test]
    fn test_shortest_separator_prefix_relationship() {
        // "foo" is a prefix of "foobar" → unchanged
        assert_eq!(shortest_separator(b"foo", b"foobar"), b"foo".to_vec());
    }

    #[test]
    fn test_shortest_separator_long_shared_prefix() {
        let mut a = vec![b'x'; 32];
        let mut b = vec![b'x'; 32];
        a.push(b'a');
        b.push(b'z');
        let result = shortest_separator(&a, &b);
        // a[32]=a, b[32]=z, a+1=b, b < z → true → 33 bytes with last = 'b'
        assert_eq!(result.len(), 33);
        assert_eq!(result[32], b'b');
    }

    #[test]
    fn test_shortest_successor_basic() {
        // First non-0xFF byte is 'a' at index 0, increment → 'b', truncate
        assert_eq!(shortest_successor(b"abc"), b"b".to_vec());
    }

    #[test]
    fn test_shortest_successor_trailing_ff() {
        // First non-0xFF byte is 'a' at index 0, increment → 'b', truncate
        assert_eq!(shortest_successor(b"ab\xff"), b"b".to_vec());
    }

    #[test]
    fn test_shortest_successor_all_ff() {
        assert_eq!(shortest_successor(b"\xff\xff"), b"\xff\xff".to_vec());
    }

    #[test]
    fn test_shorten_index_key_preserves_suffix() {
        let mut a = b"abcd".to_vec();
        a.extend_from_slice(&42_u64.to_be_bytes());
        let mut b = b"abef".to_vec();
        b.extend_from_slice(&99_u64.to_be_bytes());

        let result = shorten_index_key(&a, &b);
        // Prefix shortened: "abd", then u64::MAX suffix
        assert!(result.len() >= 8);
        let suffix = &result[result.len() - 8..];
        assert_eq!(suffix, &(!0_u64).to_be_bytes());
        let prefix = &result[..result.len() - 8];
        assert_eq!(prefix, b"abd");
    }

    #[test]
    fn test_shorten_index_key_same_prefix_fallback() {
        // When typed_key_prefix is identical (keys differ only in commit_id),
        // shorten_index_key must fall back to the full prev_last key to
        // preserve the invariant: separator < next_first.
        let prefix = b"same_prefix";
        let mut a = prefix.to_vec();
        a.extend_from_slice(&(!50_u64).to_be_bytes()); // commit_id 50
        let mut b = prefix.to_vec();
        b.extend_from_slice(&(!49_u64).to_be_bytes()); // commit_id 49

        let result = shorten_index_key(&a, &b);
        // Must equal the full prev_last key, NOT prefix + u64::MAX
        assert_eq!(result, a);
        // Verify the invariant: a <= result < b
        assert!(result.as_slice() >= a.as_slice());
        assert!(result.as_slice() < b.as_slice());
    }

    #[test]
    fn test_shorten_index_key_adjacent_byte_no_shorten() {
        // Keys like "key_0005" vs "key_0006" differ by exactly 1 at the
        // differing byte, so shortest_separator can't shorten. Must fall
        // back to the full key.
        let mut a = b"key_0005".to_vec();
        a.extend_from_slice(&(!1_u64).to_be_bytes());
        let mut b = b"key_0006".to_vec();
        b.extend_from_slice(&(!2_u64).to_be_bytes());

        let result = shorten_index_key(&a, &b);
        // Can't shorten: '5'+1 = '6', '6' < '6' is false → full key
        assert_eq!(result, a);
    }

    #[test]
    fn test_shorten_index_key_invariant_holds() {
        // Test the separator invariant for various key pairs.
        // All prefixes must have equal length so that the appended commit_id
        // suffix doesn't create byte-order anomalies (in real segments, keys
        // with different typed_key_prefix lengths never have the shorter one
        // sort before the longer one because commit_id bytes are near 0xFF).
        let cases: Vec<(&[u8], &[u8])> = vec![
            (b"aaa", b"zzz"),         // large gap → shortened
            (b"abc", b"abd"),         // adjacent → not shortened
            (b"abc", b"abc"),         // identical prefix → not shortened (same commit_id scenario)
            (b"tes", b"tex"),         // 's' vs 'x': shortened to "tet"
            (b"key0042", b"key0100"), // realistic sequential keys with gap
        ];
        for (a_raw, b_raw) in cases {
            let mut a = a_raw.to_vec();
            a.extend_from_slice(&(!10_u64).to_be_bytes());
            let mut b = b_raw.to_vec();
            b.extend_from_slice(&(!20_u64).to_be_bytes());

            // Skip cases where a >= b (identical prefix with different commit_id
            // ordering is handled by the same-prefix fallback path).
            if a.as_slice() >= b.as_slice() {
                // For same-prefix case, verify fallback gives a == separator
                let sep = shorten_index_key(&a, &b);
                assert_eq!(sep, a, "same-prefix case must return prev_last");
                continue;
            }

            let sep = shorten_index_key(&a, &b);
            assert!(
                sep.as_slice() >= a.as_slice(),
                "separator {:?} must be >= a {:?} (prefixes {:?} vs {:?})",
                sep,
                a,
                a_raw,
                b_raw
            );
            assert!(
                sep.as_slice() < b.as_slice(),
                "separator {:?} must be < b {:?} (prefixes {:?} vs {:?})",
                sep,
                b,
                a_raw,
                b_raw
            );
        }
    }

    #[test]
    fn test_shorten_index_key_roundtrip_unique_keys() {
        // Build a multi-block segment with unique keys, verify all lookups.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shortened.sst");

        let mt = Memtable::new(0);
        for i in 0..200 {
            mt.put(
                &key(&format!("key_{:04}", i)),
                i as u64 + 1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 512,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::default(),
            rate_limiter: None,
        };
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = crate::segment::KVSegment::open(&path).unwrap();

        for i in 0..200 {
            let k = key(&format!("key_{:04}", i));
            let e = seg.point_lookup(&k, i as u64 + 1);
            assert!(e.is_some(), "key_{:04} not found", i);
            assert_eq!(e.unwrap().value, Value::Int(i as i64));
        }
    }

    #[test]
    fn test_shorten_index_key_roundtrip_multi_version() {
        // Critical edge case: many versions of the same key spanning multiple
        // blocks. Adjacent blocks share the same typed_key_prefix, so the
        // separator must fall back to the full key (not prefix + u64::MAX).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi_version.sst");

        let mt = Memtable::new(0);
        let k = key("hotkey");
        // Write 200 versions of the same key → will span many 512-byte blocks
        for commit in 1..=200_u64 {
            mt.put(&k, commit, Value::Int(commit as i64), false);
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 512,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::default(),
            rate_limiter: None,
        };
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = crate::segment::KVSegment::open(&path).unwrap();

        // Verify every version is accessible at its own snapshot
        for commit in 1..=200_u64 {
            let e = seg.point_lookup(&k, commit);
            assert!(e.is_some(), "commit {} not found", commit);
            let entry = e.unwrap();
            assert_eq!(entry.value, Value::Int(commit as i64));
            assert_eq!(entry.commit_id, commit);
        }

        // Also verify the latest snapshot returns the newest version
        let e = seg.point_lookup(&k, 200).unwrap();
        assert_eq!(e.value, Value::Int(200));
    }

    #[test]
    fn test_shorten_index_key_roundtrip_mixed() {
        // Mix of unique keys and multi-version keys to exercise both
        // shortening paths in the same segment.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.sst");

        let mt = Memtable::new(0);
        // 50 unique keys
        for i in 0..50 {
            mt.put(
                &key(&format!("alpha_{:04}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        // 100 versions of one key
        let hot = key("beta_hot");
        for c in 1..=100_u64 {
            mt.put(&hot, c, Value::Int(c as i64), false);
        }
        // 50 more unique keys
        for i in 0..50 {
            mt.put(
                &key(&format!("gamma_{:04}", i)),
                1,
                Value::Int(i as i64),
                false,
            );
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 512,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::default(),
            rate_limiter: None,
        };
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = crate::segment::KVSegment::open(&path).unwrap();

        // Check unique alpha keys
        for i in 0..50 {
            let k = key(&format!("alpha_{:04}", i));
            let e = seg.point_lookup(&k, 1);
            assert!(e.is_some(), "alpha_{:04} not found", i);
        }
        // Check hot key versions
        for c in 1..=100_u64 {
            let e = seg.point_lookup(&hot, c);
            assert!(e.is_some(), "beta_hot commit {} not found", c);
            assert_eq!(e.unwrap().commit_id, c);
        }
        // Check unique gamma keys
        for i in 0..50 {
            let k = key(&format!("gamma_{:04}", i));
            let e = seg.point_lookup(&k, 1);
            assert!(e.is_some(), "gamma_{:04} not found", i);
        }
    }

    #[test]
    fn test_index_size_reduction() {
        // Use keys with large gaps in the user key portion to ensure actual
        // shortening occurs. Keys like "a_<pad>", "c_<pad>", "e_<pad>"...
        // give gaps of 2 at the differing byte, enabling shortening.
        let dir = tempfile::tempdir().unwrap();

        let mt = Memtable::new(0);
        let letters: Vec<char> = ('a'..='z').collect();
        let mut idx = 0u64;
        for &c1 in &letters {
            for &c2 in &letters {
                // Each key has a long suffix to inflate the full key size,
                // making the shortening savings more visible.
                let user_key = format!("{}{}_padding_to_make_key_longer_{:04}", c1, c2, idx);
                mt.put(&key(&user_key), idx + 1, Value::Int(idx as i64), false);
                idx += 1;
            }
        }
        mt.freeze();

        let builder = SegmentBuilder {
            data_block_size: 512,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::default(),
            rate_limiter: None,
        };

        let path_v3 = dir.path().join("full_keys.sst");
        let meta_v3 = builder.build_from_iter_v3(mt.iter_all(), &path_v3).unwrap();

        let path_v5 = dir.path().join("short_keys.sst");
        let meta_v5 = builder.build_from_iter(mt.iter_all(), &path_v5).unwrap();

        assert!(
            meta_v5.file_size < meta_v3.file_size,
            "shortened segment ({}) should be strictly smaller than full-key segment ({})",
            meta_v5.file_size,
            meta_v3.file_size,
        );

        // Verify the shortened segment is still fully readable
        let seg = crate::segment::KVSegment::open(&path_v5).unwrap();
        idx = 0;
        for &c1 in &letters {
            for &c2 in &letters {
                let user_key = format!("{}{}_padding_to_make_key_longer_{:04}", c1, c2, idx);
                let k = key(&user_key);
                let e = seg.point_lookup(&k, idx + 1);
                assert!(e.is_some(), "key {} not found", user_key);
                assert_eq!(e.unwrap().value, Value::Int(idx as i64));
                idx += 1;
            }
        }
    }

    #[test]
    fn codec_none_roundtrip_multi_block() {
        // Use small blocks (512B) + enough entries to span multiple data blocks,
        // exercising cross-block iteration and point lookups with no compression.
        let dir = tempfile::tempdir().unwrap();
        let path_none = dir.path().join("none.sst");
        let path_zstd = dir.path().join("zstd.sst");

        let mt = Memtable::new(0);
        for i in 0..200u64 {
            // Long values to ensure data spans multiple 512B blocks
            let val = Value::String(format!("value_{:06}_padding_to_inflate_size", i));
            mt.put(&key(&format!("k_{:04}", i)), i + 1, val, false);
        }
        mt.freeze();

        let builder_none = SegmentBuilder {
            data_block_size: 512,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::None,
            rate_limiter: None,
        };
        builder_none
            .build_from_iter(mt.iter_all(), &path_none)
            .unwrap();

        let builder_zstd = SegmentBuilder {
            data_block_size: 512,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::Zstd(3),
            rate_limiter: None,
        };
        builder_zstd
            .build_from_iter(mt.iter_all(), &path_zstd)
            .unwrap();

        // Uncompressed file should be larger than compressed
        let none_size = std::fs::metadata(&path_none).unwrap().len();
        let zstd_size = std::fs::metadata(&path_zstd).unwrap().len();
        assert!(
            none_size > zstd_size,
            "Uncompressed ({}) should be larger than Zstd ({})",
            none_size,
            zstd_size
        );

        // Verify point lookups on the uncompressed segment
        let seg = KVSegment::open(&path_none).unwrap();
        for i in 0..200u64 {
            let e = seg.point_lookup(&key(&format!("k_{:04}", i)), i + 1);
            assert!(e.is_some(), "key k_{:04} not found with None codec", i);
            let expected = Value::String(format!("value_{:06}_padding_to_inflate_size", i));
            assert_eq!(e.unwrap().value, expected);
        }

        // Verify full iteration yields all entries
        let iter = crate::segment::OwnedSegmentIter::new(Arc::new(seg));
        let count = iter.count();
        assert_eq!(count, 200, "Iterator should yield all 200 entries");

        // Verify MVCC snapshot isolation: looking up a key at an older snapshot
        // should not see newer versions
        let seg = KVSegment::open(&path_none).unwrap();
        let e = seg.point_lookup(&key("k_0100"), 50);
        assert!(
            e.is_none(),
            "key k_0100 at commit 50 should not be visible (written at 101)"
        );
    }

    #[test]
    fn codec_zstd_levels_roundtrip_multi_block() {
        // Test that different zstd levels all produce valid segments
        // with correct data across multiple blocks.
        for zstd_level in [1, 3, 6, 9] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("zstd_{}.sst", zstd_level));

            let mt = Memtable::new(0);
            for i in 0..200u64 {
                let val = Value::String(format!("value_{:06}_padding_to_inflate_size", i));
                mt.put(&key(&format!("k_{:04}", i)), i + 1, val, false);
            }
            mt.freeze();

            let builder = SegmentBuilder {
                data_block_size: 512,
                bloom_bits_per_key: 10,
                compression: CompressionCodec::Zstd(zstd_level),
                rate_limiter: None,
            };
            builder.build_from_iter(mt.iter_all(), &path).unwrap();

            let seg = KVSegment::open(&path).unwrap();

            // Point lookups: first, last, and middle keys
            for &i in &[0u64, 99, 199] {
                let e = seg.point_lookup(&key(&format!("k_{:04}", i)), i + 1);
                assert!(
                    e.is_some(),
                    "key k_{:04} not found with Zstd({})",
                    i,
                    zstd_level
                );
                let expected = Value::String(format!("value_{:06}_padding_to_inflate_size", i));
                assert_eq!(e.unwrap().value, expected);
            }

            // Full iteration
            let iter = crate::segment::OwnedSegmentIter::new(Arc::new(seg));
            let count = iter.count();
            assert_eq!(
                count, 200,
                "Zstd({}) iterator should yield all 200 entries",
                zstd_level
            );
        }

        // Higher zstd levels should produce smaller (or equal) files than lower levels
        let dir = tempfile::tempdir().unwrap();
        let mt = Memtable::new(0);
        for i in 0..500u64 {
            let val = Value::String(format!("value_{:08}_repeated_data_for_compression", i));
            mt.put(&key(&format!("k_{:04}", i)), i + 1, val, false);
        }
        mt.freeze();

        let mut sizes = Vec::new();
        for zstd_level in [1, 3, 6] {
            let path = dir.path().join(format!("size_cmp_{}.sst", zstd_level));
            let builder = SegmentBuilder {
                data_block_size: 512,
                bloom_bits_per_key: 10,
                compression: CompressionCodec::Zstd(zstd_level),
                rate_limiter: None,
            };
            builder.build_from_iter(mt.iter_all(), &path).unwrap();
            sizes.push((zstd_level, std::fs::metadata(&path).unwrap().len()));
        }
        // Level 6 should be <= level 1 (higher level = better compression)
        assert!(
            sizes[2].1 <= sizes[0].1,
            "Zstd(6) size {} should be <= Zstd(1) size {}",
            sizes[2].1,
            sizes[0].1
        );
    }

    #[test]
    fn codec_none_with_tombstones() {
        // Ensure tombstones round-trip correctly with no compression
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tombstones.sst");

        let mt = Memtable::new(0);
        for i in 0..20u64 {
            mt.put(
                &key(&format!("k_{:04}", i)),
                i + 1,
                Value::Int(i as i64),
                false,
            );
        }
        // Tombstone every other key at a higher commit
        for i in (0..20u64).step_by(2) {
            mt.put(&key(&format!("k_{:04}", i)), 100 + i, Value::Null, true);
        }
        mt.freeze();

        let builder = SegmentBuilder::default().with_compression(CompressionCodec::None);
        builder.build_from_iter(mt.iter_all(), &path).unwrap();

        let seg = KVSegment::open(&path).unwrap();
        // Even keys should have tombstone at commit 100+i
        for i in (0..20u64).step_by(2) {
            let e = seg.point_lookup(&key(&format!("k_{:04}", i)), 200);
            assert!(e.is_some(), "tombstone k_{:04} should be visible", i);
            assert!(e.unwrap().is_tombstone, "k_{:04} should be a tombstone", i);
        }
        // Odd keys should still have their values
        for i in (1..20u64).step_by(2) {
            let e = seg.point_lookup(&key(&format!("k_{:04}", i)), 200);
            assert!(e.is_some());
            assert_eq!(e.unwrap().value, Value::Int(i as i64));
        }
    }
}

// ---------------------------------------------------------------------------
// SplittingSegmentBuilder — splits output at target file size
// ---------------------------------------------------------------------------

/// Builds multiple segment files, splitting at `target_file_size` boundaries.
///
/// Wraps `SegmentBuilder` and automatically finalizes the current segment
/// and starts a new one when the output exceeds the target size. Splits
/// only at key boundaries (never between versions of the same logical key).
pub struct SplittingSegmentBuilder {
    /// Underlying builder configuration.
    inner: SegmentBuilder,
    /// Target file size in bytes (default 64MB).
    pub target_file_size: u64,
}

impl SplittingSegmentBuilder {
    /// Create a new splitting builder with the given target file size.
    pub fn new(target_file_size: u64) -> Self {
        Self {
            inner: SegmentBuilder::default(),
            target_file_size,
        }
    }

    /// Set the bloom filter bits per key for output segments.
    pub fn with_bloom_bits(mut self, bits_per_key: usize) -> Self {
        self.inner.bloom_bits_per_key = bits_per_key;
        self
    }

    /// Set the compression codec for output segments.
    pub fn with_compression(mut self, codec: CompressionCodec) -> Self {
        self.inner.compression = codec;
        self
    }

    /// Set a rate limiter for throttling data block writes during compaction.
    pub fn with_rate_limiter(
        mut self,
        limiter: std::sync::Arc<crate::rate_limiter::RateLimiter>,
    ) -> Self {
        self.inner.rate_limiter = Some(limiter);
        self
    }

    /// Build one or more segment files from a sorted iterator.
    ///
    /// Returns `(path, metadata)` for each output segment. The `path_fn`
    /// closure is called with a split index (0, 1, 2, ...) to generate
    /// the path for each output segment.
    ///
    /// Split points are chosen at logical key boundaries — all versions
    /// of a given key stay in the same segment.
    pub fn build_split<I, F>(
        &self,
        iter: I,
        path_fn: F,
    ) -> io::Result<Vec<(std::path::PathBuf, SegmentMeta)>>
    where
        I: Iterator<Item = (InternalKey, MemtableEntry)>,
        F: Fn(usize) -> std::path::PathBuf,
    {
        self.build_split_with_predicate(iter, path_fn, |_| false)
    }

    /// Build one or more segment files, with an additional split predicate.
    ///
    /// In addition to splitting at `target_file_size` boundaries, the caller
    /// can supply a `should_split` predicate that receives the typed_key_prefix
    /// of each new logical key.  When the predicate returns `true`, a split is
    /// forced even if the current segment hasn't reached `target_file_size`.
    ///
    /// This is used for grandparent-aware splitting during leveled compaction:
    /// the predicate tracks cumulative overlap with L+2 files and forces a
    /// split when the overlap exceeds a threshold.
    pub fn build_split_with_predicate<I, F, P>(
        &self,
        iter: I,
        path_fn: F,
        mut should_split: P,
    ) -> io::Result<Vec<(std::path::PathBuf, SegmentMeta)>>
    where
        I: Iterator<Item = (InternalKey, MemtableEntry)>,
        F: Fn(usize) -> std::path::PathBuf,
        P: FnMut(&[u8]) -> bool,
    {
        let mut results: Vec<(std::path::PathBuf, SegmentMeta)> = Vec::new();
        let mut split_idx: usize = 0;

        // Buffer entries for the current segment
        let mut current_entries: Vec<(InternalKey, MemtableEntry)> = Vec::new();
        let mut current_bytes: u64 = 0;
        let mut last_typed_key: Option<Vec<u8>> = None;

        for (ik, entry) in iter {
            let typed_key = ik.typed_key_prefix().to_vec();

            // At a key boundary (new logical key), check if we should split
            if last_typed_key.as_ref() != Some(&typed_key)
                && !current_entries.is_empty()
                && (current_bytes >= self.target_file_size || should_split(&typed_key))
            {
                let path = path_fn(split_idx);
                let meta = self
                    .inner
                    .build_from_iter(current_entries.drain(..), &path)?;
                if meta.entry_count > 0 {
                    results.push((path, meta));
                    split_idx += 1;
                }
                current_bytes = 0;
            }

            last_typed_key = Some(typed_key);

            // Estimate entry size: ik + 25 bytes header + value
            let entry_size = ik.as_bytes().len() as u64 + 25 + estimate_value_size(&entry);
            current_bytes += entry_size;
            current_entries.push((ik, entry));
        }

        // Flush remaining entries
        if !current_entries.is_empty() {
            let path = path_fn(split_idx);
            let meta = self
                .inner
                .build_from_iter(current_entries.drain(..), &path)?;
            if meta.entry_count > 0 {
                results.push((path, meta));
            }
        }

        Ok(results)
    }
}

impl Default for SplittingSegmentBuilder {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024) // 64MB
    }
}

/// Estimate serialized value size without actually serializing.
fn estimate_value_size(entry: &MemtableEntry) -> u64 {
    if entry.is_tombstone {
        0
    } else {
        match &entry.value {
            strata_core::value::Value::Null => 1,
            strata_core::value::Value::Bool(_) => 2,
            strata_core::value::Value::Int(_) => 9,
            strata_core::value::Value::Float(_) => 9,
            strata_core::value::Value::String(s) => 8 + s.len() as u64,
            strata_core::value::Value::Bytes(b) => 8 + b.len() as u64,
            _ => 64, // arrays, objects — rough estimate
        }
    }
}
