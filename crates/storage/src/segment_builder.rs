//! Segment builder — flushes sorted entries into an immutable KV segment file.
//!
//! Takes a sorted iterator of `(InternalKey, MemtableEntry)` and produces a
//! segment file with the layout:
//!
//! ```text
//! | KVHeader (64 bytes)       |
//! | DataBlock 0..N-1          |
//! | IndexBlock                |
//! | BloomFilterBlock          |
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
const FORMAT_VERSION: u16 = 4;

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
// SegmentBuilder
// ---------------------------------------------------------------------------

/// Builds an immutable KV segment file from sorted entries.
pub struct SegmentBuilder {
    /// Target data block size in bytes (before framing).
    pub data_block_size: usize,
    /// Bloom filter bits per key.
    pub bloom_bits_per_key: usize,
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        Self {
            data_block_size: 64 * 1024, // 64 KiB
            bloom_bits_per_key: 10,
        }
    }
}

impl SegmentBuilder {
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

        // Bloom filter keys (TypedKeyBytes, deduplicated)
        let mut bloom_keys: Vec<Vec<u8>> = Vec::new();
        let mut last_typed_key: Option<Vec<u8>> = None;

        // Track current block's first key
        let mut block_first_key: Option<Vec<u8>> = None;

        // Restart point tracking
        let mut block_entry_count: usize = 0;
        let mut restart_offsets: Vec<u32> = vec![0]; // entry 0 always at offset 0

        // v4 prefix compression state
        let mut prev_key: Vec<u8> = Vec::new();

        let mut file_offset = KV_HEADER_SIZE as u64;

        for (ik, entry) in iter {
            let commit_id = ik.commit_id();
            commit_min = commit_min.min(commit_id);
            commit_max = commit_max.max(commit_id);

            if first_key.is_none() {
                first_key = Some(ik.as_bytes().to_vec());
            }
            last_key = Some(ik.as_bytes().to_vec());

            // Collect unique typed keys for bloom filter
            let typed = ik.typed_key_prefix().to_vec();
            if last_typed_key.as_ref() != Some(&typed) {
                bloom_keys.push(typed.clone());
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
            entry_count += 1;
            block_entry_count += 1;

            // Flush block when it reaches target size
            if block_buf.len() >= self.data_block_size {
                // Append restart trailer before compression
                append_restart_trailer(&mut block_buf, &restart_offsets);

                let bfk = block_first_key.take().unwrap();
                let framed_size =
                    write_framed_block_compressed(&mut w, BLOCK_TYPE_DATA, &block_buf, true)?;
                let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
                index_entries.push((bfk, file_offset, on_disk_data_len));
                file_offset += framed_size as u64;
                block_buf.clear();
                restart_offsets.clear();
                restart_offsets.push(0);
                block_entry_count = 0;
                prev_key.clear();
            }
        }

        // Flush final partial block
        if !block_buf.is_empty() {
            // Append restart trailer before compression
            append_restart_trailer(&mut block_buf, &restart_offsets);

            let bfk = block_first_key.take().unwrap_or_default();
            let framed_size =
                write_framed_block_compressed(&mut w, BLOCK_TYPE_DATA, &block_buf, true)?;
            let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
            index_entries.push((bfk, file_offset, on_disk_data_len));
            file_offset += framed_size as u64;
            block_buf.clear();
        }

        // Handle empty segment
        if entry_count == 0 {
            commit_min = 0;
            commit_max = 0;
        }

        // 2. Write IndexBlock
        let index_block_offset = file_offset;
        let index_data = encode_index_block(&index_entries);
        write_framed_block(&mut w, BLOCK_TYPE_INDEX, &index_data)?;
        let index_block_len = (BLOCK_FRAME_OVERHEAD + index_data.len()) as u32;
        file_offset += index_block_len as u64;

        // 3. Write BloomFilterBlock
        let filter_block_offset = file_offset;
        let bloom_key_refs: Vec<&[u8]> = bloom_keys.iter().map(|k| k.as_slice()).collect();
        let bloom = BloomFilter::build(&bloom_key_refs, self.bloom_bits_per_key);
        let bloom_data = bloom.to_bytes();
        write_framed_block(&mut w, BLOCK_TYPE_FILTER, &bloom_data)?;
        let filter_block_len = (BLOCK_FRAME_OVERHEAD + bloom_data.len()) as u32;
        file_offset += filter_block_len as u64;

        // 4. Write PropertiesBlock
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

        // 5. Write Footer
        let footer = encode_footer(
            index_block_offset,
            index_block_len,
            filter_block_offset,
            filter_block_len,
            props_block_offset,
            props_block_len,
        );
        w.write_all(&footer)?;
        file_offset += FOOTER_SIZE as u64;

        // 6. Backfill header
        w.seek(SeekFrom::Start(0))?;
        let header = encode_header(entry_count, commit_min, commit_max, self.data_block_size);
        w.write_all(&header)?;

        // 7. Flush and sync
        w.flush()?;
        w.get_ref().sync_all()?;
        drop(w);

        // 8. Atomic rename
        std::fs::rename(&tmp_path, path)?;

        Ok(SegmentMeta {
            entry_count,
            commit_min,
            commit_max,
            file_size: file_offset,
        })
    }
}

/// Test-only: build a v3 segment (no prefix compression) for backward-compat testing.
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
        let mut bloom_keys: Vec<Vec<u8>> = Vec::new();
        let mut last_typed_key: Option<Vec<u8>> = None;
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
                bloom_keys.push(typed.clone());
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
                let framed_size =
                    write_framed_block_compressed(&mut w, BLOCK_TYPE_DATA, &block_buf, true)?;
                let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
                index_entries.push((bfk, file_offset, on_disk_data_len));
                file_offset += framed_size as u64;
                block_buf.clear();
                restart_offsets.clear();
                restart_offsets.push(0);
                block_entry_count = 0;
            }
        }
        if !block_buf.is_empty() {
            append_restart_trailer(&mut block_buf, &restart_offsets);
            let bfk = block_first_key.take().unwrap_or_default();
            let framed_size =
                write_framed_block_compressed(&mut w, BLOCK_TYPE_DATA, &block_buf, true)?;
            let on_disk_data_len = (framed_size - BLOCK_FRAME_OVERHEAD) as u32;
            index_entries.push((bfk, file_offset, on_disk_data_len));
            file_offset += framed_size as u64;
            block_buf.clear();
        }
        if entry_count == 0 {
            commit_min = 0;
            commit_max = 0;
        }

        let index_block_offset = file_offset;
        let index_data = encode_index_block(&index_entries);
        write_framed_block(&mut w, BLOCK_TYPE_INDEX, &index_data)?;
        let index_block_len = (BLOCK_FRAME_OVERHEAD + index_data.len()) as u32;
        file_offset += index_block_len as u64;

        let filter_block_offset = file_offset;
        let bloom_key_refs: Vec<&[u8]> = bloom_keys.iter().map(|k| k.as_slice()).collect();
        let bloom = BloomFilter::build(&bloom_key_refs, self.bloom_bits_per_key);
        let bloom_data = bloom.to_bytes();
        write_framed_block(&mut w, BLOCK_TYPE_FILTER, &bloom_data)?;
        let filter_block_len = (BLOCK_FRAME_OVERHEAD + bloom_data.len()) as u32;
        file_offset += filter_block_len as u64;

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
        );
        w.write_all(&footer)?;
        file_offset += FOOTER_SIZE as u64;

        // Write header with format_version = 3
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

/// Write a framed block with optional zstd compression.
///
/// When `compress` is true, compresses the data with zstd level 3.
/// If compression doesn't reduce size, falls back to uncompressed.
/// The codec byte in the frame header indicates the compression used:
/// - 0 = uncompressed
/// - 1 = zstd
///
/// Returns the total framed size written (overhead + data).
fn write_framed_block_compressed<W: Write>(
    w: &mut W,
    block_type: u8,
    data: &[u8],
    compress: bool,
) -> io::Result<usize> {
    let (write_data, codec_byte) = if compress && !data.is_empty() {
        match zstd::encode_all(std::io::Cursor::new(data), 3) {
            Ok(compressed) if compressed.len() < data.len() => {
                (compressed, 1u8) // zstd compressed
            }
            _ => (data.to_vec(), 0u8), // fallback to uncompressed
        }
    } else {
        (data.to_vec(), 0u8)
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
/// reserved: [u8; 12]
/// magic: [u8; 8] — "STRAKEND"
/// ```
fn encode_footer(
    index_offset: u64,
    index_len: u32,
    filter_offset: u64,
    filter_len: u32,
    props_offset: u64,
    props_len: u32,
) -> [u8; FOOTER_SIZE] {
    let mut f = [0u8; FOOTER_SIZE];
    f[0..8].copy_from_slice(&index_offset.to_le_bytes());
    f[8..12].copy_from_slice(&index_len.to_le_bytes());
    f[12..20].copy_from_slice(&filter_offset.to_le_bytes());
    f[20..24].copy_from_slice(&filter_len.to_le_bytes());
    f[24..32].copy_from_slice(&props_offset.to_le_bytes());
    f[32..36].copy_from_slice(&props_len.to_le_bytes());
    // 36..48: reserved
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
}

// Re-export constants for the segment reader.
pub(crate) const HEADER_SIZE: usize = KV_HEADER_SIZE;
pub(crate) const FOOTER_SZ: usize = FOOTER_SIZE;
pub(crate) const FRAME_OVERHEAD: usize = BLOCK_FRAME_OVERHEAD;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
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
        let footer = encode_footer(1000, 200, 1200, 50, 1250, 80);
        let parsed = parse_footer(&footer).unwrap();
        assert_eq!(parsed.index_block_offset, 1000);
        assert_eq!(parsed.index_block_len, 200);
        assert_eq!(parsed.filter_block_offset, 1200);
        assert_eq!(parsed.filter_block_len, 50);
        assert_eq!(parsed.props_block_offset, 1250);
        assert_eq!(parsed.props_block_len, 80);
    }

    #[test]
    fn footer_rejects_bad_magic() {
        let mut footer = encode_footer(0, 0, 0, 0, 0, 0);
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
        assert_eq!(seg.format_version(), 4);

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

        // v4 should be smaller than v3
        assert!(
            v4_meta.file_size < v3_meta.file_size,
            "v4 ({}) should be smaller than v3 ({})",
            v4_meta.file_size,
            v3_meta.file_size,
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
