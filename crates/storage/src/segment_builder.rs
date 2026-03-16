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
const FORMAT_VERSION: u16 = 1;

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

            // Encode entry into block buffer
            encode_entry(&ik, &entry, &mut block_buf);
            entry_count += 1;

            // Flush block when it reaches target size
            if block_buf.len() >= self.data_block_size {
                let bfk = block_first_key.take().unwrap();
                let block_data_len = block_buf.len() as u32;
                write_framed_block(&mut w, BLOCK_TYPE_DATA, &block_buf)?;
                index_entries.push((bfk, file_offset, block_data_len));
                file_offset += (BLOCK_FRAME_OVERHEAD + block_buf.len()) as u64;
                block_buf.clear();
            }
        }

        // Flush final partial block
        if !block_buf.is_empty() {
            let bfk = block_first_key.take().unwrap_or_default();
            let block_data_len = block_buf.len() as u32;
            write_framed_block(&mut w, BLOCK_TYPE_DATA, &block_buf)?;
            index_entries.push((bfk, file_offset, block_data_len));
            file_offset += (BLOCK_FRAME_OVERHEAD + block_buf.len()) as u64;
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

// ---------------------------------------------------------------------------
// Entry encoding (within data blocks)
// ---------------------------------------------------------------------------

/// Encode a single entry into a data block buffer.
///
/// Format: `| ik_len: u32 | ik_bytes | value_kind: u8 | value_len: u32 | value_bytes |`
fn encode_entry(ik: &InternalKey, entry: &MemtableEntry, buf: &mut Vec<u8>) {
    let ik_bytes = ik.as_bytes();
    buf.extend_from_slice(&(ik_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(ik_bytes);

    if entry.is_tombstone {
        buf.push(VALUE_KIND_DEL);
        buf.extend_from_slice(&0u32.to_le_bytes());
    } else {
        buf.push(VALUE_KIND_PUT);
        let value_bytes =
            bincode::serialize(&entry.value).expect("Value serialization should not fail");
        buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&value_bytes);
    }
}

/// Decode a single entry from a data block at the given offset.
///
/// Returns `(internal_key, is_tombstone, value, bytes_consumed)`.
pub(crate) fn decode_entry(data: &[u8]) -> Option<(InternalKey, bool, Value, usize)> {
    if data.len() < 4 {
        return None;
    }
    let mut pos = 0;

    let ik_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if pos + ik_len > data.len() {
        return None;
    }
    let ik = InternalKey::from_bytes(data[pos..pos + ik_len].to_vec());
    pos += ik_len;

    if pos >= data.len() {
        return None;
    }
    let value_kind = data[pos];
    pos += 1;

    if pos + 4 > data.len() {
        return None;
    }
    let value_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    if value_kind == VALUE_KIND_DEL {
        return Some((ik, true, Value::Null, pos));
    }

    if value_kind != VALUE_KIND_PUT {
        return None; // unknown value_kind
    }

    if pos + value_len > data.len() {
        return None;
    }
    let value: Value = bincode::deserialize(&data[pos..pos + value_len]).ok()?;
    pos += value_len;

    Some((ik, false, value, pos))
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
    if format_version != FORMAT_VERSION {
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
    use strata_core::types::{BranchId, Key, Namespace, TypeTag};
    use std::sync::Arc;

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
        let meta = builder
            .build_from_iter(std::iter::empty(), &path)
            .unwrap();

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

        let (decoded_ik, is_tomb, decoded_val, consumed) = decode_entry(&buf).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(!is_tomb);
        assert_eq!(decoded_val, Value::String("world".into()));
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

        let (decoded_ik, is_tomb, _, consumed) = decode_entry(&buf).unwrap();
        assert_eq!(decoded_ik, ik);
        assert!(is_tomb);
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
}
