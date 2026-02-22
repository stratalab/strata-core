//! Sealed segment file format (.sidx) for the inverted index
//!
//! Provides immutable, mmap-able segments that store term dictionaries
//! and delta-encoded posting lists in a compact binary format.
//!
//! ## File Format
//!
//! ```text
//! HEADER (48 bytes):
//!   magic "SIDX"           4B
//!   version                 u32 LE
//!   segment_id              u64 LE
//!   doc_count               u32 LE
//!   term_count              u32 LE
//!   total_doc_len           u64 LE
//!   term_offsets_offset     u64 LE    → byte offset to term offset table
//!   postings_offset         u64 LE    → byte offset to postings section
//!
//! TERM DICTIONARY (variable length, sorted by term):
//!   per term:
//!     term_len              u16 LE
//!     term_bytes            [u8; term_len]
//!     df                    u32 LE
//!     posting_offset        u32 LE    → relative to postings section start
//!     posting_byte_len      u32 LE
//!
//! TERM OFFSET TABLE (term_count × 4 bytes):
//!   per term: offset        u32 LE    → byte offset of term entry in dict
//!   (enables binary search: jump to offset[mid], read term, compare)
//!
//! POSTINGS SECTION:
//!   per term's posting list:
//!     num_entries           u32 LE
//!     delta-encoded triples: (delta_doc_id: varint, tf: varint, doc_len: varint)
//! ```

use std::collections::{BTreeMap, HashSet};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{RwLock, RwLockReadGuard};

use super::index::PostingEntry;

/// Magic bytes for .sidx files
const SIDX_MAGIC: &[u8; 4] = b"SIDX";
/// Current format version
const SIDX_VERSION: u32 = 1;
/// Header size in bytes
const HEADER_SIZE: usize = 48;

// ============================================================================
// Varint (LEB128) Codec
// ============================================================================

/// Encode a u32 as a variable-length integer (LEB128).
pub(crate) fn encode_varint(mut value: u32, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint from a byte slice, returning (value, bytes_consumed).
pub(crate) fn decode_varint(data: &[u8]) -> Option<(u32, usize)> {
    let mut value: u32 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
        if shift >= 35 {
            return None; // overflow
        }
    }
    None // truncated
}

// ============================================================================
// SegmentData
// ============================================================================

/// Underlying storage for a sealed segment.
enum SegmentData {
    /// In-memory owned data (before mmap flush)
    Owned(Vec<u8>),
    /// Memory-mapped file data
    Mmap(memmap2::Mmap),
}

impl SegmentData {
    fn as_bytes(&self) -> &[u8] {
        match self {
            SegmentData::Owned(v) => v,
            SegmentData::Mmap(m) => m,
        }
    }
}

// ============================================================================
// SealedSegment
// ============================================================================

/// An immutable, searchable segment of the inverted index.
///
/// Contains a sorted term dictionary and delta-encoded posting lists.
/// Can be backed by either owned memory or an mmap'd file.
pub struct SealedSegment {
    data: SegmentData,
    segment_id: u64,
    doc_count: u32,
    term_count: u32,
    total_doc_len: u64,
    term_offsets_offset: u64,
    postings_offset: u64,
    /// Deleted doc_ids (not physically removed until compaction)
    tombstones: RwLock<HashSet<u32>>,
    /// Fast check: true if any tombstones exist (avoids RwLock read)
    has_tombstones: AtomicBool,
}

#[allow(dead_code)]
impl SealedSegment {
    /// Create a sealed segment from raw bytes.
    pub fn from_bytes(data: Vec<u8>) -> io::Result<Self> {
        Self::validate_and_create(SegmentData::Owned(data))
    }

    /// Load a sealed segment from an mmap'd file.
    pub fn from_mmap(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Self::validate_and_create(SegmentData::Mmap(mmap))
    }

    fn validate_and_create(data: SegmentData) -> io::Result<Self> {
        let bytes = data.as_bytes();
        if bytes.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment too small",
            ));
        }
        if &bytes[0..4] != SIDX_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad SIDX magic"));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if version != SIDX_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported SIDX version {}", version),
            ));
        }
        let segment_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let doc_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let term_count = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let total_doc_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let term_offsets_offset = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let postings_offset = u64::from_le_bytes(bytes[40..48].try_into().unwrap());

        Ok(SealedSegment {
            data,
            segment_id,
            doc_count,
            term_count,
            total_doc_len,
            term_offsets_offset,
            postings_offset,
            tombstones: RwLock::new(HashSet::new()),
            has_tombstones: AtomicBool::new(false),
        })
    }

    /// Segment ID
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Number of documents in this segment (including tombstoned)
    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }

    /// Total document length across all docs in this segment
    pub fn total_doc_len(&self) -> u64 {
        self.total_doc_len
    }

    /// Number of tombstoned (deleted) docs
    pub fn tombstone_count(&self) -> usize {
        self.tombstones.read().unwrap().len()
    }

    /// Live document count (doc_count - tombstones)
    pub fn live_doc_count(&self) -> u32 {
        self.doc_count.saturating_sub(self.tombstone_count() as u32)
    }

    /// Mark a doc_id as deleted in this segment
    pub fn add_tombstone(&self, doc_id: u32) {
        self.tombstones.write().unwrap().insert(doc_id);
        self.has_tombstones.store(true, Ordering::Release);
    }

    /// Check if a doc_id is tombstoned
    pub fn is_tombstoned(&self, doc_id: u32) -> bool {
        self.tombstones.read().unwrap().contains(&doc_id)
    }

    /// Acquire the tombstone set for batch lookups (avoids per-entry lock).
    pub fn tombstone_guard(&self) -> RwLockReadGuard<'_, HashSet<u32>> {
        self.tombstones.read().unwrap()
    }

    /// Get tombstone set (for manifest serialization)
    pub fn tombstones(&self) -> HashSet<u32> {
        self.tombstones.read().unwrap().clone()
    }

    /// Set tombstones (for manifest deserialization)
    pub fn set_tombstones(&self, tombstones: HashSet<u32>) {
        self.has_tombstones
            .store(!tombstones.is_empty(), Ordering::Release);
        *self.tombstones.write().unwrap() = tombstones;
    }

    /// Check if any tombstones exist (lock-free fast path).
    pub fn has_tombstones(&self) -> bool {
        self.has_tombstones.load(Ordering::Acquire)
    }

    // ========================================================================
    // Term Dictionary Access
    // ========================================================================

    /// Get document frequency for a term in this segment.
    pub fn doc_freq(&self, term: &str) -> u32 {
        match self.find_term(term) {
            Some((_, df, _, _)) => df,
            None => 0,
        }
    }

    /// Decode posting entries for a term.
    ///
    /// Returns entries with absolute doc_ids (delta-decoded).
    /// Does NOT filter tombstones — caller must check.
    pub fn posting_entries(&self, term: &str) -> Option<Vec<PostingEntry>> {
        let (_, _, posting_offset, posting_len) = self.find_term(term)?;
        let bytes = self.data.as_bytes();
        let abs_offset = self.postings_offset as usize + posting_offset as usize;
        let end = abs_offset + posting_len as usize;
        if end > bytes.len() {
            return None;
        }
        let posting_bytes = &bytes[abs_offset..end];
        decode_posting_list(posting_bytes)
    }

    /// Zero-allocation iterator over posting entries for a term.
    ///
    /// Lazily decodes delta-encoded varints directly from the segment data.
    /// Does NOT filter tombstones — caller must check.
    pub fn posting_iter(&self, term: &str) -> Option<PostingIter<'_>> {
        let (_, _, posting_offset, posting_len) = self.find_term(term)?;
        let bytes = self.data.as_bytes();
        let abs_offset = self.postings_offset as usize + posting_offset as usize;
        let end = abs_offset + posting_len as usize;
        if end > bytes.len() {
            return None;
        }
        let posting_bytes = &bytes[abs_offset..end];
        if posting_bytes.len() < 4 {
            return None;
        }
        let num_entries = u32::from_le_bytes(posting_bytes[0..4].try_into().unwrap());
        Some(PostingIter {
            data: posting_bytes,
            pos: 4,
            remaining: num_entries,
            prev_doc_id: 0,
        })
    }

    /// Single binary search returning (df, posting_offset, posting_byte_len).
    ///
    /// Callers can cache the result and later use `posting_iter_from_offset()`
    /// to avoid a redundant binary search.
    pub fn find_term_info(&self, term: &str) -> Option<(u32, u32, u32)> {
        let (_, df, offset, len) = self.find_term(term)?;
        Some((df, offset, len))
    }

    /// Create a PostingIter from pre-cached offset and length (skips binary search).
    pub fn posting_iter_from_offset(
        &self,
        posting_offset: u32,
        posting_byte_len: u32,
    ) -> Option<PostingIter<'_>> {
        let bytes = self.data.as_bytes();
        let abs_offset = self.postings_offset as usize + posting_offset as usize;
        let end = abs_offset + posting_byte_len as usize;
        if end > bytes.len() {
            return None;
        }
        let posting_bytes = &bytes[abs_offset..end];
        if posting_bytes.len() < 4 {
            return None;
        }
        let num_entries = u32::from_le_bytes(posting_bytes[0..4].try_into().unwrap());
        Some(PostingIter {
            data: posting_bytes,
            pos: 4,
            remaining: num_entries,
            prev_doc_id: 0,
        })
    }

    /// Binary search the term dictionary for a term.
    ///
    /// Returns (term_dict_offset, df, posting_offset, posting_byte_len).
    fn find_term(&self, term: &str) -> Option<(u32, u32, u32, u32)> {
        let bytes = self.data.as_bytes();
        let tc = self.term_count as usize;
        if tc == 0 {
            return None;
        }
        let offsets_start = self.term_offsets_offset as usize;

        let mut lo = 0usize;
        let mut hi = tc;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset_pos = offsets_start + mid * 4;
            if offset_pos + 4 > bytes.len() {
                return None;
            }
            let dict_offset =
                u32::from_le_bytes(bytes[offset_pos..offset_pos + 4].try_into().unwrap()) as usize;
            let abs_pos = HEADER_SIZE + dict_offset;

            if abs_pos + 2 > bytes.len() {
                return None;
            }
            let term_len =
                u16::from_le_bytes(bytes[abs_pos..abs_pos + 2].try_into().unwrap()) as usize;
            let term_start = abs_pos + 2;
            let term_end = term_start + term_len;
            if term_end + 12 > bytes.len() {
                return None;
            }
            let entry_term = std::str::from_utf8(&bytes[term_start..term_end]).ok()?;

            match entry_term.cmp(term) {
                std::cmp::Ordering::Equal => {
                    let df = u32::from_le_bytes(bytes[term_end..term_end + 4].try_into().unwrap());
                    let p_offset =
                        u32::from_le_bytes(bytes[term_end + 4..term_end + 8].try_into().unwrap());
                    let p_len =
                        u32::from_le_bytes(bytes[term_end + 8..term_end + 12].try_into().unwrap());
                    return Some((dict_offset as u32, df, p_offset, p_len));
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// Write this segment's data to a file (atomic temp+rename).
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let dir = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir)?;
        let tmp_path = path.with_extension("sidx.tmp");
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(self.data.as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

// ============================================================================
// PostingIter — zero-allocation posting list traversal
// ============================================================================

/// Iterator that lazily decodes delta-encoded posting entries from segment data.
///
/// Avoids the `Vec<PostingEntry>` allocation of `posting_entries()`.
pub struct PostingIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: u32,
    prev_doc_id: u32,
}

impl<'a> Iterator for PostingIter<'a> {
    type Item = PostingEntry;

    #[inline]
    fn next(&mut self) -> Option<PostingEntry> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let (delta, n1) = decode_varint(&self.data[self.pos..])?;
        self.pos += n1;
        let (tf, n2) = decode_varint(&self.data[self.pos..])?;
        self.pos += n2;
        let (doc_len, n3) = decode_varint(&self.data[self.pos..])?;
        self.pos += n3;
        self.prev_doc_id += delta;
        Some(PostingEntry::new(self.prev_doc_id, tf, doc_len))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

// ============================================================================
// Segment Builder
// ============================================================================

/// Build a sealed segment from active segment data.
///
/// Takes sorted term→postings data and produces a `SealedSegment`
/// in the .sidx format. The `BTreeMap` provides alphabetical ordering.
pub fn build_sealed_segment(
    segment_id: u64,
    term_postings: BTreeMap<String, Vec<PostingEntry>>,
    doc_count: u32,
    total_doc_len: u64,
) -> SealedSegment {
    let term_count = term_postings.len() as u32;

    let mut dict_buf: Vec<u8> = Vec::new();
    let mut postings_buf: Vec<u8> = Vec::new();
    let mut term_offsets: Vec<u32> = Vec::with_capacity(term_postings.len());

    for (term, mut entries) in term_postings {
        term_offsets.push(dict_buf.len() as u32);

        // Sort entries by doc_id for delta encoding
        entries.sort_by_key(|e| e.doc_id);

        let df = entries.len() as u32;
        let posting_offset = postings_buf.len() as u32;

        // Encode posting list
        encode_posting_list(&entries, &mut postings_buf);

        let posting_byte_len = postings_buf.len() as u32 - posting_offset;

        // Write term dictionary entry
        let term_bytes = term.as_bytes();
        dict_buf.extend_from_slice(&(term_bytes.len() as u16).to_le_bytes());
        dict_buf.extend_from_slice(term_bytes);
        dict_buf.extend_from_slice(&df.to_le_bytes());
        dict_buf.extend_from_slice(&posting_offset.to_le_bytes());
        dict_buf.extend_from_slice(&posting_byte_len.to_le_bytes());
    }

    let dict_size = dict_buf.len();
    let offsets_size = term_offsets.len() * 4;
    let term_offsets_offset = (HEADER_SIZE + dict_size) as u64;
    let postings_offset = term_offsets_offset + offsets_size as u64;

    let total_size = HEADER_SIZE + dict_size + offsets_size + postings_buf.len();
    let mut buf = Vec::with_capacity(total_size);

    // Header (48 bytes)
    buf.extend_from_slice(SIDX_MAGIC);
    buf.extend_from_slice(&SIDX_VERSION.to_le_bytes());
    buf.extend_from_slice(&segment_id.to_le_bytes());
    buf.extend_from_slice(&doc_count.to_le_bytes());
    buf.extend_from_slice(&term_count.to_le_bytes());
    buf.extend_from_slice(&total_doc_len.to_le_bytes());
    buf.extend_from_slice(&term_offsets_offset.to_le_bytes());
    buf.extend_from_slice(&postings_offset.to_le_bytes());
    debug_assert_eq!(buf.len(), HEADER_SIZE);

    // Term dictionary
    buf.extend_from_slice(&dict_buf);

    // Term offset table
    for offset in &term_offsets {
        buf.extend_from_slice(&offset.to_le_bytes());
    }

    // Postings section
    buf.extend_from_slice(&postings_buf);
    debug_assert_eq!(buf.len(), total_size);

    SealedSegment {
        data: SegmentData::Owned(buf),
        segment_id,
        doc_count,
        term_count,
        total_doc_len,
        term_offsets_offset,
        postings_offset,
        tombstones: RwLock::new(HashSet::new()),
        has_tombstones: AtomicBool::new(false),
    }
}

/// Encode a posting list as delta-encoded varint triples.
fn encode_posting_list(entries: &[PostingEntry], buf: &mut Vec<u8>) {
    // Write num_entries as u32 LE
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    let mut prev_doc_id: u32 = 0;
    for entry in entries {
        let delta = entry.doc_id - prev_doc_id;
        encode_varint(delta, buf);
        encode_varint(entry.tf, buf);
        encode_varint(entry.doc_len, buf);
        prev_doc_id = entry.doc_id;
    }
}

/// Decode a posting list from delta-encoded varint triples.
fn decode_posting_list(data: &[u8]) -> Option<Vec<PostingEntry>> {
    if data.len() < 4 {
        return None;
    }
    let num_entries = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(num_entries);
    let mut pos = 4;
    let mut prev_doc_id: u32 = 0;
    for _ in 0..num_entries {
        if pos >= data.len() {
            return None;
        }
        let (delta, n1) = decode_varint(&data[pos..])?;
        pos += n1;
        let (tf, n2) = decode_varint(&data[pos..])?;
        pos += n2;
        let (doc_len, n3) = decode_varint(&data[pos..])?;
        pos += n3;

        prev_doc_id += delta;
        entries.push(PostingEntry::new(prev_doc_id, tf, doc_len));
    }
    Some(entries)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        for &val in &[0u32, 1, 127, 128, 16383, 16384, u32::MAX] {
            let mut buf = Vec::new();
            encode_varint(val, &mut buf);
            let (decoded, len) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(len, buf.len());
        }
    }

    #[test]
    fn test_varint_encoding_size() {
        // 0-127: 1 byte
        let mut buf = Vec::new();
        encode_varint(127, &mut buf);
        assert_eq!(buf.len(), 1);

        // 128-16383: 2 bytes
        buf.clear();
        encode_varint(128, &mut buf);
        assert_eq!(buf.len(), 2);

        // u32::MAX: 5 bytes
        buf.clear();
        encode_varint(u32::MAX, &mut buf);
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn test_empty_segment() {
        let seg = build_sealed_segment(0, BTreeMap::new(), 0, 0);
        assert_eq!(seg.doc_count(), 0);
        assert_eq!(seg.term_count, 0);
        assert_eq!(seg.doc_freq("anything"), 0);
        assert!(seg.posting_entries("anything").is_none());
    }

    #[test]
    fn test_single_term_segment() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "hello".to_string(),
            vec![PostingEntry::new(0, 2, 5), PostingEntry::new(3, 1, 3)],
        );

        let seg = build_sealed_segment(1, terms, 2, 8);
        assert_eq!(seg.segment_id(), 1);
        assert_eq!(seg.doc_count(), 2);
        assert_eq!(seg.total_doc_len(), 8);
        assert_eq!(seg.doc_freq("hello"), 2);
        assert_eq!(seg.doc_freq("missing"), 0);

        let entries = seg.posting_entries("hello").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].doc_id, 0);
        assert_eq!(entries[0].tf, 2);
        assert_eq!(entries[0].doc_len, 5);
        assert_eq!(entries[1].doc_id, 3);
        assert_eq!(entries[1].tf, 1);
        assert_eq!(entries[1].doc_len, 3);
    }

    #[test]
    fn test_multi_term_binary_search() {
        let mut terms = BTreeMap::new();
        terms.insert("alpha".to_string(), vec![PostingEntry::new(0, 1, 10)]);
        terms.insert("beta".to_string(), vec![PostingEntry::new(1, 2, 10)]);
        terms.insert("gamma".to_string(), vec![PostingEntry::new(2, 3, 10)]);
        terms.insert("zeta".to_string(), vec![PostingEntry::new(3, 4, 10)]);

        let seg = build_sealed_segment(2, terms, 4, 40);

        assert_eq!(seg.doc_freq("alpha"), 1);
        assert_eq!(seg.doc_freq("beta"), 1);
        assert_eq!(seg.doc_freq("gamma"), 1);
        assert_eq!(seg.doc_freq("zeta"), 1);
        assert_eq!(seg.doc_freq("missing"), 0);

        let entries = seg.posting_entries("gamma").unwrap();
        assert_eq!(entries[0].doc_id, 2);
        assert_eq!(entries[0].tf, 3);
    }

    #[test]
    fn test_tombstones() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "hello".to_string(),
            vec![PostingEntry::new(0, 1, 5), PostingEntry::new(1, 1, 3)],
        );
        let seg = build_sealed_segment(1, terms, 2, 8);

        assert_eq!(seg.live_doc_count(), 2);
        assert!(!seg.is_tombstoned(0));

        seg.add_tombstone(0);
        assert!(seg.is_tombstoned(0));
        assert_eq!(seg.live_doc_count(), 1);
    }

    #[test]
    fn test_write_and_load_from_file() {
        let mut terms = BTreeMap::new();
        terms.insert("hello".to_string(), vec![PostingEntry::new(0, 2, 5)]);
        terms.insert("world".to_string(), vec![PostingEntry::new(1, 1, 3)]);

        let seg = build_sealed_segment(42, terms, 2, 8);

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sidx");

        seg.write_to_file(&path).unwrap();

        let loaded = SealedSegment::from_mmap(&path).unwrap();
        assert_eq!(loaded.segment_id(), 42);
        assert_eq!(loaded.doc_count(), 2);
        assert_eq!(loaded.doc_freq("hello"), 1);
        assert_eq!(loaded.doc_freq("world"), 1);

        let entries = loaded.posting_entries("hello").unwrap();
        assert_eq!(entries[0].doc_id, 0);
        assert_eq!(entries[0].tf, 2);
    }

    #[test]
    fn test_delta_encoding_large_gaps() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "test".to_string(),
            vec![
                PostingEntry::new(100, 1, 10),
                PostingEntry::new(10000, 2, 20),
                PostingEntry::new(1000000, 3, 30),
            ],
        );
        let seg = build_sealed_segment(1, terms, 3, 60);

        let entries = seg.posting_entries("test").unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].doc_id, 100);
        assert_eq!(entries[1].doc_id, 10000);
        assert_eq!(entries[2].doc_id, 1000000);
    }

    #[test]
    fn test_many_terms_binary_search() {
        let mut terms = BTreeMap::new();
        for i in 0..100 {
            terms.insert(format!("term_{:04}", i), vec![PostingEntry::new(i, 1, 10)]);
        }
        let seg = build_sealed_segment(1, terms, 100, 1000);

        // Check first, middle, and last terms
        assert_eq!(seg.doc_freq("term_0000"), 1);
        assert_eq!(seg.doc_freq("term_0050"), 1);
        assert_eq!(seg.doc_freq("term_0099"), 1);
        assert_eq!(seg.doc_freq("term_9999"), 0);
    }

    #[test]
    fn test_invalid_magic() {
        let mut buf = vec![0u8; 48];
        buf[0..4].copy_from_slice(b"XXXX"); // wrong magic
        assert!(SealedSegment::from_bytes(buf).is_err());
    }

    #[test]
    fn test_too_small_buffer() {
        let buf = vec![0u8; 10]; // too small for header
        assert!(SealedSegment::from_bytes(buf).is_err());
    }

    // ------------------------------------------------------------------
    // PostingIter tests
    // ------------------------------------------------------------------

    #[test]
    fn test_posting_iter_matches_posting_entries() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "hello".to_string(),
            vec![PostingEntry::new(0, 2, 5), PostingEntry::new(3, 1, 3)],
        );
        terms.insert("world".to_string(), vec![PostingEntry::new(1, 4, 7)]);

        let seg = build_sealed_segment(1, terms, 3, 15);

        // Verify posting_iter produces identical results to posting_entries
        for term in &["hello", "world"] {
            let entries = seg.posting_entries(term).unwrap();
            let iter_entries: Vec<_> = seg.posting_iter(term).unwrap().collect();
            assert_eq!(entries.len(), iter_entries.len(), "term={}", term);
            for (a, b) in entries.iter().zip(iter_entries.iter()) {
                assert_eq!(a.doc_id, b.doc_id, "term={}", term);
                assert_eq!(a.tf, b.tf, "term={}", term);
                assert_eq!(a.doc_len, b.doc_len, "term={}", term);
            }
        }
    }

    #[test]
    fn test_posting_iter_missing_term() {
        let mut terms = BTreeMap::new();
        terms.insert("alpha".to_string(), vec![PostingEntry::new(0, 1, 5)]);
        let seg = build_sealed_segment(1, terms, 1, 5);

        assert!(seg.posting_iter("missing").is_none());
    }

    #[test]
    fn test_posting_iter_empty_segment() {
        let seg = build_sealed_segment(0, BTreeMap::new(), 0, 0);
        assert!(seg.posting_iter("anything").is_none());
    }

    #[test]
    fn test_posting_iter_large_delta_gaps() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "test".to_string(),
            vec![
                PostingEntry::new(100, 1, 10),
                PostingEntry::new(10000, 2, 20),
                PostingEntry::new(1000000, 3, 30),
            ],
        );
        let seg = build_sealed_segment(1, terms, 3, 60);

        let iter_entries: Vec<_> = seg.posting_iter("test").unwrap().collect();
        assert_eq!(iter_entries.len(), 3);
        assert_eq!(iter_entries[0].doc_id, 100);
        assert_eq!(iter_entries[1].doc_id, 10000);
        assert_eq!(iter_entries[2].doc_id, 1000000);
    }

    #[test]
    fn test_posting_iter_size_hint() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "word".to_string(),
            vec![
                PostingEntry::new(0, 1, 5),
                PostingEntry::new(1, 1, 5),
                PostingEntry::new(2, 1, 5),
            ],
        );
        let seg = build_sealed_segment(1, terms, 3, 15);

        let mut iter = seg.posting_iter("word").unwrap();
        assert_eq!(iter.size_hint(), (3, Some(3)));
        iter.next();
        assert_eq!(iter.size_hint(), (2, Some(2)));
        iter.next();
        assert_eq!(iter.size_hint(), (1, Some(1)));
        iter.next();
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_posting_iter_after_mmap_roundtrip() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "hello".to_string(),
            vec![PostingEntry::new(0, 2, 5), PostingEntry::new(3, 1, 3)],
        );
        let seg = build_sealed_segment(42, terms, 2, 8);

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sidx");
        seg.write_to_file(&path).unwrap();

        let loaded = SealedSegment::from_mmap(&path).unwrap();
        let iter_entries: Vec<_> = loaded.posting_iter("hello").unwrap().collect();
        assert_eq!(iter_entries.len(), 2);
        assert_eq!(iter_entries[0].doc_id, 0);
        assert_eq!(iter_entries[0].tf, 2);
        assert_eq!(iter_entries[1].doc_id, 3);
        assert_eq!(iter_entries[1].tf, 1);
    }

    // ------------------------------------------------------------------
    // tombstone_guard tests
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // has_tombstones / find_term_info / posting_iter_from_offset tests
    // ------------------------------------------------------------------

    #[test]
    fn test_has_tombstones_flag() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "hello".to_string(),
            vec![PostingEntry::new(0, 1, 5), PostingEntry::new(1, 1, 5)],
        );
        let seg = build_sealed_segment(1, terms, 2, 10);

        assert!(!seg.has_tombstones());
        seg.add_tombstone(0);
        assert!(seg.has_tombstones());
    }

    #[test]
    fn test_has_tombstones_via_set_tombstones() {
        let seg = build_sealed_segment(0, BTreeMap::new(), 0, 0);

        assert!(!seg.has_tombstones());
        let mut ts = HashSet::new();
        ts.insert(42);
        seg.set_tombstones(ts);
        assert!(seg.has_tombstones());

        seg.set_tombstones(HashSet::new());
        assert!(!seg.has_tombstones());
    }

    #[test]
    fn test_posting_iter_from_offset_matches_posting_iter() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "hello".to_string(),
            vec![PostingEntry::new(0, 2, 5), PostingEntry::new(3, 1, 3)],
        );
        terms.insert("world".to_string(), vec![PostingEntry::new(1, 4, 7)]);
        let seg = build_sealed_segment(1, terms, 3, 15);

        for term in &["hello", "world"] {
            let info = seg.find_term_info(term).unwrap();
            let (df, offset, len) = info;

            // df should match doc_freq
            assert_eq!(df, seg.doc_freq(term));

            // posting_iter_from_offset should produce identical results to posting_iter
            let direct: Vec<_> = seg.posting_iter(term).unwrap().collect();
            let cached: Vec<_> = seg.posting_iter_from_offset(offset, len).unwrap().collect();
            assert_eq!(direct.len(), cached.len(), "term={}", term);
            for (a, b) in direct.iter().zip(cached.iter()) {
                assert_eq!(a.doc_id, b.doc_id, "term={}", term);
                assert_eq!(a.tf, b.tf, "term={}", term);
                assert_eq!(a.doc_len, b.doc_len, "term={}", term);
            }
        }

        // Missing term
        assert!(seg.find_term_info("missing").is_none());
    }

    #[test]
    fn test_tombstone_guard_batch_lookup() {
        let mut terms = BTreeMap::new();
        terms.insert(
            "hello".to_string(),
            vec![
                PostingEntry::new(0, 1, 5),
                PostingEntry::new(1, 1, 5),
                PostingEntry::new(2, 1, 5),
            ],
        );
        let seg = build_sealed_segment(1, terms, 3, 15);

        seg.add_tombstone(0);
        seg.add_tombstone(2);

        // Guard should see both tombstones without re-acquiring the lock
        let guard = seg.tombstone_guard();
        assert!(guard.contains(&0));
        assert!(!guard.contains(&1));
        assert!(guard.contains(&2));
        assert!(!guard.contains(&99));
    }
}
