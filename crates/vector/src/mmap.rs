//! Memory-mapped vector embeddings
//!
//! Provides a file format and reader/writer for memory-mapped vector data.
//! Used as a disk-backed cache for the global VectorHeap — if the mmap file
//! is missing or corrupt, recovery falls back to KV-based rebuild.
//!
//! ## File Format (Version 1)
//!
//! ```text
//! [magic "SVEC" 4B]
//! [version u32 LE]
//! [dimension u32 LE]
//! [count u64 LE]     — number of active vectors
//! [next_id u64 LE]   — next VectorId to allocate
//! [id_to_offset entries: count * (VectorId u64 LE, offset u64 LE)]
//! [free_slots_count u32 LE]
//! [free_slots: N * u64 LE]
//! [embeddings: contiguous f32 LE data]
//! ```
//!
//! The embeddings section is a flat array of f32 values. The id_to_offset map
//! gives byte offsets into the embeddings section (measured in f32 elements,
//! not bytes, matching VectorHeap conventions).

#[cfg(not(target_endian = "little"))]
compile_error!("MmapVectorData requires little-endian architecture for zero-copy f32 access");

use memmap2::Mmap;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::error::VectorError;
use crate::quantize::QuantizationParams;
use crate::types::VectorId;

/// Magic bytes identifying a Strata vector mmap file
const MAGIC: &[u8; 4] = b"SVEC";
/// Current format version
const VERSION: u32 = 1;
/// Header size: magic(4) + version(4) + dimension(4) + count(8) + next_id(8)
const HEADER_SIZE: usize = 4 + 4 + 4 + 8 + 8;

/// Compact sorted index: (vector_id_u64, offset_u64) pairs.
///
/// Uses binary search for O(log n) lookups at 16 bytes/entry, compared to
/// BTreeMap's ~72 bytes/entry. Suitable for immutable/read-only data.
///
/// Offsets are in f32 elements. u64 supports up to 2^64 elements, which is
/// sufficient for 100M+ vectors at any dimension without silent truncation.
#[derive(Clone)]
pub(crate) struct CompactIndex {
    /// Sorted by vector_id ascending. Offset is in f32 elements.
    entries: Vec<(u64, u64)>,
}

impl CompactIndex {
    fn get(&self, id: VectorId) -> Option<usize> {
        let key = id.as_u64();
        self.entries
            .binary_search_by_key(&key, |&(vid, _)| vid)
            .ok()
            .map(|idx| self.entries[idx].1 as usize)
    }

    fn to_btree(&self) -> BTreeMap<VectorId, usize> {
        self.entries
            .iter()
            .map(|&(vid, off)| (VectorId::new(vid), off as usize))
            .collect()
    }
}

/// Memory-mapped vector data (read-only)
pub(crate) struct MmapVectorData {
    /// The memory-mapped file
    mmap: Mmap,
    /// Dimension of each vector
    dimension: usize,
    /// Number of active vectors
    count: usize,
    /// Next VectorId to allocate
    next_id: u64,
    /// Compact sorted index: VectorId → offset (12 bytes/entry vs ~72 for BTreeMap)
    index: CompactIndex,
    /// Free slots for reuse
    free_slots: Vec<usize>,
    /// Byte offset where embeddings data starts in the mmap
    embeddings_offset: usize,
    /// Held to keep advisory shared lock for the struct lifetime
    _lock_file: File,
}

impl MmapVectorData {
    /// Open an existing mmap file, validating the header.
    ///
    /// Returns `None` if the file doesn't exist or is invalid (caller falls back to KV).
    pub(crate) fn open(path: &Path, expected_dimension: usize) -> Result<Self, VectorError> {
        let file = File::open(path).map_err(|e| VectorError::Io(e.to_string()))?;
        fs2::FileExt::lock_shared(&file)
            .map_err(|e| VectorError::Io(format!("failed to lock {}: {e}", path.display())))?;
        // SAFETY: We treat the mmap as read-only and the file is opened read-only.
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| VectorError::Io(e.to_string()))?;

        if mmap.len() < HEADER_SIZE {
            return Err(VectorError::Serialization(
                "mmap file too small for header".into(),
            ));
        }

        let data = &mmap[..];

        // Validate magic
        if &data[0..4] != MAGIC {
            return Err(VectorError::Serialization("invalid mmap magic".into()));
        }

        // Version
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(VectorError::Serialization(format!(
                "unsupported mmap version: {}",
                version
            )));
        }

        // Dimension
        let dimension = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        if dimension != expected_dimension {
            return Err(VectorError::Serialization(format!(
                "mmap dimension {} != expected {}",
                dimension, expected_dimension
            )));
        }

        // Count
        let count_u64 = u64::from_le_bytes(data[12..20].try_into().unwrap());
        let count = usize::try_from(count_u64).map_err(|_| {
            VectorError::Serialization(format!(
                "mmap count {count_u64} exceeds platform pointer size"
            ))
        })?;

        // next_id
        let next_id = u64::from_le_bytes(data[20..28].try_into().unwrap());

        // Validate file is large enough before allocating (#1780).
        // Each id_to_offset entry is 16 bytes (VectorId u64 + offset u64).
        let entries_bytes = count
            .checked_mul(16)
            .ok_or_else(|| VectorError::Serialization("mmap count overflow".into()))?;
        if HEADER_SIZE + entries_bytes > mmap.len() {
            return Err(VectorError::Serialization(format!(
                "mmap claims {} entries but file is only {} bytes",
                count,
                mmap.len()
            )));
        }

        let mut pos = HEADER_SIZE;

        // id_to_offset entries → compact sorted index
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 16 > mmap.len() {
                return Err(VectorError::Serialization(
                    "mmap truncated in id_to_offset".into(),
                ));
            }
            let vid = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let offset = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
            entries.push((vid, offset));
            pos += 16;
        }
        // File writes entries in BTreeMap order (sorted by VectorId), so
        // the vec is already sorted. No need to re-sort.
        let index = CompactIndex { entries };

        // Free slots
        if pos + 4 > mmap.len() {
            return Err(VectorError::Serialization(
                "mmap truncated in free_slots_count".into(),
            ));
        }
        let free_slots_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Validate file can hold claimed free slots before allocating (#1780).
        let free_slots_bytes = free_slots_count
            .checked_mul(8)
            .ok_or_else(|| VectorError::Serialization("mmap free_slots_count overflow".into()))?;
        if pos + free_slots_bytes > mmap.len() {
            return Err(VectorError::Serialization(format!(
                "mmap claims {} free slots but only {} bytes remain",
                free_slots_count,
                mmap.len().saturating_sub(pos)
            )));
        }

        let mut free_slots = Vec::with_capacity(free_slots_count);
        for _ in 0..free_slots_count {
            if pos + 8 > mmap.len() {
                return Err(VectorError::Serialization(
                    "mmap truncated in free_slots".into(),
                ));
            }
            let slot_u64 = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let slot = usize::try_from(slot_u64).map_err(|_| {
                VectorError::Serialization(format!(
                    "mmap free slot offset {slot_u64} exceeds platform pointer size"
                ))
            })?;
            free_slots.push(slot);
            pos += 8;
        }

        let embeddings_offset = pos;

        Ok(MmapVectorData {
            mmap,
            dimension,
            count,
            next_id,
            index,
            free_slots,
            embeddings_offset,
            _lock_file: file,
        })
    }

    /// Get embedding by VectorId
    pub(crate) fn get(&self, id: VectorId) -> Option<&[f32]> {
        let offset = self.index.get(id)?;
        let byte_start = self.embeddings_offset + offset * 4;
        let byte_end = byte_start + self.dimension * 4;
        if byte_end > self.mmap.len() {
            return None;
        }
        let slice = &self.mmap[byte_start..byte_end];
        assert!(
            (slice.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()),
            "mmap slice pointer is not aligned to f32"
        );
        // SAFETY: f32 is 4 bytes, alignment is asserted above,
        // and we've verified the bounds.
        let floats =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const f32, self.dimension) };
        Some(floats)
    }

    /// Get embedding by raw f32 offset (for dense acceleration).
    pub(crate) fn get_by_offset(&self, offset: usize, dim: usize) -> Option<&[f32]> {
        let byte_start = self.embeddings_offset + offset * 4;
        let byte_end = byte_start + dim * 4;
        if byte_end > self.mmap.len() {
            return None;
        }
        let slice = &self.mmap[byte_start..byte_end];
        assert!(
            (slice.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()),
            "mmap slice pointer is not aligned to f32"
        );
        let floats = unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const f32, dim) };
        Some(floats)
    }

    /// Number of active vectors
    pub(crate) fn len(&self) -> usize {
        self.count
    }

    /// Get next_id value
    pub(crate) fn next_id(&self) -> u64 {
        self.next_id
    }

    /// Get free slots
    pub(crate) fn free_slots(&self) -> &[usize] {
        &self.free_slots
    }

    /// Get the id_to_offset map (materialized from compact index).
    ///
    /// Used by `VectorHeap::from_mmap()` to populate `id_to_offset` on load.
    pub(crate) fn id_to_offset(&self) -> BTreeMap<VectorId, usize> {
        self.index.to_btree()
    }

    /// Get quantization parameters (if stored in the mmap file).
    ///
    /// Currently always returns None — quantized mmap format will be
    /// implemented in Phase 6.
    pub(crate) fn quant_params(&self) -> Option<QuantizationParams> {
        None
    }
}

/// Write a vector heap to an mmap-compatible file.
///
/// This is called after recovery/rebuild to create a disk cache.
/// The file can be opened with `MmapVectorData::open()` on subsequent starts.
pub(crate) fn write_mmap_file(
    path: &Path,
    dimension: usize,
    next_id: u64,
    id_to_offset: &BTreeMap<VectorId, usize>,
    free_slots: &[usize],
    raw_data: &[f32],
) -> Result<(), VectorError> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| VectorError::Io(e.to_string()))?;
    }

    // Write to temp file then rename for atomicity
    let temp_path = path.with_extension("vec.tmp");
    let mut file = File::create(&temp_path).map_err(|e| VectorError::Io(e.to_string()))?;
    fs2::FileExt::lock_exclusive(&file)
        .map_err(|e| VectorError::Io(format!("failed to lock {}: {e}", temp_path.display())))?;

    // Header
    file.write_all(MAGIC)
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&VERSION.to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&(dimension as u32).to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&(id_to_offset.len() as u64).to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&next_id.to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;

    // id_to_offset entries (BTreeMap iterates in sorted order)
    for (&id, &offset) in id_to_offset {
        file.write_all(&id.as_u64().to_le_bytes())
            .map_err(|e| VectorError::Io(e.to_string()))?;
        file.write_all(&(offset as u64).to_le_bytes())
            .map_err(|e| VectorError::Io(e.to_string()))?;
    }

    // Free slots
    file.write_all(&(free_slots.len() as u32).to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    for &slot in free_slots {
        file.write_all(&(slot as u64).to_le_bytes())
            .map_err(|e| VectorError::Io(e.to_string()))?;
    }

    // Embeddings (raw f32 data as bytes)
    let bytes =
        unsafe { std::slice::from_raw_parts(raw_data.as_ptr() as *const u8, raw_data.len() * 4) };
    file.write_all(bytes)
        .map_err(|e| VectorError::Io(e.to_string()))?;

    file.sync_all()
        .map_err(|e| VectorError::Io(e.to_string()))?;
    drop(file);

    // Atomic rename
    fs::rename(&temp_path, path).map_err(|e| VectorError::Io(e.to_string()))?;

    // fsync parent directory so the rename is durable on Linux/ext4
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_mmap_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.vec");

        let dimension = 3;
        let mut id_to_offset = BTreeMap::new();
        id_to_offset.insert(VectorId::new(1), 0usize);
        id_to_offset.insert(VectorId::new(3), 3usize);

        let _raw_data: Vec<f32> = vec![
            1.0, 0.0, 0.0, // id=1 at offset 0
            0.0, 0.0, 0.0, // deleted slot (offset 3 would be id=2 but it's free)
            0.0, 1.0, 0.0, // id=3 at offset 3... wait, offset 3 maps to floats 3,4,5
        ];
        // Correction: offset is in f32 elements, so offset=3 means raw_data[3..6]
        let raw_data: Vec<f32> = vec![
            1.0, 0.0, 0.0, // offset 0: id=1
            0.0, 0.0, 0.0, // offset 3: free slot
            0.0, 1.0, 0.0, // offset 6: id=3
        ];
        let mut id_to_offset = BTreeMap::new();
        id_to_offset.insert(VectorId::new(1), 0usize);
        id_to_offset.insert(VectorId::new(3), 6usize);
        let free_slots = vec![3usize];

        write_mmap_file(&path, dimension, 4, &id_to_offset, &free_slots, &raw_data).unwrap();

        let mmap = MmapVectorData::open(&path, dimension).unwrap();
        assert_eq!(mmap.len(), 2);
        assert_eq!(mmap.next_id(), 4);

        let emb1 = mmap.get(VectorId::new(1)).unwrap();
        assert_eq!(emb1, &[1.0, 0.0, 0.0]);

        let emb3 = mmap.get(VectorId::new(3)).unwrap();
        assert_eq!(emb3, &[0.0, 1.0, 0.0]);

        assert!(mmap.get(VectorId::new(2)).is_none());

        assert_eq!(mmap.free_slots(), &[3]);
    }

    #[test]
    fn test_mmap_empty() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("empty.vec");

        write_mmap_file(&path, 3, 1, &BTreeMap::new(), &[], &[]).unwrap();

        let mmap = MmapVectorData::open(&path, 3).unwrap();
        assert_eq!(mmap.len(), 0);
        assert_eq!(mmap.next_id(), 1);
    }

    #[test]
    fn test_mmap_invalid_magic() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bad.vec");
        std::fs::write(&path, b"BAAD00000000000000000000000000").unwrap();

        let result = MmapVectorData::open(&path, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_mmap_dimension_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("dim.vec");

        write_mmap_file(&path, 3, 1, &BTreeMap::new(), &[], &[]).unwrap();

        let result = MmapVectorData::open(&path, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_mmap_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nonexistent.vec");

        let result = MmapVectorData::open(&path, 3);
        assert!(result.is_err());
    }

    #[test]
    #[ignore] // OOM: Vec::with_capacity(4B) on 64-bit — see #1780
    fn test_issue_1735_mmap_rejects_oversized_count() {
        // Craft a valid-looking mmap file with count > u32::MAX.
        // On 32-bit, this must error (not silently truncate).
        // On 64-bit, this errors because the file is too small for the count.
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("big_count.vec");

        let big_count: u64 = (u32::MAX as u64) + 1;
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC); // 4B
        data.extend_from_slice(&VERSION.to_le_bytes()); // 4B
        data.extend_from_slice(&3u32.to_le_bytes()); // dimension 4B
        data.extend_from_slice(&big_count.to_le_bytes()); // count 8B
        data.extend_from_slice(&1u64.to_le_bytes()); // next_id 8B
                                                     // No entries follow — file is truncated relative to count
        data.extend(vec![0u8; 256]);

        std::fs::write(&path, &data).unwrap();
        let result = MmapVectorData::open(&path, 3);
        assert!(result.is_err(), "should reject file with oversized count");
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn test_issue_1735_mmap_count_exceeds_32bit() {
        // On 32-bit specifically, verify the error mentions platform size.
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("big_count_32.vec");

        let big_count: u64 = (u32::MAX as u64) + 1;
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&VERSION.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(&big_count.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend(vec![0u8; 256]);

        std::fs::write(&path, &data).unwrap();
        let err = MmapVectorData::open(&path, 3).unwrap_err().to_string();
        assert!(
            err.contains("exceeds"),
            "expected platform size error, got: {}",
            err
        );
    }

    // ====================================================================
    // CompactIndex tests
    // ====================================================================

    #[test]
    fn test_compact_index_binary_search() {
        let idx = CompactIndex {
            entries: vec![(1, 0), (5, 10), (10, 20), (100, 30)],
        };

        // Exact matches
        assert_eq!(idx.get(VectorId::new(1)), Some(0));
        assert_eq!(idx.get(VectorId::new(5)), Some(10));
        assert_eq!(idx.get(VectorId::new(10)), Some(20));
        assert_eq!(idx.get(VectorId::new(100)), Some(30));

        // Non-existent (between, before, after)
        assert!(idx.get(VectorId::new(0)).is_none());
        assert!(idx.get(VectorId::new(3)).is_none());
        assert!(idx.get(VectorId::new(7)).is_none());
        assert!(idx.get(VectorId::new(50)).is_none());
        assert!(idx.get(VectorId::new(200)).is_none());
    }

    #[test]
    fn test_compact_index_to_btree_roundtrip() {
        let idx = CompactIndex {
            entries: vec![(1, 0), (5, 384), (10, 768)],
        };
        let restored = idx.to_btree();

        let mut expected = BTreeMap::new();
        expected.insert(VectorId::new(1), 0usize);
        expected.insert(VectorId::new(5), 384usize);
        expected.insert(VectorId::new(10), 768usize);
        assert_eq!(expected, restored);
    }

    #[test]
    fn test_compact_index_large_offsets_no_truncation() {
        // Verify u64 offsets work for values exceeding u32::MAX
        let large_offset: u64 = (u32::MAX as u64) + 1000;
        let idx = CompactIndex {
            entries: vec![(1, large_offset)],
        };
        assert_eq!(idx.get(VectorId::new(1)), Some(large_offset as usize));

        // Also verify round-trip through to_btree
        let restored = idx.to_btree();
        assert_eq!(
            restored.get(&VectorId::new(1)),
            Some(&(large_offset as usize))
        );
    }
}
