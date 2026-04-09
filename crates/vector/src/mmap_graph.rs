//! Memory-mapped HNSW graph storage
//!
//! Provides a file format and reader/writer for memory-mapped HNSW graph data.
//! Used as a disk-backed cache for sealed segment graphs — if the mmap file
//! is missing or corrupt, recovery rebuilds the graph from scratch.
//!
//! ## File Format (Version 1)
//!
//! ```text
//! HEADER (48 bytes):
//!   [magic "SHGR" 4B]
//!   [version u32 LE]
//!   [entry_point u64 LE]      — u64::MAX if None
//!   [max_level u32 LE]
//!   [node_count u32 LE]
//!   [neighbor_data_len u64 LE] — number of u64 neighbor entries
//!   [node_section_size u64 LE] — total bytes for all node entries
//!   [_reserved u64 LE]
//!
//! NODE ENTRIES (node_section_size bytes, sorted by VectorId):
//!   per node:
//!     [VectorId u64 LE]
//!     [created_at u64 LE]
//!     [deleted_at u64 LE]      — u64::MAX if None
//!     [num_layers u32 LE]
//!     [_pad u32 LE]
//!     per layer:
//!       [start u32 LE]
//!       [count u32 LE]
//!
//! PADDING (0-7 bytes to reach 8-byte alignment)
//!
//! NEIGHBOR DATA (neighbor_data_len * 8 bytes):
//!   [u64 LE values]
//! ```
//!
//! On little-endian platforms the neighbor data is directly reinterpretable
//! as `&[u64]` from the mmap, avoiding any copy.

#[cfg(not(target_endian = "little"))]
compile_error!("mmap graph requires little-endian architecture");

use memmap2::Mmap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::error::VectorError;
use crate::hnsw::{CompactHnswGraph, CompactHnswNode, HnswConfig, NeighborData};
use crate::types::VectorId;
use crate::VectorConfig;

const MAGIC: &[u8; 4] = b"SHGR";
const VERSION: u32 = 1;
/// Header: magic(4) + version(4) + entry_point(8) + max_level(4) + node_count(4)
///       + neighbor_data_len(8) + node_section_size(8) + reserved(8) = 48
const HEADER_SIZE: usize = 48;
const NONE_SENTINEL: u64 = u64::MAX;

/// Align `n` up to the next multiple of 8.
fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// Write a `CompactHnswGraph` to disk.
///
/// Creates an mmap-compatible file at `path`. The file can be opened with
/// [`open_graph_file`] on subsequent starts to avoid rebuilding the graph.
pub(crate) fn write_graph_file(path: &Path, graph: &CompactHnswGraph) -> Result<(), VectorError> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| VectorError::Io(e.to_string()))?;
    }

    // Compute node section size
    let mut node_section_size: usize = 0;
    for (_, node) in graph.iter_nodes() {
        // VectorId(8) + created_at(8) + deleted_at(8) + num_layers(4) + pad(4)
        // + num_layers * (start(4) + count(4))
        node_section_size += 32 + node.layer_ranges.len() * 8;
    }

    let neighbor_data = graph.neighbor_data.as_slice();
    let neighbor_data_len = neighbor_data.len();

    // Write to temp file then rename for atomicity
    let temp_path = path.with_extension("hgr.tmp");
    let mut file = File::create(&temp_path).map_err(|e| VectorError::Io(e.to_string()))?;
    fs2::FileExt::lock_exclusive(&file)
        .map_err(|e| VectorError::Io(format!("failed to lock {}: {e}", temp_path.display())))?;

    // Header (48 bytes)
    file.write_all(MAGIC)
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&VERSION.to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(
        &graph
            .entry_point
            .map_or(NONE_SENTINEL, |id| id.as_u64())
            .to_le_bytes(),
    )
    .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&(graph.max_level as u32).to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&(graph.len() as u32).to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&(neighbor_data_len as u64).to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&(node_section_size as u64).to_le_bytes())
        .map_err(|e| VectorError::Io(e.to_string()))?;
    file.write_all(&0u64.to_le_bytes()) // reserved
        .map_err(|e| VectorError::Io(e.to_string()))?;

    // Node entries (sorted by VectorId — iter_nodes guarantees this)
    for (id, node) in graph.iter_nodes() {
        file.write_all(&id.as_u64().to_le_bytes())
            .map_err(|e| VectorError::Io(e.to_string()))?;
        file.write_all(&node.created_at.to_le_bytes())
            .map_err(|e| VectorError::Io(e.to_string()))?;
        file.write_all(&node.deleted_at.unwrap_or(NONE_SENTINEL).to_le_bytes())
            .map_err(|e| VectorError::Io(e.to_string()))?;
        file.write_all(&(node.layer_ranges.len() as u32).to_le_bytes())
            .map_err(|e| VectorError::Io(e.to_string()))?;
        file.write_all(&0u32.to_le_bytes()) // padding
            .map_err(|e| VectorError::Io(e.to_string()))?;
        for &(start, count) in &node.layer_ranges {
            file.write_all(&start.to_le_bytes())
                .map_err(|e| VectorError::Io(e.to_string()))?;
            file.write_all(&(count as u32).to_le_bytes())
                .map_err(|e| VectorError::Io(e.to_string()))?;
        }
    }

    // Padding to 8-byte alignment
    let current_offset = HEADER_SIZE + node_section_size;
    let padded_offset = align8(current_offset);
    if padded_offset > current_offset {
        let padding = vec![0u8; padded_offset - current_offset];
        file.write_all(&padding)
            .map_err(|e| VectorError::Io(e.to_string()))?;
    }

    // Neighbor data (u64 LE values, directly reinterpretable on LE platforms)
    let bytes = unsafe {
        std::slice::from_raw_parts(neighbor_data.as_ptr() as *const u8, neighbor_data.len() * 8)
    };
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

/// Open a `CompactHnswGraph` from an mmap file.
///
/// Node metadata is loaded into memory (dense array — small).
/// Neighbor data remains mmap-backed and is only paged in on access.
///
/// The caller provides `hnsw_config` and `vector_config` since the file
/// format does not persist these (they are available from the collection
/// configuration already stored in KV).
pub(crate) fn open_graph_file(
    path: &Path,
    hnsw_config: HnswConfig,
    vector_config: VectorConfig,
) -> Result<CompactHnswGraph, VectorError> {
    let file = File::open(path).map_err(|e| VectorError::Io(e.to_string()))?;
    fs2::FileExt::lock_shared(&file)
        .map_err(|e| VectorError::Io(format!("failed to lock {}: {e}", path.display())))?;
    // SAFETY: File is opened read-only; mmap is treated as immutable.
    let mmap = unsafe { Mmap::map(&file) }.map_err(|e| VectorError::Io(e.to_string()))?;

    if mmap.len() < HEADER_SIZE {
        return Err(VectorError::Serialization(
            "graph mmap file too small for header".into(),
        ));
    }

    let data = &mmap[..];

    // Validate magic
    if &data[0..4] != MAGIC {
        return Err(VectorError::Serialization(
            "invalid graph mmap magic".into(),
        ));
    }

    // Version
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != VERSION {
        return Err(VectorError::Serialization(format!(
            "unsupported graph mmap version: {}",
            version
        )));
    }

    // entry_point
    let ep_raw = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let entry_point = if ep_raw == NONE_SENTINEL {
        None
    } else {
        Some(VectorId::new(ep_raw))
    };

    // max_level, node_count
    let max_level = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
    let node_count = u32::from_le_bytes(data[20..24].try_into().unwrap()) as usize;

    // neighbor_data_len, node_section_size
    let neighbor_data_len_u64 = u64::from_le_bytes(data[24..32].try_into().unwrap());
    let neighbor_data_len = usize::try_from(neighbor_data_len_u64).map_err(|_| {
        VectorError::Serialization(format!(
            "neighbor_data_len {neighbor_data_len_u64} exceeds platform pointer size"
        ))
    })?;
    let node_section_size_u64 = u64::from_le_bytes(data[32..40].try_into().unwrap());
    let node_section_size = usize::try_from(node_section_size_u64).map_err(|_| {
        VectorError::Serialization(format!(
            "node_section_size {node_section_size_u64} exceeds platform pointer size"
        ))
    })?;
    // reserved at 40..48 (ignored)

    // Validate file size (with overflow protection for untrusted file values)
    let neighbor_data_offset = align8(
        HEADER_SIZE
            .checked_add(node_section_size)
            .ok_or_else(|| VectorError::Serialization("node_section_size overflow".into()))?,
    );
    let neighbor_bytes = neighbor_data_len
        .checked_mul(8)
        .ok_or_else(|| VectorError::Serialization("neighbor_data_len overflow".into()))?;
    let expected_size = neighbor_data_offset
        .checked_add(neighbor_bytes)
        .ok_or_else(|| VectorError::Serialization("file size overflow".into()))?;
    if mmap.len() < expected_size {
        return Err(VectorError::Serialization(format!(
            "graph mmap file truncated: {} < {}",
            mmap.len(),
            expected_size
        )));
    }

    // Read node entries into a sorted Vec for dense array construction
    let mut entries: Vec<(VectorId, CompactHnswNode)> = Vec::with_capacity(node_count);
    let mut pos = HEADER_SIZE;
    for _ in 0..node_count {
        if pos + 32 > HEADER_SIZE + node_section_size {
            return Err(VectorError::Serialization(
                "graph mmap truncated in node entries".into(),
            ));
        }
        let vid = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        let created_at = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        let deleted_at_raw = u64::from_le_bytes(data[pos + 16..pos + 24].try_into().unwrap());
        let deleted_at = if deleted_at_raw == NONE_SENTINEL {
            None
        } else {
            Some(deleted_at_raw)
        };
        let num_layers = u32::from_le_bytes(data[pos + 24..pos + 28].try_into().unwrap()) as usize;
        // pos+28..pos+32 is padding
        pos += 32;

        if pos + num_layers * 8 > HEADER_SIZE + node_section_size {
            return Err(VectorError::Serialization(
                "graph mmap truncated in layer ranges".into(),
            ));
        }
        let mut layer_ranges = Vec::with_capacity(num_layers);
        for _ in 0..num_layers {
            let start = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            let count = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as u16;
            // Validate that (start + count) doesn't exceed neighbor_data_len
            let end = start as usize + count as usize;
            if end > neighbor_data_len {
                return Err(VectorError::Serialization(format!(
                    "layer_range out of bounds: start={} count={} but neighbor_data_len={}",
                    start, count, neighbor_data_len
                )));
            }
            layer_ranges.push((start, count));
            pos += 8;
        }

        entries.push((
            VectorId::new(vid),
            CompactHnswNode {
                layer_ranges,
                created_at,
                deleted_at,
            },
        ));
    }

    // Build dense array (entries are already sorted by VectorId from the file format)
    let (dense_nodes, id_offset, actual_node_count) = CompactHnswGraph::build_dense(entries);

    // Neighbor data stays mmap-backed
    let neighbor_data = NeighborData::Mmap {
        mmap,
        byte_offset: neighbor_data_offset,
        len: neighbor_data_len,
    };

    Ok(CompactHnswGraph {
        config: hnsw_config,
        vector_config,
        neighbor_data,
        dense_nodes,
        id_offset,
        node_count: actual_node_count,
        entry_point,
        max_level,
        _lock_file: Some(file),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DistanceMetric;
    use tempfile::TempDir;

    fn make_test_graph() -> CompactHnswGraph {
        use crate::hnsw::HnswGraph;

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut graph = HnswGraph::new(&config, HnswConfig::default());

        // Build a small heap for distance computation
        let mut heap = crate::heap::VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        heap.upsert(VectorId::new(4), &[0.7, 0.7, 0.0]).unwrap();

        graph.insert_into_graph(VectorId::new(1), &[1.0, 0.0, 0.0], 10, &heap);
        graph.insert_into_graph(VectorId::new(2), &[0.0, 1.0, 0.0], 20, &heap);
        graph.insert_into_graph(VectorId::new(3), &[0.0, 0.0, 1.0], 30, &heap);
        graph.insert_into_graph(VectorId::new(4), &[0.7, 0.7, 0.0], 40, &heap);

        CompactHnswGraph::from_graph(&graph)
    }

    #[test]
    fn test_graph_mmap_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.hgr");

        let original = make_test_graph();

        // Write to disk
        write_graph_file(&path, &original).unwrap();

        // Read back
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let loaded = open_graph_file(&path, HnswConfig::default(), config.clone()).unwrap();

        // Verify structure matches
        assert_eq!(original.len(), loaded.len());
        assert_eq!(original.entry_point, loaded.entry_point);
        assert_eq!(original.max_level, loaded.max_level);
        assert_eq!(original.neighbor_data.len(), loaded.neighbor_data.len());

        // Verify neighbor data content matches
        let orig_data = original.neighbor_data.as_slice();
        let loaded_data = loaded.neighbor_data.as_slice();
        assert_eq!(orig_data, loaded_data);

        // Verify node metadata matches
        for (id, orig_node) in original.iter_nodes() {
            let loaded_node = loaded.get_node(id).expect("node missing");
            assert_eq!(orig_node.created_at, loaded_node.created_at);
            assert_eq!(orig_node.deleted_at, loaded_node.deleted_at);
            assert_eq!(orig_node.layer_ranges, loaded_node.layer_ranges);
        }

        // Verify neighbor data is mmap-backed (not materialized into an owned Vec).
        // This guards against a regression where open_graph_file silently falls
        // back to owned loading, which would defeat the memory-mapping invariant.
        assert!(loaded.neighbor_data.is_mmap());
    }

    #[test]
    fn test_graph_mmap_search_correctness() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("search.hgr");

        let original = make_test_graph();

        // Build heap for search
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = crate::heap::VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        heap.upsert(VectorId::new(4), &[0.7, 0.7, 0.0]).unwrap();

        // Search with in-memory graph
        let results_orig = original.search_with_heap(&[1.0, 0.0, 0.0], 4, &heap);

        // Write and reload
        write_graph_file(&path, &original).unwrap();
        let loaded = open_graph_file(&path, HnswConfig::default(), config).unwrap();

        // Search with mmap-backed graph
        let results_mmap = loaded.search_with_heap(&[1.0, 0.0, 0.0], 4, &heap);

        // Results must be identical
        assert_eq!(results_orig.len(), results_mmap.len());
        for (a, b) in results_orig.iter().zip(results_mmap.iter()) {
            assert_eq!(a.0, b.0, "VectorIds differ");
            assert!(
                (a.1 - b.1).abs() < 1e-10,
                "Scores differ: {} vs {}",
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn test_graph_mmap_empty() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("empty.hgr");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let graph = CompactHnswGraph {
            config: HnswConfig::default(),
            vector_config: config.clone(),
            neighbor_data: NeighborData::Owned(Vec::new()),
            dense_nodes: Vec::new(),
            id_offset: 0,
            node_count: 0,
            entry_point: None,
            max_level: 0,
            _lock_file: None,
        };

        write_graph_file(&path, &graph).unwrap();
        let loaded = open_graph_file(&path, HnswConfig::default(), config).unwrap();

        assert_eq!(loaded.len(), 0);
        assert!(loaded.entry_point.is_none());
        assert_eq!(loaded.neighbor_data.len(), 0);
    }

    #[test]
    fn test_graph_mmap_delete_with_timestamp() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("delete.hgr");

        let mut original = make_test_graph();
        // Soft-delete a node before writing
        original.delete_with_timestamp(VectorId::new(2), 50);

        write_graph_file(&path, &original).unwrap();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut loaded = open_graph_file(&path, HnswConfig::default(), config.clone()).unwrap();

        // Deleted node should be persisted
        assert!(!loaded.contains(VectorId::new(2)));
        assert!(loaded.contains(VectorId::new(1)));

        // Runtime delete on mmap-backed graph should also work
        let was_alive = loaded.delete_with_timestamp(VectorId::new(3), 60);
        assert!(was_alive);
        assert!(!loaded.contains(VectorId::new(3)));

        // Search with heap should exclude deleted nodes
        let mut heap = crate::heap::VectorHeap::new(config);
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        heap.upsert(VectorId::new(4), &[0.7, 0.7, 0.0]).unwrap();

        let results = loaded.search_with_heap(&[1.0, 0.0, 0.0], 4, &heap);
        let ids: Vec<u64> = results.iter().map(|r| r.0.as_u64()).collect();
        assert!(!ids.contains(&2));
        assert!(!ids.contains(&3));
    }

    #[test]
    fn test_graph_mmap_invalid_magic() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bad.hgr");
        std::fs::write(&path, b"BAAD00000000000000000000000000000000000000000000").unwrap();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let result = open_graph_file(&path, HnswConfig::default(), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_graph_mmap_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nonexistent.hgr");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let result = open_graph_file(&path, HnswConfig::default(), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_issue_1735_graph_mmap_rejects_oversized_fields() {
        // Craft a graph file with neighbor_data_len > u32::MAX.
        // On 32-bit, try_from must reject. On 64-bit, size validation catches it.
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("big_neighbor.hgr");

        let big_len: u64 = (u32::MAX as u64) + 1;
        let mut data = vec![0u8; HEADER_SIZE + 64];
        data[0..4].copy_from_slice(MAGIC);
        data[4..8].copy_from_slice(&1u32.to_le_bytes()); // version
        data[8..16].copy_from_slice(&NONE_SENTINEL.to_le_bytes()); // entry_point = None
        data[16..20].copy_from_slice(&0u32.to_le_bytes()); // max_level
        data[20..24].copy_from_slice(&0u32.to_le_bytes()); // node_count
        data[24..32].copy_from_slice(&big_len.to_le_bytes()); // neighbor_data_len > u32::MAX
        data[32..40].copy_from_slice(&0u64.to_le_bytes()); // node_section_size
        data[40..48].copy_from_slice(&0u64.to_le_bytes()); // reserved

        std::fs::write(&path, &data).unwrap();
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let result = open_graph_file(&path, HnswConfig::default(), config);
        assert!(
            result.is_err(),
            "should reject file with oversized neighbor_data_len"
        );
    }

    #[test]
    fn test_graph_mmap_corrupt_layer_ranges() {
        // Craft a file with a valid header but layer_ranges that point beyond
        // the neighbor data. open_graph_file should reject it.
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("corrupt_layers.hgr");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Build a valid graph, write it, then corrupt it
        let graph = make_test_graph();
        write_graph_file(&path, &graph).unwrap();

        // Read raw bytes, find first node's layer range, and set count to huge value
        let mut data = std::fs::read(&path).unwrap();
        // Node section starts at HEADER_SIZE (48). First node:
        // vid(8) + created_at(8) + deleted_at(8) + num_layers(4) + pad(4) = 32
        // Then first layer: start(4) + count(4)
        // Set count to 0xFFFF (way beyond neighbor_data_len)
        let layer_count_offset = HEADER_SIZE + 32 + 4; // after start(4) of first layer
        data[layer_count_offset..layer_count_offset + 4].copy_from_slice(&0xFFFFu32.to_le_bytes());

        std::fs::write(&path, &data).unwrap();

        let result = open_graph_file(&path, HnswConfig::default(), config);
        assert!(result.is_err(), "Should reject out-of-bounds layer_ranges");
        let err_msg = format!("{}", result.err().unwrap());
        assert!(
            err_msg.contains("layer_range out of bounds"),
            "Error should mention layer_range: {}",
            err_msg
        );
    }

    #[test]
    fn test_graph_mmap_truncated_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("truncated.hgr");

        // Write just a valid header with inflated sizes
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&VERSION.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes()); // entry_point = None sentinel
        data.extend_from_slice(&0u32.to_le_bytes()); // max_level
        data.extend_from_slice(&1u32.to_le_bytes()); // node_count = 1
        data.extend_from_slice(&100u64.to_le_bytes()); // neighbor_data_len
        data.extend_from_slice(&1000u64.to_le_bytes()); // node_section_size (too big)
        data.extend_from_slice(&0u64.to_le_bytes()); // reserved
        std::fs::write(&path, &data).unwrap();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let result = open_graph_file(&path, HnswConfig::default(), config);
        assert!(result.is_err(), "Should reject truncated file");
    }

    #[test]
    fn test_graph_mmap_single_node() {
        // Edge case: graph with exactly one node, no neighbors
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("single.hgr");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        let entries = vec![(
            VectorId::new(42),
            CompactHnswNode {
                layer_ranges: vec![(0, 0)], // layer 0, zero neighbors
                created_at: 100,
                deleted_at: None,
            },
        )];
        let (dense_nodes, id_offset, node_count) = CompactHnswGraph::build_dense(entries);

        let graph = CompactHnswGraph {
            config: HnswConfig::default(),
            vector_config: config.clone(),
            neighbor_data: NeighborData::Owned(Vec::new()),
            dense_nodes,
            id_offset,
            node_count,
            entry_point: Some(VectorId::new(42)),
            max_level: 0,
            _lock_file: None,
        };

        write_graph_file(&path, &graph).unwrap();
        let loaded = open_graph_file(&path, HnswConfig::default(), config.clone()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.entry_point, Some(VectorId::new(42)));
        assert!(loaded.neighbor_data.is_mmap());

        // Search should find the single node
        let mut heap = crate::heap::VectorHeap::new(config);
        heap.upsert(VectorId::new(42), &[1.0, 0.0, 0.0]).unwrap();
        let results = loaded.search_with_heap(&[1.0, 0.0, 0.0], 10, &heap);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VectorId::new(42));
    }
}
